use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use chrono::Utc;
use serde_json::Value;
use tokio::{
    sync::{oneshot, watch, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::{sleep, sleep_until, timeout, Instant},
};
use uuid::Uuid;

use crate::{
    dsl::{compiled::CompiledAgent, CompileError},
    events::hub::{EventError, EventHub},
    history::{
        repository::{HistoryError, RunRepository},
        types::{summarize_input, NewRun, RunAttachment, RunRecord},
    },
    nodes::registry::NodeExecutorRegistry,
};

use super::{
    attachment::{AttachedRun, LeaseOwner, RunSubscription, SubscriptionLease},
    stop_pair, RunCoordinator, RunState, StopController, StopReason, StopSignal,
};

#[derive(Clone, Default)]
pub struct CompiledAgentRegistry {
    agents: BTreeMap<String, Arc<CompiledAgent>>,
}

impl CompiledAgentRegistry {
    pub fn new(agents: Vec<Arc<CompiledAgent>>) -> Result<Self, CompileError> {
        let mut registry = Self::default();
        for agent in agents {
            let agent_id = agent.id.clone();
            if registry.agents.insert(agent_id.clone(), agent).is_some() {
                return Err(CompileError::new(
                    "DUPLICATE_AGENT",
                    format!("compiled agent '{agent_id}' is already registered"),
                ));
            }
        }
        Ok(registry)
    }

    pub fn get(&self, agent_id: &str) -> Option<Arc<CompiledAgent>> {
        self.agents.get(agent_id).cloned()
    }

    pub fn list(&self) -> impl Iterator<Item = &Arc<CompiledAgent>> {
        self.agents.values()
    }
}

#[derive(Debug, Clone, Default)]
pub struct RequestMetadata {
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct RunServiceConfig {
    pub max_concurrent_runs: usize,
    pub run_timeout: Duration,
    pub attached_reconnect_grace: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceError {
    code: &'static str,
    message: String,
}

impl ServiceError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ServiceError {}

struct ActiveRun {
    attachment: RunAttachment,
    stop: StopController,
    state: Arc<RunState>,
    subscribers: usize,
    grace_generation: u64,
    task: JoinHandle<()>,
    _permit: OwnedSemaphorePermit,
}

struct RunServiceInner {
    agents: CompiledAgentRegistry,
    executors: NodeExecutorRegistry,
    repository: Arc<dyn RunRepository>,
    events: EventHub,
    capacity: Arc<Semaphore>,
    config: RunServiceConfig,
    active: Mutex<BTreeMap<String, ActiveRun>>,
    active_changed: watch::Sender<u64>,
    accepting: AtomicBool,
}

#[derive(Clone)]
pub struct RunService {
    inner: Arc<RunServiceInner>,
}

impl fmt::Debug for RunService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunService")
            .field("agent_ids", &self.inner.agents.agents.keys())
            .field(
                "max_concurrent_runs",
                &self.inner.config.max_concurrent_runs,
            )
            .finish_non_exhaustive()
    }
}

struct PreparedRun {
    agent: Arc<CompiledAgent>,
    new_run: NewRun,
    input: Value,
    stop: StopController,
    signal: StopSignal,
    state: Arc<RunState>,
    permit: OwnedSemaphorePermit,
}

impl RunService {
    pub fn new(
        agents: CompiledAgentRegistry,
        executors: NodeExecutorRegistry,
        repository: Arc<dyn RunRepository>,
        events: EventHub,
        config: RunServiceConfig,
    ) -> Result<Self, ServiceError> {
        if config.max_concurrent_runs == 0
            || config.run_timeout.is_zero()
            || config.attached_reconnect_grace.is_zero()
        {
            return Err(ServiceError::new(
                "RUN_SERVICE_CONFIG_INVALID",
                "run service capacities and durations must be greater than zero",
            ));
        }
        Ok(Self {
            inner: Arc::new(RunServiceInner {
                agents,
                executors,
                repository,
                events,
                capacity: Arc::new(Semaphore::new(config.max_concurrent_runs)),
                config,
                active: Mutex::new(BTreeMap::new()),
                active_changed: watch::channel(0).0,
                accepting: AtomicBool::new(true),
            }),
        })
    }

    pub fn agents(&self) -> &CompiledAgentRegistry {
        &self.inner.agents
    }

    pub fn is_healthy(&self) -> bool {
        self.inner.accepting.load(Ordering::Acquire) && self.inner.events.is_healthy()
    }

    pub async fn create_attached(
        &self,
        agent_id: &str,
        input: Value,
        request: RequestMetadata,
    ) -> Result<AttachedRun, ServiceError> {
        let prepared = self
            .prepare_run(agent_id, input, request, RunAttachment::Attached)
            .await?;
        let run_id = prepared.new_run.run_id.clone();
        let request_id = prepared.new_run.request_id.clone();
        let live = self.inner.events.subscribe(&run_id).await;
        self.launch(prepared, 1);
        let owner: Arc<dyn LeaseOwner> = self.inner.clone();
        let lease = Arc::new(SubscriptionLease::new(owner, &run_id, true));
        Ok(AttachedRun {
            run_id: run_id.clone(),
            request_id,
            subscription: RunSubscription::new(
                run_id,
                Vec::new(),
                Some(live),
                true,
                false,
                0,
                lease,
            ),
        })
    }

    pub async fn create_detached(
        &self,
        agent_id: &str,
        input: Value,
        request: RequestMetadata,
    ) -> Result<RunRecord, ServiceError> {
        let prepared = self
            .prepare_run(agent_id, input, request, RunAttachment::Detached)
            .await?;
        let record = self.get_run(&prepared.new_run.run_id).await?;
        self.launch(prepared, 0);
        Ok(record)
    }

    pub async fn subscribe(
        &self,
        run_id: &str,
        after_seq: u64,
    ) -> Result<RunSubscription, ServiceError> {
        let record = self.get_run(run_id).await?;
        let terminal = record.status.is_terminal();
        let counted = {
            let mut active = lock_active(&self.inner);
            match active.get_mut(run_id).filter(|_| !terminal) {
                Some(run) => {
                    run.subscribers = run.subscribers.saturating_add(1);
                    run.grace_generation = run.grace_generation.wrapping_add(1);
                    true
                }
                None => false,
            }
        };
        let live = if counted {
            self.inner.events.subscribe_existing(run_id).await
        } else {
            None
        };
        let live_counted = counted && live.is_some();
        if counted && !live_counted {
            Arc::clone(&self.inner).release_subscription(run_id);
        }
        let replay = match self.inner.events.replay_page_after(run_id, after_seq).await {
            Ok(replay) => replay,
            Err(error) => {
                if live_counted {
                    Arc::clone(&self.inner).release_subscription(run_id);
                }
                return Err(service_event_error(error));
            }
        };
        let owner: Arc<dyn LeaseOwner> = self.inner.clone();
        let lease = Arc::new(SubscriptionLease::new(owner, run_id, live_counted));
        Ok(RunSubscription::new(
            run_id,
            replay.events,
            live,
            live_counted && !replay.has_more,
            replay.has_more,
            after_seq,
            lease,
        ))
    }

    pub async fn get_run(&self, run_id: &str) -> Result<RunRecord, ServiceError> {
        self.inner
            .repository
            .get_run(run_id)
            .await
            .map_err(service_history_error)?
            .ok_or_else(|| ServiceError::new("RUN_NOT_FOUND", "run not found"))
    }

    pub async fn cancel(&self, run_id: &str) -> Result<RunRecord, ServiceError> {
        let record = self.get_run(run_id).await?;
        if record.status.is_terminal() {
            return Ok(record);
        }
        let (stop, mut active_changed) = {
            let active = lock_active(&self.inner);
            let run = active.get(run_id).ok_or_else(|| {
                ServiceError::new("RUN_CONFLICT", "run is not active in this process")
            })?;
            (run.stop.clone(), self.inner.active_changed.subscribe())
        };
        stop.request(StopReason::Cancelled);
        loop {
            let current = self.get_run(run_id).await?;
            if current.status.is_terminal() {
                return Ok(current);
            }
            if !lock_active(&self.inner).contains_key(run_id) {
                return Err(ServiceError::new(
                    "RUN_CONFLICT",
                    "run execution ended without a durable terminal state",
                ));
            }
            active_changed
                .changed()
                .await
                .map_err(|_| ServiceError::new("RUN_CONFLICT", "run completion channel closed"))?;
        }
    }

    pub async fn reconcile_startup(&self) -> Result<u64, ServiceError> {
        self.inner
            .repository
            .mark_incomplete_interrupted(Utc::now())
            .await
            .map_err(service_history_error)
    }

    pub async fn shutdown(&self, deadline: Duration) -> Result<(), ServiceError> {
        self.inner.accepting.store(false, Ordering::Release);
        let mut active_changed = self.inner.active_changed.subscribe();
        let stops = {
            let active = lock_active(&self.inner);
            active
                .values()
                .map(|run| {
                    let _already_finished = run.task.is_finished();
                    (run.stop.clone(), run.attachment)
                })
                .collect::<Vec<_>>()
        };
        for (stop, attachment) in stops {
            let reason = match attachment {
                RunAttachment::Attached => StopReason::Cancelled,
                RunAttachment::Detached => StopReason::Interrupted,
            };
            stop.request(reason);
        }
        let wait_for_empty = async {
            loop {
                if lock_active(&self.inner).is_empty() {
                    break;
                }
                active_changed.changed().await.map_err(|_| {
                    ServiceError::new("SHUTDOWN_FAILED", "run completion channel closed")
                })?;
            }
            Ok::<(), ServiceError>(())
        };
        timeout(deadline, wait_for_empty)
            .await
            .map_err(|_| ServiceError::new("SHUTDOWN_TIMEOUT", "run shutdown timed out"))??;
        Ok(())
    }

    async fn prepare_run(
        &self,
        agent_id: &str,
        input: Value,
        request: RequestMetadata,
        attachment: RunAttachment,
    ) -> Result<PreparedRun, ServiceError> {
        if !self.inner.events.is_healthy() {
            self.inner.accepting.store(false, Ordering::Release);
            return Err(ServiceError::new(
                "RUN_SERVICE_UNAVAILABLE",
                "event journal is unavailable",
            ));
        }
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::new(
                "RUN_SERVICE_STOPPING",
                "run service is stopping",
            ));
        }
        let agent = self
            .inner
            .agents
            .get(agent_id)
            .ok_or_else(|| ServiceError::new("AGENT_NOT_FOUND", "agent not found"))?;
        if !agent.input_schema.is_valid(&input) {
            return Err(ServiceError::new(
                "INPUT_INVALID",
                "input does not match the agent schema",
            ));
        }
        let permit = Arc::clone(&self.inner.capacity)
            .try_acquire_owned()
            .map_err(|_| ServiceError::new("RUN_CAPACITY_EXCEEDED", "run capacity exceeded"))?;
        let run_id = format!("run_{}", Uuid::new_v4().simple());
        let request_id = request
            .request_id
            .filter(|request_id| !request_id.trim().is_empty())
            .unwrap_or_else(|| format!("req_{}", Uuid::new_v4().simple()));
        let new_run = NewRun {
            run_id,
            request_id,
            agent_id: agent.id.clone(),
            agent_version: agent.version_hash.clone(),
            attachment,
            created_at: Utc::now(),
            input_summary: summarize_input(&input),
        };
        self.inner
            .repository
            .create_run(new_run.clone())
            .await
            .map_err(service_history_error)?;
        self.inner.events.open_run(&new_run.run_id).await;
        let (stop, signal) = stop_pair();
        Ok(PreparedRun {
            agent,
            new_run,
            input,
            stop,
            signal,
            state: Arc::new(RunState::new()),
            permit,
        })
    }

    fn launch(&self, prepared: PreparedRun, subscribers: usize) {
        let PreparedRun {
            agent,
            new_run,
            input,
            stop,
            signal,
            state,
            permit,
        } = prepared;
        let run_id = new_run.run_id.clone();
        let attachment = new_run.attachment;
        let task_stop = stop.clone();
        let task_state = Arc::clone(&state);
        let inner = Arc::clone(&self.inner);
        let (start, started) = oneshot::channel();
        let task_run_id = run_id.clone();
        let task = tokio::spawn(async move {
            if started.await.is_err() {
                return;
            }
            let coordinator = RunCoordinator::new(
                agent,
                inner.executors.clone(),
                inner.events.clone(),
                Arc::clone(&inner.repository),
            );
            let execution =
                coordinator.execute_existing(new_run, input, signal, Arc::clone(&task_state));
            tokio::pin!(execution);
            let result = tokio::select! {
                result = &mut execution => result,
                _ = sleep(inner.config.run_timeout) => {
                    task_stop.request(StopReason::TimedOut);
                    execution.await
                }
            };
            if let Err(error) = result {
                inner.accepting.store(false, Ordering::Release);
                tracing::error!(
                    run_id = task_run_id,
                    code = error.code(),
                    "run coordinator failed"
                );
            }
            if !inner.events.is_healthy() {
                inner.accepting.store(false, Ordering::Release);
            }
            let removed = lock_active(&inner).remove(&task_run_id);
            drop(removed);
            inner
                .active_changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        });
        lock_active(&self.inner).insert(
            run_id,
            ActiveRun {
                attachment,
                stop,
                state,
                subscribers,
                grace_generation: 0,
                task,
                _permit: permit,
            },
        );
        let _ = start.send(());
    }
}

impl LeaseOwner for RunServiceInner {
    fn release_subscription(self: Arc<Self>, run_id: &str) {
        let generation = {
            let mut active = lock_active(&self);
            let Some(run) = active.get_mut(run_id) else {
                return;
            };
            run.subscribers = run.subscribers.saturating_sub(1);
            if run.attachment != RunAttachment::Attached || run.subscribers != 0 {
                return;
            }
            run.grace_generation = run.grace_generation.wrapping_add(1);
            run.grace_generation
        };
        let grace = self.config.attached_reconnect_grace;
        let deadline = Instant::now() + grace;
        let run_id = run_id.to_string();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                sleep_until(deadline).await;
                let candidate = {
                    let active = lock_active(&self);
                    active.get(&run_id).and_then(|run| {
                        (run.attachment == RunAttachment::Attached
                            && run.subscribers == 0
                            && run.grace_generation == generation)
                            .then(|| (run.stop.clone(), Arc::clone(&run.state)))
                    })
                };
                if let Some((stop, state)) = candidate {
                    if !state.status().await.is_terminal() {
                        stop.request(StopReason::Cancelled);
                    }
                }
            });
        } else {
            let stop = lock_active(&self).get(&run_id).map(|run| run.stop.clone());
            if let Some(stop) = stop {
                stop.request(StopReason::Cancelled);
            }
        }
    }
}

fn lock_active(inner: &RunServiceInner) -> MutexGuard<'_, BTreeMap<String, ActiveRun>> {
    inner
        .active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn service_history_error(error: HistoryError) -> ServiceError {
    ServiceError::new(error.code(), error.to_string())
}

fn service_event_error(error: EventError) -> ServiceError {
    ServiceError::new(error.code(), error.to_string())
}
