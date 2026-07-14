use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::{Duration, Instant},
};

use chrono::Utc;
use serde_json::{json, Value};
use tokio::{
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::{sleep, timeout},
};
use uuid::Uuid;

use crate::{
    dsl::{compiled::CompiledAgent, CompileError},
    events::{
        hub::EventHub,
        protocol::{RunEventScope, RunEventType},
    },
    history::{
        repository::{HistoryError, RunRepository},
        types::{
            summarize_input, NewRun, RunAttachment, RunRecord, RunStatus, RunTerminal, StopError,
            TerminalUpdate,
        },
    },
    nodes::registry::NodeExecutorRegistry,
    outcome::{FailureKind, RunFailure},
};

use super::{
    attachment::{AttachedRun, LeaseOwner, RunSubscription, SubscriptionLease},
    stop_pair, ExecutionLimiter, RunCoordinator, RunState, StopController, StopReason, StopSignal,
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
    pub max_parallel_node_executions: usize,
    pub max_parallel_branches_per_run: usize,
    pub run_timeout: Duration,
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
    task: JoinHandle<()>,
    _permit: OwnedSemaphorePermit,
}

struct PreparingRun {
    agent: Arc<CompiledAgent>,
    new_run: NewRun,
    input: Value,
    attachment: RunAttachment,
    stop: StopController,
    signal: StopSignal,
    state: Arc<RunState>,
    permit: OwnedSemaphorePermit,
    durable: bool,
}

struct RunServiceInner {
    agents: CompiledAgentRegistry,
    executors: NodeExecutorRegistry,
    repository: Arc<dyn RunRepository>,
    events: EventHub,
    capacity: Arc<Semaphore>,
    node_capacity: Arc<Semaphore>,
    config: RunServiceConfig,
    preparing: Mutex<BTreeMap<String, PreparingRun>>,
    active: Mutex<BTreeMap<String, ActiveRun>>,
    lifecycle_changed: watch::Sender<u64>,
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
    run_id: String,
    request_id: String,
    guard: PreparingGuard,
}

struct PreparingGuard {
    inner: Arc<RunServiceInner>,
    run_id: String,
    armed: bool,
}

impl PreparingGuard {
    fn new(inner: Arc<RunServiceInner>, run_id: String) -> Self {
        Self {
            inner,
            run_id,
            armed: true,
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PreparingGuard {
    fn drop(&mut self) {
        if self.armed {
            self.inner.finalize_preparing_from_guard_drop(&self.run_id);
        }
    }
}

impl RunServiceInner {
    fn notify_lifecycle(&self) {
        self.lifecycle_changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    fn lifecycle_is_empty(&self) -> bool {
        lock_preparing(self).is_empty() && lock_active(self).is_empty()
    }

    fn mark_preparing_durable(&self, run_id: &str) {
        if let Some(run) = lock_preparing(self).get_mut(run_id) {
            run.durable = true;
        }
        self.notify_lifecycle();
    }

    fn remove_preparing(&self, run_id: &str) {
        let removed = lock_preparing(self).remove(run_id);
        drop(removed);
        self.notify_lifecycle();
    }

    fn lifecycle_stops(&self) -> Vec<(StopController, RunAttachment)> {
        let preparing = lock_preparing(self)
            .values()
            .map(|run| (run.stop.clone(), run.attachment))
            .collect::<Vec<_>>();
        let active = lock_active(self)
            .values()
            .map(|run| {
                let _already_finished = run.task.is_finished();
                (run.stop.clone(), run.attachment)
            })
            .collect::<Vec<_>>();
        preparing.into_iter().chain(active).collect()
    }

    fn finalize_preparing_from_guard_drop(self: &Arc<Self>, run_id: &str) {
        let mut preparing = lock_preparing(self);
        let Some(preparing_run) = preparing.remove(run_id) else {
            return;
        };
        let mut active = lock_active(self);
        let reason = preparing_run
            .signal
            .reason()
            .unwrap_or_else(|| stop_reason_for_attachment(preparing_run.attachment));
        preparing_run.stop.request(reason);
        let service = RunService {
            inner: Arc::clone(self),
        };
        let active_run = service.spawn_finalizer_active(preparing_run, reason);
        active.insert(run_id.to_string(), active_run);
        drop(active);
        drop(preparing);
        self.notify_lifecycle();
    }
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
            || config.max_parallel_node_executions == 0
            || config.max_parallel_branches_per_run == 0
            || config.run_timeout.is_zero()
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
                node_capacity: Arc::new(Semaphore::new(config.max_parallel_node_executions)),
                config,
                preparing: Mutex::new(BTreeMap::new()),
                active: Mutex::new(BTreeMap::new()),
                lifecycle_changed: watch::channel(0).0,
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
        let run_id = prepared.run_id.clone();
        let request_id = prepared.request_id.clone();
        let live = self.inner.events.subscribe(&run_id).await;
        let owner: Arc<dyn LeaseOwner> = self.inner.clone();
        let lease = SubscriptionLease::new(owner, &run_id);
        self.launch(prepared);
        Ok(AttachedRun {
            run_id: run_id.clone(),
            request_id,
            subscription: RunSubscription::new(run_id, live, lease),
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
        let record = self.get_run(&prepared.run_id).await?;
        self.launch(prepared);
        Ok(record)
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
        if record.status().is_terminal() {
            return Ok(record);
        }
        let (stop, mut lifecycle_changed) = {
            let active = lock_active(&self.inner);
            let run = active.get(run_id).ok_or_else(|| {
                ServiceError::new("RUN_CONFLICT", "run is not active in this process")
            })?;
            (run.stop.clone(), self.inner.lifecycle_changed.subscribe())
        };
        stop.request(StopReason::Cancelled);
        loop {
            let current = self.get_run(run_id).await?;
            if current.status().is_terminal() {
                return Ok(current);
            }
            if !lock_active(&self.inner).contains_key(run_id) {
                return Err(ServiceError::new(
                    "RUN_CONFLICT",
                    "run execution ended without a durable terminal state",
                ));
            }
            lifecycle_changed
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
        let started = Instant::now();
        let mut lifecycle_changed = self.inner.lifecycle_changed.subscribe();
        let stops = self.inner.lifecycle_stops();
        for (stop, attachment) in stops {
            stop.request(stop_reason_for_attachment(attachment));
        }
        let wait_for_empty = async {
            loop {
                if self.inner.lifecycle_is_empty() {
                    break;
                }
                lifecycle_changed.changed().await.map_err(|_| {
                    ServiceError::new("SHUTDOWN_FAILED", "run completion channel closed")
                })?;
            }
            Ok::<(), ServiceError>(())
        };
        let result = timeout(deadline, wait_for_empty)
            .await
            .map_err(|_| ServiceError::new("SHUTDOWN_TIMEOUT", "run shutdown timed out"))?;
        result?;
        let remaining = deadline.checked_sub(started.elapsed()).unwrap_or_default();
        self.inner
            .events
            .wait_for_recoveries(remaining)
            .await
            .map_err(|error| {
                if error.code() == "JOURNAL_OPERATION_TIMEOUT" {
                    ServiceError::new("SHUTDOWN_TIMEOUT", "run shutdown timed out")
                } else {
                    ServiceError::new(error.code(), error.to_string())
                }
            })?;
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
            run_id: run_id.clone(),
            request_id: request_id.clone(),
            agent_id: agent.id.clone(),
            agent_version: agent.version_hash.clone(),
            attachment,
            created_at: Utc::now(),
            input_summary: summarize_input(&input),
        };
        let (stop, signal) = stop_pair();
        lock_preparing(&self.inner).insert(
            run_id.clone(),
            PreparingRun {
                agent,
                new_run: new_run.clone(),
                input,
                attachment,
                stop,
                signal,
                state: Arc::new(RunState::new()),
                permit,
                durable: false,
            },
        );
        self.inner.notify_lifecycle();
        let guard = PreparingGuard::new(Arc::clone(&self.inner), run_id.clone());

        self.inner
            .repository
            .create_run(new_run.clone())
            .await
            .map_err(|error| {
                self.inner.remove_preparing(&run_id);
                service_history_error(error)
            })?;
        self.inner.mark_preparing_durable(&run_id);
        self.inner.events.open_run(&run_id).await;

        Ok(PreparedRun {
            run_id,
            request_id,
            guard,
        })
    }

    fn launch(&self, prepared: PreparedRun) {
        let run_id = prepared.run_id.clone();
        let promoted = self.promote_preparing(&run_id);
        prepared.guard.disarm();
        if !promoted {
            tracing::warn!(run_id, "prepared run was already removed before launch");
        }
    }

    fn promote_preparing(&self, run_id: &str) -> bool {
        let mut preparing = lock_preparing(&self.inner);
        let Some(preparing_run) = preparing.remove(run_id) else {
            return false;
        };
        let mut active = lock_active(&self.inner);
        let should_run =
            self.inner.accepting.load(Ordering::Acquire) && preparing_run.signal.reason().is_none();
        let active_run = if should_run {
            self.spawn_scheduler_active(preparing_run)
        } else {
            let reason = preparing_run
                .signal
                .reason()
                .unwrap_or_else(|| stop_reason_for_attachment(preparing_run.attachment));
            preparing_run.stop.request(reason);
            self.spawn_finalizer_active(preparing_run, reason)
        };
        active.insert(run_id.to_string(), active_run);
        drop(active);
        drop(preparing);
        self.inner.notify_lifecycle();
        true
    }

    fn spawn_scheduler_active(&self, preparing: PreparingRun) -> ActiveRun {
        let PreparingRun {
            agent,
            new_run,
            input,
            attachment,
            stop,
            signal,
            state,
            permit,
            durable: _,
        } = preparing;
        let run_id = new_run.run_id.clone();
        let task_stop = stop.clone();
        let task_state = Arc::clone(&state);
        let inner = Arc::clone(&self.inner);
        let task_run_id = run_id.clone();
        let task = tokio::spawn(async move {
            let coordinator = RunCoordinator::new(
                agent,
                inner.executors.clone(),
                inner.events.clone(),
                Arc::clone(&inner.repository),
                ExecutionLimiter::new(
                    Arc::clone(&inner.node_capacity),
                    Arc::new(Semaphore::new(inner.config.max_parallel_branches_per_run)),
                ),
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
            inner.notify_lifecycle();
        });
        ActiveRun {
            attachment,
            stop,
            task,
            _permit: permit,
        }
    }

    fn spawn_finalizer_active(&self, preparing: PreparingRun, reason: StopReason) -> ActiveRun {
        let PreparingRun {
            new_run,
            attachment,
            stop,
            permit,
            durable,
            ..
        } = preparing;
        let inner = Arc::clone(&self.inner);
        let run_id = new_run.run_id.clone();
        let task_run_id = run_id.clone();
        let task = tokio::spawn(async move {
            if durable {
                if let Err(error) =
                    publish_prelaunch_terminal(Arc::clone(&inner), new_run, reason).await
                {
                    inner.accepting.store(false, Ordering::Release);
                    tracing::error!(
                        run_id = task_run_id,
                        code = error.code(),
                        "prelaunch finalizer failed"
                    );
                }
            }
            let removed = lock_active(&inner).remove(&task_run_id);
            drop(removed);
            inner.notify_lifecycle();
        });
        ActiveRun {
            attachment,
            stop,
            task,
            _permit: permit,
        }
    }
}

impl LeaseOwner for RunServiceInner {
    fn release_subscription(self: Arc<Self>, run_id: &str) {
        let preparing_stop = {
            let preparing = lock_preparing(&self);
            preparing
                .get(run_id)
                .filter(|run| run.attachment == RunAttachment::Attached)
                .map(|run| run.stop.clone())
        };
        if let Some(stop) = preparing_stop {
            stop.request(StopReason::Cancelled);
            return;
        }

        let active_stop = {
            let active = lock_active(&self);
            active
                .get(run_id)
                .filter(|run| run.attachment == RunAttachment::Attached)
                .map(|run| run.stop.clone())
        };
        if let Some(stop) = active_stop {
            stop.request(StopReason::Cancelled);
        }
    }
}

fn lock_preparing(inner: &RunServiceInner) -> MutexGuard<'_, BTreeMap<String, PreparingRun>> {
    inner
        .preparing
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_active(inner: &RunServiceInner) -> MutexGuard<'_, BTreeMap<String, ActiveRun>> {
    inner
        .active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn stop_reason_for_attachment(attachment: RunAttachment) -> StopReason {
    match attachment {
        RunAttachment::Attached => StopReason::Cancelled,
        RunAttachment::Detached => StopReason::Interrupted,
    }
}

fn run_scope(run: &NewRun) -> RunEventScope {
    RunEventScope::for_run(
        run.request_id.clone(),
        run.run_id.clone(),
        run.agent_id.clone(),
        run.agent_version.clone(),
    )
}

fn terminal_spec(reason: StopReason) -> RunTerminal {
    match reason {
        StopReason::Cancelled => RunTerminal::Cancelled {
            error: StopError {
                code: "RUN_CANCELLED".to_string(),
                message: "run cancelled".to_string(),
            },
        },
        StopReason::Interrupted => RunTerminal::Interrupted {
            error: StopError {
                code: "RUN_INTERRUPTED".to_string(),
                message: "run interrupted".to_string(),
            },
        },
        StopReason::TimedOut => RunTerminal::Failed {
            error: RunFailure {
                kind: FailureKind::Timeout,
                code: "RUN_TIMEOUT".to_string(),
                message: "run timed out".to_string(),
            },
        },
    }
}

async fn publish_prelaunch_terminal(
    inner: Arc<RunServiceInner>,
    run: NewRun,
    reason: StopReason,
) -> Result<(), ServiceError> {
    inner
        .events
        .publish(
            run_scope(&run),
            RunEventType::RunCreated,
            json!({
                "status": RunStatus::Created,
                "attachment": run.attachment,
            }),
        )
        .await
        .map_err(|error| ServiceError::new(error.code(), error.to_string()))?;
    let terminal = terminal_spec(reason);
    let update = TerminalUpdate::new(&run.run_id, Utc::now(), terminal);
    inner
        .events
        .publish_terminal(run_scope(&run), update)
        .await
        .map_err(|error| ServiceError::new(error.code(), error.to_string()))?;
    Ok(())
}

fn service_history_error(error: HistoryError) -> ServiceError {
    ServiceError::new(error.code(), error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use serde_json::json;

    use crate::{
        dsl::compiled::ExecutionPlan,
        history::types::{NodeOutputRecord, RunLifecycle},
    };

    #[derive(Default)]
    struct UnitRepository {
        runs: tokio::sync::Mutex<BTreeMap<String, RunRecord>>,
        events: tokio::sync::Mutex<Vec<crate::events::protocol::RunEvent>>,
    }

    #[async_trait]
    impl RunRepository for UnitRepository {
        async fn create_run(&self, run: NewRun) -> Result<(), HistoryError> {
            self.runs.lock().await.insert(
                run.run_id.clone(),
                RunRecord {
                    run_id: run.run_id,
                    request_id: run.request_id,
                    agent_id: run.agent_id,
                    agent_version: run.agent_version,
                    attachment: run.attachment,
                    lifecycle: RunLifecycle::Created,
                    started_at: None,
                    ended_at: None,
                    updated_at: run.created_at,
                    input_summary: run.input_summary,
                },
            );
            Ok(())
        }

        async fn mark_running(
            &self,
            run_id: &str,
            started_at: DateTime<Utc>,
        ) -> Result<(), HistoryError> {
            let mut runs = self.runs.lock().await;
            let run = runs
                .get_mut(run_id)
                .ok_or_else(|| HistoryError::new("RUN_NOT_FOUND", "run not found"))?;
            run.lifecycle = RunLifecycle::Running;
            run.started_at = Some(started_at);
            run.updated_at = started_at;
            Ok(())
        }

        async fn append_events(
            &self,
            events: &[crate::events::protocol::RunEvent],
        ) -> Result<(), HistoryError> {
            self.events.lock().await.extend_from_slice(events);
            Ok(())
        }

        async fn put_node_output(&self, _output: NodeOutputRecord) -> Result<(), HistoryError> {
            Ok(())
        }

        async fn finish_run(
            &self,
            update: TerminalUpdate,
            event: crate::events::protocol::RunEvent,
        ) -> Result<bool, HistoryError> {
            let mut runs = self.runs.lock().await;
            let run = runs
                .get_mut(&update.run_id)
                .ok_or_else(|| HistoryError::new("RUN_NOT_FOUND", "run not found"))?;
            run.lifecycle = match update.terminal {
                RunTerminal::Completed { output } => RunLifecycle::Completed { output },
                RunTerminal::Failed { error } => RunLifecycle::Failed { error },
                RunTerminal::Cancelled { error } => RunLifecycle::Cancelled { error },
                RunTerminal::Interrupted { error } => RunLifecycle::Interrupted { error },
            };
            run.ended_at = Some(update.ended_at);
            run.updated_at = update.ended_at;
            drop(runs);
            self.events.lock().await.push(event);
            Ok(true)
        }

        async fn recover_run(
            &self,
            update: TerminalUpdate,
            terminal: crate::events::protocol::RunEvent,
        ) -> Result<crate::events::protocol::RunEvent, HistoryError> {
            self.finish_run(update, terminal.clone()).await?;
            Ok(terminal)
        }

        async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, HistoryError> {
            Ok(self.runs.lock().await.get(run_id).cloned())
        }

        async fn list_events_after(
            &self,
            run_id: &str,
            after_seq: u64,
            limit: usize,
        ) -> Result<Vec<crate::events::protocol::RunEvent>, HistoryError> {
            Ok(self
                .events
                .lock()
                .await
                .iter()
                .filter(|event| event.run_id == run_id && event.seq > after_seq)
                .take(limit)
                .cloned()
                .collect())
        }

        async fn mark_incomplete_interrupted(
            &self,
            _at: DateTime<Utc>,
        ) -> Result<u64, HistoryError> {
            Ok(0)
        }
    }

    #[tokio::test]
    async fn attached_subscription_drop_before_launch_finalizes_cancelled() {
        let repository = Arc::new(UnitRepository::default());
        let repository_trait: Arc<dyn RunRepository> = repository.clone();
        let events = EventHub::new(
            Arc::clone(&repository_trait),
            crate::events::hub::EventHubConfig {
                subscriber_capacity: 8,
                journal_capacity: 32,
                journal_batch_size: 8,
                operation_timeout: Duration::from_secs(1),
            },
        );
        let service = RunService::new(
            CompiledAgentRegistry::new(Vec::new()).unwrap(),
            NodeExecutorRegistry::default(),
            Arc::clone(&repository_trait),
            events,
            RunServiceConfig {
                max_concurrent_runs: 1,
                max_parallel_node_executions: 1,
                max_parallel_branches_per_run: 1,
                run_timeout: Duration::from_secs(1),
            },
        )
        .unwrap();
        let permit = Arc::clone(&service.inner.capacity)
            .try_acquire_owned()
            .unwrap();
        let new_run = NewRun {
            run_id: "run_unit_attached".to_string(),
            request_id: "req_unit_attached".to_string(),
            agent_id: "agent".to_string(),
            agent_version: "sha256:agent".to_string(),
            attachment: RunAttachment::Attached,
            created_at: Utc::now(),
            input_summary: json!({"keys":[], "serialized_bytes":2}),
        };
        repository.create_run(new_run.clone()).await.unwrap();
        let (stop, signal) = stop_pair();
        lock_preparing(&service.inner).insert(
            new_run.run_id.clone(),
            PreparingRun {
                agent: Arc::new(CompiledAgent {
                    id: "agent".to_string(),
                    name: "agent".to_string(),
                    description: String::new(),
                    version_hash: "sha256:agent".to_string(),
                    input_schema: Arc::new(crate::schema::compile_schema(&json!({})).unwrap()),
                    entry: "missing".to_string(),
                    execution_plan: ExecutionPlan::sequential("missing", Vec::<String>::new()),
                    nodes: BTreeMap::new(),
                    templates: Arc::new(handlebars::Handlebars::new()),
                }),
                new_run: new_run.clone(),
                input: json!({}),
                attachment: RunAttachment::Attached,
                stop,
                signal,
                state: Arc::new(RunState::new()),
                permit,
                durable: true,
            },
        );
        service.inner.notify_lifecycle();

        let owner: Arc<dyn LeaseOwner> = service.inner.clone();
        drop(SubscriptionLease::new(owner, &new_run.run_id));
        service.launch(PreparedRun {
            run_id: new_run.run_id.clone(),
            request_id: new_run.request_id.clone(),
            guard: PreparingGuard::new(Arc::clone(&service.inner), new_run.run_id.clone()),
        });

        for _ in 0..200 {
            if let Some(record) = repository.get_run(&new_run.run_id).await.unwrap() {
                if record.status() == RunStatus::Cancelled {
                    assert!(matches!(
                        record.lifecycle,
                        RunLifecycle::Cancelled { ref error } if error.code == "RUN_CANCELLED"
                    ));
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
        panic!("attached preparing run did not become cancelled");
    }
}
