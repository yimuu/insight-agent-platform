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
use futures::FutureExt;
use serde_json::{json, Value};
use tokio::{
    sync::{oneshot, watch, Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::{timeout, Instant as TokioInstant},
};
use uuid::Uuid;

use crate::{
    catalog::AgentCatalog,
    dsl::vnext::compiler::CompiledWorkflow,
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
    outcome::{FailureKind, RunFailure},
};

use super::{
    attachment::{AttachedRun, LeaseOwner, RunSubscription, SubscriptionLease},
    stop_pair, RunCoordinator, RunState, StopController, StopReason, StopSignal,
};

const READINESS_PROBE_CACHE_TTL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Default)]
pub struct RequestMetadata {
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct RunServiceConfig {
    pub max_concurrent_runs: usize,
    pub max_concurrent_operations: usize,
    pub max_concurrent_operations_per_run: usize,
    pub operation_timeout: Duration,
    pub operation_cancel_grace_period: Duration,
    pub max_template_output_bytes: usize,
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

struct PendingActiveRun {
    active: ActiveRun,
    start: oneshot::Sender<()>,
}

struct PreparingRun {
    agent: Arc<CompiledWorkflow>,
    new_run: NewRun,
    input: Value,
    attachment: RunAttachment,
    stop: StopController,
    signal: StopSignal,
    state: Arc<RunState>,
    permit: OwnedSemaphorePermit,
    durable: bool,
    #[cfg(test)]
    panic_active_task: Option<Arc<tokio::sync::Notify>>,
}

struct RunServiceInner {
    agents: AgentCatalog,
    repository: Arc<dyn RunRepository>,
    events: EventHub,
    capacity: Arc<Semaphore>,
    operation_capacity: Arc<Semaphore>,
    config: RunServiceConfig,
    preparing: Mutex<BTreeMap<String, PreparingRun>>,
    active: Mutex<BTreeMap<String, ActiveRun>>,
    lifecycle_changed: watch::Sender<u64>,
    accepting: AtomicBool,
    runtime_fatal: AtomicBool,
    runtime_fatal_changed: watch::Sender<bool>,
    readiness_probe: AsyncMutex<ReadinessProbeCache>,
}

#[derive(Default)]
struct ReadinessProbeCache {
    checked_at: Option<Instant>,
    available: bool,
}

#[derive(Clone)]
pub struct RunService {
    inner: Arc<RunServiceInner>,
}

impl fmt::Debug for RunService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunService")
            .field("agent_ids", &self.inner.agents.ids().collect::<Vec<_>>())
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

struct ActiveCleanupGuard {
    inner: Arc<RunServiceInner>,
    run_id: String,
    fatal_on_drop: bool,
}

impl ActiveCleanupGuard {
    fn new(inner: Arc<RunServiceInner>, run_id: String) -> Self {
        Self {
            inner,
            run_id,
            fatal_on_drop: true,
        }
    }

    fn complete(mut self) {
        self.fatal_on_drop = false;
    }
}

impl Drop for ActiveCleanupGuard {
    fn drop(&mut self) {
        if self.fatal_on_drop {
            self.inner.trip_runtime_fatal();
            tracing::error!(
                run_id = self.run_id,
                code = "RUNTIME_TASK_PANICKED",
                "run task ended outside its containment boundary"
            );
        }
        let removed = lock_active(&self.inner).remove(&self.run_id);
        drop(removed);
        self.inner.notify_lifecycle();
    }
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

    fn close_admission(&self) {
        let _preparing = lock_preparing(self);
        self.close_admission_locked();
    }

    fn close_admission_locked(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    fn trip_runtime_fatal(&self) {
        // Admission closure must become visible before the fatal notification so
        // observers can never receive fatal while readiness still reports healthy.
        self.close_admission();
        if self
            .runtime_fatal
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.runtime_fatal_changed.send_replace(true);
        }
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
        let pending = service.spawn_finalizer_active(preparing_run, reason);
        active.insert(run_id.to_string(), pending.active);
        drop(active);
        drop(preparing);
        self.notify_lifecycle();
        service.start_active_task(run_id, pending.start);
    }
}

impl RunService {
    pub fn new(
        agents: AgentCatalog,
        repository: Arc<dyn RunRepository>,
        events: EventHub,
        config: RunServiceConfig,
    ) -> Result<Self, ServiceError> {
        if config.max_concurrent_runs == 0
            || config.max_concurrent_operations == 0
            || config.max_concurrent_operations_per_run == 0
            || config.operation_timeout.is_zero()
            || config.operation_cancel_grace_period.is_zero()
            || config.max_template_output_bytes == 0
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
                repository,
                events,
                capacity: Arc::new(Semaphore::new(config.max_concurrent_runs)),
                operation_capacity: Arc::new(Semaphore::new(config.max_concurrent_operations)),
                config,
                preparing: Mutex::new(BTreeMap::new()),
                active: Mutex::new(BTreeMap::new()),
                lifecycle_changed: watch::channel(0).0,
                accepting: AtomicBool::new(true),
                runtime_fatal: AtomicBool::new(false),
                runtime_fatal_changed: watch::channel(false).0,
                readiness_probe: AsyncMutex::new(ReadinessProbeCache::default()),
            }),
        })
    }

    pub fn agents(&self) -> &AgentCatalog {
        &self.inner.agents
    }

    pub fn is_healthy(&self) -> bool {
        self.inner.accepting.load(Ordering::Acquire) && self.inner.events.is_healthy()
    }

    #[doc(hidden)]
    pub fn is_fatal(&self) -> bool {
        self.inner.runtime_fatal.load(Ordering::Acquire)
    }

    #[doc(hidden)]
    pub fn subscribe_fatal(&self) -> watch::Receiver<bool> {
        self.inner.runtime_fatal_changed.subscribe()
    }

    pub async fn check_readiness(&self, deadline: Duration) -> Result<(), ServiceError> {
        if !self.is_healthy() {
            return Err(readiness_unavailable());
        }
        let probe = async {
            let mut cached = self.inner.readiness_probe.lock().await;
            if cached
                .checked_at
                .is_some_and(|checked_at| checked_at.elapsed() < READINESS_PROBE_CACHE_TTL)
            {
                return cached.available;
            }
            let available = self.inner.repository.check_health().await.is_ok();
            cached.checked_at = Some(Instant::now());
            cached.available = available;
            available
        };
        match timeout(deadline, probe).await {
            Ok(true) if self.is_healthy() => Ok(()),
            Ok(_) | Err(_) => Err(readiness_unavailable()),
        }
    }

    pub fn begin_shutdown(&self) {
        // Linearize admission closure with preparing ownership registration. A create
        // request is therefore either visible to shutdown or rejected after this store.
        self.inner.close_admission();
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
        self.begin_shutdown();
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
            self.inner.close_admission();
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
        let input = agent.normalize_input(input).map_err(|_| {
            ServiceError::new("INPUT_INVALID", "input does not match the agent schema")
        })?;
        if !agent.normalized_input_validator().is_valid(&input) {
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
            agent_id: agent.ir.metadata.id.as_str().to_string(),
            agent_version: agent.version_hash.clone(),
            attachment,
            created_at: Utc::now(),
            input_summary: summarize_input(&input),
        };
        let (stop, signal) = stop_pair();
        {
            let mut preparing = lock_preparing(&self.inner);
            if !self.inner.events.is_healthy() {
                self.inner.close_admission_locked();
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
            preparing.insert(
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
                    #[cfg(test)]
                    panic_active_task: None,
                },
            );
        }
        self.inner.notify_lifecycle();
        let guard = PreparingGuard::new(Arc::clone(&self.inner), run_id.clone());

        self.inner
            .repository
            .create_run(new_run.clone())
            .await
            .map_err(|error| {
                if error.code() == "HISTORY_OWNERSHIP_LOST" {
                    self.inner.close_admission();
                }
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
        let pending = if should_run {
            self.spawn_scheduler_active(preparing_run)
        } else {
            let reason = preparing_run
                .signal
                .reason()
                .unwrap_or_else(|| stop_reason_for_attachment(preparing_run.attachment));
            preparing_run.stop.request(reason);
            self.spawn_finalizer_active(preparing_run, reason)
        };
        active.insert(run_id.to_string(), pending.active);
        drop(active);
        drop(preparing);
        self.inner.notify_lifecycle();
        self.start_active_task(run_id, pending.start);
        true
    }

    fn start_active_task(&self, run_id: &str, start: oneshot::Sender<()>) {
        if start.send(()).is_ok() {
            return;
        }
        self.inner.trip_runtime_fatal();
        tracing::error!(
            run_id,
            code = "RUN_TASK_START_FAILED",
            "registered run task could not be started"
        );
        let removed = lock_active(&self.inner).remove(run_id);
        drop(removed);
        self.inner.notify_lifecycle();
    }

    fn spawn_scheduler_active(&self, preparing: PreparingRun) -> PendingActiveRun {
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
            #[cfg(test)]
            panic_active_task,
        } = preparing;
        let run_id = new_run.run_id.clone();
        let task_state = Arc::clone(&state);
        let inner = Arc::clone(&self.inner);
        let task_run_id = run_id.clone();
        let (start, started) = oneshot::channel();
        let task = tokio::spawn(async move {
            let cleanup = ActiveCleanupGuard::new(Arc::clone(&inner), task_run_id.clone());
            if started.await.is_err() {
                inner.trip_runtime_fatal();
                tracing::error!(
                    run_id = task_run_id,
                    code = "RUN_TASK_START_FAILED",
                    "registered run task start gate closed"
                );
                cleanup.complete();
                return;
            }
            let execution_started = Instant::now();
            let recovery_agent = Arc::clone(&agent);
            let recovery_run = new_run.clone();
            let recovery_state = Arc::clone(&task_state);
            let task_inner = Arc::clone(&inner);
            let body_run_id = task_run_id.clone();
            let outcome = std::panic::AssertUnwindSafe(async move {
                #[cfg(test)]
                if let Some(gate) = panic_active_task {
                    gate.notified().await;
                    panic!("private scheduler panic payload");
                }
                let coordinator = active_task_coordinator(&task_inner, agent);
                let execution_deadline = TokioInstant::now() + task_inner.config.run_timeout;
                let result = if signal.bind_deadline(execution_deadline).is_err() {
                    Err(super::RunError::infrastructure(
                        "RUN_STOP_DEADLINE_INVALID",
                        "Run stop signal was bound to a different execution deadline",
                    ))
                } else {
                    coordinator
                        .execute_existing(new_run, input, signal, task_state, execution_deadline)
                        .await
                };
                if let Err(error) = result {
                    task_inner.close_admission();
                    tracing::error!(
                        run_id = body_run_id,
                        code = error.code(),
                        "run coordinator failed"
                    );
                }
                if !task_inner.events.is_healthy() {
                    task_inner.close_admission();
                }
            })
            .catch_unwind()
            .await;
            if let Err(payload) = outcome {
                drop(payload);
                inner.trip_runtime_fatal();
                tracing::error!(
                    run_id = task_run_id,
                    code = "RUNTIME_TASK_PANICKED",
                    "run task panicked"
                );
                recover_task_panic(
                    Arc::clone(&inner),
                    recovery_agent,
                    recovery_state,
                    recovery_run,
                    execution_started,
                    task_run_id.clone(),
                )
                .await;
            }
            cleanup.complete();
        });
        PendingActiveRun {
            active: ActiveRun {
                attachment,
                stop,
                task,
                _permit: permit,
            },
            start,
        }
    }

    fn spawn_finalizer_active(
        &self,
        preparing: PreparingRun,
        reason: StopReason,
    ) -> PendingActiveRun {
        let PreparingRun {
            agent,
            new_run,
            attachment,
            stop,
            state,
            permit,
            durable,
            #[cfg(test)]
            panic_active_task,
            ..
        } = preparing;
        let inner = Arc::clone(&self.inner);
        let run_id = new_run.run_id.clone();
        let task_run_id = run_id.clone();
        let (start, started) = oneshot::channel();
        let task = tokio::spawn(async move {
            let cleanup = ActiveCleanupGuard::new(Arc::clone(&inner), task_run_id.clone());
            if started.await.is_err() {
                inner.trip_runtime_fatal();
                tracing::error!(
                    run_id = task_run_id,
                    code = "RUN_TASK_START_FAILED",
                    "registered run task start gate closed"
                );
                cleanup.complete();
                return;
            }
            let recovery_agent = Arc::clone(&agent);
            let recovery_run = new_run.clone();
            let recovery_state = Arc::clone(&state);
            let task_inner = Arc::clone(&inner);
            let body_run_id = task_run_id.clone();
            let outcome = std::panic::AssertUnwindSafe(async move {
                #[cfg(test)]
                if let Some(gate) = panic_active_task {
                    gate.notified().await;
                    panic!("private finalizer panic payload");
                }
                if durable {
                    if let Err(error) =
                        publish_prelaunch_terminal(Arc::clone(&task_inner), new_run, reason).await
                    {
                        task_inner.close_admission();
                        tracing::error!(
                            run_id = body_run_id,
                            code = error.code(),
                            "prelaunch finalizer failed"
                        );
                    }
                }
            })
            .catch_unwind()
            .await;
            if let Err(payload) = outcome {
                drop(payload);
                inner.trip_runtime_fatal();
                tracing::error!(
                    run_id = task_run_id,
                    code = "RUNTIME_TASK_PANICKED",
                    "run task panicked"
                );
                if durable {
                    recover_task_panic(
                        Arc::clone(&inner),
                        recovery_agent,
                        recovery_state,
                        recovery_run,
                        Instant::now(),
                        task_run_id.clone(),
                    )
                    .await;
                }
            }
            cleanup.complete();
        });
        PendingActiveRun {
            active: ActiveRun {
                attachment,
                stop,
                task,
                _permit: permit,
            },
            start,
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

fn active_task_coordinator(
    inner: &RunServiceInner,
    agent: Arc<CompiledWorkflow>,
) -> RunCoordinator {
    RunCoordinator::new(
        agent,
        inner.events.clone(),
        Arc::clone(&inner.repository),
        Arc::clone(&inner.operation_capacity),
        super::scope_scheduler::ScopeSchedulerConfig {
            max_concurrent_operations_per_run: inner.config.max_concurrent_operations_per_run,
            operation_timeout: inner.config.operation_timeout,
            operation_cancel_grace_period: inner.config.operation_cancel_grace_period,
            max_template_output_bytes: inner.config.max_template_output_bytes,
        },
    )
}

async fn recover_task_panic(
    inner: Arc<RunServiceInner>,
    agent: Arc<CompiledWorkflow>,
    state: Arc<RunState>,
    run: NewRun,
    started: Instant,
    run_id: String,
) {
    let recovery = std::panic::AssertUnwindSafe(async move {
        let coordinator = active_task_coordinator(&inner, agent);
        coordinator.recover_task_panic(&state, &run, started).await
    })
    .catch_unwind()
    .await;
    match recovery {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => tracing::error!(
            run_id = run_id,
            code = error.code(),
            "panicked run terminal recovery failed"
        ),
        Err(payload) => {
            drop(payload);
            tracing::error!(
                run_id = run_id,
                code = "RUN_TASK_RECOVERY_PANICKED",
                "panicked run terminal recovery panicked"
            );
        }
    }
}

fn service_history_error(error: HistoryError) -> ServiceError {
    match error.code() {
        "HISTORY_OWNERSHIP_LOST" | "HISTORY_STORE_ALREADY_OWNED" => ServiceError::new(
            "RUN_SERVICE_UNAVAILABLE",
            "history store ownership is unavailable",
        ),
        _ => ServiceError::new(error.code(), error.to_string()),
    }
}

fn readiness_unavailable() -> ServiceError {
    ServiceError::new(
        "RUN_SERVICE_UNAVAILABLE",
        "runtime dependencies are unavailable",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use serde_json::json;

    use crate::{
        dsl::vnext::compiler::WorkflowCompiler,
        history::{
            repository::{TerminalProposal, TerminalSequence},
            types::RunLifecycle,
        },
        resources::{actions::ActionRegistry, models::ModelRegistry},
    };

    fn test_agent(id: &str) -> Arc<CompiledWorkflow> {
        let source = format!(
            r#"
api_version: insight.agent/v2
kind: agent
metadata:
  id: {id}
  name: Test Agent
types:
  Empty:
    fields: {{}}
inputs: {{}}
output: Empty
workflow:
  result:
    return: {{}}
"#
        );
        Arc::new(
            WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default())
                .compile_source(std::path::Path::new("."), &source)
                .unwrap(),
        )
    }

    fn test_config() -> RunServiceConfig {
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_concurrent_operations: 1,
            max_concurrent_operations_per_run: 1,
            operation_timeout: Duration::from_secs(1),
            operation_cancel_grace_period: Duration::from_millis(100),
            max_template_output_bytes: 16_384,
            run_timeout: Duration::from_secs(1),
        }
    }

    #[derive(Default)]
    struct UnitRepository {
        runs: tokio::sync::Mutex<BTreeMap<String, RunRecord>>,
        events: tokio::sync::Mutex<Vec<crate::events::protocol::RunEvent>>,
        create_error_code: Option<&'static str>,
        commit_error_code: Option<&'static str>,
    }

    #[async_trait]
    impl RunRepository for UnitRepository {
        async fn check_health(&self) -> Result<(), HistoryError> {
            Ok(())
        }

        async fn create_run(&self, run: NewRun) -> Result<(), HistoryError> {
            if let Some(code) = self.create_error_code {
                return Err(HistoryError::new(code, "private ownership failure"));
            }
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

        async fn commit_terminal(
            &self,
            proposal: TerminalProposal,
            sequence: TerminalSequence,
        ) -> Result<crate::events::protocol::RunEvent, HistoryError> {
            if let Some(code) = self.commit_error_code {
                return Err(HistoryError::new(code, "private terminal failure"));
            }
            let run_id = proposal.run_id().to_string();
            let mut runs = self.runs.lock().await;
            let run = runs
                .get_mut(&run_id)
                .ok_or_else(|| HistoryError::new("RUN_NOT_FOUND", "run not found"))?;
            if run.status().is_terminal() {
                return self
                    .events
                    .lock()
                    .await
                    .iter()
                    .rev()
                    .find(|event| event.run_id == run_id)
                    .cloned()
                    .ok_or_else(|| {
                        HistoryError::new(
                            "HISTORY_TERMINAL_EVENT_MISSING",
                            "terminal run has no terminal event",
                        )
                    });
            }
            let next_seq = self
                .events
                .lock()
                .await
                .iter()
                .rev()
                .find(|event| event.run_id == run_id)
                .map_or(1, |event| event.seq + 1);
            if matches!(sequence, TerminalSequence::Expected(expected) if expected != next_seq) {
                return Err(HistoryError::new(
                    "HISTORY_EVENT_INVALID",
                    "expected terminal sequence does not match durable next sequence",
                ));
            }
            let event = proposal.event_at(next_seq);
            let update = proposal.update();
            run.lifecycle = match &update.terminal {
                RunTerminal::Completed { output } => RunLifecycle::Completed {
                    output: output.clone(),
                },
                RunTerminal::Failed { error } => RunLifecycle::Failed {
                    error: error.clone(),
                },
                RunTerminal::Cancelled { error } => RunLifecycle::Cancelled {
                    error: error.clone(),
                },
                RunTerminal::Interrupted { error } => RunLifecycle::Interrupted {
                    error: error.clone(),
                },
            };
            run.ended_at = Some(update.ended_at);
            run.updated_at = update.ended_at;
            drop(runs);
            self.events.lock().await.push(event.clone());
            Ok(event)
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

    #[test]
    fn ownership_history_errors_map_to_service_unavailable_without_changing_other_errors() {
        for code in ["HISTORY_OWNERSHIP_LOST", "HISTORY_STORE_ALREADY_OWNED"] {
            let error = service_history_error(HistoryError::new(code, "private ownership detail"));
            assert_eq!(error.code(), "RUN_SERVICE_UNAVAILABLE");
            assert_eq!(error.to_string(), "history store ownership is unavailable");
            assert!(!error.to_string().contains("private ownership detail"));
        }

        let error = service_history_error(HistoryError::new(
            "HISTORY_WRITE_FAILED",
            "existing history detail",
        ));
        assert_eq!(error.code(), "HISTORY_WRITE_FAILED");
        assert_eq!(error.to_string(), "existing history detail");
    }

    #[tokio::test]
    async fn create_run_ownership_loss_closes_admission_and_cleans_preparing_state() {
        let repository = Arc::new(UnitRepository {
            create_error_code: Some("HISTORY_OWNERSHIP_LOST"),
            ..UnitRepository::default()
        });
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
        let agent = test_agent("ownership_agent");
        let service = RunService::new(
            AgentCatalog::new(vec![agent]).unwrap(),
            repository_trait,
            events,
            test_config(),
        )
        .unwrap();

        let error = service
            .create_detached("ownership_agent", json!({}), RequestMetadata::default())
            .await
            .unwrap_err();

        assert_eq!(error.code(), "RUN_SERVICE_UNAVAILABLE");
        assert!(!service.is_healthy());
        assert!(lock_preparing(&service.inner).is_empty());
        assert!(lock_active(&service.inner).is_empty());
        assert!(repository.runs.lock().await.is_empty());

        let rejected = service
            .create_detached("ownership_agent", json!({}), RequestMetadata::default())
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), "RUN_SERVICE_STOPPING");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admission_closure_linearizes_with_preparing_registration() {
        let repository: Arc<dyn RunRepository> = Arc::new(UnitRepository::default());
        let events = EventHub::new(
            Arc::clone(&repository),
            crate::events::hub::EventHubConfig {
                subscriber_capacity: 8,
                journal_capacity: 32,
                journal_batch_size: 8,
                operation_timeout: Duration::from_secs(1),
            },
        );
        let service = RunService::new(
            AgentCatalog::new(Vec::new()).unwrap(),
            repository,
            events,
            test_config(),
        )
        .unwrap();

        let preparing = lock_preparing(&service.inner);
        let contender = service.clone();
        let (started_sender, started_receiver) = std::sync::mpsc::channel();
        let (done_sender, done_receiver) = std::sync::mpsc::channel();
        let shutdown = std::thread::spawn(move || {
            started_sender.send(()).unwrap();
            contender.inner.close_admission();
            done_sender.send(()).unwrap();
        });
        started_receiver.recv().unwrap();
        assert!(
            done_receiver
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "shutdown must wait for the preparing admission boundary"
        );

        drop(preparing);
        done_receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        shutdown.join().unwrap();
        assert!(!service.is_healthy());
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
            AgentCatalog::new(Vec::new()).unwrap(),
            Arc::clone(&repository_trait),
            events,
            test_config(),
        )
        .unwrap();
        let permit = Arc::clone(&service.inner.capacity)
            .try_acquire_owned()
            .unwrap();
        let agent = test_agent("agent");
        let new_run = NewRun {
            run_id: "run_unit_attached".to_string(),
            request_id: "req_unit_attached".to_string(),
            agent_id: "agent".to_string(),
            agent_version: agent.version_hash.clone(),
            attachment: RunAttachment::Attached,
            created_at: Utc::now(),
            input_summary: json!({"keys":[], "serialized_bytes":2}),
        };
        repository.create_run(new_run.clone()).await.unwrap();
        let (stop, signal) = stop_pair();
        lock_preparing(&service.inner).insert(
            new_run.run_id.clone(),
            PreparingRun {
                agent,
                new_run: new_run.clone(),
                input: json!({}),
                attachment: RunAttachment::Attached,
                stop,
                signal,
                state: Arc::new(RunState::new()),
                permit,
                durable: true,
                panic_active_task: None,
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

    fn panic_test_agent() -> Arc<CompiledWorkflow> {
        test_agent("panic_agent")
    }

    fn panic_test_service(repository: Arc<UnitRepository>) -> RunService {
        let repository_trait: Arc<dyn RunRepository> = repository;
        let events = EventHub::new(
            Arc::clone(&repository_trait),
            crate::events::hub::EventHubConfig {
                subscriber_capacity: 8,
                journal_capacity: 32,
                journal_batch_size: 8,
                operation_timeout: Duration::from_millis(100),
            },
        );
        RunService::new(
            AgentCatalog::new(vec![panic_test_agent()]).unwrap(),
            repository_trait,
            events,
            test_config(),
        )
        .unwrap()
    }

    async fn prepare_gated_panicking_run(
        service: &RunService,
    ) -> (PreparedRun, Arc<tokio::sync::Notify>, StopSignal) {
        let prepared = service
            .prepare_run(
                "panic_agent",
                json!({}),
                RequestMetadata::default(),
                RunAttachment::Detached,
            )
            .await
            .unwrap();
        let gate = Arc::new(tokio::sync::Notify::new());
        let signal = {
            let mut preparing = lock_preparing(&service.inner);
            let run = preparing.get_mut(&prepared.run_id).unwrap();
            run.panic_active_task = Some(Arc::clone(&gate));
            run.signal.clone()
        };
        (prepared, gate, signal)
    }

    async fn prepare_panicking_run(service: &RunService) -> PreparedRun {
        let (prepared, gate, _) = prepare_gated_panicking_run(service).await;
        gate.notify_one();
        prepared
    }

    async fn observe_runtime_fatal(receiver: &mut watch::Receiver<bool>) {
        timeout(Duration::from_secs(1), async {
            loop {
                if *receiver.borrow() {
                    return;
                }
                receiver.changed().await.unwrap();
            }
        })
        .await
        .expect("runtime fatal notification timed out");
    }

    fn assert_run_ownership_cleaned(service: &RunService) {
        assert!(lock_preparing(&service.inner).is_empty());
        assert!(lock_active(&service.inner).is_empty());
        assert_eq!(service.inner.capacity.available_permits(), 1);
    }

    async fn assert_infrastructure_failed(
        repository: &UnitRepository,
        run_id: &str,
        private_payload: &str,
    ) {
        let record = repository.get_run(run_id).await.unwrap().unwrap();
        assert!(matches!(
            record.lifecycle,
            RunLifecycle::Failed { ref error }
                if error.kind == FailureKind::Infrastructure
                    && error.code == "INFRASTRUCTURE_FAILURE"
                    && error.message == "runtime infrastructure failed"
        ));
        assert!(!serde_json::to_string(&record)
            .unwrap()
            .contains(private_payload));

        let events = repository.events.lock().await;
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event.event_type, RunEventType::RunFailed))
                .count(),
            1
        );
        assert!(!serde_json::to_string(&*events)
            .unwrap()
            .contains(private_payload));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scheduler_outer_panic_trips_fatal_recovers_terminal_and_releases_ownership() {
        const PRIVATE_PAYLOAD: &str = "private scheduler panic payload";

        let repository = Arc::new(UnitRepository::default());
        let service = panic_test_service(Arc::clone(&repository));
        let mut fatal = service.subscribe_fatal();
        let (prepared, panic_gate, stop_signal) = prepare_gated_panicking_run(&service).await;
        let run_id = prepared.run_id.clone();
        service.launch(prepared);

        let cancel_service = service.clone();
        let cancel_run_id = run_id.clone();
        let cancel = tokio::spawn(async move { cancel_service.cancel(&cancel_run_id).await });

        timeout(Duration::from_secs(1), async {
            while stop_signal.reason() != Some(StopReason::Cancelled) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancel waiter did not subscribe and request stop");
        panic_gate.notify_one();

        observe_runtime_fatal(&mut fatal).await;
        assert!(service.is_fatal());
        assert!(*service.subscribe_fatal().borrow());
        assert!(!service.is_healthy());
        assert_eq!(
            service
                .check_readiness(Duration::from_millis(20))
                .await
                .unwrap_err()
                .code(),
            "RUN_SERVICE_UNAVAILABLE"
        );
        assert_eq!(
            service
                .create_detached("panic_agent", json!({}), RequestMetadata::default(),)
                .await
                .unwrap_err()
                .code(),
            "RUN_SERVICE_STOPPING"
        );

        let cancelled = timeout(Duration::from_secs(1), cancel)
            .await
            .expect("cancel waiter timed out")
            .unwrap()
            .unwrap();
        assert_eq!(cancelled.status(), RunStatus::Failed);
        timeout(
            Duration::from_secs(1),
            service.shutdown(Duration::from_millis(500)),
        )
        .await
        .expect("shutdown waiter timed out")
        .unwrap();

        assert_run_ownership_cleaned(&service);
        assert_infrastructure_failed(&repository, &run_id, PRIVATE_PAYLOAD).await;
        assert_eq!(repository.runs.lock().await.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prelaunch_finalizer_outer_panic_uses_same_fatal_cleanup_and_recovery() {
        const PRIVATE_PAYLOAD: &str = "private finalizer panic payload";

        let repository = Arc::new(UnitRepository::default());
        let service = panic_test_service(Arc::clone(&repository));
        let mut fatal = service.subscribe_fatal();
        let prepared = prepare_panicking_run(&service).await;
        let run_id = prepared.run_id.clone();
        service.begin_shutdown();
        service.launch(prepared);

        observe_runtime_fatal(&mut fatal).await;
        timeout(
            Duration::from_secs(1),
            service.shutdown(Duration::from_millis(500)),
        )
        .await
        .expect("shutdown waiter timed out")
        .unwrap();

        assert!(service.is_fatal());
        assert!(!service.is_healthy());
        assert_run_ownership_cleaned(&service);
        assert_infrastructure_failed(&repository, &run_id, PRIVATE_PAYLOAD).await;
        assert!(repository
            .events
            .lock()
            .await
            .iter()
            .all(|event| event.event_type != RunEventType::RunStarted));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn panic_recovery_failure_still_releases_local_ownership_and_shutdown_waiters() {
        let repository = Arc::new(UnitRepository {
            commit_error_code: Some("HISTORY_WRITE_FAILED"),
            ..UnitRepository::default()
        });
        let service = panic_test_service(Arc::clone(&repository));
        let mut fatal = service.subscribe_fatal();
        let prepared = prepare_panicking_run(&service).await;
        let run_id = prepared.run_id.clone();
        service.launch(prepared);

        observe_runtime_fatal(&mut fatal).await;
        timeout(
            Duration::from_secs(1),
            service.shutdown(Duration::from_millis(500)),
        )
        .await
        .expect("shutdown waiter timed out")
        .unwrap();

        assert!(service.is_fatal());
        assert_run_ownership_cleaned(&service);
        assert_eq!(
            repository.get_run(&run_id).await.unwrap().unwrap().status(),
            RunStatus::Created
        );
        assert!(repository.events.lock().await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_task_start_gate_blocks_work_until_registration() {
        let repository = Arc::new(UnitRepository::default());
        let service = panic_test_service(Arc::clone(&repository));
        let prepared = prepare_panicking_run(&service).await;
        let run_id = prepared.run_id.clone();
        let preparing = lock_preparing(&service.inner).remove(&run_id).unwrap();
        prepared.guard.disarm();

        let pending = service.spawn_scheduler_active(preparing);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(!service.is_fatal());
        assert!(lock_active(&service.inner).is_empty());
        assert_eq!(service.inner.capacity.available_permits(), 0);

        lock_active(&service.inner).insert(run_id.clone(), pending.active);
        service.inner.notify_lifecycle();
        pending.start.send(()).unwrap();
        let mut fatal = service.subscribe_fatal();
        observe_runtime_fatal(&mut fatal).await;
        timeout(
            Duration::from_secs(1),
            service.shutdown(Duration::from_millis(500)),
        )
        .await
        .expect("shutdown waiter timed out")
        .unwrap();
        assert_run_ownership_cleaned(&service);
    }
}
