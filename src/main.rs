use std::{
    error::Error,
    future::{pending, Future, IntoFuture},
    io,
    sync::Arc,
    time::Duration,
};

use insight_agent_platform::{
    api::formal::{build_router, ApiAuth, FormalApiState},
    catalog::compile_enabled_agents,
    config::{HistoryConfig, PlatformConfig},
    dsl::vnext::compiler::WorkflowCompiler,
    events::hub::{EventHub, EventHubConfig},
    history::{
        postgres::{PostgresRunRepository, PostgresStoreOwner},
        repository::{HistoryError, RunRepository},
        sqlite::SqliteRunRepository,
    },
    resources::{
        builtin_actions::{builtin_action_registry, RestrictedHttpGetAction},
        config::load_model_registry,
    },
    runtime::{RunService, RunServiceConfig},
};

type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const PROCESS_PANICKED_CODE: &str = "PROCESS_PANICKED";
const PROCESS_PANICKED_MESSAGE: &str = "process panic captured";
const RUNTIME_TASK_PANICKED_CODE: &str = "RUNTIME_TASK_PANICKED";

struct InitializedRepository {
    repository: Arc<dyn RunRepository>,
    owner: Option<PostgresStoreOwner>,
}

enum ShutdownTrigger {
    HttpStopped(io::Result<()>),
    Signal,
    SignalFailed,
    OwnershipLost,
    RuntimeFatal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownOutcome {
    OwnershipLost,
    RuntimeFatal,
    HttpStopped,
    SignalFailed,
    Clean,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ShutdownDecision {
    outcome: ShutdownOutcome,
    release_store_owner: bool,
}

#[tokio::main]
async fn main() -> MainResult<()> {
    let _ = dotenvy::dotenv();
    init_tracing();
    install_sanitized_panic_hook();

    let config = PlatformConfig::from_env()?;
    let models = load_model_registry(&config.models.config)?;
    let http_get = config
        .actions
        .http_get
        .as_ref()
        .map(|http| {
            RestrictedHttpGetAction::new(
                http.timeout,
                http.max_bytes,
                http.allowlist.iter().cloned().collect(),
            )
        })
        .transpose()?;
    let actions = builtin_action_registry(
        &config.actions.enabled.iter().cloned().collect::<Vec<_>>(),
        http_get,
    )?;
    let compiler = WorkflowCompiler::new(models, actions);
    let agents =
        compile_enabled_agents(&config.agents.directory, &config.agents.enabled, &compiler)?;
    let InitializedRepository {
        repository,
        mut owner,
    } = initialize_repository(
        &config.history,
        config.runtime.journal_operation_timeout,
        config.runtime.readiness_probe_timeout,
    )
    .await?;
    let mut ownership_loss = owner.as_ref().map(PostgresStoreOwner::subscribe_loss);
    let events = EventHub::new(
        Arc::clone(&repository),
        EventHubConfig {
            subscriber_capacity: config.runtime.subscriber_capacity,
            journal_capacity: config.runtime.journal_capacity,
            journal_batch_size: config.runtime.journal_batch_size,
            operation_timeout: config.runtime.journal_operation_timeout,
        },
    );
    let service = RunService::new(
        agents,
        repository,
        events,
        RunServiceConfig {
            max_concurrent_runs: config.runtime.max_concurrent_runs,
            max_concurrent_operations: config.runtime.max_concurrent_operations,
            max_concurrent_operations_per_run: config.runtime.max_concurrent_operations_per_run,
            operation_timeout: config.runtime.operation_timeout,
            operation_cancel_grace_period: config.runtime.operation_cancel_grace_period,
            max_template_output_bytes: config.runtime.max_template_output_bytes,
            run_timeout: config.runtime.run_timeout,
        },
    )?;
    let mut runtime_fatal = service.subscribe_fatal();
    let interrupted = service.reconcile_startup().await?;
    if interrupted > 0 {
        tracing::warn!(
            interrupted,
            "reconciled incomplete runs from a previous process"
        );
    }
    ensure_store_ownership(owner.as_ref())?;

    let app = build_router(FormalApiState {
        service: service.clone(),
        auth: ApiAuth::from(&config.auth),
        sse_keep_alive_interval: config.runtime.sse_keep_alive_interval,
        readiness_probe_timeout: config.runtime.readiness_probe_timeout,
    });
    let bind = tokio::net::TcpListener::bind(config.bind_addr);
    tokio::pin!(bind);
    let listener = tokio::select! {
        biased;
        _ = wait_for_ownership_loss(&mut ownership_loss) => {
            return Err(ownership_lost_error().into());
        }
        result = &mut bind => result?,
    };
    ensure_store_ownership(owner.as_ref())?;
    tracing::info!(
        bind_addr = %config.bind_addr,
        agents = service.agents().list().count(),
        "v2 runtime listening"
    );
    let (http_shutdown, wait_http_shutdown) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = wait_http_shutdown.await;
        })
        .into_future();
    supervise_runtime(
        &service,
        &mut owner,
        &mut ownership_loss,
        &mut runtime_fatal,
        server,
        http_shutdown,
        wait_for_shutdown_signal(),
        config.runtime.shutdown_grace_period,
        config.runtime.shutdown_hard_deadline,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn supervise_runtime<Server, ShutdownSignal>(
    service: &RunService,
    owner: &mut Option<PostgresStoreOwner>,
    ownership_loss: &mut Option<tokio::sync::watch::Receiver<bool>>,
    runtime_fatal: &mut tokio::sync::watch::Receiver<bool>,
    server: Server,
    http_shutdown: tokio::sync::oneshot::Sender<()>,
    shutdown_signal: ShutdownSignal,
    shutdown_grace_period: Duration,
    shutdown_hard_deadline: Duration,
) -> MainResult<()>
where
    Server: Future<Output = io::Result<()>>,
    ShutdownSignal: Future<Output = io::Result<()>>,
{
    tokio::pin!(server);
    tokio::pin!(shutdown_signal);

    let trigger = tokio::select! {
        biased;
        _ = wait_for_ownership_loss(ownership_loss) => {
            tracing::error!(
                code = "HISTORY_OWNERSHIP_LOST",
                "PostgreSQL history store ownership was lost"
            );
            ShutdownTrigger::OwnershipLost
        }
        _ = wait_for_runtime_fatal(runtime_fatal) => {
            tracing::error!(
                code = RUNTIME_TASK_PANICKED_CODE,
                "a run task panicked; forcing process shutdown"
            );
            ShutdownTrigger::RuntimeFatal
        }
        result = &mut server => {
            tracing::warn!("HTTP server stopped before a shutdown signal");
            ShutdownTrigger::HttpStopped(result)
        }
        result = &mut shutdown_signal => match result {
            Ok(()) => {
                tracing::info!("shutdown signal received");
                ShutdownTrigger::Signal
            }
            Err(error) => {
                tracing::error!(%error, "shutdown signal handler failed");
                ShutdownTrigger::SignalFailed
            }
        }
    };
    service.begin_shutdown();

    let drain = async {
        let (
            ownership_loss_triggered,
            runtime_fatal_triggered,
            signal_failed_triggered,
            mut http_stopped_early,
            mut http,
        ) = match trigger {
            ShutdownTrigger::OwnershipLost => (true, false, false, false, None),
            ShutdownTrigger::RuntimeFatal => (false, true, false, false, None),
            ShutdownTrigger::HttpStopped(result) => (false, false, false, true, Some(result)),
            ShutdownTrigger::Signal => (false, false, false, false, None),
            ShutdownTrigger::SignalFailed => (false, false, true, false, None),
        };

        let runtime = if http.is_some() {
            service.shutdown(shutdown_grace_period).await
        } else {
            let runtime = service.shutdown(shutdown_grace_period);
            tokio::pin!(runtime);
            tokio::select! {
                biased;
                result = &mut server => {
                    tracing::warn!("HTTP server stopped while the runtime was draining");
                    http_stopped_early = true;
                    http = Some(result);
                    (&mut runtime).await
                }
                result = &mut runtime => {
                    if http_shutdown.send(()).is_err() {
                        tracing::error!("HTTP graceful-shutdown channel closed unexpectedly");
                        http_stopped_early = true;
                    }
                    result
                }
            }
        };

        let http = match http {
            Some(result) => result,
            None => (&mut server).await,
        };
        let lost_during_drain = owner.as_ref().is_some_and(PostgresStoreOwner::is_lost);
        let fatal_during_drain = service.is_fatal();
        let decision = shutdown_decision(
            ownership_loss_triggered || lost_during_drain,
            runtime_fatal_triggered || fatal_during_drain,
            http_stopped_early,
            signal_failed_triggered,
        );
        let release = if decision.release_store_owner {
            release_store_owner(owner).await
        } else {
            Ok(())
        };
        let decision = merge_release_ownership_loss(decision, &release);

        match decision.outcome {
            ShutdownOutcome::OwnershipLost => {
                debug_assert!(!decision.release_store_owner);
                log_shutdown_failures(&runtime, &http, &release, "ownership loss");
                Err(ownership_lost_error().into())
            }
            ShutdownOutcome::RuntimeFatal => {
                log_shutdown_failures(&runtime, &http, &release, "a run task panic");
                Err(runtime_fatal_error().into())
            }
            ShutdownOutcome::HttpStopped => {
                log_shutdown_failures(&runtime, &http, &release, "an unexpected HTTP stop");
                Err(http_stopped_error().into())
            }
            ShutdownOutcome::SignalFailed => {
                log_shutdown_failures(&runtime, &http, &release, "a signal handler failure");
                Err(signal_failed_error().into())
            }
            ShutdownOutcome::Clean => {
                runtime?;
                http?;
                release?;
                Ok(())
            }
        }
    };

    tokio::time::timeout(shutdown_hard_deadline, drain)
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "server shutdown exceeded its hard deadline",
            )
        })?
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

fn install_sanitized_panic_hook() {
    std::panic::set_hook(Box::new(|_| {
        let (code, message) = sanitized_panic_event();
        tracing::error!(code, "{message}");
    }));
}

fn sanitized_panic_event() -> (&'static str, &'static str) {
    (PROCESS_PANICKED_CODE, PROCESS_PANICKED_MESSAGE)
}

async fn initialize_repository(
    config: &HistoryConfig,
    operation_timeout: Duration,
    probe_timeout: Duration,
) -> MainResult<InitializedRepository> {
    match config {
        HistoryConfig::Sqlite { path } => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            Ok(InitializedRepository {
                repository: Arc::new(SqliteRunRepository::connect_path(path).await?),
                owner: None,
            })
        }
        HistoryConfig::Postgres { database_url } => {
            let (repository, owner) = PostgresRunRepository::connect_owned(
                database_url.expose(),
                operation_timeout,
                probe_timeout,
            )
            .await?;
            Ok(InitializedRepository {
                repository: Arc::new(repository),
                owner: Some(owner),
            })
        }
    }
}

fn ensure_store_ownership(owner: Option<&PostgresStoreOwner>) -> Result<(), HistoryError> {
    if owner.is_some_and(PostgresStoreOwner::is_lost) {
        Err(ownership_lost_error())
    } else {
        Ok(())
    }
}

fn ownership_lost_error() -> HistoryError {
    HistoryError::new(
        "HISTORY_OWNERSHIP_LOST",
        "PostgreSQL history store ownership was lost",
    )
}

fn runtime_fatal_error() -> std::io::Error {
    std::io::Error::other("a run task panicked; the process was shut down")
}

fn http_stopped_error() -> io::Error {
    io::Error::other("HTTP server stopped before a graceful shutdown command")
}

fn signal_failed_error() -> io::Error {
    io::Error::other("shutdown signal handler failed")
}

fn log_shutdown_failures(
    runtime: &Result<(), insight_agent_platform::runtime::ServiceError>,
    http: &io::Result<()>,
    release: &Result<(), HistoryError>,
    context: &str,
) {
    if let Err(error) = runtime {
        tracing::error!(code = error.code(), context, "runtime drain failed");
    }
    if let Err(error) = http {
        tracing::error!(%error, context, "HTTP drain failed");
    }
    if let Err(error) = release {
        tracing::error!(
            code = error.code(),
            context,
            "history-store owner release failed"
        );
    }
}

fn shutdown_decision(
    ownership_lost: bool,
    runtime_fatal: bool,
    http_stopped: bool,
    signal_failed: bool,
) -> ShutdownDecision {
    let outcome = if ownership_lost {
        ShutdownOutcome::OwnershipLost
    } else if runtime_fatal {
        ShutdownOutcome::RuntimeFatal
    } else if http_stopped {
        ShutdownOutcome::HttpStopped
    } else if signal_failed {
        ShutdownOutcome::SignalFailed
    } else {
        ShutdownOutcome::Clean
    };
    ShutdownDecision {
        outcome,
        release_store_owner: outcome != ShutdownOutcome::OwnershipLost,
    }
}

fn merge_release_ownership_loss(
    decision: ShutdownDecision,
    release: &Result<(), HistoryError>,
) -> ShutdownDecision {
    match release {
        Err(error) if error.code() == "HISTORY_OWNERSHIP_LOST" => ShutdownDecision {
            outcome: ShutdownOutcome::OwnershipLost,
            release_store_owner: false,
        },
        Ok(()) | Err(_) => decision,
    }
}

async fn release_store_owner(owner: &mut Option<PostgresStoreOwner>) -> Result<(), HistoryError> {
    match owner.take() {
        Some(owner) => owner.release().await,
        None => Ok(()),
    }
}

async fn wait_for_ownership_loss(receiver: &mut Option<tokio::sync::watch::Receiver<bool>>) {
    let Some(receiver) = receiver else {
        pending::<()>().await;
        return;
    };
    loop {
        if *receiver.borrow() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

async fn wait_for_runtime_fatal(receiver: &mut tokio::sync::watch::Receiver<bool>) {
    loop {
        if *receiver.borrow() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        received = terminate.recv() => received
            .map(|_| ())
            .ok_or_else(|| io::Error::other("SIGTERM signal stream closed")),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use insight_agent_platform::{
        catalog::AgentCatalog,
        dsl::vnext::compiler::WorkflowCompiler,
        events::protocol::{RunEvent, RunEventType},
        history::types::{RunAttachment, RunLifecycle, RunRecord, RunStatus},
        resources::{
            actions::{
                Action, ActionContext, ActionDescriptor, ActionRegistry, CancellationClass,
                EffectClass, IdempotencyClass,
            },
            models::ModelRegistry,
        },
        runtime::{AttachedRun, RequestMetadata, RunError},
    };
    use serde_json::{json, Value};
    use tempfile::tempdir;
    use tokio::sync::{Notify, Semaphore};

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);
    const TEST_HARD_DEADLINE: Duration = Duration::from_secs(5);

    #[derive(Default)]
    struct BlockingProgress {
        started: AtomicUsize,
        started_changed: Notify,
        stopped: AtomicUsize,
        stopped_changed: Notify,
        release_after_stop: Option<Arc<Semaphore>>,
    }

    impl BlockingProgress {
        fn held_after_stop() -> Self {
            Self {
                release_after_stop: Some(Arc::new(Semaphore::new(0))),
                ..Self::default()
            }
        }

        async fn wait_started(&self, expected: usize) {
            wait_for_count(&self.started, &self.started_changed, expected, "started").await;
        }

        async fn wait_stopped(&self, expected: usize) {
            wait_for_count(&self.stopped, &self.stopped_changed, expected, "stopped").await;
        }

        fn release_stopped(&self, permits: usize) {
            self.release_after_stop
                .as_ref()
                .expect("test executor is not held after stop")
                .add_permits(permits);
        }
    }

    async fn wait_for_count(count: &AtomicUsize, changed: &Notify, expected: usize, label: &str) {
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                let notified = changed.notified();
                if count.load(Ordering::SeqCst) >= expected {
                    return;
                }
                notified.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("test executors did not reach {label} count {expected}"));
    }

    #[derive(Clone)]
    struct BlockingTestAction {
        progress: Arc<BlockingProgress>,
    }

    #[async_trait]
    impl Action for BlockingTestAction {
        fn descriptor(&self) -> ActionDescriptor {
            ActionDescriptor {
                id: "test.lifecycle_block",
                version: "1.0.0",
                input_schema: json!({
                    "type":"object",
                    "properties":{},
                    "additionalProperties":false
                }),
                output_schema: json!({"type":"null"}),
                effect: EffectClass::Pure,
                idempotency: IdempotencyClass::Idempotent,
                cancellation: CancellationClass::Cooperative,
                required_capabilities: BTreeSet::new(),
            }
        }

        async fn call(&self, input: Value, context: ActionContext) -> Result<Value, RunError> {
            assert_eq!(input, json!({}));
            self.progress.started.fetch_add(1, Ordering::SeqCst);
            self.progress.started_changed.notify_waiters();
            context.control.stopped().await;
            self.progress.stopped.fetch_add(1, Ordering::SeqCst);
            self.progress.stopped_changed.notify_waiters();
            if let Some(release) = &self.progress.release_after_stop {
                release
                    .acquire()
                    .await
                    .expect("release semaphore closed")
                    .forget();
            }
            Err(RunError::stopped(
                context
                    .control
                    .stop_reason()
                    .expect("stop notification omitted reason"),
            ))
        }
    }

    struct LifecycleTestRuntime {
        service: RunService,
        repository: Arc<SqliteRunRepository>,
        progress: Arc<BlockingProgress>,
    }

    async fn lifecycle_test_runtime(hold_after_stop: bool) -> LifecycleTestRuntime {
        let repository = Arc::new(SqliteRunRepository::in_memory().await.unwrap());
        let repository_trait: Arc<dyn RunRepository> = repository.clone();
        let events = EventHub::new(
            Arc::clone(&repository_trait),
            EventHubConfig {
                subscriber_capacity: 8,
                journal_capacity: 32,
                journal_batch_size: 8,
                operation_timeout: TEST_TIMEOUT,
            },
        );
        let progress = Arc::new(if hold_after_stop {
            BlockingProgress::held_after_stop()
        } else {
            BlockingProgress::default()
        });
        let mut actions = ActionRegistry::default();
        actions
            .register(BlockingTestAction {
                progress: Arc::clone(&progress),
            })
            .unwrap();
        let source = r#"
api_version: insight.agent/v2
kind: agent
metadata:
  id: lifecycle_test
  name: Lifecycle Test
schema_dialect: https://json-schema.org/draft/2020-12/schema
input:
  schema:
    type: object
    additionalProperties: false
output:
  data_schema: {type: "null"}
workflow:
  steps:
    - kind: action
      id: block
      call: test.lifecycle_block
  result:
    return:
      data: {literal: null}
"#;
        let root = tempdir().unwrap();
        let workflow = WorkflowCompiler::new(ModelRegistry::default(), actions)
            .compile_source(root.path(), source)
            .unwrap();
        let service = RunService::new(
            AgentCatalog::new(vec![Arc::new(workflow)]).unwrap(),
            repository_trait,
            events,
            RunServiceConfig {
                max_concurrent_runs: 2,
                max_concurrent_operations: 2,
                max_concurrent_operations_per_run: 2,
                operation_timeout: Duration::from_secs(60),
                operation_cancel_grace_period: Duration::from_secs(1),
                max_template_output_bytes: 1024,
                run_timeout: Duration::from_secs(60),
            },
        )
        .unwrap();
        LifecycleTestRuntime {
            service,
            repository,
            progress,
        }
    }

    async fn start_attached_and_detached(
        runtime: &LifecycleTestRuntime,
    ) -> (AttachedRun, RunRecord) {
        let detached = runtime
            .service
            .create_detached("lifecycle_test", json!({}), RequestMetadata::default())
            .await
            .unwrap();
        let attached = runtime
            .service
            .create_attached("lifecycle_test", json!({}), RequestMetadata::default())
            .await
            .unwrap();
        runtime.progress.wait_started(2).await;
        wait_for_run_status(&runtime.service, &detached.run_id, RunStatus::Running).await;
        wait_for_run_status(&runtime.service, &attached.run_id, RunStatus::Running).await;
        (attached, detached)
    }

    async fn wait_for_run_status(service: &RunService, run_id: &str, expected: RunStatus) {
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                if service.get_run(run_id).await.unwrap().status() == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("run {run_id} did not reach {expected:?}"));
    }

    async fn assert_terminal(
        repository: &SqliteRunRepository,
        run_id: &str,
        expected_attachment: RunAttachment,
        expected_status: RunStatus,
        expected_code: &str,
        expected_event: RunEventType,
    ) {
        let record = repository.get_run(run_id).await.unwrap().unwrap();
        assert_eq!(record.attachment, expected_attachment);
        assert_eq!(record.status(), expected_status);
        let code = match record.lifecycle {
            RunLifecycle::Cancelled { error } | RunLifecycle::Interrupted { error } => error.code,
            lifecycle => panic!("expected stop terminal, got {lifecycle:?}"),
        };
        assert_eq!(code, expected_code);
        let events = repository.list_events_after(run_id, 0, 64).await.unwrap();
        let terminal = events.iter().filter(is_terminal_event).collect::<Vec<_>>();
        assert_eq!(terminal.len(), 1, "unexpected events: {events:?}");
        assert_eq!(terminal[0].event_type, expected_event);
        assert_eq!(terminal[0].code, expected_code);
        assert_eq!(terminal[0].data, json!({}));
        assert_eq!(
            events.last().map(|event| event.event_type),
            Some(expected_event)
        );
    }

    fn is_terminal_event(event: &&RunEvent) -> bool {
        matches!(
            event.event_type,
            RunEventType::RunCompleted
                | RunEventType::RunFailed
                | RunEventType::RunCancelled
                | RunEventType::RunInterrupted
        )
    }

    #[tokio::test]
    async fn ownership_loss_waiter_observes_sticky_loss_and_channel_failure() {
        let (_sender, receiver) = tokio::sync::watch::channel(true);
        let mut loss = Some(receiver);
        wait_for_ownership_loss(&mut loss).await;

        let (sender, receiver) = tokio::sync::watch::channel(false);
        let mut closed = Some(receiver);
        drop(sender);
        wait_for_ownership_loss(&mut closed).await;
    }

    #[tokio::test]
    async fn ownership_loss_waiter_stays_pending_without_a_postgres_owner() {
        let mut absent = None;
        assert!(tokio::time::timeout(
            Duration::from_millis(10),
            wait_for_ownership_loss(&mut absent),
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn runtime_fatal_waiter_observes_sticky_fatal_and_channel_failure() {
        let (_sender, mut fatal) = tokio::sync::watch::channel(true);
        wait_for_runtime_fatal(&mut fatal).await;

        let (sender, mut closed) = tokio::sync::watch::channel(false);
        drop(sender);
        wait_for_runtime_fatal(&mut closed).await;
    }

    #[tokio::test]
    async fn finite_http_error_drains_real_runs_to_exact_attachment_terminals() {
        let runtime = lifecycle_test_runtime(false).await;
        let (attached, detached) = start_attached_and_detached(&runtime).await;
        let attached_id = attached.run_id.clone();
        let detached_id = detached.run_id.clone();
        let mut owner = None;
        let mut ownership_loss = None;
        let mut runtime_fatal = runtime.service.subscribe_fatal();
        let (http_shutdown, graceful_command) = tokio::sync::oneshot::channel();

        let error = supervise_runtime(
            &runtime.service,
            &mut owner,
            &mut ownership_loss,
            &mut runtime_fatal,
            std::future::ready(Err(io::Error::other("private HTTP failure detail"))),
            http_shutdown,
            pending::<io::Result<()>>(),
            TEST_TIMEOUT,
            TEST_HARD_DEADLINE,
        )
        .await
        .unwrap_err();

        assert_eq!(error.to_string(), http_stopped_error().to_string());
        assert!(
            graceful_command.await.is_err(),
            "an already-stopped HTTP server received a graceful command"
        );
        assert_terminal(
            &runtime.repository,
            &attached_id,
            RunAttachment::Attached,
            RunStatus::Cancelled,
            "RUN_CANCELLED",
            RunEventType::RunCancelled,
        )
        .await;
        assert_terminal(
            &runtime.repository,
            &detached_id,
            RunAttachment::Detached,
            RunStatus::Interrupted,
            "RUN_INTERRUPTED",
            RunEventType::RunInterrupted,
        )
        .await;
        drop(attached);
    }

    #[tokio::test]
    async fn http_completion_during_runtime_drain_is_latched_before_graceful_command() {
        let runtime = lifecycle_test_runtime(true).await;
        let (attached, detached) = start_attached_and_detached(&runtime).await;
        let attached_id = attached.run_id.clone();
        let detached_id = detached.run_id.clone();
        let (complete_http, wait_http_completion) =
            tokio::sync::oneshot::channel::<io::Result<()>>();
        let (http_observed, wait_http_observed) = tokio::sync::oneshot::channel();
        let server = async move {
            let result = wait_http_completion
                .await
                .expect("HTTP completion sender dropped");
            let _ = http_observed.send(());
            result
        };
        let (http_shutdown, graceful_command) = tokio::sync::oneshot::channel();
        let supervised_service = runtime.service.clone();
        let supervisor = tokio::spawn(async move {
            let mut owner = None;
            let mut ownership_loss = None;
            let mut runtime_fatal = supervised_service.subscribe_fatal();
            supervise_runtime(
                &supervised_service,
                &mut owner,
                &mut ownership_loss,
                &mut runtime_fatal,
                server,
                http_shutdown,
                std::future::ready(Ok(())),
                TEST_TIMEOUT,
                TEST_HARD_DEADLINE,
            )
            .await
        });

        runtime.progress.wait_stopped(2).await;
        complete_http.send(Ok(())).unwrap();
        tokio::time::timeout(TEST_TIMEOUT, wait_http_observed)
            .await
            .expect("supervisor did not poll HTTP during runtime drain")
            .expect("HTTP observation sender dropped");
        runtime.progress.release_stopped(2);
        let error = tokio::time::timeout(TEST_TIMEOUT, supervisor)
            .await
            .expect("supervisor did not finish")
            .expect("supervisor task panicked")
            .unwrap_err();

        assert_eq!(error.to_string(), http_stopped_error().to_string());
        assert!(
            graceful_command.await.is_err(),
            "early HTTP completion was followed by a graceful command"
        );
        assert_terminal(
            &runtime.repository,
            &attached_id,
            RunAttachment::Attached,
            RunStatus::Cancelled,
            "RUN_CANCELLED",
            RunEventType::RunCancelled,
        )
        .await;
        assert_terminal(
            &runtime.repository,
            &detached_id,
            RunAttachment::Detached,
            RunStatus::Interrupted,
            "RUN_INTERRUPTED",
            RunEventType::RunInterrupted,
        )
        .await;
        drop(attached);
    }

    #[tokio::test]
    async fn supervisor_preserves_fatal_precedence_and_rejects_signal_failure() {
        let runtime = lifecycle_test_runtime(false).await;
        let mut owner = None;
        let (_ownership_sender, ownership_receiver) = tokio::sync::watch::channel(true);
        let mut ownership_loss = Some(ownership_receiver);
        let (_fatal_sender, mut runtime_fatal) = tokio::sync::watch::channel(true);
        let (http_shutdown, _graceful_command) = tokio::sync::oneshot::channel();
        let error = supervise_runtime(
            &runtime.service,
            &mut owner,
            &mut ownership_loss,
            &mut runtime_fatal,
            std::future::ready(Ok(())),
            http_shutdown,
            std::future::ready(Err(io::Error::other("private signal failure"))),
            TEST_TIMEOUT,
            TEST_HARD_DEADLINE,
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), ownership_lost_error().to_string());

        let runtime = lifecycle_test_runtime(false).await;
        let mut owner = None;
        let mut ownership_loss = None;
        let (_fatal_sender, mut runtime_fatal) = tokio::sync::watch::channel(true);
        let (http_shutdown, _graceful_command) = tokio::sync::oneshot::channel();
        let error = supervise_runtime(
            &runtime.service,
            &mut owner,
            &mut ownership_loss,
            &mut runtime_fatal,
            std::future::ready(Ok(())),
            http_shutdown,
            std::future::ready(Err(io::Error::other("private signal failure"))),
            TEST_TIMEOUT,
            TEST_HARD_DEADLINE,
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), runtime_fatal_error().to_string());

        let runtime = lifecycle_test_runtime(false).await;
        let mut owner = None;
        let mut ownership_loss = None;
        let mut runtime_fatal = runtime.service.subscribe_fatal();
        let (http_shutdown, _graceful_command) = tokio::sync::oneshot::channel();
        let error = supervise_runtime(
            &runtime.service,
            &mut owner,
            &mut ownership_loss,
            &mut runtime_fatal,
            std::future::ready(Ok(())),
            http_shutdown,
            std::future::ready(Err(io::Error::other("private signal failure"))),
            TEST_TIMEOUT,
            TEST_HARD_DEADLINE,
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), http_stopped_error().to_string());

        let runtime = lifecycle_test_runtime(false).await;
        let mut owner = None;
        let mut ownership_loss = None;
        let mut runtime_fatal = runtime.service.subscribe_fatal();
        let (http_shutdown, wait_http_shutdown) = tokio::sync::oneshot::channel();
        let server = async move {
            wait_http_shutdown
                .await
                .map_err(|_| io::Error::other("graceful command sender dropped"))?;
            Ok(())
        };
        let error = supervise_runtime(
            &runtime.service,
            &mut owner,
            &mut ownership_loss,
            &mut runtime_fatal,
            server,
            http_shutdown,
            std::future::ready(Err(io::Error::other("private signal failure"))),
            TEST_TIMEOUT,
            TEST_HARD_DEADLINE,
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), signal_failed_error().to_string());
    }

    #[test]
    fn runtime_fatal_error_has_a_stable_sanitized_message() {
        assert_eq!(
            runtime_fatal_error().to_string(),
            "a run task panicked; the process was shut down"
        );
    }

    #[test]
    fn shutdown_outcome_preserves_fatal_and_ownership_precedence() {
        assert_eq!(
            shutdown_decision(false, true, false, false).outcome,
            ShutdownOutcome::RuntimeFatal
        );
        assert_eq!(
            shutdown_decision(false, true, true, true).outcome,
            ShutdownOutcome::RuntimeFatal
        );
        assert_eq!(
            shutdown_decision(true, true, true, true).outcome,
            ShutdownOutcome::OwnershipLost
        );
        assert_eq!(
            shutdown_decision(false, false, true, true).outcome,
            ShutdownOutcome::HttpStopped
        );
        assert_eq!(
            shutdown_decision(false, false, false, true).outcome,
            ShutdownOutcome::SignalFailed
        );
        assert_eq!(
            shutdown_decision(false, false, false, false).outcome,
            ShutdownOutcome::Clean
        );
        assert!(!shutdown_decision(true, true, true, true).release_store_owner);
        assert!(shutdown_decision(false, true, true, true).release_store_owner);
        assert!(shutdown_decision(false, false, true, true).release_store_owner);
        assert!(shutdown_decision(false, false, false, true).release_store_owner);
        assert!(shutdown_decision(false, false, false, false).release_store_owner);
    }

    #[test]
    fn owner_release_ownership_loss_promotes_every_lower_shutdown_outcome() {
        let lower_decisions = [
            shutdown_decision(false, true, false, false),
            shutdown_decision(false, false, true, false),
            shutdown_decision(false, false, false, true),
            shutdown_decision(false, false, false, false),
        ];

        for decision in lower_decisions {
            let ownership_loss = Err(HistoryError::new(
                "HISTORY_OWNERSHIP_LOST",
                "private ownership detail",
            ));
            assert_eq!(
                merge_release_ownership_loss(decision, &ownership_loss),
                ShutdownDecision {
                    outcome: ShutdownOutcome::OwnershipLost,
                    release_store_owner: false,
                }
            );

            let other_release_error = Err(HistoryError::new(
                "HISTORY_RELEASE_FAILED",
                "private release detail",
            ));
            assert_eq!(
                merge_release_ownership_loss(decision, &other_release_error),
                decision,
                "non-ownership release errors must preserve {decision:?}"
            );
        }
    }

    #[test]
    fn panic_hook_event_is_fixed_and_payload_independent() {
        let private_payload = "panic payload with private input";
        let (code, message) = sanitized_panic_event();
        assert_eq!(code, "PROCESS_PANICKED");
        assert_eq!(message, "process panic captured");
        assert!(!code.contains(private_payload));
        assert!(!message.contains(private_payload));
    }
}
