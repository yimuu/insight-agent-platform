use std::{
    error::Error,
    future::{pending, IntoFuture},
    sync::Arc,
    time::Duration,
};

use insight_agent_platform::{
    api::formal::{build_router, ApiAuth, FormalApiState},
    catalog::compile_enabled_agents,
    config::{HistoryConfig, PlatformConfig},
    dsl::compiler::CompileLimits,
    events::hub::{EventHub, EventHubConfig},
    history::{
        postgres::{PostgresRunRepository, PostgresStoreOwner},
        repository::{HistoryError, RunRepository},
        sqlite::SqliteRunRepository,
    },
    nodes::default_node_registries,
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
    HttpStopped(std::io::Result<()>),
    Signal,
    OwnershipLost,
    RuntimeFatal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownOutcome {
    OwnershipLost,
    RuntimeFatal,
    HttpStopped,
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
    let (node_types, executors) = default_node_registries()?;
    let agents = compile_enabled_agents(
        &config.agents.directory,
        &config.agents.enabled,
        node_types,
        models,
        actions,
        config.runtime.default_node_timeout,
        CompileLimits {
            max_fork_branches: config.runtime.max_fork_branches,
        },
    )?;
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
        executors,
        repository,
        events,
        RunServiceConfig {
            max_concurrent_runs: config.runtime.max_concurrent_runs,
            max_parallel_node_executions: config.runtime.max_parallel_node_executions,
            max_parallel_branches_per_run: config.runtime.max_parallel_branches_per_run,
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
        "formal V1 runtime listening"
    );
    let (http_shutdown, wait_http_shutdown) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = wait_http_shutdown.await;
        })
        .into_future();
    tokio::pin!(server);
    let trigger = tokio::select! {
        result = &mut server => {
            tracing::warn!("HTTP server stopped before a shutdown signal");
            ShutdownTrigger::HttpStopped(result)
        },
        _ = wait_for_shutdown_signal() => {
            tracing::info!("shutdown signal received");
            ShutdownTrigger::Signal
        },
        _ = wait_for_ownership_loss(&mut ownership_loss) => {
            tracing::error!(
                code = "HISTORY_OWNERSHIP_LOST",
                "PostgreSQL history store ownership was lost"
            );
            ShutdownTrigger::OwnershipLost
        },
        _ = wait_for_runtime_fatal(&mut runtime_fatal) => {
            tracing::error!(
                code = RUNTIME_TASK_PANICKED_CODE,
                "a run task panicked; forcing process shutdown"
            );
            ShutdownTrigger::RuntimeFatal
        }
    };
    service.begin_shutdown();

    let ownership_loss_triggered = matches!(&trigger, ShutdownTrigger::OwnershipLost);
    let runtime_fatal_triggered = matches!(&trigger, ShutdownTrigger::RuntimeFatal);
    let http_stopped_early = matches!(&trigger, ShutdownTrigger::HttpStopped(_));
    let drain = async {
        let runtime = service.shutdown(config.runtime.shutdown_grace_period).await;
        let http = match trigger {
            ShutdownTrigger::HttpStopped(result) => result,
            ShutdownTrigger::Signal
            | ShutdownTrigger::OwnershipLost
            | ShutdownTrigger::RuntimeFatal => {
                let _ = http_shutdown.send(());
                (&mut server).await
            }
        };
        let lost_during_drain = owner.as_ref().is_some_and(PostgresStoreOwner::is_lost);
        let fatal_during_drain = service.is_fatal();

        let decision = shutdown_decision(
            ownership_loss_triggered || lost_during_drain,
            runtime_fatal_triggered || fatal_during_drain,
            http_stopped_early,
        );
        match decision.outcome {
            ShutdownOutcome::OwnershipLost => {
                debug_assert!(!decision.release_store_owner);
                if let Err(error) = &runtime {
                    tracing::error!(
                        code = error.code(),
                        "runtime drain failed after ownership loss"
                    );
                }
                if let Err(error) = &http {
                    tracing::error!(%error, "HTTP drain failed after ownership loss");
                }
                Err(ownership_lost_error().into())
            }
            ShutdownOutcome::RuntimeFatal => {
                if let Err(error) = &runtime {
                    tracing::error!(
                        code = error.code(),
                        "runtime drain failed after a run task panic"
                    );
                }
                if let Err(error) = &http {
                    tracing::error!(%error, "HTTP drain failed after a run task panic");
                }
                if decision.release_store_owner {
                    release_store_owner(&mut owner).await?;
                }
                Err(runtime_fatal_error().into())
            }
            ShutdownOutcome::HttpStopped | ShutdownOutcome::Clean => {
                runtime?;
                http?;
                if decision.release_store_owner {
                    release_store_owner(&mut owner).await?;
                }
                if http_stopped_early {
                    return Err(std::io::Error::other(
                        "HTTP server stopped before a shutdown signal",
                    )
                    .into());
                }
                MainResult::Ok(())
            }
        }
    };
    tokio::time::timeout(config.runtime.shutdown_hard_deadline, drain)
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "server shutdown exceeded its hard deadline",
            )
        })??;
    Ok(())
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

fn shutdown_decision(
    ownership_lost: bool,
    runtime_fatal: bool,
    http_stopped: bool,
) -> ShutdownDecision {
    let outcome = if ownership_lost {
        ShutdownOutcome::OwnershipLost
    } else if runtime_fatal {
        ShutdownOutcome::RuntimeFatal
    } else if http_stopped {
        ShutdownOutcome::HttpStopped
    } else {
        ShutdownOutcome::Clean
    };
    ShutdownDecision {
        outcome,
        release_store_owner: outcome != ShutdownOutcome::OwnershipLost,
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
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(error) => {
            tracing::warn!(%error, "failed to install SIGTERM handler; waiting for Ctrl-C");
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                tracing::warn!(%error, "Ctrl-C handler failed");
            }
        }
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "Ctrl-C handler failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            shutdown_decision(false, true, false).outcome,
            ShutdownOutcome::RuntimeFatal
        );
        assert_eq!(
            shutdown_decision(false, true, true).outcome,
            ShutdownOutcome::RuntimeFatal
        );
        assert_eq!(
            shutdown_decision(true, true, true).outcome,
            ShutdownOutcome::OwnershipLost
        );
        assert_eq!(
            shutdown_decision(false, false, true).outcome,
            ShutdownOutcome::HttpStopped
        );
        assert_eq!(
            shutdown_decision(false, false, false).outcome,
            ShutdownOutcome::Clean
        );
        assert!(!shutdown_decision(true, true, true).release_store_owner);
        assert!(shutdown_decision(false, true, true).release_store_owner);
        assert!(shutdown_decision(false, false, true).release_store_owner);
        assert!(shutdown_decision(false, false, false).release_store_owner);
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
