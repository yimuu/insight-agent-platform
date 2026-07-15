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

struct InitializedRepository {
    repository: Arc<dyn RunRepository>,
    owner: Option<PostgresStoreOwner>,
}

enum ShutdownTrigger {
    HttpStopped(std::io::Result<()>),
    Signal,
    OwnershipLost,
}

#[tokio::main]
async fn main() -> MainResult<()> {
    let _ = dotenvy::dotenv();
    init_tracing();

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
        }
    };
    service.begin_shutdown();

    let ownership_loss_triggered = matches!(&trigger, ShutdownTrigger::OwnershipLost);
    let http_stopped_early = matches!(&trigger, ShutdownTrigger::HttpStopped(_));
    let drain = async {
        let runtime = service.shutdown(config.runtime.shutdown_grace_period).await;
        let http = match trigger {
            ShutdownTrigger::HttpStopped(result) => result,
            ShutdownTrigger::Signal | ShutdownTrigger::OwnershipLost => {
                let _ = http_shutdown.send(());
                (&mut server).await
            }
        };
        let lost_during_drain = owner.as_ref().is_some_and(PostgresStoreOwner::is_lost);

        if ownership_loss_triggered || lost_during_drain {
            if let Err(error) = &runtime {
                tracing::error!(
                    code = error.code(),
                    "runtime drain failed after ownership loss"
                );
            }
            if let Err(error) = &http {
                tracing::error!(%error, "HTTP drain failed after ownership loss");
            }
            return Err(ownership_lost_error().into());
        }

        runtime?;
        http?;
        release_store_owner(&mut owner).await?;
        if http_stopped_early {
            return Err(
                std::io::Error::other("HTTP server stopped before a shutdown signal").into(),
            );
        }
        MainResult::Ok(())
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
}
