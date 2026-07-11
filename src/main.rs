use std::{error::Error, future::IntoFuture, sync::Arc, time::Duration};

use insight_agent_platform::{
    api::formal::{build_router, ApiAuth, FormalApiState},
    catalog::compile_enabled_agents,
    config::{HistoryConfig, PlatformConfig},
    dsl::compiler::CompileLimits,
    events::hub::{EventHub, EventHubConfig},
    history::{
        postgres::PostgresRunRepository, repository::RunRepository, sqlite::SqliteRunRepository,
    },
    nodes::default_node_registries,
    resources::{
        builtin_actions::{builtin_action_registry, RestrictedHttpGetAction},
        config::load_model_registry,
    },
    runtime::{RunService, RunServiceConfig},
};

type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

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
    let repository = initialize_repository(&config.history).await?;
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

    let app = build_router(FormalApiState {
        service: service.clone(),
        auth: ApiAuth::from(&config.auth),
    });
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
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
    tokio::select! {
        result = &mut server => {
            tracing::warn!("HTTP server stopped before a shutdown signal");
            tokio::time::timeout(
                Duration::from_secs(35),
                service.shutdown(Duration::from_secs(30)),
            )
            .await
            .map_err(|_| std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "runtime shutdown exceeded its hard deadline",
            ))??;
            result?;
        },
        _ = wait_for_shutdown_signal() => {
            tracing::info!("shutdown signal received");
            let _ = http_shutdown.send(());
            let drain = async {
                let (runtime, http) = tokio::join!(
                    service.shutdown(Duration::from_secs(30)),
                    &mut server,
                );
                runtime?;
                http?;
                MainResult::Ok(())
            };
            tokio::time::timeout(Duration::from_secs(35), drain)
                .await
                .map_err(|_| std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "server shutdown exceeded its hard deadline",
                ))??;
        }
    }
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

async fn initialize_repository(config: &HistoryConfig) -> MainResult<Arc<dyn RunRepository>> {
    match config {
        HistoryConfig::Sqlite { path } => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            Ok(Arc::new(SqliteRunRepository::connect_path(path).await?))
        }
        HistoryConfig::Postgres { database_url } => Ok(Arc::new(
            PostgresRunRepository::connect(database_url.expose()).await?,
        )),
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
