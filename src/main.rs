use std::{error::Error, future::IntoFuture, io, sync::Arc};

use insight_agent_platform::{
    api::formal::{build_router, ApiAuth, BearerHumanPrincipalResolver, FormalApiState},
    catalog_v3::{
        compile_enabled_v3_agents, deploy_v3_agents, OwnedProductionLeafDeploymentResolver,
        ProductionLeafDeploymentResolver,
    },
    config::{ArtifactStoreProvider, DeploymentMode, HistoryConfig, PlatformConfig},
    engine::{
        production_worker_registry,
        repository::{PostgresDurableRepository, SqliteDurableRepository},
        LocalContentAddressedArtifactStore,
    },
    resources::{
        builtin_actions::{builtin_action_registry, RestrictedHttpGetAction},
        config::load_model_registry,
    },
    runtime::{DeployedAgentCatalog, ProductionRunRepository, RunService, RunServiceConfig},
};

type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const PROCESS_PANICKED_CODE: &str = "PROCESS_PANICKED";
const PROCESS_PANICKED_MESSAGE: &str = "process panic captured";

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

    let published = compile_enabled_v3_agents(&config.agents.directory, &config.agents.enabled)?;
    let resolver = ProductionLeafDeploymentResolver::new(&models, &actions)
        .with_operation_timeout(config.runtime.operation_timeout)?;
    let graph_publication_resolver = Arc::new(
        OwnedProductionLeafDeploymentResolver::new(&models, &actions)
            .with_operation_timeout(config.runtime.operation_timeout)?,
    );
    let deployed = deploy_v3_agents(&published, &resolver)?;
    let agents = DeployedAgentCatalog::new(deployed)?;
    let workers = production_worker_registry(&models, &actions)?;
    let repository = initialize_repository(&config.history).await?;
    let artifact_store = Arc::new(match config.artifacts.provider {
        ArtifactStoreProvider::LocalFilesystem => {
            LocalContentAddressedArtifactStore::open(
                config.artifacts.directory.clone(),
                config.artifacts.inline_threshold_bytes,
            )
            .await?
        }
        ArtifactStoreProvider::SharedFilesystem => {
            LocalContentAddressedArtifactStore::open_shared(
                config.artifacts.directory.clone(),
                config.artifacts.inline_threshold_bytes,
                config.artifacts.namespace.clone().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "shared Artifact namespace is missing",
                    )
                })?,
            )
            .await?
        }
    });
    let service_config = match config.deployment_mode {
        DeploymentMode::Production => RunServiceConfig::production(
            config.runtime.max_concurrent_runs,
            config.runtime.max_concurrent_operations,
            config.runtime.max_concurrent_operations_per_run,
            config.runtime.subscriber_capacity,
        ),
        DeploymentMode::SingleProcessDevelopment => RunServiceConfig::single_process_development(
            config.runtime.max_concurrent_runs,
            config.runtime.max_concurrent_operations,
            config.runtime.max_concurrent_operations_per_run,
            config.runtime.subscriber_capacity,
        ),
    }
    .with_run_timeout(config.runtime.run_timeout)
    .with_public_event_retention(
        config.runtime.public_event_retention,
        config.runtime.public_event_prune_interval,
    )
    .with_artifact_gc(
        config.artifacts.orphan_retention,
        config.artifacts.reference_retention,
        config.artifacts.gc_interval,
        config.artifacts.deletion_claim_seconds,
    );
    let service = RunService::start_with_artifact_store_and_graph_publication(
        agents,
        repository,
        workers,
        artifact_store,
        graph_publication_resolver,
        service_config,
    )
    .await?;

    let app = build_router(FormalApiState {
        service: service.clone(),
        auth: build_api_auth(&config)?,
        sse_keep_alive_interval: config.runtime.sse_keep_alive_interval,
        readiness_probe_timeout: config.runtime.readiness_probe_timeout,
    });
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tracing::info!(
        bind_addr = %config.bind_addr,
        agents = service.agents().list().count(),
        "v3 durable runtime listening"
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
            service.begin_shutdown();
            service.shutdown(config.runtime.shutdown_grace_period).await?;
            result?;
            Err(io::Error::other("HTTP server stopped before a shutdown signal").into())
        }
        signal = wait_for_shutdown_signal() => {
            signal?;
            tracing::info!("shutdown signal received");
            service.begin_shutdown();
            let _ = http_shutdown.send(());
            tokio::time::timeout(config.runtime.shutdown_hard_deadline, async {
                service.shutdown(config.runtime.shutdown_grace_period).await?;
                (&mut server).await?;
                Ok::<(), Box<dyn Error + Send + Sync>>(())
            })
            .await
            .map_err(|_| io::Error::new(
                io::ErrorKind::TimedOut,
                "server shutdown exceeded its hard deadline",
            ))??;
            Ok(())
        }
    }
}

fn build_api_auth(config: &PlatformConfig) -> MainResult<ApiAuth> {
    let base = ApiAuth::from(&config.auth);
    if config.human_task_credentials.is_empty() {
        return Ok(base);
    }

    let resolver =
        BearerHumanPrincipalResolver::new(config.human_task_credentials.iter().map(|credential| {
            (
                credential.token().expose().to_owned(),
                credential.identity().to_owned(),
                credential.groups().iter().cloned().collect(),
            )
        }))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "HumanTask credential configuration is invalid",
            )
        })?;
    Ok(base.with_human_principal_resolver(Arc::new(resolver)))
}

async fn initialize_repository(
    config: &HistoryConfig,
) -> MainResult<Arc<dyn ProductionRunRepository>> {
    match config {
        HistoryConfig::Sqlite { path } => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            Ok(Arc::new(SqliteDurableRepository::connect_path(path).await?))
        }
        HistoryConfig::Postgres { database_url } => {
            let repository = PostgresDurableRepository::connect(database_url.expose()).await?;
            // Migration is part of PostgreSQL startup authority and must finish
            // before RunService performs any catalog or runtime table reads.
            repository.migrate_schema().await?;
            Ok(Arc::new(repository))
        }
    }
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
        tracing::error!(code = PROCESS_PANICKED_CODE, "{PROCESS_PANICKED_MESSAGE}");
    }));
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
