use std::{error::Error, future::IntoFuture, io, sync::Arc};

use insight_agent_platform::{
    catalog::{
        compile_enabled_agents, deploy_agents, OwnedProductionLeafDeploymentResolver,
        ProductionLeafDeploymentResolver,
    },
    config::{
        ArtifactStoreProvider, DeploymentMode, HistoryConfig, LiveResponseBrokerProvider,
        PlatformConfig, ResponseStreamConfig,
    },
    engine::{
        production_worker_registry_with_live_response_and_retrievals,
        repository::{PostgresDurableRepository, SqliteDurableRepository},
        LocalContentAddressedArtifactStore,
    },
    resources::{
        builtin_actions::{builtin_action_registry, RestrictedHttpGetAction},
        config::load_model_registry,
        retrievals::RetrievalRegistry,
    },
    runtime::{
        DeployedAgentCatalog, InMemoryLiveResponseBroker, LiveResponseBroker,
        LiveResponseByteLimits, PostgresLiveResponseBroker, PostgresLiveResponseBrokerOptions,
        ProductionRunRepository, RunService, RunServiceConfig, WorkCoordinatorConfig,
    },
};
use insight_api::v1::{build_router, ApiAuth, ApiState, BearerHumanPrincipalResolver};

type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const PROCESS_PANICKED_CODE: &str = "PROCESS_PANICKED";
const PROCESS_PANICKED_MESSAGE: &str = "process panic captured";
const RUNTIME_POSTGRES_APPLICATION_NAME: &str = "insight-agent-platform-runtime";

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
    let retrievals = RetrievalRegistry::default();

    let published = compile_enabled_agents(&config.agents.directory, &config.agents.enabled)?;
    let resolver = ProductionLeafDeploymentResolver::new(&models, &actions)
        .with_retrievals(&retrievals)
        .with_llm_tool_continuation_capability()
        .with_operation_timeout(config.runtime.operation_timeout)?
        .with_llm_tool_limits(
            config.runtime.max_llm_tool_rounds,
            config.runtime.max_llm_tool_calls,
        )?;
    let graph_publication_resolver = Arc::new(
        OwnedProductionLeafDeploymentResolver::new(&models, &actions)
            .with_retrievals(&retrievals)
            .with_llm_tool_continuation_capability()
            .with_operation_timeout(config.runtime.operation_timeout)?
            .with_llm_tool_limits(
                config.runtime.max_llm_tool_rounds,
                config.runtime.max_llm_tool_calls,
            )?,
    );
    let deployed = deploy_agents(&published, &resolver)?;
    let agents = DeployedAgentCatalog::new(deployed)?;
    let (repository, live_response_broker) =
        initialize_repository_and_live_response(&config.history, config.runtime.response_stream)
            .await?;
    let workers = production_worker_registry_with_live_response_and_retrievals(
        &models,
        &actions,
        &retrievals,
        Arc::clone(&live_response_broker),
    )?;
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
    .with_work_coordinator(WorkCoordinatorConfig {
        active_poll_interval: config.runtime.scheduler.active_poll_interval,
        idle_poll_min_interval: config.runtime.scheduler.idle_poll_min_interval,
        idle_poll_max_interval: config.runtime.scheduler.idle_poll_max_interval,
        safety_poll_interval: config.runtime.scheduler.safety_poll_interval,
        claim_batch_size: config.runtime.scheduler.claim_batch_size,
        notification_reconnect_interval: config.runtime.scheduler.notification_reconnect_interval,
    })
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
    )
    .with_artifact_read_limit(config.artifacts.max_read_bytes)
    .with_live_response_limits(
        config.runtime.response_stream.body_queue_capacity,
        config.runtime.response_stream.control_queue_capacity,
        config.runtime.response_stream.terminal_barrier_timeout,
        config.runtime.response_stream.outbound_write_timeout,
    )
    .with_live_response_byte_limits(
        config.runtime.response_stream.max_frame_bytes,
        config.runtime.response_stream.max_item_bytes,
        config.runtime.response_stream.max_run_bytes,
    );
    let service = RunService::start_with_artifact_store_graph_publication_and_live_response(
        agents,
        repository,
        workers,
        live_response_broker,
        artifact_store,
        graph_publication_resolver,
        service_config,
    )
    .await?;

    let app = build_router(ApiState {
        service: service.clone(),
        auth: build_api_auth(&config)?,
        sse_keep_alive_interval: config.runtime.sse_keep_alive_interval,
        readiness_probe_timeout: config.runtime.readiness_probe_timeout,
    });
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tracing::info!(
        bind_addr = %config.bind_addr,
        agents = service.agents().list().count(),
        "durable runtime listening"
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

async fn initialize_repository_and_live_response(
    config: &HistoryConfig,
    response_stream: ResponseStreamConfig,
) -> MainResult<(
    Arc<dyn ProductionRunRepository>,
    Arc<dyn LiveResponseBroker>,
)> {
    match config {
        HistoryConfig::Sqlite { path } => {
            if response_stream.broker != LiveResponseBrokerProvider::InProcess {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "SQLite history supports only the in-process live response broker",
                )
                .into());
            }
            let repository = Arc::new(SqliteDurableRepository::connect_path(path).await?)
                as Arc<dyn ProductionRunRepository>;
            let broker = Arc::new(InMemoryLiveResponseBroker::new_with_limits(
                response_stream.body_queue_capacity,
                response_stream.control_queue_capacity,
                LiveResponseByteLimits::new(
                    response_stream.max_frame_bytes,
                    response_stream.max_item_bytes,
                    response_stream.max_run_bytes,
                )?,
            )?) as Arc<dyn LiveResponseBroker>;
            Ok((repository, broker))
        }
        HistoryConfig::Postgres {
            database_url,
            max_connections,
        } => {
            let database_url = runtime_postgres_url(database_url.expose());
            let repository = PostgresDurableRepository::connect_with_max_connections(
                &database_url,
                *max_connections,
            )
            .await?;
            let broker: Arc<dyn LiveResponseBroker> = match response_stream.broker {
                LiveResponseBrokerProvider::InProcess => {
                    Arc::new(InMemoryLiveResponseBroker::new_with_limits(
                        response_stream.body_queue_capacity,
                        response_stream.control_queue_capacity,
                        LiveResponseByteLimits::new(
                            response_stream.max_frame_bytes,
                            response_stream.max_item_bytes,
                            response_stream.max_run_bytes,
                        )?,
                    )?)
                }
                LiveResponseBrokerProvider::PostgresNotify => Arc::new(
                    PostgresLiveResponseBroker::start(
                        repository.connection_pool(),
                        PostgresLiveResponseBrokerOptions::new(
                            response_stream.body_queue_capacity,
                            response_stream.control_queue_capacity,
                            response_stream.max_frame_bytes,
                        )?
                        .with_publication_limits(
                            response_stream.max_item_bytes,
                            response_stream.max_run_bytes,
                        )?,
                    )
                    .await?,
                ),
            };
            Ok((Arc::new(repository), broker))
        }
    }
}

fn runtime_postgres_url(database_url: &str) -> String {
    let separator = if database_url.contains('?') { '&' } else { '?' };
    format!("{database_url}{separator}application_name={RUNTIME_POSTGRES_APPLICATION_NAME}")
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
