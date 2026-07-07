use insight_agent_platform::{
    agent::{loader::load_agents, registry::AgentRegistry},
    api::{
        routes::{build_router, AppState},
        sse::encode_event,
    },
    config::PlatformConfig,
    engine::runner::RunEngine,
    error::AppError,
    handlers::code_registry_for_agents,
    history::store::RunHistoryStore,
    model::providers::{validate_agent_models, ModelProviderCatalog, ModelRouter},
    tools::registry::default_tool_registry,
};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = PlatformConfig::from_env()?;
    tracing::info!(
        bind_addr = %config.bind_addr,
        agents_dir = %config.agents_dir.display(),
        run_history_db = %config.run_history_db.display(),
        providers_count = config.model_providers.providers.len(),
        default_provider = ?config.model_providers.default_provider,
        "platform configuration loaded"
    );
    let agents = config.filter_enabled_agents(load_agents(&config.agents_dir)?)?;
    let agent_ids = agents
        .iter()
        .map(|agent| agent.config.id.as_str())
        .collect::<Vec<_>>();
    tracing::info!(
        agents_count = agents.len(),
        agents = ?agent_ids,
        "agents loaded"
    );
    let model_catalog = ModelProviderCatalog::new(config.model_providers.clone())?;
    validate_agent_models(&agents, &model_catalog)?;
    tracing::info!("agent model configuration validated");
    let code_handlers = code_registry_for_agents(&agents)?;
    let code_handler_names = code_handlers.names().collect::<Vec<_>>();
    tracing::info!(
        code_handlers_count = code_handlers.len(),
        code_handlers = ?code_handler_names,
        "code handlers registered for enabled agents"
    );
    let registry = AgentRegistry::new(agents)?;
    let model = ModelRouter::from_openai_compatible_config(&config.model_providers)?;
    let history_store = RunHistoryStore::sqlite(&config.run_history_db)?;
    let engine = RunEngine::new(model, default_tool_registry())
        .with_code_handlers(code_handlers)
        .with_history_store(history_store);
    let app = build_router(AppState {
        registry,
        engine,
        event_encoder: encode_event,
    });

    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .map_err(|err| AppError::Config(format!("failed to bind server: {err}")))?;
    tracing::info!(bind_addr = %config.bind_addr, "server listening");
    axum::serve(listener, app)
        .await
        .map_err(|err| AppError::Run(format!("server error: {err}")))?;
    Ok(())
}
