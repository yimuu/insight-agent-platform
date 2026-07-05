use insight_agent_platform::{
    agent::{loader::load_agents, registry::AgentRegistry},
    api::{
        routes::{build_router, AppState},
        sse::encode_event,
    },
    config::PlatformConfig,
    engine::runner::RunEngine,
    error::AppError,
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
    let agents = load_agents(&config.agents_dir)?;
    let model_catalog = ModelProviderCatalog::new(config.model_providers.clone())?;
    validate_agent_models(&agents, &model_catalog)?;
    let registry = AgentRegistry::new(agents)?;
    let model = ModelRouter::from_openai_compatible_config(&config.model_providers)?;
    let engine = RunEngine::new(model, default_tool_registry());
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
