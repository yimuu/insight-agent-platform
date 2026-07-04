use insight_agent_platform::{
    agent::{loader::load_agents, registry::AgentRegistry},
    api::routes::{build_router, AppState},
    config::PlatformConfig,
    engine::runner::RunEngine,
    error::AppError,
    model::openai::OpenAiModelClient,
    tools::registry::default_tool_registry,
};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = PlatformConfig::from_env()?;
    let agents = load_agents(&config.agents_dir)?;
    let registry = AgentRegistry::new(agents)?;
    let model = OpenAiModelClient::new(
        config.openai_api_key,
        config.openai_base_url,
        config.openai_default_model,
    );
    let engine = RunEngine::new(model, default_tool_registry());
    let app = build_router(AppState { registry, engine });

    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .map_err(|err| AppError::Config(format!("failed to bind server: {err}")))?;
    tracing::info!(bind_addr = %config.bind_addr, "server listening");
    axum::serve(listener, app)
        .await
        .map_err(|err| AppError::Run(format!("server error: {err}")))?;
    Ok(())
}
