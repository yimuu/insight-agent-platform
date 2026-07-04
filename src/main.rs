use insight_agent_platform::{config::PlatformConfig, error::AppError};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = PlatformConfig::from_env()?;
    tracing::info!(bind_addr = %config.bind_addr, "starting insight agent platform");
    Ok(())
}
