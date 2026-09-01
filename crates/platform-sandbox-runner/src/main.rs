use insight_platform_sandbox_runner::{serve, RunnerStartupV1};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let startup = RunnerStartupV1::from_environment()?;
    serve(startup).await?;
    Ok(())
}
