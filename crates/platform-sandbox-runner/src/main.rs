use insight_platform_sandbox_runner::{prepare_runner_boundary, serve, RunnerStartupV1};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    prepare_runner_boundary()?;
    let startup = RunnerStartupV1::from_environment()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(serve(startup))?;
    Ok(())
}
