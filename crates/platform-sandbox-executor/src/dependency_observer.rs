use insight_platform_observability::{
    DependencyObservationMetrics, DependencyObservationOutcome, MetricsInstallError,
    PlatformDependency,
};
use insight_platform_sandbox_rpc::{SandboxNatsDependencyObserver, SandboxNatsDependencyOutcome};
use std::sync::Arc;

struct SandboxExecutorNatsObserver(Arc<DependencyObservationMetrics>);

impl SandboxNatsDependencyObserver for SandboxExecutorNatsObserver {
    fn observe(&self, outcome: SandboxNatsDependencyOutcome) {
        self.0
            .observe(
                PlatformDependency::Nats,
                match outcome {
                    SandboxNatsDependencyOutcome::Success => DependencyObservationOutcome::Success,
                    SandboxNatsDependencyOutcome::Failure => DependencyObservationOutcome::Failure,
                },
            )
            .expect("Sandbox Executor installs its NATS dependency metric");
    }
}

pub fn install_sandbox_executor_dependency_metrics() -> Result<
    (
        Arc<DependencyObservationMetrics>,
        Arc<dyn SandboxNatsDependencyObserver>,
    ),
    MetricsInstallError,
> {
    let metrics = Arc::new(DependencyObservationMetrics::install(&[
        PlatformDependency::Nats,
    ])?);
    let observer: Arc<dyn SandboxNatsDependencyObserver> =
        Arc::new(SandboxExecutorNatsObserver(Arc::clone(&metrics)));
    Ok((metrics, observer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_observability::{ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS};

    #[test]
    fn adapter_maps_only_fixed_nats_outcomes() {
        let (dependencies, observer) = install_sandbox_executor_dependency_metrics().unwrap();
        observer.observe(SandboxNatsDependencyOutcome::Failure);
        let rendered =
            ProcessHttpMetrics::install("sandbox-wasi-executor", PROCESS_OBSERVABILITY_OPERATIONS)
                .unwrap()
                .with_dependency_observations(dependencies)
                .render_prometheus();
        assert!(rendered.contains("dependency=\"nats\",outcome=\"failure\"} 1"));
        assert!(!rendered.contains("server="));
        assert!(!rendered.contains("subject="));
        assert!(!rendered.contains("tenant="));
        assert!(!rendered.contains("error="));
    }
}
