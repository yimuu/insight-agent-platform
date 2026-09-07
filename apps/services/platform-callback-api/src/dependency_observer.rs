use insight_platform_egress_rpc::{EgressRpcDependencyObserver, EgressRpcDependencyOutcome};
use insight_platform_observability::{
    DependencyObservationMetrics, DependencyObservationOutcome, MetricsInstallError,
    PlatformDependency,
};
use insight_platform_postgres::dependency_health::{PostgresHealthObserver, PostgresHealthOutcome};
use std::sync::Arc;

struct CallbackDependencyObserver(Arc<DependencyObservationMetrics>);

impl PostgresHealthObserver for CallbackDependencyObserver {
    fn observe(&self, outcome: PostgresHealthOutcome) {
        self.0
            .observe(
                PlatformDependency::Postgresql,
                match outcome {
                    PostgresHealthOutcome::Success => DependencyObservationOutcome::Success,
                    PostgresHealthOutcome::Failure => DependencyObservationOutcome::Failure,
                },
            )
            .expect("Callback API installs its PostgreSQL dependency metric");
    }
}

impl EgressRpcDependencyObserver for CallbackDependencyObserver {
    fn observe(&self, outcome: EgressRpcDependencyOutcome) {
        self.0
            .observe(
                PlatformDependency::Egress,
                match outcome {
                    EgressRpcDependencyOutcome::Success => DependencyObservationOutcome::Success,
                    EgressRpcDependencyOutcome::Failure => DependencyObservationOutcome::Failure,
                },
            )
            .expect("Callback API installs its Egress dependency metric");
    }
}

pub type InstalledCallbackDependencyMetrics = (
    Arc<DependencyObservationMetrics>,
    Arc<dyn PostgresHealthObserver>,
    Arc<dyn EgressRpcDependencyObserver>,
);

pub fn install_callback_dependency_metrics(
) -> Result<InstalledCallbackDependencyMetrics, MetricsInstallError> {
    let metrics = Arc::new(DependencyObservationMetrics::install(&[
        PlatformDependency::Postgresql,
        PlatformDependency::Egress,
    ])?);
    let observer = Arc::new(CallbackDependencyObserver(Arc::clone(&metrics)));
    let postgres: Arc<dyn PostgresHealthObserver> = observer.clone();
    let egress: Arc<dyn EgressRpcDependencyObserver> = observer;
    Ok((metrics, postgres, egress))
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_observability::{ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS};

    #[test]
    fn adapter_maps_only_fixed_postgresql_outcomes() {
        let (dependencies, postgres, egress) = install_callback_dependency_metrics().unwrap();
        postgres.observe(PostgresHealthOutcome::Success);
        egress.observe(EgressRpcDependencyOutcome::Failure);
        let rendered =
            ProcessHttpMetrics::install("mcp-callback-api", PROCESS_OBSERVABILITY_OPERATIONS)
                .unwrap()
                .with_dependency_observations(dependencies)
                .render_prometheus();
        assert!(rendered.contains("dependency=\"postgresql\",outcome=\"success\"} 1"));
        assert!(rendered.contains("dependency=\"egress\",outcome=\"failure\"} 1"));
        assert!(!rendered.contains("database="));
        assert!(!rendered.contains("pool="));
        assert!(!rendered.contains("error="));
    }
}
