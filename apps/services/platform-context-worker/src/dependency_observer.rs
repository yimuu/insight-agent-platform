use insight_platform_egress_rpc::{EgressRpcDependencyObserver, EgressRpcDependencyOutcome};
use insight_platform_observability::{
    DependencyObservationMetrics, DependencyObservationOutcome, MetricsInstallError,
    PlatformDependency,
};
use insight_platform_postgres::dependency_health::{PostgresHealthObserver, PostgresHealthOutcome};
use std::sync::Arc;

struct ContextPostgresObserver {
    metrics: Arc<DependencyObservationMetrics>,
}

impl PostgresHealthObserver for ContextPostgresObserver {
    fn observe(&self, outcome: PostgresHealthOutcome) {
        self.metrics
            .observe(
                PlatformDependency::Postgresql,
                match outcome {
                    PostgresHealthOutcome::Success => DependencyObservationOutcome::Success,
                    PostgresHealthOutcome::Failure => DependencyObservationOutcome::Failure,
                },
            )
            .expect("Context roles install their PostgreSQL dependency metric");
    }
}

impl EgressRpcDependencyObserver for ContextPostgresObserver {
    fn observe(&self, outcome: EgressRpcDependencyOutcome) {
        self.metrics
            .observe(
                PlatformDependency::Egress,
                match outcome {
                    EgressRpcDependencyOutcome::Success => DependencyObservationOutcome::Success,
                    EgressRpcDependencyOutcome::Failure => DependencyObservationOutcome::Failure,
                },
            )
            .expect("Remote Context installs its Egress dependency metric");
    }
}

pub struct InstalledContextDependencyMetrics {
    pub process: Arc<DependencyObservationMetrics>,
    pub postgres: Arc<dyn PostgresHealthObserver>,
    pub egress: Option<Arc<dyn EgressRpcDependencyObserver>>,
}

pub fn install_context_dependency_metrics(
    include_egress: bool,
) -> Result<InstalledContextDependencyMetrics, MetricsInstallError> {
    let dependencies = if include_egress {
        &[PlatformDependency::Postgresql, PlatformDependency::Egress][..]
    } else {
        &[PlatformDependency::Postgresql][..]
    };
    let metrics = Arc::new(DependencyObservationMetrics::install(dependencies)?);
    let observer = Arc::new(ContextPostgresObserver {
        metrics: Arc::clone(&metrics),
    });
    let postgres: Arc<dyn PostgresHealthObserver> = observer.clone();
    let egress = include_egress.then(|| {
        let observer: Arc<dyn EgressRpcDependencyObserver> = observer;
        observer
    });
    Ok(InstalledContextDependencyMetrics {
        process: metrics,
        postgres,
        egress,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_observability::{ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS};

    #[test]
    fn adapter_maps_only_the_fixed_postgresql_outcome() {
        let dependencies = install_context_dependency_metrics(false).unwrap();
        assert!(dependencies.egress.is_none());
        dependencies
            .postgres
            .observe(PostgresHealthOutcome::Success);
        let metrics =
            ProcessHttpMetrics::install("context-worker", PROCESS_OBSERVABILITY_OPERATIONS)
                .unwrap()
                .with_dependency_observations(dependencies.process);
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("dependency=\"postgresql\",outcome=\"success\"} 1"));
        for line in rendered
            .lines()
            .filter(|line| line.starts_with("insight_platform_dependency_observations_total{"))
        {
            assert!(!line.contains("database="));
            assert!(!line.contains("pool="));
            assert!(!line.contains("error="));
        }
    }

    #[test]
    fn remote_adapter_adds_only_the_fixed_egress_outcome() {
        let dependencies = install_context_dependency_metrics(true).unwrap();
        dependencies
            .egress
            .unwrap()
            .observe(EgressRpcDependencyOutcome::Failure);
        let metrics =
            ProcessHttpMetrics::install("context-remote-worker", PROCESS_OBSERVABILITY_OPERATIONS)
                .unwrap()
                .with_dependency_observations(dependencies.process);
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("dependency=\"egress\",outcome=\"failure\"} 1"));
        assert!(!rendered.contains("endpoint="));
        assert!(!rendered.contains("tenant="));
        assert!(!rendered.contains("error="));
    }
}
