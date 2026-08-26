use insight_platform_observability::{
    DependencyObservationMetrics, DependencyObservationOutcome, MetricsInstallError,
    PlatformDependency,
};
use insight_platform_postgres::dependency_health::{PostgresHealthObserver, PostgresHealthOutcome};
use std::sync::Arc;

struct CapabilityPostgresObserver {
    metrics: Arc<DependencyObservationMetrics>,
}

impl PostgresHealthObserver for CapabilityPostgresObserver {
    fn observe(&self, outcome: PostgresHealthOutcome) {
        self.metrics
            .observe(
                PlatformDependency::Postgresql,
                match outcome {
                    PostgresHealthOutcome::Success => DependencyObservationOutcome::Success,
                    PostgresHealthOutcome::Failure => DependencyObservationOutcome::Failure,
                },
            )
            .expect("Capability roles install their PostgreSQL dependency metric");
    }
}

pub struct InstalledCapabilityDependencyMetrics {
    pub process: Arc<DependencyObservationMetrics>,
    pub postgres: Arc<dyn PostgresHealthObserver>,
}

pub fn install_capability_dependency_metrics(
) -> Result<InstalledCapabilityDependencyMetrics, MetricsInstallError> {
    let metrics = Arc::new(DependencyObservationMetrics::install(&[
        PlatformDependency::Postgresql,
    ])?);
    let postgres: Arc<dyn PostgresHealthObserver> = Arc::new(CapabilityPostgresObserver {
        metrics: Arc::clone(&metrics),
    });
    Ok(InstalledCapabilityDependencyMetrics {
        process: metrics,
        postgres,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_observability::{ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS};

    #[test]
    fn adapter_maps_only_the_fixed_postgresql_outcome() {
        let dependencies = install_capability_dependency_metrics().unwrap();
        dependencies
            .postgres
            .observe(PostgresHealthOutcome::Failure);
        let metrics =
            ProcessHttpMetrics::install("capability-worker", PROCESS_OBSERVABILITY_OPERATIONS)
                .unwrap()
                .with_dependency_observations(dependencies.process);
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("dependency=\"postgresql\",outcome=\"failure\"} 1"));
        for line in rendered
            .lines()
            .filter(|line| line.starts_with("insight_platform_dependency_observations_total{"))
        {
            assert!(!line.contains("database="));
            assert!(!line.contains("pool="));
            assert!(!line.contains("error="));
        }
    }
}
