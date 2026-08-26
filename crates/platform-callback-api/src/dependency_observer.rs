use insight_platform_observability::{
    DependencyObservationMetrics, DependencyObservationOutcome, MetricsInstallError,
    PlatformDependency,
};
use insight_platform_postgres::dependency_health::{PostgresHealthObserver, PostgresHealthOutcome};
use std::sync::Arc;

struct CallbackPostgresObserver(Arc<DependencyObservationMetrics>);

impl PostgresHealthObserver for CallbackPostgresObserver {
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

pub fn install_postgres_dependency_metrics() -> Result<
    (
        Arc<DependencyObservationMetrics>,
        Arc<dyn PostgresHealthObserver>,
    ),
    MetricsInstallError,
> {
    let metrics = Arc::new(DependencyObservationMetrics::install(&[
        PlatformDependency::Postgresql,
    ])?);
    let observer: Arc<dyn PostgresHealthObserver> =
        Arc::new(CallbackPostgresObserver(Arc::clone(&metrics)));
    Ok((metrics, observer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_observability::{ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS};

    #[test]
    fn adapter_maps_only_fixed_postgresql_outcomes() {
        let (dependencies, observer) = install_postgres_dependency_metrics().unwrap();
        observer.observe(PostgresHealthOutcome::Success);
        let rendered =
            ProcessHttpMetrics::install("mcp-callback-api", PROCESS_OBSERVABILITY_OPERATIONS)
                .unwrap()
                .with_dependency_observations(dependencies)
                .render_prometheus();
        assert!(rendered.contains("dependency=\"postgresql\",outcome=\"success\"} 1"));
        assert!(!rendered.contains("database="));
        assert!(!rendered.contains("pool="));
        assert!(!rendered.contains("error="));
    }
}
