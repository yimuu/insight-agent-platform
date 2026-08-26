use insight_platform_egress_rpc::{EgressRpcDependencyObserver, EgressRpcDependencyOutcome};
use insight_platform_model_worker::{ModelNatsDependencyObserver, ModelNatsDependencyOutcome};
use insight_platform_observability::{
    DependencyObservationMetrics, DependencyObservationOutcome, MetricsInstallError,
    PlatformDependency,
};
use insight_platform_postgres::dependency_health::{PostgresHealthObserver, PostgresHealthOutcome};
use std::sync::Arc;

struct ModelDependencyObserver {
    metrics: Arc<DependencyObservationMetrics>,
}

impl ModelNatsDependencyObserver for ModelDependencyObserver {
    fn observe(&self, outcome: ModelNatsDependencyOutcome) {
        observe(
            &self.metrics,
            PlatformDependency::Nats,
            matches!(outcome, ModelNatsDependencyOutcome::Success),
        );
    }
}

impl PostgresHealthObserver for ModelDependencyObserver {
    fn observe(&self, outcome: PostgresHealthOutcome) {
        observe(
            &self.metrics,
            PlatformDependency::Postgresql,
            matches!(outcome, PostgresHealthOutcome::Success),
        );
    }
}

impl EgressRpcDependencyObserver for ModelDependencyObserver {
    fn observe(&self, outcome: EgressRpcDependencyOutcome) {
        observe(
            &self.metrics,
            PlatformDependency::Egress,
            matches!(outcome, EgressRpcDependencyOutcome::Success),
        );
    }
}

fn observe(metrics: &DependencyObservationMetrics, dependency: PlatformDependency, success: bool) {
    metrics
        .observe(
            dependency,
            if success {
                DependencyObservationOutcome::Success
            } else {
                DependencyObservationOutcome::Failure
            },
        )
        .expect("Model Worker installs every dependency accepted by its observer");
}

pub struct InstalledModelDependencyMetrics {
    pub process: Arc<DependencyObservationMetrics>,
    pub nats: Arc<dyn ModelNatsDependencyObserver>,
    pub postgres: Arc<dyn PostgresHealthObserver>,
    pub egress: Arc<dyn EgressRpcDependencyObserver>,
}

pub fn install_model_dependency_metrics(
) -> Result<InstalledModelDependencyMetrics, MetricsInstallError> {
    let metrics = Arc::new(DependencyObservationMetrics::install(&[
        PlatformDependency::Postgresql,
        PlatformDependency::Nats,
        PlatformDependency::Egress,
    ])?);
    let observer = Arc::new(ModelDependencyObserver {
        metrics: Arc::clone(&metrics),
    });
    let nats: Arc<dyn ModelNatsDependencyObserver> = observer.clone();
    let postgres: Arc<dyn PostgresHealthObserver> = observer.clone();
    let egress: Arc<dyn EgressRpcDependencyObserver> = observer;
    Ok(InstalledModelDependencyMetrics {
        process: metrics,
        nats,
        postgres,
        egress,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_observability::{ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS};

    #[test]
    fn adapter_maps_only_fixed_nats_and_postgresql_outcomes() {
        let dependencies = install_model_dependency_metrics().unwrap();
        dependencies
            .nats
            .observe(ModelNatsDependencyOutcome::Failure);
        dependencies
            .postgres
            .observe(PostgresHealthOutcome::Success);
        dependencies
            .egress
            .observe(EgressRpcDependencyOutcome::Failure);
        let metrics = ProcessHttpMetrics::install("model-worker", PROCESS_OBSERVABILITY_OPERATIONS)
            .unwrap()
            .with_dependency_observations(dependencies.process);
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("dependency=\"nats\",outcome=\"failure\"} 1"));
        assert!(rendered.contains("dependency=\"postgresql\",outcome=\"success\"} 1"));
        assert!(rendered.contains("dependency=\"egress\",outcome=\"failure\"} 1"));
        assert!(!rendered.contains("server="));
        assert!(!rendered.contains("subject="));
        assert!(!rendered.contains("tenant="));
        assert!(!rendered.contains("error="));
    }
}
