use insight_platform_artifact_broker::{
    ArtifactExternalDependency, ArtifactExternalDependencyObserver,
    ArtifactExternalDependencyOutcome,
};
use insight_platform_observability::{
    DependencyObservationMetrics, DependencyObservationOutcome, MetricsInstallError,
    PlatformDependency,
};
use insight_platform_postgres::dependency_health::{PostgresHealthObserver, PostgresHealthOutcome};
use std::sync::Arc;

struct ArtifactDependencyObserver {
    metrics: Arc<DependencyObservationMetrics>,
}

impl ArtifactExternalDependencyObserver for ArtifactDependencyObserver {
    fn observe(
        &self,
        dependency: ArtifactExternalDependency,
        outcome: ArtifactExternalDependencyOutcome,
    ) {
        let dependency = match dependency {
            ArtifactExternalDependency::S3 => PlatformDependency::S3,
            ArtifactExternalDependency::Kms => PlatformDependency::Kms,
        };
        let outcome = match outcome {
            ArtifactExternalDependencyOutcome::Success => DependencyObservationOutcome::Success,
            ArtifactExternalDependencyOutcome::Failure => DependencyObservationOutcome::Failure,
        };
        self.metrics
            .observe(dependency, outcome)
            .expect("Artifact roles install S3 and KMS dependency metrics");
    }
}

impl PostgresHealthObserver for ArtifactDependencyObserver {
    fn observe(&self, outcome: PostgresHealthOutcome) {
        self.metrics
            .observe(
                PlatformDependency::Postgresql,
                match outcome {
                    PostgresHealthOutcome::Success => DependencyObservationOutcome::Success,
                    PostgresHealthOutcome::Failure => DependencyObservationOutcome::Failure,
                },
            )
            .expect("Artifact roles install their PostgreSQL dependency metric");
    }
}

pub struct InstalledArtifactDependencyMetrics {
    pub process: Arc<DependencyObservationMetrics>,
    pub artifact: Arc<dyn ArtifactExternalDependencyObserver>,
    pub postgres: Arc<dyn PostgresHealthObserver>,
}

pub fn install_artifact_dependency_metrics(
) -> Result<InstalledArtifactDependencyMetrics, MetricsInstallError> {
    let metrics = Arc::new(DependencyObservationMetrics::install(&[
        PlatformDependency::S3,
        PlatformDependency::Kms,
        PlatformDependency::Postgresql,
    ])?);
    let observer = Arc::new(ArtifactDependencyObserver {
        metrics: Arc::clone(&metrics),
    });
    let artifact_observer: Arc<dyn ArtifactExternalDependencyObserver> = observer.clone();
    let postgres_observer: Arc<dyn PostgresHealthObserver> = observer;
    Ok(InstalledArtifactDependencyMetrics {
        process: metrics,
        artifact: artifact_observer,
        postgres: postgres_observer,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_observability::{ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS};

    #[test]
    fn adapter_maps_only_s3_and_kms_outcomes() {
        let dependencies = install_artifact_dependency_metrics().unwrap();
        dependencies.artifact.observe(
            ArtifactExternalDependency::S3,
            ArtifactExternalDependencyOutcome::Success,
        );
        dependencies
            .postgres
            .observe(PostgresHealthOutcome::Success);
        dependencies.artifact.observe(
            ArtifactExternalDependency::Kms,
            ArtifactExternalDependencyOutcome::Failure,
        );
        let metrics =
            ProcessHttpMetrics::install("artifact-data-worker", PROCESS_OBSERVABILITY_OPERATIONS)
                .unwrap()
                .with_dependency_observations(dependencies.process);
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("dependency=\"s3\",outcome=\"success\"} 1"));
        assert!(rendered.contains("dependency=\"kms\",outcome=\"failure\"} 1"));
        assert!(rendered.contains("dependency=\"postgresql\",outcome=\"success\"} 1"));
        assert!(!rendered.contains("bucket="));
        assert!(!rendered.contains("object_key="));
        assert!(!rendered.contains("storage_binding="));
    }
}
