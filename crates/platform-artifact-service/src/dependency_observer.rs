use insight_platform_artifact_broker::{
    ArtifactExternalDependency, ArtifactExternalDependencyObserver,
    ArtifactExternalDependencyOutcome,
};
use insight_platform_observability::{
    DependencyObservationMetrics, DependencyObservationOutcome, MetricsInstallError,
    PlatformDependency,
};
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

pub fn install_artifact_dependency_metrics() -> Result<
    (
        Arc<DependencyObservationMetrics>,
        Arc<dyn ArtifactExternalDependencyObserver>,
    ),
    MetricsInstallError,
> {
    let metrics = Arc::new(DependencyObservationMetrics::install(&[
        PlatformDependency::S3,
        PlatformDependency::Kms,
    ])?);
    let observer: Arc<dyn ArtifactExternalDependencyObserver> =
        Arc::new(ArtifactDependencyObserver {
            metrics: Arc::clone(&metrics),
        });
    Ok((metrics, observer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_observability::{ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS};

    #[test]
    fn adapter_maps_only_s3_and_kms_outcomes() {
        let (dependencies, observer) = install_artifact_dependency_metrics().unwrap();
        observer.observe(
            ArtifactExternalDependency::S3,
            ArtifactExternalDependencyOutcome::Success,
        );
        observer.observe(
            ArtifactExternalDependency::Kms,
            ArtifactExternalDependencyOutcome::Failure,
        );
        let metrics =
            ProcessHttpMetrics::install("artifact-data-worker", PROCESS_OBSERVABILITY_OPERATIONS)
                .unwrap()
                .with_dependency_observations(dependencies);
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("dependency=\"s3\",outcome=\"success\"} 1"));
        assert!(rendered.contains("dependency=\"kms\",outcome=\"failure\"} 1"));
        assert!(!rendered.contains("bucket="));
        assert!(!rendered.contains("object_key="));
        assert!(!rendered.contains("storage_binding="));
    }
}
