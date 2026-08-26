use insight_platform_artifact_broker::ArtifactBrokerCapacitySnapshot;
use insight_platform_observability::{
    OperationalCapacityMetric, OperationalCapacitySnapshot, OperationalCapacitySource,
};
use std::sync::Arc;

struct ArtifactCapacityObservation<T> {
    owner: Arc<T>,
    snapshot: fn(&T) -> ArtifactBrokerCapacitySnapshot,
}

impl<T> OperationalCapacitySource for ArtifactCapacityObservation<T>
where
    T: Send + Sync + 'static,
{
    fn snapshot(&self) -> OperationalCapacitySnapshot {
        let snapshot = (self.snapshot)(&self.owner);
        OperationalCapacitySnapshot::new(
            u64::try_from(snapshot.maximum_in_flight).unwrap_or(u64::MAX),
            u64::try_from(snapshot.available).unwrap_or(u64::MAX),
        )
        .expect("Artifact broker semaphore preserves its configured capacity")
    }
}

pub(crate) fn artifact_capacity_metric<T>(
    resource: &'static str,
    owner: Arc<T>,
    snapshot: fn(&T) -> ArtifactBrokerCapacitySnapshot,
) -> OperationalCapacityMetric
where
    T: Send + Sync + 'static,
{
    let source: Arc<dyn OperationalCapacitySource> =
        Arc::new(ArtifactCapacityObservation { owner, snapshot });
    OperationalCapacityMetric::new(resource, source)
}
