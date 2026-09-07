//! Sampling local permits into a bounded telemetry projection. Metrics never authorize work.
use crate::LocalWorkerPools;
use insight_platform_observability::{WorkerPermitMetrics, WorkerPermitSnapshot};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
pub fn update_worker_permits(metrics: &WorkerPermitMetrics, pools: &LocalWorkerPools) {
    let snapshot = pools.snapshot();
    metrics.update(WorkerPermitSnapshot {
        business_capacity: u64::try_from(snapshot.business_capacity).unwrap_or(u64::MAX),
        business_available: u64::try_from(snapshot.business_available).unwrap_or(u64::MAX),
        critical_control_capacity: u64::try_from(snapshot.critical_control_capacity)
            .unwrap_or(u64::MAX),
        critical_control_available: u64::try_from(snapshot.critical_control_available)
            .unwrap_or(u64::MAX),
    });
}

pub async fn run_worker_permit_sampler(
    metrics: Arc<WorkerPermitMetrics>,
    pools: LocalWorkerPools,
    cancellation: CancellationToken,
) {
    update_worker_permits(&metrics, &pools);
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = interval.tick() => update_worker_permits(&metrics, &pools),
        }
    }
}
