use insight_platform_contracts::{JobKind, WorkClass};
use insight_platform_observability::{DurableJobQueueMetrics, DurableJobQueueSnapshot};
use insight_platform_postgres::operational_metrics::observe_durable_job_queue_for_kinds;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub async fn run_artifact_queue_sampler(
    pool: PgPool,
    metrics: Arc<DurableJobQueueMetrics>,
    job_kinds: &'static [JobKind],
    cancellation: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = interval.tick() => {
                match observe_durable_job_queue_for_kinds(
                    &pool,
                    WorkClass::Artifact,
                    job_kinds,
                ).await {
                    Ok(snapshot) => metrics.observe(DurableJobQueueSnapshot {
                        due_jobs: snapshot.due_jobs,
                        due_oldest_age_seconds: snapshot.due_oldest_age_seconds,
                        expired_leases: snapshot.expired_leases,
                        expired_oldest_lag_seconds: snapshot.expired_oldest_lag_seconds,
                    }),
                    Err(_) => metrics.observe_query_failure(),
                }
            }
        }
    }
}
