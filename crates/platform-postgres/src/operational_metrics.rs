//! Read-only, bounded operational observations from the durable PostgreSQL authority.

use sqlx::{PgPool, Row};

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct DurableJobQueueSnapshot {
    pub due_jobs: u64,
    pub due_oldest_age_seconds: f64,
    pub expired_leases: u64,
    pub expired_oldest_lag_seconds: f64,
}

pub async fn observe_durable_job_queue(
    pool: &PgPool,
    work_class: &str,
) -> Result<DurableJobQueueSnapshot, sqlx::Error> {
    let row = sqlx::query(
        r#"
        WITH authority_now AS (
            SELECT clock_timestamp() AS observed_at
        ), due AS (
            SELECT COUNT(*)::bigint AS count,
                   COALESCE(
                       EXTRACT(EPOCH FROM (
                           MAX(authority_now.observed_at) - MIN(COALESCE(job.retry_at, job.scheduled_at))
                       )),
                       0
                   )::double precision AS oldest_age_seconds
            FROM insight_platform.jobs AS job
            CROSS JOIN authority_now
            WHERE job.work_class = $1
              AND job.terminal_at IS NULL
              AND job.worker_id IS NULL
              AND job.state IN ('ready', 'retry_scheduled')
              AND job.scheduled_at <= authority_now.observed_at
              AND (job.retry_at IS NULL OR job.retry_at <= authority_now.observed_at)
        ), expired AS (
            SELECT COUNT(*)::bigint AS count,
                   COALESCE(
                       EXTRACT(EPOCH FROM (
                           MAX(authority_now.observed_at) - MIN(job.lease_expires_at)
                       )),
                       0
                   )::double precision AS oldest_lag_seconds
            FROM insight_platform.jobs AS job
            CROSS JOIN authority_now
            WHERE job.work_class = $1
              AND job.terminal_at IS NULL
              AND job.worker_id IS NOT NULL
              AND job.lease_expires_at <= authority_now.observed_at
        )
        SELECT due.count AS due_count,
               GREATEST(due.oldest_age_seconds, 0) AS due_oldest_age_seconds,
               expired.count AS expired_count,
               GREATEST(expired.oldest_lag_seconds, 0) AS expired_oldest_lag_seconds
        FROM due CROSS JOIN expired
        "#,
    )
    .bind(work_class)
    .fetch_one(pool)
    .await?;

    Ok(DurableJobQueueSnapshot {
        due_jobs: u64::try_from(row.try_get::<i64, _>("due_count")?).unwrap_or(u64::MAX),
        due_oldest_age_seconds: row.try_get("due_oldest_age_seconds")?,
        expired_leases: u64::try_from(row.try_get::<i64, _>("expired_count")?).unwrap_or(u64::MAX),
        expired_oldest_lag_seconds: row.try_get("expired_oldest_lag_seconds")?,
    })
}
