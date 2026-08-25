//! Read-only, bounded operational observations from the durable PostgreSQL authority.

use sqlx::{PgPool, Row};

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct DurableJobQueueSnapshot {
    pub due_jobs: u64,
    pub due_oldest_age_seconds: f64,
    pub expired_leases: u64,
    pub expired_oldest_lag_seconds: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct DurableOutboxSnapshot {
    pub due_events: u64,
    pub due_oldest_age_seconds: f64,
    pub expired_claims: u64,
    pub expired_oldest_lag_seconds: f64,
    pub dead_events: u64,
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

pub async fn observe_durable_outbox(pool: &PgPool) -> Result<DurableOutboxSnapshot, sqlx::Error> {
    let row = sqlx::query(
        r#"
        WITH authority_now AS (
            SELECT clock_timestamp() AS observed_at
        ), due AS (
            SELECT COUNT(*)::bigint AS count,
                   COALESCE(
                       EXTRACT(EPOCH FROM (
                           MAX(authority_now.observed_at) - MIN(outbox.next_publish_at)
                       )),
                       0
                   )::double precision AS oldest_age_seconds
            FROM insight_platform.outbox_events AS outbox
            CROSS JOIN authority_now
            WHERE outbox.published_at IS NULL
              AND outbox.state = 'pending'
              AND outbox.claim_owner IS NULL
              AND outbox.next_publish_at <= authority_now.observed_at
        ), expired AS (
            SELECT COUNT(*)::bigint AS count,
                   COALESCE(
                       EXTRACT(EPOCH FROM (
                           MAX(authority_now.observed_at) - MIN(outbox.claim_expires_at)
                       )),
                       0
                   )::double precision AS oldest_lag_seconds
            FROM insight_platform.outbox_events AS outbox
            CROSS JOIN authority_now
            WHERE outbox.published_at IS NULL
              AND outbox.claim_owner IS NOT NULL
              AND outbox.claim_expires_at <= authority_now.observed_at
        ), dead AS (
            SELECT COUNT(*)::bigint AS count
            FROM insight_platform.outbox_events AS outbox
            WHERE outbox.published_at IS NULL
              AND outbox.state IN ('dead', 'cleanup_dead')
        )
        SELECT due.count AS due_count,
               GREATEST(due.oldest_age_seconds, 0) AS due_oldest_age_seconds,
               expired.count AS expired_count,
               GREATEST(expired.oldest_lag_seconds, 0) AS expired_oldest_lag_seconds,
               dead.count AS dead_count
        FROM due CROSS JOIN expired CROSS JOIN dead
        "#,
    )
    .fetch_one(pool)
    .await?;

    Ok(DurableOutboxSnapshot {
        due_events: u64::try_from(row.try_get::<i64, _>("due_count")?).unwrap_or(u64::MAX),
        due_oldest_age_seconds: row.try_get("due_oldest_age_seconds")?,
        expired_claims: u64::try_from(row.try_get::<i64, _>("expired_count")?).unwrap_or(u64::MAX),
        expired_oldest_lag_seconds: row.try_get("expired_oldest_lag_seconds")?,
        dead_events: u64::try_from(row.try_get::<i64, _>("dead_count")?).unwrap_or(u64::MAX),
    })
}
