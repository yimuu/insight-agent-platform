//! PostgreSQL-only authority for committed-event delivery. Event bodies are never selected.

use crate::repository::{PgRepository, RepositoryError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    ClaimDueCommittedEvents, ClaimedCommittedEvent, CommittedEventNoticeV1, OutboxBacklogWatermark,
    OutboxClaimFence, OutboxFailureCode, OutboxSettlement, ResourceId, UtcTimestamp,
    COMMITTED_EVENT_NOTICE_VERSION, MAX_OUTBOX_BACKLOG_SCAN,
};
use insight_platform_worker::outbox::{OutboxDeliveryError, OutboxDeliveryStore};
use sqlx::{postgres::PgRow, Postgres, Row, Transaction};

/// Admission pressure gate, not a second global capacity counter. Concurrent admissions may
/// observe the same watermark. Existing work, replay and recovery must remain able to finish.
pub(crate) async fn require_new_work_capacity(
    transaction: &mut Transaction<'_, Postgres>,
    threshold: u32,
) -> Result<(), RepositoryError> {
    let observed = observe_outbox_backlog_in_transaction(transaction, threshold).await?;
    if observed.limit_reached {
        Err(RepositoryError::CapacityUnavailable)
    } else {
        Ok(())
    }
}

/// Shared bounded watermark SQL for read-only observation and an admission transaction. Includes
/// incompatible obligations; a full scan window conservatively means capacity is exhausted.
/// PostgreSQL callers retain exact transaction aborts. Only the external delivery port maps
/// database failures to its safe `Unavailable` outcome.
pub async fn observe_outbox_backlog_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    maximum_scan_rows: u32,
) -> Result<OutboxBacklogWatermark, RepositoryError> {
    if maximum_scan_rows == 0 || maximum_scan_rows > MAX_OUTBOX_BACKLOG_SCAN {
        return Err(RepositoryError::InvalidInput(
            "invalid Outbox watermark bound".into(),
        ));
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (SELECT 1 FROM insight_platform.outbox_events WHERE published_at IS NULL LIMIT $1) AS obligations",
    ).bind(i64::from(maximum_scan_rows)).fetch_one(&mut **transaction).await?;
    let observed_undelivered = u32::try_from(count)
        .map_err(|_| RepositoryError::CorruptRow("invalid Outbox watermark count".into()))?;
    Ok(OutboxBacklogWatermark {
        observed_undelivered,
        scan_limit: maximum_scan_rows,
        limit_reached: observed_undelivered >= maximum_scan_rows,
    })
}

#[async_trait]
impl OutboxDeliveryStore for PgRepository {
    async fn claim_due_committed_events(
        &self,
        command: ClaimDueCommittedEvents,
    ) -> Result<Vec<ClaimedCommittedEvent>, OutboxDeliveryError> {
        command
            .validate()
            .map_err(|_| OutboxDeliveryError::InvalidCommand)?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| OutboxDeliveryError::Unavailable)?;
        sqlx::query("SET LOCAL statement_timeout = '5s'")
            .execute(&mut *transaction)
            .await
            .map_err(|_| OutboxDeliveryError::Unavailable)?;
        sqlx::query("SET LOCAL lock_timeout = '1s'")
            .execute(&mut *transaction)
            .await
            .map_err(|_| OutboxDeliveryError::Unavailable)?;
        let rows = sqlx::query(r#"
            WITH candidates AS (
                SELECT tenant_id, outbox_id FROM insight_platform.outbox_events
                WHERE published_at IS NULL AND next_publish_at <= clock_timestamp()
                  AND ((state IN ('pending', 'retry') AND claim_owner IS NULL AND claim_expires_at IS NULL)
                       OR (state = 'publishing' AND claim_expires_at <= clock_timestamp()))
                  AND claim_epoch < 9223372036854775807
                ORDER BY next_publish_at, tenant_id, outbox_id
                FOR UPDATE SKIP LOCKED LIMIT $2
            ), claimed AS (
                UPDATE insight_platform.outbox_events AS outbox
                SET state = 'publishing', claim_owner = $1, claim_epoch = claim_epoch + 1,
                    claim_expires_at = clock_timestamp() + ($3::bigint * interval '1 millisecond'),
                    publish_attempts = LEAST(publish_attempts::bigint + 1, 2147483647)::integer,
                    updated_at = clock_timestamp()
                FROM candidates
                WHERE outbox.tenant_id = candidates.tenant_id AND outbox.outbox_id = candidates.outbox_id
                RETURNING outbox.tenant_id, outbox.outbox_id, outbox.event_id, outbox.claim_epoch, outbox.publish_attempts
            )
            SELECT claimed.*, event.aggregate_id, event.aggregate_version, event.run_id,
                   event.public_sequence, event.trace_id, event.occurred_at
            FROM claimed JOIN insight_platform.events AS event
              ON event.tenant_id = claimed.tenant_id AND event.event_id = claimed.event_id
            ORDER BY claimed.tenant_id, claimed.outbox_id
        "#).bind(command.process_generation.to_string()).bind(i64::from(command.maximum_claims))
            .bind(command.lease_milliseconds as i64).fetch_all(&mut *transaction).await
            .map_err(|_| OutboxDeliveryError::Unavailable)?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            match decode_claim(&row, &command.process_generation) {
                Ok(claim) => claims.push(claim),
                Err(_) => {
                    let tenant: String = row
                        .try_get("tenant_id")
                        .map_err(|_| OutboxDeliveryError::Incompatible)?;
                    let outbox: String = row
                        .try_get("outbox_id")
                        .map_err(|_| OutboxDeliveryError::Incompatible)?;
                    sqlx::query(
                        r#"UPDATE insight_platform.outbox_events
                        SET state = 'incompatible', claim_owner = NULL, claim_expires_at = NULL,
                            last_failure_code = $3, updated_at = clock_timestamp()
                        WHERE tenant_id = $1 AND outbox_id = $2 AND claim_owner = $4"#,
                    )
                    .bind(tenant)
                    .bind(outbox)
                    .bind(OutboxFailureCode::ContractIncompatible.as_str())
                    .bind(command.process_generation.to_string())
                    .execute(&mut *transaction)
                    .await
                    .map_err(|_| OutboxDeliveryError::Unavailable)?;
                }
            }
        }
        transaction
            .commit()
            .await
            .map_err(|_| OutboxDeliveryError::Unavailable)?;
        Ok(claims)
    }

    async fn settle_committed_event(
        &self,
        fence: &OutboxClaimFence,
        settlement: OutboxSettlement,
    ) -> Result<bool, OutboxDeliveryError> {
        fence
            .validate()
            .map_err(|_| OutboxDeliveryError::InvalidCommand)?;
        settlement
            .validate()
            .map_err(|_| OutboxDeliveryError::InvalidCommand)?;
        let (state, failure, delay) = match settlement {
            OutboxSettlement::Published => ("published", None, 0),
            OutboxSettlement::Retry {
                failure,
                delay_milliseconds,
            } => ("retry", Some(failure.as_str()), delay_milliseconds),
            OutboxSettlement::Incompatible => (
                "incompatible",
                Some(OutboxFailureCode::ContractIncompatible.as_str()),
                0,
            ),
        };
        let result = sqlx::query(
            r#"
            UPDATE insight_platform.outbox_events
            SET state = $6, last_failure_code = $7, claim_owner = NULL, claim_expires_at = NULL,
                next_publish_at = clock_timestamp() + ($8::bigint * interval '1 millisecond'),
                published_at = CASE WHEN $6 = 'published' THEN clock_timestamp() ELSE NULL END,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND outbox_id = $2 AND event_id = $3
              AND claim_owner = $4 AND claim_epoch = $5 AND state = 'publishing'
              AND claim_expires_at > clock_timestamp() AND published_at IS NULL
        "#,
        )
        .bind(fence.tenant_id.to_string())
        .bind(fence.outbox_id.to_string())
        .bind(fence.event_id.to_string())
        .bind(fence.process_generation.to_string())
        .bind(fence.epoch as i64)
        .bind(state)
        .bind(failure)
        .bind(delay as i64)
        .execute(self.pool())
        .await
        .map_err(|_| OutboxDeliveryError::Unavailable)?;
        Ok(result.rows_affected() == 1)
    }

    async fn observe_outbox_backlog(
        &self,
        maximum_scan_rows: u32,
    ) -> Result<OutboxBacklogWatermark, OutboxDeliveryError> {
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| OutboxDeliveryError::Unavailable)?;
        let watermark = observe_outbox_backlog_in_transaction(&mut transaction, maximum_scan_rows)
            .await
            .map_err(|error| match error {
                RepositoryError::InvalidInput(_) => OutboxDeliveryError::InvalidCommand,
                RepositoryError::CorruptRow(_) => OutboxDeliveryError::Incompatible,
                _ => OutboxDeliveryError::Unavailable,
            })?;
        transaction
            .commit()
            .await
            .map_err(|_| OutboxDeliveryError::Unavailable)?;
        Ok(watermark)
    }
}

fn decode_claim(
    row: &PgRow,
    process_generation: &ResourceId,
) -> Result<ClaimedCommittedEvent, OutboxDeliveryError> {
    let id = |column| -> Result<ResourceId, OutboxDeliveryError> {
        row.try_get::<String, _>(column)
            .map_err(|_| OutboxDeliveryError::Incompatible)?
            .parse()
            .map_err(|_| OutboxDeliveryError::Incompatible)
    };
    let version = |column| -> Result<Option<u64>, OutboxDeliveryError> {
        row.try_get::<Option<i64>, _>(column)
            .map_err(|_| OutboxDeliveryError::Incompatible)?
            .map(|value| u64::try_from(value).map_err(|_| OutboxDeliveryError::Incompatible))
            .transpose()
    };
    let tenant_id = id("tenant_id")?;
    let event_id = id("event_id")?;
    let run_id = row
        .try_get::<Option<String>, _>("run_id")
        .map_err(|_| OutboxDeliveryError::Incompatible)?
        .map(|value| value.parse().map_err(|_| OutboxDeliveryError::Incompatible))
        .transpose()?;
    let claim = ClaimedCommittedEvent {
        fence: OutboxClaimFence {
            tenant_id: tenant_id.clone(),
            outbox_id: id("outbox_id")?,
            event_id: event_id.clone(),
            process_generation: process_generation.clone(),
            epoch: u64::try_from(
                row.try_get::<i64, _>("claim_epoch")
                    .map_err(|_| OutboxDeliveryError::Incompatible)?,
            )
            .map_err(|_| OutboxDeliveryError::Incompatible)?,
        },
        notice: CommittedEventNoticeV1 {
            schema_version: COMMITTED_EVENT_NOTICE_VERSION,
            tenant_id,
            event_id,
            aggregate_id: id("aggregate_id")?,
            aggregate_version: version("aggregate_version")?,
            run_id,
            public_sequence: version("public_sequence")?,
            trace_id: row
                .try_get::<String, _>("trace_id")
                .map_err(|_| OutboxDeliveryError::Incompatible)?
                .parse()
                .map_err(|_| OutboxDeliveryError::Incompatible)?,
            occurred_at: UtcTimestamp::from_datetime(
                row.try_get::<DateTime<Utc>, _>("occurred_at")
                    .map_err(|_| OutboxDeliveryError::Incompatible)?,
            ),
        },
        publish_attempts: u32::try_from(
            row.try_get::<i32, _>("publish_attempts")
                .map_err(|_| OutboxDeliveryError::Incompatible)?,
        )
        .map_err(|_| OutboxDeliveryError::Incompatible)?,
    };
    claim
        .validate()
        .map_err(|_| OutboxDeliveryError::Incompatible)?;
    Ok(claim)
}
/// Provisioning text belongs to the PostgreSQL adapter; callers never reach into its tree.
pub fn outbox_role_grants_sql() -> &'static str {
    include_str!("../outbox-role-grants.sql")
}
