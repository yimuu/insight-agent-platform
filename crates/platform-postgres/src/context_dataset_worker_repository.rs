//! Bounded owner recovery for the independent Context Dataset Builder lane.

use crate::repository::{
    append_scheduler_event_with_trace, decode_versioned_payload, job_from_row, job_projection,
    PgRepository, RepositoryError, TypedPayload,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_context::ContextDatasetBuildJobPayload;
use insight_platform_contracts::{JobState, ResourceId, ResourceKind, Sha256Digest};
use insight_platform_jobs::decide_expired_lease;
use uuid::Uuid;

impl PgRepository {
    /// Recovers only Dataset build Jobs supported by this process' exact source closure.
    ///
    /// A pre-start expired lease returns to `ready`; an expired running attempt gets a bounded
    /// retry. Exhausted or deadline-crossed work becomes `reconciliation_required` because an
    /// Artifact stage may have committed while its RPC response was lost.
    pub async fn recover_expired_context_dataset_build_jobs_for_sources(
        &self,
        limit: u16,
        retry_backoff_milliseconds: i64,
        source_binding_digests: &[Sha256Digest],
    ) -> Result<u64, RepositoryError> {
        let supported = validated_supported_digests(source_binding_digests)?;
        if limit == 0
            || limit > 256
            || retry_backoff_milliseconds <= 0
            || retry_backoff_milliseconds > 60_000
        {
            return Err(RepositoryError::InvalidInput(
                "Context Dataset recovery bounds are invalid".to_owned(),
            ));
        }
        let mut transaction = self.pool().begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let rows = sqlx::query(
            r#"
            SELECT *
            FROM insight_platform.jobs
            WHERE work_class = 'context' AND job_kind = 'context_dataset_build'
              AND owner_kind = 'context_dataset' AND owner_id <> ''
              AND state IN ('leased', 'running') AND terminal_at IS NULL
              AND lease_expires_at <= $1
              AND payload #>> '{source_binding,canonical_digest}' = ANY($2::text[])
            ORDER BY lease_expires_at, tenant_id, job_id
            FOR UPDATE SKIP LOCKED
            LIMIT $3
            "#,
        )
        .bind(database_now)
        .bind(&supported)
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await?;
        let mut recovered = 0_u64;
        for row in rows {
            let current = job_from_row(row)?;
            let owner = ResourceId::parse_expected(&current.owner_id, ResourceKind::ContextDataset)
                .map_err(|_| {
                    RepositoryError::CorruptRow(
                        "Context Dataset recovery owner is invalid".to_owned(),
                    )
                })?;
            let payload: ContextDatasetBuildJobPayload =
                decode_versioned_payload(&current.payload, "Context Dataset build Job")?;
            payload.validate_for_owner(&owner)?;
            if payload.job_id.to_string() != current.job_id
                || !supported
                    .iter()
                    .any(|digest| digest == payload.source_binding.canonical_digest.as_str())
            {
                return Err(RepositoryError::CorruptRow(
                    "Context Dataset recovery closure drifted".to_owned(),
                ));
            }
            let projection = job_projection(&current)?;
            let retry_at = database_now
                .checked_add_signed(Duration::milliseconds(retry_backoff_milliseconds))
                .ok_or_else(|| {
                    RepositoryError::InvalidInput(
                        "Context Dataset recovery retry time overflowed".to_owned(),
                    )
                })?;
            let target =
                if projection.state == JobState::Leased && database_now < projection.deadline {
                    JobState::Ready
                } else if projection.state == JobState::Running
                    && projection.attempt_count < projection.attempt_limit
                    && retry_at < projection.deadline
                {
                    JobState::RetryScheduled
                } else {
                    JobState::ReconciliationRequired
                };
            let next_retry_at = (target == JobState::RetryScheduled).then_some(retry_at);
            let next = decide_expired_lease(
                &projection,
                u64::try_from(current.version).map_err(|_| {
                    RepositoryError::CorruptRow("negative Context Dataset Job version".to_owned())
                })?,
                u64::try_from(current.lease_epoch).map_err(|_| {
                    RepositoryError::CorruptRow(
                        "negative Context Dataset lease generation".to_owned(),
                    )
                })?,
                database_now,
                target,
                next_retry_at,
            )?;
            let evidence = TypedPayload::new(
                1,
                &serde_json::json!({
                    "attempt_count": next.attempt_count,
                    "job_id": current.job_id,
                    "observed_lease_generation": current.lease_epoch,
                    "recovered_state": next.state,
                }),
            )?;
            let terminal_at =
                (next.state == JobState::ReconciliationRequired).then_some(database_now);
            let result_digest = terminal_at.map(|_| evidence.digest.clone());
            let next_version = i64::try_from(next.version).map_err(|_| {
                RepositoryError::InvalidInput(
                    "Context Dataset Job version exceeds bigint".to_owned(),
                )
            })?;
            let affected = sqlx::query(
                r#"
                UPDATE insight_platform.jobs
                SET state = $4, version = $5, worker_id = NULL,
                    lease_token_digest = NULL, lease_expires_at = NULL,
                    heartbeat_at = NULL, retry_at = $6,
                    started_at = CASE WHEN $4 IN ('ready', 'retry_scheduled')
                                      THEN NULL ELSE started_at END,
                    result_digest = $7, terminal_at = $8, updated_at = $9
                WHERE tenant_id = $1 AND job_id = $2 AND version = $3
                  AND work_class = 'context' AND job_kind = 'context_dataset_build'
                  AND owner_kind = 'context_dataset' AND state IN ('leased', 'running')
                "#,
            )
            .bind(&current.tenant_id)
            .bind(&current.job_id)
            .bind(current.version)
            .bind(next.state.as_str())
            .bind(next_version)
            .bind(next.retry_at)
            .bind(result_digest)
            .bind(terminal_at)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if affected != 1 {
                return Err(RepositoryError::Conflict(
                    "expired Context Dataset build Job",
                ));
            }
            let event_id = ResourceId::from_uuid_v7(ResourceKind::Event, Uuid::now_v7())
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
            let outbox_id = ResourceId::from_uuid_v7(ResourceKind::OutboxEvent, Uuid::now_v7())
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
            append_scheduler_event_with_trace(
                &mut transaction,
                current.trace,
                &current.tenant_id,
                &event_id,
                &outbox_id,
                "job",
                &current.job_id,
                next_version,
                None,
                if next.state == JobState::ReconciliationRequired {
                    "context.dataset_build_reconciliation_required"
                } else {
                    "context.dataset_build_lease_recovered"
                },
                &evidence,
            )
            .await?;
            recovered = recovered.checked_add(1).ok_or_else(|| {
                RepositoryError::InvalidInput(
                    "Context Dataset recovery count overflowed".to_owned(),
                )
            })?;
        }
        transaction.commit().await?;
        Ok(recovered)
    }
}

fn validated_supported_digests(
    source_binding_digests: &[Sha256Digest],
) -> Result<Vec<String>, RepositoryError> {
    if source_binding_digests.is_empty() || source_binding_digests.len() > 64 {
        return Err(RepositoryError::InvalidInput(
            "Context Dataset recovery source closure is invalid".to_owned(),
        ));
    }
    let mut supported = source_binding_digests
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    supported.sort();
    supported.dedup();
    if supported.len() != source_binding_digests.len() {
        return Err(RepositoryError::InvalidInput(
            "Context Dataset recovery source closure contains duplicates".to_owned(),
        ));
    }
    Ok(supported)
}
