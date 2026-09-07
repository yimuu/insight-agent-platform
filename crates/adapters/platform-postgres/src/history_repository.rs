//! Run-root holds and monotonic public history-prefix retention.
use crate::repository::{
    append_command_event, claim_command_receipt, require_tenant_permission,
    terminalize_command_receipt, PgRepository, RepositoryError,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    CommandAudit, CommandOutcome, Permission, ResourceId, ResourceKind, TypedPayload,
};
use insight_platform_orchestrator::history::*;
use sqlx::{Postgres, Row, Transaction};
fn invalid(error: impl ToString) -> RepositoryError {
    RepositoryError::CorruptRow(error.to_string())
}
async fn lock_hold_root(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    run: &ResourceId,
    now: DateTime<Utc>,
) -> Result<RunHistoryHoldOutcome, RepositoryError> {
    let row=sqlx::query("SELECT version,history_holds FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2 FOR UPDATE").bind(tenant.to_string()).bind(run.to_string()).fetch_optional(&mut **tx).await?.ok_or(RepositoryError::NotFound("Run history root"))?;
    let holds: RunHistoryHolds =
        serde_json::from_value(row.try_get("history_holds")?).map_err(invalid)?;
    holds
        .validate_at(now)
        .map_err(|_| invalid("Run history hold structure invalid"))?;
    Ok(RunHistoryHoldOutcome {
        run_id: run.clone(),
        run_version: u64::try_from(row.try_get::<i64, _>("version")?).map_err(invalid)?,
        holds,
    })
}
async fn commit_holds(
    tx: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    current: RunHistoryHoldOutcome,
    holds: RunHistoryHolds,
    operation: &str,
    evidence: serde_json::Value,
    now: DateTime<Utc>,
) -> Result<RunHistoryHoldOutcome, RepositoryError> {
    holds.validate_at(now).map_err(|_| {
        RepositoryError::InvalidInput("Run history holds exceed bounded authority".into())
    })?;
    let version = current
        .run_version
        .checked_add(1)
        .filter(|version| *version <= i64::MAX as u64)
        .ok_or_else(|| invalid("Run history version overflow"))?;
    let payload = TypedPayload::from_versioned(1, &holds, 16384)?;
    let updated=sqlx::query("UPDATE insight_platform.runs SET history_holds=$4,version=$5,updated_at=$6 WHERE tenant_id=$1 AND run_id=$2 AND version=$3")
        .bind(audit.tenant_id.to_string()).bind(current.run_id.to_string()).bind(current.run_version as i64).bind(payload.value).bind(version as i64).bind(now).execute(&mut **tx).await?.rows_affected();
    if updated != 1 {
        return Err(RepositoryError::Conflict("Run history version"));
    }
    append_command_event(tx,audit,"run",&current.run_id.to_string(),version as i64,operation,&TypedPayload::new(1,&serde_json::json!({"command_digest":audit.request_digest,"hold_count":holds.holds.len(),"evidence":evidence}))?).await?;
    terminalize_command_receipt(tx, audit, &current.run_id.to_string(), "applied").await?;
    Ok(RunHistoryHoldOutcome {
        run_id: current.run_id,
        run_version: version,
        holds,
    })
}
impl PgRepository {
    pub async fn place_run_history_hold(
        &self,
        command: PlaceRunHistoryHold,
    ) -> Result<CommandOutcome<RunHistoryHoldOutcome>, RepositoryError> {
        let mut tx = self.pool().begin().await?;
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        command.audit.validate_at(now).map_err(invalid)?;
        if command.run_id.kind() != ResourceKind::Run
            || command.expected_run_version == 0
            || command
                .request_digest()
                .map_err(|_| invalid("invalid hold request"))?
                != command.audit.request_digest
        {
            return Err(RepositoryError::InvalidInput(
                "invalid hold placement".into(),
            ));
        }
        require_tenant_permission(&mut tx, &command.audit, Permission::HistoryHoldManage).await?;
        let current =
            lock_hold_root(&mut tx, &command.audit.tenant_id, &command.run_id, now).await?;
        if claim_command_receipt(
            &mut tx,
            &command.audit,
            "run",
            &command.run_id.to_string(),
            "run.history_hold.place",
        )
        .await?
        {
            tx.commit().await?;
            return Ok(CommandOutcome::Replayed(current));
        }
        if current.run_version != command.expected_run_version {
            return Err(RepositoryError::Conflict("Run history version"));
        }
        let mut holds = current.holds.clone();
        if holds
            .holds
            .insert(
                command.audit.request_digest.clone(),
                RunHistoryHold {
                    reason_evidence_digest: command.reason_evidence_digest.clone(),
                    placed_by: command.audit.principal_id.clone(),
                    placed_at: now,
                },
            )
            .is_some()
        {
            return Err(RepositoryError::Conflict("Run hold key"));
        }
        let next=commit_holds(&mut tx,&command.audit,current,holds,"run.history_hold.placed",serde_json::json!({"hold_key":command.audit.request_digest,"reason_evidence_digest":command.reason_evidence_digest,"placed_by":command.audit.principal_id}),now).await?;
        tx.commit().await?;
        Ok(CommandOutcome::Applied(next))
    }
    pub async fn release_run_history_hold(
        &self,
        command: ReleaseRunHistoryHold,
    ) -> Result<CommandOutcome<RunHistoryHoldOutcome>, RepositoryError> {
        let mut tx = self.pool().begin().await?;
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        command.audit.validate_at(now).map_err(invalid)?;
        if command.run_id.kind() != ResourceKind::Run
            || command.expected_run_version == 0
            || command
                .request_digest()
                .map_err(|_| invalid("invalid hold release request"))?
                != command.audit.request_digest
        {
            return Err(RepositoryError::InvalidInput("invalid hold release".into()));
        }
        require_tenant_permission(&mut tx, &command.audit, Permission::HistoryHoldManage).await?;
        let current =
            lock_hold_root(&mut tx, &command.audit.tenant_id, &command.run_id, now).await?;
        if claim_command_receipt(
            &mut tx,
            &command.audit,
            "run",
            &command.run_id.to_string(),
            "run.history_hold.release",
        )
        .await?
        {
            tx.commit().await?;
            return Ok(CommandOutcome::Replayed(current));
        }
        if current.run_version != command.expected_run_version {
            return Err(RepositoryError::Conflict("Run history version"));
        }
        let mut holds = current.holds.clone();
        let released = holds
            .holds
            .remove(&command.hold_key)
            .ok_or(RepositoryError::NotFound("Run history hold"))?;
        let next=commit_holds(&mut tx,&command.audit,current,holds,"run.history_hold.released",serde_json::json!({"hold_key":command.hold_key,"placed_by":released.placed_by,"reason_evidence_digest":released.reason_evidence_digest,"release_evidence_digest":command.release_evidence_digest}),now).await?;
        tx.commit().await?;
        Ok(CommandOutcome::Applied(next))
    }
    /// Called by the restricted maintenance role with deployment-verified policy.
    /// The caller chooses a bounded target, never its own retention time cutoff.
    pub async fn purge_public_run_event_prefix(
        &self,
        command: PurgePublicRunEventPrefix,
        policy: &HistoryRetentionPolicy,
    ) -> Result<PublicRunRetentionOutcome, RepositoryError> {
        policy.validate().map_err(|_| {
            RepositoryError::InvalidInput("invalid signed history retention policy".into())
        })?;
        let mut tx = self.pool().begin().await?;
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        command.validate_at(now).map_err(|_| {
            RepositoryError::InvalidInput("invalid Run history purge target".into())
        })?;
        let snapshot: Option<serde_json::Value> =
            sqlx::query_scalar("SELECT insight_platform.history_lock_run($1,$2)")
                .bind(command.tenant_id.to_string())
                .bind(command.run_id.to_string())
                .fetch_one(&mut *tx)
                .await?;
        let snapshot: HistoryMaintenanceSnapshot =
            serde_json::from_value(snapshot.ok_or(RepositoryError::NotFound("Run history root"))?)
                .map_err(invalid)?;
        let floor = snapshot.public_replay_floor;
        let high = snapshot.public_sequence;
        if floor > high || high > i64::MAX as u64 || command.through_sequence > high {
            return Err(RepositoryError::InvalidInput(
                "Run history target exceeds authority".into(),
            ));
        }
        let mut outcome = PublicRunRetentionOutcome {
            previous_floor: floor,
            replay_floor: floor,
            deleted_events: 0,
            target_reached: floor >= command.through_sequence,
        };
        if outcome.target_reached
            || snapshot.hold_count > 0
            || snapshot.terminal_at.is_none()
            || snapshot.active_work_count != 0
            || snapshot.live_jobs > 0
            || snapshot.live_invocations > 0
            || snapshot.sandbox_cleanup_obligations > 0
            || snapshot.pending_tasks > 0
            || snapshot.cleanup_states.len() >= 32
            || snapshot
                .cleanup_states
                .iter()
                .any(|cleanup| cleanup.state != "succeeded" || !cleanup.has_deletion_proof)
        {
            tx.commit().await?;
            return Ok(outcome);
        }
        let rows =
            sqlx::query("SELECT * FROM insight_platform.history_lock_event_prefix($1,$2,$3,$4,$5)")
                .bind(command.tenant_id.to_string())
                .bind(command.run_id.to_string())
                .bind(floor as i64)
                .bind(command.through_sequence as i64)
                .bind(i32::from(command.maximum_events))
                .fetch_all(&mut *tx)
                .await?;
        let event_before = now
            - Duration::seconds(
                policy
                    .public_event_minimum_seconds
                    .max(policy.audit_event_minimum_seconds)
                    .max(policy.receipt_minimum_seconds) as i64,
            );
        let outbox_before = now - Duration::seconds(policy.published_outbox_minimum_seconds as i64);
        for event in rows {
            let sequence =
                u64::try_from(event.try_get::<i64, _>("public_sequence")?).map_err(invalid)?;
            if sequence != outcome.replay_floor + 1 {
                return Err(invalid("Run history has an unexplained non-prefix gap"));
            }
            if event.try_get::<DateTime<Utc>, _>("occurred_at")? > event_before {
                break;
            }
            let event_id: ResourceId = event
                .try_get::<String, _>("event_id")?
                .parse()
                .map_err(invalid)?;
            let references = crate::history_retirement_repository::event_obligations(
                &mut tx,
                &command.tenant_id,
                &event_id,
                policy,
            )
            .await?;
            if references.retained_reason(now).is_some() {
                break;
            }
            let aggregate_id: ResourceId = event
                .try_get::<String, _>("aggregate_id")?
                .parse()
                .map_err(invalid)?;
            let owners = crate::history_retirement_repository::inspect_history_owners(
                &mut tx,
                &command.tenant_id,
                vec![aggregate_id, command.run_id.clone()],
                false,
                false,
            )
            .await?;
            if owners.reason.is_some()
                || (references.receipt_until.is_some()
                    && owners.deadline.is_some_and(|deadline| deadline > now))
            {
                break;
            }

            if event.try_get::<bool, _>("outbox_exists")?
                && (event
                    .try_get::<Option<String>, _>("outbox_state")?
                    .as_deref()
                    != Some("published")
                    || event
                        .try_get::<Option<DateTime<Utc>>, _>("published_at")?
                        .is_none_or(|published| published > outbox_before))
            {
                break;
            }
            outcome.replay_floor = sequence;
            outcome.deleted_events += 1;
        }
        if outcome.replay_floor > floor {
            let advanced: i64 =
                sqlx::query_scalar("SELECT insight_platform.history_delete_prefix($1,$2,$3,$4)")
                    .bind(command.tenant_id.to_string())
                    .bind(command.run_id.to_string())
                    .bind(floor as i64)
                    .bind(outcome.replay_floor as i64)
                    .fetch_one(&mut *tx)
                    .await?;
            if advanced != outcome.replay_floor as i64 {
                return Err(RepositoryError::Conflict("Run history floor"));
            }
        }
        outcome.target_reached = outcome.replay_floor >= command.through_sequence;
        tx.commit().await?;
        Ok(outcome)
    }
}

impl PgRepository {
    /// Restricted maintenance discovery. A finite creation cohort persists in the
    /// caller's typed cursor; every purge rechecks current Run holds and obligations.
    pub async fn scan_history_retention_runs(
        &self,
        command: ScanHistoryRetentionRuns,
    ) -> Result<HistoryRetentionRunPage, RepositoryError> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *tx)
            .await?;
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        command
            .validate_at(now)
            .map_err(|_| RepositoryError::InvalidInput("invalid history retention scan".into()))?;
        let cursor = command.cursor;
        let value: serde_json::Value =
            sqlx::query_scalar("SELECT insight_platform.history_scan_runs($1,$2,$3,$4,$5,$6)")
                .bind(cursor.as_ref().map(|cursor| cursor.creation_cutoff))
                .bind(
                    cursor
                        .as_ref()
                        .map(|cursor| cursor.upper.tenant_id.to_string()),
                )
                .bind(
                    cursor
                        .as_ref()
                        .map(|cursor| cursor.upper.run_id.to_string()),
                )
                .bind(
                    cursor
                        .as_ref()
                        .map(|cursor| cursor.after.tenant_id.to_string()),
                )
                .bind(
                    cursor
                        .as_ref()
                        .map(|cursor| cursor.after.run_id.to_string()),
                )
                .bind(i32::from(command.maximum_runs))
                .fetch_one(&mut *tx)
                .await?;
        let snapshot: HistoryScanSnapshot = serde_json::from_value(value).map_err(invalid)?;
        let observed_after: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        if snapshot.rows.len() > usize::from(command.maximum_runs)
            || snapshot.creation_cutoff > observed_after
        {
            return Err(invalid("history scan primitive exceeded its bound"));
        }
        let upper = match (snapshot.upper_tenant, snapshot.upper_run) {
            (Some(tenant_id), Some(run_id)) => Some(HistoryRetentionRunKey { tenant_id, run_id }),
            (None, None) if snapshot.rows.is_empty() => None,
            _ => return Err(invalid("history scan primitive cursor mismatch")),
        };
        let mut candidates = Vec::new();
        let mut last = None;
        let row_count = snapshot.rows.len();
        for row in snapshot.rows {
            if row.public_replay_floor > row.public_sequence
                || row.public_sequence > i64::MAX as u64
            {
                return Err(invalid("invalid retention watermarks"));
            }
            if row.terminal_at.is_some()
                && row.active_work_count == 0
                && row.public_sequence > row.public_replay_floor
            {
                candidates.push(HistoryRetentionRunCandidate {
                    tenant_id: row.tenant_id.clone(),
                    run_id: row.run_id.clone(),
                    through_sequence: row.public_sequence,
                });
            }
            last = Some(HistoryRetentionRunKey {
                tenant_id: row.tenant_id,
                run_id: row.run_id,
            });
        }
        let next_cursor = last
            .zip(upper)
            .filter(|(last, upper)| row_count == usize::from(command.maximum_runs) && last != upper)
            .map(|(after, upper)| HistoryRetentionCursor {
                schema_version: 1,
                creation_cutoff: snapshot.creation_cutoff,
                upper,
                after,
            });
        tx.commit().await?;
        Ok(HistoryRetentionRunPage {
            candidates,
            next_cursor,
        })
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryMaintenanceSnapshot {
    #[serde(rename = "version")]
    _version: u64,
    public_replay_floor: u64,
    public_sequence: u64,
    terminal_at: Option<DateTime<Utc>>,
    active_work_count: u64,
    hold_count: u64,
    live_jobs: u64,
    live_invocations: u64,
    sandbox_cleanup_obligations: u64,
    pending_tasks: u64,
    cleanup_states: Vec<CleanupStateFact>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CleanupStateFact {
    state: String,
    has_deletion_proof: bool,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryScanSnapshot {
    creation_cutoff: DateTime<Utc>,
    upper_tenant: Option<ResourceId>,
    upper_run: Option<ResourceId>,
    rows: Vec<HistoryScanRun>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryScanRun {
    tenant_id: ResourceId,
    run_id: ResourceId,
    public_sequence: u64,
    public_replay_floor: u64,
    terminal_at: Option<DateTime<Utc>>,
    active_work_count: u64,
}

/// Provisioning-only grant script; runtime callers do not execute role DDL.
pub fn history_role_grants_sql() -> &'static str {
    include_str!("../history-role-grants.sql")
}
