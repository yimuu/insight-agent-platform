//! Isolated retirement adapter. SQL provides locks and safe metadata; Rust owns
//! policy, current obligations, and complete cleanup-chain decisions.
use crate::repository::{PgRepository, RepositoryError};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{canonical_digest, ResourceId, ResourceKind, Sha256Digest};
use insight_platform_mcp_host::{
    validate_mcp_oauth_cleanup_chain, McpOAuthCleanupChainIdentity, McpOAuthCleanupChainJob,
};
use insight_platform_orchestrator::history::{retirement::*, HistoryRetentionPolicy};
use serde::Deserialize;
use sqlx::{Postgres, Transaction};
use std::collections::{BTreeSet, VecDeque};

fn corrupt(error: impl ToString) -> RepositoryError {
    RepositoryError::CorruptRow(error.to_string())
}
fn decode<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> Result<T, RepositoryError> {
    serde_json::from_value(value).map_err(corrupt)
}
fn busy(error: &RepositoryError) -> bool {
    matches!(error, RepositoryError::Database(sqlx::Error::Database(db)) if db.code().as_deref()==Some("55P03"))
}
fn retained(reason: HistoryRetainedReason) -> HistoryRetirementOutcome {
    HistoryRetirementOutcome::Retained(reason)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScanSnapshot {
    schema_version: u32,
    creation_cutoff: DateTime<Utc>,
    upper_tenant: Option<ResourceId>,
    upper_id: Option<ResourceId>,
    rows: Vec<HistoryRecordKey>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptMetadata {
    receipt_id: ResourceId,
    receipt_kind: String,
    scope_kind: String,
    scope_id: ResourceId,
    response_reference_id: Option<ResourceId>,
    request_digest: Sha256Digest,
    state: String,
    created_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    expires_at: DateTime<Utc>,
    claim_expires_at: Option<DateTime<Utc>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerMetadata {
    schema_version: u32,
    object_kind: String,
    object_id: ResourceId,
    related_ids: Vec<ResourceId>,
    state: Option<String>,
    terminal_at: Option<DateTime<Utc>>,
    deadline: Option<DateTime<Utc>>,
    active_work: bool,
    hold_count: u64,
    cleanup_required: bool,
    live_jobs: bool,
    live_invocations: bool,
    pending_tasks: bool,
}
#[derive(Default)]
pub(crate) struct OwnerInspection {
    pub(crate) reason: Option<HistoryRetainedReason>,
    pub(crate) deadline: Option<DateTime<Utc>>,
    ids: Vec<ResourceId>,
}
/// Only safe root metadata is read; no Task OAuth body or invocation body leaves
/// the owning SQL implementation. Cyclic current references are visited once.
pub(crate) async fn inspect_history_owners(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    roots: Vec<ResourceId>,
    allow_absent: bool,
    check_delivery: bool,
) -> Result<OwnerInspection, RepositoryError> {
    let mut queue = VecDeque::from(roots);
    let mut seen = BTreeSet::new();
    let mut result = OwnerInspection::default();
    while let Some(id) = queue.pop_front() {
        if !seen.insert(id.clone()) {
            continue;
        }
        if seen.len() > MAX_HISTORY_OWNER_ROOTS {
            return Err(corrupt("history owner closure exceeds bound"));
        }
        let value: serde_json::Value =
            sqlx::query_scalar("SELECT insight_platform.history_lock_owner($1,$2)")
                .bind(tenant.to_string())
                .bind(id.to_string())
                .fetch_one(&mut **tx)
                .await?;
        let fact: OwnerMetadata = decode(value)?;
        if fact.schema_version != 1
            || fact.object_id != id
            || fact.related_ids.len() > MAX_HISTORY_OWNER_ROOTS
        {
            return Err(corrupt("invalid history owner metadata"));
        }
        if fact.object_kind == "artifact" {
            let state: insight_platform_contracts::ArtifactState = fact
                .state
                .as_deref()
                .ok_or_else(|| corrupt("Artifact retention state missing"))?
                .parse()
                .map_err(corrupt)?;
            if matches!(
                state,
                insight_platform_contracts::ArtifactState::Staging
                    | insight_platform_contracts::ArtifactState::Uploaded
                    | insight_platform_contracts::ArtifactState::Verifying
                    | insight_platform_contracts::ArtifactState::Deleting
            ) {
                result
                    .reason
                    .get_or_insert(HistoryRetainedReason::ActiveEffect);
            }
        }
        result.deadline = result.deadline.max(fact.deadline);
        result.ids.push(id);
        if fact.object_kind == "absent" && !allow_absent {
            result.reason = Some(HistoryRetainedReason::UnknownScope);
        }
        if fact.hold_count > 0 {
            result.reason = Some(HistoryRetainedReason::Hold);
        }
        if fact.cleanup_required || fact.state.as_deref() == Some("reconciliation_required") {
            result.reason = Some(HistoryRetainedReason::UnknownEffect);
        }
        if fact.active_work
            || fact.live_jobs
            || fact.live_invocations
            || fact.pending_tasks
            || (matches!(
                fact.object_kind.as_str(),
                "run" | "job" | "invocation" | "task"
            ) && fact.terminal_at.is_none())
        {
            result
                .reason
                .get_or_insert(HistoryRetainedReason::ActiveEffect);
        }
        queue.extend(fact.related_ids.into_iter().filter(|related| {
            !(fact.object_kind == "task" && related.kind() == ResourceKind::McpAuthorizationBinding)
        }));
    }
    if check_delivery && !result.ids.is_empty() {
        let pending: bool =
            sqlx::query_scalar("SELECT insight_platform.history_owner_delivery($1,$2)")
                .bind(tenant.to_string())
                .bind(
                    result
                        .ids
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>(),
                )
                .fetch_one(&mut **tx)
                .await?;
        if pending {
            result.reason.get_or_insert(HistoryRetainedReason::Delivery);
        }
    }
    Ok(result)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EventMetadata {
    event_id: ResourceId,
    aggregate_id: ResourceId,
    aggregate_kind: String,
    event_type: String,
    run_id: Option<ResourceId>,
    occurred_at: DateTime<Utc>,
    payload_digest: Sha256Digest,
    outbox_exists: bool,
    outbox_state: Option<String>,
    published_at: Option<DateTime<Utc>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EventObligations {
    pub(crate) receipt_until: Option<DateTime<Utc>>,
    pub(crate) processing_receipt: bool,
    pub(crate) source_reference: bool,
    pub(crate) provenance_reference: bool,
    pub(crate) installation_reference: bool,
}
impl EventObligations {
    pub(crate) fn retained_reason(&self, now: DateTime<Utc>) -> Option<HistoryRetainedReason> {
        if self.processing_receipt {
            Some(HistoryRetainedReason::ActiveEffect)
        } else if self.receipt_until.is_some_and(|until| until > now) {
            Some(HistoryRetainedReason::Window)
        } else if self.source_reference || self.provenance_reference || self.installation_reference
        {
            Some(HistoryRetainedReason::Reference)
        } else {
            None
        }
    }
}
pub(crate) async fn event_obligations(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    event: &ResourceId,
    policy: &HistoryRetentionPolicy,
) -> Result<EventObligations, RepositoryError> {
    let value: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT insight_platform.history_event_obligations($1,$2,$3)")
            .bind(tenant.to_string())
            .bind(event.to_string())
            .bind(policy.receipt_minimum_seconds as i64)
            .fetch_one(&mut **tx)
            .await?;
    decode(value.ok_or_else(|| corrupt("history Event disappeared under lock"))?)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskMetadata {
    schema_version: u32,
    state: String,
    version: u64,
    owner_id: ResourceId,
    run_id: Option<ResourceId>,
    invocation_id: Option<ResourceId>,
    response_value_id: Option<ResourceId>,
    responded_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
    deadline: DateTime<Utc>,
    identity: serde_json::Value,
    chain: Vec<McpOAuthCleanupChainJob>,
    valid_structure: bool,
    receipt_reference: bool,
    artifact_reference: bool,
    other_job_reference: bool,
}
impl PgRepository {
    pub async fn scan_history_retirement(
        &self,
        command: ScanHistoryRetirement,
    ) -> Result<HistoryRetirementPage, RepositoryError> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *tx)
            .await?;
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        command
            .validate_at(now)
            .map_err(|e| RepositoryError::InvalidInput(e.into()))?;
        let cursor = command.cursor;
        let value: serde_json::Value = sqlx::query_scalar(
            "SELECT insight_platform.history_scan_records($1,$2,$3,$4,$5,$6,$7)",
        )
        .bind(command.lane.as_str())
        .bind(cursor.as_ref().map(|c| c.creation_cutoff))
        .bind(cursor.as_ref().map(|c| c.upper.tenant_id.to_string()))
        .bind(cursor.as_ref().map(|c| c.upper.record_id.to_string()))
        .bind(cursor.as_ref().map(|c| c.after.tenant_id.to_string()))
        .bind(cursor.as_ref().map(|c| c.after.record_id.to_string()))
        .bind(i32::from(command.maximum_records))
        .fetch_one(&mut *tx)
        .await?;
        let snapshot: ScanSnapshot = decode(value)?;
        if snapshot.schema_version != 1
            || snapshot.rows.len() > usize::from(command.maximum_records)
        {
            return Err(corrupt("invalid retirement scan metadata"));
        }
        let upper = match (snapshot.upper_tenant, snapshot.upper_id) {
            (Some(tenant_id), Some(record_id)) => Some(HistoryRecordKey {
                tenant_id,
                record_id,
            }),
            (None, None) => None,
            _ => return Err(corrupt("partial retirement upper bound")),
        };
        let checked_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        if snapshot.creation_cutoff > checked_at {
            return Err(corrupt("history scan cutoff is in the future"));
        }
        let tuple = |key: &HistoryRecordKey| (key.tenant_id.to_string(), key.record_id.to_string());
        let mut previous = cursor.as_ref().map(|cursor| tuple(&cursor.after));
        for key in &snapshot.rows {
            let current = tuple(key);
            if key.tenant_id.kind() != ResourceKind::Tenant
                || !command.lane.accepts(&key.record_id)
                || previous
                    .as_ref()
                    .is_some_and(|previous| &current <= previous)
                || upper.as_ref().is_none_or(|upper| current > tuple(upper))
            {
                return Err(corrupt("history scan row exceeds ordered nominal cursor"));
            }
            previous = Some(current);
        }
        if upper.as_ref().is_some_and(|key| {
            key.tenant_id.kind() != ResourceKind::Tenant || !command.lane.accepts(&key.record_id)
        }) {
            return Err(corrupt("invalid history scan upper identity"));
        }
        let next_cursor = upper.and_then(|upper| {
            snapshot
                .rows
                .last()
                .filter(|last| *last != &upper)
                .map(|last| HistoryRetirementCursor {
                    schema_version: 1,
                    lane: command.lane,
                    creation_cutoff: snapshot.creation_cutoff,
                    upper,
                    after: last.clone(),
                })
        });
        tx.commit().await?;
        Ok(HistoryRetirementPage {
            candidates: snapshot.rows,
            next_cursor,
        })
    }
    pub async fn retire_history_record(
        &self,
        lane: HistoryRetirementLane,
        key: HistoryRecordKey,
        policy: &HistoryRetentionPolicy,
    ) -> Result<HistoryRetirementOutcome, RepositoryError> {
        policy.validate().map_err(|_| {
            RepositoryError::InvalidInput("invalid signed history retention policy".into())
        })?;
        if key.tenant_id.kind() != ResourceKind::Tenant || !lane.accepts(&key.record_id) {
            return Err(RepositoryError::InvalidInput(
                "invalid retirement identity".into(),
            ));
        }
        let mut tx = self.pool().begin().await?;
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        let result = match lane {
            HistoryRetirementLane::Receipt => retire_receipt(&mut tx, &key, policy, now).await,
            HistoryRetirementLane::EventDelivery => retire_event(&mut tx, &key, policy, now).await,
            HistoryRetirementLane::OAuthTask => retire_task(&mut tx, &key, policy, now).await,
        };
        match result {
            Ok(outcome) => {
                tx.commit().await?;
                Ok(outcome)
            }
            Err(error) if busy(&error) => {
                tx.rollback().await?;
                Ok(retained(HistoryRetainedReason::Busy))
            }
            Err(error) => Err(error),
        }
    }
}
async fn retire_receipt(
    tx: &mut Transaction<'_, Postgres>,
    key: &HistoryRecordKey,
    policy: &HistoryRetentionPolicy,
    now: DateTime<Utc>,
) -> Result<HistoryRetirementOutcome, RepositoryError> {
    let value: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT insight_platform.history_lock_receipt($1,$2)")
            .bind(key.tenant_id.to_string())
            .bind(key.record_id.to_string())
            .fetch_one(&mut **tx)
            .await?;
    let Some(value) = value else {
        return Ok(HistoryRetirementOutcome::AlreadyAbsent);
    };
    let row: ReceiptMetadata = decode(value)?;
    if !known_receipt_scope(&row.receipt_kind, &row.scope_kind) {
        return Ok(retained(HistoryRetainedReason::UnknownScope));
    }
    if row.state != "succeeded" || row.completed_at.is_none() {
        return Ok(retained(HistoryRetainedReason::ActiveEffect));
    }
    let mut roots = vec![row.scope_id];
    roots.extend(row.response_reference_id);
    let inspected = inspect_history_owners(tx, &key.tenant_id, roots, false, true).await?;
    if let Some(reason) = inspected.reason {
        return Ok(retained(reason));
    }
    let window = HistoryReceiptWindow {
        created_at: row.created_at,
        completed_at: row.completed_at,
        expires_at: row.expires_at,
        claim_expires_at: row.claim_expires_at,
        owner_deadline: inspected.deadline,
    };
    if window.ends_at(policy).is_none_or(|end| end > now) {
        return Ok(retained(HistoryRetainedReason::Window));
    }
    let deleted: bool =
        sqlx::query_scalar("SELECT insight_platform.history_delete_receipt($1,$2,$3)")
            .bind(key.tenant_id.to_string())
            .bind(row.receipt_id.to_string())
            .bind(row.request_digest.to_string())
            .fetch_one(&mut **tx)
            .await?;
    if !deleted {
        return Err(RepositoryError::Conflict("history Receipt fence"));
    }
    Ok(HistoryRetirementOutcome::Retired {
        receipts: 1,
        events: 0,
        outbox: 0,
        tasks: 0,
        jobs: 0,
    })
}
async fn retire_event(
    tx: &mut Transaction<'_, Postgres>,
    key: &HistoryRecordKey,
    policy: &HistoryRetentionPolicy,
    now: DateTime<Utc>,
) -> Result<HistoryRetirementOutcome, RepositoryError> {
    let value: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT insight_platform.history_lock_event($1,$2)")
            .bind(key.tenant_id.to_string())
            .bind(key.record_id.to_string())
            .fetch_one(&mut **tx)
            .await?;
    let Some(value) = value else {
        return Ok(HistoryRetirementOutcome::AlreadyAbsent);
    };
    let row: EventMetadata = decode(value)?;
    let mut outbox = 0;
    if row.outbox_exists {
        let Some(published) = row.published_at.filter(|at| {
            row.outbox_state.as_deref() == Some("published")
                && *at <= now - Duration::seconds(policy.published_outbox_minimum_seconds as i64)
        }) else {
            return Ok(retained(HistoryRetainedReason::Delivery));
        };
        let deleted: bool =
            sqlx::query_scalar("SELECT insight_platform.history_delete_published_outbox($1,$2,$3)")
                .bind(key.tenant_id.to_string())
                .bind(row.event_id.to_string())
                .bind(published)
                .fetch_one(&mut **tx)
                .await?;
        outbox = u16::from(deleted);
    }
    let delivery_only = |reason| {
        if outbox > 0 {
            HistoryRetirementOutcome::Retired {
                receipts: 0,
                events: 0,
                outbox,
                tasks: 0,
                jobs: 0,
            }
        } else {
            retained(reason)
        }
    };
    if row.run_id.is_some()
        || row.occurred_at > now - Duration::seconds(policy.audit_event_minimum_seconds as i64)
    {
        return Ok(delivery_only(HistoryRetainedReason::Window));
    }
    let obligations = event_obligations(tx, &key.tenant_id, &row.event_id, policy).await?;
    if let Some(reason) = obligations.retained_reason(now) {
        return Ok(delivery_only(reason));
    }
    let absent_retired_oauth = matches!(row.aggregate_kind.as_str(), "mcp_oauth_task" | "job")
        && matches!(
            row.event_type.as_str(),
            "mcp.oauth_authorization_started"
                | "mcp.oauth_authorization_completed"
                | "mcp.oauth_authorization_declined"
                | "mcp.oauth_authorization_expired"
                | "mcp.pkce.cleanup_started"
                | "mcp.pkce.cleanup_settled"
                | "mcp.pkce.cleanup_lease_recovered"
                | "mcp.pkce.cleanup_recovered"
                | "mcp.pkce.cleanup_retired"
        );
    let inspected = inspect_history_owners(
        tx,
        &key.tenant_id,
        vec![row.aggregate_id],
        absent_retired_oauth,
        false,
    )
    .await?;
    if let Some(reason) = inspected.reason {
        return Ok(delivery_only(reason));
    }
    if obligations.receipt_until.is_some()
        && inspected.deadline.is_some_and(|deadline| deadline > now)
    {
        return Ok(delivery_only(HistoryRetainedReason::Window));
    }
    let deleted: bool =
        sqlx::query_scalar("SELECT insight_platform.history_delete_event($1,$2,$3)")
            .bind(key.tenant_id.to_string())
            .bind(row.event_id.to_string())
            .bind(row.payload_digest.to_string())
            .fetch_one(&mut **tx)
            .await?;
    if !deleted {
        return Err(RepositoryError::Conflict("history Event fence"));
    }
    Ok(HistoryRetirementOutcome::Retired {
        receipts: 0,
        events: 1,
        outbox,
        tasks: 0,
        jobs: 0,
    })
}
async fn retire_task(
    tx: &mut Transaction<'_, Postgres>,
    key: &HistoryRecordKey,
    policy: &HistoryRetentionPolicy,
    now: DateTime<Utc>,
) -> Result<HistoryRetirementOutcome, RepositoryError> {
    let value: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT insight_platform.history_lock_task_chain($1,$2)")
            .bind(key.tenant_id.to_string())
            .bind(key.record_id.to_string())
            .fetch_one(&mut **tx)
            .await?;
    let Some(value) = value else {
        return Ok(HistoryRetirementOutcome::AlreadyAbsent);
    };
    let evidence = serde_json::json!({"schema_version":1,"identity":value["identity"].clone(),"chain":value["chain"].clone()});
    let row: TaskMetadata = decode(value)?;
    if row.schema_version != 1 {
        return Err(corrupt("unknown OAuth retirement metadata"));
    }
    if !matches!(row.state.as_str(), "responded" | "declined" | "expired")
        || row.responded_at.is_none()
    {
        return Ok(retained(HistoryRetainedReason::ActiveEffect));
    }
    if !row.valid_structure {
        return Err(corrupt("incomplete OAuth cleanup chain"));
    }
    let identity: McpOAuthCleanupChainIdentity = decode(row.identity.clone())?;
    validate_mcp_oauth_cleanup_chain(&identity, &row.chain).map_err(corrupt)?;
    let current = &row.chain[0];
    if current.state != insight_platform_contracts::JobState::Succeeded
        || current.terminal_at.is_none()
        || current.payload.deletion_proof.is_none()
    {
        return Ok(retained(HistoryRetainedReason::UnknownEffect));
    }
    let latest = row
        .chain
        .iter()
        .fold(row.updated_at.max(row.deadline), |until, job| {
            until
                .max(job.deadline)
                .max(job.terminal_at.unwrap_or(job.created_at))
        });
    if latest
        > now
            - Duration::seconds(
                policy
                    .cleanup_minimum_seconds
                    .max(policy.audit_event_minimum_seconds) as i64,
            )
    {
        return Ok(retained(HistoryRetainedReason::Window));
    }
    if row.receipt_reference
        || row.artifact_reference
        || row.other_job_reference
        || row.run_id.is_some()
        || row.invocation_id.is_some()
        || row.response_value_id.is_some()
    {
        return Ok(retained(HistoryRetainedReason::Reference));
    }
    if row.owner_id.kind() != ResourceKind::McpAuthorizationBinding {
        return Err(corrupt(
            "OAuth Task owner is not its prospective authorization identity",
        ));
    }
    let mut roots = vec![key.record_id.clone()];
    roots.extend(row.chain.iter().map(|job| job.job_id.clone()));
    let inspected = inspect_history_owners(tx, &key.tenant_id, roots, false, true).await?;
    if let Some(reason) = inspected.reason {
        return Ok(retained(reason));
    }
    let digest = canonical_digest(&evidence).map_err(corrupt)?;
    let event =
        ResourceId::from_uuid_v7(ResourceKind::Event, uuid::Uuid::now_v7()).map_err(corrupt)?;
    let outbox = ResourceId::from_uuid_v7(ResourceKind::OutboxEvent, uuid::Uuid::now_v7())
        .map_err(corrupt)?;
    let deleted: i32 = sqlx::query_scalar(
        "SELECT insight_platform.history_retire_oauth_chain($1,$2,$3,$4,$5,$6,$7)",
    )
    .bind(key.tenant_id.to_string())
    .bind(key.record_id.to_string())
    .bind(row.version as i64)
    .bind(identity.current_job_id.to_string())
    .bind(event.to_string())
    .bind(outbox.to_string())
    .bind(digest)
    .fetch_one(&mut **tx)
    .await?;
    Ok(HistoryRetirementOutcome::Retired {
        receipts: 0,
        events: 0,
        outbox: 0,
        tasks: u16::from(deleted > 0),
        jobs: u16::try_from(deleted).map_err(corrupt)?,
    })
}
