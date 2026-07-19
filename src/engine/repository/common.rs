use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::engine::{
    AdmissionState, ContentHash, EventSeq, ExecutionEventEnvelope, ExecutionEventId, IntentHash,
    PendingExecutionEvent, PublicEventContext, PublicEventEnvelope, PublicEventId, PublicEventKind,
    PublicEventPayload, RunId, RunLifecycle, TransitionKey, EXECUTION_EVENT_SCHEMA_VERSION,
    PUBLIC_EVENT_SCHEMA_VERSION,
};

use super::{RepositoryError, REPOSITORY_DATA_INVALID};

pub(crate) fn canonical_intent_hash<T>(intent: &T) -> Result<IntentHash, RepositoryError>
where
    T: Serialize + ?Sized,
{
    IntentHash::from_serializable(intent).map_err(|_| RepositoryError::canonicalization())
}

pub(crate) fn canonical_json(value: &Value) -> Result<String, RepositoryError> {
    serde_jcs::to_string(value).map_err(|_| RepositoryError::canonicalization())
}

/// Decodes the schema discriminator on every persisted execution-event row
/// before any of that row's authority is used. Database constraints prevent
/// new unknown rows, while this decoder fails closed for legacy corruption,
/// disabled constraints, snapshots, and future writers.
pub(crate) fn decode_execution_event_schema_version(stored: i64) -> Result<(), RepositoryError> {
    let version = u32::try_from(stored).map_err(|_| RepositoryError::invalid_data())?;
    if version != EXECUTION_EVENT_SCHEMA_VERSION {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

/// Backend-neutral persisted row used by the one closed execution-event
/// decoder. Repositories must not independently infer payload/context
/// validity from a subset of columns.
pub(crate) struct StoredExecutionEventRow {
    pub schema_version: i64,
    pub event_id: String,
    pub run_id: String,
    pub transition_key: String,
    pub intent_hash: String,
    pub seq: i64,
    pub occurred_at: DateTime<Utc>,
    pub kind: String,
    pub node_id: Option<String>,
    pub scope_instance_id: Option<String>,
    pub activation_id: Option<String>,
    pub attempt_no: Option<i64>,
    pub causation_event_id: Option<String>,
    pub safe_payload: Value,
}

/// Reconstructs the canonical typed envelope so serde's closed payload wire,
/// event-kind equality, identifier parsing, context-level validation, and
/// causation checks all run at the repository boundary. This is deliberately
/// Rust-owned rather than duplicating every payload rule in SQL triggers.
pub(crate) fn decode_stored_execution_event(
    stored: StoredExecutionEventRow,
) -> Result<ExecutionEventEnvelope, RepositoryError> {
    decode_execution_event_schema_version(stored.schema_version)?;
    let schema_version =
        u32::try_from(stored.schema_version).map_err(|_| RepositoryError::invalid_data())?;
    serde_json::from_value(serde_json::json!({
        "schema_version": schema_version,
        "event_id": stored.event_id,
        "run_id": stored.run_id,
        "transition_key": stored.transition_key,
        "intent_hash": stored.intent_hash,
        "seq": stored.seq,
        "occurred_at": stored.occurred_at,
        "kind": stored.kind,
        "node_id": stored.node_id,
        "scope_instance_id": stored.scope_instance_id,
        "activation_id": stored.activation_id,
        "attempt_no": stored.attempt_no,
        "causation_event_id": stored.causation_event_id,
        "payload": stored.safe_payload,
    }))
    .map_err(|_| RepositoryError::invalid_data())
}

/// Permanent public-projection decision plus the optional receipt/outbox
/// witnesses observed at a repository boundary. All backend-specific SQL
/// decoders normalize into this one closed authority contract.
pub(crate) struct StoredPublicProjectionDecision {
    pub run_id: String,
    pub execution_event_id: String,
    pub execution_seq: i64,
    pub execution_occurred_at: DateTime<Utc>,
    pub execution_transition_key: String,
    pub decision: String,
    pub public_event_id: Option<String>,
    pub public_ordinal: Option<i64>,
    pub public_schema_version: Option<i64>,
    pub event_kind: Option<String>,
    pub is_terminal: Option<bool>,
    pub receipt_public_event_id: Option<String>,
    pub receipt_causation_event_id: Option<String>,
    pub receipt_public_ordinal: Option<i64>,
    pub receipt_public_schema_version: Option<i64>,
    pub receipt_event_kind: Option<String>,
    pub receipt_is_terminal: Option<bool>,
    pub outbox_public_event_id: Option<String>,
    pub outbox_causation_event_id: Option<String>,
    pub outbox_public_ordinal: Option<i64>,
    pub outbox_public_schema_version: Option<i64>,
    pub outbox_event_kind: Option<String>,
    pub outbox_is_terminal: Option<bool>,
}

pub(crate) fn decode_public_event_kind(value: &str) -> Result<PublicEventKind, RepositoryError> {
    match value {
        "run.created" => Ok(PublicEventKind::RunCreated),
        "run.started" => Ok(PublicEventKind::RunStarted),
        "operation.started" => Ok(PublicEventKind::OperationStarted),
        "operation.completed" => Ok(PublicEventKind::OperationCompleted),
        "operation.failed" => Ok(PublicEventKind::OperationFailed),
        "run.completed" => Ok(PublicEventKind::RunCompleted),
        "run.failed" => Ok(PublicEventKind::RunFailed),
        "run.cancelled" => Ok(PublicEventKind::RunCancelled),
        "run.interrupted" => Ok(PublicEventKind::RunInterrupted),
        _ => Err(RepositoryError::invalid_data()),
    }
}

pub(crate) const fn public_event_is_terminal(kind: PublicEventKind) -> bool {
    matches!(
        kind,
        PublicEventKind::RunCompleted
            | PublicEventKind::RunFailed
            | PublicEventKind::RunCancelled
            | PublicEventKind::RunInterrupted
    )
}

/// Decodes None versus Some without treating a missing prunable outbox body as
/// evidence. A public decision requires an exact permanent receipt; an outbox
/// witness is optional only because a published nonterminal body may expire.
pub(crate) fn decode_public_projection_decision(
    stored: StoredPublicProjectionDecision,
    execution: &ExecutionEventEnvelope,
) -> Result<Option<String>, RepositoryError> {
    if execution.run_id().as_str() != stored.run_id
        || execution.event_id().as_str() != stored.execution_event_id
        || i64_from_u64(execution.seq().get())? != stored.execution_seq
        || execution.occurred_at() != &stored.execution_occurred_at
        || execution.transition_key().as_str() != stored.execution_transition_key
    {
        return Err(RepositoryError::invalid_data());
    }

    let no_public_metadata = stored.public_event_id.is_none()
        && stored.public_ordinal.is_none()
        && stored.public_schema_version.is_none()
        && stored.event_kind.is_none()
        && stored.is_terminal.is_none();
    let no_receipt = stored.receipt_public_event_id.is_none()
        && stored.receipt_causation_event_id.is_none()
        && stored.receipt_public_ordinal.is_none()
        && stored.receipt_public_schema_version.is_none()
        && stored.receipt_event_kind.is_none()
        && stored.receipt_is_terminal.is_none();
    let no_outbox = stored.outbox_public_event_id.is_none()
        && stored.outbox_causation_event_id.is_none()
        && stored.outbox_public_ordinal.is_none()
        && stored.outbox_public_schema_version.is_none()
        && stored.outbox_event_kind.is_none()
        && stored.outbox_is_terminal.is_none();
    if stored.decision == "none" {
        return if no_public_metadata && no_receipt && no_outbox {
            Ok(None)
        } else {
            Err(RepositoryError::invalid_data())
        };
    }
    if stored.decision != "public" || no_public_metadata {
        return Err(RepositoryError::invalid_data());
    }

    let stored_public_event_id = stored
        .public_event_id
        .as_deref()
        .ok_or_else(RepositoryError::invalid_data)?;
    let event_kind = stored
        .event_kind
        .as_deref()
        .ok_or_else(RepositoryError::invalid_data)?;
    let kind = decode_public_event_kind(event_kind)?;
    let ordinal = stored
        .public_ordinal
        .ok_or_else(RepositoryError::invalid_data)?;
    let schema_version = stored
        .public_schema_version
        .ok_or_else(RepositoryError::invalid_data)?;
    let terminal = stored
        .is_terminal
        .ok_or_else(RepositoryError::invalid_data)?;
    let run_id = RunId::new(stored.run_id.clone()).map_err(|_| RepositoryError::invalid_data())?;
    let expected_id = public_event_id(&run_id, execution.transition_key(), kind);
    if schema_version != i64::from(PUBLIC_EVENT_SCHEMA_VERSION)
        || ordinal != i64::from(public_event_ordinal(kind))
        || terminal != public_event_is_terminal(kind)
        || stored_public_event_id != expected_id
        || stored.receipt_public_event_id.as_deref() != Some(stored_public_event_id)
        || stored.receipt_causation_event_id.as_deref() != Some(execution.event_id().as_str())
        || stored.receipt_public_ordinal != Some(ordinal)
        || stored.receipt_public_schema_version != Some(schema_version)
        || stored.receipt_event_kind.as_deref() != Some(event_kind)
        || stored.receipt_is_terminal != Some(terminal)
    {
        return Err(RepositoryError::invalid_data());
    }
    if !no_outbox
        && (stored.outbox_public_event_id.as_deref() != Some(stored_public_event_id)
            || stored.outbox_causation_event_id.as_deref() != Some(execution.event_id().as_str())
            || stored.outbox_public_ordinal != Some(ordinal)
            || stored.outbox_public_schema_version != Some(schema_version)
            || stored.outbox_event_kind.as_deref() != Some(event_kind)
            || stored.outbox_is_terminal != Some(terminal))
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(Some(stored_public_event_id.to_owned()))
}

/// Builds the one canonical public wire envelope from the committed execution
/// event authority. In particular, `occurred_at` must be the timestamp returned
/// by the execution-event INSERT in the same transaction.
pub(crate) fn durable_public_event_envelope(
    run_id: &RunId,
    public_event_id: &str,
    causation_event_id: &str,
    seq: u64,
    occurred_at: DateTime<Utc>,
    payload: PublicEventPayload,
) -> Result<PublicEventEnvelope, RepositoryError> {
    let public_event_id = PublicEventId::parse(public_event_id.to_owned())
        .map_err(|_| RepositoryError::invalid_data())?;
    let causation_event_id = ExecutionEventId::parse(causation_event_id.to_owned())
        .map_err(|_| RepositoryError::invalid_data())?;
    let seq = EventSeq::new(seq).map_err(|_| RepositoryError::invalid_data())?;
    Ok(PublicEventEnvelope::new(
        PublicEventContext::new(public_event_id, run_id.clone(), causation_event_id),
        seq,
        occurred_at,
        payload,
    ))
}

/// Stable idempotency identity shared by direct loser delivery and the
/// startup/runtime late-audit reconciler. Caller request keys cannot provide
/// this authority because a process may die after observing the durable
/// first-winner state but before preserving that request for replay.
pub(crate) fn wait_late_audit_identity(
    loser_kind: &'static str,
    run_id: &RunId,
    loser_id: &str,
) -> Result<(TransitionKey, IntentHash), RepositoryError> {
    if !matches!(loser_kind, "signal" | "timer") || loser_id.is_empty() {
        return Err(RepositoryError::invalid_data());
    }
    let transition_key = TransitionKey::derive(
        "repository.wait-late-audit.v1",
        &[loser_kind, run_id.as_str(), loser_id],
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    #[derive(Serialize)]
    struct WaitLateAuditIntent<'a> {
        contract: &'static str,
        loser_kind: &'static str,
        run_id: &'a str,
        loser_id: &'a str,
    }
    let intent_hash = canonical_intent_hash(&WaitLateAuditIntent {
        contract: "wait_late_audit.v1",
        loser_kind,
        run_id: run_id.as_str(),
        loser_id,
    })?;
    Ok((transition_key, intent_hash))
}

pub(crate) fn canonical_value(value: &Value) -> Result<(String, ContentHash), RepositoryError> {
    let encoded = canonical_json(value)?;
    let hash = ContentHash::from_bytes(encoded.as_bytes());
    Ok((encoded, hash))
}

/// A JSON payload reconstructed from the bytes/value stored in the durable
/// payload authority.  Callers must use this result rather than trusting the
/// denormalized `content_hash` or `canonical_bytes` columns independently.
pub(crate) struct ValidatedInlinePayload {
    value: Value,
    canonical: String,
    content_hash: ContentHash,
    canonical_bytes: u64,
}

impl ValidatedInlinePayload {
    pub(crate) fn value(&self) -> &Value {
        &self.value
    }

    pub(crate) fn canonical(&self) -> &str {
        &self.canonical
    }

    pub(crate) fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }

    pub(crate) const fn canonical_bytes(&self) -> u64 {
        self.canonical_bytes
    }
}

/// Validates the complete backend-neutral inline-payload authority.
///
/// `sqlite_source` is the exact TEXT stored by SQLite. PostgreSQL JSONB does
/// not preserve its source spelling, so its callers pass `None`; both backends
/// still recompute RFC 8785 JCS from the parsed JSON value. `binary_is_null`
/// is checked in Rust as well as by the schema so disabled constraints and
/// imported/corrupt snapshots fail closed.
pub(crate) fn validate_inline_payload(
    stored_payload_id: &str,
    stored_content_hash: &str,
    stored_canonical_bytes: i64,
    stored_encoding: &str,
    value: Value,
    sqlite_source: Option<&str>,
    binary_is_null: bool,
) -> Result<ValidatedInlinePayload, RepositoryError> {
    if stored_encoding != "json_jcs" || !binary_is_null {
        return Err(RepositoryError::invalid_data());
    }
    let (canonical, content_hash) = canonical_value(&value)?;
    let canonical_bytes =
        u64::try_from(canonical.len()).map_err(|_| RepositoryError::invalid_data())?;
    if stored_content_hash != content_hash.as_str()
        || u64_from_i64(stored_canonical_bytes)? != canonical_bytes
        || stored_payload_id != payload_id(&content_hash)
        || sqlite_source.is_some_and(|source| source != canonical)
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(ValidatedInlinePayload {
        value,
        canonical,
        content_hash,
        canonical_bytes,
    })
}

/// Canonical authority for a durable scheduler checkpoint. The hash binds the
/// complete immutable provenance of the committed boundary rather than only
/// its fact payload, so a checkpoint cannot be transplanted between Runs or
/// scheduler transitions.
#[allow(clippy::too_many_arguments)]
pub(crate) fn scheduler_checkpoint_content_hash(
    run_id: &str,
    checkpoint_id: &str,
    checkpoint_kind: &str,
    transition_key: &str,
    intent_hash: &str,
    event_id: &str,
    checkpoint_schema_version: u32,
    scheduler_projection_version: u64,
    fact_payload: &Value,
) -> Result<ContentHash, RepositoryError> {
    #[derive(Serialize)]
    struct Provenance<'a> {
        domain: &'static str,
        run_id: &'a str,
        checkpoint_id: &'a str,
        checkpoint_kind: &'a str,
        transition_key: &'a str,
        intent_hash: &'a str,
        event_id: &'a str,
        checkpoint_schema_version: u32,
        scheduler_projection_version: u64,
        fact_payload: &'a Value,
    }

    let provenance = Provenance {
        domain: "insight-agent/scheduler-checkpoint/v1",
        run_id,
        checkpoint_id,
        checkpoint_kind,
        transition_key,
        intent_hash,
        event_id,
        checkpoint_schema_version,
        scheduler_projection_version,
        fact_payload,
    };
    serde_jcs::to_vec(&provenance)
        .map(|encoded| ContentHash::from_bytes(&encoded))
        .map_err(|_| RepositoryError::canonicalization())
}

pub(crate) fn event_id(key: &TransitionKey) -> String {
    format!("event_{}", transition_id_token(key))
}

pub(crate) const fn public_event_ordinal(kind: crate::engine::PublicEventKind) -> u16 {
    match kind {
        crate::engine::PublicEventKind::RunCreated => 10,
        crate::engine::PublicEventKind::RunStarted => 20,
        crate::engine::PublicEventKind::OperationStarted => 30,
        crate::engine::PublicEventKind::OperationCompleted
        | crate::engine::PublicEventKind::OperationFailed => 40,
        crate::engine::PublicEventKind::RunCompleted
        | crate::engine::PublicEventKind::RunFailed
        | crate::engine::PublicEventKind::RunCancelled
        | crate::engine::PublicEventKind::RunInterrupted => 50,
    }
}

pub(crate) fn public_event_id(
    run_id: &RunId,
    key: &TransitionKey,
    kind: crate::engine::PublicEventKind,
) -> String {
    // Transition keys are intentionally scoped by Run and may be identical in
    // two Runs. Public notifications contain only this ID, so their identity
    // must bind both dimensions before the globally unique database lookup.
    // Both model IDs reject control characters, making NUL unambiguous framing.
    let derived = ContentHash::from_bytes(
        format!(
            "insight-agent/public-event/v4\0{}\0{}\0{}",
            run_id.as_str(),
            key.as_str(),
            kind.as_str(),
        )
        .as_bytes(),
    );
    format!(
        "public_event_{}",
        &derived.as_str()["sha256:".len()..][..32]
    )
}

pub(crate) fn task_id(key: &TransitionKey) -> String {
    format!("task_{}", transition_id_token(key))
}

pub(crate) fn timer_id(key: &TransitionKey, purpose: &str) -> String {
    let derived = ContentHash::from_bytes(
        format!("insight-agent/repository/{purpose}/{}", key.as_str()).as_bytes(),
    );
    format!("timer_{}", &derived.as_str()["sha256:".len()..][..32])
}

pub(crate) fn companion_transition_key(
    primary: &TransitionKey,
    role: &str,
) -> Result<TransitionKey, RepositoryError> {
    TransitionKey::derive("repository.companion", &[primary.as_str(), role])
        .map_err(|_| RepositoryError::invalid_data())
}

#[derive(Serialize)]
struct CompanionEventIntent<'a> {
    operation: &'static str,
    primary_transition_key: &'a TransitionKey,
    primary_event_id: &'a str,
    role: &'a str,
    event: &'a PendingExecutionEvent,
}

pub(crate) fn companion_event_intent_hash(
    primary_transition_key: &TransitionKey,
    primary_event_id: &str,
    role: &str,
    event: &PendingExecutionEvent,
) -> Result<IntentHash, RepositoryError> {
    canonical_intent_hash(&CompanionEventIntent {
        operation: "repository.companion_event",
        primary_transition_key,
        primary_event_id,
        role,
        event,
    })
}

pub(crate) fn fencing_token(key: &TransitionKey) -> String {
    format!("fence_{}", transition_id_token(key))
}

pub(crate) fn payload_id(hash: &ContentHash) -> String {
    format!(
        "payload_{}",
        hash.as_str()
            .strip_prefix("sha256:")
            .unwrap_or(hash.as_str())
    )
}

fn transition_digest(key: &TransitionKey) -> &str {
    key.as_str()
        .strip_prefix("transition_")
        .unwrap_or(key.as_str())
}

fn transition_id_token(key: &TransitionKey) -> &str {
    // Transition keys carry a SHA-256 digest, while the closed event identity
    // contract uses 128-bit (32 hex digit) tokens. Truncation is deterministic
    // and keeps repository-assigned IDs parseable by Execution/PublicEventId.
    &transition_digest(key)[..32]
}

pub(crate) fn i64_from_u64(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| {
        RepositoryError::new(
            REPOSITORY_DATA_INVALID,
            "durable repository counter is out of range",
        )
    })
}

pub(crate) fn u64_from_i64(value: i64) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| RepositoryError::invalid_data())
}

pub(crate) fn parse_run_lifecycle(value: &str) -> Result<RunLifecycle, RepositoryError> {
    match value {
        "created" => Ok(RunLifecycle::Created),
        "active" => Ok(RunLifecycle::Active),
        "waiting" => Ok(RunLifecycle::Waiting),
        "completing" => Ok(RunLifecycle::Completing),
        "terminating" => Ok(RunLifecycle::Terminating),
        "succeeded" => Ok(RunLifecycle::Succeeded),
        "failed" => Ok(RunLifecycle::Failed),
        "cancelled" => Ok(RunLifecycle::Cancelled),
        "interrupted" => Ok(RunLifecycle::Interrupted),
        "timed_out" => Ok(RunLifecycle::TimedOut),
        _ => Err(RepositoryError::invalid_data()),
    }
}

pub(crate) fn parse_admission_state(value: &str) -> Result<AdmissionState, RepositoryError> {
    match value {
        "open" => Ok(AdmissionState::Open),
        "paused" => Ok(AdmissionState::Paused),
        "draining" => Ok(AdmissionState::Draining),
        "closed" => Ok(AdmissionState::Closed),
        _ => Err(RepositoryError::invalid_data()),
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::{PublicEventId, PublicEventKind, RunId, TransitionKey};

    use super::{public_event_id, public_event_ordinal};

    #[test]
    fn public_event_identity_binds_run_transition_key_and_kind() {
        let transition =
            TransitionKey::derive("repository.public-event.test", &["same-key"]).unwrap();
        let run_a = RunId::new("run_public_identity_a").unwrap();
        let run_b = RunId::new("run_public_identity_b").unwrap();

        let first = public_event_id(&run_a, &transition, PublicEventKind::RunCreated);
        assert_eq!(
            first,
            public_event_id(&run_a, &transition, PublicEventKind::RunCreated)
        );
        assert_ne!(
            first,
            public_event_id(&run_b, &transition, PublicEventKind::RunCreated)
        );
        assert_ne!(
            first,
            public_event_id(&run_a, &transition, PublicEventKind::RunStarted)
        );
        PublicEventId::parse(first).unwrap();
        PublicEventId::parse(public_event_id(
            &run_b,
            &transition,
            PublicEventKind::RunCreated,
        ))
        .unwrap();
    }

    #[test]
    fn public_event_ordinal_totally_orders_same_execution_event_projections() {
        assert!(
            public_event_ordinal(PublicEventKind::OperationFailed)
                < public_event_ordinal(PublicEventKind::RunFailed)
        );
        assert_eq!(
            public_event_ordinal(PublicEventKind::OperationCompleted),
            public_event_ordinal(PublicEventKind::OperationFailed)
        );
    }
}
