use super::RepositoryErrorExt as _;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::{ActivationId, AttemptNo, RunId, TimerId};

use super::{common::canonical_value, DurableRepository, RepositoryError};

pub(crate) const PROJECTION_CHECKPOINT_SCHEMA_VERSION: u32 = 1;

/// Closed set understood by the v3 checkpoint envelope and repair registry.
/// Delivery/inbox/idempotency authorities such as `task_outbox`,
/// `signals_inbox`, `human_work_items`, and `public_event_outbox` are
/// intentionally excluded: rebuilding one could duplicate an external
/// observation or overwrite a receipt/claim authority. Those tables have
/// independent retention and idempotency contracts instead of projection
/// repair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionSubjectKind {
    Run,
    Scope,
    Activation,
    Attempt,
    Timer,
    Control,
    Fork,
    Join,
    Scheduler,
    DataValue,
}

impl ProjectionSubjectKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Scope => "scope",
            Self::Activation => "activation",
            Self::Attempt => "attempt",
            Self::Timer => "timer",
            Self::Control => "control",
            Self::Fork => "fork",
            Self::Join => "join",
            Self::Scheduler => "scheduler",
            Self::DataValue => "data_value",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProjectionSubject {
    kind: ProjectionSubjectKind,
    subject_id: String,
}

impl ProjectionSubject {
    pub fn new(
        kind: ProjectionSubjectKind,
        subject_id: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        let subject_id = subject_id.into();
        if subject_id.is_empty() {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self { kind, subject_id })
    }

    pub fn run(run_id: &RunId) -> Self {
        Self {
            kind: ProjectionSubjectKind::Run,
            subject_id: run_id.as_str().to_owned(),
        }
    }

    pub fn scope(scope_instance_id: impl Into<String>) -> Result<Self, RepositoryError> {
        Self::new(ProjectionSubjectKind::Scope, scope_instance_id)
    }

    pub fn activation(activation_id: &ActivationId) -> Self {
        Self {
            kind: ProjectionSubjectKind::Activation,
            subject_id: activation_id.as_str().to_owned(),
        }
    }

    pub fn attempt(activation_id: &ActivationId, attempt_no: AttemptNo) -> Self {
        Self {
            kind: ProjectionSubjectKind::Attempt,
            subject_id: attempt_subject_id(activation_id.as_str(), attempt_no.get()),
        }
    }

    pub fn timer(timer_id: &TimerId) -> Self {
        Self {
            kind: ProjectionSubjectKind::Timer,
            subject_id: timer_id.as_str().to_owned(),
        }
    }

    pub fn kind(&self) -> ProjectionSubjectKind {
        self.kind
    }

    pub fn subject_id(&self) -> &str {
        &self.subject_id
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionRebuildSnapshot {
    subject: ProjectionSubject,
    event_id: String,
    event_seq: u64,
    projection_version: u64,
    projection_hash: String,
    canonical_projection: Value,
}

impl ProjectionRebuildSnapshot {
    pub(crate) fn new(
        subject: ProjectionSubject,
        event_id: String,
        event_seq: u64,
        projection_version: u64,
        projection_hash: String,
        canonical_projection: Value,
    ) -> Self {
        Self {
            subject,
            event_id,
            event_seq,
            projection_version,
            projection_hash,
            canonical_projection,
        }
    }

    pub fn subject(&self) -> &ProjectionSubject {
        &self.subject
    }
    pub fn event_id(&self) -> &str {
        &self.event_id
    }
    pub fn event_seq(&self) -> u64 {
        self.event_seq
    }
    pub fn projection_version(&self) -> u64 {
        self.projection_version
    }
    pub fn projection_hash(&self) -> &str {
        &self.projection_hash
    }
    pub fn canonical_projection(&self) -> &Value {
        &self.canonical_projection
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ProjectionAudit {
    Match {
        authoritative: ProjectionRebuildSnapshot,
    },
    Mismatch {
        authoritative: ProjectionRebuildSnapshot,
        actual_hash: Option<String>,
    },
}

impl ProjectionAudit {
    pub fn is_match(&self) -> bool {
        matches!(self, Self::Match { .. })
    }

    pub fn authoritative(&self) -> &ProjectionRebuildSnapshot {
        match self {
            Self::Match { authoritative } | Self::Mismatch { authoritative, .. } => authoritative,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionRepairReceipt {
    repaired: bool,
    authoritative: ProjectionRebuildSnapshot,
}

impl ProjectionRepairReceipt {
    pub(crate) fn new(repaired: bool, authoritative: ProjectionRebuildSnapshot) -> Self {
        Self {
            repaired,
            authoritative,
        }
    }

    pub fn repaired(&self) -> bool {
        self.repaired
    }

    pub fn authoritative(&self) -> &ProjectionRebuildSnapshot {
        &self.authoritative
    }
}

#[async_trait]
pub trait ProjectionDurableRepository: DurableRepository {
    async fn audit_projection(
        &self,
        run_id: &RunId,
        subject: &ProjectionSubject,
    ) -> Result<ProjectionAudit, RepositoryError>;

    async fn load_authoritative_rebuild_snapshot(
        &self,
        run_id: &RunId,
        subject: &ProjectionSubject,
    ) -> Result<Option<ProjectionRebuildSnapshot>, RepositoryError>;

    /// Repairs one materialized projection from its latest authoritative,
    /// manifest-verified checkpoint. The checkpoint ledger supplies values;
    /// the repository's closed table/column registry supplies SQL structure and
    /// preserves row identity. A malformed ledger fails closed before repair.
    async fn repair_projection(
        &self,
        run_id: &RunId,
        subject: &ProjectionSubject,
    ) -> Result<ProjectionRepairReceipt, RepositoryError>;

    /// Rebuilds every subject found in the immutable event ledger in the
    /// repository's closed dependency order. This is the deterministic entry
    /// point for repairing a lost group of mutually dependent projections.
    async fn repair_all_projections(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<ProjectionRepairReceipt>, RepositoryError>;
}

pub(crate) fn decode_hex_subject_components(
    subject_id: &str,
    prefix: &str,
    expected: usize,
) -> Result<Vec<String>, RepositoryError> {
    let encoded = subject_id
        .strip_prefix(prefix)
        .ok_or_else(RepositoryError::invalid_data)?;
    let parts = encoded.split(':').collect::<Vec<_>>();
    if parts.len() != expected
        || parts
            .iter()
            .any(|part| part.is_empty() || part.len() % 2 != 0)
    {
        return Err(RepositoryError::invalid_data());
    }
    parts
        .into_iter()
        .map(|part| {
            let bytes = part
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| {
                    let high = hex_nibble(pair[0])?;
                    let low = hex_nibble(pair[1])?;
                    Ok((high << 4) | low)
                })
                .collect::<Result<Vec<_>, RepositoryError>>()?;
            String::from_utf8(bytes).map_err(|_| RepositoryError::invalid_data())
        })
        .collect()
}

fn hex_nibble(byte: u8) -> Result<u8, RepositoryError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(RepositoryError::invalid_data()),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CheckpointRecord {
    pub subject: ProjectionSubject,
    pub projection_version: u64,
    pub canonical_projection: Value,
    pub projection_hash: String,
}

/// Closed, self-verifying projection facts embedded in the immutable
/// `execution_events` ledger row. The checkpoint tables are indexes/caches;
/// this envelope is the rebuild authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectionLedgerBatch {
    schema_version: u32,
    subject_count: u64,
    manifest_hash: String,
    subjects: Vec<ProjectionLedgerRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionLedgerRecord {
    subject_kind: ProjectionSubjectKind,
    subject_id: String,
    projection_version: u64,
    projection_hash: String,
    canonical_projection: Value,
}

impl ProjectionLedgerBatch {
    pub(crate) fn from_records(records: &[CheckpointRecord]) -> Result<Self, RepositoryError> {
        Ok(Self {
            schema_version: PROJECTION_CHECKPOINT_SCHEMA_VERSION,
            subject_count: u64::try_from(records.len())
                .map_err(|_| RepositoryError::invalid_data())?,
            manifest_hash: checkpoint_manifest_hash(records)?,
            subjects: records
                .iter()
                .map(|record| ProjectionLedgerRecord {
                    subject_kind: record.subject.kind(),
                    subject_id: record.subject.subject_id().to_owned(),
                    projection_version: record.projection_version,
                    projection_hash: record.projection_hash.clone(),
                    canonical_projection: record.canonical_projection.clone(),
                })
                .collect(),
        })
    }

    pub(crate) fn validate(&self) -> Result<Vec<CheckpointRecord>, RepositoryError> {
        if self.schema_version != PROJECTION_CHECKPOINT_SCHEMA_VERSION
            || self.subject_count
                != u64::try_from(self.subjects.len())
                    .map_err(|_| RepositoryError::invalid_data())?
        {
            return Err(RepositoryError::invalid_data());
        }
        let mut records = Vec::with_capacity(self.subjects.len());
        for subject in &self.subjects {
            let record = CheckpointRecord::from_value(
                ProjectionSubject::new(subject.subject_kind, subject.subject_id.clone())?,
                subject.projection_version,
                subject.canonical_projection.clone(),
            )?;
            if record.projection_hash != subject.projection_hash {
                return Err(RepositoryError::invalid_data());
            }
            records.push(record);
        }
        if records.windows(2).any(|pair| {
            (
                pair[0].subject.kind().as_str(),
                pair[0].subject.subject_id(),
            ) >= (
                pair[1].subject.kind().as_str(),
                pair[1].subject.subject_id(),
            )
        }) || checkpoint_manifest_hash(&records)? != self.manifest_hash
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(records)
    }

    pub(crate) fn record_for(
        &self,
        subject: &ProjectionSubject,
    ) -> Result<Option<CheckpointRecord>, RepositoryError> {
        Ok(self.validate()?.into_iter().find(|record| {
            record.subject.kind() == subject.kind()
                && record.subject.subject_id() == subject.subject_id()
        }))
    }
}

impl CheckpointRecord {
    pub(crate) fn from_value(
        subject: ProjectionSubject,
        projection_version: u64,
        canonical_projection: Value,
    ) -> Result<Self, RepositoryError> {
        let (_, hash) = canonical_value(&canonical_projection)?;
        Ok(Self {
            subject,
            projection_version,
            canonical_projection,
            projection_hash: hash.as_str().to_owned(),
        })
    }
}

pub(crate) fn checkpoint_manifest_hash(
    records: &[CheckpointRecord],
) -> Result<String, RepositoryError> {
    let manifest = Value::Array(
        records
            .iter()
            .map(|record| {
                serde_json::json!({
                    "subject_kind": record.subject.kind().as_str(),
                    "subject_id": record.subject.subject_id(),
                    "projection_version": record.projection_version,
                    "projection_hash": record.projection_hash,
                })
            })
            .collect(),
    );
    let (_, hash) = canonical_value(&manifest)?;
    Ok(hash.as_str().to_owned())
}

pub(crate) fn attempt_subject_id(activation_id: &str, attempt_no: u32) -> String {
    format!("{activation_id}#{attempt_no}")
}

pub(crate) fn parse_attempt_subject_id(value: &str) -> Result<(&str, u32), RepositoryError> {
    let (activation_id, attempt_no) = value
        .rsplit_once('#')
        .ok_or_else(RepositoryError::invalid_data)?;
    let attempt_no = attempt_no
        .parse::<u32>()
        .map_err(|_| RepositoryError::invalid_data())?;
    if activation_id.is_empty() || attempt_no == 0 {
        return Err(RepositoryError::invalid_data());
    }
    Ok((activation_id, attempt_no))
}

#[cfg(test)]
mod authority_classification_tests {
    use super::{
        ProjectionLedgerBatch, ProjectionSubjectKind, PROJECTION_CHECKPOINT_SCHEMA_VERSION,
    };

    #[test]
    fn delivery_authorities_cannot_masquerade_as_rebuildable_projections() {
        for authority in [
            "public_event_outbox",
            "signal",
            "signals_inbox",
            "task_outbox",
            "human_work_item",
            "human_work_items",
            "control_transition_result",
            "recovery_transition_result",
            "artifact_retention_release",
        ] {
            assert!(
                serde_json::from_value::<ProjectionSubjectKind>(serde_json::json!(authority))
                    .is_err()
            );
        }
    }

    #[test]
    fn authority_subjects_in_a_ledger_batch_fail_closed() {
        for authority in ["signal", "task_outbox", "human_work_item"] {
            let encoded = serde_json::json!({
                "schema_version": PROJECTION_CHECKPOINT_SCHEMA_VERSION,
                "subject_count": 1,
                "manifest_hash": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "subjects": [{
                    "subject_kind": authority,
                    "subject_id": "forbidden-authority",
                    "projection_version": 1,
                    "projection_hash": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "canonical_projection": {}
                }]
            });
            assert!(serde_json::from_value::<ProjectionLedgerBatch>(encoded).is_err());
        }
    }
}
