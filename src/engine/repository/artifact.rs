//! Run-scoped, content-addressed payload and artifact persistence.
//!
//! The database row is the authority for metadata only. Uploading or deleting
//! an object in an external object store remains an at-least-once integration
//! concern; this module deliberately does not claim external exactly-once I/O.

use super::RepositoryErrorExt as _;

use std::{collections::HashSet, fmt};

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use insight_engine::repository::adapter as repository_adapter;
use serde::Serialize;
use serde_json::Value;
use sqlx::{postgres::PgRow, sqlite::SqliteRow, PgPool, Postgres, Row, Sqlite, Transaction};

use crate::engine::{
    ActivationId, ArtifactId, ArtifactRef, ContentHash, ExecutionEventContext,
    ExecutionEventPayload, InlineValueRef, PendingExecutionEvent, ProjectionMutationKind, RunId,
    TransitionKey, TransitionOutcome,
};

use super::{
    common::{
        canonical_intent_hash, canonical_json, event_id, i64_from_u64, payload_id, u64_from_i64,
    },
    DurableRepository, PostgresDurableRepository, RepositoryError, SqliteDurableRepository,
    StorageLocator, REPOSITORY_ARTIFACT_STORE_CONFLICT, REPOSITORY_RUN_NOT_FOUND,
};

const MAX_SWEEP_BATCH: u32 = 1_000;
const MAX_CLAIMANT_BYTES: usize = 256;
const MAX_CLAIM_SECONDS: u32 = 3_600;
const MAX_RETENTION_SECONDS: u32 = 10 * 365 * 24 * 60 * 60;
const MAX_RETENTION_RELEASE_BATCH: u32 = 1_000;
const SHARED_FILESYSTEM_BACKEND: &str = "shared_filesystem";
const MAX_ARTIFACT_STORE_NAMESPACE_BYTES: usize = 128;

fn valid_artifact_store_namespace(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ARTIFACT_STORE_NAMESPACE_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_artifact_store_id(value: &str) -> bool {
    value.strip_prefix("artifact_store_").is_some_and(|suffix| {
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

const SQLITE_ORPHAN_CANDIDATES: &str =
    "SELECT a.run_id, a.artifact_id, a.content_hash, a.size_bytes,
            a.media_type, a.storage_uri
     FROM artifacts a
     WHERE (
       (
         (
           a.artifact_state IN ('staged', 'verified')
           AND julianday(a.created_at) <= julianday('now', ?1)
           AND (a.retain_until IS NULL OR julianday(a.retain_until) <= julianday('now'))
           AND NOT EXISTS (SELECT 1 FROM workflow_runs r
                           WHERE r.run_id = a.run_id AND r.output_artifact_id = a.artifact_id)
           AND NOT EXISTS (SELECT 1 FROM node_activations n
                           WHERE n.run_id = a.run_id AND n.output_artifact_id = a.artifact_id)
           AND NOT EXISTS (SELECT 1 FROM node_attempts t
                           WHERE t.run_id = a.run_id AND t.output_artifact_id = a.artifact_id)
           AND NOT EXISTS (SELECT 1 FROM join_arrivals j
                           WHERE j.run_id = a.run_id AND j.value_artifact_id = a.artifact_id)
           AND NOT EXISTS (SELECT 1 FROM scheduler_values sv
                           WHERE sv.run_id = a.run_id AND sv.artifact_id = a.artifact_id)
           AND NOT EXISTS (SELECT 1 FROM scheduler_occurrence_values sov
                           WHERE sov.run_id = a.run_id AND sov.artifact_id = a.artifact_id)
         )
         OR (
           a.artifact_state = 'referenced'
           AND EXISTS (
             SELECT 1 FROM artifact_retention_releases own_release
             WHERE own_release.run_id = a.run_id
               AND julianday(own_release.retain_until) <= julianday('now')
           )
         )
       )
       AND NOT EXISTS (
         SELECT 1 FROM recovery_artifact_roots rr
         LEFT JOIN artifact_retention_releases root_release
           ON root_release.run_id = rr.run_id
         WHERE rr.artifact_run_id = a.run_id AND rr.artifact_id = a.artifact_id
           AND (root_release.run_id IS NULL
                OR julianday(root_release.retain_until) > julianday('now'))
       )
       OR (a.artifact_state = 'deleting'
           AND julianday(a.deletion_claim_expires_at) <= julianday('now'))
     )
     ORDER BY a.created_at, a.run_id, a.artifact_id
     LIMIT ?2";

const POSTGRES_ORPHAN_CANDIDATES: &str =
    "SELECT a.run_id, a.artifact_id, a.content_hash, a.size_bytes,
            a.media_type, a.storage_uri
     FROM artifacts a
     WHERE (
       (
         (
           a.artifact_state IN ('staged', 'verified')
           AND a.created_at <= CURRENT_TIMESTAMP - make_interval(secs => $1)
           AND (a.retain_until IS NULL OR a.retain_until <= CURRENT_TIMESTAMP)
           AND NOT EXISTS (SELECT 1 FROM workflow_runs r
                           WHERE r.run_id = a.run_id AND r.output_artifact_id = a.artifact_id)
           AND NOT EXISTS (SELECT 1 FROM node_activations n
                           WHERE n.run_id = a.run_id AND n.output_artifact_id = a.artifact_id)
           AND NOT EXISTS (SELECT 1 FROM node_attempts t
                           WHERE t.run_id = a.run_id AND t.output_artifact_id = a.artifact_id)
           AND NOT EXISTS (SELECT 1 FROM join_arrivals j
                           WHERE j.run_id = a.run_id AND j.value_artifact_id = a.artifact_id)
           AND NOT EXISTS (SELECT 1 FROM scheduler_values sv
                           WHERE sv.run_id = a.run_id AND sv.artifact_id = a.artifact_id)
           AND NOT EXISTS (SELECT 1 FROM scheduler_occurrence_values sov
                           WHERE sov.run_id = a.run_id AND sov.artifact_id = a.artifact_id)
         )
         OR (
           a.artifact_state = 'referenced'
           AND EXISTS (
             SELECT 1 FROM artifact_retention_releases own_release
             WHERE own_release.run_id = a.run_id AND own_release.retain_until <= CURRENT_TIMESTAMP
           )
         )
       )
       AND NOT EXISTS (
         SELECT 1 FROM recovery_artifact_roots rr
         LEFT JOIN artifact_retention_releases root_release
           ON root_release.run_id = rr.run_id
         WHERE rr.artifact_run_id = a.run_id AND rr.artifact_id = a.artifact_id
           AND (root_release.run_id IS NULL OR root_release.retain_until > CURRENT_TIMESTAMP)
       )
       OR (a.artifact_state = 'deleting'
           AND a.deletion_claim_expires_at <= CURRENT_TIMESTAMP)
     )
     ORDER BY a.created_at, a.run_id, a.artifact_id
     LIMIT $2";

fn invalid_command() -> RepositoryError {
    RepositoryError::invalid_configuration()
}

fn run_not_found() -> RepositoryError {
    RepositoryError::new(
        REPOSITORY_RUN_NOT_FOUND,
        "durable workflow run was not found",
    )
}

fn model_data<T>(value: Result<T, crate::engine::ModelError>) -> Result<T, RepositoryError> {
    value.map_err(|_| RepositoryError::invalid_data())
}

fn sort_deletion_claims(claims: &mut [ArtifactDeletionClaim]) {
    claims.sort_by(|left, right| {
        (
            left.run_id().as_str(),
            left.artifact().artifact_id().as_str(),
        )
            .cmp(&(
                right.run_id().as_str(),
                right.artifact().artifact_id().as_str(),
            ))
    });
}

fn now_text(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn database_time(value: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(value.timestamp_micros())
        .expect("a valid DateTime always has a representable microsecond timestamp")
}

fn framed_hash(domain: &str, parts: &[&str]) -> ContentHash {
    let mut encoded = Vec::new();
    for part in std::iter::once(domain).chain(parts.iter().copied()) {
        encoded.extend_from_slice(&u64::try_from(part.len()).unwrap_or(u64::MAX).to_be_bytes());
        encoded.extend_from_slice(part.as_bytes());
    }
    ContentHash::from_bytes(&encoded)
}

fn parse_time(value: &str) -> Result<DateTime<Utc>, RepositoryError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| RepositoryError::invalid_data())
}

fn sqlite_retention_release_from_row(
    row: &SqliteRow,
) -> Result<ArtifactRetentionRelease, RepositoryError> {
    Ok(ArtifactRetentionRelease::new(
        model_data(RunId::new(
            row.try_get::<String, _>("run_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?,
        row.try_get("event_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        u64_from_i64(
            row.try_get("event_seq")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        parse_time(
            &row.try_get::<String, _>("retain_until")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        u64_from_i64(
            row.try_get("artifact_count")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
    ))
}

fn postgres_retention_release_from_row(
    row: &PgRow,
) -> Result<ArtifactRetentionRelease, RepositoryError> {
    Ok(ArtifactRetentionRelease::new(
        model_data(RunId::new(
            row.try_get::<String, _>("run_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?,
        row.try_get("event_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        u64_from_i64(
            row.try_get("event_seq")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get("retain_until")
            .map_err(|_| RepositoryError::invalid_data())?,
        u64_from_i64(
            row.try_get("artifact_count")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
    ))
}

/// Deterministic identifier of an inline payload within one Run authority.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct PayloadId(String);

impl PayloadId {
    pub fn parse(value: impl Into<String>) -> Result<Self, RepositoryError> {
        let value = value.into();
        let Some(digest) = value.strip_prefix("payload_") else {
            return Err(invalid_command());
        };
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid_command());
        }
        Ok(Self(value))
    }

    fn from_hash(hash: &ContentHash) -> Self {
        Self(payload_id(hash))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PutInlinePayloadCommand {
    run_id: RunId,
    value: InlineValueRef,
}

impl PutInlinePayloadCommand {
    pub fn new(run_id: RunId, value: Value) -> Result<Self, RepositoryError> {
        Ok(Self {
            run_id,
            value: model_data(InlineValueRef::new(value))?,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn value(&self) -> &InlineValueRef {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PayloadReceipt {
    run_id: RunId,
    payload_id: PayloadId,
    content_hash: ContentHash,
    canonical_bytes: u64,
}

impl PayloadReceipt {
    fn from_inline(run_id: RunId, value: &InlineValueRef) -> Self {
        Self {
            run_id,
            payload_id: PayloadId::from_hash(value.content_hash()),
            content_hash: value.content_hash().clone(),
            canonical_bytes: value.canonical_bytes(),
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn payload_id(&self) -> &PayloadId {
        &self.payload_id
    }

    pub fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }

    pub fn canonical_bytes(&self) -> u64 {
        self.canonical_bytes
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StoredInlinePayload {
    receipt: PayloadReceipt,
    value: Value,
}

impl StoredInlinePayload {
    pub fn receipt(&self) -> &PayloadReceipt {
        &self.receipt
    }

    pub fn value(&self) -> &Value {
        &self.value
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactState {
    Staged,
    Verified,
    Referenced,
    Deleting,
    Deleted,
}

impl ArtifactState {
    fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "staged" => Ok(Self::Staged),
            "verified" => Ok(Self::Verified),
            "referenced" => Ok(Self::Referenced),
            "deleting" => Ok(Self::Deleting),
            "deleted" => Ok(Self::Deleted),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StageArtifactCommand {
    run_id: RunId,
    artifact: ArtifactRef,
    storage_locator: StorageLocator,
    retain_until: Option<DateTime<Utc>>,
}

impl StageArtifactCommand {
    pub fn new(
        run_id: RunId,
        artifact: ArtifactRef,
        storage_locator: StorageLocator,
        retain_until: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            run_id,
            artifact,
            storage_locator,
            retain_until: retain_until.map(database_time),
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn artifact(&self) -> &ArtifactRef {
        &self.artifact
    }

    fn storage_locator(&self) -> &StorageLocator {
        &self.storage_locator
    }

    pub fn retain_until(&self) -> Option<DateTime<Utc>> {
        self.retain_until
    }
}

impl fmt::Debug for StageArtifactCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StageArtifactCommand")
            .field("run_id", &self.run_id)
            .field("artifact", &self.artifact)
            .field("storage_locator", &self.storage_locator)
            .field("retain_until", &self.retain_until)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactReceipt {
    run_id: RunId,
    artifact: ArtifactRef,
    state: ArtifactState,
}

/// Private metadata hand-off for a bounded object-store read.
///
/// This proves only that the exact Run-scoped Artifact is referenced and its
/// owning Run retention has not expired. Callers exposing bytes publicly must
/// additionally prove that the ArtifactRef occurs in that Run's durable
/// public response snapshot.
#[derive(Clone, PartialEq, Eq)]
pub struct RetainedArtifact {
    run_id: RunId,
    artifact: ArtifactRef,
    storage_locator: StorageLocator,
}

impl RetainedArtifact {
    fn new(run_id: RunId, artifact: ArtifactRef, storage_locator: StorageLocator) -> Self {
        Self {
            run_id,
            artifact,
            storage_locator,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn artifact(&self) -> &ArtifactRef {
        &self.artifact
    }

    pub fn storage_locator(&self) -> &StorageLocator {
        &self.storage_locator
    }
}

impl fmt::Debug for RetainedArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedArtifact")
            .field("run_id", &self.run_id)
            .field("artifact", &self.artifact)
            .field("storage_locator", &self.storage_locator)
            .finish()
    }
}

impl ArtifactReceipt {
    fn new(run_id: RunId, artifact: ArtifactRef, state: ArtifactState) -> Self {
        Self {
            run_id,
            artifact,
            state,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn artifact(&self) -> &ArtifactRef {
        &self.artifact
    }

    pub fn state(&self) -> ArtifactState {
        self.state
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerifyArtifactCommand {
    run_id: RunId,
    artifact_id: ArtifactId,
    actual_content_hash: ContentHash,
    actual_size_bytes: u64,
}

impl VerifyArtifactCommand {
    pub fn new(
        run_id: RunId,
        artifact_id: ArtifactId,
        actual_content_hash: ContentHash,
        actual_size_bytes: u64,
    ) -> Self {
        Self {
            run_id,
            artifact_id,
            actual_content_hash,
            actual_size_bytes,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn artifact_id(&self) -> &ArtifactId {
        &self.artifact_id
    }

    pub fn actual_content_hash(&self) -> &ContentHash {
        &self.actual_content_hash
    }

    pub fn actual_size_bytes(&self) -> u64 {
        self.actual_size_bytes
    }
}

/// The durable projection which already owns an artifact output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "owner", rename_all = "snake_case")]
pub enum ArtifactReferenceTarget {
    Run {
        expected_projection_version: u64,
    },
    Activation {
        activation_id: ActivationId,
        expected_projection_version: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReferenceArtifactCommand {
    run_id: RunId,
    artifact: ArtifactRef,
    target: ArtifactReferenceTarget,
}

impl ReferenceArtifactCommand {
    pub fn new(run_id: RunId, artifact: ArtifactRef, target: ArtifactReferenceTarget) -> Self {
        Self {
            run_id,
            artifact,
            target,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn artifact(&self) -> &ArtifactRef {
        &self.artifact
    }

    pub fn target(&self) -> &ArtifactReferenceTarget {
        &self.target
    }
}

/// Proof that a verified object and its terminal projection reference were
/// observed and committed under one database transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactReferenceAuthority {
    receipt: ArtifactReceipt,
    target: ArtifactReferenceTarget,
}

impl ArtifactReferenceAuthority {
    fn new(receipt: ArtifactReceipt, target: ArtifactReferenceTarget) -> Self {
        Self { receipt, target }
    }

    pub fn receipt(&self) -> &ArtifactReceipt {
        &self.receipt
    }

    pub fn target(&self) -> &ArtifactReferenceTarget {
        &self.target
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrphanSweepCommand {
    orphan_retention_seconds: u32,
    claimed_by: String,
    claim_seconds: u32,
    limit: u32,
}

/// Registers the end of the audit/recovery hold for all Artifacts owned by a
/// terminal Run. The deadline is derived from the durable terminal timestamp,
/// and the retention duration frozen at Run admission, not the caller clock or
/// the current process configuration. `retention_seconds` remains in this
/// legacy recovery command for bounded wire compatibility; it is not policy
/// authority. Existing downstream recovery roots continue to hold the object
/// until their owning Run's own retention deadline expires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReleaseRunArtifactRetentionCommand {
    run_id: RunId,
    retention_seconds: u32,
}

impl ReleaseRunArtifactRetentionCommand {
    pub fn new(run_id: RunId, retention_seconds: u32) -> Result<Self, RepositoryError> {
        if retention_seconds == 0 || retention_seconds > MAX_RETENTION_SECONDS {
            return Err(invalid_command());
        }
        Ok(Self {
            run_id,
            retention_seconds,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn retention_seconds(&self) -> u32 {
        self.retention_seconds
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactRetentionRelease {
    run_id: RunId,
    event_id: String,
    event_seq: u64,
    retain_until: DateTime<Utc>,
    artifact_count: u64,
}

impl ArtifactRetentionRelease {
    fn new(
        run_id: RunId,
        event_id: String,
        event_seq: u64,
        retain_until: DateTime<Utc>,
        artifact_count: u64,
    ) -> Self {
        Self {
            run_id,
            event_id,
            event_seq,
            retain_until,
            artifact_count,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    pub fn event_seq(&self) -> u64 {
        self.event_seq
    }

    pub fn retain_until(&self) -> DateTime<Utc> {
        self.retain_until
    }

    pub fn artifact_count(&self) -> u64 {
        self.artifact_count
    }
}

impl OrphanSweepCommand {
    pub fn new(
        orphan_retention_seconds: u32,
        claimed_by: impl Into<String>,
        claim_seconds: u32,
        limit: u32,
    ) -> Result<Self, RepositoryError> {
        let claimed_by = claimed_by.into();
        if orphan_retention_seconds == 0
            || orphan_retention_seconds > MAX_RETENTION_SECONDS
            || limit == 0
            || limit > MAX_SWEEP_BATCH
            || claim_seconds == 0
            || claim_seconds > MAX_CLAIM_SECONDS
            || claimed_by.is_empty()
            || claimed_by.len() > MAX_CLAIMANT_BYTES
            || claimed_by
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
        {
            return Err(invalid_command());
        }
        Ok(Self {
            orphan_retention_seconds,
            claimed_by,
            claim_seconds,
            limit,
        })
    }

    pub fn orphan_retention_seconds(&self) -> u32 {
        self.orphan_retention_seconds
    }

    pub fn limit(&self) -> u32 {
        self.limit
    }

    pub fn claimed_by(&self) -> &str {
        &self.claimed_by
    }

    pub fn claim_seconds(&self) -> u32 {
        self.claim_seconds
    }
}

/// Durable deletion hand-off. Calling the external object store is explicitly
/// outside the database transaction and must be idempotent/at-least-once.
#[derive(Clone, PartialEq, Eq)]
pub struct ArtifactDeletionClaim {
    run_id: RunId,
    artifact: ArtifactRef,
    storage_locator: StorageLocator,
    deletion_fence: ContentHash,
    claim_token: ContentHash,
    claimed_by: String,
    claim_expires_at: DateTime<Utc>,
}

impl ArtifactDeletionClaim {
    fn new(
        transition_key: &TransitionKey,
        run_id: RunId,
        artifact: ArtifactRef,
        storage_locator: StorageLocator,
        claimed_by: String,
        claim_expires_at: DateTime<Utc>,
    ) -> Self {
        let deletion_fence = framed_hash(
            "artifact-object-delete-fence/v1",
            &[
                artifact.content_hash().as_str(),
                storage_locator.expose_to_storage_adapter(),
            ],
        );
        let claim_token = framed_hash(
            "artifact-object-delete-claim/v1",
            &[
                transition_key.as_str(),
                artifact.content_hash().as_str(),
                storage_locator.expose_to_storage_adapter(),
            ],
        );
        Self {
            run_id,
            artifact,
            storage_locator,
            deletion_fence,
            claim_token,
            claimed_by,
            claim_expires_at,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn artifact(&self) -> &ArtifactRef {
        &self.artifact
    }

    pub fn storage_locator(&self) -> &StorageLocator {
        &self.storage_locator
    }

    pub fn deletion_fence(&self) -> &ContentHash {
        &self.deletion_fence
    }

    pub fn claim_token(&self) -> &ContentHash {
        &self.claim_token
    }

    pub fn claimed_by(&self) -> &str {
        &self.claimed_by
    }

    pub fn claim_expires_at(&self) -> DateTime<Utc> {
        self.claim_expires_at
    }
}

impl fmt::Debug for ArtifactDeletionClaim {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactDeletionClaim")
            .field("run_id", &self.run_id)
            .field("artifact", &self.artifact)
            .field("storage_locator", &self.storage_locator)
            .field("deletion_fence", &self.deletion_fence)
            .field("claim_token", &self.claim_token)
            .field("claimed_by", &self.claimed_by)
            .field("claim_expires_at", &self.claim_expires_at)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanSweepBatch {
    claims: Vec<ArtifactDeletionClaim>,
}

impl OrphanSweepBatch {
    pub fn claims(&self) -> &[ArtifactDeletionClaim] {
        &self.claims
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AcknowledgeArtifactDeletionCommand {
    run_id: RunId,
    artifact: ArtifactRef,
    deletion_fence: ContentHash,
    claim_token: ContentHash,
    claimed_by: String,
}

impl AcknowledgeArtifactDeletionCommand {
    pub fn from_claim(claim: &ArtifactDeletionClaim) -> Self {
        Self {
            run_id: claim.run_id.clone(),
            artifact: claim.artifact.clone(),
            deletion_fence: claim.deletion_fence.clone(),
            claim_token: claim.claim_token.clone(),
            claimed_by: claim.claimed_by.clone(),
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn artifact(&self) -> &ArtifactRef {
        &self.artifact
    }

    pub fn deletion_fence(&self) -> &ContentHash {
        &self.deletion_fence
    }

    pub fn claim_token(&self) -> &ContentHash {
        &self.claim_token
    }

    pub fn claimed_by(&self) -> &str {
        &self.claimed_by
    }
}

/// Immutable identity of the shared Artifact backend used by every production
/// runtime attached to one durable repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindArtifactStoreAuthorityCommand {
    backend: String,
    namespace: String,
    store_id: String,
}

impl BindArtifactStoreAuthorityCommand {
    pub fn shared_filesystem(
        namespace: impl Into<String>,
        store_id: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        let command = Self {
            backend: SHARED_FILESYSTEM_BACKEND.to_owned(),
            namespace: namespace.into(),
            store_id: store_id.into(),
        };
        if !valid_artifact_store_namespace(&command.namespace)
            || !valid_artifact_store_id(&command.store_id)
        {
            return Err(RepositoryError::invalid_configuration());
        }
        Ok(command)
    }

    pub fn backend(&self) -> &str {
        &self.backend
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn store_id(&self) -> &str {
        &self.store_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactStoreAuthority {
    backend: String,
    namespace: String,
    store_id: String,
    bound_at: DateTime<Utc>,
}

impl ArtifactStoreAuthority {
    fn new(
        backend: String,
        namespace: String,
        store_id: String,
        bound_at: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        if backend != SHARED_FILESYSTEM_BACKEND
            || !valid_artifact_store_namespace(&namespace)
            || !valid_artifact_store_id(&store_id)
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            backend,
            namespace,
            store_id,
            bound_at,
        })
    }

    fn matches(&self, command: &BindArtifactStoreAuthorityCommand) -> bool {
        self.backend == command.backend
            && self.namespace == command.namespace
            && self.store_id == command.store_id
    }

    pub fn backend(&self) -> &str {
        &self.backend
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn store_id(&self) -> &str {
        &self.store_id
    }

    pub fn bound_at(&self) -> DateTime<Utc> {
        self.bound_at
    }
}

fn artifact_store_conflict() -> RepositoryError {
    RepositoryError::new(
        REPOSITORY_ARTIFACT_STORE_CONFLICT,
        "durable repository is bound to a different Artifact store",
    )
}

#[async_trait]
pub trait ArtifactDurableRepository: DurableRepository {
    /// Atomically creates or verifies the singleton production Artifact-store
    /// authority before any Run or catalog publication can be written.
    async fn bind_artifact_store_authority(
        &self,
        command: BindArtifactStoreAuthorityCommand,
    ) -> Result<TransitionOutcome<ArtifactStoreAuthority>, RepositoryError>;

    async fn put_inline_payload(
        &self,
        command: PutInlinePayloadCommand,
    ) -> Result<TransitionOutcome<PayloadReceipt>, RepositoryError>;

    async fn get_inline_payload(
        &self,
        run_id: &RunId,
        payload_id: &PayloadId,
    ) -> Result<Option<StoredInlinePayload>, RepositoryError>;

    /// Loads private object metadata only while the exact Run-scoped row is
    /// referenced and the owning Run's retention deadline has not elapsed.
    /// This method deliberately does not grant public visibility by itself.
    async fn get_retained_artifact(
        &self,
        run_id: &RunId,
        artifact_id: &ArtifactId,
    ) -> Result<Option<RetainedArtifact>, RepositoryError>;

    async fn stage_artifact(
        &self,
        command: StageArtifactCommand,
    ) -> Result<TransitionOutcome<ArtifactReceipt>, RepositoryError>;

    async fn verify_artifact(
        &self,
        command: VerifyArtifactCommand,
    ) -> Result<TransitionOutcome<ArtifactReceipt>, RepositoryError>;

    async fn reference_artifact(
        &self,
        command: ReferenceArtifactCommand,
    ) -> Result<TransitionOutcome<ArtifactReferenceAuthority>, RepositoryError>;

    /// Finds terminal Runs whose reference-retention deadline has not yet
    /// been registered. This discovery is a hint; the release transaction is
    /// the authority and is idempotent by Run + transition identity.
    async fn list_unreleased_terminal_artifact_runs(
        &self,
        limit: u32,
    ) -> Result<Vec<RunId>, RepositoryError>;

    async fn release_run_artifact_retention(
        &self,
        transition_key: TransitionKey,
        command: ReleaseRunArtifactRetentionCommand,
    ) -> Result<TransitionOutcome<ArtifactRetentionRelease>, RepositoryError>;

    async fn sweep_orphan_artifacts(
        &self,
        transition_key: TransitionKey,
        command: OrphanSweepCommand,
    ) -> Result<TransitionOutcome<OrphanSweepBatch>, RepositoryError>;

    async fn acknowledge_artifact_deleted(
        &self,
        command: AcknowledgeArtifactDeletionCommand,
    ) -> Result<TransitionOutcome<ArtifactReceipt>, RepositoryError>;
}

struct ArtifactRow {
    artifact: ArtifactRef,
    storage_locator: StorageLocator,
    state: ArtifactState,
    retain_until: Option<DateTime<Utc>>,
}

struct ArtifactObjectRow {
    run_id: RunId,
    artifact: ArtifactRef,
    state: ArtifactState,
    deletion_eligible: bool,
}

fn artifact_object_key(artifact: &ArtifactRef, locator: &StorageLocator) -> (String, String) {
    (
        artifact.content_hash().as_str().to_owned(),
        locator.expose_to_storage_adapter().to_owned(),
    )
}

fn artifact_row_claim_token(
    object_claim_token: &ContentHash,
    run_id: &RunId,
    artifact_id: &ArtifactId,
) -> ContentHash {
    framed_hash(
        "artifact-object-delete-row-claim/v1",
        &[
            object_claim_token.as_str(),
            run_id.as_str(),
            artifact_id.as_str(),
        ],
    )
}

fn artifact_ref(
    artifact_id: String,
    content_hash: String,
    size_bytes: i64,
    media_type: Option<String>,
) -> Result<ArtifactRef, RepositoryError> {
    model_data(ArtifactRef::new(
        model_data(ArtifactId::new(artifact_id))?,
        model_data(ContentHash::parse(content_hash))?,
        u64_from_i64(size_bytes)?,
        media_type,
    ))
}

fn matching_stage_identity(row: &ArtifactRow, command: &StageArtifactCommand) -> bool {
    row.artifact == *command.artifact() && row.storage_locator == *command.storage_locator()
}

fn payload_receipt(command: &PutInlinePayloadCommand) -> PayloadReceipt {
    PayloadReceipt::from_inline(command.run_id().clone(), command.value())
}

async fn ensure_sqlite_run(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
) -> Result<(), RepositoryError> {
    let exists = sqlx::query_scalar::<_, i64>("SELECT 1 FROM workflow_runs WHERE run_id = ?")
        .bind(run_id.as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    if exists.is_none() {
        return Err(run_not_found());
    }
    Ok(())
}

async fn ensure_postgres_run(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<(), RepositoryError> {
    let exists =
        sqlx::query_scalar::<_, i32>("SELECT 1 FROM workflow_runs WHERE run_id = $1 FOR SHARE")
            .bind(run_id.as_str())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
    if exists.is_none() {
        return Err(run_not_found());
    }
    Ok(())
}

async fn lock_postgres_artifact_object(
    transaction: &mut Transaction<'_, Postgres>,
    content_hash: &ContentHash,
    storage_locator: &StorageLocator,
) -> Result<(), RepositoryError> {
    sqlx::query(
        "SELECT pg_advisory_xact_lock(
             hashtextextended($1 || chr(31) || $2, 0)
         )",
    )
    .bind(content_hash.as_str())
    .bind(storage_locator.expose_to_storage_adapter())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

async fn sqlite_artifact_object_is_deleting(
    transaction: &mut Transaction<'_, Sqlite>,
    content_hash: &ContentHash,
    storage_locator: &StorageLocator,
) -> Result<bool, RepositoryError> {
    sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS(
           SELECT 1 FROM artifacts
           WHERE content_hash=? AND storage_uri=? AND artifact_state='deleting'
         )",
    )
    .bind(content_hash.as_str())
    .bind(storage_locator.expose_to_storage_adapter())
    .fetch_one(&mut **transaction)
    .await
    .map(|value| value != 0)
    .map_err(RepositoryError::storage)
}

async fn sqlite_artifact_object_size_matches(
    transaction: &mut Transaction<'_, Sqlite>,
    content_hash: &ContentHash,
    storage_locator: &StorageLocator,
    size_bytes: u64,
) -> Result<bool, RepositoryError> {
    sqlx::query_scalar::<_, i64>(
        "SELECT NOT EXISTS(
           SELECT 1 FROM artifacts
           WHERE content_hash=? AND storage_uri=? AND size_bytes<>?
         )",
    )
    .bind(content_hash.as_str())
    .bind(storage_locator.expose_to_storage_adapter())
    .bind(i64_from_u64(size_bytes)?)
    .fetch_one(&mut **transaction)
    .await
    .map(|value| value != 0)
    .map_err(RepositoryError::storage)
}

async fn postgres_artifact_object_is_deleting(
    transaction: &mut Transaction<'_, Postgres>,
    content_hash: &ContentHash,
    storage_locator: &StorageLocator,
) -> Result<bool, RepositoryError> {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
           SELECT 1 FROM artifacts
           WHERE content_hash=$1 AND storage_uri=$2 AND artifact_state='deleting'
         )",
    )
    .bind(content_hash.as_str())
    .bind(storage_locator.expose_to_storage_adapter())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)
}

async fn postgres_artifact_object_size_matches(
    transaction: &mut Transaction<'_, Postgres>,
    content_hash: &ContentHash,
    storage_locator: &StorageLocator,
    size_bytes: u64,
) -> Result<bool, RepositoryError> {
    sqlx::query_scalar::<_, bool>(
        "SELECT NOT EXISTS(
           SELECT 1 FROM artifacts
           WHERE content_hash=$1 AND storage_uri=$2 AND size_bytes<>$3
         )",
    )
    .bind(content_hash.as_str())
    .bind(storage_locator.expose_to_storage_adapter())
    .bind(i64_from_u64(size_bytes)?)
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)
}

async fn load_sqlite_artifact_candidates(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &StageArtifactCommand,
) -> Result<Vec<ArtifactRow>, RepositoryError> {
    let rows = sqlx::query(
        "SELECT artifact_id, content_hash, size_bytes, media_type, storage_uri,
                artifact_state, retain_until
         FROM artifacts
         WHERE run_id = ? AND (artifact_id = ? OR content_hash = ?)",
    )
    .bind(command.run_id().as_str())
    .bind(command.artifact().artifact_id().as_str())
    .bind(command.artifact().content_hash().as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    rows.into_iter()
        .map(|row| {
            Ok(ArtifactRow {
                artifact: artifact_ref(
                    row.try_get("artifact_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("content_hash")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("size_bytes")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("media_type")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                storage_locator: repository_adapter::storage_locator_from_validated_parts(
                    row.try_get("storage_uri")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ),
                state: ArtifactState::parse(
                    &row.try_get::<String, _>("artifact_state")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                retain_until: row
                    .try_get::<Option<String>, _>("retain_until")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .map(|value| parse_time(&value))
                    .transpose()?,
            })
        })
        .collect()
}

async fn load_postgres_artifact_candidates(
    transaction: &mut Transaction<'_, Postgres>,
    command: &StageArtifactCommand,
) -> Result<Vec<ArtifactRow>, RepositoryError> {
    let rows = sqlx::query(
        "SELECT artifact_id, content_hash, size_bytes, media_type, storage_uri,
                artifact_state, retain_until
         FROM artifacts
         WHERE run_id = $1 AND (artifact_id = $2 OR content_hash = $3)
         FOR UPDATE",
    )
    .bind(command.run_id().as_str())
    .bind(command.artifact().artifact_id().as_str())
    .bind(command.artifact().content_hash().as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    rows.into_iter()
        .map(|row| {
            Ok(ArtifactRow {
                artifact: artifact_ref(
                    row.try_get("artifact_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("content_hash")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("size_bytes")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("media_type")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                storage_locator: repository_adapter::storage_locator_from_validated_parts(
                    row.try_get("storage_uri")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ),
                state: ArtifactState::parse(
                    &row.try_get::<String, _>("artifact_state")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                retain_until: row
                    .try_get("retain_until")
                    .map_err(|_| RepositoryError::invalid_data())?,
            })
        })
        .collect()
}

async fn load_sqlite_artifact_object_rows(
    transaction: &mut Transaction<'_, Sqlite>,
    content_hash: &ContentHash,
    storage_locator: &StorageLocator,
    orphan_cutoff: &str,
) -> Result<Vec<ArtifactObjectRow>, RepositoryError> {
    let rows = sqlx::query(
        "SELECT a.run_id,a.artifact_id,a.content_hash,a.size_bytes,a.media_type,a.artifact_state,
                CASE
                  WHEN a.artifact_state='deleted' THEN 1
                  WHEN a.artifact_state='deleting'
                       AND julianday(a.deletion_claim_expires_at)<=julianday('now') THEN 1
                  WHEN (
                    (
                      (a.artifact_state IN ('staged','verified')
                       AND julianday(a.created_at)<=julianday('now',?3)
                       AND (a.retain_until IS NULL
                            OR julianday(a.retain_until)<=julianday('now'))
                       AND NOT EXISTS (SELECT 1 FROM workflow_runs r
                                       WHERE r.run_id=a.run_id
                                         AND r.output_artifact_id=a.artifact_id)
                       AND NOT EXISTS (SELECT 1 FROM node_activations n
                                       WHERE n.run_id=a.run_id
                                         AND n.output_artifact_id=a.artifact_id)
                       AND NOT EXISTS (SELECT 1 FROM node_attempts t
                                       WHERE t.run_id=a.run_id
                                         AND t.output_artifact_id=a.artifact_id)
                       AND NOT EXISTS (SELECT 1 FROM join_arrivals j
                                       WHERE j.run_id=a.run_id
                                         AND j.value_artifact_id=a.artifact_id)
                       AND NOT EXISTS (SELECT 1 FROM scheduler_values sv
                                       WHERE sv.run_id=a.run_id AND sv.artifact_id=a.artifact_id)
                       AND NOT EXISTS (SELECT 1 FROM scheduler_occurrence_values sov
                                       WHERE sov.run_id=a.run_id AND sov.artifact_id=a.artifact_id))
                      OR
                      (a.artifact_state='referenced'
                       AND EXISTS (SELECT 1 FROM artifact_retention_releases own_release
                                   WHERE own_release.run_id=a.run_id
                                     AND julianday(own_release.retain_until)<=julianday('now')))
                    )
                    AND NOT EXISTS (
                      SELECT 1 FROM recovery_artifact_roots rr
                      LEFT JOIN artifact_retention_releases root_release
                        ON root_release.run_id=rr.run_id
                      WHERE rr.artifact_run_id=a.run_id AND rr.artifact_id=a.artifact_id
                        AND (root_release.run_id IS NULL
                             OR julianday(root_release.retain_until)>julianday('now'))
                    )
                  ) THEN 1 ELSE 0
                END AS deletion_eligible
         FROM artifacts a
         WHERE a.content_hash=?1 AND a.storage_uri=?2
         ORDER BY a.run_id,a.artifact_id",
    )
    .bind(content_hash.as_str())
    .bind(storage_locator.expose_to_storage_adapter())
    .bind(orphan_cutoff)
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    rows.into_iter()
        .map(|row| {
            Ok(ArtifactObjectRow {
                run_id: model_data(RunId::new(
                    row.try_get::<String, _>("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                artifact: artifact_ref(
                    row.try_get("artifact_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("content_hash")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("size_bytes")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("media_type")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                state: ArtifactState::parse(
                    &row.try_get::<String, _>("artifact_state")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                deletion_eligible: row
                    .try_get::<i64, _>("deletion_eligible")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != 0,
            })
        })
        .collect()
}

async fn load_postgres_artifact_object_rows(
    transaction: &mut Transaction<'_, Postgres>,
    content_hash: &ContentHash,
    storage_locator: &StorageLocator,
    orphan_retention_seconds: i32,
) -> Result<Vec<ArtifactObjectRow>, RepositoryError> {
    let rows = sqlx::query(
        "SELECT a.run_id,a.artifact_id,a.content_hash,a.size_bytes,a.media_type,a.artifact_state,
                CASE
                  WHEN a.artifact_state='deleted' THEN TRUE
                  WHEN a.artifact_state='deleting'
                       AND a.deletion_claim_expires_at<=CURRENT_TIMESTAMP THEN TRUE
                  WHEN (
                    (
                      (a.artifact_state IN ('staged','verified')
                       AND a.created_at<=CURRENT_TIMESTAMP-make_interval(secs=>$3)
                       AND (a.retain_until IS NULL OR a.retain_until<=CURRENT_TIMESTAMP)
                       AND NOT EXISTS (SELECT 1 FROM workflow_runs r
                                       WHERE r.run_id=a.run_id
                                         AND r.output_artifact_id=a.artifact_id)
                       AND NOT EXISTS (SELECT 1 FROM node_activations n
                                       WHERE n.run_id=a.run_id
                                         AND n.output_artifact_id=a.artifact_id)
                       AND NOT EXISTS (SELECT 1 FROM node_attempts t
                                       WHERE t.run_id=a.run_id
                                         AND t.output_artifact_id=a.artifact_id)
                       AND NOT EXISTS (SELECT 1 FROM join_arrivals j
                                       WHERE j.run_id=a.run_id
                                         AND j.value_artifact_id=a.artifact_id)
                       AND NOT EXISTS (SELECT 1 FROM scheduler_values sv
                                       WHERE sv.run_id=a.run_id AND sv.artifact_id=a.artifact_id)
                       AND NOT EXISTS (SELECT 1 FROM scheduler_occurrence_values sov
                                       WHERE sov.run_id=a.run_id AND sov.artifact_id=a.artifact_id))
                      OR
                      (a.artifact_state='referenced'
                       AND EXISTS (SELECT 1 FROM artifact_retention_releases own_release
                                   WHERE own_release.run_id=a.run_id
                                     AND own_release.retain_until<=CURRENT_TIMESTAMP))
                    )
                    AND NOT EXISTS (
                      SELECT 1 FROM recovery_artifact_roots rr
                      LEFT JOIN artifact_retention_releases root_release
                        ON root_release.run_id=rr.run_id
                      WHERE rr.artifact_run_id=a.run_id AND rr.artifact_id=a.artifact_id
                        AND (root_release.run_id IS NULL
                             OR root_release.retain_until>CURRENT_TIMESTAMP)
                    )
                  ) THEN TRUE ELSE FALSE
                END AS deletion_eligible
         FROM artifacts a
         WHERE a.content_hash=$1 AND a.storage_uri=$2
         ORDER BY a.run_id,a.artifact_id
         FOR UPDATE OF a",
    )
    .bind(content_hash.as_str())
    .bind(storage_locator.expose_to_storage_adapter())
    .bind(orphan_retention_seconds)
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    rows.into_iter()
        .map(|row| {
            Ok(ArtifactObjectRow {
                run_id: model_data(RunId::new(
                    row.try_get::<String, _>("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                artifact: artifact_ref(
                    row.try_get("artifact_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("content_hash")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("size_bytes")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("media_type")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                state: ArtifactState::parse(
                    &row.try_get::<String, _>("artifact_state")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                deletion_eligible: row
                    .try_get::<bool, _>("deletion_eligible")
                    .map_err(|_| RepositoryError::invalid_data())?,
            })
        })
        .collect()
}

fn replayable_artifact_row(
    rows: &[ArtifactRow],
    command: &StageArtifactCommand,
) -> Option<(ArtifactReceipt, Option<DateTime<Utc>>)> {
    if rows.len() != 1
        || !matching_stage_identity(&rows[0], command)
        || matches!(
            rows[0].state,
            ArtifactState::Deleting | ArtifactState::Deleted
        )
    {
        return None;
    }
    let extension = command.retain_until().filter(|requested| {
        rows[0]
            .retain_until
            .as_ref()
            .is_none_or(|current| requested > current)
    });
    Some((
        ArtifactReceipt::new(
            command.run_id().clone(),
            rows[0].artifact.clone(),
            rows[0].state,
        ),
        extension,
    ))
}

async fn sqlite_payload(
    pool: &sqlx::SqlitePool,
    run_id: &RunId,
    id: &PayloadId,
) -> Result<Option<StoredInlinePayload>, RepositoryError> {
    let row = sqlx::query(
        "SELECT content_hash, canonical_bytes, encoding, inline_value
         FROM payloads WHERE run_id = ? AND payload_id = ?",
    )
    .bind(run_id.as_str())
    .bind(id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let encoding: String = row
        .try_get("encoding")
        .map_err(|_| RepositoryError::invalid_data())?;
    if encoding != "json_jcs" {
        return Err(RepositoryError::invalid_data());
    }
    let encoded: String = row
        .try_get("inline_value")
        .map_err(|_| RepositoryError::invalid_data())?;
    let value: Value =
        serde_json::from_str(&encoded).map_err(|_| RepositoryError::invalid_data())?;
    let inline = model_data(InlineValueRef::new(value.clone()))?;
    let stored_hash: String = row
        .try_get("content_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    let stored_bytes: i64 = row
        .try_get("canonical_bytes")
        .map_err(|_| RepositoryError::invalid_data())?;
    if inline.content_hash().as_str() != stored_hash
        || inline.canonical_bytes() != u64_from_i64(stored_bytes)?
        || PayloadId::from_hash(inline.content_hash()) != *id
        || canonical_json(&value)? != encoded
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(Some(StoredInlinePayload {
        receipt: PayloadReceipt::from_inline(run_id.clone(), &inline),
        value,
    }))
}

async fn postgres_payload(
    pool: &PgPool,
    run_id: &RunId,
    id: &PayloadId,
) -> Result<Option<StoredInlinePayload>, RepositoryError> {
    let row = sqlx::query(
        "SELECT content_hash, canonical_bytes, encoding, inline_value
         FROM payloads WHERE run_id = $1 AND payload_id = $2",
    )
    .bind(run_id.as_str())
    .bind(id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let encoding: String = row
        .try_get("encoding")
        .map_err(|_| RepositoryError::invalid_data())?;
    if encoding != "json_jcs" {
        return Err(RepositoryError::invalid_data());
    }
    let value: Value = row
        .try_get("inline_value")
        .map_err(|_| RepositoryError::invalid_data())?;
    let inline = model_data(InlineValueRef::new(value.clone()))?;
    let stored_hash: String = row
        .try_get("content_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    let stored_bytes: i64 = row
        .try_get("canonical_bytes")
        .map_err(|_| RepositoryError::invalid_data())?;
    if inline.content_hash().as_str() != stored_hash
        || inline.canonical_bytes() != u64_from_i64(stored_bytes)?
        || PayloadId::from_hash(inline.content_hash()) != *id
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(Some(StoredInlinePayload {
        receipt: PayloadReceipt::from_inline(run_id.clone(), &inline),
        value,
    }))
}

#[async_trait]
impl ArtifactDurableRepository for SqliteDurableRepository {
    async fn bind_artifact_store_authority(
        &self,
        command: BindArtifactStoreAuthorityCommand,
    ) -> Result<TransitionOutcome<ArtifactStoreAuthority>, RepositoryError> {
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let inserted = sqlx::query(
            "INSERT INTO artifact_store_authority (
                singleton,backend,namespace,store_id,bound_at
             ) VALUES (1,?,?,?,STRFTIME('%Y-%m-%dT%H:%M:%fZ','now'))
             ON CONFLICT(singleton) DO NOTHING",
        )
        .bind(command.backend())
        .bind(command.namespace())
        .bind(command.store_id())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let row = sqlx::query(
            "SELECT backend,namespace,store_id,bound_at
             FROM artifact_store_authority WHERE singleton=1",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let authority = ArtifactStoreAuthority::new(
            row.try_get("backend")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("namespace")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("store_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            parse_time(
                &row.try_get::<String, _>("bound_at")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
        )?;
        if !authority.matches(&command) {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Err(artifact_store_conflict());
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(if inserted.rows_affected() == 1 {
            TransitionOutcome::Committed { result: authority }
        } else {
            TransitionOutcome::ExactReplay {
                authoritative: authority,
            }
        })
    }

    async fn put_inline_payload(
        &self,
        command: PutInlinePayloadCommand,
    ) -> Result<TransitionOutcome<PayloadReceipt>, RepositoryError> {
        let canonical = canonical_json(command.value().value())?;
        let receipt = payload_receipt(&command);
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        ensure_sqlite_run(&mut transaction, command.run_id()).await?;

        if let Some(stored) = sqlx::query(
            "SELECT content_hash, canonical_bytes, encoding, inline_value
             FROM payloads WHERE run_id = ? AND payload_id = ?",
        )
        .bind(command.run_id().as_str())
        .bind(receipt.payload_id().as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        {
            let stored_hash: String = stored
                .try_get("content_hash")
                .map_err(|_| RepositoryError::invalid_data())?;
            let stored_bytes: i64 = stored
                .try_get("canonical_bytes")
                .map_err(|_| RepositoryError::invalid_data())?;
            let encoding: String = stored
                .try_get("encoding")
                .map_err(|_| RepositoryError::invalid_data())?;
            let stored_value: String = stored
                .try_get("inline_value")
                .map_err(|_| RepositoryError::invalid_data())?;
            let exact = stored_hash == receipt.content_hash().as_str()
                && u64_from_i64(stored_bytes)? == receipt.canonical_bytes()
                && encoding == "json_jcs"
                && stored_value == canonical;
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(if exact {
                TransitionOutcome::ExactReplay {
                    authoritative: receipt,
                }
            } else {
                TransitionOutcome::StateConflict
            });
        }

        sqlx::query(
            "INSERT INTO payloads (
                run_id, payload_id, content_hash, canonical_bytes, encoding,
                inline_value, binary_value, created_at, retain_until
             ) VALUES (?, ?, ?, ?, 'json_jcs', ?, NULL, CURRENT_TIMESTAMP, NULL)",
        )
        .bind(command.run_id().as_str())
        .bind(receipt.payload_id().as_str())
        .bind(receipt.content_hash().as_str())
        .bind(i64_from_u64(receipt.canonical_bytes())?)
        .bind(&canonical)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn get_inline_payload(
        &self,
        run_id: &RunId,
        payload_id: &PayloadId,
    ) -> Result<Option<StoredInlinePayload>, RepositoryError> {
        sqlite_payload(&self.pool, run_id, payload_id).await
    }

    async fn get_retained_artifact(
        &self,
        run_id: &RunId,
        artifact_id: &ArtifactId,
    ) -> Result<Option<RetainedArtifact>, RepositoryError> {
        let row = sqlx::query(
            "SELECT a.content_hash,a.size_bytes,a.media_type,a.storage_uri
             FROM artifacts a
             LEFT JOIN artifact_retention_releases release ON release.run_id=a.run_id
             WHERE a.run_id=? AND a.artifact_id=? AND a.artifact_state='referenced'
               AND (release.run_id IS NULL
                    OR julianday(release.retain_until)>julianday('now'))",
        )
        .bind(run_id.as_str())
        .bind(artifact_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let artifact = artifact_ref(
            artifact_id.as_str().to_owned(),
            row.try_get("content_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("size_bytes")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("media_type")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let storage_locator = StorageLocator::new(
            row.try_get::<String, _>("storage_uri")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        Ok(Some(RetainedArtifact::new(
            run_id.clone(),
            artifact,
            storage_locator,
        )))
    }

    async fn stage_artifact(
        &self,
        command: StageArtifactCommand,
    ) -> Result<TransitionOutcome<ArtifactReceipt>, RepositoryError> {
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        ensure_sqlite_run(&mut transaction, command.run_id()).await?;
        if !sqlite_artifact_object_size_matches(
            &mut transaction,
            command.artifact().content_hash(),
            command.storage_locator(),
            command.artifact().size_bytes(),
        )
        .await?
            || sqlite_artifact_object_is_deleting(
                &mut transaction,
                command.artifact().content_hash(),
                command.storage_locator(),
            )
            .await?
        {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let rows = load_sqlite_artifact_candidates(&mut transaction, &command).await?;
        if !rows.is_empty() {
            let replay = replayable_artifact_row(&rows, &command);
            let extended = replay.as_ref().and_then(|(_, value)| *value);
            if let Some(retain_until) = extended {
                let updated = sqlx::query(
                    "UPDATE artifacts SET retain_until=?
                     WHERE run_id=? AND artifact_id=?
                       AND (retain_until IS NULL OR julianday(retain_until) < julianday(?))",
                )
                .bind(now_text(retain_until))
                .bind(command.run_id().as_str())
                .bind(command.artifact().artifact_id().as_str())
                .bind(now_text(retain_until))
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
                .rows_affected();
                if updated != 1 {
                    return Err(RepositoryError::invalid_data());
                }
            }
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(match replay {
                Some((authoritative, Some(_))) => TransitionOutcome::Committed {
                    result: authoritative,
                },
                Some((authoritative, None)) => TransitionOutcome::ExactReplay { authoritative },
                None => TransitionOutcome::StateConflict,
            });
        }

        sqlx::query(
            "INSERT INTO artifacts (
                run_id, artifact_id, content_hash, size_bytes, media_type,
                storage_uri, artifact_state, verified_at, referenced_at,
                retain_until, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, 'staged', NULL, NULL, ?, CURRENT_TIMESTAMP)",
        )
        .bind(command.run_id().as_str())
        .bind(command.artifact().artifact_id().as_str())
        .bind(command.artifact().content_hash().as_str())
        .bind(i64_from_u64(command.artifact().size_bytes())?)
        .bind(command.artifact().media_type())
        .bind(command.storage_locator().expose_to_storage_adapter())
        .bind(command.retain_until().map(now_text))
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let receipt = ArtifactReceipt::new(
            command.run_id().clone(),
            command.artifact().clone(),
            ArtifactState::Staged,
        );
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn verify_artifact(
        &self,
        command: VerifyArtifactCommand,
    ) -> Result<TransitionOutcome<ArtifactReceipt>, RepositoryError> {
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        ensure_sqlite_run(&mut transaction, command.run_id()).await?;
        let row = sqlx::query(
            "SELECT content_hash, size_bytes, media_type, artifact_state
             FROM artifacts WHERE run_id = ? AND artifact_id = ?",
        )
        .bind(command.run_id().as_str())
        .bind(command.artifact_id().as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        let stored_hash: String = row
            .try_get("content_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        let stored_size: i64 = row
            .try_get("size_bytes")
            .map_err(|_| RepositoryError::invalid_data())?;
        let state = ArtifactState::parse(
            &row.try_get::<String, _>("artifact_state")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let media_type: Option<String> = row
            .try_get("media_type")
            .map_err(|_| RepositoryError::invalid_data())?;
        if stored_hash != command.actual_content_hash().as_str()
            || u64_from_i64(stored_size)? != command.actual_size_bytes()
            || matches!(state, ArtifactState::Deleting | ArtifactState::Deleted)
        {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let artifact = model_data(ArtifactRef::new(
            command.artifact_id().clone(),
            command.actual_content_hash().clone(),
            command.actual_size_bytes(),
            media_type,
        ))?;
        if matches!(state, ArtifactState::Verified | ArtifactState::Referenced) {
            let authority = ArtifactReceipt::new(command.run_id().clone(), artifact, state);
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: authority,
            });
        }
        let updated = sqlx::query(
            "UPDATE artifacts
             SET artifact_state = 'verified', verified_at = CURRENT_TIMESTAMP
             WHERE run_id = ? AND artifact_id = ? AND artifact_state = 'staged'
               AND content_hash = ? AND size_bytes = ?",
        )
        .bind(command.run_id().as_str())
        .bind(command.artifact_id().as_str())
        .bind(command.actual_content_hash().as_str())
        .bind(i64_from_u64(command.actual_size_bytes())?)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if updated.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let receipt =
            ArtifactReceipt::new(command.run_id().clone(), artifact, ArtifactState::Verified);
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn reference_artifact(
        &self,
        command: ReferenceArtifactCommand,
    ) -> Result<TransitionOutcome<ArtifactReferenceAuthority>, RepositoryError> {
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let outcome = reference_sqlite_artifact(&mut transaction, &command).await?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(outcome)
    }

    async fn list_unreleased_terminal_artifact_runs(
        &self,
        limit: u32,
    ) -> Result<Vec<RunId>, RepositoryError> {
        if limit == 0 || limit > MAX_RETENTION_RELEASE_BATCH {
            return Err(invalid_command());
        }
        let rows = sqlx::query(
            "SELECT r.run_id FROM workflow_runs r
             LEFT JOIN artifact_retention_releases release ON release.run_id=r.run_id
             WHERE r.lifecycle IN ('succeeded','failed','cancelled','interrupted','timed_out')
               AND r.admission_state='closed' AND r.terminal_at IS NOT NULL
               AND release.run_id IS NULL
             ORDER BY r.terminal_at,r.run_id LIMIT ?",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        rows.into_iter()
            .map(|row| {
                model_data(RunId::new(
                    row.try_get::<String, _>("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))
            })
            .collect()
    }

    async fn release_run_artifact_retention(
        &self,
        transition_key: TransitionKey,
        command: ReleaseRunArtifactRetentionCommand,
    ) -> Result<TransitionOutcome<ArtifactRetentionRelease>, RepositoryError> {
        let intent_hash = canonical_intent_hash(&command)?;
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let mut existing = sqlx::query(
            "SELECT run_id,transition_key,intent_hash,event_id,event_seq,retain_until,artifact_count,
                    registration_kind
             FROM artifact_retention_releases WHERE run_id=? OR transition_key=?",
        )
        .bind(command.run_id().as_str())
        .bind(transition_key.as_str())
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if existing.len() > 1 {
            return Err(RepositoryError::intent_conflict());
        }
        if let Some(row) = existing.pop() {
            let same_run = row.try_get::<String, _>("run_id").ok().as_deref()
                == Some(command.run_id().as_str());
            let terminal_atomic = row
                .try_get::<String, _>("registration_kind")
                .ok()
                .as_deref()
                == Some("terminal_atomic");
            let same_transition = row.try_get::<String, _>("transition_key").ok().as_deref()
                == Some(transition_key.as_str());
            if !same_run || (!terminal_atomic && !same_transition) {
                return Err(RepositoryError::intent_conflict());
            }
            let receipt = sqlite_retention_release_from_row(&row)?;
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: receipt,
            });
        }
        let run = sqlx::query(
            "SELECT lifecycle,admission_state,terminal_at,projection_version,
                    artifact_reference_retention_seconds
             FROM workflow_runs WHERE run_id=?",
        )
        .bind(command.run_id().as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(run) = run else {
            return Err(run_not_found());
        };
        let lifecycle: String = run
            .try_get("lifecycle")
            .map_err(|_| RepositoryError::invalid_data())?;
        let admission: String = run
            .try_get("admission_state")
            .map_err(|_| RepositoryError::invalid_data())?;
        let terminal_at: Option<String> = run
            .try_get("terminal_at")
            .map_err(|_| RepositoryError::invalid_data())?;
        if !matches!(
            lifecycle.as_str(),
            "succeeded" | "failed" | "cancelled" | "interrupted" | "timed_out"
        ) || admission != "closed"
        {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let Some(terminal_at) = terminal_at else {
            return Err(RepositoryError::invalid_data());
        };
        let frozen_retention_seconds = run
            .try_get::<i64, _>("artifact_reference_retention_seconds")
            .map_err(|_| RepositoryError::invalid_data())?;
        if !(1..=i64::from(MAX_RETENTION_SECONDS)).contains(&frozen_retention_seconds) {
            return Err(RepositoryError::invalid_data());
        }
        let retain_until = parse_time(
            &sqlx::query_scalar::<_, String>(
                "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', julianday(?) + CAST(? AS REAL) / 86400.0)",
            )
            .bind(&terminal_at)
            .bind(frozen_retention_seconds)
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?,
        )?;
        let artifact_count = u64_from_i64(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM artifacts WHERE run_id=? AND artifact_state='referenced'",
            )
            .bind(command.run_id().as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?,
        )?;
        // This no-op metadata update serializes expiry registration against a
        // concurrent recovery candidate taking a source-artifact read lock.
        sqlx::query(
            "UPDATE artifacts SET retain_until=CASE
                 WHEN retain_until IS NULL OR julianday(retain_until)<julianday(?) THEN ?
                 ELSE retain_until END
             WHERE run_id=? AND artifact_state='referenced'",
        )
        .bind(now_text(retain_until))
        .bind(now_text(retain_until))
        .bind(command.run_id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let event_seq =
            super::sqlite::allocate_event_seq(&mut transaction, command.run_id()).await?;
        let id = event_id(&transition_key);
        let event = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone()),
            ExecutionEventPayload::ProjectionMutated {
                mutation: ProjectionMutationKind::ArtifactRetentionReleased,
            },
        ))?;
        let projection_version = u64_from_i64(
            run.try_get("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        super::sqlite::insert_event(
            &mut transaction,
            command.run_id(),
            event_seq,
            &id,
            &transition_key,
            intent_hash.as_str(),
            projection_version,
            &event,
        )
        .await?;
        sqlx::query(
            "INSERT INTO artifact_retention_releases
             (run_id,transition_key,intent_hash,event_id,event_seq,retain_until,artifact_count,
              created_at,registration_kind)
             VALUES (?,?,?,?,?,?,?,CURRENT_TIMESTAMP,'legacy')",
        )
        .bind(command.run_id().as_str())
        .bind(transition_key.as_str())
        .bind(intent_hash.as_str())
        .bind(&id)
        .bind(i64_from_u64(event_seq)?)
        .bind(now_text(retain_until))
        .bind(i64_from_u64(artifact_count)?)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        super::sqlite_projection::finalize_projection_checkpoints(
            &mut transaction,
            command.run_id(),
            &id,
        )
        .await?;
        let receipt = ArtifactRetentionRelease::new(
            command.run_id().clone(),
            id,
            event_seq,
            retain_until,
            artifact_count,
        );
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn sweep_orphan_artifacts(
        &self,
        transition_key: TransitionKey,
        command: OrphanSweepCommand,
    ) -> Result<TransitionOutcome<OrphanSweepBatch>, RepositoryError> {
        let intent_hash = canonical_intent_hash(&command)?;
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(replay) =
            replay_sqlite_sweep(&mut transaction, &transition_key, intent_hash.as_str()).await?
        {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(replay);
        }
        sqlx::query(
            "INSERT INTO artifact_gc_sweeps (
                transition_key, intent_hash, claimed_by, created_at
             ) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
        )
        .bind(transition_key.as_str())
        .bind(intent_hash.as_str())
        .bind(command.claimed_by())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let orphan_cutoff = format!("-{} seconds", command.orphan_retention_seconds());
        let claim_expires_at = parse_time(
            &sqlx::query_scalar::<_, String>("SELECT STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now', ?)")
                .bind(format!("+{} seconds", command.claim_seconds()))
                .fetch_one(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?,
        )?;
        let rows = sqlx::query(SQLITE_ORPHAN_CANDIDATES)
            .bind(&orphan_cutoff)
            .bind(i64::from(command.limit()))
            .fetch_all(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
        let mut claims = Vec::with_capacity(rows.len());
        let mut seen_objects = HashSet::new();
        for row in rows {
            let run_id = model_data(RunId::new(
                row.try_get::<String, _>("run_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let artifact = artifact_ref(
                row.try_get("artifact_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
                row.try_get("content_hash")
                    .map_err(|_| RepositoryError::invalid_data())?,
                row.try_get("size_bytes")
                    .map_err(|_| RepositoryError::invalid_data())?,
                row.try_get("media_type")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let storage_locator = repository_adapter::storage_locator_from_validated_parts(
                row.try_get("storage_uri")
                    .map_err(|_| RepositoryError::invalid_data())?,
            );
            if !seen_objects.insert(artifact_object_key(&artifact, &storage_locator)) {
                continue;
            }
            let object_rows = load_sqlite_artifact_object_rows(
                &mut transaction,
                artifact.content_hash(),
                &storage_locator,
                &orphan_cutoff,
            )
            .await?;
            if object_rows.is_empty()
                || object_rows
                    .iter()
                    .all(|row| row.state == ArtifactState::Deleted)
                || object_rows.iter().any(|row| !row.deletion_eligible)
            {
                continue;
            }
            let claim = ArtifactDeletionClaim::new(
                &transition_key,
                run_id,
                artifact,
                storage_locator,
                command.claimed_by().to_owned(),
                claim_expires_at,
            );
            let updated = sqlx::query(
                "UPDATE artifacts
                 SET artifact_state = 'deleting', deletion_fence = ?,
                     deletion_claim_token = ?, deletion_claimed_by = ?,
                     deletion_claim_request_key = ?, deletion_claimed_at = CURRENT_TIMESTAMP,
                     deletion_claim_expires_at = ?, referenced_at = NULL
                 WHERE run_id = ? AND artifact_id = ?
                   AND ((artifact_state IN ('staged', 'verified')
                        AND julianday(created_at) <= julianday('now', ?)
                        AND (retain_until IS NULL
                             OR julianday(retain_until) <= julianday('now'))
                        AND NOT EXISTS (SELECT 1 FROM workflow_runs r
                                        WHERE r.run_id=artifacts.run_id
                                          AND r.output_artifact_id=artifacts.artifact_id)
                        AND NOT EXISTS (SELECT 1 FROM node_activations n
                                        WHERE n.run_id=artifacts.run_id
                                          AND n.output_artifact_id=artifacts.artifact_id)
                        AND NOT EXISTS (SELECT 1 FROM node_attempts t
                                        WHERE t.run_id=artifacts.run_id
                                          AND t.output_artifact_id=artifacts.artifact_id)
                        AND NOT EXISTS (SELECT 1 FROM join_arrivals j
                                        WHERE j.run_id=artifacts.run_id
                                          AND j.value_artifact_id=artifacts.artifact_id)
                        AND NOT EXISTS (SELECT 1 FROM scheduler_values sv
                                        WHERE sv.run_id=artifacts.run_id
                                          AND sv.artifact_id=artifacts.artifact_id)
                        AND NOT EXISTS (SELECT 1 FROM scheduler_occurrence_values sov
                                        WHERE sov.run_id=artifacts.run_id
                                          AND sov.artifact_id=artifacts.artifact_id)
                        AND NOT EXISTS (
                          SELECT 1 FROM recovery_artifact_roots rr
                          LEFT JOIN artifact_retention_releases root_release
                            ON root_release.run_id=rr.run_id
                          WHERE rr.artifact_run_id=artifacts.run_id
                            AND rr.artifact_id=artifacts.artifact_id
                            AND (root_release.run_id IS NULL
                                 OR julianday(root_release.retain_until)>julianday('now'))
                        ))
                        OR (artifact_state = 'referenced'
                            AND EXISTS (
                              SELECT 1 FROM artifact_retention_releases own_release
                              WHERE own_release.run_id=artifacts.run_id
                                AND julianday(own_release.retain_until)<=julianday('now')
                            )
                            AND NOT EXISTS (
                              SELECT 1 FROM recovery_artifact_roots rr
                              LEFT JOIN artifact_retention_releases root_release
                                ON root_release.run_id=rr.run_id
                              WHERE rr.artifact_run_id=artifacts.run_id
                                AND rr.artifact_id=artifacts.artifact_id
                                AND (root_release.run_id IS NULL
                                     OR julianday(root_release.retain_until)>julianday('now'))
                            ))
                        OR (artifact_state = 'deleting'
                            AND julianday(deletion_claim_expires_at) <= julianday('now')))",
            )
            .bind(claim.deletion_fence().as_str())
            .bind(claim.claim_token().as_str())
            .bind(claim.claimed_by())
            .bind(transition_key.as_str())
            .bind(now_text(claim.claim_expires_at()))
            .bind(claim.run_id().as_str())
            .bind(claim.artifact().artifact_id().as_str())
            .bind(&orphan_cutoff)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if updated.rows_affected() != 1 {
                transaction
                    .rollback()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Err(RepositoryError::invalid_data());
            }
            for object_row in object_rows.iter().filter(|row| {
                row.state != ArtifactState::Deleted
                    && (row.run_id != *claim.run_id()
                        || row.artifact.artifact_id() != claim.artifact().artifact_id())
            }) {
                let row_claim_token = artifact_row_claim_token(
                    claim.claim_token(),
                    &object_row.run_id,
                    object_row.artifact.artifact_id(),
                );
                let updated = sqlx::query(
                    "UPDATE artifacts
                     SET artifact_state='deleting',deletion_fence=?,deletion_claim_token=?,
                         deletion_claimed_by=?,deletion_claim_request_key=?,
                         deletion_claimed_at=CURRENT_TIMESTAMP,deletion_claim_expires_at=?,
                         referenced_at=NULL
                     WHERE run_id=? AND artifact_id=? AND artifact_state<>'deleted'",
                )
                .bind(claim.deletion_fence().as_str())
                .bind(row_claim_token.as_str())
                .bind(claim.claimed_by())
                .bind(transition_key.as_str())
                .bind(now_text(claim.claim_expires_at()))
                .bind(object_row.run_id.as_str())
                .bind(object_row.artifact.artifact_id().as_str())
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
                if updated.rows_affected() != 1 {
                    return Err(RepositoryError::invalid_data());
                }
            }
            sqlx::query(
                "INSERT INTO artifact_gc_claims (
                    transition_key, run_id, artifact_id, claim_token,
                    deletion_fence, claim_expires_at
                 ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(transition_key.as_str())
            .bind(claim.run_id().as_str())
            .bind(claim.artifact().artifact_id().as_str())
            .bind(claim.claim_token().as_str())
            .bind(claim.deletion_fence().as_str())
            .bind(now_text(claim.claim_expires_at()))
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            claims.push(claim);
        }
        sort_deletion_claims(&mut claims);
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed {
            result: OrphanSweepBatch { claims },
        })
    }

    async fn acknowledge_artifact_deleted(
        &self,
        command: AcknowledgeArtifactDeletionCommand,
    ) -> Result<TransitionOutcome<ArtifactReceipt>, RepositoryError> {
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let outcome = acknowledge_sqlite_deletion(&mut transaction, &command).await?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(outcome)
    }
}

async fn replay_sqlite_sweep(
    transaction: &mut Transaction<'_, Sqlite>,
    transition_key: &TransitionKey,
    intent_hash: &str,
) -> Result<Option<TransitionOutcome<OrphanSweepBatch>>, RepositoryError> {
    let existing = sqlx::query_scalar::<_, String>(
        "SELECT intent_hash FROM artifact_gc_sweeps WHERE transition_key = ?",
    )
    .bind(transition_key.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(existing) = existing else {
        return Ok(None);
    };
    if existing != intent_hash {
        return Err(RepositoryError::intent_conflict());
    }
    Ok(Some(TransitionOutcome::ExactReplay {
        authoritative: OrphanSweepBatch {
            claims: load_sqlite_sweep_claims(transaction, transition_key).await?,
        },
    }))
}

async fn load_sqlite_sweep_claims(
    transaction: &mut Transaction<'_, Sqlite>,
    transition_key: &TransitionKey,
) -> Result<Vec<ArtifactDeletionClaim>, RepositoryError> {
    let rows = sqlx::query(
        "SELECT c.run_id, c.artifact_id, c.claim_token, c.deletion_fence,
                c.claim_expires_at, s.claimed_by, a.content_hash, a.size_bytes,
                a.media_type, a.storage_uri
         FROM artifact_gc_claims c
         JOIN artifact_gc_sweeps s ON s.transition_key = c.transition_key
         JOIN artifacts a ON a.run_id = c.run_id AND a.artifact_id = c.artifact_id
         WHERE c.transition_key = ? ORDER BY c.run_id, c.artifact_id",
    )
    .bind(transition_key.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    rows.into_iter()
        .map(|row| {
            Ok(ArtifactDeletionClaim {
                run_id: model_data(RunId::new(
                    row.try_get::<String, _>("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                artifact: artifact_ref(
                    row.try_get("artifact_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("content_hash")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("size_bytes")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("media_type")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                storage_locator: repository_adapter::storage_locator_from_validated_parts(
                    row.try_get("storage_uri")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ),
                deletion_fence: model_data(ContentHash::parse(
                    row.try_get::<String, _>("deletion_fence")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                claim_token: model_data(ContentHash::parse(
                    row.try_get::<String, _>("claim_token")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                claimed_by: row
                    .try_get("claimed_by")
                    .map_err(|_| RepositoryError::invalid_data())?,
                claim_expires_at: parse_time(
                    &row.try_get::<String, _>("claim_expires_at")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
            })
        })
        .collect()
}

async fn acknowledge_sqlite_deletion(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &AcknowledgeArtifactDeletionCommand,
) -> Result<TransitionOutcome<ArtifactReceipt>, RepositoryError> {
    let row = sqlx::query(
        "SELECT artifact_state, content_hash, size_bytes, media_type,
                storage_uri, deletion_fence, deletion_claim_token, deletion_claimed_by,
                deletion_claim_request_key
         FROM artifacts WHERE run_id = ? AND artifact_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.artifact().artifact_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(TransitionOutcome::StateConflict);
    };
    let state = ArtifactState::parse(
        &row.try_get::<String, _>("artifact_state")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let storage_locator = repository_adapter::storage_locator_from_validated_parts(
        row.try_get("storage_uri")
            .map_err(|_| RepositoryError::invalid_data())?,
    );
    let claim_request_key: Option<String> = row
        .try_get("deletion_claim_request_key")
        .map_err(|_| RepositoryError::invalid_data())?;
    let exact = row
        .try_get::<String, _>("content_hash")
        .map_err(|_| RepositoryError::invalid_data())?
        == command.artifact().content_hash().as_str()
        && u64_from_i64(
            row.try_get("size_bytes")
                .map_err(|_| RepositoryError::invalid_data())?,
        )? == command.artifact().size_bytes()
        && row
            .try_get::<Option<String>, _>("deletion_fence")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            == Some(command.deletion_fence().as_str())
        && row
            .try_get::<Option<String>, _>("deletion_claim_token")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            == Some(command.claim_token().as_str())
        && row
            .try_get::<Option<String>, _>("deletion_claimed_by")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            == Some(command.claimed_by());
    let Some(claim_request_key) = claim_request_key else {
        return Ok(TransitionOutcome::StateConflict);
    };
    if !exact {
        return Ok(TransitionOutcome::StateConflict);
    }
    let receipt = ArtifactReceipt::new(
        command.run_id().clone(),
        command.artifact().clone(),
        ArtifactState::Deleted,
    );
    let object_rows = sqlx::query(
        "SELECT run_id, artifact_id, artifact_state, deletion_fence,
                deletion_claim_token, deletion_claimed_by, deletion_claim_request_key
         FROM artifacts
         WHERE content_hash = ? AND storage_uri = ?
         ORDER BY run_id, artifact_id",
    )
    .bind(command.artifact().content_hash().as_str())
    .bind(storage_locator.expose_to_storage_adapter())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let mut deleting_rows = 0_u64;
    let mut claimed_rows = 0_u64;
    for object_row in object_rows {
        let row_run_id = model_data(RunId::new(
            object_row
                .try_get::<String, _>("run_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let row_artifact_id = model_data(ArtifactId::new(
            object_row
                .try_get::<String, _>("artifact_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let row_state = ArtifactState::parse(
            &object_row
                .try_get::<String, _>("artifact_state")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let same_claim = object_row
            .try_get::<Option<String>, _>("deletion_fence")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            == Some(command.deletion_fence().as_str())
            && object_row
                .try_get::<Option<String>, _>("deletion_claim_request_key")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_deref()
                == Some(claim_request_key.as_str())
            && object_row
                .try_get::<Option<String>, _>("deletion_claimed_by")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_deref()
                == Some(command.claimed_by());
        if !same_claim {
            if row_state != ArtifactState::Deleted {
                return Ok(TransitionOutcome::StateConflict);
            }
            continue;
        }
        let expected_token = if row_run_id == *command.run_id()
            && row_artifact_id == *command.artifact().artifact_id()
        {
            command.claim_token().clone()
        } else {
            artifact_row_claim_token(command.claim_token(), &row_run_id, &row_artifact_id)
        };
        if object_row
            .try_get::<Option<String>, _>("deletion_claim_token")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            != Some(expected_token.as_str())
        {
            return Ok(TransitionOutcome::StateConflict);
        }
        claimed_rows += 1;
        if row_state == ArtifactState::Deleting {
            deleting_rows += 1;
        } else if row_state != ArtifactState::Deleted {
            return Ok(TransitionOutcome::StateConflict);
        }
    }
    if claimed_rows == 0 {
        return Ok(TransitionOutcome::StateConflict);
    }
    if deleting_rows == 0 {
        return Ok(if state == ArtifactState::Deleted {
            TransitionOutcome::ExactReplay {
                authoritative: receipt,
            }
        } else {
            TransitionOutcome::StateConflict
        });
    }
    if state != ArtifactState::Deleting {
        return Ok(TransitionOutcome::StateConflict);
    }
    let updated = sqlx::query(
        "UPDATE artifacts SET artifact_state = 'deleted'
         WHERE content_hash = ? AND storage_uri = ? AND artifact_state = 'deleting'
           AND deletion_fence = ? AND deletion_claim_request_key = ?
           AND deletion_claimed_by = ?",
    )
    .bind(command.artifact().content_hash().as_str())
    .bind(storage_locator.expose_to_storage_adapter())
    .bind(command.deletion_fence().as_str())
    .bind(&claim_request_key)
    .bind(command.claimed_by())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(if updated.rows_affected() == deleting_rows {
        TransitionOutcome::Committed { result: receipt }
    } else {
        TransitionOutcome::StateConflict
    })
}

pub(crate) async fn reference_sqlite_artifact(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &ReferenceArtifactCommand,
) -> Result<TransitionOutcome<ArtifactReferenceAuthority>, RepositoryError> {
    if !validate_sqlite_reference_target(transaction, command).await? {
        return Ok(TransitionOutcome::StateConflict);
    }
    let state = sqlx::query_scalar::<_, String>(
        "SELECT artifact_state FROM artifacts
         WHERE run_id = ? AND artifact_id = ? AND content_hash = ? AND size_bytes = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.artifact().artifact_id().as_str())
    .bind(command.artifact().content_hash().as_str())
    .bind(i64_from_u64(command.artifact().size_bytes())?)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(state) = state else {
        return Ok(TransitionOutcome::StateConflict);
    };
    let state = ArtifactState::parse(&state)?;
    let authority = |state| {
        ArtifactReferenceAuthority::new(
            ArtifactReceipt::new(command.run_id().clone(), command.artifact().clone(), state),
            command.target().clone(),
        )
    };
    match state {
        ArtifactState::Referenced => Ok(TransitionOutcome::ExactReplay {
            authoritative: authority(state),
        }),
        ArtifactState::Verified => {
            let updated = sqlx::query(
                "UPDATE artifacts
                 SET artifact_state = 'referenced', referenced_at = CURRENT_TIMESTAMP
                 WHERE run_id = ? AND artifact_id = ? AND artifact_state = 'verified'
                   AND content_hash = ? AND size_bytes = ?",
            )
            .bind(command.run_id().as_str())
            .bind(command.artifact().artifact_id().as_str())
            .bind(command.artifact().content_hash().as_str())
            .bind(i64_from_u64(command.artifact().size_bytes())?)
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if updated.rows_affected() == 1 {
                Ok(TransitionOutcome::Committed {
                    result: authority(ArtifactState::Referenced),
                })
            } else {
                Ok(TransitionOutcome::StateConflict)
            }
        }
        ArtifactState::Staged | ArtifactState::Deleting | ArtifactState::Deleted => {
            Ok(TransitionOutcome::StateConflict)
        }
    }
}

async fn validate_sqlite_reference_target(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &ReferenceArtifactCommand,
) -> Result<bool, RepositoryError> {
    let (row, version) = match command.target() {
        ArtifactReferenceTarget::Run {
            expected_projection_version,
        } => (
            sqlx::query(
                "SELECT lifecycle, projection_version, output_artifact_id, output_value_hash
                 FROM workflow_runs WHERE run_id = ?",
            )
            .bind(command.run_id().as_str())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(run_not_found)?,
            *expected_projection_version,
        ),
        ArtifactReferenceTarget::Activation {
            activation_id,
            expected_projection_version,
        } => (
            sqlx::query(
                "SELECT lifecycle, projection_version, output_artifact_id, output_value_hash
                 FROM node_activations WHERE run_id = ? AND activation_id = ?",
            )
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(RepositoryError::activation_not_found)?,
            *expected_projection_version,
        ),
    };
    let lifecycle: String = row
        .try_get("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?;
    let stored_version: i64 = row
        .try_get("projection_version")
        .map_err(|_| RepositoryError::invalid_data())?;
    let artifact_id: Option<String> = row
        .try_get("output_artifact_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let value_hash: Option<String> = row
        .try_get("output_value_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    if lifecycle != "succeeded"
        || u64_from_i64(stored_version)? != version
        || artifact_id.as_deref() != Some(command.artifact().artifact_id().as_str())
        || value_hash.as_deref() != Some(command.artifact().content_hash().as_str())
    {
        return Ok(false);
    }
    Ok(true)
}

#[async_trait]
impl ArtifactDurableRepository for PostgresDurableRepository {
    async fn bind_artifact_store_authority(
        &self,
        command: BindArtifactStoreAuthorityCommand,
    ) -> Result<TransitionOutcome<ArtifactStoreAuthority>, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let inserted = sqlx::query(
            "INSERT INTO artifact_store_authority (
                singleton,backend,namespace,store_id,bound_at
             ) VALUES (TRUE,$1,$2,$3,clock_timestamp())
             ON CONFLICT(singleton) DO NOTHING",
        )
        .bind(command.backend())
        .bind(command.namespace())
        .bind(command.store_id())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let row = sqlx::query(
            "SELECT backend,namespace,store_id,bound_at
             FROM artifact_store_authority WHERE singleton=TRUE FOR SHARE",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let authority = ArtifactStoreAuthority::new(
            row.try_get("backend")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("namespace")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("store_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("bound_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        if !authority.matches(&command) {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Err(artifact_store_conflict());
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(if inserted.rows_affected() == 1 {
            TransitionOutcome::Committed { result: authority }
        } else {
            TransitionOutcome::ExactReplay {
                authoritative: authority,
            }
        })
    }

    async fn put_inline_payload(
        &self,
        command: PutInlinePayloadCommand,
    ) -> Result<TransitionOutcome<PayloadReceipt>, RepositoryError> {
        let receipt = payload_receipt(&command);
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        ensure_postgres_run(&mut transaction, command.run_id()).await?;
        let inserted = sqlx::query(
            "INSERT INTO payloads (
                run_id, payload_id, content_hash, canonical_bytes, encoding,
                inline_value, binary_value, created_at, retain_until
             ) VALUES ($1, $2, $3, $4, 'json_jcs', $5, NULL, CURRENT_TIMESTAMP, NULL)
             ON CONFLICT (run_id, content_hash) DO NOTHING",
        )
        .bind(command.run_id().as_str())
        .bind(receipt.payload_id().as_str())
        .bind(receipt.content_hash().as_str())
        .bind(i64_from_u64(receipt.canonical_bytes())?)
        .bind(command.value().value())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if inserted.rows_affected() == 1 {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::Committed { result: receipt });
        }

        let row = sqlx::query(
            "SELECT payload_id, content_hash, canonical_bytes, encoding, inline_value
             FROM payloads WHERE run_id = $1 AND content_hash = $2 FOR SHARE",
        )
        .bind(command.run_id().as_str())
        .bind(receipt.content_hash().as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        let stored_id: String = row
            .try_get("payload_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let stored_hash: String = row
            .try_get("content_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        let stored_bytes: i64 = row
            .try_get("canonical_bytes")
            .map_err(|_| RepositoryError::invalid_data())?;
        let encoding: String = row
            .try_get("encoding")
            .map_err(|_| RepositoryError::invalid_data())?;
        let stored_value: Value = row
            .try_get("inline_value")
            .map_err(|_| RepositoryError::invalid_data())?;
        let exact = stored_id == receipt.payload_id().as_str()
            && stored_hash == receipt.content_hash().as_str()
            && u64_from_i64(stored_bytes)? == receipt.canonical_bytes()
            && encoding == "json_jcs"
            && stored_value == *command.value().value();
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(if exact {
            TransitionOutcome::ExactReplay {
                authoritative: receipt,
            }
        } else {
            TransitionOutcome::StateConflict
        })
    }

    async fn get_inline_payload(
        &self,
        run_id: &RunId,
        payload_id: &PayloadId,
    ) -> Result<Option<StoredInlinePayload>, RepositoryError> {
        postgres_payload(&self.pool, run_id, payload_id).await
    }

    async fn get_retained_artifact(
        &self,
        run_id: &RunId,
        artifact_id: &ArtifactId,
    ) -> Result<Option<RetainedArtifact>, RepositoryError> {
        let row = sqlx::query(
            "SELECT a.content_hash,a.size_bytes,a.media_type,a.storage_uri
             FROM artifacts a
             LEFT JOIN artifact_retention_releases release ON release.run_id=a.run_id
             WHERE a.run_id=$1 AND a.artifact_id=$2 AND a.artifact_state='referenced'
               AND (release.run_id IS NULL OR release.retain_until>CURRENT_TIMESTAMP)",
        )
        .bind(run_id.as_str())
        .bind(artifact_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let artifact = artifact_ref(
            artifact_id.as_str().to_owned(),
            row.try_get("content_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("size_bytes")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("media_type")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let storage_locator = StorageLocator::new(
            row.try_get::<String, _>("storage_uri")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        Ok(Some(RetainedArtifact::new(
            run_id.clone(),
            artifact,
            storage_locator,
        )))
    }

    async fn stage_artifact(
        &self,
        command: StageArtifactCommand,
    ) -> Result<TransitionOutcome<ArtifactReceipt>, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        ensure_postgres_run(&mut transaction, command.run_id()).await?;
        lock_postgres_artifact_object(
            &mut transaction,
            command.artifact().content_hash(),
            command.storage_locator(),
        )
        .await?;
        if !postgres_artifact_object_size_matches(
            &mut transaction,
            command.artifact().content_hash(),
            command.storage_locator(),
            command.artifact().size_bytes(),
        )
        .await?
            || postgres_artifact_object_is_deleting(
                &mut transaction,
                command.artifact().content_hash(),
                command.storage_locator(),
            )
            .await?
        {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let inserted = sqlx::query(
            "INSERT INTO artifacts (
                run_id, artifact_id, content_hash, size_bytes, media_type,
                storage_uri, artifact_state, verified_at, referenced_at,
                retain_until, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, 'staged', NULL, NULL, $7, CURRENT_TIMESTAMP)
             ON CONFLICT DO NOTHING",
        )
        .bind(command.run_id().as_str())
        .bind(command.artifact().artifact_id().as_str())
        .bind(command.artifact().content_hash().as_str())
        .bind(i64_from_u64(command.artifact().size_bytes())?)
        .bind(command.artifact().media_type())
        .bind(command.storage_locator().expose_to_storage_adapter())
        .bind(command.retain_until())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if inserted.rows_affected() == 1 {
            let receipt = ArtifactReceipt::new(
                command.run_id().clone(),
                command.artifact().clone(),
                ArtifactState::Staged,
            );
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::Committed { result: receipt });
        }
        let rows = load_postgres_artifact_candidates(&mut transaction, &command).await?;
        let replay = replayable_artifact_row(&rows, &command);
        let extended = replay.as_ref().and_then(|(_, value)| *value);
        if let Some(retain_until) = extended {
            let updated = sqlx::query(
                "UPDATE artifacts SET retain_until=$1
                 WHERE run_id=$2 AND artifact_id=$3
                   AND (retain_until IS NULL OR retain_until < $1)",
            )
            .bind(retain_until)
            .bind(command.run_id().as_str())
            .bind(command.artifact().artifact_id().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if updated != 1 {
                return Err(RepositoryError::invalid_data());
            }
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(match replay {
            Some((authoritative, Some(_))) => TransitionOutcome::Committed {
                result: authoritative,
            },
            Some((authoritative, None)) => TransitionOutcome::ExactReplay { authoritative },
            None => TransitionOutcome::StateConflict,
        })
    }

    async fn verify_artifact(
        &self,
        command: VerifyArtifactCommand,
    ) -> Result<TransitionOutcome<ArtifactReceipt>, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        ensure_postgres_run(&mut transaction, command.run_id()).await?;
        let row = sqlx::query(
            "SELECT content_hash, size_bytes, media_type, artifact_state
             FROM artifacts WHERE run_id = $1 AND artifact_id = $2 FOR UPDATE",
        )
        .bind(command.run_id().as_str())
        .bind(command.artifact_id().as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        let stored_hash: String = row
            .try_get("content_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        let stored_size: i64 = row
            .try_get("size_bytes")
            .map_err(|_| RepositoryError::invalid_data())?;
        let state = ArtifactState::parse(
            &row.try_get::<String, _>("artifact_state")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let media_type: Option<String> = row
            .try_get("media_type")
            .map_err(|_| RepositoryError::invalid_data())?;
        if stored_hash != command.actual_content_hash().as_str()
            || u64_from_i64(stored_size)? != command.actual_size_bytes()
            || matches!(state, ArtifactState::Deleting | ArtifactState::Deleted)
        {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let artifact = model_data(ArtifactRef::new(
            command.artifact_id().clone(),
            command.actual_content_hash().clone(),
            command.actual_size_bytes(),
            media_type,
        ))?;
        if matches!(state, ArtifactState::Verified | ArtifactState::Referenced) {
            let authority = ArtifactReceipt::new(command.run_id().clone(), artifact, state);
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: authority,
            });
        }
        let updated = sqlx::query(
            "UPDATE artifacts SET artifact_state = 'verified', verified_at = CURRENT_TIMESTAMP
             WHERE run_id = $1 AND artifact_id = $2 AND artifact_state = 'staged'
               AND content_hash = $3 AND size_bytes = $4",
        )
        .bind(command.run_id().as_str())
        .bind(command.artifact_id().as_str())
        .bind(command.actual_content_hash().as_str())
        .bind(i64_from_u64(command.actual_size_bytes())?)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if updated.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let receipt =
            ArtifactReceipt::new(command.run_id().clone(), artifact, ArtifactState::Verified);
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn reference_artifact(
        &self,
        command: ReferenceArtifactCommand,
    ) -> Result<TransitionOutcome<ArtifactReferenceAuthority>, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let outcome = reference_postgres_artifact(&mut transaction, &command).await?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(outcome)
    }

    async fn list_unreleased_terminal_artifact_runs(
        &self,
        limit: u32,
    ) -> Result<Vec<RunId>, RepositoryError> {
        if limit == 0 || limit > MAX_RETENTION_RELEASE_BATCH {
            return Err(invalid_command());
        }
        let rows = sqlx::query(
            "SELECT r.run_id FROM workflow_runs r
             LEFT JOIN artifact_retention_releases release ON release.run_id=r.run_id
             WHERE r.lifecycle IN ('succeeded','failed','cancelled','interrupted','timed_out')
               AND r.admission_state='closed' AND r.terminal_at IS NOT NULL
               AND release.run_id IS NULL
             ORDER BY r.terminal_at,r.run_id LIMIT $1",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        rows.into_iter()
            .map(|row| {
                model_data(RunId::new(
                    row.try_get::<String, _>("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))
            })
            .collect()
    }

    async fn release_run_artifact_retention(
        &self,
        transition_key: TransitionKey,
        command: ReleaseRunArtifactRetentionCommand,
    ) -> Result<TransitionOutcome<ArtifactRetentionRelease>, RepositoryError> {
        let intent_hash = canonical_intent_hash(&command)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let mut existing = sqlx::query(
            "SELECT run_id,transition_key,intent_hash,event_id,event_seq,retain_until,artifact_count,
                    registration_kind
             FROM artifact_retention_releases
             WHERE run_id=$1 OR transition_key=$2 FOR UPDATE",
        )
        .bind(command.run_id().as_str())
        .bind(transition_key.as_str())
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if existing.len() > 1 {
            return Err(RepositoryError::intent_conflict());
        }
        if let Some(row) = existing.pop() {
            let same_run = row.try_get::<String, _>("run_id").ok().as_deref()
                == Some(command.run_id().as_str());
            let terminal_atomic = row
                .try_get::<String, _>("registration_kind")
                .ok()
                .as_deref()
                == Some("terminal_atomic");
            let same_transition = row.try_get::<String, _>("transition_key").ok().as_deref()
                == Some(transition_key.as_str());
            if !same_run || (!terminal_atomic && !same_transition) {
                return Err(RepositoryError::intent_conflict());
            }
            let receipt = postgres_retention_release_from_row(&row)?;
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: receipt,
            });
        }
        let run = sqlx::query(
            "SELECT lifecycle,admission_state,terminal_at,projection_version,
                    artifact_reference_retention_seconds
             FROM workflow_runs WHERE run_id=$1 FOR UPDATE",
        )
        .bind(command.run_id().as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(run) = run else {
            return Err(run_not_found());
        };
        let lifecycle: String = run
            .try_get("lifecycle")
            .map_err(|_| RepositoryError::invalid_data())?;
        let admission: String = run
            .try_get("admission_state")
            .map_err(|_| RepositoryError::invalid_data())?;
        let terminal_at: Option<DateTime<Utc>> = run
            .try_get("terminal_at")
            .map_err(|_| RepositoryError::invalid_data())?;
        if !matches!(
            lifecycle.as_str(),
            "succeeded" | "failed" | "cancelled" | "interrupted" | "timed_out"
        ) || admission != "closed"
        {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let Some(terminal_at) = terminal_at else {
            return Err(RepositoryError::invalid_data());
        };
        let frozen_retention_seconds = run
            .try_get::<i64, _>("artifact_reference_retention_seconds")
            .map_err(|_| RepositoryError::invalid_data())?;
        if !(1..=i64::from(MAX_RETENTION_SECONDS)).contains(&frozen_retention_seconds) {
            return Err(RepositoryError::invalid_data());
        }
        let retain_until = sqlx::query_scalar::<_, DateTime<Utc>>(
            "SELECT $1::timestamptz + make_interval(secs => $2)",
        )
        .bind(terminal_at)
        .bind(i32::try_from(frozen_retention_seconds).map_err(|_| invalid_command())?)
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let artifact_count = u64_from_i64(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM artifacts WHERE run_id=$1 AND artifact_state='referenced'",
            )
            .bind(command.run_id().as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?,
        )?;
        sqlx::query(
            "UPDATE artifacts SET retain_until=CASE
                 WHEN retain_until IS NULL OR retain_until<$1 THEN $1 ELSE retain_until END
             WHERE run_id=$2 AND artifact_state='referenced'",
        )
        .bind(retain_until)
        .bind(command.run_id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let event_seq =
            super::postgres::allocate_event_seq(&mut transaction, command.run_id()).await?;
        let id = event_id(&transition_key);
        let event = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone()),
            ExecutionEventPayload::ProjectionMutated {
                mutation: ProjectionMutationKind::ArtifactRetentionReleased,
            },
        ))?;
        let projection_version = u64_from_i64(
            run.try_get("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        super::postgres::insert_event(
            &mut transaction,
            command.run_id(),
            event_seq,
            &id,
            &transition_key,
            intent_hash.as_str(),
            projection_version,
            &event,
        )
        .await?;
        sqlx::query(
            "INSERT INTO artifact_retention_releases
             (run_id,transition_key,intent_hash,event_id,event_seq,retain_until,artifact_count,
              created_at,registration_kind)
             VALUES ($1,$2,$3,$4,$5,$6,$7,CURRENT_TIMESTAMP,'legacy')",
        )
        .bind(command.run_id().as_str())
        .bind(transition_key.as_str())
        .bind(intent_hash.as_str())
        .bind(&id)
        .bind(i64_from_u64(event_seq)?)
        .bind(retain_until)
        .bind(i64_from_u64(artifact_count)?)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        super::postgres_projection::finalize_projection_checkpoints(
            &mut transaction,
            command.run_id(),
            &id,
        )
        .await?;
        let receipt = ArtifactRetentionRelease::new(
            command.run_id().clone(),
            id,
            event_seq,
            retain_until,
            artifact_count,
        );
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn sweep_orphan_artifacts(
        &self,
        transition_key: TransitionKey,
        command: OrphanSweepCommand,
    ) -> Result<TransitionOutcome<OrphanSweepBatch>, RepositoryError> {
        let intent_hash = canonical_intent_hash(&command)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let inserted = sqlx::query(
            "INSERT INTO artifact_gc_sweeps (
                transition_key, intent_hash, claimed_by, created_at
             ) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
             ON CONFLICT (transition_key) DO NOTHING",
        )
        .bind(transition_key.as_str())
        .bind(intent_hash.as_str())
        .bind(command.claimed_by())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if inserted.rows_affected() == 0 {
            let existing = sqlx::query_scalar::<_, String>(
                "SELECT intent_hash FROM artifact_gc_sweeps
                 WHERE transition_key = $1 FOR UPDATE",
            )
            .bind(transition_key.as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if existing != intent_hash.as_str() {
                return Err(RepositoryError::intent_conflict());
            }
            let outcome = TransitionOutcome::ExactReplay {
                authoritative: OrphanSweepBatch {
                    claims: load_postgres_sweep_claims(&mut transaction, &transition_key).await?,
                },
            };
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(outcome);
        }
        let claim_expires_at = sqlx::query_scalar::<_, DateTime<Utc>>(
            "SELECT CURRENT_TIMESTAMP + make_interval(secs => $1)",
        )
        .bind(i32::try_from(command.claim_seconds()).map_err(|_| invalid_command())?)
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let orphan_retention_seconds =
            i32::try_from(command.orphan_retention_seconds()).map_err(|_| invalid_command())?;
        let rows = sqlx::query(POSTGRES_ORPHAN_CANDIDATES)
            .bind(orphan_retention_seconds)
            .bind(i64::from(command.limit()))
            .fetch_all(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
        let mut claims = Vec::with_capacity(rows.len());
        let mut seen_objects = HashSet::new();
        for row in rows {
            let run_id = model_data(RunId::new(
                row.try_get::<String, _>("run_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let artifact = artifact_ref(
                row.try_get("artifact_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
                row.try_get("content_hash")
                    .map_err(|_| RepositoryError::invalid_data())?,
                row.try_get("size_bytes")
                    .map_err(|_| RepositoryError::invalid_data())?,
                row.try_get("media_type")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let storage_locator = repository_adapter::storage_locator_from_validated_parts(
                row.try_get("storage_uri")
                    .map_err(|_| RepositoryError::invalid_data())?,
            );
            if !seen_objects.insert(artifact_object_key(&artifact, &storage_locator)) {
                continue;
            }
            lock_postgres_artifact_object(
                &mut transaction,
                artifact.content_hash(),
                &storage_locator,
            )
            .await?;
            let object_rows = load_postgres_artifact_object_rows(
                &mut transaction,
                artifact.content_hash(),
                &storage_locator,
                orphan_retention_seconds,
            )
            .await?;
            if object_rows.is_empty()
                || object_rows
                    .iter()
                    .all(|row| row.state == ArtifactState::Deleted)
                || object_rows.iter().any(|row| !row.deletion_eligible)
            {
                continue;
            }
            let claim = ArtifactDeletionClaim::new(
                &transition_key,
                run_id,
                artifact,
                storage_locator,
                command.claimed_by().to_owned(),
                claim_expires_at,
            );
            let updated = sqlx::query(
                "UPDATE artifacts
                 SET artifact_state = 'deleting', deletion_fence = $1,
                     deletion_claim_token = $2, deletion_claimed_by = $3,
                     deletion_claim_request_key = $4, deletion_claimed_at = CURRENT_TIMESTAMP,
                     deletion_claim_expires_at = $5, referenced_at = NULL
                 WHERE run_id = $6 AND artifact_id = $7
                   AND ((artifact_state IN ('staged', 'verified')
                        AND created_at <= CURRENT_TIMESTAMP - make_interval(secs => $8)
                        AND (retain_until IS NULL OR retain_until <= CURRENT_TIMESTAMP)
                        AND NOT EXISTS (SELECT 1 FROM workflow_runs r
                                        WHERE r.run_id=artifacts.run_id
                                          AND r.output_artifact_id=artifacts.artifact_id)
                        AND NOT EXISTS (SELECT 1 FROM node_activations n
                                        WHERE n.run_id=artifacts.run_id
                                          AND n.output_artifact_id=artifacts.artifact_id)
                        AND NOT EXISTS (SELECT 1 FROM node_attempts t
                                        WHERE t.run_id=artifacts.run_id
                                          AND t.output_artifact_id=artifacts.artifact_id)
                        AND NOT EXISTS (SELECT 1 FROM join_arrivals j
                                        WHERE j.run_id=artifacts.run_id
                                          AND j.value_artifact_id=artifacts.artifact_id)
                        AND NOT EXISTS (SELECT 1 FROM scheduler_values sv
                                        WHERE sv.run_id=artifacts.run_id
                                          AND sv.artifact_id=artifacts.artifact_id)
                        AND NOT EXISTS (SELECT 1 FROM scheduler_occurrence_values sov
                                        WHERE sov.run_id=artifacts.run_id
                                          AND sov.artifact_id=artifacts.artifact_id)
                        AND NOT EXISTS (
                          SELECT 1 FROM recovery_artifact_roots rr
                          LEFT JOIN artifact_retention_releases root_release
                            ON root_release.run_id=rr.run_id
                          WHERE rr.artifact_run_id=artifacts.run_id
                            AND rr.artifact_id=artifacts.artifact_id
                            AND (root_release.run_id IS NULL
                                 OR root_release.retain_until>CURRENT_TIMESTAMP)
                        ))
                        OR (artifact_state = 'referenced'
                            AND EXISTS (
                              SELECT 1 FROM artifact_retention_releases own_release
                              WHERE own_release.run_id=artifacts.run_id
                                AND own_release.retain_until<=CURRENT_TIMESTAMP
                            )
                            AND NOT EXISTS (
                              SELECT 1 FROM recovery_artifact_roots rr
                              LEFT JOIN artifact_retention_releases root_release
                                ON root_release.run_id=rr.run_id
                              WHERE rr.artifact_run_id=artifacts.run_id
                                AND rr.artifact_id=artifacts.artifact_id
                                AND (root_release.run_id IS NULL
                                     OR root_release.retain_until>CURRENT_TIMESTAMP)
                            ))
                        OR (artifact_state = 'deleting'
                            AND deletion_claim_expires_at <= CURRENT_TIMESTAMP))",
            )
            .bind(claim.deletion_fence().as_str())
            .bind(claim.claim_token().as_str())
            .bind(claim.claimed_by())
            .bind(transition_key.as_str())
            .bind(claim.claim_expires_at())
            .bind(claim.run_id().as_str())
            .bind(claim.artifact().artifact_id().as_str())
            .bind(orphan_retention_seconds)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if updated.rows_affected() != 1 {
                transaction
                    .rollback()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Err(RepositoryError::invalid_data());
            }
            for object_row in object_rows.iter().filter(|row| {
                row.state != ArtifactState::Deleted
                    && (row.run_id != *claim.run_id()
                        || row.artifact.artifact_id() != claim.artifact().artifact_id())
            }) {
                let row_claim_token = artifact_row_claim_token(
                    claim.claim_token(),
                    &object_row.run_id,
                    object_row.artifact.artifact_id(),
                );
                let updated = sqlx::query(
                    "UPDATE artifacts
                     SET artifact_state='deleting',deletion_fence=$1,deletion_claim_token=$2,
                         deletion_claimed_by=$3,deletion_claim_request_key=$4,
                         deletion_claimed_at=CURRENT_TIMESTAMP,deletion_claim_expires_at=$5,
                         referenced_at=NULL
                     WHERE run_id=$6 AND artifact_id=$7 AND artifact_state<>'deleted'",
                )
                .bind(claim.deletion_fence().as_str())
                .bind(row_claim_token.as_str())
                .bind(claim.claimed_by())
                .bind(transition_key.as_str())
                .bind(claim.claim_expires_at())
                .bind(object_row.run_id.as_str())
                .bind(object_row.artifact.artifact_id().as_str())
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
                if updated.rows_affected() != 1 {
                    return Err(RepositoryError::invalid_data());
                }
            }
            sqlx::query(
                "INSERT INTO artifact_gc_claims (
                    transition_key, run_id, artifact_id, claim_token,
                    deletion_fence, claim_expires_at
                 ) VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(transition_key.as_str())
            .bind(claim.run_id().as_str())
            .bind(claim.artifact().artifact_id().as_str())
            .bind(claim.claim_token().as_str())
            .bind(claim.deletion_fence().as_str())
            .bind(claim.claim_expires_at())
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            claims.push(claim);
        }
        sort_deletion_claims(&mut claims);
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed {
            result: OrphanSweepBatch { claims },
        })
    }

    async fn acknowledge_artifact_deleted(
        &self,
        command: AcknowledgeArtifactDeletionCommand,
    ) -> Result<TransitionOutcome<ArtifactReceipt>, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let outcome = acknowledge_postgres_deletion(&mut transaction, &command).await?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(outcome)
    }
}

async fn load_postgres_sweep_claims(
    transaction: &mut Transaction<'_, Postgres>,
    transition_key: &TransitionKey,
) -> Result<Vec<ArtifactDeletionClaim>, RepositoryError> {
    let rows = sqlx::query(
        "SELECT c.run_id, c.artifact_id, c.claim_token, c.deletion_fence,
                c.claim_expires_at, s.claimed_by, a.content_hash, a.size_bytes,
                a.media_type, a.storage_uri
         FROM artifact_gc_claims c
         JOIN artifact_gc_sweeps s ON s.transition_key = c.transition_key
         JOIN artifacts a ON a.run_id = c.run_id AND a.artifact_id = c.artifact_id
         WHERE c.transition_key = $1 ORDER BY c.run_id, c.artifact_id",
    )
    .bind(transition_key.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    rows.into_iter()
        .map(|row| {
            Ok(ArtifactDeletionClaim {
                run_id: model_data(RunId::new(
                    row.try_get::<String, _>("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                artifact: artifact_ref(
                    row.try_get("artifact_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("content_hash")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("size_bytes")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("media_type")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                storage_locator: repository_adapter::storage_locator_from_validated_parts(
                    row.try_get("storage_uri")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ),
                deletion_fence: model_data(ContentHash::parse(
                    row.try_get::<String, _>("deletion_fence")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                claim_token: model_data(ContentHash::parse(
                    row.try_get::<String, _>("claim_token")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                claimed_by: row
                    .try_get("claimed_by")
                    .map_err(|_| RepositoryError::invalid_data())?,
                claim_expires_at: row
                    .try_get("claim_expires_at")
                    .map_err(|_| RepositoryError::invalid_data())?,
            })
        })
        .collect()
}

async fn acknowledge_postgres_deletion(
    transaction: &mut Transaction<'_, Postgres>,
    command: &AcknowledgeArtifactDeletionCommand,
) -> Result<TransitionOutcome<ArtifactReceipt>, RepositoryError> {
    let object_identity = sqlx::query(
        "SELECT content_hash, storage_uri
         FROM artifacts WHERE run_id = $1 AND artifact_id = $2",
    )
    .bind(command.run_id().as_str())
    .bind(command.artifact().artifact_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(object_identity) = object_identity else {
        return Ok(TransitionOutcome::StateConflict);
    };
    let stored_content_hash = model_data(ContentHash::parse(
        object_identity
            .try_get::<String, _>("content_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let storage_locator = repository_adapter::storage_locator_from_validated_parts(
        object_identity
            .try_get("storage_uri")
            .map_err(|_| RepositoryError::invalid_data())?,
    );
    if stored_content_hash != *command.artifact().content_hash() {
        return Ok(TransitionOutcome::StateConflict);
    }
    lock_postgres_artifact_object(transaction, &stored_content_hash, &storage_locator).await?;
    let row = sqlx::query(
        "SELECT artifact_state, content_hash, size_bytes, media_type,
                storage_uri, deletion_fence, deletion_claim_token, deletion_claimed_by,
                deletion_claim_request_key
         FROM artifacts WHERE run_id = $1 AND artifact_id = $2 FOR UPDATE",
    )
    .bind(command.run_id().as_str())
    .bind(command.artifact().artifact_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(TransitionOutcome::StateConflict);
    };
    let state = ArtifactState::parse(
        &row.try_get::<String, _>("artifact_state")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let locked_storage_locator: String = row
        .try_get("storage_uri")
        .map_err(|_| RepositoryError::invalid_data())?;
    let claim_request_key: Option<String> = row
        .try_get("deletion_claim_request_key")
        .map_err(|_| RepositoryError::invalid_data())?;
    let exact = row
        .try_get::<String, _>("content_hash")
        .map_err(|_| RepositoryError::invalid_data())?
        == command.artifact().content_hash().as_str()
        && u64_from_i64(
            row.try_get("size_bytes")
                .map_err(|_| RepositoryError::invalid_data())?,
        )? == command.artifact().size_bytes()
        && row
            .try_get::<Option<String>, _>("deletion_fence")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            == Some(command.deletion_fence().as_str())
        && row
            .try_get::<Option<String>, _>("deletion_claim_token")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            == Some(command.claim_token().as_str())
        && row
            .try_get::<Option<String>, _>("deletion_claimed_by")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            == Some(command.claimed_by())
        && locked_storage_locator == storage_locator.expose_to_storage_adapter();
    let Some(claim_request_key) = claim_request_key else {
        return Ok(TransitionOutcome::StateConflict);
    };
    if !exact {
        return Ok(TransitionOutcome::StateConflict);
    }
    let receipt = ArtifactReceipt::new(
        command.run_id().clone(),
        command.artifact().clone(),
        ArtifactState::Deleted,
    );
    let object_rows = sqlx::query(
        "SELECT run_id, artifact_id, artifact_state, deletion_fence,
                deletion_claim_token, deletion_claimed_by, deletion_claim_request_key
         FROM artifacts
         WHERE content_hash = $1 AND storage_uri = $2
         ORDER BY run_id, artifact_id
         FOR UPDATE",
    )
    .bind(command.artifact().content_hash().as_str())
    .bind(storage_locator.expose_to_storage_adapter())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let mut deleting_rows = 0_u64;
    let mut claimed_rows = 0_u64;
    for object_row in object_rows {
        let row_run_id = model_data(RunId::new(
            object_row
                .try_get::<String, _>("run_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let row_artifact_id = model_data(ArtifactId::new(
            object_row
                .try_get::<String, _>("artifact_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let row_state = ArtifactState::parse(
            &object_row
                .try_get::<String, _>("artifact_state")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let same_claim = object_row
            .try_get::<Option<String>, _>("deletion_fence")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            == Some(command.deletion_fence().as_str())
            && object_row
                .try_get::<Option<String>, _>("deletion_claim_request_key")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_deref()
                == Some(claim_request_key.as_str())
            && object_row
                .try_get::<Option<String>, _>("deletion_claimed_by")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_deref()
                == Some(command.claimed_by());
        if !same_claim {
            if row_state != ArtifactState::Deleted {
                return Ok(TransitionOutcome::StateConflict);
            }
            continue;
        }
        let expected_token = if row_run_id == *command.run_id()
            && row_artifact_id == *command.artifact().artifact_id()
        {
            command.claim_token().clone()
        } else {
            artifact_row_claim_token(command.claim_token(), &row_run_id, &row_artifact_id)
        };
        if object_row
            .try_get::<Option<String>, _>("deletion_claim_token")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            != Some(expected_token.as_str())
        {
            return Ok(TransitionOutcome::StateConflict);
        }
        claimed_rows += 1;
        if row_state == ArtifactState::Deleting {
            deleting_rows += 1;
        } else if row_state != ArtifactState::Deleted {
            return Ok(TransitionOutcome::StateConflict);
        }
    }
    if claimed_rows == 0 {
        return Ok(TransitionOutcome::StateConflict);
    }
    if deleting_rows == 0 {
        return Ok(if state == ArtifactState::Deleted {
            TransitionOutcome::ExactReplay {
                authoritative: receipt,
            }
        } else {
            TransitionOutcome::StateConflict
        });
    }
    if state != ArtifactState::Deleting {
        return Ok(TransitionOutcome::StateConflict);
    }
    let updated = sqlx::query(
        "UPDATE artifacts SET artifact_state = 'deleted'
         WHERE content_hash = $1 AND storage_uri = $2 AND artifact_state = 'deleting'
           AND deletion_fence = $3 AND deletion_claim_request_key = $4
           AND deletion_claimed_by = $5",
    )
    .bind(command.artifact().content_hash().as_str())
    .bind(storage_locator.expose_to_storage_adapter())
    .bind(command.deletion_fence().as_str())
    .bind(&claim_request_key)
    .bind(command.claimed_by())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(if updated.rows_affected() == deleting_rows {
        TransitionOutcome::Committed { result: receipt }
    } else {
        TransitionOutcome::StateConflict
    })
}

pub(crate) async fn reference_postgres_artifact(
    transaction: &mut Transaction<'_, Postgres>,
    command: &ReferenceArtifactCommand,
) -> Result<TransitionOutcome<ArtifactReferenceAuthority>, RepositoryError> {
    if !validate_postgres_reference_target(transaction, command).await? {
        return Ok(TransitionOutcome::StateConflict);
    }
    let state = sqlx::query_scalar::<_, String>(
        "SELECT artifact_state FROM artifacts
         WHERE run_id = $1 AND artifact_id = $2 AND content_hash = $3 AND size_bytes = $4
         FOR UPDATE",
    )
    .bind(command.run_id().as_str())
    .bind(command.artifact().artifact_id().as_str())
    .bind(command.artifact().content_hash().as_str())
    .bind(i64_from_u64(command.artifact().size_bytes())?)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(state) = state else {
        return Ok(TransitionOutcome::StateConflict);
    };
    let state = ArtifactState::parse(&state)?;
    let authority = |state| {
        ArtifactReferenceAuthority::new(
            ArtifactReceipt::new(command.run_id().clone(), command.artifact().clone(), state),
            command.target().clone(),
        )
    };
    match state {
        ArtifactState::Referenced => Ok(TransitionOutcome::ExactReplay {
            authoritative: authority(state),
        }),
        ArtifactState::Verified => {
            let updated = sqlx::query(
                "UPDATE artifacts
                 SET artifact_state = 'referenced', referenced_at = CURRENT_TIMESTAMP
                 WHERE run_id = $1 AND artifact_id = $2 AND artifact_state = 'verified'
                   AND content_hash = $3 AND size_bytes = $4",
            )
            .bind(command.run_id().as_str())
            .bind(command.artifact().artifact_id().as_str())
            .bind(command.artifact().content_hash().as_str())
            .bind(i64_from_u64(command.artifact().size_bytes())?)
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if updated.rows_affected() == 1 {
                Ok(TransitionOutcome::Committed {
                    result: authority(ArtifactState::Referenced),
                })
            } else {
                Ok(TransitionOutcome::StateConflict)
            }
        }
        ArtifactState::Staged | ArtifactState::Deleting | ArtifactState::Deleted => {
            Ok(TransitionOutcome::StateConflict)
        }
    }
}

async fn validate_postgres_reference_target(
    transaction: &mut Transaction<'_, Postgres>,
    command: &ReferenceArtifactCommand,
) -> Result<bool, RepositoryError> {
    let row = match command.target() {
        ArtifactReferenceTarget::Run { .. } => sqlx::query(
            "SELECT lifecycle, projection_version, output_artifact_id, output_value_hash
             FROM workflow_runs WHERE run_id = $1 FOR UPDATE",
        )
        .bind(command.run_id().as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?,
        ArtifactReferenceTarget::Activation { activation_id, .. } => sqlx::query(
            "SELECT lifecycle, projection_version, output_artifact_id, output_value_hash
             FROM node_activations WHERE run_id = $1 AND activation_id = $2 FOR UPDATE",
        )
        .bind(command.run_id().as_str())
        .bind(activation_id.as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?,
    };
    let row = row.ok_or_else(|| match command.target() {
        ArtifactReferenceTarget::Run { .. } => run_not_found(),
        ArtifactReferenceTarget::Activation { .. } => RepositoryError::activation_not_found(),
    })?;
    let expected_version = match command.target() {
        ArtifactReferenceTarget::Run {
            expected_projection_version,
        }
        | ArtifactReferenceTarget::Activation {
            expected_projection_version,
            ..
        } => *expected_projection_version,
    };
    let lifecycle: String = row
        .try_get("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?;
    let stored_version: i64 = row
        .try_get("projection_version")
        .map_err(|_| RepositoryError::invalid_data())?;
    let artifact_id: Option<String> = row
        .try_get("output_artifact_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let value_hash: Option<String> = row
        .try_get("output_value_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    if lifecycle != "succeeded"
        || u64_from_i64(stored_version)? != expected_version
        || artifact_id.as_deref() != Some(command.artifact().artifact_id().as_str())
        || value_hash.as_deref() != Some(command.artifact().content_hash().as_str())
    {
        return Ok(false);
    }
    Ok(true)
}
