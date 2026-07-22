//! Backend-neutral durable payload and Artifact metadata contracts.

use super::RepositoryErrorExt as _;

use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use insight_engine::{
    ActivationId, ArtifactId, ArtifactRef, ContentHash, InlineValueRef, RunId, TransitionKey,
    TransitionOutcome,
};

use super::{
    common::payload_id, DurableRepository, RepositoryError, StorageLocator,
    REPOSITORY_ARTIFACT_STORE_CONFLICT, REPOSITORY_CONFIGURATION_INVALID,
};

const MAX_SWEEP_BATCH: u32 = 1_000;
const MAX_CLAIMANT_BYTES: usize = 256;
const MAX_CLAIM_SECONDS: u32 = 3_600;
const MAX_RETENTION_SECONDS: u32 = 10 * 365 * 24 * 60 * 60;
const SHARED_FILESYSTEM_BACKEND: &str = "shared_filesystem";
const MAX_ARTIFACT_STORE_NAMESPACE_BYTES: usize = 128;

fn invalid_command() -> RepositoryError {
    RepositoryError::new(
        REPOSITORY_CONFIGURATION_INVALID,
        "durable repository command is invalid",
    )
}

fn model_data<T>(value: Result<T, insight_engine::ModelError>) -> Result<T, RepositoryError> {
    value.map_err(|_| RepositoryError::invalid_data())
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

/// Workspace-internal construction surface for storage adapters.
#[doc(hidden)]
pub mod adapter {
    use chrono::{DateTime, Utc};
    use serde_json::Value;

    use insight_engine::{ArtifactRef, ContentHash, InlineValueRef, RunId, TransitionKey};

    use super::{
        ArtifactDeletionClaim, ArtifactReceipt, ArtifactReferenceAuthority,
        ArtifactReferenceTarget, ArtifactRetentionRelease, ArtifactState, ArtifactStoreAuthority,
        BindArtifactStoreAuthorityCommand, OrphanSweepBatch, PayloadId, PayloadReceipt,
        RetainedArtifact, StageArtifactCommand, StorageLocator, StoredInlinePayload,
    };
    use crate::RepositoryError;

    pub fn artifact_store_conflict() -> RepositoryError {
        super::artifact_store_conflict()
    }

    pub fn payload_id_from_hash(hash: &ContentHash) -> PayloadId {
        PayloadId::from_hash(hash)
    }

    pub fn payload_receipt_from_inline(run_id: RunId, value: &InlineValueRef) -> PayloadReceipt {
        PayloadReceipt::from_inline(run_id, value)
    }

    pub fn stored_inline_payload(receipt: PayloadReceipt, value: Value) -> StoredInlinePayload {
        StoredInlinePayload { receipt, value }
    }

    pub fn artifact_state(value: &str) -> Result<ArtifactState, RepositoryError> {
        ArtifactState::parse(value)
    }

    pub fn stage_storage_locator(command: &StageArtifactCommand) -> &StorageLocator {
        command.storage_locator()
    }

    pub fn retained_artifact(
        run_id: RunId,
        artifact: ArtifactRef,
        storage_locator: StorageLocator,
    ) -> RetainedArtifact {
        RetainedArtifact::new(run_id, artifact, storage_locator)
    }

    pub fn artifact_receipt(
        run_id: RunId,
        artifact: ArtifactRef,
        state: ArtifactState,
    ) -> ArtifactReceipt {
        ArtifactReceipt::new(run_id, artifact, state)
    }

    pub fn artifact_reference_authority(
        receipt: ArtifactReceipt,
        target: ArtifactReferenceTarget,
    ) -> ArtifactReferenceAuthority {
        ArtifactReferenceAuthority::new(receipt, target)
    }

    pub fn artifact_retention_release(
        run_id: RunId,
        event_id: String,
        event_seq: u64,
        retain_until: DateTime<Utc>,
        artifact_count: u64,
    ) -> ArtifactRetentionRelease {
        ArtifactRetentionRelease::new(run_id, event_id, event_seq, retain_until, artifact_count)
    }

    pub fn artifact_deletion_claim(
        transition_key: &TransitionKey,
        run_id: RunId,
        artifact: ArtifactRef,
        storage_locator: StorageLocator,
        claimed_by: String,
        claim_expires_at: DateTime<Utc>,
    ) -> ArtifactDeletionClaim {
        ArtifactDeletionClaim::new(
            transition_key,
            run_id,
            artifact,
            storage_locator,
            claimed_by,
            claim_expires_at,
        )
    }

    pub fn artifact_deletion_claim_from_parts(
        run_id: RunId,
        artifact: ArtifactRef,
        storage_locator: StorageLocator,
        deletion_fence: ContentHash,
        claim_token: ContentHash,
        claimed_by: String,
        claim_expires_at: DateTime<Utc>,
    ) -> ArtifactDeletionClaim {
        ArtifactDeletionClaim {
            run_id,
            artifact,
            storage_locator,
            deletion_fence,
            claim_token,
            claimed_by,
            claim_expires_at,
        }
    }

    pub fn orphan_sweep_batch(claims: Vec<ArtifactDeletionClaim>) -> OrphanSweepBatch {
        OrphanSweepBatch { claims }
    }

    pub fn artifact_store_authority(
        backend: String,
        namespace: String,
        store_id: String,
        bound_at: DateTime<Utc>,
    ) -> Result<ArtifactStoreAuthority, RepositoryError> {
        ArtifactStoreAuthority::new(backend, namespace, store_id, bound_at)
    }

    pub fn artifact_store_authority_matches(
        authority: &ArtifactStoreAuthority,
        command: &BindArtifactStoreAuthorityCommand,
    ) -> bool {
        authority.matches(command)
    }
}
