//! Artifact lifecycle commands and transaction ports for the Platform v1 shared persistence model.
//!
//! This crate owns closed Artifact semantics but no SQL, object-store client, HTTP route, scanner,
//! or worker process. Adapters must execute commands inside caller-owned transactions.

#![allow(async_fn_in_trait)]

mod read;
mod skill_package;
mod work;

pub use read::*;
pub use skill_package::*;
pub use work::*;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    canonical_digest, ArtifactGrantOperation, ArtifactPurpose, ArtifactRef, ArtifactReferenceKind,
    ArtifactState, ArtifactWorkloadAudience, BlobIntegrityState, CommandAudit, CommandOutcome,
    DataClassification, ExactVersionRef, HardLimitProfile, JobState, ResourceId, ResourceKind,
    Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

const MAX_BACKEND_BYTES: usize = 64;
const MAX_KEY_ID_BYTES: usize = 255;
const MAX_MEDIA_TYPE_BYTES: usize = 255;
const MAX_DISPLAY_NAME_BYTES: usize = 255;
const MAX_OBJECT_REFERENCE_BYTES: usize = 16_384;
const MAX_OBJECT_GENERATION_BYTES: usize = 255;
pub(crate) const MAX_ARTIFACT_RETRY_BACKOFF_MILLISECONDS: u64 = 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactCommandLimits {
    maximum_single_bytes: u64,
    maximum_staging_seconds: i64,
    maximum_job_attempts: u32,
    maximum_links_per_artifact: u64,
    maximum_provenance_depth: u64,
}

impl ArtifactCommandLimits {
    pub fn from_profile(profile: &HardLimitProfile) -> Result<Self, ArtifactCommandError> {
        profile
            .validate()
            .map_err(|failure| ArtifactCommandError::InvalidProfile(failure.to_string()))?;
        let maximum_single_bytes = profile.artifact.single_bytes.q1_default;
        let maximum_staging_seconds = i64::try_from(profile.artifact.staging_seconds.q1_default)
            .map_err(|_| ArtifactCommandError::InvalidLimits)?;
        let maximum_job_attempts =
            u32::try_from(profile.run_scheduler.attempts_per_work.q1_default)
                .map_err(|_| ArtifactCommandError::InvalidLimits)?;
        let maximum_links_per_artifact = profile.artifact.references_per_artifact.q1_default;
        let maximum_provenance_depth = profile.registry_plan.dependency_closure.q1_default;
        let limits = Self {
            maximum_single_bytes,
            maximum_staging_seconds,
            maximum_job_attempts,
            maximum_links_per_artifact,
            maximum_provenance_depth,
        };
        if maximum_single_bytes == 0
            || maximum_staging_seconds <= 0
            || maximum_job_attempts == 0
            || maximum_links_per_artifact == 0
            || maximum_provenance_depth == 0
        {
            return Err(ArtifactCommandError::InvalidLimits);
        }
        Ok(limits)
    }

    pub const fn maximum_single_bytes(self) -> u64 {
        self.maximum_single_bytes
    }

    pub const fn maximum_staging_seconds(self) -> i64 {
        self.maximum_staging_seconds
    }

    pub const fn maximum_job_attempts(self) -> u32 {
        self.maximum_job_attempts
    }

    pub const fn maximum_links_per_artifact(self) -> u64 {
        self.maximum_links_per_artifact
    }

    pub const fn maximum_provenance_depth(self) -> u64 {
        self.maximum_provenance_depth
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactMetadataSnapshot {
    pub schema_version: u32,
    pub display_name: Option<String>,
    pub operation_id: ResourceId,
    pub current_verification: Option<ArtifactCurrentVerification>,
}

impl ArtifactMetadataSnapshot {
    pub fn new(
        display_name: Option<String>,
        operation_id: ResourceId,
    ) -> Result<Self, ArtifactCommandError> {
        if operation_id.kind() != ResourceKind::Job
            || display_name.as_deref().is_some_and(|value| {
                value.is_empty()
                    || value.len() > MAX_DISPLAY_NAME_BYTES
                    || value == "."
                    || value == ".."
                    || value
                        .chars()
                        .any(|character| character.is_control() || matches!(character, '/' | '\\'))
            })
        {
            return Err(ArtifactCommandError::InvalidMetadata);
        }
        Ok(Self {
            schema_version: 1,
            display_name,
            operation_id,
            current_verification: None,
        })
    }

    pub fn validate(&self) -> Result<(), ArtifactCommandError> {
        if self.schema_version != 1
            || self.operation_id.kind() != ResourceKind::Job
            || self.display_name.as_deref().is_some_and(|value| {
                value.is_empty()
                    || value.len() > MAX_DISPLAY_NAME_BYTES
                    || value == "."
                    || value == ".."
                    || value
                        .chars()
                        .any(|character| character.is_control() || matches!(character, '/' | '\\'))
            })
            || self
                .current_verification
                .as_ref()
                .is_some_and(|evidence| evidence.validate().is_err())
        {
            return Err(ArtifactCommandError::InvalidMetadata);
        }
        Ok(())
    }

    pub fn with_current_verification(
        &self,
        evidence: ArtifactCurrentVerification,
    ) -> Result<Self, ArtifactCommandError> {
        evidence.validate()?;
        let mut next = self.clone();
        next.current_verification = Some(evidence);
        next.validate()?;
        Ok(next)
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, ArtifactCommandError> {
        self.validate()?;
        digest(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactBlobSecurityDomain {
    pub schema_version: u32,
    pub classification: DataClassification,
    pub retention_policy_revision_id: ResourceId,
    pub encryption_domain_id: ResourceId,
}

impl ArtifactBlobSecurityDomain {
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ArtifactCommandError> {
        if self.schema_version != 1
            || self.retention_policy_revision_id.kind() != ResourceKind::PolicyRevision
            || self.encryption_domain_id.kind() != ResourceKind::EncryptionDomain
        {
            return Err(ArtifactCommandError::InvalidStorageBinding);
        }
        digest(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactUploadOperationSnapshot {
    pub schema_version: u32,
    pub artifact_id: ResourceId,
    pub purpose: ArtifactPurpose,
    pub expected_size_bytes: u64,
    pub expected_digest: Option<Sha256Digest>,
    pub retention_policy_revision_id: ResourceId,
    pub scan_policy_revision: ExactVersionRef,
    pub scanner_contract_digest: Sha256Digest,
    pub ruleset_digest: Sha256Digest,
    pub evidence_ttl_milliseconds: u64,
    pub retry_backoff_milliseconds: u64,
}

impl ArtifactUploadOperationSnapshot {
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ArtifactCommandError> {
        if self.schema_version != 1
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.retention_policy_revision_id.kind() != ResourceKind::PolicyRevision
            || self.scan_policy_revision.resource_kind != ResourceKind::PolicyRevision
            || self.scan_policy_revision.validate().is_err()
            || self.expected_size_bytes == 0
            || self.evidence_ttl_milliseconds == 0
            || self.evidence_ttl_milliseconds > 86_400_000
            || self.retry_backoff_milliseconds == 0
            || self.retry_backoff_milliseconds > MAX_ARTIFACT_RETRY_BACKOFF_MILLISECONDS
        {
            return Err(ArtifactCommandError::InvalidVerification);
        }
        digest(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadGrantSnapshot {
    pub schema_version: u32,
    pub artifact_id: ResourceId,
    pub operation_id: ResourceId,
    pub subject_principal_id: ResourceId,
    pub operations: Vec<ArtifactGrantOperation>,
    pub audience: ArtifactWorkloadAudience,
    pub purpose: ArtifactPurpose,
    pub max_bytes: u64,
    pub token_digest: Sha256Digest,
    pub expires_at: DateTime<Utc>,
    pub generation: u64,
}

impl UploadGrantSnapshot {
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ArtifactCommandError> {
        digest(self)
    }

    pub fn link_key_digest(&self) -> Result<Sha256Digest, ArtifactCommandError> {
        let identity = serde_json::json!({
            "artifact_id": self.artifact_id,
            "audience": self.audience,
            "generation": self.generation,
            "operation_id": self.operation_id,
            "operations": self.operations,
            "purpose": self.purpose,
            "schema_version": 1,
            "subject_principal_id": self.subject_principal_id,
        });
        canonical_digest(&identity)
            .map_err(|_| ArtifactCommandError::Canonicalization)?
            .parse()
            .map_err(|_| ArtifactCommandError::Canonicalization)
    }
}

#[derive(Debug, Clone)]
pub struct PrepareArtifact {
    pub audit: CommandAudit,
    pub operation_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub upload_grant_id: ResourceId,
    pub quota_account_id: ResourceId,
    pub quota_entry_id: ResourceId,
    pub purpose: ArtifactPurpose,
    pub classification: DataClassification,
    pub expected_size_bytes: u64,
    pub expected_digest: Option<Sha256Digest>,
    pub declared_media_type: Option<String>,
    pub retention_policy_revision_id: ResourceId,
    pub scan_policy_revision: ExactVersionRef,
    pub scanner_contract_digest: Sha256Digest,
    pub ruleset_digest: Sha256Digest,
    pub evidence_ttl_milliseconds: u64,
    pub retry_backoff_milliseconds: u64,
    pub retain_until: DateTime<Utc>,
    pub operation_deadline: DateTime<Utc>,
    pub grant_expires_at: DateTime<Utc>,
    pub grant_token_digest: Sha256Digest,
    pub storage_backend: String,
    pub storage_binding_digest: Sha256Digest,
    pub object_reference_ciphertext: Vec<u8>,
    pub key_id: String,
    pub encryption_domain_id: ResourceId,
    pub display_name: Option<String>,
}

impl PrepareArtifact {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: ArtifactCommandLimits,
    ) -> Result<(), ArtifactCommandError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ArtifactCommandError::InvalidAudit)?;
        let ids = [
            (&self.operation_id, ResourceKind::Job),
            (&self.artifact_id, ResourceKind::Artifact),
            (&self.blob_id, ResourceKind::InternalBlob),
            (&self.upload_grant_id, ResourceKind::ArtifactGrant),
            (&self.quota_account_id, ResourceKind::QuotaAccount),
            (&self.quota_entry_id, ResourceKind::QuotaLedgerEntry),
            (
                &self.retention_policy_revision_id,
                ResourceKind::PolicyRevision,
            ),
            (&self.encryption_domain_id, ResourceKind::EncryptionDomain),
        ];
        if ids
            .iter()
            .any(|(actual, expected)| actual.kind() != *expected)
        {
            return Err(ArtifactCommandError::InvalidIdentity);
        }
        if self.expected_size_bytes > limits.maximum_single_bytes
            || self.operation_deadline <= now
            || self.grant_expires_at <= now
            || self.grant_expires_at > self.operation_deadline
            || self.retain_until < self.operation_deadline
        {
            return Err(ArtifactCommandError::InvalidTimeOrSize);
        }
        let maximum_staging = ChronoDuration::seconds(limits.maximum_staging_seconds);
        if self.grant_expires_at - now > maximum_staging {
            return Err(ArtifactCommandError::InvalidTimeOrSize);
        }
        if !is_code(&self.storage_backend, MAX_BACKEND_BYTES)
            || self.key_id.is_empty()
            || self.key_id.len() > MAX_KEY_ID_BYTES
            || self.object_reference_ciphertext.is_empty()
            || self.object_reference_ciphertext.len() > MAX_OBJECT_REFERENCE_BYTES
            || self
                .declared_media_type
                .as_deref()
                .is_some_and(|value| !is_media_type(value))
        {
            return Err(ArtifactCommandError::InvalidStorageBinding);
        }
        self.metadata_snapshot()?;
        self.operation_snapshot().canonical_digest()?;
        self.upload_grant_snapshot()?;
        self.blob_security_domain().canonical_digest()?;
        Ok(())
    }

    pub fn metadata_snapshot(&self) -> Result<ArtifactMetadataSnapshot, ArtifactCommandError> {
        ArtifactMetadataSnapshot::new(self.display_name.clone(), self.operation_id.clone())
    }

    pub fn operation_snapshot(&self) -> ArtifactUploadOperationSnapshot {
        ArtifactUploadOperationSnapshot {
            schema_version: 1,
            artifact_id: self.artifact_id.clone(),
            purpose: self.purpose,
            expected_size_bytes: self.expected_size_bytes,
            expected_digest: self.expected_digest.clone(),
            retention_policy_revision_id: self.retention_policy_revision_id.clone(),
            scan_policy_revision: self.scan_policy_revision.clone(),
            scanner_contract_digest: self.scanner_contract_digest.clone(),
            ruleset_digest: self.ruleset_digest.clone(),
            evidence_ttl_milliseconds: self.evidence_ttl_milliseconds,
            retry_backoff_milliseconds: self.retry_backoff_milliseconds,
        }
    }

    pub fn upload_grant_snapshot(&self) -> Result<UploadGrantSnapshot, ArtifactCommandError> {
        let snapshot = UploadGrantSnapshot {
            schema_version: 1,
            artifact_id: self.artifact_id.clone(),
            operation_id: self.operation_id.clone(),
            subject_principal_id: self.audit.principal_id.clone(),
            operations: vec![
                ArtifactGrantOperation::WriteStaging,
                ArtifactGrantOperation::CommitStaging,
            ],
            audience: ArtifactWorkloadAudience::Principal,
            purpose: self.purpose,
            max_bytes: self.expected_size_bytes,
            token_digest: self.grant_token_digest.clone(),
            expires_at: self.grant_expires_at,
            generation: 1,
        };
        snapshot.canonical_digest()?;
        snapshot.link_key_digest()?;
        Ok(snapshot)
    }

    pub fn blob_security_domain(&self) -> ArtifactBlobSecurityDomain {
        ArtifactBlobSecurityDomain {
            schema_version: 1,
            classification: self.classification,
            retention_policy_revision_id: self.retention_policy_revision_id.clone(),
            encryption_domain_id: self.encryption_domain_id.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompleteArtifactUpload {
    pub audit: CommandAudit,
    pub operation_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub upload_grant_id: ResourceId,
    pub expected_artifact_version: u64,
    pub expected_blob_version: u64,
    pub expected_operation_version: u64,
    pub expected_grant_version: u64,
    pub grant_generation: u64,
    pub grant_token_digest: Sha256Digest,
    pub object_generation: String,
    pub observed_size_bytes: u64,
    pub backend_evidence_digest: Sha256Digest,
}

impl CompleteArtifactUpload {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: ArtifactCommandLimits,
    ) -> Result<(), ArtifactCommandError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ArtifactCommandError::InvalidAudit)?;
        let ids = [
            (&self.operation_id, ResourceKind::Job),
            (&self.artifact_id, ResourceKind::Artifact),
            (&self.blob_id, ResourceKind::InternalBlob),
            (&self.upload_grant_id, ResourceKind::ArtifactGrant),
        ];
        if ids
            .iter()
            .any(|(actual, expected)| actual.kind() != *expected)
            || self.expected_artifact_version == 0
            || self.expected_blob_version == 0
            || self.expected_operation_version == 0
            || self.expected_grant_version == 0
            || self.grant_generation == 0
            || self.observed_size_bytes > limits.maximum_single_bytes
            || self.object_generation.is_empty()
            || self.object_generation.len() > MAX_OBJECT_GENERATION_BYTES
            || self.object_generation.chars().any(char::is_control)
        {
            return Err(ArtifactCommandError::InvalidUploadCompletion);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactLinkState {
    Active,
    Consumed,
    Released,
    Revoked,
    Expired,
}

impl ArtifactLinkState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Consumed => "consumed",
            Self::Released => "released",
            Self::Revoked => "revoked",
            Self::Expired => "expired",
        }
    }
}

impl fmt::Display for ArtifactLinkState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for ArtifactLinkState {
    type Err = ArtifactCommandError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "active" => Ok(Self::Active),
            "consumed" => Ok(Self::Consumed),
            "released" => Ok(Self::Released),
            "revoked" => Ok(Self::Revoked),
            "expired" => Ok(Self::Expired),
            _ => Err(ArtifactCommandError::InvalidPersistedState),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadCompletionDecision {
    pub artifact_state: ArtifactState,
    pub artifact_version: u64,
    pub blob_version: u64,
    pub grant_state: ArtifactLinkState,
    pub grant_version: u64,
    pub operation_state: JobState,
    pub operation_version: u64,
}

pub fn decide_complete_upload(
    artifact: &ArtifactRecord,
    blob: &ArtifactBlobRecord,
    grant: &ArtifactGrantRecord,
    operation: &ArtifactOperationRecord,
    command: &CompleteArtifactUpload,
    now: DateTime<Utc>,
) -> Result<UploadCompletionDecision, ArtifactCommandError> {
    if artifact.tenant_id != command.audit.tenant_id
        || blob.tenant_id != command.audit.tenant_id
        || grant.tenant_id != command.audit.tenant_id
        || operation.tenant_id != command.audit.tenant_id
        || artifact.artifact_id != command.artifact_id
        || artifact.blob_id.as_ref() != Some(&command.blob_id)
        || blob.blob_id != command.blob_id
        || grant.upload_grant_id != command.upload_grant_id
        || grant.artifact_id != command.artifact_id
        || operation.operation_id != command.operation_id
        || operation.snapshot.artifact_id != command.artifact_id
    {
        return Err(ArtifactCommandError::InvalidIdentity);
    }
    if artifact.version != command.expected_artifact_version
        || blob.version != command.expected_blob_version
        || grant.version != command.expected_grant_version
        || operation.version != command.expected_operation_version
    {
        return Err(ArtifactCommandError::StaleVersion);
    }
    if artifact.state != ArtifactState::Staging
        || blob.state != insight_platform_contracts::BlobIntegrityState::Staging
        || grant.state != ArtifactLinkState::Active
        || operation.state != JobState::Waiting
        || !artifact.state.can_transition_to(ArtifactState::Uploaded)
    {
        return Err(ArtifactCommandError::InvalidTransition);
    }
    if grant.snapshot.artifact_id != command.artifact_id
        || grant.snapshot.operation_id != command.operation_id
        || grant.snapshot.subject_principal_id != command.audit.principal_id
        || grant.snapshot.generation != command.grant_generation
        || grant.snapshot.token_digest != command.grant_token_digest
        || grant.snapshot.expires_at <= now
        || !grant
            .snapshot
            .operations
            .contains(&ArtifactGrantOperation::CommitStaging)
    {
        return Err(ArtifactCommandError::GrantRejected);
    }
    if command.observed_size_bytes != artifact.expected_size_bytes
        || command.observed_size_bytes > grant.snapshot.max_bytes
    {
        return Err(ArtifactCommandError::UploadEvidenceMismatch);
    }
    Ok(UploadCompletionDecision {
        artifact_state: ArtifactState::Uploaded,
        artifact_version: next_version(artifact.version)?,
        blob_version: next_version(blob.version)?,
        grant_state: ArtifactLinkState::Consumed,
        grant_version: next_version(grant.version)?,
        operation_state: JobState::Waiting,
        operation_version: next_version(operation.version)?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactBlobCleanupSnapshot {
    pub schema_version: u32,
    pub artifact_id: ResourceId,
    pub discarded_blob_id: ResourceId,
    pub replacement_blob_id: ResourceId,
    pub object_generation: String,
    pub verification_evidence_digest: Sha256Digest,
    pub expected_blob_version: u64,
    pub retry_backoff_milliseconds: u64,
}

impl ArtifactBlobCleanupSnapshot {
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ArtifactCommandError> {
        if self.schema_version != 1
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.discarded_blob_id.kind() != ResourceKind::InternalBlob
            || self.replacement_blob_id.kind() != ResourceKind::InternalBlob
            || self.discarded_blob_id == self.replacement_blob_id
            || self.object_generation.is_empty()
            || self.object_generation.len() > MAX_OBJECT_GENERATION_BYTES
            || self.object_generation.chars().any(char::is_control)
            || self.expected_blob_version == 0
            || self.retry_backoff_milliseconds == 0
            || self.retry_backoff_milliseconds > MAX_ARTIFACT_RETRY_BACKOFF_MILLISECONDS
        {
            return Err(ArtifactCommandError::InvalidVerification);
        }
        digest(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactHoldKind {
    Legal,
    Incident,
}

impl ArtifactHoldKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Legal => "legal",
            Self::Incident => "incident",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactHoldSnapshot {
    pub schema_version: u32,
    pub artifact_id: ResourceId,
    pub hold_kind: ArtifactHoldKind,
    pub authority_principal_id: ResourceId,
    pub reason_class: String,
    pub evidence_digest: Sha256Digest,
    pub placed_at: DateTime<Utc>,
}

impl ArtifactHoldSnapshot {
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ArtifactCommandError> {
        if self.schema_version != 1
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.authority_principal_id.kind() != ResourceKind::Principal
            || !is_code(&self.reason_class, 64)
        {
            return Err(ArtifactCommandError::InvalidHold);
        }
        digest(self)
    }

    pub fn link_key_digest(&self) -> Result<Sha256Digest, ArtifactCommandError> {
        let identity = serde_json::json!({
            "artifact_id": self.artifact_id,
            "authority_principal_id": self.authority_principal_id,
            "hold_kind": self.hold_kind,
            "schema_version": self.schema_version,
        });
        canonical_digest(&identity)
            .map_err(|_| ArtifactCommandError::Canonicalization)?
            .parse()
            .map_err(|_| ArtifactCommandError::Canonicalization)
    }
}

#[derive(Debug, Clone)]
pub struct PlaceArtifactHold {
    pub audit: CommandAudit,
    pub artifact_hold_id: ResourceId,
    pub artifact_id: ResourceId,
    pub expected_artifact_version: u64,
    pub hold_kind: ArtifactHoldKind,
    pub reason_class: String,
    pub evidence_digest: Sha256Digest,
    pub expires_at: Option<DateTime<Utc>>,
}

impl PlaceArtifactHold {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactCommandError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ArtifactCommandError::InvalidAudit)?;
        if self.artifact_hold_id.kind() != ResourceKind::ArtifactLink
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.expected_artifact_version == 0
            || !is_code(&self.reason_class, 64)
            || self.expires_at.is_some_and(|expires_at| expires_at <= now)
        {
            return Err(ArtifactCommandError::InvalidHold);
        }
        Ok(())
    }

    pub fn snapshot(&self, placed_at: DateTime<Utc>) -> ArtifactHoldSnapshot {
        ArtifactHoldSnapshot {
            schema_version: 1,
            artifact_id: self.artifact_id.clone(),
            hold_kind: self.hold_kind,
            authority_principal_id: self.audit.principal_id.clone(),
            reason_class: self.reason_class.clone(),
            evidence_digest: self.evidence_digest.clone(),
            placed_at,
        }
    }
}

pub fn decide_place_artifact_hold(
    artifact: &ArtifactRecord,
    command: &PlaceArtifactHold,
    database_now: DateTime<Utc>,
) -> Result<ArtifactHoldSnapshot, ArtifactCommandError> {
    if artifact.tenant_id != command.audit.tenant_id
        || artifact.artifact_id != command.artifact_id
        || artifact.version != command.expected_artifact_version
    {
        return Err(ArtifactCommandError::StaleVersion);
    }
    if matches!(
        artifact.state,
        ArtifactState::Deleting | ArtifactState::Deleted
    ) {
        return Err(ArtifactCommandError::InvalidTransition);
    }
    let snapshot = command.snapshot(database_now);
    snapshot.canonical_digest()?;
    snapshot.link_key_digest()?;
    Ok(snapshot)
}

#[derive(Debug, Clone)]
pub struct ReleaseArtifactHold {
    pub audit: CommandAudit,
    pub artifact_hold_id: ResourceId,
    pub artifact_id: ResourceId,
    pub expected_hold_version: u64,
    pub reason_class: String,
    pub evidence_digest: Sha256Digest,
}

impl ReleaseArtifactHold {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactCommandError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ArtifactCommandError::InvalidAudit)?;
        if self.artifact_hold_id.kind() != ResourceKind::ArtifactLink
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.expected_hold_version == 0
            || !is_code(&self.reason_class, 64)
        {
            return Err(ArtifactCommandError::InvalidHold);
        }
        Ok(())
    }
}

pub fn decide_release_artifact_hold(
    hold: &ArtifactHoldRecord,
    command: &ReleaseArtifactHold,
) -> Result<(ArtifactLinkState, u64), ArtifactCommandError> {
    if hold.tenant_id != command.audit.tenant_id
        || hold.artifact_hold_id != command.artifact_hold_id
        || hold.artifact_id != command.artifact_id
    {
        return Err(ArtifactCommandError::InvalidIdentity);
    }
    if hold.version != command.expected_hold_version {
        return Err(ArtifactCommandError::StaleVersion);
    }
    if hold.state != ArtifactLinkState::Active {
        return Err(ArtifactCommandError::InvalidTransition);
    }
    Ok((ArtifactLinkState::Released, next_version(hold.version)?))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactProvenanceSnapshot {
    pub schema_version: u32,
    pub source_artifact_id: ResourceId,
    pub derived_artifact_id: ResourceId,
    pub transformation_deployment_id: ResourceId,
    pub producer_owner_id: ResourceId,
    pub parameters_digest: Sha256Digest,
    pub evidence_event_id: ResourceId,
}

impl ArtifactProvenanceSnapshot {
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ArtifactCommandError> {
        if self.schema_version != 1
            || self.source_artifact_id.kind() != ResourceKind::Artifact
            || self.derived_artifact_id.kind() != ResourceKind::Artifact
            || !self.transformation_deployment_id.kind().is_deployment()
            || !is_producer_kind(self.producer_owner_id.kind())
            || self.evidence_event_id.kind() != ResourceKind::Event
        {
            return Err(ArtifactCommandError::InvalidProvenance);
        }
        digest(self)
    }

    pub fn link_key_digest(&self) -> Result<Sha256Digest, ArtifactCommandError> {
        let identity = serde_json::json!({
            "derived_artifact_id": self.derived_artifact_id,
            "parameters_digest": self.parameters_digest,
            "producer_owner_id": self.producer_owner_id,
            "schema_version": self.schema_version,
            "source_artifact_id": self.source_artifact_id,
            "transformation_deployment_id": self.transformation_deployment_id,
        });
        canonical_digest(&identity)
            .map_err(|_| ArtifactCommandError::Canonicalization)?
            .parse()
            .map_err(|_| ArtifactCommandError::Canonicalization)
    }
}

#[derive(Debug, Clone)]
pub struct CreateArtifactProvenance {
    pub audit: CommandAudit,
    pub provenance_link_id: ResourceId,
    pub source_artifact_id: ResourceId,
    pub derived_artifact_id: ResourceId,
    pub transformation_deployment_id: ResourceId,
    pub producer_owner_id: ResourceId,
    pub expected_source_version: u64,
    pub expected_derived_version: u64,
    pub parameters_digest: Sha256Digest,
}

impl CreateArtifactProvenance {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactCommandError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ArtifactCommandError::InvalidAudit)?;
        if self.provenance_link_id.kind() != ResourceKind::ArtifactLink
            || self.source_artifact_id.kind() != ResourceKind::Artifact
            || self.derived_artifact_id.kind() != ResourceKind::Artifact
            || self.source_artifact_id == self.derived_artifact_id
            || !self.transformation_deployment_id.kind().is_deployment()
            || !is_producer_kind(self.producer_owner_id.kind())
            || self.expected_source_version == 0
            || self.expected_derived_version == 0
        {
            return Err(ArtifactCommandError::InvalidProvenance);
        }
        Ok(())
    }

    pub fn snapshot(&self) -> ArtifactProvenanceSnapshot {
        ArtifactProvenanceSnapshot {
            schema_version: 1,
            source_artifact_id: self.source_artifact_id.clone(),
            derived_artifact_id: self.derived_artifact_id.clone(),
            transformation_deployment_id: self.transformation_deployment_id.clone(),
            producer_owner_id: self.producer_owner_id.clone(),
            parameters_digest: self.parameters_digest.clone(),
            evidence_event_id: self.audit.event_id.clone(),
        }
    }
}

pub fn decide_create_artifact_provenance(
    source: &ArtifactRecord,
    derived: &ArtifactRecord,
    command: &CreateArtifactProvenance,
) -> Result<ArtifactProvenanceSnapshot, ArtifactCommandError> {
    if source.tenant_id != command.audit.tenant_id
        || derived.tenant_id != command.audit.tenant_id
        || source.artifact_id != command.source_artifact_id
        || derived.artifact_id != command.derived_artifact_id
    {
        return Err(ArtifactCommandError::InvalidIdentity);
    }
    if source.version != command.expected_source_version
        || derived.version != command.expected_derived_version
    {
        return Err(ArtifactCommandError::StaleVersion);
    }
    if source.state != ArtifactState::Ready
        || derived.state != ArtifactState::Ready
        || derived.classification.rank() < source.classification.rank()
    {
        return Err(ArtifactCommandError::InvalidProvenance);
    }
    let snapshot = command.snapshot();
    snapshot.canonical_digest()?;
    snapshot.link_key_digest()?;
    Ok(snapshot)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReferenceSnapshot {
    pub schema_version: u32,
    pub artifact_id: ResourceId,
    pub owner_id: ResourceId,
    pub reference_kind: ArtifactReferenceKind,
    pub purpose: ArtifactPurpose,
    pub created_by: ResourceId,
}

impl ArtifactReferenceSnapshot {
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ArtifactCommandError> {
        digest(self)
    }

    pub fn link_key_digest(&self) -> Result<Sha256Digest, ArtifactCommandError> {
        let identity = serde_json::json!({
            "artifact_id": self.artifact_id,
            "owner_id": self.owner_id,
            "purpose": self.purpose,
            "reference_kind": self.reference_kind,
            "schema_version": self.schema_version,
        });
        canonical_digest(&identity)
            .map_err(|_| ArtifactCommandError::Canonicalization)?
            .parse()
            .map_err(|_| ArtifactCommandError::Canonicalization)
    }
}

#[derive(Debug, Clone)]
pub struct ReleaseArtifactReference {
    pub audit: CommandAudit,
    pub artifact_reference_id: ResourceId,
    pub artifact_id: ResourceId,
    pub expected_reference_version: u64,
    pub reason_class: String,
}

impl ReleaseArtifactReference {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactCommandError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ArtifactCommandError::InvalidAudit)?;
        if self.artifact_reference_id.kind() != ResourceKind::ArtifactLink
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.expected_reference_version == 0
            || !is_code(&self.reason_class, 64)
        {
            return Err(ArtifactCommandError::InvalidReference);
        }
        Ok(())
    }
}

pub fn decide_release_artifact_reference(
    reference: &ArtifactReferenceRecord,
    command: &ReleaseArtifactReference,
) -> Result<(ArtifactLinkState, u64), ArtifactCommandError> {
    if reference.tenant_id != command.audit.tenant_id
        || reference.artifact_reference_id != command.artifact_reference_id
        || reference.artifact_id != command.artifact_id
    {
        return Err(ArtifactCommandError::InvalidIdentity);
    }
    if reference.version != command.expected_reference_version {
        return Err(ArtifactCommandError::StaleVersion);
    }
    if reference.state != ArtifactLinkState::Active {
        return Err(ArtifactCommandError::InvalidTransition);
    }
    Ok((
        ArtifactLinkState::Released,
        next_version(reference.version)?,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactDeletionMode {
    ArtifactOnly {
        alias_artifact_id: ResourceId,
        alias_artifact_version: u64,
    },
    BlobGeneration {
        object_generation: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactDeletionOperationSnapshot {
    pub schema_version: u32,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub retention_policy_revision_id: ResourceId,
    pub approval_task_id: Option<ResourceId>,
    pub mode: ArtifactDeletionMode,
}

impl ArtifactDeletionOperationSnapshot {
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ArtifactCommandError> {
        if self.schema_version != 1
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.retention_policy_revision_id.kind() != ResourceKind::PolicyRevision
            || self
                .approval_task_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ApprovalTask)
        {
            return Err(ArtifactCommandError::InvalidDeletion);
        }
        validate_deletion_mode(&self.mode)?;
        digest(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactDeletionJobSnapshot {
    pub schema_version: u32,
    pub operation_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub mode: ArtifactDeletionMode,
    pub expected_artifact_version: u64,
    pub expected_blob_version: u64,
    pub expected_operation_version: u64,
    pub retry_backoff_milliseconds: u64,
}

impl ArtifactDeletionJobSnapshot {
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ArtifactCommandError> {
        if self.schema_version != 1
            || self.operation_id.kind() != ResourceKind::Job
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.expected_artifact_version == 0
            || self.expected_blob_version == 0
            || self.expected_operation_version == 0
            || self.retry_backoff_milliseconds == 0
            || self.retry_backoff_milliseconds > MAX_ARTIFACT_RETRY_BACKOFF_MILLISECONDS
        {
            return Err(ArtifactCommandError::InvalidDeletion);
        }
        validate_deletion_mode(&self.mode)?;
        digest(self)
    }
}

#[derive(Debug, Clone)]
pub struct MarkArtifactDeletion {
    pub audit: CommandAudit,
    pub deletion_operation_id: ResourceId,
    pub deletion_job_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub expected_artifact_version: u64,
    pub expected_blob_version: u64,
    pub approval_task_id: Option<ResourceId>,
    pub retry_backoff_milliseconds: u64,
    pub deadline: DateTime<Utc>,
}

impl MarkArtifactDeletion {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactCommandError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ArtifactCommandError::InvalidAudit)?;
        if self.deletion_operation_id.kind() != ResourceKind::Job
            || self.deletion_job_id.kind() != ResourceKind::Job
            || self.deletion_job_id != self.deletion_operation_id
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.expected_artifact_version == 0
            || self.expected_blob_version == 0
            || self.retry_backoff_milliseconds == 0
            || self.retry_backoff_milliseconds > MAX_ARTIFACT_RETRY_BACKOFF_MILLISECONDS
            || self
                .approval_task_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ApprovalTask)
            || self.deadline <= now
        {
            return Err(ArtifactCommandError::InvalidDeletion);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactDeletionAdmissionFacts {
    pub approval_required: bool,
    pub approval_satisfied: bool,
    pub gc_grace_seconds: u64,
    pub live_reference_count: u64,
    pub active_hold_count: u64,
    pub blocking_provenance_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkArtifactDeletionDecision {
    pub artifact_state: ArtifactState,
    pub artifact_version: u64,
    pub blob_state: BlobIntegrityState,
    pub blob_version: u64,
    pub mode: ArtifactDeletionMode,
    pub operation: ArtifactDeletionOperationSnapshot,
    pub job: ArtifactDeletionJobSnapshot,
}

pub fn decide_mark_artifact_deletion(
    artifact: &ArtifactRecord,
    blob: &ArtifactBlobRecord,
    live_alias: Option<&ArtifactRecord>,
    facts: ArtifactDeletionAdmissionFacts,
    command: &MarkArtifactDeletion,
    database_now: DateTime<Utc>,
) -> Result<MarkArtifactDeletionDecision, ArtifactCommandError> {
    if artifact.tenant_id != command.audit.tenant_id
        || blob.tenant_id != command.audit.tenant_id
        || artifact.artifact_id != command.artifact_id
        || artifact.blob_id.as_ref() != Some(&command.blob_id)
        || blob.blob_id != command.blob_id
    {
        return Err(ArtifactCommandError::InvalidIdentity);
    }
    if artifact.version != command.expected_artifact_version
        || blob.version != command.expected_blob_version
    {
        return Err(ArtifactCommandError::StaleVersion);
    }
    let gc_grace_seconds =
        i64::try_from(facts.gc_grace_seconds).map_err(|_| ArtifactCommandError::InvalidDeletion)?;
    let gc_eligible_at = artifact
        .retain_until
        .checked_add_signed(ChronoDuration::seconds(gc_grace_seconds))
        .ok_or(ArtifactCommandError::InvalidDeletion)?;
    if facts.gc_grace_seconds == 0
        || gc_eligible_at > database_now
        || !artifact.state.can_transition_to(ArtifactState::Deleting)
        || !matches!(
            artifact.state,
            ArtifactState::Ready
                | ArtifactState::Rejected
                | ArtifactState::Quarantined
                | ArtifactState::Corrupt
        )
        || !matches!(
            blob.state,
            BlobIntegrityState::Verified | BlobIntegrityState::Corrupt
        )
        || facts.live_reference_count != 0
        || facts.active_hold_count != 0
        || facts.blocking_provenance_count != 0
        || (facts.approval_required && !facts.approval_satisfied)
        || (!facts.approval_required && command.approval_task_id.is_some())
    {
        return Err(ArtifactCommandError::DeletionBlocked);
    }
    let mode = if let Some(alias) = live_alias {
        if alias.tenant_id != artifact.tenant_id
            || alias.artifact_id == artifact.artifact_id
            || alias.blob_id.as_ref() != Some(&blob.blob_id)
            || alias.state == ArtifactState::Deleted
        {
            return Err(ArtifactCommandError::InvalidDeletion);
        }
        ArtifactDeletionMode::ArtifactOnly {
            alias_artifact_id: alias.artifact_id.clone(),
            alias_artifact_version: alias.version,
        }
    } else {
        ArtifactDeletionMode::BlobGeneration {
            object_generation: blob
                .object_generation
                .clone()
                .ok_or(ArtifactCommandError::InvalidDeletion)?,
        }
    };
    let (blob_state, blob_version) = match mode {
        ArtifactDeletionMode::ArtifactOnly { .. } => (blob.state, blob.version),
        ArtifactDeletionMode::BlobGeneration { .. } => {
            (BlobIntegrityState::Deleting, next_version(blob.version)?)
        }
    };
    let artifact_version = next_version(artifact.version)?;
    let operation = ArtifactDeletionOperationSnapshot {
        schema_version: 1,
        artifact_id: artifact.artifact_id.clone(),
        blob_id: blob.blob_id.clone(),
        retention_policy_revision_id: artifact.retention_policy_revision_id.clone(),
        approval_task_id: command.approval_task_id.clone(),
        mode: mode.clone(),
    };
    let job = ArtifactDeletionJobSnapshot {
        schema_version: 1,
        operation_id: command.deletion_operation_id.clone(),
        artifact_id: artifact.artifact_id.clone(),
        blob_id: blob.blob_id.clone(),
        mode: mode.clone(),
        expected_artifact_version: artifact_version,
        expected_blob_version: blob_version,
        expected_operation_version: 1,
        retry_backoff_milliseconds: command.retry_backoff_milliseconds,
    };
    operation.canonical_digest()?;
    job.canonical_digest()?;
    Ok(MarkArtifactDeletionDecision {
        artifact_state: ArtifactState::Deleting,
        artifact_version,
        blob_state,
        blob_version,
        mode,
        operation,
        job,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactDeletionEvidence {
    ArtifactOnly {
        alias_artifact_id: ResourceId,
        alias_artifact_version: u64,
    },
    BlobGeneration {
        object_generation: String,
        backend_receipt_digest: Sha256Digest,
        absence_evidence_digest: Sha256Digest,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteArtifactDeletion {
    pub audit: ArtifactWorkerAudit,
    pub deletion_operation_id: ResourceId,
    pub deletion_job_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub expected_artifact_version: u64,
    pub expected_blob_version: u64,
    pub expected_operation_version: u64,
    pub fence: insight_platform_jobs::JobFence,
    pub evidence: ArtifactDeletionEvidence,
}

impl CompleteArtifactDeletion {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactCommandError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ArtifactCommandError::InvalidAudit)?;
        if self.deletion_operation_id.kind() != ResourceKind::Job
            || self.deletion_job_id.kind() != ResourceKind::Job
            || self.deletion_job_id != self.deletion_operation_id
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.expected_artifact_version == 0
            || self.expected_blob_version == 0
            || self.expected_operation_version == 0
            || self.fence.expected_version == 0
            || self.expected_operation_version != self.fence.expected_version
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
        {
            return Err(ArtifactCommandError::InvalidDeletion);
        }
        match &self.evidence {
            ArtifactDeletionEvidence::ArtifactOnly {
                alias_artifact_id,
                alias_artifact_version,
            } => {
                if alias_artifact_id.kind() != ResourceKind::Artifact
                    || alias_artifact_id == &self.artifact_id
                    || *alias_artifact_version == 0
                {
                    return Err(ArtifactCommandError::InvalidDeletion);
                }
            }
            ArtifactDeletionEvidence::BlobGeneration {
                object_generation, ..
            } => {
                if object_generation.is_empty()
                    || object_generation.len() > MAX_OBJECT_GENERATION_BYTES
                    || object_generation.chars().any(char::is_control)
                {
                    return Err(ArtifactCommandError::InvalidDeletion);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteArtifactDeletionDecision {
    pub artifact_state: ArtifactState,
    pub artifact_version: u64,
    pub blob_state: BlobIntegrityState,
    pub blob_version: u64,
    pub operation_state: JobState,
    pub operation_version: u64,
}

pub fn decide_complete_artifact_deletion(
    artifact: &ArtifactRecord,
    blob: &ArtifactBlobRecord,
    deletion: &ArtifactDeletionRecord,
    alias_witness: Option<&ArtifactRecord>,
    command: &CompleteArtifactDeletion,
) -> Result<CompleteArtifactDeletionDecision, ArtifactCommandError> {
    if artifact.tenant_id != command.audit.tenant_id
        || blob.tenant_id != command.audit.tenant_id
        || deletion.tenant_id != command.audit.tenant_id
        || artifact.artifact_id != command.artifact_id
        || artifact.blob_id.as_ref() != Some(&command.blob_id)
        || blob.blob_id != command.blob_id
        || deletion.operation_id != command.deletion_operation_id
        || deletion.job_id != command.deletion_job_id
        || deletion.artifact_id != command.artifact_id
        || deletion.blob_id != command.blob_id
    {
        return Err(ArtifactCommandError::InvalidIdentity);
    }
    if artifact.version != command.expected_artifact_version
        || blob.version != command.expected_blob_version
        || deletion.operation_version != command.expected_operation_version
    {
        return Err(ArtifactCommandError::StaleVersion);
    }
    if artifact.state != ArtifactState::Deleting
        || deletion.operation_state != JobState::Running
        || !artifact.state.can_transition_to(ArtifactState::Deleted)
        || !deletion
            .operation_state
            .can_transition_to(JobState::Succeeded)
    {
        return Err(ArtifactCommandError::InvalidTransition);
    }
    let (blob_state, blob_version) = match (&deletion.mode, &command.evidence) {
        (
            ArtifactDeletionMode::ArtifactOnly {
                alias_artifact_id,
                alias_artifact_version,
            },
            ArtifactDeletionEvidence::ArtifactOnly {
                alias_artifact_id: evidence_id,
                alias_artifact_version: evidence_version,
            },
        ) if alias_artifact_id == evidence_id && alias_artifact_version == evidence_version => {
            let alias = alias_witness.ok_or(ArtifactCommandError::DeletionEvidenceMismatch)?;
            if alias.tenant_id != artifact.tenant_id
                || alias.artifact_id != *alias_artifact_id
                || alias.version != *alias_artifact_version
                || alias.blob_id.as_ref() != artifact.blob_id.as_ref()
                || alias.state == ArtifactState::Deleted
            {
                return Err(ArtifactCommandError::DeletionEvidenceMismatch);
            }
            (blob.state, blob.version)
        }
        (
            ArtifactDeletionMode::BlobGeneration { object_generation },
            ArtifactDeletionEvidence::BlobGeneration {
                object_generation: evidence_generation,
                ..
            },
        ) if object_generation == evidence_generation
            && blob.object_generation.as_ref() == Some(object_generation)
            && blob.state == BlobIntegrityState::Deleting
            && alias_witness.is_none() =>
        {
            (BlobIntegrityState::Deleted, next_version(blob.version)?)
        }
        _ => return Err(ArtifactCommandError::DeletionEvidenceMismatch),
    };
    Ok(CompleteArtifactDeletionDecision {
        artifact_state: ArtifactState::Deleted,
        artifact_version: next_version(artifact.version)?,
        blob_state,
        blob_version,
        operation_state: JobState::Succeeded,
        operation_version: next_version(deletion.operation_version)?,
    })
}

#[derive(Debug, Clone)]
pub struct FinalizeArtifact {
    pub audit: CommandAudit,
    pub operation_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub upload_grant_id: ResourceId,
    pub artifact_reference_id: ResourceId,
    pub quota_account_id: ResourceId,
    pub quota_settle_entry_id: ResourceId,
    pub expected_artifact_version: u64,
    pub expected_blob_version: u64,
    pub expected_operation_version: u64,
    pub expected_grant_version: u64,
    pub expected_quota_account_version: u64,
    pub grant_generation: u64,
    pub object_generation: String,
    pub content_digest: Sha256Digest,
    pub size_bytes: u64,
    pub verified_media_type: String,
    pub reference_kind: ArtifactReferenceKind,
}

impl FinalizeArtifact {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: ArtifactCommandLimits,
    ) -> Result<(), ArtifactCommandError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ArtifactCommandError::InvalidAudit)?;
        let ids = [
            (&self.operation_id, ResourceKind::Job),
            (&self.artifact_id, ResourceKind::Artifact),
            (&self.blob_id, ResourceKind::InternalBlob),
            (&self.upload_grant_id, ResourceKind::ArtifactGrant),
            (&self.artifact_reference_id, ResourceKind::ArtifactLink),
            (&self.quota_account_id, ResourceKind::QuotaAccount),
            (&self.quota_settle_entry_id, ResourceKind::QuotaLedgerEntry),
        ];
        if ids
            .iter()
            .any(|(actual, expected)| actual.kind() != *expected)
            || self.expected_artifact_version == 0
            || self.expected_blob_version == 0
            || self.expected_operation_version == 0
            || self.expected_grant_version == 0
            || self.expected_quota_account_version == 0
            || self.grant_generation == 0
            || self.object_generation.is_empty()
            || self.object_generation.len() > MAX_OBJECT_GENERATION_BYTES
            || self.object_generation.chars().any(char::is_control)
            || self.size_bytes > limits.maximum_single_bytes
            || !is_media_type(&self.verified_media_type)
        {
            return Err(ArtifactCommandError::InvalidFinalization);
        }
        Ok(())
    }

    pub fn reference_snapshot(&self, purpose: ArtifactPurpose) -> ArtifactReferenceSnapshot {
        ArtifactReferenceSnapshot {
            schema_version: 1,
            artifact_id: self.artifact_id.clone(),
            owner_id: self.operation_id.clone(),
            reference_kind: self.reference_kind,
            purpose,
            created_by: self.audit.principal_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeArtifactDecision {
    pub artifact_state: ArtifactState,
    pub artifact_version: u64,
    pub operation_state: JobState,
    pub operation_version: u64,
    pub reference: ArtifactReferenceSnapshot,
    pub artifact_ref: ArtifactRef,
}

pub fn decide_finalize_artifact(
    artifact: &ArtifactRecord,
    blob: &ArtifactBlobRecord,
    grant: &ArtifactGrantRecord,
    operation: &ArtifactOperationRecord,
    command: &FinalizeArtifact,
) -> Result<FinalizeArtifactDecision, ArtifactCommandError> {
    require_artifact_operation_identity(
        artifact,
        blob,
        operation,
        &command.audit,
        &command.artifact_id,
        &command.blob_id,
        &command.operation_id,
    )?;
    if grant.tenant_id != command.audit.tenant_id
        || grant.upload_grant_id != command.upload_grant_id
        || grant.artifact_id != command.artifact_id
        || grant.snapshot.operation_id != command.operation_id
        || grant.snapshot.generation != command.grant_generation
    {
        return Err(ArtifactCommandError::InvalidIdentity);
    }
    if artifact.version != command.expected_artifact_version
        || blob.version != command.expected_blob_version
        || grant.version != command.expected_grant_version
        || operation.version != command.expected_operation_version
    {
        return Err(ArtifactCommandError::StaleVersion);
    }
    if artifact.state != ArtifactState::Verified
        || blob.state != BlobIntegrityState::Verified
        || grant.state != ArtifactLinkState::Consumed
        || operation.state != JobState::Waiting
        || !artifact.state.can_transition_to(ArtifactState::Ready)
        || !operation.state.can_transition_to(JobState::Succeeded)
    {
        return Err(ArtifactCommandError::InvalidTransition);
    }
    if blob.object_generation.as_deref() != Some(command.object_generation.as_str())
        || blob.content_digest.as_ref() != Some(&command.content_digest)
        || blob.size_bytes != Some(command.size_bytes)
        || artifact.verified_media_type.as_deref() != Some(command.verified_media_type.as_str())
        || artifact.expected_size_bytes != command.size_bytes
        || artifact
            .expected_digest
            .as_ref()
            .is_some_and(|expected| expected != &command.content_digest)
    {
        return Err(ArtifactCommandError::VerificationEvidenceMismatch);
    }
    let reference = command.reference_snapshot(artifact.purpose);
    reference.canonical_digest()?;
    reference.link_key_digest()?;
    let artifact_ref = ArtifactRef::new(
        artifact.artifact_id.clone(),
        command.content_digest.clone(),
        command.size_bytes,
        command.verified_media_type.clone(),
        artifact.classification,
        artifact.metadata.display_name.clone(),
    )
    .map_err(|_| ArtifactCommandError::InvalidFinalization)?;
    Ok(FinalizeArtifactDecision {
        artifact_state: ArtifactState::Ready,
        artifact_version: next_version(artifact.version)?,
        operation_state: JobState::Succeeded,
        operation_version: next_version(operation.version)?,
        reference,
        artifact_ref,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRecord {
    pub tenant_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: Option<ResourceId>,
    pub purpose: ArtifactPurpose,
    pub classification: DataClassification,
    pub expected_size_bytes: u64,
    pub expected_digest: Option<Sha256Digest>,
    pub declared_media_type: Option<String>,
    pub verified_media_type: Option<String>,
    pub state: ArtifactState,
    pub version: u64,
    pub metadata: ArtifactMetadataSnapshot,
    pub retention_policy_revision_id: ResourceId,
    pub retain_until: DateTime<Utc>,
    pub created_by: ResourceId,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub terminal_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactBlobRecord {
    pub tenant_id: ResourceId,
    pub blob_id: ResourceId,
    pub backend: String,
    pub storage_binding_digest: Sha256Digest,
    pub security_domain_digest: Sha256Digest,
    pub object_generation: Option<String>,
    pub encryption_domain_id: ResourceId,
    pub content_digest: Option<Sha256Digest>,
    pub size_bytes: Option<u64>,
    pub state: insight_platform_contracts::BlobIntegrityState,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactGrantRecord {
    pub tenant_id: ResourceId,
    pub upload_grant_id: ResourceId,
    pub artifact_id: ResourceId,
    pub state: ArtifactLinkState,
    pub version: u64,
    pub snapshot: UploadGrantSnapshot,
    pub link_key_digest: Sha256Digest,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactOperationRecord {
    pub tenant_id: ResourceId,
    pub operation_id: ResourceId,
    pub state: JobState,
    pub version: u64,
    pub snapshot: ArtifactUploadOperationSnapshot,
    pub deadline: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedArtifact {
    pub artifact: ArtifactRecord,
    pub blob: ArtifactBlobRecord,
    pub grant: ArtifactGrantRecord,
    pub operation: ArtifactOperationRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedArtifactUpload {
    pub artifact: ArtifactRecord,
    pub blob: ArtifactBlobRecord,
    pub grant: ArtifactGrantRecord,
    pub operation: ArtifactOperationRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactHoldRecord {
    pub tenant_id: ResourceId,
    pub artifact_hold_id: ResourceId,
    pub artifact_id: ResourceId,
    pub state: ArtifactLinkState,
    pub version: u64,
    pub snapshot: ArtifactHoldSnapshot,
    pub link_key_digest: Sha256Digest,
    pub created_at: DateTime<Utc>,
    pub released_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactProvenanceRecord {
    pub tenant_id: ResourceId,
    pub provenance_link_id: ResourceId,
    pub source_artifact_id: ResourceId,
    pub derived_artifact_id: ResourceId,
    pub state: ArtifactLinkState,
    pub version: u64,
    pub snapshot: ArtifactProvenanceSnapshot,
    pub link_key_digest: Sha256Digest,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactReferenceRecord {
    pub tenant_id: ResourceId,
    pub artifact_reference_id: ResourceId,
    pub artifact_id: ResourceId,
    pub state: ArtifactLinkState,
    pub version: u64,
    pub snapshot: ArtifactReferenceSnapshot,
    pub link_key_digest: Sha256Digest,
    pub created_at: DateTime<Utc>,
    pub released_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDeletionRecord {
    pub tenant_id: ResourceId,
    pub operation_id: ResourceId,
    pub operation_state: JobState,
    pub operation_version: u64,
    pub job_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub mode: ArtifactDeletionMode,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkedArtifactDeletion {
    pub artifact: ArtifactRecord,
    pub blob: ArtifactBlobRecord,
    pub deletion: ArtifactDeletionRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedArtifactDeletion {
    pub artifact: ArtifactRecord,
    pub blob: ArtifactBlobRecord,
    pub deletion: ArtifactDeletionRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedArtifact {
    pub artifact: ArtifactRecord,
    pub blob: ArtifactBlobRecord,
    pub operation: ArtifactOperationRecord,
    pub reference: ArtifactReferenceRecord,
    pub artifact_ref: ArtifactRef,
}

pub trait ArtifactTransaction {
    type Error;

    async fn prepare_artifact(
        &mut self,
        command: PrepareArtifact,
    ) -> Result<CommandOutcome<PreparedArtifact>, Self::Error>;
    async fn complete_upload(
        &mut self,
        command: CompleteArtifactUpload,
    ) -> Result<CommandOutcome<CompletedArtifactUpload>, Self::Error>;
    async fn schedule_initial_scan(
        &mut self,
        command: ScheduleInitialArtifactScan,
    ) -> Result<CommandOutcome<ArtifactScanWorkRecord>, Self::Error>;
    async fn schedule_rescan(
        &mut self,
        command: ScheduleArtifactRescan,
    ) -> Result<CommandOutcome<ArtifactScanWorkRecord>, Self::Error>;
    async fn commit_scan_outcome(
        &mut self,
        command: CommitArtifactScanOutcome,
    ) -> Result<CommandOutcome<ArtifactScanWorkRecord>, Self::Error>;
    async fn commit_blob_cleanup(
        &mut self,
        command: CommitArtifactBlobCleanup,
    ) -> Result<CommandOutcome<CompletedArtifactBlobCleanup>, Self::Error>;
    async fn place_hold(
        &mut self,
        command: PlaceArtifactHold,
    ) -> Result<CommandOutcome<ArtifactHoldRecord>, Self::Error>;
    async fn release_hold(
        &mut self,
        command: ReleaseArtifactHold,
    ) -> Result<CommandOutcome<ArtifactHoldRecord>, Self::Error>;
    async fn create_provenance(
        &mut self,
        command: CreateArtifactProvenance,
    ) -> Result<CommandOutcome<ArtifactProvenanceRecord>, Self::Error>;
    async fn release_reference(
        &mut self,
        command: ReleaseArtifactReference,
    ) -> Result<CommandOutcome<ArtifactReferenceRecord>, Self::Error>;
    async fn mark_deletion(
        &mut self,
        command: MarkArtifactDeletion,
    ) -> Result<CommandOutcome<MarkedArtifactDeletion>, Self::Error>;
    async fn complete_deletion(
        &mut self,
        command: CompleteArtifactDeletion,
    ) -> Result<CommandOutcome<CompletedArtifactDeletion>, Self::Error>;
    async fn finalize_artifact(
        &mut self,
        command: FinalizeArtifact,
    ) -> Result<CommandOutcome<FinalizedArtifact>, Self::Error>;
    async fn commit(self) -> Result<(), Self::Error>;
    async fn rollback(self) -> Result<(), Self::Error>;
}

pub trait ArtifactStore {
    type Error;
    type Transaction<'a>: ArtifactTransaction<Error = Self::Error>
    where
        Self: 'a;

    async fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactCommandError {
    InvalidProfile(String),
    InvalidLimits,
    InvalidAudit,
    InvalidIdentity,
    InvalidTimeOrSize,
    InvalidStorageBinding,
    InvalidMetadata,
    InvalidUploadCompletion,
    InvalidPersistedState,
    StaleVersion,
    InvalidTransition,
    GrantRejected,
    UploadEvidenceMismatch,
    InvalidVerification,
    VerificationEvidenceMismatch,
    InvalidReusableBlob,
    InvalidHold,
    InvalidProvenance,
    InvalidReference,
    InvalidDeletion,
    DeletionBlocked,
    DeletionEvidenceMismatch,
    InvalidFinalization,
    VersionOverflow,
    Canonicalization,
}

impl fmt::Display for ArtifactCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProfile(message) => {
                write!(formatter, "invalid HardLimitProfile: {message}")
            }
            Self::InvalidLimits => formatter.write_str("Artifact command limits are invalid"),
            Self::InvalidAudit => formatter.write_str("Artifact command audit is invalid"),
            Self::InvalidIdentity => formatter.write_str("Artifact command identity is invalid"),
            Self::InvalidTimeOrSize => {
                formatter.write_str("Artifact size, retention, or deadline is invalid")
            }
            Self::InvalidStorageBinding => {
                formatter.write_str("Artifact storage binding is invalid")
            }
            Self::InvalidMetadata => formatter.write_str("Artifact metadata is invalid"),
            Self::InvalidUploadCompletion => {
                formatter.write_str("Artifact upload completion is invalid")
            }
            Self::InvalidPersistedState => {
                formatter.write_str("Artifact persisted state is unknown")
            }
            Self::StaleVersion => formatter.write_str("Artifact aggregate version is stale"),
            Self::InvalidTransition => formatter.write_str("Artifact transition is invalid"),
            Self::GrantRejected => formatter.write_str("ArtifactGrant is not usable"),
            Self::UploadEvidenceMismatch => {
                formatter.write_str("uploaded object evidence does not match the Artifact intent")
            }
            Self::InvalidVerification => {
                formatter.write_str("Artifact verification command is invalid")
            }
            Self::VerificationEvidenceMismatch => {
                formatter.write_str("Artifact verification evidence does not match its intent")
            }
            Self::InvalidReusableBlob => {
                formatter.write_str("reusable Artifact Blob is outside the exact security domain")
            }
            Self::InvalidHold => formatter.write_str("Artifact hold command is invalid"),
            Self::InvalidProvenance => {
                formatter.write_str("Artifact provenance command is invalid")
            }
            Self::InvalidReference => formatter.write_str("Artifact reference command is invalid"),
            Self::InvalidDeletion => formatter.write_str("Artifact deletion command is invalid"),
            Self::DeletionBlocked => {
                formatter.write_str("Artifact deletion is blocked by current authority facts")
            }
            Self::DeletionEvidenceMismatch => {
                formatter.write_str("Artifact deletion evidence does not match the admitted mode")
            }
            Self::InvalidFinalization => {
                formatter.write_str("Artifact finalization command is invalid")
            }
            Self::VersionOverflow => formatter.write_str("Artifact version exceeds its bound"),
            Self::Canonicalization => {
                formatter.write_str("Artifact snapshot cannot be canonicalized")
            }
        }
    }
}

impl Error for ArtifactCommandError {}

fn digest(value: &impl Serialize) -> Result<Sha256Digest, ArtifactCommandError> {
    let value = serde_json::to_value(value).map_err(|_| ArtifactCommandError::Canonicalization)?;
    canonical_digest(&value)
        .map_err(|_| ArtifactCommandError::Canonicalization)?
        .parse()
        .map_err(|_| ArtifactCommandError::Canonicalization)
}

fn next_version(version: u64) -> Result<u64, ArtifactCommandError> {
    version
        .checked_add(1)
        .ok_or(ArtifactCommandError::VersionOverflow)
}

fn is_producer_kind(kind: ResourceKind) -> bool {
    matches!(
        kind,
        ResourceKind::CapabilityInvocation
            | ResourceKind::ModelTurn
            | ResourceKind::ContextQuery
            | ResourceKind::Job
    )
}

fn validate_deletion_mode(mode: &ArtifactDeletionMode) -> Result<(), ArtifactCommandError> {
    match mode {
        ArtifactDeletionMode::ArtifactOnly {
            alias_artifact_id,
            alias_artifact_version,
        } => {
            if alias_artifact_id.kind() != ResourceKind::Artifact || *alias_artifact_version == 0 {
                return Err(ArtifactCommandError::InvalidDeletion);
            }
        }
        ArtifactDeletionMode::BlobGeneration { object_generation } => {
            if object_generation.is_empty()
                || object_generation.len() > MAX_OBJECT_GENERATION_BYTES
                || object_generation.chars().any(char::is_control)
            {
                return Err(ArtifactCommandError::InvalidDeletion);
            }
        }
    }
    Ok(())
}

fn require_artifact_operation_identity(
    artifact: &ArtifactRecord,
    blob: &ArtifactBlobRecord,
    operation: &ArtifactOperationRecord,
    audit: &CommandAudit,
    artifact_id: &ResourceId,
    blob_id: &ResourceId,
    operation_id: &ResourceId,
) -> Result<(), ArtifactCommandError> {
    if artifact.tenant_id != audit.tenant_id
        || blob.tenant_id != audit.tenant_id
        || operation.tenant_id != audit.tenant_id
        || artifact.artifact_id != *artifact_id
        || artifact.blob_id.as_ref() != Some(blob_id)
        || blob.blob_id != *blob_id
        || operation.operation_id != *operation_id
        || operation.snapshot.artifact_id != *artifact_id
    {
        return Err(ArtifactCommandError::InvalidIdentity);
    }
    Ok(())
}

fn is_code(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        })
}

fn is_media_type(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_MEDIA_TYPE_BYTES || !value.is_ascii() {
        return false;
    }
    let Some((major, minor)) = value.split_once('/') else {
        return false;
    };
    !major.is_empty()
        && !minor.is_empty()
        && !minor.contains('/')
        && major.bytes().chain(minor.bytes()).all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(
                    byte,
                    b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-'
                )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{checked_in_hard_limit_profile, PrincipalKind, ResourceKind};

    fn id(kind: ResourceKind, suffix: &str) -> ResourceId {
        format!(
            "{}_0198f1c5-0787-75e1-a9e8-d95ca0f4{}",
            kind.descriptor().prefix,
            suffix
        )
        .parse()
        .unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn worker_audit(
        tenant_id: &ResourceId,
        worker_process_generation_id: &ResourceId,
        base: &str,
    ) -> ArtifactWorkerAudit {
        let receipt_suffix = format!("{base}1");
        let event_suffix = format!("{base}2");
        let outbox_suffix = format!("{base}3");
        ArtifactWorkerAudit {
            tenant_id: tenant_id.clone(),
            worker_process_generation_id: worker_process_generation_id.clone(),
            receipt_id: id(ResourceKind::Receipt, &receipt_suffix),
            event_id: id(ResourceKind::Event, &event_suffix),
            outbox_id: id(ResourceKind::OutboxEvent, &outbox_suffix),
            idempotency_key_digest: digest('8'),
            request_digest: digest('9'),
            receipt_expires_at: Utc::now() + ChronoDuration::hours(2),
        }
    }

    fn command(now: DateTime<Utc>) -> PrepareArtifact {
        PrepareArtifact {
            audit: CommandAudit {
                trace: insight_platform_contracts::TraceIdentityV1::generate(),
                tenant_id: id(ResourceKind::Tenant, "1001"),
                principal_id: id(ResourceKind::Principal, "1002"),
                principal_kind: PrincipalKind::TenantAdmin,
                receipt_id: id(ResourceKind::Receipt, "1003"),
                event_id: id(ResourceKind::Event, "1004"),
                outbox_id: id(ResourceKind::OutboxEvent, "1005"),
                idempotency_key_digest: digest('a'),
                request_digest: digest('b'),
                receipt_expires_at: now + ChronoDuration::hours(2),
            },
            operation_id: id(ResourceKind::Job, "1006"),
            artifact_id: id(ResourceKind::Artifact, "1007"),
            blob_id: id(ResourceKind::InternalBlob, "1008"),
            upload_grant_id: id(ResourceKind::ArtifactGrant, "1009"),
            quota_account_id: id(ResourceKind::QuotaAccount, "100a"),
            quota_entry_id: id(ResourceKind::QuotaLedgerEntry, "100b"),
            purpose: ArtifactPurpose::RunInput,
            classification: DataClassification::Internal,
            expected_size_bytes: 1024,
            expected_digest: None,
            declared_media_type: None,
            retention_policy_revision_id: id(ResourceKind::PolicyRevision, "100c"),
            scan_policy_revision: ExactVersionRef::new(
                id(ResourceKind::PolicyRevision, "100e"),
                digest('e'),
            )
            .unwrap(),
            scanner_contract_digest: digest('f'),
            ruleset_digest: digest('1'),
            evidence_ttl_milliseconds: 60_000,
            retry_backoff_milliseconds: 100,
            retain_until: now + ChronoDuration::days(1),
            operation_deadline: now + ChronoDuration::hours(1),
            grant_expires_at: now + ChronoDuration::minutes(30),
            grant_token_digest: digest('c'),
            storage_backend: "s3".to_owned(),
            storage_binding_digest: digest('d'),
            object_reference_ciphertext: vec![1, 2, 3],
            key_id: "artifact-kek-v1".to_owned(),
            encryption_domain_id: id(ResourceKind::EncryptionDomain, "100d"),
            display_name: Some("input.json".to_owned()),
        }
    }

    fn prepared_bundle(now: DateTime<Utc>) -> PreparedArtifact {
        let command = command(now);
        PreparedArtifact {
            artifact: ArtifactRecord {
                tenant_id: command.audit.tenant_id.clone(),
                artifact_id: command.artifact_id.clone(),
                blob_id: Some(command.blob_id.clone()),
                purpose: command.purpose,
                classification: command.classification,
                expected_size_bytes: command.expected_size_bytes,
                expected_digest: command.expected_digest.clone(),
                declared_media_type: command.declared_media_type.clone(),
                verified_media_type: None,
                state: ArtifactState::Staging,
                version: 1,
                metadata: command.metadata_snapshot().unwrap(),
                retention_policy_revision_id: command.retention_policy_revision_id.clone(),
                retain_until: command.retain_until,
                created_by: command.audit.principal_id.clone(),
                created_at: now,
                updated_at: now,
                terminal_at: None,
            },
            blob: ArtifactBlobRecord {
                tenant_id: command.audit.tenant_id.clone(),
                blob_id: command.blob_id.clone(),
                backend: command.storage_backend.clone(),
                storage_binding_digest: command.storage_binding_digest.clone(),
                security_domain_digest: command.blob_security_domain().canonical_digest().unwrap(),
                object_generation: None,
                encryption_domain_id: command.encryption_domain_id.clone(),
                content_digest: None,
                size_bytes: None,
                state: insight_platform_contracts::BlobIntegrityState::Staging,
                version: 1,
            },
            grant: ArtifactGrantRecord {
                tenant_id: command.audit.tenant_id.clone(),
                upload_grant_id: command.upload_grant_id.clone(),
                artifact_id: command.artifact_id.clone(),
                state: ArtifactLinkState::Active,
                version: 1,
                snapshot: command.upload_grant_snapshot().unwrap(),
                link_key_digest: command
                    .upload_grant_snapshot()
                    .unwrap()
                    .link_key_digest()
                    .unwrap(),
                created_at: now,
            },
            operation: ArtifactOperationRecord {
                tenant_id: command.audit.tenant_id.clone(),
                operation_id: command.operation_id.clone(),
                state: JobState::Waiting,
                version: 1,
                snapshot: command.operation_snapshot(),
                deadline: command.operation_deadline,
                created_at: now,
            },
        }
    }

    fn completion(now: DateTime<Utc>) -> CompleteArtifactUpload {
        let prepared = command(now);
        CompleteArtifactUpload {
            audit: prepared.audit,
            operation_id: prepared.operation_id,
            artifact_id: prepared.artifact_id,
            blob_id: prepared.blob_id,
            upload_grant_id: prepared.upload_grant_id,
            expected_artifact_version: 1,
            expected_blob_version: 1,
            expected_operation_version: 1,
            expected_grant_version: 1,
            grant_generation: 1,
            grant_token_digest: prepared.grant_token_digest,
            object_generation: "version-1".to_owned(),
            observed_size_bytes: prepared.expected_size_bytes,
            backend_evidence_digest: digest('f'),
        }
    }

    #[test]
    fn prepare_allows_unknown_digest_and_media_without_faking_verified_content() {
        let now = Utc::now();
        let limits = ArtifactCommandLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let command = command(now);
        command.validate_at(now, limits).unwrap();
        assert_eq!(command.expected_digest, None);
        assert_eq!(command.declared_media_type, None);
        assert_eq!(command.upload_grant_snapshot().unwrap().operations.len(), 2);
        let frozen = command.operation_snapshot();
        frozen.canonical_digest().unwrap();
        assert_eq!(frozen.scan_policy_revision, command.scan_policy_revision);
        assert_eq!(
            frozen.scanner_contract_digest,
            command.scanner_contract_digest
        );
        assert_eq!(frozen.ruleset_digest, command.ruleset_digest);
    }

    #[test]
    fn prepare_rejects_identity_swaps_unsafe_names_and_unbounded_staging() {
        let now = Utc::now();
        let limits = ArtifactCommandLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();

        let mut wrong_id = command(now);
        wrong_id.blob_id = id(ResourceKind::Artifact, "1010");
        assert_eq!(
            wrong_id.validate_at(now, limits),
            Err(ArtifactCommandError::InvalidIdentity)
        );

        let mut unsafe_name = command(now);
        unsafe_name.display_name = Some("../secret".to_owned());
        assert_eq!(
            unsafe_name.validate_at(now, limits),
            Err(ArtifactCommandError::InvalidMetadata)
        );

        let mut long_grant = command(now);
        long_grant.grant_expires_at =
            now + ChronoDuration::seconds(limits.maximum_staging_seconds() + 1);
        long_grant.operation_deadline = long_grant.grant_expires_at + ChronoDuration::seconds(1);
        assert_eq!(
            long_grant.validate_at(now, limits),
            Err(ArtifactCommandError::InvalidTimeOrSize)
        );
    }

    #[test]
    fn grant_link_key_excludes_bearer_token_but_binds_authority_shape() {
        let now = Utc::now();
        let first = command(now).upload_grant_snapshot().unwrap();
        let mut second_command = command(now);
        second_command.grant_token_digest = digest('e');
        let second = second_command.upload_grant_snapshot().unwrap();
        assert_eq!(
            first.link_key_digest().unwrap(),
            second.link_key_digest().unwrap()
        );
        assert_ne!(
            first.canonical_digest().unwrap(),
            second.canonical_digest().unwrap()
        );
    }

    #[test]
    fn complete_upload_is_grant_fenced_and_advances_all_versions_once() {
        let now = Utc::now();
        let bundle = prepared_bundle(now);
        let completion = completion(now);
        let decision = decide_complete_upload(
            &bundle.artifact,
            &bundle.blob,
            &bundle.grant,
            &bundle.operation,
            &completion,
            now,
        )
        .unwrap();
        assert_eq!(decision.artifact_state, ArtifactState::Uploaded);
        assert_eq!(decision.artifact_version, 2);
        assert_eq!(decision.blob_version, 2);
        assert_eq!(decision.grant_state, ArtifactLinkState::Consumed);
        assert_eq!(decision.grant_version, 2);
        assert_eq!(decision.operation_state, JobState::Waiting);
        assert_eq!(decision.operation_version, 2);

        let mut forged = completion.clone();
        forged.grant_token_digest = digest('0');
        assert_eq!(
            decide_complete_upload(
                &bundle.artifact,
                &bundle.blob,
                &bundle.grant,
                &bundle.operation,
                &forged,
                now,
            ),
            Err(ArtifactCommandError::GrantRejected)
        );

        let mut wrong_size = completion.clone();
        wrong_size.observed_size_bytes -= 1;
        assert_eq!(
            decide_complete_upload(
                &bundle.artifact,
                &bundle.blob,
                &bundle.grant,
                &bundle.operation,
                &wrong_size,
                now,
            ),
            Err(ArtifactCommandError::UploadEvidenceMismatch)
        );

        let mut stale = completion;
        stale.expected_artifact_version = 2;
        assert_eq!(
            decide_complete_upload(
                &bundle.artifact,
                &bundle.blob,
                &bundle.grant,
                &bundle.operation,
                &stale,
                now,
            ),
            Err(ArtifactCommandError::StaleVersion)
        );
    }

    #[test]
    fn finalize_requires_exact_verified_facts_before_ready() {
        let now = Utc::now();
        let prepared = command(now);
        let mut bundle = prepared_bundle(now);
        bundle.artifact.state = ArtifactState::Uploaded;
        bundle.artifact.version = 2;
        bundle.blob.object_generation = Some("version-1".to_owned());
        bundle.blob.version = 2;
        bundle.grant.state = ArtifactLinkState::Consumed;
        bundle.grant.version = 2;
        bundle.operation.state = JobState::Waiting;
        bundle.operation.version = 2;

        bundle.artifact.state = ArtifactState::Verified;
        bundle.artifact.version = 4;
        bundle.artifact.verified_media_type = Some("application/json".to_owned());
        bundle.blob.state = BlobIntegrityState::Verified;
        bundle.blob.version = 3;
        bundle.blob.content_digest = Some(digest('9'));
        bundle.blob.size_bytes = Some(prepared.expected_size_bytes);
        bundle.operation.version = 3;
        let finalize = FinalizeArtifact {
            audit: prepared.audit,
            operation_id: prepared.operation_id,
            artifact_id: prepared.artifact_id,
            blob_id: prepared.blob_id,
            upload_grant_id: prepared.upload_grant_id,
            artifact_reference_id: id(ResourceKind::ArtifactLink, "1011"),
            quota_account_id: prepared.quota_account_id,
            quota_settle_entry_id: id(ResourceKind::QuotaLedgerEntry, "1012"),
            expected_artifact_version: 4,
            expected_blob_version: 3,
            expected_operation_version: 3,
            expected_grant_version: 2,
            expected_quota_account_version: 2,
            grant_generation: 1,
            object_generation: "version-1".to_owned(),
            content_digest: digest('9'),
            size_bytes: prepared.expected_size_bytes,
            verified_media_type: "application/json".to_owned(),
            reference_kind: ArtifactReferenceKind::Input,
        };
        let final_decision = decide_finalize_artifact(
            &bundle.artifact,
            &bundle.blob,
            &bundle.grant,
            &bundle.operation,
            &finalize,
        )
        .unwrap();
        assert_eq!(final_decision.artifact_state, ArtifactState::Ready);
        assert_eq!(final_decision.artifact_version, 5);
        assert_eq!(final_decision.operation_state, JobState::Succeeded);
        assert_eq!(final_decision.operation_version, 4);
        assert_eq!(final_decision.artifact_ref.content_digest(), &digest('9'));
        assert_eq!(
            final_decision.reference.reference_kind,
            ArtifactReferenceKind::Input
        );
    }

    #[test]
    fn hold_and_provenance_are_closed_independent_link_lifecycles() {
        let now = Utc::now();
        let prepared = command(now);
        let mut source = prepared_bundle(now).artifact;
        source.state = ArtifactState::Ready;
        source.version = 5;
        source.verified_media_type = Some("application/json".to_owned());

        let place = PlaceArtifactHold {
            audit: prepared.audit.clone(),
            artifact_hold_id: id(ResourceKind::ArtifactLink, "1015"),
            artifact_id: source.artifact_id.clone(),
            expected_artifact_version: 5,
            hold_kind: ArtifactHoldKind::Legal,
            reason_class: "litigation".to_owned(),
            evidence_digest: digest('7'),
            expires_at: None,
        };
        let snapshot = decide_place_artifact_hold(&source, &place, now).unwrap();
        assert_eq!(snapshot.hold_kind, ArtifactHoldKind::Legal);
        let hold = ArtifactHoldRecord {
            tenant_id: source.tenant_id.clone(),
            artifact_hold_id: place.artifact_hold_id.clone(),
            artifact_id: source.artifact_id.clone(),
            state: ArtifactLinkState::Active,
            version: 1,
            link_key_digest: snapshot.link_key_digest().unwrap(),
            snapshot,
            created_at: now,
            released_at: None,
        };
        let release = ReleaseArtifactHold {
            audit: prepared.audit.clone(),
            artifact_hold_id: hold.artifact_hold_id.clone(),
            artifact_id: source.artifact_id.clone(),
            expected_hold_version: 1,
            reason_class: "matter_closed".to_owned(),
            evidence_digest: digest('6'),
        };
        assert_eq!(
            decide_release_artifact_hold(&hold, &release).unwrap(),
            (ArtifactLinkState::Released, 2)
        );

        let mut derived = source.clone();
        derived.artifact_id = id(ResourceKind::Artifact, "1016");
        let provenance = CreateArtifactProvenance {
            audit: prepared.audit,
            provenance_link_id: id(ResourceKind::ArtifactLink, "1017"),
            source_artifact_id: source.artifact_id.clone(),
            derived_artifact_id: derived.artifact_id.clone(),
            transformation_deployment_id: id(ResourceKind::CapabilityDeployment, "1018"),
            producer_owner_id: id(ResourceKind::Job, "1019"),
            expected_source_version: 5,
            expected_derived_version: 5,
            parameters_digest: digest('5'),
        };
        let edge = decide_create_artifact_provenance(&source, &derived, &provenance).unwrap();
        assert_eq!(edge.source_artifact_id, source.artifact_id);
        assert_eq!(edge.derived_artifact_id, derived.artifact_id);

        source.classification = DataClassification::Restricted;
        assert_eq!(
            decide_create_artifact_provenance(&source, &derived, &provenance),
            Err(ArtifactCommandError::InvalidProvenance)
        );
    }

    #[test]
    fn deletion_enforces_gc_grace_links_and_exact_shared_blob_evidence() {
        let now = Utc::now();
        let prepared = command(now);
        let mut target = prepared_bundle(now).artifact;
        target.state = ArtifactState::Ready;
        target.version = 5;
        target.verified_media_type = Some("application/json".to_owned());
        target.retain_until = now - ChronoDuration::hours(2);

        let mut blob = prepared_bundle(now).blob;
        blob.state = BlobIntegrityState::Verified;
        blob.version = 3;
        blob.object_generation = Some("version-1".to_owned());
        blob.content_digest = Some(digest('9'));
        blob.size_bytes = Some(target.expected_size_bytes);

        let mark = MarkArtifactDeletion {
            audit: CommandAudit {
                trace: insight_platform_contracts::TraceIdentityV1::generate(),
                tenant_id: target.tenant_id.clone(),
                principal_id: prepared.audit.principal_id.clone(),
                principal_kind: prepared.audit.principal_kind,
                receipt_id: id(ResourceKind::Receipt, "1020"),
                event_id: id(ResourceKind::Event, "1021"),
                outbox_id: id(ResourceKind::OutboxEvent, "1022"),
                idempotency_key_digest: digest('1'),
                request_digest: digest('2'),
                receipt_expires_at: now + ChronoDuration::hours(2),
            },
            deletion_operation_id: id(ResourceKind::Job, "1023"),
            deletion_job_id: id(ResourceKind::Job, "1024"),
            artifact_id: target.artifact_id.clone(),
            blob_id: blob.blob_id.clone(),
            expected_artifact_version: 5,
            expected_blob_version: 3,
            approval_task_id: Some(id(ResourceKind::ApprovalTask, "1025")),
            retry_backoff_milliseconds: 100,
            deadline: now + ChronoDuration::hours(1),
        };
        let admitted = ArtifactDeletionAdmissionFacts {
            approval_required: true,
            approval_satisfied: true,
            gc_grace_seconds: 3_600,
            live_reference_count: 0,
            active_hold_count: 0,
            blocking_provenance_count: 0,
        };

        let mut blocked = admitted;
        blocked.live_reference_count = 1;
        assert_eq!(
            decide_mark_artifact_deletion(&target, &blob, None, blocked, &mark, now),
            Err(ArtifactCommandError::DeletionBlocked)
        );
        assert_eq!(
            decide_mark_artifact_deletion(
                &target,
                &blob,
                None,
                admitted,
                &mark,
                target.retain_until + ChronoDuration::minutes(59),
            ),
            Err(ArtifactCommandError::DeletionBlocked)
        );

        let mut alias = target.clone();
        alias.artifact_id = id(ResourceKind::Artifact, "1026");
        alias.version = 8;
        let shared =
            decide_mark_artifact_deletion(&target, &blob, Some(&alias), admitted, &mark, now)
                .unwrap();
        assert_eq!(shared.artifact_state, ArtifactState::Deleting);
        assert_eq!(shared.artifact_version, 6);
        assert_eq!(shared.blob_state, BlobIntegrityState::Verified);
        assert_eq!(shared.blob_version, 3);
        assert_eq!(
            shared.mode,
            ArtifactDeletionMode::ArtifactOnly {
                alias_artifact_id: alias.artifact_id.clone(),
                alias_artifact_version: 8,
            }
        );

        let mut deleting_target = target.clone();
        deleting_target.state = shared.artifact_state;
        deleting_target.version = shared.artifact_version;
        let shared_deletion = ArtifactDeletionRecord {
            tenant_id: target.tenant_id.clone(),
            operation_id: mark.deletion_operation_id.clone(),
            operation_state: JobState::Running,
            operation_version: 1,
            job_id: mark.deletion_job_id.clone(),
            artifact_id: target.artifact_id.clone(),
            blob_id: blob.blob_id.clone(),
            mode: shared.mode.clone(),
            deadline: mark.deadline,
        };
        let deletion_worker = id(ResourceKind::WorkerProcessGeneration, "1027");
        let complete_shared = CompleteArtifactDeletion {
            audit: worker_audit(&target.tenant_id, &deletion_worker, "103"),
            deletion_operation_id: mark.deletion_operation_id.clone(),
            deletion_job_id: mark.deletion_job_id.clone(),
            artifact_id: target.artifact_id.clone(),
            blob_id: blob.blob_id.clone(),
            expected_artifact_version: 6,
            expected_blob_version: 3,
            expected_operation_version: 1,
            fence: insight_platform_jobs::JobFence {
                expected_version: 3,
                worker_process_generation_id: deletion_worker,
                lease_generation: 1,
                token_digest: digest('3'),
            },
            evidence: ArtifactDeletionEvidence::ArtifactOnly {
                alias_artifact_id: alias.artifact_id.clone(),
                alias_artifact_version: alias.version,
            },
        };
        let completed_shared = decide_complete_artifact_deletion(
            &deleting_target,
            &blob,
            &shared_deletion,
            Some(&alias),
            &complete_shared,
        )
        .unwrap();
        assert_eq!(completed_shared.artifact_state, ArtifactState::Deleted);
        assert_eq!(completed_shared.blob_state, BlobIntegrityState::Verified);
        assert_eq!(completed_shared.blob_version, 3);

        let physical =
            decide_mark_artifact_deletion(&target, &blob, None, admitted, &mark, now).unwrap();
        assert_eq!(
            physical.mode,
            ArtifactDeletionMode::BlobGeneration {
                object_generation: "version-1".to_owned(),
            }
        );
        assert_eq!(physical.blob_state, BlobIntegrityState::Deleting);
        assert_eq!(physical.blob_version, 4);

        deleting_target.state = physical.artifact_state;
        deleting_target.version = physical.artifact_version;
        let mut deleting_blob = blob.clone();
        deleting_blob.state = physical.blob_state;
        deleting_blob.version = physical.blob_version;
        let physical_deletion = ArtifactDeletionRecord {
            mode: physical.mode,
            ..shared_deletion
        };
        let mut complete_physical = CompleteArtifactDeletion {
            expected_blob_version: 4,
            evidence: ArtifactDeletionEvidence::BlobGeneration {
                object_generation: "wrong-generation".to_owned(),
                backend_receipt_digest: digest('4'),
                absence_evidence_digest: digest('5'),
            },
            ..complete_shared
        };
        assert_eq!(
            decide_complete_artifact_deletion(
                &deleting_target,
                &deleting_blob,
                &physical_deletion,
                None,
                &complete_physical,
            ),
            Err(ArtifactCommandError::DeletionEvidenceMismatch)
        );
        complete_physical.evidence = ArtifactDeletionEvidence::BlobGeneration {
            object_generation: "version-1".to_owned(),
            backend_receipt_digest: digest('4'),
            absence_evidence_digest: digest('5'),
        };
        let completed_physical = decide_complete_artifact_deletion(
            &deleting_target,
            &deleting_blob,
            &physical_deletion,
            None,
            &complete_physical,
        )
        .unwrap();
        assert_eq!(completed_physical.artifact_state, ArtifactState::Deleted);
        assert_eq!(completed_physical.blob_state, BlobIntegrityState::Deleted);
        assert_eq!(completed_physical.blob_version, 5);
    }
}
