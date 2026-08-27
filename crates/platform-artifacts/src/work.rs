use crate::{
    ArtifactBlobCleanupSnapshot, ArtifactBlobRecord, ArtifactBlobSecurityDomain,
    ArtifactCommandError, ArtifactDeletionEvidence, ArtifactDeletionJobSnapshot,
    ArtifactDeletionMode, ArtifactMetadataSnapshot, ArtifactOperationRecord, ArtifactRecord,
    CompleteArtifactDeletion, CompletedArtifactDeletion, MAX_ARTIFACT_RETRY_BACKOFF_MILLISECONDS,
    MAX_BACKEND_BYTES, MAX_KEY_ID_BYTES, MAX_OBJECT_REFERENCE_BYTES,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, ArtifactPurpose, ArtifactState, BlobIntegrityState, CommandAudit,
    CommandOutcome, DataClassification, ExactVersionRef, JobState, ResourceId, ResourceKind,
    Sha256Digest, MAX_MCP_RESPONSE_BYTES,
};
use insight_platform_jobs::{
    decide_expired_lease, decide_reconciliation, decide_retry, decide_terminal, JobFence,
    JobProjection,
};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest as _, Sha256};
use std::{error::Error, fmt, fmt::Write as _};

const MAX_ARTIFACT_REASON_BYTES: usize = 64;
const MAX_ARTIFACT_OBJECT_GENERATION_BYTES: usize = 255;
const MAX_ARTIFACT_BACKEND_FAILURE_BYTES: usize = 1_024;
const MAX_ARTIFACT_EVIDENCE_TTL_MILLISECONDS: u64 = 7 * 24 * 60 * 60 * 1_000;
pub const MAX_WORKLOAD_ARTIFACT_STAGE_BYTES: usize = MAX_MCP_RESPONSE_BYTES as usize;

mod canonical_base64url_bytes {
    use super::*;

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        let decoded = URL_SAFE_NO_PAD.decode(&encoded).map_err(D::Error::custom)?;
        if URL_SAFE_NO_PAD.encode(&decoded) != encoded {
            return Err(D::Error::custom(
                "descriptor bytes are not canonical base64url",
            ));
        }
        Ok(decoded)
    }
}

#[derive(Debug, Clone)]
pub struct ScheduleInitialArtifactScan {
    pub audit: CommandAudit,
    pub scan_job_id: ResourceId,
    pub operation_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub expected_artifact_version: u64,
    pub expected_blob_version: u64,
    pub expected_operation_version: u64,
    pub scan_policy_revision: ExactVersionRef,
    pub scanner_contract_digest: Sha256Digest,
    pub ruleset_digest: Sha256Digest,
    pub evidence_ttl_milliseconds: u64,
    pub retry_backoff_milliseconds: u64,
    pub deadline: DateTime<Utc>,
}

impl ScheduleInitialArtifactScan {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactWorkError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ArtifactWorkError::InvalidAudit)?;
        validate_scan_schedule(
            &self.scan_job_id,
            &self.operation_id,
            &self.artifact_id,
            &self.blob_id,
            self.expected_artifact_version,
            self.expected_blob_version,
            self.expected_operation_version,
            &self.scan_policy_revision,
            self.evidence_ttl_milliseconds,
            self.retry_backoff_milliseconds,
            self.deadline,
            now,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactScanScheduleDecision {
    pub artifact_state: ArtifactState,
    pub artifact_version: u64,
    pub job: ArtifactScanJobSnapshot,
}

pub fn decide_schedule_initial_scan(
    artifact: &ArtifactRecord,
    blob: &ArtifactBlobRecord,
    operation: &ArtifactOperationRecord,
    command: &ScheduleInitialArtifactScan,
    now: DateTime<Utc>,
) -> Result<ArtifactScanScheduleDecision, ArtifactWorkError> {
    command.validate_at(now)?;
    if artifact.tenant_id != command.audit.tenant_id
        || blob.tenant_id != command.audit.tenant_id
        || operation.tenant_id != command.audit.tenant_id
        || artifact.artifact_id != command.artifact_id
        || artifact.blob_id.as_ref() != Some(&command.blob_id)
        || blob.blob_id != command.blob_id
        || operation.operation_id != command.operation_id
        || operation.snapshot.artifact_id != command.artifact_id
    {
        return Err(ArtifactWorkError::InvalidCommand);
    }
    if artifact.version != command.expected_artifact_version
        || blob.version != command.expected_blob_version
        || operation.version != command.expected_operation_version
    {
        return Err(ArtifactWorkError::EvidenceMismatch);
    }
    if artifact.state != ArtifactState::Uploaded
        || blob.state != BlobIntegrityState::Staging
        || blob.object_generation.is_none()
        || operation.state != JobState::Waiting
        || !operation.state.can_transition_to(JobState::Ready)
        || !artifact.state.can_transition_to(ArtifactState::Verifying)
    {
        return Err(ArtifactWorkError::InvalidTransition);
    }
    let artifact_version = increment(artifact.version)?;
    let job = ArtifactScanJobSnapshot {
        schema_version: 2,
        scan_kind: ArtifactScanKind::Initial,
        operation_id: command.operation_id.clone(),
        producer_job_id: None,
        artifact_id: command.artifact_id.clone(),
        blob_id: command.blob_id.clone(),
        expected_artifact_version: artifact_version,
        expected_blob_version: blob.version,
        expected_operation_version: increment(operation.version)?,
        object_generation: blob
            .object_generation
            .clone()
            .ok_or(ArtifactWorkError::InvalidTransition)?,
        scan_policy_revision: command.scan_policy_revision.clone(),
        scanner_contract_digest: command.scanner_contract_digest.clone(),
        ruleset_digest: command.ruleset_digest.clone(),
        evidence_ttl_milliseconds: command.evidence_ttl_milliseconds,
        retry_backoff_milliseconds: command.retry_backoff_milliseconds,
    };
    job.validate()?;
    Ok(ArtifactScanScheduleDecision {
        artifact_state: ArtifactState::Verifying,
        artifact_version,
        job,
    })
}

#[derive(Debug, Clone)]
pub struct ScheduleArtifactRescan {
    pub audit: CommandAudit,
    pub rescan_operation_id: ResourceId,
    pub rescan_job_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub expected_artifact_version: u64,
    pub expected_blob_version: u64,
    pub scan_policy_revision: ExactVersionRef,
    pub scanner_contract_digest: Sha256Digest,
    pub ruleset_digest: Sha256Digest,
    pub evidence_ttl_milliseconds: u64,
    pub retry_backoff_milliseconds: u64,
    pub deadline: DateTime<Utc>,
}

impl ScheduleArtifactRescan {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactWorkError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ArtifactWorkError::InvalidAudit)?;
        validate_scan_schedule(
            &self.rescan_job_id,
            &self.rescan_operation_id,
            &self.artifact_id,
            &self.blob_id,
            self.expected_artifact_version,
            self.expected_blob_version,
            1,
            &self.scan_policy_revision,
            self.evidence_ttl_milliseconds,
            self.retry_backoff_milliseconds,
            self.deadline,
            now,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRescanOperationSnapshot {
    pub schema_version: u32,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub scan_policy_revision: ExactVersionRef,
    pub scanner_contract_digest: Sha256Digest,
    pub ruleset_digest: Sha256Digest,
}

impl ArtifactRescanOperationSnapshot {
    pub fn validate(&self) -> Result<(), ArtifactWorkError> {
        if self.schema_version != 1
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.scan_policy_revision.resource_kind != ResourceKind::PolicyRevision
            || self.scan_policy_revision.validate().is_err()
        {
            return Err(ArtifactWorkError::InvalidJobPayload);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRescanScheduleDecision {
    pub artifact_state: ArtifactState,
    pub artifact_version: u64,
    pub operation: ArtifactRescanOperationSnapshot,
    pub job: ArtifactScanJobSnapshot,
}

pub fn decide_schedule_artifact_rescan(
    artifact: &ArtifactRecord,
    blob: &ArtifactBlobRecord,
    command: &ScheduleArtifactRescan,
    now: DateTime<Utc>,
) -> Result<ArtifactRescanScheduleDecision, ArtifactWorkError> {
    command.validate_at(now)?;
    if artifact.tenant_id != command.audit.tenant_id
        || blob.tenant_id != command.audit.tenant_id
        || artifact.artifact_id != command.artifact_id
        || artifact.blob_id.as_ref() != Some(&command.blob_id)
        || blob.blob_id != command.blob_id
        || artifact.version != command.expected_artifact_version
        || blob.version != command.expected_blob_version
    {
        return Err(ArtifactWorkError::EvidenceMismatch);
    }
    if artifact.state != ArtifactState::Ready
        || blob.state != BlobIntegrityState::Verified
        || blob.object_generation.is_none()
        || blob.content_digest.is_none()
        || blob.size_bytes.is_none()
        || artifact.verified_media_type.is_none()
        || !artifact.state.can_transition_to(ArtifactState::Quarantined)
    {
        return Err(ArtifactWorkError::InvalidTransition);
    }
    let operation = ArtifactRescanOperationSnapshot {
        schema_version: 1,
        artifact_id: artifact.artifact_id.clone(),
        blob_id: blob.blob_id.clone(),
        scan_policy_revision: command.scan_policy_revision.clone(),
        scanner_contract_digest: command.scanner_contract_digest.clone(),
        ruleset_digest: command.ruleset_digest.clone(),
    };
    operation.validate()?;
    let artifact_version = increment(artifact.version)?;
    let job = ArtifactScanJobSnapshot {
        schema_version: 2,
        scan_kind: ArtifactScanKind::Rescan,
        operation_id: command.rescan_operation_id.clone(),
        producer_job_id: None,
        artifact_id: artifact.artifact_id.clone(),
        blob_id: blob.blob_id.clone(),
        expected_artifact_version: artifact_version,
        expected_blob_version: blob.version,
        expected_operation_version: 1,
        object_generation: blob
            .object_generation
            .clone()
            .ok_or(ArtifactWorkError::InvalidTransition)?,
        scan_policy_revision: command.scan_policy_revision.clone(),
        scanner_contract_digest: command.scanner_contract_digest.clone(),
        ruleset_digest: command.ruleset_digest.clone(),
        evidence_ttl_milliseconds: command.evidence_ttl_milliseconds,
        retry_backoff_milliseconds: command.retry_backoff_milliseconds,
    };
    job.validate()?;
    Ok(ArtifactRescanScheduleDecision {
        artifact_state: ArtifactState::Quarantined,
        artifact_version,
        operation,
        job,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactScanKind {
    Initial,
    Rescan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactScanDisposition {
    Verified,
    Quarantined,
    Rejected,
    Corrupt,
}

impl ArtifactScanDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Quarantined => "quarantined",
            Self::Rejected => "rejected",
            Self::Corrupt => "corrupt",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactScanJobSnapshot {
    pub schema_version: u32,
    pub scan_kind: ArtifactScanKind,
    pub operation_id: ResourceId,
    pub producer_job_id: Option<ResourceId>,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub expected_artifact_version: u64,
    pub expected_blob_version: u64,
    pub expected_operation_version: u64,
    pub object_generation: String,
    pub scan_policy_revision: ExactVersionRef,
    pub scanner_contract_digest: Sha256Digest,
    pub ruleset_digest: Sha256Digest,
    pub evidence_ttl_milliseconds: u64,
    pub retry_backoff_milliseconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactAwaitingStageSnapshot {
    pub schema_version: u32,
    pub producer_job_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub quota_account_id: ResourceId,
    pub quota_entry_id: ResourceId,
    pub purpose: ArtifactPurpose,
    pub classification: DataClassification,
    pub maximum_bytes: u64,
    pub declared_media_type: String,
    pub retention_policy_revision: ExactVersionRef,
    pub artifact_io_policy_revision: ExactVersionRef,
    pub scanner_contract_digest: Sha256Digest,
    pub ruleset_digest: Sha256Digest,
    pub evidence_ttl_milliseconds: u64,
    pub retry_backoff_milliseconds: u64,
    pub write_storage_binding_digest: Sha256Digest,
    pub encryption_domain_id: ResourceId,
    pub retain_until: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
}

/// Closed, credential-free evidence submitted by a trusted producer after the Data Worker has
/// persisted one exact object generation. Object locators remain encrypted and are never accepted
/// from public or untrusted callers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageWorkloadArtifact {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub caller: insight_platform_contracts::ArtifactWorkloadAudience,
    pub producer_job_id: ResourceId,
    pub producer_fence: JobFence,
    pub verification_job_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub content_digest: Sha256Digest,
    pub size_bytes: u64,
    pub media_type: String,
    pub storage_backend: String,
    pub storage_binding_digest: Sha256Digest,
    pub object_reference_ciphertext: Vec<u8>,
    pub object_generation: String,
    pub key_id: String,
    pub encryption_domain_id: ResourceId,
    pub backend_evidence_digest: Sha256Digest,
    pub staged_at: DateTime<Utc>,
}

/// RPC-safe producer request. Storage selection, encrypted locators and backend evidence are
/// deliberately absent because they are owned by the Artifact Data Worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageWorkloadArtifactRequest {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub producer_job_id: ResourceId,
    pub producer_fence: JobFence,
    pub verification_job_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    #[serde(with = "canonical_base64url_bytes")]
    pub descriptor_bytes: Vec<u8>,
    pub descriptor_digest: Sha256Digest,
    pub media_type: String,
}

impl StageWorkloadArtifactRequest {
    pub fn validate(&self) -> Result<(), ArtifactWorkError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.producer_job_id.kind() != ResourceKind::Job
            || self.producer_fence.expected_version == 0
            || self.producer_fence.worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.producer_fence.lease_generation == 0
            || self.verification_job_id.kind() != ResourceKind::Job
            || self.verification_job_id == self.producer_job_id
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.descriptor_bytes.is_empty()
            || self.descriptor_bytes.len() > MAX_WORKLOAD_ARTIFACT_STAGE_BYTES
            || self.media_type.is_empty()
            || self.media_type.len() > 255
            || self.media_type.chars().any(char::is_control)
            || raw_digest(&self.descriptor_bytes) != self.descriptor_digest
        {
            return Err(ArtifactWorkError::InvalidCommand);
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        awaiting: &ArtifactAwaitingStageSnapshot,
        now: DateTime<Utc>,
    ) -> Result<(), ArtifactWorkError> {
        self.validate()?;
        awaiting.validate()?;
        if self.producer_job_id != awaiting.producer_job_id
            || self.artifact_id != awaiting.artifact_id
            || self.blob_id != awaiting.blob_id
            || u64::try_from(self.descriptor_bytes.len())
                .ok()
                .is_none_or(|length| length > awaiting.maximum_bytes)
            || self.media_type != awaiting.declared_media_type
            || now > awaiting.deadline
        {
            return Err(ArtifactWorkError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedWorkloadArtifactStage {
    pub tenant_id: ResourceId,
    pub producer_job_id: ResourceId,
    pub verification_job_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub descriptor_digest: Sha256Digest,
    pub size_bytes: u64,
    pub media_type: String,
    pub write_storage_binding_digest: Sha256Digest,
    pub encryption_domain_id: ResourceId,
    pub deadline: DateTime<Utc>,
}

impl AuthorizedWorkloadArtifactStage {
    pub fn validate_for(
        &self,
        request: &StageWorkloadArtifactRequest,
        now: DateTime<Utc>,
    ) -> Result<(), ArtifactWorkError> {
        request.validate()?;
        if self.tenant_id != request.tenant_id
            || self.producer_job_id != request.producer_job_id
            || self.verification_job_id != request.verification_job_id
            || self.artifact_id != request.artifact_id
            || self.blob_id != request.blob_id
            || self.descriptor_digest != request.descriptor_digest
            || self.size_bytes != u64::try_from(request.descriptor_bytes.len()).unwrap_or(u64::MAX)
            || self.media_type != request.media_type
            || self.encryption_domain_id.kind() != ResourceKind::EncryptionDomain
            || now > self.deadline
        {
            return Err(ArtifactWorkError::InvalidEvidence);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkloadArtifactStagePreflight {
    Authorized(AuthorizedWorkloadArtifactStage),
    Replayed(StagedWorkloadArtifact),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedWorkloadArtifact {
    pub schema_version: u32,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub verification_job_id: ResourceId,
    pub content_digest: Sha256Digest,
    pub size_bytes: u64,
    pub object_generation: String,
    pub artifact_version: u64,
    pub blob_version: u64,
    pub verification_job_version: u64,
}

impl StagedWorkloadArtifact {
    pub fn validate(&self) -> Result<(), ArtifactWorkError> {
        if self.schema_version != 1
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.verification_job_id.kind() != ResourceKind::Job
            || self.size_bytes == 0
            || !valid_object_generation(&self.object_generation)
            || self.artifact_version == 0
            || self.blob_version == 0
            || self.verification_job_version == 0
        {
            return Err(ArtifactWorkError::InvalidEvidence);
        }
        Ok(())
    }
}

impl StageWorkloadArtifact {
    pub fn validate_for(
        &self,
        awaiting: &ArtifactAwaitingStageSnapshot,
        now: DateTime<Utc>,
    ) -> Result<(), ArtifactWorkError> {
        awaiting.validate()?;
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.caller != insight_platform_contracts::ArtifactWorkloadAudience::McpHost
            || self.producer_job_id != awaiting.producer_job_id
            || self.verification_job_id == self.producer_job_id
            || self.producer_fence.expected_version == 0
            || self.producer_fence.worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.producer_fence.lease_generation == 0
            || self.verification_job_id.kind() != ResourceKind::Job
            || self.artifact_id != awaiting.artifact_id
            || self.blob_id != awaiting.blob_id
            || self.size_bytes == 0
            || self.size_bytes > awaiting.maximum_bytes
            || self.media_type != awaiting.declared_media_type
            || self.storage_binding_digest != awaiting.write_storage_binding_digest
            || !valid_code(&self.storage_backend, MAX_BACKEND_BYTES)
            || self.object_reference_ciphertext.is_empty()
            || self.object_reference_ciphertext.len() > MAX_OBJECT_REFERENCE_BYTES
            || !valid_object_generation(&self.object_generation)
            || self.key_id.is_empty()
            || self.key_id.len() > MAX_KEY_ID_BYTES
            || self.key_id.chars().any(char::is_control)
            || self.encryption_domain_id != awaiting.encryption_domain_id
            || self.staged_at > now
            || self.staged_at > awaiting.deadline
            || now > awaiting.deadline
        {
            return Err(ArtifactWorkError::InvalidCommand);
        }
        Ok(())
    }

    pub fn metadata(&self) -> Result<ArtifactMetadataSnapshot, ArtifactWorkError> {
        ArtifactMetadataSnapshot::new(None, self.verification_job_id.clone())
            .map_err(|_| ArtifactWorkError::InvalidCommand)
    }

    pub fn security_domain(
        &self,
        awaiting: &ArtifactAwaitingStageSnapshot,
    ) -> Result<ArtifactBlobSecurityDomain, ArtifactWorkError> {
        self.validate_for(awaiting, self.staged_at)?;
        Ok(ArtifactBlobSecurityDomain {
            schema_version: 1,
            classification: awaiting.classification,
            retention_policy_revision_id: awaiting.retention_policy_revision.revision_id.clone(),
            encryption_domain_id: self.encryption_domain_id.clone(),
        })
    }

    pub fn scan_payload(
        &self,
        awaiting: &ArtifactAwaitingStageSnapshot,
        artifact_version: u64,
        blob_version: u64,
        verification_job_version: u64,
    ) -> Result<ArtifactJobPayload, ArtifactWorkError> {
        self.validate_for(awaiting, self.staged_at)?;
        let payload = ArtifactJobPayload::Scan {
            scan: ArtifactScanJobSnapshot {
                schema_version: 2,
                scan_kind: ArtifactScanKind::Initial,
                operation_id: self.verification_job_id.clone(),
                producer_job_id: Some(self.producer_job_id.clone()),
                artifact_id: self.artifact_id.clone(),
                blob_id: self.blob_id.clone(),
                expected_artifact_version: artifact_version,
                expected_blob_version: blob_version,
                expected_operation_version: verification_job_version,
                object_generation: self.object_generation.clone(),
                scan_policy_revision: awaiting.artifact_io_policy_revision.clone(),
                scanner_contract_digest: awaiting.scanner_contract_digest.clone(),
                ruleset_digest: awaiting.ruleset_digest.clone(),
                evidence_ttl_milliseconds: awaiting.evidence_ttl_milliseconds,
                retry_backoff_milliseconds: awaiting.retry_backoff_milliseconds,
            },
        };
        payload.validate_for_owner(&self.artifact_id)?;
        Ok(payload)
    }
}

impl ArtifactAwaitingStageSnapshot {
    pub fn validate(&self) -> Result<(), ArtifactWorkError> {
        if self.schema_version != 1
            || self.producer_job_id.kind() != ResourceKind::Job
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.quota_account_id.kind() != ResourceKind::QuotaAccount
            || self.quota_entry_id.kind() != ResourceKind::QuotaLedgerEntry
            || self.maximum_bytes == 0
            || self.declared_media_type.is_empty()
            || self.declared_media_type.len() > 255
            || self.retention_policy_revision.resource_kind != ResourceKind::PolicyRevision
            || self.retention_policy_revision.validate().is_err()
            || self.artifact_io_policy_revision.resource_kind != ResourceKind::PolicyRevision
            || self.artifact_io_policy_revision.validate().is_err()
            || self.evidence_ttl_milliseconds == 0
            || self.evidence_ttl_milliseconds > 86_400_000
            || self.retry_backoff_milliseconds == 0
            || self.retry_backoff_milliseconds > MAX_ARTIFACT_RETRY_BACKOFF_MILLISECONDS
            || self.retry_backoff_milliseconds >= self.evidence_ttl_milliseconds
            || self.encryption_domain_id.kind() != ResourceKind::EncryptionDomain
            || self.deadline > self.retain_until
        {
            return Err(ArtifactWorkError::InvalidJobPayload);
        }
        Ok(())
    }
}

impl ArtifactScanJobSnapshot {
    pub fn validate(&self) -> Result<(), ArtifactWorkError> {
        if self.schema_version != 2
            || self.operation_id.kind() != ResourceKind::Job
            || self.producer_job_id.as_ref().is_some_and(|producer| {
                producer.kind() != ResourceKind::Job || producer == &self.operation_id
            })
            || (self.scan_kind == ArtifactScanKind::Rescan && self.producer_job_id.is_some())
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.expected_artifact_version == 0
            || self.expected_blob_version == 0
            || self.expected_operation_version == 0
            || !valid_object_generation(&self.object_generation)
            || self.scan_policy_revision.resource_kind != ResourceKind::PolicyRevision
            || self.scan_policy_revision.validate().is_err()
            || self.evidence_ttl_milliseconds == 0
            || self.retry_backoff_milliseconds == 0
            || self.retry_backoff_milliseconds > MAX_ARTIFACT_RETRY_BACKOFF_MILLISECONDS
        {
            return Err(ArtifactWorkError::InvalidJobPayload);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactJobPayload {
    AwaitingStage {
        stage: ArtifactAwaitingStageSnapshot,
    },
    Scan {
        scan: ArtifactScanJobSnapshot,
    },
    Rescan {
        scan: ArtifactScanJobSnapshot,
    },
    Delete {
        deletion: ArtifactDeletionJobSnapshot,
    },
    BlobCleanup {
        cleanup: ArtifactBlobCleanupSnapshot,
    },
}

impl ArtifactJobPayload {
    pub fn validate_for_owner(&self, owner_id: &ResourceId) -> Result<(), ArtifactWorkError> {
        match self {
            Self::AwaitingStage { stage } => {
                stage.validate()?;
                require_owner(owner_id, ResourceKind::Artifact, &stage.artifact_id)
            }
            Self::Scan { scan } if scan.scan_kind == ArtifactScanKind::Initial => {
                scan.validate()?;
                require_owner(owner_id, ResourceKind::Artifact, &scan.artifact_id)
            }
            Self::Rescan { scan } if scan.scan_kind == ArtifactScanKind::Rescan => {
                scan.validate()?;
                require_owner(owner_id, ResourceKind::Artifact, &scan.artifact_id)
            }
            Self::Delete { deletion } => {
                deletion
                    .canonical_digest()
                    .map_err(|_| ArtifactWorkError::InvalidJobPayload)?;
                require_owner(owner_id, ResourceKind::Artifact, &deletion.artifact_id)
            }
            Self::BlobCleanup { cleanup } => {
                cleanup
                    .canonical_digest()
                    .map_err(|_| ArtifactWorkError::InvalidJobPayload)?;
                require_owner(
                    owner_id,
                    ResourceKind::InternalBlob,
                    &cleanup.discarded_blob_id,
                )
            }
            _ => Err(ArtifactWorkError::InvalidJobPayload),
        }
    }

    pub const fn retry_backoff_milliseconds(&self) -> u64 {
        match self {
            Self::AwaitingStage { stage } => stage.retry_backoff_milliseconds,
            Self::Scan { scan } | Self::Rescan { scan } => scan.retry_backoff_milliseconds,
            Self::Delete { deletion } => deletion.retry_backoff_milliseconds,
            Self::BlobCleanup { cleanup } => cleanup.retry_backoff_milliseconds,
        }
    }

    pub const fn may_have_uncertain_physical_effect(&self) -> bool {
        matches!(
            self,
            Self::Delete {
                deletion: ArtifactDeletionJobSnapshot {
                    mode: ArtifactDeletionMode::BlobGeneration { .. },
                    ..
                }
            } | Self::BlobCleanup { .. }
        )
    }

    pub const fn kind_name(&self) -> &'static str {
        match self {
            Self::AwaitingStage { .. } => "awaiting_stage",
            Self::Scan { .. } => "scan",
            Self::Rescan { .. } => "rescan",
            Self::Delete { .. } => "delete",
            Self::BlobCleanup { .. } => "blob_cleanup",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactRecoveryParentAction {
    None,
    Scan {
        artifact_state: ArtifactState,
        operation_state: JobState,
    },
    Deletion {
        operation_state: JobState,
    },
    BlobCleanupReconciliation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactAttemptRecoveryDecision {
    pub job: JobProjection,
    pub parent_action: ArtifactRecoveryParentAction,
}

pub fn decide_expired_artifact_attempt(
    current: &JobProjection,
    payload: &ArtifactJobPayload,
    observed_version: u64,
    observed_lease_generation: u64,
    database_now: DateTime<Utc>,
) -> Result<ArtifactAttemptRecoveryDecision, ArtifactWorkError> {
    payload.validate_for_owner(&current.owner.owner_id)?;
    if current.work_class != insight_platform_contracts::WorkClass::Artifact {
        return Err(ArtifactWorkError::InvalidJobPayload);
    }
    let (target, retry_at) = match current.state {
        JobState::Leased if database_now < current.deadline => (JobState::Ready, None),
        JobState::Leased => (JobState::TimedOut, None),
        JobState::Running
            if !payload.may_have_uncertain_physical_effect()
                && current.attempt_count < current.attempt_limit =>
        {
            let retry_at = database_now
                .checked_add_signed(chrono::Duration::milliseconds(
                    i64::try_from(payload.retry_backoff_milliseconds())
                        .map_err(|_| ArtifactWorkError::InvalidJobPayload)?,
                ))
                .ok_or(ArtifactWorkError::CounterOverflow)?;
            if retry_at < current.deadline {
                (JobState::RetryScheduled, Some(retry_at))
            } else {
                (JobState::ReconciliationRequired, None)
            }
        }
        JobState::Running | JobState::Cancelling => (JobState::ReconciliationRequired, None),
        _ => return Err(ArtifactWorkError::InvalidTransition),
    };
    let job = decide_expired_lease(
        current,
        observed_version,
        observed_lease_generation,
        database_now,
        target,
        retry_at,
    )
    .map_err(|_| ArtifactWorkError::InvalidTransition)?;
    let parent_action = match job.state {
        JobState::Ready | JobState::RetryScheduled => ArtifactRecoveryParentAction::None,
        JobState::TimedOut => match payload {
            ArtifactJobPayload::AwaitingStage { .. } => {
                return Err(ArtifactWorkError::InvalidTransition);
            }
            ArtifactJobPayload::Scan { .. } | ArtifactJobPayload::Rescan { .. } => {
                ArtifactRecoveryParentAction::Scan {
                    artifact_state: ArtifactState::Quarantined,
                    operation_state: JobState::TimedOut,
                }
            }
            ArtifactJobPayload::Delete { .. } => ArtifactRecoveryParentAction::Deletion {
                operation_state: JobState::TimedOut,
            },
            ArtifactJobPayload::BlobCleanup { .. } => {
                ArtifactRecoveryParentAction::BlobCleanupReconciliation
            }
        },
        JobState::ReconciliationRequired => match payload {
            ArtifactJobPayload::AwaitingStage { .. } => {
                return Err(ArtifactWorkError::InvalidTransition);
            }
            ArtifactJobPayload::Scan { .. } | ArtifactJobPayload::Rescan { .. } => {
                ArtifactRecoveryParentAction::Scan {
                    artifact_state: ArtifactState::Quarantined,
                    operation_state: JobState::Failed,
                }
            }
            ArtifactJobPayload::Delete { .. } => ArtifactRecoveryParentAction::Deletion {
                operation_state: JobState::Failed,
            },
            ArtifactJobPayload::BlobCleanup { .. } => {
                ArtifactRecoveryParentAction::BlobCleanupReconciliation
            }
        },
        _ => return Err(ArtifactWorkError::InvalidTransition),
    };
    Ok(ArtifactAttemptRecoveryDecision { job, parent_action })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitArtifactAttemptFailure {
    pub audit: ArtifactWorkerAudit,
    pub job_id: ResourceId,
    pub fence: JobFence,
    pub failure: ArtifactBackendFailure,
}

impl CommitArtifactAttemptFailure {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactWorkError> {
        self.audit.validate_at(now)?;
        self.failure.validate()?;
        if self.job_id.kind() != ResourceKind::Job
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
        {
            return Err(ArtifactWorkError::InvalidCommand);
        }
        Ok(())
    }
}

pub fn decide_artifact_backend_failure(
    current: &JobProjection,
    payload: &ArtifactJobPayload,
    command: &CommitArtifactAttemptFailure,
    database_now: DateTime<Utc>,
) -> Result<ArtifactAttemptRecoveryDecision, ArtifactWorkError> {
    command.validate_at(database_now)?;
    payload.validate_for_owner(&current.owner.owner_id)?;
    if current.work_class != insight_platform_contracts::WorkClass::Artifact
        || current.job_id != command.job_id
    {
        return Err(ArtifactWorkError::InvalidJobPayload);
    }
    let (job, parent_action) = match payload {
        ArtifactJobPayload::AwaitingStage { .. } => {
            return Err(ArtifactWorkError::InvalidBackendFailure);
        }
        ArtifactJobPayload::Scan { .. } | ArtifactJobPayload::Rescan { .. }
            if command.failure.retryable =>
        {
            let retry_at = database_now
                .checked_add_signed(chrono::Duration::milliseconds(
                    i64::try_from(payload.retry_backoff_milliseconds())
                        .map_err(|_| ArtifactWorkError::InvalidJobPayload)?,
                ))
                .ok_or(ArtifactWorkError::CounterOverflow)?;
            match decide_retry(current, &command.fence, database_now, retry_at) {
                Ok(job) => (job, ArtifactRecoveryParentAction::None),
                Err(_) => (
                    decide_reconciliation(current, &command.fence, database_now)
                        .map_err(|_| ArtifactWorkError::InvalidTransition)?,
                    ArtifactRecoveryParentAction::Scan {
                        artifact_state: ArtifactState::Quarantined,
                        operation_state: JobState::Failed,
                    },
                ),
            }
        }
        ArtifactJobPayload::Scan { .. } | ArtifactJobPayload::Rescan { .. } => (
            decide_terminal(current, &command.fence, database_now, JobState::Failed)
                .map_err(|_| ArtifactWorkError::InvalidTransition)?,
            ArtifactRecoveryParentAction::Scan {
                artifact_state: ArtifactState::Quarantined,
                operation_state: JobState::Failed,
            },
        ),
        ArtifactJobPayload::Delete {
            deletion:
                ArtifactDeletionJobSnapshot {
                    mode: ArtifactDeletionMode::BlobGeneration { .. },
                    ..
                },
        } => (
            decide_reconciliation(current, &command.fence, database_now)
                .map_err(|_| ArtifactWorkError::InvalidTransition)?,
            ArtifactRecoveryParentAction::Deletion {
                operation_state: JobState::Failed,
            },
        ),
        ArtifactJobPayload::BlobCleanup { .. } => (
            decide_reconciliation(current, &command.fence, database_now)
                .map_err(|_| ArtifactWorkError::InvalidTransition)?,
            ArtifactRecoveryParentAction::BlobCleanupReconciliation,
        ),
        ArtifactJobPayload::Delete { .. } => return Err(ArtifactWorkError::InvalidBackendFailure),
    };
    Ok(ArtifactAttemptRecoveryDecision { job, parent_action })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactCurrentVerification {
    pub schema_version: u32,
    pub scan_kind: ArtifactScanKind,
    pub scan_job_id: ResourceId,
    pub scan_policy_revision: ExactVersionRef,
    pub scanner_contract_digest: Sha256Digest,
    pub ruleset_digest: Sha256Digest,
    pub object_generation: String,
    pub content_digest: Sha256Digest,
    pub size_bytes: u64,
    pub verified_media_type: String,
    pub disposition: ArtifactScanDisposition,
    pub reason_class: Option<String>,
    pub evidence_digest: Sha256Digest,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl ArtifactCurrentVerification {
    pub fn validate(&self) -> Result<(), ArtifactCommandError> {
        let reason_valid = match self.disposition {
            ArtifactScanDisposition::Verified => self.reason_class.is_none(),
            ArtifactScanDisposition::Quarantined
            | ArtifactScanDisposition::Rejected
            | ArtifactScanDisposition::Corrupt => self
                .reason_class
                .as_deref()
                .is_some_and(|reason| valid_code(reason, MAX_ARTIFACT_REASON_BYTES)),
        };
        if self.schema_version != 1
            || self.scan_job_id.kind() != ResourceKind::Job
            || self.scan_policy_revision.resource_kind != ResourceKind::PolicyRevision
            || self.scan_policy_revision.validate().is_err()
            || !valid_object_generation(&self.object_generation)
            || !valid_media_type(&self.verified_media_type)
            || self.expires_at <= self.observed_at
            || !reason_valid
        {
            return Err(ArtifactCommandError::InvalidVerification);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactScanEvidenceDraft {
    pub schema_version: u32,
    pub scan_kind: ArtifactScanKind,
    pub scan_job_id: ResourceId,
    pub scan_policy_revision: ExactVersionRef,
    pub scanner_contract_digest: Sha256Digest,
    pub ruleset_digest: Sha256Digest,
    pub object_generation: String,
    pub content_digest: Sha256Digest,
    pub size_bytes: u64,
    pub verified_media_type: String,
    pub disposition: ArtifactScanDisposition,
    pub reason_class: Option<String>,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl ArtifactScanEvidenceDraft {
    pub fn seal(self) -> Result<ArtifactScanEvidence, ArtifactWorkError> {
        let mut evidence = ArtifactScanEvidence {
            schema_version: self.schema_version,
            scan_kind: self.scan_kind,
            scan_job_id: self.scan_job_id,
            scan_policy_revision: self.scan_policy_revision,
            scanner_contract_digest: self.scanner_contract_digest,
            ruleset_digest: self.ruleset_digest,
            object_generation: self.object_generation,
            content_digest: self.content_digest,
            size_bytes: self.size_bytes,
            verified_media_type: self.verified_media_type,
            disposition: self.disposition,
            reason_class: self.reason_class,
            observed_at: self.observed_at,
            expires_at: self.expires_at,
            canonical_digest: format!("sha256:{}", "0".repeat(64))
                .parse::<Sha256Digest>()
                .map_err(|_| ArtifactWorkError::Canonicalization)?,
        };
        evidence.canonical_digest = evidence.canonical_digest_without_field()?;
        evidence.validate()?;
        Ok(evidence)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactScanEvidence {
    pub schema_version: u32,
    pub scan_kind: ArtifactScanKind,
    pub scan_job_id: ResourceId,
    pub scan_policy_revision: ExactVersionRef,
    pub scanner_contract_digest: Sha256Digest,
    pub ruleset_digest: Sha256Digest,
    pub object_generation: String,
    pub content_digest: Sha256Digest,
    pub size_bytes: u64,
    pub verified_media_type: String,
    pub disposition: ArtifactScanDisposition,
    pub reason_class: Option<String>,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub canonical_digest: Sha256Digest,
}

impl ArtifactScanEvidence {
    pub fn validate(&self) -> Result<(), ArtifactWorkError> {
        let current = self.current();
        current
            .validate()
            .map_err(|_| ArtifactWorkError::InvalidEvidence)?;
        if self.canonical_digest_without_field()? != self.canonical_digest {
            return Err(ArtifactWorkError::InvalidEvidence);
        }
        Ok(())
    }

    pub fn current(&self) -> ArtifactCurrentVerification {
        ArtifactCurrentVerification {
            schema_version: self.schema_version,
            scan_kind: self.scan_kind,
            scan_job_id: self.scan_job_id.clone(),
            scan_policy_revision: self.scan_policy_revision.clone(),
            scanner_contract_digest: self.scanner_contract_digest.clone(),
            ruleset_digest: self.ruleset_digest.clone(),
            object_generation: self.object_generation.clone(),
            content_digest: self.content_digest.clone(),
            size_bytes: self.size_bytes,
            verified_media_type: self.verified_media_type.clone(),
            disposition: self.disposition,
            reason_class: self.reason_class.clone(),
            evidence_digest: self.canonical_digest.clone(),
            observed_at: self.observed_at,
            expires_at: self.expires_at,
        }
    }

    fn canonical_digest_without_field(&self) -> Result<Sha256Digest, ArtifactWorkError> {
        let mut value =
            serde_json::to_value(self).map_err(|_| ArtifactWorkError::Canonicalization)?;
        value
            .as_object_mut()
            .ok_or(ArtifactWorkError::Canonicalization)?
            .remove("canonical_digest")
            .ok_or(ArtifactWorkError::Canonicalization)?;
        canonical_digest(&value)
            .map_err(|_| ArtifactWorkError::Canonicalization)?
            .parse()
            .map_err(|_| ArtifactWorkError::Canonicalization)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactWorkerAudit {
    pub trace: insight_platform_contracts::TraceIdentityV1,
    pub tenant_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl ArtifactWorkerAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactWorkError> {
        if self.trace.validate().is_err()
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= now
        {
            return Err(ArtifactWorkError::InvalidAudit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitArtifactScanOutcome {
    pub audit: ArtifactWorkerAudit,
    pub scan_job_id: ResourceId,
    pub fence: JobFence,
    pub operation_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub expected_artifact_version: u64,
    pub expected_blob_version: u64,
    pub expected_operation_version: u64,
    pub evidence: ArtifactScanEvidence,
    pub duplicate_blob_cleanup_job_id: ResourceId,
}

impl CommitArtifactScanOutcome {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactWorkError> {
        self.audit.validate_at(now)?;
        self.evidence.validate()?;
        if self.scan_job_id.kind() != ResourceKind::Job
            || self.scan_job_id != self.operation_id
            || self.evidence.scan_job_id != self.scan_job_id
            || self.operation_id.kind() != ResourceKind::Job
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.duplicate_blob_cleanup_job_id.kind() != ResourceKind::Job
            || self.expected_artifact_version == 0
            || self.expected_blob_version == 0
            || self.expected_operation_version == 0
            || self.fence.expected_version == 0
            || self.expected_operation_version != self.fence.expected_version
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
        {
            return Err(ArtifactWorkError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactScanDecision {
    pub artifact_state: ArtifactState,
    pub artifact_version: u64,
    pub blob_state: BlobIntegrityState,
    pub blob_version: u64,
    pub operation_state: JobState,
    pub operation_version: u64,
    pub metadata: ArtifactMetadataSnapshot,
    pub artifact_blob_id: ResourceId,
    pub duplicate_blob_cleanup: Option<ArtifactBlobCleanupSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactWorkerOperationRecord {
    pub tenant_id: ResourceId,
    pub operation_id: ResourceId,
    pub state: JobState,
    pub version: u64,
    pub scan_kind: ArtifactScanKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactScanWorkRecord {
    pub artifact: ArtifactRecord,
    pub blob: ArtifactBlobRecord,
    pub operation: ArtifactWorkerOperationRecord,
    pub scan_job_id: ResourceId,
    pub scan_job_state: JobState,
    pub scan_job_version: u64,
    pub scan: ArtifactScanJobSnapshot,
}

pub fn decide_commit_artifact_scan(
    artifact: &ArtifactRecord,
    blob: &ArtifactBlobRecord,
    operation: &ArtifactWorkerOperationRecord,
    reusable_blob: Option<&ArtifactBlobRecord>,
    job: &ArtifactScanJobSnapshot,
    command: &CommitArtifactScanOutcome,
    database_now: DateTime<Utc>,
) -> Result<ArtifactScanDecision, ArtifactWorkError> {
    command.validate_at(database_now)?;
    job.validate()?;
    if artifact.tenant_id != command.audit.tenant_id
        || blob.tenant_id != command.audit.tenant_id
        || operation.tenant_id != command.audit.tenant_id
        || artifact.artifact_id != command.artifact_id
        || artifact.blob_id.as_ref() != Some(&command.blob_id)
        || blob.blob_id != command.blob_id
        || operation.operation_id != command.operation_id
        || operation.scan_kind != job.scan_kind
        || operation.version != command.expected_operation_version
        || operation.state != JobState::Running
        || job.operation_id != command.operation_id
        || job.artifact_id != command.artifact_id
        || job.blob_id != command.blob_id
        || job.expected_artifact_version != command.expected_artifact_version
        || job.expected_blob_version != command.expected_blob_version
        || artifact.version != command.expected_artifact_version
        || blob.version != command.expected_blob_version
        || job.object_generation != command.evidence.object_generation
        || blob.object_generation.as_deref() != Some(job.object_generation.as_str())
        || job.scan_kind != command.evidence.scan_kind
        || job.scan_policy_revision != command.evidence.scan_policy_revision
        || job.scanner_contract_digest != command.evidence.scanner_contract_digest
        || job.ruleset_digest != command.evidence.ruleset_digest
        || command.evidence.observed_at > database_now
        || command.evidence.expires_at <= database_now
    {
        return Err(ArtifactWorkError::EvidenceMismatch);
    }
    let expected_expiry = command
        .evidence
        .observed_at
        .checked_add_signed(chrono::Duration::milliseconds(
            i64::try_from(job.evidence_ttl_milliseconds)
                .map_err(|_| ArtifactWorkError::InvalidJobPayload)?,
        ))
        .ok_or(ArtifactWorkError::InvalidEvidence)?;
    if command.evidence.expires_at != expected_expiry {
        return Err(ArtifactWorkError::EvidenceMismatch);
    }
    let matches_intent = artifact.expected_size_bytes == command.evidence.size_bytes
        && artifact
            .expected_digest
            .as_ref()
            .is_none_or(|digest| digest == &command.evidence.content_digest);
    if !matches_intent && command.evidence.disposition != ArtifactScanDisposition::Rejected {
        return Err(ArtifactWorkError::EvidenceMismatch);
    }
    let expected_blob_state = match job.scan_kind {
        ArtifactScanKind::Initial => BlobIntegrityState::Staging,
        ArtifactScanKind::Rescan => BlobIntegrityState::Verified,
    };
    if blob.state != expected_blob_state {
        return Err(ArtifactWorkError::InvalidTransition);
    }
    if job.scan_kind == ArtifactScanKind::Rescan {
        let matches_current = blob.content_digest.as_ref()
            == Some(&command.evidence.content_digest)
            && blob.size_bytes == Some(command.evidence.size_bytes)
            && artifact.verified_media_type.as_deref()
                == Some(command.evidence.verified_media_type.as_str());
        if !matches_current && command.evidence.disposition != ArtifactScanDisposition::Corrupt {
            return Err(ArtifactWorkError::EvidenceMismatch);
        }
    }
    let (artifact_state, operation_state) = match (job.scan_kind, command.evidence.disposition) {
        (ArtifactScanKind::Initial, ArtifactScanDisposition::Verified) => {
            (ArtifactState::Verified, JobState::Waiting)
        }
        (ArtifactScanKind::Initial, ArtifactScanDisposition::Quarantined)
        | (ArtifactScanKind::Initial, ArtifactScanDisposition::Corrupt) => {
            (ArtifactState::Quarantined, JobState::Waiting)
        }
        (ArtifactScanKind::Initial, ArtifactScanDisposition::Rejected) => {
            (ArtifactState::Rejected, JobState::Failed)
        }
        (ArtifactScanKind::Rescan, ArtifactScanDisposition::Verified) => {
            (ArtifactState::Ready, JobState::Succeeded)
        }
        (ArtifactScanKind::Rescan, ArtifactScanDisposition::Quarantined) => {
            (ArtifactState::Quarantined, JobState::Succeeded)
        }
        (ArtifactScanKind::Rescan, ArtifactScanDisposition::Rejected) => {
            (ArtifactState::Rejected, JobState::Succeeded)
        }
        (ArtifactScanKind::Rescan, ArtifactScanDisposition::Corrupt) => {
            (ArtifactState::Corrupt, JobState::Succeeded)
        }
    };
    let expected_state = match job.scan_kind {
        ArtifactScanKind::Initial => ArtifactState::Verifying,
        ArtifactScanKind::Rescan => ArtifactState::Quarantined,
    };
    if artifact.state != expected_state {
        return Err(ArtifactWorkError::InvalidTransition);
    }
    if !artifact.state.can_transition_to(artifact_state)
        || (operation_state != operation.state
            && !operation.state.can_transition_to(operation_state))
    {
        return Err(ArtifactWorkError::InvalidTransition);
    }
    let desired_blob_state = if job.scan_kind == ArtifactScanKind::Rescan
        && command.evidence.disposition == ArtifactScanDisposition::Corrupt
    {
        BlobIntegrityState::Corrupt
    } else {
        BlobIntegrityState::Verified
    };
    let (artifact_blob_id, blob_state, blob_version, duplicate_blob_cleanup) =
        if job.scan_kind == ArtifactScanKind::Initial {
            if let Some(reusable) = reusable_blob {
                validate_reusable_blob(blob, reusable, &command.evidence)?;
                let cleanup = ArtifactBlobCleanupSnapshot {
                    schema_version: 1,
                    artifact_id: artifact.artifact_id.clone(),
                    discarded_blob_id: blob.blob_id.clone(),
                    replacement_blob_id: reusable.blob_id.clone(),
                    object_generation: job.object_generation.clone(),
                    verification_evidence_digest: command.evidence.canonical_digest.clone(),
                    expected_blob_version: increment(blob.version)?,
                    retry_backoff_milliseconds: job.retry_backoff_milliseconds,
                };
                cleanup
                    .canonical_digest()
                    .map_err(|_| ArtifactWorkError::InvalidEvidence)?;
                (
                    reusable.blob_id.clone(),
                    BlobIntegrityState::Deleting,
                    increment(blob.version)?,
                    Some(cleanup),
                )
            } else {
                (
                    blob.blob_id.clone(),
                    desired_blob_state,
                    increment(blob.version)?,
                    None,
                )
            }
        } else {
            (
                blob.blob_id.clone(),
                desired_blob_state,
                if blob.state == desired_blob_state {
                    blob.version
                } else {
                    increment(blob.version)?
                },
                None,
            )
        };
    let metadata = artifact
        .metadata
        .with_current_verification(command.evidence.current())
        .map_err(|_| ArtifactWorkError::InvalidEvidence)?;
    Ok(ArtifactScanDecision {
        artifact_state,
        artifact_version: increment(artifact.version)?,
        blob_state,
        blob_version,
        operation_state,
        operation_version: increment(operation.version)?,
        metadata,
        artifact_blob_id,
        duplicate_blob_cleanup,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactBlobDeletionEvidence {
    pub schema_version: u32,
    pub object_generation: String,
    pub backend_receipt_digest: Sha256Digest,
    pub absence_evidence_digest: Sha256Digest,
    pub observed_at: DateTime<Utc>,
}

impl ArtifactBlobDeletionEvidence {
    pub fn validate(&self) -> Result<(), ArtifactWorkError> {
        if self.schema_version != 1 || !valid_object_generation(&self.object_generation) {
            return Err(ArtifactWorkError::InvalidEvidence);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitArtifactBlobCleanup {
    pub audit: ArtifactWorkerAudit,
    pub cleanup_job_id: ResourceId,
    pub fence: JobFence,
    pub discarded_blob_id: ResourceId,
    pub expected_blob_version: u64,
    pub evidence: ArtifactBlobDeletionEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedArtifactBlobCleanup {
    pub blob: ArtifactBlobRecord,
    pub cleanup_job_id: ResourceId,
    pub cleanup_job_state: JobState,
    pub cleanup_job_version: u64,
}

impl CommitArtifactBlobCleanup {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactWorkError> {
        self.audit.validate_at(now)?;
        self.evidence.validate()?;
        if self.cleanup_job_id.kind() != ResourceKind::Job
            || self.discarded_blob_id.kind() != ResourceKind::InternalBlob
            || self.expected_blob_version == 0
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
        {
            return Err(ArtifactWorkError::InvalidCommand);
        }
        Ok(())
    }
}

pub fn decide_commit_blob_cleanup(
    blob: &ArtifactBlobRecord,
    cleanup: &ArtifactBlobCleanupSnapshot,
    command: &CommitArtifactBlobCleanup,
    database_now: DateTime<Utc>,
) -> Result<(BlobIntegrityState, u64), ArtifactWorkError> {
    command.validate_at(database_now)?;
    cleanup
        .canonical_digest()
        .map_err(|_| ArtifactWorkError::InvalidJobPayload)?;
    if blob.tenant_id != command.audit.tenant_id
        || blob.blob_id != command.discarded_blob_id
        || cleanup.discarded_blob_id != command.discarded_blob_id
        || blob.version != command.expected_blob_version
        || cleanup.expected_blob_version != command.expected_blob_version
        || blob.state != BlobIntegrityState::Deleting
        || blob.object_generation.as_deref() != Some(command.evidence.object_generation.as_str())
        || cleanup.object_generation != command.evidence.object_generation
    {
        return Err(ArtifactWorkError::EvidenceMismatch);
    }
    Ok((BlobIntegrityState::Deleted, increment(blob.version)?))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactScanRequest {
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub fence: JobFence,
    pub job: ArtifactScanJobSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteArtifactBlobGeneration {
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub blob_id: ResourceId,
    pub object_generation: String,
    pub fence: JobFence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactBackendFailure {
    pub retryable: bool,
    pub reason_class: String,
}

impl ArtifactBackendFailure {
    pub fn validate(&self) -> Result<(), ArtifactWorkError> {
        if !valid_code(&self.reason_class, MAX_ARTIFACT_BACKEND_FAILURE_BYTES) {
            return Err(ArtifactWorkError::InvalidBackendFailure);
        }
        Ok(())
    }
}

impl fmt::Display for ArtifactBackendFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Artifact backend failed with reason class {} (retryable: {})",
            self.reason_class, self.retryable
        )
    }
}

impl Error for ArtifactBackendFailure {}

pub trait ArtifactScanner {
    async fn scan(
        &self,
        request: ArtifactScanRequest,
    ) -> Result<ArtifactScanEvidence, ArtifactBackendFailure>;
}

pub trait ArtifactBlobBackend {
    async fn delete_generation(
        &self,
        request: DeleteArtifactBlobGeneration,
    ) -> Result<ArtifactBlobDeletionEvidence, ArtifactBackendFailure>;
}

pub trait ArtifactWorkAuthority {
    type Error;

    async fn commit_attempt_failure(
        &self,
        command: CommitArtifactAttemptFailure,
    ) -> Result<CommandOutcome<()>, Self::Error>;

    async fn commit_scan_outcome(
        &self,
        command: CommitArtifactScanOutcome,
    ) -> Result<CommandOutcome<ArtifactScanWorkRecord>, Self::Error>;

    async fn commit_blob_cleanup(
        &self,
        command: CommitArtifactBlobCleanup,
    ) -> Result<CommandOutcome<CompletedArtifactBlobCleanup>, Self::Error>;

    async fn commit_deletion(
        &self,
        command: CompleteArtifactDeletion,
    ) -> Result<CommandOutcome<CompletedArtifactDeletion>, Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactScanExecution {
    pub audit: ArtifactWorkerAudit,
    pub scan_job_id: ResourceId,
    pub fence: JobFence,
    pub scan: ArtifactScanJobSnapshot,
    pub operation_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub expected_artifact_version: u64,
    pub expected_blob_version: u64,
    pub expected_operation_version: u64,
    pub duplicate_blob_cleanup_job_id: ResourceId,
}

impl ArtifactScanExecution {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactWorkError> {
        self.audit.validate_at(now)?;
        self.scan.validate()?;
        if self.scan_job_id.kind() != ResourceKind::Job
            || self.scan_job_id != self.operation_id
            || self.scan.operation_id != self.operation_id
            || self.scan.artifact_id != self.artifact_id
            || self.scan.blob_id != self.blob_id
            || self.scan.expected_artifact_version != self.expected_artifact_version
            || self.scan.expected_blob_version != self.expected_blob_version
            || self.operation_id.kind() != ResourceKind::Job
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.expected_operation_version == 0
            || self.duplicate_blob_cleanup_job_id.kind() != ResourceKind::Job
            || self.fence.expected_version == 0
            || self.expected_operation_version != self.fence.expected_version
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
        {
            return Err(ArtifactWorkError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactBlobCleanupExecution {
    pub audit: ArtifactWorkerAudit,
    pub cleanup_job_id: ResourceId,
    pub fence: JobFence,
    pub cleanup: ArtifactBlobCleanupSnapshot,
    pub expected_blob_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDeletionExecution {
    pub audit: ArtifactWorkerAudit,
    pub deletion_job_id: ResourceId,
    pub fence: JobFence,
    pub deletion: ArtifactDeletionJobSnapshot,
    pub expected_artifact_version: u64,
    pub expected_blob_version: u64,
    pub expected_operation_version: u64,
}

impl ArtifactDeletionExecution {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactWorkError> {
        self.audit.validate_at(now)?;
        self.deletion
            .canonical_digest()
            .map_err(|_| ArtifactWorkError::InvalidJobPayload)?;
        if self.deletion_job_id.kind() != ResourceKind::Job
            || self.deletion_job_id != self.deletion.operation_id
            || self.expected_artifact_version == 0
            || self.expected_blob_version == 0
            || self.expected_operation_version == 0
            || self.deletion.expected_artifact_version != self.expected_artifact_version
            || self.deletion.expected_blob_version != self.expected_blob_version
            || self.fence.expected_version == 0
            || self.expected_operation_version != self.fence.expected_version
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
        {
            return Err(ArtifactWorkError::InvalidCommand);
        }
        Ok(())
    }
}

impl ArtifactBlobCleanupExecution {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactWorkError> {
        self.audit.validate_at(now)?;
        self.cleanup
            .canonical_digest()
            .map_err(|_| ArtifactWorkError::InvalidJobPayload)?;
        if self.cleanup_job_id.kind() != ResourceKind::Job
            || self.expected_blob_version == 0
            || self.cleanup.expected_blob_version != self.expected_blob_version
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
        {
            return Err(ArtifactWorkError::InvalidCommand);
        }
        Ok(())
    }
}

pub struct ArtifactWorkerService<S, B, A> {
    scanner: S,
    blob_backend: B,
    authority: A,
}

impl<S, B, A> ArtifactWorkerService<S, B, A>
where
    S: ArtifactScanner,
    B: ArtifactBlobBackend,
    A: ArtifactWorkAuthority,
{
    pub const fn new(scanner: S, blob_backend: B, authority: A) -> Self {
        Self {
            scanner,
            blob_backend,
            authority,
        }
    }

    async fn commit_attempt_failure(
        &self,
        audit: &ArtifactWorkerAudit,
        job_id: &ResourceId,
        fence: &JobFence,
        failure: ArtifactBackendFailure,
    ) -> Result<(), ArtifactWorkerExecutionError<A::Error>> {
        failure
            .validate()
            .map_err(ArtifactWorkerExecutionError::Contract)?;
        self.authority
            .commit_attempt_failure(CommitArtifactAttemptFailure {
                audit: audit.clone(),
                job_id: job_id.clone(),
                fence: fence.clone(),
                failure,
            })
            .await
            .map_err(ArtifactWorkerExecutionError::Authority)?;
        Ok(())
    }

    pub async fn execute_scan(
        &self,
        execution: ArtifactScanExecution,
        now: DateTime<Utc>,
    ) -> Result<CommandOutcome<ArtifactScanWorkRecord>, ArtifactWorkerExecutionError<A::Error>>
    {
        execution
            .validate_at(now)
            .map_err(ArtifactWorkerExecutionError::Contract)?;
        let request = ArtifactScanRequest {
            tenant_id: execution.audit.tenant_id.clone(),
            job_id: execution.scan_job_id.clone(),
            fence: execution.fence.clone(),
            job: execution.scan.clone(),
        };
        let evidence = match self.scanner.scan(request).await {
            Ok(evidence) => evidence,
            Err(failure) => {
                self.commit_attempt_failure(
                    &execution.audit,
                    &execution.scan_job_id,
                    &execution.fence,
                    failure.clone(),
                )
                .await?;
                return Err(ArtifactWorkerExecutionError::Backend(failure));
            }
        };
        let command = CommitArtifactScanOutcome {
            audit: execution.audit,
            scan_job_id: execution.scan_job_id,
            fence: execution.fence,
            operation_id: execution.operation_id,
            artifact_id: execution.artifact_id,
            blob_id: execution.blob_id,
            expected_artifact_version: execution.expected_artifact_version,
            expected_blob_version: execution.expected_blob_version,
            expected_operation_version: execution.expected_operation_version,
            evidence,
            duplicate_blob_cleanup_job_id: execution.duplicate_blob_cleanup_job_id,
        };
        if let Err(failure) = command.validate_at(now) {
            self.commit_attempt_failure(
                &command.audit,
                &command.scan_job_id,
                &command.fence,
                ArtifactBackendFailure {
                    retryable: false,
                    reason_class: "invalid_scanner_evidence".to_owned(),
                },
            )
            .await?;
            return Err(ArtifactWorkerExecutionError::Contract(failure));
        }
        self.authority
            .commit_scan_outcome(command)
            .await
            .map_err(ArtifactWorkerExecutionError::Authority)
    }

    pub async fn execute_blob_cleanup(
        &self,
        execution: ArtifactBlobCleanupExecution,
        now: DateTime<Utc>,
    ) -> Result<CommandOutcome<CompletedArtifactBlobCleanup>, ArtifactWorkerExecutionError<A::Error>>
    {
        execution
            .validate_at(now)
            .map_err(ArtifactWorkerExecutionError::Contract)?;
        let request = DeleteArtifactBlobGeneration {
            tenant_id: execution.audit.tenant_id.clone(),
            job_id: execution.cleanup_job_id.clone(),
            blob_id: execution.cleanup.discarded_blob_id.clone(),
            object_generation: execution.cleanup.object_generation.clone(),
            fence: execution.fence.clone(),
        };
        let evidence = match self.blob_backend.delete_generation(request).await {
            Ok(evidence) => evidence,
            Err(failure) => {
                self.commit_attempt_failure(
                    &execution.audit,
                    &execution.cleanup_job_id,
                    &execution.fence,
                    failure.clone(),
                )
                .await?;
                return Err(ArtifactWorkerExecutionError::Backend(failure));
            }
        };
        let evidence_failure = evidence.validate().err().or_else(|| {
            (evidence.object_generation != execution.cleanup.object_generation)
                .then_some(ArtifactWorkError::EvidenceMismatch)
        });
        if let Some(failure) = evidence_failure {
            self.commit_attempt_failure(
                &execution.audit,
                &execution.cleanup_job_id,
                &execution.fence,
                ArtifactBackendFailure {
                    retryable: false,
                    reason_class: "invalid_blob_deletion_evidence".to_owned(),
                },
            )
            .await?;
            return Err(ArtifactWorkerExecutionError::Contract(failure));
        }
        let command = CommitArtifactBlobCleanup {
            audit: execution.audit,
            cleanup_job_id: execution.cleanup_job_id,
            fence: execution.fence,
            discarded_blob_id: execution.cleanup.discarded_blob_id,
            expected_blob_version: execution.expected_blob_version,
            evidence,
        };
        command
            .validate_at(now)
            .map_err(ArtifactWorkerExecutionError::Contract)?;
        self.authority
            .commit_blob_cleanup(command)
            .await
            .map_err(ArtifactWorkerExecutionError::Authority)
    }

    pub async fn execute_deletion(
        &self,
        execution: ArtifactDeletionExecution,
        now: DateTime<Utc>,
    ) -> Result<CommandOutcome<CompletedArtifactDeletion>, ArtifactWorkerExecutionError<A::Error>>
    {
        execution
            .validate_at(now)
            .map_err(ArtifactWorkerExecutionError::Contract)?;
        let evidence = match &execution.deletion.mode {
            ArtifactDeletionMode::ArtifactOnly {
                alias_artifact_id,
                alias_artifact_version,
            } => ArtifactDeletionEvidence::ArtifactOnly {
                alias_artifact_id: alias_artifact_id.clone(),
                alias_artifact_version: *alias_artifact_version,
            },
            ArtifactDeletionMode::BlobGeneration { object_generation } => {
                let request = DeleteArtifactBlobGeneration {
                    tenant_id: execution.audit.tenant_id.clone(),
                    job_id: execution.deletion_job_id.clone(),
                    blob_id: execution.deletion.blob_id.clone(),
                    object_generation: object_generation.clone(),
                    fence: execution.fence.clone(),
                };
                let backend_evidence = match self.blob_backend.delete_generation(request).await {
                    Ok(evidence) => evidence,
                    Err(failure) => {
                        self.commit_attempt_failure(
                            &execution.audit,
                            &execution.deletion_job_id,
                            &execution.fence,
                            failure.clone(),
                        )
                        .await?;
                        return Err(ArtifactWorkerExecutionError::Backend(failure));
                    }
                };
                let evidence_failure = backend_evidence.validate().err().or_else(|| {
                    (backend_evidence.object_generation != *object_generation)
                        .then_some(ArtifactWorkError::EvidenceMismatch)
                });
                if let Some(failure) = evidence_failure {
                    self.commit_attempt_failure(
                        &execution.audit,
                        &execution.deletion_job_id,
                        &execution.fence,
                        ArtifactBackendFailure {
                            retryable: false,
                            reason_class: "invalid_blob_deletion_evidence".to_owned(),
                        },
                    )
                    .await?;
                    return Err(ArtifactWorkerExecutionError::Contract(failure));
                }
                ArtifactDeletionEvidence::BlobGeneration {
                    object_generation: backend_evidence.object_generation,
                    backend_receipt_digest: backend_evidence.backend_receipt_digest,
                    absence_evidence_digest: backend_evidence.absence_evidence_digest,
                }
            }
        };
        let command = CompleteArtifactDeletion {
            audit: execution.audit,
            deletion_operation_id: execution.deletion.operation_id,
            deletion_job_id: execution.deletion_job_id,
            artifact_id: execution.deletion.artifact_id,
            blob_id: execution.deletion.blob_id,
            expected_artifact_version: execution.expected_artifact_version,
            expected_blob_version: execution.expected_blob_version,
            expected_operation_version: execution.expected_operation_version,
            fence: execution.fence,
            evidence,
        };
        command.validate_at(now).map_err(|_| {
            ArtifactWorkerExecutionError::Contract(ArtifactWorkError::InvalidCommand)
        })?;
        self.authority
            .commit_deletion(command)
            .await
            .map_err(ArtifactWorkerExecutionError::Authority)
    }
}

#[derive(Debug)]
pub enum ArtifactWorkerExecutionError<E> {
    Contract(ArtifactWorkError),
    Backend(ArtifactBackendFailure),
    Authority(E),
}

impl<E: fmt::Display> fmt::Display for ArtifactWorkerExecutionError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(failure) => {
                write!(formatter, "Artifact worker contract failed: {failure}")
            }
            Self::Backend(failure) => {
                write!(formatter, "Artifact worker backend failed: {failure}")
            }
            Self::Authority(failure) => {
                write!(formatter, "Artifact worker authority failed: {failure}")
            }
        }
    }
}

impl<E: Error + 'static> Error for ArtifactWorkerExecutionError<E> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactWorkError {
    InvalidAudit,
    InvalidJobPayload,
    InvalidCommand,
    InvalidEvidence,
    EvidenceMismatch,
    InvalidTransition,
    InvalidBackendFailure,
    CounterOverflow,
    Canonicalization,
}

impl fmt::Display for ArtifactWorkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidAudit => "Artifact worker audit is invalid",
            Self::InvalidJobPayload => "Artifact Job payload is invalid",
            Self::InvalidCommand => "Artifact worker command is invalid",
            Self::InvalidEvidence => "Artifact backend evidence is invalid",
            Self::EvidenceMismatch => "Artifact backend evidence does not match current work",
            Self::InvalidTransition => "Artifact work transition is invalid",
            Self::InvalidBackendFailure => "Artifact backend failure is invalid",
            Self::CounterOverflow => "Artifact work counter overflow",
            Self::Canonicalization => "Artifact work canonicalization failed",
        })
    }
}

impl Error for ArtifactWorkError {}

fn require_owner(
    owner_id: &ResourceId,
    owner_kind: ResourceKind,
    expected: &ResourceId,
) -> Result<(), ArtifactWorkError> {
    if owner_id.kind() != owner_kind || owner_id != expected {
        return Err(ArtifactWorkError::InvalidJobPayload);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_scan_schedule(
    job_id: &ResourceId,
    operation_id: &ResourceId,
    artifact_id: &ResourceId,
    blob_id: &ResourceId,
    expected_artifact_version: u64,
    expected_blob_version: u64,
    expected_operation_version: u64,
    scan_policy_revision: &ExactVersionRef,
    evidence_ttl_milliseconds: u64,
    retry_backoff_milliseconds: u64,
    deadline: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<(), ArtifactWorkError> {
    if job_id.kind() != ResourceKind::Job
        || operation_id.kind() != ResourceKind::Job
        || job_id != operation_id
        || artifact_id.kind() != ResourceKind::Artifact
        || blob_id.kind() != ResourceKind::InternalBlob
        || expected_artifact_version == 0
        || expected_blob_version == 0
        || expected_operation_version == 0
        || scan_policy_revision.resource_kind != ResourceKind::PolicyRevision
        || scan_policy_revision.validate().is_err()
        || evidence_ttl_milliseconds == 0
        || evidence_ttl_milliseconds > MAX_ARTIFACT_EVIDENCE_TTL_MILLISECONDS
        || retry_backoff_milliseconds == 0
        || retry_backoff_milliseconds > MAX_ARTIFACT_RETRY_BACKOFF_MILLISECONDS
        || deadline <= now
    {
        return Err(ArtifactWorkError::InvalidCommand);
    }
    Ok(())
}

fn validate_reusable_blob(
    candidate: &ArtifactBlobRecord,
    reusable: &ArtifactBlobRecord,
    evidence: &ArtifactScanEvidence,
) -> Result<(), ArtifactWorkError> {
    if reusable.tenant_id != candidate.tenant_id
        || reusable.blob_id == candidate.blob_id
        || reusable.backend != candidate.backend
        || reusable.storage_binding_digest != candidate.storage_binding_digest
        || reusable.security_domain_digest != candidate.security_domain_digest
        || reusable.encryption_domain_id != candidate.encryption_domain_id
        || reusable.state != BlobIntegrityState::Verified
        || reusable.content_digest.as_ref() != Some(&evidence.content_digest)
        || reusable.size_bytes != Some(evidence.size_bytes)
        || reusable.object_generation.is_none()
    {
        return Err(ArtifactWorkError::EvidenceMismatch);
    }
    Ok(())
}

fn increment(value: u64) -> Result<u64, ArtifactWorkError> {
    value
        .checked_add(1)
        .ok_or(ArtifactWorkError::CounterOverflow)
}

fn valid_object_generation(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ARTIFACT_OBJECT_GENERATION_BYTES
        && !value.chars().any(char::is_control)
}

fn valid_media_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value.is_ascii()
        && !value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b' ')
        && value.split_once('/').is_some_and(|(kind, subtype)| {
            !kind.is_empty() && !subtype.is_empty() && !subtype.contains('/')
        })
}

fn raw_digest(bytes: &[u8]) -> Sha256Digest {
    let mut value = String::with_capacity(71);
    value.push_str("sha256:");
    for byte in Sha256::digest(bytes) {
        write!(&mut value, "{byte:02x}").expect("writing to a String cannot fail");
    }
    value.parse().expect("SHA-256 encoding is canonical")
}

fn valid_code(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'_' | b'.' | b':' | b'-')
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ArtifactBlobCleanupSnapshot, ArtifactMetadataSnapshot, ArtifactUploadOperationSnapshot,
    };
    use chrono::Duration;
    use insight_platform_contracts::{
        ArtifactPurpose, CommandAudit, DataClassification, ExactVersionRef, PrincipalKind,
        WorkClass,
    };
    use insight_platform_jobs::{JobLease, JobOwnerRef};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct WorkerSpy {
        calls: Mutex<Vec<&'static str>>,
        scan_request: Mutex<Option<ArtifactScanRequest>>,
        delete_requests: Mutex<Vec<DeleteArtifactBlobGeneration>>,
        failure_command: Mutex<Option<CommitArtifactAttemptFailure>>,
        scan_command: Mutex<Option<CommitArtifactScanOutcome>>,
        cleanup_command: Mutex<Option<CommitArtifactBlobCleanup>>,
        deletion_command: Mutex<Option<CompleteArtifactDeletion>>,
    }

    struct MockScanner {
        spy: Arc<WorkerSpy>,
        outcome: Result<ArtifactScanEvidence, ArtifactBackendFailure>,
    }

    impl ArtifactScanner for MockScanner {
        async fn scan(
            &self,
            request: ArtifactScanRequest,
        ) -> Result<ArtifactScanEvidence, ArtifactBackendFailure> {
            self.spy.calls.lock().unwrap().push("scan_backend");
            *self.spy.scan_request.lock().unwrap() = Some(request);
            self.outcome.clone()
        }
    }

    struct MockBlobBackend {
        spy: Arc<WorkerSpy>,
        outcome: Result<ArtifactBlobDeletionEvidence, ArtifactBackendFailure>,
    }

    impl ArtifactBlobBackend for MockBlobBackend {
        async fn delete_generation(
            &self,
            request: DeleteArtifactBlobGeneration,
        ) -> Result<ArtifactBlobDeletionEvidence, ArtifactBackendFailure> {
            self.spy.calls.lock().unwrap().push("blob_backend");
            self.spy.delete_requests.lock().unwrap().push(request);
            self.outcome.clone()
        }
    }

    struct MockAuthority {
        spy: Arc<WorkerSpy>,
    }

    impl ArtifactWorkAuthority for MockAuthority {
        type Error = &'static str;

        async fn commit_attempt_failure(
            &self,
            command: CommitArtifactAttemptFailure,
        ) -> Result<CommandOutcome<()>, Self::Error> {
            self.spy.calls.lock().unwrap().push("failure_authority");
            *self.spy.failure_command.lock().unwrap() = Some(command);
            Ok(CommandOutcome::Applied(()))
        }

        async fn commit_scan_outcome(
            &self,
            command: CommitArtifactScanOutcome,
        ) -> Result<CommandOutcome<ArtifactScanWorkRecord>, Self::Error> {
            self.spy.calls.lock().unwrap().push("scan_authority");
            *self.spy.scan_command.lock().unwrap() = Some(command);
            Err("authority_stop")
        }

        async fn commit_blob_cleanup(
            &self,
            command: CommitArtifactBlobCleanup,
        ) -> Result<CommandOutcome<CompletedArtifactBlobCleanup>, Self::Error> {
            self.spy.calls.lock().unwrap().push("cleanup_authority");
            *self.spy.cleanup_command.lock().unwrap() = Some(command);
            Err("authority_stop")
        }

        async fn commit_deletion(
            &self,
            command: CompleteArtifactDeletion,
        ) -> Result<CommandOutcome<CompletedArtifactDeletion>, Self::Error> {
            self.spy.calls.lock().unwrap().push("deletion_authority");
            *self.spy.deletion_command.lock().unwrap() = Some(command);
            Err("authority_stop")
        }
    }

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f5{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(label: &str) -> Sha256Digest {
        canonical_digest(&json!({"artifact_work": label}))
            .unwrap()
            .parse()
            .unwrap()
    }

    fn version(kind: ResourceKind, suffix: u16) -> ExactVersionRef {
        ExactVersionRef::new(id(kind, suffix), digest(&format!("version-{suffix}"))).unwrap()
    }

    fn command_audit(tenant_id: &ResourceId, base: u16, now: DateTime<Utc>) -> CommandAudit {
        CommandAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: tenant_id.clone(),
            principal_id: id(ResourceKind::Principal, base),
            principal_kind: PrincipalKind::TenantAdmin,
            receipt_id: id(ResourceKind::Receipt, base + 1),
            event_id: id(ResourceKind::Event, base + 2),
            outbox_id: id(ResourceKind::OutboxEvent, base + 3),
            idempotency_key_digest: digest("schedule-idempotency"),
            request_digest: digest("schedule-request"),
            receipt_expires_at: now + Duration::hours(1),
        }
    }

    #[test]
    fn awaiting_stage_payload_is_internal_owner_bound_and_not_recoverable() {
        let now = Utc::now();
        let artifact_id = id(ResourceKind::Artifact, 0x810);
        let mut payload = ArtifactJobPayload::AwaitingStage {
            stage: ArtifactAwaitingStageSnapshot {
                schema_version: 1,
                producer_job_id: id(ResourceKind::Job, 0x811),
                artifact_id: artifact_id.clone(),
                blob_id: id(ResourceKind::InternalBlob, 0x812),
                quota_account_id: id(ResourceKind::QuotaAccount, 0x813),
                quota_entry_id: id(ResourceKind::QuotaLedgerEntry, 0x814),
                purpose: ArtifactPurpose::McpResource,
                classification: DataClassification::Internal,
                maximum_bytes: 1_048_576,
                declared_media_type: "application/vnd.insight.mcp-discovery+json".to_owned(),
                retention_policy_revision: version(ResourceKind::PolicyRevision, 0x815),
                artifact_io_policy_revision: version(ResourceKind::PolicyRevision, 0x816),
                scanner_contract_digest: digest("scanner"),
                ruleset_digest: digest("rules"),
                evidence_ttl_milliseconds: 60_000,
                retry_backoff_milliseconds: 1_000,
                write_storage_binding_digest: digest("storage-binding"),
                encryption_domain_id: id(ResourceKind::EncryptionDomain, 0x821),
                retain_until: now + Duration::hours(2),
                deadline: now + Duration::hours(1),
            },
        };
        assert!(payload.validate_for_owner(&artifact_id).is_ok());
        assert!(payload
            .validate_for_owner(&id(ResourceKind::Artifact, 0x817))
            .is_err());
        assert_eq!(payload.kind_name(), "awaiting_stage");
        assert!(!payload.may_have_uncertain_physical_effect());
        let ArtifactJobPayload::AwaitingStage { stage } = &mut payload else {
            unreachable!("fixture is an awaiting-stage payload");
        };
        stage.retain_until = stage.deadline;
        let stage = stage.clone();
        assert!(payload.validate_for_owner(&artifact_id).is_ok());

        let verification_job_id = id(ResourceKind::Job, 0x818);
        let command = StageWorkloadArtifact {
            schema_version: 1,
            tenant_id: id(ResourceKind::Tenant, 0x819),
            caller: insight_platform_contracts::ArtifactWorkloadAudience::McpHost,
            producer_job_id: stage.producer_job_id.clone(),
            producer_fence: JobFence {
                expected_version: 3,
                worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 0x820),
                lease_generation: 2,
                token_digest: digest("producer-fence"),
            },
            verification_job_id: verification_job_id.clone(),
            artifact_id: stage.artifact_id.clone(),
            blob_id: stage.blob_id.clone(),
            content_digest: digest("descriptor-bytes"),
            size_bytes: 4_096,
            media_type: stage.declared_media_type.clone(),
            storage_backend: "s3".to_owned(),
            storage_binding_digest: digest("storage-binding"),
            object_reference_ciphertext: vec![1, 2, 3],
            object_generation: "generation-1".to_owned(),
            key_id: "artifact-kek-v1".to_owned(),
            encryption_domain_id: id(ResourceKind::EncryptionDomain, 0x821),
            backend_evidence_digest: digest("backend-evidence"),
            staged_at: now,
        };
        assert!(command.validate_for(&stage, now).is_ok());
        let mut wrong_binding = command.clone();
        wrong_binding.storage_binding_digest = digest("other-storage-binding");
        assert_eq!(
            wrong_binding.validate_for(&stage, now),
            Err(ArtifactWorkError::InvalidCommand)
        );
        let mut wrong_encryption_domain = command.clone();
        wrong_encryption_domain.encryption_domain_id = id(ResourceKind::EncryptionDomain, 0x824);
        assert_eq!(
            wrong_encryption_domain.validate_for(&stage, now),
            Err(ArtifactWorkError::InvalidCommand)
        );
        let scan = command.scan_payload(&stage, 2, 1, 2).unwrap();
        assert!(scan.validate_for_owner(&artifact_id).is_ok());
        let ArtifactJobPayload::Scan { scan } = scan else {
            unreachable!("stage creates an initial scan payload");
        };
        assert_eq!(scan.operation_id, verification_job_id);
        assert_eq!(scan.object_generation, "generation-1");

        let mut oversized = command;
        oversized.size_bytes = stage.maximum_bytes + 1;
        assert_eq!(
            oversized.validate_for(&stage, now),
            Err(ArtifactWorkError::InvalidCommand)
        );

        let descriptor_bytes = br#"{"objects":[]}"#.to_vec();
        let mut request = StageWorkloadArtifactRequest {
            schema_version: 1,
            tenant_id: id(ResourceKind::Tenant, 0x822),
            producer_job_id: stage.producer_job_id.clone(),
            producer_fence: oversized.producer_fence.clone(),
            verification_job_id: id(ResourceKind::Job, 0x823),
            artifact_id: stage.artifact_id.clone(),
            blob_id: stage.blob_id.clone(),
            descriptor_digest: raw_digest(&descriptor_bytes),
            descriptor_bytes,
            media_type: stage.declared_media_type.clone(),
        };
        assert!(request.validate().is_ok());
        assert!(request.validate_for(&stage, now).is_ok());
        let authorized = AuthorizedWorkloadArtifactStage {
            tenant_id: request.tenant_id.clone(),
            producer_job_id: request.producer_job_id.clone(),
            verification_job_id: request.verification_job_id.clone(),
            artifact_id: request.artifact_id.clone(),
            blob_id: request.blob_id.clone(),
            descriptor_digest: request.descriptor_digest.clone(),
            size_bytes: u64::try_from(request.descriptor_bytes.len()).unwrap(),
            media_type: request.media_type.clone(),
            write_storage_binding_digest: stage.write_storage_binding_digest.clone(),
            encryption_domain_id: stage.encryption_domain_id.clone(),
            deadline: stage.deadline,
        };
        assert!(authorized.validate_for(&request, now).is_ok());
        let mut swapped_authorization = authorized;
        swapped_authorization.artifact_id = id(ResourceKind::Artifact, 0x825);
        assert_eq!(
            swapped_authorization.validate_for(&request, now),
            Err(ArtifactWorkError::InvalidEvidence)
        );
        request.descriptor_bytes.push(b' ');
        assert_eq!(request.validate(), Err(ArtifactWorkError::InvalidCommand));
    }

    struct Fixture {
        now: DateTime<Utc>,
        artifact: ArtifactRecord,
        blob: ArtifactBlobRecord,
        operation: ArtifactWorkerOperationRecord,
        job: ArtifactScanJobSnapshot,
        command: CommitArtifactScanOutcome,
    }

    fn fixture(scan_kind: ArtifactScanKind) -> Fixture {
        let now = Utc::now();
        let tenant_id = id(ResourceKind::Tenant, 1);
        let worker_id = id(ResourceKind::WorkerProcessGeneration, 2);
        let operation_id = id(ResourceKind::Job, 3);
        let artifact_id = id(ResourceKind::Artifact, 4);
        let blob_id = id(ResourceKind::InternalBlob, 5);
        let scan_job_id = operation_id.clone();
        let artifact = ArtifactRecord {
            tenant_id: tenant_id.clone(),
            artifact_id: artifact_id.clone(),
            blob_id: Some(blob_id.clone()),
            purpose: ArtifactPurpose::RunInput,
            classification: DataClassification::Internal,
            expected_size_bytes: 16,
            expected_digest: Some(digest("content")),
            declared_media_type: Some("application/json".to_owned()),
            verified_media_type: (scan_kind == ArtifactScanKind::Rescan)
                .then(|| "application/json".to_owned()),
            state: match scan_kind {
                ArtifactScanKind::Initial => ArtifactState::Verifying,
                ArtifactScanKind::Rescan => ArtifactState::Quarantined,
            },
            version: 4,
            metadata: ArtifactMetadataSnapshot::new(
                Some("input.json".to_owned()),
                operation_id.clone(),
            )
            .unwrap(),
            retention_policy_revision_id: id(ResourceKind::PolicyRevision, 7),
            retain_until: now + Duration::days(1),
            created_by: id(ResourceKind::Principal, 8),
            created_at: now - Duration::minutes(1),
            updated_at: now - Duration::seconds(1),
            terminal_at: None,
        };
        let blob = ArtifactBlobRecord {
            tenant_id: tenant_id.clone(),
            blob_id: blob_id.clone(),
            backend: "s3".to_owned(),
            storage_binding_digest: digest("storage"),
            security_domain_digest: digest("security-domain"),
            object_generation: Some("object-generation-1".to_owned()),
            encryption_domain_id: id(ResourceKind::EncryptionDomain, 9),
            content_digest: (scan_kind == ArtifactScanKind::Rescan).then(|| digest("content")),
            size_bytes: (scan_kind == ArtifactScanKind::Rescan).then_some(16),
            state: match scan_kind {
                ArtifactScanKind::Initial => BlobIntegrityState::Staging,
                ArtifactScanKind::Rescan => BlobIntegrityState::Verified,
            },
            version: 3,
        };
        let scan_policy_revision = version(ResourceKind::PolicyRevision, 10);
        let scanner_contract_digest = digest("scanner-contract");
        let ruleset_digest = digest("ruleset");
        let job = ArtifactScanJobSnapshot {
            schema_version: 2,
            scan_kind,
            operation_id: operation_id.clone(),
            producer_job_id: None,
            artifact_id: artifact_id.clone(),
            blob_id: blob_id.clone(),
            expected_artifact_version: artifact.version,
            expected_blob_version: blob.version,
            expected_operation_version: 3,
            object_generation: "object-generation-1".to_owned(),
            scan_policy_revision: scan_policy_revision.clone(),
            scanner_contract_digest: scanner_contract_digest.clone(),
            ruleset_digest: ruleset_digest.clone(),
            evidence_ttl_milliseconds: 60_000,
            retry_backoff_milliseconds: 100,
        };
        let evidence = ArtifactScanEvidenceDraft {
            schema_version: 1,
            scan_kind,
            scan_job_id: scan_job_id.clone(),
            scan_policy_revision,
            scanner_contract_digest,
            ruleset_digest,
            object_generation: "object-generation-1".to_owned(),
            content_digest: digest("content"),
            size_bytes: 16,
            verified_media_type: "application/json".to_owned(),
            disposition: ArtifactScanDisposition::Verified,
            reason_class: None,
            observed_at: now - Duration::seconds(1),
            expires_at: now - Duration::seconds(1) + Duration::milliseconds(60_000),
        }
        .seal()
        .unwrap();
        let audit = ArtifactWorkerAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id,
            worker_process_generation_id: worker_id.clone(),
            receipt_id: id(ResourceKind::Receipt, 11),
            event_id: id(ResourceKind::Event, 12),
            outbox_id: id(ResourceKind::OutboxEvent, 13),
            idempotency_key_digest: digest("idempotency"),
            request_digest: digest("request"),
            receipt_expires_at: now + Duration::hours(1),
        };
        let command = CommitArtifactScanOutcome {
            audit,
            scan_job_id,
            fence: JobFence {
                expected_version: 3,
                worker_process_generation_id: worker_id,
                lease_generation: 1,
                token_digest: digest("lease-token"),
            },
            operation_id,
            artifact_id,
            blob_id,
            expected_artifact_version: artifact.version,
            expected_blob_version: blob.version,
            expected_operation_version: 3,
            evidence,
            duplicate_blob_cleanup_job_id: id(ResourceKind::Job, 14),
        };
        Fixture {
            now,
            artifact,
            blob,
            operation: ArtifactWorkerOperationRecord {
                tenant_id: command.audit.tenant_id.clone(),
                operation_id: command.operation_id.clone(),
                state: JobState::Running,
                version: command.expected_operation_version,
                scan_kind,
            },
            job,
            command,
        }
    }

    fn scan_execution(fixture: &Fixture) -> ArtifactScanExecution {
        ArtifactScanExecution {
            audit: fixture.command.audit.clone(),
            scan_job_id: fixture.command.scan_job_id.clone(),
            fence: fixture.command.fence.clone(),
            scan: fixture.job.clone(),
            operation_id: fixture.command.operation_id.clone(),
            artifact_id: fixture.command.artifact_id.clone(),
            blob_id: fixture.command.blob_id.clone(),
            expected_artifact_version: fixture.command.expected_artifact_version,
            expected_blob_version: fixture.command.expected_blob_version,
            expected_operation_version: fixture.command.expected_operation_version,
            duplicate_blob_cleanup_job_id: fixture.command.duplicate_blob_cleanup_job_id.clone(),
        }
    }

    fn cleanup_execution(
        now: DateTime<Utc>,
    ) -> (ArtifactBlobCleanupExecution, CommitArtifactBlobCleanup) {
        let tenant_id = id(ResourceKind::Tenant, 80);
        let worker_id = id(ResourceKind::WorkerProcessGeneration, 81);
        let cleanup_job_id = id(ResourceKind::Job, 82);
        let discarded_blob_id = id(ResourceKind::InternalBlob, 83);
        let cleanup = ArtifactBlobCleanupSnapshot {
            schema_version: 1,
            artifact_id: id(ResourceKind::Artifact, 84),
            discarded_blob_id: discarded_blob_id.clone(),
            replacement_blob_id: id(ResourceKind::InternalBlob, 85),
            object_generation: "object-generation-cleanup".to_owned(),
            verification_evidence_digest: digest("cleanup-verification"),
            expected_blob_version: 7,
            retry_backoff_milliseconds: 100,
        };
        let audit = ArtifactWorkerAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id,
            worker_process_generation_id: worker_id.clone(),
            receipt_id: id(ResourceKind::Receipt, 86),
            event_id: id(ResourceKind::Event, 87),
            outbox_id: id(ResourceKind::OutboxEvent, 88),
            idempotency_key_digest: digest("cleanup-idempotency"),
            request_digest: digest("cleanup-request"),
            receipt_expires_at: now + Duration::hours(1),
        };
        let fence = JobFence {
            expected_version: 3,
            worker_process_generation_id: worker_id,
            lease_generation: 1,
            token_digest: digest("cleanup-lease-token"),
        };
        let evidence = ArtifactBlobDeletionEvidence {
            schema_version: 1,
            object_generation: cleanup.object_generation.clone(),
            backend_receipt_digest: digest("cleanup-backend-receipt"),
            absence_evidence_digest: digest("cleanup-absence"),
            observed_at: now,
        };
        let execution = ArtifactBlobCleanupExecution {
            audit: audit.clone(),
            cleanup_job_id: cleanup_job_id.clone(),
            fence: fence.clone(),
            cleanup,
            expected_blob_version: 7,
        };
        let command = CommitArtifactBlobCleanup {
            audit,
            cleanup_job_id,
            fence,
            discarded_blob_id,
            expected_blob_version: 7,
            evidence,
        };
        (execution, command)
    }

    fn deletion_execution(
        now: DateTime<Utc>,
        mode: ArtifactDeletionMode,
        backend_evidence: &ArtifactBlobDeletionEvidence,
    ) -> (ArtifactDeletionExecution, CompleteArtifactDeletion) {
        let tenant_id = id(ResourceKind::Tenant, 90);
        let worker_id = id(ResourceKind::WorkerProcessGeneration, 91);
        let deletion_operation_id = id(ResourceKind::Job, 92);
        let deletion_job_id = deletion_operation_id.clone();
        let deletion = ArtifactDeletionJobSnapshot {
            schema_version: 1,
            operation_id: deletion_operation_id,
            artifact_id: id(ResourceKind::Artifact, 94),
            blob_id: id(ResourceKind::InternalBlob, 95),
            mode: mode.clone(),
            expected_artifact_version: 6,
            expected_blob_version: 4,
            expected_operation_version: 3,
            retry_backoff_milliseconds: 100,
        };
        let audit = ArtifactWorkerAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id,
            worker_process_generation_id: worker_id.clone(),
            receipt_id: id(ResourceKind::Receipt, 96),
            event_id: id(ResourceKind::Event, 97),
            outbox_id: id(ResourceKind::OutboxEvent, 98),
            idempotency_key_digest: digest("deletion-idempotency"),
            request_digest: digest("deletion-request"),
            receipt_expires_at: now + Duration::hours(1),
        };
        let fence = JobFence {
            expected_version: 3,
            worker_process_generation_id: worker_id,
            lease_generation: 1,
            token_digest: digest("deletion-lease-token"),
        };
        let evidence = match mode {
            ArtifactDeletionMode::ArtifactOnly {
                alias_artifact_id,
                alias_artifact_version,
            } => ArtifactDeletionEvidence::ArtifactOnly {
                alias_artifact_id,
                alias_artifact_version,
            },
            ArtifactDeletionMode::BlobGeneration { object_generation } => {
                ArtifactDeletionEvidence::BlobGeneration {
                    object_generation,
                    backend_receipt_digest: backend_evidence.backend_receipt_digest.clone(),
                    absence_evidence_digest: backend_evidence.absence_evidence_digest.clone(),
                }
            }
        };
        let execution = ArtifactDeletionExecution {
            audit: audit.clone(),
            deletion_job_id: deletion_job_id.clone(),
            fence: fence.clone(),
            deletion: deletion.clone(),
            expected_artifact_version: 6,
            expected_blob_version: 4,
            expected_operation_version: 3,
        };
        let command = CompleteArtifactDeletion {
            audit,
            deletion_operation_id: deletion.operation_id,
            deletion_job_id,
            artifact_id: deletion.artifact_id,
            blob_id: deletion.blob_id,
            expected_artifact_version: 6,
            expected_blob_version: 4,
            expected_operation_version: 3,
            fence,
            evidence,
        };
        (execution, command)
    }

    #[tokio::test]
    async fn worker_scans_before_forwarding_exact_fenced_command() {
        let fixture = fixture(ArtifactScanKind::Initial);
        let expected = fixture.command.clone();
        let spy = Arc::new(WorkerSpy::default());
        let service = ArtifactWorkerService::new(
            MockScanner {
                spy: Arc::clone(&spy),
                outcome: Ok(expected.evidence.clone()),
            },
            MockBlobBackend {
                spy: Arc::clone(&spy),
                outcome: Err(ArtifactBackendFailure {
                    retryable: false,
                    reason_class: "unused".to_owned(),
                }),
            },
            MockAuthority {
                spy: Arc::clone(&spy),
            },
        );

        let failure = service
            .execute_scan(scan_execution(&fixture), fixture.now)
            .await
            .unwrap_err();
        assert!(matches!(
            failure,
            ArtifactWorkerExecutionError::Authority("authority_stop")
        ));
        assert_eq!(
            *spy.calls.lock().unwrap(),
            vec!["scan_backend", "scan_authority"]
        );
        assert_eq!(
            spy.scan_request.lock().unwrap().as_ref(),
            Some(&ArtifactScanRequest {
                tenant_id: expected.audit.tenant_id.clone(),
                job_id: expected.scan_job_id.clone(),
                fence: expected.fence.clone(),
                job: fixture.job.clone(),
            })
        );
        assert_eq!(spy.scan_command.lock().unwrap().as_ref(), Some(&expected));
    }

    #[tokio::test]
    async fn worker_deletes_generation_before_forwarding_exact_cleanup_command() {
        let now = Utc::now();
        let (execution, expected) = cleanup_execution(now);
        let spy = Arc::new(WorkerSpy::default());
        let service = ArtifactWorkerService::new(
            MockScanner {
                spy: Arc::clone(&spy),
                outcome: Err(ArtifactBackendFailure {
                    retryable: false,
                    reason_class: "unused".to_owned(),
                }),
            },
            MockBlobBackend {
                spy: Arc::clone(&spy),
                outcome: Ok(expected.evidence.clone()),
            },
            MockAuthority {
                spy: Arc::clone(&spy),
            },
        );

        let failure = service
            .execute_blob_cleanup(execution, now)
            .await
            .unwrap_err();
        assert!(matches!(
            failure,
            ArtifactWorkerExecutionError::Authority("authority_stop")
        ));
        assert_eq!(
            *spy.calls.lock().unwrap(),
            vec!["blob_backend", "cleanup_authority"]
        );
        assert_eq!(
            *spy.delete_requests.lock().unwrap(),
            vec![DeleteArtifactBlobGeneration {
                tenant_id: expected.audit.tenant_id.clone(),
                job_id: expected.cleanup_job_id.clone(),
                blob_id: expected.discarded_blob_id.clone(),
                object_generation: expected.evidence.object_generation.clone(),
                fence: expected.fence.clone(),
            }]
        );
        assert_eq!(
            spy.cleanup_command.lock().unwrap().as_ref(),
            Some(&expected)
        );
    }

    #[tokio::test]
    async fn worker_backend_failure_is_fenced_and_committed_before_returning() {
        let fixture = fixture(ArtifactScanKind::Initial);
        let backend_failure = ArtifactBackendFailure {
            retryable: true,
            reason_class: "scanner_timeout".to_owned(),
        };
        let spy = Arc::new(WorkerSpy::default());
        let service = ArtifactWorkerService::new(
            MockScanner {
                spy: Arc::clone(&spy),
                outcome: Err(backend_failure.clone()),
            },
            MockBlobBackend {
                spy: Arc::clone(&spy),
                outcome: Err(backend_failure.clone()),
            },
            MockAuthority {
                spy: Arc::clone(&spy),
            },
        );

        let failure = service
            .execute_scan(scan_execution(&fixture), fixture.now)
            .await
            .unwrap_err();
        let ArtifactWorkerExecutionError::Backend(actual) = failure else {
            panic!("valid backend failure must remain a typed backend failure");
        };
        assert_eq!(actual, backend_failure);
        assert_eq!(
            *spy.calls.lock().unwrap(),
            vec!["scan_backend", "failure_authority"]
        );
        let committed = spy.failure_command.lock().unwrap().clone().unwrap();
        assert_eq!(committed.audit, fixture.command.audit);
        assert_eq!(committed.job_id, fixture.command.scan_job_id);
        assert_eq!(committed.fence, fixture.command.fence);
        assert_eq!(committed.failure, backend_failure);
        assert!(spy.scan_command.lock().unwrap().is_none());
        assert!(spy.cleanup_command.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn worker_commits_untrusted_backend_evidence_as_a_fenced_failure() {
        let mut fixture = fixture(ArtifactScanKind::Initial);
        fixture.command.evidence.canonical_digest = digest("invalid-scanner-evidence");
        let scan_spy = Arc::new(WorkerSpy::default());
        let scan_service = ArtifactWorkerService::new(
            MockScanner {
                spy: Arc::clone(&scan_spy),
                outcome: Ok(fixture.command.evidence.clone()),
            },
            MockBlobBackend {
                spy: Arc::clone(&scan_spy),
                outcome: Err(ArtifactBackendFailure {
                    retryable: false,
                    reason_class: "unused".to_owned(),
                }),
            },
            MockAuthority {
                spy: Arc::clone(&scan_spy),
            },
        );
        assert!(matches!(
            scan_service
                .execute_scan(scan_execution(&fixture), fixture.now)
                .await,
            Err(ArtifactWorkerExecutionError::Contract(
                ArtifactWorkError::InvalidEvidence
            ))
        ));
        assert_eq!(
            *scan_spy.calls.lock().unwrap(),
            vec!["scan_backend", "failure_authority"]
        );
        let scan_failure = scan_spy.failure_command.lock().unwrap().clone().unwrap();
        assert_eq!(scan_failure.job_id, fixture.command.scan_job_id);
        assert_eq!(scan_failure.fence, fixture.command.fence);
        assert_eq!(
            scan_failure.failure.reason_class,
            "invalid_scanner_evidence"
        );
        assert!(!scan_failure.failure.retryable);

        let now = Utc::now();
        let (cleanup, expected) = cleanup_execution(now);
        let mut wrong_generation = expected.evidence;
        wrong_generation.object_generation = "different-generation".to_owned();
        let cleanup_spy = Arc::new(WorkerSpy::default());
        let cleanup_service = ArtifactWorkerService::new(
            MockScanner {
                spy: Arc::clone(&cleanup_spy),
                outcome: Err(ArtifactBackendFailure {
                    retryable: false,
                    reason_class: "unused".to_owned(),
                }),
            },
            MockBlobBackend {
                spy: Arc::clone(&cleanup_spy),
                outcome: Ok(wrong_generation),
            },
            MockAuthority {
                spy: Arc::clone(&cleanup_spy),
            },
        );
        assert!(matches!(
            cleanup_service.execute_blob_cleanup(cleanup, now).await,
            Err(ArtifactWorkerExecutionError::Contract(
                ArtifactWorkError::EvidenceMismatch
            ))
        ));
        assert_eq!(
            *cleanup_spy.calls.lock().unwrap(),
            vec!["blob_backend", "failure_authority"]
        );
        let cleanup_failure = cleanup_spy.failure_command.lock().unwrap().clone().unwrap();
        assert_eq!(
            cleanup_failure.failure.reason_class,
            "invalid_blob_deletion_evidence"
        );
        assert!(!cleanup_failure.failure.retryable);
        assert!(cleanup_spy.cleanup_command.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn worker_deletion_uses_backend_only_for_physical_generation() {
        let now = Utc::now();
        let backend_evidence = ArtifactBlobDeletionEvidence {
            schema_version: 1,
            object_generation: "deletion-generation".to_owned(),
            backend_receipt_digest: digest("deletion-backend-receipt"),
            absence_evidence_digest: digest("deletion-absence"),
            observed_at: now,
        };
        let (physical_execution, physical_expected) = deletion_execution(
            now,
            ArtifactDeletionMode::BlobGeneration {
                object_generation: backend_evidence.object_generation.clone(),
            },
            &backend_evidence,
        );
        let physical_spy = Arc::new(WorkerSpy::default());
        let physical_service = ArtifactWorkerService::new(
            MockScanner {
                spy: Arc::clone(&physical_spy),
                outcome: Err(ArtifactBackendFailure {
                    retryable: false,
                    reason_class: "unused".to_owned(),
                }),
            },
            MockBlobBackend {
                spy: Arc::clone(&physical_spy),
                outcome: Ok(backend_evidence.clone()),
            },
            MockAuthority {
                spy: Arc::clone(&physical_spy),
            },
        );
        assert!(matches!(
            physical_service
                .execute_deletion(physical_execution, now)
                .await
                .unwrap_err(),
            ArtifactWorkerExecutionError::Authority("authority_stop")
        ));
        assert_eq!(
            *physical_spy.calls.lock().unwrap(),
            vec!["blob_backend", "deletion_authority"]
        );
        assert_eq!(
            *physical_spy.delete_requests.lock().unwrap(),
            vec![DeleteArtifactBlobGeneration {
                tenant_id: physical_expected.audit.tenant_id.clone(),
                job_id: physical_expected.deletion_job_id.clone(),
                blob_id: physical_expected.blob_id.clone(),
                object_generation: backend_evidence.object_generation.clone(),
                fence: physical_expected.fence.clone(),
            }]
        );
        assert_eq!(
            physical_spy.deletion_command.lock().unwrap().as_ref(),
            Some(&physical_expected)
        );

        let alias_id = id(ResourceKind::Artifact, 99);
        let (logical_execution, logical_expected) = deletion_execution(
            now,
            ArtifactDeletionMode::ArtifactOnly {
                alias_artifact_id: alias_id,
                alias_artifact_version: 5,
            },
            &backend_evidence,
        );
        let logical_spy = Arc::new(WorkerSpy::default());
        let logical_service = ArtifactWorkerService::new(
            MockScanner {
                spy: Arc::clone(&logical_spy),
                outcome: Err(ArtifactBackendFailure {
                    retryable: false,
                    reason_class: "unused".to_owned(),
                }),
            },
            MockBlobBackend {
                spy: Arc::clone(&logical_spy),
                outcome: Err(ArtifactBackendFailure {
                    retryable: false,
                    reason_class: "must_not_call".to_owned(),
                }),
            },
            MockAuthority {
                spy: Arc::clone(&logical_spy),
            },
        );
        assert!(matches!(
            logical_service
                .execute_deletion(logical_execution, now)
                .await
                .unwrap_err(),
            ArtifactWorkerExecutionError::Authority("authority_stop")
        ));
        assert_eq!(
            *logical_spy.calls.lock().unwrap(),
            vec!["deletion_authority"]
        );
        assert!(logical_spy.delete_requests.lock().unwrap().is_empty());
        assert_eq!(
            logical_spy.deletion_command.lock().unwrap().as_ref(),
            Some(&logical_expected)
        );
    }

    #[test]
    fn exact_initial_scan_commits_current_evidence() {
        let fixture = fixture(ArtifactScanKind::Initial);
        let decision = decide_commit_artifact_scan(
            &fixture.artifact,
            &fixture.blob,
            &fixture.operation,
            None,
            &fixture.job,
            &fixture.command,
            fixture.now,
        )
        .unwrap();
        assert_eq!(decision.artifact_state, ArtifactState::Verified);
        assert_eq!(decision.blob_state, BlobIntegrityState::Verified);
        assert_eq!(decision.operation_state, JobState::Waiting);
        let current = decision.metadata.current_verification.unwrap();
        assert_eq!(current.scan_job_id, fixture.command.scan_job_id);
        assert_eq!(
            current.evidence_digest,
            fixture.command.evidence.canonical_digest
        );
    }

    #[test]
    fn initial_and_rescan_scheduling_freeze_post_transition_versions() {
        let mut initial = fixture(ArtifactScanKind::Initial);
        initial.artifact.state = ArtifactState::Uploaded;
        initial.artifact.version = 3;
        let upload_operation = ArtifactOperationRecord {
            tenant_id: initial.artifact.tenant_id.clone(),
            operation_id: initial.command.operation_id.clone(),
            state: JobState::Waiting,
            version: 2,
            snapshot: ArtifactUploadOperationSnapshot {
                schema_version: 1,
                artifact_id: initial.artifact.artifact_id.clone(),
                purpose: initial.artifact.purpose,
                expected_size_bytes: initial.artifact.expected_size_bytes,
                expected_digest: initial.artifact.expected_digest.clone(),
                retention_policy_revision_id: initial.artifact.retention_policy_revision_id.clone(),
                scan_policy_revision: initial.job.scan_policy_revision.clone(),
                scanner_contract_digest: initial.job.scanner_contract_digest.clone(),
                ruleset_digest: initial.job.ruleset_digest.clone(),
                evidence_ttl_milliseconds: initial.job.evidence_ttl_milliseconds,
                retry_backoff_milliseconds: initial.job.retry_backoff_milliseconds,
            },
            deadline: initial.now + Duration::hours(1),
            created_at: initial.now - Duration::minutes(1),
        };
        let schedule = ScheduleInitialArtifactScan {
            audit: command_audit(&initial.artifact.tenant_id, 50, initial.now),
            scan_job_id: initial.command.scan_job_id.clone(),
            operation_id: initial.command.operation_id.clone(),
            artifact_id: initial.artifact.artifact_id.clone(),
            blob_id: initial.blob.blob_id.clone(),
            expected_artifact_version: initial.artifact.version,
            expected_blob_version: initial.blob.version,
            expected_operation_version: upload_operation.version,
            scan_policy_revision: initial.job.scan_policy_revision.clone(),
            scanner_contract_digest: initial.job.scanner_contract_digest.clone(),
            ruleset_digest: initial.job.ruleset_digest.clone(),
            evidence_ttl_milliseconds: 60_000,
            retry_backoff_milliseconds: 100,
            deadline: initial.now + Duration::hours(1),
        };
        let scheduled = decide_schedule_initial_scan(
            &initial.artifact,
            &initial.blob,
            &upload_operation,
            &schedule,
            initial.now,
        )
        .unwrap();
        assert_eq!(scheduled.artifact_version, 4);
        assert_eq!(scheduled.job.expected_artifact_version, 4);
        assert_eq!(scheduled.job.scan_kind, ArtifactScanKind::Initial);

        let mut rescan = fixture(ArtifactScanKind::Rescan);
        rescan.artifact.state = ArtifactState::Ready;
        rescan.artifact.version = 3;
        let schedule = ScheduleArtifactRescan {
            audit: command_audit(&rescan.artifact.tenant_id, 60, rescan.now),
            rescan_operation_id: rescan.command.operation_id.clone(),
            rescan_job_id: rescan.command.operation_id.clone(),
            artifact_id: rescan.artifact.artifact_id.clone(),
            blob_id: rescan.blob.blob_id.clone(),
            expected_artifact_version: rescan.artifact.version,
            expected_blob_version: rescan.blob.version,
            scan_policy_revision: rescan.job.scan_policy_revision.clone(),
            scanner_contract_digest: rescan.job.scanner_contract_digest.clone(),
            ruleset_digest: rescan.job.ruleset_digest.clone(),
            evidence_ttl_milliseconds: 60_000,
            retry_backoff_milliseconds: 100,
            deadline: rescan.now + Duration::hours(1),
        };
        let scheduled =
            decide_schedule_artifact_rescan(&rescan.artifact, &rescan.blob, &schedule, rescan.now)
                .unwrap();
        assert_eq!(scheduled.artifact_state, ArtifactState::Quarantined);
        assert_eq!(scheduled.job.expected_artifact_version, 4);
        assert_eq!(scheduled.job.scan_kind, ArtifactScanKind::Rescan);
    }

    #[test]
    fn initial_scan_reuses_only_an_exact_security_domain_blob() {
        let fixture = fixture(ArtifactScanKind::Initial);
        let mut reusable = fixture.blob.clone();
        reusable.blob_id = id(ResourceKind::InternalBlob, 70);
        reusable.object_generation = Some("reusable-generation".to_owned());
        reusable.content_digest = Some(fixture.command.evidence.content_digest.clone());
        reusable.size_bytes = Some(fixture.command.evidence.size_bytes);
        reusable.state = BlobIntegrityState::Verified;
        reusable.version = 9;
        let decision = decide_commit_artifact_scan(
            &fixture.artifact,
            &fixture.blob,
            &fixture.operation,
            Some(&reusable),
            &fixture.job,
            &fixture.command,
            fixture.now,
        )
        .unwrap();
        assert_eq!(decision.artifact_blob_id, reusable.blob_id);
        assert_eq!(decision.blob_state, BlobIntegrityState::Deleting);
        let cleanup = decision.duplicate_blob_cleanup.unwrap();
        assert_eq!(cleanup.discarded_blob_id, fixture.blob.blob_id);
        assert_eq!(cleanup.replacement_blob_id, reusable.blob_id);

        let mut wrong_domain = reusable;
        wrong_domain.security_domain_digest = digest("wrong-security-domain");
        assert_eq!(
            decide_commit_artifact_scan(
                &fixture.artifact,
                &fixture.blob,
                &fixture.operation,
                Some(&wrong_domain),
                &fixture.job,
                &fixture.command,
                fixture.now,
            ),
            Err(ArtifactWorkError::EvidenceMismatch)
        );
    }

    #[test]
    fn rescan_is_fail_closed_until_verified() {
        let fixture = fixture(ArtifactScanKind::Rescan);
        let decision = decide_commit_artifact_scan(
            &fixture.artifact,
            &fixture.blob,
            &fixture.operation,
            None,
            &fixture.job,
            &fixture.command,
            fixture.now,
        )
        .unwrap();
        assert_eq!(decision.artifact_state, ArtifactState::Ready);
        assert_eq!(decision.operation_state, JobState::Succeeded);
    }

    #[test]
    fn worker_fence_profile_expiry_and_job_variant_fail_closed() {
        let mut wrong_worker = fixture(ArtifactScanKind::Initial);
        wrong_worker.command.audit.worker_process_generation_id =
            id(ResourceKind::WorkerProcessGeneration, 31);
        assert_eq!(
            decide_commit_artifact_scan(
                &wrong_worker.artifact,
                &wrong_worker.blob,
                &wrong_worker.operation,
                None,
                &wrong_worker.job,
                &wrong_worker.command,
                wrong_worker.now,
            ),
            Err(ArtifactWorkError::InvalidCommand)
        );

        let mut wrong_rules = fixture(ArtifactScanKind::Initial);
        wrong_rules.command.evidence.ruleset_digest = digest("wrong-rules");
        wrong_rules.command.evidence.canonical_digest = wrong_rules
            .command
            .evidence
            .canonical_digest_without_field()
            .unwrap();
        assert_eq!(
            decide_commit_artifact_scan(
                &wrong_rules.artifact,
                &wrong_rules.blob,
                &wrong_rules.operation,
                None,
                &wrong_rules.job,
                &wrong_rules.command,
                wrong_rules.now,
            ),
            Err(ArtifactWorkError::EvidenceMismatch)
        );

        let scan_payload = ArtifactJobPayload::Scan {
            scan: fixture(ArtifactScanKind::Rescan).job,
        };
        assert_eq!(
            scan_payload.validate_for_owner(&id(ResourceKind::Job, 3)),
            Err(ArtifactWorkError::InvalidJobPayload)
        );
    }

    #[test]
    fn blob_cleanup_requires_exact_generation() {
        let fixture = fixture(ArtifactScanKind::Initial);
        let mut blob = fixture.blob;
        blob.state = BlobIntegrityState::Deleting;
        let cleanup = ArtifactBlobCleanupSnapshot {
            schema_version: 1,
            artifact_id: fixture.artifact.artifact_id,
            discarded_blob_id: blob.blob_id.clone(),
            replacement_blob_id: id(ResourceKind::InternalBlob, 40),
            object_generation: "object-generation-1".to_owned(),
            verification_evidence_digest: digest("verification"),
            expected_blob_version: blob.version,
            retry_backoff_milliseconds: 100,
        };
        let worker = id(ResourceKind::WorkerProcessGeneration, 41);
        let command = CommitArtifactBlobCleanup {
            audit: ArtifactWorkerAudit {
                trace: insight_platform_contracts::TraceIdentityV1::generate(),
                tenant_id: blob.tenant_id.clone(),
                worker_process_generation_id: worker.clone(),
                receipt_id: id(ResourceKind::Receipt, 42),
                event_id: id(ResourceKind::Event, 43),
                outbox_id: id(ResourceKind::OutboxEvent, 44),
                idempotency_key_digest: digest("cleanup-idempotency"),
                request_digest: digest("cleanup-request"),
                receipt_expires_at: fixture.now + Duration::hours(1),
            },
            cleanup_job_id: id(ResourceKind::Job, 45),
            fence: JobFence {
                expected_version: 3,
                worker_process_generation_id: worker,
                lease_generation: 1,
                token_digest: digest("cleanup-token"),
            },
            discarded_blob_id: blob.blob_id.clone(),
            expected_blob_version: blob.version,
            evidence: ArtifactBlobDeletionEvidence {
                schema_version: 1,
                object_generation: "wrong-generation".to_owned(),
                backend_receipt_digest: digest("backend-receipt"),
                absence_evidence_digest: digest("absence"),
                observed_at: fixture.now,
            },
        };
        assert_eq!(
            decide_commit_blob_cleanup(&blob, &cleanup, &command, fixture.now),
            Err(ArtifactWorkError::EvidenceMismatch)
        );
    }

    fn expired_job(
        now: DateTime<Utc>,
        payload: &ArtifactJobPayload,
        state: JobState,
        attempt_count: u32,
        attempt_limit: u32,
    ) -> JobProjection {
        let owner_id = match payload {
            ArtifactJobPayload::AwaitingStage { stage } => stage.artifact_id.clone(),
            ArtifactJobPayload::Scan { scan } | ArtifactJobPayload::Rescan { scan } => {
                scan.artifact_id.clone()
            }
            ArtifactJobPayload::Delete { deletion } => deletion.artifact_id.clone(),
            ArtifactJobPayload::BlobCleanup { cleanup } => cleanup.discarded_blob_id.clone(),
        };
        JobProjection {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: id(ResourceKind::Tenant, 120),
            job_id: id(ResourceKind::Job, 121),
            work_class: WorkClass::Artifact,
            owner: JobOwnerRef {
                owner_kind: owner_id.kind(),
                owner_id,
            },
            state,
            version: 3,
            attempt_count,
            attempt_limit,
            lease_generation: 1,
            lease: Some(JobLease {
                worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 122),
                lease_generation: 1,
                token_digest: digest("expired-token"),
                heartbeat_at: now - Duration::seconds(2),
                expires_at: now - Duration::seconds(1),
            }),
            scheduled_at: now - Duration::minutes(1),
            retry_at: None,
            wake: None,
            deadline: now + Duration::minutes(1),
        }
    }

    #[test]
    fn expired_artifact_lease_before_start_returns_ready_without_parent_mutation() {
        let fixture = fixture(ArtifactScanKind::Initial);
        let payload = ArtifactJobPayload::Scan { scan: fixture.job };
        let current = expired_job(fixture.now, &payload, JobState::Leased, 0, 3);
        let decision = decide_expired_artifact_attempt(
            &current,
            &payload,
            current.version,
            current.lease_generation,
            fixture.now,
        )
        .unwrap();
        assert_eq!(decision.job.state, JobState::Ready);
        assert_eq!(decision.job.attempt_count, 0);
        assert_eq!(decision.parent_action, ArtifactRecoveryParentAction::None);
    }

    #[test]
    fn expired_read_only_artifact_attempt_schedules_exact_bounded_retry() {
        let fixture = fixture(ArtifactScanKind::Initial);
        let payload = ArtifactJobPayload::Scan { scan: fixture.job };
        let current = expired_job(fixture.now, &payload, JobState::Running, 1, 3);
        let decision = decide_expired_artifact_attempt(
            &current,
            &payload,
            current.version,
            current.lease_generation,
            fixture.now,
        )
        .unwrap();
        assert_eq!(decision.job.state, JobState::RetryScheduled);
        assert_eq!(
            decision.job.retry_at,
            Some(fixture.now + Duration::milliseconds(100))
        );
        assert_eq!(decision.parent_action, ArtifactRecoveryParentAction::None);
    }

    #[test]
    fn expired_physical_delete_attempt_requires_reconciliation_without_retry() {
        let now = Utc::now();
        let backend_evidence = ArtifactBlobDeletionEvidence {
            schema_version: 1,
            object_generation: "deletion-generation".to_owned(),
            backend_receipt_digest: digest("deletion-receipt"),
            absence_evidence_digest: digest("deletion-absence"),
            observed_at: now,
        };
        let (execution, _) = deletion_execution(
            now,
            ArtifactDeletionMode::BlobGeneration {
                object_generation: "deletion-generation".to_owned(),
            },
            &backend_evidence,
        );
        let payload = ArtifactJobPayload::Delete {
            deletion: execution.deletion,
        };
        let current = expired_job(now, &payload, JobState::Running, 1, 3);
        let decision = decide_expired_artifact_attempt(
            &current,
            &payload,
            current.version,
            current.lease_generation,
            now,
        )
        .unwrap();
        assert_eq!(decision.job.state, JobState::ReconciliationRequired);
        assert_eq!(
            decision.parent_action,
            ArtifactRecoveryParentAction::Deletion {
                operation_state: JobState::Failed,
            }
        );
    }

    #[test]
    fn exhausted_scan_attempt_fails_closed_into_reconciliation() {
        let fixture = fixture(ArtifactScanKind::Initial);
        let payload = ArtifactJobPayload::Scan { scan: fixture.job };
        let current = expired_job(fixture.now, &payload, JobState::Running, 3, 3);
        let decision = decide_expired_artifact_attempt(
            &current,
            &payload,
            current.version,
            current.lease_generation,
            fixture.now,
        )
        .unwrap();
        assert_eq!(decision.job.state, JobState::ReconciliationRequired);
        assert_eq!(
            decision.parent_action,
            ArtifactRecoveryParentAction::Scan {
                artifact_state: ArtifactState::Quarantined,
                operation_state: JobState::Failed,
            }
        );
    }

    #[test]
    fn explicit_backend_failure_uses_fence_and_retryability_without_waiting_for_expiry() {
        let fixture = fixture(ArtifactScanKind::Initial);
        let payload = ArtifactJobPayload::Scan { scan: fixture.job };
        let mut current = expired_job(fixture.now, &payload, JobState::Running, 1, 3);
        let lease = current.lease.as_mut().unwrap();
        lease.heartbeat_at = fixture.now - Duration::seconds(1);
        lease.expires_at = fixture.now + Duration::seconds(30);
        let command = CommitArtifactAttemptFailure {
            audit: ArtifactWorkerAudit {
                trace: insight_platform_contracts::TraceIdentityV1::generate(),
                tenant_id: current.tenant_id.clone(),
                worker_process_generation_id: lease.worker_process_generation_id.clone(),
                receipt_id: id(ResourceKind::Receipt, 123),
                event_id: id(ResourceKind::Event, 124),
                outbox_id: id(ResourceKind::OutboxEvent, 125),
                idempotency_key_digest: digest("failure-idempotency"),
                request_digest: digest("failure-request"),
                receipt_expires_at: fixture.now + Duration::hours(1),
            },
            job_id: current.job_id.clone(),
            fence: JobFence {
                expected_version: current.version,
                worker_process_generation_id: lease.worker_process_generation_id.clone(),
                lease_generation: lease.lease_generation,
                token_digest: lease.token_digest.clone(),
            },
            failure: ArtifactBackendFailure {
                retryable: true,
                reason_class: "scanner_timeout".to_owned(),
            },
        };
        let retry =
            decide_artifact_backend_failure(&current, &payload, &command, fixture.now).unwrap();
        assert_eq!(retry.job.state, JobState::RetryScheduled);
        assert_eq!(retry.parent_action, ArtifactRecoveryParentAction::None);

        let mut permanent = command;
        permanent.failure.retryable = false;
        let failed =
            decide_artifact_backend_failure(&current, &payload, &permanent, fixture.now).unwrap();
        assert_eq!(failed.job.state, JobState::Failed);
        assert_eq!(
            failed.parent_action,
            ArtifactRecoveryParentAction::Scan {
                artifact_state: ArtifactState::Quarantined,
                operation_state: JobState::Failed,
            }
        );
    }
}
