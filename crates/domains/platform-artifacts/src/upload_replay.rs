use super::*;

/// Existing command identity only. A replay lookup neither admits work nor extends a deadline.
#[derive(Debug, Clone)]
pub struct ArtifactUploadReplayIdentity {
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
}

impl ArtifactUploadReplayIdentity {
    pub fn validate(&self) -> Result<(), ArtifactCommandError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.principal_id.kind() != ResourceKind::Principal
            || self.principal_kind == PrincipalKind::InstallationOperator
        {
            return Err(ArtifactCommandError::InvalidIdentity);
        }
        Ok(())
    }

    pub fn from_audit(audit: &CommandAudit) -> Self {
        Self {
            tenant_id: audit.tenant_id.clone(),
            principal_id: audit.principal_id.clone(),
            principal_kind: audit.principal_kind,
            idempotency_key_digest: audit.idempotency_key_digest.clone(),
            request_digest: audit.request_digest.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactUploadCompletionReplay {
    pub identity: ArtifactUploadReplayIdentity,
    pub artifact_id: ResourceId,
    pub expected_artifact_version: u64,
    pub grant_token_digest: Sha256Digest,
    pub scan_request_digest: Sha256Digest,
}

impl ArtifactUploadCompletionReplay {
    pub fn validate(&self) -> Result<(), ArtifactCommandError> {
        self.identity.validate()?;
        if self.artifact_id.kind() != ResourceKind::Artifact || self.expected_artifact_version == 0
        {
            return Err(ArtifactCommandError::InvalidIdentity);
        }
        Ok(())
    }
}

/// Receipt evidence identifies the committed upload attempt, not the Artifact's current Blob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactUploadCompletionReceiptV1 {
    pub schema_version: u32,
    pub artifact_id: ResourceId,
    pub original_blob_id: ResourceId,
    pub upload_grant_id: ResourceId,
    pub operation_id: ResourceId,
    pub expected_artifact_version: u64,
    pub grant_generation: u64,
    pub grant_token_digest: Sha256Digest,
}

impl ArtifactUploadCompletionReceiptV1 {
    pub fn validate(&self) -> Result<(), ArtifactCommandError> {
        if self.schema_version != 1
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.original_blob_id.kind() != ResourceKind::InternalBlob
            || self.upload_grant_id.kind() != ResourceKind::ArtifactGrant
            || self.operation_id.kind() != ResourceKind::Job
            || self.expected_artifact_version == 0
            || self.grant_generation == 0
        {
            return Err(ArtifactCommandError::InvalidIdentity);
        }
        Ok(())
    }

    pub fn from_command(command: &CompleteArtifactUpload) -> Self {
        Self {
            schema_version: 1,
            artifact_id: command.artifact_id.clone(),
            original_blob_id: command.blob_id.clone(),
            upload_grant_id: command.upload_grant_id.clone(),
            operation_id: command.operation_id.clone(),
            expected_artifact_version: command.expected_artifact_version,
            grant_generation: command.grant_generation,
            grant_token_digest: command.grant_token_digest.clone(),
        }
    }
}

/// Signing is permitted only for the original, still-live staging attempt. This is not renewal.
pub fn validate_artifact_prepare_replay(
    prepared: &PreparedArtifact,
    identity: &ArtifactUploadReplayIdentity,
    now: DateTime<Utc>,
) -> Result<(), ArtifactCommandError> {
    identity.validate()?;
    let PreparedArtifact {
        artifact,
        blob,
        grant,
        operation,
    } = prepared;
    if artifact.tenant_id != identity.tenant_id
        || blob.tenant_id != identity.tenant_id
        || grant.tenant_id != identity.tenant_id
        || operation.tenant_id != identity.tenant_id
        || artifact.blob_id.as_ref() != Some(&blob.blob_id)
        || grant.artifact_id != artifact.artifact_id
        || grant.snapshot.artifact_id != artifact.artifact_id
        || operation.snapshot.artifact_id != artifact.artifact_id
        || operation.snapshot.purpose != artifact.purpose
        || operation.snapshot.expected_size_bytes != artifact.expected_size_bytes
        || operation.snapshot.expected_digest != artifact.expected_digest
        || operation.snapshot.retention_policy_revision_id != artifact.retention_policy_revision_id
        || artifact.metadata.upload_operation_id()? != &operation.operation_id
        || grant.snapshot.operation_id != operation.operation_id
        || grant.snapshot.subject_principal_id != identity.principal_id
        || grant.snapshot.subject_principal_kind != identity.principal_kind
        || grant.snapshot.purpose != artifact.purpose
        || grant.snapshot.max_bytes != artifact.expected_size_bytes
        || grant.snapshot.expires_at > operation.deadline
        || blob.security_domain_digest
            != (ArtifactBlobSecurityDomain {
                schema_version: 1,
                classification: artifact.classification,
                retention_policy_revision_id: artifact.retention_policy_revision_id.clone(),
                encryption_domain_id: blob.encryption_domain_id.clone(),
            })
            .canonical_digest()?
    {
        return Err(ArtifactCommandError::InvalidIdentity);
    }
    if artifact.state != ArtifactState::Staging
        || blob.state != BlobIntegrityState::Staging
        || grant.state != ArtifactLinkState::Active
        || operation.state != JobState::Waiting
        || grant.snapshot.expires_at <= now
        || operation.deadline <= now
        || !grant
            .snapshot
            .operations
            .contains(&ArtifactGrantOperation::WriteStaging)
        || !grant
            .snapshot
            .operations
            .contains(&ArtifactGrantOperation::CommitStaging)
    {
        return Err(ArtifactCommandError::InvalidTransition);
    }
    Ok(())
}
