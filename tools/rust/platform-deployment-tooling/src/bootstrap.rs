//! Shared seed material preparation; the existing PostgreSQL transaction owns its installation.
use insight_platform_contracts::{
    ArtifactRetentionPolicy, ResourceId, ResourceKind, SandboxArtifactIoPolicyDocument,
    SchedulingPolicyDocument, Sha256Digest,
};
use insight_platform_deployment_contracts::{
    development::DevelopmentArtifactAuthorityConfigV1, installation::InstallationError,
};

pub fn artifact_authority(
    write_storage_binding_digest: Sha256Digest,
    encryption_domain_id: ResourceId,
    preserved: Option<&DevelopmentArtifactAuthorityConfigV1>,
) -> Result<DevelopmentArtifactAuthorityConfigV1, InstallationError> {
    let artifact_io_policy = SandboxArtifactIoPolicyDocument {
        schema_version: 3,
        allowed_input_media_types: vec![
            "application/json".into(),
            "application/octet-stream".into(),
            "application/wasm".into(),
            "text/plain".into(),
        ],
        allowed_output_media_types: vec![
            "application/json".into(),
            "application/octet-stream".into(),
            "application/wasm".into(),
            "text/plain".into(),
        ],
        maximum_input_artifacts: 64,
        maximum_output_artifacts: 64,
        scanner_contract_digest:
            insight_platform_artifacts::execution::integrity_scanner_contract_digest(),
        verification_evidence_ttl_milliseconds: 3_600_000,
        verification_retry_backoff_milliseconds: 250,
        write_storage_binding_digest,
        encryption_domain_id,
        deny_symlink: true,
        deny_hardlink: true,
        deny_device: true,
        deny_fifo: true,
        deny_socket: true,
        deny_sparse_file: true,
        archive_expansion_disabled: true,
    };
    let retention_policy = ArtifactRetentionPolicy {
        version: 1,
        minimum_retention_seconds: 3_600,
        gc_grace_seconds: 86_400,
        tombstone_retention_seconds: 2_592_000,
        retain_provenance_sources: true,
        delete_requires_approval: false,
    };
    let scheduling_policy = SchedulingPolicyDocument {
        version: 1,
        weight: 1,
        burst: 2,
        aging_rounds: 2,
    };
    if let Some(seed) = preserved {
        seed.validate()
            .map_err(|_| InstallationError::InvalidInput)?;
        let expected = serde_json::json!({"retention_policy":retention_policy,"artifact_io_policy":artifact_io_policy,"scheduling_policy":scheduling_policy,"staging_quota_bytes":67_108_864_i64,"orchestration_concurrent_jobs":4_i64});
        let actual = serde_json::json!({"retention_policy":seed.retention_policy,"artifact_io_policy":seed.artifact_io_policy,"scheduling_policy":seed.scheduling_policy,"staging_quota_bytes":seed.staging_quota_bytes,"orchestration_concurrent_jobs":seed.orchestration_concurrent_jobs});
        if expected != actual {
            return Err(InstallationError::ConfigurationDrift);
        }
        return Ok(seed.clone());
    }
    let fresh = |kind| {
        ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7())
            .map_err(|_| InstallationError::InvalidInput)
    };
    let seed = DevelopmentArtifactAuthorityConfigV1 {
        schema_version: 1,
        environment_class: "development".into(),
        authoring_artifact_id: fresh(ResourceKind::Artifact)?,
        authoring_blob_id: fresh(ResourceKind::InternalBlob)?,
        retention_policy_id: fresh(ResourceKind::Policy)?,
        retention_policy_revision_id: fresh(ResourceKind::PolicyRevision)?,
        retention_policy_deployment_id: fresh(ResourceKind::PolicyDeployment)?,
        artifact_io_policy_id: fresh(ResourceKind::Policy)?,
        artifact_io_policy_revision_id: fresh(ResourceKind::PolicyRevision)?,
        artifact_io_policy_deployment_id: fresh(ResourceKind::PolicyDeployment)?,
        scheduling_policy_id: fresh(ResourceKind::Policy)?,
        scheduling_policy_revision_id: fresh(ResourceKind::PolicyRevision)?,
        scheduling_policy_deployment_id: fresh(ResourceKind::PolicyDeployment)?,
        staging_quota_account_id: fresh(ResourceKind::QuotaAccount)?,
        orchestration_quota_account_id: fresh(ResourceKind::QuotaAccount)?,
        retention_policy,
        artifact_io_policy,
        scheduling_policy,
        staging_quota_bytes: 67_108_864,
        orchestration_concurrent_jobs: 4,
    };
    seed.validate()
        .map_err(|_| InstallationError::InvalidInput)?;
    Ok(seed)
}
