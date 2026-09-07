//! Exact development bootstrap inputs, shared by the CLI producer and one-shot storage tool.
//! They do not authorize production deployment or replace PostgreSQL's bootstrap transaction.
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, ArtifactRetentionPolicy, JsonLimits, ResourceId,
    ResourceKind, SandboxArtifactIoPolicyDocument, SchedulingPolicyDocument, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, error::Error, fmt};

pub const MAX_DEVELOPMENT_ARTIFACT_BOOTSTRAP_BYTES: usize = 65_536;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevelopmentArtifactAuthorityConfigV1 {
    pub schema_version: u32,
    pub environment_class: String,
    pub authoring_artifact_id: ResourceId,
    pub authoring_blob_id: ResourceId,
    pub retention_policy_id: ResourceId,
    pub retention_policy_revision_id: ResourceId,
    pub retention_policy_deployment_id: ResourceId,
    pub artifact_io_policy_id: ResourceId,
    pub artifact_io_policy_revision_id: ResourceId,
    pub artifact_io_policy_deployment_id: ResourceId,
    pub scheduling_policy_id: ResourceId,
    pub scheduling_policy_revision_id: ResourceId,
    pub scheduling_policy_deployment_id: ResourceId,
    pub staging_quota_account_id: ResourceId,
    pub orchestration_quota_account_id: ResourceId,
    pub retention_policy: ArtifactRetentionPolicy,
    pub artifact_io_policy: SandboxArtifactIoPolicyDocument,
    pub scheduling_policy: SchedulingPolicyDocument,
    pub staging_quota_bytes: i64,
    pub orchestration_concurrent_jobs: i64,
}

impl DevelopmentArtifactAuthorityConfigV1 {
    pub fn decode(
        bytes: &[u8],
        expected: &Sha256Digest,
    ) -> Result<Self, DevelopmentBootstrapConfigError> {
        let value = parse_strict_json(
            bytes,
            JsonLimits {
                max_bytes: MAX_DEVELOPMENT_ARTIFACT_BOOTSTRAP_BYTES,
                max_depth: 10,
                max_properties_per_object: 32,
                max_items_per_array: 64,
                max_string_bytes: 512,
            },
        )
        .map_err(|_| DevelopmentBootstrapConfigError)?;
        let actual: Sha256Digest = canonical_digest(&value)
            .map_err(|_| DevelopmentBootstrapConfigError)?
            .parse()
            .map_err(|_| DevelopmentBootstrapConfigError)?;
        if &actual != expected {
            return Err(DevelopmentBootstrapConfigError);
        }
        let config: Self =
            serde_json::from_value(value).map_err(|_| DevelopmentBootstrapConfigError)?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), DevelopmentBootstrapConfigError> {
        if self.schema_version != 1 || self.environment_class != "development" {
            return Err(DevelopmentBootstrapConfigError);
        }
        for (id, kind) in [
            (&self.authoring_artifact_id, ResourceKind::Artifact),
            (&self.authoring_blob_id, ResourceKind::InternalBlob),
            (&self.retention_policy_id, ResourceKind::Policy),
            (
                &self.retention_policy_revision_id,
                ResourceKind::PolicyRevision,
            ),
            (
                &self.retention_policy_deployment_id,
                ResourceKind::PolicyDeployment,
            ),
            (&self.artifact_io_policy_id, ResourceKind::Policy),
            (
                &self.artifact_io_policy_revision_id,
                ResourceKind::PolicyRevision,
            ),
            (
                &self.artifact_io_policy_deployment_id,
                ResourceKind::PolicyDeployment,
            ),
            (&self.scheduling_policy_id, ResourceKind::Policy),
            (
                &self.scheduling_policy_revision_id,
                ResourceKind::PolicyRevision,
            ),
            (
                &self.scheduling_policy_deployment_id,
                ResourceKind::PolicyDeployment,
            ),
            (&self.staging_quota_account_id, ResourceKind::QuotaAccount),
            (
                &self.orchestration_quota_account_id,
                ResourceKind::QuotaAccount,
            ),
            (
                &self.artifact_io_policy.encryption_domain_id,
                ResourceKind::EncryptionDomain,
            ),
        ] {
            if id.kind() != kind {
                return Err(DevelopmentBootstrapConfigError);
            }
        }
        let unique = [
            &self.authoring_artifact_id,
            &self.authoring_blob_id,
            &self.retention_policy_id,
            &self.retention_policy_revision_id,
            &self.retention_policy_deployment_id,
            &self.artifact_io_policy_id,
            &self.artifact_io_policy_revision_id,
            &self.artifact_io_policy_deployment_id,
            &self.scheduling_policy_id,
            &self.scheduling_policy_revision_id,
            &self.scheduling_policy_deployment_id,
            &self.staging_quota_account_id,
            &self.orchestration_quota_account_id,
            &self.artifact_io_policy.encryption_domain_id,
        ]
        .into_iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
        if unique.len() != 14
            || self.staging_quota_bytes <= 0
            || self.orchestration_concurrent_jobs <= 0
            || self.retention_policy.validate().is_err()
            || self.artifact_io_policy.validate().is_err()
            || self.scheduling_policy.validate().is_err()
        {
            return Err(DevelopmentBootstrapConfigError);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DevelopmentBootstrapConfigError;
impl fmt::Display for DevelopmentBootstrapConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("development Artifact bootstrap configuration is invalid")
    }
}
impl Error for DevelopmentBootstrapConfigError {}
