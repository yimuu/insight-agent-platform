//! Narrow Registry authoring read authority. Callers select an owner and a closed
//! document role; they cannot supply an object locator or unrelated Artifact ID.
use base64::Engine as _;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    ArtifactPurpose, ArtifactRef, ResourceId, ResourceKind, Sha256Digest,
};
use serde::{Deserialize, Serialize};

pub const REGISTRY_ARTIFACT_READ_SCHEMA_VERSION: u32 = 1;
pub const MAX_REGISTRY_ARTIFACT_BYTES: usize = 8_388_608;
pub const MAX_REGISTRY_ARTIFACT_RESPONSE_BYTES: usize = 12_582_912;
pub const MAX_REGISTRY_ARTIFACT_READ_REQUEST_BYTES: usize = 16_384;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RegistryArtifactReadAuthorityV1 {
    RegistryDraft {
        resource_id: ResourceId,
        expected_resource_version: u64,
    },
    RegistryJob {
        job_id: ResourceId,
        worker_process_generation_id: ResourceId,
        lease_epoch: u64,
        expected_job_version: u64,
        lease_token_digest: Sha256Digest,
    },
}

impl RegistryArtifactReadAuthorityV1 {
    pub fn validate(&self) -> Result<(), crate::ArtifactObjectReadAuthorityError> {
        let valid = match self {
            Self::RegistryDraft {
                resource_id,
                expected_resource_version,
            } => resource_id.kind() == ResourceKind::Agent && *expected_resource_version > 0,
            Self::RegistryJob {
                job_id,
                worker_process_generation_id,
                lease_epoch,
                expected_job_version,
                ..
            } => {
                job_id.kind() == ResourceKind::Job
                    && worker_process_generation_id.kind() == ResourceKind::WorkerProcessGeneration
                    && *lease_epoch > 0
                    && *expected_job_version > 0
            }
        };
        if valid {
            Ok(())
        } else {
            Err(crate::ArtifactObjectReadAuthorityError::Denied)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistryArtifactSelectorV1 {
    Authoring,
    TypedPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryArtifactReadRequestV1 {
    pub schema_version: u32,
    pub authority: RegistryArtifactReadAuthorityV1,
    pub selector: RegistryArtifactSelectorV1,
    pub deadline: DateTime<Utc>,
}

impl RegistryArtifactReadRequestV1 {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), crate::ArtifactObjectReadAuthorityError> {
        self.authority.validate()?;
        if self.schema_version != REGISTRY_ARTIFACT_READ_SCHEMA_VERSION
            || self.deadline <= now
            || self.deadline > now + chrono::Duration::seconds(30)
        {
            return Err(crate::ArtifactObjectReadAuthorityError::Denied);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryArtifactReadResponseV1 {
    pub schema_version: u32,
    pub artifact: ArtifactRef,
    pub purpose: ArtifactPurpose,
    pub content_base64: String,
}

impl RegistryArtifactReadResponseV1 {
    pub fn from_verified(
        artifact: ArtifactRef,
        purpose: ArtifactPurpose,
        bytes: &[u8],
    ) -> Result<Self, crate::ArtifactObjectReadAuthorityError> {
        let response = Self {
            schema_version: 1,
            artifact,
            purpose,
            content_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
        };
        response.decode_verified()?;
        Ok(response)
    }

    pub fn decode_verified(&self) -> Result<Vec<u8>, crate::ArtifactObjectReadAuthorityError> {
        use sha2::{Digest, Sha256};
        if self.schema_version != 1
            || self.artifact.validate().is_err()
            || !matches!(
                self.purpose,
                ArtifactPurpose::AuthoringDocument | ArtifactPurpose::TypedPlan
            )
            || self.artifact.byte_length() > MAX_REGISTRY_ARTIFACT_BYTES as u64
            || self.content_base64.len() > MAX_REGISTRY_ARTIFACT_RESPONSE_BYTES - 16_384
        {
            return Err(crate::ArtifactObjectReadAuthorityError::InvalidEvidence);
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&self.content_base64)
            .map_err(|_| crate::ArtifactObjectReadAuthorityError::InvalidEvidence)?;
        let digest = format!(
            "sha256:{}",
            Sha256::digest(&bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        if bytes.len() as u64 != self.artifact.byte_length()
            || bytes.len() > MAX_REGISTRY_ARTIFACT_BYTES
            || digest != self.artifact.content_digest().as_str()
            || base64::engine::general_purpose::STANDARD.encode(&bytes) != self.content_base64
        {
            return Err(crate::ArtifactObjectReadAuthorityError::InvalidEvidence);
        }
        Ok(bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GatewayArtifactReadAuthority {
    PublicContent,
    RegistryValidation {
        authority: RegistryArtifactReadAuthorityV1,
        selector: RegistryArtifactSelectorV1,
    },
}
