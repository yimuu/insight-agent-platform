//! One current process catalog: S3 object transport and an explicit reference-key backend.
use crate::aws::{ArtifactProviderConfigError, AwsKmsKeyBindingConfig, S3StorageBindingConfig};
use crate::MAX_INSTALLED_ARTIFACT_STORAGE_BINDINGS;
use insight_platform_contracts::Sha256Digest;
use insight_platform_openbao::{BaoClientConfigV1, TransitBindingV1};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactProviderCatalogConfigV2 {
    pub schema_version: u32,
    pub write_storage_binding_digest: Sha256Digest,
    pub s3_storage_bindings: Vec<S3StorageBindingConfig>,
    pub reference_key_bindings: Vec<ArtifactReferenceKeyBindingConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "config",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ArtifactReferenceKeyBindingConfig {
    AwsKms(AwsKmsKeyBindingConfig),
    #[serde(rename = "openbao_transit")]
    OpenBaoTransit(Box<OpenBaoArtifactKeyBindingConfigV1>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenBaoArtifactKeyBindingConfigV1 {
    pub schema_version: u32,
    /// Physical key identity is shared across role-specific mTLS credentials and file paths.
    pub kms_binding_digest: Sha256Digest,
    pub client: BaoClientConfigV1,
    pub key: TransitBindingV1,
}

impl OpenBaoArtifactKeyBindingConfigV1 {
    pub fn validate(&self) -> Result<(), ArtifactProviderConfigError> {
        if self.schema_version != 1
            || self.key.validate_for(&self.client).is_err()
            || self.kms_binding_digest != self.key.identity_digest
        {
            return Err(ArtifactProviderConfigError::InvalidKmsBinding);
        }
        Ok(())
    }
}

impl ArtifactReferenceKeyBindingConfig {
    pub fn digest(&self) -> &Sha256Digest {
        match self {
            Self::AwsKms(config) => &config.kms_binding_digest,
            Self::OpenBaoTransit(config) => &config.kms_binding_digest,
        }
    }

    fn key_id(&self) -> String {
        match self {
            Self::AwsKms(config) => config.key_id.clone(),
            Self::OpenBaoTransit(config) => config.key.key_id(),
        }
    }

    fn validate(&self) -> Result<(), ArtifactProviderConfigError> {
        match self {
            Self::AwsKms(config) => config.validate(),
            Self::OpenBaoTransit(config) => config.validate(),
        }
    }
}

impl ArtifactProviderCatalogConfigV2 {
    pub fn validate(&self) -> Result<(), ArtifactProviderConfigError> {
        if self.schema_version != 2
            || self.s3_storage_bindings.is_empty()
            || self.reference_key_bindings.is_empty()
            || self.s3_storage_bindings.len() > MAX_INSTALLED_ARTIFACT_STORAGE_BINDINGS
            || self.reference_key_bindings.len() > MAX_INSTALLED_ARTIFACT_STORAGE_BINDINGS
            || !self
                .s3_storage_bindings
                .iter()
                .any(|binding| binding.storage_binding_digest == self.write_storage_binding_digest)
        {
            return Err(ArtifactProviderConfigError::InvalidCatalog);
        }
        let mut storage_digests = BTreeSet::new();
        let mut referenced_keys = BTreeSet::new();
        for binding in &self.s3_storage_bindings {
            binding.validate()?;
            if !storage_digests.insert(binding.storage_binding_digest.clone()) {
                return Err(ArtifactProviderConfigError::DuplicateStorageBinding);
            }
            referenced_keys.insert(binding.kms_binding_digest.clone());
        }
        let mut actual_keys = BTreeSet::new();
        let mut key_ids = BTreeSet::new();
        for binding in &self.reference_key_bindings {
            binding.validate()?;
            if !actual_keys.insert(binding.digest().clone()) {
                return Err(ArtifactProviderConfigError::DuplicateKmsBinding);
            }
            if !key_ids.insert(binding.key_id()) {
                return Err(ArtifactProviderConfigError::DuplicateKmsKey);
            }
        }
        if actual_keys != referenced_keys {
            return Err(ArtifactProviderConfigError::KmsBindingClosureMismatch);
        }
        Ok(())
    }
}
