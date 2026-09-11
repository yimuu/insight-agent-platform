//! Closed process catalog for explicitly installed Secret physical providers.
use crate::{
    AwsSecretProviderCatalogConfig, AwsSecretProviderConfig, MAX_INSTALLED_SECRET_PROVIDERS,
};
use insight_platform_contracts::{canonical_digest, ResourceId, ResourceKind, Sha256Digest};
use insight_platform_openbao::{BaoClientConfigV1, BaoSecretPath, KvV2BindingV1, TransitBindingV1};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretProviderConfigError {
    InvalidCatalog,
    InvalidProvider,
    DuplicateProvider,
    DuplicateNamespace,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretProviderCatalogConfigV2 {
    pub schema_version: u32,
    pub providers: Vec<SecretProviderConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "config",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SecretProviderConfig {
    AwsSecretsManager(Box<AwsSecretProviderConfig>),
    #[serde(rename = "openbao_kv_v2")]
    OpenBaoKvV2(Box<OpenBaoSecretProviderConfigV1>),
}

impl SecretProviderConfig {
    pub fn provider_id(&self) -> &ResourceId {
        match self {
            Self::AwsSecretsManager(config) => &config.provider_id,
            Self::OpenBaoKvV2(config) => &config.provider_id,
        }
    }
}

impl SecretProviderCatalogConfigV2 {
    pub fn validate(&self) -> Result<(), SecretProviderConfigError> {
        if self.schema_version != 2
            || self.providers.is_empty()
            || self.providers.len() > MAX_INSTALLED_SECRET_PROVIDERS
        {
            return Err(SecretProviderConfigError::InvalidCatalog);
        }
        let mut provider_ids = BTreeSet::new();
        let mut aws = Vec::new();
        let mut bao: Vec<&OpenBaoSecretProviderConfigV1> = Vec::new();
        for provider in &self.providers {
            if !provider_ids.insert(provider.provider_id()) {
                return Err(SecretProviderConfigError::DuplicateProvider);
            }
            match provider {
                SecretProviderConfig::AwsSecretsManager(config) => {
                    aws.push(config.as_ref().clone())
                }
                SecretProviderConfig::OpenBaoKvV2(config) => {
                    config.validate()?;
                    if bao.iter().any(|other| {
                        other.kv.identity_digest == config.kv.identity_digest
                            && paths_overlap(&other.secret_path_prefix, &config.secret_path_prefix)
                    }) {
                        return Err(SecretProviderConfigError::DuplicateNamespace);
                    }
                    bao.push(config);
                }
            }
        }
        if !aws.is_empty() {
            AwsSecretProviderCatalogConfig {
                schema_version: 1,
                providers: aws,
            }
            .validate()
            .map_err(|_| SecretProviderConfigError::InvalidProvider)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenBaoSecretProviderConfigV1 {
    pub schema_version: u32,
    pub provider_id: ResourceId,
    pub provider_config_digest: Sha256Digest,
    pub client: BaoClientConfigV1,
    pub kv: KvV2BindingV1,
    pub reference_key: TransitBindingV1,
    pub secret_path_prefix: String,
    pub readiness: OpenBaoSecretReadinessV1,
}

/// Exact non-secret installation canary; never a user credential health probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenBaoSecretReadinessV1 {
    pub relative_path: String,
    pub version: u64,
    pub content_digest: Sha256Digest,
}

impl OpenBaoSecretProviderConfigV1 {
    pub fn calculated_digest(&self) -> Result<Sha256Digest, SecretProviderConfigError> {
        canonical_digest(&serde_json::json!({
            "schema_version": self.schema_version,
            "provider": "openbao_kv_v2",
            "provider_id": self.provider_id,
            "cluster_id": self.client.expected_cluster_id,
            "kv": self.kv,
            "reference_key": self.reference_key,
            "secret_path_prefix": self.secret_path_prefix,
            "readiness": self.readiness,
        }))
        .map_err(|_| SecretProviderConfigError::InvalidProvider)?
        .parse()
        .map_err(|_| SecretProviderConfigError::InvalidProvider)
    }

    pub fn validate(&self) -> Result<(), SecretProviderConfigError> {
        if self.schema_version != 1
            || self.provider_id.kind() != ResourceKind::SecretProvider
            || self.client.validate().is_err()
            || self.kv.validate_for(&self.client).is_err()
            || self.reference_key.validate_for(&self.client).is_err()
            || self.kv.mount == self.reference_key.mount
            || self.kv.mount_accessor == self.reference_key.mount_accessor
            || self.secret_path_prefix.len() > 128
            || BaoSecretPath::parse(&self.secret_path_prefix).is_err()
            || BaoSecretPath::parse(&self.readiness.relative_path).is_err()
            || paths_overlap(&self.secret_path_prefix, &self.readiness.relative_path)
            || self.readiness.version != 1
            || self.calculated_digest()? != self.provider_config_digest
        {
            return Err(SecretProviderConfigError::InvalidProvider);
        }
        Ok(())
    }
}

pub(crate) fn paths_overlap(left: &str, right: &str) -> bool {
    path_is_within(left, right) || path_is_within(right, left)
}

pub(crate) fn path_is_within(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

#[cfg(test)]
pub(crate) mod tests;
