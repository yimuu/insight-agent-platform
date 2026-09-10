//! Production AWS Secrets Manager/KMS implementation for the trusted Secret Broker role.
//!
//! No static credential is accepted by this module. SDK clients use the default workload-identity
//! chain, while every provider endpoint, KMS key and namespace is frozen by the CandidateManifest.

use super::prepared::{
    check_import_permit, decode_prepared, deterministic_binding_id, encode_prepared, exact_binding,
    reference_encryption_context as kms_context, validate_seal_identity, PreparedMcpOAuthPkce,
    PreparedMcpOAuthToken, PreparedModelCredential, PreparedSecretEnvelope, SecretBytes,
};
use super::{
    InstalledSecretProvider, InstalledSecretProviderCatalog, NoopSecretExternalDependencyObserver,
    OpaqueSecretReference, ProviderPreparedSecretVersion, ProviderSecretMaterial,
    ProviderStoredMcpOAuthTokenSecret, ProviderStoredMcpOAuthTransientSecretBundle,
    SealedSecretReference, SecretExternalDependency, SecretExternalDependencyObserver,
    SecretExternalDependencyOutcome, SecretProviderDeleteDisposition, SecretProviderDeleteError,
    SecretProviderPrepareError, SecretProviderResolveError, SecretReferenceSealError,
    SecretReferenceSealer, SecretReferenceUnsealError, SecretReferenceUnsealer,
    MAX_OPAQUE_SECRET_REFERENCE_BYTES,
};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_kms::{
    primitives::Blob as KmsBlob, types::EncryptionAlgorithmSpec, Client as KmsClient,
};
use aws_sdk_secretsmanager::{primitives::Blob as SecretBlob, Client as SecretsClient};
use chrono::Utc;
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ResourceId, ResourceKind,
    SecretResolutionPolicy, Sha256Digest,
};
use insight_platform_egress::{
    McpOAuthTokenPreparation, McpOAuthTokenSet, NewMcpOAuthTransientSecretBundle,
    SensitiveMcpOAuthPkceVerifier, StoredMcpOAuthTokenSecret, StoredMcpOAuthTransientSecretBundle,
    VerifiedMcpOAuthToken,
};
use insight_platform_mcp_host::{
    SensitiveMcpOAuthNonce, SensitiveOAuthValue, MCP_OAUTH_PKCE_SECRET_PURPOSE,
};
use insight_platform_security::{EncryptedOpaqueReference, SecretBindingResolutionRecord};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{collections::HashMap, fmt, str::FromStr, sync::Arc, time::Duration};
use url::Url;
use uuid::Uuid;

const MAX_PROVIDER_CONFIGS: usize = 64;
const MAX_PROVIDER_ENDPOINT_BYTES: usize = 2_048;
const MAX_SECRET_ID_BYTES: usize = 2_048;
const MAX_SECRET_VERSION_ID_BYTES: usize = 64;
const MAX_SECRET_NAME_PREFIX_BYTES: usize = 128;
const MAX_OPERATION_TIMEOUT_MILLISECONDS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsSecretProviderConfig {
    pub schema_version: u32,
    pub provider_id: ResourceId,
    pub provider_config_digest: Sha256Digest,
    pub region: String,
    pub secrets_endpoint: String,
    pub kms_endpoint: String,
    pub kms_key_arn: String,
    pub secret_arn_prefix: String,
    pub secret_name_prefix: String,
    pub readiness_secret_id: String,
    pub connect_timeout_milliseconds: u64,
    pub operation_timeout_milliseconds: u64,
}

impl AwsSecretProviderConfig {
    fn calculated_digest(&self) -> Result<Sha256Digest, AwsSecretProviderConfigError> {
        canonical_digest(&serde_json::json!({
            "connect_timeout_milliseconds": self.connect_timeout_milliseconds,
            "kms_endpoint": self.kms_endpoint,
            "kms_key_arn": self.kms_key_arn,
            "operation_timeout_milliseconds": self.operation_timeout_milliseconds,
            "provider_id": self.provider_id,
            "readiness_secret_id": self.readiness_secret_id,
            "region": self.region,
            "schema_version": self.schema_version,
            "secret_arn_prefix": self.secret_arn_prefix,
            "secret_name_prefix": self.secret_name_prefix,
            "secrets_endpoint": self.secrets_endpoint,
        }))
        .map_err(|_| AwsSecretProviderConfigError::InvalidProvider)?
        .parse()
        .map_err(|_| AwsSecretProviderConfigError::InvalidProvider)
    }

    fn validate(&self) -> Result<(), AwsSecretProviderConfigError> {
        if self.schema_version != 1
            || self.provider_id.kind() != ResourceKind::SecretProvider
            || self.calculated_digest()? != self.provider_config_digest
            || !valid_region(&self.region)
            || !valid_https_endpoint(&self.secrets_endpoint)
            || !valid_https_endpoint(&self.kms_endpoint)
            || !valid_kms_key_arn(&self.kms_key_arn, &self.region)
            || !valid_secret_arn_prefix(&self.secret_arn_prefix, &self.region)
            || !valid_secret_prefix(&self.secret_name_prefix)
            || !secret_name_in_arn_namespace(&self.secret_name_prefix, &self.secret_arn_prefix)
            || !valid_secret_arn(&self.readiness_secret_id)
            || !self
                .readiness_secret_id
                .starts_with(&self.secret_arn_prefix)
            || !valid_timeouts(
                self.connect_timeout_milliseconds,
                self.operation_timeout_milliseconds,
            )
        {
            return Err(AwsSecretProviderConfigError::InvalidProvider);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsSecretProviderCatalogConfig {
    pub schema_version: u32,
    pub providers: Vec<AwsSecretProviderConfig>,
}

impl AwsSecretProviderCatalogConfig {
    pub fn validate(&self) -> Result<(), AwsSecretProviderConfigError> {
        if self.schema_version != 1
            || self.providers.is_empty()
            || self.providers.len() > MAX_PROVIDER_CONFIGS
        {
            return Err(AwsSecretProviderConfigError::InvalidCatalog);
        }
        let mut provider_ids = std::collections::BTreeSet::new();
        let mut name_prefixes = Vec::new();
        let mut arn_prefixes = Vec::new();
        for provider in &self.providers {
            provider.validate()?;
            if !provider_ids.insert(provider.provider_id.clone()) {
                return Err(AwsSecretProviderConfigError::DuplicateProvider);
            }
            if name_prefixes.iter().any(|prefix: &String| {
                prefix.starts_with(&provider.secret_name_prefix)
                    || provider.secret_name_prefix.starts_with(prefix)
            }) || arn_prefixes.iter().any(|prefix: &String| {
                prefix.starts_with(&provider.secret_arn_prefix)
                    || provider.secret_arn_prefix.starts_with(prefix)
            }) {
                return Err(AwsSecretProviderConfigError::DuplicateNamespace);
            }
            name_prefixes.push(provider.secret_name_prefix.clone());
            arn_prefixes.push(provider.secret_arn_prefix.clone());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwsSecretProviderConfigError {
    InvalidCatalog,
    InvalidProvider,
    DuplicateProvider,
    DuplicateNamespace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwsSecretProviderReadinessError {
    SecretsManagerUnavailable,
    SecretsManagerInvalidEvidence,
    KmsUnavailable,
    KmsInvalidEvidence,
}

pub struct AwsSecretProviderCatalog {
    providers: InstalledSecretProviderCatalog,
    sealer: Arc<dyn SecretReferenceSealer>,
    unsealer: Arc<dyn SecretReferenceUnsealer>,
    readiness: Vec<AwsProviderReadiness>,
}

impl fmt::Debug for AwsSecretProviderCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AwsSecretProviderCatalog")
            .field("provider_count", &self.readiness.len())
            .finish_non_exhaustive()
    }
}

impl AwsSecretProviderCatalog {
    pub async fn install(
        config: AwsSecretProviderCatalogConfig,
    ) -> Result<Self, AwsSecretProviderConfigError> {
        Self::install_with_observer(config, Arc::new(NoopSecretExternalDependencyObserver)).await
    }

    pub async fn install_with_observer(
        config: AwsSecretProviderCatalogConfig,
        observer: Arc<dyn SecretExternalDependencyObserver>,
    ) -> Result<Self, AwsSecretProviderConfigError> {
        config.validate()?;
        let mut installed: Vec<Arc<dyn InstalledSecretProvider>> = Vec::new();
        let mut kms = HashMap::new();
        let mut readiness = Vec::new();
        for provider in config.providers {
            let shared = aws_config::defaults(BehaviorVersion::latest())
                .region(aws_sdk_secretsmanager::config::Region::new(
                    provider.region.clone(),
                ))
                .load()
                .await;
            let timeout = aws_sdk_secretsmanager::config::timeout::TimeoutConfig::builder()
                .connect_timeout(Duration::from_millis(provider.connect_timeout_milliseconds))
                .operation_timeout(Duration::from_millis(
                    provider.operation_timeout_milliseconds,
                ))
                .build();
            let secrets = SecretsClient::from_conf(
                aws_sdk_secretsmanager::Config::from(&shared)
                    .to_builder()
                    .endpoint_url(provider.secrets_endpoint)
                    .region(aws_sdk_secretsmanager::config::Region::new(
                        provider.region.clone(),
                    ))
                    .timeout_config(timeout)
                    .build(),
            );
            let kms_timeout = aws_sdk_kms::config::timeout::TimeoutConfig::builder()
                .connect_timeout(Duration::from_millis(provider.connect_timeout_milliseconds))
                .operation_timeout(Duration::from_millis(
                    provider.operation_timeout_milliseconds,
                ))
                .build();
            let kms_client = KmsClient::from_conf(
                aws_sdk_kms::Config::from(&shared)
                    .to_builder()
                    .endpoint_url(provider.kms_endpoint)
                    .region(aws_sdk_kms::config::Region::new(provider.region))
                    .timeout_config(kms_timeout)
                    .build(),
            );
            let provider_id = provider.provider_id;
            let kms_key_arn: Arc<str> = Arc::from(provider.kms_key_arn);
            let secret_arn_prefix: Arc<str> = Arc::from(provider.secret_arn_prefix);
            let kms_binding = Arc::new(AwsKmsBinding {
                client: kms_client,
                key_id: Arc::clone(&kms_key_arn),
                observer: Arc::clone(&observer),
            });
            kms.insert(provider_id.clone(), Arc::clone(&kms_binding));
            readiness.push(AwsProviderReadiness {
                secrets: secrets.clone(),
                readiness_secret_id: Arc::from(provider.readiness_secret_id),
                kms: kms_binding,
                observer: Arc::clone(&observer),
            });
            installed.push(Arc::new(AwsSecretsManagerProvider {
                provider_id,
                client: secrets,
                kms_key_arn,
                secret_arn_prefix,
                secret_name_prefix: Arc::from(provider.secret_name_prefix),
                observer: Arc::clone(&observer),
            }));
        }
        let providers = InstalledSecretProviderCatalog::new(installed)
            .map_err(|_| AwsSecretProviderConfigError::InvalidCatalog)?;
        let kms = Arc::new(AwsSecretReferenceKms { bindings: kms });
        Ok(Self {
            providers,
            sealer: Arc::clone(&kms) as Arc<dyn SecretReferenceSealer>,
            unsealer: kms as Arc<dyn SecretReferenceUnsealer>,
            readiness,
        })
    }

    pub fn into_components(
        self,
    ) -> (
        Arc<dyn SecretReferenceSealer>,
        Arc<dyn SecretReferenceUnsealer>,
        InstalledSecretProviderCatalog,
    ) {
        (self.sealer, self.unsealer, self.providers)
    }

    pub(super) fn cloned_components(
        &self,
    ) -> (
        Arc<dyn SecretReferenceSealer>,
        Arc<dyn SecretReferenceUnsealer>,
        InstalledSecretProviderCatalog,
    ) {
        (
            Arc::clone(&self.sealer),
            Arc::clone(&self.unsealer),
            self.providers.clone(),
        )
    }

    pub async fn check_readiness(&self) -> Result<(), AwsSecretProviderReadinessError> {
        for provider in &self.readiness {
            provider.check().await?;
        }
        Ok(())
    }
}

struct AwsProviderReadiness {
    secrets: SecretsClient,
    readiness_secret_id: Arc<str>,
    kms: Arc<AwsKmsBinding>,
    observer: Arc<dyn SecretExternalDependencyObserver>,
}

impl AwsProviderReadiness {
    async fn check(&self) -> Result<(), AwsSecretProviderReadinessError> {
        let result = self
            .secrets
            .describe_secret()
            .secret_id(&*self.readiness_secret_id)
            .send()
            .await;
        observe_external(
            &self.observer,
            SecretExternalDependency::Secret,
            result.is_ok(),
        );
        let secret =
            result.map_err(|_| AwsSecretProviderReadinessError::SecretsManagerUnavailable)?;
        if secret.arn() != Some(&*self.readiness_secret_id) || secret.deleted_date().is_some() {
            return Err(AwsSecretProviderReadinessError::SecretsManagerInvalidEvidence);
        }
        let result = self
            .kms
            .client
            .describe_key()
            .key_id(&*self.kms.key_id)
            .send()
            .await;
        observe_external(
            &self.observer,
            SecretExternalDependency::Kms,
            result.is_ok(),
        );
        let key = result.map_err(|_| AwsSecretProviderReadinessError::KmsUnavailable)?;
        let metadata = key
            .key_metadata()
            .ok_or(AwsSecretProviderReadinessError::KmsInvalidEvidence)?;
        if metadata.arn() != Some(&*self.kms.key_id)
            || !metadata.enabled()
            || metadata.key_state() != Some(&aws_sdk_kms::types::KeyState::Enabled)
            || metadata.key_usage() != Some(&aws_sdk_kms::types::KeyUsageType::EncryptDecrypt)
            || metadata.key_spec() != Some(&aws_sdk_kms::types::KeySpec::SymmetricDefault)
        {
            return Err(AwsSecretProviderReadinessError::KmsInvalidEvidence);
        }
        Ok(())
    }
}

struct AwsKmsBinding {
    client: KmsClient,
    key_id: Arc<str>,
    observer: Arc<dyn SecretExternalDependencyObserver>,
}

struct AwsSecretReferenceKms {
    bindings: HashMap<ResourceId, Arc<AwsKmsBinding>>,
}

#[async_trait]
impl SecretReferenceSealer for AwsSecretReferenceKms {
    async fn seal(
        &self,
        tenant_id: &ResourceId,
        secret_binding_id: &ResourceId,
        provider_id: &ResourceId,
        binding_generation: u64,
        reference: &OpaqueSecretReference,
    ) -> Result<SealedSecretReference, SecretReferenceSealError> {
        validate_seal_identity(
            tenant_id,
            secret_binding_id,
            provider_id,
            binding_generation,
        )?;
        let binding = self
            .bindings
            .get(provider_id)
            .ok_or(SecretReferenceSealError::Rejected)?;
        let context = kms_context(
            tenant_id,
            secret_binding_id,
            provider_id,
            binding_generation,
            &binding.key_id,
        );
        let result = binding
            .client
            .encrypt()
            .key_id(&*binding.key_id)
            .plaintext(KmsBlob::new(reference.expose()))
            .set_encryption_context(Some(context))
            .encryption_algorithm(EncryptionAlgorithmSpec::SymmetricDefault)
            .send()
            .await;
        observe_external(
            &binding.observer,
            SecretExternalDependency::Kms,
            result.is_ok(),
        );
        let output = result.map_err(|error| match error.as_service_error() {
            Some(service)
                if service.is_disabled_exception()
                    || service.is_kms_invalid_state_exception()
                    || service.is_not_found_exception() =>
            {
                SecretReferenceSealError::Rejected
            }
            _ => SecretReferenceSealError::Unavailable,
        })?;
        if output.key_id() != Some(&*binding.key_id)
            || output.encryption_algorithm() != Some(&EncryptionAlgorithmSpec::SymmetricDefault)
        {
            return Err(SecretReferenceSealError::InvalidEvidence);
        }
        let ciphertext = output
            .ciphertext_blob
            .ok_or(SecretReferenceSealError::InvalidEvidence)?
            .into_inner();
        Ok(SealedSecretReference {
            encrypted_reference: EncryptedOpaqueReference::new(ciphertext)
                .map_err(|_| SecretReferenceSealError::InvalidEvidence)?,
            key_id: binding.key_id.to_string(),
            reference_digest: digest(reference.expose()),
        })
    }
}

#[async_trait]
impl SecretReferenceUnsealer for AwsSecretReferenceKms {
    async fn unseal(
        &self,
        binding_record: &SecretBindingResolutionRecord,
    ) -> Result<OpaqueSecretReference, SecretReferenceUnsealError> {
        binding_record
            .validate()
            .map_err(|_| SecretReferenceUnsealError::InvalidEvidence)?;
        let binding = self
            .bindings
            .get(&binding_record.provider_id)
            .ok_or(SecretReferenceUnsealError::Rejected)?;
        if binding_record.key_id != *binding.key_id {
            return Err(SecretReferenceUnsealError::Rejected);
        }
        let context = kms_context(
            &binding_record.tenant_id,
            &binding_record.secret_binding_id,
            &binding_record.provider_id,
            binding_record.generation,
            &binding.key_id,
        );
        let result = binding
            .client
            .decrypt()
            .key_id(&*binding.key_id)
            .ciphertext_blob(KmsBlob::new(binding_record.encrypted_reference.as_bytes()))
            .set_encryption_context(Some(context))
            .encryption_algorithm(EncryptionAlgorithmSpec::SymmetricDefault)
            .send()
            .await;
        observe_external(
            &binding.observer,
            SecretExternalDependency::Kms,
            result.is_ok(),
        );
        let output = result.map_err(|error| match error.as_service_error() {
            Some(service)
                if service.is_incorrect_key_exception()
                    || service.is_invalid_ciphertext_exception()
                    || service.is_invalid_grant_token_exception()
                    || service.is_invalid_key_usage_exception()
                    || service.is_not_found_exception()
                    || service.is_disabled_exception()
                    || service.is_kms_invalid_state_exception() =>
            {
                SecretReferenceUnsealError::Rejected
            }
            _ => SecretReferenceUnsealError::Unavailable,
        })?;
        if output.key_id() != Some(&*binding.key_id)
            || output.encryption_algorithm() != Some(&EncryptionAlgorithmSpec::SymmetricDefault)
            || output.ciphertext_for_recipient().is_some()
        {
            zero_optional_kms_plaintext(output.plaintext);
            return Err(SecretReferenceUnsealError::InvalidEvidence);
        }
        let plaintext = output
            .plaintext
            .ok_or(SecretReferenceUnsealError::InvalidEvidence)?
            .into_inner();
        OpaqueSecretReference::new(plaintext)
            .map_err(|_| SecretReferenceUnsealError::InvalidEvidence)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AwsSecretMaterialKind {
    ModelCredential,
    Raw,
    McpOAuthPkce,
    McpOAuthToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AwsOpaqueSecretReference {
    schema_version: u32,
    secret_id: String,
    version_id: Option<String>,
    version_stage: Option<String>,
    material_kind: AwsSecretMaterialKind,
    dedicated_version_secret: bool,
}

impl AwsOpaqueSecretReference {
    fn validate(&self) -> Result<(), SecretProviderResolveError> {
        let selector_count =
            usize::from(self.version_id.is_some()) + usize::from(self.version_stage.is_some());
        if self.schema_version != 1
            || (!valid_secret_arn(&self.secret_id) && !valid_secret_name(&self.secret_id))
            || selector_count != 1
            || self
                .version_id
                .as_ref()
                .is_some_and(|version| !valid_version_id(version))
            || self
                .version_stage
                .as_ref()
                .is_some_and(|stage| !valid_version_stage(stage))
            || (self.dedicated_version_secret && self.version_id.is_none())
            || (!self.dedicated_version_secret
                && !matches!(self.material_kind, AwsSecretMaterialKind::Raw))
        {
            return Err(SecretProviderResolveError::Rejected);
        }
        Ok(())
    }

    fn decode(reference: &OpaqueSecretReference) -> Result<Self, SecretProviderResolveError> {
        let value = parse_strict_json(reference.expose(), reference_json_limits())
            .map_err(|_| SecretProviderResolveError::Rejected)?;
        let parsed: Self =
            serde_json::from_value(value).map_err(|_| SecretProviderResolveError::Rejected)?;
        parsed.validate()?;
        if !valid_secret_arn(&parsed.secret_id) {
            return Err(SecretProviderResolveError::Rejected);
        }
        Ok(parsed)
    }

    fn encode(&self) -> Result<OpaqueSecretReference, SecretProviderPrepareError> {
        self.validate()
            .map_err(|_| SecretProviderPrepareError::Rejected)?;
        if !valid_secret_arn(&self.secret_id) {
            return Err(SecretProviderPrepareError::Rejected);
        }
        let bytes = serde_jcs::to_vec(self).map_err(|_| SecretProviderPrepareError::Rejected)?;
        OpaqueSecretReference::new(bytes).map_err(|_| SecretProviderPrepareError::Rejected)
    }
}

struct AwsSecretsManagerProvider {
    provider_id: ResourceId,
    client: SecretsClient,
    kms_key_arn: Arc<str>,
    secret_arn_prefix: Arc<str>,
    secret_name_prefix: Arc<str>,
    observer: Arc<dyn SecretExternalDependencyObserver>,
}

impl fmt::Debug for AwsSecretsManagerProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AwsSecretsManagerProvider")
            .field("provider_id", &self.provider_id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl InstalledSecretProvider for AwsSecretsManagerProvider {
    fn provider_id(&self) -> &ResourceId {
        &self.provider_id
    }

    async fn resolve(
        &self,
        tenant_id: &ResourceId,
        reference: &OpaqueSecretReference,
        policy: &SecretResolutionPolicy,
    ) -> Result<ProviderSecretMaterial, SecretProviderResolveError> {
        if tenant_id.kind() != ResourceKind::Tenant {
            return Err(SecretProviderResolveError::Rejected);
        }
        let reference = AwsOpaqueSecretReference::decode(reference)?;
        if !reference.secret_id.starts_with(&*self.secret_arn_prefix) {
            return Err(SecretProviderResolveError::Rejected);
        }
        validate_reference_policy(&reference, policy)?;
        let output = self.get_secret(&reference).await?;
        let version_id = output
            .version_id()
            .filter(|version| valid_version_id(version))
            .ok_or(SecretProviderResolveError::InvalidEvidence)?
            .to_owned();
        if reference
            .version_id
            .as_deref()
            .is_some_and(|expected| expected != version_id)
        {
            return Err(SecretProviderResolveError::InvalidEvidence);
        }
        let mut bytes = exact_secret_bytes(output)?;
        let material = match reference.material_kind {
            AwsSecretMaterialKind::Raw => std::mem::take(&mut bytes),
            AwsSecretMaterialKind::ModelCredential => {
                let envelope = decode_prepared(&bytes);
                bytes.fill(0);
                let PreparedSecretEnvelope::ModelCredential(value) = envelope? else {
                    return Err(SecretProviderResolveError::InvalidEvidence);
                };
                if value.schema_version != 1
                    || !value.identity.validate()
                    || value.identity.tenant_id != *tenant_id
                    || value.identity.provider_id != self.provider_id
                {
                    return Err(SecretProviderResolveError::InvalidEvidence);
                }
                let key =
                    insight_platform_contracts::SensitiveModelApiKey::new(value.api_key.decode()?)
                        .map_err(|_| SecretProviderResolveError::InvalidEvidence)?;
                key.expose().to_vec()
            }
            AwsSecretMaterialKind::McpOAuthPkce => {
                let envelope: PreparedSecretEnvelope = decode_prepared(&bytes)?;
                bytes.fill(0);
                match envelope {
                    PreparedSecretEnvelope::McpOAuthPkce(value) => value.pkce_verifier.decode()?,
                    PreparedSecretEnvelope::McpOAuthToken(_)
                    | PreparedSecretEnvelope::ModelCredential(_) => {
                        return Err(SecretProviderResolveError::InvalidEvidence)
                    }
                }
            }
            AwsSecretMaterialKind::McpOAuthToken => {
                let envelope: PreparedSecretEnvelope = decode_prepared(&bytes)?;
                bytes.fill(0);
                match envelope {
                    PreparedSecretEnvelope::McpOAuthToken(value) => value.access_token.decode()?,
                    PreparedSecretEnvelope::McpOAuthPkce(_)
                    | PreparedSecretEnvelope::ModelCredential(_) => {
                        return Err(SecretProviderResolveError::InvalidEvidence)
                    }
                }
            }
        };
        ProviderSecretMaterial::new(digest(version_id.as_bytes()), material)
    }

    async fn delete_exact(
        &self,
        tenant_id: &ResourceId,
        reference: &OpaqueSecretReference,
        policy: &SecretResolutionPolicy,
    ) -> Result<SecretProviderDeleteDisposition, SecretProviderDeleteError> {
        if tenant_id.kind() != ResourceKind::Tenant
            || !matches!(policy, SecretResolutionPolicy::Pinned { .. })
        {
            return Err(SecretProviderDeleteError::Rejected);
        }
        let reference = AwsOpaqueSecretReference::decode(reference)
            .map_err(|_| SecretProviderDeleteError::Rejected)?;
        if !reference.dedicated_version_secret
            || !reference.secret_id.starts_with(&*self.secret_arn_prefix)
        {
            return Err(SecretProviderDeleteError::Rejected);
        }
        validate_reference_policy(&reference, policy)
            .map_err(|_| SecretProviderDeleteError::Rejected)?;
        let output = match self.get_secret(&reference).await {
            Ok(output) => output,
            Err(SecretProviderResolveError::NotFound) => {
                return Ok(SecretProviderDeleteDisposition::AlreadyAbsent)
            }
            Err(
                SecretProviderResolveError::Rejected | SecretProviderResolveError::InvalidEvidence,
            ) => {
                if self.is_scheduled_for_deletion(&reference.secret_id).await? {
                    return Ok(SecretProviderDeleteDisposition::AlreadyAbsent);
                }
                return Err(SecretProviderDeleteError::Rejected);
            }
            Err(SecretProviderResolveError::Unavailable) => {
                return Err(SecretProviderDeleteError::Unavailable)
            }
        };
        if output.version_id() != reference.version_id.as_deref() {
            return Err(SecretProviderDeleteError::Rejected);
        }
        let result = self
            .client
            .delete_secret()
            .secret_id(&reference.secret_id)
            .recovery_window_in_days(7)
            .send()
            .await;
        observe_external(
            &self.observer,
            SecretExternalDependency::Secret,
            result.is_ok(),
        );
        result.map_err(|error| match error.as_service_error() {
            Some(service) if service.is_resource_not_found_exception() => {
                SecretProviderDeleteError::OutcomeUncertain
            }
            Some(service)
                if service.is_invalid_parameter_exception()
                    || service.is_invalid_request_exception() =>
            {
                SecretProviderDeleteError::Rejected
            }
            _ => SecretProviderDeleteError::OutcomeUncertain,
        })?;
        Ok(SecretProviderDeleteDisposition::Deleted)
    }

    async fn prepare_or_load_model_credential(
        &self,
        request: &insight_platform_contracts::ModelCredentialImportAuthorizationV1,
        permit: &insight_platform_contracts::ModelCredentialImportPermitV1,
        key: &insight_platform_contracts::SensitiveModelApiKey,
    ) -> Result<ProviderPreparedSecretVersion, SecretProviderPrepareError> {
        self.prepare_model_credential(request, permit, key).await
    }

    async fn prepare_or_load_mcp_oauth_transient(
        &self,
        candidate: NewMcpOAuthTransientSecretBundle,
    ) -> Result<ProviderStoredMcpOAuthTransientSecretBundle, SecretProviderPrepareError> {
        candidate
            .validate()
            .map_err(|_| SecretProviderPrepareError::Rejected)?;
        if candidate.pkce_secret_provider_id != self.provider_id {
            return Err(SecretProviderPrepareError::Rejected);
        }
        let secret_name =
            self.prepared_secret_name(&candidate.tenant_id, &candidate.preparation_digest);
        let version_id = deterministic_version_id(&candidate.preparation_digest)?;
        let proposed = PreparedSecretEnvelope::McpOAuthPkce(PreparedMcpOAuthPkce {
            schema_version: 1,
            tenant_id: candidate.tenant_id.clone(),
            task_id: candidate.task_id.clone(),
            authorization_binding_id: candidate.authorization_binding_id.clone(),
            mcp_deployment: candidate.mcp_deployment.clone(),
            preparation_digest: candidate.preparation_digest.clone(),
            callback_binding_digest: candidate.callback_binding_digest.clone(),
            expires_at: candidate.expires_at,
            state: SecretBytes::encode(candidate.state.as_bytes()),
            nonce: SecretBytes::encode(candidate.nonce.as_bytes()),
            pkce_verifier: SecretBytes::encode(candidate.pkce_verifier.expose()),
        });
        let stored_entry = self
            .create_or_load(&secret_name, &version_id, proposed, None)
            .await?;
        let secret_id = stored_secret_id(&stored_entry)?.to_owned();
        if stored_entry.version_id != version_id {
            return Err(SecretProviderPrepareError::Rejected);
        }
        let PreparedSecretEnvelope::McpOAuthPkce(stored) = stored_entry.envelope else {
            return Err(SecretProviderPrepareError::Rejected);
        };
        stored.validate_for_transient(&candidate)?;
        let binding_id =
            deterministic_binding_id(&candidate.task_id, &candidate.preparation_digest)?;
        let version_digest = digest(version_id.as_bytes());
        let exact = exact_binding(
            binding_id.clone(),
            self.provider_id.clone(),
            MCP_OAUTH_PKCE_SECRET_PURPOSE
                .parse()
                .map_err(|_| SecretProviderPrepareError::Rejected)?,
            version_digest.clone(),
        )?;
        let reference = AwsOpaqueSecretReference {
            schema_version: 1,
            secret_id,
            version_id: Some(version_id),
            version_stage: None,
            material_kind: AwsSecretMaterialKind::McpOAuthPkce,
            dedicated_version_secret: true,
        }
        .encode()?;
        let evidence =
            prepared_storage_evidence(&candidate.preparation_digest, &reference, &version_digest)?;
        Ok(ProviderStoredMcpOAuthTransientSecretBundle {
            stored: StoredMcpOAuthTransientSecretBundle {
                schema_version: 1,
                tenant_id: stored.tenant_id,
                task_id: stored.task_id,
                authorization_binding_id: stored.authorization_binding_id,
                mcp_deployment: stored.mcp_deployment,
                pkce_secret_provider_id: self.provider_id.clone(),
                preparation_digest: stored.preparation_digest,
                callback_binding_digest: stored.callback_binding_digest,
                expires_at: stored.expires_at,
                state: SensitiveOAuthValue::from_decoded(
                    stored.state.decode().map_err(map_resolve_to_prepare)?,
                    insight_platform_mcp_host::MAX_MCP_OAUTH_STATE_BYTES,
                )
                .map_err(|_| SecretProviderPrepareError::Rejected)?,
                nonce: SensitiveMcpOAuthNonce::new(
                    stored.nonce.decode().map_err(map_resolve_to_prepare)?,
                )
                .map_err(|_| SecretProviderPrepareError::Rejected)?,
                pkce_verifier: SensitiveMcpOAuthPkceVerifier::new(
                    stored
                        .pkce_verifier
                        .decode()
                        .map_err(map_resolve_to_prepare)?,
                )
                .map_err(|_| SecretProviderPrepareError::Rejected)?,
                pkce_secret_binding: exact,
                storage_evidence_digest: evidence.clone(),
            },
            prepared_secret: ProviderPreparedSecretVersion {
                secret_binding_id: binding_id,
                provider_id: self.provider_id.clone(),
                opaque_reference: reference,
                opaque_version_identity_digest: version_digest,
                storage_evidence_digest: evidence,
            },
        })
    }

    async fn load_prepared_mcp_oauth_token(
        &self,
        preparation: &McpOAuthTokenPreparation,
    ) -> Result<Option<ProviderStoredMcpOAuthTokenSecret>, SecretProviderPrepareError> {
        preparation
            .validate_at(Utc::now())
            .map_err(|_| SecretProviderPrepareError::Rejected)?;
        if preparation.token_secret_provider_id != self.provider_id {
            return Err(SecretProviderPrepareError::Rejected);
        }
        let name =
            self.prepared_secret_name(&preparation.tenant_id, &preparation.preparation_digest);
        let version = deterministic_version_id(&preparation.preparation_digest)?;
        let Some(stored) = self.load_prepared(&name, &version).await? else {
            return Ok(None);
        };
        self.token_result(preparation, version, stored).map(Some)
    }

    async fn prepare_or_load_mcp_oauth_token(
        &self,
        preparation: &McpOAuthTokenPreparation,
        tokens: &McpOAuthTokenSet,
        verified: &VerifiedMcpOAuthToken,
    ) -> Result<ProviderStoredMcpOAuthTokenSecret, SecretProviderPrepareError> {
        preparation
            .validate_at(Utc::now())
            .map_err(|_| SecretProviderPrepareError::Rejected)?;
        if preparation.token_secret_provider_id != self.provider_id {
            return Err(SecretProviderPrepareError::Rejected);
        }
        let name =
            self.prepared_secret_name(&preparation.tenant_id, &preparation.preparation_digest);
        let version = deterministic_version_id(&preparation.preparation_digest)?;
        let proposed = PreparedSecretEnvelope::McpOAuthToken(PreparedMcpOAuthToken {
            schema_version: 1,
            preparation_digest: preparation.preparation_digest.clone(),
            access_token: SecretBytes::encode(tokens.access_token.expose()),
            refresh_token: tokens
                .refresh_token
                .as_ref()
                .map(|value| SecretBytes::encode(value.expose())),
            id_token: tokens
                .id_token
                .as_ref()
                .map(|value| SecretBytes::encode(value.expose())),
            granted_scopes: verified.granted_scopes.clone(),
            audience_identity_digest: verified.audience_identity_digest.clone(),
            issuer_identity_digest: verified.issuer_identity_digest.clone(),
            subject_identity_digest: verified.subject_identity_digest.clone(),
            verification_evidence_digest: verified.verification_evidence_digest.clone(),
            expires_at: verified.expires_at,
        });
        let stored = self.create_or_load(&name, &version, proposed, None).await?;
        self.token_result(preparation, version, stored)
    }
}

impl AwsSecretsManagerProvider {
    fn prepared_secret_name(&self, tenant: &ResourceId, preparation: &Sha256Digest) -> String {
        format!(
            "{}/{}/{}",
            self.secret_name_prefix,
            tenant.uuid(),
            preparation.as_str().trim_start_matches("sha256:")
        )
    }

    async fn get_secret(
        &self,
        reference: &AwsOpaqueSecretReference,
    ) -> Result<
        aws_sdk_secretsmanager::operation::get_secret_value::GetSecretValueOutput,
        SecretProviderResolveError,
    > {
        let mut request = self
            .client
            .get_secret_value()
            .secret_id(&reference.secret_id);
        if let Some(version) = &reference.version_id {
            request = request.version_id(version);
        }
        if let Some(stage) = &reference.version_stage {
            request = request.version_stage(stage);
        }
        let result = request.send().await;
        observe_external(
            &self.observer,
            SecretExternalDependency::Secret,
            result.is_ok(),
        );
        let output = result.map_err(|error| match error.as_service_error() {
            Some(service) if service.is_resource_not_found_exception() => {
                SecretProviderResolveError::NotFound
            }
            Some(service)
                if service.is_invalid_parameter_exception()
                    || service.is_invalid_request_exception() =>
            {
                SecretProviderResolveError::Rejected
            }
            _ => SecretProviderResolveError::Unavailable,
        })?;
        if !secret_identity_matches(&reference.secret_id, &output) {
            return Err(SecretProviderResolveError::InvalidEvidence);
        }
        Ok(output)
    }

    async fn is_scheduled_for_deletion(
        &self,
        secret_id: &str,
    ) -> Result<bool, SecretProviderDeleteError> {
        let result = self
            .client
            .describe_secret()
            .secret_id(secret_id)
            .send()
            .await;
        observe_external(
            &self.observer,
            SecretExternalDependency::Secret,
            result.is_ok(),
        );
        result
            .map(|output| output.deleted_date().is_some())
            .map_err(|error| match error.as_service_error() {
                Some(service) if service.is_resource_not_found_exception() => {
                    SecretProviderDeleteError::OutcomeUncertain
                }
                Some(service) if service.is_invalid_parameter_exception() => {
                    SecretProviderDeleteError::Rejected
                }
                _ => SecretProviderDeleteError::Unavailable,
            })
    }

    async fn load_prepared(
        &self,
        name: &str,
        version: &str,
    ) -> Result<Option<StoredPreparedSecret>, SecretProviderPrepareError> {
        let reference = AwsOpaqueSecretReference {
            schema_version: 1,
            secret_id: name.to_owned(),
            version_id: Some(version.to_owned()),
            version_stage: None,
            material_kind: AwsSecretMaterialKind::Raw,
            dedicated_version_secret: true,
        };
        match self.get_secret(&reference).await {
            Ok(output) => {
                let secret_id = output
                    .arn()
                    .filter(|value| value.starts_with(&*self.secret_arn_prefix))
                    .ok_or(SecretProviderPrepareError::Rejected)?
                    .to_owned();
                if output.name() != Some(name) || output.version_id() != Some(version) {
                    return Err(SecretProviderPrepareError::Rejected);
                }
                let mut bytes = exact_secret_bytes(output).map_err(map_resolve_to_prepare)?;
                let envelope = decode_prepared(&bytes).map_err(map_resolve_to_prepare);
                bytes.fill(0);
                let envelope = envelope?;
                Ok(Some(StoredPreparedSecret {
                    secret_id,
                    version_id: version.to_owned(),
                    envelope,
                }))
            }
            Err(SecretProviderResolveError::NotFound) => Ok(None),
            Err(error) => Err(map_resolve_to_prepare(error)),
        }
    }

    async fn create_or_load(
        &self,
        name: &str,
        version: &str,
        envelope: PreparedSecretEnvelope,
        import: Option<(
            &insight_platform_contracts::ModelCredentialImportAuthorizationV1,
            &insight_platform_contracts::ModelCredentialImportPermitV1,
        )>,
    ) -> Result<StoredPreparedSecret, SecretProviderPrepareError> {
        check_import_permit(import)?;
        if let Some(existing) = self.load_prepared(name, version).await? {
            return Ok(existing);
        }
        let mut bytes = encode_prepared(&envelope)?;
        if let Err(error) = check_import_permit(import) {
            bytes.fill(0);
            return Err(error);
        }
        let result = self
            .client
            .create_secret()
            .name(name)
            .client_request_token(version)
            .kms_key_id(&*self.kms_key_arn)
            .description("Insight Platform prepared credential")
            .secret_binary(SecretBlob::new(std::mem::take(&mut bytes)))
            .send()
            .await;
        observe_external(
            &self.observer,
            SecretExternalDependency::Secret,
            result.is_ok(),
        );
        bytes.fill(0);
        match result {
            Ok(output) => {
                let secret_id = output
                    .arn()
                    .filter(|value| value.starts_with(&*self.secret_arn_prefix))
                    .ok_or(SecretProviderPrepareError::WriteUncertain)?
                    .to_owned();
                if output.name() != Some(name) || output.version_id() != Some(version) {
                    return Err(SecretProviderPrepareError::WriteUncertain);
                }
                Ok(StoredPreparedSecret {
                    secret_id,
                    version_id: version.to_owned(),
                    envelope,
                })
            }
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|service| service.is_resource_exists_exception()) =>
            {
                check_import_permit(import)?;
                self.load_prepared(name, version)
                    .await?
                    .ok_or(SecretProviderPrepareError::WriteUncertain)
            }
            Err(error)
                if error.as_service_error().is_some_and(|service| {
                    service.is_invalid_parameter_exception()
                        || service.is_invalid_request_exception()
                }) =>
            {
                Err(SecretProviderPrepareError::Rejected)
            }
            Err(_) if import.is_some() => {
                // A failed write response remains uncertain when its readback deadline expires.
                check_import_permit(import)
                    .map_err(|_| SecretProviderPrepareError::WriteUncertain)?;
                self.load_prepared(name, version)
                    .await
                    .map_err(|_| SecretProviderPrepareError::WriteUncertain)?
                    .ok_or(SecretProviderPrepareError::WriteUncertain)
            }
            Err(_) => Err(SecretProviderPrepareError::WriteUncertain),
        }
    }

    fn token_result(
        &self,
        preparation: &McpOAuthTokenPreparation,
        version: String,
        stored: StoredPreparedSecret,
    ) -> Result<ProviderStoredMcpOAuthTokenSecret, SecretProviderPrepareError> {
        let secret_id = stored_secret_id(&stored)?.to_owned();
        if stored.version_id != version {
            return Err(SecretProviderPrepareError::Rejected);
        }
        let PreparedSecretEnvelope::McpOAuthToken(token) = stored.envelope else {
            return Err(SecretProviderPrepareError::Rejected);
        };
        token.validate_for(preparation)?;
        let version_digest = digest(version.as_bytes());
        let binding_id =
            deterministic_binding_id(&preparation.task_id, &preparation.preparation_digest)?;
        let exact = exact_binding(
            binding_id.clone(),
            self.provider_id.clone(),
            preparation.token_credential_purpose.clone(),
            version_digest.clone(),
        )?;
        let reference = AwsOpaqueSecretReference {
            schema_version: 1,
            secret_id,
            version_id: Some(version),
            version_stage: None,
            material_kind: AwsSecretMaterialKind::McpOAuthToken,
            dedicated_version_secret: true,
        }
        .encode()?;
        let evidence = prepared_storage_evidence(
            &preparation.preparation_digest,
            &reference,
            &version_digest,
        )?;
        Ok(ProviderStoredMcpOAuthTokenSecret {
            stored: StoredMcpOAuthTokenSecret {
                schema_version: 1,
                preparation_digest: token.preparation_digest,
                token_secret_binding: exact,
                granted_scopes: token.granted_scopes,
                audience_identity_digest: token.audience_identity_digest,
                issuer_identity_digest: token.issuer_identity_digest,
                subject_identity_digest: token.subject_identity_digest,
                verification_evidence_digest: token.verification_evidence_digest,
                expires_at: token.expires_at,
                storage_evidence_digest: evidence.clone(),
            },
            prepared_secret: ProviderPreparedSecretVersion {
                secret_binding_id: binding_id,
                provider_id: self.provider_id.clone(),
                opaque_reference: reference,
                opaque_version_identity_digest: version_digest,
                storage_evidence_digest: evidence,
            },
        })
    }
}

fn observe_external(
    observer: &Arc<dyn SecretExternalDependencyObserver>,
    dependency: SecretExternalDependency,
    success: bool,
) {
    observer.observe(
        dependency,
        if success {
            SecretExternalDependencyOutcome::Success
        } else {
            SecretExternalDependencyOutcome::Failure
        },
    );
}

struct StoredPreparedSecret {
    secret_id: String,
    version_id: String,
    envelope: PreparedSecretEnvelope,
}

impl AwsSecretsManagerProvider {
    async fn prepare_model_credential(
        &self,
        request: &insight_platform_contracts::ModelCredentialImportAuthorizationV1,
        permit: &insight_platform_contracts::ModelCredentialImportPermitV1,
        key: &insight_platform_contracts::SensitiveModelApiKey,
    ) -> Result<ProviderPreparedSecretVersion, SecretProviderPrepareError> {
        check_import_permit(Some((request, permit)))?;
        let identity = &request.identity;
        if identity.provider_id != self.provider_id {
            return Err(SecretProviderPrepareError::Rejected);
        }
        let preparation = identity
            .preparation_digest()
            .map_err(|_| SecretProviderPrepareError::Rejected)?;
        let name = self.prepared_secret_name(&identity.tenant_id, &preparation);
        let version = deterministic_version_id(&preparation)?;
        let proposed = PreparedSecretEnvelope::ModelCredential(PreparedModelCredential {
            schema_version: 1,
            identity: identity.clone(),
            api_key: SecretBytes::encode(key.expose()),
        });
        let stored = self
            .create_or_load(&name, &version, proposed, Some((request, permit)))
            .await?;
        let PreparedSecretEnvelope::ModelCredential(envelope) = &stored.envelope else {
            return Err(SecretProviderPrepareError::Rejected);
        };
        let stored_key = insight_platform_contracts::SensitiveModelApiKey::new(
            envelope.api_key.decode().map_err(map_resolve_to_prepare)?,
        )
        .map_err(|_| SecretProviderPrepareError::Rejected)?;
        if envelope.schema_version != 1
            || envelope.identity != *identity
            || stored_key.expose() != key.expose()
            || stored.version_id != version
        {
            return Err(SecretProviderPrepareError::Rejected);
        }
        let reference = AwsOpaqueSecretReference {
            schema_version: 1,
            secret_id: stored.secret_id.clone(),
            version_id: Some(version.clone()),
            version_stage: None,
            material_kind: AwsSecretMaterialKind::ModelCredential,
            dedicated_version_secret: true,
        }
        .encode()?;
        Ok(ProviderPreparedSecretVersion {
            secret_binding_id: identity
                .secret_binding_id()
                .map_err(|_| SecretProviderPrepareError::Rejected)?,
            provider_id: self.provider_id.clone(),
            opaque_reference: reference,
            opaque_version_identity_digest: digest(version.as_bytes()),
            storage_evidence_digest: digest(
                format!("model_credential_storage_v1:{}:{version}", stored.secret_id).as_bytes(),
            ),
        })
    }
}

fn exact_secret_bytes(
    mut output: aws_sdk_secretsmanager::operation::get_secret_value::GetSecretValueOutput,
) -> Result<Vec<u8>, SecretProviderResolveError> {
    match (output.secret_binary.take(), output.secret_string.take()) {
        (Some(binary), None) if !binary.as_ref().is_empty() => Ok(binary.into_inner()),
        (None, Some(string)) if !string.is_empty() => Ok(string.into_bytes()),
        (binary, string) => {
            if let Some(binary) = binary {
                let mut bytes = binary.into_inner();
                bytes.fill(0);
            }
            if let Some(mut string) = string {
                // SAFETY: the String is dropped immediately after its bytes are cleared.
                unsafe { string.as_bytes_mut().fill(0) };
            }
            Err(SecretProviderResolveError::InvalidEvidence)
        }
    }
}

fn deterministic_version_id(
    preparation: &Sha256Digest,
) -> Result<String, SecretProviderPrepareError> {
    let hex = preparation
        .as_str()
        .strip_prefix("sha256:")
        .ok_or(SecretProviderPrepareError::Rejected)?;
    let bytes = (0..32)
        .map(|index| u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| SecretProviderPrepareError::Rejected)?;
    let mut id = [0_u8; 16];
    id.copy_from_slice(&bytes[..16]);
    id[6] = (id[6] & 0x0f) | 0x40;
    id[8] = (id[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(id).to_string())
}

fn stored_secret_id(stored: &StoredPreparedSecret) -> Result<&str, SecretProviderPrepareError> {
    if stored.secret_id.is_empty() {
        Err(SecretProviderPrepareError::Rejected)
    } else if valid_secret_arn(&stored.secret_id) {
        Ok(&stored.secret_id)
    } else {
        Err(SecretProviderPrepareError::Rejected)
    }
}

fn prepared_storage_evidence(
    preparation: &Sha256Digest,
    reference: &OpaqueSecretReference,
    version: &Sha256Digest,
) -> Result<Sha256Digest, SecretProviderPrepareError> {
    canonical_digest(&serde_json::json!({
        "domain": "aws_secrets_manager_prepared_secret_v1",
        "preparation_digest": preparation,
        "reference_digest": digest(reference.expose()),
        "schema_version": 1,
        "version_identity_digest": version,
    }))
    .map_err(|_| SecretProviderPrepareError::Rejected)?
    .parse()
    .map_err(|_| SecretProviderPrepareError::Rejected)
}

fn validate_reference_policy(
    reference: &AwsOpaqueSecretReference,
    policy: &SecretResolutionPolicy,
) -> Result<(), SecretProviderResolveError> {
    match policy {
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest,
        } => {
            let version = reference
                .version_id
                .as_ref()
                .ok_or(SecretProviderResolveError::Rejected)?;
            if &digest(version.as_bytes()) != opaque_version_identity_digest {
                return Err(SecretProviderResolveError::Rejected);
            }
        }
        SecretResolutionPolicy::FollowProviderRotation { .. } => {
            if reference.version_stage.as_deref() != Some("AWSCURRENT")
                || !matches!(reference.material_kind, AwsSecretMaterialKind::Raw)
            {
                return Err(SecretProviderResolveError::Rejected);
            }
        }
    }
    Ok(())
}

fn zero_optional_kms_plaintext(plaintext: Option<KmsBlob>) {
    if let Some(plaintext) = plaintext {
        let mut bytes = plaintext.into_inner();
        bytes.fill(0);
    }
}

fn reference_json_limits() -> JsonLimits {
    JsonLimits {
        max_bytes: MAX_OPAQUE_SECRET_REFERENCE_BYTES,
        max_depth: 8,
        max_items_per_array: 1,
        max_properties_per_object: 8,
        max_string_bytes: MAX_SECRET_ID_BYTES,
    }
}

fn map_resolve_to_prepare(error: SecretProviderResolveError) -> SecretProviderPrepareError {
    match error {
        SecretProviderResolveError::Unavailable => SecretProviderPrepareError::Unavailable,
        SecretProviderResolveError::NotFound
        | SecretProviderResolveError::Rejected
        | SecretProviderResolveError::InvalidEvidence => SecretProviderPrepareError::Rejected,
    }
}

fn digest(bytes: &[u8]) -> Sha256Digest {
    let hash = Sha256::digest(bytes);
    let hex = hash
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Sha256Digest::from_str(&format!("sha256:{hex}"))
        .expect("SHA-256 output is always a canonical digest")
}

fn valid_https_endpoint(value: &str) -> bool {
    if value.len() > MAX_PROVIDER_ENDPOINT_BYTES {
        return false;
    }
    let Ok(endpoint) = Url::parse(value) else {
        return false;
    };
    endpoint.scheme() == "https"
        && endpoint.host_str().is_some()
        && endpoint.username().is_empty()
        && endpoint.password().is_none()
        && endpoint.query().is_none()
        && endpoint.fragment().is_none()
        && matches!(endpoint.path(), "" | "/")
}

fn valid_region(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn valid_kms_key_arn(value: &str, region: &str) -> bool {
    value.len() <= 255
        && value.starts_with("arn:")
        && value.contains(&format!(":kms:{region}:"))
        && value.contains(":key/")
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn valid_secret_prefix(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SECRET_NAME_PREFIX_BYTES
        && !value.starts_with('/')
        && !value.ends_with('/')
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'/' | b'_' | b'+' | b'=' | b'.' | b'@' | b'-')
        })
}

fn valid_secret_arn(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SECRET_ID_BYTES
        && value.starts_with("arn:")
        && value.contains(":secretsmanager:")
        && value.contains(":secret:")
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn valid_secret_arn_prefix(value: &str, region: &str) -> bool {
    valid_secret_arn(&format!("{value}placeholder"))
        && value.contains(&format!(":secretsmanager:{region}:"))
        && value.contains(":secret:")
        && value.ends_with('/')
        && value
            .split_once(":secret:")
            .is_some_and(|(_, scope)| valid_secret_prefix(scope.trim_end_matches('/')))
}

fn secret_name_in_arn_namespace(name: &str, arn_prefix: &str) -> bool {
    arn_prefix
        .split_once(":secret:")
        .is_some_and(|(_, scope)| name.starts_with(scope.trim_end_matches('/')))
}

fn valid_secret_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && !value.starts_with('/')
        && !value.ends_with('/')
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'/' | b'_' | b'+' | b'=' | b'.' | b'@' | b'-')
        })
}

fn secret_identity_matches(
    requested: &str,
    output: &aws_sdk_secretsmanager::operation::get_secret_value::GetSecretValueOutput,
) -> bool {
    if requested.starts_with("arn:") {
        output.arn() == Some(requested)
    } else {
        valid_secret_name(requested) && output.name() == Some(requested)
    }
}

fn valid_version_id(value: &str) -> bool {
    (32..=MAX_SECRET_VERSION_ID_BYTES).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn valid_version_stage(value: &str) -> bool {
    value == "AWSCURRENT"
}

fn valid_timeouts(connect: u64, operation: u64) -> bool {
    connect > 0 && connect <= operation && operation <= MAX_OPERATION_TIMEOUT_MILLISECONDS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(prefix: &str, suffix: u128) -> ResourceId {
        format!("{prefix}_018f0f6e-7b2a-7000-8000-{suffix:012x}")
            .parse()
            .unwrap()
    }

    fn sha(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn provider() -> AwsSecretProviderConfig {
        let mut provider = AwsSecretProviderConfig {
            schema_version: 1,
            provider_id: id("spr", 1),
            provider_config_digest: sha('0'),
            region: "us-east-1".to_owned(),
            secrets_endpoint: "https://secretsmanager.us-east-1.amazonaws.com".to_owned(),
            kms_endpoint: "https://kms.us-east-1.amazonaws.com".to_owned(),
            kms_key_arn:
                "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000001"
                    .to_owned(),
            secret_arn_prefix:
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:insight/platform/"
                    .to_owned(),
            secret_name_prefix: "insight/platform/prepared".to_owned(),
            readiness_secret_id:
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:insight/platform/readiness-abcdef"
                    .to_owned(),
            connect_timeout_milliseconds: 1_000,
            operation_timeout_milliseconds: 5_000,
        };
        provider.provider_config_digest = provider.calculated_digest().unwrap();
        provider
    }

    #[test]
    fn candidate_catalog_is_exact_closed_and_credential_free() {
        let provider = provider();
        AwsSecretProviderCatalogConfig {
            schema_version: 1,
            providers: vec![provider.clone()],
        }
        .validate()
        .unwrap();
        let mut drifted = provider.clone();
        drifted.secret_name_prefix.push_str("/drift");
        assert_eq!(
            drifted.validate(),
            Err(AwsSecretProviderConfigError::InvalidProvider)
        );
        let duplicate = AwsSecretProviderCatalogConfig {
            schema_version: 1,
            providers: vec![provider.clone(), provider],
        };
        assert_eq!(
            duplicate.validate(),
            Err(AwsSecretProviderConfigError::DuplicateProvider)
        );
        let serialized = serde_json::to_string(&duplicate).unwrap();
        assert!(!serialized.contains("access_key"));
        assert!(!serialized.contains("secret_key"));
        assert!(!serialized.contains("session_token"));
    }

    #[test]
    fn opaque_reference_requires_one_exact_selector_and_pinned_digest() {
        let version = "018f0f6e-7b2a-4000-8000-000000000001".to_owned();
        let reference = AwsOpaqueSecretReference {
            schema_version: 1,
            secret_id: "arn:aws:secretsmanager:us-east-1:123:secret:value-abcdef".to_owned(),
            version_id: Some(version.clone()),
            version_stage: None,
            material_kind: AwsSecretMaterialKind::Raw,
            dedicated_version_secret: false,
        };
        reference.validate().unwrap();
        validate_reference_policy(
            &reference,
            &SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: digest(version.as_bytes()),
            },
        )
        .unwrap();
        let mut ambiguous = reference.clone();
        ambiguous.version_stage = Some("AWSCURRENT".to_owned());
        assert!(ambiguous.validate().is_err());
        assert!(validate_reference_policy(
            &reference,
            &SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: sha('f'),
            },
        )
        .is_err());
    }

    #[test]
    fn deterministic_prepared_identities_are_replay_stable_and_kind_safe() {
        let preparation = sha('a');
        let task = id("int", 9);
        let first = deterministic_version_id(&preparation).unwrap();
        let second = deterministic_version_id(&preparation).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 36);
        let binding = deterministic_binding_id(&task, &preparation).unwrap();
        assert_eq!(binding.kind(), ResourceKind::SecretBinding);
        assert_eq!(
            binding,
            deterministic_binding_id(&task, &preparation).unwrap()
        );
    }

    #[test]
    fn prepared_payload_and_reference_are_strict_and_bounded() {
        let envelope = PreparedSecretEnvelope::McpOAuthToken(PreparedMcpOAuthToken {
            schema_version: 1,
            preparation_digest: sha('a'),
            access_token: SecretBytes::encode(b"access-token"),
            refresh_token: Some(SecretBytes::encode(b"refresh-token")),
            id_token: None,
            granted_scopes: vec!["openid".to_owned()],
            audience_identity_digest: sha('b'),
            issuer_identity_digest: sha('c'),
            subject_identity_digest: sha('d'),
            verification_evidence_digest: sha('e'),
            expires_at: Utc::now() + chrono::Duration::minutes(5),
        });
        let bytes = encode_prepared(&envelope).unwrap();
        let decoded = decode_prepared(&bytes).unwrap();
        assert!(matches!(decoded, PreparedSecretEnvelope::McpOAuthToken(_)));
        let duplicate = br#"{"kind":"mcp_oauth_token","kind":"mcp_oauth_token"}"#;
        assert!(decode_prepared(duplicate).is_err());

        let reference = AwsOpaqueSecretReference {
            schema_version: 1,
            secret_id: "arn:aws:secretsmanager:us-east-1:123:secret:value-abcdef".to_owned(),
            version_id: Some("018f0f6e-7b2a-4000-8000-000000000001".to_owned()),
            version_stage: None,
            material_kind: AwsSecretMaterialKind::McpOAuthToken,
            dedicated_version_secret: true,
        };
        let encoded = reference.encode().unwrap();
        assert_eq!(
            AwsOpaqueSecretReference::decode(&encoded).unwrap(),
            reference
        );
    }

    #[test]
    fn kms_context_binds_every_secret_authority_dimension() {
        let context = kms_context(
            &id("ten", 1),
            &id("sbd", 2),
            &id("spr", 3),
            4,
            "arn:aws:kms:us-east-1:123:key/4",
        );
        assert_eq!(context.len(), 6);
        assert_eq!(
            context.get("binding_generation").map(String::as_str),
            Some("4")
        );
        assert!(context.contains_key("tenant_id"));
        assert!(context.contains_key("secret_binding_id"));
        assert!(context.contains_key("provider_id"));
        assert!(context.contains_key("key_id"));
    }
}

#[cfg(test)]
#[path = "aws_model_credential_tests.rs"]
mod model_credential_tests;
