//! Production AWS-compatible S3/KMS providers installed by an immutable CandidateManifest.
//!
//! Configuration contains no credentials. Both SDK clients use the default credential chain so a
//! deployment can supply short-lived workload identity (for example Kubernetes web identity).

use super::{
    valid_opaque_object_key, ArtifactBrokerConfigurationError, ArtifactObjectBytes,
    ArtifactObjectDeletionReceipt, ArtifactObjectMetadata, ArtifactObjectReferenceUnsealError,
    ArtifactObjectReferenceUnsealer, ArtifactObjectStoreError, DecryptedArtifactObjectReference,
    InstalledArtifactObjectStore, InstalledArtifactObjectStoreCatalog, MAX_ARTIFACT_BROKER_TIMEOUT,
    MAX_INSTALLED_ARTIFACT_STORAGE_BINDINGS,
};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_kms::{primitives::Blob, types::EncryptionAlgorithmSpec, Client as KmsClient};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::Client as S3Client;
use insight_platform_artifacts::{
    AuthorizedArtifactDeleteObject, AuthorizedArtifactObjectRead, AuthorizedArtifactScanObjectRead,
    MAX_ARTIFACT_KMS_KEY_ID_BYTES,
};
use insight_platform_contracts::{canonical_digest, ResourceId, ResourceKind, Sha256Digest};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    sync::Arc,
    time::Duration,
};
use url::Url;

pub const MAX_AWS_ARTIFACT_PROVIDER_ENDPOINT_BYTES: usize = 2_048;
pub const MAX_AWS_ARTIFACT_OBJECT_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_AWS_ARTIFACT_PROVIDER_TIMEOUT_MILLISECONDS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsS3StorageBindingConfig {
    pub schema_version: u32,
    pub storage_binding_digest: Sha256Digest,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub force_path_style: bool,
    pub kms_binding_digest: Sha256Digest,
    pub connect_timeout_milliseconds: u64,
    pub operation_timeout_milliseconds: u64,
    pub maximum_object_bytes: u64,
}

impl AwsS3StorageBindingConfig {
    fn calculated_digest(&self) -> Result<Sha256Digest, AwsArtifactProviderConfigError> {
        canonical_digest(&serde_json::json!({
            "backend": "s3",
            "bucket": self.bucket,
            "connect_timeout_milliseconds": self.connect_timeout_milliseconds,
            "endpoint": self.endpoint,
            "force_path_style": self.force_path_style,
            "kms_binding_digest": self.kms_binding_digest,
            "maximum_object_bytes": self.maximum_object_bytes,
            "operation_timeout_milliseconds": self.operation_timeout_milliseconds,
            "region": self.region,
            "schema_version": self.schema_version,
        }))
        .map_err(|_| AwsArtifactProviderConfigError::InvalidStorageBinding)?
        .parse()
        .map_err(|_| AwsArtifactProviderConfigError::InvalidStorageBinding)
    }

    fn validate(&self) -> Result<(), AwsArtifactProviderConfigError> {
        if self.schema_version != 1
            || !valid_https_service_endpoint(&self.endpoint)
            || !valid_region(&self.region)
            || !valid_bucket(&self.bucket)
            || !valid_timeouts(
                self.connect_timeout_milliseconds,
                self.operation_timeout_milliseconds,
            )
            || self.maximum_object_bytes == 0
            || self.maximum_object_bytes > MAX_AWS_ARTIFACT_OBJECT_BYTES
            || self.calculated_digest()? != self.storage_binding_digest
        {
            return Err(AwsArtifactProviderConfigError::InvalidStorageBinding);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsKmsKeyBindingConfig {
    pub schema_version: u32,
    pub kms_binding_digest: Sha256Digest,
    pub endpoint: String,
    pub region: String,
    pub key_id: String,
    pub connect_timeout_milliseconds: u64,
    pub operation_timeout_milliseconds: u64,
}

impl AwsKmsKeyBindingConfig {
    fn calculated_digest(&self) -> Result<Sha256Digest, AwsArtifactProviderConfigError> {
        canonical_digest(&serde_json::json!({
            "connect_timeout_milliseconds": self.connect_timeout_milliseconds,
            "endpoint": self.endpoint,
            "key_id": self.key_id,
            "operation_timeout_milliseconds": self.operation_timeout_milliseconds,
            "provider": "aws_kms",
            "region": self.region,
            "schema_version": self.schema_version,
        }))
        .map_err(|_| AwsArtifactProviderConfigError::InvalidKmsBinding)?
        .parse()
        .map_err(|_| AwsArtifactProviderConfigError::InvalidKmsBinding)
    }

    fn validate(&self) -> Result<(), AwsArtifactProviderConfigError> {
        if self.schema_version != 1
            || !valid_https_service_endpoint(&self.endpoint)
            || !valid_region(&self.region)
            || !valid_kms_key_arn(&self.key_id, &self.region)
            || !valid_timeouts(
                self.connect_timeout_milliseconds,
                self.operation_timeout_milliseconds,
            )
            || self.calculated_digest()? != self.kms_binding_digest
        {
            return Err(AwsArtifactProviderConfigError::InvalidKmsBinding);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsArtifactProviderCatalogConfig {
    pub schema_version: u32,
    pub write_storage_binding_digest: Sha256Digest,
    pub s3_storage_bindings: Vec<AwsS3StorageBindingConfig>,
    pub kms_key_bindings: Vec<AwsKmsKeyBindingConfig>,
}

impl AwsArtifactProviderCatalogConfig {
    pub fn validate(&self) -> Result<(), AwsArtifactProviderConfigError> {
        if self.schema_version != 1
            || self.s3_storage_bindings.is_empty()
            || self.kms_key_bindings.is_empty()
            || self.s3_storage_bindings.len() > MAX_INSTALLED_ARTIFACT_STORAGE_BINDINGS
            || self.kms_key_bindings.len() > MAX_INSTALLED_ARTIFACT_STORAGE_BINDINGS
            || !self
                .s3_storage_bindings
                .iter()
                .any(|binding| binding.storage_binding_digest == self.write_storage_binding_digest)
        {
            return Err(AwsArtifactProviderConfigError::InvalidCatalog);
        }
        let mut storage_digests = BTreeSet::new();
        let mut referenced_kms_digests = BTreeSet::new();
        for binding in &self.s3_storage_bindings {
            binding.validate()?;
            if !storage_digests.insert(binding.storage_binding_digest.clone()) {
                return Err(AwsArtifactProviderConfigError::DuplicateStorageBinding);
            }
            referenced_kms_digests.insert(binding.kms_binding_digest.clone());
        }
        let mut kms_digests = BTreeSet::new();
        let mut kms_key_ids = BTreeSet::new();
        for binding in &self.kms_key_bindings {
            binding.validate()?;
            if !kms_digests.insert(binding.kms_binding_digest.clone()) {
                return Err(AwsArtifactProviderConfigError::DuplicateKmsBinding);
            }
            if !kms_key_ids.insert(binding.key_id.clone()) {
                return Err(AwsArtifactProviderConfigError::DuplicateKmsKey);
            }
        }
        if referenced_kms_digests != kms_digests {
            return Err(AwsArtifactProviderConfigError::KmsBindingClosureMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwsArtifactProviderConfigError {
    InvalidCatalog,
    InvalidStorageBinding,
    InvalidKmsBinding,
    DuplicateStorageBinding,
    DuplicateKmsBinding,
    DuplicateKmsKey,
    KmsBindingClosureMismatch,
}

/// Installed production providers. Construction performs no object/KMS operation and does not
/// accept credentials; readiness and qualification must exercise the resulting clients.
pub struct AwsArtifactProviderCatalog {
    stores: InstalledArtifactObjectStoreCatalog,
    unsealer: Arc<dyn ArtifactObjectReferenceUnsealer>,
    readiness: Vec<AwsArtifactProviderReadiness>,
    upload: AwsArtifactUploadProvider,
}

impl fmt::Debug for AwsArtifactProviderCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AwsArtifactProviderCatalog")
            .finish_non_exhaustive()
    }
}

impl AwsArtifactProviderCatalog {
    pub async fn install(
        config: AwsArtifactProviderCatalogConfig,
    ) -> Result<Self, AwsArtifactProviderConfigError> {
        config.validate()?;

        let mut kms_by_digest = BTreeMap::new();
        for binding in config.kms_key_bindings {
            let shared = aws_config::defaults(BehaviorVersion::latest())
                .region(aws_sdk_kms::config::Region::new(binding.region.clone()))
                .load()
                .await;
            let timeout = aws_sdk_kms::config::timeout::TimeoutConfig::builder()
                .connect_timeout(Duration::from_millis(binding.connect_timeout_milliseconds))
                .operation_timeout(Duration::from_millis(
                    binding.operation_timeout_milliseconds,
                ))
                .build();
            let client = KmsClient::from_conf(
                aws_sdk_kms::Config::from(&shared)
                    .to_builder()
                    .endpoint_url(binding.endpoint)
                    .region(aws_sdk_kms::config::Region::new(binding.region))
                    .timeout_config(timeout)
                    .build(),
            );
            kms_by_digest.insert(
                binding.kms_binding_digest,
                Arc::new(AwsKmsKeyBinding {
                    client,
                    key_id: Arc::from(binding.key_id),
                }),
            );
        }

        let write_digest = config.write_storage_binding_digest.clone();
        let mut stores: Vec<Arc<dyn InstalledArtifactObjectStore>> = Vec::new();
        let mut kms_by_storage_digest = BTreeMap::new();
        let mut readiness = Vec::new();
        let mut upload = None;
        for binding in config.s3_storage_bindings {
            let kms = kms_by_digest
                .get(&binding.kms_binding_digest)
                .cloned()
                .ok_or(AwsArtifactProviderConfigError::KmsBindingClosureMismatch)?;
            let shared = aws_config::defaults(BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new(binding.region.clone()))
                .load()
                .await;
            let timeout = aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                .connect_timeout(Duration::from_millis(binding.connect_timeout_milliseconds))
                .operation_timeout(Duration::from_millis(
                    binding.operation_timeout_milliseconds,
                ))
                .build();
            let client = S3Client::from_conf(
                aws_sdk_s3::Config::from(&shared)
                    .to_builder()
                    .endpoint_url(binding.endpoint)
                    .region(aws_sdk_s3::config::Region::new(binding.region))
                    .force_path_style(binding.force_path_style)
                    .timeout_config(timeout)
                    .build(),
            );
            let bucket: Arc<str> = Arc::from(binding.bucket);
            readiness.push(AwsArtifactProviderReadiness {
                s3: client.clone(),
                bucket: Arc::clone(&bucket),
                kms: Arc::clone(&kms),
            });
            kms_by_storage_digest.insert(binding.storage_binding_digest.clone(), Arc::clone(&kms));
            if binding.storage_binding_digest == write_digest {
                upload = Some(AwsArtifactUploadProvider {
                    s3: client.clone(),
                    bucket: Arc::clone(&bucket),
                    storage_binding_digest: binding.storage_binding_digest.clone(),
                    maximum_object_bytes: binding.maximum_object_bytes,
                    kms: Arc::clone(&kms),
                });
            }
            stores.push(Arc::new(AwsS3ObjectStore {
                client,
                bucket,
                storage_binding_digest: binding.storage_binding_digest,
                maximum_object_bytes: binding.maximum_object_bytes,
            }));
        }

        let stores = InstalledArtifactObjectStoreCatalog::new(stores)
            .map_err(map_catalog_configuration_error)?;
        Ok(Self {
            stores,
            unsealer: Arc::new(AwsKmsArtifactObjectReferenceUnsealer {
                bindings: kms_by_storage_digest,
            }),
            readiness,
            upload: upload.ok_or(AwsArtifactProviderConfigError::InvalidCatalog)?,
        })
    }

    pub fn into_components(
        self,
    ) -> (
        Arc<dyn ArtifactObjectReferenceUnsealer>,
        InstalledArtifactObjectStoreCatalog,
    ) {
        (self.unsealer, self.stores)
    }

    pub fn into_gateway_provider(self) -> AwsArtifactUploadProvider {
        self.upload
    }

    pub fn into_gateway_components(
        self,
    ) -> (
        AwsArtifactUploadProvider,
        Arc<dyn ArtifactObjectReferenceUnsealer>,
        InstalledArtifactObjectStoreCatalog,
    ) {
        (self.upload, self.unsealer, self.stores)
    }

    pub async fn check_readiness(&self) -> Result<(), AwsArtifactProviderReadinessError> {
        for readiness in &self.readiness {
            readiness.check().await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedAwsArtifactUpload {
    pub upload_url: String,
    pub storage_backend: String,
    pub storage_binding_digest: Sha256Digest,
    pub object_reference_ciphertext: Vec<u8>,
    pub key_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedAwsArtifactUploadEvidence {
    pub object_generation: String,
    pub observed_size_bytes: u64,
    pub backend_evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwsArtifactUploadError {
    InvalidRequest,
    TooLarge,
    StorageUnavailable,
    KmsUnavailable,
    InvalidEvidence,
}

#[derive(Clone)]
pub struct AwsArtifactUploadProvider {
    s3: S3Client,
    bucket: Arc<str>,
    storage_binding_digest: Sha256Digest,
    maximum_object_bytes: u64,
    kms: Arc<AwsKmsKeyBinding>,
}

#[derive(Debug, Clone, Copy)]
pub struct AwsArtifactUploadRequest<'a> {
    pub tenant_id: &'a ResourceId,
    pub artifact_id: &'a ResourceId,
    pub blob_id: &'a ResourceId,
    pub encryption_domain_id: &'a ResourceId,
    pub expected_size_bytes: u64,
    pub declared_media_type: Option<&'a str>,
    pub expires_in: Duration,
}

impl fmt::Debug for AwsArtifactUploadProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AwsArtifactUploadProvider")
            .field("storage_binding_digest", &self.storage_binding_digest)
            .finish_non_exhaustive()
    }
}

impl AwsArtifactUploadProvider {
    pub async fn prepare_upload(
        &self,
        request: AwsArtifactUploadRequest<'_>,
    ) -> Result<PreparedAwsArtifactUpload, AwsArtifactUploadError> {
        if request.tenant_id.kind() != ResourceKind::Tenant
            || request.artifact_id.kind() != ResourceKind::Artifact
            || request.blob_id.kind() != ResourceKind::InternalBlob
            || request.encryption_domain_id.kind() != ResourceKind::EncryptionDomain
            || request.expected_size_bytes == 0
            || request.expected_size_bytes > self.maximum_object_bytes
            || request.expires_in.is_zero()
            || request.expires_in > Duration::from_secs(3_600)
            || request.declared_media_type.is_some_and(|value| {
                value.is_empty()
                    || value.len() > 255
                    || !value.is_ascii()
                    || value.chars().any(char::is_control)
            })
        {
            return Err(AwsArtifactUploadError::InvalidRequest);
        }
        let object_key = format!(
            "v1/{}/{}/{}",
            request.tenant_id, request.artifact_id, request.blob_id
        );
        if !valid_opaque_object_key(&object_key) {
            return Err(AwsArtifactUploadError::InvalidRequest);
        }
        let plaintext = serde_jcs::to_vec(&serde_json::json!({
            "backend": "s3",
            "object_key": object_key,
            "schema_version": 1,
            "storage_binding_digest": self.storage_binding_digest,
        }))
        .map_err(|_| AwsArtifactUploadError::InvalidEvidence)?;
        let encryption_context = object_encryption_context(
            request.tenant_id,
            request.blob_id,
            &self.storage_binding_digest,
            request.encryption_domain_id,
            &self.kms.key_id,
        );
        let encrypted = self
            .kms
            .client
            .encrypt()
            .key_id(&*self.kms.key_id)
            .plaintext(Blob::new(plaintext))
            .set_encryption_context(Some(encryption_context))
            .encryption_algorithm(EncryptionAlgorithmSpec::SymmetricDefault)
            .send()
            .await
            .map_err(|_| AwsArtifactUploadError::KmsUnavailable)?;
        if encrypted.key_id() != Some(&*self.kms.key_id)
            || encrypted.encryption_algorithm() != Some(&EncryptionAlgorithmSpec::SymmetricDefault)
        {
            return Err(AwsArtifactUploadError::InvalidEvidence);
        }
        let ciphertext = encrypted
            .ciphertext_blob
            .ok_or(AwsArtifactUploadError::InvalidEvidence)?
            .into_inner();
        let content_length = i64::try_from(request.expected_size_bytes)
            .map_err(|_| AwsArtifactUploadError::TooLarge)?;
        let mut upload_request = self
            .s3
            .put_object()
            .bucket(&*self.bucket)
            .key(&object_key)
            .content_length(content_length);
        if let Some(media_type) = request.declared_media_type {
            upload_request = upload_request.content_type(media_type);
        }
        let presigned = upload_request
            .presigned(
                PresigningConfig::expires_in(request.expires_in)
                    .map_err(|_| AwsArtifactUploadError::InvalidRequest)?,
            )
            .await
            .map_err(|_| AwsArtifactUploadError::StorageUnavailable)?;
        let upload_url = presigned.uri().to_string();
        if !upload_url.starts_with("https://") || upload_url.len() > 16_384 {
            return Err(AwsArtifactUploadError::InvalidEvidence);
        }
        Ok(PreparedAwsArtifactUpload {
            upload_url,
            storage_backend: "s3".to_owned(),
            storage_binding_digest: self.storage_binding_digest.clone(),
            object_reference_ciphertext: ciphertext,
            key_id: self.kms.key_id.to_string(),
        })
    }

    pub async fn complete_upload(
        &self,
        tenant_id: &ResourceId,
        artifact_id: &ResourceId,
        blob_id: &ResourceId,
        object_generation: &str,
        expected_size_bytes: u64,
    ) -> Result<CompletedAwsArtifactUploadEvidence, AwsArtifactUploadError> {
        self.complete_upload_inner(
            tenant_id,
            artifact_id,
            blob_id,
            Some(object_generation),
            expected_size_bytes,
        )
        .await
    }

    pub async fn complete_current_upload(
        &self,
        tenant_id: &ResourceId,
        artifact_id: &ResourceId,
        blob_id: &ResourceId,
        expected_size_bytes: u64,
    ) -> Result<CompletedAwsArtifactUploadEvidence, AwsArtifactUploadError> {
        self.complete_upload_inner(tenant_id, artifact_id, blob_id, None, expected_size_bytes)
            .await
    }

    async fn complete_upload_inner(
        &self,
        tenant_id: &ResourceId,
        artifact_id: &ResourceId,
        blob_id: &ResourceId,
        expected_generation: Option<&str>,
        expected_size_bytes: u64,
    ) -> Result<CompletedAwsArtifactUploadEvidence, AwsArtifactUploadError> {
        if tenant_id.kind() != ResourceKind::Tenant
            || artifact_id.kind() != ResourceKind::Artifact
            || blob_id.kind() != ResourceKind::InternalBlob
            || expected_generation.is_some_and(|generation| !valid_object_generation(generation))
            || expected_size_bytes > self.maximum_object_bytes
        {
            return Err(AwsArtifactUploadError::InvalidRequest);
        }
        let object_key = format!("v1/{tenant_id}/{artifact_id}/{blob_id}");
        let output = self
            .s3
            .head_object()
            .bucket(&*self.bucket)
            .key(&object_key)
            .set_version_id(expected_generation.map(ToOwned::to_owned))
            .send()
            .await
            .map_err(|_| AwsArtifactUploadError::StorageUnavailable)?;
        let observed_generation = output
            .version_id()
            .filter(|generation| valid_object_generation(generation))
            .ok_or(AwsArtifactUploadError::InvalidEvidence)?;
        if expected_generation.is_some_and(|expected| expected != observed_generation) {
            return Err(AwsArtifactUploadError::InvalidEvidence);
        }
        let metadata = metadata_from_s3(
            output.version_id(),
            output.content_length(),
            observed_generation,
            self.maximum_object_bytes,
        )
        .map_err(|error| match error {
            ArtifactObjectStoreError::TooLarge => AwsArtifactUploadError::TooLarge,
            ArtifactObjectStoreError::Unavailable | ArtifactObjectStoreError::NotFound => {
                AwsArtifactUploadError::StorageUnavailable
            }
            ArtifactObjectStoreError::Rejected | ArtifactObjectStoreError::InvalidEvidence => {
                AwsArtifactUploadError::InvalidEvidence
            }
        })?;
        if metadata.byte_length != expected_size_bytes {
            return Err(AwsArtifactUploadError::InvalidEvidence);
        }
        let backend_evidence_digest = canonical_digest(&serde_json::json!({
            "artifact_id": artifact_id,
            "blob_id": blob_id,
            "kind": "s3_upload_observed",
            "object_generation": observed_generation,
            "schema_version": 1,
            "size_bytes": metadata.byte_length,
            "storage_binding_digest": self.storage_binding_digest,
            "tenant_id": tenant_id,
        }))
        .map_err(|_| AwsArtifactUploadError::InvalidEvidence)?
        .parse()
        .map_err(|_| AwsArtifactUploadError::InvalidEvidence)?;
        Ok(CompletedAwsArtifactUploadEvidence {
            object_generation: observed_generation.to_owned(),
            observed_size_bytes: metadata.byte_length,
            backend_evidence_digest,
        })
    }
}

struct AwsArtifactProviderReadiness {
    s3: S3Client,
    bucket: Arc<str>,
    kms: Arc<AwsKmsKeyBinding>,
}

impl AwsArtifactProviderReadiness {
    async fn check(&self) -> Result<(), AwsArtifactProviderReadinessError> {
        self.s3
            .head_bucket()
            .bucket(&*self.bucket)
            .send()
            .await
            .map_err(|_| AwsArtifactProviderReadinessError::StorageUnavailable)?;
        let output = self
            .kms
            .client
            .describe_key()
            .key_id(&*self.kms.key_id)
            .send()
            .await
            .map_err(|_| AwsArtifactProviderReadinessError::KmsUnavailable)?;
        let metadata = output
            .key_metadata()
            .ok_or(AwsArtifactProviderReadinessError::KmsInvalidEvidence)?;
        if metadata.arn() != Some(&*self.kms.key_id)
            || !metadata.enabled()
            || metadata.key_state() != Some(&aws_sdk_kms::types::KeyState::Enabled)
            || metadata.key_usage() != Some(&aws_sdk_kms::types::KeyUsageType::EncryptDecrypt)
            || metadata.key_spec() != Some(&aws_sdk_kms::types::KeySpec::SymmetricDefault)
        {
            return Err(AwsArtifactProviderReadinessError::KmsInvalidEvidence);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwsArtifactProviderReadinessError {
    StorageUnavailable,
    KmsUnavailable,
    KmsInvalidEvidence,
}

struct AwsS3ObjectStore {
    client: S3Client,
    bucket: Arc<str>,
    storage_binding_digest: Sha256Digest,
    maximum_object_bytes: u64,
}

impl fmt::Debug for AwsS3ObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AwsS3ObjectStore")
            .field("storage_binding_digest", &self.storage_binding_digest)
            .field("bucket", &"[redacted]")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl InstalledArtifactObjectStore for AwsS3ObjectStore {
    fn backend(&self) -> &str {
        "s3"
    }

    fn storage_binding_digest(&self) -> &Sha256Digest {
        &self.storage_binding_digest
    }

    async fn head_exact(
        &self,
        object_key: &str,
        object_generation: &str,
    ) -> Result<ArtifactObjectMetadata, ArtifactObjectStoreError> {
        if !valid_opaque_object_key(object_key) || !valid_object_generation(object_generation) {
            return Err(ArtifactObjectStoreError::Rejected);
        }
        let output = self
            .client
            .head_object()
            .bucket(&*self.bucket)
            .key(object_key)
            .version_id(object_generation)
            .send()
            .await
            .map_err(|error| {
                if error
                    .as_service_error()
                    .is_some_and(|service| service.is_not_found())
                    || error
                        .raw_response()
                        .is_some_and(|response| response.status().as_u16() == 404)
                {
                    ArtifactObjectStoreError::NotFound
                } else {
                    ArtifactObjectStoreError::Unavailable
                }
            })?;
        metadata_from_s3(
            output.version_id(),
            output.content_length(),
            object_generation,
            self.maximum_object_bytes,
        )
    }

    async fn read_exact(
        &self,
        object_key: &str,
        object_generation: &str,
        maximum_bytes: usize,
    ) -> Result<ArtifactObjectBytes, ArtifactObjectStoreError> {
        if !valid_opaque_object_key(object_key)
            || !valid_object_generation(object_generation)
            || maximum_bytes == 0
        {
            return Err(ArtifactObjectStoreError::Rejected);
        }
        let mut output = self
            .client
            .get_object()
            .bucket(&*self.bucket)
            .key(object_key)
            .version_id(object_generation)
            .send()
            .await
            .map_err(|error| {
                if error
                    .as_service_error()
                    .is_some_and(|service| service.is_no_such_key())
                    || error
                        .raw_response()
                        .is_some_and(|response| response.status().as_u16() == 404)
                {
                    ArtifactObjectStoreError::NotFound
                } else {
                    ArtifactObjectStoreError::Unavailable
                }
            })?;
        let provider_limit = usize::try_from(self.maximum_object_bytes)
            .unwrap_or(usize::MAX)
            .min(maximum_bytes);
        let metadata = metadata_from_s3(
            output.version_id(),
            output.content_length(),
            object_generation,
            u64::try_from(provider_limit).unwrap_or(u64::MAX),
        )?;
        let capacity = usize::try_from(metadata.byte_length)
            .map_err(|_| ArtifactObjectStoreError::TooLarge)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| ArtifactObjectStoreError::Unavailable)?;
        while let Some(next) = output.body.next().await {
            let chunk = match next {
                Ok(chunk) => chunk,
                Err(_) => {
                    bytes.fill(0);
                    return Err(ArtifactObjectStoreError::Unavailable);
                }
            };
            let next_length = bytes
                .len()
                .checked_add(chunk.len())
                .ok_or(ArtifactObjectStoreError::TooLarge)?;
            if next_length > provider_limit || next_length > capacity {
                bytes.fill(0);
                return Err(ArtifactObjectStoreError::TooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        ArtifactObjectBytes::new(metadata, bytes, provider_limit)
    }

    async fn delete_exact(
        &self,
        object_key: &str,
        object_generation: &str,
    ) -> Result<ArtifactObjectDeletionReceipt, ArtifactObjectStoreError> {
        if !valid_opaque_object_key(object_key) || !valid_object_generation(object_generation) {
            return Err(ArtifactObjectStoreError::Rejected);
        }
        let output = self
            .client
            .delete_object()
            .bucket(&*self.bucket)
            .key(object_key)
            .version_id(object_generation)
            .send()
            .await
            .map_err(|_| ArtifactObjectStoreError::Unavailable)?;
        if output.version_id() != Some(object_generation) || output.delete_marker() == Some(true) {
            return Err(ArtifactObjectStoreError::InvalidEvidence);
        }
        let provider_receipt_digest = canonical_digest(&serde_json::json!({
            "delete_marker": output.delete_marker().unwrap_or(false),
            "kind": "s3_delete_object",
            "object_generation": object_generation,
            "request_charged": output.request_charged().map(|value| value.as_str()),
            "schema_version": 1,
            "storage_binding_digest": self.storage_binding_digest,
        }))
        .map_err(|_| ArtifactObjectStoreError::InvalidEvidence)?
        .parse()
        .map_err(|_| ArtifactObjectStoreError::InvalidEvidence)?;
        Ok(ArtifactObjectDeletionReceipt {
            object_generation: object_generation.to_owned(),
            provider_receipt_digest,
        })
    }
}

struct AwsKmsKeyBinding {
    client: KmsClient,
    key_id: Arc<str>,
}

struct AwsKmsArtifactObjectReferenceUnsealer {
    bindings: BTreeMap<Sha256Digest, Arc<AwsKmsKeyBinding>>,
}

impl AwsKmsArtifactObjectReferenceUnsealer {
    async fn decrypt_reference(
        &self,
        tenant_id: &ResourceId,
        blob_id: &ResourceId,
        storage_binding_digest: &Sha256Digest,
        encryption_domain_id: &ResourceId,
        key_id: &str,
        ciphertext: &[u8],
    ) -> Result<DecryptedArtifactObjectReference, ArtifactObjectReferenceUnsealError> {
        let binding = self
            .bindings
            .get(storage_binding_digest)
            .ok_or(ArtifactObjectReferenceUnsealError::Rejected)?;
        if binding.key_id.as_ref() != key_id {
            return Err(ArtifactObjectReferenceUnsealError::Rejected);
        }
        let encryption_context = object_encryption_context(
            tenant_id,
            blob_id,
            storage_binding_digest,
            encryption_domain_id,
            key_id,
        );
        let output = binding
            .client
            .decrypt()
            .ciphertext_blob(Blob::new(ciphertext))
            .key_id(key_id)
            .set_encryption_context(Some(encryption_context))
            .encryption_algorithm(EncryptionAlgorithmSpec::SymmetricDefault)
            .send()
            .await
            .map_err(|error| match error.as_service_error() {
                Some(service)
                    if service.is_incorrect_key_exception()
                        || service.is_invalid_ciphertext_exception()
                        || service.is_invalid_grant_token_exception()
                        || service.is_invalid_key_usage_exception()
                        || service.is_not_found_exception()
                        || service.is_disabled_exception()
                        || service.is_kms_invalid_state_exception() =>
                {
                    ArtifactObjectReferenceUnsealError::Rejected
                }
                _ => ArtifactObjectReferenceUnsealError::Unavailable,
            })?;
        if output.key_id() != Some(key_id)
            || output.encryption_algorithm() != Some(&EncryptionAlgorithmSpec::SymmetricDefault)
            || output.ciphertext_for_recipient().is_some()
        {
            if let Some(plaintext) = output.plaintext {
                let mut bytes = plaintext.into_inner();
                bytes.fill(0);
            }
            return Err(ArtifactObjectReferenceUnsealError::InvalidEvidence);
        }
        DecryptedArtifactObjectReference::new(
            output
                .plaintext
                .ok_or(ArtifactObjectReferenceUnsealError::InvalidEvidence)?
                .into_inner(),
        )
    }
}

impl fmt::Debug for AwsKmsArtifactObjectReferenceUnsealer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AwsKmsArtifactObjectReferenceUnsealer")
            .field("binding_count", &self.bindings.len())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ArtifactObjectReferenceUnsealer for AwsKmsArtifactObjectReferenceUnsealer {
    async fn unseal(
        &self,
        authorized: &AuthorizedArtifactObjectRead,
    ) -> Result<DecryptedArtifactObjectReference, ArtifactObjectReferenceUnsealError> {
        self.decrypt_reference(
            &authorized.tenant_id,
            &authorized.blob_id,
            &authorized.storage_binding_digest,
            &authorized.encryption_domain_id,
            &authorized.key_id,
            authorized.object_reference_ciphertext.as_bytes(),
        )
        .await
    }

    async fn unseal_scan(
        &self,
        authorized: &AuthorizedArtifactScanObjectRead,
    ) -> Result<DecryptedArtifactObjectReference, ArtifactObjectReferenceUnsealError> {
        self.decrypt_reference(
            &authorized.tenant_id,
            &authorized.blob_id,
            &authorized.storage_binding_digest,
            &authorized.encryption_domain_id,
            &authorized.key_id,
            authorized.object_reference_ciphertext.as_bytes(),
        )
        .await
    }

    async fn unseal_delete(
        &self,
        authorized: &AuthorizedArtifactDeleteObject,
    ) -> Result<DecryptedArtifactObjectReference, ArtifactObjectReferenceUnsealError> {
        self.decrypt_reference(
            &authorized.tenant_id,
            &authorized.blob_id,
            &authorized.storage_binding_digest,
            &authorized.encryption_domain_id,
            &authorized.key_id,
            authorized.object_reference_ciphertext.as_bytes(),
        )
        .await
    }
}

fn object_encryption_context(
    tenant_id: &ResourceId,
    blob_id: &ResourceId,
    storage_binding_digest: &Sha256Digest,
    encryption_domain_id: &ResourceId,
    key_id: &str,
) -> HashMap<String, String> {
    HashMap::from([
        ("schema_version".to_owned(), "1".to_owned()),
        ("tenant_id".to_owned(), tenant_id.to_string()),
        ("blob_id".to_owned(), blob_id.to_string()),
        (
            "storage_binding_digest".to_owned(),
            storage_binding_digest.to_string(),
        ),
        (
            "encryption_domain_id".to_owned(),
            encryption_domain_id.to_string(),
        ),
        ("key_id".to_owned(), key_id.to_owned()),
    ])
}

fn metadata_from_s3(
    actual_generation: Option<&str>,
    content_length: Option<i64>,
    expected_generation: &str,
    maximum_bytes: u64,
) -> Result<ArtifactObjectMetadata, ArtifactObjectStoreError> {
    if actual_generation != Some(expected_generation) {
        return Err(ArtifactObjectStoreError::InvalidEvidence);
    }
    let byte_length = content_length
        .and_then(|length| u64::try_from(length).ok())
        .ok_or(ArtifactObjectStoreError::InvalidEvidence)?;
    if byte_length > maximum_bytes {
        return Err(ArtifactObjectStoreError::TooLarge);
    }
    Ok(ArtifactObjectMetadata {
        object_generation: expected_generation.to_owned(),
        byte_length,
    })
}

fn valid_https_service_endpoint(value: &str) -> bool {
    if value.len() > MAX_AWS_ARTIFACT_PROVIDER_ENDPOINT_BYTES {
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

fn valid_bucket(value: &str) -> bool {
    (3..=63).contains(&value.len())
        && !value.starts_with('.')
        && !value.ends_with('.')
        && !value.starts_with('-')
        && !value.ends_with('-')
        && !value.contains("..")
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
}

fn valid_kms_key_arn(value: &str, region: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ARTIFACT_KMS_KEY_ID_BYTES
        && value.starts_with("arn:")
        && value.contains(&format!(":kms:{region}:"))
        && value.contains(":key/")
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn valid_timeouts(connect_milliseconds: u64, operation_milliseconds: u64) -> bool {
    connect_milliseconds > 0
        && connect_milliseconds <= operation_milliseconds
        && operation_milliseconds <= MAX_AWS_ARTIFACT_PROVIDER_TIMEOUT_MILLISECONDS
        && Duration::from_millis(operation_milliseconds) <= MAX_ARTIFACT_BROKER_TIMEOUT
}

fn valid_object_generation(value: &str) -> bool {
    !value.is_empty() && value.len() <= 255 && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn map_catalog_configuration_error(
    error: ArtifactBrokerConfigurationError,
) -> AwsArtifactProviderConfigError {
    match error {
        ArtifactBrokerConfigurationError::DuplicateStorageBinding => {
            AwsArtifactProviderConfigError::DuplicateStorageBinding
        }
        ArtifactBrokerConfigurationError::InvalidLimits
        | ArtifactBrokerConfigurationError::InvalidStorageBinding
        | ArtifactBrokerConfigurationError::StorageBindingCatalogTooLarge => {
            AwsArtifactProviderConfigError::InvalidCatalog
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::primitives::ByteStream;
    use insight_platform_artifacts::EncryptedArtifactObjectReference;
    use insight_platform_contracts::{ArtifactRef, DataClassification};

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn id(kind: ResourceKind, suffix: &str) -> ResourceId {
        format!(
            "{}_0198f1c8-32e4-75e1-a9e8-d95ca0f4{suffix}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn valid_kms() -> AwsKmsKeyBindingConfig {
        let mut binding = AwsKmsKeyBindingConfig {
            schema_version: 1,
            kms_binding_digest: digest('a'),
            endpoint: "https://kms.platform.example".to_owned(),
            region: "us-east-1".to_owned(),
            key_id: "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000001"
                .to_owned(),
            connect_timeout_milliseconds: 1_000,
            operation_timeout_milliseconds: 5_000,
        };
        binding.kms_binding_digest = binding.calculated_digest().unwrap();
        binding
    }

    fn valid_s3(kms_binding_digest: Sha256Digest) -> AwsS3StorageBindingConfig {
        let mut binding = AwsS3StorageBindingConfig {
            schema_version: 1,
            storage_binding_digest: digest('b'),
            endpoint: "https://s3.platform.example".to_owned(),
            region: "us-east-1".to_owned(),
            bucket: "platform-artifacts".to_owned(),
            force_path_style: true,
            kms_binding_digest,
            connect_timeout_milliseconds: 1_000,
            operation_timeout_milliseconds: 5_000,
            maximum_object_bytes: 16 * 1024 * 1024,
        };
        binding.storage_binding_digest = binding.calculated_digest().unwrap();
        binding
    }

    #[test]
    fn candidate_catalog_is_closed_and_digest_bound() {
        let kms = valid_kms();
        let s3 = valid_s3(kms.kms_binding_digest.clone());
        let catalog = AwsArtifactProviderCatalogConfig {
            schema_version: 1,
            write_storage_binding_digest: s3.storage_binding_digest.clone(),
            s3_storage_bindings: vec![s3.clone()],
            kms_key_bindings: vec![kms.clone()],
        };
        catalog.validate().unwrap();

        let mut drifted = catalog.clone();
        drifted.s3_storage_bindings[0].bucket = "other-artifacts".to_owned();
        assert_eq!(
            drifted.validate(),
            Err(AwsArtifactProviderConfigError::InvalidStorageBinding)
        );

        let mut missing = catalog;
        missing.s3_storage_bindings[0].kms_binding_digest = digest('f');
        missing.s3_storage_bindings[0].storage_binding_digest =
            missing.s3_storage_bindings[0].calculated_digest().unwrap();
        missing.write_storage_binding_digest = missing.s3_storage_bindings[0]
            .storage_binding_digest
            .clone();
        assert_eq!(
            missing.validate(),
            Err(AwsArtifactProviderConfigError::KmsBindingClosureMismatch)
        );
    }

    #[test]
    fn provider_endpoints_and_exact_s3_metadata_fail_closed() {
        assert!(!valid_https_service_endpoint("http://s3.platform.example"));
        assert!(!valid_https_service_endpoint(
            "https://user:secret@s3.platform.example"
        ));
        assert_eq!(
            metadata_from_s3(Some("other"), Some(7), "version-1", 8),
            Err(ArtifactObjectStoreError::InvalidEvidence)
        );
        assert_eq!(
            metadata_from_s3(Some("version-1"), Some(9), "version-1", 8),
            Err(ArtifactObjectStoreError::TooLarge)
        );
    }

    #[tokio::test]
    async fn real_https_s3_and_kms_round_trip_exact_generation_when_configured() {
        let (Ok(endpoint), Ok(bucket), Ok(key_id)) = (
            std::env::var("PLATFORM_TEST_AWS_ENDPOINT"),
            std::env::var("PLATFORM_TEST_S3_BUCKET"),
            std::env::var("PLATFORM_TEST_KMS_KEY_ID"),
        ) else {
            return;
        };
        let mut kms = valid_kms();
        kms.endpoint.clone_from(&endpoint);
        kms.key_id = key_id;
        kms.kms_binding_digest = kms.calculated_digest().unwrap();
        let mut s3 = valid_s3(kms.kms_binding_digest.clone());
        s3.endpoint = endpoint;
        s3.bucket = bucket;
        s3.storage_binding_digest = s3.calculated_digest().unwrap();
        let storage_binding_digest = s3.storage_binding_digest.clone();
        let catalog = AwsArtifactProviderCatalog::install(AwsArtifactProviderCatalogConfig {
            schema_version: 1,
            write_storage_binding_digest: storage_binding_digest.clone(),
            s3_storage_bindings: vec![s3],
            kms_key_bindings: vec![kms],
        })
        .await
        .unwrap();
        catalog.check_readiness().await.unwrap();
        let (upload, unsealer, stores) = catalog.into_gateway_components();

        let tenant_id = id(ResourceKind::Tenant, "4001");
        let artifact_id = id(ResourceKind::Artifact, "4002");
        let blob_id = id(ResourceKind::InternalBlob, "4003");
        let encryption_domain_id = id(ResourceKind::EncryptionDomain, "4004");
        let bytes = b"real s3/kms fixture";
        let prepared = upload
            .prepare_upload(AwsArtifactUploadRequest {
                tenant_id: &tenant_id,
                artifact_id: &artifact_id,
                blob_id: &blob_id,
                encryption_domain_id: &encryption_domain_id,
                expected_size_bytes: u64::try_from(bytes.len()).unwrap(),
                declared_media_type: Some("application/octet-stream"),
                expires_in: Duration::from_secs(60),
            })
            .await
            .unwrap();
        assert!(prepared.upload_url.starts_with("https://"));
        let object_key = format!("v1/{tenant_id}/{artifact_id}/{blob_id}");
        let put = upload
            .s3
            .put_object()
            .bucket(&*upload.bucket)
            .key(&object_key)
            .content_type("application/octet-stream")
            .body(ByteStream::from_static(bytes))
            .send()
            .await
            .unwrap();
        let generation = put.version_id().unwrap().to_owned();
        let completed = upload
            .complete_upload(
                &tenant_id,
                &artifact_id,
                &blob_id,
                &generation,
                u64::try_from(bytes.len()).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(completed.object_generation, generation);

        let artifact = ArtifactRef::new(
            artifact_id,
            crate::sha256(bytes),
            u64::try_from(bytes.len()).unwrap(),
            "application/octet-stream",
            DataClassification::Internal,
            Some("real-s3-kms-fixture.bin".to_owned()),
        )
        .unwrap();
        let wrong_tenant_authorization = AuthorizedArtifactObjectRead {
            tenant_id: id(ResourceKind::Tenant, "4099"),
            blob_id: blob_id.clone(),
            artifact: artifact.clone(),
            backend: prepared.storage_backend.clone(),
            storage_binding_digest: storage_binding_digest.clone(),
            encryption_domain_id: encryption_domain_id.clone(),
            key_id: prepared.key_id.clone(),
            object_reference_ciphertext: EncryptedArtifactObjectReference::new(
                prepared.object_reference_ciphertext.clone(),
            )
            .unwrap(),
            object_generation: generation.clone(),
            authorization_digest: digest('e'),
        };
        assert!(unsealer.unseal(&wrong_tenant_authorization).await.is_err());
        let authorized = AuthorizedArtifactObjectRead {
            tenant_id,
            blob_id,
            artifact,
            backend: prepared.storage_backend,
            storage_binding_digest: storage_binding_digest.clone(),
            encryption_domain_id,
            key_id: prepared.key_id,
            object_reference_ciphertext: EncryptedArtifactObjectReference::new(
                prepared.object_reference_ciphertext,
            )
            .unwrap(),
            object_generation: generation.clone(),
            authorization_digest: digest('f'),
        };
        let locator = unsealer.unseal(&authorized).await.unwrap();
        let locator: serde_json::Value = serde_json::from_slice(locator.expose()).unwrap();
        assert_eq!(locator["object_key"], object_key);
        assert!(locator.get("object_generation").is_none());

        let store = stores.get(&storage_binding_digest).unwrap();
        assert_eq!(
            store
                .read_exact(&object_key, &generation, bytes.len())
                .await
                .unwrap()
                .expose(),
            bytes
        );
        assert_eq!(
            store.head_exact(&object_key, "wrong-generation").await,
            Err(ArtifactObjectStoreError::NotFound)
        );
        assert_eq!(
            store
                .delete_exact(&object_key, &generation)
                .await
                .unwrap()
                .object_generation,
            generation
        );
        assert_eq!(
            store.head_exact(&object_key, &generation).await,
            Err(ArtifactObjectStoreError::NotFound)
        );
    }
}
