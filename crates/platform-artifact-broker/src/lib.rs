//! Trusted Artifact materialization for credential-free execution roles.
//!
//! The Broker owns no durable state. PostgreSQL authorizes the exact read, a KMS adapter unseals
//! the locator, and a CandidateManifest-installed object-store adapter reads one immutable
//! generation. Bytes are returned only after a second authority check closes the I/O race.

use async_trait::async_trait;
use chrono::Utc;
use insight_platform_artifacts::{
    ArtifactObjectReadAuthority, ArtifactObjectReadAuthorityError, AuthorizedArtifactObjectRead,
};
use insight_platform_contracts::{parse_strict_json, JsonLimits, Sha256Digest};
use insight_platform_sandbox::{
    WasiArtifactBroker, WasiArtifactBrokerError, WasiArtifactReadRequest,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};
use tokio::{sync::OwnedSemaphorePermit, sync::Semaphore, time::timeout};

mod aws;

pub use aws::{
    AwsArtifactProviderCatalog, AwsArtifactProviderCatalogConfig, AwsArtifactProviderConfigError,
    AwsArtifactProviderReadinessError, AwsArtifactUploadError, AwsArtifactUploadProvider,
    AwsKmsKeyBindingConfig, AwsS3StorageBindingConfig, CompletedAwsArtifactUploadEvidence,
    PreparedAwsArtifactUpload,
};

pub const MAX_INSTALLED_ARTIFACT_STORAGE_BINDINGS: usize = 64;
pub const MAX_ARTIFACT_READ_IN_FLIGHT_HARD: usize = 4_096;
pub const MAX_DECRYPTED_ARTIFACT_LOCATOR_BYTES: usize = 16_384;
pub const MAX_ARTIFACT_OBJECT_KEY_BYTES: usize = 1_024;
pub const MAX_ARTIFACT_BROKER_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactBrokerLimits {
    pub maximum_in_flight: usize,
    pub maximum_read_bytes: usize,
    pub operation_timeout: Duration,
}

impl ArtifactBrokerLimits {
    pub fn validate(self) -> Result<(), ArtifactBrokerConfigurationError> {
        if self.maximum_in_flight == 0
            || self.maximum_in_flight > MAX_ARTIFACT_READ_IN_FLIGHT_HARD
            || self.maximum_read_bytes == 0
            || self.operation_timeout.is_zero()
            || self.operation_timeout > MAX_ARTIFACT_BROKER_TIMEOUT
        {
            return Err(ArtifactBrokerConfigurationError::InvalidLimits);
        }
        Ok(())
    }
}

impl Default for ArtifactBrokerLimits {
    fn default() -> Self {
        Self {
            maximum_in_flight: 128,
            maximum_read_bytes: 16 * 1024 * 1024,
            operation_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactBrokerConfigurationError {
    InvalidLimits,
    InvalidStorageBinding,
    DuplicateStorageBinding,
    StorageBindingCatalogTooLarge,
}

/// KMS plaintext. It cannot be cloned or formatted and is zeroed on drop.
pub struct DecryptedArtifactObjectReference(Vec<u8>);

impl DecryptedArtifactObjectReference {
    pub fn new(mut bytes: Vec<u8>) -> Result<Self, ArtifactObjectReferenceUnsealError> {
        if bytes.is_empty() || bytes.len() > MAX_DECRYPTED_ARTIFACT_LOCATOR_BYTES {
            bytes.fill(0);
            return Err(ArtifactObjectReferenceUnsealError::InvalidEvidence);
        }
        Ok(Self(bytes))
    }

    fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for DecryptedArtifactObjectReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecryptedArtifactObjectReference")
            .field("byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Drop for DecryptedArtifactObjectReference {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactObjectReferenceUnsealError {
    Unavailable,
    Rejected,
    InvalidEvidence,
}

/// KMS port. Associated data must contain every identity in the authorized projection.
#[async_trait]
pub trait ArtifactObjectReferenceUnsealer: Send + Sync {
    async fn unseal(
        &self,
        authorized: &AuthorizedArtifactObjectRead,
    ) -> Result<DecryptedArtifactObjectReference, ArtifactObjectReferenceUnsealError>;
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactObjectLocator {
    schema_version: u32,
    backend: String,
    storage_binding_digest: Sha256Digest,
    object_key: String,
}

impl ArtifactObjectLocator {
    fn validate_for(&self, authorized: &AuthorizedArtifactObjectRead) -> bool {
        self.schema_version == 1
            && self.backend == "s3"
            && self.backend == authorized.backend
            && self.storage_binding_digest == authorized.storage_binding_digest
            && valid_opaque_object_key(&self.object_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactObjectMetadata {
    pub object_generation: String,
    pub byte_length: u64,
}

/// Non-clone object bytes. Providers must enforce their own streaming ceiling before creating it.
pub struct ArtifactObjectBytes {
    pub metadata: ArtifactObjectMetadata,
    bytes: Vec<u8>,
}

impl ArtifactObjectBytes {
    pub fn new(
        metadata: ArtifactObjectMetadata,
        mut bytes: Vec<u8>,
        maximum_bytes: usize,
    ) -> Result<Self, ArtifactObjectStoreError> {
        if bytes.len() > maximum_bytes
            || u64::try_from(bytes.len()).ok() != Some(metadata.byte_length)
            || metadata.object_generation.is_empty()
        {
            bytes.fill(0);
            return Err(ArtifactObjectStoreError::InvalidEvidence);
        }
        Ok(Self { metadata, bytes })
    }

    fn expose(&self) -> &[u8] {
        &self.bytes
    }

    fn into_bytes(mut self) -> Vec<u8> {
        std::mem::take(&mut self.bytes)
    }
}

impl fmt::Debug for ArtifactObjectBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactObjectBytes")
            .field("metadata", &self.metadata)
            .field("byte_length", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

impl Drop for ArtifactObjectBytes {
    fn drop(&mut self) {
        self.bytes.fill(0);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactObjectStoreError {
    Unavailable,
    NotFound,
    Rejected,
    TooLarge,
    InvalidEvidence,
}

#[async_trait]
pub trait InstalledArtifactObjectStore: Send + Sync {
    fn backend(&self) -> &str;
    fn storage_binding_digest(&self) -> &Sha256Digest;

    async fn head_exact(
        &self,
        object_key: &str,
        object_generation: &str,
    ) -> Result<ArtifactObjectMetadata, ArtifactObjectStoreError>;

    async fn read_exact(
        &self,
        object_key: &str,
        object_generation: &str,
        maximum_bytes: usize,
    ) -> Result<ArtifactObjectBytes, ArtifactObjectStoreError>;
}

#[derive(Clone)]
pub struct InstalledArtifactObjectStoreCatalog {
    stores: BTreeMap<Sha256Digest, Arc<dyn InstalledArtifactObjectStore>>,
}

impl InstalledArtifactObjectStoreCatalog {
    pub fn new(
        stores: Vec<Arc<dyn InstalledArtifactObjectStore>>,
    ) -> Result<Self, ArtifactBrokerConfigurationError> {
        if stores.is_empty() || stores.len() > MAX_INSTALLED_ARTIFACT_STORAGE_BINDINGS {
            return Err(ArtifactBrokerConfigurationError::StorageBindingCatalogTooLarge);
        }
        let mut installed = BTreeMap::new();
        for store in stores {
            if store.backend() != "s3" {
                return Err(ArtifactBrokerConfigurationError::InvalidStorageBinding);
            }
            let digest = store.storage_binding_digest().clone();
            if installed.insert(digest, store).is_some() {
                return Err(ArtifactBrokerConfigurationError::DuplicateStorageBinding);
            }
        }
        Ok(Self { stores: installed })
    }

    fn get(&self, digest: &Sha256Digest) -> Option<Arc<dyn InstalledArtifactObjectStore>> {
        self.stores.get(digest).cloned()
    }
}

pub struct BrokeredSandboxArtifactBroker {
    wasi_authority: Arc<dyn ArtifactObjectReadAuthority<WasiArtifactReadRequest>>,
    core: ArtifactBrokerCore,
}

/// An exact Artifact read whose audience permit remains owned until the caller finishes with the
/// returned bytes. RPC adapters must move this lease into the response stream.
pub struct BrokeredArtifactRead {
    bytes: Vec<u8>,
    permit: Option<OwnedSemaphorePermit>,
}

/// Opaque ownership of one audience capacity slot.
pub struct ArtifactBrokerReadPermit {
    _permit: OwnedSemaphorePermit,
}

impl BrokeredArtifactRead {
    fn new(bytes: Vec<u8>, permit: OwnedSemaphorePermit) -> Self {
        Self {
            bytes,
            permit: Some(permit),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(mut self) -> Vec<u8> {
        std::mem::take(&mut self.bytes)
    }

    pub fn into_response_parts(mut self) -> (Vec<u8>, ArtifactBrokerReadPermit) {
        let bytes = std::mem::take(&mut self.bytes);
        let permit = self
            .permit
            .take()
            .expect("Artifact read permit is present until the lease is consumed");
        (bytes, ArtifactBrokerReadPermit { _permit: permit })
    }
}

impl Drop for BrokeredArtifactRead {
    fn drop(&mut self) {
        self.bytes.fill(0);
    }
}

struct ArtifactBrokerCore {
    unsealer: Arc<dyn ArtifactObjectReferenceUnsealer>,
    stores: InstalledArtifactObjectStoreCatalog,
    limits: ArtifactBrokerLimits,
    in_flight: Arc<Semaphore>,
}

trait ArtifactReadRequest {
    fn deadline(&self) -> chrono::DateTime<Utc>;
    fn maximum_bytes(&self) -> usize;
    fn artifact(&self) -> Option<&insight_platform_contracts::ArtifactRef>;
}

impl ArtifactReadRequest for WasiArtifactReadRequest {
    fn deadline(&self) -> chrono::DateTime<Utc> {
        self.deadline
    }

    fn maximum_bytes(&self) -> usize {
        self.maximum_bytes
    }

    fn artifact(&self) -> Option<&insight_platform_contracts::ArtifactRef> {
        Some(&self.artifact)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactBrokerReadError {
    Unavailable,
    Denied,
    NotFound,
    TooLarge,
    Integrity,
}

impl BrokeredSandboxArtifactBroker {
    pub fn new(
        wasi_authority: Arc<dyn ArtifactObjectReadAuthority<WasiArtifactReadRequest>>,
        unsealer: Arc<dyn ArtifactObjectReferenceUnsealer>,
        stores: InstalledArtifactObjectStoreCatalog,
        limits: ArtifactBrokerLimits,
    ) -> Result<Self, ArtifactBrokerConfigurationError> {
        limits.validate()?;
        Ok(Self {
            wasi_authority,
            core: ArtifactBrokerCore::new(unsealer, stores, limits)?,
        })
    }
}

impl BrokeredSandboxArtifactBroker {
    pub async fn read_wasi_for_response(
        &self,
        request: WasiArtifactReadRequest,
    ) -> Result<BrokeredArtifactRead, WasiArtifactBrokerError> {
        self.core
            .read(self.wasi_authority.as_ref(), &request)
            .await
            .map_err(map_wasi_broker_error)
    }
}

impl ArtifactBrokerCore {
    fn new(
        unsealer: Arc<dyn ArtifactObjectReferenceUnsealer>,
        stores: InstalledArtifactObjectStoreCatalog,
        limits: ArtifactBrokerLimits,
    ) -> Result<Self, ArtifactBrokerConfigurationError> {
        limits.validate()?;
        Ok(Self {
            unsealer,
            stores,
            limits,
            in_flight: Arc::new(Semaphore::new(limits.maximum_in_flight)),
        })
    }

    async fn read_inner<R>(
        &self,
        authority: &dyn ArtifactObjectReadAuthority<R>,
        request: &R,
    ) -> Result<Vec<u8>, ArtifactBrokerReadError>
    where
        R: ArtifactReadRequest + Sync,
    {
        let maximum_bytes = request.maximum_bytes();
        let artifact = request.artifact().ok_or(ArtifactBrokerReadError::Denied)?;
        if request.deadline() <= Utc::now()
            || maximum_bytes == 0
            || maximum_bytes > self.limits.maximum_read_bytes
            || u64::try_from(maximum_bytes).map_or(true, |maximum| maximum < artifact.byte_length())
        {
            return Err(ArtifactBrokerReadError::Denied);
        }
        let authorized = authority
            .authorize_object_read(request)
            .await
            .map_err(map_authority_error)?;
        authorized
            .validate()
            .map_err(|_| ArtifactBrokerReadError::Integrity)?;
        if &authorized.artifact != artifact {
            return Err(ArtifactBrokerReadError::Integrity);
        }
        let store = self
            .stores
            .get(&authorized.storage_binding_digest)
            .ok_or(ArtifactBrokerReadError::Denied)?;
        if store.backend() != authorized.backend {
            return Err(ArtifactBrokerReadError::Integrity);
        }
        let plaintext = self
            .unsealer
            .unseal(&authorized)
            .await
            .map_err(map_unseal_error)?;
        let locator = parse_locator(plaintext.expose())?;
        if !locator.validate_for(&authorized) {
            return Err(ArtifactBrokerReadError::Integrity);
        }
        let head = store
            .head_exact(&locator.object_key, &authorized.object_generation)
            .await
            .map_err(map_store_error)?;
        require_object_metadata(&authorized, &head, maximum_bytes)?;
        let object = store
            .read_exact(
                &locator.object_key,
                &authorized.object_generation,
                maximum_bytes,
            )
            .await
            .map_err(map_store_error)?;
        require_object_metadata(&authorized, &object.metadata, maximum_bytes)?;
        if sha256(object.expose()) != authorized.artifact.content_digest().clone() {
            return Err(ArtifactBrokerReadError::Integrity);
        }

        // The object I/O happened outside the database transaction. Re-authorize before release
        // so terminal/revoke/lease changes cannot race a successful read into the Executor.
        let final_authorization = authority
            .authorize_object_read(request)
            .await
            .map_err(map_authority_error)?;
        if final_authorization.authorization_digest != authorized.authorization_digest {
            return Err(ArtifactBrokerReadError::Denied);
        }
        Ok(object.into_bytes())
    }

    async fn read<R>(
        &self,
        authority: &dyn ArtifactObjectReadAuthority<R>,
        request: &R,
    ) -> Result<BrokeredArtifactRead, ArtifactBrokerReadError>
    where
        R: ArtifactReadRequest + Sync,
    {
        let permit = self
            .in_flight
            .clone()
            .try_acquire_owned()
            .map_err(|_| ArtifactBrokerReadError::Unavailable)?;
        let deadline_budget = request
            .deadline()
            .signed_duration_since(Utc::now())
            .to_std()
            .map_err(|_| ArtifactBrokerReadError::Denied)?;
        let budget = self.limits.operation_timeout.min(deadline_budget);
        let bytes = timeout(budget, self.read_inner(authority, request))
            .await
            .map_err(|_| ArtifactBrokerReadError::Unavailable)??;
        Ok(BrokeredArtifactRead::new(bytes, permit))
    }
}

#[async_trait]
impl WasiArtifactBroker for BrokeredSandboxArtifactBroker {
    async fn read_exact(
        &self,
        request: WasiArtifactReadRequest,
    ) -> Result<Vec<u8>, WasiArtifactBrokerError> {
        self.read_wasi_for_response(request)
            .await
            .map(BrokeredArtifactRead::into_bytes)
    }
}

fn parse_locator(bytes: &[u8]) -> Result<ArtifactObjectLocator, ArtifactBrokerReadError> {
    let value = parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: MAX_DECRYPTED_ARTIFACT_LOCATOR_BYTES,
            max_depth: 4,
            max_items_per_array: 1,
            max_properties_per_object: 5,
            max_string_bytes: MAX_ARTIFACT_OBJECT_KEY_BYTES,
        },
    )
    .map_err(|_| ArtifactBrokerReadError::Integrity)?;
    if serde_jcs::to_vec(&value).map_err(|_| ArtifactBrokerReadError::Integrity)? != bytes {
        return Err(ArtifactBrokerReadError::Integrity);
    }
    serde_json::from_value(value).map_err(|_| ArtifactBrokerReadError::Integrity)
}

fn require_object_metadata(
    authorized: &AuthorizedArtifactObjectRead,
    metadata: &ArtifactObjectMetadata,
    maximum_bytes: usize,
) -> Result<(), ArtifactBrokerReadError> {
    if metadata.object_generation != authorized.object_generation
        || metadata.byte_length != authorized.artifact.byte_length()
        || metadata.byte_length > u64::try_from(maximum_bytes).unwrap_or(u64::MAX)
    {
        return Err(ArtifactBrokerReadError::Integrity);
    }
    Ok(())
}

fn valid_opaque_object_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ARTIFACT_OBJECT_KEY_BYTES
        && !value.starts_with('/')
        && !value.ends_with('/')
        && !value.contains("//")
        && value
            .split('/')
            .all(|segment| !matches!(segment, "" | "." | ".."))
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'\\')
}

fn sha256(bytes: &[u8]) -> Sha256Digest {
    let value = Sha256::digest(bytes);
    format!("sha256:{}", lower_hex(&value))
        .parse()
        .expect("SHA-256 output has the nominal digest shape")
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn map_authority_error(error: ArtifactObjectReadAuthorityError) -> ArtifactBrokerReadError {
    match error {
        ArtifactObjectReadAuthorityError::Unavailable => ArtifactBrokerReadError::Unavailable,
        ArtifactObjectReadAuthorityError::Denied => ArtifactBrokerReadError::Denied,
        ArtifactObjectReadAuthorityError::NotFound => ArtifactBrokerReadError::NotFound,
        ArtifactObjectReadAuthorityError::InvalidEvidence => ArtifactBrokerReadError::Integrity,
    }
}

fn map_unseal_error(error: ArtifactObjectReferenceUnsealError) -> ArtifactBrokerReadError {
    match error {
        ArtifactObjectReferenceUnsealError::Unavailable => ArtifactBrokerReadError::Unavailable,
        ArtifactObjectReferenceUnsealError::Rejected => ArtifactBrokerReadError::Denied,
        ArtifactObjectReferenceUnsealError::InvalidEvidence => ArtifactBrokerReadError::Integrity,
    }
}

fn map_store_error(error: ArtifactObjectStoreError) -> ArtifactBrokerReadError {
    match error {
        ArtifactObjectStoreError::Unavailable => ArtifactBrokerReadError::Unavailable,
        ArtifactObjectStoreError::NotFound => ArtifactBrokerReadError::NotFound,
        ArtifactObjectStoreError::Rejected => ArtifactBrokerReadError::Denied,
        ArtifactObjectStoreError::TooLarge => ArtifactBrokerReadError::TooLarge,
        ArtifactObjectStoreError::InvalidEvidence => ArtifactBrokerReadError::Integrity,
    }
}

fn map_wasi_broker_error(error: ArtifactBrokerReadError) -> WasiArtifactBrokerError {
    match error {
        ArtifactBrokerReadError::Unavailable => WasiArtifactBrokerError::Unavailable,
        ArtifactBrokerReadError::Denied => WasiArtifactBrokerError::Denied,
        ArtifactBrokerReadError::NotFound => WasiArtifactBrokerError::NotFound,
        ArtifactBrokerReadError::TooLarge => WasiArtifactBrokerError::TooLarge,
        ArtifactBrokerReadError::Integrity => WasiArtifactBrokerError::Integrity,
    }
}

#[cfg(test)]
mod tests;
