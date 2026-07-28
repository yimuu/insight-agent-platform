//! External content-addressed storage adapter for large worker values.
//!
//! Database rows remain the metadata/reference authority. This adapter owns
//! only idempotent object writes, byte verification, and deletion. It never
//! claims external exactly-once I/O.

use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use ring::{
    aead::{self, Aad, LessSafeKey, Nonce, UnboundKey},
    digest, hkdf,
    rand::{SecureRandom as _, SystemRandom},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use insight_engine::artifact_store::adapter as artifact_store_adapter;
pub use insight_engine::artifact_store::{
    ArtifactStoreDeploymentCapability, ArtifactStoreDeploymentContract, WorkerArtifactStore,
};
use insight_engine::repository::{RepositoryError, StorageLocator, REPOSITORY_STORAGE_FAILURE};
#[cfg(test)]
use insight_engine::repository::{REPOSITORY_CONFIGURATION_INVALID, REPOSITORY_DATA_INVALID};
use insight_engine::{ArtifactRef, ContentHash};

use crate::repository::RepositoryErrorExt as _;

const STORAGE_LOCATOR_PREFIX: &str = "content-addressed:v1/sha256/";
const SHARED_STORE_MARKER_FILE: &str = ".insight-agent-artifact-store-v1.json";
const SHARED_STORE_MARKER_SCHEMA_VERSION: u32 = 1;
const MAX_SHARED_STORE_MARKER_BYTES: usize = 4 * 1024;
const MAX_SHARED_STORE_NAMESPACE_BYTES: usize = 128;
const TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE: &str = "application/vnd.insight.terminal-object.v1+json";
const TERMINAL_SCOPED_ARTIFACT_KIND: &str = "insight_terminal_object";
const TENANT_ENCRYPTION_MAGIC: &[u8; 8] = b"IAPTEA01";
const TENANT_ENCRYPTION_NONCE_BYTES: usize = 12;
const TENANT_ENCRYPTION_DIGEST_BYTES: usize = 32;
const TENANT_ENCRYPTION_TAG_BYTES: usize = 16;
const TENANT_ENCRYPTION_MAX_KEY_VERSION_BYTES: usize = 64;
const TENANT_ENCRYPTION_MAX_OVERHEAD: usize = TENANT_ENCRYPTION_MAGIC.len()
    + 1
    + TENANT_ENCRYPTION_MAX_KEY_VERSION_BYTES
    + TENANT_ENCRYPTION_DIGEST_BYTES
    + TENANT_ENCRYPTION_NONCE_BYTES
    + TENANT_ENCRYPTION_TAG_BYTES;
const TENANT_ENCRYPTION_HKDF_SALT: &[u8] = b"insight-agent-platform/tenant-artifact-encryption/v1";

/// Versioned keyring for tenant-scoped Conversation and terminal objects.
///
/// The active key encrypts new objects. Older versions remain readable while
/// present in the keyring, which permits a rolling key rotation without ever
/// writing a key or plaintext tenant identifier into an object header.
#[derive(Clone)]
pub struct TenantArtifactEncryptionKeyring {
    inner: Arc<TenantArtifactEncryptionKeyringInner>,
}

struct TenantArtifactEncryptionKeyringInner {
    active_key_version: String,
    keys: BTreeMap<String, [u8; 32]>,
}

impl Drop for TenantArtifactEncryptionKeyringInner {
    fn drop(&mut self) {
        for key in self.keys.values_mut() {
            key.fill(0);
        }
    }
}

impl fmt::Debug for TenantArtifactEncryptionKeyring {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TenantArtifactEncryptionKeyring")
            .field("active_key_version", &self.inner.active_key_version)
            .field("key_versions", &self.inner.keys.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl TenantArtifactEncryptionKeyring {
    /// Parses a Secret value shaped as `{"version":"64 lowercase hex chars"}`.
    ///
    /// The JSON belongs in a Secret-backed environment variable. It must not
    /// be embedded in platform YAML or Helm values.
    pub fn from_secret_json(
        active_key_version: impl Into<String>,
        secret_json: &str,
    ) -> Result<Self, RepositoryError> {
        let active_key_version = active_key_version.into();
        if !valid_key_version(&active_key_version) {
            return Err(RepositoryError::invalid_configuration());
        }
        let encoded = serde_json::from_str::<BTreeMap<String, String>>(secret_json)
            .map_err(|_| RepositoryError::invalid_configuration())?;
        if encoded.is_empty() || encoded.len() > 32 {
            return Err(RepositoryError::invalid_configuration());
        }
        let mut keys = BTreeMap::new();
        for (version, value) in encoded {
            if !valid_key_version(&version) || keys.insert(version, decode_key(&value)?).is_some() {
                return Err(RepositoryError::invalid_configuration());
            }
        }
        if !keys.contains_key(&active_key_version) {
            return Err(RepositoryError::invalid_configuration());
        }
        Ok(Self {
            inner: Arc::new(TenantArtifactEncryptionKeyringInner {
                active_key_version,
                keys,
            }),
        })
    }

    pub fn active_key_version(&self) -> &str {
        &self.inner.active_key_version
    }

    fn encryption_key(
        &self,
        key_version: &str,
        tenant_digest: &[u8; TENANT_ENCRYPTION_DIGEST_BYTES],
    ) -> Result<LessSafeKey, RepositoryError> {
        let master = self
            .inner
            .keys
            .get(key_version)
            .ok_or_else(RepositoryError::invalid_configuration)?;
        let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, TENANT_ENCRYPTION_HKDF_SALT);
        let pseudo_random_key = salt.extract(master);
        let prefix = b"tenant-aead:";
        let separator = b":";
        let info = [
            prefix.as_slice(),
            key_version.as_bytes(),
            separator.as_slice(),
            tenant_digest.as_slice(),
        ];
        let output = pseudo_random_key
            .expand(&info, hkdf::HKDF_SHA256)
            .map_err(|_| RepositoryError::invalid_configuration())?;
        let mut derived = [0_u8; 32];
        output
            .fill(&mut derived)
            .map_err(|_| RepositoryError::invalid_configuration())?;
        let key = UnboundKey::new(&aead::AES_256_GCM, &derived)
            .map(LessSafeKey::new)
            .map_err(|_| RepositoryError::invalid_configuration());
        derived.fill(0);
        key
    }
}

fn valid_key_version(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= TENANT_ENCRYPTION_MAX_KEY_VERSION_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn decode_key(value: &str) -> Result<[u8; 32], RepositoryError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RepositoryError::invalid_configuration());
    }
    let mut decoded = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (decode_hex_digit(chunk[0])? << 4) | decode_hex_digit(chunk[1])?;
    }
    Ok(decoded)
}

fn decode_hex_digit(value: u8) -> Result<u8, RepositoryError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(RepositoryError::invalid_configuration()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedStoreMarker {
    schema_version: u32,
    namespace: String,
    store_id: String,
}

impl SharedStoreMarker {
    fn new(namespace: String) -> Self {
        Self {
            schema_version: SHARED_STORE_MARKER_SCHEMA_VERSION,
            namespace,
            store_id: format!("artifact_store_{}", Uuid::new_v4().simple()),
        }
    }

    fn validate(&self, expected_namespace: &str) -> Result<(), RepositoryError> {
        if self.schema_version != SHARED_STORE_MARKER_SCHEMA_VERSION
            || !valid_namespace(&self.namespace)
            || !valid_store_id(&self.store_id)
        {
            return Err(RepositoryError::invalid_data());
        }
        if self.namespace != expected_namespace {
            return Err(RepositoryError::invalid_configuration());
        }
        Ok(())
    }
}

fn valid_namespace(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SHARED_STORE_NAMESPACE_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_store_id(value: &str) -> bool {
    value.strip_prefix("artifact_store_").is_some_and(|suffix| {
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn storage_failure() -> RepositoryError {
    RepositoryError::new(
        REPOSITORY_STORAGE_FAILURE,
        "artifact object storage operation failed",
    )
}

/// Production-capable local filesystem store. Paths are derived exclusively
/// from lowercase SHA-256 content hashes and cannot contain author input.
#[derive(Debug, Clone)]
pub struct LocalContentAddressedArtifactStore {
    root: Arc<PathBuf>,
    inline_threshold_bytes: usize,
    deployment_contract: ArtifactStoreDeploymentContract,
    tenant_encryption: Option<TenantArtifactEncryptionKeyring>,
}

impl LocalContentAddressedArtifactStore {
    pub async fn open(
        root: PathBuf,
        inline_threshold_bytes: usize,
    ) -> Result<Self, RepositoryError> {
        let root = open_root(root, inline_threshold_bytes).await?;
        Ok(Self {
            root: Arc::new(root),
            inline_threshold_bytes,
            deployment_contract: ArtifactStoreDeploymentContract::single_process_local(),
            tenant_encryption: None,
        })
    }

    /// Opens a local store that encrypts tenant-scoped terminal objects.
    pub async fn open_with_tenant_encryption(
        root: PathBuf,
        inline_threshold_bytes: usize,
        tenant_encryption: TenantArtifactEncryptionKeyring,
    ) -> Result<Self, RepositoryError> {
        let root = open_root(root, inline_threshold_bytes).await?;
        Ok(Self {
            root: Arc::new(root),
            inline_threshold_bytes,
            deployment_contract: ArtifactStoreDeploymentContract::single_process_local(),
            tenant_encryption: Some(tenant_encryption),
        })
    }

    /// Opens a filesystem root that is explicitly safe for independent
    /// runtimes. The root marker is atomically installed with `create_new`
    /// candidate files plus a no-replace hard link, so readers can never
    /// observe a partially written authoritative marker.
    pub async fn open_shared(
        root: PathBuf,
        inline_threshold_bytes: usize,
        namespace: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        let namespace = namespace.into();
        if !valid_namespace(&namespace) {
            return Err(RepositoryError::invalid_configuration());
        }
        let root = open_root(root, inline_threshold_bytes).await?;
        let marker = open_shared_marker(&root, &namespace).await?;
        Ok(Self {
            root: Arc::new(root),
            inline_threshold_bytes,
            deployment_contract: artifact_store_adapter::shared_deployment_contract(
                marker.store_id,
                marker.namespace,
            ),
            tenant_encryption: None,
        })
    }

    /// Opens a shared store with a Secret-backed tenant encryption keyring.
    pub async fn open_shared_with_tenant_encryption(
        root: PathBuf,
        inline_threshold_bytes: usize,
        namespace: impl Into<String>,
        tenant_encryption: TenantArtifactEncryptionKeyring,
    ) -> Result<Self, RepositoryError> {
        let namespace = namespace.into();
        if !valid_namespace(&namespace) {
            return Err(RepositoryError::invalid_configuration());
        }
        let root = open_root(root, inline_threshold_bytes).await?;
        let marker = open_shared_marker(&root, &namespace).await?;
        Ok(Self {
            root: Arc::new(root),
            inline_threshold_bytes,
            deployment_contract: artifact_store_adapter::shared_deployment_contract(
                marker.store_id,
                marker.namespace,
            ),
            tenant_encryption: Some(tenant_encryption),
        })
    }

    fn hash_hex<'a>(&self, artifact: &'a ArtifactRef) -> Result<&'a str, RepositoryError> {
        let value = artifact
            .content_hash()
            .as_str()
            .strip_prefix("sha256:")
            .ok_or_else(RepositoryError::invalid_data)?;
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(value)
    }

    fn path_for(&self, artifact: &ArtifactRef) -> Result<PathBuf, RepositoryError> {
        let hex = self.hash_hex(artifact)?;
        Ok(self.root.join(&hex[..2]).join(hex))
    }

    fn locator_path(
        &self,
        artifact: &ArtifactRef,
        locator: &StorageLocator,
    ) -> Result<PathBuf, RepositoryError> {
        let value = locator
            .expose_to_storage_adapter()
            .strip_prefix(STORAGE_LOCATOR_PREFIX)
            .ok_or_else(RepositoryError::invalid_data)?;
        let (shard, hash) = value
            .split_once('/')
            .ok_or_else(RepositoryError::invalid_data)?;
        let expected_hash = self.hash_hex(artifact)?;
        if shard.len() != 2
            || hash.len() != 64
            || shard != &expected_hash[..2]
            || hash != expected_hash
            || value.matches('/').count() != 1
        {
            return Err(RepositoryError::invalid_data());
        }
        self.path_for(artifact)
    }

    async fn cleanup_staging_files(&self, artifact: &ArtifactRef) -> Result<(), RepositoryError> {
        let path = self.path_for(artifact)?;
        let Some(parent) = path.parent() else {
            return Err(RepositoryError::invalid_data());
        };
        let prefix = format!(".{}.", self.hash_hex(artifact)?);
        let mut entries = match tokio::fs::read_dir(parent).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(storage_failure()),
        };
        while let Some(entry) = entries.next_entry().await.map_err(|_| storage_failure())? {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(&prefix) && name.ends_with(".staging") {
                match tokio::fs::remove_file(entry.path()).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => return Err(storage_failure()),
                }
            }
        }
        Ok(())
    }

    fn encode_stored_bytes(
        &self,
        artifact: &ArtifactRef,
        bytes: &[u8],
    ) -> Result<Vec<u8>, RepositoryError> {
        let Some(keyring) = self.tenant_encryption.as_ref() else {
            return Ok(bytes.to_vec());
        };
        let Some(tenant_digest) = scoped_tenant_digest(artifact, bytes)? else {
            return Ok(bytes.to_vec());
        };
        let key_version = keyring.active_key_version();
        let key_version_length = u8::try_from(key_version.len())
            .map_err(|_| RepositoryError::invalid_configuration())?;
        let key = keyring.encryption_key(key_version, &tenant_digest)?;
        let mut nonce_bytes = [0_u8; TENANT_ENCRYPTION_NONCE_BYTES];
        SystemRandom::new()
            .fill(&mut nonce_bytes)
            .map_err(|_| storage_failure())?;
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        let aad = tenant_encryption_aad(artifact, key_version, &tenant_digest)?;
        let mut encrypted = bytes.to_vec();
        key.seal_in_place_append_tag(nonce, Aad::from(aad.as_slice()), &mut encrypted)
            .map_err(|_| storage_failure())?;
        let mut stored = Vec::with_capacity(
            TENANT_ENCRYPTION_MAGIC.len()
                + 1
                + key_version.len()
                + tenant_digest.len()
                + nonce_bytes.len()
                + encrypted.len(),
        );
        stored.extend_from_slice(TENANT_ENCRYPTION_MAGIC);
        stored.push(key_version_length);
        stored.extend_from_slice(key_version.as_bytes());
        stored.extend_from_slice(&tenant_digest);
        stored.extend_from_slice(&nonce_bytes);
        stored.extend_from_slice(&encrypted);
        Ok(stored)
    }

    fn decode_stored_bytes(
        &self,
        artifact: &ArtifactRef,
        stored: &[u8],
    ) -> Result<Vec<u8>, RepositoryError> {
        if artifact.media_type() != Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE)
            || !stored.starts_with(TENANT_ENCRYPTION_MAGIC)
        {
            return Ok(stored.to_vec());
        }
        let keyring = self
            .tenant_encryption
            .as_ref()
            .ok_or_else(RepositoryError::invalid_configuration)?;
        let key_version_length = usize::from(
            *stored
                .get(TENANT_ENCRYPTION_MAGIC.len())
                .ok_or_else(RepositoryError::invalid_data)?,
        );
        if key_version_length == 0 || key_version_length > TENANT_ENCRYPTION_MAX_KEY_VERSION_BYTES {
            return Err(RepositoryError::invalid_data());
        }
        let version_start = TENANT_ENCRYPTION_MAGIC.len() + 1;
        let version_end = version_start
            .checked_add(key_version_length)
            .ok_or_else(RepositoryError::invalid_data)?;
        let digest_end = version_end
            .checked_add(TENANT_ENCRYPTION_DIGEST_BYTES)
            .ok_or_else(RepositoryError::invalid_data)?;
        let nonce_end = digest_end
            .checked_add(TENANT_ENCRYPTION_NONCE_BYTES)
            .ok_or_else(RepositoryError::invalid_data)?;
        let key_version = std::str::from_utf8(
            stored
                .get(version_start..version_end)
                .ok_or_else(RepositoryError::invalid_data)?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        if !valid_key_version(key_version) {
            return Err(RepositoryError::invalid_data());
        }
        let tenant_digest: [u8; TENANT_ENCRYPTION_DIGEST_BYTES] = stored
            .get(version_end..digest_end)
            .ok_or_else(RepositoryError::invalid_data)?
            .try_into()
            .map_err(|_| RepositoryError::invalid_data())?;
        let nonce_bytes: [u8; TENANT_ENCRYPTION_NONCE_BYTES] = stored
            .get(digest_end..nonce_end)
            .ok_or_else(RepositoryError::invalid_data)?
            .try_into()
            .map_err(|_| RepositoryError::invalid_data())?;
        let mut encrypted = stored
            .get(nonce_end..)
            .ok_or_else(RepositoryError::invalid_data)?
            .to_vec();
        if encrypted.len() < TENANT_ENCRYPTION_TAG_BYTES {
            return Err(RepositoryError::invalid_data());
        }
        let key = keyring.encryption_key(key_version, &tenant_digest)?;
        let aad = tenant_encryption_aad(artifact, key_version, &tenant_digest)?;
        let plaintext = key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(aad.as_slice()),
                &mut encrypted,
            )
            .map_err(|_| RepositoryError::invalid_data())?
            .to_vec();
        if scoped_tenant_digest(artifact, &plaintext)? != Some(tenant_digest) {
            return Err(RepositoryError::invalid_data());
        }
        Ok(plaintext)
    }

    async fn write_object_atomically(
        &self,
        artifact: &ArtifactRef,
        path: &Path,
        stored: &[u8],
    ) -> Result<(), RepositoryError> {
        let parent = path.parent().ok_or_else(RepositoryError::invalid_data)?;
        let temporary = parent.join(format!(
            ".{}.{}.staging",
            self.hash_hex(artifact)?,
            Uuid::new_v4().simple()
        ));
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await
            .map_err(|_| storage_failure())?;
        file.write_all(stored)
            .await
            .map_err(|_| storage_failure())?;
        file.sync_all().await.map_err(|_| storage_failure())?;
        drop(file);
        match tokio::fs::rename(&temporary, path).await {
            Ok(()) => Ok(()),
            Err(_) if tokio::fs::metadata(path).await.is_ok() => {
                let _ = tokio::fs::remove_file(&temporary).await;
                Ok(())
            }
            Err(_) => {
                let _ = tokio::fs::remove_file(&temporary).await;
                Err(storage_failure())
            }
        }
    }
}

fn scoped_tenant_digest(
    artifact: &ArtifactRef,
    bytes: &[u8],
) -> Result<Option<[u8; TENANT_ENCRYPTION_DIGEST_BYTES]>, RepositoryError> {
    if artifact.media_type() != Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE) {
        return Ok(None);
    }
    let envelope =
        serde_json::from_slice::<Value>(bytes).map_err(|_| RepositoryError::invalid_data())?;
    let object = envelope
        .as_object()
        .ok_or_else(RepositoryError::invalid_data)?;
    if object.get("kind").and_then(Value::as_str) != Some(TERMINAL_SCOPED_ARTIFACT_KIND) {
        return Err(RepositoryError::invalid_data());
    }
    let scope = object
        .get("scope")
        .and_then(Value::as_str)
        .ok_or_else(RepositoryError::invalid_data)?;
    let tenant_and_scope = scope
        .strip_prefix("tenant:")
        .ok_or_else(RepositoryError::invalid_data)?;
    if tenant_and_scope.is_empty() {
        return Err(RepositoryError::invalid_data());
    }
    // The authenticated digest covers the complete tenant-prefixed scope.
    // Identity components may legally contain ':', so parsing a delimiter
    // here would collapse distinct tenants. A per-object subkey is stricter
    // than reusing one key for every object owned by the same tenant.
    let digest = digest::digest(&digest::SHA256, scope.as_bytes());
    digest
        .as_ref()
        .try_into()
        .map(Some)
        .map_err(|_| RepositoryError::invalid_data())
}

fn tenant_encryption_aad(
    artifact: &ArtifactRef,
    key_version: &str,
    tenant_digest: &[u8; TENANT_ENCRYPTION_DIGEST_BYTES],
) -> Result<Vec<u8>, RepositoryError> {
    let media_type = artifact.media_type().unwrap_or_default();
    let mut aad = Vec::with_capacity(256);
    append_aad_part(&mut aad, TENANT_ENCRYPTION_MAGIC)?;
    append_aad_part(&mut aad, key_version.as_bytes())?;
    append_aad_part(&mut aad, tenant_digest)?;
    append_aad_part(&mut aad, artifact.content_hash().as_str().as_bytes())?;
    append_aad_part(&mut aad, &artifact.size_bytes().to_be_bytes())?;
    append_aad_part(&mut aad, media_type.as_bytes())?;
    Ok(aad)
}

fn append_aad_part(output: &mut Vec<u8>, value: &[u8]) -> Result<(), RepositoryError> {
    let length = u32::try_from(value.len()).map_err(|_| RepositoryError::invalid_data())?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn stored_encryption_version(stored: &[u8]) -> Option<&str> {
    if !stored.starts_with(TENANT_ENCRYPTION_MAGIC) {
        return None;
    }
    let length = usize::from(*stored.get(TENANT_ENCRYPTION_MAGIC.len())?);
    let start = TENANT_ENCRYPTION_MAGIC.len() + 1;
    let end = start.checked_add(length)?;
    std::str::from_utf8(stored.get(start..end)?)
        .ok()
        .filter(|version| valid_key_version(version))
}

#[async_trait]
impl WorkerArtifactStore for LocalContentAddressedArtifactStore {
    fn inline_threshold_bytes(&self) -> usize {
        self.inline_threshold_bytes
    }

    fn deployment_contract(&self) -> ArtifactStoreDeploymentContract {
        self.deployment_contract.clone()
    }

    fn storage_locator(&self, artifact: &ArtifactRef) -> Result<StorageLocator, RepositoryError> {
        let hash = self.hash_hex(artifact)?;
        StorageLocator::new(format!("{STORAGE_LOCATOR_PREFIX}{}/{hash}", &hash[..2]))
    }

    async fn put_and_verify(
        &self,
        artifact: &ArtifactRef,
        bytes: &[u8],
    ) -> Result<(ContentHash, u64), RepositoryError> {
        if ContentHash::from_bytes(bytes) != *artifact.content_hash()
            || u64::try_from(bytes.len()).map_err(|_| RepositoryError::invalid_data())?
                != artifact.size_bytes()
        {
            return Err(RepositoryError::invalid_data());
        }
        let path = self.path_for(artifact)?;
        let parent = path.parent().ok_or_else(RepositoryError::invalid_data)?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|_| storage_failure())?;
        let desired = self.encode_stored_bytes(artifact, bytes)?;
        let existing = match tokio::fs::read(&path).await {
            Ok(existing) => Some(existing),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(_) => return Err(storage_failure()),
        };
        let replace = if let Some(existing) = existing.as_ref() {
            let decoded = self.decode_stored_bytes(artifact, existing)?;
            if decoded != bytes {
                return Err(RepositoryError::invalid_data());
            }
            desired.starts_with(TENANT_ENCRYPTION_MAGIC)
                && stored_encryption_version(existing)
                    != stored_encryption_version(desired.as_slice())
        } else {
            true
        };
        if replace {
            self.write_object_atomically(artifact, &path, &desired)
                .await?;
        }
        let stored = tokio::fs::read(&path)
            .await
            .map_err(|_| storage_failure())?;
        let plaintext = self.decode_stored_bytes(artifact, &stored)?;
        let actual_hash = ContentHash::from_bytes(&plaintext);
        let actual_size =
            u64::try_from(plaintext.len()).map_err(|_| RepositoryError::invalid_data())?;
        if actual_hash != *artifact.content_hash() || actual_size != artifact.size_bytes() {
            return Err(RepositoryError::invalid_data());
        }
        self.cleanup_staging_files(artifact).await?;
        Ok((actual_hash, actual_size))
    }

    async fn read_and_verify(
        &self,
        artifact: &ArtifactRef,
        locator: &StorageLocator,
        max_bytes: usize,
    ) -> Result<Vec<u8>, RepositoryError> {
        if max_bytes == 0
            || artifact.size_bytes()
                > u64::try_from(max_bytes).map_err(|_| RepositoryError::invalid_configuration())?
        {
            return Err(RepositoryError::invalid_configuration());
        }
        let path = self.locator_path(artifact, locator)?;
        let link_metadata = tokio::fs::symlink_metadata(&path)
            .await
            .map_err(|_| storage_failure())?;
        let maximum_stored_bytes = artifact
            .size_bytes()
            .checked_add(
                u64::try_from(TENANT_ENCRYPTION_MAX_OVERHEAD)
                    .map_err(|_| RepositoryError::invalid_configuration())?,
            )
            .ok_or_else(RepositoryError::invalid_configuration)?;
        if !link_metadata.file_type().is_file()
            || link_metadata.file_type().is_symlink()
            || link_metadata.len() > maximum_stored_bytes
        {
            return Err(RepositoryError::invalid_data());
        }

        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .await
            .map_err(|_| storage_failure())?;
        let metadata = file.metadata().await.map_err(|_| storage_failure())?;
        if !metadata.is_file() || metadata.len() != link_metadata.len() {
            return Err(RepositoryError::invalid_data());
        }

        let read_limit = u64::try_from(max_bytes)
            .map_err(|_| RepositoryError::invalid_configuration())?
            .checked_add(
                u64::try_from(TENANT_ENCRYPTION_MAX_OVERHEAD)
                    .map_err(|_| RepositoryError::invalid_configuration())?,
            )
            .and_then(|limit| limit.checked_add(1))
            .ok_or_else(RepositoryError::invalid_configuration)?;
        let mut stored = Vec::with_capacity(
            usize::try_from(metadata.len()).map_err(|_| RepositoryError::invalid_data())?,
        );
        file.take(read_limit)
            .read_to_end(&mut stored)
            .await
            .map_err(|_| storage_failure())?;
        if u64::try_from(stored.len()).map_err(|_| RepositoryError::invalid_data())?
            != metadata.len()
        {
            return Err(RepositoryError::invalid_data());
        }
        let bytes = self.decode_stored_bytes(artifact, &stored)?;
        let actual_size =
            u64::try_from(bytes.len()).map_err(|_| RepositoryError::invalid_data())?;
        if actual_size != artifact.size_bytes()
            || actual_size > u64::try_from(max_bytes).unwrap_or(u64::MAX)
            || ContentHash::from_bytes(&bytes) != *artifact.content_hash()
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(bytes)
    }

    async fn delete(
        &self,
        artifact: &ArtifactRef,
        locator: &StorageLocator,
    ) -> Result<(), RepositoryError> {
        let path = self.locator_path(artifact, locator)?;
        match tokio::fs::remove_file(path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(storage_failure()),
        }
        self.cleanup_staging_files(artifact).await
    }
}

async fn open_root(
    root: PathBuf,
    inline_threshold_bytes: usize,
) -> Result<PathBuf, RepositoryError> {
    if inline_threshold_bytes == 0 || root.as_os_str().is_empty() {
        return Err(RepositoryError::invalid_configuration());
    }
    tokio::fs::create_dir_all(&root)
        .await
        .map_err(|_| storage_failure())?;
    tokio::fs::canonicalize(root)
        .await
        .map_err(|_| storage_failure())
}

async fn open_shared_marker(
    root: &Path,
    namespace: &str,
) -> Result<SharedStoreMarker, RepositoryError> {
    let marker_path = root.join(SHARED_STORE_MARKER_FILE);
    let candidate_path = root.join(format!(
        ".{SHARED_STORE_MARKER_FILE}.{}.candidate",
        Uuid::new_v4().simple()
    ));
    let candidate = SharedStoreMarker::new(namespace.to_owned());
    let encoded = serde_json::to_vec(&candidate).map_err(|_| RepositoryError::invalid_data())?;
    if encoded.len() > MAX_SHARED_STORE_MARKER_BYTES {
        return Err(RepositoryError::invalid_configuration());
    }

    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&candidate_path)
        .await
        .map_err(|_| storage_failure())?;
    file.write_all(&encoded)
        .await
        .map_err(|_| storage_failure())?;
    file.sync_all().await.map_err(|_| storage_failure())?;
    drop(file);

    let publish = tokio::fs::hard_link(&candidate_path, &marker_path).await;
    let published_here = match publish {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(_) => {
            let _ = tokio::fs::remove_file(&candidate_path).await;
            return Err(storage_failure());
        }
    };
    let _ = tokio::fs::remove_file(&candidate_path).await;
    if published_here {
        let directory = tokio::fs::File::open(root)
            .await
            .map_err(|_| storage_failure())?;
        directory.sync_all().await.map_err(|_| storage_failure())?;
    }

    let marker = read_shared_marker(&marker_path).await?;
    marker.validate(namespace)?;
    Ok(marker)
}

async fn read_shared_marker(path: &Path) -> Result<SharedStoreMarker, RepositoryError> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|_| storage_failure())?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > u64::try_from(MAX_SHARED_STORE_MARKER_BYTES).unwrap_or(u64::MAX)
    {
        return Err(RepositoryError::invalid_data());
    }
    let first = tokio::fs::read(path).await.map_err(|_| storage_failure())?;
    let second = tokio::fs::read(path).await.map_err(|_| storage_failure())?;
    if first != second || first.len() > MAX_SHARED_STORE_MARKER_BYTES {
        return Err(RepositoryError::invalid_data());
    }
    serde_json::from_slice(&first).map_err(|_| RepositoryError::invalid_data())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encryption_keyring(
        active: &str,
        entries: &[(&str, &str)],
    ) -> TenantArtifactEncryptionKeyring {
        let value = Value::Object(
            entries
                .iter()
                .map(|(version, key)| ((*version).to_owned(), Value::String((*key).to_owned())))
                .collect(),
        );
        TenantArtifactEncryptionKeyring::from_secret_json(
            active,
            &serde_json::to_string(&value).unwrap(),
        )
        .unwrap()
    }

    fn scoped_bytes(tenant: &str, scope: &str, marker: &str) -> Vec<u8> {
        serde_jcs::to_vec(&serde_json::json!({
            "kind": TERMINAL_SCOPED_ARTIFACT_KIND,
            "scope": format!("tenant:{tenant}:{scope}"),
            "value": {"marker": marker},
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn local_store_is_content_addressed_verified_and_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalContentAddressedArtifactStore::open(directory.path().join("objects"), 8)
            .await
            .unwrap();
        assert_eq!(
            store.deployment_contract(),
            ArtifactStoreDeploymentContract::single_process_local()
        );
        let bytes = br#"{"answer":"large"}"#;
        let artifact = store
            .artifact_for_bytes(bytes, Some("application/json".to_owned()))
            .unwrap();
        let locator = store.storage_locator(&artifact).unwrap();
        let hash = store.hash_hex(&artifact).unwrap();
        assert_eq!(
            locator.expose_to_storage_adapter(),
            format!("{STORAGE_LOCATOR_PREFIX}{}/{hash}", &hash[..2])
        );
        assert!(!locator
            .expose_to_storage_adapter()
            .contains(directory.path().to_string_lossy().as_ref()));
        let path = store.path_for(&artifact).unwrap();
        let first = store.put_and_verify(&artifact, bytes).await.unwrap();
        let second = store.put_and_verify(&artifact, bytes).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(first.0, *artifact.content_hash());
        let staging = path.parent().unwrap().join(format!(
            ".{}.crash.staging",
            store.hash_hex(&artifact).unwrap()
        ));
        tokio::fs::write(&staging, b"partial").await.unwrap();
        store.delete(&artifact, &locator).await.unwrap();
        assert!(!staging.exists());
        store.delete(&artifact, &locator).await.unwrap();
    }

    #[tokio::test]
    async fn authorized_read_is_bounded_and_revalidates_locator_size_and_hash() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalContentAddressedArtifactStore::open(directory.path().join("objects"), 8)
            .await
            .unwrap();
        let bytes = b"public artifact";
        let artifact = store
            .artifact_for_bytes(bytes, Some("text/plain".to_owned()))
            .unwrap();
        let locator = store.storage_locator(&artifact).unwrap();
        store.put_and_verify(&artifact, bytes).await.unwrap();

        assert_eq!(
            store
                .read_and_verify(&artifact, &locator, bytes.len())
                .await
                .unwrap(),
            bytes
        );
        assert_eq!(
            store
                .read_and_verify(&artifact, &locator, bytes.len() - 1)
                .await
                .unwrap_err()
                .code(),
            REPOSITORY_CONFIGURATION_INVALID
        );

        let other = store.artifact_for_bytes(b"other", None).unwrap();
        assert_eq!(
            store
                .read_and_verify(
                    &artifact,
                    &store.storage_locator(&other).unwrap(),
                    bytes.len()
                )
                .await
                .unwrap_err()
                .code(),
            REPOSITORY_DATA_INVALID
        );

        tokio::fs::write(store.path_for(&artifact).unwrap(), b"tampered value!")
            .await
            .unwrap();
        assert_eq!(
            store
                .read_and_verify(&artifact, &locator, bytes.len())
                .await
                .unwrap_err()
                .code(),
            REPOSITORY_DATA_INVALID
        );
    }

    #[tokio::test]
    async fn local_store_rejects_tampered_existing_content() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalContentAddressedArtifactStore::open(directory.path().join("objects"), 8)
            .await
            .unwrap();
        let bytes = b"expected";
        let artifact = store.artifact_for_bytes(bytes, None).unwrap();
        let path = store.path_for(&artifact).unwrap();
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(path, b"tampered").await.unwrap();
        assert_eq!(
            store
                .put_and_verify(&artifact, bytes)
                .await
                .unwrap_err()
                .code(),
            REPOSITORY_DATA_INVALID
        );
    }

    #[tokio::test]
    async fn tenant_scoped_objects_are_encrypted_authenticated_and_deleted() {
        let directory = tempfile::tempdir().unwrap();
        let keyring = encryption_keyring(
            "v1",
            &[(
                "v1",
                "1111111111111111111111111111111111111111111111111111111111111111",
            )],
        );
        let store = LocalContentAddressedArtifactStore::open_with_tenant_encryption(
            directory.path().join("objects"),
            8,
            keyring,
        )
        .await
        .unwrap();
        let bytes = scoped_bytes("tenant-a", "message:one", "PRIVATE-MARKER");
        let artifact = store
            .artifact_for_bytes(&bytes, Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE.to_owned()))
            .unwrap();
        let locator = store.storage_locator(&artifact).unwrap();
        store.put_and_verify(&artifact, &bytes).await.unwrap();

        let path = store.path_for(&artifact).unwrap();
        let stored = tokio::fs::read(&path).await.unwrap();
        assert!(stored.starts_with(TENANT_ENCRYPTION_MAGIC));
        assert_eq!(stored_encryption_version(&stored), Some("v1"));
        assert!(!stored
            .windows(b"PRIVATE-MARKER".len())
            .any(|window| window == b"PRIVATE-MARKER"));
        assert!(!stored
            .windows(b"tenant-a".len())
            .any(|window| window == b"tenant-a"));
        assert_eq!(
            store
                .read_and_verify(&artifact, &locator, bytes.len())
                .await
                .unwrap(),
            bytes
        );

        let digest_offset = TENANT_ENCRYPTION_MAGIC.len() + 1 + "v1".len();
        let mut tampered = stored;
        tampered[digest_offset] ^= 1;
        tokio::fs::write(&path, tampered).await.unwrap();
        assert_eq!(
            store
                .read_and_verify(&artifact, &locator, bytes.len())
                .await
                .unwrap_err()
                .code(),
            REPOSITORY_DATA_INVALID
        );
        store.delete(&artifact, &locator).await.unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn tenant_key_rotation_reads_old_versions_and_rewrites_legacy_plaintext() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("objects");
        let legacy = LocalContentAddressedArtifactStore::open(root.clone(), 8)
            .await
            .unwrap();
        let legacy_bytes = scoped_bytes("tenant-a", "message:legacy", "legacy");
        let legacy_artifact = legacy
            .artifact_for_bytes(
                &legacy_bytes,
                Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE.to_owned()),
            )
            .unwrap();
        legacy
            .put_and_verify(&legacy_artifact, &legacy_bytes)
            .await
            .unwrap();
        assert!(!tokio::fs::read(legacy.path_for(&legacy_artifact).unwrap())
            .await
            .unwrap()
            .starts_with(TENANT_ENCRYPTION_MAGIC));

        let rotating = LocalContentAddressedArtifactStore::open_with_tenant_encryption(
            root.clone(),
            8,
            encryption_keyring(
                "v2",
                &[
                    (
                        "v1",
                        "1111111111111111111111111111111111111111111111111111111111111111",
                    ),
                    (
                        "v2",
                        "2222222222222222222222222222222222222222222222222222222222222222",
                    ),
                ],
            ),
        )
        .await
        .unwrap();
        rotating
            .put_and_verify(&legacy_artifact, &legacy_bytes)
            .await
            .unwrap();
        let migrated = tokio::fs::read(rotating.path_for(&legacy_artifact).unwrap())
            .await
            .unwrap();
        assert_eq!(stored_encryption_version(&migrated), Some("v2"));

        let old_bytes = scoped_bytes("tenant-a", "message:old-v1", "old");
        let old_artifact = rotating
            .artifact_for_bytes(
                &old_bytes,
                Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE.to_owned()),
            )
            .unwrap();
        let v1_store = LocalContentAddressedArtifactStore::open_with_tenant_encryption(
            root.clone(),
            8,
            encryption_keyring(
                "v1",
                &[(
                    "v1",
                    "1111111111111111111111111111111111111111111111111111111111111111",
                )],
            ),
        )
        .await
        .unwrap();
        v1_store
            .put_and_verify(&old_artifact, &old_bytes)
            .await
            .unwrap();
        assert_eq!(
            rotating
                .read_and_verify(
                    &old_artifact,
                    &rotating.storage_locator(&old_artifact).unwrap(),
                    old_bytes.len(),
                )
                .await
                .unwrap(),
            old_bytes
        );

        let v2_only = LocalContentAddressedArtifactStore::open_with_tenant_encryption(
            root,
            8,
            encryption_keyring(
                "v2",
                &[(
                    "v2",
                    "2222222222222222222222222222222222222222222222222222222222222222",
                )],
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            v2_only
                .read_and_verify(
                    &legacy_artifact,
                    &v2_only.storage_locator(&legacy_artifact).unwrap(),
                    legacy_bytes.len(),
                )
                .await
                .unwrap(),
            legacy_bytes
        );
        assert_eq!(
            v2_only
                .read_and_verify(
                    &old_artifact,
                    &v2_only.storage_locator(&old_artifact).unwrap(),
                    old_bytes.len(),
                )
                .await
                .unwrap_err()
                .code(),
            REPOSITORY_CONFIGURATION_INVALID
        );
    }

    #[tokio::test]
    async fn delete_rejects_locator_for_another_hash_and_malformed_relative_paths() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalContentAddressedArtifactStore::open(directory.path().join("objects"), 8)
            .await
            .unwrap();
        let first = store.artifact_for_bytes(b"first", None).unwrap();
        let second = store.artifact_for_bytes(b"second", None).unwrap();
        store.put_and_verify(&first, b"first").await.unwrap();
        let first_path = store.path_for(&first).unwrap();

        let second_locator = store.storage_locator(&second).unwrap();
        assert_eq!(
            store
                .delete(&first, &second_locator)
                .await
                .unwrap_err()
                .code(),
            REPOSITORY_DATA_INVALID
        );
        assert!(first_path.exists());

        let hash = store.hash_hex(&first).unwrap();
        let malformed =
            StorageLocator::new(format!("{STORAGE_LOCATOR_PREFIX}../{}/{hash}", &hash[..2]))
                .unwrap();
        assert_eq!(
            store.delete(&first, &malformed).await.unwrap_err().code(),
            REPOSITORY_DATA_INVALID
        );
        assert!(first_path.exists());

        store
            .delete(&first, &store.storage_locator(&first).unwrap())
            .await
            .unwrap();
        assert!(!first_path.exists());
    }

    #[tokio::test]
    async fn concurrent_shared_opens_publish_one_stable_identity() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("objects");
        let mut handles = Vec::new();
        for _ in 0..16 {
            let root = root.clone();
            handles.push(tokio::spawn(async move {
                LocalContentAddressedArtifactStore::open_shared(root, 8, "production")
                    .await
                    .unwrap()
                    .deployment_contract()
            }));
        }
        let mut contracts = Vec::new();
        for handle in handles {
            contracts.push(handle.await.unwrap());
        }
        let expected = contracts.first().unwrap();
        assert_eq!(
            expected.capability(),
            ArtifactStoreDeploymentCapability::SharedFilesystem
        );
        assert_eq!(expected.namespace(), Some("production"));
        assert!(expected.store_id().is_some());
        assert!(contracts.iter().all(|contract| contract == expected));
    }

    #[tokio::test]
    async fn shared_identity_survives_root_aliases_and_separates_distinct_roots() {
        let directory = tempfile::tempdir().unwrap();
        let first_root = directory.path().join("first");
        let first =
            LocalContentAddressedArtifactStore::open_shared(first_root.clone(), 8, "production")
                .await
                .unwrap();
        let aliased = LocalContentAddressedArtifactStore::open_shared(
            first_root.join("..").join("first"),
            8,
            "production",
        )
        .await
        .unwrap();
        assert_eq!(first.deployment_contract(), aliased.deployment_contract());

        let bytes = b"shared object";
        let artifact = first.artifact_for_bytes(bytes, None).unwrap();
        first.put_and_verify(&artifact, bytes).await.unwrap();
        aliased
            .delete(&artifact, &first.storage_locator(&artifact).unwrap())
            .await
            .unwrap();
        assert!(!first.path_for(&artifact).unwrap().exists());

        let second = LocalContentAddressedArtifactStore::open_shared(
            directory.path().join("second"),
            8,
            "production",
        )
        .await
        .unwrap();
        assert_ne!(
            first.deployment_contract().store_id(),
            second.deployment_contract().store_id()
        );
    }

    #[tokio::test]
    async fn shared_open_fails_closed_for_namespace_conflict_and_marker_tampering() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("objects");
        LocalContentAddressedArtifactStore::open_shared(root.clone(), 8, "production")
            .await
            .unwrap();
        assert_eq!(
            LocalContentAddressedArtifactStore::open_shared(root.clone(), 8, "staging")
                .await
                .unwrap_err()
                .code(),
            REPOSITORY_CONFIGURATION_INVALID
        );
        assert_eq!(
            LocalContentAddressedArtifactStore::open_shared(root.clone(), 8, "../invalid")
                .await
                .unwrap_err()
                .code(),
            REPOSITORY_CONFIGURATION_INVALID
        );

        tokio::fs::write(
            root.join(SHARED_STORE_MARKER_FILE),
            br#"{"schema_version":1,"namespace":"production","store_id":"forged","unknown":true}"#,
        )
        .await
        .unwrap();
        assert_eq!(
            LocalContentAddressedArtifactStore::open_shared(root, 8, "production")
                .await
                .unwrap_err()
                .code(),
            REPOSITORY_DATA_INVALID
        );
    }
}
