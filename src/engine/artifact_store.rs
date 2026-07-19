//! External content-addressed storage adapter for large worker values.
//!
//! Database rows remain the metadata/reference authority. This adapter owns
//! only idempotent object writes, byte verification, and deletion. It never
//! claims external exactly-once I/O.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use super::{ArtifactId, ArtifactRef, ContentHash};
use crate::engine::repository::{RepositoryError, StorageLocator, REPOSITORY_STORAGE_FAILURE};

const STORAGE_LOCATOR_PREFIX: &str = "content-addressed:v1/sha256/";
const SHARED_STORE_MARKER_FILE: &str = ".insight-agent-artifact-store-v1.json";
const SHARED_STORE_MARKER_SCHEMA_VERSION: u32 = 1;
const MAX_SHARED_STORE_MARKER_BYTES: usize = 4 * 1024;
const MAX_SHARED_STORE_NAMESPACE_BYTES: usize = 128;

/// Declares whether an Artifact store can be shared by independent runtime
/// processes. Capability is a deployment property, not an inference from a
/// filesystem path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactStoreDeploymentCapability {
    SingleProcessLocal,
    SharedFilesystem,
}

/// Stable storage identity used by deployment validation. Local stores have no
/// cross-process identity; shared stores expose the random identity persisted
/// in their closed root marker together with its namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactStoreDeploymentContract {
    capability: ArtifactStoreDeploymentCapability,
    store_id: Option<String>,
    namespace: Option<String>,
}

impl ArtifactStoreDeploymentContract {
    pub fn single_process_local() -> Self {
        Self {
            capability: ArtifactStoreDeploymentCapability::SingleProcessLocal,
            store_id: None,
            namespace: None,
        }
    }

    fn shared(store_id: String, namespace: String) -> Self {
        Self {
            capability: ArtifactStoreDeploymentCapability::SharedFilesystem,
            store_id: Some(store_id),
            namespace: Some(namespace),
        }
    }

    pub fn capability(&self) -> ArtifactStoreDeploymentCapability {
        self.capability
    }

    pub fn store_id(&self) -> Option<&str> {
        self.store_id.as_deref()
    }

    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
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

/// External byte-store boundary used before a fenced result transaction.
#[async_trait]
pub trait WorkerArtifactStore: Send + Sync {
    fn inline_threshold_bytes(&self) -> usize;

    /// Deployment capability defaults to process-local. A new adapter cannot
    /// accidentally become production-shareable merely by implementing byte
    /// operations.
    fn deployment_contract(&self) -> ArtifactStoreDeploymentContract {
        ArtifactStoreDeploymentContract::single_process_local()
    }

    fn artifact_for_bytes(
        &self,
        bytes: &[u8],
        media_type: Option<String>,
    ) -> Result<ArtifactRef, RepositoryError> {
        let hash = ContentHash::from_bytes(bytes);
        let artifact_id = ArtifactId::new(format!(
            "artifact_{}",
            hash.as_str().trim_start_matches("sha256:")
        ))
        .map_err(|_| RepositoryError::invalid_data())?;
        ArtifactRef::new(
            artifact_id,
            hash,
            u64::try_from(bytes.len()).map_err(|_| RepositoryError::invalid_data())?,
            media_type,
        )
        .map_err(|_| RepositoryError::invalid_data())
    }

    /// Returns the private, durable locator before upload so a staged metadata
    /// row can be committed first and recover an upload-time process crash.
    fn storage_locator(&self, artifact: &ArtifactRef) -> Result<StorageLocator, RepositoryError>;

    /// Idempotently writes and then re-reads the complete object. Returned
    /// metadata is derived from the stored bytes, never trusted from input.
    async fn put_and_verify(
        &self,
        artifact: &ArtifactRef,
        bytes: &[u8],
    ) -> Result<(ContentHash, u64), RepositoryError>;

    /// Idempotent deletion used after a durable orphan-GC claim.
    async fn delete(
        &self,
        artifact: &ArtifactRef,
        locator: &StorageLocator,
    ) -> Result<(), RepositoryError>;
}

/// Production-capable local filesystem store. Paths are derived exclusively
/// from lowercase SHA-256 content hashes and cannot contain author input.
#[derive(Debug, Clone)]
pub struct LocalContentAddressedArtifactStore {
    root: Arc<PathBuf>,
    inline_threshold_bytes: usize,
    deployment_contract: ArtifactStoreDeploymentContract,
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
            deployment_contract: ArtifactStoreDeploymentContract::shared(
                marker.store_id,
                marker.namespace,
            ),
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
        if tokio::fs::metadata(&path).await.is_err() {
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
            file.write_all(bytes).await.map_err(|_| storage_failure())?;
            file.sync_all().await.map_err(|_| storage_failure())?;
            drop(file);
            match tokio::fs::rename(&temporary, &path).await {
                Ok(()) => {}
                Err(_) if tokio::fs::metadata(&path).await.is_ok() => {
                    let _ = tokio::fs::remove_file(&temporary).await;
                }
                Err(_) => {
                    let _ = tokio::fs::remove_file(&temporary).await;
                    return Err(storage_failure());
                }
            }
        }
        let stored = tokio::fs::read(&path)
            .await
            .map_err(|_| storage_failure())?;
        let actual_hash = ContentHash::from_bytes(&stored);
        let actual_size =
            u64::try_from(stored.len()).map_err(|_| RepositoryError::invalid_data())?;
        if actual_hash != *artifact.content_hash() || actual_size != artifact.size_bytes() {
            return Err(RepositoryError::invalid_data());
        }
        self.cleanup_staging_files(artifact).await?;
        Ok((actual_hash, actual_size))
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
            crate::engine::repository::REPOSITORY_DATA_INVALID
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
            crate::engine::repository::REPOSITORY_DATA_INVALID
        );
        assert!(first_path.exists());

        let hash = store.hash_hex(&first).unwrap();
        let malformed =
            StorageLocator::new(format!("{STORAGE_LOCATOR_PREFIX}../{}/{hash}", &hash[..2]))
                .unwrap();
        assert_eq!(
            store.delete(&first, &malformed).await.unwrap_err().code(),
            crate::engine::repository::REPOSITORY_DATA_INVALID
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
            crate::engine::repository::REPOSITORY_CONFIGURATION_INVALID
        );
        assert_eq!(
            LocalContentAddressedArtifactStore::open_shared(root.clone(), 8, "../invalid")
                .await
                .unwrap_err()
                .code(),
            crate::engine::repository::REPOSITORY_CONFIGURATION_INVALID
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
            crate::engine::repository::REPOSITORY_DATA_INVALID
        );
    }
}
