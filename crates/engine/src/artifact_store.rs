use async_trait::async_trait;

use crate::{
    repository::{adapter as repository_adapter, RepositoryError, StorageLocator},
    ArtifactId, ArtifactRef, ContentHash,
};

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

fn storage_failure() -> RepositoryError {
    repository_adapter::repository_error(
        crate::repository::REPOSITORY_STORAGE_FAILURE,
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

    /// Reads one repository-authorized object under an explicit caller bound
    /// and verifies the complete byte identity before returning any content.
    ///
    /// Authorization belongs to the durable metadata repository; stores must
    /// only accept its private locator and must never derive a path from an
    /// HTTP parameter. The default is deliberately unavailable so a write-only
    /// adapter cannot accidentally become a public read adapter.
    async fn read_and_verify(
        &self,
        _artifact: &ArtifactRef,
        _locator: &StorageLocator,
        _max_bytes: usize,
    ) -> Result<Vec<u8>, RepositoryError> {
        Err(storage_failure())
    }

    /// Idempotent deletion used after a durable orphan-GC claim.
    async fn delete(
        &self,
        artifact: &ArtifactRef,
        locator: &StorageLocator,
    ) -> Result<(), RepositoryError>;
}

/// Workspace-internal constructors used by concrete Artifact store adapters.
#[doc(hidden)]
pub mod adapter {
    use super::ArtifactStoreDeploymentContract;

    pub fn shared_deployment_contract(
        store_id: String,
        namespace: String,
    ) -> ArtifactStoreDeploymentContract {
        ArtifactStoreDeploymentContract::shared(store_id, namespace)
    }
}
