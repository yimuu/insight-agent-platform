//! Explicit durable Schema provisioning for root integration tests.
//!
//! Production repository constructors deliberately cannot execute these
//! assets. Every test that needs a database must install the Schema first and
//! only then connect the repository.

#![allow(dead_code)]

use std::{fs::File, path::Path, sync::Arc};

use async_trait::async_trait;
use insight_agent_platform::engine::{
    repository::{RepositoryError, SqliteDurableRepository, StorageLocator},
    ArtifactRef, ContentHash, WorkerArtifactStore,
};
use insight_engine::artifact_store::adapter as artifact_store_adapter;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    PgPool,
};
use tempfile::TempDir;

pub(crate) const DURABLE_SCHEMA_CONTRACT_ID: &str =
    "durable-schema-bc893e0d-33b5-4a90-9aa3-1db4f6d17c87";
pub(crate) const POSTGRES_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/database/durable/postgres/schema.sql"
));
pub(crate) const SQLITE_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/database/durable/sqlite/schema.sql"
));

/// Hermetic test double for the production S3 deployment capability. The
/// delegated store supplies local bytes only inside tests.
pub(crate) struct TestS3ArtifactStore {
    inner: Arc<dyn WorkerArtifactStore>,
    store_id: String,
    namespace: String,
}

impl TestS3ArtifactStore {
    pub(crate) fn new(
        inner: Arc<dyn WorkerArtifactStore>,
        store_id: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            store_id: store_id.into(),
            namespace: namespace.into(),
        }
    }
}

#[async_trait]
impl WorkerArtifactStore for TestS3ArtifactStore {
    fn inline_threshold_bytes(&self) -> usize {
        self.inner.inline_threshold_bytes()
    }

    fn deployment_contract(
        &self,
    ) -> insight_agent_platform::engine::ArtifactStoreDeploymentContract {
        artifact_store_adapter::s3_deployment_contract(
            self.store_id.clone(),
            self.namespace.clone(),
        )
    }

    fn artifact_for_bytes(
        &self,
        bytes: &[u8],
        media_type: Option<String>,
    ) -> Result<ArtifactRef, RepositoryError> {
        self.inner.artifact_for_bytes(bytes, media_type)
    }

    fn storage_locator(&self, artifact: &ArtifactRef) -> Result<StorageLocator, RepositoryError> {
        self.inner.storage_locator(artifact)
    }

    fn storage_locator_for_tenant(
        &self,
        namespace: &str,
        tenant_id: &str,
        artifact: &ArtifactRef,
    ) -> Result<StorageLocator, RepositoryError> {
        self.inner
            .storage_locator_for_tenant(namespace, tenant_id, artifact)
    }

    async fn put_and_verify(
        &self,
        artifact: &ArtifactRef,
        bytes: &[u8],
    ) -> Result<(ContentHash, u64), RepositoryError> {
        self.inner.put_and_verify(artifact, bytes).await
    }

    async fn put_and_verify_at(
        &self,
        artifact: &ArtifactRef,
        locator: &StorageLocator,
        bytes: &[u8],
    ) -> Result<(ContentHash, u64), RepositoryError> {
        self.inner.put_and_verify_at(artifact, locator, bytes).await
    }

    async fn read_and_verify(
        &self,
        artifact: &ArtifactRef,
        locator: &StorageLocator,
        max_bytes: usize,
    ) -> Result<Vec<u8>, RepositoryError> {
        self.inner
            .read_and_verify(artifact, locator, max_bytes)
            .await
    }

    async fn delete(
        &self,
        artifact: &ArtifactRef,
        locator: &StorageLocator,
    ) -> Result<(), RepositoryError> {
        self.inner.delete(artifact, locator).await
    }
}

pub(crate) async fn test_s3_artifact_store(
    root: impl AsRef<Path>,
    inline_threshold_bytes: usize,
    namespace: &str,
) -> Arc<TestS3ArtifactStore> {
    let inner = Arc::new(
        insight_agent_platform::engine::LocalContentAddressedArtifactStore::open_shared(
            root.as_ref().to_path_buf(),
            inline_threshold_bytes,
            namespace,
        )
        .await
        .unwrap(),
    );
    let store_id = inner
        .deployment_contract()
        .store_id()
        .expect("test shared store has a stable identity")
        .to_owned();
    Arc::new(TestS3ArtifactStore::new(
        inner,
        store_id,
        namespace.to_owned(),
    ))
}

pub(crate) async fn provision_sqlite_database(path: &Path) {
    assert!(
        !path.exists(),
        "SQLite provisioning target must not exist: {}",
        path.display()
    );
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    File::create(path).unwrap();
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::raw_sql(SQLITE_SCHEMA).execute(&pool).await.unwrap();
    pool.close().await;
}

pub(crate) async fn temporary_sqlite_repository() -> (TempDir, SqliteDurableRepository) {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("durable.sqlite3");
    provision_sqlite_database(&database).await;
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    (temporary, repository)
}

pub(crate) async fn provision_postgres_schema(pool: &PgPool) {
    sqlx::raw_sql(POSTGRES_SCHEMA).execute(pool).await.unwrap();
}

pub(crate) async fn provision_postgres_url(database_url: &str) {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .unwrap();
    provision_postgres_schema(&pool).await;
    pool.close().await;
}
