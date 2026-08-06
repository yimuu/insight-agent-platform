//! Explicit Schema provisioning helpers for root-crate unit tests.
//!
//! The checked-in DDL is read only by test builds and is never embedded in the
//! service or storage library.

use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use insight_engine::{
    artifact_store::{adapter as artifact_store_adapter, WorkerArtifactStore},
    repository::{RepositoryError, StorageLocator},
    ArtifactRef, ContentHash,
};
use insight_storage::SqliteDurableRepository;
use sqlx::{
    postgres::PgPoolOptions,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    AssertSqlSafe, PgPool,
};

pub(crate) struct TestS3ArtifactStore {
    inner: Arc<dyn WorkerArtifactStore>,
    store_id: String,
    namespace: String,
}

#[async_trait]
impl WorkerArtifactStore for TestS3ArtifactStore {
    fn inline_threshold_bytes(&self) -> usize {
        self.inner.inline_threshold_bytes()
    }

    fn deployment_contract(
        &self,
    ) -> insight_engine::artifact_store::ArtifactStoreDeploymentContract {
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
        insight_storage::artifact_store::LocalContentAddressedArtifactStore::open_shared(
            root.as_ref().to_path_buf(),
            inline_threshold_bytes,
            namespace,
        )
        .await
        .unwrap(),
    );
    let store_id = inner.deployment_contract().store_id().unwrap().to_owned();
    Arc::new(TestS3ArtifactStore {
        inner,
        store_id,
        namespace: namespace.to_owned(),
    })
}

async fn schema_for(backend: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("database")
        .join("durable")
        .join(backend)
        .join("schema.sql");
    tokio::fs::read_to_string(path)
        .await
        .expect("the checked-in durable test Schema must be readable")
}

pub(crate) async fn provision_postgres_pool(pool: &PgPool) {
    let schema = schema_for("postgres").await;
    sqlx::raw_sql(AssertSqlSafe(schema))
        .execute(pool)
        .await
        .expect("the PostgreSQL durable test Schema must provision an empty target");
}

pub(crate) async fn provision_postgres_url(database_url: &str) {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .expect("the PostgreSQL test target must be reachable");
    provision_postgres_pool(&pool).await;
    pool.close().await;
}

pub(crate) async fn provision_sqlite_path(path: &Path) {
    assert!(
        !path.exists(),
        "SQLite test provisioning requires a missing target file"
    );
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("SQLite test parent directory must be writable");
    }
    std::fs::File::create(path).expect("SQLite test target must be creatable");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(false)
                .foreign_keys(true),
        )
        .await
        .expect("the SQLite test target must be reachable");
    let schema = schema_for("sqlite").await;
    sqlx::raw_sql(AssertSqlSafe(schema))
        .execute(&pool)
        .await
        .expect("the SQLite durable test Schema must provision an empty target");
    pool.close().await;
}

pub(crate) async fn provisioned_sqlite_repository(path: &Path) -> SqliteDurableRepository {
    provision_sqlite_path(path).await;
    SqliteDurableRepository::connect_path(path)
        .await
        .expect("the provisioned SQLite test repository must validate")
}

pub(crate) async fn sqlite_in_memory_repository() -> SqliteDurableRepository {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(":memory:")
                .foreign_keys(true),
        )
        .await
        .expect("the in-memory SQLite test target must be reachable");
    let schema = schema_for("sqlite").await;
    sqlx::raw_sql(AssertSqlSafe(schema))
        .execute(&pool)
        .await
        .expect("the SQLite durable test Schema must provision an empty target");
    SqliteDurableRepository::from_pool(pool)
        .await
        .expect("the provisioned SQLite test repository must validate")
}
