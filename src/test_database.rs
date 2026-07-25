//! Explicit Schema provisioning helpers for root-crate unit tests.
//!
//! The checked-in DDL is read only by test builds and is never embedded in the
//! service or storage library.

use std::path::Path;

use insight_storage::SqliteDurableRepository;
use sqlx::{
    postgres::PgPoolOptions,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    AssertSqlSafe, PgPool,
};

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
