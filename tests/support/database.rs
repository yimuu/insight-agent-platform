//! Explicit durable Schema provisioning for root integration tests.
//!
//! Production repository constructors deliberately cannot execute these
//! assets. Every test that needs a database must install the Schema first and
//! only then connect the repository.

#![allow(dead_code)]

use std::{fs::File, path::Path};

use insight_agent_platform::engine::repository::SqliteDurableRepository;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    PgPool,
};
use tempfile::TempDir;

pub(crate) const DURABLE_SCHEMA_CONTRACT_ID: &str =
    "durable-schema-7f3c2a8e-6d54-4b91-9ac0-2e75f186bd43";
pub(crate) const POSTGRES_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/database/durable/postgres/schema.sql"
));
pub(crate) const SQLITE_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/database/durable/sqlite/schema.sql"
));

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
