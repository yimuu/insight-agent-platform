mod support;

use std::fs::File;

use insight_storage::{
    SqliteDurableRepository, DATABASE_SCHEMA_BACKEND_MISMATCH, DATABASE_SCHEMA_CONTRACT_MISMATCH,
    DATABASE_SCHEMA_NOT_INITIALIZED, DURABLE_SCHEMA_CONTRACT_ID,
};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    SqlitePool,
};

async fn inspection_pool(path: &std::path::Path) -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(false)
                .foreign_keys(true),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn sqlite_missing_file_is_rejected_without_creating_it() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("missing.sqlite3");
    let error = SqliteDurableRepository::connect_path(&database)
        .await
        .err()
        .expect("a missing SQLite file must be rejected");
    assert_eq!(error.code(), DATABASE_SCHEMA_NOT_INITIALIZED);
    assert!(
        !database.exists(),
        "repository connect must not create a missing SQLite file"
    );
}

#[tokio::test]
async fn sqlite_empty_file_and_partial_schema_are_not_initialized() {
    let temporary = tempfile::tempdir().unwrap();
    let empty = temporary.path().join("empty.sqlite3");
    File::create(&empty).unwrap();
    let error = SqliteDurableRepository::connect_path(&empty)
        .await
        .err()
        .expect("an empty SQLite file must be rejected");
    assert_eq!(error.code(), DATABASE_SCHEMA_NOT_INITIALIZED);

    let partial = temporary.path().join("partial.sqlite3");
    File::create(&partial).unwrap();
    let pool = inspection_pool(&partial).await;
    sqlx::query("CREATE TABLE workflow_runs (partial_marker INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let error = SqliteDurableRepository::connect_path(&partial)
        .await
        .err()
        .expect("a partial SQLite Schema must be rejected");
    assert_eq!(error.code(), DATABASE_SCHEMA_NOT_INITIALIZED);
}

#[tokio::test]
async fn sqlite_legacy_contract_and_wrong_backend_fail_closed() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("wrong-contract.sqlite3");
    support::provision_sqlite_database(&database).await;
    let pool = inspection_pool(&database).await;

    sqlx::query(
        "UPDATE durable_schema_contract
         SET contract_id='durable-schema-d98dcd93-4911-426d-a826-9d8a5b04b461'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let error = SqliteDurableRepository::connect_path(&database)
        .await
        .err()
        .expect("the pre-run-stream/v1 contract ID must be rejected");
    assert_eq!(error.code(), DATABASE_SCHEMA_CONTRACT_MISMATCH);

    sqlx::query(
        "UPDATE durable_schema_contract
         SET contract_id=?,backend='postgres'",
    )
    .bind(DURABLE_SCHEMA_CONTRACT_ID)
    .execute(&pool)
    .await
    .unwrap();
    let error = SqliteDurableRepository::connect_path(&database)
        .await
        .err()
        .expect("a wrong backend must be rejected");
    assert_eq!(error.code(), DATABASE_SCHEMA_BACKEND_MISMATCH);
    pool.close().await;
}

#[tokio::test]
async fn sqlite_repository_restart_does_not_change_contract_or_schema_objects() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("restart.sqlite3");
    support::provision_sqlite_database(&database).await;
    let pool = inspection_pool(&database).await;
    let before_contract = sqlx::query_as::<_, (String, String, String)>(
        "SELECT contract_id,backend,installed_at
         FROM durable_schema_contract WHERE singleton=1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let before_objects = sqlx::query_as::<_, (String, String, String)>(
        "SELECT type,name,sql FROM sqlite_master
         WHERE name NOT LIKE 'sqlite_%'
         ORDER BY type,name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    repository.validate_schema_contract().await.unwrap();
    drop(repository);
    let restarted = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    drop(restarted);

    assert_eq!(
        sqlx::query_as::<_, (String, String, String)>(
            "SELECT contract_id,backend,installed_at
             FROM durable_schema_contract WHERE singleton=1",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        before_contract
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String, String)>(
            "SELECT type,name,sql FROM sqlite_master
             WHERE name NOT LIKE 'sqlite_%'
             ORDER BY type,name",
        )
        .fetch_all(&pool)
        .await
        .unwrap(),
        before_objects
    );
    pool.close().await;
}
