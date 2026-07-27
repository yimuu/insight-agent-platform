//! Runtime identity for the pre-provisioned durable database Schema.
//!
//! The service reads this contract once while constructing a repository. Schema
//! installation remains an external deployment/test responsibility; this
//! module intentionally contains no DDL or Schema repair path.

use insight_engine::repository::RepositoryError;

use super::RepositoryErrorExt as _;

/// Opaque identity shared by the PostgreSQL and SQLite durable Schemas.
pub const DURABLE_SCHEMA_CONTRACT_ID: &str = "durable-schema-cd9a5c3f-5f12-46d2-ab96-78820a13186f";

pub const POSTGRES_SCHEMA_BACKEND: &str = "postgres";
pub const SQLITE_SCHEMA_BACKEND: &str = "sqlite";

pub const DATABASE_SCHEMA_NOT_INITIALIZED: &str = "DATABASE_SCHEMA_NOT_INITIALIZED";
pub const DATABASE_SCHEMA_CONTRACT_MISMATCH: &str = "DATABASE_SCHEMA_CONTRACT_MISMATCH";
pub const DATABASE_SCHEMA_BACKEND_MISMATCH: &str = "DATABASE_SCHEMA_BACKEND_MISMATCH";

pub(crate) fn validate_contract_row(
    row: Option<(String, String)>,
    expected_backend: &'static str,
) -> Result<(), RepositoryError> {
    let Some((contract_id, backend)) = row else {
        return Err(RepositoryError::schema_not_initialized());
    };
    if contract_id != DURABLE_SCHEMA_CONTRACT_ID {
        return Err(RepositoryError::schema_contract_mismatch());
    }
    if backend != expected_backend {
        return Err(RepositoryError::schema_backend_mismatch());
    }
    Ok(())
}

#[cfg(test)]
async fn read_schema_for_test(backend: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("database")
        .join("durable")
        .join(backend)
        .join("schema.sql");
    tokio::fs::read_to_string(path)
        .await
        .expect("the checked-in durable test Schema must be readable")
}

#[cfg(test)]
pub(crate) async fn provision_postgres_for_test(pool: &sqlx::PgPool) {
    use sqlx::AssertSqlSafe;

    let schema = read_schema_for_test(POSTGRES_SCHEMA_BACKEND).await;
    sqlx::raw_sql(AssertSqlSafe(schema))
        .execute(pool)
        .await
        .expect("the PostgreSQL durable test Schema must provision an empty target");
}

#[cfg(test)]
pub(crate) async fn provision_sqlite_for_test(pool: &sqlx::SqlitePool) {
    use sqlx::AssertSqlSafe;

    let schema = read_schema_for_test(SQLITE_SCHEMA_BACKEND).await;
    sqlx::raw_sql(AssertSqlSafe(schema))
        .execute(pool)
        .await
        .expect("the SQLite durable test Schema must provision an empty target");
}
