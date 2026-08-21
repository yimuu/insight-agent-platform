//! PostgreSQL authority for the clean-cut `insight.platform/v1` architecture.
//!
//! The crate owns the contract for one fresh baseline and a small shared repository. Linking or
//! running platform services never executes DDL. The baseline is installed only by the external
//! provisioning workflow, after which runtime processes use [`verify_schema`] read-only.

pub mod artifact_repository;
pub mod capability_execution_repository;
pub mod context_dataset_repository;
pub mod context_query_repository;
pub mod invocation_repository;
mod mcp_oauth_cleanup_outbox;
pub mod mcp_repository;
pub mod model_turn_repository;
pub mod operation_repository;
pub mod principal_authentication;
pub mod repository;
pub mod sandbox_repository;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use sqlx::{PgPool, Row};
use std::{collections::BTreeSet, error::Error, fmt};

pub const AUTHORITY_SCHEMA: &str = "insight_platform";
pub const SCHEMA_CONTRACT_VERSION: u32 = 7;
pub const POSTGRES_MAJOR_VERSION: i32 = 16;
pub const BASELINE_TABLE_COUNT: usize = 23;

const CHECKED_IN_SCHEMA_CONTRACT: &[u8] = include_bytes!("../schema-contract.json");

#[derive(Debug, Clone, Copy)]
pub struct Migration {
    pub version: i64,
    pub name: &'static str,
    pub sql: &'static str,
}

impl Migration {
    pub fn checksum(self) -> String {
        prefixed_sha256(self.sql.as_bytes())
    }
}

pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "platform_baseline",
    sql: include_str!("../migrations/0001_platform_baseline.sql"),
}];

pub const EXPECTED_TABLES: &[&str] = &[
    "artifact_blobs",
    "artifact_links",
    "artifacts",
    "deployments",
    "events",
    "invocations",
    "jobs",
    "outbox_events",
    "principals",
    "quota_accounts",
    "quota_ledger",
    "receipts",
    "resource_versions",
    "resources",
    "run_nodes",
    "run_values",
    "runs",
    "scheduler_state",
    "schema_migrations",
    "secret_bindings",
    "tasks",
    "tenant_principals",
    "tenants",
];

pub const EXPECTED_FUNCTIONS: &[&str] = &[
    "is_bounded_object(jsonb, integer)",
    "is_platform_id(text)",
    "is_sha256(text)",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaVerification {
    pub contract_version: u32,
    pub migration_set_digest: String,
    pub schema_inventory_digest: String,
    pub table_count: usize,
}

#[derive(Debug)]
pub enum AuthoritySchemaError {
    Database(sqlx::Error),
    UnsupportedPostgresVersion {
        actual: i32,
        minimum: i32,
    },
    MigrationConflict {
        version: i64,
        expected_name: String,
        actual_name: String,
        expected_checksum: String,
        actual_checksum: String,
    },
    UnexpectedMigration {
        version: i64,
        name: String,
    },
    MigrationSetMismatch {
        expected: String,
        actual: String,
    },
    TableSetMismatch {
        missing: Vec<String>,
        unexpected: Vec<String>,
    },
    FunctionSetMismatch {
        missing: Vec<String>,
        unexpected: Vec<String>,
    },
    CheckedInContractMismatch,
}

impl fmt::Display for AuthoritySchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(failure) => write!(formatter, "PostgreSQL schema operation failed: {failure}"),
            Self::UnsupportedPostgresVersion { actual, minimum } => write!(
                formatter,
                "PostgreSQL server version {actual} is unsupported; version {minimum} or newer is required"
            ),
            Self::MigrationConflict {
                version,
                expected_name,
                actual_name,
                expected_checksum,
                actual_checksum,
            } => write!(
                formatter,
                "migration {version} conflicts: expected {expected_name}/{expected_checksum}, found {actual_name}/{actual_checksum}"
            ),
            Self::UnexpectedMigration { version, name } => {
                write!(formatter, "unexpected migration {version} ({name}) is installed")
            }
            Self::MigrationSetMismatch { expected, actual } => write!(
                formatter,
                "migration set digest differs: expected {expected}, found {actual}"
            ),
            Self::TableSetMismatch {
                missing,
                unexpected,
            } => write!(
                formatter,
                "schema table set differs (missing: {missing:?}, unexpected: {unexpected:?})"
            ),
            Self::FunctionSetMismatch {
                missing,
                unexpected,
            } => write!(
                formatter,
                "schema function set differs (missing: {missing:?}, unexpected: {unexpected:?})"
            ),
            Self::CheckedInContractMismatch => {
                formatter.write_str("checked-in schema-contract.json differs from generated authority")
            }
        }
    }
}

impl Error for AuthoritySchemaError {}

impl From<sqlx::Error> for AuthoritySchemaError {
    fn from(failure: sqlx::Error) -> Self {
        Self::Database(failure)
    }
}

pub async fn verify_schema(pool: &PgPool) -> Result<SchemaVerification, AuthoritySchemaError> {
    ensure_postgres_version(pool).await?;
    validate_checked_in_schema_contract()?;

    let installed_rows = sqlx::query(
        "SELECT version, name, checksum FROM insight_platform.schema_migrations ORDER BY version",
    )
    .fetch_all(pool)
    .await?;
    let mut installed = Vec::with_capacity(installed_rows.len());
    for row in installed_rows {
        let version: i64 = row.try_get("version")?;
        let name: String = row.try_get("name")?;
        let checksum: String = row.try_get("checksum")?;
        let Some(expected) = MIGRATIONS
            .iter()
            .find(|migration| migration.version == version)
        else {
            return Err(AuthoritySchemaError::UnexpectedMigration { version, name });
        };
        let expected_checksum = expected.checksum();
        if expected.name != name || expected_checksum != checksum {
            return Err(AuthoritySchemaError::MigrationConflict {
                version,
                expected_name: expected.name.to_owned(),
                actual_name: name,
                expected_checksum,
                actual_checksum: checksum,
            });
        }
        installed.push((version, expected.name, expected.checksum()));
    }

    let expected_migration_digest = migration_set_digest();
    let actual_migration_digest = migration_rows_digest(&installed);
    if installed.len() != MIGRATIONS.len() || actual_migration_digest != expected_migration_digest {
        return Err(AuthoritySchemaError::MigrationSetMismatch {
            expected: expected_migration_digest,
            actual: actual_migration_digest,
        });
    }

    let table_rows = sqlx::query(
        "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = $1 ORDER BY tablename",
    )
    .bind(AUTHORITY_SCHEMA)
    .fetch_all(pool)
    .await?;
    let actual_tables = table_rows
        .into_iter()
        .map(|row| row.try_get::<String, _>("tablename"))
        .collect::<Result<BTreeSet<_>, _>>()?;
    compare_set("table", EXPECTED_TABLES, &actual_tables)?;

    let function_rows = sqlx::query(
        r#"
        SELECT p.proname || '(' || pg_catalog.oidvectortypes(p.proargtypes) || ')' AS identity
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = $1
        ORDER BY identity
        "#,
    )
    .bind(AUTHORITY_SCHEMA)
    .fetch_all(pool)
    .await?;
    let actual_functions = function_rows
        .into_iter()
        .map(|row| row.try_get::<String, _>("identity"))
        .collect::<Result<BTreeSet<_>, _>>()?;
    compare_functions(EXPECTED_FUNCTIONS, &actual_functions)?;

    let inventory = capture_schema_inventory_value(pool).await?;
    let inventory_bytes = serde_jcs::to_vec(&inventory)
        .expect("schema inventory contains only canonicalizable JSON values");
    Ok(SchemaVerification {
        contract_version: SCHEMA_CONTRACT_VERSION,
        migration_set_digest: migration_set_digest(),
        schema_inventory_digest: prefixed_sha256(&inventory_bytes),
        table_count: actual_tables.len(),
    })
}

pub async fn capture_schema_inventory(pool: &PgPool) -> Result<Vec<u8>, AuthoritySchemaError> {
    let value = capture_schema_inventory_value(pool).await?;
    let mut bytes = serde_json::to_vec_pretty(&sorted_json(&value))
        .expect("schema inventory contains only JSON-serializable values");
    bytes.push(b'\n');
    Ok(bytes)
}

async fn capture_schema_inventory_value(pool: &PgPool) -> Result<Value, AuthoritySchemaError> {
    let rows = sqlx::query(
        r#"
        SELECT
            c.relname AS table_name,
            a.attnum AS ordinal,
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            a.attnotnull AS not_null,
            pg_get_expr(d.adbin, d.adrelid, false) AS default_expression
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
        WHERE n.nspname = $1 AND c.relkind IN ('r', 'p')
          AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY c.relname, a.attnum
        "#,
    )
    .bind(AUTHORITY_SCHEMA)
    .fetch_all(pool)
    .await?;
    let columns = rows
        .into_iter()
        .map(|row| {
            json!({
                "table": row.get::<String, _>("table_name"),
                "ordinal": row.get::<i16, _>("ordinal"),
                "column": row.get::<String, _>("column_name"),
                "type": row.get::<String, _>("data_type"),
                "not_null": row.get::<bool, _>("not_null"),
                "default": row.get::<Option<String>, _>("default_expression"),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema": AUTHORITY_SCHEMA,
        "postgres_major": POSTGRES_MAJOR_VERSION,
        "tables": EXPECTED_TABLES,
        "functions": EXPECTED_FUNCTIONS,
        "columns": columns,
    }))
}

pub fn generated_schema_contract() -> Vec<u8> {
    let contract = json!({
        "contract": "insight.platform/v1/postgres-baseline",
        "schema_contract_version": SCHEMA_CONTRACT_VERSION,
        "postgres_major": POSTGRES_MAJOR_VERSION,
        "schema": AUTHORITY_SCHEMA,
        "table_count": BASELINE_TABLE_COUNT,
        "tables": EXPECTED_TABLES,
        "functions": EXPECTED_FUNCTIONS,
        "migration_set_digest": migration_set_digest(),
        "migrations": MIGRATIONS.iter().map(|migration| json!({
            "version": migration.version,
            "name": migration.name,
            "checksum": migration.checksum(),
        })).collect::<Vec<_>>(),
        "architecture": {
            "adr": "docs/adr/0001-platform-v2-postgres-baseline.md",
            "current_state_is_not_reconstructed_from_events": true,
            "event_payload_is_not_duplicated_in_outbox": true,
            "business_state_triggers": false,
            "compatibility_schema": false,
        }
    });
    let mut bytes = serde_json::to_vec_pretty(&sorted_json(&contract))
        .expect("schema contract contains only JSON-serializable values");
    bytes.push(b'\n');
    bytes
}

pub fn validate_checked_in_schema_contract() -> Result<(), AuthoritySchemaError> {
    if CHECKED_IN_SCHEMA_CONTRACT == generated_schema_contract() {
        Ok(())
    } else {
        Err(AuthoritySchemaError::CheckedInContractMismatch)
    }
}

pub fn migration_set_digest() -> String {
    let rows = MIGRATIONS
        .iter()
        .map(|migration| (migration.version, migration.name, migration.checksum()))
        .collect::<Vec<_>>();
    migration_rows_digest(&rows)
}

fn migration_rows_digest(rows: &[(i64, &str, String)]) -> String {
    let mut hasher = Sha256::new();
    for (version, name, checksum) in rows {
        hasher.update(version.to_be_bytes());
        hasher.update([0]);
        hasher.update(name.as_bytes());
        hasher.update([0]);
        hasher.update(checksum.as_bytes());
        hasher.update([b'\n']);
    }
    format!("sha256:{}", lower_hex(&hasher.finalize()))
}

fn prefixed_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", lower_hex(&hasher.finalize()))
}

fn sorted_json(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(sorted_json).collect()),
        Value::Object(object) => {
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.clone(), sorted_json(value)))
                    .collect(),
            )
        }
        scalar => scalar.clone(),
    }
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

async fn ensure_postgres_version(pool: &PgPool) -> Result<(), AuthoritySchemaError> {
    let version: i32 = sqlx::query_scalar("SELECT current_setting('server_version_num')::integer")
        .fetch_one(pool)
        .await?;
    let major = version / 10_000;
    if major < POSTGRES_MAJOR_VERSION {
        return Err(AuthoritySchemaError::UnsupportedPostgresVersion {
            actual: major,
            minimum: POSTGRES_MAJOR_VERSION,
        });
    }
    Ok(())
}

fn compare_set(
    _kind: &str,
    expected: &[&str],
    actual: &BTreeSet<String>,
) -> Result<(), AuthoritySchemaError> {
    let expected = expected
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<BTreeSet<_>>();
    let missing = expected.difference(actual).cloned().collect::<Vec<_>>();
    let unexpected = actual.difference(&expected).cloned().collect::<Vec<_>>();
    if missing.is_empty() && unexpected.is_empty() {
        Ok(())
    } else {
        Err(AuthoritySchemaError::TableSetMismatch {
            missing,
            unexpected,
        })
    }
}

fn compare_functions(
    expected: &[&str],
    actual: &BTreeSet<String>,
) -> Result<(), AuthoritySchemaError> {
    let expected = expected
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<BTreeSet<_>>();
    let missing = expected.difference(actual).cloned().collect::<Vec<_>>();
    let unexpected = actual.difference(&expected).cloned().collect::<Vec<_>>();
    if missing.is_empty() && unexpected.is_empty() {
        Ok(())
    } else {
        Err(AuthoritySchemaError::FunctionSetMismatch {
            missing,
            unexpected,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn baseline_has_exactly_twenty_three_tables() {
        assert_eq!(EXPECTED_TABLES.len(), BASELINE_TABLE_COUNT);
        assert_eq!(
            EXPECTED_TABLES
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len(),
            23
        );
    }

    #[test]
    fn one_clean_cut_migration_is_registered() {
        assert_eq!(MIGRATIONS.len(), 1);
        assert_eq!(MIGRATIONS[0].version, 1);
        assert_eq!(MIGRATIONS[0].name, "platform_baseline");
        assert!(!MIGRATIONS[0].sql.contains("CREATE TRIGGER"));
        assert!(!MIGRATIONS[0].sql.contains("migration 35"));
    }

    #[test]
    fn checked_in_contract_matches_generated_contract() {
        validate_checked_in_schema_contract().unwrap();
    }
}
