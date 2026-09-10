//! PostgreSQL authority for the clean-cut `insight.platform/v1` architecture.
//!
//! The crate owns the contract for one fresh baseline and a small shared repository. Linking or
//! running platform services never executes DDL. The baseline is installed only by the external
//! provisioning workflow, after which runtime processes use [`verify_schema`] read-only.

/// Administrative provisioning for the explicit non-production shared DML role.
pub fn development_runtime_role_grants_sql() -> &'static str {
    include_str!("../development-runtime-role-grants.sql")
}

/// Provisioning-only SQL for the independently deployed Security Authority role.
pub fn security_authority_role_grants_sql() -> &'static str {
    include_str!("../security-authority-grants.sql")
}

mod agent_feature_repository;
pub mod artifact_repository;
pub mod capability_execution_repository;
mod claim_admission;
pub mod context_dataset_repository;
pub mod context_dataset_worker_repository;
pub mod context_query_repository;
pub mod dependency_health;
pub(crate) mod execution_authorization;
mod execution_requirements;
pub mod history_repository;
mod history_retirement_repository;
pub mod invocation_repository;
mod mcp_oauth_cleanup_repository;
pub mod mcp_repository;
pub mod model_turn_repository;
pub mod opensandbox_repository;
pub mod operation_repository;
pub mod operational_metrics;
pub mod outbox_repository;
mod partition_scheduler;
pub mod principal_authentication;
mod product_reads;
pub mod product_repository;
mod recovery_isolation;
mod registry_validation_repository;
pub mod repository;
pub mod sandbox_repository;
mod schema_inventory;
mod transaction_retry;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use sqlx::{PgPool, Row};
use std::{collections::BTreeSet, error::Error, fmt};

pub const AUTHORITY_SCHEMA: &str = "insight_platform";
pub const SCHEMA_CONTRACT_VERSION: u32 = 15;
pub const POSTGRES_MAJOR_VERSION: i32 = 16;
pub const BASELINE_TABLE_COUNT: usize = 23;

const CHECKED_IN_SCHEMA_CONTRACT: &[u8] = include_bytes!("../schema-contract.json");
const EXPECTED_SCHEMA_INVENTORY: &[u8] = include_bytes!("../schema-inventory.json");

pub const CURRENT_SCHEMA_SQL: &str = include_str!("../schema.sql");

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
    "scheduler_tenant_state",
    "secret_bindings",
    "tasks",
    "tenant_principals",
    "tenants",
];

pub const EXPECTED_FUNCTIONS: &[&str] = &[
    "artifact_lock_scan_policy(text, text)",
    "history_delete_event(text, text, text)",
    "history_delete_prefix(text, text, bigint, bigint)",
    "history_delete_published_outbox(text, text, timestamp with time zone)",
    "history_delete_receipt(text, text, text)",
    "history_event_obligations(text, text, bigint)",
    "history_lock_event(text, text)",
    "history_lock_event_prefix(text, text, bigint, bigint, integer)",
    "history_lock_owner(text, text)",
    "history_lock_receipt(text, text)",
    "history_lock_run(text, text)",
    "history_lock_task_chain(text, text)",
    "history_owner_delivery(text, text[])",
    "history_retire_oauth_chain(text, text, bigint, text, text, text, text)",
    "history_scan_records(text, timestamp with time zone, text, text, text, text, integer)",
    "history_scan_runs(timestamp with time zone, text, text, text, text, integer)",
    "is_bounded_object(jsonb, integer)",
    "is_platform_id(text)",
    "is_sha256(text)",
    "is_trace_id(text)",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaVerification {
    pub contract_version: u32,
    pub schema_snapshot_digest: String,
    pub schema_inventory_digest: String,
    pub table_count: usize,
}

#[derive(Debug)]
pub enum AuthoritySchemaError {
    Database(sqlx::Error),
    SchemaAlreadyProvisioned,
    UnsupportedPostgresVersion {
        actual: i32,
        minimum: i32,
    },
    TableSetMismatch {
        missing: Vec<String>,
        unexpected: Vec<String>,
    },
    FunctionSetMismatch {
        missing: Vec<String>,
        unexpected: Vec<String>,
    },
    SchemaInventoryMismatch {
        expected: String,
        actual: String,
    },
    InvalidExpectedInventory,
    CheckedInContractMismatch,
}

impl fmt::Display for AuthoritySchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(failure) => write!(formatter, "PostgreSQL schema operation failed: {failure}"),
            Self::SchemaAlreadyProvisioned => formatter.write_str(
                "insight_platform schema already exists; baseline provisioning requires a fresh target",
            ),
            Self::UnsupportedPostgresVersion { actual, minimum } => write!(
                formatter,
                "PostgreSQL server version {actual} is unsupported; version {minimum} or newer is required"
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
            Self::SchemaInventoryMismatch { expected, actual } => write!(formatter,
                "physical PostgreSQL schema differs: expected {expected}, found {actual}"),
            Self::InvalidExpectedInventory => formatter.write_str("checked-in schema-inventory.json is invalid"),
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

/// Installs the one checked-in Platform baseline on a fresh PostgreSQL authority.
///
/// This is deliberately a provisioning-only operation. Runtime services must use
/// [`verify_schema`] and do not execute DDL. The existence check and all baseline
/// statements share one transaction, so a concurrent or repeated provision attempt
/// cannot leave a partial authority behind.
pub async fn provision_schema(pool: &PgPool) -> Result<SchemaVerification, AuthoritySchemaError> {
    ensure_postgres_version(pool).await?;
    validate_checked_in_schema_contract()?;

    let mut transaction = pool.begin().await?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1)",
    )
    .bind(AUTHORITY_SCHEMA)
    .fetch_one(&mut *transaction)
    .await?;
    if exists {
        return Err(AuthoritySchemaError::SchemaAlreadyProvisioned);
    }

    sqlx::raw_sql(CURRENT_SCHEMA_SQL)
        .execute(&mut *transaction)
        .await?;
    verify_inventory_value(&schema_inventory::capture(&mut transaction).await?)?;
    transaction.commit().await?;

    verify_schema(pool).await
}

pub async fn verify_schema(pool: &PgPool) -> Result<SchemaVerification, AuthoritySchemaError> {
    ensure_postgres_version(pool).await?;
    validate_checked_in_schema_contract()?;

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
    verify_inventory_value(&inventory)?;
    let inventory_bytes = serde_jcs::to_vec(&inventory)
        .expect("schema inventory contains only canonicalizable JSON values");
    Ok(SchemaVerification {
        contract_version: SCHEMA_CONTRACT_VERSION,
        schema_snapshot_digest: schema_snapshot_digest(),
        schema_inventory_digest: prefixed_sha256(&inventory_bytes),
        table_count: actual_tables.len(),
    })
}

fn verify_inventory_value(actual: &Value) -> Result<(), AuthoritySchemaError> {
    let expected: Value = serde_json::from_slice(EXPECTED_SCHEMA_INVENTORY)
        .map_err(|_| AuthoritySchemaError::InvalidExpectedInventory)?;
    if expected == *actual {
        return Ok(());
    }
    let digest = |value: &Value| {
        prefixed_sha256(
            &serde_jcs::to_vec(value)
                .expect("schema inventory contains only canonicalizable JSON values"),
        )
    };
    Err(AuthoritySchemaError::SchemaInventoryMismatch {
        expected: digest(&expected),
        actual: digest(actual),
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
    let mut transaction = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await?;
    let inventory = schema_inventory::capture(&mut transaction).await?;
    transaction.commit().await?;
    Ok(inventory)
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
        "schema_snapshot_digest": schema_snapshot_digest(),
        "physical_inventory": { "path": "schema-inventory.json", "digest": prefixed_sha256(EXPECTED_SCHEMA_INVENTORY) },
        "schema_snapshot": { "path": "schema.sql", "digest": schema_snapshot_digest() },
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

pub fn schema_snapshot_digest() -> String {
    prefixed_sha256(CURRENT_SCHEMA_SQL.as_bytes())
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
    fn current_schema_has_no_business_state_triggers_or_migration_ledger() {
        assert!(!CURRENT_SCHEMA_SQL.contains("CREATE TRIGGER"));
        assert!(!CURRENT_SCHEMA_SQL.contains("schema_migrations"));
    }

    #[test]
    fn checked_in_contract_matches_generated_contract() {
        validate_checked_in_schema_contract().unwrap();
    }
}

pub mod connections;
pub mod controller_admission;
pub mod execution_store;
pub mod orchestration_store;
pub mod plan_generation_store;
pub mod safety_store;
