//! Ordered durable-v3 migration manifest shared by both repository backends.
//!
//! Repository initialization iterates this exact manifest. The integration
//! layout gate also compares it with both on-disk migration directories, so a
//! migration cannot be added to one backend or to disk without entering the
//! execution path.

use std::fmt::Write;

use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableV3Migration {
    pub version: u64,
    pub name: &'static str,
    pub postgres_sql: &'static str,
    pub sqlite_sql: &'static str,
    pub sqlite_guard: SqliteMigrationGuard,
}

impl DurableV3Migration {
    /// The immutable PostgreSQL migration identity recorded by the production
    /// coordinator. The filename and SQL bytes are both bound by the ledger.
    pub fn postgres_checksum(&self) -> String {
        let digest = Sha256::digest(self.postgres_sql.as_bytes());
        let mut checksum = String::with_capacity("sha256:".len() + digest.len() * 2);
        checksum.push_str("sha256:");
        for byte in digest {
            write!(&mut checksum, "{byte:02x}").expect("writing to String cannot fail");
        }
        checksum
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteMigrationGuard {
    Always,
    WhenQueryMissing(&'static str),
}

macro_rules! migration {
    ($version:literal, $name:literal, $sqlite_guard:expr) => {
        DurableV3Migration {
            version: $version,
            name: concat!(stringify!($version), "_", $name, ".sql"),
            postgres_sql: include_str!(concat!(
                "../../../migrations/durable_v3/postgres/",
                stringify!($version),
                "_",
                $name,
                ".sql"
            )),
            sqlite_sql: include_str!(concat!(
                "../../../migrations/durable_v3/sqlite/",
                stringify!($version),
                "_",
                $name,
                ".sql"
            )),
            sqlite_guard: $sqlite_guard,
        }
    };
}

pub const DURABLE_V3_MIGRATIONS: [DurableV3Migration; 23] = [
    migration!(
        202607180001,
        "durable_v3",
        SqliteMigrationGuard::WhenQueryMissing(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='workflow_runs'"
        )
    ),
    migration!(
        202607180002,
        "run_public_metadata",
        SqliteMigrationGuard::WhenQueryMissing(
            "SELECT 1 FROM pragma_table_info('workflow_runs') WHERE name='request_id'"
        )
    ),
    migration!(
        202607180003,
        "run_deadline",
        SqliteMigrationGuard::WhenQueryMissing(
            "SELECT 1 FROM pragma_table_info('workflow_runs') WHERE name='deadline_at'"
        )
    ),
    migration!(
        202607180004,
        "public_event_identity",
        SqliteMigrationGuard::Always
    ),
    migration!(
        202607180005,
        "artifact_retention",
        SqliteMigrationGuard::Always
    ),
    migration!(
        202607180006,
        "graph_view_documents",
        SqliteMigrationGuard::Always
    ),
    migration!(
        202607180007,
        "recovery_deadline_policy",
        SqliteMigrationGuard::WhenQueryMissing(
            "SELECT 1 FROM pragma_table_info('run_migration_intents') WHERE name='target_timeout_ms'"
        )
    ),
    migration!(
        202607180008,
        "execution_event_projection_ledger",
        SqliteMigrationGuard::WhenQueryMissing(
            "SELECT 1 FROM pragma_table_info('execution_events') WHERE name='projection_ledger_batch'"
        )
    ),
    migration!(
        202607180009,
        "human_work_items",
        SqliteMigrationGuard::Always
    ),
    migration!(
        202607180010,
        "publication_heads",
        SqliteMigrationGuard::Always
    ),
    migration!(
        202607180011,
        "execution_event_authority",
        SqliteMigrationGuard::Always
    ),
    migration!(
        202607180012,
        "public_event_authority",
        SqliteMigrationGuard::Always
    ),
    migration!(
        202607180013,
        "public_event_receipts",
        SqliteMigrationGuard::Always
    ),
    migration!(
        202607180014,
        "public_event_projection_decisions",
        SqliteMigrationGuard::Always
    ),
    migration!(
        202607180015,
        "scheduler_claim_authority",
        SqliteMigrationGuard::WhenQueryMissing(
            "SELECT 1 FROM pragma_table_info('task_outbox') WHERE name='claim_mode'"
        )
    ),
    migration!(
        202607180016,
        "public_event_delivery_heads",
        SqliteMigrationGuard::Always
    ),
    migration!(
        202607180017,
        "artifact_store_authority",
        SqliteMigrationGuard::Always
    ),
    migration!(
        202607180018,
        "response_stream_authority",
        SqliteMigrationGuard::WhenQueryMissing(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='response_snapshots'"
        )
    ),
    migration!(
        202607180019,
        "llm_tool_call_checkpoints",
        SqliteMigrationGuard::WhenQueryMissing(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='model_tool_call_batches'"
        )
    ),
    migration!(
        202607180020,
        "model_tool_parent_deadline",
        SqliteMigrationGuard::WhenQueryMissing(
            "SELECT 1 FROM pragma_table_info('model_tool_call_batches') WHERE name='parent_operation_deadline'"
        )
    ),
    migration!(
        202607180021,
        "function_call_publication_sequence",
        SqliteMigrationGuard::WhenQueryMissing(
            "SELECT 1 FROM pragma_table_info('model_tool_calls') WHERE name='response_seal_index'"
        )
    ),
    migration!(
        202607180022,
        "atomic_artifact_retention",
        SqliteMigrationGuard::WhenQueryMissing(
            "SELECT 1 FROM pragma_table_info('workflow_runs') WHERE name='artifact_reference_retention_seconds'"
        )
    ),
    migration!(
        202607180023,
        "retrieval_publications",
        SqliteMigrationGuard::WhenQueryMissing(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='workflow_retrieval_publications'"
        )
    ),
];
