mod support;

use std::{collections::BTreeSet, fs::File};

use insight_storage::DURABLE_SCHEMA_CONTRACT_ID;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    AssertSqlSafe,
};

const REQUIRED_TABLES: &[&str] = &[
    "agent_publication_heads",
    "artifact_gc_claims",
    "artifact_gc_sweeps",
    "artifact_retention_releases",
    "artifact_store_authority",
    "artifacts",
    "control_tokens",
    "control_transition_results",
    "deployment_revisions",
    "durable_schema_contract",
    "execution_events",
    "fork_groups",
    "fork_legs",
    "graph_view_documents",
    "human_work_items",
    "join_arrivals",
    "model_call_usage",
    "model_tool_call_batches",
    "model_tool_calls",
    "node_activations",
    "node_attempts",
    "payloads",
    "projection_checkpoint_batches",
    "projection_checkpoints",
    "public_event_delivery_heads",
    "public_event_outbox",
    "public_event_projection_decisions",
    "public_event_receipts",
    "recovery_artifact_roots",
    "recovery_effect_roots",
    "recovery_revision_roots",
    "recovery_transition_results",
    "response_public_items",
    "response_snapshots",
    "run_migration_intents",
    "run_recovery_lineage",
    "run_reuse_candidates",
    "scheduler_checkpoints",
    "scheduler_occurrence_values",
    "scheduler_subflow_invocations",
    "scheduler_values",
    "scheduler_wait_registrations",
    "scope_instances",
    "signals_inbox",
    "task_outbox",
    "timers",
    "workflow_definition_public_metadata",
    "workflow_definition_revisions",
    "workflow_definitions",
    "workflow_retrieval_publications",
    "workflow_runs",
];

const SHARED_CRITICAL_TRIGGERS: &[&str] = &[
    "artifact_retention_release_delete_forbidden",
    "control_transition_result_delete_forbidden",
    "control_transition_result_rewrite_forbidden",
    "execution_event_projection_ledger_immutable",
    "execution_event_public_projection_decision_insert",
    "public_event_outbox_authority_insert",
    "public_event_outbox_delete_forbidden",
    "public_event_outbox_delivery_head_update",
    "public_event_outbox_update_contract",
    "public_event_receipt_delete_forbidden",
    "public_event_receipt_insert_provenance",
    "public_event_receipt_update_forbidden",
    "recovery_transition_result_delete_forbidden",
    "recovery_transition_result_rewrite_forbidden",
    "trg_deployment_revision_immutable",
];

fn normalize(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn table_names(sql: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let tokens = sql
        .split(|character: char| character.is_whitespace() || character == '(')
        .collect::<Vec<_>>();
    for window in tokens.windows(3) {
        if window[0].eq_ignore_ascii_case("create") && window[1].eq_ignore_ascii_case("table") {
            names.insert(
                window[2]
                    .trim_matches('"')
                    .trim_start_matches("public.")
                    .to_ascii_lowercase(),
            );
        }
    }
    names
}

#[test]
fn durable_schema_has_one_authoritative_file_per_backend_and_no_migration_history() {
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert!(workspace.join("database/durable/README.md").is_file());
    assert!(workspace
        .join("database/durable/postgres/schema.sql")
        .is_file());
    assert!(workspace
        .join("database/durable/sqlite/schema.sql")
        .is_file());
    assert!(
        !workspace.join("migrations/durable").exists(),
        "pre-1.0 migration history must not remain alongside the Schema"
    );
}

#[test]
fn backend_schemas_expose_the_same_complete_table_contract() {
    let expected = REQUIRED_TABLES
        .iter()
        .map(|table| (*table).to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(table_names(support::POSTGRES_SCHEMA), expected);
    assert_eq!(table_names(support::SQLITE_SCHEMA), expected);
}

#[test]
fn schemas_are_atomic_final_state_installers_with_contract_metadata_last() {
    for (backend, schema) in [
        ("postgres", support::POSTGRES_SCHEMA),
        ("sqlite", support::SQLITE_SCHEMA),
    ] {
        let normalized = normalize(schema);
        assert!(normalized.starts_with("-- durable repository schema"));
        assert!(normalized.contains("begin"));
        assert!(normalized.ends_with("commit;"));
        assert!(!normalized.contains("schema_migrations"));
        for repeatable_ddl in [
            "create table if not exists",
            "create index if not exists",
            "create unique index if not exists",
            "create trigger if not exists",
            "create function if not exists",
        ] {
            assert!(!normalized.contains(repeatable_ddl));
        }
        let preflight = normalized
            .find("durable_schema_target_must_be_empty")
            .expect("Schema must reject a non-empty target before installing objects");
        let first_managed_object = normalized
            .find("create function")
            .or_else(|| normalized.find("create table workflow_definitions"))
            .expect("Schema must create managed objects");
        assert!(preflight < first_managed_object);
        assert!(!normalized.contains("truncate "));
        assert!(!normalized.contains("migration-time"));
        assert!(!normalized.contains("legacy registration"));
        if backend == "sqlite" {
            assert!(!normalized.contains("alter table"));
            assert!(normalized.contains("pragma foreign_keys = on; begin immediate;"));
            assert!(!normalized.contains("create temp"));
        }
        assert!(!normalized.contains("drop table"));

        let contract_insert = normalized
            .rfind("insert into durable_schema_contract")
            .expect("contract row must be installed");
        let final_create = normalized
            .rfind("create ")
            .expect("Schema must create managed objects");
        assert!(
            contract_insert > final_create,
            "contract metadata must be the last logical install step"
        );
        assert!(normalized[contract_insert..].contains(DURABLE_SCHEMA_CONTRACT_ID));
        assert!(normalized[contract_insert..].contains(&format!("'{backend}'")));
    }
}

#[test]
fn durable_safety_authorities_remain_present_in_both_final_schemas() {
    let postgres = normalize(support::POSTGRES_SCHEMA);
    let sqlite = normalize(support::SQLITE_SCHEMA);
    for trigger in SHARED_CRITICAL_TRIGGERS {
        assert!(postgres.contains(trigger), "PostgreSQL missing {trigger}");
        assert!(sqlite.contains(trigger), "SQLite missing {trigger}");
    }

    for sql in [&postgres, &sqlite] {
        for authority in [
            "public_event_projection_decisions",
            "public_event_delivery_heads",
            "workflow_retrieval_publications",
            "artifact_store_authority",
            "model_tool_call_batches",
            "response_snapshots",
            "artifact_retention_releases",
            "recovery_transition_results",
        ] {
            assert!(
                sql.contains(authority),
                "missing durable authority {authority}"
            );
        }
        assert!(sql.contains("workflow retrieval publication is immutable"));
        assert!(sql.contains("artifact store authority is immutable"));
        assert!(sql.contains("public event receipt is immutable"));
        assert!(sql.contains("execution event authority"));
        assert!(sql.contains("foreign key"));
        assert!(sql.contains("check"));
    }
}

#[tokio::test]
async fn sqlite_schema_installs_on_a_new_file_and_rejects_a_second_install() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("schema.sqlite3");
    support::provision_sqlite_database(&database).await;

    let options = SqliteConnectOptions::new()
        .filename(&database)
        .create_if_missing(false)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let tables = sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_master
         WHERE type='table' AND name NOT LIKE 'sqlite_%'
         ORDER BY name",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .collect::<BTreeSet<_>>();
    let expected = REQUIRED_TABLES
        .iter()
        .map(|table| (*table).to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(tables, expected);

    let nullable_text_primary_keys = sqlx::query_as::<_, (String, String)>(
        r#"SELECT schema_object.name, column_info.name
           FROM sqlite_master AS schema_object
           JOIN pragma_table_info(schema_object.name) AS column_info
           WHERE schema_object.type='table'
             AND lower(column_info.type)='text'
             AND column_info.pk > 0
             AND column_info."notnull" = 0
           ORDER BY schema_object.name, column_info.pk"#,
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        nullable_text_primary_keys.is_empty(),
        "SQLite TEXT primary keys must explicitly preserve PostgreSQL NOT NULL semantics: \
         {nullable_text_primary_keys:?}"
    );

    let indexes = sqlx::query_as::<_, (String, String)>(
        "SELECT name, tbl_name
         FROM sqlite_master
         WHERE type='index' AND name NOT LIKE 'sqlite_%'",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(indexes.len(), 36, "all explicit indexes must be installed");
    for (index, table) in [
        ("idx_runs_dispatch", "workflow_runs"),
        ("idx_execution_events_rebuild", "execution_events"),
        ("idx_task_outbox_dispatch", "task_outbox"),
        ("idx_public_outbox_dispatch", "public_event_outbox"),
        (
            "idx_public_projection_order",
            "public_event_projection_decisions",
        ),
        ("idx_model_tool_calls_claim", "model_tool_calls"),
        ("uq_public_terminal_per_run", "public_event_outbox"),
        ("uq_workflow_runs_response_id", "workflow_runs"),
    ] {
        assert!(
            indexes.contains(&(index.to_owned(), table.to_owned())),
            "SQLite catalog is missing index {index} on {table}"
        );
    }

    let triggers = sqlx::query_as::<_, (String, String)>(
        "SELECT name, tbl_name FROM sqlite_master WHERE type='trigger'",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(triggers.len(), 37, "all user triggers must be installed");
    for (trigger, table) in [
        (
            "execution_event_projection_ledger_immutable",
            "execution_events",
        ),
        (
            "execution_event_public_projection_decision_insert",
            "execution_events",
        ),
        (
            "public_event_outbox_authority_insert",
            "public_event_outbox",
        ),
        (
            "public_event_outbox_delivery_head_update",
            "public_event_outbox",
        ),
        (
            "public_event_receipt_insert_provenance",
            "public_event_receipts",
        ),
        (
            "artifact_retention_release_delete_forbidden",
            "artifact_retention_releases",
        ),
        (
            "workflow_retrieval_publication_update_forbidden",
            "workflow_retrieval_publications",
        ),
    ] {
        assert!(
            triggers.contains(&(trigger.to_owned(), table.to_owned())),
            "SQLite catalog is missing trigger {trigger} on {table}"
        );
    }

    let foreign_keys = sqlx::query_as::<_, (String, String, String, String, String)>(
        r#"SELECT 'workflow_runs', "from", "table", "to", on_delete
           FROM pragma_foreign_key_list('workflow_runs')
           UNION ALL
           SELECT 'execution_events', "from", "table", "to", on_delete
           FROM pragma_foreign_key_list('execution_events')
           UNION ALL
           SELECT 'public_event_outbox', "from", "table", "to", on_delete
           FROM pragma_foreign_key_list('public_event_outbox')
           UNION ALL
           SELECT 'public_event_delivery_heads', "from", "table", "to", on_delete
           FROM pragma_foreign_key_list('public_event_delivery_heads')
           UNION ALL
           SELECT 'model_tool_calls', "from", "table", "to", on_delete
           FROM pragma_foreign_key_list('model_tool_calls')"#,
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .collect::<BTreeSet<_>>();
    for edge in [
        (
            "workflow_runs",
            "definition_id",
            "deployment_revisions",
            "definition_id",
            "RESTRICT",
        ),
        (
            "execution_events",
            "run_id",
            "workflow_runs",
            "run_id",
            "RESTRICT",
        ),
        (
            "public_event_outbox",
            "causation_event_id",
            "execution_events",
            "event_id",
            "RESTRICT",
        ),
        (
            "public_event_delivery_heads",
            "public_event_id",
            "public_event_outbox",
            "public_event_id",
            "RESTRICT",
        ),
        (
            "model_tool_calls",
            "model_call_no",
            "model_tool_call_batches",
            "model_call_no",
            "RESTRICT",
        ),
    ] {
        assert!(
            foreign_keys.contains(&(
                edge.0.to_owned(),
                edge.1.to_owned(),
                edge.2.to_owned(),
                edge.3.to_owned(),
                edge.4.to_owned(),
            )),
            "SQLite catalog is missing foreign-key edge {edge:?}"
        );
    }

    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT contract_id,backend
             FROM durable_schema_contract WHERE singleton=1",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        (DURABLE_SCHEMA_CONTRACT_ID.to_owned(), "sqlite".to_owned())
    );
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_optional(&pool)
        .await
        .unwrap()
        .is_none());
    assert!(
        sqlx::raw_sql(support::SQLITE_SCHEMA)
            .execute(&pool)
            .await
            .is_err(),
        "the complete Schema is not a repeatable migration"
    );
    pool.close().await;
}

#[tokio::test]
async fn sqlite_nonempty_target_is_rejected_without_publishing_contract_metadata() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("partial.sqlite3");
    File::create(&database).unwrap();
    let options = SqliteConnectOptions::new()
        .filename(&database)
        .create_if_missing(false)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE unrelated_preexisting_object (marker INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    assert!(sqlx::raw_sql(support::SQLITE_SCHEMA)
        .execute(&pool)
        .await
        .is_err());
    let _ = sqlx::query("ROLLBACK").execute(&pool).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type='table' AND name='durable_schema_contract'",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    pool.close().await;
}

#[tokio::test]
async fn sqlite_mid_install_failure_rolls_back_every_managed_object() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("failed-install.sqlite3");
    File::create(&database).unwrap();
    let options = SqliteConnectOptions::new()
        .filename(&database)
        .create_if_missing(false)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let failing_schema = support::SQLITE_SCHEMA.replacen(
        "CREATE TABLE durable_schema_contract (",
        "CREATE TABLE deliberate_schema_failure (marker INTEGER);\n\
         CREATE TABLE deliberate_schema_failure (marker INTEGER);\n\n\
         CREATE TABLE durable_schema_contract (",
        1,
    );
    assert_ne!(failing_schema, support::SQLITE_SCHEMA);
    assert!(sqlx::raw_sql(AssertSqlSafe(failing_schema))
        .execute(&pool)
        .await
        .is_err());
    sqlx::query("ROLLBACK").execute(&pool).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0,
        "a failure before contract publication must roll back the whole install"
    );
    pool.close().await;
}

#[test]
fn production_sources_do_not_embed_or_reference_the_schema_installer() {
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for root in [workspace.join("src"), workspace.join("crates")] {
        for entry in walkdir(&root) {
            if entry.extension().and_then(|extension| extension.to_str()) != Some("rs") {
                continue;
            }
            if entry
                .components()
                .any(|component| component.as_os_str() == "tests")
            {
                continue;
            }
            let source = std::fs::read_to_string(&entry).unwrap();
            assert!(
                !source.contains("database/durable/postgres/schema.sql")
                    && !source.contains("database/durable/sqlite/schema.sql"),
                "production source embeds provisioning DDL: {}",
                entry.display()
            );
        }
    }
}

fn walkdir(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                pending.push(entry.path());
            } else {
                files.push(entry.path());
            }
        }
    }
    files
}
