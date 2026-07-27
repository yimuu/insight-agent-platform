mod support;

use chrono::{DateTime, Utc};
use insight_dsl::{compile_source, CompileOptions};
use insight_durable::{
    CreateRunCommand, DurableRepository, RuntimeIngressDurableRepository, VersionedPlan, WorkClass,
    WorkWakeupRepository,
};
use insight_engine::{
    DefinitionRevisionId, DeploymentRevisionId, RunId, TransitionKey, TransitionOutcome,
};
use insight_storage::{
    PostgresDurableRepository, DATABASE_SCHEMA_BACKEND_MISMATCH, DATABASE_SCHEMA_CONTRACT_MISMATCH,
    DATABASE_SCHEMA_NOT_INITIALIZED, DURABLE_SCHEMA_CONTRACT_ID,
};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use std::collections::BTreeSet;
use uuid::Uuid;

struct IsolatedPostgresSchema {
    admin: PgPool,
    control: PgPool,
    scoped_url: String,
    schema: String,
}

fn postgres_test_url() -> Option<String> {
    match std::env::var("TEST_POSTGRES_URL") {
        Ok(value) => Some(value),
        Err(error) if std::env::var_os("CI").is_some() => {
            panic!("CI must set TEST_POSTGRES_URL for Schema contract tests: {error}")
        }
        Err(_) => None,
    }
}

async fn isolated_schema(label: &str) -> Option<IsolatedPostgresSchema> {
    let database_url = postgres_test_url()?;
    let schema = format!("schema_{label}_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    Some(IsolatedPostgresSchema {
        admin,
        control,
        scoped_url,
        schema,
    })
}

async fn cleanup(schema: IsolatedPostgresSchema) {
    schema.control.close().await;
    sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA {} CASCADE",
        schema.schema
    )))
    .execute(&schema.admin)
    .await
    .unwrap();
    schema.admin.close().await;
}

#[tokio::test]
async fn postgres_runtime_ingress_delay_is_none_without_due_work() {
    let Some(schema) = isolated_schema("no_ingress_deadline").await else {
        return;
    };

    support::provision_postgres_schema(&schema.control).await;
    let repository = PostgresDurableRepository::connect(&schema.scoped_url)
        .await
        .unwrap();

    assert_eq!(
        repository.next_runtime_ingress_delay().await.unwrap(),
        None,
        "an empty ingress set must not become an immediate deadline"
    );

    drop(repository);
    cleanup(schema).await;
}

fn versioned_plan(label: &str) -> VersionedPlan {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs:
  question: string
output: string
workflow:
  steps:
    - return: fixed
"#;
    let plan = compile_source(
        source,
        CompileOptions::new(
            DefinitionRevisionId::new(format!("definition_revision_{label}")).unwrap(),
            format!("{label}.yaml"),
            source,
        ),
    )
    .unwrap();
    VersionedPlan::from_verified_plan(
        format!("definition_{label}"),
        format!("agent_{label}"),
        format!("Schema contract {label}"),
        DeploymentRevisionId::new(format!("deployment_revision_{label}")).unwrap(),
        "expression-3.0.0",
        json!({"author": "structured"}),
        &plan,
        json!({}),
        json!([]),
        json!([]),
    )
    .unwrap()
}

#[tokio::test]
async fn postgres_schema_provisions_once_and_repository_connect_is_read_only() {
    let Some(schema) = isolated_schema("install").await else {
        return;
    };

    support::provision_postgres_schema(&schema.control).await;
    let tables = sqlx::query_scalar::<_, String>(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema=current_schema() AND table_type='BASE TABLE'",
    )
    .fetch_all(&schema.control)
    .await
    .unwrap()
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(tables.len(), 52, "the complete table contract must install");
    for table in [
        "durable_schema_contract",
        "workflow_runs",
        "execution_events",
        "task_outbox",
        "public_event_outbox",
        "public_event_projection_decisions",
        "public_event_delivery_heads",
        "model_tool_call_batches",
        "model_tool_calls",
        "wait_late_audit_outbox",
        "workflow_retrieval_publications",
    ] {
        assert!(
            tables.contains(table),
            "PostgreSQL catalog is missing {table}"
        );
    }

    let indexes = sqlx::query_as::<_, (String, String)>(
        "SELECT indexname, tablename
         FROM pg_catalog.pg_indexes
         WHERE schemaname=current_schema()",
    )
    .fetch_all(&schema.control)
    .await
    .unwrap()
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(
        indexes.len(),
        167,
        "all explicit and constraint-backed indexes must be installed"
    );
    for (index, table) in [
        ("idx_runs_dispatch", "workflow_runs"),
        ("idx_runs_recovery", "workflow_runs"),
        ("idx_runs_scheduler_lease", "workflow_runs"),
        ("idx_execution_events_rebuild", "execution_events"),
        ("idx_task_outbox_dispatch", "task_outbox"),
        ("idx_task_outbox_acknowledge", "task_outbox"),
        ("idx_public_outbox_dispatch", "public_event_outbox"),
        ("idx_public_outbox_retention", "public_event_outbox"),
        (
            "idx_public_projection_order",
            "public_event_projection_decisions",
        ),
        ("idx_model_tool_calls_claim", "model_tool_calls"),
        ("idx_model_tool_calls_reclaim", "model_tool_calls"),
        ("uq_public_terminal_per_run", "public_event_outbox"),
        ("idx_wait_late_audit_pending", "wait_late_audit_outbox"),
        ("idx_wait_late_audit_reclaim", "wait_late_audit_outbox"),
        ("uq_wait_late_audit_claim_token", "wait_late_audit_outbox"),
    ] {
        assert!(
            indexes.contains(&(index.to_owned(), table.to_owned())),
            "PostgreSQL catalog is missing index {index} on {table}"
        );
    }
    let mut plan_transaction = schema.control.begin().await.unwrap();
    sqlx::query("SET LOCAL enable_seqscan=off")
        .execute(&mut *plan_transaction)
        .await
        .unwrap();
    for (query, expected_index) in [
        (
            "EXPLAIN
             SELECT run_id,task_id FROM task_outbox
             WHERE task_state='pending' AND available_at<=statement_timestamp()
             ORDER BY available_at,run_id,task_id LIMIT 8",
            "idx_task_outbox_dispatch",
        ),
        (
            "EXPLAIN
             SELECT run_id,task_id FROM task_outbox
             WHERE task_state='claimed' AND claim_expires_at<=statement_timestamp()
             ORDER BY claim_expires_at,run_id,task_id LIMIT 8",
            "idx_task_outbox_reclaim",
        ),
        (
            "EXPLAIN
             SELECT run_id,task_id FROM task_outbox
             WHERE task_state='published'
             ORDER BY available_at,run_id,task_id LIMIT 8",
            "idx_task_outbox_acknowledge",
        ),
        (
            "EXPLAIN
             SELECT run_id,tool_task_id FROM model_tool_calls
             WHERE call_status='pending' AND available_at<=statement_timestamp()
             ORDER BY available_at,run_id,tool_task_id LIMIT 8",
            "idx_model_tool_calls_claim",
        ),
        (
            "EXPLAIN
             SELECT run_id,activation_id,attempt_no,model_call_no,call_index
             FROM model_tool_calls
             WHERE call_status IN ('claimed','running')
               AND claim_expires_at<=statement_timestamp()
             ORDER BY claim_expires_at,run_id,activation_id,attempt_no,model_call_no,call_index
             LIMIT 8",
            "idx_model_tool_calls_reclaim",
        ),
        (
            "EXPLAIN
             SELECT run_id FROM workflow_runs
             WHERE lifecycle='terminating'
                OR (lifecycle IN ('created','active','waiting') AND admission_state='open')
             ORDER BY updated_at,run_id LIMIT 8",
            "idx_runs_recovery",
        ),
        (
            "EXPLAIN
             SELECT run_id,loser_kind,loser_id FROM wait_late_audit_outbox
             WHERE audit_state='pending' AND due_at<=statement_timestamp()
             ORDER BY due_at,run_id,loser_kind,loser_id LIMIT 8",
            "idx_wait_late_audit_pending",
        ),
        (
            "EXPLAIN
             SELECT run_id,loser_kind,loser_id FROM wait_late_audit_outbox
             WHERE audit_state='claimed' AND claim_expires_at<=statement_timestamp()
             ORDER BY claim_expires_at,run_id,loser_kind,loser_id LIMIT 8",
            "idx_wait_late_audit_reclaim",
        ),
        (
            "EXPLAIN
             SELECT run_id,public_event_id FROM public_event_outbox
             WHERE publish_state='published' AND NOT is_terminal
               AND retain_until IS NOT NULL
               AND retain_until<=statement_timestamp()
             ORDER BY retain_until,run_id,public_event_id LIMIT 8",
            "idx_public_outbox_retention",
        ),
    ] {
        let plan = sqlx::query_scalar::<_, String>(query)
            .fetch_all(&mut *plan_transaction)
            .await
            .unwrap();
        assert!(
            plan.iter()
                .any(|line| line.to_ascii_lowercase().contains(expected_index)),
            "PostgreSQL discovery branch must use {expected_index}: {plan:?}"
        );
        if expected_index == "idx_public_outbox_retention" {
            let normalized = plan.join(" ").to_ascii_lowercase();
            assert!(
                normalized.contains("index cond:")
                    && normalized.contains("retain_until <= statement_timestamp()"),
                "retention deadline must be an index range condition, not a post-index filter: {plan:?}"
            );
        }
    }
    plan_transaction.rollback().await.unwrap();

    let triggers = sqlx::query_as::<_, (String, String)>(
        "SELECT trigger_row.tgname::text, table_row.relname::text
         FROM pg_catalog.pg_trigger AS trigger_row
         JOIN pg_catalog.pg_class AS table_row
           ON table_row.oid=trigger_row.tgrelid
         JOIN pg_catalog.pg_namespace AS namespace_row
           ON namespace_row.oid=table_row.relnamespace
         WHERE namespace_row.nspname=current_schema()
           AND NOT trigger_row.tgisinternal",
    )
    .fetch_all(&schema.control)
    .await
    .unwrap()
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(triggers.len(), 30, "all user triggers must be installed");
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
            "workflow_retrieval_publication_immutable",
            "workflow_retrieval_publications",
        ),
        ("task_outbox_work_wakeup", "task_outbox"),
        ("model_tool_call_work_wakeup", "model_tool_calls"),
        ("timer_ingress_work_wakeup", "timers"),
        ("signal_ingress_work_wakeup", "signals_inbox"),
        ("run_recovery_work_wakeup", "workflow_runs"),
        (
            "public_event_delivery_work_wakeup",
            "public_event_delivery_heads",
        ),
        ("wait_late_audit_work_wakeup", "wait_late_audit_outbox"),
    ] {
        assert!(
            triggers.contains(&(trigger.to_owned(), table.to_owned())),
            "PostgreSQL catalog is missing trigger {trigger} on {table}"
        );
    }
    let recovery_trigger = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_triggerdef(trigger_row.oid)
         FROM pg_catalog.pg_trigger AS trigger_row
         WHERE trigger_row.tgname='run_recovery_work_wakeup'
           AND NOT trigger_row.tgisinternal",
    )
    .fetch_one(&schema.control)
    .await
    .unwrap()
    .to_ascii_lowercase();
    assert!(recovery_trigger.contains("projection_version"));
    assert!(
        !recovery_trigger.contains("updated_at"),
        "lease/control timestamp updates must not self-wake recovery: {recovery_trigger}"
    );

    let functions = sqlx::query_scalar::<_, String>(
        "SELECT procedure_row.proname::text
         FROM pg_catalog.pg_proc AS procedure_row
         JOIN pg_catalog.pg_namespace AS namespace_row
           ON namespace_row.oid=procedure_row.pronamespace
         WHERE namespace_row.nspname=current_schema()
           AND procedure_row.prokind='f'",
    )
    .fetch_all(&schema.control)
    .await
    .unwrap()
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(
        functions.len(),
        22,
        "all schema functions must be installed"
    );
    for function in [
        "bind_public_projection_decision",
        "establish_public_event_authority",
        "guard_public_event_receipt_provenance",
        "notify_durable_work",
        "reject_execution_event_projection_ledger_rewrite",
        "synchronize_public_event_delivery_head",
    ] {
        assert!(
            functions.contains(function),
            "PostgreSQL catalog is missing function {function}"
        );
    }

    let foreign_keys = sqlx::query_as::<_, (String, String, String, String)>(
        "SELECT source_table.relname::text,
                string_agg(source_column.attname::text, ',' ORDER BY key_columns.ordinality),
                target_table.relname::text,
                string_agg(target_column.attname::text, ',' ORDER BY key_columns.ordinality)
         FROM pg_catalog.pg_constraint AS constraint_row
         JOIN pg_catalog.pg_class AS source_table
           ON source_table.oid=constraint_row.conrelid
         JOIN pg_catalog.pg_namespace AS source_namespace
           ON source_namespace.oid=source_table.relnamespace
         JOIN pg_catalog.pg_class AS target_table
           ON target_table.oid=constraint_row.confrelid
         JOIN LATERAL unnest(constraint_row.conkey, constraint_row.confkey)
           WITH ORDINALITY AS key_columns(source_attnum, target_attnum, ordinality)
           ON TRUE
         JOIN pg_catalog.pg_attribute AS source_column
           ON source_column.attrelid=source_table.oid
          AND source_column.attnum=key_columns.source_attnum
         JOIN pg_catalog.pg_attribute AS target_column
           ON target_column.attrelid=target_table.oid
          AND target_column.attnum=key_columns.target_attnum
         WHERE constraint_row.contype='f'
           AND source_namespace.nspname=current_schema()
         GROUP BY constraint_row.oid, source_table.relname, target_table.relname",
    )
    .fetch_all(&schema.control)
    .await
    .unwrap()
    .into_iter()
    .collect::<BTreeSet<_>>();
    for edge in [
        (
            "workflow_runs",
            "definition_id,definition_revision_id,deployment_revision_id,plan_hash,binding_hash",
            "deployment_revisions",
            "definition_id,definition_revision_id,deployment_revision_id,plan_hash,binding_hash",
        ),
        (
            "execution_events",
            "run_id,activation_id,attempt_no",
            "node_attempts",
            "run_id,activation_id,attempt_no",
        ),
        (
            "public_event_delivery_heads",
            "run_id,public_event_id",
            "public_event_outbox",
            "run_id,public_event_id",
        ),
        (
            "model_tool_calls",
            "run_id,activation_id,attempt_no,model_call_no",
            "model_tool_call_batches",
            "run_id,activation_id,attempt_no,model_call_no",
        ),
        (
            "workflow_retrieval_publications",
            "run_id,task_id",
            "task_outbox",
            "run_id,task_id",
        ),
    ] {
        assert!(
            foreign_keys.contains(&(
                edge.0.to_owned(),
                edge.1.to_owned(),
                edge.2.to_owned(),
                edge.3.to_owned(),
            )),
            "PostgreSQL catalog is missing foreign-key edge {edge:?}"
        );
    }

    let keyed_constraints = sqlx::query_as::<_, (String, String, String)>(
        "SELECT table_row.relname::text,
                constraint_row.contype::text,
                string_agg(column_row.attname::text, ',' ORDER BY key_columns.ordinality)
         FROM pg_catalog.pg_constraint AS constraint_row
         JOIN pg_catalog.pg_class AS table_row
           ON table_row.oid=constraint_row.conrelid
         JOIN pg_catalog.pg_namespace AS namespace_row
           ON namespace_row.oid=table_row.relnamespace
         JOIN LATERAL unnest(constraint_row.conkey)
           WITH ORDINALITY AS key_columns(attnum, ordinality)
           ON TRUE
         JOIN pg_catalog.pg_attribute AS column_row
           ON column_row.attrelid=table_row.oid
          AND column_row.attnum=key_columns.attnum
         WHERE constraint_row.contype IN ('p', 'u')
           AND namespace_row.nspname=current_schema()
         GROUP BY constraint_row.oid, table_row.relname, constraint_row.contype",
    )
    .fetch_all(&schema.control)
    .await
    .unwrap()
    .into_iter()
    .collect::<BTreeSet<_>>();
    for constraint in [
        ("durable_schema_contract", "p", "singleton"),
        ("workflow_runs", "p", "run_id"),
        ("execution_events", "p", "run_id,seq"),
        (
            "node_attempts",
            "u",
            "run_id,activation_id,attempt_no,lease_epoch,fencing_token",
        ),
        (
            "public_event_outbox",
            "u",
            "run_id,causation_event_id,public_ordinal",
        ),
    ] {
        assert!(
            keyed_constraints.contains(&(
                constraint.0.to_owned(),
                constraint.1.to_owned(),
                constraint.2.to_owned(),
            )),
            "PostgreSQL catalog is missing keyed constraint {constraint:?}"
        );
    }

    let check_constraint_tables = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT table_row.relname::text
         FROM pg_catalog.pg_constraint AS constraint_row
         JOIN pg_catalog.pg_class AS table_row
           ON table_row.oid=constraint_row.conrelid
         JOIN pg_catalog.pg_namespace AS namespace_row
           ON namespace_row.oid=table_row.relnamespace
         WHERE constraint_row.contype='c'
           AND namespace_row.nspname=current_schema()",
    )
    .fetch_all(&schema.control)
    .await
    .unwrap()
    .into_iter()
    .collect::<BTreeSet<_>>();
    for table in [
        "workflow_runs",
        "execution_events",
        "public_event_outbox",
        "model_tool_calls",
        "durable_schema_contract",
    ] {
        assert!(
            check_constraint_tables.contains(table),
            "PostgreSQL catalog is missing check constraints on {table}"
        );
    }

    let before = sqlx::query_as::<_, (String, String, DateTime<Utc>)>(
        "SELECT contract_id,backend,installed_at
         FROM durable_schema_contract WHERE singleton=1",
    )
    .fetch_one(&schema.control)
    .await
    .unwrap();
    assert_eq!(before.0, DURABLE_SCHEMA_CONTRACT_ID);
    assert_eq!(before.1, "postgres");

    let repository = PostgresDurableRepository::connect(&schema.scoped_url)
        .await
        .unwrap();
    repository.validate_schema_contract().await.unwrap();
    drop(repository);
    let restarted = PostgresDurableRepository::connect(&schema.scoped_url)
        .await
        .unwrap();
    drop(restarted);

    let after = sqlx::query_as::<_, (String, String, DateTime<Utc>)>(
        "SELECT contract_id,backend,installed_at
         FROM durable_schema_contract WHERE singleton=1",
    )
    .fetch_one(&schema.control)
    .await
    .unwrap();
    assert_eq!(
        after, before,
        "repository startup must not rewrite metadata"
    );
    assert!(
        sqlx::raw_sql(support::POSTGRES_SCHEMA)
            .execute(&schema.control)
            .await
            .is_err(),
        "the full Schema is intentionally not an idempotent migration"
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String, DateTime<Utc>)>(
            "SELECT contract_id,backend,installed_at
             FROM durable_schema_contract WHERE singleton=1",
        )
        .fetch_one(&schema.control)
        .await
        .unwrap(),
        before,
        "a rejected second install must not disturb the installed contract"
    );

    cleanup(schema).await;
}

#[tokio::test]
async fn postgres_repository_rejects_missing_wrong_contract_and_wrong_backend() {
    let Some(schema) = isolated_schema("validation").await else {
        return;
    };

    let error = PostgresDurableRepository::connect(&schema.scoped_url)
        .await
        .err()
        .expect("an empty Schema must be rejected");
    assert_eq!(error.code(), DATABASE_SCHEMA_NOT_INITIALIZED);

    support::provision_postgres_schema(&schema.control).await;
    sqlx::query("UPDATE durable_schema_contract SET contract_id='wrong-contract'")
        .execute(&schema.control)
        .await
        .unwrap();
    let error = PostgresDurableRepository::connect(&schema.scoped_url)
        .await
        .err()
        .expect("a wrong contract ID must be rejected");
    assert_eq!(error.code(), DATABASE_SCHEMA_CONTRACT_MISMATCH);

    sqlx::query(
        "UPDATE durable_schema_contract
         SET contract_id=$1,backend='sqlite'",
    )
    .bind(DURABLE_SCHEMA_CONTRACT_ID)
    .execute(&schema.control)
    .await
    .unwrap();
    let error = PostgresDurableRepository::connect(&schema.scoped_url)
        .await
        .err()
        .expect("a wrong backend must be rejected");
    assert_eq!(error.code(), DATABASE_SCHEMA_BACKEND_MISMATCH);

    cleanup(schema).await;
}

#[tokio::test]
async fn postgres_work_notifications_are_commit_scoped_and_payload_free() {
    let Some(schema) = isolated_schema("work_notify").await else {
        return;
    };
    support::provision_postgres_schema(&schema.control).await;
    let repository = PostgresDurableRepository::connect(&schema.scoped_url)
        .await
        .unwrap();
    let mut stream = repository
        .open_work_notification_stream()
        .await
        .unwrap()
        .expect("PostgreSQL must expose durable work notifications");

    let plan = versioned_plan("work_notify");
    repository.install_versioned_plan(&plan).await.unwrap();
    let run_id = RunId::new("run_postgres_work_notify").unwrap();
    repository
        .create_run(
            TransitionKey::derive("postgres.schema.work-notify", &["create"]).unwrap(),
            CreateRunCommand::new(run_id.clone(), &plan, json!({"question": "safe"})).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), stream.recv())
            .await
            .expect("a committed eligible Run must wake the listener")
            .unwrap(),
        WorkClass::Maintenance
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), stream.recv())
            .await
            .is_err(),
        "one transaction must emit at most one all-class work hint"
    );

    sqlx::query("UPDATE workflow_runs SET updated_at=clock_timestamp() WHERE run_id=$1")
        .bind(run_id.as_str())
        .execute(&schema.control)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), stream.recv())
            .await
            .is_err(),
        "lease/control timestamp updates must not wake scheduler recovery"
    );

    let mut transaction = schema.control.begin().await.unwrap();
    sqlx::query(
        "UPDATE workflow_runs
         SET projection_version=projection_version+1
         WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.rollback().await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), stream.recv())
            .await
            .is_err(),
        "a rolled-back transition must not emit a work notification"
    );

    let listener_pid = sqlx::query_scalar::<_, i32>(
        "SELECT pid
         FROM pg_stat_activity
         WHERE datname=current_database()
           AND query LIKE 'LISTEN \"iap_work_%'
         ORDER BY backend_start DESC
         LIMIT 1",
    )
    .fetch_one(&schema.control)
    .await
    .unwrap();
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
            .bind(listener_pid)
            .fetch_one(&schema.control)
            .await
            .unwrap()
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), stream.recv())
            .await
            .expect("connection loss must be surfaced promptly")
            .is_err(),
        "the adapter must not hide a LISTEN reconnect from the coordinator"
    );

    let mut stream = repository
        .open_work_notification_stream()
        .await
        .unwrap()
        .expect("the coordinator must be able to reopen LISTEN");
    sqlx::query(
        "UPDATE workflow_runs
         SET projection_version=projection_version+1
         WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .execute(&schema.control)
    .await
    .unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), stream.recv())
            .await
            .expect("a committed projection change must wake recovery")
            .unwrap(),
        WorkClass::Maintenance
    );

    drop(repository);
    cleanup(schema).await;
}

#[tokio::test]
async fn runtime_marked_writes_defer_to_explicit_coalesced_notification() {
    let Some(schema) = isolated_schema("runtime_work_notify").await else {
        return;
    };
    support::provision_postgres_schema(&schema.control).await;
    let runtime_url = format!(
        "{}&application_name=insight-agent-platform-runtime",
        schema.scoped_url
    );
    let repository = PostgresDurableRepository::connect(&runtime_url)
        .await
        .unwrap();
    let mut stream = repository
        .open_work_notification_stream()
        .await
        .unwrap()
        .expect("PostgreSQL must expose durable work notifications");

    let plan = versioned_plan("runtime_work_notify");
    repository.install_versioned_plan(&plan).await.unwrap();
    let run_id = RunId::new("run_runtime_work_notify").unwrap();
    repository
        .create_run(
            TransitionKey::derive("postgres.schema.runtime-work-notify", &["create"]).unwrap(),
            CreateRunCommand::new(run_id, &plan, json!({"question": "safe"})).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), stream.recv())
            .await
            .is_err(),
        "runtime-marked authoritative commits must not take the NOTIFY ordering lock"
    );

    repository
        .publish_work_notification(WorkClass::Maintenance)
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), stream.recv())
            .await
            .expect("the process-coalesced hint must reach listeners")
            .unwrap(),
        WorkClass::Maintenance
    );

    drop(repository);
    cleanup(schema).await;
}

#[tokio::test]
async fn postgres_nonempty_target_is_rejected_without_publishing_contract_metadata() {
    let Some(schema) = isolated_schema("partial").await else {
        return;
    };
    sqlx::query("CREATE TABLE unrelated_preexisting_object (marker INTEGER)")
        .execute(&schema.control)
        .await
        .unwrap();

    let mut connection = schema.control.acquire().await.unwrap();
    assert!(sqlx::raw_sql(support::POSTGRES_SCHEMA)
        .execute(&mut *connection)
        .await
        .is_err());
    let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
    drop(connection);
    assert!(!sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('durable_schema_contract') IS NOT NULL",
    )
    .fetch_one(&schema.control)
    .await
    .unwrap());
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT to_regprocedure('bind_public_projection_decision()')::text",
        )
        .fetch_one(&schema.control)
        .await
        .unwrap(),
        None,
        "the empty-target preflight must run before managed functions"
    );
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('unrelated_preexisting_object') IS NOT NULL",
    )
    .fetch_one(&schema.control)
    .await
    .unwrap());
    let error = PostgresDurableRepository::connect(&schema.scoped_url)
        .await
        .err()
        .expect("a partial Schema must be rejected");
    assert_eq!(error.code(), DATABASE_SCHEMA_NOT_INITIALIZED);

    cleanup(schema).await;
}

#[tokio::test]
async fn postgres_mid_install_failure_rolls_back_every_managed_object() {
    let Some(schema) = isolated_schema("failed_install").await else {
        return;
    };
    let failing_schema = support::POSTGRES_SCHEMA.replacen(
        "CREATE TABLE durable_schema_contract (",
        "SELECT 1 / 0;\n\nCREATE TABLE durable_schema_contract (",
        1,
    );
    assert_ne!(failing_schema, support::POSTGRES_SCHEMA);

    let mut connection = schema.control.acquire().await.unwrap();
    assert!(sqlx::raw_sql(AssertSqlSafe(failing_schema))
        .execute(&mut *connection)
        .await
        .is_err());
    let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
    drop(connection);

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM pg_catalog.pg_class
             WHERE relnamespace=current_schema()::regnamespace",
        )
        .fetch_one(&schema.control)
        .await
        .unwrap(),
        0,
        "a failure before contract publication must roll back every relation"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM pg_catalog.pg_proc
             WHERE pronamespace=current_schema()::regnamespace",
        )
        .fetch_one(&schema.control)
        .await
        .unwrap(),
        0,
        "a failure before contract publication must roll back every function"
    );
    let error = PostgresDurableRepository::connect(&schema.scoped_url)
        .await
        .err()
        .expect("a rolled-back install must remain uninitialized");
    assert_eq!(error.code(), DATABASE_SCHEMA_NOT_INITIALIZED);

    cleanup(schema).await;
}

#[tokio::test]
async fn postgres_runtime_role_without_ddl_can_start_and_commit_a_representative_run() {
    let Some(schema) = isolated_schema("runtime_role").await else {
        return;
    };
    support::provision_postgres_schema(&schema.control).await;

    let role = format!("runtime_{}", Uuid::new_v4().simple());
    sqlx::query(AssertSqlSafe(format!("CREATE ROLE {role} NOLOGIN")))
        .execute(&schema.admin)
        .await
        .unwrap();
    for statement in [
        format!("REVOKE CREATE ON SCHEMA {} FROM PUBLIC", schema.schema),
        format!("GRANT USAGE ON SCHEMA {} TO {role}", schema.schema),
        format!(
            "GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA {} TO {role}",
            schema.schema
        ),
        format!(
            "GRANT USAGE,SELECT,UPDATE ON ALL SEQUENCES IN SCHEMA {} TO {role}",
            schema.schema
        ),
        format!(
            "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {} TO {role}",
            schema.schema
        ),
        format!(
            "REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON {}.durable_schema_contract FROM {role}",
            schema.schema
        ),
    ] {
        sqlx::query(AssertSqlSafe(statement))
            .execute(&schema.admin)
            .await
            .unwrap();
    }

    let runtime_pool = PgPoolOptions::new()
        .min_connections(1)
        .max_connections(1)
        .connect(&schema.scoped_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("SET ROLE {role}")))
        .execute(&runtime_pool)
        .await
        .unwrap();
    let repository = PostgresDurableRepository::from_pool(runtime_pool.clone())
        .await
        .unwrap();

    assert!(
        sqlx::query("CREATE TABLE runtime_role_must_not_create_tables (id INTEGER)")
            .execute(&runtime_pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE durable_schema_contract SET backend='sqlite'")
            .execute(&runtime_pool)
            .await
            .is_err()
    );

    let plan = versioned_plan("runtime_role");
    repository.install_versioned_plan(&plan).await.unwrap();
    let run_id = RunId::new("run_postgres_runtime_role").unwrap();
    assert!(matches!(
        repository
            .create_run(
                TransitionKey::derive("postgres.schema.runtime-role", &["create"]).unwrap(),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"question": "safe"})).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM workflow_runs WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&runtime_pool)
            .await
            .unwrap(),
        1
    );

    drop(repository);
    runtime_pool.close().await;
    sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA {} CASCADE",
        schema.schema
    )))
    .execute(&schema.admin)
    .await
    .unwrap();
    sqlx::query(AssertSqlSafe(format!("DROP ROLE {role}")))
        .execute(&schema.admin)
        .await
        .unwrap();
    schema.control.close().await;
    schema.admin.close().await;
}
