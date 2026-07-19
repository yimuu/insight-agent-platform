use std::{fs, path::Path};

use insight_agent_platform::engine::repository::migration_manifest::DURABLE_V3_MIGRATIONS;

const POSTGRES_V3: &str = DURABLE_V3_MIGRATIONS[0].postgres_sql;
const SQLITE_V3: &str = DURABLE_V3_MIGRATIONS[0].sqlite_sql;
const POSTGRES_PUBLIC_EVENT_IDENTITY: &str = DURABLE_V3_MIGRATIONS[3].postgres_sql;
const SQLITE_PUBLIC_EVENT_IDENTITY: &str = DURABLE_V3_MIGRATIONS[3].sqlite_sql;
const POSTGRES_ARTIFACT_RETENTION: &str = DURABLE_V3_MIGRATIONS[4].postgres_sql;
const SQLITE_ARTIFACT_RETENTION: &str = DURABLE_V3_MIGRATIONS[4].sqlite_sql;
const POSTGRES_GRAPH_VIEW_DOCUMENTS: &str = DURABLE_V3_MIGRATIONS[5].postgres_sql;
const SQLITE_GRAPH_VIEW_DOCUMENTS: &str = DURABLE_V3_MIGRATIONS[5].sqlite_sql;
const POSTGRES_RECOVERY_DEADLINE: &str = DURABLE_V3_MIGRATIONS[6].postgres_sql;
const SQLITE_RECOVERY_DEADLINE: &str = DURABLE_V3_MIGRATIONS[6].sqlite_sql;
const POSTGRES_EVENT_PROJECTION_LEDGER: &str = DURABLE_V3_MIGRATIONS[7].postgres_sql;
const SQLITE_EVENT_PROJECTION_LEDGER: &str = DURABLE_V3_MIGRATIONS[7].sqlite_sql;
const POSTGRES_HUMAN_WORK_ITEMS: &str = DURABLE_V3_MIGRATIONS[8].postgres_sql;
const SQLITE_HUMAN_WORK_ITEMS: &str = DURABLE_V3_MIGRATIONS[8].sqlite_sql;
const POSTGRES_PUBLICATION_HEADS: &str = DURABLE_V3_MIGRATIONS[9].postgres_sql;
const SQLITE_PUBLICATION_HEADS: &str = DURABLE_V3_MIGRATIONS[9].sqlite_sql;
const POSTGRES_EXECUTION_EVENT_AUTHORITY: &str = DURABLE_V3_MIGRATIONS[10].postgres_sql;
const SQLITE_EXECUTION_EVENT_AUTHORITY: &str = DURABLE_V3_MIGRATIONS[10].sqlite_sql;
const POSTGRES_PUBLIC_EVENT_AUTHORITY: &str = DURABLE_V3_MIGRATIONS[11].postgres_sql;
const SQLITE_PUBLIC_EVENT_AUTHORITY: &str = DURABLE_V3_MIGRATIONS[11].sqlite_sql;
const POSTGRES_PUBLIC_EVENT_RECEIPTS: &str = DURABLE_V3_MIGRATIONS[12].postgres_sql;
const SQLITE_PUBLIC_EVENT_RECEIPTS: &str = DURABLE_V3_MIGRATIONS[12].sqlite_sql;
const POSTGRES_PUBLIC_PROJECTION_DECISIONS: &str = DURABLE_V3_MIGRATIONS[13].postgres_sql;
const SQLITE_PUBLIC_PROJECTION_DECISIONS: &str = DURABLE_V3_MIGRATIONS[13].sqlite_sql;
const POSTGRES_SCHEDULER_CLAIM_AUTHORITY: &str = DURABLE_V3_MIGRATIONS[14].postgres_sql;
const SQLITE_SCHEDULER_CLAIM_AUTHORITY: &str = DURABLE_V3_MIGRATIONS[14].sqlite_sql;
const POSTGRES_PUBLIC_DELIVERY_HEADS: &str = DURABLE_V3_MIGRATIONS[15].postgres_sql;
const SQLITE_PUBLIC_DELIVERY_HEADS: &str = DURABLE_V3_MIGRATIONS[15].sqlite_sql;
const POSTGRES_ARTIFACT_STORE_AUTHORITY: &str = DURABLE_V3_MIGRATIONS[16].postgres_sql;
const SQLITE_ARTIFACT_STORE_AUTHORITY: &str = DURABLE_V3_MIGRATIONS[16].sqlite_sql;
const PUBLIC_OUTBOX_SOURCE: &str = include_str!("../src/engine/repository/public_outbox.rs");

const REQUIRED_TABLES: &[&str] = &[
    "workflow_definitions",
    "workflow_definition_revisions",
    "deployment_revisions",
    "workflow_runs",
    "scope_instances",
    "node_activations",
    "node_attempts",
    "control_tokens",
    "execution_events",
    "projection_checkpoint_batches",
    "projection_checkpoints",
    "scheduler_checkpoints",
    "scheduler_values",
    "scheduler_occurrence_values",
    "scheduler_wait_registrations",
    "scheduler_subflow_invocations",
    "task_outbox",
    "public_event_outbox",
    "signals_inbox",
    "timers",
    "fork_groups",
    "fork_legs",
    "join_arrivals",
    "run_reuse_candidates",
    "run_recovery_lineage",
    "run_migration_intents",
    "recovery_revision_roots",
    "recovery_artifact_roots",
    "recovery_effect_roots",
    "recovery_transition_results",
    "payloads",
    "artifacts",
];

fn normalize(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn migration_file_names(directory: &Path) -> Vec<String> {
    let mut names = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
        .map(|entry| {
            let entry = entry.unwrap();
            assert!(
                entry.file_type().unwrap().is_file(),
                "migration directory contains a non-file entry: {}",
                entry.path().display()
            );
            entry
                .file_name()
                .into_string()
                .expect("migration filenames must be UTF-8")
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[test]
fn durable_v3_manifest_is_contiguous_and_exactly_matches_both_backend_directories() {
    assert_eq!(DURABLE_V3_MIGRATIONS.len(), 17);
    for (index, migration) in DURABLE_V3_MIGRATIONS.iter().enumerate() {
        assert_eq!(
            migration.version,
            DURABLE_V3_MIGRATIONS[0].version + u64::try_from(index).unwrap(),
            "durable-v3 migration versions must be contiguous"
        );
        assert!(
            migration
                .name
                .starts_with(&format!("{}_", migration.version)),
            "migration filename must start with its manifest version: {}",
            migration.name
        );
        assert!(migration.name.ends_with(".sql"));
        assert!(!migration.postgres_sql.trim().is_empty());
        assert!(!migration.sqlite_sql.trim().is_empty());
    }

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations/durable_v3");
    let postgres_names = migration_file_names(&root.join("postgres"));
    let sqlite_names = migration_file_names(&root.join("sqlite"));
    let manifest_names = DURABLE_V3_MIGRATIONS
        .iter()
        .map(|migration| migration.name.to_owned())
        .collect::<Vec<_>>();

    assert_eq!(postgres_names, sqlite_names, "backend migrations diverged");
    assert_eq!(
        postgres_names, manifest_names,
        "on-disk migrations and the repository execution manifest diverged"
    );
}

#[test]
fn artifact_store_authority_is_singleton_closed_and_immutable_on_both_backends() {
    for migration in [
        POSTGRES_ARTIFACT_STORE_AUTHORITY,
        SQLITE_ARTIFACT_STORE_AUTHORITY,
    ] {
        let sql = normalize(migration);
        assert!(sql.contains("create table if not exists artifact_store_authority"));
        assert!(sql.contains("backend"));
        assert!(sql.contains("namespace"));
        assert!(sql.contains("store_id"));
        assert!(sql.contains("bound_at"));
        assert!(sql.contains("shared_filesystem"));
        assert!(sql.contains("artifact_store_"));
        assert!(sql.contains("before update"));
        assert!(sql.contains("delete"));
        assert!(sql.contains("artifact store authority is immutable"));
        assert!(!sql.contains("update artifact_store_authority"));
        assert!(!sql.contains("delete from artifact_store_authority"));
    }
}

#[test]
fn durable_v3_has_isolated_forward_only_migrations_for_both_backends() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(root.join("migrations/durable_v3/postgres").is_dir());
    assert!(root.join("migrations/durable_v3/sqlite").is_dir());

    for migration in [POSTGRES_V3, SQLITE_V3] {
        let sql = normalize(migration);
        assert!(!sql.contains("drop table"));
        assert!(!sql.contains("drop column"));
        assert!(!sql.contains("truncate "));
        assert!(!sql.contains("formal_v1"));
        assert!(!sql.contains("formal_v2"));
        assert!(!sql.contains("runtime_ownership"));

        for table in REQUIRED_TABLES {
            assert!(
                sql.contains(&format!("create table {table} (")),
                "missing durable-v3 table {table}"
            );
        }
    }
}

#[test]
fn public_event_notification_identity_has_a_forward_only_global_authority() {
    for migration in [POSTGRES_PUBLIC_EVENT_IDENTITY, SQLITE_PUBLIC_EVENT_IDENTITY] {
        let sql = normalize(migration);
        assert!(sql.contains(
            "create unique index if not exists uq_v3_public_outbox_public_event_id on public_event_outbox(public_event_id)"
        ));
        assert!(!sql.contains("drop "));
        assert!(!sql.contains("truncate "));
    }
}

#[test]
fn public_event_delivery_authority_is_closed_and_immutable_on_both_backends() {
    for migration in [POSTGRES_V3, SQLITE_V3] {
        let sql = normalize(migration);
        assert!(sql.contains("'run.completed','run.failed','run.cancelled','run.interrupted'"));
        assert!(sql.contains("published_claim_token is not null"));
        assert!(sql.contains("notified_at is not null"));
        assert!(sql.contains("retain_until is not null"));
    }
    for migration in [
        POSTGRES_PUBLIC_EVENT_AUTHORITY,
        SQLITE_PUBLIC_EVENT_AUTHORITY,
    ] {
        let sql = normalize(migration);
        assert!(sql.contains("public_event_outbox_update_contract"));
        assert!(sql.contains("safe_envelope"));
        assert!(sql.contains("public_ordinal"));
        assert!(sql.contains("publish_state = 'published'"));
        assert!(sql.contains("retain_until is not null"));
    }
}

#[test]
fn public_event_replay_receipts_survive_outbox_retention_on_both_backends() {
    for migration in [POSTGRES_PUBLIC_EVENT_RECEIPTS, SQLITE_PUBLIC_EVENT_RECEIPTS] {
        let sql = normalize(migration);
        assert!(sql.contains("public_event_receipts ("));
        assert!(sql.contains("unique (public_event_id)"));
        assert!(sql.contains("unique (run_id, causation_event_id, event_kind)"));
        assert!(sql.contains("public_event_outbox_receipt_insert"));
        assert!(sql.contains("public_event_receipt_update_forbidden"));
        assert!(sql.contains("public_event_receipt_delete_forbidden"));
        assert!(sql.contains("from public_event_outbox"));
        assert!(!sql.contains("delete from public_event_receipts"));
    }
}

#[test]
fn public_projection_decision_is_per_execution_one_way_and_run_by_both_backends() {
    for migration in [
        POSTGRES_PUBLIC_PROJECTION_DECISIONS,
        SQLITE_PUBLIC_PROJECTION_DECISIONS,
    ] {
        let sql = normalize(migration);
        assert!(sql.contains("public_event_projection_decisions ("));
        assert!(sql.contains("primary key (run_id, execution_event_id)"));
        assert!(sql.contains("decision in ('none', 'public')"));
        assert!(sql.contains("execution_seq"));
        assert!(sql.contains("execution_occurred_at"));
        assert!(sql.contains("execution_transition_key"));
        assert!(sql.contains("execution_event_public_projection_decision_insert"));
        assert!(sql.contains("public_event_outbox_projection_decision_bind"));
        assert!(sql.contains("decision='none'"));
        assert!(sql.contains("decision='public'"));
        assert!(sql.contains("public projection decision is immutable"));
        assert!(sql.contains("idx_v3_public_projection_order"));
        assert!(!sql.contains("delete from public_event_projection_decisions"));
    }
}

#[test]
fn scheduler_claim_mode_is_forward_only_closed_and_run_by_both_backends() {
    for migration in [
        POSTGRES_SCHEDULER_CLAIM_AUTHORITY,
        SQLITE_SCHEDULER_CLAIM_AUTHORITY,
    ] {
        let sql = normalize(migration);
        assert!(sql.contains("alter table task_outbox add column claim_mode text"));
        assert!(sql.contains("'execute'"));
        assert!(sql.contains("'finalize_lease_loss'"));
        assert!(sql.contains("'acknowledge'"));
        assert!(!sql.contains("drop "));
        assert!(!sql.contains("truncate "));
    }
}

#[test]
fn public_receipt_provenance_and_bounded_delivery_heads_run_on_both_backends() {
    for migration in [POSTGRES_PUBLIC_DELIVERY_HEADS, SQLITE_PUBLIC_DELIVERY_HEADS] {
        let sql = normalize(migration);
        assert!(sql.contains("uq_v3_public_receipt_causation"));
        assert!(sql.contains("run_id, causation_event_id"));
        assert!(sql.contains("public_event_receipt_insert_provenance"));
        assert!(sql.contains("public event receipt lacks committed provenance"));
        assert!(sql.contains("public_event_delivery_heads ("));
        assert!(sql.contains("primary key"));
        assert!(sql.contains("head_state in ('ready', 'drained')"));
        assert!(sql.contains("idx_v3_public_delivery_heads_due"));
        assert!(sql.contains("public_event_outbox_authority_insert"));
        assert!(sql.contains("public_event_outbox_delivery_head_update"));
        assert!(sql.contains("public event delivery head cannot be deleted"));
        assert!(!sql.contains("drop table public_event"));
        assert!(!sql.contains("drop column"));
        assert!(!sql.contains("truncate "));
        assert!(
            sql.contains("existing public event authority lacks provenance")
                || sql.contains("v3_public_016_validation")
        );
        assert!(sql.contains("next.execution_seq"));
    }
    assert!(PUBLIC_OUTBOX_SOURCE.contains("FROM public_event_delivery_heads candidate"));
    assert!(!PUBLIC_OUTBOX_SOURCE.contains("ROW_NUMBER() OVER"));
}

#[test]
fn graph_view_is_a_forward_only_presentation_authority_separate_from_plan_semantics() {
    for migration in [POSTGRES_GRAPH_VIEW_DOCUMENTS, SQLITE_GRAPH_VIEW_DOCUMENTS] {
        let sql = normalize(migration);
        assert!(sql.contains("create table if not exists graph_view_documents"));
        assert!(sql.contains("primary key (definition_id, definition_revision_id)"));
        assert!(sql.contains(
            "create index if not exists idx_v3_graph_views_document on graph_view_documents(graph_document_id)"
        ));
        assert!(!sql.contains("unique (graph_document_id)"));
        assert!(sql.contains("view_version"));
        assert!(sql.contains("view_document"));
        assert!(sql.contains(
            "foreign key (definition_id, definition_revision_id) references workflow_definition_revisions(definition_id, definition_revision_id) on delete restrict"
        ));
        assert!(!sql.contains("canonical_plan"));
        assert!(!sql.contains("semantic_hash"));
        assert!(!sql.contains("plan_hash"));
        assert!(!sql.contains("trace"));
        assert!(!sql.contains("drop "));
        assert!(!sql.contains("truncate "));
    }
}

#[test]
fn artifact_reference_retention_has_a_forward_only_terminal_authority() {
    for migration in [POSTGRES_ARTIFACT_RETENTION, SQLITE_ARTIFACT_RETENTION] {
        let sql = normalize(migration);
        assert!(sql.contains("create table if not exists artifact_retention_releases ("));
        assert!(sql.contains("run_id text primary key"));
        assert!(sql.contains("transition_key text not null unique"));
        assert!(sql.contains("retain_until"));
        assert!(sql.contains(
            "foreign key (run_id, event_id) references execution_events(run_id, event_id) on delete restrict"
        ));
        assert!(!sql.contains("drop "));
        assert!(!sql.contains("truncate "));
    }
}

#[test]
fn recovery_deadline_policy_is_forward_only_and_duration_based() {
    for migration in [POSTGRES_RECOVERY_DEADLINE, SQLITE_RECOVERY_DEADLINE] {
        let sql = normalize(migration);
        assert!(sql.contains("alter table run_migration_intents"));
        assert!(sql.contains("add column target_timeout_ms"));
        assert!(sql.contains("target_timeout_ms > 0"));
        assert!(!sql.contains("deadline_at"));
        assert!(!sql.contains("drop "));
        assert!(!sql.contains("truncate "));
    }
}

#[test]
fn immutable_revisions_and_runs_pin_both_execution_hashes() {
    for migration in [POSTGRES_V3, SQLITE_V3] {
        let sql = normalize(migration);
        assert!(sql.contains(
            "revision_status text not null check (revision_status in ('draft', 'published'))"
        ));
        assert!(sql.contains("published_at"));
        assert!(sql.contains("canonical_plan"));
        assert!(sql.contains("descriptor_contracts"));
        assert!(sql.contains("resolved_bindings"));
        assert!(sql.contains("worker_contracts"));
        assert!(sql.contains("published workflow definition revisions are immutable"));
        assert!(sql.contains("deployment revisions are immutable"));
        assert!(sql.contains("trg_v3_deployment_revision_immutable"));
        assert!(sql.contains("plan_hash"));
        assert!(sql.contains("binding_hash"));
        assert!(sql.contains("foreign key ( definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash ) references deployment_revisions"));
        assert!(sql.contains("on delete restrict"));
    }
}

#[test]
fn ledger_has_per_run_sequence_and_canonical_transition_idempotency() {
    for migration in [POSTGRES_V3, SQLITE_V3] {
        let sql = normalize(migration);
        assert!(sql.contains("next_event_seq"));
        assert!(sql.contains("primary key (run_id, seq)"));
        assert!(sql.contains("unique (run_id, event_id)"));
        assert!(sql.contains("unique (run_id, transition_key)"));
        assert!(sql.contains("intent_hash"));
        assert!(sql.contains("schema_version integer not null check (schema_version >= 1)"));
        assert!(sql.contains("projection_version_after"));
        assert!(sql.contains("causation_event_id"));
        assert!(sql.contains("safe_payload"));
        assert!(sql.contains("idx_v3_execution_events_rebuild"));
    }

    let postgres = normalize(POSTGRES_V3);
    assert!(postgres.contains("update workflow_runs set next_event_seq = next_event_seq + 1"));
    assert!(postgres.contains("returning"));
}

#[test]
fn projection_checkpoints_close_each_event_manifest_and_subject_version() {
    for migration in [POSTGRES_V3, SQLITE_V3] {
        let sql = normalize(migration);
        assert!(sql.contains(
            "checkpoint_schema_version integer not null check (checkpoint_schema_version >= 1)"
        ));
        assert!(sql.contains("subject_count"));
        assert!(sql.contains("manifest_hash"));
        assert!(sql.contains("subject_kind in ( 'run', 'scope', 'activation', 'attempt', 'timer', 'control', 'fork', 'join', 'scheduler', 'data_value' )"));
        for authority in ["'signal'", "'task_outbox'", "'human_work_item'"] {
            assert!(
                !sql[sql.find("create table projection_checkpoints (").unwrap()
                    ..sql
                        .find("create index idx_v3_projection_checkpoint_rebuild")
                        .unwrap()]
                    .contains(authority),
                "{authority} must remain outside the rebuildable projection kind constraint"
            );
        }
        assert!(sql.contains("subject_projection_version"));
        assert!(sql.contains("projection_hash"));
        assert!(sql.contains("canonical_projection"));
        assert!(sql.contains("primary key (run_id, event_id, subject_kind, subject_id)"));
        assert!(
            sql.contains("unique (run_id, subject_kind, subject_id, subject_projection_version)")
        );
        assert!(sql.contains(
            "references projection_checkpoint_batches(run_id, event_id) on delete restrict"
        ));
        assert!(sql.contains("idx_v3_projection_checkpoint_rebuild"));
    }
}

#[test]
fn execution_ledger_is_the_immutable_projection_rebuild_authority() {
    for migration in [
        POSTGRES_EVENT_PROJECTION_LEDGER,
        SQLITE_EVENT_PROJECTION_LEDGER,
    ] {
        let sql = normalize(migration);
        assert!(sql.contains("alter table execution_events"));
        assert!(sql.contains("add column projection_ledger_batch"));
        assert!(sql.contains("execution_event_projection_ledger_immutable"));
        assert!(!sql.contains("drop "));
        assert!(!sql.contains("truncate "));
    }
}

#[test]
fn execution_event_authority_has_a_closed_schema_and_one_fill_contract() {
    let postgres = normalize(POSTGRES_EXECUTION_EVENT_AUTHORITY);
    assert!(postgres.contains("check (schema_version = 2)"));
    assert!(postgres.contains("validate constraint execution_events_schema_version_supported"));
    assert!(postgres.contains("constraint execution_events_kind_supported"));
    assert!(postgres.contains("before insert or update or delete on execution_events"));
    assert!(postgres.contains("new.projection_ledger_batch is not null"));
    assert!(postgres.contains("projection ledger must be empty on insert"));
    assert!(postgres.contains("old.projection_ledger_batch is null"));

    let sqlite = normalize(SQLITE_EXECUTION_EVENT_AUTHORITY);
    assert!(sqlite.contains("unsupported execution event schema version"));
    assert!(sqlite.contains("invalid execution event insert authority"));
    assert!(sqlite.contains("new.projection_ledger_batch is null"));
    assert!(sqlite.contains("new.kind in ("));
    assert!(sqlite.contains("when coalesce(("));
    assert!(sqlite.contains("before update on execution_events"));
    assert!(sqlite.contains("old.projection_ledger_batch is null"));
    assert!(sqlite.contains("new.projection_ledger_batch is not null"));
    assert!(sqlite.contains("before delete on execution_events"));
}

#[test]
fn human_tasks_have_a_forward_only_durable_assignment_and_claim_authority() {
    for migration in [POSTGRES_HUMAN_WORK_ITEMS, SQLITE_HUMAN_WORK_ITEMS] {
        let sql = normalize(migration);
        assert!(sql.contains("create table if not exists human_work_items ("));
        assert!(sql.contains("work_item_id text primary key"));
        assert!(sql.contains("request_value"));
        assert!(sql.contains("response_type"));
        assert!(sql.contains("work_state in ('open','claimed','completed','cancelled','expired')"));
        assert!(sql.contains("claim_fence"));
        assert!(sql.contains("claim_expires_at"));
        assert!(sql.contains("completion_request_id"));
        assert!(sql.contains("completion_payload_hash"));
        assert!(sql.contains("unique (run_id, wait_id)"));
        assert!(sql.contains("unique (run_id, activation_id)"));
        assert!(sql.contains("idx_v3_human_work_items_queue"));
        assert!(sql.contains("close_human_work_items"));
        assert!(!sql.contains("drop table"));
        assert!(!sql.contains("drop column"));
        assert!(!sql.contains("truncate "));
    }
}

#[test]
fn publication_heads_are_explicit_and_public_metadata_is_immutable() {
    for base in [POSTGRES_V3, SQLITE_V3] {
        let sql = normalize(base);
        let definitions = sql
            .split("create table workflow_definition_revisions")
            .next()
            .unwrap();
        assert!(definitions.contains("definition_id text primary key"));
        assert!(definitions.contains("agent_id text not null"));
        assert!(!definitions.contains("display_name"));
    }
    for migration in [POSTGRES_PUBLICATION_HEADS, SQLITE_PUBLICATION_HEADS] {
        let sql = normalize(migration);
        assert!(sql.contains("workflow_definition_public_metadata"));
        assert!(sql.contains("agent_publication_heads"));
        assert!(sql.contains("agent_id text primary key"));
        assert!(sql.contains("definition_revision_id"));
        assert!(sql.contains("deployment_revision_id"));
        assert!(sql.contains("publication_origin"));
        assert!(sql.contains("publication_origin in ('built_in','graph')"));
        assert!(sql.contains("public_description"));
        assert!(sql.contains("published workflow definition public metadata is immutable"));
        assert!(sql.contains("x.definition_revision_id = new.definition_revision_id"));
        assert!(sql.contains("x.deployment_revision_id = new.deployment_revision_id"));
        assert!(!sql.contains("drop "));
        assert!(!sql.contains("truncate "));
    }
}

#[test]
fn every_execution_projection_is_run_scoped_and_cas_versioned() {
    for migration in [POSTGRES_V3, SQLITE_V3] {
        let sql = normalize(migration);
        for primary_key in [
            "primary key (run_id, scope_instance_id)",
            "primary key (run_id, activation_id)",
            "primary key (run_id, activation_id, attempt_no)",
            "primary key (run_id, token_id)",
            "primary key (run_id, task_id)",
            "primary key (run_id, public_event_id)",
            "primary key (run_id, signal_id)",
            "primary key (run_id, timer_id)",
            "primary key (run_id, fork_group_id)",
            "primary key (run_id, fork_group_id, leg_id)",
            "primary key (run_id, join_activation_id, fork_group_id, leg_id)",
            "primary key (run_id, candidate_id)",
            "primary key (run_id, payload_id)",
            "primary key (run_id, artifact_id)",
        ] {
            assert!(sql.contains(primary_key), "missing {primary_key}");
        }

        assert!(sql.matches("projection_version").count() >= 10);
        assert!(sql.contains("foreign key (run_id, scope_instance_id) references scope_instances(run_id, scope_instance_id)"));
        assert!(sql.contains("foreign key (run_id, activation_id, attempt_no) references node_attempts(run_id, activation_id, attempt_no)"));
        assert!(!sql.contains("references runs("));
    }
}

#[test]
fn scopes_forks_tokens_and_joins_are_first_class_projections() {
    for migration in [POSTGRES_V3, SQLITE_V3] {
        let sql = normalize(migration);
        assert!(sql.contains("uq_v3_root_scope_per_run"));
        assert!(sql.contains(
            "unique (run_id, parent_scope_instance_id, static_scope_id, stable_dynamic_key)"
        ));
        assert!(sql.contains("admitted_children"));
        assert!(sql.contains("settled_children"));
        assert!(sql.contains("'agent_loop_turn'"));
        assert!(sql.contains("'parallel_leg'"));
        assert!(sql.contains("provenance_frames"));
        assert!(sql.contains("source_activation_id"));
        assert!(sql.contains("emission_slot text not null"));
        assert!(sql.contains("emitted_by_transition_key text not null"));
        assert!(sql.contains("unique (run_id, source_activation_id, emission_slot)"));
        assert!(sql.contains("unique (run_id, emitted_by_transition_key, emission_slot)"));
        assert!(sql.contains("foreign key (run_id, emitted_by_transition_key) references execution_events(run_id, transition_key)"));
        assert!(sql.contains("current_port_id"));
        assert!(sql.contains("selected_branch_port_id"));
        assert!(sql.contains("unique (run_id, fork_group_id, declaration_index)"));
        assert!(sql.contains("primary key (run_id, join_activation_id, fork_group_id, leg_id)"));
        assert!(sql.contains("unique (run_id, arrival_transition_key)"));
    }
}

#[test]
fn attempts_and_tasks_carry_complete_monotonic_fences() {
    for migration in [POSTGRES_V3, SQLITE_V3] {
        let sql = normalize(migration);
        assert!(sql.contains("lease_epoch"));
        assert!(sql.contains("fencing_token"));
        assert!(sql.contains("lease_expires_at"));
        assert!(sql.contains("heartbeat_at"));
        assert!(sql.contains("check (lease_epoch >= attempt_no)"));
        assert!(
            sql.contains("unique (run_id, activation_id, attempt_no, lease_epoch, fencing_token)")
        );
        assert!(sql.contains("foreign key (run_id, activation_id, attempt_no, lease_epoch, fencing_token) references node_attempts"));
        assert!(sql.contains("idx_v3_attempt_lease_expiry"));
        assert!(sql.contains("idx_v3_task_outbox_dispatch"));
        assert!(sql.contains("task_state in ('pending', 'claimed', 'published', 'acked', 'dead')"));
    }
}

#[test]
fn effect_evidence_retry_and_timer_proofs_have_durable_inputs() {
    for migration in [POSTGRES_V3, SQLITE_V3] {
        let sql = normalize(migration);
        assert!(sql.contains("effect_id"));
        assert!(sql.contains("unique (run_id, effect_id)"));
        assert!(sql.contains("effect_idempotency in ('idempotent', 'non_idempotent')"));
        assert!(
            sql.contains("effect_evidence in ('not_started', 'started', 'committed', 'unknown')")
        );
        assert!(sql.contains("retry_budget_remaining"));
        assert!(sql.contains("retry_budget_snapshot"));
        assert!(sql.contains("pending_retry_timer_id"));
        assert!(sql.contains(
            "foreign key (run_id, pending_retry_timer_id) references timers(run_id, timer_id)"
        ));
        assert!(sql.contains("expected_attempt_no"));
        assert!(sql.contains("expected_lease_epoch"));
        assert!(sql.contains("expected_fencing_token"));
        assert!(sql.contains("fired_at >= deadline_at"));
        assert!(sql.contains("idx_v3_timers_due"));
    }
}

#[test]
fn terminal_rows_require_closed_admission_and_durable_terminal_outbox() {
    for migration in [POSTGRES_V3, SQLITE_V3] {
        let sql = normalize(migration);
        assert!(sql.contains("lifecycle = 'succeeded' and admission_state = 'closed'"));
        assert!(sql.contains("lifecycle in ('failed', 'cancelled', 'interrupted', 'timed_out') and admission_state = 'closed'"));
        assert!(sql.contains("terminal_event_id is not null"));
        assert!(sql.contains("terminal_public_event_id is not null"));
        assert!(sql.contains("termination_intent_transition_key"));
        assert!(sql.contains("uq_v3_public_terminal_per_run"));
        assert!(sql.contains("uq_v3_public_outbox_public_event_id"));
        assert!(sql.contains("publish_state in ('pending', 'claimed', 'published')"));
        assert!(sql.contains("published_by"));
        assert!(sql.contains("published_claim_token"));
        assert!(sql.contains("uq_v3_public_outbox_published_claim_token"));
        assert!(sql.contains(
            "publish_state = 'published' and published_at is not null and claimed_by is null and claim_token is null and claim_expires_at is null and published_by is not null and published_claim_token is not null"
        ));
        assert!(sql.contains("notified_at"));
        assert!(sql.contains("retain_until"));
    }
}

#[test]
fn payload_artifact_and_reuse_rows_preserve_hash_and_provenance_contracts() {
    for migration in [POSTGRES_V3, SQLITE_V3] {
        let sql = normalize(migration);
        assert!(sql.contains("unique (run_id, content_hash)"));
        assert!(sql.contains(
            "artifact_state in ('staged', 'verified', 'referenced', 'deleting', 'deleted')"
        ));
        assert!(sql.contains("source_control_provenance"));
        assert!(sql.contains("node_config_hash"));
        assert!(sql.contains("descriptor_hash"));
        assert!(sql.contains("output_schema_hash"));
        assert!(sql.contains("effect_policy_hash"));
        assert!(sql.contains("inherited_effect_id"));
        assert!(sql.contains("candidate_state in ('candidate', 'materialized', 'rejected')"));
        assert!(sql.contains("source_run_id <> run_id"));
    }
}

#[test]
fn recovery_lineage_migration_and_retention_roots_are_closed_and_anchored() {
    for migration in [POSTGRES_V3, SQLITE_V3] {
        let sql = normalize(migration);
        assert!(sql.contains("lineage_kind in ('redrive', 'fork', 'migrate', 'continue_as_new')"));
        assert!(sql.contains("source_checkpoint_hash"));
        assert!(sql.contains(
            "foreign key (source_run_id, source_checkpoint_hash) references scheduler_checkpoints(run_id, content_hash) on delete restrict"
        ));
        assert!(sql.contains("checkpoint_id text not null, content_hash text not null check"));
        assert!(sql.contains("target_generation = source_generation + 1"));
        assert!(sql.contains("intent_state in ('pending', 'completed')"));
        assert!(sql.contains("target_input_hash"));
        assert!(sql.contains("mapping_contracts"));
        assert!(sql.contains("mapping_hash"));
        assert!(sql.contains("reuse_candidates"));
        assert!(sql.contains("primary key (run_id, root_role)"));
        assert!(sql.contains("root_role in ('source', 'target')"));
        assert!(sql.contains(
            "foreign key (artifact_run_id, artifact_id) references artifacts(run_id, artifact_id)"
        ));
        assert!(sql.contains(
            "foreign key (source_run_id, source_activation_id, effect_id) references node_activations(run_id, activation_id, effect_id)"
        ));
        assert!(sql.contains("primary key (authority_run_id, transition_key)"));
        assert!(sql.contains(
            "foreign key (primary_event_run_id, primary_event_id) references execution_events(run_id, event_id)"
        ));
    }
}

#[test]
fn backend_specific_types_make_sqlite_limits_explicit() {
    let postgres = normalize(POSTGRES_V3);
    let sqlite = normalize(SQLITE_V3);

    assert!(postgres.contains("jsonb"));
    assert!(postgres.contains("timestamptz"));
    assert!(postgres.contains("bytea"));
    assert!(postgres.contains("deferrable initially deferred"));
    assert!(postgres.contains("scheduler_lease_epoch"));
    assert!(postgres.contains("scheduler_lease_owner"));
    assert!(postgres.contains("idx_v3_task_outbox_reclaim"));
    assert!(postgres.contains("idx_v3_public_outbox_reclaim"));

    assert!(sqlite.contains("pragma foreign_keys = on"));
    assert!(sqlite.contains("json_valid"));
    assert!(sqlite.contains("single-process test double"));
    assert!(!sqlite.contains("jsonb"));
    assert!(!sqlite.contains("timestamptz"));
    assert!(!sqlite.contains("pg_notify"));
    assert!(!sqlite.contains("skip locked"));
}
