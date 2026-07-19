use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use serde_json::Value;
use sqlx::{AssertSqlSafe, PgPool, Postgres, Row, Transaction};

use crate::engine::{
    ExecutionEventContext, ExecutionEventPayload, PendingExecutionEvent, ProjectionMutationKind,
    RunId, TransitionKey,
};

use super::common::{
    canonical_value, decode_execution_event_schema_version, event_id, i64_from_u64, u64_from_i64,
};
use super::postgres::{allocate_event_seq, insert_event, PostgresDurableRepository};
use super::projection::{
    checkpoint_manifest_hash, decode_hex_subject_components, parse_attempt_subject_id,
    CheckpointRecord, ProjectionAudit, ProjectionDurableRepository, ProjectionLedgerBatch,
    ProjectionRebuildSnapshot, ProjectionRepairReceipt, ProjectionSubject, ProjectionSubjectKind,
    PROJECTION_CHECKPOINT_SCHEMA_VERSION,
};
use super::RepositoryError;

struct ProjectionTable {
    kind: ProjectionSubjectKind,
    subject_prefix: &'static str,
    table: &'static str,
    id_expression: &'static str,
    value_expression: &'static str,
}

const TABLES: &[ProjectionTable] = &[
    ProjectionTable {
        kind: ProjectionSubjectKind::Run,
        subject_prefix: "",
        table: "workflow_runs",
        id_expression: "p.run_id",
        value_expression: "to_jsonb(p) - ARRAY['run_id','next_event_seq','scheduler_lease_epoch','scheduler_lease_owner','scheduler_fencing_token','scheduler_lease_expires_at','scheduler_heartbeat_at']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Scope,
        subject_prefix: "",
        table: "scope_instances",
        id_expression: "p.scope_instance_id",
        value_expression: "to_jsonb(p) - ARRAY['run_id','scope_instance_id']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Activation,
        subject_prefix: "",
        table: "node_activations",
        id_expression: "p.activation_id",
        value_expression: "to_jsonb(p) - ARRAY['run_id','activation_id']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Attempt,
        subject_prefix: "",
        table: "node_attempts",
        id_expression: "p.activation_id || '#' || p.attempt_no::text",
        value_expression: "to_jsonb(p) - ARRAY['run_id','activation_id','attempt_no']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Timer,
        subject_prefix: "",
        table: "timers",
        id_expression: "p.timer_id",
        value_expression: "to_jsonb(p) - ARRAY['run_id','timer_id']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Scheduler,
        subject_prefix: "",
        table: "scheduler_checkpoints",
        id_expression: "p.checkpoint_id",
        value_expression: "to_jsonb(p) - ARRAY['run_id','checkpoint_id']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::DataValue,
        subject_prefix: "occurrence:",
        table: "scheduler_occurrence_values",
        id_expression: "'occurrence:' || encode(convert_to(p.occurrence_key::text,'UTF8'),'hex') || ':' || encode(convert_to(p.port_id,'UTF8'),'hex')",
        value_expression: "to_jsonb(p) - ARRAY['run_id','occurrence_key','port_id']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::DataValue,
        subject_prefix: "",
        table: "scheduler_values",
        id_expression: "p.port_id",
        value_expression: "to_jsonb(p) - ARRAY['run_id','port_id']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "wait:",
        table: "scheduler_wait_registrations",
        id_expression: "'wait:' || p.wait_id",
        value_expression: "to_jsonb(p) - ARRAY['run_id','wait_id']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "subflow:",
        table: "scheduler_subflow_invocations",
        id_expression: "'subflow:' || p.child_run_id",
        value_expression: "to_jsonb(p) - ARRAY['run_id','child_run_id']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "token:",
        table: "control_tokens",
        id_expression: "'token:' || p.token_id",
        value_expression: "to_jsonb(p) - ARRAY['run_id','token_id']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "reuse:",
        table: "run_reuse_candidates",
        id_expression: "'reuse:' || p.candidate_id",
        value_expression: "to_jsonb(p) - ARRAY['run_id','candidate_id']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "recovery:lineage",
        table: "run_recovery_lineage",
        id_expression: "'recovery:lineage'",
        value_expression: "to_jsonb(p) - 'run_id'",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "recovery:migration",
        table: "run_migration_intents",
        id_expression: "'recovery:migration'",
        value_expression: "to_jsonb(p) - 'run_id'",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "recovery:revision:",
        table: "recovery_revision_roots",
        id_expression: "'recovery:revision:' || p.root_role",
        value_expression: "to_jsonb(p) - ARRAY['run_id','root_role']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "recovery:artifact:",
        table: "recovery_artifact_roots",
        id_expression: "'recovery:artifact:' || encode(convert_to(p.artifact_run_id,'UTF8'),'hex') || ':' || encode(convert_to(p.artifact_id,'UTF8'),'hex')",
        value_expression: "to_jsonb(p) - ARRAY['run_id','artifact_run_id','artifact_id']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "recovery:effect:",
        table: "recovery_effect_roots",
        id_expression: "'recovery:effect:' || encode(convert_to(p.effect_run_id,'UTF8'),'hex') || ':' || encode(convert_to(p.source_activation_id,'UTF8'),'hex') || ':' || encode(convert_to(p.effect_id,'UTF8'),'hex')",
        value_expression: "to_jsonb(p) - ARRAY['run_id','effect_run_id','source_activation_id','effect_id']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Fork,
        subject_prefix: "group:",
        table: "fork_groups",
        id_expression: "'group:' || p.fork_group_id",
        value_expression: "to_jsonb(p) - ARRAY['run_id','fork_group_id']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Fork,
        subject_prefix: "leg:",
        table: "fork_legs",
        id_expression: "'leg:' || encode(convert_to(p.fork_group_id,'UTF8'),'hex') || ':' || encode(convert_to(p.leg_id,'UTF8'),'hex')",
        value_expression: "to_jsonb(p) - ARRAY['run_id','fork_group_id','leg_id']",
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Join,
        subject_prefix: "arrival:",
        table: "join_arrivals",
        id_expression: "'arrival:' || encode(convert_to(p.join_activation_id,'UTF8'),'hex') || ':' || encode(convert_to(p.fork_group_id,'UTF8'),'hex') || ':' || encode(convert_to(p.leg_id,'UTF8'),'hex')",
        value_expression: "to_jsonb(p) - ARRAY['run_id','join_activation_id','fork_group_id','leg_id']",
    },
];

async fn ensure_run_execution_event_schemas(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<(), RepositoryError> {
    // Projection rebuild consumes only the separately versioned, closed
    // `projection_ledger_batch`; it never interprets event kind/context/body.
    // The event envelope schema is still checked before its sequence or ledger
    // can participate in authority selection.
    let unknown = sqlx::query_scalar::<_, i32>(
        "SELECT schema_version FROM execution_events
         WHERE run_id=$1 AND schema_version<>$2 LIMIT 1",
    )
    .bind(run_id.as_str())
    .bind(
        i32::try_from(crate::engine::EXECUTION_EVENT_SCHEMA_VERSION)
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if let Some(version) = unknown {
        decode_execution_event_schema_version(i64::from(version))?;
    }
    Ok(())
}

fn table_for(subject: &ProjectionSubject) -> Result<&'static ProjectionTable, RepositoryError> {
    TABLES
        .iter()
        .find(|table| {
            table.kind == subject.kind()
                && (table.subject_prefix.is_empty()
                    || subject.subject_id().starts_with(table.subject_prefix))
        })
        .ok_or_else(RepositoryError::invalid_configuration)
}

/// Closed repair registry. Checkpoints carry projection values only; they may
/// never choose SQL identifiers or alter the identity columns omitted here.
fn repair_columns(table: &ProjectionTable) -> Result<&'static [&'static str], RepositoryError> {
    match table.table {
        "workflow_runs" => Ok(&[
            "definition_id",
            "definition_revision_id",
            "deployment_revision_id",
            "plan_hash",
            "binding_hash",
            "request_id",
            "attachment",
            "lifecycle",
            "admission_state",
            "termination_intent_reason",
            "termination_intent_transition_key",
            "termination_intent_at",
            "input_payload_id",
            "output_payload_id",
            "output_artifact_id",
            "output_value_hash",
            "error_code",
            "terminal_event_id",
            "terminal_public_event_id",
            "parent_run_id",
            "lineage_kind",
            "generation",
            "replacement_run_id",
            "projection_version",
            "created_at",
            "started_at",
            "deadline_at",
            "updated_at",
            "terminal_at",
        ]),
        "scope_instances" => Ok(&[
            "parent_scope_instance_id",
            "static_scope_id",
            "stable_dynamic_key",
            "scope_kind",
            "is_root",
            "lifecycle",
            "admission_state",
            "admitted_children",
            "settled_children",
            "projection_version",
            "created_at",
            "settled_at",
        ]),
        "node_activations" => Ok(&[
            "scope_instance_id",
            "node_id",
            "stable_activation_key",
            "execution_kind",
            "lifecycle",
            "effect_id",
            "effect_idempotency",
            "effect_evidence",
            "last_attempt_no",
            "last_lease_epoch",
            "current_attempt_no",
            "current_lease_epoch",
            "current_fencing_token",
            "retry_budget_remaining",
            "pending_retry_timer_id",
            "wait_registration_transition_key",
            "termination_intent_reason",
            "termination_intent_transition_key",
            "termination_intent_at",
            "output_payload_id",
            "output_artifact_id",
            "output_value_hash",
            "winning_attempt_no",
            "reused_from_run_id",
            "reused_from_activation_id",
            "projection_version",
            "created_at",
            "updated_at",
            "terminal_at",
        ]),
        "node_attempts" => Ok(&[
            "lease_epoch",
            "fencing_token",
            "effect_id",
            "lifecycle",
            "effect_evidence",
            "worker_id",
            "lease_expires_at",
            "heartbeat_at",
            "output_payload_id",
            "output_artifact_id",
            "output_value_hash",
            "failure_code",
            "completion_transition_key",
            "terminal_event_id",
            "projection_version",
            "created_at",
            "started_at",
            "terminal_at",
        ]),
        "timers" => Ok(&[
            "activation_id",
            "timer_kind",
            "timer_state",
            "deadline_at",
            "expected_attempt_no",
            "expected_lease_epoch",
            "expected_fencing_token",
            "retry_budget_snapshot",
            "created_by_transition_key",
            "fired_by_transition_key",
            "fired_event_id",
            "projection_version",
            "created_at",
            "fired_at",
        ]),
        "scheduler_checkpoints" => Ok(&[
            "content_hash",
            "checkpoint_kind",
            "transition_key",
            "intent_hash",
            "event_id",
            "checkpoint_schema_version",
            "scheduler_projection_version",
            "fact_payload",
            "projection_version",
            "created_at",
        ]),
        "scheduler_occurrence_values" | "scheduler_values" => Ok(&[
            "owner_activation_id",
            "runtime_value",
            "value_ref",
            "declared_type",
            "storage_kind",
            "payload_id",
            "artifact_id",
            "content_hash",
            "projection_version",
            "created_at",
            "updated_at",
        ]),
        "scheduler_wait_registrations" => Ok(&[
            "activation_id",
            "node_id",
            "occurrence_key",
            "signal_name",
            "signal_id",
            "timer_id",
            "due_at_ms",
            "payload_type",
            "winner_kind",
            "winner_signal_id",
            "winner_timer_id",
            "projection_version",
            "created_at",
            "resolved_at",
        ]),
        "scheduler_subflow_invocations" => Ok(&[
            "parent_activation_id",
            "node_id",
            "occurrence_key",
            "definition_revision_id",
            "deployment_revision_id",
            "plan_hash",
            "binding_hash",
            "interface_version",
            "output_contracts",
            "invocation_state",
            "projection_version",
            "created_at",
            "completed_at",
        ]),
        "control_tokens" => Ok(&[
            "current_scope_instance_id",
            "current_port_id",
            "source_activation_id",
            "source_port_id",
            "emission_slot",
            "emitted_by_transition_key",
            "provenance_frames",
            "branch_activation_id",
            "selected_branch_port_id",
            "fork_group_id",
            "fork_leg_id",
            "token_state",
            "consumed_by_activation_id",
            "consumed_by_transition_key",
            "consumed_at",
            "revoked_by_transition_key",
            "revoked_at",
            "projection_version",
            "created_at",
        ]),
        "run_reuse_candidates" => Ok(&[
            "target_scope_instance_id",
            "target_node_id",
            "stable_activation_key",
            "source_run_id",
            "source_activation_id",
            "source_control_provenance",
            "definition_revision_id",
            "deployment_revision_id",
            "plan_hash",
            "binding_hash",
            "node_config_hash",
            "descriptor_hash",
            "input_value_hash",
            "output_value_hash",
            "output_schema_hash",
            "effect_policy_hash",
            "inherited_effect_id",
            "data_dependencies_hash",
            "created_by_transition_key",
            "candidate_state",
            "materialized_activation_id",
            "decision_transition_key",
            "rejection_reason",
            "projection_version",
            "created_at",
            "decided_at",
        ]),
        "run_recovery_lineage" => Ok(&[
            "source_run_id",
            "lineage_kind",
            "source_generation",
            "target_generation",
            "source_definition_id",
            "source_definition_revision_id",
            "source_deployment_revision_id",
            "source_plan_hash",
            "source_binding_hash",
            "target_definition_id",
            "target_definition_revision_id",
            "target_deployment_revision_id",
            "target_plan_hash",
            "target_binding_hash",
            "source_checkpoint_hash",
            "created_by_transition_key",
            "projection_version",
            "created_at",
        ]),
        "run_migration_intents" => Ok(&[
            "target_run_id",
            "target_definition_id",
            "target_definition_revision_id",
            "target_deployment_revision_id",
            "target_plan_hash",
            "target_binding_hash",
            "target_input",
            "target_input_hash",
            "mapping_contracts",
            "mapping_hash",
            "reuse_candidates",
            "intent_transition_key",
            "intent_event_id",
            "intent_state",
            "final_transition_key",
            "projection_version",
            "created_at",
            "completed_at",
        ]),
        "recovery_revision_roots" => Ok(&[
            "source_run_id",
            "definition_id",
            "definition_revision_id",
            "deployment_revision_id",
            "plan_hash",
            "binding_hash",
            "created_by_transition_key",
            "projection_version",
            "created_at",
        ]),
        "recovery_artifact_roots" => Ok(&[
            "source_run_id",
            "source_activation_id",
            "created_by_transition_key",
            "projection_version",
            "created_at",
        ]),
        "recovery_effect_roots" => Ok(&[
            "source_run_id",
            "created_by_transition_key",
            "projection_version",
            "created_at",
        ]),
        "fork_groups" => Ok(&[
            "fork_activation_id",
            "parent_scope_instance_id",
            "join_activation_id",
            "join_mode",
            "expected_legs",
            "group_state",
            "admitted_legs",
            "settled_legs",
            "failure_leg_id",
            "failure_settlement_class",
            "projection_version",
            "created_at",
            "settled_at",
        ]),
        "fork_legs" => Ok(&[
            "declaration_index",
            "scope_instance_id",
            "child_activation_id",
            "token_id",
            "is_required",
            "leg_state",
            "settlement_class",
            "projection_version",
            "created_at",
            "settled_at",
        ]),
        "join_arrivals" => Ok(&[
            "token_id",
            "arrival_transition_key",
            "arrival_event_id",
            "settlement_class",
            "value_payload_id",
            "value_artifact_id",
            "value_hash",
            "projection_version",
            "arrived_at",
        ]),
        _ => Err(RepositoryError::invalid_configuration()),
    }
}

fn identity_columns(table: &ProjectionTable) -> Result<&'static [&'static str], RepositoryError> {
    match table.table {
        "workflow_runs" => Ok(&["run_id"]),
        "scope_instances" => Ok(&["run_id", "scope_instance_id"]),
        "node_activations" => Ok(&["run_id", "activation_id"]),
        "node_attempts" => Ok(&["run_id", "activation_id", "attempt_no"]),
        "timers" => Ok(&["run_id", "timer_id"]),
        "scheduler_checkpoints" => Ok(&["run_id", "checkpoint_id"]),
        "scheduler_occurrence_values" => Ok(&["run_id", "occurrence_key", "port_id"]),
        "scheduler_values" => Ok(&["run_id", "port_id"]),
        "scheduler_wait_registrations" => Ok(&["run_id", "wait_id"]),
        "scheduler_subflow_invocations" => Ok(&["run_id", "child_run_id"]),
        "control_tokens" => Ok(&["run_id", "token_id"]),
        "run_reuse_candidates" => Ok(&["run_id", "candidate_id"]),
        "run_recovery_lineage" | "run_migration_intents" => Ok(&["run_id"]),
        "recovery_revision_roots" => Ok(&["run_id", "root_role"]),
        "recovery_artifact_roots" => Ok(&["run_id", "artifact_run_id", "artifact_id"]),
        "recovery_effect_roots" => Ok(&[
            "run_id",
            "effect_run_id",
            "source_activation_id",
            "effect_id",
        ]),
        "fork_groups" => Ok(&["run_id", "fork_group_id"]),
        "fork_legs" => Ok(&["run_id", "fork_group_id", "leg_id"]),
        "join_arrivals" => Ok(&["run_id", "join_activation_id", "fork_group_id", "leg_id"]),
        _ => Err(RepositoryError::invalid_configuration()),
    }
}

fn repair_identity(
    table: &ProjectionTable,
    run_id: &RunId,
    subject: &ProjectionSubject,
) -> Result<Value, RepositoryError> {
    let mut identity = serde_json::Map::new();
    identity.insert(
        "run_id".to_owned(),
        Value::String(run_id.as_str().to_owned()),
    );
    match table.table {
        "workflow_runs" => {
            if subject.subject_id() != run_id.as_str() {
                return Err(RepositoryError::invalid_data());
            }
        }
        "scope_instances" => put_identity(&mut identity, "scope_instance_id", subject.subject_id()),
        "node_activations" => put_identity(&mut identity, "activation_id", subject.subject_id()),
        "node_attempts" => {
            let (activation_id, attempt_no) = parse_attempt_subject_id(subject.subject_id())?;
            put_identity(&mut identity, "activation_id", activation_id);
            identity.insert("attempt_no".to_owned(), Value::from(attempt_no));
        }
        "timers" => put_identity(&mut identity, "timer_id", subject.subject_id()),
        "scheduler_checkpoints" => {
            put_identity(&mut identity, "checkpoint_id", subject.subject_id())
        }
        "scheduler_occurrence_values" => {
            let parts = decode_hex_subject_components(subject.subject_id(), "occurrence:", 2)?;
            let occurrence = serde_json::from_str::<Value>(&parts[0])
                .map_err(|_| RepositoryError::invalid_data())?;
            identity.insert("occurrence_key".to_owned(), occurrence);
            put_identity(&mut identity, "port_id", &parts[1]);
        }
        "scheduler_values" => put_identity(&mut identity, "port_id", subject.subject_id()),
        "scheduler_wait_registrations" => put_identity(
            &mut identity,
            "wait_id",
            &required_subject_suffix(subject.subject_id(), "wait:")?,
        ),
        "scheduler_subflow_invocations" => put_identity(
            &mut identity,
            "child_run_id",
            &required_subject_suffix(subject.subject_id(), "subflow:")?,
        ),
        "control_tokens" => put_identity(
            &mut identity,
            "token_id",
            &required_subject_suffix(subject.subject_id(), "token:")?,
        ),
        "run_reuse_candidates" => put_identity(
            &mut identity,
            "candidate_id",
            &required_subject_suffix(subject.subject_id(), "reuse:")?,
        ),
        "run_recovery_lineage" if subject.subject_id() == "recovery:lineage" => {}
        "run_migration_intents" if subject.subject_id() == "recovery:migration" => {}
        "recovery_revision_roots" => {
            let role = required_subject_suffix(subject.subject_id(), "recovery:revision:")?;
            if role != "source" && role != "target" {
                return Err(RepositoryError::invalid_data());
            }
            put_identity(&mut identity, "root_role", &role);
        }
        "recovery_artifact_roots" => {
            let parts =
                decode_hex_subject_components(subject.subject_id(), "recovery:artifact:", 2)?;
            put_identity(&mut identity, "artifact_run_id", &parts[0]);
            put_identity(&mut identity, "artifact_id", &parts[1]);
        }
        "recovery_effect_roots" => {
            let parts = decode_hex_subject_components(subject.subject_id(), "recovery:effect:", 3)?;
            put_identity(&mut identity, "effect_run_id", &parts[0]);
            put_identity(&mut identity, "source_activation_id", &parts[1]);
            put_identity(&mut identity, "effect_id", &parts[2]);
        }
        "fork_groups" => put_identity(
            &mut identity,
            "fork_group_id",
            &required_subject_suffix(subject.subject_id(), "group:")?,
        ),
        "fork_legs" => {
            let parts = decode_hex_subject_components(subject.subject_id(), "leg:", 2)?;
            put_identity(&mut identity, "fork_group_id", &parts[0]);
            put_identity(&mut identity, "leg_id", &parts[1]);
        }
        "join_arrivals" => {
            let parts = decode_hex_subject_components(subject.subject_id(), "arrival:", 3)?;
            put_identity(&mut identity, "join_activation_id", &parts[0]);
            put_identity(&mut identity, "fork_group_id", &parts[1]);
            put_identity(&mut identity, "leg_id", &parts[2]);
        }
        _ => return Err(RepositoryError::invalid_data()),
    }
    Ok(Value::Object(identity))
}

fn put_identity(identity: &mut serde_json::Map<String, Value>, column: &str, value: &str) {
    identity.insert(column.to_owned(), Value::String(value.to_owned()));
}

fn required_subject_suffix(subject_id: &str, prefix: &str) -> Result<String, RepositoryError> {
    subject_id
        .strip_prefix(prefix)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(RepositoryError::invalid_data)
}

async fn changed_records(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<Vec<CheckpointRecord>, RepositoryError> {
    let mut records = Vec::new();
    for table in TABLES {
        let query = format!(
            "SELECT {id} AS subject_id,p.projection_version,{value} AS canonical_projection
             FROM {table_name} p
             WHERE p.run_id=$1
               AND NOT EXISTS (
                   SELECT 1 FROM projection_checkpoints c
                   WHERE c.run_id=p.run_id AND c.subject_kind=$2
                     AND c.subject_id={id}
                     AND c.subject_projection_version=p.projection_version
               )
             ORDER BY subject_id",
            id = table.id_expression,
            value = table.value_expression,
            table_name = table.table,
        );
        // SQL fragments come only from the closed static TABLES list.
        let rows = sqlx::query(AssertSqlSafe(query))
            .bind(run_id.as_str())
            .bind(table.kind.as_str())
            .fetch_all(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
        for row in rows {
            let subject = ProjectionSubject::new(
                table.kind,
                row.try_get::<String, _>("subject_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let projection_version = u64_from_i64(
                row.try_get("projection_version")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            records.push(CheckpointRecord::from_value(
                subject,
                projection_version,
                row.try_get("canonical_projection")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?);
        }
    }
    records.sort_by(|left, right| {
        (left.subject.kind().as_str(), left.subject.subject_id())
            .cmp(&(right.subject.kind().as_str(), right.subject.subject_id()))
    });
    Ok(records)
}

pub(crate) async fn finalize_projection_checkpoints(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    event_id: &str,
) -> Result<(), RepositoryError> {
    let records = changed_records(transaction, run_id).await?;
    insert_checkpoint_batch(transaction, run_id, event_id, &records).await
}

pub(crate) async fn finalize_empty_projection_checkpoints(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    event_id: &str,
) -> Result<(), RepositoryError> {
    insert_checkpoint_batch(transaction, run_id, event_id, &[]).await
}

async fn insert_checkpoint_batch(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    event_id: &str,
    records: &[CheckpointRecord],
) -> Result<(), RepositoryError> {
    let ledger_batch = ProjectionLedgerBatch::from_records(records)?;
    let updated = sqlx::query(
        "UPDATE execution_events SET projection_ledger_batch=$1
         WHERE run_id=$2 AND event_id=$3 AND projection_ledger_batch IS NULL",
    )
    .bind(serde_json::to_value(&ledger_batch).map_err(|_| RepositoryError::invalid_data())?)
    .bind(run_id.as_str())
    .bind(event_id)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if updated.rows_affected() != 1 {
        return Err(RepositoryError::invalid_data());
    }
    sqlx::query(
        "INSERT INTO projection_checkpoint_batches (
            run_id,event_id,checkpoint_schema_version,subject_count,manifest_hash,created_at
         ) VALUES ($1,$2,$3,$4,$5,CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(event_id)
    .bind(
        i32::try_from(PROJECTION_CHECKPOINT_SCHEMA_VERSION)
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(i64::try_from(records.len()).map_err(|_| RepositoryError::invalid_data())?)
    .bind(checkpoint_manifest_hash(records)?)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for record in records {
        sqlx::query(
            "INSERT INTO projection_checkpoints (
                run_id,event_id,subject_kind,subject_id,checkpoint_schema_version,
                subject_projection_version,projection_hash,canonical_projection,created_at
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CURRENT_TIMESTAMP)",
        )
        .bind(run_id.as_str())
        .bind(event_id)
        .bind(record.subject.kind().as_str())
        .bind(record.subject.subject_id())
        .bind(
            i32::try_from(PROJECTION_CHECKPOINT_SCHEMA_VERSION)
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .bind(i64_from_u64(record.projection_version)?)
        .bind(&record.projection_hash)
        .bind(&record.canonical_projection)
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    }
    Ok(())
}

pub(crate) async fn verify_projection_checkpoint_batch(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    event_id: &str,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        "SELECT schema_version,projection_ledger_batch FROM execution_events
         WHERE run_id=$1 AND event_id=$2",
    )
    .bind(run_id.as_str())
    .bind(event_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    decode_execution_event_schema_version(i64::from(
        row.try_get::<i32, _>("schema_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let encoded_ledger = row
        .try_get::<Value, _>("projection_ledger_batch")
        .map_err(|_| RepositoryError::invalid_data())?;
    let ledger: ProjectionLedgerBatch =
        serde_json::from_value(encoded_ledger).map_err(|_| RepositoryError::invalid_data())?;
    ledger.validate()?;
    Ok(())
}

pub(crate) async fn append_projection_mutation_event(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    transition_key: &TransitionKey,
    intent_hash: &str,
    mutation: ProjectionMutationKind,
    projection_version_after: u64,
) -> Result<String, RepositoryError> {
    let seq = allocate_event_seq(transaction, run_id).await?;
    let id = event_id(transition_key);
    let event = PendingExecutionEvent::new(
        ExecutionEventContext::for_run(run_id.clone()),
        ExecutionEventPayload::ProjectionMutated { mutation },
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    insert_event(
        transaction,
        run_id,
        seq,
        &id,
        transition_key,
        intent_hash,
        projection_version_after,
        &event,
    )
    .await?;
    Ok(id)
}

async fn current_projection_value_pool(
    pool: &PgPool,
    run_id: &RunId,
    subject: &ProjectionSubject,
) -> Result<Option<Value>, RepositoryError> {
    let table = table_for(subject)?;
    let query = format!(
        "SELECT {value} AS canonical_projection FROM {table_name} p
         WHERE p.run_id=$1 AND {id}=$2",
        value = table.value_expression,
        table_name = table.table,
        id = table.id_expression,
    );
    sqlx::query_scalar(AssertSqlSafe(query))
        .bind(run_id.as_str())
        .bind(subject.subject_id())
        .fetch_optional(pool)
        .await
        .map_err(RepositoryError::storage)
}

async fn current_projection_value_tx(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    subject: &ProjectionSubject,
) -> Result<Option<Value>, RepositoryError> {
    let table = table_for(subject)?;
    let query = format!(
        "SELECT {value} AS canonical_projection FROM {table_name} p
         WHERE p.run_id=$1 AND {id}=$2 FOR UPDATE",
        value = table.value_expression,
        table_name = table.table,
        id = table.id_expression,
    );
    sqlx::query_scalar(AssertSqlSafe(query))
        .bind(run_id.as_str())
        .bind(subject.subject_id())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)
}

async fn authoritative_snapshot_pool(
    pool: &PgPool,
    run_id: &RunId,
    subject: &ProjectionSubject,
) -> Result<Option<ProjectionRebuildSnapshot>, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(RepositoryError::storage)?;
    let snapshot = authoritative_snapshot_tx(&mut transaction, run_id, subject).await?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(snapshot)
}

async fn authoritative_snapshot_tx(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    subject: &ProjectionSubject,
) -> Result<Option<ProjectionRebuildSnapshot>, RepositoryError> {
    ensure_run_execution_event_schemas(transaction, run_id).await?;
    let rows = sqlx::query(
        "SELECT schema_version,event_id,seq,projection_ledger_batch
         FROM execution_events
         WHERE run_id=$1 AND projection_ledger_batch IS NOT NULL
         ORDER BY seq DESC",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for row in rows {
        decode_execution_event_schema_version(i64::from(
            row.try_get::<i32, _>("schema_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let value: Value = row
            .try_get("projection_ledger_batch")
            .map_err(|_| RepositoryError::invalid_data())?;
        let batch: ProjectionLedgerBatch =
            serde_json::from_value(value).map_err(|_| RepositoryError::invalid_data())?;
        let Some(record) = batch.record_for(subject)? else {
            continue;
        };
        return Ok(Some(ProjectionRebuildSnapshot::new(
            subject.clone(),
            row.try_get("event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            u64_from_i64(
                row.try_get("seq")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
            record.projection_version,
            record.projection_hash,
            record.canonical_projection,
        )));
    }
    Ok(None)
}

async fn authoritative_snapshots_tx(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<Vec<ProjectionRebuildSnapshot>, RepositoryError> {
    ensure_run_execution_event_schemas(transaction, run_id).await?;
    let rows = sqlx::query(
        "SELECT schema_version,event_id,seq,projection_ledger_batch FROM execution_events
         WHERE run_id=$1 AND projection_ledger_batch IS NOT NULL ORDER BY seq",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let mut latest = BTreeMap::new();
    for row in rows {
        decode_execution_event_schema_version(i64::from(
            row.try_get::<i32, _>("schema_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let event_id: String = row
            .try_get("event_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let event_seq = u64_from_i64(
            row.try_get("seq")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let value: Value = row
            .try_get("projection_ledger_batch")
            .map_err(|_| RepositoryError::invalid_data())?;
        let batch: ProjectionLedgerBatch =
            serde_json::from_value(value).map_err(|_| RepositoryError::invalid_data())?;
        for record in batch.validate()? {
            latest.insert(
                record.subject.clone(),
                ProjectionRebuildSnapshot::new(
                    record.subject,
                    event_id.clone(),
                    event_seq,
                    record.projection_version,
                    record.projection_hash,
                    record.canonical_projection,
                ),
            );
        }
    }
    sort_rebuild_snapshots(latest.into_values().collect())
}

fn sort_rebuild_snapshots(
    snapshots: Vec<ProjectionRebuildSnapshot>,
) -> Result<Vec<ProjectionRebuildSnapshot>, RepositoryError> {
    let parents = snapshots
        .iter()
        .filter(|snapshot| snapshot.subject().kind() == ProjectionSubjectKind::Scope)
        .map(|snapshot| {
            let parent = match snapshot
                .canonical_projection()
                .get("parent_scope_instance_id")
            {
                Some(Value::Null) => None,
                Some(Value::String(value)) => Some(value.clone()),
                _ => return Err(RepositoryError::invalid_data()),
            };
            Ok((snapshot.subject().subject_id().to_owned(), parent))
        })
        .collect::<Result<BTreeMap<_, _>, RepositoryError>>()?;
    let mut depths = BTreeMap::new();
    for scope_id in parents.keys() {
        let mut visiting = BTreeSet::new();
        scope_depth(scope_id, &parents, &mut depths, &mut visiting)?;
    }
    let mut keyed = snapshots
        .into_iter()
        .map(|snapshot| {
            let rank = repair_dependency_rank(snapshot.subject())?;
            let depth = if snapshot.subject().kind() == ProjectionSubjectKind::Scope {
                *depths
                    .get(snapshot.subject().subject_id())
                    .ok_or_else(RepositoryError::invalid_data)?
            } else {
                0
            };
            Ok((rank, depth, snapshot))
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
    keyed.sort_by(|left, right| {
        (
            left.0,
            left.1,
            left.2.subject().kind(),
            left.2.subject().subject_id(),
        )
            .cmp(&(
                right.0,
                right.1,
                right.2.subject().kind(),
                right.2.subject().subject_id(),
            ))
    });
    Ok(keyed.into_iter().map(|(_, _, snapshot)| snapshot).collect())
}

fn scope_depth(
    scope_id: &str,
    parents: &BTreeMap<String, Option<String>>,
    depths: &mut BTreeMap<String, u64>,
    visiting: &mut BTreeSet<String>,
) -> Result<u64, RepositoryError> {
    if let Some(depth) = depths.get(scope_id) {
        return Ok(*depth);
    }
    if !visiting.insert(scope_id.to_owned()) {
        return Err(RepositoryError::invalid_data());
    }
    let parent = parents
        .get(scope_id)
        .ok_or_else(RepositoryError::invalid_data)?;
    let depth = match parent {
        None => 0,
        Some(parent_id) => {
            if parent_id == scope_id || !parents.contains_key(parent_id) {
                return Err(RepositoryError::invalid_data());
            }
            scope_depth(parent_id, parents, depths, visiting)?
                .checked_add(1)
                .ok_or_else(RepositoryError::invalid_data)?
        }
    };
    visiting.remove(scope_id);
    depths.insert(scope_id.to_owned(), depth);
    Ok(depth)
}

fn repair_dependency_rank(subject: &ProjectionSubject) -> Result<u8, RepositoryError> {
    Ok(match table_for(subject)?.table {
        "workflow_runs" => 0,
        "scope_instances" => 10,
        "node_activations" => 20,
        "node_attempts" => 30,
        "scheduler_checkpoints" => 31,
        "timers"
        | "scheduler_values"
        | "scheduler_occurrence_values"
        | "fork_groups"
        | "run_recovery_lineage"
        | "run_migration_intents"
        | "recovery_revision_roots"
        | "recovery_artifact_roots"
        | "recovery_effect_roots"
        | "run_reuse_candidates" => 40,
        "scheduler_wait_registrations" | "scheduler_subflow_invocations" => 45,
        "control_tokens" => 50,
        "fork_legs" => 60,
        "join_arrivals" => 70,
        _ => return Err(RepositoryError::invalid_configuration()),
    })
}

async fn repair_registered_subject(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    subject: &ProjectionSubject,
    value: &Value,
) -> Result<u64, RepositoryError> {
    let table = table_for(subject)?;
    let columns = repair_columns(table)?;
    let names = columns.join(",");
    let source_names = columns
        .iter()
        .map(|column| format!("s.{column}"))
        .collect::<Vec<_>>()
        .join(",");
    let id_expression = table.id_expression.replace("p.", "t.");
    let query = format!(
        "UPDATE {table_name} t SET ({names})=({source_names})
         FROM jsonb_populate_record(NULL::{table_name},$1) s
         WHERE t.run_id=$2 AND {id_expression}=$3",
        table_name = table.table,
    );
    // All identifiers and expressions originate in the closed registries;
    // checkpoint JSON and subject identity are bind parameters only.
    let rows = sqlx::query(AssertSqlSafe(query))
        .bind(value)
        .bind(run_id.as_str())
        .bind(subject.subject_id())
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
    Ok(rows)
}

async fn insert_missing_subject(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    subject: &ProjectionSubject,
    value: &Value,
) -> Result<(), RepositoryError> {
    let table = table_for(subject)?;
    let identity_columns = identity_columns(table)?;
    let projection_columns = repair_columns(table)?;
    let identity = repair_identity(table, run_id, subject)?;
    let names = identity_columns
        .iter()
        .chain(projection_columns.iter())
        .copied()
        .collect::<Vec<_>>()
        .join(",");
    let identity_values = identity_columns.iter().map(|column| format!("i.{column}"));
    let projection_values = projection_columns
        .iter()
        .map(|column| format!("s.{column}"));
    let values = identity_values
        .chain(projection_values)
        .collect::<Vec<_>>()
        .join(",");
    let query = format!(
        "INSERT INTO {table_name} ({names})
         SELECT {values}
         FROM jsonb_populate_record(NULL::{table_name},$1) s,
              jsonb_populate_record(NULL::{table_name},$2) i
         ON CONFLICT DO NOTHING",
        table_name = table.table,
    );
    sqlx::query(AssertSqlSafe(query))
        .bind(value)
        .bind(identity)
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    if table.table == "workflow_runs" {
        sqlx::query(
            "UPDATE workflow_runs SET next_event_seq=(
                 SELECT COALESCE(MAX(seq),0)+1 FROM execution_events WHERE run_id=$1
             ) WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    }
    Ok(())
}

async fn repair_subject(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    subject: &ProjectionSubject,
    value: &Value,
) -> Result<(), RepositoryError> {
    let rows = match subject.kind() {
        ProjectionSubjectKind::Run => sqlx::query(
            "UPDATE workflow_runs t SET
                (definition_id,definition_revision_id,deployment_revision_id,plan_hash,binding_hash,
                 request_id,attachment,
                 lifecycle,admission_state,termination_intent_reason,termination_intent_transition_key,
                 termination_intent_at,input_payload_id,output_payload_id,output_artifact_id,
                 output_value_hash,error_code,terminal_event_id,terminal_public_event_id,parent_run_id,
                 lineage_kind,generation,replacement_run_id,projection_version,created_at,started_at,deadline_at,updated_at,terminal_at)
                = (s.definition_id,s.definition_revision_id,s.deployment_revision_id,s.plan_hash,s.binding_hash,
                   s.request_id,s.attachment,
                   s.lifecycle,s.admission_state,s.termination_intent_reason,s.termination_intent_transition_key,
                   s.termination_intent_at,s.input_payload_id,s.output_payload_id,s.output_artifact_id,
                   s.output_value_hash,s.error_code,s.terminal_event_id,s.terminal_public_event_id,s.parent_run_id,
                   s.lineage_kind,s.generation,s.replacement_run_id,s.projection_version,s.created_at,s.started_at,s.deadline_at,s.updated_at,s.terminal_at)
             FROM jsonb_populate_record(NULL::workflow_runs,$1) s WHERE t.run_id=$2",
        )
        .bind(value)
        .bind(run_id.as_str())
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected(),
        ProjectionSubjectKind::Scope => sqlx::query(
            "UPDATE scope_instances t SET
                (parent_scope_instance_id,static_scope_id,stable_dynamic_key,scope_kind,is_root,
                 lifecycle,admission_state,admitted_children,settled_children,projection_version,created_at,settled_at)
                = (s.parent_scope_instance_id,s.static_scope_id,s.stable_dynamic_key,s.scope_kind,s.is_root,
                   s.lifecycle,s.admission_state,s.admitted_children,s.settled_children,s.projection_version,s.created_at,s.settled_at)
             FROM jsonb_populate_record(NULL::scope_instances,$1) s
             WHERE t.run_id=$2 AND t.scope_instance_id=$3",
        )
        .bind(value)
        .bind(run_id.as_str())
        .bind(subject.subject_id())
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected(),
        ProjectionSubjectKind::Activation => sqlx::query(
            "UPDATE node_activations t SET
                (scope_instance_id,node_id,stable_activation_key,execution_kind,lifecycle,effect_id,
                 effect_idempotency,effect_evidence,last_attempt_no,last_lease_epoch,current_attempt_no,
                 current_lease_epoch,current_fencing_token,retry_budget_remaining,pending_retry_timer_id,
                 wait_registration_transition_key,termination_intent_reason,termination_intent_transition_key,
                 termination_intent_at,output_payload_id,output_artifact_id,output_value_hash,winning_attempt_no,
                 reused_from_run_id,reused_from_activation_id,projection_version,created_at,updated_at,terminal_at)
                = (s.scope_instance_id,s.node_id,s.stable_activation_key,s.execution_kind,s.lifecycle,s.effect_id,
                   s.effect_idempotency,s.effect_evidence,s.last_attempt_no,s.last_lease_epoch,s.current_attempt_no,
                   s.current_lease_epoch,s.current_fencing_token,s.retry_budget_remaining,s.pending_retry_timer_id,
                   s.wait_registration_transition_key,s.termination_intent_reason,s.termination_intent_transition_key,
                   s.termination_intent_at,s.output_payload_id,s.output_artifact_id,s.output_value_hash,s.winning_attempt_no,
                   s.reused_from_run_id,s.reused_from_activation_id,s.projection_version,s.created_at,s.updated_at,s.terminal_at)
             FROM jsonb_populate_record(NULL::node_activations,$1) s
             WHERE t.run_id=$2 AND t.activation_id=$3",
        )
        .bind(value)
        .bind(run_id.as_str())
        .bind(subject.subject_id())
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected(),
        ProjectionSubjectKind::Attempt => {
            let (activation_id, attempt_no) = parse_attempt_subject_id(subject.subject_id())?;
            sqlx::query(
                "UPDATE node_attempts t SET
                    (lease_epoch,fencing_token,effect_id,lifecycle,effect_evidence,worker_id,
                     lease_expires_at,heartbeat_at,output_payload_id,output_artifact_id,output_value_hash,
                     failure_code,completion_transition_key,terminal_event_id,projection_version,
                     created_at,started_at,terminal_at)
                    = (s.lease_epoch,s.fencing_token,s.effect_id,s.lifecycle,s.effect_evidence,s.worker_id,
                       s.lease_expires_at,s.heartbeat_at,s.output_payload_id,s.output_artifact_id,s.output_value_hash,
                       s.failure_code,s.completion_transition_key,s.terminal_event_id,s.projection_version,
                       s.created_at,s.started_at,s.terminal_at)
                 FROM jsonb_populate_record(NULL::node_attempts,$1) s
                 WHERE t.run_id=$2 AND t.activation_id=$3 AND t.attempt_no=$4",
            )
            .bind(value)
            .bind(run_id.as_str())
            .bind(activation_id)
            .bind(i32::try_from(attempt_no).map_err(|_| RepositoryError::invalid_data())?)
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected()
        }
        ProjectionSubjectKind::Timer => sqlx::query(
            "UPDATE timers t SET
                (activation_id,timer_kind,timer_state,deadline_at,expected_attempt_no,
                 expected_lease_epoch,expected_fencing_token,retry_budget_snapshot,
                 created_by_transition_key,fired_by_transition_key,fired_event_id,
                 projection_version,created_at,fired_at)
                = (s.activation_id,s.timer_kind,s.timer_state,s.deadline_at,s.expected_attempt_no,
                   s.expected_lease_epoch,s.expected_fencing_token,s.retry_budget_snapshot,
                   s.created_by_transition_key,s.fired_by_transition_key,s.fired_event_id,
                   s.projection_version,s.created_at,s.fired_at)
             FROM jsonb_populate_record(NULL::timers,$1) s
             WHERE t.run_id=$2 AND t.timer_id=$3",
        )
        .bind(value)
        .bind(run_id.as_str())
        .bind(subject.subject_id())
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected(),
        ProjectionSubjectKind::Control
        | ProjectionSubjectKind::Fork
        | ProjectionSubjectKind::Join
        | ProjectionSubjectKind::Scheduler
        | ProjectionSubjectKind::DataValue => {
            repair_registered_subject(transaction, run_id, subject, value).await?
        }
    };
    if rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn lock_projection_repair(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<(), RepositoryError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!(
            "insight-agent/projection-repair/{}",
            run_id.as_str()
        ))
        .fetch_one(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    Ok(())
}

async fn repair_snapshot_tx(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    authoritative: ProjectionRebuildSnapshot,
) -> Result<ProjectionRepairReceipt, RepositoryError> {
    let subject = authoritative.subject();
    let current = current_projection_value_tx(transaction, run_id, subject).await?;
    let current_hash = current
        .as_ref()
        .map(canonical_value)
        .transpose()?
        .map(|(_, hash)| hash);
    if current_hash.as_ref().map(|hash| hash.as_str()) == Some(authoritative.projection_hash()) {
        return Ok(ProjectionRepairReceipt::new(false, authoritative));
    }
    if current.is_some() {
        repair_subject(
            transaction,
            run_id,
            subject,
            authoritative.canonical_projection(),
        )
        .await?;
    } else {
        insert_missing_subject(
            transaction,
            run_id,
            subject,
            authoritative.canonical_projection(),
        )
        .await?;
    }
    let repaired = current_projection_value_tx(transaction, run_id, subject)
        .await?
        .ok_or_else(RepositoryError::invalid_data)?;
    let (_, repaired_hash) = canonical_value(&repaired)?;
    if repaired_hash.as_str() != authoritative.projection_hash() {
        return Err(RepositoryError::invalid_data());
    }
    Ok(ProjectionRepairReceipt::new(true, authoritative))
}

#[async_trait]
impl ProjectionDurableRepository for PostgresDurableRepository {
    async fn audit_projection(
        &self,
        run_id: &RunId,
        subject: &ProjectionSubject,
    ) -> Result<ProjectionAudit, RepositoryError> {
        let authoritative = authoritative_snapshot_pool(&self.pool, run_id, subject)
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        let current = current_projection_value_pool(&self.pool, run_id, subject).await?;
        let actual_hash = current
            .as_ref()
            .map(canonical_value)
            .transpose()?
            .map(|(_, hash)| hash.as_str().to_owned());
        if actual_hash.as_deref() == Some(authoritative.projection_hash()) {
            Ok(ProjectionAudit::Match { authoritative })
        } else {
            Ok(ProjectionAudit::Mismatch {
                authoritative,
                actual_hash,
            })
        }
    }

    async fn load_authoritative_rebuild_snapshot(
        &self,
        run_id: &RunId,
        subject: &ProjectionSubject,
    ) -> Result<Option<ProjectionRebuildSnapshot>, RepositoryError> {
        authoritative_snapshot_pool(&self.pool, run_id, subject).await
    }

    async fn repair_projection(
        &self,
        run_id: &RunId,
        subject: &ProjectionSubject,
    ) -> Result<ProjectionRepairReceipt, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        lock_projection_repair(&mut transaction, run_id).await?;
        let authoritative = authoritative_snapshot_tx(&mut transaction, run_id, subject)
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        let receipt = repair_snapshot_tx(&mut transaction, run_id, authoritative).await?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(receipt)
    }

    async fn repair_all_projections(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<ProjectionRepairReceipt>, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        lock_projection_repair(&mut transaction, run_id).await?;
        let snapshots = authoritative_snapshots_tx(&mut transaction, run_id).await?;
        let mut receipts = Vec::with_capacity(snapshots.len());
        for snapshot in snapshots {
            receipts.push(repair_snapshot_tx(&mut transaction, run_id, snapshot).await?);
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(receipts)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn scope_snapshot(id: &str, parent: Option<&str>) -> ProjectionRebuildSnapshot {
        ProjectionRebuildSnapshot::new(
            ProjectionSubject::scope(id).unwrap(),
            format!("event-{id}"),
            1,
            1,
            "hash".to_owned(),
            json!({"parent_scope_instance_id": parent}),
        )
    }

    #[test]
    fn rebuild_order_is_parent_first_for_deeply_nested_scopes() {
        let ordered = sort_rebuild_snapshots(vec![
            scope_snapshot("grandchild", Some("child")),
            scope_snapshot("root", None),
            scope_snapshot("child", Some("root")),
        ])
        .unwrap();

        assert_eq!(
            ordered
                .iter()
                .map(|snapshot| snapshot.subject().subject_id())
                .collect::<Vec<_>>(),
            vec!["root", "child", "grandchild"]
        );
    }

    #[test]
    fn rebuild_order_rejects_scope_cycles() {
        assert!(sort_rebuild_snapshots(vec![
            scope_snapshot("left", Some("right")),
            scope_snapshot("right", Some("left")),
        ])
        .is_err());
    }
}
