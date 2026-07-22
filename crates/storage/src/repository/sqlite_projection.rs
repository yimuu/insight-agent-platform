use super::RepositoryErrorExt as _;

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use insight_durable::common::adapter::{
    canonical_json, canonical_value, decode_execution_event_schema_version, event_id, i64_from_u64,
    u64_from_i64,
};
use insight_durable::projection::adapter::{
    self as projection_adapter, attempt_subject_id, checkpoint_manifest_hash,
    decode_hex_subject_components, parse_attempt_subject_id,
};
use serde_json::Value;
use sqlx::{AssertSqlSafe, Row, Sqlite, SqlitePool, Transaction};

use insight_engine::{
    ExecutionEventContext, ExecutionEventPayload, PendingExecutionEvent, ProjectionMutationKind,
    RunId, TransitionKey,
};

use super::projection::{
    ProjectionAudit, ProjectionDurableRepository, ProjectionRebuildSnapshot,
    ProjectionRepairReceipt, ProjectionSubject, ProjectionSubjectKind,
};
use super::sqlite::{allocate_event_seq, insert_event, SqliteDurableRepository};
use super::RepositoryError;

const RUN_VALUE: &str = "json_object(
    'definition_id',p.definition_id,'definition_revision_id',p.definition_revision_id,
    'deployment_revision_id',p.deployment_revision_id,'plan_hash',p.plan_hash,
    'binding_hash',p.binding_hash,'request_id',p.request_id,'attachment',p.attachment,
    'lifecycle',p.lifecycle,'admission_state',p.admission_state,
    'termination_intent_reason',p.termination_intent_reason,
    'termination_intent_transition_key',p.termination_intent_transition_key,
    'termination_intent_at',p.termination_intent_at,'input_payload_id',p.input_payload_id,
    'output_payload_id',p.output_payload_id,'output_artifact_id',p.output_artifact_id,
    'output_value_hash',p.output_value_hash,'error_code',p.error_code,
    'terminal_event_id',p.terminal_event_id,'terminal_public_event_id',p.terminal_public_event_id,
    'parent_run_id',p.parent_run_id,'lineage_kind',p.lineage_kind,'generation',p.generation,
    'replacement_run_id',p.replacement_run_id,'projection_version',p.projection_version,
    'created_at',p.created_at,'started_at',p.started_at,'deadline_at',p.deadline_at,
    'updated_at',p.updated_at,'terminal_at',p.terminal_at)";

const SCOPE_VALUE: &str = "json_object(
    'parent_scope_instance_id',p.parent_scope_instance_id,'static_scope_id',p.static_scope_id,
    'stable_dynamic_key',p.stable_dynamic_key,'scope_kind',p.scope_kind,'is_root',p.is_root,
    'lifecycle',p.lifecycle,'admission_state',p.admission_state,
    'admitted_children',p.admitted_children,'settled_children',p.settled_children,
    'projection_version',p.projection_version,'created_at',p.created_at,'settled_at',p.settled_at)";

const ACTIVATION_VALUE: &str = "json_object(
    'scope_instance_id',p.scope_instance_id,'node_id',p.node_id,
    'stable_activation_key',p.stable_activation_key,'execution_kind',p.execution_kind,
    'lifecycle',p.lifecycle,'effect_id',p.effect_id,'effect_idempotency',p.effect_idempotency,
    'effect_evidence',p.effect_evidence,'last_attempt_no',p.last_attempt_no,
    'last_lease_epoch',p.last_lease_epoch,'current_attempt_no',p.current_attempt_no,
    'current_lease_epoch',p.current_lease_epoch,'current_fencing_token',p.current_fencing_token,
    'retry_budget_remaining',p.retry_budget_remaining,'pending_retry_timer_id',p.pending_retry_timer_id,
    'wait_registration_transition_key',p.wait_registration_transition_key,
    'termination_intent_reason',p.termination_intent_reason,
    'termination_intent_transition_key',p.termination_intent_transition_key,
    'termination_intent_at',p.termination_intent_at,'output_payload_id',p.output_payload_id,
    'output_artifact_id',p.output_artifact_id,'output_value_hash',p.output_value_hash,
    'winning_attempt_no',p.winning_attempt_no,'reused_from_run_id',p.reused_from_run_id,
    'reused_from_activation_id',p.reused_from_activation_id,
    'projection_version',p.projection_version,'created_at',p.created_at,
    'updated_at',p.updated_at,'terminal_at',p.terminal_at)";

const ATTEMPT_VALUE: &str = "json_object(
    'lease_epoch',p.lease_epoch,'fencing_token',p.fencing_token,'effect_id',p.effect_id,
    'lifecycle',p.lifecycle,'effect_evidence',p.effect_evidence,'worker_id',p.worker_id,
    'lease_expires_at',p.lease_expires_at,'heartbeat_at',p.heartbeat_at,
    'output_payload_id',p.output_payload_id,'output_artifact_id',p.output_artifact_id,
    'output_value_hash',p.output_value_hash,'failure_code',p.failure_code,
    'completion_transition_key',p.completion_transition_key,'terminal_event_id',p.terminal_event_id,
    'projection_version',p.projection_version,'created_at',p.created_at,
    'started_at',p.started_at,'terminal_at',p.terminal_at)";

const TIMER_VALUE: &str = "json_object(
    'activation_id',p.activation_id,'timer_kind',p.timer_kind,'timer_state',p.timer_state,
    'deadline_at',p.deadline_at,'expected_attempt_no',p.expected_attempt_no,
    'expected_lease_epoch',p.expected_lease_epoch,
    'expected_fencing_token',p.expected_fencing_token,
    'retry_budget_snapshot',p.retry_budget_snapshot,
    'created_by_transition_key',p.created_by_transition_key,
    'fired_by_transition_key',p.fired_by_transition_key,'fired_event_id',p.fired_event_id,
    'projection_version',p.projection_version,'created_at',p.created_at,'fired_at',p.fired_at)";

const SCHEDULER_CHECKPOINT_VALUE: &str = "json_object(
    'content_hash',p.content_hash,'checkpoint_kind',p.checkpoint_kind,
    'transition_key',p.transition_key,
    'intent_hash',p.intent_hash,'event_id',p.event_id,
    'checkpoint_schema_version',p.checkpoint_schema_version,
    'scheduler_projection_version',p.scheduler_projection_version,
    'fact_payload',json(p.fact_payload),'projection_version',p.projection_version,
    'created_at',p.created_at)";

const SCHEDULER_VALUE_VALUE: &str = "json_object(
    'owner_activation_id',p.owner_activation_id,'runtime_value',json(p.runtime_value),
    'value_ref',json(p.value_ref),'declared_type',json(p.declared_type),
    'storage_kind',p.storage_kind,'payload_id',p.payload_id,'artifact_id',p.artifact_id,
    'content_hash',p.content_hash,'projection_version',p.projection_version,
    'created_at',p.created_at,'updated_at',p.updated_at)";

const OCCURRENCE_VALUE_VALUE: &str = "json_object(
    'owner_activation_id',p.owner_activation_id,'runtime_value',json(p.runtime_value),
    'value_ref',json(p.value_ref),'declared_type',json(p.declared_type),
    'storage_kind',p.storage_kind,'payload_id',p.payload_id,'artifact_id',p.artifact_id,
    'content_hash',p.content_hash,'projection_version',p.projection_version,
    'created_at',p.created_at,'updated_at',p.updated_at)";

const WAIT_REGISTRATION_VALUE: &str = "json_object(
    'activation_id',p.activation_id,'node_id',p.node_id,
    'occurrence_key',json(p.occurrence_key),'signal_name',p.signal_name,
    'signal_id',p.signal_id,'timer_id',p.timer_id,'due_at_ms',p.due_at_ms,
    'payload_type',CASE WHEN p.payload_type IS NULL THEN NULL ELSE json(p.payload_type) END,
    'winner_kind',p.winner_kind,'winner_signal_id',p.winner_signal_id,
    'winner_timer_id',p.winner_timer_id,'projection_version',p.projection_version,
    'created_at',p.created_at,'resolved_at',p.resolved_at)";

const SUBFLOW_INVOCATION_VALUE: &str = "json_object(
    'parent_activation_id',p.parent_activation_id,'node_id',p.node_id,
    'occurrence_key',json(p.occurrence_key),
    'definition_revision_id',p.definition_revision_id,
    'deployment_revision_id',p.deployment_revision_id,'plan_hash',p.plan_hash,
    'binding_hash',p.binding_hash,'interface_version',p.interface_version,
    'output_contracts',json(p.output_contracts),'invocation_state',p.invocation_state,
    'projection_version',p.projection_version,'created_at',p.created_at,
    'completed_at',p.completed_at)";

const CONTROL_TOKEN_VALUE: &str = "json_object(
    'current_scope_instance_id',p.current_scope_instance_id,'current_port_id',p.current_port_id,
    'source_activation_id',p.source_activation_id,'source_port_id',p.source_port_id,
    'emission_slot',p.emission_slot,'emitted_by_transition_key',p.emitted_by_transition_key,
    'provenance_frames',json(p.provenance_frames),'branch_activation_id',p.branch_activation_id,
    'selected_branch_port_id',p.selected_branch_port_id,'fork_group_id',p.fork_group_id,
    'fork_leg_id',p.fork_leg_id,'token_state',p.token_state,
    'consumed_by_activation_id',p.consumed_by_activation_id,
    'consumed_by_transition_key',p.consumed_by_transition_key,'consumed_at',p.consumed_at,
    'revoked_by_transition_key',p.revoked_by_transition_key,'revoked_at',p.revoked_at,
    'projection_version',p.projection_version,'created_at',p.created_at)";

const REUSE_VALUE: &str = "json_object(
    'target_scope_instance_id',p.target_scope_instance_id,'target_node_id',p.target_node_id,
    'stable_activation_key',p.stable_activation_key,'source_run_id',p.source_run_id,
    'source_activation_id',p.source_activation_id,
    'source_control_provenance',json(p.source_control_provenance),
    'definition_revision_id',p.definition_revision_id,
    'deployment_revision_id',p.deployment_revision_id,'plan_hash',p.plan_hash,
    'binding_hash',p.binding_hash,'node_config_hash',p.node_config_hash,
    'descriptor_hash',p.descriptor_hash,'input_value_hash',p.input_value_hash,
    'output_value_hash',p.output_value_hash,'output_schema_hash',p.output_schema_hash,
    'effect_policy_hash',p.effect_policy_hash,'inherited_effect_id',p.inherited_effect_id,
    'data_dependencies_hash',p.data_dependencies_hash,
    'created_by_transition_key',p.created_by_transition_key,'candidate_state',p.candidate_state,
    'materialized_activation_id',p.materialized_activation_id,
    'decision_transition_key',p.decision_transition_key,
    'rejection_reason',p.rejection_reason,
    'projection_version',p.projection_version,'created_at',p.created_at,'decided_at',p.decided_at)";

const RECOVERY_LINEAGE_VALUE: &str = "json_object(
    'source_run_id',p.source_run_id,'lineage_kind',p.lineage_kind,
    'source_generation',p.source_generation,'target_generation',p.target_generation,
    'source_definition_id',p.source_definition_id,
    'source_definition_revision_id',p.source_definition_revision_id,
    'source_deployment_revision_id',p.source_deployment_revision_id,
    'source_plan_hash',p.source_plan_hash,'source_binding_hash',p.source_binding_hash,
    'target_definition_id',p.target_definition_id,
    'target_definition_revision_id',p.target_definition_revision_id,
    'target_deployment_revision_id',p.target_deployment_revision_id,
    'target_plan_hash',p.target_plan_hash,'target_binding_hash',p.target_binding_hash,
    'source_checkpoint_hash',p.source_checkpoint_hash,
    'created_by_transition_key',p.created_by_transition_key,
    'projection_version',p.projection_version,'created_at',p.created_at)";

const MIGRATION_INTENT_VALUE: &str = "json_object(
    'target_run_id',p.target_run_id,'target_definition_id',p.target_definition_id,
    'target_definition_revision_id',p.target_definition_revision_id,
    'target_deployment_revision_id',p.target_deployment_revision_id,
    'target_plan_hash',p.target_plan_hash,'target_binding_hash',p.target_binding_hash,
    'target_input',json(p.target_input),'target_input_hash',p.target_input_hash,
    'mapping_contracts',json(p.mapping_contracts),'mapping_hash',p.mapping_hash,
    'reuse_candidates',json(p.reuse_candidates),
    'intent_transition_key',p.intent_transition_key,'intent_event_id',p.intent_event_id,
    'intent_state',p.intent_state,'final_transition_key',p.final_transition_key,
    'projection_version',p.projection_version,'created_at',p.created_at,
    'completed_at',p.completed_at)";

const RECOVERY_REVISION_ROOT_VALUE: &str = "json_object(
    'source_run_id',p.source_run_id,'definition_id',p.definition_id,
    'definition_revision_id',p.definition_revision_id,
    'deployment_revision_id',p.deployment_revision_id,'plan_hash',p.plan_hash,
    'binding_hash',p.binding_hash,'created_by_transition_key',p.created_by_transition_key,
    'projection_version',p.projection_version,'created_at',p.created_at)";

const RECOVERY_ARTIFACT_ROOT_VALUE: &str = "json_object(
    'source_run_id',p.source_run_id,'source_activation_id',p.source_activation_id,
    'created_by_transition_key',p.created_by_transition_key,
    'projection_version',p.projection_version,'created_at',p.created_at)";

const RECOVERY_EFFECT_ROOT_VALUE: &str = "json_object(
    'source_run_id',p.source_run_id,
    'created_by_transition_key',p.created_by_transition_key,
    'projection_version',p.projection_version,'created_at',p.created_at)";

const FORK_GROUP_VALUE: &str = "json_object(
    'fork_activation_id',p.fork_activation_id,
    'parent_scope_instance_id',p.parent_scope_instance_id,
    'join_activation_id',p.join_activation_id,'join_mode',p.join_mode,
    'expected_legs',p.expected_legs,'group_state',p.group_state,
    'admitted_legs',p.admitted_legs,'settled_legs',p.settled_legs,
    'failure_leg_id',p.failure_leg_id,
    'failure_settlement_class',p.failure_settlement_class,
    'projection_version',p.projection_version,'created_at',p.created_at,'settled_at',p.settled_at)";

const FORK_LEG_VALUE: &str = "json_object(
    'declaration_index',p.declaration_index,'scope_instance_id',p.scope_instance_id,
    'child_activation_id',p.child_activation_id,'token_id',p.token_id,
    'is_required',p.is_required,'leg_state',p.leg_state,
    'settlement_class',p.settlement_class,'projection_version',p.projection_version,
    'created_at',p.created_at,'settled_at',p.settled_at)";

const JOIN_ARRIVAL_VALUE: &str = "json_object(
    'token_id',p.token_id,
    'arrival_transition_key',p.arrival_transition_key,'arrival_event_id',p.arrival_event_id,
    'settlement_class',p.settlement_class,'value_payload_id',p.value_payload_id,
    'value_artifact_id',p.value_artifact_id,'value_hash',p.value_hash,
    'projection_version',p.projection_version,'arrived_at',p.arrived_at)";

struct ProjectionTable {
    kind: ProjectionSubjectKind,
    subject_prefix: &'static str,
    table: &'static str,
    id_expression: &'static str,
    version_expression: &'static str,
    value_expression: &'static str,
}

const TABLES: &[ProjectionTable] = &[
    ProjectionTable {
        kind: ProjectionSubjectKind::Run,
        subject_prefix: "",
        table: "workflow_runs",
        id_expression: "p.run_id",
        version_expression: "p.projection_version",
        value_expression: RUN_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Scope,
        subject_prefix: "",
        table: "scope_instances",
        id_expression: "p.scope_instance_id",
        version_expression: "p.projection_version",
        value_expression: SCOPE_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Activation,
        subject_prefix: "",
        table: "node_activations",
        id_expression: "p.activation_id",
        version_expression: "p.projection_version",
        value_expression: ACTIVATION_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Attempt,
        subject_prefix: "",
        table: "node_attempts",
        id_expression: "p.activation_id || '#' || p.attempt_no",
        version_expression: "p.projection_version",
        value_expression: ATTEMPT_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Timer,
        subject_prefix: "",
        table: "timers",
        id_expression: "p.timer_id",
        version_expression: "p.projection_version",
        value_expression: TIMER_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Scheduler,
        subject_prefix: "",
        table: "scheduler_checkpoints",
        id_expression: "p.checkpoint_id",
        version_expression: "p.projection_version",
        value_expression: SCHEDULER_CHECKPOINT_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::DataValue,
        subject_prefix: "occurrence:",
        table: "scheduler_occurrence_values",
        id_expression: "'occurrence:' || lower(hex(CAST(p.occurrence_key AS BLOB))) || ':' || lower(hex(CAST(p.port_id AS BLOB)))",
        version_expression: "p.projection_version",
        value_expression: OCCURRENCE_VALUE_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::DataValue,
        subject_prefix: "",
        table: "scheduler_values",
        id_expression: "p.port_id",
        version_expression: "p.projection_version",
        value_expression: SCHEDULER_VALUE_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "wait:",
        table: "scheduler_wait_registrations",
        id_expression: "'wait:' || p.wait_id",
        version_expression: "p.projection_version",
        value_expression: WAIT_REGISTRATION_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "subflow:",
        table: "scheduler_subflow_invocations",
        id_expression: "'subflow:' || p.child_run_id",
        version_expression: "p.projection_version",
        value_expression: SUBFLOW_INVOCATION_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "token:",
        table: "control_tokens",
        id_expression: "'token:' || p.token_id",
        version_expression: "p.projection_version",
        value_expression: CONTROL_TOKEN_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "reuse:",
        table: "run_reuse_candidates",
        id_expression: "'reuse:' || p.candidate_id",
        version_expression: "p.projection_version",
        value_expression: REUSE_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "recovery:lineage",
        table: "run_recovery_lineage",
        id_expression: "'recovery:lineage'",
        version_expression: "p.projection_version",
        value_expression: RECOVERY_LINEAGE_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "recovery:migration",
        table: "run_migration_intents",
        id_expression: "'recovery:migration'",
        version_expression: "p.projection_version",
        value_expression: MIGRATION_INTENT_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "recovery:revision:",
        table: "recovery_revision_roots",
        id_expression: "'recovery:revision:' || p.root_role",
        version_expression: "p.projection_version",
        value_expression: RECOVERY_REVISION_ROOT_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "recovery:artifact:",
        table: "recovery_artifact_roots",
        id_expression: "'recovery:artifact:' || lower(hex(CAST(p.artifact_run_id AS BLOB))) || ':' || lower(hex(CAST(p.artifact_id AS BLOB)))",
        version_expression: "p.projection_version",
        value_expression: RECOVERY_ARTIFACT_ROOT_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Control,
        subject_prefix: "recovery:effect:",
        table: "recovery_effect_roots",
        id_expression: "'recovery:effect:' || lower(hex(CAST(p.effect_run_id AS BLOB))) || ':' || lower(hex(CAST(p.source_activation_id AS BLOB))) || ':' || lower(hex(CAST(p.effect_id AS BLOB)))",
        version_expression: "p.projection_version",
        value_expression: RECOVERY_EFFECT_ROOT_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Fork,
        subject_prefix: "group:",
        table: "fork_groups",
        id_expression: "'group:' || p.fork_group_id",
        version_expression: "p.projection_version",
        value_expression: FORK_GROUP_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Fork,
        subject_prefix: "leg:",
        table: "fork_legs",
        id_expression: "'leg:' || lower(hex(CAST(p.fork_group_id AS BLOB))) || ':' || lower(hex(CAST(p.leg_id AS BLOB)))",
        version_expression: "p.projection_version",
        value_expression: FORK_LEG_VALUE,
    },
    ProjectionTable {
        kind: ProjectionSubjectKind::Join,
        subject_prefix: "arrival:",
        table: "join_arrivals",
        id_expression: "'arrival:' || lower(hex(CAST(p.join_activation_id AS BLOB))) || ':' || lower(hex(CAST(p.fork_group_id AS BLOB))) || ':' || lower(hex(CAST(p.leg_id AS BLOB)))",
        version_expression: "p.projection_version",
        value_expression: JOIN_ARRIVAL_VALUE,
    },
];

async fn ensure_run_execution_event_schemas(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
) -> Result<(), RepositoryError> {
    // Projection rebuild consumes only the separately versioned, closed
    // `projection_ledger_batch`; it never interprets event kind/context/body.
    // The event envelope schema is still checked before its sequence or ledger
    // can participate in authority selection.
    let unknown = sqlx::query_scalar::<_, i64>(
        "SELECT schema_version FROM execution_events
         WHERE run_id=? AND schema_version<>? LIMIT 1",
    )
    .bind(run_id.as_str())
    .bind(i64::from(insight_engine::EXECUTION_EVENT_SCHEMA_VERSION))
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if let Some(version) = unknown {
        decode_execution_event_schema_version(version)?;
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

/// Returns the only columns that projection repair may restore for a table.
///
/// Table names, subject selectors, and columns all come from this closed
/// registry. Checkpoint JSON is authoritative projection data, but it is never
/// authoritative SQL: its keys cannot select a table/column and row identity is
/// deliberately absent from every list below.
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
        "scheduler_occurrence_values" => Ok(&[
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
        "scheduler_values" => Ok(&[
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
            serde_json::from_str::<Value>(&parts[0])
                .map_err(|_| RepositoryError::invalid_data())?;
            put_identity(&mut identity, "occurrence_key", &parts[0]);
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

fn sqlite_json_column(table: &str, column: &str) -> bool {
    matches!(
        (table, column),
        ("scheduler_checkpoints", "fact_payload")
            | (
                "scheduler_values" | "scheduler_occurrence_values",
                "runtime_value" | "value_ref" | "declared_type"
            )
            | (
                "scheduler_wait_registrations",
                "occurrence_key" | "payload_type"
            )
            | (
                "scheduler_subflow_invocations",
                "occurrence_key" | "output_contracts"
            )
            | ("control_tokens", "provenance_frames")
            | ("run_reuse_candidates", "source_control_provenance")
            | (
                "run_migration_intents",
                "target_input" | "mapping_contracts" | "reuse_candidates"
            )
    )
}

fn sqlite_projection_value(table: &str, column: &str) -> String {
    if sqlite_json_column(table, column) {
        format!("CASE WHEN json_type(?1,'$.{column}')='null' THEN NULL ELSE ?1 -> '$.{column}' END")
    } else {
        format!("json_extract(?1,'$.{column}')")
    }
}

async fn changed_records(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
) -> Result<Vec<Value>, RepositoryError> {
    let mut records = Vec::new();
    for table in TABLES {
        let query = format!(
            "SELECT {id} AS subject_id, {version} AS projection_version,
                    {value} AS canonical_projection
             FROM {table_name} p
             WHERE p.run_id = ?
               AND NOT EXISTS (
                   SELECT 1 FROM projection_checkpoints c
                   WHERE c.run_id = p.run_id
                     AND c.subject_kind = ?
                     AND c.subject_id = {id}
                     AND c.subject_projection_version = {version}
               )
             ORDER BY subject_id",
            id = table.id_expression,
            version = table.version_expression,
            value = table.value_expression,
            table_name = table.table,
        );
        // SQL fragments come exclusively from the closed static TABLES list;
        // all runtime values remain bind parameters.
        let rows = sqlx::query(AssertSqlSafe(query))
            .bind(run_id.as_str())
            .bind(table.kind.as_str())
            .fetch_all(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
        for row in rows {
            let subject_id: String = row
                .try_get("subject_id")
                .map_err(|_| RepositoryError::invalid_data())?;
            let projection_version = u64_from_i64(
                row.try_get("projection_version")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let encoded: String = row
                .try_get("canonical_projection")
                .map_err(|_| RepositoryError::invalid_data())?;
            let value =
                serde_json::from_str(&encoded).map_err(|_| RepositoryError::invalid_data())?;
            records.push(projection_adapter::checkpoint_record(
                ProjectionSubject::new(table.kind, subject_id)?,
                projection_version,
                value,
            )?);
        }
    }
    projection_adapter::sort_checkpoint_records(&mut records)?;
    Ok(records)
}

pub(crate) async fn finalize_projection_checkpoints(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    event_id: &str,
) -> Result<(), RepositoryError> {
    let records = changed_records(transaction, run_id).await?;
    insert_checkpoint_batch(transaction, run_id, event_id, &records).await
}

pub(crate) async fn finalize_empty_projection_checkpoints(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    event_id: &str,
) -> Result<(), RepositoryError> {
    insert_checkpoint_batch(transaction, run_id, event_id, &[]).await
}

async fn insert_checkpoint_batch(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    event_id: &str,
    records: &[Value],
) -> Result<(), RepositoryError> {
    let manifest_hash = checkpoint_manifest_hash(records)?;
    let ledger_batch = projection_adapter::projection_ledger_batch(records)?;
    let encoded_ledger_batch = canonical_json(&ledger_batch)?;
    let updated = sqlx::query(
        "UPDATE execution_events SET projection_ledger_batch=?
         WHERE run_id=? AND event_id=? AND projection_ledger_batch IS NULL",
    )
    .bind(encoded_ledger_batch)
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
         ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(event_id)
    .bind(i64::from(
        projection_adapter::projection_checkpoint_schema_version(),
    ))
    .bind(i64::try_from(records.len()).map_err(|_| RepositoryError::invalid_data())?)
    .bind(manifest_hash)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for record in records {
        let (subject, projection_version, canonical_projection, projection_hash) =
            projection_adapter::checkpoint_record_parts(record)?;
        sqlx::query(
            "INSERT INTO projection_checkpoints (
                run_id,event_id,subject_kind,subject_id,checkpoint_schema_version,
                subject_projection_version,projection_hash,canonical_projection,created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
        )
        .bind(run_id.as_str())
        .bind(event_id)
        .bind(subject.kind().as_str())
        .bind(subject.subject_id())
        .bind(i64::from(
            projection_adapter::projection_checkpoint_schema_version(),
        ))
        .bind(i64_from_u64(projection_version)?)
        .bind(projection_hash)
        .bind(canonical_json(&canonical_projection)?)
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    }
    Ok(())
}

pub(crate) async fn verify_projection_checkpoint_batch(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    event_id: &str,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        "SELECT schema_version,projection_ledger_batch FROM execution_events
         WHERE run_id=? AND event_id=?",
    )
    .bind(run_id.as_str())
    .bind(event_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    decode_execution_event_schema_version(
        row.try_get::<i64, _>("schema_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let encoded_ledger = row
        .try_get::<String, _>("projection_ledger_batch")
        .map_err(|_| RepositoryError::invalid_data())?;
    let ledger =
        serde_json::from_str(&encoded_ledger).map_err(|_| RepositoryError::invalid_data())?;
    projection_adapter::validate_projection_ledger_batch(&ledger)?;
    Ok(())
}

pub(crate) async fn append_projection_mutation_event(
    transaction: &mut Transaction<'_, Sqlite>,
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

async fn current_projection_value(
    pool: &SqlitePool,
    run_id: &RunId,
    subject: &ProjectionSubject,
) -> Result<Option<Value>, RepositoryError> {
    let table = table_for(subject)?;
    let query = format!(
        "SELECT {value} AS canonical_projection FROM {table_name} p
         WHERE p.run_id = ? AND {id} = ?",
        value = table.value_expression,
        table_name = table.table,
        id = table.id_expression,
    );
    // SQL fragments come exclusively from the closed static TABLES list;
    // all runtime values remain bind parameters.
    let encoded = sqlx::query_scalar::<_, String>(AssertSqlSafe(query))
        .bind(run_id.as_str())
        .bind(subject.subject_id())
        .fetch_optional(pool)
        .await
        .map_err(RepositoryError::storage)?;
    encoded
        .map(|encoded| serde_json::from_str(&encoded).map_err(|_| RepositoryError::invalid_data()))
        .transpose()
}

async fn current_projection_value_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    subject: &ProjectionSubject,
) -> Result<Option<Value>, RepositoryError> {
    let table = table_for(subject)?;
    let query = format!(
        "SELECT {value} AS canonical_projection FROM {table_name} p
         WHERE p.run_id = ? AND {id} = ?",
        value = table.value_expression,
        table_name = table.table,
        id = table.id_expression,
    );
    let encoded = sqlx::query_scalar::<_, String>(AssertSqlSafe(query))
        .bind(run_id.as_str())
        .bind(subject.subject_id())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    encoded
        .map(|encoded| serde_json::from_str(&encoded).map_err(|_| RepositoryError::invalid_data()))
        .transpose()
}

async fn authoritative_snapshot(
    pool: &SqlitePool,
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
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    subject: &ProjectionSubject,
) -> Result<Option<ProjectionRebuildSnapshot>, RepositoryError> {
    ensure_run_execution_event_schemas(transaction, run_id).await?;
    let rows = sqlx::query(
        "SELECT schema_version,event_id,seq,projection_ledger_batch
         FROM execution_events
         WHERE run_id=? AND projection_ledger_batch IS NOT NULL
         ORDER BY seq DESC",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let mut snapshot = None;
    for row in rows {
        decode_execution_event_schema_version(
            row.try_get::<i64, _>("schema_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let encoded: String = row
            .try_get("projection_ledger_batch")
            .map_err(|_| RepositoryError::invalid_data())?;
        let batch = serde_json::from_str(&encoded).map_err(|_| RepositoryError::invalid_data())?;
        let Some(record) = projection_adapter::projection_ledger_record_for(&batch, subject)?
        else {
            continue;
        };
        let (_, projection_version, canonical_projection, projection_hash) =
            projection_adapter::checkpoint_record_parts(&record)?;
        snapshot = Some(projection_adapter::projection_rebuild_snapshot(
            subject.clone(),
            row.try_get("event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            u64_from_i64(
                row.try_get("seq")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
            projection_version,
            projection_hash,
            canonical_projection,
        ));
        break;
    }
    Ok(snapshot)
}

async fn authoritative_snapshots_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
) -> Result<Vec<ProjectionRebuildSnapshot>, RepositoryError> {
    ensure_run_execution_event_schemas(transaction, run_id).await?;
    let rows = sqlx::query(
        "SELECT schema_version,event_id,seq,projection_ledger_batch FROM execution_events
         WHERE run_id=? AND projection_ledger_batch IS NOT NULL ORDER BY seq",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let mut latest = BTreeMap::new();
    for row in rows {
        decode_execution_event_schema_version(
            row.try_get::<i64, _>("schema_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let event_id: String = row
            .try_get("event_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let event_seq = u64_from_i64(
            row.try_get("seq")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let encoded: String = row
            .try_get("projection_ledger_batch")
            .map_err(|_| RepositoryError::invalid_data())?;
        let batch = serde_json::from_str(&encoded).map_err(|_| RepositoryError::invalid_data())?;
        for record in projection_adapter::validate_projection_ledger_batch(&batch)? {
            let (subject, projection_version, canonical_projection, projection_hash) =
                projection_adapter::checkpoint_record_parts(&record)?;
            latest.insert(
                subject.clone(),
                projection_adapter::projection_rebuild_snapshot(
                    subject,
                    event_id.clone(),
                    event_seq,
                    projection_version,
                    projection_hash,
                    canonical_projection,
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

fn json_string(value: &Value, key: &str) -> Result<String, RepositoryError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(RepositoryError::invalid_data)
}

fn json_optional_string(value: &Value, key: &str) -> Result<Option<String>, RepositoryError> {
    match value.get(key) {
        Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => Err(RepositoryError::invalid_data()),
    }
}

fn json_i64(value: &Value, key: &str) -> Result<i64, RepositoryError> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(RepositoryError::invalid_data)
}

async fn repair_activation(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    subject: &ProjectionSubject,
    value: &Value,
) -> Result<(), RepositoryError> {
    let rows = sqlx::query(
        "UPDATE node_activations SET
            scope_instance_id=?,node_id=?,stable_activation_key=?,execution_kind=?,
            lifecycle=?,effect_id=?,effect_idempotency=?,effect_evidence=?,
            last_attempt_no=?,last_lease_epoch=?,current_attempt_no=?,current_lease_epoch=?,
            current_fencing_token=?,retry_budget_remaining=?,pending_retry_timer_id=?,
            wait_registration_transition_key=?,termination_intent_reason=?,
            termination_intent_transition_key=?,termination_intent_at=?,output_payload_id=?,
            output_artifact_id=?,output_value_hash=?,winning_attempt_no=?,reused_from_run_id=?,
            reused_from_activation_id=?,projection_version=?,created_at=?,updated_at=?,terminal_at=?
         WHERE run_id=? AND activation_id=?",
    )
    .bind(json_string(value, "scope_instance_id")?)
    .bind(json_string(value, "node_id")?)
    .bind(json_string(value, "stable_activation_key")?)
    .bind(json_string(value, "execution_kind")?)
    .bind(json_string(value, "lifecycle")?)
    .bind(json_string(value, "effect_id")?)
    .bind(json_string(value, "effect_idempotency")?)
    .bind(json_string(value, "effect_evidence")?)
    .bind(value.get("last_attempt_no").and_then(Value::as_i64))
    .bind(value.get("last_lease_epoch").and_then(Value::as_i64))
    .bind(value.get("current_attempt_no").and_then(Value::as_i64))
    .bind(value.get("current_lease_epoch").and_then(Value::as_i64))
    .bind(json_optional_string(value, "current_fencing_token")?)
    .bind(json_i64(value, "retry_budget_remaining")?)
    .bind(json_optional_string(value, "pending_retry_timer_id")?)
    .bind(json_optional_string(
        value,
        "wait_registration_transition_key",
    )?)
    .bind(json_optional_string(value, "termination_intent_reason")?)
    .bind(json_optional_string(
        value,
        "termination_intent_transition_key",
    )?)
    .bind(json_optional_string(value, "termination_intent_at")?)
    .bind(json_optional_string(value, "output_payload_id")?)
    .bind(json_optional_string(value, "output_artifact_id")?)
    .bind(json_optional_string(value, "output_value_hash")?)
    .bind(value.get("winning_attempt_no").and_then(Value::as_i64))
    .bind(json_optional_string(value, "reused_from_run_id")?)
    .bind(json_optional_string(value, "reused_from_activation_id")?)
    .bind(json_i64(value, "projection_version")?)
    .bind(json_string(value, "created_at")?)
    .bind(json_string(value, "updated_at")?)
    .bind(json_optional_string(value, "terminal_at")?)
    .bind(run_id.as_str())
    .bind(subject.subject_id())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        return Err(RepositoryError::activation_not_found());
    }
    Ok(())
}

async fn repair_run(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    value: &Value,
) -> Result<(), RepositoryError> {
    let encoded = canonical_json(value)?;
    let rows = sqlx::query(
        "UPDATE workflow_runs SET
            definition_id=json_extract(?1,'$.definition_id'),
            definition_revision_id=json_extract(?1,'$.definition_revision_id'),
            deployment_revision_id=json_extract(?1,'$.deployment_revision_id'),
            plan_hash=json_extract(?1,'$.plan_hash'),binding_hash=json_extract(?1,'$.binding_hash'),
            request_id=json_extract(?1,'$.request_id'),attachment=json_extract(?1,'$.attachment'),
            lifecycle=json_extract(?1,'$.lifecycle'),admission_state=json_extract(?1,'$.admission_state'),
            termination_intent_reason=json_extract(?1,'$.termination_intent_reason'),
            termination_intent_transition_key=json_extract(?1,'$.termination_intent_transition_key'),
            termination_intent_at=json_extract(?1,'$.termination_intent_at'),
            input_payload_id=json_extract(?1,'$.input_payload_id'),
            output_payload_id=json_extract(?1,'$.output_payload_id'),
            output_artifact_id=json_extract(?1,'$.output_artifact_id'),
            output_value_hash=json_extract(?1,'$.output_value_hash'),error_code=json_extract(?1,'$.error_code'),
            terminal_event_id=json_extract(?1,'$.terminal_event_id'),
            terminal_public_event_id=json_extract(?1,'$.terminal_public_event_id'),
            parent_run_id=json_extract(?1,'$.parent_run_id'),lineage_kind=json_extract(?1,'$.lineage_kind'),
            generation=json_extract(?1,'$.generation'),replacement_run_id=json_extract(?1,'$.replacement_run_id'),
            projection_version=json_extract(?1,'$.projection_version'),created_at=json_extract(?1,'$.created_at'),
            started_at=json_extract(?1,'$.started_at'),deadline_at=json_extract(?1,'$.deadline_at'),
            updated_at=json_extract(?1,'$.updated_at'),terminal_at=json_extract(?1,'$.terminal_at')
         WHERE run_id=?2",
    )
    .bind(encoded)
    .bind(run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    if matches!(
        json_string(value, "lifecycle")?.as_str(),
        "succeeded" | "failed" | "cancelled" | "interrupted" | "timed_out"
    ) {
        super::sqlite_model_tool_queue::close_model_tool_work_for_terminal_run_sqlite(
            transaction,
            run_id,
        )
        .await?;
    }
    Ok(())
}

async fn repair_scope(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    subject: &ProjectionSubject,
    value: &Value,
) -> Result<(), RepositoryError> {
    let encoded = canonical_json(value)?;
    let rows = sqlx::query(
        "UPDATE scope_instances SET
            parent_scope_instance_id=json_extract(?1,'$.parent_scope_instance_id'),
            static_scope_id=json_extract(?1,'$.static_scope_id'),
            stable_dynamic_key=json_extract(?1,'$.stable_dynamic_key'),
            scope_kind=json_extract(?1,'$.scope_kind'),is_root=json_extract(?1,'$.is_root'),
            lifecycle=json_extract(?1,'$.lifecycle'),admission_state=json_extract(?1,'$.admission_state'),
            admitted_children=json_extract(?1,'$.admitted_children'),
            settled_children=json_extract(?1,'$.settled_children'),
            projection_version=json_extract(?1,'$.projection_version'),
            created_at=json_extract(?1,'$.created_at'),settled_at=json_extract(?1,'$.settled_at')
         WHERE run_id=?2 AND scope_instance_id=?3",
    )
    .bind(encoded)
    .bind(run_id.as_str())
    .bind(subject.subject_id())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn repair_attempt(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    subject: &ProjectionSubject,
    value: &Value,
) -> Result<(), RepositoryError> {
    let (activation_id, attempt_no) = parse_attempt_subject_id(subject.subject_id())?;
    let encoded = canonical_json(value)?;
    let rows = sqlx::query(
        "UPDATE node_attempts SET
            lease_epoch=json_extract(?1,'$.lease_epoch'),fencing_token=json_extract(?1,'$.fencing_token'),
            effect_id=json_extract(?1,'$.effect_id'),lifecycle=json_extract(?1,'$.lifecycle'),
            effect_evidence=json_extract(?1,'$.effect_evidence'),worker_id=json_extract(?1,'$.worker_id'),
            lease_expires_at=json_extract(?1,'$.lease_expires_at'),heartbeat_at=json_extract(?1,'$.heartbeat_at'),
            output_payload_id=json_extract(?1,'$.output_payload_id'),
            output_artifact_id=json_extract(?1,'$.output_artifact_id'),
            output_value_hash=json_extract(?1,'$.output_value_hash'),failure_code=json_extract(?1,'$.failure_code'),
            completion_transition_key=json_extract(?1,'$.completion_transition_key'),
            terminal_event_id=json_extract(?1,'$.terminal_event_id'),
            projection_version=json_extract(?1,'$.projection_version'),created_at=json_extract(?1,'$.created_at'),
            started_at=json_extract(?1,'$.started_at'),terminal_at=json_extract(?1,'$.terminal_at')
         WHERE run_id=?2 AND activation_id=?3 AND attempt_no=?4",
    )
    .bind(encoded)
    .bind(run_id.as_str())
    .bind(activation_id)
    .bind(i64::from(attempt_no))
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn repair_timer(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    subject: &ProjectionSubject,
    value: &Value,
) -> Result<(), RepositoryError> {
    let encoded = canonical_json(value)?;
    let rows = sqlx::query(
        "UPDATE timers SET
            activation_id=json_extract(?1,'$.activation_id'),timer_kind=json_extract(?1,'$.timer_kind'),
            timer_state=json_extract(?1,'$.timer_state'),deadline_at=json_extract(?1,'$.deadline_at'),
            expected_attempt_no=json_extract(?1,'$.expected_attempt_no'),
            expected_lease_epoch=json_extract(?1,'$.expected_lease_epoch'),
            expected_fencing_token=json_extract(?1,'$.expected_fencing_token'),
            retry_budget_snapshot=json_extract(?1,'$.retry_budget_snapshot'),
            created_by_transition_key=json_extract(?1,'$.created_by_transition_key'),
            fired_by_transition_key=json_extract(?1,'$.fired_by_transition_key'),
            fired_event_id=json_extract(?1,'$.fired_event_id'),
            projection_version=json_extract(?1,'$.projection_version'),
            created_at=json_extract(?1,'$.created_at'),fired_at=json_extract(?1,'$.fired_at')
         WHERE run_id=?2 AND timer_id=?3",
    )
    .bind(encoded)
    .bind(run_id.as_str())
    .bind(subject.subject_id())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn repair_registered_subject(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    subject: &ProjectionSubject,
    value: &Value,
) -> Result<(), RepositoryError> {
    let table = table_for(subject)?;
    let columns = repair_columns(table)?;
    let assignments = columns
        .iter()
        .map(|column| format!("{column}={}", sqlite_projection_value(table.table, column)))
        .collect::<Vec<_>>()
        .join(",");
    let id_expression = table.id_expression.replace("p.", "t.");
    let query = format!(
        "UPDATE {table_name} AS t SET {assignments}
         WHERE t.run_id=?2 AND {id_expression}=?3",
        table_name = table.table,
    );
    // Every SQL identifier and expression is selected from the closed static
    // registry above. The checkpoint and subject remain ordinary bind values.
    let encoded = canonical_json(value)?;
    let rows = sqlx::query(AssertSqlSafe(query))
        .bind(&encoded)
        .bind(run_id.as_str())
        .bind(subject.subject_id())
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
    if rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn insert_missing_subject(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    subject: &ProjectionSubject,
    value: &Value,
) -> Result<(), RepositoryError> {
    let table = table_for(subject)?;
    let identity_columns = identity_columns(table)?;
    let projection_columns = repair_columns(table)?;
    let identity = canonical_json(&repair_identity(table, run_id, subject)?)?;
    let projection = canonical_json(value)?;
    let names = identity_columns
        .iter()
        .chain(projection_columns.iter())
        .copied()
        .collect::<Vec<_>>()
        .join(",");
    let identity_values = identity_columns
        .iter()
        .map(|column| format!("json_extract(?2,'$.{column}')"));
    let projection_values = projection_columns
        .iter()
        .map(|column| sqlite_projection_value(table.table, column));
    let values = identity_values
        .chain(projection_values)
        .collect::<Vec<_>>()
        .join(",");
    let query = format!(
        "INSERT INTO {table_name} ({names}) VALUES ({values}) ON CONFLICT DO NOTHING",
        table_name = table.table,
    );
    sqlx::query(AssertSqlSafe(query))
        .bind(projection)
        .bind(identity)
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    if table.table == "workflow_runs" {
        sqlx::query(
            "UPDATE workflow_runs SET next_event_seq=(
                 SELECT COALESCE(MAX(seq),0)+1 FROM execution_events WHERE run_id=?
             ) WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .bind(run_id.as_str())
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    }
    Ok(())
}

async fn repair_snapshot_tx(
    transaction: &mut Transaction<'_, Sqlite>,
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
        return Ok(projection_adapter::projection_repair_receipt(
            false,
            authoritative,
        ));
    }
    if current.is_none() {
        insert_missing_subject(
            transaction,
            run_id,
            subject,
            authoritative.canonical_projection(),
        )
        .await?;
    } else {
        match subject.kind() {
            ProjectionSubjectKind::Run => {
                repair_run(transaction, run_id, authoritative.canonical_projection()).await?
            }
            ProjectionSubjectKind::Scope => {
                repair_scope(
                    transaction,
                    run_id,
                    subject,
                    authoritative.canonical_projection(),
                )
                .await?
            }
            ProjectionSubjectKind::Activation => {
                repair_activation(
                    transaction,
                    run_id,
                    subject,
                    authoritative.canonical_projection(),
                )
                .await?
            }
            ProjectionSubjectKind::Attempt => {
                repair_attempt(
                    transaction,
                    run_id,
                    subject,
                    authoritative.canonical_projection(),
                )
                .await?
            }
            ProjectionSubjectKind::Timer => {
                repair_timer(
                    transaction,
                    run_id,
                    subject,
                    authoritative.canonical_projection(),
                )
                .await?
            }
            ProjectionSubjectKind::Control
            | ProjectionSubjectKind::Fork
            | ProjectionSubjectKind::Join
            | ProjectionSubjectKind::Scheduler
            | ProjectionSubjectKind::DataValue => {
                repair_registered_subject(
                    transaction,
                    run_id,
                    subject,
                    authoritative.canonical_projection(),
                )
                .await?
            }
        }
    }
    let repaired = current_projection_value_tx(transaction, run_id, subject)
        .await?
        .ok_or_else(RepositoryError::invalid_data)?;
    let (_, repaired_hash) = canonical_value(&repaired)?;
    if repaired_hash.as_str() != authoritative.projection_hash() {
        return Err(RepositoryError::invalid_data());
    }
    Ok(projection_adapter::projection_repair_receipt(
        true,
        authoritative,
    ))
}

#[async_trait]
impl ProjectionDurableRepository for SqliteDurableRepository {
    async fn audit_projection(
        &self,
        run_id: &RunId,
        subject: &ProjectionSubject,
    ) -> Result<ProjectionAudit, RepositoryError> {
        let authoritative = authoritative_snapshot(&self.pool, run_id, subject)
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        let current = current_projection_value(&self.pool, run_id, subject).await?;
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
        authoritative_snapshot(&self.pool, run_id, subject).await
    }

    async fn repair_projection(
        &self,
        run_id: &RunId,
        subject: &ProjectionSubject,
    ) -> Result<ProjectionRepairReceipt, RepositoryError> {
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
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
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
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

#[allow(dead_code)]
fn _attempt_key_contract(activation_id: &str, attempt_no: u32) -> Result<String, RepositoryError> {
    let key = attempt_subject_id(activation_id, attempt_no);
    let _ = parse_attempt_subject_id(&key)?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn scope_snapshot(id: &str, parent: Option<&str>) -> ProjectionRebuildSnapshot {
        projection_adapter::projection_rebuild_snapshot(
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
