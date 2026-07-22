use super::RepositoryErrorExt as _;

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Row, Sqlite, Transaction};
use uuid::Uuid;

use insight_durable::activation::adapter::{
    effect_evidence_str, effect_idempotency_str, parse_effect_evidence,
};
use insight_durable::common::adapter::{
    self as common_contract_adapter, canonical_intent_hash, canonical_json, canonical_value,
    decode_execution_event_schema_version, durable_public_event_envelope, event_id, fencing_token,
    function_call_response_item_id, i64_from_u64, payload_id, prepare_model_call_completion,
    public_event_id, public_event_ordinal, response_item_id, scheduler_checkpoint_content_hash,
    u64_from_i64, validate_incomplete_function_call_item, validate_inline_payload,
    ValidatedInlinePayload,
};
use insight_durable::control_repository::adapter::{
    decode_durable_reuse_provenance, reuse_matches_admission_contract,
};
use insight_durable::model::adapter::termination_reason_as_str;
use insight_durable::model_tool_parent_resume::adapter::*;
use insight_durable::model_tool_queue::adapter::*;
use insight_durable::retrieval_publication::adapter as retrieval_publication_adapter;
use insight_durable::scheduler_repository::adapter::*;
use insight_engine::scheduler_adapter;

use insight_engine::scheduler::{TaskFailureFact, TaskOutcomeFact};
use insight_engine::worker::{
    ModelCallAuthority, ModelCallCompletion, ModelContinuationTurn, ModelToolCall,
    ModelToolCallBatch, ModelToolResult, ResponseItemAuthority,
};
use insight_engine::{
    plan::{DataPortId, DescriptorValue, Plan, PlanType},
    ActivationId, ArtifactRef, AttemptNo, ContentHash, EffectEvidence, EffectId,
    ExecutionControlFrame, ExecutionEventContext, ExecutionEventPayload, ExecutionValueSummary,
    LeaseEpoch, PendingExecutionEvent, PlannedSchedulerAction, ProjectionMutationKind,
    PublicEventPayload, RunId, RunLifecycle, RunTerminalFact, RuntimeValue, SchedulerAction,
    SchedulerCheckpointId, SchedulerFacts, SchedulerIntent, SchedulerTaskId, TerminationReason,
    TransitionKey, TransitionOutcome, ValueRef,
};

use super::model_tool_parent_resume::{
    classify_parent_task_claim, latest_continuation_status, latest_execution_status,
    latest_fencing_token, latest_is_checkpointed, latest_is_ready, latest_is_waiting_tools,
    latest_lease_epoch, latest_model_call_no, latest_parent_model_call_view, latest_task_id,
    LatestParentModelCallView,
};
use super::scheduler_repository::{
    DurableTaskExecutionRequest, ModelToolCallCheckpoint, SchedulerCommitReceipt,
    SchedulerDurableRepository, SchedulerFailureDisposition, SchedulerStoredValue,
    SchedulerTaskClaim, SchedulerTaskClaimMode, SchedulerTaskCommitOutcome,
    SchedulerTaskCompletionReceipt, SchedulerTaskHeartbeatOutcome, SchedulerTaskOutcome,
    SchedulerTaskSuccess, SCHEDULER_CHECKPOINT_SCHEMA_VERSION,
    SCHEDULER_TASK_ENVELOPE_SCHEMA_VERSION,
};
use super::sqlite::{
    allocate_event_seq, decode_execution_event_row, insert_event, insert_or_get_payload,
    load_replay, parse_run_timestamp, Replay, SqliteDurableRepository,
};
use super::sqlite_model_tool_queue::{
    activate_model_tool_call_batch_sqlite, claim_model_tool_calls_sqlite,
    commit_model_tool_call_outcome_sqlite, heartbeat_model_tool_call_sqlite,
    mark_model_tool_call_started_sqlite,
};
use super::sqlite_projection::{append_projection_mutation_event, finalize_projection_checkpoints};
use super::{FencedSchedulerRunCommand, RepositoryError};
use super::{
    ModelToolBatchActivationOutcome, ModelToolParentResume, ModelToolTaskClaim,
    ModelToolTaskCommitReceipt, ModelToolTaskHeartbeatOutcome, ModelToolTaskOutcome,
    ModelToolTaskTransitionOutcome,
};
use insight_engine::response::WorkflowToolPublicProjection;

const MAX_CLAIM_SECONDS: u32 = 3_600;
const MAX_CLAIM_LIMIT: u32 = 1_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskCompletionFact {
    task_id: SchedulerTaskId,
    occurrence: insight_engine::LogicalOccurrence,
    outcome: TaskOutcomeFact,
    output_receipts: BTreeMap<DataPortId, TaskOutputReceipt>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskOutputReceipt {
    owner_activation_id: ActivationId,
    occurrence: insight_engine::LogicalOccurrence,
    runtime_value: RuntimeValue,
    declared_type: PlanType,
    content_hash: ContentHash,
    canonical_value_ref: ValueRef,
    canonical_storage_kind: String,
    canonical_payload_id: Option<String>,
    canonical_artifact_id: Option<String>,
    canonical_projection_version: u64,
    occurrence_value_ref: ValueRef,
    occurrence_storage_kind: String,
    occurrence_payload_id: Option<String>,
    occurrence_artifact_id: Option<String>,
    occurrence_projection_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskRetryFact {
    task_id: SchedulerTaskId,
    activation_id: ActivationId,
    attempt_no: AttemptNo,
    lease_epoch: LeaseEpoch,
    fencing_token: String,
    failure: TaskFailureFact,
    effect_evidence: EffectEvidence,
    retry_at: DateTime<Utc>,
    remaining_attempts: u32,
    next_attempt_no: AttemptNo,
    next_lease_epoch: LeaseEpoch,
    next_fencing_token: String,
    next_envelope: DurableTaskExecutionRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskStartedFact {
    task_id: SchedulerTaskId,
    activation_id: ActivationId,
    attempt_no: AttemptNo,
    lease_epoch: LeaseEpoch,
    fencing_token: String,
    claimed_by: String,
    claim_token: String,
}

struct ValidatedPlannedActionCheckpoint {
    transition_key: TransitionKey,
    event_id: String,
    intent: SchedulerIntent,
}

fn model_data<T>(value: Result<T, insight_engine::ModelError>) -> Result<T, RepositoryError> {
    value.map_err(|_| RepositoryError::invalid_data())
}

fn retry_envelope_is_consistent(
    retry: &TaskRetryFact,
    run_id: &RunId,
    scheduler_projection_version: u64,
) -> bool {
    retry.next_envelope.request().run_id() == run_id
        && retry.next_envelope.request().task_id() == &retry.task_id
        && retry.next_envelope.request().activation_id() == &retry.activation_id
        && retry.next_envelope.attempt_no() == retry.next_attempt_no
        && retry.next_envelope.lease_epoch() == retry.next_lease_epoch
        && retry.next_envelope.fencing_token() == retry.next_fencing_token.as_str()
        && retry.next_envelope.dispatch_scheduler_projection_version()
            == scheduler_projection_version
        && retry.remaining_attempts
            == retry
                .next_envelope
                .request()
                .effect_policy()
                .max_attempts()
                .saturating_sub(retry.attempt_no.get())
}

fn scheduler_data<T>(
    value: Result<T, insight_engine::SchedulerError>,
) -> Result<T, RepositoryError> {
    value.map_err(|_| RepositoryError::invalid_data())
}

fn now_text(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn scheduler_checkpoint_for_task(task_id: &SchedulerTaskId) -> SchedulerCheckpointId {
    let digest = ContentHash::from_bytes(
        format!(
            "insight-agent/scheduler/task-completed/v1/{}",
            task_id.as_str()
        )
        .as_bytes(),
    );
    SchedulerCheckpointId::parse(format!(
        "checkpoint_{}",
        digest
            .as_str()
            .strip_prefix("sha256:")
            .unwrap_or(digest.as_str())
    ))
    .expect("a SHA-256 scheduler checkpoint is valid")
}

fn operation_checkpoint(transition_key: &TransitionKey) -> SchedulerCheckpointId {
    let digest = transition_key
        .as_str()
        .strip_prefix("transition_")
        .unwrap_or(transition_key.as_str());
    SchedulerCheckpointId::parse(format!("checkpoint_{digest}"))
        .expect("a transition digest is a valid scheduler checkpoint token")
}

fn task_outcome_transition_key(
    claim: &SchedulerTaskClaim,
) -> Result<TransitionKey, RepositoryError> {
    let attempt_no = claim.envelope().attempt_no().get().to_string();
    let lease_epoch = claim.envelope().lease_epoch().get().to_string();
    TransitionKey::derive(
        "scheduler.task.outcome.v1",
        &[
            claim.run_id().as_str(),
            claim.task_id().as_str(),
            &attempt_no,
            &lease_epoch,
            claim.envelope().fencing_token(),
        ],
    )
    .map_err(|_| RepositoryError::invalid_data())
}

fn effect_id_for_activation(activation_id: &ActivationId) -> Result<EffectId, RepositoryError> {
    model_data(EffectId::new(format!("effect_{}", activation_id.as_str())))
}

fn output_summary(value: &Value) -> Result<ExecutionValueSummary, RepositoryError> {
    let encoded = canonical_json(value)?;
    Ok(ExecutionValueSummary::new(
        ContentHash::from_bytes(encoded.as_bytes()),
        u64::try_from(encoded.len()).map_err(|_| RepositoryError::invalid_data())?,
    ))
}

fn operation_elapsed_ms(
    started_at: Option<DateTime<Utc>>,
    occurred_at: DateTime<Utc>,
) -> Result<u64, RepositoryError> {
    let elapsed = started_at
        .map(|started_at| {
            occurred_at
                .signed_duration_since(started_at)
                .num_milliseconds()
        })
        .unwrap_or(0)
        .max(0);
    u64::try_from(elapsed).map_err(|_| RepositoryError::invalid_data())
}

fn public_operation_failure(
    failure: &super::SchedulerTaskFailure,
) -> Result<insight_engine::PublicFailureSummary, RepositoryError> {
    let (kind, code) = match failure.disposition() {
        SchedulerFailureDisposition::TimedOut => (
            insight_engine::PublicFailureKind::Timeout,
            "OPERATION_TIMEOUT",
        ),
        SchedulerFailureDisposition::Retry { .. } | SchedulerFailureDisposition::Terminal => {
            match failure.class() {
                insight_engine::WorkerFailureClass::InfrastructureFailure
                | insight_engine::WorkerFailureClass::InvariantCorruption => (
                    insight_engine::PublicFailureKind::Infrastructure,
                    "OPERATION_FAILED",
                ),
                insight_engine::WorkerFailureClass::ControlTermination => {
                    (insight_engine::PublicFailureKind::Stop, "OPERATION_STOPPED")
                }
                insight_engine::WorkerFailureClass::SafeBusinessFailure
                | insight_engine::WorkerFailureClass::EffectOutcomeUnknown => (
                    insight_engine::PublicFailureKind::Operation,
                    "OPERATION_FAILED",
                ),
            }
        }
    };
    Ok(insight_engine::PublicFailureSummary {
        kind,
        code: model_data(insight_engine::PublicErrorCode::new(code))?,
    })
}

#[allow(clippy::too_many_arguments)]
async fn insert_public_operation(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    transition_key: &TransitionKey,
    event_id_value: &str,
    event_seq: u64,
    occurred_at: DateTime<Utc>,
    payload: PublicEventPayload,
) -> Result<(), RepositoryError> {
    let kind = payload.kind();
    let public_id = public_event_id(run_id, transition_key, kind);
    let envelope = durable_public_event_envelope(
        run_id,
        &public_id,
        event_id_value,
        event_seq,
        occurred_at,
        payload,
    )?;
    let envelope = canonical_json(
        &serde_json::to_value(envelope).map_err(|_| RepositoryError::canonicalization())?,
    )?;
    sqlx::query(
        "INSERT INTO public_event_outbox (
            run_id,public_event_id,causation_event_id,public_ordinal,public_schema_version,event_kind,
            is_terminal,publish_state,safe_envelope,available_at,claimed_by,claim_token,
            claim_expires_at,publish_attempts,published_at,published_by,published_claim_token,
            notified_at,retain_until,created_at
         ) VALUES (?,?,?,?,1,?,0,'pending',?,CURRENT_TIMESTAMP,NULL,NULL,NULL,0,
                   NULL,NULL,NULL,NULL,NULL,CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(public_id)
    .bind(event_id_value)
    .bind(i64::from(public_event_ordinal(kind)))
    .bind(kind.as_str())
    .bind(envelope)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

async fn activation_identity(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    activation_id: &ActivationId,
) -> Result<(insight_engine::ScopeInstanceId, insight_engine::NodeId), RepositoryError> {
    let row = sqlx::query(
        "SELECT scope_instance_id,node_id FROM node_activations
         WHERE run_id=? AND activation_id=?",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    Ok((
        model_data(insight_engine::ScopeInstanceId::new(
            row.try_get::<String, _>("scope_instance_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?,
        model_data(insight_engine::NodeId::new(
            row.try_get::<String, _>("node_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?,
    ))
}

async fn activation_occurrence(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    activation_id: &ActivationId,
) -> Result<insight_engine::LogicalOccurrence, RepositoryError> {
    let encoded = sqlx::query_scalar::<_, String>(
        "SELECT stable_activation_key FROM node_activations
         WHERE run_id=? AND activation_id=?",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    serde_json::from_str(&encoded).map_err(|_| RepositoryError::invalid_data())
}

#[allow(clippy::too_many_arguments)]
async fn create_dynamic_child(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    owner_activation_id: &ActivationId,
    child_scope_instance_id: &insight_engine::ScopeInstanceId,
    static_scope_id: &insight_engine::plan::ScopeId,
    scope_kind: &str,
    stable_dynamic_key: &str,
    child_node_id: &insight_engine::NodeId,
    child_activation_id: &ActivationId,
    occurrence: &insight_engine::LogicalOccurrence,
    token_id: &insight_engine::ControlTokenId,
    output_port: &insight_engine::plan::ControlPortId,
    transition_key: &TransitionKey,
) -> Result<(), RepositoryError> {
    let (parent_scope_instance_id, _) =
        activation_identity(transaction, run_id, owner_activation_id).await?;
    let parent_rows = sqlx::query(
        "UPDATE scope_instances SET admitted_children=admitted_children+1,
            projection_version=projection_version+1
         WHERE run_id=? AND scope_instance_id=? AND lifecycle='active'
           AND admission_state='open'",
    )
    .bind(run_id.as_str())
    .bind(parent_scope_instance_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if parent_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    let scope_rows = sqlx::query(
        "INSERT INTO scope_instances (
            run_id,scope_instance_id,parent_scope_instance_id,static_scope_id,
            stable_dynamic_key,scope_kind,is_root,lifecycle,admission_state,
            admitted_children,settled_children,projection_version,created_at,settled_at
         ) VALUES (?,?,?,?,?,?,0,'active','open',0,0,0,CURRENT_TIMESTAMP,NULL)",
    )
    .bind(run_id.as_str())
    .bind(child_scope_instance_id.as_str())
    .bind(parent_scope_instance_id.as_str())
    .bind(static_scope_id.as_str())
    .bind(stable_dynamic_key)
    .bind(scope_kind)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if scope_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }

    let stable_activation_key = canonical_json(
        &serde_json::to_value(occurrence).map_err(|_| RepositoryError::canonicalization())?,
    )?;
    let effect_id = effect_id_for_activation(child_activation_id)?;
    let activation_rows = sqlx::query(
        "INSERT INTO node_activations (
            run_id,activation_id,scope_instance_id,node_id,stable_activation_key,
            execution_kind,lifecycle,effect_id,effect_idempotency,effect_evidence,
            retry_budget_remaining,projection_version,created_at,updated_at
         ) VALUES (?,?,?,?,?,'scheduler_native','created',?,'idempotent',
                   'not_started',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(child_activation_id.as_str())
    .bind(child_scope_instance_id.as_str())
    .bind(child_node_id.as_str())
    .bind(stable_activation_key)
    .bind(effect_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if activation_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }

    sqlx::query(
        "INSERT INTO control_tokens (
            run_id,token_id,current_scope_instance_id,current_port_id,
            source_activation_id,source_port_id,emission_slot,
            emitted_by_transition_key,provenance_frames,branch_activation_id,
            selected_branch_port_id,fork_group_id,fork_leg_id,token_state,
            consumed_by_activation_id,consumed_by_transition_key,consumed_at,
            revoked_by_transition_key,revoked_at,projection_version,created_at
         ) VALUES (?,?,?,?,?,?,?,?, '[]',NULL,NULL,NULL,NULL,'available',
                   NULL,NULL,NULL,NULL,NULL,0,CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(token_id.as_str())
    .bind(child_scope_instance_id.as_str())
    .bind(output_port.as_str())
    .bind(owner_activation_id.as_str())
    .bind(output_port.as_str())
    .bind(token_id.as_str())
    .bind(transition_key.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

async fn settle_dynamic_scope(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    scope_instance_id: &insight_engine::ScopeInstanceId,
    cancelled: bool,
) -> Result<(), RepositoryError> {
    let scope = sqlx::query(
        "SELECT parent_scope_instance_id,scope_kind,is_root,lifecycle,admission_state,
                admitted_children,settled_children
         FROM scope_instances WHERE run_id=? AND scope_instance_id=?",
    )
    .bind(run_id.as_str())
    .bind(scope_instance_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let parent_scope_instance_id = scope
        .try_get::<Option<String>, _>("parent_scope_instance_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let scope_kind = scope
        .try_get::<String, _>("scope_kind")
        .map_err(|_| RepositoryError::invalid_data())?;
    let is_root = scope
        .try_get::<i64, _>("is_root")
        .map_err(|_| RepositoryError::invalid_data())?
        == 1;
    let lifecycle = scope
        .try_get::<String, _>("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?;
    let admission_state = scope
        .try_get::<String, _>("admission_state")
        .map_err(|_| RepositoryError::invalid_data())?;
    let admitted_children = scope
        .try_get::<i64, _>("admitted_children")
        .map_err(|_| RepositoryError::invalid_data())?;
    let settled_children = scope
        .try_get::<i64, _>("settled_children")
        .map_err(|_| RepositoryError::invalid_data())?;
    if lifecycle != "active"
        || admission_state != "open"
        || admitted_children < 0
        || admitted_children != settled_children
        || (is_root != parent_scope_instance_id.is_none())
    {
        return Err(RepositoryError::invalid_data());
    }

    let activation_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM node_activations WHERE run_id=? AND scope_instance_id=?",
    )
    .bind(run_id.as_str())
    .bind(scope_instance_id.as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let active_work = sqlx::query_scalar::<_, i64>(
        "SELECT
            (SELECT COUNT(*) FROM node_activations
              WHERE run_id=? AND scope_instance_id=?
                AND lifecycle IN ('created','ready','leased','running','retry_wait','waiting','terminating'))
          + (SELECT COUNT(*) FROM node_attempts
              WHERE run_id=? AND activation_id IN (
                SELECT activation_id FROM node_activations
                 WHERE run_id=? AND scope_instance_id=?)
                AND lifecycle IN ('created','leased','running'))
          + (SELECT COUNT(*) FROM task_outbox
              WHERE run_id=? AND activation_id IN (
                SELECT activation_id FROM node_activations
                 WHERE run_id=? AND scope_instance_id=?)
                AND task_state IN ('pending','claimed','published'))
          + (SELECT COUNT(*) FROM timers
              WHERE run_id=? AND activation_id IN (
                SELECT activation_id FROM node_activations
                 WHERE run_id=? AND scope_instance_id=?)
                AND timer_state='scheduled')
          + (SELECT COUNT(*) FROM scope_instances
              WHERE run_id=? AND parent_scope_instance_id=?
                AND lifecycle IN ('active','settling'))",
    )
    .bind(run_id.as_str())
    .bind(scope_instance_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(scope_instance_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(scope_instance_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(scope_instance_id.as_str())
    .bind(run_id.as_str())
    .bind(scope_instance_id.as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let empty_subflow_has_completion_proof =
        if activation_count == 0 && scope_kind == "subflow_invocation" {
            sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM scheduler_subflow_invocations
             WHERE run_id=? AND invocation_scope_instance_id=?
               AND invocation_state='completed' AND completed_at IS NOT NULL",
            )
            .bind(run_id.as_str())
            .bind(scope_instance_id.as_str())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .is_some()
        } else {
            false
        };
    if (activation_count == 0 && !empty_subflow_has_completion_proof) || active_work != 0 {
        return Err(RepositoryError::invalid_data());
    }

    let rows = sqlx::query(
        "UPDATE scope_instances SET lifecycle=?,admission_state='closed',
            projection_version=projection_version+1,settled_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND scope_instance_id=? AND lifecycle='active'
           AND admission_state='open' AND admitted_children=settled_children",
    )
    .bind(if cancelled { "cancelled" } else { "settled" })
    .bind(run_id.as_str())
    .bind(scope_instance_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    if let Some(parent_scope_instance_id) = parent_scope_instance_id {
        let parent_rows = sqlx::query(
            "UPDATE scope_instances SET settled_children=settled_children+1,
                projection_version=projection_version+1
             WHERE run_id=? AND scope_instance_id=?
               AND lifecycle IN ('active','settling')
               AND settled_children < admitted_children",
        )
        .bind(run_id.as_str())
        .bind(parent_scope_instance_id)
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if parent_rows != 1 {
            return Err(RepositoryError::invalid_data());
        }
    }
    Ok(())
}

fn structural_settlement_class(
    outcome: &insight_engine::scheduler::StructuralOutcomeFact,
) -> &'static str {
    match outcome {
        insight_engine::scheduler::StructuralOutcomeFact::Succeeded { .. } => "succeeded",
        insight_engine::scheduler::StructuralOutcomeFact::Failed { failure } => {
            match failure.class() {
                insight_engine::WorkerFailureClass::SafeBusinessFailure => "safe_failure",
                insight_engine::WorkerFailureClass::InfrastructureFailure
                | insight_engine::WorkerFailureClass::EffectOutcomeUnknown => {
                    "infrastructure_failure"
                }
                insight_engine::WorkerFailureClass::ControlTermination => {
                    if failure.code().contains("TIMEOUT") {
                        "timed_out"
                    } else {
                        "cancelled"
                    }
                }
                insight_engine::WorkerFailureClass::InvariantCorruption => "panic",
            }
        }
    }
}

async fn cancel_and_drain_scope(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    scope_instance_id: &insight_engine::ScopeInstanceId,
    transition_key: &TransitionKey,
    event_id_value: &str,
) -> Result<(), RepositoryError> {
    let now = now_text(Utc::now());
    sqlx::query(
        "UPDATE task_outbox SET task_state='dead',claimed_by=NULL,claim_token=NULL,
            claim_expires_at=NULL,claim_mode=NULL,last_error_code='SCOPE_CANCELLED',
            projection_version=projection_version+1
         WHERE run_id=? AND activation_id IN (
            SELECT activation_id FROM node_activations
             WHERE run_id=? AND scope_instance_id=?
         ) AND task_state IN ('pending','claimed','published')",
    )
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(scope_instance_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;

    let attempts = sqlx::query(
        "SELECT activation_id,attempt_no FROM node_attempts
         WHERE run_id=? AND activation_id IN (
            SELECT activation_id FROM node_activations
             WHERE run_id=? AND scope_instance_id=?
         ) AND lifecycle IN ('created','leased','running')",
    )
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(scope_instance_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for attempt in attempts {
        let activation_id: String = attempt
            .try_get("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let attempt_no: i64 = attempt
            .try_get("attempt_no")
            .map_err(|_| RepositoryError::invalid_data())?;
        let completion_key = TransitionKey::derive(
            "scheduler.scope.cancel.attempt.v1",
            &[
                run_id.as_str(),
                scope_instance_id.as_str(),
                &activation_id,
                &attempt_no.to_string(),
                transition_key.as_str(),
            ],
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        sqlx::query(
            "UPDATE node_attempts SET lifecycle='cancelled',completion_transition_key=?,
                terminal_event_id=?,projection_version=projection_version+1,terminal_at=?
             WHERE run_id=? AND activation_id=? AND attempt_no=?
               AND lifecycle IN ('created','leased','running')",
        )
        .bind(completion_key.as_str())
        .bind(event_id_value)
        .bind(&now)
        .bind(run_id.as_str())
        .bind(&activation_id)
        .bind(attempt_no)
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    }

    sqlx::query(
        "UPDATE timers SET timer_state='cancelled',fired_at=?,
            projection_version=projection_version+1
         WHERE run_id=? AND activation_id IN (
            SELECT activation_id FROM node_activations
             WHERE run_id=? AND scope_instance_id=?
         ) AND timer_state='scheduled'",
    )
    .bind(&now)
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(scope_instance_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;

    sqlx::query(
        "UPDATE node_activations SET lifecycle='cancelled',
            termination_intent_reason='cancelled',termination_intent_transition_key=?,
            termination_intent_at=?,current_attempt_no=NULL,current_lease_epoch=NULL,
            current_fencing_token=NULL,pending_retry_timer_id=NULL,
            projection_version=projection_version+1,updated_at=?,terminal_at=?
         WHERE run_id=? AND scope_instance_id=?
           AND lifecycle IN ('created','ready','leased','running','retry_wait','waiting','terminating')",
    )
    .bind(transition_key.as_str())
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .bind(run_id.as_str())
    .bind(scope_instance_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    settle_dynamic_scope(transaction, run_id, scope_instance_id, true).await
}

/// Fences every pre-finalizer unit of work without closing the owning scopes.
/// The ErrorBoundary activation remains live so the scheduler can admit the
/// finalizer path while the Run itself is already `terminating`.
async fn prepare_termination_finalizer_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    boundary_activation_id: &ActivationId,
    transition_key: &TransitionKey,
    event_id_value: &str,
) -> Result<(), RepositoryError> {
    let attempts = sqlx::query(
        "SELECT activation_id,attempt_no FROM node_attempts
         WHERE run_id=? AND activation_id<>?
           AND lifecycle IN ('created','leased','running')",
    )
    .bind(run_id.as_str())
    .bind(boundary_activation_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for row in attempts {
        let activation_id: String = row
            .try_get("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let attempt_no: i64 = row
            .try_get("attempt_no")
            .map_err(|_| RepositoryError::invalid_data())?;
        let completion = TransitionKey::derive(
            "scheduler.finalizer.fence.attempt.v1",
            &[
                run_id.as_str(),
                &activation_id,
                &attempt_no.to_string(),
                transition_key.as_str(),
            ],
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        sqlx::query(
            "UPDATE node_attempts SET lifecycle='cancelled',failure_code='RUN_TERMINATING',
                completion_transition_key=?,terminal_event_id=?,
                projection_version=projection_version+1,terminal_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND activation_id=? AND attempt_no=?
               AND lifecycle IN ('created','leased','running')",
        )
        .bind(completion.as_str())
        .bind(event_id_value)
        .bind(run_id.as_str())
        .bind(&activation_id)
        .bind(attempt_no)
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    }
    sqlx::query(
        "UPDATE task_outbox SET task_state='dead',claimed_by=NULL,claim_token=NULL,
            claim_expires_at=NULL,claim_mode=NULL,last_error_code='RUN_TERMINATING',
            projection_version=projection_version+1
         WHERE run_id=? AND activation_id<>? AND task_state IN ('pending','claimed')",
    )
    .bind(run_id.as_str())
    .bind(boundary_activation_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "UPDATE timers SET timer_state='cancelled',fired_at=CURRENT_TIMESTAMP,
            projection_version=projection_version+1
         WHERE run_id=? AND activation_id IS NOT NULL AND activation_id<>?
           AND timer_state='scheduled'",
    )
    .bind(run_id.as_str())
    .bind(boundary_activation_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "UPDATE node_activations SET lifecycle='cancelled',
            termination_intent_reason='cancelled',termination_intent_transition_key=?,
            termination_intent_at=CURRENT_TIMESTAMP,current_attempt_no=NULL,
            current_lease_epoch=NULL,current_fencing_token=NULL,pending_retry_timer_id=NULL,
            projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP,
            terminal_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id<>?
           AND execution_kind<>'scheduler_native'
           AND lifecycle IN ('created','ready','leased','running','retry_wait','waiting','terminating')",
    )
    .bind(transition_key.as_str())
    .bind(run_id.as_str())
    .bind(boundary_activation_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

/// Atomically closes every scheduler-owned live row before publishing the
/// fail-closed Run terminal. Unlike authored FailRun, this path has no
/// Activation subject: the projection may be inconsistent before entry was
/// ever admitted.
async fn fail_run_planning_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    action: &PlannedSchedulerAction,
    event_id_value: &str,
    event_seq: u64,
    next_version: u64,
    failure: insight_engine::SchedulerPlanningFailure,
) -> Result<(), RepositoryError> {
    let run_id = action.intent().run_id();
    let internal_code = failure.internal_code();

    sqlx::query(
        "UPDATE task_outbox SET task_state='dead',claimed_by=NULL,claim_token=NULL,
            claim_expires_at=NULL,claim_mode=NULL,last_error_code=?,
            projection_version=projection_version+1
         WHERE run_id=? AND task_state IN ('pending','claimed','published')",
    )
    .bind(internal_code)
    .bind(run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;

    let attempts = sqlx::query(
        "SELECT activation_id,attempt_no FROM node_attempts
         WHERE run_id=? AND lifecycle IN ('created','leased','running')
         ORDER BY activation_id,attempt_no",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for attempt in attempts {
        let activation_id = attempt
            .try_get::<String, _>("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let attempt_no = attempt
            .try_get::<i64, _>("attempt_no")
            .map_err(|_| RepositoryError::invalid_data())?;
        let completion = TransitionKey::derive(
            "scheduler.planning_failure.attempt.v1",
            &[
                run_id.as_str(),
                &activation_id,
                &attempt_no.to_string(),
                action.transition_key().as_str(),
            ],
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        sqlx::query(
            "UPDATE node_attempts SET lifecycle='cancelled',failure_code=?,
                completion_transition_key=?,terminal_event_id=?,
                projection_version=projection_version+1,terminal_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND activation_id=? AND attempt_no=?
               AND lifecycle IN ('created','leased','running')",
        )
        .bind(internal_code)
        .bind(completion.as_str())
        .bind(event_id_value)
        .bind(run_id.as_str())
        .bind(&activation_id)
        .bind(attempt_no)
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    }

    sqlx::query(
        "UPDATE timers SET timer_state='cancelled',fired_at=CURRENT_TIMESTAMP,
            projection_version=projection_version+1
         WHERE run_id=? AND timer_state='scheduled'",
    )
    .bind(run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;

    let pending_signals = sqlx::query_scalar::<_, String>(
        "SELECT signal_id FROM signals_inbox WHERE run_id=? AND signal_state='pending'
         ORDER BY signal_id",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for signal_id in pending_signals {
        let rejection = TransitionKey::derive(
            "scheduler.planning_failure.signal.v1",
            &[
                run_id.as_str(),
                &signal_id,
                action.transition_key().as_str(),
            ],
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        sqlx::query(
            "UPDATE signals_inbox SET signal_state='rejected',
                consumed_by_transition_key=?,consumed_event_id=?,terminal_at=CURRENT_TIMESTAMP,
                projection_version=projection_version+1
             WHERE run_id=? AND signal_id=? AND signal_state='pending'",
        )
        .bind(rejection.as_str())
        .bind(event_id_value)
        .bind(run_id.as_str())
        .bind(&signal_id)
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    }

    sqlx::query(
        "UPDATE scheduler_wait_registrations
         SET winner_kind='cancelled',resolved_at=CURRENT_TIMESTAMP,
             projection_version=projection_version+1
         WHERE run_id=? AND winner_kind IS NULL",
    )
    .bind(run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "UPDATE control_tokens SET token_state='revoked',revoked_by_transition_key=?,
            revoked_at=CURRENT_TIMESTAMP,projection_version=projection_version+1
         WHERE run_id=? AND token_state='available'",
    )
    .bind(action.transition_key().as_str())
    .bind(run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "UPDATE fork_legs SET leg_state='cancelled',settlement_class='cancelled',
            settled_at=CURRENT_TIMESTAMP,projection_version=projection_version+1
         WHERE run_id=? AND leg_state='admitted'",
    )
    .bind(run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "UPDATE fork_groups SET group_state='cancelled',settled_legs=admitted_legs,
            settled_at=CURRENT_TIMESTAMP,projection_version=projection_version+1
         WHERE run_id=? AND group_state IN ('open','settling')",
    )
    .bind(run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "UPDATE node_activations SET lifecycle='failed',
            termination_intent_reason='failure',termination_intent_transition_key=?,
            termination_intent_at=CURRENT_TIMESTAMP,current_attempt_no=NULL,
            current_lease_epoch=NULL,
            current_fencing_token=NULL,pending_retry_timer_id=NULL,
            projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP,
            terminal_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND lifecycle IN
            ('created','ready','leased','running','retry_wait','waiting','terminating')",
    )
    .bind(action.transition_key().as_str())
    .bind(run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "UPDATE scope_instances SET lifecycle='cancelled',admission_state='closed',
            settled_children=admitted_children,projection_version=projection_version+1,
            settled_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND lifecycle IN ('active','settling')",
    )
    .bind(run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;

    let live_rows = sqlx::query_scalar::<_, i64>(
        "SELECT
            (SELECT COUNT(*) FROM scope_instances WHERE run_id=?
               AND lifecycle IN ('active','settling'))
          + (SELECT COUNT(*) FROM node_activations WHERE run_id=?
               AND lifecycle IN ('created','ready','leased','running','retry_wait','waiting','terminating'))
          + (SELECT COUNT(*) FROM node_attempts WHERE run_id=?
               AND lifecycle IN ('created','leased','running'))
          + (SELECT COUNT(*) FROM task_outbox WHERE run_id=?
               AND task_state IN ('pending','claimed','published'))
          + (SELECT COUNT(*) FROM timers WHERE run_id=? AND timer_state='scheduled')
          + (SELECT COUNT(*) FROM scheduler_wait_registrations WHERE run_id=?
               AND winner_kind IS NULL)
          + (SELECT COUNT(*) FROM signals_inbox WHERE run_id=? AND signal_state='pending')
          + (SELECT COUNT(*) FROM control_tokens WHERE run_id=? AND token_state='available')
          + (SELECT COUNT(*) FROM fork_legs WHERE run_id=? AND leg_state='admitted')
          + (SELECT COUNT(*) FROM fork_groups WHERE run_id=?
               AND group_state IN ('open','settling'))",
    )
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let root_closed = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM scope_instances WHERE run_id=? AND is_root=1
           AND lifecycle='cancelled' AND admission_state='closed'
           AND settled_children=admitted_children",
    )
    .bind(run_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if live_rows != 0 || root_closed.is_none() {
        return Err(RepositoryError::invalid_data());
    }

    let public_kind = match failure.kind() {
        insight_engine::SchedulerPlanningFailureKind::Workflow => {
            insight_engine::PublicFailureKind::Workflow
        }
        insight_engine::SchedulerPlanningFailureKind::Invariant => {
            insight_engine::PublicFailureKind::Infrastructure
        }
    };
    let public_id = insert_public_terminal(
        transaction,
        run_id,
        action.transition_key(),
        event_id_value,
        event_seq,
        PublicEventPayload::RunFailed {
            failure: insight_engine::PublicFailureSummary {
                kind: public_kind,
                code: model_data(insight_engine::PublicErrorCode::new(failure.public_code()))?,
            },
        },
    )
    .await?;
    let rows = sqlx::query(
        "UPDATE workflow_runs SET lifecycle='failed',admission_state='closed',
            termination_intent_reason='failure',termination_intent_transition_key=?,
            termination_intent_at=CURRENT_TIMESTAMP,output_payload_id=NULL,
            output_artifact_id=NULL,
            output_value_hash=NULL,error_code=?,terminal_event_id=?,terminal_public_event_id=?,
            projection_version=?,updated_at=CURRENT_TIMESTAMP,terminal_at=CURRENT_TIMESTAMP,
            scheduler_lease_owner=NULL,
            scheduler_fencing_token=NULL,scheduler_lease_expires_at=NULL,
            scheduler_heartbeat_at=NULL
         WHERE run_id=? AND projection_version=?",
    )
    .bind(action.transition_key().as_str())
    .bind(internal_code)
    .bind(event_id_value)
    .bind(public_id)
    .bind(i64_from_u64(next_version)?)
    .bind(run_id.as_str())
    .bind(i64_from_u64(
        action.precondition().expected_projection_version(),
    )?)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn event_for_action(
    transaction: &mut Transaction<'_, Sqlite>,
    action: &PlannedSchedulerAction,
) -> Result<PendingExecutionEvent, RepositoryError> {
    let run_id = action.intent().run_id();
    let (context, payload) = match action.intent().action() {
        SchedulerAction::FailRunPlanning { .. } => (
            ExecutionEventContext::for_run(run_id.clone()),
            ExecutionEventPayload::RunLifecycleChanged {
                lifecycle: RunLifecycle::Failed,
            },
        ),
        SchedulerAction::AdmitActivation {
            activation_id,
            node_id,
            scope_instance_id,
            reuse_candidate,
            ..
        } => {
            if reuse_candidate.is_some() {
                (
                    ExecutionEventContext::for_run(run_id.clone()),
                    ExecutionEventPayload::ProjectionMutated {
                        mutation: ProjectionMutationKind::SchedulerControlCommitted,
                    },
                )
            } else {
                (
                    ExecutionEventContext::for_run(run_id.clone()).for_activation(
                        scope_instance_id.clone(),
                        node_id.clone(),
                        activation_id.clone(),
                    ),
                    ExecutionEventPayload::ActivationCreated {
                        effect_id: Some(effect_id_for_activation(activation_id)?),
                    },
                )
            }
        }
        SchedulerAction::ConsumeToken {
            token_id,
            target_activation_id,
            ..
        } => {
            let (scope, node) =
                activation_identity(transaction, run_id, target_activation_id).await?;
            (
                ExecutionEventContext::for_run(run_id.clone()).for_activation(
                    scope,
                    node,
                    target_activation_id.clone(),
                ),
                ExecutionEventPayload::ControlTokenConsumed {
                    token_id: token_id.clone(),
                },
            )
        }
        SchedulerAction::EmitToken {
            token_id,
            source_activation_id,
            output_port,
            scope_instance_id,
        } => {
            let (_, node) = activation_identity(transaction, run_id, source_activation_id).await?;
            (
                ExecutionEventContext::for_run(run_id.clone()).for_activation(
                    scope_instance_id.clone(),
                    node,
                    source_activation_id.clone(),
                ),
                ExecutionEventPayload::ControlTokenEmitted {
                    token_id: token_id.clone(),
                    source_port: model_data(insight_engine::PortId::new(
                        output_port.as_str().to_owned(),
                    ))?,
                    token_scope_instance_id: scope_instance_id.clone(),
                    frames: Vec::new(),
                },
            )
        }
        SchedulerAction::DispatchTask {
            activation_id,
            node_id,
            ..
        } => {
            let (scope, _) = activation_identity(transaction, run_id, activation_id).await?;
            (
                ExecutionEventContext::for_run(run_id.clone()).for_activation(
                    scope,
                    node_id.clone(),
                    activation_id.clone(),
                ),
                ExecutionEventPayload::ActivationLeased {
                    attempt_no: AttemptNo::FIRST,
                    lease_epoch: LeaseEpoch::FIRST,
                },
            )
        }
        SchedulerAction::CommitNativeOutput {
            activation_id,
            output,
            ..
        } => {
            let (scope, node) = activation_identity(transaction, run_id, activation_id).await?;
            let encoded =
                serde_json::to_value(output).map_err(|_| RepositoryError::canonicalization())?;
            (
                ExecutionEventContext::for_run(run_id.clone()).for_activation(
                    scope,
                    node,
                    activation_id.clone(),
                ),
                ExecutionEventPayload::ActivationSucceeded {
                    attempt_no: None,
                    output: Some(output_summary(&encoded)?),
                },
            )
        }
        SchedulerAction::SelectBranchAndAdmit { selection } => {
            let encoded =
                serde_json::to_value(selection).map_err(|_| RepositoryError::canonicalization())?;
            (
                ExecutionEventContext::for_run(run_id.clone()).for_activation(
                    selection.branch_scope_instance_id().clone(),
                    selection.branch_node_id().clone(),
                    selection.branch_activation_id().clone(),
                ),
                ExecutionEventPayload::ActivationSucceeded {
                    attempt_no: None,
                    output: Some(output_summary(&encoded)?),
                },
            )
        }
        SchedulerAction::CommitOccurrenceValues { .. } => (
            ExecutionEventContext::for_run(run_id.clone()),
            ExecutionEventPayload::ProjectionMutated {
                mutation: ProjectionMutationKind::SchedulerValuesCommitted,
            },
        ),
        SchedulerAction::CompleteRun { .. } => (
            ExecutionEventContext::for_run(run_id.clone()),
            ExecutionEventPayload::RunLifecycleChanged {
                lifecycle: RunLifecycle::Succeeded,
            },
        ),
        SchedulerAction::FailRun { .. } | SchedulerAction::FailRunInternal { .. } => (
            ExecutionEventContext::for_run(run_id.clone()),
            ExecutionEventPayload::RunLifecycleChanged {
                lifecycle: RunLifecycle::Failed,
            },
        ),
        SchedulerAction::CancelRun { reason, .. } => (
            ExecutionEventContext::for_run(run_id.clone()),
            ExecutionEventPayload::RunLifecycleChanged {
                lifecycle: match reason {
                    TerminationReason::Failure => RunLifecycle::Failed,
                    TerminationReason::Cancelled => RunLifecycle::Cancelled,
                    TerminationReason::Interrupted => RunLifecycle::Interrupted,
                    TerminationReason::TimedOut => RunLifecycle::TimedOut,
                },
            },
        ),
        SchedulerAction::RegisterWait { registration } => {
            let registration = scheduler_adapter::wait_registration_parts(registration);
            let (scope, node) =
                activation_identity(transaction, run_id, registration.activation_id).await?;
            (
                ExecutionEventContext::for_run(run_id.clone()).for_activation(
                    scope,
                    node,
                    registration.activation_id.clone(),
                ),
                ExecutionEventPayload::ActivationWaiting,
            )
        }
        SchedulerAction::OpenFork { .. }
        | SchedulerAction::SettleForkLeg { .. }
        | SchedulerAction::CompleteFork { .. }
        | SchedulerAction::RequestScopeCancellation { .. }
        | SchedulerAction::OpenMap { .. }
        | SchedulerAction::SpawnMapItem { .. }
        | SchedulerAction::SettleMapItem { .. }
        | SchedulerAction::CompleteMap { .. }
        | SchedulerAction::OpenLoop { .. }
        | SchedulerAction::StartLoopIteration { .. }
        | SchedulerAction::AdvanceLoop { .. }
        | SchedulerAction::SettleLoopIteration { .. }
        | SchedulerAction::CompleteLoop { .. }
        | SchedulerAction::StartSubflow { .. }
        | SchedulerAction::RequestChildRunCancellation { .. }
        | SchedulerAction::SettleSubflow { .. }
        | SchedulerAction::OpenErrorBoundary { .. }
        | SchedulerAction::TransitionErrorBoundary { .. } => (
            ExecutionEventContext::for_run(run_id.clone()),
            ExecutionEventPayload::ProjectionMutated {
                mutation: ProjectionMutationKind::SchedulerControlCommitted,
            },
        ),
    };
    model_data(PendingExecutionEvent::new(context, payload))
}

async fn exact_scheduler_action(
    transaction: &mut Transaction<'_, Sqlite>,
    action: &PlannedSchedulerAction,
    replay: &super::CommitReceipt,
) -> Result<SchedulerCommitReceipt, RepositoryError> {
    let row = sqlx::query(
        "SELECT checkpoint_id,content_hash,checkpoint_kind,transition_key,intent_hash,event_id,
                checkpoint_schema_version,scheduler_projection_version,fact_payload
         FROM scheduler_checkpoints WHERE run_id=? AND transition_key=?",
    )
    .bind(action.intent().run_id().as_str())
    .bind(action.transition_key().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let event_id: String = row
        .try_get("event_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let checkpoint_id = scheduler_data(SchedulerCheckpointId::parse(
        row.try_get::<String, _>("checkpoint_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let schema_version = u32::try_from(
        row.try_get::<i64, _>("checkpoint_schema_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let scheduler_version = u64_from_i64(
        row.try_get("scheduler_projection_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let payload_text: String = row
        .try_get("fact_payload")
        .map_err(|_| RepositoryError::invalid_data())?;
    let payload: Value =
        serde_json::from_str(&payload_text).map_err(|_| RepositoryError::invalid_data())?;
    let expected_payload =
        serde_json::to_value(action.intent()).map_err(|_| RepositoryError::canonicalization())?;
    let stored_hash: String = row
        .try_get("content_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    if &checkpoint_id != action.intent().checkpoint_id()
        || row
            .try_get::<String, _>("checkpoint_kind")
            .map_err(|_| RepositoryError::invalid_data())?
            != "planned_action"
        || row
            .try_get::<String, _>("transition_key")
            .map_err(|_| RepositoryError::invalid_data())?
            != action.transition_key().as_str()
        || row
            .try_get::<String, _>("intent_hash")
            .map_err(|_| RepositoryError::invalid_data())?
            != action.intent_hash().as_str()
        || event_id != replay.event_id()
        || schema_version != SCHEDULER_CHECKPOINT_SCHEMA_VERSION
        || scheduler_version != replay.projection_version()
        || payload != expected_payload
        || scheduler_checkpoint_content_hash(
            action.intent().run_id().as_str(),
            checkpoint_id.as_str(),
            "planned_action",
            action.transition_key().as_str(),
            action.intent_hash().as_str(),
            &event_id,
            schema_version,
            scheduler_version,
            &payload,
        )?
        .as_str()
            != stored_hash
    {
        return Err(RepositoryError::invalid_data());
    }
    let expected_event = event_for_action(transaction, action).await?;
    validate_execution_event_sqlite(
        transaction,
        action.intent().run_id(),
        action.transition_key(),
        action.intent_hash().as_str(),
        replay,
        &expected_event,
    )
    .await?;
    Ok(SchedulerCommitReceipt::new(
        replay.event_seq(),
        event_id,
        checkpoint_id,
        scheduler_version,
    ))
}

async fn validate_scheduler_fence(
    transaction: &mut Transaction<'_, Sqlite>,
    fence: &FencedSchedulerRunCommand,
    expected_projection_version: u64,
) -> Result<bool, RepositoryError> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM workflow_runs
         WHERE run_id=? AND scheduler_lease_owner=? AND scheduler_lease_epoch=?
           AND scheduler_fencing_token=?
           AND julianday(scheduler_lease_expires_at) > julianday('now')
           AND projection_version=? AND lifecycle NOT IN
               ('succeeded','failed','cancelled','interrupted','timed_out')",
    )
    .bind(fence.run_id().as_str())
    .bind(fence.owner())
    .bind(i64_from_u64(fence.lease_epoch())?)
    .bind(fence.fencing_token())
    .bind(i64_from_u64(expected_projection_version)?)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .is_some())
}

async fn insert_scheduler_checkpoint(
    transaction: &mut Transaction<'_, Sqlite>,
    action: &PlannedSchedulerAction,
    event_id: &str,
    next_version: u64,
) -> Result<(), RepositoryError> {
    let fact_payload =
        serde_json::to_value(action.intent()).map_err(|_| RepositoryError::canonicalization())?;
    let content_hash = scheduler_checkpoint_content_hash(
        action.intent().run_id().as_str(),
        action.intent().checkpoint_id().as_str(),
        "planned_action",
        action.transition_key().as_str(),
        action.intent_hash().as_str(),
        event_id,
        SCHEDULER_CHECKPOINT_SCHEMA_VERSION,
        next_version,
        &fact_payload,
    )?;
    let payload = canonical_json(&fact_payload)?;
    sqlx::query(
        "INSERT INTO scheduler_checkpoints (
            run_id,checkpoint_id,content_hash,checkpoint_kind,transition_key,intent_hash,event_id,
            checkpoint_schema_version,scheduler_projection_version,fact_payload,
            projection_version,created_at
         ) VALUES (?,?,?,'planned_action',?,?,?,?,?,?,0,CURRENT_TIMESTAMP)",
    )
    .bind(action.intent().run_id().as_str())
    .bind(action.intent().checkpoint_id().as_str())
    .bind(content_hash.as_str())
    .bind(action.transition_key().as_str())
    .bind(action.intent_hash().as_str())
    .bind(event_id)
    .bind(i64::from(SCHEDULER_CHECKPOINT_SCHEMA_VERSION))
    .bind(i64_from_u64(next_version)?)
    .bind(payload)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

async fn validated_planned_action_checkpoint_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    transition_key: &str,
) -> Result<ValidatedPlannedActionCheckpoint, RepositoryError> {
    let transition = model_data(TransitionKey::parse(transition_key.to_owned()))?;
    let row = sqlx::query(
        "SELECT checkpoint_id,content_hash,checkpoint_kind,transition_key,intent_hash,event_id,
                checkpoint_schema_version,scheduler_projection_version,fact_payload
         FROM scheduler_checkpoints WHERE run_id=? AND transition_key=?",
    )
    .bind(run_id.as_str())
    .bind(transition.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let checkpoint_id = scheduler_data(SchedulerCheckpointId::parse(
        row.try_get::<String, _>("checkpoint_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let schema_version = u32::try_from(
        row.try_get::<i64, _>("checkpoint_schema_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let scheduler_version = u64_from_i64(
        row.try_get("scheduler_projection_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let payload_text: String = row
        .try_get("fact_payload")
        .map_err(|_| RepositoryError::invalid_data())?;
    let payload: Value =
        serde_json::from_str(&payload_text).map_err(|_| RepositoryError::invalid_data())?;
    let intent: SchedulerIntent =
        serde_json::from_value(payload.clone()).map_err(|_| RepositoryError::invalid_data())?;
    let intent_hash = canonical_intent_hash(&intent)?;
    let stored_event: String = row
        .try_get("event_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let stored_hash: String = row
        .try_get("content_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    if intent.run_id() != run_id
        || intent.checkpoint_id() != &checkpoint_id
        || row
            .try_get::<String, _>("checkpoint_kind")
            .map_err(|_| RepositoryError::invalid_data())?
            != "planned_action"
        || row
            .try_get::<String, _>("transition_key")
            .map_err(|_| RepositoryError::invalid_data())?
            != transition.as_str()
        || row
            .try_get::<String, _>("intent_hash")
            .map_err(|_| RepositoryError::invalid_data())?
            != intent_hash.as_str()
        || schema_version != SCHEDULER_CHECKPOINT_SCHEMA_VERSION
        || scheduler_version == 0
        || scheduler_checkpoint_content_hash(
            run_id.as_str(),
            checkpoint_id.as_str(),
            "planned_action",
            transition.as_str(),
            intent_hash.as_str(),
            &stored_event,
            schema_version,
            scheduler_version,
            &payload,
        )?
        .as_str()
            != stored_hash
    {
        return Err(RepositoryError::invalid_data());
    }
    let replay = match load_replay(transaction, run_id, &transition, intent_hash.as_str()).await? {
        Replay::Exact(replay)
            if replay.event_id() == stored_event
                && replay.projection_version() == scheduler_version =>
        {
            replay
        }
        Replay::Exact(_) | Replay::Vacant => return Err(RepositoryError::invalid_data()),
    };
    let action = insight_engine::internal::planned_scheduler_action(
        insight_engine::internal::scheduler_precondition(
            scheduler_version
                .checked_sub(1)
                .ok_or_else(RepositoryError::invalid_data)?,
        ),
        transition.clone(),
        intent_hash,
        intent.clone(),
    );
    let expected_event = event_for_action(transaction, &action).await?;
    validate_execution_event_sqlite(
        transaction,
        run_id,
        &transition,
        action.intent_hash().as_str(),
        &replay,
        &expected_event,
    )
    .await?;
    Ok(ValidatedPlannedActionCheckpoint {
        transition_key: transition,
        event_id: stored_event,
        intent,
    })
}

/// The scheduler value row and the artifact state change are committed in the
/// same fenced result transaction. A verified object can therefore never be
/// collected after its output reference becomes authoritative.
async fn reference_scheduler_artifact(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    artifact: &ArtifactRef,
) -> Result<(), RepositoryError> {
    let referenced = sqlx::query_scalar::<_, i64>(
        "UPDATE artifacts
         SET artifact_state='referenced',
             referenced_at=COALESCE(referenced_at,CURRENT_TIMESTAMP)
         WHERE run_id=? AND artifact_id=? AND content_hash=? AND size_bytes=?
           AND artifact_state IN ('verified','referenced')
         RETURNING 1",
    )
    .bind(run_id.as_str())
    .bind(artifact.artifact_id().as_str())
    .bind(artifact.content_hash().as_str())
    .bind(i64_from_u64(artifact.size_bytes())?)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if referenced.is_none() {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn preserve_global_artifact_ref(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    port_id: &DataPortId,
    proposed: &ValueRef,
) -> Result<ValueRef, RepositoryError> {
    if matches!(proposed, ValueRef::Artifact(_)) {
        return Ok(proposed.clone());
    }
    let existing = sqlx::query(
        "SELECT value_ref,content_hash FROM scheduler_values WHERE run_id=? AND port_id=?",
    )
    .bind(run_id.as_str())
    .bind(port_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(existing) = existing else {
        return Ok(proposed.clone());
    };
    if existing
        .try_get::<String, _>("content_hash")
        .map_err(|_| RepositoryError::invalid_data())?
        != proposed.content_hash().as_str()
    {
        return Ok(proposed.clone());
    }
    let value_ref = serde_json::from_str::<ValueRef>(
        &existing
            .try_get::<String, _>("value_ref")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    Ok(match value_ref {
        ValueRef::Artifact(_) => value_ref,
        ValueRef::Inline(_) => proposed.clone(),
    })
}

async fn preserve_occurrence_artifact_ref(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    occurrence_key: &str,
    port_id: &DataPortId,
    proposed: &ValueRef,
) -> Result<ValueRef, RepositoryError> {
    if matches!(proposed, ValueRef::Artifact(_)) {
        return Ok(proposed.clone());
    }
    let existing = sqlx::query(
        "SELECT value_ref,content_hash FROM scheduler_occurrence_values
         WHERE run_id=? AND occurrence_key=? AND port_id=?",
    )
    .bind(run_id.as_str())
    .bind(occurrence_key)
    .bind(port_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(existing) = existing else {
        return Ok(proposed.clone());
    };
    if existing
        .try_get::<String, _>("content_hash")
        .map_err(|_| RepositoryError::invalid_data())?
        != proposed.content_hash().as_str()
    {
        return Ok(proposed.clone());
    }
    let value_ref = serde_json::from_str::<ValueRef>(
        &existing
            .try_get::<String, _>("value_ref")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    Ok(match value_ref {
        ValueRef::Artifact(_) => value_ref,
        ValueRef::Inline(_) => proposed.clone(),
    })
}

async fn upsert_scheduler_value(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    owner_activation_id: &ActivationId,
    port_id: &DataPortId,
    runtime_value: &RuntimeValue,
    value_ref: &ValueRef,
    declared_type: &PlanType,
) -> Result<u64, RepositoryError> {
    if !runtime_value.matches(declared_type) {
        return Err(RepositoryError::invalid_data());
    }
    let effective_value_ref =
        preserve_global_artifact_ref(transaction, run_id, port_id, value_ref).await?;
    validate_runtime_value_ref(runtime_value, &effective_value_ref)?;
    let (storage_kind, payload_id_value, artifact_id_value) = match &effective_value_ref {
        ValueRef::Inline(inline) => {
            if inline.value() != runtime_value.value() {
                return Err(RepositoryError::invalid_data());
            }
            let (payload_id, _) =
                insert_or_get_payload(transaction, run_id, runtime_value.value()).await?;
            ("inline", Some(payload_id), None)
        }
        ValueRef::Artifact(artifact) => {
            reference_scheduler_artifact(transaction, run_id, artifact).await?;
            (
                "artifact",
                None,
                Some(artifact.artifact_id().as_str().to_owned()),
            )
        }
    };
    let existing = sqlx::query_scalar::<_, i64>(
        "SELECT projection_version FROM scheduler_values WHERE run_id=? AND port_id=?",
    )
    .bind(run_id.as_str())
    .bind(port_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let next = existing
        .unwrap_or(-1)
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    sqlx::query(
        "INSERT INTO scheduler_values (
            run_id,port_id,owner_activation_id,runtime_value,value_ref,declared_type,
            storage_kind,payload_id,artifact_id,content_hash,projection_version,created_at,updated_at
         ) VALUES (?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
         ON CONFLICT(run_id,port_id) DO UPDATE SET
            owner_activation_id=excluded.owner_activation_id,
            runtime_value=excluded.runtime_value,value_ref=excluded.value_ref,
            declared_type=excluded.declared_type,storage_kind=excluded.storage_kind,
            payload_id=excluded.payload_id,artifact_id=excluded.artifact_id,
            content_hash=excluded.content_hash,projection_version=excluded.projection_version,
            updated_at=CURRENT_TIMESTAMP",
    )
    .bind(run_id.as_str())
    .bind(port_id.as_str())
    .bind(owner_activation_id.as_str())
    .bind(canonical_json(
        &serde_json::to_value(runtime_value)
            .map_err(|_| RepositoryError::canonicalization())?,
    )?)
    .bind(canonical_json(
        &serde_json::to_value(&effective_value_ref)
            .map_err(|_| RepositoryError::canonicalization())?,
    )?)
    .bind(canonical_json(
        &serde_json::to_value(declared_type)
            .map_err(|_| RepositoryError::canonicalization())?,
    )?)
    .bind(storage_kind)
    .bind(payload_id_value)
    .bind(artifact_id_value)
    .bind(effective_value_ref.content_hash().as_str())
    .bind(next)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    u64_from_i64(next)
}

#[allow(clippy::too_many_arguments)]
async fn upsert_occurrence_value(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    occurrence: &insight_engine::LogicalOccurrence,
    owner_activation_id: &ActivationId,
    port_id: &DataPortId,
    runtime_value: &RuntimeValue,
    value_ref: &ValueRef,
    declared_type: &PlanType,
) -> Result<u64, RepositoryError> {
    if !runtime_value.matches(declared_type) {
        return Err(RepositoryError::invalid_data());
    }
    let occurrence_key = canonical_json(
        &serde_json::to_value(occurrence).map_err(|_| RepositoryError::canonicalization())?,
    )?;
    let effective_value_ref =
        preserve_occurrence_artifact_ref(transaction, run_id, &occurrence_key, port_id, value_ref)
            .await?;
    validate_runtime_value_ref(runtime_value, &effective_value_ref)?;
    let (storage_kind, payload_id_value, artifact_id_value) = match &effective_value_ref {
        ValueRef::Inline(inline) => {
            if inline.value() != runtime_value.value() {
                return Err(RepositoryError::invalid_data());
            }
            let (payload_id, _) =
                insert_or_get_payload(transaction, run_id, runtime_value.value()).await?;
            ("inline", Some(payload_id), None)
        }
        ValueRef::Artifact(artifact) => {
            reference_scheduler_artifact(transaction, run_id, artifact).await?;
            (
                "artifact",
                None,
                Some(artifact.artifact_id().as_str().to_owned()),
            )
        }
    };
    let inserted = sqlx::query(
        "INSERT INTO scheduler_occurrence_values (
            run_id,occurrence_key,port_id,owner_activation_id,runtime_value,value_ref,
            declared_type,storage_kind,payload_id,artifact_id,content_hash,
            projection_version,created_at,updated_at
         ) VALUES (?,?,?,?,?,?,?,?,?,?,?,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
         ON CONFLICT(run_id,occurrence_key,port_id) DO NOTHING",
    )
    .bind(run_id.as_str())
    .bind(&occurrence_key)
    .bind(port_id.as_str())
    .bind(owner_activation_id.as_str())
    .bind(canonical_json(
        &serde_json::to_value(runtime_value).map_err(|_| RepositoryError::canonicalization())?,
    )?)
    .bind(canonical_json(
        &serde_json::to_value(&effective_value_ref)
            .map_err(|_| RepositoryError::canonicalization())?,
    )?)
    .bind(canonical_json(
        &serde_json::to_value(declared_type).map_err(|_| RepositoryError::canonicalization())?,
    )?)
    .bind(storage_kind)
    .bind(payload_id_value)
    .bind(artifact_id_value)
    .bind(effective_value_ref.content_hash().as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if inserted == 1 {
        return Ok(0);
    }
    let row = sqlx::query(
        "SELECT occurrence_key,port_id,owner_activation_id,runtime_value,value_ref,declared_type,
                storage_kind,payload_id,artifact_id,content_hash,projection_version
         FROM scheduler_occurrence_values
         WHERE run_id=? AND occurrence_key=? AND port_id=?",
    )
    .bind(run_id.as_str())
    .bind(&occurrence_key)
    .bind(port_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let stored = stored_value_from_row(run_id, &row)?;
    let stored_occurrence: insight_engine::LogicalOccurrence = serde_json::from_str(
        &row.try_get::<String, _>("occurrence_key")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if stored_occurrence != *occurrence
        || stored.port_id() != port_id
        || row
            .try_get::<String, _>("owner_activation_id")
            .map_err(|_| RepositoryError::invalid_data())?
            != owner_activation_id.as_str()
        || stored.runtime_value() != runtime_value
        || stored.value_ref() != &effective_value_ref
        || stored.declared_type() != declared_type
        || stored.projection_version() != 0
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(0)
}

async fn insert_public_terminal(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    transition_key: &TransitionKey,
    event_id_value: &str,
    event_seq: u64,
    payload: PublicEventPayload,
) -> Result<String, RepositoryError> {
    let event_kind = payload.kind();
    let public_id = public_event_id(run_id, transition_key, event_kind);
    let event = sqlx::query(
        "SELECT schema_version,occurred_at FROM execution_events WHERE run_id=? AND event_id=?",
    )
    .bind(run_id.as_str())
    .bind(event_id_value)
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    decode_execution_event_schema_version(
        event
            .try_get::<i64, _>("schema_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let occurred_at = event
        .try_get::<String, _>("occurred_at")
        .map_err(|_| RepositoryError::invalid_data())?;
    let envelope = durable_public_event_envelope(
        run_id,
        &public_id,
        event_id_value,
        event_seq,
        parse_run_timestamp(&occurred_at)?,
        payload,
    )?;
    let envelope = canonical_json(
        &serde_json::to_value(envelope).map_err(|_| RepositoryError::canonicalization())?,
    )?;
    sqlx::query(
        "INSERT INTO public_event_outbox (
            run_id,public_event_id,causation_event_id,public_ordinal,public_schema_version,event_kind,
            is_terminal,publish_state,safe_envelope,available_at,claimed_by,claim_token,
            claim_expires_at,publish_attempts,published_at,published_by,
            published_claim_token,notified_at,retain_until,created_at
         ) VALUES (?,?,?,?,1,?,1,'pending',?,CURRENT_TIMESTAMP,NULL,NULL,NULL,0,
                   NULL,NULL,NULL,NULL,NULL,CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(&public_id)
    .bind(event_id_value)
    .bind(i64::from(public_event_ordinal(event_kind)))
    .bind(event_kind.as_str())
    .bind(envelope)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(public_id)
}

async fn succeed_native_activation(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    activation_id: &ActivationId,
    value: &Value,
) -> Result<(), RepositoryError> {
    let (payload_id, value_hash) = insert_or_get_payload(transaction, run_id, value).await?;
    let rows = sqlx::query(
        "UPDATE node_activations SET lifecycle='succeeded',effect_evidence='committed',
            output_payload_id=?,output_artifact_id=NULL,output_value_hash=?,
            winning_attempt_no=NULL,current_attempt_no=NULL,current_lease_epoch=NULL,
            current_fencing_token=NULL,projection_version=projection_version+1,
            updated_at=CURRENT_TIMESTAMP,terminal_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND execution_kind='scheduler_native'
           AND lifecycle IN ('created','ready')",
    )
    .bind(payload_id)
    .bind(value_hash)
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows == 1 {
        return Ok(());
    }
    let already_succeeded = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM node_activations WHERE run_id=? AND activation_id=?
         AND execution_kind='scheduler_native' AND lifecycle='succeeded'",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if already_succeeded.is_none() {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn fail_activation_for_run_terminal(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    activation_id: &ActivationId,
    transition_key: &TransitionKey,
) -> Result<(), RepositoryError> {
    let rows = sqlx::query(
        "UPDATE node_activations SET lifecycle='failed',
            termination_intent_reason='failure',termination_intent_transition_key=?,
            termination_intent_at=CURRENT_TIMESTAMP,effect_evidence='committed',
            projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP,
            terminal_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND lifecycle IN ('created','ready')",
    )
    .bind(transition_key.as_str())
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows == 1 {
        return Ok(());
    }
    let already_terminal = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM node_activations WHERE run_id=? AND activation_id=?
         AND lifecycle IN ('succeeded','failed','cancelled','timed_out')",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if already_terminal.is_none() {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn start_subflow_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    parent_run_id: &RunId,
    transition_key: &TransitionKey,
    invocation: &insight_engine::scheduler::SubflowInvocationFact,
    execution_revision: &insight_engine::ExecutionRevisionPin,
    interface_version: &insight_engine::plan::VersionTag,
    timeout_ms: u64,
    run_input: &insight_engine::scheduler::RuntimeValue,
    outputs: &[insight_engine::scheduler::TaskOutputContract],
) -> Result<(), RepositoryError> {
    let child_run_id = invocation.child_run_id();
    let (actual_parent_scope, actual_node) = activation_identity(
        transaction,
        parent_run_id,
        invocation.parent_activation_id(),
    )
    .await?;
    let actual_occurrence = activation_occurrence(
        transaction,
        parent_run_id,
        invocation.parent_activation_id(),
    )
    .await?;
    if actual_parent_scope != *invocation.parent_scope_instance_id()
        || actual_node != *invocation.node_id()
        || actual_occurrence != *invocation.occurrence()
    {
        return Err(RepositoryError::invalid_data());
    }
    let parent_rows = sqlx::query(
        "UPDATE scope_instances SET admitted_children=admitted_children+1,
            projection_version=projection_version+1
         WHERE run_id=? AND scope_instance_id=? AND lifecycle='active'
           AND admission_state='open'",
    )
    .bind(parent_run_id.as_str())
    .bind(invocation.parent_scope_instance_id().as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if parent_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    let invocation_scope_rows = sqlx::query(
        "INSERT INTO scope_instances (
            run_id,scope_instance_id,parent_scope_instance_id,static_scope_id,
            stable_dynamic_key,scope_kind,is_root,lifecycle,admission_state,
            admitted_children,settled_children,projection_version,created_at,settled_at
         ) VALUES (?,?,?,?,?,'subflow_invocation',0,'active','open',0,0,0,
                   CURRENT_TIMESTAMP,NULL)",
    )
    .bind(parent_run_id.as_str())
    .bind(invocation.invocation_scope_instance_id().as_str())
    .bind(invocation.parent_scope_instance_id().as_str())
    .bind(invocation.static_scope_id().as_str())
    .bind(child_run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if invocation_scope_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    let (definition_id, canonical_plan) = sqlx::query_as::<_, (String, String)>(
        "SELECT d.definition_id,r.canonical_plan FROM deployment_revisions d
         JOIN workflow_definition_revisions r
           ON r.definition_id=d.definition_id
          AND r.definition_revision_id=d.definition_revision_id
          AND r.plan_hash=d.plan_hash
         WHERE d.definition_revision_id=? AND d.deployment_revision_id=?
           AND d.plan_hash=? AND d.binding_hash=?",
    )
    .bind(execution_revision.definition_revision_id().as_str())
    .bind(execution_revision.deployment_revision_id().as_str())
    .bind(execution_revision.plan_hash().as_str())
    .bind(execution_revision.binding_hash().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;

    let input = run_input.value().clone();
    let child_plan = serde_json::from_str::<Plan>(&canonical_plan)
        .map_err(|_| RepositoryError::invalid_data())?;
    let run_type = child_plan
        .metadata()
        .input_contract()
        .run_type()
        .map_err(|_| RepositoryError::invalid_data())?;
    if !run_type.accepts_literal(&input).unwrap_or(false) {
        return Err(RepositoryError::invalid_data());
    }
    let (input_json, input_hash) = canonical_value(&input)?;
    let input_payload_id = payload_id(&input_hash);
    let request_id = format!(
        "subflow:{}:{}",
        parent_run_id.as_str(),
        invocation.parent_activation_id().as_str()
    );
    let child_deadline = sqlx::query_scalar::<_, String>(
        "WITH policy(deadline_at) AS (
             SELECT strftime('%Y-%m-%dT%H:%M:%fZ',
                             julianday('now') + CAST(? AS REAL) / 86400000.0)
         )
         SELECT CASE
             WHEN r.deadline_at IS NOT NULL
              AND julianday(r.deadline_at) <= julianday(policy.deadline_at)
             THEN r.deadline_at
             ELSE policy.deadline_at
         END
         FROM workflow_runs r CROSS JOIN policy WHERE r.run_id=?",
    )
    .bind(i64_from_u64(timeout_ms)?)
    .bind(parent_run_id.as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let child_deadline_at = super::sqlite::parse_run_timestamp(&child_deadline)?;
    sqlx::query(
        "INSERT INTO workflow_runs (
            run_id,definition_id,definition_revision_id,deployment_revision_id,
            plan_hash,binding_hash,request_id,attachment,lifecycle,admission_state,
            termination_intent_reason,termination_intent_transition_key,termination_intent_at,
            input_payload_id,output_payload_id,output_artifact_id,output_value_hash,error_code,
            terminal_event_id,terminal_public_event_id,parent_run_id,lineage_kind,generation,
            replacement_run_id,next_event_seq,projection_version,scheduler_lease_epoch,
            scheduler_lease_owner,scheduler_fencing_token,scheduler_lease_expires_at,
            scheduler_heartbeat_at,created_at,started_at,updated_at,terminal_at,deadline_at
         ) VALUES (?,?,?,?,?,?,?,'detached','created','open',NULL,NULL,NULL,?,NULL,NULL,NULL,NULL,
                   NULL,NULL,?,'subflow',1,NULL,1,0,0,NULL,NULL,NULL,NULL,
                   CURRENT_TIMESTAMP,NULL,CURRENT_TIMESTAMP,NULL,?)",
    )
    .bind(child_run_id.as_str())
    .bind(&definition_id)
    .bind(execution_revision.definition_revision_id().as_str())
    .bind(execution_revision.deployment_revision_id().as_str())
    .bind(execution_revision.plan_hash().as_str())
    .bind(execution_revision.binding_hash().as_str())
    .bind(request_id)
    .bind(&input_payload_id)
    .bind(parent_run_id.as_str())
    .bind(child_deadline_at.to_rfc3339())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "INSERT INTO payloads (
            run_id,payload_id,content_hash,canonical_bytes,encoding,inline_value,
            binary_value,created_at,retain_until
         ) VALUES (?,?,?,?,'json_jcs',?,NULL,CURRENT_TIMESTAMP,NULL)",
    )
    .bind(child_run_id.as_str())
    .bind(&input_payload_id)
    .bind(input_hash.as_str())
    .bind(i64::try_from(input_json.len()).map_err(|_| RepositoryError::invalid_data())?)
    .bind(&input_json)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "INSERT INTO scope_instances (
            run_id,scope_instance_id,parent_scope_instance_id,static_scope_id,
            stable_dynamic_key,scope_kind,is_root,lifecycle,admission_state,
            admitted_children,settled_children,projection_version,created_at,settled_at
         ) VALUES (?,'scope_root',NULL,'root',NULL,'root',1,'active','open',
                   0,0,0,CURRENT_TIMESTAMP,NULL)",
    )
    .bind(child_run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;

    let child_transition = TransitionKey::derive(
        "scheduler.subflow.created.v2",
        &[transition_key.as_str(), child_run_id.as_str()],
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let child_intent_hash = canonical_intent_hash(&json!({
        "operation": "scheduler.subflow.created.v2",
        "parent_run_id": parent_run_id,
        "parent_transition_key": transition_key,
        "child_run_id": child_run_id,
        "execution_revision": execution_revision,
        "timeout_ms": timeout_ms,
        "run_input_hash": input_hash,
    }))?;
    let child_seq = allocate_event_seq(transaction, child_run_id).await?;
    let child_event_id = event_id(&child_transition);
    let child_event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(child_run_id.clone()),
        ExecutionEventPayload::RunCreated {
            definition_revision_id: execution_revision.definition_revision_id().clone(),
            deployment_revision_id: execution_revision.deployment_revision_id().clone(),
            run_deadline_at: Some(child_deadline_at),
        },
    ))?;
    insert_event(
        transaction,
        child_run_id,
        child_seq,
        &child_event_id,
        &child_transition,
        child_intent_hash.as_str(),
        0,
        &child_event,
    )
    .await?;
    finalize_projection_checkpoints(transaction, child_run_id, &child_event_id).await?;

    sqlx::query(
        "INSERT INTO scheduler_subflow_invocations (
            run_id,child_run_id,parent_activation_id,node_id,occurrence_key,
            invocation_scope_instance_id,parent_scope_instance_id,static_scope_id,
            definition_revision_id,deployment_revision_id,plan_hash,binding_hash,
            interface_version,output_contracts,invocation_state,projection_version,
            created_at,completed_at
         ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,'started',0,CURRENT_TIMESTAMP,NULL)",
    )
    .bind(parent_run_id.as_str())
    .bind(child_run_id.as_str())
    .bind(invocation.parent_activation_id().as_str())
    .bind(invocation.node_id().as_str())
    .bind(canonical_json(
        &serde_json::to_value(invocation.occurrence())
            .map_err(|_| RepositoryError::canonicalization())?,
    )?)
    .bind(invocation.invocation_scope_instance_id().as_str())
    .bind(invocation.parent_scope_instance_id().as_str())
    .bind(invocation.static_scope_id().as_str())
    .bind(execution_revision.definition_revision_id().as_str())
    .bind(execution_revision.deployment_revision_id().as_str())
    .bind(execution_revision.plan_hash().as_str())
    .bind(execution_revision.binding_hash().as_str())
    .bind(interface_version.as_str())
    .bind(canonical_json(
        &serde_json::to_value(outputs).map_err(|_| RepositoryError::canonicalization())?,
    )?)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

async fn observed_subflow_outcome_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    child_run_id: &RunId,
    output_contracts: &str,
    lifecycle: &str,
    error_code: Option<&str>,
    inline_value: Option<&str>,
) -> Result<insight_engine::scheduler::SubflowOutcomeFact, RepositoryError> {
    use insight_engine::scheduler::{SubflowOutcomeFact, TaskFailureFact};

    match lifecycle {
        "succeeded" => {
            let contracts = serde_json::from_str::<
                Vec<insight_engine::scheduler::TaskOutputContract>,
            >(output_contracts)
            .map_err(|_| RepositoryError::invalid_data())?;
            let encoded = inline_value.ok_or_else(RepositoryError::invalid_data)?;
            let raw = serde_json::from_str::<Value>(encoded)
                .map_err(|_| RepositoryError::invalid_data())?;
            let mut outputs = BTreeMap::new();
            for contract in &contracts {
                let selected = raw
                    .as_object()
                    .and_then(|object| object.get(contract.name().as_str()))
                    .cloned()
                    .or_else(|| (contracts.len() == 1).then(|| raw.clone()));
                let Some(selected) = selected else {
                    if contract.required() {
                        return Err(RepositoryError::invalid_data());
                    }
                    continue;
                };
                let value = scheduler_data(RuntimeValue::new(selected))?;
                if !value.matches(contract.value_type()) {
                    return Err(RepositoryError::invalid_data());
                }
                outputs.insert(contract.port_id().clone(), value);
            }
            Ok(SubflowOutcomeFact::Succeeded { outputs })
        }
        "cancelled" => Ok(SubflowOutcomeFact::Cancelled),
        "failed" | "timed_out" | "interrupted" => {
            let checkpoint_rows = sqlx::query_scalar::<_, String>(
                "SELECT transition_key FROM scheduler_checkpoints
                 WHERE run_id=? AND checkpoint_kind='planned_action'
                 ORDER BY scheduler_projection_version DESC,checkpoint_id DESC",
            )
            .bind(child_run_id.as_str())
            .fetch_all(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            let mut failure = None;
            for transition_key in checkpoint_rows {
                let checkpoint = validated_planned_action_checkpoint_sqlite(
                    transaction,
                    child_run_id,
                    &transition_key,
                )
                .await?;
                match checkpoint.intent.action() {
                    SchedulerAction::FailRun { error, .. } => {
                        failure = Some(scheduler_data(TaskFailureFact::new(
                            insight_engine::WorkerFailureClass::SafeBusinessFailure,
                            error
                                .value()
                                .as_object()
                                .and_then(|object| object.get("code"))
                                .and_then(Value::as_str)
                                .unwrap_or("SUBFLOW_SAFE_FAILURE"),
                            Some(error.runtime_value().clone()),
                        ))?);
                        break;
                    }
                    SchedulerAction::FailRunInternal { failure: value, .. } => {
                        failure = Some(value.clone());
                        break;
                    }
                    SchedulerAction::CancelRun { .. } => break,
                    _ => {}
                }
            }
            Ok(SubflowOutcomeFact::Failed {
                failure: failure.unwrap_or(scheduler_data(TaskFailureFact::new(
                    if lifecycle == "timed_out" || lifecycle == "interrupted" {
                        insight_engine::WorkerFailureClass::ControlTermination
                    } else {
                        insight_engine::WorkerFailureClass::InfrastructureFailure
                    },
                    error_code.unwrap_or("SUBFLOW_TERMINAL_FAILURE"),
                    None,
                ))?),
            })
        }
        _ => Err(RepositoryError::invalid_data()),
    }
}

async fn settle_subflow_activation_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    activation_id: &ActivationId,
    outcome: &insight_engine::scheduler::SubflowOutcomeFact,
    transition_key: &TransitionKey,
) -> Result<(), RepositoryError> {
    if matches!(
        outcome,
        insight_engine::scheduler::SubflowOutcomeFact::Succeeded { .. }
    ) {
        return succeed_native_activation(
            transaction,
            run_id,
            activation_id,
            &json!({"kind": "subflow_succeeded"}),
        )
        .await;
    }
    let lifecycle = match outcome {
        insight_engine::scheduler::SubflowOutcomeFact::Succeeded { .. } => unreachable!(),
        insight_engine::scheduler::SubflowOutcomeFact::Failed { .. } => "failed",
        insight_engine::scheduler::SubflowOutcomeFact::Cancelled => "cancelled",
    };
    let reason = match outcome {
        insight_engine::scheduler::SubflowOutcomeFact::Succeeded { .. } => unreachable!(),
        insight_engine::scheduler::SubflowOutcomeFact::Failed { .. } => "failure",
        insight_engine::scheduler::SubflowOutcomeFact::Cancelled => "cancelled",
    };
    let rows = sqlx::query(
        "UPDATE node_activations SET lifecycle=?,termination_intent_reason=?,
            termination_intent_transition_key=?,termination_intent_at=CURRENT_TIMESTAMP,
            effect_evidence='committed',projection_version=projection_version+1,
            updated_at=CURRENT_TIMESTAMP,terminal_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND lifecycle IN ('created','ready','waiting')",
    )
    .bind(lifecycle)
    .bind(reason)
    .bind(transition_key.as_str())
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn settle_subflow_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    parent_run_id: &RunId,
    transition_key: &TransitionKey,
    invocation: &insight_engine::scheduler::SubflowInvocationFact,
    outcome: &insight_engine::scheduler::SubflowOutcomeFact,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        "SELECT i.parent_activation_id,i.node_id,i.occurrence_key,
                i.invocation_scope_instance_id,i.parent_scope_instance_id,i.static_scope_id,
                i.output_contracts,i.invocation_state,c.lifecycle,c.error_code,
                c.output_value_hash AS expected_payload_hash,
                p.payload_id AS payload_id,p.content_hash AS payload_content_hash,
                p.canonical_bytes AS payload_canonical_bytes,p.encoding AS payload_encoding,
                p.inline_value AS payload_inline_value,p.binary_value AS payload_binary_value
         FROM scheduler_subflow_invocations i
         JOIN workflow_runs c ON c.run_id=i.child_run_id
         LEFT JOIN payloads p ON p.run_id=c.run_id AND p.payload_id=c.output_payload_id
         WHERE i.run_id=? AND i.child_run_id=?",
    )
    .bind(parent_run_id.as_str())
    .bind(invocation.child_run_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let stored_occurrence = serde_json::from_str::<insight_engine::LogicalOccurrence>(
        &row.try_get::<String, _>("occurrence_key")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if row
        .try_get::<String, _>("parent_activation_id")
        .map_err(|_| RepositoryError::invalid_data())?
        != invocation.parent_activation_id().as_str()
        || row
            .try_get::<String, _>("node_id")
            .map_err(|_| RepositoryError::invalid_data())?
            != invocation.node_id().as_str()
        || stored_occurrence != *invocation.occurrence()
        || row
            .try_get::<String, _>("invocation_scope_instance_id")
            .map_err(|_| RepositoryError::invalid_data())?
            != invocation.invocation_scope_instance_id().as_str()
        || row
            .try_get::<String, _>("parent_scope_instance_id")
            .map_err(|_| RepositoryError::invalid_data())?
            != invocation.parent_scope_instance_id().as_str()
        || row
            .try_get::<String, _>("static_scope_id")
            .map_err(|_| RepositoryError::invalid_data())?
            != invocation.static_scope_id().as_str()
        || !matches!(
            row.try_get::<String, _>("invocation_state")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_str(),
            "started" | "cancellation_requested"
        )
    {
        return Err(RepositoryError::invalid_data());
    }
    let output_contracts = row
        .try_get::<String, _>("output_contracts")
        .map_err(|_| RepositoryError::invalid_data())?;
    let lifecycle = row
        .try_get::<String, _>("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?;
    let error_code = row
        .try_get::<Option<String>, _>("error_code")
        .map_err(|_| RepositoryError::invalid_data())?;
    let inline_payload = restored_inline_payload_sqlite(&row)?;
    if lifecycle == "succeeded"
        && row
            .try_get::<Option<String>, _>("expected_payload_hash")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            != inline_payload.as_ref().map(|payload| {
                common_contract_adapter::validated_inline_payload_content_hash(payload).as_str()
            })
    {
        return Err(RepositoryError::invalid_data());
    }
    let observed = observed_subflow_outcome_sqlite(
        transaction,
        invocation.child_run_id(),
        &output_contracts,
        &lifecycle,
        error_code.as_deref(),
        inline_payload
            .as_ref()
            .map(common_contract_adapter::validated_inline_payload_canonical),
    )
    .await?;
    if observed != *outcome {
        return Err(RepositoryError::invalid_data());
    }

    if let insight_engine::scheduler::SubflowOutcomeFact::Succeeded { outputs } = outcome {
        for (port_id, runtime_value) in outputs {
            let value_ref = model_data(ValueRef::inline(runtime_value.value().clone()))?;
            upsert_scheduler_value(
                transaction,
                parent_run_id,
                invocation.parent_activation_id(),
                port_id,
                runtime_value,
                &value_ref,
                runtime_value.value_type(),
            )
            .await?;
            upsert_occurrence_value(
                transaction,
                parent_run_id,
                invocation.occurrence(),
                invocation.parent_activation_id(),
                port_id,
                runtime_value,
                &value_ref,
                runtime_value.value_type(),
            )
            .await?;
        }
    }
    settle_subflow_activation_sqlite(
        transaction,
        parent_run_id,
        invocation.parent_activation_id(),
        outcome,
        transition_key,
    )
    .await?;
    let rows = sqlx::query(
        "UPDATE scheduler_subflow_invocations
         SET invocation_state='completed',projection_version=projection_version+1,
             completed_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND child_run_id=?
           AND invocation_scope_instance_id=?
           AND invocation_state IN ('started','cancellation_requested')",
    )
    .bind(parent_run_id.as_str())
    .bind(invocation.child_run_id().as_str())
    .bind(invocation.invocation_scope_instance_id().as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    settle_dynamic_scope(
        transaction,
        parent_run_id,
        invocation.invocation_scope_instance_id(),
        matches!(
            outcome,
            insight_engine::scheduler::SubflowOutcomeFact::Cancelled
        ),
    )
    .await
}

async fn insert_plain_activation_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    activation_id: &ActivationId,
    node_id: &insight_engine::NodeId,
    scope_instance_id: &insight_engine::ScopeInstanceId,
    occurrence: &insight_engine::LogicalOccurrence,
) -> Result<(), RepositoryError> {
    let stable_key = canonical_json(
        &serde_json::to_value(occurrence).map_err(|_| RepositoryError::canonicalization())?,
    )?;
    let effect_id = effect_id_for_activation(activation_id)?;
    sqlx::query(
        "INSERT INTO node_activations (
            run_id,activation_id,scope_instance_id,node_id,stable_activation_key,
            execution_kind,lifecycle,effect_id,effect_idempotency,effect_evidence,
            retry_budget_remaining,projection_version,created_at,updated_at
         ) VALUES (?,?,?,?,?,'scheduler_native','created',?,'idempotent',
                   'not_started',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .bind(scope_instance_id.as_str())
    .bind(node_id.as_str())
    .bind(stable_key)
    .bind(effect_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

fn sqlite_candidate_contract(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<super::ReuseCompatibility, RepositoryError> {
    Ok(super::ReuseCompatibility::new(
        model_data(ContentHash::parse(
            row.try_get::<String, _>("node_config_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?,
        model_data(ContentHash::parse(
            row.try_get::<String, _>("descriptor_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?,
        model_data(ContentHash::parse(
            row.try_get::<String, _>("input_value_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?,
        model_data(ContentHash::parse(
            row.try_get::<String, _>("output_schema_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?,
        model_data(ContentHash::parse(
            row.try_get::<String, _>("effect_policy_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?,
        model_data(ContentHash::parse(
            row.try_get::<String, _>("data_dependencies_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?,
    ))
}

async fn source_dispatch_intent_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    source_run_id: &RunId,
    activation_id: &ActivationId,
) -> Result<Option<SchedulerIntent>, RepositoryError> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT transition_key FROM scheduler_checkpoints
         WHERE run_id=? AND checkpoint_kind='planned_action'
         ORDER BY scheduler_projection_version,checkpoint_id",
    )
    .bind(source_run_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for transition_key in rows {
        let checkpoint =
            validated_planned_action_checkpoint_sqlite(transaction, source_run_id, &transition_key)
                .await?;
        if matches!(
            checkpoint.intent.action(),
            SchedulerAction::DispatchTask {
                activation_id: candidate,
                ..
            } if candidate == activation_id
        ) {
            return Ok(Some(checkpoint.intent));
        }
    }
    Ok(None)
}

async fn copy_scheduler_artifact_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    source_run_id: &RunId,
    target_run_id: &RunId,
    artifact: &insight_engine::ArtifactRef,
) -> Result<bool, RepositoryError> {
    let existing: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM artifacts WHERE run_id=? AND artifact_id=? AND content_hash=?
         AND artifact_state IN ('verified','referenced')",
    )
    .bind(target_run_id.as_str())
    .bind(artifact.artifact_id().as_str())
    .bind(artifact.content_hash().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if existing.is_some() {
        return Ok(true);
    }
    let rows = sqlx::query(
        "INSERT INTO artifacts (
            run_id,artifact_id,content_hash,size_bytes,media_type,storage_uri,
            artifact_state,verified_at,referenced_at,retain_until,deletion_fence,
            deletion_claim_token,deletion_claimed_by,deletion_claim_request_key,
            deletion_claimed_at,deletion_claim_expires_at,created_at)
         SELECT ?,artifact_id,content_hash,size_bytes,media_type,storage_uri,
                'referenced',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,retain_until,NULL,
                NULL,NULL,NULL,NULL,NULL,CURRENT_TIMESTAMP
         FROM artifacts WHERE run_id=? AND artifact_id=? AND content_hash=?
           AND artifact_state IN ('verified','referenced')",
    )
    .bind(target_run_id.as_str())
    .bind(source_run_id.as_str())
    .bind(artifact.artifact_id().as_str())
    .bind(artifact.content_hash().as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    Ok(rows == 1)
}

#[allow(clippy::too_many_arguments)]
async fn reject_reuse_and_admit_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    action: &PlannedSchedulerAction,
    candidate_id: &str,
    expected_candidate_version: u64,
    reason: &str,
    activation_id: &ActivationId,
    node_id: &insight_engine::NodeId,
    scope_instance_id: &insight_engine::ScopeInstanceId,
    occurrence: &insight_engine::LogicalOccurrence,
) -> Result<(), RepositoryError> {
    let rows = sqlx::query(
        "UPDATE run_reuse_candidates SET candidate_state='rejected',
                decision_transition_key=?,rejection_reason=?,
                projection_version=projection_version+1,decided_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND candidate_id=? AND candidate_state='candidate'
           AND projection_version=?",
    )
    .bind(action.transition_key().as_str())
    .bind(reason)
    .bind(action.intent().run_id().as_str())
    .bind(candidate_id)
    .bind(i64_from_u64(expected_candidate_version)?)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    insert_plain_activation_sqlite(
        transaction,
        action.intent().run_id(),
        activation_id,
        node_id,
        scope_instance_id,
        occurrence,
    )
    .await
}

struct ReusedSchedulerValue {
    target_port_id: DataPortId,
    runtime_value: RuntimeValue,
    value_ref: ValueRef,
    declared_type: PlanType,
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn resolve_reuse_at_admission_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    action: &PlannedSchedulerAction,
    activation_id: &ActivationId,
    node_id: &insight_engine::NodeId,
    scope_instance_id: &insight_engine::ScopeInstanceId,
    occurrence: &insight_engine::LogicalOccurrence,
    admission: &insight_engine::scheduler::ReuseAdmissionCandidate,
) -> Result<(), RepositoryError> {
    let run_id = action.intent().run_id();
    let stable_key = canonical_json(
        &serde_json::to_value(occurrence).map_err(|_| RepositoryError::canonicalization())?,
    )?;
    let candidate =
        sqlx::query("SELECT * FROM run_reuse_candidates WHERE run_id=? AND candidate_id=?")
            .bind(run_id.as_str())
            .bind(admission.candidate_id())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(RepositoryError::invalid_data)?;
    if candidate
        .try_get::<String, _>("candidate_state")
        .map_err(|_| RepositoryError::invalid_data())?
        != "candidate"
        || u64_from_i64(
            candidate
                .try_get::<i64, _>("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )? != admission.expected_projection_version()
        || candidate
            .try_get::<String, _>("target_scope_instance_id")
            .map_err(|_| RepositoryError::invalid_data())?
            != scope_instance_id.as_str()
        || candidate
            .try_get::<String, _>("target_node_id")
            .map_err(|_| RepositoryError::invalid_data())?
            != node_id.as_str()
        || candidate
            .try_get::<String, _>("stable_activation_key")
            .map_err(|_| RepositoryError::invalid_data())?
            != stable_key
    {
        return Err(RepositoryError::invalid_data());
    }

    macro_rules! reject {
        ($reason:literal) => {
            return reject_reuse_and_admit_sqlite(
                transaction,
                action,
                admission.candidate_id(),
                admission.expected_projection_version(),
                $reason,
                activation_id,
                node_id,
                scope_instance_id,
                occurrence,
            )
            .await
        };
    }

    let Some(target_contract) = admission.contract() else {
        reject!("node_class_forbidden");
    };
    if !reuse_matches_admission_contract(&sqlite_candidate_contract(&candidate)?, target_contract) {
        reject!("target_contract_mismatch");
    }
    let target_scope = sqlx::query(
        "SELECT static_scope_id,stable_dynamic_key,scope_kind,lifecycle,admission_state
         FROM scope_instances WHERE run_id=? AND scope_instance_id=?",
    )
    .bind(run_id.as_str())
    .bind(scope_instance_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    if target_scope
        .try_get::<String, _>("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?
        != "active"
        || target_scope
            .try_get::<String, _>("admission_state")
            .map_err(|_| RepositoryError::invalid_data())?
            != "open"
    {
        return Err(RepositoryError::invalid_data());
    }

    let source_run_id = model_data(RunId::new(
        candidate
            .try_get::<String, _>("source_run_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let source_activation_id = model_data(ActivationId::new(
        candidate
            .try_get::<String, _>("source_activation_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let lineage = sqlx::query(
        "SELECT l.lineage_kind,l.source_run_id,
                l.source_definition_revision_id,l.source_deployment_revision_id,
                l.source_plan_hash,l.source_binding_hash,
                l.target_definition_revision_id,l.target_deployment_revision_id,
                l.target_plan_hash,l.target_binding_hash,
                sr.definition_revision_id AS actual_source_definition_revision_id,
                sr.deployment_revision_id AS actual_source_deployment_revision_id,
                sr.plan_hash AS actual_source_plan_hash,sr.binding_hash AS actual_source_binding_hash,
                tr.definition_revision_id AS actual_target_definition_revision_id,
                tr.deployment_revision_id AS actual_target_deployment_revision_id,
                tr.plan_hash AS actual_target_plan_hash,tr.binding_hash AS actual_target_binding_hash
         FROM run_recovery_lineage l
         JOIN workflow_runs sr ON sr.run_id=l.source_run_id
         JOIN workflow_runs tr ON tr.run_id=l.run_id
         WHERE l.run_id=?",
    )
    .bind(run_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(lineage) = lineage else {
        reject!("lineage_missing");
    };
    if lineage
        .try_get::<String, _>("source_run_id")
        .map_err(|_| RepositoryError::invalid_data())?
        != source_run_id.as_str()
    {
        reject!("lineage_mismatch");
    }
    for (lineage_column, actual_column) in [
        (
            "source_definition_revision_id",
            "actual_source_definition_revision_id",
        ),
        (
            "source_deployment_revision_id",
            "actual_source_deployment_revision_id",
        ),
        ("source_plan_hash", "actual_source_plan_hash"),
        ("source_binding_hash", "actual_source_binding_hash"),
        (
            "target_definition_revision_id",
            "actual_target_definition_revision_id",
        ),
        (
            "target_deployment_revision_id",
            "actual_target_deployment_revision_id",
        ),
        ("target_plan_hash", "actual_target_plan_hash"),
        ("target_binding_hash", "actual_target_binding_hash"),
    ] {
        if lineage
            .try_get::<String, _>(lineage_column)
            .map_err(|_| RepositoryError::invalid_data())?
            != lineage
                .try_get::<String, _>(actual_column)
                .map_err(|_| RepositoryError::invalid_data())?
        {
            reject!("revision_provenance_mismatch");
        }
    }
    for (candidate_column, target_column) in [
        (
            "definition_revision_id",
            "actual_target_definition_revision_id",
        ),
        (
            "deployment_revision_id",
            "actual_target_deployment_revision_id",
        ),
        ("plan_hash", "actual_target_plan_hash"),
        ("binding_hash", "actual_target_binding_hash"),
    ] {
        if candidate
            .try_get::<String, _>(candidate_column)
            .map_err(|_| RepositoryError::invalid_data())?
            != lineage
                .try_get::<String, _>(target_column)
                .map_err(|_| RepositoryError::invalid_data())?
        {
            reject!("target_revision_mismatch");
        }
    }

    let source = sqlx::query(
        "SELECT a.*,s.static_scope_id,s.stable_dynamic_key,s.scope_kind
         FROM node_activations a
         JOIN scope_instances s ON s.run_id=a.run_id AND s.scope_instance_id=a.scope_instance_id
         WHERE a.run_id=? AND a.activation_id=?",
    )
    .bind(source_run_id.as_str())
    .bind(source_activation_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(source) = source else {
        reject!("source_activation_missing");
    };
    if source
        .try_get::<String, _>("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?
        != "succeeded"
        || source
            .try_get::<String, _>("execution_kind")
            .map_err(|_| RepositoryError::invalid_data())?
            != "worker"
        || source
            .try_get::<String, _>("effect_evidence")
            .map_err(|_| RepositoryError::invalid_data())?
            != "committed"
        || source
            .try_get::<String, _>("effect_id")
            .map_err(|_| RepositoryError::invalid_data())?
            != candidate
                .try_get::<String, _>("inherited_effect_id")
                .map_err(|_| RepositoryError::invalid_data())?
        || source
            .try_get::<Option<String>, _>("output_value_hash")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            != Some(
                candidate
                    .try_get::<String, _>("output_value_hash")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .as_str(),
            )
        || source
            .try_get::<String, _>("stable_activation_key")
            .map_err(|_| RepositoryError::invalid_data())?
            != stable_key
        || !super::sqlite_control::source_output_exists(transaction, &source_run_id, &source)
            .await?
    {
        reject!("source_output_ineligible");
    }
    for column in ["static_scope_id", "stable_dynamic_key", "scope_kind"] {
        let source_value = source
            .try_get::<Option<String>, _>(column)
            .map_err(|_| RepositoryError::invalid_data())?;
        let target_value = target_scope
            .try_get::<Option<String>, _>(column)
            .map_err(|_| RepositoryError::invalid_data())?;
        if source_value != target_value {
            reject!("scope_contract_mismatch");
        }
    }

    let provenance_value = serde_json::from_str::<Value>(
        &candidate
            .try_get::<String, _>("source_control_provenance")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let (control_provenance, data_dependencies) =
        decode_durable_reuse_provenance(&provenance_value)?;
    if control_provenance.run_id() != &source_run_id
        || control_provenance.source_activation_id() != &source_activation_id
    {
        reject!("control_provenance_mismatch");
    }
    let provenance_exists: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM control_tokens WHERE run_id=? AND source_activation_id=?
           AND source_port_id=? AND current_scope_instance_id=?",
    )
    .bind(source_run_id.as_str())
    .bind(source_activation_id.as_str())
    .bind(control_provenance.source_port().as_str())
    .bind(control_provenance.scope_instance_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if provenance_exists.is_none() {
        reject!("control_provenance_missing");
    }
    for dependency in &data_dependencies {
        let rows = sqlx::query(
            "SELECT c.candidate_state,c.materialized_activation_id,
                    a.lifecycle,a.reused_from_run_id,a.reused_from_activation_id
             FROM run_reuse_candidates c
             LEFT JOIN node_activations a
               ON a.run_id=c.run_id AND a.activation_id=c.materialized_activation_id
             WHERE c.run_id=? AND c.source_run_id=? AND c.source_activation_id=?",
        )
        .bind(run_id.as_str())
        .bind(source_run_id.as_str())
        .bind(dependency.as_str())
        .fetch_all(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if rows.len() != 1 {
            reject!("dependency_candidate_missing");
        }
        let row = &rows[0];
        if row
            .try_get::<String, _>("candidate_state")
            .map_err(|_| RepositoryError::invalid_data())?
            != "materialized"
            || row
                .try_get::<Option<String>, _>("lifecycle")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_deref()
                != Some("succeeded")
            || row
                .try_get::<Option<String>, _>("reused_from_run_id")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_deref()
                != Some(source_run_id.as_str())
            || row
                .try_get::<Option<String>, _>("reused_from_activation_id")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_deref()
                != Some(dependency.as_str())
        {
            reject!("dependency_closure_open");
        }
    }

    let Some(source_intent) =
        source_dispatch_intent_sqlite(transaction, &source_run_id, &source_activation_id).await?
    else {
        reject!("source_task_contract_missing");
    };
    let SchedulerAction::DispatchTask {
        task_kind,
        implementation,
        descriptor_version,
        worker_version,
        effect_policy,
        deployment_binding,
        public_configuration,
        secret_configuration,
        inputs,
        outputs,
        ..
    } = source_intent.action()
    else {
        unreachable!("source_dispatch_intent_sqlite only returns dispatch intents")
    };
    let lineage_kind = lineage
        .try_get::<String, _>("lineage_kind")
        .map_err(|_| RepositoryError::invalid_data())?;
    let source_node_id = model_data(insight_engine::NodeId::new(
        source
            .try_get::<String, _>("node_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let port_mapping = if lineage_kind == "migrate" {
        let encoded = sqlx::query_scalar::<_, String>(
            "SELECT mapping_contracts FROM run_migration_intents
             WHERE run_id=? AND target_run_id=? AND intent_state='completed'",
        )
        .bind(source_run_id.as_str())
        .bind(run_id.as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(encoded) = encoded else {
            reject!("migration_mapping_missing");
        };
        let mappings = serde_json::from_str::<Vec<super::MigrationMappingCompatibility>>(&encoded)
            .map_err(|_| RepositoryError::invalid_data())?;
        let Some(mapping) = mappings.iter().find(|mapping| {
            mapping.source().source_node_id() == &source_node_id
                && mapping.source().target_node_id() == node_id
        }) else {
            reject!("migration_node_mapping_missing");
        };
        if mapping.target().output_schema_hash() != target_contract.output_schema_hash()
            || mapping.target().effect_policy_hash() != target_contract.effect_policy_hash()
        {
            reject!("migration_contract_mismatch");
        }
        Some(mapping.target().port_mapping().clone())
    } else {
        if !matches!(
            lineage_kind.as_str(),
            "redrive" | "fork" | "continue_as_new"
        ) || &source_node_id != node_id
        {
            reject!("node_mapping_mismatch");
        }
        None
    };
    let source_contract = insight_engine::scheduler::ReuseAdmissionContract::from_task_parts(
        *task_kind,
        implementation,
        descriptor_version,
        worker_version,
        effect_policy,
        deployment_binding,
        public_configuration,
        secret_configuration,
        inputs,
        outputs,
        port_mapping.as_ref(),
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if &source_contract != target_contract {
        reject!("source_target_contract_mismatch");
    }

    let source_values = sqlx::query(
        "SELECT occurrence_key,port_id,runtime_value,value_ref,declared_type,
                storage_kind,payload_id,artifact_id,content_hash
         FROM scheduler_occurrence_values
         WHERE run_id=? AND owner_activation_id=? AND occurrence_key=?
         ORDER BY port_id",
    )
    .bind(source_run_id.as_str())
    .bind(source_activation_id.as_str())
    .bind(&stable_key)
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let output_contracts = outputs
        .iter()
        .map(|output| (output.port_id(), output))
        .collect::<BTreeMap<_, _>>();
    let mut seen_outputs = std::collections::BTreeSet::new();
    let mut copied_values = Vec::with_capacity(source_values.len());
    for row in source_values {
        let source_port = DataPortId::new(
            row.try_get::<String, _>("port_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        if !output_contracts.contains_key(&source_port) || !seen_outputs.insert(source_port.clone())
        {
            reject!("source_output_contract_mismatch");
        }
        let target_port = match &port_mapping {
            Some(mapping) => match mapping.get(&source_port) {
                Some(port) => port.clone(),
                None => reject!("migration_port_mapping_missing"),
            },
            None => source_port,
        };
        let runtime_value = serde_json::from_str::<RuntimeValue>(
            &row.try_get::<String, _>("runtime_value")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let value_ref = serde_json::from_str::<ValueRef>(
            &row.try_get::<String, _>("value_ref")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let declared_type = serde_json::from_str::<PlanType>(
            &row.try_get::<String, _>("declared_type")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        if !runtime_value.matches(&declared_type)
            || validate_runtime_value_ref(&runtime_value, &value_ref).is_err()
            || value_ref.content_hash().as_str()
                != row
                    .try_get::<String, _>("content_hash")
                    .map_err(|_| RepositoryError::invalid_data())?
        {
            reject!("source_value_hash_mismatch");
        }
        match &value_ref {
            ValueRef::Inline(_) => {
                let source_payload_id = row
                    .try_get::<Option<String>, _>("payload_id")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .ok_or_else(RepositoryError::invalid_data)?;
                let payload = sqlx::query(
                    "SELECT content_hash,canonical_bytes,encoding,inline_value,binary_value
                     FROM payloads WHERE run_id=? AND payload_id=?",
                )
                .bind(source_run_id.as_str())
                .bind(&source_payload_id)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(RepositoryError::storage)?;
                let Some(payload) = payload else {
                    reject!("source_value_payload_missing");
                };
                let encoded = payload
                    .try_get::<Option<String>, _>("inline_value")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .ok_or_else(RepositoryError::invalid_data)?;
                let stored_value =
                    serde_json::from_str(&encoded).map_err(|_| RepositoryError::invalid_data())?;
                let validated = validate_inline_payload(
                    &source_payload_id,
                    &payload
                        .try_get::<String, _>("content_hash")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    payload
                        .try_get::<i64, _>("canonical_bytes")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    &payload
                        .try_get::<String, _>("encoding")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    stored_value,
                    Some(&encoded),
                    payload
                        .try_get::<Option<Vec<u8>>, _>("binary_value")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .is_none(),
                )?;
                if common_contract_adapter::validated_inline_payload_content_hash(&validated)
                    != value_ref.content_hash()
                    || common_contract_adapter::validated_inline_payload_value(&validated)
                        != runtime_value.value()
                {
                    reject!("source_value_payload_mismatch");
                }
            }
            ValueRef::Artifact(artifact) => {
                let valid: Option<i64> = sqlx::query_scalar(
                    "SELECT 1 FROM artifacts WHERE run_id=? AND artifact_id=? AND content_hash=?
                       AND artifact_state IN ('verified','referenced')",
                )
                .bind(source_run_id.as_str())
                .bind(artifact.artifact_id().as_str())
                .bind(artifact.content_hash().as_str())
                .fetch_optional(&mut **transaction)
                .await
                .map_err(RepositoryError::storage)?;
                if valid.is_none() {
                    reject!("source_value_artifact_missing");
                }
            }
        }
        copied_values.push(ReusedSchedulerValue {
            target_port_id: target_port,
            runtime_value,
            value_ref,
            declared_type,
        });
    }
    if outputs
        .iter()
        .any(|output| output.required() && !seen_outputs.contains(output.port_id()))
    {
        reject!("required_source_output_missing");
    }

    let copied =
        super::sqlite_control::copy_source_output(transaction, &source_run_id, run_id, &source)
            .await?;
    sqlx::query(
        "INSERT INTO node_activations (
            run_id,activation_id,scope_instance_id,node_id,stable_activation_key,
            execution_kind,lifecycle,effect_id,effect_idempotency,effect_evidence,
            last_attempt_no,last_lease_epoch,current_attempt_no,current_lease_epoch,
            current_fencing_token,retry_budget_remaining,pending_retry_timer_id,
            wait_registration_transition_key,termination_intent_reason,
            termination_intent_transition_key,termination_intent_at,output_payload_id,
            output_artifact_id,output_value_hash,winning_attempt_no,reused_from_run_id,
            reused_from_activation_id,projection_version,created_at,updated_at,terminal_at)
         VALUES (?,?,?,?,?,?,'succeeded',?,?,?,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,
                 NULL,NULL,?,?,?,NULL,?,?,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .bind(scope_instance_id.as_str())
    .bind(node_id.as_str())
    .bind(&stable_key)
    .bind(
        source
            .try_get::<String, _>("execution_kind")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(
        candidate
            .try_get::<String, _>("inherited_effect_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(
        source
            .try_get::<String, _>("effect_idempotency")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(
        source
            .try_get::<String, _>("effect_evidence")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(copied.payload_id.as_deref())
    .bind(copied.artifact_id.as_deref())
    .bind(&copied.value_hash)
    .bind(source_run_id.as_str())
    .bind(source_activation_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for value in copied_values {
        if let ValueRef::Artifact(artifact) = &value.value_ref {
            if !copy_scheduler_artifact_sqlite(transaction, &source_run_id, run_id, artifact)
                .await?
            {
                return Err(RepositoryError::invalid_data());
            }
        }
        upsert_scheduler_value(
            transaction,
            run_id,
            activation_id,
            &value.target_port_id,
            &value.runtime_value,
            &value.value_ref,
            &value.declared_type,
        )
        .await?;
        upsert_occurrence_value(
            transaction,
            run_id,
            occurrence,
            activation_id,
            &value.target_port_id,
            &value.runtime_value,
            &value.value_ref,
            &value.declared_type,
        )
        .await?;
    }
    let updated = sqlx::query(
        "UPDATE run_reuse_candidates SET candidate_state='materialized',
                materialized_activation_id=?,decision_transition_key=?,rejection_reason=NULL,
                projection_version=projection_version+1,decided_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND candidate_id=? AND candidate_state='candidate'
           AND projection_version=?",
    )
    .bind(activation_id.as_str())
    .bind(action.transition_key().as_str())
    .bind(run_id.as_str())
    .bind(admission.candidate_id())
    .bind(i64_from_u64(admission.expected_projection_version())?)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if updated != 1 {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn apply_scheduler_action(
    transaction: &mut Transaction<'_, Sqlite>,
    action: &PlannedSchedulerAction,
    event_id_value: &str,
    event_seq: u64,
    next_version: u64,
) -> Result<(), RepositoryError> {
    let run_id = action.intent().run_id();
    match action.intent().action() {
        SchedulerAction::FailRunPlanning { failure } => {
            fail_run_planning_sqlite(
                transaction,
                action,
                event_id_value,
                event_seq,
                next_version,
                *failure,
            )
            .await?;
        }
        SchedulerAction::AdmitActivation {
            activation_id,
            node_id,
            scope_instance_id,
            occurrence,
            reuse_candidate,
        } => {
            if let Some(reuse_candidate) = reuse_candidate {
                resolve_reuse_at_admission_sqlite(
                    transaction,
                    action,
                    activation_id,
                    node_id,
                    scope_instance_id,
                    occurrence,
                    reuse_candidate,
                )
                .await?;
            } else {
                insert_plain_activation_sqlite(
                    transaction,
                    run_id,
                    activation_id,
                    node_id,
                    scope_instance_id,
                    occurrence,
                )
                .await?;
            }
        }
        SchedulerAction::ConsumeToken {
            token_id,
            target_activation_id,
            input_port,
        } => {
            let rows = sqlx::query(
                "UPDATE control_tokens SET token_state='consumed',current_port_id=?,
                    consumed_by_activation_id=?,consumed_by_transition_key=?,
                    consumed_at=CURRENT_TIMESTAMP,projection_version=projection_version+1
                 WHERE run_id=? AND token_id=? AND token_state='available'",
            )
            .bind(input_port.as_str())
            .bind(target_activation_id.as_str())
            .bind(action.transition_key().as_str())
            .bind(run_id.as_str())
            .bind(token_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                return Err(RepositoryError::invalid_data());
            }
        }
        SchedulerAction::EmitToken {
            token_id,
            source_activation_id,
            output_port,
            scope_instance_id,
        } => {
            sqlx::query(
                "INSERT INTO control_tokens (
                    run_id,token_id,current_scope_instance_id,current_port_id,
                    source_activation_id,source_port_id,emission_slot,
                    emitted_by_transition_key,provenance_frames,branch_activation_id,
                    selected_branch_port_id,fork_group_id,fork_leg_id,token_state,
                    consumed_by_activation_id,consumed_by_transition_key,consumed_at,
                    revoked_by_transition_key,revoked_at,projection_version,created_at
                 ) VALUES (?,?,?,?,?,?,?,?, '[]',NULL,NULL,NULL,NULL,'available',
                           NULL,NULL,NULL,NULL,NULL,0,CURRENT_TIMESTAMP)",
            )
            .bind(run_id.as_str())
            .bind(token_id.as_str())
            .bind(scope_instance_id.as_str())
            .bind(output_port.as_str())
            .bind(source_activation_id.as_str())
            .bind(output_port.as_str())
            .bind(output_port.as_str())
            .bind(action.transition_key().as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
        }
        SchedulerAction::DispatchTask {
            task_id,
            effect_id,
            activation_id,
            ..
        } => {
            let attempt_no = AttemptNo::FIRST;
            let lease_epoch = LeaseEpoch::FIRST;
            let fence = fencing_token(action.transition_key());
            let request = insight_engine::worker::TaskExecutionRequest::from_scheduler_intent(
                action.intent(),
            )
            .map_err(|_| RepositoryError::invalid_data())?;
            let retry_budget = request.effect_policy().max_attempts().saturating_sub(1);
            let effect_idempotency =
                effect_idempotency_str(request.effect_policy().effect_idempotency());
            let envelope = DurableTaskExecutionRequest::new(
                request,
                attempt_no,
                lease_epoch,
                fence.clone(),
                next_version,
            )?;
            let envelope = canonical_json(
                &serde_json::to_value(&envelope)
                    .map_err(|_| RepositoryError::canonicalization())?,
            )?;
            let activation_rows = sqlx::query(
                "UPDATE node_activations SET execution_kind='worker',lifecycle='leased',
                    effect_id=?,last_attempt_no=1,last_lease_epoch=1,current_attempt_no=1,
                    current_lease_epoch=1,current_fencing_token=?,retry_budget_remaining=?,
                    effect_idempotency=?,
                    projection_version=projection_version+1,
                    updated_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND activation_id=? AND lifecycle='created'",
            )
            .bind(effect_id.as_str())
            .bind(&fence)
            .bind(i64::from(retry_budget))
            .bind(effect_idempotency)
            .bind(run_id.as_str())
            .bind(activation_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if activation_rows != 1 {
                return Err(RepositoryError::invalid_data());
            }
            sqlx::query(
                "INSERT INTO node_attempts (
                    run_id,activation_id,attempt_no,lease_epoch,fencing_token,effect_id,
                    lifecycle,effect_evidence,worker_id,lease_expires_at,heartbeat_at,
                    projection_version,created_at
                 ) VALUES (?,?,1,1,?,?,'leased','not_started','scheduler-outbox',
                           datetime('now','+1 day'),CURRENT_TIMESTAMP,0,CURRENT_TIMESTAMP)",
            )
            .bind(run_id.as_str())
            .bind(activation_id.as_str())
            .bind(&fence)
            .bind(effect_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            sqlx::query(
                "INSERT INTO task_outbox (
                    run_id,task_id,activation_id,attempt_no,lease_epoch,fencing_token,
                    effect_id,created_by_transition_key,task_state,task_envelope,
                    available_at,publish_attempts,projection_version,created_at
                 ) VALUES (?,?,?,1,1,?, ?,?,'pending',?,CURRENT_TIMESTAMP,0,0,CURRENT_TIMESTAMP)",
            )
            .bind(run_id.as_str())
            .bind(task_id.as_str())
            .bind(activation_id.as_str())
            .bind(&fence)
            .bind(effect_id.as_str())
            .bind(action.transition_key().as_str())
            .bind(envelope)
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
        }
        SchedulerAction::CommitNativeOutput {
            activation_id,
            occurrence,
            output,
            ..
        } => {
            let encoded =
                serde_json::to_value(output).map_err(|_| RepositoryError::canonicalization())?;
            succeed_native_activation(transaction, run_id, activation_id, &encoded).await?;
            let insight_engine::NativeOutput::Values { values } = output;
            for (port_id, runtime_value) in values {
                let value_ref = model_data(ValueRef::inline(runtime_value.value().clone()))?;
                upsert_scheduler_value(
                    transaction,
                    run_id,
                    activation_id,
                    port_id,
                    runtime_value,
                    &value_ref,
                    runtime_value.value_type(),
                )
                .await?;
                upsert_occurrence_value(
                    transaction,
                    run_id,
                    occurrence,
                    activation_id,
                    port_id,
                    runtime_value,
                    &value_ref,
                    runtime_value.value_type(),
                )
                .await?;
            }
        }
        SchedulerAction::SelectBranchAndAdmit { selection } => {
            let stable_branch_key = canonical_json(
                &serde_json::to_value(selection.occurrence())
                    .map_err(|_| RepositoryError::canonicalization())?,
            )?;
            let branch_identity = sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM node_activations
                 WHERE run_id=? AND activation_id=? AND node_id=?
                   AND scope_instance_id=? AND stable_activation_key=?
                   AND execution_kind='scheduler_native' AND lifecycle='created'",
            )
            .bind(run_id.as_str())
            .bind(selection.branch_activation_id().as_str())
            .bind(selection.branch_node_id().as_str())
            .bind(selection.branch_scope_instance_id().as_str())
            .bind(stable_branch_key)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if branch_identity.is_none() {
                return Err(RepositoryError::invalid_data());
            }
            let encoded =
                serde_json::to_value(selection).map_err(|_| RepositoryError::canonicalization())?;
            succeed_native_activation(
                transaction,
                run_id,
                selection.branch_activation_id(),
                &encoded,
            )
            .await?;

            let successor = selection.successor();
            let stable_successor_key = canonical_json(
                &serde_json::to_value(successor.occurrence())
                    .map_err(|_| RepositoryError::canonicalization())?,
            )?;
            let successor_effect = effect_id_for_activation(successor.activation_id())?;
            sqlx::query(
                "INSERT INTO node_activations (
                    run_id,activation_id,scope_instance_id,node_id,stable_activation_key,
                    execution_kind,lifecycle,effect_id,effect_idempotency,effect_evidence,
                    retry_budget_remaining,projection_version,created_at,updated_at
                 ) VALUES (?,?,?,?,?,'scheduler_native','created',?,'idempotent',
                           'not_started',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            )
            .bind(run_id.as_str())
            .bind(successor.activation_id().as_str())
            .bind(successor.scope_instance_id().as_str())
            .bind(successor.node_id().as_str())
            .bind(stable_successor_key)
            .bind(successor_effect.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;

            let selected_port = model_data(insight_engine::PortId::new(
                selection.output_port().as_str().to_owned(),
            ))?;
            let frames = canonical_json(
                &serde_json::to_value(vec![ExecutionControlFrame::Branch {
                    branch_activation_id: selection.branch_activation_id().clone(),
                    selected_port,
                    scope_instance_id: selection.branch_scope_instance_id().clone(),
                }])
                .map_err(|_| RepositoryError::canonicalization())?,
            )?;
            sqlx::query(
                "INSERT INTO control_tokens (
                    run_id,token_id,current_scope_instance_id,current_port_id,
                    source_activation_id,source_port_id,emission_slot,emitted_by_transition_key,
                    provenance_frames,branch_activation_id,selected_branch_port_id,
                    fork_group_id,fork_leg_id,token_state,consumed_by_activation_id,
                    consumed_by_transition_key,consumed_at,revoked_by_transition_key,revoked_at,
                    projection_version,created_at
                 ) VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,'consumed',?,?,CURRENT_TIMESTAMP,
                           NULL,NULL,1,CURRENT_TIMESTAMP)",
            )
            .bind(run_id.as_str())
            .bind(selection.token_id().as_str())
            .bind(selection.branch_scope_instance_id().as_str())
            .bind(successor.input_port().as_str())
            .bind(selection.branch_activation_id().as_str())
            .bind(selection.output_port().as_str())
            .bind(selection.output_port().as_str())
            .bind(action.transition_key().as_str())
            .bind(frames)
            .bind(selection.branch_activation_id().as_str())
            .bind(selection.output_port().as_str())
            .bind(successor.activation_id().as_str())
            .bind(action.transition_key().as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
        }
        SchedulerAction::CommitOccurrenceValues {
            activation_id,
            occurrence,
            values,
            ..
        } => {
            for (port_id, runtime_value) in values {
                let value_ref = model_data(ValueRef::inline(runtime_value.value().clone()))?;
                upsert_scheduler_value(
                    transaction,
                    run_id,
                    activation_id,
                    port_id,
                    runtime_value,
                    &value_ref,
                    runtime_value.value_type(),
                )
                .await?;
                upsert_occurrence_value(
                    transaction,
                    run_id,
                    occurrence,
                    activation_id,
                    port_id,
                    runtime_value,
                    &value_ref,
                    runtime_value.value_type(),
                )
                .await?;
            }
        }
        SchedulerAction::CompleteRun {
            activation_id,
            output,
        } => {
            succeed_native_activation(transaction, run_id, activation_id, output.value()).await?;
            settle_dynamic_scope(
                transaction,
                run_id,
                &insight_engine::ScopeInstanceId::root(),
                false,
            )
            .await?;
            let (payload_id, value_hash) =
                insert_or_get_payload(transaction, run_id, output.value()).await?;
            let public_id = insert_public_terminal(
                transaction,
                run_id,
                action.transition_key(),
                event_id_value,
                event_seq,
                PublicEventPayload::RunCompleted,
            )
            .await?;
            let rows = sqlx::query(
                "UPDATE workflow_runs SET lifecycle='succeeded',admission_state='closed',
                    termination_intent_reason=NULL,termination_intent_transition_key=NULL,
                    termination_intent_at=NULL,output_payload_id=?,output_artifact_id=NULL,
                    output_value_hash=?,error_code=NULL,terminal_event_id=?,
                    terminal_public_event_id=?,projection_version=?,updated_at=CURRENT_TIMESTAMP,
                    terminal_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND projection_version=?",
            )
            .bind(payload_id)
            .bind(value_hash)
            .bind(event_id_value)
            .bind(public_id)
            .bind(i64_from_u64(next_version)?)
            .bind(run_id.as_str())
            .bind(i64_from_u64(
                action.precondition().expected_projection_version(),
            )?)
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                return Err(RepositoryError::invalid_data());
            }
        }
        SchedulerAction::FailRun {
            activation_id,
            error,
        } => {
            fail_activation_for_run_terminal(
                transaction,
                run_id,
                activation_id,
                action.transition_key(),
            )
            .await?;
            settle_dynamic_scope(
                transaction,
                run_id,
                &insight_engine::ScopeInstanceId::root(),
                false,
            )
            .await?;
            let authored_code = error
                .value()
                .as_object()
                .and_then(|value| value.get("code"))
                .and_then(Value::as_str)
                .ok_or_else(RepositoryError::invalid_data)?;
            let public_code = model_data(insight_engine::PublicErrorCode::new(authored_code))?;
            let public_id = insert_public_terminal(
                transaction,
                run_id,
                action.transition_key(),
                event_id_value,
                event_seq,
                PublicEventPayload::RunFailed {
                    failure: insight_engine::PublicFailureSummary {
                        kind: insight_engine::PublicFailureKind::Workflow,
                        code: public_code.clone(),
                    },
                },
            )
            .await?;
            let rows = sqlx::query(
                "UPDATE workflow_runs SET lifecycle='failed',admission_state='closed',
                    termination_intent_reason='failure',termination_intent_transition_key=?,
                    termination_intent_at=CURRENT_TIMESTAMP,output_payload_id=NULL,
                    output_artifact_id=NULL,output_value_hash=NULL,error_code=?,terminal_event_id=?,
                    terminal_public_event_id=?,projection_version=?,updated_at=CURRENT_TIMESTAMP,
                    terminal_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND projection_version=?",
            )
            .bind(action.transition_key().as_str())
            .bind(public_code.as_str())
            .bind(event_id_value)
            .bind(public_id)
            .bind(i64_from_u64(next_version)?)
            .bind(run_id.as_str())
            .bind(i64_from_u64(
                action.precondition().expected_projection_version(),
            )?)
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                return Err(RepositoryError::invalid_data());
            }
        }
        SchedulerAction::FailRunInternal {
            activation_id,
            failure,
        } => {
            fail_activation_for_run_terminal(
                transaction,
                run_id,
                activation_id,
                action.transition_key(),
            )
            .await?;
            settle_dynamic_scope(
                transaction,
                run_id,
                &insight_engine::ScopeInstanceId::root(),
                false,
            )
            .await?;
            let public_kind = match failure.class() {
                insight_engine::WorkerFailureClass::InfrastructureFailure => {
                    insight_engine::PublicFailureKind::Infrastructure
                }
                insight_engine::WorkerFailureClass::ControlTermination => {
                    insight_engine::PublicFailureKind::Stop
                }
                _ => insight_engine::PublicFailureKind::Operation,
            };
            let public_id = insert_public_terminal(
                transaction,
                run_id,
                action.transition_key(),
                event_id_value,
                event_seq,
                PublicEventPayload::RunFailed {
                    failure: insight_engine::PublicFailureSummary {
                        kind: public_kind,
                        code: model_data(insight_engine::PublicErrorCode::new(
                            "SCHEDULER_INTERNAL_FAILURE",
                        ))?,
                    },
                },
            )
            .await?;
            let rows = sqlx::query(
                "UPDATE workflow_runs SET lifecycle='failed',admission_state='closed',
                    termination_intent_reason='failure',termination_intent_transition_key=?,
                    termination_intent_at=CURRENT_TIMESTAMP,output_payload_id=NULL,
                    output_artifact_id=NULL,output_value_hash=NULL,error_code=?,terminal_event_id=?,
                    terminal_public_event_id=?,projection_version=?,updated_at=CURRENT_TIMESTAMP,
                    terminal_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND projection_version=?",
            )
            .bind(action.transition_key().as_str())
            .bind(failure.code())
            .bind(event_id_value)
            .bind(public_id)
            .bind(i64_from_u64(next_version)?)
            .bind(run_id.as_str())
            .bind(i64_from_u64(
                action.precondition().expected_projection_version(),
            )?)
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                return Err(RepositoryError::invalid_data());
            }
        }
        SchedulerAction::CancelRun {
            activation_id,
            reason,
        } => {
            let reason_value = termination_reason_as_str(*reason);
            let (terminal_lifecycle, public_code, public_payload) = match reason {
                TerminationReason::Failure => (
                    RunLifecycle::Failed,
                    "RUN_TERMINATED_FAILURE",
                    PublicEventPayload::RunFailed {
                        failure: insight_engine::PublicFailureSummary {
                            kind: insight_engine::PublicFailureKind::Infrastructure,
                            code: model_data(insight_engine::PublicErrorCode::new(
                                "RUN_TERMINATED_FAILURE",
                            ))?,
                        },
                    },
                ),
                TerminationReason::Cancelled => (
                    RunLifecycle::Cancelled,
                    "SCHEDULER_CANCELLED",
                    PublicEventPayload::RunCancelled {
                        failure: insight_engine::PublicFailureSummary {
                            kind: insight_engine::PublicFailureKind::Stop,
                            code: model_data(insight_engine::PublicErrorCode::new(
                                "SCHEDULER_CANCELLED",
                            ))?,
                        },
                    },
                ),
                TerminationReason::Interrupted => (
                    RunLifecycle::Interrupted,
                    "RUN_INTERRUPTED",
                    PublicEventPayload::RunInterrupted {
                        failure: insight_engine::PublicFailureSummary {
                            kind: insight_engine::PublicFailureKind::Stop,
                            code: model_data(insight_engine::PublicErrorCode::new(
                                "RUN_INTERRUPTED",
                            ))?,
                        },
                    },
                ),
                TerminationReason::TimedOut => (
                    RunLifecycle::TimedOut,
                    "RUN_TIMEOUT",
                    PublicEventPayload::RunFailed {
                        failure: insight_engine::PublicFailureSummary {
                            kind: insight_engine::PublicFailureKind::Stop,
                            code: model_data(insight_engine::PublicErrorCode::new("RUN_TIMEOUT"))?,
                        },
                    },
                ),
            };
            let scopes = sqlx::query_scalar::<_, String>(
                "SELECT scope_instance_id FROM scope_instances
                 WHERE run_id=? AND lifecycle IN ('active','settling')
                 ORDER BY is_root ASC,scope_instance_id",
            )
            .bind(run_id.as_str())
            .fetch_all(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            for scope in scopes {
                let scope = model_data(insight_engine::ScopeInstanceId::new(scope))?;
                cancel_and_drain_scope(
                    transaction,
                    run_id,
                    &scope,
                    action.transition_key(),
                    event_id_value,
                )
                .await?;
            }
            sqlx::query(
                "UPDATE task_outbox SET task_state='dead',claimed_by=NULL,claim_token=NULL,
                    claim_expires_at=NULL,claim_mode=NULL,last_error_code='SCHEDULER_CANCELLED',
                    projection_version=projection_version+1
                 WHERE run_id=? AND activation_id=? AND task_state IN ('pending','claimed')",
            )
            .bind(run_id.as_str())
            .bind(activation_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            sqlx::query(
                "UPDATE node_attempts SET lifecycle='cancelled',failure_code='SCHEDULER_CANCELLED',
                    completion_transition_key=?,terminal_event_id=?,
                    projection_version=projection_version+1,terminal_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND activation_id=?
                   AND lifecycle IN ('created','leased','running')",
            )
            .bind(action.transition_key().as_str())
            .bind(event_id_value)
            .bind(run_id.as_str())
            .bind(activation_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            sqlx::query(
                "UPDATE timers SET timer_state='cancelled',fired_at=CURRENT_TIMESTAMP,
                    projection_version=projection_version+1
                 WHERE run_id=? AND activation_id=? AND timer_state='scheduled'",
            )
            .bind(run_id.as_str())
            .bind(activation_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            sqlx::query(
                "UPDATE node_activations SET lifecycle='cancelled',
                    termination_intent_reason='cancelled',termination_intent_transition_key=?,
                    termination_intent_at=CURRENT_TIMESTAMP,current_attempt_no=NULL,
                    current_lease_epoch=NULL,current_fencing_token=NULL,pending_retry_timer_id=NULL,
                    projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP,
                    terminal_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND activation_id=?
                   AND lifecycle IN ('created','ready','leased','running','retry_wait','waiting','terminating')",
            )
            .bind(action.transition_key().as_str())
            .bind(run_id.as_str())
            .bind(activation_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            let active_rows = sqlx::query_scalar::<_, i64>(
                "SELECT
                    (SELECT COUNT(*) FROM task_outbox WHERE run_id=?
                       AND task_state IN ('pending','claimed','published'))
                  + (SELECT COUNT(*) FROM node_attempts WHERE run_id=?
                       AND lifecycle IN ('created','leased','running'))
                  + (SELECT COUNT(*) FROM timers WHERE run_id=? AND timer_state='scheduled')
                  + (SELECT COUNT(*) FROM node_activations WHERE run_id=?
                       AND lifecycle IN ('created','ready','leased','running','retry_wait','waiting','terminating'))
                  + (SELECT COUNT(*) FROM scope_instances WHERE run_id=?
                       AND lifecycle IN ('active','settling'))
                  + (SELECT COUNT(*) FROM scheduler_subflow_invocations i
                       JOIN workflow_runs c ON c.run_id=i.child_run_id
                       WHERE i.run_id=? AND c.lifecycle NOT IN
                           ('succeeded','failed','cancelled','interrupted','timed_out'))",
            )
            .bind(run_id.as_str())
            .bind(run_id.as_str())
            .bind(run_id.as_str())
            .bind(run_id.as_str())
            .bind(run_id.as_str())
            .bind(run_id.as_str())
            .fetch_one(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if active_rows != 0 {
                return Err(RepositoryError::invalid_data());
            }
            let public_id = insert_public_terminal(
                transaction,
                run_id,
                action.transition_key(),
                event_id_value,
                event_seq,
                public_payload,
            )
            .await?;
            let rows = sqlx::query(
                "UPDATE workflow_runs SET lifecycle=?,admission_state='closed',
                    termination_intent_reason=?,termination_intent_transition_key=?,
                    termination_intent_at=CURRENT_TIMESTAMP,output_payload_id=NULL,
                    output_artifact_id=NULL,output_value_hash=NULL,error_code=?,
                    terminal_event_id=?,terminal_public_event_id=?,projection_version=?,
                    updated_at=CURRENT_TIMESTAMP,terminal_at=CURRENT_TIMESTAMP,
                    scheduler_lease_owner=NULL,scheduler_fencing_token=NULL,
                    scheduler_lease_expires_at=NULL,scheduler_heartbeat_at=NULL
                 WHERE run_id=? AND projection_version=?
                   AND termination_intent_reason=?",
            )
            .bind(terminal_lifecycle.as_str())
            .bind(reason_value)
            .bind(action.transition_key().as_str())
            .bind(public_code)
            .bind(event_id_value)
            .bind(public_id)
            .bind(i64_from_u64(next_version)?)
            .bind(run_id.as_str())
            .bind(i64_from_u64(
                action.precondition().expected_projection_version(),
            )?)
            .bind(reason_value)
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                return Err(RepositoryError::invalid_data());
            }
        }
        SchedulerAction::OpenFork { admission } => {
            let group = admission.group();
            let group_parts = scheduler_adapter::fork_group_parts(group);
            let (fork_scope, fork_node) =
                activation_identity(transaction, run_id, group_parts.fork_activation_id).await?;
            if fork_scope != *group_parts.parent_scope_instance_id
                || fork_node != *group_parts.fork_node_id
            {
                return Err(RepositoryError::invalid_data());
            }
            let join_mode = match group.mode() {
                insight_engine::plan::PlanJoinMode::AllSuccess => "all_success",
                insight_engine::plan::PlanJoinMode::AllSettled => "all_settled",
            };
            sqlx::query(
                "INSERT INTO fork_groups (
                    run_id,fork_group_id,fork_activation_id,parent_scope_instance_id,
                    join_activation_id,join_mode,failure_leg_id,failure_settlement_class,
                    expected_legs,group_state,admitted_legs,settled_legs,
                    projection_version,created_at,settled_at
                 ) VALUES (?,?,?,?,NULL,?,NULL,NULL,?,'open',?,0,0,CURRENT_TIMESTAMP,NULL)",
            )
            .bind(run_id.as_str())
            .bind(group_parts.group_id.as_str())
            .bind(group_parts.fork_activation_id.as_str())
            .bind(group_parts.parent_scope_instance_id.as_str())
            .bind(join_mode)
            .bind(
                i64::try_from(group_parts.members.len())
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .bind(
                i64::try_from(group_parts.members.len())
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            for (declaration_index, leg_admission) in admission.legs().iter().enumerate() {
                let leg = leg_admission.leg();
                create_dynamic_child(
                    transaction,
                    run_id,
                    group_parts.fork_activation_id,
                    leg.scope_instance_id(),
                    leg.static_scope_id(),
                    "parallel_leg",
                    &format!("fork:{}", leg.key().leg_id()),
                    leg.child_node_id(),
                    leg.child_activation_id(),
                    leg.occurrence(),
                    leg.token_id(),
                    leg_admission.output_port(),
                    action.transition_key(),
                )
                .await?;
                let frames = canonical_json(
                    &serde_json::to_value(vec![ExecutionControlFrame::ForkLeg {
                        fork_activation_id: group_parts.fork_activation_id.clone(),
                        fork_group_id: group_parts.group_id.clone(),
                        leg_id: leg.key().leg_id().clone(),
                        scope_instance_id: leg.scope_instance_id().clone(),
                    }])
                    .map_err(|_| RepositoryError::canonicalization())?,
                )?;
                let token_rows = sqlx::query(
                    "UPDATE control_tokens SET provenance_frames=?,fork_group_id=?,fork_leg_id=?
                     WHERE run_id=? AND token_id=? AND fork_group_id IS NULL
                       AND fork_leg_id IS NULL",
                )
                .bind(frames)
                .bind(group_parts.group_id.as_str())
                .bind(leg.key().leg_id().as_str())
                .bind(run_id.as_str())
                .bind(leg.token_id().as_str())
                .execute(&mut **transaction)
                .await
                .map_err(RepositoryError::storage)?
                .rows_affected();
                if token_rows != 1 {
                    return Err(RepositoryError::invalid_data());
                }
                sqlx::query(
                    "INSERT INTO fork_legs (
                        run_id,fork_group_id,leg_id,declaration_index,scope_instance_id,
                        child_activation_id,token_id,is_required,leg_state,settlement_class,
                        projection_version,created_at,settled_at
                     ) VALUES (?,?,?,?,?,?,?,1,'admitted',NULL,0,CURRENT_TIMESTAMP,NULL)",
                )
                .bind(run_id.as_str())
                .bind(group_parts.group_id.as_str())
                .bind(leg.key().leg_id().as_str())
                .bind(
                    i64::try_from(declaration_index)
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .bind(leg.scope_instance_id().as_str())
                .bind(leg.child_activation_id().as_str())
                .bind(leg.token_id().as_str())
                .execute(&mut **transaction)
                .await
                .map_err(RepositoryError::storage)?;
            }
        }
        SchedulerAction::SettleForkLeg { leg, outcome } => {
            let settlement = structural_settlement_class(outcome);
            settle_dynamic_scope(
                transaction,
                run_id,
                leg.scope_instance_id(),
                settlement == "cancelled",
            )
            .await?;
            let rows = sqlx::query(
                "UPDATE fork_legs SET leg_state=?,settlement_class=?,
                    projection_version=projection_version+1,settled_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND fork_group_id=? AND leg_id=? AND leg_state='admitted'",
            )
            .bind(if settlement == "cancelled" {
                "cancelled"
            } else {
                "settled"
            })
            .bind(settlement)
            .bind(run_id.as_str())
            .bind(leg.key().group_id().as_str())
            .bind(leg.key().leg_id().as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                return Err(RepositoryError::invalid_data());
            }
            sqlx::query(
                "UPDATE fork_groups SET settled_legs=settled_legs+1,
                    group_state=CASE WHEN settled_legs+1=admitted_legs
                                     THEN 'settling' ELSE group_state END,
                    failure_leg_id=CASE WHEN ?='succeeded' THEN failure_leg_id
                                        ELSE COALESCE(failure_leg_id,?) END,
                    failure_settlement_class=CASE WHEN ?='succeeded'
                        THEN failure_settlement_class
                        ELSE COALESCE(failure_settlement_class,?) END,
                    projection_version=projection_version+1
                 WHERE run_id=? AND fork_group_id=? AND group_state IN ('open','settling')",
            )
            .bind(settlement)
            .bind(leg.key().leg_id().as_str())
            .bind(settlement)
            .bind(settlement)
            .bind(run_id.as_str())
            .bind(leg.key().group_id().as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
        }
        SchedulerAction::CompleteFork {
            group_id,
            join_activation_id,
        } => {
            // A Join is not merely the terminal state of a Fork group.  Persist one
            // immutable arrival per leg, tied to the exact settlement transition and
            // event, so recovery and artifact retention never have to infer arrivals
            // from the current fork_legs projection.
            let settlement_rows = sqlx::query_scalar::<_, String>(
                "SELECT transition_key
                 FROM scheduler_checkpoints
                 WHERE run_id=? AND checkpoint_kind='planned_action'
                 ORDER BY scheduler_projection_version,checkpoint_id",
            )
            .bind(run_id.as_str())
            .fetch_all(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            let mut arrival_count = 0_i64;
            for transition_key in settlement_rows {
                let checkpoint = validated_planned_action_checkpoint_sqlite(
                    transaction,
                    run_id,
                    &transition_key,
                )
                .await?;
                let SchedulerAction::SettleForkLeg { leg, outcome } = checkpoint.intent.action()
                else {
                    continue;
                };
                if leg.key().group_id() != group_id {
                    continue;
                }
                let (value_payload_id, value_hash) = match outcome.value() {
                    Some(value) => {
                        let (payload_id, hash) =
                            insert_or_get_payload(transaction, run_id, value.value()).await?;
                        (Some(payload_id), Some(hash))
                    }
                    None => (None, None),
                };
                sqlx::query(
                    "INSERT INTO join_arrivals (
                        run_id,join_activation_id,fork_group_id,leg_id,token_id,
                        arrival_transition_key,arrival_event_id,settlement_class,
                        value_payload_id,value_artifact_id,value_hash,
                        projection_version,arrived_at
                     ) VALUES (?,?,?,?,?,?,?,?,?,NULL,?,0,CURRENT_TIMESTAMP)",
                )
                .bind(run_id.as_str())
                .bind(join_activation_id.as_str())
                .bind(group_id.as_str())
                .bind(leg.key().leg_id().as_str())
                .bind(leg.token_id().as_str())
                .bind(checkpoint.transition_key.as_str())
                .bind(&checkpoint.event_id)
                .bind(structural_settlement_class(outcome))
                .bind(value_payload_id)
                .bind(value_hash)
                .execute(&mut **transaction)
                .await
                .map_err(RepositoryError::storage)?;
                arrival_count = arrival_count
                    .checked_add(1)
                    .ok_or_else(RepositoryError::invalid_data)?;
            }
            let (expected_arrivals, fork_activation_id) = sqlx::query_as::<_, (i64, String)>(
                "SELECT expected_legs,fork_activation_id FROM fork_groups
                 WHERE run_id=? AND fork_group_id=?",
            )
            .bind(run_id.as_str())
            .bind(group_id.as_str())
            .fetch_one(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if arrival_count != expected_arrivals {
                return Err(RepositoryError::invalid_data());
            }
            let undrained_scopes = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM fork_legs l
                 JOIN scope_instances s ON s.run_id=l.run_id
                    AND s.scope_instance_id=l.scope_instance_id
                 WHERE l.run_id=? AND l.fork_group_id=?
                   AND (s.lifecycle NOT IN ('settled','cancelled')
                        OR s.admission_state<>'closed'
                        OR s.settled_children<>s.admitted_children)",
            )
            .bind(run_id.as_str())
            .bind(group_id.as_str())
            .fetch_one(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if undrained_scopes != 0 {
                return Err(RepositoryError::invalid_data());
            }
            let rows = sqlx::query(
                "UPDATE fork_groups SET join_activation_id=?,group_state='settled',
                    projection_version=projection_version+1,settled_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND fork_group_id=? AND group_state='settling'
                   AND admitted_legs=expected_legs AND settled_legs=expected_legs",
            )
            .bind(join_activation_id.as_str())
            .bind(run_id.as_str())
            .bind(group_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                return Err(RepositoryError::invalid_data());
            }
            let fork_activation_id = model_data(ActivationId::new(fork_activation_id))?;
            succeed_native_activation(
                transaction,
                run_id,
                &fork_activation_id,
                &json!({"kind": "fork_completed", "group_id": group_id.as_str()}),
            )
            .await?;
            succeed_native_activation(
                transaction,
                run_id,
                join_activation_id,
                &json!({"kind": "fork_join", "group_id": group_id.as_str()}),
            )
            .await?;
        }
        SchedulerAction::RequestScopeCancellation {
            scope_instance_id, ..
        } => {
            cancel_and_drain_scope(
                transaction,
                run_id,
                scope_instance_id,
                action.transition_key(),
                event_id_value,
            )
            .await?;
        }
        SchedulerAction::OpenMap { .. } | SchedulerAction::OpenLoop { .. } => {}
        SchedulerAction::SpawnMapItem {
            item,
            item_port,
            item_value,
            output_port,
        } => {
            let stable_dynamic_key = item.key().stable_dynamic_key();
            create_dynamic_child(
                transaction,
                run_id,
                item.key().map_activation_id(),
                item.scope_instance_id(),
                item.static_scope_id(),
                "map_item",
                &stable_dynamic_key,
                item.child_node_id(),
                item.child_activation_id(),
                item.occurrence(),
                item.token_id(),
                output_port,
                action.transition_key(),
            )
            .await?;
            let value_ref = model_data(ValueRef::inline(item_value.value().clone()))?;
            upsert_scheduler_value(
                transaction,
                run_id,
                item.child_activation_id(),
                item_port,
                item_value,
                &value_ref,
                item_value.value_type(),
            )
            .await?;
            upsert_occurrence_value(
                transaction,
                run_id,
                item.occurrence(),
                item.child_activation_id(),
                item_port,
                item_value,
                &value_ref,
                item_value.value_type(),
            )
            .await?;
        }
        SchedulerAction::SettleMapItem { item, outcome } => {
            settle_dynamic_scope(
                transaction,
                run_id,
                item.scope_instance_id(),
                structural_settlement_class(outcome) == "cancelled",
            )
            .await?;
        }
        SchedulerAction::CompleteMap { map_activation_id } => {
            succeed_native_activation(
                transaction,
                run_id,
                map_activation_id,
                &json!({"kind": "map_completed"}),
            )
            .await?;
        }
        SchedulerAction::StartLoopIteration {
            iteration,
            state_port,
            output_port,
        } => {
            let (scope_kind, stable_dynamic_key) = match iteration.flavor() {
                insight_engine::plan::LoopFlavor::Workflow => (
                    "loop_iteration",
                    format!("loop:{}", iteration.key().iteration()),
                ),
                insight_engine::plan::LoopFlavor::Agent => (
                    "agent_loop_turn",
                    format!("agent_loop:{}", iteration.key().iteration()),
                ),
            };
            create_dynamic_child(
                transaction,
                run_id,
                iteration.key().loop_activation_id(),
                iteration.scope_instance_id(),
                iteration.static_scope_id(),
                scope_kind,
                &stable_dynamic_key,
                iteration.child_node_id(),
                iteration.child_activation_id(),
                iteration.occurrence(),
                iteration.token_id(),
                output_port,
                action.transition_key(),
            )
            .await?;
            let iteration_state = scheduler_adapter::loop_iteration_state(iteration);
            let value_ref = model_data(ValueRef::inline(iteration_state.value().clone()))?;
            upsert_occurrence_value(
                transaction,
                run_id,
                iteration.occurrence(),
                iteration.child_activation_id(),
                state_port,
                iteration_state,
                &value_ref,
                iteration_state.value_type(),
            )
            .await?;
        }
        SchedulerAction::AdvanceLoop { iteration, .. } => {
            settle_dynamic_scope(transaction, run_id, iteration.scope_instance_id(), false).await?;
        }
        SchedulerAction::SettleLoopIteration { iteration, outcome } => {
            settle_dynamic_scope(
                transaction,
                run_id,
                iteration.scope_instance_id(),
                structural_settlement_class(outcome) == "cancelled",
            )
            .await?;
        }
        SchedulerAction::CompleteLoop {
            loop_activation_id,
            iteration,
            state,
        } => {
            if let Some(iteration) = iteration {
                settle_dynamic_scope(transaction, run_id, iteration.scope_instance_id(), false)
                    .await?;
            }
            succeed_native_activation(transaction, run_id, loop_activation_id, state.value())
                .await?;
        }
        SchedulerAction::RegisterWait { registration } => {
            let registration_parts = scheduler_adapter::wait_registration_parts(registration);
            let rows = sqlx::query(
                "UPDATE node_activations SET execution_kind='durable_wait',lifecycle='waiting',
                    wait_registration_transition_key=?,projection_version=projection_version+1,
                    updated_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND activation_id=? AND lifecycle='created'",
            )
            .bind(action.transition_key().as_str())
            .bind(run_id.as_str())
            .bind(registration_parts.activation_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                return Err(RepositoryError::invalid_data());
            }
            if let (Some(timer_id), Some(due_at_ms)) =
                (registration_parts.timer_id, registration_parts.due_at_ms)
            {
                let due = DateTime::<Utc>::from_timestamp_millis(
                    i64::try_from(due_at_ms).map_err(|_| RepositoryError::invalid_data())?,
                )
                .ok_or_else(RepositoryError::invalid_data)?;
                sqlx::query(
                    "INSERT INTO timers (
                        run_id,timer_id,activation_id,timer_kind,timer_state,deadline_at,
                        expected_attempt_no,expected_lease_epoch,expected_fencing_token,
                        retry_budget_snapshot,created_by_transition_key,fired_by_transition_key,
                        fired_event_id,projection_version,created_at,fired_at
                     ) VALUES (?,? ,?,'wait','scheduled',?,NULL,NULL,NULL,NULL,?,NULL,NULL,0,
                               CURRENT_TIMESTAMP,NULL)",
                )
                .bind(run_id.as_str())
                .bind(timer_id.as_str())
                .bind(registration_parts.activation_id.as_str())
                .bind(now_text(due))
                .bind(action.transition_key().as_str())
                .execute(&mut **transaction)
                .await
                .map_err(RepositoryError::storage)?;
            }
            sqlx::query(
                "INSERT INTO scheduler_wait_registrations (
                    run_id,wait_id,activation_id,node_id,occurrence_key,signal_name,signal_id,
                    timer_id,due_at_ms,payload_type,winner_kind,winner_signal_id,winner_timer_id,
                    projection_version,created_at,resolved_at
                 ) VALUES (?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,0,CURRENT_TIMESTAMP,NULL)",
            )
            .bind(run_id.as_str())
            .bind(registration_parts.wait_id.as_str())
            .bind(registration_parts.activation_id.as_str())
            .bind(registration_parts.node_id.as_str())
            .bind(canonical_json(
                &serde_json::to_value(registration_parts.occurrence)
                    .map_err(|_| RepositoryError::canonicalization())?,
            )?)
            .bind(registration_parts.signal_name)
            .bind(registration_parts.signal_id.map(|value| value.as_str()))
            .bind(registration_parts.timer_id.map(|value| value.as_str()))
            .bind(
                registration_parts
                    .due_at_ms
                    .map(i64::try_from)
                    .transpose()
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .bind(
                registration_parts
                    .payload_type
                    .map(serde_json::to_value)
                    .transpose()
                    .map_err(|_| RepositoryError::canonicalization())?
                    .as_ref()
                    .map(canonical_json)
                    .transpose()?,
            )
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            insert_human_work_item_sqlite(transaction, run_id, registration).await?;
        }
        SchedulerAction::StartSubflow {
            invocation,
            execution_revision,
            interface_version,
            timeout_ms,
            run_input,
            outputs,
        } => {
            start_subflow_sqlite(
                transaction,
                run_id,
                action.transition_key(),
                invocation,
                execution_revision,
                interface_version,
                *timeout_ms,
                run_input,
                outputs,
            )
            .await?;
        }
        SchedulerAction::RequestChildRunCancellation { child_run_id } => {
            let rows = sqlx::query(
                "UPDATE scheduler_subflow_invocations
                 SET invocation_state='cancellation_requested',projection_version=projection_version+1
                 WHERE run_id=? AND child_run_id=? AND invocation_state='started'",
            )
            .bind(run_id.as_str())
            .bind(child_run_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                return Err(RepositoryError::invalid_data());
            }
            sqlx::query(
                "UPDATE workflow_runs SET lifecycle='terminating',admission_state='draining',
                    termination_intent_reason='cancelled',termination_intent_transition_key=?,
                    termination_intent_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
                    updated_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND lifecycle IN ('created','active','waiting')",
            )
            .bind(action.transition_key().as_str())
            .bind(child_run_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
        }
        SchedulerAction::SettleSubflow {
            invocation,
            outcome,
        } => {
            settle_subflow_sqlite(
                transaction,
                run_id,
                action.transition_key(),
                invocation,
                outcome,
            )
            .await?;
        }
        SchedulerAction::OpenErrorBoundary { .. } => {}
        SchedulerAction::TransitionErrorBoundary { boundary } => {
            if boundary.phase() == insight_engine::scheduler::ErrorBoundaryPhase::Finalizer
                && matches!(
                    boundary.exit(),
                    insight_engine::scheduler::ErrorBoundaryExit::Terminate { .. }
                )
            {
                prepare_termination_finalizer_sqlite(
                    transaction,
                    run_id,
                    boundary.boundary_activation_id(),
                    action.transition_key(),
                    event_id_value,
                )
                .await?;
            }
            if boundary.phase() == insight_engine::scheduler::ErrorBoundaryPhase::Completed {
                succeed_native_activation(
                    transaction,
                    run_id,
                    boundary.boundary_activation_id(),
                    &json!({"kind": "error_boundary_completed"}),
                )
                .await?;
            }
        }
    }

    let terminal_snapshot = match action.intent().action() {
        SchedulerAction::FailRunPlanning { failure } => {
            Some((RunLifecycle::Failed, None, Some(failure.public_code())))
        }
        SchedulerAction::CompleteRun { output, .. } => {
            Some((RunLifecycle::Succeeded, Some(output.value()), None))
        }
        SchedulerAction::FailRun { error, .. } => Some((
            RunLifecycle::Failed,
            None,
            error
                .value()
                .as_object()
                .and_then(|value| value.get("code"))
                .and_then(Value::as_str),
        )),
        SchedulerAction::FailRunInternal { failure, .. } => {
            Some((RunLifecycle::Failed, None, Some(failure.code())))
        }
        SchedulerAction::CancelRun { reason, .. } => Some(match reason {
            TerminationReason::Failure => {
                (RunLifecycle::Failed, None, Some("RUN_TERMINATED_FAILURE"))
            }
            TerminationReason::Cancelled => {
                (RunLifecycle::Cancelled, None, Some("SCHEDULER_CANCELLED"))
            }
            TerminationReason::Interrupted => {
                (RunLifecycle::Interrupted, None, Some("RUN_INTERRUPTED"))
            }
            TerminationReason::TimedOut => (RunLifecycle::TimedOut, None, Some("RUN_TIMEOUT")),
        }),
        _ => None,
    };
    if let Some((lifecycle, output, error_code)) = terminal_snapshot {
        super::sqlite_model_tool_queue::close_model_tool_work_for_terminal_run_sqlite(
            transaction,
            run_id,
        )
        .await?;
        super::sqlite::persist_terminal_response_snapshot_sqlite(
            transaction,
            run_id,
            lifecycle,
            output,
            error_code,
        )
        .await?;
        super::sqlite::register_terminal_artifact_retention_sqlite(
            transaction,
            run_id,
            action.transition_key(),
            action.intent_hash().as_str(),
            event_id_value,
            event_seq,
        )
        .await?;
    }

    if !matches!(
        action.intent().action(),
        SchedulerAction::FailRunPlanning { .. }
            | SchedulerAction::CompleteRun { .. }
            | SchedulerAction::FailRun { .. }
            | SchedulerAction::FailRunInternal { .. }
            | SchedulerAction::CancelRun { .. }
    ) {
        let rows = sqlx::query(
            "UPDATE workflow_runs SET projection_version=?,updated_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND projection_version=?",
        )
        .bind(i64_from_u64(next_version)?)
        .bind(run_id.as_str())
        .bind(i64_from_u64(
            action.precondition().expected_projection_version(),
        )?)
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows != 1 {
            return Err(RepositoryError::invalid_data());
        }
    }
    Ok(())
}

async fn insert_human_work_item_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    registration: &insight_engine::scheduler::WaitRegistrationFact,
) -> Result<(), RepositoryError> {
    let registration = scheduler_adapter::wait_registration_parts(registration);
    let Some(human_task) = registration.human_task else {
        return Ok(());
    };
    let response_type = canonical_json(
        &serde_json::to_value(
            registration
                .payload_type
                .ok_or_else(RepositoryError::invalid_data)?,
        )
        .map_err(|_| RepositoryError::canonicalization())?,
    )?;
    let assignees = canonical_json(
        &serde_json::to_value(human_task.assignees())
            .map_err(|_| RepositoryError::canonicalization())?,
    )?;
    let candidate_groups = canonical_json(
        &serde_json::to_value(human_task.candidate_groups())
            .map_err(|_| RepositoryError::canonicalization())?,
    )?;
    sqlx::query(
        "INSERT INTO human_work_items (
            work_item_id,run_id,wait_id,activation_id,signal_id,signal_name,request_value,response_type,
            assignees,candidate_groups,claim_lease_ms,work_state,claim_fence,
            projection_version,created_at,updated_at
         ) VALUES (?,?,?,?,?,?,?,?,?,?,?,'open',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    )
    .bind(registration.wait_id.as_str())
    .bind(run_id.as_str())
    .bind(registration.wait_id.as_str())
    .bind(registration.activation_id.as_str())
    .bind(
        registration
            .signal_id
            .ok_or_else(RepositoryError::invalid_data)?
            .as_str(),
    )
    .bind(
        registration
            .signal_name
            .ok_or_else(RepositoryError::invalid_data)?,
    )
    .bind(canonical_json(human_task.request().value())?)
    .bind(response_type)
    .bind(assignees)
    .bind(candidate_groups)
    .bind(
        i64::try_from(human_task.claim_lease_ms())
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

fn apply_planned_fact(
    facts: &mut SchedulerFacts,
    checkpoint_id: SchedulerCheckpointId,
    intent: SchedulerIntent,
) -> Result<(), RepositoryError> {
    if intent.run_id() != facts.run_id() || intent.checkpoint_id() != &checkpoint_id {
        return Err(RepositoryError::invalid_data());
    }
    facts.commit_checkpoint(checkpoint_id);
    match intent.action() {
        SchedulerAction::FailRunPlanning { failure } => {
            facts.record_terminal(RunTerminalFact::FailedPlanning(*failure))
        }
        SchedulerAction::AdmitActivation { activation_id, .. } => {
            facts.record_activation(activation_id.clone())
        }
        SchedulerAction::ConsumeToken { token_id, .. } => {
            facts.record_consumed_token(token_id.clone())
        }
        SchedulerAction::EmitToken { token_id, .. } => facts.record_emitted_token(token_id.clone()),
        SchedulerAction::DispatchTask { task_id, .. } => {
            facts.record_dispatched_task(task_id.clone())
        }
        SchedulerAction::CommitNativeOutput {
            occurrence, output, ..
        } => {
            let insight_engine::NativeOutput::Values { values } = output;
            for (port_id, value) in values {
                facts.record_value(port_id.clone(), value.clone());
                facts.record_occurrence_value(occurrence.clone(), port_id.clone(), value.clone());
            }
        }
        SchedulerAction::SelectBranchAndAdmit { selection } => {
            facts.record_branch_selection(
                selection.branch_node_id().clone(),
                selection.case_id().clone(),
            );
            facts.record_occurrence_branch_selection(
                selection.occurrence().clone(),
                selection.branch_node_id().clone(),
                selection.case_id().clone(),
            );
            facts.record_activation(selection.successor().activation_id().clone());
            facts.record_emitted_token(selection.token_id().clone());
            facts.record_consumed_token(selection.token_id().clone());
        }
        SchedulerAction::CommitOccurrenceValues {
            occurrence, values, ..
        } => {
            for (port_id, value) in values {
                facts.record_value(port_id.clone(), value.clone());
                facts.record_occurrence_value(occurrence.clone(), port_id.clone(), value.clone());
            }
        }
        SchedulerAction::CompleteRun { output, .. } => {
            facts.record_terminal(RunTerminalFact::Succeeded(output.clone()))
        }
        SchedulerAction::FailRun { error, .. } => {
            facts.record_terminal(RunTerminalFact::Failed(error.clone()))
        }
        SchedulerAction::FailRunInternal { failure, .. } => {
            facts.record_terminal(RunTerminalFact::FailedInternal(failure.clone()))
        }
        SchedulerAction::CancelRun { reason, .. } => facts.record_terminal(match reason {
            TerminationReason::Failure => {
                RunTerminalFact::FailedInternal(scheduler_data(TaskFailureFact::new(
                    insight_engine::WorkerFailureClass::InfrastructureFailure,
                    "RUN_TERMINATED_FAILURE",
                    None,
                ))?)
            }
            TerminationReason::Cancelled => RunTerminalFact::Cancelled,
            TerminationReason::Interrupted => RunTerminalFact::Interrupted,
            TerminationReason::TimedOut => RunTerminalFact::TimedOut,
        }),
        SchedulerAction::OpenFork { admission } => {
            facts.record_fork_group(admission.group().clone());
            for leg in admission.legs() {
                facts.record_fork_leg(leg.leg().clone());
            }
        }
        SchedulerAction::SettleForkLeg { leg, outcome } => {
            facts.settle_fork_leg(leg.key().clone(), outcome.clone())
        }
        SchedulerAction::CompleteFork { group_id, .. } => facts.complete_fork(group_id.clone()),
        SchedulerAction::RequestScopeCancellation {
            scope_instance_id, ..
        } => facts.request_scope_cancellation(scope_instance_id.clone()),
        SchedulerAction::OpenMap { map } => facts.record_map_instance(map.clone()),
        SchedulerAction::SpawnMapItem {
            item,
            item_port,
            item_value,
            ..
        } => facts.record_map_item(item.clone(), item_port.clone(), item_value.clone()),
        SchedulerAction::SettleMapItem { item, outcome } => {
            facts.settle_map_item(item.key().clone(), outcome.clone())
        }
        SchedulerAction::CompleteMap { map_activation_id } => {
            facts.complete_map(map_activation_id.clone())
        }
        SchedulerAction::OpenLoop { loop_instance } => {
            facts.record_loop_instance(loop_instance.clone())
        }
        SchedulerAction::StartLoopIteration {
            iteration,
            state_port,
            ..
        } => facts.record_loop_iteration(iteration.clone(), state_port.clone()),
        SchedulerAction::AdvanceLoop { iteration, state } => {
            scheduler_data(facts.advance_loop(iteration.key().loop_activation_id(), state.clone()))?
        }
        SchedulerAction::SettleLoopIteration { iteration, outcome } => {
            facts.settle_loop_iteration(iteration.key().clone(), outcome.clone())
        }
        SchedulerAction::CompleteLoop {
            loop_activation_id,
            state,
            ..
        } => scheduler_data(facts.complete_loop(loop_activation_id, state.clone()))?,
        SchedulerAction::RegisterWait { registration } => facts.register_wait(registration.clone()),
        SchedulerAction::StartSubflow { invocation, .. } => {
            facts.record_subflow(invocation.clone())
        }
        SchedulerAction::RequestChildRunCancellation { child_run_id } => {
            facts.request_child_cancellation(child_run_id.clone())
        }
        SchedulerAction::SettleSubflow {
            invocation,
            outcome,
        } => facts.settle_subflow(invocation.child_run_id().clone(), outcome.clone()),
        SchedulerAction::OpenErrorBoundary { boundary }
        | SchedulerAction::TransitionErrorBoundary { boundary } => {
            facts.record_boundary(boundary.clone())
        }
    }
    Ok(())
}

fn stored_value_from_row(
    run_id: &RunId,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<SchedulerStoredValue, RepositoryError> {
    let port_id = DataPortId::new(
        row.try_get::<String, _>("port_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let runtime_value = serde_json::from_str::<RuntimeValue>(
        &row.try_get::<String, _>("runtime_value")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let value_ref = serde_json::from_str::<ValueRef>(
        &row.try_get::<String, _>("value_ref")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let declared_type = serde_json::from_str::<PlanType>(
        &row.try_get::<String, _>("declared_type")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if value_ref.content_hash().as_str()
        != row
            .try_get::<String, _>("content_hash")
            .map_err(|_| RepositoryError::invalid_data())?
    {
        return Err(RepositoryError::invalid_data());
    }
    let storage_kind: String = row
        .try_get("storage_kind")
        .map_err(|_| RepositoryError::invalid_data())?;
    let payload: Option<String> = row
        .try_get("payload_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let artifact: Option<String> = row
        .try_get("artifact_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    match &value_ref {
        ValueRef::Inline(_) => {
            if storage_kind != "inline"
                || payload.as_deref() != Some(payload_id(value_ref.content_hash()).as_str())
                || artifact.is_some()
            {
                return Err(RepositoryError::invalid_data());
            }
        }
        ValueRef::Artifact(reference) => {
            if storage_kind != "artifact"
                || payload.is_some()
                || artifact.as_deref() != Some(reference.artifact_id().as_str())
            {
                return Err(RepositoryError::invalid_data());
            }
        }
    }
    SchedulerStoredValue::new(
        run_id.clone(),
        port_id,
        model_data(ActivationId::new(
            row.try_get::<String, _>("owner_activation_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?,
        runtime_value,
        value_ref,
        declared_type,
        u64_from_i64(
            row.try_get("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
    )
}

fn value_ref_matches_locator(
    reference: &ValueRef,
    storage_kind: &str,
    payload: Option<&str>,
    artifact: Option<&str>,
    content_hash: &ContentHash,
) -> bool {
    if reference.content_hash() != content_hash {
        return false;
    }
    match reference {
        ValueRef::Inline(_) => {
            storage_kind == "inline"
                && payload == Some(payload_id(content_hash).as_str())
                && artifact.is_none()
        }
        ValueRef::Artifact(reference) => {
            storage_kind == "artifact"
                && payload.is_none()
                && artifact == Some(reference.artifact_id().as_str())
        }
    }
}

fn task_output_receipt_from_rows_sqlite(
    run_id: &RunId,
    occurrence: &insight_engine::LogicalOccurrence,
    canonical_row: &sqlx::sqlite::SqliteRow,
    occurrence_row: &sqlx::sqlite::SqliteRow,
) -> Result<(DataPortId, TaskOutputReceipt), RepositoryError> {
    let canonical = stored_value_from_row(run_id, canonical_row)?;
    let historical = stored_value_from_row(run_id, occurrence_row)?;
    let stored_occurrence: insight_engine::LogicalOccurrence = serde_json::from_str(
        &occurrence_row
            .try_get::<String, _>("occurrence_key")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let canonical_owner = model_data(ActivationId::new(
        canonical_row
            .try_get::<String, _>("owner_activation_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let occurrence_owner = model_data(ActivationId::new(
        occurrence_row
            .try_get::<String, _>("owner_activation_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    if stored_occurrence != *occurrence
        || canonical.port_id() != historical.port_id()
        || canonical.runtime_value() != historical.runtime_value()
        || canonical.declared_type() != historical.declared_type()
        || canonical_owner != occurrence_owner
    {
        return Err(RepositoryError::invalid_data());
    }
    let content_hash = model_data(ContentHash::parse(
        occurrence_row
            .try_get::<String, _>("content_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    Ok((
        canonical.port_id().clone(),
        TaskOutputReceipt {
            owner_activation_id: occurrence_owner,
            occurrence: occurrence.clone(),
            runtime_value: historical.runtime_value().clone(),
            declared_type: historical.declared_type().clone(),
            content_hash,
            canonical_value_ref: canonical.value_ref().clone(),
            canonical_storage_kind: canonical_row
                .try_get("storage_kind")
                .map_err(|_| RepositoryError::invalid_data())?,
            canonical_payload_id: canonical_row
                .try_get("payload_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            canonical_artifact_id: canonical_row
                .try_get("artifact_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            canonical_projection_version: canonical.projection_version(),
            occurrence_value_ref: historical.value_ref().clone(),
            occurrence_storage_kind: occurrence_row
                .try_get("storage_kind")
                .map_err(|_| RepositoryError::invalid_data())?,
            occurrence_payload_id: occurrence_row
                .try_get("payload_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            occurrence_artifact_id: occurrence_row
                .try_get("artifact_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            occurrence_projection_version: historical.projection_version(),
        },
    ))
}

async fn load_task_output_receipt_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    occurrence: &insight_engine::LogicalOccurrence,
    port_id: &DataPortId,
) -> Result<TaskOutputReceipt, RepositoryError> {
    let canonical_row = sqlx::query(
        "SELECT port_id,owner_activation_id,runtime_value,value_ref,declared_type,storage_kind,
                payload_id,artifact_id,content_hash,projection_version
         FROM scheduler_values WHERE run_id=? AND port_id=?",
    )
    .bind(run_id.as_str())
    .bind(port_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let occurrence_key = canonical_json(
        &serde_json::to_value(occurrence).map_err(|_| RepositoryError::canonicalization())?,
    )?;
    let occurrence_row = sqlx::query(
        "SELECT occurrence_key,port_id,owner_activation_id,runtime_value,value_ref,declared_type,
                storage_kind,payload_id,artifact_id,content_hash,projection_version
         FROM scheduler_occurrence_values
         WHERE run_id=? AND occurrence_key=? AND port_id=?",
    )
    .bind(run_id.as_str())
    .bind(occurrence_key)
    .bind(port_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let (stored_port, receipt) =
        task_output_receipt_from_rows_sqlite(run_id, occurrence, &canonical_row, &occurrence_row)?;
    if stored_port != *port_id {
        return Err(RepositoryError::invalid_data());
    }
    validate_value_ref_resource_sqlite(transaction, run_id, &receipt.canonical_value_ref).await?;
    validate_value_ref_resource_sqlite(transaction, run_id, &receipt.occurrence_value_ref).await?;
    Ok(receipt)
}

async fn validate_canonical_task_output_projection_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    port_id: &DataPortId,
    receipt: &TaskOutputReceipt,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        "SELECT port_id,owner_activation_id,runtime_value,value_ref,declared_type,storage_kind,
                payload_id,artifact_id,content_hash,projection_version
         FROM scheduler_values WHERE run_id=? AND port_id=?",
    )
    .bind(run_id.as_str())
    .bind(port_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let stored = stored_value_from_row(run_id, &row)?;
    if stored.port_id() != port_id
        || stored.projection_version() < receipt.canonical_projection_version
    {
        return Err(RepositoryError::invalid_data());
    }
    if stored.projection_version() == receipt.canonical_projection_version {
        let owner = model_data(ActivationId::new(
            row.try_get::<String, _>("owner_activation_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let storage_kind: String = row
            .try_get("storage_kind")
            .map_err(|_| RepositoryError::invalid_data())?;
        let payload_id: Option<String> = row
            .try_get("payload_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let artifact_id: Option<String> = row
            .try_get("artifact_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let content_hash: String = row
            .try_get("content_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        if owner != receipt.owner_activation_id
            || stored.runtime_value() != &receipt.runtime_value
            || stored.value_ref() != &receipt.canonical_value_ref
            || stored.declared_type() != &receipt.declared_type
            || storage_kind != receipt.canonical_storage_kind
            || payload_id != receipt.canonical_payload_id
            || artifact_id != receipt.canonical_artifact_id
            || content_hash != receipt.content_hash.as_str()
        {
            return Err(RepositoryError::invalid_data());
        }
    }
    validate_value_ref_resource_sqlite(transaction, run_id, stored.value_ref()).await
}

fn restored_inline_payload_sqlite(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<Option<ValidatedInlinePayload>, RepositoryError> {
    let payload_id = row
        .try_get::<Option<String>, _>("payload_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let content_hash = row
        .try_get::<Option<String>, _>("payload_content_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    let canonical_bytes = row
        .try_get::<Option<i64>, _>("payload_canonical_bytes")
        .map_err(|_| RepositoryError::invalid_data())?;
    let encoding = row
        .try_get::<Option<String>, _>("payload_encoding")
        .map_err(|_| RepositoryError::invalid_data())?;
    let encoded = row
        .try_get::<Option<String>, _>("payload_inline_value")
        .map_err(|_| RepositoryError::invalid_data())?;
    let binary = row
        .try_get::<Option<Vec<u8>>, _>("payload_binary_value")
        .map_err(|_| RepositoryError::invalid_data())?;
    let Some(payload_id) = payload_id else {
        if content_hash.is_some()
            || canonical_bytes.is_some()
            || encoding.is_some()
            || encoded.is_some()
            || binary.is_some()
        {
            return Err(RepositoryError::invalid_data());
        }
        return Ok(None);
    };
    let content_hash = content_hash.ok_or_else(RepositoryError::invalid_data)?;
    let canonical_bytes = canonical_bytes.ok_or_else(RepositoryError::invalid_data)?;
    let encoding = encoding.ok_or_else(RepositoryError::invalid_data)?;
    let encoded = encoded.ok_or_else(RepositoryError::invalid_data)?;
    let value = serde_json::from_str(&encoded).map_err(|_| RepositoryError::invalid_data())?;
    validate_inline_payload(
        &payload_id,
        &content_hash,
        canonical_bytes,
        &encoding,
        value,
        Some(&encoded),
        binary.is_none(),
    )
    .map(Some)
}

async fn validate_value_ref_resource_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    reference: &ValueRef,
) -> Result<(), RepositoryError> {
    match reference {
        ValueRef::Inline(inline) => {
            let row = sqlx::query(
                "SELECT payload_id,p.content_hash AS payload_content_hash,
                        p.canonical_bytes AS payload_canonical_bytes,
                        p.encoding AS payload_encoding,p.inline_value AS payload_inline_value,
                        p.binary_value AS payload_binary_value
                 FROM payloads p WHERE p.run_id=? AND p.payload_id=?",
            )
            .bind(run_id.as_str())
            .bind(payload_id(reference.content_hash()))
            .fetch_optional(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(RepositoryError::invalid_data)?;
            let payload =
                restored_inline_payload_sqlite(&row)?.ok_or_else(RepositoryError::invalid_data)?;
            if common_contract_adapter::validated_inline_payload_value(&payload) != inline.value()
                || common_contract_adapter::validated_inline_payload_content_hash(&payload)
                    != inline.content_hash()
                || common_contract_adapter::validated_inline_payload_canonical_bytes(&payload)
                    != inline.canonical_bytes()
            {
                return Err(RepositoryError::invalid_data());
            }
        }
        ValueRef::Artifact(artifact) => {
            let row = sqlx::query(
                "SELECT content_hash,size_bytes,media_type,artifact_state FROM artifacts
                 WHERE run_id=? AND artifact_id=?",
            )
            .bind(run_id.as_str())
            .bind(artifact.artifact_id().as_str())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(RepositoryError::invalid_data)?;
            if row
                .try_get::<String, _>("content_hash")
                .map_err(|_| RepositoryError::invalid_data())?
                != artifact.content_hash().as_str()
                || u64_from_i64(
                    row.try_get("size_bytes")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )? != artifact.size_bytes()
                || row
                    .try_get::<Option<String>, _>("media_type")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .as_deref()
                    != artifact.media_type()
                || row
                    .try_get::<String, _>("artifact_state")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != "referenced"
            {
                return Err(RepositoryError::invalid_data());
            }
        }
    }
    Ok(())
}

async fn load_facts_sqlite(
    repository: &SqliteDurableRepository,
    run_id: &RunId,
) -> Result<SchedulerFacts, RepositoryError> {
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let run = sqlx::query(
        "SELECT r.projection_version,r.termination_intent_reason,
                p.payload_id AS payload_id,p.content_hash AS payload_content_hash,
                p.canonical_bytes AS payload_canonical_bytes,p.encoding AS payload_encoding,
                p.inline_value AS payload_inline_value,p.binary_value AS payload_binary_value,
                CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) AS observed_time_ms
         FROM workflow_runs r JOIN payloads p
           ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id
         WHERE r.run_id=?",
    )
    .bind(run_id.as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let input = common_contract_adapter::validated_inline_payload_value(
        &restored_inline_payload_sqlite(&run)?.ok_or_else(RepositoryError::invalid_data)?,
    )
    .clone();
    let mut facts = SchedulerFacts::new(
        run_id.clone(),
        u64_from_i64(
            run.try_get("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        scheduler_data(RuntimeValue::new(input))?,
    );
    facts.set_observed_time_ms(u64_from_i64(
        run.try_get("observed_time_ms")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?);
    if let Some(reason) = run
        .try_get::<Option<String>, _>("termination_intent_reason")
        .map_err(|_| RepositoryError::invalid_data())?
    {
        facts.request_run_termination(match reason.as_str() {
            "failure" => TerminationReason::Failure,
            "cancelled" => TerminationReason::Cancelled,
            "interrupted" => TerminationReason::Interrupted,
            "timed_out" => TerminationReason::TimedOut,
            _ => return Err(RepositoryError::invalid_data()),
        });
    }
    let reuse_candidates = sqlx::query(
        "SELECT candidate_id,target_scope_instance_id,target_node_id,
                stable_activation_key,projection_version
         FROM run_reuse_candidates
         WHERE run_id=? AND candidate_state='candidate'
         ORDER BY candidate_id",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for row in reuse_candidates {
        let occurrence = serde_json::from_str::<insight_engine::LogicalOccurrence>(
            &row.try_get::<String, _>("stable_activation_key")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        facts.record_reuse_candidate(scheduler_data(
            insight_engine::scheduler::ReuseCandidateFact::new(
                row.try_get::<String, _>("candidate_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
                model_data(insight_engine::ScopeInstanceId::new(
                    row.try_get::<String, _>("target_scope_instance_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                model_data(insight_engine::NodeId::new(
                    row.try_get::<String, _>("target_node_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                occurrence,
                u64_from_i64(
                    row.try_get::<i64, _>("projection_version")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
            ),
        )?);
    }
    let redrive_effects = sqlx::query(
        "SELECT e.source_activation_id,e.effect_id,a.node_id,a.stable_activation_key
         FROM recovery_effect_roots e
         JOIN run_recovery_lineage l ON l.run_id=e.run_id AND l.source_run_id=e.source_run_id
         JOIN node_activations a
           ON a.run_id=e.source_run_id AND a.activation_id=e.source_activation_id
          AND a.effect_id=e.effect_id
         WHERE e.run_id=? AND l.lineage_kind='redrive'
           AND a.execution_kind='worker' AND a.lifecycle<>'succeeded'
         ORDER BY a.node_id,a.stable_activation_key,e.source_activation_id",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for row in redrive_effects {
        facts.record_redrive_effect(insight_engine::scheduler::RedriveEffectFact::new(
            model_data(ActivationId::new(
                row.try_get::<String, _>("source_activation_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            model_data(insight_engine::NodeId::new(
                row.try_get::<String, _>("node_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            serde_json::from_str(
                &row.try_get::<String, _>("stable_activation_key")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?,
            model_data(EffectId::new(
                row.try_get::<String, _>("effect_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
        ));
    }
    let mut expected_occurrence_receipts =
        BTreeMap::<(String, DataPortId), TaskOutputReceipt>::new();
    let checkpoints = sqlx::query(
        "SELECT checkpoint_id,content_hash,checkpoint_kind,transition_key,intent_hash,event_id,
                checkpoint_schema_version,scheduler_projection_version,fact_payload
         FROM scheduler_checkpoints WHERE run_id=?
         ORDER BY scheduler_projection_version,checkpoint_id",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for row in checkpoints {
        let checkpoint_id = scheduler_data(SchedulerCheckpointId::parse(
            row.try_get::<String, _>("checkpoint_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let payload: String = row
            .try_get("fact_payload")
            .map_err(|_| RepositoryError::invalid_data())?;
        let payload_value: Value =
            serde_json::from_str(&payload).map_err(|_| RepositoryError::invalid_data())?;
        let kind: String = row
            .try_get("checkpoint_kind")
            .map_err(|_| RepositoryError::invalid_data())?;
        let transition = model_data(TransitionKey::parse(
            row.try_get::<String, _>("transition_key")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let intent_hash: String = row
            .try_get("intent_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        let event_id_value: String = row
            .try_get("event_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let schema_version = u32::try_from(
            row.try_get::<i64, _>("checkpoint_schema_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let scheduler_version = u64_from_i64(
            row.try_get("scheduler_projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let stored_hash: String = row
            .try_get("content_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        if schema_version != SCHEDULER_CHECKPOINT_SCHEMA_VERSION
            || scheduler_version > facts.projection_version()
            || scheduler_checkpoint_content_hash(
                run_id.as_str(),
                checkpoint_id.as_str(),
                &kind,
                transition.as_str(),
                &intent_hash,
                &event_id_value,
                schema_version,
                scheduler_version,
                &payload_value,
            )?
            .as_str()
                != stored_hash
        {
            return Err(RepositoryError::invalid_data());
        }
        let replay = match load_replay(&mut transaction, run_id, &transition, &intent_hash).await? {
            Replay::Exact(replay)
                if replay.event_id() == event_id_value
                    && replay.projection_version() == scheduler_version =>
            {
                replay
            }
            Replay::Exact(_) | Replay::Vacant => return Err(RepositoryError::invalid_data()),
        };
        match kind.as_str() {
            "planned_action" => {
                let intent: SchedulerIntent = serde_json::from_value(payload_value)
                    .map_err(|_| RepositoryError::invalid_data())?;
                let validated = validated_planned_action_checkpoint_sqlite(
                    &mut transaction,
                    run_id,
                    transition.as_str(),
                )
                .await?;
                if validated.intent != intent
                    || intent.run_id() != run_id
                    || intent.checkpoint_id() != &checkpoint_id
                    || canonical_intent_hash(&intent)?.as_str() != intent_hash
                {
                    return Err(RepositoryError::invalid_data());
                }
                apply_planned_fact(&mut facts, checkpoint_id, intent)?;
            }
            "task_completed" => {
                let completion = serde_json::from_value::<TaskCompletionFact>(payload_value)
                    .map_err(|_| RepositoryError::invalid_data())?;
                if checkpoint_id != scheduler_checkpoint_for_task(&completion.task_id)
                    || completion.output_receipts.len()
                        != match &completion.outcome {
                            TaskOutcomeFact::Succeeded { outputs } => outputs.len(),
                            TaskOutcomeFact::Failed { .. } => 0,
                        }
                {
                    return Err(RepositoryError::invalid_data());
                }
                validate_task_completion_projection_sqlite(
                    &mut transaction,
                    run_id,
                    &transition,
                    &intent_hash,
                    &replay,
                    &completion,
                )
                .await?;
                facts.commit_checkpoint(checkpoint_id);
                if let TaskOutcomeFact::Succeeded { outputs } = &completion.outcome {
                    for (port, value) in outputs {
                        let receipt = completion
                            .output_receipts
                            .get(port)
                            .ok_or_else(RepositoryError::invalid_data)?;
                        if receipt.occurrence != completion.occurrence
                            || receipt.runtime_value != *value
                            || !value.matches(&receipt.declared_type)
                            || validate_runtime_value_ref(value, &receipt.canonical_value_ref)
                                .is_err()
                            || validate_runtime_value_ref(value, &receipt.occurrence_value_ref)
                                .is_err()
                            || !value_ref_matches_locator(
                                &receipt.canonical_value_ref,
                                &receipt.canonical_storage_kind,
                                receipt.canonical_payload_id.as_deref(),
                                receipt.canonical_artifact_id.as_deref(),
                                &receipt.content_hash,
                            )
                            || !value_ref_matches_locator(
                                &receipt.occurrence_value_ref,
                                &receipt.occurrence_storage_kind,
                                receipt.occurrence_payload_id.as_deref(),
                                receipt.occurrence_artifact_id.as_deref(),
                                &receipt.content_hash,
                            )
                        {
                            return Err(RepositoryError::invalid_data());
                        }
                        validate_value_ref_resource_sqlite(
                            &mut transaction,
                            run_id,
                            &receipt.canonical_value_ref,
                        )
                        .await?;
                        validate_value_ref_resource_sqlite(
                            &mut transaction,
                            run_id,
                            &receipt.occurrence_value_ref,
                        )
                        .await?;
                        let occurrence_key = canonical_json(
                            &serde_json::to_value(&completion.occurrence)
                                .map_err(|_| RepositoryError::canonicalization())?,
                        )?;
                        if expected_occurrence_receipts
                            .insert((occurrence_key, port.clone()), receipt.clone())
                            .is_some()
                        {
                            return Err(RepositoryError::invalid_data());
                        }
                        facts.record_occurrence_value(
                            completion.occurrence.clone(),
                            port.clone(),
                            value.clone(),
                        );
                    }
                }
                facts.record_task_outcome(completion.task_id, completion.outcome);
            }
            "task_started" => {
                let started: TaskStartedFact = serde_json::from_value(payload_value)
                    .map_err(|_| RepositoryError::invalid_data())?;
                if checkpoint_id != operation_checkpoint(&transition)
                    || started.task_id.as_str().is_empty()
                    || started.activation_id.as_str().is_empty()
                    || started.fencing_token.is_empty()
                    || started.claim_token.is_empty()
                    || started.claimed_by.is_empty()
                    || TransitionKey::derive(
                        "scheduler.task.started.v1",
                        &[
                            run_id.as_str(),
                            started.task_id.as_str(),
                            &started.claim_token,
                        ],
                    )
                    .map_err(|_| RepositoryError::invalid_data())?
                        != transition
                {
                    return Err(RepositoryError::invalid_data());
                }
                let task_activation = sqlx::query_scalar::<_, String>(
                    "SELECT activation_id FROM task_outbox WHERE run_id=? AND task_id=?",
                )
                .bind(run_id.as_str())
                .bind(started.task_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
                .ok_or_else(RepositoryError::invalid_data)?;
                let attempt_started_projection = sqlx::query_scalar::<_, i64>(
                    "SELECT CASE WHEN worker_id IS NOT NULL AND started_at IS NOT NULL
                                      AND lifecycle IN (
                                          'running','succeeded','failed','timed_out',
                                          'abandoned','cancelled'
                                      )
                                      AND effect_evidence IN ('started','committed','unknown')
                                 THEN 1 ELSE 0 END
                     FROM node_attempts
                     WHERE run_id=? AND activation_id=? AND attempt_no=? AND lease_epoch=?
                       AND fencing_token=?",
                )
                .bind(run_id.as_str())
                .bind(started.activation_id.as_str())
                .bind(i64::from(started.attempt_no.get()))
                .bind(i64_from_u64(started.lease_epoch.get())?)
                .bind(&started.fencing_token)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
                .ok_or_else(RepositoryError::invalid_data)?;
                if task_activation != started.activation_id.as_str()
                    || attempt_started_projection != 1
                {
                    return Err(RepositoryError::invalid_data());
                }
                let (scope, node) =
                    activation_identity(&mut transaction, run_id, &started.activation_id).await?;
                let expected = model_data(PendingExecutionEvent::new(
                    ExecutionEventContext::for_run(run_id.clone()).for_attempt(
                        scope,
                        node,
                        started.activation_id,
                        started.attempt_no,
                    ),
                    ExecutionEventPayload::AttemptRunning {
                        lease_epoch: started.lease_epoch,
                    },
                ))?;
                validate_execution_event_sqlite(
                    &mut transaction,
                    run_id,
                    &transition,
                    &intent_hash,
                    &replay,
                    &expected,
                )
                .await?;
            }
            "task_retry_scheduled" => {
                let retry: TaskRetryFact = serde_json::from_value(payload_value)
                    .map_err(|_| RepositoryError::invalid_data())?;
                if checkpoint_id != operation_checkpoint(&transition)
                    || retry.next_attempt_no != model_data(retry.attempt_no.next())?
                    || retry.next_lease_epoch != model_data(retry.lease_epoch.next())?
                    || retry.next_fencing_token != fencing_token(&transition)
                    || retry.remaining_attempts == 0
                    || !retry_envelope_is_consistent(&retry, run_id, scheduler_version)
                {
                    return Err(RepositoryError::invalid_data());
                }
                let attempt = sqlx::query(
                    "SELECT lifecycle,effect_evidence,fencing_token,failure_code,
                            completion_transition_key,terminal_event_id
                     FROM node_attempts
                     WHERE run_id=? AND activation_id=? AND attempt_no=? AND lease_epoch=?",
                )
                .bind(run_id.as_str())
                .bind(retry.activation_id.as_str())
                .bind(i64::from(retry.attempt_no.get()))
                .bind(i64_from_u64(retry.lease_epoch.get())?)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
                .ok_or_else(RepositoryError::invalid_data)?;
                if attempt
                    .try_get::<String, _>("lifecycle")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != "failed"
                    || parse_effect_evidence(
                        &attempt
                            .try_get::<String, _>("effect_evidence")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )? != retry.effect_evidence
                    || attempt
                        .try_get::<String, _>("fencing_token")
                        .map_err(|_| RepositoryError::invalid_data())?
                        != retry.fencing_token
                    || attempt
                        .try_get::<Option<String>, _>("failure_code")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .as_deref()
                        != Some(retry.failure.code())
                    || attempt
                        .try_get::<Option<String>, _>("completion_transition_key")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .as_deref()
                        != Some(transition.as_str())
                    || attempt
                        .try_get::<Option<String>, _>("terminal_event_id")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .as_deref()
                        != Some(replay.event_id())
                {
                    return Err(RepositoryError::invalid_data());
                }
                let task = sqlx::query(
                    "SELECT activation_id,task_envelope FROM task_outbox
                     WHERE run_id=? AND task_id=?",
                )
                .bind(run_id.as_str())
                .bind(retry.task_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
                .ok_or_else(RepositoryError::invalid_data)?;
                let current_envelope = serde_json::from_str::<DurableTaskExecutionRequest>(
                    &task
                        .try_get::<String, _>("task_envelope")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?;
                let next_attempt = sqlx::query(
                    "SELECT fencing_token,effect_id FROM node_attempts
                     WHERE run_id=? AND activation_id=? AND attempt_no=? AND lease_epoch=?",
                )
                .bind(run_id.as_str())
                .bind(retry.activation_id.as_str())
                .bind(i64::from(retry.next_attempt_no.get()))
                .bind(i64_from_u64(retry.next_lease_epoch.get())?)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
                .ok_or_else(RepositoryError::invalid_data)?;
                if task
                    .try_get::<String, _>("activation_id")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != retry.activation_id.as_str()
                    || current_envelope.request().task_id() != &retry.task_id
                    || current_envelope.request().activation_id() != &retry.activation_id
                    || retry.next_envelope.request() != current_envelope.request()
                    || retry.remaining_attempts
                        != current_envelope
                            .request()
                            .effect_policy()
                            .max_attempts()
                            .saturating_sub(retry.attempt_no.get())
                    || next_attempt
                        .try_get::<String, _>("fencing_token")
                        .map_err(|_| RepositoryError::invalid_data())?
                        != retry.next_fencing_token
                    || next_attempt
                        .try_get::<String, _>("effect_id")
                        .map_err(|_| RepositoryError::invalid_data())?
                        != current_envelope.request().effect_id().as_str()
                {
                    return Err(RepositoryError::invalid_data());
                }
                let (scope, node) =
                    activation_identity(&mut transaction, run_id, &retry.activation_id).await?;
                let expected = model_data(PendingExecutionEvent::new(
                    ExecutionEventContext::for_run(run_id.clone()).for_attempt(
                        scope,
                        node,
                        retry.activation_id,
                        retry.attempt_no,
                    ),
                    ExecutionEventPayload::AttemptFailed {
                        failure: Some(internal_failure_from_fact(&retry.failure)?),
                    },
                ))?;
                validate_execution_event_sqlite(
                    &mut transaction,
                    run_id,
                    &transition,
                    &intent_hash,
                    &replay,
                    &expected,
                )
                .await?;
            }
            _ => return Err(RepositoryError::invalid_data()),
        }
    }
    let wait_winners = sqlx::query(
        "SELECT w.wait_id,w.winner_kind,w.winner_signal_id,w.winner_timer_id,
                p.payload_id AS payload_id,p.content_hash AS payload_content_hash,
                p.canonical_bytes AS payload_canonical_bytes,p.encoding AS payload_encoding,
                p.inline_value AS payload_inline_value,p.binary_value AS payload_binary_value
         FROM scheduler_wait_registrations w
         LEFT JOIN signals_inbox s ON s.run_id=w.run_id
              AND s.signal_id=w.winner_signal_id AND s.signal_state='consumed'
         LEFT JOIN payloads p ON p.run_id=s.run_id AND p.payload_id=s.payload_id
         WHERE w.run_id=? AND w.winner_kind IN ('signal','timer')
         ORDER BY w.wait_id",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for row in wait_winners {
        let wait_id = scheduler_data(insight_engine::SchedulerWaitId::parse(
            row.try_get::<String, _>("wait_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let (subject, payload) = match row
            .try_get::<String, _>("winner_kind")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_str()
        {
            "signal" => {
                let signal_id = model_data(insight_engine::SignalId::new(
                    row.try_get::<String, _>("winner_signal_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?;
                let value = common_contract_adapter::validated_inline_payload_value(
                    &restored_inline_payload_sqlite(&row)?
                        .ok_or_else(RepositoryError::invalid_data)?,
                )
                .clone();
                (
                    insight_engine::scheduler::WaitSubjectFact::Signal { signal_id },
                    Some(scheduler_data(RuntimeValue::new(value))?),
                )
            }
            "timer" => (
                insight_engine::scheduler::WaitSubjectFact::Timer {
                    timer_id: model_data(insight_engine::TimerId::new(
                        row.try_get::<String, _>("winner_timer_id")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    ))?,
                },
                None,
            ),
            _ => return Err(RepositoryError::invalid_data()),
        };
        let resolution = scheduler_data(insight_engine::scheduler::WaitResolutionFact::new(
            subject, payload,
        ))?;
        if !scheduler_data(facts.resolve_wait_first_winner(wait_id, resolution))? {
            return Err(RepositoryError::invalid_data());
        }
    }
    let terminal_subflows = sqlx::query(
        "SELECT i.child_run_id,i.output_contracts,i.invocation_state,
                c.lifecycle,c.error_code,c.output_value_hash AS expected_payload_hash,
                p.payload_id AS payload_id,p.content_hash AS payload_content_hash,
                p.canonical_bytes AS payload_canonical_bytes,p.encoding AS payload_encoding,
                p.inline_value AS payload_inline_value,p.binary_value AS payload_binary_value
         FROM scheduler_subflow_invocations i
         JOIN workflow_runs c ON c.run_id=i.child_run_id
         LEFT JOIN payloads p ON p.run_id=c.run_id AND p.payload_id=c.output_payload_id
         WHERE i.run_id=? AND c.lifecycle IN ('succeeded','failed','cancelled','timed_out','interrupted')
         ORDER BY i.child_run_id",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for row in terminal_subflows {
        let child_run_id = model_data(RunId::new(
            row.try_get::<String, _>("child_run_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let lifecycle: String = row
            .try_get("lifecycle")
            .map_err(|_| RepositoryError::invalid_data())?;
        let outcome = match lifecycle.as_str() {
            "succeeded" => {
                let contracts =
                    serde_json::from_str::<Vec<insight_engine::scheduler::TaskOutputContract>>(
                        &row.try_get::<String, _>("output_contracts")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )
                    .map_err(|_| RepositoryError::invalid_data())?;
                let payload = restored_inline_payload_sqlite(&row)?
                    .ok_or_else(RepositoryError::invalid_data)?;
                if row
                    .try_get::<Option<String>, _>("expected_payload_hash")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .as_deref()
                    != Some(
                        common_contract_adapter::validated_inline_payload_content_hash(&payload)
                            .as_str(),
                    )
                {
                    return Err(RepositoryError::invalid_data());
                }
                let raw = common_contract_adapter::validated_inline_payload_value(&payload).clone();
                let mut outputs = BTreeMap::new();
                for contract in &contracts {
                    let selected = raw
                        .as_object()
                        .and_then(|object| object.get(contract.name().as_str()))
                        .cloned()
                        .or_else(|| (contracts.len() == 1).then(|| raw.clone()));
                    let Some(selected) = selected else {
                        if contract.required() {
                            return Err(RepositoryError::invalid_data());
                        }
                        continue;
                    };
                    let value = scheduler_data(RuntimeValue::new(selected))?;
                    if !value.matches(contract.value_type()) {
                        return Err(RepositoryError::invalid_data());
                    }
                    outputs.insert(contract.port_id().clone(), value);
                }
                insight_engine::scheduler::SubflowOutcomeFact::Succeeded { outputs }
            }
            "cancelled" => insight_engine::scheduler::SubflowOutcomeFact::Cancelled,
            "failed" | "timed_out" | "interrupted" => {
                let checkpoint_rows = sqlx::query_scalar::<_, String>(
                    "SELECT transition_key FROM scheduler_checkpoints
                     WHERE run_id=? AND checkpoint_kind='planned_action'
                     ORDER BY scheduler_projection_version DESC,checkpoint_id DESC",
                )
                .bind(child_run_id.as_str())
                .fetch_all(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
                let mut failure = None;
                for transition_key in checkpoint_rows {
                    let checkpoint = validated_planned_action_checkpoint_sqlite(
                        &mut transaction,
                        &child_run_id,
                        &transition_key,
                    )
                    .await?;
                    match checkpoint.intent.action() {
                        SchedulerAction::FailRun { error, .. } => {
                            failure = Some(scheduler_data(TaskFailureFact::new(
                                insight_engine::WorkerFailureClass::SafeBusinessFailure,
                                error
                                    .value()
                                    .as_object()
                                    .and_then(|object| object.get("code"))
                                    .and_then(Value::as_str)
                                    .unwrap_or("SUBFLOW_SAFE_FAILURE"),
                                Some(error.runtime_value().clone()),
                            ))?);
                            break;
                        }
                        SchedulerAction::FailRunInternal { failure: value, .. } => {
                            failure = Some(value.clone());
                            break;
                        }
                        SchedulerAction::CancelRun { .. } => break,
                        _ => {}
                    }
                }
                let failure = failure.unwrap_or(scheduler_data(TaskFailureFact::new(
                    if lifecycle == "timed_out" || lifecycle == "interrupted" {
                        insight_engine::WorkerFailureClass::ControlTermination
                    } else {
                        insight_engine::WorkerFailureClass::InfrastructureFailure
                    },
                    row.try_get::<Option<String>, _>("error_code")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .as_deref()
                        .unwrap_or("SUBFLOW_TERMINAL_FAILURE"),
                    None,
                ))?);
                insight_engine::scheduler::SubflowOutcomeFact::Failed { failure }
            }
            _ => return Err(RepositoryError::invalid_data()),
        };
        facts.observe_subflow_outcome(child_run_id.clone(), outcome.clone());
        if row
            .try_get::<String, _>("invocation_state")
            .map_err(|_| RepositoryError::invalid_data())?
            == "completed"
        {
            facts.settle_subflow(child_run_id, outcome);
        }
    }
    let drained_scopes = sqlx::query_scalar::<_, String>(
        "SELECT scope_instance_id FROM scope_instances
         WHERE run_id=? AND lifecycle='cancelled' AND admission_state='closed'
         ORDER BY scope_instance_id",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for scope in drained_scopes {
        let scope = model_data(insight_engine::ScopeInstanceId::new(scope))?;
        facts.record_scope_cancelled_and_drained(&scope);
    }
    let values = sqlx::query(
        "SELECT port_id,owner_activation_id,runtime_value,value_ref,declared_type,storage_kind,
                payload_id,artifact_id,content_hash,projection_version
         FROM scheduler_values WHERE run_id=? ORDER BY port_id",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for row in values {
        let stored = stored_value_from_row(run_id, &row)?;
        validate_value_ref_resource_sqlite(&mut transaction, run_id, stored.value_ref()).await?;
        facts.record_value_from(
            stored.port_id().clone(),
            model_data(ActivationId::new(
                row.try_get::<String, _>("owner_activation_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            stored.runtime_value().clone(),
        );
    }
    let occurrence_values = sqlx::query(
        "SELECT occurrence_key,port_id,owner_activation_id,runtime_value,value_ref,
                declared_type,storage_kind,payload_id,artifact_id,content_hash,projection_version
         FROM scheduler_occurrence_values WHERE run_id=?
         ORDER BY occurrence_key,port_id",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for row in occurrence_values {
        let occurrence = serde_json::from_str::<insight_engine::LogicalOccurrence>(
            &row.try_get::<String, _>("occurrence_key")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let stored = stored_value_from_row(run_id, &row)?;
        validate_value_ref_resource_sqlite(&mut transaction, run_id, stored.value_ref()).await?;
        let occurrence_key = canonical_json(
            &serde_json::to_value(&occurrence).map_err(|_| RepositoryError::canonicalization())?,
        )?;
        if let Some(receipt) =
            expected_occurrence_receipts.remove(&(occurrence_key, stored.port_id().clone()))
        {
            let storage_kind: String = row
                .try_get("storage_kind")
                .map_err(|_| RepositoryError::invalid_data())?;
            let payload_id: Option<String> = row
                .try_get("payload_id")
                .map_err(|_| RepositoryError::invalid_data())?;
            let artifact_id: Option<String> = row
                .try_get("artifact_id")
                .map_err(|_| RepositoryError::invalid_data())?;
            if receipt.owner_activation_id.as_str()
                != row
                    .try_get::<String, _>("owner_activation_id")
                    .map_err(|_| RepositoryError::invalid_data())?
                || receipt.runtime_value != *stored.runtime_value()
                || receipt.declared_type != *stored.declared_type()
                || receipt.occurrence_value_ref != *stored.value_ref()
                || receipt.occurrence_storage_kind != storage_kind
                || receipt.occurrence_payload_id != payload_id
                || receipt.occurrence_artifact_id != artifact_id
                || receipt.occurrence_projection_version != stored.projection_version()
            {
                return Err(RepositoryError::invalid_data());
            }
        }
        facts.record_occurrence_value_from(
            occurrence,
            stored.port_id().clone(),
            model_data(ActivationId::new(
                row.try_get::<String, _>("owner_activation_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            stored.runtime_value().clone(),
        );
    }
    if !expected_occurrence_receipts.is_empty() {
        return Err(RepositoryError::invalid_data());
    }
    let reused = sqlx::query_scalar::<_, String>(
        "SELECT activation_id FROM node_activations
         WHERE run_id=? AND reused_from_activation_id IS NOT NULL
         ORDER BY activation_id",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    for activation_id in reused {
        facts.record_reused_activation(model_data(ActivationId::new(activation_id))?);
    }
    let snapshot_version = facts.projection_version();
    let current_version =
        sqlx::query_scalar::<_, i64>("SELECT projection_version FROM workflow_runs WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(RepositoryError::invalid_data)?;
    if u64_from_i64(current_version)? != snapshot_version {
        return Err(RepositoryError::invalid_data());
    }
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(facts)
}

async fn current_scheduler_version(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
) -> Result<u64, RepositoryError> {
    u64_from_i64(
        sqlx::query_scalar::<_, i64>("SELECT projection_version FROM workflow_runs WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(RepositoryError::invalid_data)?,
    )
}

fn task_started_fact(claim: &SchedulerTaskClaim) -> TaskStartedFact {
    TaskStartedFact {
        task_id: claim.task_id().clone(),
        activation_id: claim.activation_id().clone(),
        attempt_no: claim.envelope().attempt_no(),
        lease_epoch: claim.envelope().lease_epoch(),
        fencing_token: claim.envelope().fencing_token().to_owned(),
        claimed_by: claim.claimed_by().to_owned(),
        claim_token: claim.claim_token().to_owned(),
    }
}

async fn insert_task_started_checkpoint_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &SchedulerTaskClaim,
    transition_key: &TransitionKey,
    intent_hash: &str,
    event_id_value: &str,
    scheduler_projection_version: u64,
) -> Result<SchedulerCheckpointId, RepositoryError> {
    let checkpoint_id = operation_checkpoint(transition_key);
    let fact_payload = serde_json::to_value(task_started_fact(claim))
        .map_err(|_| RepositoryError::canonicalization())?;
    let content_hash = scheduler_checkpoint_content_hash(
        claim.run_id().as_str(),
        checkpoint_id.as_str(),
        "task_started",
        transition_key.as_str(),
        intent_hash,
        event_id_value,
        SCHEDULER_CHECKPOINT_SCHEMA_VERSION,
        scheduler_projection_version,
        &fact_payload,
    )?;
    sqlx::query(
        "INSERT INTO scheduler_checkpoints (
            run_id,checkpoint_id,content_hash,checkpoint_kind,transition_key,intent_hash,event_id,
            checkpoint_schema_version,scheduler_projection_version,fact_payload,
            projection_version,created_at
         ) VALUES (?,?,?,'task_started',?,?,?,?,?,?,0,CURRENT_TIMESTAMP)",
    )
    .bind(claim.run_id().as_str())
    .bind(checkpoint_id.as_str())
    .bind(content_hash.as_str())
    .bind(transition_key.as_str())
    .bind(intent_hash)
    .bind(event_id_value)
    .bind(i64::from(SCHEDULER_CHECKPOINT_SCHEMA_VERSION))
    .bind(i64_from_u64(scheduler_projection_version)?)
    .bind(canonical_json(&fact_payload)?)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(checkpoint_id)
}

async fn exact_task_started_receipt_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &SchedulerTaskClaim,
    transition_key: &TransitionKey,
    intent_hash: &str,
    replay: &super::CommitReceipt,
) -> Result<SchedulerCommitReceipt, RepositoryError> {
    let row = sqlx::query(
        "SELECT checkpoint_id,content_hash,checkpoint_kind,transition_key,intent_hash,event_id,
                checkpoint_schema_version,scheduler_projection_version,fact_payload
         FROM scheduler_checkpoints WHERE run_id=? AND transition_key=?",
    )
    .bind(claim.run_id().as_str())
    .bind(transition_key.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let checkpoint_id = scheduler_data(SchedulerCheckpointId::parse(
        row.try_get::<String, _>("checkpoint_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let schema_version = u32::try_from(
        row.try_get::<i64, _>("checkpoint_schema_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let scheduler_version = u64_from_i64(
        row.try_get("scheduler_projection_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let payload_text: String = row
        .try_get("fact_payload")
        .map_err(|_| RepositoryError::invalid_data())?;
    let payload: Value =
        serde_json::from_str(&payload_text).map_err(|_| RepositoryError::invalid_data())?;
    let stored_fact: TaskStartedFact =
        serde_json::from_value(payload.clone()).map_err(|_| RepositoryError::invalid_data())?;
    let stored_event: String = row
        .try_get("event_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let stored_hash: String = row
        .try_get("content_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    if checkpoint_id != operation_checkpoint(transition_key)
        || row
            .try_get::<String, _>("checkpoint_kind")
            .map_err(|_| RepositoryError::invalid_data())?
            != "task_started"
        || row
            .try_get::<String, _>("transition_key")
            .map_err(|_| RepositoryError::invalid_data())?
            != transition_key.as_str()
        || row
            .try_get::<String, _>("intent_hash")
            .map_err(|_| RepositoryError::invalid_data())?
            != intent_hash
        || stored_event != replay.event_id()
        || schema_version != SCHEDULER_CHECKPOINT_SCHEMA_VERSION
        || scheduler_version != replay.projection_version()
        || stored_fact != task_started_fact(claim)
        || scheduler_checkpoint_content_hash(
            claim.run_id().as_str(),
            checkpoint_id.as_str(),
            "task_started",
            transition_key.as_str(),
            intent_hash,
            &stored_event,
            schema_version,
            scheduler_version,
            &payload,
        )?
        .as_str()
            != stored_hash
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(SchedulerCommitReceipt::new(
        replay.event_seq(),
        stored_event,
        checkpoint_id,
        scheduler_version,
    ))
}

async fn validate_execution_event_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    transition_key: &TransitionKey,
    intent_hash: &str,
    replay: &super::CommitReceipt,
    expected: &PendingExecutionEvent,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        "SELECT schema_version,seq,event_id,run_id,transition_key,intent_hash,
                projection_version_after,kind,node_id,scope_instance_id,activation_id,
                attempt_no,causation_event_id,safe_payload,occurred_at
         FROM execution_events WHERE run_id=? AND transition_key=?",
    )
    .bind(run_id.as_str())
    .bind(transition_key.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let execution = decode_execution_event_row(&row)?;
    if execution.event_id().as_str() != replay.event_id()
        || execution.event_id().as_str() != event_id(transition_key)
        || execution.seq().get() != replay.event_seq()
        || execution.run_id() != run_id
        || execution.transition_key() != transition_key
        || execution.intent_hash().as_str() != intent_hash
        || u64_from_i64(
            row.try_get("projection_version_after")
                .map_err(|_| RepositoryError::invalid_data())?,
        )? != replay.projection_version()
        || execution.payload() != expected.payload()
        || execution.node_id() != expected.context().node_id()
        || execution.scope_instance_id() != expected.context().scope_instance_id()
        || execution.activation_id() != expected.context().activation_id()
        || execution.attempt_no() != expected.context().attempt_no()
        || execution.causation_event_id() != expected.context().causation_event_id()
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn validate_task_transition_event_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &SchedulerTaskClaim,
    transition_key: &TransitionKey,
    intent_hash: &str,
    replay: &super::CommitReceipt,
    expected_payload: &ExecutionEventPayload,
    expect_attempt_context: bool,
) -> Result<(), RepositoryError> {
    let context = if expect_attempt_context {
        let (scope, node) =
            activation_identity(transaction, claim.run_id(), claim.activation_id()).await?;
        ExecutionEventContext::for_run(claim.run_id().clone()).for_attempt(
            scope,
            node,
            claim.activation_id().clone(),
            claim.envelope().attempt_no(),
        )
    } else {
        ExecutionEventContext::for_run(claim.run_id().clone())
    };
    let expected = model_data(PendingExecutionEvent::new(
        context,
        expected_payload.clone(),
    ))?;
    validate_execution_event_sqlite(
        transaction,
        claim.run_id(),
        transition_key,
        intent_hash,
        replay,
        &expected,
    )
    .await
}

async fn validate_task_completion_projection_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    transition: &TransitionKey,
    intent_hash: &str,
    replay: &super::CommitReceipt,
    completion: &TaskCompletionFact,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        "SELECT o.activation_id,o.attempt_no,o.lease_epoch,o.fencing_token,o.effect_id,
                o.task_state,o.task_envelope,o.created_by_transition_key,o.last_error_code,
                v.lifecycle AS activation_lifecycle,
                v.effect_evidence AS activation_effect_evidence,
                v.last_attempt_no,v.last_lease_epoch,v.current_attempt_no,v.current_lease_epoch,
                v.current_fencing_token,v.winning_attempt_no,v.stable_activation_key,
                v.termination_intent_transition_key,
                a.lifecycle AS attempt_lifecycle,a.effect_evidence AS attempt_effect_evidence,
                a.fencing_token AS attempt_fencing_token,a.effect_id AS attempt_effect_id,
                a.failure_code,a.completion_transition_key,a.terminal_event_id
         FROM task_outbox o
         JOIN node_activations v ON v.run_id=o.run_id AND v.activation_id=o.activation_id
         JOIN node_attempts a ON a.run_id=o.run_id AND a.activation_id=o.activation_id
           AND a.attempt_no=o.attempt_no AND a.lease_epoch=o.lease_epoch
           AND a.fencing_token=o.fencing_token
         WHERE o.run_id=? AND o.task_id=?",
    )
    .bind(run_id.as_str())
    .bind(completion.task_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let activation_id = model_data(ActivationId::new(
        row.try_get::<String, _>("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let envelope = serde_json::from_str::<DurableTaskExecutionRequest>(
        &row.try_get::<String, _>("task_envelope")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let occurrence = serde_json::from_str::<insight_engine::LogicalOccurrence>(
        &row.try_get::<String, _>("stable_activation_key")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let task_state: String = row
        .try_get("task_state")
        .map_err(|_| RepositoryError::invalid_data())?;
    let attempt_lifecycle: String = row
        .try_get("attempt_lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?;
    let activation_lifecycle: String = row
        .try_get("activation_lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?;
    let attempt_evidence = parse_effect_evidence(
        &row.try_get::<String, _>("attempt_effect_evidence")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let activation_evidence = parse_effect_evidence(
        &row.try_get::<String, _>("activation_effect_evidence")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let origin_transition: String = row
        .try_get("created_by_transition_key")
        .map_err(|_| RepositoryError::invalid_data())?;
    let origin =
        validated_planned_action_checkpoint_sqlite(transaction, run_id, &origin_transition).await?;
    let origin_request =
        insight_engine::worker::TaskExecutionRequest::from_scheduler_intent(&origin.intent)
            .map_err(|_| RepositoryError::invalid_data())?;
    if origin_request != *envelope.request()
        || completion.occurrence != occurrence
        || envelope.request().task_id() != &completion.task_id
        || envelope.request().activation_id() != &activation_id
        || u32::try_from(
            row.try_get::<i64, _>("attempt_no")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?
            != envelope.attempt_no().get()
        || u64_from_i64(
            row.try_get("lease_epoch")
                .map_err(|_| RepositoryError::invalid_data())?,
        )? != envelope.lease_epoch().get()
        || row
            .try_get::<String, _>("fencing_token")
            .map_err(|_| RepositoryError::invalid_data())?
            != envelope.fencing_token()
        || row
            .try_get::<String, _>("effect_id")
            .map_err(|_| RepositoryError::invalid_data())?
            != envelope.request().effect_id().as_str()
        || row
            .try_get::<String, _>("attempt_fencing_token")
            .map_err(|_| RepositoryError::invalid_data())?
            != envelope.fencing_token()
        || row
            .try_get::<String, _>("attempt_effect_id")
            .map_err(|_| RepositoryError::invalid_data())?
            != envelope.request().effect_id().as_str()
        || row
            .try_get::<Option<String>, _>("completion_transition_key")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            != Some(transition.as_str())
        || row
            .try_get::<Option<String>, _>("terminal_event_id")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            != Some(replay.event_id())
        || row
            .try_get::<Option<i64>, _>("last_attempt_no")
            .map_err(|_| RepositoryError::invalid_data())?
            != Some(i64::from(envelope.attempt_no().get()))
        || row
            .try_get::<Option<i64>, _>("last_lease_epoch")
            .map_err(|_| RepositoryError::invalid_data())?
            != Some(i64_from_u64(envelope.lease_epoch().get())?)
        || row
            .try_get::<Option<i64>, _>("current_attempt_no")
            .map_err(|_| RepositoryError::invalid_data())?
            .is_some()
        || row
            .try_get::<Option<i64>, _>("current_lease_epoch")
            .map_err(|_| RepositoryError::invalid_data())?
            .is_some()
        || row
            .try_get::<Option<String>, _>("current_fencing_token")
            .map_err(|_| RepositoryError::invalid_data())?
            .is_some()
    {
        return Err(RepositoryError::invalid_data());
    }
    let expected_event = match &completion.outcome {
        TaskOutcomeFact::Succeeded { outputs } => {
            if !matches!(task_state.as_str(), "published" | "acked")
                || attempt_lifecycle != "succeeded"
                || activation_lifecycle != "succeeded"
                || attempt_evidence != EffectEvidence::Committed
                || activation_evidence != EffectEvidence::Committed
                || row
                    .try_get::<Option<i64>, _>("winning_attempt_no")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != Some(i64::from(envelope.attempt_no().get()))
                || row
                    .try_get::<Option<String>, _>("failure_code")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .is_some()
                || row
                    .try_get::<Option<String>, _>("last_error_code")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .is_some()
            {
                return Err(RepositoryError::invalid_data());
            }
            let contracts = envelope.request().outputs();
            if outputs
                .keys()
                .any(|port| !contracts.iter().any(|contract| contract.port_id() == port))
                || contracts.iter().any(|contract| {
                    contract.required() && !outputs.contains_key(contract.port_id())
                })
            {
                return Err(RepositoryError::invalid_data());
            }
            for (port, value) in outputs {
                let contract = contracts
                    .iter()
                    .find(|contract| contract.port_id() == port)
                    .ok_or_else(RepositoryError::invalid_data)?;
                let receipt = completion
                    .output_receipts
                    .get(port)
                    .ok_or_else(RepositoryError::invalid_data)?;
                if receipt.owner_activation_id != activation_id
                    || receipt.occurrence != occurrence
                    || receipt.declared_type != *contract.value_type()
                    || receipt.runtime_value != *value
                {
                    return Err(RepositoryError::invalid_data());
                }
            }
            let encoded = Value::Object(
                outputs
                    .iter()
                    .map(|(port, value)| (port.as_str().to_owned(), value.value().clone()))
                    .collect(),
            );
            ExecutionEventPayload::AttemptSucceeded {
                output: Some(output_summary(&encoded)?),
            }
        }
        TaskOutcomeFact::Failed { failure } => {
            let timed_out = attempt_lifecycle == "timed_out";
            if task_state != "dead"
                || !matches!(attempt_lifecycle.as_str(), "failed" | "timed_out")
                || activation_lifecycle != attempt_lifecycle
                || activation_evidence != attempt_evidence
                || row
                    .try_get::<Option<i64>, _>("winning_attempt_no")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .is_some()
                || row
                    .try_get::<Option<String>, _>("failure_code")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .as_deref()
                    != Some(failure.code())
                || row
                    .try_get::<Option<String>, _>("last_error_code")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .as_deref()
                    != Some(failure.code())
                || row
                    .try_get::<Option<String>, _>("termination_intent_transition_key")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .as_deref()
                    != Some(transition.as_str())
            {
                return Err(RepositoryError::invalid_data());
            }
            if timed_out {
                ExecutionEventPayload::AttemptTimedOut
            } else {
                ExecutionEventPayload::AttemptFailed {
                    failure: Some(internal_failure_from_fact(failure)?),
                }
            }
        }
    };
    let (scope, node) = activation_identity(transaction, run_id, &activation_id).await?;
    let expected = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(run_id.clone()).for_attempt(
            scope,
            node,
            activation_id,
            envelope.attempt_no(),
        ),
        expected_event,
    ))?;
    validate_execution_event_sqlite(
        transaction,
        run_id,
        transition,
        intent_hash,
        replay,
        &expected,
    )
    .await
}

fn validate_claim_parameters(
    claimed_by: &str,
    claim_seconds: u32,
    limit: u32,
) -> Result<(), RepositoryError> {
    if claimed_by.is_empty()
        || claimed_by.len() > 256
        || claimed_by
            .chars()
            .any(|value| value.is_control() || value.is_whitespace())
        || !(3..=MAX_CLAIM_SECONDS).contains(&claim_seconds)
        || limit == 0
        || limit > MAX_CLAIM_LIMIT
    {
        return Err(RepositoryError::invalid_configuration());
    }
    Ok(())
}

async fn load_latest_parent_model_call_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    activation_id: &ActivationId,
    attempt_no: AttemptNo,
) -> Result<Option<LatestParentModelCallView>, RepositoryError> {
    let row = sqlx::query(
        "SELECT u.model_call_no,u.task_id,u.lease_epoch,u.fencing_token,
                u.call_status,u.finish_reason,b.execution_status,b.continuation_status
         FROM model_call_usage u
         LEFT JOIN model_tool_call_batches b ON b.run_id=u.run_id
           AND b.activation_id=u.activation_id AND b.attempt_no=u.attempt_no
           AND b.model_call_no=u.model_call_no
         WHERE u.run_id=? AND u.activation_id=? AND u.attempt_no=?
         ORDER BY u.model_call_no DESC LIMIT 1",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .bind(i64::from(attempt_no.get()))
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    row.map(|row| {
        Ok(latest_parent_model_call_view(
            u32::try_from(
                row.try_get::<i64, _>("model_call_no")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("task_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            u64_from_i64(
                row.try_get("lease_epoch")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
            row.try_get("fencing_token")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("call_status")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("finish_reason")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("execution_status")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("continuation_status")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))
    })
    .transpose()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskClaimComparison {
    Exact,
    AcknowledgeTransition,
}

struct AuthoritativeTaskClaim {
    claim: SchedulerTaskClaim,
    task_state: String,
    run_lifecycle: String,
    attempt_lifecycle: String,
    activation_lifecycle: String,
    current_effect_evidence: EffectEvidence,
    started_at: Option<DateTime<Utc>>,
    claim_is_fresh: bool,
    attempt_lease_is_fresh: bool,
    database_now: DateTime<Utc>,
}

impl AuthoritativeTaskClaim {
    fn permits_execution(&self) -> bool {
        matches!(
            self.run_lifecycle.as_str(),
            "created" | "active" | "waiting"
        ) || (self.run_lifecycle == "terminating"
            && self.claim.envelope().request().admission_class()
                == insight_engine::TaskAdmissionClass::TerminationFinalizer)
    }

    fn is_fresh(&self) -> bool {
        self.claim_is_fresh
            && (self.claim.mode() != SchedulerTaskClaimMode::Execute || self.attempt_lease_is_fresh)
    }
}

fn same_claim_snapshot(left: &SchedulerTaskClaim, right: &SchedulerTaskClaim) -> bool {
    left.envelope() == right.envelope()
        && left.claimed_by() == right.claimed_by()
        && left.claim_token() == right.claim_token()
        && left.claim_expires_at().timestamp_micros() == right.claim_expires_at().timestamp_micros()
        && left.task_projection_version() == right.task_projection_version()
        && left.mode() == right.mode()
        && left.lease_loss_evidence() == right.lease_loss_evidence()
}

fn same_acknowledgement_snapshot(
    authority: &AuthoritativeTaskClaim,
    supplied: &SchedulerTaskClaim,
) -> bool {
    let stored = &authority.claim;
    if stored.envelope() != supplied.envelope()
        || stored.claimed_by() != supplied.claimed_by()
        || stored.claim_token() != supplied.claim_token()
        || stored.claim_expires_at().timestamp_micros()
            != supplied.claim_expires_at().timestamp_micros()
        || stored.mode() != SchedulerTaskClaimMode::Acknowledge
        || supplied.lease_loss_evidence().is_some()
        || !matches!(
            supplied.mode(),
            SchedulerTaskClaimMode::Execute | SchedulerTaskClaimMode::Acknowledge
        )
    {
        return false;
    }
    let expected_delta = match (authority.task_state.as_str(), supplied.mode()) {
        ("published", SchedulerTaskClaimMode::Execute)
        | ("acked", SchedulerTaskClaimMode::Acknowledge) => 1,
        ("published", SchedulerTaskClaimMode::Acknowledge) => 0,
        ("acked", SchedulerTaskClaimMode::Execute) => 2,
        _ => return false,
    };
    supplied
        .task_projection_version()
        .checked_add(expected_delta)
        == Some(stored.task_projection_version())
}

async fn validate_retry_lineage_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    envelope: &DurableTaskExecutionRequest,
) -> Result<(), RepositoryError> {
    let rows = sqlx::query(
        "SELECT checkpoint_id,content_hash,checkpoint_kind,transition_key,intent_hash,event_id,
                checkpoint_schema_version,scheduler_projection_version,fact_payload
         FROM scheduler_checkpoints
         WHERE run_id=? AND checkpoint_kind='task_retry_scheduled'
         ORDER BY scheduler_projection_version,checkpoint_id",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let mut matches = 0_u32;
    for row in rows {
        let checkpoint_id = scheduler_data(SchedulerCheckpointId::parse(
            row.try_get::<String, _>("checkpoint_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let transition = model_data(TransitionKey::parse(
            row.try_get::<String, _>("transition_key")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let intent_hash: String = row
            .try_get("intent_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        let event_id_value: String = row
            .try_get("event_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let schema_version = u32::try_from(
            row.try_get::<i64, _>("checkpoint_schema_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let scheduler_version = u64_from_i64(
            row.try_get("scheduler_projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let payload_text: String = row
            .try_get("fact_payload")
            .map_err(|_| RepositoryError::invalid_data())?;
        let payload: Value =
            serde_json::from_str(&payload_text).map_err(|_| RepositoryError::invalid_data())?;
        let retry: TaskRetryFact =
            serde_json::from_value(payload.clone()).map_err(|_| RepositoryError::invalid_data())?;
        let stored_hash: String = row
            .try_get("content_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        if row
            .try_get::<String, _>("checkpoint_kind")
            .map_err(|_| RepositoryError::invalid_data())?
            != "task_retry_scheduled"
            || checkpoint_id != operation_checkpoint(&transition)
            || schema_version != SCHEDULER_CHECKPOINT_SCHEMA_VERSION
            || retry.next_attempt_no != model_data(retry.attempt_no.next())?
            || retry.next_lease_epoch != model_data(retry.lease_epoch.next())?
            || retry.next_fencing_token != fencing_token(&transition)
            || retry.remaining_attempts == 0
            || !retry_envelope_is_consistent(&retry, run_id, scheduler_version)
            || scheduler_checkpoint_content_hash(
                run_id.as_str(),
                checkpoint_id.as_str(),
                "task_retry_scheduled",
                transition.as_str(),
                &intent_hash,
                &event_id_value,
                schema_version,
                scheduler_version,
                &payload,
            )?
            .as_str()
                != stored_hash
        {
            return Err(RepositoryError::invalid_data());
        }
        let replay = match load_replay(transaction, run_id, &transition, &intent_hash).await? {
            Replay::Exact(replay)
                if replay.event_id() == event_id_value
                    && replay.projection_version() == scheduler_version =>
            {
                replay
            }
            Replay::Exact(_) | Replay::Vacant => return Err(RepositoryError::invalid_data()),
        };
        let (scope, node) = activation_identity(transaction, run_id, &retry.activation_id).await?;
        let expected_event = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(run_id.clone()).for_attempt(
                scope,
                node,
                retry.activation_id.clone(),
                retry.attempt_no,
            ),
            ExecutionEventPayload::AttemptFailed {
                failure: Some(internal_failure_from_fact(&retry.failure)?),
            },
        ))?;
        validate_execution_event_sqlite(
            transaction,
            run_id,
            &transition,
            &intent_hash,
            &replay,
            &expected_event,
        )
        .await?;
        let attempt = sqlx::query(
            "SELECT lifecycle,effect_evidence,fencing_token,failure_code,
                    completion_transition_key,terminal_event_id
             FROM node_attempts
             WHERE run_id=? AND activation_id=? AND attempt_no=? AND lease_epoch=?",
        )
        .bind(run_id.as_str())
        .bind(retry.activation_id.as_str())
        .bind(i64::from(retry.attempt_no.get()))
        .bind(i64_from_u64(retry.lease_epoch.get())?)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?
        .ok_or_else(RepositoryError::invalid_data)?;
        if attempt
            .try_get::<String, _>("lifecycle")
            .map_err(|_| RepositoryError::invalid_data())?
            != "failed"
            || parse_effect_evidence(
                &attempt
                    .try_get::<String, _>("effect_evidence")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )? != retry.effect_evidence
            || attempt
                .try_get::<String, _>("fencing_token")
                .map_err(|_| RepositoryError::invalid_data())?
                != retry.fencing_token
            || attempt
                .try_get::<Option<String>, _>("failure_code")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_deref()
                != Some(retry.failure.code())
            || attempt
                .try_get::<Option<String>, _>("completion_transition_key")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_deref()
                != Some(transition.as_str())
            || attempt
                .try_get::<Option<String>, _>("terminal_event_id")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_deref()
                != Some(event_id_value.as_str())
        {
            return Err(RepositoryError::invalid_data());
        }
        if retry.task_id == *envelope.request().task_id()
            && retry.activation_id == *envelope.request().activation_id()
            && retry.next_attempt_no == envelope.attempt_no()
            && retry.next_lease_epoch == envelope.lease_epoch()
            && retry.next_fencing_token == envelope.fencing_token()
            && retry.next_envelope == *envelope
            && scheduler_version == envelope.dispatch_scheduler_projection_version()
        {
            let projection = sqlx::query(
                "SELECT o.task_state,o.activation_id,o.attempt_no,o.lease_epoch,o.fencing_token,
                        o.effect_id,o.task_envelope,o.available_at,o.last_error_code,o.claimed_by,
                        v.lifecycle AS activation_lifecycle,
                        v.effect_evidence AS activation_effect_evidence,
                        v.last_attempt_no,v.last_lease_epoch,v.current_attempt_no,
                        v.current_lease_epoch,v.current_fencing_token,v.retry_budget_remaining,
                        a.lifecycle AS next_attempt_lifecycle,
                        a.effect_evidence AS next_attempt_effect_evidence,
                        a.fencing_token AS next_attempt_fencing_token,
                        a.effect_id AS next_attempt_effect_id,a.worker_id AS next_attempt_worker_id
                 FROM task_outbox o
                 JOIN node_activations v ON v.run_id=o.run_id AND v.activation_id=o.activation_id
                 JOIN node_attempts a ON a.run_id=o.run_id AND a.activation_id=o.activation_id
                   AND a.attempt_no=o.attempt_no AND a.lease_epoch=o.lease_epoch
                   AND a.fencing_token=o.fencing_token
                 WHERE o.run_id=? AND o.task_id=?",
            )
            .bind(run_id.as_str())
            .bind(retry.task_id.as_str())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(RepositoryError::invalid_data)?;
            let task_state: String = projection
                .try_get("task_state")
                .map_err(|_| RepositoryError::invalid_data())?;
            let stored_envelope = serde_json::from_str::<DurableTaskExecutionRequest>(
                &projection
                    .try_get::<String, _>("task_envelope")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?;
            let available_at = parse_run_timestamp(
                &projection
                    .try_get::<String, _>("available_at")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let activation_lifecycle: String = projection
                .try_get("activation_lifecycle")
                .map_err(|_| RepositoryError::invalid_data())?;
            let activation_evidence = parse_effect_evidence(
                &projection
                    .try_get::<String, _>("activation_effect_evidence")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let attempt_lifecycle: String = projection
                .try_get("next_attempt_lifecycle")
                .map_err(|_| RepositoryError::invalid_data())?;
            let attempt_evidence = parse_effect_evidence(
                &projection
                    .try_get::<String, _>("next_attempt_effect_evidence")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let expected_budget = envelope
                .request()
                .effect_policy()
                .max_attempts()
                .saturating_sub(envelope.attempt_no().get());
            let active_projection_valid = match task_state.as_str() {
                "pending" => {
                    activation_lifecycle == "leased"
                        && activation_evidence == EffectEvidence::NotStarted
                        && attempt_lifecycle == "leased"
                        && attempt_evidence == EffectEvidence::NotStarted
                        && projection
                            .try_get::<Option<String>, _>("next_attempt_worker_id")
                            .map_err(|_| RepositoryError::invalid_data())?
                            .as_deref()
                            == Some("scheduler-outbox")
                }
                "claimed" => {
                    (activation_lifecycle == "leased"
                        && activation_evidence == EffectEvidence::NotStarted
                        && attempt_lifecycle == "leased"
                        && attempt_evidence == EffectEvidence::NotStarted)
                        || (activation_lifecycle == "running"
                            && activation_evidence == EffectEvidence::Started
                            && attempt_lifecycle == "running"
                            && attempt_evidence == EffectEvidence::Started
                            && projection
                                .try_get::<Option<String>, _>("next_attempt_worker_id")
                                .map_err(|_| RepositoryError::invalid_data())?
                                == projection
                                    .try_get::<Option<String>, _>("claimed_by")
                                    .map_err(|_| RepositoryError::invalid_data())?)
                }
                "published" | "acked" | "dead" => true,
                _ => false,
            };
            if projection
                .try_get::<String, _>("activation_id")
                .map_err(|_| RepositoryError::invalid_data())?
                != retry.activation_id.as_str()
                || u32::try_from(
                    projection
                        .try_get::<i64, _>("attempt_no")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?
                    != retry.next_attempt_no.get()
                || u64_from_i64(
                    projection
                        .try_get("lease_epoch")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )? != retry.next_lease_epoch.get()
                || projection
                    .try_get::<String, _>("fencing_token")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != retry.next_fencing_token
                || projection
                    .try_get::<String, _>("effect_id")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != envelope.request().effect_id().as_str()
                || stored_envelope != *envelope
                || available_at.timestamp_micros() != retry.retry_at.timestamp_micros()
                || (matches!(task_state.as_str(), "pending" | "claimed")
                    && projection
                        .try_get::<Option<String>, _>("last_error_code")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .as_deref()
                        != Some(retry.failure.code()))
                || u32::try_from(
                    projection
                        .try_get::<Option<i64>, _>("last_attempt_no")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .ok_or_else(RepositoryError::invalid_data)?,
                )
                .map_err(|_| RepositoryError::invalid_data())?
                    != retry.next_attempt_no.get()
                || u64_from_i64(
                    projection
                        .try_get::<Option<i64>, _>("last_lease_epoch")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .ok_or_else(RepositoryError::invalid_data)?,
                )? != retry.next_lease_epoch.get()
                || u32::try_from(
                    projection
                        .try_get::<i64, _>("retry_budget_remaining")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?
                    != expected_budget
                || projection
                    .try_get::<String, _>("next_attempt_fencing_token")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != retry.next_fencing_token
                || projection
                    .try_get::<String, _>("next_attempt_effect_id")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != envelope.request().effect_id().as_str()
                || !active_projection_valid
                || (matches!(task_state.as_str(), "pending" | "claimed")
                    && (projection
                        .try_get::<Option<i64>, _>("current_attempt_no")
                        .map_err(|_| RepositoryError::invalid_data())?
                        != Some(i64::from(retry.next_attempt_no.get()))
                        || projection
                            .try_get::<Option<i64>, _>("current_lease_epoch")
                            .map_err(|_| RepositoryError::invalid_data())?
                            != Some(i64_from_u64(retry.next_lease_epoch.get())?)
                        || projection
                            .try_get::<Option<String>, _>("current_fencing_token")
                            .map_err(|_| RepositoryError::invalid_data())?
                            .as_deref()
                            != Some(retry.next_fencing_token.as_str())))
            {
                return Err(RepositoryError::invalid_data());
            }
            matches = matches
                .checked_add(1)
                .ok_or_else(RepositoryError::invalid_data)?;
        }
    }
    if matches != 1 {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn validate_task_request_origin_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    created_by_transition_key: &str,
    envelope: &DurableTaskExecutionRequest,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        "SELECT checkpoint_id,content_hash,checkpoint_kind,transition_key,intent_hash,event_id,
                checkpoint_schema_version,scheduler_projection_version,fact_payload
         FROM scheduler_checkpoints WHERE run_id=? AND transition_key=?",
    )
    .bind(run_id.as_str())
    .bind(created_by_transition_key)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let checkpoint_id = scheduler_data(SchedulerCheckpointId::parse(
        row.try_get::<String, _>("checkpoint_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let transition_key = model_data(TransitionKey::parse(
        row.try_get::<String, _>("transition_key")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let intent_hash: String = row
        .try_get("intent_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    let event_id_value: String = row
        .try_get("event_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let schema_version = u32::try_from(
        row.try_get::<i64, _>("checkpoint_schema_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let scheduler_projection_version = u64_from_i64(
        row.try_get("scheduler_projection_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let fact_payload_text: String = row
        .try_get("fact_payload")
        .map_err(|_| RepositoryError::invalid_data())?;
    let fact_payload: Value =
        serde_json::from_str(&fact_payload_text).map_err(|_| RepositoryError::invalid_data())?;
    let intent: SchedulerIntent = serde_json::from_value(fact_payload.clone())
        .map_err(|_| RepositoryError::invalid_data())?;
    let stored_content_hash: String = row
        .try_get("content_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    if row
        .try_get::<String, _>("checkpoint_kind")
        .map_err(|_| RepositoryError::invalid_data())?
        != "planned_action"
        || transition_key.as_str() != created_by_transition_key
        || schema_version != SCHEDULER_CHECKPOINT_SCHEMA_VERSION
        || intent.run_id() != run_id
        || intent.checkpoint_id() != &checkpoint_id
        || canonical_intent_hash(&intent)?.as_str() != intent_hash
        || scheduler_checkpoint_content_hash(
            run_id.as_str(),
            checkpoint_id.as_str(),
            "planned_action",
            transition_key.as_str(),
            &intent_hash,
            &event_id_value,
            schema_version,
            scheduler_projection_version,
            &fact_payload,
        )?
        .as_str()
            != stored_content_hash
        || insight_engine::worker::TaskExecutionRequest::from_scheduler_intent(&intent)
            .map_err(|_| RepositoryError::invalid_data())?
            != *envelope.request()
    {
        return Err(RepositoryError::invalid_data());
    }
    match load_replay(transaction, run_id, &transition_key, &intent_hash).await? {
        Replay::Exact(replay)
            if replay.event_id() == event_id_value
                && replay.projection_version() == scheduler_projection_version => {}
        Replay::Exact(_) | Replay::Vacant => return Err(RepositoryError::invalid_data()),
    }
    if envelope.attempt_no() == AttemptNo::FIRST {
        if envelope.lease_epoch() != LeaseEpoch::FIRST
            || envelope.fencing_token() != fencing_token(&transition_key)
            || envelope.dispatch_scheduler_projection_version() != scheduler_projection_version
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(())
    } else {
        validate_retry_lineage_sqlite(transaction, run_id, envelope).await
    }
}

async fn load_authoritative_task_claim_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    supplied: &SchedulerTaskClaim,
    comparison: TaskClaimComparison,
) -> Result<Option<AuthoritativeTaskClaim>, RepositoryError> {
    let row = sqlx::query(
        "SELECT o.task_state,o.task_envelope,o.created_by_transition_key,o.claimed_by,
                o.claim_token,o.claim_expires_at,o.projection_version,o.claim_mode,
                o.activation_id,o.attempt_no,o.lease_epoch,o.fencing_token,o.effect_id,
                a.lifecycle AS attempt_lifecycle,a.effect_evidence AS attempt_effect_evidence,
                a.effect_id AS attempt_effect_id,a.worker_id AS attempt_worker_id,a.started_at,
                a.lease_expires_at AS attempt_lease_expires_at,
                a.projection_version AS attempt_version,
                CASE WHEN a.lease_expires_at IS NOT NULL
                           AND julianday(a.lease_expires_at)>julianday('now')
                     THEN 1 ELSE 0 END AS attempt_lease_is_fresh,
                v.execution_kind AS activation_execution_kind,
                v.lifecycle AS activation_lifecycle,
                v.effect_evidence AS activation_effect_evidence,
                v.effect_id AS activation_effect_id,v.current_attempt_no,v.current_lease_epoch,
                v.current_fencing_token,v.winning_attempt_no,v.effect_idempotency,
                v.retry_budget_remaining,v.last_attempt_no,v.last_lease_epoch,
                v.projection_version AS activation_version,r.lifecycle AS run_lifecycle
                ,CASE WHEN o.claim_expires_at IS NOT NULL
                           AND julianday(o.claim_expires_at)>julianday('now')
                      THEN 1 ELSE 0 END AS claim_is_fresh
                ,strftime('%Y-%m-%dT%H:%M:%fZ','now') AS database_now
         FROM task_outbox o
         JOIN node_attempts a ON a.run_id=o.run_id AND a.activation_id=o.activation_id
           AND a.attempt_no=o.attempt_no AND a.lease_epoch=o.lease_epoch
           AND a.fencing_token=o.fencing_token
         JOIN node_activations v ON v.run_id=o.run_id AND v.activation_id=o.activation_id
         JOIN workflow_runs r ON r.run_id=o.run_id
         WHERE o.run_id=? AND o.task_id=?
           AND o.task_state IN ('claimed','published','acked')",
    )
    .bind(supplied.run_id().as_str())
    .bind(supplied.task_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let envelope = serde_json::from_str::<DurableTaskExecutionRequest>(
        &row.try_get::<String, _>("task_envelope")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let activation_id = model_data(ActivationId::new(
        row.try_get::<String, _>("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let attempt_no = model_data(AttemptNo::new(
        u32::try_from(
            row.try_get::<i64, _>("attempt_no")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let lease_epoch = model_data(LeaseEpoch::new(u64_from_i64(
        row.try_get("lease_epoch")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?))?;
    let fencing_token: String = row
        .try_get("fencing_token")
        .map_err(|_| RepositoryError::invalid_data())?;
    let effect_id: String = row
        .try_get("effect_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let attempt_effect_id: String = row
        .try_get("attempt_effect_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let activation_effect_id: String = row
        .try_get("activation_effect_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    if envelope.request().run_id() != supplied.run_id()
        || envelope.request().task_id() != supplied.task_id()
        || envelope.request().activation_id() != &activation_id
        || envelope.attempt_no() != attempt_no
        || envelope.lease_epoch() != lease_epoch
        || envelope.fencing_token() != fencing_token
        || envelope.request().effect_id().as_str() != effect_id
        || attempt_effect_id != effect_id
        || activation_effect_id != effect_id
        || row
            .try_get::<String, _>("activation_execution_kind")
            .map_err(|_| RepositoryError::invalid_data())?
            != "worker"
    {
        return Err(RepositoryError::invalid_data());
    }
    let expected_retry_budget = envelope
        .request()
        .effect_policy()
        .max_attempts()
        .saturating_sub(envelope.attempt_no().get());
    if row
        .try_get::<String, _>("effect_idempotency")
        .map_err(|_| RepositoryError::invalid_data())?
        != effect_idempotency_str(envelope.request().effect_policy().effect_idempotency())
        || row
            .try_get::<i64, _>("retry_budget_remaining")
            .map_err(|_| RepositoryError::invalid_data())?
            != i64::from(expected_retry_budget)
        || row
            .try_get::<Option<i64>, _>("last_attempt_no")
            .map_err(|_| RepositoryError::invalid_data())?
            != Some(i64::from(attempt_no.get()))
        || row
            .try_get::<Option<i64>, _>("last_lease_epoch")
            .map_err(|_| RepositoryError::invalid_data())?
            != Some(i64_from_u64(lease_epoch.get())?)
    {
        return Err(RepositoryError::invalid_data());
    }
    let task_state: String = row
        .try_get("task_state")
        .map_err(|_| RepositoryError::invalid_data())?;
    let mode = SchedulerTaskClaimMode::parse(
        row.try_get::<Option<String>, _>("claim_mode")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            .ok_or_else(RepositoryError::invalid_data)?,
    )?;
    if (task_state == "claimed"
        && !matches!(
            mode,
            SchedulerTaskClaimMode::Execute | SchedulerTaskClaimMode::FinalizeLeaseLoss
        ))
        || (matches!(task_state.as_str(), "published" | "acked")
            && mode != SchedulerTaskClaimMode::Acknowledge)
        || !matches!(task_state.as_str(), "claimed" | "published" | "acked")
    {
        return Err(RepositoryError::invalid_data());
    }
    let current_effect_evidence = parse_effect_evidence(
        &row.try_get::<String, _>("attempt_effect_evidence")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let activation_effect_evidence = parse_effect_evidence(
        &row.try_get::<String, _>("activation_effect_evidence")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    if current_effect_evidence != activation_effect_evidence {
        return Err(RepositoryError::invalid_data());
    }
    let attempt_lifecycle: String = row
        .try_get("attempt_lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?;
    let activation_lifecycle: String = row
        .try_get("activation_lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?;
    let current_attempt_no = row
        .try_get::<Option<i64>, _>("current_attempt_no")
        .map_err(|_| RepositoryError::invalid_data())?
        .map(|value| u32::try_from(value).map_err(|_| RepositoryError::invalid_data()))
        .transpose()?;
    let current_lease_epoch = row
        .try_get::<Option<i64>, _>("current_lease_epoch")
        .map_err(|_| RepositoryError::invalid_data())?
        .map(u64_from_i64)
        .transpose()?;
    let current_fencing_token: Option<String> = row
        .try_get("current_fencing_token")
        .map_err(|_| RepositoryError::invalid_data())?;
    let winning_attempt_no = row
        .try_get::<Option<i64>, _>("winning_attempt_no")
        .map_err(|_| RepositoryError::invalid_data())?
        .map(|value| u32::try_from(value).map_err(|_| RepositoryError::invalid_data()))
        .transpose()?;
    if mode == SchedulerTaskClaimMode::Execute {
        let attempt_worker_id: String = row
            .try_get("attempt_worker_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let started_at = row
            .try_get::<Option<String>, _>("started_at")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(|value| parse_run_timestamp(&value))
            .transpose()?;
        let attempt_lease_expires_at = parse_run_timestamp(
            &row.try_get::<String, _>("attempt_lease_expires_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let task_claim_expires_at = parse_run_timestamp(
            &row.try_get::<String, _>("claim_expires_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let execution_projection_matches = match attempt_lifecycle.as_str() {
            "leased" => {
                current_effect_evidence == EffectEvidence::NotStarted
                    && attempt_worker_id == "scheduler-outbox"
                    && started_at.is_none()
            }
            "running" => {
                current_effect_evidence == EffectEvidence::Started
                    && attempt_worker_id
                        == row
                            .try_get::<String, _>("claimed_by")
                            .map_err(|_| RepositoryError::invalid_data())?
                    && started_at.is_some()
            }
            _ => false,
        };
        if !execution_projection_matches
            || attempt_lease_expires_at.timestamp_micros()
                != task_claim_expires_at.timestamp_micros()
        {
            return Err(RepositoryError::invalid_data());
        }
    }
    match task_state.as_str() {
        "claimed"
            if matches!(attempt_lifecycle.as_str(), "leased" | "running")
                && matches!(activation_lifecycle.as_str(), "leased" | "running")
                && current_attempt_no == Some(attempt_no.get())
                && current_lease_epoch == Some(lease_epoch.get())
                && current_fencing_token.as_deref() == Some(fencing_token.as_str()) => {}
        "published" | "acked"
            if attempt_lifecycle == "succeeded"
                && activation_lifecycle == "succeeded"
                && current_effect_evidence == EffectEvidence::Committed
                && current_attempt_no.is_none()
                && current_lease_epoch.is_none()
                && current_fencing_token.is_none()
                && winning_attempt_no == Some(attempt_no.get()) => {}
        _ => return Err(RepositoryError::invalid_data()),
    }
    let lease_loss_evidence = (mode == SchedulerTaskClaimMode::FinalizeLeaseLoss)
        .then(|| current_effect_evidence.after_lease_loss());
    let mut stored_claim = SchedulerTaskClaim::new(
        envelope,
        row.try_get("claimed_by")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("claim_token")
            .map_err(|_| RepositoryError::invalid_data())?,
        parse_run_timestamp(
            &row.try_get::<String, _>("claim_expires_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        u64_from_i64(
            row.try_get("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        mode,
    );
    if let Some(evidence) = lease_loss_evidence {
        stored_claim = stored_claim.with_lease_loss_evidence(evidence);
    }
    let authority = AuthoritativeTaskClaim {
        claim: stored_claim,
        task_state,
        run_lifecycle: row
            .try_get("run_lifecycle")
            .map_err(|_| RepositoryError::invalid_data())?,
        attempt_lifecycle,
        activation_lifecycle,
        current_effect_evidence,
        started_at: row
            .try_get::<Option<String>, _>("started_at")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(|value| parse_run_timestamp(&value))
            .transpose()?,
        claim_is_fresh: row
            .try_get::<i64, _>("claim_is_fresh")
            .map_err(|_| RepositoryError::invalid_data())?
            == 1,
        attempt_lease_is_fresh: row
            .try_get::<i64, _>("attempt_lease_is_fresh")
            .map_err(|_| RepositoryError::invalid_data())?
            == 1,
        database_now: parse_run_timestamp(
            &row.try_get::<String, _>("database_now")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
    };
    let matches = match comparison {
        TaskClaimComparison::Exact => same_claim_snapshot(&authority.claim, supplied),
        TaskClaimComparison::AcknowledgeTransition => {
            same_acknowledgement_snapshot(&authority, supplied)
        }
    };
    if !matches {
        return Ok(None);
    }
    validate_task_request_origin_sqlite(
        transaction,
        supplied.run_id(),
        &row.try_get::<String, _>("created_by_transition_key")
            .map_err(|_| RepositoryError::invalid_data())?,
        authority.claim.envelope(),
    )
    .await?;
    Ok(Some(authority))
}

async fn claim_tasks_sqlite(
    repository: &SqliteDurableRepository,
    claimed_by: &str,
    claim_seconds: u32,
    limit: u32,
    max_claimed_per_run: u32,
) -> Result<Vec<SchedulerTaskClaim>, RepositoryError> {
    validate_claim_parameters(claimed_by, claim_seconds, limit)?;
    if max_claimed_per_run == 0 || max_claimed_per_run > MAX_CLAIM_LIMIT {
        return Err(RepositoryError::invalid_configuration());
    }
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let candidates = sqlx::query(
        "SELECT o.run_id,o.task_id,o.task_state,o.task_envelope,o.projection_version,
                a.lifecycle AS attempt_lifecycle,a.effect_evidence AS attempt_effect_evidence
         FROM task_outbox o JOIN workflow_runs r ON r.run_id=o.run_id
         JOIN node_attempts a ON a.run_id=o.run_id AND a.activation_id=o.activation_id
           AND a.attempt_no=o.attempt_no AND a.lease_epoch=o.lease_epoch
           AND a.fencing_token=o.fencing_token
         WHERE ((((o.task_state='pending' AND julianday(o.available_at) <= julianday('now'))
              OR (o.task_state='claimed' AND julianday(o.claim_expires_at) <= julianday('now')))
                AND (r.lifecycle IN ('created','active','waiting')
                  OR (r.lifecycle='terminating'
                    AND json_extract(o.task_envelope,'$.request.admission_class')='termination_finalizer')))
            OR o.task_state='published')
           AND NOT EXISTS (
               SELECT 1 FROM model_tool_call_batches latest_waiting
               WHERE latest_waiting.run_id=o.run_id
                 AND latest_waiting.activation_id=o.activation_id
                 AND latest_waiting.attempt_no=o.attempt_no
                 AND latest_waiting.model_call_no=(
                     SELECT MAX(latest_usage.model_call_no) FROM model_call_usage latest_usage
                     WHERE latest_usage.run_id=o.run_id
                       AND latest_usage.activation_id=o.activation_id
                       AND latest_usage.attempt_no=o.attempt_no
                 )
                 AND latest_waiting.execution_status='active'
                 AND latest_waiting.continuation_status='waiting_tools'
           )
           AND (o.task_state='published' OR (
               SELECT COUNT(*) FROM task_outbox active
               WHERE active.run_id=o.run_id AND active.task_state='claimed'
                 AND julianday(active.claim_expires_at)>julianday('now')
                 AND NOT EXISTS (
                     SELECT 1 FROM model_tool_call_batches waiting_parent
                     WHERE waiting_parent.run_id=active.run_id
                       AND waiting_parent.parent_task_id=active.task_id
                       AND waiting_parent.execution_status='active'
                       AND waiting_parent.continuation_status='waiting_tools'
                 )
           ) + (
               SELECT COUNT(*) FROM model_tool_calls active_tool
               JOIN model_tool_call_batches active_batch
                 ON active_batch.run_id=active_tool.run_id
                AND active_batch.activation_id=active_tool.activation_id
                AND active_batch.attempt_no=active_tool.attempt_no
                AND active_batch.model_call_no=active_tool.model_call_no
               WHERE active_tool.run_id=o.run_id
                 AND active_batch.execution_status='active'
                 AND active_batch.continuation_status='waiting_tools'
                 AND active_tool.call_status IN ('claimed','running')
                 AND julianday(active_tool.claim_expires_at)>julianday('now')
           ) < ?)
         ORDER BY CASE o.task_state WHEN 'published' THEN 0 ELSE 1 END,
                  o.available_at,o.run_id,o.task_id LIMIT ?",
    )
    .bind(i64::from(max_claimed_per_run))
    .bind(i64::from(limit))
    .fetch_all(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let mut claims = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let run_id = model_data(RunId::new(
            candidate
                .try_get::<String, _>("run_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let task_id = scheduler_data(SchedulerTaskId::parse(
            candidate
                .try_get::<String, _>("task_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let prior_state: String = candidate
            .try_get("task_state")
            .map_err(|_| RepositoryError::invalid_data())?;
        let envelope = serde_json::from_str::<DurableTaskExecutionRequest>(
            &candidate
                .try_get::<String, _>("task_envelope")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        if envelope.schema_version() != SCHEDULER_TASK_ENVELOPE_SCHEMA_VERSION
            || envelope.request().run_id() != &run_id
            || envelope.request().task_id() != &task_id
        {
            return Err(RepositoryError::invalid_data());
        }
        let attempt_lifecycle: String = candidate
            .try_get("attempt_lifecycle")
            .map_err(|_| RepositoryError::invalid_data())?;
        let attempt_effect_evidence = parse_effect_evidence(
            &candidate
                .try_get::<String, _>("attempt_effect_evidence")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let latest = load_latest_parent_model_call_sqlite(
            &mut transaction,
            &run_id,
            envelope.request().activation_id(),
            envelope.attempt_no(),
        )
        .await?;
        if let Some(latest) = &latest {
            if latest_task_id(latest) != task_id.as_str()
                || latest_lease_epoch(latest) != envelope.lease_epoch().get()
                || latest_fencing_token(latest) != envelope.fencing_token()
            {
                return Err(RepositoryError::invalid_data());
            }
        }
        let claim_class = classify_parent_task_claim(
            &prior_state,
            &attempt_lifecycle,
            attempt_effect_evidence,
            latest.as_ref(),
        );
        let mode = match claim_class {
            "initial_execute" | "activate_checkpointed" | "continue_ready" => {
                SchedulerTaskClaimMode::Execute
            }
            "finalize_lease_loss" => SchedulerTaskClaimMode::FinalizeLeaseLoss,
            "acknowledge" => SchedulerTaskClaimMode::Acknowledge,
            "ineligible" => continue,
            _ => return Err(RepositoryError::invalid_data()),
        };
        let claim_token = format!("claim_{}", Uuid::new_v4().simple());
        let claimed_row = sqlx::query(
            "UPDATE task_outbox SET
                task_state=CASE WHEN task_state='published' THEN 'published' ELSE 'claimed' END,
                claimed_by=?,claim_token=?,
                claim_expires_at=strftime('%Y-%m-%dT%H:%M:%fZ','now',printf('+%d seconds',?)),
                claim_mode=?,publish_attempts=publish_attempts+1,
                projection_version=projection_version+1
             WHERE run_id=? AND task_id=?
               AND projection_version=? AND task_state=?
               AND ((task_state='pending' AND julianday(available_at) <= julianday('now'))
                 OR (task_state='claimed' AND julianday(claim_expires_at) <= julianday('now'))
                 OR task_state='published')
               AND NOT EXISTS (
                   SELECT 1 FROM model_tool_call_batches latest_waiting
                   WHERE latest_waiting.run_id=task_outbox.run_id
                     AND latest_waiting.activation_id=task_outbox.activation_id
                     AND latest_waiting.attempt_no=task_outbox.attempt_no
                     AND latest_waiting.model_call_no=(
                         SELECT MAX(latest_usage.model_call_no) FROM model_call_usage latest_usage
                         WHERE latest_usage.run_id=task_outbox.run_id
                           AND latest_usage.activation_id=task_outbox.activation_id
                           AND latest_usage.attempt_no=task_outbox.attempt_no
                     )
                     AND latest_waiting.execution_status='active'
                     AND latest_waiting.continuation_status='waiting_tools'
               )
               AND CASE ?
                 WHEN 'initial_execute' THEN task_state='pending'
                   AND NOT EXISTS (
                       SELECT 1 FROM model_call_usage initial_usage
                       WHERE initial_usage.run_id=task_outbox.run_id
                         AND initial_usage.activation_id=task_outbox.activation_id
                         AND initial_usage.attempt_no=task_outbox.attempt_no
                   )
                   AND EXISTS (
                       SELECT 1 FROM node_attempts initial_attempt
                       WHERE initial_attempt.run_id=task_outbox.run_id
                         AND initial_attempt.activation_id=task_outbox.activation_id
                         AND initial_attempt.attempt_no=task_outbox.attempt_no
                         AND initial_attempt.lease_epoch=task_outbox.lease_epoch
                         AND initial_attempt.fencing_token=task_outbox.fencing_token
                         AND initial_attempt.lifecycle='leased'
                         AND initial_attempt.effect_evidence='not_started'
                         AND initial_attempt.started_at IS NULL
                   )
                 WHEN 'activate_checkpointed' THEN task_state='claimed'
                   AND EXISTS (
                       SELECT 1 FROM model_call_usage resume_usage
                       JOIN model_tool_call_batches resume_batch
                         ON resume_batch.run_id=resume_usage.run_id
                        AND resume_batch.activation_id=resume_usage.activation_id
                        AND resume_batch.attempt_no=resume_usage.attempt_no
                        AND resume_batch.model_call_no=resume_usage.model_call_no
                       WHERE resume_usage.run_id=task_outbox.run_id
                         AND resume_usage.activation_id=task_outbox.activation_id
                         AND resume_usage.attempt_no=task_outbox.attempt_no
                         AND resume_usage.model_call_no=(
                             SELECT MAX(latest_usage.model_call_no)
                             FROM model_call_usage latest_usage
                             WHERE latest_usage.run_id=task_outbox.run_id
                               AND latest_usage.activation_id=task_outbox.activation_id
                               AND latest_usage.attempt_no=task_outbox.attempt_no
                         )
                         AND resume_usage.call_status='completed'
                         AND resume_usage.finish_reason='tool_calls'
                         AND resume_batch.execution_status='checkpointed'
                         AND resume_batch.continuation_status='checkpointed'
                   )
                 WHEN 'continue_ready' THEN task_state='pending'
                   AND EXISTS (
                       SELECT 1 FROM model_call_usage resume_usage
                       JOIN model_tool_call_batches resume_batch
                         ON resume_batch.run_id=resume_usage.run_id
                        AND resume_batch.activation_id=resume_usage.activation_id
                        AND resume_batch.attempt_no=resume_usage.attempt_no
                        AND resume_batch.model_call_no=resume_usage.model_call_no
                       WHERE resume_usage.run_id=task_outbox.run_id
                         AND resume_usage.activation_id=task_outbox.activation_id
                         AND resume_usage.attempt_no=task_outbox.attempt_no
                         AND resume_usage.model_call_no=(
                             SELECT MAX(latest_usage.model_call_no)
                             FROM model_call_usage latest_usage
                             WHERE latest_usage.run_id=task_outbox.run_id
                               AND latest_usage.activation_id=task_outbox.activation_id
                               AND latest_usage.attempt_no=task_outbox.attempt_no
                         )
                         AND resume_usage.call_status='completed'
                         AND resume_usage.finish_reason='tool_calls'
                         AND ((resume_batch.execution_status='succeeded'
                                AND resume_batch.continuation_status='ready_continue')
                           OR (resume_batch.execution_status='failed'
                                AND resume_batch.continuation_status='ready_failed')
                           OR (resume_batch.execution_status='cancelled'
                                AND resume_batch.continuation_status='ready_cancelled'))
                   )
                 WHEN 'finalize_lease_loss' THEN task_state='claimed'
                 WHEN 'acknowledge' THEN task_state='published'
                 ELSE 0
               END
               AND (task_state='published' OR EXISTS (
                   SELECT 1 FROM workflow_runs r WHERE r.run_id=task_outbox.run_id
                     AND (r.lifecycle IN ('created','active','waiting')
                       OR (r.lifecycle='terminating'
                         AND ?='termination_finalizer'
                         AND json_extract(task_outbox.task_envelope,'$.request.admission_class')=?))
               ))
             RETURNING projection_version,claim_expires_at",
        )
        .bind(claimed_by)
        .bind(&claim_token)
        .bind(i64::from(claim_seconds))
        .bind(mode.as_str())
        .bind(run_id.as_str())
        .bind(task_id.as_str())
        .bind(
            candidate
                .try_get::<i64, _>("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .bind(&prior_state)
        .bind(claim_class)
        .bind(envelope.request().admission_class().as_str())
        .bind(envelope.request().admission_class().as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(claimed_row) = claimed_row else {
            continue;
        };
        let next_task_version = claimed_row
            .try_get::<i64, _>("projection_version")
            .map_err(|_| RepositoryError::invalid_data())?;
        let claim_expires_at = parse_run_timestamp(
            &claimed_row
                .try_get::<String, _>("claim_expires_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        if mode == SchedulerTaskClaimMode::Execute {
            let resume_running = matches!(claim_class, "activate_checkpointed" | "continue_ready");
            let attempt_rows = if resume_running {
                sqlx::query(
                    "UPDATE node_attempts SET lease_expires_at=?,heartbeat_at=CURRENT_TIMESTAMP,
                        worker_id=?,projection_version=projection_version+1
                     WHERE run_id=? AND activation_id=? AND attempt_no=? AND lease_epoch=?
                       AND fencing_token=? AND lifecycle='running' AND effect_evidence='started'
                       AND started_at IS NOT NULL",
                )
                .bind(now_text(claim_expires_at))
                .bind(claimed_by)
                .bind(run_id.as_str())
                .bind(envelope.request().activation_id().as_str())
                .bind(i64::from(envelope.attempt_no().get()))
                .bind(i64_from_u64(envelope.lease_epoch().get())?)
                .bind(envelope.fencing_token())
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
                .rows_affected()
            } else {
                sqlx::query(
                    "UPDATE node_attempts SET lease_expires_at=?,heartbeat_at=CURRENT_TIMESTAMP,
                        projection_version=projection_version+1
                     WHERE run_id=? AND activation_id=? AND attempt_no=? AND lease_epoch=?
                       AND fencing_token=? AND lifecycle='leased' AND effect_evidence='not_started'
                       AND started_at IS NULL",
                )
                .bind(now_text(claim_expires_at))
                .bind(run_id.as_str())
                .bind(envelope.request().activation_id().as_str())
                .bind(i64::from(envelope.attempt_no().get()))
                .bind(i64_from_u64(envelope.lease_epoch().get())?)
                .bind(envelope.fencing_token())
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
                .rows_affected()
            };
            if attempt_rows != 1 {
                return Err(RepositoryError::invalid_data());
            }
        }
        let transition_key = TransitionKey::derive(
            "scheduler.task.claim.v1",
            &[run_id.as_str(), task_id.as_str(), &claim_token],
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let intent_hash = canonical_intent_hash(&json!({
            "operation": "scheduler.task.claim",
            "run_id": run_id,
            "task_id": task_id,
            "claimed_by": claimed_by,
            "claim_token": claim_token,
            "claim_expires_at": claim_expires_at,
            "admission_class": envelope.request().admission_class(),
        }))?;
        let event_id_value = append_projection_mutation_event(
            &mut transaction,
            &run_id,
            &transition_key,
            intent_hash.as_str(),
            ProjectionMutationKind::TaskClaimed,
            u64_from_i64(next_task_version)?,
        )
        .await?;
        finalize_projection_checkpoints(&mut transaction, &run_id, &event_id_value).await?;
        let mut claim = SchedulerTaskClaim::new(
            envelope,
            claimed_by.to_owned(),
            claim_token,
            claim_expires_at,
            u64_from_i64(next_task_version)?,
            mode,
        );
        if mode == SchedulerTaskClaimMode::FinalizeLeaseLoss {
            let evidence = parse_effect_evidence(
                &candidate
                    .try_get::<String, _>("attempt_effect_evidence")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?
            .after_lease_loss();
            claim = claim.with_lease_loss_evidence(evidence);
        }
        let authority = load_authoritative_task_claim_sqlite(
            &mut transaction,
            &claim,
            TaskClaimComparison::Exact,
        )
        .await?
        .ok_or_else(RepositoryError::invalid_data)?;
        claims.push(authority.claim);
    }
    for claim in &claims {
        let Some(authority) = load_authoritative_task_claim_sqlite(
            &mut transaction,
            claim,
            TaskClaimComparison::Exact,
        )
        .await?
        else {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(Vec::new());
        };
        if !authority.is_fresh() {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(Vec::new());
        }
        if load_latest_parent_model_call_sqlite(
            &mut transaction,
            claim.run_id(),
            claim.activation_id(),
            claim.envelope().attempt_no(),
        )
        .await?
        .as_ref()
        .is_some_and(latest_is_waiting_tools)
        {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(Vec::new());
        }
    }
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(claims)
}

async fn start_task_sqlite(
    repository: &SqliteDurableRepository,
    claim: &SchedulerTaskClaim,
) -> Result<TransitionOutcome<SchedulerCommitReceipt>, RepositoryError> {
    if claim.mode() != SchedulerTaskClaimMode::Execute {
        return Ok(TransitionOutcome::StateConflict);
    }
    let transition_key = TransitionKey::derive(
        "scheduler.task.started.v1",
        &[
            claim.run_id().as_str(),
            claim.task_id().as_str(),
            claim.claim_token(),
        ],
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let intent_hash = canonical_intent_hash(&json!({
        "operation": "scheduler.task.started",
        "run_id": claim.run_id(),
        "task_id": claim.task_id(),
        "attempt_no": claim.envelope().attempt_no(),
        "lease_epoch": claim.envelope().lease_epoch(),
        "fencing_token": claim.envelope().fencing_token(),
        "claimed_by": claim.claimed_by(),
        "claim_token": claim.claim_token(),
        "admission_class": claim.envelope().request().admission_class(),
        "task_envelope": claim.envelope(),
    }))?;
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    match load_replay(
        &mut transaction,
        claim.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(replay) => {
            validate_task_transition_event_sqlite(
                &mut transaction,
                claim,
                &transition_key,
                intent_hash.as_str(),
                &replay,
                &ExecutionEventPayload::AttemptRunning {
                    lease_epoch: claim.envelope().lease_epoch(),
                },
                true,
            )
            .await?;
            let receipt = exact_task_started_receipt_sqlite(
                &mut transaction,
                claim,
                &transition_key,
                intent_hash.as_str(),
                &replay,
            )
            .await?;
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: receipt,
            });
        }
        Replay::Vacant => {}
    }
    let Some(authority) =
        load_authoritative_task_claim_sqlite(&mut transaction, claim, TaskClaimComparison::Exact)
            .await?
    else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    if authority.task_state != "claimed"
        || authority.claim.mode() != SchedulerTaskClaimMode::Execute
        || !authority.is_fresh()
        || !authority.permits_execution()
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    if authority.attempt_lifecycle != "leased"
        || authority.activation_lifecycle != "leased"
        || authority.current_effect_evidence != EffectEvidence::NotStarted
    {
        return Err(RepositoryError::invalid_data());
    }
    let attempt_next = sqlx::query_scalar::<_, i64>(
        "UPDATE node_attempts SET lifecycle='running',effect_evidence='started',worker_id=?,
            lease_expires_at=?,heartbeat_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),
            started_at=COALESCE(started_at,strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            projection_version=projection_version+1
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND lease_epoch=?
           AND fencing_token=? AND lifecycle='leased' AND effect_evidence='not_started'
         RETURNING projection_version",
    )
    .bind(claim.claimed_by())
    .bind(now_text(claim.claim_expires_at()))
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64_from_u64(claim.envelope().lease_epoch().get())?)
    .bind(claim.envelope().fencing_token())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(_attempt_next) = attempt_next else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    let activation_rows = sqlx::query(
        "UPDATE node_activations SET lifecycle='running',effect_evidence='started',
            projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND execution_kind='worker'
           AND lifecycle='leased' AND effect_evidence='not_started' AND current_attempt_no=?
           AND current_lease_epoch=? AND current_fencing_token=?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64_from_u64(claim.envelope().lease_epoch().get())?)
    .bind(claim.envelope().fencing_token())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if activation_rows != 1 {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let (scope, node) =
        activation_identity(&mut transaction, claim.run_id(), claim.activation_id()).await?;
    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(claim.run_id().clone()).for_attempt(
            scope,
            node.clone(),
            claim.activation_id().clone(),
            claim.envelope().attempt_no(),
        ),
        ExecutionEventPayload::AttemptRunning {
            lease_epoch: claim.envelope().lease_epoch(),
        },
    ))?;
    let seq = allocate_event_seq(&mut transaction, claim.run_id()).await?;
    let event_id_value = event_id(&transition_key);
    let scheduler_version = current_scheduler_version(&mut transaction, claim.run_id()).await?;
    let occurred_at = insert_event(
        &mut transaction,
        claim.run_id(),
        seq,
        &event_id_value,
        &transition_key,
        intent_hash.as_str(),
        scheduler_version,
        &event,
    )
    .await?;
    let checkpoint_id = insert_task_started_checkpoint_sqlite(
        &mut transaction,
        claim,
        &transition_key,
        intent_hash.as_str(),
        &event_id_value,
        scheduler_version,
    )
    .await?;
    insert_public_operation(
        &mut transaction,
        claim.run_id(),
        &transition_key,
        &event_id_value,
        seq,
        occurred_at,
        PublicEventPayload::OperationStarted {
            node_id: node,
            activation_id: claim.activation_id().clone(),
            attempt_no: claim.envelope().attempt_no(),
        },
    )
    .await?;
    finalize_projection_checkpoints(&mut transaction, claim.run_id(), &event_id_value).await?;
    let still_authorized = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM task_outbox o JOIN node_attempts a
           ON a.run_id=o.run_id AND a.activation_id=o.activation_id
          AND a.attempt_no=o.attempt_no AND a.lease_epoch=o.lease_epoch
          AND a.fencing_token=o.fencing_token
         WHERE o.run_id=? AND o.task_id=? AND o.task_state='claimed'
           AND o.claimed_by=? AND o.claim_token=? AND o.claim_mode='execute'
           AND o.projection_version=?
           AND julianday(o.claim_expires_at)>julianday('now')
           AND julianday(a.lease_expires_at)>julianday('now')",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.task_id().as_str())
    .bind(claim.claimed_by())
    .bind(claim.claim_token())
    .bind(i64_from_u64(claim.task_projection_version())?)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if still_authorized.is_none() {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed {
        result: SchedulerCommitReceipt::new(seq, event_id_value, checkpoint_id, scheduler_version),
    })
}

async fn heartbeat_task_sqlite(
    repository: &SqliteDurableRepository,
    claim: &SchedulerTaskClaim,
    claim_seconds: u32,
) -> Result<SchedulerTaskHeartbeatOutcome, RepositoryError> {
    if claim.mode() != SchedulerTaskClaimMode::Execute
        || !(3..=MAX_CLAIM_SECONDS).contains(&claim_seconds)
    {
        return Err(RepositoryError::invalid_configuration());
    }
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let Some(authority) =
        load_authoritative_task_claim_sqlite(&mut transaction, claim, TaskClaimComparison::Exact)
            .await?
    else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskHeartbeatOutcome::LeaseLost);
    };
    if authority.task_state != "claimed"
        || authority.claim.mode() != SchedulerTaskClaimMode::Execute
        || !authority.is_fresh()
        || !authority.permits_execution()
        || authority.attempt_lifecycle != "running"
        || authority.activation_lifecycle != "running"
        || authority.current_effect_evidence != EffectEvidence::Started
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskHeartbeatOutcome::LeaseLost);
    }
    let operation_deadline = authority
        .started_at
        .and_then(|started_at| {
            i64::try_from(
                authority
                    .claim
                    .envelope()
                    .request()
                    .effect_policy()
                    .timeout_ms(),
            )
            .ok()
            .and_then(|milliseconds| {
                started_at.checked_add_signed(chrono::Duration::milliseconds(milliseconds))
            })
        })
        .ok_or_else(RepositoryError::invalid_data)?;
    let renewed_row = sqlx::query(
        "UPDATE task_outbox SET
             claim_expires_at=strftime('%Y-%m-%dT%H:%M:%fZ','now',printf('+%d seconds',?)),
             projection_version=projection_version+1
         WHERE run_id=? AND task_id=? AND task_state='claimed'
           AND claim_mode='execute'
           AND claimed_by=? AND claim_token=? AND projection_version=?
           AND julianday(claim_expires_at)>julianday('now')
           AND EXISTS (SELECT 1 FROM workflow_runs r WHERE r.run_id=task_outbox.run_id
                         AND (r.lifecycle IN ('created','active','waiting')
                           OR (r.lifecycle='terminating'
                             AND ?='termination_finalizer'
                             AND json_extract(task_outbox.task_envelope,'$.request.admission_class')=?)))
         RETURNING projection_version,claim_expires_at",
    )
    .bind(i64::from(claim_seconds))
    .bind(claim.run_id().as_str())
    .bind(claim.task_id().as_str())
    .bind(claim.claimed_by())
    .bind(claim.claim_token())
    .bind(i64_from_u64(claim.task_projection_version())?)
    .bind(claim.envelope().request().admission_class().as_str())
    .bind(claim.envelope().request().admission_class().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(renewed_row) = renewed_row else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskHeartbeatOutcome::LeaseLost);
    };
    let task_version = renewed_row
        .try_get::<i64, _>("projection_version")
        .map_err(|_| RepositoryError::invalid_data())?;
    let expires = parse_run_timestamp(
        &renewed_row
            .try_get::<String, _>("claim_expires_at")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let attempt_rows = sqlx::query(
        "UPDATE node_attempts SET lease_expires_at=?,heartbeat_at=CURRENT_TIMESTAMP,
             projection_version=projection_version+1
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND lease_epoch=?
           AND fencing_token=? AND lifecycle='running'
           AND effect_evidence='started'
           AND julianday(lease_expires_at)>julianday('now')",
    )
    .bind(now_text(expires))
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64_from_u64(claim.envelope().lease_epoch().get())?)
    .bind(claim.envelope().fencing_token())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if attempt_rows != 1 {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskHeartbeatOutcome::LeaseLost);
    }
    let timing = sqlx::query(
        "WITH clock(db_now) AS (SELECT julianday('now'))
         SELECT db_now >= julianday(?) AS deadline_elapsed,
                db_now < julianday(?) AS lease_fresh
         FROM clock",
    )
    .bind(now_text(operation_deadline))
    .bind(now_text(expires))
    .fetch_one(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let deadline_elapsed = timing
        .try_get::<i64, _>("deadline_elapsed")
        .map_err(|_| RepositoryError::invalid_data())?
        == 1;
    if timing
        .try_get::<i64, _>("lease_fresh")
        .map_err(|_| RepositoryError::invalid_data())?
        != 1
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskHeartbeatOutcome::LeaseLost);
    }
    let renewed = SchedulerTaskClaim::new(
        authority.claim.envelope().clone(),
        authority.claim.claimed_by().to_owned(),
        authority.claim.claim_token().to_owned(),
        expires,
        u64_from_i64(task_version)?,
        authority.claim.mode(),
    );
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(if deadline_elapsed {
        SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(renewed)
    } else {
        SchedulerTaskHeartbeatOutcome::Renewed(renewed)
    })
}

fn task_failure_fact(
    failure: &super::SchedulerTaskFailure,
) -> Result<TaskFailureFact, RepositoryError> {
    scheduler_data(TaskFailureFact::new(
        failure.class(),
        failure.code(),
        failure.safe_error().cloned(),
    ))
}

fn internal_failure(
    failure: &super::SchedulerTaskFailure,
) -> Result<insight_engine::InternalFailureSummary, RepositoryError> {
    internal_failure_from_fact(&task_failure_fact(failure)?)
}

fn internal_failure_from_fact(
    failure: &TaskFailureFact,
) -> Result<insight_engine::InternalFailureSummary, RepositoryError> {
    let kind = match failure.class() {
        insight_engine::WorkerFailureClass::SafeBusinessFailure => {
            insight_engine::InternalFailureKind::Business
        }
        insight_engine::WorkerFailureClass::InfrastructureFailure => {
            insight_engine::InternalFailureKind::Infrastructure
        }
        insight_engine::WorkerFailureClass::EffectOutcomeUnknown => {
            insight_engine::InternalFailureKind::EffectOutcomeUnknown
        }
        insight_engine::WorkerFailureClass::ControlTermination => {
            insight_engine::InternalFailureKind::Cancelled
        }
        insight_engine::WorkerFailureClass::InvariantCorruption => {
            insight_engine::InternalFailureKind::Invariant
        }
    };
    Ok(insight_engine::InternalFailureSummary::new(
        kind,
        model_data(insight_engine::InternalFailureCode::new(
            failure.code().to_owned(),
        ))?,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn insert_task_completion_checkpoint(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &SchedulerTaskClaim,
    transition_key: &TransitionKey,
    intent_hash: &str,
    event_id_value: &str,
    scheduler_projection_version: u64,
    outcome: TaskOutcomeFact,
    output_receipts: BTreeMap<DataPortId, TaskOutputReceipt>,
) -> Result<SchedulerCheckpointId, RepositoryError> {
    let checkpoint_id = scheduler_checkpoint_for_task(claim.task_id());
    let occurrence =
        activation_occurrence(transaction, claim.run_id(), claim.activation_id()).await?;
    let fact_payload = serde_json::to_value(TaskCompletionFact {
        task_id: claim.task_id().clone(),
        occurrence,
        outcome,
        output_receipts,
    })
    .map_err(|_| RepositoryError::canonicalization())?;
    let content_hash = scheduler_checkpoint_content_hash(
        claim.run_id().as_str(),
        checkpoint_id.as_str(),
        "task_completed",
        transition_key.as_str(),
        intent_hash,
        event_id_value,
        SCHEDULER_CHECKPOINT_SCHEMA_VERSION,
        scheduler_projection_version,
        &fact_payload,
    )?;
    let payload = canonical_json(&fact_payload)?;
    sqlx::query(
        "INSERT INTO scheduler_checkpoints (
            run_id,checkpoint_id,content_hash,checkpoint_kind,transition_key,intent_hash,event_id,
            checkpoint_schema_version,scheduler_projection_version,fact_payload,
            projection_version,created_at
         ) VALUES (?,?,?,'task_completed',?,?,?,?,?,?,0,CURRENT_TIMESTAMP)",
    )
    .bind(claim.run_id().as_str())
    .bind(checkpoint_id.as_str())
    .bind(content_hash.as_str())
    .bind(transition_key.as_str())
    .bind(intent_hash)
    .bind(event_id_value)
    .bind(i64::from(SCHEDULER_CHECKPOINT_SCHEMA_VERSION))
    .bind(i64_from_u64(scheduler_projection_version)?)
    .bind(payload)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(checkpoint_id)
}

#[allow(clippy::too_many_arguments)]
async fn insert_task_retry_checkpoint_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &SchedulerTaskClaim,
    transition_key: &TransitionKey,
    intent_hash: &str,
    event_id_value: &str,
    scheduler_projection_version: u64,
    failure: &super::SchedulerTaskFailure,
    retry_at: DateTime<Utc>,
    remaining_attempts: u32,
    next_attempt_no: AttemptNo,
    next_lease_epoch: LeaseEpoch,
    next_fencing_token: &str,
    next_envelope: &DurableTaskExecutionRequest,
) -> Result<SchedulerCheckpointId, RepositoryError> {
    let checkpoint_id = operation_checkpoint(transition_key);
    let fact_payload = serde_json::to_value(TaskRetryFact {
        task_id: claim.task_id().clone(),
        activation_id: claim.activation_id().clone(),
        attempt_no: claim.envelope().attempt_no(),
        lease_epoch: claim.envelope().lease_epoch(),
        fencing_token: claim.envelope().fencing_token().to_owned(),
        failure: task_failure_fact(failure)?,
        effect_evidence: failure.effect_evidence(),
        retry_at,
        remaining_attempts,
        next_attempt_no,
        next_lease_epoch,
        next_fencing_token: next_fencing_token.to_owned(),
        next_envelope: next_envelope.clone(),
    })
    .map_err(|_| RepositoryError::canonicalization())?;
    let content_hash = scheduler_checkpoint_content_hash(
        claim.run_id().as_str(),
        checkpoint_id.as_str(),
        "task_retry_scheduled",
        transition_key.as_str(),
        intent_hash,
        event_id_value,
        SCHEDULER_CHECKPOINT_SCHEMA_VERSION,
        scheduler_projection_version,
        &fact_payload,
    )?;
    sqlx::query(
        "INSERT INTO scheduler_checkpoints (
            run_id,checkpoint_id,content_hash,checkpoint_kind,transition_key,intent_hash,event_id,
            checkpoint_schema_version,scheduler_projection_version,fact_payload,
            projection_version,created_at
         ) VALUES (?,?,?,'task_retry_scheduled',?,?,?,?,?,?,0,CURRENT_TIMESTAMP)",
    )
    .bind(claim.run_id().as_str())
    .bind(checkpoint_id.as_str())
    .bind(content_hash.as_str())
    .bind(transition_key.as_str())
    .bind(intent_hash)
    .bind(event_id_value)
    .bind(i64::from(SCHEDULER_CHECKPOINT_SCHEMA_VERSION))
    .bind(i64_from_u64(scheduler_projection_version)?)
    .bind(canonical_json(&fact_payload)?)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(checkpoint_id)
}

async fn exact_task_outcome_receipt(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &SchedulerTaskClaim,
    transition_key: &TransitionKey,
    intent_hash: &str,
    outcome: &SchedulerTaskOutcome,
    replay: &super::CommitReceipt,
) -> Result<SchedulerTaskCompletionReceipt, RepositoryError> {
    let created_by_transition_key = sqlx::query_scalar::<_, String>(
        "SELECT created_by_transition_key FROM task_outbox WHERE run_id=? AND task_id=?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.task_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    validate_task_request_origin_sqlite(
        transaction,
        claim.run_id(),
        &created_by_transition_key,
        claim.envelope(),
    )
    .await?;
    let expected_event = match outcome {
        SchedulerTaskOutcome::Succeeded(success) => {
            let encoded = Value::Object(
                success
                    .result()
                    .outputs()
                    .iter()
                    .map(|(port, value)| (port.as_str().to_owned(), value.value().clone()))
                    .collect(),
            );
            ExecutionEventPayload::AttemptSucceeded {
                output: Some(output_summary(&encoded)?),
            }
        }
        SchedulerTaskOutcome::Failed(failure) => match failure.disposition() {
            SchedulerFailureDisposition::TimedOut => ExecutionEventPayload::AttemptTimedOut,
            SchedulerFailureDisposition::Retry { .. } | SchedulerFailureDisposition::Terminal => {
                ExecutionEventPayload::AttemptFailed {
                    failure: Some(internal_failure(failure)?),
                }
            }
        },
    };
    validate_task_transition_event_sqlite(
        transaction,
        claim,
        transition_key,
        intent_hash,
        replay,
        &expected_event,
        true,
    )
    .await?;
    let expected_outcome = match outcome {
        SchedulerTaskOutcome::Succeeded(success) => Some(TaskOutcomeFact::Succeeded {
            outputs: success.result().outputs().clone(),
        }),
        SchedulerTaskOutcome::Failed(failure)
            if !matches!(
                failure.disposition(),
                SchedulerFailureDisposition::Retry { .. }
            ) =>
        {
            Some(TaskOutcomeFact::Failed {
                failure: task_failure_fact(failure)?,
            })
        }
        SchedulerTaskOutcome::Failed(_) => None,
    };
    let expected_retry = match outcome {
        SchedulerTaskOutcome::Failed(failure) => match failure.disposition() {
            SchedulerFailureDisposition::Retry {
                retry_at,
                remaining_attempts,
            } => {
                let next_attempt_no = model_data(claim.envelope().attempt_no().next())?;
                let next_lease_epoch = model_data(claim.envelope().lease_epoch().next())?;
                let next_fencing_token = fencing_token(transition_key);
                let next_envelope = DurableTaskExecutionRequest::new(
                    claim.envelope().request().clone(),
                    next_attempt_no,
                    next_lease_epoch,
                    next_fencing_token.clone(),
                    replay.projection_version(),
                )?;
                Some(TaskRetryFact {
                    task_id: claim.task_id().clone(),
                    activation_id: claim.activation_id().clone(),
                    attempt_no: claim.envelope().attempt_no(),
                    lease_epoch: claim.envelope().lease_epoch(),
                    fencing_token: claim.envelope().fencing_token().to_owned(),
                    failure: task_failure_fact(failure)?,
                    effect_evidence: failure.effect_evidence(),
                    retry_at: *retry_at,
                    remaining_attempts: *remaining_attempts,
                    next_attempt_no,
                    next_lease_epoch,
                    next_fencing_token,
                    next_envelope,
                })
            }
            SchedulerFailureDisposition::Terminal | SchedulerFailureDisposition::TimedOut => None,
        },
        SchedulerTaskOutcome::Succeeded(_) => None,
    };
    let checkpoint = sqlx::query(
        "SELECT checkpoint_id,content_hash,checkpoint_kind,transition_key,intent_hash,event_id,
                checkpoint_schema_version,scheduler_projection_version,fact_payload
         FROM scheduler_checkpoints
         WHERE run_id=? AND transition_key=?",
    )
    .bind(claim.run_id().as_str())
    .bind(transition_key.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let (checkpoint_id, completion) = match (
        checkpoint,
        expected_outcome.as_ref(),
        expected_retry.as_ref(),
    ) {
        (Some(row), Some(expected_outcome), None) => {
            let checkpoint_id = scheduler_data(SchedulerCheckpointId::parse(
                row.try_get::<String, _>("checkpoint_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let schema_version = u32::try_from(
                row.try_get::<i64, _>("checkpoint_schema_version")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?;
            let scheduler_version = u64_from_i64(
                row.try_get("scheduler_projection_version")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let stored_transition: String = row
                .try_get("transition_key")
                .map_err(|_| RepositoryError::invalid_data())?;
            let stored_intent: String = row
                .try_get("intent_hash")
                .map_err(|_| RepositoryError::invalid_data())?;
            let stored_event: String = row
                .try_get("event_id")
                .map_err(|_| RepositoryError::invalid_data())?;
            let payload_text: String = row
                .try_get("fact_payload")
                .map_err(|_| RepositoryError::invalid_data())?;
            let payload: Value =
                serde_json::from_str(&payload_text).map_err(|_| RepositoryError::invalid_data())?;
            let completion: TaskCompletionFact = serde_json::from_value(payload.clone())
                .map_err(|_| RepositoryError::invalid_data())?;
            let expected_occurrence =
                activation_occurrence(transaction, claim.run_id(), claim.activation_id()).await?;
            let stored_hash: String = row
                .try_get("content_hash")
                .map_err(|_| RepositoryError::invalid_data())?;
            if row
                .try_get::<String, _>("checkpoint_kind")
                .map_err(|_| RepositoryError::invalid_data())?
                != "task_completed"
                || checkpoint_id != scheduler_checkpoint_for_task(claim.task_id())
                || schema_version != SCHEDULER_CHECKPOINT_SCHEMA_VERSION
                || scheduler_version != replay.projection_version()
                || stored_transition != transition_key.as_str()
                || stored_intent != intent_hash
                || stored_event != replay.event_id()
                || completion.task_id != *claim.task_id()
                || completion.occurrence != expected_occurrence
                || completion.outcome != *expected_outcome
                || scheduler_checkpoint_content_hash(
                    claim.run_id().as_str(),
                    checkpoint_id.as_str(),
                    "task_completed",
                    &stored_transition,
                    &stored_intent,
                    &stored_event,
                    schema_version,
                    scheduler_version,
                    &payload,
                )?
                .as_str()
                    != stored_hash
            {
                return Err(RepositoryError::invalid_data());
            }
            (checkpoint_id, Some(completion))
        }
        (Some(row), None, Some(expected_retry)) => {
            let checkpoint_id = scheduler_data(SchedulerCheckpointId::parse(
                row.try_get::<String, _>("checkpoint_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let schema_version = u32::try_from(
                row.try_get::<i64, _>("checkpoint_schema_version")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?;
            let scheduler_version = u64_from_i64(
                row.try_get("scheduler_projection_version")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let stored_transition: String = row
                .try_get("transition_key")
                .map_err(|_| RepositoryError::invalid_data())?;
            let stored_intent: String = row
                .try_get("intent_hash")
                .map_err(|_| RepositoryError::invalid_data())?;
            let stored_event: String = row
                .try_get("event_id")
                .map_err(|_| RepositoryError::invalid_data())?;
            let payload_text: String = row
                .try_get("fact_payload")
                .map_err(|_| RepositoryError::invalid_data())?;
            let payload: Value =
                serde_json::from_str(&payload_text).map_err(|_| RepositoryError::invalid_data())?;
            let retry: TaskRetryFact = serde_json::from_value(payload.clone())
                .map_err(|_| RepositoryError::invalid_data())?;
            let stored_hash: String = row
                .try_get("content_hash")
                .map_err(|_| RepositoryError::invalid_data())?;
            if row
                .try_get::<String, _>("checkpoint_kind")
                .map_err(|_| RepositoryError::invalid_data())?
                != "task_retry_scheduled"
                || checkpoint_id != operation_checkpoint(transition_key)
                || schema_version != SCHEDULER_CHECKPOINT_SCHEMA_VERSION
                || scheduler_version != replay.projection_version()
                || stored_transition != transition_key.as_str()
                || stored_intent != intent_hash
                || stored_event != replay.event_id()
                || retry != *expected_retry
                || scheduler_checkpoint_content_hash(
                    claim.run_id().as_str(),
                    checkpoint_id.as_str(),
                    "task_retry_scheduled",
                    &stored_transition,
                    &stored_intent,
                    &stored_event,
                    schema_version,
                    scheduler_version,
                    &payload,
                )?
                .as_str()
                    != stored_hash
            {
                return Err(RepositoryError::invalid_data());
            }
            (checkpoint_id, None)
        }
        _ => return Err(RepositoryError::invalid_data()),
    };
    let mut output_versions = BTreeMap::new();
    if let Some(completion) = completion.as_ref() {
        let expected_outputs = match &completion.outcome {
            TaskOutcomeFact::Succeeded { outputs } => Some(outputs),
            TaskOutcomeFact::Failed { .. } => None,
        };
        if completion.output_receipts.len() != expected_outputs.map_or(0, BTreeMap::len) {
            return Err(RepositoryError::invalid_data());
        }
        let occurrence_key = canonical_json(
            &serde_json::to_value(&completion.occurrence)
                .map_err(|_| RepositoryError::canonicalization())?,
        )?;
        let occurrence_values = sqlx::query(
            "SELECT occurrence_key,port_id,owner_activation_id,runtime_value,value_ref,
                    declared_type,storage_kind,payload_id,artifact_id,content_hash,
                    projection_version
             FROM scheduler_occurrence_values
             WHERE run_id=? AND occurrence_key=? ORDER BY port_id",
        )
        .bind(claim.run_id().as_str())
        .bind(occurrence_key)
        .fetch_all(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if occurrence_values.len() != completion.output_receipts.len() {
            return Err(RepositoryError::invalid_data());
        }
        let contracts = claim
            .envelope()
            .request()
            .outputs()
            .iter()
            .map(|contract| (contract.port_id(), contract))
            .collect::<BTreeMap<_, _>>();
        let submitted_refs = match outcome {
            SchedulerTaskOutcome::Succeeded(success) => Some(success.value_refs()),
            SchedulerTaskOutcome::Failed(_) => None,
        };
        for row in occurrence_values {
            let stored = stored_value_from_row(claim.run_id(), &row)?;
            let receipt = completion
                .output_receipts
                .get(stored.port_id())
                .ok_or_else(RepositoryError::invalid_data)?;
            validate_canonical_task_output_projection_sqlite(
                transaction,
                claim.run_id(),
                stored.port_id(),
                receipt,
            )
            .await?;
            let expected = expected_outputs
                .and_then(|outputs| outputs.get(stored.port_id()))
                .ok_or_else(RepositoryError::invalid_data)?;
            let contract = contracts
                .get(stored.port_id())
                .ok_or_else(RepositoryError::invalid_data)?;
            let submitted_ref = submitted_refs
                .and_then(|refs| refs.get(stored.port_id()))
                .ok_or_else(RepositoryError::invalid_data)?;
            let occurrence: insight_engine::LogicalOccurrence = serde_json::from_str(
                &row.try_get::<String, _>("occurrence_key")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?;
            let storage_kind: String = row
                .try_get("storage_kind")
                .map_err(|_| RepositoryError::invalid_data())?;
            let payload_id: Option<String> = row
                .try_get("payload_id")
                .map_err(|_| RepositoryError::invalid_data())?;
            let artifact_id: Option<String> = row
                .try_get("artifact_id")
                .map_err(|_| RepositoryError::invalid_data())?;
            let row_hash: String = row
                .try_get("content_hash")
                .map_err(|_| RepositoryError::invalid_data())?;
            if receipt.owner_activation_id != *claim.activation_id()
                || receipt.occurrence != completion.occurrence
                || occurrence != completion.occurrence
                || receipt.runtime_value != *expected
                || receipt.runtime_value != *stored.runtime_value()
                || receipt.declared_type != *contract.value_type()
                || receipt.declared_type != *stored.declared_type()
                || receipt.content_hash.as_str() != row_hash
                || receipt.content_hash != *submitted_ref.content_hash()
                || receipt.occurrence_value_ref != *stored.value_ref()
                || receipt.occurrence_storage_kind != storage_kind
                || receipt.occurrence_payload_id != payload_id
                || receipt.occurrence_artifact_id != artifact_id
                || receipt.occurrence_projection_version != stored.projection_version()
                || !value_ref_matches_locator(
                    &receipt.canonical_value_ref,
                    &receipt.canonical_storage_kind,
                    receipt.canonical_payload_id.as_deref(),
                    receipt.canonical_artifact_id.as_deref(),
                    &receipt.content_hash,
                )
                || !value_ref_matches_locator(
                    &receipt.occurrence_value_ref,
                    &receipt.occurrence_storage_kind,
                    receipt.occurrence_payload_id.as_deref(),
                    receipt.occurrence_artifact_id.as_deref(),
                    &receipt.content_hash,
                )
            {
                return Err(RepositoryError::invalid_data());
            }
            validate_value_ref_resource_sqlite(
                transaction,
                claim.run_id(),
                &receipt.canonical_value_ref,
            )
            .await?;
            validate_value_ref_resource_sqlite(
                transaction,
                claim.run_id(),
                &receipt.occurrence_value_ref,
            )
            .await?;
            output_versions.insert(
                stored.port_id().clone(),
                receipt.canonical_projection_version,
            );
        }
    }
    let expected_retrieval = match outcome {
        SchedulerTaskOutcome::Succeeded(success) => {
            retrieval_publication_adapter::prepare_retrieval_publication(
                claim,
                success,
                transition_key.as_str(),
                intent_hash,
                replay.event_id(),
                replay.event_seq(),
            )?
        }
        SchedulerTaskOutcome::Failed(_) => None,
    };
    super::sqlite_retrieval_publication::validate_exact_retrieval_publication_sqlite(
        transaction,
        claim.run_id(),
        claim.task_id().as_str(),
        expected_retrieval.as_ref(),
    )
    .await?;
    Ok(SchedulerTaskCompletionReceipt::new(
        SchedulerCommitReceipt::new(
            replay.event_seq(),
            replay.event_id().to_owned(),
            checkpoint_id,
            replay.projection_version(),
        ),
        claim.task_id().clone(),
        claim.envelope().attempt_no(),
        claim.envelope().lease_epoch(),
        output_versions,
    ))
}

fn validate_success(
    claim: &SchedulerTaskClaim,
    success: &SchedulerTaskSuccess,
) -> Result<(), RepositoryError> {
    success.validate_value_ref_integrity()?;
    if success.result().effect_evidence() != EffectEvidence::Committed
        || success
            .result()
            .outputs()
            .keys()
            .ne(success.value_refs().keys())
    {
        return Err(RepositoryError::invalid_data());
    }
    let contracts = claim
        .envelope()
        .request()
        .outputs()
        .iter()
        .map(|contract| (contract.port_id(), contract))
        .collect::<BTreeMap<_, _>>();
    if success.result().outputs().iter().any(|(port_id, value)| {
        contracts
            .get(port_id)
            .is_none_or(|contract| !value.matches(contract.value_type()))
    }) || claim.envelope().request().outputs().iter().any(|contract| {
        contract.required() && !success.result().outputs().contains_key(contract.port_id())
    }) {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

fn validate_task_outcome(
    claim: &SchedulerTaskClaim,
    current_effect_evidence: EffectEvidence,
    outcome: &SchedulerTaskOutcome,
) -> Result<(), RepositoryError> {
    match outcome {
        SchedulerTaskOutcome::Succeeded(success) => {
            if claim.mode() != SchedulerTaskClaimMode::Execute
                || claim.lease_loss_evidence().is_some()
                || !current_effect_evidence.can_transition_to(EffectEvidence::Committed)
            {
                return Err(RepositoryError::invalid_data());
            }
            validate_success(claim, success)
        }
        SchedulerTaskOutcome::Failed(failure) => {
            failure.validate_for_authority(claim, current_effect_evidence)
        }
    }
}

fn validate_runtime_deadline_authority(
    authority: &AuthoritativeTaskClaim,
    outcome: &SchedulerTaskOutcome,
) -> Result<bool, RepositoryError> {
    if authority.claim.mode() != SchedulerTaskClaimMode::Execute {
        return Ok(true);
    }
    let is_runtime_deadline = matches!(
        outcome,
        SchedulerTaskOutcome::Failed(failure) if failure.is_runtime_deadline()
    );
    let timeout = chrono::Duration::milliseconds(
        i64::try_from(
            authority
                .claim
                .envelope()
                .request()
                .effect_policy()
                .timeout_ms(),
        )
        .map_err(|_| RepositoryError::invalid_data())?,
    );
    let deadline = authority
        .started_at
        .and_then(|started_at| started_at.checked_add_signed(timeout))
        .ok_or_else(RepositoryError::invalid_data)?;
    if authority.attempt_lifecycle != "running"
        || authority.activation_lifecycle != "running"
        || authority.current_effect_evidence != EffectEvidence::Started
        || (is_runtime_deadline && authority.database_now < deadline)
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(is_runtime_deadline || authority.database_now < deadline)
}

#[allow(clippy::too_many_arguments)]
async fn commit_success_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &SchedulerTaskClaim,
    success: &SchedulerTaskSuccess,
    transition_key: &TransitionKey,
    intent_hash: &str,
    event_id_value: &str,
    event_seq: u64,
    current_run_version: u64,
) -> Result<SchedulerTaskCompletionReceipt, RepositoryError> {
    validate_success(claim, success)?;
    let retrieval_publication = retrieval_publication_adapter::prepare_retrieval_publication(
        claim,
        success,
        transition_key.as_str(),
        intent_hash,
        event_id_value,
        event_seq,
    )?;
    let output_value = Value::Object(
        success
            .result()
            .outputs()
            .iter()
            .map(|(port_id, value)| (port_id.as_str().to_owned(), value.value().clone()))
            .collect(),
    );
    let (payload_id, output_hash) =
        insert_or_get_payload(transaction, claim.run_id(), &output_value).await?;
    let attempt_rows = sqlx::query(
        "UPDATE node_attempts SET lifecycle='succeeded',effect_evidence='committed',
            output_payload_id=?,output_artifact_id=NULL,output_value_hash=?,failure_code=NULL,
            completion_transition_key=?,terminal_event_id=?,projection_version=projection_version+1,
            terminal_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND lease_epoch=?
           AND fencing_token=? AND lifecycle='running' AND effect_evidence='started'",
    )
    .bind(&payload_id)
    .bind(&output_hash)
    .bind(transition_key.as_str())
    .bind(event_id_value)
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64_from_u64(claim.envelope().lease_epoch().get())?)
    .bind(claim.envelope().fencing_token())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if attempt_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    let activation_rows = sqlx::query(
        "UPDATE node_activations SET lifecycle='succeeded',effect_evidence='committed',
            current_attempt_no=NULL,current_lease_epoch=NULL,current_fencing_token=NULL,
            output_payload_id=?,output_artifact_id=NULL,output_value_hash=?,winning_attempt_no=?,
            projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP,
            terminal_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND lifecycle IN ('leased','running')
           AND effect_evidence='started'
           AND current_attempt_no=? AND current_lease_epoch=? AND current_fencing_token=?",
    )
    .bind(&payload_id)
    .bind(&output_hash)
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64_from_u64(claim.envelope().lease_epoch().get())?)
    .bind(claim.envelope().fencing_token())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if activation_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    let contracts = claim
        .envelope()
        .request()
        .outputs()
        .iter()
        .map(|contract| (contract.port_id(), contract))
        .collect::<BTreeMap<_, _>>();
    let occurrence =
        activation_occurrence(transaction, claim.run_id(), claim.activation_id()).await?;
    let mut output_versions = BTreeMap::new();
    let mut output_receipts = BTreeMap::new();
    for (port_id, runtime_value) in success.result().outputs() {
        let contract = contracts
            .get(port_id)
            .ok_or_else(RepositoryError::invalid_data)?;
        let value_ref = success
            .value_refs()
            .get(port_id)
            .ok_or_else(RepositoryError::invalid_data)?;
        let version = upsert_scheduler_value(
            transaction,
            claim.run_id(),
            claim.activation_id(),
            port_id,
            runtime_value,
            value_ref,
            contract.value_type(),
        )
        .await?;
        upsert_occurrence_value(
            transaction,
            claim.run_id(),
            &occurrence,
            claim.activation_id(),
            port_id,
            runtime_value,
            value_ref,
            contract.value_type(),
        )
        .await?;
        let receipt =
            load_task_output_receipt_sqlite(transaction, claim.run_id(), &occurrence, port_id)
                .await?;
        if receipt.owner_activation_id != *claim.activation_id()
            || receipt.runtime_value != *runtime_value
            || receipt.declared_type != *contract.value_type()
            || receipt.canonical_projection_version != version
        {
            return Err(RepositoryError::invalid_data());
        }
        output_versions.insert(port_id.clone(), receipt.canonical_projection_version);
        output_receipts.insert(port_id.clone(), receipt);
    }
    if let Some(publication) = retrieval_publication.as_ref() {
        super::sqlite_retrieval_publication::insert_retrieval_publication_sqlite(
            transaction,
            publication,
        )
        .await?;
    }
    let task_rows = sqlx::query(
        "UPDATE task_outbox SET task_state='published',claim_mode='acknowledge',
            published_at=CURRENT_TIMESTAMP,
            last_error_code=NULL,projection_version=projection_version+1
         WHERE run_id=? AND task_id=? AND task_state='claimed' AND claimed_by=? AND claim_token=?
           AND claim_mode='execute'
           AND json_extract(task_envelope,'$.request.admission_class')=?
           AND projection_version=? AND julianday(claim_expires_at)>julianday('now')",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.task_id().as_str())
    .bind(claim.claimed_by())
    .bind(claim.claim_token())
    .bind(claim.envelope().request().admission_class().as_str())
    .bind(i64_from_u64(claim.task_projection_version())?)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if task_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    let next_run_version = current_run_version
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let run_rows = sqlx::query(
        "UPDATE workflow_runs SET projection_version=projection_version+1,
            updated_at=CURRENT_TIMESTAMP WHERE run_id=? AND projection_version=?",
    )
    .bind(claim.run_id().as_str())
    .bind(i64_from_u64(current_run_version)?)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if run_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    let checkpoint_id = insert_task_completion_checkpoint(
        transaction,
        claim,
        transition_key,
        intent_hash,
        event_id_value,
        next_run_version,
        TaskOutcomeFact::Succeeded {
            outputs: success.result().outputs().clone(),
        },
        output_receipts,
    )
    .await?;
    Ok(SchedulerTaskCompletionReceipt::new(
        SchedulerCommitReceipt::new(
            event_seq,
            event_id_value.to_owned(),
            checkpoint_id,
            next_run_version,
        ),
        claim.task_id().clone(),
        claim.envelope().attempt_no(),
        claim.envelope().lease_epoch(),
        output_versions,
    ))
}

async fn bump_scheduler_version_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    current_version: u64,
) -> Result<u64, RepositoryError> {
    let next = current_version
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let rows = sqlx::query(
        "UPDATE workflow_runs SET projection_version=?,updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND projection_version=?
           AND lifecycle IN ('created','active','waiting','terminating')",
    )
    .bind(i64_from_u64(next)?)
    .bind(run_id.as_str())
    .bind(i64_from_u64(current_version)?)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    Ok(next)
}

#[allow(clippy::too_many_arguments)]
async fn commit_retry_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &SchedulerTaskClaim,
    current_effect_evidence: EffectEvidence,
    failure: &super::SchedulerTaskFailure,
    retry_at: DateTime<Utc>,
    remaining_attempts: u32,
    transition_key: &TransitionKey,
    intent_hash: &str,
    event_id_value: &str,
    event_seq: u64,
    current_run_version: u64,
) -> Result<SchedulerTaskCompletionReceipt, RepositoryError> {
    let policy = claim.envelope().request().effect_policy();
    if !failure.retryable()
        || !failure
            .effect_evidence()
            .permits_automatic_retry(policy.effect_idempotency())
        || remaining_attempts == 0
        || remaining_attempts
            != policy
                .max_attempts()
                .saturating_sub(claim.envelope().attempt_no().get())
    {
        return Err(RepositoryError::invalid_data());
    }
    let next_attempt = model_data(claim.envelope().attempt_no().next())?;
    let next_epoch = model_data(claim.envelope().lease_epoch().next())?;
    let next_retry_budget = policy.max_attempts().saturating_sub(next_attempt.get());
    let next_fencing_token = fencing_token(transition_key);
    let next_envelope = DurableTaskExecutionRequest::new(
        claim.envelope().request().clone(),
        next_attempt,
        next_epoch,
        next_fencing_token.clone(),
        current_run_version
            .checked_add(1)
            .ok_or_else(RepositoryError::invalid_data)?,
    )?;
    let encoded_envelope = canonical_json(
        &serde_json::to_value(&next_envelope).map_err(|_| RepositoryError::canonicalization())?,
    )?;
    let attempt_rows = sqlx::query(
        "UPDATE node_attempts SET lifecycle='failed',effect_evidence=?,failure_code=?,
            completion_transition_key=?,terminal_event_id=?,projection_version=projection_version+1,
            terminal_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND lease_epoch=?
           AND fencing_token=? AND lifecycle IN ('leased','running') AND effect_evidence=?",
    )
    .bind(effect_evidence_str(failure.effect_evidence()))
    .bind(failure.code())
    .bind(transition_key.as_str())
    .bind(event_id_value)
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64_from_u64(claim.envelope().lease_epoch().get())?)
    .bind(claim.envelope().fencing_token())
    .bind(effect_evidence_str(current_effect_evidence))
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if attempt_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    sqlx::query(
        "INSERT INTO node_attempts (
            run_id,activation_id,attempt_no,lease_epoch,fencing_token,effect_id,lifecycle,
            effect_evidence,worker_id,lease_expires_at,heartbeat_at,projection_version,created_at
         ) VALUES (?,?,?,?,?,?,'leased','not_started','scheduler-outbox',
                   datetime('now','+1 day'),CURRENT_TIMESTAMP,0,CURRENT_TIMESTAMP)",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(next_attempt.get()))
    .bind(i64_from_u64(next_epoch.get())?)
    .bind(&next_fencing_token)
    .bind(claim.envelope().request().effect_id().as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let activation_rows = sqlx::query(
        "UPDATE node_activations SET lifecycle='leased',effect_evidence='not_started',last_attempt_no=?,
            last_lease_epoch=?,current_attempt_no=?,current_lease_epoch=?,current_fencing_token=?,
            retry_budget_remaining=?,projection_version=projection_version+1,
            updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND lifecycle IN ('leased','running')
           AND effect_evidence=? AND current_attempt_no=? AND current_lease_epoch=?
           AND current_fencing_token=?",
    )
    .bind(i64::from(next_attempt.get()))
    .bind(i64_from_u64(next_epoch.get())?)
    .bind(i64::from(next_attempt.get()))
    .bind(i64_from_u64(next_epoch.get())?)
    .bind(&next_fencing_token)
    .bind(i64::from(next_retry_budget))
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(effect_evidence_str(current_effect_evidence))
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64_from_u64(claim.envelope().lease_epoch().get())?)
    .bind(claim.envelope().fencing_token())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if activation_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    let task_rows = sqlx::query(
        "UPDATE task_outbox SET attempt_no=?,lease_epoch=?,fencing_token=?,task_state='pending',
            task_envelope=?,available_at=?,claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL,
            claim_mode=NULL,published_at=NULL,acked_at=NULL,last_error_code=?,
            projection_version=projection_version+1
         WHERE run_id=? AND task_id=? AND task_state='claimed' AND claimed_by=? AND claim_token=?
           AND claim_mode=? AND projection_version=?
           AND julianday(claim_expires_at)>julianday('now')",
    )
    .bind(i64::from(next_attempt.get()))
    .bind(i64_from_u64(next_epoch.get())?)
    .bind(&next_fencing_token)
    .bind(encoded_envelope)
    .bind(now_text(retry_at))
    .bind(failure.code())
    .bind(claim.run_id().as_str())
    .bind(claim.task_id().as_str())
    .bind(claim.claimed_by())
    .bind(claim.claim_token())
    .bind(claim.mode().as_str())
    .bind(i64_from_u64(claim.task_projection_version())?)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if task_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    let next_run_version =
        bump_scheduler_version_sqlite(transaction, claim.run_id(), current_run_version).await?;
    let checkpoint_id = insert_task_retry_checkpoint_sqlite(
        transaction,
        claim,
        transition_key,
        intent_hash,
        event_id_value,
        next_run_version,
        failure,
        retry_at,
        remaining_attempts,
        next_attempt,
        next_epoch,
        &next_fencing_token,
        &next_envelope,
    )
    .await?;
    Ok(SchedulerTaskCompletionReceipt::new(
        SchedulerCommitReceipt::new(
            event_seq,
            event_id_value.to_owned(),
            checkpoint_id,
            next_run_version,
        ),
        claim.task_id().clone(),
        claim.envelope().attempt_no(),
        claim.envelope().lease_epoch(),
        BTreeMap::new(),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn commit_terminal_failure_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &SchedulerTaskClaim,
    current_effect_evidence: EffectEvidence,
    failure: &super::SchedulerTaskFailure,
    timed_out: bool,
    transition_key: &TransitionKey,
    intent_hash: &str,
    event_id_value: &str,
    event_seq: u64,
    current_run_version: u64,
) -> Result<SchedulerTaskCompletionReceipt, RepositoryError> {
    let attempt_lifecycle = if timed_out { "timed_out" } else { "failed" };
    let activation_lifecycle = attempt_lifecycle;
    let termination_reason = if timed_out {
        "timed_out"
    } else if failure.effect_evidence() == EffectEvidence::Unknown {
        "effect_outcome_unknown"
    } else {
        "failure"
    };
    let attempt_rows = sqlx::query(
        "UPDATE node_attempts SET lifecycle=?,effect_evidence=?,failure_code=?,
            completion_transition_key=?,terminal_event_id=?,projection_version=projection_version+1,
            terminal_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND lease_epoch=?
           AND fencing_token=? AND lifecycle IN ('leased','running') AND effect_evidence=?",
    )
    .bind(attempt_lifecycle)
    .bind(effect_evidence_str(failure.effect_evidence()))
    .bind(failure.code())
    .bind(transition_key.as_str())
    .bind(event_id_value)
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64_from_u64(claim.envelope().lease_epoch().get())?)
    .bind(claim.envelope().fencing_token())
    .bind(effect_evidence_str(current_effect_evidence))
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if attempt_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    let activation_rows = sqlx::query(
        "UPDATE node_activations SET lifecycle=?,effect_evidence=?,current_attempt_no=NULL,
            current_lease_epoch=NULL,current_fencing_token=NULL,termination_intent_reason=?,
            termination_intent_transition_key=?,termination_intent_at=CURRENT_TIMESTAMP,
            projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP,
            terminal_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND lifecycle IN ('leased','running')
           AND effect_evidence=? AND current_attempt_no=? AND current_lease_epoch=?
           AND current_fencing_token=?",
    )
    .bind(activation_lifecycle)
    .bind(effect_evidence_str(failure.effect_evidence()))
    .bind(termination_reason)
    .bind(transition_key.as_str())
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(effect_evidence_str(current_effect_evidence))
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64_from_u64(claim.envelope().lease_epoch().get())?)
    .bind(claim.envelope().fencing_token())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if activation_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    let task_rows = sqlx::query(
        "UPDATE task_outbox SET task_state='dead',claimed_by=NULL,claim_token=NULL,
            claim_expires_at=NULL,claim_mode=NULL,last_error_code=?,
            projection_version=projection_version+1
         WHERE run_id=? AND task_id=? AND task_state='claimed' AND claimed_by=? AND claim_token=?
           AND claim_mode=? AND projection_version=?
           AND julianday(claim_expires_at)>julianday('now')",
    )
    .bind(failure.code())
    .bind(claim.run_id().as_str())
    .bind(claim.task_id().as_str())
    .bind(claim.claimed_by())
    .bind(claim.claim_token())
    .bind(claim.mode().as_str())
    .bind(i64_from_u64(claim.task_projection_version())?)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if task_rows != 1 {
        return Err(RepositoryError::invalid_data());
    }
    // A leaf failure is not a Run terminal. Persist the closed task outcome
    // and let the pure planner propagate it through fork/map/all-settled and
    // error-boundary semantics before it chooses FailRunInternal/FailRun.
    let next_run_version =
        bump_scheduler_version_sqlite(transaction, claim.run_id(), current_run_version).await?;
    let checkpoint_id = insert_task_completion_checkpoint(
        transaction,
        claim,
        transition_key,
        intent_hash,
        event_id_value,
        next_run_version,
        TaskOutcomeFact::Failed {
            failure: task_failure_fact(failure)?,
        },
        BTreeMap::new(),
    )
    .await?;
    Ok(SchedulerTaskCompletionReceipt::new(
        SchedulerCommitReceipt::new(
            event_seq,
            event_id_value.to_owned(),
            checkpoint_id,
            next_run_version,
        ),
        claim.task_id().clone(),
        claim.envelope().attempt_no(),
        claim.envelope().lease_epoch(),
        BTreeMap::new(),
    ))
}

async fn commit_task_outcome_sqlite(
    repository: &SqliteDurableRepository,
    claim: &SchedulerTaskClaim,
    outcome: &SchedulerTaskOutcome,
) -> Result<SchedulerTaskCommitOutcome<SchedulerTaskCompletionReceipt>, RepositoryError> {
    let transition_key = task_outcome_transition_key(claim)?;
    let intent_hash = canonical_intent_hash(&json!({
        "operation": "scheduler.task.outcome",
        "run_id": claim.run_id(),
        "task_id": claim.task_id(),
        "activation_id": claim.activation_id(),
        "attempt_no": claim.envelope().attempt_no(),
        "lease_epoch": claim.envelope().lease_epoch(),
        "fencing_token": claim.envelope().fencing_token(),
        "admission_class": claim.envelope().request().admission_class(),
        "outcome": outcome,
    }))?;
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    match load_replay(
        &mut transaction,
        claim.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(replay) => {
            let receipt = exact_task_outcome_receipt(
                &mut transaction,
                claim,
                &transition_key,
                intent_hash.as_str(),
                outcome,
                &replay,
            )
            .await?;
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(SchedulerTaskCommitOutcome::ExactReplay {
                authoritative: receipt,
            });
        }
        Replay::Vacant => {}
    }
    let Some(authority) =
        load_authoritative_task_claim_sqlite(&mut transaction, claim, TaskClaimComparison::Exact)
            .await?
    else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    };
    if authority.task_state != "claimed" || !authority.is_fresh() || !authority.permits_execution()
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    }
    if authority.claim.mode() == SchedulerTaskClaimMode::Execute
        && (authority.attempt_lifecycle != "running"
            || authority.activation_lifecycle != "running"
            || authority.current_effect_evidence != EffectEvidence::Started)
    {
        return Err(RepositoryError::invalid_data());
    }
    validate_task_outcome(&authority.claim, authority.current_effect_evidence, outcome)?;
    if !validate_runtime_deadline_authority(&authority, outcome)? {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::OperationDeadlineElapsed);
    }
    let started_at = authority.started_at;
    let claim = &authority.claim;
    let current_run_version = current_scheduler_version(&mut transaction, claim.run_id()).await?;
    let next_run_version = current_run_version
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let (scope, node) =
        activation_identity(&mut transaction, claim.run_id(), claim.activation_id()).await?;
    let payload = match outcome {
        SchedulerTaskOutcome::Succeeded(success) => {
            let encoded = Value::Object(
                success
                    .result()
                    .outputs()
                    .iter()
                    .map(|(port, value)| (port.as_str().to_owned(), value.value().clone()))
                    .collect(),
            );
            ExecutionEventPayload::AttemptSucceeded {
                output: Some(output_summary(&encoded)?),
            }
        }
        SchedulerTaskOutcome::Failed(failure) => match failure.disposition() {
            SchedulerFailureDisposition::TimedOut => ExecutionEventPayload::AttemptTimedOut,
            SchedulerFailureDisposition::Retry { .. } | SchedulerFailureDisposition::Terminal => {
                ExecutionEventPayload::AttemptFailed {
                    failure: Some(internal_failure(failure)?),
                }
            }
        },
    };
    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(claim.run_id().clone()).for_attempt(
            scope,
            node.clone(),
            claim.activation_id().clone(),
            claim.envelope().attempt_no(),
        ),
        payload,
    ))?;
    let event_seq = allocate_event_seq(&mut transaction, claim.run_id()).await?;
    let event_id_value = event_id(&transition_key);
    let occurred_at = insert_event(
        &mut transaction,
        claim.run_id(),
        event_seq,
        &event_id_value,
        &transition_key,
        intent_hash.as_str(),
        next_run_version,
        &event,
    )
    .await?;
    let receipt = match outcome {
        SchedulerTaskOutcome::Succeeded(success) => {
            commit_success_sqlite(
                &mut transaction,
                claim,
                success,
                &transition_key,
                intent_hash.as_str(),
                &event_id_value,
                event_seq,
                current_run_version,
            )
            .await?
        }
        SchedulerTaskOutcome::Failed(failure) => match failure.disposition() {
            SchedulerFailureDisposition::Retry {
                retry_at,
                remaining_attempts,
            } => {
                commit_retry_sqlite(
                    &mut transaction,
                    claim,
                    authority.current_effect_evidence,
                    failure,
                    *retry_at,
                    *remaining_attempts,
                    &transition_key,
                    intent_hash.as_str(),
                    &event_id_value,
                    event_seq,
                    current_run_version,
                )
                .await?
            }
            SchedulerFailureDisposition::Terminal => {
                commit_terminal_failure_sqlite(
                    &mut transaction,
                    claim,
                    authority.current_effect_evidence,
                    failure,
                    false,
                    &transition_key,
                    intent_hash.as_str(),
                    &event_id_value,
                    event_seq,
                    current_run_version,
                )
                .await?
            }
            SchedulerFailureDisposition::TimedOut => {
                commit_terminal_failure_sqlite(
                    &mut transaction,
                    claim,
                    authority.current_effect_evidence,
                    failure,
                    true,
                    &transition_key,
                    intent_hash.as_str(),
                    &event_id_value,
                    event_seq,
                    current_run_version,
                )
                .await?
            }
        },
    };
    let elapsed_ms = operation_elapsed_ms(started_at, occurred_at)?;
    let public_payload = match outcome {
        SchedulerTaskOutcome::Succeeded(success) => {
            let output = Value::Object(
                success
                    .result()
                    .outputs()
                    .iter()
                    .map(|(port, value)| (port.as_str().to_owned(), value.value().clone()))
                    .collect(),
            );
            PublicEventPayload::OperationCompleted {
                node_id: node,
                activation_id: claim.activation_id().clone(),
                attempt_no: claim.envelope().attempt_no(),
                elapsed_ms,
                output_bytes: output_summary(&output)?.size_bytes(),
            }
        }
        SchedulerTaskOutcome::Failed(failure) => PublicEventPayload::OperationFailed {
            node_id: node,
            activation_id: claim.activation_id().clone(),
            attempt_no: claim.envelope().attempt_no(),
            elapsed_ms,
            failure: public_operation_failure(failure)?,
        },
    };
    insert_public_operation(
        &mut transaction,
        claim.run_id(),
        &transition_key,
        &event_id_value,
        event_seq,
        occurred_at,
        public_payload,
    )
    .await?;
    finalize_projection_checkpoints(&mut transaction, claim.run_id(), &event_id_value).await?;
    if sqlx::query_scalar::<_, i64>("SELECT julianday('now') < julianday(?)")
        .bind(now_text(claim.claim_expires_at()))
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        != 1
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    }
    if claim.mode() == SchedulerTaskClaimMode::Execute {
        let operation_deadline = started_at
            .and_then(|value| {
                i64::try_from(claim.envelope().request().effect_policy().timeout_ms())
                    .ok()
                    .and_then(|milliseconds| {
                        value.checked_add_signed(chrono::Duration::milliseconds(milliseconds))
                    })
            })
            .ok_or_else(RepositoryError::invalid_data)?;
        let deadline_elapsed =
            sqlx::query_scalar::<_, i64>("SELECT julianday('now') >= julianday(?)")
                .bind(now_text(operation_deadline))
                .fetch_one(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
                == 1;
        let is_runtime_deadline = matches!(
            outcome,
            SchedulerTaskOutcome::Failed(failure) if failure.is_runtime_deadline()
        );
        if deadline_elapsed != is_runtime_deadline {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            if deadline_elapsed {
                return Ok(SchedulerTaskCommitOutcome::OperationDeadlineElapsed);
            }
            return Err(RepositoryError::invalid_data());
        }
    }
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(SchedulerTaskCommitOutcome::Committed { result: receipt })
}

async fn acknowledge_task_sqlite(
    repository: &SqliteDurableRepository,
    claim: &SchedulerTaskClaim,
) -> Result<bool, RepositoryError> {
    if !matches!(
        claim.mode(),
        SchedulerTaskClaimMode::Execute | SchedulerTaskClaimMode::Acknowledge
    ) {
        return Ok(false);
    }
    let transition_key = TransitionKey::derive(
        "scheduler.task.acknowledge.v1",
        &[
            claim.run_id().as_str(),
            claim.task_id().as_str(),
            claim.envelope().fencing_token(),
        ],
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let intent_hash = canonical_intent_hash(&json!({
        "operation": "scheduler.task.acknowledge",
        "run_id": claim.run_id(),
        "task_id": claim.task_id(),
        "attempt_no": claim.envelope().attempt_no(),
        "lease_epoch": claim.envelope().lease_epoch(),
        "fencing_token": claim.envelope().fencing_token(),
        "admission_class": claim.envelope().request().admission_class(),
    }))?;
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let Some(authority) = load_authoritative_task_claim_sqlite(
        &mut transaction,
        claim,
        TaskClaimComparison::AcknowledgeTransition,
    )
    .await?
    else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(false);
    };
    if authority.task_state == "acked" {
        match load_replay(
            &mut transaction,
            claim.run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            Replay::Exact(replay) => {
                validate_task_transition_event_sqlite(
                    &mut transaction,
                    claim,
                    &transition_key,
                    intent_hash.as_str(),
                    &replay,
                    &ExecutionEventPayload::ProjectionMutated {
                        mutation: ProjectionMutationKind::TaskAcknowledged,
                    },
                    false,
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Ok(true);
            }
            Replay::Vacant => return Err(RepositoryError::invalid_data()),
        }
    }
    if authority.task_state != "published" || !authority.is_fresh() {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(false);
    }
    if matches!(
        load_replay(
            &mut transaction,
            claim.run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?,
        Replay::Exact(_)
    ) {
        return Err(RepositoryError::invalid_data());
    }
    let next_task_version = sqlx::query_scalar::<_, i64>(
        "UPDATE task_outbox SET task_state='acked',acked_at=CURRENT_TIMESTAMP,
            projection_version=projection_version+1
         WHERE run_id=? AND task_id=? AND task_state='published' AND claimed_by=?
           AND claim_token=? AND claim_mode='acknowledge' AND projection_version=?
           AND julianday(claim_expires_at)>julianday('now')
         RETURNING projection_version",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.task_id().as_str())
    .bind(authority.claim.claimed_by())
    .bind(claim.claim_token())
    .bind(i64_from_u64(authority.claim.task_projection_version())?)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(next_task_version) = next_task_version else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(false);
    };
    let event_id_value = append_projection_mutation_event(
        &mut transaction,
        claim.run_id(),
        &transition_key,
        intent_hash.as_str(),
        ProjectionMutationKind::TaskAcknowledged,
        u64_from_i64(next_task_version)?,
    )
    .await?;
    finalize_projection_checkpoints(&mut transaction, claim.run_id(), &event_id_value).await?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(true)
}

fn model_call_operation_deadline(
    authority: &AuthoritativeTaskClaim,
) -> Result<DateTime<Utc>, RepositoryError> {
    let timeout = chrono::Duration::milliseconds(
        i64::try_from(
            authority
                .claim
                .envelope()
                .request()
                .effect_policy()
                .timeout_ms(),
        )
        .map_err(|_| RepositoryError::invalid_data())?,
    );
    authority
        .started_at
        .and_then(|started_at| started_at.checked_add_signed(timeout))
        .ok_or_else(RepositoryError::invalid_data)
}

fn latest_model_call_matches_claim(
    latest: &LatestParentModelCallView,
    claim: &SchedulerTaskClaim,
) -> bool {
    latest_task_id(latest) == claim.task_id().as_str()
        && latest_lease_epoch(latest) == claim.envelope().lease_epoch().get()
        && latest_fencing_token(latest) == claim.envelope().fencing_token()
}

async fn load_ready_continuation_turn_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &SchedulerTaskClaim,
    model_call_no: u32,
) -> Result<ModelContinuationTurn, RepositoryError> {
    let assistant_content = sqlx::query_scalar::<_, Option<String>>(
        "SELECT assistant_content FROM model_tool_call_batches
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
           AND execution_status='succeeded' AND continuation_status='ready_continue'",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let rows = sqlx::query(
        "SELECT call_index,call_id,tool_name,arguments,result_json,call_status,effect_evidence
         FROM model_tool_calls
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
         ORDER BY call_index",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if rows.is_empty() {
        return Err(RepositoryError::invalid_data());
    }
    let mut calls = Vec::with_capacity(rows.len());
    let mut results = Vec::with_capacity(rows.len());
    for (expected_index, row) in rows.into_iter().enumerate() {
        if row
            .try_get::<String, _>("call_status")
            .map_err(|_| RepositoryError::invalid_data())?
            != "succeeded"
            || row
                .try_get::<String, _>("effect_evidence")
                .map_err(|_| RepositoryError::invalid_data())?
                != "committed"
        {
            return Err(RepositoryError::invalid_data());
        }
        let index = u32::try_from(
            row.try_get::<i64, _>("call_index")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        if index != u32::try_from(expected_index).map_err(|_| RepositoryError::invalid_data())? {
            return Err(RepositoryError::invalid_data());
        }
        let call_id: String = row
            .try_get("call_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let arguments = serde_json::from_str::<Value>(
            &row.try_get::<String, _>("arguments")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let result_json = row
            .try_get::<Option<String>, _>("result_json")
            .map_err(|_| RepositoryError::invalid_data())?
            .ok_or_else(RepositoryError::invalid_data)?;
        let result = serde_json::from_str::<Value>(&result_json)
            .map_err(|_| RepositoryError::invalid_data())?;
        calls.push(
            ModelToolCall::new(
                index,
                call_id.clone(),
                row.try_get::<String, _>("tool_name")
                    .map_err(|_| RepositoryError::invalid_data())?,
                arguments,
            )
            .map_err(|_| RepositoryError::invalid_data())?,
        );
        results.push(
            ModelToolResult::new(call_id, result).map_err(|_| RepositoryError::invalid_data())?,
        );
    }
    ModelContinuationTurn::new(model_call_no, assistant_content, calls, results)
        .map_err(|_| RepositoryError::invalid_data())
}

async fn validate_parent_model_round_chain_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &SchedulerTaskClaim,
    latest: &LatestParentModelCallView,
) -> Result<Vec<ModelContinuationTurn>, RepositoryError> {
    let rows = sqlx::query(
        "SELECT u.model_call_no,u.task_id,u.lease_epoch,u.fencing_token,u.call_status,
                u.finish_reason,b.execution_status,b.continuation_status
         FROM model_call_usage u
         LEFT JOIN model_tool_call_batches b ON b.run_id=u.run_id
           AND b.activation_id=u.activation_id AND b.attempt_no=u.attempt_no
           AND b.model_call_no=u.model_call_no
         WHERE u.run_id=? AND u.activation_id=? AND u.attempt_no=?
         ORDER BY u.model_call_no",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if rows.len()
        != usize::try_from(latest_model_call_no(latest))
            .map_err(|_| RepositoryError::invalid_data())?
    {
        return Err(RepositoryError::invalid_data());
    }
    let mut turns = Vec::new();
    for (index, row) in rows.into_iter().enumerate() {
        let expected_no = u32::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(RepositoryError::invalid_data)?;
        let actual_no = u32::try_from(
            row.try_get::<i64, _>("model_call_no")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        if actual_no != expected_no
            || row
                .try_get::<String, _>("task_id")
                .map_err(|_| RepositoryError::invalid_data())?
                != claim.task_id().as_str()
            || u64_from_i64(
                row.try_get("lease_epoch")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )? != claim.envelope().lease_epoch().get()
            || row
                .try_get::<String, _>("fencing_token")
                .map_err(|_| RepositoryError::invalid_data())?
                != claim.envelope().fencing_token()
            || row
                .try_get::<String, _>("call_status")
                .map_err(|_| RepositoryError::invalid_data())?
                != "completed"
            || row
                .try_get::<Option<String>, _>("finish_reason")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_deref()
                != Some("tool_calls")
        {
            return Err(RepositoryError::invalid_data());
        }
        let execution_status = row
            .try_get::<Option<String>, _>("execution_status")
            .map_err(|_| RepositoryError::invalid_data())?;
        let continuation_status = row
            .try_get::<Option<String>, _>("continuation_status")
            .map_err(|_| RepositoryError::invalid_data())?;
        if actual_no < latest_model_call_no(latest)
            || latest_continuation_status(latest) == Some("ready_continue")
        {
            if execution_status.as_deref() != Some("succeeded")
                || continuation_status.as_deref() != Some("ready_continue")
            {
                return Err(RepositoryError::invalid_data());
            }
            turns.push(load_ready_continuation_turn_sqlite(transaction, claim, actual_no).await?);
        } else if execution_status.as_deref() != latest_execution_status(latest)
            || continuation_status.as_deref() != latest_continuation_status(latest)
        {
            return Err(RepositoryError::invalid_data());
        }
    }
    Ok(turns)
}

async fn validate_terminal_tool_batch_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &SchedulerTaskClaim,
    latest: &LatestParentModelCallView,
) -> Result<bool, RepositoryError> {
    let row = sqlx::query(
        "SELECT COUNT(*) AS total,
                SUM(CASE WHEN call_status IN ('succeeded','failed','cancelled') THEN 1 ELSE 0 END) AS terminal,
                SUM(CASE WHEN failure_class='effect_outcome_unknown'
                              OR effect_evidence='unknown' THEN 1 ELSE 0 END) AS unknown,
                SUM(CASE WHEN call_status='failed' THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN call_status='cancelled' THEN 1 ELSE 0 END) AS cancelled
         FROM model_tool_calls
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(latest_model_call_no(latest)))
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let total: i64 = row
        .try_get("total")
        .map_err(|_| RepositoryError::invalid_data())?;
    let terminal = row
        .try_get::<Option<i64>, _>("terminal")
        .map_err(|_| RepositoryError::invalid_data())?
        .unwrap_or(0);
    let failed = row
        .try_get::<Option<i64>, _>("failed")
        .map_err(|_| RepositoryError::invalid_data())?
        .unwrap_or(0);
    let cancelled = row
        .try_get::<Option<i64>, _>("cancelled")
        .map_err(|_| RepositoryError::invalid_data())?
        .unwrap_or(0);
    let expected_terminal = match latest_continuation_status(latest) {
        Some("ready_failed") => failed > 0,
        Some("ready_cancelled") => failed == 0 && cancelled > 0,
        _ => false,
    };
    if total == 0 || terminal != total || !expected_terminal {
        return Err(RepositoryError::invalid_data());
    }
    Ok(row
        .try_get::<Option<i64>, _>("unknown")
        .map_err(|_| RepositoryError::invalid_data())?
        .unwrap_or(0)
        > 0)
}

async fn load_model_tool_parent_resume_sqlite(
    repository: &SqliteDurableRepository,
    claim: &SchedulerTaskClaim,
) -> Result<SchedulerTaskCommitOutcome<Option<ModelToolParentResume>>, RepositoryError> {
    if claim.mode() != SchedulerTaskClaimMode::Execute
        || claim.envelope().request().task_kind() != insight_engine::SchedulerTaskKind::Llm
    {
        return Err(RepositoryError::invalid_configuration());
    }
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let Some(authority) =
        load_authoritative_task_claim_sqlite(&mut transaction, claim, TaskClaimComparison::Exact)
            .await?
    else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StaleLease);
    };
    if authority.task_state != "claimed" || !authority.is_fresh() || !authority.permits_execution()
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StaleLease);
    }
    if authority.attempt_lifecycle != "running"
        || authority.activation_lifecycle != "running"
        || authority.current_effect_evidence != EffectEvidence::Started
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::Committed { result: None });
    }
    let operation_deadline = model_call_operation_deadline(&authority)?;
    if authority.database_now >= operation_deadline {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::OperationDeadlineElapsed);
    }
    let latest = load_latest_parent_model_call_sqlite(
        &mut transaction,
        claim.run_id(),
        claim.activation_id(),
        claim.envelope().attempt_no(),
    )
    .await?;
    let Some(latest) = latest else {
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::Committed { result: None });
    };
    if !latest_model_call_matches_claim(&latest, claim) || latest_is_waiting_tools(&latest) {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    }
    let resume = if latest_is_checkpointed(&latest) {
        validate_parent_model_round_chain_sqlite(&mut transaction, claim, &latest).await?;
        model_tool_parent_resume_activate_checkpointed(
            latest_model_call_no(&latest),
            operation_deadline,
        )?
    } else if latest_is_ready(&latest) {
        let turns =
            validate_parent_model_round_chain_sqlite(&mut transaction, claim, &latest).await?;
        match latest_continuation_status(&latest) {
            Some("ready_continue") => {
                model_tool_parent_resume_ready_continue(turns, operation_deadline)?
            }
            Some("ready_failed") => model_tool_parent_resume_ready_failed(
                latest_model_call_no(&latest),
                operation_deadline,
                validate_terminal_tool_batch_sqlite(&mut transaction, claim, &latest).await?,
            )?,
            Some("ready_cancelled") => {
                validate_terminal_tool_batch_sqlite(&mut transaction, claim, &latest).await?;
                model_tool_parent_resume_ready_cancelled(
                    latest_model_call_no(&latest),
                    operation_deadline,
                )?
            }
            _ => return Err(RepositoryError::invalid_data()),
        }
    } else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    };
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(SchedulerTaskCommitOutcome::Committed {
        result: Some(resume),
    })
}

fn response_item_authority(
    item_id: String,
    output_index: i64,
) -> Result<ResponseItemAuthority, RepositoryError> {
    ResponseItemAuthority::new(
        item_id,
        u32::try_from(output_index).map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())
}

fn frozen_model_publish(claim: &SchedulerTaskClaim) -> Result<bool, RepositoryError> {
    match claim
        .envelope()
        .request()
        .public_configuration()
        .get("publish")
    {
        Some(DescriptorValue::Boolean(publish)) => Ok(*publish),
        _ => Err(RepositoryError::invalid_data()),
    }
}

async fn reserve_model_call_sqlite(
    repository: &SqliteDurableRepository,
    claim: &SchedulerTaskClaim,
    model_call_no: u32,
    publish: bool,
) -> Result<SchedulerTaskCommitOutcome<ModelCallAuthority>, RepositoryError> {
    if claim.mode() != SchedulerTaskClaimMode::Execute
        || claim.envelope().request().task_kind() != insight_engine::SchedulerTaskKind::Llm
        || model_call_no == 0
        || frozen_model_publish(claim)? != publish
    {
        return Err(RepositoryError::invalid_configuration());
    }
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let Some(authority) =
        load_authoritative_task_claim_sqlite(&mut transaction, claim, TaskClaimComparison::Exact)
            .await?
    else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StaleLease);
    };
    if authority.task_state != "claimed" || !authority.is_fresh() || !authority.permits_execution()
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StaleLease);
    }
    if authority.claim.mode() != SchedulerTaskClaimMode::Execute
        || authority.attempt_lifecycle != "running"
        || authority.activation_lifecycle != "running"
        || authority.current_effect_evidence != EffectEvidence::Started
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    }
    let operation_deadline = model_call_operation_deadline(&authority)?;
    if authority.database_now >= operation_deadline {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::OperationDeadlineElapsed);
    }

    let response_id =
        sqlx::query_scalar::<_, String>("SELECT response_id FROM workflow_runs WHERE run_id=?")
            .bind(claim.run_id().as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(RepositoryError::invalid_data)?;
    let existing = sqlx::query(
        "SELECT task_id,lease_epoch,fencing_token,call_status
         FROM model_call_usage
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if let Some(existing) = existing {
        if existing
            .try_get::<String, _>("task_id")
            .map_err(|_| RepositoryError::invalid_data())?
            != claim.task_id().as_str()
            || u64_from_i64(
                existing
                    .try_get("lease_epoch")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )? != claim.envelope().lease_epoch().get()
            || existing
                .try_get::<String, _>("fencing_token")
                .map_err(|_| RepositoryError::invalid_data())?
                != claim.envelope().fencing_token()
        {
            return Err(RepositoryError::invalid_data());
        }
        let item = sqlx::query(
            "SELECT item_id,output_index,node_id,item_kind FROM response_public_items
             WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
               AND item_ordinal=0",
        )
        .bind(claim.run_id().as_str())
        .bind(claim.activation_id().as_str())
        .bind(i64::from(claim.envelope().attempt_no().get()))
        .bind(i64::from(model_call_no))
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let public_item = item
            .map(|item| {
                let item_id: String = item
                    .try_get("item_id")
                    .map_err(|_| RepositoryError::invalid_data())?;
                if item_id
                    != response_item_id(
                        claim.run_id(),
                        claim.activation_id(),
                        claim.envelope().attempt_no(),
                        model_call_no,
                    )
                    || item
                        .try_get::<String, _>("node_id")
                        .map_err(|_| RepositoryError::invalid_data())?
                        != claim.envelope().request().node_id().as_str()
                    || item
                        .try_get::<String, _>("item_kind")
                        .map_err(|_| RepositoryError::invalid_data())?
                        != "message"
                {
                    return Err(RepositoryError::invalid_data());
                }
                response_item_authority(
                    item_id,
                    item.try_get("output_index")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
            })
            .transpose()?;
        let authority = ModelCallAuthority::new_with_publication(
            response_id,
            model_call_no,
            publish,
            public_item,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::ExactReplay {
            authoritative: authority,
        });
    }

    let previous_call_no = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(model_call_no),0) FROM model_call_usage
         WHERE run_id=? AND activation_id=? AND attempt_no=?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .fetch_one(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if previous_call_no
        .checked_add(1)
        .and_then(|value| u32::try_from(value).ok())
        != Some(model_call_no)
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    }
    if model_call_no > 1 {
        let ready_rounds = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM model_call_usage u
             JOIN model_tool_call_batches b ON b.run_id=u.run_id
               AND b.activation_id=u.activation_id AND b.attempt_no=u.attempt_no
               AND b.model_call_no=u.model_call_no
             WHERE u.run_id=? AND u.activation_id=? AND u.attempt_no=?
               AND u.model_call_no<? AND u.call_status='completed'
               AND u.finish_reason='tool_calls' AND b.execution_status='succeeded'
               AND b.continuation_status='ready_continue'
               AND EXISTS (
                   SELECT 1 FROM model_tool_calls c
                   WHERE c.run_id=u.run_id AND c.activation_id=u.activation_id
                     AND c.attempt_no=u.attempt_no AND c.model_call_no=u.model_call_no
               )
               AND NOT EXISTS (
                   SELECT 1 FROM model_tool_calls c
                   WHERE c.run_id=u.run_id AND c.activation_id=u.activation_id
                     AND c.attempt_no=u.attempt_no AND c.model_call_no=u.model_call_no
                     AND (c.call_status<>'succeeded' OR c.effect_evidence IS NULL
                          OR c.effect_evidence<>'committed'
                          OR c.result_json IS NULL)
               )",
        )
        .bind(claim.run_id().as_str())
        .bind(claim.activation_id().as_str())
        .bind(i64::from(claim.envelope().attempt_no().get()))
        .bind(i64::from(model_call_no))
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if ready_rounds != i64::from(model_call_no - 1) {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(SchedulerTaskCommitOutcome::StateConflict);
        }
    }
    sqlx::query(
        "INSERT INTO model_call_usage (
            run_id,activation_id,attempt_no,model_call_no,task_id,lease_epoch,
            fencing_token,call_status,finish_reason,usage,usage_complete,
            created_at,updated_at
         ) VALUES (?,?,?,?,?,?,?,'started',NULL,NULL,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
    .bind(claim.task_id().as_str())
    .bind(i64_from_u64(claim.envelope().lease_epoch().get())?)
    .bind(claim.envelope().fencing_token())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if sqlx::query_scalar::<_, i64>("SELECT julianday('now') < julianday(?)")
        .bind(now_text(claim.claim_expires_at()))
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        != 1
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StaleLease);
    }
    if sqlx::query_scalar::<_, i64>("SELECT julianday('now') >= julianday(?)")
        .bind(now_text(operation_deadline))
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        == 1
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::OperationDeadlineElapsed);
    }
    let authority =
        ModelCallAuthority::new_with_publication(response_id, model_call_no, publish, None)
            .map_err(|_| RepositoryError::invalid_data())?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(SchedulerTaskCommitOutcome::Committed { result: authority })
}

async fn reserve_model_call_public_item_sqlite(
    repository: &SqliteDurableRepository,
    claim: &SchedulerTaskClaim,
    model_call_no: u32,
) -> Result<SchedulerTaskCommitOutcome<ResponseItemAuthority>, RepositoryError> {
    if claim.mode() != SchedulerTaskClaimMode::Execute
        || claim.envelope().request().task_kind() != insight_engine::SchedulerTaskKind::Llm
        || model_call_no == 0
        || !frozen_model_publish(claim)?
    {
        return Err(RepositoryError::invalid_configuration());
    }
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let Some(authority) =
        load_authoritative_task_claim_sqlite(&mut transaction, claim, TaskClaimComparison::Exact)
            .await?
    else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StaleLease);
    };
    if authority.task_state != "claimed" || !authority.is_fresh() || !authority.permits_execution()
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StaleLease);
    }
    if authority.claim.mode() != SchedulerTaskClaimMode::Execute
        || authority.attempt_lifecycle != "running"
        || authority.activation_lifecycle != "running"
        || authority.current_effect_evidence != EffectEvidence::Started
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    }
    let operation_deadline = model_call_operation_deadline(&authority)?;
    if authority.database_now >= operation_deadline {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::OperationDeadlineElapsed);
    }
    let call = sqlx::query(
        "SELECT task_id,lease_epoch,fencing_token,call_status
         FROM model_call_usage
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(call) = call else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    };
    if call
        .try_get::<String, _>("task_id")
        .map_err(|_| RepositoryError::invalid_data())?
        != claim.task_id().as_str()
        || u64_from_i64(
            call.try_get("lease_epoch")
                .map_err(|_| RepositoryError::invalid_data())?,
        )? != claim.envelope().lease_epoch().get()
        || call
            .try_get::<String, _>("fencing_token")
            .map_err(|_| RepositoryError::invalid_data())?
            != claim.envelope().fencing_token()
    {
        return Err(RepositoryError::invalid_data());
    }
    if call
        .try_get::<String, _>("call_status")
        .map_err(|_| RepositoryError::invalid_data())?
        != "started"
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    }
    let existing = sqlx::query(
        "SELECT item_id,output_index,node_id,item_kind,item_status
         FROM response_public_items
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
           AND item_ordinal=0",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if let Some(existing) = existing {
        let item_id: String = existing
            .try_get("item_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        if item_id
            != response_item_id(
                claim.run_id(),
                claim.activation_id(),
                claim.envelope().attempt_no(),
                model_call_no,
            )
            || existing
                .try_get::<String, _>("node_id")
                .map_err(|_| RepositoryError::invalid_data())?
                != claim.envelope().request().node_id().as_str()
            || existing
                .try_get::<String, _>("item_kind")
                .map_err(|_| RepositoryError::invalid_data())?
                != "message"
            || existing
                .try_get::<String, _>("item_status")
                .map_err(|_| RepositoryError::invalid_data())?
                != "reserved"
        {
            return Err(RepositoryError::invalid_data());
        }
        let item = response_item_authority(
            item_id,
            existing
                .try_get("output_index")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::ExactReplay {
            authoritative: item,
        });
    }
    let previous_output_index = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(output_index),-1) FROM response_public_items WHERE run_id=?",
    )
    .bind(claim.run_id().as_str())
    .fetch_one(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let output_index = previous_output_index
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let item_id = response_item_id(
        claim.run_id(),
        claim.activation_id(),
        claim.envelope().attempt_no(),
        model_call_no,
    );
    sqlx::query(
        "INSERT INTO response_public_items (
            run_id,activation_id,attempt_no,model_call_no,item_ordinal,item_id,
            output_index,node_id,item_kind,item_status,seal_index,safe_item,
            created_at,updated_at
         ) VALUES (?,?,?,?,0,?,?,?,'message','reserved',NULL,NULL,
                   CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
    .bind(&item_id)
    .bind(output_index)
    .bind(claim.envelope().request().node_id().as_str())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if sqlx::query_scalar::<_, i64>("SELECT julianday('now') < julianday(?)")
        .bind(now_text(claim.claim_expires_at()))
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        != 1
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StaleLease);
    }
    if sqlx::query_scalar::<_, i64>("SELECT julianday('now') >= julianday(?)")
        .bind(now_text(operation_deadline))
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        == 1
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::OperationDeadlineElapsed);
    }
    let item = response_item_authority(item_id, output_index)?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(SchedulerTaskCommitOutcome::Committed { result: item })
}

async fn reserve_model_call_public_function_item_sqlite(
    repository: &SqliteDurableRepository,
    claim: &SchedulerTaskClaim,
    model_call_no: u32,
    call_index: u32,
    call_id: &str,
    tool_name: &str,
) -> Result<SchedulerTaskCommitOutcome<ResponseItemAuthority>, RepositoryError> {
    if claim.mode() != SchedulerTaskClaimMode::Execute
        || claim.envelope().request().task_kind() != insight_engine::SchedulerTaskKind::Llm
        || model_call_no == 0
        || !frozen_model_publish(claim)?
        || call_id.is_empty()
        || call_id.len() > 256
        || call_id
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
        || tool_name.is_empty()
        || tool_name.len() > 64
        || !tool_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(RepositoryError::invalid_configuration());
    }
    let (_, max_calls, tools) =
        parse_frozen_model_tool_contract(claim.envelope().request().deployment_binding())?;
    let action = tools
        .get(tool_name)
        .ok_or_else(RepositoryError::invalid_data)?;
    let projection = WorkflowToolPublicProjection::from_frozen_effective_policy(
        action.effective_public_policy(),
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if call_index >= max_calls || !projection.raw_argument_deltas_authorized() {
        return Err(RepositoryError::invalid_configuration());
    }

    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let Some(authority) =
        load_authoritative_task_claim_sqlite(&mut transaction, claim, TaskClaimComparison::Exact)
            .await?
    else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StaleLease);
    };
    if authority.task_state != "claimed" || !authority.is_fresh() || !authority.permits_execution()
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StaleLease);
    }
    if authority.claim.mode() != SchedulerTaskClaimMode::Execute
        || authority.attempt_lifecycle != "running"
        || authority.activation_lifecycle != "running"
        || authority.current_effect_evidence != EffectEvidence::Started
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    }
    let operation_deadline = model_call_operation_deadline(&authority)?;
    if authority.database_now >= operation_deadline {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::OperationDeadlineElapsed);
    }
    let call = sqlx::query(
        "SELECT task_id,lease_epoch,fencing_token,call_status
         FROM model_call_usage
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(call) = call else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    };
    if call
        .try_get::<String, _>("task_id")
        .map_err(|_| RepositoryError::invalid_data())?
        != claim.task_id().as_str()
        || u64_from_i64(
            call.try_get("lease_epoch")
                .map_err(|_| RepositoryError::invalid_data())?,
        )? != claim.envelope().lease_epoch().get()
        || call
            .try_get::<String, _>("fencing_token")
            .map_err(|_| RepositoryError::invalid_data())?
            != claim.envelope().fencing_token()
    {
        return Err(RepositoryError::invalid_data());
    }
    if call
        .try_get::<String, _>("call_status")
        .map_err(|_| RepositoryError::invalid_data())?
        != "started"
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    }

    let item_ordinal = i64::from(call_index)
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let item_id = function_call_response_item_id(
        claim.run_id(),
        claim.activation_id(),
        claim.envelope().attempt_no(),
        model_call_no,
        call_index,
        call_id,
        tool_name,
    );
    let incomplete_item = json!({
        "id": item_id,
        "type": "function_call",
        "status": "incomplete",
        "call_id": call_id,
        "name": tool_name,
        "arguments": "",
    });
    validate_incomplete_function_call_item(&incomplete_item, &item_id)?;
    let encoded_incomplete_item = canonical_json(&incomplete_item)?;
    let existing = sqlx::query(
        "SELECT item_id,output_index,node_id,item_kind,item_status,seal_index,safe_item
         FROM response_public_items
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
           AND item_ordinal=?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
    .bind(item_ordinal)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if let Some(existing) = existing {
        if existing
            .try_get::<String, _>("item_id")
            .map_err(|_| RepositoryError::invalid_data())?
            != item_id
            || existing
                .try_get::<String, _>("node_id")
                .map_err(|_| RepositoryError::invalid_data())?
                != claim.envelope().request().node_id().as_str()
            || existing
                .try_get::<String, _>("item_kind")
                .map_err(|_| RepositoryError::invalid_data())?
                != "function_call"
            || existing
                .try_get::<String, _>("item_status")
                .map_err(|_| RepositoryError::invalid_data())?
                != "reserved"
            || existing
                .try_get::<Option<i64>, _>("seal_index")
                .map_err(|_| RepositoryError::invalid_data())?
                .is_some()
            || existing
                .try_get::<Option<String>, _>("safe_item")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_deref()
                != Some(encoded_incomplete_item.as_str())
        {
            return Err(RepositoryError::invalid_data());
        }
        let item = response_item_authority(
            item_id,
            existing
                .try_get("output_index")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::ExactReplay {
            authoritative: item,
        });
    }

    let previous_output_index = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(output_index),-1) FROM response_public_items WHERE run_id=?",
    )
    .bind(claim.run_id().as_str())
    .fetch_one(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let output_index = previous_output_index
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    sqlx::query(
        "INSERT INTO response_public_items (
            run_id,activation_id,attempt_no,model_call_no,item_ordinal,item_id,
            output_index,node_id,item_kind,item_status,seal_index,safe_item,
            created_at,updated_at
         ) VALUES (?,?,?,?,?,?,?,?,'function_call','reserved',NULL,?,
                   CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
    .bind(item_ordinal)
    .bind(&item_id)
    .bind(output_index)
    .bind(claim.envelope().request().node_id().as_str())
    .bind(encoded_incomplete_item)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if sqlx::query_scalar::<_, i64>("SELECT julianday('now') < julianday(?)")
        .bind(now_text(claim.claim_expires_at()))
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        != 1
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StaleLease);
    }
    if sqlx::query_scalar::<_, i64>("SELECT julianday('now') >= julianday(?)")
        .bind(now_text(operation_deadline))
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        == 1
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::OperationDeadlineElapsed);
    }
    let item = response_item_authority(item_id, output_index)?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(SchedulerTaskCommitOutcome::Committed { result: item })
}

async fn checkpoint_model_call_completion_sqlite(
    repository: &SqliteDurableRepository,
    claim: &SchedulerTaskClaim,
    completion: &ModelCallCompletion,
) -> Result<SchedulerTaskCommitOutcome<()>, RepositoryError> {
    if completion.finish_reason() == insight_engine::worker::ModelFinishReason::ToolCalls {
        // A tool-call finish is only durable together with its exact batch.
        return Err(RepositoryError::invalid_configuration());
    }
    checkpoint_model_call_completion_with_batch_sqlite(repository, claim, completion, None).await
}

async fn checkpoint_model_tool_call_batch_sqlite(
    repository: &SqliteDurableRepository,
    claim: &SchedulerTaskClaim,
    checkpoint: &ModelToolCallCheckpoint,
) -> Result<SchedulerTaskCommitOutcome<()>, RepositoryError> {
    checkpoint_model_call_completion_with_batch_sqlite(
        repository,
        claim,
        checkpoint.completion(),
        Some(checkpoint.batch()),
    )
    .await
}

async fn checkpoint_model_call_completion_with_batch_sqlite(
    repository: &SqliteDurableRepository,
    claim: &SchedulerTaskClaim,
    completion: &ModelCallCompletion,
    batch: Option<&ModelToolCallBatch>,
) -> Result<SchedulerTaskCommitOutcome<()>, RepositoryError> {
    if claim.mode() != SchedulerTaskClaimMode::Execute
        || claim.envelope().request().task_kind() != insight_engine::SchedulerTaskKind::Llm
        || completion.model_call_no() == 0
        || (batch.is_none()
            && completion.finish_reason() == insight_engine::worker::ModelFinishReason::ToolCalls)
        || batch.is_some_and(|batch| {
            completion.finish_reason() != insight_engine::worker::ModelFinishReason::ToolCalls
                || batch.model_call_no() != completion.model_call_no()
                || batch.calls().is_empty()
                || batch
                    .assistant_content()
                    .is_some_and(|content| content.len() > 1_048_576)
        })
    {
        return Err(RepositoryError::invalid_configuration());
    }
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let Some(authority) =
        load_authoritative_task_claim_sqlite(&mut transaction, claim, TaskClaimComparison::Exact)
            .await?
    else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StaleLease);
    };
    if authority.task_state != "claimed" || !authority.is_fresh() || !authority.permits_execution()
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StaleLease);
    }
    if authority.claim.mode() != SchedulerTaskClaimMode::Execute
        || authority.attempt_lifecycle != "running"
        || authority.activation_lifecycle != "running"
        || authority.current_effect_evidence != EffectEvidence::Started
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    }
    let operation_deadline = model_call_operation_deadline(&authority)?;
    if authority.database_now >= operation_deadline {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::OperationDeadlineElapsed);
    }
    let call = sqlx::query(
        "SELECT task_id,lease_epoch,fencing_token,call_status,finish_reason,usage,usage_complete
         FROM model_call_usage
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(completion.model_call_no()))
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(call) = call else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    };
    if call
        .try_get::<String, _>("task_id")
        .map_err(|_| RepositoryError::invalid_data())?
        != claim.task_id().as_str()
        || u64_from_i64(
            call.try_get("lease_epoch")
                .map_err(|_| RepositoryError::invalid_data())?,
        )? != claim.envelope().lease_epoch().get()
        || call
            .try_get::<String, _>("fencing_token")
            .map_err(|_| RepositoryError::invalid_data())?
            != claim.envelope().fencing_token()
    {
        return Err(RepositoryError::invalid_data());
    }
    let item = sqlx::query(
        "SELECT item_id,item_status,seal_index,safe_item FROM response_public_items
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
           AND item_ordinal=0",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(completion.model_call_no()))
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let expected_item_id = item
        .as_ref()
        .map(|item| {
            item.try_get::<String, _>("item_id")
                .map_err(|_| RepositoryError::invalid_data())
        })
        .transpose()?;
    let mut prepared = prepare_model_call_completion(
        completion,
        expected_item_id.as_deref(),
        claim.run_id(),
        claim.activation_id(),
        claim.envelope().attempt_no(),
    )?;
    let encoded_usage = prepared.usage.as_ref().map(canonical_json).transpose()?;
    let encoded_safe_item = prepared
        .safe_item
        .as_ref()
        .map(canonical_json)
        .transpose()?;
    let expected_item_status = if prepared.safe_item.is_some() {
        "completed"
    } else if prepared.seal_index.is_some() {
        "incomplete"
    } else {
        "incomplete_unsealed"
    };
    let prepared_function_items = match batch {
        Some(batch) => prepare_model_function_call_publications(
            claim.run_id(),
            claim.activation_id(),
            claim.envelope().attempt_no(),
            batch,
            claim.envelope().request().deployment_binding(),
            frozen_model_publish(claim)?,
        )?,
        None => std::mem::take(&mut prepared.function_items),
    };
    let call_status: String = call
        .try_get("call_status")
        .map_err(|_| RepositoryError::invalid_data())?;
    if call_status != "started" {
        let batch_matches = if let Some(batch) = batch {
            let stored = sqlx::query(
                "SELECT batch_status,assistant_content FROM model_tool_call_batches
                 WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?",
            )
            .bind(claim.run_id().as_str())
            .bind(claim.activation_id().as_str())
            .bind(i64::from(claim.envelope().attempt_no().get()))
            .bind(i64::from(completion.model_call_no()))
            .fetch_optional(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            match stored {
                None => false,
                Some(stored) => {
                    if stored
                        .try_get::<String, _>("batch_status")
                        .map_err(|_| RepositoryError::invalid_data())?
                        != "checkpointed"
                        || stored
                            .try_get::<Option<String>, _>("assistant_content")
                            .map_err(|_| RepositoryError::invalid_data())?
                            .as_deref()
                            != batch.assistant_content()
                    {
                        false
                    } else {
                        let rows = sqlx::query(
                            "SELECT call_index,call_id,tool_name,arguments
                             FROM model_tool_calls
                             WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
                             ORDER BY call_index",
                        )
                        .bind(claim.run_id().as_str())
                        .bind(claim.activation_id().as_str())
                        .bind(i64::from(claim.envelope().attempt_no().get()))
                        .bind(i64::from(completion.model_call_no()))
                        .fetch_all(&mut *transaction)
                        .await
                        .map_err(RepositoryError::storage)?;
                        rows.len() == batch.calls().len()
                            && rows.iter().zip(batch.calls()).all(|(row, expected)| {
                                row.try_get::<i64, _>("call_index").ok()
                                    == Some(i64::from(expected.index()))
                                    && row.try_get::<String, _>("call_id").ok().as_deref()
                                        == Some(expected.call_id())
                                    && row.try_get::<String, _>("tool_name").ok().as_deref()
                                        == Some(expected.name())
                                    && row.try_get::<String, _>("arguments").ok().as_deref()
                                        == canonical_json(expected.arguments()).ok().as_deref()
                            })
                    }
                }
            }
        } else {
            true
        };
        let call_matches = call_status == prepared.call_status
            && call
                .try_get::<Option<String>, _>("finish_reason")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_deref()
                == Some(prepared.finish_reason)
            && call
                .try_get::<Option<String>, _>("usage")
                .map_err(|_| RepositoryError::invalid_data())?
                == encoded_usage
            && (call
                .try_get::<i64, _>("usage_complete")
                .map_err(|_| RepositoryError::invalid_data())?
                == 1)
                == prepared.usage_complete;
        let item_matches = match item.as_ref() {
            Some(item) => {
                item.try_get::<String, _>("item_status")
                    .map_err(|_| RepositoryError::invalid_data())?
                    == expected_item_status
                    && item
                        .try_get::<Option<i64>, _>("seal_index")
                        .map_err(|_| RepositoryError::invalid_data())?
                        == prepared.seal_index.map(i64_from_u64).transpose()?
                    && item
                        .try_get::<Option<String>, _>("safe_item")
                        .map_err(|_| RepositoryError::invalid_data())?
                        == encoded_safe_item
            }
            None => prepared.seal_index.is_none() && prepared.safe_item.is_none(),
        };
        let function_rows = sqlx::query(
            "SELECT item_ordinal,item_id,output_index,item_status,seal_index,safe_item
             FROM response_public_items
             WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
               AND item_ordinal>0 ORDER BY item_ordinal",
        )
        .bind(claim.run_id().as_str())
        .bind(claim.activation_id().as_str())
        .bind(i64::from(claim.envelope().attempt_no().get()))
        .bind(i64::from(completion.model_call_no()))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let function_items_match = function_rows.len() == prepared_function_items.len()
            && function_rows
                .iter()
                .zip(&prepared_function_items)
                .all(|(row, expected)| {
                    row.try_get::<i64, _>("item_ordinal").ok()
                        == Some(i64::from(expected.call_index) + 1)
                        && row.try_get::<String, _>("item_id").ok().as_deref()
                            == Some(expected.item.item_id())
                        && row.try_get::<i64, _>("output_index").ok()
                            == Some(i64::from(expected.item.output_index()))
                        && row.try_get::<String, _>("item_status").ok().as_deref()
                            == Some(expected.terminal_item_status)
                        && row.try_get::<Option<i64>, _>("seal_index").ok().flatten()
                            == i64::try_from(expected.seal_index).ok()
                        && row.try_get::<Option<String>, _>("safe_item").ok().flatten()
                            == canonical_json(&expected.terminal_safe_item).ok()
                });
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        return if call_matches && item_matches && batch_matches && function_items_match {
            Ok(SchedulerTaskCommitOutcome::ExactReplay { authoritative: () })
        } else {
            Ok(SchedulerTaskCommitOutcome::StateConflict)
        };
    }
    if call
        .try_get::<Option<String>, _>("finish_reason")
        .map_err(|_| RepositoryError::invalid_data())?
        .is_some()
        || call
            .try_get::<Option<String>, _>("usage")
            .map_err(|_| RepositoryError::invalid_data())?
            .is_some()
        || call
            .try_get::<i64, _>("usage_complete")
            .map_err(|_| RepositoryError::invalid_data())?
            != 0
    {
        return Err(RepositoryError::invalid_data());
    }
    if batch.is_some()
        && sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                SELECT 1 FROM model_tool_call_batches
                WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
            )",
        )
        .bind(claim.run_id().as_str())
        .bind(claim.activation_id().as_str())
        .bind(i64::from(claim.envelope().attempt_no().get()))
        .bind(i64::from(completion.model_call_no()))
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
            == 1
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    }
    let call_rows = sqlx::query(
        "UPDATE model_call_usage SET call_status=?,finish_reason=?,usage=?,usage_complete=?,
             updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
           AND task_id=? AND lease_epoch=? AND fencing_token=? AND call_status='started'",
    )
    .bind(prepared.call_status)
    .bind(prepared.finish_reason)
    .bind(&encoded_usage)
    .bind(if prepared.usage_complete {
        1_i64
    } else {
        0_i64
    })
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(completion.model_call_no()))
    .bind(claim.task_id().as_str())
    .bind(i64_from_u64(claim.envelope().lease_epoch().get())?)
    .bind(claim.envelope().fencing_token())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if call_rows != 1 {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StateConflict);
    }
    if let Some(batch) = batch {
        let batch_rows = sqlx::query(
            "INSERT INTO model_tool_call_batches (
                run_id,activation_id,attempt_no,model_call_no,batch_status,assistant_content
             ) VALUES (?,?,?,?, 'checkpointed',?)",
        )
        .bind(claim.run_id().as_str())
        .bind(claim.activation_id().as_str())
        .bind(i64::from(claim.envelope().attempt_no().get()))
        .bind(i64::from(completion.model_call_no()))
        .bind(batch.assistant_content())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if batch_rows != 1 {
            return Err(RepositoryError::invalid_data());
        }
        for tool_call in batch.calls() {
            let arguments = canonical_json(tool_call.arguments())?;
            let call_rows = sqlx::query(
                "INSERT INTO model_tool_calls (
                    run_id,activation_id,attempt_no,model_call_no,call_index,call_id,tool_name,
                    arguments,call_status
                 ) VALUES (?,?,?,?,?,?,?,?,'pending')",
            )
            .bind(claim.run_id().as_str())
            .bind(claim.activation_id().as_str())
            .bind(i64::from(claim.envelope().attempt_no().get()))
            .bind(i64::from(completion.model_call_no()))
            .bind(i64::from(tool_call.index()))
            .bind(tool_call.call_id())
            .bind(tool_call.name())
            .bind(arguments)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if call_rows != 1 {
                return Err(RepositoryError::invalid_data());
            }
        }
    }
    if let Some(item) = item {
        if item
            .try_get::<String, _>("item_status")
            .map_err(|_| RepositoryError::invalid_data())?
            != "reserved"
        {
            return Err(RepositoryError::invalid_data());
        }
        let item_rows = sqlx::query(
            "UPDATE response_public_items SET item_status=?,seal_index=?,safe_item=?,
                 updated_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
               AND item_ordinal=0 AND item_status='reserved'",
        )
        .bind(expected_item_status)
        .bind(prepared.seal_index.map(i64_from_u64).transpose()?)
        .bind(&encoded_safe_item)
        .bind(claim.run_id().as_str())
        .bind(claim.activation_id().as_str())
        .bind(i64::from(claim.envelope().attempt_no().get()))
        .bind(i64::from(completion.model_call_no()))
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if item_rows != 1 {
            return Err(RepositoryError::invalid_data());
        }
    }
    {
        let existing_function_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM response_public_items
             WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
               AND item_ordinal>0",
        )
        .bind(claim.run_id().as_str())
        .bind(claim.activation_id().as_str())
        .bind(i64::from(claim.envelope().attempt_no().get()))
        .bind(i64::from(completion.model_call_no()))
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if usize::try_from(existing_function_count).ok() != Some(prepared_function_items.len())
            || batch.is_some_and(|batch| {
                batch.public_function_calls().len() != prepared_function_items.len()
            })
        {
            return Err(RepositoryError::invalid_data());
        }
        for expected in &prepared_function_items {
            let ordinal = i64::from(expected.call_index) + 1;
            let row = sqlx::query(
                "SELECT item_id,output_index,item_status,seal_index,safe_item
                 FROM response_public_items
                 WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
                   AND item_ordinal=?",
            )
            .bind(claim.run_id().as_str())
            .bind(claim.activation_id().as_str())
            .bind(i64::from(claim.envelope().attempt_no().get()))
            .bind(i64::from(completion.model_call_no()))
            .bind(ordinal)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(RepositoryError::invalid_data)?;
            if row.try_get::<String, _>("item_id").ok().as_deref() != Some(expected.item.item_id())
                || row.try_get::<i64, _>("output_index").ok()
                    != Some(i64::from(expected.item.output_index()))
                || row.try_get::<String, _>("item_status").ok().as_deref() != Some("reserved")
                || row
                    .try_get::<Option<i64>, _>("seal_index")
                    .ok()
                    .flatten()
                    .is_some()
                || row.try_get::<Option<String>, _>("safe_item").ok().flatten()
                    != Some(canonical_json(&expected.reserved_safe_item)?)
            {
                return Err(RepositoryError::invalid_data());
            }
            let updated = sqlx::query(
                "UPDATE response_public_items
                 SET item_status=?,seal_index=?,safe_item=?,updated_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
                   AND item_ordinal=? AND item_status='reserved'",
            )
            .bind(expected.terminal_item_status)
            .bind(i64_from_u64(expected.seal_index)?)
            .bind(canonical_json(&expected.terminal_safe_item)?)
            .bind(claim.run_id().as_str())
            .bind(claim.activation_id().as_str())
            .bind(i64::from(claim.envelope().attempt_no().get()))
            .bind(i64::from(completion.model_call_no()))
            .bind(ordinal)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if updated != 1 {
                return Err(RepositoryError::invalid_data());
            }
        }
    }
    if sqlx::query_scalar::<_, i64>("SELECT julianday('now') < julianday(?)")
        .bind(now_text(claim.claim_expires_at()))
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        != 1
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::StaleLease);
    }
    if sqlx::query_scalar::<_, i64>("SELECT julianday('now') >= julianday(?)")
        .bind(now_text(operation_deadline))
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        == 1
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(SchedulerTaskCommitOutcome::OperationDeadlineElapsed);
    }
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(SchedulerTaskCommitOutcome::Committed { result: () })
}

#[async_trait]
impl SchedulerDurableRepository for SqliteDurableRepository {
    async fn load_scheduler_facts(
        &self,
        run_id: &RunId,
    ) -> Result<SchedulerFacts, RepositoryError> {
        load_facts_sqlite(self, run_id).await
    }

    async fn commit_scheduler_action(
        &self,
        fence: &FencedSchedulerRunCommand,
        action: &PlannedSchedulerAction,
    ) -> Result<TransitionOutcome<SchedulerCommitReceipt>, RepositoryError> {
        validate_subflow_admission(self, fence, action).await?;
        if action.intent().run_id() != fence.run_id()
            || canonical_intent_hash(action.intent())? != *action.intent_hash()
        {
            return Err(RepositoryError::invalid_data());
        }
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        match load_replay(
            &mut transaction,
            action.intent().run_id(),
            action.transition_key(),
            action.intent_hash().as_str(),
        )
        .await?
        {
            Replay::Exact(replay) => {
                let receipt = exact_scheduler_action(&mut transaction, action, &replay).await?;
                transaction
                    .commit()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::ExactReplay {
                    authoritative: receipt,
                });
            }
            Replay::Vacant => {}
        }
        let expected = action.precondition().expected_projection_version();
        if !validate_scheduler_fence(&mut transaction, fence, expected).await? {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let fence_expires_at = sqlx::query_scalar::<_, String>(
            "SELECT scheduler_lease_expires_at FROM workflow_runs WHERE run_id=?",
        )
        .bind(fence.run_id().as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .ok_or_else(RepositoryError::invalid_data)?;
        let next = expected
            .checked_add(1)
            .ok_or_else(RepositoryError::invalid_data)?;
        let seq = allocate_event_seq(&mut transaction, action.intent().run_id()).await?;
        let event_id_value = event_id(action.transition_key());
        let event = event_for_action(&mut transaction, action).await?;
        if matches!(
            action.intent().action(),
            SchedulerAction::AdmitActivation { .. }
        ) {
            // AdmitActivation creates the event's activation subject.
            // SQLite's execution-event FK is immediate, so materialize that
            // projection first; the transaction still makes both atomic.
            apply_scheduler_action(&mut transaction, action, &event_id_value, seq, next).await?;
            insert_event(
                &mut transaction,
                action.intent().run_id(),
                seq,
                &event_id_value,
                action.transition_key(),
                action.intent_hash().as_str(),
                next,
                &event,
            )
            .await?;
        } else {
            insert_event(
                &mut transaction,
                action.intent().run_id(),
                seq,
                &event_id_value,
                action.transition_key(),
                action.intent_hash().as_str(),
                next,
                &event,
            )
            .await?;
            apply_scheduler_action(&mut transaction, action, &event_id_value, seq, next).await?;
        }
        insert_scheduler_checkpoint(&mut transaction, action, &event_id_value, next).await?;
        finalize_projection_checkpoints(
            &mut transaction,
            action.intent().run_id(),
            &event_id_value,
        )
        .await?;
        if sqlx::query_scalar::<_, i64>("SELECT julianday('now') < julianday(?)")
            .bind(&fence_expires_at)
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            != 1
        {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed {
            result: SchedulerCommitReceipt::new(
                seq,
                event_id_value,
                action.intent().checkpoint_id().clone(),
                next,
            ),
        })
    }

    async fn claim_scheduler_tasks(
        &self,
        claimed_by: &str,
        claim_seconds: u32,
        limit: u32,
    ) -> Result<Vec<SchedulerTaskClaim>, RepositoryError> {
        claim_tasks_sqlite(self, claimed_by, claim_seconds, limit, MAX_CLAIM_LIMIT).await
    }

    async fn claim_scheduler_tasks_with_run_limit(
        &self,
        claimed_by: &str,
        claim_seconds: u32,
        limit: u32,
        max_claimed_per_run: u32,
    ) -> Result<Vec<SchedulerTaskClaim>, RepositoryError> {
        claim_tasks_sqlite(self, claimed_by, claim_seconds, limit, max_claimed_per_run).await
    }

    async fn mark_scheduler_task_started(
        &self,
        claim: &SchedulerTaskClaim,
    ) -> Result<TransitionOutcome<SchedulerCommitReceipt>, RepositoryError> {
        start_task_sqlite(self, claim).await
    }

    async fn reserve_model_call(
        &self,
        claim: &SchedulerTaskClaim,
        model_call_no: u32,
        publish: bool,
    ) -> Result<SchedulerTaskCommitOutcome<ModelCallAuthority>, RepositoryError> {
        reserve_model_call_sqlite(self, claim, model_call_no, publish).await
    }

    async fn reserve_model_call_public_item(
        &self,
        claim: &SchedulerTaskClaim,
        model_call_no: u32,
    ) -> Result<SchedulerTaskCommitOutcome<ResponseItemAuthority>, RepositoryError> {
        reserve_model_call_public_item_sqlite(self, claim, model_call_no).await
    }

    async fn reserve_model_call_public_function_item(
        &self,
        claim: &SchedulerTaskClaim,
        model_call_no: u32,
        call_index: u32,
        call_id: &str,
        tool_name: &str,
    ) -> Result<SchedulerTaskCommitOutcome<ResponseItemAuthority>, RepositoryError> {
        reserve_model_call_public_function_item_sqlite(
            self,
            claim,
            model_call_no,
            call_index,
            call_id,
            tool_name,
        )
        .await
    }

    async fn checkpoint_model_call_completion(
        &self,
        claim: &SchedulerTaskClaim,
        completion: &ModelCallCompletion,
    ) -> Result<SchedulerTaskCommitOutcome<()>, RepositoryError> {
        checkpoint_model_call_completion_sqlite(self, claim, completion).await
    }

    async fn checkpoint_model_tool_call_batch(
        &self,
        claim: &SchedulerTaskClaim,
        checkpoint: &ModelToolCallCheckpoint,
    ) -> Result<SchedulerTaskCommitOutcome<()>, RepositoryError> {
        checkpoint_model_tool_call_batch_sqlite(self, claim, checkpoint).await
    }

    async fn load_model_tool_parent_resume(
        &self,
        claim: &SchedulerTaskClaim,
    ) -> Result<SchedulerTaskCommitOutcome<Option<ModelToolParentResume>>, RepositoryError> {
        load_model_tool_parent_resume_sqlite(self, claim).await
    }

    async fn activate_model_tool_call_batch(
        &self,
        parent_claim: &SchedulerTaskClaim,
        model_call_no: u32,
    ) -> Result<ModelToolBatchActivationOutcome, RepositoryError> {
        activate_model_tool_call_batch_sqlite(self, parent_claim, model_call_no).await
    }

    async fn claim_model_tool_calls(
        &self,
        claimed_by: &str,
        claim_seconds: u32,
        limit: u32,
        max_claimed_per_run: u32,
    ) -> Result<Vec<ModelToolTaskClaim>, RepositoryError> {
        claim_model_tool_calls_sqlite(self, claimed_by, claim_seconds, limit, max_claimed_per_run)
            .await
    }

    async fn mark_model_tool_call_started(
        &self,
        claim: &ModelToolTaskClaim,
    ) -> Result<ModelToolTaskTransitionOutcome<()>, RepositoryError> {
        mark_model_tool_call_started_sqlite(self, claim).await
    }

    async fn heartbeat_model_tool_call(
        &self,
        claim: &ModelToolTaskClaim,
        claim_seconds: u32,
    ) -> Result<ModelToolTaskHeartbeatOutcome, RepositoryError> {
        heartbeat_model_tool_call_sqlite(self, claim, claim_seconds).await
    }

    async fn commit_model_tool_call_outcome(
        &self,
        claim: &ModelToolTaskClaim,
        outcome: &ModelToolTaskOutcome,
    ) -> Result<ModelToolTaskTransitionOutcome<ModelToolTaskCommitReceipt>, RepositoryError> {
        commit_model_tool_call_outcome_sqlite(self, claim, outcome).await
    }

    async fn heartbeat_scheduler_task(
        &self,
        claim: &SchedulerTaskClaim,
        claim_seconds: u32,
    ) -> Result<SchedulerTaskHeartbeatOutcome, RepositoryError> {
        heartbeat_task_sqlite(self, claim, claim_seconds).await
    }

    async fn commit_scheduler_task_outcome(
        &self,
        claim: &SchedulerTaskClaim,
        outcome: &SchedulerTaskOutcome,
    ) -> Result<SchedulerTaskCommitOutcome<SchedulerTaskCompletionReceipt>, RepositoryError> {
        commit_task_outcome_sqlite(self, claim, outcome).await
    }

    async fn acknowledge_scheduler_task(
        &self,
        claim: &SchedulerTaskClaim,
    ) -> Result<bool, RepositoryError> {
        acknowledge_task_sqlite(self, claim).await
    }

    async fn load_scheduler_value(
        &self,
        run_id: &RunId,
        port_id: &DataPortId,
    ) -> Result<Option<SchedulerStoredValue>, RepositoryError> {
        let _guard = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let row = sqlx::query(
            "SELECT port_id,owner_activation_id,runtime_value,value_ref,declared_type,storage_kind,
                    payload_id,artifact_id,content_hash,projection_version
             FROM scheduler_values WHERE run_id=? AND port_id=?",
        )
        .bind(run_id.as_str())
        .bind(port_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let stored = row
            .as_ref()
            .map(|row| stored_value_from_row(run_id, row))
            .transpose()?;
        if let Some(stored) = stored.as_ref() {
            validate_value_ref_resource_sqlite(&mut transaction, run_id, stored.value_ref())
                .await?;
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(stored)
    }

    async fn list_recoverable_scheduler_runs(
        &self,
        limit: u32,
    ) -> Result<Vec<RunId>, RepositoryError> {
        if limit == 0 || limit > MAX_CLAIM_LIMIT {
            return Err(RepositoryError::invalid_configuration());
        }
        sqlx::query_scalar::<_, String>(
            "SELECT run_id FROM workflow_runs
             WHERE lifecycle='terminating'
                OR (lifecycle IN ('created','active','waiting') AND admission_state='open')
             ORDER BY updated_at,run_id LIMIT ?",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .into_iter()
        .map(|run_id| model_data(RunId::new(run_id)))
        .collect()
    }
}

#[cfg(test)]
mod integrity_tests {
    use super::*;
    use insight_engine::worker::WorkerFailure;
    use insight_engine::{ArtifactId, EffectIdempotency, WorkerFailureClass};

    use super::super::scheduler_repository::SchedulerTaskFailure;
    use insight_durable::scheduler_repository::adapter::{
        scheduler_success_validation_fixture, scheduler_task_failure_unchecked,
        scheduler_task_success_unchecked, scheduler_validation_fixture,
    };

    fn fixture_claim_expires_at() -> DateTime<Utc> {
        Utc::now() + chrono::Duration::minutes(1)
    }

    #[test]
    fn sqlite_validate_success_rejects_forged_value_reference_hash() {
        let (claim, port_id, result) =
            scheduler_success_validation_fixture(fixture_claim_expires_at());
        assert!(validate_success(
            &claim,
            &SchedulerTaskSuccess::inline(result.clone()).unwrap()
        )
        .is_ok());

        let runtime_value = result.outputs().get(&port_id).unwrap();
        let canonical = serde_jcs::to_vec(runtime_value.value()).unwrap();
        let valid_artifact = ArtifactRef::new(
            ArtifactId::new("artifact_sqlite_valid_hash").unwrap(),
            ContentHash::from_bytes(&canonical),
            u64::try_from(canonical.len()).unwrap(),
            Some("application/json".to_owned()),
        )
        .unwrap();
        assert!(validate_success(
            &claim,
            &SchedulerTaskSuccess::with_value_refs(
                result.clone(),
                BTreeMap::from([(port_id.clone(), ValueRef::Artifact(valid_artifact))]),
            )
            .unwrap(),
        )
        .is_ok());

        let forged = scheduler_task_success_unchecked(
            result,
            BTreeMap::from([(
                port_id,
                ValueRef::Artifact(
                    ArtifactRef::new(
                        ArtifactId::new("artifact_sqlite_wrong_hash").unwrap(),
                        ContentHash::from_bytes(b"value-b"),
                        7,
                        Some("application/json".to_owned()),
                    )
                    .unwrap(),
                ),
            )]),
        );
        assert!(validate_success(&claim, &forged).is_err());
    }

    #[test]
    fn sqlite_outcome_boundary_rejects_forged_retry_policy() {
        let (claim, _, _) = scheduler_validation_fixture(
            EffectIdempotency::NonIdempotent,
            2,
            fixture_claim_expires_at(),
        );
        let retry_at = Utc::now() + chrono::Duration::seconds(1);
        let worker_failure = WorkerFailure::new(
            WorkerFailureClass::InfrastructureFailure,
            "RETRYABLE_FAILURE",
            true,
        )
        .unwrap();
        let valid = SchedulerTaskFailure::from_worker_failure(
            &claim,
            &worker_failure,
            EffectEvidence::NotStarted,
            SchedulerFailureDisposition::Retry {
                retry_at,
                remaining_attempts: 1,
            },
        )
        .unwrap();
        assert!(validate_task_outcome(
            &claim,
            EffectEvidence::NotStarted,
            &SchedulerTaskOutcome::Failed(valid)
        )
        .is_ok());

        for forged in [
            scheduler_task_failure_unchecked(
                &claim,
                WorkerFailureClass::InfrastructureFailure,
                "RETRYABLE_FAILURE",
                true,
                EffectEvidence::Started,
                SchedulerFailureDisposition::Retry {
                    retry_at,
                    remaining_attempts: 1,
                },
            ),
            scheduler_task_failure_unchecked(
                &claim,
                WorkerFailureClass::EffectOutcomeUnknown,
                "UNKNOWN_EFFECT",
                true,
                EffectEvidence::Unknown,
                SchedulerFailureDisposition::Retry {
                    retry_at,
                    remaining_attempts: 1,
                },
            ),
            scheduler_task_failure_unchecked(
                &claim,
                WorkerFailureClass::ControlTermination,
                "CONTROL_RETRY",
                true,
                EffectEvidence::NotStarted,
                SchedulerFailureDisposition::Retry {
                    retry_at,
                    remaining_attempts: 1,
                },
            ),
            scheduler_task_failure_unchecked(
                &claim,
                WorkerFailureClass::InfrastructureFailure,
                "WRONG_REMAINING",
                true,
                EffectEvidence::NotStarted,
                SchedulerFailureDisposition::Retry {
                    retry_at,
                    remaining_attempts: 99,
                },
            ),
        ] {
            assert!(validate_task_outcome(
                &claim,
                EffectEvidence::NotStarted,
                &SchedulerTaskOutcome::Failed(forged)
            )
            .is_err());
        }

        let downgrade = SchedulerTaskOutcome::Failed(
            SchedulerTaskFailure::from_worker_failure(
                &claim,
                &WorkerFailure::new(
                    WorkerFailureClass::InfrastructureFailure,
                    "FORGED_EVIDENCE_DOWNGRADE",
                    false,
                )
                .unwrap(),
                EffectEvidence::NotStarted,
                SchedulerFailureDisposition::Terminal,
            )
            .unwrap(),
        );
        for current in [EffectEvidence::Started, EffectEvidence::Unknown] {
            assert!(validate_task_outcome(&claim, current, &downgrade).is_err());
        }

        let finalize_claim = SchedulerTaskClaim::new(
            claim.envelope().clone(),
            claim.claimed_by().to_owned(),
            claim.claim_token().to_owned(),
            claim.claim_expires_at(),
            claim.task_projection_version(),
            SchedulerTaskClaimMode::FinalizeLeaseLoss,
        )
        .with_lease_loss_evidence(EffectEvidence::Unknown);
        let lease_loss = WorkerFailure::new(
            WorkerFailureClass::EffectOutcomeUnknown,
            "WORKER_LEASE_LOST",
            true,
        )
        .unwrap();
        let valid_finalize = SchedulerTaskOutcome::Failed(
            SchedulerTaskFailure::from_worker_failure(
                &finalize_claim,
                &lease_loss,
                EffectEvidence::Unknown,
                SchedulerFailureDisposition::Terminal,
            )
            .unwrap(),
        );
        assert!(
            validate_task_outcome(&finalize_claim, EffectEvidence::Started, &valid_finalize)
                .is_ok()
        );
        let mismatched_finalize = SchedulerTaskOutcome::Failed(scheduler_task_failure_unchecked(
            &finalize_claim,
            WorkerFailureClass::InfrastructureFailure,
            "WORKER_LEASE_LOST",
            false,
            EffectEvidence::Started,
            SchedulerFailureDisposition::Terminal,
        ));
        assert!(validate_task_outcome(
            &finalize_claim,
            EffectEvidence::Started,
            &mismatched_finalize
        )
        .is_err());
    }
}
