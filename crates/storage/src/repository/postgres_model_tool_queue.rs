use super::RepositoryErrorExt as _;

use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};
use sqlx::{postgres::PgRow, Postgres, Row, Transaction};
use uuid::Uuid;

use insight_durable::common::adapter::{
    canonical_json, function_call_response_item_id, i64_from_u64, u64_from_i64,
};
use insight_durable::model_tool_queue::adapter::{
    deterministic_tool_identity, model_tool_batch_activation_new,
    model_tool_continuation_status_as_str, model_tool_continuation_status_parse,
    model_tool_task_claim_new, model_tool_task_commit_receipt_new,
    model_tool_task_disposition_parse, model_tool_task_outcome_canonical_hash,
    model_tool_task_status_parse, parse_action_from_stored_evidence,
    parse_frozen_model_tool_contract, validate_tool_arguments, validate_tool_result,
};

use insight_engine::run_stream::{RunToolPublicProjection, RunToolResult};
use insight_engine::worker::ResponseItemAuthority;
use insight_engine::{
    ActivationId, AttemptNo, ContentHash, EffectEvidence, EffectIdempotency, LeaseEpoch, RunId,
    SchedulerTaskId, SchedulerTaskKind, WorkerCancellation, WorkerEffectPolicy,
};

use super::{
    model_tool_queue::{
        FrozenModelToolAction, ModelToolBatchActivation, ModelToolBatchActivationOutcome,
        ModelToolContinuationStatus, ModelToolFailureClass, ModelToolTaskClaim,
        ModelToolTaskCommitReceipt, ModelToolTaskDisposition, ModelToolTaskHeartbeatOutcome,
        ModelToolTaskIdentity, ModelToolTaskOutcome, ModelToolTaskStatus,
        ModelToolTaskTransitionOutcome,
    },
    postgres::{begin_write_transaction, lock_run_for_event_write, PostgresDurableRepository},
    scheduler_repository::{
        DurableTaskExecutionRequest, SchedulerTaskClaim, SchedulerTaskClaimMode,
    },
    RepositoryError,
};

const MAX_CLAIM_SECONDS: u32 = 3_600;
const MAX_CLAIM_LIMIT: u32 = 1_000;
const MODEL_TOOL_PARENT_DEADLINE_EXCEEDED: &str = "MODEL_TOOL_PARENT_DEADLINE_EXCEEDED";

fn valid_claim_parameters(
    claimed_by: &str,
    claim_seconds: u32,
    limit: u32,
    max_claimed_per_run: u32,
) -> bool {
    !claimed_by.is_empty()
        && claimed_by.len() <= 256
        && !claimed_by
            .chars()
            .any(|value| value.is_control() || value.is_whitespace())
        && (3..=MAX_CLAIM_SECONDS).contains(&claim_seconds)
        && (1..=MAX_CLAIM_LIMIT).contains(&limit)
        && (1..=MAX_CLAIM_LIMIT).contains(&max_claimed_per_run)
}

fn effect_evidence_str(value: EffectEvidence) -> &'static str {
    match value {
        EffectEvidence::NotStarted => "not_started",
        EffectEvidence::Started => "started",
        EffectEvidence::Committed => "committed",
        EffectEvidence::Unknown => "unknown",
    }
}

fn failure_class_str(value: ModelToolFailureClass) -> &'static str {
    match value {
        ModelToolFailureClass::Safe => "safe",
        ModelToolFailureClass::Infrastructure => "infrastructure",
        ModelToolFailureClass::EffectOutcomeUnknown => "effect_outcome_unknown",
    }
}

fn tool_fencing_token(
    task_id: &SchedulerTaskId,
    attempt_no: AttemptNo,
    lease_epoch: LeaseEpoch,
) -> String {
    let evidence = format!(
        "model_tool_fence.v1\0{}\0{}\0{}",
        task_id.as_str(),
        attempt_no.get(),
        lease_epoch.get()
    );
    let hash = ContentHash::from_bytes(evidence.as_bytes());
    format!(
        "fence_{}",
        hash.as_str()
            .strip_prefix("sha256:")
            .expect("content hashes have a stable prefix")
    )
}

enum ParentAuthority {
    Exact,
    Stale,
    Conflict,
    Terminal,
}

async fn validate_parent_authority(
    tx: &mut Transaction<'_, Postgres>,
    claim: &SchedulerTaskClaim,
) -> Result<ParentAuthority, RepositoryError> {
    if claim.mode() != SchedulerTaskClaimMode::Execute
        || claim.envelope().request().task_kind() != SchedulerTaskKind::Llm
    {
        return Err(RepositoryError::invalid_configuration());
    }
    let row = sqlx::query(
        "SELECT o.task_state,o.task_envelope,o.claimed_by,o.claim_token,o.claim_expires_at,
                o.projection_version,o.claim_mode,r.lifecycle AS run_lifecycle,
                a.lifecycle AS attempt_lifecycle,a.effect_evidence,a.lease_expires_at,
                v.lifecycle AS activation_lifecycle,
                o.claim_expires_at>clock_timestamp() AS claim_fresh,
                a.lease_expires_at>clock_timestamp() AS lease_fresh
         FROM task_outbox o
         JOIN workflow_runs r ON r.run_id=o.run_id
         JOIN node_attempts a ON a.run_id=o.run_id AND a.activation_id=o.activation_id
           AND a.attempt_no=o.attempt_no AND a.lease_epoch=o.lease_epoch
           AND a.fencing_token=o.fencing_token
         JOIN node_activations v ON v.run_id=o.run_id AND v.activation_id=o.activation_id
         WHERE o.run_id=$1 AND o.task_id=$2 AND o.activation_id=$3 AND o.attempt_no=$4
           AND o.lease_epoch=$5 AND o.fencing_token=$6
         FOR UPDATE OF o,a,v",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.task_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(
        i32::try_from(claim.envelope().attempt_no().get())
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(i64_from_u64(claim.envelope().lease_epoch().get())?)
    .bind(claim.envelope().fencing_token())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(ParentAuthority::Stale);
    };
    let run_lifecycle: String = row
        .try_get("run_lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?;
    if matches!(
        run_lifecycle.as_str(),
        "succeeded" | "failed" | "cancelled" | "interrupted" | "timed_out"
    ) {
        return Ok(ParentAuthority::Terminal);
    }
    let envelope: DurableTaskExecutionRequest = serde_json::from_value(
        row.try_get::<Value, _>("task_envelope")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let exact = envelope == *claim.envelope()
        && row.try_get::<String, _>("claimed_by").ok().as_deref() == Some(claim.claimed_by())
        && row.try_get::<String, _>("claim_token").ok().as_deref() == Some(claim.claim_token())
        && row.try_get::<DateTime<Utc>, _>("claim_expires_at").ok()
            == Some(claim.claim_expires_at())
        && row.try_get::<i64, _>("projection_version").ok()
            == i64::try_from(claim.task_projection_version()).ok()
        && row
            .try_get::<Option<String>, _>("claim_mode")
            .ok()
            .flatten()
            .as_deref()
            == Some("execute");
    if !exact {
        return Ok(ParentAuthority::Stale);
    }
    if !matches!(run_lifecycle.as_str(), "created" | "active" | "waiting")
        || row.try_get::<String, _>("task_state").ok().as_deref() != Some("claimed")
        || row
            .try_get::<String, _>("attempt_lifecycle")
            .ok()
            .as_deref()
            != Some("running")
        || row
            .try_get::<String, _>("activation_lifecycle")
            .ok()
            .as_deref()
            != Some("running")
        || row.try_get::<String, _>("effect_evidence").ok().as_deref() != Some("started")
    {
        return Ok(ParentAuthority::Conflict);
    }
    if row.try_get::<bool, _>("claim_fresh").ok() != Some(true)
        || row.try_get::<bool, _>("lease_fresh").ok() != Some(true)
    {
        return Ok(ParentAuthority::Stale);
    }
    Ok(ParentAuthority::Exact)
}

async fn parent_operation_deadline_postgres(
    tx: &mut Transaction<'_, Postgres>,
    claim: &SchedulerTaskClaim,
) -> Result<DateTime<Utc>, RepositoryError> {
    let started_at = sqlx::query_scalar::<_, DateTime<Utc>>(
        "SELECT started_at FROM node_attempts
         WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3 AND lease_epoch=$4
           AND fencing_token=$5 AND lifecycle='running' AND effect_evidence='started'",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(
        i32::try_from(claim.envelope().attempt_no().get())
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(i64_from_u64(claim.envelope().lease_epoch().get())?)
    .bind(claim.envelope().fencing_token())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let timeout_ms = i64::try_from(claim.envelope().request().effect_policy().timeout_ms())
        .map_err(|_| RepositoryError::invalid_configuration())?;
    started_at
        .checked_add_signed(Duration::milliseconds(timeout_ms))
        .ok_or_else(RepositoryError::invalid_configuration)
}

async fn validate_projected_tool_artifacts_postgres(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    projected: Option<&RunToolResult>,
) -> Result<(), RepositoryError> {
    let Some(projected) = projected else {
        return Ok(());
    };
    for artifact in projected
        .content()
        .iter()
        .filter_map(insight_engine::run_stream::RunToolContent::artifact)
    {
        let exact = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1 FROM artifacts
                WHERE run_id=$1 AND artifact_id=$2 AND content_hash=$3 AND size_bytes=$4
                  AND media_type IS NOT DISTINCT FROM $5 AND artifact_state='referenced'
             )",
        )
        .bind(run_id.as_str())
        .bind(artifact.artifact_id().as_str())
        .bind(artifact.content_hash().as_str())
        .bind(i64_from_u64(artifact.size_bytes())?)
        .bind(artifact.media_type())
        .fetch_one(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
        if !exact {
            return Err(RepositoryError::invalid_data());
        }
    }
    Ok(())
}

fn decode_action(row: &PgRow) -> Result<FrozenModelToolAction, RepositoryError> {
    let effect_policy: WorkerEffectPolicy = serde_json::from_value(
        row.try_get::<Value, _>("action_effect_policy")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    parse_action_from_stored_evidence(
        row.try_get("tool_name")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("action_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("action_version")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("action_descriptor_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("action_input_schema")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("action_output_schema")
            .map_err(|_| RepositoryError::invalid_data())?,
        effect_policy,
        row.try_get("action_deployment_binding")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("effective_public_policy")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
}

fn decode_public_item(row: &PgRow) -> Result<Option<ResponseItemAuthority>, RepositoryError> {
    match (
        row.try_get::<Option<String>, _>("response_item_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get::<Option<i32>, _>("response_output_index")
            .map_err(|_| RepositoryError::invalid_data())?,
    ) {
        (None, None) => Ok(None),
        (Some(item_id), Some(output_index)) => Ok(Some(
            ResponseItemAuthority::new(
                item_id,
                u32::try_from(output_index).map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?,
        )),
        _ => Err(RepositoryError::invalid_data()),
    }
}

fn decode_identity(
    row: &PgRow,
    run_id: &RunId,
    activation_id: &ActivationId,
    attempt_no: AttemptNo,
    model_call_no: u32,
) -> Result<ModelToolTaskIdentity, RepositoryError> {
    let call_index = u32::try_from(
        row.try_get::<i32, _>("call_index")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let call_id: String = row
        .try_get("call_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let public_item = decode_public_item(row)?;
    let public_arguments_jcs = public_item
        .as_ref()
        .map(|_| {
            canonical_json(
                &row.try_get::<Value, _>("arguments")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
        })
        .transpose()?;
    let public_seal_index = row
        .try_get::<Option<i64>, _>("response_seal_index")
        .map_err(|_| RepositoryError::invalid_data())?
        .map(u64_from_i64)
        .transpose()?;
    let identity = deterministic_tool_identity(
        run_id,
        activation_id,
        attempt_no,
        model_call_no,
        call_index,
        &call_id,
        decode_action(row)?,
        public_item,
        public_arguments_jcs,
        public_seal_index,
    )?;
    if row.try_get::<String, _>("tool_task_id").ok().as_deref()
        != Some(identity.tool_task_id().as_str())
        || row.try_get::<String, _>("effect_id").ok().as_deref()
            != Some(identity.effect_id().as_str())
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(identity)
}

async fn load_activation(
    tx: &mut Transaction<'_, Postgres>,
    claim: &SchedulerTaskClaim,
    model_call_no: u32,
) -> Result<ModelToolBatchActivation, RepositoryError> {
    let rows = sqlx::query(
        "SELECT * FROM model_tool_calls
         WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3 AND model_call_no=$4
         ORDER BY call_index",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(
        i32::try_from(claim.envelope().attempt_no().get())
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(i32::try_from(model_call_no).map_err(|_| RepositoryError::invalid_data())?)
    .fetch_all(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let tasks = rows
        .iter()
        .map(|row| {
            decode_identity(
                row,
                claim.run_id(),
                claim.activation_id(),
                claim.envelope().attempt_no(),
                model_call_no,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    model_tool_batch_activation_new(
        claim.run_id().clone(),
        claim.activation_id().clone(),
        claim.envelope().attempt_no(),
        model_call_no,
        tasks,
    )
}

pub(crate) async fn activate_model_tool_call_batch_postgres(
    repository: &PostgresDurableRepository,
    claim: &SchedulerTaskClaim,
    model_call_no: u32,
) -> Result<ModelToolBatchActivationOutcome, RepositoryError> {
    if model_call_no == 0 {
        return Err(RepositoryError::invalid_configuration());
    }
    let mut tx = begin_write_transaction(&repository.pool).await?;
    lock_run_for_event_write(&mut tx, claim.run_id()).await?;
    let parent_attempt = i32::try_from(claim.envelope().attempt_no().get())
        .map_err(|_| RepositoryError::invalid_data())?;
    let call_no = i32::try_from(model_call_no).map_err(|_| RepositoryError::invalid_data())?;
    let batch = sqlx::query(
        "SELECT execution_status,continuation_status,parent_task_id,parent_lease_epoch,
                parent_fencing_token,parent_claimed_by,parent_claim_token,parent_claim_expires_at,
                parent_task_projection_version,parent_operation_deadline
         FROM model_tool_call_batches
         WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3 AND model_call_no=$4
         FOR UPDATE",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(parent_attempt)
    .bind(call_no)
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(batch) = batch else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolBatchActivationOutcome::StateConflict);
    };
    let execution_status: String = batch
        .try_get("execution_status")
        .map_err(|_| RepositoryError::invalid_data())?;
    if execution_status != "checkpointed" {
        let parent_matches = batch
            .try_get::<Option<String>, _>("parent_task_id")
            .ok()
            .flatten()
            .as_deref()
            == Some(claim.task_id().as_str())
            && batch
                .try_get::<Option<i64>, _>("parent_lease_epoch")
                .ok()
                .flatten()
                == Some(i64_from_u64(claim.envelope().lease_epoch().get())?)
            && batch
                .try_get::<Option<String>, _>("parent_fencing_token")
                .ok()
                .flatten()
                .as_deref()
                == Some(claim.envelope().fencing_token())
            && batch
                .try_get::<Option<String>, _>("parent_claim_token")
                .ok()
                .flatten()
                .as_deref()
                == Some(claim.claim_token());
        if !parent_matches {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(ModelToolBatchActivationOutcome::StateConflict);
        }
        expire_parent_operation_deadline_batch_postgres(
            &mut tx,
            claim.run_id().as_str(),
            claim.activation_id().as_str(),
            parent_attempt,
            call_no,
        )
        .await?;
        let activation = load_activation(&mut tx, claim, model_call_no).await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolBatchActivationOutcome::ExactReplay(activation));
    }
    match validate_parent_authority(&mut tx, claim).await? {
        ParentAuthority::Exact => {}
        ParentAuthority::Stale => {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(ModelToolBatchActivationOutcome::StaleParentLease);
        }
        ParentAuthority::Conflict => {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(ModelToolBatchActivationOutcome::StateConflict);
        }
        ParentAuthority::Terminal => {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(ModelToolBatchActivationOutcome::RunTerminal);
        }
    }
    let parent_operation_deadline = parent_operation_deadline_postgres(&mut tx, claim).await?;
    let (max_rounds, max_calls, tools) =
        parse_frozen_model_tool_contract(claim.envelope().request().deployment_binding())?;
    let prior_rounds = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM model_tool_call_batches
         WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3 AND model_call_no<=$4",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(parent_attempt)
    .bind(call_no)
    .fetch_one(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    if u32::try_from(prior_rounds).map_err(|_| RepositoryError::invalid_data())? > max_rounds {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolBatchActivationOutcome::RoundLimitExceeded);
    }
    let total_calls = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM model_tool_calls
         WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(parent_attempt)
    .fetch_one(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    if u32::try_from(total_calls).map_err(|_| RepositoryError::invalid_data())? > max_calls {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolBatchActivationOutcome::CallLimitExceeded);
    }
    let rows = sqlx::query(
        "SELECT call_index,call_id,tool_name,arguments FROM model_tool_calls
         WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3 AND model_call_no=$4
         ORDER BY call_index FOR UPDATE",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(parent_attempt)
    .bind(call_no)
    .fetch_all(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    if rows.is_empty() {
        return Err(RepositoryError::invalid_data());
    }
    for (expected_index, row) in rows.iter().enumerate() {
        let call_index = u32::try_from(
            row.try_get::<i32, _>("call_index")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        if call_index != expected_index as u32 {
            return Err(RepositoryError::invalid_data());
        }
        let tool_name: String = row
            .try_get("tool_name")
            .map_err(|_| RepositoryError::invalid_data())?;
        let action = tools
            .get(&tool_name)
            .cloned()
            .ok_or_else(RepositoryError::invalid_data)?;
        let arguments: Value = row
            .try_get("arguments")
            .map_err(|_| RepositoryError::invalid_data())?;
        validate_tool_arguments(&action, &arguments)?;
        let public_projection =
            RunToolPublicProjection::from_frozen_effective_policy(action.effective_public_policy())
                .map_err(|_| RepositoryError::invalid_data())?;
        let projected_arguments = public_projection
            .project_validated_completed_arguments(&arguments)
            .map_err(|_| RepositoryError::invalid_data())?;
        let public_arguments_jcs = projected_arguments
            .standard_function_call_arguments()
            .map(str::to_owned);
        let call_id: String = row
            .try_get("call_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let provisional = deterministic_tool_identity(
            claim.run_id(),
            claim.activation_id(),
            claim.envelope().attempt_no(),
            model_call_no,
            call_index,
            &call_id,
            action.clone(),
            None,
            None,
            None,
        )?;
        let (public_item, public_seal_index) = if let Some(arguments_jcs) = &public_arguments_jcs {
            let item = sqlx::query(
                "SELECT item_id,output_index,node_id,item_kind,item_status,seal_index,safe_item
                 FROM response_public_items
                 WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3 AND model_call_no=$4
                   AND item_ordinal=$5 FOR UPDATE",
            )
            .bind(claim.run_id().as_str())
            .bind(claim.activation_id().as_str())
            .bind(parent_attempt)
            .bind(call_no)
            .bind(i32::try_from(call_index).map_err(|_| RepositoryError::invalid_data())? + 1)
            .fetch_optional(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(RepositoryError::invalid_data)?;
            let item_id: String = item
                .try_get("item_id")
                .map_err(|_| RepositoryError::invalid_data())?;
            let expected_item_id = function_call_response_item_id(
                claim.run_id(),
                claim.activation_id(),
                claim.envelope().attempt_no(),
                model_call_no,
                call_index,
                &call_id,
                &tool_name,
            );
            let expected_safe_item = json!({
                "id": expected_item_id,
                "type": "function_call",
                "status": "completed",
                "call_id": call_id,
                "name": tool_name,
                "arguments": arguments_jcs,
            });
            let seal_index = item
                .try_get::<Option<i64>, _>("seal_index")
                .map_err(|_| RepositoryError::invalid_data())?
                .map(u64_from_i64)
                .transpose()?
                .filter(|seal| *seal >= 3)
                .ok_or_else(RepositoryError::invalid_data)?;
            if item_id != expected_item_id
                || item
                    .try_get::<String, _>("node_id")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != claim.envelope().request().node_id().as_str()
                || item
                    .try_get::<String, _>("item_kind")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != "function_call"
                || item
                    .try_get::<String, _>("item_status")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != "completed"
                || item
                    .try_get::<Option<Value>, _>("safe_item")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .as_ref()
                    != Some(&expected_safe_item)
            {
                return Err(RepositoryError::invalid_data());
            }
            (
                Some(
                    ResponseItemAuthority::new(
                        item_id,
                        u32::try_from(
                            item.try_get::<i32, _>("output_index")
                                .map_err(|_| RepositoryError::invalid_data())?,
                        )
                        .map_err(|_| RepositoryError::invalid_data())?,
                    )
                    .map_err(|_| RepositoryError::invalid_data())?,
                ),
                Some(seal_index),
            )
        } else {
            (None, None)
        };
        let identity = deterministic_tool_identity(
            claim.run_id(),
            claim.activation_id(),
            claim.envelope().attempt_no(),
            model_call_no,
            call_index,
            &call_id,
            action.clone(),
            public_item.clone(),
            public_arguments_jcs,
            public_seal_index,
        )?;
        if identity.tool_task_id() != provisional.tool_task_id()
            || identity.effect_id() != provisional.effect_id()
        {
            return Err(RepositoryError::invalid_data());
        }
        let policy = action.effect_policy();
        let rows_updated = sqlx::query(
            "UPDATE model_tool_calls SET tool_task_id=$1,effect_id=$2,action_id=$3,action_version=$4,
                action_descriptor_hash=$5,action_input_schema=$6,action_output_schema=$7,
                action_effect_policy=$8,action_deployment_binding=$9,effective_public_policy=$10,
                response_item_id=$11,response_output_index=$12,response_seal_index=$13,
                effect_idempotency=$14,cancellation=$15,
                max_attempts=$16,initial_backoff_ms=$17,max_backoff_ms=$18,timeout_ms=$19,
                tool_attempt_no=1,lease_epoch=1,fencing_token=$20,effect_evidence='not_started',
                available_at=clock_timestamp(),projection_version=1,updated_at=clock_timestamp()
             WHERE run_id=$21 AND activation_id=$22 AND attempt_no=$23 AND model_call_no=$24
               AND call_index=$25 AND call_status='pending' AND tool_task_id IS NULL
               AND projection_version=0",
        )
        .bind(identity.tool_task_id().as_str())
        .bind(identity.effect_id().as_str())
        .bind(action.action_id())
        .bind(action.action_version())
        .bind(action.descriptor_hash())
        .bind(action.input_schema())
        .bind(action.output_schema())
        .bind(serde_json::to_value(policy).map_err(|_| RepositoryError::canonicalization())?)
        .bind(action.deployment_binding())
        .bind(action.effective_public_policy())
        .bind(public_item.as_ref().map(ResponseItemAuthority::item_id))
        .bind(public_item.as_ref().map(|item| i32::try_from(item.output_index())).transpose().map_err(|_| RepositoryError::invalid_data())?)
        .bind(public_seal_index.map(i64_from_u64).transpose()?)
        .bind(match policy.effect_idempotency() {
            EffectIdempotency::Idempotent => "idempotent",
            EffectIdempotency::NonIdempotent => "non_idempotent",
        })
        .bind(match policy.cancellation() {
            WorkerCancellation::Cooperative => "cooperative",
            WorkerCancellation::LeaseOnly => "lease_only",
        })
        .bind(i32::try_from(policy.max_attempts()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(i64_from_u64(policy.initial_backoff_ms())?)
        .bind(i64_from_u64(policy.max_backoff_ms())?)
        .bind(i64_from_u64(policy.timeout_ms())?)
        .bind(tool_fencing_token(identity.tool_task_id(), AttemptNo::FIRST, LeaseEpoch::FIRST))
        .bind(claim.run_id().as_str())
        .bind(claim.activation_id().as_str())
        .bind(parent_attempt)
        .bind(call_no)
        .bind(i32::try_from(call_index).map_err(|_| RepositoryError::invalid_data())?)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows_updated != 1 {
            return Err(RepositoryError::invalid_data());
        }
    }
    let activated = sqlx::query(
        "UPDATE model_tool_call_batches SET execution_status='active',
            continuation_status='waiting_tools',parent_task_id=$1,parent_lease_epoch=$2,
            parent_fencing_token=$3,parent_claimed_by=$4,parent_claim_token=$5,
            parent_claim_expires_at=$6,parent_task_projection_version=$7,
            parent_operation_deadline=$8,activated_at=clock_timestamp(),updated_at=clock_timestamp()
         WHERE run_id=$9 AND activation_id=$10 AND attempt_no=$11 AND model_call_no=$12
           AND execution_status='checkpointed' AND continuation_status='checkpointed'",
    )
    .bind(claim.task_id().as_str())
    .bind(i64_from_u64(claim.envelope().lease_epoch().get())?)
    .bind(claim.envelope().fencing_token())
    .bind(claim.claimed_by())
    .bind(claim.claim_token())
    .bind(claim.claim_expires_at())
    .bind(
        i64::try_from(claim.task_projection_version())
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(parent_operation_deadline)
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(parent_attempt)
    .bind(call_no)
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if activated != 1 {
        return Err(RepositoryError::invalid_data());
    }
    expire_parent_operation_deadline_batch_postgres(
        &mut tx,
        claim.run_id().as_str(),
        claim.activation_id().as_str(),
        parent_attempt,
        call_no,
    )
    .await?;
    let activation = load_activation(&mut tx, claim, model_call_no).await?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(ModelToolBatchActivationOutcome::Activated(activation))
}

fn decode_claim(row: &PgRow) -> Result<ModelToolTaskClaim, RepositoryError> {
    let run_id = RunId::new(
        row.try_get::<String, _>("run_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let activation_id = ActivationId::new(
        row.try_get::<String, _>("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let parent_attempt_no = AttemptNo::new(
        u32::try_from(
            row.try_get::<i32, _>("attempt_no")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let model_call_no = u32::try_from(
        row.try_get::<i32, _>("model_call_no")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let identity = decode_identity(
        row,
        &run_id,
        &activation_id,
        parent_attempt_no,
        model_call_no,
    )?;
    model_tool_task_claim_new(
        run_id,
        activation_id,
        parent_attempt_no,
        model_call_no,
        identity,
        row.try_get("arguments")
            .map_err(|_| RepositoryError::invalid_data())?,
        AttemptNo::new(
            u32::try_from(
                row.try_get::<i32, _>("tool_attempt_no")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        LeaseEpoch::new(u64_from_i64(
            row.try_get("lease_epoch")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?)
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("fencing_token")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("claim_owner")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("claim_token")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("claim_expires_at")
            .map_err(|_| RepositoryError::invalid_data())?,
        u64_from_i64(
            row.try_get("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
    )
}

/// Closes executable model-tool work after the caller has won a global Run
/// terminal transition in the same transaction. It intentionally bypasses the
/// normal barrier wake-up, because a terminal Run cannot resume its parent LLM.
pub(crate) async fn close_model_tool_work_for_terminal_run_postgres(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<(), RepositoryError> {
    let terminal = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
            SELECT 1 FROM workflow_runs
            WHERE run_id=$1 AND lifecycle IN
                ('succeeded','failed','cancelled','interrupted','timed_out')
         )",
    )
    .bind(run_id.as_str())
    .fetch_one(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    if !terminal {
        return Err(RepositoryError::invalid_data());
    }

    sqlx::query(
        "UPDATE model_tool_calls SET call_status='failed',effect_evidence='unknown',
            failure_class='effect_outcome_unknown',
            failure_code='MODEL_TOOL_RUN_TERMINATED_EFFECT_UNKNOWN',failure_retryable=FALSE,
            available_at=NULL,claim_owner=NULL,claim_token=NULL,claim_expires_at=NULL,
            lease_epoch=lease_epoch+1,
            fencing_token=fencing_token || ':run-terminal:' || (projection_version+1)::TEXT,
            completed_at=clock_timestamp(),projection_version=projection_version+1,
            updated_at=clock_timestamp()
         WHERE run_id=$1 AND call_status='running'
           AND EXISTS (
               SELECT 1 FROM model_tool_call_batches b
               WHERE b.run_id=model_tool_calls.run_id
                 AND b.activation_id=model_tool_calls.activation_id
                 AND b.attempt_no=model_tool_calls.attempt_no
                 AND b.model_call_no=model_tool_calls.model_call_no
                 AND b.execution_status='active'
                 AND b.continuation_status='waiting_tools'
           )",
    )
    .bind(run_id.as_str())
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;

    sqlx::query(
        "UPDATE model_tool_calls SET call_status='cancelled',effect_evidence='not_started',
            failure_class='safe',failure_code='MODEL_TOOL_RUN_TERMINATED',failure_retryable=FALSE,
            available_at=NULL,claim_owner=NULL,claim_token=NULL,claim_expires_at=NULL,
            lease_epoch=lease_epoch+1,
            fencing_token=fencing_token || ':run-terminal:' || (projection_version+1)::TEXT,
            completed_at=clock_timestamp(),projection_version=projection_version+1,
            updated_at=clock_timestamp()
         WHERE run_id=$1 AND call_status IN ('pending','claimed')
           AND EXISTS (
               SELECT 1 FROM model_tool_call_batches b
               WHERE b.run_id=model_tool_calls.run_id
                 AND b.activation_id=model_tool_calls.activation_id
                 AND b.attempt_no=model_tool_calls.attempt_no
                 AND b.model_call_no=model_tool_calls.model_call_no
                 AND b.execution_status='active'
                 AND b.continuation_status='waiting_tools'
           )",
    )
    .bind(run_id.as_str())
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;

    sqlx::query(
        "UPDATE model_tool_call_batches
         SET execution_status=CASE WHEN EXISTS (
                 SELECT 1 FROM model_tool_calls c
                 WHERE c.run_id=model_tool_call_batches.run_id
                   AND c.activation_id=model_tool_call_batches.activation_id
                   AND c.attempt_no=model_tool_call_batches.attempt_no
                   AND c.model_call_no=model_tool_call_batches.model_call_no
                   AND c.call_status='failed'
             ) THEN 'failed' ELSE 'cancelled' END,
             continuation_status=CASE WHEN EXISTS (
                 SELECT 1 FROM model_tool_calls c
                 WHERE c.run_id=model_tool_call_batches.run_id
                   AND c.activation_id=model_tool_call_batches.activation_id
                   AND c.attempt_no=model_tool_call_batches.attempt_no
                   AND c.model_call_no=model_tool_call_batches.model_call_no
                   AND c.call_status='failed'
             ) THEN 'ready_failed' ELSE 'ready_cancelled' END,
             completed_at=clock_timestamp(),updated_at=clock_timestamp()
         WHERE run_id=$1 AND execution_status='active'
           AND continuation_status='waiting_tools'",
    )
    .bind(run_id.as_str())
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;

    let live = sqlx::query_scalar::<_, i64>(
        "SELECT
            (SELECT COUNT(*) FROM model_tool_call_batches
             WHERE run_id=$1 AND execution_status='active'
               AND continuation_status='waiting_tools')
          + (SELECT COUNT(*) FROM model_tool_calls c
             JOIN model_tool_call_batches b ON b.run_id=c.run_id
               AND b.activation_id=c.activation_id AND b.attempt_no=c.attempt_no
               AND b.model_call_no=c.model_call_no
             WHERE c.run_id=$1 AND b.execution_status IN ('active','failed','cancelled')
               AND c.call_status IN ('pending','claimed','running'))",
    )
    .bind(run_id.as_str())
    .fetch_one(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    if live != 0 {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn lock_batch(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &str,
    activation_id: &str,
    attempt_no: i32,
    model_call_no: i32,
) -> Result<Option<String>, RepositoryError> {
    sqlx::query_scalar(
        "SELECT continuation_status FROM model_tool_call_batches
         WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3 AND model_call_no=$4
         FOR UPDATE",
    )
    .bind(run_id)
    .bind(activation_id)
    .bind(attempt_no)
    .bind(model_call_no)
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)
}

async fn finalize_batch_barrier_postgres(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &str,
    activation_id: &str,
    attempt_no: i32,
    model_call_no: i32,
) -> Result<ModelToolContinuationStatus, RepositoryError> {
    let mut counts = sqlx::query(
        "SELECT COUNT(*) AS total,
                COUNT(*) FILTER (WHERE call_status='succeeded') AS succeeded,
                COUNT(*) FILTER (WHERE call_status='failed') AS failed,
                COUNT(*) FILTER (WHERE call_status='cancelled') AS cancelled,
                COUNT(*) FILTER (WHERE failure_class='effect_outcome_unknown') AS unknown
         FROM model_tool_calls
         WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3 AND model_call_no=$4",
    )
    .bind(run_id)
    .bind(activation_id)
    .bind(attempt_no)
    .bind(model_call_no)
    .fetch_one(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let mut failed: i64 = counts
        .try_get("failed")
        .map_err(|_| RepositoryError::invalid_data())?;
    let mut cancelled: i64 = counts
        .try_get("cancelled")
        .map_err(|_| RepositoryError::invalid_data())?;
    let mut unknown: i64 = counts
        .try_get("unknown")
        .map_err(|_| RepositoryError::invalid_data())?;
    if failed > 0 || cancelled > 0 {
        sqlx::query(
            "UPDATE model_tool_calls SET call_status='failed',effect_evidence='unknown',
                failure_class='effect_outcome_unknown',
                failure_code='MODEL_TOOL_SIBLING_EFFECT_UNKNOWN',failure_retryable=FALSE,
                lease_epoch=lease_epoch+1,
                fencing_token=fencing_token || ':batch-abort:' || (projection_version+1)::TEXT,
                completed_at=clock_timestamp(),projection_version=projection_version+1,
                updated_at=clock_timestamp()
             WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3 AND model_call_no=$4
               AND call_status='running'",
        )
        .bind(run_id)
        .bind(activation_id)
        .bind(attempt_no)
        .bind(model_call_no)
        .execute(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
        sqlx::query(
            "UPDATE model_tool_calls SET call_status='cancelled',effect_evidence='not_started',
                available_at=NULL,failure_class='safe',
                failure_code='MODEL_TOOL_SIBLING_ABORTED',failure_retryable=FALSE,
                lease_epoch=lease_epoch+1,
                fencing_token=fencing_token || ':batch-abort:' || (projection_version+1)::TEXT,
                completed_at=clock_timestamp(),projection_version=projection_version+1,
                updated_at=clock_timestamp()
             WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3 AND model_call_no=$4
               AND call_status IN ('pending','claimed')",
        )
        .bind(run_id)
        .bind(activation_id)
        .bind(attempt_no)
        .bind(model_call_no)
        .execute(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
        counts = sqlx::query(
            "SELECT COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE call_status='succeeded') AS succeeded,
                    COUNT(*) FILTER (WHERE call_status='failed') AS failed,
                    COUNT(*) FILTER (WHERE call_status='cancelled') AS cancelled,
                    COUNT(*) FILTER (WHERE failure_class='effect_outcome_unknown') AS unknown
             FROM model_tool_calls
             WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3 AND model_call_no=$4",
        )
        .bind(run_id)
        .bind(activation_id)
        .bind(attempt_no)
        .bind(model_call_no)
        .fetch_one(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
        failed = counts
            .try_get("failed")
            .map_err(|_| RepositoryError::invalid_data())?;
        cancelled = counts
            .try_get("cancelled")
            .map_err(|_| RepositoryError::invalid_data())?;
        unknown = counts
            .try_get("unknown")
            .map_err(|_| RepositoryError::invalid_data())?;
    }
    let total: i64 = counts
        .try_get("total")
        .map_err(|_| RepositoryError::invalid_data())?;
    let succeeded: i64 = counts
        .try_get("succeeded")
        .map_err(|_| RepositoryError::invalid_data())?;
    if total == 0 || succeeded + failed + cancelled != total {
        return Ok(ModelToolContinuationStatus::WaitingTools);
    }
    let (execution, continuation) = if failed > 0 || unknown > 0 {
        ("failed", ModelToolContinuationStatus::ReadyFailed)
    } else if cancelled > 0 {
        ("cancelled", ModelToolContinuationStatus::ReadyCancelled)
    } else if succeeded == total {
        ("succeeded", ModelToolContinuationStatus::ReadyContinue)
    } else {
        return Err(RepositoryError::invalid_data());
    };
    let transitioned = sqlx::query(
        "UPDATE model_tool_call_batches SET execution_status=$1,continuation_status=$2,
                completed_at=clock_timestamp(),updated_at=clock_timestamp()
         WHERE run_id=$3 AND activation_id=$4 AND attempt_no=$5 AND model_call_no=$6
           AND execution_status='active' AND continuation_status='waiting_tools'",
    )
    .bind(execution)
    .bind(model_tool_continuation_status_as_str(continuation))
    .bind(run_id)
    .bind(activation_id)
    .bind(attempt_no)
    .bind(model_call_no)
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if transitioned == 1 {
        let parent = sqlx::query(
            "SELECT parent_task_id,parent_claim_token,parent_task_projection_version
             FROM model_tool_call_batches
             WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3 AND model_call_no=$4",
        )
        .bind(run_id)
        .bind(activation_id)
        .bind(attempt_no)
        .bind(model_call_no)
        .fetch_one(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
        let parent_rows = sqlx::query(
            "UPDATE task_outbox SET task_state='pending',available_at=clock_timestamp(),
                    claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL,claim_mode=NULL,
                    projection_version=projection_version+1
             WHERE run_id=$1 AND task_id=$2 AND task_state='claimed'
               AND claim_token=$3 AND projection_version=$4",
        )
        .bind(run_id)
        .bind(
            parent
                .try_get::<String, _>("parent_task_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .bind(
            parent
                .try_get::<String, _>("parent_claim_token")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .bind(
            parent
                .try_get::<i64, _>("parent_task_projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .execute(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if parent_rows != 1 {
            return Err(RepositoryError::invalid_data());
        }
    }
    Ok(continuation)
}

async fn expire_parent_operation_deadline_batch_postgres(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &str,
    activation_id: &str,
    attempt_no: i32,
    model_call_no: i32,
) -> Result<bool, RepositoryError> {
    let row = sqlx::query(
        "SELECT b.execution_status,b.continuation_status,b.parent_operation_deadline,
                b.parent_operation_deadline<=clock_timestamp() AS deadline_elapsed,
                r.lifecycle AS run_lifecycle
         FROM model_tool_call_batches b
         JOIN workflow_runs r ON r.run_id=b.run_id
         WHERE b.run_id=$1 AND b.activation_id=$2 AND b.attempt_no=$3 AND b.model_call_no=$4
         FOR UPDATE OF b",
    )
    .bind(run_id)
    .bind(activation_id)
    .bind(attempt_no)
    .bind(model_call_no)
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(false);
    };
    if matches!(
        row.try_get::<String, _>("run_lifecycle").ok().as_deref(),
        Some("succeeded" | "failed" | "cancelled" | "interrupted" | "timed_out")
    ) || row.try_get::<String, _>("execution_status").ok().as_deref() != Some("active")
        || row
            .try_get::<String, _>("continuation_status")
            .ok()
            .as_deref()
            != Some("waiting_tools")
    {
        return Ok(false);
    }
    let missing_deadline = row
        .try_get::<Option<DateTime<Utc>>, _>("parent_operation_deadline")
        .map_err(|_| RepositoryError::invalid_data())?
        .is_none();
    let elapsed = row
        .try_get::<Option<bool>, _>("deadline_elapsed")
        .map_err(|_| RepositoryError::invalid_data())?;
    if !missing_deadline && elapsed != Some(true) {
        return Ok(false);
    }

    sqlx::query(
        "UPDATE model_tool_calls SET call_status='failed',effect_evidence='unknown',
            failure_class='effect_outcome_unknown',failure_code=$1,failure_retryable=FALSE,
            available_at=NULL,claim_owner=NULL,claim_token=NULL,claim_expires_at=NULL,
            lease_epoch=lease_epoch+1,
            fencing_token=fencing_token || ':parent-deadline:' || (projection_version+1)::TEXT,
            completed_at=clock_timestamp(),projection_version=projection_version+1,
            updated_at=clock_timestamp()
         WHERE run_id=$2 AND activation_id=$3 AND attempt_no=$4 AND model_call_no=$5
           AND call_status='running'",
    )
    .bind(MODEL_TOOL_PARENT_DEADLINE_EXCEEDED)
    .bind(run_id)
    .bind(activation_id)
    .bind(attempt_no)
    .bind(model_call_no)
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "UPDATE model_tool_calls SET call_status='cancelled',effect_evidence='not_started',
            failure_class='safe',failure_code=$1,failure_retryable=FALSE,available_at=NULL,
            claim_owner=NULL,claim_token=NULL,claim_expires_at=NULL,
            lease_epoch=lease_epoch+1,
            fencing_token=fencing_token || ':parent-deadline:' || (projection_version+1)::TEXT,
            completed_at=clock_timestamp(),projection_version=projection_version+1,
            updated_at=clock_timestamp()
         WHERE run_id=$2 AND activation_id=$3 AND attempt_no=$4 AND model_call_no=$5
           AND call_status IN ('pending','claimed')",
    )
    .bind(MODEL_TOOL_PARENT_DEADLINE_EXCEEDED)
    .bind(run_id)
    .bind(activation_id)
    .bind(attempt_no)
    .bind(model_call_no)
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;

    if finalize_batch_barrier_postgres(tx, run_id, activation_id, attempt_no, model_call_no).await?
        == ModelToolContinuationStatus::WaitingTools
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(true)
}

async fn expire_parent_operation_deadlines_postgres(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), RepositoryError> {
    let batches = sqlx::query(
        "SELECT b.run_id,b.activation_id,b.attempt_no,b.model_call_no
         FROM model_tool_call_batches b
         JOIN workflow_runs r ON r.run_id=b.run_id
         WHERE b.execution_status='active' AND b.continuation_status='waiting_tools'
           AND r.lifecycle NOT IN ('succeeded','failed','cancelled','interrupted','timed_out')
           AND (b.parent_operation_deadline IS NULL
                OR b.parent_operation_deadline<=clock_timestamp())
         ORDER BY b.run_id,b.activation_id,b.attempt_no,b.model_call_no
         LIMIT $1
         FOR UPDATE OF b SKIP LOCKED",
    )
    .bind(i64::from(MAX_CLAIM_LIMIT))
    .fetch_all(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    for batch in batches {
        expire_parent_operation_deadline_batch_postgres(
            tx,
            &batch
                .try_get::<String, _>("run_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            &batch
                .try_get::<String, _>("activation_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            batch
                .try_get("attempt_no")
                .map_err(|_| RepositoryError::invalid_data())?,
            batch
                .try_get("model_call_no")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .await?;
    }
    Ok(())
}

async fn recover_expired_model_tool_calls_postgres(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), RepositoryError> {
    expire_parent_operation_deadlines_postgres(tx).await?;
    let expired = sqlx::query(
        "SELECT c.run_id,c.activation_id,c.attempt_no,c.model_call_no,c.call_index
         FROM model_tool_calls c
         JOIN model_tool_call_batches b ON b.run_id=c.run_id
           AND b.activation_id=c.activation_id AND b.attempt_no=c.attempt_no
           AND b.model_call_no=c.model_call_no
         JOIN workflow_runs r ON r.run_id=c.run_id
         WHERE b.execution_status='active' AND b.continuation_status='waiting_tools'
           AND r.lifecycle NOT IN ('succeeded','failed','cancelled','interrupted','timed_out')
           AND c.call_status IN ('claimed','running')
           AND c.claim_expires_at<=statement_timestamp()
         ORDER BY c.claim_expires_at,c.run_id,c.activation_id,c.attempt_no,
                  c.model_call_no,c.call_index
         LIMIT $1",
    )
    .bind(i64::from(MAX_CLAIM_LIMIT))
    .fetch_all(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    for identity in expired {
        let run_id: String = identity
            .try_get("run_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let activation_id: String = identity
            .try_get("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let parent_attempt: i32 = identity
            .try_get("attempt_no")
            .map_err(|_| RepositoryError::invalid_data())?;
        let model_call_no: i32 = identity
            .try_get("model_call_no")
            .map_err(|_| RepositoryError::invalid_data())?;
        let call_index: i32 = identity
            .try_get("call_index")
            .map_err(|_| RepositoryError::invalid_data())?;
        if lock_batch(tx, &run_id, &activation_id, parent_attempt, model_call_no)
            .await?
            .as_deref()
            != Some("waiting_tools")
        {
            continue;
        }
        let row = sqlx::query(
            "SELECT tool_task_id,call_status,tool_attempt_no,lease_epoch,effect_idempotency,
                    max_attempts,initial_backoff_ms,max_backoff_ms
             FROM model_tool_calls
             WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3 AND model_call_no=$4
               AND call_index=$5 AND call_status IN ('claimed','running')
               AND claim_expires_at<=clock_timestamp()
               AND EXISTS (
                   SELECT 1 FROM workflow_runs r
                   WHERE r.run_id=model_tool_calls.run_id
                     AND r.lifecycle NOT IN
                         ('succeeded','failed','cancelled','interrupted','timed_out')
               )
             FOR UPDATE",
        )
        .bind(&run_id)
        .bind(&activation_id)
        .bind(parent_attempt)
        .bind(model_call_no)
        .bind(call_index)
        .fetch_optional(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            continue;
        };
        let status: String = row
            .try_get("call_status")
            .map_err(|_| RepositoryError::invalid_data())?;
        let task_id = SchedulerTaskId::parse(
            row.try_get::<String, _>("tool_task_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let attempt_no = AttemptNo::new(
            u32::try_from(
                row.try_get::<i32, _>("tool_attempt_no")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let lease_epoch = LeaseEpoch::new(u64_from_i64(
            row.try_get("lease_epoch")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?)
        .map_err(|_| RepositoryError::invalid_data())?;
        let max_attempts = u32::try_from(
            row.try_get::<i32, _>("max_attempts")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let idempotent = row
            .try_get::<String, _>("effect_idempotency")
            .map_err(|_| RepositoryError::invalid_data())?
            == "idempotent";
        if status == "claimed" || (idempotent && attempt_no.get() < max_attempts) {
            let next_attempt = if status == "running" {
                attempt_no
                    .next()
                    .map_err(|_| RepositoryError::invalid_data())?
            } else {
                attempt_no
            };
            let next_lease = lease_epoch
                .next()
                .map_err(|_| RepositoryError::invalid_data())?;
            let evidence = if status == "running" {
                "unknown"
            } else {
                "not_started"
            };
            let delay_ms = if status == "running" {
                let initial = u64_from_i64(
                    row.try_get("initial_backoff_ms")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?;
                let maximum = u64_from_i64(
                    row.try_get("max_backoff_ms")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?;
                initial
                    .saturating_mul(1_u64 << attempt_no.get().saturating_sub(1).min(63))
                    .min(maximum)
            } else {
                0
            };
            let rows_updated = sqlx::query(
                "UPDATE model_tool_calls SET call_status='pending',tool_attempt_no=$1,lease_epoch=$2,
                    fencing_token=$3,effect_evidence='not_started',
                    available_at=clock_timestamp()+$4*INTERVAL '1 millisecond',
                    claim_owner=NULL,claim_token=NULL,claim_expires_at=NULL,
                    projection_version=projection_version+1,lease_loss_count=lease_loss_count+1,
                    last_lease_loss_at=clock_timestamp(),last_lease_loss_evidence=$5,
                    updated_at=clock_timestamp()
                 WHERE run_id=$6 AND activation_id=$7 AND attempt_no=$8 AND model_call_no=$9
                   AND call_index=$10 AND call_status=$11 AND lease_epoch=$12
                   AND EXISTS (
                       SELECT 1 FROM workflow_runs r
                       WHERE r.run_id=model_tool_calls.run_id
                         AND r.lifecycle NOT IN
                             ('succeeded','failed','cancelled','interrupted','timed_out')
                   )",
            )
            .bind(i32::try_from(next_attempt.get()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(i64_from_u64(next_lease.get())?)
            .bind(tool_fencing_token(&task_id, next_attempt, next_lease))
            .bind(i64_from_u64(delay_ms)?)
            .bind(evidence)
            .bind(&run_id)
            .bind(&activation_id)
            .bind(parent_attempt)
            .bind(model_call_no)
            .bind(call_index)
            .bind(&status)
            .bind(i64_from_u64(lease_epoch.get())?)
            .execute(&mut **tx)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows_updated != 1 {
                return Err(RepositoryError::invalid_data());
            }
        } else {
            let rows_updated = sqlx::query(
                "UPDATE model_tool_calls SET call_status='failed',effect_evidence='unknown',
                    failure_class='effect_outcome_unknown',failure_code='TOOL_EFFECT_OUTCOME_UNKNOWN',
                    failure_retryable=FALSE,completed_at=clock_timestamp(),
                    projection_version=projection_version+1,lease_loss_count=lease_loss_count+1,
                    last_lease_loss_at=clock_timestamp(),last_lease_loss_evidence='unknown',
                    updated_at=clock_timestamp()
                 WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3 AND model_call_no=$4
                   AND call_index=$5 AND call_status='running' AND lease_epoch=$6
                   AND EXISTS (
                       SELECT 1 FROM workflow_runs r
                       WHERE r.run_id=model_tool_calls.run_id
                         AND r.lifecycle NOT IN
                             ('succeeded','failed','cancelled','interrupted','timed_out')
                   )",
            )
            .bind(&run_id)
            .bind(&activation_id)
            .bind(parent_attempt)
            .bind(model_call_no)
            .bind(call_index)
            .bind(i64_from_u64(lease_epoch.get())?)
            .execute(&mut **tx)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows_updated != 1 {
                return Err(RepositoryError::invalid_data());
            }
            finalize_batch_barrier_postgres(
                tx,
                &run_id,
                &activation_id,
                parent_attempt,
                model_call_no,
            )
            .await?;
        }
    }
    Ok(())
}

pub(crate) async fn claim_model_tool_calls_postgres(
    repository: &PostgresDurableRepository,
    claimed_by: &str,
    claim_seconds: u32,
    limit: u32,
    max_claimed_per_run: u32,
) -> Result<Vec<ModelToolTaskClaim>, RepositoryError> {
    if !valid_claim_parameters(claimed_by, claim_seconds, limit, max_claimed_per_run) {
        return Err(RepositoryError::invalid_configuration());
    }
    let mut tx = begin_write_transaction(&repository.pool).await?;
    recover_expired_model_tool_calls_postgres(&mut tx).await?;
    expire_parent_operation_deadlines_postgres(&mut tx).await?;
    let candidates = sqlx::query(
        "SELECT c.run_id,c.activation_id,c.attempt_no,c.model_call_no,c.tool_task_id
         FROM model_tool_calls c
         JOIN model_tool_call_batches b ON b.run_id=c.run_id
           AND b.activation_id=c.activation_id AND b.attempt_no=c.attempt_no
           AND b.model_call_no=c.model_call_no
         JOIN workflow_runs r ON r.run_id=c.run_id
         WHERE c.call_status='pending' AND c.available_at<=statement_timestamp()
           AND b.execution_status='active' AND b.continuation_status='waiting_tools'
           AND r.lifecycle IN ('created','active','waiting')
         ORDER BY c.available_at,c.run_id,c.activation_id,c.attempt_no,
                  c.model_call_no,c.call_index
         LIMIT $1",
    )
    .bind(i64::from(MAX_CLAIM_LIMIT))
    .fetch_all(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let mut active_by_run = std::collections::BTreeMap::<String, u32>::new();
    let mut claims = Vec::new();
    for candidate in candidates {
        if claims.len() >= limit as usize {
            break;
        }
        let run_id: String = candidate
            .try_get("run_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let current = if let Some(value) = active_by_run.get(&run_id) {
            *value
        } else {
            let run_authority =
                RunId::new(run_id.clone()).map_err(|_| RepositoryError::invalid_data())?;
            // Serialize the combined ordinary-task/tool-task per-Run bound
            // with the normal scheduler claimant before counting either set.
            lock_run_for_event_write(&mut tx, &run_authority).await?;
            let value = u32::try_from(
                sqlx::query_scalar::<_, i64>(
                    "SELECT
                        (SELECT COUNT(*) FROM model_tool_calls
                         WHERE run_id=$1 AND call_status IN ('claimed','running')
                           AND claim_expires_at>clock_timestamp())
                        +
                        (SELECT COUNT(*) FROM task_outbox o
                         WHERE o.run_id=$1 AND o.task_state='claimed'
                           AND o.claim_expires_at>clock_timestamp()
                           AND NOT EXISTS (
                               SELECT 1 FROM model_tool_call_batches b
                               WHERE b.run_id=o.run_id AND b.parent_task_id=o.task_id
                                 AND b.execution_status='active'
                                 AND b.continuation_status='waiting_tools'
                           ))",
                )
                .bind(&run_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(RepositoryError::storage)?,
            )
            .map_err(|_| RepositoryError::invalid_data())?;
            active_by_run.insert(run_id.clone(), value);
            value
        };
        if current >= max_claimed_per_run {
            continue;
        }
        let task_id: String = candidate
            .try_get("tool_task_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let activation_id: String = candidate
            .try_get("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let parent_attempt: i32 = candidate
            .try_get("attempt_no")
            .map_err(|_| RepositoryError::invalid_data())?;
        let model_call_no: i32 = candidate
            .try_get("model_call_no")
            .map_err(|_| RepositoryError::invalid_data())?;
        if expire_parent_operation_deadline_batch_postgres(
            &mut tx,
            &run_id,
            &activation_id,
            parent_attempt,
            model_call_no,
        )
        .await?
        {
            continue;
        }
        let claim_token = format!("tool_claim_{}", Uuid::new_v4().simple());
        let claimed = sqlx::query(
            "UPDATE model_tool_calls SET call_status='claimed',claim_owner=$1,claim_token=$2,
                    claim_expires_at=clock_timestamp()+$3*INTERVAL '1 second',
                    available_at=NULL,projection_version=projection_version+1,
                    updated_at=clock_timestamp()
             WHERE run_id=$4 AND tool_task_id=$5 AND call_status='pending'
               AND available_at<=clock_timestamp()
               AND EXISTS (
                   SELECT 1 FROM model_tool_call_batches b
                   WHERE b.run_id=model_tool_calls.run_id
                     AND b.activation_id=model_tool_calls.activation_id
                     AND b.attempt_no=model_tool_calls.attempt_no
                     AND b.model_call_no=model_tool_calls.model_call_no
                     AND b.execution_status='active'
                     AND b.continuation_status='waiting_tools'
                     AND b.parent_operation_deadline IS NOT NULL
                     AND b.parent_operation_deadline>clock_timestamp()
               )
               AND EXISTS (
                   SELECT 1 FROM workflow_runs r
                   WHERE r.run_id=model_tool_calls.run_id
                     AND r.lifecycle IN ('created','active','waiting')
               )
             RETURNING *",
        )
        .bind(claimed_by)
        .bind(&claim_token)
        .bind(i64::from(claim_seconds))
        .bind(&run_id)
        .bind(&task_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(claimed) = claimed else {
            expire_parent_operation_deadline_batch_postgres(
                &mut tx,
                &run_id,
                &activation_id,
                parent_attempt,
                model_call_no,
            )
            .await?;
            continue;
        };
        claims.push(decode_claim(&claimed)?);
        active_by_run.insert(run_id, current + 1);
    }
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(claims)
}

fn same_current_claim(row: &PgRow, claim: &ModelToolTaskClaim) -> bool {
    row.try_get::<String, _>("run_id").ok().as_deref() == Some(claim.run_id().as_str())
        && row.try_get::<String, _>("tool_task_id").ok().as_deref()
            == Some(claim.identity().tool_task_id().as_str())
        && row.try_get::<i32, _>("tool_attempt_no").ok()
            == i32::try_from(claim.tool_attempt_no().get()).ok()
        && row.try_get::<i64, _>("lease_epoch").ok()
            == i64::try_from(claim.lease_epoch().get()).ok()
        && row.try_get::<String, _>("fencing_token").ok().as_deref() == Some(claim.fencing_token())
        && row.try_get::<String, _>("claim_owner").ok().as_deref() == Some(claim.claimed_by())
        && row.try_get::<String, _>("claim_token").ok().as_deref() == Some(claim.claim_token())
        && row.try_get::<i64, _>("projection_version").ok()
            == i64::try_from(claim.projection_version()).ok()
        && row
            .try_get::<Option<DateTime<Utc>>, _>("claim_expires_at")
            .ok()
            .flatten()
            == Some(claim.claim_expires_at())
}

async fn load_tool_row_postgres(
    tx: &mut Transaction<'_, Postgres>,
    claim: &ModelToolTaskClaim,
) -> Result<Option<PgRow>, RepositoryError> {
    sqlx::query(
        "SELECT c.*,b.continuation_status,r.lifecycle AS run_lifecycle,
                c.claim_expires_at>clock_timestamp() AS claim_fresh
         FROM model_tool_calls c
         JOIN model_tool_call_batches b ON b.run_id=c.run_id
           AND b.activation_id=c.activation_id AND b.attempt_no=c.attempt_no
           AND b.model_call_no=c.model_call_no
         JOIN workflow_runs r ON r.run_id=c.run_id
         WHERE c.run_id=$1 AND c.tool_task_id=$2
         FOR UPDATE OF c",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.identity().tool_task_id().as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)
}

fn row_run_is_terminal(row: &PgRow) -> Result<bool, RepositoryError> {
    Ok(matches!(
        row.try_get::<String, _>("run_lifecycle")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_str(),
        "succeeded" | "failed" | "cancelled" | "interrupted" | "timed_out"
    ))
}

async fn lock_claim_batch(
    tx: &mut Transaction<'_, Postgres>,
    claim: &ModelToolTaskClaim,
) -> Result<Option<String>, RepositoryError> {
    lock_batch(
        tx,
        claim.run_id().as_str(),
        claim.parent_activation_id().as_str(),
        i32::try_from(claim.parent_attempt_no().get())
            .map_err(|_| RepositoryError::invalid_data())?,
        i32::try_from(claim.model_call_no()).map_err(|_| RepositoryError::invalid_data())?,
    )
    .await
}

pub(crate) async fn mark_model_tool_call_started_postgres(
    repository: &PostgresDurableRepository,
    claim: &ModelToolTaskClaim,
) -> Result<ModelToolTaskTransitionOutcome<()>, RepositoryError> {
    let mut tx = begin_write_transaction(&repository.pool).await?;
    if lock_claim_batch(&mut tx, claim).await?.is_none() {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    }
    if expire_parent_operation_deadline_batch_postgres(
        &mut tx,
        claim.run_id().as_str(),
        claim.parent_activation_id().as_str(),
        i32::try_from(claim.parent_attempt_no().get())
            .map_err(|_| RepositoryError::invalid_data())?,
        i32::try_from(claim.model_call_no()).map_err(|_| RepositoryError::invalid_data())?,
    )
    .await?
    {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    }
    let Some(row) = load_tool_row_postgres(&mut tx, claim).await? else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    };
    if !same_current_claim(&row, claim) {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    }
    let status = model_tool_task_status_parse(
        &row.try_get::<String, _>("call_status")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    if status == ModelToolTaskStatus::Running {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::ExactReplay(()));
    }
    if row_run_is_terminal(&row)? {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::RunTerminal);
    }
    if status != ModelToolTaskStatus::Claimed
        || row
            .try_get::<String, _>("continuation_status")
            .ok()
            .as_deref()
            != Some("waiting_tools")
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StateConflict);
    }
    if row.try_get::<bool, _>("claim_fresh").ok() != Some(true) {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    }
    let rows = sqlx::query(
        "UPDATE model_tool_calls SET call_status='running',effect_evidence='started',
                started_at=COALESCE(started_at,clock_timestamp()),updated_at=clock_timestamp()
         WHERE run_id=$1 AND tool_task_id=$2 AND call_status='claimed'
           AND tool_attempt_no=$3 AND lease_epoch=$4 AND fencing_token=$5
           AND claim_owner=$6 AND claim_token=$7 AND projection_version=$8
           AND claim_expires_at>clock_timestamp()
           AND EXISTS (
               SELECT 1 FROM model_tool_call_batches b
               WHERE b.run_id=model_tool_calls.run_id
                 AND b.activation_id=model_tool_calls.activation_id
                 AND b.attempt_no=model_tool_calls.attempt_no
                 AND b.model_call_no=model_tool_calls.model_call_no
                 AND b.execution_status='active'
                 AND b.continuation_status='waiting_tools'
                 AND b.parent_operation_deadline IS NOT NULL
                 AND b.parent_operation_deadline>clock_timestamp()
           )",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.identity().tool_task_id().as_str())
    .bind(
        i32::try_from(claim.tool_attempt_no().get())
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(i64_from_u64(claim.lease_epoch().get())?)
    .bind(claim.fencing_token())
    .bind(claim.claimed_by())
    .bind(claim.claim_token())
    .bind(i64::try_from(claim.projection_version()).map_err(|_| RepositoryError::invalid_data())?)
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        if expire_parent_operation_deadline_batch_postgres(
            &mut tx,
            claim.run_id().as_str(),
            claim.parent_activation_id().as_str(),
            i32::try_from(claim.parent_attempt_no().get())
                .map_err(|_| RepositoryError::invalid_data())?,
            i32::try_from(claim.model_call_no()).map_err(|_| RepositoryError::invalid_data())?,
        )
        .await?
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
        } else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
        }
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    }
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(ModelToolTaskTransitionOutcome::Committed(()))
}

pub(crate) async fn heartbeat_model_tool_call_postgres(
    repository: &PostgresDurableRepository,
    claim: &ModelToolTaskClaim,
    claim_seconds: u32,
) -> Result<ModelToolTaskHeartbeatOutcome, RepositoryError> {
    if !(3..=MAX_CLAIM_SECONDS).contains(&claim_seconds) {
        return Err(RepositoryError::invalid_configuration());
    }
    let mut tx = begin_write_transaction(&repository.pool).await?;
    if lock_claim_batch(&mut tx, claim).await?.is_none() {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskHeartbeatOutcome::StaleLease);
    }
    if expire_parent_operation_deadline_batch_postgres(
        &mut tx,
        claim.run_id().as_str(),
        claim.parent_activation_id().as_str(),
        i32::try_from(claim.parent_attempt_no().get())
            .map_err(|_| RepositoryError::invalid_data())?,
        i32::try_from(claim.model_call_no()).map_err(|_| RepositoryError::invalid_data())?,
    )
    .await?
    {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskHeartbeatOutcome::StaleLease);
    }
    let Some(row) = load_tool_row_postgres(&mut tx, claim).await? else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskHeartbeatOutcome::StaleLease);
    };
    if !same_current_claim(&row, claim) {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskHeartbeatOutcome::StaleLease);
    }
    if row_run_is_terminal(&row)? {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskHeartbeatOutcome::RunTerminal);
    }
    let status = model_tool_task_status_parse(
        &row.try_get::<String, _>("call_status")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    if !matches!(
        status,
        ModelToolTaskStatus::Claimed | ModelToolTaskStatus::Running
    ) || row
        .try_get::<String, _>("continuation_status")
        .ok()
        .as_deref()
        != Some("waiting_tools")
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskHeartbeatOutcome::StateConflict);
    }
    if row.try_get::<bool, _>("claim_fresh").ok() != Some(true) {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskHeartbeatOutcome::StaleLease);
    }
    let renewed = sqlx::query(
        "UPDATE model_tool_calls SET
            claim_expires_at=clock_timestamp()+$1*INTERVAL '1 second',
            projection_version=projection_version+1,updated_at=clock_timestamp()
         WHERE run_id=$2 AND tool_task_id=$3 AND call_status IN ('claimed','running')
           AND tool_attempt_no=$4 AND lease_epoch=$5 AND fencing_token=$6
           AND claim_owner=$7 AND claim_token=$8 AND projection_version=$9
           AND claim_expires_at>clock_timestamp()
           AND EXISTS (
               SELECT 1 FROM model_tool_call_batches b
               WHERE b.run_id=model_tool_calls.run_id
                 AND b.activation_id=model_tool_calls.activation_id
                 AND b.attempt_no=model_tool_calls.attempt_no
                 AND b.model_call_no=model_tool_calls.model_call_no
                 AND b.execution_status='active'
                 AND b.continuation_status='waiting_tools'
                 AND b.parent_operation_deadline IS NOT NULL
                 AND b.parent_operation_deadline>clock_timestamp()
           )
         RETURNING *",
    )
    .bind(i64::from(claim_seconds))
    .bind(claim.run_id().as_str())
    .bind(claim.identity().tool_task_id().as_str())
    .bind(
        i32::try_from(claim.tool_attempt_no().get())
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(i64_from_u64(claim.lease_epoch().get())?)
    .bind(claim.fencing_token())
    .bind(claim.claimed_by())
    .bind(claim.claim_token())
    .bind(i64::try_from(claim.projection_version()).map_err(|_| RepositoryError::invalid_data())?)
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(renewed) = renewed else {
        if expire_parent_operation_deadline_batch_postgres(
            &mut tx,
            claim.run_id().as_str(),
            claim.parent_activation_id().as_str(),
            i32::try_from(claim.parent_attempt_no().get())
                .map_err(|_| RepositoryError::invalid_data())?,
            i32::try_from(claim.model_call_no()).map_err(|_| RepositoryError::invalid_data())?,
        )
        .await?
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
        } else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
        }
        return Ok(ModelToolTaskHeartbeatOutcome::StaleLease);
    };
    let renewed = decode_claim(&renewed)?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(ModelToolTaskHeartbeatOutcome::Renewed(Box::new(renewed)))
}

fn retry_delay_ms(policy: &WorkerEffectPolicy, attempt_no: AttemptNo) -> u64 {
    let shift = attempt_no.get().saturating_sub(1).min(63);
    policy
        .initial_backoff_ms()
        .saturating_mul(1_u64 << shift)
        .min(policy.max_backoff_ms())
}

fn model_tool_duration_ms(row: &PgRow) -> Result<Option<u64>, RepositoryError> {
    let started_at = row
        .try_get::<Option<DateTime<Utc>>, _>("started_at")
        .map_err(|_| RepositoryError::invalid_data())?;
    let completed_at = row
        .try_get::<Option<DateTime<Utc>>, _>("completed_at")
        .map_err(|_| RepositoryError::invalid_data())?;
    match (started_at, completed_at) {
        (Some(started_at), Some(completed_at)) => u64::try_from(
            completed_at
                .signed_duration_since(started_at)
                .num_milliseconds(),
        )
        .map(Some)
        .map_err(|_| RepositoryError::invalid_data()),
        _ => Ok(None),
    }
}

fn decode_last_receipt(row: &PgRow) -> Result<ModelToolTaskCommitReceipt, RepositoryError> {
    let task_id = SchedulerTaskId::parse(
        row.try_get::<String, _>("tool_task_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let disposition = model_tool_task_disposition_parse(
        &row.try_get::<String, _>("last_outcome_disposition")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let attempt_no = AttemptNo::new(
        u32::try_from(
            row.try_get::<i32, _>("last_outcome_attempt_no")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let lease_epoch = LeaseEpoch::new(u64_from_i64(
        row.try_get("last_outcome_lease_epoch")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?)
    .map_err(|_| RepositoryError::invalid_data())?;
    let available = row
        .try_get::<Option<DateTime<Utc>>, _>("last_outcome_available_at")
        .map_err(|_| RepositoryError::invalid_data())?;
    let continuation = model_tool_continuation_status_parse(
        &row.try_get::<String, _>("continuation_status")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    model_tool_task_commit_receipt_new(
        task_id,
        disposition,
        attempt_no,
        lease_epoch,
        available,
        continuation,
        if disposition == ModelToolTaskDisposition::RetryScheduled {
            None
        } else {
            model_tool_duration_ms(row)?
        },
    )
}

async fn finish_stale_model_tool_commit_postgres(
    mut tx: Transaction<'_, Postgres>,
    claim: &ModelToolTaskClaim,
) -> Result<ModelToolTaskTransitionOutcome<ModelToolTaskCommitReceipt>, RepositoryError> {
    let expired = expire_parent_operation_deadline_batch_postgres(
        &mut tx,
        claim.run_id().as_str(),
        claim.parent_activation_id().as_str(),
        i32::try_from(claim.parent_attempt_no().get())
            .map_err(|_| RepositoryError::invalid_data())?,
        i32::try_from(claim.model_call_no()).map_err(|_| RepositoryError::invalid_data())?,
    )
    .await?;
    if expired {
        tx.commit().await.map_err(RepositoryError::storage)?;
    } else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
    }
    Ok(ModelToolTaskTransitionOutcome::StaleLease)
}

pub(crate) async fn commit_model_tool_call_outcome_postgres(
    repository: &PostgresDurableRepository,
    claim: &ModelToolTaskClaim,
    outcome: &ModelToolTaskOutcome,
) -> Result<ModelToolTaskTransitionOutcome<ModelToolTaskCommitReceipt>, RepositoryError> {
    let outcome_hash = model_tool_task_outcome_canonical_hash(outcome)?;
    let mut tx = begin_write_transaction(&repository.pool).await?;
    if lock_claim_batch(&mut tx, claim).await?.is_none() {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    }
    let parent_deadline_elapsed = expire_parent_operation_deadline_batch_postgres(
        &mut tx,
        claim.run_id().as_str(),
        claim.parent_activation_id().as_str(),
        i32::try_from(claim.parent_attempt_no().get())
            .map_err(|_| RepositoryError::invalid_data())?,
        i32::try_from(claim.model_call_no()).map_err(|_| RepositoryError::invalid_data())?,
    )
    .await?;
    let Some(row) = load_tool_row_postgres(&mut tx, claim).await? else {
        if parent_deadline_elapsed {
            tx.commit().await.map_err(RepositoryError::storage)?;
        } else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
        }
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    };
    let last_claim = row
        .try_get::<Option<String>, _>("last_commit_claim_token")
        .map_err(|_| RepositoryError::invalid_data())?;
    if last_claim.as_deref() == Some(claim.claim_token()) {
        let exact = row
            .try_get::<Option<String>, _>("last_outcome_hash")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            == Some(outcome_hash.as_str())
            && row
                .try_get::<Option<i32>, _>("last_outcome_attempt_no")
                .ok()
                .flatten()
                == i32::try_from(claim.tool_attempt_no().get()).ok()
            && row
                .try_get::<Option<i64>, _>("last_outcome_lease_epoch")
                .ok()
                .flatten()
                == i64::try_from(claim.lease_epoch().get()).ok()
            && row
                .try_get::<Option<String>, _>("last_outcome_fencing_token")
                .ok()
                .flatten()
                .as_deref()
                == Some(claim.fencing_token());
        if exact {
            let receipt = decode_last_receipt(&row)?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(ModelToolTaskTransitionOutcome::ExactReplay(receipt));
        }
        if parent_deadline_elapsed {
            tx.commit().await.map_err(RepositoryError::storage)?;
        } else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
        }
        return Ok(ModelToolTaskTransitionOutcome::StateConflict);
    }
    if parent_deadline_elapsed {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    }
    if !same_current_claim(&row, claim) {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    }
    if row_run_is_terminal(&row)? {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::RunTerminal);
    }
    if row.try_get::<bool, _>("claim_fresh").ok() != Some(true) {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    }
    let status = model_tool_task_status_parse(
        &row.try_get::<String, _>("call_status")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let stored_evidence = match row
        .try_get::<String, _>("effect_evidence")
        .map_err(|_| RepositoryError::invalid_data())?
        .as_str()
    {
        "not_started" => EffectEvidence::NotStarted,
        "started" => EffectEvidence::Started,
        _ => return Err(RepositoryError::invalid_data()),
    };
    let policy: WorkerEffectPolicy = serde_json::from_value(
        row.try_get::<Value, _>("action_effect_policy")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let action = decode_action(&row)?;
    let (disposition, next_available_at) = match outcome {
        ModelToolTaskOutcome::Succeeded { result } => {
            if status != ModelToolTaskStatus::Running {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(ModelToolTaskTransitionOutcome::StateConflict);
            }
            validate_tool_result(&action, result)?;
            let public_result = RunToolPublicProjection::from_frozen_effective_policy(
                action.effective_public_policy(),
            )
            .and_then(|projection| {
                projection.project_validated_completed_result(
                    claim.identity().call_id(),
                    action.name(),
                    result,
                )
            })
            .map_err(|_| RepositoryError::invalid_data())?;
            validate_projected_tool_artifacts_postgres(
                &mut tx,
                claim.run_id(),
                public_result.as_ref(),
            )
            .await?;
            let updated = sqlx::query(
                "UPDATE model_tool_calls SET call_status='succeeded',effect_evidence='committed',
                    result_json=$1,completed_at=clock_timestamp(),projection_version=projection_version+1,
                    last_commit_claim_token=$2,last_outcome_hash=$3,last_outcome_disposition='succeeded',
                    last_outcome_attempt_no=$4,last_outcome_lease_epoch=$5,
                    last_outcome_fencing_token=$6,last_outcome_available_at=NULL,
                    last_effect_evidence='committed',updated_at=clock_timestamp()
                 WHERE run_id=$7 AND tool_task_id=$8 AND call_status='running'
                   AND tool_attempt_no=$9 AND lease_epoch=$10 AND fencing_token=$11
                   AND claim_token=$12 AND projection_version=$13
                   AND claim_expires_at>clock_timestamp()
                   AND EXISTS(
                       SELECT 1 FROM model_tool_call_batches b
                       WHERE b.run_id=model_tool_calls.run_id
                         AND b.activation_id=model_tool_calls.activation_id
                         AND b.attempt_no=model_tool_calls.attempt_no
                         AND b.model_call_no=model_tool_calls.model_call_no
                         AND b.execution_status='active'
                         AND b.continuation_status='waiting_tools'
                         AND b.parent_operation_deadline IS NOT NULL
                         AND b.parent_operation_deadline>clock_timestamp()
                   )",
            )
            .bind(result)
            .bind(claim.claim_token())
            .bind(outcome_hash.as_str())
            .bind(i32::try_from(claim.tool_attempt_no().get()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(i64_from_u64(claim.lease_epoch().get())?)
            .bind(claim.fencing_token())
            .bind(claim.run_id().as_str())
            .bind(claim.identity().tool_task_id().as_str())
            .bind(i32::try_from(claim.tool_attempt_no().get()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(i64_from_u64(claim.lease_epoch().get())?)
            .bind(claim.fencing_token())
            .bind(claim.claim_token())
            .bind(i64::try_from(claim.projection_version()).map_err(|_| RepositoryError::invalid_data())?)
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if updated != 1 {
                return finish_stale_model_tool_commit_postgres(tx, claim).await;
            }
            (ModelToolTaskDisposition::Succeeded, None)
        }
        ModelToolTaskOutcome::Failed {
            class,
            code,
            retryable,
            effect_evidence,
        } => {
            if !matches!(
                status,
                ModelToolTaskStatus::Claimed | ModelToolTaskStatus::Running
            ) || !stored_evidence.can_transition_to(*effect_evidence)
            {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(ModelToolTaskTransitionOutcome::StateConflict);
            }
            let automatic_retry = *retryable
                && claim.tool_attempt_no().get() < policy.max_attempts()
                && effect_evidence.permits_automatic_retry(policy.effect_idempotency());
            if automatic_retry {
                let next_attempt = claim
                    .tool_attempt_no()
                    .next()
                    .map_err(|_| RepositoryError::invalid_data())?;
                let next_lease = claim
                    .lease_epoch()
                    .next()
                    .map_err(|_| RepositoryError::invalid_data())?;
                let delay_ms = retry_delay_ms(&policy, claim.tool_attempt_no());
                let next_available = sqlx::query_scalar::<_, DateTime<Utc>>(
                    "SELECT clock_timestamp()+$1*INTERVAL '1 millisecond'",
                )
                .bind(i64_from_u64(delay_ms)?)
                .fetch_one(&mut *tx)
                .await
                .map_err(RepositoryError::storage)?;
                let updated = sqlx::query(
                    "UPDATE model_tool_calls SET call_status='pending',tool_attempt_no=$1,lease_epoch=$2,
                        fencing_token=$3,effect_evidence='not_started',available_at=$4,
                        claim_owner=NULL,claim_token=NULL,claim_expires_at=NULL,
                        projection_version=projection_version+1,last_commit_claim_token=$5,
                        last_outcome_hash=$6,last_outcome_disposition='retry_scheduled',
                        last_outcome_attempt_no=$7,last_outcome_lease_epoch=$8,
                        last_outcome_fencing_token=$9,last_outcome_available_at=$10,
                        last_effect_evidence=$11,last_failure_class=$12,last_failure_code=$13,
                        last_failure_retryable=$14,updated_at=clock_timestamp()
                     WHERE run_id=$15 AND tool_task_id=$16 AND call_status IN ('claimed','running')
                       AND tool_attempt_no=$17 AND lease_epoch=$18 AND fencing_token=$19
                       AND claim_token=$20 AND projection_version=$21
                       AND claim_expires_at>clock_timestamp()
                       AND EXISTS(
                           SELECT 1 FROM model_tool_call_batches b
                           WHERE b.run_id=model_tool_calls.run_id
                             AND b.activation_id=model_tool_calls.activation_id
                             AND b.attempt_no=model_tool_calls.attempt_no
                             AND b.model_call_no=model_tool_calls.model_call_no
                             AND b.execution_status='active'
                             AND b.continuation_status='waiting_tools'
                             AND b.parent_operation_deadline IS NOT NULL
                             AND b.parent_operation_deadline>clock_timestamp()
                       )",
                )
                .bind(i32::try_from(next_attempt.get()).map_err(|_| RepositoryError::invalid_data())?)
                .bind(i64_from_u64(next_lease.get())?)
                .bind(tool_fencing_token(claim.identity().tool_task_id(), next_attempt, next_lease))
                .bind(next_available)
                .bind(claim.claim_token())
                .bind(outcome_hash.as_str())
                .bind(i32::try_from(claim.tool_attempt_no().get()).map_err(|_| RepositoryError::invalid_data())?)
                .bind(i64_from_u64(claim.lease_epoch().get())?)
                .bind(claim.fencing_token())
                .bind(next_available)
                .bind(effect_evidence_str(*effect_evidence))
                .bind(failure_class_str(*class))
                .bind(code)
                .bind(*retryable)
                .bind(claim.run_id().as_str())
                .bind(claim.identity().tool_task_id().as_str())
                .bind(i32::try_from(claim.tool_attempt_no().get()).map_err(|_| RepositoryError::invalid_data())?)
                .bind(i64_from_u64(claim.lease_epoch().get())?)
                .bind(claim.fencing_token())
                .bind(claim.claim_token())
                .bind(i64::try_from(claim.projection_version()).map_err(|_| RepositoryError::invalid_data())?)
                .execute(&mut *tx)
                .await
                .map_err(RepositoryError::storage)?
                .rows_affected();
                if updated != 1 {
                    return finish_stale_model_tool_commit_postgres(tx, claim).await;
                }
                (
                    ModelToolTaskDisposition::RetryScheduled,
                    Some(next_available),
                )
            } else {
                let updated = sqlx::query(
                    "UPDATE model_tool_calls SET call_status='failed',effect_evidence=$1,
                        failure_class=$2,failure_code=$3,failure_retryable=$4,
                        completed_at=clock_timestamp(),projection_version=projection_version+1,
                        last_commit_claim_token=$5,last_outcome_hash=$6,
                        last_outcome_disposition='failed',last_outcome_attempt_no=$7,
                        last_outcome_lease_epoch=$8,last_outcome_fencing_token=$9,
                        last_outcome_available_at=NULL,last_effect_evidence=$10,
                        last_failure_class=$11,last_failure_code=$12,last_failure_retryable=$13,
                        updated_at=clock_timestamp()
                     WHERE run_id=$14 AND tool_task_id=$15 AND call_status IN ('claimed','running')
                       AND tool_attempt_no=$16 AND lease_epoch=$17 AND fencing_token=$18
                       AND claim_token=$19 AND projection_version=$20
                       AND claim_expires_at>clock_timestamp()
                       AND EXISTS(
                           SELECT 1 FROM model_tool_call_batches b
                           WHERE b.run_id=model_tool_calls.run_id
                             AND b.activation_id=model_tool_calls.activation_id
                             AND b.attempt_no=model_tool_calls.attempt_no
                             AND b.model_call_no=model_tool_calls.model_call_no
                             AND b.execution_status='active'
                             AND b.continuation_status='waiting_tools'
                             AND b.parent_operation_deadline IS NOT NULL
                             AND b.parent_operation_deadline>clock_timestamp()
                       )",
                )
                .bind(effect_evidence_str(*effect_evidence))
                .bind(failure_class_str(*class))
                .bind(code)
                .bind(*retryable)
                .bind(claim.claim_token())
                .bind(outcome_hash.as_str())
                .bind(
                    i32::try_from(claim.tool_attempt_no().get())
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .bind(i64_from_u64(claim.lease_epoch().get())?)
                .bind(claim.fencing_token())
                .bind(effect_evidence_str(*effect_evidence))
                .bind(failure_class_str(*class))
                .bind(code)
                .bind(*retryable)
                .bind(claim.run_id().as_str())
                .bind(claim.identity().tool_task_id().as_str())
                .bind(
                    i32::try_from(claim.tool_attempt_no().get())
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .bind(i64_from_u64(claim.lease_epoch().get())?)
                .bind(claim.fencing_token())
                .bind(claim.claim_token())
                .bind(
                    i64::try_from(claim.projection_version())
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .execute(&mut *tx)
                .await
                .map_err(RepositoryError::storage)?
                .rows_affected();
                if updated != 1 {
                    return finish_stale_model_tool_commit_postgres(tx, claim).await;
                }
                (ModelToolTaskDisposition::Failed, None)
            }
        }
        ModelToolTaskOutcome::Cancelled {
            code,
            effect_evidence,
        } => {
            if !matches!(
                status,
                ModelToolTaskStatus::Claimed | ModelToolTaskStatus::Running
            ) || !stored_evidence.can_transition_to(*effect_evidence)
            {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(ModelToolTaskTransitionOutcome::StateConflict);
            }
            let cancellation_class = if *effect_evidence == EffectEvidence::Unknown {
                ModelToolFailureClass::EffectOutcomeUnknown
            } else {
                ModelToolFailureClass::Safe
            };
            let updated = sqlx::query(
                "UPDATE model_tool_calls SET call_status='cancelled',effect_evidence=$1,
                    failure_class=$2,failure_code=$3,failure_retryable=FALSE,
                    completed_at=clock_timestamp(),projection_version=projection_version+1,
                    last_commit_claim_token=$4,last_outcome_hash=$5,
                    last_outcome_disposition='cancelled',last_outcome_attempt_no=$6,
                    last_outcome_lease_epoch=$7,last_outcome_fencing_token=$8,
                    last_outcome_available_at=NULL,last_effect_evidence=$9,
                    last_failure_class=$10,last_failure_code=$11,last_failure_retryable=FALSE,
                    updated_at=clock_timestamp()
                 WHERE run_id=$12 AND tool_task_id=$13 AND call_status IN ('claimed','running')
                   AND tool_attempt_no=$14 AND lease_epoch=$15 AND fencing_token=$16
                   AND claim_token=$17 AND projection_version=$18
                   AND claim_expires_at>clock_timestamp()
                   AND EXISTS(
                       SELECT 1 FROM model_tool_call_batches b
                       WHERE b.run_id=model_tool_calls.run_id
                         AND b.activation_id=model_tool_calls.activation_id
                         AND b.attempt_no=model_tool_calls.attempt_no
                         AND b.model_call_no=model_tool_calls.model_call_no
                         AND b.execution_status='active'
                         AND b.continuation_status='waiting_tools'
                         AND b.parent_operation_deadline IS NOT NULL
                         AND b.parent_operation_deadline>clock_timestamp()
                   )",
            )
            .bind(effect_evidence_str(*effect_evidence))
            .bind(failure_class_str(cancellation_class))
            .bind(code)
            .bind(claim.claim_token())
            .bind(outcome_hash.as_str())
            .bind(
                i32::try_from(claim.tool_attempt_no().get())
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .bind(i64_from_u64(claim.lease_epoch().get())?)
            .bind(claim.fencing_token())
            .bind(effect_evidence_str(*effect_evidence))
            .bind(failure_class_str(cancellation_class))
            .bind(code)
            .bind(claim.run_id().as_str())
            .bind(claim.identity().tool_task_id().as_str())
            .bind(
                i32::try_from(claim.tool_attempt_no().get())
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .bind(i64_from_u64(claim.lease_epoch().get())?)
            .bind(claim.fencing_token())
            .bind(claim.claim_token())
            .bind(
                i64::try_from(claim.projection_version())
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if updated != 1 {
                return finish_stale_model_tool_commit_postgres(tx, claim).await;
            }
            (ModelToolTaskDisposition::Cancelled, None)
        }
    };
    let continuation = if disposition == ModelToolTaskDisposition::RetryScheduled {
        ModelToolContinuationStatus::WaitingTools
    } else {
        finalize_batch_barrier_postgres(
            &mut tx,
            claim.run_id().as_str(),
            claim.parent_activation_id().as_str(),
            i32::try_from(claim.parent_attempt_no().get())
                .map_err(|_| RepositoryError::invalid_data())?,
            i32::try_from(claim.model_call_no()).map_err(|_| RepositoryError::invalid_data())?,
        )
        .await?
    };
    let duration_ms = if disposition == ModelToolTaskDisposition::RetryScheduled {
        None
    } else {
        let row = sqlx::query(
            "SELECT started_at,completed_at FROM model_tool_calls
             WHERE run_id=$1 AND tool_task_id=$2",
        )
        .bind(claim.run_id().as_str())
        .bind(claim.identity().tool_task_id().as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        model_tool_duration_ms(&row)?
    };
    let receipt = model_tool_task_commit_receipt_new(
        claim.identity().tool_task_id().clone(),
        disposition,
        claim.tool_attempt_no(),
        claim.lease_epoch(),
        next_available_at,
        continuation,
        duration_ms,
    )?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(ModelToolTaskTransitionOutcome::Committed(receipt))
}
