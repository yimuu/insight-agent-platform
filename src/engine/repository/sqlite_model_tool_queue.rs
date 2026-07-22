use super::RepositoryErrorExt as _;

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde_json::{json, Value};
use sqlx::{Row, Sqlite, Transaction};
use uuid::Uuid;

use crate::engine::{
    AttemptNo, ContentHash, EffectEvidence, EffectIdempotency, LeaseEpoch, ResponseItemAuthority,
    RunId, SchedulerTaskId, SchedulerTaskKind, WorkerCancellation, WorkerEffectPolicy,
};
use crate::runtime::{WorkflowToolPublicProjection, WorkflowToolResult};

use super::{
    common::{canonical_json, function_call_response_item_id, i64_from_u64, u64_from_i64},
    model_tool_queue::{
        deterministic_tool_identity, parse_action_from_stored_evidence,
        parse_frozen_model_tool_contract, validate_tool_arguments, validate_tool_result,
        FrozenModelToolAction, ModelToolBatchActivation, ModelToolBatchActivationOutcome,
        ModelToolContinuationStatus, ModelToolFailureClass, ModelToolTaskClaim,
        ModelToolTaskCommitReceipt, ModelToolTaskDisposition, ModelToolTaskHeartbeatOutcome,
        ModelToolTaskIdentity, ModelToolTaskOutcome, ModelToolTaskStatus,
        ModelToolTaskTransitionOutcome, StoredModelToolActionEvidence,
    },
    scheduler_repository::{
        DurableTaskExecutionRequest, SchedulerTaskClaim, SchedulerTaskClaimMode,
    },
    sqlite::{parse_run_timestamp, SqliteDurableRepository},
    RepositoryError,
};

const MAX_CLAIM_SECONDS: u32 = 3_600;
const MAX_CLAIM_LIMIT: u32 = 1_000;
const MODEL_TOOL_PARENT_DEADLINE_EXCEEDED: &str = "MODEL_TOOL_PARENT_DEADLINE_EXCEEDED";

fn now_text(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}

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
    tx: &mut Transaction<'_, Sqlite>,
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
                julianday(o.claim_expires_at)>julianday('now') AS claim_fresh,
                julianday(a.lease_expires_at)>julianday('now') AS lease_fresh
         FROM task_outbox o
         JOIN workflow_runs r ON r.run_id=o.run_id
         JOIN node_attempts a ON a.run_id=o.run_id AND a.activation_id=o.activation_id
           AND a.attempt_no=o.attempt_no AND a.lease_epoch=o.lease_epoch
           AND a.fencing_token=o.fencing_token
         JOIN node_activations v ON v.run_id=o.run_id AND v.activation_id=o.activation_id
         WHERE o.run_id=? AND o.task_id=? AND o.activation_id=? AND o.attempt_no=?
           AND o.lease_epoch=? AND o.fencing_token=?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.task_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(
        i64::try_from(claim.envelope().lease_epoch().get())
            .map_err(|_| RepositoryError::invalid_data())?,
    )
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
    let envelope: DurableTaskExecutionRequest = serde_json::from_str(
        &row.try_get::<String, _>("task_envelope")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let stored_expiry = parse_run_timestamp(
        &row.try_get::<String, _>("claim_expires_at")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let exact = envelope == *claim.envelope()
        && row.try_get::<String, _>("claimed_by").ok().as_deref() == Some(claim.claimed_by())
        && row.try_get::<String, _>("claim_token").ok().as_deref() == Some(claim.claim_token())
        && stored_expiry.timestamp_micros() == claim.claim_expires_at().timestamp_micros()
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
    if row.try_get::<i64, _>("claim_fresh").ok() != Some(1)
        || row.try_get::<i64, _>("lease_fresh").ok() != Some(1)
    {
        return Ok(ParentAuthority::Stale);
    }
    Ok(ParentAuthority::Exact)
}

async fn parent_operation_deadline_sqlite(
    tx: &mut Transaction<'_, Sqlite>,
    claim: &SchedulerTaskClaim,
) -> Result<String, RepositoryError> {
    let started_at = sqlx::query_scalar::<_, String>(
        "SELECT started_at FROM node_attempts
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND lease_epoch=?
           AND fencing_token=? AND lifecycle='running' AND effect_evidence='started'",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(
        i64::try_from(claim.envelope().lease_epoch().get())
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(claim.envelope().fencing_token())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let timeout_ms = i64::try_from(claim.envelope().request().effect_policy().timeout_ms())
        .map_err(|_| RepositoryError::invalid_configuration())?;
    let deadline = parse_run_timestamp(&started_at)?
        .checked_add_signed(Duration::milliseconds(timeout_ms))
        .ok_or_else(RepositoryError::invalid_configuration)?;
    Ok(now_text(deadline))
}

async fn validate_projected_tool_artifacts_sqlite(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    projected: Option<&WorkflowToolResult>,
) -> Result<(), RepositoryError> {
    let Some(projected) = projected else {
        return Ok(());
    };
    for artifact in projected
        .content()
        .iter()
        .filter_map(crate::runtime::WorkflowToolContent::artifact)
    {
        let exact = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                SELECT 1 FROM artifacts
                WHERE run_id=? AND artifact_id=? AND content_hash=? AND size_bytes=?
                  AND media_type IS ? AND artifact_state='referenced'
             )",
        )
        .bind(run_id.as_str())
        .bind(artifact.artifact_id().as_str())
        .bind(artifact.content_hash().as_str())
        .bind(i64::try_from(artifact.size_bytes()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(artifact.media_type())
        .fetch_one(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
        if exact != 1 {
            return Err(RepositoryError::invalid_data());
        }
    }
    Ok(())
}

fn decode_json_text(row: &sqlx::sqlite::SqliteRow, name: &str) -> Result<Value, RepositoryError> {
    serde_json::from_str(
        &row.try_get::<String, _>(name)
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())
}

fn decode_action(row: &sqlx::sqlite::SqliteRow) -> Result<FrozenModelToolAction, RepositoryError> {
    let effect_policy: WorkerEffectPolicy =
        serde_json::from_value(decode_json_text(row, "action_effect_policy")?)
            .map_err(|_| RepositoryError::invalid_data())?;
    parse_action_from_stored_evidence(StoredModelToolActionEvidence {
        name: row
            .try_get("tool_name")
            .map_err(|_| RepositoryError::invalid_data())?,
        action_id: row
            .try_get("action_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        action_version: row
            .try_get("action_version")
            .map_err(|_| RepositoryError::invalid_data())?,
        descriptor_hash: row
            .try_get("action_descriptor_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
        input_schema: decode_json_text(row, "action_input_schema")?,
        output_schema: decode_json_text(row, "action_output_schema")?,
        effect_policy,
        deployment_binding: decode_json_text(row, "action_deployment_binding")?,
        effective_public_policy: decode_json_text(row, "effective_public_policy")?,
    })
}

fn decode_public_item(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<Option<ResponseItemAuthority>, RepositoryError> {
    let item_id = row
        .try_get::<Option<String>, _>("response_item_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let output_index = row
        .try_get::<Option<i64>, _>("response_output_index")
        .map_err(|_| RepositoryError::invalid_data())?;
    match (item_id, output_index) {
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
    row: &sqlx::sqlite::SqliteRow,
    run_id: &RunId,
    activation_id: &crate::engine::ActivationId,
    attempt_no: AttemptNo,
    model_call_no: u32,
) -> Result<ModelToolTaskIdentity, RepositoryError> {
    let call_index = u32::try_from(
        row.try_get::<i64, _>("call_index")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let call_id: String = row
        .try_get("call_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let identity = deterministic_tool_identity(
        run_id,
        activation_id,
        attempt_no,
        model_call_no,
        call_index,
        &call_id,
        decode_action(row)?,
        decode_public_item(row)?,
        decode_public_item(row)?
            .map(|_| canonical_json(&decode_json_text(row, "arguments")?))
            .transpose()?,
        row.try_get::<Option<i64>, _>("response_seal_index")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(u64_from_i64)
            .transpose()?,
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
    tx: &mut Transaction<'_, Sqlite>,
    claim: &SchedulerTaskClaim,
    model_call_no: u32,
) -> Result<ModelToolBatchActivation, RepositoryError> {
    let rows = sqlx::query(
        "SELECT * FROM model_tool_calls
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
         ORDER BY call_index",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
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
    ModelToolBatchActivation::new(
        claim.run_id().clone(),
        claim.activation_id().clone(),
        claim.envelope().attempt_no(),
        model_call_no,
        tasks,
    )
}

pub(crate) async fn activate_model_tool_call_batch_sqlite(
    repository: &SqliteDurableRepository,
    claim: &SchedulerTaskClaim,
    model_call_no: u32,
) -> Result<ModelToolBatchActivationOutcome, RepositoryError> {
    if model_call_no == 0 {
        return Err(RepositoryError::invalid_configuration());
    }
    let _writer = repository.writer.lock().await;
    let mut tx = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let batch = sqlx::query(
        "SELECT execution_status,continuation_status,parent_task_id,parent_lease_epoch,
                parent_fencing_token,parent_claimed_by,parent_claim_token,parent_claim_expires_at,
                parent_task_projection_version,parent_operation_deadline
         FROM model_tool_call_batches
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
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
                == i64::try_from(claim.envelope().lease_epoch().get()).ok()
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
        expire_parent_operation_deadline_batch_sqlite(
            &mut tx,
            claim.run_id().as_str(),
            claim.activation_id().as_str(),
            i64::from(claim.envelope().attempt_no().get()),
            i64::from(model_call_no),
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
    let parent_operation_deadline = parent_operation_deadline_sqlite(&mut tx, claim).await?;
    let contract =
        parse_frozen_model_tool_contract(claim.envelope().request().deployment_binding())?;
    let prior_rounds = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM model_tool_call_batches
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no<=?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
    .fetch_one(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    if u32::try_from(prior_rounds).map_err(|_| RepositoryError::invalid_data())?
        > contract.max_rounds
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolBatchActivationOutcome::RoundLimitExceeded);
    }
    let total_calls = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM model_tool_calls
         WHERE run_id=? AND activation_id=? AND attempt_no=?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .fetch_one(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    if u32::try_from(total_calls).map_err(|_| RepositoryError::invalid_data())? > contract.max_calls
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolBatchActivationOutcome::CallLimitExceeded);
    }
    let rows = sqlx::query(
        "SELECT call_index,call_id,tool_name,arguments FROM model_tool_calls
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
         ORDER BY call_index",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
    .fetch_all(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    if rows.is_empty() {
        return Err(RepositoryError::invalid_data());
    }
    for (expected_index, row) in rows.iter().enumerate() {
        let call_index = u32::try_from(
            row.try_get::<i64, _>("call_index")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        if call_index != expected_index as u32 {
            return Err(RepositoryError::invalid_data());
        }
        let tool_name: String = row
            .try_get("tool_name")
            .map_err(|_| RepositoryError::invalid_data())?;
        let action = contract
            .tools
            .get(&tool_name)
            .cloned()
            .ok_or_else(RepositoryError::invalid_data)?;
        let arguments = decode_json_text(row, "arguments")?;
        validate_tool_arguments(&action, &arguments)?;
        let public_projection = WorkflowToolPublicProjection::from_frozen_effective_policy(
            action.effective_public_policy(),
        )
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
                 WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
                   AND item_ordinal=?",
            )
            .bind(claim.run_id().as_str())
            .bind(claim.activation_id().as_str())
            .bind(i64::from(claim.envelope().attempt_no().get()))
            .bind(i64::from(model_call_no))
            .bind(i64::from(call_index) + 1)
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
            let expected_safe_item = canonical_json(&json!({
                "id": expected_item_id,
                "type": "function_call",
                "status": "completed",
                "call_id": call_id,
                "name": tool_name,
                "arguments": arguments_jcs,
            }))?;
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
                    .try_get::<Option<String>, _>("safe_item")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .as_deref()
                    != Some(expected_safe_item.as_str())
            {
                return Err(RepositoryError::invalid_data());
            }
            (
                Some(
                    ResponseItemAuthority::new(
                        item_id,
                        u32::try_from(
                            item.try_get::<i64, _>("output_index")
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
            "UPDATE model_tool_calls SET tool_task_id=?,effect_id=?,action_id=?,action_version=?,
                action_descriptor_hash=?,action_input_schema=?,action_output_schema=?,
                action_effect_policy=?,action_deployment_binding=?,effective_public_policy=?,
                response_item_id=?,response_output_index=?,response_seal_index=?,
                effect_idempotency=?,cancellation=?,
                max_attempts=?,initial_backoff_ms=?,max_backoff_ms=?,timeout_ms=?,
                tool_attempt_no=1,lease_epoch=1,fencing_token=?,effect_evidence='not_started',
                available_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),projection_version=1,
                updated_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
               AND call_index=? AND call_status='pending' AND tool_task_id IS NULL
               AND projection_version=0",
        )
        .bind(identity.tool_task_id().as_str())
        .bind(identity.effect_id().as_str())
        .bind(action.action_id())
        .bind(action.action_version())
        .bind(action.descriptor_hash())
        .bind(canonical_json(action.input_schema())?)
        .bind(canonical_json(action.output_schema())?)
        .bind(canonical_json(
            &serde_json::to_value(policy).map_err(|_| RepositoryError::canonicalization())?,
        )?)
        .bind(canonical_json(action.deployment_binding())?)
        .bind(canonical_json(action.effective_public_policy())?)
        .bind(public_item.as_ref().map(ResponseItemAuthority::item_id))
        .bind(
            public_item
                .as_ref()
                .map(|item| i64::from(item.output_index())),
        )
        .bind(public_seal_index.map(i64_from_u64).transpose()?)
        .bind(match policy.effect_idempotency() {
            EffectIdempotency::Idempotent => "idempotent",
            EffectIdempotency::NonIdempotent => "non_idempotent",
        })
        .bind(match policy.cancellation() {
            WorkerCancellation::Cooperative => "cooperative",
            WorkerCancellation::LeaseOnly => "lease_only",
        })
        .bind(i64::from(policy.max_attempts()))
        .bind(
            i64::try_from(policy.initial_backoff_ms())
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .bind(i64::try_from(policy.max_backoff_ms()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(i64::try_from(policy.timeout_ms()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(tool_fencing_token(
            identity.tool_task_id(),
            AttemptNo::FIRST,
            LeaseEpoch::FIRST,
        ))
        .bind(claim.run_id().as_str())
        .bind(claim.activation_id().as_str())
        .bind(i64::from(claim.envelope().attempt_no().get()))
        .bind(i64::from(model_call_no))
        .bind(i64::from(call_index))
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
            continuation_status='waiting_tools',parent_task_id=?,parent_lease_epoch=?,
            parent_fencing_token=?,parent_claimed_by=?,parent_claim_token=?,
            parent_claim_expires_at=?,parent_task_projection_version=?,
            parent_operation_deadline=?,activated_at=CURRENT_TIMESTAMP,
            updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
           AND execution_status='checkpointed' AND continuation_status='checkpointed'",
    )
    .bind(claim.task_id().as_str())
    .bind(
        i64::try_from(claim.envelope().lease_epoch().get())
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(claim.envelope().fencing_token())
    .bind(claim.claimed_by())
    .bind(claim.claim_token())
    .bind(now_text(claim.claim_expires_at()))
    .bind(
        i64::try_from(claim.task_projection_version())
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(&parent_operation_deadline)
    .bind(claim.run_id().as_str())
    .bind(claim.activation_id().as_str())
    .bind(i64::from(claim.envelope().attempt_no().get()))
    .bind(i64::from(model_call_no))
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if activated != 1 {
        return Err(RepositoryError::invalid_data());
    }
    expire_parent_operation_deadline_batch_sqlite(
        &mut tx,
        claim.run_id().as_str(),
        claim.activation_id().as_str(),
        i64::from(claim.envelope().attempt_no().get()),
        i64::from(model_call_no),
    )
    .await?;
    let activation = load_activation(&mut tx, claim, model_call_no).await?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(ModelToolBatchActivationOutcome::Activated(activation))
}

fn decode_claim(row: &sqlx::sqlite::SqliteRow) -> Result<ModelToolTaskClaim, RepositoryError> {
    let run_id = RunId::new(
        row.try_get::<String, _>("run_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let activation_id = crate::engine::ActivationId::new(
        row.try_get::<String, _>("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let parent_attempt_no = AttemptNo::new(
        u32::try_from(
            row.try_get::<i64, _>("attempt_no")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let model_call_no = u32::try_from(
        row.try_get::<i64, _>("model_call_no")
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
    ModelToolTaskClaim::new(
        run_id,
        activation_id,
        parent_attempt_no,
        model_call_no,
        identity,
        decode_json_text(row, "arguments")?,
        AttemptNo::new(
            u32::try_from(
                row.try_get::<i64, _>("tool_attempt_no")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        LeaseEpoch::new(
            u64::try_from(
                row.try_get::<i64, _>("lease_epoch")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("fencing_token")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("claim_owner")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("claim_token")
            .map_err(|_| RepositoryError::invalid_data())?,
        parse_run_timestamp(
            &row.try_get::<String, _>("claim_expires_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        u64::try_from(
            row.try_get::<i64, _>("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
    )
}

/// Closes every executable model-tool member owned by a Run that has already
/// won a global terminal transition in the caller's transaction. This helper
/// deliberately does not use the normal batch barrier: terminal Run authority
/// must never make the parent LLM task runnable again.
pub(crate) async fn close_model_tool_work_for_terminal_run_sqlite(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
) -> Result<(), RepositoryError> {
    let terminal = sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS(
            SELECT 1 FROM workflow_runs
            WHERE run_id=? AND lifecycle IN
                ('succeeded','failed','cancelled','interrupted','timed_out')
         )",
    )
    .bind(run_id.as_str())
    .fetch_one(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    if terminal != 1 {
        return Err(RepositoryError::invalid_data());
    }

    // A started action may already have produced an external effect. Preserve
    // that uncertainty, invalidate the old worker fence, and clear its claim.
    sqlx::query(
        "UPDATE model_tool_calls SET call_status='failed',effect_evidence='unknown',
            failure_class='effect_outcome_unknown',
            failure_code='MODEL_TOOL_RUN_TERMINATED_EFFECT_UNKNOWN',failure_retryable=0,
            available_at=NULL,claim_owner=NULL,claim_token=NULL,claim_expires_at=NULL,
            lease_epoch=lease_epoch+1,
            fencing_token=fencing_token || ':run-terminal:' || CAST(projection_version+1 AS TEXT),
            completed_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
            updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND call_status='running'
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

    // Pending and claimed members have not crossed the action-start boundary,
    // so cancellation with not_started evidence is authoritative and safe.
    sqlx::query(
        "UPDATE model_tool_calls SET call_status='cancelled',effect_evidence='not_started',
            failure_class='safe',failure_code='MODEL_TOOL_RUN_TERMINATED',failure_retryable=0,
            available_at=NULL,claim_owner=NULL,claim_token=NULL,claim_expires_at=NULL,
            lease_epoch=lease_epoch+1,
            fencing_token=fencing_token || ':run-terminal:' || CAST(projection_version+1 AS TEXT),
            completed_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
            updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND call_status IN ('pending','claimed')
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

    // Close the barrier without waking the parent continuation. A batch with
    // any failed member is conservatively failed; otherwise it is cancelled.
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
             completed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND execution_status='active'
           AND continuation_status='waiting_tools'",
    )
    .bind(run_id.as_str())
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;

    let live = sqlx::query_scalar::<_, i64>(
        "SELECT
            (SELECT COUNT(*) FROM model_tool_call_batches
             WHERE run_id=? AND execution_status='active'
               AND continuation_status='waiting_tools')
          + (SELECT COUNT(*) FROM model_tool_calls c
             JOIN model_tool_call_batches b ON b.run_id=c.run_id
               AND b.activation_id=c.activation_id AND b.attempt_no=c.attempt_no
               AND b.model_call_no=c.model_call_no
             WHERE c.run_id=? AND b.execution_status IN ('active','failed','cancelled')
               AND c.call_status IN ('pending','claimed','running'))",
    )
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .fetch_one(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    if live != 0 {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn finalize_batch_barrier_sqlite(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &str,
    activation_id: &str,
    attempt_no: i64,
    model_call_no: i64,
) -> Result<ModelToolContinuationStatus, RepositoryError> {
    let mut counts = sqlx::query(
        "SELECT COUNT(*) AS total,
                SUM(CASE WHEN call_status='succeeded' THEN 1 ELSE 0 END) AS succeeded,
                SUM(CASE WHEN call_status='failed' THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN call_status='cancelled' THEN 1 ELSE 0 END) AS cancelled,
                SUM(CASE WHEN failure_class='effect_outcome_unknown' THEN 1 ELSE 0 END) AS unknown
         FROM model_tool_calls
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?",
    )
    .bind(run_id)
    .bind(activation_id)
    .bind(attempt_no)
    .bind(model_call_no)
    .fetch_one(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let mut failed: i64 = counts
        .try_get::<Option<i64>, _>("failed")
        .ok()
        .flatten()
        .unwrap_or(0);
    let mut cancelled: i64 = counts
        .try_get::<Option<i64>, _>("cancelled")
        .ok()
        .flatten()
        .unwrap_or(0);
    let mut unknown: i64 = counts
        .try_get::<Option<i64>, _>("unknown")
        .ok()
        .flatten()
        .unwrap_or(0);

    // A failed/cancelled member closes admission for the whole batch. Fence
    // every non-terminal sibling in this transaction before the parent can be
    // made runnable. Running work is conservatively recorded as an unknown
    // effect; work that never started is safely cancelled.
    if failed > 0 || cancelled > 0 {
        sqlx::query(
            "UPDATE model_tool_calls SET call_status='failed',effect_evidence='unknown',
                failure_class='effect_outcome_unknown',
                failure_code='MODEL_TOOL_SIBLING_EFFECT_UNKNOWN',failure_retryable=0,
                lease_epoch=lease_epoch+1,
                fencing_token=fencing_token || ':batch-abort:' || CAST(projection_version+1 AS TEXT),
                completed_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
                updated_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
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
                failure_code='MODEL_TOOL_SIBLING_ABORTED',failure_retryable=0,
                lease_epoch=lease_epoch+1,
                fencing_token=fencing_token || ':batch-abort:' || CAST(projection_version+1 AS TEXT),
                completed_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
                updated_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
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
                    SUM(CASE WHEN call_status='succeeded' THEN 1 ELSE 0 END) AS succeeded,
                    SUM(CASE WHEN call_status='failed' THEN 1 ELSE 0 END) AS failed,
                    SUM(CASE WHEN call_status='cancelled' THEN 1 ELSE 0 END) AS cancelled,
                    SUM(CASE WHEN failure_class='effect_outcome_unknown' THEN 1 ELSE 0 END) AS unknown
             FROM model_tool_calls
             WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?",
        )
        .bind(run_id)
        .bind(activation_id)
        .bind(attempt_no)
        .bind(model_call_no)
        .fetch_one(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
        failed = counts
            .try_get::<Option<i64>, _>("failed")
            .ok()
            .flatten()
            .unwrap_or(0);
        cancelled = counts
            .try_get::<Option<i64>, _>("cancelled")
            .ok()
            .flatten()
            .unwrap_or(0);
        unknown = counts
            .try_get::<Option<i64>, _>("unknown")
            .ok()
            .flatten()
            .unwrap_or(0);
    }
    let total: i64 = counts
        .try_get("total")
        .map_err(|_| RepositoryError::invalid_data())?;
    let succeeded: i64 = counts
        .try_get::<Option<i64>, _>("succeeded")
        .ok()
        .flatten()
        .unwrap_or(0);
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
        "UPDATE model_tool_call_batches SET execution_status=?,continuation_status=?,
                completed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
           AND execution_status='active' AND continuation_status='waiting_tools'",
    )
    .bind(execution)
    .bind(continuation.as_str())
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
             WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?",
        )
        .bind(run_id)
        .bind(activation_id)
        .bind(attempt_no)
        .bind(model_call_no)
        .fetch_one(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
        let parent_task_id: String = parent
            .try_get("parent_task_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let parent_claim_token: String = parent
            .try_get("parent_claim_token")
            .map_err(|_| RepositoryError::invalid_data())?;
        let parent_projection: i64 = parent
            .try_get("parent_task_projection_version")
            .map_err(|_| RepositoryError::invalid_data())?;
        let parent_rows = sqlx::query(
            "UPDATE task_outbox SET task_state='pending',available_at=CURRENT_TIMESTAMP,
                    claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL,claim_mode=NULL,
                    projection_version=projection_version+1
             WHERE run_id=? AND task_id=? AND task_state='claimed'
               AND claim_token=? AND projection_version=?",
        )
        .bind(run_id)
        .bind(parent_task_id)
        .bind(parent_claim_token)
        .bind(parent_projection)
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

async fn expire_parent_operation_deadline_batch_sqlite(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &str,
    activation_id: &str,
    attempt_no: i64,
    model_call_no: i64,
) -> Result<bool, RepositoryError> {
    let row = sqlx::query(
        "SELECT b.execution_status,b.continuation_status,b.parent_operation_deadline,
                r.lifecycle AS run_lifecycle
         FROM model_tool_call_batches b
         JOIN workflow_runs r ON r.run_id=b.run_id
         WHERE b.run_id=? AND b.activation_id=? AND b.attempt_no=? AND b.model_call_no=?",
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
    let deadline = row
        .try_get::<Option<String>, _>("parent_operation_deadline")
        .map_err(|_| RepositoryError::invalid_data())?;
    let elapsed = if let Some(deadline) = deadline {
        parse_run_timestamp(&deadline)?;
        sqlx::query_scalar::<_, i64>("SELECT julianday(?)<=julianday('now')")
            .bind(&deadline)
            .fetch_one(&mut **tx)
            .await
            .map_err(RepositoryError::storage)?
    } else {
        // Migration 020 can observe an already-active legacy batch. Missing
        // deadline authority is never permission to keep executing.
        1
    };
    if elapsed != 1 {
        return Ok(false);
    }

    sqlx::query(
        "UPDATE model_tool_calls SET call_status='failed',effect_evidence='unknown',
            failure_class='effect_outcome_unknown',failure_code=?,failure_retryable=0,
            available_at=NULL,claim_owner=NULL,claim_token=NULL,claim_expires_at=NULL,
            lease_epoch=lease_epoch+1,
            fencing_token=fencing_token || ':parent-deadline:' || CAST(projection_version+1 AS TEXT),
            completed_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
            updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
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
            failure_class='safe',failure_code=?,failure_retryable=0,available_at=NULL,
            claim_owner=NULL,claim_token=NULL,claim_expires_at=NULL,
            lease_epoch=lease_epoch+1,
            fencing_token=fencing_token || ':parent-deadline:' || CAST(projection_version+1 AS TEXT),
            completed_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
            updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
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

    if finalize_batch_barrier_sqlite(tx, run_id, activation_id, attempt_no, model_call_no).await?
        == ModelToolContinuationStatus::WaitingTools
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(true)
}

async fn expire_parent_operation_deadlines_sqlite(
    tx: &mut Transaction<'_, Sqlite>,
) -> Result<(), RepositoryError> {
    let batches = sqlx::query(
        "SELECT b.run_id,b.activation_id,b.attempt_no,b.model_call_no
         FROM model_tool_call_batches b
         JOIN workflow_runs r ON r.run_id=b.run_id
         WHERE b.execution_status='active' AND b.continuation_status='waiting_tools'
           AND r.lifecycle NOT IN ('succeeded','failed','cancelled','interrupted','timed_out')
           AND (b.parent_operation_deadline IS NULL
                OR julianday(b.parent_operation_deadline)<=julianday('now'))
         ORDER BY b.run_id,b.activation_id,b.attempt_no,b.model_call_no",
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    for batch in batches {
        expire_parent_operation_deadline_batch_sqlite(
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

async fn recover_expired_model_tool_calls_sqlite(
    tx: &mut Transaction<'_, Sqlite>,
) -> Result<(), RepositoryError> {
    expire_parent_operation_deadlines_sqlite(tx).await?;
    let expired = sqlx::query(
        "SELECT c.run_id,c.activation_id,c.attempt_no,c.model_call_no,c.call_index,
                c.tool_task_id,c.call_status,c.tool_attempt_no,c.lease_epoch,
                c.effect_idempotency,c.max_attempts,c.initial_backoff_ms,c.max_backoff_ms
         FROM model_tool_calls c
         JOIN model_tool_call_batches b ON b.run_id=c.run_id
           AND b.activation_id=c.activation_id AND b.attempt_no=c.attempt_no
           AND b.model_call_no=c.model_call_no
         JOIN workflow_runs r ON r.run_id=c.run_id
         WHERE b.execution_status='active' AND b.continuation_status='waiting_tools'
           AND r.lifecycle NOT IN ('succeeded','failed','cancelled','interrupted','timed_out')
           AND c.call_status IN ('claimed','running')
           AND julianday(c.claim_expires_at)<=julianday('now')
         ORDER BY c.run_id,c.tool_task_id",
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    for row in expired {
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
                row.try_get::<i64, _>("tool_attempt_no")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let lease_epoch = LeaseEpoch::new(
            u64::try_from(
                row.try_get::<i64, _>("lease_epoch")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let max_attempts = u32::try_from(
            row.try_get::<i64, _>("max_attempts")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let idempotent = row
            .try_get::<String, _>("effect_idempotency")
            .map_err(|_| RepositoryError::invalid_data())?
            == "idempotent";
        let run_id: String = row
            .try_get("run_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let activation_id: String = row
            .try_get("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let parent_attempt: i64 = row
            .try_get("attempt_no")
            .map_err(|_| RepositoryError::invalid_data())?;
        let model_call_no: i64 = row
            .try_get("model_call_no")
            .map_err(|_| RepositoryError::invalid_data())?;
        let call_index: i64 = row
            .try_get("call_index")
            .map_err(|_| RepositoryError::invalid_data())?;
        let batch_still_active = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                SELECT 1 FROM model_tool_call_batches b
                JOIN workflow_runs r ON r.run_id=b.run_id
                WHERE b.run_id=? AND b.activation_id=? AND b.attempt_no=? AND b.model_call_no=?
                  AND b.execution_status='active' AND b.continuation_status='waiting_tools'
                  AND r.lifecycle NOT IN
                      ('succeeded','failed','cancelled','interrupted','timed_out')
             )",
        )
        .bind(&run_id)
        .bind(&activation_id)
        .bind(parent_attempt)
        .bind(model_call_no)
        .fetch_one(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
        if batch_still_active != 1 {
            continue;
        }
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
                let initial = u64::try_from(
                    row.try_get::<i64, _>("initial_backoff_ms")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?;
                let maximum = u64::try_from(
                    row.try_get::<i64, _>("max_backoff_ms")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?;
                initial
                    .saturating_mul(1_u64 << attempt_no.get().saturating_sub(1).min(63))
                    .min(maximum)
            } else {
                0
            };
            let available_at = sqlx::query_scalar::<_, String>(
                "SELECT strftime('%Y-%m-%dT%H:%M:%fZ',julianday('now')+CAST(? AS REAL)/86400000.0)",
            )
            .bind(i64::try_from(delay_ms).map_err(|_| RepositoryError::invalid_data())?)
            .fetch_one(&mut **tx)
            .await
            .map_err(RepositoryError::storage)?;
            let rows_updated = sqlx::query(
                "UPDATE model_tool_calls SET call_status='pending',tool_attempt_no=?,lease_epoch=?,
                    fencing_token=?,effect_evidence='not_started',available_at=?,
                    claim_owner=NULL,claim_token=NULL,claim_expires_at=NULL,started_at=NULL,
                    projection_version=projection_version+1,lease_loss_count=lease_loss_count+1,
                    last_lease_loss_at=CURRENT_TIMESTAMP,last_lease_loss_evidence=?,
                    updated_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
                   AND call_index=? AND call_status=? AND lease_epoch=?",
            )
            .bind(i64::from(next_attempt.get()))
            .bind(i64::try_from(next_lease.get()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(tool_fencing_token(&task_id, next_attempt, next_lease))
            .bind(available_at)
            .bind(evidence)
            .bind(&run_id)
            .bind(&activation_id)
            .bind(parent_attempt)
            .bind(model_call_no)
            .bind(call_index)
            .bind(&status)
            .bind(i64::try_from(lease_epoch.get()).map_err(|_| RepositoryError::invalid_data())?)
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
                    failure_retryable=0,completed_at=CURRENT_TIMESTAMP,
                    projection_version=projection_version+1,lease_loss_count=lease_loss_count+1,
                    last_lease_loss_at=CURRENT_TIMESTAMP,last_lease_loss_evidence='unknown',
                    updated_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND activation_id=? AND attempt_no=? AND model_call_no=?
                   AND call_index=? AND call_status='running' AND lease_epoch=?",
            )
            .bind(&run_id)
            .bind(&activation_id)
            .bind(parent_attempt)
            .bind(model_call_no)
            .bind(call_index)
            .bind(i64::try_from(lease_epoch.get()).map_err(|_| RepositoryError::invalid_data())?)
            .execute(&mut **tx)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows_updated != 1 {
                return Err(RepositoryError::invalid_data());
            }
            finalize_batch_barrier_sqlite(
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

pub(crate) async fn claim_model_tool_calls_sqlite(
    repository: &SqliteDurableRepository,
    claimed_by: &str,
    claim_seconds: u32,
    limit: u32,
    max_claimed_per_run: u32,
) -> Result<Vec<ModelToolTaskClaim>, RepositoryError> {
    if !valid_claim_parameters(claimed_by, claim_seconds, limit, max_claimed_per_run) {
        return Err(RepositoryError::invalid_configuration());
    }
    let _writer = repository.writer.lock().await;
    let mut tx = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    recover_expired_model_tool_calls_sqlite(&mut tx).await?;
    expire_parent_operation_deadlines_sqlite(&mut tx).await?;
    let candidates = sqlx::query(
        "SELECT c.run_id,c.activation_id,c.attempt_no,c.model_call_no,c.tool_task_id
         FROM model_tool_calls c
         JOIN model_tool_call_batches b ON b.run_id=c.run_id
           AND b.activation_id=c.activation_id AND b.attempt_no=c.attempt_no
           AND b.model_call_no=c.model_call_no
         JOIN workflow_runs r ON r.run_id=c.run_id
         WHERE c.call_status='pending' AND julianday(c.available_at)<=julianday('now')
           AND b.execution_status='active' AND b.continuation_status='waiting_tools'
           AND r.lifecycle IN ('created','active','waiting')
         ORDER BY c.available_at,c.run_id,c.activation_id,c.attempt_no,
                  c.model_call_no,c.call_index",
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let mut active_by_run = BTreeMap::<String, u32>::new();
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
            let value = u32::try_from(
                sqlx::query_scalar::<_, i64>(
                    "SELECT
                        (SELECT COUNT(*) FROM model_tool_calls
                         WHERE run_id=? AND call_status IN ('claimed','running')
                           AND julianday(claim_expires_at)>julianday('now'))
                        +
                        (SELECT COUNT(*) FROM task_outbox o
                         WHERE o.run_id=? AND o.task_state='claimed'
                           AND julianday(o.claim_expires_at)>julianday('now')
                           AND NOT EXISTS (
                               SELECT 1 FROM model_tool_call_batches b
                               WHERE b.run_id=o.run_id AND b.parent_task_id=o.task_id
                                 AND b.execution_status='active'
                                 AND b.continuation_status='waiting_tools'
                           ))",
                )
                .bind(&run_id)
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
        let parent_attempt: i64 = candidate
            .try_get("attempt_no")
            .map_err(|_| RepositoryError::invalid_data())?;
        let model_call_no: i64 = candidate
            .try_get("model_call_no")
            .map_err(|_| RepositoryError::invalid_data())?;
        if expire_parent_operation_deadline_batch_sqlite(
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
            "UPDATE model_tool_calls SET call_status='claimed',claim_owner=?,claim_token=?,
                    claim_expires_at=strftime('%Y-%m-%dT%H:%M:%fZ','now',printf('+%d seconds',?)),
                    available_at=NULL,projection_version=projection_version+1,
                    updated_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND tool_task_id=? AND call_status='pending'
               AND julianday(available_at)<=julianday('now')
               AND EXISTS (
                   SELECT 1 FROM model_tool_call_batches b
                   WHERE b.run_id=model_tool_calls.run_id
                     AND b.activation_id=model_tool_calls.activation_id
                     AND b.attempt_no=model_tool_calls.attempt_no
                     AND b.model_call_no=model_tool_calls.model_call_no
                     AND b.execution_status='active'
                     AND b.continuation_status='waiting_tools'
                     AND b.parent_operation_deadline IS NOT NULL
                     AND julianday(b.parent_operation_deadline)>julianday('now')
               )
               AND EXISTS (
                   SELECT 1 FROM workflow_runs r
                   WHERE r.run_id=model_tool_calls.run_id
                     AND r.lifecycle NOT IN
                         ('succeeded','failed','cancelled','interrupted','timed_out')
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
            expire_parent_operation_deadline_batch_sqlite(
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

fn same_current_claim(row: &sqlx::sqlite::SqliteRow, claim: &ModelToolTaskClaim) -> bool {
    row.try_get::<String, _>("run_id").ok().as_deref() == Some(claim.run_id().as_str())
        && row.try_get::<String, _>("tool_task_id").ok().as_deref()
            == Some(claim.identity().tool_task_id().as_str())
        && row.try_get::<i64, _>("tool_attempt_no").ok()
            == Some(i64::from(claim.tool_attempt_no().get()))
        && row.try_get::<i64, _>("lease_epoch").ok()
            == i64::try_from(claim.lease_epoch().get()).ok()
        && row.try_get::<String, _>("fencing_token").ok().as_deref() == Some(claim.fencing_token())
        && row
            .try_get::<Option<String>, _>("claim_owner")
            .ok()
            .flatten()
            .as_deref()
            == Some(claim.claimed_by())
        && row
            .try_get::<Option<String>, _>("claim_token")
            .ok()
            .flatten()
            .as_deref()
            == Some(claim.claim_token())
        && row.try_get::<i64, _>("projection_version").ok()
            == i64::try_from(claim.projection_version()).ok()
        && row
            .try_get::<Option<String>, _>("claim_expires_at")
            .ok()
            .flatten()
            .and_then(|value| parse_run_timestamp(&value).ok())
            .is_some_and(|value| {
                value.timestamp_micros() == claim.claim_expires_at().timestamp_micros()
            })
}

async fn load_tool_row_sqlite(
    tx: &mut Transaction<'_, Sqlite>,
    claim: &ModelToolTaskClaim,
) -> Result<Option<sqlx::sqlite::SqliteRow>, RepositoryError> {
    sqlx::query(
        "SELECT c.*,b.continuation_status,r.lifecycle AS run_lifecycle,
                julianday(c.claim_expires_at)>julianday('now') AS claim_fresh
         FROM model_tool_calls c
         JOIN model_tool_call_batches b ON b.run_id=c.run_id
           AND b.activation_id=c.activation_id AND b.attempt_no=c.attempt_no
           AND b.model_call_no=c.model_call_no
         JOIN workflow_runs r ON r.run_id=c.run_id
         WHERE c.run_id=? AND c.tool_task_id=?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.identity().tool_task_id().as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)
}

fn row_run_is_terminal(row: &sqlx::sqlite::SqliteRow) -> Result<bool, RepositoryError> {
    Ok(matches!(
        row.try_get::<String, _>("run_lifecycle")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_str(),
        "succeeded" | "failed" | "cancelled" | "interrupted" | "timed_out"
    ))
}

pub(crate) async fn mark_model_tool_call_started_sqlite(
    repository: &SqliteDurableRepository,
    claim: &ModelToolTaskClaim,
) -> Result<ModelToolTaskTransitionOutcome<()>, RepositoryError> {
    let _writer = repository.writer.lock().await;
    let mut tx = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    if expire_parent_operation_deadline_batch_sqlite(
        &mut tx,
        claim.run_id().as_str(),
        claim.parent_activation_id().as_str(),
        i64::from(claim.parent_attempt_no().get()),
        i64::from(claim.model_call_no()),
    )
    .await?
    {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    }
    let Some(row) = load_tool_row_sqlite(&mut tx, claim).await? else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    };
    if !same_current_claim(&row, claim) {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    }
    let status = ModelToolTaskStatus::parse(
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
    if status != ModelToolTaskStatus::Claimed {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StateConflict);
    }
    if row.try_get::<i64, _>("claim_fresh").ok() != Some(1) {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    }
    let rows = sqlx::query(
        "UPDATE model_tool_calls SET call_status='running',effect_evidence='started',
                started_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND tool_task_id=? AND call_status='claimed'
           AND tool_attempt_no=? AND lease_epoch=? AND fencing_token=?
           AND claim_owner=? AND claim_token=? AND projection_version=?
           AND julianday(claim_expires_at)>julianday('now')
           AND EXISTS (
               SELECT 1 FROM model_tool_call_batches b
               WHERE b.run_id=model_tool_calls.run_id
                 AND b.activation_id=model_tool_calls.activation_id
                 AND b.attempt_no=model_tool_calls.attempt_no
                 AND b.model_call_no=model_tool_calls.model_call_no
                 AND b.execution_status='active'
                 AND b.continuation_status='waiting_tools'
                 AND b.parent_operation_deadline IS NOT NULL
                 AND julianday(b.parent_operation_deadline)>julianday('now')
           )",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.identity().tool_task_id().as_str())
    .bind(i64::from(claim.tool_attempt_no().get()))
    .bind(i64::try_from(claim.lease_epoch().get()).map_err(|_| RepositoryError::invalid_data())?)
    .bind(claim.fencing_token())
    .bind(claim.claimed_by())
    .bind(claim.claim_token())
    .bind(i64::try_from(claim.projection_version()).map_err(|_| RepositoryError::invalid_data())?)
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        if expire_parent_operation_deadline_batch_sqlite(
            &mut tx,
            claim.run_id().as_str(),
            claim.parent_activation_id().as_str(),
            i64::from(claim.parent_attempt_no().get()),
            i64::from(claim.model_call_no()),
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

pub(crate) async fn heartbeat_model_tool_call_sqlite(
    repository: &SqliteDurableRepository,
    claim: &ModelToolTaskClaim,
    claim_seconds: u32,
) -> Result<ModelToolTaskHeartbeatOutcome, RepositoryError> {
    if !(3..=MAX_CLAIM_SECONDS).contains(&claim_seconds) {
        return Err(RepositoryError::invalid_configuration());
    }
    let _writer = repository.writer.lock().await;
    let mut tx = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    if expire_parent_operation_deadline_batch_sqlite(
        &mut tx,
        claim.run_id().as_str(),
        claim.parent_activation_id().as_str(),
        i64::from(claim.parent_attempt_no().get()),
        i64::from(claim.model_call_no()),
    )
    .await?
    {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskHeartbeatOutcome::StaleLease);
    }
    let Some(row) = load_tool_row_sqlite(&mut tx, claim).await? else {
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
    let status = ModelToolTaskStatus::parse(
        &row.try_get::<String, _>("call_status")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    if !matches!(
        status,
        ModelToolTaskStatus::Claimed | ModelToolTaskStatus::Running
    ) {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskHeartbeatOutcome::StateConflict);
    }
    if row.try_get::<i64, _>("claim_fresh").ok() != Some(1) {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskHeartbeatOutcome::StaleLease);
    }
    let renewed = sqlx::query(
        "UPDATE model_tool_calls SET
            claim_expires_at=strftime('%Y-%m-%dT%H:%M:%fZ','now',printf('+%d seconds',?)),
            projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND tool_task_id=? AND call_status IN ('claimed','running')
           AND tool_attempt_no=? AND lease_epoch=? AND fencing_token=?
           AND claim_owner=? AND claim_token=? AND projection_version=?
           AND julianday(claim_expires_at)>julianday('now')
           AND EXISTS (
               SELECT 1 FROM model_tool_call_batches b
               WHERE b.run_id=model_tool_calls.run_id
                 AND b.activation_id=model_tool_calls.activation_id
                 AND b.attempt_no=model_tool_calls.attempt_no
                 AND b.model_call_no=model_tool_calls.model_call_no
                 AND b.execution_status='active'
                 AND b.continuation_status='waiting_tools'
                 AND b.parent_operation_deadline IS NOT NULL
                 AND julianday(b.parent_operation_deadline)>julianday('now')
           )
         RETURNING *",
    )
    .bind(i64::from(claim_seconds))
    .bind(claim.run_id().as_str())
    .bind(claim.identity().tool_task_id().as_str())
    .bind(i64::from(claim.tool_attempt_no().get()))
    .bind(i64::try_from(claim.lease_epoch().get()).map_err(|_| RepositoryError::invalid_data())?)
    .bind(claim.fencing_token())
    .bind(claim.claimed_by())
    .bind(claim.claim_token())
    .bind(i64::try_from(claim.projection_version()).map_err(|_| RepositoryError::invalid_data())?)
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(renewed) = renewed else {
        if expire_parent_operation_deadline_batch_sqlite(
            &mut tx,
            claim.run_id().as_str(),
            claim.parent_activation_id().as_str(),
            i64::from(claim.parent_attempt_no().get()),
            i64::from(claim.model_call_no()),
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

fn decode_last_receipt(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<ModelToolTaskCommitReceipt, RepositoryError> {
    let task_id = SchedulerTaskId::parse(
        row.try_get::<String, _>("tool_task_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let disposition = ModelToolTaskDisposition::parse(
        &row.try_get::<String, _>("last_outcome_disposition")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let attempt_no = AttemptNo::new(
        u32::try_from(
            row.try_get::<i64, _>("last_outcome_attempt_no")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let lease_epoch = LeaseEpoch::new(
        u64::try_from(
            row.try_get::<i64, _>("last_outcome_lease_epoch")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let available = row
        .try_get::<Option<String>, _>("last_outcome_available_at")
        .map_err(|_| RepositoryError::invalid_data())?
        .map(|value| parse_run_timestamp(&value))
        .transpose()?;
    let continuation = ModelToolContinuationStatus::parse(
        &row.try_get::<String, _>("continuation_status")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    ModelToolTaskCommitReceipt::new(
        task_id,
        disposition,
        attempt_no,
        lease_epoch,
        available,
        continuation,
    )
}

async fn finish_stale_model_tool_commit_sqlite(
    mut tx: Transaction<'_, Sqlite>,
    claim: &ModelToolTaskClaim,
) -> Result<ModelToolTaskTransitionOutcome<ModelToolTaskCommitReceipt>, RepositoryError> {
    let expired = expire_parent_operation_deadline_batch_sqlite(
        &mut tx,
        claim.run_id().as_str(),
        claim.parent_activation_id().as_str(),
        i64::from(claim.parent_attempt_no().get()),
        i64::from(claim.model_call_no()),
    )
    .await?;
    if expired {
        tx.commit().await.map_err(RepositoryError::storage)?;
    } else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
    }
    Ok(ModelToolTaskTransitionOutcome::StaleLease)
}

pub(crate) async fn commit_model_tool_call_outcome_sqlite(
    repository: &SqliteDurableRepository,
    claim: &ModelToolTaskClaim,
    outcome: &ModelToolTaskOutcome,
) -> Result<ModelToolTaskTransitionOutcome<ModelToolTaskCommitReceipt>, RepositoryError> {
    let outcome_hash = outcome.canonical_hash()?;
    let _writer = repository.writer.lock().await;
    let mut tx = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let parent_deadline_elapsed = expire_parent_operation_deadline_batch_sqlite(
        &mut tx,
        claim.run_id().as_str(),
        claim.parent_activation_id().as_str(),
        i64::from(claim.parent_attempt_no().get()),
        i64::from(claim.model_call_no()),
    )
    .await?;
    let Some(row) = load_tool_row_sqlite(&mut tx, claim).await? else {
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
                .try_get::<Option<i64>, _>("last_outcome_attempt_no")
                .map_err(|_| RepositoryError::invalid_data())?
                == Some(i64::from(claim.tool_attempt_no().get()))
            && row
                .try_get::<Option<i64>, _>("last_outcome_lease_epoch")
                .map_err(|_| RepositoryError::invalid_data())?
                == i64::try_from(claim.lease_epoch().get()).ok()
            && row
                .try_get::<Option<String>, _>("last_outcome_fencing_token")
                .map_err(|_| RepositoryError::invalid_data())?
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
    if row.try_get::<i64, _>("claim_fresh").ok() != Some(1) {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(ModelToolTaskTransitionOutcome::StaleLease);
    }
    let status = ModelToolTaskStatus::parse(
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
    let policy: WorkerEffectPolicy =
        serde_json::from_value(decode_json_text(&row, "action_effect_policy")?)
            .map_err(|_| RepositoryError::invalid_data())?;
    let action = decode_action(&row)?;
    let (disposition, next_available_at) = match outcome {
        ModelToolTaskOutcome::Succeeded { result } => {
            if status != ModelToolTaskStatus::Running {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(ModelToolTaskTransitionOutcome::StateConflict);
            }
            validate_tool_result(&action, result)?;
            let public_result = WorkflowToolPublicProjection::from_frozen_effective_policy(
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
            validate_projected_tool_artifacts_sqlite(
                &mut tx,
                claim.run_id(),
                public_result.as_ref(),
            )
            .await?;
            let result_json = canonical_json(result)?;
            let updated = sqlx::query(
                "UPDATE model_tool_calls SET call_status='succeeded',effect_evidence='committed',
                    result_json=?,completed_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
                    last_commit_claim_token=?,last_outcome_hash=?,last_outcome_disposition='succeeded',
                    last_outcome_attempt_no=?,last_outcome_lease_epoch=?,
                    last_outcome_fencing_token=?,last_outcome_available_at=NULL,
                    last_effect_evidence='committed',updated_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND tool_task_id=? AND call_status='running'
                   AND tool_attempt_no=? AND lease_epoch=? AND fencing_token=?
                   AND claim_token=? AND projection_version=?
                   AND julianday(claim_expires_at)>julianday('now')
                   AND EXISTS(
                       SELECT 1 FROM model_tool_call_batches b
                       WHERE b.run_id=model_tool_calls.run_id
                         AND b.activation_id=model_tool_calls.activation_id
                         AND b.attempt_no=model_tool_calls.attempt_no
                         AND b.model_call_no=model_tool_calls.model_call_no
                         AND b.execution_status='active'
                         AND b.continuation_status='waiting_tools'
                         AND b.parent_operation_deadline IS NOT NULL
                         AND julianday(b.parent_operation_deadline)>julianday('now')
                   )",
            )
            .bind(result_json)
            .bind(claim.claim_token())
            .bind(outcome_hash.as_str())
            .bind(i64::from(claim.tool_attempt_no().get()))
            .bind(i64::try_from(claim.lease_epoch().get()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(claim.fencing_token())
            .bind(claim.run_id().as_str())
            .bind(claim.identity().tool_task_id().as_str())
            .bind(i64::from(claim.tool_attempt_no().get()))
            .bind(i64::try_from(claim.lease_epoch().get()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(claim.fencing_token())
            .bind(claim.claim_token())
            .bind(i64::try_from(claim.projection_version()).map_err(|_| RepositoryError::invalid_data())?)
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if updated != 1 {
                return finish_stale_model_tool_commit_sqlite(tx, claim).await;
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
                let next_available = sqlx::query_scalar::<_, String>(
                    "SELECT strftime('%Y-%m-%dT%H:%M:%fZ',julianday('now')+CAST(? AS REAL)/86400000.0)",
                )
                .bind(i64::try_from(delay_ms).map_err(|_| RepositoryError::invalid_data())?)
                .fetch_one(&mut *tx)
                .await
                .map_err(RepositoryError::storage)?;
                let updated = sqlx::query(
                    "UPDATE model_tool_calls SET call_status='pending',tool_attempt_no=?,lease_epoch=?,
                        fencing_token=?,effect_evidence='not_started',available_at=?,
                        claim_owner=NULL,claim_token=NULL,claim_expires_at=NULL,started_at=NULL,
                        projection_version=projection_version+1,last_commit_claim_token=?,
                        last_outcome_hash=?,last_outcome_disposition='retry_scheduled',
                        last_outcome_attempt_no=?,last_outcome_lease_epoch=?,
                        last_outcome_fencing_token=?,last_outcome_available_at=?,
                        last_effect_evidence=?,last_failure_class=?,last_failure_code=?,
                        last_failure_retryable=?,updated_at=CURRENT_TIMESTAMP
                     WHERE run_id=? AND tool_task_id=? AND call_status IN ('claimed','running')
                       AND tool_attempt_no=? AND lease_epoch=? AND fencing_token=?
                       AND claim_token=? AND projection_version=?
                       AND julianday(claim_expires_at)>julianday('now')
                       AND EXISTS(
                           SELECT 1 FROM model_tool_call_batches b
                           WHERE b.run_id=model_tool_calls.run_id
                             AND b.activation_id=model_tool_calls.activation_id
                             AND b.attempt_no=model_tool_calls.attempt_no
                             AND b.model_call_no=model_tool_calls.model_call_no
                             AND b.execution_status='active'
                             AND b.continuation_status='waiting_tools'
                             AND b.parent_operation_deadline IS NOT NULL
                             AND julianday(b.parent_operation_deadline)>julianday('now')
                       )",
                )
                .bind(i64::from(next_attempt.get()))
                .bind(i64::try_from(next_lease.get()).map_err(|_| RepositoryError::invalid_data())?)
                .bind(tool_fencing_token(claim.identity().tool_task_id(), next_attempt, next_lease))
                .bind(&next_available)
                .bind(claim.claim_token())
                .bind(outcome_hash.as_str())
                .bind(i64::from(claim.tool_attempt_no().get()))
                .bind(i64::try_from(claim.lease_epoch().get()).map_err(|_| RepositoryError::invalid_data())?)
                .bind(claim.fencing_token())
                .bind(&next_available)
                .bind(effect_evidence_str(*effect_evidence))
                .bind(failure_class_str(*class))
                .bind(code)
                .bind(if *retryable { 1_i64 } else { 0_i64 })
                .bind(claim.run_id().as_str())
                .bind(claim.identity().tool_task_id().as_str())
                .bind(i64::from(claim.tool_attempt_no().get()))
                .bind(i64::try_from(claim.lease_epoch().get()).map_err(|_| RepositoryError::invalid_data())?)
                .bind(claim.fencing_token())
                .bind(claim.claim_token())
                .bind(i64::try_from(claim.projection_version()).map_err(|_| RepositoryError::invalid_data())?)
                .execute(&mut *tx)
                .await
                .map_err(RepositoryError::storage)?
                .rows_affected();
                if updated != 1 {
                    return finish_stale_model_tool_commit_sqlite(tx, claim).await;
                }
                (
                    ModelToolTaskDisposition::RetryScheduled,
                    Some(parse_run_timestamp(&next_available)?),
                )
            } else {
                let updated = sqlx::query(
                    "UPDATE model_tool_calls SET call_status='failed',effect_evidence=?,
                        failure_class=?,failure_code=?,failure_retryable=?,
                        completed_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
                        last_commit_claim_token=?,last_outcome_hash=?,
                        last_outcome_disposition='failed',last_outcome_attempt_no=?,
                        last_outcome_lease_epoch=?,last_outcome_fencing_token=?,
                        last_outcome_available_at=NULL,last_effect_evidence=?,
                        last_failure_class=?,last_failure_code=?,last_failure_retryable=?,
                        updated_at=CURRENT_TIMESTAMP
                     WHERE run_id=? AND tool_task_id=? AND call_status IN ('claimed','running')
                       AND tool_attempt_no=? AND lease_epoch=? AND fencing_token=?
                       AND claim_token=? AND projection_version=?
                       AND julianday(claim_expires_at)>julianday('now')
                       AND EXISTS(
                           SELECT 1 FROM model_tool_call_batches b
                           WHERE b.run_id=model_tool_calls.run_id
                             AND b.activation_id=model_tool_calls.activation_id
                             AND b.attempt_no=model_tool_calls.attempt_no
                             AND b.model_call_no=model_tool_calls.model_call_no
                             AND b.execution_status='active'
                             AND b.continuation_status='waiting_tools'
                             AND b.parent_operation_deadline IS NOT NULL
                             AND julianday(b.parent_operation_deadline)>julianday('now')
                       )",
                )
                .bind(effect_evidence_str(*effect_evidence))
                .bind(failure_class_str(*class))
                .bind(code)
                .bind(if *retryable { 1_i64 } else { 0_i64 })
                .bind(claim.claim_token())
                .bind(outcome_hash.as_str())
                .bind(i64::from(claim.tool_attempt_no().get()))
                .bind(
                    i64::try_from(claim.lease_epoch().get())
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .bind(claim.fencing_token())
                .bind(effect_evidence_str(*effect_evidence))
                .bind(failure_class_str(*class))
                .bind(code)
                .bind(if *retryable { 1_i64 } else { 0_i64 })
                .bind(claim.run_id().as_str())
                .bind(claim.identity().tool_task_id().as_str())
                .bind(i64::from(claim.tool_attempt_no().get()))
                .bind(
                    i64::try_from(claim.lease_epoch().get())
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
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
                    return finish_stale_model_tool_commit_sqlite(tx, claim).await;
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
                "UPDATE model_tool_calls SET call_status='cancelled',effect_evidence=?,
                    failure_class=?,failure_code=?,failure_retryable=0,
                    completed_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
                    last_commit_claim_token=?,last_outcome_hash=?,
                    last_outcome_disposition='cancelled',last_outcome_attempt_no=?,
                    last_outcome_lease_epoch=?,last_outcome_fencing_token=?,
                    last_outcome_available_at=NULL,last_effect_evidence=?,
                    last_failure_class=?,last_failure_code=?,last_failure_retryable=0,
                    updated_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND tool_task_id=? AND call_status IN ('claimed','running')
                   AND tool_attempt_no=? AND lease_epoch=? AND fencing_token=?
                   AND claim_token=? AND projection_version=?
                   AND julianday(claim_expires_at)>julianday('now')
                   AND EXISTS(
                       SELECT 1 FROM model_tool_call_batches b
                       WHERE b.run_id=model_tool_calls.run_id
                         AND b.activation_id=model_tool_calls.activation_id
                         AND b.attempt_no=model_tool_calls.attempt_no
                         AND b.model_call_no=model_tool_calls.model_call_no
                         AND b.execution_status='active'
                         AND b.continuation_status='waiting_tools'
                         AND b.parent_operation_deadline IS NOT NULL
                         AND julianday(b.parent_operation_deadline)>julianday('now')
                   )",
            )
            .bind(effect_evidence_str(*effect_evidence))
            .bind(failure_class_str(cancellation_class))
            .bind(code)
            .bind(claim.claim_token())
            .bind(outcome_hash.as_str())
            .bind(i64::from(claim.tool_attempt_no().get()))
            .bind(
                i64::try_from(claim.lease_epoch().get())
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .bind(claim.fencing_token())
            .bind(effect_evidence_str(*effect_evidence))
            .bind(failure_class_str(cancellation_class))
            .bind(code)
            .bind(claim.run_id().as_str())
            .bind(claim.identity().tool_task_id().as_str())
            .bind(i64::from(claim.tool_attempt_no().get()))
            .bind(
                i64::try_from(claim.lease_epoch().get())
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
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
                return finish_stale_model_tool_commit_sqlite(tx, claim).await;
            }
            (ModelToolTaskDisposition::Cancelled, None)
        }
    };
    let continuation = if disposition == ModelToolTaskDisposition::RetryScheduled {
        ModelToolContinuationStatus::WaitingTools
    } else {
        finalize_batch_barrier_sqlite(
            &mut tx,
            claim.run_id().as_str(),
            claim.parent_activation_id().as_str(),
            i64::from(claim.parent_attempt_no().get()),
            i64::from(claim.model_call_no()),
        )
        .await?
    };
    let receipt = ModelToolTaskCommitReceipt::new(
        claim.identity().tool_task_id().clone(),
        disposition,
        claim.tool_attempt_no(),
        claim.lease_epoch(),
        next_available_at,
        continuation,
    )?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(ModelToolTaskTransitionOutcome::Committed(receipt))
}
