use super::RepositoryErrorExt as _;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{Row, Sqlite, Transaction};

use crate::engine::{
    control::{ControlEmissionSlot, ControlFrame},
    ActivationId, ChildRequirement, ContentHash, ControlTokenProvenance, ExecutionEventContext,
    ExecutionEventPayload, ExecutionForkLeg, ExecutionLegSettlementClass, ExecutionValueSummary,
    ForkLegCorrelation, InternalFailureKind, JoinMode, PendingExecutionEvent,
    ProjectionMutationKind, RunId, ScopeInstanceId, TransitionKey, TransitionOutcome,
};

use super::activation::execution_kind_fields;
use super::common::{
    canonical_intent_hash, canonical_json, event_id, i64_from_u64, u64_from_i64,
    validate_inline_payload,
};
use super::control_repository::{
    event_control_frames, join_mode_str, parse_settlement, scope_storage, settlement_str,
    ClaimSchedulerRunCommand, CloseScopeAdmissionCommand, ConsumeControlTokenCommand,
    ControlCommitReceipt, ControlDurableRepository, CreateChildScopeCommand, CreateForkCommand,
    CreateReuseCandidateCommand, EmitControlTokenCommand, FencedSchedulerRunCommand,
    HeartbeatSchedulerRunCommand, JoinArrivalReceipt, JoinBarrierAuthority,
    MaterializeReuseCandidateCommand, RecordJoinArrivalCommand, RejectReuseCandidateCommand,
    RevokeControlTokenCommand, SchedulerLeaseRepository, SchedulerRunLease, SettleScopeCommand,
};
use super::sqlite::{
    allocate_event_seq, decode_execution_event_row, insert_event, insert_or_get_payload,
    load_replay, Replay, SqliteDurableRepository,
};
use super::sqlite_projection::{
    finalize_projection_checkpoints, verify_projection_checkpoint_batch,
};
use super::{RepositoryError, REPOSITORY_CONFIGURATION_INVALID};

fn invalid_data() -> RepositoryError {
    RepositoryError::invalid_data()
}

fn unsupported_sqlite_scheduler_lease() -> RepositoryError {
    RepositoryError::new(
        REPOSITORY_CONFIGURATION_INVALID,
        "SQLite is a single-process control store and cannot own scheduler leases",
    )
}

fn model_data<T>(result: Result<T, crate::engine::ModelError>) -> Result<T, RepositoryError> {
    result.map_err(|_| invalid_data())
}

async fn replay_result<T: DeserializeOwned>(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    transition_key: &TransitionKey,
    intent_hash: &str,
) -> Result<Option<T>, RepositoryError> {
    let Replay::Exact(receipt) =
        load_replay(transaction, run_id, transition_key, intent_hash).await?
    else {
        return Ok(None);
    };
    let row = sqlx::query(
        "SELECT intent_hash, primary_event_id, result_json
         FROM control_transition_results WHERE run_id = ? AND transition_key = ?",
    )
    .bind(run_id.as_str())
    .bind(transition_key.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(invalid_data)?;
    if row
        .try_get::<String, _>("intent_hash")
        .map_err(|_| invalid_data())?
        != intent_hash
        || row
            .try_get::<String, _>("primary_event_id")
            .map_err(|_| invalid_data())?
            != receipt.event_id()
    {
        return Err(invalid_data());
    }
    serde_json::from_str(
        &row.try_get::<String, _>("result_json")
            .map_err(|_| invalid_data())?,
    )
    .map(Some)
    .map_err(|_| invalid_data())
}

async fn store_result<T: Serialize>(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    transition_key: &TransitionKey,
    intent_hash: &str,
    primary_event_id: &str,
    result: &T,
) -> Result<(), RepositoryError> {
    let result_json = canonical_json(
        &serde_json::to_value(result).map_err(|_| RepositoryError::canonicalization())?,
    )?;
    sqlx::query(
        "INSERT INTO control_transition_results (
            run_id, transition_key, intent_hash, primary_event_id, result_json, created_at
         ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(transition_key.as_str())
    .bind(intent_hash)
    .bind(primary_event_id)
    .bind(result_json)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

async fn authoritative_result<T: DeserializeOwned>(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    transition_key: &str,
) -> Result<T, RepositoryError> {
    let row = sqlx::query(
        "SELECT primary_event_id, result_json FROM control_transition_results
         WHERE run_id = ? AND transition_key = ?",
    )
    .bind(run_id.as_str())
    .bind(transition_key)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(invalid_data)?;
    let primary_event_id = row
        .try_get::<String, _>("primary_event_id")
        .map_err(|_| invalid_data())?;
    verify_projection_checkpoint_batch(transaction, run_id, &primary_event_id).await?;
    serde_json::from_str(
        &row.try_get::<String, _>("result_json")
            .map_err(|_| invalid_data())?,
    )
    .map_err(|_| invalid_data())
}

async fn activation_context(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    activation_id: &ActivationId,
) -> Result<Option<(ScopeInstanceId, crate::engine::NodeId, i64, String)>, RepositoryError> {
    let row = sqlx::query(
        "SELECT scope_instance_id, node_id, projection_version, lifecycle
         FROM node_activations WHERE run_id = ? AND activation_id = ?",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    row.map(|row| {
        Ok((
            model_data(ScopeInstanceId::new(
                row.try_get::<String, _>("scope_instance_id")
                    .map_err(|_| invalid_data())?,
            ))?,
            model_data(crate::engine::NodeId::new(
                row.try_get::<String, _>("node_id")
                    .map_err(|_| invalid_data())?,
            ))?,
            row.try_get("projection_version")
                .map_err(|_| invalid_data())?,
            row.try_get("lifecycle").map_err(|_| invalid_data())?,
        ))
    })
    .transpose()
}

async fn append_primary_event(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    transition_key: &TransitionKey,
    intent_hash: &str,
    projection_version_after: u64,
    event: &PendingExecutionEvent,
) -> Result<(u64, String), RepositoryError> {
    let seq = allocate_event_seq(transaction, run_id).await?;
    let id = event_id(transition_key);
    insert_event(
        transaction,
        run_id,
        seq,
        &id,
        transition_key,
        intent_hash,
        projection_version_after,
        event,
    )
    .await?;
    Ok((seq, id))
}

async fn finalize<T: Serialize>(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    transition_key: &TransitionKey,
    intent_hash: &str,
    event_id: &str,
    result: &T,
) -> Result<(), RepositoryError> {
    store_result(
        transaction,
        run_id,
        transition_key,
        intent_hash,
        event_id,
        result,
    )
    .await?;
    finalize_projection_checkpoints(transaction, run_id, event_id).await
}

#[async_trait]
impl ControlDurableRepository for SqliteDurableRepository {
    async fn create_child_scope(
        &self,
        transition_key: TransitionKey,
        command: CreateChildScopeCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
        let intent_hash = canonical_intent_hash(&command)?;
        let _writer = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(authoritative) = replay_result::<ControlCommitReceipt>(
            &mut tx,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }

        let parent = command.scope().parent().ok_or_else(invalid_data)?;
        let parent_row = sqlx::query(
            "SELECT lifecycle, admission_state, projection_version
             FROM scope_instances WHERE run_id = ? AND scope_instance_id = ?",
        )
        .bind(command.run_id().as_str())
        .bind(parent.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(parent_row) = parent_row else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if parent_row
            .try_get::<String, _>("lifecycle")
            .map_err(|_| invalid_data())?
            != "active"
            || parent_row
                .try_get::<String, _>("admission_state")
                .map_err(|_| invalid_data())?
                != "open"
            || u64_from_i64(
                parent_row
                    .try_get::<i64, _>("projection_version")
                    .map_err(|_| invalid_data())?,
            )? != command.expected_parent_projection_version()
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let duplicate: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM scope_instances WHERE run_id = ? AND scope_instance_id = ?",
        )
        .bind(command.run_id().as_str())
        .bind(command.scope().id().as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if duplicate.is_some() {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }

        let storage = scope_storage(command.scope())?;
        let event_scope_kind = storage.event_kind;
        sqlx::query(
            "INSERT INTO scope_instances (
                run_id, scope_instance_id, parent_scope_instance_id, static_scope_id,
                stable_dynamic_key, scope_kind, is_root, lifecycle, admission_state,
                admitted_children, settled_children, projection_version, created_at, settled_at
             ) VALUES (?, ?, ?, ?, ?, ?, 0, 'active', 'open', 0, 0, 0, CURRENT_TIMESTAMP, NULL)",
        )
        .bind(command.run_id().as_str())
        .bind(command.scope().id().as_str())
        .bind(parent.as_str())
        .bind(storage.static_scope_id)
        .bind(storage.stable_dynamic_key)
        .bind(storage.scope_kind)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let updated = sqlx::query(
            "UPDATE scope_instances
             SET admitted_children = admitted_children + 1,
                 projection_version = projection_version + 1
             WHERE run_id = ? AND scope_instance_id = ?
               AND lifecycle = 'active' AND admission_state = 'open'
               AND projection_version = ?",
        )
        .bind(command.run_id().as_str())
        .bind(parent.as_str())
        .bind(i64_from_u64(command.expected_parent_projection_version())?)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if updated.rows_affected() != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let event = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone())
                .in_scope(command.scope().id().clone()),
            ExecutionEventPayload::ScopeCreated {
                scope_kind: event_scope_kind,
                parent_scope_instance_id: Some(parent.clone()),
            },
        ))?;
        let (seq, id) = append_primary_event(
            &mut tx,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
            0,
            &event,
        )
        .await?;
        let receipt = ControlCommitReceipt::new(seq, id.clone(), 0);
        finalize(
            &mut tx,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
            &id,
            &receipt,
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn close_scope_admission(
        &self,
        transition_key: TransitionKey,
        command: CloseScopeAdmissionCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
        let intent_hash = canonical_intent_hash(&command)?;
        let _writer = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(authoritative) = replay_result::<ControlCommitReceipt>(
            &mut tx,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        let row = sqlx::query(
            "SELECT admitted_children, settled_children, lifecycle, admission_state,
                    projection_version
             FROM scope_instances WHERE run_id = ? AND scope_instance_id = ?",
        )
        .bind(command.run_id().as_str())
        .bind(command.scope_instance_id().as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if row
            .try_get::<String, _>("lifecycle")
            .map_err(|_| invalid_data())?
            != "active"
            || row
                .try_get::<String, _>("admission_state")
                .map_err(|_| invalid_data())?
                != "open"
            || u64_from_i64(
                row.try_get::<i64, _>("projection_version")
                    .map_err(|_| invalid_data())?,
            )? != command.expected_projection_version()
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let admitted = u32::try_from(
            row.try_get::<i64, _>("admitted_children")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?;
        let settled = u32::try_from(
            row.try_get::<i64, _>("settled_children")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?;
        let live_attempts = u32::try_from(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM node_attempts a JOIN node_activations n
             ON n.run_id = a.run_id AND n.activation_id = a.activation_id
             WHERE n.run_id = ? AND n.scope_instance_id = ?
               AND a.lifecycle IN ('created', 'leased', 'running')",
            )
            .bind(command.run_id().as_str())
            .bind(command.scope_instance_id().as_str())
            .fetch_one(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?,
        )
        .map_err(|_| invalid_data())?;
        let next_version = command
            .expected_projection_version()
            .checked_add(1)
            .ok_or_else(invalid_data)?;
        let event = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone())
                .in_scope(command.scope_instance_id().clone()),
            ExecutionEventPayload::ScopeDraining {
                admitted_children: admitted,
                settled_children: settled,
                live_attempts,
            },
        ))?;
        let (seq, id) = append_primary_event(
            &mut tx,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
            next_version,
            &event,
        )
        .await?;
        let updated = sqlx::query(
            "UPDATE scope_instances SET lifecycle = 'settling', admission_state = 'closed',
                    projection_version = projection_version + 1
             WHERE run_id = ? AND scope_instance_id = ? AND lifecycle = 'active'
               AND admission_state = 'open' AND projection_version = ?",
        )
        .bind(command.run_id().as_str())
        .bind(command.scope_instance_id().as_str())
        .bind(i64_from_u64(command.expected_projection_version())?)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if updated.rows_affected() != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let receipt = ControlCommitReceipt::new(seq, id.clone(), next_version);
        finalize(
            &mut tx,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
            &id,
            &receipt,
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn settle_scope(
        &self,
        transition_key: TransitionKey,
        command: SettleScopeCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
        let intent_hash = canonical_intent_hash(&command)?;
        let _writer = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(authoritative) = replay_result::<ControlCommitReceipt>(
            &mut tx,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        let row = sqlx::query(
            "SELECT parent_scope_instance_id, admitted_children, settled_children, lifecycle,
                    admission_state, projection_version
             FROM scope_instances WHERE run_id = ? AND scope_instance_id = ?",
        )
        .bind(command.run_id().as_str())
        .bind(command.scope_instance_id().as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        let admitted_i = row
            .try_get::<i64, _>("admitted_children")
            .map_err(|_| invalid_data())?;
        let settled_i = row
            .try_get::<i64, _>("settled_children")
            .map_err(|_| invalid_data())?;
        let live_attempts = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_attempts a JOIN node_activations n
             ON n.run_id = a.run_id AND n.activation_id = a.activation_id
             WHERE n.run_id = ? AND n.scope_instance_id = ?
               AND a.lifecycle IN ('created', 'leased', 'running')",
        )
        .bind(command.run_id().as_str())
        .bind(command.scope_instance_id().as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if row
            .try_get::<String, _>("lifecycle")
            .map_err(|_| invalid_data())?
            != "settling"
            || row
                .try_get::<String, _>("admission_state")
                .map_err(|_| invalid_data())?
                != "closed"
            || admitted_i != settled_i
            || live_attempts != 0
            || u64_from_i64(
                row.try_get::<i64, _>("projection_version")
                    .map_err(|_| invalid_data())?,
            )? != command.expected_projection_version()
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let next_version = command
            .expected_projection_version()
            .checked_add(1)
            .ok_or_else(invalid_data)?;
        let event = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone())
                .in_scope(command.scope_instance_id().clone()),
            ExecutionEventPayload::ScopeSettled {
                admitted_children: u32::try_from(admitted_i).map_err(|_| invalid_data())?,
                settled_children: u32::try_from(settled_i).map_err(|_| invalid_data())?,
                live_attempts: 0,
            },
        ))?;
        let (seq, id) = append_primary_event(
            &mut tx,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
            next_version,
            &event,
        )
        .await?;
        let updated = sqlx::query(
            "UPDATE scope_instances SET lifecycle = 'settled', projection_version = projection_version + 1,
                    settled_at = CURRENT_TIMESTAMP
             WHERE run_id = ? AND scope_instance_id = ? AND lifecycle = 'settling'
               AND admission_state = 'closed' AND admitted_children = settled_children
               AND projection_version = ?",
        ).bind(command.run_id().as_str()).bind(command.scope_instance_id().as_str())
        .bind(i64_from_u64(command.expected_projection_version())?)
        .execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        if updated.rows_affected() != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        if let Some(parent) = row
            .try_get::<Option<String>, _>("parent_scope_instance_id")
            .map_err(|_| invalid_data())?
        {
            let parent_update = sqlx::query(
                "UPDATE scope_instances SET settled_children = settled_children + 1,
                        projection_version = projection_version + 1
                 WHERE run_id = ? AND scope_instance_id = ? AND settled_children < admitted_children",
            ).bind(command.run_id().as_str()).bind(parent)
            .execute(&mut *tx).await.map_err(RepositoryError::storage)?;
            if parent_update.rows_affected() != 1 {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
        }
        let receipt = ControlCommitReceipt::new(seq, id.clone(), next_version);
        finalize(
            &mut tx,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
            &id,
            &receipt,
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn emit_control_token(
        &self,
        transition_key: TransitionKey,
        command: EmitControlTokenCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
        let intent_hash = canonical_intent_hash(&command)?;
        let _writer = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(authoritative) = replay_result::<ControlCommitReceipt>(
            &mut tx,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        let Some((scope, node, version, _)) = activation_context(
            &mut tx,
            command.run_id(),
            command.provenance().source_activation_id(),
        )
        .await?
        else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if scope != *command.provenance().scope_instance_id()
            || u64_from_i64(version)? != command.expected_source_projection_version()
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let duplicate: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM control_tokens WHERE run_id = ? AND
             (token_id = ? OR (source_activation_id = ? AND emission_slot = ?))",
        )
        .bind(command.run_id().as_str())
        .bind(command.token_id().as_str())
        .bind(command.provenance().source_activation_id().as_str())
        .bind(command.provenance().emission_slot().storage_key())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if duplicate.is_some() {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let event = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
                scope.clone(),
                node,
                command.provenance().source_activation_id().clone(),
            ),
            ExecutionEventPayload::ControlTokenEmitted {
                token_id: command.token_id().clone(),
                source_port: command.provenance().source_port().clone(),
                token_scope_instance_id: scope,
                frames: event_control_frames(command.provenance()),
            },
        ))?;
        let (seq, id) = append_primary_event(
            &mut tx,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
            0,
            &event,
        )
        .await?;
        insert_token(
            &mut tx,
            command.run_id(),
            command.token_id(),
            command.provenance(),
            &transition_key,
        )
        .await?;
        let receipt = ControlCommitReceipt::new(seq, id.clone(), 0);
        finalize(
            &mut tx,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
            &id,
            &receipt,
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn consume_control_token(
        &self,
        transition_key: TransitionKey,
        command: ConsumeControlTokenCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
        mutate_token_terminal(
            self,
            transition_key,
            command.run_id(),
            command.token_id(),
            command.expected_token_projection_version(),
            Some(command.consumer_activation_id()),
            false,
            &command,
        )
        .await
    }

    async fn revoke_control_token(
        &self,
        transition_key: TransitionKey,
        command: RevokeControlTokenCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
        mutate_token_terminal::<RevokeControlTokenCommand>(
            self,
            transition_key,
            command.run_id(),
            command.token_id(),
            command.expected_token_projection_version(),
            None,
            true,
            &command,
        )
        .await
    }

    async fn create_fork(
        &self,
        transition_key: TransitionKey,
        command: CreateForkCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
        create_fork(self, transition_key, command).await
    }

    async fn record_join_arrival(
        &self,
        transition_key: TransitionKey,
        command: RecordJoinArrivalCommand,
    ) -> Result<TransitionOutcome<JoinArrivalReceipt>, RepositoryError> {
        record_join_arrival(self, transition_key, command).await
    }

    async fn create_reuse_candidate(
        &self,
        transition_key: TransitionKey,
        command: CreateReuseCandidateCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
        create_reuse_candidate(self, transition_key, command).await
    }

    async fn reject_reuse_candidate(
        &self,
        transition_key: TransitionKey,
        command: RejectReuseCandidateCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
        decide_reuse_candidate(self, transition_key, command).await
    }

    async fn materialize_reuse_candidate(
        &self,
        transition_key: TransitionKey,
        command: MaterializeReuseCandidateCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
        materialize_reuse_candidate(self, transition_key, command).await
    }
}

async fn insert_token(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    token_id: &crate::engine::ControlTokenId,
    provenance: &crate::engine::ControlTokenProvenance,
    transition_key: &TransitionKey,
) -> Result<(), RepositoryError> {
    let frames = canonical_json(
        &serde_json::to_value(provenance.frames())
            .map_err(|_| RepositoryError::canonicalization())?,
    )?;
    let branch = provenance
        .frames()
        .iter()
        .rev()
        .find_map(|frame| match frame {
            ControlFrame::Branch(frame) => Some((
                frame.branch_activation_id().as_str(),
                frame.selected_port().as_str(),
            )),
            ControlFrame::ForkLeg(_) => None,
        });
    let fork = provenance
        .frames()
        .iter()
        .rev()
        .find_map(|frame| match frame {
            ControlFrame::ForkLeg(frame) => {
                Some((frame.fork_group_id().as_str(), frame.leg_id().as_str()))
            }
            ControlFrame::Branch(_) => None,
        });
    sqlx::query(
        "INSERT INTO control_tokens (
            run_id, token_id, current_scope_instance_id, current_port_id,
            source_activation_id, source_port_id, emission_slot, emitted_by_transition_key,
            provenance_frames, branch_activation_id, selected_branch_port_id,
            fork_group_id, fork_leg_id, token_state, consumed_by_activation_id,
            consumed_by_transition_key, consumed_at, revoked_by_transition_key, revoked_at,
            projection_version, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'available',
                   NULL, NULL, NULL, NULL, NULL, 0, CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(token_id.as_str())
    .bind(provenance.scope_instance_id().as_str())
    .bind(provenance.source_port().as_str())
    .bind(provenance.source_activation_id().as_str())
    .bind(provenance.source_port().as_str())
    .bind(provenance.emission_slot().storage_key())
    .bind(transition_key.as_str())
    .bind(frames)
    .bind(branch.map(|value| value.0))
    .bind(branch.map(|value| value.1))
    .bind(fork.map(|value| value.0))
    .bind(fork.map(|value| value.1))
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn mutate_token_terminal<T: Serialize>(
    repository: &SqliteDurableRepository,
    transition_key: TransitionKey,
    run_id: &RunId,
    token_id: &crate::engine::ControlTokenId,
    expected_version: u64,
    consumer: Option<&ActivationId>,
    revoke: bool,
    command: &T,
) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
    let intent_hash = canonical_intent_hash(command)?;
    let _writer = repository.writer.lock().await;
    let mut tx = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    if let Some(authoritative) = replay_result::<ControlCommitReceipt>(
        &mut tx,
        run_id,
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::ExactReplay { authoritative });
    }
    let token = sqlx::query(
        "SELECT source_activation_id, projection_version, token_state
         FROM control_tokens WHERE run_id = ? AND token_id = ?",
    )
    .bind(run_id.as_str())
    .bind(token_id.as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(token) = token else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    if token
        .try_get::<String, _>("token_state")
        .map_err(|_| invalid_data())?
        != "available"
        || u64_from_i64(
            token
                .try_get::<i64, _>("projection_version")
                .map_err(|_| invalid_data())?,
        )? != expected_version
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let event_activation = if let Some(consumer) = consumer {
        consumer.clone()
    } else {
        model_data(ActivationId::new(
            token
                .try_get::<String, _>("source_activation_id")
                .map_err(|_| invalid_data())?,
        ))?
    };
    let Some((scope, node, _, _)) = activation_context(&mut tx, run_id, &event_activation).await?
    else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    let next_version = expected_version.checked_add(1).ok_or_else(invalid_data)?;
    let payload = if revoke {
        ExecutionEventPayload::ControlTokenRevoked {
            token_id: token_id.clone(),
        }
    } else {
        ExecutionEventPayload::ControlTokenConsumed {
            token_id: token_id.clone(),
        }
    };
    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(run_id.clone()).for_activation(
            scope,
            node,
            event_activation,
        ),
        payload,
    ))?;
    let (seq, id) = append_primary_event(
        &mut tx,
        run_id,
        &transition_key,
        intent_hash.as_str(),
        next_version,
        &event,
    )
    .await?;
    let updated = if revoke {
        sqlx::query(
            "UPDATE control_tokens SET token_state = 'revoked', revoked_by_transition_key = ?,
                    revoked_at = CURRENT_TIMESTAMP, projection_version = projection_version + 1
             WHERE run_id = ? AND token_id = ? AND token_state = 'available'
               AND projection_version = ?",
        )
        .bind(transition_key.as_str())
        .bind(run_id.as_str())
        .bind(token_id.as_str())
        .bind(i64_from_u64(expected_version)?)
        .execute(&mut *tx)
        .await
    } else {
        sqlx::query(
            "UPDATE control_tokens SET token_state = 'consumed', consumed_by_activation_id = ?,
                    consumed_by_transition_key = ?, consumed_at = CURRENT_TIMESTAMP,
                    projection_version = projection_version + 1
             WHERE run_id = ? AND token_id = ? AND token_state = 'available'
               AND projection_version = ?",
        )
        .bind(consumer.expect("consume supplies consumer").as_str())
        .bind(transition_key.as_str())
        .bind(run_id.as_str())
        .bind(token_id.as_str())
        .bind(i64_from_u64(expected_version)?)
        .execute(&mut *tx)
        .await
    }
    .map_err(RepositoryError::storage)?;
    if updated.rows_affected() != 1 {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let receipt = ControlCommitReceipt::new(seq, id.clone(), next_version);
    finalize(
        &mut tx,
        run_id,
        &transition_key,
        intent_hash.as_str(),
        &id,
        &receipt,
    )
    .await?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: receipt })
}

#[async_trait]
impl SchedulerLeaseRepository for SqliteDurableRepository {
    async fn claim_scheduler_run(
        &self,
        _transition_key: TransitionKey,
        _command: ClaimSchedulerRunCommand,
    ) -> Result<TransitionOutcome<SchedulerRunLease>, RepositoryError> {
        Err(unsupported_sqlite_scheduler_lease())
    }
    async fn heartbeat_scheduler_run(
        &self,
        _transition_key: TransitionKey,
        _command: HeartbeatSchedulerRunCommand,
    ) -> Result<TransitionOutcome<SchedulerRunLease>, RepositoryError> {
        Err(unsupported_sqlite_scheduler_lease())
    }
    async fn release_scheduler_run(
        &self,
        _transition_key: TransitionKey,
        _command: FencedSchedulerRunCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
        Err(unsupported_sqlite_scheduler_lease())
    }
}

// Structured operations are below so their transaction bodies remain isolated
// from the small Scope/Token transitions above.
async fn create_fork(
    repository: &SqliteDurableRepository,
    transition_key: TransitionKey,
    command: CreateForkCommand,
) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let _writer = repository.writer.lock().await;
    let mut tx = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    if let Some(authoritative) = replay_result::<ControlCommitReceipt>(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::ExactReplay { authoritative });
    }

    if command.legs().is_empty()
        || command.inherited_token_id().is_some()
            != command
                .expected_inherited_token_projection_version()
                .is_some()
    {
        return Err(RepositoryError::invalid_configuration());
    }
    let Some((fork_scope, fork_node, fork_version, fork_lifecycle)) =
        activation_context(&mut tx, command.run_id(), command.fork_activation_id()).await?
    else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    if fork_scope != *command.parent_scope_instance_id()
        || u64_from_i64(fork_version)? != command.expected_fork_activation_projection_version()
        || !matches!(fork_lifecycle.as_str(), "created" | "ready")
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let parent = sqlx::query(
        "SELECT lifecycle, admission_state, projection_version
         FROM scope_instances WHERE run_id = ? AND scope_instance_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.parent_scope_instance_id().as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(parent) = parent else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    if parent
        .try_get::<String, _>("lifecycle")
        .map_err(|_| invalid_data())?
        != "active"
        || parent
            .try_get::<String, _>("admission_state")
            .map_err(|_| invalid_data())?
            != "open"
        || u64_from_i64(
            parent
                .try_get::<i64, _>("projection_version")
                .map_err(|_| invalid_data())?,
        )? != command.expected_parent_scope_projection_version()
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let occupied: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM fork_groups WHERE run_id = ? AND fork_group_id = ?")
            .bind(command.run_id().as_str())
            .bind(command.fork_group_id().as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
    if occupied.is_some() {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }

    let inherited_frames = if let Some(token_id) = command.inherited_token_id() {
        let token = sqlx::query(
            "SELECT current_scope_instance_id, provenance_frames, token_state, projection_version
             FROM control_tokens WHERE run_id = ? AND token_id = ?",
        )
        .bind(command.run_id().as_str())
        .bind(token_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(token) = token else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if token
            .try_get::<String, _>("token_state")
            .map_err(|_| invalid_data())?
            != "available"
            || token
                .try_get::<String, _>("current_scope_instance_id")
                .map_err(|_| invalid_data())?
                != command.parent_scope_instance_id().as_str()
            || u64_from_i64(
                token
                    .try_get::<i64, _>("projection_version")
                    .map_err(|_| invalid_data())?,
            )? != command
                .expected_inherited_token_projection_version()
                .ok_or_else(invalid_data)?
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        serde_json::from_str::<Vec<ControlFrame>>(
            &token
                .try_get::<String, _>("provenance_frames")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?
    } else {
        Vec::new()
    };

    let mut provenances = Vec::with_capacity(command.legs().len());
    let mut event_legs = Vec::with_capacity(command.legs().len());
    for admission in command.legs() {
        if admission.leg().run_id() != command.run_id()
            || admission.scope().parent() != Some(command.parent_scope_instance_id())
            || admission.scope().id() != admission.leg().scope_instance_id()
        {
            return Err(RepositoryError::invalid_configuration());
        }
        let frame = model_data(ForkLegCorrelation::new(
            command.run_id().clone(),
            command.fork_activation_id().clone(),
            command.fork_group_id().clone(),
            admission.leg().leg_id().clone(),
            command.parent_scope_instance_id().clone(),
            admission.scope().id().clone(),
            admission.leg().child_activation_id().clone(),
        ))?;
        let mut frames = inherited_frames.clone();
        frames.push(ControlFrame::ForkLeg(frame));
        let provenance = model_data(ControlTokenProvenance::new(
            command.run_id().clone(),
            command.fork_activation_id().clone(),
            admission.leg().output_port().clone(),
            ControlEmissionSlot::ForkLeg {
                fork_group_id: command.fork_group_id().clone(),
                leg_id: admission.leg().leg_id().clone(),
            },
            admission.scope().id().clone(),
            frames,
        ))?;
        event_legs.push(ExecutionForkLeg::new(
            admission.leg().leg_id().clone(),
            admission.leg().output_port().clone(),
            admission.scope().id().clone(),
        ));
        provenances.push(provenance);
    }

    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
            fork_scope,
            fork_node,
            command.fork_activation_id().clone(),
        ),
        ExecutionEventPayload::ForkCreated {
            fork_group_id: command.fork_group_id().clone(),
            legs: event_legs,
        },
    ))?;
    let (seq, id) = append_primary_event(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        0,
        &event,
    )
    .await?;
    sqlx::query(
        "INSERT INTO fork_groups (
            run_id, fork_group_id, fork_activation_id, parent_scope_instance_id,
            join_activation_id, join_mode, failure_leg_id, failure_settlement_class,
            expected_legs, group_state, admitted_legs, settled_legs,
            projection_version, created_at, settled_at
         ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, 'open', ?, 0, 0,
                   CURRENT_TIMESTAMP, NULL)",
    )
    .bind(command.run_id().as_str())
    .bind(command.fork_group_id().as_str())
    .bind(command.fork_activation_id().as_str())
    .bind(command.parent_scope_instance_id().as_str())
    .bind(i64::try_from(command.legs().len()).map_err(|_| invalid_data())?)
    .bind(i64::try_from(command.legs().len()).map_err(|_| invalid_data())?)
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;

    for (index, (admission, provenance)) in command.legs().iter().zip(&provenances).enumerate() {
        insert_scope_row(&mut tx, command.run_id(), admission.scope()).await?;
        insert_activation_row(
            &mut tx,
            command.run_id(),
            admission.leg().child_activation_id(),
            admission.scope().id(),
            admission.child_node_id(),
            admission.stable_activation_key(),
            admission.execution_kind(),
        )
        .await?;
        insert_token(
            &mut tx,
            command.run_id(),
            admission.token_id(),
            provenance,
            &transition_key,
        )
        .await?;
        sqlx::query(
            "INSERT INTO fork_legs (
                run_id, fork_group_id, leg_id, declaration_index, scope_instance_id,
                child_activation_id, token_id, is_required, leg_state, settlement_class,
                projection_version, created_at, settled_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'admitted', NULL, 0, CURRENT_TIMESTAMP, NULL)",
        )
        .bind(command.run_id().as_str())
        .bind(command.fork_group_id().as_str())
        .bind(admission.leg().leg_id().as_str())
        .bind(i64::try_from(index).map_err(|_| invalid_data())?)
        .bind(admission.scope().id().as_str())
        .bind(admission.leg().child_activation_id().as_str())
        .bind(admission.token_id().as_str())
        .bind(i64::from(matches!(
            admission.leg().requirement(),
            ChildRequirement::Required
        )))
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
    }
    let parent_update = sqlx::query(
        "UPDATE scope_instances SET admitted_children = admitted_children + ?,
                projection_version = projection_version + 1
         WHERE run_id = ? AND scope_instance_id = ? AND lifecycle = 'active'
           AND admission_state = 'open' AND projection_version = ?",
    )
    .bind(i64::try_from(command.legs().len()).map_err(|_| invalid_data())?)
    .bind(command.run_id().as_str())
    .bind(command.parent_scope_instance_id().as_str())
    .bind(i64_from_u64(
        command.expected_parent_scope_projection_version(),
    )?)
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    if parent_update.rows_affected() != 1 {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    if let Some(token_id) = command.inherited_token_id() {
        let consumed = sqlx::query(
            "UPDATE control_tokens SET token_state = 'consumed', consumed_by_activation_id = ?,
                    consumed_by_transition_key = ?, consumed_at = CURRENT_TIMESTAMP,
                    projection_version = projection_version + 1
             WHERE run_id = ? AND token_id = ? AND token_state = 'available'
               AND projection_version = ?",
        )
        .bind(command.fork_activation_id().as_str())
        .bind(transition_key.as_str())
        .bind(command.run_id().as_str())
        .bind(token_id.as_str())
        .bind(i64_from_u64(
            command
                .expected_inherited_token_projection_version()
                .ok_or_else(invalid_data)?,
        )?)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if consumed.rows_affected() != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
    }
    let receipt = ControlCommitReceipt::new(seq, id.clone(), 0);
    finalize(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        &id,
        &receipt,
    )
    .await?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: receipt })
}

async fn insert_scope_row(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    scope: &crate::engine::ScopeInstance,
) -> Result<(), RepositoryError> {
    let storage = scope_storage(scope)?;
    sqlx::query(
        "INSERT INTO scope_instances (
            run_id, scope_instance_id, parent_scope_instance_id, static_scope_id,
            stable_dynamic_key, scope_kind, is_root, lifecycle, admission_state,
            admitted_children, settled_children, projection_version, created_at, settled_at
         ) VALUES (?, ?, ?, ?, ?, ?, 0, 'active', 'open', 0, 0, 0, CURRENT_TIMESTAMP, NULL)",
    )
    .bind(run_id.as_str())
    .bind(scope.id().as_str())
    .bind(scope.parent().ok_or_else(invalid_data)?.as_str())
    .bind(storage.static_scope_id)
    .bind(storage.stable_dynamic_key)
    .bind(storage.scope_kind)
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

async fn insert_activation_row(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    activation_id: &ActivationId,
    scope_id: &ScopeInstanceId,
    node_id: &crate::engine::NodeId,
    stable_activation_key: &str,
    execution_kind: &crate::engine::ExecutionKind,
) -> Result<(), RepositoryError> {
    let effect_id = crate::engine::EffectId::for_activation(run_id, activation_id);
    let (execution_kind_name, idempotency, retry_budget) = execution_kind_fields(execution_kind);
    sqlx::query(
        "INSERT INTO node_activations (
            run_id, activation_id, scope_instance_id, node_id, stable_activation_key,
            execution_kind, lifecycle, effect_id, effect_idempotency, effect_evidence,
            last_attempt_no, last_lease_epoch, current_attempt_no, current_lease_epoch,
            current_fencing_token, retry_budget_remaining, pending_retry_timer_id,
            wait_registration_transition_key, termination_intent_reason,
            termination_intent_transition_key, termination_intent_at, output_payload_id,
            output_artifact_id, output_value_hash, winning_attempt_no, reused_from_run_id,
            reused_from_activation_id, projection_version, created_at, updated_at, terminal_at
         ) VALUES (?, ?, ?, ?, ?, ?, 'created', ?, ?, 'not_started', NULL, NULL, NULL,
                   NULL, NULL, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                   NULL, NULL, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL)",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .bind(scope_id.as_str())
    .bind(node_id.as_str())
    .bind(stable_activation_key)
    .bind(execution_kind_name)
    .bind(effect_id.as_str())
    .bind(idempotency)
    .bind(i64::from(retry_budget))
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

async fn record_join_arrival(
    repository: &SqliteDurableRepository,
    transition_key: TransitionKey,
    command: RecordJoinArrivalCommand,
) -> Result<TransitionOutcome<JoinArrivalReceipt>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let _writer = repository.writer.lock().await;
    let mut tx = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    if let Some(authoritative) = replay_result::<JoinArrivalReceipt>(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::ExactReplay { authoritative });
    }

    let duplicate = sqlx::query(
        "SELECT join_activation_id, token_id, arrival_transition_key
         FROM join_arrivals WHERE run_id = ? AND fork_group_id = ? AND leg_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.fork_group_id().as_str())
    .bind(command.leg_id().as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    if let Some(duplicate) = duplicate {
        if duplicate
            .try_get::<String, _>("join_activation_id")
            .map_err(|_| invalid_data())?
            == command.join_activation_id().as_str()
            && duplicate
                .try_get::<String, _>("token_id")
                .map_err(|_| invalid_data())?
                == command.token_id().as_str()
        {
            let authoritative = authoritative_result::<JoinArrivalReceipt>(
                &mut tx,
                command.run_id(),
                &duplicate
                    .try_get::<String, _>("arrival_transition_key")
                    .map_err(|_| invalid_data())?,
            )
            .await?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }

    let group = sqlx::query(
        "SELECT fork_activation_id, parent_scope_instance_id, join_activation_id, join_mode,
                failure_leg_id, failure_settlement_class, expected_legs, admitted_legs,
                settled_legs, group_state, projection_version
         FROM fork_groups WHERE run_id = ? AND fork_group_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.fork_group_id().as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(group) = group else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    let group_state = group
        .try_get::<String, _>("group_state")
        .map_err(|_| invalid_data())?;
    let bound_join = group
        .try_get::<Option<String>, _>("join_activation_id")
        .map_err(|_| invalid_data())?;
    let bound_mode = group
        .try_get::<Option<String>, _>("join_mode")
        .map_err(|_| invalid_data())?;
    if !matches!(group_state.as_str(), "open" | "settling")
        || u64_from_i64(
            group
                .try_get::<i64, _>("projection_version")
                .map_err(|_| invalid_data())?,
        )? != command.expected_group_projection_version()
        || bound_join
            .as_deref()
            .is_some_and(|value| value != command.join_activation_id().as_str())
        || bound_mode
            .as_deref()
            .is_some_and(|value| value != join_mode_str(command.mode()))
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let parent_scope = model_data(ScopeInstanceId::new(
        group
            .try_get::<String, _>("parent_scope_instance_id")
            .map_err(|_| invalid_data())?,
    ))?;
    let Some((join_scope, join_node, _, join_lifecycle)) =
        activation_context(&mut tx, command.run_id(), command.join_activation_id()).await?
    else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    if join_scope != parent_scope || !matches!(join_lifecycle.as_str(), "created" | "ready") {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }

    let leg = sqlx::query(
        "SELECT scope_instance_id, child_activation_id, token_id, leg_state, projection_version
         FROM fork_legs WHERE run_id = ? AND fork_group_id = ? AND leg_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.fork_group_id().as_str())
    .bind(command.leg_id().as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(leg) = leg else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    if leg
        .try_get::<String, _>("leg_state")
        .map_err(|_| invalid_data())?
        != "admitted"
        || leg
            .try_get::<String, _>("token_id")
            .map_err(|_| invalid_data())?
            != command.token_id().as_str()
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let child_scope = model_data(ScopeInstanceId::new(
        leg.try_get::<String, _>("scope_instance_id")
            .map_err(|_| invalid_data())?,
    ))?;
    let child_activation = model_data(ActivationId::new(
        leg.try_get::<String, _>("child_activation_id")
            .map_err(|_| invalid_data())?,
    ))?;
    let token = sqlx::query(
        "SELECT token_state, projection_version, current_scope_instance_id,
                fork_group_id, fork_leg_id
         FROM control_tokens WHERE run_id = ? AND token_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.token_id().as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(token) = token else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    let token_state = token
        .try_get::<String, _>("token_state")
        .map_err(|_| invalid_data())?;
    let drain_token = token_state == "revoked" && group_state == "settling";
    if !(token_state == "available" || drain_token)
        || u64_from_i64(
            token
                .try_get::<i64, _>("projection_version")
                .map_err(|_| invalid_data())?,
        )? != command.expected_token_projection_version()
        || token
            .try_get::<String, _>("current_scope_instance_id")
            .map_err(|_| invalid_data())?
            != child_scope.as_str()
        || token
            .try_get::<Option<String>, _>("fork_group_id")
            .map_err(|_| invalid_data())?
            .as_deref()
            != Some(command.fork_group_id().as_str())
        || token
            .try_get::<Option<String>, _>("fork_leg_id")
            .map_err(|_| invalid_data())?
            .as_deref()
            != Some(command.leg_id().as_str())
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }

    let terminal = derive_leg_terminal(&mut tx, command.run_id(), &child_activation).await?;
    let Some(terminal) = terminal else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    let live_attempts = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM node_attempts a JOIN node_activations n
         ON n.run_id = a.run_id AND n.activation_id = a.activation_id
         WHERE n.run_id = ? AND n.scope_instance_id = ?
           AND a.lifecycle IN ('created', 'leased', 'running')",
    )
    .bind(command.run_id().as_str())
    .bind(child_scope.as_str())
    .fetch_one(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let scope = sqlx::query(
        "SELECT parent_scope_instance_id, lifecycle, admission_state, admitted_children,
                settled_children, projection_version
         FROM scope_instances WHERE run_id = ? AND scope_instance_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(child_scope.as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(scope) = scope else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    if scope
        .try_get::<Option<String>, _>("parent_scope_instance_id")
        .map_err(|_| invalid_data())?
        .as_deref()
        != Some(parent_scope.as_str())
        || scope
            .try_get::<i64, _>("admitted_children")
            .map_err(|_| invalid_data())?
            != scope
                .try_get::<i64, _>("settled_children")
                .map_err(|_| invalid_data())?
        || live_attempts != 0
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let scope_lifecycle = scope
        .try_get::<String, _>("lifecycle")
        .map_err(|_| invalid_data())?;
    if !matches!(scope_lifecycle.as_str(), "active" | "settling" | "settled") {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }

    let expected_legs = u32::try_from(
        group
            .try_get::<i64, _>("expected_legs")
            .map_err(|_| invalid_data())?,
    )
    .map_err(|_| invalid_data())?;
    let settled_before = u32::try_from(
        group
            .try_get::<i64, _>("settled_legs")
            .map_err(|_| invalid_data())?,
    )
    .map_err(|_| invalid_data())?;
    let settled_after = settled_before.checked_add(1).ok_or_else(invalid_data)?;
    let prior_failure_leg = group
        .try_get::<Option<String>, _>("failure_leg_id")
        .map_err(|_| invalid_data())?
        .map(|value| model_data(crate::engine::LegId::new(value)))
        .transpose()?;
    let prior_failure_class = group
        .try_get::<Option<String>, _>("failure_settlement_class")
        .map_err(|_| invalid_data())?
        .map(|value| parse_settlement(&value))
        .transpose()?;
    let terminal_is_failure = match command.mode() {
        JoinMode::AllSuccess => terminal.class != ExecutionLegSettlementClass::Succeeded,
        JoinMode::AllSettled => matches!(
            terminal.class,
            ExecutionLegSettlementClass::InfrastructureFailure
                | ExecutionLegSettlementClass::Panic
                | ExecutionLegSettlementClass::Cancelled
                | ExecutionLegSettlementClass::TimedOut
        ),
    };
    let failure_leg =
        prior_failure_leg.or_else(|| terminal_is_failure.then(|| command.leg_id().clone()));
    let failure_class =
        prior_failure_class.or_else(|| terminal_is_failure.then_some(terminal.class));
    let authority = if settled_after == expected_legs {
        if let (Some(failed_leg_id), Some(settlement_class)) = (failure_leg.clone(), failure_class)
        {
            JoinBarrierAuthority::Failed {
                failed_leg_id,
                settlement_class,
                settled_legs: settled_after,
            }
        } else {
            JoinBarrierAuthority::Ready {
                mode: command.mode(),
                settled_legs: settled_after,
            }
        }
    } else if command.mode() == JoinMode::AllSuccess && failure_leg.is_some() {
        JoinBarrierAuthority::Draining {
            failed_leg_id: failure_leg.clone().ok_or_else(invalid_data)?,
            settled_legs: settled_after,
            expected_legs,
        }
    } else {
        JoinBarrierAuthority::Pending {
            settled_legs: settled_after,
            expected_legs,
        }
    };
    let next_group_version = command
        .expected_group_projection_version()
        .checked_add(1)
        .ok_or_else(invalid_data)?;
    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
            join_scope,
            join_node,
            command.join_activation_id().clone(),
        ),
        ExecutionEventPayload::JoinArrived {
            fork_group_id: command.fork_group_id().clone(),
            leg_id: command.leg_id().clone(),
            token_id: command.token_id().clone(),
            settlement: terminal.class,
            value: terminal.value_summary.clone(),
        },
    ))?;
    let (seq, id) = append_primary_event(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        next_group_version,
        &event,
    )
    .await?;

    if scope_lifecycle != "settled" {
        let scope_update = sqlx::query(
            "UPDATE scope_instances SET lifecycle = 'settled', admission_state = 'closed',
                    projection_version = projection_version + 1, settled_at = CURRENT_TIMESTAMP
             WHERE run_id = ? AND scope_instance_id = ? AND lifecycle IN ('active', 'settling')
               AND admitted_children = settled_children",
        )
        .bind(command.run_id().as_str())
        .bind(child_scope.as_str())
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if scope_update.rows_affected() != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let parent_update = sqlx::query(
            "UPDATE scope_instances SET settled_children = settled_children + 1,
                    projection_version = projection_version + 1
             WHERE run_id = ? AND scope_instance_id = ? AND settled_children < admitted_children",
        )
        .bind(command.run_id().as_str())
        .bind(parent_scope.as_str())
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if parent_update.rows_affected() != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
    }
    let leg_update = sqlx::query(
        "UPDATE fork_legs SET leg_state = 'settled', settlement_class = ?,
                projection_version = projection_version + 1, settled_at = CURRENT_TIMESTAMP
         WHERE run_id = ? AND fork_group_id = ? AND leg_id = ? AND leg_state = 'admitted'",
    )
    .bind(settlement_str(terminal.class))
    .bind(command.run_id().as_str())
    .bind(command.fork_group_id().as_str())
    .bind(command.leg_id().as_str())
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    if leg_update.rows_affected() != 1 {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    if token_state == "available" {
        let token_update = sqlx::query(
            "UPDATE control_tokens SET token_state = 'consumed', consumed_by_activation_id = ?,
                    consumed_by_transition_key = ?, consumed_at = CURRENT_TIMESTAMP,
                    projection_version = projection_version + 1
             WHERE run_id = ? AND token_id = ? AND token_state = 'available'
               AND projection_version = ?",
        )
        .bind(command.join_activation_id().as_str())
        .bind(transition_key.as_str())
        .bind(command.run_id().as_str())
        .bind(command.token_id().as_str())
        .bind(i64_from_u64(command.expected_token_projection_version())?)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if token_update.rows_affected() != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
    }
    sqlx::query(
        "INSERT INTO join_arrivals (
            run_id, join_activation_id, fork_group_id, leg_id, token_id,
            arrival_transition_key, arrival_event_id, settlement_class,
            value_payload_id, value_artifact_id, value_hash, projection_version, arrived_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP)",
    )
    .bind(command.run_id().as_str())
    .bind(command.join_activation_id().as_str())
    .bind(command.fork_group_id().as_str())
    .bind(command.leg_id().as_str())
    .bind(command.token_id().as_str())
    .bind(transition_key.as_str())
    .bind(&id)
    .bind(settlement_str(terminal.class))
    .bind(terminal.payload_id.as_deref())
    .bind(terminal.artifact_id.as_deref())
    .bind(terminal.value_hash.as_deref())
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;

    let final_group_state = if settled_after == expected_legs {
        if failure_leg.is_some() {
            "cancelled"
        } else {
            "settled"
        }
    } else if command.mode() == JoinMode::AllSuccess && failure_leg.is_some() {
        "settling"
    } else {
        "open"
    };
    let group_update = sqlx::query(
        "UPDATE fork_groups SET join_activation_id = ?, join_mode = ?, failure_leg_id = ?,
                failure_settlement_class = ?, settled_legs = settled_legs + 1,
                group_state = ?, projection_version = projection_version + 1,
                settled_at = CASE WHEN ? IN ('settled', 'cancelled') THEN CURRENT_TIMESTAMP ELSE NULL END
         WHERE run_id = ? AND fork_group_id = ? AND group_state IN ('open', 'settling')
           AND projection_version = ?",
    )
    .bind(command.join_activation_id().as_str())
    .bind(join_mode_str(command.mode()))
    .bind(failure_leg.as_ref().map(|value| value.as_str()))
    .bind(failure_class.map(settlement_str))
    .bind(final_group_state)
    .bind(final_group_state)
    .bind(command.run_id().as_str())
    .bind(command.fork_group_id().as_str())
    .bind(i64_from_u64(command.expected_group_projection_version())?)
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    if group_update.rows_affected() != 1 {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    if command.mode() == JoinMode::AllSuccess
        && terminal_is_failure
        && settled_after < expected_legs
    {
        sqlx::query(
            "UPDATE control_tokens SET token_state = 'revoked', revoked_by_transition_key = ?,
                    revoked_at = CURRENT_TIMESTAMP, projection_version = projection_version + 1
             WHERE run_id = ? AND fork_group_id = ? AND token_state = 'available' AND token_id <> ?",
        )
        .bind(transition_key.as_str())
        .bind(command.run_id().as_str())
        .bind(command.fork_group_id().as_str())
        .bind(command.token_id().as_str())
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
    }
    let receipt = JoinArrivalReceipt::new(
        ControlCommitReceipt::new(seq, id.clone(), next_group_version),
        authority,
    );
    finalize(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        &id,
        &receipt,
    )
    .await?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: receipt })
}

struct DerivedLegTerminal {
    class: ExecutionLegSettlementClass,
    payload_id: Option<String>,
    artifact_id: Option<String>,
    value_hash: Option<String>,
    value_summary: Option<ExecutionValueSummary>,
}

async fn derive_leg_terminal(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    activation_id: &ActivationId,
) -> Result<Option<DerivedLegTerminal>, RepositoryError> {
    let row = sqlx::query(
        "SELECT lifecycle, output_payload_id, output_artifact_id, output_value_hash
         FROM node_activations WHERE run_id = ? AND activation_id = ?",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let lifecycle = row
        .try_get::<String, _>("lifecycle")
        .map_err(|_| invalid_data())?;
    match lifecycle.as_str() {
        "succeeded" => {
            let payload_id = row
                .try_get::<Option<String>, _>("output_payload_id")
                .map_err(|_| invalid_data())?;
            let artifact_id = row
                .try_get::<Option<String>, _>("output_artifact_id")
                .map_err(|_| invalid_data())?;
            let value_hash = row
                .try_get::<Option<String>, _>("output_value_hash")
                .map_err(|_| invalid_data())?
                .ok_or_else(invalid_data)?;
            let size = match (&payload_id, &artifact_id) {
                (Some(payload), None) => {
                    let output = sqlx::query("SELECT content_hash, canonical_bytes FROM payloads WHERE run_id = ? AND payload_id = ?")
                        .bind(run_id.as_str()).bind(payload).fetch_optional(&mut **tx).await
                        .map_err(RepositoryError::storage)?.ok_or_else(invalid_data)?;
                    if output
                        .try_get::<String, _>("content_hash")
                        .map_err(|_| invalid_data())?
                        != value_hash
                    {
                        return Err(invalid_data());
                    }
                    u64_from_i64(
                        output
                            .try_get::<i64, _>("canonical_bytes")
                            .map_err(|_| invalid_data())?,
                    )?
                }
                (None, Some(artifact)) => {
                    let output = sqlx::query("SELECT content_hash, size_bytes, artifact_state FROM artifacts WHERE run_id = ? AND artifact_id = ?")
                        .bind(run_id.as_str()).bind(artifact).fetch_optional(&mut **tx).await
                        .map_err(RepositoryError::storage)?.ok_or_else(invalid_data)?;
                    if output
                        .try_get::<String, _>("content_hash")
                        .map_err(|_| invalid_data())?
                        != value_hash
                        || !matches!(
                            output
                                .try_get::<String, _>("artifact_state")
                                .map_err(|_| invalid_data())?
                                .as_str(),
                            "verified" | "referenced"
                        )
                    {
                        return Err(invalid_data());
                    }
                    u64_from_i64(
                        output
                            .try_get::<i64, _>("size_bytes")
                            .map_err(|_| invalid_data())?,
                    )?
                }
                _ => return Err(invalid_data()),
            };
            let hash = model_data(ContentHash::parse(value_hash.clone()))?;
            Ok(Some(DerivedLegTerminal {
                class: ExecutionLegSettlementClass::Succeeded,
                payload_id,
                artifact_id,
                value_hash: Some(value_hash),
                value_summary: Some(ExecutionValueSummary::new(hash, size)),
            }))
        }
        "failed" => {
            let event = sqlx::query(
                "SELECT schema_version,event_id,run_id,transition_key,intent_hash,seq,
                        occurred_at,kind,node_id,scope_instance_id,activation_id,attempt_no,
                        causation_event_id,safe_payload
                 FROM execution_events
                 WHERE run_id = ? AND activation_id = ?
                   AND kind = 'activation.failed' ORDER BY seq DESC LIMIT 1",
            )
            .bind(run_id.as_str())
            .bind(activation_id.as_str())
            .fetch_optional(&mut **tx)
            .await
            .map_err(RepositoryError::storage)?;
            let payload = event
                .map(|row| {
                    decode_execution_event_row(&row)?;
                    row.try_get::<String, _>("safe_payload")
                        .map_err(|_| invalid_data())
                })
                .transpose()?;
            let failure = payload
                .and_then(|payload| serde_json::from_str::<ExecutionEventPayload>(&payload).ok())
                .and_then(|payload| match payload {
                    ExecutionEventPayload::ActivationFailed { failure, .. } => failure,
                    _ => None,
                });
            if failure.as_ref().map(|value| value.kind()) == Some(InternalFailureKind::Business) {
                let value = serde_json::to_value(failure.as_ref().ok_or_else(invalid_data)?)
                    .map_err(|_| RepositoryError::canonicalization())?;
                let (payload_id, value_hash) = insert_or_get_payload(tx, run_id, &value).await?;
                let size = u64_from_i64(
                    sqlx::query_scalar::<_, i64>(
                        "SELECT canonical_bytes FROM payloads
                         WHERE run_id = ? AND payload_id = ? AND content_hash = ?",
                    )
                    .bind(run_id.as_str())
                    .bind(&payload_id)
                    .bind(&value_hash)
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(RepositoryError::storage)?
                    .ok_or_else(invalid_data)?,
                )?;
                return Ok(Some(DerivedLegTerminal {
                    class: ExecutionLegSettlementClass::SafeFailure,
                    payload_id: Some(payload_id),
                    artifact_id: None,
                    value_hash: Some(value_hash.clone()),
                    value_summary: Some(ExecutionValueSummary::new(
                        model_data(ContentHash::parse(value_hash))?,
                        size,
                    )),
                }));
            }
            if failure.as_ref().map(|value| value.kind()) == Some(InternalFailureKind::Cancelled) {
                return Err(invalid_data());
            }
            let class = if failure.as_ref().map(|value| value.kind())
                == Some(InternalFailureKind::Invariant)
            {
                ExecutionLegSettlementClass::Panic
            } else {
                ExecutionLegSettlementClass::InfrastructureFailure
            };
            Ok(Some(DerivedLegTerminal {
                class,
                payload_id: None,
                artifact_id: None,
                value_hash: None,
                value_summary: None,
            }))
        }
        "cancelled" => Ok(Some(DerivedLegTerminal {
            class: ExecutionLegSettlementClass::Cancelled,
            payload_id: None,
            artifact_id: None,
            value_hash: None,
            value_summary: None,
        })),
        "timed_out" => Ok(Some(DerivedLegTerminal {
            class: ExecutionLegSettlementClass::TimedOut,
            payload_id: None,
            artifact_id: None,
            value_hash: None,
            value_summary: None,
        })),
        _ => Ok(None),
    }
}

async fn create_reuse_candidate(
    repository: &SqliteDurableRepository,
    transition_key: TransitionKey,
    command: CreateReuseCandidateCommand,
) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let _writer = repository.writer.lock().await;
    let mut tx = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    if let Some(authoritative) = replay_result::<ControlCommitReceipt>(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::ExactReplay { authoritative });
    }
    if command.run_id() == command.source_run_id()
        || command.source_control_provenance().run_id() != command.source_run_id()
        || command.source_control_provenance().source_activation_id()
            != command.source_activation_id()
    {
        return Err(RepositoryError::invalid_configuration());
    }
    let target = sqlx::query(
        "SELECT definition_revision_id, deployment_revision_id, plan_hash, binding_hash
         FROM workflow_runs WHERE run_id = ?",
    )
    .bind(command.run_id().as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let source = sqlx::query(
        "SELECT r.definition_revision_id, r.deployment_revision_id, r.plan_hash, r.binding_hash,
                a.node_id, a.lifecycle, a.effect_id, a.effect_evidence,
                a.output_payload_id, a.output_artifact_id, a.output_value_hash
         FROM workflow_runs r JOIN node_activations a ON a.run_id = r.run_id
         WHERE r.run_id = ? AND a.activation_id = ?",
    )
    .bind(command.source_run_id().as_str())
    .bind(command.source_activation_id().as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let scope_exists: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM scope_instances WHERE run_id = ? AND scope_instance_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.target_scope_instance_id().as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let (Some(target), Some(source)) = (target, source) else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    let pins_match = |row: &sqlx::sqlite::SqliteRow| -> Result<bool, RepositoryError> {
        Ok(row
            .try_get::<String, _>("definition_revision_id")
            .map_err(|_| invalid_data())?
            == command.definition_revision_id().as_str()
            && row
                .try_get::<String, _>("deployment_revision_id")
                .map_err(|_| invalid_data())?
                == command.deployment_revision_id().as_str()
            && row
                .try_get::<String, _>("plan_hash")
                .map_err(|_| invalid_data())?
                == command.plan_hash().as_str()
            && row
                .try_get::<String, _>("binding_hash")
                .map_err(|_| invalid_data())?
                == command.binding_hash().as_str())
    };
    if scope_exists.is_none()
        || !pins_match(&target)?
        || !pins_match(&source)?
        || source
            .try_get::<String, _>("node_id")
            .map_err(|_| invalid_data())?
            != command.target_node_id().as_str()
        || source
            .try_get::<String, _>("lifecycle")
            .map_err(|_| invalid_data())?
            != "succeeded"
        || source
            .try_get::<String, _>("effect_id")
            .map_err(|_| invalid_data())?
            != command.inherited_effect_id().as_str()
        || source
            .try_get::<String, _>("effect_evidence")
            .map_err(|_| invalid_data())?
            == "unknown"
        || source
            .try_get::<Option<String>, _>("output_value_hash")
            .map_err(|_| invalid_data())?
            .as_deref()
            != Some(command.output_value_hash().as_str())
        || !source_output_exists(&mut tx, command.source_run_id(), &source).await?
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let occupied: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM run_reuse_candidates WHERE run_id = ? AND
         (candidate_id = ? OR (target_scope_instance_id = ? AND target_node_id = ?
          AND stable_activation_key = ?))",
    )
    .bind(command.run_id().as_str())
    .bind(command.candidate_id())
    .bind(command.target_scope_instance_id().as_str())
    .bind(command.target_node_id().as_str())
    .bind(command.stable_activation_key())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    if occupied.is_some() {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone()),
        ExecutionEventPayload::ProjectionMutated {
            mutation: ProjectionMutationKind::ReuseCandidateCreated,
        },
    ))?;
    let (seq, id) = append_primary_event(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        0,
        &event,
    )
    .await?;
    let provenance = canonical_json(
        &serde_json::to_value(
            super::control_repository::DurableReuseProvenance::from_command(&command),
        )
        .map_err(|_| RepositoryError::canonicalization())?,
    )?;
    sqlx::query(
        "INSERT INTO run_reuse_candidates (
            run_id, candidate_id, target_scope_instance_id, target_node_id,
            stable_activation_key, source_run_id, source_activation_id,
            source_control_provenance, definition_revision_id, deployment_revision_id,
            plan_hash, binding_hash, node_config_hash, descriptor_hash, input_value_hash,
            output_value_hash, output_schema_hash, effect_policy_hash, inherited_effect_id,
            data_dependencies_hash, created_by_transition_key, candidate_state,
            materialized_activation_id, decision_transition_key, projection_version,
            created_at, decided_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   'candidate', NULL, NULL, 0, CURRENT_TIMESTAMP, NULL)",
    )
    .bind(command.run_id().as_str())
    .bind(command.candidate_id())
    .bind(command.target_scope_instance_id().as_str())
    .bind(command.target_node_id().as_str())
    .bind(command.stable_activation_key())
    .bind(command.source_run_id().as_str())
    .bind(command.source_activation_id().as_str())
    .bind(provenance)
    .bind(command.definition_revision_id().as_str())
    .bind(command.deployment_revision_id().as_str())
    .bind(command.plan_hash().as_str())
    .bind(command.binding_hash().as_str())
    .bind(command.compatibility().node_config_hash().as_str())
    .bind(command.compatibility().descriptor_hash().as_str())
    .bind(command.compatibility().input_value_hash().as_str())
    .bind(command.output_value_hash().as_str())
    .bind(command.compatibility().output_schema_hash().as_str())
    .bind(command.compatibility().effect_policy_hash().as_str())
    .bind(command.inherited_effect_id().as_str())
    .bind(command.compatibility().data_dependencies_hash().as_str())
    .bind(transition_key.as_str())
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let receipt = ControlCommitReceipt::new(seq, id.clone(), 0);
    finalize(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        &id,
        &receipt,
    )
    .await?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: receipt })
}

async fn decide_reuse_candidate(
    repository: &SqliteDurableRepository,
    transition_key: TransitionKey,
    command: RejectReuseCandidateCommand,
) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let _writer = repository.writer.lock().await;
    let mut tx = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    if let Some(authoritative) = replay_result::<ControlCommitReceipt>(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::ExactReplay { authoritative });
    }
    let next_version = command
        .expected_projection_version()
        .checked_add(1)
        .ok_or_else(invalid_data)?;
    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone()),
        ExecutionEventPayload::ProjectionMutated {
            mutation: ProjectionMutationKind::ReuseCandidateRejected,
        },
    ))?;
    let (seq, id) = append_primary_event(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        next_version,
        &event,
    )
    .await?;
    let updated = sqlx::query(
        "UPDATE run_reuse_candidates SET candidate_state = 'rejected',
                decision_transition_key = ?, rejection_reason='manual_rejection',
                projection_version = projection_version + 1,
                decided_at = CURRENT_TIMESTAMP
         WHERE run_id = ? AND candidate_id = ? AND candidate_state = 'candidate'
           AND projection_version = ?",
    )
    .bind(transition_key.as_str())
    .bind(command.run_id().as_str())
    .bind(command.candidate_id())
    .bind(i64_from_u64(command.expected_projection_version())?)
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    if updated.rows_affected() != 1 {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let receipt = ControlCommitReceipt::new(seq, id.clone(), next_version);
    finalize(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        &id,
        &receipt,
    )
    .await?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: receipt })
}

async fn materialize_reuse_candidate(
    repository: &SqliteDurableRepository,
    transition_key: TransitionKey,
    command: MaterializeReuseCandidateCommand,
) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let _writer = repository.writer.lock().await;
    let mut tx = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    if let Some(authoritative) = replay_result::<ControlCommitReceipt>(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::ExactReplay { authoritative });
    }
    let candidate =
        sqlx::query("SELECT * FROM run_reuse_candidates WHERE run_id = ? AND candidate_id = ?")
            .bind(command.run_id().as_str())
            .bind(command.candidate_id())
            .fetch_optional(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
    let Some(candidate) = candidate else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    if candidate
        .try_get::<String, _>("candidate_state")
        .map_err(|_| invalid_data())?
        != "candidate"
        || u64_from_i64(
            candidate
                .try_get::<i64, _>("projection_version")
                .map_err(|_| invalid_data())?,
        )? != command.expected_candidate_projection_version()
        || !compatibility_matches(&candidate, command.compatibility())?
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let scope_id = candidate
        .try_get::<String, _>("target_scope_instance_id")
        .map_err(|_| invalid_data())?;
    let scope = sqlx::query(
        "SELECT lifecycle, admission_state, projection_version FROM scope_instances
         WHERE run_id = ? AND scope_instance_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(&scope_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(scope) = scope else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    if scope
        .try_get::<String, _>("lifecycle")
        .map_err(|_| invalid_data())?
        != "active"
        || scope
            .try_get::<String, _>("admission_state")
            .map_err(|_| invalid_data())?
            != "open"
        || u64_from_i64(
            scope
                .try_get::<i64, _>("projection_version")
                .map_err(|_| invalid_data())?,
        )? != command.expected_scope_projection_version()
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let source_run = model_data(RunId::new(
        candidate
            .try_get::<String, _>("source_run_id")
            .map_err(|_| invalid_data())?,
    ))?;
    let source_activation = model_data(ActivationId::new(
        candidate
            .try_get::<String, _>("source_activation_id")
            .map_err(|_| invalid_data())?,
    ))?;
    let source = sqlx::query(
        "SELECT r.definition_revision_id, r.deployment_revision_id, r.plan_hash, r.binding_hash,
                a.node_id, a.lifecycle, a.execution_kind, a.effect_idempotency, a.effect_evidence,
                a.output_payload_id, a.output_artifact_id, a.output_value_hash
         FROM workflow_runs r JOIN node_activations a ON a.run_id = r.run_id
         WHERE r.run_id = ? AND a.activation_id = ?",
    )
    .bind(source_run.as_str())
    .bind(source_activation.as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let target = sqlx::query(
        "SELECT definition_revision_id, deployment_revision_id, plan_hash, binding_hash
         FROM workflow_runs WHERE run_id = ?",
    )
    .bind(command.run_id().as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let (Some(source), Some(target)) = (source, target) else {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    for (column, candidate_column) in [
        ("definition_revision_id", "definition_revision_id"),
        ("deployment_revision_id", "deployment_revision_id"),
        ("plan_hash", "plan_hash"),
        ("binding_hash", "binding_hash"),
    ] {
        let expected = candidate
            .try_get::<String, _>(candidate_column)
            .map_err(|_| invalid_data())?;
        if source
            .try_get::<String, _>(column)
            .map_err(|_| invalid_data())?
            != expected
            || target
                .try_get::<String, _>(column)
                .map_err(|_| invalid_data())?
                != expected
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
    }
    if source
        .try_get::<String, _>("lifecycle")
        .map_err(|_| invalid_data())?
        != "succeeded"
        || source
            .try_get::<String, _>("node_id")
            .map_err(|_| invalid_data())?
            != candidate
                .try_get::<String, _>("target_node_id")
                .map_err(|_| invalid_data())?
        || source
            .try_get::<String, _>("effect_evidence")
            .map_err(|_| invalid_data())?
            == "unknown"
        || source
            .try_get::<Option<String>, _>("output_value_hash")
            .map_err(|_| invalid_data())?
            != Some(
                candidate
                    .try_get::<String, _>("output_value_hash")
                    .map_err(|_| invalid_data())?,
            )
        || !source_output_exists(&mut tx, &source_run, &source).await?
    {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let occupied: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM node_activations WHERE run_id = ? AND
         (activation_id = ? OR (scope_instance_id = ? AND node_id = ? AND stable_activation_key = ?))",
    ).bind(command.run_id().as_str()).bind(command.activation_id().as_str()).bind(&scope_id)
    .bind(candidate.try_get::<String, _>("target_node_id").map_err(|_| invalid_data())?)
    .bind(candidate.try_get::<String, _>("stable_activation_key").map_err(|_| invalid_data())?)
    .fetch_optional(&mut *tx).await.map_err(RepositoryError::storage)?;
    if occupied.is_some() {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let copied = copy_source_output(&mut tx, &source_run, command.run_id(), &source).await?;
    let hash = model_data(ContentHash::parse(copied.value_hash.clone()))?;
    let scope_typed = model_data(ScopeInstanceId::new(scope_id.clone()))?;
    let node_typed = model_data(crate::engine::NodeId::new(
        candidate
            .try_get::<String, _>("target_node_id")
            .map_err(|_| invalid_data())?,
    ))?;
    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
            scope_typed,
            node_typed,
            command.activation_id().clone(),
        ),
        ExecutionEventPayload::ActivationSucceeded {
            attempt_no: None,
            output: Some(ExecutionValueSummary::new(hash, copied.size_bytes)),
        },
    ))?;
    let next_version = command
        .expected_candidate_projection_version()
        .checked_add(1)
        .ok_or_else(invalid_data)?;
    sqlx::query(
        "INSERT INTO node_activations (
            run_id, activation_id, scope_instance_id, node_id, stable_activation_key,
            execution_kind, lifecycle, effect_id, effect_idempotency, effect_evidence,
            last_attempt_no, last_lease_epoch, current_attempt_no, current_lease_epoch,
            current_fencing_token, retry_budget_remaining, pending_retry_timer_id,
            wait_registration_transition_key, termination_intent_reason,
            termination_intent_transition_key, termination_intent_at, output_payload_id,
            output_artifact_id, output_value_hash, winning_attempt_no, reused_from_run_id,
            reused_from_activation_id, projection_version, created_at, updated_at, terminal_at
         ) VALUES (?, ?, ?, ?, ?, ?, 'succeeded', ?, ?, ?, NULL, NULL, NULL, NULL, NULL,
                   0, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, NULL, ?, ?, 0,
                   CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(&scope_id)
    .bind(
        candidate
            .try_get::<String, _>("target_node_id")
            .map_err(|_| invalid_data())?,
    )
    .bind(
        candidate
            .try_get::<String, _>("stable_activation_key")
            .map_err(|_| invalid_data())?,
    )
    .bind(
        source
            .try_get::<String, _>("execution_kind")
            .map_err(|_| invalid_data())?,
    )
    .bind(
        candidate
            .try_get::<String, _>("inherited_effect_id")
            .map_err(|_| invalid_data())?,
    )
    .bind(
        source
            .try_get::<String, _>("effect_idempotency")
            .map_err(|_| invalid_data())?,
    )
    .bind(
        source
            .try_get::<String, _>("effect_evidence")
            .map_err(|_| invalid_data())?,
    )
    .bind(copied.payload_id.as_deref())
    .bind(copied.artifact_id.as_deref())
    .bind(&copied.value_hash)
    .bind(source_run.as_str())
    .bind(source_activation.as_str())
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    let (seq, id) = append_primary_event(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        next_version,
        &event,
    )
    .await?;
    let updated = sqlx::query(
        "UPDATE run_reuse_candidates SET candidate_state = 'materialized',
                materialized_activation_id = ?, decision_transition_key = ?,
                projection_version = projection_version + 1, decided_at = CURRENT_TIMESTAMP
         WHERE run_id = ? AND candidate_id = ? AND candidate_state = 'candidate'
           AND projection_version = ?",
    )
    .bind(command.activation_id().as_str())
    .bind(transition_key.as_str())
    .bind(command.run_id().as_str())
    .bind(command.candidate_id())
    .bind(i64_from_u64(
        command.expected_candidate_projection_version(),
    )?)
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    if updated.rows_affected() != 1 {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let receipt = ControlCommitReceipt::new(seq, id.clone(), next_version);
    finalize(
        &mut tx,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        &id,
        &receipt,
    )
    .await?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: receipt })
}

fn compatibility_matches(
    row: &sqlx::sqlite::SqliteRow,
    value: &super::ReuseCompatibility,
) -> Result<bool, RepositoryError> {
    Ok(row
        .try_get::<String, _>("node_config_hash")
        .map_err(|_| invalid_data())?
        == value.node_config_hash().as_str()
        && row
            .try_get::<String, _>("descriptor_hash")
            .map_err(|_| invalid_data())?
            == value.descriptor_hash().as_str()
        && row
            .try_get::<String, _>("input_value_hash")
            .map_err(|_| invalid_data())?
            == value.input_value_hash().as_str()
        && row
            .try_get::<String, _>("output_schema_hash")
            .map_err(|_| invalid_data())?
            == value.output_schema_hash().as_str()
        && row
            .try_get::<String, _>("effect_policy_hash")
            .map_err(|_| invalid_data())?
            == value.effect_policy_hash().as_str()
        && row
            .try_get::<String, _>("data_dependencies_hash")
            .map_err(|_| invalid_data())?
            == value.data_dependencies_hash().as_str())
}

pub(crate) async fn source_output_exists(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<bool, RepositoryError> {
    let payload = row
        .try_get::<Option<String>, _>("output_payload_id")
        .map_err(|_| invalid_data())?;
    let artifact = row
        .try_get::<Option<String>, _>("output_artifact_id")
        .map_err(|_| invalid_data())?;
    let hash = row
        .try_get::<Option<String>, _>("output_value_hash")
        .map_err(|_| invalid_data())?;
    match (payload, artifact, hash) {
        (Some(payload), None, Some(hash)) => {
            let row = sqlx::query(
                "SELECT content_hash,canonical_bytes,encoding,inline_value,binary_value
                 FROM payloads WHERE run_id=? AND payload_id=?",
            )
            .bind(run_id.as_str())
            .bind(&payload)
            .fetch_optional(&mut **tx)
            .await
            .map_err(RepositoryError::storage)?;
            let Some(row) = row else {
                return Ok(false);
            };
            let encoded = row
                .try_get::<Option<String>, _>("inline_value")
                .map_err(|_| invalid_data())?
                .ok_or_else(invalid_data)?;
            let value = serde_json::from_str(&encoded).map_err(|_| invalid_data())?;
            let validated = validate_inline_payload(
                &payload,
                &row.try_get::<String, _>("content_hash")
                    .map_err(|_| invalid_data())?,
                row.try_get::<i64, _>("canonical_bytes")
                    .map_err(|_| invalid_data())?,
                &row.try_get::<String, _>("encoding")
                    .map_err(|_| invalid_data())?,
                value,
                Some(&encoded),
                row.try_get::<Option<Vec<u8>>, _>("binary_value")
                    .map_err(|_| invalid_data())?
                    .is_none(),
            )?;
            Ok(validated.content_hash().as_str() == hash)
        }
        (None, Some(artifact), Some(hash)) => Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM artifacts WHERE run_id = ? AND artifact_id = ? AND content_hash = ?
             AND artifact_state IN ('verified', 'referenced')",
        ).bind(run_id.as_str()).bind(artifact).bind(hash).fetch_one(&mut **tx).await
        .map_err(RepositoryError::storage)? == 1),
        _ => Ok(false),
    }
}

pub(crate) struct CopiedOutput {
    pub(crate) payload_id: Option<String>,
    pub(crate) artifact_id: Option<String>,
    pub(crate) value_hash: String,
    pub(crate) size_bytes: u64,
}

pub(crate) async fn copy_source_output(
    tx: &mut Transaction<'_, Sqlite>,
    source_run: &RunId,
    target_run: &RunId,
    source: &sqlx::sqlite::SqliteRow,
) -> Result<CopiedOutput, RepositoryError> {
    let payload_id = source
        .try_get::<Option<String>, _>("output_payload_id")
        .map_err(|_| invalid_data())?;
    let artifact_id = source
        .try_get::<Option<String>, _>("output_artifact_id")
        .map_err(|_| invalid_data())?;
    let value_hash = source
        .try_get::<Option<String>, _>("output_value_hash")
        .map_err(|_| invalid_data())?
        .ok_or_else(invalid_data)?;
    match (&payload_id, &artifact_id) {
        (Some(payload_id), None) => {
            let row = sqlx::query(
                "SELECT content_hash, canonical_bytes, encoding, inline_value, binary_value,
                    retain_until FROM payloads WHERE run_id = ? AND payload_id = ?",
            )
            .bind(source_run.as_str())
            .bind(payload_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(invalid_data)?;
            let encoded = row
                .try_get::<Option<String>, _>("inline_value")
                .map_err(|_| invalid_data())?
                .ok_or_else(invalid_data)?;
            let value = serde_json::from_str(&encoded).map_err(|_| invalid_data())?;
            let validated = validate_inline_payload(
                payload_id,
                &row.try_get::<String, _>("content_hash")
                    .map_err(|_| invalid_data())?,
                row.try_get::<i64, _>("canonical_bytes")
                    .map_err(|_| invalid_data())?,
                &row.try_get::<String, _>("encoding")
                    .map_err(|_| invalid_data())?,
                value,
                Some(&encoded),
                row.try_get::<Option<Vec<u8>>, _>("binary_value")
                    .map_err(|_| invalid_data())?
                    .is_none(),
            )?;
            if validated.content_hash().as_str() != value_hash {
                return Err(invalid_data());
            }
            sqlx::query(
                "INSERT INTO payloads (run_id, payload_id, content_hash, canonical_bytes,
                    encoding, inline_value, binary_value, created_at, retain_until)
                 VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)",
            )
            .bind(target_run.as_str())
            .bind(payload_id)
            .bind(&value_hash)
            .bind(i64_from_u64(validated.canonical_bytes())?)
            .bind("json_jcs")
            .bind(validated.canonical())
            .bind(Option::<Vec<u8>>::None)
            .bind(
                row.try_get::<Option<String>, _>("retain_until")
                    .map_err(|_| invalid_data())?,
            )
            .execute(&mut **tx)
            .await
            .map_err(RepositoryError::storage)?;
            Ok(CopiedOutput {
                payload_id: Some(payload_id.clone()),
                artifact_id: None,
                value_hash,
                size_bytes: validated.canonical_bytes(),
            })
        }
        (None, Some(artifact_id)) => {
            let row = sqlx::query("SELECT content_hash, size_bytes, media_type, storage_uri,
                    artifact_state, verified_at, referenced_at, retain_until
                 FROM artifacts WHERE run_id = ? AND artifact_id = ? AND artifact_state IN ('verified','referenced')")
                .bind(source_run.as_str()).bind(artifact_id).fetch_optional(&mut **tx).await
                .map_err(RepositoryError::storage)?.ok_or_else(invalid_data)?;
            if row
                .try_get::<String, _>("content_hash")
                .map_err(|_| invalid_data())?
                != value_hash
            {
                return Err(invalid_data());
            }
            sqlx::query("INSERT INTO artifacts (run_id, artifact_id, content_hash, size_bytes,
                    media_type, storage_uri, artifact_state, verified_at, referenced_at, retain_until,
                    deletion_fence, deletion_claim_token, deletion_claimed_by, deletion_claim_request_key,
                    deletion_claimed_at, deletion_claim_expires_at, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, 'referenced', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?,
                         NULL, NULL, NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP)")
                .bind(target_run.as_str()).bind(artifact_id).bind(&value_hash)
                .bind(row.try_get::<i64, _>("size_bytes").map_err(|_| invalid_data())?)
                .bind(row.try_get::<Option<String>, _>("media_type").map_err(|_| invalid_data())?)
                .bind(row.try_get::<String, _>("storage_uri").map_err(|_| invalid_data())?)
                .bind(row.try_get::<Option<String>, _>("retain_until").map_err(|_| invalid_data())?)
                .execute(&mut **tx).await.map_err(RepositoryError::storage)?;
            Ok(CopiedOutput {
                payload_id: None,
                artifact_id: Some(artifact_id.clone()),
                value_hash,
                size_bytes: u64_from_i64(
                    row.try_get::<i64, _>("size_bytes")
                        .map_err(|_| invalid_data())?,
                )?,
            })
        }
        _ => Err(invalid_data()),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::engine::repository::{
        ActivationAdmissionCommand, ActivationCasCommand, ActivationDurableRepository,
        CreateRunCommand, DurableRepository, ForkLegAdmission, PlanInstallOutcome, VersionedPlan,
    };
    use crate::engine::{
        control::ControlEmissionSlot, ActivationId, ChildRequirement, DefinitionRevisionId,
        DeploymentRevisionId, DynamicKey, ExecutionKind, ForkGroupId, ForkLeg, LegId, NodeId,
        PortId, ScopeInstance,
    };

    fn key(label: &str) -> TransitionKey {
        TransitionKey::derive("repository.control.test", &[label]).unwrap()
    }

    fn test_plan(label: &str) -> VersionedPlan {
        VersionedPlan::new_for_test(
            format!("definition_{label}"),
            format!("agent_{label}"),
            "Control repository test",
            DefinitionRevisionId::new(format!("definition_revision_{label}")).unwrap(),
            DeploymentRevisionId::new(format!("deployment_revision_{label}")).unwrap(),
            ContentHash::from_bytes(format!("plan_{label}").as_bytes()),
            ContentHash::from_bytes(format!("binding_{label}").as_bytes()),
            "compiler-3.0.0",
            "expression-3.0.0",
            json!({"kind": "structured"}),
            json!({"nodes": []}),
            json!({}),
            json!({}),
            json!({"worker": "v1"}),
        )
        .unwrap()
    }

    async fn repository_with_run(label: &str) -> (SqliteDurableRepository, RunId) {
        let repository = SqliteDurableRepository::in_memory().await.unwrap();
        let plan = test_plan(label);
        assert_eq!(
            repository.install_versioned_plan(&plan).await.unwrap(),
            PlanInstallOutcome::Installed
        );
        let run_id = RunId::new(format!("run_{label}")).unwrap();
        repository
            .create_run(
                key(&format!("{label}.run.create")),
                CreateRunCommand::new(run_id.clone(), &plan, json!({})).unwrap(),
            )
            .await
            .unwrap();
        (repository, run_id)
    }

    async fn scope_version(repository: &SqliteDurableRepository, run_id: &RunId) -> u64 {
        u64_from_i64(
            sqlx::query_scalar::<_, i64>(
                "SELECT projection_version FROM scope_instances
                 WHERE run_id = ? AND scope_instance_id = 'scope_root'",
            )
            .bind(run_id.as_str())
            .fetch_one(&repository.pool)
            .await
            .unwrap(),
        )
        .unwrap()
    }

    async fn admit_ready(
        repository: &SqliteDurableRepository,
        run_id: &RunId,
        label: &str,
        scope_version: u64,
    ) -> ActivationId {
        let activation_id = ActivationId::new(format!("activation_{label}")).unwrap();
        repository
            .admit_activation(
                key(&format!("{label}.admit")),
                ActivationAdmissionCommand::new(
                    run_id.clone(),
                    ScopeInstanceId::root(),
                    scope_version,
                    activation_id.clone(),
                    NodeId::new(format!("node_{label}")).unwrap(),
                    format!("stable_{label}"),
                    ExecutionKind::SchedulerNative,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        repository
            .make_activation_ready(
                key(&format!("{label}.ready")),
                ActivationCasCommand::new(run_id.clone(), activation_id.clone(), 0),
            )
            .await
            .unwrap();
        activation_id
    }

    #[tokio::test]
    async fn child_scope_counts_and_token_first_winners_are_transactional() {
        let (repository, run_id) = repository_with_run("scope_token").await;
        let child = ScopeInstance::subflow(
            &ScopeInstanceId::root(),
            NodeId::new("owner").unwrap(),
            DynamicKey::new("invocation_1").unwrap(),
        )
        .unwrap();
        let create = CreateChildScopeCommand::new(run_id.clone(), child.clone(), 0).unwrap();
        let create_key = key("scope_token.child.create");
        let applied = repository
            .create_child_scope(create_key.clone(), create.clone())
            .await
            .unwrap();
        assert!(matches!(applied, TransitionOutcome::Committed { .. }));
        assert!(matches!(
            repository
                .create_child_scope(create_key, create)
                .await
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        let parent = sqlx::query(
            "SELECT admitted_children, settled_children, projection_version
             FROM scope_instances WHERE run_id = ? AND scope_instance_id = 'scope_root'",
        )
        .bind(run_id.as_str())
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        assert_eq!(parent.get::<i64, _>("admitted_children"), 1);
        assert_eq!(parent.get::<i64, _>("settled_children"), 0);
        assert_eq!(parent.get::<i64, _>("projection_version"), 1);

        repository
            .close_scope_admission(
                key("scope_token.child.close"),
                CloseScopeAdmissionCommand::new(run_id.clone(), child.id().clone(), 0),
            )
            .await
            .unwrap();
        repository
            .settle_scope(
                key("scope_token.child.settle"),
                SettleScopeCommand::new(run_id.clone(), child.id().clone(), 1),
            )
            .await
            .unwrap();
        let parent = sqlx::query(
            "SELECT admitted_children, settled_children FROM scope_instances
             WHERE run_id = ? AND scope_instance_id = 'scope_root'",
        )
        .bind(run_id.as_str())
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        assert_eq!(parent.get::<i64, _>("admitted_children"), 1);
        assert_eq!(parent.get::<i64, _>("settled_children"), 1);

        let activation = admit_ready(
            &repository,
            &run_id,
            "scope_token_source",
            scope_version(&repository, &run_id).await,
        )
        .await;
        let provenance = ControlTokenProvenance::new(
            run_id.clone(),
            activation.clone(),
            PortId::new("out").unwrap(),
            ControlEmissionSlot::ActivationOutput,
            ScopeInstanceId::root(),
            vec![],
        )
        .unwrap();
        let token_id = crate::engine::ControlTokenId::new("token_scope_token").unwrap();
        repository
            .emit_control_token(
                key("scope_token.emit"),
                EmitControlTokenCommand::new(token_id.clone(), provenance.clone(), 1),
            )
            .await
            .unwrap();
        assert_eq!(
            repository
                .emit_control_token(
                    key("scope_token.emit.other"),
                    EmitControlTokenCommand::new(
                        crate::engine::ControlTokenId::new("token_other").unwrap(),
                        provenance,
                        1,
                    ),
                )
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );
        let consume_key = key("scope_token.consume");
        let consume = ConsumeControlTokenCommand::new(
            run_id.clone(),
            token_id.clone(),
            activation,
            super::super::TokenConsumerKind::Activation,
            0,
        );
        assert!(matches!(
            repository
                .consume_control_token(consume_key.clone(), consume.clone())
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            repository
                .consume_control_token(consume_key, consume)
                .await
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        assert_eq!(
            repository
                .revoke_control_token(
                    key("scope_token.revoke_loser"),
                    RevokeControlTokenCommand::new(run_id, token_id, 1),
                )
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );
    }

    #[tokio::test]
    async fn fork_and_join_commit_ordered_legs_tokens_scopes_and_barrier_atomically() {
        let (repository, run_id) = repository_with_run("fork_join").await;
        let fork_activation = admit_ready(&repository, &run_id, "fork", 0).await;
        let owner = NodeId::new("fork_owner").unwrap();
        let group_id = ForkGroupId::new("fork_group").unwrap();
        let mut admissions = Vec::new();
        for (index, leg_name) in ["a", "b"].into_iter().enumerate() {
            let leg_id = LegId::new(leg_name).unwrap();
            let scope = ScopeInstance::parallel_leg(
                &ScopeInstanceId::root(),
                owner.clone(),
                leg_id.clone(),
            )
            .unwrap();
            let child = ActivationId::new(format!("activation_child_{leg_name}")).unwrap();
            admissions.push(
                ForkLegAdmission::new(
                    ForkLeg::new(
                        run_id.clone(),
                        leg_id,
                        PortId::new(format!("out_{leg_name}")).unwrap(),
                        scope.id().clone(),
                        child,
                        ChildRequirement::Required,
                    ),
                    scope,
                    NodeId::new(format!("child_{leg_name}")).unwrap(),
                    format!("child_{index}"),
                    ExecutionKind::SchedulerNative,
                    crate::engine::ControlTokenId::new(format!("token_{leg_name}")).unwrap(),
                )
                .unwrap(),
            );
        }
        let command = CreateForkCommand::new(
            run_id.clone(),
            group_id.clone(),
            fork_activation,
            ScopeInstanceId::root(),
            1,
            0,
            None,
            admissions.clone(),
        )
        .unwrap();
        let fork_key = key("fork_join.create");
        assert!(matches!(
            repository
                .create_fork(fork_key.clone(), command.clone())
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            repository.create_fork(fork_key, command).await.unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM fork_legs WHERE run_id = ? AND fork_group_id = ?",
            )
            .bind(run_id.as_str())
            .bind(group_id.as_str())
            .fetch_one(&repository.pool)
            .await
            .unwrap(),
            2
        );

        let join_activation = admit_ready(&repository, &run_id, "join", 1).await;
        for admission in &admissions {
            let payload_id = format!("payload_{}", admission.leg().leg_id().as_str());
            let value = json!({"leg": admission.leg().leg_id().as_str()});
            let encoded = serde_jcs::to_string(&value).unwrap();
            let hash = ContentHash::from_bytes(encoded.as_bytes());
            sqlx::query(
                "INSERT INTO payloads (run_id, payload_id, content_hash, canonical_bytes,
                    encoding, inline_value, binary_value, created_at, retain_until)
                 VALUES (?, ?, ?, ?, 'json_jcs', ?, NULL, CURRENT_TIMESTAMP, NULL)",
            )
            .bind(run_id.as_str())
            .bind(&payload_id)
            .bind(hash.as_str())
            .bind(i64::try_from(encoded.len()).unwrap())
            .bind(encoded)
            .execute(&repository.pool)
            .await
            .unwrap();
            sqlx::query(
                "UPDATE node_activations SET lifecycle = 'succeeded', output_payload_id = ?,
                    output_value_hash = ?, terminal_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                 WHERE run_id = ? AND activation_id = ?",
            )
            .bind(&payload_id)
            .bind(hash.as_str())
            .bind(run_id.as_str())
            .bind(admission.leg().child_activation_id().as_str())
            .execute(&repository.pool)
            .await
            .unwrap();
        }
        let first = RecordJoinArrivalCommand::new(
            run_id.clone(),
            join_activation.clone(),
            group_id.clone(),
            admissions[0].leg().leg_id().clone(),
            admissions[0].token_id().clone(),
            JoinMode::AllSuccess,
            0,
            0,
        );
        let first_key = key("fork_join.arrive.a");
        let first_result = repository
            .record_join_arrival(first_key, first.clone())
            .await
            .unwrap();
        assert!(matches!(
            first_result.committed_result().unwrap().authority(),
            JoinBarrierAuthority::Pending {
                settled_legs: 1,
                ..
            }
        ));
        assert!(matches!(
            repository
                .record_join_arrival(key("fork_join.arrive.a.duplicate"), first)
                .await
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        let second = RecordJoinArrivalCommand::new(
            run_id.clone(),
            join_activation,
            group_id.clone(),
            admissions[1].leg().leg_id().clone(),
            admissions[1].token_id().clone(),
            JoinMode::AllSuccess,
            1,
            0,
        );
        let second_result = repository
            .record_join_arrival(key("fork_join.arrive.b"), second)
            .await
            .unwrap();
        assert!(matches!(
            second_result.committed_result().unwrap().authority(),
            JoinBarrierAuthority::Ready {
                settled_legs: 2,
                ..
            }
        ));
        let group = sqlx::query(
            "SELECT group_state, admitted_legs, settled_legs FROM fork_groups
             WHERE run_id = ? AND fork_group_id = ?",
        )
        .bind(run_id.as_str())
        .bind(group_id.as_str())
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        assert_eq!(group.get::<String, _>("group_state"), "settled");
        assert_eq!(group.get::<i64, _>("admitted_legs"), 2);
        assert_eq!(group.get::<i64, _>("settled_legs"), 2);
    }

    #[tokio::test]
    async fn reuse_candidate_does_not_create_an_activation_until_materialization() {
        let (repository, source_run) = repository_with_run("reuse").await;
        let plan = test_plan("reuse");
        let target_run = RunId::new("run_reuse_target").unwrap();
        repository
            .create_run(
                key("reuse.target.create"),
                CreateRunCommand::new(target_run.clone(), &plan, json!({})).unwrap(),
            )
            .await
            .unwrap();
        let source_activation = admit_ready(&repository, &source_run, "reuse_source", 0).await;
        let encoded = serde_jcs::to_string(&json!({"answer": 42})).unwrap();
        let output_hash = ContentHash::from_bytes(encoded.as_bytes());
        let output_payload_id = super::super::common::payload_id(&output_hash);
        sqlx::query(
            "INSERT INTO payloads (run_id, payload_id, content_hash, canonical_bytes,
                encoding, inline_value, binary_value, created_at, retain_until)
             VALUES (?, ?, ?, ?, 'json_jcs', ?, NULL, CURRENT_TIMESTAMP, NULL)",
        )
        .bind(source_run.as_str())
        .bind(&output_payload_id)
        .bind(output_hash.as_str())
        .bind(i64::try_from(encoded.len()).unwrap())
        .bind(&encoded)
        .execute(&repository.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE node_activations SET lifecycle = 'succeeded',
                output_payload_id = ?, output_value_hash = ?,
                updated_at = CURRENT_TIMESTAMP, terminal_at = CURRENT_TIMESTAMP
             WHERE run_id = ? AND activation_id = ?",
        )
        .bind(&output_payload_id)
        .bind(output_hash.as_str())
        .bind(source_run.as_str())
        .bind(source_activation.as_str())
        .execute(&repository.pool)
        .await
        .unwrap();
        let provenance = ControlTokenProvenance::new(
            source_run.clone(),
            source_activation.clone(),
            PortId::new("out").unwrap(),
            ControlEmissionSlot::ActivationOutput,
            ScopeInstanceId::root(),
            vec![],
        )
        .unwrap();
        let compatibility = super::super::ReuseCompatibility::new(
            ContentHash::from_bytes(b"node-config"),
            ContentHash::from_bytes(b"descriptor"),
            ContentHash::from_bytes(b"input"),
            ContentHash::from_bytes(b"output-schema"),
            ContentHash::from_bytes(b"effect-policy"),
            ContentHash::from_bytes(b"dependencies"),
        );
        let candidate = CreateReuseCandidateCommand::new(
            target_run.clone(),
            "candidate_reuse",
            ScopeInstanceId::root(),
            NodeId::new("node_reuse_source").unwrap(),
            "stable_reused",
            source_run.clone(),
            source_activation.clone(),
            provenance,
            plan.definition_revision_id().clone(),
            plan.deployment_revision_id().clone(),
            plan.plan_hash().clone(),
            plan.binding_hash().clone(),
            output_hash.clone(),
            crate::engine::EffectId::for_activation(&source_run, &source_activation),
            compatibility.clone(),
        )
        .unwrap();
        assert!(matches!(
            repository
                .create_reuse_candidate(key("reuse.candidate"), candidate)
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM node_activations WHERE run_id = ?",)
                .bind(target_run.as_str())
                .fetch_one(&repository.pool)
                .await
                .unwrap(),
            0
        );
        let materialized = ActivationId::new("activation_reused").unwrap();
        sqlx::query("UPDATE payloads SET inline_value=? WHERE run_id=? AND payload_id=?")
            .bind(serde_jcs::to_string(&json!({"answer": 43})).unwrap())
            .bind(source_run.as_str())
            .bind(&output_payload_id)
            .execute(&repository.pool)
            .await
            .unwrap();
        let corruption = repository
            .materialize_reuse_candidate(
                key("reuse.materialize"),
                MaterializeReuseCandidateCommand::new(
                    target_run.clone(),
                    "candidate_reuse",
                    materialized.clone(),
                    0,
                    0,
                    compatibility.clone(),
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert_eq!(corruption.code(), super::super::REPOSITORY_DATA_INVALID);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM node_activations WHERE run_id=?")
                .bind(target_run.as_str())
                .fetch_one(&repository.pool)
                .await
                .unwrap(),
            0
        );
        sqlx::query("UPDATE payloads SET inline_value=? WHERE run_id=? AND payload_id=?")
            .bind(&encoded)
            .bind(source_run.as_str())
            .bind(&output_payload_id)
            .execute(&repository.pool)
            .await
            .unwrap();
        assert!(matches!(
            repository
                .materialize_reuse_candidate(
                    key("reuse.materialize"),
                    MaterializeReuseCandidateCommand::new(
                        target_run.clone(),
                        "candidate_reuse",
                        materialized.clone(),
                        0,
                        0,
                        compatibility,
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        let row = sqlx::query(
            "SELECT lifecycle, reused_from_run_id, reused_from_activation_id,
                    output_value_hash
             FROM node_activations WHERE run_id = ? AND activation_id = ?",
        )
        .bind(target_run.as_str())
        .bind(materialized.as_str())
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("lifecycle"), "succeeded");
        assert_eq!(
            row.get::<String, _>("reused_from_run_id"),
            source_run.as_str()
        );
        assert_eq!(
            row.get::<String, _>("reused_from_activation_id"),
            source_activation.as_str()
        );
        assert_eq!(
            row.get::<String, _>("output_value_hash"),
            output_hash.as_str()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM node_attempts WHERE run_id = ? AND activation_id = ?",
            )
            .bind(target_run.as_str())
            .bind(materialized.as_str())
            .fetch_one(&repository.pool)
            .await
            .unwrap(),
            0
        );
    }
}
