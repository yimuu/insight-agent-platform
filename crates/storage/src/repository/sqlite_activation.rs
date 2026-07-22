use super::RepositoryErrorExt as _;

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use insight_durable::activation::adapter::{
    self as activation_adapter, effect_evidence_str, execution_kind_fields,
    parse_activation_lifecycle, parse_effect_evidence,
};
use sqlx::{sqlite::SqliteRow, Row, Sqlite, Transaction};
use uuid::Uuid;

use insight_durable::common::adapter::{
    self as common_contract_adapter, canonical_intent_hash, canonical_json,
    companion_event_intent_hash, companion_transition_key, decode_execution_event_schema_version,
    event_id, fencing_token, i64_from_u64, task_id, timer_id, u64_from_i64,
    validate_inline_payload, wait_late_audit_identity,
};

use insight_engine::{
    ActivationLifecycle, ActivationTerminationReason, ArtifactId, ArtifactRef, AttemptNo,
    EffectEvidence, ExecutionEventContext, ExecutionEventId, ExecutionEventPayload,
    ExecutionValueSummary, LeaseEpoch, LeaseFence, PendingExecutionEvent, ProjectionMutationKind,
    RunId, SignalId, TimerId, TransitionKey, TransitionOutcome, ValueRef,
};

use super::sqlite::{
    allocate_event_seq, decode_execution_event_row as decode_closed_execution_event_row,
    insert_event, insert_or_get_payload, load_replay, Replay,
};
use super::sqlite_projection::{
    append_projection_mutation_event, finalize_empty_projection_checkpoints,
    finalize_projection_checkpoints,
};
use super::{
    ActivationAdmissionCommand, ActivationCasCommand, ActivationCommitReceipt,
    ActivationDurableRepository, ActivationProjection, ActivationTimerKind, AttemptCompletion,
    AttemptCompletionAuthority, CompleteAttemptCommand, FencedAttemptCommand, FireTimerCommand,
    GrantAttemptLeaseCommand, HeartbeatAttemptCommand, LeaseGrantAuthority, ReceiveSignalCommand,
    RecordEffectEvidenceCommand, RegisterWaitCommand, RepositoryError, ResolveSignalCommand,
    RetryScheduleAuthority, ScheduleActivationTimerCommand, SignalReceipt, SqliteDurableRepository,
    TaskClaim, TaskEnvelope, TimerFireAuthority, WaitResolutionAuthority,
};

fn now_text(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn parse_time(value: &str) -> Result<DateTime<Utc>, RepositoryError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| RepositoryError::invalid_data())
}

fn model_data<T>(value: Result<T, insight_engine::ModelError>) -> Result<T, RepositoryError> {
    value.map_err(|_| RepositoryError::invalid_data())
}

fn decode_execution_event_schema_row(row: &SqliteRow) -> Result<(), RepositoryError> {
    decode_execution_event_schema_version(
        row.try_get::<i64, _>("execution_event_schema_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
}

fn make_receipt(
    replay: &super::CommitReceipt,
    activation_projection_version: u64,
    scope_projection_version: Option<u64>,
) -> ActivationCommitReceipt {
    activation_adapter::activation_commit_receipt(
        replay.event_seq(),
        replay.event_id().to_owned(),
        activation_projection_version,
        scope_projection_version,
    )
}

async fn activation_version(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    activation_id: &insight_engine::ActivationId,
) -> Result<u64, RepositoryError> {
    let value = sqlx::query_scalar::<_, i64>(
        "SELECT projection_version FROM node_activations
         WHERE run_id = ? AND activation_id = ?",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::activation_not_found)?;
    u64_from_i64(value)
}

async fn replay_activation_receipt(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    activation_id: &insight_engine::ActivationId,
    transition_key: &TransitionKey,
    intent_hash: &str,
) -> Result<Option<TransitionOutcome<ActivationCommitReceipt>>, RepositoryError> {
    match load_replay(transaction, run_id, transition_key, intent_hash).await? {
        Replay::Vacant => Ok(None),
        Replay::Exact(replay) => {
            let version = activation_version(transaction, run_id, activation_id).await?;
            Ok(Some(TransitionOutcome::ExactReplay {
                authoritative: make_receipt(&replay, version, None),
            }))
        }
    }
}

async fn insert_closed_event(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    transition_key: &TransitionKey,
    intent_hash: &str,
    projection_version: u64,
    event: PendingExecutionEvent,
) -> Result<ActivationCommitReceipt, RepositoryError> {
    let seq = allocate_event_seq(transaction, run_id).await?;
    let id = event_id(transition_key);
    insert_event(
        transaction,
        run_id,
        seq,
        &id,
        transition_key,
        intent_hash,
        projection_version,
        &event,
    )
    .await?;
    Ok(activation_adapter::activation_commit_receipt(
        seq,
        id,
        projection_version,
        None,
    ))
}

async fn insert_companion_event(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    primary_transition_key: &TransitionKey,
    primary_event_id: &str,
    role: &str,
    projection_version: u64,
    event: PendingExecutionEvent,
) -> Result<ActivationCommitReceipt, RepositoryError> {
    let transition_key = companion_transition_key(primary_transition_key, role)?;
    let intent_hash =
        companion_event_intent_hash(primary_transition_key, primary_event_id, role, &event)?;
    if !matches!(
        load_replay(transaction, run_id, &transition_key, intent_hash.as_str()).await?,
        Replay::Vacant
    ) {
        return Err(RepositoryError::invalid_data());
    }
    let receipt = insert_closed_event(
        transaction,
        run_id,
        &transition_key,
        intent_hash.as_str(),
        projection_version,
        event,
    )
    .await?;
    finalize_empty_projection_checkpoints(transaction, run_id, receipt.event_id()).await?;
    Ok(receipt)
}

async fn verify_companion_event(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    primary_transition_key: &TransitionKey,
    primary_event_id: &str,
    role: &str,
    event: &PendingExecutionEvent,
) -> Result<(), RepositoryError> {
    let transition_key = companion_transition_key(primary_transition_key, role)?;
    let intent_hash =
        companion_event_intent_hash(primary_transition_key, primary_event_id, role, event)?;
    if !matches!(
        load_replay(transaction, run_id, &transition_key, intent_hash.as_str()).await?,
        Replay::Exact(_)
    ) {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

fn attempt_context(
    run_id: &RunId,
    scope_id: &str,
    node_id: &str,
    activation_id: &insight_engine::ActivationId,
    attempt_no: AttemptNo,
) -> Result<ExecutionEventContext, RepositoryError> {
    Ok(ExecutionEventContext::for_run(run_id.clone()).for_attempt(
        model_data(insight_engine::ScopeInstanceId::new(scope_id))?,
        model_data(insight_engine::NodeId::new(node_id))?,
        activation_id.clone(),
        attempt_no,
    ))
}

async fn load_attempt_identity(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &FencedAttemptCommand,
) -> Result<Option<(String, String, String, String, String, i64, i64)>, RepositoryError> {
    let row = sqlx::query(
        "SELECT a.scope_instance_id, a.node_id, a.lifecycle AS activation_lifecycle,
                t.lifecycle AS attempt_lifecycle, t.effect_evidence,
                a.projection_version AS activation_version,
                t.projection_version AS attempt_version
         FROM node_activations a
         JOIN node_attempts t ON t.run_id = a.run_id AND t.activation_id = a.activation_id
         WHERE a.run_id = ? AND a.activation_id = ? AND t.attempt_no = ?
           AND t.lease_epoch = ? AND t.fencing_token = ? AND t.worker_id = ?
           AND a.current_attempt_no = t.attempt_no
           AND a.current_lease_epoch = t.lease_epoch
           AND a.current_fencing_token = t.fencing_token",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(i64::from(command.fence().attempt_no().get()))
    .bind(i64_from_u64(command.fence().lease_epoch().get())?)
    .bind(command.fencing_token())
    .bind(command.worker_id())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    row.map(|row| {
        Ok((
            row.try_get("scope_instance_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("node_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("activation_lifecycle")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("attempt_lifecycle")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("effect_evidence")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("activation_version")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("attempt_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))
    })
    .transpose()
}

fn value_summary(value: &ValueRef) -> ExecutionValueSummary {
    let size = match value {
        ValueRef::Inline(value) => value.canonical_bytes(),
        ValueRef::Artifact(value) => value.size_bytes(),
    };
    ExecutionValueSummary::new(value.content_hash().clone(), size)
}

async fn stored_value_ref(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    payload_id: Option<&str>,
    artifact_id: Option<&str>,
) -> Result<ValueRef, RepositoryError> {
    if let Some(payload_id) = payload_id {
        let row = sqlx::query(
            "SELECT payload_id,content_hash,canonical_bytes,encoding,inline_value,binary_value
             FROM payloads WHERE run_id = ? AND payload_id = ?",
        )
        .bind(run_id.as_str())
        .bind(payload_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?
        .ok_or_else(RepositoryError::invalid_data)?;
        let stored_payload_id = row
            .try_get::<String, _>("payload_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        if stored_payload_id != payload_id {
            return Err(RepositoryError::invalid_data());
        }
        let encoded = row
            .try_get::<Option<String>, _>("inline_value")
            .map_err(|_| RepositoryError::invalid_data())?
            .ok_or_else(RepositoryError::invalid_data)?;
        let value = serde_json::from_str(&encoded).map_err(|_| RepositoryError::invalid_data())?;
        let validated = validate_inline_payload(
            &stored_payload_id,
            &row.try_get::<String, _>("content_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get::<i64, _>("canonical_bytes")
                .map_err(|_| RepositoryError::invalid_data())?,
            &row.try_get::<String, _>("encoding")
                .map_err(|_| RepositoryError::invalid_data())?,
            value,
            Some(&encoded),
            row.try_get::<Option<Vec<u8>>, _>("binary_value")
                .map_err(|_| RepositoryError::invalid_data())?
                .is_none(),
        )?;
        return model_data(ValueRef::inline(
            common_contract_adapter::validated_inline_payload_value(&validated).clone(),
        ));
    }
    let artifact_id = artifact_id.ok_or_else(RepositoryError::invalid_data)?;
    let row = sqlx::query(
        "SELECT content_hash, size_bytes, media_type FROM artifacts
         WHERE run_id = ? AND artifact_id = ? AND artifact_state IN ('verified', 'referenced')",
    )
    .bind(run_id.as_str())
    .bind(artifact_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let content_hash = model_data(insight_engine::ContentHash::parse(
        row.try_get::<String, _>("content_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let size = u64_from_i64(
        row.try_get("size_bytes")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let media_type = row
        .try_get("media_type")
        .map_err(|_| RepositoryError::invalid_data())?;
    Ok(ValueRef::Artifact(model_data(ArtifactRef::new(
        model_data(ArtifactId::new(artifact_id))?,
        content_hash,
        size,
        media_type,
    ))?))
}

async fn persist_value_ref(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    value: &ValueRef,
) -> Result<(Option<String>, Option<String>, String), RepositoryError> {
    match value {
        ValueRef::Inline(inline) => {
            let (payload_id, hash) =
                insert_or_get_payload(transaction, run_id, inline.value()).await?;
            Ok((Some(payload_id), None, hash))
        }
        ValueRef::Artifact(artifact) => {
            let exists = sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM artifacts WHERE run_id = ? AND artifact_id = ?
                 AND content_hash = ? AND artifact_state IN ('verified', 'referenced')",
            )
            .bind(run_id.as_str())
            .bind(artifact.artifact_id().as_str())
            .bind(artifact.content_hash().as_str())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if exists.is_none() {
                return Err(RepositoryError::invalid_data());
            }
            Ok((
                None,
                Some(artifact.artifact_id().as_str().to_owned()),
                artifact.content_hash().as_str().to_owned(),
            ))
        }
    }
}

#[async_trait::async_trait]
impl ActivationDurableRepository for SqliteDurableRepository {
    async fn admit_activation(
        &self,
        transition_key: TransitionKey,
        command: ActivationAdmissionCommand,
    ) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError> {
        let intent_hash = canonical_intent_hash(&command)?;
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        match load_replay(
            &mut transaction,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            Replay::Exact(replay) => {
                let activation_version =
                    activation_version(&mut transaction, command.run_id(), command.activation_id())
                        .await?;
                transaction
                    .commit()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::ExactReplay {
                    authoritative: make_receipt(&replay, activation_version, None),
                });
            }
            Replay::Vacant => {}
        }

        let occupied = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM node_activations WHERE run_id = ?
             AND (activation_id = ? OR (scope_instance_id = ? AND node_id = ?
                  AND stable_activation_key = ?))",
        )
        .bind(command.run_id().as_str())
        .bind(command.activation_id().as_str())
        .bind(command.scope_instance_id().as_str())
        .bind(command.node_id().as_str())
        .bind(activation_adapter::activation_admission_stable_key(
            &command,
        ))
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if occupied.is_some() {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let expected_scope_version = i64_from_u64(command.expected_scope_projection_version())?;
        let scope_rows = sqlx::query(
            "SELECT 1 FROM scope_instances
             WHERE run_id = ? AND scope_instance_id = ? AND lifecycle = 'active'
               AND admission_state = 'open' AND projection_version = ?",
        )
        .bind(command.run_id().as_str())
        .bind(command.scope_instance_id().as_str())
        .bind(expected_scope_version)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if scope_rows.is_none() {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let effect_id = command.effect_id();
        let (execution_kind, idempotency, retry_budget) =
            execution_kind_fields(command.execution_kind());
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
             ) VALUES (?, ?, ?, ?, ?, ?, 'created', ?, ?, 'not_started',
                       NULL, NULL, NULL, NULL, NULL, ?, NULL, NULL, NULL, NULL, NULL,
                       NULL, NULL, NULL, NULL, NULL, NULL, 0, CURRENT_TIMESTAMP,
                       CURRENT_TIMESTAMP, NULL)",
        )
        .bind(command.run_id().as_str())
        .bind(command.activation_id().as_str())
        .bind(command.scope_instance_id().as_str())
        .bind(command.node_id().as_str())
        .bind(activation_adapter::activation_admission_stable_key(
            &command,
        ))
        .bind(execution_kind)
        .bind(effect_id.as_str())
        .bind(idempotency)
        .bind(i64::from(retry_budget))
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let event = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
                command.scope_instance_id().clone(),
                command.node_id().clone(),
                command.activation_id().clone(),
            ),
            ExecutionEventPayload::ActivationCreated {
                effect_id: Some(effect_id),
            },
        ))?;
        let receipt = insert_closed_event(
            &mut transaction,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
            0,
            event,
        )
        .await?;
        let receipt = activation_adapter::activation_commit_receipt(
            receipt.event_seq(),
            receipt.event_id().to_owned(),
            receipt.activation_projection_version(),
            None,
        );
        finalize_projection_checkpoints(&mut transaction, command.run_id(), receipt.event_id())
            .await?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn make_activation_ready(
        &self,
        transition_key: TransitionKey,
        command: ActivationCasCommand,
    ) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError> {
        let intent_hash = canonical_intent_hash(&command)?;
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(outcome) = replay_activation_receipt(
            &mut transaction,
            command.run_id(),
            command.activation_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(outcome);
        }
        let next_version = command
            .expected_projection_version()
            .checked_add(1)
            .ok_or_else(RepositoryError::invalid_data)?;
        let rows = sqlx::query(
            "UPDATE node_activations SET lifecycle = 'ready', projection_version = ?,
                    updated_at = CURRENT_TIMESTAMP
             WHERE run_id = ? AND activation_id = ? AND lifecycle = 'created'
               AND projection_version = ?",
        )
        .bind(i64_from_u64(next_version)?)
        .bind(command.run_id().as_str())
        .bind(command.activation_id().as_str())
        .bind(i64_from_u64(command.expected_projection_version())?)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows == 0 {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let identity = sqlx::query(
            "SELECT scope_instance_id, node_id FROM node_activations
             WHERE run_id = ? AND activation_id = ?",
        )
        .bind(command.run_id().as_str())
        .bind(command.activation_id().as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let event = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
                model_data(insight_engine::ScopeInstanceId::new(
                    identity
                        .try_get::<String, _>("scope_instance_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                model_data(insight_engine::NodeId::new(
                    identity
                        .try_get::<String, _>("node_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                command.activation_id().clone(),
            ),
            ExecutionEventPayload::ActivationReady,
        ))?;
        let receipt = insert_closed_event(
            &mut transaction,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
            next_version,
            event,
        )
        .await?;
        finalize_projection_checkpoints(&mut transaction, command.run_id(), receipt.event_id())
            .await?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn grant_attempt_lease(
        &self,
        transition_key: TransitionKey,
        command: GrantAttemptLeaseCommand,
    ) -> Result<TransitionOutcome<LeaseGrantAuthority>, RepositoryError> {
        grant_attempt_lease(self, transition_key, command).await
    }

    async fn mark_attempt_running(
        &self,
        transition_key: TransitionKey,
        command: FencedAttemptCommand,
    ) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError> {
        fenced_state_change(self, transition_key, command, true, None).await
    }

    async fn heartbeat_attempt(
        &self,
        command: HeartbeatAttemptCommand,
    ) -> Result<TransitionOutcome<()>, RepositoryError> {
        heartbeat_attempt(self, command).await
    }

    async fn record_effect_evidence(
        &self,
        transition_key: TransitionKey,
        command: RecordEffectEvidenceCommand,
    ) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError> {
        fenced_state_change(
            self,
            transition_key,
            command.authority().clone(),
            false,
            Some(command.evidence()),
        )
        .await
    }

    async fn complete_attempt(
        &self,
        transition_key: TransitionKey,
        command: CompleteAttemptCommand,
    ) -> Result<TransitionOutcome<AttemptCompletionAuthority>, RepositoryError> {
        complete_attempt(self, transition_key, command).await
    }

    async fn register_wait(
        &self,
        transition_key: TransitionKey,
        command: RegisterWaitCommand,
    ) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError> {
        register_wait(self, transition_key, command).await
    }

    async fn schedule_activation_timer(
        &self,
        transition_key: TransitionKey,
        command: ScheduleActivationTimerCommand,
    ) -> Result<TransitionOutcome<TimerId>, RepositoryError> {
        schedule_activation_timer(self, transition_key, command).await
    }

    async fn fire_timer(
        &self,
        transition_key: TransitionKey,
        command: FireTimerCommand,
    ) -> Result<TransitionOutcome<TimerFireAuthority>, RepositoryError> {
        fire_timer(self, transition_key, command).await
    }

    async fn cancel_timer(
        &self,
        run_id: &RunId,
        timer_id: &TimerId,
    ) -> Result<bool, RepositoryError> {
        let _writer = self.writer.lock().await;
        let now = now_text(Utc::now());
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let version = sqlx::query_scalar::<_, i64>(
            "UPDATE timers SET timer_state = 'cancelled', fired_at = ?,
                    projection_version = projection_version + 1
             WHERE run_id = ? AND timer_id = ? AND timer_state = 'scheduled'
             RETURNING projection_version",
        )
        .bind(now)
        .bind(run_id.as_str())
        .bind(timer_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(version) = version else {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(false);
        };
        let version_text = version.to_string();
        let transition_key = model_data(TransitionKey::derive(
            "repository.projection_mutation",
            &[
                "timer.cancel",
                run_id.as_str(),
                timer_id.as_str(),
                &version_text,
            ],
        ))?;
        let intent_hash = canonical_intent_hash(&serde_json::json!({
            "operation": "timer.cancel",
            "run_id": run_id.as_str(),
            "timer_id": timer_id.as_str(),
            "projection_version": version,
        }))?;
        let event_id = append_projection_mutation_event(
            &mut transaction,
            run_id,
            &transition_key,
            intent_hash.as_str(),
            ProjectionMutationKind::TimerCancelled,
            u64_from_i64(version)?,
        )
        .await?;
        finalize_projection_checkpoints(&mut transaction, run_id, &event_id).await?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(true)
    }

    async fn receive_signal(
        &self,
        command: ReceiveSignalCommand,
    ) -> Result<TransitionOutcome<SignalReceipt>, RepositoryError> {
        receive_signal(self, command).await
    }

    async fn resolve_wait_signal(
        &self,
        transition_key: TransitionKey,
        command: ResolveSignalCommand,
    ) -> Result<TransitionOutcome<WaitResolutionAuthority>, RepositoryError> {
        resolve_wait_signal(self, transition_key, command).await
    }

    async fn claim_task_outbox(
        &self,
        claimant: &str,
        claim_seconds: u32,
        limit: u32,
    ) -> Result<Vec<TaskClaim>, RepositoryError> {
        claim_task_outbox(self, claimant, claim_seconds, limit).await
    }

    async fn mark_task_published(&self, claim: &TaskClaim) -> Result<bool, RepositoryError> {
        mutate_task_claim(self, claim, true).await
    }

    async fn ack_task(&self, claim: &TaskClaim) -> Result<bool, RepositoryError> {
        mutate_task_claim(self, claim, false).await
    }

    async fn load_activation(
        &self,
        run_id: &RunId,
        activation_id: &insight_engine::ActivationId,
    ) -> Result<Option<ActivationProjection>, RepositoryError> {
        load_activation(&self.pool, run_id, activation_id).await
    }
}

async fn grant_attempt_lease(
    repository: &SqliteDurableRepository,
    transition_key: TransitionKey,
    command: GrantAttemptLeaseCommand,
) -> Result<TransitionOutcome<LeaseGrantAuthority>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    match load_replay(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(replay) => {
            let row = sqlx::query(
                "SELECT t.attempt_no, t.lease_epoch, t.fencing_token, t.lease_expires_at,
                        m.timer_id, o.task_id, a.projection_version, a.scope_instance_id,
                        a.node_id, e.schema_version AS execution_event_schema_version,
                        e.safe_payload
                 FROM execution_events e
                 JOIN task_outbox o ON o.run_id = e.run_id
                    AND o.created_by_transition_key = e.transition_key
                 JOIN node_attempts t ON t.run_id = o.run_id
                    AND t.activation_id = o.activation_id AND t.attempt_no = o.attempt_no
                    AND t.lease_epoch = o.lease_epoch
                 JOIN node_activations a ON a.run_id = t.run_id
                    AND a.activation_id = t.activation_id
                 JOIN timers m ON m.run_id = t.run_id AND m.activation_id = t.activation_id
                    AND m.timer_kind = 'lease' AND m.expected_attempt_no = t.attempt_no
                    AND m.expected_lease_epoch = t.lease_epoch
                 WHERE e.run_id = ? AND e.transition_key = ?",
            )
            .bind(command.run_id().as_str())
            .bind(transition_key.as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            decode_execution_event_schema_row(&row)?;
            let attempt = model_data(AttemptNo::new(
                u32::try_from(
                    row.try_get::<i64, _>("attempt_no")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let epoch = model_data(LeaseEpoch::new(u64_from_i64(
                row.try_get("lease_epoch")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?))?;
            let fence = model_data(LeaseFence::new(attempt, epoch))?;
            let expected_primary = ExecutionEventPayload::ActivationLeased {
                attempt_no: attempt,
                lease_epoch: epoch,
            };
            let stored_primary = serde_json::from_str::<ExecutionEventPayload>(
                &row.try_get::<String, _>("safe_payload")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?;
            if stored_primary != expected_primary {
                return Err(RepositoryError::invalid_data());
            }
            let scope_id = model_data(insight_engine::ScopeInstanceId::new(
                row.try_get::<String, _>("scope_instance_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let node_id = model_data(insight_engine::NodeId::new(
                row.try_get::<String, _>("node_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let cause = model_data(ExecutionEventId::parse(replay.event_id().to_owned()))?;
            let attempt_event = model_data(PendingExecutionEvent::new(
                ExecutionEventContext::for_run(command.run_id().clone())
                    .for_attempt(
                        scope_id.clone(),
                        node_id.clone(),
                        command.activation_id().clone(),
                        attempt,
                    )
                    .caused_by(cause.clone()),
                ExecutionEventPayload::AttemptLeased { lease_epoch: epoch },
            ))?;
            verify_companion_event(
                &mut transaction,
                command.run_id(),
                &transition_key,
                replay.event_id(),
                "attempt_leased",
                &attempt_event,
            )
            .await?;
            let lease_timer_id = model_data(TimerId::new(
                row.try_get::<String, _>("timer_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let lease_deadline = parse_time(
                &row.try_get::<String, _>("lease_expires_at")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let timer_event = model_data(PendingExecutionEvent::new(
                ExecutionEventContext::for_run(command.run_id().clone())
                    .for_activation(scope_id, node_id, command.activation_id().clone())
                    .caused_by(cause),
                ExecutionEventPayload::TimerScheduled {
                    timer_id: lease_timer_id.clone(),
                    fire_at: lease_deadline,
                },
            ))?;
            verify_companion_event(
                &mut transaction,
                command.run_id(),
                &transition_key,
                replay.event_id(),
                "lease_timer_scheduled",
                &timer_event,
            )
            .await?;
            let authority = activation_adapter::lease_grant_authority(
                make_receipt(
                    &replay,
                    u64_from_i64(
                        row.try_get("projection_version")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )?,
                    None,
                ),
                command.run_id().clone(),
                command.activation_id().clone(),
                fence,
                row.try_get("fencing_token")
                    .map_err(|_| RepositoryError::invalid_data())?,
                lease_timer_id,
                lease_deadline,
                row.try_get("task_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            );
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: authority,
            });
        }
        Replay::Vacant => {}
    }

    let row = sqlx::query(
        "SELECT scope_instance_id, node_id, effect_id, lifecycle, execution_kind,
                last_attempt_no, last_lease_epoch, retry_budget_remaining, projection_version
         FROM node_activations WHERE run_id = ? AND activation_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::activation_not_found)?;
    let lifecycle = row
        .try_get::<String, _>("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?;
    let kind = row
        .try_get::<String, _>("execution_kind")
        .map_err(|_| RepositoryError::invalid_data())?;
    let version = row
        .try_get::<i64, _>("projection_version")
        .map_err(|_| RepositoryError::invalid_data())?;
    let retry_budget = row
        .try_get::<i64, _>("retry_budget_remaining")
        .map_err(|_| RepositoryError::invalid_data())?;
    if lifecycle != "ready"
        || kind != "worker"
        || version != i64_from_u64(command.expected_projection_version())?
        || retry_budget <= 0
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let last_attempt = row
        .try_get::<Option<i64>, _>("last_attempt_no")
        .map_err(|_| RepositoryError::invalid_data())?
        .unwrap_or(0);
    let last_epoch = row
        .try_get::<Option<i64>, _>("last_lease_epoch")
        .map_err(|_| RepositoryError::invalid_data())?
        .unwrap_or(0);
    let attempt_value = last_attempt
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let epoch_value = last_epoch
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?
        .max(attempt_value);
    let attempt = model_data(AttemptNo::new(
        u32::try_from(attempt_value).map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let epoch = model_data(LeaseEpoch::new(u64_from_i64(epoch_value)?))?;
    let fence = model_data(LeaseFence::new(attempt, epoch))?;
    let token = fencing_token(&transition_key);
    let timer = model_data(TimerId::new(timer_id(&transition_key, "lease")))?;
    let task = task_id(&transition_key);
    let now = Utc::now();
    let deadline = now
        .checked_add_signed(Duration::seconds(i64::from(command.lease_seconds())))
        .ok_or_else(RepositoryError::invalid_data)?;
    let now_encoded = now_text(now);
    let deadline_encoded = now_text(deadline);
    let effect_id = row
        .try_get::<String, _>("effect_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    sqlx::query(
        "INSERT INTO node_attempts (
            run_id, activation_id, attempt_no, lease_epoch, fencing_token, effect_id,
            lifecycle, effect_evidence, worker_id, lease_expires_at, heartbeat_at,
            output_payload_id, output_artifact_id, output_value_hash, failure_code,
            completion_transition_key, terminal_event_id, projection_version,
            created_at, started_at, terminal_at
         ) VALUES (?, ?, ?, ?, ?, ?, 'leased', 'not_started', ?, ?, ?, NULL, NULL,
                   NULL, NULL, NULL, NULL, 0, ?, NULL, NULL)",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(attempt_value)
    .bind(epoch_value)
    .bind(&token)
    .bind(&effect_id)
    .bind(command.worker_id())
    .bind(&deadline_encoded)
    .bind(&now_encoded)
    .bind(&now_encoded)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let next_version = command
        .expected_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let rows = sqlx::query(
        "UPDATE node_activations
         SET lifecycle = 'leased', last_attempt_no = ?, last_lease_epoch = ?,
             current_attempt_no = ?, current_lease_epoch = ?, current_fencing_token = ?,
             retry_budget_remaining = retry_budget_remaining - 1,
             projection_version = ?, updated_at = ?
         WHERE run_id = ? AND activation_id = ? AND lifecycle = 'ready'
           AND projection_version = ? AND retry_budget_remaining > 0",
    )
    .bind(attempt_value)
    .bind(epoch_value)
    .bind(attempt_value)
    .bind(epoch_value)
    .bind(&token)
    .bind(i64_from_u64(next_version)?)
    .bind(&now_encoded)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(i64_from_u64(command.expected_projection_version())?)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows == 0 {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    sqlx::query(
        "INSERT INTO timers (
            run_id, timer_id, activation_id, timer_kind, timer_state, deadline_at,
            expected_attempt_no, expected_lease_epoch, expected_fencing_token,
            retry_budget_snapshot, created_by_transition_key, fired_by_transition_key,
            fired_event_id, projection_version, created_at, fired_at
         ) VALUES (?, ?, ?, 'lease', 'scheduled', ?, ?, ?, ?, ?, ?, NULL, NULL, 0, ?, NULL)",
    )
    .bind(command.run_id().as_str())
    .bind(timer.as_str())
    .bind(command.activation_id().as_str())
    .bind(&deadline_encoded)
    .bind(attempt_value)
    .bind(epoch_value)
    .bind(&token)
    .bind(retry_budget - 1)
    .bind(transition_key.as_str())
    .bind(&now_encoded)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let node_id = model_data(insight_engine::NodeId::new(
        row.try_get::<String, _>("node_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let scope_id = row
        .try_get::<String, _>("scope_instance_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let task_envelope = activation_adapter::task_envelope(
        command.run_id().clone(),
        command.activation_id().clone(),
        node_id.clone(),
        model_data(insight_engine::EffectId::new(effect_id))?,
        fence,
        token.clone(),
        command.task(),
    );
    let envelope = canonical_json(
        &serde_json::to_value(&task_envelope).map_err(|_| RepositoryError::canonicalization())?,
    )?;
    sqlx::query(
        "INSERT INTO task_outbox (
            run_id, task_id, activation_id, attempt_no, lease_epoch, fencing_token,
            effect_id, created_by_transition_key, task_state, task_envelope, available_at,
            claimed_by, claim_token, claim_expires_at, publish_attempts, published_at,
            acked_at, last_error_code, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, NULL, NULL, NULL, 0,
                   NULL, NULL, NULL, ?)",
    )
    .bind(command.run_id().as_str())
    .bind(&task)
    .bind(command.activation_id().as_str())
    .bind(attempt_value)
    .bind(epoch_value)
    .bind(&token)
    .bind(task_envelope.effect_id().as_str())
    .bind(transition_key.as_str())
    .bind(envelope)
    .bind(&now_encoded)
    .bind(&now_encoded)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let scope_identity = model_data(insight_engine::ScopeInstanceId::new(&scope_id))?;
    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
            scope_identity.clone(),
            node_id.clone(),
            command.activation_id().clone(),
        ),
        ExecutionEventPayload::ActivationLeased {
            attempt_no: attempt,
            lease_epoch: epoch,
        },
    ))?;
    let receipt = insert_closed_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        next_version,
        event,
    )
    .await?;
    let cause = model_data(ExecutionEventId::parse(receipt.event_id().to_owned()))?;
    let attempt_event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone())
            .for_attempt(
                scope_identity.clone(),
                node_id.clone(),
                command.activation_id().clone(),
                attempt,
            )
            .caused_by(cause.clone()),
        ExecutionEventPayload::AttemptLeased { lease_epoch: epoch },
    ))?;
    insert_companion_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        receipt.event_id(),
        "attempt_leased",
        next_version,
        attempt_event,
    )
    .await?;
    let timer_event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone())
            .for_activation(scope_identity, node_id, command.activation_id().clone())
            .caused_by(cause),
        ExecutionEventPayload::TimerScheduled {
            timer_id: timer.clone(),
            fire_at: deadline,
        },
    ))?;
    insert_companion_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        receipt.event_id(),
        "lease_timer_scheduled",
        next_version,
        timer_event,
    )
    .await?;
    let authority = activation_adapter::lease_grant_authority(
        receipt,
        command.run_id().clone(),
        command.activation_id().clone(),
        fence,
        token,
        timer,
        deadline,
        task,
    );
    finalize_projection_checkpoints(
        &mut transaction,
        command.run_id(),
        authority.receipt().event_id(),
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: authority })
}

async fn fenced_state_change(
    repository: &SqliteDurableRepository,
    transition_key: TransitionKey,
    command: FencedAttemptCommand,
    mark_running: bool,
    evidence: Option<EffectEvidence>,
) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError> {
    #[derive(serde::Serialize)]
    struct Intent<'a> {
        operation: &'static str,
        command: &'a FencedAttemptCommand,
        evidence: Option<EffectEvidence>,
    }
    let intent_hash = canonical_intent_hash(&Intent {
        operation: if mark_running {
            "attempt.mark_running"
        } else {
            "attempt.effect_evidence"
        },
        command: &command,
        evidence,
    })?;
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    if let Some(outcome) = replay_fenced_receipt(
        &mut transaction,
        &transition_key,
        intent_hash.as_str(),
        &command,
        mark_running,
        evidence,
    )
    .await?
    {
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(outcome);
    }
    let identity = load_attempt_identity(&mut transaction, &command).await?;
    let Some((
        scope_id,
        node_id,
        activation_lifecycle,
        attempt_lifecycle,
        current_evidence,
        activation_version,
        attempt_version,
    )) = identity
    else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StaleLease);
    };
    if activation_version != i64_from_u64(command.expected_activation_projection_version())?
        || attempt_version != i64_from_u64(command.expected_attempt_projection_version())?
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    if (mark_running && (activation_lifecycle != "leased" || attempt_lifecycle != "leased"))
        || (!mark_running
            && (!matches!(activation_lifecycle.as_str(), "leased" | "running")
                || !matches!(attempt_lifecycle.as_str(), "leased" | "running")))
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let next_activation_version = command
        .expected_activation_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let next_attempt_version = command
        .expected_attempt_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let (activation_lifecycle_next, attempt_lifecycle_next, evidence_next, payload) =
        if mark_running {
            (
                "running",
                "running",
                current_evidence,
                ExecutionEventPayload::AttemptRunning {
                    lease_epoch: command.fence().lease_epoch(),
                },
            )
        } else {
            let requested = evidence.ok_or_else(RepositoryError::invalid_data)?;
            let current = parse_effect_evidence(&current_evidence)?;
            if !current.can_transition_to(requested) {
                transaction
                    .rollback()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
            (
                activation_lifecycle.as_str(),
                attempt_lifecycle.as_str(),
                effect_evidence_str(requested).to_owned(),
                ExecutionEventPayload::EffectEvidenceRecorded {
                    evidence: requested,
                },
            )
        };
    let attempt_rows = sqlx::query(
        "UPDATE node_attempts SET lifecycle = ?, effect_evidence = ?, projection_version = ?,
                started_at = CASE WHEN ? = 'running' THEN CURRENT_TIMESTAMP ELSE started_at END
         WHERE run_id = ? AND activation_id = ? AND attempt_no = ? AND lease_epoch = ?
           AND fencing_token = ? AND worker_id = ? AND projection_version = ?
           AND lifecycle = ?",
    )
    .bind(attempt_lifecycle_next)
    .bind(&evidence_next)
    .bind(i64_from_u64(next_attempt_version)?)
    .bind(attempt_lifecycle_next)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(i64::from(command.fence().attempt_no().get()))
    .bind(i64_from_u64(command.fence().lease_epoch().get())?)
    .bind(command.fencing_token())
    .bind(command.worker_id())
    .bind(i64_from_u64(command.expected_attempt_projection_version())?)
    .bind(&attempt_lifecycle)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    let activation_rows = sqlx::query(
        "UPDATE node_activations SET lifecycle = ?, effect_evidence = ?, projection_version = ?,
                updated_at = CURRENT_TIMESTAMP
         WHERE run_id = ? AND activation_id = ? AND current_attempt_no = ?
           AND current_lease_epoch = ? AND current_fencing_token = ?
           AND projection_version = ? AND lifecycle = ?",
    )
    .bind(activation_lifecycle_next)
    .bind(&evidence_next)
    .bind(i64_from_u64(next_activation_version)?)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(i64::from(command.fence().attempt_no().get()))
    .bind(i64_from_u64(command.fence().lease_epoch().get())?)
    .bind(command.fencing_token())
    .bind(i64_from_u64(
        command.expected_activation_projection_version(),
    )?)
    .bind(&activation_lifecycle)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if attempt_rows != 1 || activation_rows != 1 {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let scope_identity = model_data(insight_engine::ScopeInstanceId::new(&scope_id))?;
    let node_identity = model_data(insight_engine::NodeId::new(&node_id))?;
    let event = if mark_running {
        model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
                scope_identity.clone(),
                node_identity.clone(),
                command.activation_id().clone(),
            ),
            ExecutionEventPayload::ActivationRunning {
                attempt_no: command.fence().attempt_no(),
            },
        ))?
    } else {
        model_data(PendingExecutionEvent::new(
            attempt_context(
                command.run_id(),
                &scope_id,
                &node_id,
                command.activation_id(),
                command.fence().attempt_no(),
            )?,
            payload,
        ))?
    };
    let receipt = insert_closed_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        next_activation_version,
        event,
    )
    .await?;
    if mark_running {
        let companion = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone())
                .for_attempt(
                    scope_identity,
                    node_identity,
                    command.activation_id().clone(),
                    command.fence().attempt_no(),
                )
                .caused_by(model_data(ExecutionEventId::parse(
                    receipt.event_id().to_owned(),
                ))?),
            ExecutionEventPayload::AttemptRunning {
                lease_epoch: command.fence().lease_epoch(),
            },
        ))?;
        insert_companion_event(
            &mut transaction,
            command.run_id(),
            &transition_key,
            receipt.event_id(),
            "attempt_running",
            next_activation_version,
            companion,
        )
        .await?;
    }
    finalize_projection_checkpoints(&mut transaction, command.run_id(), receipt.event_id()).await?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: receipt })
}

async fn replay_fenced_receipt(
    transaction: &mut Transaction<'_, Sqlite>,
    transition_key: &TransitionKey,
    intent_hash: &str,
    command: &FencedAttemptCommand,
    mark_running: bool,
    evidence: Option<EffectEvidence>,
) -> Result<Option<TransitionOutcome<ActivationCommitReceipt>>, RepositoryError> {
    let replay =
        match load_replay(transaction, command.run_id(), transition_key, intent_hash).await? {
            Replay::Vacant => return Ok(None),
            Replay::Exact(replay) => replay,
        };
    let row = sqlx::query(
        "SELECT a.scope_instance_id,a.node_id,a.projection_version,
                e.schema_version AS execution_event_schema_version,e.safe_payload
         FROM node_activations a JOIN execution_events e ON e.run_id=a.run_id
            AND e.transition_key=?
         WHERE a.run_id=? AND a.activation_id=?",
    )
    .bind(transition_key.as_str())
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    decode_execution_event_schema_row(&row)?;
    let expected = if mark_running {
        ExecutionEventPayload::ActivationRunning {
            attempt_no: command.fence().attempt_no(),
        }
    } else {
        ExecutionEventPayload::EffectEvidenceRecorded {
            evidence: evidence.ok_or_else(RepositoryError::invalid_data)?,
        }
    };
    let stored = serde_json::from_str::<ExecutionEventPayload>(
        &row.try_get::<String, _>("safe_payload")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if stored != expected {
        return Err(RepositoryError::invalid_data());
    }
    if mark_running {
        let companion = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone())
                .for_attempt(
                    model_data(insight_engine::ScopeInstanceId::new(
                        row.try_get::<String, _>("scope_instance_id")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    ))?,
                    model_data(insight_engine::NodeId::new(
                        row.try_get::<String, _>("node_id")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    ))?,
                    command.activation_id().clone(),
                    command.fence().attempt_no(),
                )
                .caused_by(model_data(ExecutionEventId::parse(
                    replay.event_id().to_owned(),
                ))?),
            ExecutionEventPayload::AttemptRunning {
                lease_epoch: command.fence().lease_epoch(),
            },
        ))?;
        verify_companion_event(
            transaction,
            command.run_id(),
            transition_key,
            replay.event_id(),
            "attempt_running",
            &companion,
        )
        .await?;
    }
    Ok(Some(TransitionOutcome::ExactReplay {
        authoritative: make_receipt(
            &replay,
            u64_from_i64(
                row.try_get("projection_version")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
            None,
        ),
    }))
}

async fn heartbeat_attempt(
    repository: &SqliteDurableRepository,
    command: HeartbeatAttemptCommand,
) -> Result<TransitionOutcome<()>, RepositoryError> {
    let authority = command.authority();
    let _writer = repository.writer.lock().await;
    let now = Utc::now();
    let now_encoded = now_text(now);
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let transition_key = activation_adapter::heartbeat_transition_key(&command)?;
    let intent_hash = canonical_intent_hash(&command)?;
    match load_replay(
        &mut transaction,
        authority.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(_) => {
            let replay_row = sqlx::query(
                "SELECT m.timer_id, m.deadline_at,
                        e.schema_version AS execution_event_schema_version,e.safe_payload
                 FROM timers m JOIN execution_events e ON e.run_id = m.run_id
                   AND e.transition_key = ?
                 WHERE m.run_id = ? AND m.activation_id = ? AND m.timer_kind = 'lease'
                   AND m.expected_attempt_no = ? AND m.expected_lease_epoch = ?
                   AND m.expected_fencing_token = ?",
            )
            .bind(transition_key.as_str())
            .bind(authority.run_id().as_str())
            .bind(authority.activation_id().as_str())
            .bind(i64::from(authority.fence().attempt_no().get()))
            .bind(i64_from_u64(authority.fence().lease_epoch().get())?)
            .bind(authority.fencing_token())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(RepositoryError::invalid_data)?;
            decode_execution_event_schema_row(&replay_row)?;
            let timer_id = model_data(TimerId::new(
                replay_row
                    .try_get::<String, _>("timer_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let deadline = parse_time(
                &replay_row
                    .try_get::<String, _>("deadline_at")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let stored = serde_json::from_str::<ExecutionEventPayload>(
                &replay_row
                    .try_get::<String, _>("safe_payload")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?;
            if !matches!(
                stored,
                ExecutionEventPayload::TimerScheduled {
                    timer_id: stored_timer,
                    fire_at,
                } if stored_timer == timer_id && fire_at <= deadline
            ) {
                return Err(RepositoryError::invalid_data());
            }
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative: () });
        }
        Replay::Vacant => {}
    }
    let row = sqlx::query(
        "SELECT t.lease_expires_at, t.projection_version AS attempt_version,
                a.projection_version AS activation_version,
                a.scope_instance_id, a.node_id, m.timer_id,
                m.deadline_at AS timer_deadline, m.projection_version AS timer_version
         FROM node_attempts t JOIN node_activations a
           ON a.run_id = t.run_id AND a.activation_id = t.activation_id
         JOIN timers m ON m.run_id = t.run_id AND m.activation_id = t.activation_id
           AND m.timer_kind = 'lease' AND m.timer_state = 'scheduled'
           AND m.expected_attempt_no = t.attempt_no
           AND m.expected_lease_epoch = t.lease_epoch
           AND m.expected_fencing_token = t.fencing_token
         WHERE t.run_id = ? AND t.activation_id = ? AND t.attempt_no = ?
           AND t.lease_epoch = ? AND t.fencing_token = ? AND t.worker_id = ?
           AND t.lifecycle IN ('leased', 'running')
           AND a.lifecycle IN ('leased', 'running')
           AND a.current_attempt_no = t.attempt_no
           AND a.current_lease_epoch = t.lease_epoch
           AND a.current_fencing_token = t.fencing_token",
    )
    .bind(authority.run_id().as_str())
    .bind(authority.activation_id().as_str())
    .bind(i64::from(authority.fence().attempt_no().get()))
    .bind(i64_from_u64(authority.fence().lease_epoch().get())?)
    .bind(authority.fencing_token())
    .bind(authority.worker_id())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StaleLease);
    };
    if row
        .try_get::<i64, _>("attempt_version")
        .map_err(|_| RepositoryError::invalid_data())?
        != i64_from_u64(authority.expected_attempt_projection_version())?
        || row
            .try_get::<i64, _>("activation_version")
            .map_err(|_| RepositoryError::invalid_data())?
            != i64_from_u64(authority.expected_activation_projection_version())?
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let deadline = parse_time(
        &row.try_get::<String, _>("lease_expires_at")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let timer_deadline = parse_time(
        &row.try_get::<String, _>("timer_deadline")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    if now >= deadline || now >= timer_deadline || deadline != timer_deadline {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StaleLease);
    }
    let candidate = now
        .checked_add_signed(Duration::seconds(i64::from(command.extend_seconds())))
        .ok_or_else(RepositoryError::invalid_data)?;
    let extended_deadline = deadline.max(candidate);
    let extended_deadline_encoded = now_text(extended_deadline);
    let next_attempt_version = authority
        .expected_attempt_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let attempt_rows = sqlx::query(
        "UPDATE node_attempts SET heartbeat_at = ?, lease_expires_at = ?,
                projection_version = ?
         WHERE run_id = ? AND activation_id = ? AND attempt_no = ? AND lease_epoch = ?
           AND fencing_token = ? AND worker_id = ? AND lifecycle IN ('leased', 'running')
           AND projection_version = ? AND lease_expires_at = ?",
    )
    .bind(&now_encoded)
    .bind(&extended_deadline_encoded)
    .bind(i64_from_u64(next_attempt_version)?)
    .bind(authority.run_id().as_str())
    .bind(authority.activation_id().as_str())
    .bind(i64::from(authority.fence().attempt_no().get()))
    .bind(i64_from_u64(authority.fence().lease_epoch().get())?)
    .bind(authority.fencing_token())
    .bind(authority.worker_id())
    .bind(i64_from_u64(
        authority.expected_attempt_projection_version(),
    )?)
    .bind(now_text(deadline))
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    let timer_version = row
        .try_get::<i64, _>("timer_version")
        .map_err(|_| RepositoryError::invalid_data())?;
    let timer_rows = sqlx::query(
        "UPDATE timers SET deadline_at = ?, projection_version = projection_version + 1
         WHERE run_id = ? AND activation_id = ? AND timer_kind = 'lease'
           AND expected_attempt_no = ? AND expected_lease_epoch = ?
           AND expected_fencing_token = ? AND timer_state = 'scheduled'
           AND projection_version = ? AND deadline_at = ?",
    )
    .bind(&extended_deadline_encoded)
    .bind(authority.run_id().as_str())
    .bind(authority.activation_id().as_str())
    .bind(i64::from(authority.fence().attempt_no().get()))
    .bind(i64_from_u64(authority.fence().lease_epoch().get())?)
    .bind(authority.fencing_token())
    .bind(timer_version)
    .bind(now_text(timer_deadline))
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if attempt_rows != 1 || timer_rows != 1 {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StaleLease);
    }
    let heartbeat_event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(authority.run_id().clone()).for_activation(
            model_data(insight_engine::ScopeInstanceId::new(
                row.try_get::<String, _>("scope_instance_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            model_data(insight_engine::NodeId::new(
                row.try_get::<String, _>("node_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            authority.activation_id().clone(),
        ),
        ExecutionEventPayload::TimerScheduled {
            timer_id: model_data(TimerId::new(
                row.try_get::<String, _>("timer_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            fire_at: extended_deadline,
        },
    ))?;
    let receipt = insert_closed_event(
        &mut transaction,
        authority.run_id(),
        &transition_key,
        intent_hash.as_str(),
        authority.expected_activation_projection_version(),
        heartbeat_event,
    )
    .await?;
    finalize_projection_checkpoints(&mut transaction, authority.run_id(), receipt.event_id())
        .await?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: () })
}

async fn complete_attempt(
    repository: &SqliteDurableRepository,
    transition_key: TransitionKey,
    command: CompleteAttemptCommand,
) -> Result<TransitionOutcome<AttemptCompletionAuthority>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let authority = command.authority();
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    match load_replay(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(replay) => {
            let row = sqlx::query(
                "SELECT t.lifecycle AS attempt_lifecycle, t.output_payload_id,
                        t.output_artifact_id, a.lifecycle AS activation_lifecycle,
                        a.projection_version, a.scope_instance_id, a.node_id,
                        a.pending_retry_timer_id,
                        e.schema_version AS execution_event_schema_version,e.safe_payload
                 FROM node_attempts t
                 JOIN node_activations a ON a.run_id = t.run_id
                    AND a.activation_id = t.activation_id
                 JOIN execution_events e ON e.run_id = t.run_id
                    AND e.transition_key = t.completion_transition_key
                 WHERE t.run_id = ? AND t.activation_id = ? AND t.completion_transition_key = ?",
            )
            .bind(command.run_id().as_str())
            .bind(command.activation_id().as_str())
            .bind(transition_key.as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            decode_execution_event_schema_row(&row)?;
            let receipt = make_receipt(
                &replay,
                u64_from_i64(
                    row.try_get("projection_version")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                None,
            );
            let activation_lifecycle = parse_activation_lifecycle(
                &row.try_get::<String, _>("activation_lifecycle")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let scope_id = model_data(insight_engine::ScopeInstanceId::new(
                row.try_get::<String, _>("scope_instance_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let node_id = model_data(insight_engine::NodeId::new(
                row.try_get::<String, _>("node_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let expected_primary_payload = match command.completion() {
                AttemptCompletion::Succeeded { output } => {
                    ExecutionEventPayload::ActivationSucceeded {
                        attempt_no: Some(authority.fence().attempt_no()),
                        output: Some(value_summary(output)),
                    }
                }
                AttemptCompletion::Failed {
                    reason, failure, ..
                } if activation_lifecycle == ActivationLifecycle::RetryWait => {
                    let _ = (reason, failure);
                    ExecutionEventPayload::ActivationRetryWait {
                        attempt_no: authority.fence().attempt_no(),
                    }
                }
                AttemptCompletion::Failed {
                    reason, failure, ..
                } => ExecutionEventPayload::ActivationFailed {
                    attempt_no: Some(authority.fence().attempt_no()),
                    reason: *reason,
                    failure: failure.clone(),
                },
            };
            let stored_primary_payload = serde_json::from_str::<ExecutionEventPayload>(
                &row.try_get::<String, _>("safe_payload")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?;
            if stored_primary_payload != expected_primary_payload {
                return Err(RepositoryError::invalid_data());
            }
            let expected_attempt_payload = match command.completion() {
                AttemptCompletion::Succeeded { output } => {
                    ExecutionEventPayload::AttemptSucceeded {
                        output: Some(value_summary(output)),
                    }
                }
                AttemptCompletion::Failed { failure, .. } => ExecutionEventPayload::AttemptFailed {
                    failure: failure.clone(),
                },
            };
            let expected_companion = model_data(PendingExecutionEvent::new(
                ExecutionEventContext::for_run(command.run_id().clone())
                    .for_attempt(
                        scope_id.clone(),
                        node_id,
                        command.activation_id().clone(),
                        authority.fence().attempt_no(),
                    )
                    .caused_by(model_data(ExecutionEventId::parse(
                        replay.event_id().to_owned(),
                    ))?),
                expected_attempt_payload,
            ))?;
            verify_companion_event(
                &mut transaction,
                command.run_id(),
                &transition_key,
                replay.event_id(),
                "attempt_terminal",
                &expected_companion,
            )
            .await?;
            let result = if activation_lifecycle == ActivationLifecycle::Succeeded {
                let payload_id = row
                    .try_get::<Option<String>, _>("output_payload_id")
                    .map_err(|_| RepositoryError::invalid_data())?;
                let artifact_id = row
                    .try_get::<Option<String>, _>("output_artifact_id")
                    .map_err(|_| RepositoryError::invalid_data())?;
                let output = stored_value_ref(
                    &mut transaction,
                    command.run_id(),
                    payload_id.as_deref(),
                    artifact_id.as_deref(),
                )
                .await?;
                let terminal = activation_adapter::committed_terminal_activation_authority(
                    receipt,
                    command.run_id().clone(),
                    scope_id,
                    command.activation_id().clone(),
                    super::CommittedTerminalActivationResult::Succeeded {
                        content_hash: output.content_hash().clone(),
                        output: output.clone(),
                    },
                )?;
                AttemptCompletionAuthority::Succeeded {
                    terminal,
                    fence: authority.fence(),
                    output,
                }
            } else if activation_lifecycle == ActivationLifecycle::RetryWait {
                let timer_id = row
                    .try_get::<Option<String>, _>("pending_retry_timer_id")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .ok_or_else(RepositoryError::invalid_data)?;
                let timer = sqlx::query(
                    "SELECT deadline_at, retry_budget_snapshot FROM timers
                     WHERE run_id = ? AND timer_id = ? AND timer_kind = 'retry'",
                )
                .bind(command.run_id().as_str())
                .bind(&timer_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
                let retry = activation_adapter::retry_schedule_authority(
                    command.run_id().clone(),
                    command.activation_id().clone(),
                    authority.fence(),
                    model_data(TimerId::new(timer_id))?,
                    parse_time(
                        &timer
                            .try_get::<String, _>("deadline_at")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )?,
                    u32::try_from(
                        timer
                            .try_get::<i64, _>("retry_budget_snapshot")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )
                    .map_err(|_| RepositoryError::invalid_data())?,
                );
                AttemptCompletionAuthority::RetryScheduled { receipt, retry }
            } else {
                let (reason, failure) = match command.completion() {
                    AttemptCompletion::Failed {
                        reason, failure, ..
                    } => (*reason, failure.clone()),
                    AttemptCompletion::Succeeded { .. } => {
                        return Err(RepositoryError::invalid_data());
                    }
                };
                let terminal = activation_adapter::committed_terminal_activation_authority(
                    receipt,
                    command.run_id().clone(),
                    scope_id,
                    command.activation_id().clone(),
                    super::CommittedTerminalActivationResult::Failed { reason, failure },
                )?;
                AttemptCompletionAuthority::Failed { terminal }
            };
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: result,
            });
        }
        Replay::Vacant => {}
    }

    let identity = load_attempt_identity(&mut transaction, authority).await?;
    let Some((
        scope_id,
        node_id,
        activation_lifecycle,
        attempt_lifecycle,
        evidence,
        activation_version,
        attempt_version,
    )) = identity
    else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StaleLease);
    };
    if activation_version != i64_from_u64(authority.expected_activation_projection_version())?
        || attempt_version != i64_from_u64(authority.expected_attempt_projection_version())?
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let lifecycle_is_valid = match command.completion() {
        AttemptCompletion::Succeeded { .. } => {
            activation_lifecycle == "running" && attempt_lifecycle == "running"
        }
        AttemptCompletion::Failed { .. } => {
            matches!(activation_lifecycle.as_str(), "leased" | "running")
                && matches!(attempt_lifecycle.as_str(), "leased" | "running")
        }
    };
    if !lifecycle_is_valid {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let activation_row = sqlx::query(
        "SELECT effect_idempotency, retry_budget_remaining FROM node_activations
         WHERE run_id = ? AND activation_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .fetch_one(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let idempotency = activation_row
        .try_get::<String, _>("effect_idempotency")
        .map_err(|_| RepositoryError::invalid_data())?;
    let remaining_budget = activation_row
        .try_get::<i64, _>("retry_budget_remaining")
        .map_err(|_| RepositoryError::invalid_data())?;
    let next_activation_version = authority
        .expected_activation_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let next_attempt_version = authority
        .expected_attempt_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let attempt_event_key = companion_transition_key(&transition_key, "attempt_terminal")?;
    let attempt_event_id = event_id(&attempt_event_key);
    let now = Utc::now();
    let now_encoded = now_text(now);

    let (
        attempt_lifecycle_next,
        activation_lifecycle_next,
        failure_code,
        output_storage,
        retry_authority,
        attempt_payload,
    ) = match command.completion() {
        AttemptCompletion::Succeeded { output } => {
            let stored = persist_value_ref(&mut transaction, command.run_id(), output).await?;
            (
                "succeeded",
                "succeeded",
                None,
                Some(stored),
                None,
                ExecutionEventPayload::AttemptSucceeded {
                    output: Some(value_summary(output)),
                },
            )
        }
        AttemptCompletion::Failed {
            reason: _,
            failure,
            retry_at,
        } => {
            let evidence = parse_effect_evidence(&evidence)?;
            let retry_safe = idempotency == "idempotent" || evidence == EffectEvidence::NotStarted;
            let retry = if let Some(retry_at) = retry_at {
                if !retry_safe || remaining_budget <= 0 || retry_at <= &now {
                    transaction
                        .rollback()
                        .await
                        .map_err(RepositoryError::storage)?;
                    return Ok(TransitionOutcome::StateConflict);
                }
                let retry_timer = model_data(TimerId::new(timer_id(&transition_key, "retry")))?;
                sqlx::query(
                    "INSERT INTO timers (
                        run_id, timer_id, activation_id, timer_kind, timer_state, deadline_at,
                        expected_attempt_no, expected_lease_epoch, expected_fencing_token,
                        retry_budget_snapshot, created_by_transition_key,
                        fired_by_transition_key, fired_event_id, projection_version,
                        created_at, fired_at
                     ) VALUES (?, ?, ?, 'retry', 'scheduled', ?, ?, ?, ?, ?, ?, NULL, NULL,
                               0, ?, NULL)",
                )
                .bind(command.run_id().as_str())
                .bind(retry_timer.as_str())
                .bind(command.activation_id().as_str())
                .bind(now_text(*retry_at))
                .bind(i64::from(authority.fence().attempt_no().get()))
                .bind(i64_from_u64(authority.fence().lease_epoch().get())?)
                .bind(authority.fencing_token())
                .bind(remaining_budget)
                .bind(transition_key.as_str())
                .bind(&now_encoded)
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
                Some(activation_adapter::retry_schedule_authority(
                    command.run_id().clone(),
                    command.activation_id().clone(),
                    authority.fence(),
                    retry_timer,
                    *retry_at,
                    u32::try_from(remaining_budget).map_err(|_| RepositoryError::invalid_data())?,
                ))
            } else {
                None
            };
            (
                "failed",
                if retry.is_some() {
                    "retry_wait"
                } else {
                    "failed"
                },
                failure
                    .as_ref()
                    .map(|value| value.code().as_str().to_owned()),
                None,
                retry,
                ExecutionEventPayload::AttemptFailed {
                    failure: failure.clone(),
                },
            )
        }
    };
    let (output_payload_id, output_artifact_id, output_hash) = output_storage
        .as_ref()
        .map(|(payload, artifact, hash)| {
            (payload.as_deref(), artifact.as_deref(), Some(hash.as_str()))
        })
        .unwrap_or((None, None, None));
    let attempt_rows = sqlx::query(
        "UPDATE node_attempts
         SET lifecycle = ?, output_payload_id = ?, output_artifact_id = ?,
             output_value_hash = ?, failure_code = ?, completion_transition_key = ?,
             terminal_event_id = ?, projection_version = ?, terminal_at = ?
         WHERE run_id = ? AND activation_id = ? AND attempt_no = ? AND lease_epoch = ?
           AND fencing_token = ? AND worker_id = ? AND lifecycle = ? AND projection_version = ?",
    )
    .bind(attempt_lifecycle_next)
    .bind(output_payload_id)
    .bind(output_artifact_id)
    .bind(output_hash)
    .bind(failure_code.as_deref())
    .bind(transition_key.as_str())
    .bind(&attempt_event_id)
    .bind(i64_from_u64(next_attempt_version)?)
    .bind(&now_encoded)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(i64::from(authority.fence().attempt_no().get()))
    .bind(i64_from_u64(authority.fence().lease_epoch().get())?)
    .bind(authority.fencing_token())
    .bind(authority.worker_id())
    .bind(&attempt_lifecycle)
    .bind(i64_from_u64(
        authority.expected_attempt_projection_version(),
    )?)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    let retry_timer_id = retry_authority
        .as_ref()
        .map(|retry| retry.timer_id().as_str());
    let terminal = retry_authority.is_none();
    let activation_rows = sqlx::query(
        "UPDATE node_activations
         SET lifecycle = ?, current_attempt_no = NULL, current_lease_epoch = NULL,
             current_fencing_token = NULL, pending_retry_timer_id = ?,
             output_payload_id = ?, output_artifact_id = ?, output_value_hash = ?,
             winning_attempt_no = CASE WHEN ? = 'succeeded' THEN ? ELSE NULL END,
             termination_intent_reason = CASE WHEN ? THEN 'failure' ELSE NULL END,
             termination_intent_transition_key = CASE WHEN ? THEN ? ELSE NULL END,
             termination_intent_at = CASE WHEN ? THEN ? ELSE NULL END,
             projection_version = ?, updated_at = ?,
             terminal_at = CASE WHEN ? THEN ? ELSE NULL END
         WHERE run_id = ? AND activation_id = ? AND current_attempt_no = ?
           AND current_lease_epoch = ? AND current_fencing_token = ?
           AND lifecycle = ? AND projection_version = ?",
    )
    .bind(activation_lifecycle_next)
    .bind(retry_timer_id)
    .bind(output_payload_id)
    .bind(output_artifact_id)
    .bind(output_hash)
    .bind(activation_lifecycle_next)
    .bind(i64::from(authority.fence().attempt_no().get()))
    .bind(terminal && activation_lifecycle_next == "failed")
    .bind(terminal && activation_lifecycle_next == "failed")
    .bind(transition_key.as_str())
    .bind(terminal && activation_lifecycle_next == "failed")
    .bind(&now_encoded)
    .bind(i64_from_u64(next_activation_version)?)
    .bind(&now_encoded)
    .bind(terminal)
    .bind(&now_encoded)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(i64::from(authority.fence().attempt_no().get()))
    .bind(i64_from_u64(authority.fence().lease_epoch().get())?)
    .bind(authority.fencing_token())
    .bind(&activation_lifecycle)
    .bind(i64_from_u64(
        authority.expected_activation_projection_version(),
    )?)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if attempt_rows != 1 || activation_rows != 1 {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    sqlx::query(
        "UPDATE timers SET timer_state = 'cancelled', fired_at = ?,
                projection_version = projection_version + 1
         WHERE run_id = ? AND activation_id = ? AND timer_kind = 'lease'
           AND expected_attempt_no = ? AND expected_lease_epoch = ?
           AND expected_fencing_token = ? AND timer_state = 'scheduled'",
    )
    .bind(&now_encoded)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(i64::from(authority.fence().attempt_no().get()))
    .bind(i64_from_u64(authority.fence().lease_epoch().get())?)
    .bind(authority.fencing_token())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let primary_payload = match command.completion() {
        AttemptCompletion::Succeeded { output } => ExecutionEventPayload::ActivationSucceeded {
            attempt_no: Some(authority.fence().attempt_no()),
            output: Some(value_summary(output)),
        },
        AttemptCompletion::Failed {
            reason, failure, ..
        } => {
            if retry_authority.is_some() {
                ExecutionEventPayload::ActivationRetryWait {
                    attempt_no: authority.fence().attempt_no(),
                }
            } else {
                ExecutionEventPayload::ActivationFailed {
                    attempt_no: Some(authority.fence().attempt_no()),
                    reason: *reason,
                    failure: failure.clone(),
                }
            }
        }
    };
    let primary_event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
            model_data(insight_engine::ScopeInstanceId::new(&scope_id))?,
            model_data(insight_engine::NodeId::new(&node_id))?,
            command.activation_id().clone(),
        ),
        primary_payload,
    ))?;
    let receipt = insert_closed_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        next_activation_version,
        primary_event,
    )
    .await?;
    let companion_event = model_data(PendingExecutionEvent::new(
        attempt_context(
            command.run_id(),
            &scope_id,
            &node_id,
            command.activation_id(),
            authority.fence().attempt_no(),
        )?
        .caused_by(model_data(ExecutionEventId::parse(
            receipt.event_id().to_owned(),
        ))?),
        attempt_payload,
    ))?;
    let companion_receipt = insert_companion_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        receipt.event_id(),
        "attempt_terminal",
        next_activation_version,
        companion_event,
    )
    .await?;
    if companion_receipt.event_id() != attempt_event_id {
        return Err(RepositoryError::invalid_data());
    }
    let checkpoint_event_id = receipt.event_id().to_owned();
    let result = match command.completion() {
        AttemptCompletion::Succeeded { output } => {
            let terminal = activation_adapter::committed_terminal_activation_authority(
                receipt,
                command.run_id().clone(),
                model_data(insight_engine::ScopeInstanceId::new(scope_id))?,
                command.activation_id().clone(),
                super::CommittedTerminalActivationResult::Succeeded {
                    output: output.clone(),
                    content_hash: output.content_hash().clone(),
                },
            )?;
            AttemptCompletionAuthority::Succeeded {
                terminal,
                fence: authority.fence(),
                output: output.clone(),
            }
        }
        AttemptCompletion::Failed {
            reason, failure, ..
        } => {
            if let Some(retry) = retry_authority {
                AttemptCompletionAuthority::RetryScheduled { receipt, retry }
            } else {
                let terminal = activation_adapter::committed_terminal_activation_authority(
                    receipt,
                    command.run_id().clone(),
                    model_data(insight_engine::ScopeInstanceId::new(scope_id))?,
                    command.activation_id().clone(),
                    super::CommittedTerminalActivationResult::Failed {
                        reason: *reason,
                        failure: failure.clone(),
                    },
                )?;
                AttemptCompletionAuthority::Failed { terminal }
            }
        }
    };
    finalize_projection_checkpoints(&mut transaction, command.run_id(), &checkpoint_event_id)
        .await?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result })
}

async fn register_wait(
    repository: &SqliteDurableRepository,
    transition_key: TransitionKey,
    command: RegisterWaitCommand,
) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    if let Some(outcome) = replay_wait_receipt(
        &mut transaction,
        &transition_key,
        intent_hash.as_str(),
        &command,
    )
    .await?
    {
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(outcome);
    }
    let row = sqlx::query(
        "SELECT scope_instance_id, node_id, lifecycle, execution_kind, projection_version
         FROM node_activations WHERE run_id = ? AND activation_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::activation_not_found)?;
    if row
        .try_get::<String, _>("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?
        != "ready"
        || row
            .try_get::<String, _>("execution_kind")
            .map_err(|_| RepositoryError::invalid_data())?
            != "durable_wait"
        || row
            .try_get::<i64, _>("projection_version")
            .map_err(|_| RepositoryError::invalid_data())?
            != i64_from_u64(command.expected_projection_version())?
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let next_version = command
        .expected_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    sqlx::query(
        "UPDATE node_activations SET lifecycle = 'waiting',
                wait_registration_transition_key = ?, projection_version = ?,
                updated_at = CURRENT_TIMESTAMP
         WHERE run_id = ? AND activation_id = ? AND lifecycle = 'ready'
           AND execution_kind = 'durable_wait' AND projection_version = ?",
    )
    .bind(transition_key.as_str())
    .bind(i64_from_u64(next_version)?)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(i64_from_u64(command.expected_projection_version())?)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if let Some(deadline) = command.wait_deadline() {
        let id = timer_id(&transition_key, "wait");
        let now = now_text(Utc::now());
        sqlx::query(
            "INSERT INTO timers (
                run_id, timer_id, activation_id, timer_kind, timer_state, deadline_at,
                expected_attempt_no, expected_lease_epoch, expected_fencing_token,
                retry_budget_snapshot, created_by_transition_key, fired_by_transition_key,
                fired_event_id, projection_version, created_at, fired_at
             ) VALUES (?, ?, ?, 'wait', 'scheduled', ?, NULL, NULL, NULL, NULL, ?,
                       NULL, NULL, 0, ?, NULL)",
        )
        .bind(command.run_id().as_str())
        .bind(id)
        .bind(command.activation_id().as_str())
        .bind(now_text(*deadline))
        .bind(transition_key.as_str())
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
    }
    let scope_id = model_data(insight_engine::ScopeInstanceId::new(
        row.try_get::<String, _>("scope_instance_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let node_id = model_data(insight_engine::NodeId::new(
        row.try_get::<String, _>("node_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
            scope_id.clone(),
            node_id.clone(),
            command.activation_id().clone(),
        ),
        ExecutionEventPayload::ActivationWaiting,
    ))?;
    let receipt = insert_closed_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        next_version,
        event,
    )
    .await?;
    if let Some(deadline) = command.wait_deadline() {
        let id = model_data(TimerId::new(timer_id(&transition_key, "wait")))?;
        let companion = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone())
                .for_activation(scope_id, node_id, command.activation_id().clone())
                .caused_by(model_data(ExecutionEventId::parse(
                    receipt.event_id().to_owned(),
                ))?),
            ExecutionEventPayload::TimerScheduled {
                timer_id: id,
                fire_at: *deadline,
            },
        ))?;
        insert_companion_event(
            &mut transaction,
            command.run_id(),
            &transition_key,
            receipt.event_id(),
            "wait_timer_scheduled",
            next_version,
            companion,
        )
        .await?;
    }
    finalize_projection_checkpoints(&mut transaction, command.run_id(), receipt.event_id()).await?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: receipt })
}

async fn replay_wait_receipt(
    transaction: &mut Transaction<'_, Sqlite>,
    transition_key: &TransitionKey,
    intent_hash: &str,
    command: &RegisterWaitCommand,
) -> Result<Option<TransitionOutcome<ActivationCommitReceipt>>, RepositoryError> {
    let replay =
        match load_replay(transaction, command.run_id(), transition_key, intent_hash).await? {
            Replay::Vacant => return Ok(None),
            Replay::Exact(replay) => replay,
        };
    let row = sqlx::query(
        "SELECT a.scope_instance_id,a.node_id,a.projection_version,
                e.schema_version AS execution_event_schema_version,e.safe_payload
         FROM node_activations a JOIN execution_events e ON e.run_id=a.run_id
            AND e.transition_key=?
         WHERE a.run_id=? AND a.activation_id=?",
    )
    .bind(transition_key.as_str())
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    decode_execution_event_schema_row(&row)?;
    let stored = serde_json::from_str::<ExecutionEventPayload>(
        &row.try_get::<String, _>("safe_payload")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if stored != ExecutionEventPayload::ActivationWaiting {
        return Err(RepositoryError::invalid_data());
    }
    if let Some(deadline) = command.wait_deadline() {
        let timer = sqlx::query(
            "SELECT timer_id,deadline_at FROM timers
             WHERE run_id=? AND created_by_transition_key=? AND timer_kind='wait'",
        )
        .bind(command.run_id().as_str())
        .bind(transition_key.as_str())
        .fetch_one(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let id = model_data(TimerId::new(
            timer
                .try_get::<String, _>("timer_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        if parse_time(
            &timer
                .try_get::<String, _>("deadline_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )? != *deadline
        {
            return Err(RepositoryError::invalid_data());
        }
        let companion = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone())
                .for_activation(
                    model_data(insight_engine::ScopeInstanceId::new(
                        row.try_get::<String, _>("scope_instance_id")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    ))?,
                    model_data(insight_engine::NodeId::new(
                        row.try_get::<String, _>("node_id")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    ))?,
                    command.activation_id().clone(),
                )
                .caused_by(model_data(ExecutionEventId::parse(
                    replay.event_id().to_owned(),
                ))?),
            ExecutionEventPayload::TimerScheduled {
                timer_id: id,
                fire_at: *deadline,
            },
        ))?;
        verify_companion_event(
            transaction,
            command.run_id(),
            transition_key,
            replay.event_id(),
            "wait_timer_scheduled",
            &companion,
        )
        .await?;
    }
    Ok(Some(TransitionOutcome::ExactReplay {
        authoritative: make_receipt(
            &replay,
            u64_from_i64(
                row.try_get("projection_version")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
            None,
        ),
    }))
}

async fn schedule_activation_timer(
    repository: &SqliteDurableRepository,
    transition_key: TransitionKey,
    command: ScheduleActivationTimerCommand,
) -> Result<TransitionOutcome<TimerId>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    match load_replay(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(_) => {
            let id = sqlx::query_scalar::<_, String>(
                "SELECT timer_id FROM timers WHERE run_id = ? AND created_by_transition_key = ?",
            )
            .bind(command.run_id().as_str())
            .bind(transition_key.as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: model_data(TimerId::new(id))?,
            });
        }
        Replay::Vacant => {}
    }
    let row = sqlx::query(
        "SELECT scope_instance_id, node_id, projection_version, lifecycle
         FROM node_activations WHERE run_id = ? AND activation_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::activation_not_found)?;
    let lifecycle = parse_activation_lifecycle(
        &row.try_get::<String, _>("lifecycle")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    if lifecycle.is_terminal()
        || (command.kind() == ActivationTimerKind::Wait
            && lifecycle != ActivationLifecycle::Waiting)
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let id = model_data(TimerId::new(timer_id(
        &transition_key,
        activation_adapter::activation_timer_kind_str(command.kind()),
    )))?;
    let now = now_text(Utc::now());
    sqlx::query(
        "INSERT INTO timers (
            run_id, timer_id, activation_id, timer_kind, timer_state, deadline_at,
            expected_attempt_no, expected_lease_epoch, expected_fencing_token,
            retry_budget_snapshot, created_by_transition_key, fired_by_transition_key,
            fired_event_id, projection_version, created_at, fired_at
         ) VALUES (?, ?, ?, ?, 'scheduled', ?, NULL, NULL, NULL, NULL, ?, NULL, NULL,
                   0, ?, NULL)",
    )
    .bind(command.run_id().as_str())
    .bind(id.as_str())
    .bind(command.activation_id().as_str())
    .bind(activation_adapter::activation_timer_kind_str(
        command.kind(),
    ))
    .bind(now_text(*command.deadline()))
    .bind(transition_key.as_str())
    .bind(now)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let context = ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
        model_data(insight_engine::ScopeInstanceId::new(
            row.try_get::<String, _>("scope_instance_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?,
        model_data(insight_engine::NodeId::new(
            row.try_get::<String, _>("node_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?,
        command.activation_id().clone(),
    );
    let event = model_data(PendingExecutionEvent::new(
        context,
        ExecutionEventPayload::TimerScheduled {
            timer_id: id.clone(),
            fire_at: *command.deadline(),
        },
    ))?;
    let receipt = insert_closed_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        u64_from_i64(
            row.try_get("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        event,
    )
    .await?;
    finalize_projection_checkpoints(&mut transaction, command.run_id(), receipt.event_id()).await?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: id })
}

async fn receive_signal(
    repository: &SqliteDurableRepository,
    command: ReceiveSignalCommand,
) -> Result<TransitionOutcome<SignalReceipt>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    if let Some(row) = sqlx::query(
        "SELECT signal_id, intent_hash, payload_id FROM signals_inbox
         WHERE run_id = ? AND message_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.message_id())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    {
        let stored_hash = row
            .try_get::<String, _>("intent_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        if stored_hash != intent_hash.as_str() {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let payload_id = row
            .try_get::<String, _>("payload_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let value =
            stored_value_ref(&mut transaction, command.run_id(), Some(&payload_id), None).await?;
        let receipt = activation_adapter::signal_receipt(
            model_data(SignalId::new(
                row.try_get::<String, _>("signal_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            payload_id,
            value.content_hash().clone(),
        );
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::ExactReplay {
            authoritative: receipt,
        });
    }
    let migration_state = sqlx::query_scalar::<_, Option<String>>(
        "SELECT termination_intent_reason FROM workflow_runs WHERE run_id = ?",
    )
    .bind(command.run_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if matches!(migration_state, Some(Some(reason)) if reason == "migrated") {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Err(RepositoryError::run_migrating());
    }
    let target = sqlx::query_scalar::<_, String>(
        "SELECT execution_kind FROM node_activations
         WHERE run_id = ? AND activation_id = ? AND lifecycle NOT IN
             ('succeeded', 'failed', 'cancelled', 'timed_out')",
    )
    .bind(command.run_id().as_str())
    .bind(command.target_activation_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if target.as_deref() != Some("durable_wait") {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let (payload_id, hash) =
        insert_or_get_payload(&mut transaction, command.run_id(), command.value()).await?;
    sqlx::query(
        "INSERT INTO signals_inbox (
            run_id, signal_id, message_id, intent_hash, signal_name, target_activation_id,
            payload_id, signal_state, received_at, consumed_by_transition_key,
            consumed_event_id, terminal_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL, NULL, NULL)",
    )
    .bind(command.run_id().as_str())
    .bind(command.signal_id().as_str())
    .bind(command.message_id())
    .bind(intent_hash.as_str())
    .bind(command.signal_name())
    .bind(command.target_activation_id().as_str())
    .bind(&payload_id)
    .bind(now_text(Utc::now()))
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let receipt = activation_adapter::signal_receipt(
        command.signal_id().clone(),
        payload_id,
        model_data(insight_engine::ContentHash::parse(hash))?,
    );
    let transition_key = model_data(TransitionKey::derive(
        "repository.projection_mutation",
        &[
            "signal.receive",
            command.run_id().as_str(),
            command.message_id(),
        ],
    ))?;
    let event_id = append_projection_mutation_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        ProjectionMutationKind::SignalReceived,
        0,
    )
    .await?;
    finalize_projection_checkpoints(&mut transaction, command.run_id(), &event_id).await?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: receipt })
}

async fn is_timer_late_replay(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &FireTimerCommand,
    event_id: &str,
) -> Result<bool, RepositoryError> {
    let row = sqlx::query(
        "SELECT e.schema_version AS execution_event_schema_version,
                e.kind,e.safe_payload,e.activation_id,m.activation_id AS timer_activation_id
         FROM execution_events e JOIN timers m ON m.run_id=e.run_id
            AND m.timer_id=?
         WHERE e.run_id=? AND e.event_id=?",
    )
    .bind(command.timer_id().as_str())
    .bind(command.run_id().as_str())
    .bind(event_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    decode_execution_event_schema_row(&row)?;
    if row
        .try_get::<String, _>("kind")
        .map_err(|_| RepositoryError::invalid_data())?
        != "timer.late"
    {
        return Ok(false);
    }
    let payload = serde_json::from_str::<ExecutionEventPayload>(
        &row.try_get::<String, _>("safe_payload")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if payload
        != (ExecutionEventPayload::TimerLate {
            timer_id: command.timer_id().clone(),
        })
        || row
            .try_get::<Option<String>, _>("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            != Some(
                row.try_get::<String, _>("timer_activation_id")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .as_str(),
            )
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(true)
}

async fn append_timer_late_if_wait_loser(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &FireTimerCommand,
) -> Result<Option<bool>, RepositoryError> {
    let row = sqlx::query(
        "SELECT a.activation_id,a.scope_instance_id,a.node_id,a.projection_version,
                s.consumed_event_id AS winner_event_id
         FROM timers m JOIN node_activations a ON a.run_id=m.run_id
            AND a.activation_id=m.activation_id
         JOIN scheduler_wait_registrations w ON w.run_id=m.run_id
            AND w.activation_id=m.activation_id AND w.timer_id=m.timer_id
         JOIN signals_inbox s ON s.run_id=w.run_id AND s.signal_id=w.winner_signal_id
         WHERE m.run_id=? AND m.timer_id=? AND m.timer_kind='wait'
           AND m.timer_state='cancelled' AND w.winner_kind='signal'
           AND s.signal_state='consumed'
           AND julianday(m.deadline_at) <= julianday('now')",
    )
    .bind(command.run_id().as_str())
    .bind(command.timer_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let (transition_key, intent_hash) =
        wait_late_audit_identity("timer", command.run_id(), command.timer_id().as_str())?;
    match load_replay(
        transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(replay) => {
            if !is_timer_late_replay(transaction, command, replay.event_id()).await? {
                return Err(RepositoryError::invalid_data());
            }
            return Ok(Some(false));
        }
        Replay::Vacant => {}
    }
    let activation_id = model_data(insight_engine::ActivationId::new(
        row.try_get::<String, _>("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let context = ExecutionEventContext::for_run(command.run_id().clone())
        .for_activation(
            model_data(insight_engine::ScopeInstanceId::new(
                row.try_get::<String, _>("scope_instance_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            model_data(insight_engine::NodeId::new(
                row.try_get::<String, _>("node_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            activation_id,
        )
        .caused_by(model_data(ExecutionEventId::parse(
            row.try_get::<String, _>("winner_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?);
    let event = model_data(PendingExecutionEvent::new(
        context,
        ExecutionEventPayload::TimerLate {
            timer_id: command.timer_id().clone(),
        },
    ))?;
    let receipt = insert_closed_event(
        transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        u64_from_i64(
            row.try_get::<i64, _>("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        event,
    )
    .await?;
    finalize_empty_projection_checkpoints(transaction, command.run_id(), receipt.event_id())
        .await?;
    Ok(Some(true))
}

async fn is_signal_late_replay(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &ResolveSignalCommand,
    event_id: &str,
) -> Result<bool, RepositoryError> {
    let row = sqlx::query(
        "SELECT schema_version AS execution_event_schema_version,
                kind,safe_payload,activation_id FROM execution_events
         WHERE run_id=? AND event_id=?",
    )
    .bind(command.run_id().as_str())
    .bind(event_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    decode_execution_event_schema_row(&row)?;
    if row
        .try_get::<String, _>("kind")
        .map_err(|_| RepositoryError::invalid_data())?
        != "signal.late"
    {
        return Ok(false);
    }
    let payload = row
        .try_get::<String, _>("safe_payload")
        .map_err(|_| RepositoryError::invalid_data())?;
    let ExecutionEventPayload::SignalLate { signal_id, .. } =
        serde_json::from_str::<ExecutionEventPayload>(&payload)
            .map_err(|_| RepositoryError::invalid_data())?
    else {
        return Err(RepositoryError::invalid_data());
    };
    if signal_id != *command.signal_id()
        || row
            .try_get::<Option<String>, _>("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            != Some(command.activation_id().as_str())
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(true)
}

async fn append_signal_late_if_wait_loser(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &ResolveSignalCommand,
) -> Result<Option<bool>, RepositoryError> {
    let row = sqlx::query(
        "SELECT a.scope_instance_id,a.node_id,a.projection_version,s.payload_id,
                t.fired_event_id AS winner_event_id
         FROM node_activations a JOIN signals_inbox s ON s.run_id=a.run_id
            AND s.target_activation_id=a.activation_id
         JOIN scheduler_wait_registrations w ON w.run_id=a.run_id
            AND w.activation_id=a.activation_id AND w.signal_id=s.signal_id
         JOIN timers t ON t.run_id=w.run_id AND t.timer_id=w.winner_timer_id
         WHERE a.run_id=? AND a.activation_id=? AND s.signal_id=?
           AND s.signal_state='rejected' AND w.winner_kind='timer'
           AND t.timer_state='fired'",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(command.signal_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let (transition_key, intent_hash) =
        wait_late_audit_identity("signal", command.run_id(), command.signal_id().as_str())?;
    match load_replay(
        transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(replay) => {
            if !is_signal_late_replay(transaction, command, replay.event_id()).await? {
                return Err(RepositoryError::invalid_data());
            }
            return Ok(Some(false));
        }
        Replay::Vacant => {}
    }
    let output = stored_value_ref(
        transaction,
        command.run_id(),
        Some(
            row.try_get::<String, _>("payload_id")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_str(),
        ),
        None,
    )
    .await?;
    let context = ExecutionEventContext::for_run(command.run_id().clone())
        .for_activation(
            model_data(insight_engine::ScopeInstanceId::new(
                row.try_get::<String, _>("scope_instance_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            model_data(insight_engine::NodeId::new(
                row.try_get::<String, _>("node_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            command.activation_id().clone(),
        )
        .caused_by(model_data(ExecutionEventId::parse(
            row.try_get::<String, _>("winner_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?);
    let event = model_data(PendingExecutionEvent::new(
        context,
        ExecutionEventPayload::SignalLate {
            signal_id: command.signal_id().clone(),
            value: Some(value_summary(&output)),
        },
    ))?;
    let receipt = insert_closed_event(
        transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        u64_from_i64(
            row.try_get::<i64, _>("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        event,
    )
    .await?;
    finalize_empty_projection_checkpoints(transaction, command.run_id(), receipt.event_id())
        .await?;
    Ok(Some(true))
}

pub(super) async fn reconcile_wait_late_audits(
    repository: &SqliteDurableRepository,
    limit: i64,
) -> Result<u64, RepositoryError> {
    let _writer = repository.writer.lock().await;
    let late_events = sqlx::query(
        "SELECT schema_version,event_id,run_id,transition_key,intent_hash,seq,
                occurred_at,kind,node_id,scope_instance_id,activation_id,attempt_no,
                causation_event_id,safe_payload
         FROM execution_events WHERE kind IN ('timer.late','signal.late')",
    )
    .fetch_all(&repository.pool)
    .await
    .map_err(RepositoryError::storage)?;
    for event in late_events {
        decode_closed_execution_event_row(&event)?;
    }
    let rows = sqlx::query(
        "SELECT loser_kind,run_id,activation_id,loser_id,projection_version
         FROM (
           SELECT 'timer' AS loser_kind,m.run_id,a.activation_id,m.timer_id AS loser_id,
                  a.projection_version,m.deadline_at AS ordered_at
           FROM timers m
           JOIN node_activations a ON a.run_id=m.run_id AND a.activation_id=m.activation_id
           JOIN scheduler_wait_registrations w ON w.run_id=m.run_id
              AND w.activation_id=m.activation_id AND w.timer_id=m.timer_id
           JOIN signals_inbox s ON s.run_id=w.run_id AND s.signal_id=w.winner_signal_id
           WHERE m.timer_kind='wait' AND m.timer_state='cancelled'
             AND w.winner_kind='signal' AND s.signal_state='consumed'
             AND julianday(m.deadline_at) <= julianday('now')
             AND NOT EXISTS (
               SELECT 1 FROM execution_events e
               WHERE e.run_id=m.run_id AND e.kind='timer.late'
                 AND json_extract(e.safe_payload,'$.timer_id')=m.timer_id
             )
           UNION ALL
           SELECT 'signal' AS loser_kind,s.run_id,a.activation_id,s.signal_id AS loser_id,
                  a.projection_version,COALESCE(s.terminal_at,s.received_at) AS ordered_at
           FROM signals_inbox s
           JOIN node_activations a ON a.run_id=s.run_id
              AND a.activation_id=s.target_activation_id
           JOIN scheduler_wait_registrations w ON w.run_id=s.run_id
              AND w.activation_id=s.target_activation_id AND w.signal_id=s.signal_id
           JOIN timers t ON t.run_id=w.run_id AND t.timer_id=w.winner_timer_id
           WHERE s.signal_state='rejected' AND w.winner_kind='timer'
             AND t.timer_state='fired'
             AND NOT EXISTS (
               SELECT 1 FROM execution_events e
               WHERE e.run_id=s.run_id AND e.kind='signal.late'
                 AND json_extract(e.safe_payload,'$.signal_id')=s.signal_id
             )
         ) candidates
         ORDER BY ordered_at,run_id,loser_kind,loser_id
         LIMIT ?",
    )
    .bind(limit)
    .fetch_all(&repository.pool)
    .await
    .map_err(RepositoryError::storage)?;
    let mut appended = 0_u64;
    for row in rows {
        let loser_kind = row
            .try_get::<String, _>("loser_kind")
            .map_err(|_| RepositoryError::invalid_data())?;
        let run_id = model_data(RunId::new(
            row.try_get::<String, _>("run_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let activation_id = model_data(insight_engine::ActivationId::new(
            row.try_get::<String, _>("activation_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let loser_id = row
            .try_get::<String, _>("loser_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let projection_version = u64_from_i64(
            row.try_get::<i64, _>("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let mut transaction = repository
            .pool
            .begin()
            .await
            .map_err(RepositoryError::storage)?;
        let outcome = match loser_kind.as_str() {
            "timer" => {
                let timer_id = model_data(TimerId::new(loser_id))?;
                append_timer_late_if_wait_loser(
                    &mut transaction,
                    &FireTimerCommand::new(run_id, timer_id, None),
                )
                .await?
            }
            "signal" => {
                let signal_id = model_data(SignalId::new(loser_id))?;
                append_signal_late_if_wait_loser(
                    &mut transaction,
                    &ResolveSignalCommand::new(
                        run_id,
                        activation_id,
                        signal_id,
                        projection_version,
                    ),
                )
                .await?
            }
            _ => return Err(RepositoryError::invalid_data()),
        };
        if outcome == Some(true) {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            appended = appended
                .checked_add(1)
                .ok_or_else(RepositoryError::invalid_data)?;
        } else {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
        }
    }
    Ok(appended)
}

async fn fire_timer(
    repository: &SqliteDurableRepository,
    transition_key: TransitionKey,
    command: FireTimerCommand,
) -> Result<TransitionOutcome<TimerFireAuthority>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    match load_replay(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(replay) => {
            if is_timer_late_replay(&mut transaction, &command, replay.event_id()).await? {
                transaction
                    .commit()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
            let row = sqlx::query(
                "SELECT m.timer_kind, m.deadline_at, m.fired_at,
                        m.expected_attempt_no, m.expected_lease_epoch,
                        m.retry_budget_snapshot, a.activation_id, a.scope_instance_id,
                        a.node_id, a.projection_version,
                        a.pending_retry_timer_id, a.wait_registration_transition_key,
                        a.output_payload_id, a.output_artifact_id,
                        e.schema_version AS execution_event_schema_version,e.safe_payload,
                        (SELECT w.signal_id FROM scheduler_wait_registrations w
                         WHERE w.run_id=m.run_id AND w.activation_id=m.activation_id
                           AND w.timer_id=m.timer_id) AS wait_signal_id
                 FROM timers m
                 JOIN node_activations a ON a.run_id = m.run_id
                    AND a.activation_id = m.activation_id
                 JOIN execution_events e ON e.run_id = m.run_id
                    AND e.transition_key = m.fired_by_transition_key
                 WHERE m.run_id = ? AND m.timer_id = ? AND m.timer_state = 'fired'
                   AND m.fired_by_transition_key = ?",
            )
            .bind(command.run_id().as_str())
            .bind(command.timer_id().as_str())
            .bind(transition_key.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(RepositoryError::invalid_data)?;
            decode_execution_event_schema_row(&row)?;
            let activation_id = model_data(insight_engine::ActivationId::new(
                row.try_get::<String, _>("activation_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let receipt = make_receipt(
                &replay,
                u64_from_i64(
                    row.try_get("projection_version")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                None,
            );
            let deadline = parse_time(
                &row.try_get::<String, _>("deadline_at")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let observed = parse_time(
                &row.try_get::<String, _>("fired_at")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let timer_kind = row
                .try_get::<String, _>("timer_kind")
                .map_err(|_| RepositoryError::invalid_data())?;
            let scope_identity = model_data(insight_engine::ScopeInstanceId::new(
                row.try_get::<String, _>("scope_instance_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let node_identity = model_data(insight_engine::NodeId::new(
                row.try_get::<String, _>("node_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let cause = model_data(ExecutionEventId::parse(replay.event_id().to_owned()))?;
            let timer_companion = model_data(PendingExecutionEvent::new(
                ExecutionEventContext::for_run(command.run_id().clone())
                    .for_activation(
                        scope_identity.clone(),
                        node_identity.clone(),
                        activation_id.clone(),
                    )
                    .caused_by(cause.clone()),
                ExecutionEventPayload::TimerFired {
                    timer_id: command.timer_id().clone(),
                },
            ))?;
            verify_companion_event(
                &mut transaction,
                command.run_id(),
                &transition_key,
                replay.event_id(),
                "timer_fired",
                &timer_companion,
            )
            .await?;
            let result = match timer_kind.as_str() {
                "lease" => {
                    let fence = timer_fence_from_row(&row)?;
                    let evidence_text = sqlx::query_scalar::<_, String>(
                        "SELECT effect_evidence FROM node_attempts
                         WHERE run_id = ? AND activation_id = ? AND attempt_no = ?",
                    )
                    .bind(command.run_id().as_str())
                    .bind(activation_id.as_str())
                    .bind(i64::from(fence.attempt_no().get()))
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(RepositoryError::storage)?;
                    let attempt_companion = model_data(PendingExecutionEvent::new(
                        ExecutionEventContext::for_run(command.run_id().clone())
                            .for_attempt(
                                scope_identity.clone(),
                                node_identity.clone(),
                                activation_id.clone(),
                                fence.attempt_no(),
                            )
                            .caused_by(cause.clone()),
                        ExecutionEventPayload::AttemptAbandoned {
                            effect_evidence: parse_effect_evidence(&evidence_text)?,
                        },
                    ))?;
                    verify_companion_event(
                        &mut transaction,
                        command.run_id(),
                        &transition_key,
                        replay.event_id(),
                        "attempt_terminal",
                        &attempt_companion,
                    )
                    .await?;
                    let retry = if let Some(retry_id) = row
                        .try_get::<Option<String>, _>("pending_retry_timer_id")
                        .map_err(|_| RepositoryError::invalid_data())?
                    {
                        let retry_row = sqlx::query(
                            "SELECT deadline_at, retry_budget_snapshot FROM timers
                             WHERE run_id = ? AND timer_id = ? AND timer_kind = 'retry'",
                        )
                        .bind(command.run_id().as_str())
                        .bind(&retry_id)
                        .fetch_one(&mut *transaction)
                        .await
                        .map_err(RepositoryError::storage)?;
                        Some(activation_adapter::retry_schedule_authority(
                            command.run_id().clone(),
                            activation_id.clone(),
                            fence,
                            model_data(TimerId::new(retry_id))?,
                            parse_time(
                                &retry_row
                                    .try_get::<String, _>("deadline_at")
                                    .map_err(|_| RepositoryError::invalid_data())?,
                            )?,
                            u32::try_from(
                                retry_row
                                    .try_get::<i64, _>("retry_budget_snapshot")
                                    .map_err(|_| RepositoryError::invalid_data())?,
                            )
                            .map_err(|_| RepositoryError::invalid_data())?,
                        ))
                    } else {
                        None
                    };
                    let primary = if retry.is_some() {
                        ExecutionEventPayload::ActivationRetryWait {
                            attempt_no: fence.attempt_no(),
                        }
                    } else {
                        ExecutionEventPayload::ActivationFailed {
                            attempt_no: Some(fence.attempt_no()),
                            reason: lease_terminal_reason(parse_effect_evidence(&evidence_text)?),
                            failure: None,
                        }
                    };
                    verify_primary_timer_payload(&row, &primary)?;
                    let terminal = if retry.is_none() {
                        Some(activation_adapter::committed_terminal_activation_authority(
                            receipt.clone(),
                            command.run_id().clone(),
                            scope_identity.clone(),
                            activation_id.clone(),
                            super::CommittedTerminalActivationResult::Failed {
                                reason: lease_terminal_reason(parse_effect_evidence(
                                    &evidence_text,
                                )?),
                                failure: None,
                            },
                        )?)
                    } else {
                        None
                    };
                    TimerFireAuthority::LeaseExpired {
                        receipt,
                        fence,
                        lease_timer_id: command.timer_id().clone(),
                        lease_deadline: deadline,
                        observed_at: observed,
                        retry,
                        terminal,
                    }
                }
                "retry" => {
                    verify_primary_timer_payload(&row, &ExecutionEventPayload::ActivationReady)?;
                    TimerFireAuthority::RetryDue {
                        receipt,
                        previous_fence: timer_fence_from_row(&row)?,
                        retry_timer_id: command.timer_id().clone(),
                        retry_at: deadline,
                        observed_at: observed,
                        remaining_attempt_budget: u32::try_from(
                            row.try_get::<i64, _>("retry_budget_snapshot")
                                .map_err(|_| RepositoryError::invalid_data())?,
                        )
                        .map_err(|_| RepositoryError::invalid_data())?,
                    }
                }
                "wait" => {
                    let signal_backed = row
                        .try_get::<Option<String>, _>("wait_signal_id")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .is_some();
                    if signal_backed {
                        verify_primary_timer_payload(
                            &row,
                            &ExecutionEventPayload::ActivationTimedOut,
                        )?;
                        let terminal = activation_adapter::committed_terminal_activation_authority(
                            receipt.clone(),
                            command.run_id().clone(),
                            scope_identity,
                            activation_id,
                            super::CommittedTerminalActivationResult::TimedOut,
                        )?;
                        TimerFireAuthority::ActivationTimedOut { receipt, terminal }
                    } else {
                        let registration = model_data(TransitionKey::parse(
                            row.try_get::<String, _>("wait_registration_transition_key")
                                .map_err(|_| RepositoryError::invalid_data())?,
                        ))?;
                        let payload = row
                            .try_get::<Option<String>, _>("output_payload_id")
                            .map_err(|_| RepositoryError::invalid_data())?;
                        let artifact = row
                            .try_get::<Option<String>, _>("output_artifact_id")
                            .map_err(|_| RepositoryError::invalid_data())?;
                        let output = stored_value_ref(
                            &mut transaction,
                            command.run_id(),
                            payload.as_deref(),
                            artifact.as_deref(),
                        )
                        .await?;
                        verify_primary_timer_payload(
                            &row,
                            &ExecutionEventPayload::ActivationSucceeded {
                                attempt_no: None,
                                output: Some(value_summary(&output)),
                            },
                        )?;
                        TimerFireAuthority::WaitResolved(
                            activation_adapter::wait_resolution_authority(
                                receipt,
                                command.run_id().clone(),
                                scope_identity.clone(),
                                activation_id,
                                registration,
                                insight_engine::WaitResolutionSubject::Timer(
                                    command.timer_id().clone(),
                                ),
                                transition_key,
                                output,
                            )?,
                        )
                    }
                }
                "activation_timeout" => {
                    verify_primary_timer_payload(&row, &ExecutionEventPayload::ActivationTimedOut)?;
                    if let Some(attempt) = sqlx::query_scalar::<_, i64>(
                        "SELECT attempt_no FROM node_attempts
                         WHERE run_id = ? AND activation_id = ?
                           AND completion_transition_key = ?",
                    )
                    .bind(command.run_id().as_str())
                    .bind(activation_id.as_str())
                    .bind(transition_key.as_str())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(RepositoryError::storage)?
                    {
                        let attempt_no = model_data(AttemptNo::new(
                            u32::try_from(attempt).map_err(|_| RepositoryError::invalid_data())?,
                        ))?;
                        let companion = model_data(PendingExecutionEvent::new(
                            ExecutionEventContext::for_run(command.run_id().clone())
                                .for_attempt(
                                    scope_identity.clone(),
                                    node_identity,
                                    activation_id.clone(),
                                    attempt_no,
                                )
                                .caused_by(cause),
                            ExecutionEventPayload::AttemptTimedOut,
                        ))?;
                        verify_companion_event(
                            &mut transaction,
                            command.run_id(),
                            &transition_key,
                            replay.event_id(),
                            "attempt_terminal",
                            &companion,
                        )
                        .await?;
                    }
                    let terminal = activation_adapter::committed_terminal_activation_authority(
                        receipt.clone(),
                        command.run_id().clone(),
                        scope_identity,
                        activation_id,
                        super::CommittedTerminalActivationResult::TimedOut,
                    )?;
                    TimerFireAuthority::ActivationTimedOut { receipt, terminal }
                }
                _ => return Err(RepositoryError::invalid_data()),
            };
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: result,
            });
        }
        Replay::Vacant => {}
    }

    let row = sqlx::query(
        "SELECT m.timer_kind, m.timer_state, m.deadline_at, m.expected_attempt_no,
                m.expected_lease_epoch, m.expected_fencing_token,
                m.retry_budget_snapshot, a.activation_id, a.scope_instance_id,
                a.node_id, a.lifecycle, a.effect_idempotency, a.effect_evidence,
                a.current_attempt_no, a.current_lease_epoch, a.current_fencing_token,
                a.retry_budget_remaining, a.pending_retry_timer_id,
                a.wait_registration_transition_key, a.projection_version,
                (SELECT w.signal_id FROM scheduler_wait_registrations w
                 WHERE w.run_id=m.run_id AND w.activation_id=m.activation_id
                   AND w.timer_id=m.timer_id) AS wait_signal_id
         FROM timers m JOIN node_activations a
           ON a.run_id = m.run_id AND a.activation_id = m.activation_id
         WHERE m.run_id = ? AND m.timer_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.timer_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    let deadline = parse_time(
        &row.try_get::<String, _>("deadline_at")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let observed = Utc::now();
    if row
        .try_get::<String, _>("timer_state")
        .map_err(|_| RepositoryError::invalid_data())?
        != "scheduled"
        || observed < deadline
    {
        let late = append_timer_late_if_wait_loser(&mut transaction, &command).await?;
        if late.is_some() {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
        } else {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
        }
        return Ok(TransitionOutcome::StateConflict);
    }
    let activation_id = model_data(insight_engine::ActivationId::new(
        row.try_get::<String, _>("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let scope_id = row
        .try_get::<String, _>("scope_instance_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let node_id = row
        .try_get::<String, _>("node_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let lifecycle = row
        .try_get::<String, _>("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?;
    let current_version = u64_from_i64(
        row.try_get("projection_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let next_version = current_version
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let observed_text = now_text(observed);
    let timer_kind = row
        .try_get::<String, _>("timer_kind")
        .map_err(|_| RepositoryError::invalid_data())?;
    let (payload, authority_seed) = match timer_kind.as_str() {
        "retry" => {
            if lifecycle != "retry_wait"
                || row
                    .try_get::<Option<String>, _>("pending_retry_timer_id")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .as_deref()
                    != Some(command.timer_id().as_str())
                || row
                    .try_get::<i64, _>("retry_budget_remaining")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != row
                        .try_get::<i64, _>("retry_budget_snapshot")
                        .map_err(|_| RepositoryError::invalid_data())?
            {
                transaction
                    .rollback()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
            let fence = timer_fence_from_row(&row)?;
            let rows = sqlx::query(
                "UPDATE node_activations SET lifecycle = 'ready',
                        pending_retry_timer_id = NULL, projection_version = ?, updated_at = ?
                 WHERE run_id = ? AND activation_id = ? AND lifecycle = 'retry_wait'
                   AND pending_retry_timer_id = ? AND projection_version = ?",
            )
            .bind(i64_from_u64(next_version)?)
            .bind(&observed_text)
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .bind(command.timer_id().as_str())
            .bind(i64_from_u64(current_version)?)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                return Ok(TransitionOutcome::StateConflict);
            }
            (
                ExecutionEventPayload::ActivationReady,
                TimerAuthoritySeed::Retry {
                    fence,
                    remaining: u32::try_from(
                        row.try_get::<i64, _>("retry_budget_snapshot")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )
                    .map_err(|_| RepositoryError::invalid_data())?,
                },
            )
        }
        "wait" => {
            let registration = row
                .try_get::<Option<String>, _>("wait_registration_transition_key")
                .map_err(|_| RepositoryError::invalid_data())?;
            if lifecycle != "waiting" || registration.is_none() {
                transaction
                    .rollback()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
            let signal_backed = row
                .try_get::<Option<String>, _>("wait_signal_id")
                .map_err(|_| RepositoryError::invalid_data())?
                .is_some();
            let (rows, payload, authority_seed) = if signal_backed {
                let rows = sqlx::query(
                    "UPDATE node_activations
                     SET lifecycle='timed_out',output_payload_id=NULL,output_artifact_id=NULL,
                         output_value_hash=NULL,termination_intent_reason='timed_out',
                         termination_intent_transition_key=?,termination_intent_at=?,
                         projection_version=?,updated_at=?,terminal_at=?
                     WHERE run_id=? AND activation_id=? AND lifecycle='waiting'
                       AND projection_version=? AND wait_registration_transition_key=?",
                )
                .bind(transition_key.as_str())
                .bind(&observed_text)
                .bind(i64_from_u64(next_version)?)
                .bind(&observed_text)
                .bind(&observed_text)
                .bind(command.run_id().as_str())
                .bind(activation_id.as_str())
                .bind(i64_from_u64(current_version)?)
                .bind(registration.as_deref())
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
                .rows_affected();
                (
                    rows,
                    ExecutionEventPayload::ActivationTimedOut,
                    TimerAuthoritySeed::WaitSignalTimeout,
                )
            } else {
                let output = model_data(ValueRef::inline(serde_json::Value::Null))?;
                let (payload_id, _, output_hash) =
                    persist_value_ref(&mut transaction, command.run_id(), &output).await?;
                let payload_id = payload_id.ok_or_else(RepositoryError::invalid_data)?;
                let rows = sqlx::query(
                    "UPDATE node_activations SET lifecycle = 'succeeded', output_payload_id = ?,
                            output_value_hash = ?, projection_version = ?, updated_at = ?, terminal_at = ?
                     WHERE run_id = ? AND activation_id = ? AND lifecycle = 'waiting'
                       AND projection_version = ? AND wait_registration_transition_key = ?",
                )
                .bind(payload_id)
                .bind(output_hash)
                .bind(i64_from_u64(next_version)?)
                .bind(&observed_text)
                .bind(&observed_text)
                .bind(command.run_id().as_str())
                .bind(activation_id.as_str())
                .bind(i64_from_u64(current_version)?)
                .bind(registration.as_deref())
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
                .rows_affected();
                (
                    rows,
                    ExecutionEventPayload::ActivationSucceeded {
                        attempt_no: None,
                        output: Some(value_summary(&output)),
                    },
                    TimerAuthoritySeed::Wait {
                        registration: model_data(TransitionKey::parse(
                            registration.ok_or_else(RepositoryError::invalid_data)?,
                        ))?,
                        output,
                    },
                )
            };
            if rows != 1 {
                return Ok(TransitionOutcome::StateConflict);
            }
            sqlx::query(
                "UPDATE timers SET timer_state = 'cancelled', fired_at = ?,
                        projection_version = projection_version + 1
                 WHERE run_id = ? AND activation_id = ? AND timer_kind = 'wait'
                   AND timer_id <> ? AND timer_state = 'scheduled'",
            )
            .bind(&observed_text)
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .bind(command.timer_id().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            (payload, authority_seed)
        }
        "lease" => {
            let fence = timer_fence_from_row(&row)?;
            if !matches!(lifecycle.as_str(), "leased" | "running")
                || row
                    .try_get::<Option<i64>, _>("current_attempt_no")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != row
                        .try_get::<Option<i64>, _>("expected_attempt_no")
                        .map_err(|_| RepositoryError::invalid_data())?
                || row
                    .try_get::<Option<i64>, _>("current_lease_epoch")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != row
                        .try_get::<Option<i64>, _>("expected_lease_epoch")
                        .map_err(|_| RepositoryError::invalid_data())?
                || row
                    .try_get::<Option<String>, _>("current_fencing_token")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != row
                        .try_get::<Option<String>, _>("expected_fencing_token")
                        .map_err(|_| RepositoryError::invalid_data())?
            {
                transaction
                    .rollback()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StaleLease);
            }
            let evidence = parse_effect_evidence(
                &row.try_get::<String, _>("effect_evidence")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let lost_evidence = evidence.after_lease_loss();
            let retry_safe = row
                .try_get::<String, _>("effect_idempotency")
                .map_err(|_| RepositoryError::invalid_data())?
                == "idempotent"
                || lost_evidence == EffectEvidence::NotStarted;
            let remaining = row
                .try_get::<i64, _>("retry_budget_snapshot")
                .map_err(|_| RepositoryError::invalid_data())?;
            let retry = if retry_safe && remaining > 0 {
                let retry_at = command
                    .retry_at()
                    .ok_or_else(RepositoryError::invalid_data)?;
                if retry_at <= &observed {
                    return Ok(TransitionOutcome::StateConflict);
                }
                let retry_id = model_data(TimerId::new(timer_id(&transition_key, "retry")))?;
                sqlx::query(
                    "INSERT INTO timers (
                        run_id, timer_id, activation_id, timer_kind, timer_state, deadline_at,
                        expected_attempt_no, expected_lease_epoch, expected_fencing_token,
                        retry_budget_snapshot, created_by_transition_key,
                        fired_by_transition_key, fired_event_id, projection_version,
                        created_at, fired_at
                     ) VALUES (?, ?, ?, 'retry', 'scheduled', ?, ?, ?, ?, ?, ?, NULL, NULL,
                               0, ?, NULL)",
                )
                .bind(command.run_id().as_str())
                .bind(retry_id.as_str())
                .bind(activation_id.as_str())
                .bind(now_text(*retry_at))
                .bind(i64::from(fence.attempt_no().get()))
                .bind(i64_from_u64(fence.lease_epoch().get())?)
                .bind(
                    row.try_get::<String, _>("expected_fencing_token")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .bind(remaining)
                .bind(transition_key.as_str())
                .bind(&observed_text)
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
                Some(activation_adapter::retry_schedule_authority(
                    command.run_id().clone(),
                    activation_id.clone(),
                    fence,
                    retry_id,
                    *retry_at,
                    u32::try_from(remaining).map_err(|_| RepositoryError::invalid_data())?,
                ))
            } else {
                if command.retry_at().is_some() {
                    return Ok(TransitionOutcome::StateConflict);
                }
                None
            };
            let attempt_transition_key =
                companion_transition_key(&transition_key, "attempt_terminal")?;
            let attempt_event_id = event_id(&attempt_transition_key);
            let attempt_rows = sqlx::query(
                "UPDATE node_attempts SET lifecycle = 'abandoned', effect_evidence = ?,
                        completion_transition_key = ?, terminal_event_id = ?,
                        projection_version = projection_version + 1, terminal_at = ?
                 WHERE run_id = ? AND activation_id = ? AND attempt_no = ?
                   AND lease_epoch = ? AND fencing_token = ?
                   AND lifecycle IN ('leased', 'running')",
            )
            .bind(effect_evidence_str(lost_evidence))
            .bind(transition_key.as_str())
            .bind(&attempt_event_id)
            .bind(&observed_text)
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .bind(i64::from(fence.attempt_no().get()))
            .bind(i64_from_u64(fence.lease_epoch().get())?)
            .bind(
                row.try_get::<String, _>("expected_fencing_token")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            let terminal_reason = if lost_evidence == EffectEvidence::Unknown {
                "effect_outcome_unknown"
            } else {
                "failure"
            };
            let activation_rows = sqlx::query(
                "UPDATE node_activations
                 SET lifecycle = ?, current_attempt_no = NULL, current_lease_epoch = NULL,
                     current_fencing_token = NULL, pending_retry_timer_id = ?,
                     effect_evidence = ?, termination_intent_reason = ?,
                     termination_intent_transition_key = ?, termination_intent_at = ?,
                     projection_version = ?, updated_at = ?, terminal_at = ?
                 WHERE run_id = ? AND activation_id = ? AND lifecycle = ?
                   AND current_attempt_no = ? AND current_lease_epoch = ?
                   AND current_fencing_token = ? AND projection_version = ?",
            )
            .bind(if retry.is_some() {
                "retry_wait"
            } else {
                "failed"
            })
            .bind(retry.as_ref().map(|value| value.timer_id().as_str()))
            .bind(effect_evidence_str(lost_evidence))
            .bind(if retry.is_some() {
                None
            } else {
                Some(terminal_reason)
            })
            .bind(if retry.is_some() {
                None
            } else {
                Some(transition_key.as_str())
            })
            .bind(if retry.is_some() {
                None
            } else {
                Some(observed_text.as_str())
            })
            .bind(i64_from_u64(next_version)?)
            .bind(&observed_text)
            .bind(if retry.is_some() {
                None
            } else {
                Some(observed_text.as_str())
            })
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .bind(&lifecycle)
            .bind(i64::from(fence.attempt_no().get()))
            .bind(i64_from_u64(fence.lease_epoch().get())?)
            .bind(
                row.try_get::<String, _>("expected_fencing_token")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .bind(i64_from_u64(current_version)?)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if attempt_rows != 1 || activation_rows != 1 {
                return Ok(TransitionOutcome::StaleLease);
            }
            let primary = if retry.is_some() {
                ExecutionEventPayload::ActivationRetryWait {
                    attempt_no: fence.attempt_no(),
                }
            } else {
                ExecutionEventPayload::ActivationFailed {
                    attempt_no: Some(fence.attempt_no()),
                    reason: lease_terminal_reason(lost_evidence),
                    failure: None,
                }
            };
            (
                primary,
                TimerAuthoritySeed::Lease {
                    fence,
                    retry,
                    evidence: lost_evidence,
                },
            )
        }
        "activation_timeout" => {
            if matches!(
                lifecycle.as_str(),
                "succeeded" | "failed" | "cancelled" | "timed_out"
            ) {
                return Ok(TransitionOutcome::StateConflict);
            }
            let active_attempt = row
                .try_get::<Option<i64>, _>("current_attempt_no")
                .map_err(|_| RepositoryError::invalid_data())?;
            let active_epoch = row
                .try_get::<Option<i64>, _>("current_lease_epoch")
                .map_err(|_| RepositoryError::invalid_data())?;
            let active_token = row
                .try_get::<Option<String>, _>("current_fencing_token")
                .map_err(|_| RepositoryError::invalid_data())?;
            let attempt_no = active_attempt
                .map(|attempt| {
                    model_data(AttemptNo::new(
                        u32::try_from(attempt).map_err(|_| RepositoryError::invalid_data())?,
                    ))
                })
                .transpose()?;
            let attempt_event_id = if attempt_no.is_some() {
                Some(event_id(&companion_transition_key(
                    &transition_key,
                    "attempt_terminal",
                )?))
            } else {
                None
            };
            if let (Some(attempt), Some(epoch), Some(token), Some(attempt_event_id)) = (
                active_attempt,
                active_epoch,
                active_token,
                attempt_event_id.as_ref(),
            ) {
                sqlx::query(
                    "UPDATE node_attempts SET lifecycle = 'timed_out',
                            completion_transition_key = ?, terminal_event_id = ?,
                            projection_version = projection_version + 1, terminal_at = ?
                     WHERE run_id = ? AND activation_id = ? AND attempt_no = ?
                       AND lease_epoch = ? AND fencing_token = ?
                       AND lifecycle IN ('leased', 'running')",
                )
                .bind(transition_key.as_str())
                .bind(attempt_event_id)
                .bind(&observed_text)
                .bind(command.run_id().as_str())
                .bind(activation_id.as_str())
                .bind(attempt)
                .bind(epoch)
                .bind(token)
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
            }
            let rows = sqlx::query(
                "UPDATE node_activations
                 SET lifecycle = 'timed_out', current_attempt_no = NULL,
                     current_lease_epoch = NULL, current_fencing_token = NULL,
                     pending_retry_timer_id = NULL, termination_intent_reason = 'timed_out',
                     termination_intent_transition_key = ?, termination_intent_at = ?,
                     projection_version = ?, updated_at = ?, terminal_at = ?
                 WHERE run_id = ? AND activation_id = ? AND lifecycle = ?
                   AND projection_version = ?",
            )
            .bind(transition_key.as_str())
            .bind(&observed_text)
            .bind(i64_from_u64(next_version)?)
            .bind(&observed_text)
            .bind(&observed_text)
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .bind(&lifecycle)
            .bind(i64_from_u64(current_version)?)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                return Ok(TransitionOutcome::StateConflict);
            }
            sqlx::query(
                "UPDATE timers SET timer_state = 'cancelled', fired_at = ?,
                        projection_version = projection_version + 1
                 WHERE run_id = ? AND activation_id = ? AND timer_id <> ?
                   AND timer_state = 'scheduled'",
            )
            .bind(&observed_text)
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .bind(command.timer_id().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            (
                ExecutionEventPayload::ActivationTimedOut,
                TimerAuthoritySeed::ActivationTimeout { attempt_no },
            )
        }
        _ => return Err(RepositoryError::invalid_data()),
    };
    let scope_identity = model_data(insight_engine::ScopeInstanceId::new(&scope_id))?;
    let node_identity = model_data(insight_engine::NodeId::new(&node_id))?;
    let context = ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
        scope_identity.clone(),
        node_identity.clone(),
        activation_id.clone(),
    );
    let event = model_data(PendingExecutionEvent::new(context, payload))?;
    let receipt = insert_closed_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        next_version,
        event,
    )
    .await?;
    if let TimerAuthoritySeed::Lease {
        fence, evidence, ..
    } = &authority_seed
    {
        let attempt_event = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone())
                .for_attempt(
                    scope_identity.clone(),
                    node_identity.clone(),
                    activation_id.clone(),
                    fence.attempt_no(),
                )
                .caused_by(model_data(ExecutionEventId::parse(
                    receipt.event_id().to_owned(),
                ))?),
            ExecutionEventPayload::AttemptAbandoned {
                effect_evidence: *evidence,
            },
        ))?;
        insert_companion_event(
            &mut transaction,
            command.run_id(),
            &transition_key,
            receipt.event_id(),
            "attempt_terminal",
            next_version,
            attempt_event,
        )
        .await?;
    }
    if let TimerAuthoritySeed::ActivationTimeout {
        attempt_no: Some(attempt_no),
    } = &authority_seed
    {
        let attempt_event = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone())
                .for_attempt(
                    scope_identity.clone(),
                    node_identity.clone(),
                    activation_id.clone(),
                    *attempt_no,
                )
                .caused_by(model_data(ExecutionEventId::parse(
                    receipt.event_id().to_owned(),
                ))?),
            ExecutionEventPayload::AttemptTimedOut,
        ))?;
        insert_companion_event(
            &mut transaction,
            command.run_id(),
            &transition_key,
            receipt.event_id(),
            "attempt_terminal",
            next_version,
            attempt_event,
        )
        .await?;
    }
    let timer_event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone())
            .for_activation(scope_identity.clone(), node_identity, activation_id.clone())
            .caused_by(model_data(ExecutionEventId::parse(
                receipt.event_id().to_owned(),
            ))?),
        ExecutionEventPayload::TimerFired {
            timer_id: command.timer_id().clone(),
        },
    ))?;
    let timer_receipt = insert_companion_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        receipt.event_id(),
        "timer_fired",
        next_version,
        timer_event,
    )
    .await?;
    let fired = sqlx::query(
        "UPDATE timers SET timer_state = 'fired', fired_by_transition_key = ?,
                fired_event_id = ?, projection_version = projection_version + 1, fired_at = ?
         WHERE run_id = ? AND timer_id = ? AND timer_state = 'scheduled'
           AND deadline_at <= ?",
    )
    .bind(transition_key.as_str())
    .bind(timer_receipt.event_id())
    .bind(&observed_text)
    .bind(command.run_id().as_str())
    .bind(command.timer_id().as_str())
    .bind(&observed_text)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if fired != 1 {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    if matches!(
        &authority_seed,
        TimerAuthoritySeed::Wait { .. } | TimerAuthoritySeed::WaitSignalTimeout
    ) {
        sqlx::query(
            "UPDATE scheduler_wait_registrations
             SET winner_kind='timer',winner_signal_id=NULL,winner_timer_id=?,
                 projection_version=projection_version+1,resolved_at=?
             WHERE run_id=? AND activation_id=? AND timer_id=? AND winner_kind IS NULL",
        )
        .bind(command.timer_id().as_str())
        .bind(&observed_text)
        .bind(command.run_id().as_str())
        .bind(activation_id.as_str())
        .bind(command.timer_id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        // Receipt and resolution are separate durable commands.  If receipt
        // committed first but this timeout wins the wait CAS, close the inbox
        // loser atomically with the timer instead of leaving it pending after
        // the target Activation is terminal.
        sqlx::query(
            "UPDATE signals_inbox
             SET signal_state='rejected',consumed_by_transition_key=?,
                 consumed_event_id=?,terminal_at=?,projection_version=projection_version+1
             WHERE run_id=? AND target_activation_id=? AND signal_state='pending'",
        )
        .bind(transition_key.as_str())
        .bind(timer_receipt.event_id())
        .bind(&observed_text)
        .bind(command.run_id().as_str())
        .bind(activation_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
    }
    let checkpoint_event_id = receipt.event_id().to_owned();
    let result = match authority_seed {
        TimerAuthoritySeed::Lease {
            fence,
            retry,
            evidence,
        } => {
            let terminal = if retry.is_none() {
                Some(activation_adapter::committed_terminal_activation_authority(
                    receipt.clone(),
                    command.run_id().clone(),
                    scope_identity.clone(),
                    activation_id.clone(),
                    super::CommittedTerminalActivationResult::Failed {
                        reason: lease_terminal_reason(evidence),
                        failure: None,
                    },
                )?)
            } else {
                None
            };
            TimerFireAuthority::LeaseExpired {
                receipt,
                fence,
                lease_timer_id: command.timer_id().clone(),
                lease_deadline: deadline,
                observed_at: observed,
                retry,
                terminal,
            }
        }
        TimerAuthoritySeed::Retry { fence, remaining } => TimerFireAuthority::RetryDue {
            receipt,
            previous_fence: fence,
            retry_timer_id: command.timer_id().clone(),
            retry_at: deadline,
            observed_at: observed,
            remaining_attempt_budget: remaining,
        },
        TimerAuthoritySeed::Wait {
            registration,
            output,
        } => TimerFireAuthority::WaitResolved(activation_adapter::wait_resolution_authority(
            receipt,
            command.run_id().clone(),
            scope_identity.clone(),
            activation_id,
            registration,
            insight_engine::WaitResolutionSubject::Timer(command.timer_id().clone()),
            transition_key,
            output,
        )?),
        TimerAuthoritySeed::WaitSignalTimeout => {
            let terminal = activation_adapter::committed_terminal_activation_authority(
                receipt.clone(),
                command.run_id().clone(),
                scope_identity,
                activation_id,
                super::CommittedTerminalActivationResult::TimedOut,
            )?;
            TimerFireAuthority::ActivationTimedOut { receipt, terminal }
        }
        TimerAuthoritySeed::ActivationTimeout { .. } => {
            let terminal = activation_adapter::committed_terminal_activation_authority(
                receipt.clone(),
                command.run_id().clone(),
                scope_identity,
                activation_id,
                super::CommittedTerminalActivationResult::TimedOut,
            )?;
            TimerFireAuthority::ActivationTimedOut { receipt, terminal }
        }
    };
    finalize_projection_checkpoints(&mut transaction, command.run_id(), &checkpoint_event_id)
        .await?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result })
}

enum TimerAuthoritySeed {
    Lease {
        fence: LeaseFence,
        retry: Option<RetryScheduleAuthority>,
        evidence: EffectEvidence,
    },
    Retry {
        fence: LeaseFence,
        remaining: u32,
    },
    Wait {
        registration: TransitionKey,
        output: ValueRef,
    },
    WaitSignalTimeout,
    ActivationTimeout {
        attempt_no: Option<AttemptNo>,
    },
}

fn verify_primary_timer_payload(
    row: &SqliteRow,
    expected: &ExecutionEventPayload,
) -> Result<(), RepositoryError> {
    decode_execution_event_schema_row(row)?;
    let stored = serde_json::from_str::<ExecutionEventPayload>(
        &row.try_get::<String, _>("safe_payload")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if &stored != expected {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

fn timer_fence_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<LeaseFence, RepositoryError> {
    let attempt = row
        .try_get::<Option<i64>, _>("expected_attempt_no")
        .map_err(|_| RepositoryError::invalid_data())?
        .ok_or_else(RepositoryError::invalid_data)?;
    let epoch = row
        .try_get::<Option<i64>, _>("expected_lease_epoch")
        .map_err(|_| RepositoryError::invalid_data())?
        .ok_or_else(RepositoryError::invalid_data)?;
    model_data(LeaseFence::new(
        model_data(AttemptNo::new(
            u32::try_from(attempt).map_err(|_| RepositoryError::invalid_data())?,
        ))?,
        model_data(LeaseEpoch::new(u64_from_i64(epoch)?))?,
    ))
}

fn lease_terminal_reason(evidence: EffectEvidence) -> ActivationTerminationReason {
    if evidence == EffectEvidence::Unknown {
        ActivationTerminationReason::EffectOutcomeUnknown
    } else {
        ActivationTerminationReason::Failure
    }
}

async fn resolve_wait_signal(
    repository: &SqliteDurableRepository,
    transition_key: TransitionKey,
    command: ResolveSignalCommand,
) -> Result<TransitionOutcome<WaitResolutionAuthority>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    match load_replay(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(replay) => {
            if is_signal_late_replay(&mut transaction, &command, replay.event_id()).await? {
                transaction
                    .commit()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
            let row = sqlx::query(
                "SELECT a.projection_version, a.wait_registration_transition_key,
                        a.scope_instance_id, a.node_id, s.payload_id,
                        e.schema_version AS execution_event_schema_version,e.safe_payload
                 FROM node_activations a
                 JOIN signals_inbox s ON s.run_id = a.run_id
                    AND s.target_activation_id = a.activation_id
                 JOIN execution_events e ON e.run_id = a.run_id
                    AND e.transition_key = s.consumed_by_transition_key
                 WHERE a.run_id = ? AND a.activation_id = ? AND s.signal_id = ?
                   AND s.consumed_by_transition_key = ?",
            )
            .bind(command.run_id().as_str())
            .bind(command.activation_id().as_str())
            .bind(command.signal_id().as_str())
            .bind(transition_key.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(RepositoryError::invalid_data)?;
            decode_execution_event_schema_row(&row)?;
            let registration = model_data(TransitionKey::parse(
                row.try_get::<String, _>("wait_registration_transition_key")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let payload_id = row
                .try_get::<String, _>("payload_id")
                .map_err(|_| RepositoryError::invalid_data())?;
            let output =
                stored_value_ref(&mut transaction, command.run_id(), Some(&payload_id), None)
                    .await?;
            let expected_primary = ExecutionEventPayload::ActivationSucceeded {
                attempt_no: None,
                output: Some(value_summary(&output)),
            };
            let stored_primary = serde_json::from_str::<ExecutionEventPayload>(
                &row.try_get::<String, _>("safe_payload")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?;
            if stored_primary != expected_primary {
                return Err(RepositoryError::invalid_data());
            }
            let scope_id = model_data(insight_engine::ScopeInstanceId::new(
                row.try_get::<String, _>("scope_instance_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            let companion = model_data(PendingExecutionEvent::new(
                ExecutionEventContext::for_run(command.run_id().clone())
                    .for_activation(
                        scope_id.clone(),
                        model_data(insight_engine::NodeId::new(
                            row.try_get::<String, _>("node_id")
                                .map_err(|_| RepositoryError::invalid_data())?,
                        ))?,
                        command.activation_id().clone(),
                    )
                    .caused_by(model_data(ExecutionEventId::parse(
                        replay.event_id().to_owned(),
                    ))?),
                ExecutionEventPayload::SignalReceived {
                    signal_id: command.signal_id().clone(),
                    value: Some(value_summary(&output)),
                },
            ))?;
            verify_companion_event(
                &mut transaction,
                command.run_id(),
                &transition_key,
                replay.event_id(),
                "signal_received",
                &companion,
            )
            .await?;
            let authority = activation_adapter::wait_resolution_authority(
                make_receipt(
                    &replay,
                    u64_from_i64(
                        row.try_get("projection_version")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )?,
                    None,
                ),
                command.run_id().clone(),
                scope_id,
                command.activation_id().clone(),
                registration,
                insight_engine::WaitResolutionSubject::Signal(command.signal_id().clone()),
                transition_key,
                output,
            )?;
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: authority,
            });
        }
        Replay::Vacant => {}
    }

    let row = sqlx::query(
        "SELECT a.scope_instance_id, a.node_id, a.lifecycle, a.execution_kind,
                a.projection_version, a.wait_registration_transition_key,
                s.payload_id, s.signal_state
         FROM node_activations a
         JOIN signals_inbox s ON s.run_id = a.run_id
            AND s.target_activation_id = a.activation_id
         WHERE a.run_id = ? AND a.activation_id = ? AND s.signal_id = ?",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(command.signal_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    let current_version = row
        .try_get::<i64, _>("projection_version")
        .map_err(|_| RepositoryError::invalid_data())?;
    let registration_text = row
        .try_get::<Option<String>, _>("wait_registration_transition_key")
        .map_err(|_| RepositoryError::invalid_data())?;
    if row
        .try_get::<String, _>("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?
        != "waiting"
        || row
            .try_get::<String, _>("execution_kind")
            .map_err(|_| RepositoryError::invalid_data())?
            != "durable_wait"
        || current_version != i64_from_u64(command.expected_activation_projection_version())?
        || row
            .try_get::<String, _>("signal_state")
            .map_err(|_| RepositoryError::invalid_data())?
            != "pending"
        || registration_text.is_none()
    {
        let late = append_signal_late_if_wait_loser(&mut transaction, &command).await?;
        if late.is_some() {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
        } else {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
        }
        return Ok(TransitionOutcome::StateConflict);
    }
    let registration = model_data(TransitionKey::parse(
        registration_text.ok_or_else(RepositoryError::invalid_data)?,
    ))?;
    let payload_id = row
        .try_get::<String, _>("payload_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let output =
        stored_value_ref(&mut transaction, command.run_id(), Some(&payload_id), None).await?;
    let output_hash = output.content_hash().as_str().to_owned();
    let next_version = command
        .expected_activation_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let now = now_text(Utc::now());
    let activation_rows = sqlx::query(
        "UPDATE node_activations
         SET lifecycle = 'succeeded', output_payload_id = ?, output_value_hash = ?,
             projection_version = ?, updated_at = ?, terminal_at = ?
         WHERE run_id = ? AND activation_id = ? AND lifecycle = 'waiting'
           AND execution_kind = 'durable_wait' AND projection_version = ?
           AND wait_registration_transition_key = ?",
    )
    .bind(&payload_id)
    .bind(&output_hash)
    .bind(i64_from_u64(next_version)?)
    .bind(&now)
    .bind(&now)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(current_version)
    .bind(registration.as_str())
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
    let scope_id = model_data(insight_engine::ScopeInstanceId::new(
        row.try_get::<String, _>("scope_instance_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let node_id = model_data(insight_engine::NodeId::new(
        row.try_get::<String, _>("node_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
            scope_id.clone(),
            node_id.clone(),
            command.activation_id().clone(),
        ),
        ExecutionEventPayload::ActivationSucceeded {
            attempt_no: None,
            output: Some(value_summary(&output)),
        },
    ))?;
    let receipt = insert_closed_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        next_version,
        event,
    )
    .await?;
    let signal_event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone())
            .for_activation(scope_id.clone(), node_id, command.activation_id().clone())
            .caused_by(model_data(ExecutionEventId::parse(
                receipt.event_id().to_owned(),
            ))?),
        ExecutionEventPayload::SignalReceived {
            signal_id: command.signal_id().clone(),
            value: Some(value_summary(&output)),
        },
    ))?;
    let signal_receipt = insert_companion_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        receipt.event_id(),
        "signal_received",
        next_version,
        signal_event,
    )
    .await?;
    let signal_rows = sqlx::query(
        "UPDATE signals_inbox
         SET signal_state = 'consumed', consumed_by_transition_key = ?,
             consumed_event_id = ?, terminal_at = ?,
             projection_version = projection_version + 1
         WHERE run_id = ? AND signal_id = ? AND target_activation_id = ?
           AND signal_state = 'pending'",
    )
    .bind(transition_key.as_str())
    .bind(signal_receipt.event_id())
    .bind(&now)
    .bind(command.run_id().as_str())
    .bind(command.signal_id().as_str())
    .bind(command.activation_id().as_str())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if signal_rows != 1 {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    sqlx::query(
        "UPDATE scheduler_wait_registrations
         SET winner_kind='signal',winner_signal_id=?,winner_timer_id=NULL,
             projection_version=projection_version+1,resolved_at=?
         WHERE run_id=? AND activation_id=? AND signal_id=? AND winner_kind IS NULL",
    )
    .bind(command.signal_id().as_str())
    .bind(&now)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(command.signal_id().as_str())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let checkpoint_event_id = receipt.event_id().to_owned();
    sqlx::query(
        "UPDATE timers SET timer_state = 'cancelled', fired_at = ?,
                projection_version = projection_version + 1
         WHERE run_id = ? AND activation_id = ? AND timer_kind = 'wait'
           AND timer_state = 'scheduled'",
    )
    .bind(&now)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let authority = activation_adapter::wait_resolution_authority(
        receipt,
        command.run_id().clone(),
        scope_id,
        command.activation_id().clone(),
        registration,
        insight_engine::WaitResolutionSubject::Signal(command.signal_id().clone()),
        transition_key,
        output,
    )?;
    finalize_projection_checkpoints(&mut transaction, command.run_id(), &checkpoint_event_id)
        .await?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: authority })
}

async fn claim_task_outbox(
    repository: &SqliteDurableRepository,
    claimant: &str,
    claim_seconds: u32,
    limit: u32,
) -> Result<Vec<TaskClaim>, RepositoryError> {
    if claimant.is_empty()
        || claimant.len() > 256
        || claimant
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
        || claim_seconds == 0
        || claim_seconds > 86_400
        || limit == 0
        || limit > 1_000
    {
        return Err(RepositoryError::invalid_configuration());
    }
    let _writer = repository.writer.lock().await;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let now = Utc::now();
    let now_encoded = now_text(now);
    let claim_expires_at = now
        .checked_add_signed(Duration::seconds(i64::from(claim_seconds)))
        .ok_or_else(RepositoryError::invalid_data)?;
    let expires_encoded = now_text(claim_expires_at);
    let rows = sqlx::query(
        "SELECT run_id, task_id, task_envelope
         FROM task_outbox
         WHERE (task_state = 'pending' AND available_at <= ?)
            OR (task_state = 'claimed' AND claim_expires_at <= ?)
         ORDER BY available_at, run_id, task_id LIMIT ?",
    )
    .bind(&now_encoded)
    .bind(&now_encoded)
    .bind(i64::from(limit))
    .fetch_all(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let mut claims = Vec::with_capacity(rows.len());
    let mut checkpoint_tokens: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in rows {
        let run_text = row
            .try_get::<String, _>("run_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let task = row
            .try_get::<String, _>("task_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let envelope_text = row
            .try_get::<String, _>("task_envelope")
            .map_err(|_| RepositoryError::invalid_data())?;
        let envelope: TaskEnvelope =
            serde_json::from_str(&envelope_text).map_err(|_| RepositoryError::invalid_data())?;
        let run_id = model_data(RunId::new(run_text.clone()))?;
        if envelope.run_id() != &run_id {
            return Err(RepositoryError::invalid_data());
        }
        let token = format!("claim_{}", Uuid::new_v4().simple());
        let updated = sqlx::query(
            "UPDATE task_outbox
             SET task_state = 'claimed', claimed_by = ?, claim_token = ?,
                 claim_expires_at = ?, publish_attempts = publish_attempts + 1,
                 projection_version = projection_version + 1
             WHERE run_id = ? AND task_id = ?
               AND ((task_state = 'pending' AND available_at <= ?)
                    OR (task_state = 'claimed' AND claim_expires_at <= ?))",
        )
        .bind(claimant)
        .bind(&token)
        .bind(&expires_encoded)
        .bind(&run_text)
        .bind(&task)
        .bind(&now_encoded)
        .bind(&now_encoded)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if updated == 1 {
            checkpoint_tokens
                .entry(run_text.clone())
                .or_default()
                .push(token.clone());
            claims.push(activation_adapter::task_claim(
                run_id,
                task,
                token,
                claimant.to_owned(),
                claim_expires_at,
                envelope,
            ));
        }
    }
    for (run_text, mut tokens) in checkpoint_tokens {
        tokens.sort();
        let joined_tokens = tokens.join(",");
        let run_id = model_data(RunId::new(run_text))?;
        let transition_key = model_data(TransitionKey::derive(
            "repository.projection_mutation",
            &["task_outbox.claim", run_id.as_str(), &joined_tokens],
        ))?;
        let intent_hash = canonical_intent_hash(&serde_json::json!({
            "operation": "task_outbox.claim",
            "run_id": run_id.as_str(),
            "claimant": claimant,
            "claim_tokens": tokens,
        }))?;
        let event_id = append_projection_mutation_event(
            &mut transaction,
            &run_id,
            &transition_key,
            intent_hash.as_str(),
            ProjectionMutationKind::TaskClaimed,
            0,
        )
        .await?;
        finalize_projection_checkpoints(&mut transaction, &run_id, &event_id).await?;
    }
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(claims)
}

async fn mutate_task_claim(
    repository: &SqliteDurableRepository,
    claim: &TaskClaim,
    publish: bool,
) -> Result<bool, RepositoryError> {
    let _writer = repository.writer.lock().await;
    let now = now_text(Utc::now());
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let version = if publish {
        sqlx::query_scalar::<_, i64>(
            "UPDATE task_outbox SET task_state = 'published', published_at = ?,
                    projection_version = projection_version + 1
             WHERE run_id = ? AND task_id = ? AND task_state = 'claimed'
               AND claimed_by = ? AND claim_token = ? AND claim_expires_at > ?
             RETURNING projection_version",
        )
        .bind(&now)
        .bind(claim.run_id().as_str())
        .bind(claim.task_id())
        .bind(claim.claimant())
        .bind(claim.claim_token())
        .bind(&now)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
    } else {
        sqlx::query_scalar::<_, i64>(
            "UPDATE task_outbox SET task_state = 'acked', acked_at = ?,
                    projection_version = projection_version + 1
             WHERE run_id = ? AND task_id = ? AND task_state = 'published'
               AND claimed_by = ? AND claim_token = ?
             RETURNING projection_version",
        )
        .bind(&now)
        .bind(claim.run_id().as_str())
        .bind(claim.task_id())
        .bind(claim.claimant())
        .bind(claim.claim_token())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
    };
    if let Some(version) = version {
        let operation = if publish {
            "task_outbox.publish"
        } else {
            "task_outbox.ack"
        };
        let mutation = if publish {
            ProjectionMutationKind::TaskPublished
        } else {
            ProjectionMutationKind::TaskAcknowledged
        };
        let version_text = version.to_string();
        let transition_key = model_data(TransitionKey::derive(
            "repository.projection_mutation",
            &[
                operation,
                claim.run_id().as_str(),
                claim.task_id(),
                claim.claim_token(),
                &version_text,
            ],
        ))?;
        let intent_hash = canonical_intent_hash(&serde_json::json!({
            "operation": operation,
            "run_id": claim.run_id().as_str(),
            "task_id": claim.task_id(),
            "claim_token": claim.claim_token(),
            "projection_version": version,
        }))?;
        let event_id = append_projection_mutation_event(
            &mut transaction,
            claim.run_id(),
            &transition_key,
            intent_hash.as_str(),
            mutation,
            u64_from_i64(version)?,
        )
        .await?;
        finalize_projection_checkpoints(&mut transaction, claim.run_id(), &event_id).await?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(true);
    }
    let state = sqlx::query_scalar::<_, String>(
        "SELECT task_state FROM task_outbox
         WHERE run_id = ? AND task_id = ? AND claimed_by = ? AND claim_token = ?",
    )
    .bind(claim.run_id().as_str())
    .bind(claim.task_id())
    .bind(claim.claimant())
    .bind(claim.claim_token())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let exact = if publish {
        matches!(state.as_deref(), Some("published" | "acked"))
    } else {
        state.as_deref() == Some("acked")
    };
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(exact)
}

async fn load_activation(
    pool: &sqlx::SqlitePool,
    run_id: &RunId,
    activation_id: &insight_engine::ActivationId,
) -> Result<Option<ActivationProjection>, RepositoryError> {
    let row = sqlx::query(
        "SELECT lifecycle, projection_version, current_attempt_no, current_lease_epoch,
                current_fencing_token, retry_budget_remaining, pending_retry_timer_id,
                wait_registration_transition_key
         FROM node_activations WHERE run_id = ? AND activation_id = ?",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let attempt = row
        .try_get::<Option<i64>, _>("current_attempt_no")
        .map_err(|_| RepositoryError::invalid_data())?;
    let epoch = row
        .try_get::<Option<i64>, _>("current_lease_epoch")
        .map_err(|_| RepositoryError::invalid_data())?;
    let token = row
        .try_get::<Option<String>, _>("current_fencing_token")
        .map_err(|_| RepositoryError::invalid_data())?;
    let current_fence = match (attempt, epoch, token.as_ref()) {
        (None, None, None) => None,
        (Some(attempt), Some(epoch), Some(_)) => Some(model_data(LeaseFence::new(
            model_data(AttemptNo::new(
                u32::try_from(attempt).map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            model_data(LeaseEpoch::new(u64_from_i64(epoch)?))?,
        ))?),
        _ => return Err(RepositoryError::invalid_data()),
    };
    let pending_retry_timer_id = row
        .try_get::<Option<String>, _>("pending_retry_timer_id")
        .map_err(|_| RepositoryError::invalid_data())?
        .map(TimerId::new)
        .transpose()
        .map_err(|_| RepositoryError::invalid_data())?;
    let wait_registration_transition_key = row
        .try_get::<Option<String>, _>("wait_registration_transition_key")
        .map_err(|_| RepositoryError::invalid_data())?
        .map(TransitionKey::parse)
        .transpose()
        .map_err(|_| RepositoryError::invalid_data())?;
    Ok(Some(activation_adapter::activation_projection(
        run_id.clone(),
        activation_id.clone(),
        parse_activation_lifecycle(
            &row.try_get::<String, _>("lifecycle")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        u64_from_i64(
            row.try_get("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        current_fence,
        token,
        u32::try_from(
            row.try_get::<i64, _>("retry_budget_remaining")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        pending_retry_timer_id,
        wait_registration_transition_key,
    )))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use insight_engine::{
        scheduler::LogicalOccurrence, ActivationId, ContentHash, DefinitionRevisionId,
        DeploymentRevisionId, EffectIdempotency, ExecutionKind, NodeId, ScopeInstanceId,
        WorkerCancellation, WorkerExecutionPolicy,
    };

    use super::*;
    use insight_durable::{CreateRunCommand, DurableRepository, PlanInstallOutcome};

    fn key(label: &str) -> TransitionKey {
        TransitionKey::derive("repository.activation.test", &[label]).unwrap()
    }

    async fn repository_with_run(label: &str) -> (SqliteDurableRepository, RunId) {
        let repository = SqliteDurableRepository::in_memory().await.unwrap();
        let plan = insight_durable::model::adapter::versioned_plan_for_test(
            format!("definition_{label}"),
            format!("agent_{label}"),
            "Activation repository test",
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
        .unwrap();
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

    async fn admit_and_ready(
        repository: &SqliteDurableRepository,
        run_id: &RunId,
        label: &str,
        kind: ExecutionKind,
    ) -> ActivationId {
        let activation_id = ActivationId::new(format!("activation_{label}")).unwrap();
        let admitted = repository
            .admit_activation(
                key(&format!("{label}.admit")),
                ActivationAdmissionCommand::new(
                    run_id.clone(),
                    ScopeInstanceId::root(),
                    0,
                    activation_id.clone(),
                    NodeId::new(format!("node_{label}")).unwrap(),
                    format!("stable_{label}"),
                    kind,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(admitted, TransitionOutcome::Committed { .. }));
        let ready = repository
            .make_activation_ready(
                key(&format!("{label}.ready")),
                ActivationCasCommand::new(run_id.clone(), activation_id.clone(), 0),
            )
            .await
            .unwrap();
        assert!(matches!(ready, TransitionOutcome::Committed { .. }));
        activation_id
    }

    #[tokio::test]
    async fn lease_heartbeat_outbox_and_completion_share_fenced_authority() {
        let (repository, run_id) = repository_with_run("worker_contract").await;
        let activation_id = admit_and_ready(
            &repository,
            &run_id,
            "worker_contract",
            ExecutionKind::Worker(
                WorkerExecutionPolicy::new(
                    EffectIdempotency::Idempotent,
                    2,
                    WorkerCancellation::LeaseOnly,
                )
                .unwrap(),
            ),
        )
        .await;
        let lease_key = key("worker_contract.lease");
        let lease_command = GrantAttemptLeaseCommand::new(
            run_id.clone(),
            activation_id.clone(),
            1,
            "worker-a",
            60,
            super::super::TaskDispatchSpec::new("contract-v1", None).unwrap(),
        )
        .unwrap();
        let lease = match repository
            .grant_attempt_lease(lease_key.clone(), lease_command.clone())
            .await
            .unwrap()
        {
            TransitionOutcome::Committed { result } => result,
            other => panic!("unexpected lease outcome: {other:?}"),
        };
        assert!(matches!(
            repository
                .grant_attempt_lease(lease_key, lease_command)
                .await
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));

        let claims = repository
            .claim_task_outbox("publisher-a", 30, 10)
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].task_id(), lease.task_id());
        assert_eq!(claims[0].envelope().fence().unwrap(), lease.fence());
        assert!(repository.mark_task_published(&claims[0]).await.unwrap());
        assert!(repository.mark_task_published(&claims[0]).await.unwrap());
        assert!(repository.ack_task(&claims[0]).await.unwrap());

        let initial_fence = FencedAttemptCommand::new(
            run_id.clone(),
            activation_id.clone(),
            lease.fence(),
            lease.fencing_token(),
            "worker-a",
            2,
            0,
        )
        .unwrap();
        assert!(matches!(
            repository
                .heartbeat_attempt(
                    HeartbeatAttemptCommand::new(initial_fence.clone(), 120).unwrap()
                )
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            repository
                .heartbeat_attempt(
                    HeartbeatAttemptCommand::new(initial_fence.clone(), 120).unwrap()
                )
                .await
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        let running = FencedAttemptCommand::new(
            run_id.clone(),
            activation_id.clone(),
            lease.fence(),
            lease.fencing_token(),
            "worker-a",
            2,
            1,
        )
        .unwrap();
        assert!(matches!(
            repository
                .mark_attempt_running(key("worker_contract.running"), running.clone())
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        let completion = CompleteAttemptCommand::new(
            FencedAttemptCommand::new(
                run_id.clone(),
                activation_id.clone(),
                lease.fence(),
                lease.fencing_token(),
                "worker-a",
                3,
                2,
            )
            .unwrap(),
            AttemptCompletion::Succeeded {
                output: ValueRef::inline(json!({"ok": true})).unwrap(),
            },
        );
        let completion_key = key("worker_contract.complete");
        assert!(matches!(
            repository
                .complete_attempt(completion_key.clone(), completion.clone())
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            repository
                .complete_attempt(completion_key, completion)
                .await
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        assert_eq!(
            repository
                .load_activation(&run_id, &activation_id)
                .await
                .unwrap()
                .unwrap()
                .lifecycle(),
            ActivationLifecycle::Succeeded
        );
        assert!(matches!(
            repository
                .heartbeat_attempt(HeartbeatAttemptCommand::new(initial_fence, 120).unwrap())
                .await
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
    }

    #[tokio::test]
    async fn exhausted_lease_loss_commits_truthful_terminal_without_synthetic_failure() {
        let (repository, run_id) = repository_with_run("lease_exhausted").await;
        let activation_id = admit_and_ready(
            &repository,
            &run_id,
            "lease_exhausted",
            ExecutionKind::Worker(
                WorkerExecutionPolicy::new(
                    EffectIdempotency::Idempotent,
                    1,
                    WorkerCancellation::LeaseOnly,
                )
                .unwrap(),
            ),
        )
        .await;
        let lease = match repository
            .grant_attempt_lease(
                key("lease_exhausted.grant"),
                GrantAttemptLeaseCommand::new(
                    run_id.clone(),
                    activation_id.clone(),
                    1,
                    "worker-a",
                    60,
                    super::super::TaskDispatchSpec::new("contract-v1", None).unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap()
        {
            TransitionOutcome::Committed { result } => result,
            other => panic!("unexpected lease outcome: {other:?}"),
        };
        let expired = now_text(Utc::now() - Duration::seconds(1));
        sqlx::query(
            "UPDATE node_attempts SET lease_expires_at = ?
             WHERE run_id = ? AND activation_id = ? AND attempt_no = ?",
        )
        .bind(&expired)
        .bind(run_id.as_str())
        .bind(activation_id.as_str())
        .bind(i64::from(lease.fence().attempt_no().get()))
        .execute(&repository.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE timers SET deadline_at = ? WHERE run_id = ? AND timer_id = ?")
            .bind(&expired)
            .bind(run_id.as_str())
            .bind(lease.lease_timer_id().as_str())
            .execute(&repository.pool)
            .await
            .unwrap();

        let fire_key = key("lease_exhausted.fire");
        let command = FireTimerCommand::new(run_id.clone(), lease.lease_timer_id().clone(), None);
        assert!(matches!(
            repository
                .fire_timer(fire_key.clone(), command.clone())
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            repository.fire_timer(fire_key, command).await.unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        assert_eq!(
            repository
                .load_activation(&run_id, &activation_id)
                .await
                .unwrap()
                .unwrap()
                .lifecycle(),
            ActivationLifecycle::Failed
        );
        let payload: String = sqlx::query_scalar(
            "SELECT safe_payload FROM execution_events
             WHERE run_id = ? AND kind = 'activation.failed'",
        )
        .bind(run_id.as_str())
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        assert!(matches!(
            serde_json::from_str::<ExecutionEventPayload>(&payload).unwrap(),
            ExecutionEventPayload::ActivationFailed { failure: None, .. }
        ));
    }

    #[tokio::test]
    async fn signal_replay_and_resolution_recompute_persisted_payload_authority() {
        let (repository, run_id) = repository_with_run("signal_payload_authority").await;
        let activation_id = admit_and_ready(
            &repository,
            &run_id,
            "signal_payload_authority",
            ExecutionKind::DurableWait,
        )
        .await;
        repository
            .register_wait(
                key("signal_payload_authority.register"),
                RegisterWaitCommand::new(run_id.clone(), activation_id.clone(), 1, None),
            )
            .await
            .unwrap();
        let signal_id = SignalId::new("signal_payload_authority").unwrap();
        let receive = ReceiveSignalCommand::new(
            run_id.clone(),
            signal_id.clone(),
            "message-signal-payload-authority",
            "resume",
            activation_id.clone(),
            json!({"answer": 42}),
        )
        .unwrap();
        assert!(matches!(
            repository.receive_signal(receive.clone()).await.unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        let payload_id: String = sqlx::query_scalar(
            "SELECT payload_id FROM signals_inbox WHERE run_id=? AND signal_id=?",
        )
        .bind(run_id.as_str())
        .bind(signal_id.as_str())
        .fetch_one(repository.test_pool())
        .await
        .unwrap();
        let overwrite = |value: serde_json::Value| {
            let repository = repository.clone();
            let run_id = run_id.clone();
            let payload_id = payload_id.clone();
            async move {
                sqlx::query("UPDATE payloads SET inline_value=? WHERE run_id=? AND payload_id=?")
                    .bind(serde_jcs::to_string(&value).unwrap())
                    .bind(run_id.as_str())
                    .bind(payload_id)
                    .execute(repository.test_pool())
                    .await
                    .unwrap();
            }
        };
        overwrite(json!({"answer": 43})).await;
        assert_eq!(
            repository
                .receive_signal(receive.clone())
                .await
                .unwrap_err()
                .code(),
            insight_engine::repository::REPOSITORY_DATA_INVALID
        );
        overwrite(json!({"answer": 42})).await;
        assert!(matches!(
            repository.receive_signal(receive).await.unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        overwrite(json!({"answer": 43})).await;
        assert_eq!(
            repository
                .resolve_wait_signal(
                    key("signal_payload_authority.resolve"),
                    ResolveSignalCommand::new(run_id.clone(), activation_id.clone(), signal_id, 2),
                )
                .await
                .unwrap_err()
                .code(),
            insight_engine::repository::REPOSITORY_DATA_INVALID
        );
        assert_eq!(
            repository
                .load_activation(&run_id, &activation_id)
                .await
                .unwrap()
                .unwrap()
                .lifecycle(),
            ActivationLifecycle::Waiting
        );
    }

    #[tokio::test]
    async fn signal_and_due_wait_timer_have_exactly_one_terminal_winner() {
        let (repository, run_id) = repository_with_run("wait_race").await;
        let activation_id = admit_and_ready(
            &repository,
            &run_id,
            "wait_race",
            ExecutionKind::DurableWait,
        )
        .await;
        let registration_key = key("wait_race.register");
        let deadline = Utc::now() - Duration::seconds(1);
        repository
            .register_wait(
                registration_key.clone(),
                RegisterWaitCommand::new(run_id.clone(), activation_id.clone(), 1, Some(deadline)),
            )
            .await
            .unwrap();
        let signal_id = SignalId::new("signal_wait_race").unwrap();
        repository
            .receive_signal(
                ReceiveSignalCommand::new(
                    run_id.clone(),
                    signal_id.clone(),
                    "message-wait-race",
                    "resume",
                    activation_id.clone(),
                    json!({"winner": "signal"}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let timer = TimerId::new(timer_id(&registration_key, "wait")).unwrap();
        sqlx::query(
            "INSERT INTO scheduler_wait_registrations (
                run_id,wait_id,activation_id,node_id,occurrence_key,signal_name,signal_id,
                timer_id,due_at_ms,payload_type,winner_kind,winner_signal_id,winner_timer_id,
                projection_version,created_at,resolved_at
             ) SELECT a.run_id,?,a.activation_id,a.node_id,?,'resume',?,?,0,NULL,
                      NULL,NULL,NULL,0,CURRENT_TIMESTAMP,NULL
               FROM node_activations a WHERE a.run_id=? AND a.activation_id=?",
        )
        .bind("wait_race")
        .bind(serde_json::to_string(&LogicalOccurrence::entry()).unwrap())
        .bind(signal_id.as_str())
        .bind(timer.as_str())
        .bind(run_id.as_str())
        .bind(activation_id.as_str())
        .execute(&repository.pool)
        .await
        .unwrap();
        let signal_repository = repository.clone();
        let timer_repository = repository.clone();
        let signal_key = key("wait_race.signal");
        let signal_command =
            ResolveSignalCommand::new(run_id.clone(), activation_id.clone(), signal_id, 2);
        let timer_key = key("wait_race.timer");
        let timer_command = FireTimerCommand::new(run_id.clone(), timer, None);
        let (signal, timer_result) = tokio::join!(
            signal_repository.resolve_wait_signal(signal_key.clone(), signal_command.clone()),
            timer_repository.fire_timer(timer_key.clone(), timer_command.clone())
        );
        let signal_won = matches!(signal.unwrap(), TransitionOutcome::Committed { .. });
        let timer_won = matches!(timer_result.unwrap(), TransitionOutcome::Committed { .. });
        let outcomes = [signal_won, timer_won];
        assert_eq!(outcomes.into_iter().filter(|won| *won).count(), 1);
        if signal_won {
            assert_eq!(
                repository
                    .fire_timer(timer_key, timer_command)
                    .await
                    .unwrap(),
                TransitionOutcome::StateConflict
            );
        } else {
            assert_eq!(
                repository
                    .resolve_wait_signal(signal_key, signal_command)
                    .await
                    .unwrap(),
                TransitionOutcome::StateConflict
            );
        }
        assert_eq!(
            repository
                .load_activation(&run_id, &activation_id)
                .await
                .unwrap()
                .unwrap()
                .lifecycle(),
            if signal_won {
                ActivationLifecycle::Succeeded
            } else {
                ActivationLifecycle::TimedOut
            }
        );
        let terminal_events: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM execution_events
             WHERE run_id = ? AND activation_id = ?
               AND kind IN ('signal.received', 'timer.fired')",
        )
        .bind(run_id.as_str())
        .bind(activation_id.as_str())
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        assert_eq!(terminal_events, 1);
    }
}
