use super::RepositoryErrorExt as _;

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use insight_durable::activation::adapter::{
    self as activation_adapter, effect_evidence_str, execution_kind_fields,
    parse_activation_lifecycle, parse_effect_evidence,
};
use serde_json::Value;
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use insight_durable::common::adapter::{
    self as common_contract_adapter, canonical_intent_hash, companion_event_intent_hash,
    companion_transition_key, decode_execution_event_schema_version, event_id, fencing_token,
    i64_from_u64, task_id, timer_id, u64_from_i64, validate_inline_payload,
    wait_late_audit_identity,
};

use insight_engine::{
    ActivationId, ActivationLifecycle, ArtifactId, ArtifactRef, AttemptNo, EffectEvidence,
    ExecutionEventContext, ExecutionEventId, ExecutionEventPayload, ExecutionValueSummary,
    LeaseEpoch, LeaseFence, PendingExecutionEvent, ProjectionMutationKind, RunId, SignalId,
    TimerId, TransitionKey, TransitionOutcome, ValueRef,
};

use super::postgres::{
    allocate_event_seq, decode_execution_event_row as decode_closed_execution_event_row,
    insert_event, insert_or_get_payload, load_replay, lock_run_for_event_write,
    lock_runs_for_event_write, Replay,
};
use super::postgres_projection::{
    append_projection_mutation_event, finalize_empty_projection_checkpoints,
    finalize_projection_checkpoints,
};
use super::{
    database_time, ActivationAdmissionCommand, ActivationCasCommand, ActivationCommitReceipt,
    ActivationDurableRepository, ActivationProjection, ActivationTimerKind, AttemptCompletion,
    AttemptCompletionAuthority, CompleteAttemptCommand, FencedAttemptCommand, FireTimerCommand,
    GrantAttemptLeaseCommand, HeartbeatAttemptCommand, LeaseGrantAuthority,
    PostgresDurableRepository, ReceiveSignalCommand, RecordEffectEvidenceCommand,
    RegisterWaitCommand, RepositoryError, ResolveSignalCommand, RetryScheduleAuthority,
    ScheduleActivationTimerCommand, SignalReceipt, TaskClaim, TaskEnvelope, TimerFireAuthority,
    WaitResolutionAuthority,
};

fn model_data<T>(value: Result<T, insight_engine::ModelError>) -> Result<T, RepositoryError> {
    value.map_err(|_| RepositoryError::invalid_data())
}

fn decode_execution_event_schema_row(row: &PgRow) -> Result<(), RepositoryError> {
    decode_execution_event_schema_version(i64::from(
        row.try_get::<i32, _>("execution_event_schema_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))
}

fn attempt_i32(value: AttemptNo) -> Result<i32, RepositoryError> {
    i32::try_from(value.get()).map_err(|_| RepositoryError::invalid_data())
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
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    activation_id: &ActivationId,
) -> Result<u64, RepositoryError> {
    let value = sqlx::query_scalar::<_, i64>(
        "SELECT projection_version FROM node_activations
         WHERE run_id = $1 AND activation_id = $2",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::activation_not_found)?;
    u64_from_i64(value)
}

async fn insert_closed_event(
    transaction: &mut Transaction<'_, Postgres>,
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
    transaction: &mut Transaction<'_, Postgres>,
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
    transaction: &mut Transaction<'_, Postgres>,
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

fn activation_context(
    run_id: &RunId,
    scope_id: &str,
    node_id: &str,
    activation_id: &ActivationId,
) -> Result<ExecutionEventContext, RepositoryError> {
    Ok(
        ExecutionEventContext::for_run(run_id.clone()).for_activation(
            model_data(insight_engine::ScopeInstanceId::new(scope_id))?,
            model_data(insight_engine::NodeId::new(node_id))?,
            activation_id.clone(),
        ),
    )
}

fn attempt_context(
    run_id: &RunId,
    scope_id: &str,
    node_id: &str,
    activation_id: &ActivationId,
    attempt_no: AttemptNo,
) -> Result<ExecutionEventContext, RepositoryError> {
    Ok(ExecutionEventContext::for_run(run_id.clone()).for_attempt(
        model_data(insight_engine::ScopeInstanceId::new(scope_id))?,
        model_data(insight_engine::NodeId::new(node_id))?,
        activation_id.clone(),
        attempt_no,
    ))
}

fn value_summary(value: &ValueRef) -> ExecutionValueSummary {
    let size = match value {
        ValueRef::Inline(value) => value.canonical_bytes(),
        ValueRef::Artifact(value) => value.size_bytes(),
    };
    ExecutionValueSummary::new(value.content_hash().clone(), size)
}

async fn persist_value_ref(
    transaction: &mut Transaction<'_, Postgres>,
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
                "SELECT 1 FROM artifacts WHERE run_id = $1 AND artifact_id = $2
                 AND content_hash = $3 AND artifact_state IN ('verified', 'referenced')",
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

async fn stored_value_ref(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    payload_id: Option<&str>,
    artifact_id: Option<&str>,
) -> Result<ValueRef, RepositoryError> {
    if let Some(payload_id) = payload_id {
        let row = sqlx::query(
            "SELECT payload_id,content_hash,canonical_bytes,encoding,inline_value,binary_value
             FROM payloads WHERE run_id = $1 AND payload_id = $2",
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
        let validated = validate_inline_payload(
            &stored_payload_id,
            &row.try_get::<String, _>("content_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get::<i64, _>("canonical_bytes")
                .map_err(|_| RepositoryError::invalid_data())?,
            &row.try_get::<String, _>("encoding")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get::<Option<Value>, _>("inline_value")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?,
            None,
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
         WHERE run_id = $1 AND artifact_id = $2
           AND artifact_state IN ('verified', 'referenced')",
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
    Ok(ValueRef::Artifact(model_data(ArtifactRef::new(
        model_data(ArtifactId::new(artifact_id))?,
        content_hash,
        size,
        row.try_get("media_type")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?))
}

async fn load_attempt_identity_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    command: &FencedAttemptCommand,
) -> Result<Option<(String, String, String, String, String, i64, i64)>, RepositoryError> {
    let row = sqlx::query(
        "SELECT a.scope_instance_id, a.node_id, a.lifecycle AS activation_lifecycle,
                t.lifecycle AS attempt_lifecycle, t.effect_evidence,
                a.projection_version AS activation_version,
                t.projection_version AS attempt_version
         FROM node_activations a JOIN node_attempts t
           ON t.run_id = a.run_id AND t.activation_id = a.activation_id
         WHERE a.run_id = $1 AND a.activation_id = $2 AND t.attempt_no = $3
           AND t.lease_epoch = $4 AND t.fencing_token = $5 AND t.worker_id = $6
           AND a.current_attempt_no = t.attempt_no
           AND a.current_lease_epoch = t.lease_epoch
           AND a.current_fencing_token = t.fencing_token
         FOR UPDATE OF a, t",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(attempt_i32(command.fence().attempt_no())?)
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

#[async_trait::async_trait]
impl ActivationDurableRepository for PostgresDurableRepository {
    async fn admit_activation(
        &self,
        transition_key: TransitionKey,
        command: ActivationAdmissionCommand,
    ) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError> {
        admit_activation(self, transition_key, command).await
    }

    async fn make_activation_ready(
        &self,
        transition_key: TransitionKey,
        command: ActivationCasCommand,
    ) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError> {
        make_activation_ready(self, transition_key, command).await
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
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        lock_run_for_event_write(&mut transaction, run_id).await?;
        let version = sqlx::query_scalar::<_, i64>(
            "UPDATE timers SET timer_state = 'cancelled', fired_at = CURRENT_TIMESTAMP,
                    projection_version = projection_version + 1
             WHERE run_id = $1 AND timer_id = $2 AND timer_state = 'scheduled'
             RETURNING projection_version",
        )
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
        activation_id: &ActivationId,
    ) -> Result<Option<ActivationProjection>, RepositoryError> {
        load_activation(&self.pool, run_id, activation_id).await
    }
}

async fn replay_activation_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    activation_id: &ActivationId,
    transition_key: &TransitionKey,
    intent_hash: &str,
) -> Result<Option<TransitionOutcome<ActivationCommitReceipt>>, RepositoryError> {
    match load_replay(transaction, run_id, transition_key, intent_hash).await? {
        Replay::Vacant => Ok(None),
        Replay::Exact(replay) => Ok(Some(TransitionOutcome::ExactReplay {
            authoritative: make_receipt(
                &replay,
                activation_version(transaction, run_id, activation_id).await?,
                None,
            ),
        })),
    }
}

async fn admit_activation(
    repository: &PostgresDurableRepository,
    transition_key: TransitionKey,
    command: ActivationAdmissionCommand,
) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    if let Replay::Exact(replay) = load_replay(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        let activation_version =
            activation_version(&mut transaction, command.run_id(), command.activation_id()).await?;
        let scope_version = sqlx::query_scalar::<_, i64>(
            "SELECT projection_version FROM scope_instances
             WHERE run_id = $1 AND scope_instance_id = $2",
        )
        .bind(command.run_id().as_str())
        .bind(command.scope_instance_id().as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::ExactReplay {
            authoritative: make_receipt(
                &replay,
                activation_version,
                Some(u64_from_i64(scope_version)?),
            ),
        });
    }
    lock_run_for_event_write(&mut transaction, command.run_id()).await?;

    let scope = sqlx::query(
        "SELECT lifecycle, admission_state, projection_version
         FROM scope_instances WHERE run_id = $1 AND scope_instance_id = $2 FOR UPDATE",
    )
    .bind(command.run_id().as_str())
    .bind(command.scope_instance_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(scope) = scope else {
        return Ok(TransitionOutcome::StateConflict);
    };
    match load_replay(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(replay) => {
            let version =
                activation_version(&mut transaction, command.run_id(), command.activation_id())
                    .await?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: make_receipt(&replay, version, None),
            });
        }
        Replay::Vacant => {}
    }
    let scope_version = scope
        .try_get::<i64, _>("projection_version")
        .map_err(|_| RepositoryError::invalid_data())?;
    if scope
        .try_get::<String, _>("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?
        != "active"
        || scope
            .try_get::<String, _>("admission_state")
            .map_err(|_| RepositoryError::invalid_data())?
            != "open"
        || scope_version != i64_from_u64(command.expected_scope_projection_version())?
    {
        return Ok(TransitionOutcome::StateConflict);
    }
    if sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM node_activations WHERE run_id = $1
         AND (activation_id = $2 OR (scope_instance_id = $3 AND node_id = $4
              AND stable_activation_key = $5))",
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
    .map_err(RepositoryError::storage)?
    .is_some()
    {
        return Ok(TransitionOutcome::StateConflict);
    }
    let effect_id = command.effect_id();
    let (execution_kind, idempotency, retry_budget) =
        execution_kind_fields(command.execution_kind());
    sqlx::query(
        "INSERT INTO node_activations (
            run_id, activation_id, scope_instance_id, node_id, stable_activation_key,
            execution_kind, lifecycle, effect_id, effect_idempotency, effect_evidence,
            retry_budget_remaining, projection_version, created_at, updated_at
         ) VALUES ($1,$2,$3,$4,$5,$6,'created',$7,$8,'not_started',$9,0,
                   CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
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
    .bind(i32::try_from(retry_budget).map_err(|_| RepositoryError::invalid_data())?)
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
    let raw = insert_closed_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        0,
        event,
    )
    .await?;
    let receipt = activation_adapter::activation_commit_receipt(
        raw.event_seq(),
        raw.event_id().to_owned(),
        0,
        None,
    );
    finalize_projection_checkpoints(&mut transaction, command.run_id(), receipt.event_id()).await?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: receipt })
}

async fn make_activation_ready(
    repository: &PostgresDurableRepository,
    transition_key: TransitionKey,
    command: ActivationCasCommand,
) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    if let Some(outcome) = replay_activation_receipt(
        &mut transaction,
        command.run_id(),
        command.activation_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        return Ok(outcome);
    }
    lock_run_for_event_write(&mut transaction, command.run_id()).await?;
    let row = sqlx::query(
        "SELECT scope_instance_id,node_id,lifecycle,projection_version
         FROM node_activations WHERE run_id=$1 AND activation_id=$2 FOR UPDATE",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(TransitionOutcome::StateConflict);
    };
    if let Some(outcome) = replay_activation_receipt(
        &mut transaction,
        command.run_id(),
        command.activation_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        return Ok(outcome);
    }
    if row
        .try_get::<String, _>("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?
        != "created"
        || row
            .try_get::<i64, _>("projection_version")
            .map_err(|_| RepositoryError::invalid_data())?
            != i64_from_u64(command.expected_projection_version())?
    {
        return Ok(TransitionOutcome::StateConflict);
    }
    let next = command
        .expected_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    sqlx::query(
        "UPDATE node_activations SET lifecycle='ready',projection_version=$1,updated_at=CURRENT_TIMESTAMP
         WHERE run_id=$2 AND activation_id=$3",
    )
    .bind(i64_from_u64(next)?)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let event = model_data(PendingExecutionEvent::new(
        activation_context(
            command.run_id(),
            &row.try_get::<String, _>("scope_instance_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            &row.try_get::<String, _>("node_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            command.activation_id(),
        )?,
        ExecutionEventPayload::ActivationReady,
    ))?;
    let receipt = insert_closed_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        next,
        event,
    )
    .await?;
    finalize_projection_checkpoints(&mut transaction, command.run_id(), receipt.event_id()).await?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result: receipt })
}

async fn lease_replay_authority(
    transaction: &mut Transaction<'_, Postgres>,
    command: &GrantAttemptLeaseCommand,
    transition_key: &TransitionKey,
    replay: &super::CommitReceipt,
) -> Result<LeaseGrantAuthority, RepositoryError> {
    let row = sqlx::query(
        "SELECT t.attempt_no,t.lease_epoch,t.fencing_token,t.lease_expires_at,
                m.timer_id,o.task_id,a.projection_version,a.scope_instance_id,
                a.node_id,e.schema_version AS execution_event_schema_version,e.safe_payload
         FROM execution_events e
         JOIN task_outbox o ON o.run_id=e.run_id AND o.created_by_transition_key=e.transition_key
         JOIN node_attempts t ON t.run_id=o.run_id AND t.activation_id=o.activation_id
            AND t.attempt_no=o.attempt_no AND t.lease_epoch=o.lease_epoch
         JOIN node_activations a ON a.run_id=t.run_id AND a.activation_id=t.activation_id
         JOIN timers m ON m.run_id=t.run_id AND m.activation_id=t.activation_id
            AND m.timer_kind='lease' AND m.expected_attempt_no=t.attempt_no
            AND m.expected_lease_epoch=t.lease_epoch
         WHERE e.run_id=$1 AND e.transition_key=$2",
    )
    .bind(command.run_id().as_str())
    .bind(transition_key.as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    decode_execution_event_schema_row(&row)?;
    let attempt = model_data(AttemptNo::new(
        u32::try_from(
            row.try_get::<i32, _>("attempt_no")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let epoch = model_data(LeaseEpoch::new(u64_from_i64(
        row.try_get("lease_epoch")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?))?;
    let fence = model_data(LeaseFence::new(attempt, epoch))?;
    let stored = serde_json::from_value::<ExecutionEventPayload>(
        row.try_get::<Value, _>("safe_payload")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if stored
        != (ExecutionEventPayload::ActivationLeased {
            attempt_no: attempt,
            lease_epoch: epoch,
        })
    {
        return Err(RepositoryError::invalid_data());
    }
    let scope = model_data(insight_engine::ScopeInstanceId::new(
        row.try_get::<String, _>("scope_instance_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let node = model_data(insight_engine::NodeId::new(
        row.try_get::<String, _>("node_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let cause = model_data(ExecutionEventId::parse(replay.event_id().to_owned()))?;
    let attempt_event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone())
            .for_attempt(
                scope.clone(),
                node.clone(),
                command.activation_id().clone(),
                attempt,
            )
            .caused_by(cause.clone()),
        ExecutionEventPayload::AttemptLeased { lease_epoch: epoch },
    ))?;
    verify_companion_event(
        transaction,
        command.run_id(),
        transition_key,
        replay.event_id(),
        "attempt_leased",
        &attempt_event,
    )
    .await?;
    let lease_timer_id = model_data(TimerId::new(
        row.try_get::<String, _>("timer_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let lease_deadline = row
        .try_get::<DateTime<Utc>, _>("lease_expires_at")
        .map_err(|_| RepositoryError::invalid_data())?;
    let timer_event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone())
            .for_activation(scope, node, command.activation_id().clone())
            .caused_by(cause),
        ExecutionEventPayload::TimerScheduled {
            timer_id: lease_timer_id.clone(),
            fire_at: lease_deadline,
        },
    ))?;
    verify_companion_event(
        transaction,
        command.run_id(),
        transition_key,
        replay.event_id(),
        "lease_timer_scheduled",
        &timer_event,
    )
    .await?;
    Ok(activation_adapter::lease_grant_authority(
        make_receipt(
            replay,
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
    ))
}

async fn grant_attempt_lease(
    repository: &PostgresDurableRepository,
    transition_key: TransitionKey,
    command: GrantAttemptLeaseCommand,
) -> Result<TransitionOutcome<LeaseGrantAuthority>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
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
            let authority =
                lease_replay_authority(&mut transaction, &command, &transition_key, &replay)
                    .await?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: authority,
            });
        }
        Replay::Vacant => {}
    }
    lock_run_for_event_write(&mut transaction, command.run_id()).await?;
    let row = sqlx::query(
        "SELECT scope_instance_id,node_id,effect_id,lifecycle,execution_kind,
                last_attempt_no,last_lease_epoch,retry_budget_remaining,projection_version
         FROM node_activations WHERE run_id=$1 AND activation_id=$2 FOR UPDATE",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::activation_not_found)?;
    match load_replay(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(replay) => {
            let authority =
                lease_replay_authority(&mut transaction, &command, &transition_key, &replay)
                    .await?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: authority,
            });
        }
        Replay::Vacant => {}
    }
    let version = row
        .try_get::<i64, _>("projection_version")
        .map_err(|_| RepositoryError::invalid_data())?;
    let retry_budget = row
        .try_get::<i32, _>("retry_budget_remaining")
        .map_err(|_| RepositoryError::invalid_data())?;
    if row
        .try_get::<String, _>("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?
        != "ready"
        || row
            .try_get::<String, _>("execution_kind")
            .map_err(|_| RepositoryError::invalid_data())?
            != "worker"
        || version != i64_from_u64(command.expected_projection_version())?
        || retry_budget <= 0
    {
        return Ok(TransitionOutcome::StateConflict);
    }
    let attempt_value = row
        .try_get::<Option<i32>, _>("last_attempt_no")
        .map_err(|_| RepositoryError::invalid_data())?
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let epoch_value = row
        .try_get::<Option<i64>, _>("last_lease_epoch")
        .map_err(|_| RepositoryError::invalid_data())?
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?
        .max(i64::from(attempt_value));
    let attempt = model_data(AttemptNo::new(
        u32::try_from(attempt_value).map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let epoch = model_data(LeaseEpoch::new(u64_from_i64(epoch_value)?))?;
    let fence = model_data(LeaseFence::new(attempt, epoch))?;
    let token = fencing_token(&transition_key);
    let lease_timer = model_data(TimerId::new(timer_id(&transition_key, "lease")))?;
    let task = task_id(&transition_key);
    let now = database_time(Utc::now());
    let deadline = now
        .checked_add_signed(Duration::seconds(i64::from(command.lease_seconds())))
        .ok_or_else(RepositoryError::invalid_data)?;
    let effect_id = row
        .try_get::<String, _>("effect_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    sqlx::query(
        "INSERT INTO node_attempts (
            run_id,activation_id,attempt_no,lease_epoch,fencing_token,effect_id,lifecycle,
            effect_evidence,worker_id,lease_expires_at,heartbeat_at,projection_version,created_at
         ) VALUES ($1,$2,$3,$4,$5,$6,'leased','not_started',$7,$8,$9,0,$9)",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(attempt_value)
    .bind(epoch_value)
    .bind(&token)
    .bind(&effect_id)
    .bind(command.worker_id())
    .bind(deadline)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let next = command
        .expected_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    sqlx::query(
        "UPDATE node_activations SET lifecycle='leased',last_attempt_no=$1,last_lease_epoch=$2,
            current_attempt_no=$1,current_lease_epoch=$2,current_fencing_token=$3,
            retry_budget_remaining=retry_budget_remaining-1,projection_version=$4,updated_at=$5
         WHERE run_id=$6 AND activation_id=$7",
    )
    .bind(attempt_value)
    .bind(epoch_value)
    .bind(&token)
    .bind(i64_from_u64(next)?)
    .bind(now)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "INSERT INTO timers (
            run_id,timer_id,activation_id,timer_kind,timer_state,deadline_at,
            expected_attempt_no,expected_lease_epoch,expected_fencing_token,
            retry_budget_snapshot,created_by_transition_key,projection_version,created_at
         ) VALUES ($1,$2,$3,'lease','scheduled',$4,$5,$6,$7,$8,$9,0,$10)",
    )
    .bind(command.run_id().as_str())
    .bind(lease_timer.as_str())
    .bind(command.activation_id().as_str())
    .bind(deadline)
    .bind(attempt_value)
    .bind(epoch_value)
    .bind(&token)
    .bind(retry_budget - 1)
    .bind(transition_key.as_str())
    .bind(now)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let node_id_text = row
        .try_get::<String, _>("node_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let node_id = model_data(insight_engine::NodeId::new(&node_id_text))?;
    let task_envelope = activation_adapter::task_envelope(
        command.run_id().clone(),
        command.activation_id().clone(),
        node_id.clone(),
        model_data(insight_engine::EffectId::new(&effect_id))?,
        fence,
        token.clone(),
        command.task(),
    );
    let envelope =
        serde_json::to_value(&task_envelope).map_err(|_| RepositoryError::canonicalization())?;
    sqlx::query(
        "INSERT INTO task_outbox (
            run_id,task_id,activation_id,attempt_no,lease_epoch,fencing_token,effect_id,
            created_by_transition_key,task_state,task_envelope,available_at,publish_attempts,created_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pending',$9,$10,0,$10)",
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
    .bind(now)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let scope = model_data(insight_engine::ScopeInstanceId::new(
        row.try_get::<String, _>("scope_instance_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
            scope.clone(),
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
        next,
        event,
    )
    .await?;
    let cause = model_data(ExecutionEventId::parse(receipt.event_id().to_owned()))?;
    let attempt_event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone())
            .for_attempt(
                scope.clone(),
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
        next,
        attempt_event,
    )
    .await?;
    let timer_event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone())
            .for_activation(scope, node_id, command.activation_id().clone())
            .caused_by(cause),
        ExecutionEventPayload::TimerScheduled {
            timer_id: lease_timer.clone(),
            fire_at: deadline,
        },
    ))?;
    insert_companion_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        receipt.event_id(),
        "lease_timer_scheduled",
        next,
        timer_event,
    )
    .await?;
    let authority = activation_adapter::lease_grant_authority(
        receipt,
        command.run_id().clone(),
        command.activation_id().clone(),
        fence,
        token,
        lease_timer,
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
    repository: &PostgresDurableRepository,
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
        return Ok(outcome);
    }
    lock_run_for_event_write(&mut transaction, command.run_id()).await?;
    let identity = load_attempt_identity_for_update(&mut transaction, &command).await?;
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
        return Ok(TransitionOutcome::StaleLease);
    };
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
        return Ok(outcome);
    }
    if activation_version != i64_from_u64(command.expected_activation_projection_version())?
        || attempt_version != i64_from_u64(command.expected_attempt_projection_version())?
    {
        return Ok(TransitionOutcome::StateConflict);
    }
    if (mark_running && (activation_lifecycle != "leased" || attempt_lifecycle != "leased"))
        || (!mark_running
            && (!matches!(activation_lifecycle.as_str(), "leased" | "running")
                || !matches!(attempt_lifecycle.as_str(), "leased" | "running")))
    {
        return Ok(TransitionOutcome::StateConflict);
    }
    let next_activation = command
        .expected_activation_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let next_attempt = command
        .expected_attempt_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let (activation_next, attempt_next, evidence_next, payload) = if mark_running {
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
        if !parse_effect_evidence(&current_evidence)?.can_transition_to(requested) {
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
    sqlx::query(
        "UPDATE node_attempts SET lifecycle=$1,effect_evidence=$2,projection_version=$3,
            started_at=CASE WHEN $1='running' THEN CURRENT_TIMESTAMP ELSE started_at END
         WHERE run_id=$4 AND activation_id=$5 AND attempt_no=$6",
    )
    .bind(attempt_next)
    .bind(&evidence_next)
    .bind(i64_from_u64(next_attempt)?)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(attempt_i32(command.fence().attempt_no())?)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "UPDATE node_activations SET lifecycle=$1,effect_evidence=$2,projection_version=$3,
            updated_at=CURRENT_TIMESTAMP WHERE run_id=$4 AND activation_id=$5",
    )
    .bind(activation_next)
    .bind(&evidence_next)
    .bind(i64_from_u64(next_activation)?)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let scope = model_data(insight_engine::ScopeInstanceId::new(&scope_id))?;
    let node = model_data(insight_engine::NodeId::new(&node_id))?;
    let event = if mark_running {
        model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
                scope.clone(),
                node.clone(),
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
        next_activation,
        event,
    )
    .await?;
    if mark_running {
        let companion = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone())
                .for_attempt(
                    scope,
                    node,
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
            next_activation,
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
    transaction: &mut Transaction<'_, Postgres>,
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
            AND e.transition_key=$1
         WHERE a.run_id=$2 AND a.activation_id=$3",
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
    let stored = serde_json::from_value::<ExecutionEventPayload>(
        row.try_get::<Value, _>("safe_payload")
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

async fn verify_heartbeat_replay(
    transaction: &mut Transaction<'_, Postgres>,
    authority: &FencedAttemptCommand,
    transition_key: &TransitionKey,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        "SELECT m.timer_id,m.deadline_at,
                e.schema_version AS execution_event_schema_version,e.safe_payload
         FROM timers m JOIN execution_events e ON e.run_id=m.run_id AND e.transition_key=$1
         WHERE m.run_id=$2 AND m.activation_id=$3 AND m.timer_kind='lease'
           AND m.expected_attempt_no=$4 AND m.expected_lease_epoch=$5
           AND m.expected_fencing_token=$6",
    )
    .bind(transition_key.as_str())
    .bind(authority.run_id().as_str())
    .bind(authority.activation_id().as_str())
    .bind(attempt_i32(authority.fence().attempt_no())?)
    .bind(i64_from_u64(authority.fence().lease_epoch().get())?)
    .bind(authority.fencing_token())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    decode_execution_event_schema_row(&row)?;
    let id = model_data(TimerId::new(
        row.try_get::<String, _>("timer_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let deadline = row
        .try_get::<DateTime<Utc>, _>("deadline_at")
        .map_err(|_| RepositoryError::invalid_data())?;
    let stored = serde_json::from_value::<ExecutionEventPayload>(
        row.try_get::<Value, _>("safe_payload")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if !matches!(stored, ExecutionEventPayload::TimerScheduled { timer_id, fire_at } if timer_id == id && fire_at <= deadline)
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn heartbeat_attempt(
    repository: &PostgresDurableRepository,
    command: HeartbeatAttemptCommand,
) -> Result<TransitionOutcome<()>, RepositoryError> {
    let authority = command.authority();
    let transition_key = activation_adapter::heartbeat_transition_key(&command)?;
    let intent_hash = canonical_intent_hash(&command)?;
    let now = Utc::now();
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    match load_replay(
        &mut transaction,
        authority.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(_) => {
            verify_heartbeat_replay(&mut transaction, authority, &transition_key).await?;
            return Ok(TransitionOutcome::ExactReplay { authoritative: () });
        }
        Replay::Vacant => {}
    }
    lock_run_for_event_write(&mut transaction, authority.run_id()).await?;
    let row = sqlx::query(
        "SELECT t.lease_expires_at,t.projection_version AS attempt_version,
                a.projection_version AS activation_version,a.scope_instance_id,a.node_id,
                m.timer_id,m.deadline_at AS timer_deadline,m.projection_version AS timer_version
         FROM node_attempts t JOIN node_activations a ON a.run_id=t.run_id AND a.activation_id=t.activation_id
         JOIN timers m ON m.run_id=t.run_id AND m.activation_id=t.activation_id
           AND m.timer_kind='lease' AND m.timer_state='scheduled'
           AND m.expected_attempt_no=t.attempt_no AND m.expected_lease_epoch=t.lease_epoch
           AND m.expected_fencing_token=t.fencing_token
         WHERE t.run_id=$1 AND t.activation_id=$2 AND t.attempt_no=$3 AND t.lease_epoch=$4
           AND t.fencing_token=$5 AND t.worker_id=$6 AND t.lifecycle IN ('leased','running')
           AND a.lifecycle IN ('leased','running') AND a.current_attempt_no=t.attempt_no
           AND a.current_lease_epoch=t.lease_epoch AND a.current_fencing_token=t.fencing_token
         FOR UPDATE OF t,a,m",
    )
    .bind(authority.run_id().as_str())
    .bind(authority.activation_id().as_str())
    .bind(attempt_i32(authority.fence().attempt_no())?)
    .bind(i64_from_u64(authority.fence().lease_epoch().get())?)
    .bind(authority.fencing_token())
    .bind(authority.worker_id())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(TransitionOutcome::StaleLease);
    };
    match load_replay(
        &mut transaction,
        authority.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(_) => {
            verify_heartbeat_replay(&mut transaction, authority, &transition_key).await?;
            return Ok(TransitionOutcome::ExactReplay { authoritative: () });
        }
        Replay::Vacant => {}
    }
    if row
        .try_get::<i64, _>("attempt_version")
        .map_err(|_| RepositoryError::invalid_data())?
        != i64_from_u64(authority.expected_attempt_projection_version())?
        || row
            .try_get::<i64, _>("activation_version")
            .map_err(|_| RepositoryError::invalid_data())?
            != i64_from_u64(authority.expected_activation_projection_version())?
    {
        return Ok(TransitionOutcome::StateConflict);
    }
    let deadline = row
        .try_get::<DateTime<Utc>, _>("lease_expires_at")
        .map_err(|_| RepositoryError::invalid_data())?;
    let timer_deadline = row
        .try_get::<DateTime<Utc>, _>("timer_deadline")
        .map_err(|_| RepositoryError::invalid_data())?;
    if now >= deadline || now >= timer_deadline || deadline != timer_deadline {
        return Ok(TransitionOutcome::StaleLease);
    }
    let candidate = now
        .checked_add_signed(Duration::seconds(i64::from(command.extend_seconds())))
        .ok_or_else(RepositoryError::invalid_data)?;
    let extended = deadline.max(candidate);
    let next_attempt = authority
        .expected_attempt_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    sqlx::query(
        "UPDATE node_attempts SET heartbeat_at=$1,lease_expires_at=$2,projection_version=$3
         WHERE run_id=$4 AND activation_id=$5 AND attempt_no=$6",
    )
    .bind(now)
    .bind(extended)
    .bind(i64_from_u64(next_attempt)?)
    .bind(authority.run_id().as_str())
    .bind(authority.activation_id().as_str())
    .bind(attempt_i32(authority.fence().attempt_no())?)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "UPDATE timers SET deadline_at=$1,projection_version=projection_version+1
         WHERE run_id=$2 AND timer_id=$3",
    )
    .bind(extended)
    .bind(authority.run_id().as_str())
    .bind(
        row.try_get::<String, _>("timer_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let event = model_data(PendingExecutionEvent::new(
        activation_context(
            authority.run_id(),
            &row.try_get::<String, _>("scope_instance_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            &row.try_get::<String, _>("node_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            authority.activation_id(),
        )?,
        ExecutionEventPayload::TimerScheduled {
            timer_id: model_data(TimerId::new(
                row.try_get::<String, _>("timer_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            fire_at: extended,
        },
    ))?;
    let receipt = insert_closed_event(
        &mut transaction,
        authority.run_id(),
        &transition_key,
        intent_hash.as_str(),
        authority.expected_activation_projection_version(),
        event,
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

async fn completion_replay_authority(
    transaction: &mut Transaction<'_, Postgres>,
    transition_key: &TransitionKey,
    command: &CompleteAttemptCommand,
    replay: &super::CommitReceipt,
) -> Result<AttemptCompletionAuthority, RepositoryError> {
    let authority = command.authority();
    let row = sqlx::query(
        "SELECT t.lifecycle AS attempt_lifecycle,t.output_payload_id,t.output_artifact_id,
                a.lifecycle AS activation_lifecycle,a.projection_version,a.scope_instance_id,
                a.node_id,a.pending_retry_timer_id,
                e.schema_version AS execution_event_schema_version,e.safe_payload
         FROM node_attempts t JOIN node_activations a ON a.run_id=t.run_id AND a.activation_id=t.activation_id
         JOIN execution_events e ON e.run_id=t.run_id AND e.transition_key=t.completion_transition_key
         WHERE t.run_id=$1 AND t.activation_id=$2 AND t.completion_transition_key=$3",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(transition_key.as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    decode_execution_event_schema_row(&row)?;
    let receipt = make_receipt(
        replay,
        u64_from_i64(
            row.try_get("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        None,
    );
    let lifecycle = parse_activation_lifecycle(
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
    let expected_primary = match command.completion() {
        AttemptCompletion::Succeeded { output } => ExecutionEventPayload::ActivationSucceeded {
            attempt_no: Some(authority.fence().attempt_no()),
            output: Some(value_summary(output)),
        },
        AttemptCompletion::Failed { .. } if lifecycle == ActivationLifecycle::RetryWait => {
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
    let stored = serde_json::from_value::<ExecutionEventPayload>(
        row.try_get::<Value, _>("safe_payload")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if stored != expected_primary {
        return Err(RepositoryError::invalid_data());
    }
    let attempt_payload = match command.completion() {
        AttemptCompletion::Succeeded { output } => ExecutionEventPayload::AttemptSucceeded {
            output: Some(value_summary(output)),
        },
        AttemptCompletion::Failed { failure, .. } => ExecutionEventPayload::AttemptFailed {
            failure: failure.clone(),
        },
    };
    let companion = model_data(PendingExecutionEvent::new(
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
        attempt_payload,
    ))?;
    verify_companion_event(
        transaction,
        command.run_id(),
        transition_key,
        replay.event_id(),
        "attempt_terminal",
        &companion,
    )
    .await?;
    if lifecycle == ActivationLifecycle::Succeeded {
        let payload = row
            .try_get::<Option<String>, _>("output_payload_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let artifact = row
            .try_get::<Option<String>, _>("output_artifact_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let output = stored_value_ref(
            transaction,
            command.run_id(),
            payload.as_deref(),
            artifact.as_deref(),
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
        Ok(AttemptCompletionAuthority::Succeeded {
            terminal,
            fence: authority.fence(),
            output,
        })
    } else if lifecycle == ActivationLifecycle::RetryWait {
        let id = row
            .try_get::<Option<String>, _>("pending_retry_timer_id")
            .map_err(|_| RepositoryError::invalid_data())?
            .ok_or_else(RepositoryError::invalid_data)?;
        let timer = sqlx::query(
            "SELECT deadline_at,retry_budget_snapshot FROM timers
             WHERE run_id=$1 AND timer_id=$2 AND timer_kind='retry'",
        )
        .bind(command.run_id().as_str())
        .bind(&id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let retry = activation_adapter::retry_schedule_authority(
            command.run_id().clone(),
            command.activation_id().clone(),
            authority.fence(),
            model_data(TimerId::new(id))?,
            timer
                .try_get("deadline_at")
                .map_err(|_| RepositoryError::invalid_data())?,
            u32::try_from(
                timer
                    .try_get::<i32, _>("retry_budget_snapshot")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?,
        );
        Ok(AttemptCompletionAuthority::RetryScheduled { receipt, retry })
    } else {
        let (reason, failure) = match command.completion() {
            AttemptCompletion::Failed {
                reason, failure, ..
            } => (*reason, failure.clone()),
            AttemptCompletion::Succeeded { .. } => return Err(RepositoryError::invalid_data()),
        };
        let terminal = activation_adapter::committed_terminal_activation_authority(
            receipt,
            command.run_id().clone(),
            scope_id,
            command.activation_id().clone(),
            super::CommittedTerminalActivationResult::Failed { reason, failure },
        )?;
        Ok(AttemptCompletionAuthority::Failed { terminal })
    }
}

async fn complete_attempt(
    repository: &PostgresDurableRepository,
    transition_key: TransitionKey,
    command: CompleteAttemptCommand,
) -> Result<TransitionOutcome<AttemptCompletionAuthority>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let authority = command.authority();
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
            let result =
                completion_replay_authority(&mut transaction, &transition_key, &command, &replay)
                    .await?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: result,
            });
        }
        Replay::Vacant => {}
    }
    lock_run_for_event_write(&mut transaction, command.run_id()).await?;
    let identity = load_attempt_identity_for_update(&mut transaction, authority).await?;
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
        return Ok(TransitionOutcome::StaleLease);
    };
    match load_replay(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(replay) => {
            let result =
                completion_replay_authority(&mut transaction, &transition_key, &command, &replay)
                    .await?;
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: result,
            });
        }
        Replay::Vacant => {}
    }
    if activation_version != i64_from_u64(authority.expected_activation_projection_version())?
        || attempt_version != i64_from_u64(authority.expected_attempt_projection_version())?
    {
        return Ok(TransitionOutcome::StateConflict);
    }
    let valid = match command.completion() {
        AttemptCompletion::Succeeded { .. } => {
            activation_lifecycle == "running" && attempt_lifecycle == "running"
        }
        AttemptCompletion::Failed { .. } => {
            matches!(activation_lifecycle.as_str(), "leased" | "running")
                && matches!(attempt_lifecycle.as_str(), "leased" | "running")
        }
    };
    if !valid {
        return Ok(TransitionOutcome::StateConflict);
    }
    let activation_row = sqlx::query(
        "SELECT effect_idempotency,retry_budget_remaining FROM node_activations
         WHERE run_id=$1 AND activation_id=$2",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .fetch_one(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let idempotency = activation_row
        .try_get::<String, _>("effect_idempotency")
        .map_err(|_| RepositoryError::invalid_data())?;
    let remaining = activation_row
        .try_get::<i32, _>("retry_budget_remaining")
        .map_err(|_| RepositoryError::invalid_data())?;
    let next_activation = authority
        .expected_activation_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let next_attempt = authority
        .expected_attempt_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let attempt_event_id = event_id(&companion_transition_key(
        &transition_key,
        "attempt_terminal",
    )?);
    let now = Utc::now();
    let (
        attempt_next,
        activation_next,
        failure_code,
        output_storage,
        retry_authority,
        attempt_payload,
    ) = match command.completion() {
        AttemptCompletion::Succeeded { output } => (
            "succeeded",
            "succeeded",
            None,
            Some(persist_value_ref(&mut transaction, command.run_id(), output).await?),
            None,
            ExecutionEventPayload::AttemptSucceeded {
                output: Some(value_summary(output)),
            },
        ),
        AttemptCompletion::Failed {
            failure, retry_at, ..
        } => {
            let evidence = parse_effect_evidence(&evidence)?;
            let retry_safe = idempotency == "idempotent" || evidence == EffectEvidence::NotStarted;
            let retry = if let Some(retry_at) = retry_at {
                if !retry_safe || remaining <= 0 || retry_at <= &now {
                    return Ok(TransitionOutcome::StateConflict);
                }
                let retry_timer = model_data(TimerId::new(timer_id(&transition_key, "retry")))?;
                sqlx::query(
                    "INSERT INTO timers (
                        run_id,timer_id,activation_id,timer_kind,timer_state,deadline_at,
                        expected_attempt_no,expected_lease_epoch,expected_fencing_token,
                        retry_budget_snapshot,created_by_transition_key,projection_version,created_at
                     ) VALUES ($1,$2,$3,'retry','scheduled',$4,$5,$6,$7,$8,$9,0,$10)",
                )
                .bind(command.run_id().as_str())
                .bind(retry_timer.as_str())
                .bind(command.activation_id().as_str())
                .bind(*retry_at)
                .bind(attempt_i32(authority.fence().attempt_no())?)
                .bind(i64_from_u64(authority.fence().lease_epoch().get())?)
                .bind(authority.fencing_token())
                .bind(remaining)
                .bind(transition_key.as_str())
                .bind(now)
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
                Some(activation_adapter::retry_schedule_authority(
                    command.run_id().clone(),
                    command.activation_id().clone(),
                    authority.fence(),
                    retry_timer,
                    *retry_at,
                    u32::try_from(remaining).map_err(|_| RepositoryError::invalid_data())?,
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
    let (output_payload, output_artifact, output_hash) = output_storage
        .as_ref()
        .map(|(p, a, h)| (p.as_deref(), a.as_deref(), Some(h.as_str())))
        .unwrap_or((None, None, None));
    sqlx::query(
        "UPDATE node_attempts SET lifecycle=$1,output_payload_id=$2,output_artifact_id=$3,
            output_value_hash=$4,failure_code=$5,completion_transition_key=$6,
            terminal_event_id=$7,projection_version=$8,terminal_at=$9
         WHERE run_id=$10 AND activation_id=$11 AND attempt_no=$12",
    )
    .bind(attempt_next)
    .bind(output_payload)
    .bind(output_artifact)
    .bind(output_hash)
    .bind(failure_code.as_deref())
    .bind(transition_key.as_str())
    .bind(&attempt_event_id)
    .bind(i64_from_u64(next_attempt)?)
    .bind(now)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(attempt_i32(authority.fence().attempt_no())?)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let retry_timer = retry_authority
        .as_ref()
        .map(|value| value.timer_id().as_str());
    let terminal = retry_authority.is_none();
    let termination_reason = terminal
        .then_some("failure")
        .filter(|_| activation_next == "failed");
    sqlx::query(
        "UPDATE node_activations SET lifecycle=$1,current_attempt_no=NULL,current_lease_epoch=NULL,
            current_fencing_token=NULL,pending_retry_timer_id=$2,output_payload_id=$3,
            output_artifact_id=$4,output_value_hash=$5,winning_attempt_no=$6,
            termination_intent_reason=$7,termination_intent_transition_key=$8,
            termination_intent_at=$9,projection_version=$10,updated_at=$11,terminal_at=$12
         WHERE run_id=$13 AND activation_id=$14",
    )
    .bind(activation_next)
    .bind(retry_timer)
    .bind(output_payload)
    .bind(output_artifact)
    .bind(output_hash)
    .bind((activation_next == "succeeded").then_some(attempt_i32(authority.fence().attempt_no())?))
    .bind(termination_reason)
    .bind(termination_reason.map(|_| transition_key.as_str()))
    .bind(termination_reason.map(|_| now))
    .bind(i64_from_u64(next_activation)?)
    .bind(now)
    .bind(terminal.then_some(now))
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "UPDATE timers SET timer_state='cancelled',fired_at=$1,projection_version=projection_version+1
         WHERE run_id=$2 AND activation_id=$3 AND timer_kind='lease'
           AND expected_attempt_no=$4 AND expected_lease_epoch=$5
           AND expected_fencing_token=$6 AND timer_state='scheduled'",
    )
    .bind(now)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(attempt_i32(authority.fence().attempt_no())?)
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
        } if retry_authority.is_none() => ExecutionEventPayload::ActivationFailed {
            attempt_no: Some(authority.fence().attempt_no()),
            reason: *reason,
            failure: failure.clone(),
        },
        AttemptCompletion::Failed { .. } => ExecutionEventPayload::ActivationRetryWait {
            attempt_no: authority.fence().attempt_no(),
        },
    };
    let primary = model_data(PendingExecutionEvent::new(
        activation_context(
            command.run_id(),
            &scope_id,
            &node_id,
            command.activation_id(),
        )?,
        primary_payload,
    ))?;
    let receipt = insert_closed_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        next_activation,
        primary,
    )
    .await?;
    let companion = model_data(PendingExecutionEvent::new(
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
        next_activation,
        companion,
    )
    .await?;
    if companion_receipt.event_id() != attempt_event_id {
        return Err(RepositoryError::invalid_data());
    }
    let checkpoint_event_id = receipt.event_id().to_owned();
    let scope = model_data(insight_engine::ScopeInstanceId::new(scope_id))?;
    let result = match command.completion() {
        AttemptCompletion::Succeeded { output } => {
            let terminal = activation_adapter::committed_terminal_activation_authority(
                receipt,
                command.run_id().clone(),
                scope,
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
                    scope,
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
    repository: &PostgresDurableRepository,
    transition_key: TransitionKey,
    command: RegisterWaitCommand,
) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
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
        return Ok(outcome);
    }
    lock_run_for_event_write(&mut transaction, command.run_id()).await?;
    let row = sqlx::query(
        "SELECT scope_instance_id,node_id,lifecycle,execution_kind,projection_version
         FROM node_activations WHERE run_id=$1 AND activation_id=$2 FOR UPDATE",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::activation_not_found)?;
    if let Some(outcome) = replay_wait_receipt(
        &mut transaction,
        &transition_key,
        intent_hash.as_str(),
        &command,
    )
    .await?
    {
        return Ok(outcome);
    }
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
        return Ok(TransitionOutcome::StateConflict);
    }
    let next = command
        .expected_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    sqlx::query(
        "UPDATE node_activations SET lifecycle='waiting',wait_registration_transition_key=$1,
            projection_version=$2,updated_at=CURRENT_TIMESTAMP WHERE run_id=$3 AND activation_id=$4",
    )
    .bind(transition_key.as_str())
    .bind(i64_from_u64(next)?)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if let Some(deadline) = command.wait_deadline() {
        sqlx::query(
            "INSERT INTO timers (
                run_id,timer_id,activation_id,timer_kind,timer_state,deadline_at,
                created_by_transition_key,projection_version,created_at
             ) VALUES ($1,$2,$3,'wait','scheduled',$4,$5,0,CURRENT_TIMESTAMP)",
        )
        .bind(command.run_id().as_str())
        .bind(timer_id(&transition_key, "wait"))
        .bind(command.activation_id().as_str())
        .bind(*deadline)
        .bind(transition_key.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
    }
    let event = model_data(PendingExecutionEvent::new(
        activation_context(
            command.run_id(),
            &row.try_get::<String, _>("scope_instance_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            &row.try_get::<String, _>("node_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            command.activation_id(),
        )?,
        ExecutionEventPayload::ActivationWaiting,
    ))?;
    let receipt = insert_closed_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        next,
        event,
    )
    .await?;
    if let Some(deadline) = command.wait_deadline() {
        let scope = model_data(insight_engine::ScopeInstanceId::new(
            row.try_get::<String, _>("scope_instance_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let node = model_data(insight_engine::NodeId::new(
            row.try_get::<String, _>("node_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let companion = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone())
                .for_activation(scope, node, command.activation_id().clone())
                .caused_by(model_data(ExecutionEventId::parse(
                    receipt.event_id().to_owned(),
                ))?),
            ExecutionEventPayload::TimerScheduled {
                timer_id: model_data(TimerId::new(timer_id(&transition_key, "wait")))?,
                fire_at: *deadline,
            },
        ))?;
        insert_companion_event(
            &mut transaction,
            command.run_id(),
            &transition_key,
            receipt.event_id(),
            "wait_timer_scheduled",
            next,
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
    transaction: &mut Transaction<'_, Postgres>,
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
            AND e.transition_key=$1
         WHERE a.run_id=$2 AND a.activation_id=$3",
    )
    .bind(transition_key.as_str())
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    decode_execution_event_schema_row(&row)?;
    let stored = serde_json::from_value::<ExecutionEventPayload>(
        row.try_get::<Value, _>("safe_payload")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if stored != ExecutionEventPayload::ActivationWaiting {
        return Err(RepositoryError::invalid_data());
    }
    if let Some(deadline) = command.wait_deadline() {
        let timer = sqlx::query(
            "SELECT timer_id,deadline_at FROM timers
             WHERE run_id=$1 AND created_by_transition_key=$2 AND timer_kind='wait'",
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
        if timer
            .try_get::<DateTime<Utc>, _>("deadline_at")
            .map_err(|_| RepositoryError::invalid_data())?
            != *deadline
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

async fn timer_schedule_replay(
    transaction: &mut Transaction<'_, Postgres>,
    command: &ScheduleActivationTimerCommand,
    transition_key: &TransitionKey,
) -> Result<TimerId, RepositoryError> {
    let id = sqlx::query_scalar::<_, String>(
        "SELECT timer_id FROM timers WHERE run_id=$1 AND created_by_transition_key=$2",
    )
    .bind(command.run_id().as_str())
    .bind(transition_key.as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    model_data(TimerId::new(id))
}

async fn schedule_activation_timer(
    repository: &PostgresDurableRepository,
    transition_key: TransitionKey,
    command: ScheduleActivationTimerCommand,
) -> Result<TransitionOutcome<TimerId>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
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
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: timer_schedule_replay(&mut transaction, &command, &transition_key)
                    .await?,
            });
        }
        Replay::Vacant => {}
    }
    lock_run_for_event_write(&mut transaction, command.run_id()).await?;
    let row = sqlx::query(
        "SELECT scope_instance_id,node_id,projection_version,lifecycle
         FROM node_activations WHERE run_id=$1 AND activation_id=$2 FOR UPDATE",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::activation_not_found)?;
    match load_replay(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
    )
    .await?
    {
        Replay::Exact(_) => {
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: timer_schedule_replay(&mut transaction, &command, &transition_key)
                    .await?,
            });
        }
        Replay::Vacant => {}
    }
    let lifecycle = parse_activation_lifecycle(
        &row.try_get::<String, _>("lifecycle")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    if lifecycle.is_terminal()
        || (command.kind() == ActivationTimerKind::Wait
            && lifecycle != ActivationLifecycle::Waiting)
    {
        return Ok(TransitionOutcome::StateConflict);
    }
    let id = model_data(TimerId::new(timer_id(
        &transition_key,
        activation_adapter::activation_timer_kind_str(command.kind()),
    )))?;
    sqlx::query(
        "INSERT INTO timers (
            run_id,timer_id,activation_id,timer_kind,timer_state,deadline_at,
            created_by_transition_key,projection_version,created_at
         ) VALUES ($1,$2,$3,$4,'scheduled',$5,$6,0,CURRENT_TIMESTAMP)",
    )
    .bind(command.run_id().as_str())
    .bind(id.as_str())
    .bind(command.activation_id().as_str())
    .bind(activation_adapter::activation_timer_kind_str(
        command.kind(),
    ))
    .bind(*command.deadline())
    .bind(transition_key.as_str())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let event = model_data(PendingExecutionEvent::new(
        activation_context(
            command.run_id(),
            &row.try_get::<String, _>("scope_instance_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            &row.try_get::<String, _>("node_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            command.activation_id(),
        )?,
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
    repository: &PostgresDurableRepository,
    command: ReceiveSignalCommand,
) -> Result<TransitionOutcome<SignalReceipt>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let advisory = format!("{}:{}", command.run_id().as_str(), command.message_id());
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(advisory)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
    if let Some(row) = sqlx::query(
        "SELECT signal_id,intent_hash,payload_id FROM signals_inbox
         WHERE run_id=$1 AND message_id=$2 FOR UPDATE",
    )
    .bind(command.run_id().as_str())
    .bind(command.message_id())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    {
        if row
            .try_get::<String, _>("intent_hash")
            .map_err(|_| RepositoryError::invalid_data())?
            != intent_hash.as_str()
        {
            return Ok(TransitionOutcome::StateConflict);
        }
        let payload = row
            .try_get::<String, _>("payload_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let value =
            stored_value_ref(&mut transaction, command.run_id(), Some(&payload), None).await?;
        let receipt = activation_adapter::signal_receipt(
            model_data(SignalId::new(
                row.try_get::<String, _>("signal_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            payload,
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
    // Every transition that can append an execution event takes the Run row
    // write lock before any Activation row. `allocate_event_seq` updates this
    // same row; starting with FOR SHARE would require a lock upgrade and two
    // concurrent signals could deadlock while each retained its share lock.
    let migration_state = sqlx::query_scalar::<_, Option<String>>(
        "SELECT termination_intent_reason FROM workflow_runs WHERE run_id=$1 FOR UPDATE",
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
        "SELECT execution_kind FROM node_activations WHERE run_id=$1 AND activation_id=$2
           AND lifecycle NOT IN ('succeeded','failed','cancelled','timed_out') FOR UPDATE",
    )
    .bind(command.run_id().as_str())
    .bind(command.target_activation_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if target.as_deref() != Some("durable_wait") {
        return Ok(TransitionOutcome::StateConflict);
    }
    let (payload, hash) =
        insert_or_get_payload(&mut transaction, command.run_id(), command.value()).await?;
    sqlx::query(
        "INSERT INTO signals_inbox (
            run_id,signal_id,message_id,intent_hash,signal_name,target_activation_id,
            payload_id,signal_state,received_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,'pending',CURRENT_TIMESTAMP)",
    )
    .bind(command.run_id().as_str())
    .bind(command.signal_id().as_str())
    .bind(command.message_id())
    .bind(intent_hash.as_str())
    .bind(command.signal_name())
    .bind(command.target_activation_id().as_str())
    .bind(&payload)
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let receipt = activation_adapter::signal_receipt(
        command.signal_id().clone(),
        payload,
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

async fn signal_replay_authority(
    transaction: &mut Transaction<'_, Postgres>,
    transition_key: &TransitionKey,
    command: &ResolveSignalCommand,
    replay: &super::CommitReceipt,
) -> Result<WaitResolutionAuthority, RepositoryError> {
    let row = sqlx::query(
        "SELECT a.projection_version,a.wait_registration_transition_key,
                a.scope_instance_id,a.node_id,s.payload_id,
                e.schema_version AS execution_event_schema_version,e.safe_payload
         FROM node_activations a JOIN signals_inbox s ON s.run_id=a.run_id
            AND s.target_activation_id=a.activation_id
         JOIN execution_events e ON e.run_id=a.run_id AND e.transition_key=s.consumed_by_transition_key
         WHERE a.run_id=$1 AND a.activation_id=$2 AND s.signal_id=$3
           AND s.consumed_by_transition_key=$4",
    )
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(command.signal_id().as_str())
    .bind(transition_key.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    decode_execution_event_schema_row(&row)?;
    let registration = model_data(TransitionKey::parse(
        row.try_get::<String, _>("wait_registration_transition_key")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let payload = row
        .try_get::<String, _>("payload_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let output = stored_value_ref(transaction, command.run_id(), Some(&payload), None).await?;
    let primary = serde_json::from_value::<ExecutionEventPayload>(
        row.try_get::<Value, _>("safe_payload")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if primary
        != (ExecutionEventPayload::ActivationSucceeded {
            attempt_no: None,
            output: Some(value_summary(&output)),
        })
    {
        return Err(RepositoryError::invalid_data());
    }
    let scope = model_data(insight_engine::ScopeInstanceId::new(
        row.try_get::<String, _>("scope_instance_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let companion = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone())
            .for_activation(
                scope.clone(),
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
        transaction,
        command.run_id(),
        transition_key,
        replay.event_id(),
        "signal_received",
        &companion,
    )
    .await?;
    activation_adapter::wait_resolution_authority(
        make_receipt(
            replay,
            u64_from_i64(
                row.try_get("projection_version")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
            None,
        ),
        command.run_id().clone(),
        scope,
        command.activation_id().clone(),
        registration,
        insight_engine::WaitResolutionSubject::Signal(command.signal_id().clone()),
        transition_key.clone(),
        output,
    )
}

async fn is_timer_late_replay(
    transaction: &mut Transaction<'_, Postgres>,
    command: &FireTimerCommand,
    event_id: &str,
) -> Result<bool, RepositoryError> {
    let row = sqlx::query(
        "SELECT e.schema_version AS execution_event_schema_version,
                e.kind,e.safe_payload,e.activation_id,m.activation_id AS timer_activation_id
         FROM execution_events e JOIN timers m ON m.run_id=e.run_id AND m.timer_id=$1
         WHERE e.run_id=$2 AND e.event_id=$3",
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
    let payload = serde_json::from_value::<ExecutionEventPayload>(
        row.try_get::<Value, _>("safe_payload")
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
    transaction: &mut Transaction<'_, Postgres>,
    command: &FireTimerCommand,
) -> Result<Option<bool>, RepositoryError> {
    let (transition_key, intent_hash) =
        wait_late_audit_identity("timer", command.run_id(), command.timer_id().as_str())?;
    sqlx::query("SELECT 1 FROM workflow_runs WHERE run_id=$1 FOR UPDATE")
        .bind(command.run_id().as_str())
        .fetch_one(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(transition_key.as_str())
        .fetch_one(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    let row = sqlx::query(
        "SELECT a.activation_id,a.scope_instance_id,a.node_id,a.projection_version,
                s.consumed_event_id AS winner_event_id
         FROM timers m JOIN node_activations a ON a.run_id=m.run_id
            AND a.activation_id=m.activation_id
         JOIN scheduler_wait_registrations w ON w.run_id=m.run_id
            AND w.activation_id=m.activation_id AND w.timer_id=m.timer_id
         JOIN signals_inbox s ON s.run_id=w.run_id AND s.signal_id=w.winner_signal_id
         WHERE m.run_id=$1 AND m.timer_id=$2 AND m.timer_kind='wait'
           AND m.timer_state='cancelled' AND w.winner_kind='signal'
           AND s.signal_state='consumed'
           AND m.deadline_at <= clock_timestamp()",
    )
    .bind(command.run_id().as_str())
    .bind(command.timer_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(None);
    };
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
            model_data(ActivationId::new(
                row.try_get::<String, _>("activation_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
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
    transaction: &mut Transaction<'_, Postgres>,
    command: &ResolveSignalCommand,
    event_id: &str,
) -> Result<bool, RepositoryError> {
    let row = sqlx::query(
        "SELECT schema_version AS execution_event_schema_version,
                kind,safe_payload,activation_id FROM execution_events
         WHERE run_id=$1 AND event_id=$2",
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
    let ExecutionEventPayload::SignalLate { signal_id, .. } =
        serde_json::from_value::<ExecutionEventPayload>(
            row.try_get::<Value, _>("safe_payload")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
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
    transaction: &mut Transaction<'_, Postgres>,
    command: &ResolveSignalCommand,
) -> Result<Option<bool>, RepositoryError> {
    let (transition_key, intent_hash) =
        wait_late_audit_identity("signal", command.run_id(), command.signal_id().as_str())?;
    sqlx::query("SELECT 1 FROM workflow_runs WHERE run_id=$1 FOR UPDATE")
        .bind(command.run_id().as_str())
        .fetch_one(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(transition_key.as_str())
        .fetch_one(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    let row = sqlx::query(
        "SELECT a.scope_instance_id,a.node_id,a.projection_version,s.payload_id,
                t.fired_event_id AS winner_event_id
         FROM node_activations a JOIN signals_inbox s ON s.run_id=a.run_id
            AND s.target_activation_id=a.activation_id
         JOIN scheduler_wait_registrations w ON w.run_id=a.run_id
            AND w.activation_id=a.activation_id AND w.signal_id=s.signal_id
         JOIN timers t ON t.run_id=w.run_id AND t.timer_id=w.winner_timer_id
         WHERE a.run_id=$1 AND a.activation_id=$2 AND s.signal_id=$3
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
    let payload_id = row
        .try_get::<String, _>("payload_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let output = stored_value_ref(transaction, command.run_id(), Some(&payload_id), None).await?;
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
    repository: &PostgresDurableRepository,
    limit: i64,
) -> Result<u64, RepositoryError> {
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
             AND m.deadline_at <= clock_timestamp()
             AND NOT EXISTS (
               SELECT 1 FROM execution_events e
               WHERE e.run_id=m.run_id AND e.kind='timer.late'
                 AND e.safe_payload->>'timer_id'=m.timer_id
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
                 AND e.safe_payload->>'signal_id'=s.signal_id
             )
         ) candidates
         ORDER BY ordered_at,run_id,loser_kind,loser_id
         LIMIT $1",
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
        let activation_id = model_data(ActivationId::new(
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

async fn resolve_wait_signal(
    repository: &PostgresDurableRepository,
    transition_key: TransitionKey,
    command: ResolveSignalCommand,
) -> Result<TransitionOutcome<WaitResolutionAuthority>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    sqlx::query("SELECT 1 FROM workflow_runs WHERE run_id=$1 FOR UPDATE")
        .bind(command.run_id().as_str())
        .fetch_one(&mut *transaction)
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
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: signal_replay_authority(
                    &mut transaction,
                    &transition_key,
                    &command,
                    &replay,
                )
                .await?,
            });
        }
        Replay::Vacant => {}
    }
    let row = sqlx::query(
        "SELECT a.scope_instance_id,a.node_id,a.lifecycle,a.execution_kind,a.projection_version,
                a.wait_registration_transition_key,s.payload_id,s.signal_state
         FROM node_activations a JOIN signals_inbox s ON s.run_id=a.run_id
            AND s.target_activation_id=a.activation_id
         WHERE a.run_id=$1 AND a.activation_id=$2 AND s.signal_id=$3 FOR UPDATE OF a,s",
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
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: signal_replay_authority(
                    &mut transaction,
                    &transition_key,
                    &command,
                    &replay,
                )
                .await?,
            });
        }
        Replay::Vacant => {}
    }
    let version = row
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
        || version != i64_from_u64(command.expected_activation_projection_version())?
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
    let payload = row
        .try_get::<String, _>("payload_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let output = stored_value_ref(&mut transaction, command.run_id(), Some(&payload), None).await?;
    let next = command
        .expected_activation_projection_version()
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let now = Utc::now();
    sqlx::query(
        "UPDATE node_activations SET lifecycle='succeeded',output_payload_id=$1,output_value_hash=$2,
            projection_version=$3,updated_at=$4,terminal_at=$4 WHERE run_id=$5 AND activation_id=$6",
    )
    .bind(&payload)
    .bind(output.content_hash().as_str())
    .bind(i64_from_u64(next)?)
    .bind(now)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let scope = model_data(insight_engine::ScopeInstanceId::new(
        row.try_get::<String, _>("scope_instance_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let node = model_data(insight_engine::NodeId::new(
        row.try_get::<String, _>("node_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let primary = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
            scope.clone(),
            node.clone(),
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
        next,
        primary,
    )
    .await?;
    let companion = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone())
            .for_activation(scope.clone(), node, command.activation_id().clone())
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
        next,
        companion,
    )
    .await?;
    let signal_rows = sqlx::query(
        "UPDATE signals_inbox SET signal_state='consumed',consumed_by_transition_key=$1,
            consumed_event_id=$2,terminal_at=$3,projection_version=projection_version+1
         WHERE run_id=$4 AND signal_id=$5 AND target_activation_id=$6
           AND signal_state='pending'",
    )
    .bind(transition_key.as_str())
    .bind(signal_receipt.event_id())
    .bind(now)
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
    let wait_winner_rows = sqlx::query(
        "UPDATE scheduler_wait_registrations
         SET winner_kind='signal',winner_signal_id=$1,winner_timer_id=NULL,
             projection_version=projection_version+1,resolved_at=$2
         WHERE run_id=$3 AND activation_id=$4 AND signal_id=$5 AND winner_kind IS NULL",
    )
    .bind(command.signal_id().as_str())
    .bind(now)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .bind(command.signal_id().as_str())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if wait_winner_rows != 1
        && sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1 FROM scheduler_wait_registrations
                WHERE run_id=$1 AND activation_id=$2
             )",
        )
        .bind(command.run_id().as_str())
        .bind(command.activation_id().as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
    {
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let checkpoint_event_id = receipt.event_id().to_owned();
    sqlx::query(
        "UPDATE timers SET timer_state='cancelled',fired_at=$1,projection_version=projection_version+1
         WHERE run_id=$2 AND activation_id=$3 AND timer_kind='wait' AND timer_state='scheduled'",
    )
    .bind(now)
    .bind(command.run_id().as_str())
    .bind(command.activation_id().as_str())
    .execute(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let authority = activation_adapter::wait_resolution_authority(
        receipt,
        command.run_id().clone(),
        scope,
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
    ActivationTimeout {
        attempt_no: Option<AttemptNo>,
    },
}

fn timer_fence_from_pg_row(row: &PgRow) -> Result<LeaseFence, RepositoryError> {
    let attempt = row
        .try_get::<Option<i32>, _>("expected_attempt_no")
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

fn lease_terminal_reason(evidence: EffectEvidence) -> insight_engine::ActivationTerminationReason {
    match evidence {
        EffectEvidence::Unknown | EffectEvidence::Started | EffectEvidence::Committed => {
            insight_engine::ActivationTerminationReason::EffectOutcomeUnknown
        }
        EffectEvidence::NotStarted => insight_engine::ActivationTerminationReason::Failure,
    }
}

fn verify_primary_timer_payload(
    row: &PgRow,
    expected: &ExecutionEventPayload,
) -> Result<(), RepositoryError> {
    decode_execution_event_schema_row(row)?;
    let stored = serde_json::from_value::<ExecutionEventPayload>(
        row.try_get::<Value, _>("safe_payload")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if &stored != expected {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

async fn timer_replay_authority(
    transaction: &mut Transaction<'_, Postgres>,
    transition_key: &TransitionKey,
    command: &FireTimerCommand,
    replay: &super::CommitReceipt,
) -> Result<TimerFireAuthority, RepositoryError> {
    let row = sqlx::query(
        "SELECT m.timer_kind,m.deadline_at,m.fired_at,m.expected_attempt_no,
                m.expected_lease_epoch,m.retry_budget_snapshot,a.activation_id,
                a.scope_instance_id,a.node_id,a.lifecycle,a.projection_version,a.pending_retry_timer_id,
                a.wait_registration_transition_key,a.output_payload_id,a.output_artifact_id,
                e.schema_version AS execution_event_schema_version,e.safe_payload
         FROM timers m JOIN node_activations a ON a.run_id=m.run_id AND a.activation_id=m.activation_id
         JOIN execution_events e ON e.run_id=m.run_id AND e.transition_key=m.fired_by_transition_key
         WHERE m.run_id=$1 AND m.timer_id=$2 AND m.timer_state='fired'
           AND m.fired_by_transition_key=$3",
    )
    .bind(command.run_id().as_str())
    .bind(command.timer_id().as_str())
    .bind(transition_key.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    decode_execution_event_schema_row(&row)?;
    let activation_id = model_data(ActivationId::new(
        row.try_get::<String, _>("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let receipt = make_receipt(
        replay,
        u64_from_i64(
            row.try_get("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        None,
    );
    let deadline = row
        .try_get::<DateTime<Utc>, _>("deadline_at")
        .map_err(|_| RepositoryError::invalid_data())?;
    let observed = row
        .try_get::<DateTime<Utc>, _>("fired_at")
        .map_err(|_| RepositoryError::invalid_data())?;
    let scope = model_data(insight_engine::ScopeInstanceId::new(
        row.try_get::<String, _>("scope_instance_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let node = model_data(insight_engine::NodeId::new(
        row.try_get::<String, _>("node_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let cause = model_data(ExecutionEventId::parse(replay.event_id().to_owned()))?;
    let timer_event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone())
            .for_activation(scope.clone(), node.clone(), activation_id.clone())
            .caused_by(cause.clone()),
        ExecutionEventPayload::TimerFired {
            timer_id: command.timer_id().clone(),
        },
    ))?;
    verify_companion_event(
        transaction,
        command.run_id(),
        transition_key,
        replay.event_id(),
        "timer_fired",
        &timer_event,
    )
    .await?;
    match row
        .try_get::<String, _>("timer_kind")
        .map_err(|_| RepositoryError::invalid_data())?
        .as_str()
    {
        "lease" => {
            let fence = timer_fence_from_pg_row(&row)?;
            let evidence_text = sqlx::query_scalar::<_, String>(
                "SELECT effect_evidence FROM node_attempts
                 WHERE run_id=$1 AND activation_id=$2 AND attempt_no=$3",
            )
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .bind(attempt_i32(fence.attempt_no())?)
            .fetch_one(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            let evidence = parse_effect_evidence(&evidence_text)?;
            let attempt_event = model_data(PendingExecutionEvent::new(
                ExecutionEventContext::for_run(command.run_id().clone())
                    .for_attempt(
                        scope.clone(),
                        node.clone(),
                        activation_id.clone(),
                        fence.attempt_no(),
                    )
                    .caused_by(cause),
                ExecutionEventPayload::AttemptAbandoned {
                    effect_evidence: evidence,
                },
            ))?;
            verify_companion_event(
                transaction,
                command.run_id(),
                transition_key,
                replay.event_id(),
                "attempt_terminal",
                &attempt_event,
            )
            .await?;
            let retry = if let Some(id) = row
                .try_get::<Option<String>, _>("pending_retry_timer_id")
                .map_err(|_| RepositoryError::invalid_data())?
            {
                let timer = sqlx::query(
                    "SELECT deadline_at,retry_budget_snapshot FROM timers
                     WHERE run_id=$1 AND timer_id=$2 AND timer_kind='retry'",
                )
                .bind(command.run_id().as_str())
                .bind(&id)
                .fetch_one(&mut **transaction)
                .await
                .map_err(RepositoryError::storage)?;
                Some(activation_adapter::retry_schedule_authority(
                    command.run_id().clone(),
                    activation_id.clone(),
                    fence,
                    model_data(TimerId::new(id))?,
                    timer
                        .try_get("deadline_at")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    u32::try_from(
                        timer
                            .try_get::<i32, _>("retry_budget_snapshot")
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
                    reason: lease_terminal_reason(evidence),
                    failure: None,
                }
            };
            verify_primary_timer_payload(&row, &primary)?;
            let terminal = if retry.is_none() {
                Some(activation_adapter::committed_terminal_activation_authority(
                    receipt.clone(),
                    command.run_id().clone(),
                    scope,
                    activation_id.clone(),
                    super::CommittedTerminalActivationResult::Failed {
                        reason: lease_terminal_reason(evidence),
                        failure: None,
                    },
                )?)
            } else {
                None
            };
            Ok(TimerFireAuthority::LeaseExpired {
                receipt,
                fence,
                lease_timer_id: command.timer_id().clone(),
                lease_deadline: deadline,
                observed_at: observed,
                retry,
                terminal,
            })
        }
        "retry" => {
            verify_primary_timer_payload(&row, &ExecutionEventPayload::ActivationReady)?;
            Ok(TimerFireAuthority::RetryDue {
                receipt,
                previous_fence: timer_fence_from_pg_row(&row)?,
                retry_timer_id: command.timer_id().clone(),
                retry_at: deadline,
                observed_at: observed,
                remaining_attempt_budget: u32::try_from(
                    row.try_get::<i32, _>("retry_budget_snapshot")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
            })
        }
        "wait" => {
            let registration = model_data(TransitionKey::parse(
                row.try_get::<String, _>("wait_registration_transition_key")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            if row
                .try_get::<String, _>("lifecycle")
                .map_err(|_| RepositoryError::invalid_data())?
                == "timed_out"
            {
                verify_primary_timer_payload(&row, &ExecutionEventPayload::ActivationTimedOut)?;
                let terminal = activation_adapter::committed_terminal_activation_authority(
                    receipt.clone(),
                    command.run_id().clone(),
                    scope,
                    activation_id,
                    super::CommittedTerminalActivationResult::TimedOut,
                )?;
                return Ok(TimerFireAuthority::ActivationTimedOut { receipt, terminal });
            }
            let payload = row
                .try_get::<Option<String>, _>("output_payload_id")
                .map_err(|_| RepositoryError::invalid_data())?;
            let artifact = row
                .try_get::<Option<String>, _>("output_artifact_id")
                .map_err(|_| RepositoryError::invalid_data())?;
            let output = stored_value_ref(
                transaction,
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
            Ok(TimerFireAuthority::WaitResolved(
                activation_adapter::wait_resolution_authority(
                    receipt,
                    command.run_id().clone(),
                    scope,
                    activation_id,
                    registration,
                    insight_engine::WaitResolutionSubject::Timer(command.timer_id().clone()),
                    transition_key.clone(),
                    output,
                )?,
            ))
        }
        "activation_timeout" => {
            verify_primary_timer_payload(&row, &ExecutionEventPayload::ActivationTimedOut)?;
            if let Some(attempt) = sqlx::query_scalar::<_, i32>(
                "SELECT attempt_no FROM node_attempts WHERE run_id=$1 AND activation_id=$2
                   AND completion_transition_key=$3",
            )
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .bind(transition_key.as_str())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?
            {
                let attempt = model_data(AttemptNo::new(
                    u32::try_from(attempt).map_err(|_| RepositoryError::invalid_data())?,
                ))?;
                let companion = model_data(PendingExecutionEvent::new(
                    ExecutionEventContext::for_run(command.run_id().clone())
                        .for_attempt(scope.clone(), node, activation_id.clone(), attempt)
                        .caused_by(cause),
                    ExecutionEventPayload::AttemptTimedOut,
                ))?;
                verify_companion_event(
                    transaction,
                    command.run_id(),
                    transition_key,
                    replay.event_id(),
                    "attempt_terminal",
                    &companion,
                )
                .await?;
            }
            let terminal = activation_adapter::committed_terminal_activation_authority(
                receipt.clone(),
                command.run_id().clone(),
                scope,
                activation_id,
                super::CommittedTerminalActivationResult::TimedOut,
            )?;
            Ok(TimerFireAuthority::ActivationTimedOut { receipt, terminal })
        }
        _ => Err(RepositoryError::invalid_data()),
    }
}

async fn fire_timer(
    repository: &PostgresDurableRepository,
    transition_key: TransitionKey,
    command: FireTimerCommand,
) -> Result<TransitionOutcome<TimerFireAuthority>, RepositoryError> {
    let intent_hash = canonical_intent_hash(&command)?;
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    sqlx::query("SELECT 1 FROM workflow_runs WHERE run_id=$1 FOR UPDATE")
        .bind(command.run_id().as_str())
        .fetch_one(&mut *transaction)
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
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: timer_replay_authority(
                    &mut transaction,
                    &transition_key,
                    &command,
                    &replay,
                )
                .await?,
            });
        }
        Replay::Vacant => {}
    }
    let timer_activation = sqlx::query_scalar::<_, String>(
        "SELECT activation_id FROM timers WHERE run_id=$1 AND timer_id=$2",
    )
    .bind(command.run_id().as_str())
    .bind(command.timer_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(timer_activation) = timer_activation else {
        return Ok(TransitionOutcome::StateConflict);
    };
    // Keep a global lock order with signal resolution and other activation
    // transitions: activation first, then its timer. A single join with
    // `FOR UPDATE OF m,a` lets PostgreSQL choose the opposite tuple order and
    // can deadlock against resolve_wait_signal cancelling scheduled timers.
    sqlx::query("SELECT 1 FROM node_activations WHERE run_id=$1 AND activation_id=$2 FOR UPDATE")
        .bind(command.run_id().as_str())
        .bind(&timer_activation)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .ok_or_else(RepositoryError::activation_not_found)?;
    let row = sqlx::query(
        "SELECT m.timer_kind,m.timer_state,m.deadline_at,m.expected_attempt_no,
                m.expected_lease_epoch,m.expected_fencing_token,m.retry_budget_snapshot,
                a.activation_id,a.scope_instance_id,a.node_id,a.lifecycle,a.effect_idempotency,
                a.effect_evidence,a.current_attempt_no,a.current_lease_epoch,
                a.current_fencing_token,a.retry_budget_remaining,a.pending_retry_timer_id,
                a.wait_registration_transition_key,a.projection_version
         FROM timers m JOIN node_activations a ON a.run_id=m.run_id AND a.activation_id=m.activation_id
         WHERE m.run_id=$1 AND m.timer_id=$2 FOR UPDATE OF m",
    )
    .bind(command.run_id().as_str())
    .bind(command.timer_id().as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(TransitionOutcome::StateConflict);
    };
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
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: timer_replay_authority(
                    &mut transaction,
                    &transition_key,
                    &command,
                    &replay,
                )
                .await?,
            });
        }
        Replay::Vacant => {}
    }
    let deadline = row
        .try_get::<DateTime<Utc>, _>("deadline_at")
        .map_err(|_| RepositoryError::invalid_data())?;
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
    let activation_id = model_data(ActivationId::new(
        row.try_get::<String, _>("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let scope_text = row
        .try_get::<String, _>("scope_instance_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let node_text = row
        .try_get::<String, _>("node_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let lifecycle = row
        .try_get::<String, _>("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?;
    let current = u64_from_i64(
        row.try_get("projection_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let next = current
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let kind = row
        .try_get::<String, _>("timer_kind")
        .map_err(|_| RepositoryError::invalid_data())?;
    let (primary_payload, seed) = match kind.as_str() {
        "retry" => {
            let snapshot = row
                .try_get::<Option<i32>, _>("retry_budget_snapshot")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?;
            if lifecycle != "retry_wait"
                || row
                    .try_get::<Option<String>, _>("pending_retry_timer_id")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .as_deref()
                    != Some(command.timer_id().as_str())
                || row
                    .try_get::<i32, _>("retry_budget_remaining")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != snapshot
            {
                return Ok(TransitionOutcome::StateConflict);
            }
            let fence = timer_fence_from_pg_row(&row)?;
            sqlx::query(
                "UPDATE node_activations SET lifecycle='ready',pending_retry_timer_id=NULL,
                    projection_version=$1,updated_at=$2 WHERE run_id=$3 AND activation_id=$4",
            )
            .bind(i64_from_u64(next)?)
            .bind(observed)
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            (
                ExecutionEventPayload::ActivationReady,
                TimerAuthoritySeed::Retry {
                    fence,
                    remaining: u32::try_from(snapshot)
                        .map_err(|_| RepositoryError::invalid_data())?,
                },
            )
        }
        "wait" => {
            let registration = row
                .try_get::<Option<String>, _>("wait_registration_transition_key")
                .map_err(|_| RepositoryError::invalid_data())?;
            if lifecycle != "waiting" || registration.is_none() {
                return Ok(TransitionOutcome::StateConflict);
            }
            let signal_backed_wait = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(
                    SELECT 1 FROM scheduler_wait_registrations
                    WHERE run_id=$1 AND activation_id=$2 AND timer_id=$3
                      AND signal_id IS NOT NULL
                 )",
            )
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .bind(command.timer_id().as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if signal_backed_wait {
                sqlx::query(
                    "UPDATE node_activations SET lifecycle='timed_out',output_payload_id=NULL,
                        output_artifact_id=NULL,output_value_hash=NULL,
                        termination_intent_reason='timed_out',termination_intent_transition_key=$1,
                        termination_intent_at=$2,projection_version=$3,updated_at=$2,terminal_at=$2
                     WHERE run_id=$4 AND activation_id=$5",
                )
                .bind(transition_key.as_str())
                .bind(observed)
                .bind(i64_from_u64(next)?)
                .bind(command.run_id().as_str())
                .bind(activation_id.as_str())
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
                sqlx::query(
                    "UPDATE timers SET timer_state='cancelled',fired_at=$1,
                        projection_version=projection_version+1
                     WHERE run_id=$2 AND activation_id=$3 AND timer_kind='wait' AND timer_id<>$4
                       AND timer_state='scheduled'",
                )
                .bind(observed)
                .bind(command.run_id().as_str())
                .bind(activation_id.as_str())
                .bind(command.timer_id().as_str())
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
                (
                    ExecutionEventPayload::ActivationTimedOut,
                    TimerAuthoritySeed::ActivationTimeout { attempt_no: None },
                )
            } else {
                let output = model_data(ValueRef::inline(Value::Null))?;
                let (payload, _, hash) =
                    persist_value_ref(&mut transaction, command.run_id(), &output).await?;
                let payload = payload.ok_or_else(RepositoryError::invalid_data)?;
                sqlx::query(
                    "UPDATE node_activations SET lifecycle='succeeded',output_payload_id=$1,
                    output_value_hash=$2,projection_version=$3,updated_at=$4,terminal_at=$4
                 WHERE run_id=$5 AND activation_id=$6",
                )
                .bind(payload)
                .bind(hash)
                .bind(i64_from_u64(next)?)
                .bind(observed)
                .bind(command.run_id().as_str())
                .bind(activation_id.as_str())
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
                sqlx::query(
                "UPDATE timers SET timer_state='cancelled',fired_at=$1,projection_version=projection_version+1
                 WHERE run_id=$2 AND activation_id=$3 AND timer_kind='wait' AND timer_id<>$4
                   AND timer_state='scheduled'",
            )
            .bind(observed)
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .bind(command.timer_id().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
                (
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
            }
        }
        "lease" => {
            let fence = timer_fence_from_pg_row(&row)?;
            if !matches!(lifecycle.as_str(), "leased" | "running")
                || row
                    .try_get::<Option<i32>, _>("current_attempt_no")
                    .map_err(|_| RepositoryError::invalid_data())?
                    != row
                        .try_get::<Option<i32>, _>("expected_attempt_no")
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
                return Ok(TransitionOutcome::StaleLease);
            }
            let evidence = parse_effect_evidence(
                &row.try_get::<String, _>("effect_evidence")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let lost = evidence.after_lease_loss();
            let retry_safe = row
                .try_get::<String, _>("effect_idempotency")
                .map_err(|_| RepositoryError::invalid_data())?
                == "idempotent"
                || lost == EffectEvidence::NotStarted;
            let remaining = row
                .try_get::<Option<i32>, _>("retry_budget_snapshot")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?;
            let retry = if retry_safe && remaining > 0 {
                let retry_at = command
                    .retry_at()
                    .ok_or_else(RepositoryError::invalid_data)?;
                if retry_at <= &observed {
                    return Ok(TransitionOutcome::StateConflict);
                }
                let id = model_data(TimerId::new(timer_id(&transition_key, "retry")))?;
                sqlx::query(
                    "INSERT INTO timers (
                        run_id,timer_id,activation_id,timer_kind,timer_state,deadline_at,
                        expected_attempt_no,expected_lease_epoch,expected_fencing_token,
                        retry_budget_snapshot,created_by_transition_key,projection_version,created_at
                     ) VALUES ($1,$2,$3,'retry','scheduled',$4,$5,$6,$7,$8,$9,0,$10)",
                )
                .bind(command.run_id().as_str())
                .bind(id.as_str())
                .bind(activation_id.as_str())
                .bind(*retry_at)
                .bind(attempt_i32(fence.attempt_no())?)
                .bind(i64_from_u64(fence.lease_epoch().get())?)
                .bind(row.try_get::<String, _>("expected_fencing_token").map_err(|_| RepositoryError::invalid_data())?)
                .bind(remaining)
                .bind(transition_key.as_str())
                .bind(observed)
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
                Some(activation_adapter::retry_schedule_authority(
                    command.run_id().clone(),
                    activation_id.clone(),
                    fence,
                    id,
                    *retry_at,
                    u32::try_from(remaining).map_err(|_| RepositoryError::invalid_data())?,
                ))
            } else {
                if command.retry_at().is_some() {
                    return Ok(TransitionOutcome::StateConflict);
                }
                None
            };
            let attempt_event_id = event_id(&companion_transition_key(
                &transition_key,
                "attempt_terminal",
            )?);
            sqlx::query(
                "UPDATE node_attempts SET lifecycle='abandoned',effect_evidence=$1,
                    completion_transition_key=$2,terminal_event_id=$3,
                    projection_version=projection_version+1,terminal_at=$4
                 WHERE run_id=$5 AND activation_id=$6 AND attempt_no=$7 AND lease_epoch=$8
                   AND fencing_token=$9 AND lifecycle IN ('leased','running')",
            )
            .bind(effect_evidence_str(lost))
            .bind(transition_key.as_str())
            .bind(attempt_event_id)
            .bind(observed)
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .bind(attempt_i32(fence.attempt_no())?)
            .bind(i64_from_u64(fence.lease_epoch().get())?)
            .bind(
                row.try_get::<String, _>("expected_fencing_token")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            let termination = if lost == EffectEvidence::Unknown {
                "effect_outcome_unknown"
            } else {
                "failure"
            };
            sqlx::query(
                "UPDATE node_activations SET lifecycle=$1,current_attempt_no=NULL,current_lease_epoch=NULL,
                    current_fencing_token=NULL,pending_retry_timer_id=$2,effect_evidence=$3,
                    termination_intent_reason=$4,termination_intent_transition_key=$5,
                    termination_intent_at=$6,projection_version=$7,updated_at=$8,terminal_at=$9
                 WHERE run_id=$10 AND activation_id=$11",
            )
            .bind(if retry.is_some() { "retry_wait" } else { "failed" })
            .bind(retry.as_ref().map(|value| value.timer_id().as_str()))
            .bind(effect_evidence_str(lost))
            .bind(retry.is_none().then_some(termination))
            .bind(retry.is_none().then_some(transition_key.as_str()))
            .bind(retry.is_none().then_some(observed))
            .bind(i64_from_u64(next)?)
            .bind(observed)
            .bind(retry.is_none().then_some(observed))
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            let primary = if retry.is_some() {
                ExecutionEventPayload::ActivationRetryWait {
                    attempt_no: fence.attempt_no(),
                }
            } else {
                ExecutionEventPayload::ActivationFailed {
                    attempt_no: Some(fence.attempt_no()),
                    reason: lease_terminal_reason(lost),
                    failure: None,
                }
            };
            (
                primary,
                TimerAuthoritySeed::Lease {
                    fence,
                    retry,
                    evidence: lost,
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
                .try_get::<Option<i32>, _>("current_attempt_no")
                .map_err(|_| RepositoryError::invalid_data())?;
            let active_epoch = row
                .try_get::<Option<i64>, _>("current_lease_epoch")
                .map_err(|_| RepositoryError::invalid_data())?;
            let active_token = row
                .try_get::<Option<String>, _>("current_fencing_token")
                .map_err(|_| RepositoryError::invalid_data())?;
            let attempt_no = active_attempt
                .map(|value| {
                    model_data(AttemptNo::new(
                        u32::try_from(value).map_err(|_| RepositoryError::invalid_data())?,
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
            if let (Some(attempt), Some(epoch), Some(token), Some(event_id)) = (
                active_attempt,
                active_epoch,
                active_token,
                attempt_event_id.as_ref(),
            ) {
                sqlx::query(
                    "UPDATE node_attempts SET lifecycle='timed_out',completion_transition_key=$1,
                        terminal_event_id=$2,projection_version=projection_version+1,terminal_at=$3
                     WHERE run_id=$4 AND activation_id=$5 AND attempt_no=$6 AND lease_epoch=$7
                       AND fencing_token=$8 AND lifecycle IN ('leased','running')",
                )
                .bind(transition_key.as_str())
                .bind(event_id)
                .bind(observed)
                .bind(command.run_id().as_str())
                .bind(activation_id.as_str())
                .bind(attempt)
                .bind(epoch)
                .bind(token)
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
            }
            sqlx::query(
                "UPDATE node_activations SET lifecycle='timed_out',current_attempt_no=NULL,
                    current_lease_epoch=NULL,current_fencing_token=NULL,pending_retry_timer_id=NULL,
                    termination_intent_reason='timed_out',termination_intent_transition_key=$1,
                    termination_intent_at=$2,projection_version=$3,updated_at=$2,terminal_at=$2
                 WHERE run_id=$4 AND activation_id=$5",
            )
            .bind(transition_key.as_str())
            .bind(observed)
            .bind(i64_from_u64(next)?)
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            sqlx::query(
                "UPDATE timers SET timer_state='cancelled',fired_at=$1,projection_version=projection_version+1
                 WHERE run_id=$2 AND activation_id=$3 AND timer_id<>$4 AND timer_state='scheduled'",
            )
            .bind(observed)
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
    let scope = model_data(insight_engine::ScopeInstanceId::new(scope_text))?;
    let node = model_data(insight_engine::NodeId::new(node_text))?;
    let primary = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone()).for_activation(
            scope.clone(),
            node.clone(),
            activation_id.clone(),
        ),
        primary_payload,
    ))?;
    let receipt = insert_closed_event(
        &mut transaction,
        command.run_id(),
        &transition_key,
        intent_hash.as_str(),
        next,
        primary,
    )
    .await?;
    if let TimerAuthoritySeed::Lease {
        fence, evidence, ..
    } = &seed
    {
        let companion = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone())
                .for_attempt(
                    scope.clone(),
                    node.clone(),
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
            next,
            companion,
        )
        .await?;
    }
    if let TimerAuthoritySeed::ActivationTimeout {
        attempt_no: Some(attempt_no),
    } = &seed
    {
        let companion = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone())
                .for_attempt(
                    scope.clone(),
                    node.clone(),
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
            next,
            companion,
        )
        .await?;
    }
    let timer_event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(command.run_id().clone())
            .for_activation(scope.clone(), node, activation_id.clone())
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
        next,
        timer_event,
    )
    .await?;
    let fired = sqlx::query(
        "UPDATE timers SET timer_state='fired',fired_by_transition_key=$1,fired_event_id=$2,
            projection_version=projection_version+1,fired_at=$3
         WHERE run_id=$4 AND timer_id=$5 AND timer_state='scheduled' AND deadline_at<=$3",
    )
    .bind(transition_key.as_str())
    .bind(timer_receipt.event_id())
    .bind(observed)
    .bind(command.run_id().as_str())
    .bind(command.timer_id().as_str())
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
    if kind == "wait" {
        let wait_winner_rows = sqlx::query(
            "UPDATE scheduler_wait_registrations
             SET winner_kind='timer',winner_signal_id=NULL,winner_timer_id=$1,
                 projection_version=projection_version+1,resolved_at=$2
             WHERE run_id=$3 AND activation_id=$4 AND timer_id=$5 AND winner_kind IS NULL",
        )
        .bind(command.timer_id().as_str())
        .bind(observed)
        .bind(command.run_id().as_str())
        .bind(activation_id.as_str())
        .bind(command.timer_id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if wait_winner_rows != 1
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(
                    SELECT 1 FROM scheduler_wait_registrations
                    WHERE run_id=$1 AND activation_id=$2
                 )",
            )
            .bind(command.run_id().as_str())
            .bind(activation_id.as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
        {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        // A signal may have committed its inbox receipt immediately before
        // this timeout won the activation/wait locks.  Terminalize that loser
        // in this same first-winner transaction; otherwise it would remain a
        // permanently pending recovery candidate after the wait is closed.
        sqlx::query(
            "UPDATE signals_inbox SET signal_state='rejected',
                consumed_by_transition_key=$1,consumed_event_id=$2,terminal_at=$3,
                projection_version=projection_version+1
             WHERE run_id=$4 AND target_activation_id=$5 AND signal_state='pending'",
        )
        .bind(transition_key.as_str())
        .bind(timer_receipt.event_id())
        .bind(observed)
        .bind(command.run_id().as_str())
        .bind(activation_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
    }
    let checkpoint_event_id = receipt.event_id().to_owned();
    let result = match seed {
        TimerAuthoritySeed::Lease {
            fence,
            retry,
            evidence,
        } => {
            let terminal = if retry.is_none() {
                Some(activation_adapter::committed_terminal_activation_authority(
                    receipt.clone(),
                    command.run_id().clone(),
                    scope.clone(),
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
            scope.clone(),
            activation_id,
            registration,
            insight_engine::WaitResolutionSubject::Timer(command.timer_id().clone()),
            transition_key,
            output,
        )?),
        TimerAuthoritySeed::ActivationTimeout { .. } => {
            let terminal = activation_adapter::committed_terminal_activation_authority(
                receipt.clone(),
                command.run_id().clone(),
                scope,
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

async fn claim_task_outbox(
    repository: &PostgresDurableRepository,
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
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let now = Utc::now();
    let expires = now
        .checked_add_signed(Duration::seconds(i64::from(claim_seconds)))
        .ok_or_else(RepositoryError::invalid_data)?;
    // Discover candidates without taking projection locks, then serialize all
    // affected Runs in deterministic order. Eligibility is rechecked while
    // locking each task row so a concurrent claimant cannot make a stale
    // discovery authoritative.
    let candidate_rows = sqlx::query(
        "SELECT run_id,task_id FROM task_outbox
         WHERE (task_state='pending' AND available_at<=$1)
            OR (task_state='claimed' AND claim_expires_at<=$1)
         ORDER BY available_at,run_id,task_id LIMIT $2",
    )
    .bind(now)
    .bind(i64::from(limit))
    .fetch_all(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let candidates = candidate_rows
        .into_iter()
        .map(|row| {
            Ok((
                model_data(RunId::new(
                    row.try_get::<String, _>("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                row.try_get::<String, _>("task_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
    let candidate_run_ids = candidates
        .iter()
        .map(|(run_id, _)| run_id)
        .collect::<Vec<_>>();
    lock_runs_for_event_write(&mut transaction, &candidate_run_ids).await?;

    let mut rows = Vec::with_capacity(candidates.len());
    for (run_id, task_id) in candidates {
        let envelope = sqlx::query_scalar::<_, Value>(
            "SELECT task_envelope FROM task_outbox
             WHERE run_id=$1 AND task_id=$2
               AND ((task_state='pending' AND available_at<=$3)
                 OR (task_state='claimed' AND claim_expires_at<=$3))
             FOR UPDATE SKIP LOCKED",
        )
        .bind(run_id.as_str())
        .bind(&task_id)
        .bind(now)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if let Some(envelope) = envelope {
            rows.push((run_id, task_id, envelope));
        }
    }

    let mut claims = Vec::with_capacity(rows.len());
    let mut checkpoint_tokens: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (run_id, task, envelope) in rows {
        let envelope = serde_json::from_value::<TaskEnvelope>(envelope)
            .map_err(|_| RepositoryError::invalid_data())?;
        if envelope.run_id() != &run_id {
            return Err(RepositoryError::invalid_data());
        }
        let token = format!("claim_{}", Uuid::new_v4().simple());
        sqlx::query(
            "UPDATE task_outbox SET task_state='claimed',claimed_by=$1,claim_token=$2,
                claim_expires_at=$3,publish_attempts=publish_attempts+1,
                projection_version=projection_version+1
             WHERE run_id=$4 AND task_id=$5",
        )
        .bind(claimant)
        .bind(&token)
        .bind(expires)
        .bind(run_id.as_str())
        .bind(&task)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        checkpoint_tokens
            .entry(run_id.as_str().to_owned())
            .or_default()
            .push(token.clone());
        claims.push(activation_adapter::task_claim(
            run_id,
            task,
            token,
            claimant.to_owned(),
            expires,
            envelope,
        ));
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
    repository: &PostgresDurableRepository,
    claim: &TaskClaim,
    publish: bool,
) -> Result<bool, RepositoryError> {
    let now = Utc::now();
    let mut transaction = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    lock_run_for_event_write(&mut transaction, claim.run_id()).await?;
    let version = if publish {
        sqlx::query_scalar::<_, i64>(
            "UPDATE task_outbox SET task_state='published',published_at=$1,
                projection_version=projection_version+1
             WHERE run_id=$2 AND task_id=$3 AND task_state='claimed' AND claimed_by=$4
               AND claim_token=$5 AND claim_expires_at>$1 RETURNING projection_version",
        )
        .bind(now)
        .bind(claim.run_id().as_str())
        .bind(claim.task_id())
        .bind(claim.claimant())
        .bind(claim.claim_token())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
    } else {
        sqlx::query_scalar::<_, i64>(
            "UPDATE task_outbox SET task_state='acked',acked_at=$1,
                projection_version=projection_version+1
             WHERE run_id=$2 AND task_id=$3 AND task_state='published' AND claimed_by=$4
               AND claim_token=$5 RETURNING projection_version",
        )
        .bind(now)
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
        "SELECT task_state FROM task_outbox WHERE run_id=$1 AND task_id=$2
           AND claimed_by=$3 AND claim_token=$4",
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
    pool: &PgPool,
    run_id: &RunId,
    activation_id: &ActivationId,
) -> Result<Option<ActivationProjection>, RepositoryError> {
    let row = sqlx::query(
        "SELECT lifecycle,projection_version,current_attempt_no,current_lease_epoch,
                current_fencing_token,retry_budget_remaining,pending_retry_timer_id,
                wait_registration_transition_key
         FROM node_activations WHERE run_id=$1 AND activation_id=$2",
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
        .try_get::<Option<i32>, _>("current_attempt_no")
        .map_err(|_| RepositoryError::invalid_data())?;
    let epoch = row
        .try_get::<Option<i64>, _>("current_lease_epoch")
        .map_err(|_| RepositoryError::invalid_data())?;
    let fence = match (attempt, epoch) {
        (Some(attempt), Some(epoch)) => Some(model_data(LeaseFence::new(
            model_data(AttemptNo::new(
                u32::try_from(attempt).map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            model_data(LeaseEpoch::new(u64_from_i64(epoch)?))?,
        ))?),
        (None, None) => None,
        _ => return Err(RepositoryError::invalid_data()),
    };
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
        fence,
        row.try_get("current_fencing_token")
            .map_err(|_| RepositoryError::invalid_data())?,
        u32::try_from(
            row.try_get::<i32, _>("retry_budget_remaining")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get::<Option<String>, _>("pending_retry_timer_id")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(TimerId::new)
            .transpose()
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get::<Option<String>, _>("wait_registration_transition_key")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(TransitionKey::parse)
            .transpose()
            .map_err(|_| RepositoryError::invalid_data())?,
    )))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sqlx::{postgres::PgPoolOptions, AssertSqlSafe};

    use insight_engine::{
        scheduler::LogicalOccurrence, ContentHash, DefinitionRevisionId, DeploymentRevisionId,
        EffectIdempotency, ExecutionKind, NodeId, ScopeInstanceId, WorkerCancellation,
        WorkerExecutionPolicy,
    };

    use super::*;
    use insight_durable::{
        CreateRunCommand, DurableRepository, PlanInstallOutcome, TaskDispatchSpec, VersionedPlan,
    };

    fn key(label: &str) -> TransitionKey {
        TransitionKey::derive("repository.activation.pg.test", &[label]).unwrap()
    }

    fn plan(label: &str) -> VersionedPlan {
        insight_durable::model::adapter::versioned_plan_for_test(
            format!("definition_{label}"),
            format!("agent_{label}"),
            "PostgreSQL activation contract",
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

    async fn create_run(
        repository: &PostgresDurableRepository,
        plan: &VersionedPlan,
        label: &str,
    ) -> RunId {
        let run_id = RunId::new(format!("run_{label}")).unwrap();
        repository
            .create_run(
                key(&format!("{label}.run.create")),
                CreateRunCommand::new(run_id.clone(), plan, json!({})).unwrap(),
            )
            .await
            .unwrap();
        run_id
    }

    async fn admit_and_ready(
        repository: &PostgresDurableRepository,
        run_id: &RunId,
        label: &str,
        kind: ExecutionKind,
    ) -> ActivationId {
        let activation_id = ActivationId::new(format!("activation_{label}")).unwrap();
        assert!(matches!(
            repository
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
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            repository
                .make_activation_ready(
                    key(&format!("{label}.ready")),
                    ActivationCasCommand::new(run_id.clone(), activation_id.clone(), 0),
                )
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        activation_id
    }

    #[tokio::test]
    async fn postgres_signal_replay_and_resolution_recompute_persisted_payload_authority() {
        let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
            return;
        };
        let schema = format!("activation_payload_v3_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .unwrap();
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let separator = if database_url.contains('?') { '&' } else { '?' };
        let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
        let repository = PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap();
        repository.initialize_schema().await.unwrap();
        let plan = plan("signal_payload_authority");
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = create_run(&repository, &plan, "signal_payload_authority").await;
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
            "SELECT payload_id FROM signals_inbox WHERE run_id=$1 AND signal_id=$2",
        )
        .bind(run_id.as_str())
        .bind(signal_id.as_str())
        .fetch_one(repository.test_pool())
        .await
        .unwrap();
        sqlx::query("UPDATE payloads SET inline_value=$1 WHERE run_id=$2 AND payload_id=$3")
            .bind(json!({"answer": 43}))
            .bind(run_id.as_str())
            .bind(&payload_id)
            .execute(repository.test_pool())
            .await
            .unwrap();
        assert_eq!(
            repository
                .receive_signal(receive.clone())
                .await
                .unwrap_err()
                .code(),
            insight_engine::repository::REPOSITORY_DATA_INVALID
        );
        sqlx::query("UPDATE payloads SET inline_value=$1 WHERE run_id=$2 AND payload_id=$3")
            .bind(json!({"answer": 42}))
            .bind(run_id.as_str())
            .bind(&payload_id)
            .execute(repository.test_pool())
            .await
            .unwrap();
        assert!(matches!(
            repository.receive_signal(receive).await.unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        sqlx::query("UPDATE payloads SET inline_value=$1 WHERE run_id=$2 AND payload_id=$3")
            .bind(json!({"answer": 43}))
            .bind(run_id.as_str())
            .bind(payload_id)
            .execute(repository.test_pool())
            .await
            .unwrap();
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
    async fn postgres_activation_contract_when_pg16_is_available() {
        let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
            return;
        };
        let schema = format!("activation_v3_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .unwrap();
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let separator = if database_url.contains('?') { '&' } else { '?' };
        let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
        let repository = PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap();
        repository.initialize_schema().await.unwrap();
        let plan = plan("activation_contract");
        assert_eq!(
            repository.install_versioned_plan(&plan).await.unwrap(),
            PlanInstallOutcome::Installed
        );

        // Two schedulers race the same Ready activation. The activation row lock
        // admits exactly one new fence and the loser observes Conflict.
        let run_id = create_run(&repository, &plan, "pg_worker").await;
        let activation_id = admit_and_ready(
            &repository,
            &run_id,
            "pg_worker",
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
        let lease_command = GrantAttemptLeaseCommand::new(
            run_id.clone(),
            activation_id.clone(),
            1,
            "worker-a",
            60,
            TaskDispatchSpec::new("contract-v1", None).unwrap(),
        )
        .unwrap();
        let lease_replay_command = lease_command.clone();
        let left_repository = repository.clone();
        let right_repository = repository.clone();
        let left_command = lease_command.clone();
        let right_command = lease_command;
        let left_key = key("pg_worker.lease.left");
        let right_key = key("pg_worker.lease.right");
        let (left, right) = tokio::join!(
            left_repository.grant_attempt_lease(left_key.clone(), left_command),
            right_repository.grant_attempt_lease(right_key.clone(), right_command),
        );
        let (lease, loser, winning_key) = match (left.unwrap(), right.unwrap()) {
            (TransitionOutcome::Committed { result }, other) => (result, other, left_key),
            (other, TransitionOutcome::Committed { result }) => (result, other, right_key),
            other => panic!("unexpected dual scheduler outcomes: {other:?}"),
        };
        assert_eq!(loser, TransitionOutcome::StateConflict);
        assert!(matches!(
            repository
                .grant_attempt_lease(winning_key, lease_replay_command)
                .await
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));

        // SKIP LOCKED claims are reclaimable after expiry, while the old token
        // cannot publish the reclaimed row.
        let first_claim = repository
            .claim_task_outbox("publisher-a", 30, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        sqlx::query(
            "UPDATE task_outbox SET claim_expires_at=CURRENT_TIMESTAMP-INTERVAL '1 second'
             WHERE run_id=$1 AND task_id=$2",
        )
        .bind(run_id.as_str())
        .bind(first_claim.task_id())
        .execute(repository.test_pool())
        .await
        .unwrap();
        let reclaimed = repository
            .claim_task_outbox("publisher-b", 30, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_ne!(first_claim.claim_token(), reclaimed.claim_token());
        assert!(!repository.mark_task_published(&first_claim).await.unwrap());
        assert!(repository.mark_task_published(&reclaimed).await.unwrap());
        assert!(repository.ack_task(&reclaimed).await.unwrap());

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
        let heartbeat = HeartbeatAttemptCommand::new(initial_fence.clone(), 120).unwrap();
        assert!(matches!(
            repository
                .heartbeat_attempt(heartbeat.clone())
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            repository
                .heartbeat_attempt(heartbeat.clone())
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
        let running_key = key("pg_worker.running");
        assert!(matches!(
            repository
                .mark_attempt_running(running_key.clone(), running.clone())
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            repository
                .mark_attempt_running(running_key, running)
                .await
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));

        // A rejected retry proposal rolls the whole transaction back: no timer,
        // event, or attempt terminal half-state survives.
        let completion_fence = FencedAttemptCommand::new(
            run_id.clone(),
            activation_id.clone(),
            lease.fence(),
            lease.fencing_token(),
            "worker-a",
            3,
            2,
        )
        .unwrap();
        let rejected_key = key("pg_worker.rejected_retry");
        let rejected = CompleteAttemptCommand::new(
            completion_fence.clone(),
            AttemptCompletion::Failed {
                reason: insight_engine::ActivationTerminationReason::Failure,
                failure: None,
                retry_at: Some(Utc::now() - Duration::seconds(1)),
            },
        );
        assert_eq!(
            repository
                .complete_attempt(rejected_key.clone(), rejected)
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );
        let half_state: (i64, i64, String) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM timers WHERE run_id=$1 AND timer_kind='retry'),
                (SELECT COUNT(*) FROM execution_events WHERE run_id=$1 AND transition_key=$2),
                (SELECT lifecycle FROM node_attempts WHERE run_id=$1 AND activation_id=$3 AND attempt_no=1)",
        )
        .bind(run_id.as_str())
        .bind(rejected_key.as_str())
        .bind(activation_id.as_str())
        .fetch_one(repository.test_pool())
        .await
        .unwrap();
        assert_eq!(half_state, (0, 0, "running".to_owned()));

        let complete_key = key("pg_worker.complete");
        let complete = CompleteAttemptCommand::new(
            completion_fence,
            AttemptCompletion::Succeeded {
                output: ValueRef::inline(json!({"ok": true})).unwrap(),
            },
        );
        assert!(matches!(
            repository
                .complete_attempt(complete_key.clone(), complete.clone())
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            repository
                .complete_attempt(complete_key, complete.clone())
                .await
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        assert_eq!(
            repository
                .complete_attempt(key("pg_worker.zombie_complete"), complete)
                .await
                .unwrap(),
            TransitionOutcome::StaleLease
        );
        assert!(matches!(
            repository.heartbeat_attempt(heartbeat).await.unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));

        // A signal and due wait timer serialize on the same activation row.
        let wait_run = create_run(&repository, &plan, "pg_wait").await;
        let wait_activation = admit_and_ready(
            &repository,
            &wait_run,
            "pg_wait",
            ExecutionKind::DurableWait,
        )
        .await;
        let registration_key = key("pg_wait.register");
        let registration_command = RegisterWaitCommand::new(
            wait_run.clone(),
            wait_activation.clone(),
            1,
            Some(Utc::now() - Duration::seconds(1)),
        );
        assert!(matches!(
            repository
                .register_wait(registration_key.clone(), registration_command.clone())
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            repository
                .register_wait(registration_key.clone(), registration_command)
                .await
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        let signal_id = SignalId::new("signal_pg_wait").unwrap();
        repository
            .receive_signal(
                ReceiveSignalCommand::new(
                    wait_run.clone(),
                    signal_id.clone(),
                    "message-pg-wait",
                    "resume",
                    wait_activation.clone(),
                    json!({"winner": "signal"}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let wait_timer = TimerId::new(timer_id(&registration_key, "wait")).unwrap();
        sqlx::query(
            "INSERT INTO scheduler_wait_registrations (
                run_id,wait_id,activation_id,node_id,occurrence_key,signal_name,signal_id,
                timer_id,due_at_ms,payload_type,winner_kind,winner_signal_id,winner_timer_id,
                projection_version,created_at,resolved_at
             ) SELECT a.run_id,$1,a.activation_id,a.node_id,$2,'resume',$3,$4,0,NULL,
                      NULL,NULL,NULL,0,CURRENT_TIMESTAMP,NULL
               FROM node_activations a WHERE a.run_id=$5 AND a.activation_id=$6",
        )
        .bind("wait_pg_wait")
        .bind(serde_json::to_value(LogicalOccurrence::entry()).unwrap())
        .bind(signal_id.as_str())
        .bind(wait_timer.as_str())
        .bind(wait_run.as_str())
        .bind(wait_activation.as_str())
        .execute(repository.test_pool())
        .await
        .unwrap();
        let signal_repository = repository.clone();
        let timer_repository = repository.clone();
        let signal_key = key("pg_wait.signal");
        let signal_command =
            ResolveSignalCommand::new(wait_run.clone(), wait_activation.clone(), signal_id, 2);
        let timer_key = key("pg_wait.timer");
        let timer_command = FireTimerCommand::new(wait_run.clone(), wait_timer, None);
        let (signal_result, timer_result) = tokio::join!(
            signal_repository.resolve_wait_signal(signal_key.clone(), signal_command.clone()),
            timer_repository.fire_timer(timer_key.clone(), timer_command.clone()),
        );
        let signal_won = matches!(signal_result.unwrap(), TransitionOutcome::Committed { .. });
        let timer_won = matches!(timer_result.unwrap(), TransitionOutcome::Committed { .. });
        let winners = [signal_won, timer_won]
            .into_iter()
            .filter(|won| *won)
            .count();
        assert_eq!(winners, 1);
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
                .load_activation(&wait_run, &wait_activation)
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
        // Exhausted lease loss and activation timeout both return committed
        // terminal authorities, not merely event receipts.
        let lease_run = create_run(&repository, &plan, "pg_lease_terminal").await;
        let lease_activation = admit_and_ready(
            &repository,
            &lease_run,
            "pg_lease_terminal",
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
        let exhausted_lease = match repository
            .grant_attempt_lease(
                key("pg_lease_terminal.grant"),
                GrantAttemptLeaseCommand::new(
                    lease_run.clone(),
                    lease_activation.clone(),
                    1,
                    "worker-expired",
                    60,
                    TaskDispatchSpec::new("contract-v1", None).unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap()
        {
            TransitionOutcome::Committed { result } => result,
            other => panic!("unexpected lease: {other:?}"),
        };
        sqlx::query(
            "UPDATE node_attempts SET lease_expires_at=CURRENT_TIMESTAMP-INTERVAL '1 second'
             WHERE run_id=$1 AND activation_id=$2 AND attempt_no=1",
        )
        .bind(lease_run.as_str())
        .bind(lease_activation.as_str())
        .execute(repository.test_pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE timers SET deadline_at=CURRENT_TIMESTAMP-INTERVAL '1 second'
             WHERE run_id=$1 AND timer_id=$2",
        )
        .bind(lease_run.as_str())
        .bind(exhausted_lease.lease_timer_id().as_str())
        .execute(repository.test_pool())
        .await
        .unwrap();
        match repository
            .fire_timer(
                key("pg_lease_terminal.fire"),
                FireTimerCommand::new(
                    lease_run.clone(),
                    exhausted_lease.lease_timer_id().clone(),
                    None,
                ),
            )
            .await
            .unwrap()
        {
            TransitionOutcome::Committed {
                result:
                    TimerFireAuthority::LeaseExpired {
                        retry: None,
                        terminal: Some(terminal),
                        ..
                    },
            } => assert_eq!(terminal.lifecycle(), ActivationLifecycle::Failed),
            other => panic!("unexpected exhausted lease result: {other:?}"),
        }

        let timeout_run = create_run(&repository, &plan, "pg_timeout").await;
        let timeout_activation = admit_and_ready(
            &repository,
            &timeout_run,
            "pg_timeout",
            ExecutionKind::SchedulerNative,
        )
        .await;
        let timeout_key = key("pg_timeout.schedule");
        let timeout_timer = match repository
            .schedule_activation_timer(
                timeout_key,
                ScheduleActivationTimerCommand::new(
                    timeout_run.clone(),
                    timeout_activation,
                    ActivationTimerKind::ActivationTimeout,
                    Utc::now() - Duration::seconds(1),
                ),
            )
            .await
            .unwrap()
        {
            TransitionOutcome::Committed { result } => result,
            other => panic!("unexpected timeout schedule: {other:?}"),
        };
        match repository
            .fire_timer(
                key("pg_timeout.fire"),
                FireTimerCommand::new(timeout_run, timeout_timer, None),
            )
            .await
            .unwrap()
        {
            TransitionOutcome::Committed {
                result: TimerFireAuthority::ActivationTimedOut { terminal, .. },
            } => assert_eq!(terminal.lifecycle(), ActivationLifecycle::TimedOut),
            other => panic!("unexpected timeout result: {other:?}"),
        }

        repository.pool.close().await;
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&admin)
            .await
            .unwrap();
        admin.close().await;
    }
}
