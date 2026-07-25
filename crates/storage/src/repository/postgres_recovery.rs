use super::RepositoryErrorExt as _;

#[cfg(test)]
use super::model::{
    PublicEventIntentAdapter, PublicationHeadAdapter as _, RunTransitionCommandAdapter as _,
};
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction};
use std::time::Duration;

use insight_durable::common::adapter::{
    self as common_contract_adapter, canonical_intent_hash, canonical_value,
    decode_execution_event_schema_version, durable_public_event_envelope, event_id, i64_from_u64,
    parse_admission_state, parse_run_lifecycle, payload_id, public_event_id, public_event_ordinal,
    scheduler_checkpoint_content_hash, u64_from_i64, validate_inline_payload,
};
use insight_durable::{
    activation::adapter::{
        parse_activation_lifecycle, parse_effect_evidence, parse_effect_idempotency,
    },
    control_repository::adapter as control_adapter,
    projection::adapter as projection_adapter,
    recovery_repository::adapter as recovery_adapter,
};

use insight_engine::{
    control::{ControlEmissionSlot, ControlFrame},
    decide_redrive_effect,
    scheduler::ReuseAdmissionContract,
    ActivationId, ActivationLifecycle, AdmissionState, ContentHash, ControlTokenProvenance,
    DefinitionRevisionId, DeploymentRevisionId, EffectId, ExecutionEventContext, ExecutionEventId,
    ExecutionEventPayload, ExecutionRevisionPin, Generation, LogicalOccurrence, MigrationReadiness,
    NodeId, PendingExecutionEvent, Plan, PlanIndex, PortId, ProjectionMutationKind,
    PublicErrorCode, PublicEventPayload, PublicFailureKind, PublicFailureSummary,
    RedriveEffectDecision, RunId, RunLifecycle, RunLineage, RunLineageKind, RuntimeValue,
    SchedulerCheckpointId, ScopeInstanceId, TransitionKey, TransitionOutcome,
};

use super::postgres::{
    allocate_event_seq, begin_write_transaction, insert_event, insert_or_get_payload,
};
use super::postgres_projection::{
    finalize_projection_checkpoints, verify_projection_checkpoint_batch,
};
use super::projection::{ProjectionSubject, ProjectionSubjectKind};
use super::{
    BeginMigrationCommand, ContinueAsNewCommand, CreateReuseCandidateCommand,
    DurableTaskExecutionRequest, FinalizeMigrationCommand, ForkRunCommand, MigrationIntentReceipt,
    MigrationMappingCompatibility, PostgresDurableRepository, RecoveryDurableRepository,
    RecoveryEventReceipt, RecoveryRevisionSpec, RecoveryRunReceipt, RedriveRunCommand,
    RepositoryError, ReuseCompatibility, SCHEDULER_CHECKPOINT_SCHEMA_VERSION,
};

fn model_data<T>(value: Result<T, insight_engine::ModelError>) -> Result<T, RepositoryError> {
    value.map_err(|_| RepositoryError::invalid_data())
}

async fn lock_transition(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    transition_key: &TransitionKey,
) -> Result<(), RepositoryError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))")
        .bind(run_id.as_str())
        .bind(transition_key.as_str())
        .execute(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
    Ok(())
}

#[derive(Debug, Clone)]
struct SourceRun {
    run_id: RunId,
    definition_id: String,
    revision: ExecutionRevisionPin,
    lifecycle: RunLifecycle,
    admission: AdmissionState,
    input: Value,
    generation: Generation,
    projection_version: u64,
    termination_intent_reason: Option<String>,
}

async fn ensure_redrive_effect_safety_postgres(
    tx: &mut Transaction<'_, Postgres>,
    source_run_id: &RunId,
) -> Result<(), RepositoryError> {
    let effects = sqlx::query(
        "SELECT lifecycle,effect_evidence,effect_idempotency FROM node_activations
         WHERE run_id=$1 AND execution_kind='worker'
         ORDER BY activation_id FOR UPDATE",
    )
    .bind(source_run_id.as_str())
    .fetch_all(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    for effect in effects {
        let lifecycle = parse_activation_lifecycle(
            &effect
                .try_get::<String, _>("lifecycle")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let evidence = parse_effect_evidence(
            &effect
                .try_get::<String, _>("effect_evidence")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let idempotency = parse_effect_idempotency(
            &effect
                .try_get::<String, _>("effect_idempotency")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        if matches!(
            decide_redrive_effect(
                lifecycle == ActivationLifecycle::Succeeded,
                evidence,
                idempotency
            ),
            RedriveEffectDecision::BlockOutcomeUnknown
                | RedriveEffectDecision::RequireNewForkEffectLineage
        ) {
            return Err(RepositoryError::redrive_requires_fork());
        }
    }
    Ok(())
}

async fn insert_redrive_effect_roots_postgres(
    tx: &mut Transaction<'_, Postgres>,
    source_run_id: &RunId,
    target_run_id: &RunId,
    created_by: &TransitionKey,
) -> Result<(), RepositoryError> {
    sqlx::query(
        "INSERT INTO recovery_effect_roots (
            run_id,source_run_id,effect_run_id,source_activation_id,effect_id,
            created_by_transition_key,projection_version,created_at
         )
         SELECT $1,a.run_id,a.run_id,a.activation_id,a.effect_id,$2,0,clock_timestamp()
         FROM node_activations a
         WHERE a.run_id=$3 AND a.execution_kind='worker' AND a.lifecycle<>'succeeded'",
    )
    .bind(target_run_id.as_str())
    .bind(created_by.as_str())
    .bind(source_run_id.as_str())
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

async fn authoritative_scheduler_checkpoint(
    tx: &mut Transaction<'_, Postgres>,
    source: &SourceRun,
    expected_hash: &ContentHash,
) -> Result<bool, RepositoryError> {
    // Recovery binds the scheduler checkpoint to the event's separately
    // closed projection ledger and provenance columns; it does not consume the
    // event payload/context. Schema and ProjectionLedgerBatch are both decoded
    // below before the checkpoint can be reused.
    let row = sqlx::query(
        "SELECT c.checkpoint_id,c.content_hash,c.checkpoint_kind,c.transition_key,
                c.intent_hash,c.event_id,c.checkpoint_schema_version,
                c.scheduler_projection_version,c.fact_payload,c.projection_version,
                e.schema_version AS execution_event_schema_version,e.projection_ledger_batch
         FROM scheduler_checkpoints c
         JOIN execution_events e
           ON e.run_id=c.run_id AND e.event_id=c.event_id
          AND e.transition_key=c.transition_key
         WHERE c.run_id=$1 AND c.content_hash=$2
           AND c.checkpoint_kind IN ('planned_action','task_started','task_completed','task_retry_scheduled')
           AND c.scheduler_projection_version<=$3
           AND e.projection_version_after=c.scheduler_projection_version
         FOR SHARE OF c,e",
    )
    .bind(source.run_id.as_str())
    .bind(expected_hash.as_str())
    .bind(i64_from_u64(source.projection_version)?)
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(false);
    };
    decode_execution_event_schema_version(i64::from(
        row.try_get::<i32, _>("execution_event_schema_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let schema = u32::try_from(
        row.try_get::<i32, _>("checkpoint_schema_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if schema != SCHEDULER_CHECKPOINT_SCHEMA_VERSION {
        return Ok(false);
    }
    let scheduler_version = u64_from_i64(
        row.try_get("scheduler_projection_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let fact_payload: Value = row
        .try_get("fact_payload")
        .map_err(|_| RepositoryError::invalid_data())?;
    let event_id_value: String = row
        .try_get("event_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let checkpoint_id: String = row
        .try_get("checkpoint_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let checkpoint_kind: String = row
        .try_get("checkpoint_kind")
        .map_err(|_| RepositoryError::invalid_data())?;
    let transition_key: String = row
        .try_get("transition_key")
        .map_err(|_| RepositoryError::invalid_data())?;
    let intent_hash: String = row
        .try_get("intent_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    let stored_hash: String = row
        .try_get("content_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    let computed = scheduler_checkpoint_content_hash(
        source.run_id.as_str(),
        &checkpoint_id,
        &checkpoint_kind,
        &transition_key,
        &intent_hash,
        &event_id_value,
        schema,
        scheduler_version,
        &fact_payload,
    )?;
    let stored = model_data(ContentHash::parse(stored_hash.clone()))?;
    if computed != stored || &stored != expected_hash {
        return Ok(false);
    }
    let projection_version = u64_from_i64(
        row.try_get("projection_version")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let Some(encoded_ledger_batch) = row
        .try_get::<Option<Value>, _>("projection_ledger_batch")
        .map_err(|_| RepositoryError::invalid_data())?
    else {
        return Ok(false);
    };
    let subject = ProjectionSubject::new(ProjectionSubjectKind::Scheduler, checkpoint_id.clone())?;
    let Some(checkpoint_record) =
        projection_adapter::projection_ledger_record_for(&encoded_ledger_batch, &subject)?
    else {
        return Ok(false);
    };
    let (_, checkpoint_projection_version, checkpoint_projection, _) =
        projection_adapter::checkpoint_record_parts(&checkpoint_record)?;
    if checkpoint_projection_version != projection_version {
        return Ok(false);
    }
    if checkpoint_projection
        .get("content_hash")
        .and_then(Value::as_str)
        != Some(stored_hash.as_str())
        || checkpoint_projection
            .get("checkpoint_kind")
            .and_then(Value::as_str)
            != Some(checkpoint_kind.as_str())
        || checkpoint_projection
            .get("transition_key")
            .and_then(Value::as_str)
            != Some(transition_key.as_str())
        || checkpoint_projection
            .get("intent_hash")
            .and_then(Value::as_str)
            != Some(intent_hash.as_str())
        || checkpoint_projection
            .get("event_id")
            .and_then(Value::as_str)
            != Some(event_id_value.as_str())
        || checkpoint_projection
            .get("checkpoint_schema_version")
            .and_then(Value::as_u64)
            != Some(u64::from(schema))
        || checkpoint_projection
            .get("scheduler_projection_version")
            .and_then(Value::as_u64)
            != Some(scheduler_version)
        || checkpoint_projection.get("fact_payload") != Some(&fact_payload)
    {
        return Ok(false);
    }
    Ok(true)
}

async fn checkpoint_projection_version_postgres(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    content_hash: &ContentHash,
) -> Result<Option<u64>, RepositoryError> {
    sqlx::query_scalar::<_, i64>(
        "SELECT scheduler_projection_version FROM scheduler_checkpoints
         WHERE run_id=$1 AND content_hash=$2",
    )
    .bind(run_id.as_str())
    .bind(content_hash.as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?
    .map(u64_from_i64)
    .transpose()
}

async fn revision_plan_postgres(
    tx: &mut Transaction<'_, Postgres>,
    revision: &RecoveryRevisionSpec,
) -> Result<Option<Plan>, RepositoryError> {
    let value = sqlx::query_scalar::<_, Value>(
        "SELECT r.canonical_plan FROM workflow_definition_revisions r
         JOIN deployment_revisions d
           ON d.definition_id=r.definition_id
          AND d.definition_revision_id=r.definition_revision_id
          AND d.plan_hash=r.plan_hash
         WHERE d.definition_id=$1 AND d.definition_revision_id=$2
           AND d.deployment_revision_id=$3 AND d.plan_hash=$4 AND d.binding_hash=$5",
    )
    .bind(revision.definition_id())
    .bind(revision.revision().definition_revision_id().as_str())
    .bind(revision.revision().deployment_revision_id().as_str())
    .bind(revision.revision().plan_hash().as_str())
    .bind(revision.revision().binding_hash().as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(value.and_then(|value| serde_json::from_value(value).ok()))
}

async fn fork_contract_compatible_postgres(
    tx: &mut Transaction<'_, Postgres>,
    source: &SourceRun,
    target_revision: &RecoveryRevisionSpec,
    target_input: &Value,
) -> Result<bool, RepositoryError> {
    let source_revision = source.revision_spec()?;
    if source.definition_id == target_revision.definition_id()
        && source.revision == *target_revision.revision()
        && target_input == &source.input
    {
        return Ok(true);
    }
    let Some(source_plan) = revision_plan_postgres(tx, &source_revision).await? else {
        return Ok(false);
    };
    let Some(target_plan) = revision_plan_postgres(tx, target_revision).await? else {
        return Ok(false);
    };
    let input =
        RuntimeValue::new(target_input.clone()).map_err(|_| RepositoryError::invalid_data())?;
    let Ok(target_run_type) = target_plan.metadata().input_contract().run_type() else {
        return Ok(false);
    };
    Ok(
        source_plan.metadata().input_contract() == target_plan.metadata().input_contract()
            && source_plan.metadata().output_type() == target_plan.metadata().output_type()
            && input.matches(&target_run_type),
    )
}

async fn derive_reuse_candidates_postgres(
    tx: &mut Transaction<'_, Postgres>,
    source: &SourceRun,
    target_run_id: &RunId,
    target_revision: &RecoveryRevisionSpec,
    source_checkpoint_hash: Option<&ContentHash>,
    mappings: Option<&[MigrationMappingCompatibility]>,
) -> Result<Option<Vec<CreateReuseCandidateCommand>>, RepositoryError> {
    if !revision_exists(tx, target_revision).await? {
        return Ok(None);
    }
    let ceiling = if let Some(hash) = source_checkpoint_hash {
        if !authoritative_scheduler_checkpoint(tx, source, hash).await? {
            return Ok(None);
        }
        let Some(version) =
            checkpoint_projection_version_postgres(tx, &source.run_id, hash).await?
        else {
            return Ok(None);
        };
        version
    } else {
        source.projection_version
    };
    let Some(target_plan) = revision_plan_postgres(tx, target_revision).await? else {
        return Ok(None);
    };
    let target_index = PlanIndex::new(&target_plan).map_err(|_| RepositoryError::invalid_data())?;
    let rows = sqlx::query(
        "SELECT a.activation_id,a.node_id,a.stable_activation_key,a.effect_id,
                a.output_payload_id,a.output_artifact_id,a.output_value_hash,
                t.task_envelope,t.created_by_transition_key
         FROM node_activations a
         JOIN task_outbox t
           ON t.run_id=a.run_id AND t.activation_id=a.activation_id
          AND t.attempt_no=a.winning_attempt_no
         WHERE a.run_id=$1 AND a.execution_kind='worker' AND a.lifecycle='succeeded'
           AND a.effect_evidence='committed' AND a.output_value_hash IS NOT NULL
           AND t.task_state='acked'
         ORDER BY a.activation_id LIMIT 10000",
    )
    .bind(source.run_id.as_str())
    .fetch_all(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let mut candidates = Vec::with_capacity(rows.len());
    for row in rows {
        let activation_id = model_data(ActivationId::new(
            row.try_get::<String, _>("activation_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let source_node_id = model_data(NodeId::new(
            row.try_get::<String, _>("node_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let mapping = mappings.and_then(|mappings| {
            mappings
                .iter()
                .find(|mapping| mapping.source().source_node_id() == &source_node_id)
        });
        if mappings.is_some() && mapping.is_none() {
            continue;
        }
        let target_node_id = mapping.map_or_else(
            || source_node_id.clone(),
            |mapping| mapping.source().target_node_id().clone(),
        );
        let Some(target_node) = target_index.node(&target_node_id) else {
            continue;
        };
        let stable_activation_key = row
            .try_get::<String, _>("stable_activation_key")
            .map_err(|_| RepositoryError::invalid_data())?;
        let occurrence: LogicalOccurrence = serde_json::from_str(&stable_activation_key)
            .map_err(|_| RepositoryError::invalid_data())?;
        let target_scope_instance_id = insight_engine::internal::scope_instance_for_occurrence(
            &target_index,
            target_run_id,
            target_node,
            &occurrence,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let envelope: DurableTaskExecutionRequest = serde_json::from_value(
            row.try_get::<Value, _>("task_envelope")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let request = envelope.request();
        if request.run_id() != &source.run_id
            || request.activation_id() != &activation_id
            || request.node_id() != &source_node_id
            || envelope.dispatch_scheduler_projection_version() > ceiling
        {
            return Err(RepositoryError::invalid_data());
        }
        let dispatch_hash = sqlx::query_scalar::<_, String>(
            "SELECT content_hash FROM scheduler_checkpoints
             WHERE run_id=$1 AND checkpoint_kind='planned_action' AND transition_key=$2
               AND scheduler_projection_version<=$3",
        )
        .bind(source.run_id.as_str())
        .bind(
            row.try_get::<String, _>("created_by_transition_key")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .bind(i64_from_u64(ceiling)?)
        .fetch_optional(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(dispatch_hash) = dispatch_hash else {
            continue;
        };
        let dispatch_hash = model_data(ContentHash::parse(dispatch_hash))?;
        if !authoritative_scheduler_checkpoint(tx, source, &dispatch_hash).await? {
            continue;
        }
        let completion_id =
            recovery_adapter::recovery_task_completion_checkpoint(request.task_id());
        let completion_hash = sqlx::query_scalar::<_, String>(
            "SELECT content_hash FROM scheduler_checkpoints
             WHERE run_id=$1 AND checkpoint_id=$2 AND checkpoint_kind='task_completed'
               AND scheduler_projection_version<=$3",
        )
        .bind(source.run_id.as_str())
        .bind(completion_id.as_str())
        .bind(i64_from_u64(ceiling)?)
        .fetch_optional(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(completion_hash) = completion_hash else {
            continue;
        };
        let completion_hash = model_data(ContentHash::parse(completion_hash))?;
        if !authoritative_scheduler_checkpoint(tx, source, &completion_hash).await? {
            continue;
        }
        let output_hash = model_data(ContentHash::parse(
            row.try_get::<String, _>("output_value_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let payload_id = row
            .try_get::<Option<String>, _>("output_payload_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let artifact_id = row
            .try_get::<Option<String>, _>("output_artifact_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        if !source_output_valid(
            tx,
            &source.run_id,
            payload_id.as_deref(),
            artifact_id.as_deref(),
            &output_hash,
        )
        .await?
        {
            continue;
        }
        let token = sqlx::query(
            "SELECT source_port_id,current_scope_instance_id,provenance_frames
             FROM control_tokens
             WHERE run_id=$1 AND source_activation_id=$2
             ORDER BY token_id LIMIT 1",
        )
        .bind(source.run_id.as_str())
        .bind(activation_id.as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(token) = token else {
            continue;
        };
        let frames: Vec<ControlFrame> = serde_json::from_value(
            token
                .try_get::<Value, _>("provenance_frames")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let provenance = model_data(ControlTokenProvenance::new(
            source.run_id.clone(),
            activation_id.clone(),
            model_data(PortId::new(
                token
                    .try_get::<String, _>("source_port_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            ControlEmissionSlot::ActivationOutput,
            model_data(ScopeInstanceId::new(
                token
                    .try_get::<String, _>("current_scope_instance_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            frames,
        ))?;
        let port_mapping = mapping.map(|mapping| mapping.target().port_mapping());
        let contract = ReuseAdmissionContract::from_task_parts(
            request.task_kind(),
            request.implementation(),
            request.descriptor_version(),
            request.worker_version(),
            request.effect_policy(),
            request.deployment_binding(),
            request.public_configuration(),
            request.secret_configuration(),
            request.inputs(),
            request.outputs(),
            port_mapping,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let effect_id = model_data(EffectId::new(
            row.try_get::<String, _>("effect_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        if request.effect_id() != &effect_id {
            return Err(RepositoryError::invalid_data());
        }
        let mut dependencies: std::collections::BTreeSet<ActivationId> = request
            .inputs()
            .iter()
            .flat_map(|input| input.source_activations().iter().cloned())
            .collect();
        dependencies.remove(&activation_id);
        candidates.push(
            CreateReuseCandidateCommand::new(
                target_run_id.clone(),
                recovery_adapter::recovery_candidate_id(
                    &source.run_id,
                    target_run_id,
                    &activation_id,
                )?,
                target_scope_instance_id,
                target_node_id,
                stable_activation_key,
                source.run_id.clone(),
                activation_id,
                provenance,
                target_revision.revision().definition_revision_id().clone(),
                target_revision.revision().deployment_revision_id().clone(),
                target_revision.revision().plan_hash().clone(),
                target_revision.revision().binding_hash().clone(),
                output_hash,
                effect_id,
                ReuseCompatibility::from_admission_contract(&contract),
            )?
            .with_source_data_dependencies(dependencies)?,
        );
    }
    Ok(Some(recovery_adapter::dependency_closed_reuse_candidates(
        candidates,
    )))
}

impl SourceRun {
    fn revision_spec(&self) -> Result<RecoveryRevisionSpec, RepositoryError> {
        RecoveryRevisionSpec::new(self.definition_id.clone(), self.revision.clone())
    }
}

struct TargetCreation {
    lineage: RunLineage,
    created: RecoveryEventReceipt,
    input_hash: ContentHash,
    candidate_count: u32,
}

#[async_trait::async_trait]
impl RecoveryDurableRepository for PostgresDurableRepository {
    async fn latest_authoritative_scheduler_checkpoint(
        &self,
        source_run_id: &RunId,
        expected_source_projection_version: u64,
    ) -> Result<Option<ContentHash>, RepositoryError> {
        let mut tx = begin_write_transaction(&self.pool).await?;
        // Keep the source identity present while selecting immutable checkpoint
        // rows. Selection itself is historical (as of the requested version)
        // so exact retries still work after the live source advances.
        let locked_version = sqlx::query_scalar::<_, i64>(
            "SELECT projection_version FROM workflow_runs WHERE run_id=$1 FOR SHARE",
        )
        .bind(source_run_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if locked_version.is_none() {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(None);
        }
        let Some(mut source) = load_source(&mut tx, source_run_id).await? else {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(None);
        };
        source.projection_version = expected_source_projection_version;
        let rows = sqlx::query(
            "SELECT content_hash FROM scheduler_checkpoints
             WHERE run_id=$1 AND checkpoint_kind IN ('planned_action','task_started','task_completed','task_retry_scheduled')
               AND scheduler_projection_version<=$2
             ORDER BY scheduler_projection_version DESC, checkpoint_id DESC
             LIMIT 10000
             FOR SHARE",
        )
        .bind(source_run_id.as_str())
        .bind(i64_from_u64(expected_source_projection_version)?)
        .fetch_all(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        for row in rows {
            let hash = model_data(ContentHash::parse(
                row.try_get::<String, _>("content_hash")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?;
            if authoritative_scheduler_checkpoint(&mut tx, &source, &hash).await? {
                tx.commit().await.map_err(RepositoryError::storage)?;
                return Ok(Some(hash));
            }
        }
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(None)
    }

    async fn authoritative_scheduler_checkpoint_by_id(
        &self,
        source_run_id: &RunId,
        expected_source_projection_version: u64,
        checkpoint_id: &SchedulerCheckpointId,
    ) -> Result<Option<ContentHash>, RepositoryError> {
        let mut tx = begin_write_transaction(&self.pool).await?;
        let Some(mut source) = load_source(&mut tx, source_run_id).await? else {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(None);
        };
        source.projection_version = expected_source_projection_version;
        let hash = sqlx::query_scalar::<_, String>(
            "SELECT content_hash FROM scheduler_checkpoints
             WHERE run_id=$1 AND checkpoint_id=$2 AND scheduler_projection_version<=$3",
        )
        .bind(source_run_id.as_str())
        .bind(checkpoint_id.as_str())
        .bind(i64_from_u64(expected_source_projection_version)?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let result = if let Some(hash) = hash {
            let hash = model_data(ContentHash::parse(hash))?;
            authoritative_scheduler_checkpoint(&mut tx, &source, &hash)
                .await?
                .then_some(hash)
        } else {
            None
        };
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(result)
    }

    async fn derive_compatible_reuse_candidates(
        &self,
        source_run_id: &RunId,
        target_run_id: &RunId,
        expected_source_projection_version: u64,
        target_revision: &RecoveryRevisionSpec,
        source_checkpoint_hash: Option<&ContentHash>,
    ) -> Result<Option<Vec<CreateReuseCandidateCommand>>, RepositoryError> {
        let mut tx = begin_write_transaction(&self.pool).await?;
        let Some(source) = load_source(&mut tx, source_run_id).await? else {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(None);
        };
        if source.projection_version != expected_source_projection_version {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(None);
        }
        let candidates = derive_reuse_candidates_postgres(
            &mut tx,
            &source,
            target_run_id,
            target_revision,
            source_checkpoint_hash,
            None,
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(candidates)
    }

    async fn derive_migration_reuse_candidates(
        &self,
        source_run_id: &RunId,
        target_run_id: &RunId,
        expected_source_projection_version: u64,
        target_revision: &RecoveryRevisionSpec,
        mappings: &[MigrationMappingCompatibility],
    ) -> Result<Option<Vec<CreateReuseCandidateCommand>>, RepositoryError> {
        let mut tx = begin_write_transaction(&self.pool).await?;
        let Some(source) = load_source(&mut tx, source_run_id).await? else {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(None);
        };
        if source.projection_version != expected_source_projection_version {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(None);
        }
        let candidates = derive_reuse_candidates_postgres(
            &mut tx,
            &source,
            target_run_id,
            target_revision,
            None,
            Some(mappings),
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(candidates)
    }

    async fn redrive_run(
        &self,
        transition_key: TransitionKey,
        command: RedriveRunCommand,
    ) -> Result<TransitionOutcome<RecoveryRunReceipt>, RepositoryError> {
        recovery_adapter::validate_redrive(&command)?;
        let intent_hash = canonical_intent_hash(&command)?;
        let mut tx = begin_write_transaction(&self.pool).await?;
        lock_transition(&mut tx, command.source_run_id(), &transition_key).await?;
        if let Some(authoritative) = replay_result::<RecoveryRunReceipt>(
            &mut tx,
            command.source_run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        let Some(source) = load_source(&mut tx, command.source_run_id()).await? else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if source.projection_version != command.expected_source_projection_version()
            || !source.lifecycle.is_terminal()
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        ensure_redrive_effect_safety_postgres(&mut tx, &source.run_id).await?;
        let target_revision = source.revision_spec()?;
        let Some(target) = create_target_run(
            &mut tx,
            &transition_key,
            intent_hash.as_str(),
            &source,
            command.target_run_id(),
            &target_revision,
            &source.input,
            command.target_timeout_ms(),
            RunLineageKind::Redrive,
            None,
            command.reuse_candidates(),
            &[],
        )
        .await?
        else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        let receipt = recovery_adapter::recovery_run_receipt(
            target.lineage,
            None,
            target.created.clone(),
            target.input_hash,
            target.candidate_count,
        );
        store_result(
            &mut tx,
            command.source_run_id(),
            &transition_key,
            intent_hash.as_str(),
            target.created.run_id(),
            target.created.event_id().as_str(),
            &receipt,
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn fork_run(
        &self,
        transition_key: TransitionKey,
        command: ForkRunCommand,
    ) -> Result<TransitionOutcome<RecoveryRunReceipt>, RepositoryError> {
        recovery_adapter::validate_fork(&command)?;
        let intent_hash = canonical_intent_hash(&command)?;
        let mut tx = begin_write_transaction(&self.pool).await?;
        lock_transition(&mut tx, command.source_run_id(), &transition_key).await?;
        if let Some(authoritative) = replay_result::<RecoveryRunReceipt>(
            &mut tx,
            command.source_run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        let Some(source) = load_source(&mut tx, command.source_run_id()).await? else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        let target_input = command.target_input().unwrap_or(&source.input);
        if source.projection_version != command.expected_source_projection_version()
            || !fork_contract_compatible_postgres(
                &mut tx,
                &source,
                command.target_revision(),
                target_input,
            )
            .await?
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        if !authoritative_scheduler_checkpoint(&mut tx, &source, command.source_checkpoint_hash())
            .await?
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let Some(target) = create_target_run(
            &mut tx,
            &transition_key,
            intent_hash.as_str(),
            &source,
            command.target_run_id(),
            command.target_revision(),
            target_input,
            command.target_timeout_ms(),
            RunLineageKind::Fork,
            Some(command.source_checkpoint_hash().clone()),
            command.reuse_candidates(),
            &[],
        )
        .await?
        else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        let receipt = recovery_adapter::recovery_run_receipt(
            target.lineage,
            None,
            target.created.clone(),
            target.input_hash,
            target.candidate_count,
        );
        store_result(
            &mut tx,
            command.source_run_id(),
            &transition_key,
            intent_hash.as_str(),
            target.created.run_id(),
            target.created.event_id().as_str(),
            &receipt,
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn load_pending_migration_intent(
        &self,
        source_run_id: &RunId,
        transition_key: &TransitionKey,
    ) -> Result<Option<BeginMigrationCommand>, RepositoryError> {
        let row = sqlx::query(
            "SELECT i.*,e.projection_version_after AS source_projection_version_after,
                    e.intent_hash AS begin_intent_hash,
                    e.schema_version AS execution_event_schema_version,
                    d.agent_id AS target_publication_agent_id
             FROM run_migration_intents i
             JOIN execution_events e
               ON e.run_id=i.run_id AND e.event_id=i.intent_event_id
             JOIN workflow_definitions d ON d.definition_id=i.target_definition_id
             WHERE i.run_id=$1 AND i.intent_transition_key=$2 AND i.intent_state='pending'",
        )
        .bind(source_run_id.as_str())
        .bind(transition_key.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            return Ok(None);
        };
        decode_execution_event_schema_version(i64::from(
            row.try_get::<i32, _>("execution_event_schema_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        ))?;
        let expected_source_projection_version = u64_from_i64(
            row.try_get::<i64, _>("source_projection_version_after")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?
        .checked_sub(1)
        .ok_or_else(RepositoryError::invalid_data)?;
        let mappings: Vec<MigrationMappingCompatibility> =
            serde_json::from_value(json_from_postgres(&row, "mapping_contracts")?)
                .map_err(|_| RepositoryError::invalid_data())?;
        let candidates: Vec<CreateReuseCandidateCommand> =
            serde_json::from_value(json_from_postgres(&row, "reuse_candidates")?)
                .map_err(|_| RepositoryError::invalid_data())?;
        let target_timeout_ms = u64_from_i64(
            row.try_get::<i64, _>("target_timeout_ms")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let command = BeginMigrationCommand::new(
            source_run_id.clone(),
            model_data(RunId::new(
                row.try_get::<String, _>("target_run_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            expected_source_projection_version,
            revision_from_intent(&row)?,
            json_from_postgres(&row, "target_input")?,
            mappings,
            candidates,
        )?
        .with_run_timeout(Duration::from_millis(target_timeout_ms))?;
        let stored_intent_hash = row
            .try_get::<String, _>("begin_intent_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        if canonical_intent_hash(&command)?.as_str() == stored_intent_hash {
            return Ok(Some(command));
        }
        let fenced = recovery_adapter::with_expected_target_publication_agent(
            command,
            row.try_get::<String, _>("target_publication_agent_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        if canonical_intent_hash(&fenced)?.as_str() == stored_intent_hash {
            return Ok(Some(fenced));
        }
        Err(RepositoryError::invalid_data())
    }

    async fn begin_migration(
        &self,
        transition_key: TransitionKey,
        command: BeginMigrationCommand,
    ) -> Result<TransitionOutcome<MigrationIntentReceipt>, RepositoryError> {
        recovery_adapter::validate_begin_migration(&command)?;
        let intent_hash = canonical_intent_hash(&command)?;
        let mut tx = begin_write_transaction(&self.pool).await?;
        lock_transition(&mut tx, command.source_run_id(), &transition_key).await?;
        if let Some(authoritative) = replay_result::<MigrationIntentReceipt>(
            &mut tx,
            command.source_run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        let Some(source) = load_source(&mut tx, command.source_run_id()).await? else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if source.projection_version != command.expected_source_projection_version()
            || source.termination_intent_reason.is_some()
            || !migration_ready(&mut tx, &source).await?
            || !migration_wait_rebuild_contracts_valid(&mut tx, &source.run_id, command.mappings())
                .await?
            || !revision_exists(&mut tx, command.target_revision()).await?
            || !expected_target_publication_matches(&mut tx, &command).await?
            || run_exists(&mut tx, command.target_run_id()).await?
            || RunLineage::new(
                source.run_id.clone(),
                command.target_run_id().clone(),
                RunLineageKind::Migrate,
                source.generation,
                source.revision.clone(),
                command.target_revision().revision().clone(),
                None,
            )
            .is_err()
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let next_version = source
            .projection_version
            .checked_add(1)
            .ok_or_else(RepositoryError::invalid_data)?;
        let updated = sqlx::query(
            "UPDATE workflow_runs SET termination_intent_reason='migrated',
                    termination_intent_transition_key=$1,termination_intent_at=CURRENT_TIMESTAMP,
                    projection_version=$2,scheduler_lease_epoch=scheduler_lease_epoch+1,
                    scheduler_lease_owner=NULL,scheduler_fencing_token=NULL,
                    scheduler_lease_expires_at=NULL,scheduler_heartbeat_at=NULL,
                    updated_at=CURRENT_TIMESTAMP
             WHERE run_id=$3 AND projection_version=$4 AND lifecycle IN ('active','waiting')
               AND admission_state='paused' AND termination_intent_reason IS NULL",
        )
        .bind(transition_key.as_str())
        .bind(i64_from_u64(next_version)?)
        .bind(source.run_id.as_str())
        .bind(i64_from_u64(source.projection_version)?)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if updated.rows_affected() != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        sqlx::query(
            "UPDATE scope_instances SET admission_state='draining',
                    projection_version=projection_version+1
             WHERE run_id=$1 AND lifecycle='active' AND admission_state='open'",
        )
        .bind(source.run_id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let seq = allocate_event_seq(&mut tx, &source.run_id).await?;
        let id = event_id(&transition_key);
        let event = model_data(PendingExecutionEvent::new(
            ExecutionEventContext::for_run(source.run_id.clone()),
            ExecutionEventPayload::ProjectionMutated {
                mutation: ProjectionMutationKind::RecoveryMigrationIntent,
            },
        ))?;
        insert_event(
            &mut tx,
            &source.run_id,
            seq,
            &id,
            &transition_key,
            intent_hash.as_str(),
            next_version,
            &event,
        )
        .await?;
        let (_, target_input_hash) = canonical_value(command.target_input())?;
        let mappings = serde_json::to_value(command.mappings())
            .map_err(|_| RepositoryError::canonicalization())?;
        let mapping_hash = recovery_adapter::migration_mapping_hash(command.mappings())?;
        let candidates = serde_json::to_value(command.reuse_candidates())
            .map_err(|_| RepositoryError::canonicalization())?;
        sqlx::query(
            "INSERT INTO run_migration_intents (
                run_id,target_run_id,target_definition_id,target_definition_revision_id,
                target_deployment_revision_id,target_plan_hash,target_binding_hash,target_input,
                target_input_hash,mapping_contracts,mapping_hash,reuse_candidates,target_timeout_ms,
                intent_transition_key,intent_event_id,intent_state,final_transition_key,
                projection_version,created_at,completed_at
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'pending',NULL,0,CURRENT_TIMESTAMP,NULL)",
        )
        .bind(source.run_id.as_str())
        .bind(command.target_run_id().as_str())
        .bind(command.target_revision().definition_id())
        .bind(
            command
                .target_revision()
                .revision()
                .definition_revision_id()
                .as_str(),
        )
        .bind(
            command
                .target_revision()
                .revision()
                .deployment_revision_id()
                .as_str(),
        )
        .bind(command.target_revision().revision().plan_hash().as_str())
        .bind(command.target_revision().revision().binding_hash().as_str())
        .bind(command.target_input())
        .bind(target_input_hash.as_str())
        .bind(mappings)
        .bind(mapping_hash.as_str())
        .bind(candidates)
        .bind(i64_from_u64(command.target_timeout_ms())?)
        .bind(transition_key.as_str())
        .bind(&id)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        finalize_projection_checkpoints(&mut tx, &source.run_id, &id).await?;
        let receipt = recovery_adapter::migration_intent_receipt(
            recovery_event(&source.run_id, seq, &id, next_version)?,
            command.target_run_id().clone(),
            command.target_revision().clone(),
            target_input_hash,
            mapping_hash,
        );
        store_result(
            &mut tx,
            &source.run_id,
            &transition_key,
            intent_hash.as_str(),
            &source.run_id,
            &id,
            &receipt,
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn finalize_migration(
        &self,
        transition_key: TransitionKey,
        command: FinalizeMigrationCommand,
    ) -> Result<TransitionOutcome<RecoveryRunReceipt>, RepositoryError> {
        recovery_adapter::validate_finalize_migration(&command)?;
        let intent_hash = canonical_intent_hash(&command)?;
        let mut tx = begin_write_transaction(&self.pool).await?;
        lock_transition(&mut tx, command.source_run_id(), &transition_key).await?;
        if let Some(authoritative) = replay_result::<RecoveryRunReceipt>(
            &mut tx,
            command.source_run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        let Some(source) = load_source(&mut tx, command.source_run_id()).await? else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if source.projection_version != command.expected_source_projection_version()
            || source.termination_intent_reason.as_deref() != Some("migrated")
            || !migration_ready(&mut tx, &source).await?
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let intent = sqlx::query(
            "SELECT * FROM run_migration_intents WHERE run_id=$1 AND target_run_id=$2
               AND intent_state='pending' AND intent_transition_key=$3",
        )
        .bind(source.run_id.as_str())
        .bind(command.target_run_id().as_str())
        .bind(command.migration_intent_transition_key().as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(intent) = intent else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        let target_revision = revision_from_intent(&intent)?;
        let target_input = json_from_postgres(&intent, "target_input")?;
        let mappings: Vec<MigrationMappingCompatibility> =
            serde_json::from_value(json_from_postgres(&intent, "mapping_contracts")?)
                .map_err(|_| RepositoryError::invalid_data())?;
        let candidates: Vec<CreateReuseCandidateCommand> =
            serde_json::from_value(json_from_postgres(&intent, "reuse_candidates")?)
                .map_err(|_| RepositoryError::invalid_data())?;
        let target_timeout_ms = u64_from_i64(
            intent
                .try_get::<i64, _>("target_timeout_ms")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let Some(target) = create_target_run(
            &mut tx,
            &transition_key,
            intent_hash.as_str(),
            &source,
            command.target_run_id(),
            &target_revision,
            &target_input,
            target_timeout_ms,
            RunLineageKind::Migrate,
            None,
            &candidates,
            &mappings,
        )
        .await?
        else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        sqlx::query(
            "UPDATE run_migration_intents SET intent_state='completed',final_transition_key=$1,
                    projection_version=projection_version+1,completed_at=CURRENT_TIMESTAMP
             WHERE run_id=$2 AND intent_state='pending'",
        )
        .bind(transition_key.as_str())
        .bind(source.run_id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(source_terminal) = terminalize_source(
            &mut tx,
            &transition_key,
            intent_hash.as_str(),
            &source,
            command.target_run_id(),
            "migrated",
            "MIGRATED",
        )
        .await?
        else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        finalize_projection_checkpoints(
            &mut tx,
            &source.run_id,
            source_terminal.event_id().as_str(),
        )
        .await?;
        let receipt = recovery_adapter::recovery_run_receipt(
            target.lineage,
            Some(source_terminal.clone()),
            target.created,
            target.input_hash,
            target.candidate_count,
        );
        store_result(
            &mut tx,
            &source.run_id,
            &transition_key,
            intent_hash.as_str(),
            &source.run_id,
            source_terminal.event_id().as_str(),
            &receipt,
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }

    async fn continue_as_new(
        &self,
        transition_key: TransitionKey,
        command: ContinueAsNewCommand,
    ) -> Result<TransitionOutcome<RecoveryRunReceipt>, RepositoryError> {
        recovery_adapter::validate_continue_as_new(&command)?;
        let intent_hash = canonical_intent_hash(&command)?;
        let mut tx = begin_write_transaction(&self.pool).await?;
        lock_transition(&mut tx, command.source_run_id(), &transition_key).await?;
        if let Some(authoritative) = replay_result::<RecoveryRunReceipt>(
            &mut tx,
            command.source_run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        let Some(source) = load_source(&mut tx, command.source_run_id()).await? else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if source.projection_version != command.expected_source_projection_version()
            || source.termination_intent_reason.is_some()
            || !migration_ready(&mut tx, &source).await?
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let target_revision = source.revision_spec()?;
        let Some(target) = create_target_run(
            &mut tx,
            &transition_key,
            intent_hash.as_str(),
            &source,
            command.target_run_id(),
            &target_revision,
            command.target_input(),
            command.target_timeout_ms(),
            RunLineageKind::ContinueAsNew,
            None,
            command.reuse_candidates(),
            &[],
        )
        .await?
        else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        let Some(source_terminal) = terminalize_source(
            &mut tx,
            &transition_key,
            intent_hash.as_str(),
            &source,
            command.target_run_id(),
            "continue_as_new",
            "CONTINUED_AS_NEW",
        )
        .await?
        else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        finalize_projection_checkpoints(
            &mut tx,
            &source.run_id,
            source_terminal.event_id().as_str(),
        )
        .await?;
        let receipt = recovery_adapter::recovery_run_receipt(
            target.lineage,
            Some(source_terminal.clone()),
            target.created,
            target.input_hash,
            target.candidate_count,
        );
        store_result(
            &mut tx,
            &source.run_id,
            &transition_key,
            intent_hash.as_str(),
            &source.run_id,
            source_terminal.event_id().as_str(),
            &receipt,
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result: receipt })
    }
}

async fn load_source(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<Option<SourceRun>, RepositoryError> {
    let row = sqlx::query(
        "SELECT r.definition_id,r.definition_revision_id,r.deployment_revision_id,
                r.plan_hash,r.binding_hash,r.lifecycle,r.admission_state,r.generation,
                r.projection_version,r.termination_intent_reason,
                r.input_payload_id AS expected_input_payload_id,
                p.payload_id AS input_payload_id,p.content_hash AS input_content_hash,
                p.canonical_bytes AS input_canonical_bytes,p.encoding AS input_encoding,
                p.inline_value AS input_inline_value,p.binary_value AS input_binary_value
         FROM workflow_runs r LEFT JOIN payloads p
           ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id
         WHERE r.run_id=$1 FOR UPDATE OF r",
    )
    .bind(run_id.as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    row.map(|row| {
        let expected_payload_id = row
            .try_get::<String, _>("expected_input_payload_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let stored_payload_id = row
            .try_get::<Option<String>, _>("input_payload_id")
            .map_err(|_| RepositoryError::invalid_data())?
            .ok_or_else(RepositoryError::invalid_data)?;
        if stored_payload_id != expected_payload_id {
            return Err(RepositoryError::invalid_data());
        }
        let input = validate_inline_payload(
            &stored_payload_id,
            &row.try_get::<Option<String>, _>("input_content_hash")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?,
            row.try_get::<Option<i64>, _>("input_canonical_bytes")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?,
            &row.try_get::<Option<String>, _>("input_encoding")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?,
            row.try_get::<Option<Value>, _>("input_inline_value")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?,
            None,
            row.try_get::<Option<Vec<u8>>, _>("input_binary_value")
                .map_err(|_| RepositoryError::invalid_data())?
                .is_none(),
        )?;
        let input = common_contract_adapter::validated_inline_payload_value(&input).clone();
        Ok(SourceRun {
            run_id: run_id.clone(),
            definition_id: row
                .try_get("definition_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            revision: ExecutionRevisionPin::new(
                model_data(DefinitionRevisionId::new(
                    row.try_get::<String, _>("definition_revision_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                model_data(DeploymentRevisionId::new(
                    row.try_get::<String, _>("deployment_revision_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                model_data(ContentHash::parse(
                    row.try_get::<String, _>("plan_hash")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
                model_data(ContentHash::parse(
                    row.try_get::<String, _>("binding_hash")
                        .map_err(|_| RepositoryError::invalid_data())?,
                ))?,
            ),
            lifecycle: parse_run_lifecycle(
                &row.try_get::<String, _>("lifecycle")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
            admission: parse_admission_state(
                &row.try_get::<String, _>("admission_state")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
            input,
            generation: model_data(Generation::new(
                u32::try_from(
                    row.try_get::<i64, _>("generation")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            projection_version: u64_from_i64(
                row.try_get::<i64, _>("projection_version")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
            termination_intent_reason: row
                .try_get("termination_intent_reason")
                .map_err(|_| RepositoryError::invalid_data())?,
        })
    })
    .transpose()
}

async fn run_exists(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<bool, RepositoryError> {
    Ok(
        sqlx::query_scalar::<_, i32>("SELECT 1 FROM workflow_runs WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_optional(&mut **tx)
            .await
            .map_err(RepositoryError::storage)?
            .is_some(),
    )
}

async fn revision_exists(
    tx: &mut Transaction<'_, Postgres>,
    revision: &RecoveryRevisionSpec,
) -> Result<bool, RepositoryError> {
    Ok(sqlx::query_scalar::<_, i32>(
        "SELECT 1 FROM deployment_revisions WHERE definition_id=$1
           AND definition_revision_id=$2 AND deployment_revision_id=$3
           AND plan_hash=$4 AND binding_hash=$5",
    )
    .bind(revision.definition_id())
    .bind(revision.revision().definition_revision_id().as_str())
    .bind(revision.revision().deployment_revision_id().as_str())
    .bind(revision.revision().plan_hash().as_str())
    .bind(revision.revision().binding_hash().as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?
    .is_some())
}

async fn expected_target_publication_matches(
    tx: &mut Transaction<'_, Postgres>,
    command: &BeginMigrationCommand,
) -> Result<bool, RepositoryError> {
    let Some(agent_id) = command.expected_target_publication_agent() else {
        return Ok(true);
    };
    let row = sqlx::query(
        "SELECT definition_id,definition_revision_id,deployment_revision_id
         FROM agent_publication_heads WHERE agent_id=$1 FOR SHARE",
    )
    .bind(agent_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(false);
    };
    Ok(row
        .try_get::<String, _>("definition_id")
        .map_err(|_| RepositoryError::invalid_data())?
        == command.target_revision().definition_id()
        && row
            .try_get::<String, _>("definition_revision_id")
            .map_err(|_| RepositoryError::invalid_data())?
            == command
                .target_revision()
                .revision()
                .definition_revision_id()
                .as_str()
        && row
            .try_get::<String, _>("deployment_revision_id")
            .map_err(|_| RepositoryError::invalid_data())?
            == command
                .target_revision()
                .revision()
                .deployment_revision_id()
                .as_str())
}

async fn migration_ready(
    tx: &mut Transaction<'_, Postgres>,
    source: &SourceRun,
) -> Result<bool, RepositoryError> {
    let live_attempts = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM node_attempts WHERE run_id=$1
           AND lifecycle IN ('created','leased','running')",
    )
    .bind(source.run_id.as_str())
    .fetch_one(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let unsettled = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(SUM(admitted_children-settled_children)::BIGINT,0)
         FROM scope_instances WHERE run_id=$1",
    )
    .bind(source.run_id.as_str())
    .fetch_one(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(MigrationReadiness::new(
        source.lifecycle,
        source.admission,
        u32::try_from(live_attempts).map_err(|_| RepositoryError::invalid_data())?,
        u32::try_from(unsettled).map_err(|_| RepositoryError::invalid_data())?,
    )
    .require_ready()
    .is_ok())
}

/// Fail closed on unsettled wait registrations before writing a migration
/// intent.  Rebuilding live signal/timer control state is only authorized when
/// the source node is mapped and both mapping contracts explicitly opt in.
async fn migration_wait_rebuild_contracts_valid(
    tx: &mut Transaction<'_, Postgres>,
    source_run_id: &RunId,
    mappings: &[MigrationMappingCompatibility],
) -> Result<bool, RepositoryError> {
    let waits = sqlx::query(
        "SELECT w.node_id AS wait_node_id,a.node_id AS activation_node_id,
                w.signal_id,w.timer_id
         FROM scheduler_wait_registrations w
         JOIN node_activations a
           ON a.run_id=w.run_id AND a.activation_id=w.activation_id
         WHERE w.run_id=$1 AND w.winner_kind IS NULL
         FOR SHARE OF w, a",
    )
    .bind(source_run_id.as_str())
    .fetch_all(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;

    for wait in waits {
        let wait_node_id = wait
            .try_get::<String, _>("wait_node_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let activation_node_id = wait
            .try_get::<String, _>("activation_node_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        if wait_node_id != activation_node_id {
            return Ok(false);
        }
        let Some(mapping) = mappings
            .iter()
            .find(|mapping| mapping.source().source_node_id().as_str() == wait_node_id)
        else {
            return Ok(false);
        };
        let has_signal = wait
            .try_get::<Option<String>, _>("signal_id")
            .map_err(|_| RepositoryError::invalid_data())?
            .is_some();
        let has_timer = wait
            .try_get::<Option<String>, _>("timer_id")
            .map_err(|_| RepositoryError::invalid_data())?
            .is_some();
        if (has_signal
            && (!mapping.source().signal_wait_rebuild_declared()
                || !mapping.target().signal_wait_rebuild_declared()))
            || (has_timer
                && (!mapping.source().timer_rebuild_declared()
                    || !mapping.target().timer_rebuild_declared()))
        {
            return Ok(false);
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn create_target_run(
    tx: &mut Transaction<'_, Postgres>,
    parent_transition_key: &TransitionKey,
    intent_hash: &str,
    source: &SourceRun,
    target_run_id: &RunId,
    target_revision: &RecoveryRevisionSpec,
    target_input: &Value,
    target_timeout_ms: u64,
    kind: RunLineageKind,
    checkpoint: Option<ContentHash>,
    candidates: &[CreateReuseCandidateCommand],
    mappings: &[MigrationMappingCompatibility],
) -> Result<Option<TargetCreation>, RepositoryError> {
    if run_exists(tx, target_run_id).await? {
        return Ok(None);
    }
    let Some(target_plan) = revision_plan_postgres(tx, target_revision).await? else {
        return Ok(None);
    };
    let target_run_type = target_plan
        .metadata()
        .input_contract()
        .run_type()
        .map_err(|_| RepositoryError::invalid_data())?;
    if !target_run_type
        .accepts_literal(target_input)
        .unwrap_or(false)
    {
        return Ok(None);
    }
    let lineage = match RunLineage::new(
        source.run_id.clone(),
        target_run_id.clone(),
        kind,
        source.generation,
        source.revision.clone(),
        target_revision.revision().clone(),
        checkpoint,
    ) {
        Ok(lineage) => lineage,
        Err(_) => return Ok(None),
    };
    let run_deadline_at = sqlx::query_scalar::<_, DateTime<Utc>>(
        "SELECT clock_timestamp() + ($1::double precision * interval '1 millisecond')",
    )
    .bind(target_timeout_ms as f64)
    .fetch_one(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some((input_payload_id, input_hash)) = insert_target_run(
        tx,
        source,
        target_run_id,
        target_revision,
        target_input,
        &lineage,
        run_deadline_at,
    )
    .await?
    else {
        return Ok(None);
    };
    let target_transition_key = model_data(TransitionKey::derive(
        "repository.recovery.target_created",
        &[parent_transition_key.as_str(), target_run_id.as_str()],
    ))?;
    let seq = allocate_event_seq(tx, target_run_id).await?;
    let id = event_id(&target_transition_key);
    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(target_run_id.clone()),
        ExecutionEventPayload::RunCreated {
            definition_revision_id: target_revision.revision().definition_revision_id().clone(),
            deployment_revision_id: target_revision.revision().deployment_revision_id().clone(),
            run_deadline_at: Some(run_deadline_at),
        },
    ))?;
    insert_event(
        tx,
        target_run_id,
        seq,
        &id,
        &target_transition_key,
        intent_hash,
        0,
        &event,
    )
    .await?;
    insert_lineage_and_revision_roots(
        tx,
        source,
        target_run_id,
        target_revision,
        &lineage,
        &target_transition_key,
    )
    .await?;
    if kind == RunLineageKind::Redrive {
        insert_redrive_effect_roots_postgres(
            tx,
            &source.run_id,
            target_run_id,
            &target_transition_key,
        )
        .await?;
    }
    finalize_projection_checkpoints(tx, target_run_id, &id).await?;
    for candidate in candidates {
        if !insert_reuse_candidate(
            tx,
            parent_transition_key,
            intent_hash,
            source,
            target_run_id,
            target_revision,
            candidate,
            kind,
            mappings,
        )
        .await?
        {
            return Ok(None);
        }
    }
    let _ = input_payload_id;
    Ok(Some(TargetCreation {
        lineage,
        created: recovery_event(target_run_id, seq, &id, 0)?,
        input_hash: model_data(ContentHash::parse(input_hash))?,
        candidate_count: u32::try_from(candidates.len())
            .map_err(|_| RepositoryError::invalid_data())?,
    }))
}

async fn insert_target_run(
    tx: &mut Transaction<'_, Postgres>,
    source: &SourceRun,
    target_run_id: &RunId,
    target_revision: &RecoveryRevisionSpec,
    target_input: &Value,
    lineage: &RunLineage,
    run_deadline_at: DateTime<Utc>,
) -> Result<Option<(String, String)>, RepositoryError> {
    let (_, input_hash) = canonical_value(target_input)?;
    let input_payload_id = payload_id(&input_hash);
    let database_lineage = match lineage.kind() {
        RunLineageKind::Redrive => "redrive",
        RunLineageKind::Fork => "fork",
        RunLineageKind::Migrate => "migrate",
        RunLineageKind::ContinueAsNew => "generation",
    };
    let inserted = sqlx::query(
        "INSERT INTO workflow_runs (
            run_id,definition_id,definition_revision_id,deployment_revision_id,plan_hash,
            binding_hash,request_id,attachment,lifecycle,admission_state,termination_intent_reason,
            termination_intent_transition_key,termination_intent_at,input_payload_id,
            output_payload_id,output_artifact_id,output_value_hash,error_code,terminal_event_id,
            terminal_public_event_id,parent_run_id,lineage_kind,generation,replacement_run_id,
            next_event_seq,projection_version,scheduler_lease_epoch,scheduler_lease_owner,
            scheduler_fencing_token,scheduler_lease_expires_at,scheduler_heartbeat_at,
            created_at,started_at,updated_at,terminal_at,deadline_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,'detached','created','open',NULL,NULL,NULL,$8,NULL,NULL,NULL,NULL,NULL,NULL,
                   $9,$10,$11,NULL,1,0,0,NULL,NULL,NULL,NULL,CURRENT_TIMESTAMP,NULL,CURRENT_TIMESTAMP,NULL,$12)
         ON CONFLICT DO NOTHING",
    )
    .bind(target_run_id.as_str())
    .bind(target_revision.definition_id())
    .bind(target_revision.revision().definition_revision_id().as_str())
    .bind(target_revision.revision().deployment_revision_id().as_str())
    .bind(target_revision.revision().plan_hash().as_str())
    .bind(target_revision.revision().binding_hash().as_str())
    .bind(target_run_id.as_str())
    .bind(&input_payload_id)
    .bind(source.run_id.as_str())
    .bind(database_lineage)
    .bind(i64::from(lineage.target_generation().get()))
    .bind(run_deadline_at)
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if inserted != 1 {
        return Ok(None);
    }
    let (stored_payload_id, stored_hash) =
        insert_or_get_payload(tx, target_run_id, target_input).await?;
    if stored_payload_id != input_payload_id || stored_hash != input_hash.as_str() {
        return Err(RepositoryError::invalid_data());
    }
    sqlx::query(
        "INSERT INTO scope_instances (
            run_id,scope_instance_id,parent_scope_instance_id,static_scope_id,
            stable_dynamic_key,scope_kind,is_root,lifecycle,admission_state,
            admitted_children,settled_children,projection_version,created_at,settled_at
         ) VALUES ($1,'scope_root',NULL,'root',NULL,'root',TRUE,'active','open',0,0,0,
                   CURRENT_TIMESTAMP,NULL)",
    )
    .bind(target_run_id.as_str())
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(Some((input_payload_id, stored_hash)))
}

async fn insert_lineage_and_revision_roots(
    tx: &mut Transaction<'_, Postgres>,
    source: &SourceRun,
    target_run_id: &RunId,
    target_revision: &RecoveryRevisionSpec,
    lineage: &RunLineage,
    created_by: &TransitionKey,
) -> Result<(), RepositoryError> {
    let lineage_kind = match lineage.kind() {
        RunLineageKind::Redrive => "redrive",
        RunLineageKind::Fork => "fork",
        RunLineageKind::Migrate => "migrate",
        RunLineageKind::ContinueAsNew => "continue_as_new",
    };
    sqlx::query(
        "INSERT INTO run_recovery_lineage (
            run_id,source_run_id,lineage_kind,source_generation,target_generation,
            source_definition_id,source_definition_revision_id,source_deployment_revision_id,
            source_plan_hash,source_binding_hash,target_definition_id,
            target_definition_revision_id,target_deployment_revision_id,target_plan_hash,
            target_binding_hash,source_checkpoint_hash,created_by_transition_key,
            projection_version,created_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,0,CURRENT_TIMESTAMP)",
    )
    .bind(target_run_id.as_str())
    .bind(source.run_id.as_str())
    .bind(lineage_kind)
    .bind(i64::from(lineage.source_generation().get()))
    .bind(i64::from(lineage.target_generation().get()))
    .bind(&source.definition_id)
    .bind(source.revision.definition_revision_id().as_str())
    .bind(source.revision.deployment_revision_id().as_str())
    .bind(source.revision.plan_hash().as_str())
    .bind(source.revision.binding_hash().as_str())
    .bind(target_revision.definition_id())
    .bind(target_revision.revision().definition_revision_id().as_str())
    .bind(target_revision.revision().deployment_revision_id().as_str())
    .bind(target_revision.revision().plan_hash().as_str())
    .bind(target_revision.revision().binding_hash().as_str())
    .bind(lineage.source_checkpoint_hash().map(ContentHash::as_str))
    .bind(created_by.as_str())
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    for (role, definition_id, revision) in [
        ("source", source.definition_id.as_str(), &source.revision),
        (
            "target",
            target_revision.definition_id(),
            target_revision.revision(),
        ),
    ] {
        sqlx::query(
            "INSERT INTO recovery_revision_roots (
                run_id,root_role,source_run_id,definition_id,definition_revision_id,
                deployment_revision_id,plan_hash,binding_hash,created_by_transition_key,
                projection_version,created_at
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,CURRENT_TIMESTAMP)",
        )
        .bind(target_run_id.as_str())
        .bind(role)
        .bind(source.run_id.as_str())
        .bind(definition_id)
        .bind(revision.definition_revision_id().as_str())
        .bind(revision.deployment_revision_id().as_str())
        .bind(revision.plan_hash().as_str())
        .bind(revision.binding_hash().as_str())
        .bind(created_by.as_str())
        .execute(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_reuse_candidate(
    tx: &mut Transaction<'_, Postgres>,
    parent_transition_key: &TransitionKey,
    intent_hash: &str,
    source: &SourceRun,
    target_run_id: &RunId,
    target_revision: &RecoveryRevisionSpec,
    candidate: &CreateReuseCandidateCommand,
    kind: RunLineageKind,
    mappings: &[MigrationMappingCompatibility],
) -> Result<bool, RepositoryError> {
    if candidate.run_id() != target_run_id
        || candidate.source_run_id() != &source.run_id
        || candidate.definition_revision_id() != target_revision.revision().definition_revision_id()
        || candidate.deployment_revision_id() != target_revision.revision().deployment_revision_id()
        || candidate.plan_hash() != target_revision.revision().plan_hash()
        || candidate.binding_hash() != target_revision.revision().binding_hash()
    {
        return Ok(false);
    }
    let Some(target_plan) = revision_plan_postgres(tx, target_revision).await? else {
        return Ok(false);
    };
    let target_index = PlanIndex::new(&target_plan).map_err(|_| RepositoryError::invalid_data())?;
    let Some(target_node) = target_index.node(candidate.target_node_id()) else {
        return Ok(false);
    };
    let occurrence: LogicalOccurrence = serde_json::from_str(candidate.stable_activation_key())
        .map_err(|_| RepositoryError::invalid_data())?;
    let expected_scope = insight_engine::internal::scope_instance_for_occurrence(
        &target_index,
        target_run_id,
        target_node,
        &occurrence,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if candidate.target_scope_instance_id() != &expected_scope {
        return Ok(false);
    }
    let activation = sqlx::query(
        "SELECT a.node_id,a.lifecycle,a.effect_id,a.effect_evidence,a.output_payload_id,
                a.output_artifact_id,a.output_value_hash,t.task_envelope
         FROM node_activations a
         JOIN task_outbox t ON t.run_id=a.run_id AND t.activation_id=a.activation_id
          AND t.attempt_no=a.winning_attempt_no
         WHERE a.run_id=$1 AND a.activation_id=$2",
    )
    .bind(source.run_id.as_str())
    .bind(candidate.source_activation_id().as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(activation) = activation else {
        return Ok(false);
    };
    let envelope: DurableTaskExecutionRequest = serde_json::from_value(
        activation
            .try_get::<Value, _>("task_envelope")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let mut source_dependencies: std::collections::BTreeSet<ActivationId> = envelope
        .request()
        .inputs()
        .iter()
        .flat_map(|input| input.source_activations().iter().cloned())
        .collect();
    source_dependencies.remove(candidate.source_activation_id());
    if &source_dependencies != candidate.source_data_dependencies() {
        return Ok(false);
    }
    let source_node = activation
        .try_get::<String, _>("node_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let mapping = mappings.iter().find(|mapping| {
        mapping.source().source_node_id().as_str() == source_node
            && mapping.source().target_node_id() == candidate.target_node_id()
    });
    let node_compatible = if kind == RunLineageKind::Migrate {
        mapping.is_some_and(|mapping| {
            candidate.compatibility().output_schema_hash() == mapping.target().output_schema_hash()
                && candidate.compatibility().effect_policy_hash()
                    == mapping.target().effect_policy_hash()
        })
    } else {
        source_node == candidate.target_node_id().as_str()
    };
    let payload = activation
        .try_get::<Option<String>, _>("output_payload_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let artifact = activation
        .try_get::<Option<String>, _>("output_artifact_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let output_hash = activation
        .try_get::<Option<String>, _>("output_value_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    if !node_compatible
        || activation
            .try_get::<String, _>("lifecycle")
            .map_err(|_| RepositoryError::invalid_data())?
            != "succeeded"
        || activation
            .try_get::<String, _>("effect_id")
            .map_err(|_| RepositoryError::invalid_data())?
            != candidate.inherited_effect_id().as_str()
        || activation
            .try_get::<String, _>("effect_evidence")
            .map_err(|_| RepositoryError::invalid_data())?
            == "unknown"
        || output_hash.as_deref() != Some(candidate.output_value_hash().as_str())
        || !source_output_valid(
            tx,
            &source.run_id,
            payload.as_deref(),
            artifact.as_deref(),
            candidate.output_value_hash(),
        )
        .await?
    {
        return Ok(false);
    }
    let candidate_key = model_data(TransitionKey::derive(
        "repository.recovery.reuse_candidate",
        &[
            parent_transition_key.as_str(),
            target_run_id.as_str(),
            candidate.candidate_id(),
        ],
    ))?;
    let seq = allocate_event_seq(tx, target_run_id).await?;
    let id = event_id(&candidate_key);
    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(target_run_id.clone()),
        ExecutionEventPayload::ProjectionMutated {
            mutation: ProjectionMutationKind::ReuseCandidateCreated,
        },
    ))?;
    insert_event(
        tx,
        target_run_id,
        seq,
        &id,
        &candidate_key,
        intent_hash,
        0,
        &event,
    )
    .await?;
    let provenance = control_adapter::durable_reuse_provenance(candidate)?;
    sqlx::query(
        "INSERT INTO run_reuse_candidates (
            run_id,candidate_id,target_scope_instance_id,target_node_id,stable_activation_key,
            source_run_id,source_activation_id,source_control_provenance,
            definition_revision_id,deployment_revision_id,plan_hash,binding_hash,node_config_hash,
            descriptor_hash,input_value_hash,output_value_hash,output_schema_hash,
            effect_policy_hash,inherited_effect_id,data_dependencies_hash,
            created_by_transition_key,candidate_state,materialized_activation_id,
            decision_transition_key,projection_version,created_at,decided_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,'candidate',NULL,NULL,0,
                   CURRENT_TIMESTAMP,NULL)",
    )
    .bind(target_run_id.as_str())
    .bind(candidate.candidate_id())
    .bind(candidate.target_scope_instance_id().as_str())
    .bind(candidate.target_node_id().as_str())
    .bind(candidate.stable_activation_key())
    .bind(source.run_id.as_str())
    .bind(candidate.source_activation_id().as_str())
    .bind(provenance)
    .bind(candidate.definition_revision_id().as_str())
    .bind(candidate.deployment_revision_id().as_str())
    .bind(candidate.plan_hash().as_str())
    .bind(candidate.binding_hash().as_str())
    .bind(candidate.compatibility().node_config_hash().as_str())
    .bind(candidate.compatibility().descriptor_hash().as_str())
    .bind(candidate.compatibility().input_value_hash().as_str())
    .bind(candidate.output_value_hash().as_str())
    .bind(candidate.compatibility().output_schema_hash().as_str())
    .bind(candidate.compatibility().effect_policy_hash().as_str())
    .bind(candidate.inherited_effect_id().as_str())
    .bind(candidate.compatibility().data_dependencies_hash().as_str())
    .bind(candidate_key.as_str())
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "INSERT INTO recovery_effect_roots (
            run_id,source_run_id,effect_run_id,source_activation_id,effect_id,
            created_by_transition_key,projection_version,created_at
         ) VALUES ($1,$2,$3,$4,$5,$6,0,CURRENT_TIMESTAMP)",
    )
    .bind(target_run_id.as_str())
    .bind(source.run_id.as_str())
    .bind(source.run_id.as_str())
    .bind(candidate.source_activation_id().as_str())
    .bind(candidate.inherited_effect_id().as_str())
    .bind(candidate_key.as_str())
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    if let Some(artifact_id) = artifact {
        sqlx::query(
            "INSERT INTO recovery_artifact_roots (
                run_id,source_run_id,artifact_run_id,artifact_id,source_activation_id,
                created_by_transition_key,projection_version,created_at
             ) VALUES ($1,$2,$3,$4,$5,$6,0,CURRENT_TIMESTAMP)",
        )
        .bind(target_run_id.as_str())
        .bind(source.run_id.as_str())
        .bind(source.run_id.as_str())
        .bind(artifact_id)
        .bind(candidate.source_activation_id().as_str())
        .bind(candidate_key.as_str())
        .execute(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
    }
    finalize_projection_checkpoints(tx, target_run_id, &id).await?;
    Ok(true)
}

async fn source_output_valid(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    payload_id: Option<&str>,
    artifact_id: Option<&str>,
    expected_hash: &ContentHash,
) -> Result<bool, RepositoryError> {
    match (payload_id, artifact_id) {
        (Some(payload), None) => {
            let row = sqlx::query(
                "SELECT payload_id,content_hash,canonical_bytes,encoding,inline_value,binary_value
                 FROM payloads WHERE run_id=$1 AND payload_id=$2",
            )
            .bind(run_id.as_str())
            .bind(payload)
            .fetch_optional(&mut **tx)
            .await
            .map_err(RepositoryError::storage)?;
            let Some(row) = row else {
                return Ok(false);
            };
            let stored_payload_id = row
                .try_get::<String, _>("payload_id")
                .map_err(|_| RepositoryError::invalid_data())?;
            if stored_payload_id != payload {
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
            Ok(
                common_contract_adapter::validated_inline_payload_content_hash(&validated)
                    == expected_hash,
            )
        }
        (None, Some(artifact)) => {
            let row = sqlx::query(
                "SELECT a.content_hash,a.artifact_state FROM artifacts a
                 LEFT JOIN artifact_retention_releases release ON release.run_id=a.run_id
                 WHERE a.run_id=$1 AND a.artifact_id=$2
                   AND (release.run_id IS NULL OR release.retain_until>CURRENT_TIMESTAMP)
                 FOR SHARE OF a",
            )
            .bind(run_id.as_str())
            .bind(artifact)
            .fetch_optional(&mut **tx)
            .await
            .map_err(RepositoryError::storage)?;
            Ok(row.is_some_and(|row| {
                row.try_get::<String, _>("content_hash").ok().as_deref()
                    == Some(expected_hash.as_str())
                    && matches!(
                        row.try_get::<String, _>("artifact_state").ok().as_deref(),
                        Some("verified" | "referenced")
                    )
            }))
        }
        _ => Ok(false),
    }
}

#[allow(clippy::too_many_arguments)]
async fn terminalize_source(
    tx: &mut Transaction<'_, Postgres>,
    transition_key: &TransitionKey,
    intent_hash: &str,
    source: &SourceRun,
    replacement_run_id: &RunId,
    termination_reason: &str,
    public_error_code: &str,
) -> Result<Option<RecoveryEventReceipt>, RepositoryError> {
    let next_version = source
        .projection_version
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let seq = allocate_event_seq(tx, &source.run_id).await?;
    let id = event_id(transition_key);
    let event = model_data(PendingExecutionEvent::new(
        ExecutionEventContext::for_run(source.run_id.clone()),
        ExecutionEventPayload::RunLifecycleChanged {
            lifecycle: RunLifecycle::Cancelled,
        },
    ))?;
    let event_occurred_at = insert_event(
        tx,
        &source.run_id,
        seq,
        &id,
        transition_key,
        intent_hash,
        next_version,
        &event,
    )
    .await?;
    let public_payload = PublicEventPayload::RunCancelled {
        failure: PublicFailureSummary {
            kind: PublicFailureKind::Stop,
            code: model_data(PublicErrorCode::new(public_error_code))?,
        },
    };
    let public_kind = public_payload.kind();
    let public_id = public_event_id(&source.run_id, transition_key, public_kind);
    let envelope = serde_json::to_value(durable_public_event_envelope(
        &source.run_id,
        &public_id,
        &id,
        seq,
        event_occurred_at,
        public_payload,
    )?)
    .map_err(|_| RepositoryError::canonicalization())?;
    sqlx::query(
        "INSERT INTO public_event_outbox (
            run_id,public_event_id,causation_event_id,public_ordinal,public_schema_version,event_kind,
            is_terminal,publish_state,safe_envelope,available_at,claimed_by,claim_token,
            claim_expires_at,publish_attempts,published_at,published_by,published_claim_token,
            notified_at,retain_until,created_at
         ) VALUES ($1,$2,$3,$4,1,$5,TRUE,'pending',$6,CURRENT_TIMESTAMP,NULL,NULL,NULL,0,NULL,NULL,
                   NULL,NULL,NULL,CURRENT_TIMESTAMP)",
    )
    .bind(source.run_id.as_str())
    .bind(&public_id)
    .bind(&id)
    .bind(i32::from(public_event_ordinal(public_kind)))
    .bind(public_kind.as_str())
    .bind(envelope)
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let updated = sqlx::query(
        "UPDATE workflow_runs SET lifecycle='cancelled',admission_state='closed',
                termination_intent_reason=$1,
                termination_intent_transition_key=COALESCE(termination_intent_transition_key,$2),
                termination_intent_at=COALESCE(termination_intent_at,CURRENT_TIMESTAMP),
                error_code=$3,terminal_event_id=$4,terminal_public_event_id=$5,replacement_run_id=$6,
                projection_version=$7,scheduler_lease_owner=NULL,scheduler_fencing_token=NULL,
                scheduler_lease_expires_at=NULL,scheduler_heartbeat_at=NULL,
                updated_at=CURRENT_TIMESTAMP,terminal_at=CURRENT_TIMESTAMP
         WHERE run_id=$8 AND projection_version=$9 AND lifecycle IN ('active','waiting')
           AND admission_state='paused'",
    )
    .bind(termination_reason)
    .bind(transition_key.as_str())
    .bind(public_error_code)
    .bind(&id)
    .bind(&public_id)
    .bind(replacement_run_id.as_str())
    .bind(i64_from_u64(next_version)?)
    .bind(source.run_id.as_str())
    .bind(i64_from_u64(source.projection_version)?)
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    if updated.rows_affected() != 1 {
        return Ok(None);
    }
    sqlx::query(
        "UPDATE node_activations SET lifecycle='cancelled',current_attempt_no=NULL,
                current_lease_epoch=NULL,current_fencing_token=NULL,pending_retry_timer_id=NULL,
                termination_intent_reason=$1,termination_intent_transition_key=$2,
                termination_intent_at=CURRENT_TIMESTAMP,output_payload_id=NULL,
                output_artifact_id=NULL,output_value_hash=NULL,winning_attempt_no=NULL,
                projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP,
                terminal_at=CURRENT_TIMESTAMP
         WHERE run_id=$3 AND lifecycle IN
           ('created','ready','leased','running','retry_wait','waiting','terminating')",
    )
    .bind(termination_reason)
    .bind(transition_key.as_str())
    .bind(source.run_id.as_str())
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "UPDATE timers SET timer_state='cancelled',projection_version=projection_version+1,
                fired_at=CURRENT_TIMESTAMP WHERE run_id=$1 AND timer_state='scheduled'",
    )
    .bind(source.run_id.as_str())
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let pending_signals = sqlx::query_scalar::<_, String>(
        "SELECT signal_id FROM signals_inbox WHERE run_id=$1 AND signal_state='pending'
         ORDER BY signal_id FOR UPDATE",
    )
    .bind(source.run_id.as_str())
    .fetch_all(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    for signal_id in pending_signals {
        let rejection_key = model_data(TransitionKey::derive(
            "repository.recovery.signal_rejected",
            &[transition_key.as_str(), signal_id.as_str()],
        ))?;
        sqlx::query(
            "UPDATE signals_inbox SET signal_state='rejected',consumed_by_transition_key=$1,
                    consumed_event_id=$2,terminal_at=CURRENT_TIMESTAMP,
                    projection_version=projection_version+1
             WHERE run_id=$3 AND signal_id=$4 AND signal_state='pending'",
        )
        .bind(rejection_key.as_str())
        .bind(&id)
        .bind(source.run_id.as_str())
        .bind(&signal_id)
        .execute(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?;
    }
    sqlx::query(
        "UPDATE task_outbox SET task_state='dead',claimed_by=NULL,claim_token=NULL,
                claim_expires_at=NULL,last_error_code=$1,projection_version=projection_version+1
         WHERE run_id=$2 AND task_state IN ('pending','claimed')",
    )
    .bind(public_error_code)
    .bind(source.run_id.as_str())
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "UPDATE scope_instances SET lifecycle='cancelled',admission_state='closed',
                projection_version=projection_version+1,settled_at=CURRENT_TIMESTAMP
         WHERE run_id=$1 AND lifecycle IN ('active','settling')
           AND admitted_children=settled_children",
    )
    .bind(source.run_id.as_str())
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    super::postgres_model_tool_queue::close_model_tool_work_for_terminal_run_postgres(
        tx,
        &source.run_id,
    )
    .await?;
    super::postgres::persist_terminal_response_snapshot_postgres(
        tx,
        &source.run_id,
        RunLifecycle::Cancelled,
        None,
        Some(public_error_code),
    )
    .await?;
    super::postgres::register_terminal_artifact_retention_postgres(
        tx,
        &source.run_id,
        transition_key,
        intent_hash,
        &id,
        seq,
    )
    .await?;
    Ok(Some(recovery_event(
        &source.run_id,
        seq,
        &id,
        next_version,
    )?))
}

fn recovery_event(
    run_id: &RunId,
    seq: u64,
    id: &str,
    projection_version: u64,
) -> Result<RecoveryEventReceipt, RepositoryError> {
    Ok(recovery_adapter::recovery_event_receipt(
        run_id.clone(),
        seq,
        model_data(ExecutionEventId::parse(id.to_owned()))?,
        projection_version,
    ))
}

async fn replay_result<T: DeserializeOwned>(
    tx: &mut Transaction<'_, Postgres>,
    authority_run_id: &RunId,
    transition_key: &TransitionKey,
    intent_hash: &str,
) -> Result<Option<T>, RepositoryError> {
    let row = sqlx::query(
        "SELECT intent_hash,primary_event_run_id,primary_event_id,result_json
         FROM recovery_transition_results
         WHERE authority_run_id=$1 AND transition_key=$2",
    )
    .bind(authority_run_id.as_str())
    .bind(transition_key.as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row
        .try_get::<String, _>("intent_hash")
        .map_err(|_| RepositoryError::invalid_data())?
        != intent_hash
    {
        return Err(RepositoryError::intent_conflict());
    }
    let primary_run = model_data(RunId::new(
        row.try_get::<String, _>("primary_event_run_id")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))?;
    let primary_event = row
        .try_get::<String, _>("primary_event_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    verify_projection_checkpoint_batch(tx, &primary_run, &primary_event).await?;
    serde_json::from_value(
        row.try_get::<Value, _>("result_json")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map(Some)
    .map_err(|_| RepositoryError::invalid_data())
}

#[allow(clippy::too_many_arguments)]
async fn store_result<T: Serialize>(
    tx: &mut Transaction<'_, Postgres>,
    authority_run_id: &RunId,
    transition_key: &TransitionKey,
    intent_hash: &str,
    primary_event_run_id: &RunId,
    primary_event_id: &str,
    result: &T,
) -> Result<(), RepositoryError> {
    let result_json =
        serde_json::to_value(result).map_err(|_| RepositoryError::canonicalization())?;
    sqlx::query(
        "INSERT INTO recovery_transition_results (
            authority_run_id,transition_key,intent_hash,primary_event_run_id,
            primary_event_id,result_json,created_at
         ) VALUES ($1,$2,$3,$4,$5,$6,CURRENT_TIMESTAMP)",
    )
    .bind(authority_run_id.as_str())
    .bind(transition_key.as_str())
    .bind(intent_hash)
    .bind(primary_event_run_id.as_str())
    .bind(primary_event_id)
    .bind(result_json)
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

fn json_from_postgres(row: &sqlx::postgres::PgRow, column: &str) -> Result<Value, RepositoryError> {
    row.try_get::<Value, _>(column)
        .map_err(|_| RepositoryError::invalid_data())
}

fn revision_from_intent(
    row: &sqlx::postgres::PgRow,
) -> Result<RecoveryRevisionSpec, RepositoryError> {
    RecoveryRevisionSpec::new(
        row.try_get::<String, _>("target_definition_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        ExecutionRevisionPin::new(
            model_data(DefinitionRevisionId::new(
                row.try_get::<String, _>("target_definition_revision_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            model_data(DeploymentRevisionId::new(
                row.try_get::<String, _>("target_deployment_revision_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            model_data(ContentHash::parse(
                row.try_get::<String, _>("target_plan_hash")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
            model_data(ContentHash::parse(
                row.try_get::<String, _>("target_binding_hash")
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))?,
        ),
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, Row};
    use uuid::Uuid;

    use insight_engine::control::ControlEmissionSlot;
    use insight_engine::plan::{
        AuthorFormat, DataBinding, DataBindingId, DataPort, DataPortId, Node, NodeKind,
        PlanBuilder, PlanInputContract, PlanMetadata, PlanType, PortDirection, PortName,
        ReturnDescriptor, ScopeId, ScopeMetadata, ValueSource, VersionTag,
    };
    use insight_engine::{
        ActivationId, ContentHash, ControlTokenProvenance, DefinitionRevisionId,
        DeploymentRevisionId, EffectId, ExecutionKind, MigrationNodeMapping, NodeId, PortId,
        PublicFailureKind, RunLifecycle, SignalId, TerminationReason,
    };

    use super::*;
    use insight_durable::{
        ActivationAdmissionCommand, ActivationCasCommand, ActivationDurableRepository,
        ClaimSchedulerRunCommand, CreateRunCommand, DurableRepository, PublicEventIntent,
        PublicationHead, PublicationOrigin, PublishVersionedPlanCommand, ReceiveSignalCommand,
        ReuseCompatibility, RunTransitionCommand, SchedulerLeaseRepository, VersionedPlan,
        REPOSITORY_INTENT_CONFLICT, REPOSITORY_RUN_MIGRATING,
    };

    fn key(label: &str) -> TransitionKey {
        TransitionKey::derive("postgres.recovery.test", &[label]).unwrap()
    }

    async fn isolated_repository() -> Option<PostgresDurableRepository> {
        let database_url = std::env::var("TEST_POSTGRES_URL").ok()?;
        let schema = format!("recovery_{}", Uuid::new_v4().simple());
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
        let repository = PostgresDurableRepository::connect_provisioned_for_test(&scoped_url)
            .await
            .unwrap();
        Some(repository)
    }

    fn plan(label: &str) -> VersionedPlan {
        let return_node = NodeId::new("return_node").unwrap();
        let return_value = DataPortId::new("return_value").unwrap();
        let root_scope = ScopeId::new("root_scope").unwrap();
        let mut builder = PlanBuilder::new(PlanMetadata::new(
            DefinitionRevisionId::new(format!("definition_revision_{label}")).unwrap(),
            VersionTag::new("compiler-3.0.0").unwrap(),
            AuthorFormat::Programmatic,
            return_node.clone(),
            PlanInputContract::new(PlanType::Any),
            PlanType::Any,
            PlanType::safe_error().unwrap(),
        ));
        builder
            .add_scope(ScopeMetadata::root(root_scope.clone()))
            .add_node(Node::new(
                return_node.clone(),
                root_scope,
                NodeKind::Return(ReturnDescriptor {
                    value_input: return_value.clone(),
                }),
            ))
            .add_data_port(DataPort::new(
                return_value.clone(),
                return_node,
                PortName::new("value").unwrap(),
                PortDirection::Input,
                PlanType::Any,
                true,
            ))
            .add_data_binding(DataBinding::new(
                DataBindingId::new("bind_return").unwrap(),
                ValueSource::RunInput { path: vec![] },
                return_value,
            ));
        let plan = builder.build().unwrap();
        VersionedPlan::from_verified_plan(
            format!("definition_{label}"),
            format!("agent_{label}"),
            format!("Recovery {label}"),
            DeploymentRevisionId::new(format!("deployment_revision_{label}")).unwrap(),
            "expression-3.0.0",
            json!({"kind": "structured"}),
            &plan,
            json!({}),
            json!({"fixture_revision": label}),
            json!({"worker": "v1"}),
        )
        .unwrap()
    }

    fn publication_revision(agent_id: &str, label: &str) -> VersionedPlan {
        insight_durable::model::adapter::versioned_plan_for_test(
            format!("definition_{agent_id}"),
            agent_id,
            format!("Publication {label}"),
            DefinitionRevisionId::new(format!("definition_revision_{label}")).unwrap(),
            DeploymentRevisionId::new(format!("deployment_revision_{label}")).unwrap(),
            ContentHash::from_bytes(format!("plan_{label}").as_bytes()),
            ContentHash::from_bytes(format!("binding_{label}").as_bytes()),
            "compiler-3.0.0",
            "expression-3.0.0",
            json!({"kind": "structured", "revision": label}),
            json!({"nodes": []}),
            json!([]),
            json!([]),
            json!([]),
        )
        .unwrap()
    }

    fn revision(plan: &VersionedPlan) -> RecoveryRevisionSpec {
        RecoveryRevisionSpec::new(
            plan.definition_id(),
            ExecutionRevisionPin::new(
                plan.definition_revision_id().clone(),
                plan.deployment_revision_id().clone(),
                plan.plan_hash().clone(),
                plan.binding_hash().clone(),
            ),
        )
        .unwrap()
    }

    async fn install_run(
        repository: &PostgresDurableRepository,
        plan: &VersionedPlan,
        run: &str,
        input: Value,
    ) -> RunId {
        repository.install_versioned_plan(plan).await.unwrap();
        let run_id = RunId::new(run).unwrap();
        repository
            .create_run(
                key(&format!("{run}.create")),
                CreateRunCommand::new(run_id.clone(), plan, input).unwrap(),
            )
            .await
            .unwrap();
        run_id
    }

    async fn assert_recovery_deadline(
        repository: &PostgresDurableRepository,
        run_id: &RunId,
        expected_lineage_kind: &str,
    ) {
        assert!(repository
            .load_run(run_id)
            .await
            .unwrap()
            .unwrap()
            .deadline_at()
            .is_some());
        let row = sqlx::query_as::<_, (String, bool, bool)>(
            "SELECT r.lineage_kind,
                    r.deadline_at > clock_timestamp(),
                    (e.safe_payload->>'run_deadline_at')::timestamptz = r.deadline_at
             FROM workflow_runs r JOIN execution_events e
               ON e.run_id=r.run_id AND e.kind='run.created'
             WHERE r.run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_one(repository.test_pool())
        .await
        .unwrap();
        assert_eq!(row, (expected_lineage_kind.to_owned(), true, true));
    }

    async fn activate(repository: &PostgresDurableRepository, run_id: &RunId) {
        repository
            .commit_run_transition(
                key(&format!("{}.activate", run_id.as_str())),
                RunTransitionCommand::nonterminal(
                    run_id.clone(),
                    0,
                    RunLifecycle::Created,
                    AdmissionState::Open,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    PendingExecutionEvent::new(
                        ExecutionEventContext::for_run(run_id.clone()),
                        ExecutionEventPayload::RunLifecycleChanged {
                            lifecycle: RunLifecycle::Active,
                        },
                    )
                    .unwrap(),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }

    async fn pause(repository: &PostgresDurableRepository, run_id: &RunId, version: u64) {
        repository
            .commit_run_transition(
                key(&format!("{}.pause", run_id.as_str())),
                RunTransitionCommand::nonterminal(
                    run_id.clone(),
                    version,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    RunLifecycle::Active,
                    AdmissionState::Paused,
                    PendingExecutionEvent::new(
                        ExecutionEventContext::for_run(run_id.clone()),
                        ExecutionEventPayload::RunAdmissionChanged {
                            admission: AdmissionState::Paused,
                        },
                    )
                    .unwrap(),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }

    async fn fail(repository: &PostgresDurableRepository, run_id: &RunId) {
        if repository
            .load_run(run_id)
            .await
            .unwrap()
            .unwrap()
            .lifecycle()
            == RunLifecycle::Created
        {
            activate(repository, run_id).await;
        }
        repository
            .commit_run_transition(
                key(&format!("{}.terminate", run_id.as_str())),
                RunTransitionCommand::termination_intent(
                    run_id.clone(),
                    1,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    TerminationReason::Failure,
                    PendingExecutionEvent::new(
                        ExecutionEventContext::for_run(run_id.clone()),
                        ExecutionEventPayload::RunTerminationClaimed {
                            reason: TerminationReason::Failure,
                        },
                    )
                    .unwrap(),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let failure = PublicFailureSummary {
            kind: PublicFailureKind::Workflow,
            code: PublicErrorCode::new("SOURCE_FAILED").unwrap(),
        };
        repository
            .commit_run_transition(
                key(&format!("{}.failed", run_id.as_str())),
                RunTransitionCommand::terminal_failure(
                    run_id.clone(),
                    2,
                    TerminationReason::Failure,
                    PublicErrorCode::new("SOURCE_FAILED").unwrap(),
                    PendingExecutionEvent::new(
                        ExecutionEventContext::for_run(run_id.clone()),
                        ExecutionEventPayload::RunLifecycleChanged {
                            lifecycle: RunLifecycle::Failed,
                        },
                    )
                    .unwrap(),
                    <PublicEventIntent as PublicEventIntentAdapter>::new(
                        PublicEventPayload::RunFailed { failure },
                    ),
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }

    async fn install_scheduler_checkpoint(
        repository: &PostgresDurableRepository,
        run_id: &RunId,
        projection_version: u64,
        label: &str,
        close_projection_batch: bool,
    ) -> ContentHash {
        let transition_key = key(&format!("{label}.checkpoint"));
        let event_id_value = event_id(&transition_key);
        let intent_hash =
            ContentHash::from_bytes(format!("postgres/recovery/checkpoint/{label}").as_bytes());
        let checkpoint_digest =
            ContentHash::from_bytes(format!("postgres/recovery/checkpoint-id/{label}").as_bytes());
        let checkpoint_id = format!(
            "checkpoint_{}",
            checkpoint_digest.as_str().trim_start_matches("sha256:")
        );
        let fact_payload = json!({"boundary": label});
        let checkpoint_hash = scheduler_checkpoint_content_hash(
            run_id.as_str(),
            &checkpoint_id,
            "planned_action",
            transition_key.as_str(),
            intent_hash.as_str(),
            &event_id_value,
            SCHEDULER_CHECKPOINT_SCHEMA_VERSION,
            projection_version,
            &fact_payload,
        )
        .unwrap();
        let lifecycle = repository
            .load_run(run_id)
            .await
            .unwrap()
            .unwrap()
            .lifecycle();
        let event = PendingExecutionEvent::new(
            ExecutionEventContext::for_run(run_id.clone()),
            ExecutionEventPayload::RunLifecycleChanged { lifecycle },
        )
        .unwrap();
        let mut tx = repository.test_pool().begin().await.unwrap();
        let seq = allocate_event_seq(&mut tx, run_id).await.unwrap();
        insert_event(
            &mut tx,
            run_id,
            seq,
            &event_id_value,
            &transition_key,
            intent_hash.as_str(),
            projection_version,
            &event,
        )
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO scheduler_checkpoints (
                run_id,checkpoint_id,content_hash,checkpoint_kind,transition_key,intent_hash,
                event_id,checkpoint_schema_version,scheduler_projection_version,fact_payload,
                projection_version,created_at
             ) VALUES ($1,$2,$3,'planned_action',$4,$5,$6,$7,$8,$9,0,CURRENT_TIMESTAMP)",
        )
        .bind(run_id.as_str())
        .bind(&checkpoint_id)
        .bind(checkpoint_hash.as_str())
        .bind(transition_key.as_str())
        .bind(intent_hash.as_str())
        .bind(&event_id_value)
        .bind(i32::try_from(SCHEDULER_CHECKPOINT_SCHEMA_VERSION).unwrap())
        .bind(i64_from_u64(projection_version).unwrap())
        .bind(&fact_payload)
        .execute(&mut *tx)
        .await
        .unwrap();
        if close_projection_batch {
            finalize_projection_checkpoints(&mut tx, run_id, &event_id_value)
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
        checkpoint_hash
    }

    fn mapping(
        source_hash: &[u8],
        target_hash: &[u8],
    ) -> Result<MigrationMappingCompatibility, insight_engine::ModelError> {
        let source = MigrationNodeMapping::new(
            NodeId::new("analyze_v1").unwrap(),
            NodeId::new("analyze_v2").unwrap(),
            std::collections::BTreeMap::from([(
                insight_engine::plan::DataPortId::new("analyze_v1.output").unwrap(),
                insight_engine::plan::DataPortId::new("analyze_v2.output").unwrap(),
            )]),
            ContentHash::from_bytes(source_hash),
            ContentHash::from_bytes(b"output_schema"),
            ContentHash::from_bytes(b"effect_policy"),
            true,
            true,
        );
        let target = MigrationNodeMapping::new(
            NodeId::new("analyze_v1").unwrap(),
            NodeId::new("analyze_v2").unwrap(),
            std::collections::BTreeMap::from([(
                insight_engine::plan::DataPortId::new("analyze_v1.output").unwrap(),
                insight_engine::plan::DataPortId::new("analyze_v2.output").unwrap(),
            )]),
            ContentHash::from_bytes(target_hash),
            ContentHash::from_bytes(b"output_schema"),
            ContentHash::from_bytes(b"effect_policy"),
            true,
            true,
        );
        MigrationMappingCompatibility::new(source, target)
    }

    fn wait_mapping(
        source_signal_rebuild: bool,
        target_signal_rebuild: bool,
    ) -> MigrationMappingCompatibility {
        let ports = std::collections::BTreeMap::from([(
            insight_engine::plan::DataPortId::new("wait_v1.output").unwrap(),
            insight_engine::plan::DataPortId::new("wait_v2.output").unwrap(),
        )]);
        let source = MigrationNodeMapping::new(
            NodeId::new("wait_v1").unwrap(),
            NodeId::new("wait_v2").unwrap(),
            ports.clone(),
            ContentHash::from_bytes(b"wait_input"),
            ContentHash::from_bytes(b"wait_output"),
            ContentHash::from_bytes(b"wait_effect"),
            source_signal_rebuild,
            false,
        );
        let target = MigrationNodeMapping::new(
            NodeId::new("wait_v1").unwrap(),
            NodeId::new("wait_v2").unwrap(),
            ports,
            ContentHash::from_bytes(b"wait_input"),
            ContentHash::from_bytes(b"wait_output"),
            ContentHash::from_bytes(b"wait_effect"),
            target_signal_rebuild,
            false,
        );
        MigrationMappingCompatibility::new(source, target).unwrap()
    }

    fn invalid_candidate(
        source: &RunId,
        target: &RunId,
        target_revision: &RecoveryRevisionSpec,
    ) -> CreateReuseCandidateCommand {
        let missing = ActivationId::new("missing_source_activation").unwrap();
        let provenance = ControlTokenProvenance::new(
            source.clone(),
            missing.clone(),
            PortId::new("out").unwrap(),
            ControlEmissionSlot::ActivationOutput,
            ScopeInstanceId::root(),
            vec![],
        )
        .unwrap();
        CreateReuseCandidateCommand::new(
            target.clone(),
            "candidate_missing",
            ScopeInstanceId::root(),
            NodeId::new("analyze_v2").unwrap(),
            "root:analyze",
            source.clone(),
            missing,
            provenance,
            target_revision.revision().definition_revision_id().clone(),
            target_revision.revision().deployment_revision_id().clone(),
            target_revision.revision().plan_hash().clone(),
            target_revision.revision().binding_hash().clone(),
            ContentHash::from_bytes(b"missing_output"),
            EffectId::new("missing_effect").unwrap(),
            ReuseCompatibility::new(
                ContentHash::from_bytes(b"config"),
                ContentHash::from_bytes(b"descriptor"),
                ContentHash::from_bytes(b"input"),
                ContentHash::from_bytes(b"output_schema"),
                ContentHash::from_bytes(b"effect_policy"),
                ContentHash::from_bytes(b"dependencies"),
            ),
        )
        .unwrap()
    }

    async fn valid_candidate(
        repository: &PostgresDurableRepository,
        source: &RunId,
        target: &RunId,
        target_revision: &RecoveryRevisionSpec,
    ) -> CreateReuseCandidateCommand {
        let activation_id = ActivationId::new("activation_recovery_source").unwrap();
        repository
            .admit_activation(
                key("candidate.source.admit"),
                ActivationAdmissionCommand::new(
                    source.clone(),
                    ScopeInstanceId::root(),
                    0,
                    activation_id.clone(),
                    NodeId::new("analyze_v1").unwrap(),
                    "source:analyze",
                    ExecutionKind::SchedulerNative,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        repository
            .make_activation_ready(
                key("candidate.source.ready"),
                ActivationCasCommand::new(source.clone(), activation_id.clone(), 0),
            )
            .await
            .unwrap();
        let artifact_id = "artifact_recovery_source";
        let output_hash = ContentHash::from_bytes(b"recovery artifact output");
        sqlx::query(
            "INSERT INTO artifacts (
                run_id,artifact_id,content_hash,size_bytes,media_type,storage_uri,
                artifact_state,verified_at,referenced_at,retain_until,deletion_fence,
                deletion_claim_token,deletion_claimed_by,deletion_claim_request_key,
                deletion_claimed_at,deletion_claim_expires_at,created_at
             ) VALUES ($1,$2,$3,24,'application/json','object://recovery/source',
                       'verified',CURRENT_TIMESTAMP,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                       CURRENT_TIMESTAMP)",
        )
        .bind(source.as_str())
        .bind(artifact_id)
        .bind(output_hash.as_str())
        .execute(repository.test_pool())
        .await
        .unwrap();
        let effect_id: String = sqlx::query_scalar(
            "SELECT effect_id FROM node_activations WHERE run_id=$1 AND activation_id=$2",
        )
        .bind(source.as_str())
        .bind(activation_id.as_str())
        .fetch_one(repository.test_pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE node_activations SET lifecycle='succeeded',effect_evidence='committed',
                    output_artifact_id=$1,output_value_hash=$2,
                    projection_version=projection_version+1,
                    updated_at=CURRENT_TIMESTAMP,terminal_at=CURRENT_TIMESTAMP
             WHERE run_id=$3 AND activation_id=$4 AND lifecycle='ready'",
        )
        .bind(artifact_id)
        .bind(output_hash.as_str())
        .bind(source.as_str())
        .bind(activation_id.as_str())
        .execute(repository.test_pool())
        .await
        .unwrap();
        let provenance = ControlTokenProvenance::new(
            source.clone(),
            activation_id.clone(),
            PortId::new("out").unwrap(),
            ControlEmissionSlot::ActivationOutput,
            ScopeInstanceId::root(),
            vec![],
        )
        .unwrap();
        CreateReuseCandidateCommand::new(
            target.clone(),
            "candidate_recovery_source",
            ScopeInstanceId::root(),
            NodeId::new("analyze_v1").unwrap(),
            "root:analyze",
            source.clone(),
            activation_id,
            provenance,
            target_revision.revision().definition_revision_id().clone(),
            target_revision.revision().deployment_revision_id().clone(),
            target_revision.revision().plan_hash().clone(),
            target_revision.revision().binding_hash().clone(),
            output_hash,
            EffectId::new(effect_id).unwrap(),
            ReuseCompatibility::new(
                ContentHash::from_bytes(b"config"),
                ContentHash::from_bytes(b"descriptor"),
                ContentHash::from_bytes(b"input"),
                ContentHash::from_bytes(b"output_schema"),
                ContentHash::from_bytes(b"effect_policy"),
                ContentHash::from_bytes(b"dependencies"),
            ),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn postgres_run_and_recovery_payload_reads_recompute_inline_authority() {
        let Some(repository) = isolated_repository().await else {
            return;
        };
        let version = plan("payload_authority_postgres");

        let completed = install_run(
            &repository,
            &version,
            "run_payload_authority_postgres_completed",
            json!({"input": "completed"}),
        )
        .await;
        activate(&repository, &completed).await;
        repository
            .commit_run_transition(
                key("payload_authority_postgres.completing"),
                RunTransitionCommand::nonterminal(
                    completed.clone(),
                    1,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    RunLifecycle::Completing,
                    AdmissionState::Draining,
                    PendingExecutionEvent::new(
                        ExecutionEventContext::for_run(completed.clone()),
                        ExecutionEventPayload::RunLifecycleChanged {
                            lifecycle: RunLifecycle::Completing,
                        },
                    )
                    .unwrap(),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let output = json!({"answer": 42});
        assert!(matches!(
            repository
                .commit_run_transition(
                    key("payload_authority_postgres.complete"),
                    RunTransitionCommand::terminal_success(
                        completed.clone(),
                        2,
                        output,
                        PendingExecutionEvent::new(
                            ExecutionEventContext::for_run(completed.clone()),
                            ExecutionEventPayload::RunLifecycleChanged {
                                lifecycle: RunLifecycle::Succeeded,
                            },
                        )
                        .unwrap(),
                        <PublicEventIntent as PublicEventIntentAdapter>::new(
                            PublicEventPayload::RunCompleted,
                        ),
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        let (output_payload_id, output_hash): (String, String) = sqlx::query_as(
            "SELECT output_payload_id,output_value_hash FROM workflow_runs WHERE run_id=$1",
        )
        .bind(completed.as_str())
        .fetch_one(repository.test_pool())
        .await
        .unwrap();
        sqlx::query("UPDATE payloads SET inline_value=$1 WHERE run_id=$2 AND payload_id=$3")
            .bind(json!({"answer": 43}))
            .bind(completed.as_str())
            .bind(&output_payload_id)
            .execute(repository.test_pool())
            .await
            .unwrap();
        assert_eq!(
            repository.load_run(&completed).await.unwrap_err().code(),
            insight_engine::repository::REPOSITORY_DATA_INVALID
        );
        let mut tx = repository.test_pool().begin().await.unwrap();
        assert_eq!(
            source_output_valid(
                &mut tx,
                &completed,
                Some(&output_payload_id),
                None,
                &ContentHash::parse(output_hash).unwrap(),
            )
            .await
            .unwrap_err()
            .code(),
            insight_engine::repository::REPOSITORY_DATA_INVALID
        );
        tx.rollback().await.unwrap();

        let source = install_run(
            &repository,
            &version,
            "run_payload_authority_postgres_source",
            json!({"report": "original"}),
        )
        .await;
        fail(&repository, &source).await;
        let source_version = repository
            .load_run(&source)
            .await
            .unwrap()
            .unwrap()
            .projection_version();
        let input_payload_id: String =
            sqlx::query_scalar("SELECT input_payload_id FROM workflow_runs WHERE run_id=$1")
                .bind(source.as_str())
                .fetch_one(repository.test_pool())
                .await
                .unwrap();
        sqlx::query("UPDATE payloads SET inline_value=$1 WHERE run_id=$2 AND payload_id=$3")
            .bind(json!({"report": "tampered"}))
            .bind(source.as_str())
            .bind(input_payload_id)
            .execute(repository.test_pool())
            .await
            .unwrap();
        let target = RunId::new("run_payload_authority_postgres_target").unwrap();
        let error = repository
            .redrive_run(
                key("payload_authority_postgres.redrive"),
                RedriveRunCommand::new(source, target.clone(), source_version, vec![]).unwrap(),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            insight_engine::repository::REPOSITORY_DATA_INVALID
        );
        assert!(
            !run_exists(&mut repository.test_pool().begin().await.unwrap(), &target,)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn postgres_redrive_and_fork_revision_contracts_are_durable() {
        let Some(repository) = isolated_repository().await else {
            return;
        };
        let v1 = plan("recovery_v1");
        let v2 = plan("recovery_v2");
        repository.install_versioned_plan(&v2).await.unwrap();
        let source = install_run(
            &repository,
            &v1,
            "run_recovery_redrive_source",
            json!({"report": "original"}),
        )
        .await;
        activate(&repository, &source).await;
        let redrive_target = RunId::new("run_recovery_redrive_target").unwrap();
        let non_worker_candidate =
            valid_candidate(&repository, &source, &redrive_target, &revision(&v1)).await;
        sqlx::query(
            "UPDATE artifacts SET artifact_state='referenced',referenced_at=CURRENT_TIMESTAMP
             WHERE run_id=$1 AND artifact_id='artifact_recovery_source'",
        )
        .bind(source.as_str())
        .execute(repository.test_pool())
        .await
        .unwrap();
        fail(&repository, &source).await;
        let redrive_key = key("redrive.apply");
        let source_projection_version = repository
            .load_run(&source)
            .await
            .unwrap()
            .unwrap()
            .projection_version();
        assert_eq!(
            repository
                .redrive_run(
                    key("redrive.non_worker_candidate.reject"),
                    RedriveRunCommand::new(
                        source.clone(),
                        redrive_target.clone(),
                        source_projection_version,
                        vec![non_worker_candidate],
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );
        let redrive = RedriveRunCommand::new(
            source.clone(),
            redrive_target.clone(),
            source_projection_version,
            vec![],
        )
        .unwrap();
        assert!(matches!(
            repository
                .redrive_run(redrive_key.clone(), redrive.clone())
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert_recovery_deadline(&repository, &redrive_target, "redrive").await;
        assert!(matches!(
            repository
                .redrive_run(redrive_key.clone(), redrive)
                .await
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        let conflict_target = RunId::new("run_recovery_redrive_intent_conflict").unwrap();
        let conflict = repository
            .redrive_run(
                redrive_key,
                RedriveRunCommand::new(
                    source.clone(),
                    conflict_target.clone(),
                    source_projection_version,
                    vec![],
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert_eq!(conflict.code(), REPOSITORY_INTENT_CONFLICT);
        assert!(!run_exists(
            &mut repository.test_pool().begin().await.unwrap(),
            &conflict_target
        )
        .await
        .unwrap());
        let row = sqlx::query(
            "SELECT definition_revision_id,deployment_revision_id,parent_run_id,lineage_kind,
                    generation,p.inline_value
             FROM workflow_runs r JOIN payloads p
               ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id
             WHERE r.run_id=$1",
        )
        .bind(redrive_target.as_str())
        .fetch_one(repository.test_pool())
        .await
        .unwrap();
        assert_eq!(
            row.get::<String, _>("definition_revision_id"),
            v1.definition_revision_id().as_str()
        );
        assert_eq!(row.get::<String, _>("parent_run_id"), source.as_str());
        assert_eq!(row.get::<String, _>("lineage_kind"), "redrive");
        assert_eq!(row.get::<i64, _>("generation"), 1);
        assert_eq!(
            row.get::<Value, _>("inline_value"),
            json!({"report": "original"})
        );
        let candidate_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM run_reuse_candidates WHERE run_id=$1")
                .bind(redrive_target.as_str())
                .fetch_one(repository.test_pool())
                .await
                .unwrap();
        assert_eq!(candidate_count, 0);
        let target_activations: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM node_activations WHERE run_id=$1")
                .bind(redrive_target.as_str())
                .fetch_one(repository.test_pool())
                .await
                .unwrap();
        assert_eq!(target_activations, 0);
        let effect_roots: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM recovery_effect_roots WHERE run_id=$1")
                .bind(redrive_target.as_str())
                .fetch_one(repository.test_pool())
                .await
                .unwrap();
        assert_eq!(effect_roots, 0);
        let artifact_roots: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM recovery_artifact_roots WHERE run_id=$1")
                .bind(redrive_target.as_str())
                .fetch_one(repository.test_pool())
                .await
                .unwrap();
        assert_eq!(artifact_roots, 0);

        let fork_target = RunId::new("run_recovery_fork_rejected").unwrap();
        let rejected = repository
            .fork_run(
                key("fork.revision.reject"),
                ForkRunCommand::new(
                    source.clone(),
                    fork_target.clone(),
                    3,
                    revision(&v2),
                    ContentHash::from_bytes(b"checkpoint"),
                    vec![],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected, TransitionOutcome::StateConflict);
        assert!(!run_exists(
            &mut repository.test_pool().begin().await.unwrap(),
            &fork_target
        )
        .await
        .unwrap());

        let forged_target = RunId::new("run_recovery_fork_forged_checkpoint").unwrap();
        assert_eq!(
            repository
                .fork_run(
                    key("fork.forged_checkpoint"),
                    ForkRunCommand::new(
                        source.clone(),
                        forged_target.clone(),
                        3,
                        revision(&v1),
                        ContentHash::from_bytes(b"forged_checkpoint"),
                        vec![],
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );
        assert!(!run_exists(
            &mut repository.test_pool().begin().await.unwrap(),
            &forged_target
        )
        .await
        .unwrap());

        let foreign_source = install_run(
            &repository,
            &v1,
            "run_recovery_foreign_checkpoint_source",
            json!({"report": "foreign"}),
        )
        .await;
        activate(&repository, &foreign_source).await;
        let foreign_checkpoint =
            install_scheduler_checkpoint(&repository, &foreign_source, 1, "foreign_source", true)
                .await;
        let foreign_target = RunId::new("run_recovery_fork_foreign_checkpoint").unwrap();
        assert_eq!(
            repository
                .fork_run(
                    key("fork.foreign_checkpoint"),
                    ForkRunCommand::new(
                        source.clone(),
                        foreign_target,
                        3,
                        revision(&v1),
                        foreign_checkpoint.clone(),
                        vec![],
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );

        let foreign_row = sqlx::query(
            "SELECT checkpoint_id,checkpoint_kind,transition_key,intent_hash,event_id,
                    checkpoint_schema_version,scheduler_projection_version
             FROM scheduler_checkpoints WHERE run_id=$1 AND content_hash=$2",
        )
        .bind(foreign_source.as_str())
        .bind(foreign_checkpoint.as_str())
        .fetch_one(repository.test_pool())
        .await
        .unwrap();
        let tampered_payload = json!({"boundary": "tampered"});
        let tampered_checkpoint = scheduler_checkpoint_content_hash(
            foreign_source.as_str(),
            &foreign_row.get::<String, _>("checkpoint_id"),
            &foreign_row.get::<String, _>("checkpoint_kind"),
            &foreign_row.get::<String, _>("transition_key"),
            &foreign_row.get::<String, _>("intent_hash"),
            &foreign_row.get::<String, _>("event_id"),
            u32::try_from(foreign_row.get::<i32, _>("checkpoint_schema_version")).unwrap(),
            u64_from_i64(foreign_row.get("scheduler_projection_version")).unwrap(),
            &tampered_payload,
        )
        .unwrap();
        sqlx::query(
            "UPDATE scheduler_checkpoints SET content_hash=$1,fact_payload=$2
             WHERE run_id=$3 AND content_hash=$4",
        )
        .bind(tampered_checkpoint.as_str())
        .bind(&tampered_payload)
        .bind(foreign_source.as_str())
        .bind(foreign_checkpoint.as_str())
        .execute(repository.test_pool())
        .await
        .unwrap();
        let tampered_target = RunId::new("run_recovery_fork_tampered_checkpoint").unwrap();
        assert_eq!(
            repository
                .fork_run(
                    key("fork.tampered_checkpoint"),
                    ForkRunCommand::new(
                        foreign_source.clone(),
                        tampered_target,
                        1,
                        revision(&v1),
                        tampered_checkpoint,
                        vec![],
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );

        let unclosed_checkpoint =
            install_scheduler_checkpoint(&repository, &source, 3, "unclosed_boundary", false).await;
        let unclosed_target = RunId::new("run_recovery_fork_unclosed_checkpoint").unwrap();
        assert_eq!(
            repository
                .fork_run(
                    key("fork.unclosed_checkpoint"),
                    ForkRunCommand::new(
                        source.clone(),
                        unclosed_target,
                        3,
                        revision(&v1),
                        unclosed_checkpoint,
                        vec![],
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );

        let fork_applied_target = RunId::new("run_recovery_fork_applied").unwrap();
        let checkpoint =
            install_scheduler_checkpoint(&repository, &source, 3, "applied", true).await;
        let fork_key = key("fork.apply");
        let fork_command = ForkRunCommand::new(
            source.clone(),
            fork_applied_target.clone(),
            3,
            revision(&v1),
            checkpoint.clone(),
            vec![],
        )
        .unwrap();
        assert!(matches!(
            repository
                .fork_run(fork_key.clone(), fork_command.clone())
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert_recovery_deadline(&repository, &fork_applied_target, "fork").await;
        assert!(matches!(
            repository.fork_run(fork_key, fork_command).await.unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        let fork_row = sqlx::query(
            "SELECT lineage_kind,source_checkpoint_hash FROM run_recovery_lineage WHERE run_id=$1",
        )
        .bind(fork_applied_target.as_str())
        .fetch_one(repository.test_pool())
        .await
        .unwrap();
        assert_eq!(fork_row.get::<String, _>("lineage_kind"), "fork");
        assert_eq!(
            fork_row.get::<String, _>("source_checkpoint_hash"),
            checkpoint.as_str()
        );
    }

    #[tokio::test]
    async fn postgres_migration_unsettled_wait_requires_mapped_dual_rebuild_before_intent() {
        let Some(repository) = isolated_repository().await else {
            return;
        };
        let v1 = plan("migration_wait_v1");
        let v2 = plan("migration_wait_v2");
        repository.install_versioned_plan(&v2).await.unwrap();
        let source = install_run(
            &repository,
            &v1,
            "run_migration_wait_source",
            json!({"version": 1}),
        )
        .await;
        activate(&repository, &source).await;
        let activation_id = ActivationId::new("activation_migration_wait").unwrap();
        repository
            .admit_activation(
                key("migration.wait.admit"),
                ActivationAdmissionCommand::new(
                    source.clone(),
                    ScopeInstanceId::root(),
                    0,
                    activation_id.clone(),
                    NodeId::new("wait_v1").unwrap(),
                    "migration-wait:0",
                    ExecutionKind::DurableWait,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO scheduler_wait_registrations (
                run_id,wait_id,activation_id,node_id,occurrence_key,signal_name,signal_id,
                timer_id,due_at_ms,payload_type,winner_kind,winner_signal_id,winner_timer_id,
                projection_version,created_at,resolved_at
             ) VALUES ($1,$2,$3,'wait_v1',$4,'resume','signal_migration_wait',NULL,NULL,NULL,
                       NULL,NULL,NULL,0,CURRENT_TIMESTAMP,NULL)",
        )
        .bind(source.as_str())
        .bind("wait_migration")
        .bind(activation_id.as_str())
        .bind(serde_json::json!({"segments": []}))
        .execute(repository.test_pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE scope_instances SET settled_children=admitted_children
             WHERE run_id=$1 AND scope_instance_id='root'",
        )
        .bind(source.as_str())
        .execute(repository.test_pool())
        .await
        .unwrap();
        let active_version = repository
            .load_run(&source)
            .await
            .unwrap()
            .unwrap()
            .projection_version();
        pause(&repository, &source, active_version).await;
        let paused_version = repository
            .load_run(&source)
            .await
            .unwrap()
            .unwrap()
            .projection_version();

        for (label, mappings) in [
            ("missing", vec![]),
            ("one_sided", vec![wait_mapping(false, true)]),
        ] {
            let target = RunId::new(format!("run_migration_wait_target_{label}")).unwrap();
            let outcome = repository
                .begin_migration(
                    key(&format!("migration.wait.{label}")),
                    BeginMigrationCommand::new(
                        source.clone(),
                        target,
                        paused_version,
                        revision(&v2),
                        json!({"version": 2}),
                        mappings,
                        vec![],
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(outcome, TransitionOutcome::StateConflict);
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM run_migration_intents WHERE run_id=$1",
            )
            .bind(source.as_str())
            .fetch_one(repository.test_pool())
            .await
            .unwrap(),
            0,
            "rebuild validation must run before durable migration intent creation"
        );

        let target = RunId::new("run_migration_wait_target_valid").unwrap();
        assert!(matches!(
            repository
                .begin_migration(
                    key("migration.wait.valid"),
                    BeginMigrationCommand::new(
                        source.clone(),
                        target,
                        paused_version,
                        revision(&v2),
                        json!({"version": 2}),
                        vec![wait_mapping(true, true)],
                        vec![],
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
    }

    #[test]
    fn postgres_recovery_rederives_map_item_non_root_scope() {
        super::super::recovery_repository::dynamic_scope_test_contract::assert_map_item_scope_rederivation();
    }

    #[tokio::test]
    async fn postgres_fresh_migration_is_fenced_by_the_durable_target_head() {
        let Some(repository) = isolated_repository().await else {
            return;
        };
        let source_plan = plan("migration_head_source");
        let target_old = publication_revision("migration_head_target", "migration_head_old");
        let target_new = publication_revision("migration_head_target", "migration_head_new");
        let source = install_run(
            &repository,
            &source_plan,
            "run_migration_head_source",
            json!({"source": true}),
        )
        .await;
        activate(&repository, &source).await;
        pause(&repository, &source, 1).await;

        repository
            .publish_versioned_plan(
                PublishVersionedPlanCommand::new(target_old.clone(), vec![]).unwrap(),
            )
            .await
            .unwrap();
        let old_head = PublicationHead::new(
            target_old.agent_id().to_owned(),
            target_old.definition_id().to_owned(),
            target_old.definition_revision_id().clone(),
            target_old.deployment_revision_id().clone(),
            PublicationOrigin::Graph,
        )
        .unwrap();
        let target = RunId::new("run_migration_head_target").unwrap();
        let stale = BeginMigrationCommand::new(
            source.clone(),
            target.clone(),
            2,
            revision(&target_old),
            json!({"target": "old"}),
            vec![],
            vec![],
        )
        .unwrap()
        .with_expected_target_publication_head(old_head)
        .unwrap();

        repository
            .publish_versioned_plan(
                PublishVersionedPlanCommand::new(target_new.clone(), vec![]).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            repository
                .begin_migration(key("migration.head.stale"), stale)
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );
        let intent_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM run_migration_intents WHERE run_id=$1")
                .bind(source.as_str())
                .fetch_one(repository.test_pool())
                .await
                .unwrap();
        assert_eq!(intent_count, 0);

        let new_head = PublicationHead::new(
            target_new.agent_id().to_owned(),
            target_new.definition_id().to_owned(),
            target_new.definition_revision_id().clone(),
            target_new.deployment_revision_id().clone(),
            PublicationOrigin::Graph,
        )
        .unwrap();
        let fresh_key = key("migration.head.fresh");
        let fresh = BeginMigrationCommand::new(
            source.clone(),
            target,
            2,
            revision(&target_new),
            json!({"target": "new"}),
            vec![],
            vec![],
        )
        .unwrap()
        .with_expected_target_publication_head(new_head)
        .unwrap();
        assert!(matches!(
            repository
                .begin_migration(fresh_key.clone(), fresh.clone())
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert_eq!(
            repository
                .load_pending_migration_intent(&source, &fresh_key)
                .await
                .unwrap(),
            Some(fresh)
        );
    }

    #[tokio::test]
    async fn postgres_migration_is_ready_fenced_signal_safe_and_atomic() {
        assert_eq!(
            mapping(b"schema_v1", b"schema_v2").unwrap_err().code(),
            insight_engine::RECOVERY_MIGRATION_SCHEMA_INCOMPATIBLE
        );
        let Some(repository) = isolated_repository().await else {
            return;
        };
        let v1 = plan("migration_v1");
        let v2 = plan("migration_v2");
        repository.install_versioned_plan(&v2).await.unwrap();
        let source = install_run(
            &repository,
            &v1,
            "run_migration_source",
            json!({"version": 1}),
        )
        .await;
        activate(&repository, &source).await;
        let target = RunId::new("run_migration_target").unwrap();
        let not_ready = BeginMigrationCommand::new(
            source.clone(),
            target.clone(),
            1,
            revision(&v2),
            json!({"version": 2}),
            vec![mapping(b"schema", b"schema").unwrap()],
            vec![],
        )
        .unwrap();
        assert_eq!(
            repository
                .begin_migration(key("migration.not_ready"), not_ready)
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );
        let scheduler_lease = repository
            .claim_scheduler_run(
                key("migration.scheduler.lease"),
                ClaimSchedulerRunCommand::new(source.clone(), "scheduler-before-migration", 60)
                    .unwrap(),
            )
            .await
            .unwrap()
            .committed_result()
            .unwrap()
            .clone();
        pause(&repository, &source, 1).await;
        let intent_key = key("migration.intent");
        let intent = repository
            .begin_migration(
                intent_key.clone(),
                BeginMigrationCommand::new(
                    source.clone(),
                    target.clone(),
                    2,
                    revision(&v2),
                    json!({"version": 2}),
                    vec![mapping(b"schema", b"schema").unwrap()],
                    vec![],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let intent = intent.committed_result().unwrap().clone();
        let fence_row = sqlx::query(
            "SELECT scheduler_lease_epoch,scheduler_lease_owner
             FROM workflow_runs WHERE run_id=$1",
        )
        .bind(source.as_str())
        .fetch_one(repository.test_pool())
        .await
        .unwrap();
        assert!(
            u64::try_from(fence_row.get::<i64, _>("scheduler_lease_epoch")).unwrap()
                > scheduler_lease.lease_epoch()
        );
        assert!(fence_row
            .get::<Option<String>, _>("scheduler_lease_owner")
            .is_none());
        assert_eq!(
            repository
                .claim_scheduler_run(
                    key("migration.scheduler.reclaim.rejected"),
                    ClaimSchedulerRunCommand::new(source.clone(), "scheduler-after-migration", 60,)
                        .unwrap(),
                )
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );
        let signal_error = repository
            .receive_signal(
                ReceiveSignalCommand::new(
                    source.clone(),
                    SignalId::new("signal_during_migration").unwrap(),
                    "message_during_migration",
                    "resume",
                    ActivationId::new("missing_wait").unwrap(),
                    json!({"unsafe": true}),
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert_eq!(signal_error.code(), REPOSITORY_RUN_MIGRATING);
        let finalized = repository
            .finalize_migration(
                key("migration.finalize"),
                FinalizeMigrationCommand::new(
                    source.clone(),
                    target.clone(),
                    intent.source_event().projection_version(),
                    intent_key,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(finalized, TransitionOutcome::Committed { .. }));
        assert_recovery_deadline(&repository, &target, "migrate").await;
        let source_row = sqlx::query(
            "SELECT lifecycle,error_code,replacement_run_id FROM workflow_runs WHERE run_id=$1",
        )
        .bind(source.as_str())
        .fetch_one(repository.test_pool())
        .await
        .unwrap();
        assert_eq!(source_row.get::<String, _>("lifecycle"), "cancelled");
        assert_eq!(source_row.get::<String, _>("error_code"), "MIGRATED");
        assert_eq!(
            source_row.get::<String, _>("replacement_run_id"),
            target.as_str()
        );
        let roots: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM recovery_revision_roots WHERE run_id=$1")
                .bind(target.as_str())
                .fetch_one(repository.test_pool())
                .await
                .unwrap();
        assert_eq!(roots, 2);

        let source_atomic = install_run(
            &repository,
            &v1,
            "run_migration_atomic_source",
            json!({"version": 1}),
        )
        .await;
        activate(&repository, &source_atomic).await;
        pause(&repository, &source_atomic, 1).await;
        let target_atomic = RunId::new("run_migration_atomic_target").unwrap();
        let target_revision = revision(&v2);
        let atomic_intent_key = key("migration.atomic.intent");
        let atomic_intent = repository
            .begin_migration(
                atomic_intent_key.clone(),
                BeginMigrationCommand::new(
                    source_atomic.clone(),
                    target_atomic.clone(),
                    2,
                    target_revision.clone(),
                    json!({"version": 2}),
                    vec![mapping(b"schema", b"schema").unwrap()],
                    vec![invalid_candidate(
                        &source_atomic,
                        &target_atomic,
                        &target_revision,
                    )],
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .committed_result()
            .unwrap()
            .clone();
        let finalize_key = key("migration.atomic.finalize");
        let finalize = FinalizeMigrationCommand::new(
            source_atomic.clone(),
            target_atomic.clone(),
            atomic_intent.source_event().projection_version(),
            atomic_intent_key,
        )
        .unwrap();
        assert_eq!(
            repository
                .finalize_migration(finalize_key.clone(), finalize.clone())
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );
        assert_eq!(
            repository
                .finalize_migration(finalize_key, finalize)
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );
        let target_exists: Option<i32> =
            sqlx::query_scalar("SELECT 1 FROM workflow_runs WHERE run_id=$1")
                .bind(target_atomic.as_str())
                .fetch_optional(repository.test_pool())
                .await
                .unwrap();
        assert!(target_exists.is_none());
        let atomic_source = sqlx::query(
            "SELECT lifecycle,termination_intent_reason FROM workflow_runs WHERE run_id=$1",
        )
        .bind(source_atomic.as_str())
        .fetch_one(repository.test_pool())
        .await
        .unwrap();
        assert_eq!(atomic_source.get::<String, _>("lifecycle"), "active");
        assert_eq!(
            atomic_source.get::<String, _>("termination_intent_reason"),
            "migrated"
        );
    }

    #[tokio::test]
    async fn postgres_cancel_and_success_race_has_one_durable_first_winner() {
        let Some(repository) = isolated_repository().await else {
            return;
        };
        let plan = plan("cancel_success_race");

        for (suffix, completion_queues_first) in
            [("completion_first", true), ("cancel_first", false)]
        {
            let run_id = install_run(
                &repository,
                &plan,
                &format!("run_{suffix}"),
                json!({"race": suffix}),
            )
            .await;
            activate(&repository, &run_id).await;

            let completion_key = key(&format!("{suffix}.completion.claim"));
            let completion = RunTransitionCommand::nonterminal(
                run_id.clone(),
                1,
                RunLifecycle::Active,
                AdmissionState::Open,
                RunLifecycle::Completing,
                AdmissionState::Draining,
                PendingExecutionEvent::new(
                    ExecutionEventContext::for_run(run_id.clone()),
                    ExecutionEventPayload::RunLifecycleChanged {
                        lifecycle: RunLifecycle::Completing,
                    },
                )
                .unwrap(),
                None,
            )
            .unwrap();
            let cancel_key = key(&format!("{suffix}.cancel.claim"));
            let cancel = RunTransitionCommand::termination_intent(
                run_id.clone(),
                1,
                RunLifecycle::Active,
                AdmissionState::Open,
                TerminationReason::Cancelled,
                PendingExecutionEvent::new(
                    ExecutionEventContext::for_run(run_id.clone()),
                    ExecutionEventPayload::RunTerminationClaimed {
                        reason: TerminationReason::Cancelled,
                    },
                )
                .unwrap(),
                None,
            )
            .unwrap();

            // Hold the Run row while two independent repository transactions
            // queue for the same FOR UPDATE lock. Queue order deliberately
            // covers both legal first-winner outcomes without making the
            // transition calls sequential.
            let mut blocker = repository.test_pool().begin().await.unwrap();
            sqlx::query("SELECT run_id FROM workflow_runs WHERE run_id=$1 FOR UPDATE")
                .bind(run_id.as_str())
                .fetch_one(&mut *blocker)
                .await
                .unwrap();

            let spawn_completion = || {
                let repository = repository.clone();
                let transition_key = completion_key.clone();
                let command = completion.clone();
                tokio::spawn(async move {
                    repository
                        .commit_run_transition(transition_key, command)
                        .await
                        .unwrap()
                })
            };
            let spawn_cancel = || {
                let repository = repository.clone();
                let transition_key = cancel_key.clone();
                let command = cancel.clone();
                tokio::spawn(async move {
                    repository
                        .commit_run_transition(transition_key, command)
                        .await
                        .unwrap()
                })
            };
            let (completion_task, cancel_task) = if completion_queues_first {
                let first = spawn_completion();
                tokio::time::sleep(Duration::from_millis(100)).await;
                let second = spawn_cancel();
                (first, second)
            } else {
                let first = spawn_cancel();
                tokio::time::sleep(Duration::from_millis(100)).await;
                let second = spawn_completion();
                (second, first)
            };
            tokio::time::sleep(Duration::from_millis(100)).await;
            blocker.commit().await.unwrap();

            let completion_outcome = completion_task.await.unwrap();
            let cancel_outcome = cancel_task.await.unwrap();
            if completion_queues_first {
                assert!(matches!(
                    completion_outcome,
                    TransitionOutcome::Committed { .. }
                ));
                assert_eq!(cancel_outcome, TransitionOutcome::StateConflict);
                assert!(matches!(
                    repository
                        .commit_run_transition(completion_key.clone(), completion.clone())
                        .await
                        .unwrap(),
                    TransitionOutcome::ExactReplay { .. }
                ));
                assert_eq!(
                    repository
                        .commit_run_transition(cancel_key.clone(), cancel.clone())
                        .await
                        .unwrap(),
                    TransitionOutcome::StateConflict
                );

                assert!(matches!(
                    repository
                        .commit_run_transition(
                            key(&format!("{suffix}.succeeded")),
                            RunTransitionCommand::terminal_success(
                                run_id.clone(),
                                2,
                                json!({"winner": "success"}),
                                PendingExecutionEvent::new(
                                    ExecutionEventContext::for_run(run_id.clone()),
                                    ExecutionEventPayload::RunLifecycleChanged {
                                        lifecycle: RunLifecycle::Succeeded,
                                    },
                                )
                                .unwrap(),
                                <PublicEventIntent as PublicEventIntentAdapter>::new(
                                    PublicEventPayload::RunCompleted,
                                ),
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap(),
                    TransitionOutcome::Committed { .. }
                ));
            } else {
                assert_eq!(completion_outcome, TransitionOutcome::StateConflict);
                assert!(matches!(
                    cancel_outcome,
                    TransitionOutcome::Committed { .. }
                ));
                assert!(matches!(
                    repository
                        .commit_run_transition(cancel_key.clone(), cancel.clone())
                        .await
                        .unwrap(),
                    TransitionOutcome::ExactReplay { .. }
                ));
                assert_eq!(
                    repository
                        .commit_run_transition(completion_key.clone(), completion.clone())
                        .await
                        .unwrap(),
                    TransitionOutcome::StateConflict
                );

                let failure = PublicFailureSummary {
                    kind: PublicFailureKind::Stop,
                    code: PublicErrorCode::new("SCHEDULER_CANCELLED").unwrap(),
                };
                assert!(matches!(
                    repository
                        .commit_run_transition(
                            key(&format!("{suffix}.cancelled")),
                            RunTransitionCommand::terminal_failure(
                                run_id.clone(),
                                2,
                                TerminationReason::Cancelled,
                                PublicErrorCode::new("SCHEDULER_CANCELLED").unwrap(),
                                PendingExecutionEvent::new(
                                    ExecutionEventContext::for_run(run_id.clone()),
                                    ExecutionEventPayload::RunLifecycleChanged {
                                        lifecycle: RunLifecycle::Cancelled,
                                    },
                                )
                                .unwrap(),
                                <PublicEventIntent as PublicEventIntentAdapter>::new(
                                    PublicEventPayload::RunCancelled { failure }
                                ),
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap(),
                    TransitionOutcome::Committed { .. }
                ));
            }

            let row = sqlx::query_as::<_, (String, i64, i64)>(
                "SELECT lifecycle,
                        (SELECT COUNT(*) FROM execution_events
                         WHERE run_id=$1 AND transition_key IN ($2,$3)),
                        (SELECT COUNT(*) FROM public_event_outbox
                         WHERE run_id=$1 AND is_terminal=TRUE)
                 FROM workflow_runs WHERE run_id=$1",
            )
            .bind(run_id.as_str())
            .bind(completion_key.as_str())
            .bind(cancel_key.as_str())
            .fetch_one(repository.test_pool())
            .await
            .unwrap();
            assert_eq!(
                row,
                (
                    if completion_queues_first {
                        "succeeded".to_owned()
                    } else {
                        "cancelled".to_owned()
                    },
                    1,
                    1,
                )
            );
        }
    }

    #[tokio::test]
    async fn postgres_continue_as_new_increments_generation_atomically() {
        let Some(repository) = isolated_repository().await else {
            return;
        };
        let plan = plan("continue_v1");
        let source = install_run(
            &repository,
            &plan,
            "run_continue_source",
            json!({"generation": 1}),
        )
        .await;
        activate(&repository, &source).await;
        pause(&repository, &source, 1).await;
        let target = RunId::new("run_continue_target").unwrap();
        let result = repository
            .continue_as_new(
                key("continue.apply"),
                ContinueAsNewCommand::new(
                    source.clone(),
                    target.clone(),
                    2,
                    json!({"generation": 2}),
                    vec![],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let receipt = result.committed_result().unwrap();
        assert_recovery_deadline(&repository, &target, "generation").await;
        assert_eq!(receipt.lineage().source_generation().get(), 1);
        assert_eq!(receipt.lineage().target_generation().get(), 2);
        let target_row = sqlx::query(
            "SELECT generation,lineage_kind,parent_run_id FROM workflow_runs WHERE run_id=$1",
        )
        .bind(target.as_str())
        .fetch_one(repository.test_pool())
        .await
        .unwrap();
        assert_eq!(target_row.get::<i64, _>("generation"), 2);
        assert_eq!(target_row.get::<String, _>("lineage_kind"), "generation");
        assert_eq!(
            target_row.get::<String, _>("parent_run_id"),
            source.as_str()
        );
    }
}
