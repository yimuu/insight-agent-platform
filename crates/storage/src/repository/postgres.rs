use super::RepositoryErrorExt as _;

use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    PgPool, Postgres, Row, Transaction,
};

use insight_durable::common::adapter::{
    self as common_contract_adapter, build_terminal_response_snapshot, canonical_intent_hash,
    canonical_value, decode_public_projection_decision, decode_stored_execution_event,
    durable_public_event_envelope, event_id, i64_from_u64, parse_admission_state,
    parse_run_lifecycle, payload_id, project_terminal_tool_results, public_event_id,
    public_event_ordinal, u64_from_i64, validate_inline_payload, StoredExecutionEventRow,
    StoredModelCallUsage, StoredPublicProjectionDecision, StoredResponseItem,
    StoredSucceededModelToolCall, TerminalResponseSnapshotInput,
};
use insight_durable::model::adapter as model_adapter;
use insight_durable::production::adapter as production_contract_adapter;
use insight_durable::{
    ClaimSchedulerRunCommand, FencedSchedulerRunCommand, PendingMigrationWait,
    ProductionRunRepository, RunRepositoryCapability, SchedulerLeaseRepository,
};
use insight_engine::response::adapter::{
    durable_response_snapshot_new, response_terminal_kind_parse, response_usage_status_parse,
};

use insight_engine::{
    ContentHash, DefinitionRevisionId, DeploymentRevisionId, ExecutionEventContext,
    ExecutionEventPayload, NodeId, PendingExecutionEvent, RunId, RunLifecycle, TransitionKey,
    TransitionOutcome, EXECUTION_EVENT_SCHEMA_VERSION,
};

use super::model::{
    CommitReceiptAdapter as _, CreateRunCommandAdapter as _, PublicEventIntentAdapter as _,
    PublicRunAttachment, PublicRunAttachmentAdapter as _, PublicationHeadAdapter as _,
    PublicationOriginAdapter as _, RunProjectionAdapter as _, RunTransitionCommandAdapter as _,
    VersionedPlanAdapter as _, VersionedPlanCatalogAdapter as _,
};
use super::postgres_projection::{
    finalize_projection_checkpoints, verify_projection_checkpoint_batch,
};
use super::schema_contract::{validate_contract_row, POSTGRES_SCHEMA_BACKEND};
use super::{
    CommitReceipt, CreateRunCommand, DurableRepository, DurableResponseSnapshot,
    PlanInstallOutcome, PlanPublicationOutcome, PublicationHead, PublicationOrigin,
    PublishVersionedPlanCommand, RepositoryError, RunProjection, RunTransitionCommand,
    VersionedPlan, VersionedPlanCatalog, REPOSITORY_PLAN_CONFLICT, REPOSITORY_RUN_NOT_FOUND,
};

fn plan_conflict() -> RepositoryError {
    RepositoryError::new(
        REPOSITORY_PLAN_CONFLICT,
        "immutable plan revision conflicts with durable authority",
    )
}

pub(crate) fn run_not_found() -> RepositoryError {
    RepositoryError::new(
        REPOSITORY_RUN_NOT_FOUND,
        "durable workflow run was not found",
    )
}

fn postgres_schema_contract_read_error(error: sqlx::Error) -> RepositoryError {
    let missing_contract_object = error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .is_some_and(|code| matches!(code.as_ref(), "42P01" | "42703"));
    if missing_contract_object {
        RepositoryError::schema_not_initialized()
    } else {
        RepositoryError::storage(error)
    }
}

/// Acquire the per-Run event-writer authority before any mutable projection
/// row is locked. Every PostgreSQL transaction that can allocate an execution
/// event sequence must use this order, otherwise two paths can form a cycle by
/// holding a projection row while waiting to update `workflow_runs`.
pub(crate) async fn lock_run_for_event_write(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<(), RepositoryError> {
    let _exists: Option<i32> =
        sqlx::query_scalar("SELECT 1 FROM workflow_runs WHERE run_id = $1 FOR UPDATE")
            .bind(run_id.as_str())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
    // Absence is intentionally left to the caller's existing aggregate
    // lookup/CAS semantics (StateConflict, StaleLease, or RunNotFound). A
    // missing Run cannot have FK-backed mutable projections to lock.
    Ok(())
}

/// Acquire event-writer authority for a multi-Run transaction in stable RunId
/// order. Sorting and de-duplicating here prevents parent/child, fork/redrive,
/// and migration transactions from deadlocking one another in reverse order.
pub(crate) async fn lock_runs_for_event_write(
    transaction: &mut Transaction<'_, Postgres>,
    run_ids: &[&RunId],
) -> Result<(), RepositoryError> {
    let mut ordered = run_ids.to_vec();
    ordered.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
    ordered.dedup_by(|left, right| left.as_str() == right.as_str());
    for run_id in ordered {
        lock_run_for_event_write(transaction, run_id).await?;
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) enum Replay {
    Vacant,
    Exact(CommitReceipt),
}

#[derive(Debug)]
struct CurrentRunRow {
    lifecycle: String,
    admission_state: String,
    projection_version: i64,
    termination_intent_reason: Option<String>,
}

/// PostgreSQL production repository.
///
/// Every Run transition is serialized by a row lock; correctness additionally
/// relies on the Schema's composite foreign keys and transition uniqueness.
#[derive(Clone)]
pub struct PostgresDurableRepository {
    pub(crate) pool: PgPool,
}

impl PostgresDurableRepository {
    /// Explicitly provisions an empty, isolated PostgreSQL target for unit
    /// tests before applying the normal read-only constructor gate.
    #[cfg(test)]
    pub(crate) async fn connect_provisioned_for_test(
        database_url: &str,
    ) -> Result<Self, RepositoryError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .map_err(RepositoryError::storage)?;
        super::schema_contract::provision_postgres_for_test(&pool).await;
        Self::from_pool(pool).await
    }

    /// Wraps an existing runtime pool after validating its pre-provisioned
    /// durable Schema. The pool is never used to install or repair objects.
    pub async fn from_pool(pool: PgPool) -> Result<Self, RepositoryError> {
        let repository = Self { pool };
        repository.validate_schema_contract().await?;
        Ok(repository)
    }

    pub async fn connect(database_url: &str) -> Result<Self, RepositoryError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .map_err(RepositoryError::storage)?;
        Self::from_pool(pool).await
    }

    /// Shares the configured PostgreSQL endpoint with infrastructure that is
    /// explicitly non-durable (for example LISTEN/NOTIFY live responses).
    /// Cloning a `PgPool` does not duplicate credentials into application
    /// payloads and preserves one startup/readiness authority.
    pub fn connection_pool(&self) -> PgPool {
        self.pool.clone()
    }

    /// Performs the bounded, read-only startup gate for the durable Schema.
    pub async fn validate_schema_contract(&self) -> Result<(), RepositoryError> {
        let row = sqlx::query_as::<_, (String, String)>(
            "SELECT contract_id,backend
             FROM durable_schema_contract
             WHERE singleton=$1",
        )
        .bind(1_i32)
        .fetch_optional(&self.pool)
        .await
        .map_err(postgres_schema_contract_read_error)?;
        validate_contract_row(row, POSTGRES_SCHEMA_BACKEND)
    }

    pub async fn check_health(&self) -> Result<(), RepositoryError> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(RepositoryError::storage)?;
        Ok(())
    }

    #[cfg(test)]
    pub fn test_pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl ProductionRunRepository for PostgresDurableRepository {
    fn run_repository_capability(&self) -> RunRepositoryCapability {
        RunRepositoryCapability::Production
    }

    async fn check_production_health(&self) -> Result<(), RepositoryError> {
        self.check_health().await
    }

    async fn load_production_run_input(
        &self,
        run_id: &RunId,
    ) -> Result<Option<Value>, RepositoryError> {
        let row = sqlx::query(
            "SELECT p.encoding,p.inline_value FROM workflow_runs r
             JOIN payloads p ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id
             WHERE r.run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| {
            if row
                .try_get::<String, _>("encoding")
                .map_err(|_| RepositoryError::invalid_data())?
                != "json_jcs"
            {
                return Err(RepositoryError::invalid_data());
            }
            row.try_get::<Value, _>("inline_value")
                .map_err(|_| RepositoryError::invalid_data())
        })
        .transpose()
    }

    async fn claim_production_scheduler_run(
        &self,
        transition_key: TransitionKey,
        command: ClaimSchedulerRunCommand,
    ) -> Result<Option<FencedSchedulerRunCommand>, RepositoryError> {
        match SchedulerLeaseRepository::claim_scheduler_run(self, transition_key, command).await? {
            TransitionOutcome::Committed { result } => Ok(Some(result.fence()?)),
            TransitionOutcome::ExactReplay { authoritative } => Ok(Some(authoritative.fence()?)),
            TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => Ok(None),
        }
    }

    async fn release_production_scheduler_run(
        &self,
        transition_key: TransitionKey,
        fence: FencedSchedulerRunCommand,
    ) -> Result<(), RepositoryError> {
        let _ =
            SchedulerLeaseRepository::release_scheduler_run(self, transition_key, fence).await?;
        Ok(())
    }

    async fn load_pending_migration_waits(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<PendingMigrationWait>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT w.node_id AS wait_node_id,a.node_id AS activation_node_id,
                    w.signal_id,w.timer_id
             FROM scheduler_wait_registrations w
             JOIN node_activations a
               ON a.run_id=w.run_id AND a.activation_id=w.activation_id
             WHERE w.run_id=$1 AND w.winner_kind IS NULL
             ORDER BY w.node_id,w.wait_id",
        )
        .bind(run_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        rows.into_iter()
            .map(|row| {
                let wait_node = row
                    .try_get::<String, _>("wait_node_id")
                    .map_err(|_| RepositoryError::invalid_data())?;
                if wait_node
                    != row
                        .try_get::<String, _>("activation_node_id")
                        .map_err(|_| RepositoryError::invalid_data())?
                {
                    return Err(RepositoryError::invalid_data());
                }
                production_contract_adapter::pending_migration_wait(
                    NodeId::new(wait_node).map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get::<Option<String>, _>("signal_id")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .is_some(),
                    row.try_get::<Option<String>, _>("timer_id")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .is_some(),
                )
            })
            .collect()
    }
}

#[async_trait]
impl DurableRepository for PostgresDurableRepository {
    async fn install_versioned_plan(
        &self,
        plan: &VersionedPlan,
    ) -> Result<PlanInstallOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let installed = install_plan(&mut transaction, plan).await?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(if installed {
            PlanInstallOutcome::Installed
        } else {
            PlanInstallOutcome::AlreadyInstalled
        })
    }

    async fn publish_versioned_plan(
        &self,
        command: PublishVersionedPlanCommand,
    ) -> Result<PlanPublicationOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if !lock_and_match_publication_heads(&mut transaction, &command).await? {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(PlanPublicationOutcome::DependencyHeadChanged);
        }
        let installed = install_plan(&mut transaction, command.plan()).await?;
        upsert_publication_head(&mut transaction, command.plan()).await?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(PlanPublicationOutcome::Published(if installed {
            PlanInstallOutcome::Installed
        } else {
            PlanInstallOutcome::AlreadyInstalled
        }))
    }

    async fn publish_builtin_versioned_plan(
        &self,
        plan: &VersionedPlan,
    ) -> Result<PublicationHead, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        install_plan(&mut transaction, plan).await?;
        sqlx::query(
            "INSERT INTO agent_publication_heads (
                agent_id,definition_id,definition_revision_id,deployment_revision_id,
                publication_origin,updated_at
             ) VALUES ($1,$2,$3,$4,'built_in',CURRENT_TIMESTAMP)
             ON CONFLICT (agent_id) DO UPDATE SET
                definition_id=EXCLUDED.definition_id,
                definition_revision_id=EXCLUDED.definition_revision_id,
                deployment_revision_id=EXCLUDED.deployment_revision_id,
                publication_origin='built_in',
                updated_at=CURRENT_TIMESTAMP
             WHERE agent_publication_heads.publication_origin='built_in'",
        )
        .bind(plan.agent_id())
        .bind(plan.definition_id())
        .bind(plan.definition_revision_id().as_str())
        .bind(plan.deployment_revision_id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let row = sqlx::query(
            "SELECT agent_id,definition_id,definition_revision_id,deployment_revision_id,
                    publication_origin
             FROM agent_publication_heads WHERE agent_id=$1",
        )
        .bind(plan.agent_id())
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let head = postgres_publication_head(&row)?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(head)
    }

    async fn load_versioned_plan_catalog(&self) -> Result<VersionedPlanCatalog, RepositoryError> {
        load_postgres_versioned_plan_catalog(&self.pool).await
    }

    async fn create_run(
        &self,
        transition_key: TransitionKey,
        command: CreateRunCommand,
    ) -> Result<TransitionOutcome<CommitReceipt>, RepositoryError> {
        let intent_hash = canonical_intent_hash(&command)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        match load_replay(
            &mut transaction,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            Replay::Exact(receipt) => {
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

        if let Some(expected) = command.expected_publication_head() {
            // Hold a row lock through Run insertion. A concurrent publication
            // must linearize either before this read (and fail the comparison)
            // or after this Run has committed with the expected immutable pin.
            let current = sqlx::query(
                "SELECT agent_id,definition_id,definition_revision_id,deployment_revision_id,
                        publication_origin
                 FROM agent_publication_heads WHERE agent_id=$1 FOR SHARE",
            )
            .bind(expected.agent_id())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .map(|row| postgres_publication_head(&row))
            .transpose()?;
            if current.as_ref() != Some(expected) {
                transaction
                    .rollback()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
        }

        let (input_json, input_hash) = canonical_value(command.input())?;
        let input_payload_id = payload_id(&input_hash);
        let run_deadline_at = if let Some(timeout_ms) = command.run_timeout_ms() {
            Some(
                sqlx::query_scalar::<_, DateTime<Utc>>(
                    "SELECT clock_timestamp() + ($1::double precision * interval '1 millisecond')",
                )
                .bind(timeout_ms as f64)
                .fetch_one(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?,
            )
        } else {
            None
        };
        let inserted = sqlx::query(
            "INSERT INTO workflow_runs (
                run_id, definition_id, definition_revision_id, deployment_revision_id,
                plan_hash, binding_hash, request_id, attachment, lifecycle, admission_state,
                termination_intent_reason, termination_intent_transition_key,
                termination_intent_at, input_payload_id, output_payload_id,
                output_artifact_id, output_value_hash, error_code, terminal_event_id,
                terminal_public_event_id, parent_run_id, lineage_kind, generation,
                replacement_run_id, next_event_seq, projection_version,
                scheduler_lease_epoch, scheduler_lease_owner,
                scheduler_lease_expires_at, scheduler_heartbeat_at,
                created_at, started_at, updated_at, terminal_at, deadline_at,
                artifact_reference_retention_seconds
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, 'created', 'open',
                NULL, NULL, NULL, $9, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, 1, NULL, 1, 0, 0, NULL, NULL, NULL,
                CURRENT_TIMESTAMP, NULL, CURRENT_TIMESTAMP, NULL, $10, $11
             ) ON CONFLICT DO NOTHING",
        )
        .bind(command.run_id().as_str())
        .bind(command.definition_id())
        .bind(command.definition_revision_id().as_str())
        .bind(command.deployment_revision_id().as_str())
        .bind(command.plan_hash().as_str())
        .bind(command.binding_hash().as_str())
        .bind(command.request_id())
        .bind(command.attachment().as_str())
        .bind(&input_payload_id)
        .bind(run_deadline_at)
        .bind(i64::from(command.artifact_reference_retention_seconds()))
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if inserted == 0 {
            let replay = load_replay(
                &mut transaction,
                command.run_id(),
                &transition_key,
                intent_hash.as_str(),
            )
            .await?;
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(match replay {
                Replay::Exact(receipt) => TransitionOutcome::ExactReplay {
                    authoritative: receipt,
                },
                Replay::Vacant => TransitionOutcome::StateConflict,
            });
        }

        let input_value: Value =
            serde_json::from_str(&input_json).map_err(|_| RepositoryError::canonicalization())?;
        sqlx::query(
            "INSERT INTO payloads (
                run_id, payload_id, content_hash, canonical_bytes, encoding,
                inline_value, binary_value, created_at, retain_until
             ) VALUES ($1, $2, $3, $4, 'json_jcs', $5, NULL, CURRENT_TIMESTAMP, NULL)",
        )
        .bind(command.run_id().as_str())
        .bind(&input_payload_id)
        .bind(input_hash.as_str())
        .bind(i64::try_from(input_json.len()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(input_value)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        sqlx::query(
            "INSERT INTO scope_instances (
                run_id, scope_instance_id, parent_scope_instance_id, static_scope_id,
                stable_dynamic_key, scope_kind, is_root, lifecycle, admission_state,
                admitted_children, settled_children, projection_version, created_at, settled_at
             ) VALUES ($1, 'scope_root', NULL, 'root', NULL, 'root', TRUE, 'active', 'open',
                       0, 0, 0, CURRENT_TIMESTAMP, NULL)",
        )
        .bind(command.run_id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;

        let event_seq = allocate_event_seq(&mut transaction, command.run_id()).await?;
        let event_id = event_id(&transition_key);
        let event = PendingExecutionEvent::new(
            ExecutionEventContext::for_run(command.run_id().clone()),
            ExecutionEventPayload::RunCreated {
                definition_revision_id: command.definition_revision_id().clone(),
                deployment_revision_id: command.deployment_revision_id().clone(),
                run_deadline_at,
            },
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let event_occurred_at = insert_event(
            &mut transaction,
            command.run_id(),
            event_seq,
            &event_id,
            &transition_key,
            intent_hash.as_str(),
            0,
            &event,
        )
        .await?;
        let public_payload = insight_engine::PublicEventPayload::RunCreated;
        let public_kind = public_payload.kind();
        let public_event_id = public_event_id(command.run_id(), &transition_key, public_kind);
        let safe_envelope = serde_json::to_value(durable_public_event_envelope(
            command.run_id(),
            &public_event_id,
            &event_id,
            event_seq,
            event_occurred_at,
            public_payload.clone(),
        )?)
        .map_err(|_| RepositoryError::canonicalization())?;
        sqlx::query(
            "INSERT INTO public_event_outbox (
                run_id, public_event_id, causation_event_id, public_ordinal, public_schema_version,
                event_kind, is_terminal, publish_state, safe_envelope, available_at,
                claimed_by, claim_token, claim_expires_at, publish_attempts, published_at,
                published_by, published_claim_token, notified_at, retain_until, created_at
             ) VALUES ($1, $2, $3, $4, 1, $5, FALSE, 'pending', $6, CURRENT_TIMESTAMP,
                       NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP)",
        )
        .bind(command.run_id().as_str())
        .bind(&public_event_id)
        .bind(&event_id)
        .bind(i32::from(public_event_ordinal(public_kind)))
        .bind(public_kind.as_str())
        .bind(safe_envelope)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        finalize_projection_checkpoints(&mut transaction, command.run_id(), &event_id).await?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed {
            result: CommitReceipt::new(event_seq, event_id, 0, Some(public_event_id)),
        })
    }

    async fn commit_run_transition(
        &self,
        transition_key: TransitionKey,
        command: RunTransitionCommand,
    ) -> Result<TransitionOutcome<CommitReceipt>, RepositoryError> {
        let intent_hash = canonical_intent_hash(&command)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;

        match load_replay(
            &mut transaction,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            Replay::Exact(receipt) => {
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

        let current = load_current_run_for_update(&mut transaction, command.run_id())
            .await?
            .ok_or_else(run_not_found)?;

        // A contender may have committed while this transaction waited for the
        // Run row lock, so exact replay is checked again before CAS state.
        match load_replay(
            &mut transaction,
            command.run_id(),
            &transition_key,
            intent_hash.as_str(),
        )
        .await?
        {
            Replay::Exact(receipt) => {
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

        let expected_version = i64_from_u64(command.expected_projection_version())?;
        if current.projection_version != expected_version
            || current.lifecycle != command.expected_lifecycle().as_str()
            || current.admission_state != command.expected_admission().as_str()
        {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        if let Some(reason) = model_adapter::run_transition_terminal_reason(&command) {
            if current.termination_intent_reason.as_deref() != Some(reason) {
                transaction
                    .rollback()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
        }

        let next_projection_version = command
            .expected_projection_version()
            .checked_add(1)
            .ok_or_else(RepositoryError::invalid_data)?;
        let event_seq = allocate_event_seq(&mut transaction, command.run_id()).await?;
        let event_id = event_id(&transition_key);
        let public_event_id = command.public_event().map(|public| {
            public_event_id(command.run_id(), &transition_key, public.payload().kind())
        });
        let terminal_output =
            if let Some(output) = model_adapter::run_transition_terminal_output(&command) {
                Some(insert_or_get_payload(&mut transaction, command.run_id(), output).await?)
            } else {
                None
            };

        let rows = update_run(
            &mut transaction,
            &transition_key,
            &command,
            expected_version,
            next_projection_version,
            &event_id,
            public_event_id.as_deref(),
            terminal_output.as_ref(),
        )
        .await?;
        if rows == 0 {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }

        let event_occurred_at = insert_event(
            &mut transaction,
            command.run_id(),
            event_seq,
            &event_id,
            &transition_key,
            intent_hash.as_str(),
            next_projection_version,
            command.event(),
        )
        .await?;
        if let (Some(public), Some(public_id)) =
            (command.public_event(), public_event_id.as_deref())
        {
            sqlx::query(
                "INSERT INTO public_event_outbox (
                    run_id, public_event_id, causation_event_id, public_ordinal,
                    public_schema_version,
                    event_kind, is_terminal, publish_state, safe_envelope, available_at,
                    claimed_by, claim_token, claim_expires_at, publish_attempts, published_at,
                    published_by, published_claim_token, notified_at, retain_until, created_at
                 ) VALUES ($1, $2, $3, $4, 1, $5, $6, 'pending', $7, CURRENT_TIMESTAMP,
                           NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP)",
            )
            .bind(command.run_id().as_str())
            .bind(public_id)
            .bind(&event_id)
            .bind(i32::from(public_event_ordinal(public.payload().kind())))
            .bind(public.event_kind())
            .bind(public.is_terminal())
            .bind(
                serde_json::to_value(durable_public_event_envelope(
                    command.run_id(),
                    public_id,
                    &event_id,
                    event_seq,
                    event_occurred_at,
                    public.payload().clone(),
                )?)
                .map_err(|_| RepositoryError::canonicalization())?,
            )
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
        }
        if command.next_lifecycle().is_terminal() {
            super::postgres_model_tool_queue::close_model_tool_work_for_terminal_run_postgres(
                &mut transaction,
                command.run_id(),
            )
            .await?;
            persist_terminal_response_snapshot_postgres(
                &mut transaction,
                command.run_id(),
                command.next_lifecycle(),
                model_adapter::run_transition_terminal_output(&command),
                model_adapter::run_transition_terminal_error_code(&command),
            )
            .await?;
            register_terminal_artifact_retention_postgres(
                &mut transaction,
                command.run_id(),
                &transition_key,
                intent_hash.as_str(),
                &event_id,
                event_seq,
            )
            .await?;
        }
        finalize_projection_checkpoints(&mut transaction, command.run_id(), &event_id).await?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed {
            result: CommitReceipt::new(
                event_seq,
                event_id,
                next_projection_version,
                public_event_id,
            ),
        })
    }

    async fn load_run(&self, run_id: &RunId) -> Result<Option<RunProjection>, RepositoryError> {
        let row = sqlx::query(
            "SELECT r.response_id,r.definition_id,d.agent_id,r.definition_revision_id,
                    r.deployment_revision_id,r.plan_hash,r.binding_hash,
                    r.request_id,r.attachment,r.lifecycle,
                    r.admission_state,r.projection_version,r.next_event_seq,
                    r.termination_intent_reason,r.error_code,r.created_at,r.started_at,
                    r.updated_at,r.terminal_at,r.deadline_at,
                    r.output_payload_id AS expected_output_payload_id,
                    r.output_value_hash AS expected_output_value_hash,
                    o.payload_id AS output_payload_id,o.content_hash AS output_content_hash,
                    o.canonical_bytes AS output_canonical_bytes,o.encoding AS output_encoding,
                    o.inline_value AS output_inline,o.binary_value AS output_binary
             FROM workflow_runs r
             JOIN workflow_definitions d ON d.definition_id=r.definition_id
             LEFT JOIN payloads o
               ON o.run_id=r.run_id AND o.payload_id=r.output_payload_id
             WHERE r.run_id = $1",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| projection_from_row(run_id, &row)).transpose()
    }

    async fn load_response_snapshot(
        &self,
        run_id: &RunId,
    ) -> Result<Option<DurableResponseSnapshot>, RepositoryError> {
        load_response_snapshot_postgres(&self.pool, run_id).await
    }
}

/// Registers the referenced-Artifact hold in the same first-winner transaction
/// as Run terminal state, the response snapshot, and terminal public outbox.
/// The caller must first build the snapshot and validate every first-class
/// public ArtifactRef. It already owns the Run row lock, preserving the global
/// Run-first order before any Artifact metadata is touched.
pub(crate) async fn register_terminal_artifact_retention_postgres(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    terminal_transition_key: &TransitionKey,
    terminal_intent_hash: &str,
    terminal_event_id: &str,
    terminal_event_seq: u64,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        "SELECT terminal_at,artifact_reference_retention_seconds
         FROM workflow_runs
         WHERE run_id=$1
           AND lifecycle IN ('succeeded','failed','cancelled','interrupted','timed_out')
           AND admission_state='closed' AND terminal_at IS NOT NULL",
    )
    .bind(run_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    let terminal_at = row
        .try_get::<DateTime<Utc>, _>("terminal_at")
        .map_err(|_| RepositoryError::invalid_data())?;
    let retention_seconds = row
        .try_get::<i64, _>("artifact_reference_retention_seconds")
        .map_err(|_| RepositoryError::invalid_data())?;
    if !(1..=315_360_000).contains(&retention_seconds) {
        return Err(RepositoryError::invalid_data());
    }
    let retain_until = sqlx::query_scalar::<_, DateTime<Utc>>(
        "SELECT $1::timestamptz + make_interval(secs => $2)",
    )
    .bind(terminal_at)
    .bind(i32::try_from(retention_seconds).map_err(|_| RepositoryError::invalid_data())?)
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let artifact_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM artifacts WHERE run_id=$1 AND artifact_state='referenced'",
    )
    .bind(run_id.as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "UPDATE artifacts SET retain_until=CASE
             WHEN retain_until IS NULL OR retain_until<$1 THEN $1 ELSE retain_until END
         WHERE run_id=$2 AND artifact_state='referenced'",
    )
    .bind(retain_until)
    .bind(run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query(
        "INSERT INTO artifact_retention_releases (
            run_id,transition_key,intent_hash,event_id,event_seq,retain_until,
            artifact_count,created_at,registration_kind
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,clock_timestamp(),'terminal_atomic')",
    )
    .bind(run_id.as_str())
    .bind(terminal_transition_key.as_str())
    .bind(terminal_intent_hash)
    .bind(terminal_event_id)
    .bind(i64_from_u64(terminal_event_seq)?)
    .bind(retain_until)
    .bind(artifact_count)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

pub(crate) async fn persist_terminal_response_snapshot_postgres(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    lifecycle: RunLifecycle,
    output: Option<&Value>,
    error_code: Option<&str>,
) -> Result<(), RepositoryError> {
    sqlx::query(
        "UPDATE response_public_items
         SET item_status='incomplete_unsealed',updated_at=clock_timestamp()
         WHERE run_id=$1 AND item_status='reserved'",
    )
    .bind(run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let item_rows = sqlx::query(
        "SELECT activation_id,attempt_no,model_call_no,item_id,output_index,node_id,
                item_kind,item_status,seal_index,safe_item
         FROM response_public_items WHERE run_id=$1 ORDER BY output_index",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let items = item_rows
        .into_iter()
        .map(|row| {
            Ok(StoredResponseItem {
                activation_id: row
                    .try_get("activation_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
                attempt_no: u32::try_from(
                    row.try_get::<i32, _>("attempt_no")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
                model_call_no: u32::try_from(
                    row.try_get::<i32, _>("model_call_no")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
                item_id: row
                    .try_get("item_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
                output_index: u32::try_from(
                    row.try_get::<i32, _>("output_index")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
                node_id: row
                    .try_get("node_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
                item_kind: row
                    .try_get("item_kind")
                    .map_err(|_| RepositoryError::invalid_data())?,
                item_status: row
                    .try_get("item_status")
                    .map_err(|_| RepositoryError::invalid_data())?,
                seal_index: row
                    .try_get::<Option<i64>, _>("seal_index")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .map(u64_from_i64)
                    .transpose()?,
                safe_item: row
                    .try_get("safe_item")
                    .map_err(|_| RepositoryError::invalid_data())?,
            })
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
    let usage_rows = sqlx::query(
        "SELECT usage,usage_complete FROM model_call_usage
         WHERE run_id=$1 ORDER BY activation_id,attempt_no,model_call_no",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let model_calls = usage_rows
        .into_iter()
        .map(|row| {
            Ok(StoredModelCallUsage {
                usage: row
                    .try_get("usage")
                    .map_err(|_| RepositoryError::invalid_data())?,
                usage_complete: row
                    .try_get("usage_complete")
                    .map_err(|_| RepositoryError::invalid_data())?,
            })
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
    let tool_results = load_terminal_tool_results_postgres(transaction, run_id).await?;
    validate_terminal_tool_result_artifacts_postgres(transaction, run_id, &tool_results).await?;
    let retrievals = super::postgres_retrieval_publication::load_terminal_retrievals_postgres(
        transaction,
        run_id,
    )
    .await?;
    validate_terminal_retrieval_artifacts_postgres(transaction, run_id, &retrievals).await?;
    let snapshot = build_terminal_response_snapshot(TerminalResponseSnapshotInput {
        run_id,
        lifecycle,
        output,
        error_code,
        items,
        model_calls,
        tool_results,
        retrievals,
    })?;
    sqlx::query(
        "INSERT INTO response_snapshots (
            run_id,response_id,terminal_kind,response_status,response_payload,
            workflow_payload,public_item_manifest,usage,usage_status,snapshot_hash,created_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,clock_timestamp())",
    )
    .bind(run_id.as_str())
    .bind(&snapshot.response_id)
    .bind(snapshot.terminal_kind.as_str())
    .bind(snapshot.response_status)
    .bind(snapshot.response)
    .bind(snapshot.workflow)
    .bind(snapshot.manifest)
    .bind(snapshot.usage)
    .bind(snapshot.usage_status.as_str())
    .bind(snapshot.snapshot_hash.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

async fn load_terminal_tool_results_postgres(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<Vec<insight_engine::response::WorkflowToolResult>, RepositoryError> {
    let rows = sqlx::query(
        "SELECT activation_id,attempt_no,model_call_no,call_index,call_id,tool_name,
                effective_public_policy,result_json
         FROM model_tool_calls
         WHERE run_id=$1 AND call_status='succeeded'
         ORDER BY activation_id COLLATE \"C\",attempt_no,model_call_no,call_index",
    )
    .bind(run_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let calls = rows
        .into_iter()
        .map(|row| {
            Ok(StoredSucceededModelToolCall {
                activation_id: row
                    .try_get("activation_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
                attempt_no: u32::try_from(
                    row.try_get::<i32, _>("attempt_no")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
                model_call_no: u32::try_from(
                    row.try_get::<i32, _>("model_call_no")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
                call_index: u32::try_from(
                    row.try_get::<i32, _>("call_index")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
                call_id: row
                    .try_get("call_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
                tool_name: row
                    .try_get("tool_name")
                    .map_err(|_| RepositoryError::invalid_data())?,
                effective_public_policy: row
                    .try_get("effective_public_policy")
                    .map_err(|_| RepositoryError::invalid_data())?,
                completed_result: row
                    .try_get("result_json")
                    .map_err(|_| RepositoryError::invalid_data())?,
            })
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
    project_terminal_tool_results(calls)
}

async fn validate_terminal_tool_result_artifacts_postgres(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    tool_results: &[insight_engine::response::WorkflowToolResult],
) -> Result<(), RepositoryError> {
    for artifact in tool_results
        .iter()
        .flat_map(|result| result.content())
        .filter_map(insight_engine::response::WorkflowToolContent::artifact)
    {
        let row = sqlx::query(
            "SELECT content_hash,size_bytes,media_type,artifact_state
             FROM artifacts WHERE run_id=$1 AND artifact_id=$2",
        )
        .bind(run_id.as_str())
        .bind(artifact.artifact_id().as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?
        .ok_or_else(RepositoryError::invalid_data)?;
        let content_hash = row
            .try_get::<String, _>("content_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        let size_bytes = u64_from_i64(
            row.try_get::<i64, _>("size_bytes")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let media_type = row
            .try_get::<Option<String>, _>("media_type")
            .map_err(|_| RepositoryError::invalid_data())?;
        let artifact_state = row
            .try_get::<String, _>("artifact_state")
            .map_err(|_| RepositoryError::invalid_data())?;
        if artifact_state != "referenced"
            || content_hash != artifact.content_hash().as_str()
            || size_bytes != artifact.size_bytes()
            || media_type.as_deref() != artifact.media_type()
        {
            return Err(RepositoryError::invalid_data());
        }
    }
    Ok(())
}

async fn validate_terminal_retrieval_artifacts_postgres(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    retrievals: &[insight_engine::response::WorkflowRetrieval],
) -> Result<(), RepositoryError> {
    for artifact in retrievals
        .iter()
        .flat_map(|retrieval| retrieval.results())
        .filter_map(insight_engine::response::WorkflowRetrievalResult::artifact)
    {
        let row = sqlx::query(
            "SELECT content_hash,size_bytes,media_type,artifact_state
             FROM artifacts WHERE run_id=$1 AND artifact_id=$2",
        )
        .bind(run_id.as_str())
        .bind(artifact.artifact_id().as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?
        .ok_or_else(RepositoryError::invalid_data)?;
        let content_hash = row
            .try_get::<String, _>("content_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        let size_bytes = u64_from_i64(
            row.try_get::<i64, _>("size_bytes")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        let media_type = row
            .try_get::<Option<String>, _>("media_type")
            .map_err(|_| RepositoryError::invalid_data())?;
        let artifact_state = row
            .try_get::<String, _>("artifact_state")
            .map_err(|_| RepositoryError::invalid_data())?;
        if artifact_state != "referenced"
            || content_hash != artifact.content_hash().as_str()
            || size_bytes != artifact.size_bytes()
            || media_type.as_deref() != artifact.media_type()
        {
            return Err(RepositoryError::invalid_data());
        }
    }
    Ok(())
}

async fn load_response_snapshot_postgres(
    pool: &PgPool,
    run_id: &RunId,
) -> Result<Option<DurableResponseSnapshot>, RepositoryError> {
    let row = sqlx::query(
        "SELECT response_id,terminal_kind,response_payload,workflow_payload,
                public_item_manifest,usage,usage_status,snapshot_hash
         FROM response_snapshots WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(RepositoryError::storage)?;
    row.map(|row| {
        durable_response_snapshot_new(
            row.try_get("response_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            response_terminal_kind_parse(
                &row.try_get::<String, _>("terminal_kind")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
            row.try_get("response_payload")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("workflow_payload")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("public_item_manifest")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get("usage")
                .map_err(|_| RepositoryError::invalid_data())?,
            response_usage_status_parse(
                &row.try_get::<String, _>("usage_status")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
            ContentHash::parse(
                row.try_get::<String, _>("snapshot_hash")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .map_err(|_| RepositoryError::invalid_data())?,
        )
    })
    .transpose()
}

async fn lock_and_match_publication_heads(
    transaction: &mut Transaction<'_, Postgres>,
    command: &PublishVersionedPlanCommand,
) -> Result<bool, RepositoryError> {
    // Include the route being advanced in the global lock order. This keeps
    // parent publications and dependency publications from acquiring the
    // same set of route rows in opposite orders.
    let mut route_ids = command
        .expected_dependency_heads()
        .iter()
        .map(|head| head.agent_id().to_owned())
        .collect::<BTreeSet<_>>();
    route_ids.insert(command.plan().agent_id().to_owned());

    for agent_id in route_ids {
        let row = sqlx::query(
            "SELECT agent_id,definition_id,definition_revision_id,deployment_revision_id,
                    publication_origin
             FROM agent_publication_heads WHERE agent_id=$1 FOR UPDATE",
        )
        .bind(&agent_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let actual = row.as_ref().map(postgres_publication_head).transpose()?;
        if command
            .expected_target_publication_head()
            .is_some_and(|expected| {
                expected.agent_id() == agent_id && actual.as_ref() != Some(expected)
            })
        {
            return Ok(false);
        }
        let Some(expected_dependency) = command
            .expected_dependency_heads()
            .iter()
            .find(|head| head.agent_id() == agent_id)
        else {
            continue;
        };
        if actual.as_ref() != Some(expected_dependency) {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn upsert_publication_head(
    transaction: &mut Transaction<'_, Postgres>,
    plan: &VersionedPlan,
) -> Result<(), RepositoryError> {
    sqlx::query(
        "INSERT INTO agent_publication_heads (
            agent_id,definition_id,definition_revision_id,deployment_revision_id,
            publication_origin,updated_at
         ) VALUES ($1,$2,$3,$4,'graph',CURRENT_TIMESTAMP)
         ON CONFLICT (agent_id) DO UPDATE SET
            definition_id=EXCLUDED.definition_id,
            definition_revision_id=EXCLUDED.definition_revision_id,
            deployment_revision_id=EXCLUDED.deployment_revision_id,
            publication_origin='graph',
            updated_at=CURRENT_TIMESTAMP",
    )
    .bind(plan.agent_id())
    .bind(plan.definition_id())
    .bind(plan.definition_revision_id().as_str())
    .bind(plan.deployment_revision_id().as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(())
}

fn postgres_publication_head(row: &PgRow) -> Result<PublicationHead, RepositoryError> {
    PublicationHead::new(
        row.try_get("agent_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("definition_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        DefinitionRevisionId::new(
            row.try_get::<String, _>("definition_revision_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        DeploymentRevisionId::new(
            row.try_get::<String, _>("deployment_revision_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        PublicationOrigin::parse(
            &row.try_get::<String, _>("publication_origin")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
    )
}

fn postgres_versioned_plan(row: &PgRow) -> Result<VersionedPlan, RepositoryError> {
    VersionedPlan::from_stored_parts(
        row.try_get("definition_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("agent_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("display_name")
            .map_err(|_| RepositoryError::invalid_data())?,
        DefinitionRevisionId::new(
            row.try_get::<String, _>("definition_revision_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        DeploymentRevisionId::new(
            row.try_get::<String, _>("deployment_revision_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        ContentHash::parse(
            row.try_get::<String, _>("plan_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        ContentHash::parse(
            row.try_get::<String, _>("binding_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("compiler_version")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("expression_engine_version")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("public_description")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("author_document")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("canonical_plan")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("descriptor_contracts")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("resolved_bindings")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("worker_contracts")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
}

async fn load_postgres_versioned_plan_catalog(
    pool: &PgPool,
) -> Result<VersionedPlanCatalog, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(RepositoryError::storage)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
    let plan_rows = sqlx::query(
        "SELECT d.definition_id,d.agent_id,m.display_name,m.public_description,
                r.definition_revision_id,x.deployment_revision_id,x.plan_hash,x.binding_hash,
                r.compiler_version,r.expression_engine_version,r.author_document,
                r.canonical_plan,r.descriptor_contracts,x.resolved_bindings,x.worker_contracts
         FROM deployment_revisions x
         JOIN workflow_definitions d ON d.definition_id=x.definition_id
         JOIN workflow_definition_revisions r
           ON r.definition_id=x.definition_id
          AND r.definition_revision_id=x.definition_revision_id
         JOIN workflow_definition_public_metadata m
           ON m.definition_id=x.definition_id
          AND m.definition_revision_id=x.definition_revision_id
         ORDER BY d.definition_id,x.deployment_revision_id",
    )
    .fetch_all(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let head_rows = sqlx::query(
        "SELECT agent_id,definition_id,definition_revision_id,deployment_revision_id,
                publication_origin
         FROM agent_publication_heads ORDER BY agent_id",
    )
    .fetch_all(&mut *transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let plans = plan_rows
        .iter()
        .map(postgres_versioned_plan)
        .collect::<Result<Vec<_>, _>>()?;
    let heads = head_rows
        .iter()
        .map(postgres_publication_head)
        .collect::<Result<Vec<_>, _>>()?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    VersionedPlanCatalog::new(plans, heads)
}

async fn install_plan(
    transaction: &mut Transaction<'_, Postgres>,
    plan: &VersionedPlan,
) -> Result<bool, RepositoryError> {
    let mut installed = false;
    installed |= sqlx::query(
        "INSERT INTO workflow_definitions (
            definition_id, agent_id, created_at, updated_at
         ) VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         ON CONFLICT (definition_id) DO NOTHING",
    )
    .bind(plan.definition_id())
    .bind(plan.agent_id())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected()
        == 1;
    let definition = sqlx::query(
        "SELECT agent_id FROM workflow_definitions
         WHERE definition_id = $1 FOR UPDATE",
    )
    .bind(plan.definition_id())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if definition
        .try_get::<String, _>("agent_id")
        .map_err(|_| RepositoryError::invalid_data())?
        != plan.agent_id()
    {
        return Err(plan_conflict());
    }

    installed |= sqlx::query(
        "INSERT INTO workflow_definition_revisions (
            definition_id, definition_revision_id, revision_status,
            author_document, canonical_plan, plan_hash, compiler_version,
            expression_engine_version, descriptor_contracts, created_at, published_at
         ) VALUES ($1, $2, 'published', $3, $4, $5, $6, $7, $8,
                   CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         ON CONFLICT (definition_id, definition_revision_id) DO NOTHING",
    )
    .bind(plan.definition_id())
    .bind(plan.definition_revision_id().as_str())
    .bind(plan.author_document())
    .bind(plan.canonical_plan())
    .bind(plan.plan_hash().as_str())
    .bind(plan.compiler_version())
    .bind(plan.expression_engine_version())
    .bind(plan.descriptor_contracts())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected()
        == 1;
    let revision = sqlx::query(
        "SELECT plan_hash, compiler_version, expression_engine_version,
                author_document, canonical_plan, descriptor_contracts
         FROM workflow_definition_revisions
         WHERE definition_id = $1 AND definition_revision_id = $2",
    )
    .bind(plan.definition_id())
    .bind(plan.definition_revision_id().as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if revision
        .try_get::<String, _>("plan_hash")
        .map_err(|_| RepositoryError::invalid_data())?
        != plan.plan_hash().as_str()
        || revision
            .try_get::<String, _>("compiler_version")
            .map_err(|_| RepositoryError::invalid_data())?
            != plan.compiler_version()
        || revision
            .try_get::<String, _>("expression_engine_version")
            .map_err(|_| RepositoryError::invalid_data())?
            != plan.expression_engine_version()
        || revision
            .try_get::<Value, _>("author_document")
            .map_err(|_| RepositoryError::invalid_data())?
            != *plan.author_document()
        || revision
            .try_get::<Value, _>("canonical_plan")
            .map_err(|_| RepositoryError::invalid_data())?
            != *plan.canonical_plan()
        || revision
            .try_get::<Value, _>("descriptor_contracts")
            .map_err(|_| RepositoryError::invalid_data())?
            != *plan.descriptor_contracts()
    {
        return Err(plan_conflict());
    }

    sqlx::query(
        "INSERT INTO workflow_definition_public_metadata (
            definition_id,definition_revision_id,display_name,public_description
         ) VALUES ($1,$2,$3,$4)
         ON CONFLICT (definition_id,definition_revision_id) DO NOTHING",
    )
    .bind(plan.definition_id())
    .bind(plan.definition_revision_id().as_str())
    .bind(plan.display_name())
    .bind(plan.public_description())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let metadata = sqlx::query(
        "SELECT display_name,public_description
         FROM workflow_definition_public_metadata
         WHERE definition_id=$1 AND definition_revision_id=$2",
    )
    .bind(plan.definition_id())
    .bind(plan.definition_revision_id().as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if metadata
        .try_get::<String, _>("display_name")
        .map_err(|_| RepositoryError::invalid_data())?
        != plan.display_name()
        || metadata
            .try_get::<String, _>("public_description")
            .map_err(|_| RepositoryError::invalid_data())?
            != plan.public_description()
    {
        return Err(plan_conflict());
    }

    installed |= sqlx::query(
        "INSERT INTO deployment_revisions (
            definition_id, definition_revision_id, deployment_revision_id,
            plan_hash, binding_hash, resolved_bindings, worker_contracts, created_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
         ON CONFLICT (definition_id, deployment_revision_id) DO NOTHING",
    )
    .bind(plan.definition_id())
    .bind(plan.definition_revision_id().as_str())
    .bind(plan.deployment_revision_id().as_str())
    .bind(plan.plan_hash().as_str())
    .bind(plan.binding_hash().as_str())
    .bind(plan.resolved_bindings())
    .bind(plan.worker_contracts())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected()
        == 1;
    let deployment = sqlx::query(
        "SELECT definition_revision_id, plan_hash, binding_hash,
                resolved_bindings, worker_contracts
         FROM deployment_revisions
         WHERE definition_id = $1 AND deployment_revision_id = $2",
    )
    .bind(plan.definition_id())
    .bind(plan.deployment_revision_id().as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if deployment
        .try_get::<String, _>("definition_revision_id")
        .map_err(|_| RepositoryError::invalid_data())?
        != plan.definition_revision_id().as_str()
        || deployment
            .try_get::<String, _>("plan_hash")
            .map_err(|_| RepositoryError::invalid_data())?
            != plan.plan_hash().as_str()
        || deployment
            .try_get::<String, _>("binding_hash")
            .map_err(|_| RepositoryError::invalid_data())?
            != plan.binding_hash().as_str()
        || deployment
            .try_get::<Value, _>("resolved_bindings")
            .map_err(|_| RepositoryError::invalid_data())?
            != *plan.resolved_bindings()
        || deployment
            .try_get::<Value, _>("worker_contracts")
            .map_err(|_| RepositoryError::invalid_data())?
            != *plan.worker_contracts()
    {
        return Err(plan_conflict());
    }
    Ok(installed)
}

pub(crate) fn decode_execution_event_row(
    row: &PgRow,
) -> Result<insight_engine::ExecutionEventEnvelope, RepositoryError> {
    decode_stored_execution_event(StoredExecutionEventRow {
        schema_version: i64::from(
            row.try_get::<i32, _>("schema_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        ),
        event_id: row
            .try_get("event_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        run_id: row
            .try_get("run_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        transition_key: row
            .try_get("transition_key")
            .map_err(|_| RepositoryError::invalid_data())?,
        intent_hash: row
            .try_get("intent_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
        seq: row
            .try_get("seq")
            .map_err(|_| RepositoryError::invalid_data())?,
        occurred_at: row
            .try_get("occurred_at")
            .map_err(|_| RepositoryError::invalid_data())?,
        kind: row
            .try_get("kind")
            .map_err(|_| RepositoryError::invalid_data())?,
        node_id: row
            .try_get("node_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        scope_instance_id: row
            .try_get("scope_instance_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        activation_id: row
            .try_get("activation_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        attempt_no: row
            .try_get::<Option<i32>, _>("attempt_no")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(i64::from),
        causation_event_id: row
            .try_get("causation_event_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        safe_payload: row
            .try_get("safe_payload")
            .map_err(|_| RepositoryError::invalid_data())?,
    })
}

pub(crate) async fn load_replay(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    transition_key: &TransitionKey,
    intent_hash: &str,
) -> Result<Replay, RepositoryError> {
    let row = sqlx::query(
        "SELECT schema_version,seq,event_id,run_id,transition_key,intent_hash,
                projection_version_after,kind,node_id,scope_instance_id,activation_id,
                attempt_no,causation_event_id,safe_payload,occurred_at
         FROM execution_events WHERE run_id = $1 AND transition_key = $2",
    )
    .bind(run_id.as_str())
    .bind(transition_key.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    let Some(row) = row else {
        return Ok(Replay::Vacant);
    };
    let execution = decode_execution_event_row(&row)?;
    if row
        .try_get::<String, _>("intent_hash")
        .map_err(|_| RepositoryError::invalid_data())?
        != intent_hash
    {
        return Err(RepositoryError::intent_conflict());
    }
    let event_id = row
        .try_get::<String, _>("event_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    verify_projection_checkpoint_batch(transaction, run_id, &event_id).await?;
    let public_event_id =
        load_replay_public_projection(transaction, run_id, &event_id, &execution).await?;
    Ok(Replay::Exact(CommitReceipt::new(
        u64_from_i64(
            row.try_get("seq")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        event_id,
        u64_from_i64(
            row.try_get("projection_version_after")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        public_event_id,
    )))
}

async fn load_replay_public_projection(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    event_id: &str,
    execution: &insight_engine::ExecutionEventEnvelope,
) -> Result<Option<String>, RepositoryError> {
    let rows = sqlx::query(
        "SELECT decision.run_id AS decision_run_id,
                decision.execution_event_id,decision.execution_seq,
                decision.execution_occurred_at,decision.execution_transition_key,
                decision.decision,decision.public_event_id AS decision_public_event_id,
                decision.public_ordinal AS decision_public_ordinal,
                decision.public_schema_version AS decision_public_schema_version,
                decision.event_kind AS decision_event_kind,
                decision.is_terminal AS decision_is_terminal,
                receipt.public_event_id AS receipt_public_event_id,
                receipt.causation_event_id AS receipt_causation_event_id,
                receipt.public_ordinal AS receipt_public_ordinal,
                receipt.public_schema_version AS receipt_public_schema_version,
                receipt.event_kind AS receipt_event_kind,
                receipt.is_terminal AS receipt_is_terminal,
                outbox.public_event_id AS outbox_public_event_id,
                outbox.causation_event_id AS outbox_causation_event_id,
                outbox.public_ordinal AS outbox_public_ordinal,
                outbox.public_schema_version AS outbox_public_schema_version,
                outbox.event_kind AS outbox_event_kind,
                outbox.is_terminal AS outbox_is_terminal
         FROM public_event_projection_decisions decision
         LEFT JOIN public_event_receipts receipt
           ON receipt.run_id=decision.run_id
          AND receipt.causation_event_id=decision.execution_event_id
         LEFT JOIN public_event_outbox outbox
           ON outbox.run_id=decision.run_id
          AND outbox.causation_event_id=decision.execution_event_id
         WHERE decision.run_id=$1 AND decision.execution_event_id=$2",
    )
    .bind(run_id.as_str())
    .bind(event_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    if rows.len() != 1 {
        return Err(RepositoryError::invalid_data());
    }
    let row = &rows[0];
    decode_public_projection_decision(
        StoredPublicProjectionDecision {
            run_id: row
                .try_get("decision_run_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            execution_event_id: row
                .try_get("execution_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            execution_seq: row
                .try_get("execution_seq")
                .map_err(|_| RepositoryError::invalid_data())?,
            execution_occurred_at: row
                .try_get("execution_occurred_at")
                .map_err(|_| RepositoryError::invalid_data())?,
            execution_transition_key: row
                .try_get("execution_transition_key")
                .map_err(|_| RepositoryError::invalid_data())?,
            decision: row
                .try_get("decision")
                .map_err(|_| RepositoryError::invalid_data())?,
            public_event_id: row
                .try_get("decision_public_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            public_ordinal: row
                .try_get::<Option<i32>, _>("decision_public_ordinal")
                .map_err(|_| RepositoryError::invalid_data())?
                .map(i64::from),
            public_schema_version: row
                .try_get::<Option<i32>, _>("decision_public_schema_version")
                .map_err(|_| RepositoryError::invalid_data())?
                .map(i64::from),
            event_kind: row
                .try_get("decision_event_kind")
                .map_err(|_| RepositoryError::invalid_data())?,
            is_terminal: row
                .try_get("decision_is_terminal")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_public_event_id: row
                .try_get("receipt_public_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_causation_event_id: row
                .try_get("receipt_causation_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_public_ordinal: row
                .try_get::<Option<i32>, _>("receipt_public_ordinal")
                .map_err(|_| RepositoryError::invalid_data())?
                .map(i64::from),
            receipt_public_schema_version: row
                .try_get::<Option<i32>, _>("receipt_public_schema_version")
                .map_err(|_| RepositoryError::invalid_data())?
                .map(i64::from),
            receipt_event_kind: row
                .try_get("receipt_event_kind")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_is_terminal: row
                .try_get("receipt_is_terminal")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_public_event_id: row
                .try_get("outbox_public_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_causation_event_id: row
                .try_get("outbox_causation_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_public_ordinal: row
                .try_get::<Option<i32>, _>("outbox_public_ordinal")
                .map_err(|_| RepositoryError::invalid_data())?
                .map(i64::from),
            outbox_public_schema_version: row
                .try_get::<Option<i32>, _>("outbox_public_schema_version")
                .map_err(|_| RepositoryError::invalid_data())?
                .map(i64::from),
            outbox_event_kind: row
                .try_get("outbox_event_kind")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_is_terminal: row
                .try_get("outbox_is_terminal")
                .map_err(|_| RepositoryError::invalid_data())?,
        },
        execution,
    )
}

async fn load_current_run_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<Option<CurrentRunRow>, RepositoryError> {
    let row = sqlx::query(
        "SELECT lifecycle, admission_state, projection_version, termination_intent_reason
         FROM workflow_runs WHERE run_id = $1 FOR UPDATE",
    )
    .bind(run_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    row.map(|row| {
        Ok(CurrentRunRow {
            lifecycle: row
                .try_get("lifecycle")
                .map_err(|_| RepositoryError::invalid_data())?,
            admission_state: row
                .try_get("admission_state")
                .map_err(|_| RepositoryError::invalid_data())?,
            projection_version: row
                .try_get("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
            termination_intent_reason: row
                .try_get("termination_intent_reason")
                .map_err(|_| RepositoryError::invalid_data())?,
        })
    })
    .transpose()
}

pub(crate) async fn allocate_event_seq(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<u64, RepositoryError> {
    let seq = sqlx::query_scalar::<_, i64>(
        "UPDATE workflow_runs
         SET next_event_seq = next_event_seq + 1
         WHERE run_id = $1
         RETURNING next_event_seq - 1",
    )
    .bind(run_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(run_not_found)?;
    u64_from_i64(seq)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert_event(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    seq: u64,
    event_id: &str,
    transition_key: &TransitionKey,
    intent_hash: &str,
    projection_version_after: u64,
    event: &PendingExecutionEvent,
) -> Result<DateTime<Utc>, RepositoryError> {
    let safe_payload =
        serde_json::to_value(event.payload()).map_err(|_| RepositoryError::canonicalization())?;
    let context = event.context();
    sqlx::query_scalar::<_, DateTime<Utc>>(
        "INSERT INTO execution_events (
            run_id, seq, event_id, schema_version, kind, transition_key,
            intent_hash, node_id, scope_instance_id, activation_id, attempt_no,
            causation_event_id, projection_version_after, safe_payload, occurred_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                   $13, $14, CURRENT_TIMESTAMP)
         RETURNING occurred_at",
    )
    .bind(run_id.as_str())
    .bind(i64_from_u64(seq)?)
    .bind(event_id)
    .bind(
        i32::try_from(EXECUTION_EVENT_SCHEMA_VERSION)
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(event.kind().as_str())
    .bind(transition_key.as_str())
    .bind(intent_hash)
    .bind(context.node_id().map(|value| value.as_str()))
    .bind(context.scope_instance_id().map(|value| value.as_str()))
    .bind(context.activation_id().map(|value| value.as_str()))
    .bind(
        context
            .attempt_no()
            .map(|value| i32::try_from(value.get()))
            .transpose()
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .bind(context.causation_event_id().map(|value| value.as_str()))
    .bind(i64_from_u64(projection_version_after)?)
    .bind(safe_payload)
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)
}

pub(crate) async fn insert_or_get_payload(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    value: &Value,
) -> Result<(String, String), RepositoryError> {
    let (canonical, hash) = canonical_value(value)?;
    if let Some(existing) = sqlx::query(
        "SELECT payload_id,content_hash,canonical_bytes,encoding,inline_value,binary_value
         FROM payloads WHERE run_id = $1 AND content_hash = $2",
    )
    .bind(run_id.as_str())
    .bind(hash.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    {
        let id = existing
            .try_get::<String, _>("payload_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let stored_value = existing
            .try_get::<Option<Value>, _>("inline_value")
            .map_err(|_| RepositoryError::invalid_data())?
            .ok_or_else(RepositoryError::invalid_data)?;
        let validated = validate_inline_payload(
            &id,
            &existing
                .try_get::<String, _>("content_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
            existing
                .try_get::<i64, _>("canonical_bytes")
                .map_err(|_| RepositoryError::invalid_data())?,
            &existing
                .try_get::<String, _>("encoding")
                .map_err(|_| RepositoryError::invalid_data())?,
            stored_value,
            None,
            existing
                .try_get::<Option<Vec<u8>>, _>("binary_value")
                .map_err(|_| RepositoryError::invalid_data())?
                .is_none(),
        )?;
        if common_contract_adapter::validated_inline_payload_content_hash(&validated) != &hash
            || common_contract_adapter::validated_inline_payload_canonical(&validated) != canonical
            || common_contract_adapter::validated_inline_payload_value(&validated) != value
        {
            return Err(RepositoryError::invalid_data());
        }
        return Ok((id, hash.as_str().to_string()));
    }
    let id = payload_id(&hash);
    sqlx::query(
        "INSERT INTO payloads (
            run_id, payload_id, content_hash, canonical_bytes, encoding,
            inline_value, binary_value, created_at, retain_until
         ) VALUES ($1, $2, $3, $4, 'json_jcs', $5, NULL, CURRENT_TIMESTAMP, NULL)",
    )
    .bind(run_id.as_str())
    .bind(&id)
    .bind(hash.as_str())
    .bind(i64::try_from(canonical.len()).map_err(|_| RepositoryError::invalid_data())?)
    .bind(value)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok((id, hash.as_str().to_string()))
}

#[allow(clippy::too_many_arguments)]
async fn update_run(
    transaction: &mut Transaction<'_, Postgres>,
    transition_key: &TransitionKey,
    command: &RunTransitionCommand,
    expected_version: i64,
    next_version: u64,
    terminal_event_id: &str,
    terminal_public_event_id: Option<&str>,
    terminal_output: Option<&(String, String)>,
) -> Result<u64, RepositoryError> {
    let result = match (
        model_adapter::run_transition_terminal_output(command),
        model_adapter::run_transition_terminal_reason(command),
    ) {
        (Some(_), None) => {
            let (payload_id, value_hash) =
                terminal_output.ok_or_else(RepositoryError::invalid_data)?;
            sqlx::query(
                "UPDATE workflow_runs
                 SET lifecycle = $1, admission_state = $2,
                     termination_intent_reason = NULL,
                     termination_intent_transition_key = NULL,
                     termination_intent_at = NULL,
                     output_payload_id = $3, output_artifact_id = NULL,
                     output_value_hash = $4, error_code = NULL,
                     terminal_event_id = $5, terminal_public_event_id = $6,
                     projection_version = $7, updated_at = CURRENT_TIMESTAMP,
                     terminal_at = CURRENT_TIMESTAMP
                 WHERE run_id = $8 AND projection_version = $9
                   AND lifecycle = $10 AND admission_state = $11",
            )
            .bind(command.next_lifecycle().as_str())
            .bind(command.next_admission().as_str())
            .bind(payload_id)
            .bind(value_hash)
            .bind(terminal_event_id)
            .bind(terminal_public_event_id)
            .bind(i64_from_u64(next_version)?)
            .bind(command.run_id().as_str())
            .bind(expected_version)
            .bind(command.expected_lifecycle().as_str())
            .bind(command.expected_admission().as_str())
            .execute(&mut **transaction)
            .await
        }
        (None, Some(reason)) => {
            sqlx::query(
                "UPDATE workflow_runs
                 SET lifecycle = $1, admission_state = $2,
                     output_payload_id = NULL, output_artifact_id = NULL,
                     output_value_hash = NULL, error_code = $3,
                     terminal_event_id = $4, terminal_public_event_id = $5,
                     projection_version = $6, updated_at = CURRENT_TIMESTAMP,
                     terminal_at = CURRENT_TIMESTAMP
                 WHERE run_id = $7 AND projection_version = $8
                   AND lifecycle = $9 AND admission_state = $10
                   AND termination_intent_reason = $11",
            )
            .bind(command.next_lifecycle().as_str())
            .bind(command.next_admission().as_str())
            .bind(model_adapter::run_transition_terminal_error_code(command))
            .bind(terminal_event_id)
            .bind(terminal_public_event_id)
            .bind(i64_from_u64(next_version)?)
            .bind(command.run_id().as_str())
            .bind(expected_version)
            .bind(command.expected_lifecycle().as_str())
            .bind(command.expected_admission().as_str())
            .bind(reason)
            .execute(&mut **transaction)
            .await
        }
        (None, None) if command.next_lifecycle() == RunLifecycle::Terminating => {
            sqlx::query(
                "UPDATE workflow_runs
                 SET lifecycle = 'terminating', admission_state = 'draining',
                     termination_intent_reason = $1,
                     termination_intent_transition_key = $2,
                     termination_intent_at = CURRENT_TIMESTAMP,
                     projection_version = $3, updated_at = CURRENT_TIMESTAMP
                 WHERE run_id = $4 AND projection_version = $5
                   AND lifecycle = $6 AND admission_state = $7
                   AND termination_intent_reason IS NULL",
            )
            .bind(command.termination_intent_reason())
            .bind(transition_key.as_str())
            .bind(i64_from_u64(next_version)?)
            .bind(command.run_id().as_str())
            .bind(expected_version)
            .bind(command.expected_lifecycle().as_str())
            .bind(command.expected_admission().as_str())
            .execute(&mut **transaction)
            .await
        }
        (None, None) => {
            sqlx::query(
                "UPDATE workflow_runs
                 SET lifecycle = $1, admission_state = $2, projection_version = $3,
                     started_at = CASE
                         WHEN started_at IS NULL AND lifecycle = 'created' AND $4 = 'active'
                         THEN CURRENT_TIMESTAMP ELSE started_at END,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE run_id = $5 AND projection_version = $6
                   AND lifecycle = $7 AND admission_state = $8
                   AND termination_intent_reason IS NULL",
            )
            .bind(command.next_lifecycle().as_str())
            .bind(command.next_admission().as_str())
            .bind(i64_from_u64(next_version)?)
            .bind(command.next_lifecycle().as_str())
            .bind(command.run_id().as_str())
            .bind(expected_version)
            .bind(command.expected_lifecycle().as_str())
            .bind(command.expected_admission().as_str())
            .execute(&mut **transaction)
            .await
        }
        (Some(_), Some(_)) => return Err(RepositoryError::invalid_data()),
    }
    .map_err(RepositoryError::storage)?;
    Ok(result.rows_affected())
}

fn projection_from_row(run_id: &RunId, row: &PgRow) -> Result<RunProjection, RepositoryError> {
    let lifecycle = row
        .try_get::<String, _>("lifecycle")
        .map_err(|_| RepositoryError::invalid_data())?;
    let admission = row
        .try_get::<String, _>("admission_state")
        .map_err(|_| RepositoryError::invalid_data())?;
    let expected_output_payload_id = row
        .try_get::<Option<String>, _>("expected_output_payload_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let output = if let Some(expected_payload_id) = expected_output_payload_id {
        let stored_payload_id = row
            .try_get::<Option<String>, _>("output_payload_id")
            .map_err(|_| RepositoryError::invalid_data())?
            .ok_or_else(RepositoryError::invalid_data)?;
        if stored_payload_id != expected_payload_id {
            return Err(RepositoryError::invalid_data());
        }
        let validated = validate_inline_payload(
            &stored_payload_id,
            &row.try_get::<Option<String>, _>("output_content_hash")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?,
            row.try_get::<Option<i64>, _>("output_canonical_bytes")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?,
            &row.try_get::<Option<String>, _>("output_encoding")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?,
            row.try_get::<Option<Value>, _>("output_inline")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?,
            None,
            row.try_get::<Option<Vec<u8>>, _>("output_binary")
                .map_err(|_| RepositoryError::invalid_data())?
                .is_none(),
        )?;
        if row
            .try_get::<Option<String>, _>("expected_output_value_hash")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            != Some(
                common_contract_adapter::validated_inline_payload_content_hash(&validated).as_str(),
            )
        {
            return Err(RepositoryError::invalid_data());
        }
        Some(common_contract_adapter::validated_inline_payload_value(&validated).clone())
    } else {
        if row
            .try_get::<Option<String>, _>("output_payload_id")
            .map_err(|_| RepositoryError::invalid_data())?
            .is_some()
        {
            return Err(RepositoryError::invalid_data());
        }
        None
    };
    Ok(RunProjection::new(
        run_id.clone(),
        row.try_get("response_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("definition_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("agent_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        DefinitionRevisionId::new(
            row.try_get::<String, _>("definition_revision_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        DeploymentRevisionId::new(
            row.try_get::<String, _>("deployment_revision_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        ContentHash::parse(
            row.try_get::<String, _>("plan_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        ContentHash::parse(
            row.try_get::<String, _>("binding_hash")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("request_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        PublicRunAttachment::parse(
            &row.try_get::<String, _>("attachment")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        parse_run_lifecycle(&lifecycle)?,
        parse_admission_state(&admission)?,
        u64_from_i64(
            row.try_get("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        u64_from_i64(
            row.try_get("next_event_seq")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get("termination_intent_reason")
            .map_err(|_| RepositoryError::invalid_data())?,
        output,
        row.try_get("error_code")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("created_at")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("started_at")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("updated_at")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("terminal_at")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("deadline_at")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))
}
