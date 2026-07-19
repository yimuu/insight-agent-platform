use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow},
    Row, Sqlite, SqlitePool, Transaction,
};
use tokio::sync::Mutex;

use crate::engine::{
    ContentHash, DefinitionRevisionId, DeploymentRevisionId, ExecutionEventContext,
    ExecutionEventPayload, PendingExecutionEvent, RunId, RunLifecycle, TransitionKey,
    TransitionOutcome, EXECUTION_EVENT_SCHEMA_VERSION,
};

use super::migration_manifest::{DurableV3Migration, SqliteMigrationGuard, DURABLE_V3_MIGRATIONS};
use super::model::PublicRunAttachment;
use super::model::TerminalRunMutation;
use super::sqlite_projection::{
    finalize_projection_checkpoints, verify_projection_checkpoint_batch,
};
use super::{
    common::{
        canonical_intent_hash, canonical_json, canonical_value, decode_public_projection_decision,
        decode_stored_execution_event, durable_public_event_envelope, event_id, i64_from_u64,
        parse_admission_state, parse_run_lifecycle, payload_id, public_event_id,
        public_event_ordinal, u64_from_i64, validate_inline_payload, StoredExecutionEventRow,
        StoredPublicProjectionDecision,
    },
    CommitReceipt, CreateRunCommand, DurableRepository, PlanInstallOutcome, PlanPublicationOutcome,
    PublicationHead, PublicationOrigin, PublishVersionedPlanCommand, RepositoryError,
    RunProjection, RunTransitionCommand, VersionedPlan, VersionedPlanCatalog,
    REPOSITORY_PLAN_CONFLICT, REPOSITORY_RUN_NOT_FOUND,
};

fn plan_conflict() -> RepositoryError {
    RepositoryError::new(
        REPOSITORY_PLAN_CONFLICT,
        "immutable plan revision conflicts with durable authority",
    )
}

fn run_not_found() -> RepositoryError {
    RepositoryError::new(
        REPOSITORY_RUN_NOT_FOUND,
        "durable workflow run was not found",
    )
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

async fn sqlite_migration_is_required(
    pool: &SqlitePool,
    migration: DurableV3Migration,
) -> Result<bool, RepositoryError> {
    let presence_query = match migration.sqlite_guard {
        // All other SQLite migrations are idempotent and intentionally run on
        // every open. This restores trigger-based authority after interrupted
        // development and test runs.
        SqliteMigrationGuard::Always => return Ok(true),
        SqliteMigrationGuard::WhenQueryMissing(query) => query,
    };
    Ok(sqlx::query_scalar::<_, i64>(presence_query)
        .fetch_optional(pool)
        .await
        .map_err(RepositoryError::storage)?
        .is_none())
}

/// SQLite test-double repository.
///
/// A single connection and shared writer mutex deliberately serialize all
/// mutations. This backend is not a multi-runtime lease authority.
#[derive(Clone)]
pub struct SqliteDurableRepository {
    pub(crate) pool: SqlitePool,
    pub(crate) writer: Arc<Mutex<()>>,
}

impl SqliteDurableRepository {
    pub async fn in_memory() -> Result<Self, RepositoryError> {
        let options = SqliteConnectOptions::new()
            .filename(":memory:")
            .foreign_keys(true);
        Self::connect_options(options).await
    }

    pub async fn connect_path(path: &Path) -> Result<Self, RepositoryError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true);
        Self::connect_options(options).await
    }

    async fn connect_options(options: SqliteConnectOptions) -> Result<Self, RepositoryError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .connect_with(options)
            .await
            .map_err(RepositoryError::storage)?;
        for migration in DURABLE_V3_MIGRATIONS {
            if sqlite_migration_is_required(&pool, migration).await? {
                sqlx::raw_sql(migration.sqlite_sql)
                    .execute(&pool)
                    .await
                    .map_err(RepositoryError::storage)?;
            }
        }
        Ok(Self {
            pool,
            writer: Arc::new(Mutex::new(())),
        })
    }

    pub async fn check_health(&self) -> Result<(), RepositoryError> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(RepositoryError::storage)?;
        Ok(())
    }

    #[cfg(test)]
    pub fn test_pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[async_trait]
impl DurableRepository for SqliteDurableRepository {
    async fn install_versioned_plan(
        &self,
        plan: &VersionedPlan,
    ) -> Result<PlanInstallOutcome, RepositoryError> {
        let _writer = self.writer.lock().await;
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
        let _writer = self.writer.lock().await;
        // Acquire the database-wide writer reservation before reading route
        // heads. Separate repository instances do not share `writer`, and a
        // deferred read transaction cannot safely upgrade after another
        // runtime advances a dependency route.
        let mut transaction = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(RepositoryError::storage)?;
        if !match_publication_heads(&mut transaction, &command).await? {
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
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        install_plan(&mut transaction, plan).await?;
        sqlx::query(
            "INSERT INTO agent_publication_heads (
                agent_id,definition_id,definition_revision_id,deployment_revision_id,
                publication_origin,updated_at
             ) VALUES (?,?,?,?,'built_in',CURRENT_TIMESTAMP)
             ON CONFLICT(agent_id) DO UPDATE SET
                definition_id=excluded.definition_id,
                definition_revision_id=excluded.definition_revision_id,
                deployment_revision_id=excluded.deployment_revision_id,
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
             FROM agent_publication_heads WHERE agent_id=?",
        )
        .bind(plan.agent_id())
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let head = sqlite_publication_head(&row)?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(head)
    }

    async fn load_versioned_plan_catalog(&self) -> Result<VersionedPlanCatalog, RepositoryError> {
        load_sqlite_versioned_plan_catalog(&self.pool).await
    }

    async fn create_run(
        &self,
        transition_key: TransitionKey,
        command: CreateRunCommand,
    ) -> Result<TransitionOutcome<CommitReceipt>, RepositoryError> {
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
            // SQLite is a single-process subset; the repository writer mutex is
            // the publication/admission fence and this comparison is retained
            // in the same transaction as Run insertion.
            let current = sqlx::query(
                "SELECT agent_id,definition_id,definition_revision_id,deployment_revision_id,
                        publication_origin
                 FROM agent_publication_heads WHERE agent_id=?",
            )
            .bind(expected.agent_id())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .map(|row| sqlite_publication_head(&row))
            .transpose()?;
            if current.as_ref() != Some(expected) {
                transaction
                    .rollback()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
        }

        let run_exists: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM workflow_runs WHERE run_id = ?")
                .bind(command.run_id().as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
        if run_exists.is_some() {
            transaction
                .rollback()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }

        let (input_json, input_hash) = canonical_value(command.input())?;
        let input_payload_id = payload_id(&input_hash);
        let run_deadline_at = if let Some(timeout_ms) = command.run_timeout_ms() {
            let deadline = sqlx::query_scalar::<_, String>(
                "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', julianday('now') + CAST(? AS REAL) / 86400000.0)",
            )
            .bind(i64::try_from(timeout_ms).map_err(|_| RepositoryError::invalid_data())?)
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            Some(parse_run_timestamp(&deadline)?)
        } else {
            None
        };

        sqlx::query(
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
                created_at, started_at, updated_at, terminal_at, deadline_at
             ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, 'created', 'open',
                NULL, NULL, NULL, ?, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, 1, NULL, 1, 0, 0, NULL, NULL, NULL,
                CURRENT_TIMESTAMP, NULL, CURRENT_TIMESTAMP, NULL, ?
             )",
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
        .bind(run_deadline_at.map(|deadline| deadline.to_rfc3339()))
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;

        sqlx::query(
            "INSERT INTO payloads (
                run_id, payload_id, content_hash, canonical_bytes, encoding,
                inline_value, binary_value, created_at, retain_until
             ) VALUES (?, ?, ?, ?, 'json_jcs', ?, NULL, CURRENT_TIMESTAMP, NULL)",
        )
        .bind(command.run_id().as_str())
        .bind(&input_payload_id)
        .bind(input_hash.as_str())
        .bind(i64::try_from(input_json.len()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(&input_json)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;

        sqlx::query(
            "INSERT INTO scope_instances (
                run_id, scope_instance_id, parent_scope_instance_id, static_scope_id,
                stable_dynamic_key, scope_kind, is_root, lifecycle, admission_state,
                admitted_children, settled_children, projection_version, created_at, settled_at
             ) VALUES (?, 'scope_root', NULL, 'root', NULL, 'root', 1, 'active', 'open',
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
        let public_payload = crate::engine::PublicEventPayload::RunCreated;
        let public_kind = public_payload.kind();
        let public_event_id = public_event_id(command.run_id(), &transition_key, public_kind);
        let safe_envelope = durable_public_event_envelope(
            command.run_id(),
            &public_event_id,
            &event_id,
            event_seq,
            event_occurred_at,
            public_payload.clone(),
        )?;
        let safe_envelope = canonical_json(
            &serde_json::to_value(safe_envelope)
                .map_err(|_| RepositoryError::canonicalization())?,
        )?;
        sqlx::query(
            "INSERT INTO public_event_outbox (
                run_id, public_event_id, causation_event_id, public_ordinal, public_schema_version,
                event_kind, is_terminal, publish_state, safe_envelope, available_at,
                claimed_by, claim_token, claim_expires_at, publish_attempts, published_at,
                published_by, published_claim_token, notified_at, retain_until, created_at
             ) VALUES (?, ?, ?, ?, 1, ?, 0, 'pending', ?, CURRENT_TIMESTAMP,
                       NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP)",
        )
        .bind(command.run_id().as_str())
        .bind(&public_event_id)
        .bind(&event_id)
        .bind(i64::from(public_event_ordinal(public_kind)))
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

        let current = load_current_run(&mut transaction, command.run_id())
            .await?
            .ok_or_else(run_not_found)?;
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

        if let Some(TerminalRunMutation::Failed { reason, .. }) = command.terminal() {
            if current.termination_intent_reason.as_deref()
                != Some(super::model::termination_reason_as_str(*reason))
            {
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
            if let Some(output) = command.terminal().and_then(TerminalRunMutation::output) {
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
            let envelope = durable_public_event_envelope(
                command.run_id(),
                public_id,
                &event_id,
                event_seq,
                event_occurred_at,
                public.payload().clone(),
            )?;
            let envelope = canonical_json(
                &serde_json::to_value(envelope).map_err(|_| RepositoryError::canonicalization())?,
            )?;
            sqlx::query(
                "INSERT INTO public_event_outbox (
                    run_id, public_event_id, causation_event_id, public_ordinal,
                    public_schema_version,
                    event_kind, is_terminal, publish_state, safe_envelope, available_at,
                    claimed_by, claim_token, claim_expires_at, publish_attempts, published_at,
                    published_by, published_claim_token, notified_at, retain_until, created_at
                 ) VALUES (?, ?, ?, ?, 1, ?, ?, 'pending', ?, CURRENT_TIMESTAMP,
                           NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP)",
            )
            .bind(command.run_id().as_str())
            .bind(public_id)
            .bind(&event_id)
            .bind(i64::from(public_event_ordinal(public.payload().kind())))
            .bind(public.event_kind())
            .bind(public.is_terminal())
            .bind(envelope)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
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
            "SELECT r.definition_id,d.agent_id,r.definition_revision_id,
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
             WHERE r.run_id = ?",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| projection_from_row(run_id, &row)).transpose()
    }
}

async fn match_publication_heads(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &PublishVersionedPlanCommand,
) -> Result<bool, RepositoryError> {
    // SQLite is the serialized test-double backend. The repository writer
    // guard covers this read/compare together with the following install and
    // route advance; expectations are already sorted by agent id.
    let expected = command
        .expected_target_publication_head()
        .into_iter()
        .chain(command.expected_dependency_heads());
    for expected in expected {
        let row = sqlx::query(
            "SELECT agent_id,definition_id,definition_revision_id,deployment_revision_id,
                    publication_origin
             FROM agent_publication_heads WHERE agent_id=?",
        )
        .bind(expected.agent_id())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let actual = row.as_ref().map(sqlite_publication_head).transpose()?;
        if actual.as_ref() != Some(expected) {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn upsert_publication_head(
    transaction: &mut Transaction<'_, Sqlite>,
    plan: &VersionedPlan,
) -> Result<(), RepositoryError> {
    sqlx::query(
        "INSERT INTO agent_publication_heads (
            agent_id,definition_id,definition_revision_id,deployment_revision_id,
            publication_origin,updated_at
         ) VALUES (?,?,?,?,'graph',CURRENT_TIMESTAMP)
         ON CONFLICT(agent_id) DO UPDATE SET
            definition_id=excluded.definition_id,
            definition_revision_id=excluded.definition_revision_id,
            deployment_revision_id=excluded.deployment_revision_id,
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

fn sqlite_publication_head(row: &SqliteRow) -> Result<PublicationHead, RepositoryError> {
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

fn sqlite_json(row: &SqliteRow, column: &str) -> Result<serde_json::Value, RepositoryError> {
    let value = row
        .try_get::<String, _>(column)
        .map_err(|_| RepositoryError::invalid_data())?;
    serde_json::from_str(&value).map_err(|_| RepositoryError::invalid_data())
}

fn sqlite_versioned_plan(row: &SqliteRow) -> Result<VersionedPlan, RepositoryError> {
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
        sqlite_json(row, "author_document")?,
        sqlite_json(row, "canonical_plan")?,
        sqlite_json(row, "descriptor_contracts")?,
        sqlite_json(row, "resolved_bindings")?,
        sqlite_json(row, "worker_contracts")?,
    )
}

async fn load_sqlite_versioned_plan_catalog(
    pool: &SqlitePool,
) -> Result<VersionedPlanCatalog, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(RepositoryError::storage)?;
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
        .map(sqlite_versioned_plan)
        .collect::<Result<Vec<_>, _>>()?;
    let heads = head_rows
        .iter()
        .map(sqlite_publication_head)
        .collect::<Result<Vec<_>, _>>()?;
    transaction
        .commit()
        .await
        .map_err(RepositoryError::storage)?;
    VersionedPlanCatalog::new(plans, heads)
}

async fn install_plan(
    transaction: &mut Transaction<'_, Sqlite>,
    plan: &VersionedPlan,
) -> Result<bool, RepositoryError> {
    let mut installed = false;
    let definition =
        sqlx::query("SELECT agent_id FROM workflow_definitions WHERE definition_id = ?")
            .bind(plan.definition_id())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
    match definition {
        Some(row)
            if row
                .try_get::<String, _>("agent_id")
                .map_err(|_| RepositoryError::invalid_data())?
                == plan.agent_id() => {}
        Some(_) => return Err(plan_conflict()),
        None => {
            sqlx::query(
                "INSERT INTO workflow_definitions (
                    definition_id, agent_id, created_at, updated_at
                 ) VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            )
            .bind(plan.definition_id())
            .bind(plan.agent_id())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            installed = true;
        }
    }

    let author_document = canonical_json(plan.author_document())?;
    let canonical_plan = canonical_json(plan.canonical_plan())?;
    let descriptor_contracts = canonical_json(plan.descriptor_contracts())?;
    let revision = sqlx::query(
        "SELECT plan_hash, compiler_version, expression_engine_version,
                author_document, canonical_plan, descriptor_contracts
         FROM workflow_definition_revisions
         WHERE definition_id = ? AND definition_revision_id = ?",
    )
    .bind(plan.definition_id())
    .bind(plan.definition_revision_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    match revision {
        Some(row)
            if row
                .try_get::<String, _>("plan_hash")
                .map_err(|_| RepositoryError::invalid_data())?
                == plan.plan_hash().as_str()
                && row
                    .try_get::<String, _>("compiler_version")
                    .map_err(|_| RepositoryError::invalid_data())?
                    == plan.compiler_version()
                && row
                    .try_get::<String, _>("expression_engine_version")
                    .map_err(|_| RepositoryError::invalid_data())?
                    == plan.expression_engine_version()
                && row
                    .try_get::<String, _>("author_document")
                    .map_err(|_| RepositoryError::invalid_data())?
                    == author_document
                && row
                    .try_get::<String, _>("canonical_plan")
                    .map_err(|_| RepositoryError::invalid_data())?
                    == canonical_plan
                && row
                    .try_get::<String, _>("descriptor_contracts")
                    .map_err(|_| RepositoryError::invalid_data())?
                    == descriptor_contracts => {}
        Some(_) => return Err(plan_conflict()),
        None => {
            sqlx::query(
                "INSERT INTO workflow_definition_revisions (
                    definition_id, definition_revision_id, revision_status,
                    author_document, canonical_plan, plan_hash, compiler_version,
                    expression_engine_version, descriptor_contracts, created_at, published_at
                 ) VALUES (?, ?, 'published', ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            )
            .bind(plan.definition_id())
            .bind(plan.definition_revision_id().as_str())
            .bind(&author_document)
            .bind(&canonical_plan)
            .bind(plan.plan_hash().as_str())
            .bind(plan.compiler_version())
            .bind(plan.expression_engine_version())
            .bind(&descriptor_contracts)
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            installed = true;
        }
    }

    let public_metadata = sqlx::query(
        "SELECT display_name,public_description
         FROM workflow_definition_public_metadata
         WHERE definition_id=? AND definition_revision_id=?",
    )
    .bind(plan.definition_id())
    .bind(plan.definition_revision_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    match public_metadata {
        Some(row)
            if row
                .try_get::<String, _>("display_name")
                .map_err(|_| RepositoryError::invalid_data())?
                == plan.display_name()
                && row
                    .try_get::<String, _>("public_description")
                    .map_err(|_| RepositoryError::invalid_data())?
                    == plan.public_description() => {}
        Some(_) => return Err(plan_conflict()),
        None => {
            sqlx::query(
                "INSERT INTO workflow_definition_public_metadata (
                    definition_id,definition_revision_id,display_name,public_description
                 ) VALUES (?,?,?,?)",
            )
            .bind(plan.definition_id())
            .bind(plan.definition_revision_id().as_str())
            .bind(plan.display_name())
            .bind(plan.public_description())
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            installed = true;
        }
    }

    let resolved_bindings = canonical_json(plan.resolved_bindings())?;
    let worker_contracts = canonical_json(plan.worker_contracts())?;
    let deployment = sqlx::query(
        "SELECT definition_revision_id, plan_hash, binding_hash,
                resolved_bindings, worker_contracts
         FROM deployment_revisions
         WHERE definition_id = ? AND deployment_revision_id = ?",
    )
    .bind(plan.definition_id())
    .bind(plan.deployment_revision_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    match deployment {
        Some(row)
            if row
                .try_get::<String, _>("definition_revision_id")
                .map_err(|_| RepositoryError::invalid_data())?
                == plan.definition_revision_id().as_str()
                && row
                    .try_get::<String, _>("plan_hash")
                    .map_err(|_| RepositoryError::invalid_data())?
                    == plan.plan_hash().as_str()
                && row
                    .try_get::<String, _>("binding_hash")
                    .map_err(|_| RepositoryError::invalid_data())?
                    == plan.binding_hash().as_str()
                && row
                    .try_get::<String, _>("resolved_bindings")
                    .map_err(|_| RepositoryError::invalid_data())?
                    == resolved_bindings
                && row
                    .try_get::<String, _>("worker_contracts")
                    .map_err(|_| RepositoryError::invalid_data())?
                    == worker_contracts => {}
        Some(_) => return Err(plan_conflict()),
        None => {
            sqlx::query(
                "INSERT INTO deployment_revisions (
                    definition_id, definition_revision_id, deployment_revision_id,
                    plan_hash, binding_hash, resolved_bindings, worker_contracts, created_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
            )
            .bind(plan.definition_id())
            .bind(plan.definition_revision_id().as_str())
            .bind(plan.deployment_revision_id().as_str())
            .bind(plan.plan_hash().as_str())
            .bind(plan.binding_hash().as_str())
            .bind(&resolved_bindings)
            .bind(&worker_contracts)
            .execute(&mut **transaction)
            .await
            .map_err(RepositoryError::storage)?;
            installed = true;
        }
    }
    Ok(installed)
}

pub(crate) fn decode_execution_event_row(
    row: &SqliteRow,
) -> Result<crate::engine::ExecutionEventEnvelope, RepositoryError> {
    let safe_payload = serde_json::from_str(
        &row.try_get::<String, _>("safe_payload")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    decode_stored_execution_event(StoredExecutionEventRow {
        schema_version: row
            .try_get("schema_version")
            .map_err(|_| RepositoryError::invalid_data())?,
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
        occurred_at: parse_run_timestamp(
            &row.try_get::<String, _>("occurred_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
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
            .try_get("attempt_no")
            .map_err(|_| RepositoryError::invalid_data())?,
        causation_event_id: row
            .try_get("causation_event_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        safe_payload,
    })
}

pub(crate) async fn load_replay(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    transition_key: &TransitionKey,
    intent_hash: &str,
) -> Result<Replay, RepositoryError> {
    let row = sqlx::query(
        "SELECT schema_version,seq,event_id,run_id,transition_key,intent_hash,
                projection_version_after,kind,node_id,scope_instance_id,activation_id,
                attempt_no,causation_event_id,safe_payload,occurred_at
         FROM execution_events WHERE run_id = ? AND transition_key = ?",
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
            row.try_get::<i64, _>("seq")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        event_id,
        u64_from_i64(
            row.try_get::<i64, _>("projection_version_after")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        public_event_id,
    )))
}

async fn load_replay_public_projection(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    event_id: &str,
    execution: &crate::engine::ExecutionEventEnvelope,
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
         WHERE decision.run_id=? AND decision.execution_event_id=?",
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
    let sqlite_bool = |value: Option<i64>| match value {
        None => Ok(None),
        Some(0) => Ok(Some(false)),
        Some(1) => Ok(Some(true)),
        Some(_) => Err(RepositoryError::invalid_data()),
    };
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
            execution_occurred_at: parse_run_timestamp(
                &row.try_get::<String, _>("execution_occurred_at")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
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
                .try_get("decision_public_ordinal")
                .map_err(|_| RepositoryError::invalid_data())?,
            public_schema_version: row
                .try_get("decision_public_schema_version")
                .map_err(|_| RepositoryError::invalid_data())?,
            event_kind: row
                .try_get("decision_event_kind")
                .map_err(|_| RepositoryError::invalid_data())?,
            is_terminal: sqlite_bool(
                row.try_get("decision_is_terminal")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
            receipt_public_event_id: row
                .try_get("receipt_public_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_causation_event_id: row
                .try_get("receipt_causation_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_public_ordinal: row
                .try_get("receipt_public_ordinal")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_public_schema_version: row
                .try_get("receipt_public_schema_version")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_event_kind: row
                .try_get("receipt_event_kind")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_is_terminal: sqlite_bool(
                row.try_get("receipt_is_terminal")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
            outbox_public_event_id: row
                .try_get("outbox_public_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_causation_event_id: row
                .try_get("outbox_causation_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_public_ordinal: row
                .try_get("outbox_public_ordinal")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_public_schema_version: row
                .try_get("outbox_public_schema_version")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_event_kind: row
                .try_get("outbox_event_kind")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_is_terminal: sqlite_bool(
                row.try_get("outbox_is_terminal")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
        },
        execution,
    )
}

async fn load_current_run(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
) -> Result<Option<CurrentRunRow>, RepositoryError> {
    let row = sqlx::query(
        "SELECT lifecycle, admission_state, projection_version, termination_intent_reason
         FROM workflow_runs WHERE run_id = ?",
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
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
) -> Result<u64, RepositoryError> {
    let seq = sqlx::query_scalar::<_, i64>(
        "UPDATE workflow_runs
         SET next_event_seq = next_event_seq + 1
         WHERE run_id = ?
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
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    seq: u64,
    event_id: &str,
    transition_key: &TransitionKey,
    intent_hash: &str,
    projection_version_after: u64,
    event: &PendingExecutionEvent,
) -> Result<DateTime<Utc>, RepositoryError> {
    let safe_payload = canonical_json(
        &serde_json::to_value(event.payload()).map_err(|_| RepositoryError::canonicalization())?,
    )?;
    let context = event.context();
    let occurred_at = sqlx::query_scalar::<_, String>(
        "INSERT INTO execution_events (
            run_id, seq, event_id, schema_version, kind, transition_key,
            intent_hash, node_id, scope_instance_id, activation_id, attempt_no,
            causation_event_id, projection_version_after, safe_payload, occurred_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
         RETURNING occurred_at",
    )
    .bind(run_id.as_str())
    .bind(i64_from_u64(seq)?)
    .bind(event_id)
    .bind(i64::from(EXECUTION_EVENT_SCHEMA_VERSION))
    .bind(event.kind().as_str())
    .bind(transition_key.as_str())
    .bind(intent_hash)
    .bind(context.node_id().map(|value| value.as_str()))
    .bind(context.scope_instance_id().map(|value| value.as_str()))
    .bind(context.activation_id().map(|value| value.as_str()))
    .bind(context.attempt_no().map(|value| i64::from(value.get())))
    .bind(context.causation_event_id().map(|value| value.as_str()))
    .bind(i64_from_u64(projection_version_after)?)
    .bind(&safe_payload)
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    parse_run_timestamp(&occurred_at)
}

pub(crate) async fn insert_or_get_payload(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    value: &serde_json::Value,
) -> Result<(String, String), RepositoryError> {
    let (canonical, hash) = canonical_value(value)?;
    if let Some(existing) = sqlx::query(
        "SELECT payload_id,content_hash,canonical_bytes,encoding,inline_value,binary_value
         FROM payloads WHERE run_id = ? AND content_hash = ?",
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
        let encoded = existing
            .try_get::<Option<String>, _>("inline_value")
            .map_err(|_| RepositoryError::invalid_data())?
            .ok_or_else(RepositoryError::invalid_data)?;
        let parsed = serde_json::from_str(&encoded).map_err(|_| RepositoryError::invalid_data())?;
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
            parsed,
            Some(&encoded),
            existing
                .try_get::<Option<Vec<u8>>, _>("binary_value")
                .map_err(|_| RepositoryError::invalid_data())?
                .is_none(),
        )?;
        if validated.content_hash() != &hash
            || validated.canonical() != canonical
            || validated.value() != value
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
         ) VALUES (?, ?, ?, ?, 'json_jcs', ?, NULL, CURRENT_TIMESTAMP, NULL)",
    )
    .bind(run_id.as_str())
    .bind(&id)
    .bind(hash.as_str())
    .bind(i64::try_from(canonical.len()).map_err(|_| RepositoryError::invalid_data())?)
    .bind(&canonical)
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?;
    Ok((id, hash.as_str().to_string()))
}

#[allow(clippy::too_many_arguments)]
async fn update_run(
    transaction: &mut Transaction<'_, Sqlite>,
    transition_key: &TransitionKey,
    command: &RunTransitionCommand,
    expected_version: i64,
    next_version: u64,
    terminal_event_id: &str,
    terminal_public_event_id: Option<&str>,
    terminal_output: Option<&(String, String)>,
) -> Result<u64, RepositoryError> {
    let result = match command.terminal() {
        Some(TerminalRunMutation::Succeeded { .. }) => {
            let (payload_id, value_hash) =
                terminal_output.ok_or_else(RepositoryError::invalid_data)?;
            sqlx::query(
                "UPDATE workflow_runs
                 SET lifecycle = ?, admission_state = ?,
                     termination_intent_reason = NULL,
                     termination_intent_transition_key = NULL,
                     termination_intent_at = NULL,
                     output_payload_id = ?, output_artifact_id = NULL,
                     output_value_hash = ?, error_code = NULL,
                     terminal_event_id = ?, terminal_public_event_id = ?,
                     projection_version = ?, updated_at = CURRENT_TIMESTAMP,
                     terminal_at = CURRENT_TIMESTAMP
                 WHERE run_id = ? AND projection_version = ?
                   AND lifecycle = ? AND admission_state = ?",
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
        Some(terminal @ TerminalRunMutation::Failed { .. }) => {
            sqlx::query(
                "UPDATE workflow_runs
                 SET lifecycle = ?, admission_state = ?,
                     output_payload_id = NULL, output_artifact_id = NULL,
                     output_value_hash = NULL, error_code = ?,
                     terminal_event_id = ?, terminal_public_event_id = ?,
                     projection_version = ?, updated_at = CURRENT_TIMESTAMP,
                     terminal_at = CURRENT_TIMESTAMP
                 WHERE run_id = ? AND projection_version = ?
                   AND lifecycle = ? AND admission_state = ?
                   AND termination_intent_reason = ?",
            )
            .bind(command.next_lifecycle().as_str())
            .bind(command.next_admission().as_str())
            .bind(terminal.error_code())
            .bind(terminal_event_id)
            .bind(terminal_public_event_id)
            .bind(i64_from_u64(next_version)?)
            .bind(command.run_id().as_str())
            .bind(expected_version)
            .bind(command.expected_lifecycle().as_str())
            .bind(command.expected_admission().as_str())
            .bind(terminal.reason())
            .execute(&mut **transaction)
            .await
        }
        None if command.next_lifecycle() == RunLifecycle::Terminating => {
            sqlx::query(
                "UPDATE workflow_runs
                 SET lifecycle = 'terminating', admission_state = 'draining',
                     termination_intent_reason = ?,
                     termination_intent_transition_key = ?,
                     termination_intent_at = CURRENT_TIMESTAMP,
                     projection_version = ?, updated_at = CURRENT_TIMESTAMP
                 WHERE run_id = ? AND projection_version = ?
                   AND lifecycle = ? AND admission_state = ?
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
        None => {
            sqlx::query(
                "UPDATE workflow_runs
                 SET lifecycle = ?, admission_state = ?, projection_version = ?,
                     started_at = CASE
                         WHEN started_at IS NULL AND lifecycle = 'created' AND ? = 'active'
                         THEN CURRENT_TIMESTAMP ELSE started_at END,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE run_id = ? AND projection_version = ?
                   AND lifecycle = ? AND admission_state = ?
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
    }
    .map_err(RepositoryError::storage)?;
    Ok(result.rows_affected())
}

fn projection_from_row(run_id: &RunId, row: &SqliteRow) -> Result<RunProjection, RepositoryError> {
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
        let encoded = row
            .try_get::<Option<String>, _>("output_inline")
            .map_err(|_| RepositoryError::invalid_data())?
            .ok_or_else(RepositoryError::invalid_data)?;
        let value = serde_json::from_str(&encoded).map_err(|_| RepositoryError::invalid_data())?;
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
            value,
            Some(&encoded),
            row.try_get::<Option<Vec<u8>>, _>("output_binary")
                .map_err(|_| RepositoryError::invalid_data())?
                .is_none(),
        )?;
        if row
            .try_get::<Option<String>, _>("expected_output_value_hash")
            .map_err(|_| RepositoryError::invalid_data())?
            .as_deref()
            != Some(validated.content_hash().as_str())
        {
            return Err(RepositoryError::invalid_data());
        }
        Some(validated.value().clone())
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
        parse_run_timestamp(
            &row.try_get::<String, _>("created_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get::<Option<String>, _>("started_at")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(|value| parse_run_timestamp(&value))
            .transpose()?,
        parse_run_timestamp(
            &row.try_get::<String, _>("updated_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get::<Option<String>, _>("terminal_at")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(|value| parse_run_timestamp(&value))
            .transpose()?,
        row.try_get::<Option<String>, _>("deadline_at")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(|value| parse_run_timestamp(&value))
            .transpose()?,
    ))
}

pub(super) fn parse_run_timestamp(value: &str) -> Result<DateTime<Utc>, RepositoryError> {
    if let Ok(value) = DateTime::parse_from_rfc3339(value) {
        return Ok(value.with_timezone(&Utc));
    }
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .map(|value| value.and_utc())
        .map_err(|_| RepositoryError::invalid_data())
}
