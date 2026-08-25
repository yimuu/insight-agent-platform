use crate::{
    invocation_repository::load_enabled_exact_published_version,
    repository::{
        append_command_event, append_scheduler_event, claim_command_receipt,
        decode_deployment_closure, decode_typed_payload, decode_versioned_payload, job_from_row,
        job_projection, load_deployment, load_job_for_update_by_text, load_resource,
        load_run_for_update, payload_from_row, require_ready_run_artifact,
        require_tenant_permission, safety_scan_cursor_from_row, safety_scan_page,
        settle_context_leaf_success_in_transaction, terminalize_command_receipt,
        validate_safety_scan_request, DeploymentRecord, JobRecord, PgRepository, RepositoryError,
        RunRecord, SafetyScanCursor, SafetyScanPage, SafetyScanShard, TypedPayload,
    },
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_artifacts::ArtifactReferenceSnapshot;
use insight_platform_context::{
    decide_claim_context_dispatch, decide_context_outcome, decide_context_query_admission,
    decide_expired_context_lease, decide_prepare_context_dispatch, decide_start_context_dispatch,
    decide_wake_context_dispatch, ClaimContextJobs, CommitContextOutcome, ContextAdmissionFacts,
    ContextDatasetView, ContextJobPayload, ContextLeaseIdentity, ContextObservationOutput,
    ContextQueryError, ContextQueryLimits, ContextQueryPayload, ContextQueryRecord,
    ContextQueryStore, ContextQueryTransaction, ContextSignalAudit, ContextWorkerAudit,
    CreateContextQuery, ExpiredContextLeaseObservation, PrepareContextDispatch,
    PreparedContextDispatch, WakeContextDispatch, CONTEXT_QUOTA_LINES,
};
use insight_platform_contracts::{
    canonical_digest, ArtifactPurpose, ArtifactReferenceKind, CommandOutcome,
    ContextConsistencyPolicy, ContextDatasetGenerationSpec, ContextQueryState, DeploymentClosure,
    EntityLifecycle, ExactDatasetGenerationRef, JobState, NodeExecutionState, Permission,
    PlanNodeKind, PublishedVersionPayload, QuotaDimension, RegistryResourceKind, ResourceDocument,
    ResourceId, ResourceKind, RunState, Sha256Digest, ValueRef, WorkClass,
};
use insight_platform_jobs::JobProjection;
use insight_platform_orchestrator::ScopeEnvironmentLimits;
use sqlx::{postgres::PgRow, Acquire, Postgres, Row, Transaction};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedContextExecution {
    pub query: ContextQueryRecord,
    pub job: JobRecord,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedContextExecution {
    pub query: ContextQueryRecord,
    pub job: JobRecord,
    pub query_input: ValueRef,
    pub quota_account_ids: Vec<String>,
    pub resume_mutations: insight_platform_contracts::ExternalLeafResumeMutationIds,
    pub failure_mutations: insight_platform_contracts::ExternalLeafFailureMutationIds,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimableNativeContextJob {
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimableRemoteContextJob {
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
}

#[derive(Debug, Clone)]
pub struct ExpiredContextRecoverySlot {
    pub quota_entry_ids: [ResourceId; CONTEXT_QUOTA_LINES],
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
}

impl ExpiredContextRecoverySlot {
    fn validate(&self) -> Result<(), RepositoryError> {
        let unique = self
            .quota_entry_ids
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        if unique.len() != CONTEXT_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
        {
            return Err(RepositoryError::InvalidInput(
                "expired Context recovery identity is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DriveExpiredContextJobs {
    pub shard: SafetyScanShard,
    pub after: Option<SafetyScanCursor>,
    pub limit: u16,
    pub retry_backoff_milliseconds: u64,
    pub slots: Vec<ExpiredContextRecoverySlot>,
}

impl DriveExpiredContextJobs {
    fn validate(&self, maximum_batch: u16, maximum_shards: u16) -> Result<(), RepositoryError> {
        validate_safety_scan_request(
            self.shard,
            self.after.as_ref(),
            ResourceKind::Job,
            self.limit,
            self.slots.len(),
            maximum_batch,
            maximum_shards,
        )?;
        if self.retry_backoff_milliseconds == 0 {
            return Err(RepositoryError::InvalidInput(
                "expired Context recovery request is invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            for id in slot
                .quota_entry_ids
                .iter()
                .chain([&slot.event_id, &slot.outbox_id])
            {
                if !unique.insert(id.to_string()) {
                    return Err(RepositoryError::InvalidInput(
                        "expired Context recovery identities must be globally unique".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveredContextExecution {
    pub query: ContextQueryRecord,
    pub job: JobRecord,
}

pub struct PgContextQueryTransaction {
    transaction: Transaction<'static, Postgres>,
    limits: ContextQueryLimits,
    scope_environment_limits: ScopeEnvironmentLimits,
}

#[derive(Debug)]
struct LockedContextNode {
    run_id: String,
    state: NodeExecutionState,
    version: u64,
    node_kind: PlanNodeKind,
    deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct ContextQuotaAccount {
    tenant_id: String,
    quota_account_id: String,
    scope_kind: String,
    scope_id: String,
    work_class: String,
    metric: String,
    limit_value: i64,
    reserved_value: i64,
    used_value: i64,
    version: i64,
}

#[derive(Debug, Clone)]
struct ContextQuotaLine {
    account: ContextQuotaAccount,
    reserved_amount: i64,
}

impl PgRepository {
    pub async fn begin_context_query_transaction(
        &self,
    ) -> Result<PgContextQueryTransaction, RepositoryError> {
        Ok(PgContextQueryTransaction {
            transaction: self.pool().begin().await?,
            limits: self.context_query_limits(),
            scope_environment_limits: self.scope_environment_limits(),
        })
    }

    /// Discovers ready Context Jobs whose frozen NativeCatalog binding matches this process.
    ///
    /// This is deliberately read-only. The subsequent exact-slot claim remains the sole lease,
    /// quota, and parent-state authority, so a race after discovery is handled as first-winner
    /// loss without creating a second scheduler authority.
    pub async fn scan_claimable_native_context_jobs(
        &self,
        worker_manifest_digest: &Sha256Digest,
        installed_adapter_digest: &Sha256Digest,
        adapter_contract_digest: &Sha256Digest,
        limit: u16,
    ) -> Result<Vec<ClaimableNativeContextJob>, RepositoryError> {
        if limit == 0 || limit > 64 {
            return Err(RepositoryError::InvalidInput(
                "Native Context claim scan limit is invalid".to_owned(),
            ));
        }
        let rows = sqlx::query(
            r#"
            SELECT job.tenant_id, job.job_id
            FROM insight_platform.jobs AS job
            JOIN insight_platform.invocations AS query
              ON query.tenant_id = job.tenant_id
             AND query.invocation_id = job.owner_id
             AND query.invocation_kind = 'context'
            WHERE job.work_class = 'context'
              AND job.owner_kind = 'context_query'
              AND job.state IN ('ready', 'retry_scheduled')
              AND job.scheduled_at <= clock_timestamp()
              AND COALESCE(job.retry_at, job.scheduled_at) <= clock_timestamp()
              AND job.deadline > clock_timestamp()
              AND job.attempt_no < job.attempt_limit
              AND query.payload #>> '{admission,context_closure,required_worker_manifest_digest}' = $2
              AND query.payload #>> '{admission,implementation,contract,backend,kind}' = 'native_catalog'
              AND query.payload #>> '{admission,implementation,contract,backend,adapter_contract_digest}' = $3
              AND query.payload #>> '{admission,context_closure,backend,kind}' = 'native_catalog'
              AND query.payload #>> '{admission,context_closure,backend,installed_adapter_digest}' = $4
            ORDER BY job.priority DESC, COALESCE(job.retry_at, job.scheduled_at), job.job_id
            LIMIT $1
            "#,
        )
        .bind(i64::from(limit))
        .bind(worker_manifest_digest.to_string())
        .bind(adapter_contract_digest.to_string())
        .bind(installed_adapter_digest.to_string())
        .fetch_all(self.pool())
        .await?;
        let mut candidates = Vec::with_capacity(usize::from(limit));
        for row in rows {
            let tenant_id = parse_id(&row.try_get::<String, _>("tenant_id")?, "Context tenant")?;
            let job_id = parse_id(&row.try_get::<String, _>("job_id")?, "Context Job")?;
            candidates.push(ClaimableNativeContextJob { tenant_id, job_id });
        }
        Ok(candidates)
    }

    /// Discovers ready RemoteSearch Context Jobs for one exact qualified Worker manifest.
    /// Endpoint, transport-policy, and adapter-contract closure is revalidated by the worker and
    /// Egress Broker before outbound I/O; this scan remains a read-only scheduling hint.
    pub async fn scan_claimable_remote_context_jobs(
        &self,
        worker_manifest_digest: &Sha256Digest,
        limit: u16,
    ) -> Result<Vec<ClaimableRemoteContextJob>, RepositoryError> {
        if limit == 0 || limit > 64 {
            return Err(RepositoryError::InvalidInput(
                "Remote Context claim scan limit is invalid".to_owned(),
            ));
        }
        let rows = sqlx::query(
            r#"
            SELECT job.tenant_id, job.job_id
            FROM insight_platform.jobs AS job
            JOIN insight_platform.invocations AS query
              ON query.tenant_id = job.tenant_id
             AND query.invocation_id = job.owner_id
             AND query.invocation_kind = 'context'
            WHERE job.work_class = 'context'
              AND job.owner_kind = 'context_query'
              AND job.state IN ('ready', 'retry_scheduled')
              AND job.scheduled_at <= clock_timestamp()
              AND COALESCE(job.retry_at, job.scheduled_at) <= clock_timestamp()
              AND job.deadline > clock_timestamp()
              AND job.attempt_no < job.attempt_limit
              AND query.payload #>> '{admission,context_closure,required_worker_manifest_digest}' = $2
              AND query.payload #>> '{admission,implementation,contract,backend,kind}' = 'remote_search'
              AND query.payload #>> '{admission,context_closure,backend,kind}' = 'remote_search'
            ORDER BY job.priority DESC, COALESCE(job.retry_at, job.scheduled_at), job.job_id
            LIMIT $1
            "#,
        )
        .bind(i64::from(limit))
        .bind(worker_manifest_digest.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ClaimableRemoteContextJob {
                    tenant_id: parse_id(&row.try_get::<String, _>("tenant_id")?, "Context tenant")?,
                    job_id: parse_id(&row.try_get::<String, _>("job_id")?, "Context Job")?,
                })
            })
            .collect()
    }
}

impl ContextQueryStore for PgRepository {
    type Error = RepositoryError;
    type Transaction<'a>
        = PgContextQueryTransaction
    where
        Self: 'a;

    async fn begin_context_query(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        self.begin_context_query_transaction().await
    }
}

impl ContextQueryTransaction for PgContextQueryTransaction {
    type Error = RepositoryError;
    type ExecutionRecord = PreparedContextExecution;

    async fn create_context_query(
        &mut self,
        command: CreateContextQuery,
    ) -> Result<CommandOutcome<ContextQueryRecord>, Self::Error> {
        self.create_context_query_inner(command).await
    }

    async fn prepare_context_dispatch(
        &mut self,
        command: PrepareContextDispatch,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error> {
        self.prepare_context_dispatch_inner(command).await
    }

    async fn commit_context_outcome(
        &mut self,
        command: CommitContextOutcome,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error> {
        self.commit_context_outcome_inner(command).await
    }

    async fn commit(self) -> Result<(), Self::Error> {
        self.transaction.commit().await?;
        Ok(())
    }

    async fn rollback(self) -> Result<(), Self::Error> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

impl PgContextQueryTransaction {
    async fn create_context_query_inner(
        &mut self,
        command: CreateContextQuery,
    ) -> Result<CommandOutcome<ContextQueryRecord>, RepositoryError> {
        let mut transaction = self.transaction.begin().await?;
        let outcome =
            Self::create_context_query_in_transaction(&mut transaction, command, self.limits)
                .await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub(crate) async fn create_context_query_in_transaction(
        transaction: &mut Transaction<'_, Postgres>,
        command: CreateContextQuery,
        limits: ContextQueryLimits,
    ) -> Result<CommandOutcome<ContextQueryRecord>, RepositoryError> {
        command.validate_at(Utc::now(), limits)?;
        let database_now = database_now(transaction).await?;
        command.validate_at(database_now, limits)?;
        if claim_command_receipt(
            transaction,
            &command.audit,
            "context_query",
            &command.context_query_id.to_string(),
            "context.create",
        )
        .await?
        {
            let query = load_context_query(
                transaction,
                &command.audit.tenant_id,
                &command.context_query_id,
                false,
                limits,
            )
            .await?;
            require_context_create_replay(&query, &command)?;
            return Ok(CommandOutcome::Replayed(query));
        }

        let run =
            load_run_for_update(transaction, &command.audit.tenant_id, &command.run_id).await?;
        let principal =
            require_tenant_permission(transaction, &command.audit, Permission::ContextQuery)
                .await?;
        let node = load_context_node_for_update(
            transaction,
            &command.audit.tenant_id,
            &command.node_execution_id,
        )
        .await?;
        if node.run_id != run.run_id {
            return Err(RepositoryError::InvalidInput(
                "ContextQuery NodeExecution does not belong to its Run".to_owned(),
            ));
        }
        let binding = run
            .bindings
            .slots
            .iter()
            .find(|slot| slot.slot_id == command.slot_id)
            .and_then(|slot| match &slot.target {
                insight_platform_contracts::FrozenSlotTarget::Context { binding } => {
                    Some(binding.as_ref().clone())
                }
                _ => None,
            })
            .ok_or(RepositoryError::NotFound("Context binding"))?;
        let context_deployment = load_exact_context_deployment(
            transaction,
            &command.audit.tenant_id,
            &binding.context_deployment,
        )
        .await?;
        let context_resource_id = parse_id(&context_deployment.resource_id, "Context resource")?;
        let context_resource =
            load_resource(transaction, &command.audit.tenant_id, &context_resource_id).await?;
        let context_closure = match decode_deployment_closure(&context_deployment.bindings)? {
            DeploymentClosure::ContextSourceInterface(closure) => closure,
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Context Deployment has a non-Context closure".to_owned(),
                ));
            }
        };
        if context_deployment.resource_version_id
            != context_closure.interface.revision_id.to_string()
        {
            return Err(RepositoryError::CorruptRow(
                "Context Deployment root revision differs from Interface closure".to_owned(),
            ));
        }
        let interface_payload = load_enabled_exact_published_version(
            transaction,
            &command.audit.tenant_id,
            &context_closure.interface,
            RegistryResourceKind::ContextSourceInterface,
        )
        .await?;
        let ResourceDocument::ContextSourceInterface(interface) = interface_payload.document else {
            return Err(RepositoryError::CorruptRow(
                "Context Interface revision contains the wrong document".to_owned(),
            ));
        };
        let implementation_payload = load_enabled_exact_published_version(
            transaction,
            &command.audit.tenant_id,
            &context_closure.implementation,
            RegistryResourceKind::ContextSourceImplementation,
        )
        .await?;
        let ResourceDocument::ContextSourceImplementation(implementation) =
            implementation_payload.document
        else {
            return Err(RepositoryError::CorruptRow(
                "Context Implementation revision contains the wrong document".to_owned(),
            ));
        };
        if implementation.interface_revision != context_closure.interface
            || implementation.backend_kind != context_closure.backend.kind()
        {
            return Err(RepositoryError::Conflict(
                "Context Deployment implementation binding",
            ));
        }
        let (dataset_view, dataset_generation) = resolve_context_dataset_view(
            transaction,
            &command.audit.tenant_id,
            &run,
            &binding,
            &context_deployment,
        )
        .await?;
        lock_context_input(
            transaction,
            &command.audit.tenant_id,
            &command.run_id,
            &command.request,
        )
        .await?;
        let interface_revision = context_closure.interface.clone();
        let implementation_revision = context_closure.implementation.clone();
        let facts = ContextAdmissionFacts {
            run_state: parse_run_state(&run.state)?,
            run_version: parse_u64(run.version, "Run version")?,
            run_pause_requested: run.current.control.pause_requested,
            run_cancel_requested: run.current.control.cancel_requested_at.is_some(),
            run_timeout_requested: run.current.control.timeout_requested_at.is_some(),
            run_deadline: run.deadline,
            run_bindings: run.bindings.clone(),
            node_state: node.state,
            node_version: node.version,
            node_kind: node.node_kind,
            node_deadline: node.deadline,
            context_gate_enabled: context_resource.lifecycle_state
                == EntityLifecycle::Active.as_str()
                && context_resource.gate_state == "enabled",
            binding,
            context_closure,
            interface_revision,
            interface,
            implementation_revision,
            implementation,
            dataset_view,
            dataset_generation,
            principal,
            policy_generation: run.bindings.principal.binding_generation,
            database_now,
        };
        let query = decide_context_query_admission(&command, facts, limits)?;
        insert_context_query(transaction, &query).await?;
        insert_context_input_artifact_reference(
            transaction,
            &query,
            &command.request,
            database_now,
        )
        .await?;
        append_command_event(
            transaction,
            &command.audit,
            "context_query",
            &query.context_query_id.to_string(),
            as_i64(query.version, "ContextQuery version")?,
            "context.created",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "context_binding_id": query.payload.admission.binding.context_binding_id,
                    "context_deployment_id": query.deployment_id,
                    "dataset_view": query.payload.admission.dataset_view,
                    "node_execution_id": query.node_execution_id,
                    "run_id": query.run_id,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            transaction,
            &command.audit,
            &query.context_query_id.to_string(),
            "created",
        )
        .await?;
        Ok(CommandOutcome::Applied(query))
    }

    async fn prepare_context_dispatch_inner(
        &mut self,
        command: PrepareContextDispatch,
    ) -> Result<CommandOutcome<PreparedContextExecution>, RepositoryError> {
        let mut transaction = self.transaction.begin().await?;
        let outcome =
            Self::prepare_context_dispatch_in_transaction(&mut transaction, command, self.limits)
                .await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub(crate) async fn prepare_context_dispatch_in_transaction(
        transaction: &mut Transaction<'_, Postgres>,
        command: PrepareContextDispatch,
        limits: ContextQueryLimits,
    ) -> Result<CommandOutcome<PreparedContextExecution>, RepositoryError> {
        let database_now = database_now(transaction).await?;
        if claim_command_receipt(
            transaction,
            &command.audit,
            "context_query",
            &command.context_query_id.to_string(),
            "context.dispatch.prepare",
        )
        .await?
        {
            let query = load_context_query(
                transaction,
                &command.audit.tenant_id,
                &command.context_query_id,
                false,
                limits,
            )
            .await?;
            let job = load_context_job(
                transaction,
                &command.audit.tenant_id,
                &command.job_id,
                false,
            )
            .await?;
            return Ok(CommandOutcome::Replayed(PreparedContextExecution {
                query,
                job,
            }));
        }
        require_tenant_permission(transaction, &command.audit, Permission::ContextQuery).await?;
        let current = load_context_query(
            transaction,
            &command.audit.tenant_id,
            &command.context_query_id,
            true,
            limits,
        )
        .await?;
        let prepared = decide_prepare_context_dispatch(&current, &command, database_now, limits)?;
        insert_context_job(transaction, &prepared).await?;
        update_context_query(transaction, &current, &prepared.query, limits).await?;
        append_command_event(
            transaction,
            &command.audit,
            "context_query",
            &command.context_query_id.to_string(),
            as_i64(prepared.query.version, "ContextQuery version")?,
            "context.ready",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "job_id": prepared.job.job_id,
                    "run_id": prepared.query.run_id,
                    "state": prepared.query.state,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            transaction,
            &command.audit,
            &command.context_query_id.to_string(),
            "ready",
        )
        .await?;
        let job = load_context_job(
            transaction,
            &command.audit.tenant_id,
            &command.job_id,
            false,
        )
        .await?;
        Ok(CommandOutcome::Applied(PreparedContextExecution {
            query: prepared.query,
            job,
        }))
    }

    async fn commit_context_outcome_inner(
        &mut self,
        command: CommitContextOutcome,
    ) -> Result<CommandOutcome<PreparedContextExecution>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let receipt_payload = context_worker_receipt_payload(&command)?;
        if claim_context_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "context.outcome.commit",
            &receipt_payload,
        )
        .await?
        {
            let query = load_context_query(
                &mut transaction,
                &command.audit.tenant_id,
                &command.context_query_id,
                false,
                self.limits,
            )
            .await?;
            let job = load_context_job(
                &mut transaction,
                &command.audit.tenant_id,
                &command.job_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(PreparedContextExecution {
                query,
                job,
            }));
        }
        let observed_job = load_context_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            false,
        )
        .await?;
        require_context_outcome_candidate(&observed_job, &command.fence)?;
        let quota_lines = lock_context_quota_bundle(&mut transaction, &observed_job).await?;
        let current_query = load_context_query(
            &mut transaction,
            &command.audit.tenant_id,
            &command.context_query_id,
            true,
            self.limits,
        )
        .await?;
        if current_query.version != command.expected_query_version
            || current_query.payload.current_job_id.as_ref() != Some(&command.job_id)
        {
            return Err(RepositoryError::Conflict("ContextQuery outcome"));
        }
        let current_job = load_context_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        require_same_context_job(&observed_job, &current_job)?;
        let projection = job_projection(&current_job)?;
        let payload: ContextJobPayload =
            decode_versioned_payload(&current_job.payload, "Context Job")?;
        let decision = decide_context_outcome(
            &current_query,
            &projection,
            &payload,
            &command.fence,
            &command.outcome,
            database_now,
            self.limits,
        )?;
        settle_context_quota(
            &mut transaction,
            &current_job,
            &quota_lines,
            &command.quota_entry_ids,
            decision.consumed_query,
            decision.settled_result_bytes,
            &command.audit.request_digest,
        )
        .await?;
        if let Some(output) = decision.output.as_ref() {
            insert_context_output(&mut transaction, &decision.query, output, database_now).await?;
            let exact = decision
                .query
                .payload
                .result
                .as_ref()
                .ok_or(RepositoryError::Conflict("Context terminal result"))?
                .output
                .clone();
            settle_context_leaf_success_in_transaction(
                &mut transaction,
                &decision.query,
                &command.job_id,
                &exact,
                command
                    .resume_mutations
                    .as_ref()
                    .ok_or(RepositoryError::Conflict("Context resume mutations"))?,
                self.scope_environment_limits,
                database_now,
            )
            .await?;
        } else if let insight_platform_context::ContextBackendOutcome::PermanentFailure {
            failure,
        } = &command.outcome
        {
            crate::repository::settle_context_leaf_failure_in_transaction(
                &mut transaction,
                &decision.query,
                &command.job_id,
                failure,
                command
                    .failure_mutations
                    .as_ref()
                    .ok_or(RepositoryError::Conflict(
                        "Context failure convergence mutations",
                    ))?,
                database_now,
            )
            .await?;
        }
        let job = update_context_job(
            &mut transaction,
            &current_job,
            &decision.job,
            &decision.job_payload,
            None,
            database_now,
        )
        .await?;
        update_context_query(
            &mut transaction,
            &current_query,
            &decision.query,
            self.limits,
        )
        .await?;
        let (event_type, disposition) = match command.outcome.kind() {
            insight_platform_contracts::ContextBackendOutcomeKind::Completed => {
                ("context.completed", "completed")
            }
            insight_platform_contracts::ContextBackendOutcomeKind::Deferred => {
                ("context.deferred", "deferred")
            }
            insight_platform_contracts::ContextBackendOutcomeKind::RetryableFailure => {
                ("context.retry_scheduled", "retry_scheduled")
            }
            insight_platform_contracts::ContextBackendOutcomeKind::PermanentFailure => {
                ("context.failed", "failed")
            }
        };
        append_scheduler_event(
            &mut transaction,
            &current_job.tenant_id,
            &command.audit.event_id,
            &command.audit.outbox_id,
            "context_query",
            &command.context_query_id.to_string(),
            as_i64(decision.query.version, "ContextQuery version")?,
            Some(&decision.query.run_id.to_string()),
            event_type,
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "item_count": decision.query.payload.result.as_ref().map_or(0, |result| result.observation.items.len()),
                    "job_id": decision.job.job_id,
                    "lease_generation": decision.job.lease_generation,
                    "result_bytes": decision.settled_result_bytes,
                    "state": decision.query.state,
                }),
            )?,
        )
        .await?;
        terminalize_context_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            disposition,
            &command.context_query_id,
        )
        .await?;
        let query = load_context_query(
            &mut transaction,
            &command.audit.tenant_id,
            &command.context_query_id,
            false,
            self.limits,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(PreparedContextExecution {
            query,
            job,
        }))
    }

    pub async fn wake_context_dispatch(
        &mut self,
        command: WakeContextDispatch,
    ) -> Result<CommandOutcome<PreparedContextExecution>, RepositoryError> {
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.audit.validate_at(database_now)?;
        let receipt_payload = context_signal_receipt_payload(&command)?;
        if claim_context_signal_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "context.wake",
            &receipt_payload,
        )
        .await?
        {
            let query = load_context_query(
                &mut transaction,
                &command.audit.tenant_id,
                &command.context_query_id,
                false,
                self.limits,
            )
            .await?;
            let job = load_context_job(
                &mut transaction,
                &command.audit.tenant_id,
                &command.job_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(PreparedContextExecution {
                query,
                job,
            }));
        }
        let query = load_context_query(
            &mut transaction,
            &command.audit.tenant_id,
            &command.context_query_id,
            true,
            self.limits,
        )
        .await?;
        let current_job = load_context_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        let projection = job_projection(&current_job)?;
        let payload: ContextJobPayload =
            decode_versioned_payload(&current_job.payload, "Context Job")?;
        let decision = decide_wake_context_dispatch(
            &query,
            &projection,
            &payload,
            &command,
            database_now,
            self.limits,
        );
        let (query, job, payload) = match decision {
            Ok(applied) => applied,
            Err(ContextQueryError::FirstWinnerLost) => {
                terminalize_context_signal_receipt(
                    &mut transaction,
                    &command.audit,
                    &command.job_id,
                    "rejected_stale",
                    &command.context_query_id,
                )
                .await?;
                transaction.commit().await?;
                return Ok(CommandOutcome::Applied(PreparedContextExecution {
                    query,
                    job: current_job,
                }));
            }
            Err(failure) => return Err(failure.into()),
        };
        let job = update_context_job(
            &mut transaction,
            &current_job,
            &job,
            &payload,
            None,
            database_now,
        )
        .await?;
        append_scheduler_event(
            &mut transaction,
            &command.audit.tenant_id.to_string(),
            &command.audit.event_id,
            &command.audit.outbox_id,
            "context_query",
            &command.context_query_id.to_string(),
            as_i64(query.version, "ContextQuery version")?,
            Some(&query.run_id.to_string()),
            "context.woken",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "job_id": job.job_id,
                    "wake_generation": command.expected_wake_generation,
                    "wake_source": command.source,
                }),
            )?,
        )
        .await?;
        terminalize_context_signal_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "applied",
            &command.context_query_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(PreparedContextExecution {
            query,
            job,
        }))
    }
}

async fn load_exact_context_deployment(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    exact: &insight_platform_contracts::ExactDeploymentRef,
) -> Result<DeploymentRecord, RepositoryError> {
    if exact.resource_kind != ResourceKind::ContextDeployment {
        return Err(RepositoryError::InvalidInput(
            "Context binding has the wrong Deployment kind".to_owned(),
        ));
    }
    let deployment = load_deployment(transaction, tenant_id, &exact.deployment_id).await?;
    if deployment.bindings.digest != exact.deployment_digest.to_string() {
        return Err(RepositoryError::Conflict(
            "exact Context Deployment binding",
        ));
    }
    Ok(deployment)
}

async fn resolve_context_dataset_view(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    run: &RunRecord,
    binding: &insight_platform_contracts::ContextBindingSnapshot,
    context_deployment: &DeploymentRecord,
) -> Result<(ContextDatasetView, Option<ContextDatasetGenerationSpec>), RepositoryError> {
    let exact = match &binding.consistency {
        ContextConsistencyPolicy::PinnedGeneration { generation } => Some(generation.clone()),
        ContextConsistencyPolicy::PinAtRunAdmission { .. } => Some(
            run.bindings
                .context_dataset_views
                .iter()
                .find(|view| view.context_binding_id == binding.context_binding_id)
                .ok_or(RepositoryError::NotFound("Run Context Dataset view"))?
                .generation
                .clone(),
        ),
        ContextConsistencyPolicy::LatestAtQueryStart { dataset_id } => Some(
            load_active_dataset_generation(transaction, tenant_id, dataset_id)
                .await?
                .0,
        ),
        ContextConsistencyPolicy::ExternalObservation => None,
    };
    match exact {
        Some(exact) => {
            let spec = load_exact_dataset_generation(transaction, tenant_id, &exact).await?;
            if spec.context_deployment.deployment_id.to_string() != context_deployment.deployment_id
                || spec.context_deployment.deployment_digest.to_string()
                    != context_deployment.bindings.digest
            {
                return Err(RepositoryError::Conflict(
                    "Dataset Generation Context Deployment",
                ));
            }
            Ok((ContextDatasetView::Generation { exact }, Some(spec)))
        }
        None => Ok((
            ContextDatasetView::ExternalObservation {
                source_identity_digest: binding.context_deployment.deployment_digest.clone(),
            },
            None,
        )),
    }
}

async fn load_active_dataset_generation(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    dataset_id: &ResourceId,
) -> Result<(ExactDatasetGenerationRef, ContextDatasetGenerationSpec), RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT version.resource_version_id, version.content_digest,
               version.payload_schema_version, version.payload, version.payload_digest
        FROM insight_platform.resources AS resource
        JOIN insight_platform.resource_versions AS version
          ON version.tenant_id = resource.tenant_id
         AND version.resource_id = resource.resource_id
         AND version.resource_version_id = resource.active_version_id
        WHERE resource.tenant_id = $1 AND resource.resource_id = $2
          AND resource.resource_kind = 'context_dataset'
          AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
          AND version.resource_version_kind = 'dataset_generation'
        FOR SHARE OF resource, version
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(dataset_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound(
        "active Context Dataset Generation",
    ))?;
    let generation_id = parse_id(
        &row.try_get::<String, _>("resource_version_id")?,
        "Dataset Generation",
    )?;
    let generation_digest: Sha256Digest = row
        .try_get::<String, _>("content_digest")?
        .parse()
        .map_err(|failure| RepositoryError::CorruptRow(format!("Dataset digest: {failure}")))?;
    let exact = ExactDatasetGenerationRef {
        dataset_id: dataset_id.clone(),
        generation_id,
        generation_digest,
    };
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let published: PublishedVersionPayload = decode_typed_payload(&payload, "Dataset Generation")?;
    published
        .validate_for(RegistryResourceKind::ContextDataset, &exact.generation_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let ResourceDocument::ContextDataset(spec) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "Dataset Generation contains the wrong document".to_owned(),
        ));
    };
    Ok((exact, spec.generation))
}

async fn load_exact_dataset_generation(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    exact: &ExactDatasetGenerationRef,
) -> Result<ContextDatasetGenerationSpec, RepositoryError> {
    exact
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let (active_or_exact, spec) = load_dataset_generation_by_id(
        transaction,
        tenant_id,
        &exact.dataset_id,
        &exact.generation_id,
    )
    .await?;
    if active_or_exact != *exact {
        return Err(RepositoryError::Conflict("exact Dataset Generation"));
    }
    Ok(spec)
}

async fn load_dataset_generation_by_id(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    dataset_id: &ResourceId,
    generation_id: &ResourceId,
) -> Result<(ExactDatasetGenerationRef, ContextDatasetGenerationSpec), RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT version.content_digest, version.payload_schema_version,
               version.payload, version.payload_digest
        FROM insight_platform.resources AS resource
        JOIN insight_platform.resource_versions AS version
          ON version.tenant_id = resource.tenant_id
         AND version.resource_id = resource.resource_id
        WHERE resource.tenant_id = $1 AND resource.resource_id = $2
          AND resource.resource_kind = 'context_dataset'
          AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
          AND version.resource_version_id = $3
          AND version.resource_version_kind = 'dataset_generation'
        FOR SHARE OF resource, version
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(dataset_id.to_string())
    .bind(generation_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound(
        "exact Context Dataset Generation",
    ))?;
    let exact = ExactDatasetGenerationRef {
        dataset_id: dataset_id.clone(),
        generation_id: generation_id.clone(),
        generation_digest: row
            .try_get::<String, _>("content_digest")?
            .parse()
            .map_err(|failure| RepositoryError::CorruptRow(format!("Dataset digest: {failure}")))?,
    };
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let published: PublishedVersionPayload = decode_typed_payload(&payload, "Dataset Generation")?;
    published
        .validate_for(RegistryResourceKind::ContextDataset, generation_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let ResourceDocument::ContextDataset(spec) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "Dataset Generation contains the wrong document".to_owned(),
        ));
    };
    Ok((exact, spec.generation))
}

async fn lock_context_input(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    run_id: &ResourceId,
    request: &insight_platform_context::ContextQueryRequest,
) -> Result<ValueRef, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT run_id, node_id, value_kind, classification, schema_digest,
               content_digest, inline_value, artifact_id
        FROM insight_platform.run_values
        WHERE tenant_id = $1 AND value_id = $2
        FOR SHARE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(request.input.value_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Context query input RunValue"))?;
    if row.try_get::<String, _>("run_id")? != run_id.to_string()
        || row.try_get::<String, _>("value_kind")? != request.input.value_kind
        || row.try_get::<String, _>("classification")? != request.input.classification.as_str()
        || row.try_get::<String, _>("schema_digest")? != request.input.schema_digest.to_string()
        || row.try_get::<String, _>("content_digest")? != request.input.content_digest.to_string()
        || row.try_get::<Option<String>, _>("node_id")?
            != request
                .input
                .producing_node_id
                .as_ref()
                .map(ToString::to_string)
    {
        return Err(RepositoryError::Conflict("Context query input RunValue"));
    }
    let inline: Option<serde_json::Value> = row.try_get("inline_value")?;
    let artifact_id: Option<String> = row.try_get("artifact_id")?;
    match (&request.input.storage, inline, artifact_id) {
        (insight_platform_invocations::InvocationValueStorage::Inline, Some(value), None) => {
            let digest: Sha256Digest = canonical_digest(&value)
                .map_err(|_| RepositoryError::CorruptRow("Context input JSON".to_owned()))?
                .parse()
                .map_err(|_| RepositoryError::CorruptRow("Context input digest".to_owned()))?;
            if digest != request.input.content_digest {
                return Err(RepositoryError::Conflict("Context query input digest"));
            }
            Ok(ValueRef::Inline { value })
        }
        (
            insight_platform_invocations::InvocationValueStorage::Artifact { artifact },
            None,
            Some(stored),
        ) if stored == artifact.artifact_id().to_string() => {
            require_ready_run_artifact(transaction, tenant_id, artifact).await?;
            Ok(ValueRef::Artifact {
                artifact: artifact.clone(),
            })
        }
        _ => Err(RepositoryError::Conflict("Context query input storage")),
    }
}

async fn insert_context_query(
    transaction: &mut Transaction<'_, Postgres>,
    record: &ContextQueryRecord,
) -> Result<(), RepositoryError> {
    let payload = TypedPayload::from_versioned(1, &record.payload, 1_048_576)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.invocations (
            tenant_id, invocation_id, invocation_kind, owner_kind, owner_id,
            logical_key, run_id, node_id, deployment_id, state, version,
            input_value_id, output_value_id, effect_key_digest,
            payload_schema_version, payload, payload_digest, deadline,
            retry_at, started_at, terminal_at, created_at, updated_at
        ) VALUES (
            $1, $2, 'context', 'node_execution', $3,
            $4, $5, $3, $6, $7, $8,
            $9, NULL, NULL, $10, $11, $12, $13,
            NULL, NULL, NULL, $14, $14
        )
        "#,
    )
    .bind(record.tenant_id.to_string())
    .bind(record.context_query_id.to_string())
    .bind(record.node_execution_id.to_string())
    .bind(&record.logical_key)
    .bind(record.run_id.to_string())
    .bind(record.deployment_id.to_string())
    .bind(record.state.as_str())
    .bind(as_i64(record.version, "ContextQuery version")?)
    .bind(record.input_value_id.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(record.deadline)
    .bind(record.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_context_job(
    transaction: &mut Transaction<'_, Postgres>,
    prepared: &PreparedContextDispatch,
) -> Result<(), RepositoryError> {
    let payload = TypedPayload::from_versioned(1, &prepared.job_payload, 1_048_576)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
            tenant_id, job_id, work_class, owner_kind, owner_id, invocation_id,
            run_id, node_id, state, version, attempt_no, attempt_limit, lease_epoch,
            scheduled_at, deadline, priority, request_digest,
            payload_schema_version, payload, payload_digest, created_at, updated_at
        ) VALUES (
            $1, $2, 'context', 'context_query', $3, $3,
            $4, $5, 'ready', 1, 0, $6, 0,
            $7, $8, 0, $9, $10, $11, $12, $13, $13
        )
        "#,
    )
    .bind(prepared.query.tenant_id.to_string())
    .bind(prepared.job.job_id.to_string())
    .bind(prepared.query.context_query_id.to_string())
    .bind(prepared.query.run_id.to_string())
    .bind(prepared.query.node_execution_id.to_string())
    .bind(i32::try_from(prepared.job.attempt_limit).map_err(|_| {
        RepositoryError::InvalidInput("Context attempt limit exceeds integer".to_owned())
    })?)
    .bind(prepared.job.scheduled_at)
    .bind(prepared.job.deadline)
    .bind(prepared.job_payload.binding.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(prepared.query.updated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn update_context_query(
    transaction: &mut Transaction<'_, Postgres>,
    current: &ContextQueryRecord,
    next: &ContextQueryRecord,
    limits: ContextQueryLimits,
) -> Result<(), RepositoryError> {
    next.validate(limits)?;
    let payload = TypedPayload::from_versioned(1, &next.payload, 1_048_576)?;
    let updated = sqlx::query(
        r#"
        UPDATE insight_platform.invocations
        SET state = $4, version = $5, output_value_id = $6,
            payload_schema_version = $7, payload = $8, payload_digest = $9,
            retry_at = $10, started_at = $11, terminal_at = $12, updated_at = $13
        WHERE tenant_id = $1 AND invocation_id = $2 AND version = $3
          AND invocation_kind = 'context'
        RETURNING invocation_id
        "#,
    )
    .bind(next.tenant_id.to_string())
    .bind(next.context_query_id.to_string())
    .bind(as_i64(current.version, "ContextQuery version")?)
    .bind(next.state.as_str())
    .bind(as_i64(next.version, "ContextQuery version")?)
    .bind(next.output_value_id.as_ref().map(ToString::to_string))
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(next.retry_at)
    .bind(next.started_at)
    .bind(next.terminal_at)
    .bind(next.updated_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if updated.is_none() {
        return Err(RepositoryError::Conflict("ContextQuery"));
    }
    Ok(())
}

pub(crate) async fn load_context_query(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    context_query_id: &ResourceId,
    for_update: bool,
    limits: ContextQueryLimits,
) -> Result<ContextQueryRecord, RepositoryError> {
    let query = if for_update {
        "SELECT * FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2 FOR UPDATE"
    } else {
        "SELECT * FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2"
    };
    let row = sqlx::query(query)
        .bind(tenant_id.to_string())
        .bind(context_query_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("ContextQuery"))?;
    context_query_from_row(row, limits)
}

fn context_query_from_row(
    row: PgRow,
    limits: ContextQueryLimits,
) -> Result<ContextQueryRecord, RepositoryError> {
    if row.try_get::<String, _>("invocation_kind")? != "context"
        || row.try_get::<String, _>("owner_kind")? != "node_execution"
    {
        return Err(RepositoryError::CorruptRow(
            "ContextQuery row has the wrong Invocation or owner kind".to_owned(),
        ));
    }
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let payload: ContextQueryPayload = decode_versioned_payload(&payload, "ContextQuery")?;
    let node_execution_id = parse_id(
        &row.try_get::<String, _>("node_id")?,
        "ContextQuery NodeExecution",
    )?;
    if row.try_get::<String, _>("owner_id")? != node_execution_id.to_string() {
        return Err(RepositoryError::CorruptRow(
            "ContextQuery owner differs from its NodeExecution".to_owned(),
        ));
    }
    let record = ContextQueryRecord {
        tenant_id: parse_id(
            &row.try_get::<String, _>("tenant_id")?,
            "ContextQuery tenant",
        )?,
        context_query_id: parse_id(&row.try_get::<String, _>("invocation_id")?, "ContextQuery")?,
        run_id: parse_id(&row.try_get::<String, _>("run_id")?, "ContextQuery Run")?,
        node_execution_id,
        logical_key: row.try_get("logical_key")?,
        deployment_id: parse_id(
            &row.try_get::<String, _>("deployment_id")?,
            "ContextQuery Deployment",
        )?,
        input_value_id: parse_id(
            &row.try_get::<String, _>("input_value_id")?,
            "ContextQuery input",
        )?,
        output_value_id: row
            .try_get::<Option<String>, _>("output_value_id")?
            .map(|value| parse_id(&value, "ContextQuery output"))
            .transpose()?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<ContextQueryState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_u64(row.try_get("version")?, "ContextQuery version")?,
        payload,
        deadline: row.try_get("deadline")?,
        retry_at: row.try_get("retry_at")?,
        started_at: row.try_get("started_at")?,
        terminal_at: row.try_get("terminal_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    };
    record
        .validate(limits)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(record)
}

pub(crate) async fn load_context_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    for_update: bool,
) -> Result<JobRecord, RepositoryError> {
    let record = if for_update {
        load_job_for_update_by_text(transaction, &tenant_id.to_string(), &job_id.to_string())
            .await?
    } else {
        let row =
            sqlx::query("SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2")
                .bind(tenant_id.to_string())
                .bind(job_id.to_string())
                .fetch_optional(&mut **transaction)
                .await?
                .ok_or(RepositoryError::NotFound("Context Job"))?;
        job_from_row(row)?
    };
    if record.work_class != WorkClass::Context.as_str() || record.owner_kind != "context_query" {
        return Err(RepositoryError::CorruptRow(
            "Job is not a Context Job".to_owned(),
        ));
    }
    Ok(record)
}

impl PgRepository {
    pub async fn drive_expired_context_jobs(
        &self,
        command: DriveExpiredContextJobs,
    ) -> Result<SafetyScanPage<RecoveredContextExecution>, RepositoryError> {
        command.validate(self.recovery_batch_limit(), self.recovery_shard_limit())?;
        let scan_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(self.pool())
            .await?;
        let rows = sqlx::query(
            r#"
            SELECT job.*, job.lease_expires_at AS scan_sort_at
            FROM insight_platform.jobs AS job
            JOIN insight_platform.invocations AS query
              ON query.tenant_id = job.tenant_id AND query.invocation_id = job.owner_id
            WHERE job.work_class = 'context' AND job.owner_kind = 'context_query'
              AND job.state = 'running' AND job.terminal_at IS NULL
              AND job.lease_expires_at <= $1 AND job.attempt_no < job.attempt_limit
              AND job.deadline > $1 AND query.state = 'in_flight' AND query.terminal_at IS NULL
              AND mod(('x' || right(job.job_id, 8))::bit(32)::bigint, $4) = $3
              AND ($5::timestamptz IS NULL OR
                   (job.lease_expires_at, job.tenant_id, job.job_id) >
                   ($5::timestamptz, $6::text, $7::text))
            ORDER BY job.lease_expires_at, job.tenant_id, job.job_id
            LIMIT $2
            "#,
        )
        .bind(scan_now)
        .bind(i64::from(command.limit))
        .bind(i64::from(command.shard.index))
        .bind(i64::from(command.shard.count))
        .bind(command.after.as_ref().map(|cursor| cursor.sort_at))
        .bind(
            command
                .after
                .as_ref()
                .map(|cursor| cursor.tenant_id.to_string()),
        )
        .bind(
            command
                .after
                .as_ref()
                .map(|cursor| cursor.item_id.to_string()),
        )
        .fetch_all(self.pool())
        .await?;
        let scanned_count = rows.len();
        let last_cursor = rows
            .last()
            .map(|row| safety_scan_cursor_from_row(row, "job_id", ResourceKind::Job))
            .transpose()?;
        let candidates = rows
            .into_iter()
            .map(job_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let mut recovered = Vec::with_capacity(candidates.len());
        for (observed, slot) in candidates.into_iter().zip(&command.slots) {
            match self
                .recover_expired_context_job(
                    &observed,
                    slot,
                    scan_now,
                    command.retry_backoff_milliseconds,
                )
                .await
            {
                Ok(Some(record)) => recovered.push(record),
                Ok(None)
                | Err(RepositoryError::Conflict(_))
                | Err(RepositoryError::StaleFence)
                | Err(RepositoryError::LeaseExpired) => {}
                Err(failure) => return Err(failure),
            }
        }
        Ok(safety_scan_page(
            recovered,
            scanned_count,
            command.limit,
            last_cursor,
        ))
    }

    async fn recover_expired_context_job(
        &self,
        observed: &JobRecord,
        slot: &ExpiredContextRecoverySlot,
        scan_now: DateTime<Utc>,
        retry_backoff_milliseconds: u64,
    ) -> Result<Option<RecoveredContextExecution>, RepositoryError> {
        let mut transaction = self.pool().begin().await?;
        let quota_lines = lock_context_quota_bundle(&mut transaction, observed).await?;
        let tenant_id = parse_id(&observed.tenant_id, "expired Context tenant")?;
        let query_id = parse_id(&observed.owner_id, "expired ContextQuery")?;
        let job_id = parse_id(&observed.job_id, "expired Context Job")?;
        let current_query = load_context_query(
            &mut transaction,
            &tenant_id,
            &query_id,
            true,
            self.context_query_limits(),
        )
        .await?;
        let current_job = load_context_job(&mut transaction, &tenant_id, &job_id, true).await?;
        let database_now = database_now(&mut transaction).await?;
        if database_now < scan_now
            || current_job.version != observed.version
            || current_job.lease_epoch != observed.lease_epoch
            || current_job.lease_token_digest != observed.lease_token_digest
            || current_job.lease_expires_at != observed.lease_expires_at
            || current_job.payload.digest != observed.payload.digest
            || current_job.quota_reservation_id != observed.quota_reservation_id
            || current_job
                .lease_expires_at
                .is_none_or(|expires_at| expires_at > database_now)
        {
            transaction.rollback().await?;
            return Ok(None);
        }
        let projection = job_projection(&current_job)?;
        let payload: ContextJobPayload =
            decode_versioned_payload(&current_job.payload, "Context Job")?;
        let retry_at = database_now
            .checked_add_signed(Duration::milliseconds(
                i64::try_from(retry_backoff_milliseconds).map_err(|_| {
                    RepositoryError::InvalidInput("Context retry backoff exceeds bigint".to_owned())
                })?,
            ))
            .filter(|retry_at| *retry_at < current_query.deadline)
            .ok_or(RepositoryError::Conflict("expired Context retry deadline"))?;
        let observation_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "context_query_id": query_id,
            "job_id": job_id,
            "lease_generation": projection.lease_generation,
            "observed_job_version": projection.version,
            "reason": "worker_lease_expired",
            "schema_version": 1,
        }))
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
        .parse()
        .map_err(|failure: insight_platform_contracts::NominalTypeError| {
            RepositoryError::InvalidInput(failure.to_string())
        })?;
        let decision = decide_expired_context_lease(
            &current_query,
            &projection,
            &payload,
            &ExpiredContextLeaseObservation {
                observed_job_version: projection.version,
                observed_lease_generation: projection.lease_generation,
                retry_at,
                observation_digest: observation_digest.clone(),
            },
            database_now,
            self.context_query_limits(),
        )?;
        settle_context_quota(
            &mut transaction,
            &current_job,
            &quota_lines,
            &slot.quota_entry_ids,
            decision.consumed_query,
            decision.settled_result_bytes,
            &payload.binding.request_digest,
        )
        .await?;
        let job = update_context_job(
            &mut transaction,
            &current_job,
            &decision.job,
            &decision.job_payload,
            None,
            database_now,
        )
        .await?;
        update_context_query(
            &mut transaction,
            &current_query,
            &decision.query,
            self.context_query_limits(),
        )
        .await?;
        append_scheduler_event(
            &mut transaction,
            &current_job.tenant_id,
            &slot.event_id,
            &slot.outbox_id,
            "context_query",
            &query_id.to_string(),
            as_i64(decision.query.version, "ContextQuery version")?,
            Some(&decision.query.run_id.to_string()),
            "context.lease_recovered",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "attempt_count": decision.job.attempt_count,
                    "job_id": job_id,
                    "lease_generation": decision.job.lease_generation,
                    "observation_digest": observation_digest,
                    "state": decision.query.state,
                }),
            )?,
        )
        .await?;
        transaction.commit().await?;
        Ok(Some(RecoveredContextExecution {
            query: decision.query,
            job,
        }))
    }

    pub async fn claim_context_jobs(
        &self,
        command: ClaimContextJobs,
    ) -> Result<Vec<ClaimedContextExecution>, RepositoryError> {
        command.validate()?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let mut slots = command.slots.iter().collect::<Vec<_>>();
        slots.sort_by(|left, right| left.job_id.cmp(&right.job_id));
        let mut observed = Vec::with_capacity(slots.len());
        for slot in &slots {
            let job =
                load_context_job(&mut transaction, &slot.tenant_id, &slot.job_id, false).await?;
            let payload: ContextJobPayload = decode_versioned_payload(&job.payload, "Context Job")?;
            observed.push((job, payload, *slot));
        }
        let mut accounts = lock_context_quota_accounts(&mut transaction, &observed).await?;
        let mut claimed = Vec::with_capacity(observed.len());
        for (observed_job, observed_payload, slot) in observed {
            let tenant_id = parse_id(&observed_job.tenant_id, "Context Job tenant")?;
            let run_id = parse_required_id(
                observed_job.run_id.as_deref(),
                ResourceKind::Run,
                "Context Job Run",
            )?;
            let node_id = parse_required_id(
                observed_job.node_id.as_deref(),
                ResourceKind::NodeExecution,
                "Context Job Node",
            )?;
            let query_id = parse_id(&observed_job.owner_id, "ContextQuery owner")?;
            let run = load_run_for_update(&mut transaction, &tenant_id, &run_id).await?;
            let node = load_context_node_for_update(&mut transaction, &tenant_id, &node_id).await?;
            let query = load_context_query(
                &mut transaction,
                &tenant_id,
                &query_id,
                true,
                self.context_query_limits(),
            )
            .await?;
            if query
                .payload
                .admission
                .context_closure
                .required_worker_manifest_digest
                != command.worker_manifest_digest
            {
                return Err(RepositoryError::Conflict("Context Worker manifest"));
            }
            let query_input = lock_context_input(
                &mut transaction,
                &tenant_id,
                &run_id,
                &query.payload.admission.request,
            )
            .await?;
            let current_job =
                load_context_job(&mut transaction, &tenant_id, &slot.job_id, true).await?;
            require_same_context_job(&observed_job, &current_job)?;
            require_context_claim_parents(&mut transaction, &query, &run, &node, database_now)
                .await?;
            let projection = job_projection(&current_job)?;
            observed_payload.validate_for(&query, &projection, self.context_query_limits())?;
            let worker = ContextLeaseIdentity {
                worker_process_generation_id: command.worker_process_generation_id.clone(),
                lease_token_digest: slot.lease_token_digest.clone(),
            };
            let (_, leased, payload) = decide_claim_context_dispatch(
                &query,
                &projection,
                &observed_payload,
                &worker,
                database_now,
                command.lease_policy,
                self.context_query_limits(),
            )?;
            let lease_fence = insight_platform_jobs::JobFence {
                expected_version: leased.version,
                worker_process_generation_id: command.worker_process_generation_id.clone(),
                lease_generation: leased.lease_generation,
                token_digest: slot.lease_token_digest.clone(),
            };
            let (next_query, running, next_payload) = decide_start_context_dispatch(
                &query,
                &leased,
                &payload,
                &lease_fence,
                database_now,
                self.context_query_limits(),
            )?;
            let quota_account_ids = reserve_context_quota(
                &mut transaction,
                &mut accounts,
                &current_job,
                &query.deployment_id,
                query.payload.admission.quota_ceiling,
                slot,
                &running,
            )
            .await?;
            let job = update_context_job(
                &mut transaction,
                &current_job,
                &running,
                &next_payload,
                Some(&slot.quota_reservation_id),
                database_now,
            )
            .await?;
            update_context_query(
                &mut transaction,
                &query,
                &next_query,
                self.context_query_limits(),
            )
            .await?;
            append_scheduler_event(
                &mut transaction,
                &observed_job.tenant_id,
                &slot.event_id,
                &slot.outbox_id,
                "context_query",
                &query_id.to_string(),
                as_i64(next_query.version, "ContextQuery version")?,
                Some(&run_id.to_string()),
                "context.started",
                &TypedPayload::new(
                    1,
                    &serde_json::json!({
                        "attempt_count": running.attempt_count,
                        "job_id": running.job_id,
                        "lease_generation": running.lease_generation,
                        "quota_account_ids": quota_account_ids,
                        "quota_reservation_id": slot.quota_reservation_id,
                        "worker_process_generation_id": command.worker_process_generation_id,
                    }),
                )?,
            )
            .await?;
            claimed.push(ClaimedContextExecution {
                query: next_query,
                job,
                query_input,
                quota_account_ids,
                resume_mutations: slot.resume_mutations.clone(),
                failure_mutations: slot.failure_mutations.clone(),
            });
        }
        transaction.commit().await?;
        Ok(claimed)
    }
}

async fn lock_context_quota_accounts(
    transaction: &mut Transaction<'_, Postgres>,
    candidates: &[(
        JobRecord,
        ContextJobPayload,
        &insight_platform_context::ContextClaimSlot,
    )],
) -> Result<BTreeMap<String, ContextQuotaAccount>, RepositoryError> {
    let tenants = candidates
        .iter()
        .map(|(job, _, _)| job.tenant_id.clone())
        .collect::<Vec<_>>();
    let deployments = candidates
        .iter()
        .map(|(_, payload, _)| payload.binding.context_deployment.deployment_id.to_string())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT tenant_id, quota_account_id, scope_kind, scope_id, work_class, metric,
               limit_value, reserved_value, used_value, version
        FROM insight_platform.quota_accounts
        WHERE work_class = 'context' AND (
            (scope_kind = 'tenant' AND scope_id = ANY($1) AND metric = $3)
            OR
            (scope_kind = 'context_deployment' AND scope_id = ANY($2)
             AND metric = ANY($4))
        )
        ORDER BY tenant_id, quota_account_id
        FOR UPDATE
        "#,
    )
    .bind(tenants)
    .bind(deployments)
    .bind(QuotaDimension::WorkClassConcurrentOperations.as_str())
    .bind(vec![
        QuotaDimension::ContextQueries.as_str(),
        QuotaDimension::ContextResultBytes.as_str(),
    ])
    .fetch_all(&mut **transaction)
    .await?;
    let mut accounts = BTreeMap::new();
    for row in rows {
        let account = context_quota_account_from_row(&row)?;
        if accounts
            .insert(account.quota_account_id.clone(), account)
            .is_some()
        {
            return Err(RepositoryError::CorruptRow(
                "Context quota lock returned a duplicate".to_owned(),
            ));
        }
    }
    Ok(accounts)
}

async fn reserve_context_quota(
    transaction: &mut Transaction<'_, Postgres>,
    accounts: &mut BTreeMap<String, ContextQuotaAccount>,
    job: &JobRecord,
    deployment_id: &ResourceId,
    ceiling: insight_platform_context::ContextQuotaCeiling,
    slot: &insight_platform_context::ContextClaimSlot,
    next: &JobProjection,
) -> Result<Vec<String>, RepositoryError> {
    let mut relevant = accounts
        .values()
        .filter(|account| context_quota_account_matches(account, job, deployment_id))
        .map(|account| account.quota_account_id.clone())
        .collect::<Vec<_>>();
    relevant.sort();
    if relevant.len() != CONTEXT_QUOTA_LINES {
        return Err(RepositoryError::QuotaExceeded);
    }
    let request = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "job_id": job.job_id,
            "lease_generation": next.lease_generation,
            "quota_account_ids": relevant,
            "quota_reservation_id": slot.quota_reservation_id,
        }),
        65_536,
    )?;
    for (account_id, entry_id) in relevant.iter().zip(&slot.quota_entry_ids) {
        let account = accounts.get_mut(account_id).ok_or_else(|| {
            RepositoryError::CorruptRow("Context quota account disappeared".to_owned())
        })?;
        let amount = context_reserved_amount(account, ceiling)?;
        if account
            .reserved_value
            .checked_add(account.used_value)
            .and_then(|value| value.checked_add(amount))
            .is_none_or(|value| value > account.limit_value)
        {
            return Err(RepositoryError::QuotaExceeded);
        }
        let version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.quota_accounts
            SET reserved_value = reserved_value + $4, version = version + 1,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
              AND reserved_value + used_value + $4 <= limit_value
            RETURNING version
            "#,
        )
        .bind(&account.tenant_id)
        .bind(&account.quota_account_id)
        .bind(account.version)
        .bind(amount)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::QuotaExceeded)?;
        account.version = version;
        account.reserved_value += amount;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_ledger (
                tenant_id, quota_entry_id, quota_account_id, correlation_id,
                entry_kind, reserved_amount, used_amount, account_version, request_digest
            ) VALUES ($1, $2, $3, $4, 'reserve', $5, 0, $6, $7)
            "#,
        )
        .bind(&account.tenant_id)
        .bind(entry_id.to_string())
        .bind(&account.quota_account_id)
        .bind(slot.quota_reservation_id.to_string())
        .bind(amount)
        .bind(version)
        .bind(&request.digest)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(relevant)
}

fn context_quota_account_matches(
    account: &ContextQuotaAccount,
    job: &JobRecord,
    deployment_id: &ResourceId,
) -> bool {
    account.tenant_id == job.tenant_id
        && account.work_class == WorkClass::Context.as_str()
        && ((account.scope_kind == "tenant"
            && account.scope_id == job.tenant_id
            && account.metric == QuotaDimension::WorkClassConcurrentOperations.as_str())
            || (account.scope_kind == "context_deployment"
                && account.scope_id == deployment_id.to_string()
                && matches!(
                    account.metric.as_str(),
                    value if value == QuotaDimension::ContextQueries.as_str()
                        || value == QuotaDimension::ContextResultBytes.as_str()
                )))
}

fn context_reserved_amount(
    account: &ContextQuotaAccount,
    ceiling: insight_platform_context::ContextQuotaCeiling,
) -> Result<i64, RepositoryError> {
    let amount = if account.metric == QuotaDimension::WorkClassConcurrentOperations.as_str() {
        ceiling.concurrent_units
    } else if account.metric == QuotaDimension::ContextQueries.as_str() {
        ceiling.queries
    } else if account.metric == QuotaDimension::ContextResultBytes.as_str() {
        ceiling.result_bytes
    } else {
        return Err(RepositoryError::CorruptRow(
            "Context quota metric is not registered".to_owned(),
        ));
    };
    i64::try_from(amount)
        .map_err(|_| RepositoryError::InvalidInput("Context quota exceeds bigint".to_owned()))
}

fn context_quota_account_from_row(row: &PgRow) -> Result<ContextQuotaAccount, RepositoryError> {
    Ok(ContextQuotaAccount {
        tenant_id: row.try_get("tenant_id")?,
        quota_account_id: row.try_get("quota_account_id")?,
        scope_kind: row.try_get("scope_kind")?,
        scope_id: row.try_get("scope_id")?,
        work_class: row.try_get("work_class")?,
        metric: row.try_get("metric")?,
        limit_value: row.try_get("limit_value")?,
        reserved_value: row.try_get("reserved_value")?,
        used_value: row.try_get("used_value")?,
        version: row.try_get("version")?,
    })
}

fn require_context_create_replay(
    record: &ContextQueryRecord,
    command: &CreateContextQuery,
) -> Result<(), RepositoryError> {
    if record.run_id != command.run_id
        || record.node_execution_id != command.node_execution_id
        || record.payload.admission.slot_id != command.slot_id
        || record.payload.admission.request != command.request
        || record.payload.admission.attempt_limit != command.requested_attempt_limit
        || record.payload.admission.quota_ceiling.result_bytes
            != command.result_byte_ceiling.min(u64::from(
                record
                    .payload
                    .admission
                    .interface
                    .limits
                    .maximum_total_bytes,
            ))
    {
        return Err(RepositoryError::Conflict("ContextQuery create replay"));
    }
    Ok(())
}

async fn load_context_node_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    node_id: &ResourceId,
) -> Result<LockedContextNode, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT run_id, record_kind, node_kind, state, version, deadline
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(node_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Context NodeExecution"))?;
    if row.try_get::<String, _>("record_kind")? != "node_execution" {
        return Err(RepositoryError::InvalidInput(
            "Context owner is not a NodeExecution".to_owned(),
        ));
    }
    Ok(LockedContextNode {
        run_id: row.try_get("run_id")?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<NodeExecutionState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_u64(row.try_get::<i64, _>("version")?, "Context Node version")?,
        node_kind: row
            .try_get::<String, _>("node_kind")?
            .parse::<PlanNodeKind>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        deadline: row.try_get("deadline")?,
    })
}

async fn insert_context_input_artifact_reference(
    transaction: &mut Transaction<'_, Postgres>,
    record: &ContextQueryRecord,
    request: &insight_platform_context::ContextQueryRequest,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    if let insight_platform_invocations::InvocationValueStorage::Artifact { artifact } =
        &request.input.storage
    {
        let link_id = request.input_artifact_link_id.as_ref().ok_or_else(|| {
            RepositoryError::InvalidInput(
                "Artifact-backed Context query input has no link ID".to_owned(),
            )
        })?;
        insert_context_artifact_reference(
            transaction,
            record,
            link_id,
            artifact,
            ArtifactReferenceKind::Input,
            ArtifactPurpose::RunInput,
            database_now,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_context_artifact_reference(
    transaction: &mut Transaction<'_, Postgres>,
    record: &ContextQueryRecord,
    link_id: &ResourceId,
    artifact: &insight_platform_contracts::ArtifactRef,
    reference_kind: ArtifactReferenceKind,
    purpose: ArtifactPurpose,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let snapshot = ArtifactReferenceSnapshot {
        schema_version: 1,
        artifact_id: artifact.artifact_id().clone(),
        owner_id: record.context_query_id.clone(),
        reference_kind,
        purpose,
        created_by: record.payload.admission.principal.principal_id.clone(),
    };
    let payload = TypedPayload::from_versioned(1, &snapshot, 262_144)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_links (
            tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
            target_artifact_id, link_key_digest, state, payload_schema_version,
            payload, payload_digest, created_at, updated_at
        ) VALUES ($1, $2, 'reference', 'context_query', $3, $4, $5,
                  'active', $6, $7, $8, $9, $9)
        "#,
    )
    .bind(record.tenant_id.to_string())
    .bind(link_id.to_string())
    .bind(record.context_query_id.to_string())
    .bind(artifact.artifact_id().to_string())
    .bind(
        snapshot
            .link_key_digest()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
            .to_string(),
    )
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn require_context_claim_parents(
    transaction: &mut Transaction<'_, Postgres>,
    query: &ContextQueryRecord,
    run: &RunRecord,
    node: &LockedContextNode,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let deployment = load_exact_context_deployment(
        transaction,
        &query.tenant_id,
        &query.payload.admission.binding.context_deployment,
    )
    .await?;
    let resource_id = parse_id(&deployment.resource_id, "Context resource")?;
    let resource = load_resource(transaction, &query.tenant_id, &resource_id).await?;
    load_enabled_exact_published_version(
        transaction,
        &query.tenant_id,
        &query.payload.admission.interface_revision,
        RegistryResourceKind::ContextSourceInterface,
    )
    .await?;
    load_enabled_exact_published_version(
        transaction,
        &query.tenant_id,
        &query.payload.admission.implementation_revision,
        RegistryResourceKind::ContextSourceImplementation,
    )
    .await?;
    if let ContextDatasetView::Generation { exact } = &query.payload.admission.dataset_view {
        load_exact_dataset_generation(transaction, &query.tenant_id, exact).await?;
    }
    let plan_leaf = node.state == NodeExecutionState::Waiting;
    let run_state = run.state.parse::<RunState>().ok();
    if query.run_id.to_string() != run.run_id
        || node.run_id != run.run_id
        || if plan_leaf {
            !matches!(run_state, Some(RunState::Running | RunState::Waiting))
        } else {
            run_state != Some(RunState::Running)
        }
        || run.current.control.pause_requested
        || run.current.control.cancel_requested_at.is_some()
        || run.current.control.timeout_requested_at.is_some()
        || if plan_leaf {
            node.state != NodeExecutionState::Waiting
        } else {
            node.state != NodeExecutionState::Running
        }
        || node.node_kind != PlanNodeKind::ContextQuery
        || database_now >= run.deadline
        || database_now >= node.deadline
        || run.bindings.canonical_digest != query.payload.admission.run_bindings_digest
        || query.deployment_id.to_string() != deployment.deployment_id
        || resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || resource.gate_state != "enabled"
    {
        return Err(RepositoryError::Conflict("Context dispatch parent"));
    }
    if plan_leaf {
        let mut current = run.current.clone();
        if run_state == Some(RunState::Waiting) {
            current.waiting_reason = None;
        }
        current
            .validate(&query.run_id)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let payload = TypedPayload::from_versioned(1, &current, 1_048_576)?;
        let updated = sqlx::query(
            r#"
            UPDATE insight_platform.runs
            SET state = 'running', version = version + 1,
                active_work_count = active_work_count + 1,
                current_schema_version = $4, current_payload = $5,
                current_payload_digest = $6, updated_at = $7
            WHERE tenant_id = $1 AND run_id = $2 AND version = $3
              AND state IN ('running', 'waiting') AND terminal_at IS NULL
            "#,
        )
        .bind(&run.tenant_id)
        .bind(&run.run_id)
        .bind(run.version)
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .bind(database_now)
        .execute(&mut **transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(RepositoryError::Conflict("Context Plan leaf claim permit"));
        }
    }
    Ok(())
}

fn require_same_context_job(
    observed: &JobRecord,
    current: &JobRecord,
) -> Result<(), RepositoryError> {
    if current.version != observed.version
        || current.state != observed.state
        || current.lease_epoch != observed.lease_epoch
        || current.worker_id != observed.worker_id
        || current.lease_token_digest != observed.lease_token_digest
        || current.quota_reservation_id != observed.quota_reservation_id
        || current.payload.digest != observed.payload.digest
    {
        return Err(RepositoryError::Conflict("Context Job"));
    }
    Ok(())
}

async fn lock_context_quota_bundle(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
) -> Result<Vec<ContextQuotaLine>, RepositoryError> {
    let reservation_id = job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("active Context Job has no quota reservation".to_owned())
    })?;
    let payload: ContextJobPayload = decode_versioned_payload(&job.payload, "Context Job")?;
    let rows = sqlx::query(
        r#"
        SELECT account.tenant_id, account.quota_account_id, account.scope_kind,
               account.scope_id, account.work_class, account.metric, account.limit_value,
               account.reserved_value, account.used_value, account.version,
               reserve.reserved_amount, reserve.used_amount AS reservation_used_amount
        FROM insight_platform.quota_ledger AS reserve
        JOIN insight_platform.quota_accounts AS account
          ON account.tenant_id = reserve.tenant_id
         AND account.quota_account_id = reserve.quota_account_id
        WHERE reserve.tenant_id = $1 AND reserve.correlation_id = $2
          AND reserve.entry_kind = 'reserve'
        ORDER BY account.tenant_id, account.quota_account_id
        FOR UPDATE OF account
        "#,
    )
    .bind(&job.tenant_id)
    .bind(reservation_id)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != CONTEXT_QUOTA_LINES {
        return Err(RepositoryError::CorruptRow(
            "Context quota bundle does not contain exactly three lines".to_owned(),
        ));
    }
    let already_settled: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM insight_platform.quota_ledger
            WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'
        )
        "#,
    )
    .bind(&job.tenant_id)
    .bind(reservation_id)
    .fetch_one(&mut **transaction)
    .await?;
    if already_settled {
        return Err(RepositoryError::Conflict("Context quota settlement"));
    }
    let deployment_id = &payload.binding.context_deployment.deployment_id;
    let ceiling = payload.binding.quota_ceiling;
    let mut metrics = BTreeSet::new();
    let mut lines = Vec::with_capacity(rows.len());
    for row in rows {
        let account = context_quota_account_from_row(&row)?;
        if !context_quota_account_matches(&account, job, deployment_id)
            || !metrics.insert(account.metric.clone())
        {
            return Err(RepositoryError::CorruptRow(
                "Context quota bundle has an unexpected or duplicate line".to_owned(),
            ));
        }
        let reserved_amount: i64 = row.try_get("reserved_amount")?;
        if reserved_amount != context_reserved_amount(&account, ceiling)?
            || row.try_get::<i64, _>("reservation_used_amount")? != 0
            || account.reserved_value < reserved_amount
        {
            return Err(RepositoryError::CorruptRow(
                "Context quota reservation amount is invalid".to_owned(),
            ));
        }
        lines.push(ContextQuotaLine {
            account,
            reserved_amount,
        });
    }
    let expected = BTreeSet::from([
        QuotaDimension::WorkClassConcurrentOperations
            .as_str()
            .to_owned(),
        QuotaDimension::ContextQueries.as_str().to_owned(),
        QuotaDimension::ContextResultBytes.as_str().to_owned(),
    ]);
    if metrics != expected {
        return Err(RepositoryError::CorruptRow(
            "Context quota bundle is incomplete".to_owned(),
        ));
    }
    Ok(lines)
}

async fn settle_context_quota(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
    lines: &[ContextQuotaLine],
    settlement_entry_ids: &[ResourceId],
    consumed_query: bool,
    result_bytes: u64,
    request_digest: &Sha256Digest,
) -> Result<(), RepositoryError> {
    if lines.len() != CONTEXT_QUOTA_LINES || settlement_entry_ids.len() != CONTEXT_QUOTA_LINES {
        return Err(RepositoryError::InvalidInput(
            "Context quota settlement line count is invalid".to_owned(),
        ));
    }
    let reservation_id = job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("active Context Job has no quota reservation".to_owned())
    })?;
    for (line, entry_id) in lines.iter().zip(settlement_entry_ids) {
        let used_amount = context_settlement_amount(&line.account, consumed_query, result_bytes)?;
        if used_amount < 0 || used_amount > line.reserved_amount {
            return Err(RepositoryError::QuotaExceeded);
        }
        let version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.quota_accounts
            SET reserved_value = reserved_value - $4,
                used_value = used_value + $5,
                version = version + 1, updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
              AND reserved_value >= $4
              AND reserved_value + used_value - $4 + $5 <= limit_value
            RETURNING version
            "#,
        )
        .bind(&line.account.tenant_id)
        .bind(&line.account.quota_account_id)
        .bind(line.account.version)
        .bind(line.reserved_amount)
        .bind(used_amount)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("Context quota account"))?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_ledger (
                tenant_id, quota_entry_id, quota_account_id, correlation_id,
                entry_kind, reserved_amount, used_amount, account_version, request_digest
            ) VALUES ($1, $2, $3, $4, 'settle', $5, $6, $7, $8)
            "#,
        )
        .bind(&line.account.tenant_id)
        .bind(entry_id.to_string())
        .bind(&line.account.quota_account_id)
        .bind(reservation_id)
        .bind(line.reserved_amount)
        .bind(used_amount)
        .bind(version)
        .bind(request_digest.to_string())
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

fn context_settlement_amount(
    account: &ContextQuotaAccount,
    consumed_query: bool,
    result_bytes: u64,
) -> Result<i64, RepositoryError> {
    let amount = if account.metric == QuotaDimension::WorkClassConcurrentOperations.as_str() {
        0
    } else if account.metric == QuotaDimension::ContextQueries.as_str() {
        u64::from(consumed_query)
    } else if account.metric == QuotaDimension::ContextResultBytes.as_str() {
        result_bytes
    } else {
        return Err(RepositoryError::CorruptRow(
            "Context quota settlement metric is not registered".to_owned(),
        ));
    };
    i64::try_from(amount)
        .map_err(|_| RepositoryError::InvalidInput("Context quota usage exceeds bigint".to_owned()))
}

async fn update_context_job(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobRecord,
    next: &JobProjection,
    next_payload: &ContextJobPayload,
    quota_reservation_id: Option<&ResourceId>,
    database_now: DateTime<Utc>,
) -> Result<JobRecord, RepositoryError> {
    let payload = TypedPayload::from_versioned(1, next_payload, 1_048_576)?;
    let (worker_id, lease_token_digest, lease_expires_at, heartbeat_at) = next
        .lease
        .as_ref()
        .map(|lease| {
            (
                Some(lease.worker_process_generation_id.to_string()),
                Some(lease.token_digest.to_string()),
                Some(lease.expires_at),
                Some(lease.heartbeat_at),
            )
        })
        .unwrap_or((None, None, None, None));
    if quota_reservation_id.is_some() != next.lease.is_some() {
        return Err(RepositoryError::CorruptRow(
            "Context Job lease and quota reservation shape disagree".to_owned(),
        ));
    }
    let terminal = matches!(
        next.state,
        JobState::Succeeded | JobState::Failed | JobState::Cancelled | JobState::TimedOut
    );
    let (wake_kind, wake_state, wake_generation) = match &next.wake {
        Some(wake) => (
            Some(wake.kind.as_str()),
            Some("pending"),
            as_i64(wake.generation, "Context Job wake generation")?,
        ),
        None => (None, None, 0),
    };
    let started_at = current
        .started_at
        .or((next.state == JobState::Running).then_some(database_now));
    let row = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $4, version = $5, attempt_no = $6, lease_epoch = $7,
            worker_id = $8, lease_token_digest = $9, lease_expires_at = $10,
            heartbeat_at = $11, scheduled_at = $12, retry_at = $13,
            wake_kind = $14, wake_state = $15, wake_generation = $16,
            result_digest = $17, quota_reservation_id = $18,
            payload_schema_version = $19, payload = $20, payload_digest = $21,
            started_at = $22, terminal_at = $23, updated_at = $24
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
        RETURNING *
        "#,
    )
    .bind(&current.tenant_id)
    .bind(&current.job_id)
    .bind(current.version)
    .bind(next.state.as_str())
    .bind(as_i64(next.version, "Context Job version")?)
    .bind(i32::try_from(next.attempt_count).map_err(|_| {
        RepositoryError::InvalidInput("Context Job attempt count exceeds integer".to_owned())
    })?)
    .bind(as_i64(
        next.lease_generation,
        "Context Job lease generation",
    )?)
    .bind(worker_id)
    .bind(lease_token_digest)
    .bind(lease_expires_at)
    .bind(heartbeat_at)
    .bind(next.scheduled_at)
    .bind(next.retry_at)
    .bind(wake_kind)
    .bind(wake_state)
    .bind(wake_generation)
    .bind(terminal.then_some(payload.digest.clone()))
    .bind(quota_reservation_id.map(ToString::to_string))
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(started_at)
    .bind(terminal.then_some(database_now))
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Context Job"))?;
    job_from_row(row)
}

async fn insert_context_output(
    transaction: &mut Transaction<'_, Postgres>,
    query: &ContextQueryRecord,
    output: &ContextObservationOutput,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    if let ValueRef::Artifact { artifact } = &output.value {
        require_ready_run_artifact(transaction, &query.tenant_id, artifact).await?;
    }
    let (inline_value, artifact_id) = match &output.value {
        ValueRef::Inline { value } => (Some(value), None),
        ValueRef::Artifact { artifact } => (None, Some(artifact.artifact_id().to_string())),
    };
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, node_id, value_kind, classification,
            schema_digest, content_digest, inline_value, artifact_id, created_at
        ) VALUES ($1, $2, $3, $4, 'context_observation', $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(query.tenant_id.to_string())
    .bind(output.value_id.to_string())
    .bind(query.run_id.to_string())
    .bind(query.node_execution_id.to_string())
    .bind(output.classification.as_str())
    .bind(
        query
            .payload
            .admission
            .interface
            .observation_schema_digest
            .to_string(),
    )
    .bind(output.observation.canonical_digest.to_string())
    .bind(inline_value)
    .bind(artifact_id)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;
    if let ValueRef::Artifact { artifact } = &output.value {
        let link_id = output.artifact_link_id.as_ref().ok_or_else(|| {
            RepositoryError::InvalidInput(
                "Artifact-backed Context observation has no link ID".to_owned(),
            )
        })?;
        insert_context_artifact_reference(
            transaction,
            query,
            link_id,
            artifact,
            ArtifactReferenceKind::Output,
            ArtifactPurpose::ContextDerived,
            database_now,
        )
        .await?;
    }
    Ok(())
}

fn require_context_outcome_candidate(
    job: &JobRecord,
    fence: &insight_platform_jobs::JobFence,
) -> Result<(), RepositoryError> {
    let worker_id = fence.worker_process_generation_id.to_string();
    let token_digest = fence.token_digest.to_string();
    if job.state != JobState::Running.as_str()
        || job.quota_reservation_id.is_none()
        || job.version != as_i64(fence.expected_version, "Context Job fence version")?
        || job.lease_epoch != as_i64(fence.lease_generation, "Context Job fence lease generation")?
        || job.worker_id.as_deref() != Some(worker_id.as_str())
        || job.lease_token_digest.as_deref() != Some(token_digest.as_str())
    {
        return Err(RepositoryError::Conflict("Context outcome fence"));
    }
    Ok(())
}

fn context_worker_receipt_payload(
    command: &CommitContextOutcome,
) -> Result<TypedPayload, RepositoryError> {
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "context_query_id": command.context_query_id,
            "expected_query_version": command.expected_query_version,
            "fence": {
                "expected_version": command.fence.expected_version,
                "lease_generation": command.fence.lease_generation,
                "lease_token_digest": command.fence.token_digest,
                "worker_process_generation_id": command.fence.worker_process_generation_id,
            },
            "job_id": command.job_id,
            "outcome_kind": command.outcome.kind(),
            "quota_entry_ids": command.quota_entry_ids,
        }),
        65_536,
    )
}

async fn claim_context_worker_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &ContextWorkerAudit,
    job_id: &ResourceId,
    operation: &str,
    payload: &TypedPayload,
) -> Result<bool, RepositoryError> {
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest, state,
            payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'job_commit', 'job', $3, $4, $5, $6, $7,
                  'processing', $8, $9, $10, $11)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(job_id.to_string())
    .bind(audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .bind(audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(false);
    }
    let row = sqlx::query(
        r#"
        SELECT request_digest, state, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'job_commit'
          AND scope_kind = 'job' AND scope_id = $2 AND dedupe_owner_id = $3
          AND operation = $4 AND idempotency_key_digest = $5
        FOR UPDATE
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(job_id.to_string())
    .bind(audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if row.try_get::<String, _>("request_digest")? != audit.request_digest.to_string()
        || row.try_get::<String, _>("payload_digest")? != payload.digest
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("Context worker receipt"));
    }
    Ok(true)
}

async fn terminalize_context_worker_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &ContextWorkerAudit,
    job_id: &ResourceId,
    disposition: &str,
    response_reference_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4, response_reference_id = $5,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND scope_id = $6 AND state = 'processing'
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(audit.request_digest.to_string())
    .bind(disposition)
    .bind(response_reference_id.to_string())
    .bind(job_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("Context worker receipt"));
    }
    Ok(())
}

fn context_signal_receipt_payload(
    command: &WakeContextDispatch,
) -> Result<TypedPayload, RepositoryError> {
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "context_query_id": command.context_query_id,
            "expected_query_version": command.expected_query_version,
            "expected_wake_generation": command.expected_wake_generation,
            "job_id": command.job_id,
            "source": command.source,
        }),
        65_536,
    )
}

async fn claim_context_signal_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &ContextSignalAudit,
    job_id: &ResourceId,
    operation: &str,
    payload: &TypedPayload,
) -> Result<bool, RepositoryError> {
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest, state,
            payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'callback', 'job', $3, $3, $4, $5, $6,
                  'processing', $7, $8, $9, $10)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(job_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .bind(audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(false);
    }
    let row = sqlx::query(
        r#"
        SELECT request_digest, state, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'callback'
          AND scope_kind = 'job' AND scope_id = $2 AND dedupe_owner_id = $2
          AND operation = $3 AND idempotency_key_digest = $4
        FOR UPDATE
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(job_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if row.try_get::<String, _>("request_digest")? != audit.request_digest.to_string()
        || row.try_get::<String, _>("payload_digest")? != payload.digest
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("Context signal receipt"));
    }
    Ok(true)
}

async fn terminalize_context_signal_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &ContextSignalAudit,
    job_id: &ResourceId,
    disposition: &str,
    response_reference_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4, response_reference_id = $5,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND scope_id = $6 AND state = 'processing'
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(audit.request_digest.to_string())
    .bind(disposition)
    .bind(response_reference_id.to_string())
    .bind(job_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("Context signal receipt"));
    }
    Ok(())
}

async fn database_now(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<DateTime<Utc>, RepositoryError> {
    Ok(sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut **transaction)
        .await?)
}

fn parse_run_state(value: &str) -> Result<RunState, RepositoryError> {
    value
        .parse()
        .map_err(|failure| RepositoryError::CorruptRow(format!("Run state: {failure}")))
}

fn parse_id(value: &str, kind: &str) -> Result<ResourceId, RepositoryError> {
    value
        .parse()
        .map_err(|failure| RepositoryError::CorruptRow(format!("{kind}: {failure}")))
}

fn parse_required_id(
    value: Option<&str>,
    expected: ResourceKind,
    kind: &str,
) -> Result<ResourceId, RepositoryError> {
    let value = value.ok_or_else(|| RepositoryError::CorruptRow(format!("{kind} is missing")))?;
    let parsed = parse_id(value, kind)?;
    if parsed.kind() != expected {
        return Err(RepositoryError::CorruptRow(format!(
            "{kind} has the wrong ID kind"
        )));
    }
    Ok(parsed)
}

fn parse_u64(value: i64, kind: &str) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| RepositoryError::CorruptRow(format!("negative {kind}")))
}

fn as_i64(value: u64, kind: &str) -> Result<i64, RepositoryError> {
    i64::try_from(value)
        .map_err(|_| RepositoryError::InvalidInput(format!("{kind} exceeds bigint")))
}
