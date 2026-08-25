use crate::{
    invocation_repository::{
        load_capability_continuation_input, load_capability_execution_contract,
        load_capability_execution_input, load_capability_invocation,
        load_capability_node_for_update, load_exact_capability_interface_spec,
        validate_capability_value_against_schema, PgInvocationTransaction,
    },
    repository::{
        append_command_event, append_scheduler_event, claim_command_receipt,
        decode_versioned_payload, job_from_row, job_projection, load_deployment, load_resource,
        load_run_for_update, load_task_for_update, require_ready_run_artifact,
        require_tenant_permission, task_projection, terminalize_command_receipt, JobRecord,
        PgRepository, RepositoryError, TaskRecord, TypedPayload,
    },
};
use chrono::{DateTime, Utc};
use insight_platform_artifacts::ArtifactReferenceSnapshot;
use insight_platform_capability_adapters::{
    CancelCapabilityAdapterJob, CapabilityAdapterContinuation, CapabilityAdapterRequest,
    CapabilityExecutionAuthority, ExecuteCapabilityAdapterJob,
};
use insight_platform_contracts::{
    ArtifactPurpose, ArtifactReferenceKind, CapabilityBackendKind, CapabilityProgressDurability,
    CommandOutcome, EntityLifecycle, InvocationState, JobState, Permission, QuotaDimension,
    ResourceId, ResourceKind, RunState, Sha256Digest, ValueRef, WorkClass,
    MAX_CAPABILITY_TIMEOUT_MILLISECONDS,
};
use insight_platform_invocations::{
    decide_cancellation_outcome, decide_capability_wake, decide_control,
    decide_detached_input_response, decide_detached_job_control, decide_dispatch_outcome,
    decide_input_response, decide_prepare_dispatch, decide_progress,
    decide_reconciliation_resolution, decide_start_dispatch, CapabilityCancellationObservation,
    CapabilityControlKind, CapabilityDetachedPending, CapabilityJobPayload, CapabilitySignalAudit,
    CapabilityWakeDisposition, CapabilityWorkerAudit, ClaimCapabilityJobs,
    CommitCapabilityCancellationOutcome, CommitCapabilityOutcome, ControlCapabilityInvocation,
    DetachedSandboxSourceKind, DispatchOutcome, PrepareCapabilityDispatch,
    ReconciliationResolution, RecordCapabilityProgress, ResolveCapabilityInput,
    ResolveCapabilityReconciliation, WakeCapabilityInvocation, CAPABILITY_QUOTA_LINES,
};
use insight_platform_jobs::{JobFence, LeasePolicy};
use insight_platform_sandbox::{
    SandboxCommandLimits, SandboxExecutionJobPayload, SandboxExecutionSource, SandboxJobPayload,
};
use insight_platform_tasks::{
    decide_resolution as decide_task_resolution, ResolveTask, TaskDefinition, TaskPayload,
    TaskState,
};
use sqlx::{Acquire, Postgres, Row, Transaction};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedCapabilityExecution {
    pub invocation: insight_platform_invocations::CapabilityInvocationRecord,
    pub job: JobRecord,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedCapabilityExecution {
    pub invocation: insight_platform_invocations::CapabilityInvocationRecord,
    pub job: JobRecord,
    pub execution_contract: insight_platform_invocations::CapabilityExecutionContract,
    pub input: insight_platform_invocations::CapabilityExecutionInput,
    pub continuation_input: Option<insight_platform_invocations::CapabilityExecutionInput>,
    pub fence: JobFence,
    /// Ledger identities consumed by the claim transaction to record the quota reservation.
    /// Terminal settlement must use a distinct caller-generated pair.
    pub quota_reservation_entry_ids: Vec<ResourceId>,
    pub resume_mutations: Option<insight_platform_contracts::ExternalLeafResumeMutationIds>,
}

impl ClaimedCapabilityExecution {
    /// Builds the credential-free exact request consumed by a process-installed Capability
    /// adapter. The claim transaction has already loaded and validated the immutable Deployment
    /// closure and input; this method rechecks their binding to the Running Job fence before any
    /// external dispatch.
    pub fn adapter_execution(
        &self,
        worker_manifest_digest: Sha256Digest,
    ) -> Result<CapabilityAdapterRequest, RepositoryError> {
        self.invocation
            .validate()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        self.execution_contract
            .validate_for(&self.invocation.payload.admission)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        self.input
            .validate()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        if self
            .continuation_input
            .as_ref()
            .is_some_and(|input| input.validate().is_err())
        {
            return Err(RepositoryError::CorruptRow(
                "Capability continuation input is invalid".to_owned(),
            ));
        }
        let projection = job_projection(&self.job)?;
        let payload: CapabilityJobPayload =
            decode_versioned_payload(&self.job.payload, "Capability Job")?;
        payload
            .validate_for(&self.invocation, &projection)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        if projection.state != JobState::Running
            || self.invocation.payload.current_job_id.as_ref() != Some(&projection.job_id)
            || projection.version != self.fence.expected_version
            || projection.lease_generation != self.fence.lease_generation
            || projection.attempt_count == 0
            || projection.lease.as_ref().is_none_or(|lease| {
                lease.worker_process_generation_id != self.fence.worker_process_generation_id
                    || lease.lease_generation != self.fence.lease_generation
                    || lease.token_digest != self.fence.token_digest
            })
        {
            return Err(RepositoryError::Conflict(
                "claimed Capability execution fence",
            ));
        }
        let admission = &self.invocation.payload.admission;
        let execution = CapabilityAdapterRequest {
            tenant_id: self.invocation.tenant_id.clone(),
            invocation_id: self.invocation.invocation_id.clone(),
            job_id: projection.job_id,
            worker_process_generation_id: self.fence.worker_process_generation_id.clone(),
            worker_manifest_digest,
            lease_generation: projection.lease_generation,
            physical_attempt: projection.attempt_count,
            attempt_limit: projection.attempt_limit,
            admission_digest: admission.canonical_digest.clone(),
            idempotency_key_digest: admission.idempotency_key_digest.clone(),
            effect: admission.effect,
            idempotency: admission.idempotency,
            deadline: self.invocation.deadline,
            execution: self.execution_contract.clone(),
            input: self.input.clone(),
            continuation: payload.encrypted_remote_state.clone().map(|state| {
                CapabilityAdapterContinuation {
                    encrypted_remote_state: state,
                    external_identity_digest: payload.external_identity_digest.clone(),
                    resume_input: self.continuation_input.clone(),
                    resume_input_action: payload.resume_input_action,
                    poll_count: payload.poll_count,
                }
            }),
            mcp_runtime: admission.mcp_runtime.clone(),
        };
        execution
            .validate_shape()
            .map_err(|_| RepositoryError::Conflict("Capability adapter execution contract"))?;
        Ok(execution)
    }

    pub fn adapter_job(
        &self,
        worker_manifest_digest: Sha256Digest,
        audit: CapabilityWorkerAudit,
        quota_settlement_entry_ids: Vec<ResourceId>,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<ExecuteCapabilityAdapterJob, RepositoryError> {
        let reservation_ids = self
            .quota_reservation_entry_ids
            .iter()
            .collect::<BTreeSet<_>>();
        if quota_settlement_entry_ids
            .iter()
            .any(|identity| reservation_ids.contains(identity))
        {
            return Err(RepositoryError::InvalidInput(
                "Capability quota settlement identities reuse reservation identities".to_owned(),
            ));
        }
        let command = ExecuteCapabilityAdapterJob {
            execution: self.adapter_execution(worker_manifest_digest)?,
            audit,
            expected_invocation_version: self.invocation.version,
            fence: self.fence.clone(),
            quota_entry_ids: quota_settlement_entry_ids,
            retry_at,
            resume_mutations: self.resume_mutations.clone(),
        };
        command
            .validate_at(Utc::now())
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        Ok(command)
    }
}

#[async_trait::async_trait]
impl CapabilityExecutionAuthority for PgRepository {
    type Error = RepositoryError;
    type Record = PreparedCapabilityExecution;

    async fn commit_capability_outcome(
        &self,
        command: CommitCapabilityOutcome,
    ) -> Result<CommandOutcome<Self::Record>, Self::Error> {
        let mut transaction = self.begin_invocation_transaction().await?;
        let result =
            PgInvocationTransaction::commit_capability_outcome(&mut transaction, command).await?;
        insight_platform_invocations::InvocationTransaction::commit(transaction).await?;
        Ok(result)
    }

    async fn commit_capability_cancellation_outcome(
        &self,
        command: CommitCapabilityCancellationOutcome,
    ) -> Result<CommandOutcome<Self::Record>, Self::Error> {
        let mut transaction = self.begin_invocation_transaction().await?;
        let result = PgInvocationTransaction::commit_capability_cancellation_outcome(
            &mut transaction,
            command,
        )
        .await?;
        insight_platform_invocations::InvocationTransaction::commit(transaction).await?;
        Ok(result)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ControlledCapabilityExecution {
    pub invocation: insight_platform_invocations::CapabilityInvocationRecord,
    pub job: Option<JobRecord>,
}

impl ControlledCapabilityExecution {
    /// Rebinds a durable `Cancelling` winner to the still-live physical execution that was
    /// returned by the original claim. Only the optimistic Job version is rotated: worker,
    /// lease generation, lease token, physical attempt and immutable execution closure must all
    /// remain exact, so a replacement generation is rejected before adapter I/O.
    pub fn cancel_adapter_job(
        &self,
        claimed: &ClaimedCapabilityExecution,
        worker_manifest_digest: Sha256Digest,
        audit: CapabilityWorkerAudit,
        quota_settlement_entry_ids: Vec<ResourceId>,
        cancel_deadline: DateTime<Utc>,
    ) -> Result<CancelCapabilityAdapterJob, RepositoryError> {
        self.invocation
            .validate()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let current_job = self
            .job
            .as_ref()
            .ok_or(RepositoryError::Conflict("Capability cancellation Job"))?;
        let current = job_projection(current_job)?;
        let payload: CapabilityJobPayload =
            decode_versioned_payload(&current_job.payload, "Capability Job")?;
        payload
            .validate_for(&self.invocation, &current)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let execution = claimed.adapter_execution(worker_manifest_digest)?;
        let lease = current
            .lease
            .as_ref()
            .ok_or(RepositoryError::Conflict("Capability cancellation lease"))?;
        if self.invocation.state != InvocationState::Cancelling
            || current.state != JobState::Cancelling
            || self.invocation.tenant_id != claimed.invocation.tenant_id
            || self.invocation.invocation_id != claimed.invocation.invocation_id
            || self.invocation.run_id != claimed.invocation.run_id
            || self.invocation.node_execution_id != claimed.invocation.node_execution_id
            || self.invocation.payload.admission != claimed.invocation.payload.admission
            || self.invocation.payload.current_job_id.as_ref() != Some(&current.job_id)
            || current.job_id != execution.job_id
            || current.version <= claimed.fence.expected_version
            || current.attempt_count != execution.physical_attempt
            || current.attempt_limit != execution.attempt_limit
            || current.lease_generation != execution.lease_generation
            || lease.worker_process_generation_id != claimed.fence.worker_process_generation_id
            || lease.lease_generation != claimed.fence.lease_generation
            || lease.token_digest != claimed.fence.token_digest
        {
            return Err(RepositoryError::Conflict(
                "Capability cancellation physical execution",
            ));
        }
        reject_quota_identity_reuse(
            &claimed.quota_reservation_entry_ids,
            &quota_settlement_entry_ids,
        )?;
        let command = CancelCapabilityAdapterJob {
            execution,
            audit,
            expected_invocation_version: self.invocation.version,
            fence: JobFence {
                expected_version: current.version,
                worker_process_generation_id: lease.worker_process_generation_id.clone(),
                lease_generation: lease.lease_generation,
                token_digest: lease.token_digest.clone(),
            },
            quota_entry_ids: quota_settlement_entry_ids,
            cancel_deadline,
        };
        command
            .validate_at(Utc::now())
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        Ok(command)
    }
}

fn reject_quota_identity_reuse(
    reservation_entry_ids: &[ResourceId],
    settlement_entry_ids: &[ResourceId],
) -> Result<(), RepositoryError> {
    let reservation_ids = reservation_entry_ids.iter().collect::<BTreeSet<_>>();
    if settlement_entry_ids
        .iter()
        .any(|identity| reservation_ids.contains(identity))
    {
        return Err(RepositoryError::InvalidInput(
            "Capability quota settlement identities reuse reservation identities".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct CapabilityClaimCandidate<'a> {
    job: JobRecord,
    deployment_id: ResourceId,
    slot: &'a insight_platform_invocations::CapabilityClaimSlot,
}

#[derive(Debug, Clone)]
struct CapabilityQuotaAccount {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CapabilityOwnerJobKind {
    Capability,
    DetachedSandbox(DetachedSandboxSourceKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CapabilityOwnerJobLock {
    None,
    Share,
    Update,
}

fn classify_capability_owner_job(
    invocation: &insight_platform_invocations::CapabilityInvocationRecord,
    job: &JobRecord,
    sandbox_limits: SandboxCommandLimits,
) -> Result<CapabilityOwnerJobKind, RepositoryError> {
    let invocation_id = invocation.invocation_id.to_string();
    if job.tenant_id != invocation.tenant_id.to_string()
        || job.invocation_id.as_deref() != Some(invocation_id.as_str())
    {
        return Err(RepositoryError::CorruptRow(
            "Capability owner Job points at a different Invocation".to_owned(),
        ));
    }
    match job
        .work_class
        .parse::<WorkClass>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
    {
        WorkClass::CapabilityNative | WorkClass::CapabilityRemote => {
            if job.owner_kind != ResourceKind::CapabilityInvocation.descriptor().name
                || job.owner_id != invocation_id
            {
                return Err(RepositoryError::CorruptRow(
                    "Capability Job owner binding is invalid".to_owned(),
                ));
            }
            let projection = job_projection(job)?;
            let payload: CapabilityJobPayload =
                decode_versioned_payload(&job.payload, "Capability Job")?;
            payload
                .validate_for(invocation, &projection)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            Ok(CapabilityOwnerJobKind::Capability)
        }
        WorkClass::Sandbox => {
            let projection = job_projection(job)?;
            let payload: SandboxExecutionJobPayload =
                decode_versioned_payload::<SandboxJobPayload>(&job.payload, "Sandbox Job")?
                    .into_capability_execution()
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            payload
                .validate_for(&projection, sandbox_limits)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            if job.owner_kind != ResourceKind::Job.descriptor().name
                || job.owner_id != payload.request.sandbox_job_id.to_string()
                || payload.request.invocation_id != invocation.invocation_id
                || payload.request.job_id != projection.job_id
                || payload.request.capability_deployment != invocation.payload.admission.deployment
                || payload.request.execution_source.capability_binding().kind()
                    != invocation.payload.admission.backend_kind
            {
                return Err(RepositoryError::CorruptRow(
                    "detached Sandbox Job owner binding is invalid".to_owned(),
                ));
            }
            let source_kind = match &payload.request.execution_source {
                SandboxExecutionSource::SandboxCapability { .. } => {
                    if invocation.payload.admission.backend_kind != CapabilityBackendKind::Sandbox
                        || invocation.payload.admission.mcp_runtime.is_some()
                    {
                        return Err(RepositoryError::CorruptRow(
                            "Sandbox Capability Job has the wrong logical backend".to_owned(),
                        ));
                    }
                    DetachedSandboxSourceKind::SandboxCapability
                }
                #[cfg(any())]
                SandboxExecutionSource::ManagedMcp {
                    mcp_contract,
                    operation,
                    ..
                } => {
                    let runtime = invocation
                        .payload
                        .admission
                        .mcp_runtime
                        .as_ref()
                        .ok_or_else(|| {
                            RepositoryError::CorruptRow(
                                "Managed MCP Sandbox Job lost its logical runtime binding"
                                    .to_owned(),
                            )
                        })?;
                    if invocation.payload.admission.backend_kind != CapabilityBackendKind::Mcp
                        || runtime.mcp_operation_id != operation.mcp_operation_id
                        || runtime.mcp_deployment != mcp_contract.deployment
                        || runtime.discovery_snapshot_id != mcp_contract.discovery.snapshot_id
                        || runtime.discovery_snapshot_digest
                            != mcp_contract.discovery.canonical_digest
                        || runtime.authorization_binding_id
                            != mcp_contract.authorization.authorization_binding_id
                        || runtime.authorization_generation != mcp_contract.authorization.generation
                        || runtime.authorization_context_digest
                            != mcp_contract.authorization.canonical_digest
                        || runtime.principal_id != mcp_contract.authorization.principal_id
                    {
                        return Err(RepositoryError::CorruptRow(
                            "Managed MCP Sandbox Job runtime binding is invalid".to_owned(),
                        ));
                    }
                    DetachedSandboxSourceKind::ManagedMcp
                }
            };
            Ok(CapabilityOwnerJobKind::DetachedSandbox(source_kind))
        }
        _ => Err(RepositoryError::CorruptRow(
            "Invocation current Job is not a Capability execution owner".to_owned(),
        )),
    }
}

async fn load_capability_owner_job(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &insight_platform_invocations::CapabilityInvocationRecord,
    job_id: &ResourceId,
    lock: CapabilityOwnerJobLock,
    sandbox_limits: SandboxCommandLimits,
) -> Result<(JobRecord, CapabilityOwnerJobKind), RepositoryError> {
    let query = match lock {
        CapabilityOwnerJobLock::None => {
            "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2"
        }
        CapabilityOwnerJobLock::Share => {
            "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 FOR SHARE"
        }
        CapabilityOwnerJobLock::Update => {
            "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 FOR UPDATE"
        }
    };
    let row = sqlx::query(query)
        .bind(invocation.tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("Capability owner Job"))?;
    let job = job_from_row(row)?;
    let kind = classify_capability_owner_job(invocation, &job, sandbox_limits)?;
    Ok((job, kind))
}

pub(crate) async fn prepare_capability_dispatch_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    command: PrepareCapabilityDispatch,
    database_now: DateTime<Utc>,
) -> Result<CommandOutcome<PreparedCapabilityExecution>, RepositoryError> {
    if claim_command_receipt(
        transaction,
        &command.audit,
        "capability_invocation",
        &command.invocation_id.to_string(),
        "capability.dispatch.prepare",
    )
    .await?
    {
        let invocation = load_capability_invocation(
            transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            false,
        )
        .await?;
        load_capability_execution_contract(
            transaction,
            &command.audit.tenant_id,
            &invocation.payload.admission,
        )
        .await?;
        let job_id = invocation
            .payload
            .current_job_id
            .as_ref()
            .ok_or(RepositoryError::Conflict("Capability dispatch replay"))?;
        if job_id != &command.job_id {
            return Err(RepositoryError::Conflict("Capability dispatch replay"));
        }
        let job = load_capability_job(transaction, &command.audit.tenant_id, job_id, false).await?;
        return Ok(CommandOutcome::Replayed(PreparedCapabilityExecution {
            invocation,
            job,
        }));
    }
    let current = load_capability_invocation(
        transaction,
        &command.audit.tenant_id,
        &command.invocation_id,
        true,
    )
    .await?;
    let principal =
        require_tenant_permission(transaction, &command.audit, Permission::CapabilityInvoke)
            .await?;
    if principal != current.payload.admission.principal {
        return Err(RepositoryError::Conflict(
            "Capability dispatch principal snapshot",
        ));
    }
    // Re-resolve the exact immutable execution closure before creating a Capability-owned
    // Job. Managed stdio MCP is rejected by this route check and must instead be admitted by
    // SandboxGatewayAuthority as the sole physical Sandbox Job.
    load_capability_execution_contract(
        transaction,
        &command.audit.tenant_id,
        &current.payload.admission,
    )
    .await?;
    let prepared = decide_prepare_dispatch(&current, &command, database_now)?;
    insert_capability_job(transaction, &prepared).await?;
    update_capability_invocation(transaction, &current, &prepared.invocation).await?;
    append_command_event(
        transaction,
        &command.audit,
        "capability_invocation",
        &command.invocation_id.to_string(),
        as_i64(prepared.invocation.version, "Invocation version")?,
        "capability.dispatch_prepared",
        &TypedPayload::new(
            1,
            &serde_json::json!({
                "admission_digest": prepared.invocation.payload.admission.canonical_digest,
                "job_id": prepared.job.job_id,
                "work_class": prepared.job.work_class,
            }),
        )?,
    )
    .await?;
    terminalize_command_receipt(
        transaction,
        &command.audit,
        &command.invocation_id.to_string(),
        "prepared",
    )
    .await?;
    let invocation = load_capability_invocation(
        transaction,
        &command.audit.tenant_id,
        &command.invocation_id,
        false,
    )
    .await?;
    let job = load_capability_job(
        transaction,
        &command.audit.tenant_id,
        &command.job_id,
        false,
    )
    .await?;
    Ok(CommandOutcome::Applied(PreparedCapabilityExecution {
        invocation,
        job,
    }))
}

impl PgInvocationTransaction {
    pub async fn prepare_capability_dispatch(
        &mut self,
        command: PrepareCapabilityDispatch,
    ) -> Result<CommandOutcome<PreparedCapabilityExecution>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let outcome =
            prepare_capability_dispatch_in_transaction(&mut transaction, command, database_now)
                .await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn commit_capability_outcome(
        &mut self,
        command: CommitCapabilityOutcome,
    ) -> Result<CommandOutcome<PreparedCapabilityExecution>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let receipt_payload = TypedPayload::new(
            1,
            &serde_json::json!({
                "expected_invocation_version": command.expected_invocation_version,
                "fence": {
                    "expected_version": command.fence.expected_version,
                    "lease_generation": command.fence.lease_generation,
                    "token_digest": command.fence.token_digest,
                    "worker_process_generation_id": command.fence.worker_process_generation_id,
                },
                "invocation_id": command.invocation_id,
                "job_id": command.job_id,
                "outcome": command.outcome,
                "quota_entry_ids": command.quota_entry_ids,
            }),
        )?;
        if claim_capability_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "capability.outcome.commit",
            &receipt_payload,
        )
        .await?
        {
            let invocation = load_capability_invocation(
                &mut transaction,
                &command.audit.tenant_id,
                &command.invocation_id,
                false,
            )
            .await?;
            let job = load_capability_job(
                &mut transaction,
                &command.audit.tenant_id,
                &command.job_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(PreparedCapabilityExecution {
                invocation,
                job,
            }));
        }

        let observed_job = load_capability_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            false,
        )
        .await?;
        let quota_accounts = lock_capability_quota_bundle(&mut transaction, &observed_job).await?;

        let current_invocation = load_capability_invocation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            true,
        )
        .await?;
        if current_invocation.version != command.expected_invocation_version
            || current_invocation.payload.current_job_id.as_ref() != Some(&command.job_id)
        {
            return Err(RepositoryError::Conflict("CapabilityInvocation outcome"));
        }
        let current_job = load_capability_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        if current_job.version != observed_job.version
            || current_job.payload.digest != observed_job.payload.digest
            || current_job.quota_reservation_id != observed_job.quota_reservation_id
        {
            return Err(RepositoryError::Conflict("Capability outcome Job"));
        }
        let current_projection = job_projection(&current_job)?;
        let current_payload: CapabilityJobPayload =
            decode_versioned_payload(&current_job.payload, "Capability Job")?;
        let mut trusted_outcome = command.outcome.clone();
        if let DispatchOutcome::Completed(output) = &mut trusted_outcome {
            let interface = load_exact_capability_interface_spec(
                &mut transaction,
                &command.audit.tenant_id,
                &current_invocation.payload.admission,
            )
            .await?;
            if !interface.data_policy.permits_output(
                current_invocation.payload.admission.input.classification,
                output.classification,
            ) {
                return Err(RepositoryError::InvalidInput(
                    "Capability output violates the frozen data-flow policy".to_owned(),
                ));
            }
            validate_capability_value_against_schema(
                &interface.output_schema,
                &output.value,
                interface.execution_limits.maximum_output_bytes,
            )?;
            output.validation_evidence_digest =
                insight_platform_contracts::canonical_digest(&serde_json::json!({
                    "classification": output.classification,
                    "interface_revision": current_invocation.payload.admission.interface,
                    "invocation_id": current_invocation.invocation_id,
                    "job_id": current_job.job_id,
                    "output_content_digest": output.content_digest,
                    "output_schema_digest": output.schema_digest,
                    "schema_version": 1,
                    "tenant_id": current_invocation.tenant_id,
                }))
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
                .parse()
                .map_err(|failure| {
                    RepositoryError::InvalidInput(format!(
                        "Capability validation evidence digest is invalid: {failure}"
                    ))
                })?;
        }
        let decision = decide_dispatch_outcome(
            &current_invocation,
            &current_projection,
            &current_payload,
            &command.fence,
            &trusted_outcome,
            database_now,
            self.limits,
        )?;
        settle_capability_quota(
            &mut transaction,
            &current_job,
            &quota_accounts,
            &command.quota_entry_ids,
            &command.audit.request_digest,
        )
        .await?;

        if let Some(output) = &decision.output {
            insert_capability_value_and_reference(
                &mut transaction,
                &decision.invocation,
                output,
                database_now,
            )
            .await?;
        }
        if let Some(request) = &decision.input_request {
            insert_capability_input_task(
                &mut transaction,
                &decision.invocation,
                &decision.job,
                request,
                database_now,
            )
            .await?;
        }
        update_capability_job(
            &mut transaction,
            &current_job,
            &decision.job,
            &decision.job_payload,
            database_now,
        )
        .await?;
        update_capability_invocation(&mut transaction, &current_invocation, &decision.invocation)
            .await?;

        let plan_leaf_waiting: bool = sqlx::query_scalar(
            r#"
            SELECT state = 'waiting'
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
              AND record_kind = 'node_execution'
            "#,
        )
        .bind(current_invocation.tenant_id.to_string())
        .bind(current_invocation.node_execution_id.to_string())
        .bind(current_invocation.run_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if plan_leaf_waiting {
            if matches!(trusted_outcome, DispatchOutcome::Completed(_)) {
                let mutations =
                    command
                        .resume_mutations
                        .as_ref()
                        .ok_or(RepositoryError::InvalidInput(
                            "Capability Plan leaf completion has no continuation identities"
                                .to_owned(),
                        ))?;
                let exact_output =
                    decision.invocation.payload.result.as_ref().ok_or_else(|| {
                        RepositoryError::CorruptRow(
                            "Capability success has no exact output".to_owned(),
                        )
                    })?;
                crate::repository::settle_capability_leaf_success_in_transaction(
                    &mut transaction,
                    &decision.invocation,
                    &command.job_id,
                    &exact_output.output,
                    mutations,
                    self.scope_environment_limits,
                    database_now,
                )
                .await?;
            } else {
                if command.resume_mutations.is_some() {
                    return Err(RepositoryError::InvalidInput(
                        "non-success Capability outcome has continuation identities".to_owned(),
                    ));
                }
                release_capability_plan_leaf_permit(
                    &mut transaction,
                    &decision.invocation,
                    database_now,
                )
                .await?;
            }
        } else if command.resume_mutations.is_some() {
            return Err(RepositoryError::InvalidInput(
                "non-Plan Capability outcome has continuation identities".to_owned(),
            ));
        }

        let (event_type, disposition) = match &trusted_outcome {
            DispatchOutcome::Completed(_) => ("capability.completed", "completed"),
            DispatchOutcome::Deferred(_) => ("capability.waiting", "deferred"),
            DispatchOutcome::InputRequired(_) => ("capability.input_required", "input_required"),
            DispatchOutcome::RetryableFailure { .. } => ("capability.waiting", "retry_scheduled"),
            DispatchOutcome::PermanentFailure(_) => ("capability.failed", "failed"),
            DispatchOutcome::Uncertain(_) => (
                "capability.reconciliation_required",
                "reconciliation_required",
            ),
        };
        append_scheduler_event(
            &mut transaction,
            &current_job.tenant_id,
            &command.audit.event_id,
            &command.audit.outbox_id,
            "capability_invocation",
            &command.invocation_id.to_string(),
            as_i64(decision.invocation.version, "Invocation version")?,
            Some(&current_invocation.run_id.to_string()),
            event_type,
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "job_id": decision.job.job_id,
                    "job_state": decision.job.state,
                    "lease_generation": decision.job.lease_generation,
                    "state": decision.invocation.state,
                }),
            )?,
        )
        .await?;
        terminalize_capability_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            disposition,
            &command.invocation_id,
        )
        .await?;
        let invocation = load_capability_invocation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            false,
        )
        .await?;
        let job = load_capability_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(PreparedCapabilityExecution {
            invocation,
            job,
        }))
    }

    pub async fn commit_capability_cancellation_outcome(
        &mut self,
        command: CommitCapabilityCancellationOutcome,
    ) -> Result<CommandOutcome<PreparedCapabilityExecution>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let fence = command.fence.as_ref().map(|fence| {
            serde_json::json!({
                "expected_version": fence.expected_version,
                "lease_generation": fence.lease_generation,
                "token_digest": fence.token_digest,
                "worker_process_generation_id": fence.worker_process_generation_id,
            })
        });
        let receipt_payload = TypedPayload::new(
            1,
            &serde_json::json!({
                "expected_invocation_version": command.expected_invocation_version,
                "fence": fence,
                "invocation_id": command.invocation_id,
                "job_id": command.job_id,
                "cancellation_observation_digest": command.cancellation_observation_digest,
                "external_identity_digest": command.external_identity_digest,
                "no_effect_proof_digest": command.no_effect_proof_digest,
                "cleanup_deadline": command.cleanup_deadline,
                "quota_entry_ids": command.quota_entry_ids,
            }),
        )?;
        if claim_capability_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "capability.cancellation.commit",
            &receipt_payload,
        )
        .await?
        {
            let invocation = load_capability_invocation(
                &mut transaction,
                &command.audit.tenant_id,
                &command.invocation_id,
                false,
            )
            .await?;
            let job = load_capability_job(
                &mut transaction,
                &command.audit.tenant_id,
                &command.job_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(PreparedCapabilityExecution {
                invocation,
                job,
            }));
        }
        let observed_job = load_capability_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            false,
        )
        .await?;
        let quota_accounts = match observed_job.quota_reservation_id.as_ref() {
            Some(_) => Some(lock_capability_quota_bundle(&mut transaction, &observed_job).await?),
            None => None,
        };
        let current_invocation = load_capability_invocation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            true,
        )
        .await?;
        if current_invocation.version != command.expected_invocation_version
            || current_invocation.payload.current_job_id.as_ref() != Some(&command.job_id)
        {
            return Err(RepositoryError::Conflict("Capability cancellation owner"));
        }
        let current_job = load_capability_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        if current_job.version != observed_job.version
            || current_job.payload.digest != observed_job.payload.digest
            || current_job.quota_reservation_id != observed_job.quota_reservation_id
        {
            return Err(RepositoryError::Conflict("Capability cancellation Job"));
        }
        let current_projection = job_projection(&current_job)?;
        let current_payload: CapabilityJobPayload =
            decode_versioned_payload(&current_job.payload, "Capability Job")?;
        if let Some(cleanup_deadline) = command.cleanup_deadline {
            let maximum_milliseconds = i64::try_from(MAX_CAPABILITY_TIMEOUT_MILLISECONDS)
                .map_err(|_| RepositoryError::CorruptRow("Capability cleanup limit".to_owned()))?;
            let maximum_cleanup_deadline = current_projection
                .deadline
                .checked_add_signed(chrono::Duration::milliseconds(maximum_milliseconds))
                .ok_or_else(|| {
                    RepositoryError::CorruptRow("Capability cleanup deadline overflowed".to_owned())
                })?;
            if cleanup_deadline > maximum_cleanup_deadline {
                return Err(RepositoryError::InvalidInput(
                    "Capability cleanup deadline exceeds the platform hard limit".to_owned(),
                ));
            }
        }
        let decision = decide_cancellation_outcome(
            &current_invocation,
            &current_projection,
            &current_payload,
            command.fence.as_ref(),
            &CapabilityCancellationObservation {
                cancellation_observation_digest: command.cancellation_observation_digest.clone(),
                external_identity_digest: command.external_identity_digest.clone(),
                no_effect_proof_digest: command.no_effect_proof_digest.clone(),
                cleanup_deadline: command.cleanup_deadline,
            },
            database_now,
        )?;
        match quota_accounts.as_deref() {
            Some(accounts) => {
                settle_capability_quota(
                    &mut transaction,
                    &current_job,
                    accounts,
                    &command.quota_entry_ids,
                    &command.audit.request_digest,
                )
                .await?;
            }
            None if !command.quota_entry_ids.is_empty() => {
                return Err(RepositoryError::InvalidInput(
                    "Capability cancellation supplied unused quota identities".to_owned(),
                ));
            }
            None => {}
        }
        let next_job = decision.job.as_ref().ok_or_else(|| {
            RepositoryError::CorruptRow("Capability cancellation lost its Job".to_owned())
        })?;
        let next_payload = decision.job_payload.as_ref().ok_or_else(|| {
            RepositoryError::CorruptRow("Capability cancellation lost its Job payload".to_owned())
        })?;
        let job = update_capability_job(
            &mut transaction,
            &current_job,
            next_job,
            next_payload,
            database_now,
        )
        .await?;
        update_capability_invocation(&mut transaction, &current_invocation, &decision.invocation)
            .await?;
        let (event_type, disposition) = match decision.invocation.state {
            insight_platform_contracts::InvocationState::Cancelled => {
                ("capability.cancelled", "cancelled")
            }
            insight_platform_contracts::InvocationState::ReconciliationRequired => (
                "capability.reconciliation_required",
                "reconciliation_required",
            ),
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Capability cancellation produced an invalid state".to_owned(),
                ))
            }
        };
        append_scheduler_event(
            &mut transaction,
            &current_job.tenant_id,
            &command.audit.event_id,
            &command.audit.outbox_id,
            "capability_invocation",
            &command.invocation_id.to_string(),
            as_i64(decision.invocation.version, "Invocation version")?,
            current_job.run_id.as_deref(),
            event_type,
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "job_id": command.job_id,
                    "cancellation_observation_digest": command.cancellation_observation_digest,
                    "state": decision.invocation.state,
                }),
            )?,
        )
        .await?;
        terminalize_capability_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            disposition,
            &command.invocation_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(PreparedCapabilityExecution {
            invocation: decision.invocation,
            job,
        }))
    }

    pub async fn wake_capability_invocation(
        &mut self,
        command: WakeCapabilityInvocation,
    ) -> Result<CommandOutcome<PreparedCapabilityExecution>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let operation = format!("capability.wake.{}", command.source.as_str());
        let receipt_payload = TypedPayload::new(
            1,
            &serde_json::json!({
                "callback_binding_digest": command.callback_binding_digest,
                "expected_generation": command.expected_generation,
                "invocation_id": command.invocation_id,
                "job_id": command.job_id,
                "source": command.source,
            }),
        )?;
        if claim_capability_signal_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            &operation,
            &receipt_payload,
        )
        .await?
        {
            let invocation = load_capability_invocation(
                &mut transaction,
                &command.audit.tenant_id,
                &command.invocation_id,
                false,
            )
            .await?;
            let job = load_capability_job(
                &mut transaction,
                &command.audit.tenant_id,
                &command.job_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(PreparedCapabilityExecution {
                invocation,
                job,
            }));
        }
        let current_invocation = load_capability_invocation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            true,
        )
        .await?;
        let current_job = load_capability_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        if current_invocation.payload.current_job_id.as_ref() != Some(&command.job_id) {
            return Err(RepositoryError::Conflict("Capability wake Job binding"));
        }
        let current_projection = job_projection(&current_job)?;
        let current_payload: CapabilityJobPayload =
            decode_versioned_payload(&current_job.payload, "Capability Job")?;
        let decision = decide_capability_wake(
            &current_invocation,
            &current_projection,
            &current_payload,
            command.expected_generation,
            command.source,
            command.callback_binding_digest.as_ref(),
            database_now,
        )?;
        let (invocation, job, disposition) = match decision {
            CapabilityWakeDisposition::Rejected => {
                terminalize_capability_signal_receipt(
                    &mut transaction,
                    &command.audit,
                    &command.job_id,
                    "rejected_stale",
                    &command.invocation_id,
                )
                .await?;
                (current_invocation, current_job, "rejected_stale")
            }
            CapabilityWakeDisposition::Applied(applied) => {
                let insight_platform_invocations::AppliedCapabilityWake {
                    invocation,
                    job,
                    job_payload,
                } = *applied;
                let persisted_job = update_capability_job(
                    &mut transaction,
                    &current_job,
                    &job,
                    &job_payload,
                    database_now,
                )
                .await?;
                update_capability_invocation(&mut transaction, &current_invocation, &invocation)
                    .await?;
                append_scheduler_event(
                    &mut transaction,
                    &current_job.tenant_id,
                    &command.audit.event_id,
                    &command.audit.outbox_id,
                    "capability_invocation",
                    &command.invocation_id.to_string(),
                    as_i64(invocation.version, "Invocation version")?,
                    Some(&invocation.run_id.to_string()),
                    "capability.woken",
                    &TypedPayload::new(
                        1,
                        &serde_json::json!({
                            "job_id": job.job_id,
                            "source": command.source,
                            "wake_generation": command.expected_generation,
                        }),
                    )?,
                )
                .await?;
                terminalize_capability_signal_receipt(
                    &mut transaction,
                    &command.audit,
                    &command.job_id,
                    "applied",
                    &command.invocation_id,
                )
                .await?;
                (invocation, persisted_job, "applied")
            }
        };
        debug_assert!(matches!(disposition, "applied" | "rejected_stale"));
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(PreparedCapabilityExecution {
            invocation,
            job,
        }))
    }

    pub async fn resolve_capability_input(
        &mut self,
        command: ResolveCapabilityInput,
    ) -> Result<CommandOutcome<PreparedCapabilityExecution>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "capability_invocation",
            &command.invocation_id.to_string(),
            "capability.input.resolve",
        )
        .await?
        {
            let invocation = load_capability_invocation(
                &mut transaction,
                &command.audit.tenant_id,
                &command.invocation_id,
                false,
            )
            .await?;
            let job = load_capability_owner_job(
                &mut transaction,
                &invocation,
                &command.job_id,
                CapabilityOwnerJobLock::None,
                self.sandbox_limits,
            )
            .await?
            .0;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(PreparedCapabilityExecution {
                invocation,
                job,
            }));
        }
        let resolver = require_tenant_permission(
            &mut transaction,
            &command.audit,
            Permission::InteractionRespond,
        )
        .await?;
        let current_invocation = load_capability_invocation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            true,
        )
        .await?;
        if current_invocation.version != command.expected_invocation_version
            || current_invocation.payload.current_job_id.as_ref() != Some(&command.job_id)
            || current_invocation.payload.input_task_id.as_ref() != Some(&command.input_task_id)
        {
            return Err(RepositoryError::Conflict("Capability input owner"));
        }
        let (current_job, current_job_kind) = load_capability_owner_job(
            &mut transaction,
            &current_invocation,
            &command.job_id,
            CapabilityOwnerJobLock::Update,
            self.sandbox_limits,
        )
        .await?;
        if u64::try_from(current_job.version).ok() != Some(command.expected_job_version) {
            return Err(RepositoryError::Conflict("Capability input Job"));
        }
        let current_task = load_task_for_update(
            &mut transaction,
            &command.audit.tenant_id,
            &command.input_task_id,
        )
        .await?;
        let current_job_projection = job_projection(&current_job)?;
        let current_job_payload = match current_job_kind {
            CapabilityOwnerJobKind::Capability => {
                let payload: CapabilityJobPayload =
                    decode_versioned_payload(&current_job.payload, "Capability Job")?;
                require_exact_capability_input_task(
                    &current_invocation,
                    &current_job_projection,
                    &payload,
                    &current_task,
                    &command,
                )?;
                Some(payload)
            }
            CapabilityOwnerJobKind::DetachedSandbox(DetachedSandboxSourceKind::ManagedMcp) => {
                require_exact_detached_capability_input_task(
                    &current_invocation,
                    &current_job_projection,
                    &current_task,
                    &command,
                )?;
                None
            }
            CapabilityOwnerJobKind::DetachedSandbox(
                DetachedSandboxSourceKind::SandboxCapability,
            ) => {
                return Err(RepositoryError::Conflict(
                    "Sandbox Capability does not own MCP input continuation",
                ));
            }
        };
        let task_target = match command.action {
            insight_platform_invocations::CapabilityInputAction::Accept => TaskState::Responded,
            insight_platform_invocations::CapabilityInputAction::Decline => TaskState::Declined,
            insight_platform_invocations::CapabilityInputAction::Cancel => TaskState::Cancelled,
        };
        let task_decision = decide_task_resolution(
            &task_projection(&current_task)?,
            ResolveTask {
                expected_generation: command.expected_task_generation,
                expected_version: command.expected_task_version,
                target: task_target,
                principal: (task_target != TaskState::Cancelled).then_some(resolver),
                response_value_id: command
                    .response
                    .as_ref()
                    .map(|response| response.value_id.clone()),
                response_schema_digest: command
                    .response
                    .as_ref()
                    .map(|response| response.schema_digest.clone()),
            },
            database_now,
        )?;
        let (next_invocation, capability_job_decision) = match current_job_payload.as_ref() {
            Some(payload) => {
                let decision = decide_input_response(
                    &current_invocation,
                    &current_job_projection,
                    payload,
                    insight_platform_invocations::CapabilityInputResolution {
                        expected_generation: command.expected_wake_generation,
                        action: command.action,
                        response: command.response.as_ref(),
                        database_now,
                    },
                    self.limits,
                )?;
                (decision.invocation.clone(), Some(decision))
            }
            None => (
                decide_detached_input_response(
                    &current_invocation,
                    &current_job_projection,
                    command.expected_wake_generation,
                    command.action,
                    command.response.as_ref(),
                    database_now,
                    self.limits,
                )?,
                None,
            ),
        };
        if let Some(response) = command.response.as_ref() {
            insert_capability_input_response_value(
                &mut transaction,
                &current_invocation,
                response,
                database_now,
            )
            .await?;
        }
        update_capability_task(
            &mut transaction,
            &current_task,
            &task_decision,
            database_now,
        )
        .await?;
        let job = match capability_job_decision.as_ref() {
            Some(decision) => {
                update_capability_job(
                    &mut transaction,
                    &current_job,
                    &decision.job,
                    &decision.job_payload,
                    database_now,
                )
                .await?
            }
            None => current_job.clone(),
        };
        update_capability_invocation(&mut transaction, &current_invocation, &next_invocation)
            .await?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "capability_invocation",
            &command.invocation_id.to_string(),
            as_i64(next_invocation.version, "Invocation version")?,
            match command.action {
                insight_platform_invocations::CapabilityInputAction::Accept => {
                    "interaction.responded"
                }
                insight_platform_invocations::CapabilityInputAction::Decline => {
                    "interaction.declined"
                }
                insight_platform_invocations::CapabilityInputAction::Cancel => {
                    "interaction.cancelled"
                }
            },
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "input_task_id": command.input_task_id,
                    "job_id": command.job_id,
                    "action": command.action,
                    "response_value_id": command.response.as_ref().map(|response| &response.value_id),
                    "wake_generation": command.expected_wake_generation,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.invocation_id.to_string(),
            match command.action {
                insight_platform_invocations::CapabilityInputAction::Accept => "responded",
                insight_platform_invocations::CapabilityInputAction::Decline => "declined",
                insight_platform_invocations::CapabilityInputAction::Cancel => "cancelled",
            },
        )
        .await?;
        let invocation = load_capability_invocation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(PreparedCapabilityExecution {
            invocation,
            job,
        }))
    }

    pub async fn record_capability_progress(
        &mut self,
        command: RecordCapabilityProgress,
    ) -> Result<CommandOutcome<JobRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let receipt_payload = TypedPayload::new(
            1,
            &serde_json::json!({
                "expected_invocation_version": command.expected_invocation_version,
                "fence": {
                    "expected_version": command.fence.expected_version,
                    "lease_generation": command.fence.lease_generation,
                    "token_digest": command.fence.token_digest,
                    "worker_process_generation_id": command.fence.worker_process_generation_id,
                },
                "invocation_id": command.invocation_id,
                "job_id": command.job_id,
                "progress": command.progress,
            }),
        )?;
        if claim_capability_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "capability.progress.record",
            &receipt_payload,
        )
        .await?
        {
            let job = load_capability_job(
                &mut transaction,
                &command.audit.tenant_id,
                &command.job_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(job));
        }
        let invocation = load_capability_invocation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            true,
        )
        .await?;
        if invocation.version != command.expected_invocation_version
            || invocation.payload.current_job_id.as_ref() != Some(&command.job_id)
        {
            return Err(RepositoryError::Conflict("Capability progress owner"));
        }
        let current_job = load_capability_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        let current_projection = job_projection(&current_job)?;
        let current_payload: CapabilityJobPayload =
            decode_versioned_payload(&current_job.payload, "Capability Job")?;
        let decision = decide_progress(
            &invocation,
            &current_projection,
            &current_payload,
            &command.fence,
            &command.progress,
            database_now,
            self.limits,
        )?;
        let job = update_capability_job(
            &mut transaction,
            &current_job,
            &decision.job,
            &decision.job_payload,
            database_now,
        )
        .await?;
        let disposition = match decision.durability {
            CapabilityProgressDurability::CoarseDurable => {
                append_scheduler_event(
                    &mut transaction,
                    &current_job.tenant_id,
                    &command.audit.event_id,
                    &command.audit.outbox_id,
                    "job",
                    &current_job.job_id,
                    job.version,
                    current_job.run_id.as_deref(),
                    "capability.progress",
                    &TypedPayload::new(
                        1,
                        &serde_json::json!({
                            "invocation_id": command.invocation_id,
                            "payload_digest": command.progress.payload_digest,
                            "sequence": command.progress.sequence,
                            "stage_key": command.progress.stage_key,
                        }),
                    )?,
                )
                .await?;
                "coarse_durable"
            }
            CapabilityProgressDurability::LiveOnly => "live_only",
            CapabilityProgressDurability::None => {
                return Err(RepositoryError::CorruptRow(
                    "accepted Capability progress has no durability".to_owned(),
                ))
            }
        };
        terminalize_capability_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            disposition,
            &command.job_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(job))
    }

    pub async fn control_capability_invocation(
        &mut self,
        command: ControlCapabilityInvocation,
    ) -> Result<CommandOutcome<ControlledCapabilityExecution>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let operation = match command.kind {
            CapabilityControlKind::Cancel => "capability.cancel",
            CapabilityControlKind::Timeout => "capability.timeout",
        };
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "capability_invocation",
            &command.invocation_id.to_string(),
            operation,
        )
        .await?
        {
            let invocation = load_capability_invocation(
                &mut transaction,
                &command.audit.tenant_id,
                &command.invocation_id,
                false,
            )
            .await?;
            let job = match invocation.payload.current_job_id.as_ref() {
                Some(job_id) => Some(
                    load_capability_owner_job(
                        &mut transaction,
                        &invocation,
                        job_id,
                        CapabilityOwnerJobLock::None,
                        self.sandbox_limits,
                    )
                    .await?
                    .0,
                ),
                None => None,
            };
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(ControlledCapabilityExecution {
                invocation,
                job,
            }));
        }
        let observed_invocation = load_capability_invocation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            false,
        )
        .await?;
        let (observed_job, observed_job_kind) =
            match observed_invocation.payload.current_job_id.as_ref() {
                Some(job_id) => {
                    let (job, kind) = load_capability_owner_job(
                        &mut transaction,
                        &observed_invocation,
                        job_id,
                        CapabilityOwnerJobLock::None,
                        self.sandbox_limits,
                    )
                    .await?;
                    (Some(job), Some(kind))
                }
                None => (None, None),
            };
        let quota_accounts = match observed_job.as_ref() {
            Some(job)
                if observed_job_kind == Some(CapabilityOwnerJobKind::Capability)
                    && job.quota_reservation_id.is_some() =>
            {
                Some(lock_capability_quota_bundle(&mut transaction, job).await?)
            }
            _ => None,
        };
        require_tenant_permission(&mut transaction, &command.audit, Permission::RuntimeControl)
            .await?;
        let current_invocation = load_capability_invocation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            true,
        )
        .await?;
        if current_invocation.version != command.expected_invocation_version
            || current_invocation.version != observed_invocation.version
            || current_invocation.payload != observed_invocation.payload
        {
            return Err(RepositoryError::Conflict("Capability control version"));
        }
        if let Some(CapabilityOwnerJobKind::DetachedSandbox(source_kind)) = observed_job_kind {
            let job_id = current_invocation
                .payload
                .current_job_id
                .as_ref()
                .ok_or(RepositoryError::Conflict("Sandbox Capability control Job"))?;
            let (sandbox_job, current_kind) = load_capability_owner_job(
                &mut transaction,
                &current_invocation,
                job_id,
                CapabilityOwnerJobLock::Share,
                self.sandbox_limits,
            )
            .await?;
            if current_kind != CapabilityOwnerJobKind::DetachedSandbox(source_kind)
                || observed_job.as_ref().is_none_or(|observed| {
                    observed.version != sandbox_job.version
                        || observed.payload.digest != sandbox_job.payload.digest
                        || observed.quota_reservation_id != sandbox_job.quota_reservation_id
                })
            {
                return Err(RepositoryError::Conflict("Sandbox Capability control Job"));
            }
            if !command.quota_entry_ids.is_empty() {
                return Err(RepositoryError::InvalidInput(
                    "Sandbox control quota remains owned by Sandbox authority".to_owned(),
                ));
            }
            let projection = job_projection(&sandbox_job)?;
            let invocation = decide_detached_job_control(
                &current_invocation,
                source_kind,
                &projection,
                command.kind,
                database_now,
            )?;
            update_capability_invocation(&mut transaction, &current_invocation, &invocation)
                .await?;
            let event_type = match command.kind {
                CapabilityControlKind::Cancel => "capability.cancelling",
                CapabilityControlKind::Timeout => "capability.cancelling",
            };
            append_command_event(
                &mut transaction,
                &command.audit,
                "capability_invocation",
                &command.invocation_id.to_string(),
                as_i64(invocation.version, "Invocation version")?,
                event_type,
                &TypedPayload::new(
                    1,
                    &serde_json::json!({
                        "control_kind": command.kind,
                        "job_id": job_id,
                        "state": invocation.state,
                        "task_id": null,
                    }),
                )?,
            )
            .await?;
            terminalize_command_receipt(
                &mut transaction,
                &command.audit,
                &command.invocation_id.to_string(),
                "cancelling",
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Applied(ControlledCapabilityExecution {
                invocation,
                job: Some(sandbox_job),
            }));
        }
        let current_job = match current_invocation.payload.current_job_id.as_ref() {
            Some(job_id) => {
                let (job, kind) = load_capability_owner_job(
                    &mut transaction,
                    &current_invocation,
                    job_id,
                    CapabilityOwnerJobLock::Update,
                    self.sandbox_limits,
                )
                .await?;
                if kind != CapabilityOwnerJobKind::Capability {
                    return Err(RepositoryError::Conflict("Capability control Job kind"));
                }
                Some(job)
            }
            None => None,
        };
        if let (Some(observed), Some(current)) = (&observed_job, &current_job) {
            if current.version != observed.version
                || current.payload.digest != observed.payload.digest
                || current.quota_reservation_id != observed.quota_reservation_id
            {
                return Err(RepositoryError::Conflict("Capability control Job"));
            }
        }
        let current_projection = current_job.as_ref().map(job_projection).transpose()?;
        let current_payload = current_job
            .as_ref()
            .map(|job| {
                decode_versioned_payload::<CapabilityJobPayload>(&job.payload, "Capability Job")
            })
            .transpose()?;
        let task_id = current_invocation
            .payload
            .input_task_id
            .as_ref()
            .or(current_invocation.payload.approval_task_id.as_ref())
            .cloned();
        let current_task = match task_id.as_ref() {
            Some(task_id) => Some(
                load_task_for_update(&mut transaction, &command.audit.tenant_id, task_id).await?,
            ),
            None => None,
        };
        let decision = decide_control(
            &current_invocation,
            current_projection.as_ref(),
            current_payload.as_ref(),
            command.kind,
            database_now,
        )?;
        let releases_quota = decision.job.as_ref().is_some_and(|job| job.lease.is_none())
            && quota_accounts.is_some();
        if releases_quota {
            settle_capability_quota(
                &mut transaction,
                observed_job.as_ref().ok_or_else(|| {
                    RepositoryError::CorruptRow("Capability quota owner Job disappeared".to_owned())
                })?,
                quota_accounts.as_deref().unwrap_or_default(),
                &command.quota_entry_ids,
                &command.audit.request_digest,
            )
            .await?;
        } else if !command.quota_entry_ids.is_empty() {
            return Err(RepositoryError::InvalidInput(
                "Capability control supplied unused quota settlement identities".to_owned(),
            ));
        }
        if let Some(task) = &current_task {
            cancel_capability_task(&mut transaction, task, database_now).await?;
        }
        let job = match (
            current_job.as_ref(),
            decision.job.as_ref(),
            decision.job_payload.as_ref(),
        ) {
            (Some(current), Some(next), Some(payload)) => Some(
                update_capability_job(&mut transaction, current, next, payload, database_now)
                    .await?,
            ),
            (None, None, None) => None,
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Capability control decision lost its Job pair".to_owned(),
                ))
            }
        };
        update_capability_invocation(&mut transaction, &current_invocation, &decision.invocation)
            .await?;
        let (event_type, disposition) = match decision.invocation.state {
            insight_platform_contracts::InvocationState::Cancelling => {
                ("capability.cancelling", "cancelling")
            }
            insight_platform_contracts::InvocationState::Cancelled => {
                ("capability.cancelled", "cancelled")
            }
            insight_platform_contracts::InvocationState::TimedOut => {
                ("capability.timed_out", "timed_out")
            }
            insight_platform_contracts::InvocationState::ReconciliationRequired => (
                "capability.reconciliation_required",
                "reconciliation_required",
            ),
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Capability control produced an invalid state".to_owned(),
                ))
            }
        };
        append_command_event(
            &mut transaction,
            &command.audit,
            "capability_invocation",
            &command.invocation_id.to_string(),
            as_i64(decision.invocation.version, "Invocation version")?,
            event_type,
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "control_kind": command.kind,
                    "job_id": decision.invocation.payload.current_job_id,
                    "state": decision.invocation.state,
                    "task_id": task_id,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.invocation_id.to_string(),
            disposition,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(ControlledCapabilityExecution {
            invocation: decision.invocation,
            job,
        }))
    }

    pub async fn resolve_capability_reconciliation(
        &mut self,
        command: ResolveCapabilityReconciliation,
    ) -> Result<CommandOutcome<PreparedCapabilityExecution>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "capability_invocation",
            &command.invocation_id.to_string(),
            "capability.reconciliation.resolve",
        )
        .await?
        {
            let invocation = load_capability_invocation(
                &mut transaction,
                &command.audit.tenant_id,
                &command.invocation_id,
                false,
            )
            .await?;
            let job_id = invocation.payload.current_job_id.as_ref().ok_or_else(|| {
                RepositoryError::CorruptRow("reconciled Invocation has no Job".to_owned())
            })?;
            let job =
                load_capability_job(&mut transaction, &command.audit.tenant_id, job_id, false)
                    .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(PreparedCapabilityExecution {
                invocation,
                job,
            }));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::RuntimeControl)
            .await?;
        let current_invocation = load_capability_invocation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            true,
        )
        .await?;
        if current_invocation.version != command.expected_invocation_version {
            return Err(RepositoryError::Conflict(
                "Capability reconciliation version",
            ));
        }
        let job_id = current_invocation
            .payload
            .current_job_id
            .as_ref()
            .ok_or_else(|| {
                RepositoryError::CorruptRow("Capability reconciliation owner has no Job".to_owned())
            })?;
        let current_job =
            load_capability_job(&mut transaction, &command.audit.tenant_id, job_id, true).await?;
        if u64::try_from(current_job.version).ok() != Some(command.expected_job_version) {
            return Err(RepositoryError::Conflict("Capability reconciliation Job"));
        }
        let current_projection = job_projection(&current_job)?;
        let current_payload: CapabilityJobPayload =
            decode_versioned_payload(&current_job.payload, "Capability Job")?;
        let decision = decide_reconciliation_resolution(
            &current_invocation,
            &current_projection,
            &current_payload,
            &command.resolution,
            database_now,
            self.limits,
        )?;
        if let Some(output) = &decision.output {
            insert_capability_value_and_reference(
                &mut transaction,
                &decision.invocation,
                output,
                database_now,
            )
            .await?;
        }
        let job = update_capability_job(
            &mut transaction,
            &current_job,
            &decision.job,
            &decision.job_payload,
            database_now,
        )
        .await?;
        update_capability_invocation(&mut transaction, &current_invocation, &decision.invocation)
            .await?;
        let (event_type, disposition) = match &command.resolution {
            ReconciliationResolution::Succeeded(_) => ("capability.completed", "succeeded"),
            ReconciliationResolution::Failed(_) => ("capability.failed", "failed"),
            ReconciliationResolution::Cancelled => ("capability.cancelled", "cancelled"),
        };
        append_command_event(
            &mut transaction,
            &command.audit,
            "capability_invocation",
            &command.invocation_id.to_string(),
            as_i64(decision.invocation.version, "Invocation version")?,
            event_type,
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "job_id": decision.job.job_id,
                    "state": decision.invocation.state,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.invocation_id.to_string(),
            disposition,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(PreparedCapabilityExecution {
            invocation: decision.invocation,
            job,
        }))
    }
}

impl PgRepository {
    pub async fn claim_capability_jobs(
        &self,
        command: ClaimCapabilityJobs,
    ) -> Result<Vec<ClaimedCapabilityExecution>, RepositoryError> {
        command.validate()?;
        if command.lease_milliseconds > self.invocation_limits().maximum_lease_milliseconds() {
            return Err(RepositoryError::InvalidInput(
                "Capability Job lease exceeds the frozen hard maximum".to_owned(),
            ));
        }
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let rows = sqlx::query(
            r#"
            SELECT * FROM insight_platform.jobs
            WHERE work_class = $1 AND state IN ('ready', 'retry_scheduled')
              AND terminal_at IS NULL AND worker_id IS NULL
              AND scheduled_at <= $2 AND (retry_at IS NULL OR retry_at <= $2)
              AND deadline > $2
            ORDER BY priority DESC, COALESCE(retry_at, scheduled_at), job_id
            LIMIT $3
            "#,
        )
        .bind(command.work_class.as_str())
        .bind(database_now)
        .bind(i64::from(command.limit))
        .fetch_all(&mut *transaction)
        .await?;
        let candidates = rows
            .into_iter()
            .zip(&command.slots)
            .map(|(row, slot)| capability_claim_candidate(job_from_row(row)?, slot))
            .collect::<Result<Vec<_>, RepositoryError>>()?;
        // Quota is the first mutable authority in the shared lock rank. Lock the complete,
        // sorted batch before any Run/Node/Invocation/Job row so overlapping deployment batches
        // cannot invert tenant and deployment account locks.
        let mut quota_accounts =
            lock_capability_quota_accounts(&mut transaction, &candidates, command.work_class)
                .await?;
        let mut claimed = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            // Discovery is lock-free. The authoritative path below locks
            // Run -> Node -> Invocation -> Job and then rechecks this observation.
            let CapabilityClaimCandidate {
                job: candidate_job,
                deployment_id,
                slot,
            } = candidate;
            let invocation_id = candidate_job
                .invocation_id
                .as_deref()
                .ok_or_else(|| {
                    RepositoryError::CorruptRow("Capability Job has no Invocation".to_owned())
                })?
                .parse::<ResourceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            if invocation_id.kind() != ResourceKind::CapabilityInvocation
                || candidate_job.owner_id != invocation_id.to_string()
            {
                return Err(RepositoryError::CorruptRow(
                    "Capability Job owner is invalid".to_owned(),
                ));
            }
            let tenant_id = parse_required_id(
                Some(&candidate_job.tenant_id),
                ResourceKind::Tenant,
                "Capability Job Tenant",
            )?;
            let run_id = parse_required_id(
                candidate_job.run_id.as_deref(),
                ResourceKind::Run,
                "Capability Job Run",
            )?;
            let node_id = parse_required_id(
                candidate_job.node_id.as_deref(),
                ResourceKind::NodeExecution,
                "Capability Job NodeExecution",
            )?;
            let job_id = parse_required_id(
                Some(&candidate_job.job_id),
                ResourceKind::Job,
                "Capability Job",
            )?;
            let run = load_run_for_update(&mut transaction, &tenant_id, &run_id).await?;
            let node =
                load_capability_node_for_update(&mut transaction, &tenant_id, &node_id).await?;
            let current_invocation =
                load_capability_invocation(&mut transaction, &tenant_id, &invocation_id, true)
                    .await?;
            if current_invocation.run_id != run_id
                || current_invocation.node_execution_id != node_id
            {
                return Err(RepositoryError::CorruptRow(
                    "Capability Job parent binding does not match its Invocation".to_owned(),
                ));
            }
            let plan_leaf_waiting =
                node.state == insight_platform_contracts::NodeExecutionState::Waiting;
            require_claim_parents(
                &mut transaction,
                &current_invocation,
                &run,
                &node,
                database_now,
            )
            .await?;
            let execution_contract = load_capability_execution_contract(
                &mut transaction,
                &tenant_id,
                &current_invocation.payload.admission,
            )
            .await?;
            let input =
                load_capability_execution_input(&mut transaction, &current_invocation).await?;
            let current_job =
                load_capability_job(&mut transaction, &tenant_id, &job_id, true).await?;
            // A concurrent claimant may win between discovery and ordered lock acquisition.
            // Skipping a stale candidate is the expected scheduler first-winner result.
            if current_job.version != candidate_job.version
                || current_job.state != candidate_job.state
                || current_job.worker_id != candidate_job.worker_id
                || current_job.lease_epoch != candidate_job.lease_epoch
            {
                continue;
            }
            let current_projection = job_projection(&current_job)?;
            let payload: CapabilityJobPayload =
                decode_versioned_payload(&current_job.payload, "Capability Job")?;
            let decision = decide_start_dispatch(
                &current_invocation,
                &current_projection,
                &payload,
                command.worker_process_generation_id.clone(),
                slot.lease_token_digest.clone(),
                LeasePolicy {
                    requested_milliseconds: command.lease_milliseconds,
                    hard_maximum_milliseconds: self
                        .invocation_limits()
                        .maximum_lease_milliseconds(),
                },
                database_now,
            )?;
            let continuation_input = match decision.job_payload.resume_input.as_ref() {
                Some(exact) => Some(
                    load_capability_continuation_input(
                        &mut transaction,
                        &decision.invocation,
                        exact,
                        decision.job_payload.resume_input_artifact_link_id.as_ref(),
                    )
                    .await?,
                ),
                None => None,
            };
            let quota_account_ids = reserve_capability_quota(
                &mut transaction,
                &mut quota_accounts,
                &candidate_job,
                &deployment_id,
                slot,
                &decision.job,
            )
            .await?;
            let resumed_physical_attempt =
                decision.job.attempt_count == current_projection.attempt_count;
            let job = update_started_capability_job(
                &mut transaction,
                &current_job,
                &decision.job,
                &decision.job_payload,
                &slot.quota_reservation_id,
                database_now,
            )
            .await?;
            update_capability_invocation(
                &mut transaction,
                &current_invocation,
                &decision.invocation,
            )
            .await?;
            append_scheduler_event(
                &mut transaction,
                &current_job.tenant_id,
                &slot.event_id,
                &slot.outbox_id,
                "capability_invocation",
                &current_invocation.invocation_id.to_string(),
                as_i64(decision.invocation.version, "Invocation version")?,
                Some(&current_invocation.run_id.to_string()),
                "capability.started",
                &TypedPayload::new(
                    1,
                    &serde_json::json!({
                        "attempt_count": decision.job.attempt_count,
                        "job_id": decision.job.job_id,
                        "lease_generation": decision.job.lease_generation,
                        "start_mode": if resumed_physical_attempt {
                            "resume_physical_attempt"
                        } else {
                            "new_physical_attempt"
                        },
                        "quota_account_ids": quota_account_ids,
                        "quota_reservation_id": slot.quota_reservation_id,
                        "worker_process_generation_id": command.worker_process_generation_id,
                    }),
                )?,
            )
            .await?;
            let invocation =
                load_capability_invocation(&mut transaction, &tenant_id, &invocation_id, false)
                    .await?;
            claimed.push(ClaimedCapabilityExecution {
                resume_mutations: plan_leaf_waiting.then(|| slot.resume_mutations.clone()),
                invocation,
                job,
                execution_contract,
                input,
                continuation_input,
                fence: JobFence {
                    expected_version: decision.job.version,
                    worker_process_generation_id: command.worker_process_generation_id.clone(),
                    lease_generation: decision.job.lease_generation,
                    token_digest: slot.lease_token_digest.clone(),
                },
                quota_reservation_entry_ids: slot.quota_entry_ids.clone(),
            });
        }
        transaction.commit().await?;
        Ok(claimed)
    }
}

fn capability_claim_candidate<'a>(
    job: JobRecord,
    slot: &'a insight_platform_invocations::CapabilityClaimSlot,
) -> Result<CapabilityClaimCandidate<'a>, RepositoryError> {
    let payload: CapabilityJobPayload = decode_versioned_payload(&job.payload, "Capability Job")?;
    if job.owner_kind != "capability_invocation"
        || job.owner_id != payload.binding.invocation_id.to_string()
        || job.invocation_id.as_deref() != Some(job.owner_id.as_str())
    {
        return Err(RepositoryError::CorruptRow(
            "Capability Job claim binding is invalid".to_owned(),
        ));
    }
    Ok(CapabilityClaimCandidate {
        job,
        deployment_id: payload.binding.deployment.deployment_id,
        slot,
    })
}

async fn lock_capability_quota_accounts(
    transaction: &mut Transaction<'_, Postgres>,
    candidates: &[CapabilityClaimCandidate<'_>],
    work_class: WorkClass,
) -> Result<BTreeMap<String, CapabilityQuotaAccount>, RepositoryError> {
    let tenants = candidates
        .iter()
        .map(|candidate| candidate.job.tenant_id.clone())
        .collect::<Vec<_>>();
    let deployments = candidates
        .iter()
        .map(|candidate| candidate.deployment_id.to_string())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT tenant_id, quota_account_id, scope_kind, scope_id, work_class, metric,
               limit_value, reserved_value, used_value, version
        FROM insight_platform.quota_accounts
        WHERE work_class = $1 AND (
            (scope_kind = 'tenant' AND scope_id = ANY($2)
             AND metric = $4)
            OR
            (scope_kind = 'capability_deployment' AND scope_id = ANY($3)
             AND metric = $5)
        )
        ORDER BY tenant_id, quota_account_id
        FOR UPDATE
        "#,
    )
    .bind(work_class.as_str())
    .bind(tenants)
    .bind(deployments)
    .bind(QuotaDimension::WorkClassConcurrentOperations.as_str())
    .bind(QuotaDimension::CapabilityConcurrentInvocations.as_str())
    .fetch_all(&mut **transaction)
    .await?;
    let mut accounts = BTreeMap::new();
    for row in rows {
        let account = CapabilityQuotaAccount {
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
        };
        if accounts
            .insert(account.quota_account_id.clone(), account)
            .is_some()
        {
            return Err(RepositoryError::CorruptRow(
                "Capability quota lock returned a duplicate".to_owned(),
            ));
        }
    }
    Ok(accounts)
}

async fn reserve_capability_quota(
    transaction: &mut Transaction<'_, Postgres>,
    accounts: &mut BTreeMap<String, CapabilityQuotaAccount>,
    job: &JobRecord,
    deployment_id: &ResourceId,
    slot: &insight_platform_invocations::CapabilityClaimSlot,
    next: &insight_platform_jobs::JobProjection,
) -> Result<Vec<String>, RepositoryError> {
    let mut relevant = accounts
        .values()
        .filter(|account| {
            account.tenant_id == job.tenant_id
                && account.work_class == job.work_class
                && ((account.scope_kind == "tenant"
                    && account.scope_id == job.tenant_id
                    && account.metric == QuotaDimension::WorkClassConcurrentOperations.as_str())
                    || (account.scope_kind == "capability_deployment"
                        && account.scope_id == deployment_id.to_string()
                        && account.metric
                            == QuotaDimension::CapabilityConcurrentInvocations.as_str()))
        })
        .map(|account| account.quota_account_id.clone())
        .collect::<Vec<_>>();
    relevant.sort();
    if relevant.len() != CAPABILITY_QUOTA_LINES {
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
            RepositoryError::CorruptRow("Capability quota account disappeared".to_owned())
        })?;
        if account
            .reserved_value
            .checked_add(account.used_value)
            .and_then(|value| value.checked_add(1))
            .is_none_or(|value| value > account.limit_value)
        {
            return Err(RepositoryError::QuotaExceeded);
        }
        let version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.quota_accounts
            SET reserved_value = reserved_value + 1, version = version + 1,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
              AND reserved_value + used_value + 1 <= limit_value
            RETURNING version
            "#,
        )
        .bind(&account.tenant_id)
        .bind(&account.quota_account_id)
        .bind(account.version)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::QuotaExceeded)?;
        account.version = version;
        account.reserved_value += 1;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_ledger (
                tenant_id, quota_entry_id, quota_account_id, correlation_id,
                entry_kind, reserved_amount, used_amount, account_version, request_digest
            ) VALUES ($1, $2, $3, $4, 'reserve', 1, 0, $5, $6)
            "#,
        )
        .bind(&account.tenant_id)
        .bind(entry_id.to_string())
        .bind(&account.quota_account_id)
        .bind(slot.quota_reservation_id.to_string())
        .bind(version)
        .bind(&request.digest)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(relevant)
}

async fn lock_capability_quota_bundle(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
) -> Result<Vec<CapabilityQuotaAccount>, RepositoryError> {
    let reservation_id = job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("active Capability Job has no quota reservation".to_owned())
    })?;
    let payload: CapabilityJobPayload = decode_versioned_payload(&job.payload, "Capability Job")?;
    let deployment_id = payload.binding.deployment.deployment_id.to_string();
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
    if rows.len() != CAPABILITY_QUOTA_LINES {
        return Err(RepositoryError::CorruptRow(
            "Capability quota bundle does not contain exactly two lines".to_owned(),
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
        return Err(RepositoryError::Conflict("Capability quota settlement"));
    }
    let mut tenant_lines = 0;
    let mut deployment_lines = 0;
    let mut accounts = Vec::with_capacity(rows.len());
    for row in rows {
        if row.try_get::<i64, _>("reserved_amount")? != 1
            || row.try_get::<i64, _>("reservation_used_amount")? != 0
        {
            return Err(RepositoryError::CorruptRow(
                "Capability quota reservation amount is invalid".to_owned(),
            ));
        }
        let account = CapabilityQuotaAccount {
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
        };
        if account.tenant_id != job.tenant_id
            || account.work_class != job.work_class
            || account.reserved_value < 1
        {
            return Err(RepositoryError::CorruptRow(
                "Capability quota account does not match its Job".to_owned(),
            ));
        }
        if account.scope_kind == "tenant"
            && account.scope_id == job.tenant_id
            && account.metric == QuotaDimension::WorkClassConcurrentOperations.as_str()
        {
            tenant_lines += 1;
        } else if account.scope_kind == "capability_deployment"
            && account.scope_id == deployment_id
            && account.metric == QuotaDimension::CapabilityConcurrentInvocations.as_str()
        {
            deployment_lines += 1;
        } else {
            return Err(RepositoryError::CorruptRow(
                "Capability quota bundle has an unexpected scope".to_owned(),
            ));
        }
        accounts.push(account);
    }
    if tenant_lines != 1 || deployment_lines != 1 {
        return Err(RepositoryError::CorruptRow(
            "Capability quota bundle is incomplete".to_owned(),
        ));
    }
    Ok(accounts)
}

async fn settle_capability_quota(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
    accounts: &[CapabilityQuotaAccount],
    settlement_entry_ids: &[ResourceId],
    request_digest: &insight_platform_contracts::Sha256Digest,
) -> Result<(), RepositoryError> {
    if accounts.len() != CAPABILITY_QUOTA_LINES
        || settlement_entry_ids.len() != CAPABILITY_QUOTA_LINES
    {
        return Err(RepositoryError::InvalidInput(
            "Capability quota settlement line count is invalid".to_owned(),
        ));
    }
    let reservation_id = job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("active Capability Job has no quota reservation".to_owned())
    })?;
    for (account, entry_id) in accounts.iter().zip(settlement_entry_ids) {
        let version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.quota_accounts
            SET reserved_value = reserved_value - 1, version = version + 1,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
              AND reserved_value >= 1
            RETURNING version
            "#,
        )
        .bind(&account.tenant_id)
        .bind(&account.quota_account_id)
        .bind(account.version)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("Capability quota account"))?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_ledger (
                tenant_id, quota_entry_id, quota_account_id, correlation_id,
                entry_kind, reserved_amount, used_amount, account_version, request_digest
            ) VALUES ($1, $2, $3, $4, 'settle', 1, 0, $5, $6)
            "#,
        )
        .bind(&account.tenant_id)
        .bind(entry_id.to_string())
        .bind(&account.quota_account_id)
        .bind(reservation_id)
        .bind(version)
        .bind(request_digest.to_string())
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

async fn release_capability_plan_leaf_permit(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &insight_platform_invocations::CapabilityInvocationRecord,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let run = load_run_for_update(transaction, &invocation.tenant_id, &invocation.run_id).await?;
    if run.active_work_count < 1 || run.terminal_at.is_some() {
        return Err(RepositoryError::Conflict(
            "Capability Plan leaf active permit",
        ));
    }
    let mut current = run.current.clone();
    if run.active_work_count == 1 {
        current.waiting_reason = Some("capability_invocation".to_owned());
    }
    current
        .validate(&invocation.run_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let payload = TypedPayload::from_versioned(1, &current, 1_048_576)?;
    let updated = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = CASE WHEN active_work_count = 1 THEN 'waiting' ELSE state END,
            version = version + 1, active_work_count = active_work_count - 1,
            current_schema_version = $4, current_payload = $5,
            current_payload_digest = $6, updated_at = $7
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state = 'running' AND active_work_count > 0 AND terminal_at IS NULL
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
        return Err(RepositoryError::Conflict(
            "Capability Plan leaf permit release",
        ));
    }
    Ok(())
}

async fn require_claim_parents(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &insight_platform_invocations::CapabilityInvocationRecord,
    run: &crate::repository::RunRecord,
    node: &crate::invocation_repository::LockedCapabilityNode,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let deployment = load_deployment(
        transaction,
        &invocation.tenant_id,
        &invocation.deployment_id,
    )
    .await?;
    let resource_id = deployment
        .resource_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let resource = load_resource(transaction, &invocation.tenant_id, &resource_id).await?;
    let plan_leaf = matches!(
        invocation.payload.admission.origin_key,
        insight_platform_invocations::InvocationOrigin::PlanNode { .. }
    ) && node.state == insight_platform_contracts::NodeExecutionState::Waiting;
    let run_state = run.state.parse::<RunState>().ok();
    if invocation.run_id.to_string() != run.run_id
        || if plan_leaf {
            !matches!(run_state, Some(RunState::Running | RunState::Waiting))
        } else {
            run_state != Some(RunState::Running)
        }
        || run.current.control.pause_requested
        || run.current.control.cancel_requested_at.is_some()
        || run.current.control.timeout_requested_at.is_some()
        || if plan_leaf {
            node.state != insight_platform_contracts::NodeExecutionState::Waiting
        } else {
            node.state != insight_platform_contracts::NodeExecutionState::Running
        }
        || node.run_id != run.run_id
        || database_now >= run.deadline
        || database_now >= node.deadline
        || deployment.bindings.digest
            != invocation
                .payload
                .admission
                .deployment
                .deployment_digest
                .to_string()
        || resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || resource.gate_state != "enabled"
    {
        return Err(RepositoryError::Conflict("Capability dispatch parent"));
    }
    if plan_leaf {
        let mut current = run.current.clone();
        if run_state == Some(RunState::Waiting) {
            current.waiting_reason = None;
        }
        current
            .validate(&invocation.run_id)
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
            return Err(RepositoryError::Conflict(
                "Capability Plan leaf claim permit",
            ));
        }
    }
    Ok(())
}

async fn insert_capability_job(
    transaction: &mut Transaction<'_, Postgres>,
    prepared: &insight_platform_invocations::PreparedCapabilityDispatch,
) -> Result<(), RepositoryError> {
    let payload = TypedPayload::from_versioned(1, &prepared.job_payload, 1_048_576)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
            tenant_id, job_id, work_class, owner_kind, owner_id, invocation_id,
            run_id, node_id, state, version, attempt_no, attempt_limit, lease_epoch,
            scheduled_at, deadline, priority, request_digest, effect_key_digest,
            payload_schema_version, payload, payload_digest, created_at, updated_at
        ) VALUES (
            $1, $2, $3, 'capability_invocation', $4, $4,
            $5, $6, 'ready', 1, 0, $7, 0,
            $8, $9, 0, $10, $11, $12, $13, $14, $15, $15
        )
        "#,
    )
    .bind(prepared.job.tenant_id.to_string())
    .bind(prepared.job.job_id.to_string())
    .bind(prepared.job.work_class.as_str())
    .bind(prepared.invocation.invocation_id.to_string())
    .bind(prepared.invocation.run_id.to_string())
    .bind(prepared.invocation.node_execution_id.to_string())
    .bind(i32::try_from(prepared.job.attempt_limit).map_err(|_| {
        RepositoryError::InvalidInput("Capability attempt limit exceeds integer".to_owned())
    })?)
    .bind(prepared.job.scheduled_at)
    .bind(prepared.job.deadline)
    .bind(prepared.job_payload.binding.admission_digest.to_string())
    .bind(prepared.job_payload.binding.effect_key_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(prepared.invocation.updated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(crate) async fn update_capability_invocation(
    transaction: &mut Transaction<'_, Postgres>,
    current: &insight_platform_invocations::CapabilityInvocationRecord,
    next: &insight_platform_invocations::CapabilityInvocationRecord,
) -> Result<(), RepositoryError> {
    next.validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let payload = TypedPayload::from_versioned(1, &next.payload, 1_048_576)?;
    let updated = sqlx::query(
        r#"
        UPDATE insight_platform.invocations
        SET state = $4, version = $5, output_value_id = $6,
            payload_schema_version = $7, payload = $8, payload_digest = $9,
            retry_at = $10, started_at = $11, terminal_at = $12, updated_at = $13
        WHERE tenant_id = $1 AND invocation_id = $2 AND version = $3
        RETURNING invocation_id
        "#,
    )
    .bind(next.tenant_id.to_string())
    .bind(next.invocation_id.to_string())
    .bind(as_i64(current.version, "Invocation version")?)
    .bind(next.state.as_str())
    .bind(as_i64(next.version, "Invocation version")?)
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
        return Err(RepositoryError::Conflict("CapabilityInvocation"));
    }
    Ok(())
}

async fn update_started_capability_job(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobRecord,
    next: &insight_platform_jobs::JobProjection,
    next_payload: &CapabilityJobPayload,
    quota_reservation_id: &ResourceId,
    database_now: DateTime<Utc>,
) -> Result<JobRecord, RepositoryError> {
    let lease = next.lease.as_ref().ok_or_else(|| {
        RepositoryError::CorruptRow("started Capability Job has no lease".to_owned())
    })?;
    let payload = TypedPayload::from_versioned(1, next_payload, 1_048_576)?;
    let row = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'running', version = $4, attempt_no = $5, lease_epoch = $6,
            worker_id = $7, lease_token_digest = $8, lease_expires_at = $9,
            heartbeat_at = $10, retry_at = NULL, started_at = COALESCE(started_at, $10),
            quota_reservation_id = $11, payload_schema_version = $12, payload = $13,
            payload_digest = $14, updated_at = $10
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND state IN ('ready', 'retry_scheduled') AND worker_id IS NULL
        RETURNING *
        "#,
    )
    .bind(&current.tenant_id)
    .bind(&current.job_id)
    .bind(current.version)
    .bind(as_i64(next.version, "Job version")?)
    .bind(i32::try_from(next.attempt_count).map_err(|_| {
        RepositoryError::InvalidInput("Job attempt count exceeds integer".to_owned())
    })?)
    .bind(as_i64(next.lease_generation, "Job lease generation")?)
    .bind(lease.worker_process_generation_id.to_string())
    .bind(lease.token_digest.to_string())
    .bind(lease.expires_at)
    .bind(database_now)
    .bind(quota_reservation_id.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Capability Job claim"))?;
    job_from_row(row)
}

pub(crate) async fn load_capability_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    for_update: bool,
) -> Result<JobRecord, RepositoryError> {
    let query = if for_update {
        "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 FOR UPDATE"
    } else {
        "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2"
    };
    let row = sqlx::query(query)
        .bind(tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("Capability Job"))?;
    let record = job_from_row(row)?;
    if !matches!(
        record.work_class.as_str(),
        "capability_native" | "capability_remote"
    ) || record.owner_kind != "capability_invocation"
    {
        return Err(RepositoryError::CorruptRow(
            "Job is not a Capability Job".to_owned(),
        ));
    }
    Ok(record)
}

async fn update_capability_job(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobRecord,
    next: &insight_platform_jobs::JobProjection,
    next_payload: &CapabilityJobPayload,
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
    let (wake_kind, wake_state, wake_generation) = next
        .wake
        .as_ref()
        .map(|wake| {
            (
                Some(wake.kind.as_str()),
                Some("pending"),
                as_i64(wake.generation, "Job wake generation"),
            )
        })
        .transpose_tuple()?;
    let terminal = matches!(
        next.state,
        JobState::Succeeded | JobState::Failed | JobState::Cancelled | JobState::TimedOut
    );
    let quota_reservation_id = if next.lease.is_some() {
        Some(current.quota_reservation_id.as_deref().ok_or_else(|| {
            RepositoryError::CorruptRow("leased Capability Job has no quota reservation".to_owned())
        })?)
    } else {
        None
    };
    let row = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $4, version = $5, attempt_no = $6, lease_epoch = $7,
            worker_id = $8, lease_token_digest = $9, lease_expires_at = $10,
            heartbeat_at = $11, scheduled_at = $12, retry_at = $13,
            wake_kind = $14, wake_state = $15, wake_generation = $16,
            result_digest = $17, payload_schema_version = $18, payload = $19,
            payload_digest = $20, terminal_at = $21, updated_at = $22,
            quota_reservation_id = $23
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
        RETURNING *
        "#,
    )
    .bind(&current.tenant_id)
    .bind(&current.job_id)
    .bind(current.version)
    .bind(next.state.as_str())
    .bind(as_i64(next.version, "Job version")?)
    .bind(i32::try_from(next.attempt_count).map_err(|_| {
        RepositoryError::InvalidInput("Job attempt count exceeds integer".to_owned())
    })?)
    .bind(as_i64(next.lease_generation, "Job lease generation")?)
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
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(terminal.then_some(database_now))
    .bind(database_now)
    .bind(quota_reservation_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Capability Job"))?;
    job_from_row(row)
}

trait TransposeWakeTuple {
    fn transpose_tuple(
        self,
    ) -> Result<(Option<&'static str>, Option<&'static str>, i64), RepositoryError>;
}

impl TransposeWakeTuple
    for Option<(
        Option<&'static str>,
        Option<&'static str>,
        Result<i64, RepositoryError>,
    )>
{
    fn transpose_tuple(
        self,
    ) -> Result<(Option<&'static str>, Option<&'static str>, i64), RepositoryError> {
        match self {
            Some((kind, state, generation)) => Ok((kind, state, generation?)),
            None => Ok((None, None, 0)),
        }
    }
}

pub(crate) async fn insert_capability_value_and_reference(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &insight_platform_invocations::CapabilityInvocationRecord,
    output: &insight_platform_invocations::CapabilityOutputValue,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    if let ValueRef::Artifact { artifact } = &output.value {
        require_ready_run_artifact(transaction, &invocation.tenant_id, artifact).await?;
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
        ) VALUES ($1, $2, $3, $4, 'capability_output', $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(invocation.tenant_id.to_string())
    .bind(output.value_id.to_string())
    .bind(invocation.run_id.to_string())
    .bind(invocation.node_execution_id.to_string())
    .bind(output.classification.as_str())
    .bind(output.schema_digest.to_string())
    .bind(output.content_digest.to_string())
    .bind(inline_value)
    .bind(artifact_id)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;

    let ValueRef::Artifact { artifact } = &output.value else {
        return Ok(());
    };
    let link_id = output.artifact_link_id.as_ref().ok_or_else(|| {
        RepositoryError::InvalidInput("Capability Artifact output has no link identity".to_owned())
    })?;
    let snapshot = ArtifactReferenceSnapshot {
        schema_version: 1,
        artifact_id: artifact.artifact_id().clone(),
        owner_id: invocation.invocation_id.clone(),
        reference_kind: ArtifactReferenceKind::Output,
        purpose: ArtifactPurpose::CapabilityOutput,
        created_by: invocation.payload.admission.principal.principal_id.clone(),
    };
    let payload = TypedPayload::from_versioned(1, &snapshot, 262_144)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_links (
            tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
            target_artifact_id, link_key_digest, state, payload_schema_version,
            payload, payload_digest, created_at, updated_at
        ) VALUES ($1, $2, 'reference', 'capability_invocation', $3, $4, $5,
                  'active', $6, $7, $8, $9, $9)
        "#,
    )
    .bind(invocation.tenant_id.to_string())
    .bind(link_id.to_string())
    .bind(invocation.invocation_id.to_string())
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

pub(crate) async fn insert_capability_input_task(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &insight_platform_invocations::CapabilityInvocationRecord,
    job: &insight_platform_jobs::JobProjection,
    request: &insight_platform_invocations::BackendInputRequest,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let definition = TaskDefinition::CapabilityInput {
        owner_version: invocation.version,
        owner_snapshot_digest: invocation.payload.admission.canonical_digest.clone(),
        job_id: job.job_id.clone(),
        wake_generation: job.lease_generation,
        opaque_state_digest: request.opaque_state_digest.clone(),
        interaction_kind: request.interaction_kind,
        response_schema: request.response_schema.clone(),
        eligible_principal_rule_digest: request.eligible_principal_rule_digest.clone(),
        exact_eligible_principal_id: request.exact_eligible_principal_id.clone(),
        safe_prompt_key: request.safe_prompt_key.clone(),
    };
    let task_kind = definition.task_kind();
    let task = TaskPayload {
        definition,
        created_by: invocation.payload.admission.principal.clone(),
        resolution: None,
    };
    task.validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let payload = TypedPayload::new(1, &task)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.tasks (
            tenant_id, task_id, task_kind, owner_kind, owner_id, run_id, node_id,
            invocation_id, state, generation, version, response_schema_digest,
            principal_snapshot_schema_version, payload_schema_version, payload,
            payload_digest, deadline, created_at, updated_at
        ) VALUES ($1, $2, $3, 'capability_invocation', $4, $5, $6, $4,
                  'pending', 1, 1, $7, 1, $8, $9, $10, $11, $12, $12)
        "#,
    )
    .bind(invocation.tenant_id.to_string())
    .bind(request.input_task_id.to_string())
    .bind(task_kind.as_str())
    .bind(invocation.invocation_id.to_string())
    .bind(invocation.run_id.to_string())
    .bind(invocation.node_execution_id.to_string())
    .bind(request.response_schema_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(request.deadline)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn require_exact_capability_input_task(
    invocation: &insight_platform_invocations::CapabilityInvocationRecord,
    job: &insight_platform_jobs::JobProjection,
    job_payload: &CapabilityJobPayload,
    task: &TaskRecord,
    command: &ResolveCapabilityInput,
) -> Result<(), RepositoryError> {
    job_payload
        .validate_for(invocation, job)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let request = job_payload.input_request.as_ref().ok_or_else(|| {
        RepositoryError::CorruptRow("Capability input Job has no request binding".to_owned())
    })?;
    let projection = task_projection(task)?;
    let TaskDefinition::CapabilityInput {
        owner_version,
        owner_snapshot_digest,
        job_id,
        wake_generation,
        opaque_state_digest,
        interaction_kind,
        response_schema,
        eligible_principal_rule_digest,
        exact_eligible_principal_id,
        safe_prompt_key,
    } = &projection.payload.definition
    else {
        return Err(RepositoryError::CorruptRow(
            "Capability input Task has the wrong definition".to_owned(),
        ));
    };
    if task.owner_kind != "capability_invocation"
        || task.owner_id != invocation.invocation_id.to_string()
        || task.invocation_id.as_deref() != Some(invocation.invocation_id.to_string().as_str())
        || task.run_id.as_deref() != Some(invocation.run_id.to_string().as_str())
        || task.node_id.as_deref() != Some(invocation.node_execution_id.to_string().as_str())
        || projection.state != TaskState::Pending
        || projection.generation != command.expected_task_generation
        || projection.version != command.expected_task_version
        || projection.response_schema_digest.as_ref() != Some(&request.response_schema_digest)
        || projection.deadline != request.deadline
        || projection.payload.created_by != invocation.payload.admission.principal
        || *owner_version != invocation.version
        || owner_snapshot_digest != &invocation.payload.admission.canonical_digest
        || job_id != &job.job_id
        || *wake_generation != request.wake_generation
        || *wake_generation != command.expected_wake_generation
        || opaque_state_digest != &request.opaque_state_digest
        || *interaction_kind != request.interaction_kind
        || response_schema != &request.response_schema
        || response_schema.canonical_digest != request.response_schema_digest
        || exact_eligible_principal_id != &request.exact_eligible_principal_id
        || exact_eligible_principal_id
            .as_ref()
            .is_some_and(|principal_id| principal_id != &command.audit.principal_id)
        || eligible_principal_rule_digest != &request.eligible_principal_rule_digest
        || eligible_principal_rule_digest != &command.eligible_principal_rule_digest
        || safe_prompt_key != &request.safe_prompt_key
    {
        return Err(RepositoryError::Conflict("Capability input Task binding"));
    }
    Ok(())
}

fn require_exact_detached_capability_input_task(
    invocation: &insight_platform_invocations::CapabilityInvocationRecord,
    job: &insight_platform_jobs::JobProjection,
    task: &TaskRecord,
    command: &ResolveCapabilityInput,
) -> Result<(), RepositoryError> {
    let Some(CapabilityDetachedPending::InputRequired {
        request,
        resolution: None,
        ..
    }) = &invocation.payload.detached_pending
    else {
        return Err(RepositoryError::Conflict(
            "Managed MCP input continuation is not pending",
        ));
    };
    let projection = task_projection(task)?;
    let TaskDefinition::CapabilityInput {
        owner_version,
        owner_snapshot_digest,
        job_id,
        wake_generation,
        opaque_state_digest,
        interaction_kind,
        response_schema,
        eligible_principal_rule_digest,
        exact_eligible_principal_id,
        safe_prompt_key,
    } = &projection.payload.definition
    else {
        return Err(RepositoryError::CorruptRow(
            "Managed MCP input Task has the wrong definition".to_owned(),
        ));
    };
    if job.state != JobState::Succeeded
        || job.lease.is_some()
        || job.work_class != WorkClass::Sandbox
        || task.owner_kind != "capability_invocation"
        || task.owner_id != invocation.invocation_id.to_string()
        || task.invocation_id.as_deref() != Some(invocation.invocation_id.to_string().as_str())
        || task.run_id.as_deref() != Some(invocation.run_id.to_string().as_str())
        || task.node_id.as_deref() != Some(invocation.node_execution_id.to_string().as_str())
        || projection.state != TaskState::Pending
        || projection.generation != command.expected_task_generation
        || projection.version != command.expected_task_version
        || projection.response_schema_digest.as_ref() != Some(&request.response_schema_digest)
        || projection.deadline != request.deadline
        || projection.payload.created_by != invocation.payload.admission.principal
        || *owner_version != invocation.version
        || owner_snapshot_digest != &invocation.payload.admission.canonical_digest
        || job_id != &job.job_id
        || *wake_generation != job.lease_generation
        || *wake_generation != command.expected_wake_generation
        || opaque_state_digest != &request.opaque_state_digest
        || *interaction_kind != request.interaction_kind
        || response_schema != &request.response_schema
        || response_schema.canonical_digest != request.response_schema_digest
        || exact_eligible_principal_id != &request.exact_eligible_principal_id
        || exact_eligible_principal_id
            .as_ref()
            .is_some_and(|principal_id| principal_id != &command.audit.principal_id)
        || eligible_principal_rule_digest != &request.eligible_principal_rule_digest
        || eligible_principal_rule_digest != &command.eligible_principal_rule_digest
        || safe_prompt_key != &request.safe_prompt_key
    {
        return Err(RepositoryError::Conflict("Managed MCP input Task binding"));
    }
    Ok(())
}

async fn update_capability_task(
    transaction: &mut Transaction<'_, Postgres>,
    current: &TaskRecord,
    next: &insight_platform_tasks::TaskProjection,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let payload = TypedPayload::new(1, &next.payload)?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.tasks
        SET state = $4, version = $5, payload_schema_version = $6,
            payload = $7, payload_digest = $8, response_value_id = $9,
            responded_at = $10, updated_at = $10
        WHERE tenant_id = $1 AND task_id = $2 AND version = $3
          AND generation = $11 AND state = 'pending' AND responded_at IS NULL
        "#,
    )
    .bind(&current.tenant_id)
    .bind(&current.task_id)
    .bind(current.version)
    .bind(next.state.as_str())
    .bind(as_i64(next.version, "Task version")?)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(next.response_value_id.as_ref().map(ToString::to_string))
    .bind(database_now)
    .bind(as_i64(next.generation, "Task generation")?)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("Capability input Task"));
    }
    Ok(())
}

async fn cancel_capability_task(
    transaction: &mut Transaction<'_, Postgres>,
    current: &TaskRecord,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let projection = task_projection(current)?;
    if projection.state != TaskState::Pending {
        return Err(RepositoryError::Conflict("Capability Task first-winner"));
    }
    let next = decide_task_resolution(
        &projection,
        ResolveTask {
            expected_generation: projection.generation,
            expected_version: projection.version,
            target: TaskState::Cancelled,
            principal: None,
            response_value_id: None,
            response_schema_digest: None,
        },
        database_now,
    )?;
    update_capability_task(transaction, current, &next, database_now).await
}

async fn insert_capability_input_response_value(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &insight_platform_invocations::CapabilityInvocationRecord,
    response: &insight_platform_invocations::CapabilityInputResponse,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    if let ValueRef::Artifact { artifact } = &response.value {
        require_ready_run_artifact(transaction, &invocation.tenant_id, artifact).await?;
    }
    let (inline_value, artifact_id) = match &response.value {
        ValueRef::Inline { value } => (Some(value), None),
        ValueRef::Artifact { artifact } => (None, Some(artifact.artifact_id().to_string())),
    };
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, node_id, value_kind, classification,
            schema_digest, content_digest, inline_value, artifact_id, created_at
        ) VALUES ($1, $2, $3, $4, 'capability_input_response', $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(invocation.tenant_id.to_string())
    .bind(response.value_id.to_string())
    .bind(invocation.run_id.to_string())
    .bind(invocation.node_execution_id.to_string())
    .bind(response.classification.as_str())
    .bind(response.schema_digest.to_string())
    .bind(response.content_digest.to_string())
    .bind(inline_value)
    .bind(artifact_id)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;

    let ValueRef::Artifact { artifact } = &response.value else {
        return Ok(());
    };
    let link_id = response.artifact_link_id.as_ref().ok_or_else(|| {
        RepositoryError::InvalidInput(
            "Capability input response Artifact has no link identity".to_owned(),
        )
    })?;
    let snapshot = ArtifactReferenceSnapshot {
        schema_version: 1,
        artifact_id: artifact.artifact_id().clone(),
        owner_id: invocation.invocation_id.clone(),
        reference_kind: ArtifactReferenceKind::Input,
        purpose: ArtifactPurpose::CapabilityInput,
        created_by: invocation.payload.admission.principal.principal_id.clone(),
    };
    let payload = TypedPayload::from_versioned(1, &snapshot, 262_144)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_links (
            tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
            target_artifact_id, link_key_digest, state, payload_schema_version,
            payload, payload_digest, created_at, updated_at
        ) VALUES ($1, $2, 'reference', 'capability_invocation', $3, $4, $5,
                  'active', $6, $7, $8, $9, $9)
        "#,
    )
    .bind(invocation.tenant_id.to_string())
    .bind(link_id.to_string())
    .bind(invocation.invocation_id.to_string())
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

async fn claim_capability_worker_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CapabilityWorkerAudit,
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
        return Err(RepositoryError::Conflict("Capability worker receipt"));
    }
    Ok(true)
}

async fn terminalize_capability_worker_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CapabilityWorkerAudit,
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
        return Err(RepositoryError::Conflict("Capability worker receipt"));
    }
    Ok(())
}

async fn claim_capability_signal_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CapabilitySignalAudit,
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
        return Err(RepositoryError::Conflict("Capability signal receipt"));
    }
    Ok(true)
}

async fn terminalize_capability_signal_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CapabilitySignalAudit,
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
        return Err(RepositoryError::Conflict("Capability signal receipt"));
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

fn as_i64(value: u64, kind: &str) -> Result<i64, RepositoryError> {
    i64::try_from(value)
        .map_err(|_| RepositoryError::InvalidInput(format!("{kind} exceeds bigint")))
}

fn parse_required_id(
    value: Option<&str>,
    expected_kind: ResourceKind,
    label: &str,
) -> Result<ResourceId, RepositoryError> {
    let value = value.ok_or_else(|| RepositoryError::CorruptRow(format!("{label} is missing")))?;
    let id = value
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if id.kind() != expected_kind {
        return Err(RepositoryError::CorruptRow(format!(
            "{label} has the wrong resource kind"
        )));
    }
    Ok(id)
}
