use crate::{
    digest, digest_without_field, value_ref_digest, ContextAdmissionSnapshot, ContextDatasetView,
    ContextObservation, ContextQueryError, ContextQueryLimits, ContextQueryRequest,
    ContextQuotaCeiling, DataAccessGrant,
};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    CommandAudit, CommandOutcome, ContextBackendOutcomeKind, ContextBindingSnapshot,
    ContextConsistencyPolicy, ContextDeploymentClosure, ContextImplementationResourceSpec,
    ContextInterfaceResourceSpec, ContextQueryState, ExactVersionRef, Failure, FrozenSlotTarget,
    JobState, NodeExecutionState, Permission, PlanNodeKind, PrincipalSnapshot, ResourceId,
    ResourceKind, RunBindingsSnapshot, RunState, Sha256Digest, ValueRef, WorkClass,
};
use insight_platform_invocations::{ExactInvocationValueRef, InvocationValueStorage};
use insight_platform_jobs::{
    decide_claim, decide_claim_continuation, decide_owner_cancelling, decide_owner_terminal,
    decide_resume, decide_retry, decide_start, decide_terminal, decide_wait, decide_wake, JobFence,
    JobOwnerRef, JobProjection, LeasePolicy, WakeContract, WakeSource,
};
use serde::{Deserialize, Serialize};

pub const CONTEXT_QUOTA_LINES: usize = 3;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextQueryResult {
    pub schema_version: u32,
    pub observation: ContextObservation,
    pub output: ExactInvocationValueRef,
    pub validation_evidence_digest: Sha256Digest,
    pub result_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextQueryPayload {
    pub schema_version: u32,
    pub admission: ContextAdmissionSnapshot,
    pub current_job_id: Option<ResourceId>,
    pub result: Option<ContextQueryResult>,
    pub failure: Option<Failure>,
}

impl ContextQueryPayload {
    fn validate_for(
        &self,
        state: ContextQueryState,
        query_id: &ResourceId,
        limits: ContextQueryLimits,
    ) -> Result<(), ContextQueryError> {
        self.admission.validate(limits)?;
        if self.schema_version != 1
            || &self.admission.context_query_id != query_id
            || self
                .current_job_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::Job)
            || self
                .failure
                .as_ref()
                .is_some_and(|failure| failure.validate(1_024).is_err())
        {
            return Err(ContextQueryError::InvalidAdmission);
        }
        if let Some(result) = &self.result {
            result
                .observation
                .validate_for(query_id, &self.admission, limits)?;
            result
                .output
                .validate()
                .map_err(|_| ContextQueryError::InvalidOutcome)?;
            if result.schema_version != 1
                || result.output.run_id != self.admission.run_id
                || result.output.producing_node_id.as_ref()
                    != Some(&self.admission.node_execution_id)
                || result.output.schema_digest != self.admission.interface.observation_schema_digest
                || result.output.content_digest != result.observation.canonical_digest
                || result.result_digest
                    != digest(&serde_json::json!({
                        "observation": result.observation,
                        "output": result.output,
                        "validation_evidence_digest": result.validation_evidence_digest,
                    }))?
            {
                return Err(ContextQueryError::InvalidOutcome);
            }
        }
        let valid = match state {
            ContextQueryState::Created | ContextQueryState::AwaitingAuthorization => {
                self.current_job_id.is_none() && self.result.is_none() && self.failure.is_none()
            }
            ContextQueryState::Ready | ContextQueryState::RetryScheduled => {
                self.result.is_none() && self.failure.is_none()
            }
            ContextQueryState::InFlight | ContextQueryState::Deferred => {
                self.current_job_id.is_some() && self.result.is_none() && self.failure.is_none()
            }
            ContextQueryState::Succeeded => self.result.is_some() && self.failure.is_none(),
            ContextQueryState::Failed => self.result.is_none() && self.failure.is_some(),
            ContextQueryState::Cancelled | ContextQueryState::TimedOut => self.result.is_none(),
        };
        if !valid {
            return Err(ContextQueryError::InvalidTransition);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContextQueryRecord {
    pub tenant_id: ResourceId,
    pub context_query_id: ResourceId,
    pub run_id: ResourceId,
    pub node_execution_id: ResourceId,
    pub logical_key: String,
    pub deployment_id: ResourceId,
    pub input_value_id: ResourceId,
    pub output_value_id: Option<ResourceId>,
    pub state: ContextQueryState,
    pub version: u64,
    pub payload: ContextQueryPayload,
    pub deadline: DateTime<Utc>,
    pub retry_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub terminal_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ContextQueryRecord {
    pub fn validate(&self, limits: ContextQueryLimits) -> Result<(), ContextQueryError> {
        self.payload
            .validate_for(self.state, &self.context_query_id, limits)?;
        let terminal = is_terminal(self.state);
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.context_query_id.kind() != ResourceKind::ContextQuery
            || self.run_id != self.payload.admission.run_id
            || self.node_execution_id != self.payload.admission.node_execution_id
            || self.logical_key
                != context_query_logical_key(
                    &self.payload.admission.slot_id,
                    self.payload.admission.request.page_ordinal,
                )
            || self.deployment_id
                != self
                    .payload
                    .admission
                    .binding
                    .context_deployment
                    .deployment_id
            || self.input_value_id != self.payload.admission.request.input.value_id
            || self.output_value_id
                != self
                    .payload
                    .result
                    .as_ref()
                    .map(|result| result.output.value_id.clone())
            || self.deadline != self.payload.admission.deadline
            || self.version == 0
            || (self.state == ContextQueryState::RetryScheduled) != self.retry_at.is_some()
            || terminal != self.terminal_at.is_some()
            || self.updated_at < self.created_at
            || self.deadline < self.created_at
        {
            return Err(ContextQueryError::InvalidTransition);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CreateContextQuery {
    pub audit: CommandAudit,
    pub context_query_id: ResourceId,
    pub run_id: ResourceId,
    pub node_execution_id: ResourceId,
    pub expected_run_version: u64,
    pub expected_node_version: u64,
    pub slot_id: String,
    pub request: ContextQueryRequest,
    pub requested_attempt_limit: u32,
    pub result_byte_ceiling: u64,
}

impl CreateContextQuery {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: ContextQueryLimits,
    ) -> Result<(), ContextQueryError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ContextQueryError::InvalidIdentity)?;
        if self.context_query_id.kind() != ResourceKind::ContextQuery
            || self.run_id.kind() != ResourceKind::Run
            || self.node_execution_id.kind() != ResourceKind::NodeExecution
            || self.expected_run_version == 0
            || self.expected_node_version == 0
            || self.requested_attempt_limit == 0
            || self.requested_attempt_limit > limits.maximum_attempts()
            || self.result_byte_ceiling == 0
            || self.result_byte_ceiling > limits.maximum_result_bytes()
        {
            return Err(ContextQueryError::InvalidRequest);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ContextAdmissionFacts {
    pub run_state: RunState,
    pub run_version: u64,
    pub run_pause_requested: bool,
    pub run_cancel_requested: bool,
    pub run_timeout_requested: bool,
    pub run_deadline: DateTime<Utc>,
    pub run_bindings: RunBindingsSnapshot,
    pub node_state: NodeExecutionState,
    pub node_version: u64,
    pub node_kind: PlanNodeKind,
    pub node_deadline: DateTime<Utc>,
    pub context_gate_enabled: bool,
    pub binding: ContextBindingSnapshot,
    pub context_closure: ContextDeploymentClosure,
    pub interface_revision: ExactVersionRef,
    pub interface: ContextInterfaceResourceSpec,
    pub implementation_revision: ExactVersionRef,
    pub implementation: ContextImplementationResourceSpec,
    pub dataset_view: ContextDatasetView,
    pub dataset_generation: Option<insight_platform_contracts::ContextDatasetGenerationSpec>,
    pub principal: PrincipalSnapshot,
    pub policy_generation: u64,
    pub database_now: DateTime<Utc>,
}

pub fn decide_context_query_admission(
    command: &CreateContextQuery,
    facts: ContextAdmissionFacts,
    limits: ContextQueryLimits,
) -> Result<ContextQueryRecord, ContextQueryError> {
    command.validate_at(facts.database_now, limits)?;
    facts
        .run_bindings
        .validate()
        .map_err(|_| ContextQueryError::InvalidAdmission)?;
    facts
        .binding
        .validate()
        .map_err(|_| ContextQueryError::InvalidBinding)?;
    facts
        .principal
        .validate()
        .map_err(|_| ContextQueryError::InvalidAdmission)?;
    let deadline = facts.run_deadline.min(facts.node_deadline);
    if facts.run_state != RunState::Running
        || facts.run_version != command.expected_run_version
        || facts.run_pause_requested
        || facts.run_cancel_requested
        || facts.run_timeout_requested
        || facts.node_state != NodeExecutionState::Running
        || facts.node_version != command.expected_node_version
        || facts.node_kind != PlanNodeKind::ContextQuery
        || facts.database_now >= deadline
        || !facts.context_gate_enabled
        || facts.principal.tenant_id != command.audit.tenant_id
        || facts.principal.principal_id != command.audit.principal_id
        || facts.principal.principal_kind != command.audit.principal_kind
        || !facts
            .principal
            .permissions
            .contains(Permission::ContextQuery)
        || facts.run_bindings.principal.principal_id != command.audit.principal_id
    {
        return Err(ContextQueryError::AdmissionRejected);
    }
    let slot = facts
        .run_bindings
        .slots
        .iter()
        .find(|slot| slot.slot_id == command.slot_id)
        .ok_or(ContextQueryError::InvalidBinding)?;
    let FrozenSlotTarget::Context { binding } = &slot.target else {
        return Err(ContextQueryError::InvalidBinding);
    };
    if binding.as_ref() != &facts.binding
        || facts.binding.owner_agent_deployment_id != facts.run_bindings.agent.deployment_id
    {
        return Err(ContextQueryError::InvalidBinding);
    }
    validate_dataset_view(&facts)?;
    command
        .request
        .validate_for(&facts.interface, &facts.binding, limits)?;
    if command.request.input.run_id != command.run_id {
        return Err(ContextQueryError::InvalidRequest);
    }
    let grant = DataAccessGrant::build(
        command.context_query_id.clone(),
        &facts.binding,
        &facts.principal,
        command.request.requested_projection.clone(),
        facts.interface.data_policy.maximum_classification,
        facts.policy_generation,
        deadline,
    )?;
    let mut policies = facts
        .run_bindings
        .policies
        .iter()
        .map(|binding| binding.revision.clone())
        .collect::<Vec<_>>();
    policies.extend([
        facts.binding.authorization_policy.clone(),
        facts.binding.ranking_policy.clone(),
        facts.context_closure.parser_policy.clone(),
        facts.context_closure.ranking_policy.clone(),
        facts.context_closure.data_policy.clone(),
    ]);
    policies.extend(facts.context_closure.network_policy.iter().cloned());
    policies.sort_by(|left, right| left.revision_id.cmp(&right.revision_id));
    policies.dedup_by(|left, right| left.revision_id == right.revision_id);
    let quota_ceiling = ContextQuotaCeiling {
        concurrent_units: 1,
        queries: 1,
        result_bytes: command
            .result_byte_ceiling
            .min(u64::from(facts.interface.limits.maximum_total_bytes)),
    };
    quota_ceiling.validate(limits)?;
    let mut admission = ContextAdmissionSnapshot {
        schema_version: 1,
        context_query_id: command.context_query_id.clone(),
        run_id: command.run_id.clone(),
        node_execution_id: command.node_execution_id.clone(),
        slot_id: command.slot_id.clone(),
        slot_binding_digest: slot.binding_digest.clone(),
        run_bindings_digest: facts.run_bindings.canonical_digest,
        binding: facts.binding,
        context_closure: facts.context_closure,
        interface_revision: facts.interface_revision,
        interface: facts.interface,
        implementation_revision: facts.implementation_revision,
        implementation: facts.implementation,
        dataset_view: facts.dataset_view,
        dataset_generation: facts.dataset_generation,
        request: command.request.clone(),
        principal: facts.principal,
        grant,
        policies,
        quota_ceiling,
        attempt_limit: command
            .requested_attempt_limit
            .min(limits.maximum_attempts()),
        deadline,
        canonical_digest: command.audit.request_digest.clone(),
    };
    admission.canonical_digest = digest_without_field(&admission, "canonical_digest")?;
    let record = ContextQueryRecord {
        tenant_id: command.audit.tenant_id.clone(),
        context_query_id: command.context_query_id.clone(),
        run_id: command.run_id.clone(),
        node_execution_id: command.node_execution_id.clone(),
        logical_key: context_query_logical_key(&command.slot_id, command.request.page_ordinal),
        deployment_id: admission.binding.context_deployment.deployment_id.clone(),
        input_value_id: admission.request.input.value_id.clone(),
        output_value_id: None,
        state: ContextQueryState::Ready,
        version: 1,
        payload: ContextQueryPayload {
            schema_version: 1,
            admission,
            current_job_id: None,
            result: None,
            failure: None,
        },
        deadline,
        retry_at: None,
        started_at: None,
        terminal_at: None,
        created_at: facts.database_now,
        updated_at: facts.database_now,
    };
    record.validate(limits)?;
    Ok(record)
}

fn validate_dataset_view(facts: &ContextAdmissionFacts) -> Result<(), ContextQueryError> {
    facts.dataset_view.validate()?;
    let generation = match &facts.dataset_view {
        ContextDatasetView::Generation { exact } => Some(exact),
        ContextDatasetView::ExternalObservation { .. } => None,
    };
    let valid = match (&facts.binding.consistency, generation) {
        (ContextConsistencyPolicy::PinnedGeneration { generation }, Some(actual)) => {
            generation == actual
        }
        (ContextConsistencyPolicy::PinAtRunAdmission { dataset_id }, Some(actual))
        | (ContextConsistencyPolicy::LatestAtQueryStart { dataset_id }, Some(actual)) => {
            dataset_id == &actual.dataset_id
        }
        (ContextConsistencyPolicy::ExternalObservation, None) => true,
        _ => false,
    };
    if !valid
        || facts.dataset_generation.is_some() != generation.is_some()
        || facts.dataset_generation.as_ref().is_some_and(|spec| {
            spec.validate().is_err() || spec.context_deployment != facts.binding.context_deployment
        })
    {
        return Err(ContextQueryError::InvalidDatasetView);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextJobBinding {
    pub schema_version: u32,
    pub context_query_id: ResourceId,
    pub admission_digest: Sha256Digest,
    pub context_deployment: insight_platform_contracts::ExactDeploymentRef,
    pub dataset_view: ContextDatasetView,
    pub grant_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub quota_ceiling: ContextQuotaCeiling,
    pub deadline: DateTime<Utc>,
}

impl ContextJobBinding {
    fn from_query(query: &ContextQueryRecord) -> Self {
        Self {
            schema_version: 1,
            context_query_id: query.context_query_id.clone(),
            admission_digest: query.payload.admission.canonical_digest.clone(),
            context_deployment: query.payload.admission.binding.context_deployment.clone(),
            dataset_view: query.payload.admission.dataset_view.clone(),
            grant_digest: query.payload.admission.grant.canonical_digest.clone(),
            request_digest: query
                .payload
                .admission
                .request
                .normalized_query_digest
                .clone(),
            quota_ceiling: query.payload.admission.quota_ceiling,
            deadline: query.deadline,
        }
    }

    fn validate_for(&self, query: &ContextQueryRecord) -> Result<(), ContextQueryError> {
        if self.schema_version != 1
            || self.context_query_id != query.context_query_id
            || self.admission_digest != query.payload.admission.canonical_digest
            || self.context_deployment != query.payload.admission.binding.context_deployment
            || self.dataset_view != query.payload.admission.dataset_view
            || self.grant_digest != query.payload.admission.grant.canonical_digest
            || self.request_digest != query.payload.admission.request.normalized_query_digest
            || self.quota_ceiling != query.payload.admission.quota_ceiling
            || self.deadline != query.deadline
        {
            return Err(ContextQueryError::InvalidJob);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextPhysicalOutcomeEvidence {
    Completed {
        observation_digest: Sha256Digest,
        output_value_id: ResourceId,
        result_bytes: u64,
    },
    Deferred {
        remote_state_digest: Sha256Digest,
    },
    RetryScheduled {
        failure_digest: Sha256Digest,
    },
    Failed {
        failure_digest: Sha256Digest,
    },
    Cancelled {
        observation_digest: Sha256Digest,
    },
    TimedOut {
        observation_digest: Sha256Digest,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextJobPayload {
    pub schema_version: u32,
    pub binding: ContextJobBinding,
    pub wake_contract: Option<WakeContract>,
    pub remote_state_digest: Option<Sha256Digest>,
    pub resume_same_attempt: bool,
    pub query_consumed: bool,
    pub physical_outcome: Option<ContextPhysicalOutcomeEvidence>,
}

impl ContextJobPayload {
    fn initial(binding: ContextJobBinding) -> Self {
        Self {
            schema_version: 1,
            binding,
            wake_contract: None,
            remote_state_digest: None,
            resume_same_attempt: false,
            query_consumed: false,
            physical_outcome: None,
        }
    }

    pub fn validate_for(
        &self,
        query: &ContextQueryRecord,
        job: &JobProjection,
        limits: ContextQueryLimits,
    ) -> Result<(), ContextQueryError> {
        self.binding.validate_for(query)?;
        query.validate(limits)?;
        if self.schema_version != 1
            || job.tenant_id != query.tenant_id
            || job.work_class != WorkClass::Context
            || job.owner.owner_kind != ResourceKind::ContextQuery
            || job.owner.owner_id != query.context_query_id
            || job.attempt_limit != query.payload.admission.attempt_limit
            || job.deadline != query.deadline
            || self.wake_contract != job.wake
            || (job.state == JobState::Waiting) != self.wake_contract.is_some()
            || self.resume_same_attempt != self.remote_state_digest.is_some()
            || (self.resume_same_attempt && !self.query_consumed)
        {
            return Err(ContextQueryError::InvalidJob);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PrepareContextDispatch {
    pub audit: CommandAudit,
    pub context_query_id: ResourceId,
    pub expected_query_version: u64,
    pub job_id: ResourceId,
    pub scheduled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedContextDispatch {
    pub query: ContextQueryRecord,
    pub job: JobProjection,
    pub job_payload: ContextJobPayload,
}

pub fn decide_prepare_context_dispatch(
    current: &ContextQueryRecord,
    command: &PrepareContextDispatch,
    database_now: DateTime<Utc>,
    limits: ContextQueryLimits,
) -> Result<PreparedContextDispatch, ContextQueryError> {
    current.validate(limits)?;
    command
        .audit
        .validate_at(database_now)
        .map_err(|_| ContextQueryError::InvalidIdentity)?;
    if command.context_query_id != current.context_query_id
        || command.expected_query_version != current.version
        || command.job_id.kind() != ResourceKind::Job
        || current.state != ContextQueryState::Ready
        || current.payload.current_job_id.is_some()
        || command.scheduled_at >= current.deadline
        || database_now >= current.deadline
    {
        return Err(ContextQueryError::InvalidTransition);
    }
    let mut query = current.clone();
    query.version = increment(query.version)?;
    query.payload.current_job_id = Some(command.job_id.clone());
    query.updated_at = database_now;
    let job = JobProjection {
        tenant_id: current.tenant_id.clone(),
        job_id: command.job_id.clone(),
        work_class: WorkClass::Context,
        owner: JobOwnerRef {
            owner_kind: ResourceKind::ContextQuery,
            owner_id: current.context_query_id.clone(),
        },
        state: JobState::Ready,
        version: 1,
        attempt_count: 0,
        attempt_limit: current.payload.admission.attempt_limit,
        lease_generation: 0,
        lease: None,
        scheduled_at: command.scheduled_at.max(database_now),
        retry_at: None,
        wake: None,
        deadline: current.deadline,
    };
    let job_payload = ContextJobPayload::initial(ContextJobBinding::from_query(&query));
    job_payload.validate_for(&query, &job, limits)?;
    query.validate(limits)?;
    Ok(PreparedContextDispatch {
        query,
        job,
        job_payload,
    })
}

#[derive(Debug, Clone)]
pub struct ContextLeaseIdentity {
    pub worker_process_generation_id: ResourceId,
    pub lease_token_digest: Sha256Digest,
}

#[derive(Debug, Clone)]
pub struct ContextClaimSlot {
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub lease_token_digest: Sha256Digest,
    pub quota_reservation_id: ResourceId,
    pub quota_entry_ids: [ResourceId; CONTEXT_QUOTA_LINES],
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
}

impl ContextClaimSlot {
    pub fn validate(&self) -> Result<(), ContextQueryError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.job_id.kind() != ResourceKind::Job
            || self.quota_reservation_id.kind() != ResourceKind::UsageReservation
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
        {
            return Err(ContextQueryError::InvalidIdentity);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ClaimContextJobs {
    pub worker_process_generation_id: ResourceId,
    pub slots: Vec<ContextClaimSlot>,
    pub lease_policy: LeasePolicy,
}

impl ClaimContextJobs {
    pub fn validate(&self) -> Result<(), ContextQueryError> {
        if self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.slots.is_empty()
            || self.slots.len() > 64
        {
            return Err(ContextQueryError::InvalidIdentity);
        }
        let mut jobs = std::collections::BTreeSet::new();
        let mut identities = std::collections::BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            if !jobs.insert(slot.job_id.to_string())
                || !std::iter::once(&slot.quota_reservation_id)
                    .chain(slot.quota_entry_ids.iter())
                    .chain([&slot.event_id, &slot.outbox_id])
                    .all(|id| identities.insert(id.to_string()))
            {
                return Err(ContextQueryError::InvalidIdentity);
            }
        }
        Ok(())
    }
}

pub fn decide_claim_context_dispatch(
    query: &ContextQueryRecord,
    job: &JobProjection,
    payload: &ContextJobPayload,
    worker: &ContextLeaseIdentity,
    database_now: DateTime<Utc>,
    lease_policy: LeasePolicy,
    limits: ContextQueryLimits,
) -> Result<(ContextQueryRecord, JobProjection, ContextJobPayload), ContextQueryError> {
    payload.validate_for(query, job, limits)?;
    if !((payload.resume_same_attempt && query.state == ContextQueryState::Deferred)
        || (!payload.resume_same_attempt
            && matches!(
                query.state,
                ContextQueryState::Ready | ContextQueryState::RetryScheduled
            )))
        || query.payload.current_job_id.as_ref() != Some(&job.job_id)
    {
        return Err(ContextQueryError::FirstWinnerLost);
    }
    let claim = if payload.resume_same_attempt {
        decide_claim_continuation
    } else {
        decide_claim
    };
    let claimed = claim(
        job,
        database_now,
        worker.worker_process_generation_id.clone(),
        worker.lease_token_digest.clone(),
        lease_policy,
    )
    .map_err(|_| ContextQueryError::FirstWinnerLost)?;
    Ok((query.clone(), claimed, payload.clone()))
}

pub fn decide_start_context_dispatch(
    query: &ContextQueryRecord,
    job: &JobProjection,
    payload: &ContextJobPayload,
    fence: &JobFence,
    database_now: DateTime<Utc>,
    limits: ContextQueryLimits,
) -> Result<(ContextQueryRecord, JobProjection, ContextJobPayload), ContextQueryError> {
    payload.validate_for(query, job, limits)?;
    let start = if payload.resume_same_attempt {
        decide_resume
    } else {
        decide_start
    };
    let started = start(job, fence, database_now).map_err(|_| ContextQueryError::StaleFence)?;
    let mut query = query.clone();
    query.state = ContextQueryState::InFlight;
    query.version = increment(query.version)?;
    query.retry_at = None;
    query.started_at.get_or_insert(database_now);
    query.updated_at = database_now;
    query.validate(limits)?;
    Ok((query, started, payload.clone()))
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContextObservationOutput {
    pub value_id: ResourceId,
    pub value_kind: String,
    pub classification: insight_platform_contracts::DataClassification,
    pub value: ValueRef,
    pub artifact_link_id: Option<ResourceId>,
    pub observation: ContextObservation,
    pub validation_evidence_digest: Sha256Digest,
}

impl ContextObservationOutput {
    fn exact_for(
        &self,
        query: &ContextQueryRecord,
        limits: ContextQueryLimits,
    ) -> Result<ExactInvocationValueRef, ContextQueryError> {
        self.observation
            .validate_for(&query.context_query_id, &query.payload.admission, limits)?;
        self.value
            .validate(limits.inline_value_limits())
            .map_err(|_| ContextQueryError::InvalidOutcome)?;
        if self.value_id.kind() != ResourceKind::RunValue
            || self.value_kind != "context_observation"
            || value_ref_digest(&self.value)? != self.observation.canonical_digest
            || self.artifact_link_id.is_some() != matches!(self.value, ValueRef::Artifact { .. })
            || self
                .artifact_link_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ArtifactLink)
        {
            return Err(ContextQueryError::InvalidOutcome);
        }
        let storage = match &self.value {
            ValueRef::Inline { .. } => InvocationValueStorage::Inline,
            ValueRef::Artifact { artifact } => InvocationValueStorage::Artifact {
                artifact: artifact.clone(),
            },
        };
        Ok(ExactInvocationValueRef {
            schema_version: 1,
            value_id: self.value_id.clone(),
            run_id: query.run_id.clone(),
            producing_node_id: Some(query.node_execution_id.clone()),
            value_kind: self.value_kind.clone(),
            classification: self.classification,
            schema_digest: query
                .payload
                .admission
                .interface
                .observation_schema_digest
                .clone(),
            content_digest: self.observation.canonical_digest.clone(),
            storage,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ContextBackendOutcome {
    Completed(Box<ContextObservationOutput>),
    Deferred {
        wake: WakeContract,
        remote_state_digest: Sha256Digest,
    },
    RetryableFailure {
        failure: Failure,
        retry_at: DateTime<Utc>,
    },
    PermanentFailure {
        failure: Failure,
    },
}

impl ContextBackendOutcome {
    pub const fn kind(&self) -> ContextBackendOutcomeKind {
        match self {
            Self::Completed(_) => ContextBackendOutcomeKind::Completed,
            Self::Deferred { .. } => ContextBackendOutcomeKind::Deferred,
            Self::RetryableFailure { .. } => ContextBackendOutcomeKind::RetryableFailure,
            Self::PermanentFailure { .. } => ContextBackendOutcomeKind::PermanentFailure,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContextOutcomeDecision {
    pub query: ContextQueryRecord,
    pub job: JobProjection,
    pub job_payload: ContextJobPayload,
    pub output: Option<ContextObservationOutput>,
    pub settled_result_bytes: u64,
    pub consumed_query: bool,
}

pub fn decide_context_outcome(
    current_query: &ContextQueryRecord,
    current_job: &JobProjection,
    current_payload: &ContextJobPayload,
    fence: &JobFence,
    outcome: &ContextBackendOutcome,
    database_now: DateTime<Utc>,
    limits: ContextQueryLimits,
) -> Result<ContextOutcomeDecision, ContextQueryError> {
    current_query.validate(limits)?;
    current_payload.validate_for(current_query, current_job, limits)?;
    if current_query.state != ContextQueryState::InFlight
        || current_query.payload.current_job_id.as_ref() != Some(&current_job.job_id)
        || current_job.state != JobState::Running
        || database_now >= current_query.deadline
    {
        return Err(ContextQueryError::FirstWinnerLost);
    }
    let mut query = current_query.clone();
    let mut job_payload = current_payload.clone();
    let consumed_query = !current_payload.query_consumed;
    let (job, output, result_bytes) = match outcome {
        ContextBackendOutcome::Completed(output) => {
            let exact = output.exact_for(&query, limits)?;
            let result_digest = digest(&serde_json::json!({
                "observation": output.observation,
                "output": exact,
                "validation_evidence_digest": output.validation_evidence_digest,
            }))?;
            let job = decide_terminal(current_job, fence, database_now, JobState::Succeeded)
                .map_err(|_| ContextQueryError::FirstWinnerLost)?;
            query.state = ContextQueryState::Succeeded;
            query.output_value_id = Some(output.value_id.clone());
            query.payload.result = Some(ContextQueryResult {
                schema_version: 1,
                observation: output.observation.clone(),
                output: exact,
                validation_evidence_digest: output.validation_evidence_digest.clone(),
                result_digest,
            });
            query.terminal_at = Some(database_now);
            job_payload.wake_contract = None;
            job_payload.remote_state_digest = None;
            job_payload.resume_same_attempt = false;
            job_payload.query_consumed = true;
            job_payload.physical_outcome = Some(ContextPhysicalOutcomeEvidence::Completed {
                observation_digest: output.observation.canonical_digest.clone(),
                output_value_id: output.value_id.clone(),
                result_bytes: output.observation.total_bytes,
            });
            (
                job,
                Some(output.as_ref().clone()),
                output.observation.total_bytes,
            )
        }
        ContextBackendOutcome::Deferred {
            wake,
            remote_state_digest,
        } => {
            let job = decide_wait(current_job, fence, database_now, wake.clone())
                .map_err(|_| ContextQueryError::FirstWinnerLost)?;
            query.state = ContextQueryState::Deferred;
            job_payload.wake_contract = Some(wake.clone());
            job_payload.remote_state_digest = Some(remote_state_digest.clone());
            job_payload.resume_same_attempt = true;
            job_payload.query_consumed = true;
            job_payload.physical_outcome = Some(ContextPhysicalOutcomeEvidence::Deferred {
                remote_state_digest: remote_state_digest.clone(),
            });
            (job, None, 0)
        }
        ContextBackendOutcome::RetryableFailure { failure, retry_at } => {
            failure
                .validate(1_024)
                .map_err(|_| ContextQueryError::InvalidOutcome)?;
            let job = decide_retry(current_job, fence, database_now, *retry_at)
                .map_err(|_| ContextQueryError::FirstWinnerLost)?;
            query.state = ContextQueryState::RetryScheduled;
            query.retry_at = Some(*retry_at);
            job_payload.wake_contract = None;
            job_payload.remote_state_digest = None;
            job_payload.resume_same_attempt = false;
            job_payload.query_consumed = false;
            job_payload.physical_outcome = Some(ContextPhysicalOutcomeEvidence::RetryScheduled {
                failure_digest: digest(failure)?,
            });
            (job, None, 0)
        }
        ContextBackendOutcome::PermanentFailure { failure } => {
            failure
                .validate(1_024)
                .map_err(|_| ContextQueryError::InvalidOutcome)?;
            let job = decide_terminal(current_job, fence, database_now, JobState::Failed)
                .map_err(|_| ContextQueryError::FirstWinnerLost)?;
            query.state = ContextQueryState::Failed;
            query.payload.failure = Some(failure.clone());
            query.terminal_at = Some(database_now);
            job_payload.wake_contract = None;
            job_payload.remote_state_digest = None;
            job_payload.resume_same_attempt = false;
            job_payload.query_consumed = true;
            job_payload.physical_outcome = Some(ContextPhysicalOutcomeEvidence::Failed {
                failure_digest: digest(failure)?,
            });
            (job, None, 0)
        }
    };
    query.version = increment(query.version)?;
    query.updated_at = database_now;
    query.validate(limits)?;
    job_payload.validate_for(&query, &job, limits)?;
    Ok(ContextOutcomeDecision {
        query,
        job,
        job_payload,
        output,
        settled_result_bytes: result_bytes,
        consumed_query,
    })
}

#[derive(Debug, Clone)]
pub struct WakeContextDispatch {
    pub audit: ContextSignalAudit,
    pub context_query_id: ResourceId,
    pub expected_query_version: u64,
    pub job_id: ResourceId,
    pub expected_wake_generation: u64,
    pub source: WakeSource,
}

#[derive(Debug, Clone)]
pub struct ContextSignalAudit {
    pub tenant_id: ResourceId,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl ContextSignalAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ContextQueryError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= now
        {
            return Err(ContextQueryError::InvalidIdentity);
        }
        Ok(())
    }
}

pub fn decide_wake_context_dispatch(
    current_query: &ContextQueryRecord,
    current_job: &JobProjection,
    current_payload: &ContextJobPayload,
    command: &WakeContextDispatch,
    database_now: DateTime<Utc>,
    limits: ContextQueryLimits,
) -> Result<(ContextQueryRecord, JobProjection, ContextJobPayload), ContextQueryError> {
    current_query.validate(limits)?;
    current_payload.validate_for(current_query, current_job, limits)?;
    command
        .audit
        .validate_at(database_now)
        .map_err(|_| ContextQueryError::InvalidIdentity)?;
    if current_query.context_query_id != command.context_query_id
        || current_query.version != command.expected_query_version
        || current_query.state != ContextQueryState::Deferred
        || current_job.job_id != command.job_id
        || !current_payload.resume_same_attempt
    {
        return Err(ContextQueryError::FirstWinnerLost);
    }
    let job = decide_wake(
        current_job,
        command.expected_wake_generation,
        command.source,
        database_now,
    )
    .map_err(|_| ContextQueryError::FirstWinnerLost)?;
    let mut payload = current_payload.clone();
    payload.wake_contract = None;
    payload.physical_outcome = None;
    payload.validate_for(current_query, &job, limits)?;
    Ok((current_query.clone(), job, payload))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextControlKind {
    Cancel,
    Timeout,
}

pub fn decide_context_control(
    current_query: &ContextQueryRecord,
    current_job: Option<&JobProjection>,
    kind: ContextControlKind,
    database_now: DateTime<Utc>,
    limits: ContextQueryLimits,
) -> Result<(ContextQueryRecord, Option<JobProjection>), ContextQueryError> {
    current_query.validate(limits)?;
    if is_terminal(current_query.state)
        || (kind == ContextControlKind::Timeout && database_now < current_query.deadline)
    {
        return Err(ContextQueryError::FirstWinnerLost);
    }
    let mut query = current_query.clone();
    query.state = match kind {
        ContextControlKind::Cancel => ContextQueryState::Cancelled,
        ContextControlKind::Timeout => ContextQueryState::TimedOut,
    };
    query.version = increment(query.version)?;
    query.terminal_at = Some(database_now);
    query.updated_at = database_now;
    let job = current_job
        .map(|job| {
            let target = match kind {
                ContextControlKind::Cancel => JobState::Cancelled,
                ContextControlKind::Timeout => JobState::TimedOut,
            };
            if job.state == JobState::Running {
                decide_owner_cancelling(job)
            } else {
                decide_owner_terminal(job, target)
            }
        })
        .transpose()
        .map_err(|_| ContextQueryError::FirstWinnerLost)?;
    query.validate(limits)?;
    Ok((query, job))
}

pub trait ContextQueryTransaction {
    type Error;
    type ExecutionRecord;

    async fn create_context_query(
        &mut self,
        command: CreateContextQuery,
    ) -> Result<CommandOutcome<ContextQueryRecord>, Self::Error>;

    async fn prepare_context_dispatch(
        &mut self,
        command: PrepareContextDispatch,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error>;

    async fn commit_context_outcome(
        &mut self,
        command: CommitContextOutcome,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error>;

    async fn commit(self) -> Result<(), Self::Error>;
    async fn rollback(self) -> Result<(), Self::Error>;
}

pub trait ContextQueryStore {
    type Error;
    type Transaction<'a>: ContextQueryTransaction<Error = Self::Error>
    where
        Self: 'a;

    async fn begin_context_query(&self) -> Result<Self::Transaction<'_>, Self::Error>;
}

#[derive(Debug, Clone)]
pub struct CommitContextOutcome {
    pub audit: ContextWorkerAudit,
    pub context_query_id: ResourceId,
    pub expected_query_version: u64,
    pub job_id: ResourceId,
    pub fence: JobFence,
    pub outcome: ContextBackendOutcome,
    pub quota_entry_ids: [ResourceId; CONTEXT_QUOTA_LINES],
}

impl CommitContextOutcome {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ContextQueryError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ContextQueryError::InvalidIdentity)?;
        if self.context_query_id.kind() != ResourceKind::ContextQuery
            || self.expected_query_version == 0
            || self.job_id.kind() != ResourceKind::Job
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self
                .quota_entry_ids
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != CONTEXT_QUOTA_LINES
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
        {
            return Err(ContextQueryError::InvalidIdentity);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ContextWorkerAudit {
    pub tenant_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl ContextWorkerAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ContextQueryError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= now
        {
            return Err(ContextQueryError::InvalidIdentity);
        }
        Ok(())
    }
}

pub fn context_query_logical_key(slot_id: &str, page_ordinal: u32) -> String {
    format!("context:{slot_id}:page:{page_ordinal}")
}

fn increment(value: u64) -> Result<u64, ContextQueryError> {
    value
        .checked_add(1)
        .ok_or(ContextQueryError::CounterOverflow)
}

const fn is_terminal(state: ContextQueryState) -> bool {
    matches!(
        state,
        ContextQueryState::Succeeded
            | ContextQueryState::Failed
            | ContextQueryState::Cancelled
            | ContextQueryState::TimedOut
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CitationLocator, ContextCitation, ContextItem, ContextRetrievalEvidence,
        NormalizedContextScore,
    };
    use chrono::Duration;
    use insight_platform_contracts::{
        canonical_digest, checked_in_hard_limit_profile, AgentDeploymentClosure, ArtifactRef,
        AuthoringPackage, ClosedJsonValue, ContextBackendBinding, ContextBackendContract,
        ContextBackendKind, ContextBackendLimits, ContextCitationContract, ContextCitationStrength,
        ContextConsistencyMode, ContextDataPolicyContract, ContextImplementationContract,
        ContextInterfaceLimits, ContextLocatorKind, ContextPaginationContract,
        ContextRankingContract, DataClassification, DataRegion, ExactDeploymentRef,
        ExactPolicyBinding, FrozenSlotBinding, PermissionSet, PrincipalKind,
    };
    use insight_platform_invocations::InvocationValueStorage;
    use insight_platform_jobs::{JobFence, WakeKind};
    use serde_json::json;

    struct Fixture {
        limits: ContextQueryLimits,
        command: CreateContextQuery,
        facts: ContextAdmissionFacts,
    }

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f5{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn policy_binding(revision: ExactVersionRef, suffix: u16) -> ExactPolicyBinding {
        ExactPolicyBinding {
            deployment: ExactDeploymentRef::new(
                id(ResourceKind::PolicyDeployment, suffix),
                named_digest(&format!("policy-deployment-{suffix}")),
            )
            .unwrap(),
            revision,
        }
    }

    fn named_digest(label: &str) -> Sha256Digest {
        canonical_digest(&json!({"context_state": label}))
            .unwrap()
            .parse()
            .unwrap()
    }

    fn version(kind: ResourceKind, suffix: u16) -> ExactVersionRef {
        ExactVersionRef::new(id(kind, suffix), named_digest(&format!("version-{suffix}"))).unwrap()
    }

    fn artifact(suffix: u16) -> ArtifactRef {
        ArtifactRef::new(
            id(ResourceKind::Artifact, suffix),
            named_digest(&format!("artifact-{suffix}")),
            16,
            "application/json",
            DataClassification::Internal,
            Some(format!("context-{suffix}.json")),
        )
        .unwrap()
    }

    fn authoring(suffix: u16) -> AuthoringPackage {
        AuthoringPackage {
            artifact: artifact(suffix),
            manifest_digest: named_digest(&format!("manifest-{suffix}")),
        }
    }

    fn audit(tenant_id: &ResourceId, principal_id: &ResourceId, base: u16) -> CommandAudit {
        CommandAudit {
            tenant_id: tenant_id.clone(),
            principal_id: principal_id.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            receipt_id: id(ResourceKind::Receipt, base),
            event_id: id(ResourceKind::Event, base + 1),
            outbox_id: id(ResourceKind::OutboxEvent, base + 2),
            idempotency_key_digest: named_digest(&format!("idempotency-{base}")),
            request_digest: named_digest(&format!("request-{base}")),
            receipt_expires_at: Utc::now() + Duration::hours(2),
        }
    }

    fn fixture() -> Fixture {
        let limits = ContextQueryLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let tenant_id = id(ResourceKind::Tenant, 1);
        let principal_id = id(ResourceKind::Principal, 2);
        let run_id = id(ResourceKind::Run, 3);
        let node_id = id(ResourceKind::NodeExecution, 4);
        let agent_deployment_id = id(ResourceKind::AgentDeployment, 5);
        let context_deployment = ExactDeploymentRef::new(
            id(ResourceKind::ContextDeployment, 6),
            named_digest("context-deployment"),
        )
        .unwrap();
        let authorization_policy = version(ResourceKind::PolicyRevision, 10);
        let ranking_policy = version(ResourceKind::PolicyRevision, 11);
        let parser_policy = version(ResourceKind::PolicyRevision, 12);
        let data_policy = version(ResourceKind::PolicyRevision, 13);
        let chunker_policy = version(ResourceKind::PolicyRevision, 17);
        let entitlement_policy = version(ResourceKind::PolicyRevision, 14);
        let cache_policy = version(ResourceKind::PolicyRevision, 15);
        let execution_profile = version(ResourceKind::PolicyRevision, 16);
        let interface_revision = version(ResourceKind::ContextSourceInterfaceRevision, 20);
        let implementation_revision =
            version(ResourceKind::ContextSourceImplementationRevision, 21);
        let query_schema_digest = named_digest("query-schema");
        let item_schema_digest = named_digest("item-schema");
        let score_domain_digest = named_digest("score-domain");
        let interface = ContextInterfaceResourceSpec {
            authoring_package: authoring(30),
            contract_digest: named_digest("interface-contract"),
            dependency_versions: vec![],
            policy_versions: vec![entitlement_policy.clone(), cache_policy.clone()],
            query_schema_digest: query_schema_digest.clone(),
            filter_schema_digest: named_digest("filter-schema"),
            item_schema_digest,
            observation_schema_digest: named_digest("observation-schema"),
            allowed_consistency: vec![ContextConsistencyMode::ExternalObservation],
            citation: ContextCitationContract {
                allowed_strengths: vec![ContextCitationStrength::ObservationOnly],
                locator_kinds: vec![ContextLocatorKind::RemoteOpaque],
                require_content_digest: true,
                maximum_display_label_bytes: 256,
            },
            pagination: ContextPaginationContract {
                maximum_page_size: 100,
                maximum_cursor_bytes: 1_024,
                cursor_ttl_milliseconds: 60_000,
            },
            ranking: ContextRankingContract {
                score_domain_digest: score_domain_digest.clone(),
                reranker_contract_digest: None,
                maximum_candidates: 1_000,
            },
            data_policy: ContextDataPolicyContract {
                maximum_classification: DataClassification::Confidential,
                allowed_regions: vec!["cn-east-1".parse::<DataRegion>().unwrap()],
                entitlement_policy,
                cache_policy,
                maximum_retention_milliseconds: 86_400_000,
            },
            limits: ContextInterfaceLimits {
                maximum_query_bytes: 4_096,
                maximum_filter_bytes: 4_096,
                maximum_item_bytes: 65_536,
                maximum_total_bytes: 1_048_576,
                maximum_items: 100,
                maximum_fan_out: 4,
            },
        };
        let implementation = ContextImplementationResourceSpec {
            authoring_package: authoring(31),
            contract_digest: named_digest("implementation-contract"),
            dependency_versions: vec![interface_revision.clone()],
            policy_versions: vec![],
            interface_revision: interface_revision.clone(),
            backend_kind: ContextBackendKind::SqlCatalog,
            contract: ContextImplementationContract {
                backend: ContextBackendContract::SqlCatalog {
                    dialect: "postgres".to_owned(),
                    catalog_projection_digest: named_digest("catalog-projection"),
                },
                credential_requirements: vec![],
                limits: ContextBackendLimits {
                    maximum_request_bytes: 65_536,
                    maximum_response_bytes: 1_048_576,
                    maximum_candidates: 1_000,
                    maximum_remote_state_bytes: 1_024,
                    maximum_poll_count: 4,
                    total_timeout_milliseconds: 30_000,
                },
            },
        };
        let context_closure = ContextDeploymentClosure {
            implementation: implementation_revision.clone(),
            interface: interface_revision.clone(),
            backend: ContextBackendBinding::SqlCatalog {
                database_identity_digest: named_digest("database"),
                dialect: "postgres".to_owned(),
                catalog_scope_digest: named_digest("catalog-scope"),
            },
            secret_bindings: vec![],
            network_policy: None,
            parser_policy: parser_policy.clone(),
            chunker_policy,
            embedding_model_deployment: None,
            ranking_policy: ranking_policy.clone(),
            data_policy: data_policy.clone(),
            conformance_evidence: artifact(32),
        };
        let binding = ContextBindingSnapshot::build(
            id(ResourceKind::ContextBinding, 33),
            agent_deployment_id.clone(),
            context_deployment.clone(),
            ContextConsistencyPolicy::ExternalObservation,
            vec!["customer_id".to_owned()],
            authorization_policy.clone(),
            ranking_policy.clone(),
        )
        .unwrap();
        let agent_closure = AgentDeploymentClosure {
            interface: version(ResourceKind::AgentInterfaceRevision, 40),
            plan: version(ResourceKind::AgentPlanRevision, 41),
            entry_node_id: "start".to_owned(),
            entry_node_kind: insight_platform_contracts::PlanNodeKind::Start,
            slots: vec![FrozenSlotBinding {
                slot_id: "catalog".to_owned(),
                requirement_digest: named_digest("slot-requirement"),
                target: FrozenSlotTarget::Context {
                    binding: Box::new(binding.clone()),
                },
                binding_digest: named_digest("slot-binding"),
            }],
            policies: vec![
                policy_binding(authorization_policy, 42),
                policy_binding(ranking_policy, 43),
                policy_binding(parser_policy, 44),
                policy_binding(data_policy, 45),
            ],
            execution_profile: policy_binding(execution_profile, 46),
        };
        let principal = PrincipalSnapshot::build(
            tenant_id.clone(),
            principal_id.clone(),
            PrincipalKind::AgentRunner,
            PermissionSet::new(vec![Permission::ContextQuery]).unwrap(),
            1,
            1,
            1,
        )
        .unwrap();
        let run_bindings = RunBindingsSnapshot::build(
            ExactDeploymentRef::new(agent_deployment_id, named_digest("agent-deployment")).unwrap(),
            principal.clone(),
            &agent_closure,
        )
        .unwrap();
        let input = ExactInvocationValueRef {
            schema_version: 1,
            value_id: id(ResourceKind::RunValue, 50),
            run_id: run_id.clone(),
            producing_node_id: None,
            value_kind: "context_query".to_owned(),
            classification: DataClassification::Internal,
            schema_digest: query_schema_digest,
            content_digest: named_digest("query-content"),
            storage: InvocationValueStorage::Inline,
        };
        let request = ContextQueryRequest {
            schema_version: 1,
            input,
            input_artifact_link_id: None,
            normalized_query_digest: named_digest("normalized-query"),
            normalized_filter_digest: named_digest("normalized-filter"),
            requested_projection: vec!["customer_id".to_owned()],
            query_bytes: 64,
            filter_bytes: 2,
            page_size: 20,
            page_ordinal: 0,
            cursor_digest: None,
        };
        let now = Utc::now();
        let command = CreateContextQuery {
            audit: audit(&tenant_id, &principal_id, 60),
            context_query_id: id(ResourceKind::ContextQuery, 63),
            run_id: run_id.clone(),
            node_execution_id: node_id,
            expected_run_version: 1,
            expected_node_version: 1,
            slot_id: "catalog".to_owned(),
            request,
            requested_attempt_limit: 3,
            result_byte_ceiling: 1_048_576,
        };
        let facts = ContextAdmissionFacts {
            run_state: RunState::Running,
            run_version: 1,
            run_pause_requested: false,
            run_cancel_requested: false,
            run_timeout_requested: false,
            run_deadline: now + Duration::hours(1),
            run_bindings,
            node_state: NodeExecutionState::Running,
            node_version: 1,
            node_kind: PlanNodeKind::ContextQuery,
            node_deadline: now + Duration::minutes(30),
            context_gate_enabled: true,
            binding,
            context_closure,
            interface_revision,
            interface,
            implementation_revision,
            implementation,
            dataset_view: ContextDatasetView::ExternalObservation {
                source_identity_digest: context_deployment.deployment_digest,
            },
            dataset_generation: None,
            principal,
            policy_generation: 1,
            database_now: now,
        };
        Fixture {
            limits,
            command,
            facts,
        }
    }

    fn fence(job: &JobProjection) -> JobFence {
        let lease = job.lease.as_ref().unwrap();
        JobFence {
            expected_version: job.version,
            worker_process_generation_id: lease.worker_process_generation_id.clone(),
            lease_generation: lease.lease_generation,
            token_digest: lease.token_digest.clone(),
        }
    }

    fn observation_output(
        query: &ContextQueryRecord,
        observed_at: DateTime<Utc>,
    ) -> ContextObservationOutput {
        let content = ValueRef::Inline {
            value: json!("authorized row"),
        };
        let content_digest = value_ref_digest(&content).unwrap();
        let item = ContextItem {
            item_id: id(ResourceKind::ContextItem, 90),
            source_item_identity_digest: named_digest("source-item"),
            content: content.clone(),
            structured_fields: ClosedJsonValue::build(
                named_digest("structured-schema"),
                json!({"customer_id": 42}),
            )
            .unwrap(),
            score: Some(NormalizedContextScore {
                millionths: 900_000,
                score_domain_digest: query
                    .payload
                    .admission
                    .interface
                    .ranking
                    .score_domain_digest
                    .clone(),
            }),
            classification: DataClassification::Internal,
            citation: ContextCitation {
                context_deployment: query.payload.admission.binding.context_deployment.clone(),
                interface_revision: query.payload.admission.interface_revision.clone(),
                dataset_view: query.payload.admission.dataset_view.clone(),
                locator: CitationLocator::RemoteOpaque {
                    locator_digest: named_digest("locator"),
                },
                strength: ContextCitationStrength::ObservationOnly,
                content_digest,
                observed_at,
                display_label: "authorized row".to_owned(),
            },
            authorization_evidence_digest: named_digest("authorization-evidence"),
        };
        let total_bytes = match &content {
            ValueRef::Inline { value } => {
                u64::try_from(serde_json::to_vec(value).unwrap().len()).unwrap()
            }
            ValueRef::Artifact { artifact } => artifact.byte_length(),
        };
        let mut observation = ContextObservation {
            schema_version: 1,
            observation_id: id(ResourceKind::ContextObservation, 91),
            context_query_id: query.context_query_id.clone(),
            dataset_view: query.payload.admission.dataset_view.clone(),
            normalized_query_digest: query
                .payload
                .admission
                .request
                .normalized_query_digest
                .clone(),
            items: vec![item],
            next_cursor_digest: None,
            evidence: ContextRetrievalEvidence {
                backend_request_digest: named_digest("backend-request"),
                backend_response_digest: named_digest("backend-response"),
                authorization_evidence_digest: named_digest("authorization-evidence"),
                ranking_evidence_digest: named_digest("ranking-evidence"),
                candidate_count: 1,
                rejected_count: 0,
                truncated: false,
            },
            observed_at,
            total_bytes,
            canonical_digest: named_digest("placeholder"),
        };
        observation.canonical_digest =
            digest_without_field(&observation, "canonical_digest").unwrap();
        let mut unsigned = serde_json::to_value(&observation).unwrap();
        unsigned.as_object_mut().unwrap().remove("canonical_digest");
        ContextObservationOutput {
            value_id: id(ResourceKind::RunValue, 92),
            value_kind: "context_observation".to_owned(),
            classification: DataClassification::Internal,
            value: ValueRef::Inline { value: unsigned },
            artifact_link_id: None,
            observation,
            validation_evidence_digest: named_digest("validation-evidence"),
        }
    }

    #[test]
    fn deferred_resume_preserves_attempt_and_commits_one_observation() {
        let fixture = fixture();
        let query = decide_context_query_admission(&fixture.command, fixture.facts, fixture.limits)
            .unwrap();
        assert_eq!(query.state, ContextQueryState::Ready);
        let prepared = decide_prepare_context_dispatch(
            &query,
            &PrepareContextDispatch {
                audit: audit(
                    &query.tenant_id,
                    &query.payload.admission.principal.principal_id,
                    70,
                ),
                context_query_id: query.context_query_id.clone(),
                expected_query_version: query.version,
                job_id: id(ResourceKind::Job, 73),
                scheduled_at: query.created_at,
            },
            query.created_at,
            fixture.limits,
        )
        .unwrap();
        let worker = ContextLeaseIdentity {
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 74),
            lease_token_digest: named_digest("lease-one"),
        };
        let (_, leased, payload) = decide_claim_context_dispatch(
            &prepared.query,
            &prepared.job,
            &prepared.job_payload,
            &worker,
            query.created_at,
            LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: fixture.limits.maximum_lease_milliseconds(),
            },
            fixture.limits,
        )
        .unwrap();
        let (running_query, running_job, running_payload) = decide_start_context_dispatch(
            &prepared.query,
            &leased,
            &payload,
            &fence(&leased),
            query.created_at,
            fixture.limits,
        )
        .unwrap();
        assert_eq!(running_job.attempt_count, 1);
        let wake = WakeContract {
            kind: WakeKind::RemoteInvocation,
            generation: running_job.lease_generation,
            accepted_sources: vec![WakeSource::Callback, WakeSource::Poll],
            expected_response_schema_digest: None,
            opaque_state_digest: Some(named_digest("remote-state")),
            next_poll_at: Some(query.created_at + Duration::seconds(1)),
            poll_count: 0,
            poll_limit: 4,
            callback_binding_digest: Some(named_digest("callback-binding")),
            deadline: running_query.deadline,
        };
        let deferred = decide_context_outcome(
            &running_query,
            &running_job,
            &running_payload,
            &fence(&running_job),
            &ContextBackendOutcome::Deferred {
                wake,
                remote_state_digest: named_digest("remote-state"),
            },
            query.created_at + Duration::milliseconds(10),
            fixture.limits,
        )
        .unwrap();
        assert!(deferred.consumed_query);
        assert_eq!(deferred.query.state, ContextQueryState::Deferred);
        assert_eq!(deferred.job.state, JobState::Waiting);

        let wake_time = query.created_at + Duration::seconds(1);
        let (woken_query, woken_job, woken_payload) = decide_wake_context_dispatch(
            &deferred.query,
            &deferred.job,
            &deferred.job_payload,
            &WakeContextDispatch {
                audit: ContextSignalAudit {
                    tenant_id: query.tenant_id.clone(),
                    receipt_id: id(ResourceKind::Receipt, 80),
                    event_id: id(ResourceKind::Event, 81),
                    outbox_id: id(ResourceKind::OutboxEvent, 82),
                    idempotency_key_digest: named_digest("wake-idempotency"),
                    request_digest: named_digest("wake-request"),
                    receipt_expires_at: Utc::now() + Duration::hours(2),
                },
                context_query_id: query.context_query_id.clone(),
                expected_query_version: deferred.query.version,
                job_id: deferred.job.job_id.clone(),
                expected_wake_generation: deferred.job.lease_generation,
                source: WakeSource::Poll,
            },
            wake_time,
            fixture.limits,
        )
        .unwrap();
        assert_eq!(woken_job.state, JobState::Ready);
        let resume_worker = ContextLeaseIdentity {
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 83),
            lease_token_digest: named_digest("lease-two"),
        };
        let (_, resumed_lease, resumed_payload) = decide_claim_context_dispatch(
            &woken_query,
            &woken_job,
            &woken_payload,
            &resume_worker,
            wake_time,
            LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: fixture.limits.maximum_lease_milliseconds(),
            },
            fixture.limits,
        )
        .unwrap();
        let (resumed_query, resumed_job, resumed_payload) = decide_start_context_dispatch(
            &woken_query,
            &resumed_lease,
            &resumed_payload,
            &fence(&resumed_lease),
            wake_time,
            fixture.limits,
        )
        .unwrap();
        assert_eq!(resumed_job.attempt_count, 1);
        let output = observation_output(&resumed_query, wake_time);
        let completed = decide_context_outcome(
            &resumed_query,
            &resumed_job,
            &resumed_payload,
            &fence(&resumed_job),
            &ContextBackendOutcome::Completed(Box::new(output)),
            wake_time + Duration::milliseconds(10),
            fixture.limits,
        )
        .unwrap();
        assert!(!completed.consumed_query);
        assert_eq!(completed.query.state, ContextQueryState::Succeeded);
        assert_eq!(completed.job.state, JobState::Succeeded);
        let result = completed.query.payload.result.as_ref().unwrap();
        assert_eq!(
            result.output.schema_digest,
            completed
                .query
                .payload
                .admission
                .interface
                .observation_schema_digest
        );
        assert_ne!(
            result.output.schema_digest,
            completed
                .query
                .payload
                .admission
                .interface
                .item_schema_digest
        );
        assert_eq!(result.observation.items.len(), 1);
    }

    #[test]
    fn citation_content_digest_and_stale_fence_fail_closed() {
        let fixture = fixture();
        let query = decide_context_query_admission(&fixture.command, fixture.facts, fixture.limits)
            .unwrap();
        let mut output = observation_output(&query, query.created_at);
        output.observation.items[0].citation.content_digest = named_digest("forged-content");
        assert_eq!(
            output.exact_for(&query, fixture.limits),
            Err(ContextQueryError::InvalidObservation)
        );
    }

    #[test]
    fn backend_binding_and_nested_item_artifact_fail_closed() {
        let mut mismatched = fixture();
        let ContextBackendBinding::SqlCatalog { dialect, .. } =
            &mut mismatched.facts.context_closure.backend
        else {
            panic!("fixture backend changed")
        };
        *dialect = "mysql".to_owned();
        assert_eq!(
            decide_context_query_admission(
                &mismatched.command,
                mismatched.facts,
                mismatched.limits,
            ),
            Err(ContextQueryError::InvalidBinding)
        );

        let valid = fixture();
        let query =
            decide_context_query_admission(&valid.command, valid.facts, valid.limits).unwrap();
        let mut output = observation_output(&query, query.created_at);
        output.observation.items[0].content = ValueRef::Artifact {
            artifact: artifact(99),
        };
        assert_eq!(
            output.exact_for(&query, valid.limits),
            Err(ContextQueryError::InvalidObservation)
        );
    }
}
