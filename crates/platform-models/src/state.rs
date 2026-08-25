use crate::{
    digest, is_code, AccountingQuality, CanonicalModelRequest, ModelObservation, ModelOutputValue,
    ModelRequestValue, ModelTurnError, ModelTurnLimits, ModelUsage, ModelUsageAmounts,
    MAX_MODEL_NAME_BYTES,
};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    CommandAudit, CommandOutcome, DecimalMoney, DeploymentClosure, ExactDeploymentRef,
    ExactPolicyBinding, ExactVersionRef, ExternalLeafFailureMutationIds,
    ExternalLeafResumeMutationIds, Failure, FailureClass, FailureCode, FailureSource,
    FrozenSlotTarget, ModelDeploymentClosure, ModelProfileResourceSpec,
    ModelProviderDeploymentClosure, ModelProviderResourceSpec, ModelTurnState, NodeExecutionState,
    Permission, PlanNodeKind, PlatformFailureCode, PrincipalSnapshot, ResourceDocument, ResourceId,
    ResourceKind, Retryability, RunBindingsSnapshot, RunState, Sha256Digest, WorkClass,
};
use insight_platform_invocations::{ExactInvocationValueRef, InvocationSelectionEvidence};
use insight_platform_jobs::{
    decide_claim, decide_owner_cancelling, decide_owner_terminal, decide_retry, decide_start,
    decide_terminal, JobFence, JobOwnerRef, JobProjection, LeasePolicy,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MODEL_QUOTA_LINES: usize = 4;
const MAX_MODEL_POLICIES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelQuotaCeiling {
    pub concurrent_units: u64,
    pub requests: u64,
    pub tokens: u64,
    pub cost_microunits: u64,
}

impl ModelQuotaCeiling {
    fn validate(&self, limits: ModelTurnLimits) -> Result<(), ModelTurnError> {
        if self.concurrent_units != 1
            || self.requests != 1
            || self.tokens == 0
            || self.tokens > limits.maximum_tokens_per_turn()
            || self.cost_microunits == 0
        {
            return Err(ModelTurnError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTurnAdmissionSnapshot {
    pub schema_version: u32,
    pub scope_instance_id: ResourceId,
    pub round_ordinal: u16,
    pub slot_id: String,
    pub slot_binding_digest: Sha256Digest,
    pub run_bindings_digest: Sha256Digest,
    pub selection_policy: ExactPolicyBinding,
    pub selection_evidence: InvocationSelectionEvidence,
    pub model_deployment: ExactDeploymentRef,
    pub model_closure: ModelDeploymentClosure,
    pub profile_revision: ExactVersionRef,
    pub profile: Box<ModelProfileResourceSpec>,
    pub provider_deployment: ExactDeploymentRef,
    pub provider_closure: ModelProviderDeploymentClosure,
    pub provider_revision: ExactVersionRef,
    pub provider: ModelProviderResourceSpec,
    pub request: ExactInvocationValueRef,
    pub request_digest: Sha256Digest,
    pub policies: Vec<ExactVersionRef>,
    pub principal: PrincipalSnapshot,
    pub quota_ceiling: ModelQuotaCeiling,
    pub attempt_limit: u32,
    pub deadline: DateTime<Utc>,
    pub canonical_digest: Sha256Digest,
}

impl ModelTurnAdmissionSnapshot {
    pub fn validate(&self, limits: ModelTurnLimits) -> Result<(), ModelTurnError> {
        self.selection_evidence
            .validate()
            .map_err(|_| ModelTurnError::InvalidSelection)?;
        self.selection_policy
            .validate()
            .map_err(|_| ModelTurnError::InvalidSelection)?;
        self.model_deployment
            .validate()
            .map_err(|_| ModelTurnError::InvalidDeployment)?;
        self.provider_deployment
            .validate()
            .map_err(|_| ModelTurnError::InvalidDeployment)?;
        DeploymentClosure::ModelProfile(self.model_closure.clone())
            .validate()
            .map_err(|_| ModelTurnError::InvalidDeployment)?;
        DeploymentClosure::ModelProvider(self.provider_closure.clone())
            .validate()
            .map_err(|_| ModelTurnError::InvalidDeployment)?;
        ResourceDocument::ModelProfile(self.profile.clone())
            .validate()
            .map_err(|_| ModelTurnError::InvalidProfile)?;
        ResourceDocument::ModelProvider(self.provider.clone())
            .validate()
            .map_err(|_| ModelTurnError::InvalidProvider)?;
        self.request
            .validate()
            .map_err(|_| ModelTurnError::InvalidRequestValue)?;
        self.principal
            .validate()
            .map_err(|_| ModelTurnError::InvalidPrincipal)?;
        self.quota_ceiling.validate(limits)?;
        validate_model_policies(&self.policies)?;
        if self.schema_version != 1
            || self.scope_instance_id.kind() != ResourceKind::ScopeInstance
            || self.round_ordinal == 0
            || !is_code(&self.slot_id, MAX_MODEL_NAME_BYTES)
            || self.model_deployment.resource_kind != ResourceKind::ModelDeployment
            || self.provider_deployment.resource_kind != ResourceKind::ModelProviderDeployment
            || self.profile_revision.resource_kind != ResourceKind::ModelProfileRevision
            || self.provider_revision.resource_kind != ResourceKind::ModelProviderRevision
            || self.model_closure.profile_revision != self.profile_revision
            || self.model_closure.provider_deployment != self.provider_deployment
            || self.provider_closure.provider_revision != self.provider_revision
            || self.profile.provider_revision != self.provider_revision
            || self.provider.protocol_policy != self.provider_closure.protocol_policy
            || !self.policies.contains(&self.selection_policy.revision)
            || self.model_closure.generation_defaults.schema_digest
                != self.profile.parameter_schema_digest
            || !matches!(
                self.request.storage,
                insight_platform_invocations::InvocationValueStorage::Inline
            )
            || self.attempt_limit == 0
            || self.attempt_limit > limits.maximum_attempts()
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(ModelTurnError::InvalidDeployment);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelAttemptOutcomeKind {
    Succeeded,
    RetryScheduled,
    Failed,
    Cancelled,
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelAttemptAccounting {
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub usage_reservation_id: ResourceId,
    pub outcome: ModelAttemptOutcomeKind,
    pub usage: ModelUsageAmounts,
    pub accounting_quality: Option<AccountingQuality>,
    pub request_sent: bool,
    pub observation_digest: Sha256Digest,
    pub settled_at: DateTime<Utc>,
}

impl ModelAttemptAccounting {
    fn validate(&self, ceiling: ModelQuotaCeiling) -> Result<(), ModelTurnError> {
        if self.attempt_no == 0
            || self.lease_generation == 0
            || self.usage_reservation_id.kind() != ResourceKind::UsageReservation
            || self.usage.requests > ceiling.requests
            || self.usage.tokens > ceiling.tokens
            || self.usage.cost_microunits > ceiling.cost_microunits
            || self.request_sent != (self.usage.requests == 1)
            || self.request_sent != self.accounting_quality.is_some()
        {
            return Err(ModelTurnError::InvalidUsage);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTurnResult {
    pub schema_version: u32,
    pub output: ExactInvocationValueRef,
    pub response_digest: Sha256Digest,
    pub finish_reason: crate::CanonicalFinishReason,
    pub tool_intent_count: u32,
    pub validation_evidence_digest: Sha256Digest,
    pub result_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTurnPayload {
    pub schema_version: u32,
    pub admission: ModelTurnAdmissionSnapshot,
    pub current_job_id: Option<ResourceId>,
    pub attempts: Vec<ModelAttemptAccounting>,
    pub result: Option<ModelTurnResult>,
    pub failure: Option<Failure>,
}

impl ModelTurnPayload {
    fn validate_for(
        &self,
        state: ModelTurnState,
        limits: ModelTurnLimits,
    ) -> Result<(), ModelTurnError> {
        self.admission.validate(limits)?;
        if self.schema_version != 1
            || self
                .current_job_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::Job)
            || self.attempts.len()
                > usize::try_from(self.admission.attempt_limit)
                    .map_err(|_| ModelTurnError::InvalidLimits)?
            || self
                .failure
                .as_ref()
                .is_some_and(|failure| failure.validate(1_024).is_err())
        {
            return Err(ModelTurnError::InvalidCommand);
        }
        let mut reservations = BTreeSet::new();
        for (index, attempt) in self.attempts.iter().enumerate() {
            attempt.validate(self.admission.quota_ceiling)?;
            if attempt.attempt_no
                != u32::try_from(index + 1).map_err(|_| ModelTurnError::InvalidLimits)?
                || !reservations.insert(attempt.usage_reservation_id.clone())
            {
                return Err(ModelTurnError::InvalidUsage);
            }
        }
        if let Some(result) = &self.result {
            result
                .output
                .validate()
                .map_err(|_| ModelTurnError::InvalidOutputValue)?;
            if result.schema_version != 1
                || result.output.run_id != self.admission.request.run_id
                || result.result_digest
                    != digest(&serde_json::json!({
                        "finish_reason": result.finish_reason,
                        "output": result.output,
                        "response_digest": result.response_digest,
                        "tool_intent_count": result.tool_intent_count,
                        "validation_evidence_digest": result.validation_evidence_digest,
                    }))?
            {
                return Err(ModelTurnError::InvalidOutputValue);
            }
        }
        let valid = match state {
            ModelTurnState::Created | ModelTurnState::AwaitingBudget => {
                self.current_job_id.is_none() && self.result.is_none() && self.failure.is_none()
            }
            ModelTurnState::Ready | ModelTurnState::RetryScheduled => {
                self.result.is_none() && self.failure.is_none()
            }
            ModelTurnState::InFlight | ModelTurnState::Cancelling => {
                self.current_job_id.is_some() && self.result.is_none() && self.failure.is_none()
            }
            ModelTurnState::Succeeded => self.result.is_some() && self.failure.is_none(),
            ModelTurnState::Failed => self.result.is_none() && self.failure.is_some(),
            ModelTurnState::Cancelled | ModelTurnState::TimedOut => self.result.is_none(),
        };
        if !valid {
            return Err(ModelTurnError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelTurnRecord {
    pub tenant_id: ResourceId,
    pub model_turn_id: ResourceId,
    pub run_id: ResourceId,
    pub node_execution_id: ResourceId,
    pub scope_instance_id: ResourceId,
    pub round_ordinal: u16,
    pub logical_key: String,
    pub deployment_id: ResourceId,
    pub request_value_id: ResourceId,
    pub output_value_id: Option<ResourceId>,
    pub state: ModelTurnState,
    pub version: u64,
    pub payload: ModelTurnPayload,
    pub deadline: DateTime<Utc>,
    pub retry_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub terminal_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ModelTurnRecord {
    pub fn validate(&self, limits: ModelTurnLimits) -> Result<(), ModelTurnError> {
        self.payload.validate_for(self.state, limits)?;
        let terminal = is_terminal(self.state);
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.model_turn_id.kind() != ResourceKind::ModelTurn
            || self.run_id.kind() != ResourceKind::Run
            || self.node_execution_id.kind() != ResourceKind::NodeExecution
            || self.scope_instance_id != self.payload.admission.scope_instance_id
            || self.round_ordinal != self.payload.admission.round_ordinal
            || self.logical_key != model_turn_logical_key(self.round_ordinal)
            || self.deployment_id != self.payload.admission.model_deployment.deployment_id
            || self.request_value_id != self.payload.admission.request.value_id
            || self.output_value_id
                != self
                    .payload
                    .result
                    .as_ref()
                    .map(|result| result.output.value_id.clone())
            || self.deadline != self.payload.admission.deadline
            || self.version == 0
            || (self.state == ModelTurnState::RetryScheduled) != self.retry_at.is_some()
            || terminal != self.terminal_at.is_some()
            || self.updated_at < self.created_at
            || self.deadline < self.created_at
        {
            return Err(ModelTurnError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CreateModelTurn {
    pub audit: CommandAudit,
    pub model_turn_id: ResourceId,
    pub run_id: ResourceId,
    pub node_execution_id: ResourceId,
    pub scope_instance_id: ResourceId,
    pub expected_run_version: u64,
    pub expected_node_version: u64,
    pub round_ordinal: u16,
    pub slot_id: String,
    pub selected_candidate_ordinal: u16,
    pub selector_input_digest: Sha256Digest,
    pub request: ModelRequestValue,
    pub requested_attempt_limit: u32,
    pub cost_ceiling_microunits: u64,
}

impl CreateModelTurn {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: ModelTurnLimits,
    ) -> Result<(), ModelTurnError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ModelTurnError::InvalidAudit)?;
        if self.model_turn_id.kind() != ResourceKind::ModelTurn
            || self.run_id.kind() != ResourceKind::Run
            || self.node_execution_id.kind() != ResourceKind::NodeExecution
            || self.scope_instance_id.kind() != ResourceKind::ScopeInstance
            || self.expected_run_version == 0
            || self.expected_node_version == 0
            || self.round_ordinal == 0
            || !is_code(&self.slot_id, MAX_MODEL_NAME_BYTES)
            || self.requested_attempt_limit == 0
            || self.requested_attempt_limit > limits.maximum_attempts()
            || self.cost_ceiling_microunits == 0
        {
            return Err(ModelTurnError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ModelAdmissionFacts {
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
    pub node_scope_instance_id: ResourceId,
    pub node_deadline: DateTime<Utc>,
    pub model_deployment: ExactDeploymentRef,
    pub model_closure: ModelDeploymentClosure,
    pub model_gate_enabled: bool,
    pub profile_revision: ExactVersionRef,
    pub profile: ModelProfileResourceSpec,
    pub provider_deployment: ExactDeploymentRef,
    pub provider_closure: ModelProviderDeploymentClosure,
    pub provider_gate_enabled: bool,
    pub provider_revision: ExactVersionRef,
    pub provider: ModelProviderResourceSpec,
    pub principal: PrincipalSnapshot,
    pub database_now: DateTime<Utc>,
}

pub fn decide_model_turn_admission(
    command: &CreateModelTurn,
    facts: ModelAdmissionFacts,
    limits: ModelTurnLimits,
) -> Result<ModelTurnRecord, ModelTurnError> {
    command.validate_at(facts.database_now, limits)?;
    facts
        .run_bindings
        .validate()
        .map_err(|_| ModelTurnError::InvalidRun)?;
    facts
        .principal
        .validate()
        .map_err(|_| ModelTurnError::InvalidPrincipal)?;
    DeploymentClosure::ModelProfile(facts.model_closure.clone())
        .validate()
        .map_err(|_| ModelTurnError::InvalidDeployment)?;
    DeploymentClosure::ModelProvider(facts.provider_closure.clone())
        .validate()
        .map_err(|_| ModelTurnError::InvalidDeployment)?;
    let deadline = facts.run_deadline.min(facts.node_deadline);
    if facts.run_state != RunState::Running
        || facts.run_version != command.expected_run_version
        || facts.run_pause_requested
        || facts.run_cancel_requested
        || facts.run_timeout_requested
        || facts.node_state != NodeExecutionState::Running
        || facts.node_version != command.expected_node_version
        || facts.node_kind != PlanNodeKind::ModelLoop
        || facts.node_scope_instance_id != command.scope_instance_id
        || facts.database_now >= deadline
        || !facts.model_gate_enabled
        || !facts.provider_gate_enabled
        || facts.principal.tenant_id != command.audit.tenant_id
        || facts.principal.principal_id != command.audit.principal_id
        || facts.principal.principal_kind != command.audit.principal_kind
        || !facts
            .principal
            .permissions
            .contains(Permission::ModelInvoke)
        || facts.run_bindings.principal.principal_id != command.audit.principal_id
    {
        return Err(ModelTurnError::AdmissionRejected);
    }
    let slot = facts
        .run_bindings
        .slots
        .iter()
        .find(|slot| slot.slot_id == command.slot_id)
        .ok_or(ModelTurnError::InvalidSelection)?;
    let FrozenSlotTarget::Model {
        candidates,
        selection_policy,
    } = &slot.target
    else {
        return Err(ModelTurnError::InvalidSelection);
    };
    let selected = candidates
        .get(usize::from(command.selected_candidate_ordinal))
        .ok_or(ModelTurnError::InvalidSelection)?;
    if selected != &facts.model_deployment
        || facts.model_deployment.resource_kind != ResourceKind::ModelDeployment
        || facts.model_closure.profile_revision != facts.profile_revision
        || facts.model_closure.provider_deployment != facts.provider_deployment
        || facts.provider_closure.provider_revision != facts.provider_revision
        || facts.profile.provider_revision != facts.provider_revision
        || facts.provider.protocol_policy != facts.provider_closure.protocol_policy
        || facts.model_closure.generation_defaults.schema_digest
            != facts.profile.parameter_schema_digest
    {
        return Err(ModelTurnError::InvalidDeployment);
    }
    command.request.request.validate_for(
        &command.model_turn_id,
        &facts.provider,
        &facts.profile,
        &facts.provider_closure.region,
        facts.database_now,
        limits,
    )?;
    if command.request.request.deadline != deadline {
        return Err(ModelTurnError::InvalidRequest);
    }
    let request = command
        .request
        .exact_for(&command.run_id, &command.node_execution_id, limits)?;
    let request_digest = command.request.request.canonical_digest()?;
    if request.content_digest != request_digest {
        return Err(ModelTurnError::InvalidRequestValue);
    }
    let selection_evidence = InvocationSelectionEvidence::build(
        candidates,
        command.selected_candidate_ordinal,
        command.selector_input_digest.clone(),
    )
    .map_err(|_| ModelTurnError::InvalidSelection)?;
    selection_evidence
        .validate_for(candidates, selected)
        .map_err(|_| ModelTurnError::InvalidSelection)?;
    let policies = exact_model_policies(
        &facts.run_bindings.policies,
        selection_policy,
        &command.request.request.truncation_policy,
        &facts.model_closure,
        &facts.provider_closure,
    )?;
    let quota_ceiling = ModelQuotaCeiling {
        concurrent_units: 1,
        requests: 1,
        tokens: command
            .request
            .request
            .input_token_estimate
            .checked_add(u64::from(command.request.request.max_output_tokens))
            .ok_or(ModelTurnError::InvalidLimits)?,
        cost_microunits: command.cost_ceiling_microunits,
    };
    quota_ceiling.validate(limits)?;
    let mut admission = ModelTurnAdmissionSnapshot {
        schema_version: 1,
        scope_instance_id: command.scope_instance_id.clone(),
        round_ordinal: command.round_ordinal,
        slot_id: command.slot_id.clone(),
        slot_binding_digest: slot.binding_digest.clone(),
        run_bindings_digest: facts.run_bindings.canonical_digest,
        selection_policy: selection_policy.clone(),
        selection_evidence,
        model_deployment: facts.model_deployment,
        model_closure: facts.model_closure,
        profile_revision: facts.profile_revision,
        profile: Box::new(facts.profile),
        provider_deployment: facts.provider_deployment,
        provider_closure: facts.provider_closure,
        provider_revision: facts.provider_revision,
        provider: facts.provider,
        request,
        request_digest,
        policies,
        principal: facts.principal,
        quota_ceiling,
        attempt_limit: command
            .requested_attempt_limit
            .min(limits.maximum_attempts()),
        deadline,
        canonical_digest: command.selector_input_digest.clone(),
    };
    admission.canonical_digest = digest_without_field(&admission, "canonical_digest")?;
    let record = ModelTurnRecord {
        tenant_id: command.audit.tenant_id.clone(),
        model_turn_id: command.model_turn_id.clone(),
        run_id: command.run_id.clone(),
        node_execution_id: command.node_execution_id.clone(),
        scope_instance_id: command.scope_instance_id.clone(),
        round_ordinal: command.round_ordinal,
        logical_key: model_turn_logical_key(command.round_ordinal),
        deployment_id: admission.model_deployment.deployment_id.clone(),
        request_value_id: admission.request.value_id.clone(),
        output_value_id: None,
        state: ModelTurnState::Ready,
        version: 1,
        payload: ModelTurnPayload {
            schema_version: 1,
            admission,
            current_job_id: None,
            attempts: Vec::new(),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelJobBinding {
    pub schema_version: u32,
    pub model_turn_id: ResourceId,
    pub admission_digest: Sha256Digest,
    pub model_deployment: ExactDeploymentRef,
    pub provider_deployment: ExactDeploymentRef,
    pub request: ExactInvocationValueRef,
    pub request_digest: Sha256Digest,
    pub quota_ceiling: ModelQuotaCeiling,
    pub deadline: DateTime<Utc>,
}

impl ModelJobBinding {
    fn from_turn(turn: &ModelTurnRecord) -> Self {
        Self {
            schema_version: 1,
            model_turn_id: turn.model_turn_id.clone(),
            admission_digest: turn.payload.admission.canonical_digest.clone(),
            model_deployment: turn.payload.admission.model_deployment.clone(),
            provider_deployment: turn.payload.admission.provider_deployment.clone(),
            request: turn.payload.admission.request.clone(),
            request_digest: turn.payload.admission.request_digest.clone(),
            quota_ceiling: turn.payload.admission.quota_ceiling,
            deadline: turn.deadline,
        }
    }

    fn validate_for(&self, turn: &ModelTurnRecord) -> Result<(), ModelTurnError> {
        if self.schema_version != 1
            || self.model_turn_id != turn.model_turn_id
            || self.admission_digest != turn.payload.admission.canonical_digest
            || self.model_deployment != turn.payload.admission.model_deployment
            || self.provider_deployment != turn.payload.admission.provider_deployment
            || self.request != turn.payload.admission.request
            || self.request_digest != turn.payload.admission.request_digest
            || self.quota_ceiling != turn.payload.admission.quota_ceiling
            || self.deadline != turn.deadline
        {
            return Err(ModelTurnError::InvalidJob);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelPhysicalOutcomeEvidence {
    Succeeded {
        response_digest: Sha256Digest,
        output_value_id: ResourceId,
        tool_intent_count: u32,
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
pub struct ModelJobPayload {
    pub schema_version: u32,
    pub binding: ModelJobBinding,
    pub active_usage_reservation_id: Option<ResourceId>,
    pub physical_outcome: Option<ModelPhysicalOutcomeEvidence>,
}

impl ModelJobPayload {
    fn initial(binding: ModelJobBinding) -> Self {
        Self {
            schema_version: 1,
            binding,
            active_usage_reservation_id: None,
            physical_outcome: None,
        }
    }

    pub fn validate_for(
        &self,
        turn: &ModelTurnRecord,
        job: &JobProjection,
        limits: ModelTurnLimits,
    ) -> Result<(), ModelTurnError> {
        self.binding.validate_for(turn)?;
        let active = matches!(
            job.state,
            insight_platform_contracts::JobState::Leased
                | insight_platform_contracts::JobState::Running
                | insight_platform_contracts::JobState::Cancelling
        );
        if self.schema_version != 1
            || job.tenant_id != turn.tenant_id
            || job.work_class != WorkClass::Model
            || job.owner.owner_kind != ResourceKind::ModelTurn
            || job.owner.owner_id != turn.model_turn_id
            || job.attempt_limit != turn.payload.admission.attempt_limit
            || job.deadline != turn.deadline
            || active != self.active_usage_reservation_id.is_some()
            || self
                .active_usage_reservation_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::UsageReservation)
        {
            return Err(ModelTurnError::InvalidJob);
        }
        turn.validate(limits)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PrepareModelDispatch {
    pub audit: CommandAudit,
    pub model_turn_id: ResourceId,
    pub expected_turn_version: u64,
    pub job_id: ResourceId,
    pub scheduled_at: DateTime<Utc>,
}

impl PrepareModelDispatch {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ModelTurnError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ModelTurnError::InvalidAudit)?;
        if self.model_turn_id.kind() != ResourceKind::ModelTurn
            || self.expected_turn_version == 0
            || self.job_id.kind() != ResourceKind::Job
        {
            return Err(ModelTurnError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedModelDispatch {
    pub turn: ModelTurnRecord,
    pub job: JobProjection,
    pub job_payload: ModelJobPayload,
}

pub fn decide_prepare_model_dispatch(
    current: &ModelTurnRecord,
    command: &PrepareModelDispatch,
    database_now: DateTime<Utc>,
    limits: ModelTurnLimits,
) -> Result<PreparedModelDispatch, ModelTurnError> {
    current.validate(limits)?;
    command.validate_at(database_now)?;
    if current.tenant_id != command.audit.tenant_id
        || current.payload.admission.principal.principal_id != command.audit.principal_id
        || current.model_turn_id != command.model_turn_id
        || current.version != command.expected_turn_version
        || current.state != ModelTurnState::Ready
        || current.payload.current_job_id.is_some()
        || command.scheduled_at >= current.deadline
        || database_now >= current.deadline
    {
        return Err(ModelTurnError::FirstWinnerLost);
    }
    let job = JobProjection {
        tenant_id: current.tenant_id.clone(),
        job_id: command.job_id.clone(),
        work_class: WorkClass::Model,
        owner: JobOwnerRef {
            owner_id: current.model_turn_id.clone(),
            owner_kind: ResourceKind::ModelTurn,
        },
        state: insight_platform_contracts::JobState::Ready,
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
    job.validate().map_err(|_| ModelTurnError::InvalidJob)?;
    let job_payload = ModelJobPayload::initial(ModelJobBinding::from_turn(current));
    let mut turn = current.clone();
    turn.version = increment(turn.version)?;
    turn.payload.current_job_id = Some(command.job_id.clone());
    turn.updated_at = database_now;
    turn.validate(limits)?;
    job_payload.validate_for(&turn, &job, limits)?;
    Ok(PreparedModelDispatch {
        turn,
        job,
        job_payload,
    })
}

#[derive(Debug, Clone)]
pub struct ModelClaimSlot {
    pub lease_token_digest: Sha256Digest,
    pub usage_reservation_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub resume_mutations: Option<ExternalLeafResumeMutationIds>,
    pub failure_mutations: Option<ExternalLeafFailureMutationIds>,
}

impl ModelClaimSlot {
    pub fn validate(&self) -> Result<(), ModelTurnError> {
        validate_quota_entry_ids(&self.quota_entry_ids)?;
        if self.usage_reservation_id.kind() != ResourceKind::UsageReservation
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.resume_mutations.is_some() != self.failure_mutations.is_some()
            || self
                .resume_mutations
                .as_ref()
                .is_some_and(|mutations| mutations.validate().is_err())
            || self
                .failure_mutations
                .as_ref()
                .is_some_and(|mutations| mutations.validate().is_err())
        {
            return Err(ModelTurnError::InvalidIdentity);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ClaimModelJobs {
    pub worker_process_generation_id: ResourceId,
    pub limit: u16,
    pub lease_milliseconds: u64,
    pub slots: Vec<ModelClaimSlot>,
}

impl ClaimModelJobs {
    pub fn validate(&self, limits: ModelTurnLimits) -> Result<(), ModelTurnError> {
        if self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.limit == 0
            || self.slots.len() != usize::from(self.limit)
            || self.lease_milliseconds == 0
            || self.lease_milliseconds > limits.maximum_lease_milliseconds()
        {
            return Err(ModelTurnError::InvalidCommand);
        }
        for slot in &self.slots {
            slot.validate()?;
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub fn decide_start_model_dispatch(
    current_turn: &ModelTurnRecord,
    current_job: &JobProjection,
    current_payload: &ModelJobPayload,
    worker_process_generation_id: ResourceId,
    lease_token_digest: Sha256Digest,
    usage_reservation_id: ResourceId,
    lease_policy: LeasePolicy,
    database_now: DateTime<Utc>,
    limits: ModelTurnLimits,
) -> Result<PreparedModelDispatch, ModelTurnError> {
    current_turn.validate(limits)?;
    current_payload.validate_for(current_turn, current_job, limits)?;
    if current_turn.payload.current_job_id.as_ref() != Some(&current_job.job_id)
        || !matches!(
            current_turn.state,
            ModelTurnState::Ready | ModelTurnState::RetryScheduled
        )
        || usage_reservation_id.kind() != ResourceKind::UsageReservation
        || database_now >= current_turn.deadline
    {
        return Err(ModelTurnError::InvalidJob);
    }
    let claimed = decide_claim(
        current_job,
        database_now,
        worker_process_generation_id.clone(),
        lease_token_digest.clone(),
        lease_policy,
    )
    .map_err(|_| ModelTurnError::FirstWinnerLost)?;
    let fence = JobFence {
        expected_version: claimed.version,
        worker_process_generation_id,
        lease_generation: claimed.lease_generation,
        token_digest: lease_token_digest,
    };
    let job =
        decide_start(&claimed, &fence, database_now).map_err(|_| ModelTurnError::InvalidJob)?;
    let mut turn = current_turn.clone();
    turn.version = increment(turn.version)?;
    turn.state = ModelTurnState::InFlight;
    turn.retry_at = None;
    turn.started_at.get_or_insert(database_now);
    turn.updated_at = database_now;
    let mut job_payload = current_payload.clone();
    job_payload.active_usage_reservation_id = Some(usage_reservation_id);
    job_payload.physical_outcome = None;
    turn.validate(limits)?;
    job_payload.validate_for(&turn, &job, limits)?;
    Ok(PreparedModelDispatch {
        turn,
        job,
        job_payload,
    })
}

#[derive(Debug, Clone)]
pub struct ModelWorkerAudit {
    pub tenant_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl ModelWorkerAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ModelTurnError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= now
        {
            return Err(ModelTurnError::InvalidAudit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelAttemptMeasurement {
    pub usage: Option<ModelUsage>,
    pub observation: ModelObservation,
}

impl ModelAttemptMeasurement {
    /// Produces the conservative terminal accounting used after a dispatched request can no
    /// longer return trustworthy Provider usage (for example, a durable cancellation winner).
    /// The exact admission ceiling is charged so a missing Provider response cannot undersettle.
    pub fn conservative_dispatched(
        admission: &ModelTurnAdmissionSnapshot,
        limits: ModelTurnLimits,
    ) -> Result<Self, ModelTurnError> {
        admission.validate(limits)?;
        let profile = &admission.profile;
        let provider_reported_cost = if profile.usage.reports_cost {
            let currency = profile
                .usage
                .cost_currency
                .as_ref()
                .ok_or(ModelTurnError::InvalidUsage)?;
            Some(
                DecimalMoney::new(
                    currency.clone(),
                    i64::try_from(admission.quota_ceiling.cost_microunits)
                        .map_err(|_| ModelTurnError::InvalidUsage)?,
                    6,
                )
                .map_err(|_| ModelTurnError::InvalidUsage)?,
            )
        } else {
            None
        };
        let measurement = Self {
            usage: Some(ModelUsage {
                input_tokens: Some(admission.quota_ceiling.tokens),
                output_tokens: Some(0),
                cached_input_tokens: profile.usage.reports_cached_input_tokens.then_some(0),
                reasoning_tokens: profile.usage.reports_reasoning_tokens.then_some(0),
                provider_reported_cost,
                accounting_quality: AccountingQuality::Reconciled,
            }),
            observation: ModelObservation {
                request_sent: true,
                provider_response_digest: None,
                actual_model_identity: None,
                model_fingerprint: None,
                possible_duplicate_charge: true,
                stream_delta_count: 0,
                stream_bytes: 0,
            },
        };
        let (usage, _) = measurement.validate_for(admission, limits)?;
        if usage.requests != admission.quota_ceiling.requests
            || usage.tokens != admission.quota_ceiling.tokens
            || (profile.usage.reports_cost
                && usage.cost_microunits != admission.quota_ceiling.cost_microunits)
            || (!profile.usage.reports_cost && usage.cost_microunits != 0)
        {
            return Err(ModelTurnError::InvalidUsage);
        }
        Ok(measurement)
    }

    fn validate_for(
        &self,
        admission: &ModelTurnAdmissionSnapshot,
        limits: ModelTurnLimits,
    ) -> Result<(ModelUsageAmounts, Option<AccountingQuality>), ModelTurnError> {
        self.observation
            .validate_for(&admission.profile, limits.maximum_response_bytes())?;
        match (&self.usage, self.observation.request_sent) {
            (Some(usage), true) => Ok((
                usage.validate_for(&admission.profile)?,
                Some(usage.accounting_quality),
            )),
            (None, false) => Ok((
                ModelUsageAmounts {
                    requests: 0,
                    tokens: 0,
                    cost_microunits: 0,
                },
                None,
            )),
            _ => Err(ModelTurnError::InvalidUsage),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeModelFailure {
    pub failure: Failure,
    pub safe_code: String,
    pub evidence_digest: Sha256Digest,
}

impl SafeModelFailure {
    fn validate(&self) -> Result<(), ModelTurnError> {
        self.failure
            .validate(1_024)
            .map_err(|_| ModelTurnError::InvalidFailure)?;
        if self.failure.source != FailureSource::Model {
            return Err(ModelTurnError::InvalidFailure);
        }
        if !is_code(&self.safe_code, MAX_MODEL_NAME_BYTES) {
            return Err(ModelTurnError::InvalidFailure);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelDispatchOutcome {
    Succeeded(Box<ModelOutputValue>),
    RetryableFailure {
        failure: SafeModelFailure,
        retry_at: DateTime<Utc>,
        measurement: ModelAttemptMeasurement,
    },
    PermanentFailure {
        failure: SafeModelFailure,
        measurement: ModelAttemptMeasurement,
    },
}

#[derive(Debug, Clone)]
pub struct CommitModelOutcome {
    pub audit: ModelWorkerAudit,
    pub model_turn_id: ResourceId,
    pub job_id: ResourceId,
    pub expected_turn_version: u64,
    pub fence: JobFence,
    pub usage_reservation_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
    pub request: CanonicalModelRequest,
    pub outcome: ModelDispatchOutcome,
    pub resume_mutations: Option<ExternalLeafResumeMutationIds>,
    pub failure_mutations: Option<ExternalLeafFailureMutationIds>,
}

impl CommitModelOutcome {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ModelTurnError> {
        self.audit.validate_at(now)?;
        validate_quota_entry_ids(&self.quota_entry_ids)?;
        if self.model_turn_id.kind() != ResourceKind::ModelTurn
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_turn_version == 0
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
            || self.usage_reservation_id.kind() != ResourceKind::UsageReservation
            || self.resume_mutations.is_some()
                && !matches!(
                    &self.outcome,
                    ModelDispatchOutcome::Succeeded(output) if output.response.tool_intents.is_empty()
                )
            || self.failure_mutations.is_some()
                && !matches!(self.outcome, ModelDispatchOutcome::PermanentFailure { .. })
            || (self.resume_mutations.is_some() && self.failure_mutations.is_some())
            || self
                .resume_mutations
                .as_ref()
                .is_some_and(|mutations| mutations.validate().is_err())
            || self
                .failure_mutations
                .as_ref()
                .is_some_and(|mutations| mutations.validate().is_err())
        {
            return Err(ModelTurnError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelQuotaSettlement {
    pub concurrency_used: u64,
    pub requests_used: u64,
    pub tokens_used: u64,
    pub cost_microunits_used: u64,
}

impl ModelQuotaSettlement {
    fn from_usage(
        usage: ModelUsageAmounts,
        ceiling: ModelQuotaCeiling,
    ) -> Result<Self, ModelTurnError> {
        if usage.requests > ceiling.requests
            || usage.tokens > ceiling.tokens
            || usage.cost_microunits > ceiling.cost_microunits
        {
            return Err(ModelTurnError::UsageCeilingExceeded);
        }
        Ok(Self {
            concurrency_used: 0,
            requests_used: usage.requests,
            tokens_used: usage.tokens,
            cost_microunits_used: usage.cost_microunits,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelOutcomeDecision {
    pub turn: ModelTurnRecord,
    pub job: JobProjection,
    pub job_payload: ModelJobPayload,
    pub output: Option<Box<ModelOutputValue>>,
    pub settlement: ModelQuotaSettlement,
}

#[allow(clippy::too_many_arguments)]
pub fn decide_model_outcome(
    current_turn: &ModelTurnRecord,
    current_job: &JobProjection,
    current_payload: &ModelJobPayload,
    fence: &JobFence,
    usage_reservation_id: &ResourceId,
    request: &CanonicalModelRequest,
    outcome: &ModelDispatchOutcome,
    database_now: DateTime<Utc>,
    limits: ModelTurnLimits,
) -> Result<ModelOutcomeDecision, ModelTurnError> {
    current_turn.validate(limits)?;
    current_payload.validate_for(current_turn, current_job, limits)?;
    if current_turn.state != ModelTurnState::InFlight
        || current_turn.payload.current_job_id.as_ref() != Some(&current_job.job_id)
        || current_job.state != insight_platform_contracts::JobState::Running
        || current_payload.active_usage_reservation_id.as_ref() != Some(usage_reservation_id)
        || database_now >= current_turn.deadline
    {
        return Err(ModelTurnError::FirstWinnerLost);
    }
    request.validate_for(
        &current_turn.model_turn_id,
        &current_turn.payload.admission.provider,
        &current_turn.payload.admission.profile,
        &current_turn.payload.admission.provider_closure.region,
        current_turn.created_at,
        limits,
    )?;
    if request.canonical_digest()? != current_turn.payload.admission.request_digest {
        return Err(ModelTurnError::InvalidRequest);
    }
    let mut turn = current_turn.clone();
    let mut job_payload = current_payload.clone();
    let (job, output, usage, quality, observation, outcome_kind) = match outcome {
        ModelDispatchOutcome::Succeeded(output) => {
            let usage = output.response.validate_for(
                request,
                &turn.payload.admission.provider,
                &turn.payload.admission.profile,
                limits,
            )?;
            let exact = output.exact_for(&turn.run_id, &turn.node_execution_id, request, limits)?;
            let response_digest = digest(&output.response)?;
            let result_digest = digest(&serde_json::json!({
                "finish_reason": output.response.finish_reason,
                "output": exact,
                "response_digest": response_digest,
                "tool_intent_count": output.response.tool_intents.len(),
                "validation_evidence_digest": output.validation_evidence_digest,
            }))?;
            let job = decide_terminal(
                current_job,
                fence,
                database_now,
                insight_platform_contracts::JobState::Succeeded,
            )
            .map_err(|_| ModelTurnError::FirstWinnerLost)?;
            turn.state = ModelTurnState::Succeeded;
            turn.output_value_id = Some(output.value_id.clone());
            turn.payload.result = Some(ModelTurnResult {
                schema_version: 1,
                output: exact,
                response_digest: response_digest.clone(),
                finish_reason: output.response.finish_reason,
                tool_intent_count: u32::try_from(output.response.tool_intents.len())
                    .map_err(|_| ModelTurnError::InvalidLimits)?,
                validation_evidence_digest: output.validation_evidence_digest.clone(),
                result_digest,
            });
            turn.terminal_at = Some(database_now);
            job_payload.physical_outcome = Some(ModelPhysicalOutcomeEvidence::Succeeded {
                response_digest,
                output_value_id: output.value_id.clone(),
                tool_intent_count: u32::try_from(output.response.tool_intents.len())
                    .map_err(|_| ModelTurnError::InvalidLimits)?,
            });
            (
                job,
                Some(output.clone()),
                usage,
                Some(output.response.usage.accounting_quality),
                output.response.observation.clone(),
                ModelAttemptOutcomeKind::Succeeded,
            )
        }
        ModelDispatchOutcome::RetryableFailure {
            failure,
            retry_at,
            measurement,
        } => {
            failure.validate()?;
            if failure.failure.retryability == Retryability::Never
                || current_job.attempt_count >= current_job.attempt_limit
            {
                return Err(ModelTurnError::InvalidFailure);
            }
            let (usage, quality) = measurement.validate_for(&turn.payload.admission, limits)?;
            let job = decide_retry(current_job, fence, database_now, *retry_at)
                .map_err(|_| ModelTurnError::InvalidJob)?;
            turn.state = ModelTurnState::RetryScheduled;
            turn.retry_at = Some(*retry_at);
            job_payload.physical_outcome = Some(ModelPhysicalOutcomeEvidence::RetryScheduled {
                failure_digest: digest(failure)?,
            });
            (
                job,
                None,
                usage,
                quality,
                measurement.observation.clone(),
                ModelAttemptOutcomeKind::RetryScheduled,
            )
        }
        ModelDispatchOutcome::PermanentFailure {
            failure,
            measurement,
        } => {
            failure.validate()?;
            let (usage, quality) = measurement.validate_for(&turn.payload.admission, limits)?;
            let job = decide_terminal(
                current_job,
                fence,
                database_now,
                insight_platform_contracts::JobState::Failed,
            )
            .map_err(|_| ModelTurnError::FirstWinnerLost)?;
            turn.state = ModelTurnState::Failed;
            turn.payload.failure = Some(failure.failure.clone());
            turn.terminal_at = Some(database_now);
            job_payload.physical_outcome = Some(ModelPhysicalOutcomeEvidence::Failed {
                failure_digest: digest(failure)?,
            });
            (
                job,
                None,
                usage,
                quality,
                measurement.observation.clone(),
                ModelAttemptOutcomeKind::Failed,
            )
        }
    };
    let settlement = ModelQuotaSettlement::from_usage(usage, turn.payload.admission.quota_ceiling)?;
    let accounting = ModelAttemptAccounting {
        attempt_no: current_job.attempt_count,
        lease_generation: current_job.lease_generation,
        usage_reservation_id: usage_reservation_id.clone(),
        outcome: outcome_kind,
        usage,
        accounting_quality: quality,
        request_sent: observation.request_sent,
        observation_digest: digest(&observation)?,
        settled_at: database_now,
    };
    accounting.validate(turn.payload.admission.quota_ceiling)?;
    turn.payload.attempts.push(accounting);
    job_payload.active_usage_reservation_id = None;
    turn.version = increment(turn.version)?;
    turn.updated_at = database_now;
    turn.validate(limits)?;
    job_payload.validate_for(&turn, &job, limits)?;
    Ok(ModelOutcomeDecision {
        turn,
        job,
        job_payload,
        output,
        settlement,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelControlKind {
    Cancel,
    Timeout,
}

#[derive(Debug, Clone)]
pub struct ControlModelTurn {
    pub audit: CommandAudit,
    pub model_turn_id: ResourceId,
    pub expected_turn_version: u64,
    pub quota_entry_ids: Vec<ResourceId>,
    pub kind: ModelControlKind,
}

impl ControlModelTurn {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ModelTurnError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ModelTurnError::InvalidAudit)?;
        if self.model_turn_id.kind() != ResourceKind::ModelTurn || self.expected_turn_version == 0 {
            return Err(ModelTurnError::InvalidCommand);
        }
        if self.quota_entry_ids.is_empty() {
            Ok(())
        } else {
            validate_quota_entry_ids(&self.quota_entry_ids)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelControlDecision {
    pub turn: ModelTurnRecord,
    pub job: Option<JobProjection>,
    pub job_payload: Option<ModelJobPayload>,
    pub settlement: Option<ModelQuotaSettlement>,
}

pub fn decide_model_control(
    current_turn: &ModelTurnRecord,
    current_job: Option<&JobProjection>,
    current_payload: Option<&ModelJobPayload>,
    kind: ModelControlKind,
    database_now: DateTime<Utc>,
    limits: ModelTurnLimits,
) -> Result<ModelControlDecision, ModelTurnError> {
    current_turn.validate(limits)?;
    if is_terminal(current_turn.state)
        || (kind == ModelControlKind::Timeout && database_now < current_turn.deadline)
    {
        return Err(ModelTurnError::FirstWinnerLost);
    }
    let paired = match (current_job, current_payload) {
        (Some(job), Some(payload)) => {
            payload.validate_for(current_turn, job, limits)?;
            if current_turn.payload.current_job_id.as_ref() != Some(&job.job_id) {
                return Err(ModelTurnError::InvalidJob);
            }
            Some((job, payload))
        }
        (None, None) if current_turn.payload.current_job_id.is_none() => None,
        _ => return Err(ModelTurnError::InvalidJob),
    };
    let mut turn = current_turn.clone();
    let mut next_job = paired.map(|(job, _)| job.clone());
    let mut next_payload = paired.map(|(_, payload)| payload.clone());
    let mut settlement = None;
    if current_turn.state == ModelTurnState::InFlight && kind == ModelControlKind::Cancel {
        let job = paired
            .map(|(job, _)| job)
            .ok_or(ModelTurnError::InvalidJob)?;
        next_job = Some(decide_owner_cancelling(job).map_err(|_| ModelTurnError::InvalidControl)?);
        turn.state = ModelTurnState::Cancelling;
    } else {
        let target_job = match kind {
            ModelControlKind::Cancel => insight_platform_contracts::JobState::Cancelled,
            ModelControlKind::Timeout => insight_platform_contracts::JobState::TimedOut,
        };
        if let Some((job, payload)) = paired {
            next_job = Some(
                decide_owner_terminal(job, target_job)
                    .map_err(|_| ModelTurnError::InvalidControl)?,
            );
            if let Some(reservation_id) = &payload.active_usage_reservation_id {
                let usage = ModelUsageAmounts {
                    requests: 1,
                    tokens: turn.payload.admission.quota_ceiling.tokens,
                    cost_microunits: turn.payload.admission.quota_ceiling.cost_microunits,
                };
                turn.payload.attempts.push(ModelAttemptAccounting {
                    attempt_no: job.attempt_count,
                    lease_generation: job.lease_generation,
                    usage_reservation_id: reservation_id.clone(),
                    outcome: match kind {
                        ModelControlKind::Cancel => ModelAttemptOutcomeKind::Cancelled,
                        ModelControlKind::Timeout => ModelAttemptOutcomeKind::TimedOut,
                    },
                    usage,
                    accounting_quality: Some(AccountingQuality::Estimated),
                    request_sent: true,
                    observation_digest: digest(&serde_json::json!({
                        "control": match kind { ModelControlKind::Cancel => "cancel", ModelControlKind::Timeout => "timeout" },
                        "job_id": job.job_id,
                        "lease_generation": job.lease_generation,
                    }))?,
                    settled_at: database_now,
                });
                settlement = Some(ModelQuotaSettlement::from_usage(
                    usage,
                    turn.payload.admission.quota_ceiling,
                )?);
                if let Some(payload) = next_payload.as_mut() {
                    payload.active_usage_reservation_id = None;
                    payload.physical_outcome = Some(match kind {
                        ModelControlKind::Cancel => ModelPhysicalOutcomeEvidence::Cancelled {
                            observation_digest: digest(&serde_json::json!({"control":"cancel"}))?,
                        },
                        ModelControlKind::Timeout => ModelPhysicalOutcomeEvidence::TimedOut {
                            observation_digest: digest(&serde_json::json!({"control":"timeout"}))?,
                        },
                    });
                }
            }
        }
        turn.state = match kind {
            ModelControlKind::Cancel => ModelTurnState::Cancelled,
            ModelControlKind::Timeout => ModelTurnState::TimedOut,
        };
        turn.terminal_at = Some(database_now);
    }
    turn.version = increment(turn.version)?;
    turn.updated_at = database_now;
    turn.validate(limits)?;
    if let (Some(job), Some(payload)) = (&next_job, &next_payload) {
        payload.validate_for(&turn, job, limits)?;
    }
    Ok(ModelControlDecision {
        turn,
        job: next_job,
        job_payload: next_payload,
        settlement,
    })
}

#[derive(Debug, Clone)]
pub struct CommitModelCancellationOutcome {
    pub audit: ModelWorkerAudit,
    pub model_turn_id: ResourceId,
    pub job_id: ResourceId,
    pub expected_turn_version: u64,
    pub fence: Option<JobFence>,
    pub usage_reservation_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
    pub measurement: ModelAttemptMeasurement,
}

impl CommitModelCancellationOutcome {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ModelTurnError> {
        self.audit.validate_at(now)?;
        validate_quota_entry_ids(&self.quota_entry_ids)?;
        if self.model_turn_id.kind() != ResourceKind::ModelTurn
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_turn_version == 0
            || self.usage_reservation_id.kind() != ResourceKind::UsageReservation
            || self.fence.as_ref().is_some_and(|fence| {
                fence.expected_version == 0
                    || fence.lease_generation == 0
                    || fence.worker_process_generation_id != self.audit.worker_process_generation_id
            })
        {
            return Err(ModelTurnError::InvalidCommand);
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub fn decide_model_cancellation_outcome(
    current_turn: &ModelTurnRecord,
    current_job: &JobProjection,
    current_payload: &ModelJobPayload,
    fence: Option<&JobFence>,
    usage_reservation_id: &ResourceId,
    measurement: &ModelAttemptMeasurement,
    database_now: DateTime<Utc>,
    limits: ModelTurnLimits,
) -> Result<ModelOutcomeDecision, ModelTurnError> {
    current_turn.validate(limits)?;
    current_payload.validate_for(current_turn, current_job, limits)?;
    if current_turn.state != ModelTurnState::Cancelling
        || current_turn.payload.current_job_id.as_ref() != Some(&current_job.job_id)
        || current_job.state != insight_platform_contracts::JobState::Cancelling
        || current_payload.active_usage_reservation_id.as_ref() != Some(usage_reservation_id)
    {
        return Err(ModelTurnError::FirstWinnerLost);
    }
    let (usage, quality) = measurement.validate_for(&current_turn.payload.admission, limits)?;
    let job = match fence {
        Some(fence) => decide_terminal(
            current_job,
            fence,
            database_now,
            insight_platform_contracts::JobState::Cancelled,
        ),
        None => decide_owner_terminal(current_job, insight_platform_contracts::JobState::Cancelled),
    }
    .map_err(|_| ModelTurnError::FirstWinnerLost)?;
    let settlement =
        ModelQuotaSettlement::from_usage(usage, current_turn.payload.admission.quota_ceiling)?;
    let observation_digest = digest(&measurement.observation)?;
    let mut turn = current_turn.clone();
    turn.payload.attempts.push(ModelAttemptAccounting {
        attempt_no: current_job.attempt_count,
        lease_generation: current_job.lease_generation,
        usage_reservation_id: usage_reservation_id.clone(),
        outcome: ModelAttemptOutcomeKind::Cancelled,
        usage,
        accounting_quality: quality,
        request_sent: measurement.observation.request_sent,
        observation_digest: observation_digest.clone(),
        settled_at: database_now,
    });
    turn.state = ModelTurnState::Cancelled;
    turn.version = increment(turn.version)?;
    turn.terminal_at = Some(database_now);
    turn.updated_at = database_now;
    let mut job_payload = current_payload.clone();
    job_payload.active_usage_reservation_id = None;
    job_payload.physical_outcome =
        Some(ModelPhysicalOutcomeEvidence::Cancelled { observation_digest });
    turn.validate(limits)?;
    job_payload.validate_for(&turn, &job, limits)?;
    Ok(ModelOutcomeDecision {
        turn,
        job,
        job_payload,
        output: None,
        settlement,
    })
}

#[async_trait::async_trait]
pub trait ModelTurnTransaction: Send {
    type Error;
    type ExecutionRecord;
    type ControlRecord;

    async fn create_model_turn(
        &mut self,
        command: CreateModelTurn,
    ) -> Result<CommandOutcome<ModelTurnRecord>, Self::Error>;

    async fn prepare_model_dispatch(
        &mut self,
        command: PrepareModelDispatch,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error>;

    async fn commit_model_outcome(
        &mut self,
        command: CommitModelOutcome,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error>;

    async fn commit_model_cancellation_outcome(
        &mut self,
        command: CommitModelCancellationOutcome,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error>;

    async fn control_model_turn(
        &mut self,
        command: ControlModelTurn,
    ) -> Result<CommandOutcome<Self::ControlRecord>, Self::Error>;

    async fn commit(self) -> Result<(), Self::Error>;
    async fn rollback(self) -> Result<(), Self::Error>;
}

#[async_trait::async_trait]
pub trait ModelTurnStore {
    type Error;
    type Transaction<'a>: ModelTurnTransaction<Error = Self::Error>
    where
        Self: 'a;

    async fn begin_model_turn(&self) -> Result<Self::Transaction<'_>, Self::Error>;
}

fn exact_model_policies(
    run: &[ExactPolicyBinding],
    selection: &ExactPolicyBinding,
    truncation: &ExactVersionRef,
    model: &ModelDeploymentClosure,
    provider: &ModelProviderDeploymentClosure,
) -> Result<Vec<ExactVersionRef>, ModelTurnError> {
    let mut policies = run
        .iter()
        .map(|binding| binding.revision.clone())
        .collect::<Vec<_>>();
    policies.extend([selection.revision.clone(), truncation.clone()]);
    policies.extend([
        model.data_policy.clone(),
        model.budget_policy.clone(),
        model.public_projection_policy.clone(),
        provider.protocol_policy.clone(),
        provider.network_policy.clone(),
        provider.tls_policy.clone(),
        provider.trust_policy.clone(),
        provider.data_policy.clone(),
    ]);
    policies.sort_by(|left, right| left.revision_id.cmp(&right.revision_id));
    policies.dedup_by(|left, right| left.revision_id == right.revision_id);
    validate_model_policies(&policies)?;
    Ok(policies)
}

fn validate_model_policies(policies: &[ExactVersionRef]) -> Result<(), ModelTurnError> {
    if policies.is_empty() || policies.len() > MAX_MODEL_POLICIES {
        return Err(ModelTurnError::InvalidPolicy);
    }
    for policy in policies {
        policy
            .validate()
            .map_err(|_| ModelTurnError::InvalidPolicy)?;
        if policy.resource_kind != ResourceKind::PolicyRevision {
            return Err(ModelTurnError::InvalidPolicy);
        }
    }
    if !policies
        .windows(2)
        .all(|pair| pair[0].revision_id < pair[1].revision_id)
    {
        return Err(ModelTurnError::NonCanonicalCollection);
    }
    Ok(())
}

fn validate_quota_entry_ids(ids: &[ResourceId]) -> Result<(), ModelTurnError> {
    if ids.len() != MODEL_QUOTA_LINES
        || ids
            .iter()
            .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
        || ids.iter().collect::<BTreeSet<_>>().len() != ids.len()
    {
        return Err(ModelTurnError::InvalidIdentity);
    }
    Ok(())
}

fn model_turn_logical_key(round_ordinal: u16) -> String {
    format!("round:{round_ordinal:05}")
}

fn digest_without_field<T: Serialize>(
    value: &T,
    field: &str,
) -> Result<Sha256Digest, ModelTurnError> {
    let mut value = serde_json::to_value(value).map_err(|_| ModelTurnError::Canonicalization)?;
    value
        .as_object_mut()
        .ok_or(ModelTurnError::Canonicalization)?
        .remove(field)
        .ok_or(ModelTurnError::Canonicalization)?;
    digest(&value)
}

fn is_terminal(state: ModelTurnState) -> bool {
    matches!(
        state,
        ModelTurnState::Succeeded
            | ModelTurnState::Failed
            | ModelTurnState::Cancelled
            | ModelTurnState::TimedOut
    )
}

fn increment(value: u64) -> Result<u64, ModelTurnError> {
    value.checked_add(1).ok_or(ModelTurnError::CounterOverflow)
}

pub fn model_failure(class: FailureClass, retryability: Retryability) -> SafeModelFailure {
    SafeModelFailure {
        failure: Failure {
            code: FailureCode::Platform {
                code: PlatformFailureCode::ModelTurnFailed,
            },
            class,
            retryability,
            safe_message: None,
            details_ref: None,
            source: FailureSource::Model,
        },
        safe_code: "model_turn_failed".to_owned(),
        evidence_digest: digest(&serde_json::json!({
            "class": class,
            "domain": "model_turn_failed",
            "retryability": retryability,
            "schema_version": 1,
        }))
        .expect("static Model failure evidence is canonical"),
    }
}
