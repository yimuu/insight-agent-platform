use super::{
    digest, domain_digest, is_stable_code, CapabilityInvocationRecord, ExactInvocationValueRef,
    InvocationCommandLimits, InvocationError, InvocationValueStorage,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    CapabilityBackendKind, CapabilityProgressDurability, CapabilityProgressMode, CommandAudit,
    DataClassification, Effect, Failure, InteractionSchemaDocument, InvocationState, JobState,
    ResourceId, ResourceKind, Retryability, Sha256Digest, ValueRef, WorkClass,
};
use insight_platform_jobs::{
    decide_claim, decide_claim_continuation, decide_cleanup_reconciliation,
    decide_cleanup_terminal, decide_expired_lease, decide_observation_update,
    decide_owner_cancelling, decide_owner_reconciliation, decide_owner_terminal,
    decide_reconciliation, decide_resume, decide_retry, decide_start, decide_terminal, decide_wait,
    decide_wake, JobFence, JobOwnerRef, JobProjection, LeasePolicy, WakeContract, WakeKind,
    WakeSource,
};
use serde::{Deserialize, Serialize};

const MAX_ENCRYPTION_SCHEME_BYTES: usize = 64;
const MAX_SAFE_STAGE_KEY_BYTES: usize = 128;
pub const CAPABILITY_QUOTA_LINES: usize = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityJobBinding {
    pub schema_version: u32,
    pub invocation_id: ResourceId,
    pub admission_digest: Sha256Digest,
    pub deployment: insight_platform_contracts::ExactDeploymentRef,
    pub effect_key_digest: Sha256Digest,
    pub idempotency_key_digest: Sha256Digest,
    pub input: ExactInvocationValueRef,
    pub deadline: DateTime<Utc>,
}

impl CapabilityJobBinding {
    pub fn from_invocation(current: &CapabilityInvocationRecord) -> Result<Self, InvocationError> {
        current.validate()?;
        Ok(Self {
            schema_version: 1,
            invocation_id: current.invocation_id.clone(),
            admission_digest: current.payload.admission.canonical_digest.clone(),
            deployment: current.payload.admission.deployment.clone(),
            effect_key_digest: current.payload.admission.effect_key_digest.clone(),
            idempotency_key_digest: current.payload.admission.idempotency_key_digest.clone(),
            input: current.payload.admission.input.clone(),
            deadline: current.deadline,
        })
    }

    pub fn validate_for(
        &self,
        invocation: &CapabilityInvocationRecord,
    ) -> Result<(), InvocationError> {
        if self.schema_version != 1
            || self.invocation_id != invocation.invocation_id
            || self.admission_digest != invocation.payload.admission.canonical_digest
            || self.deployment != invocation.payload.admission.deployment
            || self.effect_key_digest != invocation.payload.admission.effect_key_digest
            || self.idempotency_key_digest != invocation.payload.admission.idempotency_key_digest
            || self.input != invocation.payload.admission.input
            || self.deadline != invocation.deadline
        {
            return Err(InvocationError::InvalidJob);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptedRemoteState {
    pub scheme: String,
    pub key_id: String,
    pub key_reference_digest: Sha256Digest,
    pub ciphertext: String,
    pub plaintext_digest: Sha256Digest,
}

impl EncryptedRemoteState {
    pub fn validate(&self, maximum_bytes: u32) -> Result<(), InvocationError> {
        if !is_stable_code(&self.scheme, MAX_ENCRYPTION_SCHEME_BYTES)
            || !is_stable_code(&self.key_id, MAX_ENCRYPTION_SCHEME_BYTES)
            || self.ciphertext.is_empty()
            || !self.ciphertext.is_ascii()
            || self.ciphertext.len()
                > usize::try_from(maximum_bytes).map_err(|_| InvocationError::InvalidLimits)?
        {
            return Err(InvocationError::InvalidOutcome);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityInputRequestBinding {
    pub input_task_id: ResourceId,
    pub interaction_kind: insight_platform_contracts::InteractionKind,
    pub safe_prompt_key: String,
    pub response_schema: InteractionSchemaDocument,
    pub response_schema_digest: Sha256Digest,
    pub eligible_principal_rule_digest: Sha256Digest,
    pub exact_eligible_principal_id: Option<ResourceId>,
    pub opaque_state_digest: Sha256Digest,
    pub wake_generation: u64,
    pub deadline: DateTime<Utc>,
}

impl CapabilityInputRequestBinding {
    pub fn validate(&self, invocation_deadline: DateTime<Utc>) -> Result<(), InvocationError> {
        if self.input_task_id.kind() != ResourceKind::Interaction
            || !is_stable_code(&self.safe_prompt_key, MAX_SAFE_STAGE_KEY_BYTES)
            || self.response_schema.validate().is_err()
            || self.response_schema.canonical_digest != self.response_schema_digest
            || self
                .exact_eligible_principal_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::Principal)
            || self.wake_generation == 0
            || self.deadline > invocation_deadline
        {
            return Err(InvocationError::InvalidOutcome);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityProgressState {
    pub accepted_count: u32,
    pub last_accepted_at: Option<DateTime<Utc>>,
    pub last_sequence: Option<u32>,
    pub last_payload_digest: Option<Sha256Digest>,
}

impl CapabilityProgressState {
    fn validate(&self) -> Result<(), InvocationError> {
        if (self.accepted_count == 0)
            != (self.last_accepted_at.is_none()
                && self.last_sequence.is_none()
                && self.last_payload_digest.is_none())
            || self.accepted_count != self.last_sequence.unwrap_or_default()
        {
            return Err(InvocationError::InvalidProgress);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CapabilityPhysicalOutcomeEvidence {
    Completed {
        output_value_id: ResourceId,
        output_content_digest: Sha256Digest,
        validation_evidence_digest: Sha256Digest,
    },
    Failed {
        failure_digest: Sha256Digest,
    },
    Uncertain {
        observation_digest: Sha256Digest,
    },
    Cancelled {
        no_effect_proof_digest: Option<Sha256Digest>,
    },
}

impl CapabilityPhysicalOutcomeEvidence {
    fn validate(&self) -> Result<(), InvocationError> {
        if matches!(
            self,
            Self::Completed {
                output_value_id,
                ..
            } if output_value_id.kind() != ResourceKind::RunValue
        ) {
            return Err(InvocationError::InvalidOutcome);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityJobPayload {
    pub schema_version: u32,
    pub binding: CapabilityJobBinding,
    pub wake_contract: Option<WakeContract>,
    pub encrypted_remote_state: Option<EncryptedRemoteState>,
    pub external_identity_digest: Option<Sha256Digest>,
    pub input_request: Option<CapabilityInputRequestBinding>,
    pub resume_input: Option<ExactInvocationValueRef>,
    pub resume_input_artifact_link_id: Option<ResourceId>,
    pub resume_input_action: Option<CapabilityInputAction>,
    pub resume_same_attempt: bool,
    pub progress: CapabilityProgressState,
    pub physical_outcome: Option<CapabilityPhysicalOutcomeEvidence>,
    pub poll_count: u32,
}

impl CapabilityJobPayload {
    pub fn initial(binding: CapabilityJobBinding) -> Self {
        Self {
            schema_version: 1,
            binding,
            wake_contract: None,
            encrypted_remote_state: None,
            external_identity_digest: None,
            input_request: None,
            resume_input: None,
            resume_input_artifact_link_id: None,
            resume_input_action: None,
            resume_same_attempt: false,
            progress: CapabilityProgressState {
                accepted_count: 0,
                last_accepted_at: None,
                last_sequence: None,
                last_payload_digest: None,
            },
            physical_outcome: None,
            poll_count: 0,
        }
    }

    pub fn validate_for(
        &self,
        invocation: &CapabilityInvocationRecord,
        job: &JobProjection,
    ) -> Result<(), InvocationError> {
        self.binding.validate_for(invocation)?;
        self.progress.validate()?;
        if self.schema_version != 1
            || job.tenant_id != invocation.tenant_id
            || job.owner.owner_kind != ResourceKind::CapabilityInvocation
            || job.owner.owner_id != invocation.invocation_id
            || job.job_id.kind() != ResourceKind::Job
            || job.attempt_limit != invocation.payload.admission.attempt_limit
            || job.deadline != invocation.deadline
            || self.wake_contract != job.wake
            || self.poll_count
                > invocation
                    .payload
                    .admission
                    .implementation_features
                    .max_poll_count
            || self
                .resume_input
                .as_ref()
                .is_some_and(|input| input.validate().is_err() || input.run_id != invocation.run_id)
            || self
                .resume_input_artifact_link_id
                .as_ref()
                .is_some_and(|link| link.kind() != ResourceKind::ArtifactLink)
            || self.resume_input.as_ref().is_some_and(|input| {
                matches!(input.storage, InvocationValueStorage::Artifact { .. })
                    != self.resume_input_artifact_link_id.is_some()
            })
            || (self.resume_input.is_none() && self.resume_input_artifact_link_id.is_some())
            || match self.resume_input_action {
                None => self.resume_input.is_some() || self.resume_input_artifact_link_id.is_some(),
                Some(CapabilityInputAction::Accept) => self.resume_input.is_none(),
                Some(CapabilityInputAction::Decline | CapabilityInputAction::Cancel) => {
                    self.resume_input.is_some() || self.resume_input_artifact_link_id.is_some()
                }
            }
            || self.input_request.as_ref().is_some_and(|request| {
                request.validate(invocation.deadline).is_err()
                    || self.wake_contract.as_ref().is_none_or(|wake| {
                        wake.generation != request.wake_generation
                            || !wake.accepted_sources.contains(&WakeSource::Signal)
                    })
            })
            || (self.resume_same_attempt
                && (job.attempt_count == 0
                    || !matches!(job.state, JobState::Waiting | JobState::Ready)
                    || self.encrypted_remote_state.is_none()))
            || (job.state == JobState::Waiting && !self.resume_same_attempt)
            || (job.state == JobState::Waiting) != self.wake_contract.is_some()
            || (self.input_request.is_some() && invocation.state != InvocationState::AwaitingInput)
            || self
                .physical_outcome
                .as_ref()
                .is_some_and(|outcome| outcome.validate().is_err())
        {
            return Err(InvocationError::InvalidJob);
        }
        if let Some(state) = &self.encrypted_remote_state {
            state.validate(
                invocation
                    .payload
                    .admission
                    .implementation_features
                    .max_remote_state_bytes,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PrepareCapabilityDispatch {
    pub audit: CommandAudit,
    pub invocation_id: ResourceId,
    pub expected_invocation_version: u64,
    pub job_id: ResourceId,
    pub scheduled_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct CapabilityWorkerAudit {
    pub tenant_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl CapabilityWorkerAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), InvocationError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= now
        {
            return Err(InvocationError::InvalidAudit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CapabilitySignalAudit {
    pub tenant_id: ResourceId,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl CapabilitySignalAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), InvocationError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= now
        {
            return Err(InvocationError::InvalidAudit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CapabilityClaimSlot {
    pub lease_token_digest: Sha256Digest,
    pub quota_reservation_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub resume_mutations: insight_platform_contracts::ExternalLeafResumeMutationIds,
    pub failure_mutations: insight_platform_contracts::ExternalLeafFailureMutationIds,
}

impl CapabilityClaimSlot {
    pub fn validate(&self) -> Result<(), InvocationError> {
        if self.quota_reservation_id.kind() != ResourceKind::UsageReservation
            || self.quota_entry_ids.len() != CAPABILITY_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self.quota_entry_ids[0] == self.quota_entry_ids[1]
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.resume_mutations.validate().is_err()
            || self.failure_mutations.validate().is_err()
        {
            return Err(InvocationError::InvalidIdentity);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ClaimCapabilityJobs {
    pub work_class: WorkClass,
    pub worker_process_generation_id: ResourceId,
    pub worker_manifest_digest: Sha256Digest,
    pub limit: u16,
    pub lease_milliseconds: u64,
    pub slots: Vec<CapabilityClaimSlot>,
}

impl ClaimCapabilityJobs {
    pub fn validate(&self) -> Result<(), InvocationError> {
        if !matches!(
            self.work_class,
            WorkClass::CapabilityNative | WorkClass::CapabilityRemote
        ) || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.limit == 0
            || self.slots.len() != usize::from(self.limit)
            || self.lease_milliseconds == 0
        {
            return Err(InvocationError::InvalidCommand);
        }
        for slot in &self.slots {
            slot.validate()?;
        }
        Ok(())
    }
}

impl PrepareCapabilityDispatch {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), InvocationError> {
        self.audit
            .validate_at(now)
            .map_err(|_| InvocationError::InvalidAudit)?;
        if self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_invocation_version == 0
        {
            return Err(InvocationError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PreparedCapabilityDispatch {
    pub invocation: CapabilityInvocationRecord,
    pub job: JobProjection,
    pub job_payload: CapabilityJobPayload,
}

pub fn capability_work_class(backend: CapabilityBackendKind) -> Result<WorkClass, InvocationError> {
    match backend {
        CapabilityBackendKind::Native => Ok(WorkClass::CapabilityNative),
        CapabilityBackendKind::Http | CapabilityBackendKind::Grpc | CapabilityBackendKind::Mcp => {
            Ok(WorkClass::CapabilityRemote)
        }
        CapabilityBackendKind::Sandbox => Err(InvocationError::InvalidCommand),
    }
}

/// Logical Capability backend that is permitted to use one backend-owned Sandbox Job as its
/// physical attempt authority.
///
/// The explicit source kind prevents a generic MCP dispatch from being silently reclassified as
/// Sandbox work: only the caller that has resolved an exact Managed stdio transport may select
/// [`Self::ManagedMcp`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DetachedSandboxSourceKind {
    SandboxCapability,
    ManagedMcp,
}

impl DetachedSandboxSourceKind {
    pub const fn logical_backend(self) -> CapabilityBackendKind {
        match self {
            Self::SandboxCapability => CapabilityBackendKind::Sandbox,
            Self::ManagedMcp => CapabilityBackendKind::Mcp,
        }
    }

    fn validates(self, current: &CapabilityInvocationRecord) -> bool {
        current.payload.admission.backend_kind == self.logical_backend()
            && match self {
                Self::SandboxCapability => current.payload.admission.mcp_runtime.is_none(),
                Self::ManagedMcp => current.payload.admission.mcp_runtime.is_some(),
            }
    }
}

/// Binds a Sandbox-backed logical Invocation directly to its sole physical Sandbox Job.
/// The caller persists this decision atomically with Sandbox admission. A due retry uses the
/// explicit `RetryScheduled -> Deferred` admission edge and creates a new physical Job.
#[derive(Debug, Clone, Copy)]
pub struct PreviousDetachedSandboxJob<'a> {
    pub job: &'a JobProjection,
    pub attempt_no: u32,
}

pub fn decide_defer_to_sandbox(
    current: &CapabilityInvocationRecord,
    source_kind: DetachedSandboxSourceKind,
    expected_invocation_version: u64,
    job_id: &ResourceId,
    attempt_no: u32,
    previous: Option<PreviousDetachedSandboxJob<'_>>,
    database_now: DateTime<Utc>,
) -> Result<CapabilityInvocationRecord, InvocationError> {
    current.validate()?;
    let initial = current.state == InvocationState::Ready
        && current.payload.current_job_id.is_none()
        && attempt_no == 1
        && previous.is_none();
    let retry = current.state == InvocationState::RetryScheduled
        && current
            .retry_at
            .is_some_and(|retry_at| retry_at <= database_now)
        && attempt_no > 1
        && previous.is_some_and(|previous| {
            previous.attempt_no.checked_add(1) == Some(attempt_no)
                && previous.job.work_class == WorkClass::Sandbox
                && previous.job.owner.owner_kind == ResourceKind::Job
                && previous.job.owner.owner_id.uuid() == previous.job.job_id.uuid()
                && current.payload.current_job_id.as_ref() == Some(&previous.job.job_id)
                && previous.job.lease.is_none()
                && (matches!(
                    previous.job.state,
                    JobState::Failed
                        | JobState::TimedOut
                        | JobState::Cancelled
                        | JobState::ReconciliationRequired
                ) || (source_kind == DetachedSandboxSourceKind::ManagedMcp
                    && previous.job.state == JobState::Succeeded))
        });
    let continuation = source_kind == DetachedSandboxSourceKind::ManagedMcp
        && attempt_no > 1
        && previous.is_some_and(|previous| {
            previous.attempt_no.checked_add(1) == Some(attempt_no)
                && previous.job.work_class == WorkClass::Sandbox
                && previous.job.owner.owner_kind == ResourceKind::Job
                && previous.job.owner.owner_id.uuid() == previous.job.job_id.uuid()
                && current.payload.current_job_id.as_ref() == Some(&previous.job.job_id)
                && previous.job.lease.is_none()
                && previous.job.state == JobState::Succeeded
        })
        && match (&current.state, &current.payload.detached_pending) {
            (
                InvocationState::Deferred,
                Some(CapabilityDetachedPending::RemoteTask { wait, .. }),
            ) => wait.next_poll_at.is_some_and(|next| next <= database_now),
            (
                InvocationState::Ready,
                Some(CapabilityDetachedPending::InputRequired {
                    resolution: Some(_),
                    ..
                }),
            ) => true,
            _ => false,
        };
    if !source_kind.validates(current)
        || current.version != expected_invocation_version
        || (!initial && !retry && !continuation)
        || job_id.kind() != ResourceKind::Job
        || current.payload.current_job_id.as_ref() == Some(job_id)
        || database_now >= current.deadline
    {
        return Err(InvocationError::FirstWinnerLost);
    }
    let mut next = current.clone();
    next.state = InvocationState::Deferred;
    next.version = increment(next.version)?;
    next.payload.current_job_id = Some(job_id.clone());
    next.payload.input_task_id = None;
    next.payload.detached_pending = None;
    next.retry_at = None;
    next.updated_at = database_now;
    next.validate()?;
    Ok(next)
}

/// A normalized logical outcome from a backend-owned shared Job that is already terminal.
/// Unlike [`DispatchOutcome`], applying this outcome never mutates or reopens the Job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum DetachedCapabilityJobOutcome {
    Completed(CapabilityOutputValue),
    Deferred {
        wait: RemoteWait,
        poll_count: u32,
    },
    InputRequired {
        request: BackendInputRequest,
        poll_count: u32,
    },
    RetryableFailure {
        failure: SafeBackendFailure,
        retry_at: DateTime<Utc>,
    },
    PermanentFailure(SafeBackendFailure),
    Cancelled,
    TimedOut,
    Uncertain(CapabilityUncertainty),
}

/// Logical continuation owned by a Capability Invocation after one detached Sandbox Job has
/// already terminated. The next poll or elicitation response is admitted as a new physical
/// Sandbox Job; the terminal Job is never reopened.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum CapabilityDetachedPending {
    RemoteTask {
        wait: RemoteWait,
        poll_count: u32,
    },
    InputRequired {
        request: Box<BackendInputRequest>,
        poll_count: u32,
        resolution: Option<CapabilityDetachedInputResolution>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityDetachedInputResolution {
    pub action: CapabilityInputAction,
    pub response: Option<ExactInvocationValueRef>,
    pub response_artifact_link_id: Option<ResourceId>,
}

impl CapabilityDetachedInputResolution {
    pub(crate) fn validate(&self) -> Result<(), InvocationError> {
        let accepted = self.action == CapabilityInputAction::Accept;
        if self.response.is_some() != accepted
            || self
                .response
                .as_ref()
                .is_some_and(|value| value.validate().is_err())
            || self.response_artifact_link_id.is_some()
                != self.response.as_ref().is_some_and(|value| {
                    matches!(value.storage, InvocationValueStorage::Artifact { .. })
                })
            || self
                .response_artifact_link_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ArtifactLink)
        {
            return Err(InvocationError::InvalidCurrentState);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DetachedCapabilityOutcomeDecision {
    pub invocation: CapabilityInvocationRecord,
    pub output: Option<CapabilityOutputValue>,
    pub input_request: Option<BackendInputRequest>,
}

pub fn decide_detached_job_outcome(
    current: &CapabilityInvocationRecord,
    source_kind: DetachedSandboxSourceKind,
    terminal_job: &JobProjection,
    logical_attempt_no: u32,
    outcome: &DetachedCapabilityJobOutcome,
    database_now: DateTime<Utc>,
    limits: InvocationCommandLimits,
) -> Result<DetachedCapabilityOutcomeDecision, InvocationError> {
    current.validate()?;
    terminal_job
        .validate()
        .map_err(|_| InvocationError::InvalidJob)?;
    let job_state_matches = match outcome {
        DetachedCapabilityJobOutcome::Completed(_)
        | DetachedCapabilityJobOutcome::Deferred { .. }
        | DetachedCapabilityJobOutcome::InputRequired { .. } => {
            terminal_job.state == JobState::Succeeded
        }
        DetachedCapabilityJobOutcome::RetryableFailure { .. }
        | DetachedCapabilityJobOutcome::PermanentFailure(_) => {
            terminal_job.state == JobState::Failed
                || (source_kind == DetachedSandboxSourceKind::ManagedMcp
                    && terminal_job.state == JobState::Succeeded)
        }
        DetachedCapabilityJobOutcome::Cancelled => terminal_job.state == JobState::Cancelled,
        DetachedCapabilityJobOutcome::TimedOut => terminal_job.state == JobState::TimedOut,
        DetachedCapabilityJobOutcome::Uncertain(_) => {
            matches!(
                terminal_job.state,
                JobState::Failed
                    | JobState::Cancelled
                    | JobState::TimedOut
                    | JobState::ReconciliationRequired
            ) || (source_kind == DetachedSandboxSourceKind::ManagedMcp
                && terminal_job.state == JobState::Succeeded)
        }
    };
    if !source_kind.validates(current)
        || !matches!(
            current.state,
            InvocationState::Deferred | InvocationState::Cancelling
        )
        || current.payload.current_job_id.as_ref() != Some(&terminal_job.job_id)
        || terminal_job.tenant_id != current.tenant_id
        || logical_attempt_no == 0
        || terminal_job.work_class != WorkClass::Sandbox
        || terminal_job.owner.owner_kind != ResourceKind::Job
        || terminal_job.owner.owner_id.uuid() != terminal_job.job_id.uuid()
        || !job_state_matches
        || terminal_job.lease.is_some()
    {
        return Err(InvocationError::FirstWinnerLost);
    }

    let mut invocation = current.clone();
    let mut output = None;
    let mut input_request = None;
    invocation.payload.detached_pending = None;
    match outcome {
        DetachedCapabilityJobOutcome::Completed(value) => {
            let exact = value.exact_for(current, limits)?;
            let result_digest = domain_digest(
                "capability_result",
                &serde_json::json!({
                    "output": exact,
                    "validation_evidence_digest": value.validation_evidence_digest,
                }),
            )?;
            invocation.state = InvocationState::Succeeded;
            invocation.output_value_id = Some(value.value_id.clone());
            invocation.payload.result = Some(super::CapabilityInvocationResult {
                schema_version: 1,
                output: exact,
                result_digest,
            });
            invocation.terminal_at = Some(database_now);
            output = Some(value.clone());
        }
        DetachedCapabilityJobOutcome::Deferred { wait, poll_count } => {
            if source_kind != DetachedSandboxSourceKind::ManagedMcp {
                return Err(InvocationError::UnsupportedOutcome);
            }
            validate_detached_remote_wait(current, wait, *poll_count, database_now, limits)?;
            invocation.state = InvocationState::Deferred;
            invocation.payload.detached_pending = Some(CapabilityDetachedPending::RemoteTask {
                wait: wait.clone(),
                poll_count: *poll_count,
            });
        }
        DetachedCapabilityJobOutcome::InputRequired {
            request,
            poll_count,
        } => {
            if source_kind != DetachedSandboxSourceKind::ManagedMcp {
                return Err(InvocationError::UnsupportedOutcome);
            }
            validate_detached_input_request(current, request, *poll_count, database_now, limits)?;
            invocation.state = InvocationState::AwaitingInput;
            invocation.payload.input_task_id = Some(request.input_task_id.clone());
            invocation.payload.detached_pending = Some(CapabilityDetachedPending::InputRequired {
                request: Box::new(request.clone()),
                poll_count: *poll_count,
                resolution: None,
            });
            input_request = Some(request.clone());
        }
        DetachedCapabilityJobOutcome::RetryableFailure { failure, retry_at } => {
            failure.validate()?;
            if failure.failure.retryability != Retryability::SafeWithinPolicy
                || *retry_at <= database_now
                || *retry_at >= current.deadline
                || logical_attempt_no >= current.payload.admission.attempt_limit
                || (current.payload.admission.effect.risk_rank()
                    >= Effect::IdempotentWrite.risk_rank()
                    && current.payload.admission.idempotency
                        == insight_platform_contracts::CapabilityIdempotencyKind::None)
            {
                return Err(InvocationError::UnsupportedOutcome);
            }
            invocation.state = InvocationState::RetryScheduled;
            invocation.retry_at = Some(*retry_at);
        }
        DetachedCapabilityJobOutcome::PermanentFailure(failure) => {
            failure.validate()?;
            invocation.state = InvocationState::Failed;
            invocation.payload.failure = Some(failure.failure.clone());
            invocation.terminal_at = Some(database_now);
        }
        DetachedCapabilityJobOutcome::Cancelled => {
            invocation.state = InvocationState::Cancelled;
            invocation.terminal_at = Some(database_now);
        }
        DetachedCapabilityJobOutcome::TimedOut => {
            invocation.state = InvocationState::TimedOut;
            invocation.terminal_at = Some(database_now);
        }
        DetachedCapabilityJobOutcome::Uncertain(uncertain) => {
            invocation.state = InvocationState::ReconciliationRequired;
            invocation.payload.reconciliation = Some(super::CapabilityReconciliationSnapshot {
                schema_version: 1,
                effect: current.payload.admission.effect,
                last_job_generation: terminal_job.lease_generation.max(1),
                external_identity_digest: Some(uncertain.external_identity_digest.clone()),
                observation_digest: uncertain.observation_digest.clone(),
                policy_path_digest: uncertain.policy_path_digest.clone(),
                manual: uncertain.manual,
            });
        }
    }
    invocation.version = increment(invocation.version)?;
    invocation.updated_at = database_now;
    invocation.validate()?;
    Ok(DetachedCapabilityOutcomeDecision {
        invocation,
        output,
        input_request,
    })
}

fn validate_detached_remote_wait(
    invocation: &CapabilityInvocationRecord,
    wait: &RemoteWait,
    poll_count: u32,
    database_now: DateTime<Utc>,
    limits: InvocationCommandLimits,
) -> Result<(), InvocationError> {
    let features = &invocation.payload.admission.implementation_features;
    if !features.deferred
        || !features.poll
        || poll_count >= features.max_poll_count
        || wait.accepted_sources != [WakeSource::Poll]
        || wait.callback_binding_digest.is_some()
        || wait.next_poll_at.is_none_or(|next| {
            next <= database_now
                || next >= invocation.deadline
                || next - database_now
                    > Duration::milliseconds(
                        i64::try_from(limits.maximum_deferred_poll_milliseconds())
                            .unwrap_or(i64::MAX),
                    )
        })
    {
        return Err(InvocationError::UnsupportedOutcome);
    }
    wait.encrypted_state.validate(
        features
            .max_remote_state_bytes
            .min(limits.maximum_remote_state_bytes()),
    )
}

fn validate_detached_input_request(
    invocation: &CapabilityInvocationRecord,
    request: &BackendInputRequest,
    poll_count: u32,
    database_now: DateTime<Utc>,
    limits: InvocationCommandLimits,
) -> Result<(), InvocationError> {
    let features = &invocation.payload.admission.implementation_features;
    if !features.input_required
        || poll_count == 0
        || poll_count > features.max_poll_count
        || request.input_task_id.kind() != ResourceKind::Interaction
        || !is_stable_code(&request.safe_prompt_key, MAX_SAFE_STAGE_KEY_BYTES)
        || request.response_schema.validate().is_err()
        || request.response_schema.canonical_digest != request.response_schema_digest
        || request.opaque_state_digest != request.encrypted_state.plaintext_digest
        || request.deadline <= database_now
        || request.deadline > invocation.deadline
    {
        return Err(InvocationError::UnsupportedOutcome);
    }
    request.encrypted_state.validate(
        features
            .max_remote_state_bytes
            .min(limits.maximum_remote_state_bytes()),
    )
}

pub fn decide_detached_input_response(
    current: &CapabilityInvocationRecord,
    terminal_job: &JobProjection,
    expected_wake_generation: u64,
    action: CapabilityInputAction,
    response: Option<&CapabilityInputResponse>,
    database_now: DateTime<Utc>,
    limits: InvocationCommandLimits,
) -> Result<CapabilityInvocationRecord, InvocationError> {
    current.validate()?;
    terminal_job
        .validate()
        .map_err(|_| InvocationError::InvalidJob)?;
    let Some(CapabilityDetachedPending::InputRequired {
        request,
        poll_count,
        resolution: None,
    }) = &current.payload.detached_pending
    else {
        return Err(InvocationError::InvalidWake);
    };
    if current.state != InvocationState::AwaitingInput
        || current.payload.current_job_id.as_ref() != Some(&terminal_job.job_id)
        || current.payload.input_task_id.as_ref() != Some(&request.input_task_id)
        || terminal_job.work_class != WorkClass::Sandbox
        || terminal_job.owner.owner_kind != ResourceKind::Job
        || terminal_job.owner.owner_id.uuid() != terminal_job.job_id.uuid()
        || terminal_job.state != JobState::Succeeded
        || terminal_job.lease.is_some()
        || terminal_job.lease_generation != expected_wake_generation
    {
        return Err(InvocationError::FirstWinnerLost);
    }
    let exact_response = match (action, response) {
        (CapabilityInputAction::Accept, Some(response)) => {
            if request.response_schema.profile
                == insight_platform_contracts::MCP_FORM_SCHEMA_PROFILE_ID
            {
                let ValueRef::Inline { value } = &response.value else {
                    return Err(InvocationError::InvalidInputValue);
                };
                request
                    .response_schema
                    .validate_mcp_form_instance(value)
                    .map_err(|_| InvocationError::InvalidInputValue)?;
            }
            Some(response.exact_for(current, &request.response_schema_digest, limits)?)
        }
        (CapabilityInputAction::Decline | CapabilityInputAction::Cancel, None) => None,
        _ => return Err(InvocationError::InvalidCommand),
    };
    let mut invocation = current.clone();
    invocation.version = increment(invocation.version)?;
    invocation.state = InvocationState::Ready;
    invocation.payload.input_task_id = None;
    invocation.payload.detached_pending = Some(CapabilityDetachedPending::InputRequired {
        request: request.clone(),
        poll_count: *poll_count,
        resolution: Some(CapabilityDetachedInputResolution {
            action,
            response: exact_response,
            response_artifact_link_id: response.and_then(|value| value.artifact_link_id.clone()),
        }),
    });
    invocation.updated_at = database_now;
    invocation.validate()?;
    Ok(invocation)
}

pub fn decide_prepare_dispatch(
    current: &CapabilityInvocationRecord,
    command: &PrepareCapabilityDispatch,
    database_now: DateTime<Utc>,
) -> Result<PreparedCapabilityDispatch, InvocationError> {
    current.validate()?;
    command.validate_at(database_now)?;
    if current.tenant_id != command.audit.tenant_id
        || current.payload.admission.principal.principal_id != command.audit.principal_id
        || current.invocation_id != command.invocation_id
        || current.version != command.expected_invocation_version
        || current.state != InvocationState::Ready
        || current.payload.current_job_id.is_some()
        || command.scheduled_at >= current.deadline
        || database_now >= current.deadline
    {
        return Err(InvocationError::FirstWinnerLost);
    }
    let binding = CapabilityJobBinding::from_invocation(current)?;
    let job_payload = CapabilityJobPayload::initial(binding);
    let job = JobProjection {
        trace: command.audit.trace,
        tenant_id: current.tenant_id.clone(),
        job_id: command.job_id.clone(),
        work_class: capability_work_class(current.payload.admission.backend_kind)?,
        owner: JobOwnerRef {
            owner_id: current.invocation_id.clone(),
            owner_kind: ResourceKind::CapabilityInvocation,
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
    job.validate().map_err(|_| InvocationError::InvalidJob)?;
    let mut invocation = current.clone();
    invocation.version = increment(invocation.version)?;
    invocation.payload.current_job_id = Some(command.job_id.clone());
    invocation.updated_at = database_now;
    invocation.validate()?;
    job_payload.validate_for(&invocation, &job)?;
    Ok(PreparedCapabilityDispatch {
        invocation,
        job,
        job_payload,
    })
}

#[derive(Debug, Clone)]
pub struct StartedCapabilityDispatch {
    pub invocation: CapabilityInvocationRecord,
    pub job: JobProjection,
    pub job_payload: CapabilityJobPayload,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveredCapabilityDispatch {
    pub invocation: CapabilityInvocationRecord,
    pub job: JobProjection,
    pub job_payload: CapabilityJobPayload,
}

#[derive(Debug, Clone)]
pub struct ExpiredCapabilityLeaseObservation {
    pub observed_job_version: u64,
    pub observed_lease_generation: u64,
    pub retry_at: Option<DateTime<Utc>>,
    pub observation_digest: Sha256Digest,
}

/// Applies the logical owner policy after a started Capability execution loses its lease.
/// Safe/replayable work receives a new physical attempt; an expired deadline is terminal; and
/// every outcome that might have produced an unproven write is fenced into reconciliation.
pub fn decide_expired_dispatch(
    current_invocation: &CapabilityInvocationRecord,
    current_job: &JobProjection,
    current_job_payload: &CapabilityJobPayload,
    database_now: DateTime<Utc>,
    observation: ExpiredCapabilityLeaseObservation,
) -> Result<RecoveredCapabilityDispatch, InvocationError> {
    current_invocation.validate()?;
    current_job_payload.validate_for(current_invocation, current_job)?;
    if current_invocation.state != InvocationState::InFlight
        || current_job.state != JobState::Running
        || current_invocation.payload.current_job_id.as_ref() != Some(&current_job.job_id)
    {
        return Err(InvocationError::FirstWinnerLost);
    }
    let mut invocation = current_invocation.clone();
    let mut job_payload = current_job_payload.clone();
    let replayable = current_invocation.payload.admission.effect.risk_rank()
        < Effect::IdempotentWrite.risk_rank()
        || current_invocation.payload.admission.idempotency
            != insight_platform_contracts::CapabilityIdempotencyKind::None;
    let job = if database_now >= current_job.deadline {
        invocation.state = InvocationState::TimedOut;
        invocation.terminal_at = Some(database_now);
        decide_expired_lease(
            current_job,
            observation.observed_job_version,
            observation.observed_lease_generation,
            database_now,
            JobState::TimedOut,
            None,
        )
    } else if replayable
        && current_job.attempt_count < current_job.attempt_limit
        && observation.retry_at.is_some()
    {
        let retry_at = observation.retry_at.expect("retry presence checked");
        invocation.state = InvocationState::RetryScheduled;
        invocation.retry_at = Some(retry_at);
        decide_expired_lease(
            current_job,
            observation.observed_job_version,
            observation.observed_lease_generation,
            database_now,
            JobState::RetryScheduled,
            Some(retry_at),
        )
    } else {
        invocation.state = InvocationState::ReconciliationRequired;
        invocation.payload.reconciliation = Some(super::CapabilityReconciliationSnapshot {
            schema_version: 1,
            effect: invocation.payload.admission.effect,
            last_job_generation: current_job.lease_generation,
            external_identity_digest: current_job_payload.external_identity_digest.clone(),
            observation_digest: observation.observation_digest.clone(),
            policy_path_digest: invocation
                .payload
                .admission
                .policies
                .canonical_digest
                .clone(),
            manual: true,
        });
        job_payload.physical_outcome = Some(CapabilityPhysicalOutcomeEvidence::Uncertain {
            observation_digest: observation.observation_digest,
        });
        decide_expired_lease(
            current_job,
            observation.observed_job_version,
            observation.observed_lease_generation,
            database_now,
            JobState::ReconciliationRequired,
            None,
        )
    }
    .map_err(|_| InvocationError::FirstWinnerLost)?;
    invocation.version = increment(invocation.version)?;
    invocation.updated_at = database_now;
    invocation.validate()?;
    job_payload.validate_for(&invocation, &job)?;
    Ok(RecoveredCapabilityDispatch {
        invocation,
        job,
        job_payload,
    })
}

pub fn decide_start_dispatch(
    current_invocation: &CapabilityInvocationRecord,
    current_job: &JobProjection,
    job_payload: &CapabilityJobPayload,
    worker_process_generation_id: ResourceId,
    lease_token_digest: Sha256Digest,
    lease_policy: LeasePolicy,
    database_now: DateTime<Utc>,
) -> Result<StartedCapabilityDispatch, InvocationError> {
    current_invocation.validate()?;
    job_payload.validate_for(current_invocation, current_job)?;
    if current_invocation.payload.current_job_id.as_ref() != Some(&current_job.job_id)
        || !matches!(
            current_invocation.state,
            InvocationState::Ready | InvocationState::RetryScheduled | InvocationState::InFlight
        )
        || database_now >= current_invocation.deadline
    {
        return Err(InvocationError::InvalidJob);
    }
    let continuing = job_payload.resume_same_attempt;
    let claimed = if continuing {
        decide_claim_continuation(
            current_job,
            database_now,
            worker_process_generation_id.clone(),
            lease_token_digest.clone(),
            lease_policy,
        )
    } else {
        decide_claim(
            current_job,
            database_now,
            worker_process_generation_id.clone(),
            lease_token_digest.clone(),
            lease_policy,
        )
    }
    .map_err(|_| InvocationError::FirstWinnerLost)?;
    let fence = JobFence {
        expected_version: claimed.version,
        worker_process_generation_id,
        lease_generation: claimed.lease_generation,
        token_digest: lease_token_digest,
    };
    let started = if continuing {
        decide_resume(&claimed, &fence, database_now)
    } else {
        decide_start(&claimed, &fence, database_now)
    }
    .map_err(|_| InvocationError::InvalidJob)?;
    let mut invocation = current_invocation.clone();
    invocation.version = increment(invocation.version)?;
    invocation.state = InvocationState::InFlight;
    invocation.retry_at = None;
    invocation.started_at.get_or_insert(database_now);
    invocation.updated_at = database_now;
    let mut next_payload = job_payload.clone();
    next_payload.resume_same_attempt = false;
    invocation.validate()?;
    next_payload.validate_for(&invocation, &started)?;
    Ok(StartedCapabilityDispatch {
        invocation,
        job: started,
        job_payload: next_payload,
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityOutputValue {
    pub value_id: ResourceId,
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub value: ValueRef,
    pub artifact_link_id: Option<ResourceId>,
    pub validation_evidence_digest: Sha256Digest,
}

impl CapabilityOutputValue {
    pub fn exact_for(
        &self,
        invocation: &CapabilityInvocationRecord,
        limits: InvocationCommandLimits,
    ) -> Result<ExactInvocationValueRef, InvocationError> {
        if self.value_id.kind() != ResourceKind::RunValue
            || self.schema_digest != invocation.payload.admission.output_schema_digest
            || self.classification.rank() < invocation.payload.admission.input.classification.rank()
            || self
                .artifact_link_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ArtifactLink)
        {
            return Err(InvocationError::InvalidOutput);
        }
        self.value
            .validate(limits.inline_value_limits())
            .map_err(|_| InvocationError::InvalidOutput)?;
        let storage = match &self.value {
            ValueRef::Inline { value } => {
                let actual: Sha256Digest = insight_platform_contracts::canonical_digest(value)
                    .map_err(|_| InvocationError::Canonicalization)?
                    .parse()
                    .map_err(|_| InvocationError::Canonicalization)?;
                if actual != self.content_digest || self.artifact_link_id.is_some() {
                    return Err(InvocationError::InvalidOutput);
                }
                InvocationValueStorage::Inline
            }
            ValueRef::Artifact { artifact } => {
                if artifact.content_digest() != &self.content_digest
                    || artifact.classification() != self.classification
                    || self.artifact_link_id.is_none()
                {
                    return Err(InvocationError::InvalidOutput);
                }
                InvocationValueStorage::Artifact {
                    artifact: artifact.clone(),
                }
            }
        };
        let exact = ExactInvocationValueRef {
            schema_version: 1,
            value_id: self.value_id.clone(),
            run_id: invocation.run_id.clone(),
            producing_node_id: Some(invocation.node_execution_id.clone()),
            value_kind: "capability_output".to_owned(),
            classification: self.classification,
            schema_digest: self.schema_digest.clone(),
            content_digest: self.content_digest.clone(),
            storage,
        };
        exact.validate()?;
        Ok(exact)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteWait {
    pub encrypted_state: EncryptedRemoteState,
    pub external_identity_digest: Sha256Digest,
    pub accepted_sources: Vec<WakeSource>,
    pub next_poll_at: Option<DateTime<Utc>>,
    pub callback_binding_digest: Option<Sha256Digest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendInputRequest {
    pub input_task_id: ResourceId,
    pub interaction_kind: insight_platform_contracts::InteractionKind,
    pub safe_prompt_key: String,
    pub response_schema: InteractionSchemaDocument,
    pub response_schema_digest: Sha256Digest,
    pub eligible_principal_rule_digest: Sha256Digest,
    pub exact_eligible_principal_id: Option<ResourceId>,
    pub opaque_state_digest: Sha256Digest,
    pub encrypted_state: EncryptedRemoteState,
    pub external_identity_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

/// Human decision delivered to the backend after the shared Task first-winner commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityInputAction {
    Accept,
    Decline,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeBackendFailure {
    pub failure: Failure,
    pub evidence_digest: Sha256Digest,
}

impl SafeBackendFailure {
    fn validate(&self) -> Result<(), InvocationError> {
        self.failure
            .validate(1_024)
            .map_err(|_| InvocationError::InvalidOutcome)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityUncertainty {
    pub observation_digest: Sha256Digest,
    pub policy_path_digest: Sha256Digest,
    pub external_identity_digest: Sha256Digest,
    pub manual: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum DispatchOutcome {
    Completed(CapabilityOutputValue),
    Deferred(RemoteWait),
    InputRequired(BackendInputRequest),
    RetryableFailure {
        failure: SafeBackendFailure,
        retry_at: DateTime<Utc>,
    },
    PermanentFailure(SafeBackendFailure),
    Uncertain(CapabilityUncertainty),
}

#[derive(Debug, Clone)]
pub struct CommitCapabilityOutcome {
    pub audit: CapabilityWorkerAudit,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub expected_invocation_version: u64,
    pub fence: JobFence,
    pub quota_entry_ids: Vec<ResourceId>,
    pub outcome: DispatchOutcome,
    pub resume_mutations: Option<insight_platform_contracts::ExternalLeafResumeMutationIds>,
    pub failure_mutations: Option<insight_platform_contracts::ExternalLeafFailureMutationIds>,
}

impl CommitCapabilityOutcome {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), InvocationError> {
        self.audit.validate_at(now)?;
        if self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_invocation_version == 0
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.quota_entry_ids.len() != CAPABILITY_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self.quota_entry_ids[0] == self.quota_entry_ids[1]
            || self
                .resume_mutations
                .as_ref()
                .is_some_and(|mutations| mutations.validate().is_err())
            || self
                .failure_mutations
                .as_ref()
                .is_some_and(|mutations| mutations.validate().is_err())
            || (self.resume_mutations.is_some() && self.failure_mutations.is_some())
        {
            return Err(InvocationError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CapabilityOutcomeDecision {
    pub invocation: CapabilityInvocationRecord,
    pub job: JobProjection,
    pub job_payload: CapabilityJobPayload,
    pub output: Option<CapabilityOutputValue>,
    pub input_request: Option<BackendInputRequest>,
}

pub fn decide_dispatch_outcome(
    current_invocation: &CapabilityInvocationRecord,
    current_job: &JobProjection,
    current_job_payload: &CapabilityJobPayload,
    fence: &JobFence,
    outcome: &DispatchOutcome,
    database_now: DateTime<Utc>,
    limits: InvocationCommandLimits,
) -> Result<CapabilityOutcomeDecision, InvocationError> {
    current_invocation.validate()?;
    current_job_payload.validate_for(current_invocation, current_job)?;
    if current_invocation.state != InvocationState::InFlight
        || current_invocation.payload.current_job_id.as_ref() != Some(&current_job.job_id)
        || current_job.state != JobState::Running
        || database_now >= current_invocation.deadline
    {
        return Err(InvocationError::FirstWinnerLost);
    }
    let mut invocation = current_invocation.clone();
    let mut job_payload = current_job_payload.clone();
    let prior_encrypted_remote_state = current_job_payload.encrypted_remote_state.clone();
    let prior_resume_input = current_job_payload.resume_input.clone();
    let prior_resume_input_artifact_link_id =
        current_job_payload.resume_input_artifact_link_id.clone();
    let prior_resume_input_action = current_job_payload.resume_input_action;
    job_payload.physical_outcome = None;
    job_payload.encrypted_remote_state = None;
    job_payload.external_identity_digest = None;
    job_payload.input_request = None;
    job_payload.resume_same_attempt = false;
    job_payload.resume_input = None;
    job_payload.resume_input_artifact_link_id = None;
    job_payload.resume_input_action = None;
    let (job, output, input_request) = match outcome {
        DispatchOutcome::Completed(output) => {
            let exact = output.exact_for(current_invocation, limits)?;
            let result_digest = domain_digest(
                "capability_result",
                &serde_json::json!({
                    "output": exact,
                    "validation_evidence_digest": output.validation_evidence_digest,
                }),
            )?;
            let job = decide_terminal(current_job, fence, database_now, JobState::Succeeded)
                .map_err(|_| InvocationError::FirstWinnerLost)?;
            invocation.state = InvocationState::Succeeded;
            invocation.output_value_id = Some(output.value_id.clone());
            invocation.payload.result = Some(super::CapabilityInvocationResult {
                schema_version: 1,
                output: exact,
                result_digest: result_digest.clone(),
            });
            invocation.terminal_at = Some(database_now);
            job_payload.physical_outcome = Some(CapabilityPhysicalOutcomeEvidence::Completed {
                output_value_id: output.value_id.clone(),
                output_content_digest: output.content_digest.clone(),
                validation_evidence_digest: output.validation_evidence_digest.clone(),
            });
            (job, Some(output.clone()), None)
        }
        DispatchOutcome::Deferred(wait) => {
            let features = &current_invocation.payload.admission.implementation_features;
            if !features.deferred
                || (wait.accepted_sources.contains(&WakeSource::Poll)
                    && current_job_payload.poll_count >= features.max_poll_count)
                || wait.accepted_sources.is_empty()
                || wait.accepted_sources.iter().any(|source| match source {
                    WakeSource::Callback => !features.callback,
                    WakeSource::Poll => !features.poll,
                    _ => true,
                })
                || wait.callback_binding_digest.is_some()
                    != wait.accepted_sources.contains(&WakeSource::Callback)
                || wait.next_poll_at.is_some() != wait.accepted_sources.contains(&WakeSource::Poll)
                || wait.next_poll_at.is_some_and(|next| {
                    next <= database_now
                        || next >= current_invocation.deadline
                        || next - database_now
                            > Duration::milliseconds(
                                i64::try_from(limits.maximum_deferred_poll_milliseconds())
                                    .unwrap_or(i64::MAX),
                            )
                })
            {
                return Err(InvocationError::UnsupportedOutcome);
            }
            wait.encrypted_state.validate(
                features
                    .max_remote_state_bytes
                    .min(limits.maximum_remote_state_bytes()),
            )?;
            let wake = WakeContract {
                kind: WakeKind::RemoteInvocation,
                generation: current_job.lease_generation,
                accepted_sources: wait.accepted_sources.clone(),
                expected_response_schema_digest: None,
                opaque_state_digest: Some(wait.encrypted_state.plaintext_digest.clone()),
                next_poll_at: wait.next_poll_at,
                poll_count: current_job_payload.poll_count,
                poll_limit: if wait.accepted_sources.contains(&WakeSource::Poll) {
                    features.max_poll_count
                } else {
                    0
                },
                callback_binding_digest: wait.callback_binding_digest.clone(),
                deadline: current_invocation.deadline,
            };
            let job = decide_wait(current_job, fence, database_now, wake.clone())
                .map_err(|_| InvocationError::InvalidOutcome)?;
            invocation.state = InvocationState::Deferred;
            job_payload.wake_contract = Some(wake);
            job_payload.encrypted_remote_state = Some(wait.encrypted_state.clone());
            job_payload.external_identity_digest = Some(wait.external_identity_digest.clone());
            if prior_resume_input_action.is_some()
                && prior_encrypted_remote_state.as_ref().is_some_and(|state| {
                    state.plaintext_digest == wait.encrypted_state.plaintext_digest
                })
            {
                job_payload.resume_input = prior_resume_input;
                job_payload.resume_input_artifact_link_id = prior_resume_input_artifact_link_id;
                job_payload.resume_input_action = prior_resume_input_action;
            }
            job_payload.resume_same_attempt = true;
            (job, None, None)
        }
        DispatchOutcome::InputRequired(request) => {
            let features = &current_invocation.payload.admission.implementation_features;
            if !features.input_required
                || request.input_task_id.kind() != ResourceKind::Interaction
                || !is_stable_code(&request.safe_prompt_key, MAX_SAFE_STAGE_KEY_BYTES)
                || request.response_schema.validate().is_err()
                || request.response_schema.canonical_digest != request.response_schema_digest
                || request.opaque_state_digest != request.encrypted_state.plaintext_digest
                || request.deadline <= database_now
                || request.deadline > current_invocation.deadline
            {
                return Err(InvocationError::UnsupportedOutcome);
            }
            request.encrypted_state.validate(
                features
                    .max_remote_state_bytes
                    .min(limits.maximum_remote_state_bytes()),
            )?;
            let wake = WakeContract {
                kind: WakeKind::RemoteInvocation,
                generation: current_job.lease_generation,
                accepted_sources: vec![WakeSource::Signal],
                expected_response_schema_digest: Some(request.response_schema_digest.clone()),
                opaque_state_digest: Some(request.opaque_state_digest.clone()),
                next_poll_at: None,
                poll_count: 0,
                poll_limit: 0,
                callback_binding_digest: None,
                deadline: request.deadline,
            };
            let job = decide_wait(current_job, fence, database_now, wake.clone())
                .map_err(|_| InvocationError::InvalidOutcome)?;
            let binding = CapabilityInputRequestBinding {
                input_task_id: request.input_task_id.clone(),
                interaction_kind: request.interaction_kind,
                safe_prompt_key: request.safe_prompt_key.clone(),
                response_schema: request.response_schema.clone(),
                response_schema_digest: request.response_schema_digest.clone(),
                eligible_principal_rule_digest: request.eligible_principal_rule_digest.clone(),
                exact_eligible_principal_id: request.exact_eligible_principal_id.clone(),
                opaque_state_digest: request.opaque_state_digest.clone(),
                wake_generation: current_job.lease_generation,
                deadline: request.deadline,
            };
            binding.validate(current_invocation.deadline)?;
            invocation.state = InvocationState::AwaitingInput;
            invocation.payload.input_task_id = Some(request.input_task_id.clone());
            job_payload.wake_contract = Some(wake);
            job_payload.encrypted_remote_state = Some(request.encrypted_state.clone());
            job_payload.external_identity_digest = Some(request.external_identity_digest.clone());
            job_payload.input_request = Some(binding);
            job_payload.resume_same_attempt = true;
            (job, None, Some(request.clone()))
        }
        DispatchOutcome::RetryableFailure { failure, retry_at } => {
            failure.validate()?;
            if failure.failure.retryability == Retryability::Never
                || current_invocation.payload.admission.attempt_limit <= current_job.attempt_count
                || (current_invocation.payload.admission.effect.risk_rank()
                    >= Effect::IdempotentWrite.risk_rank()
                    && current_invocation.payload.admission.idempotency
                        == insight_platform_contracts::CapabilityIdempotencyKind::None)
            {
                return Err(InvocationError::UnsupportedOutcome);
            }
            let job = decide_retry(current_job, fence, database_now, *retry_at)
                .map_err(|_| InvocationError::InvalidOutcome)?;
            invocation.state = InvocationState::RetryScheduled;
            invocation.retry_at = Some(*retry_at);
            (job, None, None)
        }
        DispatchOutcome::PermanentFailure(failure) => {
            failure.validate()?;
            let job = decide_terminal(current_job, fence, database_now, JobState::Failed)
                .map_err(|_| InvocationError::FirstWinnerLost)?;
            invocation.state = InvocationState::Failed;
            invocation.payload.failure = Some(failure.failure.clone());
            invocation.terminal_at = Some(database_now);
            job_payload.physical_outcome = Some(CapabilityPhysicalOutcomeEvidence::Failed {
                failure_digest: digest(&failure.failure)?,
            });
            (job, None, None)
        }
        DispatchOutcome::Uncertain(uncertain) => {
            let job = decide_reconciliation(current_job, fence, database_now)
                .map_err(|_| InvocationError::FirstWinnerLost)?;
            invocation.state = InvocationState::ReconciliationRequired;
            invocation.payload.reconciliation = Some(super::CapabilityReconciliationSnapshot {
                schema_version: 1,
                effect: current_invocation.payload.admission.effect,
                last_job_generation: current_job.lease_generation,
                external_identity_digest: Some(uncertain.external_identity_digest.clone()),
                observation_digest: uncertain.observation_digest.clone(),
                policy_path_digest: uncertain.policy_path_digest.clone(),
                manual: uncertain.manual,
            });
            job_payload.physical_outcome = Some(CapabilityPhysicalOutcomeEvidence::Uncertain {
                observation_digest: uncertain.observation_digest.clone(),
            });
            // A remote operation that needs reconciliation must keep its opaque recovery handle.
            // The handle remains encrypted and exact-attempt-bound; no plaintext backend state is
            // admitted into PostgreSQL.
            job_payload.encrypted_remote_state = prior_encrypted_remote_state;
            job_payload.external_identity_digest = Some(uncertain.external_identity_digest.clone());
            (job, None, None)
        }
    };
    invocation.version = increment(invocation.version)?;
    invocation.updated_at = database_now;
    invocation.validate()?;
    job_payload.validate_for(&invocation, &job)?;
    Ok(CapabilityOutcomeDecision {
        invocation,
        job,
        job_payload,
        output,
        input_request,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedCapabilityWake {
    pub invocation: CapabilityInvocationRecord,
    pub job: JobProjection,
    pub job_payload: CapabilityJobPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityWakeDisposition {
    Applied(Box<AppliedCapabilityWake>),
    Rejected,
}

#[derive(Debug, Clone)]
pub struct WakeCapabilityInvocation {
    pub audit: CapabilitySignalAudit,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub expected_generation: u64,
    pub source: WakeSource,
    pub callback_binding_digest: Option<Sha256Digest>,
}

impl WakeCapabilityInvocation {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), InvocationError> {
        self.audit.validate_at(now)?;
        if self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_generation == 0
            || !matches!(self.source, WakeSource::Callback | WakeSource::Poll)
            || (self.source == WakeSource::Callback) != self.callback_binding_digest.is_some()
        {
            return Err(InvocationError::InvalidCommand);
        }
        Ok(())
    }
}

pub fn decide_capability_wake(
    current_invocation: &CapabilityInvocationRecord,
    current_job: &JobProjection,
    current_job_payload: &CapabilityJobPayload,
    expected_generation: u64,
    source: WakeSource,
    callback_binding_digest: Option<&Sha256Digest>,
    database_now: DateTime<Utc>,
) -> Result<CapabilityWakeDisposition, InvocationError> {
    current_invocation.validate()?;
    current_job_payload.validate_for(current_invocation, current_job)?;
    if current_invocation.state != InvocationState::Deferred
        || current_invocation.payload.current_job_id.as_ref() != Some(&current_job.job_id)
    {
        return Ok(CapabilityWakeDisposition::Rejected);
    }
    let wake = current_job_payload
        .wake_contract
        .as_ref()
        .ok_or(InvocationError::InvalidWake)?;
    if source == WakeSource::Callback
        && wake.callback_binding_digest.as_ref() != callback_binding_digest
    {
        return Ok(CapabilityWakeDisposition::Rejected);
    }
    let job = match decide_wake(current_job, expected_generation, source, database_now) {
        Ok(job) => job,
        Err(_) => return Ok(CapabilityWakeDisposition::Rejected),
    };
    let mut invocation = current_invocation.clone();
    invocation.version = increment(invocation.version)?;
    invocation.state = InvocationState::InFlight;
    invocation.updated_at = database_now;
    let mut job_payload = current_job_payload.clone();
    job_payload.wake_contract = None;
    if source == WakeSource::Poll {
        job_payload.poll_count = job_payload
            .poll_count
            .checked_add(1)
            .ok_or(InvocationError::CounterOverflow)?;
    }
    invocation.validate()?;
    job_payload.validate_for(&invocation, &job)?;
    Ok(CapabilityWakeDisposition::Applied(Box::new(
        AppliedCapabilityWake {
            invocation,
            job,
            job_payload,
        },
    )))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityInputResponse {
    pub value_id: ResourceId,
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub value: ValueRef,
    pub artifact_link_id: Option<ResourceId>,
}

impl CapabilityInputResponse {
    pub fn exact_for(
        &self,
        invocation: &CapabilityInvocationRecord,
        expected_schema: &Sha256Digest,
        limits: InvocationCommandLimits,
    ) -> Result<ExactInvocationValueRef, InvocationError> {
        if self.value_id.kind() != ResourceKind::RunValue
            || &self.schema_digest != expected_schema
            || self.classification.rank() < invocation.payload.admission.input.classification.rank()
            || self
                .artifact_link_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ArtifactLink)
        {
            return Err(InvocationError::InvalidInputValue);
        }
        self.value
            .validate(limits.inline_value_limits())
            .map_err(|_| InvocationError::InvalidInputValue)?;
        let storage = match &self.value {
            ValueRef::Inline { value } => {
                let actual: Sha256Digest = insight_platform_contracts::canonical_digest(value)
                    .map_err(|_| InvocationError::Canonicalization)?
                    .parse()
                    .map_err(|_| InvocationError::Canonicalization)?;
                if actual != self.content_digest || self.artifact_link_id.is_some() {
                    return Err(InvocationError::InvalidInputValue);
                }
                InvocationValueStorage::Inline
            }
            ValueRef::Artifact { artifact } => {
                if artifact.content_digest() != &self.content_digest
                    || artifact.classification() != self.classification
                    || self.artifact_link_id.is_none()
                {
                    return Err(InvocationError::InvalidInputValue);
                }
                InvocationValueStorage::Artifact {
                    artifact: artifact.clone(),
                }
            }
        };
        let exact = ExactInvocationValueRef {
            schema_version: 1,
            value_id: self.value_id.clone(),
            run_id: invocation.run_id.clone(),
            producing_node_id: Some(invocation.node_execution_id.clone()),
            value_kind: "capability_input_response".to_owned(),
            classification: self.classification,
            schema_digest: expected_schema.clone(),
            content_digest: self.content_digest.clone(),
            storage,
        };
        exact.validate()?;
        Ok(exact)
    }
}

#[derive(Debug, Clone)]
pub struct CapabilityInputResponseDecision {
    pub invocation: CapabilityInvocationRecord,
    pub job: JobProjection,
    pub job_payload: CapabilityJobPayload,
    pub action: CapabilityInputAction,
    pub response: Option<ExactInvocationValueRef>,
}

#[derive(Debug, Clone)]
pub struct ResolveCapabilityInput {
    pub audit: CommandAudit,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub input_task_id: ResourceId,
    pub expected_invocation_version: u64,
    pub expected_job_version: u64,
    pub expected_wake_generation: u64,
    pub expected_task_generation: u64,
    pub expected_task_version: u64,
    pub eligible_principal_rule_digest: Sha256Digest,
    pub action: CapabilityInputAction,
    pub response: Option<CapabilityInputResponse>,
}

impl ResolveCapabilityInput {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), InvocationError> {
        self.audit
            .validate_at(now)
            .map_err(|_| InvocationError::InvalidAudit)?;
        if self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.input_task_id.kind() != ResourceKind::Interaction
            || self.expected_invocation_version == 0
            || self.expected_job_version == 0
            || self.expected_wake_generation == 0
            || self.expected_task_generation == 0
            || self.expected_task_version == 0
            || self.response.is_some() != (self.action == CapabilityInputAction::Accept)
        {
            return Err(InvocationError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CapabilityInputResolution<'a> {
    pub expected_generation: u64,
    pub action: CapabilityInputAction,
    pub response: Option<&'a CapabilityInputResponse>,
    pub database_now: DateTime<Utc>,
}

pub fn decide_input_response(
    current_invocation: &CapabilityInvocationRecord,
    current_job: &JobProjection,
    current_job_payload: &CapabilityJobPayload,
    resolution: CapabilityInputResolution<'_>,
    limits: InvocationCommandLimits,
) -> Result<CapabilityInputResponseDecision, InvocationError> {
    current_invocation.validate()?;
    current_job_payload.validate_for(current_invocation, current_job)?;
    let request = current_job_payload
        .input_request
        .as_ref()
        .ok_or(InvocationError::InvalidWake)?;
    if current_invocation.state != InvocationState::AwaitingInput
        || request.wake_generation != resolution.expected_generation
        || current_invocation.payload.input_task_id.as_ref() != Some(&request.input_task_id)
    {
        return Err(InvocationError::FirstWinnerLost);
    }
    let exact = match (resolution.action, resolution.response) {
        (CapabilityInputAction::Accept, Some(response)) => {
            if request.response_schema.profile
                == insight_platform_contracts::MCP_FORM_SCHEMA_PROFILE_ID
            {
                let ValueRef::Inline { value } = &response.value else {
                    return Err(InvocationError::InvalidInputValue);
                };
                request
                    .response_schema
                    .validate_mcp_form_instance(value)
                    .map_err(|_| InvocationError::InvalidInputValue)?;
            }
            Some(response.exact_for(current_invocation, &request.response_schema_digest, limits)?)
        }
        (CapabilityInputAction::Decline | CapabilityInputAction::Cancel, None) => None,
        _ => return Err(InvocationError::InvalidCommand),
    };
    let job = decide_wake(
        current_job,
        resolution.expected_generation,
        WakeSource::Signal,
        resolution.database_now,
    )
    .map_err(|_| InvocationError::FirstWinnerLost)?;
    let mut invocation = current_invocation.clone();
    invocation.version = increment(invocation.version)?;
    invocation.state = InvocationState::Ready;
    invocation.payload.input_task_id = None;
    invocation.updated_at = resolution.database_now;
    let mut job_payload = current_job_payload.clone();
    job_payload.wake_contract = None;
    job_payload.input_request = None;
    job_payload.resume_input = exact.clone();
    job_payload.resume_input_artifact_link_id = resolution
        .response
        .and_then(|value| value.artifact_link_id.clone());
    job_payload.resume_input_action = Some(resolution.action);
    invocation.validate()?;
    job_payload.validate_for(&invocation, &job)?;
    Ok(CapabilityInputResponseDecision {
        invocation,
        job,
        job_payload,
        action: resolution.action,
        response: exact,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityProgress {
    pub sequence: u32,
    pub stage_key: String,
    pub schema_digest: Sha256Digest,
    pub payload_digest: Sha256Digest,
    pub payload_bytes: u32,
}

#[derive(Debug, Clone)]
pub struct CapabilityProgressDecision {
    pub job: JobProjection,
    pub job_payload: CapabilityJobPayload,
    pub durability: CapabilityProgressDurability,
}

#[derive(Debug, Clone)]
pub struct RecordCapabilityProgress {
    pub audit: CapabilityWorkerAudit,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub expected_invocation_version: u64,
    pub fence: JobFence,
    pub progress: CapabilityProgress,
}

impl RecordCapabilityProgress {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), InvocationError> {
        self.audit.validate_at(now)?;
        if self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_invocation_version == 0
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
        {
            return Err(InvocationError::InvalidCommand);
        }
        Ok(())
    }
}

pub fn decide_progress(
    invocation: &CapabilityInvocationRecord,
    current_job: &JobProjection,
    current_job_payload: &CapabilityJobPayload,
    fence: &JobFence,
    progress: &CapabilityProgress,
    database_now: DateTime<Utc>,
    limits: InvocationCommandLimits,
) -> Result<CapabilityProgressDecision, InvocationError> {
    invocation.validate()?;
    current_job_payload.validate_for(invocation, current_job)?;
    let contract = &invocation.payload.admission.progress;
    if invocation.state != InvocationState::InFlight
        || contract.mode != CapabilityProgressMode::Events
        || contract.schema_digest.as_ref() != Some(&progress.schema_digest)
        || !is_stable_code(&progress.stage_key, MAX_SAFE_STAGE_KEY_BYTES)
        || progress.sequence != current_job_payload.progress.accepted_count + 1
        || progress.sequence > contract.max_events
        || progress.sequence > limits.maximum_progress_events()
        || progress.payload_bytes == 0
        || progress.payload_bytes > contract.max_bytes_per_event
        || progress.payload_bytes > limits.maximum_progress_event_bytes()
        || current_job_payload
            .progress
            .last_accepted_at
            .is_some_and(|last| {
                database_now - last
                    < Duration::milliseconds(
                        i64::try_from(contract.minimum_interval_milliseconds).unwrap_or(i64::MAX),
                    )
            })
    {
        return Err(InvocationError::InvalidProgress);
    }
    let job = decide_observation_update(current_job, fence, database_now)
        .map_err(|_| InvocationError::FirstWinnerLost)?;
    let mut job_payload = current_job_payload.clone();
    job_payload.progress.accepted_count = progress.sequence;
    job_payload.progress.last_sequence = Some(progress.sequence);
    job_payload.progress.last_accepted_at = Some(database_now);
    job_payload.progress.last_payload_digest = Some(progress.payload_digest.clone());
    job_payload.validate_for(invocation, &job)?;
    Ok(CapabilityProgressDecision {
        job,
        job_payload,
        durability: contract.durability,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityControlKind {
    Cancel,
    Timeout,
}

#[derive(Debug, Clone)]
pub struct ControlCapabilityInvocation {
    pub audit: CommandAudit,
    pub invocation_id: ResourceId,
    pub expected_invocation_version: u64,
    pub quota_entry_ids: Vec<ResourceId>,
    pub kind: CapabilityControlKind,
}

impl ControlCapabilityInvocation {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), InvocationError> {
        self.audit
            .validate_at(now)
            .map_err(|_| InvocationError::InvalidAudit)?;
        if self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.expected_invocation_version == 0
            || (!self.quota_entry_ids.is_empty()
                && (self.quota_entry_ids.len() != CAPABILITY_QUOTA_LINES
                    || self
                        .quota_entry_ids
                        .iter()
                        .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
                    || self.quota_entry_ids[0] == self.quota_entry_ids[1]))
        {
            return Err(InvocationError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CapabilityControlDecision {
    pub invocation: CapabilityInvocationRecord,
    pub job: Option<JobProjection>,
    pub job_payload: Option<CapabilityJobPayload>,
}

/// Advances only the logical Invocation control state for a backend-owned Job. The owning
/// backend controller consumes the resulting committed Event and is solely responsible for
/// changing the physical Job.
pub fn decide_detached_job_control(
    current: &CapabilityInvocationRecord,
    source_kind: DetachedSandboxSourceKind,
    terminal_or_active_job: &JobProjection,
    kind: CapabilityControlKind,
    database_now: DateTime<Utc>,
) -> Result<CapabilityInvocationRecord, InvocationError> {
    current.validate()?;
    terminal_or_active_job
        .validate()
        .map_err(|_| InvocationError::InvalidJob)?;
    if !source_kind.validates(current)
        || current.payload.current_job_id.as_ref() != Some(&terminal_or_active_job.job_id)
        || terminal_or_active_job.tenant_id != current.tenant_id
        || terminal_or_active_job.work_class != WorkClass::Sandbox
        || terminal_or_active_job.owner.owner_kind != ResourceKind::Job
        || terminal_or_active_job.owner.owner_id.uuid() != terminal_or_active_job.job_id.uuid()
        || matches!(
            current.state,
            InvocationState::Succeeded
                | InvocationState::Failed
                | InvocationState::Cancelled
                | InvocationState::TimedOut
                | InvocationState::ReconciliationRequired
                | InvocationState::Cancelling
        )
        || (kind == CapabilityControlKind::Timeout && database_now < current.deadline)
    {
        return Err(InvocationError::FirstWinnerLost);
    }
    let mut next = current.clone();
    // The Job may be unclaimed, leased, or executing. In every case the durable control Event is
    // delivered to the Sandbox controller, so the logical aggregate enters Cancelling first.
    next.state = InvocationState::Cancelling;
    next.version = increment(next.version)?;
    next.updated_at = database_now;
    next.validate()?;
    Ok(next)
}

pub fn decide_control(
    current_invocation: &CapabilityInvocationRecord,
    current_job: Option<&JobProjection>,
    current_job_payload: Option<&CapabilityJobPayload>,
    kind: CapabilityControlKind,
    database_now: DateTime<Utc>,
) -> Result<CapabilityControlDecision, InvocationError> {
    current_invocation.validate()?;
    if matches!(
        current_invocation.state,
        InvocationState::Succeeded
            | InvocationState::Failed
            | InvocationState::Cancelled
            | InvocationState::TimedOut
            | InvocationState::ReconciliationRequired
    ) || (kind == CapabilityControlKind::Timeout && database_now < current_invocation.deadline)
    {
        return Err(InvocationError::FirstWinnerLost);
    }
    let paired = match (current_job, current_job_payload) {
        (Some(job), Some(payload)) => {
            payload.validate_for(current_invocation, job)?;
            if current_invocation.payload.current_job_id.as_ref() != Some(&job.job_id) {
                return Err(InvocationError::InvalidJob);
            }
            Some((job, payload))
        }
        (None, None) if current_invocation.payload.current_job_id.is_none() => None,
        _ => return Err(InvocationError::InvalidJob),
    };
    let active = matches!(
        current_invocation.state,
        InvocationState::InFlight | InvocationState::Deferred | InvocationState::Cancelling
    );
    let write_effect = current_invocation.payload.admission.effect.risk_rank()
        >= Effect::IdempotentWrite.risk_rank();
    let mut invocation = current_invocation.clone();
    let mut next_job = paired.map(|(job, _)| job.clone());
    let mut next_payload = paired.map(|(_, payload)| payload.clone());
    if let Some(payload) = next_payload.as_mut() {
        payload.resume_same_attempt = false;
    }
    let can_enter_cancelling = current_invocation
        .payload
        .admission
        .implementation_features
        .cancellation
        && current_invocation.payload.admission.cancellation
            != insight_platform_contracts::CapabilityCancellationKind::Unsupported
        && paired.is_some_and(|(job, _)| {
            matches!(
                job.state,
                JobState::Leased | JobState::Running | JobState::Waiting | JobState::RetryScheduled
            )
        });
    if active && kind == CapabilityControlKind::Cancel && can_enter_cancelling {
        invocation.state = InvocationState::Cancelling;
        if let Some(job) = paired.map(|(job, _)| job) {
            next_job =
                Some(decide_owner_cancelling(job).map_err(|_| InvocationError::InvalidControl)?);
        }
        if let Some(payload) = next_payload.as_mut() {
            payload.wake_contract = None;
            payload.input_request = None;
        }
    } else if active && write_effect {
        let job = paired
            .map(|(job, _)| job)
            .ok_or(InvocationError::InvalidJob)?;
        next_job =
            Some(decide_owner_reconciliation(job).map_err(|_| InvocationError::InvalidControl)?);
        let observation_digest = domain_digest(
            "capability_control_uncertainty",
            &serde_json::json!({
                "invocation_id": current_invocation.invocation_id,
                "job_generation": job.lease_generation,
                "kind": match kind { CapabilityControlKind::Cancel => "cancel", CapabilityControlKind::Timeout => "timeout" },
            }),
        )?;
        invocation.state = InvocationState::ReconciliationRequired;
        invocation.payload.reconciliation = Some(super::CapabilityReconciliationSnapshot {
            schema_version: 1,
            effect: invocation.payload.admission.effect,
            last_job_generation: job.lease_generation.max(1),
            external_identity_digest: next_payload
                .as_ref()
                .and_then(|payload| payload.external_identity_digest.clone()),
            observation_digest: observation_digest.clone(),
            policy_path_digest: invocation
                .payload
                .admission
                .policies
                .canonical_digest
                .clone(),
            manual: true,
        });
        if let Some(payload) = next_payload.as_mut() {
            payload.physical_outcome =
                Some(CapabilityPhysicalOutcomeEvidence::Uncertain { observation_digest });
            payload.wake_contract = None;
            payload.input_request = None;
        }
    } else {
        let target = match kind {
            CapabilityControlKind::Cancel => JobState::Cancelled,
            CapabilityControlKind::Timeout => JobState::TimedOut,
        };
        if let Some(job) = paired.map(|(job, _)| job) {
            next_job = Some(
                decide_owner_terminal(job, target).map_err(|_| InvocationError::InvalidControl)?,
            );
        }
        invocation.state = match kind {
            CapabilityControlKind::Cancel => InvocationState::Cancelled,
            CapabilityControlKind::Timeout => InvocationState::TimedOut,
        };
        invocation.terminal_at = Some(database_now);
        if let Some(payload) = next_payload.as_mut() {
            payload.wake_contract = None;
            payload.input_request = None;
            payload.physical_outcome = Some(CapabilityPhysicalOutcomeEvidence::Cancelled {
                no_effect_proof_digest: None,
            });
        }
    }
    invocation.version = increment(invocation.version)?;
    invocation.updated_at = database_now;
    invocation.validate()?;
    if let (Some(job), Some(payload)) = (&next_job, &next_payload) {
        payload.validate_for(&invocation, job)?;
    }
    Ok(CapabilityControlDecision {
        invocation,
        job: next_job,
        job_payload: next_payload,
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ReconciliationResolution {
    Succeeded(CapabilityOutputValue),
    Failed(SafeBackendFailure),
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct ResolveCapabilityReconciliation {
    pub audit: CommandAudit,
    pub invocation_id: ResourceId,
    pub expected_invocation_version: u64,
    pub expected_job_version: u64,
    pub resolution: ReconciliationResolution,
}

impl ResolveCapabilityReconciliation {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), InvocationError> {
        self.audit
            .validate_at(now)
            .map_err(|_| InvocationError::InvalidAudit)?;
        if self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.expected_invocation_version == 0
            || self.expected_job_version == 0
        {
            return Err(InvocationError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityCancellationObservation {
    pub cancellation_observation_digest: Sha256Digest,
    pub external_identity_digest: Option<Sha256Digest>,
    pub no_effect_proof_digest: Option<Sha256Digest>,
    pub cleanup_deadline: Option<DateTime<Utc>>,
}

pub fn decide_cancellation_outcome(
    current_invocation: &CapabilityInvocationRecord,
    current_job: &JobProjection,
    current_job_payload: &CapabilityJobPayload,
    fence: Option<&JobFence>,
    observation: &CapabilityCancellationObservation,
    database_now: DateTime<Utc>,
) -> Result<CapabilityControlDecision, InvocationError> {
    current_invocation.validate()?;
    current_job_payload.validate_for(current_invocation, current_job)?;
    if current_invocation.state != InvocationState::Cancelling
        || current_job.state != JobState::Cancelling
        || current_invocation.payload.current_job_id.as_ref() != Some(&current_job.job_id)
    {
        return Err(InvocationError::FirstWinnerLost);
    }
    let uncertain_write = current_invocation.payload.admission.effect.risk_rank()
        >= Effect::IdempotentWrite.risk_rank()
        && observation.no_effect_proof_digest.is_none();
    let job = if uncertain_write {
        match fence {
            Some(fence) => match observation.cleanup_deadline {
                Some(cleanup_deadline) if database_now >= current_job.deadline => {
                    decide_cleanup_reconciliation(
                        current_job,
                        fence,
                        database_now,
                        cleanup_deadline,
                    )
                }
                None => decide_reconciliation(current_job, fence, database_now),
                Some(_) => decide_reconciliation(current_job, fence, database_now),
            },
            None if current_job.lease.is_none() => decide_owner_reconciliation(current_job),
            None => return Err(InvocationError::InvalidJob),
        }
    } else {
        match fence {
            Some(fence) => match observation.cleanup_deadline {
                Some(cleanup_deadline) if database_now >= current_job.deadline => {
                    decide_cleanup_terminal(
                        current_job,
                        fence,
                        database_now,
                        cleanup_deadline,
                        JobState::Cancelled,
                    )
                }
                None => decide_terminal(current_job, fence, database_now, JobState::Cancelled),
                Some(_) => decide_terminal(current_job, fence, database_now, JobState::Cancelled),
            },
            None if current_job.lease.is_none() => {
                decide_owner_terminal(current_job, JobState::Cancelled)
            }
            None => return Err(InvocationError::InvalidJob),
        }
    }
    .map_err(|_| InvocationError::FirstWinnerLost)?;
    let mut invocation = current_invocation.clone();
    let mut job_payload = current_job_payload.clone();
    invocation.version = increment(invocation.version)?;
    invocation.updated_at = database_now;
    if uncertain_write {
        let observation_digest = observation.cancellation_observation_digest.clone();
        invocation.state = InvocationState::ReconciliationRequired;
        invocation.payload.reconciliation = Some(super::CapabilityReconciliationSnapshot {
            schema_version: 1,
            effect: invocation.payload.admission.effect,
            last_job_generation: current_job.lease_generation.max(1),
            external_identity_digest: observation
                .external_identity_digest
                .clone()
                .or_else(|| current_job_payload.external_identity_digest.clone()),
            observation_digest: observation_digest.clone(),
            policy_path_digest: invocation
                .payload
                .admission
                .policies
                .canonical_digest
                .clone(),
            manual: true,
        });
        job_payload.physical_outcome =
            Some(CapabilityPhysicalOutcomeEvidence::Uncertain { observation_digest });
        job_payload.external_identity_digest = invocation
            .payload
            .reconciliation
            .as_ref()
            .and_then(|snapshot| snapshot.external_identity_digest.clone());
    } else {
        invocation.state = InvocationState::Cancelled;
        invocation.terminal_at = Some(database_now);
        job_payload.physical_outcome = Some(CapabilityPhysicalOutcomeEvidence::Cancelled {
            no_effect_proof_digest: observation.no_effect_proof_digest.clone(),
        });
    }
    invocation.validate()?;
    job_payload.validate_for(&invocation, &job)?;
    Ok(CapabilityControlDecision {
        invocation,
        job: Some(job),
        job_payload: Some(job_payload),
    })
}

#[derive(Debug, Clone)]
pub struct CommitCapabilityCancellationOutcome {
    pub audit: CapabilityWorkerAudit,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub expected_invocation_version: u64,
    pub fence: Option<JobFence>,
    pub quota_entry_ids: Vec<ResourceId>,
    pub cancellation_observation_digest: Sha256Digest,
    pub external_identity_digest: Option<Sha256Digest>,
    pub no_effect_proof_digest: Option<Sha256Digest>,
    /// Trusted, frozen end of the bounded post-deadline cleanup authority window.
    pub cleanup_deadline: Option<DateTime<Utc>>,
}

impl CommitCapabilityCancellationOutcome {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), InvocationError> {
        self.audit.validate_at(now)?;
        if self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_invocation_version == 0
            || self.fence.as_ref().is_some_and(|fence| {
                fence.expected_version == 0
                    || fence.lease_generation == 0
                    || fence.worker_process_generation_id != self.audit.worker_process_generation_id
            })
            || (self.external_identity_digest.is_some() && self.fence.is_none())
            || (self.cleanup_deadline.is_some() && self.fence.is_none())
            || self
                .cleanup_deadline
                .is_some_and(|cleanup_deadline| cleanup_deadline <= now)
            || (!self.quota_entry_ids.is_empty()
                && (self.quota_entry_ids.len() != CAPABILITY_QUOTA_LINES
                    || self
                        .quota_entry_ids
                        .iter()
                        .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
                    || self.quota_entry_ids[0] == self.quota_entry_ids[1]))
        {
            return Err(InvocationError::InvalidCommand);
        }
        Ok(())
    }
}

pub fn decide_reconciliation_resolution(
    current_invocation: &CapabilityInvocationRecord,
    current_job: &JobProjection,
    current_job_payload: &CapabilityJobPayload,
    resolution: &ReconciliationResolution,
    database_now: DateTime<Utc>,
    limits: InvocationCommandLimits,
) -> Result<CapabilityOutcomeDecision, InvocationError> {
    current_invocation.validate()?;
    current_job_payload.validate_for(current_invocation, current_job)?;
    if current_invocation.state != InvocationState::ReconciliationRequired
        || current_job.state != JobState::ReconciliationRequired
    {
        return Err(InvocationError::FirstWinnerLost);
    }
    let mut invocation = current_invocation.clone();
    let mut job_payload = current_job_payload.clone();
    let (target, output) = match resolution {
        ReconciliationResolution::Succeeded(output) => {
            let exact = output.exact_for(current_invocation, limits)?;
            invocation.state = InvocationState::Succeeded;
            invocation.output_value_id = Some(output.value_id.clone());
            invocation.payload.result = Some(super::CapabilityInvocationResult {
                schema_version: 1,
                result_digest: domain_digest(
                    "capability_reconciled_result",
                    &serde_json::json!({
                        "output": exact,
                        "validation_evidence_digest": output.validation_evidence_digest,
                    }),
                )?,
                output: exact,
            });
            job_payload.physical_outcome = Some(CapabilityPhysicalOutcomeEvidence::Completed {
                output_value_id: output.value_id.clone(),
                output_content_digest: output.content_digest.clone(),
                validation_evidence_digest: output.validation_evidence_digest.clone(),
            });
            (JobState::Succeeded, Some(output.clone()))
        }
        ReconciliationResolution::Failed(failure) => {
            failure.validate()?;
            invocation.state = InvocationState::Failed;
            invocation.payload.failure = Some(failure.failure.clone());
            job_payload.physical_outcome = Some(CapabilityPhysicalOutcomeEvidence::Failed {
                failure_digest: digest(&failure.failure)?,
            });
            (JobState::Failed, None)
        }
        ReconciliationResolution::Cancelled => {
            invocation.state = InvocationState::Cancelled;
            job_payload.physical_outcome = Some(CapabilityPhysicalOutcomeEvidence::Cancelled {
                no_effect_proof_digest: None,
            });
            (JobState::Cancelled, None)
        }
    };
    let job =
        decide_owner_terminal(current_job, target).map_err(|_| InvocationError::InvalidControl)?;
    invocation.version = increment(invocation.version)?;
    invocation.payload.reconciliation = None;
    invocation.terminal_at = Some(database_now);
    invocation.updated_at = database_now;
    invocation.validate()?;
    job_payload.validate_for(&invocation, &job)?;
    Ok(CapabilityOutcomeDecision {
        invocation,
        job,
        job_payload,
        output,
        input_request: None,
    })
}

fn increment(value: u64) -> Result<u64, InvocationError> {
    value.checked_add(1).ok_or(InvocationError::CounterOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CapabilityAdmissionSnapshot, CapabilityInvocationPayload, InvocationOrigin,
        InvocationPolicyDecision, InvocationPolicyDecisionBundle, InvocationPolicyDisposition,
        InvocationSelectionEvidence, McpCapabilityRuntimeBinding,
    };
    use insight_platform_contracts::{
        CapabilityArtifactContract, CapabilityBackendFeatures, CapabilityCancellationKind,
        CapabilityDataFlowPolicy, CapabilityIdempotencyKind, CapabilityInterfaceLimits,
        CapabilityProgressContract, ExactDeploymentRef, ExactPolicyBinding, ExactVersionRef,
        FailureClass, FailureCode, FailureSource, Permission, PermissionSet, PlatformFailureCode,
        PrincipalKind, PrincipalSnapshot,
    };

    fn id(value: &str) -> ResourceId {
        value.parse().unwrap()
    }

    fn hash(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn exact(value: &str, character: char) -> ExactVersionRef {
        ExactVersionRef::new(id(value), hash(character)).unwrap()
    }

    fn ready_invocation(
        now: DateTime<Utc>,
        effect: Effect,
        idempotency: CapabilityIdempotencyKind,
    ) -> CapabilityInvocationRecord {
        let tenant_id = id("ten_0198f1c8-32e4-75e1-a9e8-d95ca0f40001");
        let principal_id = id("prn_0198f1c8-32e4-75e1-a9e8-d95ca0f40002");
        let run_id = id("run_0198f1c8-32e4-75e1-a9e8-d95ca0f40003");
        let node_id = id("nod_0198f1c8-32e4-75e1-a9e8-d95ca0f40004");
        let invocation_id = id("inv_0198f1c8-32e4-75e1-a9e8-d95ca0f40005");
        let deployment =
            ExactDeploymentRef::new(id("cdep_0198f1c8-32e4-75e1-a9e8-d95ca0f40006"), hash('a'))
                .unwrap();
        let policy = exact("prev_0198f1c8-32e4-75e1-a9e8-d95ca0f40007", 'b');
        let policies = InvocationPolicyDecisionBundle::build(
            vec![InvocationPolicyDecision {
                policy: policy.clone(),
                disposition: InvocationPolicyDisposition::Allowed,
                evidence_digest: hash('c'),
            }],
            None,
        )
        .unwrap();
        let input = ExactInvocationValueRef {
            schema_version: 1,
            value_id: id("val_0198f1c8-32e4-75e1-a9e8-d95ca0f40008"),
            run_id: run_id.clone(),
            producing_node_id: None,
            value_kind: "run_input".to_owned(),
            classification: DataClassification::Internal,
            schema_digest: hash('d'),
            content_digest: hash('e'),
            storage: InvocationValueStorage::Inline,
        };
        let principal = PrincipalSnapshot::build(
            tenant_id.clone(),
            principal_id,
            PrincipalKind::AgentRunner,
            PermissionSet::new(vec![Permission::CapabilityInvoke]).unwrap(),
            1,
            1,
            1,
        )
        .unwrap();
        let mut admission = CapabilityAdmissionSnapshot {
            schema_version: 1,
            origin_key: InvocationOrigin::PlanNode {
                node_execution_id: node_id.clone(),
            },
            slot_id: "capability".to_owned(),
            slot_binding_digest: hash('f'),
            run_bindings_digest: hash('1'),
            selection_policy: ExactPolicyBinding {
                deployment: ExactDeploymentRef::new(
                    id("pdep_0198f1c8-32e4-75e1-a9e8-d95ca0f40020"),
                    hash('c'),
                )
                .unwrap(),
                revision: policy,
            },
            selection_evidence: InvocationSelectionEvidence::build(
                std::slice::from_ref(&deployment),
                0,
                hash('2'),
            )
            .unwrap(),
            deployment: deployment.clone(),
            interface: exact("cirev_0198f1c8-32e4-75e1-a9e8-d95ca0f40009", '3'),
            capability_name: "fixture.http".parse().unwrap(),
            implementation: exact("cimp_0198f1c8-32e4-75e1-a9e8-d95ca0f4000a", '4'),
            backend_kind: CapabilityBackendKind::Http,
            backend_contract_digest: hash('5'),
            mcp_runtime: None,
            input: input.clone(),
            input_artifact_link_id: None,
            effect,
            idempotency,
            cancellation: CapabilityCancellationKind::BestEffort,
            progress: CapabilityProgressContract {
                mode: CapabilityProgressMode::Events,
                schema_digest: Some(hash('6')),
                max_events: 8,
                max_bytes_per_event: 4_096,
                minimum_interval_milliseconds: 1,
                durability: CapabilityProgressDurability::CoarseDurable,
            },
            implementation_features: CapabilityBackendFeatures {
                deferred: true,
                input_required: true,
                callback: true,
                poll: true,
                progress: true,
                cancellation: true,
                max_remote_state_bytes: 4_096,
                max_poll_count: 8,
            },
            input_schema_digest: input.schema_digest.clone(),
            output_schema_digest: hash('7'),
            error_schema_digest: hash('8'),
            artifact_contract: CapabilityArtifactContract { ports: vec![] },
            data_flow_policy: CapabilityDataFlowPolicy {
                maximum_input_classification: DataClassification::Restricted,
                maximum_output_classification: DataClassification::Restricted,
                allowed_regions: vec!["global".parse().unwrap()],
                declassification_policy: None,
            },
            interface_limits: CapabilityInterfaceLimits {
                maximum_input_bytes: 1_048_576,
                maximum_output_bytes: 1_048_576,
                maximum_artifacts: 0,
                maximum_execution_milliseconds: 60_000,
            },
            policies,
            principal,
            effect_key_digest: hash('9'),
            idempotency_key_digest: hash('a'),
            attempt_limit: 3,
            retry_backoff_milliseconds: 100,
            deadline: now + Duration::minutes(5),
            canonical_digest: hash('b'),
        };
        admission.canonical_digest =
            crate::digest_without_field(&admission, "canonical_digest").unwrap();
        let record = CapabilityInvocationRecord {
            tenant_id,
            invocation_id,
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            run_id,
            node_execution_id: node_id.clone(),
            owner_kind: ResourceKind::NodeExecution,
            owner_id: node_id,
            logical_key: admission.origin_key.logical_key(),
            deployment_id: deployment.deployment_id,
            input_value_id: input.value_id,
            output_value_id: None,
            effect_key_digest: admission.effect_key_digest.clone(),
            state: InvocationState::Ready,
            version: 1,
            payload: CapabilityInvocationPayload {
                schema_version: 1,
                admission,
                current_job_id: None,
                approval_task_id: None,
                input_task_id: None,
                detached_pending: None,
                result: None,
                failure: None,
                reconciliation: None,
            },
            deadline: now + Duration::minutes(5),
            retry_at: None,
            started_at: None,
            terminal_at: None,
            created_at: now,
            updated_at: now,
        };
        record.validate().unwrap();
        record
    }

    fn managed_mcp_invocation(
        now: DateTime<Utc>,
        effect: Effect,
        idempotency: CapabilityIdempotencyKind,
    ) -> CapabilityInvocationRecord {
        let mut invocation = ready_invocation(now, effect, idempotency);
        invocation.payload.admission.backend_kind = CapabilityBackendKind::Mcp;
        invocation.payload.admission.mcp_runtime = Some(McpCapabilityRuntimeBinding {
            schema_version: 1,
            mcp_operation_id: id("mop_0198f1c8-32e4-75e1-a9e8-d95ca0f40101"),
            mcp_deployment: ExactDeploymentRef::new(
                id("mcdep_0198f1c8-32e4-75e1-a9e8-d95ca0f40102"),
                hash('1'),
            )
            .unwrap(),
            discovery_snapshot_id: id("mdsc_0198f1c8-32e4-75e1-a9e8-d95ca0f40103"),
            discovery_snapshot_digest: hash('2'),
            authorization_binding_id: id("mab_0198f1c8-32e4-75e1-a9e8-d95ca0f40104"),
            authorization_generation: 3,
            authorization_context_digest: hash('3'),
            principal_id: invocation.payload.admission.principal.principal_id.clone(),
        });
        invocation.payload.admission.canonical_digest =
            crate::digest_without_field(&invocation.payload.admission, "canonical_digest").unwrap();
        invocation.validate().unwrap();
        invocation
    }

    fn audit(invocation: &CapabilityInvocationRecord, now: DateTime<Utc>) -> CommandAudit {
        CommandAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: invocation.tenant_id.clone(),
            principal_id: invocation.payload.admission.principal.principal_id.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            receipt_id: id("rcp_0198f1c8-32e4-75e1-a9e8-d95ca0f4000b"),
            event_id: id("evt_0198f1c8-32e4-75e1-a9e8-d95ca0f4000c"),
            outbox_id: id("obx_0198f1c8-32e4-75e1-a9e8-d95ca0f4000d"),
            idempotency_key_digest: hash('c'),
            request_digest: hash('d'),
            receipt_expires_at: now + Duration::hours(1),
        }
    }

    fn prepared_and_started(
        now: DateTime<Utc>,
        effect: Effect,
        idempotency: CapabilityIdempotencyKind,
    ) -> (
        CapabilityInvocationRecord,
        JobProjection,
        CapabilityJobPayload,
        JobFence,
    ) {
        prepared_and_started_with_limit(now, effect, idempotency, 3)
    }

    #[test]
    fn sandbox_uses_one_detached_shared_job_and_generic_dispatch_is_rejected() {
        let now = DateTime::parse_from_rfc3339("2026-08-10T04:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut invocation =
            ready_invocation(now, Effect::Pure, CapabilityIdempotencyKind::Intrinsic);
        invocation.payload.admission.backend_kind = CapabilityBackendKind::Sandbox;
        invocation.payload.admission.canonical_digest =
            crate::digest_without_field(&invocation.payload.admission, "canonical_digest").unwrap();
        invocation.validate().unwrap();
        let job_id = id("job_0198f1c8-32e4-75e1-a9e8-d95ca0f40031");
        assert!(decide_prepare_dispatch(
            &invocation,
            &PrepareCapabilityDispatch {
                audit: audit(&invocation, now),
                invocation_id: invocation.invocation_id.clone(),
                expected_invocation_version: 1,
                job_id: job_id.clone(),
                scheduled_at: now,
            },
            now,
        )
        .is_err());

        let deferred = decide_defer_to_sandbox(
            &invocation,
            DetachedSandboxSourceKind::SandboxCapability,
            1,
            &job_id,
            1,
            None,
            now,
        )
        .unwrap();
        assert_eq!(deferred.state, InvocationState::Deferred);
        assert_eq!(deferred.payload.current_job_id.as_ref(), Some(&job_id));
        assert_eq!(deferred.version, 2);

        let sandbox_owner = ResourceId::from_uuid_v7(ResourceKind::Job, job_id.uuid()).unwrap();
        let terminal_job = JobProjection {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: deferred.tenant_id.clone(),
            job_id: job_id.clone(),
            work_class: WorkClass::Sandbox,
            owner: JobOwnerRef {
                owner_id: sandbox_owner,
                owner_kind: ResourceKind::Job,
            },
            state: JobState::Succeeded,
            version: 7,
            attempt_count: 1,
            attempt_limit: 1,
            lease_generation: 1,
            lease: None,
            scheduled_at: now,
            retry_at: None,
            wake: None,
            deadline: deferred.deadline,
        };
        let value = serde_json::json!({"answer": 42});
        let content_digest = insight_platform_contracts::canonical_digest(&value)
            .unwrap()
            .parse()
            .unwrap();
        let output = CapabilityOutputValue {
            value_id: ResourceId::from_uuid_v7(ResourceKind::RunValue, job_id.uuid()).unwrap(),
            classification: DataClassification::Internal,
            schema_digest: deferred.payload.admission.output_schema_digest.clone(),
            content_digest,
            value: ValueRef::Inline { value },
            artifact_link_id: None,
            validation_evidence_digest: hash('e'),
        };
        let merged = decide_detached_job_outcome(
            &deferred,
            DetachedSandboxSourceKind::SandboxCapability,
            &terminal_job,
            1,
            &DetachedCapabilityJobOutcome::Completed(output.clone()),
            now + Duration::seconds(1),
            InvocationCommandLimits::new(3).unwrap(),
        )
        .unwrap();
        assert_eq!(merged.invocation.state, InvocationState::Succeeded);
        assert_eq!(merged.output, Some(output));
        assert_eq!(terminal_job.state, JobState::Succeeded);
        assert_eq!(terminal_job.version, 7);
    }

    #[test]
    fn sandbox_retry_admission_replaces_only_the_current_job_binding() {
        let now = DateTime::parse_from_rfc3339("2026-08-10T04:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut invocation =
            ready_invocation(now, Effect::Pure, CapabilityIdempotencyKind::Intrinsic);
        invocation.payload.admission.backend_kind = CapabilityBackendKind::Sandbox;
        invocation.payload.admission.canonical_digest =
            crate::digest_without_field(&invocation.payload.admission, "canonical_digest").unwrap();
        let first_job_id = id("job_0198f1c8-32e4-75e1-a9e8-d95ca0f40032");
        let deferred = decide_defer_to_sandbox(
            &invocation,
            DetachedSandboxSourceKind::SandboxCapability,
            1,
            &first_job_id,
            1,
            None,
            now,
        )
        .unwrap();
        let first_owner = ResourceId::from_uuid_v7(ResourceKind::Job, first_job_id.uuid()).unwrap();
        let first_terminal = JobProjection {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: deferred.tenant_id.clone(),
            job_id: first_job_id.clone(),
            work_class: WorkClass::Sandbox,
            owner: JobOwnerRef {
                owner_id: first_owner,
                owner_kind: ResourceKind::Job,
            },
            state: JobState::Failed,
            version: 5,
            attempt_count: 1,
            attempt_limit: 1,
            lease_generation: 1,
            lease: None,
            scheduled_at: now,
            retry_at: None,
            wake: None,
            deadline: deferred.deadline,
        };
        let retry_at = now + Duration::seconds(2);
        let scheduled = decide_detached_job_outcome(
            &deferred,
            DetachedSandboxSourceKind::SandboxCapability,
            &first_terminal,
            1,
            &DetachedCapabilityJobOutcome::RetryableFailure {
                failure: SafeBackendFailure {
                    failure: Failure {
                        code: FailureCode::Platform {
                            code: PlatformFailureCode::CapabilityFailed,
                        },
                        class: FailureClass::External,
                        retryability: Retryability::SafeWithinPolicy,
                        safe_message: Some("temporary sandbox failure".to_owned()),
                        details_ref: None,
                        source: FailureSource::Capability,
                    },
                    evidence_digest: hash('e'),
                },
                retry_at,
            },
            now + Duration::seconds(1),
            InvocationCommandLimits::new(3).unwrap(),
        )
        .unwrap();
        assert_eq!(scheduled.invocation.state, InvocationState::RetryScheduled);
        let second_job_id = id("job_0198f1c8-32e4-75e1-a9e8-d95ca0f40033");
        let second = decide_defer_to_sandbox(
            &scheduled.invocation,
            DetachedSandboxSourceKind::SandboxCapability,
            scheduled.invocation.version,
            &second_job_id,
            2,
            Some(PreviousDetachedSandboxJob {
                job: &first_terminal,
                attempt_no: 1,
            }),
            retry_at,
        )
        .unwrap();
        assert_eq!(second.state, InvocationState::Deferred);
        assert_eq!(second.payload.current_job_id.as_ref(), Some(&second_job_id));
        assert_eq!(first_terminal.state, JobState::Failed);
        assert_eq!(first_terminal.version, 5);
        assert!(decide_defer_to_sandbox(
            &scheduled.invocation,
            DetachedSandboxSourceKind::SandboxCapability,
            scheduled.invocation.version,
            &second_job_id,
            3,
            Some(PreviousDetachedSandboxJob {
                job: &first_terminal,
                attempt_no: 1,
            }),
            retry_at,
        )
        .is_err());
    }

    #[test]
    fn sandbox_control_changes_only_the_logical_owner_until_terminal_evidence() {
        let now = DateTime::parse_from_rfc3339("2026-08-10T04:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut invocation =
            ready_invocation(now, Effect::Pure, CapabilityIdempotencyKind::Intrinsic);
        invocation.payload.admission.backend_kind = CapabilityBackendKind::Sandbox;
        invocation.payload.admission.canonical_digest =
            crate::digest_without_field(&invocation.payload.admission, "canonical_digest").unwrap();
        let job_id = id("job_0198f1c8-32e4-75e1-a9e8-d95ca0f40034");
        let deferred = decide_defer_to_sandbox(
            &invocation,
            DetachedSandboxSourceKind::SandboxCapability,
            1,
            &job_id,
            1,
            None,
            now,
        )
        .unwrap();
        let sandbox_owner = ResourceId::from_uuid_v7(ResourceKind::Job, job_id.uuid()).unwrap();
        let active_job = JobProjection {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: deferred.tenant_id.clone(),
            job_id,
            work_class: WorkClass::Sandbox,
            owner: JobOwnerRef {
                owner_id: sandbox_owner,
                owner_kind: ResourceKind::Job,
            },
            state: JobState::Ready,
            version: 1,
            attempt_count: 0,
            attempt_limit: 1,
            lease_generation: 0,
            lease: None,
            scheduled_at: now,
            retry_at: None,
            wake: None,
            deadline: deferred.deadline,
        };
        let cancelling = decide_detached_job_control(
            &deferred,
            DetachedSandboxSourceKind::SandboxCapability,
            &active_job,
            CapabilityControlKind::Cancel,
            now + Duration::milliseconds(1),
        )
        .unwrap();
        assert_eq!(cancelling.state, InvocationState::Cancelling);
        assert_eq!(active_job.state, JobState::Ready);
        assert_eq!(active_job.version, 1);
        assert!(decide_detached_job_control(
            &deferred,
            DetachedSandboxSourceKind::SandboxCapability,
            &active_job,
            CapabilityControlKind::Timeout,
            deferred.deadline - Duration::milliseconds(1),
        )
        .is_err());
    }

    #[test]
    fn managed_mcp_must_use_its_explicit_detached_sandbox_source() {
        let now = DateTime::parse_from_rfc3339("2026-08-10T04:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let invocation =
            managed_mcp_invocation(now, Effect::Pure, CapabilityIdempotencyKind::Intrinsic);
        let job_id = id("job_0198f1c8-32e4-75e1-a9e8-d95ca0f40105");
        assert!(decide_defer_to_sandbox(
            &invocation,
            DetachedSandboxSourceKind::SandboxCapability,
            invocation.version,
            &job_id,
            1,
            None,
            now,
        )
        .is_err());
        let deferred = decide_defer_to_sandbox(
            &invocation,
            DetachedSandboxSourceKind::ManagedMcp,
            invocation.version,
            &job_id,
            1,
            None,
            now,
        )
        .unwrap();
        let sandbox_owner = ResourceId::from_uuid_v7(ResourceKind::Job, job_id.uuid()).unwrap();
        let active_job = JobProjection {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: deferred.tenant_id.clone(),
            job_id,
            work_class: WorkClass::Sandbox,
            owner: JobOwnerRef {
                owner_id: sandbox_owner,
                owner_kind: ResourceKind::Job,
            },
            state: JobState::Ready,
            version: 1,
            attempt_count: 0,
            attempt_limit: 1,
            lease_generation: 0,
            lease: None,
            scheduled_at: now,
            retry_at: None,
            wake: None,
            deadline: deferred.deadline,
        };
        assert!(decide_detached_job_control(
            &deferred,
            DetachedSandboxSourceKind::SandboxCapability,
            &active_job,
            CapabilityControlKind::Cancel,
            now + Duration::milliseconds(1),
        )
        .is_err());
        let cancelling = decide_detached_job_control(
            &deferred,
            DetachedSandboxSourceKind::ManagedMcp,
            &active_job,
            CapabilityControlKind::Cancel,
            now + Duration::milliseconds(1),
        )
        .unwrap();
        assert_eq!(cancelling.state, InvocationState::Cancelling);
        assert_eq!(active_job.state, JobState::Ready);
    }

    #[test]
    fn managed_mcp_remote_task_terminates_one_job_before_admitting_the_next() {
        let now = DateTime::parse_from_rfc3339("2026-08-10T04:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let invocation =
            managed_mcp_invocation(now, Effect::ReadOnly, CapabilityIdempotencyKind::Intrinsic);
        let first_job_id = id("job_0198f1c8-32e4-75e1-a9e8-d95ca0f40106");
        let deferred = decide_defer_to_sandbox(
            &invocation,
            DetachedSandboxSourceKind::ManagedMcp,
            invocation.version,
            &first_job_id,
            1,
            None,
            now,
        )
        .unwrap();
        let terminal = JobProjection {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: deferred.tenant_id.clone(),
            job_id: first_job_id.clone(),
            work_class: WorkClass::Sandbox,
            owner: JobOwnerRef {
                owner_id: ResourceId::from_uuid_v7(ResourceKind::Job, first_job_id.uuid()).unwrap(),
                owner_kind: ResourceKind::Job,
            },
            state: JobState::Succeeded,
            version: 7,
            attempt_count: 1,
            attempt_limit: 1,
            lease_generation: 4,
            lease: None,
            scheduled_at: now,
            retry_at: None,
            wake: None,
            deadline: deferred.deadline,
        };
        let wait = RemoteWait {
            encrypted_state: EncryptedRemoteState {
                scheme: "aes256_gcm_v1".to_owned(),
                key_id: "managed-mcp-state-1".to_owned(),
                key_reference_digest: hash('5'),
                ciphertext: "ZW5jcnlwdGVk".to_owned(),
                plaintext_digest: hash('6'),
            },
            external_identity_digest: hash('7'),
            accepted_sources: vec![WakeSource::Poll],
            next_poll_at: Some(now + Duration::seconds(2)),
            callback_binding_digest: None,
        };
        let waiting = decide_detached_job_outcome(
            &deferred,
            DetachedSandboxSourceKind::ManagedMcp,
            &terminal,
            1,
            &DetachedCapabilityJobOutcome::Deferred {
                wait: wait.clone(),
                poll_count: 0,
            },
            now + Duration::seconds(1),
            InvocationCommandLimits::new(3).unwrap(),
        )
        .unwrap()
        .invocation;
        assert_eq!(waiting.state, InvocationState::Deferred);
        assert!(matches!(
            waiting.payload.detached_pending,
            Some(CapabilityDetachedPending::RemoteTask { poll_count: 0, .. })
        ));
        let second_job_id = id("job_0198f1c8-32e4-75e1-a9e8-d95ca0f40107");
        assert!(decide_defer_to_sandbox(
            &waiting,
            DetachedSandboxSourceKind::ManagedMcp,
            waiting.version,
            &second_job_id,
            2,
            Some(PreviousDetachedSandboxJob {
                job: &terminal,
                attempt_no: 1,
            }),
            now + Duration::seconds(1),
        )
        .is_err());
        let continued = decide_defer_to_sandbox(
            &waiting,
            DetachedSandboxSourceKind::ManagedMcp,
            waiting.version,
            &second_job_id,
            2,
            Some(PreviousDetachedSandboxJob {
                job: &terminal,
                attempt_no: 1,
            }),
            now + Duration::seconds(2),
        )
        .unwrap();
        assert_eq!(
            continued.payload.current_job_id.as_ref(),
            Some(&second_job_id)
        );
        assert!(continued.payload.detached_pending.is_none());
        assert_eq!(terminal.state, JobState::Succeeded);
        assert_eq!(terminal.version, 7);
    }

    fn prepared_and_started_with_limit(
        now: DateTime<Utc>,
        effect: Effect,
        idempotency: CapabilityIdempotencyKind,
        attempt_limit: u32,
    ) -> (
        CapabilityInvocationRecord,
        JobProjection,
        CapabilityJobPayload,
        JobFence,
    ) {
        let mut invocation = ready_invocation(now, effect, idempotency);
        invocation.payload.admission.attempt_limit = attempt_limit;
        invocation.payload.admission.canonical_digest =
            crate::digest_without_field(&invocation.payload.admission, "canonical_digest").unwrap();
        invocation.validate().unwrap();
        let prepared = decide_prepare_dispatch(
            &invocation,
            &PrepareCapabilityDispatch {
                audit: audit(&invocation, now),
                invocation_id: invocation.invocation_id.clone(),
                expected_invocation_version: 1,
                job_id: id("job_0198f1c8-32e4-75e1-a9e8-d95ca0f4000e"),
                scheduled_at: now,
            },
            now,
        )
        .unwrap();
        let worker = id("wrk_0198f1c8-32e4-75e1-a9e8-d95ca0f4000f");
        let token = hash('e');
        let started = decide_start_dispatch(
            &prepared.invocation,
            &prepared.job,
            &prepared.job_payload,
            worker.clone(),
            token.clone(),
            LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 60_000,
            },
            now + Duration::milliseconds(1),
        )
        .unwrap();
        let fence = JobFence {
            expected_version: started.job.version,
            worker_process_generation_id: worker,
            lease_generation: started.job.lease_generation,
            token_digest: token,
        };
        (started.invocation, started.job, started.job_payload, fence)
    }

    fn inline_value(value: serde_json::Value) -> (ValueRef, Sha256Digest) {
        let digest: Sha256Digest = insight_platform_contracts::canonical_digest(&value)
            .unwrap()
            .parse()
            .unwrap();
        (ValueRef::Inline { value }, digest)
    }

    #[test]
    fn synchronous_completion_is_fenced_and_terminal_once() {
        let now = DateTime::parse_from_rfc3339("2026-08-10T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (invocation, job, payload, fence) =
            prepared_and_started(now, Effect::ReadOnly, CapabilityIdempotencyKind::Intrinsic);
        let (value, content_digest) = inline_value(serde_json::json!({"answer": 42}));
        let outcome = DispatchOutcome::Completed(CapabilityOutputValue {
            value_id: id("val_0198f1c8-32e4-75e1-a9e8-d95ca0f40010"),
            classification: DataClassification::Internal,
            schema_digest: invocation.payload.admission.output_schema_digest.clone(),
            content_digest,
            value,
            artifact_link_id: None,
            validation_evidence_digest: hash('f'),
        });
        let completed = decide_dispatch_outcome(
            &invocation,
            &job,
            &payload,
            &fence,
            &outcome,
            now + Duration::milliseconds(2),
            InvocationCommandLimits::new(4).unwrap(),
        )
        .unwrap();
        assert_eq!(completed.invocation.state, InvocationState::Succeeded);
        assert_eq!(completed.job.state, JobState::Succeeded);
        assert!(completed.invocation.payload.result.is_some());
        assert!(decide_dispatch_outcome(
            &completed.invocation,
            &completed.job,
            &completed.job_payload,
            &fence,
            &outcome,
            now + Duration::milliseconds(3),
            InvocationCommandLimits::new(4).unwrap(),
        )
        .is_err());
    }

    #[test]
    fn deferred_callback_and_input_resume_preserve_one_job() {
        let now = DateTime::parse_from_rfc3339("2026-08-10T02:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (invocation, job, payload, fence) = prepared_and_started_with_limit(
            now,
            Effect::NonIdempotentWrite,
            CapabilityIdempotencyKind::None,
            1,
        );
        assert_eq!(job.attempt_count, 1);
        assert_eq!(job.attempt_limit, 1);
        let remote_state = EncryptedRemoteState {
            scheme: "aes256_gcm_v1".to_owned(),
            key_id: "remote-state-key-1".to_owned(),
            key_reference_digest: hash('1'),
            ciphertext: "ZW5jcnlwdGVk".to_owned(),
            plaintext_digest: hash('2'),
        };
        let callback_binding = hash('3');
        let deferred = decide_dispatch_outcome(
            &invocation,
            &job,
            &payload,
            &fence,
            &DispatchOutcome::Deferred(RemoteWait {
                encrypted_state: remote_state.clone(),
                external_identity_digest: hash('4'),
                accepted_sources: vec![WakeSource::Callback, WakeSource::Poll],
                next_poll_at: Some(now + Duration::seconds(1)),
                callback_binding_digest: Some(callback_binding.clone()),
            }),
            now + Duration::milliseconds(2),
            InvocationCommandLimits::new(4).unwrap(),
        )
        .unwrap();
        assert_eq!(deferred.invocation.state, InvocationState::Deferred);
        assert_eq!(deferred.job.state, JobState::Waiting);
        let mut exhausted_payload = payload.clone();
        exhausted_payload.poll_count = invocation
            .payload
            .admission
            .implementation_features
            .max_poll_count;
        assert!(matches!(
            decide_dispatch_outcome(
                &invocation,
                &job,
                &exhausted_payload,
                &fence,
                &DispatchOutcome::Deferred(RemoteWait {
                    encrypted_state: remote_state.clone(),
                    external_identity_digest: hash('4'),
                    accepted_sources: vec![WakeSource::Poll],
                    next_poll_at: Some(now + Duration::seconds(1)),
                    callback_binding_digest: None,
                }),
                now + Duration::milliseconds(2),
                InvocationCommandLimits::new(4).unwrap(),
            ),
            Err(InvocationError::UnsupportedOutcome)
        ));
        let generation = deferred.job.lease_generation;
        assert_eq!(
            decide_capability_wake(
                &deferred.invocation,
                &deferred.job,
                &deferred.job_payload,
                generation,
                WakeSource::Callback,
                Some(&hash('9')),
                now + Duration::milliseconds(3),
            )
            .unwrap(),
            CapabilityWakeDisposition::Rejected
        );
        let CapabilityWakeDisposition::Applied(applied) = decide_capability_wake(
            &deferred.invocation,
            &deferred.job,
            &deferred.job_payload,
            generation,
            WakeSource::Callback,
            Some(&callback_binding),
            now + Duration::milliseconds(3),
        )
        .unwrap() else {
            panic!("exact callback must win")
        };
        let AppliedCapabilityWake {
            invocation,
            job,
            job_payload,
        } = *applied;
        let worker = id("wrk_0198f1c8-32e4-75e1-a9e8-d95ca0f40011");
        let token = hash('5');
        let started = decide_start_dispatch(
            &invocation,
            &job,
            &job_payload,
            worker.clone(),
            token.clone(),
            LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 60_000,
            },
            now + Duration::milliseconds(4),
        )
        .unwrap();
        assert_eq!(started.job.attempt_count, 1);
        assert!(started.job.lease_generation > generation);
        assert!(!started.job_payload.resume_same_attempt);
        let response_schema = InteractionSchemaDocument::build(serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "approved": {"type": "boolean"}
            },
            "required": ["approved"],
            "additionalProperties": false
        }))
        .unwrap();
        let input_schema = response_schema.canonical_digest.clone();
        let input_task_id = id("int_0198f1c8-32e4-75e1-a9e8-d95ca0f40012");
        let input_required = decide_dispatch_outcome(
            &started.invocation,
            &started.job,
            &started.job_payload,
            &JobFence {
                expected_version: started.job.version,
                worker_process_generation_id: worker,
                lease_generation: started.job.lease_generation,
                token_digest: token,
            },
            &DispatchOutcome::InputRequired(BackendInputRequest {
                input_task_id: input_task_id.clone(),
                interaction_kind: insight_platform_contracts::InteractionKind::Form,
                safe_prompt_key: "provide_input".to_owned(),
                response_schema,
                response_schema_digest: input_schema.clone(),
                eligible_principal_rule_digest: hash('7'),
                exact_eligible_principal_id: None,
                opaque_state_digest: remote_state.plaintext_digest.clone(),
                encrypted_state: remote_state,
                external_identity_digest: hash('9'),
                deadline: now + Duration::minutes(1),
            }),
            now + Duration::milliseconds(5),
            InvocationCommandLimits::new(4).unwrap(),
        )
        .unwrap();
        assert_eq!(
            input_required.invocation.state,
            InvocationState::AwaitingInput
        );
        let (value, content_digest) = inline_value(serde_json::json!({"approved": true}));
        let resumed = decide_input_response(
            &input_required.invocation,
            &input_required.job,
            &input_required.job_payload,
            CapabilityInputResolution {
                expected_generation: input_required.job.lease_generation,
                action: CapabilityInputAction::Accept,
                response: Some(&CapabilityInputResponse {
                    value_id: id("val_0198f1c8-32e4-75e1-a9e8-d95ca0f40013"),
                    classification: DataClassification::Internal,
                    schema_digest: input_schema,
                    content_digest,
                    value,
                    artifact_link_id: None,
                }),
                database_now: now + Duration::milliseconds(6),
            },
            InvocationCommandLimits::new(4).unwrap(),
        )
        .unwrap();
        assert_eq!(resumed.invocation.state, InvocationState::Ready);
        assert_eq!(resumed.job.state, JobState::Ready);
        assert_eq!(resumed.job.job_id, job.job_id);
        assert_eq!(
            resumed.response.as_ref().unwrap().value_kind,
            "capability_input_response"
        );
        let second_worker = id("wrk_0198f1c8-32e4-75e1-a9e8-d95ca0f40014");
        let second_token = hash('0');
        let resumed_again = decide_start_dispatch(
            &resumed.invocation,
            &resumed.job,
            &resumed.job_payload,
            second_worker,
            second_token,
            LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 60_000,
            },
            now + Duration::milliseconds(7),
        )
        .unwrap();
        assert_eq!(resumed_again.job.attempt_count, 1);
        assert!(resumed_again.job.lease_generation > started.job.lease_generation);
        assert!(!resumed_again.job_payload.resume_same_attempt);

        for action in [
            CapabilityInputAction::Decline,
            CapabilityInputAction::Cancel,
        ] {
            let declined = decide_input_response(
                &input_required.invocation,
                &input_required.job,
                &input_required.job_payload,
                CapabilityInputResolution {
                    expected_generation: input_required.job.lease_generation,
                    action,
                    response: None,
                    database_now: now + Duration::milliseconds(6),
                },
                InvocationCommandLimits::new(4).unwrap(),
            )
            .unwrap();
            assert_eq!(declined.invocation.state, InvocationState::Ready);
            assert_eq!(declined.job.state, JobState::Ready);
            assert_eq!(declined.job_payload.resume_input_action, Some(action));
            assert!(declined.job_payload.resume_input.is_none());
            assert!(declined.response.is_none());
        }
    }

    #[test]
    fn unresolved_mcp_form_rejects_invalid_response_and_preserves_decision_until_acknowledged() {
        let now = DateTime::parse_from_rfc3339("2026-08-10T02:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (invocation, job, payload, fence) =
            prepared_and_started(now, Effect::ReadOnly, CapabilityIdempotencyKind::Intrinsic);
        let remote_state = EncryptedRemoteState {
            scheme: "aes256_gcm_v1".to_owned(),
            key_id: "remote-state-key-1".to_owned(),
            key_reference_digest: hash('1'),
            ciphertext: "ZW5jcnlwdGVk".to_owned(),
            plaintext_digest: hash('2'),
        };
        let response_schema = InteractionSchemaDocument::build_mcp_form(serde_json::json!({
            "type": "object",
            "properties": {
                "region": {"type": "string", "enum": ["cn", "us"]}
            },
            "required": ["region"]
        }))
        .unwrap();
        let input_required = decide_dispatch_outcome(
            &invocation,
            &job,
            &payload,
            &fence,
            &DispatchOutcome::InputRequired(BackendInputRequest {
                input_task_id: id("int_0198f1c8-32e4-75e1-a9e8-d95ca0f40020"),
                interaction_kind: insight_platform_contracts::InteractionKind::Form,
                safe_prompt_key: "mcp_task_input_required".to_owned(),
                response_schema_digest: response_schema.canonical_digest.clone(),
                response_schema,
                eligible_principal_rule_digest: hash('3'),
                exact_eligible_principal_id: None,
                opaque_state_digest: remote_state.plaintext_digest.clone(),
                encrypted_state: remote_state.clone(),
                external_identity_digest: hash('4'),
                deadline: now + Duration::minutes(1),
            }),
            now + Duration::milliseconds(1),
            InvocationCommandLimits::new(4).unwrap(),
        )
        .unwrap();
        let (invalid_value, invalid_digest) = inline_value(serde_json::json!({"region": "eu"}));
        let invalid = CapabilityInputResponse {
            value_id: id("val_0198f1c8-32e4-75e1-a9e8-d95ca0f40021"),
            classification: DataClassification::Internal,
            schema_digest: input_required
                .job_payload
                .input_request
                .as_ref()
                .unwrap()
                .response_schema_digest
                .clone(),
            content_digest: invalid_digest,
            value: invalid_value,
            artifact_link_id: None,
        };
        assert!(matches!(
            decide_input_response(
                &input_required.invocation,
                &input_required.job,
                &input_required.job_payload,
                CapabilityInputResolution {
                    expected_generation: input_required.job.lease_generation,
                    action: CapabilityInputAction::Accept,
                    response: Some(&invalid),
                    database_now: now + Duration::milliseconds(2),
                },
                InvocationCommandLimits::new(4).unwrap(),
            ),
            Err(InvocationError::InvalidInputValue)
        ));

        let declined = decide_input_response(
            &input_required.invocation,
            &input_required.job,
            &input_required.job_payload,
            CapabilityInputResolution {
                expected_generation: input_required.job.lease_generation,
                action: CapabilityInputAction::Decline,
                response: None,
                database_now: now + Duration::milliseconds(2),
            },
            InvocationCommandLimits::new(4).unwrap(),
        )
        .unwrap();
        let worker = id("wrk_0198f1c8-32e4-75e1-a9e8-d95ca0f40022");
        let token = hash('5');
        let resumed = decide_start_dispatch(
            &declined.invocation,
            &declined.job,
            &declined.job_payload,
            worker.clone(),
            token.clone(),
            LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 60_000,
            },
            now + Duration::milliseconds(3),
        )
        .unwrap();
        let preserved = decide_dispatch_outcome(
            &resumed.invocation,
            &resumed.job,
            &resumed.job_payload,
            &JobFence {
                expected_version: resumed.job.version,
                worker_process_generation_id: worker,
                lease_generation: resumed.job.lease_generation,
                token_digest: token,
            },
            &DispatchOutcome::Deferred(RemoteWait {
                encrypted_state: remote_state,
                external_identity_digest: hash('4'),
                accepted_sources: vec![WakeSource::Poll],
                next_poll_at: Some(now + Duration::seconds(1)),
                callback_binding_digest: None,
            }),
            now + Duration::milliseconds(4),
            InvocationCommandLimits::new(4).unwrap(),
        )
        .unwrap();
        assert_eq!(
            preserved.job_payload.resume_input_action,
            Some(CapabilityInputAction::Decline)
        );
    }

    #[test]
    fn uncertain_write_is_owned_by_reconciliation() {
        let now = DateTime::parse_from_rfc3339("2026-08-10T03:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (invocation, job, payload, fence) = prepared_and_started(
            now,
            Effect::NonIdempotentWrite,
            CapabilityIdempotencyKind::ReconcileBeforeRetry,
        );
        let uncertain = decide_dispatch_outcome(
            &invocation,
            &job,
            &payload,
            &fence,
            &DispatchOutcome::Uncertain(CapabilityUncertainty {
                observation_digest: hash('1'),
                policy_path_digest: hash('2'),
                external_identity_digest: hash('3'),
                manual: true,
            }),
            now + Duration::milliseconds(2),
            InvocationCommandLimits::new(4).unwrap(),
        )
        .unwrap();
        assert_eq!(
            uncertain.invocation.state,
            InvocationState::ReconciliationRequired
        );
        assert_eq!(uncertain.job.state, JobState::ReconciliationRequired);
    }

    #[test]
    fn transport_cancel_observation_never_proves_a_write_had_no_effect() {
        let now = DateTime::parse_from_rfc3339("2026-08-10T03:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (invocation, job, payload, fence) = prepared_and_started(
            now,
            Effect::NonIdempotentWrite,
            CapabilityIdempotencyKind::None,
        );
        let controlled = decide_control(
            &invocation,
            Some(&job),
            Some(&payload),
            CapabilityControlKind::Cancel,
            now + Duration::milliseconds(1),
        )
        .unwrap();
        let cancelling_job = controlled.job.unwrap();
        let cancelling_payload = controlled.job_payload.unwrap();
        let observation = hash('6');
        let external_identity = hash('7');
        let cancelled = decide_cancellation_outcome(
            &controlled.invocation,
            &cancelling_job,
            &cancelling_payload,
            Some(&JobFence {
                expected_version: cancelling_job.version,
                ..fence
            }),
            &CapabilityCancellationObservation {
                cancellation_observation_digest: observation.clone(),
                external_identity_digest: Some(external_identity.clone()),
                no_effect_proof_digest: None,
                cleanup_deadline: None,
            },
            now + Duration::milliseconds(2),
        )
        .unwrap();
        assert_eq!(
            cancelled.invocation.state,
            InvocationState::ReconciliationRequired
        );
        assert_eq!(
            cancelled
                .invocation
                .payload
                .reconciliation
                .as_ref()
                .unwrap()
                .external_identity_digest,
            Some(external_identity.clone())
        );
        assert_eq!(
            cancelled
                .job_payload
                .as_ref()
                .unwrap()
                .external_identity_digest,
            Some(external_identity)
        );
        assert!(matches!(
            cancelled.job_payload.unwrap().physical_outcome,
            Some(CapabilityPhysicalOutcomeEvidence::Uncertain {
                observation_digest
            }) if observation_digest == observation
        ));
    }

    #[test]
    fn cancellation_can_commit_with_exact_fence_during_bounded_post_deadline_cleanup() {
        let now = DateTime::parse_from_rfc3339("2026-08-10T04:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (invocation, job, payload, fence) =
            prepared_and_started(now, Effect::ReadOnly, CapabilityIdempotencyKind::Intrinsic);
        let controlled = decide_control(
            &invocation,
            Some(&job),
            Some(&payload),
            CapabilityControlKind::Cancel,
            invocation.deadline + Duration::milliseconds(1),
        )
        .unwrap();
        let cancelling_job = controlled.job.unwrap();
        let cancelling_payload = controlled.job_payload.unwrap();
        let cancelling_fence = JobFence {
            expected_version: cancelling_job.version,
            ..fence
        };
        let cleanup_deadline = invocation.deadline + Duration::seconds(10);
        let cancelled = decide_cancellation_outcome(
            &controlled.invocation,
            &cancelling_job,
            &cancelling_payload,
            Some(&cancelling_fence),
            &CapabilityCancellationObservation {
                cancellation_observation_digest: hash('8'),
                external_identity_digest: Some(hash('9')),
                no_effect_proof_digest: None,
                cleanup_deadline: Some(cleanup_deadline),
            },
            invocation.deadline + Duration::seconds(1),
        )
        .unwrap();
        assert_eq!(cancelled.invocation.state, InvocationState::Cancelled);
        assert_eq!(cancelled.job.unwrap().state, JobState::Cancelled);

        assert!(decide_cancellation_outcome(
            &controlled.invocation,
            &cancelling_job,
            &cancelling_payload,
            Some(&cancelling_fence),
            &CapabilityCancellationObservation {
                cancellation_observation_digest: hash('a'),
                external_identity_digest: Some(hash('b')),
                no_effect_proof_digest: None,
                cleanup_deadline: Some(cleanup_deadline),
            },
            cleanup_deadline + Duration::milliseconds(1),
        )
        .is_err());
    }

    #[test]
    fn expired_dispatch_retries_only_replayable_effects() {
        let now = DateTime::parse_from_rfc3339("2026-08-10T04:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (invocation, job, payload, _) =
            prepared_and_started(now, Effect::ReadOnly, CapabilityIdempotencyKind::Intrinsic);
        let expired_at = job.lease.as_ref().unwrap().expires_at;
        let retry_at = expired_at + Duration::milliseconds(100);
        let recovered = decide_expired_dispatch(
            &invocation,
            &job,
            &payload,
            expired_at,
            ExpiredCapabilityLeaseObservation {
                observed_job_version: job.version,
                observed_lease_generation: job.lease_generation,
                retry_at: Some(retry_at),
                observation_digest: hash('8'),
            },
        )
        .unwrap();
        assert_eq!(recovered.invocation.state, InvocationState::RetryScheduled);
        assert_eq!(recovered.invocation.retry_at, Some(retry_at));
        assert_eq!(recovered.job.state, JobState::RetryScheduled);
        assert!(recovered.job.lease.is_none());

        let (unsafe_invocation, unsafe_job, unsafe_payload, _) = prepared_and_started(
            now,
            Effect::NonIdempotentWrite,
            CapabilityIdempotencyKind::None,
        );
        let unsafe_expired_at = unsafe_job.lease.as_ref().unwrap().expires_at;
        let uncertain = decide_expired_dispatch(
            &unsafe_invocation,
            &unsafe_job,
            &unsafe_payload,
            unsafe_expired_at,
            ExpiredCapabilityLeaseObservation {
                observed_job_version: unsafe_job.version,
                observed_lease_generation: unsafe_job.lease_generation,
                retry_at: Some(unsafe_expired_at + Duration::milliseconds(100)),
                observation_digest: hash('9'),
            },
        )
        .unwrap();
        assert_eq!(
            uncertain.invocation.state,
            InvocationState::ReconciliationRequired
        );
        assert_eq!(uncertain.job.state, JobState::ReconciliationRequired);
        assert!(uncertain.job.lease.is_none());

        let timed_out = decide_expired_dispatch(
            &invocation,
            &job,
            &payload,
            invocation.deadline,
            ExpiredCapabilityLeaseObservation {
                observed_job_version: job.version,
                observed_lease_generation: job.lease_generation,
                retry_at: None,
                observation_digest: hash('a'),
            },
        )
        .unwrap();
        assert_eq!(timed_out.invocation.state, InvocationState::TimedOut);
        assert_eq!(timed_out.job.state, JobState::TimedOut);
        assert!(timed_out.job.lease.is_none());
    }

    #[test]
    fn capability_control_kind_has_a_closed_wire_value() {
        assert_eq!(
            serde_json::to_value(CapabilityControlKind::Cancel).unwrap(),
            serde_json::json!("cancel")
        );
        assert_eq!(
            serde_json::from_value::<CapabilityControlKind>(serde_json::json!("timeout")).unwrap(),
            CapabilityControlKind::Timeout
        );
        assert!(
            serde_json::from_value::<CapabilityControlKind>(serde_json::json!("stop")).is_err()
        );
    }
}
