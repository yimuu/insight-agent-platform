use crate::{
    digest_without_field, stable_code, PreparedSandbox, SandboxCommandLimits, SandboxContractError,
    SandboxExecutionRequest, SandboxStopReason, MAX_SANDBOX_ARTIFACT_GRANTS,
    MAX_SANDBOX_SAFE_CODE_BYTES, MAX_SANDBOX_SAFE_MESSAGE_BYTES,
};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    ArtifactRef, CommandAudit, DataClassification, JobState, ResourceId, ResourceKind,
    Retryability, SandboxJobState, Sha256Digest, ValueRef, WorkClass,
};
use insight_platform_invocations::{
    BackendInputRequest, CapabilityOutputValue, DispatchOutcome, RemoteWait,
};
use insight_platform_jobs::{
    decide_cleanup_reconciliation, decide_cleanup_terminal, decide_expired_lease,
    decide_observation_update, decide_owner_terminal, decide_start, JobFence, JobOwnerRef,
    JobProjection, WakeSource,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const SANDBOX_QUOTA_LINES: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxCleanupDisposition {
    Destroyed,
    NodeQuarantined,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxCleanupEvidence {
    pub disposition: SandboxCleanupDisposition,
    pub sandbox_identity_digest: Sha256Digest,
    pub grants_revoked: bool,
    pub ephemeral_storage_destroyed: bool,
    pub observed_at: DateTime<Utc>,
    pub evidence_digest: Sha256Digest,
}

impl SandboxCleanupEvidence {
    pub(crate) fn validate(
        &self,
        deadline: DateTime<Utc>,
        cleanup_milliseconds: u64,
    ) -> Result<(), SandboxContractError> {
        let cleanup_milliseconds = i64::try_from(cleanup_milliseconds)
            .map_err(|_| SandboxContractError::InvalidOutcome)?;
        let cleanup_deadline = deadline
            .checked_add_signed(chrono::Duration::milliseconds(cleanup_milliseconds))
            .ok_or(SandboxContractError::InvalidOutcome)?;
        if self.observed_at > cleanup_deadline
            || (self.disposition == SandboxCleanupDisposition::Destroyed
                && (!self.grants_revoked || !self.ephemeral_storage_destroyed))
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        Ok(())
    }

    pub(crate) fn validate_recovery(&self) -> Result<(), SandboxContractError> {
        if !self.grants_revoked
            || (self.disposition == SandboxCleanupDisposition::Destroyed
                && !self.ephemeral_storage_destroyed)
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxResourceUsage {
    pub cpu_milliseconds: u64,
    pub peak_memory_mebibytes: u32,
    pub peak_pids: u32,
    pub files_created: u32,
    pub io_bytes: u64,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub result_bytes: u64,
    pub artifact_output_bytes: u64,
    pub network_connections: u32,
    pub network_request_bytes: u64,
    pub network_response_bytes: u64,
    pub wall_milliseconds: u64,
    pub wasm_fuel_consumed: Option<u64>,
}

impl SandboxResourceUsage {
    pub fn validate_for(
        &self,
        request: &SandboxExecutionRequest,
    ) -> Result<(), SandboxContractError> {
        let limits = &request.resources;
        if self.peak_memory_mebibytes > limits.memory_mebibytes
            || self.peak_pids > limits.pids
            || self.files_created > limits.files
            || self.io_bytes > limits.io_bytes
            || self.stdout_bytes > limits.stdout_bytes
            || self.stderr_bytes > limits.stderr_bytes
            || self.result_bytes > limits.result_bytes
            || self.artifact_output_bytes > limits.artifact_output_bytes
            || self.network_connections > limits.network_connections
            || self.network_request_bytes > limits.network_request_bytes
            || self.network_response_bytes > limits.network_response_bytes
            || self.wall_milliseconds > limits.wall_milliseconds
            || self.wasm_fuel_consumed.is_some() != limits.wasm_fuel.is_some()
            || self
                .wasm_fuel_consumed
                .zip(limits.wasm_fuel)
                .is_some_and(|(used, maximum)| used > maximum)
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxCompletedOutput {
    pub value_id: ResourceId,
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub value: ValueRef,
    pub artifact_link_ids: Vec<ResourceId>,
    pub artifact_outputs: Vec<ArtifactRef>,
    pub validation_evidence_digest: Sha256Digest,
    pub usage: SandboxResourceUsage,
}

impl SandboxCompletedOutput {
    fn validate_for(
        &self,
        request: &SandboxExecutionRequest,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        if self.value_id != request.output_value_id
            || self.schema_digest != request.output_schema_digest
            || self.classification.rank() < request.classification.rank()
            || self.artifact_outputs.len() > MAX_SANDBOX_ARTIFACT_GRANTS
            || self.artifact_link_ids.len() != self.artifact_outputs.len()
            || self
                .artifact_link_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::ArtifactLink)
            || !self
                .artifact_link_ids
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || self.artifact_link_ids.iter().collect::<BTreeSet<_>>().len()
                != self.artifact_link_ids.len()
            || self
                .artifact_outputs
                .iter()
                .map(ArtifactRef::artifact_id)
                .collect::<BTreeSet<_>>()
                .len()
                != self.artifact_outputs.len()
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        self.value
            .validate(limits.inline_json_limits())
            .map_err(|_| SandboxContractError::InvalidOutcome)?;
        match &self.value {
            ValueRef::Inline { value } => {
                let actual: Sha256Digest = insight_platform_contracts::canonical_digest(value)
                    .map_err(|_| SandboxContractError::Canonicalization)?
                    .parse()
                    .map_err(|_| SandboxContractError::Canonicalization)?;
                if actual != self.content_digest {
                    return Err(SandboxContractError::InvalidOutcome);
                }
            }
            ValueRef::Artifact { artifact } => {
                if artifact.content_digest() != &self.content_digest
                    || artifact.classification() != self.classification
                    || !self.artifact_outputs.contains(artifact)
                {
                    return Err(SandboxContractError::InvalidOutcome);
                }
            }
        }
        if self.artifact_outputs.iter().any(|artifact| {
            artifact.validate().is_err()
                || artifact.classification().rank() < request.classification.rank()
        }) {
            return Err(SandboxContractError::InvalidOutcome);
        }
        self.usage.validate_for(request)
    }
}

/// A trusted Managed MCP protocol adapter result produced inside the Sandbox execution plane.
///
/// The microVM provider validates the raw MCP exchange and applies the process-installed,
/// digest-selected declarative Capability codec before constructing this value. The physical
/// Sandbox Job may therefore succeed while the logical Capability remains deferred or awaits
/// input. Raw MCP frames, session identifiers and Secret material are never persisted here.
#[cfg(any())]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxManagedMcpOutput {
    pub worker_process_generation_id: ResourceId,
    pub logical_poll_count: u32,
    pub observed_at: DateTime<Utc>,
    pub outcome: DispatchOutcome,
    pub protocol_evidence_digest: Sha256Digest,
    pub usage: SandboxResourceUsage,
}

#[cfg(any())]
impl SandboxManagedMcpOutput {
    fn validate_for(
        &self,
        request: &SandboxExecutionRequest,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        let crate::SandboxExecutionSource::ManagedMcp {
            capability_implementation,
            operation,
            ..
        } = &request.execution_source
        else {
            return Err(SandboxContractError::InvalidOutcome);
        };
        let expected_poll_count = operation
            .continuation
            .as_ref()
            .map_or(0, |continuation| continuation.poll_count);
        if self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.logical_poll_count != expected_poll_count
            || self.observed_at > request.deadline
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        self.usage.validate_for(request)?;
        let features = &capability_implementation.features;
        match &self.outcome {
            DispatchOutcome::Completed(output) => {
                validate_managed_mcp_completed(output, request, limits)?;
            }
            DispatchOutcome::Deferred(wait) => {
                validate_managed_mcp_wait(
                    wait,
                    self.logical_poll_count,
                    self.observed_at,
                    request,
                    features,
                )?;
            }
            DispatchOutcome::InputRequired(input) => {
                validate_managed_mcp_input(input, self.observed_at, request, features)?;
            }
            DispatchOutcome::RetryableFailure { failure, retry_at } => {
                failure
                    .failure
                    .validate(MAX_SANDBOX_SAFE_MESSAGE_BYTES)
                    .map_err(|_| SandboxContractError::InvalidOutcome)?;
                if failure.failure.retryability != Retryability::SafeWithinPolicy
                    || *retry_at <= self.observed_at
                    || *retry_at >= request.deadline
                {
                    return Err(SandboxContractError::InvalidOutcome);
                }
            }
            DispatchOutcome::PermanentFailure(failure) => {
                failure
                    .failure
                    .validate(MAX_SANDBOX_SAFE_MESSAGE_BYTES)
                    .map_err(|_| SandboxContractError::InvalidOutcome)?;
                if failure.failure.retryability != Retryability::Never {
                    return Err(SandboxContractError::InvalidOutcome);
                }
            }
            DispatchOutcome::Uncertain(uncertainty) => {
                if !uncertainty.manual
                    || request.effect.risk_rank()
                        < insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
                {
                    return Err(SandboxContractError::InvalidOutcome);
                }
            }
        }
        Ok(())
    }
}

#[cfg(any())]
fn validate_managed_mcp_completed(
    output: &CapabilityOutputValue,
    request: &SandboxExecutionRequest,
    limits: SandboxCommandLimits,
) -> Result<(), SandboxContractError> {
    if output.value_id != request.output_value_id
        || output.schema_digest != request.output_schema_digest
        || output.classification.rank() < request.classification.rank()
        || output
            .artifact_link_id
            .as_ref()
            .is_some_and(|id| id.kind() != ResourceKind::ArtifactLink)
    {
        return Err(SandboxContractError::InvalidOutcome);
    }
    output
        .value
        .validate(limits.inline_json_limits())
        .map_err(|_| SandboxContractError::InvalidOutcome)?;
    match &output.value {
        ValueRef::Inline { value } => {
            let actual: Sha256Digest = insight_platform_contracts::canonical_digest(value)
                .map_err(|_| SandboxContractError::Canonicalization)?
                .parse()
                .map_err(|_| SandboxContractError::Canonicalization)?;
            if actual != output.content_digest || output.artifact_link_id.is_some() {
                return Err(SandboxContractError::InvalidOutcome);
            }
        }
        ValueRef::Artifact { artifact } => {
            if artifact.validate().is_err()
                || artifact.content_digest() != &output.content_digest
                || artifact.classification() != output.classification
                || output.artifact_link_id.is_none()
            {
                return Err(SandboxContractError::InvalidOutcome);
            }
        }
    }
    Ok(())
}

#[cfg(any())]
fn validate_managed_mcp_wait(
    wait: &RemoteWait,
    poll_count: u32,
    observed_at: DateTime<Utc>,
    request: &SandboxExecutionRequest,
    features: &insight_platform_contracts::CapabilityBackendFeatures,
) -> Result<(), SandboxContractError> {
    if !features.deferred
        || !features.poll
        || poll_count >= features.max_poll_count
        || wait.accepted_sources != [WakeSource::Poll]
        || wait.callback_binding_digest.is_some()
        || wait
            .next_poll_at
            .is_none_or(|next| next <= observed_at || next >= request.deadline)
        || wait
            .encrypted_state
            .validate(features.max_remote_state_bytes)
            .is_err()
    {
        return Err(SandboxContractError::InvalidOutcome);
    }
    Ok(())
}

#[cfg(any())]
fn validate_managed_mcp_input(
    input: &BackendInputRequest,
    observed_at: DateTime<Utc>,
    request: &SandboxExecutionRequest,
    features: &insight_platform_contracts::CapabilityBackendFeatures,
) -> Result<(), SandboxContractError> {
    if !features.input_required
        || input.input_task_id.kind() != ResourceKind::Interaction
        || !stable_code(&input.safe_prompt_key, MAX_SANDBOX_SAFE_CODE_BYTES)
        || input.response_schema.validate().is_err()
        || input.response_schema.canonical_digest != input.response_schema_digest
        || input.opaque_state_digest != input.encrypted_state.plaintext_digest
        || input.deadline <= observed_at
        || input.deadline > request.deadline
        || input
            .encrypted_state
            .validate(features.max_remote_state_bytes)
            .is_err()
    {
        return Err(SandboxContractError::InvalidOutcome);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeSandboxFailure {
    pub safe_code: String,
    pub safe_message: String,
    pub retryability: Retryability,
    pub evidence_digest: Sha256Digest,
    pub resource_violation: Option<String>,
    pub external_effect_possible: bool,
}

impl SafeSandboxFailure {
    fn validate_for(&self, request: &SandboxExecutionRequest) -> Result<(), SandboxContractError> {
        if !stable_code(&self.safe_code, MAX_SANDBOX_SAFE_CODE_BYTES)
            || self.safe_message.is_empty()
            || self.safe_message.len() > MAX_SANDBOX_SAFE_MESSAGE_BYTES
            || self.safe_message.chars().any(char::is_control)
            || self
                .resource_violation
                .as_ref()
                .is_some_and(|value| !stable_code(value, MAX_SANDBOX_SAFE_CODE_BYTES))
            || (self.external_effect_possible
                && request.effect.risk_rank()
                    < insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
                && request.network_mode == crate::SandboxNetworkMode::None)
            || (self.external_effect_possible
                && self.retryability == Retryability::SafeWithinPolicy)
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxTerminationEvidence {
    pub evidence_digest: Sha256Digest,
    pub guest_acknowledged: bool,
    pub process_tree_terminated: bool,
    pub grants_revoked: bool,
    pub external_effect_possible: bool,
}

impl SandboxTerminationEvidence {
    pub(crate) fn validate_for(
        &self,
        request: &SandboxExecutionRequest,
    ) -> Result<(), SandboxContractError> {
        if !self.process_tree_terminated
            || !self.grants_revoked
            || (self.external_effect_possible
                && request.effect.risk_rank()
                    < insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
                && request.network_mode == crate::SandboxNetworkMode::None)
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxUncertainty {
    pub evidence_digest: Sha256Digest,
    pub sandbox_identity_digest: Sha256Digest,
    pub execution_may_have_started: bool,
    pub external_effect_possible: bool,
    pub manual_reconciliation_required: bool,
}

impl SandboxUncertainty {
    pub(crate) fn validate_for(
        &self,
        request: &SandboxExecutionRequest,
    ) -> Result<(), SandboxContractError> {
        if self.external_effect_possible
            && (!self.execution_may_have_started || !self.manual_reconciliation_required)
            || self.external_effect_possible
                && request.effect.risk_rank()
                    < insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
                && request.network_mode == crate::SandboxNetworkMode::None
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "evidence",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SandboxExecutionOutcome {
    Completed(Box<SandboxCompletedOutput>),
    #[cfg(any())]
    ManagedMcp(Box<SandboxManagedMcpOutput>),
    Failed(SafeSandboxFailure),
    Cancelled(SandboxTerminationEvidence),
    TimedOut(SandboxTerminationEvidence),
    Uncertain(SandboxUncertainty),
}

impl SandboxExecutionOutcome {
    pub(crate) fn physical_state(&self) -> SandboxJobState {
        match self {
            Self::Completed(_) => SandboxJobState::Succeeded,
            Self::Failed(_) => SandboxJobState::Failed,
            Self::Cancelled(_) => SandboxJobState::Cancelled,
            Self::TimedOut(_) => SandboxJobState::TimedOut,
            Self::Uncertain(_) => SandboxJobState::Lost,
        }
    }

    pub fn validate_for(
        &self,
        request: &SandboxExecutionRequest,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        match self {
            Self::Completed(output)
                if matches!(
                    &request.execution_source,
                    crate::SandboxExecutionSource::SandboxCapability { .. }
                ) =>
            {
                output.validate_for(request, limits)
            }
            #[cfg(any())]
            Self::ManagedMcp(output)
                if matches!(
                    &request.execution_source,
                    crate::SandboxExecutionSource::ManagedMcp { .. }
                ) =>
            {
                output.validate_for(request, limits)
            }
            Self::Completed(_) => Err(SandboxContractError::InvalidOutcome),
            Self::Failed(failure) => failure.validate_for(request),
            Self::Cancelled(evidence) | Self::TimedOut(evidence) => evidence.validate_for(request),
            Self::Uncertain(uncertainty) => uncertainty.validate_for(request),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxExecutionJobPayload {
    pub schema_version: u32,
    pub request: Box<SandboxExecutionRequest>,
    pub submission_digest: Sha256Digest,
    pub physical_state: SandboxJobState,
    pub phase_sequence: u32,
    pub executor_identity_digest: Option<Sha256Digest>,
    pub attestor_route: Option<crate::NodeAttestorRoute>,
    pub phase_evidence_digest: Option<Sha256Digest>,
    /// Exact physical binding established by a successful prepare and committed before start.
    pub prepared: Option<PreparedSandbox>,
    pub outcome: Option<SandboxExecutionOutcome>,
    pub cleanup: Option<SandboxCleanupEvidence>,
    pub payload_digest: Sha256Digest,
}

impl SandboxExecutionJobPayload {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.payload_digest = digest_without_field(&self, "payload_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        job: &JobProjection,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        self.request
            .validate_submission_at(job.scheduled_at, limits)?;
        if self.schema_version != 1
            || job.tenant_id != self.request.tenant_id
            || job.job_id != self.request.job_id
            || job.owner
                != (JobOwnerRef {
                    owner_id: self.request.sandbox_job_id.clone(),
                    owner_kind: ResourceKind::Job,
                })
            || job.work_class != WorkClass::Sandbox
            || job.deadline != self.request.deadline
            || job.attempt_limit != 1
            || self.submission_digest != self.request.request_digest
            || digest_without_field(self, "payload_digest")? != self.payload_digest
        {
            return Err(SandboxContractError::InvalidJob);
        }
        if let Some(prepared) = &self.prepared {
            let leased_request = self
                .request
                .as_ref()
                .clone()
                .bind_lease_generation(job.lease_generation)?;
            prepared
                .validate_for(&leased_request)
                .map_err(|_| SandboxContractError::InvalidJob)?;
            if self.cleanup.as_ref().is_some_and(|cleanup| {
                cleanup.sandbox_identity_digest != prepared.sandbox_identity_digest
            }) {
                return Err(SandboxContractError::InvalidJob);
            }
        }
        let prepared_state_matches = match self.physical_state {
            SandboxJobState::Accepted | SandboxJobState::Preparing => self.prepared.is_none(),
            SandboxJobState::Starting
            | SandboxJobState::Running
            | SandboxJobState::Collecting
            | SandboxJobState::Succeeded => self.prepared.is_some(),
            SandboxJobState::Cancelling
            | SandboxJobState::Failed
            | SandboxJobState::Cancelled
            | SandboxJobState::TimedOut
            | SandboxJobState::Lost => true,
        };
        if !prepared_state_matches {
            return Err(SandboxContractError::InvalidJob);
        }
        let state_matches = match self.physical_state {
            SandboxJobState::Accepted => {
                self.phase_sequence == 0
                    && self.executor_identity_digest.is_none()
                    && self.attestor_route.is_none()
                    && self.phase_evidence_digest.is_none()
                    && self.outcome.is_none()
                    && self.cleanup.is_none()
                    && matches!(job.state, JobState::Ready | JobState::Leased)
            }
            SandboxJobState::Preparing
            | SandboxJobState::Starting
            | SandboxJobState::Running
            | SandboxJobState::Collecting
            | SandboxJobState::Cancelling => {
                self.phase_sequence > 0
                    && self.executor_identity_digest.is_some()
                    && self.attestor_route.is_some()
                    && self.phase_evidence_digest.is_some()
                    && self.outcome.is_none()
                    && self.cleanup.is_none()
                    && matches!(job.state, JobState::Running | JobState::Cancelling)
            }
            SandboxJobState::Succeeded
            | SandboxJobState::Failed
            | SandboxJobState::Cancelled
            | SandboxJobState::TimedOut
            | SandboxJobState::Lost => {
                let executor_terminal = self.phase_sequence > 0
                    && self.executor_identity_digest.is_some()
                    && self.attestor_route.is_some()
                    && self.phase_evidence_digest.is_some()
                    && self
                        .outcome
                        .as_ref()
                        .map(SandboxExecutionOutcome::physical_state)
                        == Some(self.physical_state)
                    && self.cleanup.is_some();
                let prestart_terminal = self.executor_identity_digest.is_none()
                    && self.attestor_route.is_none()
                    && self.phase_evidence_digest.is_some()
                    && self.outcome.is_none()
                    && self.cleanup.is_none()
                    && job.attempt_count == 0
                    && job.lease.is_none()
                    && matches!(
                        (self.physical_state, self.phase_sequence),
                        (SandboxJobState::Cancelled, 2) | (SandboxJobState::TimedOut, 1)
                    );
                (executor_terminal || prestart_terminal)
                    && match self.physical_state {
                        SandboxJobState::Succeeded => job.state == JobState::Succeeded,
                        SandboxJobState::Failed => job.state == JobState::Failed,
                        SandboxJobState::Cancelled => job.state == JobState::Cancelled,
                        SandboxJobState::TimedOut => job.state == JobState::TimedOut,
                        SandboxJobState::Lost => job.state == JobState::ReconciliationRequired,
                        _ => false,
                    }
            }
        };
        if !state_matches {
            return Err(SandboxContractError::InvalidJob);
        }
        if let Some(outcome) = &self.outcome {
            outcome.validate_for(&self.request, limits)?;
        }
        if let Some(cleanup) = &self.cleanup {
            if self.physical_state == SandboxJobState::Lost {
                cleanup.validate_recovery()?;
            } else {
                cleanup.validate(
                    self.request.deadline,
                    self.request.resources.cleanup_milliseconds,
                )?;
            }
            if self.physical_state == SandboxJobState::Succeeded
                && cleanup.disposition != SandboxCleanupDisposition::Destroyed
            {
                return Err(SandboxContractError::InvalidOutcome);
            }
        }
        Ok(())
    }
}

/// Closed persistent payload for every `work_class=sandbox` Job.
///
/// The discriminator is mandatory so the shared Job table never guesses a workload contract from
/// overlapping JSON fields. Adding a new Sandbox workload therefore extends this enum and its
/// validators instead of adding a table or accepting arbitrary payloads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxJobPayload {
    pub schema_version: u32,
    #[serde(flatten)]
    pub workload: SandboxJobWorkload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "workload_kind", content = "workload", rename_all = "snake_case")]
pub enum SandboxJobWorkload {
    CapabilityExecution(Box<SandboxExecutionJobPayload>),
    #[cfg(any())]
    ManagedMcpSubscriptionSession(Box<crate::ManagedMcpSandboxSessionJobPayload>),
}

impl SandboxJobPayload {
    pub fn capability_execution(payload: SandboxExecutionJobPayload) -> Self {
        Self {
            schema_version: 1,
            workload: SandboxJobWorkload::CapabilityExecution(Box::new(payload)),
        }
    }

    #[cfg(any())]
    pub fn managed_mcp_subscription_session(
        payload: crate::ManagedMcpSandboxSessionJobPayload,
    ) -> Self {
        Self {
            schema_version: 1,
            workload: SandboxJobWorkload::ManagedMcpSubscriptionSession(Box::new(payload)),
        }
    }

    pub fn validate_for(
        &self,
        job: &JobProjection,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        if self.schema_version != 1 {
            return Err(SandboxContractError::InvalidJob);
        }
        match &self.workload {
            SandboxJobWorkload::CapabilityExecution(payload) => payload.validate_for(job, limits),
            #[cfg(any())]
            SandboxJobWorkload::ManagedMcpSubscriptionSession(payload) => {
                payload.validate_for(job, limits)
            }
        }
    }

    pub fn into_capability_execution(
        self,
    ) -> Result<SandboxExecutionJobPayload, SandboxContractError> {
        match self.workload {
            SandboxJobWorkload::CapabilityExecution(payload) => Ok(*payload),
            #[cfg(any())]
            SandboxJobWorkload::ManagedMcpSubscriptionSession(_) => {
                Err(SandboxContractError::InvalidJob)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct AcceptSandboxExecution {
    pub audit: CommandAudit,
    pub request: SandboxExecutionRequest,
    pub usage_reservation_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
}

impl AcceptSandboxExecution {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        self.audit
            .validate_at(now)
            .map_err(|_| SandboxContractError::InvalidWorkerCommand)?;
        self.request.validate_submission_at(now, limits)?;
        if self.audit.tenant_id != self.request.tenant_id
            || self.audit.request_digest != self.request.request_digest
            || self.usage_reservation_id.kind() != ResourceKind::UsageReservation
            || self.quota_entry_ids.len() != SANDBOX_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self.quota_entry_ids.iter().collect::<BTreeSet<_>>().len()
                != self.quota_entry_ids.len()
        {
            return Err(SandboxContractError::InvalidWorkerCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AcceptedSandboxExecution {
    pub job: JobProjection,
    pub payload: SandboxExecutionJobPayload,
}

pub fn decide_accept(
    command: &AcceptSandboxExecution,
    database_now: DateTime<Utc>,
    limits: SandboxCommandLimits,
) -> Result<AcceptedSandboxExecution, SandboxContractError> {
    command.validate_at(database_now, limits)?;
    let job = JobProjection {
        tenant_id: command.request.tenant_id.clone(),
        job_id: command.request.job_id.clone(),
        work_class: WorkClass::Sandbox,
        owner: JobOwnerRef {
            owner_id: command.request.sandbox_job_id.clone(),
            owner_kind: ResourceKind::Job,
        },
        state: JobState::Ready,
        version: 1,
        attempt_count: 0,
        attempt_limit: 1,
        lease_generation: 0,
        lease: None,
        scheduled_at: database_now,
        retry_at: None,
        wake: None,
        deadline: command.request.deadline,
    };
    job.validate()
        .map_err(|_| SandboxContractError::InvalidJob)?;
    let payload = SandboxExecutionJobPayload {
        schema_version: 1,
        request: Box::new(command.request.clone()),
        submission_digest: command.request.request_digest.clone(),
        physical_state: SandboxJobState::Accepted,
        phase_sequence: 0,
        executor_identity_digest: None,
        attestor_route: None,
        phase_evidence_digest: None,
        prepared: None,
        outcome: None,
        cleanup: None,
        payload_digest: command.request.request_digest.clone(),
    }
    .seal()?;
    payload.validate_for(&job, limits)?;
    Ok(AcceptedSandboxExecution { job, payload })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxPhaseDecision {
    pub job: JobProjection,
    pub payload: SandboxExecutionJobPayload,
}

/// Closed recovery action for one exact expired Sandbox lease generation.
///
/// `RequeueUnstarted` and `TimeoutUnstarted` are legal only while the durable physical phase is
/// still `Accepted`. `MarkLost` is legal only after `Preparing` has been committed and therefore
/// requires cleanup/quarantine evidence before the old lease is fenced out.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SandboxLeaseRecoveryAction {
    RequeueUnstarted {
        recovery_evidence_digest: Sha256Digest,
    },
    TimeoutUnstarted {
        recovery_evidence_digest: Sha256Digest,
    },
    MarkLost {
        uncertainty: SandboxUncertainty,
        cleanup: SandboxCleanupEvidence,
        recovery_evidence_digest: Sha256Digest,
    },
}

pub fn decide_begin_execution(
    current_job: &JobProjection,
    current_payload: &SandboxExecutionJobPayload,
    fence: &JobFence,
    executor: crate::WasiExecutorProcessBinding,
    phase_evidence_digest: Sha256Digest,
    database_now: DateTime<Utc>,
    limits: SandboxCommandLimits,
) -> Result<SandboxPhaseDecision, SandboxContractError> {
    current_payload.validate_for(current_job, limits)?;
    if current_payload.physical_state != SandboxJobState::Accepted {
        return Err(SandboxContractError::InvalidTransition);
    }
    let job = decide_start(current_job, fence, database_now)
        .map_err(|_| SandboxContractError::StaleFence)?;
    let mut payload = current_payload.clone();
    payload.physical_state = SandboxJobState::Preparing;
    payload.phase_sequence = 1;
    payload.executor_identity_digest = Some(executor.executor_identity_digest);
    payload.attestor_route = Some(executor.attestor_route);
    payload.phase_evidence_digest = Some(phase_evidence_digest);
    payload = payload.seal()?;
    payload.validate_for(&job, limits)?;
    Ok(SandboxPhaseDecision { job, payload })
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdvanceSandboxPhase {
    pub target: SandboxJobState,
    pub phase_evidence_digest: Sha256Digest,
    pub prepared: Option<PreparedSandbox>,
    pub database_now: DateTime<Utc>,
}

pub fn decide_advance_phase(
    current_job: &JobProjection,
    current_payload: &SandboxExecutionJobPayload,
    fence: &JobFence,
    transition: AdvanceSandboxPhase,
    limits: SandboxCommandLimits,
) -> Result<SandboxPhaseDecision, SandboxContractError> {
    let AdvanceSandboxPhase {
        target,
        phase_evidence_digest,
        prepared,
        database_now,
    } = transition;
    current_payload.validate_for(current_job, limits)?;
    if !matches!(
        target,
        SandboxJobState::Starting
            | SandboxJobState::Running
            | SandboxJobState::Collecting
            | SandboxJobState::Cancelling
    ) || !current_payload.physical_state.can_transition_to(target)
        || (target == SandboxJobState::Starting) != prepared.is_some()
    {
        return Err(SandboxContractError::InvalidTransition);
    }
    let job = decide_observation_update(current_job, fence, database_now)
        .map_err(|_| SandboxContractError::StaleFence)?;
    let mut payload = current_payload.clone();
    payload.physical_state = target;
    payload.phase_sequence = payload
        .phase_sequence
        .checked_add(1)
        .ok_or(SandboxContractError::InvalidTransition)?;
    payload.phase_evidence_digest = Some(phase_evidence_digest);
    if target == SandboxJobState::Starting {
        payload.prepared = prepared;
    }
    payload = payload.seal()?;
    payload.validate_for(&job, limits)?;
    Ok(SandboxPhaseDecision { job, payload })
}

#[allow(clippy::too_many_arguments)]
pub fn decide_execution_outcome(
    current_job: &JobProjection,
    current_payload: &SandboxExecutionJobPayload,
    fence: &JobFence,
    outcome: SandboxExecutionOutcome,
    cleanup: SandboxCleanupEvidence,
    phase_evidence_digest: Sha256Digest,
    database_now: DateTime<Utc>,
    limits: SandboxCommandLimits,
) -> Result<SandboxPhaseDecision, SandboxContractError> {
    current_payload.validate_for(current_job, limits)?;
    outcome.validate_for(&current_payload.request, limits)?;
    cleanup.validate(
        current_payload.request.deadline,
        current_payload.request.resources.cleanup_milliseconds,
    )?;
    let cleanup_milliseconds =
        i64::try_from(current_payload.request.resources.cleanup_milliseconds)
            .map_err(|_| SandboxContractError::InvalidOutcome)?;
    let cleanup_deadline = current_payload
        .request
        .deadline
        .checked_add_signed(chrono::Duration::milliseconds(cleanup_milliseconds))
        .ok_or(SandboxContractError::InvalidOutcome)?;
    let physical_state = outcome.physical_state();
    if !current_payload
        .physical_state
        .can_transition_to(physical_state)
        || (physical_state == SandboxJobState::Succeeded
            && current_payload.physical_state != SandboxJobState::Collecting)
    {
        return Err(SandboxContractError::InvalidTransition);
    }
    let job = match physical_state {
        SandboxJobState::Succeeded => decide_cleanup_terminal(
            current_job,
            fence,
            database_now,
            cleanup_deadline,
            JobState::Succeeded,
        ),
        SandboxJobState::Failed => decide_cleanup_terminal(
            current_job,
            fence,
            database_now,
            cleanup_deadline,
            JobState::Failed,
        ),
        SandboxJobState::Cancelled => decide_cleanup_terminal(
            current_job,
            fence,
            database_now,
            cleanup_deadline,
            JobState::Cancelled,
        ),
        SandboxJobState::TimedOut => decide_cleanup_terminal(
            current_job,
            fence,
            database_now,
            cleanup_deadline,
            JobState::TimedOut,
        ),
        SandboxJobState::Lost => {
            decide_cleanup_reconciliation(current_job, fence, database_now, cleanup_deadline)
        }
        _ => return Err(SandboxContractError::InvalidTransition),
    }
    .map_err(|_| SandboxContractError::StaleFence)?;
    let mut payload = current_payload.clone();
    payload.physical_state = physical_state;
    payload.phase_sequence = payload
        .phase_sequence
        .checked_add(1)
        .ok_or(SandboxContractError::InvalidTransition)?;
    payload.phase_evidence_digest = Some(phase_evidence_digest);
    payload.outcome = Some(outcome);
    payload.cleanup = Some(cleanup);
    payload = payload.seal()?;
    payload.validate_for(&job, limits)?;
    Ok(SandboxPhaseDecision { job, payload })
}

/// Applies the logical owner's committed control decision before any Executor has acquired the
/// Job. No sandbox identity, process, or cleanup evidence is fabricated: the source Event digest
/// is retained as phase evidence and the shared Job moves directly to its owner-controlled
/// terminal state. Cancellation records the required `Accepted -> Cancelling -> Cancelled`
/// physical path in `phase_sequence`; timeout is the direct `Accepted -> TimedOut` edge.
pub fn decide_prestart_control(
    current_job: &JobProjection,
    current_payload: &SandboxExecutionJobPayload,
    reason: SandboxStopReason,
    source_event_payload_digest: Sha256Digest,
    limits: SandboxCommandLimits,
) -> Result<SandboxPhaseDecision, SandboxContractError> {
    current_payload.validate_for(current_job, limits)?;
    if current_payload.physical_state != SandboxJobState::Accepted
        || current_job.state != JobState::Ready
        || current_job.lease.is_some()
        || current_job.attempt_count != 0
        || current_job.lease_generation != 0
    {
        return Err(SandboxContractError::InvalidTransition);
    }
    let (physical_state, terminal, phase_sequence) = match reason {
        SandboxStopReason::Cancelled => {
            if !SandboxJobState::Accepted.can_transition_to(SandboxJobState::Cancelling)
                || !SandboxJobState::Cancelling.can_transition_to(SandboxJobState::Cancelled)
            {
                return Err(SandboxContractError::InvalidTransition);
            }
            (SandboxJobState::Cancelled, JobState::Cancelled, 2)
        }
        SandboxStopReason::TimedOut => {
            if !SandboxJobState::Accepted.can_transition_to(SandboxJobState::TimedOut) {
                return Err(SandboxContractError::InvalidTransition);
            }
            (SandboxJobState::TimedOut, JobState::TimedOut, 1)
        }
    };
    let job = decide_owner_terminal(current_job, terminal)
        .map_err(|_| SandboxContractError::InvalidTransition)?;
    let mut payload = current_payload.clone();
    payload.physical_state = physical_state;
    payload.phase_sequence = phase_sequence;
    payload.phase_evidence_digest = Some(source_event_payload_digest);
    payload = payload.seal()?;
    payload.validate_for(&job, limits)?;
    Ok(SandboxPhaseDecision { job, payload })
}

/// Recovers one exact expired lease without ever reviving a possibly-started execution.
///
/// An `Accepted` Job is safe to requeue because the Executor contract commits `Preparing` before
/// invoking the backend. Once `Preparing` is durable, the only generic recovery is `Lost`; a
/// backend-specific reconnect protocol may instead keep the same generation alive before this
/// first-winner mutation is committed.
#[allow(clippy::too_many_arguments)]
pub fn decide_expired_lease_recovery(
    current_job: &JobProjection,
    current_payload: &SandboxExecutionJobPayload,
    observed_job_version: u64,
    observed_lease_generation: u64,
    action: &SandboxLeaseRecoveryAction,
    database_now: DateTime<Utc>,
    limits: SandboxCommandLimits,
) -> Result<SandboxPhaseDecision, SandboxContractError> {
    current_payload.validate_for(current_job, limits)?;
    let mut payload = current_payload.clone();
    let job = match action {
        SandboxLeaseRecoveryAction::RequeueUnstarted { .. } => {
            if current_payload.physical_state != SandboxJobState::Accepted
                || current_job.state != JobState::Leased
                || database_now >= current_job.deadline
            {
                return Err(SandboxContractError::InvalidTransition);
            }
            decide_expired_lease(
                current_job,
                observed_job_version,
                observed_lease_generation,
                database_now,
                JobState::Ready,
                None,
            )
            .map_err(|_| SandboxContractError::StaleFence)?
        }
        SandboxLeaseRecoveryAction::TimeoutUnstarted {
            recovery_evidence_digest,
        } => {
            if current_payload.physical_state != SandboxJobState::Accepted
                || current_job.state != JobState::Leased
                || database_now < current_job.deadline
                || !SandboxJobState::Accepted.can_transition_to(SandboxJobState::TimedOut)
            {
                return Err(SandboxContractError::InvalidTransition);
            }
            let job = decide_expired_lease(
                current_job,
                observed_job_version,
                observed_lease_generation,
                database_now,
                JobState::TimedOut,
                None,
            )
            .map_err(|_| SandboxContractError::StaleFence)?;
            payload.physical_state = SandboxJobState::TimedOut;
            payload.phase_sequence = 1;
            payload.phase_evidence_digest = Some(recovery_evidence_digest.clone());
            payload = payload.seal()?;
            job
        }
        SandboxLeaseRecoveryAction::MarkLost {
            uncertainty,
            cleanup,
            recovery_evidence_digest,
        } => {
            if current_payload.physical_state == SandboxJobState::Accepted
                || !matches!(current_job.state, JobState::Running | JobState::Cancelling)
                || !current_payload
                    .physical_state
                    .can_transition_to(SandboxJobState::Lost)
                || cleanup.observed_at > database_now
            {
                return Err(SandboxContractError::InvalidTransition);
            }
            cleanup.validate_recovery()?;
            uncertainty.validate_for(&current_payload.request)?;
            let execution_may_have_started =
                !matches!(current_payload.physical_state, SandboxJobState::Preparing);
            let external_effect_possible = execution_may_have_started
                && (current_payload.request.effect.risk_rank()
                    >= insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
                    || current_payload.request.network_mode != crate::SandboxNetworkMode::None);
            if uncertainty.execution_may_have_started != execution_may_have_started
                || uncertainty.external_effect_possible != external_effect_possible
                || uncertainty.manual_reconciliation_required != external_effect_possible
                || uncertainty.sandbox_identity_digest != cleanup.sandbox_identity_digest
            {
                return Err(SandboxContractError::InvalidOutcome);
            }
            let job = decide_expired_lease(
                current_job,
                observed_job_version,
                observed_lease_generation,
                database_now,
                JobState::ReconciliationRequired,
                None,
            )
            .map_err(|_| SandboxContractError::StaleFence)?;
            payload.physical_state = SandboxJobState::Lost;
            payload.phase_sequence = payload
                .phase_sequence
                .checked_add(1)
                .ok_or(SandboxContractError::InvalidTransition)?;
            payload.phase_evidence_digest = Some(recovery_evidence_digest.clone());
            payload.outcome = Some(SandboxExecutionOutcome::Uncertain(uncertainty.clone()));
            payload.cleanup = Some(cleanup.clone());
            payload = payload.seal()?;
            job
        }
    };
    payload.validate_for(&job, limits)?;
    Ok(SandboxPhaseDecision { job, payload })
}
