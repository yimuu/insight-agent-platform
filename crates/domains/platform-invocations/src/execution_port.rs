//! Exact, bounded execution requests and outcome authority ports.
use crate::*;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::*;
use insight_platform_jobs::JobFence;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityAdapterRequest {
    pub tenant_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub worker_manifest_digest: Sha256Digest,
    pub lease_generation: u64,
    pub physical_attempt: u32,
    pub attempt_limit: u32,
    pub admission_digest: Sha256Digest,
    pub output_schema_digest: Sha256Digest,
    pub idempotency_key_digest: Sha256Digest,
    pub effect: Effect,
    pub idempotency: insight_platform_contracts::CapabilityIdempotencyKind,
    pub deadline: DateTime<Utc>,
    pub execution: CapabilityExecutionContract,
    pub input: CapabilityExecutionInput,
    pub continuation: Option<CapabilityAdapterContinuation>,
    pub mcp_runtime: Option<crate::McpCapabilityRuntimeBinding>,
}
impl CapabilityAdapterRequest {
    /// Validates the immutable identity, execution closure and bounded input independently of the
    /// dispatch clock. Cancellation and cleanup must still be able to address the exact physical
    /// request after its execution deadline has elapsed.
    pub fn validate_shape(&self) -> Result<(), InvocationError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation == 0
            || self.physical_attempt == 0
            || self.attempt_limit == 0
            || self.physical_attempt > self.attempt_limit
            || self.execution.validate().is_err()
            || (self.execution.implementation.backend_kind == CapabilityBackendKind::Mcp)
                != self.mcp_runtime.is_some()
            || self
                .mcp_runtime
                .as_ref()
                .is_some_and(|binding| binding.validate().is_err())
            || self.continuation.as_ref().is_some_and(|continuation| {
                let features = &self.execution.implementation.features;
                (!features.deferred && !features.input_required)
                    || continuation
                        .encrypted_remote_state
                        .validate(features.max_remote_state_bytes)
                        .is_err()
                    || continuation.poll_count > features.max_poll_count
                    || continuation.resume_input.as_ref().is_some_and(|input| {
                        !features.input_required
                            || validate_execution_input(
                                input,
                                self.execution
                                    .implementation
                                    .backend_limits
                                    .maximum_request_bytes,
                            )
                            .is_err()
                    })
                    || match continuation.resume_input_action {
                        None => continuation.resume_input.is_some(),
                        Some(CapabilityInputAction::Accept) => continuation.resume_input.is_none(),
                        Some(CapabilityInputAction::Decline | CapabilityInputAction::Cancel) => {
                            continuation.resume_input.is_some()
                        }
                    }
            })
        {
            return Err(InvocationError::InvalidCommand);
        }
        validate_execution_input(
            &self.input,
            self.execution
                .implementation
                .backend_limits
                .maximum_request_bytes,
        )
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), InvocationError> {
        self.validate_shape()?;
        if self.deadline <= now {
            return Err(InvocationError::InvalidCommand);
        }
        Ok(())
    }
}
/// Durable backend continuation recovered from the claimed Capability Job.
///
/// The state remains encrypted and opaque at this boundary. Only the exact backend codec may
/// unseal it, while PostgreSQL remains authoritative for the physical attempt and the current Job
/// fence. `resume_input` contains bounded RunValue material only after an InputRequired winner has
/// committed it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityAdapterContinuation {
    pub encrypted_remote_state: EncryptedRemoteState,
    pub external_identity_digest: Option<Sha256Digest>,
    pub resume_input: Option<CapabilityExecutionInput>,
    pub resume_input_action: Option<CapabilityInputAction>,
    pub poll_count: u32,
}
/// Exact command handed to one Capability Worker after a PostgreSQL claim transaction commits.
///
/// The command contains no plaintext Secret and grants no mutation access to Run or Invocation
/// state. The worker may execute the exact adapter request and submit one fenced outcome through
/// [`CapabilityExecutionAuthority`].
#[derive(Debug, Clone)]
pub struct ExecuteCapabilityAdapterJob {
    pub execution: CapabilityAdapterRequest,
    pub audit: CapabilityWorkerAudit,
    pub expected_invocation_version: u64,
    pub fence: JobFence,
    pub quota_entry_ids: Vec<ResourceId>,
    /// Retry time already intersected with the frozen policy and remaining Run/Invocation budget.
    /// It is consumed only when the adapter returns a safely retryable failure.
    pub retry_at: Option<DateTime<Utc>>,
    pub resume_mutations: Option<ExternalLeafResumeMutationIds>,
    pub failure_mutations: Option<ExternalLeafFailureMutationIds>,
}
impl ExecuteCapabilityAdapterJob {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), InvocationError> {
        self.audit
            .validate_at(now)
            .map_err(|_| InvocationError::InvalidCommand)?;
        self.execution
            .validate_at(now)
            .map_err(|_| InvocationError::InvalidCommand)?;
        if self.expected_invocation_version == 0
            || self.audit.tenant_id != self.execution.tenant_id
            || self.audit.worker_process_generation_id
                != self.execution.worker_process_generation_id
            || self.fence.expected_version == 0
            || self.fence.worker_process_generation_id
                != self.execution.worker_process_generation_id
            || self.fence.lease_generation != self.execution.lease_generation
            || self.quota_entry_ids.len() != CAPABILITY_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self.quota_entry_ids.iter().collect::<BTreeSet<_>>().len()
                != self.quota_entry_ids.len()
            || self.retry_at.is_some_and(|retry_at| {
                self.execution.physical_attempt >= self.execution.attempt_limit
                    || retry_at <= now
                    || retry_at >= self.execution.deadline
            })
            || self
                .resume_mutations
                .as_ref()
                .is_some_and(|mutations| mutations.validate().is_err())
            || self
                .failure_mutations
                .as_ref()
                .is_some_and(|mutations| mutations.validate().is_err())
            || self.resume_mutations.is_some() != self.failure_mutations.is_some()
        {
            return Err(InvocationError::InvalidCommand);
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct CancelCapabilityAdapterJob {
    pub execution: CapabilityAdapterRequest,
    pub audit: CapabilityWorkerAudit,
    pub expected_invocation_version: u64,
    pub fence: JobFence,
    pub quota_entry_ids: Vec<ResourceId>,
    pub cancel_deadline: DateTime<Utc>,
}
impl CancelCapabilityAdapterJob {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), InvocationError> {
        self.audit
            .validate_at(now)
            .map_err(|_| InvocationError::InvalidCommand)?;
        self.execution
            .validate_shape()
            .map_err(|_| InvocationError::InvalidCommand)?;
        if self.expected_invocation_version == 0
            || self.audit.tenant_id != self.execution.tenant_id
            || self.audit.worker_process_generation_id
                != self.execution.worker_process_generation_id
            || self.fence.expected_version == 0
            || self.fence.worker_process_generation_id
                != self.execution.worker_process_generation_id
            || self.fence.lease_generation != self.execution.lease_generation
            || self.quota_entry_ids.len() != CAPABILITY_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self.quota_entry_ids.iter().collect::<BTreeSet<_>>().len()
                != self.quota_entry_ids.len()
            || self.cancel_deadline <= now
            || self.cancel_deadline
                > self
                    .cleanup_deadline()
                    .ok_or(InvocationError::InvalidCommand)?
            || !self
                .execution
                .execution
                .implementation
                .features
                .cancellation
            || !matches!(
                self.execution.execution.implementation.backend_kind,
                insight_platform_contracts::CapabilityBackendKind::Native
                    | insight_platform_contracts::CapabilityBackendKind::Http
                    | insight_platform_contracts::CapabilityBackendKind::Grpc
                    | insight_platform_contracts::CapabilityBackendKind::Mcp
            )
        {
            return Err(InvocationError::InvalidCommand);
        }
        Ok(())
    }

    /// Bounded authority window for persisting the cancellation observation. It is derived from
    /// the frozen execution deadline and backend total timeout rather than accepted from a caller.
    pub fn cleanup_deadline(&self) -> Option<DateTime<Utc>> {
        let milliseconds = i64::try_from(
            self.execution
                .execution
                .implementation
                .backend_limits
                .total_timeout_milliseconds,
        )
        .ok()?;
        self.execution
            .deadline
            .checked_add_signed(chrono::Duration::milliseconds(milliseconds))
    }
}
#[async_trait]
pub trait CapabilityExecutionAuthority: Send + Sync {
    type Error;
    type Record;

    async fn commit_capability_outcome(
        &self,
        command: CommitCapabilityOutcome,
    ) -> Result<CommandOutcome<Self::Record>, Self::Error>;

    async fn commit_capability_cancellation_outcome(
        &self,
        command: CommitCapabilityCancellationOutcome,
    ) -> Result<CommandOutcome<Self::Record>, Self::Error>;
}
pub fn validate_execution_input(
    input: &CapabilityExecutionInput,
    maximum_request_bytes: u32,
) -> Result<(), InvocationError> {
    input
        .validate()
        .map_err(|_| InvocationError::InvalidCommand)?;
    match (&input.exact.storage, &input.material) {
        (InvocationValueStorage::Inline, CapabilityExecutionInputMaterial::Inline { value }) => {
            let encoded = serde_json::to_vec(value).map_err(|_| InvocationError::InvalidCommand)?;
            if encoded.len()
                > usize::try_from(maximum_request_bytes)
                    .map_err(|_| InvocationError::InvalidCommand)?
            {
                return Err(InvocationError::InvalidCommand);
            }
        }
        (
            InvocationValueStorage::Artifact { artifact },
            CapabilityExecutionInputMaterial::LinkedArtifact { .. },
        ) if artifact.byte_length() <= u64::from(maximum_request_bytes) => {}
        _ => return Err(InvocationError::InvalidCommand),
    }
    Ok(())
}
