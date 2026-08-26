use crate::{
    AcceptSandboxExecution, NodeAttestorRoute, PreparedSandbox, SandboxCleanupEvidence,
    SandboxCommandLimits, SandboxContractError, SandboxControlError, SandboxExecutionControlRouter,
    SandboxExecutionOutcome, SandboxExecutionRequest, SandboxExecutionStopToken,
    SandboxExecutorHost, SandboxLeaseRecoveryAction, SandboxLifecycleObservation,
    SandboxLifecycleObserver, SandboxObservedExecutionError, SandboxPhaseDecision,
    SANDBOX_QUOTA_LINES,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, CommandOutcome, HardLimitProfile, ResourceId, ResourceKind, SandboxJobState,
    Sha256Digest,
};
use insight_platform_jobs::JobFence;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::BTreeSet, error::Error, fmt, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Worker-owned Receipt/Event/Outbox identity for one fenced Sandbox phase commit.
///
/// It intentionally has no Principal: the authenticated WorkerProcessGeneration in the Job fence
/// is the durable actor. Secret values, backend diagnostics and raw guest output have no field in
/// this contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxWorkerAudit {
    pub tenant_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl SandboxWorkerAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SandboxWorkerContractError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= now
        {
            return Err(SandboxWorkerContractError::InvalidAudit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxWorkerCommitIdentity {
    pub tenant_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl SandboxWorkerCommitIdentity {
    pub(crate) fn materialize(&self, request_digest: Sha256Digest) -> SandboxWorkerAudit {
        SandboxWorkerAudit {
            tenant_id: self.tenant_id.clone(),
            worker_process_generation_id: self.worker_process_generation_id.clone(),
            receipt_id: self.receipt_id.clone(),
            event_id: self.event_id.clone(),
            outbox_id: self.outbox_id.clone(),
            idempotency_key_digest: self.idempotency_key_digest.clone(),
            request_digest,
            receipt_expires_at: self.receipt_expires_at,
        }
    }

    pub(crate) fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SandboxWorkerContractError> {
        self.materialize(self.idempotency_key_digest.clone())
            .validate_at(now)
    }
}

/// Exact identities for a successful attempt plus the optional cancellation phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxWorkerAuditBundle {
    pub preparing: SandboxWorkerCommitIdentity,
    pub starting: SandboxWorkerCommitIdentity,
    pub running: SandboxWorkerCommitIdentity,
    pub collecting: SandboxWorkerCommitIdentity,
    pub cancelling: SandboxWorkerCommitIdentity,
    pub terminal: SandboxWorkerCommitIdentity,
}

impl SandboxWorkerAuditBundle {
    pub fn validate_at(
        &self,
        tenant_id: &ResourceId,
        worker_process_generation_id: &ResourceId,
        now: DateTime<Utc>,
    ) -> Result<(), SandboxWorkerContractError> {
        let audits = self.all();
        for audit in audits {
            audit.validate_at(now)?;
            if &audit.tenant_id != tenant_id
                || &audit.worker_process_generation_id != worker_process_generation_id
            {
                return Err(SandboxWorkerContractError::InvalidAudit);
            }
        }
        if !all_distinct(audits.iter().map(|audit| &audit.receipt_id))
            || !all_distinct(audits.iter().map(|audit| &audit.event_id))
            || !all_distinct(audits.iter().map(|audit| &audit.outbox_id))
            || !all_distinct(audits.iter().map(|audit| &audit.idempotency_key_digest))
        {
            return Err(SandboxWorkerContractError::DuplicateCommitIdentity);
        }
        Ok(())
    }

    pub fn for_phase(&self, phase: SandboxJobState) -> Option<&SandboxWorkerCommitIdentity> {
        match phase {
            SandboxJobState::Preparing => Some(&self.preparing),
            SandboxJobState::Starting => Some(&self.starting),
            SandboxJobState::Running => Some(&self.running),
            SandboxJobState::Collecting => Some(&self.collecting),
            SandboxJobState::Cancelling => Some(&self.cancelling),
            SandboxJobState::Succeeded
            | SandboxJobState::Failed
            | SandboxJobState::Cancelled
            | SandboxJobState::TimedOut
            | SandboxJobState::Lost => Some(&self.terminal),
            SandboxJobState::Accepted => None,
        }
    }

    fn all(&self) -> [&SandboxWorkerCommitIdentity; 6] {
        [
            &self.preparing,
            &self.starting,
            &self.running,
            &self.collecting,
            &self.cancelling,
            &self.terminal,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitSandboxPhase {
    pub audit: SandboxWorkerAudit,
    pub sandbox_job_id: ResourceId,
    pub job_id: ResourceId,
    pub fence: JobFence,
    pub target: SandboxJobState,
    pub executor_identity_digest: Sha256Digest,
    pub attestor_route: NodeAttestorRoute,
    pub phase_evidence_digest: Sha256Digest,
    pub prepared: Option<PreparedSandbox>,
}

impl CommitSandboxPhase {
    pub fn canonical_request_digest(&self) -> Result<Sha256Digest, SandboxWorkerContractError> {
        canonical_digest(&json!({
            "schema_version": 1,
            "tenant_id": self.audit.tenant_id,
            "sandbox_job_id": self.sandbox_job_id,
            "job_id": self.job_id,
            "expected_job_version": self.fence.expected_version,
            "worker_process_generation_id": self.fence.worker_process_generation_id,
            "lease_generation": self.fence.lease_generation,
            "lease_token_digest": self.fence.token_digest,
            "target": self.target,
            "executor_identity_digest": self.executor_identity_digest,
            "attestor_route": self.attestor_route,
            "phase_evidence_digest": self.phase_evidence_digest,
            "prepared": self.prepared,
        }))
        .map_err(|_| SandboxWorkerContractError::Canonicalization)?
        .parse()
        .map_err(|_| SandboxWorkerContractError::Canonicalization)
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SandboxWorkerContractError> {
        self.audit.validate_at(now)?;
        if self.sandbox_job_id.kind() != ResourceKind::Job
            || self.job_id.kind() != ResourceKind::Job
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
            || !matches!(
                self.target,
                SandboxJobState::Preparing
                    | SandboxJobState::Starting
                    | SandboxJobState::Running
                    | SandboxJobState::Collecting
                    | SandboxJobState::Cancelling
            )
            || (self.target == SandboxJobState::Starting) != self.prepared.is_some()
            || self.audit.request_digest != self.canonical_request_digest()?
        {
            return Err(SandboxWorkerContractError::InvalidPhaseCommit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitSandboxOutcome {
    pub audit: SandboxWorkerAudit,
    pub sandbox_job_id: ResourceId,
    pub job_id: ResourceId,
    pub fence: JobFence,
    pub outcome: SandboxExecutionOutcome,
    pub cleanup: SandboxCleanupEvidence,
    pub phase_evidence_digest: Sha256Digest,
    pub usage_reservation_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
}

impl CommitSandboxOutcome {
    pub fn canonical_request_digest(&self) -> Result<Sha256Digest, SandboxWorkerContractError> {
        canonical_digest(&json!({
            "schema_version": 1,
            "tenant_id": self.audit.tenant_id,
            "sandbox_job_id": self.sandbox_job_id,
            "job_id": self.job_id,
            "expected_job_version": self.fence.expected_version,
            "worker_process_generation_id": self.fence.worker_process_generation_id,
            "lease_generation": self.fence.lease_generation,
            "lease_token_digest": self.fence.token_digest,
            "outcome": self.outcome,
            "cleanup": self.cleanup,
            "phase_evidence_digest": self.phase_evidence_digest,
            "usage_reservation_id": self.usage_reservation_id,
            "quota_entry_ids": self.quota_entry_ids,
        }))
        .map_err(|_| SandboxWorkerContractError::Canonicalization)?
        .parse()
        .map_err(|_| SandboxWorkerContractError::Canonicalization)
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SandboxWorkerContractError> {
        self.audit.validate_at(now)?;
        if self.sandbox_job_id.kind() != ResourceKind::Job
            || self.job_id.kind() != ResourceKind::Job
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
            || self.usage_reservation_id.kind() != ResourceKind::UsageReservation
            || self.quota_entry_ids.len() != SANDBOX_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || !all_distinct(self.quota_entry_ids.iter())
            || self.audit.request_digest != self.canonical_request_digest()?
        {
            return Err(SandboxWorkerContractError::InvalidTerminalCommit);
        }
        Ok(())
    }
}

/// The only port through which an Executor may advance shared durable Job authority.
#[async_trait]
pub trait SandboxExecutionAuthority: Send + Sync {
    type Error: Send;

    async fn commit_sandbox_phase(
        &self,
        command: CommitSandboxPhase,
    ) -> Result<CommandOutcome<SandboxPhaseDecision>, Self::Error>;

    async fn commit_sandbox_outcome(
        &self,
        command: CommitSandboxOutcome,
    ) -> Result<CommandOutcome<SandboxPhaseDecision>, Self::Error>;

    async fn heartbeat_sandbox_execution(
        &self,
        command: HeartbeatSandboxExecution,
    ) -> Result<SandboxPhaseDecision, Self::Error>;
}

/// Non-evented lease renewal for one exact Sandbox execution generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeartbeatSandboxExecution {
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub job_id: ResourceId,
    pub fence: JobFence,
    pub lease_milliseconds: u64,
}

impl HeartbeatSandboxExecution {
    pub fn validate(
        &self,
        maximum_lease_milliseconds: u64,
    ) -> Result<(), SandboxWorkerContractError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.sandbox_job_id.kind() != ResourceKind::Job
            || self.job_id.kind() != ResourceKind::Job
            || self.fence.expected_version == 0
            || self.fence.worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.fence.lease_generation == 0
            || self.lease_milliseconds == 0
            || self.lease_milliseconds > maximum_lease_milliseconds
        {
            return Err(SandboxWorkerContractError::InvalidHeartbeat);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxHeartbeatConfig {
    interval: Duration,
    lease_milliseconds: u64,
    maximum_lease_milliseconds: u64,
}

impl SandboxHeartbeatConfig {
    pub fn from_profile(profile: &HardLimitProfile) -> Result<Self, SandboxWorkerContractError> {
        profile
            .validate()
            .map_err(|_| SandboxWorkerContractError::InvalidHeartbeat)?;
        let config = Self {
            interval: Duration::from_millis(
                profile.run_scheduler.heartbeat_milliseconds.q1_default,
            ),
            lease_milliseconds: profile.run_scheduler.lease_milliseconds.q1_default,
            maximum_lease_milliseconds: profile.run_scheduler.lease_milliseconds.hard_max,
        };
        config.validate()?;
        Ok(config)
    }

    pub const fn interval(self) -> Duration {
        self.interval
    }

    pub const fn lease_milliseconds(self) -> u64 {
        self.lease_milliseconds
    }

    pub fn bounded(
        interval: Duration,
        lease_milliseconds: u64,
        maximum_lease_milliseconds: u64,
    ) -> Result<Self, SandboxWorkerContractError> {
        let config = Self {
            interval,
            lease_milliseconds,
            maximum_lease_milliseconds,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(self) -> Result<(), SandboxWorkerContractError> {
        if self.interval.is_zero()
            || self.lease_milliseconds == 0
            || self.lease_milliseconds > self.maximum_lease_milliseconds
            || self
                .interval
                .checked_mul(3)
                .is_none_or(|margin| margin >= Duration::from_millis(self.lease_milliseconds))
        {
            return Err(SandboxWorkerContractError::InvalidHeartbeat);
        }
        Ok(())
    }
}

#[async_trait]
pub trait SandboxGatewayAuthority: Send + Sync {
    type Error;

    async fn accept_sandbox_execution(
        &self,
        command: AcceptSandboxExecution,
    ) -> Result<CommandOutcome<SandboxPhaseDecision>, Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimSandboxJobs {
    pub worker_process_generation_id: ResourceId,
    pub worker_manifest_digest: Sha256Digest,
    pub isolation_backend_contract_digest: Sha256Digest,
    pub executor_identity_digest: Sha256Digest,
    pub attestor_route: NodeAttestorRoute,
    pub limit: u16,
    pub lease_milliseconds: u64,
    pub lease_token_digests: Vec<Sha256Digest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxClaimFailure {
    Unavailable,
    FirstWinnerLost,
    InvariantViolation,
}

/// Executor-facing durable claim port.
///
/// The dedicated Executor implements this with the authenticated internal Sandbox service. The
/// Controller may adapt PostgreSQL behind that service, but this port deliberately exposes no
/// database client or transaction handle to the execution process.
#[async_trait]
pub trait SandboxClaimAuthority: Send + Sync + 'static {
    async fn claim_sandbox_jobs(
        &self,
        command: ClaimSandboxJobs,
    ) -> Result<Vec<ClaimedSandboxJob>, SandboxClaimFailure>;
}

impl ClaimSandboxJobs {
    pub fn validate(
        &self,
        maximum_batch: u16,
        maximum_lease_milliseconds: u64,
    ) -> Result<(), SandboxWorkerContractError> {
        if self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.limit == 0
            || self.limit > maximum_batch
            || self.lease_milliseconds == 0
            || self.lease_milliseconds > maximum_lease_milliseconds
            || self.lease_token_digests.len() != usize::from(self.limit)
            || !all_distinct(self.lease_token_digests.iter())
        {
            return Err(SandboxWorkerContractError::InvalidClaim);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimedSandboxJob {
    pub trace: insight_platform_contracts::TraceIdentityV1,
    pub request: SandboxExecutionRequest,
    pub fence: JobFence,
    pub usage_reservation_id: ResourceId,
}

/// Read-only projection returned by the bounded expired-lease scan.
///
/// The lease token is intentionally omitted. Recovery addresses the backend by the exact signed
/// request/attempt/generation and commits through an optimistic Job version/generation check.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpiredSandboxLease {
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub request: SandboxExecutionRequest,
    pub observed_job_version: u64,
    pub observed_lease_generation: u64,
    pub previous_worker_process_generation_id: ResourceId,
    pub lease_expires_at: DateTime<Utc>,
    pub database_observed_at: DateTime<Utc>,
    pub physical_state: SandboxJobState,
    pub executor_identity_digest: Option<Sha256Digest>,
    pub attestor_route: Option<NodeAttestorRoute>,
}

impl ExpiredSandboxLease {
    pub fn validate(&self, limits: SandboxCommandLimits) -> Result<(), SandboxWorkerContractError> {
        let request_validation_time = self
            .lease_expires_at
            .checked_sub_signed(chrono::Duration::milliseconds(1))
            .ok_or(SandboxWorkerContractError::InvalidRecovery)?;
        self.request.validate_at(request_validation_time, limits)?;
        let active_started = matches!(
            self.physical_state,
            SandboxJobState::Preparing
                | SandboxJobState::Starting
                | SandboxJobState::Running
                | SandboxJobState::Collecting
                | SandboxJobState::Cancelling
        );
        if self.tenant_id != self.request.tenant_id
            || self.sandbox_job_id != self.request.sandbox_job_id
            || self.invocation_id != self.request.invocation_id
            || self.job_id != self.request.job_id
            || self.observed_job_version == 0
            || self.observed_lease_generation == 0
            || self.observed_lease_generation != self.request.lease_generation
            || self.previous_worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.lease_expires_at > self.database_observed_at
            || (self.physical_state == SandboxJobState::Accepted
                && (self.executor_identity_digest.is_some() || self.attestor_route.is_some()))
            || (active_started
                && (self.executor_identity_digest.is_none() || self.attestor_route.is_none()))
            || (!active_started && self.physical_state != SandboxJobState::Accepted)
        {
            return Err(SandboxWorkerContractError::InvalidRecovery);
        }
        Ok(())
    }
}

/// Recovery-process identity and mutation IDs for one exact expired lease commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxRecoveryAudit {
    pub tenant_id: ResourceId,
    pub recovery_process_generation_id: ResourceId,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl SandboxRecoveryAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SandboxWorkerContractError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.recovery_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= now
        {
            return Err(SandboxWorkerContractError::InvalidAudit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoverExpiredSandboxLease {
    pub audit: SandboxRecoveryAudit,
    pub sandbox_job_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub sandbox_request_digest: Sha256Digest,
    pub observed_job_version: u64,
    pub observed_lease_generation: u64,
    pub previous_worker_process_generation_id: ResourceId,
    pub action: SandboxLeaseRecoveryAction,
    pub quota_entry_ids: Vec<ResourceId>,
}

impl RecoverExpiredSandboxLease {
    pub fn canonical_idempotency_key_digest(
        &self,
    ) -> Result<Sha256Digest, SandboxWorkerContractError> {
        canonical_digest(&json!({
            "schema_version": 1,
            "job_id": self.job_id,
            "observed_lease_generation": self.observed_lease_generation,
        }))
        .map_err(|_| SandboxWorkerContractError::Canonicalization)?
        .parse()
        .map_err(|_| SandboxWorkerContractError::Canonicalization)
    }

    pub fn canonical_request_digest(&self) -> Result<Sha256Digest, SandboxWorkerContractError> {
        canonical_digest(&json!({
            "schema_version": 1,
            "tenant_id": self.audit.tenant_id,
            "sandbox_job_id": self.sandbox_job_id,
            "invocation_id": self.invocation_id,
            "job_id": self.job_id,
            "sandbox_request_digest": self.sandbox_request_digest,
            "observed_job_version": self.observed_job_version,
            "observed_lease_generation": self.observed_lease_generation,
            "previous_worker_process_generation_id": self.previous_worker_process_generation_id,
            "action": self.action,
        }))
        .map_err(|_| SandboxWorkerContractError::Canonicalization)?
        .parse()
        .map_err(|_| SandboxWorkerContractError::Canonicalization)
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SandboxWorkerContractError> {
        self.audit.validate_at(now)?;
        let terminal = !matches!(
            self.action,
            SandboxLeaseRecoveryAction::RequeueUnstarted { .. }
        );
        if self.sandbox_job_id.kind() != ResourceKind::Job
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.observed_job_version == 0
            || self.observed_lease_generation == 0
            || self.previous_worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || (terminal && self.quota_entry_ids.len() != SANDBOX_QUOTA_LINES)
            || (!terminal && !self.quota_entry_ids.is_empty())
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || !self
                .quota_entry_ids
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || self.audit.idempotency_key_digest != self.canonical_idempotency_key_digest()?
            || self.audit.request_digest != self.canonical_request_digest()?
        {
            return Err(SandboxWorkerContractError::InvalidRecovery);
        }
        if let SandboxLeaseRecoveryAction::MarkLost { cleanup, .. } = &self.action {
            if cleanup.observed_at > now {
                return Err(SandboxWorkerContractError::InvalidRecovery);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxLeaseRecoveryDisposition {
    Requeued,
    TimedOut,
    Lost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxLeaseRecoveryResult {
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub job_id: ResourceId,
    pub recovered_lease_generation: u64,
    pub disposition: SandboxLeaseRecoveryDisposition,
}

#[async_trait]
pub trait SandboxLeaseRecoveryAuthority: Send + Sync {
    type Error;

    async fn recover_expired_sandbox_lease(
        &self,
        command: RecoverExpiredSandboxLease,
    ) -> Result<CommandOutcome<SandboxLeaseRecoveryResult>, Self::Error>;
}

impl ClaimedSandboxJob {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxWorkerContractError> {
        self.trace
            .validate()
            .map_err(|_| SandboxWorkerContractError::InvalidClaim)?;
        self.request.validate_at(now, limits)?;
        if self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.lease_generation != self.request.lease_generation
            || self.fence.worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.usage_reservation_id.kind() != ResourceKind::UsageReservation
        {
            return Err(SandboxWorkerContractError::InvalidClaim);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ExecuteSandboxJob {
    pub request: SandboxExecutionRequest,
    pub fence: JobFence,
    pub executor_identity_digest: Sha256Digest,
    pub attestor_route: NodeAttestorRoute,
    pub audits: SandboxWorkerAuditBundle,
    pub usage_reservation_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
}

impl ExecuteSandboxJob {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxWorkerContractError> {
        self.request.validate_at(now, limits)?;
        self.audits.validate_at(
            &self.request.tenant_id,
            &self.fence.worker_process_generation_id,
            now,
        )?;
        if self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.lease_generation != self.request.lease_generation
            || self.usage_reservation_id.kind() != ResourceKind::UsageReservation
            || self.quota_entry_ids.len() != SANDBOX_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || !all_distinct(self.quota_entry_ids.iter())
        {
            return Err(SandboxWorkerContractError::InvalidExecutionCommand);
        }
        Ok(())
    }
}

pub struct SandboxExecutorWorker<A> {
    host: Arc<SandboxExecutorHost>,
    authority: Arc<A>,
    limits: SandboxCommandLimits,
    heartbeat: SandboxHeartbeatConfig,
}

impl<A> SandboxExecutorWorker<A>
where
    A: SandboxExecutionAuthority,
{
    pub fn new(
        host: Arc<SandboxExecutorHost>,
        authority: Arc<A>,
        limits: SandboxCommandLimits,
    ) -> Self {
        Self {
            host,
            authority,
            limits,
            heartbeat: SandboxHeartbeatConfig::from_profile(
                &insight_platform_contracts::checked_in_hard_limit_profile(),
            )
            .expect("checked-in HardLimitProfile has valid Sandbox heartbeat timing"),
        }
    }

    pub fn with_heartbeat(
        host: Arc<SandboxExecutorHost>,
        authority: Arc<A>,
        limits: SandboxCommandLimits,
        heartbeat: SandboxHeartbeatConfig,
    ) -> Result<Self, SandboxWorkerContractError> {
        heartbeat.validate()?;
        Ok(Self {
            host,
            authority,
            limits,
            heartbeat,
        })
    }

    pub async fn execute(
        &self,
        command: ExecuteSandboxJob,
    ) -> Result<CommandOutcome<SandboxPhaseDecision>, SandboxExecutorWorkerError<A::Error>> {
        self.execute_cancellable(command, CancellationToken::new())
            .await
    }

    pub async fn execute_cancellable(
        &self,
        command: ExecuteSandboxJob,
        cancellation: CancellationToken,
    ) -> Result<CommandOutcome<SandboxPhaseDecision>, SandboxExecutorWorkerError<A::Error>> {
        self.execute_with_stop_token(
            command,
            SandboxExecutionStopToken::from_cancellation(cancellation),
        )
        .await
    }

    /// Registers the exact leased generation before execution so a committed control event can
    /// stop it without relying on guest cooperation or an unfenced process lookup.
    pub async fn execute_registered(
        &self,
        command: ExecuteSandboxJob,
        router: &SandboxExecutionControlRouter,
    ) -> Result<CommandOutcome<SandboxPhaseDecision>, SandboxExecutorWorkerError<A::Error>> {
        command
            .validate_at(Utc::now(), self.limits)
            .map_err(SandboxExecutorWorkerError::Contract)?;
        let registration = router
            .register(
                &command.request,
                &command.fence.worker_process_generation_id,
            )
            .map_err(SandboxExecutorWorkerError::Control)?;
        self.execute_with_stop_token(command, registration.stop_token())
            .await
    }

    async fn execute_with_stop_token(
        &self,
        command: ExecuteSandboxJob,
        stop: SandboxExecutionStopToken,
    ) -> Result<CommandOutcome<SandboxPhaseDecision>, SandboxExecutorWorkerError<A::Error>> {
        command
            .validate_at(Utc::now(), self.limits)
            .map_err(SandboxExecutorWorkerError::Contract)?;
        let observer = Arc::new(DurableSandboxLifecycleObserver::new(
            Arc::clone(&self.authority),
            &command,
            self.limits,
        ));
        let host = self.host.execute_observed_with_stop_token(
            command.request,
            command.executor_identity_digest,
            observer.as_ref(),
            stop.clone(),
        );
        tokio::pin!(host);
        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + self.heartbeat.interval(),
            self.heartbeat.interval(),
        );
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let host_result = loop {
            let heartbeat_once = async {
                heartbeat.tick().await;
                observer
                    .heartbeat(
                        self.heartbeat.lease_milliseconds(),
                        self.heartbeat.maximum_lease_milliseconds,
                    )
                    .await
            };
            tokio::pin!(heartbeat_once);
            tokio::select! {
                biased;
                result = &mut host => break result,
                heartbeat_result = &mut heartbeat_once => {
                    if let Err(failure) = heartbeat_result {
                        stop.request_lease_lost()
                            .map_err(SandboxExecutorWorkerError::Control)?;
                        let _ = host.await;
                        return Err(match failure {
                            SandboxWorkerObserverError::Contract(error) => {
                                SandboxExecutorWorkerError::Contract(error)
                            }
                            SandboxWorkerObserverError::Authority(error) => {
                                SandboxExecutorWorkerError::Authority(error)
                            }
                        });
                    }
                }
            }
        };
        host_result.map_err(|error| match error {
            SandboxObservedExecutionError::Host(error) => SandboxExecutorWorkerError::Host(error),
            SandboxObservedExecutionError::Observer(SandboxWorkerObserverError::Contract(
                error,
            )) => SandboxExecutorWorkerError::Contract(error),
            SandboxObservedExecutionError::Observer(SandboxWorkerObserverError::Authority(
                error,
            )) => SandboxExecutorWorkerError::Authority(error),
        })?;
        observer
            .take_terminal()
            .await
            .ok_or(SandboxExecutorWorkerError::Contract(
                SandboxWorkerContractError::MissingTerminalDecision,
            ))
    }
}

struct DurableSandboxLifecycleObserver<A> {
    authority: Arc<A>,
    state: Mutex<DurableSandboxObserverState>,
    limits: SandboxCommandLimits,
}

struct DurableSandboxObserverState {
    tenant_id: ResourceId,
    sandbox_job_id: ResourceId,
    job_id: ResourceId,
    fence: JobFence,
    audits: SandboxWorkerAuditBundle,
    usage_reservation_id: ResourceId,
    quota_entry_ids: Vec<ResourceId>,
    attestor_route: NodeAttestorRoute,
    physical_state: SandboxJobState,
    terminal: Option<CommandOutcome<SandboxPhaseDecision>>,
}

impl<A> DurableSandboxLifecycleObserver<A> {
    fn new(authority: Arc<A>, command: &ExecuteSandboxJob, limits: SandboxCommandLimits) -> Self {
        Self {
            authority,
            state: Mutex::new(DurableSandboxObserverState {
                tenant_id: command.request.tenant_id.clone(),
                sandbox_job_id: command.request.sandbox_job_id.clone(),
                job_id: command.request.job_id.clone(),
                fence: command.fence.clone(),
                audits: command.audits.clone(),
                usage_reservation_id: command.usage_reservation_id.clone(),
                quota_entry_ids: command.quota_entry_ids.clone(),
                attestor_route: command.attestor_route.clone(),
                physical_state: SandboxJobState::Accepted,
                terminal: None,
            }),
            limits,
        }
    }

    async fn take_terminal(&self) -> Option<CommandOutcome<SandboxPhaseDecision>> {
        self.state.lock().await.terminal.take()
    }

    async fn heartbeat(
        &self,
        lease_milliseconds: u64,
        maximum_lease_milliseconds: u64,
    ) -> Result<(), SandboxWorkerObserverError<A::Error>>
    where
        A: SandboxExecutionAuthority,
    {
        let mut state = self.state.lock().await;
        if state.terminal.is_some()
            || !matches!(
                state.physical_state,
                SandboxJobState::Preparing
                    | SandboxJobState::Starting
                    | SandboxJobState::Running
                    | SandboxJobState::Collecting
                    | SandboxJobState::Cancelling
            )
        {
            return Ok(());
        }
        let command = HeartbeatSandboxExecution {
            tenant_id: state.tenant_id.clone(),
            sandbox_job_id: state.sandbox_job_id.clone(),
            job_id: state.job_id.clone(),
            fence: state.fence.clone(),
            lease_milliseconds,
        };
        command
            .validate(maximum_lease_milliseconds)
            .map_err(SandboxWorkerObserverError::Contract)?;
        let decision = self
            .authority
            .heartbeat_sandbox_execution(command)
            .await
            .map_err(SandboxWorkerObserverError::Authority)?;
        validate_authority_decision(
            &decision,
            &state.tenant_id,
            &state.job_id,
            state.physical_state,
            self.limits,
        )?;
        state.fence = next_fence(&decision, &state.fence);
        Ok(())
    }
}

#[async_trait]
impl<A> SandboxLifecycleObserver for DurableSandboxLifecycleObserver<A>
where
    A: SandboxExecutionAuthority,
{
    type Error = SandboxWorkerObserverError<A::Error>;

    async fn observe(&self, observation: SandboxLifecycleObservation) -> Result<(), Self::Error> {
        let mut state = self.state.lock().await;
        if state.terminal.is_some() {
            return Err(SandboxWorkerObserverError::Contract(
                SandboxWorkerContractError::InvalidAuthorityDecision,
            ));
        }
        match observation {
            SandboxLifecycleObservation::Phase {
                target,
                executor_identity_digest,
                phase_evidence_digest,
                prepared,
            } => {
                if !state.physical_state.can_transition_to(target)
                    || !matches!(
                        target,
                        SandboxJobState::Preparing
                            | SandboxJobState::Starting
                            | SandboxJobState::Running
                            | SandboxJobState::Collecting
                            | SandboxJobState::Cancelling
                    )
                {
                    return Err(SandboxWorkerObserverError::Contract(
                        SandboxWorkerContractError::InvalidAuthorityDecision,
                    ));
                }
                let identity =
                    state
                        .audits
                        .for_phase(target)
                        .ok_or(SandboxWorkerObserverError::Contract(
                            SandboxWorkerContractError::InvalidAudit,
                        ))?;
                let mut command = CommitSandboxPhase {
                    audit: identity.materialize(identity.idempotency_key_digest.clone()),
                    sandbox_job_id: state.sandbox_job_id.clone(),
                    job_id: state.job_id.clone(),
                    fence: state.fence.clone(),
                    target,
                    executor_identity_digest,
                    attestor_route: state.attestor_route.clone(),
                    phase_evidence_digest,
                    prepared,
                };
                command.audit.request_digest = command
                    .canonical_request_digest()
                    .map_err(SandboxWorkerObserverError::Contract)?;
                command
                    .validate_at(Utc::now())
                    .map_err(SandboxWorkerObserverError::Contract)?;
                let outcome = self
                    .authority
                    .commit_sandbox_phase(command)
                    .await
                    .map_err(SandboxWorkerObserverError::Authority)?;
                let decision = command_outcome_ref(&outcome);
                validate_authority_decision(
                    decision,
                    &state.tenant_id,
                    &state.job_id,
                    target,
                    self.limits,
                )?;
                state.fence = next_fence(decision, &state.fence);
                state.physical_state = target;
            }
            SandboxLifecycleObservation::Terminal {
                outcome,
                cleanup,
                phase_evidence_digest,
            } => {
                let target = outcome.physical_state();
                if !state.physical_state.can_transition_to(target) {
                    return Err(SandboxWorkerObserverError::Contract(
                        SandboxWorkerContractError::InvalidAuthorityDecision,
                    ));
                }
                let identity =
                    state
                        .audits
                        .for_phase(target)
                        .ok_or(SandboxWorkerObserverError::Contract(
                            SandboxWorkerContractError::InvalidAudit,
                        ))?;
                let mut command = CommitSandboxOutcome {
                    audit: identity.materialize(identity.idempotency_key_digest.clone()),
                    sandbox_job_id: state.sandbox_job_id.clone(),
                    job_id: state.job_id.clone(),
                    fence: state.fence.clone(),
                    outcome,
                    cleanup,
                    phase_evidence_digest,
                    usage_reservation_id: state.usage_reservation_id.clone(),
                    quota_entry_ids: state.quota_entry_ids.clone(),
                };
                command.audit.request_digest = command
                    .canonical_request_digest()
                    .map_err(SandboxWorkerObserverError::Contract)?;
                command
                    .validate_at(Utc::now())
                    .map_err(SandboxWorkerObserverError::Contract)?;
                let committed = self
                    .authority
                    .commit_sandbox_outcome(command)
                    .await
                    .map_err(SandboxWorkerObserverError::Authority)?;
                validate_authority_decision(
                    command_outcome_ref(&committed),
                    &state.tenant_id,
                    &state.job_id,
                    target,
                    self.limits,
                )?;
                state.physical_state = target;
                state.terminal = Some(committed);
            }
        }
        Ok(())
    }
}

fn command_outcome_ref<T>(outcome: &CommandOutcome<T>) -> &T {
    match outcome {
        CommandOutcome::Applied(value) | CommandOutcome::Replayed(value) => value,
    }
}

fn validate_authority_decision<E>(
    decision: &SandboxPhaseDecision,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    expected_state: SandboxJobState,
    limits: SandboxCommandLimits,
) -> Result<(), SandboxWorkerObserverError<E>> {
    decision
        .payload
        .validate_for(&decision.job, limits)
        .map_err(|_| {
            SandboxWorkerObserverError::Contract(
                SandboxWorkerContractError::InvalidAuthorityDecision,
            )
        })?;
    if &decision.job.tenant_id != tenant_id
        || &decision.job.job_id != job_id
        || decision.payload.physical_state != expected_state
    {
        return Err(SandboxWorkerObserverError::Contract(
            SandboxWorkerContractError::InvalidAuthorityDecision,
        ));
    }
    Ok(())
}

pub fn next_fence(decision: &SandboxPhaseDecision, prior: &JobFence) -> JobFence {
    JobFence {
        expected_version: decision.job.version,
        worker_process_generation_id: prior.worker_process_generation_id.clone(),
        lease_generation: prior.lease_generation,
        token_digest: prior.token_digest.clone(),
    }
}

fn all_distinct<'a, T>(values: impl Iterator<Item = &'a T>) -> bool
where
    T: Ord + 'a,
{
    let values = values.collect::<Vec<_>>();
    values.iter().copied().collect::<BTreeSet<_>>().len() == values.len()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxWorkerContractError {
    InvalidAudit,
    DuplicateCommitIdentity,
    InvalidPhaseCommit,
    InvalidTerminalCommit,
    Canonicalization,
    InvalidAuthorityDecision,
    InvalidExecutionCommand,
    InvalidClaim,
    InvalidHeartbeat,
    InvalidRecovery,
    MissingTerminalDecision,
    Contract(SandboxContractError),
}

impl fmt::Display for SandboxWorkerContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidAudit => "Sandbox worker audit is invalid",
            Self::DuplicateCommitIdentity => "Sandbox phase commit identities are not unique",
            Self::InvalidPhaseCommit => "Sandbox phase commit is invalid",
            Self::InvalidTerminalCommit => "Sandbox terminal commit is invalid",
            Self::Canonicalization => "Sandbox worker command canonicalization failed",
            Self::InvalidAuthorityDecision => "Sandbox authority returned an invalid decision",
            Self::InvalidExecutionCommand => "Sandbox worker execution command is invalid",
            Self::InvalidClaim => "Sandbox claim is invalid",
            Self::InvalidHeartbeat => "Sandbox heartbeat is invalid",
            Self::InvalidRecovery => "Sandbox lease recovery is invalid",
            Self::MissingTerminalDecision => "Sandbox worker did not receive a terminal decision",
            Self::Contract(_) => "Sandbox domain contract is invalid",
        })
    }
}

impl Error for SandboxWorkerContractError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Contract(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SandboxContractError> for SandboxWorkerContractError {
    fn from(value: SandboxContractError) -> Self {
        Self::Contract(value)
    }
}

#[derive(Debug)]
pub enum SandboxWorkerObserverError<E> {
    Contract(SandboxWorkerContractError),
    Authority(E),
}

#[derive(Debug)]
pub enum SandboxExecutorWorkerError<E> {
    Contract(SandboxWorkerContractError),
    Control(SandboxControlError),
    Host(crate::SandboxBackendHostError),
    Authority(E),
}

impl<E: fmt::Display> fmt::Display for SandboxExecutorWorkerError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(error) => error.fmt(formatter),
            Self::Control(error) => error.fmt(formatter),
            Self::Host(error) => error.fmt(formatter),
            Self::Authority(error) => write!(formatter, "Sandbox authority failed: {error}"),
        }
    }
}

impl<E: Error + 'static> Error for SandboxExecutorWorkerError<E> {}
