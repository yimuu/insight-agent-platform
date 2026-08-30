use crate::{
    types::{digest_without_field, SandboxExecutionRequest},
    SandboxPhaseDecision, SANDBOX_QUOTA_LINES,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, ResourceId, ResourceKind, SandboxJobState, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    sync::{Arc, Mutex, Weak},
};
use tokio_util::sync::CancellationToken;

/// Durable control intent projected from the authoritative Capability Invocation event stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxStopReason {
    Cancelled,
    TimedOut,
}

/// Controller-owned identity for one Event-driven pre-start Job control commit.
///
/// The committed Capability Event is the logical authority. The controller process generation is
/// retained only as the authenticated actor and is deliberately not a Job lease owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxControllerAudit {
    pub tenant_id: ResourceId,
    pub controller_process_generation_id: ResourceId,
    pub source_event_id: ResourceId,
    pub source_invocation_version: u64,
    pub source_event_payload_digest: Sha256Digest,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl SandboxControllerAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SandboxControlError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.controller_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.source_event_id.kind() != ResourceKind::Event
            || self.source_invocation_version == 0
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.event_id == self.source_event_id
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= now
        {
            return Err(SandboxControlError::InvalidControllerCommand);
        }
        Ok(())
    }
}

/// Applies a committed cancel/timeout to a Sandbox Job that has not acquired a lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopUnclaimedSandboxJob {
    pub audit: SandboxControllerAudit,
    pub sandbox_job_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub sandbox_request_digest: Sha256Digest,
    pub reason: SandboxStopReason,
    pub quota_entry_ids: Vec<ResourceId>,
}

impl StopUnclaimedSandboxJob {
    pub fn canonical_idempotency_key_digest(&self) -> Result<Sha256Digest, SandboxControlError> {
        canonical_digest(&serde_json::json!({
            "schema_version": 1,
            "source_event_id": self.audit.source_event_id,
            "job_id": self.job_id,
            "reason": self.reason,
        }))
        .map_err(|_| SandboxControlError::Canonicalization)?
        .parse()
        .map_err(|_| SandboxControlError::Canonicalization)
    }

    pub fn canonical_request_digest(&self) -> Result<Sha256Digest, SandboxControlError> {
        canonical_digest(&serde_json::json!({
            "schema_version": 1,
            "tenant_id": self.audit.tenant_id,
            "source_event_id": self.audit.source_event_id,
            "source_invocation_version": self.audit.source_invocation_version,
            "source_event_payload_digest": self.audit.source_event_payload_digest,
            "sandbox_job_id": self.sandbox_job_id,
            "invocation_id": self.invocation_id,
            "job_id": self.job_id,
            "sandbox_request_digest": self.sandbox_request_digest,
            "reason": self.reason,
        }))
        .map_err(|_| SandboxControlError::Canonicalization)?
        .parse()
        .map_err(|_| SandboxControlError::Canonicalization)
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SandboxControlError> {
        self.audit.validate_at(now)?;
        if self.sandbox_job_id.kind() != ResourceKind::Job
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.quota_entry_ids.len() != SANDBOX_QUOTA_LINES
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
            return Err(SandboxControlError::InvalidControllerCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SandboxPrestartControlOutcome {
    Applied(SandboxPhaseDecision),
    Replayed(SandboxPhaseDecision),
    RequiresExecutor,
    AlreadyTerminal(SandboxJobState),
}

/// Durable mutation port used only for an unleased `Accepted` Sandbox Job.
#[async_trait]
pub trait SandboxControlAuthority: Send + Sync {
    type Error;

    async fn stop_unclaimed_sandbox_job(
        &self,
        command: StopUnclaimedSandboxJob,
    ) -> Result<SandboxPrestartControlOutcome, Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxOutcomeMergeAudit {
    pub tenant_id: ResourceId,
    pub controller_process_generation_id: ResourceId,
    pub source_event_id: ResourceId,
    pub source_job_version: u64,
    pub source_event_payload_digest: Sha256Digest,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl SandboxOutcomeMergeAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SandboxControlError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.controller_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.source_event_id.kind() != ResourceKind::Event
            || self.source_job_version == 0
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.event_id == self.source_event_id
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= now
        {
            return Err(SandboxControlError::InvalidControllerCommand);
        }
        Ok(())
    }
}

/// Controller-owned command that merges one already-terminal Sandbox Job into its logical
/// Capability Invocation without reopening or rewriting that Job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeSandboxCapabilityOutcome {
    pub audit: SandboxOutcomeMergeAudit,
    pub sandbox_job_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub sandbox_request_digest: Sha256Digest,
    pub expected_invocation_version: u64,
    pub resume_mutations: insight_platform_contracts::ExternalLeafResumeMutationIds,
    pub failure_mutations: insight_platform_contracts::ExternalLeafFailureMutationIds,
}

/// Read-only durable candidate consumed by the Capability owner controller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSandboxCapabilityOutcome {
    pub tenant_id: ResourceId,
    pub source_event_id: ResourceId,
    pub source_job_version: u64,
    pub source_event_payload_digest: Sha256Digest,
    pub source_event_occurred_at: DateTime<Utc>,
    pub sandbox_job_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub sandbox_request_digest: Sha256Digest,
    pub expected_invocation_version: u64,
    pub deadline: DateTime<Utc>,
}

impl PendingSandboxCapabilityOutcome {
    pub fn validate(&self) -> Result<(), SandboxControlError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.source_event_id.kind() != ResourceKind::Event
            || self.source_job_version == 0
            || self.sandbox_job_id.kind() != ResourceKind::Job
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.sandbox_job_id.uuid() != self.job_id.uuid()
            || self.expected_invocation_version == 0
        {
            return Err(SandboxControlError::InvalidSourcePage);
        }
        Ok(())
    }
}

impl MergeSandboxCapabilityOutcome {
    pub fn canonical_idempotency_key_digest(&self) -> Result<Sha256Digest, SandboxControlError> {
        canonical_digest(&serde_json::json!({
            "schema_version": 1,
            "source_event_id": self.audit.source_event_id,
            "invocation_id": self.invocation_id,
            "job_id": self.job_id,
        }))
        .map_err(|_| SandboxControlError::Canonicalization)?
        .parse()
        .map_err(|_| SandboxControlError::Canonicalization)
    }

    pub fn canonical_request_digest(&self) -> Result<Sha256Digest, SandboxControlError> {
        canonical_digest(&serde_json::json!({
            "schema_version": 1,
            "tenant_id": self.audit.tenant_id,
            "source_event_id": self.audit.source_event_id,
            "source_event_payload_digest": self.audit.source_event_payload_digest,
            "sandbox_job_id": self.sandbox_job_id,
            "invocation_id": self.invocation_id,
            "job_id": self.job_id,
            "sandbox_request_digest": self.sandbox_request_digest,
            "expected_invocation_version": self.expected_invocation_version,
            "resume_mutations": {
                "continuation_node_execution_id": self.resume_mutations.continuation_node_execution_id,
                "continuation_job_id": self.resume_mutations.continuation_job_id,
                "run_event_id": self.resume_mutations.run_event_id,
                "run_outbox_id": self.resume_mutations.run_outbox_id,
                "leaf_node_event_id": self.resume_mutations.leaf_node_event_id,
                "leaf_node_outbox_id": self.resume_mutations.leaf_node_outbox_id,
                "continuation_node_event_id": self.resume_mutations.continuation_node_event_id,
                "continuation_node_outbox_id": self.resume_mutations.continuation_node_outbox_id,
                "continuation_job_event_id": self.resume_mutations.continuation_job_event_id,
                "continuation_job_outbox_id": self.resume_mutations.continuation_job_outbox_id,
            },
            "failure_mutations": {
                "convergence_job_id": self.failure_mutations.convergence_job_id,
                "run_event_id": self.failure_mutations.run_event_id,
                "run_outbox_id": self.failure_mutations.run_outbox_id,
                "leaf_node_event_id": self.failure_mutations.leaf_node_event_id,
                "leaf_node_outbox_id": self.failure_mutations.leaf_node_outbox_id,
                "convergence_job_event_id": self.failure_mutations.convergence_job_event_id,
                "convergence_job_outbox_id": self.failure_mutations.convergence_job_outbox_id,
            },
        }))
        .map_err(|_| SandboxControlError::Canonicalization)?
        .parse()
        .map_err(|_| SandboxControlError::Canonicalization)
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SandboxControlError> {
        self.audit.validate_at(now)?;
        if self.sandbox_job_id.kind() != ResourceKind::Job
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.sandbox_job_id.uuid() != self.job_id.uuid()
            || self.expected_invocation_version == 0
            || self.resume_mutations.validate().is_err()
            || self.failure_mutations.validate().is_err()
            || self.audit.idempotency_key_digest != self.canonical_idempotency_key_digest()?
            || self.audit.request_digest != self.canonical_request_digest()?
        {
            return Err(SandboxControlError::InvalidControllerCommand);
        }
        Ok(())
    }
}

/// Exact, self-authenticating stop signal delivered to one leased Sandbox execution.
///
/// The source event remains the durable authority. This envelope only routes that committed fact
/// to the exact in-process execution generation and therefore intentionally owns no current state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxStopSignal {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub worker_process_generation_id: ResourceId,
    pub reason: SandboxStopReason,
    pub source_event_id: ResourceId,
    pub source_invocation_version: u64,
    pub source_event_payload_digest: Sha256Digest,
    pub signal_digest: Sha256Digest,
}

impl SandboxStopSignal {
    pub fn seal(mut self) -> Result<Self, SandboxControlError> {
        self.signal_digest = digest_without_field(&self, "signal_digest")
            .map_err(|_| SandboxControlError::Canonicalization)?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), SandboxControlError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.sandbox_job_id.kind() != ResourceKind::Job
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.sandbox_job_id.uuid() != self.job_id.uuid()
            || self.attempt_no == 0
            || self.lease_generation == 0
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.source_event_id.kind() != ResourceKind::Event
            || self.source_invocation_version == 0
            || digest_without_field(self, "signal_digest")
                .map_err(|_| SandboxControlError::Canonicalization)?
                != self.signal_digest
        {
            return Err(SandboxControlError::InvalidSignal);
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        request: &SandboxExecutionRequest,
    ) -> Result<(), SandboxControlError> {
        self.validate()?;
        if self.tenant_id != request.tenant_id
            || self.sandbox_job_id != request.sandbox_job_id
            || self.invocation_id != request.invocation_id
            || self.job_id != request.job_id
            || self.request_digest != request.request_digest
            || self.attempt_no != request.attempt_no
            || self.lease_generation != request.lease_generation
        {
            return Err(SandboxControlError::StaleSignal);
        }
        Ok(())
    }

    pub fn validate_for_execution(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
    ) -> Result<(), SandboxControlError> {
        self.validate_for(request)?;
        if &self.worker_process_generation_id != worker_process_generation_id {
            return Err(SandboxControlError::StaleSignal);
        }
        Ok(())
    }
}

/// Result of delivering a valid control signal to the local Executor process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxControlDelivery {
    Delivered,
    AlreadyStopped(SandboxStopReason),
    Missing,
    Stale,
}

/// Transport boundary between the durable Controller and the target Executor generation.
///
/// An in-process Executor uses [`SandboxExecutionControlRouter`]; a production deployment may
/// implement the same contract over an authenticated bounded RPC or message transport.
#[async_trait]
pub trait SandboxControlSignalSink: Send + Sync {
    async fn deliver(
        &self,
        signal: &SandboxStopSignal,
    ) -> Result<SandboxControlDelivery, SandboxControlError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveSandboxControlEvent {
    pub tenant_id: ResourceId,
    pub source_event_id: ResourceId,
    pub executor_worker_process_generation_id: ResourceId,
    pub limit: u16,
}

impl ResolveSandboxControlEvent {
    fn validate(&self, maximum_batch: u16) -> Result<(), SandboxControlError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.source_event_id.kind() != ResourceKind::Event
            || self.executor_worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.limit == 0
            || self.limit > maximum_batch
        {
            return Err(SandboxControlError::InvalidQuery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxControlScanShard {
    pub index: u16,
    pub count: u16,
}

impl SandboxControlScanShard {
    pub const fn whole() -> Self {
        Self { index: 0, count: 1 }
    }

    fn validate(self, maximum_shards: u16) -> Result<(), SandboxControlError> {
        if self.count == 0 || self.count > maximum_shards || self.index >= self.count {
            return Err(SandboxControlError::InvalidQuery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxControlScanCursor {
    pub sort_at: DateTime<Utc>,
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
}

impl SandboxControlScanCursor {
    fn validate(&self) -> Result<(), SandboxControlError> {
        if self.tenant_id.kind() != ResourceKind::Tenant || self.job_id.kind() != ResourceKind::Job
        {
            return Err(SandboxControlError::InvalidQuery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverSandboxControlSignals {
    pub shard: SandboxControlScanShard,
    pub after: Option<SandboxControlScanCursor>,
    pub executor_worker_process_generation_id: ResourceId,
    pub limit: u16,
}

impl RecoverSandboxControlSignals {
    fn validate(&self, maximum_batch: u16, maximum_shards: u16) -> Result<(), SandboxControlError> {
        self.shard.validate(maximum_shards)?;
        if let Some(after) = &self.after {
            after.validate()?;
        }
        if self.executor_worker_process_generation_id.kind()
            != ResourceKind::WorkerProcessGeneration
            || self.limit == 0
            || self.limit > maximum_batch
        {
            return Err(SandboxControlError::InvalidQuery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxControlSignalPage {
    pub signals: Vec<SandboxStopSignal>,
    pub next_cursor: Option<SandboxControlScanCursor>,
    pub exhausted: bool,
}

#[async_trait]
pub trait SandboxControlSignalSource: Send + Sync {
    type Error;

    async fn resolve_committed_control_event(
        &self,
        query: ResolveSandboxControlEvent,
    ) -> Result<Vec<SandboxStopSignal>, Self::Error>;

    async fn recover_committed_control_signals(
        &self,
        query: RecoverSandboxControlSignals,
    ) -> Result<SandboxControlSignalPage, Self::Error>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SandboxControlDispatchReport {
    pub delivered: usize,
    pub already_stopped: usize,
    pub missing: usize,
    pub stale: usize,
}

pub struct SandboxControlController<S> {
    source: Arc<S>,
    sink: Arc<dyn SandboxControlSignalSink>,
    maximum_batch: u16,
    maximum_shards: u16,
}

impl<S> SandboxControlController<S>
where
    S: SandboxControlSignalSource,
{
    pub fn new(
        source: Arc<S>,
        sink: Arc<dyn SandboxControlSignalSink>,
        maximum_batch: u16,
        maximum_shards: u16,
    ) -> Result<Self, SandboxControlError> {
        if maximum_batch == 0 || maximum_shards == 0 {
            return Err(SandboxControlError::InvalidCapacity);
        }
        Ok(Self {
            source,
            sink,
            maximum_batch,
            maximum_shards,
        })
    }

    pub async fn dispatch_event(
        &self,
        query: ResolveSandboxControlEvent,
    ) -> Result<SandboxControlDispatchReport, SandboxControlControllerError<S::Error>> {
        query
            .validate(self.maximum_batch)
            .map_err(SandboxControlControllerError::Control)?;
        let tenant_id = query.tenant_id.clone();
        let source_event_id = query.source_event_id.clone();
        let worker_process_generation_id = query.executor_worker_process_generation_id.clone();
        let limit = query.limit;
        let signals = self
            .source
            .resolve_committed_control_event(query)
            .await
            .map_err(SandboxControlControllerError::Source)?;
        if signals.len() > usize::from(limit)
            || signals.iter().any(|signal| {
                signal.tenant_id != tenant_id
                    || signal.source_event_id != source_event_id
                    || signal.worker_process_generation_id != worker_process_generation_id
            })
        {
            return Err(SandboxControlControllerError::Control(
                SandboxControlError::InvalidSourcePage,
            ));
        }
        self.dispatch(signals)
            .await
            .map_err(SandboxControlControllerError::Control)
    }

    pub async fn recover(
        &self,
        query: RecoverSandboxControlSignals,
    ) -> Result<
        (
            SandboxControlDispatchReport,
            Option<SandboxControlScanCursor>,
            bool,
        ),
        SandboxControlControllerError<S::Error>,
    > {
        query
            .validate(self.maximum_batch, self.maximum_shards)
            .map_err(SandboxControlControllerError::Control)?;
        let limit = query.limit;
        let worker_process_generation_id = query.executor_worker_process_generation_id.clone();
        let page = self
            .source
            .recover_committed_control_signals(query)
            .await
            .map_err(SandboxControlControllerError::Source)?;
        if page.signals.len() > usize::from(limit)
            || page
                .signals
                .iter()
                .any(|signal| signal.worker_process_generation_id != worker_process_generation_id)
            || page
                .next_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.validate().is_err())
            || (page.exhausted && page.next_cursor.is_some())
            || (!page.exhausted && page.next_cursor.is_none())
        {
            return Err(SandboxControlControllerError::Control(
                SandboxControlError::InvalidSourcePage,
            ));
        }
        let report = self
            .dispatch(page.signals)
            .await
            .map_err(SandboxControlControllerError::Control)?;
        Ok((report, page.next_cursor, page.exhausted))
    }

    async fn dispatch(
        &self,
        signals: Vec<SandboxStopSignal>,
    ) -> Result<SandboxControlDispatchReport, SandboxControlError> {
        let mut unique = BTreeMap::new();
        let mut report = SandboxControlDispatchReport::default();
        for signal in signals {
            signal.validate()?;
            let key = (
                signal.job_id.clone(),
                signal.lease_generation,
                signal.source_event_id.clone(),
            );
            if unique.insert(key, ()).is_some() {
                return Err(SandboxControlError::InvalidSourcePage);
            }
            match self.sink.deliver(&signal).await? {
                SandboxControlDelivery::Delivered => report.delivered += 1,
                SandboxControlDelivery::AlreadyStopped(_) => report.already_stopped += 1,
                SandboxControlDelivery::Missing => report.missing += 1,
                SandboxControlDelivery::Stale => report.stale += 1,
            }
        }
        Ok(report)
    }
}

#[derive(Debug)]
pub enum SandboxControlControllerError<E> {
    Control(SandboxControlError),
    Source(E),
}

impl<E: fmt::Display> fmt::Display for SandboxControlControllerError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(formatter),
            Self::Source(error) => write!(formatter, "Sandbox control source failed: {error}"),
        }
    }
}

impl<E: Error + 'static> Error for SandboxControlControllerError<E> {}

/// Bounded in-memory routing for active executions. It is never a durable state authority.
#[derive(Clone)]
pub struct SandboxExecutionControlRouter {
    inner: Arc<SandboxExecutionControlRouterInner>,
}

struct SandboxExecutionControlRouterInner {
    maximum_active: usize,
    state: Mutex<SandboxExecutionControlRouterState>,
}

#[derive(Default)]
struct SandboxExecutionControlRouterState {
    next_registration: u64,
    active: BTreeMap<ResourceId, ActiveSandboxExecution>,
}

struct ActiveSandboxExecution {
    registration: u64,
    tenant_id: ResourceId,
    sandbox_job_id: ResourceId,
    invocation_id: ResourceId,
    request_digest: Sha256Digest,
    attempt_no: u32,
    lease_generation: u64,
    worker_process_generation_id: ResourceId,
    stop: SandboxExecutionStopToken,
}

impl ActiveSandboxExecution {
    fn matches(&self, signal: &SandboxStopSignal) -> bool {
        self.tenant_id == signal.tenant_id
            && self.sandbox_job_id == signal.sandbox_job_id
            && self.invocation_id == signal.invocation_id
            && self.request_digest == signal.request_digest
            && self.attempt_no == signal.attempt_no
            && self.lease_generation == signal.lease_generation
            && self.worker_process_generation_id == signal.worker_process_generation_id
    }
}

impl SandboxExecutionControlRouter {
    pub fn new(maximum_active: usize) -> Result<Self, SandboxControlError> {
        if maximum_active == 0 {
            return Err(SandboxControlError::InvalidCapacity);
        }
        Ok(Self {
            inner: Arc::new(SandboxExecutionControlRouterInner {
                maximum_active,
                state: Mutex::new(SandboxExecutionControlRouterState::default()),
            }),
        })
    }

    pub fn deliver(
        &self,
        signal: &SandboxStopSignal,
    ) -> Result<SandboxControlDelivery, SandboxControlError> {
        signal.validate()?;
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| SandboxControlError::RouterUnavailable)?;
        let Some(active) = state.active.get_mut(&signal.job_id) else {
            return Ok(SandboxControlDelivery::Missing);
        };
        if !active.matches(signal) {
            return Ok(SandboxControlDelivery::Stale);
        }
        Ok(match active.stop.request_stop(signal.reason)? {
            SandboxStopRequest::Delivered => SandboxControlDelivery::Delivered,
            SandboxStopRequest::AlreadyStopped(reason) => {
                SandboxControlDelivery::AlreadyStopped(reason)
            }
        })
    }

    pub fn active_count(&self) -> Result<usize, SandboxControlError> {
        self.inner
            .state
            .lock()
            .map(|state| state.active.len())
            .map_err(|_| SandboxControlError::RouterUnavailable)
    }

    pub(crate) fn register(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
    ) -> Result<SandboxExecutionRegistration, SandboxControlError> {
        if request.tenant_id.kind() != ResourceKind::Tenant
            || request.sandbox_job_id.kind() != ResourceKind::Job
            || request.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || request.job_id.kind() != ResourceKind::Job
            || request.attempt_no == 0
            || request.lease_generation == 0
            || worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || request.clone().seal().map(|sealed| sealed.request_digest)
                != Ok(request.request_digest.clone())
        {
            return Err(SandboxControlError::InvalidRegistration);
        }
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| SandboxControlError::RouterUnavailable)?;
        if state.active.contains_key(&request.job_id) {
            return Err(SandboxControlError::DuplicateRegistration);
        }
        if state.active.len() >= self.inner.maximum_active {
            return Err(SandboxControlError::CapacityExhausted);
        }
        state.next_registration = state
            .next_registration
            .checked_add(1)
            .ok_or(SandboxControlError::RegistrationSequenceExhausted)?;
        let registration = state.next_registration;
        let stop = SandboxExecutionStopToken::new();
        state.active.insert(
            request.job_id.clone(),
            ActiveSandboxExecution {
                registration,
                tenant_id: request.tenant_id.clone(),
                sandbox_job_id: request.sandbox_job_id.clone(),
                invocation_id: request.invocation_id.clone(),
                request_digest: request.request_digest.clone(),
                attempt_no: request.attempt_no,
                lease_generation: request.lease_generation,
                worker_process_generation_id: worker_process_generation_id.clone(),
                stop: stop.clone(),
            },
        );
        Ok(SandboxExecutionRegistration {
            router: Arc::downgrade(&self.inner),
            job_id: request.job_id.clone(),
            registration,
            stop,
        })
    }
}

#[async_trait]
impl SandboxControlSignalSink for SandboxExecutionControlRouter {
    async fn deliver(
        &self,
        signal: &SandboxStopSignal,
    ) -> Result<SandboxControlDelivery, SandboxControlError> {
        SandboxExecutionControlRouter::deliver(self, signal)
    }
}

pub(crate) struct SandboxExecutionRegistration {
    router: Weak<SandboxExecutionControlRouterInner>,
    job_id: ResourceId,
    registration: u64,
    stop: SandboxExecutionStopToken,
}

impl SandboxExecutionRegistration {
    pub(crate) fn stop_token(&self) -> SandboxExecutionStopToken {
        self.stop.clone()
    }
}

impl Drop for SandboxExecutionRegistration {
    fn drop(&mut self) {
        let Some(router) = self.router.upgrade() else {
            return;
        };
        let Ok(mut state) = router.state.lock() else {
            return;
        };
        if state
            .active
            .get(&self.job_id)
            .is_some_and(|active| active.registration == self.registration)
        {
            state.active.remove(&self.job_id);
        }
    }
}

#[derive(Clone)]
pub(crate) struct SandboxExecutionStopToken {
    cancellation: CancellationToken,
    reason: Arc<Mutex<Option<SandboxStopReason>>>,
}

enum SandboxStopRequest {
    Delivered,
    AlreadyStopped(SandboxStopReason),
}

impl SandboxExecutionStopToken {
    pub(crate) fn new() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            reason: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn from_cancellation(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            reason: Arc::new(Mutex::new(None)),
        }
    }

    fn request_stop(
        &self,
        reason: SandboxStopReason,
    ) -> Result<SandboxStopRequest, SandboxControlError> {
        let mut current = self
            .reason
            .lock()
            .map_err(|_| SandboxControlError::RouterUnavailable)?;
        if let Some(current) = *current {
            return Ok(SandboxStopRequest::AlreadyStopped(current));
        }
        *current = Some(reason);
        self.cancellation.cancel();
        Ok(SandboxStopRequest::Delivered)
    }

    pub(crate) fn request_lease_lost(&self) -> Result<(), SandboxControlError> {
        self.request_stop(SandboxStopReason::TimedOut).map(|_| ())
    }

    pub(crate) async fn cancelled(&self) -> SandboxStopReason {
        self.cancellation.cancelled().await;
        self.reason
            .lock()
            .ok()
            .and_then(|reason| *reason)
            .unwrap_or(SandboxStopReason::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxControlError {
    InvalidCapacity,
    InvalidQuery,
    InvalidSourcePage,
    InvalidSignal,
    InvalidControllerCommand,
    StaleSignal,
    InvalidRegistration,
    DuplicateRegistration,
    CapacityExhausted,
    RegistrationSequenceExhausted,
    RouterUnavailable,
    TransportUnavailable,
    InvalidTransportEnvelope,
    Canonicalization,
}

impl fmt::Display for SandboxControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidCapacity => "Sandbox control capacity is invalid",
            Self::InvalidQuery => "Sandbox control query is invalid",
            Self::InvalidSourcePage => "Sandbox control source returned an invalid page",
            Self::InvalidSignal => "Sandbox stop signal is invalid",
            Self::InvalidControllerCommand => "Sandbox controller command is invalid",
            Self::StaleSignal => "Sandbox stop signal does not match the execution request",
            Self::InvalidRegistration => "Sandbox execution registration is invalid",
            Self::DuplicateRegistration => "Sandbox Job already has an active execution",
            Self::CapacityExhausted => "Sandbox execution control capacity is exhausted",
            Self::RegistrationSequenceExhausted => {
                "Sandbox execution registration sequence is exhausted"
            }
            Self::RouterUnavailable => "Sandbox execution control router is unavailable",
            Self::TransportUnavailable => "Sandbox control transport is unavailable",
            Self::InvalidTransportEnvelope => "Sandbox control transport envelope is invalid",
            Self::Canonicalization => "Sandbox stop signal canonicalization failed",
        })
    }
}

impl Error for SandboxControlError {}
