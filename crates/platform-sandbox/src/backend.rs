use crate::{
    digest_without_field, stable_code, ExpiredSandboxLease, SafeSandboxFailure,
    SandboxCleanupDisposition, SandboxCleanupEvidence, SandboxCommandLimits, SandboxContractError,
    SandboxExecutionOutcome, SandboxExecutionRequest, SandboxExecutionStopToken,
    SandboxLeaseRecoveryAction, SandboxResourceUsage, SandboxStopReason,
    SandboxTerminationEvidence, SandboxUncertainty, MAX_SANDBOX_SAFE_CODE_BYTES,
    MAX_SANDBOX_SAFE_MESSAGE_BYTES,
};
use async_trait::async_trait;
use chrono::Utc;
use futures::FutureExt;
use insight_platform_contracts::{
    ResourceId, ResourceKind, Retryability, SandboxIsolationClass, SandboxJobState, Sha256Digest,
    WorkerManifest,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    error::Error,
    fmt,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct InstalledSandboxBackendDescriptor {
    pub backend_kind: SandboxIsolationBackendKind,
    pub isolation_class: SandboxIsolationClass,
    pub worker_manifest_digest: Sha256Digest,
    pub backend_contract_digest: Sha256Digest,
}

impl InstalledSandboxBackendDescriptor {
    fn validate(&self) -> Result<(), SandboxBackendHostError> {
        if !matches!(
            (self.backend_kind, self.isolation_class),
            (
                SandboxIsolationBackendKind::Wasi,
                SandboxIsolationClass::Wasm
            ) | (
                SandboxIsolationBackendKind::Gvisor,
                SandboxIsolationClass::SandboxedContainer
            ) | (
                SandboxIsolationBackendKind::MicroVm,
                SandboxIsolationClass::MicroVm
            )
        ) {
            return Err(SandboxBackendHostError::InvalidInstalledBackend);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxIsolationBackendKind {
    Wasi,
    Gvisor,
    MicroVm,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedSandbox {
    /// Present only for a microVM prepared by one exact provider process generation.
    pub provider_process_generation_id: Option<ResourceId>,
    pub sandbox_identity_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub prepare_evidence_digest: Sha256Digest,
}

impl PreparedSandbox {
    pub(crate) fn validate_for(
        &self,
        request: &SandboxExecutionRequest,
    ) -> Result<(), SandboxBackendHostError> {
        if self.request_digest != request.request_digest
            || self.attempt_no != request.attempt_no
            || self.lease_generation != request.lease_generation
            || match request.isolation_class {
                SandboxIsolationClass::MicroVm => self
                    .provider_process_generation_id
                    .as_ref()
                    .is_none_or(|generation| {
                        generation.kind() != ResourceKind::WorkerProcessGeneration
                    }),
                SandboxIsolationClass::Wasm | SandboxIsolationClass::SandboxedContainer => {
                    self.provider_process_generation_id.is_some()
                }
            }
        {
            return Err(SandboxBackendHostError::InvalidBackendEvidence);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunningSandbox {
    pub prepared: PreparedSandbox,
    pub start_evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectedSandbox {
    pub running: RunningSandbox,
    pub outcome: SandboxExecutionOutcome,
    pub usage: SandboxResourceUsage,
    pub collection_evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxBackendFailureStage {
    Admission,
    Preparing,
    Starting,
    Running,
    Collecting,
    Aborting,
    Terminating,
    Destroying,
    Recovering,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxBackendFailure {
    pub stage: SandboxBackendFailureStage,
    pub safe_code: String,
    pub safe_message: String,
    pub retryability: Retryability,
    pub evidence_digest: Sha256Digest,
    pub sandbox_identity_digest: Option<Sha256Digest>,
    pub execution_may_have_started: bool,
    pub external_effect_possible: bool,
}

impl SandboxBackendFailure {
    fn validate_for(
        &self,
        request: &SandboxExecutionRequest,
    ) -> Result<(), SandboxBackendHostError> {
        if !stable_code(&self.safe_code, MAX_SANDBOX_SAFE_CODE_BYTES)
            || self.safe_message.is_empty()
            || self.safe_message.len() > MAX_SANDBOX_SAFE_MESSAGE_BYTES
            || self.safe_message.chars().any(char::is_control)
            || self.execution_may_have_started
                && matches!(
                    self.stage,
                    SandboxBackendFailureStage::Admission | SandboxBackendFailureStage::Preparing
                )
            || self.external_effect_possible && !self.execution_may_have_started
            || self.external_effect_possible && self.retryability == Retryability::SafeWithinPolicy
            || self.external_effect_possible
                && request.effect.risk_rank()
                    < insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
                && request.network_mode == crate::SandboxNetworkMode::None
        {
            return Err(SandboxBackendHostError::InvalidBackendFailure);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminateSandbox {
    pub tenant_id: insight_platform_contracts::ResourceId,
    pub sandbox_job_id: insight_platform_contracts::ResourceId,
    pub request_digest: Sha256Digest,
    pub sandbox_identity_digest: Sha256Digest,
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub effect: insight_platform_contracts::Effect,
    pub network_mode: crate::SandboxNetworkMode,
    pub deadline: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestroySandbox {
    pub tenant_id: insight_platform_contracts::ResourceId,
    pub sandbox_job_id: insight_platform_contracts::ResourceId,
    pub request_digest: Sha256Digest,
    pub sandbox_identity_digest: Sha256Digest,
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub effect: insight_platform_contracts::Effect,
    pub network_mode: crate::SandboxNetworkMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AbortSandboxExecution {
    pub tenant_id: insight_platform_contracts::ResourceId,
    pub sandbox_job_id: insight_platform_contracts::ResourceId,
    pub request_digest: Sha256Digest,
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub reason: SandboxStopReason,
    pub effect: insight_platform_contracts::Effect,
    pub network_mode: crate::SandboxNetworkMode,
    pub execution_deadline: chrono::DateTime<Utc>,
}

fn validate_backend_lifecycle_identity(
    tenant_id: &insight_platform_contracts::ResourceId,
    sandbox_job_id: &insight_platform_contracts::ResourceId,
    attempt_no: u32,
    lease_generation: u64,
) -> Result<(), SandboxBackendHostError> {
    if tenant_id.kind() != insight_platform_contracts::ResourceKind::Tenant
        || sandbox_job_id.kind() != insight_platform_contracts::ResourceKind::Job
        || attempt_no == 0
        || lease_generation == 0
    {
        return Err(SandboxBackendHostError::InvalidBackendEvidence);
    }
    Ok(())
}

impl TerminateSandbox {
    pub fn validate(&self) -> Result<(), SandboxBackendHostError> {
        validate_backend_lifecycle_identity(
            &self.tenant_id,
            &self.sandbox_job_id,
            self.attempt_no,
            self.lease_generation,
        )
    }
}

impl DestroySandbox {
    pub fn validate(&self) -> Result<(), SandboxBackendHostError> {
        validate_backend_lifecycle_identity(
            &self.tenant_id,
            &self.sandbox_job_id,
            self.attempt_no,
            self.lease_generation,
        )
    }
}

impl AbortSandboxExecution {
    pub fn validate(&self) -> Result<(), SandboxBackendHostError> {
        validate_backend_lifecycle_identity(
            &self.tenant_id,
            &self.sandbox_job_id,
            self.attempt_no,
            self.lease_generation,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxAbortEvidence {
    pub reason: SandboxStopReason,
    pub request_digest: Sha256Digest,
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub sandbox_identity_digest: Sha256Digest,
    pub termination: SandboxTerminationEvidence,
    pub cleanup: SandboxCleanupEvidence,
    pub evidence_digest: Sha256Digest,
}

/// Backend proof used to close a started execution whose exact Job lease expired.
///
/// The backend must either destroy the old sandbox or quarantine its node identity and revoke all
/// grants. The evidence is bound to the read-only scan projection and cannot authorize a retry of
/// the same Sandbox Job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxLeaseRecoveryEvidence {
    pub schema_version: u32,
    pub request_digest: Sha256Digest,
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub physical_state: SandboxJobState,
    pub executor_identity_digest: Sha256Digest,
    pub uncertainty: SandboxUncertainty,
    pub cleanup: SandboxCleanupEvidence,
    pub evidence_digest: Sha256Digest,
}

impl SandboxLeaseRecoveryEvidence {
    pub fn seal(mut self) -> Result<Self, SandboxBackendHostError> {
        self.evidence_digest = digest_without_field(&self, "evidence_digest")
            .map_err(|_| SandboxBackendHostError::InvalidBackendEvidence)?;
        Ok(self)
    }

    fn validate_for(
        &self,
        expired: &ExpiredSandboxLease,
        observed_at: chrono::DateTime<Utc>,
    ) -> Result<(), SandboxBackendHostError> {
        self.uncertainty
            .validate_for(&expired.request)
            .map_err(|_| SandboxBackendHostError::InvalidBackendEvidence)?;
        self.cleanup
            .validate_recovery()
            .map_err(|_| SandboxBackendHostError::InvalidBackendEvidence)?;
        let execution_may_have_started = !matches!(self.physical_state, SandboxJobState::Preparing);
        let external_effect_possible = execution_may_have_started
            && (expired.request.effect.risk_rank()
                >= insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
                || expired.request.network_mode != crate::SandboxNetworkMode::None);
        if self.schema_version != 1
            || self.request_digest != expired.request.request_digest
            || self.attempt_no != expired.request.attempt_no
            || self.lease_generation != expired.observed_lease_generation
            || self.physical_state != expired.physical_state
            || expired.executor_identity_digest.as_ref() != Some(&self.executor_identity_digest)
            || !matches!(
                self.physical_state,
                SandboxJobState::Preparing
                    | SandboxJobState::Starting
                    | SandboxJobState::Running
                    | SandboxJobState::Collecting
                    | SandboxJobState::Cancelling
            )
            || self.cleanup.observed_at > observed_at
            || self.uncertainty.execution_may_have_started != execution_may_have_started
            || self.uncertainty.external_effect_possible != external_effect_possible
            || self.uncertainty.manual_reconciliation_required != external_effect_possible
            || self.uncertainty.sandbox_identity_digest != self.cleanup.sandbox_identity_digest
            || digest_without_field(self, "evidence_digest")
                .map_err(|_| SandboxBackendHostError::InvalidBackendEvidence)?
                != self.evidence_digest
        {
            return Err(SandboxBackendHostError::InvalidBackendEvidence);
        }
        Ok(())
    }
}

impl SandboxAbortEvidence {
    fn validate_for(
        &self,
        request: &SandboxExecutionRequest,
        reason: SandboxStopReason,
    ) -> Result<(), SandboxBackendHostError> {
        if self.reason != reason
            || self.request_digest != request.request_digest
            || self.attempt_no != request.attempt_no
            || self.lease_generation != request.lease_generation
            || self.cleanup.sandbox_identity_digest != self.sandbox_identity_digest
        {
            return Err(SandboxBackendHostError::InvalidBackendEvidence);
        }
        self.termination
            .validate_for(request)
            .map_err(|_| SandboxBackendHostError::InvalidBackendEvidence)?;
        self.cleanup
            .validate(request.deadline, request.resources.cleanup_milliseconds)
            .map_err(|_| SandboxBackendHostError::InvalidBackendEvidence)
    }
}

#[async_trait]
pub trait SandboxExecutorBackend: Send + Sync {
    fn descriptor(&self) -> InstalledSandboxBackendDescriptor;

    async fn prepare(
        &self,
        request: SandboxExecutionRequest,
    ) -> Result<PreparedSandbox, SandboxBackendFailure>;

    async fn start(
        &self,
        request: SandboxExecutionRequest,
        prepared: PreparedSandbox,
    ) -> Result<RunningSandbox, SandboxBackendFailure>;

    async fn collect(
        &self,
        request: SandboxExecutionRequest,
        running: RunningSandbox,
    ) -> Result<CollectedSandbox, SandboxBackendFailure>;

    async fn terminate(
        &self,
        command: TerminateSandbox,
    ) -> Result<SandboxTerminationEvidence, SandboxBackendFailure>;

    async fn destroy(
        &self,
        command: DestroySandbox,
    ) -> Result<SandboxCleanupEvidence, SandboxBackendFailure>;

    /// Stops any pending or running execution identified by the exact request/attempt/generation.
    ///
    /// This must also cover a prepare operation that has not yet returned a sandbox identity; it is
    /// the backend's responsibility to locate and fence such pending work by the exact request.
    async fn abort(
        &self,
        command: AbortSandboxExecution,
    ) -> Result<SandboxAbortEvidence, SandboxBackendFailure>;

    /// Terminates/destroys an expired exact generation or quarantines the backing node when exact
    /// destruction cannot be proved. It must never start or reconnect user code.
    async fn recover_expired_lease(
        &self,
        expired: ExpiredSandboxLease,
    ) -> Result<SandboxLeaseRecoveryEvidence, SandboxBackendFailure>;
}

/// Durable Executor lease owner carried across the node-local Executor -> microVM Provider
/// boundary. The mTLS workload identity authenticates the Executor role; this value independently
/// binds each physical lifecycle call to the exact PostgreSQL Job lease generation owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MicroVmProviderExecutionFence {
    pub worker_process_generation_id: ResourceId,
}

impl MicroVmProviderExecutionFence {
    pub fn validate(&self) -> Result<(), SandboxBackendHostError> {
        if self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration {
            return Err(SandboxBackendHostError::InvalidBackendEvidence);
        }
        Ok(())
    }
}

/// Provider-side lifecycle port. Unlike `SandboxExecutorBackend`, every call carries the exact
/// Executor process generation that owns the durable lease. A Provider must persist and compare
/// this fence; transport identity or a provider-local process generation cannot replace it.
#[async_trait]
pub trait MicroVmIsolationProviderBackend: Send + Sync {
    fn descriptor(&self) -> InstalledSandboxBackendDescriptor;

    async fn prepare(
        &self,
        fence: MicroVmProviderExecutionFence,
        request: SandboxExecutionRequest,
    ) -> Result<PreparedSandbox, SandboxBackendFailure>;

    async fn start(
        &self,
        fence: MicroVmProviderExecutionFence,
        request: SandboxExecutionRequest,
        prepared: PreparedSandbox,
    ) -> Result<RunningSandbox, SandboxBackendFailure>;

    async fn collect(
        &self,
        fence: MicroVmProviderExecutionFence,
        request: SandboxExecutionRequest,
        running: RunningSandbox,
    ) -> Result<CollectedSandbox, SandboxBackendFailure>;

    async fn terminate(
        &self,
        fence: MicroVmProviderExecutionFence,
        command: TerminateSandbox,
    ) -> Result<SandboxTerminationEvidence, SandboxBackendFailure>;

    async fn destroy(
        &self,
        fence: MicroVmProviderExecutionFence,
        command: DestroySandbox,
    ) -> Result<SandboxCleanupEvidence, SandboxBackendFailure>;

    async fn abort(
        &self,
        fence: MicroVmProviderExecutionFence,
        command: AbortSandboxExecution,
    ) -> Result<SandboxAbortEvidence, SandboxBackendFailure>;

    async fn recover_expired_lease(
        &self,
        fence: MicroVmProviderExecutionFence,
        expired: ExpiredSandboxLease,
    ) -> Result<SandboxLeaseRecoveryEvidence, SandboxBackendFailure>;
}

#[derive(Default, Clone)]
pub struct InstalledSandboxBackendRegistry {
    backends: BTreeMap<InstalledSandboxBackendDescriptor, Arc<dyn SandboxExecutorBackend>>,
}

impl InstalledSandboxBackendRegistry {
    pub fn install(
        &mut self,
        backend: Arc<dyn SandboxExecutorBackend>,
    ) -> Result<(), SandboxBackendHostError> {
        let descriptor = backend.descriptor();
        descriptor.validate()?;
        match self.backends.entry(descriptor) {
            Entry::Vacant(entry) => {
                entry.insert(backend);
                Ok(())
            }
            Entry::Occupied(_) => Err(SandboxBackendHostError::DuplicateInstalledBackend),
        }
    }

    fn resolve(
        &self,
        request: &SandboxExecutionRequest,
    ) -> Result<Arc<dyn SandboxExecutorBackend>, SandboxBackendHostError> {
        self.backends
            .iter()
            .find(|(descriptor, _)| {
                descriptor.isolation_class == request.isolation_class
                    && descriptor.worker_manifest_digest == request.executor_worker_manifest_digest
                    && descriptor.backend_contract_digest
                        == request.isolation_backend_contract_digest
            })
            .map(|(_, backend)| Arc::clone(backend))
            .ok_or(SandboxBackendHostError::BackendNotInstalled)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SandboxHostExecution {
    pub prepared: PreparedSandbox,
    pub running: RunningSandbox,
    pub collection_evidence_digest: Sha256Digest,
    pub outcome: SandboxExecutionOutcome,
    pub cleanup: SandboxCleanupEvidence,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SandboxHostFailedExecution {
    pub prepared: PreparedSandbox,
    pub running: Option<RunningSandbox>,
    pub failure: SandboxBackendFailure,
    pub outcome: SandboxExecutionOutcome,
    pub termination: Option<SandboxTerminationEvidence>,
    pub cleanup: SandboxCleanupEvidence,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SandboxHostStoppedExecution {
    pub reason: SandboxStopReason,
    pub abort: SandboxAbortEvidence,
    pub outcome: SandboxExecutionOutcome,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SandboxHostAttempt {
    Completed(SandboxHostExecution),
    Failed(SandboxHostFailedExecution),
    Stopped(SandboxHostStoppedExecution),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SandboxLifecycleObservation {
    Phase {
        target: SandboxJobState,
        executor_identity_digest: Sha256Digest,
        phase_evidence_digest: Sha256Digest,
        prepared: Option<PreparedSandbox>,
    },
    Terminal {
        outcome: SandboxExecutionOutcome,
        cleanup: SandboxCleanupEvidence,
        phase_evidence_digest: Sha256Digest,
    },
}

#[async_trait]
pub trait SandboxLifecycleObserver: Send + Sync {
    type Error;

    async fn observe(&self, observation: SandboxLifecycleObservation) -> Result<(), Self::Error>;
}

struct NoopSandboxLifecycleObserver;

#[async_trait]
impl SandboxLifecycleObserver for NoopSandboxLifecycleObserver {
    type Error = std::convert::Infallible;

    async fn observe(&self, _observation: SandboxLifecycleObservation) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Debug)]
pub enum SandboxObservedExecutionError<E> {
    Host(SandboxBackendHostError),
    Observer(E),
}

pub struct SandboxExecutorHost {
    registry: InstalledSandboxBackendRegistry,
    limits: SandboxCommandLimits,
}

impl SandboxExecutorHost {
    pub fn new(registry: InstalledSandboxBackendRegistry, limits: SandboxCommandLimits) -> Self {
        Self { registry, limits }
    }

    pub async fn execute(
        &self,
        request: SandboxExecutionRequest,
    ) -> Result<SandboxHostExecution, SandboxBackendHostError> {
        self.execute_cancellable(request, CancellationToken::new())
            .await
    }

    /// Produces the only legal recovery action for a read-only expired-lease scan record.
    pub async fn recover_expired_lease(
        &self,
        expired: ExpiredSandboxLease,
    ) -> Result<SandboxLeaseRecoveryAction, SandboxBackendHostError> {
        expired
            .validate(self.limits)
            .map_err(|_| SandboxBackendHostError::InvalidBackendEvidence)?;
        if expired.physical_state == SandboxJobState::Accepted {
            let recovery_evidence_digest: Sha256Digest =
                insight_platform_contracts::canonical_digest(&serde_json::json!({
                    "schema_version": 1,
                    "kind": "expired_unstarted_sandbox_lease",
                    "tenant_id": expired.tenant_id,
                    "job_id": expired.job_id,
                    "observed_job_version": expired.observed_job_version,
                    "observed_lease_generation": expired.observed_lease_generation,
                }))
                .map_err(|_| SandboxBackendHostError::InvalidBackendEvidence)?
                .parse()
                .map_err(|_| SandboxBackendHostError::InvalidBackendEvidence)?;
            return Ok(
                if expired.database_observed_at >= expired.request.deadline {
                    SandboxLeaseRecoveryAction::TimeoutUnstarted {
                        recovery_evidence_digest,
                    }
                } else {
                    SandboxLeaseRecoveryAction::RequeueUnstarted {
                        recovery_evidence_digest,
                    }
                },
            );
        }
        let backend = self.registry.resolve(&expired.request)?;
        let resolved = backend.descriptor();
        if resolved.isolation_class != expired.request.isolation_class
            || resolved.worker_manifest_digest != expired.request.executor_worker_manifest_digest
            || resolved.backend_contract_digest != expired.request.isolation_backend_contract_digest
        {
            return Err(SandboxBackendHostError::InstalledBackendChanged);
        }
        let evidence = backend
            .recover_expired_lease(expired.clone())
            .await
            .map_err(SandboxBackendHostError::BackendFailure)?;
        evidence.validate_for(&expired, Utc::now())?;
        Ok(SandboxLeaseRecoveryAction::MarkLost {
            uncertainty: evidence.uncertainty,
            cleanup: evidence.cleanup,
            recovery_evidence_digest: evidence.evidence_digest,
        })
    }

    pub async fn execute_cancellable(
        &self,
        request: SandboxExecutionRequest,
        cancellation: CancellationToken,
    ) -> Result<SandboxHostExecution, SandboxBackendHostError> {
        self.execute_with_stop_token(
            request,
            SandboxExecutionStopToken::from_cancellation(cancellation),
        )
        .await
    }

    pub(crate) async fn execute_with_stop_token(
        &self,
        request: SandboxExecutionRequest,
        stop: SandboxExecutionStopToken,
    ) -> Result<SandboxHostExecution, SandboxBackendHostError> {
        let executor_identity_digest = request.executor_worker_manifest_digest.clone();
        match self
            .execute_observed_with_stop_token(
                request,
                executor_identity_digest,
                &NoopSandboxLifecycleObserver,
                stop,
            )
            .await
        {
            Ok(SandboxHostAttempt::Completed(execution)) => Ok(execution),
            Ok(SandboxHostAttempt::Failed(execution)) => {
                Err(SandboxBackendHostError::BackendFailure(execution.failure))
            }
            Ok(SandboxHostAttempt::Stopped(execution)) => {
                Err(SandboxBackendHostError::ExecutionStopped(execution.reason))
            }
            Err(SandboxObservedExecutionError::Host(error)) => Err(error),
            Err(SandboxObservedExecutionError::Observer(error)) => match error {},
        }
    }

    pub async fn execute_observed<O>(
        &self,
        request: SandboxExecutionRequest,
        executor_identity_digest: Sha256Digest,
        observer: &O,
    ) -> Result<SandboxHostAttempt, SandboxObservedExecutionError<O::Error>>
    where
        O: SandboxLifecycleObserver,
    {
        self.execute_observed_cancellable(
            request,
            executor_identity_digest,
            observer,
            CancellationToken::new(),
        )
        .await
    }

    pub async fn execute_observed_cancellable<O>(
        &self,
        request: SandboxExecutionRequest,
        executor_identity_digest: Sha256Digest,
        observer: &O,
        cancellation: CancellationToken,
    ) -> Result<SandboxHostAttempt, SandboxObservedExecutionError<O::Error>>
    where
        O: SandboxLifecycleObserver,
    {
        self.execute_observed_with_stop_token(
            request,
            executor_identity_digest,
            observer,
            SandboxExecutionStopToken::from_cancellation(cancellation),
        )
        .await
    }

    pub(crate) async fn execute_observed_with_stop_token<O>(
        &self,
        request: SandboxExecutionRequest,
        executor_identity_digest: Sha256Digest,
        observer: &O,
        stop: SandboxExecutionStopToken,
    ) -> Result<SandboxHostAttempt, SandboxObservedExecutionError<O::Error>>
    where
        O: SandboxLifecycleObserver,
    {
        let observed_now = Utc::now();
        request
            .validate_at(observed_now, self.limits)
            .map_err(SandboxBackendHostError::from)
            .map_err(SandboxObservedExecutionError::Host)?;
        let backend = self
            .registry
            .resolve(&request)
            .map_err(SandboxObservedExecutionError::Host)?;
        let resolved = backend.descriptor();
        if resolved.isolation_class != request.isolation_class
            || resolved.worker_manifest_digest != request.executor_worker_manifest_digest
            || resolved.backend_contract_digest != request.isolation_backend_contract_digest
        {
            return Err(SandboxObservedExecutionError::Host(
                SandboxBackendHostError::InstalledBackendChanged,
            ));
        }
        observer
            .observe(SandboxLifecycleObservation::Phase {
                target: SandboxJobState::Preparing,
                executor_identity_digest: executor_identity_digest.clone(),
                phase_evidence_digest: phase_evidence_digest("preparing", &request, &resolved),
                prepared: None,
            })
            .await
            .map_err(SandboxObservedExecutionError::Observer)?;
        let wall_budget = request
            .deadline
            .signed_duration_since(observed_now)
            .to_std()
            .map_err(|_| {
                SandboxObservedExecutionError::Host(SandboxBackendHostError::InvalidBackendEvidence)
            })?
            .min(Duration::from_millis(request.resources.wall_milliseconds));
        let execution_started = tokio::time::Instant::now();
        let execution_deadline = execution_started + wall_budget;
        let startup_deadline = execution_started
            + Duration::from_millis(request.resources.startup_milliseconds).min(wall_budget);
        let prepared = match run_cancellable_stage(
            startup_deadline,
            stop.clone(),
            backend.prepare(request.clone()),
            SandboxBackendFailureStage::Preparing,
            false,
            &request,
        )
        .await
        {
            Ok(prepared) => prepared,
            Err(SandboxControlledStageError::Stopped(reason)) => {
                return self
                    .finish_stopped_attempt(
                        observer,
                        backend.as_ref(),
                        &request,
                        reason,
                        &executor_identity_digest,
                        None,
                    )
                    .await;
            }
            Err(SandboxControlledStageError::Backend(error)) if is_backend_timeout(&error) => {
                return self
                    .finish_stopped_attempt(
                        observer,
                        backend.as_ref(),
                        &request,
                        SandboxStopReason::TimedOut,
                        &executor_identity_digest,
                        None,
                    )
                    .await;
            }
            Err(SandboxControlledStageError::Backend(error)) => {
                return Err(SandboxObservedExecutionError::Host(error));
            }
        };
        prepared
            .validate_for(&request)
            .map_err(SandboxObservedExecutionError::Host)?;
        if let Err(error) = observer
            .observe(SandboxLifecycleObservation::Phase {
                target: SandboxJobState::Starting,
                executor_identity_digest: executor_identity_digest.clone(),
                phase_evidence_digest: prepared.prepare_evidence_digest.clone(),
                prepared: Some(prepared.clone()),
            })
            .await
        {
            let _ = self
                .destroy_after_failure(backend.as_ref(), &request, &prepared)
                .await;
            return Err(SandboxObservedExecutionError::Observer(error));
        }
        let running = match run_cancellable_stage(
            startup_deadline,
            stop.clone(),
            backend.start(request.clone(), prepared.clone()),
            SandboxBackendFailureStage::Starting,
            true,
            &request,
        )
        .await
        {
            Ok(running) => running,
            Err(SandboxControlledStageError::Stopped(reason)) => {
                return self
                    .finish_stopped_attempt(
                        observer,
                        backend.as_ref(),
                        &request,
                        reason,
                        &executor_identity_digest,
                        Some(&prepared.sandbox_identity_digest),
                    )
                    .await;
            }
            Err(SandboxControlledStageError::Backend(error)) if is_backend_timeout(&error) => {
                return self
                    .finish_stopped_attempt(
                        observer,
                        backend.as_ref(),
                        &request,
                        SandboxStopReason::TimedOut,
                        &executor_identity_digest,
                        Some(&prepared.sandbox_identity_digest),
                    )
                    .await;
            }
            Err(SandboxControlledStageError::Backend(error)) => {
                let cleanup = self
                    .destroy_after_failure(backend.as_ref(), &request, &prepared)
                    .await
                    .map_err(SandboxObservedExecutionError::Host)?;
                return self
                    .finish_failed_attempt(observer, prepared, None, None, error, cleanup)
                    .await;
            }
        };
        running
            .prepared
            .validate_for(&request)
            .map_err(SandboxObservedExecutionError::Host)?;
        if let Err(error) = observer
            .observe(SandboxLifecycleObservation::Phase {
                target: SandboxJobState::Running,
                executor_identity_digest: executor_identity_digest.clone(),
                phase_evidence_digest: running.start_evidence_digest.clone(),
                prepared: None,
            })
            .await
        {
            let _ = self
                .terminate_after_failure(backend.as_ref(), &request, &prepared)
                .await;
            let _ = self
                .destroy_after_failure(backend.as_ref(), &request, &prepared)
                .await;
            return Err(SandboxObservedExecutionError::Observer(error));
        }
        if let Err(error) = observer
            .observe(SandboxLifecycleObservation::Phase {
                target: SandboxJobState::Collecting,
                executor_identity_digest: executor_identity_digest.clone(),
                phase_evidence_digest: phase_evidence_digest("collecting", &request, &resolved),
                prepared: None,
            })
            .await
        {
            let _ = self
                .terminate_after_failure(backend.as_ref(), &request, &prepared)
                .await;
            let _ = self
                .destroy_after_failure(backend.as_ref(), &request, &prepared)
                .await;
            return Err(SandboxObservedExecutionError::Observer(error));
        }
        let collected = match run_cancellable_stage(
            execution_deadline,
            stop,
            backend.collect(request.clone(), running.clone()),
            SandboxBackendFailureStage::Running,
            true,
            &request,
        )
        .await
        {
            Ok(collected) => collected,
            Err(SandboxControlledStageError::Stopped(reason)) => {
                return self
                    .finish_stopped_attempt(
                        observer,
                        backend.as_ref(),
                        &request,
                        reason,
                        &executor_identity_digest,
                        Some(&prepared.sandbox_identity_digest),
                    )
                    .await;
            }
            Err(SandboxControlledStageError::Backend(error)) if is_backend_timeout(&error) => {
                return self
                    .finish_stopped_attempt(
                        observer,
                        backend.as_ref(),
                        &request,
                        SandboxStopReason::TimedOut,
                        &executor_identity_digest,
                        Some(&prepared.sandbox_identity_digest),
                    )
                    .await;
            }
            Err(SandboxControlledStageError::Backend(error)) => {
                let termination = self
                    .terminate_after_failure(backend.as_ref(), &request, &prepared)
                    .await
                    .ok();
                let cleanup = self
                    .destroy_after_failure(backend.as_ref(), &request, &prepared)
                    .await
                    .map_err(SandboxObservedExecutionError::Host)?;
                return self
                    .finish_failed_attempt(
                        observer,
                        prepared,
                        Some(running),
                        termination,
                        error,
                        cleanup,
                    )
                    .await;
            }
        };
        let evidence_valid = collected.running.prepared.validate_for(&request).is_ok()
            && collected.usage.validate_for(&request).is_ok()
            && collected
                .outcome
                .validate_for(&request, self.limits)
                .is_ok()
            && !matches!(
                &collected.outcome,
                SandboxExecutionOutcome::Completed(output) if output.usage != collected.usage
            )
            && !matches!(
                &collected.outcome,
                SandboxExecutionOutcome::ManagedMcp(output) if output.usage != collected.usage
            );
        if !evidence_valid {
            let termination = self
                .terminate_after_failure(backend.as_ref(), &request, &prepared)
                .await
                .ok();
            let cleanup = self
                .destroy_after_failure(backend.as_ref(), &request, &prepared)
                .await
                .map_err(SandboxObservedExecutionError::Host)?;
            let mut failure = host_failure(
                SandboxBackendFailureStage::Collecting,
                "sandbox_invalid_backend_evidence",
                true,
                &request,
            );
            failure.sandbox_identity_digest = Some(prepared.sandbox_identity_digest.clone());
            return self
                .finish_failed_attempt(
                    observer,
                    prepared,
                    Some(running),
                    termination,
                    SandboxBackendHostError::BackendFailure(failure),
                    cleanup,
                )
                .await;
        }
        let cleanup = self
            .destroy_after_failure(backend.as_ref(), &request, &prepared)
            .await
            .map_err(SandboxObservedExecutionError::Host)?;
        if cleanup.disposition != SandboxCleanupDisposition::Destroyed
            || cleanup.sandbox_identity_digest != prepared.sandbox_identity_digest
        {
            return Err(SandboxObservedExecutionError::Host(
                SandboxBackendHostError::InvalidBackendEvidence,
            ));
        }
        cleanup
            .validate(request.deadline, request.resources.cleanup_milliseconds)
            .map_err(|_| {
                SandboxObservedExecutionError::Host(SandboxBackendHostError::InvalidBackendEvidence)
            })?;
        observer
            .observe(SandboxLifecycleObservation::Terminal {
                outcome: collected.outcome.clone(),
                cleanup: cleanup.clone(),
                phase_evidence_digest: collected.collection_evidence_digest.clone(),
            })
            .await
            .map_err(SandboxObservedExecutionError::Observer)?;
        Ok(SandboxHostAttempt::Completed(SandboxHostExecution {
            prepared,
            running,
            collection_evidence_digest: collected.collection_evidence_digest,
            outcome: collected.outcome,
            cleanup,
        }))
    }

    async fn finish_stopped_attempt<O>(
        &self,
        observer: &O,
        backend: &dyn SandboxExecutorBackend,
        request: &SandboxExecutionRequest,
        reason: SandboxStopReason,
        executor_identity_digest: &Sha256Digest,
        expected_sandbox_identity_digest: Option<&Sha256Digest>,
    ) -> Result<SandboxHostAttempt, SandboxObservedExecutionError<O::Error>>
    where
        O: SandboxLifecycleObserver,
    {
        let cancelling_observation = SandboxLifecycleObservation::Phase {
            target: SandboxJobState::Cancelling,
            executor_identity_digest: executor_identity_digest.clone(),
            phase_evidence_digest: phase_evidence_digest(
                "cancelling",
                request,
                &backend.descriptor(),
            ),
            prepared: None,
        };
        let (cancelling, abort) = tokio::join!(
            observer.observe(cancelling_observation),
            self.abort_after_stop(backend, request, reason),
        );
        let abort = abort.map_err(SandboxObservedExecutionError::Host)?;
        cancelling.map_err(SandboxObservedExecutionError::Observer)?;
        if expected_sandbox_identity_digest
            .is_some_and(|expected| expected != &abort.sandbox_identity_digest)
        {
            return Err(SandboxObservedExecutionError::Host(
                SandboxBackendHostError::InvalidBackendEvidence,
            ));
        }
        let outcome = match reason {
            SandboxStopReason::Cancelled => {
                SandboxExecutionOutcome::Cancelled(abort.termination.clone())
            }
            SandboxStopReason::TimedOut => {
                SandboxExecutionOutcome::TimedOut(abort.termination.clone())
            }
        };
        observer
            .observe(SandboxLifecycleObservation::Terminal {
                outcome: outcome.clone(),
                cleanup: abort.cleanup.clone(),
                phase_evidence_digest: abort.evidence_digest.clone(),
            })
            .await
            .map_err(SandboxObservedExecutionError::Observer)?;
        Ok(SandboxHostAttempt::Stopped(SandboxHostStoppedExecution {
            reason,
            abort,
            outcome,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_failed_attempt<O>(
        &self,
        observer: &O,
        prepared: PreparedSandbox,
        running: Option<RunningSandbox>,
        termination: Option<SandboxTerminationEvidence>,
        error: SandboxBackendHostError,
        cleanup: SandboxCleanupEvidence,
    ) -> Result<SandboxHostAttempt, SandboxObservedExecutionError<O::Error>>
    where
        O: SandboxLifecycleObserver,
    {
        let SandboxBackendHostError::BackendFailure(mut failure) = error else {
            return Err(SandboxObservedExecutionError::Host(error));
        };
        if failure.sandbox_identity_digest.is_none() {
            failure.sandbox_identity_digest = Some(prepared.sandbox_identity_digest.clone());
        }
        let outcome = failure.clone().into_execution_outcome();
        observer
            .observe(SandboxLifecycleObservation::Terminal {
                outcome: outcome.clone(),
                cleanup: cleanup.clone(),
                phase_evidence_digest: failure.evidence_digest.clone(),
            })
            .await
            .map_err(SandboxObservedExecutionError::Observer)?;
        Ok(SandboxHostAttempt::Failed(SandboxHostFailedExecution {
            prepared,
            running,
            failure,
            outcome,
            termination,
            cleanup,
        }))
    }

    async fn terminate_after_failure(
        &self,
        backend: &dyn SandboxExecutorBackend,
        request: &SandboxExecutionRequest,
        prepared: &PreparedSandbox,
    ) -> Result<SandboxTerminationEvidence, SandboxBackendHostError> {
        run_stage(
            tokio::time::Instant::now()
                + Duration::from_millis(request.resources.cleanup_milliseconds),
            backend.terminate(TerminateSandbox {
                tenant_id: request.tenant_id.clone(),
                sandbox_job_id: request.sandbox_job_id.clone(),
                request_digest: request.request_digest.clone(),
                sandbox_identity_digest: prepared.sandbox_identity_digest.clone(),
                attempt_no: request.attempt_no,
                lease_generation: request.lease_generation,
                effect: request.effect,
                network_mode: request.network_mode,
                deadline: request.deadline,
            }),
            SandboxBackendFailureStage::Terminating,
            true,
            request,
        )
        .await
    }

    async fn abort_after_stop(
        &self,
        backend: &dyn SandboxExecutorBackend,
        request: &SandboxExecutionRequest,
        reason: SandboxStopReason,
    ) -> Result<SandboxAbortEvidence, SandboxBackendHostError> {
        let evidence = run_stage(
            tokio::time::Instant::now()
                + Duration::from_millis(request.resources.cleanup_milliseconds),
            backend.abort(AbortSandboxExecution {
                tenant_id: request.tenant_id.clone(),
                sandbox_job_id: request.sandbox_job_id.clone(),
                request_digest: request.request_digest.clone(),
                attempt_no: request.attempt_no,
                lease_generation: request.lease_generation,
                reason,
                effect: request.effect,
                network_mode: request.network_mode,
                execution_deadline: request.deadline,
            }),
            SandboxBackendFailureStage::Aborting,
            true,
            request,
        )
        .await?;
        evidence.validate_for(request, reason)?;
        Ok(evidence)
    }

    async fn destroy_after_failure(
        &self,
        backend: &dyn SandboxExecutorBackend,
        request: &SandboxExecutionRequest,
        prepared: &PreparedSandbox,
    ) -> Result<SandboxCleanupEvidence, SandboxBackendHostError> {
        run_stage(
            tokio::time::Instant::now()
                + Duration::from_millis(request.resources.cleanup_milliseconds),
            backend.destroy(DestroySandbox {
                tenant_id: request.tenant_id.clone(),
                sandbox_job_id: request.sandbox_job_id.clone(),
                request_digest: request.request_digest.clone(),
                sandbox_identity_digest: prepared.sandbox_identity_digest.clone(),
                attempt_no: request.attempt_no,
                lease_generation: request.lease_generation,
                effect: request.effect,
                network_mode: request.network_mode,
            }),
            SandboxBackendFailureStage::Destroying,
            true,
            request,
        )
        .await
    }
}

enum SandboxControlledStageError {
    Stopped(SandboxStopReason),
    Backend(SandboxBackendHostError),
}

async fn run_cancellable_stage<T>(
    deadline: tokio::time::Instant,
    stop: SandboxExecutionStopToken,
    future: impl std::future::Future<Output = Result<T, SandboxBackendFailure>>,
    stage: SandboxBackendFailureStage,
    execution_may_have_started: bool,
    request: &SandboxExecutionRequest,
) -> Result<T, SandboxControlledStageError> {
    tokio::select! {
        biased;
        reason = stop.cancelled() => Err(SandboxControlledStageError::Stopped(reason)),
        result = run_stage(
            deadline,
            future,
            stage,
            execution_may_have_started,
            request,
        ) => result.map_err(SandboxControlledStageError::Backend),
    }
}

fn is_backend_timeout(error: &SandboxBackendHostError) -> bool {
    matches!(
        error,
        SandboxBackendHostError::BackendFailure(SandboxBackendFailure { safe_code, .. })
            if safe_code == "sandbox_backend_timeout"
    )
}

async fn run_stage<T>(
    deadline: tokio::time::Instant,
    future: impl std::future::Future<Output = Result<T, SandboxBackendFailure>>,
    stage: SandboxBackendFailureStage,
    execution_may_have_started: bool,
    request: &SandboxExecutionRequest,
) -> Result<T, SandboxBackendHostError> {
    match tokio::time::timeout_at(deadline, AssertUnwindSafe(future).catch_unwind()).await {
        Ok(Ok(Ok(value))) => Ok(value),
        Ok(Ok(Err(failure))) => {
            failure.validate_for(request)?;
            Err(SandboxBackendHostError::BackendFailure(failure))
        }
        Ok(Err(_)) => Err(SandboxBackendHostError::BackendFailure(host_failure(
            stage,
            "sandbox_backend_panicked",
            execution_may_have_started,
            request,
        ))),
        Err(_) => Err(SandboxBackendHostError::BackendFailure(host_failure(
            stage,
            "sandbox_backend_timeout",
            execution_may_have_started,
            request,
        ))),
    }
}

fn host_failure(
    stage: SandboxBackendFailureStage,
    code: &str,
    execution_may_have_started: bool,
    request: &SandboxExecutionRequest,
) -> SandboxBackendFailure {
    let external_effect_possible = execution_may_have_started
        && (request.effect.risk_rank()
            >= insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
            || request.network_mode == crate::SandboxNetworkMode::BrokeredEgress);
    SandboxBackendFailure {
        stage,
        safe_code: code.to_owned(),
        safe_message: "Sandbox backend completion could not be observed".to_owned(),
        retryability: if external_effect_possible {
            Retryability::ReconcileBeforeRetry
        } else {
            Retryability::SafeWithinPolicy
        },
        evidence_digest: static_digest(code),
        sandbox_identity_digest: None,
        execution_may_have_started,
        external_effect_possible,
    }
}

fn static_digest(value: &str) -> Sha256Digest {
    let digest = insight_platform_contracts::canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "code": value,
    }))
    .expect("static Sandbox failure evidence is canonical");
    digest
        .parse()
        .expect("canonical digest is a valid SHA-256 digest")
}

fn phase_evidence_digest(
    phase: &str,
    request: &SandboxExecutionRequest,
    backend: &InstalledSandboxBackendDescriptor,
) -> Sha256Digest {
    let digest = insight_platform_contracts::canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "phase": phase,
        "request_digest": request.request_digest,
        "isolation_class": backend.isolation_class,
        "worker_manifest_digest": backend.worker_manifest_digest,
        "backend_contract_digest": backend.backend_contract_digest,
    }))
    .expect("closed Sandbox phase evidence is canonical");
    digest
        .parse()
        .expect("canonical digest is a valid SHA-256 digest")
}

impl SandboxBackendFailure {
    pub fn into_execution_outcome(self) -> SandboxExecutionOutcome {
        if self.execution_may_have_started && self.external_effect_possible {
            SandboxExecutionOutcome::Uncertain(SandboxUncertainty {
                evidence_digest: self.evidence_digest,
                sandbox_identity_digest: self
                    .sandbox_identity_digest
                    .unwrap_or_else(|| static_digest("sandbox_identity_unknown")),
                execution_may_have_started: true,
                external_effect_possible: true,
                manual_reconciliation_required: true,
            })
        } else {
            SandboxExecutionOutcome::Failed(SafeSandboxFailure {
                safe_code: self.safe_code,
                safe_message: self.safe_message,
                retryability: self.retryability,
                evidence_digest: self.evidence_digest,
                resource_violation: None,
                external_effect_possible: false,
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxBackendHostError {
    Contract(SandboxContractError),
    InvalidInstalledBackend,
    DuplicateInstalledBackend,
    BackendNotInstalled,
    InstalledBackendChanged,
    InvalidBackendEvidence,
    InvalidBackendFailure,
    ExecutionStopped(SandboxStopReason),
    BackendFailure(SandboxBackendFailure),
}

impl From<SandboxContractError> for SandboxBackendHostError {
    fn from(value: SandboxContractError) -> Self {
        Self::Contract(value)
    }
}

impl fmt::Display for SandboxBackendHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Contract(_) => "Sandbox execution contract is invalid",
            Self::InvalidInstalledBackend => {
                "Sandbox backend kind does not implement its isolation class"
            }
            Self::DuplicateInstalledBackend => "Sandbox backend is installed more than once",
            Self::BackendNotInstalled => "exact Sandbox backend is not installed",
            Self::InstalledBackendChanged => "Sandbox backend changed after exact resolution",
            Self::InvalidBackendEvidence => "Sandbox backend evidence is invalid",
            Self::InvalidBackendFailure => "Sandbox backend failure is unsafe or invalid",
            Self::ExecutionStopped(SandboxStopReason::Cancelled) => {
                "Sandbox execution was cancelled"
            }
            Self::ExecutionStopped(SandboxStopReason::TimedOut) => "Sandbox execution timed out",
            Self::BackendFailure(_) => "Sandbox backend failed",
        })
    }
}

impl Error for SandboxBackendHostError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Contract(error) => Some(error),
            _ => None,
        }
    }
}

pub fn validate_executor_manifest_for_sandbox(
    manifest: &WorkerManifest,
    request: &SandboxExecutionRequest,
) -> Result<(), SandboxBackendHostError> {
    manifest
        .validate()
        .map_err(|_| SandboxBackendHostError::InvalidBackendEvidence)?;
    if manifest
        .canonical_digest()
        .map_err(|_| SandboxBackendHostError::InvalidBackendEvidence)?
        != request.executor_worker_manifest_digest
        || manifest.work_class != insight_platform_contracts::WorkClass::Sandbox
    {
        return Err(SandboxBackendHostError::InvalidBackendEvidence);
    }
    Ok(())
}
