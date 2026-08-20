use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, Effect, ResourceId, ResourceKind, Retryability, SandboxEntrypointKind,
    SandboxIsolationClass, SandboxJobState, SandboxRuntimeFamily, Sha256Digest,
};
use insight_platform_sandbox::{
    AbortSandboxExecution, CollectedSandbox, DestroySandbox, ExpiredSandboxLease,
    InstalledSandboxBackendDescriptor, PreparedSandbox, RunningSandbox, SandboxAbortEvidence,
    SandboxBackendFailure, SandboxBackendFailureStage, SandboxCleanupDisposition,
    SandboxCleanupEvidence, SandboxExecutionOutcome, SandboxExecutionRequest,
    SandboxExecutorBackend, SandboxIsolationBackendKind, SandboxLeaseRecoveryEvidence,
    SandboxNetworkMode, SandboxResourceUsage, SandboxTerminationEvidence, SandboxUncertainty,
    TerminateSandbox,
};
use insight_platform_sandbox_gvisor::{
    GvisorContainerIdentity, GvisorRuntimeError, GvisorRuntimePort, RunscCommandOutput,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const PHASE_PREPARING: u8 = 0;
const PHASE_PREPARED: u8 = 1;
const PHASE_RUNNING: u8 = 2;
const PHASE_COLLECTED: u8 = 3;

pub trait GvisorExecutorClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default)]
pub struct SystemGvisorExecutorClock;

impl GvisorExecutorClock for SystemGvisorExecutorClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedGvisorBundle {
    pub bundle_path: PathBuf,
    pub image_digest: Sha256Digest,
    pub materialization_evidence_digest: Sha256Digest,
}

impl MaterializedGvisorBundle {
    fn validate_for(&self, request: &SandboxExecutionRequest) -> bool {
        self.bundle_path.is_absolute()
            && self.bundle_path != Path::new("/")
            && !self
                .bundle_path
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
            && self.image_digest == request.runtime.image_or_module_digest
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectedGvisorOutput {
    pub outcome: SandboxExecutionOutcome,
    pub usage: SandboxResourceUsage,
    pub collection_evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GvisorCleanupReceipt {
    pub grants_revoked: bool,
    pub ephemeral_storage_destroyed: bool,
    pub evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GvisorBrokerError {
    Denied,
    Integrity,
    Unavailable,
}

/// Trusted ports implemented by the Artifact Data Worker and Egress/Secret broker composition.
/// No object-store credential or raw Secret is returned to this backend.
#[async_trait]
pub trait GvisorWorkloadBroker: Send + Sync {
    async fn materialize_bundle(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
    ) -> Result<MaterializedGvisorBundle, GvisorBrokerError>;

    async fn collect_output(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
        runtime_output: RunscCommandOutput,
    ) -> Result<CollectedGvisorOutput, GvisorBrokerError>;

    async fn revoke_grants(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
    ) -> Result<Sha256Digest, GvisorBrokerError>;

    async fn cleanup(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
        bundle: Option<&MaterializedGvisorBundle>,
    ) -> Result<GvisorCleanupReceipt, GvisorBrokerError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GvisorExecutorBackendConfig {
    pub descriptor: InstalledSandboxBackendDescriptor,
    pub worker_process_generation_id: ResourceId,
    pub maximum_concurrent_executions: usize,
}

impl GvisorExecutorBackendConfig {
    fn validate(&self) -> bool {
        self.descriptor.backend_kind == SandboxIsolationBackendKind::Gvisor
            && self.descriptor.isolation_class == SandboxIsolationClass::SandboxedContainer
            && self.worker_process_generation_id.kind() == ResourceKind::WorkerProcessGeneration
            && (1..=4_096).contains(&self.maximum_concurrent_executions)
    }
}

pub struct RunscSandboxExecutorBackend {
    config: GvisorExecutorBackendConfig,
    runtime: Arc<dyn GvisorRuntimePort>,
    broker: Arc<dyn GvisorWorkloadBroker>,
    clock: Arc<dyn GvisorExecutorClock>,
    permits: Arc<Semaphore>,
    executions: Mutex<BTreeMap<Sha256Digest, Arc<GvisorExecution>>>,
}

struct GvisorExecution {
    request: SandboxExecutionRequest,
    identity: GvisorContainerIdentity,
    bundle: Mutex<Option<MaterializedGvisorBundle>>,
    phase: AtomicU8,
    cancelled: AtomicBool,
    _permit: OwnedSemaphorePermit,
}

impl RunscSandboxExecutorBackend {
    pub async fn new(
        config: GvisorExecutorBackendConfig,
        runtime: Arc<dyn GvisorRuntimePort>,
        broker: Arc<dyn GvisorWorkloadBroker>,
        clock: Arc<dyn GvisorExecutorClock>,
    ) -> Result<Self, GvisorBackendConfigurationError> {
        if !config.validate() || runtime.verify().await.is_err() {
            return Err(GvisorBackendConfigurationError);
        }
        let permits = Arc::new(Semaphore::new(config.maximum_concurrent_executions));
        Ok(Self {
            config,
            runtime,
            broker,
            clock,
            permits,
            executions: Mutex::new(BTreeMap::new()),
        })
    }

    fn lookup(
        &self,
        request_digest: &Sha256Digest,
        stage: SandboxBackendFailureStage,
    ) -> Result<Arc<GvisorExecution>, SandboxBackendFailure> {
        self.executions
            .lock()
            .map_err(|_| failure(stage, "sandbox_gvisor_state_unavailable", None, true))?
            .get(request_digest)
            .cloned()
            .ok_or_else(|| failure(stage, "sandbox_gvisor_execution_missing", None, true))
    }

    fn remove(&self, request_digest: &Sha256Digest) {
        if let Ok(mut executions) = self.executions.lock() {
            executions.remove(request_digest);
        }
    }

    async fn broker_cleanup(
        &self,
        execution: &GvisorExecution,
        stage: SandboxBackendFailureStage,
    ) -> Result<GvisorCleanupReceipt, SandboxBackendFailure> {
        let bundle = execution
            .bundle
            .lock()
            .map_err(|_| failure(stage, "sandbox_gvisor_state_unavailable", None, true))?
            .clone();
        self.broker
            .cleanup(
                &execution.request,
                &self.config.worker_process_generation_id,
                bundle.as_ref(),
            )
            .await
            .map_err(|error| broker_failure(error, stage, Some(identity_digest(execution)), true))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GvisorBackendConfigurationError;

#[async_trait]
impl SandboxExecutorBackend for RunscSandboxExecutorBackend {
    fn descriptor(&self) -> InstalledSandboxBackendDescriptor {
        self.config.descriptor.clone()
    }

    async fn prepare(
        &self,
        request: SandboxExecutionRequest,
    ) -> Result<PreparedSandbox, SandboxBackendFailure> {
        validate_request(&request)?;
        let permit = Arc::clone(&self.permits).try_acquire_owned().map_err(|_| {
            failure(
                SandboxBackendFailureStage::Admission,
                "sandbox_gvisor_capacity_exhausted",
                None,
                false,
            )
        })?;
        let identity = GvisorContainerIdentity {
            tenant_id: request.tenant_id.clone(),
            job_id: request.sandbox_job_id.clone(),
            lease_generation: request.lease_generation,
            worker_process_generation_id: self.config.worker_process_generation_id.clone(),
        };
        let execution = Arc::new(GvisorExecution {
            request: request.clone(),
            identity,
            bundle: Mutex::new(None),
            phase: AtomicU8::new(PHASE_PREPARING),
            cancelled: AtomicBool::new(false),
            _permit: permit,
        });
        {
            let mut executions = self.executions.lock().map_err(|_| {
                failure(
                    SandboxBackendFailureStage::Preparing,
                    "sandbox_gvisor_state_unavailable",
                    None,
                    false,
                )
            })?;
            if executions.contains_key(&request.request_digest) {
                return Err(failure(
                    SandboxBackendFailureStage::Admission,
                    "sandbox_gvisor_duplicate_execution",
                    None,
                    false,
                ));
            }
            executions.insert(request.request_digest.clone(), Arc::clone(&execution));
        }

        let prepared = async {
            let bundle = self
                .broker
                .materialize_bundle(&request, &self.config.worker_process_generation_id)
                .await
                .map_err(|error| {
                    broker_failure(error, SandboxBackendFailureStage::Preparing, None, false)
                })?;
            if !bundle.validate_for(&request) || execution.cancelled.load(Ordering::Acquire) {
                return Err(failure(
                    SandboxBackendFailureStage::Preparing,
                    "sandbox_gvisor_bundle_invalid",
                    None,
                    false,
                ));
            }
            *execution.bundle.lock().map_err(|_| {
                failure(
                    SandboxBackendFailureStage::Preparing,
                    "sandbox_gvisor_state_unavailable",
                    None,
                    false,
                )
            })? = Some(bundle.clone());
            self.runtime
                .create(&execution.identity, &bundle.bundle_path)
                .await
                .map_err(|error| {
                    runtime_failure(error, SandboxBackendFailureStage::Preparing, false)
                })?;
            if execution.cancelled.load(Ordering::Acquire) {
                return Err(failure(
                    SandboxBackendFailureStage::Preparing,
                    "sandbox_gvisor_prepare_cancelled",
                    Some(identity_digest(&execution)),
                    false,
                ));
            }
            execution.phase.store(PHASE_PREPARED, Ordering::Release);
            Ok(PreparedSandbox {
                provider_process_generation_id: None,
                sandbox_identity_digest: identity_digest(&execution),
                request_digest: request.request_digest.clone(),
                attempt_no: request.attempt_no,
                lease_generation: request.lease_generation,
                prepare_evidence_digest: bundle.materialization_evidence_digest,
            })
        }
        .await;
        if prepared.is_err() {
            let _ = self.runtime.recover_orphan(&execution.identity).await;
            let _ = self
                .broker_cleanup(&execution, SandboxBackendFailureStage::Preparing)
                .await;
            self.remove(&request.request_digest);
        }
        prepared
    }

    async fn start(
        &self,
        request: SandboxExecutionRequest,
        prepared: PreparedSandbox,
    ) -> Result<RunningSandbox, SandboxBackendFailure> {
        let execution = self.lookup(
            &request.request_digest,
            SandboxBackendFailureStage::Starting,
        )?;
        if !matches_execution(&execution, &request, &prepared.sandbox_identity_digest)
            || execution.cancelled.load(Ordering::Acquire)
            || execution
                .phase
                .compare_exchange(
                    PHASE_PREPARED,
                    PHASE_RUNNING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
        {
            return Err(failure(
                SandboxBackendFailureStage::Starting,
                "sandbox_gvisor_stale_start",
                Some(prepared.sandbox_identity_digest),
                false,
            ));
        }
        if let Err(error) = self.runtime.start(&execution.identity).await {
            let _ = self
                .broker_cleanup(&execution, SandboxBackendFailureStage::Starting)
                .await;
            self.remove(&request.request_digest);
            return Err(runtime_failure(
                error,
                SandboxBackendFailureStage::Starting,
                true,
            ));
        }
        Ok(RunningSandbox {
            prepared,
            start_evidence_digest: evidence_digest("gvisor_started", &request),
        })
    }

    async fn collect(
        &self,
        request: SandboxExecutionRequest,
        running: RunningSandbox,
    ) -> Result<CollectedSandbox, SandboxBackendFailure> {
        let execution = self.lookup(
            &request.request_digest,
            SandboxBackendFailureStage::Collecting,
        )?;
        if !matches_execution(
            &execution,
            &request,
            &running.prepared.sandbox_identity_digest,
        ) || execution.phase.load(Ordering::Acquire) != PHASE_RUNNING
        {
            return Err(failure(
                SandboxBackendFailureStage::Collecting,
                "sandbox_gvisor_stale_collect",
                Some(running.prepared.sandbox_identity_digest),
                true,
            ));
        }
        let output = self
            .runtime
            .wait(&execution.identity)
            .await
            .map_err(|error| runtime_failure(error, SandboxBackendFailureStage::Running, true))?;
        let collected = self
            .broker
            .collect_output(&request, &self.config.worker_process_generation_id, output)
            .await
            .map_err(|error| {
                broker_failure(
                    error,
                    SandboxBackendFailureStage::Collecting,
                    Some(identity_digest(&execution)),
                    true,
                )
            })?;
        execution.phase.store(PHASE_COLLECTED, Ordering::Release);
        Ok(CollectedSandbox {
            running,
            outcome: collected.outcome,
            usage: collected.usage,
            collection_evidence_digest: collected.collection_evidence_digest,
        })
    }

    async fn terminate(
        &self,
        command: TerminateSandbox,
    ) -> Result<SandboxTerminationEvidence, SandboxBackendFailure> {
        let execution = self.lookup(
            &command.request_digest,
            SandboxBackendFailureStage::Terminating,
        )?;
        if !matches_command(
            &execution,
            &command.tenant_id,
            &command.sandbox_job_id,
            command.attempt_no,
            command.lease_generation,
        ) || identity_digest(&execution) != command.sandbox_identity_digest
        {
            return Err(failure(
                SandboxBackendFailureStage::Terminating,
                "sandbox_gvisor_stale_terminate",
                Some(command.sandbox_identity_digest),
                true,
            ));
        }
        execution.cancelled.store(true, Ordering::Release);
        self.runtime
            .terminate(&execution.identity, true)
            .await
            .map_err(|error| {
                runtime_failure(error, SandboxBackendFailureStage::Terminating, true)
            })?;
        self.broker
            .revoke_grants(
                &execution.request,
                &self.config.worker_process_generation_id,
            )
            .await
            .map_err(|error| {
                broker_failure(
                    error,
                    SandboxBackendFailureStage::Terminating,
                    Some(identity_digest(&execution)),
                    true,
                )
            })?;
        Ok(termination_evidence(&execution.request))
    }

    async fn destroy(
        &self,
        command: DestroySandbox,
    ) -> Result<SandboxCleanupEvidence, SandboxBackendFailure> {
        let execution = self.lookup(
            &command.request_digest,
            SandboxBackendFailureStage::Destroying,
        )?;
        if !matches_command(
            &execution,
            &command.tenant_id,
            &command.sandbox_job_id,
            command.attempt_no,
            command.lease_generation,
        ) || identity_digest(&execution) != command.sandbox_identity_digest
        {
            return Err(failure(
                SandboxBackendFailureStage::Destroying,
                "sandbox_gvisor_stale_destroy",
                Some(command.sandbox_identity_digest),
                true,
            ));
        }
        self.runtime
            .destroy(&execution.identity)
            .await
            .map_err(|error| {
                runtime_failure(error, SandboxBackendFailureStage::Destroying, true)
            })?;
        let receipt = self
            .broker_cleanup(&execution, SandboxBackendFailureStage::Destroying)
            .await?;
        let cleanup = cleanup_evidence(identity_digest(&execution), receipt, self.clock.now())?;
        self.remove(&command.request_digest);
        Ok(cleanup)
    }

    async fn abort(
        &self,
        command: AbortSandboxExecution,
    ) -> Result<SandboxAbortEvidence, SandboxBackendFailure> {
        let execution = self.lookup(
            &command.request_digest,
            SandboxBackendFailureStage::Aborting,
        )?;
        if !matches_command(
            &execution,
            &command.tenant_id,
            &command.sandbox_job_id,
            command.attempt_no,
            command.lease_generation,
        ) {
            return Err(failure(
                SandboxBackendFailureStage::Aborting,
                "sandbox_gvisor_stale_abort",
                Some(identity_digest(&execution)),
                true,
            ));
        }
        execution.cancelled.store(true, Ordering::Release);
        self.runtime
            .recover_orphan(&execution.identity)
            .await
            .map_err(|error| runtime_failure(error, SandboxBackendFailureStage::Aborting, true))?;
        let receipt = self
            .broker_cleanup(&execution, SandboxBackendFailureStage::Aborting)
            .await?;
        let sandbox_identity_digest = identity_digest(&execution);
        let cleanup = cleanup_evidence(sandbox_identity_digest.clone(), receipt, self.clock.now())?;
        let termination = termination_evidence(&execution.request);
        self.remove(&command.request_digest);
        Ok(SandboxAbortEvidence {
            reason: command.reason,
            request_digest: command.request_digest,
            attempt_no: command.attempt_no,
            lease_generation: command.lease_generation,
            sandbox_identity_digest,
            termination,
            evidence_digest: cleanup.evidence_digest.clone(),
            cleanup,
        })
    }

    async fn recover_expired_lease(
        &self,
        expired: ExpiredSandboxLease,
    ) -> Result<SandboxLeaseRecoveryEvidence, SandboxBackendFailure> {
        let executor_identity_digest =
            expired.executor_identity_digest.clone().ok_or_else(|| {
                failure(
                    SandboxBackendFailureStage::Recovering,
                    "sandbox_gvisor_recovery_identity_missing",
                    None,
                    true,
                )
            })?;
        let identity = GvisorContainerIdentity {
            tenant_id: expired.tenant_id.clone(),
            job_id: expired.sandbox_job_id.clone(),
            lease_generation: expired.observed_lease_generation,
            worker_process_generation_id: expired.previous_worker_process_generation_id.clone(),
        };
        self.runtime
            .recover_orphan(&identity)
            .await
            .map_err(|error| {
                runtime_failure(error, SandboxBackendFailureStage::Recovering, true)
            })?;
        let receipt = self
            .broker
            .cleanup(
                &expired.request,
                &expired.previous_worker_process_generation_id,
                None,
            )
            .await
            .map_err(|error| {
                broker_failure(error, SandboxBackendFailureStage::Recovering, None, true)
            })?;
        let sandbox_identity_digest = identity_digest_from(&identity);
        let cleanup = cleanup_evidence(sandbox_identity_digest.clone(), receipt, self.clock.now())?;
        let external_effect_possible = expired.request.effect.risk_rank()
            >= Effect::IdempotentWrite.risk_rank()
            || expired.request.network_mode != SandboxNetworkMode::None;
        SandboxLeaseRecoveryEvidence {
            schema_version: 1,
            request_digest: expired.request.request_digest.clone(),
            attempt_no: expired.request.attempt_no,
            lease_generation: expired.observed_lease_generation,
            physical_state: expired.physical_state,
            executor_identity_digest,
            uncertainty: SandboxUncertainty {
                evidence_digest: evidence_digest("gvisor_recovery_uncertainty", &expired.request),
                sandbox_identity_digest,
                execution_may_have_started: !matches!(
                    expired.physical_state,
                    SandboxJobState::Preparing
                ),
                external_effect_possible,
                manual_reconciliation_required: external_effect_possible,
            },
            cleanup,
            evidence_digest: zero_digest(),
        }
        .seal()
        .map_err(|_| {
            failure(
                SandboxBackendFailureStage::Recovering,
                "sandbox_gvisor_recovery_evidence_invalid",
                None,
                true,
            )
        })
    }
}

fn validate_request(request: &SandboxExecutionRequest) -> Result<(), SandboxBackendFailure> {
    let runtime_ok = matches!(
        request.runtime.runtime_family,
        SandboxRuntimeFamily::Python
            | SandboxRuntimeFamily::NodeJs
            | SandboxRuntimeFamily::ReviewedShell
    );
    let entrypoint_ok = matches!(
        (
            request.runtime.runtime_family,
            request.package.entrypoint_kind
        ),
        (
            SandboxRuntimeFamily::Python,
            SandboxEntrypointKind::PythonModule
        ) | (
            SandboxRuntimeFamily::NodeJs,
            SandboxEntrypointKind::NodeModule
        ) | (
            SandboxRuntimeFamily::ReviewedShell,
            SandboxEntrypointKind::ReviewedExecutable
        )
    );
    if request.isolation_class != SandboxIsolationClass::SandboxedContainer
        || !runtime_ok
        || !entrypoint_ok
        || request.resources.wasm_fuel.is_some()
        || request.resources.wasm_memory_pages.is_some()
    {
        return Err(failure(
            SandboxBackendFailureStage::Admission,
            "sandbox_gvisor_contract_invalid",
            None,
            false,
        ));
    }
    Ok(())
}

fn matches_execution(
    execution: &GvisorExecution,
    request: &SandboxExecutionRequest,
    identity: &Sha256Digest,
) -> bool {
    execution.request.request_digest == request.request_digest
        && execution.request.tenant_id == request.tenant_id
        && execution.request.sandbox_job_id == request.sandbox_job_id
        && execution.request.attempt_no == request.attempt_no
        && execution.request.lease_generation == request.lease_generation
        && identity_digest(execution) == *identity
}

fn matches_command(
    execution: &GvisorExecution,
    tenant_id: &ResourceId,
    sandbox_job_id: &ResourceId,
    attempt_no: u32,
    lease_generation: u64,
) -> bool {
    &execution.request.tenant_id == tenant_id
        && &execution.request.sandbox_job_id == sandbox_job_id
        && execution.request.attempt_no == attempt_no
        && execution.request.lease_generation == lease_generation
}

fn identity_digest(execution: &GvisorExecution) -> Sha256Digest {
    identity_digest_from(&execution.identity)
}

fn identity_digest_from(identity: &GvisorContainerIdentity) -> Sha256Digest {
    canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "kind": "gvisor_container_identity",
        "tenant_id": identity.tenant_id,
        "job_id": identity.job_id,
        "lease_generation": identity.lease_generation,
        "worker_process_generation_id": identity.worker_process_generation_id,
    }))
    .expect("closed gVisor identity is canonical")
    .parse()
    .expect("canonical digest has SHA-256 shape")
}

fn termination_evidence(request: &SandboxExecutionRequest) -> SandboxTerminationEvidence {
    SandboxTerminationEvidence {
        evidence_digest: evidence_digest("gvisor_terminated", request),
        guest_acknowledged: false,
        process_tree_terminated: true,
        grants_revoked: true,
        external_effect_possible: request.effect.risk_rank() >= Effect::IdempotentWrite.risk_rank()
            || request.network_mode != SandboxNetworkMode::None,
    }
}

fn cleanup_evidence(
    identity: Sha256Digest,
    receipt: GvisorCleanupReceipt,
    observed_at: DateTime<Utc>,
) -> Result<SandboxCleanupEvidence, SandboxBackendFailure> {
    if !receipt.grants_revoked || !receipt.ephemeral_storage_destroyed {
        return Err(failure(
            SandboxBackendFailureStage::Destroying,
            "sandbox_gvisor_cleanup_incomplete",
            Some(identity),
            true,
        ));
    }
    Ok(SandboxCleanupEvidence {
        disposition: SandboxCleanupDisposition::Destroyed,
        sandbox_identity_digest: identity,
        grants_revoked: receipt.grants_revoked,
        ephemeral_storage_destroyed: receipt.ephemeral_storage_destroyed,
        observed_at,
        evidence_digest: receipt.evidence_digest,
    })
}

fn evidence_digest(kind: &str, request: &SandboxExecutionRequest) -> Sha256Digest {
    canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "kind": kind,
        "request_digest": request.request_digest,
        "attempt_no": request.attempt_no,
        "lease_generation": request.lease_generation,
    }))
    .expect("closed gVisor evidence is canonical")
    .parse()
    .expect("canonical digest has SHA-256 shape")
}

fn zero_digest() -> Sha256Digest {
    format!("sha256:{}", "0".repeat(64))
        .parse()
        .expect("zero digest is valid")
}

fn runtime_failure(
    error: GvisorRuntimeError,
    stage: SandboxBackendFailureStage,
    started: bool,
) -> SandboxBackendFailure {
    let code = match error {
        GvisorRuntimeError::TimedOut => "sandbox_gvisor_runtime_timed_out",
        GvisorRuntimeError::RuntimeMismatch => "sandbox_gvisor_runtime_mismatch",
        GvisorRuntimeError::CommandFailed => "sandbox_gvisor_runtime_command_failed",
        _ => "sandbox_gvisor_runtime_unavailable",
    };
    failure(stage, code, None, started)
}

fn broker_failure(
    error: GvisorBrokerError,
    stage: SandboxBackendFailureStage,
    identity: Option<Sha256Digest>,
    started: bool,
) -> SandboxBackendFailure {
    let (code, retryability) = match error {
        GvisorBrokerError::Denied => ("sandbox_gvisor_broker_denied", Retryability::Never),
        GvisorBrokerError::Integrity => ("sandbox_gvisor_broker_integrity", Retryability::Never),
        GvisorBrokerError::Unavailable => (
            "sandbox_gvisor_broker_unavailable",
            Retryability::SafeWithinPolicy,
        ),
    };
    let mut result = failure(stage, code, identity, started);
    result.retryability = retryability;
    result
}

fn failure(
    stage: SandboxBackendFailureStage,
    code: &str,
    identity: Option<Sha256Digest>,
    started: bool,
) -> SandboxBackendFailure {
    SandboxBackendFailure {
        stage,
        safe_code: code.to_owned(),
        safe_message: "gVisor sandbox operation failed closed".to_owned(),
        retryability: if started {
            Retryability::Never
        } else {
            Retryability::SafeWithinPolicy
        },
        evidence_digest: canonical_digest(
            &serde_json::json!({"schema_version": 1, "stage": stage, "code": code}),
        )
        .expect("closed gVisor failure evidence is canonical")
        .parse()
        .expect("canonical digest has SHA-256 shape"),
        sandbox_identity_digest: identity,
        execution_may_have_started: started,
        // The backend cannot assert an external effect from a runtime/process failure alone. The
        // durable recovery path derives uncertainty from the frozen Effect and network policy.
        external_effect_possible: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    #[test]
    fn backend_config_accepts_only_gvisor_sandboxed_container() {
        let mut config = GvisorExecutorBackendConfig {
            descriptor: InstalledSandboxBackendDescriptor {
                backend_kind: SandboxIsolationBackendKind::Gvisor,
                isolation_class: SandboxIsolationClass::SandboxedContainer,
                worker_manifest_digest: digest('a'),
                backend_contract_digest: digest('b'),
            },
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration),
            maximum_concurrent_executions: 8,
        };
        assert!(config.validate());
        config.descriptor.backend_kind = SandboxIsolationBackendKind::Wasi;
        assert!(!config.validate());
        config.descriptor.backend_kind = SandboxIsolationBackendKind::Gvisor;
        config.descriptor.isolation_class = SandboxIsolationClass::Wasm;
        assert!(!config.validate());
    }

    #[test]
    fn runtime_failure_does_not_invent_external_effect_evidence() {
        let failure = runtime_failure(
            GvisorRuntimeError::CommandFailed,
            SandboxBackendFailureStage::Running,
            true,
        );
        assert!(failure.execution_may_have_started);
        assert!(!failure.external_effect_possible);
        assert_eq!(failure.retryability, Retryability::Never);
    }
}
