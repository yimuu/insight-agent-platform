//! Production Wasmtime adapter for the closed Platform v1 Sandbox ABI.
//!
//! This crate is intentionally outside `insight-platform-sandbox`: only the dedicated Sandbox
//! Executor role may link the JIT/runtime. The API, scheduler, orchestration and generic worker
//! roles depend on the credential-free Sandbox ports and never on Wasmtime.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, ArtifactGrantOperation, ArtifactRef,
    Effect, JsonLimits, ResourceId, Retryability, SandboxAbiVersion, SandboxEntrypointKind,
    SandboxIsolationClass, SandboxRuntimeFamily, Sha256Digest, ValueRef,
};
use insight_platform_sandbox::{
    AbortSandboxExecution, CollectedSandbox, DestroySandbox, ExpiredSandboxLease,
    InstalledSandboxBackendDescriptor, PreparedSandbox, ProveSandboxProcessGenerationAbsent,
    RevokeWasiSandboxGrants, RunningSandbox, SafeSandboxFailure, SandboxAbortEvidence,
    SandboxBackendFailure, SandboxBackendFailureStage, SandboxCleanupDisposition,
    SandboxCleanupEvidence, SandboxCompletedOutput, SandboxExecutionOutcome,
    SandboxExecutionRequest, SandboxExecutorBackend, SandboxIsolationBackendKind,
    SandboxLeaseRecoveryEvidence, SandboxNetworkMode, SandboxProcessGenerationIsolation,
    SandboxProcessGenerationIsolationError, SandboxResourceUsage, SandboxTerminationEvidence,
    SandboxUncertainty, TerminateSandbox, WasiArtifactBroker, WasiArtifactBrokerError,
    WasiArtifactReadPurpose, WasiArtifactReadRequest, WasiGrantRevocationError,
    WasiGrantRevocationEvidence, WasiGrantRevoker, WasiValueDirection, WasiValueValidationError,
    WasiValueValidationRequest, WasiValueValidator,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};
use tokio::sync::Notify;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use wasmtime::{
    Config, Engine, ExternType, Instance, Module, Store, StoreLimits, StoreLimitsBuilder, Trap,
    ValType, WasmFeatures,
};

pub const WASI_ABI_V1_RUNTIME_VERSION: &str = "wasmtime-46.0.2";
pub const WASI_ABI_V1_ENTRYPOINT: &str = "run";
pub const WASI_ABI_V1_ALLOCATOR: &str = "insight_alloc";
pub const WASI_ABI_V1_MEMORY: &str = "memory";
pub const WASI_ABI_V1_MAX_MODULE_BYTES: usize = 16 * 1024 * 1024;
pub const WASI_ABI_V1_MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;
pub const WASI_ABI_V1_MAX_TABLE_ELEMENTS: usize = 0;

const PHASE_PREPARING: u8 = 0;
const PHASE_PREPARED: u8 = 1;
const PHASE_RUNNING: u8 = 2;
const PHASE_COLLECTED: u8 = 3;
const WASM_PAGE_BYTES: u64 = 65_536;

pub trait WasiExecutorClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default)]
pub struct SystemWasiExecutorClock;

impl WasiExecutorClock for SystemWasiExecutorClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasiExecutorBackendConfig {
    pub descriptor: InstalledSandboxBackendDescriptor,
    pub worker_process_generation_id: ResourceId,
    pub runtime_version: String,
    pub maximum_module_bytes: usize,
    pub maximum_input_bytes: usize,
    pub maximum_concurrent_executions: usize,
}

impl WasiExecutorBackendConfig {
    pub fn production(
        descriptor: InstalledSandboxBackendDescriptor,
        worker_process_generation_id: ResourceId,
    ) -> Self {
        Self {
            descriptor,
            worker_process_generation_id,
            runtime_version: WASI_ABI_V1_RUNTIME_VERSION.to_owned(),
            maximum_module_bytes: WASI_ABI_V1_MAX_MODULE_BYTES,
            maximum_input_bytes: WASI_ABI_V1_MAX_INPUT_BYTES,
            maximum_concurrent_executions: 32,
        }
    }

    fn validate(&self) -> bool {
        self.descriptor.backend_kind == SandboxIsolationBackendKind::Wasi
            && self.descriptor.isolation_class == SandboxIsolationClass::Wasm
            && self.worker_process_generation_id.kind()
                == insight_platform_contracts::ResourceKind::WorkerProcessGeneration
            && self.runtime_version == WASI_ABI_V1_RUNTIME_VERSION
            && (1..=WASI_ABI_V1_MAX_MODULE_BYTES).contains(&self.maximum_module_bytes)
            && (1..=WASI_ABI_V1_MAX_INPUT_BYTES).contains(&self.maximum_input_bytes)
            && (1..=4_096).contains(&self.maximum_concurrent_executions)
    }
}

pub struct WasmtimeSandboxExecutorBackend {
    config: WasiExecutorBackendConfig,
    artifact_broker: Arc<dyn WasiArtifactBroker>,
    value_validator: Arc<dyn WasiValueValidator>,
    grant_revoker: Arc<dyn WasiGrantRevoker>,
    process_generation_isolation: Arc<dyn SandboxProcessGenerationIsolation>,
    clock: Arc<dyn WasiExecutorClock>,
    permits: Arc<Semaphore>,
    executions: Mutex<BTreeMap<Sha256Digest, Arc<WasiExecution>>>,
}

struct WasiExecution {
    request: SandboxExecutionRequest,
    sandbox_identity_digest: Sha256Digest,
    engine: Engine,
    module: Mutex<Option<Module>>,
    input: Mutex<Option<ScrubbedBytes>>,
    phase: AtomicU8,
    cancelled: AtomicBool,
    guest_active: AtomicBool,
    guest_stopped: Notify,
    _permit: OwnedSemaphorePermit,
}

struct WasiStoreState {
    limits: StoreLimits,
}

struct ScrubbedBytes(Vec<u8>);

impl ScrubbedBytes {
    fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for ScrubbedBytes {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

struct GuestSuccess {
    value: Value,
    usage: SandboxResourceUsage,
}

enum GuestExecution {
    Completed(GuestSuccess),
    Failed {
        code: &'static str,
        message: &'static str,
        resource_violation: Option<&'static str>,
        usage: SandboxResourceUsage,
    },
}

impl WasmtimeSandboxExecutorBackend {
    pub fn new(
        config: WasiExecutorBackendConfig,
        artifact_broker: Arc<dyn WasiArtifactBroker>,
        value_validator: Arc<dyn WasiValueValidator>,
        grant_revoker: Arc<dyn WasiGrantRevoker>,
        process_generation_isolation: Arc<dyn SandboxProcessGenerationIsolation>,
        clock: Arc<dyn WasiExecutorClock>,
    ) -> Result<Self, WasiBackendConfigurationError> {
        if !config.validate() {
            return Err(WasiBackendConfigurationError);
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(config.maximum_concurrent_executions)),
            config,
            artifact_broker,
            value_validator,
            grant_revoker,
            process_generation_isolation,
            clock,
            executions: Mutex::new(BTreeMap::new()),
        })
    }

    fn lookup(
        &self,
        request_digest: &Sha256Digest,
    ) -> Result<Arc<WasiExecution>, SandboxBackendFailure> {
        self.executions
            .lock()
            .map_err(|_| {
                backend_failure(
                    SandboxBackendFailureStage::Running,
                    "sandbox_wasi_state_unavailable",
                    "WASI executor state is unavailable",
                    Retryability::SafeWithinPolicy,
                    None,
                    true,
                )
            })?
            .get(request_digest)
            .cloned()
            .ok_or_else(|| {
                backend_failure(
                    SandboxBackendFailureStage::Running,
                    "sandbox_wasi_execution_not_found",
                    "Exact WASI execution was not found",
                    Retryability::SafeWithinPolicy,
                    None,
                    true,
                )
            })
    }

    fn remove(&self, request_digest: &Sha256Digest) -> Option<Arc<WasiExecution>> {
        self.executions
            .lock()
            .ok()
            .and_then(|mut executions| executions.remove(request_digest))
    }

    async fn revoke(
        &self,
        request: &SandboxExecutionRequest,
        stage: SandboxBackendFailureStage,
        execution_may_have_started: bool,
    ) -> Result<WasiGrantRevocationEvidence, SandboxBackendFailure> {
        self.grant_revoker
            .revoke_exact(RevokeWasiSandboxGrants {
                tenant_id: request.tenant_id.clone(),
                sandbox_job_id: request.sandbox_job_id.clone(),
                request_digest: request.request_digest.clone(),
                worker_process_generation_id: self.config.worker_process_generation_id.clone(),
                attempt_no: request.attempt_no,
                lease_generation: request.lease_generation,
            })
            .await
            .map_err(|error| {
                let retryability = match error {
                    WasiGrantRevocationError::Unavailable => Retryability::SafeWithinPolicy,
                    WasiGrantRevocationError::Rejected => Retryability::Never,
                };
                backend_failure(
                    stage,
                    "sandbox_wasi_grant_revoke_failed",
                    "WASI sandbox grants could not be revoked",
                    retryability,
                    None,
                    execution_may_have_started,
                )
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasiBackendConfigurationError;

#[async_trait]
impl SandboxExecutorBackend for WasmtimeSandboxExecutorBackend {
    fn descriptor(&self) -> InstalledSandboxBackendDescriptor {
        self.config.descriptor.clone()
    }

    async fn prepare(
        &self,
        request: SandboxExecutionRequest,
    ) -> Result<PreparedSandbox, SandboxBackendFailure> {
        validate_wasi_request(&request, &self.config)?;
        let permit = Arc::clone(&self.permits).try_acquire_owned().map_err(|_| {
            backend_failure(
                SandboxBackendFailureStage::Admission,
                "sandbox_wasi_capacity_exhausted",
                "WASI executor capacity is exhausted",
                Retryability::SafeWithinPolicy,
                None,
                false,
            )
        })?;
        let engine = new_engine().map_err(|_| {
            backend_failure(
                SandboxBackendFailureStage::Preparing,
                "sandbox_wasi_engine_unavailable",
                "WASI runtime could not be initialized",
                Retryability::SafeWithinPolicy,
                None,
                false,
            )
        })?;
        let sandbox_identity_digest = evidence_digest(
            "wasi_sandbox_identity",
            &request.request_digest,
            request.attempt_no,
            request.lease_generation,
        );
        let execution = Arc::new(WasiExecution {
            request: request.clone(),
            sandbox_identity_digest: sandbox_identity_digest.clone(),
            engine: engine.clone(),
            module: Mutex::new(None),
            input: Mutex::new(None),
            phase: AtomicU8::new(PHASE_PREPARING),
            cancelled: AtomicBool::new(false),
            guest_active: AtomicBool::new(false),
            guest_stopped: Notify::new(),
            _permit: permit,
        });
        {
            let mut executions = self.executions.lock().map_err(|_| {
                backend_failure(
                    SandboxBackendFailureStage::Preparing,
                    "sandbox_wasi_state_unavailable",
                    "WASI executor state is unavailable",
                    Retryability::SafeWithinPolicy,
                    None,
                    false,
                )
            })?;
            if executions.contains_key(&request.request_digest) {
                return Err(backend_failure(
                    SandboxBackendFailureStage::Admission,
                    "sandbox_wasi_duplicate_execution",
                    "Exact WASI execution is already active",
                    Retryability::SafeWithinPolicy,
                    None,
                    false,
                ));
            }
            executions.insert(request.request_digest.clone(), Arc::clone(&execution));
        }

        let prepared = async {
            let module_bytes = self
                .artifact_broker
                .read_exact(WasiArtifactReadRequest {
                    tenant_id: request.tenant_id.clone(),
                    sandbox_job_id: request.sandbox_job_id.clone(),
                    request_digest: request.request_digest.clone(),
                    worker_process_generation_id: self.config.worker_process_generation_id.clone(),
                    lease_generation: request.lease_generation,
                    artifact: request.package.runtime_bundle_artifact.clone(),
                    purpose: WasiArtifactReadPurpose::RuntimeBundle,
                    read_grant: None,
                    maximum_bytes: self.config.maximum_module_bytes,
                    deadline: request.deadline,
                })
                .await
                .map_err(|error| artifact_failure(error, SandboxBackendFailureStage::Preparing))?;
            verify_artifact_bytes(
                &request.package.runtime_bundle_artifact,
                &module_bytes,
                self.config.maximum_module_bytes,
            )?;
            let input = load_input(
                &request,
                self.artifact_broker.as_ref(),
                self.value_validator.as_ref(),
                &self.config.worker_process_generation_id,
                &self.config,
            )
            .await?;
            if execution.cancelled.load(Ordering::Acquire) {
                return Err(cancelled_prepare_failure());
            }
            let entrypoint = request.package.entrypoint.clone();
            let memory_pages = request
                .resources
                .wasm_memory_pages
                .ok_or_else(|| invalid_contract_failure("WASI memory limit is absent"))?;
            let compiled = tokio::task::spawn_blocking(move || {
                let module = Module::new(&engine, module_bytes)?;
                validate_module_contract(&module, &entrypoint, memory_pages)?;
                Ok::<Module, wasmtime::Error>(module)
            })
            .await
            .map_err(|_| {
                backend_failure(
                    SandboxBackendFailureStage::Preparing,
                    "sandbox_wasi_compiler_panicked",
                    "WASI module compiler did not complete",
                    Retryability::SafeWithinPolicy,
                    Some(sandbox_identity_digest.clone()),
                    false,
                )
            })?
            .map_err(|_| {
                backend_failure(
                    SandboxBackendFailureStage::Preparing,
                    "sandbox_wasi_module_invalid",
                    "WASI module does not satisfy ABI v1",
                    Retryability::Never,
                    Some(sandbox_identity_digest.clone()),
                    false,
                )
            })?;
            if execution.cancelled.load(Ordering::Acquire) {
                return Err(cancelled_prepare_failure());
            }
            *execution.module.lock().map_err(|_| {
                backend_failure(
                    SandboxBackendFailureStage::Preparing,
                    "sandbox_wasi_state_unavailable",
                    "WASI executor state is unavailable",
                    Retryability::SafeWithinPolicy,
                    Some(sandbox_identity_digest.clone()),
                    false,
                )
            })? = Some(compiled);
            *execution.input.lock().map_err(|_| {
                backend_failure(
                    SandboxBackendFailureStage::Preparing,
                    "sandbox_wasi_state_unavailable",
                    "WASI executor state is unavailable",
                    Retryability::SafeWithinPolicy,
                    Some(sandbox_identity_digest.clone()),
                    false,
                )
            })? = Some(ScrubbedBytes(input));
            execution.phase.store(PHASE_PREPARED, Ordering::Release);
            Ok(PreparedSandbox {
                provider_process_generation_id: None,
                sandbox_identity_digest: sandbox_identity_digest.clone(),
                request_digest: request.request_digest.clone(),
                attempt_no: request.attempt_no,
                lease_generation: request.lease_generation,
                prepare_evidence_digest: evidence_digest(
                    "wasi_prepared",
                    &request.request_digest,
                    request.attempt_no,
                    request.lease_generation,
                ),
            })
        }
        .await;
        if prepared.is_err() {
            let revocation = self
                .revoke(&request, SandboxBackendFailureStage::Preparing, false)
                .await;
            self.remove(&request.request_digest);
            revocation?;
        }
        prepared
    }

    async fn start(
        &self,
        request: SandboxExecutionRequest,
        prepared: PreparedSandbox,
    ) -> Result<RunningSandbox, SandboxBackendFailure> {
        let execution = self.lookup(&request.request_digest)?;
        if !matches_execution(&execution, &request, &prepared.sandbox_identity_digest)
            || prepared.request_digest != request.request_digest
            || prepared.attempt_no != request.attempt_no
            || prepared.lease_generation != request.lease_generation
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
            return Err(backend_failure(
                SandboxBackendFailureStage::Starting,
                "sandbox_wasi_stale_start",
                "WASI execution start does not match its prepared generation",
                Retryability::Never,
                Some(prepared.sandbox_identity_digest),
                false,
            ));
        }
        Ok(RunningSandbox {
            prepared,
            start_evidence_digest: evidence_digest(
                "wasi_started",
                &request.request_digest,
                request.attempt_no,
                request.lease_generation,
            ),
        })
    }

    async fn collect(
        &self,
        request: SandboxExecutionRequest,
        running: RunningSandbox,
    ) -> Result<CollectedSandbox, SandboxBackendFailure> {
        let execution = self.lookup(&request.request_digest)?;
        if !matches_execution(
            &execution,
            &request,
            &running.prepared.sandbox_identity_digest,
        ) || execution.phase.load(Ordering::Acquire) != PHASE_RUNNING
        {
            return Err(backend_failure(
                SandboxBackendFailureStage::Collecting,
                "sandbox_wasi_stale_collect",
                "WASI collection does not match its running generation",
                Retryability::Never,
                Some(running.prepared.sandbox_identity_digest),
                true,
            ));
        }
        let module = execution
            .module
            .lock()
            .map_err(|_| state_failure(&execution, SandboxBackendFailureStage::Collecting, true))?
            .clone()
            .ok_or_else(|| {
                state_failure(&execution, SandboxBackendFailureStage::Collecting, true)
            })?;
        let input = execution
            .input
            .lock()
            .map_err(|_| state_failure(&execution, SandboxBackendFailureStage::Collecting, true))?
            .take()
            .ok_or_else(|| {
                state_failure(&execution, SandboxBackendFailureStage::Collecting, true)
            })?;
        execution.guest_active.store(true, Ordering::Release);
        if execution.cancelled.load(Ordering::Acquire) {
            execution.guest_active.store(false, Ordering::Release);
            execution.guest_stopped.notify_one();
            return Err(backend_failure(
                SandboxBackendFailureStage::Running,
                "sandbox_wasi_execution_cancelled",
                "WASI execution was cancelled before guest entry",
                Retryability::SafeWithinPolicy,
                Some(execution.sandbox_identity_digest.clone()),
                true,
            ));
        }
        let execution_for_task = Arc::clone(&execution);
        let request_for_task = request.clone();
        let guest_result = tokio::task::spawn_blocking(move || {
            run_guest(
                &execution_for_task,
                &request_for_task,
                &module,
                input.as_slice(),
            )
        })
        .await;
        execution.guest_active.store(false, Ordering::Release);
        execution.guest_stopped.notify_one();
        let guest = guest_result.map_err(|_| {
            backend_failure(
                SandboxBackendFailureStage::Running,
                "sandbox_wasi_runtime_panicked",
                "WASI runtime did not complete",
                Retryability::SafeWithinPolicy,
                Some(execution.sandbox_identity_digest.clone()),
                true,
            )
        })?;

        let (outcome, usage) = match guest {
            GuestExecution::Completed(success) => {
                let validation_evidence_digest = self
                    .value_validator
                    .validate(WasiValueValidationRequest {
                        tenant_id: request.tenant_id.clone(),
                        sandbox_job_id: request.sandbox_job_id.clone(),
                        request_digest: request.request_digest.clone(),
                        worker_process_generation_id: self
                            .config
                            .worker_process_generation_id
                            .clone(),
                        lease_generation: request.lease_generation,
                        direction: WasiValueDirection::Output,
                        schema_digest: request.output_schema_digest.clone(),
                        classification: request.classification,
                        value: success.value.clone(),
                    })
                    .await
                    .map_err(|error| {
                        let retryability = match error {
                            WasiValueValidationError::Invalid => Retryability::Never,
                            WasiValueValidationError::Unavailable => Retryability::SafeWithinPolicy,
                        };
                        backend_failure(
                            SandboxBackendFailureStage::Collecting,
                            "sandbox_wasi_output_validation_failed",
                            "WASI output did not pass exact schema validation",
                            retryability,
                            Some(execution.sandbox_identity_digest.clone()),
                            true,
                        )
                    })?;
                let content_digest: Sha256Digest = canonical_digest(&success.value)
                    .expect("strict JSON value is canonical")
                    .parse()
                    .expect("canonical digest has SHA-256 shape");
                let usage = success.usage;
                (
                    SandboxExecutionOutcome::Completed(Box::new(SandboxCompletedOutput {
                        value_id: request.output_value_id.clone(),
                        classification: request.classification,
                        schema_digest: request.output_schema_digest.clone(),
                        content_digest,
                        value: ValueRef::Inline {
                            value: success.value,
                        },
                        artifact_link_ids: Vec::new(),
                        artifact_outputs: Vec::new(),
                        validation_evidence_digest,
                        usage: usage.clone(),
                    })),
                    usage,
                )
            }
            GuestExecution::Failed {
                code,
                message,
                resource_violation,
                usage,
            } => (
                SandboxExecutionOutcome::Failed(SafeSandboxFailure {
                    safe_code: code.to_owned(),
                    safe_message: message.to_owned(),
                    retryability: Retryability::Never,
                    evidence_digest: evidence_digest(
                        code,
                        &request.request_digest,
                        request.attempt_no,
                        request.lease_generation,
                    ),
                    resource_violation: resource_violation.map(str::to_owned),
                    external_effect_possible: false,
                }),
                usage,
            ),
        };
        execution.phase.store(PHASE_COLLECTED, Ordering::Release);
        Ok(CollectedSandbox {
            running,
            outcome,
            usage,
            collection_evidence_digest: evidence_digest(
                "wasi_collected",
                &request.request_digest,
                request.attempt_no,
                request.lease_generation,
            ),
        })
    }

    async fn terminate(
        &self,
        command: TerminateSandbox,
    ) -> Result<SandboxTerminationEvidence, SandboxBackendFailure> {
        let execution = self.lookup(&command.request_digest)?;
        if !matches_command(
            &execution,
            &command.tenant_id,
            &command.sandbox_job_id,
            command.attempt_no,
            command.lease_generation,
        ) || execution.sandbox_identity_digest != command.sandbox_identity_digest
        {
            return Err(stale_command_failure(
                SandboxBackendFailureStage::Terminating,
                &execution,
            ));
        }
        execution.cancelled.store(true, Ordering::Release);
        execution.engine.increment_epoch();
        wait_for_guest_stop(&execution).await;
        self.revoke(
            &execution.request,
            SandboxBackendFailureStage::Terminating,
            true,
        )
        .await?;
        Ok(termination_evidence(&execution.request, false))
    }

    async fn destroy(
        &self,
        command: DestroySandbox,
    ) -> Result<SandboxCleanupEvidence, SandboxBackendFailure> {
        let execution = self.lookup(&command.request_digest)?;
        if !matches_command(
            &execution,
            &command.tenant_id,
            &command.sandbox_job_id,
            command.attempt_no,
            command.lease_generation,
        ) || execution.sandbox_identity_digest != command.sandbox_identity_digest
        {
            return Err(stale_command_failure(
                SandboxBackendFailureStage::Destroying,
                &execution,
            ));
        }
        execution.cancelled.store(true, Ordering::Release);
        execution.engine.increment_epoch();
        wait_for_guest_stop(&execution).await;
        self.revoke(
            &execution.request,
            SandboxBackendFailureStage::Destroying,
            true,
        )
        .await?;
        self.remove(&command.request_digest);
        Ok(cleanup_evidence(
            &execution.sandbox_identity_digest,
            self.clock.now(),
            None,
        ))
    }

    async fn abort(
        &self,
        command: AbortSandboxExecution,
    ) -> Result<SandboxAbortEvidence, SandboxBackendFailure> {
        let execution = self.lookup(&command.request_digest)?;
        if !matches_command(
            &execution,
            &command.tenant_id,
            &command.sandbox_job_id,
            command.attempt_no,
            command.lease_generation,
        ) {
            return Err(stale_command_failure(
                SandboxBackendFailureStage::Aborting,
                &execution,
            ));
        }
        execution.cancelled.store(true, Ordering::Release);
        execution.engine.increment_epoch();
        wait_for_guest_stop(&execution).await;
        let revocation = self
            .revoke(
                &execution.request,
                SandboxBackendFailureStage::Aborting,
                true,
            )
            .await?;
        self.remove(&command.request_digest);
        let termination = termination_evidence(&execution.request, false);
        let cleanup = cleanup_evidence(&execution.sandbox_identity_digest, self.clock.now(), None);
        Ok(SandboxAbortEvidence {
            reason: command.reason,
            request_digest: command.request_digest,
            attempt_no: command.attempt_no,
            lease_generation: command.lease_generation,
            sandbox_identity_digest: execution.sandbox_identity_digest.clone(),
            termination,
            cleanup,
            evidence_digest: revocation.evidence_digest,
        })
    }

    async fn recover_expired_lease(
        &self,
        expired: ExpiredSandboxLease,
    ) -> Result<SandboxLeaseRecoveryEvidence, SandboxBackendFailure> {
        let executor_identity_digest =
            expired.executor_identity_digest.clone().ok_or_else(|| {
                backend_failure(
                    SandboxBackendFailureStage::Recovering,
                    "sandbox_wasi_recovery_identity_missing",
                    "Expired WASI execution identity is missing",
                    Retryability::Never,
                    None,
                    true,
                )
            })?;
        let existing = self
            .executions
            .lock()
            .map_err(|_| {
                backend_failure(
                    SandboxBackendFailureStage::Recovering,
                    "sandbox_wasi_state_unavailable",
                    "WASI executor state is unavailable",
                    Retryability::SafeWithinPolicy,
                    None,
                    true,
                )
            })?
            .get(&expired.request.request_digest)
            .cloned();
        if existing.is_some()
            && expired.previous_worker_process_generation_id
                != self.config.worker_process_generation_id
        {
            return Err(backend_failure(
                SandboxBackendFailureStage::Recovering,
                "sandbox_wasi_recovery_generation_mismatch",
                "Live WASI execution belongs to a different process generation",
                Retryability::Never,
                None,
                true,
            ));
        }
        let sandbox_identity_digest = existing
            .as_ref()
            .map(|execution| {
                execution.cancelled.store(true, Ordering::Release);
                execution.engine.increment_epoch();
                execution.sandbox_identity_digest.clone()
            })
            .unwrap_or_else(|| {
                evidence_digest(
                    "wasi_sandbox_identity",
                    &expired.request.request_digest,
                    expired.request.attempt_no,
                    expired.request.lease_generation,
                )
            });
        let process_absence_evidence_digest = if let Some(execution) = existing {
            wait_for_guest_stop(&execution).await;
            self.remove(&expired.request.request_digest);
            None
        } else {
            if expired.previous_worker_process_generation_id
                == self.config.worker_process_generation_id
            {
                return Err(backend_failure(
                    SandboxBackendFailureStage::Recovering,
                    "sandbox_wasi_recovery_local_state_missing",
                    "Current WASI process generation cannot prove a missing execution destroyed",
                    Retryability::Never,
                    Some(sandbox_identity_digest.clone()),
                    true,
                ));
            }
            let proof_request = ProveSandboxProcessGenerationAbsent {
                tenant_id: expired.tenant_id.clone(),
                sandbox_job_id: expired.sandbox_job_id.clone(),
                request_digest: expired.request.request_digest.clone(),
                previous_worker_process_generation_id: expired
                    .previous_worker_process_generation_id
                    .clone(),
                executor_identity_digest: executor_identity_digest.clone(),
                attestor_route: expired.attestor_route.clone().ok_or_else(|| {
                    backend_failure(
                        SandboxBackendFailureStage::Recovering,
                        "sandbox_wasi_recovery_attestor_route_missing",
                        "Expired WASI execution has no attestor route",
                        Retryability::Never,
                        Some(sandbox_identity_digest.clone()),
                        true,
                    )
                })?,
            };
            let proof = self
                .process_generation_isolation
                .prove_absent(proof_request.clone())
                .await
                .map_err(process_generation_isolation_failure)?;
            if proof
                .validate_for(&proof_request, self.clock.now())
                .is_err()
            {
                return Err(backend_failure(
                    SandboxBackendFailureStage::Recovering,
                    "sandbox_wasi_process_absence_evidence_invalid",
                    "WASI process-generation absence evidence is invalid",
                    Retryability::Never,
                    Some(sandbox_identity_digest.clone()),
                    true,
                ));
            }
            Some(proof.evidence_digest)
        };
        self.revoke(
            &expired.request,
            SandboxBackendFailureStage::Recovering,
            true,
        )
        .await?;
        let cleanup = cleanup_evidence(
            &sandbox_identity_digest,
            self.clock.now(),
            process_absence_evidence_digest.as_ref(),
        );
        SandboxLeaseRecoveryEvidence {
            schema_version: 1,
            request_digest: expired.request.request_digest.clone(),
            attempt_no: expired.request.attempt_no,
            lease_generation: expired.observed_lease_generation,
            physical_state: expired.physical_state,
            executor_identity_digest,
            uncertainty: SandboxUncertainty {
                evidence_digest: evidence_digest(
                    "wasi_recovery_uncertainty",
                    &expired.request.request_digest,
                    expired.request.attempt_no,
                    expired.observed_lease_generation,
                ),
                sandbox_identity_digest,
                execution_may_have_started: true,
                external_effect_possible: false,
                manual_reconciliation_required: false,
            },
            cleanup,
            evidence_digest: zero_digest(),
        }
        .seal()
        .map_err(|_| {
            backend_failure(
                SandboxBackendFailureStage::Recovering,
                "sandbox_wasi_recovery_evidence_invalid",
                "WASI recovery evidence could not be sealed",
                Retryability::Never,
                None,
                true,
            )
        })
    }
}

async fn wait_for_guest_stop(execution: &WasiExecution) {
    while execution.guest_active.load(Ordering::Acquire) {
        execution.guest_stopped.notified().await;
    }
}

fn process_generation_isolation_failure(
    error: SandboxProcessGenerationIsolationError,
) -> SandboxBackendFailure {
    let (code, message, retryability) = match error {
        SandboxProcessGenerationIsolationError::StillLive => (
            "sandbox_wasi_previous_generation_still_live",
            "Previous WASI process generation is still live",
            Retryability::SafeWithinPolicy,
        ),
        SandboxProcessGenerationIsolationError::Unavailable => (
            "sandbox_wasi_process_isolation_unavailable",
            "WASI process-generation isolation authority is unavailable",
            Retryability::SafeWithinPolicy,
        ),
        SandboxProcessGenerationIsolationError::Rejected => (
            "sandbox_wasi_process_isolation_rejected",
            "WASI process-generation isolation proof was rejected",
            Retryability::Never,
        ),
    };
    backend_failure(
        SandboxBackendFailureStage::Recovering,
        code,
        message,
        retryability,
        None,
        true,
    )
}

async fn load_input(
    request: &SandboxExecutionRequest,
    broker: &dyn WasiArtifactBroker,
    validator: &dyn WasiValueValidator,
    worker_process_generation_id: &ResourceId,
    config: &WasiExecutorBackendConfig,
) -> Result<Vec<u8>, SandboxBackendFailure> {
    let value = match &request.input_ref {
        ValueRef::Inline { value } => value.clone(),
        ValueRef::Artifact { artifact } => {
            let grant = request
                .artifact_grants
                .iter()
                .find(|grant| {
                    grant.operation == ArtifactGrantOperation::ReadWhole
                        && grant.artifact.as_ref() == Some(artifact)
                })
                .cloned()
                .ok_or_else(|| invalid_contract_failure("WASI input Artifact grant is absent"))?;
            let bytes = broker
                .read_exact(WasiArtifactReadRequest {
                    tenant_id: request.tenant_id.clone(),
                    sandbox_job_id: request.sandbox_job_id.clone(),
                    request_digest: request.request_digest.clone(),
                    worker_process_generation_id: worker_process_generation_id.clone(),
                    lease_generation: request.lease_generation,
                    artifact: artifact.clone(),
                    purpose: WasiArtifactReadPurpose::InputValue,
                    read_grant: Some(grant),
                    maximum_bytes: config.maximum_input_bytes,
                    deadline: request.deadline,
                })
                .await
                .map_err(|error| artifact_failure(error, SandboxBackendFailureStage::Preparing))?;
            verify_artifact_bytes(artifact, &bytes, config.maximum_input_bytes)?;
            parse_strict_json(&bytes, json_limits(config.maximum_input_bytes)).map_err(|_| {
                backend_failure(
                    SandboxBackendFailureStage::Preparing,
                    "sandbox_wasi_input_invalid",
                    "WASI input Artifact is not strict JSON",
                    Retryability::Never,
                    None,
                    false,
                )
            })?
        }
    };
    validator
        .validate(WasiValueValidationRequest {
            tenant_id: request.tenant_id.clone(),
            sandbox_job_id: request.sandbox_job_id.clone(),
            request_digest: request.request_digest.clone(),
            worker_process_generation_id: worker_process_generation_id.clone(),
            lease_generation: request.lease_generation,
            direction: WasiValueDirection::Input,
            schema_digest: request.input_schema_digest.clone(),
            classification: request.classification,
            value: value.clone(),
        })
        .await
        .map_err(|error| {
            let retryability = match error {
                WasiValueValidationError::Invalid => Retryability::Never,
                WasiValueValidationError::Unavailable => Retryability::SafeWithinPolicy,
            };
            backend_failure(
                SandboxBackendFailureStage::Preparing,
                "sandbox_wasi_input_validation_failed",
                "WASI input did not pass exact schema validation",
                retryability,
                None,
                false,
            )
        })?;
    let envelope = serde_json::json!({
        "schema_version": 1,
        "request_digest": request.request_digest,
        "input_value_id": request.input_value_id,
        "input_schema_digest": request.input_schema_digest,
        "value": value,
    });
    let bytes = canonical_json(&envelope)
        .map_err(|_| invalid_contract_failure("WASI input could not be canonicalized"))?;
    if bytes.len() > config.maximum_input_bytes {
        return Err(backend_failure(
            SandboxBackendFailureStage::Preparing,
            "sandbox_wasi_input_too_large",
            "WASI input exceeds its hard byte limit",
            Retryability::Never,
            None,
            false,
        ));
    }
    Ok(bytes)
}

fn validate_wasi_request(
    request: &SandboxExecutionRequest,
    config: &WasiExecutorBackendConfig,
) -> Result<(), SandboxBackendFailure> {
    let least_privilege_input_grant = match &request.input_ref {
        ValueRef::Inline { .. } => request.artifact_grants.is_empty(),
        ValueRef::Artifact { artifact } => {
            request.artifact_grants.len() == 1
                && request.artifact_grants.iter().all(|grant| {
                    grant.operation == ArtifactGrantOperation::ReadWhole
                        && grant.artifact.as_ref() == Some(artifact)
                })
        }
    };
    if request.protocol_version != SandboxAbiVersion::V1
        || request.runtime.runtime_family != SandboxRuntimeFamily::WasmWasi
        || request.runtime.runtime_version != config.runtime_version
        || request.package.entrypoint_kind != SandboxEntrypointKind::WasmExport
        || request.package.entrypoint != WASI_ABI_V1_ENTRYPOINT
        || request.isolation_class != SandboxIsolationClass::Wasm
        || request.network_mode != SandboxNetworkMode::None
        || !request.secret_grants.is_empty()
        || !least_privilege_input_grant
        || request.effect.risk_rank() >= Effect::IdempotentWrite.risk_rank()
        || request.package.runtime_bundle_artifact.byte_length()
            > u64::try_from(config.maximum_module_bytes).unwrap_or(u64::MAX)
    {
        return Err(invalid_contract_failure(
            "Sandbox request is not eligible for WASI ABI v1",
        ));
    }
    Ok(())
}

fn new_engine() -> Result<Engine, wasmtime::Error> {
    let mut config = Config::new();
    config
        .consume_fuel(true)
        .epoch_interruption(true)
        .wasm_features(WasmFeatures::all(), false)
        .wasm_features(
            WasmFeatures::FLOATS.union(WasmFeatures::MUTABLE_GLOBAL),
            true,
        );
    Engine::new(&config)
}

fn validate_module_contract(
    module: &Module,
    entrypoint: &str,
    memory_pages: u32,
) -> Result<(), wasmtime::Error> {
    if entrypoint != WASI_ABI_V1_ENTRYPOINT || module.imports().next().is_some() {
        return Err(wasmtime::format_err!(
            "WASI ABI v1 import or entrypoint violation"
        ));
    }
    let resources = module.resources_required();
    if resources.num_memories != 1 || resources.num_tables != 0 {
        return Err(wasmtime::format_err!(
            "WASI ABI v1 resource shape violation"
        ));
    }
    let exports = module.exports().collect::<Vec<_>>();
    let names = exports
        .iter()
        .map(|export| export.name())
        .collect::<BTreeSet<_>>();
    if exports.len() != 3
        || names
            != BTreeSet::from([
                WASI_ABI_V1_MEMORY,
                WASI_ABI_V1_ALLOCATOR,
                WASI_ABI_V1_ENTRYPOINT,
            ])
    {
        return Err(wasmtime::format_err!("WASI ABI v1 export set violation"));
    }
    for export in exports {
        match (export.name(), export.ty()) {
            (WASI_ABI_V1_MEMORY, ExternType::Memory(memory))
                if !memory.is_64()
                    && !memory.is_shared()
                    && memory.minimum() <= u64::from(memory_pages)
                    && memory
                        .maximum()
                        .is_some_and(|max| max <= u64::from(memory_pages)) => {}
            (WASI_ABI_V1_ALLOCATOR, ExternType::Func(function)) if is_allocator_type(&function) => {
            }
            (WASI_ABI_V1_ENTRYPOINT, ExternType::Func(function))
                if is_entrypoint_type(&function) => {}
            _ => return Err(wasmtime::format_err!("WASI ABI v1 export type violation")),
        }
    }
    Ok(())
}

fn is_allocator_type(function: &wasmtime::FuncType) -> bool {
    let mut params = function.params();
    let mut results = function.results();
    matches!(params.next(), Some(ValType::I32))
        && params.next().is_none()
        && matches!(results.next(), Some(ValType::I32))
        && results.next().is_none()
}

fn is_entrypoint_type(function: &wasmtime::FuncType) -> bool {
    let mut params = function.params();
    let mut results = function.results();
    matches!(params.next(), Some(ValType::I32))
        && matches!(params.next(), Some(ValType::I32))
        && params.next().is_none()
        && matches!(results.next(), Some(ValType::I64))
        && results.next().is_none()
}

fn run_guest(
    execution: &WasiExecution,
    request: &SandboxExecutionRequest,
    module: &Module,
    input: &[u8],
) -> GuestExecution {
    let started = Instant::now();
    if u64::try_from(input.len()).unwrap_or(u64::MAX) > request.resources.io_bytes {
        return guest_failure_with_usage(
            "sandbox_wasi_io_exhausted",
            "WASI execution exhausted its I/O byte limit",
            Some("io_bytes"),
            request,
            started,
            0,
            request.resources.io_bytes,
            0,
            0,
        );
    }
    let maximum_pages = request.resources.wasm_memory_pages.unwrap_or(1);
    let memory_bytes = usize::try_from(u64::from(maximum_pages).saturating_mul(WASM_PAGE_BYTES))
        .unwrap_or(usize::MAX);
    let limits = StoreLimitsBuilder::new()
        .memory_size(memory_bytes)
        .table_elements(WASI_ABI_V1_MAX_TABLE_ELEMENTS)
        .instances(1)
        .tables(0)
        .memories(1)
        .trap_on_grow_failure(true)
        .build();
    let mut store = Store::new(&execution.engine, WasiStoreState { limits });
    store.limiter(|state| &mut state.limits);
    let fuel = request.resources.wasm_fuel.unwrap_or(1);
    if store.set_fuel(fuel).is_err() {
        return guest_failure(
            "sandbox_wasi_runtime_failed",
            "WASI runtime could not apply its fuel limit",
            None,
            request,
            started,
            0,
            0,
        );
    }
    store.set_epoch_deadline(1);
    store.epoch_deadline_trap();
    let instance = match Instance::new(&mut store, module, &[]) {
        Ok(instance) => instance,
        Err(error) => return guest_trap(error, request, &store, started, 0, 0),
    };
    let memory = match instance.get_memory(&mut store, WASI_ABI_V1_MEMORY) {
        Some(memory) => memory,
        None => {
            return guest_failure(
                "sandbox_wasi_abi_violation",
                "WASI guest memory export is unavailable",
                None,
                request,
                started,
                0,
                0,
            )
        }
    };
    let allocator = match instance.get_typed_func::<i32, i32>(&mut store, WASI_ABI_V1_ALLOCATOR) {
        Ok(function) => function,
        Err(error) => return guest_trap(error, request, &store, started, 0, 0),
    };
    let entrypoint =
        match instance.get_typed_func::<(i32, i32), i64>(&mut store, WASI_ABI_V1_ENTRYPOINT) {
            Ok(function) => function,
            Err(error) => return guest_trap(error, request, &store, started, 0, 0),
        };
    let input_len = match i32::try_from(input.len()) {
        Ok(length) => length,
        Err(_) => {
            return guest_failure(
                "sandbox_wasi_input_too_large",
                "WASI input exceeds the ABI pointer width",
                Some("result_bytes"),
                request,
                started,
                memory.data_size(&store),
                0,
            )
        }
    };
    let input_pointer = match allocator.call(&mut store, input_len) {
        Ok(pointer) => pointer as u32 as usize,
        Err(error) => {
            return guest_trap(error, request, &store, started, memory.data_size(&store), 0)
        }
    };
    if memory.write(&mut store, input_pointer, input).is_err() {
        return guest_failure(
            "sandbox_wasi_input_pointer_invalid",
            "WASI guest returned an invalid input pointer",
            None,
            request,
            started,
            memory.data_size(&store),
            0,
        );
    }
    let packed = match entrypoint.call(&mut store, (input_pointer as i32, input_len)) {
        Ok(packed) => packed as u64,
        Err(error) => {
            let memory_size = memory.data_size(&store);
            scrub_guest_memory(&memory, &mut store, input_pointer, input.len());
            return guest_trap(error, request, &store, started, memory_size, 0);
        }
    };
    let output_pointer = usize::try_from(packed >> 32).unwrap_or(usize::MAX);
    let output_length = usize::try_from(packed & u64::from(u32::MAX)).unwrap_or(usize::MAX);
    if output_length == 0
        || output_length > usize::try_from(request.resources.result_bytes).unwrap_or(usize::MAX)
    {
        let memory_size = memory.data_size(&store);
        scrub_guest_memory(&memory, &mut store, input_pointer, input.len());
        return guest_failure(
            "sandbox_wasi_result_too_large",
            "WASI result exceeds its hard byte limit",
            Some("result_bytes"),
            request,
            started,
            memory_size,
            0,
        );
    }
    let total_io_bytes = input.len().saturating_add(output_length);
    if u64::try_from(total_io_bytes).unwrap_or(u64::MAX) > request.resources.io_bytes {
        let memory_size = memory.data_size(&store);
        scrub_guest_memory(&memory, &mut store, input_pointer, input.len());
        scrub_guest_memory(&memory, &mut store, output_pointer, output_length);
        return guest_failure_with_usage(
            "sandbox_wasi_io_exhausted",
            "WASI execution exhausted its I/O byte limit",
            Some("io_bytes"),
            request,
            started,
            memory_size,
            request.resources.io_bytes,
            output_length,
            fuel.saturating_sub(store.get_fuel().unwrap_or(0)),
        );
    }
    let mut output = vec![0_u8; output_length];
    if memory.read(&store, output_pointer, &mut output).is_err() {
        let memory_size = memory.data_size(&store);
        scrub_guest_memory(&memory, &mut store, input_pointer, input.len());
        return guest_failure(
            "sandbox_wasi_result_pointer_invalid",
            "WASI guest returned an invalid result pointer",
            None,
            request,
            started,
            memory_size,
            output_length,
        );
    }
    let memory_size = memory.data_size(&store);
    scrub_guest_memory(&memory, &mut store, input_pointer, input.len());
    scrub_guest_memory(&memory, &mut store, output_pointer, output.len());
    let envelope = match parse_strict_json(&output, json_limits(output_length)) {
        Ok(value) => value,
        Err(_) => {
            output.fill(0);
            return guest_failure(
                "sandbox_wasi_result_invalid",
                "WASI result is not strict JSON",
                None,
                request,
                started,
                memory_size,
                output_length,
            );
        }
    };
    if canonical_json(&envelope).as_deref() != Ok(output.as_slice()) {
        output.fill(0);
        return guest_failure(
            "sandbox_wasi_result_noncanonical",
            "WASI result is not canonical JSON",
            None,
            request,
            started,
            memory_size,
            output_length,
        );
    }
    output.fill(0);
    let output = match serde_json::from_value::<WasiAbiV1Output>(envelope) {
        Ok(output) if output.schema_version == 1 => output,
        _ => {
            return guest_failure(
                "sandbox_wasi_result_invalid",
                "WASI result envelope does not satisfy ABI v1",
                None,
                request,
                started,
                memory_size,
                output_length,
            )
        }
    };
    let usage = resource_usage(
        request,
        started,
        memory_size,
        total_io_bytes,
        output_length,
        fuel.saturating_sub(store.get_fuel().unwrap_or(0)),
    );
    GuestExecution::Completed(GuestSuccess {
        value: output.value,
        usage,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WasiAbiV1Output {
    schema_version: u32,
    value: Value,
}

fn guest_trap(
    error: wasmtime::Error,
    request: &SandboxExecutionRequest,
    store: &Store<WasiStoreState>,
    started: Instant,
    memory_size: usize,
    result_bytes: usize,
) -> GuestExecution {
    let fuel_consumed = request
        .resources
        .wasm_fuel
        .unwrap_or(0)
        .saturating_sub(store.get_fuel().unwrap_or(0));
    if error.downcast_ref::<Trap>() == Some(&Trap::OutOfFuel) {
        return guest_failure_with_fuel(
            "sandbox_wasi_fuel_exhausted",
            "WASI execution exhausted its fuel limit",
            Some("wasm_fuel"),
            request,
            started,
            memory_size,
            result_bytes,
            fuel_consumed,
        );
    }
    guest_failure_with_fuel(
        "sandbox_wasi_guest_trapped",
        "WASI guest execution trapped",
        None,
        request,
        started,
        memory_size,
        result_bytes,
        fuel_consumed,
    )
}

fn guest_failure(
    code: &'static str,
    message: &'static str,
    resource_violation: Option<&'static str>,
    request: &SandboxExecutionRequest,
    started: Instant,
    memory_size: usize,
    result_bytes: usize,
) -> GuestExecution {
    guest_failure_with_fuel(
        code,
        message,
        resource_violation,
        request,
        started,
        memory_size,
        result_bytes,
        request.resources.wasm_fuel.unwrap_or(0),
    )
}

#[allow(clippy::too_many_arguments)]
fn guest_failure_with_fuel(
    code: &'static str,
    message: &'static str,
    resource_violation: Option<&'static str>,
    request: &SandboxExecutionRequest,
    started: Instant,
    memory_size: usize,
    result_bytes: usize,
    fuel_consumed: u64,
) -> GuestExecution {
    guest_failure_with_usage(
        code,
        message,
        resource_violation,
        request,
        started,
        memory_size,
        u64::try_from(result_bytes).unwrap_or(u64::MAX),
        result_bytes,
        fuel_consumed,
    )
}

#[allow(clippy::too_many_arguments)]
fn guest_failure_with_usage(
    code: &'static str,
    message: &'static str,
    resource_violation: Option<&'static str>,
    request: &SandboxExecutionRequest,
    started: Instant,
    memory_size: usize,
    io_bytes: u64,
    result_bytes: usize,
    fuel_consumed: u64,
) -> GuestExecution {
    GuestExecution::Failed {
        code,
        message,
        resource_violation,
        usage: resource_usage(
            request,
            started,
            memory_size,
            usize::try_from(io_bytes).unwrap_or(usize::MAX),
            result_bytes,
            fuel_consumed,
        ),
    }
}

fn resource_usage(
    request: &SandboxExecutionRequest,
    started: Instant,
    memory_size: usize,
    io_bytes: usize,
    result_bytes: usize,
    fuel_consumed: u64,
) -> SandboxResourceUsage {
    let wall_milliseconds = u64::try_from(started.elapsed().as_millis())
        .unwrap_or(u64::MAX)
        .max(1)
        .min(request.resources.wall_milliseconds);
    let peak_memory_mebibytes = u32::try_from(memory_size.div_ceil(1024 * 1024))
        .unwrap_or(u32::MAX)
        .min(request.resources.memory_mebibytes);
    SandboxResourceUsage {
        cpu_milliseconds: wall_milliseconds,
        peak_memory_mebibytes,
        peak_pids: 1,
        files_created: 0,
        io_bytes: u64::try_from(io_bytes)
            .unwrap_or(u64::MAX)
            .min(request.resources.io_bytes),
        stdout_bytes: 0,
        stderr_bytes: 0,
        result_bytes: u64::try_from(result_bytes)
            .unwrap_or(u64::MAX)
            .min(request.resources.result_bytes),
        artifact_output_bytes: 0,
        network_connections: 0,
        network_request_bytes: 0,
        network_response_bytes: 0,
        wall_milliseconds,
        wasm_fuel_consumed: Some(
            fuel_consumed.min(request.resources.wasm_fuel.unwrap_or(fuel_consumed)),
        ),
    }
}

fn scrub_guest_memory(
    memory: &wasmtime::Memory,
    store: &mut Store<WasiStoreState>,
    at: usize,
    len: usize,
) {
    if len == 0 {
        return;
    }
    let zeros = vec![0_u8; len];
    let _ = memory.write(store, at, &zeros);
}

fn verify_artifact_bytes(
    artifact: &ArtifactRef,
    bytes: &[u8],
    maximum: usize,
) -> Result<(), SandboxBackendFailure> {
    if bytes.len() > maximum
        || u64::try_from(bytes.len()).ok() != Some(artifact.byte_length())
        || sha256_digest(bytes) != *artifact.content_digest()
    {
        return Err(backend_failure(
            SandboxBackendFailureStage::Preparing,
            "sandbox_wasi_artifact_integrity_failed",
            "WASI Artifact failed exact length or digest verification",
            Retryability::Never,
            None,
            false,
        ));
    }
    Ok(())
}

fn sha256_digest(bytes: &[u8]) -> Sha256Digest {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
        .parse()
        .expect("SHA-256 formatter produces a valid digest")
}

fn json_limits(maximum: usize) -> JsonLimits {
    JsonLimits {
        max_bytes: maximum.max(1),
        max_depth: 128,
        max_properties_per_object: 100_000,
        max_items_per_array: 100_000,
        max_string_bytes: maximum.max(1),
    }
}

fn matches_execution(
    execution: &WasiExecution,
    request: &SandboxExecutionRequest,
    sandbox_identity_digest: &Sha256Digest,
) -> bool {
    execution.request.request_digest == request.request_digest
        && execution.request.tenant_id == request.tenant_id
        && execution.request.sandbox_job_id == request.sandbox_job_id
        && execution.request.attempt_no == request.attempt_no
        && execution.request.lease_generation == request.lease_generation
        && &execution.sandbox_identity_digest == sandbox_identity_digest
}

fn matches_command(
    execution: &WasiExecution,
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

fn termination_evidence(
    request: &SandboxExecutionRequest,
    guest_acknowledged: bool,
) -> SandboxTerminationEvidence {
    SandboxTerminationEvidence {
        evidence_digest: evidence_digest(
            "wasi_terminated",
            &request.request_digest,
            request.attempt_no,
            request.lease_generation,
        ),
        guest_acknowledged,
        process_tree_terminated: true,
        grants_revoked: true,
        external_effect_possible: false,
    }
}

fn cleanup_evidence(
    sandbox_identity_digest: &Sha256Digest,
    observed_at: DateTime<Utc>,
    process_absence_evidence_digest: Option<&Sha256Digest>,
) -> SandboxCleanupEvidence {
    let evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "kind": "wasi_destroyed",
        "sandbox_identity_digest": sandbox_identity_digest,
        "grants_revoked": true,
        "ephemeral_storage_destroyed": true,
        "observed_at": observed_at,
        "process_absence_evidence_digest": process_absence_evidence_digest,
    }))
    .expect("closed WASI cleanup evidence is canonical")
    .parse()
    .expect("canonical digest has SHA-256 shape");
    SandboxCleanupEvidence {
        disposition: SandboxCleanupDisposition::Destroyed,
        sandbox_identity_digest: sandbox_identity_digest.clone(),
        grants_revoked: true,
        ephemeral_storage_destroyed: true,
        observed_at,
        evidence_digest,
    }
}

fn evidence_digest(
    kind: &str,
    request_digest: &Sha256Digest,
    attempt_no: u32,
    lease_generation: u64,
) -> Sha256Digest {
    canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "kind": kind,
        "request_digest": request_digest,
        "attempt_no": attempt_no,
        "lease_generation": lease_generation,
    }))
    .expect("closed WASI evidence is canonical")
    .parse()
    .expect("canonical digest has SHA-256 shape")
}

fn zero_digest() -> Sha256Digest {
    format!("sha256:{}", "0".repeat(64))
        .parse()
        .expect("zero SHA-256 digest has valid shape")
}

fn backend_failure(
    stage: SandboxBackendFailureStage,
    code: &str,
    message: &str,
    retryability: Retryability,
    sandbox_identity_digest: Option<Sha256Digest>,
    execution_may_have_started: bool,
) -> SandboxBackendFailure {
    SandboxBackendFailure {
        stage,
        safe_code: code.to_owned(),
        safe_message: message.to_owned(),
        retryability,
        evidence_digest: canonical_digest(&serde_json::json!({
            "schema_version": 1,
            "stage": stage,
            "code": code,
        }))
        .expect("closed WASI failure evidence is canonical")
        .parse()
        .expect("canonical digest has SHA-256 shape"),
        sandbox_identity_digest,
        execution_may_have_started,
        external_effect_possible: false,
    }
}

fn invalid_contract_failure(message: &str) -> SandboxBackendFailure {
    backend_failure(
        SandboxBackendFailureStage::Admission,
        "sandbox_wasi_contract_invalid",
        message,
        Retryability::Never,
        None,
        false,
    )
}

fn cancelled_prepare_failure() -> SandboxBackendFailure {
    backend_failure(
        SandboxBackendFailureStage::Preparing,
        "sandbox_wasi_prepare_cancelled",
        "WASI preparation was cancelled",
        Retryability::SafeWithinPolicy,
        None,
        false,
    )
}

fn artifact_failure(
    error: WasiArtifactBrokerError,
    stage: SandboxBackendFailureStage,
) -> SandboxBackendFailure {
    let (code, retryability) = match error {
        WasiArtifactBrokerError::Unavailable => (
            "sandbox_wasi_artifact_unavailable",
            Retryability::SafeWithinPolicy,
        ),
        WasiArtifactBrokerError::Denied => ("sandbox_wasi_artifact_denied", Retryability::Never),
        WasiArtifactBrokerError::NotFound => {
            ("sandbox_wasi_artifact_not_found", Retryability::Never)
        }
        WasiArtifactBrokerError::Integrity => (
            "sandbox_wasi_artifact_integrity_failed",
            Retryability::Never,
        ),
        WasiArtifactBrokerError::TooLarge => {
            ("sandbox_wasi_artifact_too_large", Retryability::Never)
        }
    };
    backend_failure(
        stage,
        code,
        "WASI Artifact could not be materialized",
        retryability,
        None,
        false,
    )
}

fn state_failure(
    execution: &WasiExecution,
    stage: SandboxBackendFailureStage,
    execution_may_have_started: bool,
) -> SandboxBackendFailure {
    backend_failure(
        stage,
        "sandbox_wasi_state_unavailable",
        "WASI executor state is unavailable",
        Retryability::SafeWithinPolicy,
        Some(execution.sandbox_identity_digest.clone()),
        execution_may_have_started,
    )
}

fn stale_command_failure(
    stage: SandboxBackendFailureStage,
    execution: &WasiExecution,
) -> SandboxBackendFailure {
    backend_failure(
        stage,
        "sandbox_wasi_stale_generation",
        "WASI control command does not match the active generation",
        Retryability::Never,
        Some(execution.sandbox_identity_digest.clone()),
        true,
    )
}

#[cfg(test)]
mod tests;
