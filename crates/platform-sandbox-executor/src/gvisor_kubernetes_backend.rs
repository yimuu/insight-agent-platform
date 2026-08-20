use crate::{
    GvisorCleanupReceipt, GvisorExecutorBackendConfig, KubernetesGvisorError,
    KubernetesGvisorPodRuntime, LaunchedGvisorPod, ObservedGvisorPodExit,
};
use async_trait::async_trait;
use chrono::Utc;
use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, Effect, JsonLimits, ResourceId,
    ResourceKind, Retryability, SandboxEntrypointKind, SandboxIsolationClass, SandboxRuntimeFamily,
    Sha256Digest, ValueRef,
};
use insight_platform_sandbox::{
    AbortSandboxExecution, CollectedSandbox, DestroySandbox, ExpiredSandboxLease,
    InstalledSandboxBackendDescriptor, PreparedSandbox, RevokeWasiSandboxGrants, RunningSandbox,
    SandboxAbortEvidence, SandboxBackendFailure, SandboxBackendFailureStage,
    SandboxCleanupDisposition, SandboxCleanupEvidence, SandboxCompletedOutput,
    SandboxExecutionOutcome, SandboxExecutionRequest, SandboxExecutorBackend,
    SandboxIsolationBackendKind, SandboxLeaseRecoveryEvidence, SandboxNetworkMode,
    SandboxResourceUsage, SandboxTerminationEvidence, SandboxUncertainty, TerminateSandbox,
    WasiGrantRevocationError, WasiGrantRevoker, WasiValueDirection, WasiValueValidationError,
    WasiValueValidationRequest, WasiValueValidator,
};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const PHASE_PREPARING: u8 = 0;
const PHASE_PREPARED: u8 = 1;
const PHASE_RUNNING: u8 = 2;
const PHASE_COLLECTED: u8 = 3;
const MAX_GUEST_RESULT_ENVELOPE_BYTES: usize = 4_096;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GvisorGuestResultEnvelope {
    schema_version: u32,
    value: serde_json::Value,
    usage: SandboxResourceUsage,
}

#[async_trait]
pub trait KubernetesGvisorResultBroker: Send + Sync {
    async fn collect(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
        pod: &LaunchedGvisorPod,
        observed: ObservedGvisorPodExit,
    ) -> Result<KubernetesGvisorCollectedOutput, KubernetesGvisorError>;

    async fn revoke_and_cleanup(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
        physical_cleanup_evidence_digest: &Sha256Digest,
    ) -> Result<GvisorCleanupReceipt, KubernetesGvisorError>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct KubernetesGvisorCollectedOutput {
    pub outcome: SandboxExecutionOutcome,
    pub usage: SandboxResourceUsage,
    pub collection_evidence_digest: Sha256Digest,
}

pub struct GrpcKubernetesGvisorResultBroker {
    maximum_result_bytes: usize,
    value_validator: Arc<dyn WasiValueValidator>,
    grant_revoker: Arc<dyn WasiGrantRevoker>,
}

impl GrpcKubernetesGvisorResultBroker {
    pub fn new(
        maximum_result_bytes: usize,
        value_validator: Arc<dyn WasiValueValidator>,
        grant_revoker: Arc<dyn WasiGrantRevoker>,
    ) -> Result<Self, KubernetesGvisorError> {
        if maximum_result_bytes == 0 {
            return Err(KubernetesGvisorError::InvalidContract);
        }
        Ok(Self {
            maximum_result_bytes,
            value_validator,
            grant_revoker,
        })
    }
}

#[async_trait]
impl KubernetesGvisorResultBroker for GrpcKubernetesGvisorResultBroker {
    async fn collect(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
        pod: &LaunchedGvisorPod,
        observed: ObservedGvisorPodExit,
    ) -> Result<KubernetesGvisorCollectedOutput, KubernetesGvisorError> {
        if observed.exit_code != 0
            || observed.termination_message.is_empty()
            || observed.termination_message.len() > MAX_GUEST_RESULT_ENVELOPE_BYTES
        {
            return Err(KubernetesGvisorError::Integrity);
        }
        let value = parse_strict_json(
            &observed.termination_message,
            JsonLimits {
                max_bytes: MAX_GUEST_RESULT_ENVELOPE_BYTES,
                max_depth: 32,
                max_items_per_array: 256,
                max_properties_per_object: 256,
                max_string_bytes: self.maximum_result_bytes.min(4_096),
            },
        )
        .map_err(|_| KubernetesGvisorError::Integrity)?;
        if canonical_json(&value).as_deref() != Ok(observed.termination_message.as_slice()) {
            return Err(KubernetesGvisorError::Integrity);
        }
        let envelope: GvisorGuestResultEnvelope =
            serde_json::from_value(value).map_err(|_| KubernetesGvisorError::Integrity)?;
        let value_bytes =
            canonical_json(&envelope.value).map_err(|_| KubernetesGvisorError::Integrity)?;
        if envelope.schema_version != 1
            || value_bytes.len() > self.maximum_result_bytes
            || u64::try_from(value_bytes.len())
                .map_or(true, |bytes| bytes > request.resources.result_bytes)
        {
            return Err(KubernetesGvisorError::Integrity);
        }
        envelope
            .usage
            .validate_for(request)
            .map_err(|_| KubernetesGvisorError::Integrity)?;
        let validation_evidence_digest = self
            .value_validator
            .validate(WasiValueValidationRequest {
                tenant_id: request.tenant_id.clone(),
                sandbox_job_id: request.sandbox_job_id.clone(),
                request_digest: request.request_digest.clone(),
                worker_process_generation_id: worker_process_generation_id.clone(),
                lease_generation: request.lease_generation,
                direction: WasiValueDirection::Output,
                schema_digest: request.output_schema_digest.clone(),
                classification: request.classification,
                value: envelope.value.clone(),
            })
            .await
            .map_err(|error| match error {
                WasiValueValidationError::Invalid => KubernetesGvisorError::Integrity,
                WasiValueValidationError::Unavailable => KubernetesGvisorError::Unavailable,
            })?;
        let content_digest = canonical_digest(&envelope.value)
            .map_err(|_| KubernetesGvisorError::Integrity)?
            .parse()
            .map_err(|_| KubernetesGvisorError::Integrity)?;
        let collection_evidence_digest = canonical_digest(&serde_json::json!({
            "schema_version": 1,
            "kind": "gvisor_pod_result_collected",
            "request_digest": request.request_digest,
            "pod_uid": pod.uid,
            "observation_evidence_digest": observed.observation_evidence_digest,
            "content_digest": content_digest,
            "validation_evidence_digest": validation_evidence_digest,
            "usage": envelope.usage,
        }))
        .map_err(|_| KubernetesGvisorError::Integrity)?
        .parse()
        .map_err(|_| KubernetesGvisorError::Integrity)?;
        let usage = envelope.usage;
        Ok(KubernetesGvisorCollectedOutput {
            outcome: SandboxExecutionOutcome::Completed(Box::new(SandboxCompletedOutput {
                value_id: request.output_value_id.clone(),
                classification: request.classification,
                schema_digest: request.output_schema_digest.clone(),
                content_digest,
                value: ValueRef::Inline {
                    value: envelope.value,
                },
                artifact_link_ids: Vec::new(),
                artifact_outputs: Vec::new(),
                validation_evidence_digest,
                usage: usage.clone(),
            })),
            usage,
            collection_evidence_digest,
        })
    }

    async fn revoke_and_cleanup(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: &ResourceId,
        physical_cleanup_evidence_digest: &Sha256Digest,
    ) -> Result<GvisorCleanupReceipt, KubernetesGvisorError> {
        let revocation = self
            .grant_revoker
            .revoke_exact(RevokeWasiSandboxGrants {
                tenant_id: request.tenant_id.clone(),
                sandbox_job_id: request.sandbox_job_id.clone(),
                request_digest: request.request_digest.clone(),
                worker_process_generation_id: worker_process_generation_id.clone(),
                attempt_no: request.attempt_no,
                lease_generation: request.lease_generation,
            })
            .await
            .map_err(|error| match error {
                WasiGrantRevocationError::Rejected => KubernetesGvisorError::Denied,
                WasiGrantRevocationError::Unavailable => KubernetesGvisorError::Unavailable,
            })?;
        let evidence_digest = canonical_digest(&serde_json::json!({
            "schema_version": 1,
            "kind": "gvisor_pod_cleanup",
            "request_digest": request.request_digest,
            "physical_cleanup_evidence_digest": physical_cleanup_evidence_digest,
            "grant_revocation_evidence_digest": revocation.evidence_digest,
        }))
        .map_err(|_| KubernetesGvisorError::Integrity)?
        .parse()
        .map_err(|_| KubernetesGvisorError::Integrity)?;
        Ok(GvisorCleanupReceipt {
            grants_revoked: true,
            ephemeral_storage_destroyed: true,
            evidence_digest,
        })
    }
}

pub struct KubernetesGvisorSandboxExecutorBackend {
    config: GvisorExecutorBackendConfig,
    runtime: Arc<dyn KubernetesGvisorPodRuntime>,
    broker: Arc<dyn KubernetesGvisorResultBroker>,
    permits: Arc<Semaphore>,
    executions: Mutex<BTreeMap<Sha256Digest, Arc<KubernetesGvisorExecution>>>,
}

struct KubernetesGvisorExecution {
    request: SandboxExecutionRequest,
    pod: Mutex<Option<LaunchedGvisorPod>>,
    phase: AtomicU8,
    cancelled: AtomicBool,
    _permit: OwnedSemaphorePermit,
}

impl KubernetesGvisorSandboxExecutorBackend {
    pub fn new(
        config: GvisorExecutorBackendConfig,
        runtime: Arc<dyn KubernetesGvisorPodRuntime>,
        broker: Arc<dyn KubernetesGvisorResultBroker>,
    ) -> Result<Self, KubernetesGvisorError> {
        if config.descriptor.backend_kind != SandboxIsolationBackendKind::Gvisor
            || config.descriptor.isolation_class != SandboxIsolationClass::SandboxedContainer
            || config.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || !(1..=4_096).contains(&config.maximum_concurrent_executions)
        {
            return Err(KubernetesGvisorError::InvalidContract);
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(config.maximum_concurrent_executions)),
            config,
            runtime,
            broker,
            executions: Mutex::new(BTreeMap::new()),
        })
    }

    fn lookup(
        &self,
        request_digest: &Sha256Digest,
        stage: SandboxBackendFailureStage,
    ) -> Result<Arc<KubernetesGvisorExecution>, SandboxBackendFailure> {
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

    async fn delete_and_confirm_absent(
        &self,
        pod: &LaunchedGvisorPod,
        timeout: Duration,
        stage: SandboxBackendFailureStage,
    ) -> Result<Sha256Digest, SandboxBackendFailure> {
        self.runtime
            .delete(pod)
            .await
            .map_err(|error| runtime_failure(error, stage, true))?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self
                .runtime
                .is_absent(pod)
                .await
                .map_err(|error| runtime_failure(error, stage, true))?
            {
                return evidence_digest_for_pod("gvisor_pod_absent", pod, stage);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(runtime_failure(
                    KubernetesGvisorError::TimedOut,
                    stage,
                    true,
                ));
            }
            tokio::time::sleep(Duration::from_millis(50).min(deadline - now)).await;
        }
    }

    async fn cleanup_execution(
        &self,
        execution: &KubernetesGvisorExecution,
        stage: SandboxBackendFailureStage,
    ) -> Result<GvisorCleanupReceipt, SandboxBackendFailure> {
        let pod = execution
            .pod
            .lock()
            .map_err(|_| failure(stage, "sandbox_gvisor_state_unavailable", None, true))?
            .clone();
        let physical = if let Some(pod) = pod {
            self.delete_and_confirm_absent(
                &pod,
                Duration::from_millis(execution.request.resources.cleanup_milliseconds),
                stage,
            )
            .await?
        } else {
            evidence_digest("gvisor_pod_never_created", &execution.request)
        };
        self.broker
            .revoke_and_cleanup(
                &execution.request,
                &self.config.worker_process_generation_id,
                &physical,
            )
            .await
            .map_err(|error| runtime_failure(error, stage, true))
    }
}

#[async_trait]
impl SandboxExecutorBackend for KubernetesGvisorSandboxExecutorBackend {
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
        let execution = Arc::new(KubernetesGvisorExecution {
            request: request.clone(),
            pod: Mutex::new(None),
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
            let pod = self
                .runtime
                .create(&request, &self.config.worker_process_generation_id)
                .await
                .map_err(|error| {
                    runtime_failure(error, SandboxBackendFailureStage::Preparing, false)
                })?;
            if execution.cancelled.load(Ordering::Acquire) {
                return Err(failure(
                    SandboxBackendFailureStage::Preparing,
                    "sandbox_gvisor_prepare_cancelled",
                    Some(identity_digest(&pod)),
                    false,
                ));
            }
            *execution.pod.lock().map_err(|_| {
                failure(
                    SandboxBackendFailureStage::Preparing,
                    "sandbox_gvisor_state_unavailable",
                    None,
                    false,
                )
            })? = Some(pod.clone());
            execution.phase.store(PHASE_PREPARED, Ordering::Release);
            Ok(PreparedSandbox {
                provider_process_generation_id: None,
                sandbox_identity_digest: identity_digest(&pod),
                request_digest: request.request_digest.clone(),
                attempt_no: request.attempt_no,
                lease_generation: request.lease_generation,
                prepare_evidence_digest: pod.launch_evidence_digest,
            })
        }
        .await;
        if prepared.is_err() {
            if let Ok(Some(pod)) = self
                .runtime
                .find_exact(&request, &self.config.worker_process_generation_id)
                .await
            {
                if let Ok(mut stored) = execution.pod.lock() {
                    *stored = Some(pod);
                }
            }
            let _ = self
                .cleanup_execution(&execution, SandboxBackendFailureStage::Preparing)
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
        let pod = execution
            .pod
            .lock()
            .map_err(|_| {
                failure(
                    SandboxBackendFailureStage::Starting,
                    "sandbox_gvisor_state_unavailable",
                    None,
                    true,
                )
            })?
            .clone()
            .ok_or_else(|| {
                failure(
                    SandboxBackendFailureStage::Starting,
                    "sandbox_gvisor_pod_missing",
                    None,
                    true,
                )
            })?;
        let start_evidence_digest = match self.runtime.start(&pod).await {
            Ok(evidence) => evidence,
            Err(error) => {
                let _ = self
                    .cleanup_execution(&execution, SandboxBackendFailureStage::Starting)
                    .await;
                self.remove(&request.request_digest);
                return Err(runtime_failure(
                    error,
                    SandboxBackendFailureStage::Starting,
                    false,
                ));
            }
        };
        Ok(RunningSandbox {
            prepared,
            start_evidence_digest,
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
        let pod = execution
            .pod
            .lock()
            .map_err(|_| {
                failure(
                    SandboxBackendFailureStage::Collecting,
                    "sandbox_gvisor_state_unavailable",
                    None,
                    true,
                )
            })?
            .clone()
            .ok_or_else(|| {
                failure(
                    SandboxBackendFailureStage::Collecting,
                    "sandbox_gvisor_pod_missing",
                    None,
                    true,
                )
            })?;
        let remaining = request
            .deadline
            .signed_duration_since(Utc::now())
            .to_std()
            .map_err(|_| {
                runtime_failure(
                    KubernetesGvisorError::TimedOut,
                    SandboxBackendFailureStage::Running,
                    true,
                )
            })?;
        let observed = self
            .runtime
            .wait_for_exit(&pod, remaining)
            .await
            .map_err(|error| runtime_failure(error, SandboxBackendFailureStage::Running, true))?;
        let collected = self
            .broker
            .collect(
                &request,
                &self.config.worker_process_generation_id,
                &pod,
                observed,
            )
            .await
            .map_err(|error| {
                runtime_failure(error, SandboxBackendFailureStage::Collecting, true)
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
        require_command(
            &execution,
            &command.tenant_id,
            &command.sandbox_job_id,
            command.attempt_no,
            command.lease_generation,
            Some(&command.sandbox_identity_digest),
            SandboxBackendFailureStage::Terminating,
        )?;
        execution.cancelled.store(true, Ordering::Release);
        self.cleanup_execution(&execution, SandboxBackendFailureStage::Terminating)
            .await?;
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
        require_command(
            &execution,
            &command.tenant_id,
            &command.sandbox_job_id,
            command.attempt_no,
            command.lease_generation,
            Some(&command.sandbox_identity_digest),
            SandboxBackendFailureStage::Destroying,
        )?;
        let identity = current_identity(&execution, SandboxBackendFailureStage::Destroying)?;
        let receipt = self
            .cleanup_execution(&execution, SandboxBackendFailureStage::Destroying)
            .await?;
        let cleanup = cleanup_evidence(identity, receipt)?;
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
        require_command(
            &execution,
            &command.tenant_id,
            &command.sandbox_job_id,
            command.attempt_no,
            command.lease_generation,
            None,
            SandboxBackendFailureStage::Aborting,
        )?;
        execution.cancelled.store(true, Ordering::Release);
        let identity = current_identity(&execution, SandboxBackendFailureStage::Aborting)?;
        let receipt = self
            .cleanup_execution(&execution, SandboxBackendFailureStage::Aborting)
            .await?;
        let cleanup = cleanup_evidence(identity.clone(), receipt)?;
        let termination = termination_evidence(&execution.request);
        self.remove(&command.request_digest);
        Ok(SandboxAbortEvidence {
            reason: command.reason,
            request_digest: command.request_digest,
            attempt_no: command.attempt_no,
            lease_generation: command.lease_generation,
            sandbox_identity_digest: identity,
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
        let pod = self
            .runtime
            .find_exact(
                &expired.request,
                &expired.previous_worker_process_generation_id,
            )
            .await
            .map_err(|error| {
                runtime_failure(error, SandboxBackendFailureStage::Recovering, true)
            })?;
        let sandbox_identity_digest = pod.as_ref().map(identity_digest).unwrap_or_else(|| {
            evidence_digest("gvisor_pod_absent_before_recovery", &expired.request)
        });
        let physical = if let Some(pod) = pod {
            self.delete_and_confirm_absent(
                &pod,
                Duration::from_millis(expired.request.resources.cleanup_milliseconds),
                SandboxBackendFailureStage::Recovering,
            )
            .await?
        } else {
            evidence_digest("gvisor_pod_absent", &expired.request)
        };
        let receipt = self
            .broker
            .revoke_and_cleanup(
                &expired.request,
                &expired.previous_worker_process_generation_id,
                &physical,
            )
            .await
            .map_err(|error| {
                runtime_failure(error, SandboxBackendFailureStage::Recovering, true)
            })?;
        let cleanup = cleanup_evidence(sandbox_identity_digest.clone(), receipt)?;
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
                execution_may_have_started: true,
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
        || request.network_mode != SandboxNetworkMode::None
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
    execution: &KubernetesGvisorExecution,
    request: &SandboxExecutionRequest,
    identity: &Sha256Digest,
) -> bool {
    execution.request.request_digest == request.request_digest
        && execution.request.tenant_id == request.tenant_id
        && execution.request.sandbox_job_id == request.sandbox_job_id
        && execution.request.attempt_no == request.attempt_no
        && execution.request.lease_generation == request.lease_generation
        && current_identity(execution, SandboxBackendFailureStage::Admission)
            .is_ok_and(|current| current == *identity)
}

fn require_command(
    execution: &KubernetesGvisorExecution,
    tenant_id: &ResourceId,
    sandbox_job_id: &ResourceId,
    attempt_no: u32,
    lease_generation: u64,
    identity: Option<&Sha256Digest>,
    stage: SandboxBackendFailureStage,
) -> Result<(), SandboxBackendFailure> {
    let matches = execution.request.tenant_id == *tenant_id
        && execution.request.sandbox_job_id == *sandbox_job_id
        && execution.request.attempt_no == attempt_no
        && execution.request.lease_generation == lease_generation
        && identity.is_none_or(|expected| {
            current_identity(execution, stage).is_ok_and(|actual| actual == *expected)
        });
    if !matches {
        return Err(failure(
            stage,
            "sandbox_gvisor_stale_command",
            identity.cloned(),
            true,
        ));
    }
    Ok(())
}

fn current_identity(
    execution: &KubernetesGvisorExecution,
    stage: SandboxBackendFailureStage,
) -> Result<Sha256Digest, SandboxBackendFailure> {
    execution
        .pod
        .lock()
        .map_err(|_| failure(stage, "sandbox_gvisor_state_unavailable", None, true))?
        .as_ref()
        .map(identity_digest)
        .ok_or_else(|| failure(stage, "sandbox_gvisor_pod_missing", None, true))
}

fn identity_digest(pod: &LaunchedGvisorPod) -> Sha256Digest {
    canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "kind": "gvisor_pod_identity",
        "namespace": pod.namespace,
        "name": pod.name,
        "uid": pod.uid,
        "request_digest": pod.request_digest,
        "worker_process_generation_id": pod.worker_process_generation_id,
    }))
    .expect("closed gVisor Pod identity is canonical")
    .parse()
    .expect("canonical digest has SHA-256 shape")
}

fn evidence_digest_for_pod(
    kind: &str,
    pod: &LaunchedGvisorPod,
    stage: SandboxBackendFailureStage,
) -> Result<Sha256Digest, SandboxBackendFailure> {
    canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "kind": kind,
        "pod_uid": pod.uid,
        "request_digest": pod.request_digest,
    }))
    .map_err(|_| failure(stage, "sandbox_gvisor_evidence_invalid", None, true))?
    .parse()
    .map_err(|_| failure(stage, "sandbox_gvisor_evidence_invalid", None, true))
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

fn termination_evidence(request: &SandboxExecutionRequest) -> SandboxTerminationEvidence {
    SandboxTerminationEvidence {
        evidence_digest: evidence_digest("gvisor_pod_terminated", request),
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
        grants_revoked: true,
        ephemeral_storage_destroyed: true,
        observed_at: Utc::now(),
        evidence_digest: receipt.evidence_digest,
    })
}

fn runtime_failure(
    error: KubernetesGvisorError,
    stage: SandboxBackendFailureStage,
    started: bool,
) -> SandboxBackendFailure {
    let (code, retryability) = match error {
        KubernetesGvisorError::Denied | KubernetesGvisorError::InvalidContract => {
            ("sandbox_gvisor_launcher_denied", Retryability::Never)
        }
        KubernetesGvisorError::Integrity | KubernetesGvisorError::Conflict => {
            ("sandbox_gvisor_launcher_integrity", Retryability::Never)
        }
        KubernetesGvisorError::TimedOut => (
            "sandbox_gvisor_launcher_timed_out",
            Retryability::SafeWithinPolicy,
        ),
        KubernetesGvisorError::NotFound | KubernetesGvisorError::Unavailable => (
            "sandbox_gvisor_launcher_unavailable",
            Retryability::SafeWithinPolicy,
        ),
    };
    let mut failure = failure(stage, code, None, started);
    failure.retryability = retryability;
    failure
}

fn failure(
    stage: SandboxBackendFailureStage,
    code: &str,
    identity: Option<Sha256Digest>,
    started: bool,
) -> SandboxBackendFailure {
    let evidence_digest = canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "stage": stage,
        "code": code,
        "sandbox_identity_digest": identity,
        "execution_may_have_started": started,
    }))
    .expect("closed gVisor failure evidence is canonical")
    .parse()
    .expect("canonical digest has SHA-256 shape");
    SandboxBackendFailure {
        stage,
        safe_code: code.to_owned(),
        safe_message: "gVisor sandbox operation failed closed".to_owned(),
        retryability: Retryability::SafeWithinPolicy,
        sandbox_identity_digest: identity,
        execution_may_have_started: started,
        external_effect_possible: false,
        evidence_digest,
    }
}

fn zero_digest() -> Sha256Digest {
    format!("sha256:{}", "0".repeat(64))
        .parse()
        .expect("zero digest is valid")
}
