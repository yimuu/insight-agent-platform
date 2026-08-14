use crate::ManagedMcpSandboxProtocolAdapter;
use async_trait::async_trait;
use insight_platform_contracts::{
    canonical_digest, Retryability, SandboxIsolationClass, SandboxJobState, Sha256Digest,
};
use insight_platform_mcp_host::McpOperationOutcome;
use insight_platform_sandbox::{
    AbortSandboxExecution, CollectedSandbox, DestroySandbox, ExpiredSandboxLease,
    InstalledSandboxBackendDescriptor, MicroVmIsolationProviderBackend,
    MicroVmProviderExecutionFence, PreparedSandbox, RunningSandbox, SandboxAbortEvidence,
    SandboxBackendFailure, SandboxBackendFailureStage, SandboxCleanupDisposition,
    SandboxCleanupEvidence, SandboxCommandLimits, SandboxExecutionOutcome, SandboxExecutionRequest,
    SandboxExecutionSource, SandboxIsolationBackendKind, SandboxLeaseRecoveryEvidence,
    SandboxResourceUsage, SandboxTerminationEvidence, SandboxUncertainty, TerminateSandbox,
};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt, sync::Arc};

/// Evidence returned by the host-side VMM adapter after constructing a fresh, single-use jail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedMicroVm {
    pub sandbox_identity_digest: Sha256Digest,
    pub prepare_evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningMicroVm {
    pub sandbox_identity_digest: Sha256Digest,
    pub start_evidence_digest: Sha256Digest,
}

/// Raw guest result visible only inside the trusted microVM provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MicroVmGuestCollection {
    Sandbox {
        outcome: SandboxExecutionOutcome,
        usage: SandboxResourceUsage,
        collection_evidence_digest: Sha256Digest,
    },
    ManagedMcp {
        outcome: Box<McpOperationOutcome>,
        usage: SandboxResourceUsage,
        protocol_evidence_digest: Sha256Digest,
        collection_evidence_digest: Sha256Digest,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MicroVmTerminationProof {
    pub sandbox_identity_digest: Sha256Digest,
    pub evidence_digest: Sha256Digest,
    pub guest_acknowledged: bool,
    pub process_tree_terminated: bool,
    pub grants_revoked: bool,
    pub external_effect_possible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MicroVmCleanupProof {
    pub sandbox_identity_digest: Sha256Digest,
    pub evidence_digest: Sha256Digest,
    pub disposition: SandboxCleanupDisposition,
    pub grants_revoked: bool,
    pub ephemeral_storage_destroyed: bool,
    pub observed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MicroVmAbortProof {
    pub sandbox_identity_digest: Sha256Digest,
    pub termination: MicroVmTerminationProof,
    pub cleanup: MicroVmCleanupProof,
    pub evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MicroVmRecoveryProof {
    pub sandbox_identity_digest: Sha256Digest,
    pub cleanup: MicroVmCleanupProof,
    pub evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MicroVmProviderFailure {
    pub safe_code: String,
    pub safe_message: String,
    pub evidence_digest: Sha256Digest,
    pub sandbox_identity_digest: Option<Sha256Digest>,
    pub execution_may_have_started: bool,
    pub external_effect_possible: bool,
}

impl fmt::Display for MicroVmProviderFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl Error for MicroVmProviderFailure {}

/// OS/VMM port owned by the provider process.
///
/// Implementations must create a new jail/UID/cgroup/rootfs overlay/vsock identity per request,
/// never expose a host path or PID to the caller, and make abort/recovery addressable by the exact
/// signed request tuple even while `prepare` is in flight.
#[async_trait]
pub trait MicroVmLifecyclePort: Send + Sync {
    async fn prepare(
        &self,
        fence: &MicroVmProviderExecutionFence,
        request: &SandboxExecutionRequest,
    ) -> Result<PreparedMicroVm, MicroVmProviderFailure>;

    async fn start(
        &self,
        fence: &MicroVmProviderExecutionFence,
        request: &SandboxExecutionRequest,
        prepared: &PreparedMicroVm,
    ) -> Result<RunningMicroVm, MicroVmProviderFailure>;

    async fn collect(
        &self,
        fence: &MicroVmProviderExecutionFence,
        request: &SandboxExecutionRequest,
        running: &RunningMicroVm,
    ) -> Result<MicroVmGuestCollection, MicroVmProviderFailure>;

    async fn terminate(
        &self,
        fence: &MicroVmProviderExecutionFence,
        command: &TerminateSandbox,
    ) -> Result<MicroVmTerminationProof, MicroVmProviderFailure>;

    async fn destroy(
        &self,
        fence: &MicroVmProviderExecutionFence,
        command: &DestroySandbox,
    ) -> Result<MicroVmCleanupProof, MicroVmProviderFailure>;

    async fn abort(
        &self,
        fence: &MicroVmProviderExecutionFence,
        command: &AbortSandboxExecution,
    ) -> Result<MicroVmAbortProof, MicroVmProviderFailure>;

    async fn recover_expired(
        &self,
        fence: &MicroVmProviderExecutionFence,
        expired: &ExpiredSandboxLease,
    ) -> Result<MicroVmRecoveryProof, MicroVmProviderFailure>;
}

pub trait MicroVmProviderClock: Send + Sync {
    fn now(&self) -> chrono::DateTime<chrono::Utc>;
}

pub struct SystemMicroVmProviderClock;

impl MicroVmProviderClock for SystemMicroVmProviderClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

/// Production-facing `SandboxExecutorBackend` composition for one exact microVM provider build.
pub struct MicroVmSandboxExecutorBackend<V, C> {
    descriptor: InstalledSandboxBackendDescriptor,
    limits: SandboxCommandLimits,
    lifecycle: Arc<V>,
    protocol: Arc<ManagedMcpSandboxProtocolAdapter>,
    clock: Arc<C>,
}

impl<V, C> MicroVmSandboxExecutorBackend<V, C>
where
    V: MicroVmLifecyclePort,
    C: MicroVmProviderClock,
{
    pub fn new(
        descriptor: InstalledSandboxBackendDescriptor,
        limits: SandboxCommandLimits,
        lifecycle: Arc<V>,
        protocol: Arc<ManagedMcpSandboxProtocolAdapter>,
        clock: Arc<C>,
    ) -> Result<Self, MicroVmProviderFailure> {
        if descriptor.backend_kind != SandboxIsolationBackendKind::MicroVm
            || descriptor.isolation_class != SandboxIsolationClass::MicroVm
        {
            return Err(provider_contract_failure("microvm_provider_configuration"));
        }
        Ok(Self {
            descriptor,
            limits,
            lifecycle,
            protocol,
            clock,
        })
    }

    fn validate_request(
        &self,
        request: &SandboxExecutionRequest,
    ) -> Result<(), SandboxBackendFailure> {
        request
            .validate_at(self.clock.now(), self.limits)
            .map_err(|_| {
                self.failure(
                    SandboxBackendFailureStage::Admission,
                    provider_contract_failure("microvm_invalid_request"),
                )
            })?;
        if request.isolation_class != SandboxIsolationClass::MicroVm
            || request.executor_worker_manifest_digest != self.descriptor.worker_manifest_digest
            || request.isolation_backend_contract_digest != self.descriptor.backend_contract_digest
        {
            return Err(self.failure(
                SandboxBackendFailureStage::Admission,
                provider_contract_failure("microvm_provider_contract_mismatch"),
            ));
        }
        Ok(())
    }

    fn failure(
        &self,
        stage: SandboxBackendFailureStage,
        failure: MicroVmProviderFailure,
    ) -> SandboxBackendFailure {
        SandboxBackendFailure {
            stage,
            safe_code: failure.safe_code,
            safe_message: failure.safe_message,
            retryability: if failure.execution_may_have_started || failure.external_effect_possible
            {
                Retryability::ReconcileBeforeRetry
            } else {
                Retryability::SafeWithinPolicy
            },
            evidence_digest: failure.evidence_digest,
            sandbox_identity_digest: failure.sandbox_identity_digest,
            execution_may_have_started: failure.execution_may_have_started,
            external_effect_possible: failure.external_effect_possible,
        }
    }
}

#[async_trait]
impl<V, C> MicroVmIsolationProviderBackend for MicroVmSandboxExecutorBackend<V, C>
where
    V: MicroVmLifecyclePort + 'static,
    C: MicroVmProviderClock + 'static,
{
    fn descriptor(&self) -> InstalledSandboxBackendDescriptor {
        self.descriptor.clone()
    }

    async fn prepare(
        &self,
        fence: MicroVmProviderExecutionFence,
        request: SandboxExecutionRequest,
    ) -> Result<PreparedSandbox, SandboxBackendFailure> {
        fence.validate().map_err(|_| {
            self.failure(
                SandboxBackendFailureStage::Admission,
                provider_contract_failure("microvm_executor_fence_invalid"),
            )
        })?;
        self.validate_request(&request)?;
        let prepared = self
            .lifecycle
            .prepare(&fence, &request)
            .await
            .map_err(|failure| self.failure(SandboxBackendFailureStage::Preparing, failure))?;
        Ok(PreparedSandbox {
            sandbox_identity_digest: prepared.sandbox_identity_digest,
            request_digest: request.request_digest,
            attempt_no: request.attempt_no,
            lease_generation: request.lease_generation,
            prepare_evidence_digest: prepared.prepare_evidence_digest,
        })
    }

    async fn start(
        &self,
        fence: MicroVmProviderExecutionFence,
        request: SandboxExecutionRequest,
        prepared: PreparedSandbox,
    ) -> Result<RunningSandbox, SandboxBackendFailure> {
        self.validate_request(&request)?;
        if prepared.request_digest != request.request_digest
            || prepared.attempt_no != request.attempt_no
            || prepared.lease_generation != request.lease_generation
        {
            return Err(self.failure(
                SandboxBackendFailureStage::Starting,
                provider_contract_failure("microvm_prepared_identity_mismatch"),
            ));
        }
        let running = self
            .lifecycle
            .start(
                &fence,
                &request,
                &PreparedMicroVm {
                    sandbox_identity_digest: prepared.sandbox_identity_digest.clone(),
                    prepare_evidence_digest: prepared.prepare_evidence_digest.clone(),
                },
            )
            .await
            .map_err(|failure| self.failure(SandboxBackendFailureStage::Starting, failure))?;
        if running.sandbox_identity_digest != prepared.sandbox_identity_digest {
            return Err(self.failure(
                SandboxBackendFailureStage::Starting,
                provider_uncertain_failure(
                    "microvm_running_identity_mismatch",
                    Some(prepared.sandbox_identity_digest),
                    request_external_effect_possible(&request),
                ),
            ));
        }
        Ok(RunningSandbox {
            prepared,
            start_evidence_digest: running.start_evidence_digest,
        })
    }

    async fn collect(
        &self,
        fence: MicroVmProviderExecutionFence,
        request: SandboxExecutionRequest,
        running: RunningSandbox,
    ) -> Result<CollectedSandbox, SandboxBackendFailure> {
        self.validate_request(&request)?;
        if running.prepared.request_digest != request.request_digest
            || running.prepared.attempt_no != request.attempt_no
            || running.prepared.lease_generation != request.lease_generation
        {
            return Err(self.failure(
                SandboxBackendFailureStage::Collecting,
                provider_uncertain_failure(
                    "microvm_running_request_mismatch",
                    Some(running.prepared.sandbox_identity_digest.clone()),
                    request_external_effect_possible(&request),
                ),
            ));
        }
        let raw = self
            .lifecycle
            .collect(
                &fence,
                &request,
                &RunningMicroVm {
                    sandbox_identity_digest: running.prepared.sandbox_identity_digest.clone(),
                    start_evidence_digest: running.start_evidence_digest.clone(),
                },
            )
            .await
            .map_err(|failure| self.failure(SandboxBackendFailureStage::Collecting, failure))?;
        let (outcome, usage, collection_evidence_digest) = match raw {
            MicroVmGuestCollection::Sandbox {
                outcome,
                usage,
                collection_evidence_digest,
            } if matches!(
                request.execution_source,
                SandboxExecutionSource::SandboxCapability { .. }
            ) =>
            {
                (outcome, usage, collection_evidence_digest)
            }
            MicroVmGuestCollection::ManagedMcp {
                outcome,
                usage,
                protocol_evidence_digest,
                collection_evidence_digest,
            } if matches!(
                request.execution_source,
                SandboxExecutionSource::ManagedMcp { .. }
            ) =>
            {
                let mapped = self
                    .protocol
                    .decode(
                        &request,
                        fence.worker_process_generation_id.clone(),
                        *outcome,
                        self.clock.now(),
                        protocol_evidence_digest,
                        usage.clone(),
                    )
                    .map_err(|_| {
                        self.failure(
                            SandboxBackendFailureStage::Collecting,
                            provider_uncertain_failure(
                                "microvm_managed_mcp_protocol_invalid",
                                Some(running.prepared.sandbox_identity_digest.clone()),
                                request_external_effect_possible(&request),
                            ),
                        )
                    })?;
                (
                    SandboxExecutionOutcome::ManagedMcp(Box::new(mapped)),
                    usage,
                    collection_evidence_digest,
                )
            }
            _ => {
                return Err(self.failure(
                    SandboxBackendFailureStage::Collecting,
                    provider_uncertain_failure(
                        "microvm_guest_result_kind_mismatch",
                        Some(running.prepared.sandbox_identity_digest.clone()),
                        request_external_effect_possible(&request),
                    ),
                ));
            }
        };
        outcome.validate_for(&request, self.limits).map_err(|_| {
            self.failure(
                SandboxBackendFailureStage::Collecting,
                provider_uncertain_failure(
                    "microvm_guest_result_invalid",
                    Some(running.prepared.sandbox_identity_digest.clone()),
                    request_external_effect_possible(&request),
                ),
            )
        })?;
        if outcome_usage(&outcome) != Some(&usage) {
            return Err(self.failure(
                SandboxBackendFailureStage::Collecting,
                provider_uncertain_failure(
                    "microvm_usage_mismatch",
                    Some(running.prepared.sandbox_identity_digest.clone()),
                    request_external_effect_possible(&request),
                ),
            ));
        }
        Ok(CollectedSandbox {
            running,
            outcome,
            usage,
            collection_evidence_digest,
        })
    }

    async fn terminate(
        &self,
        fence: MicroVmProviderExecutionFence,
        command: TerminateSandbox,
    ) -> Result<SandboxTerminationEvidence, SandboxBackendFailure> {
        command.validate().map_err(|_| {
            self.failure(
                SandboxBackendFailureStage::Terminating,
                provider_contract_failure("microvm_invalid_terminate"),
            )
        })?;
        let proof = self
            .lifecycle
            .terminate(&fence, &command)
            .await
            .map_err(|failure| self.failure(SandboxBackendFailureStage::Terminating, failure))?;
        if proof.sandbox_identity_digest != command.sandbox_identity_digest {
            return Err(self.failure(
                SandboxBackendFailureStage::Terminating,
                provider_uncertain_failure(
                    "microvm_termination_identity_mismatch",
                    Some(command.sandbox_identity_digest),
                    true,
                ),
            ));
        }
        Ok(termination_evidence(proof))
    }

    async fn destroy(
        &self,
        fence: MicroVmProviderExecutionFence,
        command: DestroySandbox,
    ) -> Result<SandboxCleanupEvidence, SandboxBackendFailure> {
        command.validate().map_err(|_| {
            self.failure(
                SandboxBackendFailureStage::Destroying,
                provider_contract_failure("microvm_invalid_destroy"),
            )
        })?;
        let proof = self
            .lifecycle
            .destroy(&fence, &command)
            .await
            .map_err(|failure| self.failure(SandboxBackendFailureStage::Destroying, failure))?;
        if proof.sandbox_identity_digest != command.sandbox_identity_digest {
            return Err(self.failure(
                SandboxBackendFailureStage::Destroying,
                provider_uncertain_failure(
                    "microvm_cleanup_identity_mismatch",
                    Some(command.sandbox_identity_digest),
                    true,
                ),
            ));
        }
        Ok(cleanup_evidence(proof))
    }

    async fn abort(
        &self,
        fence: MicroVmProviderExecutionFence,
        command: AbortSandboxExecution,
    ) -> Result<SandboxAbortEvidence, SandboxBackendFailure> {
        command.validate().map_err(|_| {
            self.failure(
                SandboxBackendFailureStage::Aborting,
                provider_contract_failure("microvm_invalid_abort"),
            )
        })?;
        let proof = self
            .lifecycle
            .abort(&fence, &command)
            .await
            .map_err(|failure| self.failure(SandboxBackendFailureStage::Aborting, failure))?;
        if proof.termination.sandbox_identity_digest != proof.sandbox_identity_digest
            || proof.cleanup.sandbox_identity_digest != proof.sandbox_identity_digest
        {
            return Err(self.failure(
                SandboxBackendFailureStage::Aborting,
                provider_uncertain_failure(
                    "microvm_abort_identity_mismatch",
                    Some(proof.sandbox_identity_digest),
                    request_effect_possible(command.effect, command.network_mode),
                ),
            ));
        }
        Ok(SandboxAbortEvidence {
            reason: command.reason,
            request_digest: command.request_digest,
            attempt_no: command.attempt_no,
            lease_generation: command.lease_generation,
            sandbox_identity_digest: proof.sandbox_identity_digest,
            termination: termination_evidence(proof.termination),
            cleanup: cleanup_evidence(proof.cleanup),
            evidence_digest: proof.evidence_digest,
        })
    }

    async fn recover_expired_lease(
        &self,
        fence: MicroVmProviderExecutionFence,
        expired: ExpiredSandboxLease,
    ) -> Result<SandboxLeaseRecoveryEvidence, SandboxBackendFailure> {
        expired.validate(self.limits).map_err(|_| {
            self.failure(
                SandboxBackendFailureStage::Recovering,
                provider_contract_failure("microvm_invalid_recovery"),
            )
        })?;
        let proof = self
            .lifecycle
            .recover_expired(&fence, &expired)
            .await
            .map_err(|failure| self.failure(SandboxBackendFailureStage::Recovering, failure))?;
        let cleanup = cleanup_evidence(proof.cleanup);
        let execution_may_have_started = expired.physical_state != SandboxJobState::Preparing;
        let external_effect_possible =
            execution_may_have_started && request_external_effect_possible(&expired.request);
        SandboxLeaseRecoveryEvidence {
            schema_version: 1,
            request_digest: expired.request.request_digest.clone(),
            attempt_no: expired.request.attempt_no,
            lease_generation: expired.observed_lease_generation,
            physical_state: expired.physical_state,
            executor_identity_digest: expired.executor_identity_digest.clone().ok_or_else(
                || {
                    self.failure(
                        SandboxBackendFailureStage::Recovering,
                        provider_contract_failure("microvm_missing_executor_identity"),
                    )
                },
            )?,
            uncertainty: SandboxUncertainty {
                evidence_digest: proof.evidence_digest,
                sandbox_identity_digest: proof.sandbox_identity_digest,
                execution_may_have_started,
                external_effect_possible,
                manual_reconciliation_required: external_effect_possible,
            },
            cleanup,
            evidence_digest: placeholder_digest(),
        }
        .seal()
        .map_err(|_| {
            self.failure(
                SandboxBackendFailureStage::Recovering,
                provider_contract_failure("microvm_recovery_evidence_invalid"),
            )
        })
    }
}

fn termination_evidence(proof: MicroVmTerminationProof) -> SandboxTerminationEvidence {
    SandboxTerminationEvidence {
        evidence_digest: proof.evidence_digest,
        guest_acknowledged: proof.guest_acknowledged,
        process_tree_terminated: proof.process_tree_terminated,
        grants_revoked: proof.grants_revoked,
        external_effect_possible: proof.external_effect_possible,
    }
}

fn cleanup_evidence(proof: MicroVmCleanupProof) -> SandboxCleanupEvidence {
    SandboxCleanupEvidence {
        disposition: proof.disposition,
        sandbox_identity_digest: proof.sandbox_identity_digest,
        grants_revoked: proof.grants_revoked,
        ephemeral_storage_destroyed: proof.ephemeral_storage_destroyed,
        observed_at: proof.observed_at,
        evidence_digest: proof.evidence_digest,
    }
}

fn outcome_usage(outcome: &SandboxExecutionOutcome) -> Option<&SandboxResourceUsage> {
    match outcome {
        SandboxExecutionOutcome::Completed(output) => Some(&output.usage),
        SandboxExecutionOutcome::ManagedMcp(output) => Some(&output.usage),
        SandboxExecutionOutcome::Failed(_)
        | SandboxExecutionOutcome::Cancelled(_)
        | SandboxExecutionOutcome::TimedOut(_)
        | SandboxExecutionOutcome::Uncertain(_) => None,
    }
}

fn request_external_effect_possible(request: &SandboxExecutionRequest) -> bool {
    request_effect_possible(request.effect, request.network_mode)
}

fn request_effect_possible(
    effect: insight_platform_contracts::Effect,
    network_mode: insight_platform_sandbox::SandboxNetworkMode,
) -> bool {
    effect.risk_rank() >= insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
        || network_mode != insight_platform_sandbox::SandboxNetworkMode::None
}

fn provider_contract_failure(code: &str) -> MicroVmProviderFailure {
    MicroVmProviderFailure {
        safe_code: code.to_owned(),
        safe_message: "microVM provider rejected an invalid contract".to_owned(),
        evidence_digest: evidence_digest(code),
        sandbox_identity_digest: None,
        execution_may_have_started: false,
        external_effect_possible: false,
    }
}

fn provider_uncertain_failure(
    code: &str,
    sandbox_identity_digest: Option<Sha256Digest>,
    external_effect_possible: bool,
) -> MicroVmProviderFailure {
    MicroVmProviderFailure {
        safe_code: code.to_owned(),
        safe_message: "microVM provider could not prove a safe terminal result".to_owned(),
        evidence_digest: evidence_digest(code),
        sandbox_identity_digest,
        execution_may_have_started: true,
        external_effect_possible,
    }
}

fn evidence_digest(code: &str) -> Sha256Digest {
    canonical_digest(&serde_json::json!({
        "domain": code,
        "schema_version": 1,
    }))
    .expect("static microVM evidence is canonical")
    .parse()
    .expect("canonical digest is SHA-256")
}

fn placeholder_digest() -> Sha256Digest {
    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        .parse()
        .expect("static placeholder digest is valid")
}
