use crate::{
    MicroVmAbortProof, MicroVmCleanupProof, MicroVmGuestCollection, MicroVmLifecyclePort,
    MicroVmProviderFailure, MicroVmRecoveryProof, MicroVmTerminationProof, PreparedMicroVm,
    RunningMicroVm,
};
use async_trait::async_trait;
use insight_platform_contracts::{canonical_digest, ResourceId, Sha256Digest};
use insight_platform_sandbox::{
    AbortSandboxExecution, DestroySandbox, ExpiredSandboxLease, MicroVmProviderExecutionFence,
    SandboxExecutionRequest, SandboxNetworkMode, TerminateSandbox,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;

/// Stable physical-attempt key. Transport request IDs and provider process generations are not
/// part of this identity, so retries and provider restarts cannot create a second microVM for the
/// same durable Sandbox attempt.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MicroVmAttemptKey {
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub executor_worker_process_generation_id: ResourceId,
    pub attempt_no: u32,
    pub lease_generation: u64,
}

impl MicroVmAttemptKey {
    pub fn from_request(
        fence: &MicroVmProviderExecutionFence,
        request: &SandboxExecutionRequest,
    ) -> Self {
        Self {
            tenant_id: request.tenant_id.clone(),
            sandbox_job_id: request.sandbox_job_id.clone(),
            request_digest: request.request_digest.clone(),
            executor_worker_process_generation_id: fence.worker_process_generation_id.clone(),
            attempt_no: request.attempt_no,
            lease_generation: request.lease_generation,
        }
    }

    pub fn from_terminate(
        fence: &MicroVmProviderExecutionFence,
        command: &TerminateSandbox,
    ) -> Self {
        Self {
            tenant_id: command.tenant_id.clone(),
            sandbox_job_id: command.sandbox_job_id.clone(),
            request_digest: command.request_digest.clone(),
            executor_worker_process_generation_id: fence.worker_process_generation_id.clone(),
            attempt_no: command.attempt_no,
            lease_generation: command.lease_generation,
        }
    }

    pub fn from_destroy(fence: &MicroVmProviderExecutionFence, command: &DestroySandbox) -> Self {
        Self {
            tenant_id: command.tenant_id.clone(),
            sandbox_job_id: command.sandbox_job_id.clone(),
            request_digest: command.request_digest.clone(),
            executor_worker_process_generation_id: fence.worker_process_generation_id.clone(),
            attempt_no: command.attempt_no,
            lease_generation: command.lease_generation,
        }
    }

    pub fn from_abort(
        fence: &MicroVmProviderExecutionFence,
        command: &AbortSandboxExecution,
    ) -> Self {
        Self {
            tenant_id: command.tenant_id.clone(),
            sandbox_job_id: command.sandbox_job_id.clone(),
            request_digest: command.request_digest.clone(),
            executor_worker_process_generation_id: fence.worker_process_generation_id.clone(),
            attempt_no: command.attempt_no,
            lease_generation: command.lease_generation,
        }
    }

    pub fn from_expired(
        fence: &MicroVmProviderExecutionFence,
        expired: &ExpiredSandboxLease,
    ) -> Self {
        Self::from_request(fence, &expired.request)
    }
}

/// One physical VMM instance. Implementations own the process tree, guest channel, grant handles
/// and ephemeral storage and must make every method idempotent for this exact attempt.
#[async_trait]
pub trait MicroVmHostInstance: Send + Sync {
    fn sandbox_identity_digest(&self) -> Sha256Digest;

    async fn start(
        &self,
        fence: &MicroVmProviderExecutionFence,
        request: &SandboxExecutionRequest,
    ) -> Result<RunningMicroVm, MicroVmProviderFailure>;

    async fn collect(
        &self,
        fence: &MicroVmProviderExecutionFence,
        request: &SandboxExecutionRequest,
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

pub struct PreparedMicroVmHost {
    pub instance: Arc<dyn MicroVmHostInstance>,
    pub evidence: PreparedMicroVm,
}

/// Durable host factory. The implementation, not this process-local coordinator, owns uniqueness
/// and restart recovery of the physical jail. `prepare_exact` must reopen an existing sealed
/// instance for the same key and must never replace it with a fresh VM.
#[async_trait]
pub trait MicroVmHostFactory: Send + Sync {
    async fn prepare_exact(
        &self,
        fence: &MicroVmProviderExecutionFence,
        request: &SandboxExecutionRequest,
    ) -> Result<PreparedMicroVmHost, MicroVmProviderFailure>;

    async fn open_exact(
        &self,
        key: &MicroVmAttemptKey,
        expected_sandbox_identity_digest: Option<&Sha256Digest>,
    ) -> Result<Option<PreparedMicroVmHost>, MicroVmProviderFailure>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MicroVmLifecyclePhase {
    Preparing,
    Prepared,
    Starting,
    Running,
    Collecting,
    Collected,
    Terminating,
    Terminated,
    Destroying,
    Aborting,
    Recovering,
    Uncertain,
}

struct MicroVmLifecycleState {
    phase: MicroVmLifecyclePhase,
    instance: Option<Arc<dyn MicroVmHostInstance>>,
    prepared: Option<PreparedMicroVm>,
    running: Option<RunningMicroVm>,
    collection: Option<MicroVmGuestCollection>,
    termination: Option<MicroVmTerminationProof>,
}

struct MicroVmLifecycleEntry {
    state: Mutex<MicroVmLifecycleState>,
}

/// Provider-side physical lifecycle coordinator.
///
/// The map is only a bounded, process-local single-flight/cache. Physical uniqueness and recovery
/// remain the factory's responsibility, while logical Job/lease authority remains PostgreSQL.
pub struct DurableMicroVmLifecycle<F> {
    factory: Arc<F>,
    entries: Mutex<BTreeMap<MicroVmAttemptKey, Arc<MicroVmLifecycleEntry>>>,
    maximum_entries: usize,
}

impl<F> DurableMicroVmLifecycle<F>
where
    F: MicroVmHostFactory,
{
    pub fn new(factory: Arc<F>, maximum_entries: usize) -> Result<Self, MicroVmProviderFailure> {
        if maximum_entries == 0 || maximum_entries > 65_536 {
            return Err(contract_failure("microvm_lifecycle_capacity_invalid"));
        }
        Ok(Self {
            factory,
            entries: Mutex::new(BTreeMap::new()),
            maximum_entries,
        })
    }

    async fn existing(&self, key: &MicroVmAttemptKey) -> Option<Arc<MicroVmLifecycleEntry>> {
        self.entries.lock().await.get(key).cloned()
    }

    async fn insert_preparing(
        &self,
        key: MicroVmAttemptKey,
    ) -> Result<(Arc<MicroVmLifecycleEntry>, bool), MicroVmProviderFailure> {
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get(&key) {
            return Ok((Arc::clone(entry), false));
        }
        if entries.len() >= self.maximum_entries {
            return Err(contract_failure("microvm_lifecycle_capacity_exhausted"));
        }
        let entry = Arc::new(MicroVmLifecycleEntry {
            state: Mutex::new(MicroVmLifecycleState {
                phase: MicroVmLifecyclePhase::Preparing,
                instance: None,
                prepared: None,
                running: None,
                collection: None,
                termination: None,
            }),
        });
        entries.insert(key, Arc::clone(&entry));
        Ok((entry, true))
    }

    async fn remove(&self, key: &MicroVmAttemptKey, entry: &Arc<MicroVmLifecycleEntry>) {
        let mut entries = self.entries.lock().await;
        if entries
            .get(key)
            .is_some_and(|candidate| Arc::ptr_eq(candidate, entry))
        {
            entries.remove(key);
        }
    }

    async fn open_or_cache(
        &self,
        key: &MicroVmAttemptKey,
        expected_identity: Option<&Sha256Digest>,
    ) -> Result<Arc<MicroVmLifecycleEntry>, MicroVmProviderFailure> {
        if let Some(entry) = self.existing(key).await {
            return Ok(entry);
        }
        let prepared = self
            .factory
            .open_exact(key, expected_identity)
            .await?
            .ok_or_else(|| contract_failure("microvm_physical_instance_not_found"))?;
        validate_prepared_host(&prepared, expected_identity)?;
        let (entry, inserted) = self.insert_preparing(key.clone()).await?;
        if inserted {
            let mut state = entry.state.lock().await;
            state.phase = MicroVmLifecyclePhase::Prepared;
            state.instance = Some(prepared.instance);
            state.prepared = Some(prepared.evidence);
        }
        Ok(entry)
    }
}

#[async_trait]
impl<F> MicroVmLifecyclePort for DurableMicroVmLifecycle<F>
where
    F: MicroVmHostFactory + 'static,
{
    async fn prepare(
        &self,
        fence: &MicroVmProviderExecutionFence,
        request: &SandboxExecutionRequest,
    ) -> Result<PreparedMicroVm, MicroVmProviderFailure> {
        let key = MicroVmAttemptKey::from_request(fence, request);
        let (entry, inserted) = self.insert_preparing(key.clone()).await?;
        if !inserted {
            let state = entry.state.lock().await;
            return match state.phase {
                MicroVmLifecyclePhase::Prepared
                | MicroVmLifecyclePhase::Starting
                | MicroVmLifecyclePhase::Running
                | MicroVmLifecyclePhase::Collecting
                | MicroVmLifecyclePhase::Collected
                | MicroVmLifecyclePhase::Terminating
                | MicroVmLifecyclePhase::Terminated => state
                    .prepared
                    .clone()
                    .ok_or_else(|| contract_failure("microvm_prepared_evidence_missing")),
                MicroVmLifecyclePhase::Preparing => {
                    Err(contract_failure("microvm_prepare_in_progress"))
                }
                MicroVmLifecyclePhase::Destroying
                | MicroVmLifecyclePhase::Aborting
                | MicroVmLifecyclePhase::Recovering
                | MicroVmLifecyclePhase::Uncertain => Err(uncertain_failure(
                    "microvm_prepare_not_repeatable",
                    state
                        .prepared
                        .as_ref()
                        .map(|prepared| prepared.sandbox_identity_digest.clone()),
                    request_external_effect_possible(request),
                )),
            };
        }

        match self.factory.prepare_exact(fence, request).await {
            Ok(prepared) => {
                validate_prepared_host(&prepared, None)?;
                let evidence = prepared.evidence.clone();
                let mut state = entry.state.lock().await;
                state.phase = MicroVmLifecyclePhase::Prepared;
                state.instance = Some(prepared.instance);
                state.prepared = Some(prepared.evidence);
                Ok(evidence)
            }
            Err(failure) => {
                if failure.execution_may_have_started {
                    entry.state.lock().await.phase = MicroVmLifecyclePhase::Uncertain;
                } else {
                    self.remove(&key, &entry).await;
                }
                Err(failure)
            }
        }
    }

    async fn start(
        &self,
        fence: &MicroVmProviderExecutionFence,
        request: &SandboxExecutionRequest,
        prepared: &PreparedMicroVm,
    ) -> Result<RunningMicroVm, MicroVmProviderFailure> {
        let key = MicroVmAttemptKey::from_request(fence, request);
        let entry = self
            .open_or_cache(&key, Some(&prepared.sandbox_identity_digest))
            .await?;
        let instance = {
            let mut state = entry.state.lock().await;
            if state.prepared.as_ref() != Some(prepared) {
                return Err(contract_failure("microvm_prepared_evidence_mismatch"));
            }
            match state.phase {
                MicroVmLifecyclePhase::Prepared => {
                    state.phase = MicroVmLifecyclePhase::Starting;
                    state.instance.clone()
                }
                MicroVmLifecyclePhase::Running
                | MicroVmLifecyclePhase::Collecting
                | MicroVmLifecyclePhase::Collected => {
                    return state.running.clone().ok_or_else(|| {
                        uncertain_failure(
                            "microvm_running_evidence_missing",
                            Some(prepared.sandbox_identity_digest.clone()),
                            request_external_effect_possible(request),
                        )
                    })
                }
                MicroVmLifecyclePhase::Starting => {
                    return Err(uncertain_failure(
                        "microvm_start_in_progress",
                        Some(prepared.sandbox_identity_digest.clone()),
                        request_external_effect_possible(request),
                    ))
                }
                _ => {
                    return Err(uncertain_failure(
                        "microvm_start_not_allowed",
                        Some(prepared.sandbox_identity_digest.clone()),
                        request_external_effect_possible(request),
                    ))
                }
            }
        }
        .ok_or_else(|| contract_failure("microvm_host_instance_missing"))?;
        match instance.start(fence, request).await {
            Ok(running) if running.sandbox_identity_digest == prepared.sandbox_identity_digest => {
                let mut state = entry.state.lock().await;
                if state.phase != MicroVmLifecyclePhase::Starting {
                    state.phase = MicroVmLifecyclePhase::Uncertain;
                    return Err(uncertain_failure(
                        "microvm_start_raced_with_control",
                        Some(prepared.sandbox_identity_digest.clone()),
                        request_external_effect_possible(request),
                    ));
                }
                state.phase = MicroVmLifecyclePhase::Running;
                state.running = Some(running.clone());
                Ok(running)
            }
            Ok(_) => {
                entry.state.lock().await.phase = MicroVmLifecyclePhase::Uncertain;
                Err(uncertain_failure(
                    "microvm_started_identity_mismatch",
                    Some(prepared.sandbox_identity_digest.clone()),
                    request_external_effect_possible(request),
                ))
            }
            Err(failure) => {
                entry.state.lock().await.phase = MicroVmLifecyclePhase::Uncertain;
                Err(failure)
            }
        }
    }

    async fn collect(
        &self,
        fence: &MicroVmProviderExecutionFence,
        request: &SandboxExecutionRequest,
        running: &RunningMicroVm,
    ) -> Result<MicroVmGuestCollection, MicroVmProviderFailure> {
        let key = MicroVmAttemptKey::from_request(fence, request);
        let entry = self
            .open_or_cache(&key, Some(&running.sandbox_identity_digest))
            .await?;
        let instance = {
            let mut state = entry.state.lock().await;
            if state.running.as_ref() != Some(running) {
                return Err(uncertain_failure(
                    "microvm_running_evidence_mismatch",
                    Some(running.sandbox_identity_digest.clone()),
                    request_external_effect_possible(request),
                ));
            }
            match state.phase {
                MicroVmLifecyclePhase::Running => {
                    state.phase = MicroVmLifecyclePhase::Collecting;
                    state.instance.clone()
                }
                MicroVmLifecyclePhase::Collected => {
                    return state.collection.clone().ok_or_else(|| {
                        uncertain_failure(
                            "microvm_collection_evidence_missing",
                            Some(running.sandbox_identity_digest.clone()),
                            request_external_effect_possible(request),
                        )
                    })
                }
                MicroVmLifecyclePhase::Collecting => {
                    return Err(uncertain_failure(
                        "microvm_collection_in_progress",
                        Some(running.sandbox_identity_digest.clone()),
                        request_external_effect_possible(request),
                    ))
                }
                _ => {
                    return Err(uncertain_failure(
                        "microvm_collection_not_allowed",
                        Some(running.sandbox_identity_digest.clone()),
                        request_external_effect_possible(request),
                    ))
                }
            }
        }
        .ok_or_else(|| contract_failure("microvm_host_instance_missing"))?;
        match instance.collect(fence, request).await {
            Ok(collection) => {
                let mut state = entry.state.lock().await;
                if state.phase != MicroVmLifecyclePhase::Collecting {
                    state.phase = MicroVmLifecyclePhase::Uncertain;
                    return Err(uncertain_failure(
                        "microvm_collection_raced_with_control",
                        Some(running.sandbox_identity_digest.clone()),
                        request_external_effect_possible(request),
                    ));
                }
                state.phase = MicroVmLifecyclePhase::Collected;
                state.collection = Some(collection.clone());
                Ok(collection)
            }
            Err(failure) => {
                entry.state.lock().await.phase = MicroVmLifecyclePhase::Uncertain;
                Err(failure)
            }
        }
    }

    async fn terminate(
        &self,
        fence: &MicroVmProviderExecutionFence,
        command: &TerminateSandbox,
    ) -> Result<MicroVmTerminationProof, MicroVmProviderFailure> {
        let key = MicroVmAttemptKey::from_terminate(fence, command);
        let entry = self
            .open_or_cache(&key, Some(&command.sandbox_identity_digest))
            .await?;
        let instance = {
            let mut state = entry.state.lock().await;
            if let Some(proof) = &state.termination {
                return Ok(proof.clone());
            }
            state.phase = MicroVmLifecyclePhase::Terminating;
            state.instance.clone()
        }
        .ok_or_else(|| contract_failure("microvm_host_instance_missing"))?;
        let proof = instance.terminate(fence, command).await?;
        if proof.sandbox_identity_digest != command.sandbox_identity_digest {
            entry.state.lock().await.phase = MicroVmLifecyclePhase::Uncertain;
            return Err(uncertain_failure(
                "microvm_termination_identity_mismatch",
                Some(command.sandbox_identity_digest.clone()),
                true,
            ));
        }
        let mut state = entry.state.lock().await;
        state.phase = MicroVmLifecyclePhase::Terminated;
        state.termination = Some(proof.clone());
        Ok(proof)
    }

    async fn destroy(
        &self,
        fence: &MicroVmProviderExecutionFence,
        command: &DestroySandbox,
    ) -> Result<MicroVmCleanupProof, MicroVmProviderFailure> {
        let key = MicroVmAttemptKey::from_destroy(fence, command);
        let entry = self
            .open_or_cache(&key, Some(&command.sandbox_identity_digest))
            .await?;
        let instance = {
            let mut state = entry.state.lock().await;
            state.phase = MicroVmLifecyclePhase::Destroying;
            state.instance.clone()
        }
        .ok_or_else(|| contract_failure("microvm_host_instance_missing"))?;
        let proof = instance.destroy(fence, command).await?;
        if proof.sandbox_identity_digest != command.sandbox_identity_digest
            || !proof.grants_revoked
            || !proof.ephemeral_storage_destroyed
        {
            entry.state.lock().await.phase = MicroVmLifecyclePhase::Uncertain;
            return Err(uncertain_failure(
                "microvm_cleanup_incomplete",
                Some(command.sandbox_identity_digest.clone()),
                true,
            ));
        }
        self.remove(&key, &entry).await;
        Ok(proof)
    }

    async fn abort(
        &self,
        fence: &MicroVmProviderExecutionFence,
        command: &AbortSandboxExecution,
    ) -> Result<MicroVmAbortProof, MicroVmProviderFailure> {
        let key = MicroVmAttemptKey::from_abort(fence, command);
        let entry = self.open_or_cache(&key, None).await?;
        let instance = {
            let mut state = entry.state.lock().await;
            state.phase = MicroVmLifecyclePhase::Aborting;
            state.instance.clone()
        }
        .ok_or_else(|| contract_failure("microvm_host_instance_missing"))?;
        let proof = instance.abort(fence, command).await?;
        if proof.termination.sandbox_identity_digest != proof.sandbox_identity_digest
            || proof.cleanup.sandbox_identity_digest != proof.sandbox_identity_digest
            || !proof.termination.process_tree_terminated
            || !proof.termination.grants_revoked
            || !proof.cleanup.grants_revoked
            || !proof.cleanup.ephemeral_storage_destroyed
        {
            entry.state.lock().await.phase = MicroVmLifecyclePhase::Uncertain;
            return Err(uncertain_failure(
                "microvm_abort_incomplete",
                Some(proof.sandbox_identity_digest),
                true,
            ));
        }
        self.remove(&key, &entry).await;
        Ok(proof)
    }

    async fn recover_expired(
        &self,
        fence: &MicroVmProviderExecutionFence,
        expired: &ExpiredSandboxLease,
    ) -> Result<MicroVmRecoveryProof, MicroVmProviderFailure> {
        let key = MicroVmAttemptKey::from_expired(fence, expired);
        let entry = self.open_or_cache(&key, None).await?;
        let instance = {
            let mut state = entry.state.lock().await;
            state.phase = MicroVmLifecyclePhase::Recovering;
            state.instance.clone()
        }
        .ok_or_else(|| contract_failure("microvm_host_instance_missing"))?;
        let proof = instance.recover_expired(fence, expired).await?;
        if proof.cleanup.sandbox_identity_digest != proof.sandbox_identity_digest
            || !proof.cleanup.grants_revoked
            || !proof.cleanup.ephemeral_storage_destroyed
        {
            entry.state.lock().await.phase = MicroVmLifecyclePhase::Uncertain;
            return Err(uncertain_failure(
                "microvm_recovery_incomplete",
                Some(proof.sandbox_identity_digest),
                true,
            ));
        }
        self.remove(&key, &entry).await;
        Ok(proof)
    }
}

fn validate_prepared_host(
    prepared: &PreparedMicroVmHost,
    expected_identity: Option<&Sha256Digest>,
) -> Result<(), MicroVmProviderFailure> {
    let instance_identity = prepared.instance.sandbox_identity_digest();
    if instance_identity != prepared.evidence.sandbox_identity_digest
        || expected_identity.is_some_and(|expected| expected != &instance_identity)
    {
        return Err(contract_failure("microvm_factory_identity_mismatch"));
    }
    Ok(())
}

fn request_external_effect_possible(request: &SandboxExecutionRequest) -> bool {
    request.effect.risk_rank() >= insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
        || request.network_mode != SandboxNetworkMode::None
}

fn contract_failure(code: &str) -> MicroVmProviderFailure {
    MicroVmProviderFailure {
        safe_code: code.to_owned(),
        safe_message: "microVM lifecycle rejected an invalid physical transition".to_owned(),
        evidence_digest: evidence_digest(code),
        sandbox_identity_digest: None,
        execution_may_have_started: false,
        external_effect_possible: false,
    }
}

fn uncertain_failure(
    code: &str,
    sandbox_identity_digest: Option<Sha256Digest>,
    external_effect_possible: bool,
) -> MicroVmProviderFailure {
    MicroVmProviderFailure {
        safe_code: code.to_owned(),
        safe_message: "microVM lifecycle could not prove a safe physical transition".to_owned(),
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
    .expect("static microVM lifecycle evidence is canonical")
    .parse()
    .expect("canonical digest is SHA-256")
}
