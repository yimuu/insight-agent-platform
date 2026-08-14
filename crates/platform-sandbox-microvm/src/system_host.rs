use crate::{
    FirecrackerApiClient, FirecrackerGuestChannel, FirecrackerInstallation,
    InstalledMicroVmRuntime, JailerLaunchPlan, MicroVmAbortProof, MicroVmAttemptKey,
    MicroVmCleanupProof, MicroVmGuestCollection, MicroVmGuestCommandEnvelope, MicroVmGuestEvent,
    MicroVmHostFactory, MicroVmHostInstance, MicroVmProviderFailure, MicroVmRecoveryProof,
    MicroVmTerminationProof, PreparedMicroVm, PreparedMicroVmHost, RunningMicroVm,
    FIRECRACKER_GUEST_CONTROL_PORT,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{canonical_digest, ResourceId, Sha256Digest};
use insight_platform_sandbox::{
    AbortSandboxExecution, DestroySandbox, ExpiredSandboxLease, MicroVmGrantRevocationEvidence,
    MicroVmGrantRevoker, MicroVmProviderExecutionFence, RevokeMicroVmSandboxGrants,
    SandboxCleanupDisposition, SandboxExecutionRequest, SandboxNetworkMode, TerminateSandbox,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    error::Error,
    ffi::CString,
    fmt,
    os::unix::ffi::OsStrExt as _,
    os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    fs,
    io::AsyncWriteExt as _,
    process::{Child, Command},
    sync::Mutex,
    time::{sleep, timeout},
};

const HOST_RECORD_SCHEMA_VERSION: u32 = 1;
const MAX_FIRECRACKER_BINARY_BYTES: u64 = 256 * 1024 * 1024;
const MAX_KERNEL_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_ROOTFS_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const MIN_EPHEMERAL_IDENTITY: u32 = 100_000;
const MAX_EPHEMERAL_IDENTITIES: u32 = 1_000_000;
const MIN_TOMBSTONE_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_TOMBSTONE_RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);

#[derive(Debug, Clone)]
pub struct LinuxFirecrackerHostConfig {
    pub installation: FirecrackerInstallation,
    pub runtimes: Vec<InstalledMicroVmRuntime>,
    pub provider_process_generation_id: ResourceId,
    pub state_directory: PathBuf,
    pub ephemeral_uid_base: u32,
    pub ephemeral_gid_base: u32,
    pub ephemeral_identity_count: u32,
    pub maximum_instances: usize,
    pub maximum_tombstones: usize,
    pub tombstone_retention: Duration,
    pub api_timeout: Duration,
    pub guest_channel_timeout: Duration,
    pub socket_poll_interval: Duration,
    pub process_termination_timeout: Duration,
}

impl LinuxFirecrackerHostConfig {
    fn validate(&self) -> Result<(), LinuxFirecrackerHostError> {
        self.installation
            .validate()
            .map_err(|_| LinuxFirecrackerHostError::InvalidConfiguration)?;
        if self.provider_process_generation_id.kind()
            != insight_platform_contracts::ResourceKind::WorkerProcessGeneration
            || !closed_absolute_path(&self.state_directory)
            || self.state_directory == self.installation.chroot_base_directory
            || self.runtimes.is_empty()
            || self.runtimes.len() > 256
            || self
                .runtimes
                .iter()
                .any(|runtime| runtime.validate().is_err())
            || self.ephemeral_uid_base < MIN_EPHEMERAL_IDENTITY
            || self.ephemeral_gid_base < MIN_EPHEMERAL_IDENTITY
            || self.ephemeral_identity_count == 0
            || self.ephemeral_identity_count > MAX_EPHEMERAL_IDENTITIES
            || self
                .ephemeral_uid_base
                .checked_add(self.ephemeral_identity_count)
                .is_none()
            || self
                .ephemeral_gid_base
                .checked_add(self.ephemeral_identity_count)
                .is_none()
            || self.maximum_instances == 0
            || self.maximum_instances > 65_536
            || self.maximum_tombstones == 0
            || self.maximum_tombstones > 262_144
            || self.tombstone_retention < MIN_TOMBSTONE_RETENTION
            || self.tombstone_retention > MAX_TOMBSTONE_RETENTION
            || !bounded_timeout(self.api_timeout, 30)
            || !bounded_timeout(self.guest_channel_timeout, 30)
            || !bounded_timeout(self.socket_poll_interval, 1)
            || !bounded_timeout(self.process_termination_timeout, 30)
        {
            return Err(LinuxFirecrackerHostError::InvalidConfiguration);
        }
        let mut digests = self
            .runtimes
            .iter()
            .map(|runtime| runtime.runtime_digest.clone())
            .collect::<Vec<_>>();
        digests.sort();
        if digests.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(LinuxFirecrackerHostError::InvalidConfiguration);
        }
        Ok(())
    }
}

struct LinuxFirecrackerShared {
    config: LinuxFirecrackerHostConfig,
    runtimes: BTreeMap<Sha256Digest, InstalledMicroVmRuntime>,
    grant_authority: Arc<dyn MicroVmGrantRevoker>,
}

pub struct LinuxFirecrackerHostFactory {
    shared: Arc<LinuxFirecrackerShared>,
    instances: Mutex<BTreeMap<MicroVmAttemptKey, Arc<LinuxFirecrackerInstance>>>,
}

impl LinuxFirecrackerHostFactory {
    pub async fn install(
        config: LinuxFirecrackerHostConfig,
        grant_authority: Arc<dyn MicroVmGrantRevoker>,
    ) -> Result<Self, LinuxFirecrackerHostError> {
        if !cfg!(target_os = "linux") {
            return Err(LinuxFirecrackerHostError::LinuxRequired);
        }
        config.validate()?;
        verify_root_protected_file(
            &config.installation.firecracker_path,
            &config.installation.firecracker_digest,
            MAX_FIRECRACKER_BINARY_BYTES,
            true,
        )
        .await?;
        verify_root_protected_file(
            &config.installation.jailer_path,
            &config.installation.jailer_digest,
            MAX_FIRECRACKER_BINARY_BYTES,
            true,
        )
        .await?;
        verify_root_directory(&config.installation.chroot_base_directory).await?;
        verify_root_directory(&config.state_directory).await?;
        let mut runtimes = BTreeMap::new();
        for runtime in &config.runtimes {
            verify_root_protected_file(
                &runtime.guest_kernel_path,
                &runtime.guest_kernel_digest,
                MAX_KERNEL_BYTES,
                false,
            )
            .await?;
            verify_root_protected_file(
                &runtime.rootfs_path,
                &runtime.runtime_digest,
                MAX_ROOTFS_BYTES,
                false,
            )
            .await?;
            if fs::metadata(&runtime.rootfs_path)
                .await
                .map_err(|_| LinuxFirecrackerHostError::AssetUnavailable)?
                .len()
                != runtime.rootfs_bytes
            {
                return Err(LinuxFirecrackerHostError::AssetUntrusted);
            }
            runtimes.insert(runtime.runtime_digest.clone(), runtime.clone());
        }
        let factory = Self {
            shared: Arc::new(LinuxFirecrackerShared {
                config,
                runtimes,
                grant_authority,
            }),
            instances: Mutex::new(BTreeMap::new()),
        };
        factory.load_records().await?;
        Ok(factory)
    }

    async fn load_records(&self) -> Result<(), LinuxFirecrackerHostError> {
        let mut directory = fs::read_dir(&self.shared.config.state_directory)
            .await
            .map_err(|_| LinuxFirecrackerHostError::RegistryUnavailable)?;
        let mut loaded = BTreeMap::new();
        while let Some(entry) = directory
            .next_entry()
            .await
            .map_err(|_| LinuxFirecrackerHostError::RegistryUnavailable)?
        {
            let path = entry.path();
            if !entry
                .file_type()
                .await
                .map_err(|_| LinuxFirecrackerHostError::RegistryUnavailable)?
                .is_file()
                || path.extension().and_then(|value| value.to_str()) != Some("json")
            {
                return Err(LinuxFirecrackerHostError::RegistryCorrupt);
            }
            let bytes = read_bounded(&path, 64 * 1024).await?;
            let record: FirecrackerHostRecord = serde_json::from_slice(&bytes)
                .map_err(|_| LinuxFirecrackerHostError::RegistryCorrupt)?;
            if serde_jcs::to_vec(&record).map_err(|_| LinuxFirecrackerHostError::RegistryCorrupt)?
                != bytes
                || record.validate(&self.shared).is_err()
                || loaded.contains_key(&record.key)
            {
                return Err(LinuxFirecrackerHostError::RegistryCorrupt);
            }
            let tombstone_expired = record.phase == FirecrackerHostPhase::Destroyed
                && record
                    .updated_at
                    .checked_add_signed(
                        chrono::Duration::from_std(self.shared.config.tombstone_retention)
                            .map_err(|_| LinuxFirecrackerHostError::InvalidConfiguration)?,
                    )
                    .is_some_and(|expires_at| expires_at <= Utc::now());
            if tombstone_expired {
                fs::remove_file(&path)
                    .await
                    .map_err(|_| LinuxFirecrackerHostError::RegistryUnavailable)?;
                continue;
            }
            let instance = Arc::new(LinuxFirecrackerInstance::from_record(
                Arc::clone(&self.shared),
                record.clone(),
            ));
            loaded.insert(record.key, instance);
        }
        let active = loaded
            .values()
            .filter(|instance| !instance.destroyed.load(Ordering::Acquire))
            .count();
        let tombstones = loaded.len().saturating_sub(active);
        if active > self.shared.config.maximum_instances
            || tombstones > self.shared.config.maximum_tombstones
        {
            return Err(LinuxFirecrackerHostError::RegistryCorrupt);
        }
        *self.instances.lock().await = loaded;
        Ok(())
    }

    async fn prune_expired_tombstones(&self) -> Result<(), MicroVmProviderFailure> {
        let retention_millis = i64::try_from(self.shared.config.tombstone_retention.as_millis())
            .map_err(|_| contract_failure("microvm_tombstone_retention_overflow"))?;
        let cutoff = Utc::now()
            .timestamp_millis()
            .saturating_sub(retention_millis);
        let candidates = {
            let instances = self.instances.lock().await;
            instances
                .iter()
                .filter(|(_, instance)| {
                    instance.destroyed.load(Ordering::Acquire)
                        && instance.destroyed_at_millis.load(Ordering::Acquire) <= cutoff
                })
                .map(|(key, instance)| (key.clone(), Arc::clone(instance)))
                .collect::<Vec<_>>()
        };
        for (key, instance) in candidates {
            let path = record_path(
                &self.shared.config.state_directory,
                &instance.plan.instance_id,
            )?;
            match fs::remove_file(path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {
                    return Err(contract_failure("microvm_tombstone_prune_failed"));
                }
            }
            let mut instances = self.instances.lock().await;
            if instances
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, &instance))
            {
                instances.remove(&key);
            }
        }
        Ok(())
    }

    async fn allocate_instance(
        &self,
        fence: &MicroVmProviderExecutionFence,
        request: &SandboxExecutionRequest,
    ) -> Result<Arc<LinuxFirecrackerInstance>, MicroVmProviderFailure> {
        self.prune_expired_tombstones().await?;
        let key = MicroVmAttemptKey::from_request(fence, request);
        let runtime = self
            .shared
            .runtimes
            .get(&request.runtime.image_or_module_digest)
            .filter(|runtime| runtime.matches_request(request))
            .ok_or_else(|| contract_failure("microvm_runtime_not_installed"))?;
        let mut instances = self.instances.lock().await;
        if let Some(instance) = instances.get(&key) {
            return Ok(Arc::clone(instance));
        }
        let active = instances
            .values()
            .filter(|instance| !instance.destroyed.load(Ordering::Acquire))
            .count();
        let tombstones = instances.len().saturating_sub(active);
        if active >= self.shared.config.maximum_instances
            || tombstones >= self.shared.config.maximum_tombstones
        {
            return Err(contract_failure("microvm_provider_capacity_exhausted"));
        }
        let occupied = instances
            .values()
            .filter(|instance| !instance.destroyed.load(Ordering::Acquire))
            .map(|instance| instance.uid)
            .collect::<Vec<_>>();
        let offset =
            deterministic_identity_offset(&key, self.shared.config.ephemeral_identity_count)?;
        let mut selected = None;
        for probe in 0..self.shared.config.ephemeral_identity_count {
            let candidate_offset = (offset + probe) % self.shared.config.ephemeral_identity_count;
            let uid = self
                .shared
                .config
                .ephemeral_uid_base
                .checked_add(candidate_offset)
                .ok_or_else(|| contract_failure("microvm_identity_overflow"))?;
            if !occupied.contains(&uid) {
                selected = Some((
                    uid,
                    self.shared
                        .config
                        .ephemeral_gid_base
                        .checked_add(candidate_offset)
                        .ok_or_else(|| contract_failure("microvm_identity_overflow"))?,
                    candidate_offset,
                ));
                break;
            }
        }
        let (uid, gid, identity_offset) =
            selected.ok_or_else(|| contract_failure("microvm_identity_pool_exhausted"))?;
        let instance_id = instance_id(&key)?;
        let plan = JailerLaunchPlan::build(
            &self.shared.config.installation,
            request,
            instance_id.clone(),
            uid,
            gid,
        )
        .map_err(|_| contract_failure("microvm_jailer_plan_invalid"))?;
        let guest_cid = 3_u32
            .checked_add(identity_offset)
            .ok_or_else(|| contract_failure("microvm_guest_cid_overflow"))?;
        let sandbox_identity_digest = digest(&serde_json::json!({
            "domain": "linux_firecracker_sandbox_identity",
            "schema_version": 1,
            "key": key,
            "instance_id": instance_id,
            "uid": uid,
            "gid": gid,
            "guest_cid": guest_cid,
            "plan_digest": plan.plan_digest,
            "runtime_digest": runtime.runtime_digest,
            "guest_kernel_digest": runtime.guest_kernel_digest,
            "guest_agent_digest": runtime.guest_agent_digest,
            "provider_process_generation_id": self.shared.config.provider_process_generation_id,
        }))?;
        let prepare_evidence_digest = digest(&serde_json::json!({
            "domain": "linux_firecracker_prepare",
            "schema_version": 1,
            "sandbox_identity_digest": sandbox_identity_digest,
            "request_digest": request.request_digest,
            "plan_digest": plan.plan_digest,
            "firecracker_digest": self.shared.config.installation.firecracker_digest,
            "jailer_digest": self.shared.config.installation.jailer_digest,
        }))?;
        let record = FirecrackerHostRecord {
            schema_version: HOST_RECORD_SCHEMA_VERSION,
            key: key.clone(),
            instance_id,
            uid,
            gid,
            guest_cid,
            jail_root: path_string(&plan.jail_root)?,
            api_socket: path_string(&plan.api_socket)?,
            vsock_path: path_string(&plan.vsock_path)?,
            runtime_digest: runtime.runtime_digest.clone(),
            plan_digest: plan.plan_digest.clone(),
            sandbox_identity_digest,
            prepare_evidence_digest,
            start_evidence_digest: None,
            phase: FirecrackerHostPhase::Allocating,
            process_id: None,
            process_start_identity: None,
            termination_proof: None,
            cleanup_proof: None,
            updated_at: Utc::now(),
            record_digest: placeholder_digest(),
        }
        .seal()?;
        persist_record(&self.shared.config.state_directory, &record).await?;
        let instance = Arc::new(LinuxFirecrackerInstance::new(
            Arc::clone(&self.shared),
            record,
            plan,
            request.clone(),
        ));
        instances.insert(key, Arc::clone(&instance));
        Ok(instance)
    }
}

#[async_trait]
impl MicroVmHostFactory for LinuxFirecrackerHostFactory {
    async fn prepare_exact(
        &self,
        fence: &MicroVmProviderExecutionFence,
        request: &SandboxExecutionRequest,
    ) -> Result<PreparedMicroVmHost, MicroVmProviderFailure> {
        let instance = self.allocate_instance(fence, request).await?;
        instance.ensure_prepared(request).await?;
        let record = instance.record.lock().await;
        Ok(PreparedMicroVmHost {
            instance: instance.clone(),
            evidence: record.prepared_evidence(),
        })
    }

    async fn open_exact(
        &self,
        key: &MicroVmAttemptKey,
        expected_sandbox_identity_digest: Option<&Sha256Digest>,
    ) -> Result<Option<PreparedMicroVmHost>, MicroVmProviderFailure> {
        let instance = self.instances.lock().await.get(key).cloned();
        let Some(instance) = instance else {
            return Ok(None);
        };
        let record = instance.record.lock().await;
        if expected_sandbox_identity_digest
            .is_some_and(|expected| expected != &record.sandbox_identity_digest)
        {
            return Ok(None);
        }
        Ok(Some(PreparedMicroVmHost {
            instance: instance.clone(),
            evidence: record.prepared_evidence(),
        }))
    }
}

struct LinuxFirecrackerInstance {
    shared: Arc<LinuxFirecrackerShared>,
    sandbox_identity_digest: Sha256Digest,
    uid: u32,
    destroyed: AtomicBool,
    destroyed_at_millis: AtomicI64,
    record: Mutex<FirecrackerHostRecord>,
    plan: JailerLaunchPlan,
    request: Mutex<Option<SandboxExecutionRequest>>,
    operation: Mutex<()>,
    child: Mutex<Option<Child>>,
    channel: Mutex<Option<FirecrackerGuestChannel>>,
    collection: Mutex<Option<MicroVmGuestCollection>>,
}

impl LinuxFirecrackerInstance {
    fn new(
        shared: Arc<LinuxFirecrackerShared>,
        record: FirecrackerHostRecord,
        plan: JailerLaunchPlan,
        request: SandboxExecutionRequest,
    ) -> Self {
        let sandbox_identity_digest = record.sandbox_identity_digest.clone();
        let uid = record.uid;
        let destroyed = record.phase == FirecrackerHostPhase::Destroyed;
        let destroyed_at_millis = if destroyed {
            record.updated_at.timestamp_millis()
        } else {
            0
        };
        Self {
            shared,
            sandbox_identity_digest,
            uid,
            destroyed: AtomicBool::new(destroyed),
            destroyed_at_millis: AtomicI64::new(destroyed_at_millis),
            record: Mutex::new(record),
            plan,
            request: Mutex::new(Some(request)),
            operation: Mutex::new(()),
            child: Mutex::new(None),
            channel: Mutex::new(None),
            collection: Mutex::new(None),
        }
    }

    fn from_record(shared: Arc<LinuxFirecrackerShared>, record: FirecrackerHostRecord) -> Self {
        let sandbox_identity_digest = record.sandbox_identity_digest.clone();
        let uid = record.uid;
        let destroyed = record.phase == FirecrackerHostPhase::Destroyed;
        let destroyed_at_millis = if destroyed {
            record.updated_at.timestamp_millis()
        } else {
            0
        };
        let plan = JailerLaunchPlan {
            instance_id: record.instance_id.clone(),
            uid: record.uid,
            gid: record.gid,
            executable: shared.config.installation.jailer_path.clone(),
            arguments: Vec::new(),
            jail_root: PathBuf::from(&record.jail_root),
            api_socket: PathBuf::from(&record.api_socket),
            vsock_path: PathBuf::from(&record.vsock_path),
            plan_digest: record.plan_digest.clone(),
        };
        Self {
            shared,
            sandbox_identity_digest,
            uid,
            destroyed: AtomicBool::new(destroyed),
            destroyed_at_millis: AtomicI64::new(destroyed_at_millis),
            record: Mutex::new(record),
            plan,
            request: Mutex::new(None),
            operation: Mutex::new(()),
            child: Mutex::new(None),
            channel: Mutex::new(None),
            collection: Mutex::new(None),
        }
    }

    async fn ensure_prepared(
        &self,
        request: &SandboxExecutionRequest,
    ) -> Result<(), MicroVmProviderFailure> {
        let _operation = self.operation.lock().await;
        {
            let record = self.record.lock().await;
            match record.phase {
                FirecrackerHostPhase::Prepared
                | FirecrackerHostPhase::Starting
                | FirecrackerHostPhase::Running
                | FirecrackerHostPhase::Collected
                | FirecrackerHostPhase::Terminated => return Ok(()),
                FirecrackerHostPhase::Destroyed => {
                    return Err(contract_failure("microvm_attempt_already_destroyed"));
                }
                FirecrackerHostPhase::Allocating if record.process_id.is_some() => {
                    return Err(uncertain_failure(
                        "microvm_prepare_interrupted",
                        Some(record.sandbox_identity_digest.clone()),
                        request_external_effect_possible(request),
                    ));
                }
                FirecrackerHostPhase::Allocating => {}
            }
        }
        let runtime = self
            .shared
            .runtimes
            .get(&request.runtime.image_or_module_digest)
            .filter(|runtime| runtime.matches_request(request))
            .ok_or_else(|| contract_failure("microvm_runtime_not_installed"))?;
        let exact_plan = JailerLaunchPlan::build(
            &self.shared.config.installation,
            request,
            self.plan.instance_id.clone(),
            self.plan.uid,
            self.plan.gid,
        )
        .map_err(|_| contract_failure("microvm_recovered_jailer_plan_invalid"))?;
        if exact_plan.plan_digest != self.plan.plan_digest
            || exact_plan.jail_root != self.plan.jail_root
            || exact_plan.api_socket != self.plan.api_socket
            || exact_plan.vsock_path != self.plan.vsock_path
        {
            return Err(contract_failure("microvm_recovered_jailer_plan_mismatch"));
        }
        materialize_jail(&self.shared.config.installation, &exact_plan, runtime).await?;
        let mut command = Command::new(&exact_plan.executable);
        command
            .args(&exact_plan.arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        configure_child_isolation(&mut command)?;
        let mut child = command
            .spawn()
            .map_err(|_| contract_failure("microvm_jailer_spawn_failed"))?;
        let process_id = child
            .id()
            .ok_or_else(|| contract_failure("microvm_process_identity_missing"))?;
        let process_start_identity = match process_start_identity(process_id).await {
            Ok(identity) => identity,
            Err(error) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return Err(error);
            }
        };
        *self.child.lock().await = Some(child);
        {
            let mut record = self.record.lock().await;
            record.process_id = Some(process_id);
            record.process_start_identity = Some(process_start_identity);
            record.updated_at = Utc::now();
            record.seal_in_place()?;
            persist_record(&self.shared.config.state_directory, &record).await?;
        }
        wait_for_socket(
            &self.plan.api_socket,
            Duration::from_millis(request.resources.startup_milliseconds),
            self.shared.config.socket_poll_interval,
        )
        .await?;
        FirecrackerApiClient::new(self.plan.api_socket.clone(), self.shared.config.api_timeout)
            .map_err(|_| contract_failure("microvm_firecracker_api_invalid"))?
            .configure(request, self.record.lock().await.guest_cid)
            .await
            .map_err(|_| {
                uncertain_failure(
                    "microvm_firecracker_configuration_failed",
                    Some(self.sandbox_identity_digest()),
                    false,
                )
            })?;
        {
            let mut record = self.record.lock().await;
            record.phase = FirecrackerHostPhase::Prepared;
            record.updated_at = Utc::now();
            record.seal_in_place()?;
            persist_record(&self.shared.config.state_directory, &record).await?;
        }
        Ok(())
    }

    async fn kill_process_tree(&self) -> Result<bool, MicroVmProviderFailure> {
        let (process_id, process_start) = {
            let record = self.record.lock().await;
            (record.process_id, record.process_start_identity)
        };
        let Some(process_id) = process_id else {
            return Ok(true);
        };
        if process_absent(process_id).await? {
            let mut record = self.record.lock().await;
            record.process_id = None;
            record.process_start_identity = None;
            return Ok(true);
        }
        if process_start_identity(process_id).await.ok() != process_start {
            return Err(uncertain_failure(
                "microvm_process_identity_changed",
                Some(self.sandbox_identity_digest()),
                true,
            ));
        }
        terminate_process_group(process_id)?;
        if let Some(mut child) = self.child.lock().await.take() {
            let _ = timeout(self.shared.config.process_termination_timeout, child.wait()).await;
        }
        let deadline = tokio::time::Instant::now() + self.shared.config.process_termination_timeout;
        loop {
            if process_absent(process_id).await? {
                let mut record = self.record.lock().await;
                record.process_id = None;
                record.process_start_identity = None;
                return Ok(true);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(uncertain_failure(
                    "microvm_process_tree_still_alive",
                    Some(self.sandbox_identity_digest()),
                    true,
                ));
            }
            sleep(self.shared.config.socket_poll_interval).await;
        }
    }

    async fn revoke_grants(
        &self,
    ) -> Result<MicroVmGrantRevocationEvidence, MicroVmProviderFailure> {
        let (key, identity) = {
            let record = self.record.lock().await;
            (record.key.clone(), record.sandbox_identity_digest.clone())
        };
        self.shared
            .grant_authority
            .revoke_exact(RevokeMicroVmSandboxGrants {
                tenant_id: key.tenant_id,
                sandbox_job_id: key.sandbox_job_id,
                request_digest: key.request_digest,
                executor_worker_process_generation_id: key.executor_worker_process_generation_id,
                provider_process_generation_id: self
                    .shared
                    .config
                    .provider_process_generation_id
                    .clone(),
                sandbox_identity_digest: identity,
                attempt_no: key.attempt_no,
                lease_generation: key.lease_generation,
            })
            .await
            .map_err(|error| match error {
                insight_platform_sandbox::MicroVmGrantRevocationError::Unavailable => {
                    uncertain_failure(
                        "microvm_grant_authority_unavailable",
                        Some(self.sandbox_identity_digest()),
                        true,
                    )
                }
                insight_platform_sandbox::MicroVmGrantRevocationError::Rejected => {
                    uncertain_failure(
                        "microvm_grant_revocation_rejected",
                        Some(self.sandbox_identity_digest()),
                        true,
                    )
                }
            })
    }

    async fn cleanup_exact(&self) -> Result<MicroVmCleanupProof, MicroVmProviderFailure> {
        let _operation = self.operation.lock().await;
        if let Some(proof) = self.record.lock().await.cleanup_proof.clone() {
            return Ok(proof);
        }
        let grant_result = self.revoke_grants().await;
        let process_tree_result = self.kill_process_tree().await;
        let grant_proof = grant_result?;
        let process_tree_terminated = process_tree_result?;
        if !process_tree_terminated {
            return Err(uncertain_failure(
                "microvm_process_tree_not_terminated",
                Some(self.sandbox_identity_digest()),
                true,
            ));
        }
        let jail_root = self.plan.jail_root.clone();
        validate_destroy_path(&self.shared.config.installation, &self.plan, &jail_root)?;
        match fs::remove_dir_all(&jail_root).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {
                return Err(uncertain_failure(
                    "microvm_ephemeral_storage_destroy_failed",
                    Some(self.sandbox_identity_digest()),
                    true,
                ));
            }
        }
        let observed_at = Utc::now();
        let identity = self.sandbox_identity_digest();
        let evidence_digest = digest(&serde_json::json!({
            "domain": "linux_firecracker_cleanup",
            "schema_version": 1,
            "sandbox_identity_digest": identity,
            "grant_revocation_evidence_digest": grant_proof.evidence_digest,
            "process_tree_terminated": true,
            "ephemeral_storage_destroyed": true,
            "observed_at": observed_at,
        }))?;
        let proof = MicroVmCleanupProof {
            sandbox_identity_digest: identity,
            evidence_digest,
            disposition: SandboxCleanupDisposition::Destroyed,
            grants_revoked: true,
            ephemeral_storage_destroyed: true,
            observed_at,
        };
        {
            let mut record = self.record.lock().await;
            record.phase = FirecrackerHostPhase::Destroyed;
            record.cleanup_proof = Some(proof.clone());
            record.updated_at = observed_at;
            record.seal_in_place()?;
            persist_record(&self.shared.config.state_directory, &record).await?;
        }
        self.destroyed_at_millis
            .store(observed_at.timestamp_millis(), Ordering::Release);
        self.destroyed.store(true, Ordering::Release);
        Ok(proof)
    }
}

#[async_trait]
impl MicroVmHostInstance for LinuxFirecrackerInstance {
    fn sandbox_identity_digest(&self) -> Sha256Digest {
        self.sandbox_identity_digest.clone()
    }

    async fn start(
        &self,
        fence: &MicroVmProviderExecutionFence,
        request: &SandboxExecutionRequest,
    ) -> Result<RunningMicroVm, MicroVmProviderFailure> {
        let _operation = self.operation.lock().await;
        {
            let record = self.record.lock().await;
            if record.key != MicroVmAttemptKey::from_request(fence, request) {
                return Err(contract_failure("microvm_start_key_mismatch"));
            }
            if matches!(
                record.phase,
                FirecrackerHostPhase::Running | FirecrackerHostPhase::Collected
            ) {
                return record.running_evidence();
            }
            if record.phase != FirecrackerHostPhase::Prepared {
                return Err(uncertain_failure(
                    "microvm_start_phase_invalid",
                    Some(record.sandbox_identity_digest.clone()),
                    request_external_effect_possible(request),
                ));
            }
        }
        {
            let mut record = self.record.lock().await;
            record.phase = FirecrackerHostPhase::Starting;
            record.updated_at = Utc::now();
            record.seal_in_place()?;
            persist_record(&self.shared.config.state_directory, &record).await?;
        }
        FirecrackerApiClient::new(self.plan.api_socket.clone(), self.shared.config.api_timeout)
            .map_err(|_| contract_failure("microvm_firecracker_api_invalid"))?
            .start()
            .await
            .map_err(|_| {
                uncertain_failure(
                    "microvm_instance_start_failed",
                    Some(self.sandbox_identity_digest()),
                    request_external_effect_possible(request),
                )
            })?;
        wait_for_socket(
            &self.plan.vsock_path,
            Duration::from_millis(request.resources.startup_milliseconds),
            self.shared.config.socket_poll_interval,
        )
        .await?;
        let mut channel = FirecrackerGuestChannel::connect(
            &self.plan.vsock_path,
            FIRECRACKER_GUEST_CONTROL_PORT,
            self.shared.config.guest_channel_timeout,
        )
        .await
        .map_err(|_| {
            uncertain_failure(
                "microvm_guest_channel_unavailable",
                Some(self.sandbox_identity_digest()),
                request_external_effect_possible(request),
            )
        })?;
        let ready = channel.read_event().await.map_err(|_| {
            uncertain_failure(
                "microvm_guest_ready_missing",
                Some(self.sandbox_identity_digest()),
                request_external_effect_possible(request),
            )
        })?;
        ready.validate_for(request, 1).map_err(|_| {
            uncertain_failure(
                "microvm_guest_ready_invalid",
                Some(self.sandbox_identity_digest()),
                request_external_effect_possible(request),
            )
        })?;
        if !matches!(ready.event, MicroVmGuestEvent::Ready { .. }) {
            return Err(uncertain_failure(
                "microvm_guest_ready_wrong_event",
                Some(self.sandbox_identity_digest()),
                request_external_effect_possible(request),
            ));
        }
        channel
            .write_command(
                &MicroVmGuestCommandEnvelope::execute(request.clone())
                    .map_err(|_| contract_failure("microvm_guest_execute_command_invalid"))?,
            )
            .await
            .map_err(|_| {
                uncertain_failure(
                    "microvm_guest_execute_delivery_failed",
                    Some(self.sandbox_identity_digest()),
                    request_external_effect_possible(request),
                )
            })?;
        let start_evidence_digest = digest(&serde_json::json!({
            "domain": "linux_firecracker_start",
            "schema_version": 1,
            "sandbox_identity_digest": self.sandbox_identity_digest(),
            "request_digest": request.request_digest,
            "ready_envelope_digest": ready.envelope_digest,
        }))?;
        *self.channel.lock().await = Some(channel);
        *self.request.lock().await = Some(request.clone());
        let running = RunningMicroVm {
            sandbox_identity_digest: self.sandbox_identity_digest(),
            start_evidence_digest: start_evidence_digest.clone(),
        };
        {
            let mut record = self.record.lock().await;
            record.phase = FirecrackerHostPhase::Running;
            record.start_evidence_digest = Some(start_evidence_digest);
            record.updated_at = Utc::now();
            record.seal_in_place()?;
            persist_record(&self.shared.config.state_directory, &record).await?;
        }
        Ok(running)
    }

    async fn collect(
        &self,
        fence: &MicroVmProviderExecutionFence,
        request: &SandboxExecutionRequest,
    ) -> Result<MicroVmGuestCollection, MicroVmProviderFailure> {
        if self.record.lock().await.key != MicroVmAttemptKey::from_request(fence, request) {
            return Err(contract_failure("microvm_collect_key_mismatch"));
        }
        if let Some(collection) = self.collection.lock().await.clone() {
            return Ok(collection);
        }
        let mut channel = self.channel.lock().await;
        let channel = channel.as_mut().ok_or_else(|| {
            uncertain_failure(
                "microvm_collection_unavailable_after_provider_restart",
                Some(self.sandbox_identity_digest()),
                request_external_effect_possible(request),
            )
        })?;
        let event = channel.read_event().await.map_err(|_| {
            uncertain_failure(
                "microvm_guest_result_unavailable",
                Some(self.sandbox_identity_digest()),
                request_external_effect_possible(request),
            )
        })?;
        event.validate_for(request, 2).map_err(|_| {
            uncertain_failure(
                "microvm_guest_result_invalid",
                Some(self.sandbox_identity_digest()),
                request_external_effect_possible(request),
            )
        })?;
        let MicroVmGuestEvent::Result(collection) = event.event else {
            return Err(uncertain_failure(
                "microvm_guest_result_wrong_event",
                Some(self.sandbox_identity_digest()),
                request_external_effect_possible(request),
            ));
        };
        let collection = *collection;
        *self.collection.lock().await = Some(collection.clone());
        let mut record = self.record.lock().await;
        record.phase = FirecrackerHostPhase::Collected;
        record.updated_at = Utc::now();
        record.seal_in_place()?;
        persist_record(&self.shared.config.state_directory, &record).await?;
        Ok(collection)
    }

    async fn terminate(
        &self,
        fence: &MicroVmProviderExecutionFence,
        command: &TerminateSandbox,
    ) -> Result<MicroVmTerminationProof, MicroVmProviderFailure> {
        let _operation = self.operation.lock().await;
        let record = self.record.lock().await;
        if record.key != MicroVmAttemptKey::from_terminate(fence, command)
            || record.sandbox_identity_digest != command.sandbox_identity_digest
        {
            return Err(contract_failure("microvm_terminate_key_mismatch"));
        }
        if let Some(proof) = record.termination_proof.clone() {
            return Ok(proof);
        }
        drop(record);
        let guest_acknowledged = false;
        let grant_result = self.revoke_grants().await;
        let process_tree_result = self.kill_process_tree().await;
        let grant_proof = grant_result?;
        let process_tree_terminated = process_tree_result?;
        let evidence_digest = digest(&serde_json::json!({
            "domain": "linux_firecracker_termination",
            "schema_version": 1,
            "sandbox_identity_digest": command.sandbox_identity_digest,
            "request_digest": command.request_digest,
            "process_tree_terminated": process_tree_terminated,
            "grant_revocation_evidence_digest": grant_proof.evidence_digest,
        }))?;
        let proof = MicroVmTerminationProof {
            sandbox_identity_digest: command.sandbox_identity_digest.clone(),
            evidence_digest,
            guest_acknowledged,
            process_tree_terminated,
            grants_revoked: true,
            external_effect_possible: command.effect.risk_rank()
                >= insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
                || command.network_mode != SandboxNetworkMode::None,
        };
        let mut record = self.record.lock().await;
        record.phase = FirecrackerHostPhase::Terminated;
        record.termination_proof = Some(proof.clone());
        record.updated_at = Utc::now();
        record.seal_in_place()?;
        persist_record(&self.shared.config.state_directory, &record).await?;
        Ok(proof)
    }

    async fn destroy(
        &self,
        fence: &MicroVmProviderExecutionFence,
        command: &DestroySandbox,
    ) -> Result<MicroVmCleanupProof, MicroVmProviderFailure> {
        let record = self.record.lock().await;
        if record.key != MicroVmAttemptKey::from_destroy(fence, command)
            || command.sandbox_identity_digest != self.sandbox_identity_digest()
        {
            return Err(contract_failure("microvm_destroy_identity_mismatch"));
        }
        drop(record);
        self.cleanup_exact().await
    }

    async fn abort(
        &self,
        fence: &MicroVmProviderExecutionFence,
        command: &AbortSandboxExecution,
    ) -> Result<MicroVmAbortProof, MicroVmProviderFailure> {
        if self.record.lock().await.key != MicroVmAttemptKey::from_abort(fence, command) {
            return Err(contract_failure("microvm_abort_key_mismatch"));
        }
        if let Some(request) = self.request.lock().await.clone() {
            if let Some(channel) = self.channel.lock().await.as_mut() {
                if let Ok(cancel) = MicroVmGuestCommandEnvelope::cancel(&request, 2, command.reason)
                {
                    let _ = channel.write_command(&cancel).await;
                }
            }
        }
        let identity = self.sandbox_identity_digest();
        let termination = self
            .terminate(
                fence,
                &TerminateSandbox {
                    tenant_id: command.tenant_id.clone(),
                    sandbox_job_id: command.sandbox_job_id.clone(),
                    request_digest: command.request_digest.clone(),
                    sandbox_identity_digest: identity.clone(),
                    attempt_no: command.attempt_no,
                    lease_generation: command.lease_generation,
                    effect: command.effect,
                    network_mode: command.network_mode,
                    deadline: command.execution_deadline,
                },
            )
            .await?;
        let cleanup = self.cleanup_exact().await?;
        let evidence_digest = digest(&serde_json::json!({
            "domain": "linux_firecracker_abort",
            "schema_version": 1,
            "sandbox_identity_digest": identity,
            "reason": command.reason,
            "termination_evidence_digest": termination.evidence_digest,
            "cleanup_evidence_digest": cleanup.evidence_digest,
        }))?;
        Ok(MicroVmAbortProof {
            sandbox_identity_digest: identity,
            termination,
            cleanup,
            evidence_digest,
        })
    }

    async fn recover_expired(
        &self,
        fence: &MicroVmProviderExecutionFence,
        expired: &ExpiredSandboxLease,
    ) -> Result<MicroVmRecoveryProof, MicroVmProviderFailure> {
        if self.record.lock().await.key != MicroVmAttemptKey::from_expired(fence, expired) {
            return Err(contract_failure("microvm_recovery_key_mismatch"));
        }
        let identity = self.sandbox_identity_digest();
        let cleanup = self.cleanup_exact().await?;
        let evidence_digest = digest(&serde_json::json!({
            "domain": "linux_firecracker_expired_recovery",
            "schema_version": 1,
            "sandbox_identity_digest": identity,
            "request_digest": expired.request.request_digest,
            "observed_lease_generation": expired.observed_lease_generation,
            "physical_state": expired.physical_state,
            "cleanup_evidence_digest": cleanup.evidence_digest,
        }))?;
        Ok(MicroVmRecoveryProof {
            sandbox_identity_digest: identity,
            cleanup,
            evidence_digest,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FirecrackerHostPhase {
    Allocating,
    Prepared,
    Starting,
    Running,
    Collected,
    Terminated,
    Destroyed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FirecrackerHostRecord {
    schema_version: u32,
    key: MicroVmAttemptKey,
    instance_id: String,
    uid: u32,
    gid: u32,
    guest_cid: u32,
    jail_root: String,
    api_socket: String,
    vsock_path: String,
    runtime_digest: Sha256Digest,
    plan_digest: Sha256Digest,
    sandbox_identity_digest: Sha256Digest,
    prepare_evidence_digest: Sha256Digest,
    start_evidence_digest: Option<Sha256Digest>,
    phase: FirecrackerHostPhase,
    process_id: Option<u32>,
    process_start_identity: Option<u64>,
    termination_proof: Option<MicroVmTerminationProof>,
    cleanup_proof: Option<MicroVmCleanupProof>,
    updated_at: DateTime<Utc>,
    record_digest: Sha256Digest,
}

impl FirecrackerHostRecord {
    fn seal(mut self) -> Result<Self, MicroVmProviderFailure> {
        self.seal_in_place()?;
        Ok(self)
    }

    fn seal_in_place(&mut self) -> Result<(), MicroVmProviderFailure> {
        self.record_digest = digest_without_field(self, "record_digest")?;
        Ok(())
    }

    fn validate(&self, shared: &LinuxFirecrackerShared) -> Result<(), MicroVmProviderFailure> {
        let expected_root = shared
            .config
            .installation
            .chroot_base_directory
            .join("firecracker")
            .join(&self.instance_id)
            .join("root");
        let valid_termination = self.termination_proof.as_ref().is_none_or(|proof| {
            proof.sandbox_identity_digest == self.sandbox_identity_digest
                && proof.process_tree_terminated
                && proof.grants_revoked
        });
        let valid_cleanup = self.cleanup_proof.as_ref().is_none_or(|proof| {
            proof.sandbox_identity_digest == self.sandbox_identity_digest
                && proof.disposition == SandboxCleanupDisposition::Destroyed
                && proof.grants_revoked
                && proof.ephemeral_storage_destroyed
        });
        if self.schema_version != HOST_RECORD_SCHEMA_VERSION
            || self.key.attempt_no == 0
            || self.key.lease_generation == 0
            || self.key.executor_worker_process_generation_id.kind()
                != insight_platform_contracts::ResourceKind::WorkerProcessGeneration
            || self.uid < shared.config.ephemeral_uid_base
            || self.gid < shared.config.ephemeral_gid_base
            || self.uid
                >= shared
                    .config
                    .ephemeral_uid_base
                    .saturating_add(shared.config.ephemeral_identity_count)
            || self.gid
                >= shared
                    .config
                    .ephemeral_gid_base
                    .saturating_add(shared.config.ephemeral_identity_count)
            || self.guest_cid < 3
            || Path::new(&self.jail_root) != expected_root
            || Path::new(&self.api_socket) != expected_root.join("run/firecracker.socket")
            || Path::new(&self.vsock_path) != expected_root.join("run/guest.vsock")
            || !shared.runtimes.contains_key(&self.runtime_digest)
            || self.process_id.is_some() != self.process_start_identity.is_some()
            || (matches!(
                self.phase,
                FirecrackerHostPhase::Running | FirecrackerHostPhase::Collected
            ) && self.start_evidence_digest.is_none())
            || (matches!(
                self.phase,
                FirecrackerHostPhase::Allocating | FirecrackerHostPhase::Prepared
            ) && self.start_evidence_digest.is_some())
            || (self.termination_proof.is_some()
                && !matches!(
                    self.phase,
                    FirecrackerHostPhase::Terminated | FirecrackerHostPhase::Destroyed
                ))
            || (self.phase == FirecrackerHostPhase::Terminated && self.termination_proof.is_none())
            || self.cleanup_proof.is_some() != (self.phase == FirecrackerHostPhase::Destroyed)
            || (matches!(
                self.phase,
                FirecrackerHostPhase::Prepared
                    | FirecrackerHostPhase::Starting
                    | FirecrackerHostPhase::Running
                    | FirecrackerHostPhase::Collected
            ) && self.process_id.is_none())
            || (matches!(
                self.phase,
                FirecrackerHostPhase::Terminated | FirecrackerHostPhase::Destroyed
            ) && self.process_id.is_some())
            || !valid_termination
            || !valid_cleanup
            || instance_id(&self.key)? != self.instance_id
            || digest_without_field(self, "record_digest")? != self.record_digest
        {
            return Err(contract_failure("microvm_host_record_invalid"));
        }
        Ok(())
    }

    fn prepared_evidence(&self) -> PreparedMicroVm {
        PreparedMicroVm {
            sandbox_identity_digest: self.sandbox_identity_digest.clone(),
            prepare_evidence_digest: self.prepare_evidence_digest.clone(),
        }
    }

    fn running_evidence(&self) -> Result<RunningMicroVm, MicroVmProviderFailure> {
        Ok(RunningMicroVm {
            sandbox_identity_digest: self.sandbox_identity_digest.clone(),
            start_evidence_digest: self.start_evidence_digest.clone().ok_or_else(|| {
                uncertain_failure(
                    "microvm_start_evidence_missing",
                    Some(self.sandbox_identity_digest.clone()),
                    true,
                )
            })?,
        })
    }
}

async fn materialize_jail(
    installation: &FirecrackerInstallation,
    plan: &JailerLaunchPlan,
    runtime: &InstalledMicroVmRuntime,
) -> Result<(), MicroVmProviderFailure> {
    validate_destroy_path(installation, plan, &plan.jail_root)?;
    match fs::symlink_metadata(&plan.jail_root).await {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            fs::remove_dir_all(&plan.jail_root)
                .await
                .map_err(|_| contract_failure("microvm_partial_jail_cleanup_failed"))?;
        }
        Ok(_) => return Err(contract_failure("microvm_jail_root_not_directory")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(contract_failure("microvm_jail_root_unavailable")),
    }
    fs::create_dir_all(plan.jail_root.join("run"))
        .await
        .map_err(|_| contract_failure("microvm_jail_create_failed"))?;
    let kernel = plan.jail_root.join("kernel");
    let rootfs = plan.jail_root.join("rootfs.ext4");
    copy_exact(
        &runtime.guest_kernel_path,
        &kernel,
        MAX_KERNEL_BYTES,
        &runtime.guest_kernel_digest,
    )
    .await?;
    copy_exact(
        &runtime.rootfs_path,
        &rootfs,
        runtime.rootfs_bytes,
        &runtime.runtime_digest,
    )
    .await?;
    fs::set_permissions(&kernel, std::fs::Permissions::from_mode(0o400))
        .await
        .map_err(|_| contract_failure("microvm_kernel_permissions_failed"))?;
    fs::set_permissions(&rootfs, std::fs::Permissions::from_mode(0o600))
        .await
        .map_err(|_| contract_failure("microvm_rootfs_permissions_failed"))?;
    change_owner(&kernel, plan.uid, plan.gid)?;
    change_owner(&rootfs, plan.uid, plan.gid)?;
    Ok(())
}

async fn copy_exact(
    source: &Path,
    destination: &Path,
    maximum: u64,
    expected_digest: &Sha256Digest,
) -> Result<(), MicroVmProviderFailure> {
    let metadata = fs::symlink_metadata(source)
        .await
        .map_err(|_| contract_failure("microvm_asset_unavailable"))?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > maximum
    {
        return Err(contract_failure("microvm_asset_size_invalid"));
    }
    let copied = fs::copy(source, destination)
        .await
        .map_err(|_| contract_failure("microvm_asset_copy_failed"))?;
    if copied != metadata.len() {
        return Err(contract_failure("microvm_asset_copy_incomplete"));
    }
    let actual = digest_file(destination, maximum)
        .await
        .map_err(|_| contract_failure("microvm_copied_asset_digest_failed"))?;
    if &actual != expected_digest {
        return Err(contract_failure("microvm_copied_asset_digest_mismatch"));
    }
    Ok(())
}

async fn persist_record(
    state_directory: &Path,
    record: &FirecrackerHostRecord,
) -> Result<(), MicroVmProviderFailure> {
    let bytes = serde_jcs::to_vec(record)
        .map_err(|_| contract_failure("microvm_host_record_canonicalization_failed"))?;
    if bytes.len() > 64 * 1024 {
        return Err(contract_failure("microvm_host_record_too_large"));
    }
    let target = record_path(state_directory, &record.instance_id)?;
    let temporary = state_directory.join(format!("{}.tmp", record.instance_id));
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .await
        .map_err(|_| contract_failure("microvm_host_record_write_failed"))?;
    file.write_all(&bytes)
        .await
        .map_err(|_| contract_failure("microvm_host_record_write_failed"))?;
    file.sync_all()
        .await
        .map_err(|_| contract_failure("microvm_host_record_sync_failed"))?;
    drop(file);
    fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))
        .await
        .map_err(|_| contract_failure("microvm_host_record_permissions_failed"))?;
    fs::rename(&temporary, &target)
        .await
        .map_err(|_| contract_failure("microvm_host_record_commit_failed"))?;
    let directory = state_directory.to_path_buf();
    tokio::task::spawn_blocking(move || {
        std::fs::File::open(directory).and_then(|directory| directory.sync_all())
    })
    .await
    .map_err(|_| contract_failure("microvm_host_registry_sync_failed"))?
    .map_err(|_| contract_failure("microvm_host_registry_sync_failed"))
}

async fn wait_for_socket(
    path: &Path,
    maximum: Duration,
    interval: Duration,
) -> Result<(), MicroVmProviderFailure> {
    timeout(maximum, async {
        loop {
            if fs::metadata(path)
                .await
                .is_ok_and(|metadata| metadata.file_type().is_socket())
            {
                return;
            }
            sleep(interval).await;
        }
    })
    .await
    .map_err(|_| uncertain_failure("microvm_socket_startup_timeout", None, false))
}

async fn verify_root_protected_file(
    path: &Path,
    expected_digest: &Sha256Digest,
    maximum_bytes: u64,
    executable: bool,
) -> Result<(), LinuxFirecrackerHostError> {
    let metadata = fs::symlink_metadata(path)
        .await
        .map_err(|_| LinuxFirecrackerHostError::AssetUnavailable)?;
    let mode = metadata.mode();
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > maximum_bytes
        || metadata.uid() != 0
        || mode & 0o022 != 0
        || executable && mode & 0o111 == 0
    {
        return Err(LinuxFirecrackerHostError::AssetUntrusted);
    }
    let actual = digest_file(path, maximum_bytes).await?;
    if &actual != expected_digest {
        return Err(LinuxFirecrackerHostError::AssetUntrusted);
    }
    Ok(())
}

async fn verify_root_directory(path: &Path) -> Result<(), LinuxFirecrackerHostError> {
    let metadata = fs::symlink_metadata(path)
        .await
        .map_err(|_| LinuxFirecrackerHostError::RegistryUnavailable)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != 0
        || metadata.mode() & 0o077 != 0
    {
        return Err(LinuxFirecrackerHostError::AssetUntrusted);
    }
    Ok(())
}

async fn digest_file(
    path: &Path,
    maximum_bytes: u64,
) -> Result<Sha256Digest, LinuxFirecrackerHostError> {
    use tokio::io::AsyncReadExt as _;
    let mut file = fs::File::open(path)
        .await
        .map_err(|_| LinuxFirecrackerHostError::AssetUnavailable)?;
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .await
            .map_err(|_| LinuxFirecrackerHostError::AssetUnavailable)?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(
                u64::try_from(count).map_err(|_| LinuxFirecrackerHostError::AssetUntrusted)?,
            )
            .ok_or(LinuxFirecrackerHostError::AssetUntrusted)?;
        if total > maximum_bytes {
            return Err(LinuxFirecrackerHostError::AssetUntrusted);
        }
        digest.update(&buffer[..count]);
    }
    let encoded = digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("sha256:{encoded}")
        .parse()
        .map_err(|_| LinuxFirecrackerHostError::AssetUntrusted)
}

async fn read_bounded(
    path: &Path,
    maximum_bytes: usize,
) -> Result<Vec<u8>, LinuxFirecrackerHostError> {
    use tokio::io::AsyncReadExt as _;
    let file = fs::File::open(path)
        .await
        .map_err(|_| LinuxFirecrackerHostError::RegistryUnavailable)?;
    let mut bytes = Vec::new();
    file.take(u64::try_from(maximum_bytes + 1).unwrap())
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| LinuxFirecrackerHostError::RegistryUnavailable)?;
    if bytes.is_empty() || bytes.len() > maximum_bytes {
        return Err(LinuxFirecrackerHostError::RegistryCorrupt);
    }
    Ok(bytes)
}

fn configure_child_isolation(command: &mut Command) -> Result<(), MicroVmProviderFailure> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt as _;
        // SAFETY: this closure executes after fork and before exec and invokes only async-signal-
        // safe libc syscalls. It creates a new session/process group and an empty network
        // namespace before the exact, verified jailer binary is executed.
        unsafe {
            command.as_std_mut().pre_exec(|| {
                if libc::setsid() < 0 || libc::unshare(libc::CLONE_NEWNET) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = command;
        Err(contract_failure("microvm_linux_host_required"))
    }
}

fn terminate_process_group(process_id: u32) -> Result<(), MicroVmProviderFailure> {
    #[cfg(target_os = "linux")]
    {
        let pid = i32::try_from(process_id)
            .map_err(|_| contract_failure("microvm_process_identity_invalid"))?;
        // SAFETY: the PID was obtained from the spawned child and revalidated against its procfs
        // start identity immediately before this call. Negative PID targets only its new session.
        let result = unsafe { libc::kill(-pid, libc::SIGKILL) };
        if result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
            Ok(())
        } else {
            Err(uncertain_failure(
                "microvm_process_termination_failed",
                None,
                true,
            ))
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = process_id;
        Err(contract_failure("microvm_linux_host_required"))
    }
}

async fn process_start_identity(process_id: u32) -> Result<u64, MicroVmProviderFailure> {
    #[cfg(target_os = "linux")]
    {
        let stat = fs::read_to_string(format!("/proc/{process_id}/stat"))
            .await
            .map_err(|_| contract_failure("microvm_process_not_found"))?;
        let closing = stat
            .rfind(") ")
            .ok_or_else(|| contract_failure("microvm_proc_stat_invalid"))?;
        stat[closing + 2..]
            .split_ascii_whitespace()
            .nth(19)
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .ok_or_else(|| contract_failure("microvm_proc_start_identity_invalid"))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = process_id;
        Err(contract_failure("microvm_linux_host_required"))
    }
}

async fn process_absent(process_id: u32) -> Result<bool, MicroVmProviderFailure> {
    #[cfg(target_os = "linux")]
    {
        match fs::metadata(format!("/proc/{process_id}")).await {
            Ok(metadata) => Ok(!metadata.is_dir()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
            Err(_) => Err(uncertain_failure(
                "microvm_process_presence_unavailable",
                None,
                true,
            )),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = process_id;
        Err(contract_failure("microvm_linux_host_required"))
    }
}

fn change_owner(path: &Path, uid: u32, gid: u32) -> Result<(), MicroVmProviderFailure> {
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| contract_failure("microvm_asset_path_invalid"))?;
    // SAFETY: the path is an owned NUL-free CString under the validated per-instance jail root.
    if unsafe { libc::chown(path.as_ptr(), uid, gid) } != 0 {
        return Err(contract_failure("microvm_asset_owner_failed"));
    }
    Ok(())
}

fn validate_destroy_path(
    installation: &FirecrackerInstallation,
    plan: &JailerLaunchPlan,
    path: &Path,
) -> Result<(), MicroVmProviderFailure> {
    let expected = installation
        .chroot_base_directory
        .join("firecracker")
        .join(&plan.instance_id)
        .join("root");
    if path != expected || !path.starts_with(&installation.chroot_base_directory) {
        return Err(contract_failure("microvm_cleanup_path_invalid"));
    }
    Ok(())
}

fn deterministic_identity_offset(
    key: &MicroVmAttemptKey,
    count: u32,
) -> Result<u32, MicroVmProviderFailure> {
    let canonical = serde_jcs::to_vec(key)
        .map_err(|_| contract_failure("microvm_identity_canonicalization_failed"))?;
    let digest = Sha256::digest(canonical);
    Ok(u32::from_be_bytes(digest[..4].try_into().unwrap()) % count)
}

fn instance_id(key: &MicroVmAttemptKey) -> Result<String, MicroVmProviderFailure> {
    let canonical = serde_jcs::to_vec(key)
        .map_err(|_| contract_failure("microvm_instance_id_canonicalization_failed"))?;
    let digest = Sha256::digest(canonical);
    Ok(format!("sj-{}", hex_prefix(&digest, 30)))
}

fn hex_prefix(bytes: &[u8], count: usize) -> String {
    bytes
        .iter()
        .take(count.div_ceil(2))
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
        .chars()
        .take(count)
        .collect()
}

fn record_path(
    state_directory: &Path,
    instance_id: &str,
) -> Result<PathBuf, MicroVmProviderFailure> {
    if instance_id.is_empty()
        || instance_id.len() > 64
        || !instance_id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(contract_failure("microvm_instance_id_invalid"));
    }
    Ok(state_directory.join(format!("{instance_id}.json")))
}

fn path_string(path: &Path) -> Result<String, MicroVmProviderFailure> {
    path.to_str()
        .filter(|value| !value.is_empty() && value.len() <= 4_096)
        .map(ToOwned::to_owned)
        .ok_or_else(|| contract_failure("microvm_path_invalid"))
}

fn closed_absolute_path(path: &Path) -> bool {
    path.is_absolute()
        && path.as_os_str().as_bytes().len() <= 4_096
        && path.components().all(|component| {
            !matches!(
                component,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
}

fn bounded_timeout(value: Duration, maximum_seconds: u64) -> bool {
    !value.is_zero() && value <= Duration::from_secs(maximum_seconds)
}

fn request_external_effect_possible(request: &SandboxExecutionRequest) -> bool {
    request.effect.risk_rank() >= insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
        || request.network_mode != SandboxNetworkMode::None
}

fn digest(value: &serde_json::Value) -> Result<Sha256Digest, MicroVmProviderFailure> {
    canonical_digest(value)
        .map_err(|_| contract_failure("microvm_evidence_canonicalization_failed"))?
        .parse()
        .map_err(|_| contract_failure("microvm_evidence_digest_invalid"))
}

fn digest_without_field<T: Serialize>(
    value: &T,
    field: &str,
) -> Result<Sha256Digest, MicroVmProviderFailure> {
    let mut value = serde_json::to_value(value)
        .map_err(|_| contract_failure("microvm_record_canonicalization_failed"))?;
    let serde_json::Value::Object(object) = &mut value else {
        return Err(contract_failure("microvm_record_shape_invalid"));
    };
    object.remove(field);
    digest(&value)
}

fn contract_failure(code: &str) -> MicroVmProviderFailure {
    MicroVmProviderFailure {
        safe_code: code.to_owned(),
        safe_message: "microVM host rejected an invalid or unavailable physical contract"
            .to_owned(),
        evidence_digest: static_evidence(code),
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
        safe_message: "microVM host could not prove a safe physical result".to_owned(),
        evidence_digest: static_evidence(code),
        sandbox_identity_digest,
        execution_may_have_started: true,
        external_effect_possible,
    }
}

fn static_evidence(code: &str) -> Sha256Digest {
    canonical_digest(&serde_json::json!({"domain": code, "schema_version": 1}))
        .expect("static microVM host evidence is canonical")
        .parse()
        .expect("canonical digest is SHA-256")
}

fn placeholder_digest() -> Sha256Digest {
    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        .parse()
        .expect("static placeholder digest is valid")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinuxFirecrackerHostError {
    LinuxRequired,
    InvalidConfiguration,
    AssetUnavailable,
    AssetUntrusted,
    RegistryUnavailable,
    RegistryCorrupt,
}

impl fmt::Display for LinuxFirecrackerHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::LinuxRequired => "the Firecracker provider requires a Linux KVM host",
            Self::InvalidConfiguration => "the Firecracker host configuration is invalid",
            Self::AssetUnavailable => "a Firecracker runtime asset is unavailable",
            Self::AssetUntrusted => "a Firecracker runtime asset is not root-protected or exact",
            Self::RegistryUnavailable => "the Firecracker host registry is unavailable",
            Self::RegistryCorrupt => "the Firecracker host registry is corrupt",
        })
    }
}

impl Error for LinuxFirecrackerHostError {}
