use crate::system_host::{
    configure_child_isolation, contract_failure as host_contract_failure, materialize_jail,
    path_string, prepare_root_directory, prepare_state_subdirectory, process_absent,
    process_start_identity, read_bounded, terminate_process_group, uncertain_failure,
    validate_destroy_path, verify_root_protected_file, wait_for_socket,
};
use crate::{
    FirecrackerApiClient, FirecrackerGuestChannel, JailerLaunchPlan, LinuxFirecrackerHostConfig,
    LinuxFirecrackerHostError, ManagedMcpGuestCommandEnvelope, ManagedMcpGuestEvent,
    ManagedMcpGuestSecretHeader, ManagedMcpMicroVmSessionLifecyclePort, MicroVmProviderCapacity,
    MicroVmProviderFailure, FIRECRACKER_GUEST_CONTROL_PORT, MAX_MICROVM_GUEST_ARTIFACT_CHUNK_BYTES,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, ArtifactGrantOperation, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_egress::{ManagedMcpSandboxSecretBroker, ManagedMcpSandboxSecretBrokerError};
use insight_platform_jobs::JobFence;
use insight_platform_sandbox::{
    ActivatedManagedMcpSandboxSession, ManagedMcpSandboxOpaqueSessionClaims,
    ManagedMcpSandboxSecretDeliveryRequest, ManagedMcpSandboxSessionReadyEvidence,
    ManagedMcpSandboxSessionRequest, ManagedMcpSandboxSessionStateSealError,
    ManagedMcpSandboxSessionStateSealer, MicroVmArtifactBroker, MicroVmArtifactBrokerError,
    MicroVmArtifactReadPurpose, MicroVmArtifactReadRequest, MicroVmGrantRevoker,
    MicroVmSandboxWorkloadKind, PreparedManagedMcpSandboxSession,
    PreparedManagedMcpSandboxSessionActivation, RevokeMicroVmSandboxGrants,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    os::unix::fs::PermissionsExt as _,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
};
use tokio::{
    fs,
    process::{Child, Command},
    sync::Mutex,
    time::{sleep, timeout},
};

const MANAGED_RECORD_SCHEMA_VERSION: u32 = 1;
const MAX_MANAGED_RECORD_BYTES: usize = 256 * 1024;
const MAX_FIRECRACKER_BINARY_BYTES: u64 = 256 * 1024 * 1024;
const MAX_KERNEL_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_ROOTFS_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const MANAGED_MCP_PHYSICAL_ATTEMPT_NO: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpMicroVmSessionKey {
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub session_identity_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub executor_worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
}

impl ManagedMcpMicroVmSessionKey {
    pub fn from_request(request: &ManagedMcpSandboxSessionRequest, fence: &JobFence) -> Self {
        Self {
            tenant_id: request.identity.tenant_id.clone(),
            sandbox_job_id: request.identity.sandbox_job_id.clone(),
            session_identity_digest: request.identity.canonical_digest.clone(),
            request_digest: request.request_digest.clone(),
            executor_worker_process_generation_id: fence.worker_process_generation_id.clone(),
            lease_generation: fence.lease_generation,
        }
    }

    fn validate(&self) -> Result<(), MicroVmProviderFailure> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.sandbox_job_id.kind() != ResourceKind::SandboxJob
            || self.executor_worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.lease_generation == 0
        {
            return Err(host_contract_failure("managed_mcp_microvm_key_invalid"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedMcpSecretDeliveryPlan {
    secret_binding_id: ResourceId,
    receipt_id: ResourceId,
    event_id: ResourceId,
    outbox_id: ResourceId,
    idempotency_key_digest: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ManagedMcpFirecrackerPhase {
    Allocating,
    Prepared,
    Initializing,
    Initialized,
    Activated,
    Destroyed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedMcpFirecrackerRecord {
    schema_version: u32,
    key: ManagedMcpMicroVmSessionKey,
    provider_process_generation_id: ResourceId,
    executor_identity_digest: Sha256Digest,
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
    secret_delivery_plans: Vec<ManagedMcpSecretDeliveryPlan>,
    next_command_sequence: u64,
    next_event_sequence: u64,
    phase: ManagedMcpFirecrackerPhase,
    process_id: Option<u32>,
    process_start_identity: Option<u64>,
    activation: Option<PreparedManagedMcpSandboxSessionActivation>,
    activated: Option<ActivatedManagedMcpSandboxSession>,
    cleanup_evidence_digest: Option<Sha256Digest>,
    updated_at: DateTime<Utc>,
    destroyed_at: Option<DateTime<Utc>>,
    record_digest: Sha256Digest,
}

impl ManagedMcpFirecrackerRecord {
    fn seal(mut self) -> Result<Self, MicroVmProviderFailure> {
        self.seal_in_place()?;
        Ok(self)
    }

    fn seal_in_place(&mut self) -> Result<(), MicroVmProviderFailure> {
        self.record_digest = digest_without_field(self, "record_digest")?;
        Ok(())
    }

    fn prepared(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
    ) -> Result<PreparedManagedMcpSandboxSession, MicroVmProviderFailure> {
        PreparedManagedMcpSandboxSession {
            schema_version: 1,
            identity: request.identity.clone(),
            request_digest: self.key.request_digest.clone(),
            worker_process_generation_id: self.key.executor_worker_process_generation_id.clone(),
            provider_process_generation_id: self.provider_process_generation_id.clone(),
            lease_generation: self.key.lease_generation,
            executor_identity_digest: self.executor_identity_digest.clone(),
            sandbox_identity_digest: self.sandbox_identity_digest.clone(),
            prepare_evidence_digest: self.prepare_evidence_digest.clone(),
            canonical_digest: placeholder_digest(),
        }
        .seal()
        .map_err(|_| host_contract_failure("managed_mcp_microvm_prepared_seal_failed"))
    }

    fn validate(
        &self,
        config: &LinuxFirecrackerHostConfig,
        runtimes: &BTreeMap<Sha256Digest, crate::InstalledMicroVmRuntime>,
    ) -> Result<(), MicroVmProviderFailure> {
        self.key.validate()?;
        let expected_root = config
            .installation
            .chroot_base_directory
            .join("firecracker")
            .join(&self.instance_id)
            .join("root");
        let mut secret_bindings = BTreeSet::new();
        let plans_valid = !self.secret_delivery_plans.is_empty()
            && self.secret_delivery_plans.iter().all(|plan| {
                plan.secret_binding_id.kind() == ResourceKind::SecretBinding
                    && plan.receipt_id.kind() == ResourceKind::Receipt
                    && plan.event_id.kind() == ResourceKind::Event
                    && plan.outbox_id.kind() == ResourceKind::OutboxEvent
                    && secret_bindings.insert(plan.secret_binding_id.clone())
            });
        let activation_valid = self.activation.as_ref().is_none_or(|activation| {
            activation.clone().seal().ok().as_ref() == Some(activation)
                && activation.prepared.request_digest == self.key.request_digest
                && activation.prepared.sandbox_identity_digest == self.sandbox_identity_digest
        });
        let activated_valid = self.activated.as_ref().is_none_or(|activated| {
            activated.clone().seal().ok().as_ref() == Some(activated)
                && activated.request_digest == self.key.request_digest
                && activated.sandbox_identity_digest == self.sandbox_identity_digest
        });
        if self.schema_version != MANAGED_RECORD_SCHEMA_VERSION
            || self.provider_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.uid < config.ephemeral_uid_base
            || self.gid < config.ephemeral_gid_base
            || self.uid
                >= config
                    .ephemeral_uid_base
                    .saturating_add(config.ephemeral_identity_count)
            || self.gid
                >= config
                    .ephemeral_gid_base
                    .saturating_add(config.ephemeral_identity_count)
            || self.guest_cid < 3
            || Path::new(&self.jail_root) != expected_root
            || Path::new(&self.api_socket) != expected_root.join("run/firecracker.socket")
            || Path::new(&self.vsock_path) != expected_root.join("run/guest.vsock")
            || !runtimes.contains_key(&self.runtime_digest)
            || self.process_id.is_some() != self.process_start_identity.is_some()
            || self.next_command_sequence == 0
            || self.next_event_sequence == 0
            || !plans_valid
            || !activation_valid
            || !activated_valid
            || matches!(
                self.phase,
                ManagedMcpFirecrackerPhase::Prepared
                    | ManagedMcpFirecrackerPhase::Initializing
                    | ManagedMcpFirecrackerPhase::Initialized
                    | ManagedMcpFirecrackerPhase::Activated
            ) && self.process_id.is_none()
            || matches!(
                self.phase,
                ManagedMcpFirecrackerPhase::Initialized | ManagedMcpFirecrackerPhase::Activated
            ) != self.activation.is_some()
            || (self.phase == ManagedMcpFirecrackerPhase::Activated) != self.activated.is_some()
            || (self.phase == ManagedMcpFirecrackerPhase::Destroyed) != self.destroyed_at.is_some()
            || (self.phase == ManagedMcpFirecrackerPhase::Destroyed)
                != self.cleanup_evidence_digest.is_some()
            || (self.phase == ManagedMcpFirecrackerPhase::Destroyed && self.process_id.is_some())
            || managed_instance_id(&self.key)? != self.instance_id
            || digest_without_field(self, "record_digest")? != self.record_digest
        {
            return Err(host_contract_failure("managed_mcp_microvm_record_invalid"));
        }
        Ok(())
    }
}

struct LinuxManagedMcpShared {
    config: LinuxFirecrackerHostConfig,
    runtimes: BTreeMap<Sha256Digest, crate::InstalledMicroVmRuntime>,
    grant_authority: Arc<dyn MicroVmGrantRevoker>,
    artifact_broker: Arc<dyn MicroVmArtifactBroker>,
    secret_broker: Arc<dyn ManagedMcpSandboxSecretBroker>,
    state_sealer: Arc<dyn ManagedMcpSandboxSessionStateSealer>,
    capacity: Arc<MicroVmProviderCapacity>,
}

pub struct LinuxManagedMcpSessionFactory {
    shared: Arc<LinuxManagedMcpShared>,
    instances: Mutex<BTreeMap<ManagedMcpMicroVmSessionKey, Arc<LinuxManagedMcpInstance>>>,
}

impl LinuxManagedMcpSessionFactory {
    pub async fn install(
        mut config: LinuxFirecrackerHostConfig,
        grant_authority: Arc<dyn MicroVmGrantRevoker>,
        artifact_broker: Arc<dyn MicroVmArtifactBroker>,
        secret_broker: Arc<dyn ManagedMcpSandboxSecretBroker>,
        state_sealer: Arc<dyn ManagedMcpSandboxSessionStateSealer>,
        capacity: Arc<MicroVmProviderCapacity>,
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
        prepare_root_directory(&config.installation.chroot_base_directory).await?;
        prepare_root_directory(&config.state_directory).await?;
        config.state_directory =
            prepare_state_subdirectory(&config.state_directory, "managed-mcp").await?;
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
            shared: Arc::new(LinuxManagedMcpShared {
                config,
                runtimes,
                grant_authority,
                artifact_broker,
                secret_broker,
                state_sealer,
                capacity,
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
            let bytes = read_bounded(&path, MAX_MANAGED_RECORD_BYTES).await?;
            let record: ManagedMcpFirecrackerRecord = serde_json::from_slice(&bytes)
                .map_err(|_| LinuxFirecrackerHostError::RegistryCorrupt)?;
            if serde_jcs::to_vec(&record).map_err(|_| LinuxFirecrackerHostError::RegistryCorrupt)?
                != bytes
                || record
                    .validate(&self.shared.config, &self.shared.runtimes)
                    .is_err()
                || loaded.contains_key(&record.key)
            {
                return Err(LinuxFirecrackerHostError::RegistryCorrupt);
            }
            let tombstone_expired = record.phase == ManagedMcpFirecrackerPhase::Destroyed
                && record
                    .destroyed_at
                    .and_then(|destroyed_at| {
                        chrono::Duration::from_std(self.shared.config.tombstone_retention)
                            .ok()
                            .and_then(|retention| destroyed_at.checked_add_signed(retention))
                    })
                    .is_some_and(|expires_at| expires_at <= Utc::now());
            if tombstone_expired {
                fs::remove_file(&path)
                    .await
                    .map_err(|_| LinuxFirecrackerHostError::RegistryUnavailable)?;
                continue;
            }
            let instance = Arc::new(LinuxManagedMcpInstance::from_record(
                Arc::clone(&self.shared),
                record.clone(),
            ));
            loaded.insert(record.key.clone(), instance);
        }
        let active = loaded
            .values()
            .filter(|instance| !instance.destroyed.load(Ordering::Acquire))
            .count();
        if active > self.shared.config.maximum_instances
            || loaded.len().saturating_sub(active) > self.shared.config.maximum_tombstones
        {
            return Err(LinuxFirecrackerHostError::RegistryCorrupt);
        }
        self.shared.capacity.register_existing(active)?;
        *self.instances.lock().await = loaded;
        Ok(())
    }

    async fn prune_expired_tombstones(&self) -> Result<(), MicroVmProviderFailure> {
        let retention_millis = i64::try_from(self.shared.config.tombstone_retention.as_millis())
            .map_err(|_| {
                host_contract_failure("managed_mcp_microvm_tombstone_retention_overflow")
            })?;
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
            let path = managed_record_path(
                &self.shared.config.state_directory,
                &instance.plan.instance_id,
            )?;
            match fs::remove_file(path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {
                    return Err(host_contract_failure(
                        "managed_mcp_microvm_tombstone_prune_failed",
                    ));
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

    async fn allocate(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        executor_identity_digest: &Sha256Digest,
    ) -> Result<Arc<LinuxManagedMcpInstance>, MicroVmProviderFailure> {
        self.prune_expired_tombstones().await?;
        let key = ManagedMcpMicroVmSessionKey::from_request(request, fence);
        key.validate()?;
        let runtime = self
            .shared
            .runtimes
            .get(&request.runtime.image_or_module_digest)
            .filter(|runtime| runtime.matches_managed_request(request))
            .ok_or_else(|| host_contract_failure("managed_mcp_microvm_runtime_not_installed"))?;
        let mut instances = self.instances.lock().await;
        if let Some(instance) = instances.get(&key) {
            if instance.record.lock().await.executor_identity_digest != *executor_identity_digest {
                return Err(host_contract_failure(
                    "managed_mcp_microvm_executor_identity_mismatch",
                ));
            }
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
            return Err(host_contract_failure(
                "managed_mcp_microvm_capacity_exhausted",
            ));
        }
        let occupied = instances
            .values()
            .filter(|instance| !instance.destroyed.load(Ordering::Acquire))
            .map(|instance| instance.uid)
            .collect::<BTreeSet<_>>();
        let start =
            deterministic_identity_offset(&key, self.shared.config.ephemeral_identity_count)?;
        let mut selected = None;
        for probe in 0..self.shared.config.ephemeral_identity_count {
            let offset = (start + probe) % self.shared.config.ephemeral_identity_count;
            let uid = self
                .shared
                .config
                .ephemeral_uid_base
                .checked_add(offset)
                .ok_or_else(|| host_contract_failure("managed_mcp_microvm_identity_overflow"))?;
            if !occupied.contains(&uid) {
                let gid = self
                    .shared
                    .config
                    .ephemeral_gid_base
                    .checked_add(offset)
                    .ok_or_else(|| {
                        host_contract_failure("managed_mcp_microvm_identity_overflow")
                    })?;
                selected = Some((uid, gid, offset));
                break;
            }
        }
        let (uid, gid, identity_offset) = selected
            .ok_or_else(|| host_contract_failure("managed_mcp_microvm_identity_pool_exhausted"))?;
        let mut capacity_reservation = self.shared.capacity.reserve()?;
        let instance_id = managed_instance_id(&key)?;
        let plan = JailerLaunchPlan::build_managed(
            &self.shared.config.installation,
            request,
            instance_id.clone(),
            uid,
            gid,
        )
        .map_err(|_| host_contract_failure("managed_mcp_microvm_jailer_plan_invalid"))?;
        let guest_cid = 3_u32
            .checked_add(identity_offset)
            .ok_or_else(|| host_contract_failure("managed_mcp_microvm_guest_cid_overflow"))?;
        let sandbox_identity_digest = digest(&serde_json::json!({
            "domain": "linux_firecracker_managed_mcp_session_identity",
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
            "domain": "linux_firecracker_managed_mcp_prepare",
            "schema_version": 1,
            "sandbox_identity_digest": sandbox_identity_digest,
            "request_digest": request.request_digest,
            "plan_digest": plan.plan_digest,
            "firecracker_digest": self.shared.config.installation.firecracker_digest,
            "jailer_digest": self.shared.config.installation.jailer_digest,
        }))?;
        let mut secret_delivery_plans = Vec::with_capacity(request.secret_grants.len());
        for grant in &request.secret_grants {
            let receipt_id = new_resource_id(ResourceKind::Receipt)?;
            let event_id = new_resource_id(ResourceKind::Event)?;
            let outbox_id = new_resource_id(ResourceKind::OutboxEvent)?;
            let idempotency_key_digest = digest(&serde_json::json!({
                "domain": "managed_mcp_microvm_secret_delivery",
                "schema_version": 1,
                "key": key,
                "secret_binding_id": grant.secret_binding.secret_binding_id,
                "resolved_binding_generation": grant.resolved_binding_generation,
                "receipt_id": receipt_id,
            }))?;
            secret_delivery_plans.push(ManagedMcpSecretDeliveryPlan {
                secret_binding_id: grant.secret_binding.secret_binding_id.clone(),
                receipt_id,
                event_id,
                outbox_id,
                idempotency_key_digest,
            });
        }
        let record = ManagedMcpFirecrackerRecord {
            schema_version: MANAGED_RECORD_SCHEMA_VERSION,
            key: key.clone(),
            provider_process_generation_id: self
                .shared
                .config
                .provider_process_generation_id
                .clone(),
            executor_identity_digest: executor_identity_digest.clone(),
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
            secret_delivery_plans,
            next_command_sequence: 1,
            next_event_sequence: 1,
            phase: ManagedMcpFirecrackerPhase::Allocating,
            process_id: None,
            process_start_identity: None,
            activation: None,
            activated: None,
            cleanup_evidence_digest: None,
            updated_at: Utc::now(),
            destroyed_at: None,
            record_digest: placeholder_digest(),
        }
        .seal()?;
        persist_managed_record(&self.shared.config.state_directory, &record).await?;
        let instance = Arc::new(LinuxManagedMcpInstance::new(
            Arc::clone(&self.shared),
            record,
            plan,
        ));
        instances.insert(key, Arc::clone(&instance));
        capacity_reservation.commit();
        Ok(instance)
    }
}

struct LinuxManagedMcpInstance {
    shared: Arc<LinuxManagedMcpShared>,
    uid: u32,
    destroyed: AtomicBool,
    destroyed_at_millis: AtomicI64,
    capacity_released: AtomicBool,
    record: Mutex<ManagedMcpFirecrackerRecord>,
    plan: JailerLaunchPlan,
    operation: Mutex<()>,
    child: Mutex<Option<Child>>,
    channel: Mutex<Option<FirecrackerGuestChannel>>,
}

impl LinuxManagedMcpInstance {
    fn new(
        shared: Arc<LinuxManagedMcpShared>,
        record: ManagedMcpFirecrackerRecord,
        plan: JailerLaunchPlan,
    ) -> Self {
        Self {
            shared,
            uid: record.uid,
            destroyed: AtomicBool::new(false),
            destroyed_at_millis: AtomicI64::new(0),
            capacity_released: AtomicBool::new(false),
            record: Mutex::new(record),
            plan,
            operation: Mutex::new(()),
            child: Mutex::new(None),
            channel: Mutex::new(None),
        }
    }

    fn from_record(
        shared: Arc<LinuxManagedMcpShared>,
        record: ManagedMcpFirecrackerRecord,
    ) -> Self {
        let destroyed = record.phase == ManagedMcpFirecrackerPhase::Destroyed;
        let destroyed_at_millis = record
            .destroyed_at
            .map_or(0, |value| value.timestamp_millis());
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
            uid: record.uid,
            destroyed: AtomicBool::new(destroyed),
            destroyed_at_millis: AtomicI64::new(destroyed_at_millis),
            capacity_released: AtomicBool::new(destroyed),
            record: Mutex::new(record),
            plan,
            operation: Mutex::new(()),
            child: Mutex::new(None),
            channel: Mutex::new(None),
        }
    }

    async fn ensure_prepared(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
    ) -> Result<PreparedManagedMcpSandboxSession, MicroVmProviderFailure> {
        let _operation = self.operation.lock().await;
        {
            let record = self.record.lock().await;
            match record.phase {
                ManagedMcpFirecrackerPhase::Prepared
                | ManagedMcpFirecrackerPhase::Initializing
                | ManagedMcpFirecrackerPhase::Initialized
                | ManagedMcpFirecrackerPhase::Activated => return record.prepared(request),
                ManagedMcpFirecrackerPhase::Destroyed => {
                    return Err(host_contract_failure(
                        "managed_mcp_microvm_already_destroyed",
                    ));
                }
                ManagedMcpFirecrackerPhase::Allocating if record.process_id.is_some() => {
                    return Err(uncertain_failure(
                        "managed_mcp_microvm_prepare_interrupted",
                        Some(record.sandbox_identity_digest.clone()),
                        false,
                    ));
                }
                ManagedMcpFirecrackerPhase::Allocating => {}
            }
        }
        let runtime = self
            .shared
            .runtimes
            .get(&request.runtime.image_or_module_digest)
            .filter(|runtime| runtime.matches_managed_request(request))
            .ok_or_else(|| host_contract_failure("managed_mcp_microvm_runtime_not_installed"))?;
        let exact_plan = JailerLaunchPlan::build_managed(
            &self.shared.config.installation,
            request,
            self.plan.instance_id.clone(),
            self.plan.uid,
            self.plan.gid,
        )
        .map_err(|_| host_contract_failure("managed_mcp_microvm_recovered_plan_invalid"))?;
        if exact_plan.plan_digest != self.plan.plan_digest
            || exact_plan.jail_root != self.plan.jail_root
            || exact_plan.api_socket != self.plan.api_socket
            || exact_plan.vsock_path != self.plan.vsock_path
        {
            return Err(host_contract_failure(
                "managed_mcp_microvm_recovered_plan_mismatch",
            ));
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
            .map_err(|_| host_contract_failure("managed_mcp_microvm_jailer_spawn_failed"))?;
        let process_id = child
            .id()
            .ok_or_else(|| host_contract_failure("managed_mcp_microvm_process_identity_missing"))?;
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
            persist_managed_record(&self.shared.config.state_directory, &record).await?;
        }
        wait_for_socket(
            &self.plan.api_socket,
            std::time::Duration::from_millis(request.resources.startup_milliseconds),
            self.shared.config.socket_poll_interval,
        )
        .await?;
        let sandbox_identity_digest = self.record.lock().await.sandbox_identity_digest.clone();
        FirecrackerApiClient::new(self.plan.api_socket.clone(), self.shared.config.api_timeout)
            .map_err(|_| host_contract_failure("managed_mcp_microvm_firecracker_api_invalid"))?
            .configure_managed(request, self.record.lock().await.guest_cid)
            .await
            .map_err(|_| {
                uncertain_failure(
                    "managed_mcp_microvm_configuration_failed",
                    Some(sandbox_identity_digest),
                    false,
                )
            })?;
        let prepared = {
            let mut record = self.record.lock().await;
            record.phase = ManagedMcpFirecrackerPhase::Prepared;
            record.updated_at = Utc::now();
            record.seal_in_place()?;
            persist_managed_record(&self.shared.config.state_directory, &record).await?;
            record.prepared(request)?
        };
        Ok(prepared)
    }

    async fn load_runtime_artifact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
    ) -> Result<Vec<u8>, MicroVmProviderFailure> {
        let artifact = request.package.runtime_bundle_artifact.clone();
        let read_grant = request
            .artifact_grants
            .iter()
            .find(|grant| {
                grant.operation == ArtifactGrantOperation::ReadWhole
                    && grant.artifact.as_ref() == Some(&artifact)
            })
            .cloned()
            .ok_or_else(|| host_contract_failure("managed_mcp_microvm_runtime_grant_missing"))?;
        let maximum_bytes = usize::try_from(artifact.byte_length())
            .map_err(|_| host_contract_failure("managed_mcp_microvm_artifact_size_unsupported"))?;
        let bytes = self
            .shared
            .artifact_broker
            .read_exact(MicroVmArtifactReadRequest {
                workload_kind: MicroVmSandboxWorkloadKind::ManagedMcpSubscriptionSession,
                tenant_id: request.identity.tenant_id.clone(),
                sandbox_job_id: request.identity.sandbox_job_id.clone(),
                request_digest: request.request_digest.clone(),
                executor_worker_process_generation_id: fence.worker_process_generation_id.clone(),
                provider_process_generation_id: prepared.provider_process_generation_id.clone(),
                sandbox_identity_digest: prepared.sandbox_identity_digest.clone(),
                lease_generation: fence.lease_generation,
                artifact: artifact.clone(),
                purpose: MicroVmArtifactReadPurpose::RuntimeBundle,
                read_grant: Some(read_grant),
                maximum_bytes,
                deadline: request.deadline,
            })
            .await
            .map_err(map_artifact_error)?;
        if bytes.len() != maximum_bytes || sha256_bytes(&bytes)? != *artifact.content_digest() {
            return Err(host_contract_failure(
                "managed_mcp_microvm_artifact_integrity_failed",
            ));
        }
        Ok(bytes)
    }

    async fn initialize(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
    ) -> Result<PreparedManagedMcpSandboxSessionActivation, MicroVmProviderFailure> {
        let _operation = self.operation.lock().await;
        {
            let record = self.record.lock().await;
            if record.key != ManagedMcpMicroVmSessionKey::from_request(request, fence)
                || record.sandbox_identity_digest != prepared.sandbox_identity_digest
                || record.provider_process_generation_id != prepared.provider_process_generation_id
            {
                return Err(host_contract_failure(
                    "managed_mcp_microvm_initialize_identity_mismatch",
                ));
            }
            match record.phase {
                ManagedMcpFirecrackerPhase::Initialized | ManagedMcpFirecrackerPhase::Activated => {
                    return record.activation.clone().ok_or_else(|| {
                        host_contract_failure("managed_mcp_microvm_activation_missing")
                    });
                }
                ManagedMcpFirecrackerPhase::Initializing => {
                    return Err(uncertain_failure(
                        "managed_mcp_microvm_initialization_interrupted",
                        Some(record.sandbox_identity_digest.clone()),
                        true,
                    ));
                }
                ManagedMcpFirecrackerPhase::Prepared => {}
                ManagedMcpFirecrackerPhase::Allocating | ManagedMcpFirecrackerPhase::Destroyed => {
                    return Err(host_contract_failure(
                        "managed_mcp_microvm_initialize_phase_invalid",
                    ));
                }
            }
        }
        let runtime_bytes = self.load_runtime_artifact(request, fence, prepared).await?;
        {
            let mut record = self.record.lock().await;
            record.phase = ManagedMcpFirecrackerPhase::Initializing;
            record.updated_at = Utc::now();
            record.seal_in_place()?;
            persist_managed_record(&self.shared.config.state_directory, &record).await?;
        }
        FirecrackerApiClient::new(self.plan.api_socket.clone(), self.shared.config.api_timeout)
            .map_err(|_| host_contract_failure("managed_mcp_microvm_firecracker_api_invalid"))?
            .start()
            .await
            .map_err(|_| {
                uncertain_failure(
                    "managed_mcp_microvm_start_failed",
                    Some(prepared.sandbox_identity_digest.clone()),
                    true,
                )
            })?;
        wait_for_socket(
            &self.plan.vsock_path,
            std::time::Duration::from_millis(request.resources.startup_milliseconds),
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
                "managed_mcp_microvm_guest_channel_unavailable",
                Some(prepared.sandbox_identity_digest.clone()),
                true,
            )
        })?;
        let (mut command_sequence, mut event_sequence, delivery_plans) = {
            let record = self.record.lock().await;
            (
                record.next_command_sequence,
                record.next_event_sequence,
                record.secret_delivery_plans.clone(),
            )
        };
        let ready = channel
            .read_managed_event(request, fence, prepared, event_sequence, Utc::now())
            .await
            .map_err(|_| {
                uncertain_failure(
                    "managed_mcp_microvm_guest_ready_invalid",
                    Some(prepared.sandbox_identity_digest.clone()),
                    true,
                )
            })?;
        if !matches!(ready.event, ManagedMcpGuestEvent::Ready { .. }) {
            return Err(uncertain_failure(
                "managed_mcp_microvm_guest_ready_wrong_event",
                Some(prepared.sandbox_identity_digest.clone()),
                true,
            ));
        }
        event_sequence = next_sequence(event_sequence)?;
        let mut artifact_envelope_digests = Vec::new();
        for (index, content) in runtime_bytes
            .chunks(MAX_MICROVM_GUEST_ARTIFACT_CHUNK_BYTES)
            .enumerate()
        {
            let offset = index
                .checked_mul(MAX_MICROVM_GUEST_ARTIFACT_CHUNK_BYTES)
                .and_then(|value| u64::try_from(value).ok())
                .ok_or_else(|| {
                    host_contract_failure("managed_mcp_microvm_artifact_offset_overflow")
                })?;
            let final_chunk = offset
                .checked_add(u64::try_from(content.len()).map_err(|_| {
                    host_contract_failure("managed_mcp_microvm_artifact_chunk_overflow")
                })?)
                .is_some_and(|end| end == request.package.runtime_bundle_artifact.byte_length());
            let command = ManagedMcpGuestCommandEnvelope::materialize_runtime_artifact(
                request,
                fence,
                prepared,
                command_sequence,
                offset,
                content,
                final_chunk,
            )
            .map_err(|_| host_contract_failure("managed_mcp_microvm_artifact_command_invalid"))?;
            channel
                .write_managed_command(&command, request, fence, prepared, command_sequence)
                .await
                .map_err(|_| {
                    uncertain_failure(
                        "managed_mcp_microvm_artifact_delivery_failed",
                        Some(prepared.sandbox_identity_digest.clone()),
                        true,
                    )
                })?;
            artifact_envelope_digests.push(command.envelope_digest);
            command_sequence = next_sequence(command_sequence)?;
        }
        let mut secret_evidence = Vec::new();
        for grant in &request.secret_grants {
            let plan = delivery_plans
                .iter()
                .find(|plan| plan.secret_binding_id == grant.secret_binding.secret_binding_id)
                .ok_or_else(|| {
                    host_contract_failure("managed_mcp_microvm_secret_delivery_plan_missing")
                })?;
            let delivery_request = ManagedMcpSandboxSecretDeliveryRequest {
                schema_version: 1,
                receipt_id: plan.receipt_id.clone(),
                event_id: plan.event_id.clone(),
                outbox_id: plan.outbox_id.clone(),
                idempotency_key_digest: plan.idempotency_key_digest.clone(),
                identity: request.identity.clone(),
                request_digest: request.request_digest.clone(),
                fence: fence.clone(),
                prepared: prepared.clone(),
                secret_grant: grant.clone(),
                canonical_digest: placeholder_digest(),
            }
            .seal()
            .map_err(|_| host_contract_failure("managed_mcp_microvm_secret_request_invalid"))?;
            let delivered = self
                .shared
                .secret_broker
                .deliver(delivery_request)
                .await
                .map_err(|error| map_secret_error(error, &prepared.sandbox_identity_digest))?;
            let header = ManagedMcpGuestSecretHeader::from_delivery(grant, &delivered.evidence)
                .map_err(|_| host_contract_failure("managed_mcp_microvm_secret_header_invalid"))?;
            let delivery_evidence_digest = delivered.evidence.evidence_digest.clone();
            let command = ManagedMcpGuestCommandEnvelope::prepare_secret(
                request,
                fence,
                prepared,
                command_sequence,
                header,
            )
            .map_err(|_| host_contract_failure("managed_mcp_microvm_secret_command_invalid"))?;
            let mut material = delivered.into_material();
            channel
                .write_managed_secret(
                    &command,
                    request,
                    fence,
                    prepared,
                    command_sequence,
                    &mut material,
                )
                .await
                .map_err(|_| {
                    uncertain_failure(
                        "managed_mcp_microvm_secret_injection_failed",
                        Some(prepared.sandbox_identity_digest.clone()),
                        true,
                    )
                })?;
            command_sequence = next_sequence(command_sequence)?;
            let accepted = channel
                .read_managed_event(request, fence, prepared, event_sequence, Utc::now())
                .await
                .map_err(|_| {
                    uncertain_failure(
                        "managed_mcp_microvm_secret_ack_invalid",
                        Some(prepared.sandbox_identity_digest.clone()),
                        true,
                    )
                })?;
            let ManagedMcpGuestEvent::SecretAccepted {
                secret_binding_id,
                resolved_binding_generation,
                delivery_evidence_digest: accepted_delivery_digest,
                injection_binding_digest,
            } = &accepted.event
            else {
                return Err(uncertain_failure(
                    "managed_mcp_microvm_secret_ack_wrong_event",
                    Some(prepared.sandbox_identity_digest.clone()),
                    true,
                ));
            };
            if secret_binding_id != &grant.secret_binding.secret_binding_id
                || *resolved_binding_generation != grant.resolved_binding_generation
                || accepted_delivery_digest != &delivery_evidence_digest
            {
                return Err(uncertain_failure(
                    "managed_mcp_microvm_secret_ack_mismatch",
                    Some(prepared.sandbox_identity_digest.clone()),
                    true,
                ));
            }
            secret_evidence.push(serde_json::json!({
                "secret_binding_id": secret_binding_id,
                "resolved_binding_generation": resolved_binding_generation,
                "delivery_evidence_digest": delivery_evidence_digest,
                "injection_binding_digest": injection_binding_digest,
                "command_envelope_digest": command.envelope_digest,
                "ack_envelope_digest": accepted.envelope_digest,
            }));
            event_sequence = next_sequence(event_sequence)?;
        }
        let initialize =
            ManagedMcpGuestCommandEnvelope::initialize(request, fence, prepared, command_sequence)
                .map_err(|_| {
                    host_contract_failure("managed_mcp_microvm_initialize_command_invalid")
                })?;
        channel
            .write_managed_command(&initialize, request, fence, prepared, command_sequence)
            .await
            .map_err(|_| {
                uncertain_failure(
                    "managed_mcp_microvm_initialize_delivery_failed",
                    Some(prepared.sandbox_identity_digest.clone()),
                    true,
                )
            })?;
        command_sequence = next_sequence(command_sequence)?;
        let initialized = channel
            .read_managed_event(request, fence, prepared, event_sequence, Utc::now())
            .await
            .map_err(|_| {
                uncertain_failure(
                    "managed_mcp_microvm_initialized_event_invalid",
                    Some(prepared.sandbox_identity_digest.clone()),
                    true,
                )
            })?;
        let ManagedMcpGuestEvent::Initialized {
            established_at,
            expires_at,
            protocol_evidence_digest: guest_protocol_evidence_digest,
            activation_binding_digest,
        } = &initialized.event
        else {
            return Err(uncertain_failure(
                "managed_mcp_microvm_initialized_wrong_event",
                Some(prepared.sandbox_identity_digest.clone()),
                true,
            ));
        };
        event_sequence = next_sequence(event_sequence)?;
        let protocol_evidence_digest = digest(&serde_json::json!({
            "domain": "linux_firecracker_managed_mcp_initialization",
            "schema_version": 1,
            "sandbox_identity_digest": prepared.sandbox_identity_digest,
            "ready_envelope_digest": ready.envelope_digest,
            "artifact_envelope_digests": artifact_envelope_digests,
            "secret_evidence": secret_evidence,
            "initialize_envelope_digest": initialize.envelope_digest,
            "initialized_envelope_digest": initialized.envelope_digest,
            "guest_protocol_evidence_digest": guest_protocol_evidence_digest,
        }))?;
        let claims = ManagedMcpSandboxOpaqueSessionClaims {
            schema_version: 1,
            identity: request.identity.clone(),
            request_digest: request.request_digest.clone(),
            worker_process_generation_id: fence.worker_process_generation_id.clone(),
            provider_process_generation_id: prepared.provider_process_generation_id.clone(),
            lease_generation: fence.lease_generation,
            sandbox_identity_digest: prepared.sandbox_identity_digest.clone(),
            protocol_evidence_digest: protocol_evidence_digest.clone(),
            established_at: *established_at,
            expires_at: *expires_at,
        };
        claims
            .validate_for(request, fence, prepared, Utc::now())
            .map_err(|_| host_contract_failure("managed_mcp_microvm_session_claims_invalid"))?;
        let encrypted_opaque_session = self
            .shared
            .state_sealer
            .seal_managed_mcp_sandbox_session_state(claims)
            .await
            .map_err(|error| map_state_seal_error(error, &prepared.sandbox_identity_digest))?;
        let ready_evidence = ManagedMcpSandboxSessionReadyEvidence {
            schema_version: 1,
            session_identity_digest: request.identity.canonical_digest.clone(),
            sandbox_identity_digest: prepared.sandbox_identity_digest.clone(),
            encrypted_opaque_session,
            established_at: *established_at,
            expires_at: *expires_at,
            protocol_evidence_digest,
            canonical_digest: placeholder_digest(),
        }
        .seal()
        .map_err(|_| host_contract_failure("managed_mcp_microvm_ready_evidence_invalid"))?;
        let activation = PreparedManagedMcpSandboxSessionActivation {
            schema_version: 1,
            prepared: prepared.clone(),
            ready: ready_evidence,
            activation_binding_digest: activation_binding_digest.clone(),
            canonical_digest: placeholder_digest(),
        }
        .seal()
        .map_err(|_| host_contract_failure("managed_mcp_microvm_activation_seal_failed"))?;
        *self.channel.lock().await = Some(channel);
        {
            let mut record = self.record.lock().await;
            record.phase = ManagedMcpFirecrackerPhase::Initialized;
            record.next_command_sequence = command_sequence;
            record.next_event_sequence = event_sequence;
            record.activation = Some(activation.clone());
            record.updated_at = Utc::now();
            record.seal_in_place()?;
            persist_managed_record(&self.shared.config.state_directory, &record).await?;
        }
        Ok(activation)
    }

    async fn activate(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        activation: &PreparedManagedMcpSandboxSessionActivation,
    ) -> Result<ActivatedManagedMcpSandboxSession, MicroVmProviderFailure> {
        let _operation = self.operation.lock().await;
        let (command_sequence, event_sequence) = {
            let record = self.record.lock().await;
            if record.key != ManagedMcpMicroVmSessionKey::from_request(request, fence)
                || record.activation.as_ref() != Some(activation)
            {
                return Err(host_contract_failure(
                    "managed_mcp_microvm_activate_identity_mismatch",
                ));
            }
            match record.phase {
                ManagedMcpFirecrackerPhase::Activated => {
                    return record.activated.clone().ok_or_else(|| {
                        host_contract_failure("managed_mcp_microvm_activated_evidence_missing")
                    });
                }
                ManagedMcpFirecrackerPhase::Initialized => {
                    (record.next_command_sequence, record.next_event_sequence)
                }
                _ => {
                    return Err(host_contract_failure(
                        "managed_mcp_microvm_activate_phase_invalid",
                    ));
                }
            }
        };
        let mut channel = match self.channel.lock().await.take() {
            Some(channel) => channel,
            None => FirecrackerGuestChannel::connect(
                &self.plan.vsock_path,
                FIRECRACKER_GUEST_CONTROL_PORT,
                self.shared.config.guest_channel_timeout,
            )
            .await
            .map_err(|_| {
                uncertain_failure(
                    "managed_mcp_microvm_activation_channel_unavailable",
                    Some(activation.prepared.sandbox_identity_digest.clone()),
                    true,
                )
            })?,
        };
        let command = ManagedMcpGuestCommandEnvelope::activate(
            request,
            fence,
            &activation.prepared,
            command_sequence,
            activation.activation_binding_digest.clone(),
        )
        .map_err(|_| host_contract_failure("managed_mcp_microvm_activate_command_invalid"))?;
        channel
            .write_managed_command(
                &command,
                request,
                fence,
                &activation.prepared,
                command_sequence,
            )
            .await
            .map_err(|_| {
                uncertain_failure(
                    "managed_mcp_microvm_activate_delivery_failed",
                    Some(activation.prepared.sandbox_identity_digest.clone()),
                    true,
                )
            })?;
        let event = channel
            .read_managed_event(
                request,
                fence,
                &activation.prepared,
                event_sequence,
                Utc::now(),
            )
            .await
            .map_err(|_| {
                uncertain_failure(
                    "managed_mcp_microvm_activated_event_invalid",
                    Some(activation.prepared.sandbox_identity_digest.clone()),
                    true,
                )
            })?;
        let ManagedMcpGuestEvent::Activated {
            activated_at,
            activation_evidence_digest,
        } = &event.event
        else {
            return Err(uncertain_failure(
                "managed_mcp_microvm_activated_wrong_event",
                Some(activation.prepared.sandbox_identity_digest.clone()),
                true,
            ));
        };
        let activated = ActivatedManagedMcpSandboxSession {
            schema_version: 1,
            identity: request.identity.clone(),
            request_digest: request.request_digest.clone(),
            worker_process_generation_id: fence.worker_process_generation_id.clone(),
            lease_generation: fence.lease_generation,
            sandbox_identity_digest: activation.prepared.sandbox_identity_digest.clone(),
            ready_evidence_digest: activation.ready.canonical_digest.clone(),
            activated_at: *activated_at,
            activation_evidence_digest: activation_evidence_digest.clone(),
            canonical_digest: placeholder_digest(),
        }
        .seal()
        .map_err(|_| host_contract_failure("managed_mcp_microvm_activated_evidence_invalid"))?;
        *self.channel.lock().await = Some(channel);
        {
            let mut record = self.record.lock().await;
            record.phase = ManagedMcpFirecrackerPhase::Activated;
            record.next_command_sequence = next_sequence(command_sequence)?;
            record.next_event_sequence = next_sequence(event_sequence)?;
            record.activated = Some(activated.clone());
            record.updated_at = Utc::now();
            record.seal_in_place()?;
            persist_managed_record(&self.shared.config.state_directory, &record).await?;
        }
        Ok(activated)
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
                "managed_mcp_microvm_process_identity_changed",
                Some(self.record.lock().await.sandbox_identity_digest.clone()),
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
                    "managed_mcp_microvm_process_tree_still_alive",
                    Some(self.record.lock().await.sandbox_identity_digest.clone()),
                    true,
                ));
            }
            sleep(self.shared.config.socket_poll_interval).await;
        }
    }

    async fn revoke_grants(&self) -> Result<Sha256Digest, MicroVmProviderFailure> {
        let request = {
            let record = self.record.lock().await;
            RevokeMicroVmSandboxGrants {
                workload_kind: MicroVmSandboxWorkloadKind::ManagedMcpSubscriptionSession,
                tenant_id: record.key.tenant_id.clone(),
                sandbox_job_id: record.key.sandbox_job_id.clone(),
                request_digest: record.key.request_digest.clone(),
                executor_worker_process_generation_id: record
                    .key
                    .executor_worker_process_generation_id
                    .clone(),
                provider_process_generation_id: record.provider_process_generation_id.clone(),
                sandbox_identity_digest: record.sandbox_identity_digest.clone(),
                // Managed admission freezes attempt_limit=1; this is the physical Job attempt,
                // not the logical subscription session_generation.
                attempt_no: MANAGED_MCP_PHYSICAL_ATTEMPT_NO,
                lease_generation: record.key.lease_generation,
            }
        };
        let sandbox_identity_digest = request.sandbox_identity_digest.clone();
        let evidence = self
            .shared
            .grant_authority
            .revoke_exact(request)
            .await
            .map_err(|_| {
                uncertain_failure(
                    "managed_mcp_microvm_grant_revocation_failed",
                    Some(sandbox_identity_digest),
                    true,
                )
            })?;
        Ok(evidence.evidence_digest)
    }

    async fn cleanup(&self) -> Result<(), MicroVmProviderFailure> {
        let _operation = self.operation.lock().await;
        if self.record.lock().await.phase == ManagedMcpFirecrackerPhase::Destroyed {
            return Ok(());
        }
        self.channel.lock().await.take();
        let grant_result = self.revoke_grants().await;
        let process_result = self.kill_process_tree().await;
        let grant_evidence_digest = grant_result?;
        if !process_result? {
            return Err(uncertain_failure(
                "managed_mcp_microvm_process_tree_not_terminated",
                Some(self.record.lock().await.sandbox_identity_digest.clone()),
                true,
            ));
        }
        validate_destroy_path(
            &self.shared.config.installation,
            &self.plan,
            &self.plan.jail_root,
        )?;
        match fs::remove_dir_all(&self.plan.jail_root).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {
                return Err(uncertain_failure(
                    "managed_mcp_microvm_ephemeral_storage_destroy_failed",
                    Some(self.record.lock().await.sandbox_identity_digest.clone()),
                    true,
                ));
            }
        }
        let destroyed_at = Utc::now();
        {
            let mut record = self.record.lock().await;
            record.phase = ManagedMcpFirecrackerPhase::Destroyed;
            record.process_id = None;
            record.process_start_identity = None;
            record.destroyed_at = Some(destroyed_at);
            record.updated_at = destroyed_at;
            record.activation = None;
            record.activated = None;
            record.cleanup_evidence_digest = Some(digest(&serde_json::json!({
                "domain": "linux_firecracker_managed_mcp_destroyed",
                "schema_version": 1,
                "sandbox_identity_digest": record.sandbox_identity_digest,
                "grant_revocation_evidence_digest": grant_evidence_digest,
                "process_tree_terminated": true,
                "ephemeral_storage_destroyed": true,
                "destroyed_at": destroyed_at,
            }))?);
            record.seal_in_place()?;
            persist_managed_record(&self.shared.config.state_directory, &record).await?;
        }
        self.destroyed_at_millis
            .store(destroyed_at.timestamp_millis(), Ordering::Release);
        self.destroyed.store(true, Ordering::Release);
        if !self.capacity_released.swap(true, Ordering::AcqRel) {
            self.shared.capacity.release();
        }
        Ok(())
    }
}

#[async_trait]
impl ManagedMcpMicroVmSessionLifecyclePort for LinuxManagedMcpSessionFactory {
    async fn prepare_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        executor_identity_digest: &Sha256Digest,
    ) -> Result<PreparedManagedMcpSandboxSession, MicroVmProviderFailure> {
        let instance = self
            .allocate(request, fence, executor_identity_digest)
            .await?;
        match instance.ensure_prepared(request).await {
            Ok(prepared) => Ok(prepared),
            Err(error) => match instance.cleanup().await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(cleanup),
            },
        }
    }

    async fn initialize_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
    ) -> Result<PreparedManagedMcpSandboxSessionActivation, MicroVmProviderFailure> {
        let key = ManagedMcpMicroVmSessionKey::from_request(request, fence);
        let instance = self
            .instances
            .lock()
            .await
            .get(&key)
            .cloned()
            .ok_or_else(|| host_contract_failure("managed_mcp_microvm_instance_not_found"))?;
        instance.initialize(request, fence, prepared).await
    }

    async fn activate_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        activation: &PreparedManagedMcpSandboxSessionActivation,
    ) -> Result<ActivatedManagedMcpSandboxSession, MicroVmProviderFailure> {
        let key = ManagedMcpMicroVmSessionKey::from_request(request, fence);
        let instance = self
            .instances
            .lock()
            .await
            .get(&key)
            .cloned()
            .ok_or_else(|| host_contract_failure("managed_mcp_microvm_instance_not_found"))?;
        instance.activate(request, fence, activation).await
    }

    async fn destroy_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: Option<&PreparedManagedMcpSandboxSession>,
    ) -> Result<(), MicroVmProviderFailure> {
        let key = ManagedMcpMicroVmSessionKey::from_request(request, fence);
        let instance = self.instances.lock().await.get(&key).cloned();
        let Some(instance) = instance else {
            return Ok(());
        };
        if let Some(prepared) = prepared {
            let sandbox_identity_digest =
                instance.record.lock().await.sandbox_identity_digest.clone();
            if prepared.sandbox_identity_digest != sandbox_identity_digest {
                return Err(host_contract_failure(
                    "managed_mcp_microvm_destroy_identity_mismatch",
                ));
            }
        }
        instance.cleanup().await
    }
}

async fn persist_managed_record(
    state_directory: &Path,
    record: &ManagedMcpFirecrackerRecord,
) -> Result<(), MicroVmProviderFailure> {
    let bytes = serde_jcs::to_vec(record)
        .map_err(|_| host_contract_failure("managed_mcp_microvm_record_canonicalization_failed"))?;
    if bytes.len() > MAX_MANAGED_RECORD_BYTES {
        return Err(host_contract_failure(
            "managed_mcp_microvm_record_too_large",
        ));
    }
    let target = managed_record_path(state_directory, &record.instance_id)?;
    let temporary = state_directory.join(format!("{}.tmp", record.instance_id));
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .await
        .map_err(|_| host_contract_failure("managed_mcp_microvm_record_write_failed"))?;
    tokio::io::AsyncWriteExt::write_all(&mut file, &bytes)
        .await
        .map_err(|_| host_contract_failure("managed_mcp_microvm_record_write_failed"))?;
    file.sync_all()
        .await
        .map_err(|_| host_contract_failure("managed_mcp_microvm_record_sync_failed"))?;
    drop(file);
    fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))
        .await
        .map_err(|_| host_contract_failure("managed_mcp_microvm_record_permissions_failed"))?;
    fs::rename(&temporary, &target)
        .await
        .map_err(|_| host_contract_failure("managed_mcp_microvm_record_commit_failed"))?;
    let directory = state_directory.to_path_buf();
    tokio::task::spawn_blocking(move || {
        std::fs::File::open(directory).and_then(|directory| directory.sync_all())
    })
    .await
    .map_err(|_| host_contract_failure("managed_mcp_microvm_registry_sync_failed"))?
    .map_err(|_| host_contract_failure("managed_mcp_microvm_registry_sync_failed"))
}

fn managed_record_path(
    state_directory: &Path,
    instance_id: &str,
) -> Result<PathBuf, MicroVmProviderFailure> {
    if instance_id.is_empty()
        || instance_id.len() > crate::MAX_FIRECRACKER_INSTANCE_ID_BYTES
        || !instance_id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
    {
        return Err(host_contract_failure(
            "managed_mcp_microvm_instance_id_invalid",
        ));
    }
    Ok(state_directory.join(format!("{instance_id}.json")))
}

fn deterministic_identity_offset(
    key: &ManagedMcpMicroVmSessionKey,
    count: u32,
) -> Result<u32, MicroVmProviderFailure> {
    let canonical = serde_jcs::to_vec(key)
        .map_err(|_| host_contract_failure("managed_mcp_microvm_key_canonicalization_failed"))?;
    let digest = Sha256::digest(canonical);
    Ok(u32::from_be_bytes(digest[..4].try_into().unwrap()) % count)
}

fn managed_instance_id(
    key: &ManagedMcpMicroVmSessionKey,
) -> Result<String, MicroVmProviderFailure> {
    let canonical = serde_jcs::to_vec(key)
        .map_err(|_| host_contract_failure("managed_mcp_microvm_key_canonicalization_failed"))?;
    let digest = Sha256::digest(canonical);
    Ok(format!("ms-{}", hex_prefix(&digest, 30)))
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

fn new_resource_id(kind: ResourceKind) -> Result<ResourceId, MicroVmProviderFailure> {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7())
        .map_err(|_| host_contract_failure("managed_mcp_microvm_resource_id_failed"))
}

fn next_sequence(value: u64) -> Result<u64, MicroVmProviderFailure> {
    value
        .checked_add(1)
        .ok_or_else(|| host_contract_failure("managed_mcp_microvm_sequence_overflow"))
}

fn map_artifact_error(error: MicroVmArtifactBrokerError) -> MicroVmProviderFailure {
    let code = match error {
        MicroVmArtifactBrokerError::Unavailable => "managed_mcp_microvm_artifact_unavailable",
        MicroVmArtifactBrokerError::Denied => "managed_mcp_microvm_artifact_denied",
        MicroVmArtifactBrokerError::NotFound => "managed_mcp_microvm_artifact_not_found",
        MicroVmArtifactBrokerError::Integrity => "managed_mcp_microvm_artifact_integrity",
        MicroVmArtifactBrokerError::TooLarge => "managed_mcp_microvm_artifact_too_large",
    };
    host_contract_failure(code)
}

fn map_secret_error(
    error: ManagedMcpSandboxSecretBrokerError,
    sandbox_identity_digest: &Sha256Digest,
) -> MicroVmProviderFailure {
    let code = match error {
        ManagedMcpSandboxSecretBrokerError::Unavailable => {
            "managed_mcp_microvm_secret_broker_unavailable"
        }
        ManagedMcpSandboxSecretBrokerError::Denied => "managed_mcp_microvm_secret_delivery_denied",
        ManagedMcpSandboxSecretBrokerError::OutcomeUncertain => {
            "managed_mcp_microvm_secret_delivery_uncertain"
        }
    };
    uncertain_failure(code, Some(sandbox_identity_digest.clone()), true)
}

fn map_state_seal_error(
    error: ManagedMcpSandboxSessionStateSealError,
    sandbox_identity_digest: &Sha256Digest,
) -> MicroVmProviderFailure {
    let code = match error {
        ManagedMcpSandboxSessionStateSealError::Unavailable => {
            "managed_mcp_microvm_state_sealer_unavailable"
        }
        ManagedMcpSandboxSessionStateSealError::Denied => "managed_mcp_microvm_state_seal_denied",
    };
    uncertain_failure(code, Some(sandbox_identity_digest.clone()), true)
}

fn digest(value: &serde_json::Value) -> Result<Sha256Digest, MicroVmProviderFailure> {
    canonical_digest(value)
        .map_err(|_| host_contract_failure("managed_mcp_microvm_evidence_canonicalization_failed"))?
        .parse()
        .map_err(|_| host_contract_failure("managed_mcp_microvm_evidence_digest_invalid"))
}

fn sha256_bytes(value: &[u8]) -> Result<Sha256Digest, MicroVmProviderFailure> {
    let encoded = Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("sha256:{encoded}")
        .parse()
        .map_err(|_| host_contract_failure("managed_mcp_microvm_content_digest_invalid"))
}

fn digest_without_field<T: Serialize>(
    value: &T,
    field: &str,
) -> Result<Sha256Digest, MicroVmProviderFailure> {
    let mut value = serde_json::to_value(value)
        .map_err(|_| host_contract_failure("managed_mcp_microvm_record_canonicalization_failed"))?;
    let serde_json::Value::Object(object) = &mut value else {
        return Err(host_contract_failure(
            "managed_mcp_microvm_record_canonicalization_failed",
        ));
    };
    object.remove(field);
    digest(&value)
}

fn placeholder_digest() -> Sha256Digest {
    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        .parse()
        .expect("static digest is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1e8-32e4-75e1-a9e8-d95ca0f5{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn sha(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn key() -> ManagedMcpMicroVmSessionKey {
        ManagedMcpMicroVmSessionKey {
            tenant_id: id(ResourceKind::Tenant, 1),
            sandbox_job_id: id(ResourceKind::SandboxJob, 2),
            session_identity_digest: sha('a'),
            request_digest: sha('b'),
            executor_worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 3),
            lease_generation: 4,
        }
    }

    #[test]
    fn managed_instance_identity_is_stable_and_exactly_fenced() {
        let key = key();
        key.validate().unwrap();
        let first = managed_instance_id(&key).unwrap();
        assert_eq!(first, managed_instance_id(&key).unwrap());
        assert!(first.starts_with("ms-"));
        assert!(first.len() <= crate::MAX_FIRECRACKER_INSTANCE_ID_BYTES);

        let mut next_lease = key;
        next_lease.lease_generation += 1;
        assert_ne!(first, managed_instance_id(&next_lease).unwrap());
    }

    #[test]
    fn shared_capacity_bounds_finite_and_managed_lifecycles_together() {
        let capacity = Arc::new(MicroVmProviderCapacity::new(2).unwrap());
        let mut finite = capacity.reserve().unwrap();
        finite.commit();
        let managed = capacity.reserve().unwrap();
        assert!(capacity.reserve().is_err());
        drop(managed);
        assert!(capacity.reserve().is_ok());
        capacity.release();
    }

    #[test]
    fn post_start_broker_failures_are_uncertain_and_identity_bound() {
        let identity = sha('c');
        let failure = map_secret_error(ManagedMcpSandboxSecretBrokerError::Denied, &identity);
        assert_eq!(failure.sandbox_identity_digest, Some(identity));
        assert!(failure.execution_may_have_started);
        assert!(failure.external_effect_possible);
    }
}
