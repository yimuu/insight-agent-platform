//! Node-local Sandbox process-generation attestation authority.
//!
//! The authority owns no business state. It binds a WorkerProcessGeneration to process identity
//! observed from a node/runtime source, keeps a bounded node-local tamper-evident index, and proves
//! absence only when the exact observed process identity no longer exists.

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use insight_platform_contracts::{canonical_digest, parse_strict_json, JsonLimits, Sha256Digest};
use insight_platform_sandbox::{
    NodeAttestorRoute, ProveWasiProcessGenerationAbsent, RegisterWasiExecutorProcessGeneration,
    VerifyWasiExecutorProcessGeneration, WasiExecutorProcessAttestationAuthority,
    WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrationError,
    WasiExecutorProcessRegistrationVerifier, WasiExecutorRegistrationPeer,
    WasiProcessGenerationAbsenceEvidence, WasiProcessGenerationIsolation,
    WasiProcessGenerationIsolationDisposition, WasiProcessGenerationIsolationError,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    fs::{self, OpenOptions},
    io::{self, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;
use uuid::Uuid;

const REGISTRY_SCHEMA_VERSION: u32 = 1;
const MAX_REGISTRY_BYTES: usize = 16 * 1024 * 1024;
const MAX_OBSERVED_STRING_BYTES: usize = 2_048;
const WASI_EXECUTOR_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/sandbox-executor.wasi";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedExecutorProcessIdentity {
    pub schema_version: u32,
    pub node_uid: String,
    pub pod_uid: String,
    pub runtime_cgroup_locator: String,
    pub boot_id: String,
    pub pid_namespace_inode: u64,
    pub host_process_id: u32,
    pub host_user_id: u32,
    pub host_group_id: u32,
    pub process_start_ticks: u64,
}

impl ObservedExecutorProcessIdentity {
    pub fn validate_for(
        &self,
        peer: WasiExecutorRegistrationPeer,
    ) -> Result<(), ProcessObservationError> {
        peer.validate()
            .map_err(|_| ProcessObservationError::Rejected)?;
        if self.schema_version != 1
            || self.host_process_id != peer.host_process_id
            || self.host_user_id != peer.host_user_id
            || self.host_group_id != peer.host_group_id
            || self.pid_namespace_inode == 0
            || self.process_start_ticks == 0
            || !uuid_string(&self.node_uid)
            || !uuid_string(&self.pod_uid)
            || !uuid_string(&self.boot_id)
            || !bounded_locator(&self.runtime_cgroup_locator)
        {
            return Err(ProcessObservationError::Rejected);
        }
        Ok(())
    }

    pub fn binding_digest(&self) -> Result<Sha256Digest, ProcessObservationError> {
        canonical_digest(&serde_json::json!({
            "boot_id": self.boot_id,
            "host_group_id": self.host_group_id,
            "host_process_id": self.host_process_id,
            "host_user_id": self.host_user_id,
            "node_uid": self.node_uid,
            "pid_namespace_inode": self.pid_namespace_inode,
            "pod_uid": self.pod_uid,
            "process_start_ticks": self.process_start_ticks,
            "runtime_cgroup_locator": self.runtime_cgroup_locator,
            "schema_version": self.schema_version,
            "workload_identity": WASI_EXECUTOR_WORKLOAD_IDENTITY,
        }))
        .map_err(|_| ProcessObservationError::Rejected)?
        .parse()
        .map_err(|_| ProcessObservationError::Rejected)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessObservationError {
    Missing,
    Unavailable,
    Rejected,
}

#[async_trait]
pub trait ExecutorProcessObserver: Send + Sync {
    async fn observe(
        &self,
        peer: WasiExecutorRegistrationPeer,
    ) -> Result<ObservedExecutorProcessIdentity, ProcessObservationError>;
}

#[derive(Debug, Clone)]
pub struct LinuxProcfsExecutorProcessObserver {
    proc_root: PathBuf,
    node_uid_authority_path: PathBuf,
}

impl LinuxProcfsExecutorProcessObserver {
    pub fn new(
        proc_root: PathBuf,
        node_uid_authority_path: PathBuf,
    ) -> Result<Self, ProcessObservationError> {
        if !closed_absolute_path(&proc_root) || !closed_absolute_path(&node_uid_authority_path) {
            return Err(ProcessObservationError::Rejected);
        }
        Ok(Self {
            proc_root,
            node_uid_authority_path,
        })
    }

    fn observe_blocking(
        &self,
        peer: WasiExecutorRegistrationPeer,
    ) -> Result<ObservedExecutorProcessIdentity, ProcessObservationError> {
        peer.validate()
            .map_err(|_| ProcessObservationError::Rejected)?;
        let process_root = self.proc_root.join(peer.host_process_id.to_string());
        let stat = read_observation_file(&process_root.join("stat"), 32 * 1024)?;
        let process_start_ticks = parse_process_start_ticks(&stat)?;
        let status = read_observation_file(&process_root.join("status"), 128 * 1024)?;
        let (observed_uid, observed_gid) = parse_process_credentials(&status)?;
        if observed_uid != peer.host_user_id || observed_gid != peer.host_group_id {
            return Err(ProcessObservationError::Rejected);
        }
        let cgroup = read_observation_file(&process_root.join("cgroup"), 128 * 1024)?;
        let runtime_cgroup_locator = parse_unified_cgroup(&cgroup)?;
        let pod_uid = extract_pod_uid(&runtime_cgroup_locator)?;
        let pid_namespace =
            fs::read_link(process_root.join("ns/pid")).map_err(classify_observation_io)?;
        let pid_namespace_inode = parse_namespace_inode(&pid_namespace)?;
        let boot_id = read_uuid_file(&self.proc_root.join("sys/kernel/random/boot_id"))?;
        let node_uid = read_uuid_file(&self.node_uid_authority_path)?;
        let observed = ObservedExecutorProcessIdentity {
            schema_version: 1,
            node_uid,
            pod_uid,
            runtime_cgroup_locator,
            boot_id,
            pid_namespace_inode,
            host_process_id: peer.host_process_id,
            host_user_id: peer.host_user_id,
            host_group_id: peer.host_group_id,
            process_start_ticks,
        };
        observed.validate_for(peer)?;
        Ok(observed)
    }
}

#[async_trait]
impl ExecutorProcessObserver for LinuxProcfsExecutorProcessObserver {
    async fn observe(
        &self,
        peer: WasiExecutorRegistrationPeer,
    ) -> Result<ObservedExecutorProcessIdentity, ProcessObservationError> {
        let observer = self.clone();
        tokio::task::spawn_blocking(move || observer.observe_blocking(peer))
            .await
            .map_err(|_| ProcessObservationError::Unavailable)?
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRegistration {
    request: RegisterWasiExecutorProcessGeneration,
    observed: ObservedExecutorProcessIdentity,
    evidence: WasiExecutorProcessIdentityEvidence,
    confirmed_absent_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistrySnapshot {
    schema_version: u32,
    records: Vec<StoredRegistration>,
    snapshot_digest: Sha256Digest,
}

impl RegistrySnapshot {
    fn build(records: &BTreeMap<String, StoredRegistration>) -> Result<Self, NodeAttestorError> {
        let mut snapshot = Self {
            schema_version: REGISTRY_SCHEMA_VERSION,
            records: records.values().cloned().collect(),
            snapshot_digest: zero_digest(),
        };
        snapshot.snapshot_digest = snapshot.canonical_digest()?;
        Ok(snapshot)
    }

    fn validate(
        self,
        maximum_records: usize,
        expected_attestor_identity_digest: &Sha256Digest,
        expected_attestor_route: &NodeAttestorRoute,
    ) -> Result<BTreeMap<String, StoredRegistration>, NodeAttestorError> {
        if self.schema_version != REGISTRY_SCHEMA_VERSION
            || self.records.len() > maximum_records
            || self.snapshot_digest != self.canonical_digest()?
        {
            return Err(NodeAttestorError::RegistryCorrupt);
        }
        let mut records = BTreeMap::new();
        let now = Utc::now();
        for record in self.records {
            record
                .request
                .validate()
                .map_err(|_| NodeAttestorError::RegistryCorrupt)?;
            let peer = WasiExecutorRegistrationPeer {
                host_process_id: record.observed.host_process_id,
                host_user_id: record.observed.host_user_id,
                host_group_id: record.observed.host_group_id,
            };
            record
                .observed
                .validate_for(peer)
                .map_err(|_| NodeAttestorError::RegistryCorrupt)?;
            record
                .evidence
                .validate_for(&record.request, expected_attestor_identity_digest, now)
                .map_err(|_| NodeAttestorError::RegistryCorrupt)?;
            if &record.evidence.attestor_route != expected_attestor_route {
                return Err(NodeAttestorError::RegistryCorrupt);
            }
            if record.evidence.executor_instance_binding_digest
                != record
                    .observed
                    .binding_digest()
                    .map_err(|_| NodeAttestorError::RegistryCorrupt)?
            {
                return Err(NodeAttestorError::RegistryCorrupt);
            }
            let key = record.evidence.executor_identity_digest.to_string();
            if records.insert(key, record).is_some() {
                return Err(NodeAttestorError::RegistryCorrupt);
            }
        }
        Ok(records)
    }

    fn canonical_digest(&self) -> Result<Sha256Digest, NodeAttestorError> {
        canonical_digest(&serde_json::json!({
            "records": self.records,
            "schema_version": self.schema_version,
        }))
        .map_err(|_| NodeAttestorError::RegistryCorrupt)?
        .parse()
        .map_err(|_| NodeAttestorError::RegistryCorrupt)
    }
}

#[derive(Debug, Clone)]
pub struct NodeAttestorConfig {
    pub attestor_identity_digest: Sha256Digest,
    pub attestor_route: NodeAttestorRoute,
    pub registry_path: PathBuf,
    pub maximum_registrations: usize,
    pub absent_retention: std::time::Duration,
}

impl NodeAttestorConfig {
    pub fn validate(&self) -> Result<(), NodeAttestorError> {
        if !closed_absolute_path(&self.registry_path)
            || self.maximum_registrations == 0
            || self.maximum_registrations > 100_000
            || self.absent_retention.is_zero()
        {
            return Err(NodeAttestorError::InvalidConfiguration);
        }
        Ok(())
    }
}

pub struct NodeProcessAttestor<O> {
    observer: Arc<O>,
    config: NodeAttestorConfig,
    records: Mutex<BTreeMap<String, StoredRegistration>>,
}

impl<O> NodeProcessAttestor<O>
where
    O: ExecutorProcessObserver + 'static,
{
    pub fn open(observer: Arc<O>, config: NodeAttestorConfig) -> Result<Self, NodeAttestorError> {
        config.validate()?;
        let records = load_registry(
            &config.registry_path,
            config.maximum_registrations,
            &config.attestor_identity_digest,
            &config.attestor_route,
        )?;
        Ok(Self {
            observer,
            config,
            records: Mutex::new(records),
        })
    }

    async fn persist_locked(
        &self,
        records: &BTreeMap<String, StoredRegistration>,
    ) -> Result<(), NodeAttestorError> {
        let snapshot = RegistrySnapshot::build(records)?;
        let bytes = serde_json::to_vec(&snapshot).map_err(|_| NodeAttestorError::RegistryIo)?;
        if bytes.is_empty() || bytes.len() > MAX_REGISTRY_BYTES {
            return Err(NodeAttestorError::RegistryCapacity);
        }
        let path = self.config.registry_path.clone();
        tokio::task::spawn_blocking(move || persist_registry(&path, &bytes))
            .await
            .map_err(|_| NodeAttestorError::RegistryIo)??;
        Ok(())
    }

    async fn observe_stored(
        &self,
        stored: &StoredRegistration,
    ) -> Result<ObservationDisposition, ProcessObservationError> {
        let peer = WasiExecutorRegistrationPeer {
            host_process_id: stored.observed.host_process_id,
            host_user_id: stored.observed.host_user_id,
            host_group_id: stored.observed.host_group_id,
        };
        match self.observer.observe(peer).await {
            Ok(observed) if observed == stored.observed => Ok(ObservationDisposition::SameProcess),
            Ok(_) | Err(ProcessObservationError::Missing) => Ok(ObservationDisposition::Absent),
            Err(error) => Err(error),
        }
    }

    fn exact_registration<'a>(
        records: &'a BTreeMap<String, StoredRegistration>,
        request: &VerifyWasiExecutorProcessGeneration,
    ) -> Result<&'a StoredRegistration, WasiExecutorProcessRegistrationError> {
        request.validate()?;
        let stored = records
            .get(request.executor_identity_digest.as_str())
            .ok_or(WasiExecutorProcessRegistrationError::Rejected)?;
        if stored.request.worker_process_generation_id != request.worker_process_generation_id
            || stored.request.worker_manifest_digest != request.worker_manifest_digest
            || stored.request.isolation_backend_contract_digest
                != request.isolation_backend_contract_digest
            || stored.evidence.executor_identity_digest != request.executor_identity_digest
            || stored.evidence.attestor_route != request.attestor_route
        {
            return Err(WasiExecutorProcessRegistrationError::Rejected);
        }
        Ok(stored)
    }
}

#[async_trait]
impl<O> WasiExecutorProcessAttestationAuthority for NodeProcessAttestor<O>
where
    O: ExecutorProcessObserver + 'static,
{
    async fn register_observed(
        &self,
        request: RegisterWasiExecutorProcessGeneration,
        peer: WasiExecutorRegistrationPeer,
    ) -> Result<WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrationError> {
        request.validate()?;
        peer.validate()?;
        let observed = self
            .observer
            .observe(peer)
            .await
            .map_err(registration_observation_error)?;
        observed
            .validate_for(peer)
            .map_err(registration_observation_error)?;
        let observed_at = Utc::now();
        let evidence = WasiExecutorProcessIdentityEvidence {
            schema_version: 1,
            worker_process_generation_id: request.worker_process_generation_id.clone(),
            worker_manifest_digest: request.worker_manifest_digest.clone(),
            isolation_backend_contract_digest: request.isolation_backend_contract_digest.clone(),
            executor_instance_binding_digest: observed
                .binding_digest()
                .map_err(registration_observation_error)?,
            executor_identity_digest: zero_digest(),
            attestor_identity_digest: self.config.attestor_identity_digest.clone(),
            attestor_route: self.config.attestor_route.clone(),
            observed_at,
            evidence_digest: zero_digest(),
        }
        .seal()?;

        let mut records = self.records.lock().await;
        if let Some(existing) = records.values().find(|record| {
            record.request.worker_process_generation_id == request.worker_process_generation_id
        }) {
            if existing.request == request
                && existing.observed == observed
                && existing.confirmed_absent_at.is_none()
            {
                return Ok(existing.evidence.clone());
            }
            return Err(WasiExecutorProcessRegistrationError::Rejected);
        }
        if records
            .values()
            .any(|record| record.observed == observed && record.confirmed_absent_at.is_none())
        {
            return Err(WasiExecutorProcessRegistrationError::Rejected);
        }
        prune_absent(&mut records, observed_at, self.config.absent_retention);
        if records.len() >= self.config.maximum_registrations {
            return Err(WasiExecutorProcessRegistrationError::Unavailable);
        }
        let key = evidence.executor_identity_digest.to_string();
        if records.contains_key(&key) {
            return Err(WasiExecutorProcessRegistrationError::Rejected);
        }
        records.insert(
            key,
            StoredRegistration {
                request,
                observed,
                evidence: evidence.clone(),
                confirmed_absent_at: None,
            },
        );
        self.persist_locked(&records)
            .await
            .map_err(|_| WasiExecutorProcessRegistrationError::Unavailable)?;
        Ok(evidence)
    }
}

#[async_trait]
impl<O> WasiExecutorProcessRegistrationVerifier for NodeProcessAttestor<O>
where
    O: ExecutorProcessObserver + 'static,
{
    async fn verify_registered(
        &self,
        request: VerifyWasiExecutorProcessGeneration,
    ) -> Result<WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrationError> {
        let stored = {
            let records = self.records.lock().await;
            Self::exact_registration(&records, &request)?.clone()
        };
        if stored.confirmed_absent_at.is_some() {
            return Err(WasiExecutorProcessRegistrationError::Rejected);
        }
        match self.observe_stored(&stored).await {
            Ok(ObservationDisposition::SameProcess) => Ok(stored.evidence),
            Ok(ObservationDisposition::Absent) | Err(ProcessObservationError::Rejected) => {
                Err(WasiExecutorProcessRegistrationError::Rejected)
            }
            Err(ProcessObservationError::Unavailable) => {
                Err(WasiExecutorProcessRegistrationError::Unavailable)
            }
            Err(ProcessObservationError::Missing) => {
                Err(WasiExecutorProcessRegistrationError::Rejected)
            }
        }
    }
}

#[async_trait]
impl<O> WasiProcessGenerationIsolation for NodeProcessAttestor<O>
where
    O: ExecutorProcessObserver + 'static,
{
    async fn prove_absent(
        &self,
        request: ProveWasiProcessGenerationAbsent,
    ) -> Result<WasiProcessGenerationAbsenceEvidence, WasiProcessGenerationIsolationError> {
        request.validate()?;
        let stored = {
            let records = self.records.lock().await;
            records
                .get(request.executor_identity_digest.as_str())
                .filter(|stored| {
                    stored.request.worker_process_generation_id
                        == request.previous_worker_process_generation_id
                        && stored.evidence.attestor_route == request.attestor_route
                })
                .cloned()
                .ok_or(WasiProcessGenerationIsolationError::Rejected)?
        };
        match self.observe_stored(&stored).await {
            Ok(ObservationDisposition::SameProcess) => {
                return Err(WasiProcessGenerationIsolationError::StillLive)
            }
            Ok(ObservationDisposition::Absent) => {}
            Err(ProcessObservationError::Unavailable) => {
                return Err(WasiProcessGenerationIsolationError::Unavailable)
            }
            Err(ProcessObservationError::Missing) => {}
            Err(ProcessObservationError::Rejected) => {
                return Err(WasiProcessGenerationIsolationError::Rejected)
            }
        }
        let confirmation_candidate = Utc::now();
        let observed_at = {
            let mut records = self.records.lock().await;
            let (observed_at, changed) = {
                let current = records
                    .get_mut(request.executor_identity_digest.as_str())
                    .ok_or(WasiProcessGenerationIsolationError::Rejected)?;
                match current.confirmed_absent_at {
                    Some(confirmed_at) => (confirmed_at, false),
                    None => {
                        current.confirmed_absent_at = Some(confirmation_candidate);
                        (confirmation_candidate, true)
                    }
                }
            };
            if changed {
                self.persist_locked(&records)
                    .await
                    .map_err(|_| WasiProcessGenerationIsolationError::Unavailable)?;
            }
            observed_at
        };
        WasiProcessGenerationAbsenceEvidence {
            schema_version: 1,
            tenant_id: request.tenant_id.clone(),
            sandbox_job_id: request.sandbox_job_id.clone(),
            request_digest: request.request_digest.clone(),
            previous_worker_process_generation_id: request
                .previous_worker_process_generation_id
                .clone(),
            executor_identity_digest: request.executor_identity_digest.clone(),
            attestor_identity_digest: self.config.attestor_identity_digest.clone(),
            attestor_route: self.config.attestor_route.clone(),
            disposition: WasiProcessGenerationIsolationDisposition::ProcessAbsent,
            observed_at,
            evidence_digest: zero_digest(),
        }
        .seal()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservationDisposition {
    SameProcess,
    Absent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeAttestorError {
    InvalidConfiguration,
    RegistryIo,
    RegistryCorrupt,
    RegistryCapacity,
}

impl fmt::Display for NodeAttestorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "node attestor configuration is invalid",
            Self::RegistryIo => "node attestor registry I/O failed",
            Self::RegistryCorrupt => "node attestor registry is corrupt",
            Self::RegistryCapacity => "node attestor registry capacity was exceeded",
        })
    }
}

impl Error for NodeAttestorError {}

fn registration_observation_error(
    error: ProcessObservationError,
) -> WasiExecutorProcessRegistrationError {
    match error {
        ProcessObservationError::Unavailable => WasiExecutorProcessRegistrationError::Unavailable,
        ProcessObservationError::Missing | ProcessObservationError::Rejected => {
            WasiExecutorProcessRegistrationError::Rejected
        }
    }
}

fn prune_absent(
    records: &mut BTreeMap<String, StoredRegistration>,
    now: DateTime<Utc>,
    retention: std::time::Duration,
) {
    let Ok(retention) = ChronoDuration::from_std(retention) else {
        return;
    };
    records.retain(|_, record| {
        record
            .confirmed_absent_at
            .is_none_or(|confirmed| confirmed + retention > now)
    });
}

fn load_registry(
    path: &Path,
    maximum_records: usize,
    expected_attestor_identity_digest: &Sha256Digest,
    expected_attestor_route: &NodeAttestorRoute,
) -> Result<BTreeMap<String, StoredRegistration>, NodeAttestorError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(_) => return Err(NodeAttestorError::RegistryIo),
    };
    if bytes.is_empty() || bytes.len() > MAX_REGISTRY_BYTES {
        return Err(NodeAttestorError::RegistryCorrupt);
    }
    let value = parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: MAX_REGISTRY_BYTES,
            max_depth: 24,
            max_properties_per_object: 32,
            max_items_per_array: maximum_records,
            max_string_bytes: MAX_OBSERVED_STRING_BYTES,
        },
    )
    .map_err(|_| NodeAttestorError::RegistryCorrupt)?;
    serde_json::from_value::<RegistrySnapshot>(value)
        .map_err(|_| NodeAttestorError::RegistryCorrupt)?
        .validate(
            maximum_records,
            expected_attestor_identity_digest,
            expected_attestor_route,
        )
}

fn persist_registry(path: &Path, bytes: &[u8]) -> Result<(), NodeAttestorError> {
    let parent = path
        .parent()
        .ok_or(NodeAttestorError::InvalidConfiguration)?;
    if !parent.is_dir() {
        return Err(NodeAttestorError::RegistryIo);
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(NodeAttestorError::InvalidConfiguration)?;
    let temporary = parent.join(format!(".{file_name}.{}.tmp", Uuid::now_v7()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|_| NodeAttestorError::RegistryIo)?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|_| NodeAttestorError::RegistryIo)?;
        fs::rename(&temporary, path).map_err(|_| NodeAttestorError::RegistryIo)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|_| NodeAttestorError::RegistryIo)?;
        OpenOptions::new()
            .read(true)
            .open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| NodeAttestorError::RegistryIo)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn read_observation_file(
    path: &Path,
    maximum_bytes: usize,
) -> Result<String, ProcessObservationError> {
    let metadata = fs::metadata(path).map_err(classify_observation_io)?;
    if metadata.len() > maximum_bytes as u64 {
        return Err(ProcessObservationError::Rejected);
    }
    let value = fs::read_to_string(path).map_err(classify_observation_io)?;
    if value.is_empty() || value.len() > maximum_bytes {
        return Err(ProcessObservationError::Rejected);
    }
    Ok(value)
}

fn read_uuid_file(path: &Path) -> Result<String, ProcessObservationError> {
    let value = read_observation_file(path, 128)?;
    let value = value.trim();
    Uuid::parse_str(value)
        .map(|value| value.hyphenated().to_string())
        .map_err(|_| ProcessObservationError::Rejected)
}

fn parse_process_start_ticks(stat: &str) -> Result<u64, ProcessObservationError> {
    let end = stat.rfind(')').ok_or(ProcessObservationError::Rejected)?;
    stat.get(end + 1..)
        .ok_or(ProcessObservationError::Rejected)?
        .split_whitespace()
        .nth(19)
        .ok_or(ProcessObservationError::Rejected)?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(ProcessObservationError::Rejected)
}

fn parse_process_credentials(status: &str) -> Result<(u32, u32), ProcessObservationError> {
    let parse = |prefix: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(prefix))
            .and_then(|line| line.split_whitespace().next())
            .and_then(|value| value.parse::<u32>().ok())
            .ok_or(ProcessObservationError::Rejected)
    };
    Ok((parse("Uid:")?, parse("Gid:")?))
}

fn parse_unified_cgroup(cgroup: &str) -> Result<String, ProcessObservationError> {
    let mut lines = cgroup.lines().filter(|line| !line.is_empty());
    let value = lines.next().ok_or(ProcessObservationError::Rejected)?;
    if lines.next().is_some()
        || !value.starts_with("0::/")
        || value.len() > MAX_OBSERVED_STRING_BYTES
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(ProcessObservationError::Rejected);
    }
    Ok(value.to_owned())
}

fn extract_pod_uid(locator: &str) -> Result<String, ProcessObservationError> {
    let lower = locator.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    for index in 0..bytes.len().saturating_sub(3) {
        if bytes.get(index..index + 3) != Some(b"pod") {
            continue;
        }
        let tail = lower
            .get(index + 3..)
            .ok_or(ProcessObservationError::Rejected)?;
        let candidate = tail
            .chars()
            .take_while(|character| {
                character.is_ascii_hexdigit() || matches!(*character, '-' | '_')
            })
            .collect::<String>()
            .replace('_', "-");
        if let Ok(uid) = Uuid::parse_str(&candidate) {
            return Ok(uid.hyphenated().to_string());
        }
    }
    Err(ProcessObservationError::Rejected)
}

fn parse_namespace_inode(path: &Path) -> Result<u64, ProcessObservationError> {
    let value = path.to_string_lossy();
    value
        .strip_prefix("pid:[")
        .and_then(|value| value.strip_suffix(']'))
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or(ProcessObservationError::Rejected)
}

fn classify_observation_io(error: io::Error) -> ProcessObservationError {
    if error.kind() == io::ErrorKind::NotFound {
        ProcessObservationError::Missing
    } else {
        ProcessObservationError::Unavailable
    }
}

fn closed_absolute_path(path: &Path) -> bool {
    path.is_absolute()
        && path.as_os_str().as_encoded_bytes().len() <= 4_096
        && path
            .components()
            .all(|component| !matches!(component, std::path::Component::ParentDir))
}

fn bounded_locator(value: &str) -> bool {
    value.starts_with("0::/")
        && value.len() <= MAX_OBSERVED_STRING_BYTES
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn uuid_string(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|parsed| parsed.hyphenated().to_string() == value)
}

fn zero_digest() -> Sha256Digest {
    format!("sha256:{}", "0".repeat(64)).parse().unwrap()
}

#[cfg(test)]
mod tests;
