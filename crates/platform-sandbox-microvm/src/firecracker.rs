use crate::{
    read_guest_frame, write_guest_frame, MicroVmGuestCommandEnvelope, MicroVmGuestEventEnvelope,
};
use insight_platform_contracts::{
    canonical_digest, SandboxIsolationClass, SandboxRuntimeFamily, Sha256Digest,
};
use insight_platform_sandbox::SandboxExecutionRequest;
use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    fmt,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    time::timeout,
};

pub const FIRECRACKER_API_SOCKET_IN_JAIL: &str = "/run/firecracker.socket";
pub const FIRECRACKER_API_SOCKET_ON_HOST: &str = "run/firecracker.socket";
pub const FIRECRACKER_VSOCK_PATH_IN_JAIL: &str = "run/guest.vsock";
pub const FIRECRACKER_GUEST_CONTROL_PORT: u32 = 10_000;
pub const MAX_FIRECRACKER_API_RESPONSE_BYTES: usize = 64 * 1024;
pub const MAX_FIRECRACKER_INSTANCE_ID_BYTES: usize = 64;
pub const MAX_FIRECRACKER_VSOCK_HANDSHAKE_BYTES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FirecrackerInstallation {
    pub version: String,
    pub firecracker_path: PathBuf,
    pub firecracker_digest: Sha256Digest,
    pub jailer_path: PathBuf,
    pub jailer_digest: Sha256Digest,
    pub chroot_base_directory: PathBuf,
    pub parent_cgroup: String,
}

impl FirecrackerInstallation {
    pub fn validate(&self) -> Result<(), FirecrackerContractError> {
        if !stable_version(&self.version)
            || !closed_absolute_path(&self.firecracker_path)
            || !closed_absolute_path(&self.jailer_path)
            || !closed_absolute_path(&self.chroot_base_directory)
            || !stable_identifier(&self.parent_cgroup, 128)
            || self.firecracker_path == self.jailer_path
        {
            return Err(FirecrackerContractError::InvalidInstallation);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledMicroVmRuntime {
    pub runtime_family: SandboxRuntimeFamily,
    pub runtime_digest: Sha256Digest,
    pub guest_kernel_digest: Sha256Digest,
    pub guest_agent_digest: Sha256Digest,
    pub guest_kernel_path: PathBuf,
    pub rootfs_path: PathBuf,
    pub rootfs_bytes: u64,
}

impl InstalledMicroVmRuntime {
    pub fn validate(&self) -> Result<(), FirecrackerContractError> {
        if !matches!(
            self.runtime_family,
            SandboxRuntimeFamily::Python
                | SandboxRuntimeFamily::NodeJs
                | SandboxRuntimeFamily::ReviewedShell
                | SandboxRuntimeFamily::ManagedMcpServer
        ) || !closed_absolute_path(&self.guest_kernel_path)
            || !closed_absolute_path(&self.rootfs_path)
            || self.guest_kernel_path == self.rootfs_path
            || self.rootfs_bytes == 0
            || self.rootfs_bytes > 16 * 1024 * 1024 * 1024
        {
            return Err(FirecrackerContractError::InvalidRuntime);
        }
        Ok(())
    }

    pub fn matches_request(&self, request: &SandboxExecutionRequest) -> bool {
        request.isolation_class == SandboxIsolationClass::MicroVm
            && request.runtime.runtime_family == self.runtime_family
            && request.runtime.image_or_module_digest == self.runtime_digest
            && request.runtime.guest_kernel_digest.as_ref() == Some(&self.guest_kernel_digest)
            && request.runtime.guest_agent_digest == self.guest_agent_digest
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JailerLaunchPlan {
    pub instance_id: String,
    pub uid: u32,
    pub gid: u32,
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    pub jail_root: PathBuf,
    pub api_socket: PathBuf,
    pub vsock_path: PathBuf,
    pub plan_digest: Sha256Digest,
}

impl JailerLaunchPlan {
    pub fn build(
        installation: &FirecrackerInstallation,
        request: &SandboxExecutionRequest,
        instance_id: String,
        uid: u32,
        gid: u32,
    ) -> Result<Self, FirecrackerContractError> {
        installation.validate()?;
        if !stable_identifier(&instance_id, MAX_FIRECRACKER_INSTANCE_ID_BYTES)
            || uid == 0
            || gid == 0
            || request.isolation_class != SandboxIsolationClass::MicroVm
            || !request
                .policies
                .isolation
                .require_hardware_virtualization_for_microvm
            || !request.policies.isolation.fresh_jail_per_job
            || !request.policies.isolation.fresh_guest_kernel_per_job
            || !request.policies.isolation.single_tenant_guest
            || !request.policies.isolation.single_use_guest
            || !request.policies.isolation.deny_host_devices
            || !request.policies.resource.swap_disabled
        {
            return Err(FirecrackerContractError::InvalidLaunchPlan);
        }
        let period = 100_000_u64;
        let cpu_quota = u64::from(request.resources.cpu_millicores)
            .checked_mul(period)
            .and_then(|value| value.checked_div(1_000))
            .filter(|value| *value > 0)
            .ok_or(FirecrackerContractError::InvalidLaunchPlan)?;
        let memory_bytes = u64::from(request.resources.memory_mebibytes)
            .checked_mul(1024 * 1024)
            .ok_or(FirecrackerContractError::InvalidLaunchPlan)?;
        let file_size = request
            .resources
            .artifact_output_bytes
            .checked_add(request.resources.result_bytes)
            .and_then(|value| value.checked_add(request.resources.stdout_bytes))
            .and_then(|value| value.checked_add(request.resources.stderr_bytes))
            .ok_or(FirecrackerContractError::InvalidLaunchPlan)?;
        let no_file = request
            .resources
            .files
            .checked_add(1)
            .ok_or(FirecrackerContractError::InvalidLaunchPlan)?;
        let jail_root = installation
            .chroot_base_directory
            .join("firecracker")
            .join(&instance_id)
            .join("root");
        let arguments = vec![
            "--id".to_owned(),
            instance_id.clone(),
            "--exec-file".to_owned(),
            installation.firecracker_path.display().to_string(),
            "--uid".to_owned(),
            uid.to_string(),
            "--gid".to_owned(),
            gid.to_string(),
            "--chroot-base-dir".to_owned(),
            installation.chroot_base_directory.display().to_string(),
            "--cgroup-version".to_owned(),
            "2".to_owned(),
            "--parent-cgroup".to_owned(),
            installation.parent_cgroup.clone(),
            "--cgroup".to_owned(),
            format!("cpu.max={cpu_quota} {period}"),
            "--cgroup".to_owned(),
            format!("memory.max={memory_bytes}"),
            "--cgroup".to_owned(),
            "memory.swap.max=0".to_owned(),
            "--cgroup".to_owned(),
            format!("pids.max={}", request.resources.pids),
            "--resource-limit".to_owned(),
            format!("fsize={file_size}"),
            "--resource-limit".to_owned(),
            format!("no-file={no_file}"),
            "--new-pid-ns".to_owned(),
            "--".to_owned(),
            "--api-sock".to_owned(),
            FIRECRACKER_API_SOCKET_IN_JAIL.to_owned(),
        ];
        let digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "arguments": arguments,
            "firecracker_digest": installation.firecracker_digest,
            "gid": gid,
            "instance_id": instance_id,
            "jailer_digest": installation.jailer_digest,
            "schema_version": 1,
            "uid": uid,
        }))
        .map_err(|_| FirecrackerContractError::Canonicalization)?
        .parse()
        .map_err(|_| FirecrackerContractError::Canonicalization)?;
        Ok(Self {
            instance_id,
            uid,
            gid,
            executable: installation.jailer_path.clone(),
            arguments,
            api_socket: jail_root.join(FIRECRACKER_API_SOCKET_ON_HOST),
            vsock_path: jail_root.join(FIRECRACKER_VSOCK_PATH_IN_JAIL),
            jail_root,
            plan_digest: digest,
        })
    }
}

#[derive(Debug, Clone)]
pub struct FirecrackerApiClient {
    socket_path: PathBuf,
    timeout: Duration,
}

impl FirecrackerApiClient {
    pub fn new(socket_path: PathBuf, timeout: Duration) -> Result<Self, FirecrackerContractError> {
        if !closed_absolute_path(&socket_path)
            || timeout.is_zero()
            || timeout > Duration::from_secs(30)
        {
            return Err(FirecrackerContractError::InvalidApiClient);
        }
        Ok(Self {
            socket_path,
            timeout,
        })
    }

    /// Installs the complete immutable machine configuration without starting guest execution.
    ///
    /// Keeping `InstanceStart` out of this method is required by the durable Sandbox phase
    /// boundary: `prepare` may create and configure a VMM, while only the separately fenced
    /// `start` command may run the guest.
    pub async fn configure(
        &self,
        request: &SandboxExecutionRequest,
        guest_cid: u32,
    ) -> Result<(), FirecrackerApiError> {
        if request.isolation_class != SandboxIsolationClass::MicroVm
            || guest_cid < 3
            || request.resources.memory_mebibytes < 1
        {
            return Err(FirecrackerApiError::InvalidRequest);
        }
        let vcpu_count = request
            .resources
            .cpu_millicores
            .div_ceil(1_000)
            .clamp(1, 32);
        self.put(
            "/machine-config",
            &serde_json::json!({
                "vcpu_count": u8::try_from(vcpu_count)
                    .map_err(|_| FirecrackerApiError::InvalidRequest)?,
                "mem_size_mib": request.resources.memory_mebibytes,
                "smt": false,
                "track_dirty_pages": false,
            }),
        )
        .await?;
        self.put(
            "/boot-source",
            &serde_json::json!({
                "kernel_image_path": "/kernel",
                "boot_args": "reboot=k panic=1 pci=off 8250.nr_uarts=0 nomodules random.trust_cpu=on",
            }),
        )
        .await?;
        self.put(
            "/drives/rootfs",
            &serde_json::json!({
                "drive_id": "rootfs",
                "path_on_host": "/rootfs.ext4",
                "is_root_device": true,
                "is_read_only": false,
            }),
        )
        .await?;
        self.put(
            "/vsock",
            &serde_json::json!({
                "guest_cid": guest_cid,
                "uds_path": FIRECRACKER_VSOCK_PATH_IN_JAIL,
            }),
        )
        .await
    }

    pub async fn start(&self) -> Result<(), FirecrackerApiError> {
        self.put(
            "/actions",
            &serde_json::json!({"action_type": "InstanceStart"}),
        )
        .await
    }

    pub async fn put(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<(), FirecrackerApiError> {
        if !valid_api_path(path) {
            return Err(FirecrackerApiError::InvalidRequest);
        }
        let body = serde_jcs::to_vec(body).map_err(|_| FirecrackerApiError::InvalidRequest)?;
        if body.is_empty() || body.len() > 1024 * 1024 {
            return Err(FirecrackerApiError::InvalidRequest);
        }
        timeout(self.timeout, self.put_inner(path, &body))
            .await
            .map_err(|_| FirecrackerApiError::Timeout)?
    }

    async fn put_inner(&self, path: &str, body: &[u8]) -> Result<(), FirecrackerApiError> {
        let mut stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|_| FirecrackerApiError::Unavailable)?;
        let request = format!(
            "PUT {path} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|_| FirecrackerApiError::Unavailable)?;
        stream
            .write_all(body)
            .await
            .map_err(|_| FirecrackerApiError::Unavailable)?;
        stream
            .shutdown()
            .await
            .map_err(|_| FirecrackerApiError::Unavailable)?;
        let mut response = Vec::with_capacity(4_096);
        stream
            .take(u64::try_from(MAX_FIRECRACKER_API_RESPONSE_BYTES + 1).unwrap())
            .read_to_end(&mut response)
            .await
            .map_err(|_| FirecrackerApiError::Unavailable)?;
        if response.len() > MAX_FIRECRACKER_API_RESPONSE_BYTES {
            return Err(FirecrackerApiError::InvalidResponse);
        }
        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .ok_or(FirecrackerApiError::InvalidResponse)?;
        let headers = std::str::from_utf8(&response[..header_end])
            .map_err(|_| FirecrackerApiError::InvalidResponse)?;
        let status = headers
            .lines()
            .next()
            .and_then(|line| line.split_ascii_whitespace().nth(1))
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or(FirecrackerApiError::InvalidResponse)?;
        if !(200..300).contains(&status) {
            return Err(FirecrackerApiError::Rejected);
        }
        Ok(())
    }
}

/// One exact host-to-guest control connection over Firecracker's Unix-vsock bridge.
///
/// Firecracker itself only authenticates the local Unix endpoint and guest CID. The platform
/// protocol therefore independently fences every frame by Sandbox Job, request digest, attempt
/// and lease generation before any result can be trusted.
pub struct FirecrackerGuestChannel {
    stream: UnixStream,
    timeout: Duration,
}

impl FirecrackerGuestChannel {
    pub async fn connect(
        socket_path: &Path,
        guest_port: u32,
        timeout_duration: Duration,
    ) -> Result<Self, FirecrackerGuestChannelError> {
        if !closed_absolute_path(socket_path)
            || guest_port == 0
            || timeout_duration.is_zero()
            || timeout_duration > Duration::from_secs(30)
        {
            return Err(FirecrackerGuestChannelError::InvalidConfiguration);
        }
        let mut stream = timeout(timeout_duration, UnixStream::connect(socket_path))
            .await
            .map_err(|_| FirecrackerGuestChannelError::Timeout)?
            .map_err(|_| FirecrackerGuestChannelError::Unavailable)?;
        let request = format!("CONNECT {guest_port}\n");
        timeout(timeout_duration, stream.write_all(request.as_bytes()))
            .await
            .map_err(|_| FirecrackerGuestChannelError::Timeout)?
            .map_err(|_| FirecrackerGuestChannelError::Unavailable)?;
        let expected = format!("OK {guest_port}\n").into_bytes();
        let mut response = vec![0_u8; expected.len()];
        timeout(timeout_duration, stream.read_exact(&mut response))
            .await
            .map_err(|_| FirecrackerGuestChannelError::Timeout)?
            .map_err(|_| FirecrackerGuestChannelError::Unavailable)?;
        if response.len() > MAX_FIRECRACKER_VSOCK_HANDSHAKE_BYTES || response != expected {
            return Err(FirecrackerGuestChannelError::Rejected);
        }
        Ok(Self {
            stream,
            timeout: timeout_duration,
        })
    }

    pub async fn write_command(
        &mut self,
        command: &MicroVmGuestCommandEnvelope,
    ) -> Result<(), FirecrackerGuestChannelError> {
        command
            .validate()
            .map_err(|_| FirecrackerGuestChannelError::InvalidFrame)?;
        timeout(self.timeout, write_guest_frame(&mut self.stream, command))
            .await
            .map_err(|_| FirecrackerGuestChannelError::Timeout)?
            .map_err(|_| FirecrackerGuestChannelError::InvalidFrame)
    }

    pub async fn read_event(
        &mut self,
    ) -> Result<MicroVmGuestEventEnvelope, FirecrackerGuestChannelError> {
        timeout(self.timeout, read_guest_frame(&mut self.stream))
            .await
            .map_err(|_| FirecrackerGuestChannelError::Timeout)?
            .map_err(|_| FirecrackerGuestChannelError::InvalidFrame)
    }
}

fn stable_version(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.' || byte == b'-')
        && value.bytes().any(|byte| byte.is_ascii_digit())
}

fn stable_identifier(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

fn closed_absolute_path(path: &Path) -> bool {
    path.is_absolute()
        && path.as_os_str().as_encoded_bytes().len() <= 4_096
        && path.components().all(|component| {
            !matches!(
                component,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
}

fn valid_api_path(path: &str) -> bool {
    path.starts_with('/')
        && path.len() <= 128
        && !path.contains("..")
        && path.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'/' | b'-' | b'_')
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirecrackerContractError {
    InvalidInstallation,
    InvalidRuntime,
    InvalidLaunchPlan,
    InvalidApiClient,
    Canonicalization,
}

impl fmt::Display for FirecrackerContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidInstallation => "Firecracker installation contract is invalid",
            Self::InvalidRuntime => "installed microVM runtime contract is invalid",
            Self::InvalidLaunchPlan => "Firecracker jailer launch plan is invalid",
            Self::InvalidApiClient => "Firecracker API client contract is invalid",
            Self::Canonicalization => "Firecracker contract canonicalization failed",
        })
    }
}

impl Error for FirecrackerContractError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirecrackerApiError {
    InvalidRequest,
    Unavailable,
    Timeout,
    InvalidResponse,
    Rejected,
}

impl fmt::Display for FirecrackerApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidRequest => "Firecracker API request is invalid",
            Self::Unavailable => "Firecracker API is unavailable",
            Self::Timeout => "Firecracker API timed out",
            Self::InvalidResponse => "Firecracker API response is invalid",
            Self::Rejected => "Firecracker API rejected the exact configuration",
        })
    }
}

impl Error for FirecrackerApiError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirecrackerGuestChannelError {
    InvalidConfiguration,
    Unavailable,
    Timeout,
    Rejected,
    InvalidFrame,
}

impl fmt::Display for FirecrackerGuestChannelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "Firecracker guest channel configuration is invalid",
            Self::Unavailable => "Firecracker guest channel is unavailable",
            Self::Timeout => "Firecracker guest channel timed out",
            Self::Rejected => "Firecracker guest channel handshake was rejected",
            Self::InvalidFrame => "Firecracker guest channel returned an invalid frame",
        })
    }
}

impl Error for FirecrackerGuestChannelError {}
