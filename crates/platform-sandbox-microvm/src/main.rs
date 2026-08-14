//! Privileged node-local Firecracker Provider composition.
//!
//! The unprivileged microVM Executor owns the durable PostgreSQL Job lease and reaches this
//! process only through a node-local Unix socket. This process owns `/dev/kvm`, jailer/cgroup
//! operations and per-attempt host state. It has no PostgreSQL, NATS, Kubernetes or object-store
//! credential; grant revocation is delegated to the Controller over a provider-only mTLS service.

use insight_platform_capability_adapters::InstalledMcpToolCodecRegistry;
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, JsonLimits, ResourceId,
    ResourceKind, Sha256Digest,
};
use insight_platform_sandbox::{
    InstalledSandboxBackendDescriptor, MicroVmGrantRevoker, SandboxCommandLimits,
    SandboxIsolationBackendKind,
};
use insight_platform_sandbox_microvm::{
    DurableMicroVmLifecycle, FirecrackerInstallation, InstalledMicroVmRuntime,
    LinuxFirecrackerHostConfig, LinuxFirecrackerHostFactory, ManagedMcpSandboxProtocolAdapter,
    MicroVmSandboxExecutorBackend, SystemMicroVmProviderClock,
};
use insight_platform_sandbox_rpc::{
    proto::sandbox_isolation_provider_service_server::SandboxIsolationProviderServiceServer,
    MicroVmExecutorWorkloadIdentity, SandboxInternalRpcLimits, SandboxIsolationProviderGrpcService,
    SandboxMicroVmBrokerGrpcClient,
};
use serde::Deserialize;
use std::{
    error::Error,
    fmt,
    os::unix::fs::{FileTypeExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig};

const CONFIG_PATH_ENV: &str = "PLATFORM_SANDBOX_MICROVM_PROVIDER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_SANDBOX_MICROVM_PROVIDER_CONFIG_DIGEST";
const SERVER_CLIENT_CA_PATH_ENV: &str = "PLATFORM_SANDBOX_PROVIDER_CLIENT_CA_PATH";
const SERVER_CERT_PATH_ENV: &str = "PLATFORM_SANDBOX_PROVIDER_CERT_PATH";
const SERVER_KEY_PATH_ENV: &str = "PLATFORM_SANDBOX_PROVIDER_KEY_PATH";
const BROKER_CA_PATH_ENV: &str = "PLATFORM_SANDBOX_PROVIDER_BROKER_CA_PATH";
const BROKER_CERT_PATH_ENV: &str = "PLATFORM_SANDBOX_PROVIDER_BROKER_CERT_PATH";
const BROKER_KEY_PATH_ENV: &str = "PLATFORM_SANDBOX_PROVIDER_BROKER_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 1024 * 1024;
const MAX_TLS_FILE_BYTES: usize = 1024 * 1024;
const MAX_UNIX_SOCKET_PATH_BYTES: usize = 100;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderProcessConfig {
    schema_version: u32,
    provider_socket_path: String,
    provider_tls_server_name: String,
    controller_broker_endpoint: String,
    controller_broker_tls_server_name: String,
    worker_manifest_digest: Sha256Digest,
    backend_contract_digest: Sha256Digest,
    installation: FirecrackerInstallation,
    runtimes: Vec<InstalledMicroVmRuntime>,
    state_directory: String,
    ephemeral_uid_base: u32,
    ephemeral_gid_base: u32,
    ephemeral_identity_count: u32,
    maximum_instances: usize,
    maximum_tombstones: usize,
    tombstone_retention_seconds: u64,
    maximum_lifecycle_entries: usize,
    api_timeout_milliseconds: u64,
    guest_channel_timeout_milliseconds: u64,
    socket_poll_milliseconds: u64,
    process_termination_timeout_milliseconds: u64,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
    tls_handshake_timeout_milliseconds: u64,
    shutdown_grace_milliseconds: u64,
}

impl ProviderProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV, 4_096)?;
        let bytes = read_bounded(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 24,
                max_items_per_array: 256,
                max_properties_per_object: 128,
                max_string_bytes: 8_192,
            },
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        let expected: Sha256Digest = required(CONFIG_DIGEST_ENV)?
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let actual: Sha256Digest = canonical_digest(&value)
            .map_err(|_| ProcessError::InvalidConfiguration)?
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if actual != expected {
            return Err(ProcessError::InvalidConfiguration);
        }
        let config: Self =
            serde_json::from_value(value).map_err(|_| ProcessError::InvalidConfiguration)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ProcessError> {
        let socket = Path::new(&self.provider_socket_path);
        let state = Path::new(&self.state_directory);
        self.installation
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || !closed_absolute_path(socket, MAX_UNIX_SOCKET_PATH_BYTES)
            || socket.parent().is_none_or(is_broad_directory)
            || !stable_dns_name(&self.provider_tls_server_name)
            || !closed_service_endpoint(&self.controller_broker_endpoint, "https")
            || !stable_dns_name(&self.controller_broker_tls_server_name)
            || !closed_absolute_path(state, 4_096)
            || is_broad_directory(state)
            || state == self.installation.chroot_base_directory
            || self.runtimes.is_empty()
            || self.runtimes.len() > 256
            || self.runtimes.iter().any(|runtime| {
                runtime.validate().is_err()
                    || runtime.runtime_family
                        == insight_platform_contracts::SandboxRuntimeFamily::ManagedMcpServer
            })
            || self.maximum_instances == 0
            || self.maximum_instances > 65_536
            || self.maximum_tombstones < self.maximum_instances
            || self.maximum_tombstones > 262_144
            || !(86_400..=2_592_000).contains(&self.tombstone_retention_seconds)
            || self.maximum_lifecycle_entries < self.maximum_instances
            || self.maximum_lifecycle_entries > 65_536
            || !bounded_milliseconds(self.api_timeout_milliseconds, 30_000)
            || !bounded_milliseconds(self.guest_channel_timeout_milliseconds, 30_000)
            || !bounded_milliseconds(self.socket_poll_milliseconds, 1_000)
            || !bounded_milliseconds(self.process_termination_timeout_milliseconds, 30_000)
            || !bounded_milliseconds(self.connect_timeout_milliseconds, 30_000)
            || self.request_timeout_milliseconds < self.connect_timeout_milliseconds
            || self.request_timeout_milliseconds > 120_000
            || !bounded_milliseconds(self.tls_handshake_timeout_milliseconds, 30_000)
            || !bounded_milliseconds(self.shutdown_grace_milliseconds, 120_000)
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        let mut runtime_digests = self
            .runtimes
            .iter()
            .map(|runtime| runtime.runtime_digest.clone())
            .collect::<Vec<_>>();
        runtime_digests.sort();
        if runtime_digests.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ProcessError::InvalidConfiguration);
        }
        Ok(())
    }

    fn descriptor(&self) -> InstalledSandboxBackendDescriptor {
        InstalledSandboxBackendDescriptor {
            backend_kind: SandboxIsolationBackendKind::MicroVm,
            isolation_class: insight_platform_contracts::SandboxIsolationClass::MicroVm,
            worker_manifest_digest: self.worker_manifest_digest.clone(),
            backend_contract_digest: self.backend_contract_digest.clone(),
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-sandbox-microvm-provider failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    if !cfg!(target_os = "linux") {
        return Err(ProcessError::LinuxRequired);
    }
    let config = ProviderProcessConfig::load()?;
    let profile = checked_in_hard_limit_profile();
    let sandbox_limits = SandboxCommandLimits::from_profile(&profile)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let rpc_limits = SandboxInternalRpcLimits::from_profile(&profile)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let provider_process_generation_id =
        ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, uuid::Uuid::now_v7())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let broker = Arc::new(SandboxMicroVmBrokerGrpcClient::new(
        connect_broker(&config).await?,
        rpc_limits,
    ));
    let grant_revoker: Arc<dyn MicroVmGrantRevoker> = broker;
    let host = Arc::new(
        LinuxFirecrackerHostFactory::install(
            LinuxFirecrackerHostConfig {
                installation: config.installation.clone(),
                runtimes: config.runtimes.clone(),
                provider_process_generation_id,
                state_directory: PathBuf::from(&config.state_directory),
                ephemeral_uid_base: config.ephemeral_uid_base,
                ephemeral_gid_base: config.ephemeral_gid_base,
                ephemeral_identity_count: config.ephemeral_identity_count,
                maximum_instances: config.maximum_instances,
                maximum_tombstones: config.maximum_tombstones,
                tombstone_retention: Duration::from_secs(config.tombstone_retention_seconds),
                api_timeout: Duration::from_millis(config.api_timeout_milliseconds),
                guest_channel_timeout: Duration::from_millis(
                    config.guest_channel_timeout_milliseconds,
                ),
                socket_poll_interval: Duration::from_millis(config.socket_poll_milliseconds),
                process_termination_timeout: Duration::from_millis(
                    config.process_termination_timeout_milliseconds,
                ),
            },
            grant_revoker,
        )
        .await
        .map_err(|_| ProcessError::HostUnavailable)?,
    );
    let lifecycle = Arc::new(
        DurableMicroVmLifecycle::new(host, config.maximum_lifecycle_entries)
            .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let protocol = Arc::new(ManagedMcpSandboxProtocolAdapter::new(
        InstalledMcpToolCodecRegistry::default(),
        sandbox_limits,
    ));
    let backend = Arc::new(
        MicroVmSandboxExecutorBackend::new(
            config.descriptor(),
            sandbox_limits,
            lifecycle,
            protocol,
            Arc::new(SystemMicroVmProviderClock),
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let maximum = rpc_limits.maximum_message_bytes();
    let service = SandboxIsolationProviderServiceServer::new(
        SandboxIsolationProviderGrpcService::new(backend, rpc_limits, sandbox_limits)
            .map_err(|_| ProcessError::InvalidConfiguration)?,
    )
    .max_encoding_message_size(maximum)
    .max_decoding_message_size(maximum);
    let service = tonic::service::interceptor::InterceptedService::new(
        service,
        MicroVmExecutorWorkloadIdentity,
    );
    let tls = server_tls(&config)?;
    let socket_path = PathBuf::from(&config.provider_socket_path);
    let listener = bind_provider_socket(&socket_path)?;
    let _socket_guard = ProviderSocketGuard(socket_path);
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
    let server = Server::builder()
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidTls)?
        .add_service(service)
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async {
            let _ = shutdown_receiver.await;
        });
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result.map_err(|_| ProcessError::RpcUnavailable),
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|_| ProcessError::SignalUnavailable)?;
            let _ = shutdown_sender.send(());
            tokio::time::timeout(
                Duration::from_millis(config.shutdown_grace_milliseconds),
                &mut server,
            )
            .await
            .map_err(|_| ProcessError::ShutdownDeadlineExceeded)?
            .map_err(|_| ProcessError::RpcUnavailable)
        }
    }
}

async fn connect_broker(
    config: &ProviderProcessConfig,
) -> Result<tonic::transport::Channel, ProcessError> {
    let ca = read_bounded(
        &required_absolute_path(BROKER_CA_PATH_ENV, 4_096)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let certificate = read_bounded(
        &required_absolute_path(BROKER_CERT_PATH_ENV, 4_096)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let key = read_bounded(
        &required_absolute_path(BROKER_KEY_PATH_ENV, 4_096)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let tls = ClientTlsConfig::new()
        .domain_name(config.controller_broker_tls_server_name.clone())
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(certificate, key));
    Endpoint::from_shared(config.controller_broker_endpoint.clone())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(config.connect_timeout_milliseconds))
        .timeout(Duration::from_millis(config.request_timeout_milliseconds))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect()
        .await
        .map_err(|_| ProcessError::BrokerUnavailable)
}

fn server_tls(config: &ProviderProcessConfig) -> Result<ServerTlsConfig, ProcessError> {
    let ca = read_bounded(
        &required_absolute_path(SERVER_CLIENT_CA_PATH_ENV, 4_096)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let certificate = read_bounded(
        &required_absolute_path(SERVER_CERT_PATH_ENV, 4_096)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let key = read_bounded(
        &required_absolute_path(SERVER_KEY_PATH_ENV, 4_096)?,
        MAX_TLS_FILE_BYTES,
    )?;
    Ok(ServerTlsConfig::new()
        .identity(Identity::from_pem(certificate, key))
        .client_ca_root(Certificate::from_pem(ca))
        .timeout(Duration::from_millis(
            config.tls_handshake_timeout_milliseconds,
        )))
}

fn bind_provider_socket(path: &Path) -> Result<UnixListener, ProcessError> {
    let parent = path
        .parent()
        .filter(|parent| !is_broad_directory(parent))
        .ok_or(ProcessError::InvalidConfiguration)?;
    let parent_metadata =
        std::fs::symlink_metadata(parent).map_err(|_| ProcessError::ProviderSocketUnavailable)?;
    if !parent_metadata.is_dir() || parent_metadata.file_type().is_symlink() {
        return Err(ProcessError::ProviderSocketUnavailable);
    }
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            std::fs::remove_file(path).map_err(|_| ProcessError::ProviderSocketUnavailable)?;
        }
        Ok(_) => return Err(ProcessError::ProviderSocketUnavailable),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(ProcessError::ProviderSocketUnavailable),
    }
    let listener = UnixListener::bind(path).map_err(|_| ProcessError::ProviderSocketUnavailable)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660))
        .map_err(|_| ProcessError::ProviderSocketUnavailable)?;
    Ok(listener)
}

struct ProviderSocketGuard(PathBuf);

impl Drop for ProviderSocketGuard {
    fn drop(&mut self) {
        if std::fs::symlink_metadata(&self.0)
            .ok()
            .is_some_and(|metadata| metadata.file_type().is_socket())
        {
            let _ = std::fs::remove_file(&self.0);
        }
    }
}

fn required(name: &'static str) -> Result<String, ProcessError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or(ProcessError::MissingConfiguration(name))
}

fn required_absolute_path(
    name: &'static str,
    maximum_bytes: usize,
) -> Result<PathBuf, ProcessError> {
    let path = PathBuf::from(required(name)?);
    if !closed_absolute_path(&path, maximum_bytes) || is_broad_directory(&path) {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(path)
}

fn read_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>, ProcessError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| ProcessError::UnreadableMaterial)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(ProcessError::UnreadableMaterial);
    }
    let bytes = std::fs::read(path).map_err(|_| ProcessError::UnreadableMaterial)?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(ProcessError::UnreadableMaterial);
    }
    Ok(bytes)
}

fn closed_absolute_path(path: &Path, maximum_bytes: usize) -> bool {
    path.is_absolute()
        && path.as_os_str().as_encoded_bytes().len() <= maximum_bytes
        && !path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
}

fn is_broad_directory(path: &Path) -> bool {
    path == Path::new("/")
        || path == Path::new("/run")
        || path == Path::new("/var")
        || path == Path::new("/tmp")
}

fn bounded_milliseconds(value: u64, maximum: u64) -> bool {
    value > 0 && value <= maximum
}

fn stable_dns_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value == value.to_ascii_lowercase()
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
}

fn closed_service_endpoint(value: &str, scheme: &str) -> bool {
    let Ok(endpoint) = url::Url::parse(value) else {
        return false;
    };
    endpoint.scheme() == scheme
        && endpoint.host_str().is_some_and(stable_dns_name)
        && endpoint.port().is_some()
        && endpoint.username().is_empty()
        && endpoint.password().is_none()
        && endpoint.query().is_none()
        && endpoint.fragment().is_none()
        && matches!(endpoint.path(), "" | "/")
}

#[derive(Debug)]
enum ProcessError {
    MissingConfiguration(&'static str),
    InvalidConfiguration,
    UnreadableMaterial,
    LinuxRequired,
    BrokerUnavailable,
    HostUnavailable,
    ProviderSocketUnavailable,
    InvalidTls,
    RpcUnavailable,
    SignalUnavailable,
    ShutdownDeadlineExceeded,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingConfiguration(name) => {
                write!(formatter, "required setting {name} is absent")
            }
            Self::InvalidConfiguration => formatter.write_str("configuration is invalid"),
            Self::UnreadableMaterial => formatter.write_str("configuration material is unreadable"),
            Self::LinuxRequired => formatter.write_str("the microVM Provider requires Linux"),
            Self::BrokerUnavailable => formatter.write_str("the microVM broker is unavailable"),
            Self::HostUnavailable => formatter.write_str("the Firecracker host is unavailable"),
            Self::ProviderSocketUnavailable => {
                formatter.write_str("the provider Unix socket is unavailable")
            }
            Self::InvalidTls => formatter.write_str("provider TLS configuration is invalid"),
            Self::RpcUnavailable => formatter.write_str("provider RPC server is unavailable"),
            Self::SignalUnavailable => formatter.write_str("shutdown signal handling failed"),
            Self::ShutdownDeadlineExceeded => {
                formatter.write_str("provider shutdown deadline exceeded")
            }
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn config() -> ProviderProcessConfig {
        ProviderProcessConfig {
            schema_version: 1,
            provider_socket_path: "/run/insight-sandbox-provider/provider.sock".to_owned(),
            provider_tls_server_name: "sandbox-provider.platform.svc".to_owned(),
            controller_broker_endpoint: "https://sandbox-controller.platform.svc:7443".to_owned(),
            controller_broker_tls_server_name: "sandbox-controller.platform.svc".to_owned(),
            worker_manifest_digest: digest('a'),
            backend_contract_digest: digest('b'),
            installation: FirecrackerInstallation {
                version: "1.12.1".to_owned(),
                firecracker_path: "/opt/insight/firecracker/firecracker".into(),
                firecracker_digest: digest('c'),
                jailer_path: "/opt/insight/firecracker/jailer".into(),
                jailer_digest: digest('d'),
                chroot_base_directory: "/srv/insight-firecracker-jails".into(),
                parent_cgroup: "insight-sandbox".to_owned(),
            },
            runtimes: vec![InstalledMicroVmRuntime {
                runtime_family: insight_platform_contracts::SandboxRuntimeFamily::Python,
                runtime_digest: digest('e'),
                guest_kernel_digest: digest('f'),
                guest_agent_digest: digest('1'),
                guest_kernel_path: "/opt/insight/runtimes/python/vmlinux".into(),
                rootfs_path: "/opt/insight/runtimes/python/rootfs.ext4".into(),
                rootfs_bytes: 1024,
            }],
            state_directory: "/var/lib/insight-sandbox-provider".to_owned(),
            ephemeral_uid_base: 100_000,
            ephemeral_gid_base: 100_000,
            ephemeral_identity_count: 1_024,
            maximum_instances: 128,
            maximum_tombstones: 512,
            tombstone_retention_seconds: 86_400,
            maximum_lifecycle_entries: 256,
            api_timeout_milliseconds: 3_000,
            guest_channel_timeout_milliseconds: 10_000,
            socket_poll_milliseconds: 25,
            process_termination_timeout_milliseconds: 5_000,
            connect_timeout_milliseconds: 3_000,
            request_timeout_milliseconds: 10_000,
            tls_handshake_timeout_milliseconds: 3_000,
            shutdown_grace_milliseconds: 10_000,
        }
    }

    #[test]
    fn config_is_closed_and_rejects_broad_paths_or_uninstalled_managed_mcp() {
        config().validate().unwrap();

        let mut broad = config();
        broad.state_directory = "/".to_owned();
        assert!(broad.validate().is_err());

        let mut remote_provider = config();
        remote_provider.provider_socket_path = "https://provider.platform.svc".to_owned();
        assert!(remote_provider.validate().is_err());

        let mut managed = config();
        managed.runtimes[0].runtime_family =
            insight_platform_contracts::SandboxRuntimeFamily::ManagedMcpServer;
        assert!(managed.validate().is_err());
    }
}
