//! Dedicated node/runtime Sandbox process attestor.
//!
//! Registration is node-local UDS+mTLS and receives kernel peer credentials. Controller
//! verification and absence proof use a separate mTLS TCP listener. The process has no database,
//! NATS, Kubernetes API or Sandbox Job mutation authority.

use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, JsonLimits, Sha256Digest,
};
use insight_platform_sandbox_attestor::{
    LinuxProcfsExecutorProcessObserver, NodeAttestorConfig, NodeProcessAttestor,
};
use insight_platform_sandbox_rpc::{
    proto::{
        sandbox_executor_process_registration_service_server::SandboxExecutorProcessRegistrationServiceServer,
        sandbox_micro_vm_executor_process_registration_service_server::SandboxMicroVmExecutorProcessRegistrationServiceServer,
        sandbox_process_isolation_attestor_service_server::SandboxProcessIsolationAttestorServiceServer,
    },
    MicroVmExecutorNodeRegistrationIdentity, SandboxControllerWorkloadIdentity,
    SandboxExecutorProcessRegistrationGrpcService, SandboxInternalRpcLimits,
    SandboxProcessIsolationAttestorGrpcService, WasiExecutorNodeRegistrationIdentity,
};
use serde::Deserialize;
use std::{
    error::Error,
    fmt,
    net::SocketAddr,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

const CONFIG_PATH_ENV: &str = "PLATFORM_SANDBOX_ATTESTOR_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_SANDBOX_ATTESTOR_CONFIG_DIGEST";
const REGISTRATION_CA_PATH_ENV: &str = "PLATFORM_SANDBOX_ATTESTOR_EXECUTOR_CA_PATH";
const REGISTRATION_CERT_PATH_ENV: &str = "PLATFORM_SANDBOX_ATTESTOR_REGISTRATION_CERT_PATH";
const REGISTRATION_KEY_PATH_ENV: &str = "PLATFORM_SANDBOX_ATTESTOR_REGISTRATION_KEY_PATH";
const CONTROLLER_CA_PATH_ENV: &str = "PLATFORM_SANDBOX_ATTESTOR_CONTROLLER_CA_PATH";
const CONTROLLER_CERT_PATH_ENV: &str = "PLATFORM_SANDBOX_ATTESTOR_CONTROLLER_CERT_PATH";
const CONTROLLER_KEY_PATH_ENV: &str = "PLATFORM_SANDBOX_ATTESTOR_CONTROLLER_KEY_PATH";
const ADVERTISED_IP_ENV: &str = "PLATFORM_SANDBOX_ATTESTOR_ADVERTISED_IP";
const MAX_CONFIG_BYTES: usize = 64 * 1024;
const MAX_TLS_FILE_BYTES: usize = 1024 * 1024;
const REGISTRATION_SOCKET_MODE: u32 = 0o660;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct AttestorProcessConfig {
    schema_version: u32,
    registration_socket_path: String,
    controller_listen_address: String,
    proc_root: String,
    node_uid_authority_path: String,
    registry_path: String,
    maximum_registrations: usize,
    absent_retention_seconds: u64,
    attestor_identity_digest: Sha256Digest,
    tls_handshake_timeout_milliseconds: u64,
    shutdown_grace_milliseconds: u64,
}

impl AttestorProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 12,
                max_items_per_array: 8,
                max_properties_per_object: 32,
                max_string_bytes: 4_096,
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
        let registration_socket_path = Path::new(&self.registration_socket_path);
        let proc_root = Path::new(&self.proc_root);
        let node_uid_authority_path = Path::new(&self.node_uid_authority_path);
        let registry_path = Path::new(&self.registry_path);
        let controller: SocketAddr = self
            .controller_listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let profile = checked_in_hard_limit_profile();
        let minimum_retention = profile
            .capability_sandbox
            .wall_seconds
            .hard_max
            .checked_add(profile.capability_sandbox.cleanup_seconds.hard_max)
            .ok_or(ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || !closed_absolute_path(registration_socket_path, 100)
            || !closed_absolute_path(proc_root, 4_096)
            || !closed_absolute_path(node_uid_authority_path, 4_096)
            || !closed_absolute_path(registry_path, 4_096)
            || registration_socket_path == registry_path
            || controller.port() == 0
            || self.maximum_registrations == 0
            || self.maximum_registrations > 100_000
            || self.absent_retention_seconds < minimum_retention
            || self.tls_handshake_timeout_milliseconds == 0
            || self.shutdown_grace_milliseconds == 0
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-sandbox-attestor failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = AttestorProcessConfig::load()?;
    let advertised_ip = required(ADVERTISED_IP_ENV)?
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let controller_address: SocketAddr = config
        .controller_listen_address
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let advertised_route = insight_platform_sandbox::NodeAttestorRoute::from_ip_port(
        advertised_ip,
        controller_address.port(),
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let observer = Arc::new(
        LinuxProcfsExecutorProcessObserver::new(
            PathBuf::from(&config.proc_root),
            PathBuf::from(&config.node_uid_authority_path),
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let authority = Arc::new(
        NodeProcessAttestor::open(
            observer,
            NodeAttestorConfig {
                attestor_identity_digest: config.attestor_identity_digest.clone(),
                attestor_route: advertised_route,
                registry_path: PathBuf::from(&config.registry_path),
                maximum_registrations: config.maximum_registrations,
                absent_retention: Duration::from_secs(config.absent_retention_seconds),
            },
        )
        .map_err(|_| ProcessError::RegistryUnavailable)?,
    );
    let limits = SandboxInternalRpcLimits::from_profile(&checked_in_hard_limit_profile())
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let maximum = limits.maximum_message_bytes();

    let socket_path = PathBuf::from(&config.registration_socket_path);
    let listener = bind_registration_socket(&socket_path)?;
    let _socket_guard = RegistrationSocketGuard(socket_path);
    let registration_service = SandboxExecutorProcessRegistrationServiceServer::new(
        SandboxExecutorProcessRegistrationGrpcService::new(Arc::clone(&authority), limits),
    )
    .max_encoding_message_size(maximum)
    .max_decoding_message_size(maximum);
    let registration_service = tonic::service::interceptor::InterceptedService::new(
        registration_service,
        WasiExecutorNodeRegistrationIdentity,
    );
    let micro_vm_registration_service =
        SandboxMicroVmExecutorProcessRegistrationServiceServer::new(
            SandboxExecutorProcessRegistrationGrpcService::new(Arc::clone(&authority), limits),
        )
        .max_encoding_message_size(maximum)
        .max_decoding_message_size(maximum);
    let micro_vm_registration_service = tonic::service::interceptor::InterceptedService::new(
        micro_vm_registration_service,
        MicroVmExecutorNodeRegistrationIdentity,
    );
    let registration_tls = server_tls(
        REGISTRATION_CA_PATH_ENV,
        REGISTRATION_CERT_PATH_ENV,
        REGISTRATION_KEY_PATH_ENV,
        config.tls_handshake_timeout_milliseconds,
    )?;

    let controller_service = SandboxProcessIsolationAttestorServiceServer::new(
        SandboxProcessIsolationAttestorGrpcService::new(authority, limits),
    )
    .max_encoding_message_size(maximum)
    .max_decoding_message_size(maximum);
    let controller_service = tonic::service::interceptor::InterceptedService::new(
        controller_service,
        SandboxControllerWorkloadIdentity,
    );
    let controller_tls = server_tls(
        CONTROLLER_CA_PATH_ENV,
        CONTROLLER_CERT_PATH_ENV,
        CONTROLLER_KEY_PATH_ENV,
        config.tls_handshake_timeout_milliseconds,
    )?;

    let shutdown = CancellationToken::new();
    let registration_shutdown = shutdown.child_token();
    let controller_shutdown = shutdown.child_token();
    let mut registration_server = tokio::spawn(async move {
        Server::builder()
            .tls_config(registration_tls)
            .map_err(|_| ProcessError::InvalidConfiguration)?
            .add_service(registration_service)
            .add_service(micro_vm_registration_service)
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                registration_shutdown.cancelled().await;
            })
            .await
            .map_err(|_| ProcessError::ServerFailed)
    });
    let mut controller_server = tokio::spawn(async move {
        Server::builder()
            .tls_config(controller_tls)
            .map_err(|_| ProcessError::InvalidConfiguration)?
            .add_service(controller_service)
            .serve_with_shutdown(controller_address, async move {
                controller_shutdown.cancelled().await;
            })
            .await
            .map_err(|_| ProcessError::ServerFailed)
    });

    tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|_| ProcessError::SignalUnavailable)?;
        }
        result = &mut registration_server => {
            result.map_err(|_| ProcessError::ServerFailed)??;
            return Err(ProcessError::ServerExitedUnexpectedly);
        }
        result = &mut controller_server => {
            result.map_err(|_| ProcessError::ServerFailed)??;
            return Err(ProcessError::ServerExitedUnexpectedly);
        }
    }
    shutdown.cancel();
    let grace = Duration::from_millis(config.shutdown_grace_milliseconds);
    tokio::time::timeout(grace, async {
        (&mut registration_server)
            .await
            .map_err(|_| ProcessError::ServerFailed)??;
        (&mut controller_server)
            .await
            .map_err(|_| ProcessError::ServerFailed)??;
        Ok::<_, ProcessError>(())
    })
    .await
    .map_err(|_| ProcessError::ShutdownTimeout)??;
    Ok(())
}

fn bind_registration_socket(path: &Path) -> Result<UnixListener, ProcessError> {
    let parent = path.parent().ok_or(ProcessError::InvalidConfiguration)?;
    if !parent.is_dir() {
        return Err(ProcessError::RegistrationSocketUnavailable);
    }
    remove_exact_stale_socket(path)?;
    let listener =
        UnixListener::bind(path).map_err(|_| ProcessError::RegistrationSocketUnavailable)?;
    std::fs::set_permissions(
        path,
        std::fs::Permissions::from_mode(REGISTRATION_SOCKET_MODE),
    )
    .map_err(|_| ProcessError::RegistrationSocketUnavailable)?;
    Ok(listener)
}

fn remove_exact_stale_socket(path: &Path) -> Result<(), ProcessError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(ProcessError::RegistrationSocketUnavailable),
    };
    // SAFETY: geteuid/getegid are side-effect-free process identity queries.
    let (effective_uid, effective_gid) = unsafe { (libc::geteuid(), libc::getegid()) };
    if !metadata.file_type().is_socket()
        || metadata.uid() != effective_uid
        || metadata.gid() != effective_gid
        || metadata.permissions().mode() & 0o777 != REGISTRATION_SOCKET_MODE
        || std::os::unix::net::UnixStream::connect(path).is_ok()
    {
        return Err(ProcessError::RegistrationSocketUnavailable);
    }
    std::fs::remove_file(path).map_err(|_| ProcessError::RegistrationSocketUnavailable)
}

fn server_tls(
    client_ca_env: &'static str,
    certificate_env: &'static str,
    key_env: &'static str,
    timeout_milliseconds: u64,
) -> Result<ServerTlsConfig, ProcessError> {
    let ca = read_bounded(&required_absolute_path(client_ca_env)?, MAX_TLS_FILE_BYTES)?;
    let certificate = read_bounded(
        &required_absolute_path(certificate_env)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let key = read_bounded(&required_absolute_path(key_env)?, MAX_TLS_FILE_BYTES)?;
    Ok(ServerTlsConfig::new()
        .identity(Identity::from_pem(certificate, key))
        .client_ca_root(Certificate::from_pem(ca))
        .timeout(Duration::from_millis(timeout_milliseconds)))
}

fn required(name: &'static str) -> Result<String, ProcessError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or(ProcessError::MissingConfiguration(name))
}

fn required_absolute_path(name: &'static str) -> Result<PathBuf, ProcessError> {
    let path = PathBuf::from(required(name)?);
    if !closed_absolute_path(&path, 4_096) {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(path)
}

fn closed_absolute_path(path: &Path, maximum_bytes: usize) -> bool {
    path.is_absolute()
        && path.as_os_str().as_encoded_bytes().len() <= maximum_bytes
        && path
            .components()
            .all(|component| !matches!(component, std::path::Component::ParentDir))
}

fn read_bounded(path: &Path, maximum_bytes: usize) -> Result<Vec<u8>, ProcessError> {
    let metadata = std::fs::metadata(path).map_err(|_| ProcessError::ConfigurationIo)?;
    if metadata.len() == 0 || metadata.len() > maximum_bytes as u64 || !metadata.is_file() {
        return Err(ProcessError::InvalidConfiguration);
    }
    let bytes = std::fs::read(path).map_err(|_| ProcessError::ConfigurationIo)?;
    if bytes.is_empty() || bytes.len() > maximum_bytes {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}

struct RegistrationSocketGuard(PathBuf);

impl Drop for RegistrationSocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[derive(Debug)]
enum ProcessError {
    MissingConfiguration(&'static str),
    InvalidConfiguration,
    ConfigurationIo,
    RegistryUnavailable,
    RegistrationSocketUnavailable,
    ServerFailed,
    ServerExitedUnexpectedly,
    SignalUnavailable,
    ShutdownTimeout,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingConfiguration(name) => write!(formatter, "missing configuration {name}"),
            Self::InvalidConfiguration => formatter.write_str("configuration is invalid"),
            Self::ConfigurationIo => formatter.write_str("configuration I/O failed"),
            Self::RegistryUnavailable => formatter.write_str("attestor registry unavailable"),
            Self::RegistrationSocketUnavailable => {
                formatter.write_str("registration socket unavailable")
            }
            Self::ServerFailed => formatter.write_str("attestor server failed"),
            Self::ServerExitedUnexpectedly => formatter.write_str("attestor server exited"),
            Self::SignalUnavailable => formatter.write_str("shutdown signal unavailable"),
            Self::ShutdownTimeout => formatter.write_str("attestor shutdown timed out"),
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

    fn config() -> AttestorProcessConfig {
        AttestorProcessConfig {
            schema_version: 1,
            registration_socket_path: "/run/insight-sandbox-attestor/registration.sock".into(),
            controller_listen_address: "127.0.0.1:7444".into(),
            proc_root: "/host/proc".into(),
            node_uid_authority_path: "/var/run/insight/node-uid".into(),
            registry_path: "/var/lib/insight-sandbox-attestor/registrations.json".into(),
            maximum_registrations: 4096,
            absent_retention_seconds: 3900,
            attestor_identity_digest: digest('a'),
            tls_handshake_timeout_milliseconds: 5000,
            shutdown_grace_milliseconds: 10_000,
        }
    }

    #[test]
    fn configuration_requires_node_local_paths_and_hard_retention_floor() {
        config().validate().unwrap();
        let mut invalid = config();
        invalid.registration_socket_path = "registration.sock".into();
        assert!(matches!(
            invalid.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));
        invalid = config();
        invalid.absent_retention_seconds = 3899;
        assert!(matches!(
            invalid.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));
        invalid = config();
        invalid.controller_listen_address = "0.0.0.0:0".into();
        assert!(matches!(
            invalid.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));
    }

    #[test]
    fn registration_socket_recovery_removes_only_exact_stale_socket() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("registration.sock");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660)).unwrap();
        assert!(matches!(
            remove_exact_stale_socket(&path),
            Err(ProcessError::RegistrationSocketUnavailable)
        ));
        drop(listener);
        remove_exact_stale_socket(&path).unwrap();
        assert!(!path.exists());

        std::fs::write(&path, b"not-a-socket").unwrap();
        assert!(matches!(
            remove_exact_stale_socket(&path),
            Err(ProcessError::RegistrationSocketUnavailable)
        ));
        assert!(path.exists());
    }
}
