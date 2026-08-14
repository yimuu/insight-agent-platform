//! Dedicated trusted Sandbox Controller composition.
//!
//! This process owns PostgreSQL authority and the Artifact S3/KMS broker. It never loads an
//! untrusted runtime. The Executor connects over mandatory mTLS and the Controller delegates old
//! process-generation absence proof to an independently deployed attestor.

use insight_platform_artifact_broker::{
    ArtifactBrokerLimits, AwsArtifactProviderCatalog, AwsArtifactProviderCatalogConfig,
    BrokeredSandboxArtifactBroker,
};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, JsonLimits, Sha256Digest,
};
use insight_platform_postgres::{repository::PgRepository, verify_schema};
use insight_platform_sandbox::{
    NodeAttestorRoute, ProveWasiProcessGenerationAbsent, VerifyWasiExecutorProcessGeneration,
    WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrationError,
    WasiExecutorProcessRegistrationVerifier, WasiProcessGenerationAbsenceEvidence,
    WasiProcessGenerationIsolation, WasiProcessGenerationIsolationError,
};
use insight_platform_sandbox_rpc::{
    proto::{
        sandbox_executor_authority_service_server::SandboxExecutorAuthorityServiceServer,
        sandbox_executor_broker_service_server::SandboxExecutorBrokerServiceServer,
        sandbox_managed_mcp_session_authority_service_server::SandboxManagedMcpSessionAuthorityServiceServer,
        sandbox_micro_vm_broker_service_server::SandboxMicroVmBrokerServiceServer,
    },
    MicroVmExecutorWorkloadIdentity, MicroVmProviderWorkloadIdentity, SandboxAuthorityGrpcService,
    SandboxBrokerGrpcService, SandboxExecutorAuthorityWorkloadIdentity, SandboxInternalRpcLimits,
    SandboxManagedMcpSessionAuthorityGrpcService, SandboxMicroVmBrokerGrpcService,
    SandboxProcessIsolationAttestorGrpcClient, WasiExecutorWorkloadIdentity,
};
use ipnet::IpNet;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{
    collections::BTreeMap, error::Error, fmt, net::SocketAddr, path::PathBuf, sync::Arc,
    time::Duration,
};
use tokio::sync::RwLock;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig};

const CONFIG_PATH_ENV: &str = "PLATFORM_SANDBOX_CONTROLLER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_SANDBOX_CONTROLLER_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_DATABASE_URL";
const SERVER_CA_PATH_ENV: &str = "PLATFORM_SANDBOX_GRPC_CLIENT_CA_PATH";
const SERVER_CERT_PATH_ENV: &str = "PLATFORM_SANDBOX_GRPC_CERT_PATH";
const SERVER_KEY_PATH_ENV: &str = "PLATFORM_SANDBOX_GRPC_KEY_PATH";
const ATTESTOR_CA_PATH_ENV: &str = "PLATFORM_SANDBOX_ATTESTOR_CA_PATH";
const ATTESTOR_CERT_PATH_ENV: &str = "PLATFORM_SANDBOX_ATTESTOR_CERT_PATH";
const ATTESTOR_KEY_PATH_ENV: &str = "PLATFORM_SANDBOX_ATTESTOR_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 1024 * 1024;
const MAX_TLS_FILE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControllerProcessConfig {
    schema_version: u32,
    listen_address: String,
    database_max_connections: u32,
    artifact_provider_catalog: AwsArtifactProviderCatalogConfig,
    artifact_broker: ArtifactBrokerProcessConfig,
    process_isolation_attestor: AttestorProcessConfig,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
    shutdown_grace_milliseconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactBrokerProcessConfig {
    maximum_in_flight: usize,
    maximum_read_bytes: usize,
    operation_timeout_milliseconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct AttestorProcessConfig {
    tls_server_name: String,
    attestor_identity_digest: Sha256Digest,
    maximum_cached_routes: usize,
    controller_port: u16,
    allowed_node_cidrs: Vec<String>,
}

impl ControllerProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 24,
                max_items_per_array: 128,
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
        self.artifact_provider_catalog
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let _listen: SocketAddr = self
            .listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || !(2..=64).contains(&self.database_max_connections)
            || self.artifact_broker.maximum_in_flight == 0
            || self.artifact_broker.maximum_read_bytes == 0
            || self.artifact_broker.operation_timeout_milliseconds == 0
            || !stable_dns_name(&self.process_isolation_attestor.tls_server_name)
            || self.process_isolation_attestor.maximum_cached_routes == 0
            || self.process_isolation_attestor.maximum_cached_routes > 10_000
            || self.process_isolation_attestor.controller_port == 0
            || self
                .process_isolation_attestor
                .allowed_node_cidrs
                .is_empty()
            || self.process_isolation_attestor.allowed_node_cidrs.len() > 32
            || self.connect_timeout_milliseconds == 0
            || self.request_timeout_milliseconds < self.connect_timeout_milliseconds
            || self.shutdown_grace_milliseconds == 0
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        let mut cidrs = self
            .process_isolation_attestor
            .allowed_node_cidrs
            .iter()
            .map(|value| value.parse::<IpNet>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        cidrs.sort();
        cidrs.dedup();
        if cidrs.len() != self.process_isolation_attestor.allowed_node_cidrs.len()
            || cidrs.iter().any(|network| !private_network(network))
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        ArtifactBrokerLimits {
            maximum_in_flight: self.artifact_broker.maximum_in_flight,
            maximum_read_bytes: self.artifact_broker.maximum_read_bytes,
            operation_timeout: Duration::from_millis(
                self.artifact_broker.operation_timeout_milliseconds,
            ),
        }
        .validate()
        .map_err(|_| ProcessError::InvalidConfiguration)
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-sandbox-controller failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = ControllerProcessConfig::load()?;
    let limits = SandboxInternalRpcLimits::from_profile(&checked_in_hard_limit_profile())
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let database_url = required(DATABASE_URL_ENV)?;
    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(Duration::from_millis(config.request_timeout_milliseconds))
        .connect(&database_url)
        .await
        .map_err(|_| ProcessError::DatabaseUnavailable)?;
    verify_schema(&pool)
        .await
        .map_err(|_| ProcessError::SchemaMismatch)?;
    let repository = Arc::new(PgRepository::new(pool));

    let providers = AwsArtifactProviderCatalog::install(config.artifact_provider_catalog.clone())
        .await
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    providers
        .check_readiness()
        .await
        .map_err(|_| ProcessError::ArtifactProviderUnavailable)?;
    let (unsealer, stores) = providers.into_components();
    let artifacts = Arc::new(
        BrokeredSandboxArtifactBroker::new(
            repository.clone(),
            repository.clone(),
            unsealer,
            stores,
            ArtifactBrokerLimits {
                maximum_in_flight: config.artifact_broker.maximum_in_flight,
                maximum_read_bytes: config.artifact_broker.maximum_read_bytes,
                operation_timeout: Duration::from_millis(
                    config.artifact_broker.operation_timeout_milliseconds,
                ),
            },
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );

    let process_isolation = Arc::new(RoutedProcessAttestor::new(&config, limits)?);

    let maximum = limits.maximum_message_bytes();
    let authority_service = SandboxExecutorAuthorityServiceServer::new(
        SandboxAuthorityGrpcService::new(repository.clone(), process_isolation.clone(), limits),
    )
    .max_encoding_message_size(maximum)
    .max_decoding_message_size(maximum);
    let broker_service = SandboxExecutorBrokerServiceServer::new(SandboxBrokerGrpcService::new(
        artifacts.clone(),
        repository.clone(),
        repository.clone(),
        process_isolation.clone(),
        limits,
    ))
    .max_encoding_message_size(maximum)
    .max_decoding_message_size(maximum);
    let managed_session_authority_service = SandboxManagedMcpSessionAuthorityServiceServer::new(
        SandboxManagedMcpSessionAuthorityGrpcService::new(
            repository.clone(),
            process_isolation.clone(),
            limits,
        ),
    )
    .max_encoding_message_size(maximum)
    .max_decoding_message_size(maximum);
    let micro_vm_broker_service = SandboxMicroVmBrokerServiceServer::new(
        SandboxMicroVmBrokerGrpcService::new(artifacts, repository.clone(), limits),
    )
    .max_encoding_message_size(maximum)
    .max_decoding_message_size(maximum);
    let authority_service = tonic::service::interceptor::InterceptedService::new(
        authority_service,
        SandboxExecutorAuthorityWorkloadIdentity,
    );
    let broker_service = tonic::service::interceptor::InterceptedService::new(
        broker_service,
        WasiExecutorWorkloadIdentity,
    );
    let managed_session_authority_service = tonic::service::interceptor::InterceptedService::new(
        managed_session_authority_service,
        MicroVmExecutorWorkloadIdentity,
    );
    let micro_vm_broker_service = tonic::service::interceptor::InterceptedService::new(
        micro_vm_broker_service,
        MicroVmProviderWorkloadIdentity,
    );

    let ca = read_bounded(
        &required_absolute_path(SERVER_CA_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let certificate = read_bounded(
        &required_absolute_path(SERVER_CERT_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let key = read_bounded(
        &required_absolute_path(SERVER_KEY_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(certificate, key))
        .client_ca_root(Certificate::from_pem(ca))
        .timeout(Duration::from_millis(config.connect_timeout_milliseconds));
    let address: SocketAddr = config
        .listen_address
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
    let server = Server::builder()
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidTls)?
        .add_service(authority_service)
        .add_service(broker_service)
        .add_service(managed_session_authority_service)
        .add_service(micro_vm_broker_service)
        .serve_with_shutdown(address, async {
            let _ = shutdown_receiver.await;
        });
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result.map_err(|_| ProcessError::RpcUnavailable),
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|_| ProcessError::RpcUnavailable)?;
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

struct RoutedProcessAttestor {
    limits: SandboxInternalRpcLimits,
    attestor_identity_digest: Sha256Digest,
    tls: ClientTlsConfig,
    connect_timeout: Duration,
    request_timeout: Duration,
    maximum_cached_routes: usize,
    controller_port: u16,
    allowed_node_cidrs: Vec<IpNet>,
    channels: RwLock<BTreeMap<NodeAttestorRoute, tonic::transport::Channel>>,
}

impl RoutedProcessAttestor {
    fn new(
        config: &ControllerProcessConfig,
        limits: SandboxInternalRpcLimits,
    ) -> Result<Self, ProcessError> {
        let ca = read_bounded(
            &required_absolute_path(ATTESTOR_CA_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?;
        let certificate = read_bounded(
            &required_absolute_path(ATTESTOR_CERT_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?;
        let key = read_bounded(
            &required_absolute_path(ATTESTOR_KEY_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?;
        let tls = ClientTlsConfig::new()
            .domain_name(&config.process_isolation_attestor.tls_server_name)
            .ca_certificate(Certificate::from_pem(ca))
            .identity(Identity::from_pem(certificate, key))
            .timeout(Duration::from_millis(config.connect_timeout_milliseconds));
        Ok(Self {
            limits,
            attestor_identity_digest: config
                .process_isolation_attestor
                .attestor_identity_digest
                .clone(),
            tls,
            connect_timeout: Duration::from_millis(config.connect_timeout_milliseconds),
            request_timeout: Duration::from_millis(config.request_timeout_milliseconds),
            maximum_cached_routes: config.process_isolation_attestor.maximum_cached_routes,
            controller_port: config.process_isolation_attestor.controller_port,
            allowed_node_cidrs: config
                .process_isolation_attestor
                .allowed_node_cidrs
                .iter()
                .map(|value| value.parse())
                .collect::<Result<_, _>>()
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            channels: RwLock::new(BTreeMap::new()),
        })
    }

    async fn client_for(
        &self,
        route: &NodeAttestorRoute,
    ) -> Result<SandboxProcessIsolationAttestorGrpcClient, ()> {
        let address = route.socket_address();
        if address.port() != self.controller_port
            || !self
                .allowed_node_cidrs
                .iter()
                .any(|network| network.contains(&address.ip()))
        {
            return Err(());
        }
        if let Some(channel) = self.channels.read().await.get(route).cloned() {
            return Ok(SandboxProcessIsolationAttestorGrpcClient::new(
                channel,
                self.limits,
                self.attestor_identity_digest.clone(),
            ));
        }
        if self.channels.read().await.len() >= self.maximum_cached_routes {
            return Err(());
        }
        let channel = Endpoint::from_shared(route.to_string())
            .map_err(|_| ())?
            .tls_config(self.tls.clone())
            .map_err(|_| ())?
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout)
            .connect()
            .await
            .map_err(|_| ())?;
        let mut channels = self.channels.write().await;
        if let Some(channel) = channels.get(route).cloned() {
            return Ok(SandboxProcessIsolationAttestorGrpcClient::new(
                channel,
                self.limits,
                self.attestor_identity_digest.clone(),
            ));
        }
        if channels.len() >= self.maximum_cached_routes {
            return Err(());
        }
        let channel = channels.entry(route.clone()).or_insert(channel).clone();
        Ok(SandboxProcessIsolationAttestorGrpcClient::new(
            channel,
            self.limits,
            self.attestor_identity_digest.clone(),
        ))
    }
}

#[tonic::async_trait]
impl WasiExecutorProcessRegistrationVerifier for RoutedProcessAttestor {
    async fn verify_registered(
        &self,
        request: VerifyWasiExecutorProcessGeneration,
    ) -> Result<WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrationError> {
        let client = self
            .client_for(&request.attestor_route)
            .await
            .map_err(|_| WasiExecutorProcessRegistrationError::Unavailable)?;
        client.verify_registered(request).await
    }
}

#[tonic::async_trait]
impl WasiProcessGenerationIsolation for RoutedProcessAttestor {
    async fn prove_absent(
        &self,
        request: ProveWasiProcessGenerationAbsent,
    ) -> Result<WasiProcessGenerationAbsenceEvidence, WasiProcessGenerationIsolationError> {
        let client = self
            .client_for(&request.attestor_route)
            .await
            .map_err(|_| WasiProcessGenerationIsolationError::Unavailable)?;
        client.prove_absent(request).await
    }
}

fn required(name: &str) -> Result<String, ProcessError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or(ProcessError::InvalidConfiguration)
}

fn required_absolute_path(name: &str) -> Result<PathBuf, ProcessError> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(path)
}

fn read_bounded(path: &PathBuf, maximum: usize) -> Result<Vec<u8>, ProcessError> {
    let metadata = std::fs::metadata(path).map_err(|_| ProcessError::InvalidConfiguration)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > maximum as u64 {
        return Err(ProcessError::InvalidConfiguration);
    }
    let bytes = std::fs::read(path).map_err(|_| ProcessError::InvalidConfiguration)?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}

fn stable_dns_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn private_network(network: &IpNet) -> bool {
    ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fc00::/7"]
        .into_iter()
        .map(|value| value.parse::<IpNet>().expect("constant private CIDR"))
        .any(|private| {
            private.addr().is_ipv4() == network.addr().is_ipv4()
                && network.prefix_len() >= private.prefix_len()
                && private.contains(&network.addr())
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessError {
    InvalidConfiguration,
    DatabaseUnavailable,
    SchemaMismatch,
    ArtifactProviderUnavailable,
    InvalidTls,
    RpcUnavailable,
    ShutdownDeadlineExceeded,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "invalid Controller configuration",
            Self::DatabaseUnavailable => "PostgreSQL authority is unavailable",
            Self::SchemaMismatch => "PostgreSQL authority schema is incompatible",
            Self::ArtifactProviderUnavailable => "Artifact S3/KMS provider is unavailable",
            Self::InvalidTls => "Controller TLS configuration is invalid",
            Self::RpcUnavailable => "Controller RPC server failed",
            Self::ShutdownDeadlineExceeded => "Controller graceful shutdown deadline exceeded",
        })
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attestor_routes_and_dns_names_are_closed() {
        assert!("https://10.0.0.7:9443".parse::<NodeAttestorRoute>().is_ok());
        assert!("https://[fd00::7]:9443"
            .parse::<NodeAttestorRoute>()
            .is_ok());
        assert!("https://8.8.8.8:9443".parse::<NodeAttestorRoute>().is_err());
        assert!("https://attestor.platform.svc:9443"
            .parse::<NodeAttestorRoute>()
            .is_err());
        assert!(stable_dns_name("sandbox-attestor.platform.svc"));
        assert!(!stable_dns_name("sandbox_attestor.platform.svc"));
        assert!(!stable_dns_name("-sandbox-attestor.platform.svc"));
        assert!(!stable_dns_name("sandbox-attestor-.platform.svc"));
        for valid in [
            "10.20.0.0/16",
            "172.16.4.0/24",
            "192.168.0.0/16",
            "fd42::/64",
        ] {
            assert!(private_network(&valid.parse().unwrap()), "rejected {valid}");
        }
        for invalid in [
            "0.0.0.0/0",
            "10.0.0.0/7",
            "172.0.0.0/8",
            "8.8.8.0/24",
            "::/0",
        ] {
            assert!(
                !private_network(&invalid.parse().unwrap()),
                "accepted {invalid}"
            );
        }
    }
}
