//! Dedicated trusted Sandbox Controller composition.
//!
//! This process owns PostgreSQL mutation authority but no object-store or KMS credential. It never
//! loads an untrusted runtime. Executors connect over mandatory mTLS; exact Artifact bytes come
//! from the independently deployed Artifact Broker, and old process-generation absence proof comes
//! from the independently deployed attestor.

use insight_platform_artifact_rpc::{ArtifactInternalRpcLimits, ArtifactSandboxBrokerGrpcClient};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, JsonLimits, Sha256Digest,
};
use insight_platform_postgres::{repository::PgRepository, verify_schema};
use insight_platform_sandbox::{
    NodeAttestorRoute, ProveSandboxProcessGenerationAbsent,
    SandboxProcessGenerationAbsenceEvidence, SandboxProcessGenerationIsolation,
    SandboxProcessGenerationIsolationError, VerifyWasiExecutorProcessGeneration,
    WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrationError,
    WasiExecutorProcessRegistrationVerifier,
};
use insight_platform_sandbox_rpc::{
    proto::{
        sandbox_executor_authority_service_server::SandboxExecutorAuthorityServiceServer,
        sandbox_executor_broker_service_server::SandboxExecutorBrokerServiceServer,
        sandbox_managed_mcp_session_authority_service_server::SandboxManagedMcpSessionAuthorityServiceServer,
        sandbox_micro_vm_broker_service_server::SandboxMicroVmBrokerServiceServer,
        sandbox_secret_delivery_authority_service_server::SandboxSecretDeliveryAuthorityServiceServer,
    },
    EgressBrokerWorkloadIdentity, MicroVmExecutorWorkloadIdentity, MicroVmProviderWorkloadIdentity,
    SandboxArtifactResponseCapacity, SandboxAuthorityGrpcService, SandboxBrokerGrpcService,
    SandboxExecutorAuthorityWorkloadIdentity, SandboxInternalRpcLimits,
    SandboxManagedMcpSessionAuthorityGrpcService, SandboxMicroVmBrokerGrpcService,
    SandboxProcessIsolationAttestorGrpcClient, SandboxSecretDeliveryAuthorityGrpcService,
    WasiExecutorWorkloadIdentity,
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
const ARTIFACT_CA_PATH_ENV: &str = "PLATFORM_SANDBOX_ARTIFACT_CA_PATH";
const ARTIFACT_CERT_PATH_ENV: &str = "PLATFORM_SANDBOX_ARTIFACT_CERT_PATH";
const ARTIFACT_KEY_PATH_ENV: &str = "PLATFORM_SANDBOX_ARTIFACT_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 1024 * 1024;
const MAX_TLS_FILE_BYTES: usize = 1024 * 1024;
const MAX_IN_FLIGHT_ARTIFACT_RESPONSES_HARD: usize = 4;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControllerProcessConfig {
    schema_version: u32,
    listen_address: String,
    database_max_connections: u32,
    artifact_broker: ArtifactBrokerProcessConfig,
    process_isolation_attestor: AttestorProcessConfig,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
    shutdown_grace_milliseconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactBrokerProcessConfig {
    endpoint: String,
    tls_server_name: String,
    maximum_request_bytes: usize,
    maximum_chunk_bytes: usize,
    maximum_in_flight_responses: usize,
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
        let _listen: SocketAddr = self
            .listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || !(2..=64).contains(&self.database_max_connections)
            || !closed_https_endpoint(
                &self.artifact_broker.endpoint,
                &self.artifact_broker.tls_server_name,
            )
            || !(1..=MAX_IN_FLIGHT_ARTIFACT_RESPONSES_HARD)
                .contains(&self.artifact_broker.maximum_in_flight_responses)
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
        ArtifactInternalRpcLimits::new(
            self.artifact_broker.maximum_request_bytes,
            self.artifact_broker.maximum_chunk_bytes,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)
        .map(|_| ())
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

    let artifacts = Arc::new(connect_artifact_broker(&config).await?);

    let process_isolation = Arc::new(RoutedProcessAttestor::new(&config, limits)?);

    let maximum = limits.maximum_message_bytes();
    let authority_service = SandboxExecutorAuthorityServiceServer::new(
        SandboxAuthorityGrpcService::new(repository.clone(), process_isolation.clone(), limits),
    )
    .max_encoding_message_size(maximum)
    .max_decoding_message_size(maximum);
    let artifact_response_capacity =
        SandboxArtifactResponseCapacity::new(config.artifact_broker.maximum_in_flight_responses)
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let broker_service = SandboxExecutorBrokerServiceServer::new(SandboxBrokerGrpcService::new(
        artifacts.clone(),
        repository.clone(),
        repository.clone(),
        process_isolation.clone(),
        limits,
        artifact_response_capacity.clone(),
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
    let micro_vm_broker_service =
        SandboxMicroVmBrokerServiceServer::new(SandboxMicroVmBrokerGrpcService::new(
            artifacts,
            repository.clone(),
            limits,
            artifact_response_capacity,
        ))
        .max_encoding_message_size(maximum)
        .max_decoding_message_size(maximum);
    let secret_delivery_authority_service = SandboxSecretDeliveryAuthorityServiceServer::new(
        SandboxSecretDeliveryAuthorityGrpcService::new(repository.clone(), limits),
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
    let secret_delivery_authority_service = tonic::service::interceptor::InterceptedService::new(
        secret_delivery_authority_service,
        EgressBrokerWorkloadIdentity,
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
        .add_service(secret_delivery_authority_service)
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

async fn connect_artifact_broker(
    config: &ControllerProcessConfig,
) -> Result<ArtifactSandboxBrokerGrpcClient, ProcessError> {
    let ca = read_bounded(
        &required_absolute_path(ARTIFACT_CA_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let certificate = read_bounded(
        &required_absolute_path(ARTIFACT_CERT_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let key = read_bounded(
        &required_absolute_path(ARTIFACT_KEY_PATH_ENV)?,
        MAX_TLS_FILE_BYTES,
    )?;
    let tls = ClientTlsConfig::new()
        .domain_name(&config.artifact_broker.tls_server_name)
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(certificate, key))
        .timeout(Duration::from_millis(config.connect_timeout_milliseconds));
    let channel = Endpoint::from_shared(config.artifact_broker.endpoint.clone())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidTls)?
        .connect_timeout(Duration::from_millis(config.connect_timeout_milliseconds))
        .timeout(Duration::from_millis(config.request_timeout_milliseconds))
        .connect()
        .await
        .map_err(|_| ProcessError::ArtifactBrokerUnavailable)?;
    let limits = ArtifactInternalRpcLimits::new(
        config.artifact_broker.maximum_request_bytes,
        config.artifact_broker.maximum_chunk_bytes,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    Ok(ArtifactSandboxBrokerGrpcClient::new(channel, limits))
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
impl SandboxProcessGenerationIsolation for RoutedProcessAttestor {
    async fn prove_absent(
        &self,
        request: ProveSandboxProcessGenerationAbsent,
    ) -> Result<SandboxProcessGenerationAbsenceEvidence, SandboxProcessGenerationIsolationError>
    {
        let client = self
            .client_for(&request.attestor_route)
            .await
            .map_err(|_| SandboxProcessGenerationIsolationError::Unavailable)?;
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

fn closed_https_endpoint(value: &str, tls_server_name: &str) -> bool {
    let Ok(endpoint) = url::Url::parse(value) else {
        return false;
    };
    endpoint.scheme() == "https"
        && stable_dns_name(tls_server_name)
        && endpoint.host_str() == Some(tls_server_name)
        && endpoint.port().is_some()
        && endpoint.username().is_empty()
        && endpoint.password().is_none()
        && endpoint.query().is_none()
        && endpoint.fragment().is_none()
        && matches!(endpoint.path(), "" | "/")
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
    ArtifactBrokerUnavailable,
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
            Self::ArtifactBrokerUnavailable => "Artifact Broker is unavailable",
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

    fn valid_config() -> ControllerProcessConfig {
        serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "listen_address": "0.0.0.0:7443",
            "database_max_connections": 8,
            "artifact_broker": {
                "endpoint": "https://artifact-broker.platform-artifacts.svc:9443",
                "tls_server_name": "artifact-broker.platform-artifacts.svc",
                "maximum_request_bytes": 1048576,
                "maximum_chunk_bytes": 262144,
                "maximum_in_flight_responses": 1
            },
            "process_isolation_attestor": {
                "tls_server_name": "sandbox-attestor.insight.internal",
                "attestor_identity_digest": "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "maximum_cached_routes": 1024,
                "controller_port": 7444,
                "allowed_node_cidrs": ["10.0.0.0/8"]
            },
            "connect_timeout_milliseconds": 5000,
            "request_timeout_milliseconds": 30000,
            "shutdown_grace_milliseconds": 30000
        }))
        .unwrap()
    }

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

    #[test]
    fn controller_requires_bounded_exact_artifact_broker_transport() {
        let valid = valid_config();
        valid.validate().unwrap();

        let mut cleartext = valid.clone();
        cleartext.artifact_broker.endpoint =
            "http://artifact-broker.platform-artifacts.svc:9443".to_owned();
        assert!(cleartext.validate().is_err());

        let mut identity_drift = valid.clone();
        identity_drift.artifact_broker.tls_server_name =
            "other-broker.platform-artifacts.svc".to_owned();
        assert!(identity_drift.validate().is_err());

        let mut unbounded = valid;
        unbounded.artifact_broker.maximum_chunk_bytes = 0;
        assert!(unbounded.validate().is_err());

        let mut excessive_response_capacity = valid_config();
        excessive_response_capacity
            .artifact_broker
            .maximum_in_flight_responses = MAX_IN_FLIGHT_ARTIFACT_RESPONSES_HARD + 1;
        assert!(excessive_response_capacity.validate().is_err());
    }
}
