//! Independently deployed outbound Egress Broker.
//!
//! This process owns controlled DNS, outbound HTTPS, KMS and Secret Provider access. It has no
//! PostgreSQL dependency or credential and reaches durable SecretBinding authority only through
//! the two-method Security Authority mTLS RPC.

use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_egress::{
    AeadMcpRemoteTaskStateCodec, AeadMcpSubscriptionStateCodec, BrokeredMcpOAuthPkceSecretCleaner,
    CapabilityGrpcEgressLimits, CapabilityHttpEgressLimits, HyperCapabilityGrpcEgressTransport,
    InstalledCapabilityGrpcEndpoint, InstalledCapabilityGrpcEndpointCatalog,
    InstalledCapabilityHttpEndpoint, InstalledCapabilityHttpEndpointCatalog,
    InstalledMcpOAuthJwtVerifier, InstalledMcpOAuthVerificationBinding,
    InstalledMcpOAuthVerificationCatalog, InstalledMcpStreamableHttpEndpoint,
    InstalledMcpStreamableHttpEndpointCatalog, InstalledModelProviderEndpoint,
    InstalledModelProviderEndpointCatalog, InstalledRemoteContextEndpoint,
    InstalledRemoteContextEndpointCatalog, McpOAuthEgressLimits, McpRemoteTaskStateKey,
    McpStreamableHttpEgressLimits, McpSubscriptionStateKey, ModelProviderEgressLimits,
    RemoteContextEgressLimits, ReqwestCapabilityHttpEgressTransport,
    ReqwestMcpOAuthCredentialBroker, ReqwestMcpStreamableHttpConnector,
    ReqwestMcpStreamableHttpSubscriptionConnector, ReqwestModelProviderEgressBroker,
    ReqwestRemoteContextSearchConnector, SecretMaterialResolver, SensitiveMcpRemoteTaskStateKey,
    SensitiveMcpSubscriptionStateKey, TokioEgressDnsResolver,
};
use insight_platform_egress_rpc::{
    proto::egress_broker_service_server::EgressBrokerServiceServer, EgressBrokerGrpcService,
    EgressCallerWorkloadIdentity, EgressInternalRpcLimits, EgressMcpSubscriptionBridge,
    EgressMcpSubscriptionBridgeLimits,
};
use insight_platform_model_adapters::{
    BrokeredModelProviderWireConnector, ModelProviderEgressBroker,
};
use insight_platform_observability::{
    process_observability_router, ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS,
};
use insight_platform_secret_broker::{
    AwsSecretProviderCatalog, AwsSecretProviderCatalogConfig, BrokeredMcpOAuthSecretStore,
    BrokeredSecretMaterialResolver, SecretBrokerLimits,
};
use insight_platform_security_rpc::{SecurityInternalRpcLimits, SecuritySecretAuthorityGrpcClient};
use serde::Deserialize;
use std::{
    error::Error,
    fmt,
    io::Read as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig};
use url::Url;

const CONFIG_PATH_ENV: &str = "PLATFORM_EGRESS_BROKER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_EGRESS_BROKER_CONFIG_DIGEST";
const AUTHORITY_CA_PATH_ENV: &str = "PLATFORM_EGRESS_BROKER_AUTHORITY_CA_PATH";
const AUTHORITY_CERT_PATH_ENV: &str = "PLATFORM_EGRESS_BROKER_AUTHORITY_CERT_PATH";
const AUTHORITY_KEY_PATH_ENV: &str = "PLATFORM_EGRESS_BROKER_AUTHORITY_KEY_PATH";
const CLIENT_CA_PATH_ENV: &str = "PLATFORM_EGRESS_BROKER_CLIENT_CA_PATH";
const SERVER_CERT_PATH_ENV: &str = "PLATFORM_EGRESS_BROKER_CERT_PATH";
const SERVER_KEY_PATH_ENV: &str = "PLATFORM_EGRESS_BROKER_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 16 * 1_048_576;
const MAX_TLS_FILE_BYTES: usize = 1_048_576;
const MCP_STATE_KEY_BYTES: usize = 32;
const MCP_STATE_KEY_DIRECTORY: &str = "/etc/insight/mcp-state-keys";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    listen_address: String,
    observability_listen_address: String,
    security_authority_endpoint: String,
    security_authority_tls_server_name: String,
    maximum_rpc_metadata_bytes: usize,
    maximum_rpc_payload_bytes: usize,
    security_authority_maximum_rpc_message_bytes: usize,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
    tls_handshake_timeout_milliseconds: u64,
    shutdown_grace_milliseconds: u64,
    secret_broker: SecretBrokerProcessLimits,
    model_limits: ModelProviderEgressLimits,
    capability_http_limits: CapabilityHttpEgressLimits,
    capability_grpc_limits: CapabilityGrpcEgressLimits,
    remote_context_limits: RemoteContextEgressLimits,
    mcp_oauth_limits: McpOAuthEgressLimits,
    mcp_oauth_service_principal_id: ResourceId,
    mcp_oauth_verification_bindings: Vec<InstalledMcpOAuthVerificationBinding>,
    mcp_streamable_http_limits: McpStreamableHttpEgressLimits,
    mcp_streamable_http_endpoints: Vec<InstalledMcpStreamableHttpEndpoint>,
    mcp_state_keys: McpStateKeyProcessConfig,
    mcp_subscription_bridge: EgressMcpSubscriptionBridgeLimits,
    secret_provider_catalog: AwsSecretProviderCatalogConfig,
    model_endpoints: Vec<InstalledModelProviderEndpoint>,
    capability_http_endpoints: Vec<InstalledCapabilityHttpEndpoint>,
    capability_grpc_endpoints: Vec<InstalledCapabilityGrpcEndpoint>,
    remote_context_endpoints: Vec<InstalledRemoteContextEndpoint>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretBrokerProcessLimits {
    maximum_in_flight: usize,
    maximum_material_bytes: usize,
    resolution_timeout_milliseconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpStateKeyProcessConfig {
    active_key_id: String,
    keys: Vec<McpStateKeyFile>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpStateKeyFile {
    key_id: String,
    key_reference_digest: Sha256Digest,
    key_material_path: String,
}

impl McpStateKeyProcessConfig {
    fn load_remote_tasks(&self) -> Result<AeadMcpRemoteTaskStateCodec, ProcessError> {
        let keys = self
            .keys
            .iter()
            .map(|key| {
                let path = PathBuf::from(&key.key_material_path);
                if !valid_projected_secret_path(&path) {
                    return Err(ProcessError::InvalidConfiguration);
                }
                let material = read_projected_secret(&path, MCP_STATE_KEY_BYTES)?;
                Ok(McpRemoteTaskStateKey {
                    key_id: key.key_id.clone(),
                    key_reference_digest: key.key_reference_digest.clone(),
                    key_material: SensitiveMcpRemoteTaskStateKey::new(material)
                        .map_err(|_| ProcessError::InvalidConfiguration)?,
                })
            })
            .collect::<Result<Vec<_>, ProcessError>>()?;
        AeadMcpRemoteTaskStateCodec::new(self.active_key_id.clone(), keys)
            .map_err(|_| ProcessError::InvalidConfiguration)
    }

    fn load_subscriptions(&self) -> Result<AeadMcpSubscriptionStateCodec, ProcessError> {
        let keys = self
            .keys
            .iter()
            .map(|key| {
                let path = PathBuf::from(&key.key_material_path);
                if !valid_projected_secret_path(&path) {
                    return Err(ProcessError::InvalidConfiguration);
                }
                let material = read_projected_secret(&path, MCP_STATE_KEY_BYTES)?;
                Ok(McpSubscriptionStateKey {
                    key_id: key.key_id.clone(),
                    key_reference_digest: key.key_reference_digest.clone(),
                    key_material: SensitiveMcpSubscriptionStateKey::new(material)
                        .map_err(|_| ProcessError::InvalidConfiguration)?,
                })
            })
            .collect::<Result<Vec<_>, ProcessError>>()?;
        AeadMcpSubscriptionStateCodec::new(self.active_key_id.clone(), keys)
            .map_err(|_| ProcessError::InvalidConfiguration)
    }
}

impl SecretBrokerProcessLimits {
    fn build(self) -> Result<SecretBrokerLimits, ProcessError> {
        let limits = SecretBrokerLimits {
            maximum_in_flight: self.maximum_in_flight,
            maximum_material_bytes: self.maximum_material_bytes,
            resolution_timeout: Duration::from_millis(self.resolution_timeout_milliseconds),
        };
        limits
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        Ok(limits)
    }
}

impl ProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 64,
                max_items_per_array: 4_096,
                max_properties_per_object: 128,
                max_string_bytes: 16_384,
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
        let listen: SocketAddr = self
            .listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let observability: SocketAddr = self
            .observability_listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let authority = Url::parse(&self.security_authority_endpoint)
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || listen.port() == 0
            || observability.port() == 0
            || observability == listen
            || authority.scheme() != "https"
            || authority.host_str().is_none()
            || authority.port().is_none()
            || authority.username() != ""
            || authority.password().is_some()
            || authority.path() != "/"
            || authority.query().is_some()
            || authority.fragment().is_some()
            || !valid_tls_server_name(&self.security_authority_tls_server_name)
            || self.connect_timeout_milliseconds == 0
            || self.connect_timeout_milliseconds > 30_000
            || self.request_timeout_milliseconds == 0
            || self.request_timeout_milliseconds > 3_600_000
            || self.tls_handshake_timeout_milliseconds == 0
            || self.tls_handshake_timeout_milliseconds > 30_000
            || self.shutdown_grace_milliseconds == 0
            || self.shutdown_grace_milliseconds > 60_000
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        EgressInternalRpcLimits::new(
            self.maximum_rpc_metadata_bytes,
            self.maximum_rpc_payload_bytes,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        SecurityInternalRpcLimits::new(self.security_authority_maximum_rpc_message_bytes)
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.secret_broker.build()?;
        self.model_limits
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.capability_http_limits
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.capability_grpc_limits
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.remote_context_limits
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.mcp_oauth_limits
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.mcp_oauth_service_principal_id.kind() != ResourceKind::Principal {
            return Err(ProcessError::InvalidConfiguration);
        }
        InstalledMcpOAuthVerificationCatalog::new(self.mcp_oauth_verification_bindings.clone())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.mcp_streamable_http_limits
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        InstalledMcpStreamableHttpEndpointCatalog::new(self.mcp_streamable_http_endpoints.clone())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.mcp_state_keys.load_remote_tasks()?;
        self.mcp_state_keys.load_subscriptions()?;
        self.mcp_subscription_bridge
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.secret_provider_catalog
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        InstalledModelProviderEndpointCatalog::new(self.model_endpoints.clone())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        InstalledCapabilityHttpEndpointCatalog::new(self.capability_http_endpoints.clone())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        InstalledCapabilityGrpcEndpointCatalog::new(self.capability_grpc_endpoints.clone())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        InstalledRemoteContextEndpointCatalog::new(self.remote_context_endpoints.clone())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-egress-broker failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = ProcessConfig::load()?;
    let rpc_limits = EgressInternalRpcLimits::new(
        config.maximum_rpc_metadata_bytes,
        config.maximum_rpc_payload_bytes,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let security_limits =
        SecurityInternalRpcLimits::new(config.security_authority_maximum_rpc_message_bytes)
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let secret_limits = config.secret_broker.build()?;
    let remote_task_state = Arc::new(config.mcp_state_keys.load_remote_tasks()?);
    let subscription_state = Arc::new(config.mcp_state_keys.load_subscriptions()?);

    let authority_channel = connect_security_authority(&config).await?;
    let security_authority = Arc::new(SecuritySecretAuthorityGrpcClient::new(
        authority_channel,
        security_limits,
    ));
    let provider_catalog = AwsSecretProviderCatalog::install(config.secret_provider_catalog)
        .await
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    provider_catalog
        .check_readiness()
        .await
        .map_err(|_| ProcessError::SecretProviderUnavailable)?;
    let (sealer, unsealer, providers) = provider_catalog.into_components();
    let secret_resolver = Arc::new(
        BrokeredSecretMaterialResolver::new(
            security_authority.clone(),
            unsealer,
            providers.clone(),
            secret_limits,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let secrets: Arc<dyn SecretMaterialResolver> = secret_resolver.clone();
    let dns = Arc::new(TokioEgressDnsResolver);

    let verification_catalog =
        InstalledMcpOAuthVerificationCatalog::new(config.mcp_oauth_verification_bindings)
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let token_store = Arc::new(
        BrokeredMcpOAuthSecretStore::new(
            security_authority,
            sealer,
            providers,
            config.mcp_oauth_service_principal_id,
            secret_limits,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let oauth_verifier = Arc::new(InstalledMcpOAuthJwtVerifier::new(
        verification_catalog.clone(),
    ));
    let oauth = Arc::new(
        ReqwestMcpOAuthCredentialBroker::new(
            Arc::clone(&secrets),
            dns.clone(),
            verification_catalog,
            oauth_verifier,
            token_store,
            config.mcp_oauth_limits,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let oauth_pkce_cleaner = Arc::new(BrokeredMcpOAuthPkceSecretCleaner::new(
        secret_resolver.clone(),
    ));
    let mcp_endpoints = InstalledMcpStreamableHttpEndpointCatalog::new(
        config.mcp_streamable_http_endpoints.clone(),
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let mcp_streamable_http = Arc::new(
        ReqwestMcpStreamableHttpConnector::new(
            mcp_endpoints.clone(),
            Arc::clone(&secrets),
            dns.clone(),
            remote_task_state,
            config.mcp_streamable_http_limits,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let mcp_subscription_bridge = Arc::new(
        EgressMcpSubscriptionBridge::new(rpc_limits, config.mcp_subscription_bridge)
            .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let mcp_subscription_sink = mcp_subscription_bridge.sink();
    let mcp_streamable_http_subscription = Arc::new(
        ReqwestMcpStreamableHttpSubscriptionConnector::new(
            mcp_endpoints,
            Arc::clone(&secrets),
            dns.clone(),
            Arc::clone(&subscription_state),
            mcp_subscription_sink,
            config.mcp_streamable_http_limits,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );

    let model_broker: Arc<dyn ModelProviderEgressBroker> = Arc::new(
        ReqwestModelProviderEgressBroker::new(
            InstalledModelProviderEndpointCatalog::new(config.model_endpoints)
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            Arc::clone(&secrets),
            dns.clone(),
            config.model_limits,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let model = Arc::new(BrokeredModelProviderWireConnector::new(model_broker));
    let http = Arc::new(
        ReqwestCapabilityHttpEgressTransport::new(
            InstalledCapabilityHttpEndpointCatalog::new(config.capability_http_endpoints)
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            Arc::clone(&secrets),
            dns.clone(),
            config.capability_http_limits,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let remote_context = Arc::new(
        ReqwestRemoteContextSearchConnector::new(
            InstalledRemoteContextEndpointCatalog::new(config.remote_context_endpoints)
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            Arc::clone(&secrets),
            dns.clone(),
            config.remote_context_limits,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let grpc = Arc::new(
        HyperCapabilityGrpcEgressTransport::new(
            InstalledCapabilityGrpcEndpointCatalog::new(config.capability_grpc_endpoints)
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            secrets,
            dns,
            config.capability_grpc_limits,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );

    let maximum = rpc_limits.maximum_message_bytes();
    let service = EgressBrokerServiceServer::new(
        EgressBrokerGrpcService::new(model, http, grpc, rpc_limits)
            .with_remote_context(remote_context)
            .with_mcp_oauth(oauth, oauth_pkce_cleaner)
            .with_mcp_streamable_http(mcp_streamable_http)
            .with_mcp_streamable_http_subscription(
                mcp_streamable_http_subscription,
                mcp_subscription_bridge,
            ),
    )
    .max_encoding_message_size(maximum)
    .max_decoding_message_size(maximum);
    let service =
        tonic::service::interceptor::InterceptedService::new(service, EgressCallerWorkloadIdentity);
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(
            read_bounded(
                &required_absolute_path(SERVER_CERT_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
            read_bounded(
                &required_absolute_path(SERVER_KEY_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
        ))
        .client_ca_root(Certificate::from_pem(read_bounded(
            &required_absolute_path(CLIENT_CA_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?))
        .timeout(Duration::from_millis(
            config.tls_handshake_timeout_milliseconds,
        ));
    let listen: SocketAddr = config
        .listen_address
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let cancellation = CancellationToken::new();
    let server_cancellation = cancellation.child_token();
    let server = Server::builder()
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .add_service(service)
        .serve_with_shutdown(listen, server_cancellation.cancelled_owned());
    let mut server = tokio::spawn(server);
    let metrics = Arc::new(
        ProcessHttpMetrics::install("egress-secret-broker", PROCESS_OBSERVABILITY_OPERATIONS)
            .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let listener = tokio::net::TcpListener::bind(&config.observability_listen_address)
        .await
        .map_err(|_| ProcessError::ObservabilityFailure)?;
    let observability_cancellation = cancellation.child_token();
    let router = process_observability_router(Arc::clone(&metrics));
    let mut observability = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(observability_cancellation.cancelled_owned())
            .await
    });
    metrics.mark_ready();
    tokio::select! {
        _ = shutdown_signal() => cancellation.cancel(),
        result = &mut server => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::ServerFailure)?
                .map_err(|_| ProcessError::ServerFailure)?;
            observability.await.map_err(|_| ProcessError::ObservabilityFailure)?
                .map_err(|_| ProcessError::ObservabilityFailure)?;
            return Err(ProcessError::ServerFailure);
        }
        result = &mut observability => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::ObservabilityFailure)?
                .map_err(|_| ProcessError::ObservabilityFailure)?;
            server.await.map_err(|_| ProcessError::ServerFailure)?
                .map_err(|_| ProcessError::ServerFailure)?;
            return Err(ProcessError::ObservabilityFailure);
        }
    }
    tokio::time::timeout(
        Duration::from_millis(config.shutdown_grace_milliseconds),
        async {
            server
                .await
                .map_err(|_| ProcessError::ServerFailure)?
                .map_err(|_| ProcessError::ServerFailure)?;
            observability
                .await
                .map_err(|_| ProcessError::ObservabilityFailure)?
                .map_err(|_| ProcessError::ObservabilityFailure)
        },
    )
    .await
    .map_err(|_| ProcessError::ShutdownTimeout)?
}

async fn connect_security_authority(
    config: &ProcessConfig,
) -> Result<tonic::transport::Channel, ProcessError> {
    let tls = ClientTlsConfig::new()
        .domain_name(config.security_authority_tls_server_name.clone())
        .ca_certificate(Certificate::from_pem(read_bounded(
            &required_absolute_path(AUTHORITY_CA_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?))
        .identity(Identity::from_pem(
            read_bounded(
                &required_absolute_path(AUTHORITY_CERT_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
            read_bounded(
                &required_absolute_path(AUTHORITY_KEY_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
        ));
    Endpoint::from_shared(config.security_authority_endpoint.clone())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(config.connect_timeout_milliseconds))
        .timeout(Duration::from_millis(config.request_timeout_milliseconds))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect()
        .await
        .map_err(|_| ProcessError::SecurityAuthorityUnavailable)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            _ = terminate.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn valid_tls_server_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && !value.starts_with('.')
        && !value.ends_with('.')
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn required(name: &'static str) -> Result<String, ProcessError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty() && value.len() <= 16_384)
        .ok_or(ProcessError::MissingConfiguration(name))
}

fn required_absolute_path(name: &'static str) -> Result<PathBuf, ProcessError> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(path)
}

fn valid_projected_secret_path(path: &Path) -> bool {
    path.is_absolute()
        && path.parent() == Some(Path::new(MCP_STATE_KEY_DIRECTORY))
        && path.file_name().is_some()
}

fn read_projected_secret(path: &Path, exact_size: usize) -> Result<Vec<u8>, ProcessError> {
    let file = std::fs::File::open(path).map_err(|_| ProcessError::FileUnavailable)?;
    let metadata = file.metadata().map_err(|_| ProcessError::FileUnavailable)?;
    if !metadata.is_file() || metadata.len() != u64::try_from(exact_size).unwrap_or(u64::MAX) {
        return Err(ProcessError::InvalidConfiguration);
    }
    let maximum = exact_size
        .checked_add(1)
        .ok_or(ProcessError::InvalidConfiguration)?;
    let mut bytes = Vec::with_capacity(maximum);
    let read_result = file
        .take(u64::try_from(maximum).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes);
    if read_result.is_err() || bytes.len() != exact_size {
        bytes.fill(0);
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}

fn read_bounded(path: &PathBuf, maximum: usize) -> Result<Vec<u8>, ProcessError> {
    let file = std::fs::File::open(path).map_err(|_| ProcessError::FileUnavailable)?;
    let metadata = file.metadata().map_err(|_| ProcessError::FileUnavailable)?;
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > u64::try_from(maximum).unwrap_or(u64::MAX)
    {
        return Err(ProcessError::InvalidConfiguration);
    }
    let mut bytes = Vec::with_capacity(maximum.min(64 * 1_024));
    let read_result = file
        .take(u64::try_from(maximum.saturating_add(1)).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes);
    if read_result.is_err() || bytes.is_empty() || bytes.len() > maximum {
        bytes.fill(0);
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}

#[derive(Debug)]
enum ProcessError {
    MissingConfiguration(&'static str),
    InvalidConfiguration,
    FileUnavailable,
    SecurityAuthorityUnavailable,
    SecretProviderUnavailable,
    ServerFailure,
    ShutdownTimeout,
    ObservabilityFailure,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingConfiguration(name) => {
                write!(formatter, "required configuration {name} is missing")
            }
            Self::InvalidConfiguration => formatter.write_str("configuration is invalid"),
            Self::FileUnavailable => formatter.write_str("configuration file is unavailable"),
            Self::SecurityAuthorityUnavailable => {
                formatter.write_str("Security Authority is unavailable")
            }
            Self::SecretProviderUnavailable => {
                formatter.write_str("Secret Provider readiness failed")
            }
            Self::ServerFailure => formatter.write_str("internal gRPC server failed"),
            Self::ShutdownTimeout => formatter.write_str("graceful shutdown timed out"),
            Self::ObservabilityFailure => formatter.write_str("observability server failed"),
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_server_name_is_closed() {
        assert!(valid_tls_server_name(
            "security-authority.platform-security.svc"
        ));
        assert!(!valid_tls_server_name("https://security-authority"));
        assert!(!valid_tls_server_name("authority/path"));
        assert!(!valid_tls_server_name("authority."));
    }

    #[test]
    fn remote_task_key_path_is_confined_to_projected_secret_directory() {
        assert!(valid_projected_secret_path(Path::new(
            "/etc/insight/mcp-state-keys/current"
        )));
        assert!(!valid_projected_secret_path(Path::new(
            "/etc/insight/mcp-state-keys/../other/current"
        )));
        assert!(!valid_projected_secret_path(Path::new(
            "/etc/insight/other/current"
        )));
        assert!(!valid_projected_secret_path(Path::new("current")));
    }
}
