//! Independently scalable MCP Resource Refresh Host for Context subscription Jobs.

use insight_platform_contracts::{canonical_digest, parse_strict_json, JsonLimits, Sha256Digest};
use insight_platform_egress_rpc::{EgressBrokerGrpcClient, EgressInternalRpcLimits};
use insight_platform_mcp_host::{
    McpResourceRefreshConnector, McpResourceRefreshHost, StreamableHttpMcpResourceRefreshProtocol,
};
use insight_platform_mcp_rpc::{
    proto::mcp_resource_refresh_service_server::McpResourceRefreshServiceServer,
    ContextWorkerWorkloadIdentity, McpHostInternalRpcLimits, McpResourceRefreshGrpcService,
};
use insight_platform_observability::{
    process_observability_router, ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS,
};
use insight_platform_postgres::{repository::PgRepository, verify_schema};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{
    error::Error, fmt, io::Read as _, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig};
use url::Url;

const CONFIG_PATH_ENV: &str = "PLATFORM_MCP_RESOURCE_HOST_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_MCP_RESOURCE_HOST_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_MCP_RESOURCE_HOST_DATABASE_URL";
const SERVER_CA_PATH_ENV: &str = "PLATFORM_MCP_RESOURCE_HOST_SERVER_CLIENT_CA_PATH";
const SERVER_CERT_PATH_ENV: &str = "PLATFORM_MCP_RESOURCE_HOST_SERVER_CERT_PATH";
const SERVER_KEY_PATH_ENV: &str = "PLATFORM_MCP_RESOURCE_HOST_SERVER_KEY_PATH";
const EGRESS_CA_PATH_ENV: &str = "PLATFORM_MCP_RESOURCE_HOST_EGRESS_CA_PATH";
const EGRESS_CERT_PATH_ENV: &str = "PLATFORM_MCP_RESOURCE_HOST_EGRESS_CERT_PATH";
const EGRESS_KEY_PATH_ENV: &str = "PLATFORM_MCP_RESOURCE_HOST_EGRESS_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_TLS_FILE_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    listen_address: String,
    observability_listen_address: String,
    maximum_rpc_message_bytes: usize,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    egress: EgressConfig,
    drain_grace_milliseconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct EgressConfig {
    endpoint: String,
    tls_server_name: String,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
    maximum_rpc_metadata_bytes: usize,
    maximum_rpc_payload_bytes: usize,
}

impl ProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let bytes = read_bounded_file(&required_absolute_path(CONFIG_PATH_ENV)?, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 12,
                max_properties_per_object: 32,
                max_items_per_array: 16,
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
        let endpoint =
            Url::parse(&self.egress.endpoint).map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || listen.port() == 0
            || observability.port() == 0
            || listen == observability
            || !(2..=64).contains(&self.database_max_connections)
            || self.database_acquire_timeout_milliseconds == 0
            || self.database_acquire_timeout_milliseconds > 30_000
            || endpoint.scheme() != "https"
            || endpoint.host_str().is_none()
            || endpoint.port().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !valid_tls_server_name(&self.egress.tls_server_name)
            || self.egress.connect_timeout_milliseconds == 0
            || self.egress.connect_timeout_milliseconds > 30_000
            || self.egress.request_timeout_milliseconds == 0
            || self.egress.request_timeout_milliseconds > 120_000
            || self.drain_grace_milliseconds == 0
            || self.drain_grace_milliseconds > 120_000
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        self.host_rpc_limits()?;
        self.egress_rpc_limits()?;
        Ok(())
    }

    fn host_rpc_limits(&self) -> Result<McpHostInternalRpcLimits, ProcessError> {
        McpHostInternalRpcLimits::new(self.maximum_rpc_message_bytes)
            .map_err(|_| ProcessError::InvalidConfiguration)
    }

    fn egress_rpc_limits(&self) -> Result<EgressInternalRpcLimits, ProcessError> {
        EgressInternalRpcLimits::new(
            self.egress.maximum_rpc_metadata_bytes,
            self.egress.maximum_rpc_payload_bytes,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)
    }
}

#[tokio::main]
async fn main() {
    if let Err(failure) = run().await {
        eprintln!("platform-mcp-resource-host failed: {failure}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = ProcessConfig::load()?;
    let database_url = required(DATABASE_URL_ENV)?;
    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(Duration::from_millis(
            config.database_acquire_timeout_milliseconds,
        ))
        .connect(&database_url)
        .await
        .map_err(|_| ProcessError::DatabaseUnavailable)?;
    verify_schema(&pool)
        .await
        .map_err(|_| ProcessError::SchemaMismatch)?;
    let repository = Arc::new(PgRepository::new(pool));
    let egress = Arc::new(EgressBrokerGrpcClient::new(
        connect_egress(&config).await?,
        config.egress_rpc_limits()?,
    ));
    let connector: Arc<dyn McpResourceRefreshConnector> = egress;
    let protocol = Arc::new(StreamableHttpMcpResourceRefreshProtocol::new(connector));
    let host = Arc::new(McpResourceRefreshHost::new(repository, protocol));
    let limits = config.host_rpc_limits()?;
    let maximum = limits.maximum_message_bytes();
    let service =
        McpResourceRefreshServiceServer::new(McpResourceRefreshGrpcService::new(host, limits))
            .max_encoding_message_size(maximum)
            .max_decoding_message_size(maximum);
    let service = tonic::service::interceptor::InterceptedService::new(
        service,
        ContextWorkerWorkloadIdentity,
    );
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(
            read_bounded_file(
                &required_absolute_path(SERVER_CERT_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
            read_bounded_file(
                &required_absolute_path(SERVER_KEY_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
        ))
        .client_ca_root(Certificate::from_pem(read_bounded_file(
            &required_absolute_path(SERVER_CA_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?));
    let address = config
        .listen_address
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let cancellation = CancellationToken::new();
    let server_cancellation = cancellation.child_token();
    let server = Server::builder()
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .add_service(service)
        .serve_with_shutdown(address, server_cancellation.cancelled_owned());
    let mut server_task = tokio::spawn(server);
    let metrics = Arc::new(
        ProcessHttpMetrics::install("mcp-resource-host", PROCESS_OBSERVABILITY_OPERATIONS)
            .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let listener = tokio::net::TcpListener::bind(&config.observability_listen_address)
        .await
        .map_err(|_| ProcessError::ObservabilityFailed)?;
    let observability_cancellation = cancellation.child_token();
    let router = process_observability_router(Arc::clone(&metrics));
    let mut observability_task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(observability_cancellation.cancelled_owned())
            .await
    });
    metrics.mark_ready();
    eprintln!("platform-mcp-resource-host started");
    tokio::select! {
        signal = shutdown_signal() => {
            signal?;
            cancellation.cancel();
            drain(&config, &mut server_task, &mut observability_task).await
        }
        result = &mut server_task => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::ServerFailed)?
                .map_err(|_| ProcessError::ServerFailed)?;
            Err(ProcessError::ServerExitedUnexpectedly)
        }
        result = &mut observability_task => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::ObservabilityFailed)?
                .map_err(|_| ProcessError::ObservabilityFailed)?;
            Err(ProcessError::ObservabilityFailed)
        }
    }
}

async fn drain(
    config: &ProcessConfig,
    server: &mut tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    observability: &mut tokio::task::JoinHandle<Result<(), std::io::Error>>,
) -> Result<(), ProcessError> {
    let timeout = Duration::from_millis(config.drain_grace_milliseconds);
    tokio::time::timeout(timeout, server)
        .await
        .map_err(|_| ProcessError::DrainTimeout)?
        .map_err(|_| ProcessError::ServerFailed)?
        .map_err(|_| ProcessError::ServerFailed)?;
    tokio::time::timeout(timeout, observability)
        .await
        .map_err(|_| ProcessError::DrainTimeout)?
        .map_err(|_| ProcessError::ObservabilityFailed)?
        .map_err(|_| ProcessError::ObservabilityFailed)?;
    Ok(())
}

async fn connect_egress(config: &ProcessConfig) -> Result<tonic::transport::Channel, ProcessError> {
    let tls = ClientTlsConfig::new()
        .domain_name(config.egress.tls_server_name.clone())
        .ca_certificate(Certificate::from_pem(read_bounded_file(
            &required_absolute_path(EGRESS_CA_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?))
        .identity(Identity::from_pem(
            read_bounded_file(
                &required_absolute_path(EGRESS_CERT_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
            read_bounded_file(
                &required_absolute_path(EGRESS_KEY_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
        ));
    Endpoint::from_shared(config.egress.endpoint.clone())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(
            config.egress.connect_timeout_milliseconds,
        ))
        .timeout(Duration::from_millis(
            config.egress.request_timeout_milliseconds,
        ))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect()
        .await
        .map_err(|_| ProcessError::EgressUnavailable)
}

async fn shutdown_signal() -> Result<(), ProcessError> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|_| ProcessError::SignalUnavailable)?;
        tokio::select! {
            received = terminate.recv() => received.ok_or(ProcessError::SignalUnavailable),
            received = tokio::signal::ctrl_c() => received.map_err(|_| ProcessError::SignalUnavailable),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(|_| ProcessError::SignalUnavailable)
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
        .ok_or(ProcessError::MissingEnvironment(name))
}

fn required_absolute_path(name: &'static str) -> Result<PathBuf, ProcessError> {
    let path = PathBuf::from(required(name)?);
    path.is_absolute()
        .then_some(path)
        .ok_or(ProcessError::InvalidConfiguration)
}

fn read_bounded_file(path: &PathBuf, maximum: usize) -> Result<Vec<u8>, ProcessError> {
    let mut file = std::fs::File::open(path).map_err(|_| ProcessError::FileUnavailable)?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(u64::try_from(maximum).unwrap_or(u64::MAX).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| ProcessError::FileUnavailable)?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(ProcessError::FileUnavailable);
    }
    Ok(bytes)
}

#[derive(Debug)]
enum ProcessError {
    MissingEnvironment(&'static str),
    InvalidConfiguration,
    FileUnavailable,
    DatabaseUnavailable,
    SchemaMismatch,
    EgressUnavailable,
    SignalUnavailable,
    ServerFailed,
    DrainTimeout,
    ServerExitedUnexpectedly,
    ObservabilityFailed,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvironment(name) => {
                write!(formatter, "required environment {name} is missing")
            }
            Self::InvalidConfiguration => formatter.write_str("process configuration is invalid"),
            Self::FileUnavailable => formatter.write_str("required file is unavailable"),
            Self::DatabaseUnavailable => formatter.write_str("PostgreSQL is unavailable"),
            Self::SchemaMismatch => formatter.write_str("PostgreSQL schema is incompatible"),
            Self::EgressUnavailable => formatter.write_str("Egress Broker is unavailable"),
            Self::SignalUnavailable => {
                formatter.write_str("shutdown signal handler is unavailable")
            }
            Self::ServerFailed => formatter.write_str("MCP Resource Host RPC server failed"),
            Self::DrainTimeout => formatter.write_str("MCP Resource Host drain grace expired"),
            Self::ServerExitedUnexpectedly => {
                formatter.write_str("MCP Resource Host exited unexpectedly")
            }
            Self::ObservabilityFailed => formatter.write_str("observability server failed"),
        }
    }
}

impl Error for ProcessError {}
