//! Independent remote Streamable HTTP MCP Host process.
//!
//! Capability Workers call the closed mTLS MCP Host RPC. This process owns protocol validation and
//! Task/Elicitation semantics, while the separately deployed Egress Broker owns all network and
//! late Secret resolution.

use insight_platform_contracts::{canonical_digest, parse_strict_json, JsonLimits, Sha256Digest};
use insight_platform_egress_rpc::{EgressBrokerGrpcClient, EgressInternalRpcLimits};
use insight_platform_mcp_host::{
    McpHostService, McpStreamableHttpConnector, StreamableHttpMcpTransport,
};
use insight_platform_mcp_rpc::{
    proto::mcp_host_execution_service_server::McpHostExecutionServiceServer,
    CapabilityWorkerWorkloadIdentity, McpHostGrpcService, McpHostInternalRpcLimits,
};
use serde::Deserialize;
use std::{
    error::Error, fmt, io::Read as _, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig};
use url::Url;

const CONFIG_PATH_ENV: &str = "PLATFORM_MCP_HOST_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_MCP_HOST_CONFIG_DIGEST";
const SERVER_CA_PATH_ENV: &str = "PLATFORM_MCP_HOST_SERVER_CLIENT_CA_PATH";
const SERVER_CERT_PATH_ENV: &str = "PLATFORM_MCP_HOST_SERVER_CERT_PATH";
const SERVER_KEY_PATH_ENV: &str = "PLATFORM_MCP_HOST_SERVER_KEY_PATH";
const EGRESS_CA_PATH_ENV: &str = "PLATFORM_MCP_HOST_EGRESS_CA_PATH";
const EGRESS_CERT_PATH_ENV: &str = "PLATFORM_MCP_HOST_EGRESS_CERT_PATH";
const EGRESS_KEY_PATH_ENV: &str = "PLATFORM_MCP_HOST_EGRESS_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_TLS_FILE_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    listen_address: String,
    tls_server_name: String,
    maximum_rpc_message_bytes: usize,
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
        let endpoint =
            Url::parse(&self.egress.endpoint).map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || listen.port() == 0
            || !valid_tls_server_name(&self.tls_server_name)
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
        eprintln!("platform-mcp-host failed: {failure}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = ProcessConfig::load()?;
    let address: SocketAddr = config
        .listen_address
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let egress = Arc::new(EgressBrokerGrpcClient::new(
        connect_egress(&config).await?,
        config.egress_rpc_limits()?,
    ));
    let connector: Arc<dyn McpStreamableHttpConnector> = egress;
    let host = Arc::new(McpHostService::new(Arc::new(
        StreamableHttpMcpTransport::new(connector),
    )));
    let maximum = config.host_rpc_limits()?.maximum_message_bytes();
    let service = McpHostExecutionServiceServer::new(McpHostGrpcService::new(
        host,
        config.host_rpc_limits()?,
    ))
    .max_encoding_message_size(maximum)
    .max_decoding_message_size(maximum);
    let service = tonic::service::interceptor::InterceptedService::new(
        service,
        CapabilityWorkerWorkloadIdentity,
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
    let cancellation = CancellationToken::new();
    let server_cancellation = cancellation.child_token();
    let server = Server::builder()
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .add_service(service)
        .serve_with_shutdown(address, server_cancellation.cancelled_owned());
    let mut server_task = tokio::spawn(server);
    eprintln!(
        "platform-mcp-host started address={} server_name={}",
        address, config.tls_server_name
    );
    tokio::select! {
        signal = shutdown_signal() => {
            signal?;
            cancellation.cancel();
            tokio::time::timeout(
                Duration::from_millis(config.drain_grace_milliseconds),
                &mut server_task,
            )
            .await
            .map_err(|_| ProcessError::DrainTimeout)?
            .map_err(|_| ProcessError::ServerFailed)?
            .map_err(|_| ProcessError::ServerFailed)?;
            Ok(())
        }
        result = &mut server_task => {
            cancellation.cancel();
            result
                .map_err(|_| ProcessError::ServerFailed)?
                .map_err(|_| ProcessError::ServerFailed)?;
            Err(ProcessError::ServerExitedUnexpectedly)
        }
    }
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
    if !path.is_absolute() {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(path)
}

fn read_bounded_file(path: &PathBuf, maximum: usize) -> Result<Vec<u8>, ProcessError> {
    let mut file = std::fs::File::open(path).map_err(|_| ProcessError::FileUnavailable)?;
    let metadata = file.metadata().map_err(|_| ProcessError::FileUnavailable)?;
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > u64::try_from(maximum).unwrap_or(u64::MAX)
    {
        return Err(ProcessError::FileUnavailable);
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
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
    EgressUnavailable,
    SignalUnavailable,
    ServerFailed,
    DrainTimeout,
    ServerExitedUnexpectedly,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvironment(name) => {
                write!(formatter, "required environment {name} is missing")
            }
            Self::InvalidConfiguration => formatter.write_str("process configuration is invalid"),
            Self::FileUnavailable => formatter.write_str("required file is unavailable"),
            Self::EgressUnavailable => formatter.write_str("Egress Broker is unavailable"),
            Self::SignalUnavailable => {
                formatter.write_str("shutdown signal handler is unavailable")
            }
            Self::ServerFailed => formatter.write_str("MCP Host RPC server failed"),
            Self::DrainTimeout => formatter.write_str("MCP Host drain grace expired"),
            Self::ServerExitedUnexpectedly => {
                formatter.write_str("MCP Host server exited unexpectedly")
            }
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ProcessConfig {
        ProcessConfig {
            schema_version: 1,
            listen_address: "0.0.0.0:9443".to_owned(),
            tls_server_name: "platform-mcp-host.platform-mcp-host.svc".to_owned(),
            maximum_rpc_message_bytes: 1_048_576,
            egress: EgressConfig {
                endpoint: "https://platform-egress-broker.platform-egress.svc:8443/".to_owned(),
                tls_server_name: "platform-egress-broker.platform-egress.svc".to_owned(),
                connect_timeout_milliseconds: 1_000,
                request_timeout_milliseconds: 30_000,
                maximum_rpc_metadata_bytes: 65_536,
                maximum_rpc_payload_bytes: 1_048_576,
            },
            drain_grace_milliseconds: 30_000,
        }
    }

    #[test]
    fn config_closes_listener_rpc_egress_and_drain_limits() {
        config().validate().unwrap();
        let mut plaintext = config();
        plaintext.egress.endpoint = "http://egress:8443/".to_owned();
        assert!(plaintext.validate().is_err());
        let mut ephemeral = config();
        ephemeral.listen_address = "0.0.0.0:0".to_owned();
        assert!(ephemeral.validate().is_err());
        let mut unbounded = config();
        unbounded.maximum_rpc_message_bytes = usize::MAX;
        assert!(unbounded.validate().is_err());
    }
}
