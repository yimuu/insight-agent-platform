//! Deployable durable MCP OAuth PKCE cleanup worker.
//!
//! The process has PostgreSQL authority only for the shared outbox claim/settlement and exact MCP
//! cleanup authorization. Secret deletion crosses the MCP Host mTLS Egress RPC; this process has
//! no Secret Manager credential or unrestricted external egress.

mod dependency_observer;

use dependency_observer::install_cleanup_dependency_metrics;

use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_egress_rpc::{EgressBrokerGrpcClient, EgressInternalRpcLimits};
use insight_platform_mcp_host::{
    McpOAuthPkceCleanupConsumer, McpOAuthPkceCleanupWorker, McpOAuthPkceCleanupWorkerConfig,
};
use insight_platform_observability::{
    process_observability_router, ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS,
};
use insight_platform_postgres::{
    dependency_health::run_postgres_health_sampler, repository::PgRepository, verify_schema,
};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{error::Error, fmt, io::Read as _, path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use url::Url;
use uuid::Uuid;

const CONFIG_PATH_ENV: &str = "PLATFORM_MCP_CLEANUP_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_MCP_CLEANUP_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_MCP_CLEANUP_DATABASE_URL";
const EGRESS_CA_PATH_ENV: &str = "PLATFORM_MCP_CLEANUP_EGRESS_CA_PATH";
const EGRESS_CERT_PATH_ENV: &str = "PLATFORM_MCP_CLEANUP_EGRESS_CERT_PATH";
const EGRESS_KEY_PATH_ENV: &str = "PLATFORM_MCP_CLEANUP_EGRESS_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_TLS_FILE_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    observability_listen_address: String,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    egress_endpoint: String,
    egress_tls_server_name: String,
    egress_connect_timeout_milliseconds: u64,
    egress_request_timeout_milliseconds: u64,
    maximum_rpc_metadata_bytes: usize,
    maximum_rpc_payload_bytes: usize,
    poll_interval_milliseconds: u64,
    maximum_batch: u16,
    maximum_lease_milliseconds: u64,
    claim_batch: u16,
    lease_milliseconds: u64,
    retry_base_milliseconds: u64,
    retry_maximum_milliseconds: u64,
}

impl ProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded_file(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 16,
                max_items_per_array: 16,
                max_properties_per_object: 32,
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
        let observability: std::net::SocketAddr = self
            .observability_listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let egress =
            Url::parse(&self.egress_endpoint).map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || observability.port() == 0
            || !(2..=32).contains(&self.database_max_connections)
            || self.database_acquire_timeout_milliseconds == 0
            || self.database_acquire_timeout_milliseconds > 30_000
            || egress.scheme() != "https"
            || egress.host_str().is_none()
            || egress.port().is_none()
            || egress.username() != ""
            || egress.password().is_some()
            || egress.path() != "/"
            || egress.query().is_some()
            || egress.fragment().is_some()
            || !valid_tls_server_name(&self.egress_tls_server_name)
            || self.egress_connect_timeout_milliseconds == 0
            || self.egress_connect_timeout_milliseconds > 30_000
            || self.egress_request_timeout_milliseconds == 0
            || self.egress_request_timeout_milliseconds > 120_000
            || self.poll_interval_milliseconds == 0
            || self.poll_interval_milliseconds > 60_000
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        EgressInternalRpcLimits::new(
            self.maximum_rpc_metadata_bytes,
            self.maximum_rpc_payload_bytes,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.worker_config()
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)
    }

    fn worker_config(&self) -> McpOAuthPkceCleanupWorkerConfig {
        McpOAuthPkceCleanupWorkerConfig {
            maximum_batch: self.maximum_batch,
            maximum_lease_milliseconds: self.maximum_lease_milliseconds,
            claim_batch: self.claim_batch,
            lease_milliseconds: self.lease_milliseconds,
            retry_base_milliseconds: self.retry_base_milliseconds,
            retry_maximum_milliseconds: self.retry_maximum_milliseconds,
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-mcp-cleanup-worker failed: {error}");
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
    let database_health_pool = pool.clone();
    let repository = Arc::new(PgRepository::new(pool));
    let (dependency_metrics, postgres_observer, egress_observer) =
        install_cleanup_dependency_metrics().map_err(|_| ProcessError::InvalidConfiguration)?;
    let rpc_limits = EgressInternalRpcLimits::new(
        config.maximum_rpc_metadata_bytes,
        config.maximum_rpc_payload_bytes,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let broker = Arc::new(EgressBrokerGrpcClient::new_with_observer(
        connect_egress(&config).await?,
        rpc_limits,
        egress_observer,
    ));
    let consumer = Arc::new(McpOAuthPkceCleanupConsumer::new(repository.clone(), broker));
    let generation_id =
        ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let worker =
        McpOAuthPkceCleanupWorker::new(generation_id, repository, consumer, config.worker_config())
            .map_err(|_| ProcessError::InvalidConfiguration)?;

    let metrics = Arc::new(
        ProcessHttpMetrics::install("mcp-cleanup-worker", PROCESS_OBSERVABILITY_OPERATIONS)
            .map_err(|_| ProcessError::InvalidConfiguration)?
            .with_dependency_observations(dependency_metrics),
    );
    let listener = tokio::net::TcpListener::bind(&config.observability_listen_address)
        .await
        .map_err(|_| ProcessError::ObservabilityFailed)?;
    let (http_shutdown, http_shutdown_receiver) = tokio::sync::oneshot::channel();
    let router = process_observability_router(Arc::clone(&metrics));
    let mut http = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = http_shutdown_receiver.await;
            })
            .await
    });
    metrics.mark_ready();

    let cancellation = CancellationToken::new();
    let mut postgres_health = tokio::spawn(run_postgres_health_sampler(
        database_health_pool,
        postgres_observer,
        cancellation.child_token(),
    ));

    let mut interval =
        tokio::time::interval(Duration::from_millis(config.poll_interval_milliseconds));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut shutdown = Box::pin(shutdown_signal());
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                cancellation.cancel();
                let _ = http_shutdown.send(());
                let http_result = http.await.map_err(|_| ProcessError::ObservabilityFailed)
                    .and_then(|result| result.map_err(|_| ProcessError::ObservabilityFailed));
                let postgres_result = postgres_health.await
                    .map_err(|_| ProcessError::DependencyObserverFailed);
                http_result?;
                postgres_result?;
                return Ok(());
            },
            result = &mut http => {
                cancellation.cancel();
                let http_result = result.map_err(|_| ProcessError::ObservabilityFailed)
                    .and_then(|result| result.map_err(|_| ProcessError::ObservabilityFailed));
                let postgres_result = postgres_health.await
                    .map_err(|_| ProcessError::DependencyObserverFailed);
                http_result?;
                postgres_result?;
                return Err(ProcessError::ObservabilityFailed);
            },
            result = &mut postgres_health => {
                cancellation.cancel();
                let _ = http_shutdown.send(());
                let postgres_result = result.map_err(|_| ProcessError::DependencyObserverFailed);
                let http_result = http.await.map_err(|_| ProcessError::ObservabilityFailed)
                    .and_then(|result| result.map_err(|_| ProcessError::ObservabilityFailed));
                postgres_result?;
                http_result?;
                return Err(ProcessError::DependencyObserverFailed);
            },
            _ = interval.tick() => {
                if worker.run_once().await.is_err() {
                    eprintln!("MCP OAuth cleanup delivery iteration was unavailable");
                }
            }
        }
    }
}

async fn connect_egress(config: &ProcessConfig) -> Result<tonic::transport::Channel, ProcessError> {
    let tls = ClientTlsConfig::new()
        .domain_name(config.egress_tls_server_name.clone())
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
    Endpoint::from_shared(config.egress_endpoint.clone())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(
            config.egress_connect_timeout_milliseconds,
        ))
        .timeout(Duration::from_millis(
            config.egress_request_timeout_milliseconds,
        ))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect()
        .await
        .map_err(|_| ProcessError::EgressUnavailable)
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

fn read_bounded_file(path: &PathBuf, maximum: usize) -> Result<Vec<u8>, ProcessError> {
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
    DatabaseUnavailable,
    SchemaMismatch,
    EgressUnavailable,
    DependencyObserverFailed,
    ObservabilityFailed,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingConfiguration(name) => {
                write!(formatter, "required configuration {name} is missing")
            }
            Self::InvalidConfiguration => formatter.write_str("configuration is invalid"),
            Self::FileUnavailable => formatter.write_str("configuration file is unavailable"),
            Self::DatabaseUnavailable => formatter.write_str("database is unavailable"),
            Self::SchemaMismatch => formatter.write_str("database schema does not match candidate"),
            Self::EgressUnavailable => formatter.write_str("Egress Broker is unavailable"),
            Self::DependencyObserverFailed => {
                formatter.write_str("MCP Cleanup dependency observer failed")
            }
            Self::ObservabilityFailed => formatter.write_str("observability server failed"),
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_config_rejects_unbounded_cleanup_and_non_https_egress() {
        let mut config = ProcessConfig {
            schema_version: 1,
            observability_listen_address: "127.0.0.1:9090".to_owned(),
            database_max_connections: 8,
            database_acquire_timeout_milliseconds: 5_000,
            egress_endpoint: "https://egress.platform-egress.svc:8443/".to_owned(),
            egress_tls_server_name: "egress.platform-egress.svc".to_owned(),
            egress_connect_timeout_milliseconds: 5_000,
            egress_request_timeout_milliseconds: 30_000,
            maximum_rpc_metadata_bytes: 262_144,
            maximum_rpc_payload_bytes: 1_048_576,
            poll_interval_milliseconds: 1_000,
            maximum_batch: 64,
            maximum_lease_milliseconds: 120_000,
            claim_batch: 16,
            lease_milliseconds: 30_000,
            retry_base_milliseconds: 1_000,
            retry_maximum_milliseconds: 60_000,
        };
        config.validate().unwrap();
        config.claim_batch = 65;
        assert!(config.validate().is_err());
        config.claim_batch = 16;
        config.egress_endpoint = "http://egress:8080/".to_owned();
        assert!(config.validate().is_err());
    }
}
