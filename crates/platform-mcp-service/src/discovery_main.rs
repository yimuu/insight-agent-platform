//! Independent durable MCP discovery worker.
//!
//! This process owns only the MCP discovery Job lane. It has a restricted PostgreSQL pool and
//! closed mTLS clients for Egress discovery and Artifact Data Worker staging; it exposes no MCP
//! Tool/Resource RPC listener.

mod dependency_observer;
mod discovery_capacity_observer;
mod discovery_queue_observer;

use async_trait::async_trait;
use dependency_observer::install_mcp_dependency_metrics;
use discovery_queue_observer::run_discovery_queue_sampler;
use insight_platform_artifact_rpc::{
    ArtifactDataWorkerGrpcClient, ArtifactInternalRpcLimits, ArtifactWorkloadStageError,
};
use insight_platform_artifacts::{StageWorkloadArtifactRequest, StagedWorkloadArtifact};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_egress_rpc::{EgressBrokerGrpcClient, EgressInternalRpcLimits};
use insight_platform_mcp_host::{
    McpDiscoveryArtifactStageError, McpDiscoveryArtifactStager, McpDiscoveryService,
    McpDiscoveryWorker, StreamableHttpMcpDiscoveryTransport,
};
use insight_platform_mcp_service::{
    McpDiscoveryDriver, McpDiscoveryDriverConfig, McpDiscoveryDriverTiming,
};
use insight_platform_observability::{
    process_observability_router, DurableJobQueueMetrics, ProcessHttpMetrics,
    PROCESS_OBSERVABILITY_OPERATIONS,
};
use insight_platform_postgres::{
    dependency_health::run_postgres_health_sampler, repository::PgRepository, verify_schema,
};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{
    error::Error, fmt, io::Read as _, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use url::Url;
use uuid::Uuid;

const CONFIG_PATH_ENV: &str = "PLATFORM_MCP_DISCOVERY_WORKER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_MCP_DISCOVERY_WORKER_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_MCP_DISCOVERY_WORKER_DATABASE_URL";
const CLIENT_CERT_PATH_ENV: &str = "PLATFORM_MCP_DISCOVERY_WORKER_CLIENT_CERT_PATH";
const CLIENT_KEY_PATH_ENV: &str = "PLATFORM_MCP_DISCOVERY_WORKER_CLIENT_KEY_PATH";
const EGRESS_CA_PATH_ENV: &str = "PLATFORM_MCP_DISCOVERY_WORKER_EGRESS_CA_PATH";
const ARTIFACT_CA_PATH_ENV: &str = "PLATFORM_MCP_DISCOVERY_WORKER_ARTIFACT_CA_PATH";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_TLS_FILE_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    observability_listen_address: String,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    claim_batch_size: u16,
    recovery_batch_size: u16,
    maximum_concurrency: usize,
    lease_milliseconds: u64,
    scan_interval_milliseconds: u64,
    failure_backoff_milliseconds: u64,
    heartbeat_interval_milliseconds: u64,
    retry_backoff_milliseconds: u64,
    receipt_ttl_milliseconds: u64,
    drain_grace_milliseconds: u64,
    egress: EgressConfig,
    artifact_data_worker: ArtifactDataWorkerConfig,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactDataWorkerConfig {
    endpoint: String,
    tls_server_name: String,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
    maximum_read_request_bytes: usize,
    maximum_chunk_bytes: usize,
    maximum_write_request_bytes: usize,
}

impl ProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let bytes = read_bounded(&required_absolute_path(CONFIG_PATH_ENV)?, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 12,
                max_properties_per_object: 48,
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
        let observability: SocketAddr = self
            .observability_listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || observability.port() == 0
            || !(2..=32).contains(&self.database_max_connections)
            || self.database_acquire_timeout_milliseconds == 0
            || self.database_acquire_timeout_milliseconds > 30_000
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        validate_upstream(
            &self.egress.endpoint,
            &self.egress.tls_server_name,
            self.egress.connect_timeout_milliseconds,
            self.egress.request_timeout_milliseconds,
        )?;
        validate_upstream(
            &self.artifact_data_worker.endpoint,
            &self.artifact_data_worker.tls_server_name,
            self.artifact_data_worker.connect_timeout_milliseconds,
            self.artifact_data_worker.request_timeout_milliseconds,
        )?;
        self.egress_limits()?;
        self.artifact_limits()?;
        self.driver_config().map(|_| ())
    }

    fn driver_config(&self) -> Result<McpDiscoveryDriverConfig, ProcessError> {
        let config = McpDiscoveryDriverConfig {
            claim_batch_size: self.claim_batch_size,
            recovery_batch_size: self.recovery_batch_size,
            maximum_concurrency: self.maximum_concurrency,
            lease_milliseconds: self.lease_milliseconds,
            timing: McpDiscoveryDriverTiming {
                scan_interval: Duration::from_millis(self.scan_interval_milliseconds),
                failure_backoff: Duration::from_millis(self.failure_backoff_milliseconds),
                heartbeat_interval: Duration::from_millis(self.heartbeat_interval_milliseconds),
                retry_backoff: Duration::from_millis(self.retry_backoff_milliseconds),
                receipt_ttl: Duration::from_millis(self.receipt_ttl_milliseconds),
                drain_grace: Duration::from_millis(self.drain_grace_milliseconds),
            },
        };
        config
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        Ok(config)
    }

    fn egress_limits(&self) -> Result<EgressInternalRpcLimits, ProcessError> {
        EgressInternalRpcLimits::new(
            self.egress.maximum_rpc_metadata_bytes,
            self.egress.maximum_rpc_payload_bytes,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)
    }

    fn artifact_limits(&self) -> Result<ArtifactInternalRpcLimits, ProcessError> {
        ArtifactInternalRpcLimits::with_write_limit(
            self.artifact_data_worker.maximum_read_request_bytes,
            self.artifact_data_worker.maximum_chunk_bytes,
            self.artifact_data_worker.maximum_write_request_bytes,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)
    }
}

#[derive(Clone)]
struct GrpcArtifactStager(ArtifactDataWorkerGrpcClient);

#[async_trait]
impl McpDiscoveryArtifactStager for GrpcArtifactStager {
    async fn stage_mcp_discovery_artifact(
        &self,
        request: StageWorkloadArtifactRequest,
    ) -> Result<StagedWorkloadArtifact, McpDiscoveryArtifactStageError> {
        self.0
            .stage_workload_artifact(request)
            .await
            .map_err(|failure| match failure {
                ArtifactWorkloadStageError::Denied
                | ArtifactWorkloadStageError::StaleFence
                | ArtifactWorkloadStageError::Conflict => {
                    McpDiscoveryArtifactStageError::InvalidOrStale
                }
                ArtifactWorkloadStageError::Unavailable => {
                    McpDiscoveryArtifactStageError::Unavailable
                }
                ArtifactWorkloadStageError::Integrity => {
                    McpDiscoveryArtifactStageError::CommitUncertain
                }
            })
    }
}

#[tokio::main]
async fn main() {
    if let Err(failure) = run().await {
        eprintln!("platform-mcp-discovery-worker failed: {failure}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = ProcessConfig::load()?;
    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(Duration::from_millis(
            config.database_acquire_timeout_milliseconds,
        ))
        .connect(&required(DATABASE_URL_ENV)?)
        .await
        .map_err(|_| ProcessError::DatabaseUnavailable)?;
    verify_schema(&pool)
        .await
        .map_err(|_| ProcessError::SchemaMismatch)?;
    let health_pool = pool.clone();
    let queue_pool = pool.clone();
    let repository = Arc::new(PgRepository::new(pool));
    let (dependency_metrics, postgres_observer, egress_observer) =
        install_mcp_dependency_metrics(true).map_err(|_| ProcessError::InvalidConfiguration)?;
    let egress = Arc::new(EgressBrokerGrpcClient::new_with_observer(
        connect_upstream(
            &config.egress.endpoint,
            &config.egress.tls_server_name,
            config.egress.connect_timeout_milliseconds,
            config.egress.request_timeout_milliseconds,
            EGRESS_CA_PATH_ENV,
        )
        .await?,
        config.egress_limits()?,
        egress_observer,
    ));
    let artifact = ArtifactDataWorkerGrpcClient::new(
        connect_upstream(
            &config.artifact_data_worker.endpoint,
            &config.artifact_data_worker.tls_server_name,
            config.artifact_data_worker.connect_timeout_milliseconds,
            config.artifact_data_worker.request_timeout_milliseconds,
            ARTIFACT_CA_PATH_ENV,
        )
        .await?,
        config.artifact_limits()?,
    );
    let transport = Arc::new(StreamableHttpMcpDiscoveryTransport::new(egress));
    let worker = Arc::new(McpDiscoveryWorker::new(
        repository.clone(),
        Arc::new(McpDiscoveryService::new(transport)),
        Arc::new(GrpcArtifactStager(artifact)),
        repository.clone(),
    ));
    let process_generation_id =
        ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let driver = McpDiscoveryDriver::new(
        repository,
        worker,
        process_generation_id,
        config.driver_config()?,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let discovery_capacity = driver.capacity();
    let queue_metrics = Arc::new(DurableJobQueueMetrics::default());
    let metrics = Arc::new(
        ProcessHttpMetrics::install_with_capacities(
            "mcp-discovery-worker",
            PROCESS_OBSERVABILITY_OPERATIONS,
            vec![discovery_capacity_observer::discovery_capacity_metric(
                discovery_capacity,
            )],
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .with_dependency_observations(dependency_metrics)
        .with_durable_job_queue(Arc::clone(&queue_metrics)),
    );
    let listener = tokio::net::TcpListener::bind(&config.observability_listen_address)
        .await
        .map_err(|_| ProcessError::ObservabilityFailed)?;
    let cancellation = CancellationToken::new();
    let driver_cancellation = cancellation.child_token();
    let mut driver_task = tokio::spawn(async move { driver.run(driver_cancellation).await });
    let health_cancellation = cancellation.child_token();
    let mut health_task = tokio::spawn(run_postgres_health_sampler(
        health_pool,
        postgres_observer.ok_or(ProcessError::InvalidConfiguration)?,
        health_cancellation,
    ));
    let queue_cancellation = cancellation.child_token();
    let mut queue_task = tokio::spawn(run_discovery_queue_sampler(
        queue_pool,
        queue_metrics,
        queue_cancellation,
    ));
    let http_cancellation = cancellation.child_token();
    let router = process_observability_router(Arc::clone(&metrics));
    let mut http_task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(http_cancellation.cancelled_owned())
            .await
    });
    metrics.mark_ready();
    eprintln!("platform-mcp-discovery-worker started");
    tokio::select! {
        signal = shutdown_signal() => {
            cancellation.cancel();
            signal?;
        }
        result = &mut driver_task => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            return Err(ProcessError::WorkerFailed);
        }
        result = &mut health_task => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::DependencyObserverFailed)?;
            return Err(ProcessError::DependencyObserverFailed);
        }
        result = &mut queue_task => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::DependencyObserverFailed)?;
            return Err(ProcessError::DependencyObserverFailed);
        }
        result = &mut http_task => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::ObservabilityFailed)?
                .map_err(|_| ProcessError::ObservabilityFailed)?;
            return Err(ProcessError::ObservabilityFailed);
        }
    }
    let drain = async {
        driver_task
            .await
            .map_err(|_| ProcessError::WorkerFailed)?
            .map_err(|_| ProcessError::WorkerFailed)?;
        health_task
            .await
            .map_err(|_| ProcessError::DependencyObserverFailed)?;
        queue_task
            .await
            .map_err(|_| ProcessError::DependencyObserverFailed)?;
        http_task
            .await
            .map_err(|_| ProcessError::ObservabilityFailed)?
            .map_err(|_| ProcessError::ObservabilityFailed)
    };
    tokio::time::timeout(
        Duration::from_millis(config.drain_grace_milliseconds),
        drain,
    )
    .await
    .map_err(|_| ProcessError::DrainTimeout)??;
    Ok(())
}

async fn connect_upstream(
    endpoint: &str,
    tls_server_name: &str,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
    ca_path_env: &'static str,
) -> Result<tonic::transport::Channel, ProcessError> {
    let tls = ClientTlsConfig::new()
        .domain_name(tls_server_name.to_owned())
        .ca_certificate(Certificate::from_pem(read_bounded(
            &required_absolute_path(ca_path_env)?,
            MAX_TLS_FILE_BYTES,
        )?))
        .identity(Identity::from_pem(
            read_bounded(
                &required_absolute_path(CLIENT_CERT_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
            read_bounded(
                &required_absolute_path(CLIENT_KEY_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
        ));
    Endpoint::from_shared(endpoint.to_owned())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(connect_timeout_milliseconds))
        .timeout(Duration::from_millis(request_timeout_milliseconds))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect()
        .await
        .map_err(|_| ProcessError::UpstreamUnavailable)
}

fn validate_upstream(
    endpoint: &str,
    tls_server_name: &str,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
) -> Result<(), ProcessError> {
    let endpoint = Url::parse(endpoint).map_err(|_| ProcessError::InvalidConfiguration)?;
    if endpoint.scheme() != "https"
        || endpoint.host_str().is_none()
        || endpoint.port().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.path() != "/"
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || !valid_tls_server_name(tls_server_name)
        || connect_timeout_milliseconds == 0
        || connect_timeout_milliseconds > 30_000
        || request_timeout_milliseconds == 0
        || request_timeout_milliseconds > 120_000
    {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(())
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

fn read_bounded(path: &PathBuf, maximum: usize) -> Result<Vec<u8>, ProcessError> {
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
    UpstreamUnavailable,
    SignalUnavailable,
    WorkerFailed,
    DependencyObserverFailed,
    ObservabilityFailed,
    DrainTimeout,
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
            Self::UpstreamUnavailable => {
                formatter.write_str("required mTLS upstream is unavailable")
            }
            Self::SignalUnavailable => {
                formatter.write_str("shutdown signal handler is unavailable")
            }
            Self::WorkerFailed => formatter.write_str("MCP discovery worker failed"),
            Self::DependencyObserverFailed => formatter.write_str("dependency observer failed"),
            Self::ObservabilityFailed => formatter.write_str("observability server failed"),
            Self::DrainTimeout => formatter.write_str("MCP discovery drain grace expired"),
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_config_closes_database_driver_upstreams_and_rpc_limits() {
        let config = ProcessConfig {
            schema_version: 1,
            observability_listen_address: "0.0.0.0:9090".to_owned(),
            database_max_connections: 4,
            database_acquire_timeout_milliseconds: 5_000,
            claim_batch_size: 4,
            recovery_batch_size: 4,
            maximum_concurrency: 4,
            lease_milliseconds: 30_000,
            scan_interval_milliseconds: 500,
            failure_backoff_milliseconds: 500,
            heartbeat_interval_milliseconds: 5_000,
            retry_backoff_milliseconds: 1_000,
            receipt_ttl_milliseconds: 60_000,
            drain_grace_milliseconds: 30_000,
            egress: EgressConfig {
                endpoint: "https://platform-egress-broker.platform-egress.svc:8443/".to_owned(),
                tls_server_name: "platform-egress-broker.platform-egress.svc".to_owned(),
                connect_timeout_milliseconds: 1_000,
                request_timeout_milliseconds: 30_000,
                maximum_rpc_metadata_bytes: 1_048_576,
                maximum_rpc_payload_bytes: 16_777_216,
            },
            artifact_data_worker: ArtifactDataWorkerConfig {
                endpoint: "https://platform-artifact-data-worker.platform-artifact.svc:8443/"
                    .to_owned(),
                tls_server_name: "platform-artifact-data-worker.platform-artifact.svc".to_owned(),
                connect_timeout_milliseconds: 1_000,
                request_timeout_milliseconds: 60_000,
                maximum_read_request_bytes: 1_048_576,
                maximum_chunk_bytes: 262_144,
                maximum_write_request_bytes: 67_108_864,
            },
        };
        config.validate().unwrap();
        let mut plaintext = config.clone();
        plaintext.egress.endpoint = "http://egress:8443/".to_owned();
        assert!(plaintext.validate().is_err());
        let mut unsafe_heartbeat = config;
        unsafe_heartbeat.heartbeat_interval_milliseconds = 10_000;
        assert!(unsafe_heartbeat.validate().is_err());
    }
}
