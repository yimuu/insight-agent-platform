//! Independent Context Dataset Builder process.
//!
//! This process owns only `context_dataset_build`, uses a dedicated PostgreSQL pool, and stages
//! bounded output through the Artifact Data Worker mTLS authority.

mod dependency_observer;
mod durable_queue_observer;

use async_trait::async_trait;
use dependency_observer::install_context_dependency_metrics;
use durable_queue_observer::run_context_queue_sampler;
use insight_platform_artifact_rpc::{
    ArtifactDataWorkerGrpcClient, ArtifactInternalRpcLimits, ArtifactWorkloadStageError,
};
use insight_platform_artifacts::{StageWorkloadArtifactRequest, StagedWorkloadArtifact};
use insight_platform_context_worker::dataset::{
    ContextDatasetArtifactStageError, ContextDatasetArtifactStager, ContextDatasetBuilderConfig,
    ContextDatasetBuilderDriver, ContextDatasetBuilderSource, ContextDatasetBuilderTiming,
};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JobKind, JsonLimits, ResourceId, ResourceKind,
    Sha256Digest,
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

const CONFIG_PATH_ENV: &str = "PLATFORM_CONTEXT_DATASET_WORKER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_CONTEXT_DATASET_WORKER_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_CONTEXT_DATASET_WORKER_DATABASE_URL";
const CLIENT_CERT_PATH_ENV: &str = "PLATFORM_CONTEXT_DATASET_WORKER_CLIENT_CERT_PATH";
const CLIENT_KEY_PATH_ENV: &str = "PLATFORM_CONTEXT_DATASET_WORKER_CLIENT_KEY_PATH";
const ARTIFACT_CA_PATH_ENV: &str = "PLATFORM_CONTEXT_DATASET_WORKER_ARTIFACT_CA_PATH";
const MAX_CONFIG_BYTES: usize = 4_194_304;
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
    lease_milliseconds: i64,
    scan_interval_milliseconds: u64,
    failure_backoff_milliseconds: u64,
    heartbeat_interval_milliseconds: u64,
    retry_backoff_milliseconds: u64,
    drain_grace_milliseconds: u64,
    sources: Vec<ContextDatasetBuilderSource>,
    artifact_data_worker: ArtifactDataWorkerConfig,
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
                max_depth: 16,
                max_properties_per_object: 48,
                max_items_per_array: 256,
                max_string_bytes: 262_144,
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
        let config = serde_json::from_value::<Self>(value)
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ProcessError> {
        let listen: SocketAddr = self
            .observability_listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || listen.port() == 0
            || !(2..=64).contains(&self.database_max_connections)
            || !(1..=30_000).contains(&self.database_acquire_timeout_milliseconds)
            || self.sources.is_empty()
            || self.sources.len() > 64
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        for source in &self.sources {
            source
                .validate()
                .map_err(|_| ProcessError::InvalidConfiguration)?;
        }
        validate_upstream(&self.artifact_data_worker)?;
        self.artifact_limits()?;
        self.driver_config().map(|_| ())
    }

    fn driver_config(&self) -> Result<ContextDatasetBuilderConfig, ProcessError> {
        let config = ContextDatasetBuilderConfig {
            claim_batch_size: self.claim_batch_size,
            recovery_batch_size: self.recovery_batch_size,
            maximum_concurrency: self.maximum_concurrency,
            lease_milliseconds: self.lease_milliseconds,
            timing: ContextDatasetBuilderTiming {
                scan_interval: Duration::from_millis(self.scan_interval_milliseconds),
                failure_backoff: Duration::from_millis(self.failure_backoff_milliseconds),
                heartbeat_interval: Duration::from_millis(self.heartbeat_interval_milliseconds),
                retry_backoff: Duration::from_millis(self.retry_backoff_milliseconds),
                drain_grace: Duration::from_millis(self.drain_grace_milliseconds),
            },
        };
        config
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        Ok(config)
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
impl ContextDatasetArtifactStager for GrpcArtifactStager {
    async fn stage_context_dataset_artifact(
        &self,
        request: StageWorkloadArtifactRequest,
    ) -> Result<StagedWorkloadArtifact, ContextDatasetArtifactStageError> {
        self.0
            .stage_workload_artifact(request)
            .await
            .map_err(|failure| match failure {
                ArtifactWorkloadStageError::Denied
                | ArtifactWorkloadStageError::StaleFence
                | ArtifactWorkloadStageError::Conflict => {
                    ContextDatasetArtifactStageError::InvalidOrStale
                }
                ArtifactWorkloadStageError::Unavailable => {
                    ContextDatasetArtifactStageError::Unavailable
                }
                ArtifactWorkloadStageError::Integrity => {
                    ContextDatasetArtifactStageError::CommitUncertain
                }
            })
    }
}

#[tokio::main]
async fn main() {
    if let Err(failure) = run().await {
        eprintln!("platform-context-dataset-worker failed: {failure}");
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
    let artifact = ArtifactDataWorkerGrpcClient::new(
        connect_artifact(&config.artifact_data_worker).await?,
        config.artifact_limits()?,
    );
    let process_generation_id =
        ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let driver = ContextDatasetBuilderDriver::new(
        Arc::new(PgRepository::new(pool)),
        Arc::new(GrpcArtifactStager(artifact)),
        process_generation_id,
        config.sources.clone(),
        config.driver_config()?,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let dependencies = install_context_dependency_metrics(false)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    debug_assert!(dependencies.egress.is_none());
    let queue_metrics = Arc::new(DurableJobQueueMetrics::default());
    let metrics = Arc::new(
        ProcessHttpMetrics::install("context-dataset-worker", PROCESS_OBSERVABILITY_OPERATIONS)
            .map_err(|_| ProcessError::InvalidConfiguration)?
            .with_dependency_observations(dependencies.process)
            .with_durable_job_queue(Arc::clone(&queue_metrics)),
    );
    let listener = tokio::net::TcpListener::bind(&config.observability_listen_address)
        .await
        .map_err(|_| ProcessError::ObservabilityFailed)?;
    let cancellation = CancellationToken::new();
    let mut driver_task = tokio::spawn({
        let token = cancellation.child_token();
        async move { driver.run(token).await }
    });
    let mut sampler_task = tokio::spawn({
        let token = cancellation.child_token();
        async move {
            tokio::join!(
                run_postgres_health_sampler(
                    health_pool,
                    dependencies.postgres,
                    token.child_token(),
                ),
                run_context_queue_sampler(
                    queue_pool,
                    queue_metrics,
                    JobKind::ContextDatasetBuild,
                    token,
                ),
            );
        }
    });
    let mut http_task = tokio::spawn({
        let token = cancellation.child_token();
        let router = process_observability_router(Arc::clone(&metrics));
        async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(token.cancelled_owned())
                .await
        }
    });
    metrics.mark_ready();
    eprintln!("platform-context-dataset-worker started");
    tokio::select! {
        signal = shutdown_signal() => { cancellation.cancel(); signal?; }
        result = &mut driver_task => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            return Err(ProcessError::WorkerFailed);
        }
        result = &mut sampler_task => {
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
        sampler_task
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

async fn connect_artifact(
    config: &ArtifactDataWorkerConfig,
) -> Result<tonic::transport::Channel, ProcessError> {
    let tls = ClientTlsConfig::new()
        .domain_name(config.tls_server_name.clone())
        .ca_certificate(Certificate::from_pem(read_bounded(
            &required_absolute_path(ARTIFACT_CA_PATH_ENV)?,
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
    Endpoint::from_shared(config.endpoint.clone())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(config.connect_timeout_milliseconds))
        .timeout(Duration::from_millis(config.request_timeout_milliseconds))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect()
        .await
        .map_err(|_| ProcessError::UpstreamUnavailable)
}

fn validate_upstream(config: &ArtifactDataWorkerConfig) -> Result<(), ProcessError> {
    let endpoint = Url::parse(&config.endpoint).map_err(|_| ProcessError::InvalidConfiguration)?;
    if endpoint.scheme() != "https"
        || endpoint.host_str().is_none()
        || endpoint.port().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.path() != "/"
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || !valid_tls_server_name(&config.tls_server_name)
        || !(1..=30_000).contains(&config.connect_timeout_milliseconds)
        || !(1..=120_000).contains(&config.request_timeout_milliseconds)
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
    tokio::signal::ctrl_c()
        .await
        .map_err(|_| ProcessError::SignalUnavailable)
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
            Self::UpstreamUnavailable => formatter.write_str("Artifact Data Worker is unavailable"),
            Self::SignalUnavailable => {
                formatter.write_str("shutdown signal handler is unavailable")
            }
            Self::WorkerFailed => formatter.write_str("Context Dataset Builder failed"),
            Self::DependencyObserverFailed => formatter.write_str("dependency observer failed"),
            Self::ObservabilityFailed => formatter.write_str("observability server failed"),
            Self::DrainTimeout => formatter.write_str("Context Dataset Builder drain expired"),
        }
    }
}

impl Error for ProcessError {}
