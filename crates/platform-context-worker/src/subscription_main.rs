//! Dedicated Context Worker process for durable MCP subscription refresh Jobs.

use insight_platform_context::ContextSubscriptionRefreshBackend;
use insight_platform_context_worker::{
    ContextWorkerConfig, ContextWorkerTiming, SubscriptionContextWorkerDriver, CONTEXT_WORKER_ROLE,
};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, JsonLimits, ResourceId,
    ResourceKind, Sha256Digest, WorkClass, WorkerManifest,
};
use insight_platform_mcp_rpc::{McpHostInternalRpcLimits, McpResourceRefreshGrpcClient};
use insight_platform_observability::{
    process_observability_router, run_worker_permit_sampler, update_worker_permits,
    ProcessHttpMetrics, WorkerPermitMetrics, PROCESS_OBSERVABILITY_OPERATIONS,
};
use insight_platform_postgres::{repository::PgRepository, verify_schema};
use insight_platform_worker::LocalWorkerPools;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{error::Error, fmt, fs::File, io::Read as _, path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use url::Url;
use uuid::Uuid;

const CONFIG_PATH_ENV: &str = "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_DATABASE_URL";
const HOST_CA_PATH_ENV: &str = "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_HOST_CA_PATH";
const HOST_CERT_PATH_ENV: &str = "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_HOST_CERT_PATH";
const HOST_KEY_PATH_ENV: &str = "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_HOST_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_TLS_FILE_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    observability_listen_address: String,
    worker_manifest: WorkerManifest,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    receipt_ttl_seconds: u64,
    scan_interval_milliseconds: u64,
    failure_backoff_milliseconds: u64,
    drain_grace_milliseconds: u64,
    host: HostConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct HostConfig {
    endpoint: String,
    tls_server_name: String,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
    maximum_rpc_message_bytes: usize,
}

impl ProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let bytes = read_bounded(&required_absolute_path(CONFIG_PATH_ENV)?, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 16,
                max_items_per_array: 64,
                max_properties_per_object: 32,
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
        let endpoint =
            Url::parse(&self.host.endpoint).map_err(|_| ProcessError::InvalidConfiguration)?;
        self.worker_manifest
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || observability.port() == 0
            || self.worker_manifest.worker_role != CONTEXT_WORKER_ROLE
            || self.worker_manifest.work_class != WorkClass::Context
            || !(2..=64).contains(&self.database_max_connections)
            || self.database_acquire_timeout_milliseconds == 0
            || self.database_acquire_timeout_milliseconds > 30_000
            || self.receipt_ttl_seconds == 0
            || self.scan_interval_milliseconds == 0
            || self.scan_interval_milliseconds > 60_000
            || self.failure_backoff_milliseconds == 0
            || self.failure_backoff_milliseconds > self.scan_interval_milliseconds
            || self.drain_grace_milliseconds == 0
            || self.drain_grace_milliseconds > 120_000
            || endpoint.scheme() != "https"
            || endpoint.host_str().is_none()
            || endpoint.port().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !valid_tls_server_name(&self.host.tls_server_name)
            || self.host.connect_timeout_milliseconds == 0
            || self.host.connect_timeout_milliseconds > 30_000
            || self.host.request_timeout_milliseconds == 0
            || self.host.request_timeout_milliseconds > 120_000
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        self.driver_config()?;
        self.host_rpc_limits()?;
        Ok(())
    }

    fn driver_config(&self) -> Result<ContextWorkerConfig, ProcessError> {
        ContextWorkerConfig::from_profile(
            &checked_in_hard_limit_profile(),
            ContextWorkerTiming {
                scan_interval: Duration::from_millis(self.scan_interval_milliseconds),
                failure_backoff: Duration::from_millis(self.failure_backoff_milliseconds),
                drain_grace: Duration::from_millis(self.drain_grace_milliseconds),
                receipt_ttl: Duration::from_secs(self.receipt_ttl_seconds),
            },
        )
        .map_err(|_| ProcessError::InvalidConfiguration)
    }

    fn host_rpc_limits(&self) -> Result<McpHostInternalRpcLimits, ProcessError> {
        McpHostInternalRpcLimits::new(self.host.maximum_rpc_message_bytes)
            .map_err(|_| ProcessError::InvalidConfiguration)
    }
}

#[tokio::main]
async fn main() {
    if let Err(failure) = run().await {
        eprintln!("platform-subscription-context-worker failed: {failure}");
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
    let repository = Arc::new(PgRepository::new(pool));
    let process_generation_id =
        ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let pools = LocalWorkerPools::new(config.worker_manifest.clone(), process_generation_id)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let observability_pools = pools.clone();
    let backend: Arc<dyn ContextSubscriptionRefreshBackend> = Arc::new(
        McpResourceRefreshGrpcClient::new(connect_host(&config).await?, config.host_rpc_limits()?),
    );
    let driver =
        SubscriptionContextWorkerDriver::new(repository, pools, backend, config.driver_config()?)
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let permit_metrics = Arc::new(WorkerPermitMetrics::default());
    update_worker_permits(&permit_metrics, &observability_pools);
    let metrics = Arc::new(
        ProcessHttpMetrics::install_with_worker_permits(
            "context-subscription-worker",
            PROCESS_OBSERVABILITY_OPERATIONS,
            Arc::clone(&permit_metrics),
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let listener = tokio::net::TcpListener::bind(&config.observability_listen_address)
        .await
        .map_err(|_| ProcessError::ObservabilityFailed)?;
    let cancellation = CancellationToken::new();
    let _permit_sampler = tokio::spawn(run_worker_permit_sampler(
        permit_metrics,
        observability_pools,
        cancellation.child_token(),
    ));
    let worker_cancellation = cancellation.child_token();
    let mut worker = tokio::spawn(async move { driver.run(worker_cancellation).await });
    let http_cancellation = cancellation.child_token();
    let router = process_observability_router(Arc::clone(&metrics));
    let mut http = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(http_cancellation.cancelled_owned())
            .await
    });
    metrics.mark_ready();
    eprintln!("platform-subscription-context-worker started");
    tokio::select! {
        signal = wait_for_shutdown_signal() => {
            signal?;
            cancellation.cancel();
            worker.await.map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            http.await.map_err(|_| ProcessError::ObservabilityFailed)?
                .map_err(|_| ProcessError::ObservabilityFailed)?;
            Ok(())
        }
        result = &mut worker => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            Err(ProcessError::WorkerFailed)
        }
        result = &mut http => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::ObservabilityFailed)?
                .map_err(|_| ProcessError::ObservabilityFailed)?;
            Err(ProcessError::ObservabilityFailed)
        }
    }
}

async fn connect_host(config: &ProcessConfig) -> Result<tonic::transport::Channel, ProcessError> {
    let tls = ClientTlsConfig::new()
        .domain_name(config.host.tls_server_name.clone())
        .ca_certificate(Certificate::from_pem(read_bounded(
            &required_absolute_path(HOST_CA_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?))
        .identity(Identity::from_pem(
            read_bounded(
                &required_absolute_path(HOST_CERT_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
            read_bounded(
                &required_absolute_path(HOST_KEY_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
        ));
    Endpoint::from_shared(config.host.endpoint.clone())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(
            config.host.connect_timeout_milliseconds,
        ))
        .timeout(Duration::from_millis(
            config.host.request_timeout_milliseconds,
        ))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect()
        .await
        .map_err(|_| ProcessError::HostUnavailable)
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> Result<(), ProcessError> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut terminate =
        signal(SignalKind::terminate()).map_err(|_| ProcessError::SignalUnavailable)?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.map_err(|_| ProcessError::SignalUnavailable),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> Result<(), ProcessError> {
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
        .filter(|value| !value.is_empty() && value.len() <= MAX_CONFIG_BYTES)
        .ok_or(ProcessError::MissingEnvironment(name))
}

fn required_absolute_path(name: &'static str) -> Result<PathBuf, ProcessError> {
    let path = PathBuf::from(required(name)?);
    path.is_absolute()
        .then_some(path)
        .ok_or(ProcessError::InvalidConfiguration)
}

fn read_bounded(path: &PathBuf, maximum: usize) -> Result<Vec<u8>, ProcessError> {
    let mut file = File::open(path).map_err(|_| ProcessError::FileUnavailable)?;
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
    HostUnavailable,
    SignalUnavailable,
    WorkerFailed,
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
            Self::HostUnavailable => formatter.write_str("MCP Resource Host is unavailable"),
            Self::SignalUnavailable => {
                formatter.write_str("shutdown signal handler is unavailable")
            }
            Self::WorkerFailed => formatter.write_str("subscription Context Worker failed"),
            Self::ObservabilityFailed => formatter.write_str("observability server failed"),
        }
    }
}

impl Error for ProcessError {}
