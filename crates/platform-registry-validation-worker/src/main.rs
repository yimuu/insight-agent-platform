//! Process entrypoint for the isolated Registry Validation Worker.

use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ResourceId, ResourceKind, Sha256Digest,
    WorkClass, WorkerManifest,
};
use insight_platform_observability::{
    process_observability_router, run_worker_permit_sampler, update_worker_permits,
    ProcessHttpMetrics, WorkerPermitMetrics, PROCESS_OBSERVABILITY_OPERATIONS,
};
use insight_platform_postgres::{repository::PgRepository, verify_schema};
use insight_platform_registry_validation_worker::{
    RegistryValidationWorkerConfig, RegistryValidationWorkerDriver, REGISTRY_VALIDATION_WORKER_ROLE,
};
use insight_platform_worker::LocalWorkerPools;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{error::Error, fmt, fs::File, io::Read as _, path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const CONFIG_PATH_ENV: &str = "PLATFORM_REGISTRY_VALIDATION_WORKER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_REGISTRY_VALIDATION_WORKER_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_REGISTRY_VALIDATION_WORKER_DATABASE_URL";
const MAX_CONFIG_BYTES: usize = 65_536;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    observability_listen_address: String,
    worker_manifest: WorkerManifest,
    validator_principal_id: String,
    validator_digest: Sha256Digest,
    validation_profile_digest: Sha256Digest,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    claim_batch: u16,
    lease_milliseconds: i64,
    receipt_ttl_seconds: u64,
    scan_interval_milliseconds: u64,
    failure_backoff_milliseconds: u64,
    drain_grace_milliseconds: u64,
}

impl ProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded(&path)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 12,
                max_properties_per_object: 32,
                max_items_per_array: 16,
                max_string_bytes: 1024,
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
        let address: std::net::SocketAddr = self
            .observability_listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.worker_manifest
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || address.port() == 0
            || self.worker_manifest.worker_role != REGISTRY_VALIDATION_WORKER_ROLE
            || self.worker_manifest.work_class != WorkClass::RegistryValidation
            || ResourceId::parse_expected(&self.validator_principal_id, ResourceKind::Principal)
                .is_err()
            || !(2..=64).contains(&self.database_max_connections)
            || self.database_acquire_timeout_milliseconds == 0
            || self.database_acquire_timeout_milliseconds > 30_000
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        self.driver_config().map(|_| ())
    }

    fn driver_config(&self) -> Result<RegistryValidationWorkerConfig, ProcessError> {
        let config = RegistryValidationWorkerConfig {
            claim_batch: self.claim_batch,
            lease_milliseconds: self.lease_milliseconds,
            scan_interval: Duration::from_millis(self.scan_interval_milliseconds),
            failure_backoff: Duration::from_millis(self.failure_backoff_milliseconds),
            drain_grace: Duration::from_millis(self.drain_grace_milliseconds),
            receipt_ttl: Duration::from_secs(self.receipt_ttl_seconds),
        };
        config
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        Ok(config)
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-registry-validation-worker failed: {error}");
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
    let process_generation_id =
        ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let pools = LocalWorkerPools::new(config.worker_manifest.clone(), process_generation_id)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let observability_pools = pools.clone();
    let driver = RegistryValidationWorkerDriver::new(
        Arc::new(PgRepository::new(pool)),
        pools,
        ResourceId::parse_expected(&config.validator_principal_id, ResourceKind::Principal)
            .map_err(|_| ProcessError::InvalidConfiguration)?,
        config.validator_digest.clone(),
        config.validation_profile_digest.clone(),
        config.driver_config()?,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let permit_metrics = Arc::new(WorkerPermitMetrics::default());
    update_worker_permits(&permit_metrics, &observability_pools);
    let metrics = Arc::new(
        ProcessHttpMetrics::install_with_worker_permits(
            "registry-validation-worker",
            PROCESS_OBSERVABILITY_OPERATIONS,
            Arc::clone(&permit_metrics),
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let listener = tokio::net::TcpListener::bind(&config.observability_listen_address)
        .await
        .map_err(|_| ProcessError::ObservabilityFailed)?;
    let cancellation = CancellationToken::new();
    let permit_cancellation = cancellation.child_token();
    let permit_sampler = tokio::spawn(run_worker_permit_sampler(
        permit_metrics,
        observability_pools,
        permit_cancellation,
    ));
    let worker_cancellation = cancellation.child_token();
    let mut worker = tokio::spawn(driver.run(worker_cancellation));
    let http_cancellation = cancellation.child_token();
    let router = process_observability_router(Arc::clone(&metrics));
    let mut http = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(http_cancellation.cancelled_owned())
            .await
    });
    metrics.mark_ready();
    tokio::select! {
        result = wait_for_shutdown_signal() => {
            cancellation.cancel();
            result?;
            worker.await.map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            http.await.map_err(|_| ProcessError::ObservabilityFailed)?
                .map_err(|_| ProcessError::ObservabilityFailed)?;
            permit_sampler.abort();
            Ok(())
        }
        result = &mut worker => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            http.await.map_err(|_| ProcessError::ObservabilityFailed)?
                .map_err(|_| ProcessError::ObservabilityFailed)?;
            permit_sampler.abort();
            Err(ProcessError::WorkerFailed)
        }
        result = &mut http => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::ObservabilityFailed)?
                .map_err(|_| ProcessError::ObservabilityFailed)?;
            worker.await.map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            permit_sampler.abort();
            Err(ProcessError::ObservabilityFailed)
        }
    }
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

fn required(name: &'static str) -> Result<String, ProcessError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty() && value.len() <= MAX_CONFIG_BYTES)
        .ok_or(ProcessError::MissingEnvironment(name))
}

fn required_absolute_path(name: &'static str) -> Result<PathBuf, ProcessError> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(path)
}

fn read_bounded(path: &PathBuf) -> Result<Vec<u8>, ProcessError> {
    let mut file = File::open(path).map_err(|_| ProcessError::InvalidConfiguration)?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(u64::try_from(MAX_CONFIG_BYTES + 1).expect("fixed configuration bound fits u64"))
        .read_to_end(&mut bytes)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    if bytes.is_empty() || bytes.len() > MAX_CONFIG_BYTES {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}

#[derive(Debug)]
enum ProcessError {
    MissingEnvironment(&'static str),
    InvalidConfiguration,
    DatabaseUnavailable,
    SchemaMismatch,
    ObservabilityFailed,
    WorkerFailed,
    SignalUnavailable,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvironment(name) => write!(formatter, "{name} is required"),
            Self::InvalidConfiguration => {
                formatter.write_str("registry validation worker configuration is invalid")
            }
            Self::DatabaseUnavailable => formatter.write_str("PostgreSQL authority is unavailable"),
            Self::SchemaMismatch => {
                formatter.write_str("PostgreSQL authority schema is not verified")
            }
            Self::ObservabilityFailed => formatter.write_str("observability listener failed"),
            Self::WorkerFailed => {
                formatter.write_str("registry validation worker stopped unexpectedly")
            }
            Self::SignalUnavailable => formatter.write_str("shutdown signal is unavailable"),
        }
    }
}

impl Error for ProcessError {}
