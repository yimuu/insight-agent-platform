//! Deployable Context Worker for the clean-cut Platform v1 candidate.
//!
//! The first static backend is NativeCatalog. This process intentionally has no Egress, Secret,
//! Sandbox, or arbitrary SQL execution client; exact Job state remains in PostgreSQL.

mod dependency_observer;

use dependency_observer::install_context_dependency_metrics;

use insight_platform_context_worker::{
    ContextWorkerConfig, ContextWorkerDriver, ContextWorkerTiming, NativeCatalogAdapter,
    CONTEXT_WORKER_ROLE,
};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, JsonLimits, ResourceId,
    ResourceKind, Sha256Digest, WorkClass, WorkerManifest,
};
use insight_platform_observability::{
    process_observability_router, run_worker_permit_sampler, update_worker_permits,
    ProcessHttpMetrics, WorkerPermitMetrics, PROCESS_OBSERVABILITY_OPERATIONS,
};
use insight_platform_postgres::{
    dependency_health::run_postgres_health_sampler, repository::PgRepository, verify_schema,
};
use insight_platform_worker::LocalWorkerPools;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{error::Error, fmt, fs::File, io::Read as _, path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const CONFIG_PATH_ENV: &str = "PLATFORM_CONTEXT_WORKER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_CONTEXT_WORKER_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_CONTEXT_WORKER_DATABASE_URL";
const MAX_CONFIG_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextWorkerProcessConfig {
    schema_version: u32,
    observability_listen_address: String,
    worker_manifest: WorkerManifest,
    native_catalog: NativeCatalogAdapter,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    receipt_ttl_seconds: u64,
    scan_interval_milliseconds: u64,
    failure_backoff_milliseconds: u64,
    drain_grace_milliseconds: u64,
}

impl ContextWorkerProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded(&path, MAX_CONFIG_BYTES)?;
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
        self.worker_manifest
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.native_catalog
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || observability.port() == 0
            || self.worker_manifest.worker_role != CONTEXT_WORKER_ROLE
            || self.worker_manifest.work_class != WorkClass::Context
            || self.worker_manifest.adapter_runtime_digest
                != self.native_catalog.installed_adapter_digest
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
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        self.driver_config().map(|_| ())
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
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-context-worker failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = ContextWorkerProcessConfig::load()?;
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
    let process_generation_id =
        ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let pools = LocalWorkerPools::new(
        config.worker_manifest.clone(),
        process_generation_id.clone(),
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let observability_pools = pools.clone();
    let driver = ContextWorkerDriver::new(
        repository,
        pools,
        config.native_catalog.clone(),
        config.driver_config()?,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let permit_metrics = Arc::new(WorkerPermitMetrics::default());
    update_worker_permits(&permit_metrics, &observability_pools);
    let dependency_metrics =
        install_context_dependency_metrics().map_err(|_| ProcessError::InvalidConfiguration)?;
    let metrics = Arc::new(
        ProcessHttpMetrics::install_with_worker_permits(
            "context-native-worker",
            PROCESS_OBSERVABILITY_OPERATIONS,
            Arc::clone(&permit_metrics),
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .with_dependency_observations(dependency_metrics.process),
    );
    let listener = tokio::net::TcpListener::bind(&config.observability_listen_address)
        .await
        .map_err(|_| ProcessError::ObservabilityFailed)?;
    eprintln!("platform-context-worker started");
    let cancellation = CancellationToken::new();
    let sampler_cancellation = cancellation.child_token();
    let mut sampler = tokio::spawn(async move {
        tokio::join!(
            run_worker_permit_sampler(
                permit_metrics,
                observability_pools,
                sampler_cancellation.child_token(),
            ),
            run_postgres_health_sampler(
                database_health_pool,
                dependency_metrics.postgres,
                sampler_cancellation,
            ),
        );
    });
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
    tokio::select! {
        signal = wait_for_shutdown_signal() => {
            cancellation.cancel();
            let worker_result = worker.await.map_err(|_| ProcessError::WorkerFailed)
                .and_then(|result| result.map(|_| ()).map_err(|_| ProcessError::WorkerFailed));
            let http_result = http.await.map_err(|_| ProcessError::ObservabilityFailed)
                .and_then(|result| result.map_err(|_| ProcessError::ObservabilityFailed));
            let sampler_result = sampler.await.map_err(|_| ProcessError::DependencyObserverFailed);
            signal?;
            worker_result?;
            http_result?;
            sampler_result?;
            Ok(())
        }
        result = &mut worker => {
            cancellation.cancel();
            let worker_result = result.map_err(|_| ProcessError::WorkerFailed)
                .and_then(|result| result.map(|_| ()).map_err(|_| ProcessError::WorkerFailed));
            let http_result = http.await.map_err(|_| ProcessError::ObservabilityFailed)
                .and_then(|result| result.map_err(|_| ProcessError::ObservabilityFailed));
            let sampler_result = sampler.await.map_err(|_| ProcessError::DependencyObserverFailed);
            worker_result?;
            http_result?;
            sampler_result?;
            Err(ProcessError::WorkerFailed)
        }
        result = &mut http => {
            cancellation.cancel();
            let http_result = result.map_err(|_| ProcessError::ObservabilityFailed)
                .and_then(|result| result.map_err(|_| ProcessError::ObservabilityFailed));
            let worker_result = worker.await.map_err(|_| ProcessError::WorkerFailed)
                .and_then(|result| result.map(|_| ()).map_err(|_| ProcessError::WorkerFailed));
            let sampler_result = sampler.await.map_err(|_| ProcessError::DependencyObserverFailed);
            http_result?;
            worker_result?;
            sampler_result?;
            Err(ProcessError::ObservabilityFailed)
        }
        result = &mut sampler => {
            cancellation.cancel();
            let sampler_result = result.map_err(|_| ProcessError::DependencyObserverFailed);
            let worker_result = worker.await.map_err(|_| ProcessError::WorkerFailed)
                .and_then(|result| result.map(|_| ()).map_err(|_| ProcessError::WorkerFailed));
            let http_result = http.await.map_err(|_| ProcessError::ObservabilityFailed)
                .and_then(|result| result.map_err(|_| ProcessError::ObservabilityFailed));
            sampler_result?;
            worker_result?;
            http_result?;
            Err(ProcessError::DependencyObserverFailed)
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

fn read_bounded(path: &PathBuf, maximum: usize) -> Result<Vec<u8>, ProcessError> {
    let mut file = File::open(path).map_err(|_| ProcessError::InvalidConfiguration)?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(u64::try_from(maximum + 1).map_err(|_| ProcessError::InvalidConfiguration)?)
        .read_to_end(&mut bytes)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    if bytes.is_empty() || bytes.len() > maximum {
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
    SignalUnavailable,
    WorkerFailed,
    DependencyObserverFailed,
    ObservabilityFailed,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvironment(name) => {
                write!(formatter, "required environment {name} is missing")
            }
            Self::InvalidConfiguration => formatter.write_str("configuration is invalid"),
            Self::DatabaseUnavailable => formatter.write_str("database is unavailable"),
            Self::SchemaMismatch => formatter.write_str("database schema does not match candidate"),
            Self::SignalUnavailable => formatter.write_str("shutdown signal is unavailable"),
            Self::WorkerFailed => formatter.write_str("Context Worker failed"),
            Self::DependencyObserverFailed => {
                formatter.write_str("Context dependency observer failed")
            }
            Self::ObservabilityFailed => formatter.write_str("observability server failed"),
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        DataClassification, WORKER_MANIFEST_VERSION, WORKER_PROTOCOL_VERSION,
    };

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn fixture() -> ContextWorkerProcessConfig {
        let installed = digest('a');
        ContextWorkerProcessConfig {
            schema_version: 1,
            observability_listen_address: "127.0.0.1:9090".to_owned(),
            worker_manifest: WorkerManifest {
                manifest_version: WORKER_MANIFEST_VERSION,
                worker_role: CONTEXT_WORKER_ROLE.to_owned(),
                work_class: WorkClass::Context,
                adapter_runtime_digest: installed.clone(),
                protocol_version: WORKER_PROTOCOL_VERSION,
                max_concurrency: 8,
                critical_control_reserved_slots: 2,
            },
            native_catalog: NativeCatalogAdapter {
                schema_version: 1,
                installed_adapter_digest: installed,
                adapter_contract_digest: digest('b'),
                source_item_identity_digest: digest('c'),
                content: serde_json::json!("bounded catalog entry"),
                structured_fields_schema_digest: digest('d'),
                score_millionths: Some(500_000),
                locator_digest: digest('e'),
                authorization_evidence_digest: digest('f'),
                ranking_evidence_digest: digest('1'),
                display_label: "catalog entry".to_owned(),
                classification: DataClassification::Internal,
            },
            database_max_connections: 8,
            database_acquire_timeout_milliseconds: 5_000,
            receipt_ttl_seconds: 3_600,
            scan_interval_milliseconds: 1_000,
            failure_backoff_milliseconds: 100,
            drain_grace_milliseconds: 30_000,
        }
    }

    #[test]
    fn accepts_exact_native_catalog_runtime() {
        fixture().validate().unwrap();
    }

    #[test]
    fn rejects_installed_adapter_drift_before_startup() {
        let mut config = fixture();
        config.native_catalog.installed_adapter_digest = digest('9');
        assert!(matches!(
            config.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));
    }

    #[test]
    fn rejects_wrong_process_pool() {
        let mut config = fixture();
        config.worker_manifest.work_class = WorkClass::Model;
        assert!(matches!(
            config.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));
    }
}
