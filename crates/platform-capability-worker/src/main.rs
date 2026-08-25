//! Deployable native Capability Worker for the clean-cut Platform v1 candidate.

use insight_platform_capability_adapters::{
    CapabilityAdapterWorker, CapabilityDispatcher, InstalledNativeAdapter, InstalledNativeRegistry,
};
use insight_platform_capability_worker::{
    install_builtin_native_adapters, BuiltinEchoCapabilityAdapter, CapabilityWorkerDriver,
    CapabilityWorkerDriverConfig, CapabilityWorkerDriverTiming, RegisteredCapabilityJobExecutor,
    UuidCapabilityWorkerIdentityFactory, CAPABILITY_NATIVE_WORKER_ROLE,
};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, JsonLimits, ResourceId,
    ResourceKind, Sha256Digest, WorkClass, WorkerManifest,
};
use insight_platform_postgres::{repository::PgRepository, verify_schema};
use insight_platform_worker::LocalWorkerPools;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::{error::Error, fmt, fs::File, io::Read as _, path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const CONFIG_PATH_ENV: &str = "PLATFORM_CAPABILITY_NATIVE_WORKER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_CAPABILITY_NATIVE_WORKER_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_CAPABILITY_NATIVE_WORKER_DATABASE_URL";
const MAX_CONFIG_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    worker_manifest: WorkerManifest,
    installed_adapters: Vec<InstalledNativeAdapterConfig>,
    database: DatabaseConfig,
    timing: TimingConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InstalledNativeAdapterConfig {
    adapter_id: String,
    adapter_version: String,
    module_digest: Sha256Digest,
    entrypoint_id: String,
}

impl From<&InstalledNativeAdapter> for InstalledNativeAdapterConfig {
    fn from(value: &InstalledNativeAdapter) -> Self {
        Self {
            adapter_id: value.adapter_id.clone(),
            adapter_version: value.adapter_version.clone(),
            module_digest: value.module_digest.clone(),
            entrypoint_id: value.entrypoint_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatabaseConfig {
    business_max_connections: u32,
    critical_control_max_connections: u32,
    process_connection_budget: u32,
    acquire_timeout_milliseconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimingConfig {
    receipt_ttl_milliseconds: u64,
    safety_scan_milliseconds: u64,
    claim_failure_backoff_milliseconds: u64,
    drain_grace_milliseconds: u64,
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
                max_items_per_array: 64,
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
        self.worker_manifest
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let expected_adapter = InstalledNativeAdapterConfig::from(
            &BuiltinEchoCapabilityAdapter::installed_descriptor(),
        );
        let installed_value = serde_json::to_value(&self.installed_adapters)
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let installed_digest: Sha256Digest = canonical_digest(&installed_value)
            .map_err(|_| ProcessError::InvalidConfiguration)?
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || self.worker_manifest.worker_role != CAPABILITY_NATIVE_WORKER_ROLE
            || self.worker_manifest.work_class != WorkClass::CapabilityNative
            || self.worker_manifest.adapter_runtime_digest != installed_digest
            || self.installed_adapters.as_slice() != [expected_adapter]
            || self.database.business_max_connections == 0
            || self.database.critical_control_max_connections == 0
            || self.database.process_connection_budget == 0
            || self
                .database
                .business_max_connections
                .checked_add(self.database.critical_control_max_connections)
                != Some(self.database.process_connection_budget)
            || self.database.acquire_timeout_milliseconds == 0
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        self.driver_config()?;
        Ok(())
    }

    fn driver_config(&self) -> Result<CapabilityWorkerDriverConfig, ProcessError> {
        CapabilityWorkerDriverConfig::from_profile(
            &checked_in_hard_limit_profile(),
            Duration::from_millis(self.timing.receipt_ttl_milliseconds),
            CapabilityWorkerDriverTiming {
                safety_scan_interval: Duration::from_millis(self.timing.safety_scan_milliseconds),
                claim_failure_backoff: Duration::from_millis(
                    self.timing.claim_failure_backoff_milliseconds,
                ),
                drain_grace: Duration::from_millis(self.timing.drain_grace_milliseconds),
            },
        )
        .map_err(|_| ProcessError::InvalidConfiguration)
    }
}

#[tokio::main]
async fn main() {
    if let Err(failure) = run().await {
        eprintln!("platform-capability-native-worker failed: {failure}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = ProcessConfig::load()?;
    let database_url = required(DATABASE_URL_ENV)?;
    let acquire_timeout = Duration::from_millis(config.database.acquire_timeout_milliseconds);
    let business_pool = PgPoolOptions::new()
        .max_connections(config.database.business_max_connections)
        .acquire_timeout(acquire_timeout)
        .connect(&database_url)
        .await
        .map_err(|_| ProcessError::DatabaseUnavailable)?;
    let critical_control_pool = PgPoolOptions::new()
        .max_connections(config.database.critical_control_max_connections)
        .acquire_timeout(acquire_timeout)
        .connect(&database_url)
        .await
        .map_err(|_| ProcessError::DatabaseUnavailable)?;
    verify_schema(&critical_control_pool)
        .await
        .map_err(|_| ProcessError::SchemaMismatch)?;

    let process_generation_id =
        ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let pools = LocalWorkerPools::new(
        config.worker_manifest.clone(),
        process_generation_id.clone(),
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let manifest_digest = config
        .worker_manifest
        .canonical_digest()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let claim_repository = Arc::new(PgRepository::new(business_pool.clone()));
    let commit_repository = PgRepository::new(business_pool.clone());
    let heartbeat_repository = Arc::new(PgRepository::new(critical_control_pool.clone()));
    let mut native = InstalledNativeRegistry::default();
    install_builtin_native_adapters(&mut native).map_err(|_| ProcessError::InvalidConfiguration)?;
    let worker = Arc::new(CapabilityAdapterWorker::new(
        Arc::new(CapabilityDispatcher::new(native)),
        commit_repository,
    ));
    let executor = Arc::new(RegisteredCapabilityJobExecutor::new(
        worker,
        heartbeat_repository,
    ));
    let driver = CapabilityWorkerDriver::new(
        claim_repository,
        executor,
        Arc::new(UuidCapabilityWorkerIdentityFactory),
        pools,
        config.driver_config()?,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    eprintln!(
        "platform-capability-native-worker started generation={} manifest={}",
        process_generation_id, manifest_digest
    );

    let cancellation = CancellationToken::new();
    let worker_cancellation = cancellation.child_token();
    let mut worker_task = tokio::spawn(async move { driver.run(worker_cancellation).await });
    tokio::select! {
        signal = shutdown_signal() => {
            signal?;
            cancellation.cancel();
            worker_task.await
                .map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
        }
        result = &mut worker_task => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            return Err(ProcessError::WorkerExitedUnexpectedly);
        }
    }
    business_pool.close().await;
    critical_control_pool.close().await;
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<(), ProcessError> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut terminate =
        signal(SignalKind::terminate()).map_err(|_| ProcessError::SignalUnavailable)?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.map_err(|_| ProcessError::SignalUnavailable),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<(), ProcessError> {
    tokio::signal::ctrl_c()
        .await
        .map_err(|_| ProcessError::SignalUnavailable)
}

fn required(name: &'static str) -> Result<String, ProcessError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
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
    let mut file = File::open(path).map_err(|_| ProcessError::InvalidConfiguration)?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(u64::try_from(maximum).unwrap_or(u64::MAX).saturating_add(1))
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
    WorkerExitedUnexpectedly,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvironment(name) => {
                write!(formatter, "required environment {name} is missing")
            }
            Self::InvalidConfiguration => formatter.write_str("process configuration is invalid"),
            Self::DatabaseUnavailable => formatter.write_str("PostgreSQL is unavailable"),
            Self::SchemaMismatch => {
                formatter.write_str("PostgreSQL schema contract does not match")
            }
            Self::SignalUnavailable => {
                formatter.write_str("shutdown signal handler is unavailable")
            }
            Self::WorkerFailed => formatter.write_str("Capability Worker failed"),
            Self::WorkerExitedUnexpectedly => {
                formatter.write_str("Capability Worker exited unexpectedly")
            }
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{WORKER_MANIFEST_VERSION, WORKER_PROTOCOL_VERSION};

    fn config() -> ProcessConfig {
        let installed_adapters = vec![InstalledNativeAdapterConfig::from(
            &BuiltinEchoCapabilityAdapter::installed_descriptor(),
        )];
        let adapter_runtime_digest =
            canonical_digest(&serde_json::to_value(&installed_adapters).unwrap())
                .unwrap()
                .parse()
                .unwrap();
        ProcessConfig {
            schema_version: 1,
            worker_manifest: WorkerManifest {
                manifest_version: WORKER_MANIFEST_VERSION,
                worker_role: CAPABILITY_NATIVE_WORKER_ROLE.to_owned(),
                work_class: WorkClass::CapabilityNative,
                adapter_runtime_digest,
                protocol_version: WORKER_PROTOCOL_VERSION,
                max_concurrency: 4,
                critical_control_reserved_slots: 1,
            },
            installed_adapters,
            database: DatabaseConfig {
                business_max_connections: 4,
                critical_control_max_connections: 2,
                process_connection_budget: 6,
                acquire_timeout_milliseconds: 1_000,
            },
            timing: TimingConfig {
                receipt_ttl_milliseconds: 60_000,
                safety_scan_milliseconds: 100,
                claim_failure_backoff_milliseconds: 10,
                drain_grace_milliseconds: 5_000,
            },
        }
    }

    #[test]
    fn config_binds_exact_role_adapter_digest_and_database_budget() {
        config().validate().unwrap();
        let mut wrong_adapter = config();
        wrong_adapter.installed_adapters[0].module_digest =
            "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                .parse()
                .unwrap();
        assert!(wrong_adapter.validate().is_err());
        let mut wrong_budget = config();
        wrong_budget.database.process_connection_budget += 1;
        assert!(wrong_budget.validate().is_err());
        let mut wrong_lane = config();
        wrong_lane.worker_manifest.work_class = WorkClass::CapabilityRemote;
        assert!(wrong_lane.validate().is_err());
    }
}
