//! RemoteSearch Context Worker. Outbound access is available only through mTLS Egress RPC.

mod dependency_observer;

use dependency_observer::install_context_dependency_metrics;

use insight_platform_context_worker::{
    ContextWorkerConfig, ContextWorkerTiming, RemoteContextWorkerDriver, CONTEXT_WORKER_ROLE,
};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, JsonLimits, ResourceId,
    ResourceKind, Sha256Digest, WorkClass, WorkerManifest,
};
use insight_platform_egress_rpc::{
    EgressBrokerGrpcClient, EgressInternalRpcLimits, EgressRpcDependencyObserver,
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
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use uuid::Uuid;

const CONFIG_PATH_ENV: &str = "PLATFORM_REMOTE_CONTEXT_WORKER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_REMOTE_CONTEXT_WORKER_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_REMOTE_CONTEXT_WORKER_DATABASE_URL";
const EGRESS_CA_PATH_ENV: &str = "PLATFORM_REMOTE_CONTEXT_WORKER_EGRESS_CA_PATH";
const EGRESS_CERT_PATH_ENV: &str = "PLATFORM_REMOTE_CONTEXT_WORKER_EGRESS_CERT_PATH";
const EGRESS_KEY_PATH_ENV: &str = "PLATFORM_REMOTE_CONTEXT_WORKER_EGRESS_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_TLS_FILE_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    observability_listen_address: String,
    worker_manifest: WorkerManifest,
    installed_adapter_digest: Sha256Digest,
    egress_endpoint: String,
    egress_tls_server_name: String,
    maximum_rpc_metadata_bytes: usize,
    maximum_rpc_payload_bytes: usize,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    receipt_ttl_seconds: u64,
    scan_interval_milliseconds: u64,
    failure_backoff_milliseconds: u64,
    drain_grace_milliseconds: u64,
}

impl ProcessConfig {
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
        EgressInternalRpcLimits::new(
            self.maximum_rpc_metadata_bytes,
            self.maximum_rpc_payload_bytes,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || observability.port() == 0
            || self.worker_manifest.worker_role != CONTEXT_WORKER_ROLE
            || self.worker_manifest.work_class != WorkClass::Context
            || self.worker_manifest.adapter_runtime_digest != self.installed_adapter_digest
            || !self.egress_endpoint.starts_with("https://")
            || Endpoint::from_shared(self.egress_endpoint.clone()).is_err()
            || !valid_tls_server_name(&self.egress_tls_server_name)
            || self.connect_timeout_milliseconds == 0
            || self.connect_timeout_milliseconds > 30_000
            || self.request_timeout_milliseconds == 0
            || self.request_timeout_milliseconds > 3_600_000
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
        eprintln!("platform-remote-context-worker failed: {error}");
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
    let process_generation_id =
        ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let pools = LocalWorkerPools::new(
        config.worker_manifest.clone(),
        process_generation_id.clone(),
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let observability_pools = pools.clone();
    let dependency_metrics =
        install_context_dependency_metrics(true).map_err(|_| ProcessError::InvalidConfiguration)?;
    let connector = Arc::new(
        connect_egress(
            &config,
            dependency_metrics
                .egress
                .ok_or(ProcessError::InvalidConfiguration)?,
        )
        .await?,
    );
    let driver =
        RemoteContextWorkerDriver::new(repository, pools, connector, config.driver_config()?)
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let permit_metrics = Arc::new(WorkerPermitMetrics::default());
    update_worker_permits(&permit_metrics, &observability_pools);
    let metrics = Arc::new(
        ProcessHttpMetrics::install_with_worker_permits(
            "context-remote-worker",
            PROCESS_OBSERVABILITY_OPERATIONS,
            Arc::clone(&permit_metrics),
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .with_dependency_observations(dependency_metrics.process),
    );
    let listener = tokio::net::TcpListener::bind(&config.observability_listen_address)
        .await
        .map_err(|_| ProcessError::ObservabilityFailed)?;
    eprintln!("platform-remote-context-worker started");
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

async fn connect_egress(
    config: &ProcessConfig,
    dependency_observer: Arc<dyn EgressRpcDependencyObserver>,
) -> Result<EgressBrokerGrpcClient, ProcessError> {
    let limits = EgressInternalRpcLimits::new(
        config.maximum_rpc_metadata_bytes,
        config.maximum_rpc_payload_bytes,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let tls = ClientTlsConfig::new()
        .domain_name(config.egress_tls_server_name.clone())
        .ca_certificate(Certificate::from_pem(read_bounded(
            &required_absolute_path(EGRESS_CA_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?))
        .identity(Identity::from_pem(
            read_bounded(
                &required_absolute_path(EGRESS_CERT_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
            read_bounded(
                &required_absolute_path(EGRESS_KEY_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
        ));
    let channel = Endpoint::from_shared(config.egress_endpoint.clone())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(config.connect_timeout_milliseconds))
        .timeout(Duration::from_millis(config.request_timeout_milliseconds))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect()
        .await
        .map_err(|_| ProcessError::EgressUnavailable)?;
    Ok(EgressBrokerGrpcClient::new_with_observer(
        channel,
        limits,
        dependency_observer,
    ))
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

#[derive(Debug)]
enum ProcessError {
    MissingEnvironment(&'static str),
    InvalidConfiguration,
    DatabaseUnavailable,
    EgressUnavailable,
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
            Self::EgressUnavailable => formatter.write_str("Egress Broker is unavailable"),
            Self::SchemaMismatch => formatter.write_str("database schema does not match candidate"),
            Self::SignalUnavailable => formatter.write_str("shutdown signal is unavailable"),
            Self::WorkerFailed => formatter.write_str("Remote Context Worker failed"),
            Self::DependencyObserverFailed => {
                formatter.write_str("Remote Context dependency observer failed")
            }
            Self::ObservabilityFailed => formatter.write_str("observability server failed"),
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{WORKER_MANIFEST_VERSION, WORKER_PROTOCOL_VERSION};

    fn digest(marker: char) -> Sha256Digest {
        format!("sha256:{}", marker.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn fixture() -> ProcessConfig {
        let installed = digest('1');
        ProcessConfig {
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
            installed_adapter_digest: installed,
            egress_endpoint: "https://egress.platform-egress.svc:8443".to_owned(),
            egress_tls_server_name: "egress.platform-egress.svc".to_owned(),
            maximum_rpc_metadata_bytes: 1_048_576,
            maximum_rpc_payload_bytes: 1_048_576,
            connect_timeout_milliseconds: 5_000,
            request_timeout_milliseconds: 30_000,
            database_max_connections: 8,
            database_acquire_timeout_milliseconds: 5_000,
            receipt_ttl_seconds: 3_600,
            scan_interval_milliseconds: 1_000,
            failure_backoff_milliseconds: 100,
            drain_grace_milliseconds: 30_000,
        }
    }

    #[test]
    fn accepts_exact_remote_runtime_and_rejects_adapter_drift() {
        fixture().validate().unwrap();
        let mut drifted = fixture();
        drifted.installed_adapter_digest = digest('2');
        assert!(matches!(
            drifted.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));
    }
}
