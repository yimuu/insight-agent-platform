//! Sandbox Dispatcher process composition for the CR-216 OpenSandbox Kubernetes path.

use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, JsonLimits,
    SandboxRuntimeContractV1, Sha256Digest, WorkClass, WorkerManifest,
};
use insight_platform_observability::{
    process_observability_router, run_worker_permit_sampler, update_worker_permits,
    DependencyObservationMetrics, DependencyObservationOutcome, PlatformDependency,
    ProcessHttpMetrics, WorkerPermitMetrics, PROCESS_OBSERVABILITY_OPERATIONS,
};
use insight_platform_opensandbox_client::{
    OpenSandboxApiKey, OpenSandboxHttpClient, OpenSandboxHttpClientConfig,
};
use insight_platform_postgres::{repository::PgRepository, verify_schema};
use insight_platform_sandbox_dispatcher::{
    SandboxDispatcherConfig, SandboxDispatcherDriver, SandboxDispatcherTiming,
    SANDBOX_DISPATCHER_ROLE,
};
use insight_platform_worker::LocalWorkerPools;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{
    error::Error,
    fmt,
    fs::File,
    io::Read as _,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

const CONFIG_PATH_ENV: &str = "PLATFORM_SANDBOX_DISPATCHER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_SANDBOX_DISPATCHER_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_SANDBOX_DISPATCHER_DATABASE_URL";
const OPENSANDBOX_API_KEY_ENV: &str = "PLATFORM_SANDBOX_DISPATCHER_OPENSANDBOX_API_KEY";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const DEPENDENCY_SAMPLE_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SandboxDispatcherProcessConfig {
    schema_version: u32,
    observability_listen_address: String,
    worker_manifest: WorkerManifest,
    runtime_contract: SandboxRuntimeContractV1,
    runtime_contract_digest: Sha256Digest,
    opensandbox: OpenSandboxProcessConfig,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    scan_interval_milliseconds: u64,
    runner_poll_interval_milliseconds: u64,
    cleanup_scan_interval_milliseconds: u64,
    orphan_scan_interval_milliseconds: u64,
    failure_backoff_milliseconds: u64,
    drain_grace_milliseconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenSandboxProcessConfig {
    lifecycle_base_url: String,
    request_timeout_milliseconds: u32,
    connect_timeout_milliseconds: u32,
    candidate_page_items: u8,
    orphan_page_items: u16,
}

impl SandboxDispatcherProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 16,
                max_items_per_array: 64,
                max_properties_per_object: 64,
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
        let listen: std::net::SocketAddr = self
            .observability_listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.worker_manifest
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.runtime_contract
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let runtime_digest = self
            .runtime_contract
            .canonical_digest()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let profile = checked_in_hard_limit_profile();
        let lease_milliseconds = profile.run_scheduler.lease_milliseconds.q1_default;
        if self.schema_version != 1
            || listen.port() == 0
            || self.worker_manifest.worker_role != SANDBOX_DISPATCHER_ROLE
            || self.worker_manifest.work_class != WorkClass::Sandbox
            || self.runtime_contract_digest != runtime_digest
            || self.worker_manifest.adapter_runtime_digest != runtime_digest
            || !(2..=64).contains(&self.database_max_connections)
            || self.database_acquire_timeout_milliseconds == 0
            || self.database_acquire_timeout_milliseconds > 30_000
            || self.opensandbox.request_timeout_milliseconds == 0
            || u64::from(self.opensandbox.request_timeout_milliseconds).saturating_mul(12)
                >= lease_milliseconds
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        self.provider_config(OpenSandboxApiKey::parse("x".repeat(32)).expect("fixture key"))?;
        self.driver_config()?;
        Ok(())
    }

    fn provider_config(
        &self,
        api_key: OpenSandboxApiKey,
    ) -> Result<OpenSandboxHttpClientConfig, ProcessError> {
        let config = OpenSandboxHttpClientConfig {
            lifecycle_base_url: self
                .opensandbox
                .lifecycle_base_url
                .parse::<Url>()
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            api_key,
            request_timeout_milliseconds: self.opensandbox.request_timeout_milliseconds,
            connect_timeout_milliseconds: self.opensandbox.connect_timeout_milliseconds,
            candidate_page_items: self.opensandbox.candidate_page_items,
            orphan_page_items: self.opensandbox.orphan_page_items,
        };
        config
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        Ok(config)
    }

    fn driver_config(&self) -> Result<SandboxDispatcherConfig, ProcessError> {
        SandboxDispatcherConfig::from_profile(
            &checked_in_hard_limit_profile(),
            SandboxDispatcherTiming {
                scan_interval: Duration::from_millis(self.scan_interval_milliseconds),
                runner_poll_interval: Duration::from_millis(self.runner_poll_interval_milliseconds),
                cleanup_scan_interval: Duration::from_millis(
                    self.cleanup_scan_interval_milliseconds,
                ),
                orphan_scan_interval: Duration::from_millis(self.orphan_scan_interval_milliseconds),
                failure_backoff: Duration::from_millis(self.failure_backoff_milliseconds),
                drain_grace: Duration::from_millis(self.drain_grace_milliseconds),
            },
            self.runtime_contract_digest.clone(),
        )
        .map_err(|_| ProcessError::InvalidConfiguration)
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-sandbox-dispatcher failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = SandboxDispatcherProcessConfig::load()?;
    let api_key = OpenSandboxApiKey::parse(required(OPENSANDBOX_API_KEY_ENV)?)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let provider = Arc::new(
        OpenSandboxHttpClient::new(config.provider_config(api_key)?)
            .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
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
    provider
        .readiness_probe()
        .await
        .map_err(|_| ProcessError::ProviderUnavailable)?;

    let process_generation_id = insight_platform_contracts::ResourceId::from_uuid_v7(
        insight_platform_contracts::ResourceKind::WorkerProcessGeneration,
        Uuid::now_v7(),
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let pools = LocalWorkerPools::new(config.worker_manifest.clone(), process_generation_id)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let repository = Arc::new(PgRepository::new(pool.clone()));
    let driver = SandboxDispatcherDriver::new(
        repository,
        Arc::clone(&provider),
        pools.clone(),
        config.driver_config()?,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;

    let permit_metrics = Arc::new(WorkerPermitMetrics::default());
    update_worker_permits(&permit_metrics, &pools);
    let dependency_metrics = Arc::new(
        DependencyObservationMetrics::install(&[
            PlatformDependency::Postgresql,
            PlatformDependency::OpenSandbox,
        ])
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let metrics = Arc::new(
        ProcessHttpMetrics::install_with_worker_permits(
            SANDBOX_DISPATCHER_ROLE,
            PROCESS_OBSERVABILITY_OPERATIONS,
            Arc::clone(&permit_metrics),
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .with_dependency_observations(Arc::clone(&dependency_metrics)),
    );
    let listener = tokio::net::TcpListener::bind(&config.observability_listen_address)
        .await
        .map_err(|_| ProcessError::ObservabilityFailed)?;
    let readiness = Arc::new(DependencyReadiness::ready());
    metrics.mark_ready();

    eprintln!("platform-sandbox-dispatcher started");
    let cancellation = CancellationToken::new();
    let sampler_cancellation = cancellation.child_token();
    let mut sampler = tokio::spawn(run_samplers(
        DependencySampler {
            pool,
            provider,
            observations: dependency_metrics,
            metrics: Arc::clone(&metrics),
            readiness,
        },
        permit_metrics,
        pools,
        sampler_cancellation,
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

    tokio::select! {
        signal = wait_for_shutdown_signal() => {
            cancellation.cancel();
            signal?;
            worker.await.map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            http.await.map_err(|_| ProcessError::ObservabilityFailed)?
                .map_err(|_| ProcessError::ObservabilityFailed)?;
            sampler.await.map_err(|_| ProcessError::DependencyObserverFailed)?;
            Ok(())
        }
        result = &mut worker => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::WorkerFailed)?
                .map_err(|_| ProcessError::WorkerFailed)?;
            let _ = http.await;
            let _ = sampler.await;
            Err(ProcessError::WorkerFailed)
        }
        result = &mut http => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::ObservabilityFailed)?
                .map_err(|_| ProcessError::ObservabilityFailed)?;
            let _ = worker.await;
            let _ = sampler.await;
            Err(ProcessError::ObservabilityFailed)
        }
        result = &mut sampler => {
            cancellation.cancel();
            result.map_err(|_| ProcessError::DependencyObserverFailed)?;
            let _ = worker.await;
            let _ = http.await;
            Err(ProcessError::DependencyObserverFailed)
        }
    }
}

async fn run_samplers(
    dependencies: DependencySampler,
    permit_metrics: Arc<WorkerPermitMetrics>,
    pools: LocalWorkerPools,
    cancellation: CancellationToken,
) {
    tokio::join!(
        run_worker_permit_sampler(permit_metrics, pools, cancellation.child_token()),
        run_dependency_sampler(dependencies, cancellation),
    );
}

async fn run_dependency_sampler(dependencies: DependencySampler, cancellation: CancellationToken) {
    let mut interval = tokio::time::interval(DEPENDENCY_SAMPLE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            _ = interval.tick() => {
                let postgres_ok = matches!(
                    sqlx::query_scalar::<_, i64>("SELECT 1::bigint").fetch_one(&dependencies.pool).await,
                    Ok(1)
                );
                dependencies.readiness.postgres.store(postgres_ok, Ordering::Release);
                let _ = dependencies.observations.observe(
                    PlatformDependency::Postgresql,
                    outcome(postgres_ok),
                );
                let provider_ok = dependencies.provider.readiness_probe().await.is_ok();
                dependencies.readiness.opensandbox.store(provider_ok, Ordering::Release);
                let _ = dependencies.observations.observe(
                    PlatformDependency::OpenSandbox,
                    outcome(provider_ok),
                );
                dependencies.readiness.apply(&dependencies.metrics);
            }
        }
    }
}

struct DependencySampler {
    pool: sqlx::PgPool,
    provider: Arc<OpenSandboxHttpClient>,
    observations: Arc<DependencyObservationMetrics>,
    metrics: Arc<ProcessHttpMetrics>,
    readiness: Arc<DependencyReadiness>,
}

fn outcome(success: bool) -> DependencyObservationOutcome {
    if success {
        DependencyObservationOutcome::Success
    } else {
        DependencyObservationOutcome::Failure
    }
}

struct DependencyReadiness {
    postgres: AtomicBool,
    opensandbox: AtomicBool,
}

impl DependencyReadiness {
    fn ready() -> Self {
        Self {
            postgres: AtomicBool::new(true),
            opensandbox: AtomicBool::new(true),
        }
    }

    fn apply(&self, metrics: &ProcessHttpMetrics) {
        if self.postgres.load(Ordering::Acquire) && self.opensandbox.load(Ordering::Acquire) {
            metrics.mark_ready();
        } else {
            metrics.mark_not_ready();
        }
    }
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

fn read_bounded(path: &PathBuf, maximum: usize) -> Result<Vec<u8>, ProcessError> {
    let file = File::open(path).map_err(|_| ProcessError::InvalidConfiguration)?;
    let mut bytes = Vec::new();
    file.take(u64::try_from(maximum).unwrap_or(u64::MAX).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> Result<(), ProcessError> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut terminate = signal(SignalKind::terminate()).map_err(|_| ProcessError::SignalFailed)?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.map_err(|_| ProcessError::SignalFailed),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> Result<(), ProcessError> {
    tokio::signal::ctrl_c()
        .await
        .map_err(|_| ProcessError::SignalFailed)
}

#[derive(Debug)]
enum ProcessError {
    MissingEnvironment(&'static str),
    InvalidConfiguration,
    DatabaseUnavailable,
    SchemaMismatch,
    ProviderUnavailable,
    ObservabilityFailed,
    WorkerFailed,
    DependencyObserverFailed,
    SignalFailed,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvironment(name) => {
                write!(formatter, "required environment {name} is missing")
            }
            Self::InvalidConfiguration => formatter.write_str("configuration is invalid"),
            Self::DatabaseUnavailable => formatter.write_str("PostgreSQL is unavailable"),
            Self::SchemaMismatch => formatter.write_str("PostgreSQL schema is incompatible"),
            Self::ProviderUnavailable => formatter.write_str("OpenSandbox provider is unavailable"),
            Self::ObservabilityFailed => formatter.write_str("observability listener failed"),
            Self::WorkerFailed => formatter.write_str("dispatcher worker failed"),
            Self::DependencyObserverFailed => formatter.write_str("dependency observer failed"),
            Self::SignalFailed => formatter.write_str("shutdown signal failed"),
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        SandboxProviderKind, WORKER_MANIFEST_VERSION, WORKER_PROTOCOL_VERSION,
    };

    #[test]
    fn process_config_rejects_runtime_digest_drift_and_wrong_role() {
        let mut config = fixture();
        assert!(config.validate().is_ok());
        config.runtime_contract_digest = digest('f');
        assert!(matches!(
            config.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));
        let mut config = fixture();
        config.worker_manifest.worker_role = "context-worker".to_owned();
        assert!(matches!(
            config.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));
    }

    #[test]
    fn process_config_rejects_provider_timeout_that_can_consume_the_lease() {
        let mut config = fixture();
        config.opensandbox.request_timeout_milliseconds = 10_000;
        assert!(matches!(
            config.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));
    }

    fn fixture() -> SandboxDispatcherProcessConfig {
        let runtime_contract = SandboxRuntimeContractV1 {
            schema_version: 1,
            provider: SandboxProviderKind::OpenSandboxKubernetes,
            opensandbox_server_release_digest: digest('a'),
            lifecycle_schema_digest: digest('b'),
            batchsandbox_crd_digest: digest('c'),
            batchsandbox_controller_digest: digest('d'),
            kubernetes_provider_template_digest: digest('e'),
            runner_protocol_digest: digest('1'),
            container_runtime_digest: digest('2'),
            network_policy_digest: digest('3'),
        };
        let runtime_contract_digest = runtime_contract.canonical_digest().unwrap();
        SandboxDispatcherProcessConfig {
            schema_version: 1,
            observability_listen_address: "127.0.0.1:9080".to_owned(),
            worker_manifest: WorkerManifest {
                manifest_version: WORKER_MANIFEST_VERSION,
                worker_role: SANDBOX_DISPATCHER_ROLE.to_owned(),
                work_class: WorkClass::Sandbox,
                adapter_runtime_digest: runtime_contract_digest.clone(),
                protocol_version: WORKER_PROTOCOL_VERSION,
                max_concurrency: 2,
                critical_control_reserved_slots: 1,
            },
            runtime_contract,
            runtime_contract_digest,
            opensandbox: OpenSandboxProcessConfig {
                lifecycle_base_url:
                    "http://opensandbox-server.sandbox-system.svc.cluster.local:8080/v1/".to_owned(),
                request_timeout_milliseconds: 2_000,
                connect_timeout_milliseconds: 500,
                candidate_page_items: 4,
                orphan_page_items: 50,
            },
            database_max_connections: 8,
            database_acquire_timeout_milliseconds: 2_000,
            scan_interval_milliseconds: 1_000,
            runner_poll_interval_milliseconds: 1_000,
            cleanup_scan_interval_milliseconds: 2_000,
            orphan_scan_interval_milliseconds: 5_000,
            failure_backoff_milliseconds: 100,
            drain_grace_milliseconds: 30_000,
        }
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }
}
