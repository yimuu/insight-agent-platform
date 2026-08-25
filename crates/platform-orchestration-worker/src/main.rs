//! Deployable durable orchestration process for the clean-cut Platform v1 candidate.

use insight_platform_artifact_rpc::{ArtifactInternalRpcLimits, ArtifactSchedulerGrpcClient};
use insight_platform_artifacts::MAX_TYPED_PLAN_ARTIFACT_BYTES;
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, JsonLimits, ResourceId,
    ResourceKind, Sha256Digest, WorkClass, WorkerManifest,
};
use insight_platform_observability::{
    process_observability_router, DurableJobQueueSnapshot, DurableOutboxSnapshot,
    OrchestrationOperationalMetrics, OrchestrationOperationalSnapshot, ProcessHttpMetrics,
    PROCESS_OBSERVABILITY_OPERATIONS,
};
use insight_platform_orchestrator::{ExpressionLimits, PlanLimits};
use insight_platform_postgres::{
    operational_metrics::{observe_durable_job_queue, observe_durable_outbox},
    repository::SafetyScanShard,
    verify_schema,
};
use insight_platform_runtime::postgres::{
    PostgresConnectionBulkheadConfig, PostgresConnectionBulkheads,
};
use insight_platform_runtime::{
    start_production_orchestration, CoordinatorTiming, OrchestrationCoordinatorConfig,
    OrchestrationExecutorConfig, OrchestrationExecutorTiming, OrchestrationSafetyConfig,
    ProductionOrchestrationConfig, RunningProductionOrchestration, SafetyDriverTiming,
    SchedulerPlanMaterializerConfig,
};
use insight_platform_worker::LocalWorkerPools;
use serde::Deserialize;
use std::{error::Error, fmt, io::Read as _, path::PathBuf, sync::Arc, time::Duration};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use url::Url;
use uuid::Uuid;

const WORKER_ROLE: &str = "orchestration-worker";
const CONFIG_PATH_ENV: &str = "PLATFORM_ORCHESTRATION_WORKER_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_ORCHESTRATION_WORKER_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_ORCHESTRATION_WORKER_DATABASE_URL";
const ARTIFACT_CA_PATH_ENV: &str = "PLATFORM_ORCHESTRATION_WORKER_ARTIFACT_CA_PATH";
const ARTIFACT_CERT_PATH_ENV: &str = "PLATFORM_ORCHESTRATION_WORKER_ARTIFACT_CERT_PATH";
const ARTIFACT_KEY_PATH_ENV: &str = "PLATFORM_ORCHESTRATION_WORKER_ARTIFACT_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_TLS_FILE_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    observability_listen_address: String,
    worker_manifest: WorkerManifest,
    database: DatabaseConfig,
    artifact: ArtifactConfig,
    timing: TimingConfig,
    plan_maximum_bytes: usize,
    safety_shard: SafetyScanShard,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatabaseConfig {
    business_max_connections: u32,
    critical_control_reserved_connections: u32,
    process_connection_budget: u32,
    acquire_timeout_milliseconds: u64,
    statement_timeout_milliseconds: u64,
    idle_timeout_milliseconds: u64,
    max_lifetime_milliseconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactConfig {
    endpoint: String,
    tls_server_name: String,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
    maximum_request_bytes: usize,
    maximum_chunk_bytes: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimingConfig {
    coordinator_coalesce_milliseconds: u64,
    coordinator_scan_milliseconds: u64,
    coordinator_scan_jitter_milliseconds: u64,
    claim_failure_backoff_milliseconds: u64,
    drain_grace_milliseconds: u64,
    heartbeat_jitter_milliseconds: u64,
    store_retry_backoff_milliseconds: u64,
    safety_scan_milliseconds: u64,
    safety_scan_jitter_milliseconds: u64,
    safety_failure_backoff_milliseconds: u64,
    handoff_retry_milliseconds: u64,
}

impl ProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let bytes = read_bounded_file(&required_absolute_path(CONFIG_PATH_ENV)?, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 16,
                max_properties_per_object: 64,
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
        let profile = checked_in_hard_limit_profile();
        let observability: std::net::SocketAddr = self
            .observability_listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.worker_manifest
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let endpoint =
            Url::parse(&self.artifact.endpoint).map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || observability.port() == 0
            || self.worker_manifest.worker_role != WORKER_ROLE
            || self.worker_manifest.work_class != WorkClass::Orchestration
            || endpoint.scheme() != "https"
            || endpoint.host_str().is_none()
            || endpoint.port().is_none()
            || endpoint.username() != ""
            || endpoint.password().is_some()
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !valid_tls_server_name(&self.artifact.tls_server_name)
            || self.artifact.connect_timeout_milliseconds == 0
            || self.artifact.request_timeout_milliseconds == 0
            || self.plan_maximum_bytes == 0
            || self.plan_maximum_bytes > MAX_TYPED_PLAN_ARTIFACT_BYTES
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        self.database_bulkhead_config()
            .validate(&profile)
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.artifact_rpc_limits()?;
        self.runtime_config()?;
        Ok(())
    }

    fn database_bulkhead_config(&self) -> PostgresConnectionBulkheadConfig {
        PostgresConnectionBulkheadConfig {
            worker_role: WORKER_ROLE.to_owned(),
            business_max_connections: self.database.business_max_connections,
            critical_control_reserved_connections: self
                .database
                .critical_control_reserved_connections,
            process_connection_budget: self.database.process_connection_budget,
            acquire_timeout: Duration::from_millis(self.database.acquire_timeout_milliseconds),
            statement_timeout: Duration::from_millis(self.database.statement_timeout_milliseconds),
            idle_timeout: Some(Duration::from_millis(
                self.database.idle_timeout_milliseconds,
            )),
            max_lifetime: Some(Duration::from_millis(
                self.database.max_lifetime_milliseconds,
            )),
        }
    }

    fn artifact_rpc_limits(&self) -> Result<ArtifactInternalRpcLimits, ProcessError> {
        ArtifactInternalRpcLimits::new(
            self.artifact.maximum_request_bytes,
            self.artifact.maximum_chunk_bytes,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)
    }

    fn runtime_config(&self) -> Result<ProductionOrchestrationConfig, ProcessError> {
        let profile = checked_in_hard_limit_profile();
        let milliseconds = |value| Duration::from_millis(value);
        let json_limits = JsonLimits {
            max_bytes: self.plan_maximum_bytes,
            max_depth: usize::try_from(profile.api.json_depth.hard_max)
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            max_properties_per_object: usize::try_from(profile.api.json_properties.hard_max)
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            max_items_per_array: usize::try_from(profile.api.json_items.hard_max)
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            max_string_bytes: self.plan_maximum_bytes,
        };
        let config = ProductionOrchestrationConfig {
            plan_materializer: SchedulerPlanMaterializerConfig {
                request_timeout: milliseconds(self.artifact.request_timeout_milliseconds),
                maximum_bytes: self.plan_maximum_bytes,
                json_limits,
                plan_limits: PlanLimits::from_profile(&profile)
                    .map_err(|_| ProcessError::InvalidConfiguration)?,
            },
            run_value_read_timeout: milliseconds(self.artifact.request_timeout_milliseconds),
            handoff_retry_delay: milliseconds(self.timing.handoff_retry_milliseconds),
            expression_limits: ExpressionLimits::from_profile(&profile)
                .map_err(|_| ProcessError::InvalidConfiguration)?,
            executor: OrchestrationExecutorConfig::from_profile(
                &profile,
                OrchestrationExecutorTiming {
                    heartbeat_jitter: milliseconds(self.timing.heartbeat_jitter_milliseconds),
                    store_retry_backoff: milliseconds(self.timing.store_retry_backoff_milliseconds),
                },
            )
            .map_err(|_| ProcessError::InvalidConfiguration)?,
            coordinator: OrchestrationCoordinatorConfig::from_profile(
                &profile,
                CoordinatorTiming {
                    coalesce_window: milliseconds(self.timing.coordinator_coalesce_milliseconds),
                    safety_scan_interval: milliseconds(self.timing.coordinator_scan_milliseconds),
                    safety_scan_jitter: milliseconds(
                        self.timing.coordinator_scan_jitter_milliseconds,
                    ),
                    claim_failure_backoff: milliseconds(
                        self.timing.claim_failure_backoff_milliseconds,
                    ),
                    drain_grace: milliseconds(self.timing.drain_grace_milliseconds),
                },
            )
            .map_err(|_| ProcessError::InvalidConfiguration)?,
            safety: OrchestrationSafetyConfig::from_profile(
                &profile,
                self.safety_shard,
                SafetyDriverTiming {
                    scan_interval: milliseconds(self.timing.safety_scan_milliseconds),
                    scan_jitter: milliseconds(self.timing.safety_scan_jitter_milliseconds),
                    failure_backoff: milliseconds(self.timing.safety_failure_backoff_milliseconds),
                },
            )
            .map_err(|_| ProcessError::InvalidConfiguration)?,
        };
        config
            .validate()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        Ok(config)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    run()
        .await
        .map_err(|failure| Box::new(failure) as Box<dyn Error>)
}

async fn run() -> Result<(), ProcessError> {
    let config = ProcessConfig::load()?;
    let profile = checked_in_hard_limit_profile();
    let database_url = required(DATABASE_URL_ENV)?;
    let bulkheads = PostgresConnectionBulkheads::connect(
        &database_url,
        config.database_bulkhead_config(),
        &profile,
    )
    .await
    .map_err(|_| ProcessError::DatabaseUnavailable)?;
    verify_schema(bulkheads.critical_control_pool())
        .await
        .map_err(|_| ProcessError::SchemaMismatch)?;
    let artifact_client = Arc::new(connect_artifact(&config).await?);
    let process_generation_id: ResourceId = format!(
        "{}_{}",
        ResourceKind::WorkerProcessGeneration.descriptor().prefix,
        Uuid::now_v7()
    )
    .parse()
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let pools = LocalWorkerPools::new(
        config.worker_manifest.clone(),
        process_generation_id.clone(),
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let observability_pools = pools.clone();
    let runtime = start_production_orchestration(
        bulkheads.business_repository(),
        bulkheads.critical_control_repository(),
        artifact_client,
        pools,
        config.runtime_config()?,
    )
    .map_err(|_| ProcessError::RuntimeFailed)?;
    let operational_metrics = Arc::new(OrchestrationOperationalMetrics::default());
    update_operational_metrics(&operational_metrics, &observability_pools, &runtime);
    let metrics = Arc::new(
        ProcessHttpMetrics::install_with_orchestration(
            "scheduler-recovery",
            PROCESS_OBSERVABILITY_OPERATIONS,
            Arc::clone(&operational_metrics),
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
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
    eprintln!("platform-orchestration-worker started");
    let mut health = tokio::time::interval(Duration::from_secs(1));
    health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            signal = &mut shutdown => {
                signal?;
                runtime.shutdown().await.map_err(|_| ProcessError::RuntimeFailed)?;
                let _ = http_shutdown.send(());
                http.await.map_err(|_| ProcessError::ObservabilityFailed)?
                    .map_err(|_| ProcessError::ObservabilityFailed)?;
                bulkheads.close().await;
                return Ok(());
            }
            result = &mut http => {
                result.map_err(|_| ProcessError::ObservabilityFailed)?
                    .map_err(|_| ProcessError::ObservabilityFailed)?;
                runtime.shutdown().await.map_err(|_| ProcessError::RuntimeFailed)?;
                bulkheads.close().await;
                return Err(ProcessError::ObservabilityFailed);
            }
            _ = health.tick() => {
                update_operational_metrics(&operational_metrics, &observability_pools, &runtime);
                match observe_durable_job_queue(bulkheads.critical_control_pool(), "orchestration").await {
                    Ok(snapshot) => operational_metrics.observe_durable_job_queue(DurableJobQueueSnapshot {
                        due_jobs: snapshot.due_jobs,
                        due_oldest_age_seconds: snapshot.due_oldest_age_seconds,
                        expired_leases: snapshot.expired_leases,
                        expired_oldest_lag_seconds: snapshot.expired_oldest_lag_seconds,
                    }),
                    Err(_) => operational_metrics.observe_database_failure(),
                }
                match observe_durable_outbox(bulkheads.critical_control_pool()).await {
                    Ok(snapshot) => operational_metrics.observe_durable_outbox(DurableOutboxSnapshot {
                        due_events: snapshot.due_events,
                        due_oldest_age_seconds: snapshot.due_oldest_age_seconds,
                        expired_claims: snapshot.expired_claims,
                        expired_oldest_lag_seconds: snapshot.expired_oldest_lag_seconds,
                        dead_events: snapshot.dead_events,
                    }),
                    Err(_) => operational_metrics.observe_database_failure(),
                }
                if runtime.is_finished() {
                    runtime.shutdown().await.map_err(|_| ProcessError::RuntimeFailed)?;
                    bulkheads.close().await;
                    return Err(ProcessError::RuntimeExitedUnexpectedly);
                }
            }
        }
    }
}

fn update_operational_metrics(
    metrics: &OrchestrationOperationalMetrics,
    pools: &LocalWorkerPools,
    runtime: &RunningProductionOrchestration,
) {
    let pool = pools.snapshot();
    let coordinator = runtime.coordinator.snapshot();
    let recovery = runtime.safety.snapshot();
    metrics.update(OrchestrationOperationalSnapshot {
        business_capacity: u64::try_from(pool.business_capacity).unwrap_or(u64::MAX),
        business_available: u64::try_from(pool.business_available).unwrap_or(u64::MAX),
        critical_control_capacity: u64::try_from(pool.critical_control_capacity)
            .unwrap_or(u64::MAX),
        critical_control_available: u64::try_from(pool.critical_control_available)
            .unwrap_or(u64::MAX),
        active_jobs: coordinator.active_jobs,
        jobs_claimed: coordinator.jobs_claimed,
        claim_failures: coordinator.claim_failures,
        recovery_scan_attempts: recovery.scan_attempts,
        recovery_scan_failures: recovery.scan_failures,
        recovery_capacity_skips: recovery.capacity_skips,
        recovery_mutations: recovery.mutations,
    });
}

async fn connect_artifact(
    config: &ProcessConfig,
) -> Result<ArtifactSchedulerGrpcClient, ProcessError> {
    let tls = ClientTlsConfig::new()
        .domain_name(config.artifact.tls_server_name.clone())
        .ca_certificate(Certificate::from_pem(read_bounded_file(
            &required_absolute_path(ARTIFACT_CA_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?))
        .identity(Identity::from_pem(
            read_bounded_file(
                &required_absolute_path(ARTIFACT_CERT_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
            read_bounded_file(
                &required_absolute_path(ARTIFACT_KEY_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
        ));
    let channel = Endpoint::from_shared(config.artifact.endpoint.clone())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(
            config.artifact.connect_timeout_milliseconds,
        ))
        .timeout(Duration::from_millis(
            config.artifact.request_timeout_milliseconds,
        ))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect()
        .await
        .map_err(|_| ProcessError::ArtifactUnavailable)?;
    Ok(ArtifactSchedulerGrpcClient::new(
        channel,
        config.artifact_rpc_limits()?,
    ))
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
        .ok_or(ProcessError::MissingConfiguration(name))
}

fn required_absolute_path(name: &'static str) -> Result<PathBuf, ProcessError> {
    let path = PathBuf::from(required(name)?);
    path.is_absolute()
        .then_some(path)
        .ok_or(ProcessError::InvalidConfiguration)
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
    file.take(u64::try_from(maximum.saturating_add(1)).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes)
        .map_err(|_| ProcessError::FileUnavailable)?;
    if bytes.is_empty() || bytes.len() > maximum {
        bytes.fill(0);
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessError {
    MissingConfiguration(&'static str),
    InvalidConfiguration,
    FileUnavailable,
    DatabaseUnavailable,
    SchemaMismatch,
    ArtifactUnavailable,
    SignalUnavailable,
    RuntimeFailed,
    RuntimeExitedUnexpectedly,
    ObservabilityFailed,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingConfiguration(name) => {
                write!(formatter, "required configuration {name} is missing")
            }
            Self::InvalidConfiguration => formatter.write_str("configuration is invalid"),
            Self::FileUnavailable => {
                formatter.write_str("configuration or TLS file is unavailable")
            }
            Self::DatabaseUnavailable => formatter.write_str("database is unavailable"),
            Self::SchemaMismatch => formatter.write_str("database schema does not match candidate"),
            Self::ArtifactUnavailable => {
                formatter.write_str("Artifact Scheduler service is unavailable")
            }
            Self::SignalUnavailable => formatter.write_str("shutdown signal is unavailable"),
            Self::RuntimeFailed => formatter.write_str("orchestration runtime failed"),
            Self::RuntimeExitedUnexpectedly => {
                formatter.write_str("orchestration runtime exited unexpectedly")
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

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn fixture() -> ProcessConfig {
        ProcessConfig {
            schema_version: 1,
            observability_listen_address: "127.0.0.1:9090".to_owned(),
            worker_manifest: WorkerManifest {
                manifest_version: WORKER_MANIFEST_VERSION,
                worker_role: WORKER_ROLE.to_owned(),
                work_class: WorkClass::Orchestration,
                adapter_runtime_digest: digest('a'),
                protocol_version: WORKER_PROTOCOL_VERSION,
                max_concurrency: 16,
                critical_control_reserved_slots: 2,
            },
            database: DatabaseConfig {
                business_max_connections: 8,
                critical_control_reserved_connections: 2,
                process_connection_budget: 10,
                acquire_timeout_milliseconds: 2_000,
                statement_timeout_milliseconds: 30_000,
                idle_timeout_milliseconds: 60_000,
                max_lifetime_milliseconds: 600_000,
            },
            artifact: ArtifactConfig {
                endpoint: "https://artifact-data.platform-artifact.svc:8443/".to_owned(),
                tls_server_name: "artifact-data.platform-artifact.svc".to_owned(),
                connect_timeout_milliseconds: 2_000,
                request_timeout_milliseconds: 5_000,
                maximum_request_bytes: 262_144,
                maximum_chunk_bytes: 262_144,
            },
            timing: TimingConfig {
                coordinator_coalesce_milliseconds: 5,
                coordinator_scan_milliseconds: 500,
                coordinator_scan_jitter_milliseconds: 50,
                claim_failure_backoff_milliseconds: 100,
                drain_grace_milliseconds: 30_000,
                heartbeat_jitter_milliseconds: 100,
                store_retry_backoff_milliseconds: 100,
                safety_scan_milliseconds: 500,
                safety_scan_jitter_milliseconds: 50,
                safety_failure_backoff_milliseconds: 100,
                handoff_retry_milliseconds: 100,
            },
            plan_maximum_bytes: 1_048_576,
            safety_shard: SafetyScanShard::whole(),
        }
    }

    #[test]
    fn production_config_is_closed_bounded_and_role_scoped() {
        let valid = fixture();
        assert!(valid
            .database_bulkhead_config()
            .validate(&checked_in_hard_limit_profile())
            .is_ok());
        assert!(valid.artifact_rpc_limits().is_ok());
        assert!(valid.runtime_config().is_ok());
        valid.validate().unwrap();
        let mut wrong = fixture();
        wrong.worker_manifest.work_class = WorkClass::Model;
        assert_eq!(wrong.validate(), Err(ProcessError::InvalidConfiguration));
        let mut insecure = fixture();
        insecure.artifact.endpoint = "http://artifact-data:8080/".to_owned();
        assert_eq!(insecure.validate(), Err(ProcessError::InvalidConfiguration));
        let mut collapsed = fixture();
        collapsed.database.critical_control_reserved_connections = 0;
        assert_eq!(
            collapsed.validate(),
            Err(ProcessError::InvalidConfiguration)
        );
        let mut unbounded = fixture();
        unbounded.plan_maximum_bytes = MAX_TYPED_PLAN_ARTIFACT_BYTES + 1;
        assert_eq!(
            unbounded.validate(),
            Err(ProcessError::InvalidConfiguration)
        );
    }
}
