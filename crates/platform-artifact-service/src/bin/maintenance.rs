use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_artifact_broker::{
    ArtifactBrokerLimits, AwsArtifactProviderCatalog, AwsArtifactProviderCatalogConfig,
    BrokeredArtifactDeletionBackend,
};
use insight_platform_artifacts::{
    ArtifactBackendFailure, ArtifactScanEvidence, ArtifactScanRequest, ArtifactScanner,
    ArtifactWorkerService,
};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_jobs::JobFence as DomainJobFence;
use insight_platform_observability::{
    process_observability_router, ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS,
};
use insight_platform_postgres::{
    artifact_repository::{ArtifactExecutionSlot, StartedArtifactExecution},
    repository::{
        ArtifactWorkerRole, ClaimArtifactJobs, JobFence as RepositoryJobFence, JobRecord,
        PgRepository,
    },
    verify_schema,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use sqlx::postgres::PgPoolOptions;
use std::{
    error::Error,
    fmt,
    future::IntoFuture,
    io::Read as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::watch;
use uuid::Uuid;

const CONFIG_PATH_ENV: &str = "PLATFORM_ARTIFACT_MAINTENANCE_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_ARTIFACT_MAINTENANCE_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_ARTIFACT_MAINTENANCE_DATABASE_URL";
const MAX_CONFIG_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceConfig {
    schema_version: u32,
    listen_address: String,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    artifact_provider_catalog: AwsArtifactProviderCatalogConfig,
    broker: BrokerConfig,
    worker: WorkerConfig,
    shutdown_grace_milliseconds: u64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrokerConfig {
    maximum_in_flight: usize,
    maximum_read_bytes: usize,
    operation_timeout_milliseconds: u64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerConfig {
    claim_batch: u16,
    lease_milliseconds: i64,
    receipt_ttl_milliseconds: i64,
    poll_milliseconds: u64,
}

impl MaintenanceConfig {
    fn load() -> Result<Self, MaintenanceError> {
        let bytes = read_bounded(&absolute_path(CONFIG_PATH_ENV)?, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 32,
                max_items_per_array: 128,
                max_properties_per_object: 128,
                max_string_bytes: 16_384,
            },
        )
        .map_err(|_| MaintenanceError::InvalidConfiguration)?;
        let expected: Sha256Digest = required(CONFIG_DIGEST_ENV)?
            .parse()
            .map_err(|_| MaintenanceError::InvalidConfiguration)?;
        if canonical_digest(&value).ok().as_deref() != Some(expected.as_str()) {
            return Err(MaintenanceError::InvalidConfiguration);
        }
        let config: Self =
            serde_json::from_value(value).map_err(|_| MaintenanceError::InvalidConfiguration)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), MaintenanceError> {
        let _: SocketAddr = self
            .listen_address
            .parse()
            .map_err(|_| MaintenanceError::InvalidConfiguration)?;
        self.artifact_provider_catalog
            .validate()
            .map_err(|_| MaintenanceError::InvalidConfiguration)?;
        self.broker_limits()?;
        if self.schema_version != 1
            || !(2..=64).contains(&self.database_max_connections)
            || !(1..=30_000).contains(&self.database_acquire_timeout_milliseconds)
            || self.worker.claim_batch == 0
            || self.worker.claim_batch > 32
            || !(1_000..=120_000).contains(&self.worker.lease_milliseconds)
            || !(1_000..=3_600_000).contains(&self.worker.receipt_ttl_milliseconds)
            || !(10..=60_000).contains(&self.worker.poll_milliseconds)
            || !(1..=120_000).contains(&self.shutdown_grace_milliseconds)
        {
            return Err(MaintenanceError::InvalidConfiguration);
        }
        Ok(())
    }

    fn broker_limits(&self) -> Result<ArtifactBrokerLimits, MaintenanceError> {
        let limits = ArtifactBrokerLimits {
            maximum_in_flight: self.broker.maximum_in_flight,
            maximum_read_bytes: self.broker.maximum_read_bytes,
            operation_timeout: Duration::from_millis(self.broker.operation_timeout_milliseconds),
        };
        limits
            .validate()
            .map_err(|_| MaintenanceError::InvalidConfiguration)?;
        Ok(limits)
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-artifact-maintenance failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), MaintenanceError> {
    let config = MaintenanceConfig::load()?;
    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(Duration::from_millis(
            config.database_acquire_timeout_milliseconds,
        ))
        .connect(&required(DATABASE_URL_ENV)?)
        .await
        .map_err(|_| MaintenanceError::DatabaseUnavailable)?;
    verify_schema(&pool)
        .await
        .map_err(|_| MaintenanceError::SchemaMismatch)?;
    let repository = Arc::new(PgRepository::new(pool));
    let broker_limits = config.broker_limits()?;
    let providers = AwsArtifactProviderCatalog::install(config.artifact_provider_catalog)
        .await
        .map_err(|_| MaintenanceError::InvalidConfiguration)?;
    providers
        .check_readiness()
        .await
        .map_err(|_| MaintenanceError::ProviderUnavailable)?;
    let (unsealer, stores) = providers.into_components();
    let backend =
        BrokeredArtifactDeletionBackend::new(repository.clone(), unsealer, stores, broker_limits)
            .map_err(|_| MaintenanceError::InvalidConfiguration)?;
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let worker = run_worker(
        Arc::clone(&repository),
        backend,
        config.worker,
        shutdown_receiver,
    );
    let address: SocketAddr = config
        .listen_address
        .parse()
        .map_err(|_| MaintenanceError::InvalidConfiguration)?;
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|_| MaintenanceError::HttpUnavailable)?;
    let metrics = Arc::new(
        ProcessHttpMetrics::install("artifact-maintenance", PROCESS_OBSERVABILITY_OPERATIONS)
            .map_err(|_| MaintenanceError::InvalidConfiguration)?,
    );
    let server = axum::serve(listener, process_observability_router(Arc::clone(&metrics)))
        .with_graceful_shutdown({
            let mut receiver = shutdown_sender.subscribe();
            async move {
                let _ = receiver.wait_for(|value| *value).await;
            }
        })
        .into_future();
    tokio::pin!(worker);
    tokio::pin!(server);
    metrics.mark_ready();
    tokio::select! {
        result = &mut worker => result.map_err(|_| MaintenanceError::WorkerUnavailable),
        result = &mut server => result.map_err(|_| MaintenanceError::HttpUnavailable),
        signal = shutdown_signal() => {
            signal?;
            let _ = shutdown_sender.send(true);
            tokio::time::timeout(
                Duration::from_millis(config.shutdown_grace_milliseconds),
                async {
                    let (worker, server) = tokio::join!(&mut worker, &mut server);
                    worker.map_err(|_| MaintenanceError::WorkerUnavailable)?;
                    server.map_err(|_| MaintenanceError::HttpUnavailable)
                },
            )
            .await
            .map_err(|_| MaintenanceError::ShutdownDeadlineExceeded)?
        }
    }
}

#[derive(Clone, Copy)]
struct ForbiddenScanner;

impl ArtifactScanner for ForbiddenScanner {
    async fn scan(
        &self,
        _request: ArtifactScanRequest,
    ) -> Result<ArtifactScanEvidence, ArtifactBackendFailure> {
        Err(ArtifactBackendFailure {
            retryable: false,
            reason_class: "maintenance_scan_forbidden".to_owned(),
        })
    }
}

async fn run_worker(
    repository: Arc<PgRepository>,
    backend: BrokeredArtifactDeletionBackend,
    config: WorkerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let worker_id = new_id(ResourceKind::WorkerProcessGeneration)?;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let tokens = (0..config.claim_batch)
            .map(|_| fresh_digest("artifact-maintenance-lease"))
            .collect::<Result<Vec<_>, _>>()?;
        let claimed = repository
            .claim_artifact_jobs(ClaimArtifactJobs {
                role: ArtifactWorkerRole::Maintenance,
                worker_id: worker_id.clone(),
                limit: config.claim_batch,
                lease_milliseconds: config.lease_milliseconds,
                lease_token_digests: tokens,
            })
            .await
            .map_err(|_| "Artifact Maintenance claim failed".to_owned())?;
        let mut attempts = tokio::task::JoinSet::new();
        for job in claimed {
            let repository = Arc::clone(&repository);
            let backend = backend.clone();
            let worker_id = worker_id.clone();
            attempts.spawn(async move {
                execute_claimed(repository, backend, config, worker_id, job).await
            });
        }
        while let Some(result) = attempts.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(reason)) => eprintln!("Artifact Maintenance attempt ended: {reason}"),
                Err(_) => eprintln!("Artifact Maintenance attempt task failed"),
            }
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
            }
            _ = tokio::time::sleep(Duration::from_millis(config.poll_milliseconds)) => {}
        }
    }
}

async fn execute_claimed(
    repository: Arc<PgRepository>,
    backend: BrokeredArtifactDeletionBackend,
    config: WorkerConfig,
    worker_id: ResourceId,
    claimed: JobRecord,
) -> Result<(), String> {
    let token: Sha256Digest = claimed
        .lease_token_digest
        .as_deref()
        .ok_or_else(|| "claimed Artifact Job has no token".to_owned())?
        .parse()
        .map_err(|_| "claimed Artifact Job token is invalid".to_owned())?;
    let started = repository
        .start_job(RepositoryJobFence {
            tenant_id: claimed.tenant_id.clone(),
            job_id: claimed.job_id.clone(),
            worker_id: worker_id.clone(),
            lease_epoch: claimed.lease_epoch,
            expected_job_version: claimed.version,
            lease_token_digest: token.clone(),
        })
        .await
        .map_err(|_| "Artifact Maintenance start lost its fence".to_owned())?;
    let fence = DomainJobFence {
        expected_version: u64::try_from(started.version)
            .map_err(|_| "Artifact Maintenance Job version is corrupt".to_owned())?,
        worker_process_generation_id: worker_id,
        lease_generation: u64::try_from(started.lease_epoch)
            .map_err(|_| "Artifact Maintenance lease is corrupt".to_owned())?,
        token_digest: token,
    };
    let expiry = Utc::now()
        .checked_add_signed(ChronoDuration::milliseconds(
            config.receipt_ttl_milliseconds,
        ))
        .ok_or_else(|| "Artifact Maintenance receipt expiry overflowed".to_owned())?
        .min(started.deadline);
    let execution = repository
        .load_started_artifact_execution(
            ArtifactWorkerRole::Maintenance,
            started
                .tenant_id
                .parse()
                .map_err(|_| "Artifact Maintenance tenant is corrupt".to_owned())?,
            started
                .job_id
                .parse()
                .map_err(|_| "Artifact Maintenance Job is corrupt".to_owned())?,
            fence,
            ArtifactExecutionSlot {
                receipt_id: new_id(ResourceKind::Receipt)?,
                event_id: new_id(ResourceKind::Event)?,
                outbox_id: new_id(ResourceKind::OutboxEvent)?,
                duplicate_blob_cleanup_job_id: new_id(ResourceKind::Job)?,
                receipt_expires_at: expiry,
            },
        )
        .await
        .map_err(|_| "Artifact Maintenance authority changed".to_owned())?;
    let service = ArtifactWorkerService::new(ForbiddenScanner, backend, (*repository).clone());
    match execution {
        StartedArtifactExecution::Deletion(execution) => service
            .execute_deletion(execution, Utc::now())
            .await
            .map(|_| ())
            .map_err(|_| "Artifact deletion did not commit".to_owned()),
        StartedArtifactExecution::BlobCleanup(execution) => service
            .execute_blob_cleanup(execution, Utc::now())
            .await
            .map(|_| ())
            .map_err(|_| "Artifact Blob cleanup did not commit".to_owned()),
        StartedArtifactExecution::Scan(_) => {
            Err("Maintenance received scan Artifact work".to_owned())
        }
    }
}

fn new_id(kind: ResourceKind) -> Result<ResourceId, String> {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7())
        .map_err(|_| "Artifact Maintenance identity generation failed".to_owned())
}

fn fresh_digest(domain: &str) -> Result<Sha256Digest, String> {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update(Uuid::new_v4().as_bytes());
    let value = hasher.finalize();
    format!("sha256:{}", lower_hex(&value))
        .parse()
        .map_err(|_| "Artifact Maintenance token generation failed".to_owned())
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

async fn shutdown_signal() -> Result<(), MaintenanceError> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|_| MaintenanceError::SignalUnavailable)?;
        tokio::select! {
            received = terminate.recv() => received.ok_or(MaintenanceError::SignalUnavailable),
            received = tokio::signal::ctrl_c() => received.map_err(|_| MaintenanceError::SignalUnavailable),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .map_err(|_| MaintenanceError::SignalUnavailable)
}

fn required(name: &'static str) -> Result<String, MaintenanceError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty() && value.len() <= 16_384)
        .ok_or(MaintenanceError::MissingConfiguration(name))
}

fn absolute_path(name: &'static str) -> Result<PathBuf, MaintenanceError> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() || path == Path::new("/") {
        return Err(MaintenanceError::InvalidConfiguration);
    }
    Ok(path)
}

fn read_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>, MaintenanceError> {
    let file = std::fs::File::open(path).map_err(|_| MaintenanceError::FileUnavailable)?;
    let metadata = file
        .metadata()
        .map_err(|_| MaintenanceError::FileUnavailable)?;
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > u64::try_from(maximum).unwrap_or(u64::MAX)
    {
        return Err(MaintenanceError::FileUnavailable);
    }
    let mut bytes = Vec::new();
    file.take(u64::try_from(maximum).unwrap_or(u64::MAX) + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| MaintenanceError::FileUnavailable)?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(MaintenanceError::FileUnavailable);
    }
    Ok(bytes)
}

#[derive(Debug)]
enum MaintenanceError {
    MissingConfiguration(&'static str),
    InvalidConfiguration,
    FileUnavailable,
    DatabaseUnavailable,
    SchemaMismatch,
    ProviderUnavailable,
    HttpUnavailable,
    WorkerUnavailable,
    SignalUnavailable,
    ShutdownDeadlineExceeded,
}

impl fmt::Display for MaintenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingConfiguration(name) => write!(formatter, "missing configuration {name}"),
            Self::InvalidConfiguration => formatter.write_str("invalid configuration"),
            Self::FileUnavailable => formatter.write_str("configuration file unavailable"),
            Self::DatabaseUnavailable => formatter.write_str("database unavailable"),
            Self::SchemaMismatch => formatter.write_str("database schema mismatch"),
            Self::ProviderUnavailable => formatter.write_str("Artifact provider unavailable"),
            Self::HttpUnavailable => formatter.write_str("Artifact Maintenance HTTP unavailable"),
            Self::WorkerUnavailable => {
                formatter.write_str("Artifact Maintenance worker unavailable")
            }
            Self::SignalUnavailable => formatter.write_str("shutdown signal unavailable"),
            Self::ShutdownDeadlineExceeded => formatter.write_str("shutdown deadline exceeded"),
        }
    }
}

impl Error for MaintenanceError {}
