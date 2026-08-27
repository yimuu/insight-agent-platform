//! Production MCP discovery driver composition.

pub mod subscription_driver;

use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    canonical_digest, ResourceId, ResourceKind, Sha256Digest, WorkClass,
};
use insight_platform_jobs::JobFence;
use insight_platform_mcp_host::{
    ExecuteMcpDiscoveryJob, ExpiredMcpDiscoveryJobObservation, McpDiscoveryContractQuery,
    McpDiscoveryWorker, McpDiscoveryWorkerError, McpJobPayload, McpWorkerAudit,
    RecoverExpiredMcpDiscoveryJob,
};
use insight_platform_postgres::repository::{
    ClaimJobs, HeartbeatJob, JobFence as RepositoryJobFence, JobRecord, PgRepository,
    RepositoryError,
};
use serde_json::{json, Value};
use std::{error::Error, fmt, sync::Arc, time::Duration};
use tokio::{sync::Semaphore, task::JoinSet, time::Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const MCP_DISCOVERY_MAX_BATCH: u16 = 64;

#[derive(Debug, Clone, Copy)]
pub struct McpDiscoveryDriverTiming {
    pub scan_interval: Duration,
    pub failure_backoff: Duration,
    pub heartbeat_interval: Duration,
    pub retry_backoff: Duration,
    pub receipt_ttl: Duration,
    pub drain_grace: Duration,
}

#[derive(Debug, Clone, Copy)]
pub struct McpDiscoveryDriverConfig {
    pub claim_batch_size: u16,
    pub recovery_batch_size: u16,
    pub maximum_concurrency: usize,
    pub lease_milliseconds: u64,
    pub timing: McpDiscoveryDriverTiming,
}

impl McpDiscoveryDriverConfig {
    pub fn validate(self) -> Result<(), McpDiscoveryDriverError> {
        if self.claim_batch_size == 0
            || self.claim_batch_size > MCP_DISCOVERY_MAX_BATCH
            || self.recovery_batch_size == 0
            || self.recovery_batch_size > MCP_DISCOVERY_MAX_BATCH
            || self.maximum_concurrency == 0
            || self.maximum_concurrency > usize::from(MCP_DISCOVERY_MAX_BATCH)
            || self.lease_milliseconds < 3
            || self.timing.scan_interval.is_zero()
            || self.timing.failure_backoff.is_zero()
            || self.timing.heartbeat_interval.is_zero()
            || self.timing.heartbeat_interval >= Duration::from_millis(self.lease_milliseconds / 3)
            || self.timing.retry_backoff.is_zero()
            || self.timing.receipt_ttl <= Duration::from_millis(self.lease_milliseconds)
            || self.timing.drain_grace.is_zero()
            || ChronoDuration::from_std(self.timing.retry_backoff).is_err()
            || ChronoDuration::from_std(self.timing.receipt_ttl).is_err()
        {
            return Err(McpDiscoveryDriverError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum McpDiscoveryDriverError {
    InvalidConfiguration,
    InvalidGeneratedIdentity,
    CorruptClaim,
    Repository(RepositoryError),
    Worker(McpDiscoveryWorkerError),
    DrainRequiresTermination,
}

impl fmt::Display for McpDiscoveryDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => {
                formatter.write_str("MCP discovery driver configuration is invalid")
            }
            Self::InvalidGeneratedIdentity => {
                formatter.write_str("MCP discovery driver generated an invalid identity")
            }
            Self::CorruptClaim => {
                formatter.write_str("MCP discovery authority returned a corrupt claim")
            }
            Self::Repository(failure) => {
                write!(formatter, "MCP discovery repository failed: {failure}")
            }
            Self::Worker(failure) => write!(formatter, "MCP discovery execution failed: {failure}"),
            Self::DrainRequiresTermination => {
                formatter.write_str("MCP discovery drain requires process termination")
            }
        }
    }
}

impl Error for McpDiscoveryDriverError {}

impl From<RepositoryError> for McpDiscoveryDriverError {
    fn from(failure: RepositoryError) -> Self {
        Self::Repository(failure)
    }
}

impl From<McpDiscoveryWorkerError> for McpDiscoveryDriverError {
    fn from(failure: McpDiscoveryWorkerError) -> Self {
        Self::Worker(failure)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct McpDiscoveryDriverReport {
    pub claimed: u64,
    pub recovered: u64,
    pub settled: u64,
    pub abandoned: u64,
}

pub struct McpDiscoveryDriver {
    repository: Arc<PgRepository>,
    worker: Arc<McpDiscoveryWorker>,
    worker_process_generation_id: ResourceId,
    permits: Arc<Semaphore>,
    config: McpDiscoveryDriverConfig,
}

#[derive(Clone)]
pub struct McpDiscoveryCapacity {
    permits: Arc<Semaphore>,
    maximum: usize,
}

impl McpDiscoveryCapacity {
    pub fn maximum(&self) -> usize {
        self.maximum
    }

    pub fn available(&self) -> usize {
        self.permits.available_permits()
    }
}

impl McpDiscoveryDriver {
    pub fn new(
        repository: Arc<PgRepository>,
        worker: Arc<McpDiscoveryWorker>,
        worker_process_generation_id: ResourceId,
        config: McpDiscoveryDriverConfig,
    ) -> Result<Self, McpDiscoveryDriverError> {
        config.validate()?;
        if worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration {
            return Err(McpDiscoveryDriverError::InvalidConfiguration);
        }
        Ok(Self {
            repository,
            worker,
            worker_process_generation_id,
            permits: Arc::new(Semaphore::new(config.maximum_concurrency)),
            config,
        })
    }

    pub fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }

    pub fn capacity(&self) -> McpDiscoveryCapacity {
        McpDiscoveryCapacity {
            permits: Arc::clone(&self.permits),
            maximum: self.config.maximum_concurrency,
        }
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<bool>,
    ) -> Result<usize, McpDiscoveryDriverError> {
        let requested =
            usize::from(self.config.claim_batch_size).min(self.permits.available_permits());
        if requested == 0 {
            return Ok(0);
        }
        let mut reservations = Vec::with_capacity(requested);
        for _ in 0..requested {
            let Ok(permit) = Arc::clone(&self.permits).try_acquire_owned() else {
                break;
            };
            reservations.push(permit);
        }
        if reservations.is_empty() {
            return Ok(0);
        }
        let tokens = (0..reservations.len())
            .map(|_| new_digest())
            .collect::<Result<Vec<_>, _>>()?;
        let claims = self
            .repository
            .claim_mcp_discovery_jobs(ClaimJobs {
                work_class: WorkClass::Mcp.as_str().to_owned(),
                worker_id: self.worker_process_generation_id.clone(),
                limit: u16::try_from(reservations.len())
                    .map_err(|_| McpDiscoveryDriverError::InvalidConfiguration)?,
                lease_milliseconds: i64::try_from(self.config.lease_milliseconds)
                    .map_err(|_| McpDiscoveryDriverError::InvalidConfiguration)?,
                lease_token_digests: tokens,
            })
            .await?;
        if claims.len() > reservations.len() {
            return Err(McpDiscoveryDriverError::CorruptClaim);
        }
        let count = claims.len();
        for (claim, permit) in claims.into_iter().zip(reservations.drain(..)) {
            let repository = Arc::clone(&self.repository);
            let worker = Arc::clone(&self.worker);
            let config = self.config;
            active.spawn(async move {
                let _permit = permit;
                match execute_claim(repository, worker, config, claim).await {
                    Ok(()) => true,
                    Err(failure) => {
                        eprintln!("platform-mcp-discovery-worker abandoned execution: {failure}");
                        false
                    }
                }
            });
        }
        Ok(count)
    }

    pub async fn recover_once(&self) -> Result<usize, McpDiscoveryDriverError> {
        let observations = self
            .repository
            .list_expired_mcp_discovery_jobs(self.config.recovery_batch_size)
            .await?;
        let mut recovered = 0usize;
        for observation in observations {
            let Some(command) = recovery_command(&observation, self.config.timing.retry_backoff)?
            else {
                continue;
            };
            match self
                .repository
                .recover_expired_mcp_discovery_job(command)
                .await
            {
                Ok(_) => recovered = recovered.saturating_add(1),
                Err(
                    RepositoryError::Conflict(_)
                    | RepositoryError::StaleFence
                    | RepositoryError::LeaseExpired
                    | RepositoryError::NotFound(_),
                ) => {}
                Err(failure) => return Err(failure.into()),
            }
        }
        Ok(recovered)
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<McpDiscoveryDriverReport, McpDiscoveryDriverError> {
        let mut active = JoinSet::new();
        let mut report = McpDiscoveryDriverReport::default();
        let mut scan = tokio::time::interval(self.config.timing.scan_interval);
        scan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                joined = active.join_next(), if !active.is_empty() => observe_join(joined, &mut report),
                _ = scan.tick() => {
                    match self.recover_once().await {
                        Ok(recovered) => report.recovered = report.recovered.saturating_add(recovered as u64),
                        Err(McpDiscoveryDriverError::Repository(RepositoryError::Database(_))) => {},
                        Err(failure) => return Err(failure),
                    }
                    match self.drive_once(&mut active).await {
                        Ok(claimed) => report.claimed = report.claimed.saturating_add(claimed as u64),
                        Err(McpDiscoveryDriverError::Repository(RepositoryError::Database(_))) => {
                            tokio::select! {
                                _ = cancellation.cancelled() => break,
                                _ = tokio::time::sleep(self.config.timing.failure_backoff) => {},
                            }
                        }
                        Err(failure) => return Err(failure),
                    }
                }
            }
        }
        let drain = async {
            while let Some(joined) = active.join_next().await {
                observe_join(Some(joined), &mut report);
            }
        };
        tokio::time::timeout(self.config.timing.drain_grace, drain)
            .await
            .map_err(|_| McpDiscoveryDriverError::DrainRequiresTermination)?;
        Ok(report)
    }
}

fn observe_join(
    joined: Option<Result<bool, tokio::task::JoinError>>,
    report: &mut McpDiscoveryDriverReport,
) {
    match joined {
        Some(Ok(true)) => report.settled = report.settled.saturating_add(1),
        Some(Ok(false) | Err(_)) => report.abandoned = report.abandoned.saturating_add(1),
        None => {}
    }
}

async fn execute_claim(
    repository: Arc<PgRepository>,
    worker: Arc<McpDiscoveryWorker>,
    config: McpDiscoveryDriverConfig,
    claim: JobRecord,
) -> Result<(), McpDiscoveryDriverError> {
    let tenant_id = parse_id(&claim.tenant_id, ResourceKind::Tenant)?;
    let job_id = parse_id(&claim.job_id, ResourceKind::Job)?;
    let operation_id = parse_id(&claim.owner_id, ResourceKind::McpOperation)?;
    let worker_id = parse_id(
        claim
            .worker_id
            .as_deref()
            .ok_or(McpDiscoveryDriverError::CorruptClaim)?,
        ResourceKind::WorkerProcessGeneration,
    )?;
    let token: Sha256Digest = claim
        .lease_token_digest
        .as_deref()
        .ok_or(McpDiscoveryDriverError::CorruptClaim)?
        .parse()
        .map_err(|_| McpDiscoveryDriverError::CorruptClaim)?;
    if claim.job_kind != "mcp_discovery"
        || claim.work_class != WorkClass::Mcp.as_str()
        || claim.owner_kind != "mcp_operation"
        || claim.invocation_id.as_deref() != Some(claim.owner_id.as_str())
        || claim.state != "leased"
        || claim.lease_epoch <= 0
        || claim.version <= 0
        || worker_id != claim_worker_id(&claim)?
    {
        return Err(McpDiscoveryDriverError::CorruptClaim);
    }
    let payload = discovery_payload(&claim)?;
    if payload.operation_id != operation_id {
        return Err(McpDiscoveryDriverError::CorruptClaim);
    }
    let started = repository
        .start_job(RepositoryJobFence {
            tenant_id: claim.tenant_id.clone(),
            job_id: claim.job_id.clone(),
            worker_id: worker_id.clone(),
            lease_epoch: claim.lease_epoch,
            expected_job_version: claim.version,
            lease_token_digest: token.clone(),
        })
        .await?;
    if started.state != "running"
        || started.attempt_no <= 0
        || started.version <= claim.version
        || started.lease_epoch != claim.lease_epoch
        || started.worker_id != claim.worker_id
        || started.lease_token_digest != claim.lease_token_digest
    {
        return Err(McpDiscoveryDriverError::CorruptClaim);
    }
    let mut fence = JobFence {
        expected_version: u64::try_from(started.version)
            .map_err(|_| McpDiscoveryDriverError::CorruptClaim)?,
        worker_process_generation_id: worker_id.clone(),
        lease_generation: u64::try_from(started.lease_epoch)
            .map_err(|_| McpDiscoveryDriverError::CorruptClaim)?,
        token_digest: token.clone(),
    };
    let initial_version = fence.expected_version;
    let now = Utc::now();
    let request_digest = attempt_digest(
        &tenant_id,
        &operation_id,
        &job_id,
        &worker_id,
        fence.lease_generation,
        u32::try_from(started.attempt_no).map_err(|_| McpDiscoveryDriverError::CorruptClaim)?,
        &payload.admission_digest,
    )?;
    let command = ExecuteMcpDiscoveryJob {
        query: McpDiscoveryContractQuery {
            schema_version: 1,
            tenant_id: tenant_id.clone(),
            operation_id,
            job_id: job_id.clone(),
            fence: fence.clone(),
        },
        audit: McpWorkerAudit {
            tenant_id: tenant_id.clone(),
            worker_process_generation_id: worker_id.clone(),
            receipt_id: new_id(ResourceKind::Receipt)?,
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: request_digest.clone(),
            request_digest,
            receipt_expires_at: now
                .checked_add_signed(
                    ChronoDuration::from_std(config.timing.receipt_ttl)
                        .map_err(|_| McpDiscoveryDriverError::InvalidConfiguration)?,
                )
                .ok_or(McpDiscoveryDriverError::InvalidGeneratedIdentity)?,
        },
        retry_at: now
            .checked_add_signed(
                ChronoDuration::from_std(config.timing.retry_backoff)
                    .map_err(|_| McpDiscoveryDriverError::InvalidConfiguration)?,
            )
            .ok_or(McpDiscoveryDriverError::InvalidGeneratedIdentity)?,
    };
    let prepare = worker.prepare(command);
    tokio::pin!(prepare);
    let mut heartbeat = tokio::time::interval_at(
        Instant::now()
            .checked_add(config.timing.heartbeat_interval)
            .unwrap_or_else(Instant::now),
        config.timing.heartbeat_interval,
    );
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut prepared = loop {
        tokio::select! {
            result = &mut prepare => break result?,
            _ = heartbeat.tick() => {
                match repository.heartbeat_job(HeartbeatJob {
                    fence: RepositoryJobFence {
                        tenant_id: tenant_id.to_string(),
                        job_id: job_id.to_string(),
                        worker_id: worker_id.clone(),
                        lease_epoch: i64::try_from(fence.lease_generation)
                            .map_err(|_| McpDiscoveryDriverError::CorruptClaim)?,
                        expected_job_version: i64::try_from(fence.expected_version)
                            .map_err(|_| McpDiscoveryDriverError::CorruptClaim)?,
                        lease_token_digest: token.clone(),
                    },
                    lease_milliseconds: i64::try_from(config.lease_milliseconds)
                        .map_err(|_| McpDiscoveryDriverError::InvalidConfiguration)?,
                }).await {
                    Ok(record) => fence.expected_version = u64::try_from(record.version)
                        .map_err(|_| McpDiscoveryDriverError::CorruptClaim)?,
                    Err(RepositoryError::Database(_)) => {},
                    Err(failure) => return Err(failure.into()),
                }
            }
        }
    };
    if fence.expected_version > initial_version {
        prepared.refresh_fence(fence)?;
    }
    worker.commit(prepared).await?;
    Ok(())
}

fn recovery_command(
    observation: &ExpiredMcpDiscoveryJobObservation,
    retry_backoff: Duration,
) -> Result<Option<RecoverExpiredMcpDiscoveryJob>, McpDiscoveryDriverError> {
    observation
        .validate()
        .map_err(|_| McpDiscoveryDriverError::CorruptClaim)?;
    let retry_at = if observation.job_state == insight_platform_contracts::JobState::Running
        && observation.physical_attempt < observation.attempt_limit
    {
        let candidate = observation
            .observed_at
            .checked_add_signed(
                ChronoDuration::from_std(retry_backoff)
                    .map_err(|_| McpDiscoveryDriverError::InvalidConfiguration)?,
            )
            .ok_or(McpDiscoveryDriverError::InvalidGeneratedIdentity)?;
        if candidate >= observation.deadline {
            return Ok(None);
        }
        Some(candidate)
    } else {
        None
    };
    Ok(Some(RecoverExpiredMcpDiscoveryJob {
        tenant_id: observation.tenant_id.clone(),
        operation_id: observation.operation_id.clone(),
        job_id: observation.job_id.clone(),
        observed_operation_version: observation.operation_version,
        observed_job_version: observation.job_version,
        observed_lease_generation: observation.lease_generation,
        retry_at,
        event_id: new_id(ResourceKind::Event)?,
        outbox_id: new_id(ResourceKind::OutboxEvent)?,
    }))
}

fn discovery_payload(
    claim: &JobRecord,
) -> Result<insight_platform_mcp_host::McpDiscoveryJobPayload, McpDiscoveryDriverError> {
    let mut value = claim.payload.value.clone();
    let Value::Object(object) = &mut value else {
        return Err(McpDiscoveryDriverError::CorruptClaim);
    };
    object.remove("schema_version");
    let payload: McpJobPayload =
        serde_json::from_value(value).map_err(|_| McpDiscoveryDriverError::CorruptClaim)?;
    match payload {
        McpJobPayload::Discovery(payload) => {
            payload
                .validate()
                .map_err(|_| McpDiscoveryDriverError::CorruptClaim)?;
            Ok(payload)
        }
        McpJobPayload::Subscription(_) => Err(McpDiscoveryDriverError::CorruptClaim),
    }
}

fn claim_worker_id(claim: &JobRecord) -> Result<ResourceId, McpDiscoveryDriverError> {
    parse_id(
        claim
            .worker_id
            .as_deref()
            .ok_or(McpDiscoveryDriverError::CorruptClaim)?,
        ResourceKind::WorkerProcessGeneration,
    )
}

fn attempt_digest(
    tenant_id: &ResourceId,
    operation_id: &ResourceId,
    job_id: &ResourceId,
    worker_id: &ResourceId,
    lease_generation: u64,
    physical_attempt: u32,
    admission_digest: &Sha256Digest,
) -> Result<Sha256Digest, McpDiscoveryDriverError> {
    canonical_digest(&json!({
        "schema_version": 1,
        "operation": "mcp.discovery.execute",
        "tenant_id": tenant_id,
        "operation_id": operation_id,
        "job_id": job_id,
        "worker_process_generation_id": worker_id,
        "lease_generation": lease_generation,
        "physical_attempt": physical_attempt,
        "admission_digest": admission_digest,
    }))
    .map_err(|_| McpDiscoveryDriverError::InvalidGeneratedIdentity)?
    .parse()
    .map_err(|_| McpDiscoveryDriverError::InvalidGeneratedIdentity)
}

fn new_id(kind: ResourceKind) -> Result<ResourceId, McpDiscoveryDriverError> {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7())
        .map_err(|_| McpDiscoveryDriverError::InvalidGeneratedIdentity)
}

fn new_digest() -> Result<Sha256Digest, McpDiscoveryDriverError> {
    canonical_digest(&json!({"nonce": Uuid::now_v7().to_string()}))
        .map_err(|_| McpDiscoveryDriverError::InvalidGeneratedIdentity)?
        .parse()
        .map_err(|_| McpDiscoveryDriverError::InvalidGeneratedIdentity)
}

fn parse_id(value: &str, kind: ResourceKind) -> Result<ResourceId, McpDiscoveryDriverError> {
    let id: ResourceId = value
        .parse()
        .map_err(|_| McpDiscoveryDriverError::CorruptClaim)?;
    if id.kind() != kind {
        return Err(McpDiscoveryDriverError::CorruptClaim);
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use insight_platform_contracts::JobState;

    fn id(kind: ResourceKind, _suffix: u128) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    #[test]
    fn driver_config_rejects_heartbeat_that_can_overtake_the_lease() {
        let config = McpDiscoveryDriverConfig {
            claim_batch_size: 1,
            recovery_batch_size: 1,
            maximum_concurrency: 1,
            lease_milliseconds: 300,
            timing: McpDiscoveryDriverTiming {
                scan_interval: Duration::from_millis(10),
                failure_backoff: Duration::from_millis(10),
                heartbeat_interval: Duration::from_millis(100),
                retry_backoff: Duration::from_millis(10),
                receipt_ttl: Duration::from_secs(1),
                drain_grace: Duration::from_secs(1),
            },
        };
        assert!(matches!(
            config.validate(),
            Err(McpDiscoveryDriverError::InvalidConfiguration)
        ));
    }

    #[test]
    fn recovery_command_distinguishes_unstarted_retryable_and_exhausted_jobs() {
        let now = Utc.timestamp_opt(1_800_000_000, 0).unwrap();
        let base = ExpiredMcpDiscoveryJobObservation {
            tenant_id: id(ResourceKind::Tenant, 1),
            operation_id: id(ResourceKind::McpOperation, 2),
            job_id: id(ResourceKind::Job, 3),
            operation_version: 2,
            job_version: 3,
            lease_generation: 1,
            physical_attempt: 0,
            attempt_limit: 2,
            job_state: JobState::Leased,
            lease_expires_at: now - ChronoDuration::seconds(1),
            deadline: now + ChronoDuration::minutes(1),
            observed_at: now,
        };
        assert!(recovery_command(&base, Duration::from_secs(1))
            .unwrap()
            .unwrap()
            .retry_at
            .is_none());
        let retryable = ExpiredMcpDiscoveryJobObservation {
            physical_attempt: 1,
            job_state: JobState::Running,
            ..base.clone()
        };
        assert!(recovery_command(&retryable, Duration::from_secs(1))
            .unwrap()
            .unwrap()
            .retry_at
            .is_some());
        let exhausted = ExpiredMcpDiscoveryJobObservation {
            physical_attempt: 2,
            ..retryable.clone()
        };
        assert!(recovery_command(&exhausted, Duration::from_secs(1))
            .unwrap()
            .unwrap()
            .retry_at
            .is_none());
        let too_late = ExpiredMcpDiscoveryJobObservation {
            deadline: now + ChronoDuration::milliseconds(500),
            ..retryable
        };
        assert!(recovery_command(&too_late, Duration::from_secs(1))
            .unwrap()
            .is_none());
    }
}
