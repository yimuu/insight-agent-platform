use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    canonical_digest, ResourceId, ResourceKind, Sha256Digest, TraceFlags, WorkClass,
};
use insight_platform_jobs::JobFence;
use insight_platform_mcp_host::{
    ExecuteMcpSubscriptionJob, McpJobPayload, McpSubscriptionContractQuery,
    McpSubscriptionRemoteIoLease, McpSubscriptionWorker, McpSubscriptionWorkerAudit,
    McpSubscriptionWorkerAudits, McpSubscriptionWorkerError, RecoverDueMcpSubscription,
    WakeMcpSubscriptionReconcile, MAX_MCP_SUBSCRIPTION_RECONCILE_SCAN,
    MIN_MCP_SUBSCRIPTION_RECONCILE_IDLE_MILLISECONDS,
};
use insight_platform_postgres::repository::{
    ClaimJobs, HeartbeatJob, JobFence as RepositoryJobFence, JobRecord, PgRepository,
    RepositoryError,
};
use insight_platform_rpc_trace::{scope_trace, RpcTraceContext};
use serde_json::{json, Value};
use std::{error::Error, fmt, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, Semaphore},
    task::JoinSet,
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const MCP_SUBSCRIPTION_MAX_BATCH: u16 = 64;

#[derive(Debug, Clone, Copy)]
pub struct McpSubscriptionDriverTiming {
    pub scan_interval: Duration,
    pub failure_backoff: Duration,
    pub heartbeat_interval: Duration,
    pub receipt_ttl: Duration,
    pub drain_grace: Duration,
}

#[derive(Debug, Clone, Copy)]
pub struct McpSubscriptionDriverConfig {
    pub claim_batch_size: u16,
    pub recovery_batch_size: u16,
    pub reconcile_batch_size: u16,
    pub reconcile_minimum_idle_milliseconds: u64,
    pub maximum_concurrency: usize,
    pub lease_milliseconds: u64,
    pub timing: McpSubscriptionDriverTiming,
}

impl McpSubscriptionDriverConfig {
    pub fn validate(self) -> Result<(), McpSubscriptionDriverError> {
        if self.claim_batch_size == 0
            || self.claim_batch_size > MCP_SUBSCRIPTION_MAX_BATCH
            || self.recovery_batch_size == 0
            || self.recovery_batch_size > MAX_MCP_SUBSCRIPTION_RECONCILE_SCAN
            || self.reconcile_batch_size == 0
            || self.reconcile_batch_size > MAX_MCP_SUBSCRIPTION_RECONCILE_SCAN
            || self.reconcile_minimum_idle_milliseconds
                < MIN_MCP_SUBSCRIPTION_RECONCILE_IDLE_MILLISECONDS
            || self.maximum_concurrency == 0
            || self.maximum_concurrency > usize::from(MCP_SUBSCRIPTION_MAX_BATCH)
            || self.lease_milliseconds < 3
            || self.timing.scan_interval.is_zero()
            || self.timing.failure_backoff.is_zero()
            || self.timing.heartbeat_interval.is_zero()
            || self.timing.heartbeat_interval >= Duration::from_millis(self.lease_milliseconds / 3)
            || self.timing.receipt_ttl <= Duration::from_millis(self.lease_milliseconds)
            || self.timing.drain_grace.is_zero()
            || ChronoDuration::from_std(self.timing.receipt_ttl).is_err()
        {
            return Err(McpSubscriptionDriverError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum McpSubscriptionDriverError {
    InvalidConfiguration,
    InvalidGeneratedIdentity,
    CorruptClaim,
    LeaseCoordination,
    Repository(RepositoryError),
    Worker(McpSubscriptionWorkerError),
    DrainRequiresTermination,
}

impl fmt::Display for McpSubscriptionDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => {
                formatter.write_str("MCP subscription driver configuration is invalid")
            }
            Self::InvalidGeneratedIdentity => {
                formatter.write_str("MCP subscription driver generated an invalid identity")
            }
            Self::CorruptClaim => {
                formatter.write_str("MCP subscription authority returned a corrupt claim")
            }
            Self::LeaseCoordination => {
                formatter.write_str("MCP subscription lease coordination failed")
            }
            Self::Repository(failure) => {
                write!(formatter, "MCP subscription repository failed: {failure}")
            }
            Self::Worker(failure) => {
                write!(formatter, "MCP subscription execution failed: {failure}")
            }
            Self::DrainRequiresTermination => {
                formatter.write_str("MCP subscription drain requires process termination")
            }
        }
    }
}

impl Error for McpSubscriptionDriverError {}

impl From<RepositoryError> for McpSubscriptionDriverError {
    fn from(failure: RepositoryError) -> Self {
        Self::Repository(failure)
    }
}

impl From<McpSubscriptionWorkerError> for McpSubscriptionDriverError {
    fn from(failure: McpSubscriptionWorkerError) -> Self {
        Self::Worker(failure)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct McpSubscriptionDriverReport {
    pub claimed: u64,
    pub recovered: u64,
    pub reconciled: u64,
    pub settled: u64,
    pub abandoned: u64,
}

pub struct McpSubscriptionDriver {
    repository: Arc<PgRepository>,
    worker: Arc<McpSubscriptionWorker>,
    worker_process_generation_id: ResourceId,
    permits: Arc<Semaphore>,
    config: McpSubscriptionDriverConfig,
}

#[derive(Clone)]
pub struct McpSubscriptionCapacity {
    permits: Arc<Semaphore>,
    maximum: usize,
}

impl McpSubscriptionCapacity {
    pub fn maximum(&self) -> usize {
        self.maximum
    }

    pub fn available(&self) -> usize {
        self.permits.available_permits()
    }
}

impl McpSubscriptionDriver {
    pub fn new(
        repository: Arc<PgRepository>,
        worker: Arc<McpSubscriptionWorker>,
        worker_process_generation_id: ResourceId,
        config: McpSubscriptionDriverConfig,
    ) -> Result<Self, McpSubscriptionDriverError> {
        config.validate()?;
        if worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration {
            return Err(McpSubscriptionDriverError::InvalidConfiguration);
        }
        Ok(Self {
            repository,
            worker,
            worker_process_generation_id,
            permits: Arc::new(Semaphore::new(config.maximum_concurrency)),
            config,
        })
    }

    pub fn capacity(&self) -> McpSubscriptionCapacity {
        McpSubscriptionCapacity {
            permits: Arc::clone(&self.permits),
            maximum: self.config.maximum_concurrency,
        }
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<bool>,
    ) -> Result<usize, McpSubscriptionDriverError> {
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
        let tokens = (0..reservations.len())
            .map(|_| new_digest())
            .collect::<Result<Vec<_>, _>>()?;
        let claims = self
            .repository
            .claim_mcp_subscription_jobs(ClaimJobs {
                work_class: WorkClass::Mcp.as_str().to_owned(),
                worker_id: self.worker_process_generation_id.clone(),
                limit: u16::try_from(reservations.len())
                    .map_err(|_| McpSubscriptionDriverError::InvalidConfiguration)?,
                lease_milliseconds: i64::try_from(self.config.lease_milliseconds)
                    .map_err(|_| McpSubscriptionDriverError::InvalidConfiguration)?,
                lease_token_digests: tokens,
            })
            .await?;
        if claims.len() > reservations.len() {
            return Err(McpSubscriptionDriverError::CorruptClaim);
        }
        let count = claims.len();
        for (claim, permit) in claims.into_iter().zip(reservations.drain(..)) {
            let trace = RpcTraceContext::start(claim.trace, TraceFlags::NotSampled)
                .map_err(|_| McpSubscriptionDriverError::CorruptClaim)?;
            let repository = Arc::clone(&self.repository);
            let worker = Arc::clone(&self.worker);
            let config = self.config;
            active.spawn(async move {
                let _permit = permit;
                match scope_trace(trace, execute_claim(repository, worker, config, claim)).await {
                    Ok(()) => true,
                    Err(failure) => {
                        eprintln!(
                            "platform-mcp-subscription-worker abandoned execution: {failure}"
                        );
                        false
                    }
                }
            });
        }
        Ok(count)
    }

    pub async fn recover_once(&self) -> Result<usize, McpSubscriptionDriverError> {
        let candidates = self
            .repository
            .list_due_mcp_subscription_recoveries_global(self.config.recovery_batch_size)
            .await?;
        let mut recovered = 0usize;
        for candidate in candidates {
            let mut command = RecoverDueMcpSubscription {
                audit: control_audit(
                    &candidate.tenant_id,
                    &self.worker_process_generation_id,
                    candidate.trace,
                    "mcp.subscription.recovery",
                    &candidate,
                    self.config.timing.receipt_ttl,
                )?,
                candidate,
            };
            command.audit.request_digest = command
                .request_digest()
                .map_err(|_| McpSubscriptionDriverError::CorruptClaim)?;
            match self.repository.recover_due_mcp_subscription(command).await {
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

    pub async fn reconcile_once(&self) -> Result<usize, McpSubscriptionDriverError> {
        let candidates = self
            .repository
            .list_due_mcp_subscription_reconciliations_global(
                self.config.reconcile_batch_size,
                self.config.reconcile_minimum_idle_milliseconds,
            )
            .await?;
        let mut reconciled = 0usize;
        for candidate in candidates {
            let mut command = WakeMcpSubscriptionReconcile {
                audit: control_audit(
                    &candidate.tenant_id,
                    &self.worker_process_generation_id,
                    candidate.trace,
                    "mcp.subscription.reconcile",
                    &candidate,
                    self.config.timing.receipt_ttl,
                )?,
                candidate,
            };
            command.audit.request_digest = command
                .request_digest()
                .map_err(|_| McpSubscriptionDriverError::CorruptClaim)?;
            match self
                .repository
                .wake_mcp_subscription_reconcile(command)
                .await
            {
                Ok(_) => reconciled = reconciled.saturating_add(1),
                Err(
                    RepositoryError::Conflict(_)
                    | RepositoryError::StaleFence
                    | RepositoryError::LeaseExpired
                    | RepositoryError::NotFound(_),
                ) => {}
                Err(failure) => return Err(failure.into()),
            }
        }
        Ok(reconciled)
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<McpSubscriptionDriverReport, McpSubscriptionDriverError> {
        let mut active = JoinSet::new();
        let mut report = McpSubscriptionDriverReport::default();
        let mut scan = tokio::time::interval(self.config.timing.scan_interval);
        scan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                joined = active.join_next(), if !active.is_empty() => observe_join(joined, &mut report),
                _ = scan.tick() => {
                    let control = async {
                        let recovered = self.recover_once().await?;
                        let reconciled = self.reconcile_once().await?;
                        let claimed = self.drive_once(&mut active).await?;
                        Ok::<_, McpSubscriptionDriverError>((recovered, reconciled, claimed))
                    }.await;
                    match control {
                    Ok((recovered, reconciled, claimed)) => {
                        report.recovered = report.recovered.saturating_add(recovered as u64);
                        report.reconciled = report.reconciled.saturating_add(reconciled as u64);
                        report.claimed = report.claimed.saturating_add(claimed as u64);
                    }
                    Err(McpSubscriptionDriverError::Repository(RepositoryError::Database(_))) => {
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
            .map_err(|_| McpSubscriptionDriverError::DrainRequiresTermination)?;
        Ok(report)
    }
}

enum RemoteIoLeasePhase {
    Enter {
        observed: JobFence,
        response: oneshot::Sender<JobFence>,
    },
    Exit {
        observed: JobFence,
        response: oneshot::Sender<JobFence>,
    },
}

struct ChannelRemoteIoLease {
    sender: mpsc::Sender<RemoteIoLeasePhase>,
}

#[async_trait]
impl McpSubscriptionRemoteIoLease for ChannelRemoteIoLease {
    async fn enter_remote_io(&self, fence: &JobFence) -> Result<JobFence, ()> {
        self.exchange(true, fence).await
    }

    async fn exit_remote_io(&self, fence: &JobFence) -> Result<JobFence, ()> {
        self.exchange(false, fence).await
    }
}

impl ChannelRemoteIoLease {
    async fn exchange(&self, enter: bool, fence: &JobFence) -> Result<JobFence, ()> {
        let (response, receiver) = oneshot::channel();
        let phase = if enter {
            RemoteIoLeasePhase::Enter {
                observed: fence.clone(),
                response,
            }
        } else {
            RemoteIoLeasePhase::Exit {
                observed: fence.clone(),
                response,
            }
        };
        self.sender.send(phase).await.map_err(|_| ())?;
        receiver.await.map_err(|_| ())
    }
}

async fn execute_claim(
    repository: Arc<PgRepository>,
    worker: Arc<McpSubscriptionWorker>,
    config: McpSubscriptionDriverConfig,
    claim: JobRecord,
) -> Result<(), McpSubscriptionDriverError> {
    let tenant_id = parse_id(&claim.tenant_id, ResourceKind::Tenant)?;
    let job_id = parse_id(&claim.job_id, ResourceKind::Job)?;
    let subscription_id = parse_id(&claim.owner_id, ResourceKind::McpOperation)?;
    let worker_id = parse_id(
        claim
            .worker_id
            .as_deref()
            .ok_or(McpSubscriptionDriverError::CorruptClaim)?,
        ResourceKind::WorkerProcessGeneration,
    )?;
    let token: Sha256Digest = claim
        .lease_token_digest
        .as_deref()
        .ok_or(McpSubscriptionDriverError::CorruptClaim)?
        .parse()
        .map_err(|_| McpSubscriptionDriverError::CorruptClaim)?;
    if claim.job_kind != "mcp_subscription"
        || claim.work_class != WorkClass::Mcp.as_str()
        || claim.owner_kind != "mcp_operation"
        || claim.invocation_id.as_deref() != Some(claim.owner_id.as_str())
        || claim.state != "leased"
        || claim.lease_epoch <= 0
        || claim.version <= 0
    {
        return Err(McpSubscriptionDriverError::CorruptClaim);
    }
    let payload = subscription_payload(&claim)?;
    if payload.subscription_id != subscription_id {
        return Err(McpSubscriptionDriverError::CorruptClaim);
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
        return Err(McpSubscriptionDriverError::CorruptClaim);
    }
    let mut current_fence = JobFence {
        expected_version: u64::try_from(started.version)
            .map_err(|_| McpSubscriptionDriverError::CorruptClaim)?,
        worker_process_generation_id: worker_id.clone(),
        lease_generation: u64::try_from(started.lease_epoch)
            .map_err(|_| McpSubscriptionDriverError::CorruptClaim)?,
        token_digest: token.clone(),
    };
    let now = Utc::now();
    let attempt_identity = canonical_digest(&json!({
        "schema_version": 1,
        "operation": "mcp.subscription.execute",
        "tenant_id": tenant_id,
        "subscription_id": subscription_id,
        "job_id": job_id,
        "worker_process_generation_id": worker_id,
        "lease_generation": current_fence.lease_generation,
        "physical_attempt": started.attempt_no,
        "binding_digest": payload.binding_digest,
    }))
    .map_err(|_| McpSubscriptionDriverError::InvalidGeneratedIdentity)?
    .parse()
    .map_err(|_| McpSubscriptionDriverError::InvalidGeneratedIdentity)?;
    let command = ExecuteMcpSubscriptionJob {
        query: McpSubscriptionContractQuery {
            schema_version: 1,
            tenant_id: tenant_id.clone(),
            subscription_id,
            job_id,
            fence: current_fence.clone(),
        },
        audits: worker_audits(
            &tenant_id,
            &worker_id,
            &attempt_identity,
            now.checked_add_signed(
                ChronoDuration::from_std(config.timing.receipt_ttl)
                    .map_err(|_| McpSubscriptionDriverError::InvalidConfiguration)?,
            )
            .ok_or(McpSubscriptionDriverError::InvalidGeneratedIdentity)?,
            claim.trace,
        )?,
    };
    let (phase_sender, mut phase_receiver) = mpsc::channel(2);
    let lease = Arc::new(ChannelRemoteIoLease {
        sender: phase_sender,
    });
    let execution = worker.execute_with_remote_io_lease(command, lease);
    tokio::pin!(execution);
    let mut heartbeat = tokio::time::interval_at(
        Instant::now()
            .checked_add(config.timing.heartbeat_interval)
            .unwrap_or_else(Instant::now),
        config.timing.heartbeat_interval,
    );
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut remote_active = false;
    loop {
        tokio::select! {
            result = &mut execution => {
                if remote_active {
                    return Err(McpSubscriptionDriverError::LeaseCoordination);
                }
                result?;
                return Ok(());
            }
            phase = phase_receiver.recv() => {
                let phase = phase.ok_or(McpSubscriptionDriverError::LeaseCoordination)?;
                handle_lease_phase(phase, &mut current_fence, &mut remote_active)?;
            }
            _ = heartbeat.tick(), if remote_active => {
                let record = repository.heartbeat_job(HeartbeatJob {
                    fence: RepositoryJobFence {
                        tenant_id: tenant_id.to_string(),
                        job_id: started.job_id.clone(),
                        worker_id: worker_id.clone(),
                        lease_epoch: i64::try_from(current_fence.lease_generation)
                            .map_err(|_| McpSubscriptionDriverError::CorruptClaim)?,
                        expected_job_version: i64::try_from(current_fence.expected_version)
                            .map_err(|_| McpSubscriptionDriverError::CorruptClaim)?,
                        lease_token_digest: token.clone(),
                    },
                    lease_milliseconds: i64::try_from(config.lease_milliseconds)
                        .map_err(|_| McpSubscriptionDriverError::InvalidConfiguration)?,
                }).await?;
                let candidate = JobFence {
                    expected_version: u64::try_from(record.version)
                        .map_err(|_| McpSubscriptionDriverError::CorruptClaim)?,
                    ..current_fence.clone()
                };
                validate_monotonic_fence(&current_fence, &candidate)?;
                current_fence = candidate;
            }
        }
    }
}

fn handle_lease_phase(
    phase: RemoteIoLeasePhase,
    current: &mut JobFence,
    remote_active: &mut bool,
) -> Result<(), McpSubscriptionDriverError> {
    let (enter, observed, response) = match phase {
        RemoteIoLeasePhase::Enter { observed, response } => (true, observed, response),
        RemoteIoLeasePhase::Exit { observed, response } => (false, observed, response),
    };
    if enter == *remote_active {
        return Err(McpSubscriptionDriverError::LeaseCoordination);
    }
    if enter {
        validate_monotonic_fence(current, &observed)?;
        *current = observed;
        *remote_active = true;
    } else {
        validate_same_lease(current, &observed)?;
        if observed.expected_version > current.expected_version {
            return Err(McpSubscriptionDriverError::LeaseCoordination);
        }
        *remote_active = false;
    }
    response
        .send(current.clone())
        .map_err(|_| McpSubscriptionDriverError::LeaseCoordination)
}

fn validate_monotonic_fence(
    current: &JobFence,
    candidate: &JobFence,
) -> Result<(), McpSubscriptionDriverError> {
    validate_same_lease(current, candidate)?;
    if candidate.expected_version < current.expected_version {
        return Err(McpSubscriptionDriverError::LeaseCoordination);
    }
    Ok(())
}

fn validate_same_lease(
    current: &JobFence,
    candidate: &JobFence,
) -> Result<(), McpSubscriptionDriverError> {
    if candidate.worker_process_generation_id != current.worker_process_generation_id
        || candidate.lease_generation != current.lease_generation
        || candidate.token_digest != current.token_digest
    {
        return Err(McpSubscriptionDriverError::LeaseCoordination);
    }
    Ok(())
}

fn worker_audits(
    tenant_id: &ResourceId,
    worker_id: &ResourceId,
    attempt_identity: &Sha256Digest,
    receipt_expires_at: chrono::DateTime<Utc>,
    trace: insight_platform_contracts::TraceIdentityV1,
) -> Result<McpSubscriptionWorkerAudits, McpSubscriptionDriverError> {
    let audit = |phase: &str| -> Result<McpSubscriptionWorkerAudit, McpSubscriptionDriverError> {
        let idempotency_key_digest: Sha256Digest = canonical_digest(&json!({
            "schema_version": 1,
            "attempt_identity_digest": attempt_identity,
            "phase": phase,
        }))
        .map_err(|_| McpSubscriptionDriverError::InvalidGeneratedIdentity)?
        .parse()
        .map_err(|_| McpSubscriptionDriverError::InvalidGeneratedIdentity)?;
        Ok(McpSubscriptionWorkerAudit {
            trace,
            tenant_id: tenant_id.clone(),
            worker_process_generation_id: worker_id.clone(),
            receipt_id: new_id(ResourceKind::Receipt)?,
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: idempotency_key_digest.clone(),
            request_digest: idempotency_key_digest,
            receipt_expires_at,
        })
    };
    Ok(McpSubscriptionWorkerAudits {
        connecting: audit("connecting")?,
        initializing: audit("initializing")?,
        ready: audit("ready")?,
        terminal: audit("terminal")?,
        refresh: audit("refresh")?,
    })
}

fn control_audit<T: serde::Serialize>(
    tenant_id: &ResourceId,
    worker_id: &ResourceId,
    trace: insight_platform_contracts::TraceIdentityV1,
    operation: &str,
    candidate: &T,
    receipt_ttl: Duration,
) -> Result<McpSubscriptionWorkerAudit, McpSubscriptionDriverError> {
    let now = Utc::now();
    let idempotency_key_digest: Sha256Digest = canonical_digest(&json!({
        "schema_version": 1,
        "operation": operation,
        "candidate": candidate,
    }))
    .map_err(|_| McpSubscriptionDriverError::InvalidGeneratedIdentity)?
    .parse()
    .map_err(|_| McpSubscriptionDriverError::InvalidGeneratedIdentity)?;
    Ok(McpSubscriptionWorkerAudit {
        trace,
        tenant_id: tenant_id.clone(),
        worker_process_generation_id: worker_id.clone(),
        receipt_id: new_id(ResourceKind::Receipt)?,
        event_id: new_id(ResourceKind::Event)?,
        outbox_id: new_id(ResourceKind::OutboxEvent)?,
        idempotency_key_digest: idempotency_key_digest.clone(),
        request_digest: idempotency_key_digest,
        receipt_expires_at: now
            .checked_add_signed(
                ChronoDuration::from_std(receipt_ttl)
                    .map_err(|_| McpSubscriptionDriverError::InvalidConfiguration)?,
            )
            .ok_or(McpSubscriptionDriverError::InvalidGeneratedIdentity)?,
    })
}

fn subscription_payload(
    claim: &JobRecord,
) -> Result<insight_platform_mcp_host::McpSubscriptionJobPayload, McpSubscriptionDriverError> {
    let mut value = claim.payload.value.clone();
    let Value::Object(object) = &mut value else {
        return Err(McpSubscriptionDriverError::CorruptClaim);
    };
    object.remove("schema_version");
    let payload: McpJobPayload =
        serde_json::from_value(value).map_err(|_| McpSubscriptionDriverError::CorruptClaim)?;
    match payload {
        McpJobPayload::Subscription(payload) => {
            payload
                .validate_for(&payload.subscription_id)
                .map_err(|_| McpSubscriptionDriverError::CorruptClaim)?;
            Ok(payload)
        }
        McpJobPayload::Discovery(_) => Err(McpSubscriptionDriverError::CorruptClaim),
    }
}

fn observe_join(
    joined: Option<Result<bool, tokio::task::JoinError>>,
    report: &mut McpSubscriptionDriverReport,
) {
    match joined {
        Some(Ok(true)) => report.settled = report.settled.saturating_add(1),
        Some(Ok(false) | Err(_)) => report.abandoned = report.abandoned.saturating_add(1),
        None => {}
    }
}

fn new_id(kind: ResourceKind) -> Result<ResourceId, McpSubscriptionDriverError> {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7())
        .map_err(|_| McpSubscriptionDriverError::InvalidGeneratedIdentity)
}

fn new_digest() -> Result<Sha256Digest, McpSubscriptionDriverError> {
    canonical_digest(&json!({"nonce": Uuid::now_v7().to_string()}))
        .map_err(|_| McpSubscriptionDriverError::InvalidGeneratedIdentity)?
        .parse()
        .map_err(|_| McpSubscriptionDriverError::InvalidGeneratedIdentity)
}

fn parse_id(value: &str, kind: ResourceKind) -> Result<ResourceId, McpSubscriptionDriverError> {
    let id: ResourceId = value
        .parse()
        .map_err(|_| McpSubscriptionDriverError::CorruptClaim)?;
    if id.kind() != kind {
        return Err(McpSubscriptionDriverError::CorruptClaim);
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fence(version: u64, token: char) -> JobFence {
        JobFence {
            expected_version: version,
            worker_process_generation_id: new_id(ResourceKind::WorkerProcessGeneration).unwrap(),
            lease_generation: 2,
            token_digest: format!("sha256:{}", token.to_string().repeat(64))
                .parse()
                .unwrap(),
        }
    }

    #[test]
    fn driver_config_rejects_heartbeat_that_can_overtake_the_lease() {
        let config = McpSubscriptionDriverConfig {
            claim_batch_size: 1,
            recovery_batch_size: 1,
            reconcile_batch_size: 1,
            reconcile_minimum_idle_milliseconds: 60_000,
            maximum_concurrency: 1,
            lease_milliseconds: 300,
            timing: McpSubscriptionDriverTiming {
                scan_interval: Duration::from_millis(10),
                failure_backoff: Duration::from_millis(10),
                heartbeat_interval: Duration::from_millis(100),
                receipt_ttl: Duration::from_secs(1),
                drain_grace: Duration::from_secs(1),
            },
        };
        assert!(matches!(
            config.validate(),
            Err(McpSubscriptionDriverError::InvalidConfiguration)
        ));
    }

    #[test]
    fn lease_phase_accepts_owner_advancement_and_returns_heartbeat_advancement() {
        let mut current = fence(3, 'a');
        let entered = JobFence {
            expected_version: 5,
            ..current.clone()
        };
        let (response, receiver) = oneshot::channel();
        let mut active = false;
        handle_lease_phase(
            RemoteIoLeasePhase::Enter {
                observed: entered,
                response,
            },
            &mut current,
            &mut active,
        )
        .unwrap();
        assert!(active);
        assert_eq!(receiver.blocking_recv().unwrap().expected_version, 5);
        current.expected_version = 8;
        let exited = JobFence {
            expected_version: 5,
            ..current.clone()
        };
        let (response, receiver) = oneshot::channel();
        handle_lease_phase(
            RemoteIoLeasePhase::Exit {
                observed: exited,
                response,
            },
            &mut current,
            &mut active,
        )
        .unwrap();
        assert!(!active);
        assert_eq!(receiver.blocking_recv().unwrap().expected_version, 8);
    }

    #[test]
    fn lease_phase_rejects_identity_drift() {
        let mut current = fence(3, 'a');
        let drifted = JobFence {
            token_digest: fence(3, 'b').token_digest,
            ..current.clone()
        };
        let (response, _) = oneshot::channel();
        let mut active = false;
        assert!(matches!(
            handle_lease_phase(
                RemoteIoLeasePhase::Enter {
                    observed: drifted,
                    response,
                },
                &mut current,
                &mut active,
            ),
            Err(McpSubscriptionDriverError::LeaseCoordination)
        ));
    }
}
