//! Lease-fenced orchestration generation execution with heartbeat and drain handoff.

use crate::{
    ActiveOrchestrationJob, CoordinatorIdentityFactory, ExecutionDisposition, IdentityFactoryError,
    OrchestrationJobExecutor,
};
use async_trait::async_trait;
use insight_platform_contracts::{
    canonical_digest, CommandOutcome, HardLimitProfile, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_postgres::repository::{
    HeartbeatJob, JobFence, JobRecord, PgRepository, RepositoryError, StartOrchestrationJob,
};
use serde_json::json;
use std::{error::Error, fmt, sync::Arc, time::Duration};
use tokio::time::{Instant, MissedTickBehavior};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationStoreFailure {
    Unavailable,
    FenceLost,
    InvariantViolation,
}

#[async_trait]
pub trait OrchestrationGenerationStore: Send + Sync + 'static {
    async fn start_generation(
        &self,
        command: StartOrchestrationJob,
    ) -> Result<CommandOutcome<JobRecord>, GenerationStoreFailure>;

    async fn heartbeat_generation(
        &self,
        command: HeartbeatJob,
    ) -> Result<JobRecord, GenerationStoreFailure>;
}

#[async_trait]
impl OrchestrationGenerationStore for PgRepository {
    async fn start_generation(
        &self,
        command: StartOrchestrationJob,
    ) -> Result<CommandOutcome<JobRecord>, GenerationStoreFailure> {
        let mut transaction = self
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        match transaction.start_orchestration_job(command).await {
            Ok(record) => {
                transaction
                    .commit()
                    .await
                    .map_err(classify_repository_failure)?;
                Ok(record)
            }
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                Err(classify_repository_failure(failure))
            }
        }
    }

    async fn heartbeat_generation(
        &self,
        command: HeartbeatJob,
    ) -> Result<JobRecord, GenerationStoreFailure> {
        let mut transaction = self
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        match transaction.heartbeat_orchestration_job(command).await {
            Ok(record) => {
                transaction
                    .commit()
                    .await
                    .map_err(classify_repository_failure)?;
                Ok(record)
            }
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                Err(classify_repository_failure(failure))
            }
        }
    }
}

fn classify_repository_failure(failure: RepositoryError) -> GenerationStoreFailure {
    match failure {
        RepositoryError::Database(_) => GenerationStoreFailure::Unavailable,
        RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired => GenerationStoreFailure::FenceLost,
        RepositoryError::InvalidInput(_)
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict
        | RepositoryError::CorruptRow(_) => GenerationStoreFailure::InvariantViolation,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrchestrationExecutorTiming {
    pub heartbeat_jitter: Duration,
    pub store_retry_backoff: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrchestrationExecutorConfig {
    lease_duration: Duration,
    lease_milliseconds: i64,
    heartbeat_interval: Duration,
    timing: OrchestrationExecutorTiming,
}

impl OrchestrationExecutorConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        timing: OrchestrationExecutorTiming,
    ) -> Result<Self, OrchestrationExecutorConfigError> {
        profile.validate().map_err(|failure| {
            OrchestrationExecutorConfigError::InvalidProfile(failure.to_string())
        })?;
        let lease_milliseconds = i64::try_from(profile.run_scheduler.lease_milliseconds.q1_default)
            .map_err(|_| OrchestrationExecutorConfigError::TimingOutsideRepresentation)?;
        let heartbeat_milliseconds = profile.run_scheduler.heartbeat_milliseconds.q1_default;
        let config = Self {
            lease_duration: Duration::from_millis(
                u64::try_from(lease_milliseconds)
                    .map_err(|_| OrchestrationExecutorConfigError::TimingOutsideRepresentation)?,
            ),
            lease_milliseconds,
            heartbeat_interval: Duration::from_millis(heartbeat_milliseconds),
            timing,
        };
        config.validate()?;
        Ok(config)
    }

    pub const fn heartbeat_interval(self) -> Duration {
        self.heartbeat_interval
    }

    pub const fn lease_duration(self) -> Duration {
        self.lease_duration
    }

    fn validate(self) -> Result<(), OrchestrationExecutorConfigError> {
        let heartbeat_upper = self
            .heartbeat_interval
            .checked_add(self.timing.heartbeat_jitter)
            .ok_or(OrchestrationExecutorConfigError::TimingOutsideRepresentation)?;
        let heartbeat_margin = heartbeat_upper
            .checked_mul(3)
            .ok_or(OrchestrationExecutorConfigError::TimingOutsideRepresentation)?;
        if self.lease_milliseconds <= 0
            || self.heartbeat_interval.is_zero()
            || self.timing.store_retry_backoff.is_zero()
            || self.timing.store_retry_backoff > self.heartbeat_interval
            || heartbeat_margin >= self.lease_duration
        {
            return Err(OrchestrationExecutorConfigError::InvalidTiming);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum OrchestrationExecutorConfigError {
    InvalidProfile(String),
    TimingOutsideRepresentation,
    InvalidTiming,
}

impl fmt::Display for OrchestrationExecutorConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProfile(message) => {
                write!(formatter, "invalid HardLimitProfile: {message}")
            }
            Self::TimingOutsideRepresentation => {
                formatter.write_str("orchestration executor timing exceeds its representation")
            }
            Self::InvalidTiming => formatter.write_str(
                "orchestration heartbeat, jitter, retry, and lease timing are inconsistent",
            ),
        }
    }
}

impl Error for OrchestrationExecutorConfigError {}

#[derive(Debug, Clone)]
pub struct StartedOrchestrationJob {
    claimed: insight_platform_postgres::repository::ClaimedOrchestrationJob,
    started: JobRecord,
}

impl StartedOrchestrationJob {
    pub(crate) fn from_parts(
        claimed: insight_platform_postgres::repository::ClaimedOrchestrationJob,
        started: JobRecord,
    ) -> Self {
        Self { claimed, started }
    }

    pub fn claimed(&self) -> &insight_platform_postgres::repository::ClaimedOrchestrationJob {
        &self.claimed
    }

    pub fn started(&self) -> &JobRecord {
        &self.started
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationHandlerError {
    Unavailable,
    InvariantViolation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationHandlerDisposition {
    Committed,
    FenceLost,
    NotCommitted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationHandoffReason {
    Shutdown,
    HandlerUnavailable,
    HandlerInvariantViolation,
    GenerationStoreInvariantViolation,
}

#[async_trait]
pub trait StartedOrchestrationJobHandler: Send + Sync + 'static {
    type Outcome: Send + 'static;

    async fn run(
        &self,
        job: StartedOrchestrationJob,
    ) -> Result<Self::Outcome, GenerationHandlerError>;

    async fn commit(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        outcome: Self::Outcome,
    ) -> GenerationHandlerDisposition;

    async fn handoff(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        reason: GenerationHandoffReason,
    ) -> GenerationHandlerDisposition;
}

pub struct LeaseFencedOrchestrationExecutor<S, H, I> {
    store: Arc<S>,
    handler: Arc<H>,
    identities: Arc<I>,
    config: OrchestrationExecutorConfig,
}

impl<S, H, I> LeaseFencedOrchestrationExecutor<S, H, I>
where
    S: OrchestrationGenerationStore,
    H: StartedOrchestrationJobHandler,
    I: CoordinatorIdentityFactory + 'static,
{
    pub fn new(
        store: Arc<S>,
        handler: Arc<H>,
        identities: Arc<I>,
        config: OrchestrationExecutorConfig,
    ) -> Result<Self, OrchestrationExecutorConfigError> {
        config.validate()?;
        Ok(Self {
            store,
            handler,
            identities,
            config,
        })
    }

    async fn start(
        &self,
        active: &ActiveOrchestrationJob,
    ) -> Result<(StartedOrchestrationJob, JobFence), GenerationStoreFailure> {
        let claimed = active.claimed().clone();
        let mut fence = fence_from_claimed(&claimed)?;
        let command = start_command(&fence, &claimed.job, self.identities.as_ref())?;
        let shutdown = active.shutdown_token();
        let lease_expires_at = claimed
            .job
            .lease_expires_at
            .ok_or(GenerationStoreFailure::InvariantViolation)?;
        loop {
            if shutdown.is_cancelled() {
                return Err(GenerationStoreFailure::FenceLost);
            }
            match self.store.start_generation(command.clone()).await {
                Ok(outcome) => {
                    let started = match outcome {
                        CommandOutcome::Applied(record) | CommandOutcome::Replayed(record) => {
                            record
                        }
                    };
                    validate_started_job(&claimed.job, &started)?;
                    fence.expected_job_version = started.version;
                    return Ok((StartedOrchestrationJob::from_parts(claimed, started), fence));
                }
                Err(GenerationStoreFailure::Unavailable)
                    if chrono::Utc::now() < lease_expires_at =>
                {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            return Err(GenerationStoreFailure::FenceLost);
                        }
                        _ = tokio::time::sleep(self.config.timing.store_retry_backoff) => {}
                    }
                }
                Err(failure) => return Err(failure),
            }
        }
    }

    async fn drive_started(
        &self,
        active: &ActiveOrchestrationJob,
        started: StartedOrchestrationJob,
        mut fence: JobFence,
    ) -> ExecutionDisposition {
        let heartbeat_period =
            self.config
                .heartbeat_interval
                .saturating_add(deterministic_generation_jitter(
                    &fence.job_id,
                    self.config.timing.heartbeat_jitter,
                ));
        let mut heartbeat = tokio::time::interval_at(
            Instant::now()
                .checked_add(heartbeat_period)
                .unwrap_or_else(Instant::now),
            heartbeat_period,
        );
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let shutdown = active.shutdown_token();
        let run = self.handler.run(started.clone());
        tokio::pin!(run);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    return disposition(self.handler
                        .handoff(
                            &started,
                            fence,
                            GenerationHandoffReason::Shutdown,
                        )
                        .await);
                }
                result = &mut run => {
                    return match result {
                        Ok(outcome) => disposition(
                            self.handler.commit(&started, fence, outcome).await,
                        ),
                        Err(GenerationHandlerError::Unavailable) => disposition(
                            self.handler
                                .handoff(
                                    &started,
                                    fence,
                                    GenerationHandoffReason::HandlerUnavailable,
                                )
                                .await,
                        ),
                        Err(GenerationHandlerError::InvariantViolation) => disposition(
                            self.handler
                                .handoff(
                                    &started,
                                    fence,
                                    GenerationHandoffReason::HandlerInvariantViolation,
                                )
                                .await,
                        ),
                    };
                }
                _ = heartbeat.tick() => {
                    match self
                        .store
                        .heartbeat_generation(HeartbeatJob {
                            fence: fence.clone(),
                            lease_milliseconds: self.config.lease_milliseconds,
                        })
                        .await
                    {
                        Ok(record) => {
                            if validate_heartbeat_job(&fence, &record).is_err() {
                                return disposition(
                                    self.handler
                                        .handoff(
                                            &started,
                                            fence,
                                            GenerationHandoffReason::GenerationStoreInvariantViolation,
                                        )
                                        .await,
                                );
                            }
                            fence.expected_job_version = record.version;
                        }
                        Err(GenerationStoreFailure::Unavailable) => {}
                        Err(GenerationStoreFailure::FenceLost) => {
                            return ExecutionDisposition::GenerationAbandoned;
                        }
                        Err(GenerationStoreFailure::InvariantViolation) => {
                            return disposition(
                                self.handler
                                    .handoff(
                                        &started,
                                        fence,
                                        GenerationHandoffReason::GenerationStoreInvariantViolation,
                                    )
                                    .await,
                            );
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl<S, H, I> OrchestrationJobExecutor for LeaseFencedOrchestrationExecutor<S, H, I>
where
    S: OrchestrationGenerationStore,
    H: StartedOrchestrationJobHandler,
    I: CoordinatorIdentityFactory + 'static,
{
    async fn execute(&self, active: ActiveOrchestrationJob) -> ExecutionDisposition {
        match self.start(&active).await {
            Ok((started, fence)) => self.drive_started(&active, started, fence).await,
            Err(_) => ExecutionDisposition::GenerationAbandoned,
        }
    }
}

fn disposition(value: GenerationHandlerDisposition) -> ExecutionDisposition {
    match value {
        GenerationHandlerDisposition::Committed => ExecutionDisposition::GenerationSettled,
        GenerationHandlerDisposition::FenceLost | GenerationHandlerDisposition::NotCommitted => {
            ExecutionDisposition::GenerationAbandoned
        }
    }
}

fn fence_from_claimed(
    claimed: &insight_platform_postgres::repository::ClaimedOrchestrationJob,
) -> Result<JobFence, GenerationStoreFailure> {
    let job = &claimed.job;
    let worker_id = job
        .worker_id
        .as_ref()
        .ok_or(GenerationStoreFailure::InvariantViolation)?
        .parse::<ResourceId>()
        .map_err(|_| GenerationStoreFailure::InvariantViolation)?;
    let lease_token_digest = job
        .lease_token_digest
        .as_ref()
        .ok_or(GenerationStoreFailure::InvariantViolation)?
        .parse::<Sha256Digest>()
        .map_err(|_| GenerationStoreFailure::InvariantViolation)?;
    if worker_id.kind() != ResourceKind::WorkerProcessGeneration
        || job.state != "leased"
        || job.lease_epoch <= 0
        || job.version <= 0
    {
        return Err(GenerationStoreFailure::InvariantViolation);
    }
    Ok(JobFence {
        tenant_id: job.tenant_id.clone(),
        job_id: job.job_id.clone(),
        worker_id,
        lease_epoch: job.lease_epoch,
        expected_job_version: job.version,
        lease_token_digest,
    })
}

fn start_command(
    fence: &JobFence,
    job: &JobRecord,
    identities: &impl CoordinatorIdentityFactory,
) -> Result<StartOrchestrationJob, GenerationStoreFailure> {
    if job.deadline <= chrono::Utc::now() {
        return Err(GenerationStoreFailure::FenceLost);
    }
    let request_digest = canonical_digest(&json!({
        "expected_job_version": fence.expected_job_version,
        "job_id": fence.job_id,
        "lease_generation": fence.lease_epoch,
        "lease_token_digest": fence.lease_token_digest,
        "operation": "orchestration.job.start",
        "worker_process_generation_id": fence.worker_id,
    }))
    .map_err(|_| GenerationStoreFailure::InvariantViolation)?
    .parse()
    .map_err(|_| GenerationStoreFailure::InvariantViolation)?;
    Ok(StartOrchestrationJob {
        fence: fence.clone(),
        receipt_id: new_id(identities, ResourceKind::Receipt)?,
        idempotency_key_digest: identities
            .new_lease_token_digest()
            .map_err(identity_failure)?,
        request_digest,
        receipt_expires_at: job.deadline,
        job_event_id: new_id(identities, ResourceKind::Event)?,
        job_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        node_event_id: new_id(identities, ResourceKind::Event)?,
        node_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
    })
}

fn new_id(
    identities: &impl CoordinatorIdentityFactory,
    kind: ResourceKind,
) -> Result<ResourceId, GenerationStoreFailure> {
    identities.new_resource_id(kind).map_err(identity_failure)
}

fn identity_failure(_failure: IdentityFactoryError) -> GenerationStoreFailure {
    GenerationStoreFailure::InvariantViolation
}

fn validate_started_job(
    claimed: &JobRecord,
    started: &JobRecord,
) -> Result<(), GenerationStoreFailure> {
    if started.tenant_id != claimed.tenant_id
        || started.job_id != claimed.job_id
        || started.worker_id != claimed.worker_id
        || started.lease_token_digest != claimed.lease_token_digest
        || started.lease_epoch != claimed.lease_epoch
        || started.state != "running"
        || started.version <= claimed.version
        || started.attempt_no != claimed.attempt_no + 1
        || started.started_at.is_none()
    {
        return Err(GenerationStoreFailure::InvariantViolation);
    }
    Ok(())
}

fn validate_heartbeat_job(
    fence: &JobFence,
    record: &JobRecord,
) -> Result<(), GenerationStoreFailure> {
    if record.tenant_id != fence.tenant_id
        || record.job_id != fence.job_id
        || record.worker_id.as_deref() != Some(fence.worker_id.to_string().as_str())
        || record.lease_token_digest.as_deref() != Some(fence.lease_token_digest.as_str())
        || record.lease_epoch != fence.lease_epoch
        || record.state != "running"
        || record.version <= fence.expected_job_version
        || record.heartbeat_at.is_none()
        || record.lease_expires_at.is_none()
    {
        return Err(GenerationStoreFailure::InvariantViolation);
    }
    Ok(())
}

fn deterministic_generation_jitter(job_id: &str, maximum: Duration) -> Duration {
    if maximum.is_zero() {
        return Duration::ZERO;
    }
    let Ok(job_id) = ResourceId::parse_expected(job_id, ResourceKind::Job) else {
        return maximum;
    };
    let maximum_nanos = maximum.as_nanos();
    let sample = u128::from(job_id.uuid().as_u128() as u64);
    let nanos = sample % (maximum_nanos + 1);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UuidCoordinatorIdentityFactory;
    use chrono::{Duration as ChronoDuration, Utc};
    use insight_platform_contracts::{
        checked_in_hard_limit_profile, SchedulerPriority, WorkClass, WorkerManifest,
    };
    use insight_platform_postgres::repository::{ClaimedOrchestrationJob, TypedPayload};
    use insight_platform_worker::{ClaimBatchHardLimit, ClaimedJobIdentity, LocalWorkerPools};
    use std::sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Mutex,
    };
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    struct RecordingStore {
        job: Mutex<JobRecord>,
        heartbeats: AtomicU64,
    }

    #[async_trait]
    impl OrchestrationGenerationStore for RecordingStore {
        async fn start_generation(
            &self,
            command: StartOrchestrationJob,
        ) -> Result<CommandOutcome<JobRecord>, GenerationStoreFailure> {
            let mut job = self.job.lock().unwrap();
            if command.fence.expected_job_version != job.version || job.state != "leased" {
                return Err(GenerationStoreFailure::FenceLost);
            }
            job.state = "running".to_owned();
            job.version += 1;
            job.attempt_no += 1;
            job.started_at = Some(Utc::now());
            job.updated_at = Utc::now();
            Ok(CommandOutcome::Applied(job.clone()))
        }

        async fn heartbeat_generation(
            &self,
            command: HeartbeatJob,
        ) -> Result<JobRecord, GenerationStoreFailure> {
            let mut job = self.job.lock().unwrap();
            if command.fence.expected_job_version != job.version || job.state != "running" {
                return Err(GenerationStoreFailure::FenceLost);
            }
            self.heartbeats.fetch_add(1, Ordering::Relaxed);
            job.version += 1;
            job.heartbeat_at = Some(Utc::now());
            job.lease_expires_at = Some(Utc::now() + ChronoDuration::milliseconds(100));
            job.updated_at = Utc::now();
            Ok(job.clone())
        }
    }

    struct RecordingHandler {
        running: AtomicBool,
        release: Notify,
        commit_version: AtomicI64,
        handoff_reason: Mutex<Option<GenerationHandoffReason>>,
    }

    impl RecordingHandler {
        fn new() -> Self {
            Self {
                running: AtomicBool::new(false),
                release: Notify::new(),
                commit_version: AtomicI64::new(0),
                handoff_reason: Mutex::new(None),
            }
        }

        async fn wait_running(&self) {
            while !self.running.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        }
    }

    #[async_trait]
    impl StartedOrchestrationJobHandler for RecordingHandler {
        type Outcome = ();

        async fn run(
            &self,
            _job: StartedOrchestrationJob,
        ) -> Result<Self::Outcome, GenerationHandlerError> {
            self.running.store(true, Ordering::Release);
            self.release.notified().await;
            Ok(())
        }

        async fn commit(
            &self,
            _job: &StartedOrchestrationJob,
            fence: JobFence,
            _outcome: Self::Outcome,
        ) -> GenerationHandlerDisposition {
            self.commit_version
                .store(fence.expected_job_version, Ordering::Release);
            GenerationHandlerDisposition::Committed
        }

        async fn handoff(
            &self,
            _job: &StartedOrchestrationJob,
            _fence: JobFence,
            reason: GenerationHandoffReason,
        ) -> GenerationHandlerDisposition {
            *self.handoff_reason.lock().unwrap() = Some(reason);
            GenerationHandlerDisposition::Committed
        }
    }

    fn id(kind: ResourceKind, suffix: &str) -> ResourceId {
        format!(
            "{}_0198f1c5-0787-75e1-a9e8-d95ca0f3{}",
            kind.descriptor().prefix,
            suffix
        )
        .parse()
        .unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn claimed() -> ClaimedOrchestrationJob {
        let now = Utc::now();
        ClaimedOrchestrationJob {
            job: JobRecord {
                tenant_id: id(ResourceKind::Tenant, "6601").to_string(),
                job_id: id(ResourceKind::Job, "6602").to_string(),
                work_class: WorkClass::Orchestration.as_str().to_owned(),
                owner_kind: ResourceKind::NodeExecution.descriptor().name.to_owned(),
                owner_id: id(ResourceKind::NodeExecution, "6603").to_string(),
                invocation_id: None,
                run_id: Some(id(ResourceKind::Run, "6604").to_string()),
                node_id: Some(id(ResourceKind::NodeExecution, "6603").to_string()),
                state: "leased".to_owned(),
                version: 2,
                attempt_no: 0,
                attempt_limit: 8,
                lease_epoch: 1,
                worker_id: Some(id(ResourceKind::WorkerProcessGeneration, "6605").to_string()),
                lease_token_digest: Some(digest('a').to_string()),
                lease_expires_at: Some(now + ChronoDuration::milliseconds(100)),
                heartbeat_at: Some(now),
                scheduled_at: now,
                retry_at: None,
                deadline: now + ChronoDuration::minutes(5),
                priority: SchedulerPriority::Normal,
                wake_kind: None,
                wake_state: None,
                wake_generation: 0,
                request_digest: digest('b').to_string(),
                result_digest: None,
                effect_key_digest: None,
                quota_reservation_id: Some(id(ResourceKind::UsageReservation, "6606").to_string()),
                payload: TypedPayload::new(1, &json!({"kind":"controller"})).unwrap(),
                started_at: None,
                terminal_at: None,
                created_at: now,
                updated_at: now,
            },
            run_version: 2,
            node_version: 2,
            quota_reservation_id: id(ResourceKind::UsageReservation, "6606").to_string(),
            quota_account_ids: Vec::new(),
        }
    }

    fn pools() -> LocalWorkerPools {
        LocalWorkerPools::new(
            WorkerManifest {
                manifest_version: 1,
                worker_role: "orchestration.executor".to_owned(),
                work_class: WorkClass::Orchestration,
                adapter_runtime_digest: digest('c'),
                protocol_version: 1,
                max_concurrency: 1,
                critical_control_reserved_slots: 1,
            },
            id(ResourceKind::WorkerProcessGeneration, "6605"),
        )
        .unwrap()
    }

    fn active(
        claimed: ClaimedOrchestrationJob,
        shutdown: CancellationToken,
        pools: &LocalWorkerPools,
    ) -> ActiveOrchestrationJob {
        let reservation = pools
            .reserve_claim_capacity(
                WorkClass::Orchestration,
                1,
                ClaimBatchHardLimit::from_profile(&checked_in_hard_limit_profile()).unwrap(),
            )
            .unwrap()
            .unwrap();
        let permit = reservation
            .bind_claimed_jobs(vec![ClaimedJobIdentity {
                job_id: claimed.job.job_id.parse().unwrap(),
                lease_generation: u64::try_from(claimed.job.lease_epoch).unwrap(),
            }])
            .unwrap()
            .pop()
            .unwrap();
        ActiveOrchestrationJob::new(claimed, shutdown, permit)
    }

    fn config() -> OrchestrationExecutorConfig {
        OrchestrationExecutorConfig {
            lease_duration: Duration::from_millis(100),
            lease_milliseconds: 100,
            heartbeat_interval: Duration::from_millis(10),
            timing: OrchestrationExecutorTiming {
                heartbeat_jitter: Duration::ZERO,
                store_retry_backoff: Duration::from_millis(1),
            },
        }
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_updates_the_fence_used_by_terminal_commit() {
        let claimed = claimed();
        let store = Arc::new(RecordingStore {
            job: Mutex::new(claimed.job.clone()),
            heartbeats: AtomicU64::new(0),
        });
        let handler = Arc::new(RecordingHandler::new());
        let pools = pools();
        let executor = Arc::new(
            LeaseFencedOrchestrationExecutor::new(
                Arc::clone(&store),
                Arc::clone(&handler),
                Arc::new(UuidCoordinatorIdentityFactory),
                config(),
            )
            .unwrap(),
        );
        let active = active(claimed, CancellationToken::new(), &pools);
        let execution = tokio::spawn(async move { executor.execute(active).await });
        handler.wait_running().await;
        tokio::time::advance(Duration::from_millis(11)).await;
        tokio::task::yield_now().await;
        assert!(store.heartbeats.load(Ordering::Acquire) >= 1);
        handler.release.notify_one();
        assert_eq!(
            execution.await.unwrap(),
            ExecutionDisposition::GenerationSettled
        );
        assert!(handler.commit_version.load(Ordering::Acquire) >= 4);
        assert_eq!(pools.snapshot().business_available, 1);
    }

    #[tokio::test]
    async fn coordinator_shutdown_is_offered_as_a_durable_handoff_before_abort() {
        let claimed = claimed();
        let store = Arc::new(RecordingStore {
            job: Mutex::new(claimed.job.clone()),
            heartbeats: AtomicU64::new(0),
        });
        let handler = Arc::new(RecordingHandler::new());
        let pools = pools();
        let shutdown = CancellationToken::new();
        let executor = Arc::new(
            LeaseFencedOrchestrationExecutor::new(
                store,
                Arc::clone(&handler),
                Arc::new(UuidCoordinatorIdentityFactory),
                config(),
            )
            .unwrap(),
        );
        let active = active(claimed, shutdown.clone(), &pools);
        let execution = tokio::spawn(async move { executor.execute(active).await });
        handler.wait_running().await;
        shutdown.cancel();
        assert_eq!(
            execution.await.unwrap(),
            ExecutionDisposition::GenerationSettled
        );
        assert_eq!(
            *handler.handoff_reason.lock().unwrap(),
            Some(GenerationHandoffReason::Shutdown)
        );
        assert_eq!(pools.snapshot().business_available, 1);
    }

    #[test]
    fn q1_heartbeat_jitter_preserves_the_strict_lease_margin() {
        let profile = checked_in_hard_limit_profile();
        assert!(OrchestrationExecutorConfig::from_profile(
            &profile,
            OrchestrationExecutorTiming {
                heartbeat_jitter: Duration::from_secs(1),
                store_retry_backoff: Duration::from_millis(100),
            },
        )
        .is_ok());
        assert!(OrchestrationExecutorConfig::from_profile(
            &profile,
            OrchestrationExecutorTiming {
                heartbeat_jitter: Duration::from_secs(2),
                store_retry_backoff: Duration::from_millis(100),
            },
        )
        .is_err());
    }
}
