//! Independent Model Worker claim and execution composition.
//!
//! This crate owns process-local Model capacity and orchestration only. PostgreSQL remains the
//! durable claim/commit authority, Provider traffic remains behind the Egress connector, and
//! Artifact-backed request/output bytes remain behind injected trusted materializers.

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    canonical_digest, HardLimitProfile, ResourceId, ResourceIdError, ResourceKind, Sha256Digest,
    WorkClass,
};
use insight_platform_jobs::JobFence;
use insight_platform_model_adapters::{
    ModelAdapterFailure, ModelAdapterFailureClass, ModelAdapterWorker, ModelExecutionAuthority,
    ModelOutputMaterializer,
};
use insight_platform_models::{
    CanonicalModelRequest, ClaimModelJobs, ModelClaimSlot, ModelExecutionInput,
    ModelExecutionInputMaterial, ModelTurnLimits, ModelWorkerAudit, MODEL_QUOTA_LINES,
};
use insight_platform_postgres::{
    model_turn_repository::ClaimedModelExecution, repository::PgRepository,
};
use insight_platform_worker::{
    ClaimBatchHardLimit, ClaimedJobIdentity, LocalWorkerPoolError, LocalWorkerPools,
};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeSet, error::Error, fmt, sync::Arc, time::Duration};
use tokio::{
    task::{JoinError, JoinSet},
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const MODEL_WORKER_ROLE: &str = "model-worker";

pub trait ModelWorkerIdentityFactory: Send + Sync {
    fn new_resource_id(&self, kind: ResourceKind) -> Result<ResourceId, ModelWorkerIdentityError>;
    fn new_opaque_digest(&self) -> Result<Sha256Digest, ModelWorkerIdentityError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UuidModelWorkerIdentityFactory;

impl ModelWorkerIdentityFactory for UuidModelWorkerIdentityFactory {
    fn new_resource_id(&self, kind: ResourceKind) -> Result<ResourceId, ModelWorkerIdentityError> {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).map_err(ModelWorkerIdentityError::ResourceId)
    }

    fn new_opaque_digest(&self) -> Result<Sha256Digest, ModelWorkerIdentityError> {
        let mut hasher = Sha256::new();
        hasher.update(b"insight.platform/v1/model-worker/opaque-identity\0");
        hasher.update(Uuid::new_v4().as_bytes());
        format!("sha256:{}", lower_hex(&hasher.finalize()))
            .parse()
            .map_err(|_| ModelWorkerIdentityError::Digest)
    }
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[derive(Debug)]
pub enum ModelWorkerIdentityError {
    ResourceId(ResourceIdError),
    Digest,
}

impl fmt::Display for ModelWorkerIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceId(failure) => failure.fmt(formatter),
            Self::Digest => formatter.write_str("generated Model Worker digest is invalid"),
        }
    }
}

impl Error for ModelWorkerIdentityError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelClaimFailure {
    Unavailable,
    FirstWinnerLost,
    Invariant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelClaimBinding {
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub worker_manifest_digest: Sha256Digest,
    pub expected_job_version: u64,
    pub lease_generation: u64,
    pub lease_token_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub quota_reservation_entry_ids: Vec<ResourceId>,
}

pub trait ClaimedModelJob: Send + 'static {
    fn claim_binding(&self) -> Result<ModelClaimBinding, ModelClaimFailure>;
}

impl ClaimedModelJob for ClaimedModelExecution {
    fn claim_binding(&self) -> Result<ModelClaimBinding, ModelClaimFailure> {
        self.request_input
            .validate()
            .map_err(|_| ModelClaimFailure::Invariant)?;
        let projection = self
            .job_projection()
            .map_err(|_| ModelClaimFailure::Invariant)?;
        if projection.state != insight_platform_contracts::JobState::Running
            || projection.version != self.fence.expected_version
            || projection.lease_generation != self.fence.lease_generation
            || projection.lease.as_ref().is_none_or(|lease| {
                lease.worker_process_generation_id != self.fence.worker_process_generation_id
                    || lease.token_digest != self.fence.token_digest
            })
        {
            return Err(ModelClaimFailure::Invariant);
        }
        Ok(ModelClaimBinding {
            tenant_id: self.turn.tenant_id.clone(),
            job_id: projection.job_id,
            worker_process_generation_id: self.fence.worker_process_generation_id.clone(),
            worker_manifest_digest: self
                .turn
                .payload
                .admission
                .provider
                .installed_adapter
                .worker_manifest_digest
                .clone(),
            expected_job_version: self.fence.expected_version,
            lease_generation: self.fence.lease_generation,
            lease_token_digest: self.fence.token_digest.clone(),
            request_digest: self.turn.payload.admission.request_digest.clone(),
            quota_reservation_entry_ids: self.quota_entry_ids.clone(),
        })
    }
}

#[async_trait]
pub trait ModelClaimAuthority: Send + Sync + 'static {
    type Claim: ClaimedModelJob;

    async fn claim_model_jobs(
        &self,
        command: ClaimModelJobs,
    ) -> Result<Vec<Self::Claim>, ModelClaimFailure>;
}

#[async_trait]
impl ModelClaimAuthority for PgRepository {
    type Claim = ClaimedModelExecution;

    async fn claim_model_jobs(
        &self,
        command: ClaimModelJobs,
    ) -> Result<Vec<Self::Claim>, ModelClaimFailure> {
        PgRepository::claim_model_jobs(self, command)
            .await
            .map_err(|failure| match failure {
                insight_platform_postgres::repository::RepositoryError::Database(_) => {
                    ModelClaimFailure::Unavailable
                }
                insight_platform_postgres::repository::RepositoryError::Conflict(_)
                | insight_platform_postgres::repository::RepositoryError::StaleFence
                | insight_platform_postgres::repository::RepositoryError::LeaseExpired => {
                    ModelClaimFailure::FirstWinnerLost
                }
                _ => ModelClaimFailure::Invariant,
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelHeartbeatFailure {
    Unavailable,
    FenceLost,
    Invariant,
}

#[async_trait]
pub trait ModelHeartbeatAuthority: Send + Sync + 'static {
    async fn heartbeat_model_execution(
        &self,
        tenant_id: &ResourceId,
        job_id: &ResourceId,
        fence: &JobFence,
        lease_milliseconds: u64,
    ) -> Result<JobFence, ModelHeartbeatFailure>;
}

#[async_trait]
impl ModelHeartbeatAuthority for PgRepository {
    async fn heartbeat_model_execution(
        &self,
        tenant_id: &ResourceId,
        job_id: &ResourceId,
        fence: &JobFence,
        lease_milliseconds: u64,
    ) -> Result<JobFence, ModelHeartbeatFailure> {
        let record = self
            .heartbeat_job(insight_platform_postgres::repository::HeartbeatJob {
                fence: insight_platform_postgres::repository::JobFence {
                    tenant_id: tenant_id.to_string(),
                    job_id: job_id.to_string(),
                    worker_id: fence.worker_process_generation_id.clone(),
                    lease_epoch: i64::try_from(fence.lease_generation)
                        .map_err(|_| ModelHeartbeatFailure::Invariant)?,
                    expected_job_version: i64::try_from(fence.expected_version)
                        .map_err(|_| ModelHeartbeatFailure::Invariant)?,
                    lease_token_digest: fence.token_digest.clone(),
                },
                lease_milliseconds: i64::try_from(lease_milliseconds)
                    .map_err(|_| ModelHeartbeatFailure::Invariant)?,
            })
            .await
            .map_err(|failure| match failure {
                insight_platform_postgres::repository::RepositoryError::Database(_) => {
                    ModelHeartbeatFailure::Unavailable
                }
                insight_platform_postgres::repository::RepositoryError::Conflict(_)
                | insight_platform_postgres::repository::RepositoryError::StaleFence
                | insight_platform_postgres::repository::RepositoryError::LeaseExpired
                | insight_platform_postgres::repository::RepositoryError::NotFound(_) => {
                    ModelHeartbeatFailure::FenceLost
                }
                _ => ModelHeartbeatFailure::Invariant,
            })?;
        let worker_id = fence.worker_process_generation_id.to_string();
        if record.tenant_id != tenant_id.to_string()
            || record.job_id != job_id.to_string()
            || record.worker_id.as_deref() != Some(worker_id.as_str())
            || record.lease_token_digest.as_deref() != Some(fence.token_digest.as_str())
            || u64::try_from(record.lease_epoch).ok() != Some(fence.lease_generation)
            || record.state != "running"
            || u64::try_from(record.version)
                .ok()
                .is_none_or(|version| version <= fence.expected_version)
            || record.heartbeat_at.is_none()
            || record.lease_expires_at.is_none()
        {
            return Err(ModelHeartbeatFailure::Invariant);
        }
        Ok(JobFence {
            expected_version: u64::try_from(record.version)
                .map_err(|_| ModelHeartbeatFailure::Invariant)?,
            worker_process_generation_id: fence.worker_process_generation_id.clone(),
            lease_generation: fence.lease_generation,
            token_digest: fence.token_digest.clone(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelWorkerDriverTiming {
    pub safety_scan_interval: Duration,
    pub claim_failure_backoff: Duration,
    pub drain_grace: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelWorkerDriverConfig {
    requested_claim_size: usize,
    claim_batch_hard_limit: ClaimBatchHardLimit,
    lease_milliseconds: u64,
    heartbeat_interval: Duration,
    receipt_ttl: Duration,
    timing: ModelWorkerDriverTiming,
    limits: ModelTurnLimits,
}

impl ModelWorkerDriverConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        receipt_ttl: Duration,
        timing: ModelWorkerDriverTiming,
    ) -> Result<Self, ModelWorkerDriverError> {
        profile
            .validate()
            .map_err(|failure| ModelWorkerDriverError::InvalidConfig(failure.to_string()))?;
        let config = Self {
            requested_claim_size: usize::try_from(profile.run_scheduler.claim_batch.q1_default)
                .map_err(|_| {
                    ModelWorkerDriverError::InvalidConfig(
                        "Model claim batch exceeds local address space".to_owned(),
                    )
                })?,
            claim_batch_hard_limit: ClaimBatchHardLimit::from_profile(profile)
                .map_err(ModelWorkerDriverError::LocalPool)?,
            lease_milliseconds: profile.run_scheduler.lease_milliseconds.q1_default,
            heartbeat_interval: Duration::from_millis(
                profile.run_scheduler.heartbeat_milliseconds.q1_default,
            ),
            receipt_ttl,
            timing,
            limits: ModelTurnLimits::from_profile(profile)
                .map_err(|failure| ModelWorkerDriverError::InvalidConfig(failure.to_string()))?,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(self) -> Result<(), ModelWorkerDriverError> {
        if self.requested_claim_size == 0
            || self.lease_milliseconds == 0
            || self.heartbeat_interval.is_zero()
            || self.heartbeat_interval >= Duration::from_millis(self.lease_milliseconds / 3)
            || self.receipt_ttl.is_zero()
            || self.receipt_ttl <= Duration::from_millis(self.lease_milliseconds)
            || ChronoDuration::from_std(self.receipt_ttl).is_err()
            || self.timing.safety_scan_interval.is_zero()
            || self.timing.claim_failure_backoff.is_zero()
            || self.timing.claim_failure_backoff > self.timing.safety_scan_interval
            || self.timing.drain_grace.is_zero()
        {
            return Err(ModelWorkerDriverError::InvalidConfig(
                "Model Worker timing is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

pub struct ExecuteClaimedModelJob<C> {
    pub claim: C,
    pub audit: ModelWorkerAudit,
    pub quota_settlement_entry_ids: Vec<ResourceId>,
    pub worker_manifest_digest: Sha256Digest,
    pub limits: ModelTurnLimits,
    pub heartbeat_interval: Duration,
    pub lease_milliseconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelExecutionDisposition {
    Settled,
    Abandoned,
}

#[async_trait]
pub trait ModelJobCommandExecutor<C>: Send + Sync + 'static {
    async fn execute(&self, command: ExecuteClaimedModelJob<C>) -> ModelExecutionDisposition;
}

#[async_trait]
pub trait ModelRequestMaterializer: Send + Sync {
    async fn materialize(
        &self,
        input: &ModelExecutionInput,
    ) -> Result<CanonicalModelRequest, ModelRequestMaterializationError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct InlineModelRequestMaterializer;

#[async_trait]
impl ModelRequestMaterializer for InlineModelRequestMaterializer {
    async fn materialize(
        &self,
        input: &ModelExecutionInput,
    ) -> Result<CanonicalModelRequest, ModelRequestMaterializationError> {
        input
            .validate()
            .map_err(|_| ModelRequestMaterializationError::InvalidMaterial)?;
        let ModelExecutionInputMaterial::Inline { value } = &input.material else {
            return Err(ModelRequestMaterializationError::ArtifactBrokerRequired);
        };
        serde_json::from_value(value.clone())
            .map_err(|_| ModelRequestMaterializationError::InvalidMaterial)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelRequestMaterializationError {
    InvalidMaterial,
    ArtifactBrokerRequired,
    BrokerUnavailable,
}

pub struct RegisteredModelJobExecutor<R, O, A, H> {
    request_materializer: R,
    worker: Arc<ModelAdapterWorker<O, A>>,
    heartbeat_authority: Arc<H>,
}

impl<R, O, A, H> RegisteredModelJobExecutor<R, O, A, H> {
    pub fn new(
        request_materializer: R,
        worker: Arc<ModelAdapterWorker<O, A>>,
        heartbeat_authority: Arc<H>,
    ) -> Self {
        Self {
            request_materializer,
            worker,
            heartbeat_authority,
        }
    }
}

#[async_trait]
impl<R, O, A, H> ModelJobCommandExecutor<ClaimedModelExecution>
    for RegisteredModelJobExecutor<R, O, A, H>
where
    R: ModelRequestMaterializer + 'static,
    O: ModelOutputMaterializer + 'static,
    A: ModelExecutionAuthority + Send + Sync + 'static,
    A::Error: Send,
    H: ModelHeartbeatAuthority,
{
    async fn execute(
        &self,
        command: ExecuteClaimedModelJob<ClaimedModelExecution>,
    ) -> ModelExecutionDisposition {
        let binding = match command.claim.claim_binding() {
            Ok(binding) => binding,
            Err(_) => return ModelExecutionDisposition::Abandoned,
        };
        let mut fence = JobFence {
            expected_version: binding.expected_job_version,
            worker_process_generation_id: binding.worker_process_generation_id.clone(),
            lease_generation: binding.lease_generation,
            token_digest: binding.lease_token_digest.clone(),
        };
        let heartbeat_interval = command.heartbeat_interval;
        let lease_milliseconds = command.lease_milliseconds;
        let prepare = async {
            let request = self
                .request_materializer
                .materialize(&command.claim.request_input)
                .await
                .map_err(|_| ())?;
            let adapter_job = command
                .claim
                .adapter_job(
                    request,
                    command.worker_manifest_digest,
                    command.audit,
                    command.quota_settlement_entry_ids,
                    command.limits,
                )
                .map_err(|_| ())?;
            self.worker.prepare(adapter_job).await.map_err(|_| ())
        };
        tokio::pin!(prepare);
        let mut heartbeat = tokio::time::interval_at(
            Instant::now()
                .checked_add(heartbeat_interval)
                .unwrap_or_else(Instant::now),
            heartbeat_interval,
        );
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut prepared = loop {
            tokio::select! {
                result = &mut prepare => match result {
                    Ok(prepared) => break prepared,
                    Err(()) => return ModelExecutionDisposition::Abandoned,
                },
                _ = heartbeat.tick() => {
                    match self.heartbeat_authority.heartbeat_model_execution(
                        &binding.tenant_id,
                        &binding.job_id,
                        &fence,
                        lease_milliseconds,
                    ).await {
                        Ok(next) => fence = next,
                        Err(ModelHeartbeatFailure::Unavailable) => {}
                        Err(ModelHeartbeatFailure::FenceLost | ModelHeartbeatFailure::Invariant) => {
                            return ModelExecutionDisposition::Abandoned;
                        }
                    }
                }
            }
        };
        match self
            .heartbeat_authority
            .heartbeat_model_execution(
                &binding.tenant_id,
                &binding.job_id,
                &fence,
                lease_milliseconds,
            )
            .await
        {
            Ok(next) => fence = next,
            Err(ModelHeartbeatFailure::Unavailable) => {}
            Err(ModelHeartbeatFailure::FenceLost | ModelHeartbeatFailure::Invariant) => {
                return ModelExecutionDisposition::Abandoned;
            }
        }
        if prepared.refresh_fence(fence).is_err() {
            return ModelExecutionDisposition::Abandoned;
        }
        match self.worker.commit(prepared).await {
            Ok(_) => ModelExecutionDisposition::Settled,
            Err(_) => ModelExecutionDisposition::Abandoned,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModelWorkerCycleReport {
    pub claimed: u64,
    pub settled: u64,
    pub abandoned: u64,
}

pub struct ModelWorkerDriver<S, E, I> {
    claim_authority: Arc<S>,
    executor: Arc<E>,
    identities: Arc<I>,
    pools: LocalWorkerPools,
    config: ModelWorkerDriverConfig,
}

impl<S, E, I> ModelWorkerDriver<S, E, I>
where
    S: ModelClaimAuthority,
    E: ModelJobCommandExecutor<S::Claim>,
    I: ModelWorkerIdentityFactory + 'static,
{
    pub fn new(
        claim_authority: Arc<S>,
        executor: Arc<E>,
        identities: Arc<I>,
        pools: LocalWorkerPools,
        config: ModelWorkerDriverConfig,
    ) -> Result<Self, ModelWorkerDriverError> {
        config.validate()?;
        let pool = pools.snapshot();
        if pool.worker_role != MODEL_WORKER_ROLE || pool.work_class != WorkClass::Model {
            return Err(ModelWorkerDriverError::WrongWorkerManifest);
        }
        Ok(Self {
            claim_authority,
            executor,
            identities,
            pools,
            config,
        })
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<ModelExecutionDisposition>,
    ) -> Result<usize, ModelWorkerDriverError> {
        let Some(reservation) = self
            .pools
            .reserve_claim_capacity(
                WorkClass::Model,
                self.config.requested_claim_size,
                self.config.claim_batch_hard_limit,
            )
            .map_err(ModelWorkerDriverError::LocalPool)?
        else {
            return Ok(0);
        };
        let claim_limit = reservation.claim_limit();
        let slots = (0..claim_limit)
            .map(|_| self.claim_slot())
            .collect::<Result<Vec<_>, _>>()?;
        let expected_tokens = slots
            .iter()
            .map(|slot| slot.lease_token_digest.clone())
            .collect::<BTreeSet<_>>();
        let claimed = self
            .claim_authority
            .claim_model_jobs(ClaimModelJobs {
                worker_process_generation_id: self.pools.snapshot().worker_process_generation_id,
                limit: u16::try_from(claim_limit)
                    .map_err(|_| ModelWorkerDriverError::InvalidGeneratedCommand)?,
                lease_milliseconds: self.config.lease_milliseconds,
                slots,
            })
            .await
            .map_err(ModelWorkerDriverError::Claim)?;
        if claimed.is_empty() {
            return Ok(0);
        }
        let pool = self.pools.snapshot();
        let bindings = validate_claims(&claimed, &pool, &expected_tokens)?;
        let permits = reservation
            .bind_claimed_jobs(
                bindings
                    .iter()
                    .map(|binding| ClaimedJobIdentity {
                        job_id: binding.job_id.clone(),
                        lease_generation: binding.lease_generation,
                    })
                    .collect(),
            )
            .map_err(ModelWorkerDriverError::LocalPool)?;
        if permits.len() != claimed.len() {
            return Err(ModelWorkerDriverError::CorruptClaim);
        }
        let count = claimed.len();
        for ((claim, binding), permit) in claimed.into_iter().zip(bindings).zip(permits) {
            let command = self.execution_command(claim, &binding)?;
            let executor = Arc::clone(&self.executor);
            active.spawn(async move {
                let _permit = permit;
                executor.execute(command).await
            });
        }
        Ok(count)
    }

    fn claim_slot(&self) -> Result<ModelClaimSlot, ModelWorkerDriverError> {
        Ok(ModelClaimSlot {
            lease_token_digest: self.new_digest()?,
            usage_reservation_id: self.new_id(ResourceKind::UsageReservation)?,
            quota_entry_ids: self.quota_entry_ids()?,
            event_id: self.new_id(ResourceKind::Event)?,
            outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
        })
    }

    fn execution_command(
        &self,
        claim: S::Claim,
        binding: &ModelClaimBinding,
    ) -> Result<ExecuteClaimedModelJob<S::Claim>, ModelWorkerDriverError> {
        let receipt_expires_at = Utc::now()
            .checked_add_signed(
                ChronoDuration::from_std(self.config.receipt_ttl)
                    .map_err(|_| ModelWorkerDriverError::InvalidGeneratedCommand)?,
            )
            .ok_or(ModelWorkerDriverError::InvalidGeneratedCommand)?;
        let audit = ModelWorkerAudit {
            tenant_id: binding.tenant_id.clone(),
            worker_process_generation_id: binding.worker_process_generation_id.clone(),
            receipt_id: self.new_id(ResourceKind::Receipt)?,
            event_id: self.new_id(ResourceKind::Event)?,
            outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: self.new_digest()?,
            request_digest: binding.request_digest.clone(),
            receipt_expires_at,
        };
        audit
            .validate_at(Utc::now())
            .map_err(|_| ModelWorkerDriverError::InvalidGeneratedCommand)?;
        let quota_settlement_entry_ids = self.quota_entry_ids()?;
        if quota_settlement_entry_ids
            .iter()
            .any(|identity| binding.quota_reservation_entry_ids.contains(identity))
        {
            return Err(ModelWorkerDriverError::InvalidGeneratedCommand);
        }
        Ok(ExecuteClaimedModelJob {
            claim,
            audit,
            quota_settlement_entry_ids,
            worker_manifest_digest: binding.worker_manifest_digest.clone(),
            limits: self.config.limits,
            heartbeat_interval: self.config.heartbeat_interval,
            lease_milliseconds: self.config.lease_milliseconds,
        })
    }

    fn quota_entry_ids(&self) -> Result<Vec<ResourceId>, ModelWorkerDriverError> {
        let mut ids = (0..MODEL_QUOTA_LINES)
            .map(|_| self.new_id(ResourceKind::QuotaLedgerEntry))
            .collect::<Result<Vec<_>, _>>()?;
        ids.sort();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ModelWorkerDriverError::InvalidGeneratedCommand);
        }
        Ok(ids)
    }

    fn new_id(&self, kind: ResourceKind) -> Result<ResourceId, ModelWorkerDriverError> {
        self.identities
            .new_resource_id(kind)
            .map_err(ModelWorkerDriverError::Identity)
    }

    fn new_digest(&self) -> Result<Sha256Digest, ModelWorkerDriverError> {
        self.identities
            .new_opaque_digest()
            .map_err(ModelWorkerDriverError::Identity)
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<ModelWorkerCycleReport, ModelWorkerDriverError> {
        let mut active = JoinSet::new();
        let mut report = ModelWorkerCycleReport::default();
        let mut scan =
            tokio::time::interval_at(Instant::now(), self.config.timing.safety_scan_interval);
        scan.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                joined = active.join_next(), if !active.is_empty() => {
                    observe_join(joined, &mut report);
                }
                _ = self.pools.wait_for_business_capacity(), if self.pools.snapshot().business_available == 0 => {}
                _ = scan.tick() => {
                    match self.drive_once(&mut active).await {
                        Ok(claimed) => report.claimed = report.claimed.saturating_add(claimed as u64),
                        Err(ModelWorkerDriverError::Claim(
                            ModelClaimFailure::Unavailable | ModelClaimFailure::FirstWinnerLost,
                        )) => {
                            tokio::select! {
                                _ = cancellation.cancelled() => break,
                                _ = tokio::time::sleep(self.config.timing.claim_failure_backoff) => {}
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
        if tokio::time::timeout(self.config.timing.drain_grace, drain)
            .await
            .is_err()
        {
            return Err(ModelWorkerDriverError::DrainRequiresProcessTermination);
        }
        Ok(report)
    }
}

fn validate_claims<C: ClaimedModelJob>(
    claimed: &[C],
    pool: &insight_platform_worker::LocalWorkerPoolSnapshot,
    expected_tokens: &BTreeSet<Sha256Digest>,
) -> Result<Vec<ModelClaimBinding>, ModelWorkerDriverError> {
    if claimed.len() > expected_tokens.len() {
        return Err(ModelWorkerDriverError::CorruptClaim);
    }
    let mut jobs = BTreeSet::new();
    let mut tokens = BTreeSet::new();
    let mut bindings = Vec::with_capacity(claimed.len());
    for claim in claimed {
        let binding = claim
            .claim_binding()
            .map_err(|_| ModelWorkerDriverError::CorruptClaim)?;
        if binding.worker_process_generation_id != pool.worker_process_generation_id
            || binding.worker_manifest_digest != pool.worker_manifest_digest
            || !expected_tokens.contains(&binding.lease_token_digest)
            || !jobs.insert(binding.job_id.clone())
            || !tokens.insert(binding.lease_token_digest.clone())
            || binding.quota_reservation_entry_ids.len() != MODEL_QUOTA_LINES
            || binding
                .quota_reservation_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || binding
                .quota_reservation_entry_ids
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                != binding.quota_reservation_entry_ids.len()
        {
            return Err(ModelWorkerDriverError::CorruptClaim);
        }
        bindings.push(binding);
    }
    Ok(bindings)
}

fn observe_join(
    joined: Option<Result<ModelExecutionDisposition, JoinError>>,
    report: &mut ModelWorkerCycleReport,
) {
    match joined {
        Some(Ok(ModelExecutionDisposition::Settled)) => {
            report.settled = report.settled.saturating_add(1)
        }
        Some(Ok(ModelExecutionDisposition::Abandoned)) | Some(Err(_)) | None => {
            report.abandoned = report.abandoned.saturating_add(1)
        }
    }
}

#[derive(Debug)]
pub enum ModelWorkerDriverError {
    InvalidConfig(String),
    WrongWorkerManifest,
    InvalidGeneratedCommand,
    CorruptClaim,
    DrainRequiresProcessTermination,
    Identity(ModelWorkerIdentityError),
    LocalPool(LocalWorkerPoolError),
    Claim(ModelClaimFailure),
}

impl fmt::Display for ModelWorkerDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfig(_) => "Model Worker driver configuration is invalid",
            Self::WrongWorkerManifest => "Model Worker manifest is invalid",
            Self::InvalidGeneratedCommand => "Model Worker generated an invalid command",
            Self::CorruptClaim => "Model claim response is inconsistent",
            Self::DrainRequiresProcessTermination => {
                "Model Worker drain grace expired; process-generation termination is required"
            }
            Self::Identity(_) => "Model Worker identity generation failed",
            Self::LocalPool(_) => "Model Worker local pool failed",
            Self::Claim(_) => "Model claim authority failed",
        })
    }
}

impl Error for ModelWorkerDriverError {}

fn materialization_failure(code: &'static str, request_sent: bool) -> ModelAdapterFailure {
    let evidence_digest = canonical_digest(&serde_json::json!({
        "code": code,
        "domain": "model_worker_materialization",
        "schema_version": 1,
    }))
    .ok()
    .and_then(|digest| digest.parse().ok())
    .unwrap_or_else(|| {
        "sha256:0000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .expect("static SHA-256 digest is valid")
    });
    ModelAdapterFailure {
        class: ModelAdapterFailureClass::Permanent,
        safe_code: code.to_owned(),
        safe_message: "Model value materialization failed".to_owned(),
        evidence_digest,
        request_sent,
        retry_at: None,
    }
}

const INLINE_MODEL_RESPONSE_ENVELOPE_OVERHEAD_BYTES: usize = 65_536;

/// Bounded Inline output materializer for manifests that explicitly do not advertise Artifact
/// output support. Production manifests that may exceed the inline limit must inject an Artifact
/// Broker-backed implementation instead.
pub struct InlineModelOutputMaterializer<I> {
    identities: Arc<I>,
    limits: ModelTurnLimits,
}

impl<I> InlineModelOutputMaterializer<I> {
    pub fn new(identities: Arc<I>, limits: ModelTurnLimits) -> Self {
        Self { identities, limits }
    }
}

#[async_trait]
impl<I> ModelOutputMaterializer for InlineModelOutputMaterializer<I>
where
    I: ModelWorkerIdentityFactory,
{
    fn validate_execution(
        &self,
        execution: &insight_platform_model_adapters::ModelAdapterExecutionRequest,
    ) -> Result<(), ModelAdapterFailure> {
        let maximum = usize::try_from(execution.provider.request_limits.maximum_response_bytes)
            .ok()
            .and_then(|bytes| bytes.checked_add(INLINE_MODEL_RESPONSE_ENVELOPE_OVERHEAD_BYTES));
        if maximum.is_none_or(|maximum| maximum > self.limits.inline_value_limits().max_bytes) {
            let mut failure = materialization_failure("model_output_artifact_required", false);
            failure.class = ModelAdapterFailureClass::RejectedBeforeDispatch;
            return Err(failure);
        }
        Ok(())
    }

    async fn materialize(
        &self,
        execution: &insight_platform_model_adapters::ModelAdapterExecutionRequest,
        success: insight_platform_model_adapters::ModelAdapterSuccess,
    ) -> Result<insight_platform_models::ModelOutputValue, ModelAdapterFailure> {
        let value = serde_json::to_value(&success.response)
            .map_err(|_| materialization_failure("model_output_invalid", true))?;
        insight_platform_contracts::ValueRef::Inline {
            value: value.clone(),
        }
        .validate(self.limits.inline_value_limits())
        .map_err(|_| materialization_failure("model_output_artifact_required", true))?;
        let content_digest: Sha256Digest = canonical_digest(&value)
            .map_err(|_| materialization_failure("model_output_invalid", true))?
            .parse()
            .map_err(|_| materialization_failure("model_output_invalid", true))?;
        let validation_evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "content_digest": content_digest,
            "domain": "model_inline_output_validation",
            "schema_digest": execution.request.response_contract.output_schema_digest,
            "stream_evidence": {
                "accepted_delta_bytes": success.stream_evidence.accepted_delta_bytes,
                "accepted_delta_count": success.stream_evidence.accepted_delta_count,
                "terminal_sequence": success.stream_evidence.terminal_sequence,
            },
            "schema_version": 1,
        }))
        .map_err(|_| materialization_failure("model_output_invalid", true))?
        .parse()
        .map_err(|_| materialization_failure("model_output_invalid", true))?;
        Ok(insight_platform_models::ModelOutputValue {
            value_id: self
                .identities
                .new_resource_id(ResourceKind::RunValue)
                .map_err(|_| materialization_failure("model_output_identity_failed", true))?,
            classification: execution.request.classification,
            schema_digest: execution
                .request
                .response_contract
                .output_schema_digest
                .clone(),
            content_digest,
            value: insight_platform_contracts::ValueRef::Inline { value },
            artifact_link_id: None,
            artifact_outputs: Vec::new(),
            response: *success.response,
            validation_evidence_digest,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        checked_in_hard_limit_profile, WorkerManifest, WORKER_MANIFEST_VERSION,
        WORKER_PROTOCOL_VERSION,
    };
    use std::sync::Mutex;

    #[derive(Debug, Clone)]
    struct FakeClaim(ModelClaimBinding);

    impl ClaimedModelJob for FakeClaim {
        fn claim_binding(&self) -> Result<ModelClaimBinding, ModelClaimFailure> {
            Ok(self.0.clone())
        }
    }

    #[derive(Default)]
    struct EmptyAuthority {
        claim: Mutex<Option<ClaimModelJobs>>,
    }

    #[async_trait]
    impl ModelClaimAuthority for EmptyAuthority {
        type Claim = FakeClaim;

        async fn claim_model_jobs(
            &self,
            command: ClaimModelJobs,
        ) -> Result<Vec<Self::Claim>, ModelClaimFailure> {
            *self.claim.lock().unwrap() = Some(command);
            Ok(Vec::new())
        }
    }

    struct UnusedExecutor;

    #[async_trait]
    impl ModelJobCommandExecutor<FakeClaim> for UnusedExecutor {
        async fn execute(
            &self,
            _command: ExecuteClaimedModelJob<FakeClaim>,
        ) -> ModelExecutionDisposition {
            panic!("empty authority must not dispatch")
        }
    }

    struct EchoAuthority {
        worker_manifest_digest: Sha256Digest,
        corrupt_manifest: bool,
    }

    #[async_trait]
    impl ModelClaimAuthority for EchoAuthority {
        type Claim = FakeClaim;

        async fn claim_model_jobs(
            &self,
            command: ClaimModelJobs,
        ) -> Result<Vec<Self::Claim>, ModelClaimFailure> {
            let slot = command.slots.into_iter().next().unwrap();
            Ok(vec![FakeClaim(ModelClaimBinding {
                tenant_id: ResourceId::from_uuid_v7(ResourceKind::Tenant, Uuid::now_v7()).unwrap(),
                job_id: ResourceId::from_uuid_v7(ResourceKind::Job, Uuid::now_v7()).unwrap(),
                worker_process_generation_id: command.worker_process_generation_id,
                worker_manifest_digest: if self.corrupt_manifest {
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .parse()
                        .unwrap()
                } else {
                    self.worker_manifest_digest.clone()
                },
                expected_job_version: 2,
                lease_generation: 1,
                lease_token_digest: slot.lease_token_digest,
                request_digest:
                    "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                        .parse()
                        .unwrap(),
                quota_reservation_entry_ids: slot.quota_entry_ids,
            })])
        }
    }

    struct SettlingExecutor;

    #[async_trait]
    impl ModelJobCommandExecutor<FakeClaim> for SettlingExecutor {
        async fn execute(
            &self,
            command: ExecuteClaimedModelJob<FakeClaim>,
        ) -> ModelExecutionDisposition {
            assert_eq!(command.audit.tenant_id, command.claim.0.tenant_id);
            assert_eq!(command.quota_settlement_entry_ids.len(), MODEL_QUOTA_LINES);
            ModelExecutionDisposition::Settled
        }
    }

    fn model_manifest(max_concurrency: u16) -> WorkerManifest {
        WorkerManifest {
            manifest_version: WORKER_MANIFEST_VERSION,
            worker_role: MODEL_WORKER_ROLE.to_owned(),
            work_class: WorkClass::Model,
            adapter_runtime_digest:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .parse()
                    .unwrap(),
            protocol_version: WORKER_PROTOCOL_VERSION,
            max_concurrency,
            critical_control_reserved_slots: 1,
        }
    }

    fn config() -> ModelWorkerDriverConfig {
        ModelWorkerDriverConfig::from_profile(
            &checked_in_hard_limit_profile(),
            Duration::from_secs(300),
            ModelWorkerDriverTiming {
                safety_scan_interval: Duration::from_millis(100),
                claim_failure_backoff: Duration::from_millis(10),
                drain_grace: Duration::from_secs(1),
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn reserves_local_model_capacity_before_bounded_claim() {
        let authority = Arc::new(EmptyAuthority::default());
        let process_generation =
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap();
        let pools = LocalWorkerPools::new(model_manifest(3), process_generation.clone()).unwrap();
        let driver = ModelWorkerDriver::new(
            Arc::clone(&authority),
            Arc::new(UnusedExecutor),
            Arc::new(UuidModelWorkerIdentityFactory),
            pools,
            config(),
        )
        .unwrap();
        let mut active = JoinSet::new();
        assert_eq!(driver.drive_once(&mut active).await.unwrap(), 0);
        let claim = authority.claim.lock().unwrap().clone().unwrap();
        assert_eq!(claim.worker_process_generation_id, process_generation);
        assert_eq!(claim.limit, 3);
        assert_eq!(claim.slots.len(), 3);
        assert!(claim.slots.iter().all(|slot| {
            slot.quota_entry_ids.len() == MODEL_QUOTA_LINES
                && slot
                    .quota_entry_ids
                    .windows(2)
                    .all(|pair| pair[0] < pair[1])
        }));
    }

    #[tokio::test]
    async fn saturated_model_pool_does_not_touch_claim_authority() {
        let authority = Arc::new(EmptyAuthority::default());
        let process_generation =
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap();
        let pools = LocalWorkerPools::new(model_manifest(1), process_generation).unwrap();
        let reservation = pools
            .reserve_claim_capacity(
                WorkClass::Model,
                1,
                ClaimBatchHardLimit::from_profile(&checked_in_hard_limit_profile()).unwrap(),
            )
            .unwrap()
            .unwrap();
        let driver = ModelWorkerDriver::new(
            Arc::clone(&authority),
            Arc::new(UnusedExecutor),
            Arc::new(UuidModelWorkerIdentityFactory),
            pools,
            config(),
        )
        .unwrap();
        let mut active = JoinSet::new();
        assert_eq!(driver.drive_once(&mut active).await.unwrap(), 0);
        assert!(authority.claim.lock().unwrap().is_none());
        drop(reservation);
    }

    #[tokio::test]
    async fn exact_claim_is_bound_to_one_reserved_permit_before_dispatch() {
        let process_generation =
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap();
        let pools = LocalWorkerPools::new(model_manifest(2), process_generation).unwrap();
        let authority = Arc::new(EchoAuthority {
            worker_manifest_digest: pools.snapshot().worker_manifest_digest,
            corrupt_manifest: false,
        });
        let driver = ModelWorkerDriver::new(
            authority,
            Arc::new(SettlingExecutor),
            Arc::new(UuidModelWorkerIdentityFactory),
            pools.clone(),
            config(),
        )
        .unwrap();
        let mut active = JoinSet::new();
        assert_eq!(driver.drive_once(&mut active).await.unwrap(), 1);
        assert_eq!(
            active.join_next().await.unwrap().unwrap(),
            ModelExecutionDisposition::Settled
        );
        assert_eq!(pools.snapshot().business_available, 2);
    }

    #[tokio::test]
    async fn manifest_drift_in_claim_response_fails_closed() {
        let process_generation =
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap();
        let pools = LocalWorkerPools::new(model_manifest(1), process_generation).unwrap();
        let authority = Arc::new(EchoAuthority {
            worker_manifest_digest: pools.snapshot().worker_manifest_digest,
            corrupt_manifest: true,
        });
        let driver = ModelWorkerDriver::new(
            authority,
            Arc::new(SettlingExecutor),
            Arc::new(UuidModelWorkerIdentityFactory),
            pools,
            config(),
        )
        .unwrap();
        let mut active = JoinSet::new();
        assert!(matches!(
            driver.drive_once(&mut active).await,
            Err(ModelWorkerDriverError::CorruptClaim)
        ));
        assert!(active.is_empty());
    }

    #[test]
    fn wrong_worker_role_is_rejected_at_composition() {
        let authority = Arc::new(EmptyAuthority::default());
        let process_generation =
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap();
        let mut manifest = model_manifest(1);
        manifest.worker_role = "capability-worker".to_owned();
        let pools = LocalWorkerPools::new(manifest, process_generation).unwrap();
        assert!(matches!(
            ModelWorkerDriver::new(
                authority,
                Arc::new(UnusedExecutor),
                Arc::new(UuidModelWorkerIdentityFactory),
                pools,
                config(),
            ),
            Err(ModelWorkerDriverError::WrongWorkerManifest)
        ));
    }
}
