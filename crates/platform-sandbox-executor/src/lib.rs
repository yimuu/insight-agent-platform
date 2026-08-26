//! Dedicated Sandbox Executor process composition.
//!
//! This role is the only process allowed to combine an untrusted-code backend with local claim
//! capacity. It deliberately has no SQL, object-store, general HTTP, or Secret Manager client;
//! production authority and broker access are injected through authenticated internal ports.

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    HardLimitProfile, ResourceId, ResourceIdError, ResourceKind, Sha256Digest, TraceFlags,
    TraceIdentityV1, WorkClass,
};
use insight_platform_rpc_trace::{scope_trace, RpcTraceContext};
use insight_platform_sandbox::{
    ClaimSandboxJobs, ClaimedSandboxJob, ExecuteSandboxJob, SandboxClaimAuthority,
    SandboxClaimFailure, SandboxExecutionAuthority, SandboxExecutionControlRouter,
    SandboxExecutorWorker, SandboxWorkerAuditBundle, SandboxWorkerCommitIdentity,
    SANDBOX_QUOTA_LINES,
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

mod gvisor_backend;
mod gvisor_broker;
mod gvisor_kubernetes;
mod gvisor_kubernetes_backend;

pub use gvisor_backend::*;
pub use gvisor_broker::*;
pub use gvisor_kubernetes::*;
pub use gvisor_kubernetes_backend::*;

pub trait ExecutorIdentityFactory: Send + Sync {
    fn new_resource_id(&self, kind: ResourceKind) -> Result<ResourceId, ExecutorIdentityError>;
    fn new_lease_token_digest(&self) -> Result<Sha256Digest, ExecutorIdentityError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UuidExecutorIdentityFactory;

impl ExecutorIdentityFactory for UuidExecutorIdentityFactory {
    fn new_resource_id(&self, kind: ResourceKind) -> Result<ResourceId, ExecutorIdentityError> {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).map_err(ExecutorIdentityError::ResourceId)
    }

    fn new_lease_token_digest(&self) -> Result<Sha256Digest, ExecutorIdentityError> {
        let mut hasher = Sha256::new();
        hasher.update(b"insight.platform/v1/sandbox-executor/lease-token\0");
        hasher.update(Uuid::new_v4().as_bytes());
        format!("sha256:{}", lower_hex(&hasher.finalize()))
            .parse()
            .map_err(|_| ExecutorIdentityError::Digest)
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
pub enum ExecutorIdentityError {
    ResourceId(ResourceIdError),
    Digest,
}

impl fmt::Display for ExecutorIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceId(failure) => failure.fmt(formatter),
            Self::Digest => formatter.write_str("generated Executor digest is invalid"),
        }
    }
}

impl Error for ExecutorIdentityError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxExecutionDisposition {
    Settled,
    Abandoned,
}

#[async_trait]
pub trait SandboxJobCommandExecutor: Send + Sync + 'static {
    async fn execute(&self, command: ExecuteSandboxJob) -> SandboxExecutionDisposition;
}

pub struct RegisteredSandboxJobExecutor<A> {
    worker: Arc<SandboxExecutorWorker<A>>,
    control_router: Arc<SandboxExecutionControlRouter>,
}

impl<A> RegisteredSandboxJobExecutor<A> {
    pub fn new(
        worker: Arc<SandboxExecutorWorker<A>>,
        control_router: Arc<SandboxExecutionControlRouter>,
    ) -> Self {
        Self {
            worker,
            control_router,
        }
    }
}

#[async_trait]
impl<A> SandboxJobCommandExecutor for RegisteredSandboxJobExecutor<A>
where
    A: SandboxExecutionAuthority + 'static,
    A::Error: Send,
{
    async fn execute(&self, command: ExecuteSandboxJob) -> SandboxExecutionDisposition {
        match self
            .worker
            .execute_registered(command, self.control_router.as_ref())
            .await
        {
            Ok(_) => SandboxExecutionDisposition::Settled,
            Err(_) => SandboxExecutionDisposition::Abandoned,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxExecutorDriverTiming {
    pub safety_scan_interval: Duration,
    pub claim_failure_backoff: Duration,
    pub drain_grace: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxExecutorDriverConfig {
    requested_claim_size: usize,
    claim_batch_hard_limit: ClaimBatchHardLimit,
    lease_milliseconds: u64,
    receipt_ttl: Duration,
    minimum_receipt_ttl: Duration,
    timing: SandboxExecutorDriverTiming,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(any())]
pub struct ManagedMcpSandboxSessionRecoveryTiming {
    pub scan_interval: Duration,
    pub scan_jitter: Duration,
    pub failure_backoff: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(any())]
pub struct ManagedMcpSandboxSessionRecoveryConfig {
    shard: ManagedMcpSandboxSessionRecoveryShard,
    batch_size: u16,
    maximum_shards: u16,
    limits: SandboxCommandLimits,
    receipt_ttl: Duration,
    minimum_receipt_ttl: Duration,
    timing: ManagedMcpSandboxSessionRecoveryTiming,
}

#[cfg(any())]
impl ManagedMcpSandboxSessionRecoveryConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        shard: ManagedMcpSandboxSessionRecoveryShard,
        receipt_ttl: Duration,
        timing: ManagedMcpSandboxSessionRecoveryTiming,
    ) -> Result<Self, SandboxExecutorDriverError> {
        profile
            .validate()
            .map_err(|failure| SandboxExecutorDriverError::InvalidConfig(failure.to_string()))?;
        let config = Self {
            shard,
            batch_size: u16::try_from(profile.control_data.recovery_batch.q1_default).map_err(
                |_| {
                    SandboxExecutorDriverError::InvalidConfig(
                        "Managed MCP recovery batch exceeds u16".to_owned(),
                    )
                },
            )?,
            maximum_shards: u16::try_from(profile.control_data.recovery_shards.hard_max).map_err(
                |_| {
                    SandboxExecutorDriverError::InvalidConfig(
                        "Managed MCP recovery shard count exceeds u16".to_owned(),
                    )
                },
            )?,
            limits: SandboxCommandLimits::from_profile(profile).map_err(|failure| {
                SandboxExecutorDriverError::InvalidConfig(failure.to_string())
            })?,
            receipt_ttl,
            minimum_receipt_ttl: Duration::from_secs(
                profile
                    .capability_sandbox
                    .wall_seconds
                    .hard_max
                    .saturating_add(profile.capability_sandbox.cleanup_seconds.hard_max),
            ),
            timing,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(self) -> Result<(), SandboxExecutorDriverError> {
        self.shard.validate(self.maximum_shards).map_err(|_| {
            SandboxExecutorDriverError::InvalidConfig(
                "Managed MCP recovery shard is invalid".to_owned(),
            )
        })?;
        if self.batch_size == 0
            || self.receipt_ttl.is_zero()
            || self.receipt_ttl <= self.minimum_receipt_ttl
            || ChronoDuration::from_std(self.receipt_ttl).is_err()
            || self.timing.scan_interval.is_zero()
            || self.timing.scan_jitter >= self.timing.scan_interval
            || self.timing.failure_backoff.is_zero()
            || self.timing.failure_backoff > self.timing.scan_interval
        {
            return Err(SandboxExecutorDriverError::InvalidConfig(
                "Managed MCP recovery timing is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxExecutorBinding {
    pub isolation_backend_contract_digest: Sha256Digest,
    pub executor_identity_digest: Sha256Digest,
    pub attestor_route: insight_platform_sandbox::NodeAttestorRoute,
}

impl SandboxExecutorDriverConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        receipt_ttl: Duration,
        timing: SandboxExecutorDriverTiming,
    ) -> Result<Self, SandboxExecutorDriverError> {
        profile
            .validate()
            .map_err(|failure| SandboxExecutorDriverError::InvalidConfig(failure.to_string()))?;
        let config = Self {
            requested_claim_size: usize::try_from(profile.run_scheduler.claim_batch.q1_default)
                .map_err(|_| {
                    SandboxExecutorDriverError::InvalidConfig(
                        "claim batch exceeds address space".to_owned(),
                    )
                })?,
            claim_batch_hard_limit: ClaimBatchHardLimit::from_profile(profile)
                .map_err(SandboxExecutorDriverError::LocalPool)?,
            lease_milliseconds: profile.run_scheduler.lease_milliseconds.q1_default,
            receipt_ttl,
            minimum_receipt_ttl: Duration::from_secs(
                profile
                    .capability_sandbox
                    .wall_seconds
                    .hard_max
                    .saturating_add(profile.capability_sandbox.cleanup_seconds.hard_max),
            ),
            timing,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(self) -> Result<(), SandboxExecutorDriverError> {
        if self.requested_claim_size == 0
            || self.lease_milliseconds == 0
            || self.receipt_ttl.is_zero()
            || self.receipt_ttl <= self.minimum_receipt_ttl
            || ChronoDuration::from_std(self.receipt_ttl).is_err()
            || self.timing.safety_scan_interval.is_zero()
            || self.timing.claim_failure_backoff.is_zero()
            || self.timing.claim_failure_backoff > self.timing.safety_scan_interval
            || self.timing.drain_grace.is_zero()
        {
            return Err(SandboxExecutorDriverError::InvalidConfig(
                "Sandbox Executor timing is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SandboxExecutorCycleReport {
    pub claimed: u64,
    pub settled: u64,
    pub abandoned: u64,
}

pub struct SandboxExecutorDriver<S, E, I> {
    claim_authority: Arc<S>,
    executor: Arc<E>,
    identities: Arc<I>,
    pools: LocalWorkerPools,
    binding: SandboxExecutorBinding,
    config: SandboxExecutorDriverConfig,
}

impl<S, E, I> SandboxExecutorDriver<S, E, I>
where
    S: SandboxClaimAuthority,
    E: SandboxJobCommandExecutor,
    I: ExecutorIdentityFactory + 'static,
{
    pub fn new(
        claim_authority: Arc<S>,
        executor: Arc<E>,
        identities: Arc<I>,
        pools: LocalWorkerPools,
        binding: SandboxExecutorBinding,
        config: SandboxExecutorDriverConfig,
    ) -> Result<Self, SandboxExecutorDriverError> {
        config.validate()?;
        let pool = pools.snapshot();
        if pool.work_class != WorkClass::Sandbox || pool.critical_control_capacity == 0 {
            return Err(SandboxExecutorDriverError::WrongWorkerManifest);
        }
        Ok(Self {
            claim_authority,
            executor,
            identities,
            pools,
            binding,
            config,
        })
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<SandboxExecutionDisposition>,
    ) -> Result<usize, SandboxExecutorDriverError> {
        let Some(reservation) = self
            .pools
            .reserve_claim_capacity(
                WorkClass::Sandbox,
                self.config.requested_claim_size,
                self.config.claim_batch_hard_limit,
            )
            .map_err(SandboxExecutorDriverError::LocalPool)?
        else {
            return Ok(0);
        };
        let claim_limit = reservation.claim_limit();
        let lease_token_digests = (0..claim_limit)
            .map(|_| self.identities.new_lease_token_digest())
            .collect::<Result<Vec<_>, _>>()
            .map_err(SandboxExecutorDriverError::Identity)?;
        let expected_tokens = lease_token_digests.iter().cloned().collect::<BTreeSet<_>>();
        let claim_trace =
            RpcTraceContext::start(TraceIdentityV1::generate(), TraceFlags::NotSampled)
                .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?;
        let claimed = scope_trace(
            claim_trace,
            self.claim_authority.claim_sandbox_jobs(ClaimSandboxJobs {
                worker_process_generation_id: self.pools.snapshot().worker_process_generation_id,
                worker_manifest_digest: self.pools.snapshot().worker_manifest_digest,
                isolation_backend_contract_digest: self
                    .binding
                    .isolation_backend_contract_digest
                    .clone(),
                executor_identity_digest: self.binding.executor_identity_digest.clone(),
                attestor_route: self.binding.attestor_route.clone(),
                limit: u16::try_from(claim_limit)
                    .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?,
                lease_milliseconds: self.config.lease_milliseconds,
                lease_token_digests,
            }),
        )
        .await
        .map_err(SandboxExecutorDriverError::Claim)?;
        if claimed.is_empty() {
            return Ok(0);
        }
        let pool = self.pools.snapshot();
        let identities = validate_claims(&claimed, &pool, &expected_tokens)?;
        let permits = reservation
            .bind_claimed_jobs(identities)
            .map_err(SandboxExecutorDriverError::LocalPool)?;
        if permits.len() != claimed.len() {
            return Err(SandboxExecutorDriverError::CorruptClaim);
        }
        let count = claimed.len();
        for (claimed, permit) in claimed.into_iter().zip(permits) {
            let trace = RpcTraceContext::start(claimed.trace, TraceFlags::NotSampled)
                .map_err(|_| SandboxExecutorDriverError::CorruptClaim)?;
            let command = self.execution_command(claimed)?;
            let executor = Arc::clone(&self.executor);
            active.spawn(async move {
                let _permit = permit;
                scope_trace(trace, executor.execute(command)).await
            });
        }
        Ok(count)
    }

    fn execution_command(
        &self,
        claimed: ClaimedSandboxJob,
    ) -> Result<ExecuteSandboxJob, SandboxExecutorDriverError> {
        let expires_at = Utc::now()
            .checked_add_signed(
                ChronoDuration::from_std(self.config.receipt_ttl)
                    .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?,
            )
            .ok_or(SandboxExecutorDriverError::InvalidGeneratedCommand)?;
        let worker_process_generation_id = self.pools.snapshot().worker_process_generation_id;
        let audits = SandboxWorkerAuditBundle {
            preparing: self.commit_identity(
                &claimed.request.tenant_id,
                &worker_process_generation_id,
                expires_at,
            )?,
            starting: self.commit_identity(
                &claimed.request.tenant_id,
                &worker_process_generation_id,
                expires_at,
            )?,
            running: self.commit_identity(
                &claimed.request.tenant_id,
                &worker_process_generation_id,
                expires_at,
            )?,
            collecting: self.commit_identity(
                &claimed.request.tenant_id,
                &worker_process_generation_id,
                expires_at,
            )?,
            cancelling: self.commit_identity(
                &claimed.request.tenant_id,
                &worker_process_generation_id,
                expires_at,
            )?,
            terminal: self.commit_identity(
                &claimed.request.tenant_id,
                &worker_process_generation_id,
                expires_at,
            )?,
        };
        let mut quota_entry_ids = (0..SANDBOX_QUOTA_LINES)
            .map(|_| {
                self.identities
                    .new_resource_id(ResourceKind::QuotaLedgerEntry)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(SandboxExecutorDriverError::Identity)?;
        quota_entry_ids.sort();
        Ok(ExecuteSandboxJob {
            executor_identity_digest: self.binding.executor_identity_digest.clone(),
            attestor_route: self.binding.attestor_route.clone(),
            request: claimed.request,
            fence: claimed.fence,
            audits,
            usage_reservation_id: claimed.usage_reservation_id,
            quota_entry_ids,
        })
    }

    fn commit_identity(
        &self,
        tenant_id: &ResourceId,
        worker_process_generation_id: &ResourceId,
        receipt_expires_at: chrono::DateTime<Utc>,
    ) -> Result<SandboxWorkerCommitIdentity, SandboxExecutorDriverError> {
        Ok(SandboxWorkerCommitIdentity {
            tenant_id: tenant_id.clone(),
            worker_process_generation_id: worker_process_generation_id.clone(),
            receipt_id: self.new_id(ResourceKind::Receipt)?,
            event_id: self.new_id(ResourceKind::Event)?,
            outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: self
                .identities
                .new_lease_token_digest()
                .map_err(SandboxExecutorDriverError::Identity)?,
            receipt_expires_at,
        })
    }

    fn new_id(&self, kind: ResourceKind) -> Result<ResourceId, SandboxExecutorDriverError> {
        self.identities
            .new_resource_id(kind)
            .map_err(SandboxExecutorDriverError::Identity)
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<SandboxExecutorCycleReport, SandboxExecutorDriverError> {
        let mut active = JoinSet::new();
        let mut report = SandboxExecutorCycleReport::default();
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
                        Err(SandboxExecutorDriverError::Claim(SandboxClaimFailure::Unavailable | SandboxClaimFailure::FirstWinnerLost)) => {
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
            return Err(SandboxExecutorDriverError::DrainRequiresProcessTermination);
        }
        Ok(report)
    }
}

/// Long-lived Managed MCP session executor. The future MUST remain pending while the exact
/// activated session owns physical resources; the driver retains the shared Sandbox permit until
/// this future reports a durable terminal/recovery disposition.
#[async_trait]
#[cfg(any())]
pub trait ManagedMcpSandboxSessionCommandExecutor: Send + Sync + 'static {
    async fn execute(
        &self,
        command: ExecuteManagedMcpSandboxSession,
        cancellation: CancellationToken,
    ) -> SandboxExecutionDisposition;
}

/// Production long-lived session supervisor. A transient Provider observation error never becomes
/// an `Exited` fact: the supervisor first destroys the exact prepared instance and only then asks
/// the durable authority to rebuild the logical generation.
#[cfg(any())]
pub struct RegisteredManagedMcpSandboxSessionExecutor<A, P, I> {
    worker: ManagedMcpSandboxSessionWorker<A, P>,
    authority: Arc<A>,
    provider: Arc<P>,
    identities: Arc<I>,
    limits: SandboxCommandLimits,
    heartbeat: SandboxHeartbeatConfig,
    receipt_ttl: Duration,
}

#[cfg(any())]
impl<A, P, I> RegisteredManagedMcpSandboxSessionExecutor<A, P, I>
where
    A: ManagedMcpSandboxSessionExecutionAuthority,
    P: ManagedMcpSandboxSessionProvider,
    I: ExecutorIdentityFactory,
{
    pub fn new(
        authority: Arc<A>,
        provider: Arc<P>,
        identities: Arc<I>,
        limits: SandboxCommandLimits,
        heartbeat: SandboxHeartbeatConfig,
        receipt_ttl: Duration,
    ) -> Result<Self, SandboxExecutorDriverError> {
        if receipt_ttl.is_zero() || ChronoDuration::from_std(receipt_ttl).is_err() {
            return Err(SandboxExecutorDriverError::InvalidConfig(
                "Managed MCP terminal receipt TTL is invalid".to_owned(),
            ));
        }
        Ok(Self {
            worker: ManagedMcpSandboxSessionWorker::with_heartbeat(
                Arc::clone(&authority),
                Arc::clone(&provider),
                limits,
                heartbeat,
            ),
            authority,
            provider,
            identities,
            limits,
            heartbeat,
            receipt_ttl,
        })
    }

    async fn supervise(
        &self,
        mut session: insight_platform_sandbox::EstablishedManagedMcpSandboxSession,
        cancellation: CancellationToken,
    ) -> SandboxExecutionDisposition {
        let mut heartbeat = tokio::time::interval_at(
            Instant::now() + self.heartbeat.interval(),
            self.heartbeat.interval(),
        );
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let until_deadline = (session.request.deadline - Utc::now())
            .to_std()
            .unwrap_or(Duration::ZERO);
        let deadline = tokio::time::sleep(until_deadline);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    return self.terminalize(session, "executor_shutdown", None).await;
                }
                _ = &mut deadline => {
                    return self.terminalize(session, "session_deadline", None).await;
                }
                _ = heartbeat.tick() => {
                    let observation = self.provider.observe_exact(
                        &session.request,
                        &session.fence,
                        &session.prepared,
                        &session.activated,
                    ).await;
                    match observation {
                        Ok(evidence) => {
                            if evidence.validate_for(
                                &session.request,
                                &session.fence,
                                &session.prepared,
                                &session.activated,
                                Utc::now(),
                            ).is_err() {
                                return self.terminalize(
                                    session,
                                    "provider_observation_invalid",
                                    None,
                                ).await;
                            }
                            if evidence.liveness == ManagedMcpSandboxSessionLiveness::Exited {
                                return self.terminalize(
                                    session,
                                    "provider_observed_exit",
                                    Some(&evidence),
                                ).await;
                            }
                        }
                        Err(_) => {
                            return self.terminalize(
                                session,
                                "provider_observation_unavailable",
                                None,
                            ).await;
                        }
                    }
                    let heartbeat = HeartbeatSandboxExecution {
                        tenant_id: session.request.identity.tenant_id.clone(),
                        sandbox_job_id: session.request.identity.sandbox_job_id.clone(),
                        job_id: session.request.identity.physical_job_id.clone(),
                        fence: session.fence.clone(),
                        lease_milliseconds: self.heartbeat.lease_milliseconds(),
                    };
                    let decision = match self
                        .authority
                        .heartbeat_managed_mcp_sandbox_session(heartbeat)
                        .await
                    {
                        Ok(decision) => decision,
                        Err(_) => {
                            return self.terminalize(
                                session,
                                "durable_heartbeat_unavailable",
                                None,
                            ).await;
                        }
                    };
                    if session.apply_running_heartbeat(&decision, self.limits).is_err() {
                        return self.terminalize(
                            session,
                            "durable_heartbeat_invalid",
                            None,
                        ).await;
                    }
                }
            }
        }
    }

    async fn terminalize(
        &self,
        mut session: insight_platform_sandbox::EstablishedManagedMcpSandboxSession,
        reason: &'static str,
        observation: Option<&ManagedMcpSandboxSessionLivenessEvidence>,
    ) -> SandboxExecutionDisposition {
        let cleanup = loop {
            match self
                .provider
                .destroy_exact(&session.request, &session.fence, Some(&session.prepared))
                .await
            {
                Ok(ManagedMcpSandboxSessionCleanupOutcome::Destroyed(evidence))
                    if evidence
                        .validate_for(
                            &session.request,
                            &session.fence,
                            Some(&session.prepared),
                            Utc::now(),
                        )
                        .is_ok() =>
                {
                    break evidence.cleanup;
                }
                Ok(_) | Err(_) => {
                    let heartbeat = HeartbeatSandboxExecution {
                        tenant_id: session.request.identity.tenant_id.clone(),
                        sandbox_job_id: session.request.identity.sandbox_job_id.clone(),
                        job_id: session.request.identity.physical_job_id.clone(),
                        fence: session.fence.clone(),
                        lease_milliseconds: self.heartbeat.lease_milliseconds(),
                    };
                    if let Ok(decision) = self
                        .authority
                        .heartbeat_managed_mcp_sandbox_session(heartbeat)
                        .await
                    {
                        let _ = session.apply_running_heartbeat(&decision, self.limits);
                    }
                    tokio::time::sleep(self.heartbeat.interval()).await;
                }
            }
        };
        let loss_evidence_digest = match canonical_digest(&serde_json::json!({
            "cleanup_evidence_digest": cleanup.evidence_digest,
            "domain": "managed_mcp_sandbox_session_supervisor_loss",
            "identity": session.request.identity,
            "observation_evidence_digest": observation.map(|value| &value.evidence_digest),
            "reason": reason,
            "request_digest": session.request.request_digest,
            "schema_version": 1,
        }))
        .ok()
        .and_then(|value| value.parse().ok())
        {
            Some(digest) => digest,
            None => return SandboxExecutionDisposition::Abandoned,
        };
        let now = Utc::now();
        let receipt_expires_at = match ChronoDuration::from_std(self.receipt_ttl)
            .ok()
            .and_then(|ttl| now.checked_add_signed(ttl))
        {
            Some(expires_at) => expires_at,
            None => return SandboxExecutionDisposition::Abandoned,
        };
        let mut quota_entry_ids = match (0..SANDBOX_QUOTA_LINES)
            .map(|_| {
                self.identities
                    .new_resource_id(ResourceKind::QuotaLedgerEntry)
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(ids) => ids,
            Err(_) => return SandboxExecutionDisposition::Abandoned,
        };
        quota_entry_ids.sort();
        let mut command = match self.terminal_command(
            &session,
            cleanup.clone(),
            loss_evidence_digest,
            quota_entry_ids,
            receipt_expires_at,
        ) {
            Ok(command) => command,
            Err(_) => return SandboxExecutionDisposition::Abandoned,
        };
        command.audit.request_digest = match command.canonical_request_digest() {
            Ok(digest) => digest,
            Err(_) => return SandboxExecutionDisposition::Abandoned,
        };
        if command.validate_at(Utc::now()).is_err() {
            return SandboxExecutionDisposition::Abandoned;
        }
        let outcome = match self
            .authority
            .commit_managed_mcp_sandbox_session_lost(command)
            .await
        {
            Ok(outcome) => outcome,
            Err(_) => return SandboxExecutionDisposition::Abandoned,
        };
        let decision = match &outcome {
            CommandOutcome::Applied(decision) | CommandOutcome::Replayed(decision) => decision,
        };
        if session
            .validate_lost_decision(decision, &cleanup, self.limits)
            .is_err()
        {
            return SandboxExecutionDisposition::Abandoned;
        }
        SandboxExecutionDisposition::Settled
    }

    fn terminal_command(
        &self,
        session: &insight_platform_sandbox::EstablishedManagedMcpSandboxSession,
        cleanup: insight_platform_sandbox::SandboxCleanupEvidence,
        session_loss_evidence_digest: Sha256Digest,
        quota_entry_ids: Vec<ResourceId>,
        receipt_expires_at: chrono::DateTime<Utc>,
    ) -> Result<CommitManagedMcpSandboxSessionLost, ExecutorIdentityError> {
        Ok(CommitManagedMcpSandboxSessionLost {
            audit: SandboxWorkerAudit {
                tenant_id: session.request.identity.tenant_id.clone(),
                worker_process_generation_id: session.fence.worker_process_generation_id.clone(),
                receipt_id: self.identities.new_resource_id(ResourceKind::Receipt)?,
                event_id: self.identities.new_resource_id(ResourceKind::Event)?,
                outbox_id: self.identities.new_resource_id(ResourceKind::OutboxEvent)?,
                idempotency_key_digest: self.identities.new_lease_token_digest()?,
                request_digest: session_loss_evidence_digest.clone(),
                receipt_expires_at,
            },
            identity: session.request.identity.clone(),
            fence: session.fence.clone(),
            cleanup,
            session_loss_evidence_digest,
            usage_reservation_id: session.usage_reservation_id.clone(),
            quota_entry_ids,
        })
    }
}

#[async_trait]
#[cfg(any())]
impl<A, P, I> ManagedMcpSandboxSessionCommandExecutor
    for RegisteredManagedMcpSandboxSessionExecutor<A, P, I>
where
    A: ManagedMcpSandboxSessionExecutionAuthority + 'static,
    P: ManagedMcpSandboxSessionProvider + 'static,
    I: ExecutorIdentityFactory + 'static,
{
    async fn execute(
        &self,
        command: ExecuteManagedMcpSandboxSession,
        cancellation: CancellationToken,
    ) -> SandboxExecutionDisposition {
        match self.worker.establish(command).await {
            Ok(session) => self.supervise(session, cancellation).await,
            Err(_) => SandboxExecutionDisposition::Abandoned,
        }
    }
}

/// Dedicated claim loop for Managed MCP subscription sessions. It shares `LocalWorkerPools` with
/// finite Sandbox execution, but uses the closed Managed-session claim authority so neither lane
/// can decode the other's payload shape.
#[cfg(any())]
pub struct ManagedMcpSandboxSessionExecutorDriver<S, E, I> {
    claim_authority: Arc<S>,
    executor: Arc<E>,
    identities: Arc<I>,
    pools: LocalWorkerPools,
    binding: SandboxExecutorBinding,
    config: SandboxExecutorDriverConfig,
}

#[cfg(any())]
impl<S, E, I> ManagedMcpSandboxSessionExecutorDriver<S, E, I>
where
    S: ManagedMcpSandboxSessionClaimAuthority,
    E: ManagedMcpSandboxSessionCommandExecutor,
    I: ExecutorIdentityFactory + 'static,
{
    pub fn new(
        claim_authority: Arc<S>,
        executor: Arc<E>,
        identities: Arc<I>,
        pools: LocalWorkerPools,
        binding: SandboxExecutorBinding,
        config: SandboxExecutorDriverConfig,
    ) -> Result<Self, SandboxExecutorDriverError> {
        config.validate()?;
        let pool = pools.snapshot();
        if pool.work_class != WorkClass::Sandbox || pool.critical_control_capacity == 0 {
            return Err(SandboxExecutorDriverError::WrongWorkerManifest);
        }
        Ok(Self {
            claim_authority,
            executor,
            identities,
            pools,
            binding,
            config,
        })
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<SandboxExecutionDisposition>,
        cancellation: &CancellationToken,
    ) -> Result<usize, SandboxExecutorDriverError> {
        let Some(reservation) = self
            .pools
            .reserve_claim_capacity(
                WorkClass::Sandbox,
                self.config.requested_claim_size,
                self.config.claim_batch_hard_limit,
            )
            .map_err(SandboxExecutorDriverError::LocalPool)?
        else {
            return Ok(0);
        };
        let claim_limit = reservation.claim_limit();
        let lease_token_digests = (0..claim_limit)
            .map(|_| self.identities.new_lease_token_digest())
            .collect::<Result<Vec<_>, _>>()
            .map_err(SandboxExecutorDriverError::Identity)?;
        let expected_tokens = lease_token_digests.iter().cloned().collect::<BTreeSet<_>>();
        let claimed = self
            .claim_authority
            .claim_managed_mcp_sandbox_sessions(ClaimSandboxJobs {
                worker_process_generation_id: self.pools.snapshot().worker_process_generation_id,
                worker_manifest_digest: self.pools.snapshot().worker_manifest_digest,
                isolation_backend_contract_digest: self
                    .binding
                    .isolation_backend_contract_digest
                    .clone(),
                executor_identity_digest: self.binding.executor_identity_digest.clone(),
                attestor_route: self.binding.attestor_route.clone(),
                limit: u16::try_from(claim_limit)
                    .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?,
                lease_milliseconds: self.config.lease_milliseconds,
                lease_token_digests,
            })
            .await
            .map_err(SandboxExecutorDriverError::Claim)?;
        if claimed.is_empty() {
            return Ok(0);
        }
        let pool = self.pools.snapshot();
        let identities =
            validate_managed_mcp_session_claims(&claimed, &pool, &expected_tokens, &self.binding)?;
        let permits = reservation
            .bind_claimed_jobs(identities)
            .map_err(SandboxExecutorDriverError::LocalPool)?;
        if permits.len() != claimed.len() {
            return Err(SandboxExecutorDriverError::CorruptClaim);
        }
        let count = claimed.len();
        for (claimed, permit) in claimed.into_iter().zip(permits) {
            let command = self.execution_command(claimed)?;
            let executor = Arc::clone(&self.executor);
            let cancellation = cancellation.child_token();
            active.spawn(async move {
                let _permit = permit;
                executor.execute(command, cancellation).await
            });
        }
        Ok(count)
    }

    fn execution_command(
        &self,
        claimed: ClaimedManagedMcpSandboxSession,
    ) -> Result<ExecuteManagedMcpSandboxSession, SandboxExecutorDriverError> {
        let expires_at = Utc::now()
            .checked_add_signed(
                ChronoDuration::from_std(self.config.receipt_ttl)
                    .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?,
            )
            .ok_or(SandboxExecutorDriverError::InvalidGeneratedCommand)?;
        let tenant_id = claimed.request.identity.tenant_id.clone();
        let worker_process_generation_id = self.pools.snapshot().worker_process_generation_id;
        Ok(ExecuteManagedMcpSandboxSession {
            claimed,
            executor_identity_digest: self.binding.executor_identity_digest.clone(),
            attestor_route: self.binding.attestor_route.clone(),
            audits: ManagedMcpSandboxSessionWorkerAudits {
                preparing: self.commit_identity(
                    &tenant_id,
                    &worker_process_generation_id,
                    expires_at,
                )?,
                starting: self.commit_identity(
                    &tenant_id,
                    &worker_process_generation_id,
                    expires_at,
                )?,
                ready: self.commit_identity(
                    &tenant_id,
                    &worker_process_generation_id,
                    expires_at,
                )?,
            },
        })
    }

    fn commit_identity(
        &self,
        tenant_id: &ResourceId,
        worker_process_generation_id: &ResourceId,
        receipt_expires_at: chrono::DateTime<Utc>,
    ) -> Result<SandboxWorkerCommitIdentity, SandboxExecutorDriverError> {
        Ok(SandboxWorkerCommitIdentity {
            tenant_id: tenant_id.clone(),
            worker_process_generation_id: worker_process_generation_id.clone(),
            receipt_id: self.new_id(ResourceKind::Receipt)?,
            event_id: self.new_id(ResourceKind::Event)?,
            outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: self
                .identities
                .new_lease_token_digest()
                .map_err(SandboxExecutorDriverError::Identity)?,
            receipt_expires_at,
        })
    }

    fn new_id(&self, kind: ResourceKind) -> Result<ResourceId, SandboxExecutorDriverError> {
        self.identities
            .new_resource_id(kind)
            .map_err(SandboxExecutorDriverError::Identity)
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<SandboxExecutorCycleReport, SandboxExecutorDriverError> {
        let mut active = JoinSet::new();
        let mut report = SandboxExecutorCycleReport::default();
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
                    match self.drive_once(&mut active, &cancellation).await {
                        Ok(claimed) => report.claimed = report.claimed.saturating_add(claimed as u64),
                        Err(SandboxExecutorDriverError::Claim(SandboxClaimFailure::Unavailable | SandboxClaimFailure::FirstWinnerLost)) => {
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
            return Err(SandboxExecutorDriverError::DrainRequiresProcessTermination);
        }
        Ok(report)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg(any())]
pub struct ManagedMcpSandboxSessionRecoveryCycleReport {
    pub candidates: u64,
    pub requeued: u64,
    pub timed_out: u64,
    pub lost: u64,
    pub replayed: u64,
    pub first_winner_lost: u64,
    pub evidence_pending: u64,
}

/// Node-local, bounded recovery driver for expired Managed MCP Sandbox sessions. It uses only the
/// Sandbox worker's reserved critical-control capacity; long-lived business permits are never
/// required to prove an old process absent, destroy its exact Provider instance, or close its
/// durable lease.
#[cfg(any())]
pub struct ManagedMcpSandboxSessionRecoveryDriver<A, P, I> {
    authority: Arc<A>,
    provider: Arc<P>,
    identities: Arc<I>,
    pools: LocalWorkerPools,
    binding: SandboxExecutorBinding,
    config: ManagedMcpSandboxSessionRecoveryConfig,
}

#[cfg(any())]
impl<A, P, I> ManagedMcpSandboxSessionRecoveryDriver<A, P, I>
where
    A: ManagedMcpSandboxSessionRecoveryAuthority + SandboxProcessGenerationIsolation,
    P: ManagedMcpSandboxSessionProvider,
    I: ExecutorIdentityFactory + 'static,
{
    pub fn new(
        authority: Arc<A>,
        provider: Arc<P>,
        identities: Arc<I>,
        pools: LocalWorkerPools,
        binding: SandboxExecutorBinding,
        config: ManagedMcpSandboxSessionRecoveryConfig,
    ) -> Result<Self, SandboxExecutorDriverError> {
        config.validate()?;
        let pool = pools.snapshot();
        if pool.worker_role != "sandbox-executor.microvm"
            || pool.work_class != WorkClass::Sandbox
            || pool.critical_control_capacity == 0
        {
            return Err(SandboxExecutorDriverError::WrongWorkerManifest);
        }
        Ok(Self {
            authority,
            provider,
            identities,
            pools,
            binding,
            config,
        })
    }

    pub async fn drive_once(
        &self,
        after: Option<ManagedMcpSandboxSessionRecoveryCursor>,
    ) -> Result<
        (
            ExpiredManagedMcpSandboxSessionLeasePage,
            ManagedMcpSandboxSessionRecoveryCycleReport,
        ),
        SandboxExecutorDriverError,
    > {
        let Some(_permit) = self
            .pools
            .try_acquire_critical_control()
            .map_err(SandboxExecutorDriverError::LocalPool)?
        else {
            return Err(SandboxExecutorDriverError::CriticalControlCapacityBusy);
        };
        let scan = ScanExpiredManagedMcpSandboxSessionLeases {
            executor: self.recovery_executor(),
            shard: self.config.shard,
            after,
            limit: self.config.batch_size,
        };
        let page = self
            .authority
            .scan_expired_managed_mcp_sandbox_session_leases(scan.clone())
            .await
            .map_err(SandboxExecutorDriverError::Recovery)?;
        page.validate_for(&scan, self.config.limits)
            .map_err(|_| SandboxExecutorDriverError::CorruptRecoveryPage)?;
        let mut report = ManagedMcpSandboxSessionRecoveryCycleReport {
            candidates: page.records.len() as u64,
            ..ManagedMcpSandboxSessionRecoveryCycleReport::default()
        };
        for expired in &page.records {
            let Some(action) = self.recovery_action(expired).await? else {
                report.evidence_pending = report.evidence_pending.saturating_add(1);
                continue;
            };
            let expected_disposition = match &action {
                ManagedMcpSandboxSessionLeaseRecoveryAction::RequeueUnstarted { .. } => {
                    ManagedMcpSandboxSessionLeaseRecoveryDisposition::Requeued
                }
                ManagedMcpSandboxSessionLeaseRecoveryAction::TimeoutUnstarted { .. } => {
                    ManagedMcpSandboxSessionLeaseRecoveryDisposition::TimedOut
                }
                ManagedMcpSandboxSessionLeaseRecoveryAction::MarkLost { .. } => {
                    ManagedMcpSandboxSessionLeaseRecoveryDisposition::Lost
                }
            };
            let command = self.recovery_command(expired.clone(), action)?;
            match self
                .authority
                .recover_expired_managed_mcp_sandbox_session_lease(command)
                .await
            {
                Ok(outcome) => {
                    let (result, replayed) = match outcome {
                        CommandOutcome::Applied(result) => (result, false),
                        CommandOutcome::Replayed(result) => (result, true),
                    };
                    if result.tenant_id != expired.request.identity.tenant_id
                        || result.subscription_id != expired.request.identity.subscription_id
                        || result.physical_job_id != expired.request.identity.physical_job_id
                        || result.recovered_lease_generation != expired.observed_lease_generation
                        || result.disposition != expected_disposition
                    {
                        return Err(SandboxExecutorDriverError::InvalidRecoveryResult);
                    }
                    if replayed {
                        report.replayed = report.replayed.saturating_add(1);
                    }
                    match result.disposition {
                        ManagedMcpSandboxSessionLeaseRecoveryDisposition::Requeued => {
                            report.requeued = report.requeued.saturating_add(1)
                        }
                        ManagedMcpSandboxSessionLeaseRecoveryDisposition::TimedOut => {
                            report.timed_out = report.timed_out.saturating_add(1)
                        }
                        ManagedMcpSandboxSessionLeaseRecoveryDisposition::Lost => {
                            report.lost = report.lost.saturating_add(1)
                        }
                    }
                }
                Err(ManagedMcpSandboxSessionRecoveryFailure::FirstWinnerLost) => {
                    report.first_winner_lost = report.first_winner_lost.saturating_add(1);
                }
                Err(failure) => return Err(SandboxExecutorDriverError::Recovery(failure)),
            }
        }
        Ok((page, report))
    }

    async fn recovery_action(
        &self,
        expired: &ExpiredManagedMcpSandboxSessionLease,
    ) -> Result<Option<ManagedMcpSandboxSessionLeaseRecoveryAction>, SandboxExecutorDriverError>
    {
        if expired.physical_state == SandboxJobState::Accepted {
            let recovery_evidence_digest = stable_unstarted_recovery_evidence_digest(expired)?;
            return Ok(Some(
                if expired.database_observed_at < expired.request.deadline {
                    ManagedMcpSandboxSessionLeaseRecoveryAction::RequeueUnstarted {
                        recovery_evidence_digest,
                    }
                } else {
                    ManagedMcpSandboxSessionLeaseRecoveryAction::TimeoutUnstarted {
                        recovery_evidence_digest,
                    }
                },
            ));
        }
        let absence_request = expired
            .process_absence_request(self.config.limits)
            .map_err(|_| SandboxExecutorDriverError::CorruptRecoveryPage)?;
        let process_absence = match self.authority.prove_absent(absence_request.clone()).await {
            Ok(evidence) => evidence,
            Err(_) => return Ok(None),
        };
        process_absence
            .validate_for(&absence_request, Utc::now())
            .map_err(|_| SandboxExecutorDriverError::InvalidRecoveryEvidence)?;
        let provider_cleanup = match process_absence.disposition {
            SandboxProcessGenerationIsolationDisposition::ProcessAbsent => {
                let cleanup = match self.provider.recover_expired_exact(expired).await {
                    Ok(cleanup) => cleanup,
                    Err(_) => return Ok(None),
                };
                cleanup
                    .validate_for_expired(expired, Utc::now())
                    .map_err(|_| SandboxExecutorDriverError::InvalidRecoveryEvidence)?;
                Some(cleanup)
            }
            SandboxProcessGenerationIsolationDisposition::NodeQuarantined => None,
        };
        let evidence = ManagedMcpSandboxSessionExpiredLeaseEvidence {
            schema_version: 1,
            process_absence,
            provider_cleanup,
            canonical_digest: placeholder_digest(),
        }
        .seal()
        .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?;
        evidence
            .validate_for(expired, Utc::now(), self.config.limits)
            .map_err(|_| SandboxExecutorDriverError::InvalidRecoveryEvidence)?;
        Ok(Some(
            ManagedMcpSandboxSessionLeaseRecoveryAction::MarkLost {
                evidence: Box::new(evidence),
            },
        ))
    }

    fn recovery_command(
        &self,
        expired: ExpiredManagedMcpSandboxSessionLease,
        action: ManagedMcpSandboxSessionLeaseRecoveryAction,
    ) -> Result<RecoverExpiredManagedMcpSandboxSessionLease, SandboxExecutorDriverError> {
        let now = Utc::now();
        let receipt_expires_at = now
            .checked_add_signed(
                ChronoDuration::from_std(self.config.receipt_ttl)
                    .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?,
            )
            .ok_or(SandboxExecutorDriverError::InvalidGeneratedCommand)?;
        let terminal = !matches!(
            action,
            ManagedMcpSandboxSessionLeaseRecoveryAction::RequeueUnstarted { .. }
        );
        let mut quota_entry_ids = if terminal {
            (0..SANDBOX_QUOTA_LINES)
                .map(|_| {
                    self.identities
                        .new_resource_id(ResourceKind::QuotaLedgerEntry)
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(SandboxExecutorDriverError::Identity)?
        } else {
            Vec::new()
        };
        quota_entry_ids.sort();
        let mut command = RecoverExpiredManagedMcpSandboxSessionLease {
            audit: ManagedMcpSandboxSessionRecoveryAudit {
                tenant_id: expired.request.identity.tenant_id.clone(),
                recovery_process_generation_id: self.pools.snapshot().worker_process_generation_id,
                receipt_id: self.new_id(ResourceKind::Receipt)?,
                event_id: self.new_id(ResourceKind::Event)?,
                outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                idempotency_key_digest: placeholder_digest(),
                request_digest: placeholder_digest(),
                receipt_expires_at,
            },
            executor: self.recovery_executor(),
            expired,
            action,
            quota_entry_ids,
        };
        command.audit.idempotency_key_digest = command
            .canonical_idempotency_key_digest()
            .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?;
        command.audit.request_digest = command
            .canonical_request_digest()
            .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?;
        command
            .validate_at(now, self.config.limits)
            .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?;
        Ok(command)
    }

    fn recovery_executor(&self) -> ManagedMcpSandboxSessionRecoveryExecutor {
        let pool = self.pools.snapshot();
        ManagedMcpSandboxSessionRecoveryExecutor {
            worker_process_generation_id: pool.worker_process_generation_id,
            worker_manifest_digest: pool.worker_manifest_digest,
            isolation_backend_contract_digest: self
                .binding
                .isolation_backend_contract_digest
                .clone(),
            executor_identity_digest: self.binding.executor_identity_digest.clone(),
            attestor_route: self.binding.attestor_route.clone(),
        }
    }

    fn new_id(&self, kind: ResourceKind) -> Result<ResourceId, SandboxExecutorDriverError> {
        self.identities
            .new_resource_id(kind)
            .map_err(SandboxExecutorDriverError::Identity)
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<(), SandboxExecutorDriverError> {
        let first_tick = Instant::now()
            .checked_add(deterministic_recovery_jitter(
                &self.pools.snapshot().worker_process_generation_id,
                self.config.timing.scan_jitter,
            ))
            .ok_or(SandboxExecutorDriverError::InvalidGeneratedCommand)?;
        let mut scan = tokio::time::interval_at(first_tick, self.config.timing.scan_interval);
        scan.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut cursor = None;
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = scan.tick() => {
                    match self.drive_once(cursor.clone()).await {
                        Ok((page, _)) => cursor = page.next_cursor,
                        Err(SandboxExecutorDriverError::CriticalControlCapacityBusy) => {}
                        Err(SandboxExecutorDriverError::Recovery(
                            ManagedMcpSandboxSessionRecoveryFailure::Unavailable
                                | ManagedMcpSandboxSessionRecoveryFailure::FirstWinnerLost,
                        )) => {
                            tokio::select! {
                                _ = cancellation.cancelled() => return Ok(()),
                                _ = tokio::time::sleep(self.config.timing.failure_backoff) => {}
                            }
                        }
                        Err(failure) => return Err(failure),
                    }
                }
            }
        }
    }
}

#[cfg(any())]
fn stable_unstarted_recovery_evidence_digest(
    expired: &ExpiredManagedMcpSandboxSessionLease,
) -> Result<Sha256Digest, SandboxExecutorDriverError> {
    canonical_digest(&serde_json::json!({
        "domain": "managed_mcp_sandbox_session_unstarted_lease_recovery",
        "lease_expires_at": expired.lease_expires_at,
        "observed_job_version": expired.observed_job_version,
        "observed_lease_generation": expired.observed_lease_generation,
        "physical_job_id": expired.request.identity.physical_job_id,
        "request_digest": expired.request.request_digest,
        "schema_version": 1,
    }))
    .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?
    .parse()
    .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)
}

#[cfg(any())]
fn deterministic_recovery_jitter(worker_id: &ResourceId, maximum: Duration) -> Duration {
    if maximum.is_zero() {
        return Duration::ZERO;
    }
    let maximum_nanos = maximum.as_nanos();
    let sample = u128::from(worker_id.uuid().as_u128() as u64);
    let nanos = sample % (maximum_nanos + 1);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

#[cfg(any())]
fn placeholder_digest() -> Sha256Digest {
    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        .parse()
        .expect("static SHA-256 placeholder is valid")
}

#[cfg(any())]
fn validate_managed_mcp_session_claims(
    claimed: &[ClaimedManagedMcpSandboxSession],
    pool: &insight_platform_worker::LocalWorkerPoolSnapshot,
    expected_tokens: &BTreeSet<Sha256Digest>,
    binding: &SandboxExecutorBinding,
) -> Result<Vec<ClaimedJobIdentity>, SandboxExecutorDriverError> {
    if claimed.len() > expected_tokens.len() {
        return Err(SandboxExecutorDriverError::CorruptClaim);
    }
    let limits = insight_platform_sandbox::SandboxCommandLimits::from_profile(
        &insight_platform_contracts::checked_in_hard_limit_profile(),
    )
    .map_err(|_| SandboxExecutorDriverError::CorruptClaim)?;
    let mut jobs = BTreeSet::new();
    let mut tokens = BTreeSet::new();
    let mut identities = Vec::with_capacity(claimed.len());
    for claim in claimed {
        claim
            .validate_at(Utc::now(), limits)
            .map_err(|_| SandboxExecutorDriverError::CorruptClaim)?;
        if claim.fence.worker_process_generation_id != pool.worker_process_generation_id
            || claim.request.executor_worker_manifest_digest != pool.worker_manifest_digest
            || claim.request.isolation_backend_contract_digest
                != binding.isolation_backend_contract_digest
            || !expected_tokens.contains(&claim.fence.token_digest)
            || !jobs.insert(claim.request.identity.physical_job_id.clone())
            || !tokens.insert(claim.fence.token_digest.clone())
        {
            return Err(SandboxExecutorDriverError::CorruptClaim);
        }
        identities.push(ClaimedJobIdentity {
            job_id: claim.request.identity.physical_job_id.clone(),
            lease_generation: claim.fence.lease_generation,
        });
    }
    Ok(identities)
}

fn validate_claims(
    claimed: &[ClaimedSandboxJob],
    pool: &insight_platform_worker::LocalWorkerPoolSnapshot,
    expected_tokens: &BTreeSet<Sha256Digest>,
) -> Result<Vec<ClaimedJobIdentity>, SandboxExecutorDriverError> {
    if claimed.len() > expected_tokens.len() {
        return Err(SandboxExecutorDriverError::CorruptClaim);
    }
    let limits = insight_platform_sandbox::SandboxCommandLimits::from_profile(
        &insight_platform_contracts::checked_in_hard_limit_profile(),
    )
    .map_err(|_| SandboxExecutorDriverError::CorruptClaim)?;
    let mut jobs = BTreeSet::new();
    let mut tokens = BTreeSet::new();
    let mut identities = Vec::with_capacity(claimed.len());
    for claim in claimed {
        claim
            .validate_at(Utc::now(), limits)
            .map_err(|_| SandboxExecutorDriverError::CorruptClaim)?;
        if claim.fence.worker_process_generation_id != pool.worker_process_generation_id
            || claim.request.executor_worker_manifest_digest != pool.worker_manifest_digest
            || !expected_tokens.contains(&claim.fence.token_digest)
            || !jobs.insert(claim.request.job_id.clone())
            || !tokens.insert(claim.fence.token_digest.clone())
        {
            return Err(SandboxExecutorDriverError::CorruptClaim);
        }
        identities.push(ClaimedJobIdentity {
            job_id: claim.request.job_id.clone(),
            lease_generation: claim.fence.lease_generation,
        });
    }
    Ok(identities)
}

fn observe_join(
    joined: Option<Result<SandboxExecutionDisposition, JoinError>>,
    report: &mut SandboxExecutorCycleReport,
) {
    match joined {
        Some(Ok(SandboxExecutionDisposition::Settled)) => {
            report.settled = report.settled.saturating_add(1)
        }
        Some(Ok(SandboxExecutionDisposition::Abandoned)) | Some(Err(_)) | None => {
            report.abandoned = report.abandoned.saturating_add(1)
        }
    }
}

#[derive(Debug)]
pub enum SandboxExecutorDriverError {
    InvalidConfig(String),
    WrongWorkerManifest,
    InvalidGeneratedCommand,
    CorruptClaim,
    CorruptRecoveryPage,
    InvalidRecoveryEvidence,
    InvalidRecoveryResult,
    CriticalControlCapacityBusy,
    DrainRequiresProcessTermination,
    Identity(ExecutorIdentityError),
    LocalPool(LocalWorkerPoolError),
    Claim(SandboxClaimFailure),
}

impl fmt::Display for SandboxExecutorDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfig(_) => "Sandbox Executor driver configuration is invalid",
            Self::WrongWorkerManifest => "Sandbox Executor worker manifest is invalid",
            Self::InvalidGeneratedCommand => "Sandbox Executor generated an invalid command",
            Self::CorruptClaim => "Sandbox claim response is inconsistent",
            Self::CorruptRecoveryPage => "Managed MCP recovery page is inconsistent",
            Self::InvalidRecoveryEvidence => "Managed MCP recovery evidence is invalid",
            Self::InvalidRecoveryResult => "Managed MCP recovery result is inconsistent",
            Self::CriticalControlCapacityBusy => {
                "Sandbox critical-control recovery capacity is busy"
            }
            Self::DrainRequiresProcessTermination => {
                "Sandbox drain grace expired; process-generation termination is required"
            }
            Self::Identity(_) => "Sandbox Executor identity generation failed",
            Self::LocalPool(_) => "Sandbox Executor local pool failed",
            Self::Claim(_) => "Sandbox claim authority failed",
        })
    }
}

impl Error for SandboxExecutorDriverError {}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        checked_in_hard_limit_profile, WorkerManifest, WORKER_MANIFEST_VERSION,
        WORKER_PROTOCOL_VERSION,
    };
    use std::sync::Mutex;

    #[derive(Default)]
    struct EmptyAuthority {
        claim: Mutex<Option<ClaimSandboxJobs>>,
    }

    #[async_trait]
    impl SandboxClaimAuthority for EmptyAuthority {
        async fn claim_sandbox_jobs(
            &self,
            command: ClaimSandboxJobs,
        ) -> Result<Vec<ClaimedSandboxJob>, SandboxClaimFailure> {
            *self.claim.lock().unwrap() = Some(command);
            Ok(Vec::new())
        }
    }

    struct UnusedExecutor;

    #[async_trait]
    impl SandboxJobCommandExecutor for UnusedExecutor {
        async fn execute(&self, _command: ExecuteSandboxJob) -> SandboxExecutionDisposition {
            panic!("empty authority must not dispatch")
        }
    }

    #[derive(Default)]
    #[cfg(any())]
    struct EmptyManagedSessionAuthority {
        claim: Mutex<Option<ClaimSandboxJobs>>,
    }

    #[async_trait]
    #[cfg(any())]
    impl ManagedMcpSandboxSessionClaimAuthority for EmptyManagedSessionAuthority {
        async fn claim_managed_mcp_sandbox_sessions(
            &self,
            command: ClaimSandboxJobs,
        ) -> Result<Vec<ClaimedManagedMcpSandboxSession>, SandboxClaimFailure> {
            *self.claim.lock().unwrap() = Some(command);
            Ok(Vec::new())
        }
    }

    #[cfg(any())]
    struct UnusedManagedSessionExecutor;

    #[async_trait]
    #[cfg(any())]
    impl ManagedMcpSandboxSessionCommandExecutor for UnusedManagedSessionExecutor {
        async fn execute(
            &self,
            _command: ExecuteManagedMcpSandboxSession,
            _cancellation: CancellationToken,
        ) -> SandboxExecutionDisposition {
            panic!("empty authority must not dispatch")
        }
    }

    #[derive(Default)]
    #[cfg(any())]
    struct EmptyManagedRecoveryAuthority {
        scan: Mutex<Option<ScanExpiredManagedMcpSandboxSessionLeases>>,
    }

    #[async_trait]
    #[cfg(any())]
    impl ManagedMcpSandboxSessionRecoveryAuthority for EmptyManagedRecoveryAuthority {
        async fn scan_expired_managed_mcp_sandbox_session_leases(
            &self,
            command: ScanExpiredManagedMcpSandboxSessionLeases,
        ) -> Result<ExpiredManagedMcpSandboxSessionLeasePage, ManagedMcpSandboxSessionRecoveryFailure>
        {
            *self.scan.lock().unwrap() = Some(command);
            Ok(ExpiredManagedMcpSandboxSessionLeasePage {
                records: Vec::new(),
                next_cursor: None,
                exhausted: true,
            })
        }

        async fn recover_expired_managed_mcp_sandbox_session_lease(
            &self,
            _command: RecoverExpiredManagedMcpSandboxSessionLease,
        ) -> Result<
            CommandOutcome<insight_platform_sandbox::ManagedMcpSandboxSessionLeaseRecoveryResult>,
            ManagedMcpSandboxSessionRecoveryFailure,
        > {
            unreachable!("an empty scan cannot submit recovery")
        }
    }

    #[async_trait]
    #[cfg(any())]
    impl SandboxProcessGenerationIsolation for EmptyManagedRecoveryAuthority {
        async fn prove_absent(
            &self,
            _request: insight_platform_sandbox::ProveSandboxProcessGenerationAbsent,
        ) -> Result<
            insight_platform_sandbox::SandboxProcessGenerationAbsenceEvidence,
            insight_platform_sandbox::SandboxProcessGenerationIsolationError,
        > {
            unreachable!("an empty scan cannot request process evidence")
        }
    }

    #[cfg(any())]
    struct UnusedManagedRecoveryProvider;

    #[async_trait]
    #[cfg(any())]
    impl ManagedMcpSandboxSessionProvider for UnusedManagedRecoveryProvider {
        type Error = std::convert::Infallible;

        async fn prepare(
            &self,
            _request: &insight_platform_sandbox::ManagedMcpSandboxSessionRequest,
            _fence: &insight_platform_jobs::JobFence,
            _executor_identity_digest: &Sha256Digest,
        ) -> Result<insight_platform_sandbox::PreparedManagedMcpSandboxSession, Self::Error>
        {
            unreachable!("recovery provider cannot prepare")
        }

        async fn initialize(
            &self,
            _request: &insight_platform_sandbox::ManagedMcpSandboxSessionRequest,
            _fence: &insight_platform_jobs::JobFence,
            _prepared: &insight_platform_sandbox::PreparedManagedMcpSandboxSession,
        ) -> Result<insight_platform_sandbox::PreparedManagedMcpSandboxSessionActivation, Self::Error>
        {
            unreachable!("recovery provider cannot initialize")
        }

        async fn activate(
            &self,
            _request: &insight_platform_sandbox::ManagedMcpSandboxSessionRequest,
            _fence: &insight_platform_jobs::JobFence,
            _activation: &insight_platform_sandbox::PreparedManagedMcpSandboxSessionActivation,
        ) -> Result<insight_platform_sandbox::ActivatedManagedMcpSandboxSession, Self::Error>
        {
            unreachable!("recovery provider cannot activate")
        }

        async fn observe_exact(
            &self,
            _request: &insight_platform_sandbox::ManagedMcpSandboxSessionRequest,
            _fence: &insight_platform_jobs::JobFence,
            _prepared: &insight_platform_sandbox::PreparedManagedMcpSandboxSession,
            _activated: &insight_platform_sandbox::ActivatedManagedMcpSandboxSession,
        ) -> Result<ManagedMcpSandboxSessionLivenessEvidence, Self::Error> {
            unreachable!("recovery provider cannot observe a live session")
        }

        async fn destroy_exact(
            &self,
            _request: &insight_platform_sandbox::ManagedMcpSandboxSessionRequest,
            _fence: &insight_platform_jobs::JobFence,
            _prepared: Option<&insight_platform_sandbox::PreparedManagedMcpSandboxSession>,
        ) -> Result<ManagedMcpSandboxSessionCleanupOutcome, Self::Error> {
            unreachable!("recovery provider cannot use the live-session destroy path")
        }

        async fn recover_expired_exact(
            &self,
            _expired: &ExpiredManagedMcpSandboxSessionLease,
        ) -> Result<ManagedMcpSandboxSessionCleanupOutcome, Self::Error> {
            unreachable!("an empty scan cannot recover Provider state")
        }
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn pools() -> LocalWorkerPools {
        LocalWorkerPools::new(
            WorkerManifest {
                manifest_version: WORKER_MANIFEST_VERSION,
                worker_role: "sandbox-executor.wasi".to_owned(),
                work_class: WorkClass::Sandbox,
                adapter_runtime_digest: digest('a'),
                protocol_version: WORKER_PROTOCOL_VERSION,
                max_concurrency: 2,
                critical_control_reserved_slots: 1,
            },
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap(),
        )
        .unwrap()
    }

    #[cfg(any())]
    fn micro_vm_pools() -> LocalWorkerPools {
        LocalWorkerPools::new(
            WorkerManifest {
                manifest_version: WORKER_MANIFEST_VERSION,
                worker_role: "sandbox-executor.microvm".to_owned(),
                work_class: WorkClass::Sandbox,
                adapter_runtime_digest: digest('a'),
                protocol_version: WORKER_PROTOCOL_VERSION,
                max_concurrency: 2,
                critical_control_reserved_slots: 1,
            },
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn claim_is_bounded_by_local_capacity_before_authority_access() {
        let authority = Arc::new(EmptyAuthority::default());
        let local_pools = pools();
        let generation = local_pools.snapshot().worker_process_generation_id;
        let worker_manifest_digest = local_pools.snapshot().worker_manifest_digest;
        let driver = SandboxExecutorDriver::new(
            Arc::clone(&authority),
            Arc::new(UnusedExecutor),
            Arc::new(UuidExecutorIdentityFactory),
            local_pools.clone(),
            SandboxExecutorBinding {
                isolation_backend_contract_digest: digest('c'),
                executor_identity_digest: digest('d'),
                attestor_route: "https://10.0.0.7:9443".parse().unwrap(),
            },
            SandboxExecutorDriverConfig::from_profile(
                &checked_in_hard_limit_profile(),
                Duration::from_secs(7_200),
                SandboxExecutorDriverTiming {
                    safety_scan_interval: Duration::from_millis(100),
                    claim_failure_backoff: Duration::from_millis(10),
                    drain_grace: Duration::from_secs(1),
                },
            )
            .unwrap(),
        )
        .unwrap();
        let mut active = JoinSet::new();
        assert_eq!(driver.drive_once(&mut active).await.unwrap(), 0);
        assert!(active.is_empty());
        let claim = authority.claim.lock().unwrap().clone().unwrap();
        assert_eq!(claim.worker_process_generation_id, generation);
        assert_eq!(claim.worker_manifest_digest, worker_manifest_digest);
        assert_eq!(claim.isolation_backend_contract_digest, digest('c'));
        assert_eq!(claim.attestor_route.as_str(), "https://10.0.0.7:9443");
        assert_eq!(claim.limit, 2);
        assert_eq!(claim.lease_token_digests.len(), 2);
        assert_ne!(claim.lease_token_digests[0], claim.lease_token_digests[1]);
        assert_eq!(local_pools.snapshot().business_available, 2);
    }

    #[tokio::test]
    #[cfg(any())]
    async fn managed_session_claim_uses_the_same_bounded_sandbox_pool_and_closed_lane() {
        let authority = Arc::new(EmptyManagedSessionAuthority::default());
        let local_pools = micro_vm_pools();
        let generation = local_pools.snapshot().worker_process_generation_id;
        let worker_manifest_digest = local_pools.snapshot().worker_manifest_digest;
        let driver = ManagedMcpSandboxSessionExecutorDriver::new(
            Arc::clone(&authority),
            Arc::new(UnusedManagedSessionExecutor),
            Arc::new(UuidExecutorIdentityFactory),
            local_pools.clone(),
            SandboxExecutorBinding {
                isolation_backend_contract_digest: digest('c'),
                executor_identity_digest: digest('d'),
                attestor_route: "https://10.0.0.7:9443".parse().unwrap(),
            },
            SandboxExecutorDriverConfig::from_profile(
                &checked_in_hard_limit_profile(),
                Duration::from_secs(7_200),
                SandboxExecutorDriverTiming {
                    safety_scan_interval: Duration::from_millis(100),
                    claim_failure_backoff: Duration::from_millis(10),
                    drain_grace: Duration::from_secs(1),
                },
            )
            .unwrap(),
        )
        .unwrap();
        let mut active = JoinSet::new();
        assert_eq!(
            driver
                .drive_once(&mut active, &CancellationToken::new())
                .await
                .unwrap(),
            0
        );
        assert!(active.is_empty());
        let claim = authority.claim.lock().unwrap().clone().unwrap();
        assert_eq!(claim.worker_process_generation_id, generation);
        assert_eq!(claim.worker_manifest_digest, worker_manifest_digest);
        assert_eq!(claim.isolation_backend_contract_digest, digest('c'));
        assert_eq!(claim.limit, 2);
        assert_eq!(claim.lease_token_digests.len(), 2);
        assert_eq!(local_pools.snapshot().business_available, 2);
    }

    #[tokio::test]
    #[cfg(any())]
    async fn managed_recovery_uses_reserved_control_capacity_when_business_is_saturated() {
        let authority = Arc::new(EmptyManagedRecoveryAuthority::default());
        let local_pools = micro_vm_pools();
        let reservation = local_pools
            .reserve_claim_capacity(
                WorkClass::Sandbox,
                2,
                ClaimBatchHardLimit::from_profile(&checked_in_hard_limit_profile()).unwrap(),
            )
            .unwrap()
            .unwrap();
        let _business_permits = reservation
            .bind_claimed_jobs(vec![
                ClaimedJobIdentity {
                    job_id: ResourceId::from_uuid_v7(ResourceKind::Job, Uuid::now_v7()).unwrap(),
                    lease_generation: 1,
                },
                ClaimedJobIdentity {
                    job_id: ResourceId::from_uuid_v7(ResourceKind::Job, Uuid::now_v7()).unwrap(),
                    lease_generation: 1,
                },
            ])
            .unwrap();
        assert_eq!(local_pools.snapshot().business_available, 0);
        let binding = SandboxExecutorBinding {
            isolation_backend_contract_digest: digest('c'),
            executor_identity_digest: digest('d'),
            attestor_route: "https://10.0.0.7:9443".parse().unwrap(),
        };
        let config = ManagedMcpSandboxSessionRecoveryConfig::from_profile(
            &checked_in_hard_limit_profile(),
            ManagedMcpSandboxSessionRecoveryShard { index: 0, count: 1 },
            Duration::from_secs(7_200),
            ManagedMcpSandboxSessionRecoveryTiming {
                scan_interval: Duration::from_millis(100),
                scan_jitter: Duration::ZERO,
                failure_backoff: Duration::from_millis(10),
            },
        )
        .unwrap();
        let driver = ManagedMcpSandboxSessionRecoveryDriver::new(
            Arc::clone(&authority),
            Arc::new(UnusedManagedRecoveryProvider),
            Arc::new(UuidExecutorIdentityFactory),
            local_pools.clone(),
            binding.clone(),
            config,
        )
        .unwrap();
        let (page, report) = driver.drive_once(None).await.unwrap();
        assert!(page.exhausted);
        assert_eq!(
            report,
            ManagedMcpSandboxSessionRecoveryCycleReport::default()
        );
        let scan = authority.scan.lock().unwrap().clone().unwrap();
        assert_eq!(
            scan.executor.worker_manifest_digest,
            local_pools.snapshot().worker_manifest_digest
        );
        assert_eq!(
            scan.executor.isolation_backend_contract_digest,
            binding.isolation_backend_contract_digest
        );
        assert_eq!(
            scan.executor.executor_identity_digest,
            binding.executor_identity_digest
        );
        assert_eq!(scan.executor.attestor_route, binding.attestor_route);
        assert_eq!(
            scan.shard,
            ManagedMcpSandboxSessionRecoveryShard { index: 0, count: 1 }
        );
        assert_eq!(
            u64::from(scan.limit),
            checked_in_hard_limit_profile()
                .control_data
                .recovery_batch
                .q1_default
        );
        assert_eq!(local_pools.snapshot().business_available, 0);
        assert_eq!(local_pools.snapshot().critical_control_available, 1);
    }

    #[test]
    fn driver_rejects_non_sandbox_worker_manifest() {
        let wrong = LocalWorkerPools::new(
            WorkerManifest {
                manifest_version: WORKER_MANIFEST_VERSION,
                worker_role: "model-worker".to_owned(),
                work_class: WorkClass::Model,
                adapter_runtime_digest: digest('b'),
                protocol_version: WORKER_PROTOCOL_VERSION,
                max_concurrency: 1,
                critical_control_reserved_slots: 1,
            },
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap(),
        )
        .unwrap();
        let result = SandboxExecutorDriver::new(
            Arc::new(EmptyAuthority::default()),
            Arc::new(UnusedExecutor),
            Arc::new(UuidExecutorIdentityFactory),
            wrong,
            SandboxExecutorBinding {
                isolation_backend_contract_digest: digest('c'),
                executor_identity_digest: digest('d'),
                attestor_route: "https://10.0.0.7:9443".parse().unwrap(),
            },
            SandboxExecutorDriverConfig::from_profile(
                &checked_in_hard_limit_profile(),
                Duration::from_secs(7_200),
                SandboxExecutorDriverTiming {
                    safety_scan_interval: Duration::from_secs(1),
                    claim_failure_backoff: Duration::from_millis(10),
                    drain_grace: Duration::from_secs(1),
                },
            )
            .unwrap(),
        );
        assert!(matches!(
            result,
            Err(SandboxExecutorDriverError::WrongWorkerManifest)
        ));
    }
}
