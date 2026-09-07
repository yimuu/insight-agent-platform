//! Dedicated durable worker for RegistryValidation Jobs.
//!
//! Registry validation is neither a Gateway side effect nor a generic Job payload rewrite. This
//! process has one work class, one service identity, and uses the repository's fenced atomic
//! validation commit to preserve the public Operation request target.
mod artifact_client;
pub use artifact_client::RegistryArtifactClient;
use insight_platform_jobs::store::JobCommandFence as JobFence;

use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, ResourceId, ResourceKind, Sha256Digest,
    WorkClass,
};
use insight_platform_postgres::repository::{
    ClaimJobs, CommitRegistryValidation, PgRepository, RegistryValidationCommitOutcome,
    RepositoryError,
};
use insight_platform_worker::{ClaimBatchHardLimit, ClaimedJobIdentity, LocalWorkerPools};
use std::{error::Error, fmt, sync::Arc, time::Duration};
use tokio::{task::JoinSet, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const REGISTRY_VALIDATION_WORKER_ROLE: &str = "registry-validation-worker";

#[derive(Debug, Clone, Copy)]
pub struct RegistryValidationWorkerConfig {
    pub claim_batch: u16,
    pub lease_milliseconds: i64,
    pub scan_interval: Duration,
    pub failure_backoff: Duration,
    pub drain_grace: Duration,
    pub receipt_ttl: Duration,
}

impl RegistryValidationWorkerConfig {
    pub fn validate(self) -> Result<(), RegistryValidationWorkerError> {
        let hard_limit = ClaimBatchHardLimit::from_profile(&checked_in_hard_limit_profile())
            .map_err(|_| RegistryValidationWorkerError::InvalidConfiguration)?;
        if self.claim_batch == 0
            || usize::from(self.claim_batch) > hard_limit.get()
            || self.lease_milliseconds < 30_000
            || self.lease_milliseconds > 120_000
            || self.scan_interval.is_zero()
            || self.scan_interval > Duration::from_secs(60)
            || self.failure_backoff.is_zero()
            || self.failure_backoff > self.scan_interval
            || self.drain_grace.is_zero()
            || self.drain_grace > Duration::from_secs(120)
            || self.receipt_ttl <= Duration::from_millis(self.lease_milliseconds.unsigned_abs())
        {
            return Err(RegistryValidationWorkerError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RegistryValidationWorkerReport {
    pub claimed: u64,
    pub committed: u64,
    pub abandoned: u64,
}

pub struct RegistryValidationWorkerDriver {
    artifacts: Arc<RegistryArtifactClient>,
    repository: Arc<PgRepository>,
    pools: LocalWorkerPools,
    validator_principal_id: ResourceId,
    validator_digest: Sha256Digest,
    validation_profile_digest: Sha256Digest,
    config: RegistryValidationWorkerConfig,
    claim_limit: ClaimBatchHardLimit,
    recovery_cursor: tokio::sync::Mutex<Option<insight_platform_jobs::store::SafetyScanCursor>>,
}

impl RegistryValidationWorkerDriver {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        repository: Arc<PgRepository>,
        pools: LocalWorkerPools,
        validator_principal_id: ResourceId,
        validator_digest: Sha256Digest,
        validation_profile_digest: Sha256Digest,
        config: RegistryValidationWorkerConfig,
        artifacts: Arc<RegistryArtifactClient>,
    ) -> Result<Self, RegistryValidationWorkerError> {
        config.validate()?;
        let snapshot = pools.snapshot();
        if snapshot.worker_role != REGISTRY_VALIDATION_WORKER_ROLE
            || snapshot.work_class != WorkClass::RegistryValidation
            || validator_principal_id.kind() != ResourceKind::Principal
        {
            return Err(RegistryValidationWorkerError::InvalidConfiguration);
        }
        Ok(Self {
            artifacts,
            repository,
            pools,
            validator_principal_id,
            validator_digest,
            validation_profile_digest,
            config,
            recovery_cursor: tokio::sync::Mutex::new(None),
            claim_limit: ClaimBatchHardLimit::from_profile(&checked_in_hard_limit_profile())
                .map_err(|_| RegistryValidationWorkerError::InvalidConfiguration)?,
        })
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<bool>,
    ) -> Result<usize, RegistryValidationWorkerError> {
        let mut recovery_cursor = self.recovery_cursor.lock().await;
        let page = self
            .repository
            .recover_expired_registry_validation_jobs(
                recovery_cursor.clone(),
                self.config.claim_batch,
                i64::try_from(self.config.failure_backoff.as_millis())
                    .map_err(|_| RegistryValidationWorkerError::InvalidConfiguration)?,
            )
            .await?;
        *recovery_cursor = page.next_cursor;
        drop(recovery_cursor);
        for diagnostic in &page.diagnostics {
            eprintln!(
                "Registry validation retained invalid durable object: phase={:?}, code={:?}",
                diagnostic.phase, diagnostic.code,
            );
        }

        let Some(reservation) = self
            .pools
            .reserve_claim_capacity(
                WorkClass::RegistryValidation,
                usize::from(self.config.claim_batch),
                self.claim_limit,
            )
            .map_err(|_| RegistryValidationWorkerError::LocalCapacity)?
        else {
            return Ok(0);
        };
        let process = self.pools.snapshot();
        let tokens = (0..reservation.claim_limit())
            .map(|_| generated_digest("registry_validation_lease_token"))
            .collect::<Result<Vec<_>, _>>()?;
        let claims = self
            .repository
            .claim_registry_validation_jobs(
                ClaimJobs {
                    worker_manifest: self.pools.manifest().clone(),
                    work_class: WorkClass::RegistryValidation.as_str().to_owned(),
                    worker_id: process.worker_process_generation_id.clone(),
                    limit: u16::try_from(reservation.claim_limit())
                        .map_err(|_| RegistryValidationWorkerError::InvalidGeneratedCommand)?,
                    lease_milliseconds: self.config.lease_milliseconds,
                    lease_token_digests: tokens,
                },
                &self.validator_principal_id,
            )
            .await?;
        let permits = reservation
            .bind_claimed_jobs(
                claims
                    .iter()
                    .map(|job| {
                        Ok(ClaimedJobIdentity {
                            job_id: parse_id(&job.job_id, ResourceKind::Job)?,
                            lease_generation: u64::try_from(job.lease_epoch)
                                .map_err(|_| RegistryValidationWorkerError::CorruptClaim)?,
                        })
                    })
                    .collect::<Result<Vec<_>, RegistryValidationWorkerError>>()?,
            )
            .map_err(|_| RegistryValidationWorkerError::CorruptClaim)?;
        if permits.len() != claims.len() {
            return Err(RegistryValidationWorkerError::CorruptClaim);
        }
        let claimed = claims.len();
        for (claim, permit) in claims.into_iter().zip(permits) {
            let repository = Arc::clone(&self.repository);
            let artifacts = Arc::clone(&self.artifacts);
            let lease_milliseconds = self.config.lease_milliseconds;
            let validator_principal_id = self.validator_principal_id.clone();
            let validator_digest = self.validator_digest.clone();
            let validation_profile_digest = self.validation_profile_digest.clone();
            let receipt_ttl = self.config.receipt_ttl;
            active.spawn(async move {
                let _permit = permit;
                match execute_claim(
                    repository,
                    claim,
                    ValidationClaimContext {
                        validator_principal_id,
                        validator_digest,
                        validation_profile_digest,
                        receipt_ttl,
                        artifacts,
                        lease_milliseconds,
                    },
                )
                .await
                {
                    Ok(()) => true,
                    Err(error) => {
                        eprintln!("registry validation claim was abandoned: {error}");
                        false
                    }
                }
            });
        }
        Ok(claimed)
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<RegistryValidationWorkerReport, RegistryValidationWorkerError> {
        let mut report = RegistryValidationWorkerReport::default();
        let mut active = JoinSet::new();
        let mut scan = tokio::time::interval(self.config.scan_interval);
        scan.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                result = active.join_next(), if !active.is_empty() => match result {
                    Some(Ok(true)) => report.committed += 1,
                    Some(Ok(false)) => report.abandoned += 1,
                    Some(Err(error)) => {
                        eprintln!("registry validation attempt task failed: {error}");
                        report.abandoned += 1;
                    }
                    None => {}
                },
                _ = scan.tick() => match self.drive_once(&mut active).await {
                    Ok(claimed) => report.claimed += u64::try_from(claimed)
                        .map_err(|_| RegistryValidationWorkerError::InvalidGeneratedCommand)?,
                    Err(RegistryValidationWorkerError::Repository(RepositoryError::Database(_))) => {
                        tokio::select! {
                            _ = cancellation.cancelled() => break,
                            _ = tokio::time::sleep(self.config.failure_backoff) => {}
                        }
                    }
                    Err(error) => return Err(error),
                },
            }
        }
        let drain = async {
            while let Some(result) = active.join_next().await {
                match result {
                    Ok(true) => report.committed += 1,
                    _ => report.abandoned += 1,
                }
            }
        };
        tokio::time::timeout(self.config.drain_grace, drain)
            .await
            .map_err(|_| RegistryValidationWorkerError::DrainRequiresTermination)?;
        Ok(report)
    }
}

struct ValidationClaimContext {
    validator_principal_id: ResourceId,
    validator_digest: Sha256Digest,
    validation_profile_digest: Sha256Digest,
    receipt_ttl: Duration,
    artifacts: Arc<RegistryArtifactClient>,
    lease_milliseconds: i64,
}
async fn execute_claim(
    repository: Arc<PgRepository>,
    claim: insight_platform_jobs::store::JobRecord,
    context: ValidationClaimContext,
) -> Result<(), RegistryValidationWorkerError> {
    let ValidationClaimContext {
        validator_principal_id,
        validator_digest,
        validation_profile_digest,
        receipt_ttl,
        artifacts,
        lease_milliseconds,
    } = context;
    let worker_id = claim
        .worker_id
        .as_deref()
        .ok_or(RegistryValidationWorkerError::CorruptClaim)
        .and_then(|value| parse_id(value, ResourceKind::WorkerProcessGeneration))?;
    let lease_token_digest = claim
        .lease_token_digest
        .as_deref()
        .ok_or(RegistryValidationWorkerError::CorruptClaim)?
        .parse()
        .map_err(|_| RegistryValidationWorkerError::CorruptClaim)?;
    let claimed_fence = JobFence {
        tenant_id: claim.tenant_id.clone(),
        job_id: claim.job_id.clone(),
        worker_id,
        lease_epoch: claim.lease_epoch,
        expected_job_version: claim.version,
        lease_token_digest,
    };
    let running = repository.start_job(claimed_fence.clone()).await?;
    let mut running_fence = JobFence {
        expected_job_version: running.version,
        ..claimed_fence
    };
    let compilation = if matches!(
        &running.execution_requirement,
        insight_platform_contracts::ExecutionRequirement::AgentCompilation { .. }
    ) {
        use insight_platform_artifacts::{
            RegistryArtifactReadRequestV1, RegistryArtifactSelectorV1,
        };
        let tenant = parse_id(&running_fence.tenant_id, ResourceKind::Tenant)?;
        let request =
            |fence: &JobFence,
             selector|
             -> Result<RegistryArtifactReadRequestV1, RegistryValidationWorkerError> {
                Ok(RegistryArtifactReadRequestV1 {
                    schema_version: 1,
                    authority: registry_read_authority(fence)?,
                    selector,
                    deadline: (Utc::now() + ChronoDuration::seconds(10)).min(running.deadline),
                })
            };
        let source = artifacts
            .read(
                &tenant,
                &validator_principal_id,
                &request(&running_fence, RegistryArtifactSelectorV1::Authoring)?,
            )
            .await?;
        let heartbeat = repository
            .heartbeat_job(insight_platform_jobs::store::HeartbeatJob {
                fence: running_fence.clone(),
                lease_milliseconds,
            })
            .await?;
        running_fence.expected_job_version = heartbeat.version;
        let plan = artifacts
            .read(
                &tenant,
                &validator_principal_id,
                &request(&running_fence, RegistryArtifactSelectorV1::TypedPlan)?,
            )
            .await?;
        let heartbeat = repository
            .heartbeat_job(insight_platform_jobs::store::HeartbeatJob {
                fence: running_fence.clone(),
                lease_milliseconds,
            })
            .await?;
        running_fence.expected_job_version = heartbeat.version;
        let document = repository
            .read_registry_validation_document(
                &tenant,
                &validator_principal_id,
                &registry_read_authority(&running_fence)?,
            )
            .await?;
        let source_bytes = source
            .decode_verified()
            .map_err(|_| RegistryValidationWorkerError::ArtifactReadRejected)?;
        let plan_bytes = plan
            .decode_verified()
            .map_err(|_| RegistryValidationWorkerError::ArtifactReadRejected)?;
        let evidence = tokio::task::spawn_blocking(move || {
            let source_authority = insight_platform_agent_compiler::ArtifactAuthority {
                purpose: source.purpose,
                state: insight_platform_contracts::ArtifactState::Ready,
                artifact: source.artifact,
            };
            let plan_authority = insight_platform_agent_compiler::ArtifactAuthority {
                purpose: plan.purpose,
                state: insight_platform_contracts::ArtifactState::Ready,
                artifact: plan.artifact,
            };
            insight_platform_agent_compiler::validate_frozen_agent_artifacts(
                &document,
                &source_authority,
                &source_bytes,
                &plan_authority,
                &plan_bytes,
            )
        })
        .await
        .map_err(|_| RegistryValidationWorkerError::CompilationRejected)?
        .map_err(|_| RegistryValidationWorkerError::CompilationRejected)?;
        let heartbeat = repository
            .heartbeat_job(insight_platform_jobs::store::HeartbeatJob {
                fence: running_fence.clone(),
                lease_milliseconds,
            })
            .await?;
        running_fence.expected_job_version = heartbeat.version;
        Some(evidence)
    } else {
        None
    };
    let idempotency_key_digest = generated_digest("registry_validation_commit")?;
    let request_digest = generated_digest("registry_validation_request")?;
    let receipt_ttl = ChronoDuration::from_std(receipt_ttl)
        .map_err(|_| RegistryValidationWorkerError::InvalidGeneratedCommand)?;
    match repository
        .commit_registry_validation(CommitRegistryValidation {
            compilation,
            fence: running_fence,
            validator_principal_id,
            validator_digest,
            validation_profile_digest,
            receipt_id: new_id(ResourceKind::Receipt)?,
            resource_event_id: new_id(ResourceKind::Event)?,
            resource_outbox_id: new_id(ResourceKind::OutboxEvent)?,
            job_event_id: new_id(ResourceKind::Event)?,
            job_outbox_id: new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest,
            request_digest,
            receipt_expires_at: Utc::now() + receipt_ttl,
        })
        .await?
    {
        RegistryValidationCommitOutcome::Committed { .. }
        | RegistryValidationCommitOutcome::Replayed => Ok(()),
    }
}

fn registry_read_authority(
    fence: &JobFence,
) -> Result<
    insight_platform_artifacts::RegistryArtifactReadAuthorityV1,
    RegistryValidationWorkerError,
> {
    Ok(
        insight_platform_artifacts::RegistryArtifactReadAuthorityV1::RegistryJob {
            job_id: parse_id(&fence.job_id, ResourceKind::Job)?,
            worker_process_generation_id: fence.worker_id.clone(),
            lease_epoch: u64::try_from(fence.lease_epoch)
                .map_err(|_| RegistryValidationWorkerError::CorruptClaim)?,
            expected_job_version: u64::try_from(fence.expected_job_version)
                .map_err(|_| RegistryValidationWorkerError::CorruptClaim)?,
            lease_token_digest: fence.lease_token_digest.clone(),
        },
    )
}

fn new_id(kind: ResourceKind) -> Result<ResourceId, RegistryValidationWorkerError> {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7())
        .map_err(|_| RegistryValidationWorkerError::InvalidGeneratedCommand)
}

fn parse_id(value: &str, kind: ResourceKind) -> Result<ResourceId, RegistryValidationWorkerError> {
    ResourceId::parse_expected(value, kind).map_err(|_| RegistryValidationWorkerError::CorruptClaim)
}

fn generated_digest(domain: &str) -> Result<Sha256Digest, RegistryValidationWorkerError> {
    let value = serde_json::json!({"domain": domain, "nonce": Uuid::now_v7().to_string()});
    canonical_digest(&value)
        .map_err(|_| RegistryValidationWorkerError::InvalidGeneratedCommand)?
        .parse()
        .map_err(|_| RegistryValidationWorkerError::InvalidGeneratedCommand)
}

#[derive(Debug)]
pub enum RegistryValidationWorkerError {
    ArtifactReadUnavailable,
    ArtifactReadRejected,
    CompilationRejected,
    InvalidConfiguration,
    InvalidGeneratedCommand,
    LocalCapacity,
    CorruptClaim,
    DrainRequiresTermination,
    Repository(RepositoryError),
}

impl fmt::Display for RegistryValidationWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArtifactReadUnavailable => {
                formatter.write_str("Registry Artifact reader is unavailable")
            }
            Self::ArtifactReadRejected => {
                formatter.write_str("Registry Artifact read was rejected")
            }
            Self::CompilationRejected => {
                formatter.write_str("Registry compilation evidence was rejected")
            }
            Self::InvalidConfiguration => {
                formatter.write_str("registry validation worker configuration is invalid")
            }
            Self::InvalidGeneratedCommand => {
                formatter.write_str("registry validation worker generated an invalid command")
            }
            Self::LocalCapacity => {
                formatter.write_str("registry validation worker local capacity is invalid")
            }
            Self::CorruptClaim => {
                formatter.write_str("registry validation worker received a corrupt Job claim")
            }
            Self::DrainRequiresTermination => {
                formatter.write_str("registry validation worker did not drain before its deadline")
            }
            Self::Repository(error) => {
                write!(formatter, "registry validation repository error: {error}")
            }
        }
    }
}

impl Error for RegistryValidationWorkerError {}

impl From<RepositoryError> for RegistryValidationWorkerError {
    fn from(error: RepositoryError) -> Self {
        Self::Repository(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_rejects_a_receipt_ttl_shorter_than_its_lease() {
        assert!(RegistryValidationWorkerConfig {
            claim_batch: 1,
            lease_milliseconds: 10_000,
            scan_interval: Duration::from_millis(100),
            failure_backoff: Duration::from_millis(10),
            drain_grace: Duration::from_secs(1),
            receipt_ttl: Duration::from_secs(10),
        }
        .validate()
        .is_err());
    }
}
