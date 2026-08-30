use async_trait::async_trait;
use insight_platform_artifacts::{StageWorkloadArtifactRequest, StagedWorkloadArtifact};
use insight_platform_context::{
    CommitContextDatasetBuild, ContextDatasetBuildJobPayload, FailContextDatasetBuildVerification,
    ParkContextDatasetBuildVerification, CONTEXT_DATASET_INDEX_MANIFEST_MEDIA_TYPE,
    CONTEXT_DATASET_VALIDATION_EVIDENCE_MEDIA_TYPE, MAX_CONTEXT_DATASET_ARTIFACT_BYTES,
};
use insight_platform_contracts::{
    canonical_digest, canonical_json, ContextDatasetGenerationSpec, ResourceId, ResourceKind,
    Sha256Digest, WorkClass,
};
use insight_platform_jobs::JobFence;
use insight_platform_postgres::{
    context_dataset_repository::ResolvedContextDatasetBuild,
    repository::{
        ClaimJobs, HeartbeatJob, JobFence as RepositoryJobFence, JobRecord, PgRepository,
        RepositoryError,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeSet, error::Error, fmt, sync::Arc, time::Duration};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const CONTEXT_DATASET_BUILDER_ROLE: &str = "context-dataset-worker";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextDatasetBuilderSource {
    pub schema_version: u32,
    pub context_deployment_digest: Sha256Digest,
    pub source_manifest_digest: Sha256Digest,
    pub items: Vec<Value>,
}

impl ContextDatasetBuilderSource {
    pub fn validate(&self) -> Result<(), ContextDatasetBuilderError> {
        let (index_value, validation_value) = self.descriptor_values()?;
        if self.schema_version != 1
            || self.items.is_empty()
            || self.items.len() > 256
            || canonical_digest(&index_value).ok().as_deref()
                != Some(self.source_manifest_digest.as_str())
            || canonical_json(&index_value).map_or(true, |bytes| {
                u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_CONTEXT_DATASET_ARTIFACT_BYTES
            })
            || canonical_json(&validation_value).map_or(true, |bytes| {
                u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_CONTEXT_DATASET_ARTIFACT_BYTES
            })
        {
            return Err(ContextDatasetBuilderError::InvalidConfiguration);
        }
        Ok(())
    }

    fn descriptor_values(&self) -> Result<(Value, Value), ContextDatasetBuilderError> {
        let items = self
            .items
            .iter()
            .enumerate()
            .map(|(ordinal, content)| {
                Ok(json!({
                    "content": content,
                    "ordinal": u32::try_from(ordinal)
                        .map_err(|_| ContextDatasetBuilderError::InvalidConfiguration)?,
                }))
            })
            .collect::<Result<Vec<_>, ContextDatasetBuilderError>>()?;
        let index = json!({"items": items, "schema_version": 1});
        let index_digest: Sha256Digest = canonical_digest(&index)
            .map_err(|_| ContextDatasetBuilderError::InvalidConfiguration)?
            .parse()
            .map_err(|_| ContextDatasetBuilderError::InvalidConfiguration)?;
        let validation = json!({
            "index_manifest_digest": index_digest,
            "item_count": self.items.len(),
            "schema_version": 1,
            "valid": true,
        });
        Ok((index, validation))
    }

    fn descriptors(&self) -> Result<[DatasetDescriptor; 2], ContextDatasetBuilderError> {
        self.validate()?;
        let (index, validation) = self.descriptor_values()?;
        Ok([
            DatasetDescriptor::new(index, CONTEXT_DATASET_INDEX_MANIFEST_MEDIA_TYPE)?,
            DatasetDescriptor::new(validation, CONTEXT_DATASET_VALIDATION_EVIDENCE_MEDIA_TYPE)?,
        ])
    }
}

struct DatasetDescriptor {
    bytes: Vec<u8>,
    digest: Sha256Digest,
    media_type: String,
}

impl DatasetDescriptor {
    fn new(value: Value, media_type: &str) -> Result<Self, ContextDatasetBuilderError> {
        let bytes =
            canonical_json(&value).map_err(|_| ContextDatasetBuilderError::InvalidConfiguration)?;
        if bytes.is_empty()
            || u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_CONTEXT_DATASET_ARTIFACT_BYTES
        {
            return Err(ContextDatasetBuilderError::InvalidConfiguration);
        }
        let digest = canonical_digest(&value)
            .map_err(|_| ContextDatasetBuilderError::InvalidConfiguration)?
            .parse()
            .map_err(|_| ContextDatasetBuilderError::InvalidConfiguration)?;
        Ok(Self {
            bytes,
            digest,
            media_type: media_type.to_owned(),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ContextDatasetBuilderTiming {
    pub scan_interval: Duration,
    pub failure_backoff: Duration,
    pub heartbeat_interval: Duration,
    pub retry_backoff: Duration,
    pub drain_grace: Duration,
}

#[derive(Debug, Clone)]
pub struct ContextDatasetBuilderConfig {
    pub claim_batch_size: u16,
    pub recovery_batch_size: u16,
    pub maximum_concurrency: usize,
    pub lease_milliseconds: i64,
    pub timing: ContextDatasetBuilderTiming,
}

impl ContextDatasetBuilderConfig {
    pub fn validate(&self) -> Result<(), ContextDatasetBuilderError> {
        let lease = u64::try_from(self.lease_milliseconds)
            .ok()
            .map(Duration::from_millis);
        if self.claim_batch_size == 0
            || self.claim_batch_size > 256
            || self.recovery_batch_size == 0
            || self.recovery_batch_size > 256
            || self.maximum_concurrency == 0
            || self.maximum_concurrency > 256
            || usize::from(self.claim_batch_size) > self.maximum_concurrency
            || lease.is_none()
            || lease.is_some_and(|lease| lease.is_zero())
            || self.timing.scan_interval.is_zero()
            || self.timing.scan_interval > Duration::from_secs(60)
            || self.timing.failure_backoff.is_zero()
            || self.timing.failure_backoff > self.timing.scan_interval
            || self.timing.heartbeat_interval.is_zero()
            || lease.is_some_and(|lease| self.timing.heartbeat_interval >= lease / 3)
            || self.timing.retry_backoff.is_zero()
            || self.timing.retry_backoff > Duration::from_secs(60)
            || self.timing.drain_grace.is_zero()
            || self.timing.drain_grace > Duration::from_secs(120)
        {
            return Err(ContextDatasetBuilderError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextDatasetArtifactStageError {
    InvalidOrStale,
    Unavailable,
    CommitUncertain,
}

#[async_trait]
pub trait ContextDatasetArtifactStager: Send + Sync {
    async fn stage_context_dataset_artifact(
        &self,
        request: StageWorkloadArtifactRequest,
    ) -> Result<StagedWorkloadArtifact, ContextDatasetArtifactStageError>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ContextDatasetBuilderReport {
    pub claimed: u64,
    pub recovered: u64,
    pub settled: u64,
    pub abandoned: u64,
}

pub struct ContextDatasetBuilderDriver {
    repository: Arc<PgRepository>,
    stager: Arc<dyn ContextDatasetArtifactStager>,
    process_generation_id: ResourceId,
    sources: Vec<ContextDatasetBuilderSource>,
    capacity: Arc<Semaphore>,
    config: ContextDatasetBuilderConfig,
}

impl ContextDatasetBuilderDriver {
    pub fn new(
        repository: Arc<PgRepository>,
        stager: Arc<dyn ContextDatasetArtifactStager>,
        process_generation_id: ResourceId,
        mut sources: Vec<ContextDatasetBuilderSource>,
        config: ContextDatasetBuilderConfig,
    ) -> Result<Self, ContextDatasetBuilderError> {
        config.validate()?;
        if process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || sources.is_empty()
            || sources.len() > 64
        {
            return Err(ContextDatasetBuilderError::InvalidConfiguration);
        }
        sources.sort_by(|left, right| {
            left.context_deployment_digest
                .cmp(&right.context_deployment_digest)
        });
        let mut identities = BTreeSet::new();
        for source in &sources {
            source.validate()?;
            if !identities.insert(source.context_deployment_digest.clone()) {
                return Err(ContextDatasetBuilderError::InvalidConfiguration);
            }
        }
        Ok(Self {
            repository,
            stager,
            process_generation_id,
            sources,
            capacity: Arc::new(Semaphore::new(config.maximum_concurrency)),
            config,
        })
    }

    pub fn available_permits(&self) -> usize {
        self.capacity.available_permits()
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<bool>,
    ) -> Result<(usize, u64), ContextDatasetBuilderError> {
        let supported = self
            .sources
            .iter()
            .map(|source| source.context_deployment_digest.clone())
            .collect::<Vec<_>>();
        let recovered = self
            .repository
            .recover_expired_context_dataset_build_jobs_for_sources(
                self.config.recovery_batch_size,
                i64::try_from(self.config.timing.retry_backoff.as_millis())
                    .map_err(|_| ContextDatasetBuilderError::InvalidConfiguration)?,
                &supported,
            )
            .await?;
        let claim_limit = self
            .capacity
            .available_permits()
            .min(usize::from(self.config.claim_batch_size));
        if claim_limit == 0 {
            return Ok((0, recovered));
        }
        let lease_token_digests = (0..claim_limit)
            .map(|_| new_digest())
            .collect::<Result<Vec<_>, _>>()?;
        let claims = self
            .repository
            .claim_context_dataset_build_jobs_for_sources(
                ClaimJobs {
                    work_class: WorkClass::Context.as_str().to_owned(),
                    worker_id: self.process_generation_id.clone(),
                    limit: u16::try_from(claim_limit)
                        .map_err(|_| ContextDatasetBuilderError::InvalidConfiguration)?,
                    lease_milliseconds: self.config.lease_milliseconds,
                    lease_token_digests,
                },
                &supported,
            )
            .await?;
        let count = claims.len();
        for claim in claims {
            let permit = Arc::clone(&self.capacity)
                .try_acquire_owned()
                .map_err(|_| ContextDatasetBuilderError::CorruptClaim)?;
            let repository = Arc::clone(&self.repository);
            let stager = Arc::clone(&self.stager);
            let sources = self.sources.clone();
            let config = self.config.clone();
            active.spawn(async move {
                let _permit = permit;
                execute_claim(repository, stager, sources, config, claim)
                    .await
                    .is_ok()
            });
        }
        Ok((count, recovered))
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<ContextDatasetBuilderReport, ContextDatasetBuilderError> {
        let mut active = JoinSet::new();
        let mut report = ContextDatasetBuilderReport::default();
        let mut scan = tokio::time::interval(self.config.timing.scan_interval);
        scan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                joined = active.join_next(), if !active.is_empty() => {
                    match joined { Some(Ok(true)) => report.settled += 1, _ => report.abandoned += 1 }
                }
                _ = scan.tick() => {
                    match self.drive_once(&mut active).await {
                        Ok((claimed, recovered)) => {
                            report.claimed += u64::try_from(claimed)
                                .map_err(|_| ContextDatasetBuilderError::InvalidConfiguration)?;
                            report.recovered = report.recovered.checked_add(recovered)
                                .ok_or(ContextDatasetBuilderError::InvalidConfiguration)?;
                        }
                        Err(ContextDatasetBuilderError::Repository(RepositoryError::Conflict(_)
                            | RepositoryError::StaleFence)) => {
                            tokio::select! {
                                _ = cancellation.cancelled() => break,
                                _ = tokio::time::sleep(self.config.timing.failure_backoff) => {}
                            }
                        }
                        Err(failure) => return Err(failure),
                    }
                }
            }
        }
        let drain = async {
            while let Some(joined) = active.join_next().await {
                match joined {
                    Ok(true) => report.settled += 1,
                    _ => report.abandoned += 1,
                }
            }
        };
        tokio::time::timeout(self.config.timing.drain_grace, drain)
            .await
            .map_err(|_| ContextDatasetBuilderError::DrainRequiresTermination)?;
        Ok(report)
    }
}

async fn execute_claim(
    repository: Arc<PgRepository>,
    stager: Arc<dyn ContextDatasetArtifactStager>,
    sources: Vec<ContextDatasetBuilderSource>,
    config: ContextDatasetBuilderConfig,
    claim: JobRecord,
) -> Result<(), ContextDatasetBuilderError> {
    let tenant_id = ResourceId::parse_expected(&claim.tenant_id, ResourceKind::Tenant)
        .map_err(|_| ContextDatasetBuilderError::CorruptClaim)?;
    let job_id = ResourceId::parse_expected(&claim.job_id, ResourceKind::Job)
        .map_err(|_| ContextDatasetBuilderError::CorruptClaim)?;
    let dataset_id = ResourceId::parse_expected(&claim.owner_id, ResourceKind::ContextDataset)
        .map_err(|_| ContextDatasetBuilderError::CorruptClaim)?;
    let worker_id = claim
        .worker_id
        .as_deref()
        .ok_or(ContextDatasetBuilderError::CorruptClaim)?
        .parse::<ResourceId>()
        .map_err(|_| ContextDatasetBuilderError::CorruptClaim)?;
    let token = claim
        .lease_token_digest
        .as_deref()
        .ok_or(ContextDatasetBuilderError::CorruptClaim)?
        .parse::<Sha256Digest>()
        .map_err(|_| ContextDatasetBuilderError::CorruptClaim)?;
    if claim.job_kind != "context_dataset_build"
        || claim.work_class != WorkClass::Context.as_str()
        || claim.owner_kind != ResourceKind::ContextDataset.descriptor().name
        || claim.state != "leased"
        || claim.lease_epoch <= 0
    {
        return Err(ContextDatasetBuilderError::CorruptClaim);
    }
    let started = repository
        .start_context_dataset_build_job(RepositoryJobFence {
            tenant_id: claim.tenant_id,
            job_id: claim.job_id,
            worker_id: worker_id.clone(),
            lease_epoch: claim.lease_epoch,
            expected_job_version: claim.version,
            lease_token_digest: token.clone(),
        })
        .await?;
    let payload: ContextDatasetBuildJobPayload =
        serde_json::from_value(started.payload.value.clone())
            .map_err(|_| ContextDatasetBuilderError::CorruptClaim)?;
    payload
        .validate_for_owner(&dataset_id)
        .map_err(|_| ContextDatasetBuilderError::CorruptClaim)?;
    if payload.job_id != job_id {
        return Err(ContextDatasetBuilderError::CorruptClaim);
    }
    let source = sources
        .iter()
        .find(|source| {
            source.context_deployment_digest == payload.context_deployment.deployment_digest
        })
        .ok_or(ContextDatasetBuilderError::CorruptClaim)?;
    let descriptors = source.descriptors()?;
    let mut fence = JobFence {
        expected_version: u64::try_from(started.version)
            .map_err(|_| ContextDatasetBuilderError::CorruptClaim)?,
        worker_process_generation_id: worker_id,
        lease_generation: u64::try_from(started.lease_epoch)
            .map_err(|_| ContextDatasetBuilderError::CorruptClaim)?,
        token_digest: token,
    };
    let allocations = [
        &payload.artifact_preallocations.index_manifest,
        &payload.artifact_preallocations.validation_evidence,
    ];
    for (allocation, descriptor) in allocations.into_iter().zip(&descriptors) {
        let request = StageWorkloadArtifactRequest {
            schema_version: 1,
            tenant_id: tenant_id.clone(),
            producer_job_id: job_id.clone(),
            producer_fence: fence.clone(),
            verification_job_id: allocation.verification_job_id.clone(),
            artifact_id: allocation.artifact_id.clone(),
            blob_id: allocation.blob_id.clone(),
            descriptor_bytes: descriptor.bytes.clone(),
            descriptor_digest: descriptor.digest.clone(),
            media_type: descriptor.media_type.clone(),
        };
        let (staged, next_fence) = stage_with_heartbeats(
            &repository,
            stager.as_ref(),
            &tenant_id,
            &job_id,
            fence,
            &config,
            request,
        )
        .await?;
        fence = next_fence;
        if staged.artifact_id != allocation.artifact_id
            || staged.blob_id != allocation.blob_id
            || staged.verification_job_id != allocation.verification_job_id
            || staged.content_digest != descriptor.digest
            || staged.size_bytes != u64::try_from(descriptor.bytes.len()).unwrap_or(u64::MAX)
        {
            return Err(ContextDatasetBuilderError::ArtifactIntegrity);
        }
    }
    complete_or_park(&repository, &tenant_id, &job_id, &dataset_id, fence, source).await
}

async fn stage_with_heartbeats(
    repository: &PgRepository,
    stager: &dyn ContextDatasetArtifactStager,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    mut fence: JobFence,
    config: &ContextDatasetBuilderConfig,
    request: StageWorkloadArtifactRequest,
) -> Result<(StagedWorkloadArtifact, JobFence), ContextDatasetBuilderError> {
    let stage = stager.stage_context_dataset_artifact(request);
    tokio::pin!(stage);
    let sleep = tokio::time::sleep(config.timing.heartbeat_interval);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            result = &mut stage => {
                return result
                    .map(|staged| (staged, fence))
                    .map_err(ContextDatasetBuilderError::ArtifactStage);
            }
            () = &mut sleep => {
                fence = heartbeat(repository, tenant_id, job_id, fence, config).await?;
                sleep.as_mut().reset(
                    tokio::time::Instant::now() + config.timing.heartbeat_interval,
                );
            }
        }
    }
}

async fn heartbeat(
    repository: &PgRepository,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    fence: JobFence,
    config: &ContextDatasetBuilderConfig,
) -> Result<JobFence, ContextDatasetBuilderError> {
    let heartbeat = repository
        .heartbeat_job(HeartbeatJob {
            fence: RepositoryJobFence {
                tenant_id: tenant_id.to_string(),
                job_id: job_id.to_string(),
                worker_id: fence.worker_process_generation_id.clone(),
                lease_epoch: i64::try_from(fence.lease_generation)
                    .map_err(|_| ContextDatasetBuilderError::CorruptClaim)?,
                expected_job_version: i64::try_from(fence.expected_version)
                    .map_err(|_| ContextDatasetBuilderError::CorruptClaim)?,
                lease_token_digest: fence.token_digest.clone(),
            },
            lease_milliseconds: config.lease_milliseconds,
        })
        .await?;
    Ok(JobFence {
        expected_version: u64::try_from(heartbeat.version)
            .map_err(|_| ContextDatasetBuilderError::CorruptClaim)?,
        ..fence
    })
}

async fn complete_or_park(
    repository: &PgRepository,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    dataset_id: &ResourceId,
    fence: JobFence,
    source: &ContextDatasetBuilderSource,
) -> Result<(), ContextDatasetBuilderError> {
    match repository
        .resolve_context_dataset_build(tenant_id.clone(), job_id.clone(), fence.clone())
        .await
    {
        Ok(resolved) => {
            commit_verified_dataset(
                repository, tenant_id, job_id, dataset_id, fence, source, resolved,
            )
            .await
        }
        Err(failure) if verification_is_pending(&failure) => {
            match fail_verified_dataset(repository, tenant_id, job_id, dataset_id, fence.clone())
                .await
            {
                Ok(()) => Ok(()),
                Err(ContextDatasetBuilderError::Repository(failure))
                    if verification_is_pending(&failure) =>
                {
                    match repository
                        .park_context_dataset_build_for_verification(
                            ParkContextDatasetBuildVerification {
                                tenant_id: tenant_id.clone(),
                                job_id: job_id.clone(),
                                dataset_id: dataset_id.clone(),
                                fence: fence.clone(),
                                event_id: new_id(ResourceKind::Event)?,
                                outbox_id: new_id(ResourceKind::OutboxEvent)?,
                            },
                        )
                        .await
                    {
                        Ok(_) => Ok(()),
                        Err(RepositoryError::Conflict(_)) => {
                            fail_verified_dataset(repository, tenant_id, job_id, dataset_id, fence)
                                .await
                        }
                        Err(failure) => Err(failure.into()),
                    }
                }
                Err(failure) => Err(failure),
            }
        }
        Err(failure) => Err(failure.into()),
    }
}

async fn commit_verified_dataset(
    repository: &PgRepository,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    dataset_id: &ResourceId,
    fence: JobFence,
    source: &ContextDatasetBuilderSource,
    resolved: ResolvedContextDatasetBuild,
) -> Result<(), ContextDatasetBuilderError> {
    if resolved.index_manifest.content_digest() != &source.source_manifest_digest {
        return Err(ContextDatasetBuilderError::ArtifactIntegrity);
    }
    repository
        .commit_context_dataset_build(CommitContextDatasetBuild {
            tenant_id: tenant_id.clone(),
            job_id: job_id.clone(),
            dataset_id: dataset_id.clone(),
            generation_id: resolved
                .payload
                .artifact_preallocations
                .generation_id
                .clone(),
            fence: fence.clone(),
            lease_token_digest: fence.token_digest.clone(),
            generation: ContextDatasetGenerationSpec {
                context_deployment: resolved.payload.context_deployment,
                source_manifest_digest: source.source_manifest_digest.clone(),
                parser_profile: resolved.payload.parser_profile,
                chunker_profile: resolved.payload.chunker_profile,
                embedding_model_deployment: resolved.payload.embedding_model_deployment,
                ranking_profile: resolved.payload.ranking_profile,
                index_manifest: resolved.index_manifest,
                validation_evidence: resolved.validation_evidence,
                created_by_operation_id: job_id.clone(),
            },
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
        })
        .await?;
    Ok(())
}

async fn fail_verified_dataset(
    repository: &PgRepository,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    dataset_id: &ResourceId,
    fence: JobFence,
) -> Result<(), ContextDatasetBuilderError> {
    repository
        .fail_context_dataset_build_verification(FailContextDatasetBuildVerification {
            tenant_id: tenant_id.clone(),
            job_id: job_id.clone(),
            dataset_id: dataset_id.clone(),
            fence,
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
        })
        .await?;
    Ok(())
}

fn verification_is_pending(failure: &RepositoryError) -> bool {
    matches!(
        failure,
        RepositoryError::Conflict(_) | RepositoryError::NotFound(_)
    )
}

fn new_id(kind: ResourceKind) -> Result<ResourceId, ContextDatasetBuilderError> {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7())
        .map_err(|_| ContextDatasetBuilderError::InvalidGeneratedCommand)
}

fn new_digest() -> Result<Sha256Digest, ContextDatasetBuilderError> {
    canonical_digest(&json!({"nonce": Uuid::now_v7()}))
        .map_err(|_| ContextDatasetBuilderError::InvalidGeneratedCommand)?
        .parse()
        .map_err(|_| ContextDatasetBuilderError::InvalidGeneratedCommand)
}

#[derive(Debug)]
pub enum ContextDatasetBuilderError {
    InvalidConfiguration,
    InvalidGeneratedCommand,
    CorruptClaim,
    ArtifactStage(ContextDatasetArtifactStageError),
    ArtifactIntegrity,
    DrainRequiresTermination,
    Repository(RepositoryError),
}

impl fmt::Display for ContextDatasetBuilderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => formatter.write_str("invalid Dataset Builder config"),
            Self::InvalidGeneratedCommand => {
                formatter.write_str("Dataset Builder generated an invalid command")
            }
            Self::CorruptClaim => formatter.write_str("Dataset Builder claim is corrupt"),
            Self::ArtifactStage(_) => formatter.write_str("Dataset Artifact staging failed"),
            Self::ArtifactIntegrity => {
                formatter.write_str("Dataset Artifact staging evidence is invalid")
            }
            Self::DrainRequiresTermination => {
                formatter.write_str("Dataset Builder drain exceeded its bound")
            }
            Self::Repository(failure) => write!(formatter, "Dataset authority failed: {failure}"),
        }
    }
}

impl Error for ContextDatasetBuilderError {}

impl From<RepositoryError> for ContextDatasetBuilderError {
    fn from(value: RepositoryError) -> Self {
        Self::Repository(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(value: &Value) -> Sha256Digest {
        canonical_digest(value).unwrap().parse().unwrap()
    }

    fn source() -> ContextDatasetBuilderSource {
        let content = json!({"text": "bounded context"});
        let index = json!({
            "items": [{"content": content.clone(), "ordinal": 0}],
            "schema_version": 1,
        });
        ContextDatasetBuilderSource {
            schema_version: 1,
            context_deployment_digest: digest(&json!({"deployment": "context"})),
            source_manifest_digest: digest(&index),
            items: vec![content],
        }
    }

    fn config() -> ContextDatasetBuilderConfig {
        ContextDatasetBuilderConfig {
            claim_batch_size: 4,
            recovery_batch_size: 4,
            maximum_concurrency: 4,
            lease_milliseconds: 30_000,
            timing: ContextDatasetBuilderTiming {
                scan_interval: Duration::from_millis(500),
                failure_backoff: Duration::from_millis(250),
                heartbeat_interval: Duration::from_secs(5),
                retry_backoff: Duration::from_secs(1),
                drain_grace: Duration::from_secs(30),
            },
        }
    }

    #[test]
    fn source_manifest_binds_the_exact_canonical_index() {
        let source = source();
        source.validate().unwrap();
        let descriptors = source.descriptors().unwrap();
        assert_eq!(descriptors[0].digest, source.source_manifest_digest);
        assert_eq!(
            descriptors[0].media_type,
            CONTEXT_DATASET_INDEX_MANIFEST_MEDIA_TYPE
        );
        let mut drifted = source;
        drifted.items[0] = json!({"text": "changed"});
        assert!(matches!(
            drifted.validate(),
            Err(ContextDatasetBuilderError::InvalidConfiguration)
        ));
    }

    #[test]
    fn driver_bounds_claim_recovery_heartbeat_and_drain() {
        config().validate().unwrap();
        let mut unsafe_heartbeat = config();
        unsafe_heartbeat.timing.heartbeat_interval = Duration::from_secs(10);
        assert!(unsafe_heartbeat.validate().is_err());
        let mut oversized_recovery = config();
        oversized_recovery.recovery_batch_size = 257;
        assert!(oversized_recovery.validate().is_err());
    }
}
