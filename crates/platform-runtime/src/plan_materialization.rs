//! Exact Scheduler materialization of the immutable Typed Plan bound to a running Job.

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_artifacts::{
    ArtifactObjectReadAuthorityError, SchedulerTypedPlanLease, SchedulerTypedPlanReadError,
    SchedulerTypedPlanReadRequest, SchedulerTypedPlanReader, SchedulerTypedPlanRequestResolver,
    MAX_TYPED_PLAN_ARTIFACT_BYTES,
};
use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, JsonLimits, ResourceId, ResourceKind,
};
use insight_platform_orchestrator::{PlanLimits, RuntimePlan};
use insight_platform_postgres::repository::{JobFence, JobRecord};
use serde_json::json;
use std::{error::Error, fmt, sync::Arc, time::Duration};

#[derive(Debug, Clone, Copy)]
pub struct SchedulerPlanMaterializerConfig {
    pub request_timeout: Duration,
    pub maximum_bytes: usize,
    pub json_limits: JsonLimits,
    pub plan_limits: PlanLimits,
}

impl SchedulerPlanMaterializerConfig {
    pub fn validate(self) -> Result<(), SchedulerPlanMaterializerError> {
        if self.request_timeout.is_zero()
            || self.maximum_bytes == 0
            || self.maximum_bytes > MAX_TYPED_PLAN_ARTIFACT_BYTES
        {
            return Err(SchedulerPlanMaterializerError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MaterializedTypedPlan {
    pub request: SchedulerTypedPlanReadRequest,
    pub plan: RuntimePlan,
}

pub struct SchedulerPlanMaterializer<A, R> {
    authority: Arc<A>,
    reader: Arc<R>,
    config: SchedulerPlanMaterializerConfig,
}

#[async_trait]
pub trait TypedPlanMaterializer: Send + Sync + 'static {
    async fn materialize_plan(
        &self,
        job: &JobRecord,
        fence: &JobFence,
    ) -> Result<MaterializedTypedPlan, SchedulerPlanMaterializerError>;
}

impl<A, R> SchedulerPlanMaterializer<A, R>
where
    A: SchedulerTypedPlanRequestResolver,
    R: SchedulerTypedPlanReader,
{
    pub fn new(
        authority: Arc<A>,
        reader: Arc<R>,
        config: SchedulerPlanMaterializerConfig,
    ) -> Result<Self, SchedulerPlanMaterializerError> {
        config.validate()?;
        Ok(Self {
            authority,
            reader,
            config,
        })
    }

    pub async fn materialize(
        &self,
        job: &JobRecord,
        fence: &JobFence,
    ) -> Result<MaterializedTypedPlan, SchedulerPlanMaterializerError> {
        validate_running_fence(job, fence)?;
        let lease_expires_at = job
            .lease_expires_at
            .ok_or(SchedulerPlanMaterializerError::FenceLost)?;
        let timeout = ChronoDuration::from_std(self.config.request_timeout)
            .map_err(|_| SchedulerPlanMaterializerError::InvalidConfiguration)?;
        let deadline = (Utc::now() + timeout)
            .min(lease_expires_at)
            .min(job.deadline);
        if deadline <= Utc::now() {
            return Err(SchedulerPlanMaterializerError::FenceLost);
        }
        let run_id = job
            .run_id
            .as_deref()
            .ok_or(SchedulerPlanMaterializerError::InvariantViolation)?
            .parse::<ResourceId>()
            .map_err(|_| SchedulerPlanMaterializerError::InvariantViolation)?;
        let request_digest = canonical_digest(&json!({
            "job_id": fence.job_id,
            "lease_generation": fence.lease_epoch,
            "lease_token_digest": fence.lease_token_digest,
            "operation": "scheduler.typed-plan.materialize",
            "run_id": run_id,
            "worker_process_generation_id": fence.worker_id,
        }))
        .map_err(|_| SchedulerPlanMaterializerError::InvariantViolation)?
        .parse()
        .map_err(|_| SchedulerPlanMaterializerError::InvariantViolation)?;
        let request = self
            .authority
            .resolve_typed_plan_read(SchedulerTypedPlanLease {
                tenant_id: fence
                    .tenant_id
                    .parse()
                    .map_err(|_| SchedulerPlanMaterializerError::InvariantViolation)?,
                run_id,
                orchestration_job_id: fence
                    .job_id
                    .parse()
                    .map_err(|_| SchedulerPlanMaterializerError::InvariantViolation)?,
                worker_process_generation_id: fence.worker_id.clone(),
                lease_generation: u64::try_from(fence.lease_epoch)
                    .map_err(|_| SchedulerPlanMaterializerError::InvariantViolation)?,
                lease_token_digest: fence.lease_token_digest.clone(),
                request_digest,
                maximum_bytes: self.config.maximum_bytes,
                deadline,
            })
            .await
            .map_err(map_authority_error)?;
        let bytes = self
            .reader
            .read_exact(request.clone())
            .await
            .map_err(map_read_error)?;
        let value = parse_strict_json(&bytes, self.config.json_limits)
            .map_err(|_| SchedulerPlanMaterializerError::Integrity)?;
        if canonical_json(&value).as_deref() != Ok(bytes.as_slice()) {
            return Err(SchedulerPlanMaterializerError::Integrity);
        }
        let plan: RuntimePlan =
            serde_json::from_value(value).map_err(|_| SchedulerPlanMaterializerError::Integrity)?;
        plan.validate(self.config.plan_limits)
            .map_err(|_| SchedulerPlanMaterializerError::Integrity)?;
        if plan
            .canonical_digest(self.config.plan_limits)
            .map_err(|_| SchedulerPlanMaterializerError::Integrity)?
            != *request.artifact.content_digest()
        {
            return Err(SchedulerPlanMaterializerError::Integrity);
        }
        Ok(MaterializedTypedPlan { request, plan })
    }
}

#[async_trait]
impl<A, R> TypedPlanMaterializer for SchedulerPlanMaterializer<A, R>
where
    A: SchedulerTypedPlanRequestResolver + 'static,
    R: SchedulerTypedPlanReader + 'static,
{
    async fn materialize_plan(
        &self,
        job: &JobRecord,
        fence: &JobFence,
    ) -> Result<MaterializedTypedPlan, SchedulerPlanMaterializerError> {
        self.materialize(job, fence).await
    }
}

fn validate_running_fence(
    job: &JobRecord,
    fence: &JobFence,
) -> Result<(), SchedulerPlanMaterializerError> {
    let worker_id = fence.worker_id.to_string();
    if job.work_class != "orchestration"
        || job.state != "running"
        || job.tenant_id != fence.tenant_id
        || job.job_id != fence.job_id
        || job.worker_id.as_deref() != Some(worker_id.as_str())
        || job.lease_epoch != fence.lease_epoch
        || job.version != fence.expected_job_version
        || job.lease_token_digest.as_deref() != Some(fence.lease_token_digest.as_str())
        || fence.worker_id.kind() != ResourceKind::WorkerProcessGeneration
    {
        return Err(SchedulerPlanMaterializerError::FenceLost);
    }
    Ok(())
}

fn map_authority_error(error: ArtifactObjectReadAuthorityError) -> SchedulerPlanMaterializerError {
    match error {
        ArtifactObjectReadAuthorityError::Unavailable => {
            SchedulerPlanMaterializerError::Unavailable
        }
        ArtifactObjectReadAuthorityError::Denied | ArtifactObjectReadAuthorityError::NotFound => {
            SchedulerPlanMaterializerError::FenceLost
        }
        ArtifactObjectReadAuthorityError::InvalidEvidence => {
            SchedulerPlanMaterializerError::InvariantViolation
        }
    }
}

fn map_read_error(error: SchedulerTypedPlanReadError) -> SchedulerPlanMaterializerError {
    match error {
        SchedulerTypedPlanReadError::Unavailable => SchedulerPlanMaterializerError::Unavailable,
        SchedulerTypedPlanReadError::Denied | SchedulerTypedPlanReadError::NotFound => {
            SchedulerPlanMaterializerError::FenceLost
        }
        SchedulerTypedPlanReadError::TooLarge | SchedulerTypedPlanReadError::Integrity => {
            SchedulerPlanMaterializerError::Integrity
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerPlanMaterializerError {
    InvalidConfiguration,
    Unavailable,
    FenceLost,
    InvariantViolation,
    Integrity,
}

impl fmt::Display for SchedulerPlanMaterializerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "Scheduler Plan materializer configuration is invalid",
            Self::Unavailable => "Scheduler Plan materializer dependency is unavailable",
            Self::FenceLost => "Scheduler Plan materialization fence was lost",
            Self::InvariantViolation => "Scheduler Plan authority evidence is invalid",
            Self::Integrity => "Scheduler Plan bytes failed integrity validation",
        })
    }
}

impl Error for SchedulerPlanMaterializerError {}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use insight_platform_contracts::{
        checked_in_hard_limit_profile, ArtifactRef, DataClassification, SchedulerPriority,
        Sha256Digest, WorkClass,
    };
    use insight_platform_orchestrator::{ExactDataPortRef, PlanNodeKey, RuntimeNode};
    use insight_platform_postgres::repository::TypedPayload;
    use std::{collections::BTreeMap, sync::Mutex};

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

    fn plan() -> RuntimePlan {
        let start = PlanNodeKey::new("start".to_owned()).unwrap();
        let finish = PlanNodeKey::new("finish".to_owned()).unwrap();
        RuntimePlan {
            plan_version: 5,
            interface_contract_digest: digest('5'),
            entry_node_id: start.clone(),
            dependency_slots: BTreeMap::new(),
            nodes: BTreeMap::from([
                (
                    start,
                    RuntimeNode::Start {
                        next: finish.clone(),
                    },
                ),
                (
                    finish,
                    RuntimeNode::Return {
                        value: ExactDataPortRef::RunInput {
                            schema_digest: digest('1'),
                        },
                    },
                ),
            ]),
        }
    }

    fn fence_and_job() -> (JobFence, JobRecord) {
        let now = Utc::now();
        let tenant_id = id(ResourceKind::Tenant, "7702").to_string();
        let job_id = id(ResourceKind::Job, "7703").to_string();
        let worker_id = id(ResourceKind::WorkerProcessGeneration, "7704");
        let token = digest('a');
        let fence = JobFence {
            tenant_id: tenant_id.clone(),
            job_id: job_id.clone(),
            worker_id: worker_id.clone(),
            lease_epoch: 2,
            expected_job_version: 4,
            lease_token_digest: token.clone(),
        };
        let job = JobRecord {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id,
            job_id,
            job_kind: insight_platform_contracts::JobKind::OrchestrationNode
                .as_str()
                .to_owned(),
            work_class: WorkClass::Orchestration.as_str().to_owned(),
            owner_kind: ResourceKind::NodeExecution.descriptor().name.to_owned(),
            owner_id: id(ResourceKind::NodeExecution, "7705").to_string(),
            invocation_id: None,
            run_id: Some(id(ResourceKind::Run, "7706").to_string()),
            node_id: Some(id(ResourceKind::NodeExecution, "7705").to_string()),
            state: "running".to_owned(),
            version: 4,
            attempt_no: 1,
            attempt_limit: 8,
            lease_epoch: 2,
            worker_id: Some(worker_id.to_string()),
            lease_token_digest: Some(token.to_string()),
            lease_expires_at: Some(now + ChronoDuration::seconds(30)),
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
            quota_reservation_id: Some(id(ResourceKind::UsageReservation, "7707").to_string()),
            payload: TypedPayload::new(1, &json!({"kind":"controller"})).unwrap(),
            started_at: Some(now),
            terminal_at: None,
            created_at: now,
            updated_at: now,
        };
        (fence, job)
    }

    struct Resolver {
        artifact: ArtifactRef,
        observed: Mutex<Option<SchedulerTypedPlanLease>>,
    }

    #[async_trait]
    impl SchedulerTypedPlanRequestResolver for Resolver {
        async fn resolve_typed_plan_read(
            &self,
            lease: SchedulerTypedPlanLease,
        ) -> Result<SchedulerTypedPlanReadRequest, ArtifactObjectReadAuthorityError> {
            *self.observed.lock().unwrap() = Some(lease.clone());
            Ok(SchedulerTypedPlanReadRequest {
                tenant_id: lease.tenant_id,
                run_id: lease.run_id,
                orchestration_job_id: lease.orchestration_job_id,
                worker_process_generation_id: lease.worker_process_generation_id,
                lease_generation: lease.lease_generation,
                lease_token_digest: lease.lease_token_digest,
                plan_revision_id: id(ResourceKind::AgentPlanRevision, "7708"),
                artifact: self.artifact.clone(),
                request_digest: lease.request_digest,
                maximum_bytes: lease.maximum_bytes,
                deadline: lease.deadline,
            })
        }
    }

    struct Reader(Vec<u8>);

    #[async_trait]
    impl SchedulerTypedPlanReader for Reader {
        async fn read_exact(
            &self,
            _request: SchedulerTypedPlanReadRequest,
        ) -> Result<Vec<u8>, SchedulerTypedPlanReadError> {
            Ok(self.0.clone())
        }
    }

    fn materializer(bytes: Vec<u8>) -> SchedulerPlanMaterializer<Resolver, Reader> {
        let runtime_plan = plan();
        let plan_limits = PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let artifact = ArtifactRef::new(
            id(ResourceKind::Artifact, "7709"),
            runtime_plan.canonical_digest(plan_limits).unwrap(),
            u64::try_from(bytes.len()).unwrap(),
            "application/json",
            DataClassification::Internal,
            Some("typed-plan.json".to_owned()),
        )
        .unwrap();
        SchedulerPlanMaterializer::new(
            Arc::new(Resolver {
                artifact,
                observed: Mutex::new(None),
            }),
            Arc::new(Reader(bytes)),
            SchedulerPlanMaterializerConfig {
                request_timeout: Duration::from_secs(5),
                maximum_bytes: 1_048_576,
                json_limits: JsonLimits::CONTRACT_FIXTURE,
                plan_limits,
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn exact_canonical_plan_is_materialized_under_the_running_fence() {
        let bytes = canonical_json(&serde_json::to_value(plan()).unwrap()).unwrap();
        let materializer = materializer(bytes);
        let (fence, job) = fence_and_job();
        let materialized = materializer.materialize(&job, &fence).await.unwrap();
        assert_eq!(materialized.plan, plan());
        let lease = materializer
            .authority
            .observed
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(lease.lease_generation, 2);
        assert!(lease.deadline <= job.lease_expires_at.unwrap());
    }

    #[tokio::test]
    async fn noncanonical_plan_and_fence_drift_fail_closed() {
        let mut bytes = canonical_json(&serde_json::to_value(plan()).unwrap()).unwrap();
        bytes.push(b' ');
        let materializer = materializer(bytes);
        let (fence, mut job) = fence_and_job();
        assert!(matches!(
            materializer.materialize(&job, &fence).await,
            Err(SchedulerPlanMaterializerError::Integrity)
        ));
        job.version += 1;
        assert!(matches!(
            materializer.materialize(&job, &fence).await,
            Err(SchedulerPlanMaterializerError::FenceLost)
        ));
    }
}
