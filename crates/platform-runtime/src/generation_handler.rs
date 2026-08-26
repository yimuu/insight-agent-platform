//! Production lifecycle adapter from a started orchestration Job to a durable Plan driver.

use crate::{
    GenerationHandlerDisposition, GenerationHandlerError, GenerationHandoffReason,
    MaterializedTypedPlan, SchedulerPlanMaterializerError, StartedOrchestrationJob,
    StartedOrchestrationJobHandler, TypedPlanMaterializer,
};
use async_trait::async_trait;
use insight_platform_postgres::repository::JobFence;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurablePlanDriverError {
    Unavailable,
    FenceLost,
    InvariantViolation,
}

#[async_trait]
pub trait DurableOrchestrationPlanDriver: Send + Sync + 'static {
    async fn commit_generation(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        materialized: MaterializedTypedPlan,
    ) -> Result<(), DurablePlanDriverError>;

    async fn handoff_generation(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        reason: GenerationHandoffReason,
    ) -> Result<(), DurablePlanDriverError>;
}

pub struct MaterializingOrchestrationJobHandler<M, D> {
    materializer: Arc<M>,
    driver: Arc<D>,
}

impl<M, D> MaterializingOrchestrationJobHandler<M, D>
where
    M: TypedPlanMaterializer,
    D: DurableOrchestrationPlanDriver,
{
    pub fn new(materializer: Arc<M>, driver: Arc<D>) -> Self {
        Self {
            materializer,
            driver,
        }
    }
}

#[async_trait]
impl<M, D> StartedOrchestrationJobHandler for MaterializingOrchestrationJobHandler<M, D>
where
    M: TypedPlanMaterializer,
    D: DurableOrchestrationPlanDriver,
{
    type Outcome = MaterializedTypedPlan;

    async fn run(
        &self,
        job: StartedOrchestrationJob,
    ) -> Result<Self::Outcome, GenerationHandlerError> {
        let fence = fence_from_started(&job)?;
        self.materializer
            .materialize_plan(job.started(), &fence)
            .await
            .map_err(map_materializer_error)
    }

    async fn commit(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        outcome: Self::Outcome,
    ) -> GenerationHandlerDisposition {
        map_driver_disposition(self.driver.commit_generation(job, fence, outcome).await)
    }

    async fn handoff(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        reason: GenerationHandoffReason,
    ) -> GenerationHandlerDisposition {
        map_driver_disposition(self.driver.handoff_generation(job, fence, reason).await)
    }
}

fn fence_from_started(job: &StartedOrchestrationJob) -> Result<JobFence, GenerationHandlerError> {
    let started = job.started();
    let worker_id = started
        .worker_id
        .as_deref()
        .ok_or(GenerationHandlerError::InvariantViolation)?
        .parse()
        .map_err(|_| GenerationHandlerError::InvariantViolation)?;
    let lease_token_digest = started
        .lease_token_digest
        .as_deref()
        .ok_or(GenerationHandlerError::InvariantViolation)?
        .parse()
        .map_err(|_| GenerationHandlerError::InvariantViolation)?;
    if started.state != "running" || started.lease_epoch <= 0 || started.version <= 0 {
        return Err(GenerationHandlerError::InvariantViolation);
    }
    Ok(JobFence {
        tenant_id: started.tenant_id.clone(),
        job_id: started.job_id.clone(),
        worker_id,
        lease_epoch: started.lease_epoch,
        expected_job_version: started.version,
        lease_token_digest,
    })
}

fn map_materializer_error(error: SchedulerPlanMaterializerError) -> GenerationHandlerError {
    match error {
        SchedulerPlanMaterializerError::Unavailable => GenerationHandlerError::Unavailable,
        SchedulerPlanMaterializerError::InvalidConfiguration
        | SchedulerPlanMaterializerError::FenceLost
        | SchedulerPlanMaterializerError::InvariantViolation
        | SchedulerPlanMaterializerError::Integrity => GenerationHandlerError::InvariantViolation,
    }
}

fn map_driver_disposition(
    result: Result<(), DurablePlanDriverError>,
) -> GenerationHandlerDisposition {
    match result {
        Ok(()) => GenerationHandlerDisposition::Committed,
        Err(DurablePlanDriverError::FenceLost) => GenerationHandlerDisposition::FenceLost,
        Err(DurablePlanDriverError::Unavailable | DurablePlanDriverError::InvariantViolation) => {
            GenerationHandlerDisposition::NotCommitted
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use insight_platform_contracts::{
        ResourceId, ResourceKind, SchedulerPriority, Sha256Digest, WorkClass,
    };
    use insight_platform_postgres::repository::{ClaimedOrchestrationJob, JobRecord, TypedPayload};
    use serde_json::json;

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

    fn started() -> StartedOrchestrationJob {
        let now = Utc::now();
        let job = JobRecord {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: id(ResourceKind::Tenant, "8801").to_string(),
            job_id: id(ResourceKind::Job, "8802").to_string(),
            job_kind: insight_platform_contracts::JobKind::OrchestrationNode
                .as_str()
                .to_owned(),
            work_class: WorkClass::Orchestration.as_str().to_owned(),
            owner_kind: ResourceKind::NodeExecution.descriptor().name.to_owned(),
            owner_id: id(ResourceKind::NodeExecution, "8803").to_string(),
            invocation_id: None,
            run_id: Some(id(ResourceKind::Run, "8804").to_string()),
            node_id: Some(id(ResourceKind::NodeExecution, "8803").to_string()),
            state: "running".to_owned(),
            version: 4,
            attempt_no: 1,
            attempt_limit: 8,
            lease_epoch: 2,
            worker_id: Some(id(ResourceKind::WorkerProcessGeneration, "8805").to_string()),
            lease_token_digest: Some(digest('a').to_string()),
            lease_expires_at: Some(now + Duration::seconds(30)),
            heartbeat_at: Some(now),
            scheduled_at: now,
            retry_at: None,
            deadline: now + Duration::minutes(5),
            priority: SchedulerPriority::Normal,
            wake_kind: None,
            wake_state: None,
            wake_generation: 0,
            request_digest: digest('b').to_string(),
            result_digest: None,
            effect_key_digest: None,
            quota_reservation_id: Some(id(ResourceKind::UsageReservation, "8806").to_string()),
            payload: TypedPayload::new(1, &json!({"kind":"controller"})).unwrap(),
            started_at: Some(now),
            terminal_at: None,
            created_at: now,
            updated_at: now,
        };
        let mut claimed_job = job.clone();
        claimed_job.state = "leased".to_owned();
        claimed_job.version = 3;
        claimed_job.attempt_no = 0;
        claimed_job.started_at = None;
        StartedOrchestrationJob::from_parts(
            ClaimedOrchestrationJob {
                job: claimed_job,
                run_version: 2,
                node_version: 2,
                quota_reservation_id: id(ResourceKind::UsageReservation, "8806").to_string(),
                quota_account_ids: vec![],
            },
            job,
        )
    }

    #[test]
    fn started_job_reconstructs_exact_fence_and_driver_results_are_closed() {
        let started = started();
        let fence = fence_from_started(&started).unwrap();
        assert_eq!(fence.expected_job_version, 4);
        assert_eq!(fence.lease_epoch, 2);
        assert_eq!(
            map_driver_disposition(Err(DurablePlanDriverError::FenceLost)),
            GenerationHandlerDisposition::FenceLost
        );
        assert_eq!(
            map_driver_disposition(Err(DurablePlanDriverError::Unavailable)),
            GenerationHandlerDisposition::NotCommitted
        );
        assert_eq!(
            map_materializer_error(SchedulerPlanMaterializerError::Unavailable),
            GenerationHandlerError::Unavailable
        );
    }
}
