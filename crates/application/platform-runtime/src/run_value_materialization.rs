//! Materialize exact Run values through Artifact authority and reader ports.
use crate::{DurablePlanDriverError, StartedOrchestrationJob};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_artifacts::*;
use insight_platform_contracts::*;
use insight_platform_jobs::store::JobCommandFence as JobFence;
use insight_platform_orchestrator::store::{ResolvedExpressionInput, MAX_DESCENDANT_RUNS};
use serde_json::json;
use std::{sync::Arc, time::Duration};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunValueMaterializationError {
    Unavailable,
    FenceLost,
    Integrity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerRunValueReadContext {
    pub tenant_id: ResourceId,
    pub run_id: ResourceId,
    pub orchestration_job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub lease_token_digest: Sha256Digest,
    pub deadline: chrono::DateTime<Utc>,
}

impl ControllerRunValueReadContext {
    pub fn from_started(
        job: &StartedOrchestrationJob,
        fence: &JobFence,
    ) -> Result<Self, DurablePlanDriverError> {
        let started = job.started();
        let run_id = started
            .run_id
            .as_deref()
            .ok_or(DurablePlanDriverError::InvariantViolation)?
            .parse()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let tenant_id = started
            .tenant_id
            .parse()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let orchestration_job_id = started
            .job_id
            .parse()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        if fence.tenant_id != started.tenant_id
            || fence.job_id != started.job_id
            || fence.worker_id.to_string() != started.worker_id.as_deref().unwrap_or_default()
            || fence.lease_epoch != started.lease_epoch
            || fence.lease_token_digest.as_str()
                != started.lease_token_digest.as_deref().unwrap_or_default()
        {
            return Err(DurablePlanDriverError::FenceLost);
        }
        Ok(Self {
            tenant_id,
            run_id,
            orchestration_job_id,
            worker_process_generation_id: fence.worker_id.clone(),
            lease_generation: u64::try_from(fence.lease_epoch)
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
            lease_token_digest: fence.lease_token_digest.clone(),
            deadline: started.deadline,
        })
    }
}

#[async_trait]
pub trait ControllerRunValueMaterializer: Send + Sync + 'static {
    async fn materialize(
        &self,
        context: &ControllerRunValueReadContext,
        input: &ResolvedExpressionInput,
    ) -> Result<ClosedJsonValue, RunValueMaterializationError>;
}

/// Useful for installations whose effective Plan only permits Inline expression inputs.
pub struct InlineControllerRunValueMaterializer;

#[async_trait]
impl ControllerRunValueMaterializer for InlineControllerRunValueMaterializer {
    async fn materialize(
        &self,
        _context: &ControllerRunValueReadContext,
        input: &ResolvedExpressionInput,
    ) -> Result<ClosedJsonValue, RunValueMaterializationError> {
        let ValueRef::Inline { value } = &input.value else {
            return Err(RunValueMaterializationError::Unavailable);
        };
        let materialized = ClosedJsonValue::build(input.schema_digest.clone(), value.clone())
            .map_err(|_| RunValueMaterializationError::Integrity)?;
        if materialized.canonical_digest != input.content_digest {
            return Err(RunValueMaterializationError::Integrity);
        }
        Ok(materialized)
    }
}

pub struct SchedulerControllerRunValueMaterializer<A, R> {
    resolver: Arc<A>,
    reader: Arc<R>,
    request_timeout: Duration,
}

impl<A, R> SchedulerControllerRunValueMaterializer<A, R>
where
    A: SchedulerRunValueRequestResolver,
    R: SchedulerRunValueReader,
{
    pub fn new(
        resolver: Arc<A>,
        reader: Arc<R>,
        request_timeout: Duration,
    ) -> Result<Self, RunValueMaterializationError> {
        if request_timeout.is_zero() {
            return Err(RunValueMaterializationError::Integrity);
        }
        Ok(Self {
            resolver,
            reader,
            request_timeout,
        })
    }
}

#[async_trait]
impl<A, R> ControllerRunValueMaterializer for SchedulerControllerRunValueMaterializer<A, R>
where
    A: SchedulerRunValueRequestResolver + 'static,
    R: SchedulerRunValueReader + 'static,
{
    async fn materialize(
        &self,
        context: &ControllerRunValueReadContext,
        input: &ResolvedExpressionInput,
    ) -> Result<ClosedJsonValue, RunValueMaterializationError> {
        if matches!(input.value, ValueRef::Inline { .. }) {
            return InlineControllerRunValueMaterializer
                .materialize(context, input)
                .await;
        }
        let ValueRef::Artifact { artifact } = &input.value else {
            return Err(RunValueMaterializationError::Integrity);
        };
        let maximum_bytes = usize::try_from(artifact.byte_length())
            .ok()
            .filter(|bytes| *bytes > 0 && *bytes <= MAX_SCHEDULER_RUN_VALUE_BYTES)
            .ok_or(RunValueMaterializationError::Integrity)?;
        let request_deadline = Utc::now()
            + ChronoDuration::from_std(self.request_timeout)
                .map_err(|_| RunValueMaterializationError::Integrity)?;
        let request_deadline = request_deadline.min(context.deadline);
        let request_digest: Sha256Digest = canonical_digest(&json!({
            "artifact": artifact,
            "job_id": context.orchestration_job_id,
            "lease_generation": context.lease_generation,
            "operation": "scheduler.run-value.materialize",
            "run_id": context.run_id,
            "run_value_id": input.run_value_id,
            "schema_digest": input.schema_digest,
        }))
        .map_err(|_| RunValueMaterializationError::Integrity)?
        .parse()
        .map_err(|_| RunValueMaterializationError::Integrity)?;
        let request = self
            .resolver
            .resolve_run_value_read(SchedulerRunValueLease {
                tenant_id: context.tenant_id.clone(),
                run_id: context.run_id.clone(),
                orchestration_job_id: context.orchestration_job_id.clone(),
                worker_process_generation_id: context.worker_process_generation_id.clone(),
                lease_generation: context.lease_generation,
                lease_token_digest: context.lease_token_digest.clone(),
                run_value_id: input.run_value_id.clone(),
                request_digest,
                maximum_bytes,
                deadline: request_deadline,
            })
            .await
            .map_err(map_run_value_authority_error)?;
        if request.run_value_id != input.run_value_id
            || request.schema_digest != input.schema_digest
            || request.classification != input.classification
            || request.artifact != *artifact
        {
            return Err(RunValueMaterializationError::Integrity);
        }
        let bytes = self
            .reader
            .read_exact(request)
            .await
            .map_err(map_run_value_read_error)?;
        let value = parse_strict_json(&bytes, MODEL_JSON_LIMITS)
            .map_err(|_| RunValueMaterializationError::Integrity)?;
        if canonical_json(&value).as_deref() != Ok(bytes.as_slice()) {
            return Err(RunValueMaterializationError::Integrity);
        }
        let materialized = ClosedJsonValue::build(input.schema_digest.clone(), value)
            .map_err(|_| RunValueMaterializationError::Integrity)?;
        if materialized.canonical_digest != input.content_digest {
            return Err(RunValueMaterializationError::Integrity);
        }
        Ok(materialized)
    }
}

fn map_run_value_authority_error(
    error: ArtifactObjectReadAuthorityError,
) -> RunValueMaterializationError {
    match error {
        ArtifactObjectReadAuthorityError::Unavailable => RunValueMaterializationError::Unavailable,
        ArtifactObjectReadAuthorityError::Denied | ArtifactObjectReadAuthorityError::NotFound => {
            RunValueMaterializationError::FenceLost
        }
        ArtifactObjectReadAuthorityError::InvalidEvidence => {
            RunValueMaterializationError::Integrity
        }
    }
}

fn map_run_value_read_error(error: SchedulerRunValueReadError) -> RunValueMaterializationError {
    match error {
        SchedulerRunValueReadError::Unavailable => RunValueMaterializationError::Unavailable,
        SchedulerRunValueReadError::Denied | SchedulerRunValueReadError::NotFound => {
            RunValueMaterializationError::FenceLost
        }
        SchedulerRunValueReadError::TooLarge | SchedulerRunValueReadError::Integrity => {
            RunValueMaterializationError::Integrity
        }
    }
}

pub fn child_descendant_budget_fits_hard_limit(requested_descendants: u32) -> bool {
    requested_descendants < MAX_DESCENDANT_RUNS
}
#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_artifacts::SchedulerRunValueReadRequest;
    use insight_platform_contracts::{ArtifactRef, DataClassification};
    use insight_platform_plan::ExactDataPortRef;
    use serde_json::json;
    use std::sync::Mutex;

    #[test]
    fn child_descendant_budget_reserves_capacity_for_the_child_run() {
        assert!(child_descendant_budget_fits_hard_limit(
            MAX_DESCENDANT_RUNS - 1
        ));
        assert!(!child_descendant_budget_fits_hard_limit(
            MAX_DESCENDANT_RUNS
        ));
    }

    fn id(value: &str) -> ResourceId {
        value.parse().unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    struct Resolver {
        artifact: ArtifactRef,
        schema_digest: Sha256Digest,
        observed: Mutex<Option<SchedulerRunValueLease>>,
    }

    #[async_trait]
    impl SchedulerRunValueRequestResolver for Resolver {
        async fn resolve_run_value_read(
            &self,
            lease: SchedulerRunValueLease,
        ) -> Result<SchedulerRunValueReadRequest, ArtifactObjectReadAuthorityError> {
            *self.observed.lock().unwrap() = Some(lease.clone());
            Ok(SchedulerRunValueReadRequest {
                tenant_id: lease.tenant_id,
                run_id: lease.run_id,
                orchestration_job_id: lease.orchestration_job_id,
                worker_process_generation_id: lease.worker_process_generation_id,
                lease_generation: lease.lease_generation,
                lease_token_digest: lease.lease_token_digest,
                run_value_id: lease.run_value_id,
                schema_digest: self.schema_digest.clone(),
                classification: DataClassification::Confidential,
                artifact: self.artifact.clone(),
                request_digest: lease.request_digest,
                maximum_bytes: lease.maximum_bytes,
                deadline: lease.deadline,
            })
        }
    }

    struct Reader(Vec<u8>);

    #[async_trait]
    impl SchedulerRunValueReader for Reader {
        async fn read_exact(
            &self,
            _request: SchedulerRunValueReadRequest,
        ) -> Result<Vec<u8>, SchedulerRunValueReadError> {
            Ok(self.0.clone())
        }
    }

    fn fixture(
        bytes: Vec<u8>,
    ) -> (
        SchedulerControllerRunValueMaterializer<Resolver, Reader>,
        ControllerRunValueReadContext,
        ResolvedExpressionInput,
    ) {
        let schema_digest = digest('a');
        let body = json!({"question": "artifact terminal"});
        let content_digest: Sha256Digest = canonical_digest(&body).unwrap().parse().unwrap();
        let artifact = ArtifactRef::new(
            id("art_0198f1c3-9a00-7c3e-b1f3-773c28367001"),
            content_digest.clone(),
            u64::try_from(canonical_json(&body).unwrap().len()).unwrap(),
            "application/json",
            DataClassification::Confidential,
            None,
        )
        .unwrap();
        let resolver = Arc::new(Resolver {
            artifact: artifact.clone(),
            schema_digest: schema_digest.clone(),
            observed: Mutex::new(None),
        });
        let materializer = SchedulerControllerRunValueMaterializer::new(
            resolver,
            Arc::new(Reader(bytes)),
            Duration::from_millis(100),
        )
        .unwrap();
        let context = ControllerRunValueReadContext {
            tenant_id: id("ten_0198f1c3-9a00-7c3e-b1f3-773c28367002"),
            run_id: id("run_0198f1c3-9a00-7c3e-b1f3-773c28367003"),
            orchestration_job_id: id("job_0198f1c3-9a00-7c3e-b1f3-773c28367004"),
            worker_process_generation_id: id("wrk_0198f1c3-9a00-7c3e-b1f3-773c28367005"),
            lease_generation: 2,
            lease_token_digest: digest('b'),
            deadline: Utc::now() + ChronoDuration::seconds(1),
        };
        let input = ResolvedExpressionInput {
            run_value_id: id("val_0198f1c3-9a00-7c3e-b1f3-773c28367006"),
            producing_node_id: None,
            value_kind: "run_input".to_owned(),
            port: ExactDataPortRef::RunInput {
                schema_digest: schema_digest.clone(),
            },
            classification: DataClassification::Confidential,
            schema_digest,
            content_digest,
            value: ValueRef::Artifact { artifact },
        };
        (materializer, context, input)
    }

    #[tokio::test]
    async fn artifact_run_value_is_exact_canonical_and_fenced() {
        let body = json!({"question": "artifact terminal"});
        let (materializer, context, input) = fixture(canonical_json(&body).unwrap());
        let value = materializer.materialize(&context, &input).await.unwrap();
        assert_eq!(value.value, body);
        let lease = materializer
            .resolver
            .observed
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(lease.run_value_id, input.run_value_id);
        assert_eq!(lease.run_id, context.run_id);
        assert_eq!(lease.lease_generation, context.lease_generation);
    }

    #[tokio::test]
    async fn noncanonical_artifact_json_fails_closed() {
        let (materializer, context, input) =
            fixture(br#"{ "question": "artifact terminal" }"#.to_vec());
        assert_eq!(
            materializer.materialize(&context, &input).await,
            Err(RunValueMaterializationError::Integrity)
        );
    }
}
