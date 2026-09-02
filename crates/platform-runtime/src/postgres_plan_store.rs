//! PostgreSQL-backed durable Plan-generation store.

use crate::{
    allocate_capability_invocation_mutations, allocate_child_run_mutations,
    allocate_context_query_mutations, allocate_controller_step_mutations,
    allocate_human_task_mutations, allocate_model_tool_capability_mutations,
    allocate_model_turn_mutations, allocate_orchestration_terminal_mutations,
    CoordinatorIdentityFactory, DerivedControllerCommit, DurableCapabilityDispatchFacts,
    DurableChildAgentDispatchFacts, DurableContextDispatchFacts, DurableControllerFacts,
    DurableControllerPhase, DurableModelDispatchFacts, DurablePlanDriverError,
    DurablePlanGenerationStore, GenerationHandoffReason, StartedOrchestrationJob,
};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_artifacts::{
    ArtifactObjectReadAuthorityError, SchedulerRunValueLease, SchedulerRunValueReadError,
    SchedulerRunValueReader, SchedulerRunValueRequestResolver, MAX_SCHEDULER_RUN_VALUE_BYTES,
};
use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, ClosedJsonValue, Failure, FailureClass,
    FailureCode, FailureSource, PlatformFailureCode, ResourceId, ResourceKind, Retryability,
    Sha256Digest, ValueRef, MODEL_JSON_LIMITS,
};
use insight_platform_orchestrator::{
    derive_candidate_selection, ChildBudget, CommittedExpressionInput, DurableWaitKind,
    ExactRunValueRef, HumanTaskDefinition, RunInputValue, RuntimeNode, RuntimePlan,
};
use insight_platform_postgres::repository::{
    ApplyDerivedExpressionControllerStep, ApplyOrchestrationControllerStep, CommitPlanTerminal,
    ContinueModelToolResultsToModelTurn, ControllerFacts, DeferOrchestrationToCapabilityInvocation,
    DeferOrchestrationToChildRun, DeferOrchestrationToContextQuery, DeferOrchestrationToModelTurn,
    DeferOrchestrationToTask, DerivedExpressionFailureEvidence, DispatchModelToolCapabilities,
    FailOrchestrationJob, JobFence, MaterializedTerminalValue, ModelToolCapabilityAdmission,
    OrchestrationFailureCause, OrchestrationYield, OrchestrationYieldMutationIds, PgRepository,
    RepositoryError, ResolvedExpressionInput, YieldOrchestrationJob, MAX_DESCENDANT_RUNS,
    MAX_ORCHESTRATION_QUOTA_LINES,
};
use insight_platform_postgres::sandbox_repository::SandboxCapabilitySubmission;
use insight_platform_postgres::sandbox_repository::SANDBOX_QUOTA_LINES;
use insight_platform_tasks::TaskDefinition;
use serde_json::json;
use sha2::{Digest as _, Sha256};
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
    fn from_started(
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

pub struct PostgresDurablePlanGenerationStore<M, I> {
    repository: PgRepository,
    materializer: Arc<M>,
    identities: Arc<I>,
    capability_admission: Arc<dyn ControllerCapabilityAdmissionProvider>,
    model_admission: Arc<dyn ControllerModelAdmissionProvider>,
    handoff_retry_delay: Duration,
}

#[derive(Debug, Clone)]
pub struct ControllerCapabilityAdmissionRequest {
    pub tenant_id: ResourceId,
    pub run_id: ResourceId,
    pub node_execution_id: ResourceId,
    pub slot_id: String,
    pub selected_deployment: insight_platform_contracts::ExactDeploymentRef,
    pub input_value_id: ResourceId,
    pub input_content_digest: Sha256Digest,
    pub selection_evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone)]
pub struct ControllerCapabilityAdmissionDecision {
    pub policies: insight_platform_invocations::InvocationPolicyDecisionBundle,
    pub mcp_runtime: Option<insight_platform_invocations::McpCapabilityRuntimeRequest>,
    pub sandbox: bool,
}

#[async_trait]
pub trait ControllerCapabilityAdmissionProvider: Send + Sync + 'static {
    async fn decide(
        &self,
        request: ControllerCapabilityAdmissionRequest,
    ) -> Result<ControllerCapabilityAdmissionDecision, DurablePlanDriverError>;
}

#[derive(Debug, Clone)]
pub struct ControllerModelAdmissionRequest {
    pub lease: ControllerRunValueReadContext,
    pub tenant_id: ResourceId,
    pub run_id: ResourceId,
    pub node_execution_id: ResourceId,
    pub model_turn_id: ResourceId,
    pub request_value_id: ResourceId,
    pub selected_deployment: insight_platform_contracts::ExactDeploymentRef,
    pub model_slot_id: String,
    pub plan_node_key: insight_platform_orchestrator::PlanNodeKey,
    pub plan_node: RuntimeNode,
    pub input: ResolvedExpressionInput,
    pub input_value: ClosedJsonValue,
    pub tool_slots: Vec<insight_platform_contracts::FrozenSlotBinding>,
    pub maximum_rounds: u16,
    pub maximum_capability_calls: u32,
    pub maximum_parallel_calls_per_round: u16,
    pub token_budget: u64,
    pub deadline: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ControllerModelAdmissionDecision {
    pub request: insight_platform_models::ModelRequestValue,
    pub requested_attempt_limit: u32,
    pub cost_ceiling_microunits: u64,
}

#[derive(Debug, Clone)]
pub struct ControllerModelContinuationRequest {
    pub model_turn_id: ResourceId,
    pub request_value_id: ResourceId,
    pub previous_request: insight_platform_models::ModelRequestValue,
    pub results: Vec<insight_platform_models::ModelToolResult>,
    pub deadline: chrono::DateTime<Utc>,
    pub requested_attempt_limit: u32,
    pub cost_ceiling_microunits: u64,
}

#[async_trait]
pub trait ControllerModelAdmissionProvider: Send + Sync + 'static {
    async fn assemble(
        &self,
        request: ControllerModelAdmissionRequest,
    ) -> Result<ControllerModelAdmissionDecision, DurablePlanDriverError>;

    async fn assemble_continuation(
        &self,
        request: ControllerModelContinuationRequest,
    ) -> Result<ControllerModelAdmissionDecision, DurablePlanDriverError> {
        assemble_default_model_continuation(request)
    }
}

fn assemble_default_model_continuation(
    request: ControllerModelContinuationRequest,
) -> Result<ControllerModelAdmissionDecision, DurablePlanDriverError> {
    if request.results.is_empty() || request.deadline <= Utc::now() {
        return Err(DurablePlanDriverError::InvariantViolation);
    }
    let mut canonical = request.previous_request.request.clone();
    canonical.model_turn_id = request.model_turn_id.clone();
    canonical.deadline = request.deadline;
    let classification = request
        .results
        .iter()
        .map(|result| result.classification)
        .chain(std::iter::once(canonical.classification))
        .max_by_key(|classification| classification.rank())
        .ok_or(DurablePlanDriverError::InvariantViolation)?;
    let result_evidence = serde_json::to_value(&request.results)
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
    let source_digest: Sha256Digest = canonical_digest(&result_evidence)
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
    let encoded_results = serde_json::to_vec(&request.results)
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
    let content_digest = digest_bytes(&encoded_results)?;
    let result_bytes = u32::try_from(encoded_results.len())
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
    let result_tokens = result_bytes
        .checked_add(3)
        .map(|bytes| bytes / 4)
        .ok_or(DurablePlanDriverError::InvariantViolation)?
        .max(1);
    let result_ordinal = u32::try_from(canonical.messages.len())
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
    canonical
        .messages
        .push(insight_platform_models::CanonicalMessage {
            role: insight_platform_models::CanonicalMessageRole::Tool,
            parts: request
                .results
                .iter()
                .cloned()
                .map(insight_platform_models::CanonicalMessagePart::ToolResult)
                .collect(),
            classification,
            source: insight_platform_models::ModelContentSource {
                source_kind: "capability_tool_result".to_owned(),
                source_id: request.model_turn_id.to_string(),
                source_digest: source_digest.clone(),
                content_digest,
                assembly_phase: insight_platform_models::PromptAssemblyPhase::CapabilityToolResult,
                ordinal: result_ordinal,
                byte_budget: result_bytes,
                token_budget: result_tokens,
                trusted_instruction: false,
            },
        });
    let source_map = insight_platform_models::derive_prompt_source_map(&canonical.messages)
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
    canonical.classification = source_map.classification;
    canonical.input_token_estimate = source_map.total_estimated_tokens;
    canonical.source_map_digest = source_map.canonical_digest;
    let value =
        serde_json::to_value(&canonical).map_err(|_| DurablePlanDriverError::InvariantViolation)?;
    let content_digest: Sha256Digest = canonical_digest(&value)
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
    Ok(ControllerModelAdmissionDecision {
        request: insight_platform_models::ModelRequestValue {
            value_id: request.request_value_id,
            classification,
            schema_digest: request.previous_request.schema_digest,
            content_digest,
            value: ValueRef::Inline { value },
            request: canonical,
        },
        requested_attempt_limit: request.requested_attempt_limit,
        cost_ceiling_microunits: request.cost_ceiling_microunits,
    })
}

fn digest_bytes(bytes: &[u8]) -> Result<Sha256Digest, DurablePlanDriverError> {
    let mut encoded = String::from("sha256:");
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}")
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
    }
    encoded
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)
}

impl<M, I> PostgresDurablePlanGenerationStore<M, I>
where
    M: ControllerRunValueMaterializer,
    I: CoordinatorIdentityFactory + 'static,
{
    pub fn new(
        repository: PgRepository,
        materializer: Arc<M>,
        identities: Arc<I>,
        capability_admission: Arc<dyn ControllerCapabilityAdmissionProvider>,
        model_admission: Arc<dyn ControllerModelAdmissionProvider>,
        handoff_retry_delay: Duration,
    ) -> Result<Self, DurablePlanDriverError> {
        if handoff_retry_delay.is_zero() || i64::try_from(handoff_retry_delay.as_millis()).is_err()
        {
            return Err(DurablePlanDriverError::InvariantViolation);
        }
        Ok(Self {
            repository,
            materializer,
            identities,
            capability_admission,
            model_admission,
            handoff_retry_delay,
        })
    }

    fn new_id(
        &self,
        kind: ResourceKind,
    ) -> Result<insight_platform_contracts::ResourceId, DurablePlanDriverError> {
        self.identities
            .new_resource_id(kind)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)
    }

    async fn commit_failure(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        materialized: crate::MaterializedTypedPlan,
        cause: OrchestrationFailureCause,
        operation: &'static str,
    ) -> Result<(), DurablePlanDriverError> {
        let requirements = self
            .repository
            .load_failure_mutation_requirements(&fence, &materialized.plan, &cause)
            .await
            .map_err(classify_repository_failure)?;
        let request_digest: Sha256Digest = canonical_digest(&json!({
            "cause": cause,
            "job_id": fence.job_id,
            "lease_generation": fence.lease_epoch,
            "operation": operation,
            "plan_digest": materialized.request.artifact.content_digest(),
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let wake_request_digest: Sha256Digest = canonical_digest(&json!({
            "job_id": fence.job_id,
            "operation": "orchestration.failure.wake",
            "request_digest": request_digest,
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let mutations = allocate_controller_step_mutations(
            self.identities.as_ref(),
            &requirements,
            wake_request_digest,
        )
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let command = FailOrchestrationJob {
            fence,
            plan: materialized.plan,
            cause,
            derived_expression: None,
            idempotency_key_digest: self
                .identities
                .new_lease_token_digest()
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
            request_digest,
            receipt_expires_at: job.started().deadline,
            mutations,
        };
        let mut transaction = self
            .repository
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        match transaction.fail_orchestration_job(command).await {
            Ok(_) => transaction
                .commit()
                .await
                .map_err(classify_repository_failure),
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                Err(classify_repository_failure(failure))
            }
        }
    }

    async fn commit_capability_dispatch(
        &self,
        job: &StartedOrchestrationJob,
        command: DerivedControllerCommit,
    ) -> Result<(), DurablePlanDriverError> {
        let DurableControllerPhase::CapabilityDispatch(facts) = command.facts.phase else {
            return Err(DurablePlanDriverError::InvariantViolation);
        };
        let DurableCapabilityDispatchFacts {
            input,
            route,
            selection_policy,
            selection_document,
            candidates,
        } = *facts;
        let capability_slot_id = match command
            .materialized
            .plan
            .node(&command.facts.plan_node_key)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        {
            RuntimeNode::CapabilityCall {
                capability_slot_id, ..
            } => capability_slot_id.clone(),
            _ => return Err(DurablePlanDriverError::InvariantViolation),
        };
        let route_reference = route.as_ref().map(|(resolved, _)| ExactRunValueRef {
            value_id: resolved.run_value_id.clone(),
            schema_digest: resolved.schema_digest.clone(),
            content_digest: resolved.content_digest.clone(),
        });
        let selection_evidence = derive_candidate_selection(
            &capability_slot_id,
            &selection_policy,
            &selection_document,
            &candidates,
            route_reference
                .as_ref()
                .zip(route.as_ref().map(|(_, value)| value)),
        )
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let tenant_id: ResourceId = job
            .started()
            .tenant_id
            .parse()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let run_id: ResourceId = job
            .started()
            .run_id
            .as_deref()
            .ok_or(DurablePlanDriverError::InvariantViolation)?
            .parse()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let admission = self
            .capability_admission
            .decide(ControllerCapabilityAdmissionRequest {
                tenant_id,
                run_id,
                node_execution_id: command.facts.node_execution_id.clone(),
                slot_id: capability_slot_id,
                selected_deployment: selection_evidence.selected_deployment.clone(),
                input_value_id: input.run_value_id.clone(),
                input_content_digest: input.content_digest.clone(),
                selection_evidence_digest: selection_evidence.canonical_digest.clone(),
            })
            .await?;
        let invocation_id = self.new_id(ResourceKind::CapabilityInvocation)?;
        let capability_job_id = self.new_id(ResourceKind::Job)?;
        let sandbox_submission = admission
            .sandbox
            .then(|| {
                Ok::<_, DurablePlanDriverError>(SandboxCapabilitySubmission {
                    output_value_id: ResourceId::from_uuid_v7(
                        ResourceKind::RunValue,
                        capability_job_id.uuid(),
                    )
                    .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
                    receipt_id: self.new_id(ResourceKind::Receipt)?,
                    event_id: self.new_id(ResourceKind::Event)?,
                    outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                    usage_reservation_id: self.new_id(ResourceKind::UsageReservation)?,
                    quota_entry_ids: (0..SANDBOX_QUOTA_LINES)
                        .map(|_| self.new_id(ResourceKind::QuotaLedgerEntry))
                        .collect::<Result<Vec<_>, _>>()?,
                })
            })
            .transpose()?;
        let request_digest: Sha256Digest = canonical_digest(&json!({
            "input_content_digest": input.content_digest,
            "invocation_id": invocation_id,
            "job_id": command.fence.job_id,
            "lease_generation": command.fence.lease_epoch,
            "operation": "orchestration.capability.defer",
            "plan_digest": command.materialized.request.artifact.content_digest(),
            "selection_evidence_digest": selection_evidence.canonical_digest,
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let capability = DeferOrchestrationToCapabilityInvocation {
            fence: command.fence,
            plan: command.materialized.plan,
            invocation_id,
            capability_job_id,
            input_artifact_link_id: matches!(input.value, ValueRef::Artifact { .. })
                .then(|| self.new_id(ResourceKind::ArtifactLink))
                .transpose()?,
            input,
            route,
            selection_evidence,
            approval_task_id: admission
                .policies
                .approval
                .as_ref()
                .map(|_| self.new_id(ResourceKind::ApprovalTask))
                .transpose()?,
            policy_decisions: admission.policies,
            mcp_runtime: admission.mcp_runtime,
            sandbox_submission,
            idempotency_key_digest: self
                .identities
                .new_lease_token_digest()
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
            request_digest,
            receipt_expires_at: job.started().deadline,
            mutations: allocate_capability_invocation_mutations(self.identities.as_ref())
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
        };
        let mut transaction = self
            .repository
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        match transaction
            .defer_orchestration_to_capability_invocation(capability)
            .await
        {
            Ok(_) => transaction
                .commit()
                .await
                .map_err(classify_repository_failure),
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                Err(classify_repository_failure(failure))
            }
        }
    }

    async fn commit_context_dispatch(
        &self,
        job: &StartedOrchestrationJob,
        command: DerivedControllerCommit,
    ) -> Result<(), DurablePlanDriverError> {
        let DurableControllerPhase::ContextDispatch(facts) = command.facts.phase else {
            return Err(DurablePlanDriverError::InvariantViolation);
        };
        let DurableContextDispatchFacts { input, value } = *facts;
        let RuntimeNode::ContextQuery { .. } = command
            .materialized
            .plan
            .node(&command.facts.plan_node_key)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        else {
            return Err(DurablePlanDriverError::InvariantViolation);
        };
        let request_digest: Sha256Digest = canonical_digest(&json!({
            "input_content_digest": input.content_digest,
            "job_id": command.fence.job_id,
            "lease_generation": command.fence.lease_epoch,
            "operation": "orchestration.context.defer",
            "plan_digest": command.materialized.request.artifact.content_digest(),
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let input_artifact_link_id = matches!(input.value, ValueRef::Artifact { .. })
            .then(|| self.new_id(ResourceKind::ArtifactLink))
            .transpose()?;
        let context = DeferOrchestrationToContextQuery {
            fence: command.fence,
            plan: command.materialized.plan,
            context_query_id: self.new_id(ResourceKind::ContextQuery)?,
            context_job_id: self.new_id(ResourceKind::Job)?,
            input,
            materialized_input: value,
            input_artifact_link_id,
            idempotency_key_digest: self
                .identities
                .new_lease_token_digest()
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
            request_digest,
            receipt_expires_at: job.started().deadline,
            mutations: allocate_context_query_mutations(self.identities.as_ref())
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
        };
        let mut transaction = self
            .repository
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        match transaction
            .defer_orchestration_to_context_query(context)
            .await
        {
            Ok(_) => transaction
                .commit()
                .await
                .map_err(classify_repository_failure),
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                Err(classify_repository_failure(failure))
            }
        }
    }

    async fn commit_model_dispatch(
        &self,
        job: &StartedOrchestrationJob,
        command: DerivedControllerCommit,
    ) -> Result<(), DurablePlanDriverError> {
        let DurableControllerPhase::ModelDispatch(facts) = command.facts.phase else {
            return Err(DurablePlanDriverError::InvariantViolation);
        };
        let DurableModelDispatchFacts {
            input,
            value,
            route,
            selection_policy,
            selection_document,
            candidates,
            tool_slots,
        } = *facts;
        let RuntimeNode::ModelLoop {
            model_slot_id,
            maximum_rounds,
            maximum_capability_calls,
            maximum_parallel_calls_per_round,
            token_budget,
            ..
        } = command
            .materialized
            .plan
            .node(&command.facts.plan_node_key)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        else {
            return Err(DurablePlanDriverError::InvariantViolation);
        };
        let route_reference = route.as_ref().map(|(resolved, _)| ExactRunValueRef {
            value_id: resolved.run_value_id.clone(),
            schema_digest: resolved.schema_digest.clone(),
            content_digest: resolved.content_digest.clone(),
        });
        let selection_evidence = derive_candidate_selection(
            model_slot_id,
            &selection_policy,
            &selection_document,
            &candidates,
            route_reference
                .as_ref()
                .zip(route.as_ref().map(|(_, value)| value)),
        )
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let tenant_id: ResourceId = job
            .started()
            .tenant_id
            .parse()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let run_id: ResourceId = job
            .started()
            .run_id
            .as_deref()
            .ok_or(DurablePlanDriverError::InvariantViolation)?
            .parse()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let model_turn_id = self.new_id(ResourceKind::ModelTurn)?;
        let request_value_id = self.new_id(ResourceKind::RunValue)?;
        let admission = self
            .model_admission
            .assemble(ControllerModelAdmissionRequest {
                lease: ControllerRunValueReadContext::from_started(job, &command.fence)?,
                tenant_id,
                run_id,
                node_execution_id: command.facts.node_execution_id.clone(),
                model_turn_id: model_turn_id.clone(),
                request_value_id: request_value_id.clone(),
                selected_deployment: selection_evidence.selected_deployment.clone(),
                model_slot_id: model_slot_id.clone(),
                plan_node_key: command.facts.plan_node_key.clone(),
                plan_node: command
                    .materialized
                    .plan
                    .node(&command.facts.plan_node_key)
                    .map_err(|_| DurablePlanDriverError::InvariantViolation)?
                    .clone(),
                input: input.clone(),
                input_value: value,
                tool_slots: tool_slots.clone(),
                maximum_rounds: *maximum_rounds,
                maximum_capability_calls: *maximum_capability_calls,
                maximum_parallel_calls_per_round: *maximum_parallel_calls_per_round,
                token_budget: *token_budget,
                deadline: job.started().deadline,
            })
            .await?;
        if admission.request.value_id != request_value_id {
            return Err(DurablePlanDriverError::InvariantViolation);
        }
        let model_job_id = self.new_id(ResourceKind::Job)?;
        let request_digest: Sha256Digest = canonical_digest(&json!({
            "job_id": command.fence.job_id,
            "lease_generation": command.fence.lease_epoch,
            "model_job_id": model_job_id,
            "model_turn_id": model_turn_id,
            "operation": "orchestration.model.defer",
            "plan_digest": command.materialized.request.artifact.content_digest(),
            "request_value_digest": admission.request.content_digest,
            "selection_evidence_digest": selection_evidence.canonical_digest,
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let deferred = DeferOrchestrationToModelTurn {
            fence: command.fence,
            plan: command.materialized.plan,
            model_turn_id,
            model_job_id,
            input,
            request: admission.request,
            route,
            selection_evidence,
            tool_slots,
            requested_attempt_limit: admission.requested_attempt_limit,
            cost_ceiling_microunits: admission.cost_ceiling_microunits,
            idempotency_key_digest: self
                .identities
                .new_lease_token_digest()
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
            request_digest,
            receipt_expires_at: job.started().deadline,
            mutations: allocate_model_turn_mutations(self.identities.as_ref())
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
        };
        let mut transaction = self
            .repository
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        match transaction
            .defer_orchestration_to_model_turn(deferred)
            .await
        {
            Ok(_) => transaction
                .commit()
                .await
                .map_err(classify_repository_failure),
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                Err(classify_repository_failure(failure))
            }
        }
    }

    async fn commit_child_agent_dispatch(
        &self,
        job: &StartedOrchestrationJob,
        command: DerivedControllerCommit,
    ) -> Result<(), DurablePlanDriverError> {
        let DurableControllerPhase::ChildAgentDispatch(facts) = command.facts.phase else {
            return Err(DurablePlanDriverError::InvariantViolation);
        };
        let DurableChildAgentDispatchFacts {
            input,
            route,
            selection_policy,
            selection_document,
            candidates,
        } = *facts;
        let (
            child_agent_slot_id,
            budget,
            cancellation_policy,
            attempt_limit,
            retry_backoff_milliseconds,
        ) = match command
            .materialized
            .plan
            .node(&command.facts.plan_node_key)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        {
            RuntimeNode::ChildAgentCall {
                child_agent_slot_id,
                budget,
                cancellation_policy,
                attempt_limit,
                retry_backoff_milliseconds,
                ..
            } => (
                child_agent_slot_id.clone(),
                budget.clone(),
                *cancellation_policy,
                *attempt_limit,
                *retry_backoff_milliseconds,
            ),
            _ => return Err(DurablePlanDriverError::InvariantViolation),
        };
        let route_reference = route.as_ref().map(|(resolved, _)| ExactRunValueRef {
            value_id: resolved.run_value_id.clone(),
            schema_digest: resolved.schema_digest.clone(),
            content_digest: resolved.content_digest.clone(),
        });
        let route_for_selection = route_reference
            .as_ref()
            .zip(route.as_ref().map(|(_, value)| value));
        let selection_evidence = derive_candidate_selection(
            &child_agent_slot_id,
            &selection_policy,
            &selection_document,
            &candidates,
            route_for_selection,
        )
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let maximum_duration = budget.maximum_duration_milliseconds;
        let delegated_duration = maximum_duration.saturating_div(2).max(1);
        let delegated_duration = i64::try_from(delegated_duration)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let deadline = (Utc::now() + ChronoDuration::milliseconds(delegated_duration))
            .min(job.started().deadline);
        let child_budget = ChildBudget {
            deadline,
            maximum_model_tokens: budget.maximum_model_tokens,
            maximum_capability_calls: budget.maximum_capability_calls,
            maximum_artifact_bytes: budget.maximum_artifact_bytes,
            maximum_descendant_runs: budget.maximum_descendant_runs,
        };
        if !child_descendant_budget_fits_hard_limit(child_budget.maximum_descendant_runs) {
            return self
                .commit_failure(
                    job,
                    command.fence,
                    command.materialized,
                    OrchestrationFailureCause::Admission {
                        failure: Failure {
                            code: FailureCode::Platform {
                                code: PlatformFailureCode::BudgetExhausted,
                            },
                            class: FailureClass::Quota,
                            retryability: Retryability::Never,
                            safe_message: Some(
                                "child agent descendant budget exceeds the remaining hard limit"
                                    .to_owned(),
                            ),
                            details_ref: None,
                            source: FailureSource::Plan,
                        },
                    },
                    "orchestration.child.admission.fail",
                )
                .await;
        }
        let child_input_value_id = self.new_id(ResourceKind::RunValue)?;
        let request_digest: Sha256Digest = canonical_digest(&json!({
            "input_content_digest": input.content_digest,
            "job_id": command.fence.job_id,
            "lease_generation": command.fence.lease_epoch,
            "operation": "orchestration.child.defer",
            "plan_digest": command.materialized.request.artifact.content_digest(),
            "selection_evidence_digest": selection_evidence.canonical_digest,
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let child = DeferOrchestrationToChildRun {
            fence: command.fence,
            plan: command.materialized.plan,
            slot_id: child_agent_slot_id,
            selected_child_deployment: selection_evidence.selected_deployment.clone(),
            selection_evidence,
            materialized_route: route.map(|(_, value)| value),
            child_link_id: self.new_id(ResourceKind::ChildRunLink)?,
            child_run_id: self.new_id(ResourceKind::Run)?,
            child_root_scope_id: self.new_id(ResourceKind::ScopeInstance)?,
            child_entry_node_execution_id: self.new_id(ResourceKind::NodeExecution)?,
            child_orchestration_job_id: self.new_id(ResourceKind::Job)?,
            input: RunInputValue {
                value_id: child_input_value_id,
                classification: input.classification,
                schema_digest: input.schema_digest,
                content_digest: input.content_digest,
                value: input.value,
            },
            source_value_ids: vec![input.run_value_id],
            budget: child_budget,
            cancellation_policy,
            logical_key: format!(
                "{}:{}",
                command.facts.node_execution_id,
                job.started().attempt_no
            ),
            child_attempt_limit: attempt_limit,
            child_retry_backoff_milliseconds: retry_backoff_milliseconds,
            idempotency_key_digest: self
                .identities
                .new_lease_token_digest()
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
            request_digest,
            receipt_expires_at: job.started().deadline,
            mutations: allocate_child_run_mutations(self.identities.as_ref())
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
        };
        let mut transaction = self
            .repository
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        match transaction.defer_orchestration_to_child_run(child).await {
            Ok(_) => transaction
                .commit()
                .await
                .map_err(classify_repository_failure),
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                Err(classify_repository_failure(failure))
            }
        }
    }

    async fn commit_human_task_wait(
        &self,
        job: &StartedOrchestrationJob,
        command: DerivedControllerCommit,
    ) -> Result<(), DurablePlanDriverError> {
        if !matches!(
            command.facts.phase,
            DurableControllerPhase::CommittedFacts {
                observation: insight_platform_orchestrator::ControllerObservation::None
            }
        ) {
            return Err(DurablePlanDriverError::InvariantViolation);
        }
        let (definition, response_schema_digest, timeout_milliseconds) = match command
            .materialized
            .plan
            .node(&command.facts.plan_node_key)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        {
            RuntimeNode::HumanTask {
                definition,
                response,
                timeout_milliseconds,
                ..
            } => (
                match definition {
                    HumanTaskDefinition::Interaction {
                        interaction_kind,
                        eligible_principal_rule_digest,
                        safe_prompt_key,
                    } => TaskDefinition::Interaction {
                        interaction_kind: *interaction_kind,
                        eligible_principal_rule_digest: eligible_principal_rule_digest.clone(),
                        safe_prompt_key: safe_prompt_key.clone(),
                    },
                    HumanTaskDefinition::HumanWork {
                        eligible_principal_rule_digest,
                        safe_prompt_key,
                    } => TaskDefinition::HumanWork {
                        eligible_principal_rule_digest: eligible_principal_rule_digest.clone(),
                        safe_prompt_key: safe_prompt_key.clone(),
                    },
                },
                response.schema_digest().clone(),
                *timeout_milliseconds,
            ),
            _ => return Err(DurablePlanDriverError::InvariantViolation),
        };
        let delegated_timeout = timeout_milliseconds.saturating_div(2).max(1);
        let delegated_timeout = i64::try_from(delegated_timeout)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let task_deadline = (Utc::now() + ChronoDuration::milliseconds(delegated_timeout))
            .min(job.started().deadline);
        let request_digest: Sha256Digest = canonical_digest(&json!({
            "job_id": command.fence.job_id,
            "lease_generation": command.fence.lease_epoch,
            "operation": "orchestration.task.defer",
            "plan_digest": command.materialized.request.artifact.content_digest(),
            "task_kind": definition.task_kind().as_str(),
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let task = DeferOrchestrationToTask {
            fence: command.fence,
            plan: command.materialized.plan,
            task_id: self.new_id(definition.task_kind().task_id_kind())?,
            definition,
            response_schema_digest: Some(response_schema_digest),
            task_deadline,
            idempotency_key_digest: self
                .identities
                .new_lease_token_digest()
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
            request_digest,
            receipt_expires_at: job.started().deadline,
            mutations: allocate_human_task_mutations(self.identities.as_ref())
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
        };
        let mut transaction = self
            .repository
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        match transaction.defer_orchestration_to_task(task).await {
            Ok(_) => transaction
                .commit()
                .await
                .map_err(classify_repository_failure),
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                Err(classify_repository_failure(failure))
            }
        }
    }

    async fn commit_plan_wait(
        &self,
        job: &StartedOrchestrationJob,
        command: DerivedControllerCommit,
        wait_kind: DurableWaitKind,
    ) -> Result<(), DurablePlanDriverError> {
        if !matches!(
            command.facts.phase,
            DurableControllerPhase::CommittedFacts {
                observation: insight_platform_orchestrator::ControllerObservation::None
            }
        ) {
            return Err(DurablePlanDriverError::InvariantViolation);
        }
        let exact_node = command
            .materialized
            .plan
            .node(&command.facts.plan_node_key)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        if !matches!(
            (wait_kind, exact_node),
            (DurableWaitKind::Timer, RuntimeNode::TimerWait { .. })
                | (DurableWaitKind::Signal, RuntimeNode::SignalWait { .. })
        ) {
            return Err(DurablePlanDriverError::InvariantViolation);
        }
        let request_digest: Sha256Digest = canonical_digest(&json!({
            "job_id": command.fence.job_id,
            "lease_generation": command.fence.lease_epoch,
            "operation": match wait_kind {
                DurableWaitKind::Timer => "orchestration.timer.defer",
                DurableWaitKind::Signal => "orchestration.signal.defer",
                DurableWaitKind::HumanTask => return Err(DurablePlanDriverError::InvariantViolation),
            },
            "plan_digest": command.materialized.request.artifact.content_digest(),
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let mutations = OrchestrationYieldMutationIds {
            receipt_id: self.new_id(ResourceKind::Receipt)?,
            quota_entry_ids: (0..MAX_ORCHESTRATION_QUOTA_LINES)
                .map(|_| self.new_id(ResourceKind::QuotaLedgerEntry))
                .collect::<Result<Vec<_>, _>>()?,
            run_event_id: self.new_id(ResourceKind::Event)?,
            run_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            node_event_id: self.new_id(ResourceKind::Event)?,
            node_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            job_event_id: self.new_id(ResourceKind::Event)?,
            job_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
        };
        let wait = YieldOrchestrationJob {
            fence: command.fence,
            outcome: match wait_kind {
                DurableWaitKind::Timer => OrchestrationYield::TimerWait {
                    plan: command.materialized.plan,
                },
                DurableWaitKind::Signal => OrchestrationYield::SignalWait {
                    plan: command.materialized.plan,
                },
                DurableWaitKind::HumanTask => {
                    return Err(DurablePlanDriverError::InvariantViolation)
                }
            },
            idempotency_key_digest: self
                .identities
                .new_lease_token_digest()
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
            request_digest,
            receipt_expires_at: job.started().deadline,
            mutations,
        };
        let mut transaction = self
            .repository
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        match transaction.yield_orchestration_job(wait).await {
            Ok(_) => transaction
                .commit()
                .await
                .map_err(classify_repository_failure),
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

fn child_descendant_budget_fits_hard_limit(requested_descendants: u32) -> bool {
    requested_descendants < MAX_DESCENDANT_RUNS
}

#[async_trait]
impl<M, I> DurablePlanGenerationStore for PostgresDurablePlanGenerationStore<M, I>
where
    M: ControllerRunValueMaterializer,
    I: CoordinatorIdentityFactory + 'static,
{
    async fn load_controller_facts(
        &self,
        job: &StartedOrchestrationJob,
        fence: &JobFence,
        plan: &RuntimePlan,
    ) -> Result<DurableControllerFacts, DurablePlanDriverError> {
        let context = ControllerRunValueReadContext::from_started(job, fence)?;
        match self
            .repository
            .load_controller_facts(fence, plan)
            .await
            .map_err(classify_repository_failure)?
        {
            ControllerFacts::Expression(facts) => {
                let mut inputs = Vec::with_capacity(facts.inputs.len());
                for input in facts.inputs {
                    let value = self
                        .materializer
                        .materialize(&context, &input)
                        .await
                        .map_err(|failure| match failure {
                            RunValueMaterializationError::Unavailable => {
                                DurablePlanDriverError::Unavailable
                            }
                            RunValueMaterializationError::FenceLost => {
                                DurablePlanDriverError::FenceLost
                            }
                            RunValueMaterializationError::Integrity => {
                                DurablePlanDriverError::InvariantViolation
                            }
                        })?;
                    inputs.push(CommittedExpressionInput {
                        run_value_id: input.run_value_id,
                        port: input.port,
                        classification: input.classification,
                        value,
                    });
                }
                Ok(DurableControllerFacts {
                    node_execution_id: facts.node_execution_id,
                    node_execution_version: facts.node_execution_version,
                    plan_node_key: facts.plan_node_key,
                    phase: DurableControllerPhase::Expression {
                        inputs,
                        loop_iteration: facts.loop_iteration,
                    },
                })
            }
            ControllerFacts::Committed {
                identity,
                observation,
            } => Ok(DurableControllerFacts {
                node_execution_id: identity.node_execution_id,
                node_execution_version: identity.node_execution_version,
                plan_node_key: identity.plan_node_key,
                phase: DurableControllerPhase::CommittedFacts { observation },
            }),
            ControllerFacts::ChildAgentDispatch(facts) => {
                let route = if let Some(route) = facts.route {
                    let value = self
                        .materializer
                        .materialize(&context, &route)
                        .await
                        .map_err(map_materialization_failure)?;
                    Some((route, value))
                } else {
                    None
                };
                Ok(DurableControllerFacts {
                    node_execution_id: facts.identity.node_execution_id,
                    node_execution_version: facts.identity.node_execution_version,
                    plan_node_key: facts.identity.plan_node_key,
                    phase: DurableControllerPhase::ChildAgentDispatch(Box::new(
                        DurableChildAgentDispatchFacts {
                            input: facts.input,
                            route,
                            selection_policy: facts.selection_policy,
                            selection_document: facts.selection_document,
                            candidates: facts.candidates,
                        },
                    )),
                })
            }
            ControllerFacts::CapabilityDispatch(facts) => {
                let route = if let Some(route) = facts.route {
                    let value = self
                        .materializer
                        .materialize(&context, &route)
                        .await
                        .map_err(map_materialization_failure)?;
                    Some((route, value))
                } else {
                    None
                };
                Ok(DurableControllerFacts {
                    node_execution_id: facts.identity.node_execution_id,
                    node_execution_version: facts.identity.node_execution_version,
                    plan_node_key: facts.identity.plan_node_key,
                    phase: DurableControllerPhase::CapabilityDispatch(Box::new(
                        DurableCapabilityDispatchFacts {
                            input: facts.input,
                            route,
                            selection_policy: facts.selection_policy,
                            selection_document: facts.selection_document,
                            candidates: facts.candidates,
                        },
                    )),
                })
            }
            ControllerFacts::ContextDispatch(facts) => {
                let value = self
                    .materializer
                    .materialize(&context, &facts.input)
                    .await
                    .map_err(map_materialization_failure)?;
                Ok(DurableControllerFacts {
                    node_execution_id: facts.identity.node_execution_id,
                    node_execution_version: facts.identity.node_execution_version,
                    plan_node_key: facts.identity.plan_node_key,
                    phase: DurableControllerPhase::ContextDispatch(Box::new(
                        DurableContextDispatchFacts {
                            input: facts.input,
                            value,
                        },
                    )),
                })
            }
            ControllerFacts::ModelDispatch(facts) => {
                let value = self
                    .materializer
                    .materialize(&context, &facts.input)
                    .await
                    .map_err(map_materialization_failure)?;
                let route = if let Some(route) = facts.route {
                    let value = self
                        .materializer
                        .materialize(&context, &route)
                        .await
                        .map_err(map_materialization_failure)?;
                    Some((route, value))
                } else {
                    None
                };
                Ok(DurableControllerFacts {
                    node_execution_id: facts.identity.node_execution_id,
                    node_execution_version: facts.identity.node_execution_version,
                    plan_node_key: facts.identity.plan_node_key,
                    phase: DurableControllerPhase::ModelDispatch(Box::new(
                        DurableModelDispatchFacts {
                            input: facts.input,
                            value,
                            route,
                            selection_policy: facts.selection_policy,
                            selection_document: facts.selection_document,
                            candidates: facts.candidates,
                            tool_slots: facts.tool_slots,
                        },
                    )),
                })
            }
        }
    }

    async fn commit_controller(
        &self,
        job: &StartedOrchestrationJob,
        command: DerivedControllerCommit,
    ) -> Result<(), DurablePlanDriverError> {
        let derived = match (command.facts.phase.clone(), command.evaluation.clone()) {
            (DurableControllerPhase::Expression { inputs, .. }, Some(evaluation)) => {
                Some((inputs, evaluation))
            }
            (DurableControllerPhase::CommittedFacts { .. }, None) => None,
            (DurableControllerPhase::ChildAgentDispatch(_), None) => None,
            (DurableControllerPhase::CapabilityDispatch(_), None) => None,
            (DurableControllerPhase::ContextDispatch(_), None) => None,
            (DurableControllerPhase::ModelDispatch(_), None) => None,
            _ => return Err(DurablePlanDriverError::InvariantViolation),
        };
        if matches!(
            command.decision,
            insight_platform_orchestrator::ControllerDecision::CreateChildRun { .. }
        ) {
            return self.commit_child_agent_dispatch(job, command).await;
        }
        if matches!(
            command.decision,
            insight_platform_orchestrator::ControllerDecision::DispatchLeaf {
                kind: insight_platform_orchestrator::LeafKind::Capability,
                ..
            }
        ) {
            return self.commit_capability_dispatch(job, command).await;
        }
        if matches!(
            command.decision,
            insight_platform_orchestrator::ControllerDecision::DispatchLeaf {
                kind: insight_platform_orchestrator::LeafKind::Context,
                ..
            }
        ) {
            return self.commit_context_dispatch(job, command).await;
        }
        if matches!(
            command.decision,
            insight_platform_orchestrator::ControllerDecision::DispatchLeaf {
                kind: insight_platform_orchestrator::LeafKind::ModelLoop,
                ..
            }
        ) {
            return self.commit_model_dispatch(job, command).await;
        }
        if matches!(
            command.decision,
            insight_platform_orchestrator::ControllerDecision::CreateDurableWait {
                kind: DurableWaitKind::HumanTask,
                ..
            }
        ) {
            return self.commit_human_task_wait(job, command).await;
        }
        if matches!(
            command.decision,
            insight_platform_orchestrator::ControllerDecision::CreateDurableWait {
                kind: DurableWaitKind::Timer,
                ..
            }
        ) {
            return self
                .commit_plan_wait(job, command, DurableWaitKind::Timer)
                .await;
        }
        if matches!(
            command.decision,
            insight_platform_orchestrator::ControllerDecision::CreateDurableWait {
                kind: DurableWaitKind::Signal,
                ..
            }
        ) {
            return self
                .commit_plan_wait(job, command, DurableWaitKind::Signal)
                .await;
        }
        if matches!(
            &command.decision,
            insight_platform_orchestrator::ControllerDecision::CompleteRun { .. }
                | insight_platform_orchestrator::ControllerDecision::FailRun { .. }
        ) {
            if derived.is_some() {
                return Err(DurablePlanDriverError::InvariantViolation);
            }
            let resolved = self
                .repository
                .load_plan_terminal_value(&command.fence, &command.materialized.plan)
                .await
                .map_err(|failure| {
                    eprintln!("Plan terminal value resolution rejected: {failure:?}");
                    classify_repository_failure(failure)
                })?;
            let context = ControllerRunValueReadContext::from_started(job, &command.fence)?;
            let materialized = self
                .materializer
                .materialize(&context, &resolved)
                .await
                .map_err(|failure| match failure {
                    RunValueMaterializationError::Unavailable => {
                        DurablePlanDriverError::Unavailable
                    }
                    RunValueMaterializationError::FenceLost => DurablePlanDriverError::FenceLost,
                    RunValueMaterializationError::Integrity => {
                        DurablePlanDriverError::InvariantViolation
                    }
                })?;
            let request_digest = canonical_digest(&json!({
                "content_digest": resolved.content_digest,
                "job_id": command.fence.job_id,
                "lease_generation": command.fence.lease_epoch,
                "operation": "orchestration.plan_terminal.commit",
                "plan_digest": command.materialized.request.artifact.content_digest(),
                "schema_digest": resolved.schema_digest,
                "value_id": resolved.run_value_id,
            }))
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?
            .parse()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
            let terminal = CommitPlanTerminal {
                fence: command.fence,
                plan: command.materialized.plan,
                value: MaterializedTerminalValue {
                    value_id: resolved.run_value_id,
                    classification: resolved.classification,
                    schema_digest: resolved.schema_digest,
                    content_digest: resolved.content_digest,
                    body: materialized.value,
                },
                idempotency_key_digest: self
                    .identities
                    .new_lease_token_digest()
                    .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
                request_digest,
                receipt_expires_at: job.started().deadline,
                mutations: allocate_orchestration_terminal_mutations(self.identities.as_ref())
                    .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
            };
            for retry in 0..4 {
                let mut transaction = self
                    .repository
                    .begin_scheduler_transaction()
                    .await
                    .map_err(classify_repository_failure)?;
                match transaction.commit_plan_terminal(terminal.clone()).await {
                    Ok(_) => match transaction.commit().await {
                        Ok(()) => return Ok(()),
                        Err(failure)
                            if retry < 3 && is_retryable_postgres_transaction_abort(&failure) =>
                        {
                            tokio::task::yield_now().await;
                        }
                        Err(failure) => {
                            eprintln!("Plan terminal transaction commit rejected: {failure:?}");
                            return Err(classify_repository_failure(failure));
                        }
                    },
                    Err(failure) => {
                        let retryable =
                            retry < 3 && is_retryable_postgres_transaction_abort(&failure);
                        transaction
                            .rollback()
                            .await
                            .map_err(classify_repository_failure)?;
                        if retryable {
                            tokio::task::yield_now().await;
                            continue;
                        }
                        eprintln!("Plan terminal commit rejected: {failure:?}");
                        return Err(classify_repository_failure(failure));
                    }
                }
            }
            unreachable!("bounded terminal transaction retry must return on its fourth attempt");
        }
        let controller_failure = matches!(
            command.decision,
            insight_platform_orchestrator::ControllerDecision::FailNode { .. }
        );
        let requirements = if controller_failure {
            self.repository
                .load_controller_failure_mutation_requirements(
                    &command.fence,
                    &command.materialized.plan,
                    &command.observation,
                )
                .await
        } else {
            self.repository
                .load_controller_mutation_requirements(
                    &command.fence,
                    &command.materialized.plan,
                    &command.observation,
                )
                .await
        }
        .map_err(classify_repository_failure)?;
        let request_digest: insight_platform_contracts::Sha256Digest = canonical_digest(&json!({
            "evidence_digest": derived.as_ref().map(|(_, evaluation)| &evaluation.evidence.canonical_digest),
            "job_id": command.fence.job_id,
            "lease_generation": command.fence.lease_epoch,
            "operation": if controller_failure { "orchestration.controller.fail" } else { "orchestration.controller.commit" },
            "plan_digest": command.materialized.request.artifact.content_digest(),
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let wake_request_digest = canonical_digest(&json!({
            "job_id": command.fence.job_id,
            "operation": "orchestration.controller.wake",
            "request_digest": request_digest,
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let mutations = allocate_controller_step_mutations(
            self.identities.as_ref(),
            &requirements,
            wake_request_digest,
        )
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let node = command
            .materialized
            .plan
            .node(&command.facts.plan_node_key)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let output_value_count = match node {
            RuntimeNode::Compute { .. } => derived
                .as_ref()
                .map(|(_, evaluation)| evaluation.outputs.len())
                .unwrap_or(0),
            RuntimeNode::Map { .. } => mutations.activations.len(),
            _ => 0,
        };
        let output_value_ids = (0..output_value_count)
            .map(|_| self.new_id(ResourceKind::RunValue))
            .collect::<Result<Vec<_>, _>>()?;
        let idempotency_key_digest = self
            .identities
            .new_lease_token_digest()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let step = ApplyOrchestrationControllerStep {
            fence: command.fence.clone(),
            plan: command.materialized.plan.clone(),
            observation: command.observation.clone(),
            idempotency_key_digest: idempotency_key_digest.clone(),
            request_digest: request_digest.clone(),
            receipt_expires_at: job.started().deadline,
            mutations: mutations.clone(),
        };
        let mut transaction = self
            .repository
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        if controller_failure {
            let derived_expression =
                derived.map(
                    |(materialized_inputs, evaluation)| DerivedExpressionFailureEvidence {
                        materialized_inputs,
                        evaluation,
                    },
                );
            let failure = transaction
                .fail_orchestration_job(FailOrchestrationJob {
                    fence: command.fence,
                    plan: command.materialized.plan,
                    cause: OrchestrationFailureCause::Controller {
                        observation: command.observation,
                    },
                    derived_expression,
                    idempotency_key_digest,
                    request_digest,
                    receipt_expires_at: job.started().deadline,
                    mutations,
                })
                .await;
            return match failure {
                Ok(_) => transaction
                    .commit()
                    .await
                    .map_err(classify_repository_failure),
                Err(failure) => {
                    transaction
                        .rollback()
                        .await
                        .map_err(classify_repository_failure)?;
                    Err(classify_repository_failure(failure))
                }
            };
        }
        let applied = if let Some((inputs, evaluation)) = derived {
            transaction
                .apply_derived_expression_controller_step(ApplyDerivedExpressionControllerStep {
                    step,
                    materialized_inputs: inputs,
                    evaluation,
                    output_value_ids,
                })
                .await
        } else {
            transaction.apply_orchestration_controller_step(step).await
        };
        match applied {
            Ok(_) => transaction
                .commit()
                .await
                .map_err(classify_repository_failure),
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                Err(classify_repository_failure(failure))
            }
        }
    }

    async fn commit_convergence_failure(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        materialized: crate::MaterializedTypedPlan,
        failure: Failure,
    ) -> Result<(), DurablePlanDriverError> {
        self.commit_failure(
            job,
            fence,
            materialized,
            OrchestrationFailureCause::Committed { failure },
            "orchestration.external_leaf_failure.converge",
        )
        .await
    }

    async fn commit_model_tool_continuation(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        materialized: crate::MaterializedTypedPlan,
        continuation: insight_platform_orchestrator::ModelToolContinuation,
    ) -> Result<(), DurablePlanDriverError> {
        if !continuation.results.is_empty() {
            let facts = self
                .repository
                .load_model_tool_result_continuation_facts(
                    &fence,
                    &materialized.plan,
                    &continuation,
                )
                .await
                .map_err(|failure| {
                    eprintln!("Model tool continuation facts rejected: {failure:?}");
                    classify_repository_failure(failure)
                })?;
            let model_turn_id = self.new_id(ResourceKind::ModelTurn)?;
            let request_value_id = self.new_id(ResourceKind::RunValue)?;
            let requested_attempt_limit = facts.requested_attempt_limit;
            let cost_ceiling_microunits = facts.cost_ceiling_microunits;
            let admission = self
                .model_admission
                .assemble_continuation(ControllerModelContinuationRequest {
                    model_turn_id: model_turn_id.clone(),
                    request_value_id: request_value_id.clone(),
                    previous_request: facts.previous_request,
                    results: facts.results,
                    deadline: job.started().deadline,
                    requested_attempt_limit,
                    cost_ceiling_microunits,
                })
                .await
                .inspect_err(|failure| {
                    eprintln!("Model tool continuation assembly rejected: {failure:?}");
                })?;
            if admission.request.value_id != request_value_id
                || admission.request.request.model_turn_id != model_turn_id
            {
                return Err(DurablePlanDriverError::InvariantViolation);
            }
            let model_job_id = self.new_id(ResourceKind::Job)?;
            let request_digest: Sha256Digest = canonical_digest(&json!({
                "job_id": fence.job_id,
                "lease_generation": fence.lease_epoch,
                "model_job_id": model_job_id,
                "model_turn_id": model_turn_id,
                "operation": "orchestration.model_tools.continue",
                "previous_model_turn_id": continuation.model_turn_id,
                "request_value_digest": admission.request.content_digest,
                "result_count": continuation.results.len(),
            }))
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?
            .parse()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
            let command = ContinueModelToolResultsToModelTurn {
                fence,
                plan: materialized.plan,
                continuation,
                model_turn_id,
                model_job_id,
                request: admission.request,
                requested_attempt_limit: admission.requested_attempt_limit,
                cost_ceiling_microunits: admission.cost_ceiling_microunits,
                idempotency_key_digest: self
                    .identities
                    .new_lease_token_digest()
                    .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
                request_digest,
                receipt_expires_at: job.started().deadline,
                mutations: allocate_model_turn_mutations(self.identities.as_ref())
                    .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
            };
            let mut transaction = self
                .repository
                .begin_scheduler_transaction()
                .await
                .map_err(classify_repository_failure)?;
            return match transaction
                .continue_model_tool_results_to_model_turn(command)
                .await
            {
                Ok(_) => transaction
                    .commit()
                    .await
                    .map_err(classify_repository_failure),
                Err(failure) => {
                    eprintln!("Model tool continuation commit rejected: {failure:?}");
                    transaction
                        .rollback()
                        .await
                        .map_err(classify_repository_failure)?;
                    Err(classify_repository_failure(failure))
                }
            };
        }
        let facts = self
            .repository
            .load_model_tool_continuation_facts(&fence, &materialized.plan, &continuation)
            .await
            .map_err(classify_repository_failure)?;
        let mut calls = Vec::with_capacity(facts.calls.len());
        for call in facts.calls {
            let input_value_id = self.new_id(ResourceKind::RunValue)?;
            let call_id_digest: Sha256Digest = canonical_digest(&json!({
                "call_id": &call.call_id,
            }))
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?
            .parse()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
            let decision = self
                .capability_admission
                .decide(ControllerCapabilityAdmissionRequest {
                    tenant_id: facts.tenant_id.clone(),
                    run_id: facts.run_id.clone(),
                    node_execution_id: facts.node_execution_id.clone(),
                    slot_id: call.slot_id.clone(),
                    selected_deployment: call.selected_deployment.clone(),
                    input_value_id: input_value_id.clone(),
                    input_content_digest: call.arguments.canonical_digest.clone(),
                    selection_evidence_digest: call_id_digest.clone(),
                })
                .await?;
            calls.push(ModelToolCapabilityAdmission {
                call_id: call.call_id,
                call_id_digest,
                projected_tool_name: call.projected_tool_name,
                slot_id: call.slot_id,
                selected_candidate_ordinal: call.selected_candidate_ordinal,
                selected_deployment: call.selected_deployment,
                input_value_id,
                arguments: call.arguments,
                invocation_id: self.new_id(ResourceKind::CapabilityInvocation)?,
                capability_job_id: self.new_id(ResourceKind::Job)?,
                approval_task_id: decision
                    .policies
                    .approval
                    .as_ref()
                    .map(|_| self.new_id(ResourceKind::ApprovalTask))
                    .transpose()?,
                policy_decisions: decision.policies,
                mcp_runtime: decision.mcp_runtime,
                requested_attempt_limit: 3,
                requested_retry_backoff_milliseconds: 100,
                idempotency_key_digest: self
                    .identities
                    .new_lease_token_digest()
                    .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
                mutations: allocate_model_tool_capability_mutations(self.identities.as_ref())
                    .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
            });
        }
        let request_digest: Sha256Digest = canonical_digest(&json!({
            "job_id": fence.job_id,
            "lease_generation": fence.lease_epoch,
            "model_turn_id": continuation.model_turn_id,
            "operation": "orchestration.model_tools.dispatch",
            "response_digest": continuation.response_digest,
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let source_mutations = OrchestrationYieldMutationIds {
            receipt_id: self.new_id(ResourceKind::Receipt)?,
            quota_entry_ids: (0..MAX_ORCHESTRATION_QUOTA_LINES)
                .map(|_| self.new_id(ResourceKind::QuotaLedgerEntry))
                .collect::<Result<Vec<_>, _>>()?,
            run_event_id: self.new_id(ResourceKind::Event)?,
            run_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            node_event_id: self.new_id(ResourceKind::Event)?,
            node_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            job_event_id: self.new_id(ResourceKind::Event)?,
            job_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
        };
        let mut transaction = self
            .repository
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        match transaction
            .dispatch_model_tool_capabilities(DispatchModelToolCapabilities {
                fence,
                plan: materialized.plan,
                continuation,
                calls,
                request_digest,
                receipt_expires_at: job.started().deadline,
                source_mutations,
            })
            .await
        {
            Ok(_) => transaction
                .commit()
                .await
                .map_err(classify_repository_failure),
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                Err(classify_repository_failure(failure))
            }
        }
    }

    async fn handoff_controller(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        reason: GenerationHandoffReason,
    ) -> Result<(), DurablePlanDriverError> {
        let delay = ChronoDuration::from_std(self.handoff_retry_delay)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let request_digest = canonical_digest(&json!({
            "job_id": fence.job_id,
            "lease_generation": fence.lease_epoch,
            "operation": "orchestration.controller.handoff",
            "reason": format!("{reason:?}"),
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let mutations = OrchestrationYieldMutationIds {
            receipt_id: self.new_id(ResourceKind::Receipt)?,
            quota_entry_ids: (0..MAX_ORCHESTRATION_QUOTA_LINES)
                .map(|_| self.new_id(ResourceKind::QuotaLedgerEntry))
                .collect::<Result<Vec<_>, _>>()?,
            run_event_id: self.new_id(ResourceKind::Event)?,
            run_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            node_event_id: self.new_id(ResourceKind::Event)?,
            node_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            job_event_id: self.new_id(ResourceKind::Event)?,
            job_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
        };
        let command = YieldOrchestrationJob {
            fence,
            outcome: OrchestrationYield::Retry {
                retry_at: Utc::now() + delay,
            },
            idempotency_key_digest: self
                .identities
                .new_lease_token_digest()
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
            request_digest,
            receipt_expires_at: job.started().deadline,
            mutations,
        };
        let mut transaction = self
            .repository
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        match transaction.yield_orchestration_job(command).await {
            Ok(_) => transaction
                .commit()
                .await
                .map_err(classify_repository_failure),
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

fn classify_repository_failure(failure: RepositoryError) -> DurablePlanDriverError {
    tracing::warn!("durable controller repository operation failed");
    eprintln!("durable controller repository operation failed");
    match failure {
        RepositoryError::Database(_) => DurablePlanDriverError::Unavailable,
        RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired => DurablePlanDriverError::FenceLost,
        RepositoryError::InvalidInput(_)
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict
        | RepositoryError::CorruptRow(_) => DurablePlanDriverError::InvariantViolation,
    }
}

fn is_retryable_postgres_transaction_abort(failure: &RepositoryError) -> bool {
    matches!(
        failure,
        RepositoryError::Database(sqlx::Error::Database(database))
            if matches!(database.code().as_deref(), Some("40001" | "40P01"))
    )
}

fn map_materialization_failure(failure: RunValueMaterializationError) -> DurablePlanDriverError {
    match failure {
        RunValueMaterializationError::Unavailable => DurablePlanDriverError::Unavailable,
        RunValueMaterializationError::FenceLost => DurablePlanDriverError::FenceLost,
        RunValueMaterializationError::Integrity => DurablePlanDriverError::InvariantViolation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_artifacts::SchedulerRunValueReadRequest;
    use insight_platform_contracts::{ArtifactRef, DataClassification};
    use insight_platform_orchestrator::ExactDataPortRef;
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
