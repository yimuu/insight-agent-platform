//! PostgreSQL-backed durable Plan-generation store.

use crate::{
    allocate_capability_invocation_mutations, allocate_child_run_mutations,
    allocate_context_query_mutations, allocate_controller_step_mutations,
    allocate_human_task_mutations, allocate_orchestration_terminal_mutations,
    CoordinatorIdentityFactory, DerivedControllerCommit, DurableCapabilityDispatchFacts,
    DurableChildAgentDispatchFacts, DurableContextDispatchFacts, DurableControllerFacts,
    DurableControllerPhase, DurablePlanDriverError, DurablePlanGenerationStore,
    GenerationHandoffReason, StartedOrchestrationJob,
};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_artifacts::{
    ArtifactObjectReadAuthorityError, SchedulerRunValueLease, SchedulerRunValueReadError,
    SchedulerRunValueReader, SchedulerRunValueRequestResolver, MAX_SCHEDULER_RUN_VALUE_BYTES,
};
use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, ClosedJsonValue, ResourceId, ResourceKind,
    Sha256Digest, ValueRef, MODEL_JSON_LIMITS,
};
use insight_platform_orchestrator::{
    derive_candidate_selection, ChildBudget, CommittedExpressionInput, DurableWaitKind,
    ExactRunValueRef, HumanTaskDefinition, RunInputValue, RuntimeNode, RuntimePlan,
};
use insight_platform_postgres::repository::{
    ApplyDerivedExpressionControllerStep, ApplyOrchestrationControllerStep, CommitPlanTerminal,
    ControllerFacts, DeferOrchestrationToCapabilityInvocation, DeferOrchestrationToChildRun,
    DeferOrchestrationToContextQuery, DeferOrchestrationToTask, DerivedExpressionFailureEvidence,
    FailOrchestrationJob, JobFence, MaterializedTerminalValue, OrchestrationFailureCause,
    OrchestrationYield, OrchestrationYieldMutationIds, PgRepository, RepositoryError,
    ResolvedExpressionInput, YieldOrchestrationJob, MAX_ORCHESTRATION_QUOTA_LINES,
};
use insight_platform_tasks::TaskDefinition;
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
}

#[async_trait]
pub trait ControllerCapabilityAdmissionProvider: Send + Sync + 'static {
    async fn decide(
        &self,
        request: ControllerCapabilityAdmissionRequest,
    ) -> Result<ControllerCapabilityAdmissionDecision, DurablePlanDriverError>;
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
                .map_err(classify_repository_failure)?;
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
            let mut transaction = self
                .repository
                .begin_scheduler_transaction()
                .await
                .map_err(classify_repository_failure)?;
            return match transaction.commit_plan_terminal(terminal).await {
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
