//! PostgreSQL-backed durable Plan-generation store.
use insight_platform_jobs::store::JobCommandFence as JobFence;
use insight_platform_orchestrator::store::ApplyDerivedExpressionControllerStep;
use insight_platform_orchestrator::store::ApplyOrchestrationControllerStep;
use insight_platform_orchestrator::store::CommitPlanTerminal;
use insight_platform_orchestrator::store::ContinueModelToolResultsToModelTurn;
use insight_platform_orchestrator::store::ControllerFacts;
use insight_platform_orchestrator::store::DeferOrchestrationToCapabilityInvocation;
use insight_platform_orchestrator::store::DeferOrchestrationToChildRun;
use insight_platform_orchestrator::store::DeferOrchestrationToContextQuery;
use insight_platform_orchestrator::store::DeferOrchestrationToModelTurn;
use insight_platform_orchestrator::store::DeferOrchestrationToTask;
use insight_platform_orchestrator::store::DerivedExpressionFailureEvidence;
use insight_platform_orchestrator::store::DispatchModelToolCapabilities;
use insight_platform_orchestrator::store::FailOrchestrationJob;
use insight_platform_orchestrator::store::MaterializedTerminalValue;
use insight_platform_orchestrator::store::ModelToolCapabilityAdmission;
use insight_platform_orchestrator::store::OrchestrationFailureCause;
use insight_platform_orchestrator::store::OrchestrationYield;
use insight_platform_orchestrator::store::OrchestrationYieldMutationIds;
use insight_platform_orchestrator::store::YieldOrchestrationJob;
use insight_platform_orchestrator::store::MAX_ORCHESTRATION_QUOTA_LINES;
use insight_platform_runtime::{
    child_descendant_budget_fits_hard_limit, ControllerCapabilityAdmissionProvider,
    ControllerCapabilityAdmissionRequest, ControllerModelAdmissionProvider,
    ControllerModelAdmissionRequest, ControllerModelContinuationRequest,
    ControllerRunValueMaterializer, ControllerRunValueReadContext, RunValueMaterializationError,
};

use crate::repository::{PgRepository, RepositoryError};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    canonical_digest, Failure, FailureClass, FailureCode, FailureSource, PlatformFailureCode,
    ResourceId, ResourceKind, Retryability, Sha256Digest, ValueRef,
};
use insight_platform_orchestrator::{
    derive_candidate_selection, CommittedExpressionInput, DurableWaitKind, ExactRunValueRef,
    RunInputValue,
};
use insight_platform_plan::{HumanTaskDefinition, RuntimeNode, RuntimePlan};
use insight_platform_runtime::{
    allocate_capability_invocation_mutations, allocate_child_run_mutations,
    allocate_context_query_mutations, allocate_controller_step_mutations,
    allocate_human_task_mutations, allocate_model_tool_capability_mutations,
    allocate_model_turn_mutations, allocate_orchestration_terminal_mutations,
    CoordinatorIdentityFactory, DerivedControllerCommit, DurableCapabilityDispatchFacts,
    DurableChildAgentDispatchFacts, DurableContextDispatchFacts, DurableControllerFacts,
    DurableControllerPhase, DurableModelDispatchFacts, DurablePlanDriverError,
    DurablePlanGenerationStore, GenerationHandoffReason, StartedOrchestrationJob,
};
use insight_platform_sandbox::contracts::SandboxCapabilitySubmission;
use insight_platform_sandbox::contracts::SANDBOX_QUOTA_LINES;
use insight_platform_tasks::TaskDefinition;
use serde_json::json;
use std::{sync::Arc, time::Duration};

pub struct PostgresDurablePlanGenerationStore<M, I> {
    repository: PgRepository,
    materializer: Arc<M>,
    identities: Arc<I>,
    capability_admission: Arc<dyn ControllerCapabilityAdmissionProvider>,
    model_admission: Arc<dyn ControllerModelAdmissionProvider>,
    handoff_retry_delay: Duration,
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
        materialized: insight_platform_runtime::MaterializedTypedPlan,
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
        commit_scheduler_mutation(&self.repository, |transaction| {
            let command = command.clone();
            Box::pin(async move {
                transaction
                    .fail_orchestration_job(command)
                    .await
                    .map(|_| ())
            })
        })
        .await
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
        commit_scheduler_mutation(&self.repository, |transaction| {
            let command = capability.clone();
            Box::pin(async move {
                transaction
                    .defer_orchestration_to_capability_invocation(command)
                    .await
                    .map(|_| ())
            })
        })
        .await
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
        commit_scheduler_mutation(&self.repository, |transaction| {
            let command = context.clone();
            Box::pin(async move {
                transaction
                    .defer_orchestration_to_context_query(command)
                    .await
                    .map(|_| ())
            })
        })
        .await
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
        commit_scheduler_mutation(&self.repository, |transaction| {
            let command = deferred.clone();
            Box::pin(async move {
                transaction
                    .defer_orchestration_to_model_turn(command)
                    .await
                    .map(|_| ())
            })
        })
        .await
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
        if !child_descendant_budget_fits_hard_limit(budget.maximum_descendant_runs) {
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
            "budget": budget,
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
            budget,
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
        commit_scheduler_mutation(&self.repository, |transaction| {
            let command = child.clone();
            Box::pin(async move {
                transaction
                    .defer_orchestration_to_child_run(command)
                    .await
                    .map(|_| ())
            })
        })
        .await
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
                        eligibility_rule: _,
                        interaction_kind,
                        eligible_principal_rule_digest,
                        safe_prompt_key,
                    } => TaskDefinition::Interaction {
                        interaction_kind: *interaction_kind,
                        eligible_principal_rule_digest: eligible_principal_rule_digest.clone(),
                        safe_prompt_key: safe_prompt_key.clone(),
                    },
                    HumanTaskDefinition::HumanWork {
                        eligibility_rule: _,
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
        commit_scheduler_mutation(&self.repository, |transaction| {
            let command = task.clone();
            Box::pin(async move {
                transaction
                    .defer_orchestration_to_task(command)
                    .await
                    .map(|_| ())
            })
        })
        .await
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
        commit_scheduler_mutation(&self.repository, |transaction| {
            let command = wait.clone();
            Box::pin(async move {
                transaction
                    .yield_orchestration_job(command)
                    .await
                    .map(|_| ())
            })
        })
        .await
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
            return commit_scheduler_mutation(&self.repository, |transaction| {
                let command = terminal.clone();
                Box::pin(async move { transaction.commit_plan_terminal(command).await.map(|_| ()) })
            })
            .await;
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
        if controller_failure {
            let failure = FailOrchestrationJob {
                fence: command.fence,
                plan: command.materialized.plan,
                cause: OrchestrationFailureCause::Controller {
                    observation: command.observation,
                },
                derived_expression: derived.map(|(materialized_inputs, evaluation)| {
                    DerivedExpressionFailureEvidence {
                        materialized_inputs,
                        evaluation,
                    }
                }),
                idempotency_key_digest,
                request_digest,
                receipt_expires_at: job.started().deadline,
                mutations,
            };
            return commit_scheduler_mutation(&self.repository, |transaction| {
                let command = failure.clone();
                Box::pin(async move {
                    transaction
                        .fail_orchestration_job(command)
                        .await
                        .map(|_| ())
                })
            })
            .await;
        }
        if let Some((inputs, evaluation)) = derived {
            let command = ApplyDerivedExpressionControllerStep {
                step,
                materialized_inputs: inputs,
                evaluation,
                output_value_ids,
            };
            commit_scheduler_mutation(&self.repository, |transaction| {
                let command = command.clone();
                Box::pin(async move {
                    transaction
                        .apply_derived_expression_controller_step(command)
                        .await
                        .map(|_| ())
                })
            })
            .await
        } else {
            commit_scheduler_mutation(&self.repository, |transaction| {
                let command = step.clone();
                Box::pin(async move {
                    transaction
                        .apply_orchestration_controller_step(command)
                        .await
                        .map(|_| ())
                })
            })
            .await
        }
    }

    async fn commit_convergence_failure(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        materialized: insight_platform_runtime::MaterializedTypedPlan,
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
        materialized: insight_platform_runtime::MaterializedTypedPlan,
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
            return commit_scheduler_mutation(&self.repository, |transaction| {
                let command = command.clone();
                Box::pin(async move {
                    transaction
                        .continue_model_tool_results_to_model_turn(command)
                        .await
                        .map(|_| ())
                })
            })
            .await;
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
        let command = DispatchModelToolCapabilities {
            fence,
            plan: materialized.plan,
            continuation,
            calls,
            request_digest,
            receipt_expires_at: job.started().deadline,
            source_mutations,
        };
        commit_scheduler_mutation(&self.repository, |transaction| {
            let command = command.clone();
            Box::pin(async move {
                transaction
                    .dispatch_model_tool_capabilities(command)
                    .await
                    .map(|_| ())
            })
        })
        .await
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
        commit_scheduler_mutation(&self.repository, |transaction| {
            let command = command.clone();
            Box::pin(async move {
                transaction
                    .yield_orchestration_job(command)
                    .await
                    .map(|_| ())
            })
        })
        .await
    }
}

type SchedulerMutationFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), RepositoryError>> + Send + 'a>>;

/// PostgreSQL has explicitly aborted these transactions, so retry the same allocated command
/// in a fresh transaction. A lease is revalidated by the owning command on every attempt.
/// Materialization, external effects and identity allocation stay outside this boundary.
async fn commit_scheduler_mutation<F>(
    repository: &PgRepository,
    mut apply: F,
) -> Result<(), DurablePlanDriverError>
where
    F: for<'a> FnMut(
            &'a mut crate::repository::PgSchedulerTransaction,
        ) -> SchedulerMutationFuture<'a>
        + Send,
{
    use crate::transaction_retry::{is_retryable_postgres_transaction_abort, MAXIMUM_ATTEMPTS};
    for attempt in 0..MAXIMUM_ATTEMPTS {
        let mut transaction = repository
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        let mutation = apply(&mut transaction).await;
        let failure = match mutation {
            Ok(()) => match transaction.commit().await {
                Ok(()) => return Ok(()),
                Err(failure) => failure,
            },
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                failure
            }
        };
        if attempt + 1 == MAXIMUM_ATTEMPTS || !is_retryable_postgres_transaction_abort(&failure) {
            // Connection loss and other ambiguous commit errors are deliberately not retried.
            return Err(classify_repository_failure(failure));
        }
        tokio::time::sleep(Duration::from_millis(1_u64 << attempt)).await;
    }
    unreachable!("the bounded final attempt always returns")
}

fn classify_repository_failure(failure: RepositoryError) -> DurablePlanDriverError {
    tracing::warn!("durable controller repository operation failed");
    match failure {
        RepositoryError::Database(_) | RepositoryError::CapacityUnavailable => {
            DurablePlanDriverError::Unavailable
        }
        RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired => DurablePlanDriverError::FenceLost,
        RepositoryError::InvalidInput(_)
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict
        | RepositoryError::PublicHistoryGap { .. }
        | RepositoryError::CorruptRow(_)
        | RepositoryError::InvalidPersistedObject(_) => DurablePlanDriverError::InvariantViolation,
    }
}

fn map_materialization_failure(failure: RunValueMaterializationError) -> DurablePlanDriverError {
    match failure {
        RunValueMaterializationError::Unavailable => DurablePlanDriverError::Unavailable,
        RunValueMaterializationError::FenceLost => DurablePlanDriverError::FenceLost,
        RunValueMaterializationError::Integrity => DurablePlanDriverError::InvariantViolation,
    }
}
