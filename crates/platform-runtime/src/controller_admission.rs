//! PostgreSQL-backed controller admission facts for Capability leaves.
//!
//! The provider derives the exact policy set from the immutable Run binding plus the selected
//! Capability Deployment. The owner transaction remains authoritative and repeats every binding,
//! gate and policy check before it creates an Invocation.

use crate::{
    ControllerCapabilityAdmissionDecision, ControllerCapabilityAdmissionProvider,
    ControllerCapabilityAdmissionRequest, ControllerModelAdmissionDecision,
    ControllerModelAdmissionProvider, ControllerModelAdmissionRequest,
    ControllerSandboxCapabilityAdmission, CoordinatorIdentityFactory, DurablePlanDriverError,
    UuidCoordinatorIdentityFactory,
};
use async_trait::async_trait;
use insight_platform_artifacts::{
    BrokeredSchedulerSkillInstructionMaterializer, SchedulerSkillPackageLease,
    SchedulerSkillPackageReader, SchedulerSkillPackageRequestResolver,
    MAX_SCHEDULER_SKILL_PACKAGE_BYTES,
};
use insight_platform_contracts::{
    canonical_digest, canonical_json, checked_in_hard_limit_profile, CapabilityBackendBinding,
    DataClassification, DeploymentClosure, ExactVersionRef, FrozenSlotTarget, ResourceId,
    ResourceKind, RunBindingsSnapshot, Sha256Digest, ValueRef,
};
use insight_platform_invocations::{
    InvocationPolicyDecision, InvocationPolicyDecisionBundle, InvocationPolicyDisposition,
    McpCapabilityRuntimeRequest,
};
use insight_platform_models::{
    assemble_prompt_messages, CanonicalModelRequest, ModelRequestValue, ModelResponseContract,
    ModelTurnLimits, PromptAssemblyBlock, PromptAssemblyPhase, SafeTraceContext,
};
use insight_platform_postgres::repository::PgRepository;
use serde_json::Value;
use sqlx::Row;
use std::collections::BTreeMap;
use std::sync::Arc;

pub type ControllerModelPolicyFacts =
    insight_platform_postgres::model_turn_repository::ExactModelPolicyFacts;

#[derive(Clone)]
pub struct PostgresControllerModelPolicyLoader {
    repository: PgRepository,
}

impl PostgresControllerModelPolicyLoader {
    pub fn new(repository: PgRepository) -> Self {
        Self { repository }
    }

    pub async fn load_exact(
        &self,
        tenant_id: &insight_platform_contracts::ResourceId,
        selected_deployment: &insight_platform_contracts::ExactDeploymentRef,
    ) -> Result<ControllerModelPolicyFacts, DurablePlanDriverError> {
        self.repository
            .load_exact_model_policy_facts(tenant_id, selected_deployment)
            .await
            .map_err(|failure| match failure {
                insight_platform_postgres::repository::RepositoryError::Database(_) => {
                    DurablePlanDriverError::Unavailable
                }
                insight_platform_postgres::repository::RepositoryError::NotFound(_)
                | insight_platform_postgres::repository::RepositoryError::StaleFence
                | insight_platform_postgres::repository::RepositoryError::LeaseExpired => {
                    DurablePlanDriverError::FenceLost
                }
                _ => DurablePlanDriverError::InvariantViolation,
            })
    }
}

#[derive(Clone)]
pub struct PostgresControllerModelAdmissionProvider {
    repository: PgRepository,
    skill_materializer: Arc<BrokeredSchedulerSkillInstructionMaterializer>,
}

impl PostgresControllerModelAdmissionProvider {
    pub fn new(
        repository: PgRepository,
        skill_resolver: Arc<dyn SchedulerSkillPackageRequestResolver>,
        skill_reader: Arc<dyn SchedulerSkillPackageReader>,
    ) -> Self {
        Self {
            repository,
            skill_materializer: Arc::new(BrokeredSchedulerSkillInstructionMaterializer::new(
                skill_resolver,
                skill_reader,
            )),
        }
    }
}

#[async_trait]
impl ControllerModelAdmissionProvider for PostgresControllerModelAdmissionProvider {
    async fn assemble(
        &self,
        request: ControllerModelAdmissionRequest,
    ) -> Result<ControllerModelAdmissionDecision, DurablePlanDriverError> {
        if request.lease.tenant_id != request.tenant_id
            || request.lease.run_id != request.run_id
            || request.deadline > request.lease.deadline
            || request.maximum_rounds == 0
            || request.maximum_capability_calls == 0
            || request.maximum_parallel_calls_per_round == 0
            || request.token_budget == 0
        {
            return Err(DurablePlanDriverError::InvariantViolation);
        }
        let insight_platform_orchestrator::RuntimeNode::ModelLoop {
            model_slot_id,
            skill_slot_ids,
            capability_slot_ids,
            input,
            maximum_rounds,
            maximum_capability_calls,
            maximum_parallel_calls_per_round,
            token_budget,
            ..
        } = &request.plan_node
        else {
            return Err(DurablePlanDriverError::InvariantViolation);
        };
        let requested_skill_slots = request
            .tool_slots
            .iter()
            .filter(|slot| matches!(&slot.target, FrozenSlotTarget::Skill { .. }))
            .map(|slot| slot.slot_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let requested_capability_slots = request
            .tool_slots
            .iter()
            .filter(|slot| matches!(&slot.target, FrozenSlotTarget::Capability { .. }))
            .map(|slot| slot.slot_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if model_slot_id != &request.model_slot_id
            || input != &request.input.port
            || *maximum_rounds != request.maximum_rounds
            || *maximum_capability_calls != request.maximum_capability_calls
            || *maximum_parallel_calls_per_round != request.maximum_parallel_calls_per_round
            || *token_budget != request.token_budget
            || requested_skill_slots != skill_slot_ids.iter().map(String::as_str).collect()
            || requested_capability_slots
                != capability_slot_ids.iter().map(String::as_str).collect()
            || request.input_value.schema_digest != request.input.schema_digest
            || request.input_value.canonical_digest != request.input.content_digest
        {
            return Err(DurablePlanDriverError::InvariantViolation);
        }
        let facts = self
            .repository
            .load_exact_controller_model_assembly_facts(
                &request.tenant_id,
                &request.run_id,
                &request.model_slot_id,
                &request.selected_deployment,
                &request.tool_slots,
            )
            .await
            .map_err(map_repository_error)?;
        if request.maximum_rounds > facts.profile.limits.maximum_rounds
            || request.maximum_capability_calls > facts.profile.tools.maximum_calls_per_turn
            || u32::from(request.maximum_parallel_calls_per_round)
                > facts.profile.limits.maximum_parallel_tool_calls
        {
            return Err(DurablePlanDriverError::InvariantViolation);
        }

        let mut blocks = vec![PromptAssemblyBlock {
            phase: PromptAssemblyPhase::PlatformSafety,
            ordinal: 0,
            source_kind: "model_safety_policy".to_owned(),
            source_id: facts.model.safety.contract_id.clone(),
            source_digest: facts.model.deployment.safety_policy.semantic_digest.clone(),
            classification: DataClassification::Internal,
            byte_budget: facts.model.safety.instruction_byte_budget,
            token_budget: facts.model.safety.instruction_token_budget,
            text: facts.model.safety.platform_instruction.clone(),
        }];
        let agent_text = canonical_text(&serde_json::json!({
            "agent_interface": facts.run_bindings.agent_interface,
            "contract_digest": facts.agent.contract_digest,
            "input_schema_digest": facts.agent.input_schema.canonical_digest,
            "output_schema_digest": facts.agent.output_schema.canonical_digest,
        }))?;
        blocks.push(exact_block(
            PromptAssemblyPhase::AgentContract,
            0,
            "agent_contract",
            facts.run_bindings.agent_interface.revision_id.to_string(),
            facts.run_bindings.agent_interface.semantic_digest.clone(),
            DataClassification::Internal,
            agent_text,
        )?);
        let node_text = canonical_text(&serde_json::json!({
            "node": request.plan_node,
            "plan_node_key": request.plan_node_key,
            "plan_revision": facts.run_bindings.plan,
        }))?;
        blocks.push(exact_block(
            PromptAssemblyPhase::PlanNodeInstruction,
            0,
            "plan_node",
            request.node_execution_id.to_string(),
            facts.run_bindings.plan.semantic_digest.clone(),
            DataClassification::Internal,
            node_text,
        )?);
        if let Some(block) = agent_instruction_block(
            facts.agent.author_instructions.as_deref(),
            &facts.run_bindings.agent_interface,
        )? {
            blocks.push(block);
        }

        let mut skill_ordinal = 0_u32;
        for skill in &facts.skills {
            let request_digest = digest_value(&serde_json::json!({
                "job_id": request.lease.orchestration_job_id,
                "lease_generation": request.lease.lease_generation,
                "operation": "scheduler.skill-package.materialize",
                "run_id": request.run_id,
                "skill_deployment": skill.deployment,
                "skill_slot_id": skill.slot_id,
            }))?;
            let maximum_bytes =
                usize::try_from(skill.skill.authoring_package.artifact.byte_length())
                    .ok()
                    .filter(|bytes| *bytes > 0 && *bytes <= MAX_SCHEDULER_SKILL_PACKAGE_BYTES)
                    .ok_or(DurablePlanDriverError::InvariantViolation)?;
            let sections = self
                .skill_materializer
                .materialize_all(
                    SchedulerSkillPackageLease {
                        tenant_id: request.tenant_id.clone(),
                        run_id: request.run_id.clone(),
                        orchestration_job_id: request.lease.orchestration_job_id.clone(),
                        worker_process_generation_id: request
                            .lease
                            .worker_process_generation_id
                            .clone(),
                        lease_generation: request.lease.lease_generation,
                        lease_token_digest: request.lease.lease_token_digest.clone(),
                        skill_slot_id: skill.slot_id.clone(),
                        skill_deployment_id: skill.deployment.deployment_id.clone(),
                        request_digest,
                        maximum_bytes,
                        deadline: request.deadline,
                    },
                    &skill.revision,
                    &skill.skill,
                )
                .await
                .map_err(|failure| match failure {
                    insight_platform_artifacts::SchedulerSkillInstructionMaterializeError::Unavailable => DurablePlanDriverError::Unavailable,
                    insight_platform_artifacts::SchedulerSkillInstructionMaterializeError::Denied
                    | insight_platform_artifacts::SchedulerSkillInstructionMaterializeError::NotFound => DurablePlanDriverError::FenceLost,
                    _ => DurablePlanDriverError::InvariantViolation,
                })?;
            for section in sections {
                blocks.push(PromptAssemblyBlock {
                    phase: PromptAssemblyPhase::SelectedSkill,
                    ordinal: skill_ordinal,
                    source_kind: "skill_instruction".to_owned(),
                    source_id: format!("{}:{}", skill.slot_id, section.section_id),
                    source_digest: skill.revision.semantic_digest.clone(),
                    classification: section.data_classification,
                    byte_budget: u32::try_from(section.text.len())
                        .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
                    token_budget: section.max_tokens,
                    text: section.text,
                });
                skill_ordinal = skill_ordinal
                    .checked_add(1)
                    .ok_or(DurablePlanDriverError::InvariantViolation)?;
            }
        }
        let input_text = canonical_text(&request.input_value.value)?;
        blocks.push(exact_block(
            PromptAssemblyPhase::UserInput,
            0,
            "run_value",
            request.input.run_value_id.to_string(),
            request.input.content_digest.clone(),
            request.input.classification,
            input_text,
        )?);

        let maximum_input_tokens = request
            .token_budget
            .min(facts.model.budget.maximum_input_tokens_per_turn)
            .min(u64::from(facts.profile.limits.maximum_input_tokens));
        let assembled = assemble_prompt_messages(
            blocks,
            facts.profile.limits.maximum_text_bytes,
            maximum_input_tokens,
        )
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let limits = ModelTurnLimits::from_profile(&checked_in_hard_limit_profile())
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let available_output = request
            .token_budget
            .checked_sub(assembled.total_estimated_tokens)
            .ok_or(DurablePlanDriverError::InvariantViolation)?;
        let policy_available_output = facts
            .model
            .budget
            .maximum_total_tokens_per_turn
            .checked_sub(assembled.total_estimated_tokens)
            .ok_or(DurablePlanDriverError::InvariantViolation)?;
        let hard_available_output = limits
            .maximum_tokens_per_turn()
            .checked_sub(assembled.total_estimated_tokens)
            .ok_or(DurablePlanDriverError::InvariantViolation)?;
        let max_output_tokens = u64::from(facts.profile.limits.maximum_output_tokens)
            .min(u64::from(facts.model.budget.maximum_output_tokens_per_turn))
            .min(available_output)
            .min(policy_available_output);
        let max_output_tokens = max_output_tokens.min(hard_available_output);
        let max_output_tokens = u32::try_from(max_output_tokens)
            .ok()
            .filter(|tokens| *tokens > 0)
            .ok_or(DurablePlanDriverError::InvariantViolation)?;
        let trace_id_digest = digest_value(&serde_json::json!({
            "model_turn_id": request.model_turn_id,
            "run_id": request.run_id,
            "tenant_id": request.tenant_id,
        }))?;
        let parent_span_id_digest = digest_value(&serde_json::json!({
            "node_execution_id": request.node_execution_id,
            "orchestration_job_id": request.lease.orchestration_job_id,
        }))?;
        let allow_tool_intents = !facts.tools.is_empty();
        let canonical = CanonicalModelRequest {
            schema_version: 1,
            model_turn_id: request.model_turn_id.clone(),
            messages: assembled.messages,
            tools: facts.tools,
            response_contract: ModelResponseContract {
                output_schema_digest: facts.agent.output_schema.canonical_digest.clone(),
                structured_schema: Some(facts.agent.output_schema),
                allow_tool_intents,
                allow_message_with_tool_intents: false,
            },
            generation_parameters: facts.model.deployment.generation_defaults.clone(),
            max_output_tokens,
            input_token_estimate: assembled.total_estimated_tokens,
            estimator_contract_digest: facts.profile.context.estimator_contract_digest.clone(),
            source_map_digest: assembled.source_map_digest,
            truncation_policy: facts.model.deployment.public_projection_policy.clone(),
            classification: assembled.classification,
            deadline: request.deadline,
            trace_context: SafeTraceContext {
                trace_id_digest,
                parent_span_id_digest,
            },
        };
        canonical
            .validate_for(
                &request.model_turn_id,
                &facts.provider,
                &facts.profile,
                &facts.provider_deployment.region,
                chrono::Utc::now(),
                limits,
            )
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let value = serde_json::to_value(&canonical)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let content_digest = digest_value(&value)?;
        let schema_digest = digest_value(&serde_json::json!({
            "contract": "canonical_model_request",
            "schema_version": 1,
        }))?;
        Ok(ControllerModelAdmissionDecision {
            request: ModelRequestValue {
                value_id: request.request_value_id,
                classification: canonical.classification,
                schema_digest,
                content_digest,
                value: ValueRef::Inline { value },
                request: canonical,
            },
            requested_attempt_limit: facts
                .model
                .budget
                .maximum_attempts_per_turn
                .min(limits.maximum_attempts()),
            cost_ceiling_microunits: facts.model.budget.cost_ceiling_microunits_per_turn,
        })
    }
}

fn canonical_text(value: &serde_json::Value) -> Result<String, DurablePlanDriverError> {
    String::from_utf8(
        canonical_json(value).map_err(|_| DurablePlanDriverError::InvariantViolation)?,
    )
    .map_err(|_| DurablePlanDriverError::InvariantViolation)
}

fn agent_instruction_block(
    instructions: Option<&str>,
    agent_revision: &ExactVersionRef,
) -> Result<Option<PromptAssemblyBlock>, DurablePlanDriverError> {
    instructions
        .map(|instructions| {
            exact_block(
                PromptAssemblyPhase::AgentInstruction,
                0,
                "agent_instruction",
                agent_revision.revision_id.to_string(),
                agent_revision.semantic_digest.clone(),
                DataClassification::Internal,
                instructions.to_owned(),
            )
        })
        .transpose()
}

#[allow(clippy::too_many_arguments)]
fn exact_block(
    phase: PromptAssemblyPhase,
    ordinal: u32,
    source_kind: &str,
    source_id: String,
    source_digest: Sha256Digest,
    classification: DataClassification,
    text: String,
) -> Result<PromptAssemblyBlock, DurablePlanDriverError> {
    let byte_budget =
        u32::try_from(text.len()).map_err(|_| DurablePlanDriverError::InvariantViolation)?;
    let token_budget = byte_budget
        .checked_add(3)
        .map(|bytes| bytes / 4)
        .ok_or(DurablePlanDriverError::InvariantViolation)?
        .max(1);
    Ok(PromptAssemblyBlock {
        phase,
        ordinal,
        source_kind: source_kind.to_owned(),
        source_id,
        source_digest,
        classification,
        byte_budget,
        token_budget,
        text,
    })
}

fn digest_value(value: &serde_json::Value) -> Result<Sha256Digest, DurablePlanDriverError> {
    canonical_digest(value)
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)
}

#[cfg(test)]
mod agent_instruction_tests {
    use super::*;

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .expect("fixture digest")
    }

    #[test]
    fn agent_instruction_block_is_exact_revision_scoped_and_optional() {
        let revision = ExactVersionRef::new(
            "aif_0198f1c3-8f49-7c3e-b1f3-773c28367b90"
                .parse()
                .expect("fixture revision ID"),
            digest('a'),
        )
        .expect("exact Agent Interface revision");

        assert_eq!(agent_instruction_block(None, &revision), Ok(None));
        let block = agent_instruction_block(Some("Use only the current input."), &revision)
            .expect("valid block")
            .expect("present instructions");
        assert_eq!(block.phase, PromptAssemblyPhase::AgentInstruction);
        assert_eq!(block.ordinal, 0);
        assert_eq!(block.source_kind, "agent_instruction");
        assert_eq!(block.source_id, revision.revision_id.to_string());
        assert_eq!(block.source_digest, revision.semantic_digest);
        assert_eq!(block.classification, DataClassification::Internal);
        assert_eq!(block.text, "Use only the current input.");
    }
}

fn map_repository_error(
    failure: insight_platform_postgres::repository::RepositoryError,
) -> DurablePlanDriverError {
    match failure {
        insight_platform_postgres::repository::RepositoryError::Database(_) => {
            DurablePlanDriverError::Unavailable
        }
        insight_platform_postgres::repository::RepositoryError::NotFound(_)
        | insight_platform_postgres::repository::RepositoryError::StaleFence
        | insight_platform_postgres::repository::RepositoryError::LeaseExpired => {
            DurablePlanDriverError::FenceLost
        }
        _ => DurablePlanDriverError::InvariantViolation,
    }
}

#[derive(Clone)]
pub struct PostgresControllerCapabilityAdmissionProvider {
    repository: PgRepository,
    sandbox: Option<ControllerSandboxCapabilityAdmission>,
}

impl PostgresControllerCapabilityAdmissionProvider {
    pub fn new(
        repository: PgRepository,
        sandbox: Option<ControllerSandboxCapabilityAdmission>,
    ) -> Self {
        Self {
            repository,
            sandbox,
        }
    }

    async fn load_facts(
        &self,
        request: &ControllerCapabilityAdmissionRequest,
    ) -> Result<
        (
            RunBindingsSnapshot,
            insight_platform_contracts::CapabilityDeploymentClosure,
        ),
        DurablePlanDriverError,
    > {
        let row = sqlx::query(
            r#"
            SELECT run.bindings_schema_version, run.bindings, run.bindings_digest,
                   deployment.payload_schema_version, deployment.bindings AS deployment_bindings,
                   deployment.bindings_digest AS deployment_bindings_digest
            FROM insight_platform.runs AS run
            JOIN insight_platform.deployments AS deployment
              ON deployment.tenant_id = run.tenant_id
             AND deployment.deployment_id = $3
            WHERE run.tenant_id = $1 AND run.run_id = $2
            "#,
        )
        .bind(request.tenant_id.to_string())
        .bind(request.run_id.to_string())
        .bind(request.selected_deployment.deployment_id.to_string())
        .fetch_optional(self.repository.pool())
        .await
        .map_err(|_| DurablePlanDriverError::Unavailable)?
        .ok_or(DurablePlanDriverError::FenceLost)?;
        if row
            .try_get::<i32, _>("bindings_schema_version")
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?
            != 1
            || row
                .try_get::<i32, _>("payload_schema_version")
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?
                != 1
        {
            return Err(DurablePlanDriverError::InvariantViolation);
        }
        let bindings_value: Value = row
            .try_get("bindings")
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let bindings: RunBindingsSnapshot = serde_json::from_value(bindings_value.clone())
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        bindings
            .validate()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let stored_bindings_digest: String = row
            .try_get("bindings_digest")
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        if bindings.canonical_digest.to_string() != stored_bindings_digest {
            return Err(DurablePlanDriverError::InvariantViolation);
        }
        let mut deployment_value: Value = row
            .try_get("deployment_bindings")
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let stored_deployment_digest: String = row
            .try_get("deployment_bindings_digest")
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let actual_deployment_digest: Sha256Digest = canonical_digest(&deployment_value)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?
            .parse()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        if actual_deployment_digest.to_string() != stored_deployment_digest
            || request.selected_deployment.deployment_digest != actual_deployment_digest
        {
            return Err(DurablePlanDriverError::FenceLost);
        }
        deployment_value
            .as_object_mut()
            .ok_or(DurablePlanDriverError::InvariantViolation)?
            .remove("schema_version");
        let closure: DeploymentClosure = serde_json::from_value(deployment_value)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        closure
            .validate()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let DeploymentClosure::CapabilityInterface(capability) = closure else {
            return Err(DurablePlanDriverError::InvariantViolation);
        };
        Ok((bindings, capability))
    }

    async fn resolve_mcp_runtime(
        &self,
        request: &ControllerCapabilityAdmissionRequest,
        bindings: &RunBindingsSnapshot,
        deployment: &insight_platform_contracts::CapabilityDeploymentClosure,
    ) -> Result<Option<McpCapabilityRuntimeRequest>, DurablePlanDriverError> {
        let CapabilityBackendBinding::Mcp { mcp_deployment, .. } = &deployment.backend else {
            return Ok(None);
        };
        let authorization_binding_ids: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT resource_id
            FROM insight_platform.resources
            WHERE tenant_id = $1
              AND resource_kind = 'mcp_authorization_binding'
              AND lifecycle_state = 'active'
              AND gate_state = 'enabled'
              AND payload_schema_version = 1
              AND payload->'mcp_deployment'->>'deployment_id' = $2
              AND payload->'mcp_deployment'->>'deployment_digest' = $3
              AND payload->>'principal_id' = $4
              AND payload->>'principal_identity_kind' = $5
              AND (payload->>'principal_binding_generation')::bigint = $6
              AND payload->>'state' = 'active'
              AND (payload->>'expires_at')::timestamptz > clock_timestamp()
            ORDER BY resource_id
            LIMIT 2
            "#,
        )
        .bind(request.tenant_id.to_string())
        .bind(mcp_deployment.deployment_id.to_string())
        .bind(mcp_deployment.deployment_digest.to_string())
        .bind(bindings.principal.principal_id.to_string())
        .bind(bindings.principal.principal_kind.as_str())
        .bind(
            i64::try_from(bindings.principal.binding_generation)
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
        )
        .fetch_all(self.repository.pool())
        .await
        .map_err(|_| DurablePlanDriverError::Unavailable)?;
        let [authorization_binding_id] = authorization_binding_ids.as_slice() else {
            return Err(DurablePlanDriverError::FenceLost);
        };
        let authorization_binding_id = authorization_binding_id
            .parse::<ResourceId>()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let mcp_operation_id = UuidCoordinatorIdentityFactory
            .new_resource_id(ResourceKind::McpOperation)
            .map_err(|_| DurablePlanDriverError::Unavailable)?;
        Ok(Some(McpCapabilityRuntimeRequest {
            mcp_operation_id,
            authorization_binding_id,
        }))
    }
}

#[async_trait]
impl ControllerCapabilityAdmissionProvider for PostgresControllerCapabilityAdmissionProvider {
    async fn decide(
        &self,
        request: ControllerCapabilityAdmissionRequest,
    ) -> Result<ControllerCapabilityAdmissionDecision, DurablePlanDriverError> {
        let (bindings, deployment) = self.load_facts(&request).await?;
        let slot = bindings
            .slots
            .iter()
            .find(|slot| slot.slot_id == request.slot_id)
            .ok_or(DurablePlanDriverError::FenceLost)?;
        let FrozenSlotTarget::Capability { candidates, .. } = &slot.target else {
            return Err(DurablePlanDriverError::FenceLost);
        };
        if !candidates.contains(&request.selected_deployment) {
            return Err(DurablePlanDriverError::FenceLost);
        }
        let mcp_runtime = self
            .resolve_mcp_runtime(&request, &bindings, &deployment)
            .await?;
        let sandbox = match deployment.backend {
            CapabilityBackendBinding::Sandbox { .. } => Some(
                self.sandbox
                    .clone()
                    .ok_or(DurablePlanDriverError::InvariantViolation)?,
            ),
            _ => None,
        };
        let policies = build_allowed_policy_bundle(
            &request,
            bindings
                .policies
                .iter()
                .map(|binding| binding.revision.clone())
                .chain(deployment.policies.iter().cloned()),
        )?;
        Ok(ControllerCapabilityAdmissionDecision {
            policies,
            mcp_runtime,
            sandbox,
        })
    }
}

fn build_allowed_policy_bundle(
    request: &ControllerCapabilityAdmissionRequest,
    policies: impl IntoIterator<Item = ExactVersionRef>,
) -> Result<InvocationPolicyDecisionBundle, DurablePlanDriverError> {
    let mut exact = BTreeMap::<_, ExactVersionRef>::new();
    for policy in policies {
        exact.insert(policy.revision_id.clone(), policy);
    }
    let decisions = exact
        .into_values()
        .map(|policy| {
            let evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
                "input_content_digest": request.input_content_digest,
                "input_value_id": request.input_value_id,
                "node_execution_id": request.node_execution_id,
                "policy": policy,
                "run_id": request.run_id,
                "schema_version": 1,
                "selected_deployment": request.selected_deployment,
                "selection_evidence_digest": request.selection_evidence_digest,
                "slot_id": request.slot_id,
                "tenant_id": request.tenant_id,
            }))
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?
            .parse()
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
            Ok(InvocationPolicyDecision {
                policy,
                disposition: InvocationPolicyDisposition::Allowed,
                evidence_digest,
            })
        })
        .collect::<Result<Vec<_>, DurablePlanDriverError>>()?;
    InvocationPolicyDecisionBundle::build(decisions, None)
        .map_err(|_| DurablePlanDriverError::InvariantViolation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{ExactDeploymentRef, ResourceId, ResourceKind};

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c5-0787-75e1-a9e8-d95ca0f3{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn request() -> ControllerCapabilityAdmissionRequest {
        ControllerCapabilityAdmissionRequest {
            tenant_id: id(ResourceKind::Tenant, 1),
            run_id: id(ResourceKind::Run, 2),
            node_execution_id: id(ResourceKind::NodeExecution, 3),
            slot_id: "search".to_owned(),
            selected_deployment: ExactDeploymentRef::new(
                id(ResourceKind::CapabilityDeployment, 4),
                digest('a'),
            )
            .unwrap(),
            input_value_id: id(ResourceKind::RunValue, 5),
            input_content_digest: digest('b'),
            selection_evidence_digest: digest('c'),
        }
    }

    #[test]
    fn policy_bundle_is_exact_deduplicated_and_request_bound() {
        let first = ExactVersionRef::new(id(ResourceKind::PolicyRevision, 6), digest('d')).unwrap();
        let second =
            ExactVersionRef::new(id(ResourceKind::PolicyRevision, 7), digest('e')).unwrap();
        let bundle =
            build_allowed_policy_bundle(&request(), vec![second.clone(), first.clone(), second])
                .unwrap();
        bundle
            .validate_for(&[
                first.clone(),
                first.clone(),
                bundle.decisions[1].policy.clone(),
            ])
            .unwrap();
        assert_eq!(bundle.decisions.len(), 2);
        assert!(bundle.approval.is_none());

        let mut changed = request();
        changed.input_content_digest = digest('f');
        let changed = build_allowed_policy_bundle(
            &changed,
            bundle
                .decisions
                .iter()
                .map(|decision| decision.policy.clone()),
        )
        .unwrap();
        assert_ne!(bundle.canonical_digest, changed.canonical_digest);
    }
}
