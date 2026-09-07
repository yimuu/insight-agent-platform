//! Capability and Model admission use-case ports.
use crate::{ControllerRunValueReadContext, DurablePlanDriverError};
use async_trait::async_trait;
use chrono::Utc;
use insight_platform_contracts::*;
use insight_platform_orchestrator::store::ResolvedExpressionInput;
use insight_platform_plan::RuntimeNode;
use sha2::{Digest as _, Sha256};
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
    pub plan_node_key: insight_platform_plan::PlanNodeKey,
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
