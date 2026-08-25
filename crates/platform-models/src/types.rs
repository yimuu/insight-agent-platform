use crate::{ModelTurnError, ModelTurnLimits};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, ClosedJsonValue, ClosedSchemaDocument, DataClassification, DecimalMoney,
    Effect, ExactDeploymentRef, ExactVersionRef, ModelProfileResourceSpec,
    ModelProviderResourceSpec, ResourceDocument, ResourceId, ResourceKind, Sha256Digest, ValueRef,
};
use insight_platform_invocations::{ExactInvocationValueRef, InvocationValueStorage};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_MODEL_NAME_BYTES: usize = 128;
pub const MAX_MODEL_CALL_ID_BYTES: usize = 128;
pub const MAX_MODEL_SAFE_IDENTITY_BYTES: usize = 512;
pub const MAX_MODEL_SOURCE_KIND_BYTES: usize = 64;
pub const MAX_MODEL_SOURCE_ID_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalMessageRole {
    Platform,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptAssemblyPhase {
    PlatformSafety,
    AgentContract,
    PlanNodeInstruction,
    RequiredSkill,
    SelectedSkill,
    ContextObservation,
    UserInput,
    ModelAssistant,
    CapabilityToolResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelContentSource {
    pub source_kind: String,
    pub source_id: String,
    pub source_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub assembly_phase: PromptAssemblyPhase,
    pub ordinal: u32,
    pub byte_budget: u32,
    pub token_budget: u32,
    pub trusted_instruction: bool,
}

impl ModelContentSource {
    fn validate_for_role(&self, role: CanonicalMessageRole) -> Result<(), ModelTurnError> {
        if !is_code(&self.source_kind, MAX_MODEL_SOURCE_KIND_BYTES)
            || self.source_id.is_empty()
            || self.source_id.len() > MAX_MODEL_SOURCE_ID_BYTES
            || self.source_id.chars().any(char::is_control)
            || self.byte_budget == 0
            || self.token_budget == 0
            || (self.trusted_instruction && role != CanonicalMessageRole::Platform)
            || matches!(
                self.assembly_phase,
                PromptAssemblyPhase::PlatformSafety
                    | PromptAssemblyPhase::AgentContract
                    | PromptAssemblyPhase::PlanNodeInstruction
            ) != self.trusted_instruction
            || (matches!(
                self.assembly_phase,
                PromptAssemblyPhase::RequiredSkill
                    | PromptAssemblyPhase::SelectedSkill
                    | PromptAssemblyPhase::ContextObservation
                    | PromptAssemblyPhase::UserInput
            ) && role != CanonicalMessageRole::User)
            || (self.assembly_phase == PromptAssemblyPhase::CapabilityToolResult
                && role != CanonicalMessageRole::Tool)
            || (self.assembly_phase == PromptAssemblyPhase::ModelAssistant
                && role != CanonicalMessageRole::Assistant)
        {
            return Err(ModelTurnError::InvalidMessage);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolResult {
    pub call_id: String,
    pub invocation_id: ResourceId,
    pub output_value_id: ResourceId,
    pub output_schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub classification: DataClassification,
    pub value: ValueRef,
}

impl ModelToolResult {
    fn validate(&self, limits: ModelTurnLimits) -> Result<(), ModelTurnError> {
        if !is_code(&self.call_id, MAX_MODEL_CALL_ID_BYTES)
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.output_value_id.kind() != ResourceKind::RunValue
        {
            return Err(ModelTurnError::InvalidToolResult);
        }
        self.value
            .validate(limits.inline_value_limits())
            .map_err(|_| ModelTurnError::InvalidToolResult)?;
        match &self.value {
            ValueRef::Inline { value } => {
                let digest = digest(value)?;
                if digest != self.content_digest {
                    return Err(ModelTurnError::InvalidToolResult);
                }
            }
            ValueRef::Artifact { artifact }
                if artifact.content_digest() == &self.content_digest
                    && artifact.classification() == self.classification => {}
            ValueRef::Artifact { .. } => return Err(ModelTurnError::InvalidToolResult),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum CanonicalMessagePart {
    Text(String),
    ToolResult(ModelToolResult),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalMessage {
    pub role: CanonicalMessageRole,
    pub parts: Vec<CanonicalMessagePart>,
    pub classification: DataClassification,
    pub source: ModelContentSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolProjection {
    pub projected_name: String,
    pub capability_deployment: ExactDeploymentRef,
    pub interface_revision: ExactVersionRef,
    pub input_schema: ClosedSchemaDocument,
    pub output_schema_digest: Sha256Digest,
    pub effect: Effect,
}

impl ModelToolProjection {
    fn validate(&self) -> Result<(), ModelTurnError> {
        if !is_code(&self.projected_name, MAX_MODEL_NAME_BYTES)
            || self.capability_deployment.resource_kind != ResourceKind::CapabilityDeployment
            || self.interface_revision.resource_kind != ResourceKind::CapabilityInterfaceRevision
        {
            return Err(ModelTurnError::InvalidToolProjection);
        }
        self.capability_deployment
            .validate()
            .map_err(|_| ModelTurnError::InvalidToolProjection)?;
        self.interface_revision
            .validate()
            .map_err(|_| ModelTurnError::InvalidToolProjection)?;
        self.input_schema
            .validate()
            .map_err(|_| ModelTurnError::InvalidSchema)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResponseContract {
    pub output_schema_digest: Sha256Digest,
    pub structured_schema: Option<ClosedSchemaDocument>,
    pub allow_tool_intents: bool,
    pub allow_message_with_tool_intents: bool,
}

impl ModelResponseContract {
    fn validate(&self, profile: &ModelProfileResourceSpec) -> Result<(), ModelTurnError> {
        if self.allow_tool_intents && !profile.tools.supported
            || self.allow_message_with_tool_intents
                && (!self.allow_tool_intents
                    || !profile.structured_output.may_combine_with_tool_intent)
        {
            return Err(ModelTurnError::InvalidResponseContract);
        }
        if let Some(schema) = &self.structured_schema {
            schema
                .validate()
                .map_err(|_| ModelTurnError::InvalidSchema)?;
            if !profile.structured_output.native && !profile.structured_output.textual_json_fallback
            {
                return Err(ModelTurnError::InvalidResponseContract);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeTraceContext {
    pub trace_id_digest: Sha256Digest,
    pub parent_span_id_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalModelRequest {
    pub schema_version: u32,
    pub model_turn_id: ResourceId,
    pub messages: Vec<CanonicalMessage>,
    pub tools: Vec<ModelToolProjection>,
    pub response_contract: ModelResponseContract,
    pub generation_parameters: ClosedJsonValue,
    pub max_output_tokens: u32,
    pub input_token_estimate: u64,
    pub estimator_contract_digest: Sha256Digest,
    pub source_map_digest: Sha256Digest,
    pub truncation_policy: ExactVersionRef,
    pub classification: DataClassification,
    pub deadline: DateTime<Utc>,
    pub trace_context: SafeTraceContext,
}

impl CanonicalModelRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn validate_for(
        &self,
        model_turn_id: &ResourceId,
        provider: &ModelProviderResourceSpec,
        profile: &ModelProfileResourceSpec,
        provider_region: &insight_platform_contracts::DataRegion,
        now: DateTime<Utc>,
        limits: ModelTurnLimits,
    ) -> Result<(), ModelTurnError> {
        ResourceDocument::ModelProvider(provider.clone())
            .validate()
            .map_err(|_| ModelTurnError::InvalidProvider)?;
        ResourceDocument::ModelProfile(Box::new(profile.clone()))
            .validate()
            .map_err(|_| ModelTurnError::InvalidProfile)?;
        if self.schema_version != 1
            || self.model_turn_id != *model_turn_id
            || self.model_turn_id.kind() != ResourceKind::ModelTurn
            || self.deadline <= now
            || self.truncation_policy.resource_kind != ResourceKind::PolicyRevision
            || self.generation_parameters.schema_digest != profile.parameter_schema_digest
            || self.generation_parameters.validate().is_err()
            || self.messages.is_empty()
            || self.messages.len()
                > usize::try_from(
                    profile
                        .limits
                        .maximum_messages
                        .min(provider.request_limits.maximum_messages),
                )
                .map_err(|_| ModelTurnError::InvalidLimits)?
            || self.max_output_tokens == 0
            || self.max_output_tokens > profile.limits.maximum_output_tokens
            || self.input_token_estimate == 0
            || self.input_token_estimate > u64::from(profile.limits.maximum_input_tokens)
            || self
                .input_token_estimate
                .checked_add(u64::from(self.max_output_tokens))
                .is_none_or(|total| {
                    total > u64::from(profile.context.maximum_context_tokens)
                        || total > limits.maximum_tokens_per_turn()
                })
            || self.classification.rank() > profile.data_handling.maximum_classification.rank()
            || !profile
                .data_handling
                .allowed_regions
                .contains(provider_region)
        {
            return Err(ModelTurnError::InvalidRequest);
        }
        self.truncation_policy
            .validate()
            .map_err(|_| ModelTurnError::InvalidRequest)?;
        self.response_contract.validate(profile)?;
        self.validate_tools(provider, profile, limits)?;
        self.validate_messages(provider, profile, limits)?;
        let request_bytes = serde_json::to_vec(self)
            .map_err(|_| ModelTurnError::Canonicalization)?
            .len();
        if request_bytes
            > usize::try_from(provider.request_limits.maximum_request_bytes)
                .map_err(|_| ModelTurnError::InvalidLimits)?
                .min(limits.maximum_request_bytes())
        {
            return Err(ModelTurnError::RequestTooLarge);
        }
        Ok(())
    }

    fn validate_tools(
        &self,
        provider: &ModelProviderResourceSpec,
        profile: &ModelProfileResourceSpec,
        limits: ModelTurnLimits,
    ) -> Result<(), ModelTurnError> {
        let maximum = usize::try_from(
            profile
                .tools
                .maximum_tools
                .min(profile.limits.maximum_tools)
                .min(provider.request_limits.maximum_tools),
        )
        .map_err(|_| ModelTurnError::InvalidLimits)?
        .min(limits.maximum_tool_calls());
        if (!profile.tools.supported && !self.tools.is_empty()) || self.tools.len() > maximum {
            return Err(ModelTurnError::InvalidToolProjection);
        }
        let mut names = BTreeSet::new();
        for tool in &self.tools {
            tool.validate()?;
            if !names.insert(tool.projected_name.as_str()) {
                return Err(ModelTurnError::InvalidToolProjection);
            }
        }
        if !self
            .tools
            .windows(2)
            .all(|pair| pair[0].projected_name < pair[1].projected_name)
        {
            return Err(ModelTurnError::NonCanonicalCollection);
        }
        Ok(())
    }

    fn validate_messages(
        &self,
        provider: &ModelProviderResourceSpec,
        profile: &ModelProfileResourceSpec,
        limits: ModelTurnLimits,
    ) -> Result<(), ModelTurnError> {
        let maximum_parts = usize::try_from(
            profile
                .limits
                .maximum_parts
                .min(provider.request_limits.maximum_parts),
        )
        .map_err(|_| ModelTurnError::InvalidLimits)?;
        let mut part_count = 0usize;
        let mut text_bytes = 0usize;
        let mut tool_result_calls = BTreeSet::new();
        for message in &self.messages {
            message.source.validate_for_role(message.role)?;
            if message.parts.is_empty()
                || message.classification.rank() > self.classification.rank()
                || message.classification.rank()
                    > profile.data_handling.maximum_classification.rank()
            {
                return Err(ModelTurnError::InvalidMessage);
            }
            for part in &message.parts {
                part_count = part_count
                    .checked_add(1)
                    .ok_or(ModelTurnError::InvalidLimits)?;
                match part {
                    CanonicalMessagePart::Text(text) => {
                        if text.is_empty() || text.chars().any(|character| character == '\0') {
                            return Err(ModelTurnError::InvalidMessage);
                        }
                        text_bytes = text_bytes
                            .checked_add(text.len())
                            .ok_or(ModelTurnError::InvalidLimits)?;
                    }
                    CanonicalMessagePart::ToolResult(result) => {
                        if message.role != CanonicalMessageRole::Tool
                            || !tool_result_calls.insert(result.call_id.as_str())
                        {
                            return Err(ModelTurnError::InvalidToolResult);
                        }
                        result.validate(limits)?;
                    }
                }
            }
            if message.role == CanonicalMessageRole::Tool
                && message
                    .parts
                    .iter()
                    .any(|part| !matches!(part, CanonicalMessagePart::ToolResult(_)))
            {
                return Err(ModelTurnError::InvalidToolResult);
            }
        }
        if part_count > maximum_parts
            || text_bytes
                > usize::try_from(profile.limits.maximum_text_bytes)
                    .map_err(|_| ModelTurnError::InvalidLimits)?
        {
            return Err(ModelTurnError::InvalidMessage);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, ModelTurnError> {
        digest(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalFinishReason {
    Completed,
    ToolUse,
    Length,
    ContentFiltered,
    CancelledByProvider,
    ProviderError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountingQuality {
    ProviderReported,
    Estimated,
    Reconciled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub provider_reported_cost: Option<DecimalMoney>,
    pub accounting_quality: AccountingQuality,
}

impl ModelUsage {
    pub fn validate_for(
        &self,
        profile: &ModelProfileResourceSpec,
    ) -> Result<ModelUsageAmounts, ModelTurnError> {
        if self
            .cached_input_tokens
            .zip(self.input_tokens)
            .is_some_and(|(cached, input)| cached > input)
            || self
                .reasoning_tokens
                .zip(self.output_tokens)
                .is_some_and(|(reasoning, output)| reasoning > output)
            || profile.usage.reports_cached_input_tokens != self.cached_input_tokens.is_some()
            || profile.usage.reports_reasoning_tokens != self.reasoning_tokens.is_some()
            || profile.usage.reports_cost != self.provider_reported_cost.is_some()
            || (self.accounting_quality == AccountingQuality::ProviderReported
                && profile.usage.provider_reports_usage
                && (self.input_tokens.is_none() || self.output_tokens.is_none()))
        {
            return Err(ModelTurnError::InvalidUsage);
        }
        let input = self.input_tokens.unwrap_or_default();
        let output = self.output_tokens.unwrap_or_default();
        let tokens = input
            .checked_add(output)
            .ok_or(ModelTurnError::InvalidUsage)?;
        let cost_microunits = self
            .provider_reported_cost
            .as_ref()
            .map(money_to_microunits)
            .transpose()?
            .unwrap_or_default();
        if self.provider_reported_cost.as_ref().is_some_and(|money| {
            profile
                .usage
                .cost_currency
                .as_deref()
                .is_none_or(|currency| currency != money.currency())
        }) {
            return Err(ModelTurnError::InvalidUsage);
        }
        Ok(ModelUsageAmounts {
            requests: 1,
            tokens,
            cost_microunits,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelUsageAmounts {
    pub requests: u64,
    pub tokens: u64,
    pub cost_microunits: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelObservation {
    pub request_sent: bool,
    pub provider_response_digest: Option<Sha256Digest>,
    pub actual_model_identity: Option<String>,
    pub model_fingerprint: Option<String>,
    pub possible_duplicate_charge: bool,
    pub stream_delta_count: u32,
    pub stream_bytes: u64,
}

impl ModelObservation {
    pub(crate) fn validate_for(
        &self,
        profile: &ModelProfileResourceSpec,
        maximum_response_bytes: usize,
    ) -> Result<(), ModelTurnError> {
        if (!self.request_sent
            && (self.provider_response_digest.is_some()
                || self.actual_model_identity.is_some()
                || self.model_fingerprint.is_some()
                || self.possible_duplicate_charge
                || self.stream_delta_count != 0
                || self.stream_bytes != 0))
            || self.stream_bytes > maximum_response_bytes as u64
            || self.actual_model_identity.as_ref().is_some_and(|identity| {
                identity.is_empty()
                    || identity.len() > MAX_MODEL_SAFE_IDENTITY_BYTES
                    || identity.chars().any(char::is_control)
            })
            || self.model_fingerprint.as_ref().is_some_and(|fingerprint| {
                fingerprint.is_empty()
                    || fingerprint.len() > MAX_MODEL_SAFE_IDENTITY_BYTES
                    || fingerprint.chars().any(char::is_control)
            })
            || (self.request_sent
                && self.provider_response_digest.is_some()
                && profile.model_identity.stability
                    == insight_platform_contracts::ModelIdentityStability::Pinned
                && self.actual_model_identity.as_deref()
                    != Some(profile.model_identity.value.as_str()))
        {
            return Err(ModelTurnError::InvalidObservation);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolIntent {
    pub call_id: String,
    pub projected_tool_name: String,
    pub arguments: ClosedJsonValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalAssistantMessage {
    pub parts: Vec<CanonicalMessagePart>,
    pub classification: DataClassification,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalModelResponse {
    pub schema_version: u32,
    pub message: Option<CanonicalAssistantMessage>,
    pub structured_output: Option<ClosedJsonValue>,
    pub tool_intents: Vec<ModelToolIntent>,
    pub finish_reason: CanonicalFinishReason,
    pub usage: ModelUsage,
    pub observation: ModelObservation,
}

impl CanonicalModelResponse {
    pub fn validate_for(
        &self,
        request: &CanonicalModelRequest,
        provider: &ModelProviderResourceSpec,
        profile: &ModelProfileResourceSpec,
        limits: ModelTurnLimits,
    ) -> Result<ModelUsageAmounts, ModelTurnError> {
        if self.schema_version != 1
            || !matches!(
                self.finish_reason,
                CanonicalFinishReason::Completed | CanonicalFinishReason::ToolUse
            )
            || !self.observation.request_sent
            || self.observation.provider_response_digest.is_none()
        {
            return Err(ModelTurnError::InvalidResponse);
        }
        self.observation.validate_for(
            profile,
            usize::try_from(provider.request_limits.maximum_response_bytes)
                .map_err(|_| ModelTurnError::InvalidLimits)?
                .min(limits.maximum_response_bytes()),
        )?;
        let usage = self.usage.validate_for(profile)?;
        self.validate_message(request, profile)?;
        self.validate_structured(request)?;
        self.validate_tool_intents(request, profile, limits)?;
        let response_bytes = serde_json::to_vec(self)
            .map_err(|_| ModelTurnError::Canonicalization)?
            .len();
        if response_bytes
            > usize::try_from(provider.request_limits.maximum_response_bytes)
                .map_err(|_| ModelTurnError::InvalidLimits)?
                .min(limits.maximum_response_bytes())
        {
            return Err(ModelTurnError::ResponseTooLarge);
        }
        Ok(usage)
    }

    fn validate_message(
        &self,
        request: &CanonicalModelRequest,
        profile: &ModelProfileResourceSpec,
    ) -> Result<(), ModelTurnError> {
        let Some(message) = &self.message else {
            if self.structured_output.is_none() && self.tool_intents.is_empty() {
                return Err(ModelTurnError::InvalidResponse);
            }
            return Ok(());
        };
        if message.parts.is_empty()
            || message.classification.rank() < request.classification.rank()
            || message.classification.rank() > profile.data_handling.maximum_classification.rank()
        {
            return Err(ModelTurnError::InvalidResponse);
        }
        let mut text_bytes = 0usize;
        for part in &message.parts {
            match part {
                CanonicalMessagePart::Text(text) => {
                    text_bytes = text_bytes
                        .checked_add(text.len())
                        .ok_or(ModelTurnError::InvalidLimits)?;
                }
                CanonicalMessagePart::ToolResult(_) => return Err(ModelTurnError::InvalidResponse),
            }
        }
        if text_bytes
            > usize::try_from(profile.limits.maximum_text_bytes)
                .map_err(|_| ModelTurnError::InvalidLimits)?
        {
            return Err(ModelTurnError::InvalidResponse);
        }
        Ok(())
    }

    fn validate_structured(&self, request: &CanonicalModelRequest) -> Result<(), ModelTurnError> {
        match (
            &request.response_contract.structured_schema,
            &self.structured_output,
        ) {
            (Some(schema), Some(output)) => {
                output
                    .validate()
                    .map_err(|_| ModelTurnError::SchemaValidationFailed)?;
                if output.schema_digest != schema.canonical_digest {
                    return Err(ModelTurnError::SchemaValidationFailed);
                }
                schema
                    .validate_instance(&output.value)
                    .map_err(|_| ModelTurnError::SchemaValidationFailed)
            }
            (Some(_), None) if self.finish_reason == CanonicalFinishReason::ToolUse => Ok(()),
            (Some(_), None) => Err(ModelTurnError::SchemaValidationFailed),
            (None, None) => Ok(()),
            (None, Some(_)) => Err(ModelTurnError::InvalidResponse),
        }
    }

    fn validate_tool_intents(
        &self,
        request: &CanonicalModelRequest,
        profile: &ModelProfileResourceSpec,
        limits: ModelTurnLimits,
    ) -> Result<(), ModelTurnError> {
        if (!request.response_contract.allow_tool_intents && !self.tool_intents.is_empty())
            || self.tool_intents.len()
                > usize::try_from(
                    profile
                        .tools
                        .maximum_calls_per_turn
                        .min(profile.limits.maximum_parallel_tool_calls),
                )
                .map_err(|_| ModelTurnError::InvalidLimits)?
                .min(limits.maximum_tool_calls())
            || (self.finish_reason == CanonicalFinishReason::ToolUse
                && self.tool_intents.is_empty())
            || (self.finish_reason == CanonicalFinishReason::Completed
                && !self.tool_intents.is_empty())
            || (!request.response_contract.allow_message_with_tool_intents
                && self.message.is_some()
                && !self.tool_intents.is_empty())
        {
            return Err(ModelTurnError::InvalidToolIntent);
        }
        let tools = request
            .tools
            .iter()
            .map(|tool| (tool.projected_name.as_str(), tool))
            .collect::<BTreeMap<_, _>>();
        let mut calls = BTreeSet::new();
        for intent in &self.tool_intents {
            if !is_code(&intent.call_id, MAX_MODEL_CALL_ID_BYTES)
                || !calls.insert(intent.call_id.as_str())
            {
                return Err(ModelTurnError::InvalidToolIntent);
            }
            let projection = tools
                .get(intent.projected_tool_name.as_str())
                .ok_or(ModelTurnError::InvalidToolIntent)?;
            intent
                .arguments
                .validate()
                .map_err(|_| ModelTurnError::InvalidToolIntent)?;
            if intent.arguments.schema_digest != projection.input_schema.canonical_digest {
                return Err(ModelTurnError::InvalidToolIntent);
            }
            projection
                .input_schema
                .validate_instance(&intent.arguments.value)
                .map_err(|_| ModelTurnError::SchemaValidationFailed)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRequestValue {
    pub value_id: ResourceId,
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub value: ValueRef,
    pub request: CanonicalModelRequest,
}

/// Exact logical Model request material handed to a Model Worker after a durable claim.
///
/// PostgreSQL returns the bounded Inline JSON frozen at admission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelExecutionInputMaterial {
    Inline { value: serde_json::Value },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelExecutionInput {
    pub exact: ExactInvocationValueRef,
    pub material: ModelExecutionInputMaterial,
}

impl ModelExecutionInput {
    pub fn validate(&self) -> Result<(), ModelTurnError> {
        self.exact
            .validate()
            .map_err(|_| ModelTurnError::InvalidRequestValue)?;
        if self.exact.value_kind != "model_request" {
            return Err(ModelTurnError::InvalidRequestValue);
        }
        match (&self.exact.storage, &self.material) {
            (InvocationValueStorage::Inline, ModelExecutionInputMaterial::Inline { value }) => {
                let actual: Sha256Digest = canonical_digest(value)
                    .map_err(|_| ModelTurnError::Canonicalization)?
                    .parse()
                    .map_err(|_| ModelTurnError::Canonicalization)?;
                if actual != self.exact.content_digest {
                    return Err(ModelTurnError::InvalidRequestValue);
                }
            }
            _ => return Err(ModelTurnError::InvalidRequestValue),
        }
        Ok(())
    }
}

impl ModelRequestValue {
    pub fn exact_for(
        &self,
        run_id: &ResourceId,
        node_id: &ResourceId,
        limits: ModelTurnLimits,
    ) -> Result<ExactInvocationValueRef, ModelTurnError> {
        if self.value_id.kind() != ResourceKind::RunValue
            || run_id.kind() != ResourceKind::Run
            || node_id.kind() != ResourceKind::NodeExecution
            || self.classification != self.request.classification
        {
            return Err(ModelTurnError::InvalidRequestValue);
        }
        self.value
            .validate(limits.inline_value_limits())
            .map_err(|_| ModelTurnError::InvalidRequestValue)?;
        let request_value =
            serde_json::to_value(&self.request).map_err(|_| ModelTurnError::Canonicalization)?;
        let request_digest = digest(&request_value)?;
        let storage = match &self.value {
            ValueRef::Inline { value }
                if value == &request_value && request_digest == self.content_digest =>
            {
                InvocationValueStorage::Inline
            }
            _ => return Err(ModelTurnError::InvalidRequestValue),
        };
        let exact = ExactInvocationValueRef {
            schema_version: 1,
            value_id: self.value_id.clone(),
            run_id: run_id.clone(),
            producing_node_id: Some(node_id.clone()),
            value_kind: "model_request".to_owned(),
            classification: self.classification,
            schema_digest: self.schema_digest.clone(),
            content_digest: self.content_digest.clone(),
            storage,
        };
        exact
            .validate()
            .map_err(|_| ModelTurnError::InvalidRequestValue)?;
        Ok(exact)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelOutputValue {
    pub value_id: ResourceId,
    pub structured_output_value_id: Option<ResourceId>,
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub value: ValueRef,
    pub response: CanonicalModelResponse,
    pub validation_evidence_digest: Sha256Digest,
}

impl ModelOutputValue {
    pub fn exact_for(
        &self,
        run_id: &ResourceId,
        node_id: &ResourceId,
        request: &CanonicalModelRequest,
        limits: ModelTurnLimits,
    ) -> Result<ExactInvocationValueRef, ModelTurnError> {
        if self.value_id.kind() != ResourceKind::RunValue
            || self
                .structured_output_value_id
                .as_ref()
                .is_some_and(|value_id| value_id.kind() != ResourceKind::RunValue)
            || self.structured_output_value_id.is_some()
                != self.response.structured_output.is_some()
            || self.schema_digest != request.response_contract.output_schema_digest
            || self.classification.rank() < request.classification.rank()
        {
            return Err(ModelTurnError::InvalidOutputValue);
        }
        self.value
            .validate(limits.inline_value_limits())
            .map_err(|_| ModelTurnError::InvalidOutputValue)?;
        let response_value =
            serde_json::to_value(&self.response).map_err(|_| ModelTurnError::Canonicalization)?;
        let response_digest = digest(&response_value)?;
        let storage = match &self.value {
            ValueRef::Inline { value }
                if value == &response_value && response_digest == self.content_digest =>
            {
                InvocationValueStorage::Inline
            }
            _ => return Err(ModelTurnError::InvalidOutputValue),
        };
        let exact = ExactInvocationValueRef {
            schema_version: 1,
            value_id: self.value_id.clone(),
            run_id: run_id.clone(),
            producing_node_id: Some(node_id.clone()),
            value_kind: "model_response".to_owned(),
            classification: self.classification,
            schema_digest: self.schema_digest.clone(),
            content_digest: self.content_digest.clone(),
            storage,
        };
        exact
            .validate()
            .map_err(|_| ModelTurnError::InvalidOutputValue)?;
        Ok(exact)
    }

    pub fn structured_output_exact_for(
        &self,
        run_id: &ResourceId,
        node_id: &ResourceId,
        request: &CanonicalModelRequest,
        limits: ModelTurnLimits,
    ) -> Result<ExactInvocationValueRef, ModelTurnError> {
        let value_id = self
            .structured_output_value_id
            .as_ref()
            .ok_or(ModelTurnError::InvalidOutputValue)?;
        let structured = self
            .response
            .structured_output
            .as_ref()
            .ok_or(ModelTurnError::InvalidOutputValue)?;
        if value_id.kind() != ResourceKind::RunValue
            || structured.schema_digest != self.schema_digest
            || structured.schema_digest != request.response_contract.output_schema_digest
            || self.classification.rank() < request.classification.rank()
        {
            return Err(ModelTurnError::InvalidOutputValue);
        }
        ValueRef::Inline {
            value: structured.value.clone(),
        }
        .validate(limits.inline_value_limits())
        .map_err(|_| ModelTurnError::InvalidOutputValue)?;
        let exact = ExactInvocationValueRef {
            schema_version: 1,
            value_id: value_id.clone(),
            run_id: run_id.clone(),
            producing_node_id: Some(node_id.clone()),
            value_kind: "model_structured_output".to_owned(),
            classification: self.classification,
            schema_digest: structured.schema_digest.clone(),
            content_digest: structured.canonical_digest.clone(),
            storage: InvocationValueStorage::Inline,
        };
        exact
            .validate()
            .map_err(|_| ModelTurnError::InvalidOutputValue)?;
        Ok(exact)
    }
}

fn money_to_microunits(money: &DecimalMoney) -> Result<u64, ModelTurnError> {
    money.validate().map_err(|_| ModelTurnError::InvalidUsage)?;
    let amount = u64::try_from(money.minor_units()).map_err(|_| ModelTurnError::InvalidUsage)?;
    if money.scale() <= 6 {
        amount
            .checked_mul(10u64.pow(u32::from(6 - money.scale())))
            .ok_or(ModelTurnError::InvalidUsage)
    } else {
        let divisor = 10u64.pow(u32::from(money.scale() - 6));
        if amount % divisor != 0 {
            return Err(ModelTurnError::InvalidUsage);
        }
        Ok(amount / divisor)
    }
}

pub(crate) fn digest<T: Serialize>(value: &T) -> Result<Sha256Digest, ModelTurnError> {
    let value = serde_json::to_value(value).map_err(|_| ModelTurnError::Canonicalization)?;
    canonical_digest(&value)
        .map_err(|_| ModelTurnError::Canonicalization)?
        .parse()
        .map_err(|_| ModelTurnError::Canonicalization)
}

pub(crate) fn is_code(value: &str, maximum_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum_bytes
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}
