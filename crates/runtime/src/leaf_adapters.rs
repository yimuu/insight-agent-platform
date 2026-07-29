//! Production adapters from immutable leaf requests to model and Action
//! registries. They render compiler-owned programs but never read author files,
//! resolve control flow, or commit durable state.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{Map, Value};
use tokio::sync::broadcast;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use insight_dsl::{template::compile_template, CompileError};
use insight_engine::{
    execution::{stop_pair, ExecutionControl, RunError, RunErrorKind, StopReason},
    plan::{DescriptorValue, PlanType, VersionTag},
    response::{
        LiveResponseBroker, LiveResponseItemIdentity, LiveResponsePayload, LiveResponsePublication,
        LiveResponseSeal, LiveResponseSealStatus, ResponseContentPart, ResponseItemStatus,
        ResponseOutputItem, ResponseRole, WorkflowToolPublicProjection,
    },
    worker::{
        adapter::{
            self as worker_adapter, ModelCallPublicItemAllocator,
            ModelCallPublicItemReservationError,
        },
        LeafTaskExecutor, ModelCallCompletion, ModelFinishReason, ModelFunctionCallPublication,
        ModelIncompleteFunctionCallPublication, ModelTokenUsage, ModelToolCall, ModelToolCallBatch,
        ResponseItemAuthority, TaskExecutionOrigin, TaskExecutionRequest, TaskExecutionResult,
        WorkerExecutionContext, WorkerExecutorRegistry, WorkerFailure, WorkerFailureClass,
        WorkerRuntimeServices,
    },
    ActivationId, AttemptNo, EffectEvidence, EffectIdempotency, RunId, RuntimeValue,
    SchedulerTaskKind, TaskOutputContract, WorkerCancellation, WorkerEffectClass,
    WorkerEffectPolicy,
};
use insight_resources::{
    actions::{ActionContext, ActionRegistry, EffectClass, IdempotencyClass, ToolPublicPolicy},
    models::{
        adapter::{select_structured_output_capability, validate_chat_request},
        model_response_too_large, ChatContent, ChatContentPart, ChatFinishReason, ChatMessage,
        ChatRequest, ChatRequestMode, ChatResponseFormat, ChatRole, ChatToolCall,
        ChatToolCallDelta, ChatToolChoice, ChatToolDefinition, ChatUsage, ModelCapability,
        ModelRegistry, ModelRequestCapability,
    },
};

use crate::catalog::VersionedLeafAdapterRegistry;

const LLM_DESCRIPTOR_VERSION: &str = "2";
const LEAF_DESCRIPTOR_VERSION: &str = "1";
const MAX_REQUEST_BYTES: usize = 1_048_576;
const MAX_TEMPLATE_BYTES: usize = 1_048_576;
const MAX_FUNCTION_ARGUMENT_FRAGMENTS_PER_CALL: usize = 16_384;
const MAX_FUNCTION_ARGUMENT_FRAGMENTS_PER_MODEL_CALL: usize = 65_536;

const LLM_DESCRIPTOR_INVALID: &str = "LLM_DESCRIPTOR_INVALID";
const LLM_BINDING_INVALID: &str = "LLM_BINDING_INVALID";
const LLM_MESSAGE_INVALID: &str = "LLM_MESSAGE_INVALID";
const LLM_REQUEST_TOO_LARGE: &str = "LLM_REQUEST_TOO_LARGE";
const LLM_RESPONSE_INVALID: &str = "LLM_RESPONSE_INVALID";
const LLM_PROVIDER_FAILED: &str = "LLM_PROVIDER_FAILED";
const LLM_PROVIDER_AUTHENTICATION_FAILED: &str = "LLM_PROVIDER_AUTHENTICATION_FAILED";
const LLM_PROVIDER_PERMISSION_DENIED: &str = "LLM_PROVIDER_PERMISSION_DENIED";
const LLM_PROVIDER_CONNECTION_FAILED: &str = "LLM_PROVIDER_CONNECTION_FAILED";
const LLM_PROVIDER_REQUEST_TIMEOUT: &str = "LLM_PROVIDER_REQUEST_TIMEOUT";
const LLM_PROVIDER_REQUEST_REJECTED: &str = "LLM_PROVIDER_REQUEST_REJECTED";
const LLM_PROVIDER_RATE_LIMITED: &str = "LLM_PROVIDER_RATE_LIMITED";
const LLM_PROVIDER_UNAVAILABLE: &str = "LLM_PROVIDER_UNAVAILABLE";
const LLM_PROVIDER_STREAM_FAILED: &str = "LLM_PROVIDER_STREAM_FAILED";
const LLM_PROVIDER_RESPONSE_INVALID: &str = "LLM_PROVIDER_RESPONSE_INVALID";
const LLM_PROVIDER_RESPONSE_TOO_LARGE: &str = "LLM_PROVIDER_RESPONSE_TOO_LARGE";
const MODEL_OUTPUT_TRUNCATED: &str = "MODEL_OUTPUT_TRUNCATED";
const MODEL_OUTPUT_FILTERED: &str = "MODEL_OUTPUT_FILTERED";
const MODEL_FINISH_REASON_INVALID: &str = "MODEL_FINISH_REASON_INVALID";
const LLM_TOOL_CALL_INVALID: &str = "LLM_TOOL_CALL_INVALID";
const LLM_TOOL_CONTINUATION_INVARIANT: &str = "LLM_TOOL_CONTINUATION_INVARIANT";
const LLM_TOOL_CONTINUATION_CAPABILITY: &str = "runtime.llm_tool_continuation.v1";
const LLM_TOOL_ROUND_LIMIT: &str = "LLM_TOOL_ROUND_LIMIT";
const LLM_TOOL_CALL_LIMIT: &str = "LLM_TOOL_CALL_LIMIT";
const LLM_PUBLICATION_AUTHORITY_LOST: &str = "LLM_PUBLICATION_AUTHORITY_LOST";
const MAX_FROZEN_LLM_TOOL_CALLS: u32 = 1_024;
const ACTION_DESCRIPTOR_INVALID: &str = "ACTION_DESCRIPTOR_INVALID";
const ACTION_BINDING_INVALID: &str = "ACTION_BINDING_INVALID";
const ACTION_EXECUTION_FAILED: &str = "ACTION_EXECUTION_FAILED";
const WORKER_CANCELLED: &str = "WORKER_CANCELLED";
const WORKER_DEADLINE_EXCEEDED: &str = "WORKER_DEADLINE_EXCEEDED";

#[derive(Clone)]
pub struct LlmTaskExecutor {
    models: ModelRegistry,
    token_observer: Option<broadcast::Sender<LlmTokenObservation>>,
    live_response_broker: Option<Arc<dyn LiveResponseBroker>>,
}

impl LlmTaskExecutor {
    pub fn new(models: ModelRegistry) -> Self {
        Self {
            models,
            token_observer: None,
            live_response_broker: None,
        }
    }

    pub fn with_live_response_broker(
        mut self,
        live_response_broker: Arc<dyn LiveResponseBroker>,
    ) -> Self {
        self.live_response_broker = Some(live_response_broker);
        self
    }

    /// Adds a process-local, best-effort observer for provider token chunks.
    ///
    /// Observations are deliberately outside the execution ledger and public
    /// event protocol. A closed or lagged receiver is therefore allowed to
    /// lose observations and can never change the validated worker result.
    pub fn with_token_observer(
        mut self,
        token_observer: broadcast::Sender<LlmTokenObservation>,
    ) -> Self {
        self.token_observer = Some(token_observer);
        self
    }
}

/// One transient LLM token observation.
///
/// This type is intentionally not serializable or `Debug`: token text can be
/// sensitive and must not accidentally enter a durable event, trace, or log.
#[derive(Clone)]
pub struct LlmTokenObservation {
    run_id: RunId,
    activation_id: ActivationId,
    attempt_no: AttemptNo,
    text: String,
}

impl LlmTokenObservation {
    fn new(request: &TaskExecutionRequest, context: &WorkerExecutionContext, text: String) -> Self {
        Self {
            run_id: request.run_id().clone(),
            activation_id: request.activation_id().clone(),
            attempt_no: context.attempt_no(),
            text,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn attempt_no(&self) -> AttemptNo {
        self.attempt_no
    }

    pub fn text(&self) -> &str {
        &self.text
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenLlmDeploymentBinding {
    adapter: String,
    model_alias: String,
    model_binding_hash: String,
    model_binding: Value,
    request_mode: String,
    request_capabilities: Vec<String>,
    tool_choice: String,
    tool_limits: FrozenLlmToolLimits,
    tools: Vec<FrozenLlmToolBinding>,
    #[serde(default)]
    runtime_capabilities: Vec<String>,
}

#[derive(Clone, Copy, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FrozenLlmToolLimits {
    max_rounds: u32,
    max_calls: u32,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenLlmToolBinding {
    name: String,
    action_id: String,
    action_version: String,
    descriptor_hash: String,
    input_schema: Value,
    output_schema: Value,
    effect: String,
    idempotency: String,
    cancellation: String,
    required_capabilities: Vec<String>,
    effect_policy: WorkerEffectPolicy,
    public_policy: Value,
    effective_public_policy: Value,
}

struct FrozenLlmTool {
    definition: ChatToolDefinition,
    validator: insight_engine::schema::JsonSchemaValidator,
    raw_argument_deltas_authorized: bool,
}

struct FrozenLlmToolContract {
    tools: Vec<FrozenLlmTool>,
    choice: ChatToolChoice,
    max_rounds: u32,
    max_calls: u32,
}

impl FrozenLlmToolContract {
    fn definitions(&self) -> Vec<ChatToolDefinition> {
        self.tools
            .iter()
            .map(|tool| tool.definition.clone())
            .collect()
    }
}

fn frozen_llm_tool_contract(
    request: &TaskExecutionRequest,
    configuration: &BTreeMap<String, DescriptorValue>,
    model_alias: &str,
    request_mode: ChatRequestMode,
    model_request_capabilities: &BTreeSet<ModelRequestCapability>,
) -> Result<FrozenLlmToolContract, WorkerFailure> {
    let binding =
        serde_json::from_value::<FrozenLlmDeploymentBinding>(request.deployment_binding().clone())
            .map_err(|_| invariant(LLM_BINDING_INVALID))?;
    let expected_mode = match request_mode {
        ChatRequestMode::Complete => ModelRequestCapability::Complete.as_str(),
        ChatRequestMode::Streaming => ModelRequestCapability::Streaming.as_str(),
    };
    let expected_capabilities = model_request_capabilities
        .iter()
        .map(|capability| capability.as_str())
        .collect::<BTreeSet<_>>();
    let bound_capabilities = binding
        .request_capabilities
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if binding.adapter != "core.llm"
        || binding.model_alias != model_alias
        || binding.request_mode != expected_mode
        || !valid_frozen_evidence_string(&binding.model_binding_hash, 256)
        || !binding.model_binding.is_object()
        || bound_capabilities.len() != binding.request_capabilities.len()
        || bound_capabilities != expected_capabilities
    {
        return Err(invariant(LLM_BINDING_INVALID));
    }

    let configured_tools = descriptor_array(required(configuration, "tools")?)?
        .iter()
        .map(descriptor_string)
        .collect::<Result<Vec<_>, _>>()?;
    let configured_choice = descriptor_string(required(configuration, "tool_choice")?)?;
    let configured_limits = configured_llm_tool_limits(configuration)?;
    let expected_runtime_capabilities = if configured_tools.is_empty() {
        Vec::new()
    } else {
        vec![LLM_TOOL_CONTINUATION_CAPABILITY]
    };
    if binding.tool_choice != configured_choice
        || binding.tool_limits != configured_limits
        || binding.tools.len() != configured_tools.len()
        || binding
            .runtime_capabilities
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != expected_runtime_capabilities
    {
        return Err(invariant(LLM_BINDING_INVALID));
    }

    let mut names = BTreeSet::new();
    let mut tools = Vec::with_capacity(binding.tools.len());
    for (linked, configured_name) in binding.tools.into_iter().zip(configured_tools) {
        let expected_effect = match linked.effect.as_str() {
            "pure" => WorkerEffectClass::Pure,
            "read_only" => WorkerEffectClass::ReadOnly,
            "mutating" => WorkerEffectClass::Mutating,
            _ => return Err(invariant(LLM_BINDING_INVALID)),
        };
        let expected_idempotency = match linked.idempotency.as_str() {
            "idempotent" => EffectIdempotency::Idempotent,
            "non_idempotent" => EffectIdempotency::NonIdempotent,
            _ => return Err(invariant(LLM_BINDING_INVALID)),
        };
        let expected_cancellation = match linked.cancellation.as_str() {
            "cooperative" => WorkerCancellation::Cooperative,
            "not_supported" => WorkerCancellation::LeaseOnly,
            _ => return Err(invariant(LLM_BINDING_INVALID)),
        };
        if linked.name != configured_name
            || linked.action_id != linked.name
            || !valid_tool_name(&linked.name)
            || !names.insert(linked.name.clone())
            || semver::Version::parse(&linked.action_version).is_err()
            || !is_lower_sha256(&linked.descriptor_hash)
            || !linked.input_schema.is_object()
            || !linked.output_schema.is_object()
            || linked
                .required_capabilities
                .iter()
                .any(|capability| !valid_qualified_name(capability))
            || linked
                .required_capabilities
                .windows(2)
                .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
            || linked.effect_policy.effect_class() != expected_effect
            || linked.effect_policy.effect_idempotency() != expected_idempotency
            || linked.effect_policy.cancellation() != expected_cancellation
            || !linked.public_policy.is_object()
            || !linked.effective_public_policy.is_object()
            || serde_json::from_value::<ToolPublicPolicy>(linked.public_policy.clone()).is_err()
            || serde_json::from_value::<ToolPublicPolicy>(linked.effective_public_policy.clone())
                .is_err()
        {
            return Err(invariant(LLM_BINDING_INVALID));
        }
        let public_projection = WorkflowToolPublicProjection::from_frozen_effective_policy(
            &linked.effective_public_policy,
        )
        .map_err(|_| invariant(LLM_BINDING_INVALID))?;
        let validator = insight_engine::schema::compile_schema_2020(&linked.input_schema)
            .map_err(|_| invariant(LLM_BINDING_INVALID))?;
        insight_engine::schema::compile_schema_2020(&linked.output_schema)
            .map_err(|_| invariant(LLM_BINDING_INVALID))?;
        tools.push(FrozenLlmTool {
            definition: ChatToolDefinition {
                name: linked.name,
                description: None,
                input_schema: linked.input_schema,
            },
            validator,
            raw_argument_deltas_authorized: public_projection.raw_argument_deltas_authorized(),
        });
    }

    let choice = match binding.tool_choice.as_str() {
        "auto" => ChatToolChoice::Auto,
        "required" if !tools.is_empty() => ChatToolChoice::Required,
        name if names.contains(name) => ChatToolChoice::Named(name.to_owned()),
        _ => return Err(invariant(LLM_BINDING_INVALID)),
    };
    Ok(FrozenLlmToolContract {
        tools,
        choice,
        max_rounds: configured_limits.max_rounds,
        max_calls: configured_limits.max_calls,
    })
}

fn configured_llm_tool_limits(
    configuration: &BTreeMap<String, DescriptorValue>,
) -> Result<FrozenLlmToolLimits, WorkerFailure> {
    let limits = descriptor_object(required(configuration, "tool_limits")?)?;
    if limits.len() != 2 {
        return Err(invariant(LLM_BINDING_INVALID));
    }
    let limit = |name: &str| match limits.get(name) {
        Some(DescriptorValue::Integer(value)) => u32::try_from(*value).ok(),
        _ => None,
    };
    let max_rounds = limit("max_rounds").ok_or_else(|| invariant(LLM_BINDING_INVALID))?;
    let max_calls = limit("max_calls").ok_or_else(|| invariant(LLM_BINDING_INVALID))?;
    if max_rounds == 0
        || max_calls == 0
        || max_rounds > max_calls
        || max_calls > MAX_FROZEN_LLM_TOOL_CALLS
    {
        return Err(invariant(LLM_BINDING_INVALID));
    }
    Ok(FrozenLlmToolLimits {
        max_rounds,
        max_calls,
    })
}

fn valid_frozen_evidence_string(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && !value.chars().any(|character| character.is_control())
}

fn valid_tool_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_qualified_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.split('.').all(|segment| {
            let mut bytes = segment.bytes();
            bytes
                .next()
                .is_some_and(|first| first == b'_' || first.is_ascii_alphabetic())
                && bytes.all(|byte| byte == b'_' || byte == b'-' || byte.is_ascii_alphanumeric())
        })
}

#[derive(Default)]
struct StreamingToolCall {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
    argument_fragments: Vec<String>,
    published_fragments: usize,
}

#[derive(Default)]
struct StreamingToolCallAccumulator {
    calls: BTreeMap<u32, StreamingToolCall>,
    argument_bytes: usize,
    argument_fragment_count: usize,
}

impl StreamingToolCallAccumulator {
    fn push(
        &mut self,
        deltas: Vec<ChatToolCallDelta>,
        text_bytes: usize,
        max_bytes: usize,
    ) -> Result<(), WorkerFailure> {
        for delta in deltas {
            if self.argument_fragment_count >= MAX_FUNCTION_ARGUMENT_FRAGMENTS_PER_MODEL_CALL {
                return Err(infrastructure(LLM_TOOL_CALL_INVALID, false));
            }
            let next_argument_bytes = self
                .argument_bytes
                .saturating_add(delta.arguments_delta.len());
            if text_bytes.saturating_add(next_argument_bytes) > max_bytes {
                return Err(infrastructure(LLM_TOOL_CALL_INVALID, false));
            }
            let call = self.calls.entry(delta.index).or_default();
            if call.argument_fragments.len() >= MAX_FUNCTION_ARGUMENT_FRAGMENTS_PER_CALL {
                return Err(infrastructure(LLM_TOOL_CALL_INVALID, false));
            }
            if !merge_streaming_identity(&mut call.id, delta.id)
                || !merge_streaming_identity(&mut call.name, delta.name)
            {
                return Err(infrastructure(LLM_TOOL_CALL_INVALID, false));
            }
            call.arguments.push_str(&delta.arguments_delta);
            call.argument_fragments.push(delta.arguments_delta);
            self.argument_fragment_count = self.argument_fragment_count.saturating_add(1);
            self.argument_bytes = next_argument_bytes;
        }
        Ok(())
    }

    fn argument_bytes(&self) -> usize {
        self.argument_bytes
    }

    async fn publish_ready(
        &mut self,
        publication: &mut LlmPublication,
        contract: &FrozenLlmToolContract,
    ) -> Result<(), WorkerFailure> {
        for (index, call) in &mut self.calls {
            let (Some(call_id), Some(name)) = (&call.id, &call.name) else {
                continue;
            };
            let Some(tool) = contract
                .tools
                .iter()
                .find(|tool| tool.definition.name == *name)
            else {
                continue;
            };
            if !tool.raw_argument_deltas_authorized {
                continue;
            }
            publication
                .ensure_function_started(*index, call_id, name)
                .await?;
            for fragment in &call.argument_fragments[call.published_fragments..] {
                publication.function_argument_delta(*index, fragment.clone())?;
            }
            call.published_fragments = call.argument_fragments.len();
        }
        Ok(())
    }

    fn complete(self) -> Result<Vec<ChatToolCall>, WorkerFailure> {
        self.calls
            .into_iter()
            .map(|(index, call)| {
                Ok(ChatToolCall {
                    index,
                    id: call
                        .id
                        .ok_or_else(|| infrastructure(LLM_TOOL_CALL_INVALID, false))?,
                    name: call
                        .name
                        .ok_or_else(|| infrastructure(LLM_TOOL_CALL_INVALID, false))?,
                    arguments: call.arguments,
                })
            })
            .collect()
    }
}

fn merge_streaming_identity(target: &mut Option<String>, value: Option<String>) -> bool {
    let Some(value) = value else {
        return true;
    };
    match target {
        Some(existing) => existing == &value,
        None => {
            *target = Some(value);
            true
        }
    }
}

fn normalize_model_tool_calls(
    calls: Vec<ChatToolCall>,
    contract: &FrozenLlmToolContract,
    max_argument_bytes: usize,
) -> Result<Vec<ModelToolCall>, WorkerFailure> {
    if calls.is_empty() {
        return Err(infrastructure(LLM_TOOL_CALL_INVALID, false));
    }
    let mut total_argument_bytes = 0usize;
    let mut call_ids = BTreeSet::new();
    calls
        .into_iter()
        .enumerate()
        .map(|(expected_index, call)| {
            total_argument_bytes = total_argument_bytes.saturating_add(call.arguments.len());
            let tool = contract
                .tools
                .iter()
                .find(|tool| tool.definition.name == call.name)
                .ok_or_else(|| infrastructure(LLM_TOOL_CALL_INVALID, false))?;
            let arguments = serde_json::from_str::<Value>(&call.arguments)
                .map_err(|_| infrastructure(LLM_TOOL_CALL_INVALID, false))?;
            if total_argument_bytes > max_argument_bytes
                || call.index != u32::try_from(expected_index).unwrap_or(u32::MAX)
                || !call_ids.insert(call.id.clone())
                || !arguments.is_object()
                || !tool.validator.is_valid(&arguments)
            {
                return Err(infrastructure(LLM_TOOL_CALL_INVALID, false));
            }
            ModelToolCall::new(call.index, call.id, call.name, arguments)
                .map_err(|_| infrastructure(LLM_TOOL_CALL_INVALID, false))
        })
        .collect()
}

#[async_trait]
impl LeafTaskExecutor for LlmTaskExecutor {
    fn live_response_capable(&self) -> bool {
        self.live_response_broker.is_some()
    }

    async fn execute(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        let services = WorkerRuntimeServices::default();
        self.execute_with_runtime_services(context, request, &services, cancellation)
            .await
    }

    async fn execute_with_runtime_services(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        services: &WorkerRuntimeServices,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        if request.task_kind() != SchedulerTaskKind::Llm || request.implementation() != "core.llm" {
            return Err(invariant(LLM_DESCRIPTOR_INVALID));
        }
        context
            .validate_model_continuation()
            .map_err(|_| invariant(LLM_TOOL_CONTINUATION_INVARIANT))?;
        let configuration = request.public_configuration();
        let model_alias = descriptor_string(required(configuration, "model")?)?;
        let stream_requested = descriptor_bool(required(configuration, "stream")?)?;
        let publish = descriptor_bool(required(configuration, "publish")?)?;
        let model = self
            .models
            .resolve(model_alias)
            .map_err(|_| invariant(LLM_DESCRIPTOR_INVALID))?;
        let request_mode = if stream_requested {
            ChatRequestMode::Streaming
        } else {
            ChatRequestMode::Complete
        };
        let required_request_capability = if stream_requested {
            ModelRequestCapability::Streaming
        } else {
            ModelRequestCapability::Complete
        };
        let model_request_capabilities = model.request_capabilities();
        if !model_request_capabilities.contains(&required_request_capability) {
            return Err(invariant(LLM_DESCRIPTOR_INVALID));
        }
        let tool_contract = frozen_llm_tool_contract(
            request,
            configuration,
            model_alias,
            request_mode,
            &model_request_capabilities,
        )?;
        validate_completed_tool_limits(context, &tool_contract)?;
        let parameters = configuration
            .get("parameters")
            .map(descriptor_json)
            .transpose()?
            .unwrap_or_else(|| Value::Object(Map::new()));
        model
            .validate_parameters(&parameters)
            .map_err(|_| invariant(LLM_DESCRIPTOR_INVALID))?;

        let bindings = RuntimeBindings::new(configuration, request)?;
        let mut messages = render_messages(configuration, &bindings)?;
        append_model_continuation(&mut messages, context)?;
        let output = only_output(request)?;
        let response_format = if output.value_type().string_constraints().is_some() {
            None
        } else {
            let schema = output
                .value_type()
                .json_schema_document()
                .map_err(|_| invariant(LLM_DESCRIPTOR_INVALID))?;
            let capability = select_structured_output_capability(
                &model.capabilities(),
                matches!(output.value_type(), PlanType::Object { .. }),
            )
            .ok_or_else(|| invariant(LLM_DESCRIPTOR_INVALID))?;
            Some(match capability {
                ModelCapability::JsonObjectOutput => ChatResponseFormat::JsonObject {
                    name: "response".to_owned(),
                    schema,
                },
                ModelCapability::JsonSchemaOutput => ChatResponseFormat::JsonSchema {
                    name: "response".to_owned(),
                    schema,
                },
                ModelCapability::Vision => unreachable!("vision is not structured output"),
            })
        };
        let chat_request = ChatRequest {
            messages,
            parameters,
            response_format,
            tools: tool_contract.definitions(),
            tool_choice: tool_contract.choice.clone(),
        };
        validate_chat_request(&chat_request).map_err(|_| {
            invariant(if context.continuation_turns().is_empty() {
                LLM_MESSAGE_INVALID
            } else {
                LLM_TOOL_CONTINUATION_INVARIANT
            })
        })?;
        if !model.request_body_within_limit_for_mode(&chat_request, request_mode, MAX_REQUEST_BYTES)
        {
            return Err(infrastructure(LLM_REQUEST_TOO_LARGE, false));
        }

        require_live(context, &cancellation)?;
        let mut publication = LlmPublication::start(
            self.live_response_broker.as_ref(),
            context,
            request,
            publish,
            services,
        )?;
        let mut text = String::new();
        let mut finish_reason = ChatFinishReason::Invalid;
        let mut usage = None;
        let mut complete_tool_calls = Vec::new();
        let mut streaming_tool_calls = StreamingToolCallAccumulator::default();
        let max_text_bytes = model.max_accumulated_text_bytes();
        if stream_requested {
            let stream_future = model.stream_chat_events(chat_request);
            tokio::pin!(stream_future);
            let mut stream = tokio::select! {
                result = &mut stream_future => match result {
                    Ok(stream) => stream,
                    Err(error) => {
                        let seal = publication.fail();
                        return Err(with_model_completion(
                            context,
                            map_llm_error(error),
                            ChatFinishReason::Invalid,
                            None,
                            seal,
                            None,
                        ));
                    }
                },
                _ = cancellation.cancelled() => {
                    let seal = publication.fail();
                    return Err(with_model_completion(context, control(WORKER_CANCELLED), ChatFinishReason::Invalid, None, seal, None));
                },
                _ = sleep(remaining(context)?) => {
                    let seal = publication.fail();
                    return Err(with_model_completion(context, control(WORKER_DEADLINE_EXCEEDED), ChatFinishReason::Invalid, None, seal, None));
                },
            };
            loop {
                let event = tokio::select! {
                    value = stream.next() => value,
                    _ = cancellation.cancelled() => {
                        let seal = publication.fail();
                        return Err(with_model_completion(context, control(WORKER_CANCELLED), finish_reason, usage.as_ref(), seal, None));
                    },
                    _ = sleep(remaining(context)?) => {
                        let seal = publication.fail();
                        return Err(with_model_completion(context, control(WORKER_DEADLINE_EXCEEDED), finish_reason, usage.as_ref(), seal, None));
                    },
                };
                let Some(event) = event else { break };
                let event = match event {
                    Ok(event) => event,
                    Err(error) => {
                        let seal = publication.fail();
                        return Err(with_model_completion(
                            context,
                            map_llm_error(error),
                            finish_reason,
                            usage.as_ref(),
                            seal,
                            None,
                        ));
                    }
                };
                if text
                    .len()
                    .saturating_add(streaming_tool_calls.argument_bytes())
                    .saturating_add(event.text_delta.len())
                    > max_text_bytes
                {
                    let seal = publication.fail();
                    return Err(with_model_completion(
                        context,
                        map_llm_error(model_response_too_large()),
                        finish_reason,
                        usage.as_ref(),
                        seal,
                        None,
                    ));
                }
                if !event.text_delta.is_empty() {
                    observe_text(
                        self.token_observer.as_ref(),
                        request,
                        context,
                        &event.text_delta,
                    );
                    if let Err(failure) = publication.text_delta(event.text_delta.clone()).await {
                        let seal = publication.fail();
                        return Err(with_model_completion(
                            context,
                            failure,
                            finish_reason,
                            usage.as_ref(),
                            seal,
                            None,
                        ));
                    }
                    text.push_str(&event.text_delta);
                }
                if let Err(failure) =
                    streaming_tool_calls.push(event.tool_call_deltas, text.len(), max_text_bytes)
                {
                    let seal = publication.fail();
                    return Err(with_model_completion(
                        context,
                        failure,
                        finish_reason,
                        usage.as_ref(),
                        seal,
                        None,
                    ));
                }
                if let Err(failure) = streaming_tool_calls
                    .publish_ready(&mut publication, &tool_contract)
                    .await
                {
                    let seal = publication.fail();
                    return Err(with_model_completion(
                        context,
                        failure,
                        finish_reason,
                        usage.as_ref(),
                        seal,
                        None,
                    ));
                }
                if let Some(reason) = event.finish_reason {
                    finish_reason = reason;
                }
                if event.usage.is_some() {
                    usage = event.usage;
                }
            }
        } else {
            let complete_future = model.chat(chat_request);
            tokio::pin!(complete_future);
            let response = tokio::select! {
                result = &mut complete_future => match result {
                    Ok(response) => response,
                    Err(error) => {
                        let seal = publication.fail();
                        return Err(with_model_completion(context, map_llm_error(error), ChatFinishReason::Invalid, None, seal, None));
                    }
                },
                _ = cancellation.cancelled() => {
                    let seal = publication.fail();
                    return Err(with_model_completion(context, control(WORKER_CANCELLED), ChatFinishReason::Invalid, None, seal, None));
                },
                _ = sleep(remaining(context)?) => {
                    let seal = publication.fail();
                    return Err(with_model_completion(context, control(WORKER_DEADLINE_EXCEEDED), ChatFinishReason::Invalid, None, seal, None));
                },
            };
            if response.text.len() > max_text_bytes {
                let seal = publication.fail();
                return Err(with_model_completion(
                    context,
                    map_llm_error(model_response_too_large()),
                    response.finish_reason,
                    response.usage.as_ref(),
                    seal,
                    None,
                ));
            }
            finish_reason = response.finish_reason;
            usage = response.usage;
            complete_tool_calls = response.tool_calls;
            if let Err(failure) = publication
                .publish_complete_function_calls(&complete_tool_calls, &tool_contract)
                .await
            {
                let seal = publication.fail();
                return Err(with_model_completion(
                    context,
                    failure,
                    finish_reason,
                    usage.as_ref(),
                    seal,
                    None,
                ));
            }
            if !response.text.is_empty() {
                observe_text(
                    self.token_observer.as_ref(),
                    request,
                    context,
                    &response.text,
                );
                if let Err(failure) = publication.text_delta(response.text.clone()).await {
                    let seal = publication.fail();
                    return Err(with_model_completion(
                        context,
                        failure,
                        finish_reason,
                        usage.as_ref(),
                        seal,
                        None,
                    ));
                }
                text = response.text;
            }
        }

        let raw_tool_calls = if stream_requested {
            match streaming_tool_calls.complete() {
                Ok(calls) => calls,
                Err(failure) => {
                    let seal = publication.fail();
                    return Err(with_model_completion(
                        context,
                        failure,
                        finish_reason,
                        usage.as_ref(),
                        seal,
                        None,
                    ));
                }
            }
        } else {
            complete_tool_calls
        };
        let normalized_tool_calls = if raw_tool_calls.is_empty() {
            Vec::new()
        } else {
            match normalize_model_tool_calls(
                raw_tool_calls,
                &tool_contract,
                max_text_bytes.saturating_sub(text.len()),
            ) {
                Ok(calls) => calls,
                Err(failure) => {
                    let seal = publication.fail();
                    return Err(with_model_completion(
                        context,
                        failure,
                        finish_reason,
                        usage.as_ref(),
                        seal,
                        None,
                    ));
                }
            }
        };

        if finish_reason == ChatFinishReason::ToolCalls {
            if normalized_tool_calls.is_empty() {
                let seal = publication.fail();
                return Err(with_model_completion(
                    context,
                    infrastructure(LLM_TOOL_CALL_INVALID, false),
                    finish_reason,
                    usage.as_ref(),
                    seal,
                    None,
                ));
            }
            let completed_calls = completed_tool_call_count(context);
            let limit_error = if context.continuation_turns().len()
                >= usize::try_from(tool_contract.max_rounds).unwrap_or(usize::MAX)
            {
                Some(LLM_TOOL_ROUND_LIMIT)
            } else if completed_calls.saturating_add(normalized_tool_calls.len())
                > usize::try_from(tool_contract.max_calls).unwrap_or(usize::MAX)
            {
                Some(LLM_TOOL_CALL_LIMIT)
            } else {
                None
            };
            if let Some(code) = limit_error {
                let seal = publication.fail();
                return Err(with_model_completion(
                    context,
                    infrastructure(code, false),
                    finish_reason,
                    usage.as_ref(),
                    seal,
                    None,
                ));
            }
            let seal = publication.finish_message_incomplete();
            let completion = model_completion(context, finish_reason, usage.as_ref(), seal, None)
                .ok_or_else(|| invariant(LLM_DESCRIPTOR_INVALID))?;
            let public_function_calls =
                publication.function_call_checkpoint_publications(&normalized_tool_calls)?;
            let batch = ModelToolCallBatch::new(
                completion.model_call_no(),
                (!text.is_empty()).then_some(text),
                normalized_tool_calls,
            )
            .and_then(|batch| batch.with_public_function_calls(public_function_calls))
            .map_err(|_| invariant(LLM_RESPONSE_INVALID))?;
            return Ok(
                TaskExecutionResult::new(BTreeMap::new(), EffectEvidence::Committed)
                    .with_model_call(completion)
                    .with_model_tool_call_batch(batch),
            );
        }

        let finish_error = match finish_reason {
            ChatFinishReason::Stop if normalized_tool_calls.is_empty() => None,
            ChatFinishReason::Length => Some(MODEL_OUTPUT_TRUNCATED),
            ChatFinishReason::ContentFilter => Some(MODEL_OUTPUT_FILTERED),
            ChatFinishReason::Invalid | ChatFinishReason::Stop | ChatFinishReason::ToolCalls => {
                Some(MODEL_FINISH_REASON_INVALID)
            }
        };
        if let Some(code) = finish_error {
            let seal = publication.fail();
            return Err(with_model_completion(
                context,
                infrastructure(code, false),
                finish_reason,
                usage.as_ref(),
                seal,
                None,
            ));
        }

        let value = if output.value_type().string_constraints().is_some() {
            Value::String(text.clone())
        } else {
            match serde_json::from_str(&text) {
                Ok(value) => value,
                Err(_) => {
                    let seal = publication.fail();
                    return Err(with_model_completion(
                        context,
                        infrastructure(LLM_RESPONSE_INVALID, false),
                        finish_reason,
                        usage.as_ref(),
                        seal,
                        None,
                    ));
                }
            }
        };
        let value = match RuntimeValue::new(value) {
            Ok(value) => value,
            Err(_) => {
                let seal = publication.fail();
                return Err(with_model_completion(
                    context,
                    invariant(LLM_RESPONSE_INVALID),
                    finish_reason,
                    usage.as_ref(),
                    seal,
                    None,
                ));
            }
        };
        if !value.matches(output.value_type()) {
            let seal = publication.fail();
            return Err(with_model_completion(
                context,
                infrastructure(LLM_RESPONSE_INVALID, false),
                finish_reason,
                usage.as_ref(),
                seal,
                None,
            ));
        }
        let (seal, safe_public_item) = match publication.complete(text).await {
            Ok(completion) => completion,
            Err(failure) => {
                let seal = publication.fail();
                return Err(with_model_completion(
                    context,
                    failure,
                    finish_reason,
                    usage.as_ref(),
                    seal,
                    None,
                ));
            }
        };
        let result = TaskExecutionResult::new(
            BTreeMap::from([(output.port_id().clone(), value)]),
            EffectEvidence::Committed,
        );
        if let Some(completion) = model_completion(
            context,
            finish_reason,
            usage.as_ref(),
            seal,
            safe_public_item,
        ) {
            Ok(result.with_model_call(completion))
        } else {
            Ok(result)
        }
    }
}

fn completed_tool_call_count(context: &WorkerExecutionContext) -> usize {
    context
        .continuation_turns()
        .iter()
        .map(|turn| turn.calls().len())
        .sum()
}

fn validate_completed_tool_limits(
    context: &WorkerExecutionContext,
    contract: &FrozenLlmToolContract,
) -> Result<(), WorkerFailure> {
    if context.continuation_turns().len()
        > usize::try_from(contract.max_rounds).unwrap_or(usize::MAX)
    {
        return Err(infrastructure(LLM_TOOL_ROUND_LIMIT, false));
    }
    if completed_tool_call_count(context)
        > usize::try_from(contract.max_calls).unwrap_or(usize::MAX)
    {
        return Err(infrastructure(LLM_TOOL_CALL_LIMIT, false));
    }
    Ok(())
}

fn append_model_continuation(
    messages: &mut Vec<ChatMessage>,
    context: &WorkerExecutionContext,
) -> Result<(), WorkerFailure> {
    for turn in context.continuation_turns() {
        let calls = turn
            .calls()
            .iter()
            .map(|call| {
                let arguments = serde_jcs::to_string(call.arguments())
                    .map_err(|_| invariant(LLM_TOOL_CONTINUATION_INVARIANT))?;
                Ok(ChatToolCall {
                    index: call.index(),
                    id: call.call_id().to_owned(),
                    name: call.name().to_owned(),
                    arguments,
                })
            })
            .collect::<Result<Vec<_>, WorkerFailure>>()?;
        messages.push(ChatMessage::assistant_tool_calls(
            turn.assistant_content()
                .map(|content| ChatContent::Text(content.to_owned())),
            calls,
        ));
        messages.extend(
            turn.tool_results().iter().map(|result| {
                ChatMessage::tool_result(result.call_id(), result.canonical_content())
            }),
        );
    }
    Ok(())
}

#[derive(Clone)]
struct LlmPublicationSeed {
    run_id: RunId,
    activation_id: ActivationId,
    attempt_no: AttemptNo,
    model_call_no: u32,
}

struct LlmPublication {
    broker: Option<Arc<dyn LiveResponseBroker>>,
    allocator: Option<ModelCallPublicItemAllocator>,
    seed: Option<LlmPublicationSeed>,
    reserved_item: Option<ResponseItemAuthority>,
    identity: Option<LiveResponseItemIdentity>,
    next_local_sequence: u64,
    function_calls: BTreeMap<u32, FunctionCallPublication>,
}

struct FunctionCallPublication {
    authority: ResponseItemAuthority,
    identity: LiveResponseItemIdentity,
    call_id: String,
    tool_name: String,
    next_local_sequence: u64,
}

#[derive(Default)]
struct FailedLlmPublication {
    message_seal_index: Option<u64>,
    function_calls: Vec<ModelIncompleteFunctionCallPublication>,
}

impl LlmPublication {
    fn start(
        broker: Option<&Arc<dyn LiveResponseBroker>>,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        publish: bool,
        services: &WorkerRuntimeServices,
    ) -> Result<Self, WorkerFailure> {
        let publication = Self {
            broker: None,
            allocator: None,
            seed: None,
            reserved_item: None,
            identity: None,
            next_local_sequence: 0,
            function_calls: BTreeMap::new(),
        };
        if !publish || broker.is_none() {
            return Ok(publication);
        }
        let authority = context
            .model_call()
            .ok_or_else(|| invariant(LLM_DESCRIPTOR_INVALID))?;
        if !authority.publication_enabled() {
            return Err(invariant(LLM_DESCRIPTOR_INVALID));
        }
        Ok(Self {
            broker: broker.cloned(),
            allocator: worker_adapter::services_model_call_public_item_allocator(services).cloned(),
            seed: Some(LlmPublicationSeed {
                run_id: request.run_id().clone(),
                activation_id: request.activation_id().clone(),
                attempt_no: context.attempt_no(),
                model_call_no: authority.model_call_no(),
            }),
            reserved_item: authority.public_item().cloned(),
            identity: None,
            next_local_sequence: 0,
            function_calls: BTreeMap::new(),
        })
    }

    async fn ensure_function_started(
        &mut self,
        call_index: u32,
        call_id: &str,
        tool_name: &str,
    ) -> Result<(), WorkerFailure> {
        if let Some(existing) = self.function_calls.get(&call_index) {
            return if existing.call_id == call_id && existing.tool_name == tool_name {
                Ok(())
            } else {
                Err(infrastructure(LLM_TOOL_CALL_INVALID, false))
            };
        }
        let (Some(seed), Some(broker), Some(allocator)) = (
            self.seed.clone(),
            self.broker.clone(),
            self.allocator.clone(),
        ) else {
            return Ok(());
        };
        let authority = worker_adapter::reserve_public_function_call(
            &allocator, call_index, call_id, tool_name,
        )
        .await
        .map_err(public_item_reservation_failure)?;
        let identity = LiveResponseItemIdentity::new(
            seed.run_id,
            seed.activation_id,
            seed.attempt_no,
            seed.model_call_no,
            authority.item_id(),
            authority.output_index(),
        )
        .map_err(|_| invariant(LLM_DESCRIPTOR_INVALID))?;
        let added = ResponseOutputItem::FunctionCall {
            id: authority.item_id().to_owned(),
            status: ResponseItemStatus::InProgress,
            call_id: call_id.to_owned(),
            name: tool_name.to_owned(),
            arguments: String::new(),
        };
        if let Ok(frame) = LiveResponsePublication::new(
            identity.clone(),
            0,
            LiveResponsePayload::OutputItemAdded { item: added },
        ) {
            let _ = broker.publish(frame);
        }
        self.function_calls.insert(
            call_index,
            FunctionCallPublication {
                authority,
                identity,
                call_id: call_id.to_owned(),
                tool_name: tool_name.to_owned(),
                next_local_sequence: 1,
            },
        );
        Ok(())
    }

    fn function_argument_delta(
        &mut self,
        call_index: u32,
        delta: String,
    ) -> Result<(), WorkerFailure> {
        let call = self
            .function_calls
            .get_mut(&call_index)
            .ok_or_else(|| invariant(LLM_DESCRIPTOR_INVALID))?;
        if let Some(broker) = &self.broker {
            if let Ok(frame) = LiveResponsePublication::new(
                call.identity.clone(),
                call.next_local_sequence,
                LiveResponsePayload::FunctionCallArgumentsDelta { delta },
            ) {
                call.next_local_sequence = call.next_local_sequence.saturating_add(1);
                let _ = broker.publish(frame);
            }
        }
        Ok(())
    }

    async fn publish_complete_function_calls(
        &mut self,
        calls: &[ChatToolCall],
        contract: &FrozenLlmToolContract,
    ) -> Result<(), WorkerFailure> {
        for call in calls {
            let Some(tool) = contract
                .tools
                .iter()
                .find(|tool| tool.definition.name == call.name)
            else {
                continue;
            };
            if !tool.raw_argument_deltas_authorized {
                continue;
            }
            self.ensure_function_started(call.index, &call.id, &call.name)
                .await?;
            self.function_argument_delta(call.index, call.arguments.clone())?;
        }
        Ok(())
    }

    fn function_call_checkpoint_publications(
        &self,
        calls: &[ModelToolCall],
    ) -> Result<Vec<ModelFunctionCallPublication>, WorkerFailure> {
        self.function_calls
            .iter()
            .map(|(call_index, publication)| {
                let call = calls
                    .get(usize::try_from(*call_index).unwrap_or(usize::MAX))
                    .filter(|call| {
                        call.index() == *call_index
                            && call.call_id() == publication.call_id
                            && call.name() == publication.tool_name
                    })
                    .ok_or_else(|| invariant(LLM_DESCRIPTOR_INVALID))?;
                let _ = call;
                let argument_delta_count = publication
                    .next_local_sequence
                    .checked_sub(1)
                    .ok_or_else(|| invariant(LLM_DESCRIPTOR_INVALID))?;
                ModelFunctionCallPublication::new(
                    *call_index,
                    publication.authority.clone(),
                    argument_delta_count,
                )
                .map_err(|_| invariant(LLM_DESCRIPTOR_INVALID))
            })
            .collect()
    }

    async fn ensure_started(&mut self) -> Result<(), WorkerFailure> {
        if self.identity.is_some() || self.seed.is_none() {
            return Ok(());
        }
        let item = match self.reserved_item.take() {
            Some(item) => item,
            None => worker_adapter::reserve_public_item(
                self.allocator
                    .as_ref()
                    .ok_or_else(|| invariant(LLM_DESCRIPTOR_INVALID))?,
            )
            .await
            .map_err(public_item_reservation_failure)?,
        };
        let seed = self
            .seed
            .as_ref()
            .ok_or_else(|| invariant(LLM_DESCRIPTOR_INVALID))?;
        let identity = LiveResponseItemIdentity::new(
            seed.run_id.clone(),
            seed.activation_id.clone(),
            seed.attempt_no,
            seed.model_call_no,
            item.item_id(),
            item.output_index(),
        )
        .map_err(|_| invariant(LLM_DESCRIPTOR_INVALID))?;
        self.identity = Some(identity);
        self.emit(LiveResponsePayload::OutputItemAdded {
            item: message_item(item.item_id(), ResponseItemStatus::InProgress, None),
        });
        self.emit(LiveResponsePayload::ContentPartAdded {
            content_index: 0,
            part: output_text_part(String::new()),
        });
        Ok(())
    }

    async fn text_delta(&mut self, delta: String) -> Result<(), WorkerFailure> {
        if delta.is_empty() {
            return Ok(());
        }
        self.ensure_started().await?;
        self.emit(LiveResponsePayload::OutputTextDelta {
            content_index: 0,
            delta,
        });
        Ok(())
    }

    async fn complete(
        &mut self,
        text: String,
    ) -> Result<(Option<u64>, Option<Value>), WorkerFailure> {
        if !text.is_empty() {
            self.ensure_started().await?;
        }
        let Some(identity) = self.identity.clone() else {
            return Ok((None, None));
        };
        let item = message_item(
            identity.item_id(),
            ResponseItemStatus::Completed,
            Some(text.clone()),
        );
        self.emit(LiveResponsePayload::OutputTextDone {
            content_index: 0,
            text: text.clone(),
        });
        self.emit(LiveResponsePayload::ContentPartDone {
            content_index: 0,
            part: output_text_part(text),
        });
        self.emit(LiveResponsePayload::OutputItemDone { item: item.clone() });
        let last = self.next_local_sequence.checked_sub(1);
        if let Some(broker) = &self.broker {
            let _ = broker.seal(LiveResponseSeal::new(
                identity,
                last,
                LiveResponseSealStatus::Completed,
            ));
        }
        Ok((last, serde_json::to_value(item).ok()))
    }

    fn finish_message_incomplete(&mut self) -> Option<u64> {
        let identity = self.identity.clone()?;
        self.emit(LiveResponsePayload::OutputItemDone {
            item: message_item(identity.item_id(), ResponseItemStatus::Incomplete, None),
        });
        let last = self.next_local_sequence.checked_sub(1);
        if let Some(broker) = &self.broker {
            let _ = broker.seal(LiveResponseSeal::new(
                identity,
                last,
                LiveResponseSealStatus::Incomplete,
            ));
        }
        last
    }

    fn fail(&mut self) -> FailedLlmPublication {
        let message_seal_index = self.finish_message_incomplete();
        let mut function_calls = Vec::with_capacity(self.function_calls.len());
        let Some(broker) = self.broker.clone() else {
            return FailedLlmPublication {
                message_seal_index,
                function_calls,
            };
        };
        for (call_index, call) in &mut self.function_calls {
            let seal_index = call.next_local_sequence;
            let item = ResponseOutputItem::FunctionCall {
                id: call.authority.item_id().to_owned(),
                status: ResponseItemStatus::Incomplete,
                call_id: call.call_id.clone(),
                name: call.tool_name.clone(),
                // Provider fragments remain provisional. A failed durable
                // item retains only stable call metadata, never partial JSON.
                arguments: String::new(),
            };
            let Ok(frame) = LiveResponsePublication::new(
                call.identity.clone(),
                seal_index,
                LiveResponsePayload::OutputItemDone { item },
            ) else {
                continue;
            };
            call.next_local_sequence = call.next_local_sequence.saturating_add(1);
            let _ = broker.publish(frame);
            let _ = broker.seal(LiveResponseSeal::new(
                call.identity.clone(),
                Some(seal_index),
                LiveResponseSealStatus::Incomplete,
            ));
            if let Ok(publication) = ModelIncompleteFunctionCallPublication::new(
                *call_index,
                call.authority.clone(),
                call.call_id.clone(),
                call.tool_name.clone(),
                seal_index,
            ) {
                function_calls.push(publication);
            }
        }
        FailedLlmPublication {
            message_seal_index,
            function_calls,
        }
    }

    fn emit(&mut self, payload: LiveResponsePayload) {
        let (Some(broker), Some(identity)) = (&self.broker, &self.identity) else {
            return;
        };
        if let Ok(publication) =
            LiveResponsePublication::new(identity.clone(), self.next_local_sequence, payload)
        {
            self.next_local_sequence = self.next_local_sequence.saturating_add(1);
            // Live publication is observational. Queue loss, subscriber loss,
            // and broker loss must never change the validated worker result.
            let _ = broker.publish(publication);
        }
    }
}

fn public_item_reservation_failure(error: ModelCallPublicItemReservationError) -> WorkerFailure {
    match error {
        ModelCallPublicItemReservationError::OperationDeadlineElapsed => {
            control(WORKER_DEADLINE_EXCEEDED)
        }
        ModelCallPublicItemReservationError::StaleLease
        | ModelCallPublicItemReservationError::AuthorityUnavailable => {
            infrastructure(LLM_PUBLICATION_AUTHORITY_LOST, false)
        }
        ModelCallPublicItemReservationError::StateConflict => invariant(LLM_DESCRIPTOR_INVALID),
    }
}

fn message_item(
    item_id: impl Into<String>,
    status: ResponseItemStatus,
    text: Option<String>,
) -> ResponseOutputItem {
    ResponseOutputItem::Message {
        id: item_id.into(),
        status,
        role: ResponseRole::Assistant,
        content: text.map(output_text_part).into_iter().collect::<Vec<_>>(),
    }
}

fn output_text_part(text: String) -> ResponseContentPart {
    ResponseContentPart::OutputText {
        text,
        annotations: Vec::new(),
    }
}

fn observe_text(
    observer: Option<&broadcast::Sender<LlmTokenObservation>>,
    request: &TaskExecutionRequest,
    context: &WorkerExecutionContext,
    text: &str,
) {
    if let Some(observer) = observer {
        let _ = observer.send(LlmTokenObservation::new(request, context, text.to_owned()));
    }
}

fn normalize_usage(usage: &ChatUsage) -> ModelTokenUsage {
    ModelTokenUsage {
        input_tokens: usage.input_tokens,
        cached_tokens: usage
            .input_tokens_details
            .as_ref()
            .and_then(|details| details.cached_tokens),
        output_tokens: usage.output_tokens,
        reasoning_tokens: usage
            .output_tokens_details
            .as_ref()
            .and_then(|details| details.reasoning_tokens),
        total_tokens: usage.total_tokens,
    }
}

fn normalize_finish_reason(reason: ChatFinishReason) -> ModelFinishReason {
    match reason {
        ChatFinishReason::Stop => ModelFinishReason::Stop,
        ChatFinishReason::ToolCalls => ModelFinishReason::ToolCalls,
        ChatFinishReason::Length => ModelFinishReason::Length,
        ChatFinishReason::ContentFilter => ModelFinishReason::ContentFilter,
        ChatFinishReason::Invalid => ModelFinishReason::Invalid,
    }
}

fn model_completion(
    context: &WorkerExecutionContext,
    finish_reason: ChatFinishReason,
    usage: Option<&ChatUsage>,
    public_item_seal_index: Option<u64>,
    safe_public_item: Option<Value>,
) -> Option<ModelCallCompletion> {
    let authority = context.model_call()?;
    ModelCallCompletion::new(
        authority.model_call_no(),
        normalize_finish_reason(finish_reason),
        usage.map(normalize_usage),
        public_item_seal_index,
        safe_public_item,
    )
    .ok()
}

fn with_model_completion(
    context: &WorkerExecutionContext,
    failure: WorkerFailure,
    finish_reason: ChatFinishReason,
    usage: Option<&ChatUsage>,
    failed_publication: FailedLlmPublication,
    safe_public_item: Option<Value>,
) -> WorkerFailure {
    // A Provider `tool_calls` finish is successful only together with a
    // validated, checkpointable tool batch. Error paths retain usage but must
    // never manufacture that success contract without the batch.
    let finish_reason = if finish_reason == ChatFinishReason::ToolCalls {
        ChatFinishReason::Invalid
    } else {
        finish_reason
    };
    model_completion(
        context,
        finish_reason,
        usage,
        failed_publication.message_seal_index,
        safe_public_item,
    )
    .and_then(|completion| {
        completion
            .with_incomplete_function_calls(failed_publication.function_calls)
            .ok()
    })
    .map_or(failure.clone(), |completion| {
        failure.with_model_call(completion)
    })
}

#[derive(Clone)]
pub struct ActionTaskExecutor {
    actions: ActionRegistry,
}

impl ActionTaskExecutor {
    pub fn new(actions: ActionRegistry) -> Self {
        Self { actions }
    }
}

#[async_trait]
impl LeafTaskExecutor for ActionTaskExecutor {
    async fn execute(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        let services = WorkerRuntimeServices::default();
        self.execute_with_runtime_services(context, request, &services, cancellation)
            .await
    }

    async fn execute_with_runtime_services(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        services: &WorkerRuntimeServices,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        if request.task_kind() != SchedulerTaskKind::Action {
            return Err(invariant(ACTION_DESCRIPTOR_INVALID));
        }
        let action = self
            .actions
            .resolve(request.implementation())
            .map_err(|_| invariant(ACTION_DESCRIPTOR_INVALID))?;
        validate_frozen_action_binding(action.as_ref(), request)?;
        let descriptor = action.descriptor();
        validate_frozen_action_policy(descriptor, request.effect_policy())?;
        let model_tool_request = worker_adapter::is_model_tool_action_request(request);
        if matches!(request.origin(), TaskExecutionOrigin::ModelTool { .. }) && !model_tool_request
        {
            return Err(invariant(ACTION_BINDING_INVALID));
        }
        let bindings = RuntimeBindings::new(request.public_configuration(), request)?;
        let input = request
            .public_configuration()
            .get("inputs")
            .map(|value| {
                if model_tool_request {
                    descriptor_json(value)
                } else {
                    substitute_descriptor_value(value, &bindings)
                }
            })
            .transpose()?
            .unwrap_or_else(|| Value::Object(Map::new()));
        if !input.is_object() {
            return Err(invariant(ACTION_BINDING_INVALID));
        }
        require_live(context, &cancellation)?;

        let timeout = remaining(context)?;
        let (stop, signal) = stop_pair();
        let control = ExecutionControl::new(signal, timeout);
        let cancellation_for_action = cancellation.clone();
        let stop_for_action = stop.clone();
        let cancellation_bridge = tokio::spawn(async move {
            cancellation_for_action.cancelled().await;
            stop_for_action.request(StopReason::Cancelled);
        });
        let mut action_context = ActionContext::for_durable_effect(
            request.run_id().as_str(),
            request.node_id().as_str(),
            context.attempt_no().get(),
            request.effect_id().as_str(),
            control,
        );
        if model_tool_request {
            if let Some(publisher) =
                worker_adapter::services_model_tool_progress_publisher(services)
            {
                action_context =
                    action_context.with_model_tool_progress_publisher(publisher.clone());
            }
        }
        let call = async {
            if model_tool_request {
                action.call_model_tool(input, action_context).await
            } else {
                action.call(input, action_context).await
            }
        };
        tokio::pin!(call);
        let result = tokio::select! {
            value = &mut call => value.map_err(|error| map_action_error(error, request.effect_policy())),
            _ = cancellation.cancelled() => {
                stop.request(StopReason::Cancelled);
                Err(cancelled_action_failure(request.effect_policy()))
            },
            _ = sleep(timeout) => {
                stop.request(StopReason::TimedOut);
                Err(cancelled_action_failure(request.effect_policy()))
            },
        };
        cancellation_bridge.abort();
        let value = RuntimeValue::new(result?).map_err(|_| invariant(ACTION_EXECUTION_FAILED))?;
        let output = only_output(request)?;
        if !value.matches(output.value_type()) {
            return Err(invariant(ACTION_EXECUTION_FAILED));
        }
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(output.port_id().clone(), value)]),
            EffectEvidence::Committed,
        ))
    }
}

/// Registers all exact adapter versions available at deployment publication.
pub fn production_worker_registry(
    models: &ModelRegistry,
    actions: &ActionRegistry,
) -> Result<WorkerExecutorRegistry, CompileError> {
    production_worker_registry_inner(models, actions, None, None)
}

pub fn production_worker_registry_with_live_response(
    models: &ModelRegistry,
    actions: &ActionRegistry,
    live_response_broker: Arc<dyn LiveResponseBroker>,
) -> Result<WorkerExecutorRegistry, CompileError> {
    production_worker_registry_inner(models, actions, None, Some(live_response_broker))
}

/// Registers built-in model/Action workers plus every exact versioned HTTP or
/// Tool adapter used by the production deployment resolver.
pub fn production_worker_registry_with_leaf_adapters(
    models: &ModelRegistry,
    actions: &ActionRegistry,
    external_leaf_adapters: &VersionedLeafAdapterRegistry,
) -> Result<WorkerExecutorRegistry, CompileError> {
    production_worker_registry_inner(models, actions, Some(external_leaf_adapters), None)
}

fn production_worker_registry_inner(
    models: &ModelRegistry,
    actions: &ActionRegistry,
    external_leaf_adapters: Option<&VersionedLeafAdapterRegistry>,
    live_response_broker: Option<Arc<dyn LiveResponseBroker>>,
) -> Result<WorkerExecutorRegistry, CompileError> {
    let llm_descriptor_version = VersionTag::new(LLM_DESCRIPTOR_VERSION)
        .map_err(|error| CompileError::new("WORKER_REGISTRY_INVALID", error.to_string()))?;
    let leaf_descriptor_version = VersionTag::new(LEAF_DESCRIPTOR_VERSION)
        .map_err(|error| CompileError::new("WORKER_REGISTRY_INVALID", error.to_string()))?;
    let mut registry = WorkerExecutorRegistry::new();
    let mut llm_versions = BTreeSet::new();
    for alias in models.names() {
        let identity = models.deployment_identity(alias)?;
        if llm_versions.insert(identity.worker_version().to_owned()) {
            let executor = LlmTaskExecutor::new(models.clone());
            let executor = match &live_response_broker {
                Some(broker) => executor.with_live_response_broker(Arc::clone(broker)),
                None => executor,
            };
            registry
                .register(
                    SchedulerTaskKind::Llm,
                    "core.llm",
                    llm_descriptor_version.clone(),
                    VersionTag::new(identity.worker_version()).map_err(|error| {
                        CompileError::new("WORKER_REGISTRY_INVALID", error.to_string())
                    })?,
                    Arc::new(executor),
                )
                .map_err(|code| CompileError::new(code, "failed to register LLM worker"))?;
        }
    }
    for action_id in actions.names() {
        let action = actions.resolve(action_id)?;
        registry
            .register(
                SchedulerTaskKind::Action,
                action_id,
                leaf_descriptor_version.clone(),
                VersionTag::new(action.identity().version.to_string()).map_err(|error| {
                    CompileError::new("WORKER_REGISTRY_INVALID", error.to_string())
                })?,
                Arc::new(ActionTaskExecutor::new(actions.clone())),
            )
            .map_err(|code| CompileError::new(code, "failed to register Action worker"))?;
    }
    if let Some(external_leaf_adapters) = external_leaf_adapters {
        external_leaf_adapters.install_workers(&mut registry)?;
    }
    Ok(registry)
}

struct RuntimeBindings {
    by_reference: BTreeMap<String, Value>,
    optional_references: BTreeSet<String>,
    template_root: Value,
}

impl RuntimeBindings {
    fn new(
        configuration: &BTreeMap<String, DescriptorValue>,
        request: &TaskExecutionRequest,
    ) -> Result<Self, WorkerFailure> {
        let ports = request
            .inputs()
            .iter()
            .map(|input| (input.port_id().as_str(), input.value().value().clone()))
            .collect::<BTreeMap<_, _>>();
        let mappings = match configuration.get("runtime_bindings") {
            None => None,
            Some(DescriptorValue::Object(values)) => Some(values),
            Some(_) => return Err(invariant(LLM_BINDING_INVALID)),
        };
        let mut optional_references = BTreeSet::new();
        match configuration.get("optional_runtime_bindings") {
            None => {}
            Some(DescriptorValue::Array(values)) => {
                for value in values {
                    let reference = descriptor_string(value)?;
                    if !optional_references.insert(reference.to_owned()) {
                        return Err(invariant(LLM_BINDING_INVALID));
                    }
                }
            }
            Some(_) => return Err(invariant(LLM_BINDING_INVALID)),
        }
        if optional_references
            .iter()
            .any(|reference| mappings.is_none_or(|mappings| !mappings.contains_key(reference)))
        {
            return Err(invariant(LLM_BINDING_INVALID));
        }
        let mut by_reference = BTreeMap::new();
        let mut roots = Map::new();
        for (reference, port) in mappings.into_iter().flatten() {
            let port = descriptor_string(port)?;
            let Some(value) = ports.get(port).cloned() else {
                // Scheduler omission is the only representation of an absent
                // optional binding. A missing required binding fails before
                // the message program is interpreted.
                if optional_references.contains(reference) {
                    continue;
                }
                return Err(invariant(LLM_BINDING_INVALID));
            };
            by_reference.insert(reference.clone(), value.clone());
            if !reference.contains('.') {
                roots.insert(reference.clone(), value);
            }
        }
        Ok(Self {
            by_reference,
            optional_references,
            template_root: Value::Object(roots),
        })
    }

    fn resolve(&self, reference: &str) -> Result<Value, WorkerFailure> {
        self.resolve_optional(reference)?
            .ok_or_else(|| invariant(LLM_BINDING_INVALID))
    }

    fn resolve_optional(&self, reference: &str) -> Result<Option<Value>, WorkerFailure> {
        if let Some(value) = self.by_reference.get(reference) {
            return Ok(Some(value.clone()));
        }
        let mut parts = reference.split('.');
        let root = parts.next().ok_or_else(|| invariant(LLM_BINDING_INVALID))?;
        let Some(mut value) = self.by_reference.get(root) else {
            return if self.optional_references.contains(reference) {
                Ok(None)
            } else {
                Err(invariant(LLM_BINDING_INVALID))
            };
        };
        for field in parts {
            value = value
                .as_object()
                .and_then(|object| object.get(field))
                .ok_or_else(|| invariant(LLM_BINDING_INVALID))?;
        }
        Ok(Some(value.clone()))
    }
}

fn render_messages(
    configuration: &BTreeMap<String, DescriptorValue>,
    bindings: &RuntimeBindings,
) -> Result<Vec<ChatMessage>, WorkerFailure> {
    let program = match required(configuration, "message_program")? {
        DescriptorValue::Array(program) => program,
        _ => return Err(invariant(LLM_DESCRIPTOR_INVALID)),
    };
    let prompts = match configuration.get("prompt_catalog") {
        None => None,
        Some(DescriptorValue::Object(prompts)) => Some(prompts),
        Some(_) => return Err(invariant(LLM_DESCRIPTOR_INVALID)),
    };
    let mut messages = Vec::new();
    for instruction in program {
        let instruction = descriptor_object(instruction)?;
        match descriptor_string(required(instruction, "kind")?)? {
            "message_splice" => {
                let path = descriptor_string(required(instruction, "path")?)?;
                let Value::Array(dynamic) = bindings.resolve(path)? else {
                    return Err(invariant(LLM_MESSAGE_INVALID));
                };
                for message in dynamic {
                    messages.push(dynamic_message(message)?);
                }
            }
            "message" => {
                let role = match descriptor_string(required(instruction, "role")?)? {
                    "system" => ChatRole::System,
                    "user" => ChatRole::User,
                    "assistant" => ChatRole::Assistant,
                    _ => return Err(invariant(LLM_MESSAGE_INVALID)),
                };
                let content = match required(instruction, "content")? {
                    DescriptorValue::Array(content) => content,
                    _ => return Err(invariant(LLM_MESSAGE_INVALID)),
                };
                let mut parts = Vec::with_capacity(content.len());
                for part in content {
                    let part = descriptor_object(part)?;
                    let kind = descriptor_string(required(part, "kind")?)?;
                    if let Some(text) = part.get("text") {
                        let source = descriptor_string(text)?;
                        let text = match kind {
                            "literal" => source.to_owned(),
                            "value_ref" => bindings
                                .resolve(source)?
                                .as_str()
                                .map(str::to_owned)
                                .ok_or_else(|| invariant(LLM_BINDING_INVALID))?,
                            "template" => render_template(source, bindings)?,
                            "prompt_ref" => {
                                let prompt = prompts
                                    .and_then(|catalog| catalog.get(source))
                                    .ok_or_else(|| invariant(LLM_DESCRIPTOR_INVALID))?;
                                let prompt = descriptor_object(prompt)?;
                                render_template(
                                    descriptor_string(required(prompt, "content")?)?,
                                    bindings,
                                )?
                            }
                            _ => return Err(invariant(LLM_MESSAGE_INVALID)),
                        };
                        parts.push(ChatContentPart::Text { text });
                    } else if let Some(image) = part.get("image_url") {
                        let source = descriptor_string(image)?;
                        let image = match kind {
                            "literal" => source.to_owned(),
                            "value_ref" => {
                                let Some(value) = bindings.resolve_optional(source)? else {
                                    continue;
                                };
                                value
                                    .as_str()
                                    .map(str::to_owned)
                                    .ok_or_else(|| invariant(LLM_BINDING_INVALID))?
                            }
                            _ => return Err(invariant(LLM_MESSAGE_INVALID)),
                        };
                        parts.push(ChatContentPart::Image { image });
                    } else {
                        return Err(invariant(LLM_MESSAGE_INVALID));
                    }
                }
                messages.push(ChatMessage::from_content(role, ChatContent::Parts(parts)));
            }
            _ => return Err(invariant(LLM_DESCRIPTOR_INVALID)),
        }
    }
    if messages.is_empty() {
        return Err(invariant(LLM_MESSAGE_INVALID));
    }
    Ok(messages)
}

fn render_template(source: &str, bindings: &RuntimeBindings) -> Result<String, WorkerFailure> {
    compile_template(source)
        .map_err(|_| invariant(LLM_DESCRIPTOR_INVALID))?
        .render_bounded(&bindings.template_root, MAX_TEMPLATE_BYTES)
        .map_err(|_| infrastructure(LLM_REQUEST_TOO_LARGE, false))
}

fn dynamic_message(value: Value) -> Result<ChatMessage, WorkerFailure> {
    let Value::Object(mut value) = value else {
        return Err(invariant(LLM_MESSAGE_INVALID));
    };
    if value.len() != 2 {
        return Err(invariant(LLM_MESSAGE_INVALID));
    }
    let role = match value
        .remove("role")
        .and_then(|value| value.as_str().map(str::to_owned))
    {
        Some(role) if role == "user" => ChatRole::User,
        Some(role) if role == "assistant" => ChatRole::Assistant,
        _ => return Err(invariant(LLM_MESSAGE_INVALID)),
    };
    let Value::Array(content) = value
        .remove("content")
        .ok_or_else(|| invariant(LLM_MESSAGE_INVALID))?
    else {
        return Err(invariant(LLM_MESSAGE_INVALID));
    };
    let mut parts = Vec::with_capacity(content.len());
    for part in content {
        let Value::Object(part) = part else {
            return Err(invariant(LLM_MESSAGE_INVALID));
        };
        if part.len() != 1 {
            return Err(invariant(LLM_MESSAGE_INVALID));
        }
        if let Some(Value::String(text)) = part.get("text") {
            parts.push(ChatContentPart::Text { text: text.clone() });
        } else if role == ChatRole::User {
            let Some(Value::String(image)) = part.get("image_url") else {
                return Err(invariant(LLM_MESSAGE_INVALID));
            };
            parts.push(ChatContentPart::Image {
                image: image.clone(),
            });
        } else {
            return Err(invariant(LLM_MESSAGE_INVALID));
        }
    }
    Ok(ChatMessage::from_content(role, ChatContent::Parts(parts)))
}

fn substitute_descriptor_value(
    value: &DescriptorValue,
    bindings: &RuntimeBindings,
) -> Result<Value, WorkerFailure> {
    match value {
        DescriptorValue::String(value) if value.starts_with('$') => bindings.resolve(&value[1..]),
        DescriptorValue::Array(values) => values
            .iter()
            .map(|value| substitute_descriptor_value(value, bindings))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        DescriptorValue::Object(values) => values
            .iter()
            .map(|(name, value)| Ok((name.clone(), substitute_descriptor_value(value, bindings)?)))
            .collect::<Result<Map<_, _>, WorkerFailure>>()
            .map(Value::Object),
        value => descriptor_json(value),
    }
}

fn only_output(request: &TaskExecutionRequest) -> Result<&TaskOutputContract, WorkerFailure> {
    if request.outputs().len() != 1 || request.outputs()[0].name().as_str() != "result" {
        return Err(invariant(LLM_DESCRIPTOR_INVALID));
    }
    Ok(&request.outputs()[0])
}

fn required<'a>(
    values: &'a BTreeMap<String, DescriptorValue>,
    name: &str,
) -> Result<&'a DescriptorValue, WorkerFailure> {
    values
        .get(name)
        .ok_or_else(|| invariant(LLM_DESCRIPTOR_INVALID))
}

fn descriptor_object(
    value: &DescriptorValue,
) -> Result<&BTreeMap<String, DescriptorValue>, WorkerFailure> {
    match value {
        DescriptorValue::Object(value) => Ok(value),
        _ => Err(invariant(LLM_DESCRIPTOR_INVALID)),
    }
}

fn descriptor_string(value: &DescriptorValue) -> Result<&str, WorkerFailure> {
    match value {
        DescriptorValue::String(value) => Ok(value),
        _ => Err(invariant(LLM_DESCRIPTOR_INVALID)),
    }
}

fn descriptor_bool(value: &DescriptorValue) -> Result<bool, WorkerFailure> {
    match value {
        DescriptorValue::Boolean(value) => Ok(*value),
        _ => Err(invariant(LLM_DESCRIPTOR_INVALID)),
    }
}

fn descriptor_array(value: &DescriptorValue) -> Result<&[DescriptorValue], WorkerFailure> {
    match value {
        DescriptorValue::Array(value) => Ok(value),
        _ => Err(invariant(LLM_DESCRIPTOR_INVALID)),
    }
}

fn descriptor_json(value: &DescriptorValue) -> Result<Value, WorkerFailure> {
    Ok(match value {
        DescriptorValue::Null => Value::Null,
        DescriptorValue::Boolean(value) => Value::Bool(*value),
        DescriptorValue::Integer(value) => Value::Number((*value).into()),
        DescriptorValue::Number(value) => Value::Number(value.clone()),
        DescriptorValue::String(value) => Value::String(value.clone()),
        DescriptorValue::Array(values) => Value::Array(
            values
                .iter()
                .map(descriptor_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        DescriptorValue::Object(values) => Value::Object(
            values
                .iter()
                .map(|(name, value)| Ok((name.clone(), descriptor_json(value)?)))
                .collect::<Result<Map<_, _>, WorkerFailure>>()?,
        ),
    })
}

fn remaining(context: &WorkerExecutionContext) -> Result<Duration, WorkerFailure> {
    (context.deadline() - chrono::Utc::now())
        .to_std()
        .ok()
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| control(WORKER_DEADLINE_EXCEEDED))
}

fn require_live(
    context: &WorkerExecutionContext,
    cancellation: &CancellationToken,
) -> Result<(), WorkerFailure> {
    if cancellation.is_cancelled() {
        return Err(control(WORKER_CANCELLED));
    }
    remaining(context).map(|_| ())
}

fn map_llm_error(error: RunError) -> WorkerFailure {
    let provider_error_kind = match error.kind() {
        RunErrorKind::Operation => "operation",
        RunErrorKind::Timeout => "timeout",
        RunErrorKind::Stop => "stop",
        RunErrorKind::Infrastructure => "infrastructure",
    };
    let (public_error_code, retryable) = llm_provider_public_failure(error.code());
    tracing::warn!(
        event_name = "llm.provider_failure",
        provider_error_code = error.code(),
        provider_error_kind,
        public_error_code,
        retryable,
        "LLM provider operation failed"
    );
    match error.kind() {
        RunErrorKind::Stop => control(WORKER_CANCELLED),
        RunErrorKind::Timeout => control(WORKER_DEADLINE_EXCEEDED),
        RunErrorKind::Operation | RunErrorKind::Infrastructure => {
            infrastructure(public_error_code, retryable)
        }
    }
}

fn llm_provider_public_failure(provider_error_code: &str) -> (&'static str, bool) {
    match provider_error_code {
        "UPSTREAM_AUTHENTICATION" => (LLM_PROVIDER_AUTHENTICATION_FAILED, false),
        "UPSTREAM_PERMISSION" => (LLM_PROVIDER_PERMISSION_DENIED, false),
        "UPSTREAM_CONNECTION" => (LLM_PROVIDER_CONNECTION_FAILED, true),
        "UPSTREAM_TIMEOUT" => (LLM_PROVIDER_REQUEST_TIMEOUT, true),
        "UPSTREAM_REQUEST" | "UPSTREAM_REQUEST_REJECTED" => (LLM_PROVIDER_REQUEST_REJECTED, false),
        "UPSTREAM_RATE_LIMIT" => (LLM_PROVIDER_RATE_LIMITED, true),
        "UPSTREAM_UNAVAILABLE" => (LLM_PROVIDER_UNAVAILABLE, true),
        "UPSTREAM_STREAM" | "UPSTREAM_STREAM_INCOMPLETE" => (LLM_PROVIDER_STREAM_FAILED, true),
        "UPSTREAM_STREAM_INVALID" | "UPSTREAM_RESPONSE_INVALID" => {
            (LLM_PROVIDER_RESPONSE_INVALID, true)
        }
        "MODEL_RESPONSE_TOO_LARGE" => (LLM_PROVIDER_RESPONSE_TOO_LARGE, false),
        "UPSTREAM_TRANSPORT" | "UPSTREAM_RESPONSE" => (LLM_PROVIDER_CONNECTION_FAILED, true),
        _ => (LLM_PROVIDER_FAILED, true),
    }
}

fn map_action_error(error: RunError, policy: &WorkerEffectPolicy) -> WorkerFailure {
    match error.kind() {
        RunErrorKind::Stop | RunErrorKind::Timeout => cancelled_action_failure(policy),
        RunErrorKind::Operation | RunErrorKind::Infrastructure => {
            // Retry safety is decided later from the frozen policy plus the
            // durable effect evidence, never from this mutable adapter lookup.
            infrastructure(ACTION_EXECUTION_FAILED, true)
        }
    }
}

fn cancelled_action_failure(policy: &WorkerEffectPolicy) -> WorkerFailure {
    if policy.effect_class() == WorkerEffectClass::Mutating {
        WorkerFailure::new(
            WorkerFailureClass::EffectOutcomeUnknown,
            "ACTION_EFFECT_OUTCOME_UNKNOWN",
            true,
        )
        .expect("constant failure is valid")
    } else {
        control(WORKER_CANCELLED)
    }
}

fn validate_frozen_action_policy(
    descriptor: &insight_resources::actions::ActionDescriptor,
    policy: &WorkerEffectPolicy,
) -> Result<(), WorkerFailure> {
    let effect_class = match descriptor.effect {
        EffectClass::Pure => WorkerEffectClass::Pure,
        EffectClass::ReadOnly => WorkerEffectClass::ReadOnly,
        EffectClass::Mutating => WorkerEffectClass::Mutating,
    };
    let idempotency = match descriptor.idempotency {
        IdempotencyClass::Idempotent => EffectIdempotency::Idempotent,
        IdempotencyClass::NonIdempotent => EffectIdempotency::NonIdempotent,
    };
    let cancellation = match descriptor.cancellation {
        insight_resources::actions::CancellationClass::Cooperative => {
            WorkerCancellation::Cooperative
        }
        insight_resources::actions::CancellationClass::NotSupported => {
            WorkerCancellation::LeaseOnly
        }
    };
    if policy.effect_class() != effect_class
        || policy.effect_idempotency() != idempotency
        || policy.cancellation() != cancellation
    {
        return Err(invariant(ACTION_DESCRIPTOR_INVALID));
    }
    Ok(())
}

/// Verifies that recovery is executing the exact Action revision frozen when
/// the Deployment Revision was linked. The runtime registry is an
/// implementation source, not mutable authority for identity or policy.
fn validate_frozen_action_binding(
    action: &insight_resources::actions::RegisteredAction,
    request: &TaskExecutionRequest,
) -> Result<(), WorkerFailure> {
    let identity = action.identity();
    let action_version = identity.version.to_string();
    if request.implementation() != identity.id
        || request.worker_version().as_str() != action_version
        || request.deployment_binding() != &frozen_action_binding(action)
    {
        return Err(invariant(ACTION_DESCRIPTOR_INVALID));
    }
    Ok(())
}

fn frozen_action_binding(action: &insight_resources::actions::RegisteredAction) -> Value {
    let descriptor = action.descriptor();
    let identity = action.identity();
    serde_json::json!({
        "adapter": "native_action",
        "action_id": identity.id,
        "action_version": identity.version.to_string(),
        "descriptor_hash": identity.descriptor_hash,
        "effect": descriptor.effect,
        "idempotency": descriptor.idempotency,
        "cancellation": descriptor.cancellation,
        "required_capabilities": descriptor
            .required_capabilities
            .iter()
            .map(|capability| capability.as_str())
            .collect::<Vec<_>>(),
        "public": action.public_policy(),
    })
}

fn control(code: &'static str) -> WorkerFailure {
    WorkerFailure::new(WorkerFailureClass::ControlTermination, code, false)
        .expect("constant failure is valid")
}

fn infrastructure(code: &'static str, retryable: bool) -> WorkerFailure {
    WorkerFailure::new(WorkerFailureClass::InfrastructureFailure, code, retryable)
        .expect("constant failure is valid")
}

fn invariant(code: &'static str) -> WorkerFailure {
    WorkerFailure::new(WorkerFailureClass::InvariantCorruption, code, false)
        .expect("constant failure is valid")
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use chrono::Utc;
    use futures::stream;
    use insight_durable::model_tool_queue::adapter::{
        deterministic_tool_identity, model_tool_task_claim_new, parse_action_from_stored_evidence,
    };
    use insight_engine::{
        plan::{DataPortId, LeafTaskDescriptor, PlanProperty, PortName},
        response::{LiveResponseDelivery, LiveResponseSubscriber, ResponseStreamEventType},
        scheduler::{BoundTaskInput, SchedulerAction, SchedulerCheckpointId, SchedulerTaskId},
        worker::{
            ModelCallAuthority, ModelContinuationTurn, ModelToolResult, ResponseItemAuthority,
        },
        ActivationId, AttemptNo, EffectId, EffectIdempotency, LeafTaskKind, LeaseEpoch, NodeId,
        RunId, TaskAdmissionClass, WorkerCancellation, WorkerEffectPolicy,
    };
    use insight_resources::{
        actions::{Action, ActionDescriptor, CancellationClass, IdempotencyClass},
        models::{
            ChatChunk, ChatEvent, ChatEventStream, ChatModel, ChatResponse, ChatStream,
            ModelDeploymentIdentity,
        },
    };
    use serde_json::{json, Value};

    use super::*;
    use crate::{
        catalog::{LeafDeploymentResolver, ProductionLeafDeploymentResolver},
        response_stream::InMemoryLiveResponseBroker,
    };

    #[test]
    fn provider_errors_map_to_specific_body_free_public_failures() {
        let cases = [
            (
                "UPSTREAM_AUTHENTICATION",
                LLM_PROVIDER_AUTHENTICATION_FAILED,
                false,
            ),
            ("UPSTREAM_PERMISSION", LLM_PROVIDER_PERMISSION_DENIED, false),
            ("UPSTREAM_CONNECTION", LLM_PROVIDER_CONNECTION_FAILED, true),
            ("UPSTREAM_TIMEOUT", LLM_PROVIDER_REQUEST_TIMEOUT, true),
            (
                "UPSTREAM_REQUEST_REJECTED",
                LLM_PROVIDER_REQUEST_REJECTED,
                false,
            ),
            ("UPSTREAM_RATE_LIMIT", LLM_PROVIDER_RATE_LIMITED, true),
            ("UPSTREAM_UNAVAILABLE", LLM_PROVIDER_UNAVAILABLE, true),
            (
                "UPSTREAM_STREAM_INVALID",
                LLM_PROVIDER_RESPONSE_INVALID,
                true,
            ),
            (
                "MODEL_RESPONSE_TOO_LARGE",
                LLM_PROVIDER_RESPONSE_TOO_LARGE,
                false,
            ),
        ];

        for (provider_code, public_code, retryable) in cases {
            let failure = map_llm_error(RunError::operation(
                provider_code,
                "provider body secret must not cross the worker boundary",
            ));
            assert_eq!(failure.class(), WorkerFailureClass::InfrastructureFailure);
            assert_eq!(failure.code(), public_code);
            assert_eq!(failure.retryable(), retryable);
        }
    }

    #[derive(Debug, Clone)]
    struct CapturingModel {
        requests: Arc<Mutex<Vec<ChatRequest>>>,
        response: String,
    }

    #[async_trait]
    impl ChatModel for CapturingModel {
        fn capabilities(&self) -> BTreeSet<ModelCapability> {
            BTreeSet::from([ModelCapability::JsonSchemaOutput, ModelCapability::Vision])
        }

        fn request_capabilities(&self) -> BTreeSet<ModelRequestCapability> {
            BTreeSet::from([
                ModelRequestCapability::Complete,
                ModelRequestCapability::Streaming,
            ])
        }

        fn validate_parameters(&self, parameters: &Value) -> Result<(), CompileError> {
            if parameters.is_object() {
                Ok(())
            } else {
                Err(CompileError::new(
                    "MODEL_PARAMETERS_INVALID",
                    "invalid parameters",
                ))
            }
        }

        async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError> {
            self.requests.lock().unwrap().push(request);
            Ok(Box::pin(stream::iter([Ok(ChatChunk {
                text: self.response.clone(),
                finish_reason: Some("stop".to_owned()),
                usage: None,
            })])))
        }
    }

    #[derive(Debug, Clone)]
    struct ChunkedModel;

    #[async_trait]
    impl ChatModel for ChunkedModel {
        fn capabilities(&self) -> BTreeSet<ModelCapability> {
            BTreeSet::new()
        }

        fn request_capabilities(&self) -> BTreeSet<ModelRequestCapability> {
            BTreeSet::from([
                ModelRequestCapability::Complete,
                ModelRequestCapability::Streaming,
            ])
        }

        fn validate_parameters(&self, parameters: &Value) -> Result<(), CompileError> {
            if parameters.is_object() {
                Ok(())
            } else {
                Err(CompileError::new(
                    "MODEL_PARAMETERS_INVALID",
                    "invalid parameters",
                ))
            }
        }

        async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
            Ok(Box::pin(stream::iter([
                Ok(ChatChunk {
                    text: "durable ".to_owned(),
                    finish_reason: None,
                    usage: None,
                }),
                Ok(ChatChunk {
                    text: "final ".to_owned(),
                    finish_reason: None,
                    usage: None,
                }),
                Ok(ChatChunk {
                    text: "answer".to_owned(),
                    finish_reason: Some("stop".to_owned()),
                    usage: None,
                }),
            ])))
        }
    }

    #[derive(Debug, Clone)]
    struct FinishReasonModel {
        finish_reason: Option<String>,
    }

    #[async_trait]
    impl ChatModel for FinishReasonModel {
        fn capabilities(&self) -> BTreeSet<ModelCapability> {
            BTreeSet::new()
        }

        fn request_capabilities(&self) -> BTreeSet<ModelRequestCapability> {
            BTreeSet::from([
                ModelRequestCapability::Complete,
                ModelRequestCapability::Streaming,
            ])
        }

        fn validate_parameters(&self, parameters: &Value) -> Result<(), CompileError> {
            if parameters.is_object() {
                Ok(())
            } else {
                Err(CompileError::new(
                    "MODEL_PARAMETERS_INVALID",
                    "invalid parameters",
                ))
            }
        }

        async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
            Ok(Box::pin(stream::iter([Ok(ChatChunk {
                // A non-empty prefix is intentional: truncation and filtering
                // must not turn partial text into a successful node output.
                text: "partial answer".to_owned(),
                finish_reason: self.finish_reason.clone(),
                usage: None,
            })])))
        }
    }

    #[derive(Debug, Clone)]
    struct ToolCallingModel {
        requests: Arc<Mutex<Vec<ChatRequest>>>,
    }

    #[async_trait]
    impl ChatModel for ToolCallingModel {
        fn capabilities(&self) -> BTreeSet<ModelCapability> {
            BTreeSet::new()
        }

        fn request_capabilities(&self) -> BTreeSet<ModelRequestCapability> {
            BTreeSet::from([
                ModelRequestCapability::Complete,
                ModelRequestCapability::Streaming,
            ])
        }

        fn validate_parameters(&self, parameters: &Value) -> Result<(), CompileError> {
            parameters
                .is_object()
                .then_some(())
                .ok_or_else(|| CompileError::new("MODEL_PARAMETERS_INVALID", "invalid parameters"))
        }

        async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, RunError> {
            self.requests.lock().unwrap().push(request);
            Ok(ChatResponse {
                text: String::new(),
                tool_calls: vec![ChatToolCall {
                    index: 0,
                    id: "call_lookup".to_owned(),
                    name: "lookup".to_owned(),
                    arguments: r#"{"query":"WBC"}"#.to_owned(),
                }],
                finish_reason: ChatFinishReason::ToolCalls,
                usage: None,
            })
        }

        async fn stream_chat_events(
            &self,
            request: ChatRequest,
        ) -> Result<ChatEventStream, RunError> {
            self.requests.lock().unwrap().push(request);
            Ok(Box::pin(stream::iter([
                Ok(ChatEvent {
                    tool_call_deltas: vec![ChatToolCallDelta {
                        index: 0,
                        id: Some("call_lookup".to_owned()),
                        name: Some("lookup".to_owned()),
                        arguments_delta: "{\"query\":\"".to_owned(),
                    }],
                    ..ChatEvent::default()
                }),
                Ok(ChatEvent {
                    tool_call_deltas: vec![ChatToolCallDelta {
                        index: 0,
                        id: None,
                        name: None,
                        arguments_delta: r#"WBC"}"#.to_owned(),
                    }],
                    finish_reason: Some(ChatFinishReason::ToolCalls),
                    ..ChatEvent::default()
                }),
            ])))
        }

        async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
            unreachable!("typed tool-call tests use the complete or typed streaming surface")
        }
    }

    #[derive(Debug, Clone)]
    struct ContinuationCapturingModel {
        requests: Arc<Mutex<Vec<(ChatRequestMode, ChatRequest)>>>,
    }

    #[async_trait]
    impl ChatModel for ContinuationCapturingModel {
        fn capabilities(&self) -> BTreeSet<ModelCapability> {
            BTreeSet::new()
        }

        fn request_capabilities(&self) -> BTreeSet<ModelRequestCapability> {
            BTreeSet::from([
                ModelRequestCapability::Complete,
                ModelRequestCapability::Streaming,
            ])
        }

        fn validate_parameters(&self, parameters: &Value) -> Result<(), CompileError> {
            parameters
                .is_object()
                .then_some(())
                .ok_or_else(|| CompileError::new("MODEL_PARAMETERS_INVALID", "invalid parameters"))
        }

        async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, RunError> {
            self.requests
                .lock()
                .unwrap()
                .push((ChatRequestMode::Complete, request));
            Ok(ChatResponse {
                text: "final answer".to_owned(),
                tool_calls: Vec::new(),
                finish_reason: ChatFinishReason::Stop,
                usage: None,
            })
        }

        async fn stream_chat_events(
            &self,
            request: ChatRequest,
        ) -> Result<ChatEventStream, RunError> {
            self.requests
                .lock()
                .unwrap()
                .push((ChatRequestMode::Streaming, request));
            Ok(Box::pin(stream::iter([Ok(ChatEvent {
                text_delta: "final answer".to_owned(),
                finish_reason: Some(ChatFinishReason::Stop),
                ..ChatEvent::default()
            })])))
        }

        async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
            unreachable!("continuation test uses the typed provider surfaces")
        }
    }

    #[derive(Clone)]
    struct CapturingAction {
        calls: Arc<Mutex<Vec<(Value, String, u32)>>>,
    }

    const MODEL_TOOL_SERVER_SECRET_SENTINEL: &str = "server-only-model-tool-secret";

    #[derive(Clone)]
    struct ServerInjectingCapturingAction {
        calls: Arc<Mutex<Vec<Value>>>,
        fail_with_secret: bool,
    }

    #[async_trait]
    impl Action for CapturingAction {
        fn descriptor(&self) -> ActionDescriptor {
            let contract = json!({
                "type": "object",
                "properties": {"text": {"type": "string"}},
                "required": ["text"],
                "additionalProperties": false
            });
            ActionDescriptor {
                id: "example.capture",
                version: "1.2.3",
                input_schema: contract.clone(),
                output_schema: contract,
                effect: EffectClass::Mutating,
                idempotency: IdempotencyClass::Idempotent,
                cancellation: CancellationClass::Cooperative,
                required_capabilities: BTreeSet::new(),
            }
        }

        async fn call(&self, input: Value, context: ActionContext) -> Result<Value, RunError> {
            self.calls.lock().unwrap().push((
                input.clone(),
                context.idempotency_key,
                context.attempt,
            ));
            Ok(input)
        }
    }

    #[async_trait]
    impl Action for ServerInjectingCapturingAction {
        fn descriptor(&self) -> ActionDescriptor {
            let contract = json!({
                "type": "object",
                "properties": {"text": {"type": "string"}},
                "required": ["text"],
                "additionalProperties": false
            });
            ActionDescriptor {
                id: "example.server_injecting_capture",
                version: "1.0.0",
                input_schema: contract.clone(),
                output_schema: contract,
                effect: EffectClass::Pure,
                idempotency: IdempotencyClass::Idempotent,
                cancellation: CancellationClass::Cooperative,
                required_capabilities: BTreeSet::new(),
            }
        }

        fn inject_model_tool_input(
            &self,
            mut model_visible_input: Value,
            _context: &ActionContext,
        ) -> Result<Value, RunError> {
            model_visible_input
                .as_object_mut()
                .expect("model-visible Action input is a closed object")
                .insert(
                    "server_token".to_owned(),
                    Value::String(MODEL_TOOL_SERVER_SECRET_SENTINEL.to_owned()),
                );
            Ok(model_visible_input)
        }

        async fn call(&self, mut input: Value, _context: ActionContext) -> Result<Value, RunError> {
            self.calls.lock().unwrap().push(input.clone());
            if self.fail_with_secret {
                return Err(RunError::operation(
                    "SECRET_ACTION_FAILURE",
                    format!("rejected token {MODEL_TOOL_SERVER_SECRET_SENTINEL}"),
                ));
            }
            input
                .as_object_mut()
                .expect("injected Action input remains an object")
                .remove("server_token");
            Ok(input)
        }
    }

    #[derive(Clone)]
    struct LookupContinuationAction;

    #[async_trait]
    impl Action for LookupContinuationAction {
        fn descriptor(&self) -> ActionDescriptor {
            ActionDescriptor {
                id: "lookup",
                version: "1.0.0",
                input_schema: lookup_schema(),
                output_schema: json!({"type": "object"}),
                effect: EffectClass::Pure,
                idempotency: IdempotencyClass::Idempotent,
                cancellation: CancellationClass::Cooperative,
                required_capabilities: BTreeSet::new(),
            }
        }

        async fn call(&self, input: Value, _context: ActionContext) -> Result<Value, RunError> {
            Ok(input)
        }
    }

    fn version(value: &str) -> VersionTag {
        VersionTag::new(value).unwrap()
    }

    fn worker_context(attempt: u32) -> WorkerExecutionContext {
        WorkerExecutionContext::new(
            AttemptNo::new(attempt).unwrap(),
            LeaseEpoch::new(u64::from(attempt)).unwrap(),
            format!("fence-{attempt}"),
            Utc::now() + chrono::Duration::minutes(1),
        )
        .unwrap()
    }

    fn public_llm_context(attempt: u32) -> WorkerExecutionContext {
        worker_context(attempt).with_model_call(
            ModelCallAuthority::new(
                "response_finish_matrix",
                1,
                Some(ResponseItemAuthority::new("message_finish_matrix", 0).unwrap()),
            )
            .unwrap(),
        )
    }

    fn dispatch_request(
        task_kind: SchedulerTaskKind,
        implementation: &str,
        worker_version: &str,
        configuration: BTreeMap<String, DescriptorValue>,
        inputs: Vec<BoundTaskInput>,
        output_type: PlanType,
    ) -> TaskExecutionRequest {
        dispatch_request_with_tool_bindings(
            task_kind,
            implementation,
            worker_version,
            configuration,
            inputs,
            output_type,
            Vec::new(),
        )
    }

    fn dispatch_request_with_tool_bindings(
        task_kind: SchedulerTaskKind,
        implementation: &str,
        worker_version: &str,
        configuration: BTreeMap<String, DescriptorValue>,
        inputs: Vec<BoundTaskInput>,
        output_type: PlanType,
        frozen_tools: Vec<Value>,
    ) -> TaskExecutionRequest {
        dispatch_request_with_frozen_bindings(
            task_kind,
            implementation,
            worker_version,
            configuration,
            inputs,
            output_type,
            frozen_tools,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_request_with_frozen_bindings(
        task_kind: SchedulerTaskKind,
        implementation: &str,
        worker_version: &str,
        mut configuration: BTreeMap<String, DescriptorValue>,
        inputs: Vec<BoundTaskInput>,
        output_type: PlanType,
        frozen_tools: Vec<Value>,
        deployment_binding_override: Option<Value>,
    ) -> TaskExecutionRequest {
        if task_kind == SchedulerTaskKind::Llm {
            configuration
                .entry("stream".to_owned())
                .or_insert(DescriptorValue::Boolean(true));
            configuration
                .entry("publish".to_owned())
                .or_insert(DescriptorValue::Boolean(false));
            configuration
                .entry("tools".to_owned())
                .or_insert_with(|| DescriptorValue::Array(Vec::new()));
            configuration
                .entry("tool_choice".to_owned())
                .or_insert_with(|| DescriptorValue::String("auto".to_owned()));
            configuration
                .entry("tool_limits".to_owned())
                .or_insert_with(|| {
                    object([
                        ("max_calls", DescriptorValue::Integer(32)),
                        ("max_rounds", DescriptorValue::Integer(8)),
                    ])
                });
        }
        let deployment_binding = deployment_binding_override.unwrap_or_else(|| {
            if task_kind == SchedulerTaskKind::Llm {
                test_llm_deployment_binding(&configuration, frozen_tools)
            } else {
                json!({})
            }
        });
        let action = SchedulerAction::DispatchTask {
            task_id: SchedulerTaskId::parse(format!("task_{}", "1".repeat(64))).unwrap(),
            effect_id: EffectId::new("effect_stable").unwrap(),
            activation_id: ActivationId::new("activation_leaf").unwrap(),
            node_id: NodeId::new("leaf").unwrap(),
            admission_class: TaskAdmissionClass::Normal,
            task_kind,
            implementation: implementation.to_owned(),
            descriptor_version: version(if task_kind == SchedulerTaskKind::Llm {
                "2"
            } else {
                "1"
            }),
            worker_version: version(worker_version),
            effect_policy: WorkerEffectPolicy::new(
                EffectIdempotency::Idempotent,
                1,
                WorkerCancellation::Cooperative,
            )
            .unwrap(),
            deployment_binding,
            public_configuration: configuration,
            secret_configuration: BTreeMap::new(),
            inputs,
            outputs: vec![insight_engine::internal::task_output_contract(
                DataPortId::new("leaf_result").unwrap(),
                PortName::new("result").unwrap(),
                output_type,
                true,
            )],
        };
        let intent = insight_engine::internal::scheduler_intent(
            RunId::new("run_leaf").unwrap(),
            SchedulerCheckpointId::parse(format!("checkpoint_{}", "2".repeat(64))).unwrap(),
            action,
        );
        TaskExecutionRequest::from_scheduler_intent(&intent).unwrap()
    }

    fn test_llm_deployment_binding(
        configuration: &BTreeMap<String, DescriptorValue>,
        tools: Vec<Value>,
    ) -> Value {
        let model_alias = match configuration.get("model") {
            Some(DescriptorValue::String(value)) => value.clone(),
            _ => panic!("test llm configuration must contain a model alias"),
        };
        let request_mode = match configuration.get("stream") {
            Some(DescriptorValue::Boolean(true)) => ModelRequestCapability::Streaming.as_str(),
            Some(DescriptorValue::Boolean(false)) => ModelRequestCapability::Complete.as_str(),
            _ => panic!("test llm configuration must contain stream"),
        };
        let tool_choice = match configuration.get("tool_choice") {
            Some(DescriptorValue::String(value)) => value.clone(),
            _ => panic!("test llm configuration must contain tool_choice"),
        };
        let tool_limits = descriptor_json(
            configuration
                .get("tool_limits")
                .expect("test llm configuration must contain tool_limits"),
        )
        .unwrap();
        let mut binding = json!({
            "adapter": "core.llm",
            "model_alias": model_alias,
            "model_binding_hash": "test-model-binding-hash",
            "model_binding": {"adapter": "test"},
            "request_mode": request_mode,
            "request_capabilities": ["complete_request", "streaming_request"],
            "tool_choice": tool_choice,
            "tool_limits": tool_limits,
            "tools": tools,
        });
        if binding["tools"]
            .as_array()
            .is_some_and(|tools| !tools.is_empty())
        {
            binding["runtime_capabilities"] = json!([LLM_TOOL_CONTINUATION_CAPABILITY]);
        }
        binding
    }

    fn object(
        fields: impl IntoIterator<Item = (&'static str, DescriptorValue)>,
    ) -> DescriptorValue {
        DescriptorValue::Object(
            fields
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value))
                .collect(),
        )
    }

    fn string(value: &str) -> DescriptorValue {
        DescriptorValue::String(value.to_owned())
    }

    fn finish_reason_configuration(stream: bool) -> BTreeMap<String, DescriptorValue> {
        BTreeMap::from([
            ("model".to_owned(), string("chat")),
            ("stream".to_owned(), DescriptorValue::Boolean(stream)),
            ("publish".to_owned(), DescriptorValue::Boolean(true)),
            ("parameters".to_owned(), object([])),
            (
                "message_program".to_owned(),
                DescriptorValue::Array(vec![object([
                    ("kind", string("message")),
                    ("role", string("user")),
                    (
                        "content",
                        DescriptorValue::Array(vec![object([
                            ("kind", string("literal")),
                            ("references", DescriptorValue::Array(Vec::new())),
                            ("text", string("question")),
                        ])]),
                    ),
                ])]),
            ),
        ])
    }

    fn lookup_schema() -> Value {
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
            "additionalProperties": false
        })
    }

    fn frozen_lookup_binding() -> Value {
        let effect_policy = WorkerEffectPolicy::frozen(
            WorkerEffectClass::Pure,
            EffectIdempotency::Idempotent,
            1,
            0,
            0,
            60_000,
            WorkerCancellation::Cooperative,
        )
        .unwrap();
        json!({
            "name": "lookup",
            "action_id": "lookup",
            "action_version": "1.0.0",
            "descriptor_hash": "a".repeat(64),
            "input_schema": lookup_schema(),
            "output_schema": {"type": "object"},
            "effect": "pure",
            "idempotency": "idempotent",
            "cancellation": "cooperative",
            "required_capabilities": [],
            "effect_policy": effect_policy,
            "public_policy": {"call": false, "arguments": "private", "result": null},
            "effective_public_policy": {"call": false, "arguments": "private", "result": null},
        })
    }

    fn lookup_contract() -> FrozenLlmToolContract {
        let schema = lookup_schema();
        FrozenLlmToolContract {
            tools: vec![FrozenLlmTool {
                validator: insight_engine::schema::compile_schema_2020(&schema).unwrap(),
                definition: ChatToolDefinition {
                    name: "lookup".to_owned(),
                    description: None,
                    input_schema: schema,
                },
                raw_argument_deltas_authorized: false,
            }],
            choice: ChatToolChoice::Required,
            max_rounds: 8,
            max_calls: 32,
        }
    }

    async fn collect_publication_types(
        subscriber: &mut Box<dyn LiveResponseSubscriber>,
    ) -> (Vec<ResponseStreamEventType>, LiveResponseSealStatus) {
        let mut event_types = Vec::new();
        loop {
            let delivery = tokio::time::timeout(Duration::from_secs(1), subscriber.recv())
                .await
                .expect("finish-reason publication did not seal")
                .expect("finish-reason publication broker failed");
            match delivery {
                LiveResponseDelivery::Publication(publication) => {
                    event_types.push(publication.payload_type());
                }
                LiveResponseDelivery::Seal(seal) => return (event_types, seal.status()),
                LiveResponseDelivery::Gap(_) => panic!("finish-reason matrix must not lose events"),
            }
        }
    }

    #[tokio::test]
    async fn llm_tool_calls_use_frozen_contract_and_return_one_closed_batch_in_both_modes() {
        for stream_requested in [true, false] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let mut models = ModelRegistry::default();
            models
                .register_versioned(
                    "chat",
                    ModelDeploymentIdentity::new(
                        "model-worker-1",
                        json!({"adapter": "tool-call-test"}),
                    )
                    .unwrap(),
                    ToolCallingModel {
                        requests: requests.clone(),
                    },
                )
                .unwrap();
            let mut configuration = finish_reason_configuration(stream_requested);
            configuration.insert("publish".to_owned(), DescriptorValue::Boolean(false));
            configuration.insert(
                "tools".to_owned(),
                DescriptorValue::Array(vec![string("lookup")]),
            );
            configuration.insert("tool_choice".to_owned(), string("required"));
            let request = dispatch_request_with_tool_bindings(
                SchedulerTaskKind::Llm,
                "core.llm",
                "model-worker-1",
                configuration,
                Vec::new(),
                PlanType::String,
                vec![frozen_lookup_binding()],
            );
            let mut registry = WorkerExecutorRegistry::new();
            registry
                .register(
                    SchedulerTaskKind::Llm,
                    "core.llm",
                    version("2"),
                    version("model-worker-1"),
                    Arc::new(LlmTaskExecutor::new(models)),
                )
                .unwrap();

            let result = registry
                .execute(&public_llm_context(1), &request, CancellationToken::new())
                .await
                .unwrap();
            assert!(result.outputs().is_empty());
            assert_eq!(
                result.model_call().unwrap().finish_reason(),
                ModelFinishReason::ToolCalls
            );
            let batch = result.model_tool_call_batch().unwrap();
            assert_eq!(batch.model_call_no(), 1);
            assert_eq!(batch.calls().len(), 1);
            assert_eq!(batch.calls()[0].call_id(), "call_lookup");
            assert_eq!(batch.calls()[0].name(), "lookup");
            assert_eq!(batch.calls()[0].arguments(), &json!({"query": "WBC"}));

            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].tool_choice, ChatToolChoice::Required);
            assert_eq!(requests[0].tools.len(), 1);
            assert_eq!(requests[0].tools[0].input_schema, lookup_schema());
        }
    }

    #[tokio::test]
    async fn llm_adapter_reconstructs_two_continuation_rounds_in_both_request_modes() {
        for stream_requested in [false, true] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let mut models = ModelRegistry::default();
            models
                .register_versioned(
                    "chat",
                    ModelDeploymentIdentity::new(
                        "model-worker-1",
                        json!({"adapter": "continuation-capture"}),
                    )
                    .unwrap(),
                    ContinuationCapturingModel {
                        requests: requests.clone(),
                    },
                )
                .unwrap();
            let mut configuration = finish_reason_configuration(stream_requested);
            configuration.insert("publish".to_owned(), DescriptorValue::Boolean(false));
            configuration.insert(
                "tools".to_owned(),
                DescriptorValue::Array(vec![string("lookup")]),
            );
            configuration.insert("tool_choice".to_owned(), string("required"));
            let request = dispatch_request_with_tool_bindings(
                SchedulerTaskKind::Llm,
                "core.llm",
                "model-worker-1",
                configuration,
                Vec::new(),
                PlanType::String,
                vec![frozen_lookup_binding()],
            );
            let turn = |model_call_no, call_ids: &[&str]| {
                ModelContinuationTurn::new(
                    model_call_no,
                    (model_call_no == 1).then_some("assistant preface".to_owned()),
                    call_ids
                        .iter()
                        .enumerate()
                        .map(|(index, call_id)| {
                            ModelToolCall::new(
                                u32::try_from(index).unwrap(),
                                *call_id,
                                "lookup",
                                json!({"query": call_id}),
                            )
                            .unwrap()
                        })
                        .collect(),
                    call_ids
                        .iter()
                        .enumerate()
                        .map(|(index, call_id)| {
                            ModelToolResult::new(*call_id, json!({"z": index + 1, "a": call_id}))
                                .unwrap()
                        })
                        .collect(),
                )
                .unwrap()
            };
            let context = worker_context(1)
                .with_model_call(ModelCallAuthority::new("response_continuation", 3, None).unwrap())
                .with_model_continuation(vec![turn(1, &["call_1"]), turn(2, &["call_2", "call_3"])])
                .unwrap();

            let result = LlmTaskExecutor::new(models)
                .execute(&context, &request, CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(
                result.outputs().values().next().unwrap().value(),
                &json!("final answer")
            );
            assert_eq!(result.model_call().unwrap().model_call_no(), 3);

            let captured = requests.lock().unwrap();
            assert_eq!(captured.len(), 1);
            assert_eq!(
                captured[0].0,
                if stream_requested {
                    ChatRequestMode::Streaming
                } else {
                    ChatRequestMode::Complete
                }
            );
            let messages = &captured[0].1.messages;
            assert_eq!(messages.len(), 6);
            assert_eq!(messages[0].role(), ChatRole::User);
            assert_eq!(messages[1].role(), ChatRole::Assistant);
            assert_eq!(messages[1].text(), Some("assistant preface"));
            assert_eq!(messages[1].tool_calls()[0].id, "call_1");
            assert_eq!(
                messages[1].tool_calls()[0].arguments,
                r#"{"query":"call_1"}"#
            );
            assert_eq!(messages[2].role(), ChatRole::Tool);
            assert_eq!(messages[2].tool_call_id(), Some("call_1"));
            assert_eq!(messages[2].text(), Some(r#"{"a":"call_1","z":1}"#));
            assert_eq!(messages[3].role(), ChatRole::Assistant);
            assert_eq!(messages[3].tool_calls().len(), 2);
            assert_eq!(messages[4].tool_call_id(), Some("call_2"));
            assert_eq!(messages[5].tool_call_id(), Some("call_3"));
            assert_eq!(messages[5].text(), Some(r#"{"a":"call_3","z":2}"#));
        }
    }

    #[tokio::test]
    async fn llm_adapter_accepts_exact_catalog_linked_tool_binding_and_rejects_limit_drift() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut models = ModelRegistry::default();
        models
            .register_versioned(
                "chat",
                ModelDeploymentIdentity::new(
                    "model-worker-1",
                    json!({"adapter": "catalog-linked-continuation"}),
                )
                .unwrap(),
                ContinuationCapturingModel {
                    requests: requests.clone(),
                },
            )
            .unwrap();
        let mut actions = ActionRegistry::default();
        actions.register(LookupContinuationAction).unwrap();
        let mut configuration = finish_reason_configuration(false);
        configuration.insert("publish".to_owned(), DescriptorValue::Boolean(false));
        configuration.insert(
            "tools".to_owned(),
            DescriptorValue::Array(vec![string("lookup")]),
        );
        configuration.insert("tool_choice".to_owned(), string("required"));
        configuration.insert(
            "tool_limits".to_owned(),
            object([
                ("max_calls", DescriptorValue::Integer(32)),
                ("max_rounds", DescriptorValue::Integer(8)),
            ]),
        );
        let descriptor = LeafTaskDescriptor::new("core.llm", version("2"), configuration.clone());
        let resolved = ProductionLeafDeploymentResolver::new(&models, &actions)
            .with_llm_tool_continuation_capability()
            .resolve_leaf(LeafTaskKind::Llm, &descriptor)
            .unwrap();
        let binding = resolved.binding_evidence().clone();
        assert_eq!(
            binding["tool_limits"],
            json!({"max_rounds": 8, "max_calls": 32})
        );
        assert!(binding["tools"][0].get("effect_policy").is_some());
        assert!(binding["tools"][0].get("output_schema").is_some());

        let request = dispatch_request_with_frozen_bindings(
            SchedulerTaskKind::Llm,
            "core.llm",
            "model-worker-1",
            configuration.clone(),
            Vec::new(),
            PlanType::String,
            Vec::new(),
            Some(binding.clone()),
        );
        let turn = ModelContinuationTurn::new(
            1,
            None,
            vec![ModelToolCall::new(0, "call_catalog", "lookup", json!({"query": "WBC"})).unwrap()],
            vec![ModelToolResult::new("call_catalog", json!({"answer": "normal"})).unwrap()],
        )
        .unwrap();
        let context = worker_context(1)
            .with_model_call(ModelCallAuthority::new("response_catalog", 2, None).unwrap())
            .with_model_continuation(vec![turn])
            .unwrap();
        LlmTaskExecutor::new(models.clone())
            .execute(&context, &request, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(requests.lock().unwrap().len(), 1);

        let mut drifted_binding = binding;
        drifted_binding["tool_limits"]["max_calls"] = json!(31);
        let drifted_request = dispatch_request_with_frozen_bindings(
            SchedulerTaskKind::Llm,
            "core.llm",
            "model-worker-1",
            configuration,
            Vec::new(),
            PlanType::String,
            Vec::new(),
            Some(drifted_binding),
        );
        assert_eq!(
            LlmTaskExecutor::new(models)
                .execute(&context, &drifted_request, CancellationToken::new())
                .await
                .unwrap_err()
                .code(),
            LLM_BINDING_INVALID
        );
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn llm_adapter_enforces_frozen_round_and_call_limits_before_another_tool_handoff() {
        for (max_rounds, max_calls, completed_calls, expected_code) in [
            (1, 2, 1, LLM_TOOL_ROUND_LIMIT),
            (2, 2, 2, LLM_TOOL_CALL_LIMIT),
        ] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let mut models = ModelRegistry::default();
            models
                .register_versioned(
                    "chat",
                    ModelDeploymentIdentity::new(
                        "model-worker-1",
                        json!({"adapter": "continuation-limit"}),
                    )
                    .unwrap(),
                    ToolCallingModel {
                        requests: requests.clone(),
                    },
                )
                .unwrap();
            let mut configuration = finish_reason_configuration(false);
            configuration.insert("publish".to_owned(), DescriptorValue::Boolean(false));
            configuration.insert(
                "tools".to_owned(),
                DescriptorValue::Array(vec![string("lookup")]),
            );
            configuration.insert("tool_choice".to_owned(), string("required"));
            configuration.insert(
                "tool_limits".to_owned(),
                object([
                    ("max_calls", DescriptorValue::Integer(i64::from(max_calls))),
                    (
                        "max_rounds",
                        DescriptorValue::Integer(i64::from(max_rounds)),
                    ),
                ]),
            );
            let request = dispatch_request_with_tool_bindings(
                SchedulerTaskKind::Llm,
                "core.llm",
                "model-worker-1",
                configuration,
                Vec::new(),
                PlanType::String,
                vec![frozen_lookup_binding()],
            );
            let calls = (0..completed_calls)
                .map(|index| {
                    ModelToolCall::new(
                        index,
                        format!("call_prior_{index}"),
                        "lookup",
                        json!({"query": format!("prior-{index}")}),
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();
            let results = calls
                .iter()
                .map(|call| ModelToolResult::new(call.call_id(), json!({"ok": true})).unwrap())
                .collect();
            let context = worker_context(1)
                .with_model_call(ModelCallAuthority::new("response_limit", 2, None).unwrap())
                .with_model_continuation(vec![
                    ModelContinuationTurn::new(1, None, calls, results).unwrap()
                ])
                .unwrap();

            assert_eq!(
                LlmTaskExecutor::new(models)
                    .execute(&context, &request, CancellationToken::new())
                    .await
                    .unwrap_err()
                    .code(),
                expected_code
            );
            assert_eq!(requests.lock().unwrap().len(), 1);
        }
    }

    #[test]
    fn llm_tool_call_validation_is_closed_over_whitelist_identity_json_schema_and_bytes() {
        let contract = lookup_contract();
        let call = |index, id: &str, name: &str, arguments: &str| ChatToolCall {
            index,
            id: id.to_owned(),
            name: name.to_owned(),
            arguments: arguments.to_owned(),
        };
        let cases = [
            vec![call(0, "call_1", "outside", r#"{"query":"WBC"}"#)],
            vec![
                call(0, "call_1", "lookup", r#"{"query":"WBC"}"#),
                call(1, "call_1", "lookup", r#"{"query":"RBC"}"#),
            ],
            vec![
                call(0, "call_1", "lookup", r#"{"query":"WBC"}"#),
                call(0, "call_2", "lookup", r#"{"query":"RBC"}"#),
            ],
            vec![call(0, "call_1", "lookup", "[]")],
            vec![call(0, "call_1", "lookup", r#"{"query":7}"#)],
        ];
        for calls in cases {
            assert_eq!(
                normalize_model_tool_calls(calls, &contract, 1_024)
                    .unwrap_err()
                    .code(),
                LLM_TOOL_CALL_INVALID
            );
        }
        assert_eq!(
            normalize_model_tool_calls(
                vec![call(0, "call_1", "lookup", r#"{"query":"WBC"}"#)],
                &contract,
                1,
            )
            .unwrap_err()
            .code(),
            LLM_TOOL_CALL_INVALID
        );

        let mut accumulator = StreamingToolCallAccumulator::default();
        accumulator
            .push(
                vec![ChatToolCallDelta {
                    index: 0,
                    id: Some("call_1".to_owned()),
                    name: Some("lookup".to_owned()),
                    arguments_delta: String::new(),
                }],
                0,
                1_024,
            )
            .unwrap();
        assert_eq!(
            accumulator
                .push(
                    vec![ChatToolCallDelta {
                        index: 0,
                        id: Some("call_2".to_owned()),
                        name: None,
                        arguments_delta: String::new(),
                    }],
                    0,
                    1_024,
                )
                .unwrap_err()
                .code(),
            LLM_TOOL_CALL_INVALID
        );
    }

    #[test]
    fn streaming_tool_argument_fragment_limits_count_empty_provider_deltas_at_both_scopes() {
        let fragments = |index: u32, count: usize| {
            (0..count)
                .map(|position| ChatToolCallDelta {
                    index,
                    id: (position == 0).then(|| format!("call_{index}")),
                    name: (position == 0).then(|| "lookup".to_owned()),
                    arguments_delta: String::new(),
                })
                .collect::<Vec<_>>()
        };

        let mut per_call = StreamingToolCallAccumulator::default();
        per_call
            .push(fragments(0, MAX_FUNCTION_ARGUMENT_FRAGMENTS_PER_CALL), 0, 1)
            .unwrap();
        assert_eq!(
            per_call
                .push(
                    vec![ChatToolCallDelta {
                        index: 0,
                        id: None,
                        name: None,
                        arguments_delta: String::new(),
                    }],
                    0,
                    1,
                )
                .unwrap_err()
                .code(),
            LLM_TOOL_CALL_INVALID,
        );

        let mut whole_model_call = StreamingToolCallAccumulator::default();
        for index in 0..4 {
            whole_model_call
                .push(
                    fragments(index, MAX_FUNCTION_ARGUMENT_FRAGMENTS_PER_CALL),
                    0,
                    1,
                )
                .unwrap();
        }
        assert_eq!(
            whole_model_call
                .push(
                    vec![ChatToolCallDelta {
                        index: 4,
                        id: Some("call_4".to_owned()),
                        name: Some("lookup".to_owned()),
                        arguments_delta: String::new(),
                    }],
                    0,
                    1,
                )
                .unwrap_err()
                .code(),
            LLM_TOOL_CALL_INVALID,
        );
        assert_eq!(
            whole_model_call.argument_fragment_count,
            MAX_FUNCTION_ARGUMENT_FRAGMENTS_PER_MODEL_CALL,
        );
    }

    #[tokio::test]
    async fn complete_model_request_publishes_one_full_function_argument_delta() {
        let broker = Arc::new(InMemoryLiveResponseBroker::new(8, 4).unwrap());
        let mut subscriber = broker
            .subscribe(RunId::new("run_leaf").unwrap())
            .await
            .unwrap();
        let (allocator, mut requests) =
            worker_adapter::model_call_public_item_reservation_channel();
        let responder = tokio::spawn(async move {
            let request = requests.recv().await.unwrap();
            worker_adapter::respond_reservation(
                request,
                Ok(ResponseItemAuthority::new("fc_complete_request", 0).unwrap()),
            );
        });
        let services = worker_adapter::services_with_model_call_public_item_allocator(
            WorkerRuntimeServices::default(),
            allocator,
        );
        let publication_broker = broker.clone() as Arc<dyn LiveResponseBroker>;
        let context = worker_context(1).with_model_call(
            ModelCallAuthority::new_with_publication("response_complete_function", 1, true, None)
                .unwrap(),
        );
        let request = dispatch_request(
            SchedulerTaskKind::Llm,
            "core.llm",
            "model-worker-1",
            finish_reason_configuration(false),
            Vec::new(),
            PlanType::String,
        );
        let mut publication = LlmPublication::start(
            Some(&publication_broker),
            &context,
            &request,
            true,
            &services,
        )
        .unwrap();
        let arguments = r#"{"query":"WBC"}"#;
        let calls = vec![ChatToolCall {
            index: 0,
            id: "call_lookup".to_owned(),
            name: "lookup".to_owned(),
            arguments: arguments.to_owned(),
        }];
        let mut contract = lookup_contract();
        contract.tools[0].raw_argument_deltas_authorized = true;
        publication
            .publish_complete_function_calls(&calls, &contract)
            .await
            .unwrap();
        responder.await.unwrap();

        let mut events = Vec::new();
        for sequence in 0..2 {
            let LiveResponseDelivery::Publication(frame) = subscriber.recv().await.unwrap() else {
                panic!("complete function request must publish exactly two provisional frames");
            };
            events.push(serde_json::to_value(frame.into_public_event(sequence)).unwrap());
        }
        assert_eq!(events[0]["type"], "response.output_item.added");
        assert_eq!(events[0]["item"]["status"], "in_progress");
        assert_eq!(events[1]["type"], "response.function_call_arguments.delta");
        assert_eq!(events[1]["delta"], arguments);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), subscriber.recv())
                .await
                .is_err()
        );

        let normalized =
            vec![ModelToolCall::new(0, "call_lookup", "lookup", json!({"query": "WBC"})).unwrap()];
        let checkpoint = publication
            .function_call_checkpoint_publications(&normalized)
            .unwrap();
        assert_eq!(checkpoint.len(), 1);
        assert_eq!(checkpoint[0].argument_delta_count(), 1);
        assert_eq!(checkpoint[0].completed_seal_index(), Some(3));
    }

    #[tokio::test]
    async fn failed_model_request_closes_function_item_without_persisting_provisional_arguments() {
        let broker = Arc::new(InMemoryLiveResponseBroker::new(8, 4).unwrap());
        let mut subscriber = broker
            .subscribe(RunId::new("run_leaf").unwrap())
            .await
            .unwrap();
        let (allocator, mut requests) =
            worker_adapter::model_call_public_item_reservation_channel();
        let responder = tokio::spawn(async move {
            let request = requests.recv().await.unwrap();
            worker_adapter::respond_reservation(
                request,
                Ok(ResponseItemAuthority::new("fc_failed_request", 0).unwrap()),
            );
        });
        let services = worker_adapter::services_with_model_call_public_item_allocator(
            WorkerRuntimeServices::default(),
            allocator,
        );
        let publication_broker = broker.clone() as Arc<dyn LiveResponseBroker>;
        let context = worker_context(1).with_model_call(
            ModelCallAuthority::new_with_publication("response_failed_function", 1, true, None)
                .unwrap(),
        );
        let request = dispatch_request(
            SchedulerTaskKind::Llm,
            "core.llm",
            "model-worker-1",
            finish_reason_configuration(true),
            Vec::new(),
            PlanType::String,
        );
        let mut publication = LlmPublication::start(
            Some(&publication_broker),
            &context,
            &request,
            true,
            &services,
        )
        .unwrap();
        publication
            .ensure_function_started(0, "call_lookup", "lookup")
            .await
            .unwrap();
        publication
            .function_argument_delta(0, r#"{"query":"WBC""#.to_owned())
            .unwrap();
        responder.await.unwrap();

        let failed = publication.fail();
        assert_eq!(failed.message_seal_index, None);
        assert_eq!(failed.function_calls.len(), 1);
        assert_eq!(failed.function_calls[0].call_index(), 0);
        assert_eq!(failed.function_calls[0].seal_index(), 2);

        let mut events = Vec::new();
        loop {
            match subscriber.recv().await.unwrap() {
                LiveResponseDelivery::Publication(frame) => {
                    let sequence = u64::try_from(events.len()).unwrap();
                    events.push(serde_json::to_value(frame.into_public_event(sequence)).unwrap());
                }
                LiveResponseDelivery::Seal(seal) => {
                    assert_eq!(seal.identity().item_id(), "fc_failed_request");
                    assert_eq!(seal.last_local_sequence(), Some(2));
                    assert_eq!(seal.status(), LiveResponseSealStatus::Incomplete);
                    break;
                }
                LiveResponseDelivery::Gap(_) => panic!("failure close must not lose events"),
            }
        }
        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["type"], "response.output_item.added");
        assert_eq!(events[1]["type"], "response.function_call_arguments.delta");
        assert_eq!(events[2]["type"], "response.output_item.done");
        assert_eq!(events[2]["item"]["status"], "incomplete");
        assert_eq!(events[2]["item"]["arguments"], "");
        assert!(events
            .iter()
            .all(|event| { event["type"] != "response.function_call_arguments.done" }));
    }

    #[tokio::test]
    async fn llm_finish_reason_matrix_never_completes_partial_or_invalid_output() {
        struct Case {
            label: &'static str,
            provider_reason: Option<&'static str>,
            expected_finish: ModelFinishReason,
            expected_error: Option<&'static str>,
        }

        let cases = [
            Case {
                label: "stop",
                provider_reason: Some("stop"),
                expected_finish: ModelFinishReason::Stop,
                expected_error: None,
            },
            Case {
                label: "tool_calls",
                provider_reason: Some("tool_calls"),
                expected_finish: ModelFinishReason::Invalid,
                expected_error: Some(LLM_TOOL_CALL_INVALID),
            },
            Case {
                label: "length",
                provider_reason: Some("length"),
                expected_finish: ModelFinishReason::Length,
                expected_error: Some(MODEL_OUTPUT_TRUNCATED),
            },
            Case {
                label: "content_filter",
                provider_reason: Some("content_filter"),
                expected_finish: ModelFinishReason::ContentFilter,
                expected_error: Some(MODEL_OUTPUT_FILTERED),
            },
            Case {
                label: "missing",
                provider_reason: None,
                expected_finish: ModelFinishReason::Invalid,
                expected_error: Some(MODEL_FINISH_REASON_INVALID),
            },
            Case {
                label: "unknown",
                provider_reason: Some("provider_private_reason"),
                expected_finish: ModelFinishReason::Invalid,
                expected_error: Some(MODEL_FINISH_REASON_INVALID),
            },
        ];

        for stream_requested in [true, false] {
            for case in &cases {
                let mut models = ModelRegistry::default();
                models
                    .register_versioned(
                        "chat",
                        ModelDeploymentIdentity::new(
                            "model-worker-1",
                            json!({"adapter": "finish-reason-matrix"}),
                        )
                        .unwrap(),
                        FinishReasonModel {
                            finish_reason: case.provider_reason.map(str::to_owned),
                        },
                    )
                    .unwrap();
                let broker = Arc::new(InMemoryLiveResponseBroker::new(32, 8).unwrap());
                let mut subscriber = broker
                    .subscribe(RunId::new("run_leaf").unwrap())
                    .await
                    .unwrap();
                let executor = LlmTaskExecutor::new(models)
                    .with_live_response_broker(broker as Arc<dyn LiveResponseBroker>);
                let request = dispatch_request(
                    SchedulerTaskKind::Llm,
                    "core.llm",
                    "model-worker-1",
                    finish_reason_configuration(stream_requested),
                    Vec::new(),
                    PlanType::String,
                );

                let result = executor
                    .execute(&public_llm_context(1), &request, CancellationToken::new())
                    .await;
                let (event_types, seal_status) = collect_publication_types(&mut subscriber).await;
                let mode = if stream_requested {
                    "streaming"
                } else {
                    "complete"
                };

                if let Some(expected_error) = case.expected_error {
                    let failure = result.unwrap_err();
                    assert_eq!(failure.code(), expected_error, "{} {mode}", case.label);
                    let completion = failure
                        .model_call()
                        .expect("failed provider call must retain finish telemetry");
                    assert_eq!(
                        completion.finish_reason(),
                        case.expected_finish,
                        "{} {mode}",
                        case.label
                    );
                    assert!(
                        completion.safe_public_item().is_none(),
                        "{} {mode} must not commit a successful output item",
                        case.label
                    );
                    assert_eq!(
                        seal_status,
                        LiveResponseSealStatus::Incomplete,
                        "{} {mode}",
                        case.label
                    );
                    assert!(
                        !event_types.contains(&ResponseStreamEventType::ResponseOutputTextDone),
                        "{} {mode} emitted output_text.done for incomplete text",
                        case.label
                    );
                    assert!(
                        !event_types.contains(&ResponseStreamEventType::ResponseContentPartDone),
                        "{} {mode} emitted a completed content part",
                        case.label
                    );
                } else {
                    let result = result.unwrap();
                    assert_eq!(
                        result.outputs().values().next().unwrap().value(),
                        &json!("partial answer"),
                        "{} {mode}",
                        case.label
                    );
                    let completion = result
                        .model_call()
                        .expect("successful provider call must retain finish telemetry");
                    assert_eq!(completion.finish_reason(), case.expected_finish);
                    assert!(completion.safe_public_item().is_some());
                    assert_eq!(seal_status, LiveResponseSealStatus::Completed);
                    assert!(event_types.contains(&ResponseStreamEventType::ResponseOutputTextDone));
                    assert!(event_types.contains(&ResponseStreamEventType::ResponseContentPartDone));
                }

                assert!(
                    !event_types.contains(&ResponseStreamEventType::ResponseCompleted),
                    "leaf execution must not fabricate workflow completion"
                );
            }
        }
    }

    #[tokio::test]
    async fn llm_adapter_splices_real_message_arrays_and_renders_pinned_prompts_once() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let model = CapturingModel {
            requests: requests.clone(),
            response: "answer".to_owned(),
        };
        let mut models = ModelRegistry::default();
        models
            .register_versioned(
                "chat",
                ModelDeploymentIdentity::new("model-worker-1", json!({"model": "fixed"})).unwrap(),
                model,
            )
            .unwrap();
        let message = |role: &str, content: DescriptorValue| {
            object([
                ("kind", string("message")),
                ("role", string(role)),
                ("content", DescriptorValue::Array(vec![content])),
            ])
        };
        let text = |kind: &str, source: &str| {
            object([
                ("kind", string(kind)),
                ("references", DescriptorValue::Array(Vec::new())),
                ("text", string(source)),
            ])
        };
        let configuration = BTreeMap::from([
            ("model".to_owned(), string("chat")),
            (
                "parameters".to_owned(),
                object([(
                    "temperature",
                    DescriptorValue::Number(serde_json::Number::from_f64(0.2).unwrap()),
                )]),
            ),
            (
                "runtime_bindings".to_owned(),
                object([
                    ("history", string("input_history")),
                    ("question", string("input_question")),
                ]),
            ),
            (
                "prompt_catalog".to_owned(),
                object([(
                    "system",
                    object([("content", string("Policy for {{ question }}"))]),
                )]),
            ),
            (
                "message_program".to_owned(),
                DescriptorValue::Array(vec![
                    message("system", text("prompt_ref", "system")),
                    object([
                        ("kind", string("message_splice")),
                        ("path", string("history")),
                    ]),
                    message("user", text("template", "Question: {{ question }}")),
                ]),
            ),
        ]);
        let inputs = vec![
            insight_engine::internal::bound_task_input(
                DataPortId::new("input_history").unwrap(),
                PortName::new("history").unwrap(),
                RuntimeValue::new(json!([
                    {
                        "role": "user",
                        "content": [{"text": "earlier question"}]
                    },
                    {
                        "role": "assistant",
                        "content": [{"text": "prior"}]
                    }
                ]))
                .unwrap(),
            ),
            insight_engine::internal::bound_task_input(
                DataPortId::new("input_question").unwrap(),
                PortName::new("question").unwrap(),
                RuntimeValue::new(json!("What now?")).unwrap(),
            ),
        ];
        let request = dispatch_request(
            SchedulerTaskKind::Llm,
            "core.llm",
            "model-worker-1",
            configuration,
            inputs,
            PlanType::String,
        );
        let result = LlmTaskExecutor::new(models)
            .execute(&worker_context(1), &request, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            result.outputs().values().next().unwrap().value(),
            &json!("answer")
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests[0].messages.len(), 4);
        assert_eq!(requests[0].messages[0].text(), Some("Policy for What now?"));
        assert_eq!(requests[0].messages[1].text(), Some("earlier question"));
        assert_eq!(requests[0].messages[2].text(), Some("prior"));
        assert_eq!(requests[0].messages[3].text(), Some("Question: What now?"));
    }

    #[tokio::test]
    async fn llm_adapter_omits_absent_optional_images_and_rejects_present_invalid_values() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let model = CapturingModel {
            requests: requests.clone(),
            response: "answer".to_owned(),
        };
        let mut models = ModelRegistry::default();
        models
            .register_versioned(
                "chat",
                ModelDeploymentIdentity::new("model-worker-1", json!({"model": "fixed"})).unwrap(),
                model,
            )
            .unwrap();
        let configuration = BTreeMap::from([
            ("model".to_owned(), string("chat")),
            ("parameters".to_owned(), object([])),
            (
                "runtime_bindings".to_owned(),
                object([("image_url", string("input_image_url"))]),
            ),
            (
                "optional_runtime_bindings".to_owned(),
                DescriptorValue::Array(vec![string("image_url")]),
            ),
            (
                "message_program".to_owned(),
                DescriptorValue::Array(vec![object([
                    ("kind", string("message")),
                    ("role", string("user")),
                    (
                        "content",
                        DescriptorValue::Array(vec![
                            object([
                                ("kind", string("literal")),
                                ("references", DescriptorValue::Array(Vec::new())),
                                ("text", string("describe the image")),
                            ]),
                            object([
                                ("kind", string("value_ref")),
                                ("image_url", string("image_url")),
                            ]),
                        ]),
                    ),
                ])]),
            ),
        ]);
        let executor = LlmTaskExecutor::new(models);

        let missing = dispatch_request(
            SchedulerTaskKind::Llm,
            "core.llm",
            "model-worker-1",
            configuration.clone(),
            Vec::new(),
            PlanType::String,
        );
        executor
            .execute(&worker_context(1), &missing, CancellationToken::new())
            .await
            .unwrap();

        let empty = dispatch_request(
            SchedulerTaskKind::Llm,
            "core.llm",
            "model-worker-1",
            configuration.clone(),
            vec![insight_engine::internal::bound_task_input(
                DataPortId::new("input_image_url").unwrap(),
                PortName::new("image_url").unwrap(),
                RuntimeValue::new(json!("")).unwrap(),
            )],
            PlanType::String,
        );
        assert_eq!(
            executor
                .execute(&worker_context(2), &empty, CancellationToken::new())
                .await
                .unwrap_err()
                .code(),
            LLM_MESSAGE_INVALID,
        );

        {
            let captured = requests.lock().unwrap();
            assert!(captured[0].messages[0].image_urls().is_empty());
            assert_eq!(captured[0].messages[0].text(), Some("describe the image"));
        }

        let explicit_null = dispatch_request(
            SchedulerTaskKind::Llm,
            "core.llm",
            "model-worker-1",
            configuration,
            vec![insight_engine::internal::bound_task_input(
                DataPortId::new("input_image_url").unwrap(),
                PortName::new("image_url").unwrap(),
                RuntimeValue::new(Value::Null).unwrap(),
            )],
            PlanType::String,
        );
        assert_eq!(
            executor
                .execute(&worker_context(3), &explicit_null, CancellationToken::new(),)
                .await
                .unwrap_err()
                .code(),
            LLM_BINDING_INVALID
        );
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn lost_or_lagged_token_observations_do_not_change_the_final_worker_result() {
        let mut models = ModelRegistry::default();
        models
            .register_versioned(
                "chat",
                ModelDeploymentIdentity::new("model-worker-1", json!({"adapter": "chunked-test"}))
                    .unwrap(),
                ChunkedModel,
            )
            .unwrap();
        let configuration = BTreeMap::from([
            ("model".to_owned(), string("chat")),
            ("parameters".to_owned(), object([])),
            (
                "message_program".to_owned(),
                DescriptorValue::Array(vec![object([
                    ("kind", string("message")),
                    ("role", string("user")),
                    (
                        "content",
                        DescriptorValue::Array(vec![object([
                            ("kind", string("literal")),
                            ("references", DescriptorValue::Array(Vec::new())),
                            ("text", string("question")),
                        ])]),
                    ),
                ])]),
            ),
        ]);
        let request = dispatch_request(
            SchedulerTaskKind::Llm,
            "core.llm",
            "model-worker-1",
            configuration,
            Vec::new(),
            PlanType::String,
        );

        // Capacity one forces the receiver to lag as three chunks arrive.
        // Lag only describes best-effort observation loss; the worker result
        // must still contain the complete validated response.
        let (observer, mut receiver) = broadcast::channel(1);
        let result = LlmTaskExecutor::new(models.clone())
            .with_token_observer(observer)
            .execute(&worker_context(1), &request, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            result.outputs().values().next().unwrap().value(),
            &json!("durable final answer")
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(_))
        ));
        assert_eq!(receiver.try_recv().unwrap().text(), "answer");

        // A completely detached observer is also non-authoritative.
        let (observer, receiver) = broadcast::channel(1);
        drop(receiver);
        let result = LlmTaskExecutor::new(models)
            .with_token_observer(observer)
            .execute(&worker_context(2), &request, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            result.outputs().values().next().unwrap().value(),
            &json!("durable final answer")
        );
    }

    #[tokio::test]
    async fn action_adapter_passes_stable_effect_id_and_real_object_input() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut actions = ActionRegistry::default();
        actions
            .register(CapturingAction {
                calls: calls.clone(),
            })
            .unwrap();
        let configuration = BTreeMap::from([
            ("call".to_owned(), string("example.capture")),
            ("inputs".to_owned(), object([("text", string("$payload"))])),
            (
                "runtime_bindings".to_owned(),
                object([("payload", string("input_payload"))]),
            ),
        ]);
        let output_type = PlanType::Object {
            properties: BTreeMap::from([(
                "text".to_owned(),
                PlanProperty::new(PlanType::String, true).unwrap(),
            )]),
            additional_properties: None,
        };
        let deployment_binding =
            frozen_action_binding(actions.resolve("example.capture").unwrap().as_ref());
        let request = dispatch_request_with_frozen_bindings(
            SchedulerTaskKind::Action,
            "example.capture",
            "1.2.3",
            configuration,
            vec![insight_engine::internal::bound_task_input(
                DataPortId::new("input_payload").unwrap(),
                PortName::new("payload").unwrap(),
                RuntimeValue::new(json!("real value")).unwrap(),
            )],
            output_type,
            Vec::new(),
            Some(deployment_binding),
        );
        let executor = ActionTaskExecutor::new(actions);
        executor
            .execute(&worker_context(2), &request, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            &[(json!({"text": "real value"}), "effect_stable".to_owned(), 2)]
        );

        let mut tampered = serde_json::to_value(&request).unwrap();
        tampered["deployment_binding"]["descriptor_hash"] = json!("different-descriptor");
        let tampered = serde_json::from_value::<TaskExecutionRequest>(tampered).unwrap();
        assert_eq!(
            executor
                .execute(&worker_context(3), &tampered, CancellationToken::new(),)
                .await
                .unwrap_err()
                .code(),
            ACTION_DESCRIPTOR_INVALID
        );
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn synthetic_model_tool_request_uses_frozen_action_authority_and_literal_json() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut actions = ActionRegistry::default();
        actions
            .register(CapturingAction {
                calls: calls.clone(),
            })
            .unwrap();
        let registered = actions.resolve("example.capture").unwrap();
        let descriptor = registered.descriptor();
        let identity = registered.identity();
        let effect_policy = WorkerEffectPolicy::new(
            EffectIdempotency::Idempotent,
            1,
            WorkerCancellation::Cooperative,
        )
        .unwrap();
        let deployment_binding = frozen_action_binding(registered.as_ref());
        let frozen_action = parse_action_from_stored_evidence(
            identity.id.clone(),
            identity.id.clone(),
            identity.version.to_string(),
            identity.descriptor_hash.clone(),
            descriptor.input_schema.clone(),
            descriptor.output_schema.clone(),
            effect_policy,
            deployment_binding,
            json!({
                "call": false,
                "arguments": "private",
                "result": null
            }),
        )
        .unwrap();
        let run_id = RunId::new("run_synthetic_tool").unwrap();
        let parent_activation = ActivationId::new("activation_synthetic_parent").unwrap();
        let tool_identity = deterministic_tool_identity(
            &run_id,
            &parent_activation,
            AttemptNo::FIRST,
            1,
            0,
            "call_capture",
            frozen_action,
            None,
            None,
            None,
        )
        .unwrap();
        let claim = model_tool_task_claim_new(
            run_id,
            parent_activation,
            AttemptNo::FIRST,
            1,
            tool_identity,
            json!({"text": "$literal-tool-result"}),
            AttemptNo::new(2).unwrap(),
            LeaseEpoch::new(2).unwrap(),
            "synthetic-tool-fence".to_owned(),
            "worker-a".to_owned(),
            "synthetic-claim-token".to_owned(),
            Utc::now() + chrono::Duration::minutes(1),
            1,
        )
        .unwrap();
        let request = TaskExecutionRequest::from_model_tool_claim(&claim).unwrap();
        let context = WorkerExecutionContext::new(
            claim.tool_attempt_no(),
            claim.lease_epoch(),
            claim.fencing_token(),
            claim.claim_expires_at(),
        )
        .unwrap();
        let executor = ActionTaskExecutor::new(actions);
        executor
            .execute(&context, &request, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            &[(
                json!({"text": "$literal-tool-result"}),
                claim.identity().effect_id().as_str().to_owned(),
                2,
            )]
        );

        let mut tampered_binding = serde_json::to_value(&request).unwrap();
        tampered_binding["deployment_binding"]["descriptor_hash"] = json!("c".repeat(64));
        let tampered_binding =
            serde_json::from_value::<TaskExecutionRequest>(tampered_binding).unwrap();
        assert_eq!(
            executor
                .execute(&context, &tampered_binding, CancellationToken::new())
                .await
                .unwrap_err()
                .code(),
            ACTION_DESCRIPTOR_INVALID
        );

        let mut extra_model_field = serde_json::to_value(&request).unwrap();
        extra_model_field["public_configuration"]["model_call_no"] =
            serde_json::to_value(DescriptorValue::Integer(1)).unwrap();
        let extra_model_field =
            serde_json::from_value::<TaskExecutionRequest>(extra_model_field).unwrap();
        assert_eq!(
            executor
                .execute(&context, &extra_model_field, CancellationToken::new())
                .await
                .unwrap_err()
                .code(),
            ACTION_BINDING_INVALID
        );
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn synthetic_model_tool_injects_server_input_only_inside_the_action_boundary() {
        for fail_with_secret in [false, true] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let mut actions = ActionRegistry::default();
            actions
                .register(ServerInjectingCapturingAction {
                    calls: Arc::clone(&calls),
                    fail_with_secret,
                })
                .unwrap();
            let registered = actions.resolve("example.server_injecting_capture").unwrap();
            let descriptor = registered.descriptor();
            let identity = registered.identity();
            let effect_policy = WorkerEffectPolicy::frozen(
                WorkerEffectClass::Pure,
                EffectIdempotency::Idempotent,
                1,
                0,
                0,
                60_000,
                WorkerCancellation::Cooperative,
            )
            .unwrap();
            let deployment_binding = frozen_action_binding(registered.as_ref());
            let frozen_action = parse_action_from_stored_evidence(
                identity.id.clone(),
                identity.id.clone(),
                identity.version.to_string(),
                identity.descriptor_hash.clone(),
                descriptor.input_schema.clone(),
                descriptor.output_schema.clone(),
                effect_policy,
                deployment_binding,
                json!({
                    "call": true,
                    "arguments": "all",
                    "result": null
                }),
            )
            .unwrap();
            let run_id = RunId::new("run_server_injection").unwrap();
            let parent_activation = ActivationId::new("activation_server_injection").unwrap();
            let safe_arguments = json!({"text": "model-visible"});
            let public_arguments_jcs = serde_jcs::to_string(&safe_arguments).unwrap();
            let tool_identity = deterministic_tool_identity(
                &run_id,
                &parent_activation,
                AttemptNo::FIRST,
                1,
                0,
                "call_server_injection",
                frozen_action,
                Some(ResponseItemAuthority::new("item_server_injection", 0).unwrap()),
                Some(public_arguments_jcs),
                Some(3),
            )
            .unwrap();
            let claim = model_tool_task_claim_new(
                run_id,
                parent_activation,
                AttemptNo::FIRST,
                1,
                tool_identity,
                safe_arguments.clone(),
                AttemptNo::FIRST,
                LeaseEpoch::new(1).unwrap(),
                "server-injection-fence".to_owned(),
                "worker-a".to_owned(),
                "server-injection-claim".to_owned(),
                Utc::now() + chrono::Duration::minutes(1),
                1,
            )
            .unwrap();
            let request = TaskExecutionRequest::from_model_tool_claim(&claim).unwrap();

            // Every durable/replayable worker representation remains exactly
            // the safe model object. The sentinel does not exist until the
            // process-local Action hook runs.
            assert_eq!(claim.arguments(), &safe_arguments);
            assert_eq!(
                claim.identity().public_arguments_jcs(),
                Some(r#"{"text":"model-visible"}"#)
            );
            assert!(!serde_json::to_string(&request)
                .unwrap()
                .contains(MODEL_TOOL_SERVER_SECRET_SENTINEL));
            assert!(!format!("{request:?}").contains(MODEL_TOOL_SERVER_SECRET_SENTINEL));

            let context = WorkerExecutionContext::new(
                claim.tool_attempt_no(),
                claim.lease_epoch(),
                claim.fencing_token(),
                claim.claim_expires_at(),
            )
            .unwrap();
            let executor = ActionTaskExecutor::new(actions);

            let mut identity_drift = serde_json::to_value(&request).unwrap();
            identity_drift["deployment_binding"]["descriptor_hash"] = json!("f".repeat(64));
            let identity_drift =
                serde_json::from_value::<TaskExecutionRequest>(identity_drift).unwrap();
            assert_eq!(
                executor
                    .execute(&context, &identity_drift, CancellationToken::new())
                    .await
                    .unwrap_err()
                    .code(),
                ACTION_DESCRIPTOR_INVALID
            );
            assert!(calls.lock().unwrap().is_empty());

            let mut invalid_model_input = serde_json::to_value(&request).unwrap();
            invalid_model_input["public_configuration"]["inputs"] = serde_json::to_value(object([
                ("text", string("model-visible")),
                ("server_token", string("model-forged")),
            ]))
            .unwrap();
            let invalid_model_input =
                serde_json::from_value::<TaskExecutionRequest>(invalid_model_input).unwrap();
            assert_eq!(
                executor
                    .execute(&context, &invalid_model_input, CancellationToken::new())
                    .await
                    .unwrap_err()
                    .code(),
                ACTION_EXECUTION_FAILED
            );
            assert!(calls.lock().unwrap().is_empty());

            let execution = executor
                .execute(&context, &request, CancellationToken::new())
                .await;

            let captured = calls.lock().unwrap();
            assert_eq!(captured.len(), 1);
            assert_eq!(captured[0]["text"], "model-visible");
            assert_eq!(
                captured[0]["server_token"],
                MODEL_TOOL_SERVER_SECRET_SENTINEL
            );
            drop(captured);

            if fail_with_secret {
                let failure = execution.unwrap_err();
                assert_eq!(failure.code(), ACTION_EXECUTION_FAILED);
                assert!(!format!("{failure:?}").contains(MODEL_TOOL_SERVER_SECRET_SENTINEL));
            } else {
                let result = execution.unwrap();
                assert_eq!(
                    result.outputs().values().next().unwrap().value(),
                    &safe_arguments
                );
                assert!(!serde_json::to_string(&result.outputs())
                    .unwrap()
                    .contains(MODEL_TOOL_SERVER_SECRET_SENTINEL));
                assert!(!format!("{result:?}").contains(MODEL_TOOL_SERVER_SECRET_SENTINEL));
            }
        }
    }
}
