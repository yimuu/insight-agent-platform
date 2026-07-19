//! Production adapters from immutable v3 leaf requests to model and Action
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

use crate::{
    catalog_v3::VersionedLeafAdapterRegistry,
    dsl::{v3::template::compile_template, CompileError},
    resources::{
        actions::{ActionContext, ActionRegistry, EffectClass, IdempotencyClass},
        models::{
            model_response_too_large, select_structured_output_capability, ChatContent,
            ChatContentPart, ChatMessage, ChatRequest, ChatResponseFormat, ChatRole,
            ModelCapability, ModelRegistry,
        },
    },
    runtime::{stop_pair, ExecutionControl, RunError, RunErrorKind, StopReason},
};

use super::{
    plan::{DescriptorValue, PlanType, VersionTag},
    worker::{
        LeafTaskExecutor, TaskExecutionRequest, TaskExecutionResult, WorkerExecutionContext,
        WorkerExecutorRegistry, WorkerFailure, WorkerFailureClass,
    },
    EffectEvidence, EffectIdempotency, RuntimeValue, SchedulerTaskKind, TaskOutputContract,
    WorkerCancellation, WorkerEffectClass, WorkerEffectPolicy,
};

const V3_DESCRIPTOR_VERSION: &str = "1";
const MAX_REQUEST_BYTES: usize = 1_048_576;
const MAX_TEMPLATE_BYTES: usize = 1_048_576;

const LLM_DESCRIPTOR_INVALID: &str = "V3_LLM_DESCRIPTOR_INVALID";
const LLM_BINDING_INVALID: &str = "V3_LLM_BINDING_INVALID";
const LLM_MESSAGE_INVALID: &str = "V3_LLM_MESSAGE_INVALID";
const LLM_REQUEST_TOO_LARGE: &str = "V3_LLM_REQUEST_TOO_LARGE";
const LLM_RESPONSE_INVALID: &str = "V3_LLM_RESPONSE_INVALID";
const LLM_PROVIDER_FAILED: &str = "V3_LLM_PROVIDER_FAILED";
const ACTION_DESCRIPTOR_INVALID: &str = "V3_ACTION_DESCRIPTOR_INVALID";
const ACTION_BINDING_INVALID: &str = "V3_ACTION_BINDING_INVALID";
const ACTION_EXECUTION_FAILED: &str = "V3_ACTION_EXECUTION_FAILED";
const WORKER_CANCELLED: &str = "WORKER_CANCELLED";
const WORKER_DEADLINE_EXCEEDED: &str = "WORKER_DEADLINE_EXCEEDED";

#[derive(Clone)]
pub struct V3LlmTaskExecutor {
    models: ModelRegistry,
    token_observer: Option<broadcast::Sender<LlmTokenObservation>>,
}

impl V3LlmTaskExecutor {
    pub fn new(models: ModelRegistry) -> Self {
        Self {
            models,
            token_observer: None,
        }
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
    run_id: super::RunId,
    activation_id: super::ActivationId,
    attempt_no: super::AttemptNo,
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

    pub fn run_id(&self) -> &super::RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &super::ActivationId {
        &self.activation_id
    }

    pub fn attempt_no(&self) -> super::AttemptNo {
        self.attempt_no
    }

    pub fn text(&self) -> &str {
        &self.text
    }
}

#[async_trait]
impl LeafTaskExecutor for V3LlmTaskExecutor {
    async fn execute(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        if request.task_kind() != SchedulerTaskKind::Llm || request.implementation() != "core.llm" {
            return Err(invariant(LLM_DESCRIPTOR_INVALID));
        }
        let configuration = request.public_configuration();
        let model_alias = descriptor_string(required(configuration, "model")?)?;
        let model = self
            .models
            .resolve(model_alias)
            .map_err(|_| invariant(LLM_DESCRIPTOR_INVALID))?;
        let parameters = configuration
            .get("parameters")
            .map(descriptor_json)
            .transpose()?
            .unwrap_or_else(|| Value::Object(Map::new()));
        model
            .validate_parameters(&parameters)
            .map_err(|_| invariant(LLM_DESCRIPTOR_INVALID))?;

        let bindings = RuntimeBindings::new(configuration, request)?;
        let messages = render_messages(configuration, &bindings)?;
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
        };
        if !model.request_body_within_limit(&chat_request, MAX_REQUEST_BYTES) {
            return Err(infrastructure(LLM_REQUEST_TOO_LARGE, false));
        }

        require_live(context, &cancellation)?;
        let stream_future = model.stream_chat(chat_request);
        tokio::pin!(stream_future);
        let mut stream = tokio::select! {
            result = &mut stream_future => result.map_err(map_llm_error)?,
            _ = cancellation.cancelled() => return Err(control(WORKER_CANCELLED)),
            _ = sleep(remaining(context)?) => return Err(control(WORKER_DEADLINE_EXCEEDED)),
        };
        let mut text = String::new();
        let max_text_bytes = model.max_accumulated_text_bytes();
        loop {
            let chunk = tokio::select! {
                value = stream.next() => value,
                _ = cancellation.cancelled() => return Err(control(WORKER_CANCELLED)),
                _ = sleep(remaining(context)?) => return Err(control(WORKER_DEADLINE_EXCEEDED)),
            };
            let Some(chunk) = chunk else { break };
            let chunk = chunk.map_err(map_llm_error)?;
            if text.len().saturating_add(chunk.text.len()) > max_text_bytes {
                return Err(map_llm_error(model_response_too_large()));
            }
            if let Some(observer) = &self.token_observer {
                // Receiver closure and lag are observational loss, not worker
                // failure. The authoritative result is the validated value
                // returned below and durably committed by the repository.
                let _ = observer.send(LlmTokenObservation::new(
                    request,
                    context,
                    chunk.text.clone(),
                ));
            }
            text.push_str(&chunk.text);
        }

        let value = if output.value_type().string_constraints().is_some() {
            Value::String(text)
        } else {
            serde_json::from_str(&text).map_err(|_| infrastructure(LLM_RESPONSE_INVALID, false))?
        };
        let value = RuntimeValue::new(value).map_err(|_| invariant(LLM_RESPONSE_INVALID))?;
        if !value.matches(output.value_type()) {
            return Err(infrastructure(LLM_RESPONSE_INVALID, false));
        }
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(output.port_id().clone(), value)]),
            EffectEvidence::Committed,
        ))
    }
}

#[derive(Clone)]
pub struct V3ActionTaskExecutor {
    actions: ActionRegistry,
}

impl V3ActionTaskExecutor {
    pub fn new(actions: ActionRegistry) -> Self {
        Self { actions }
    }
}

#[async_trait]
impl LeafTaskExecutor for V3ActionTaskExecutor {
    async fn execute(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        if request.task_kind() != SchedulerTaskKind::Action {
            return Err(invariant(ACTION_DESCRIPTOR_INVALID));
        }
        let action = self
            .actions
            .resolve(request.implementation())
            .map_err(|_| invariant(ACTION_DESCRIPTOR_INVALID))?;
        let descriptor = action.descriptor();
        validate_frozen_action_policy(descriptor, request.effect_policy())?;
        let bindings = RuntimeBindings::new(request.public_configuration(), request)?;
        let input = request
            .public_configuration()
            .get("inputs")
            .map(|value| substitute_descriptor_value(value, &bindings))
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
        let action_context = ActionContext::for_durable_effect(
            request.run_id().as_str(),
            request.node_id().as_str(),
            context.attempt_no().get(),
            request.effect_id().as_str(),
            control,
        );
        let call = action.call(input, action_context);
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
    production_worker_registry_inner(models, actions, None)
}

/// Registers built-in model/Action workers plus every exact versioned HTTP or
/// Tool adapter used by the production deployment resolver.
pub fn production_worker_registry_with_leaf_adapters(
    models: &ModelRegistry,
    actions: &ActionRegistry,
    external_leaf_adapters: &VersionedLeafAdapterRegistry,
) -> Result<WorkerExecutorRegistry, CompileError> {
    production_worker_registry_inner(models, actions, Some(external_leaf_adapters))
}

fn production_worker_registry_inner(
    models: &ModelRegistry,
    actions: &ActionRegistry,
    external_leaf_adapters: Option<&VersionedLeafAdapterRegistry>,
) -> Result<WorkerExecutorRegistry, CompileError> {
    let descriptor_version = VersionTag::new(V3_DESCRIPTOR_VERSION)
        .map_err(|error| CompileError::new("WORKER_REGISTRY_INVALID", error.to_string()))?;
    let mut registry = WorkerExecutorRegistry::new();
    let mut llm_versions = BTreeSet::new();
    for alias in models.names() {
        let identity = models.deployment_identity(alias)?;
        if llm_versions.insert(identity.worker_version().to_owned()) {
            registry
                .register(
                    SchedulerTaskKind::Llm,
                    "core.llm",
                    descriptor_version.clone(),
                    VersionTag::new(identity.worker_version()).map_err(|error| {
                        CompileError::new("WORKER_REGISTRY_INVALID", error.to_string())
                    })?,
                    Arc::new(V3LlmTaskExecutor::new(models.clone())),
                )
                .map_err(|code| CompileError::new(code, "failed to register v3 LLM worker"))?;
        }
    }
    for action_id in actions.names() {
        let action = actions.resolve(action_id)?;
        registry
            .register(
                SchedulerTaskKind::Action,
                action_id,
                descriptor_version.clone(),
                VersionTag::new(action.identity().version.to_string()).map_err(|error| {
                    CompileError::new("WORKER_REGISTRY_INVALID", error.to_string())
                })?,
                Arc::new(V3ActionTaskExecutor::new(actions.clone())),
            )
            .map_err(|code| CompileError::new(code, "failed to register v3 Action worker"))?;
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
                messages.push(ChatMessage {
                    role,
                    content: ChatContent::Parts(parts),
                });
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
    Ok(ChatMessage {
        role,
        content: ChatContent::Parts(parts),
    })
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
    match error.kind() {
        RunErrorKind::Stop => control(WORKER_CANCELLED),
        RunErrorKind::Timeout => control(WORKER_DEADLINE_EXCEEDED),
        RunErrorKind::Operation | RunErrorKind::Infrastructure => {
            infrastructure(LLM_PROVIDER_FAILED, true)
        }
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
    descriptor: &crate::resources::actions::ActionDescriptor,
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
        crate::resources::actions::CancellationClass::Cooperative => {
            WorkerCancellation::Cooperative
        }
        crate::resources::actions::CancellationClass::NotSupported => WorkerCancellation::LeaseOnly,
    };
    if policy.effect_class() != effect_class
        || policy.effect_idempotency() != idempotency
        || policy.cancellation() != cancellation
    {
        return Err(invariant(ACTION_DESCRIPTOR_INVALID));
    }
    Ok(())
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
    use serde_json::{json, Value};

    use super::*;
    use crate::{
        engine::{
            plan::{DataPortId, PlanProperty, PortName},
            scheduler::{
                BoundTaskInput, SchedulerAction, SchedulerCheckpointId, SchedulerIntent,
                SchedulerTaskId, TaskOutputContract,
            },
            ActivationId, AttemptNo, EffectId, EffectIdempotency, LeaseEpoch, NodeId, RunId,
            WorkerCancellation, WorkerEffectPolicy,
        },
        resources::{
            actions::{Action, ActionDescriptor, CancellationClass, IdempotencyClass},
            models::{ChatChunk, ChatModel, ChatStream, ModelDeploymentIdentity},
        },
        runtime::RunError,
    };

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

    #[derive(Clone)]
    struct CapturingAction {
        calls: Arc<Mutex<Vec<(Value, String, u32)>>>,
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

    fn dispatch_request(
        task_kind: SchedulerTaskKind,
        implementation: &str,
        worker_version: &str,
        configuration: BTreeMap<String, DescriptorValue>,
        inputs: Vec<BoundTaskInput>,
        output_type: PlanType,
    ) -> TaskExecutionRequest {
        let action = SchedulerAction::DispatchTask {
            task_id: SchedulerTaskId::parse(format!("task_{}", "1".repeat(64))).unwrap(),
            effect_id: EffectId::new("effect_stable").unwrap(),
            activation_id: ActivationId::new("activation_leaf").unwrap(),
            node_id: NodeId::new("leaf").unwrap(),
            admission_class: crate::engine::TaskAdmissionClass::Normal,
            task_kind,
            implementation: implementation.to_owned(),
            descriptor_version: version("1"),
            worker_version: version(worker_version),
            effect_policy: WorkerEffectPolicy::new(
                EffectIdempotency::Idempotent,
                1,
                WorkerCancellation::Cooperative,
            )
            .unwrap(),
            public_configuration: configuration,
            secret_configuration: BTreeMap::new(),
            inputs,
            outputs: vec![TaskOutputContract::new(
                DataPortId::new("leaf_result").unwrap(),
                PortName::new("result").unwrap(),
                output_type,
                true,
            )],
        };
        let intent = SchedulerIntent::new(
            RunId::new("run_leaf").unwrap(),
            SchedulerCheckpointId::parse(format!("checkpoint_{}", "2".repeat(64))).unwrap(),
            action,
        );
        TaskExecutionRequest::from_scheduler_intent(&intent).unwrap()
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
            BoundTaskInput::new(
                DataPortId::new("input_history").unwrap(),
                PortName::new("history").unwrap(),
                RuntimeValue::new(json!([{
                    "role": "assistant",
                    "content": [{"text": "prior"}]
                }]))
                .unwrap(),
            ),
            BoundTaskInput::new(
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
        let result = V3LlmTaskExecutor::new(models)
            .execute(&worker_context(1), &request, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            result.outputs().values().next().unwrap().value(),
            &json!("answer")
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests[0].messages.len(), 3);
        assert_eq!(requests[0].messages[0].text(), Some("Policy for What now?"));
        assert_eq!(requests[0].messages[1].text(), Some("prior"));
        assert_eq!(requests[0].messages[2].text(), Some("Question: What now?"));
    }

    #[tokio::test]
    async fn llm_adapter_omits_only_absent_optional_images() {
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
        let executor = V3LlmTaskExecutor::new(models);

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
            vec![BoundTaskInput::new(
                DataPortId::new("input_image_url").unwrap(),
                PortName::new("image_url").unwrap(),
                RuntimeValue::new(json!("")).unwrap(),
            )],
            PlanType::String,
        );
        executor
            .execute(&worker_context(2), &empty, CancellationToken::new())
            .await
            .unwrap();

        {
            let captured = requests.lock().unwrap();
            assert!(captured[0].messages[0].image_urls().is_empty());
            assert_eq!(captured[0].messages[0].text(), Some("describe the image"));
            assert_eq!(captured[1].messages[0].image_urls(), vec![""]);
        }

        let explicit_null = dispatch_request(
            SchedulerTaskKind::Llm,
            "core.llm",
            "model-worker-1",
            configuration,
            vec![BoundTaskInput::new(
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
        assert_eq!(requests.lock().unwrap().len(), 2);
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
        let result = V3LlmTaskExecutor::new(models.clone())
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
        let result = V3LlmTaskExecutor::new(models)
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
        let request = dispatch_request(
            SchedulerTaskKind::Action,
            "example.capture",
            "1.2.3",
            configuration,
            vec![BoundTaskInput::new(
                DataPortId::new("input_payload").unwrap(),
                PortName::new("payload").unwrap(),
                RuntimeValue::new(json!("real value")).unwrap(),
            )],
            output_type,
        );
        V3ActionTaskExecutor::new(actions)
            .execute(&worker_context(2), &request, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            &[(json!({"text": "real value"}), "effect_stable".to_owned(), 2)]
        );
    }
}
