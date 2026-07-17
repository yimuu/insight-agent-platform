use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
};

use async_trait::async_trait;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::sleep;

use crate::{
    resources::models::{
        model_response_too_large, serialized_json_within_limit, ChatContent, ChatContentPart,
        ChatMessage, ChatRequest, ChatRole, ModelCapability, ModelRegistry,
    },
    runtime::{RunError, StopReason},
};

use super::{
    operation::{
        CompiledOperationContract, EvaluatedCall, Operation, OperationContext, OperationEffect,
        OperationError,
    },
    plan::{
        CallPlan, CompiledContentAtom, CompiledLlmPlan, MessageSourcePlan, PlannedRole,
        ValidatedResponseContract,
    },
    runtime_message::{
        build_authored_message, dynamic_messages_into_runtime, parse_dynamic_messages_bounded,
        validate_runtime_message_budget, validate_runtime_messages, RenderedContentAtom,
        RuntimeContent, RuntimeContentPart, RuntimeMessage, RuntimeMessageLimits, RuntimeRole,
    },
    schema::compile_contract_schema,
    types::{SchemaType, ValueType},
    value::Identifier,
};

pub const AI_CHAT_USES: &str = "ai.chat";
pub const DEFAULT_MAX_CHAT_REQUEST_BYTES: usize = 262_144;
pub const MAX_CHAT_REQUEST_BYTES: usize = 1_048_576;

const CHAT_CONFIG_INVALID: &str = "VNEXT_CHAT_CONFIG_INVALID";
const CHAT_MODEL_NOT_FOUND: &str = "VNEXT_LLM_MODEL_NOT_FOUND";
const CHAT_PARAMETERS_INVALID: &str = "VNEXT_LLM_PARAMETERS_INVALID";
const CHAT_MESSAGES_INVALID: &str = "VNEXT_LLM_CONTENT_INVALID";
const CHAT_INPUT_CONTRACT_INVALID: &str = "VNEXT_LLM_TEMPLATE_BINDING_INVALID";
const CHAT_PROMPT_NOT_FOUND: &str = "VNEXT_LLM_PROMPT_NOT_FOUND";
const CHAT_VISION_REQUIRED: &str = "VNEXT_LLM_VISION_REQUIRED";
const CHAT_RESPONSE_SCHEMA_INVALID: &str = "VNEXT_LLM_RESPONSE_CONFIG_INVALID";
const CHAT_DATA_TOO_LARGE: &str = "VNEXT_LLM_REQUEST_TOO_LARGE";
const CHAT_RESPONSE_JSON_INVALID: &str = "VNEXT_LLM_RESPONSE_JSON_INVALID";
const CHAT_RESPONSE_CONTRACT_INVALID: &str = "VNEXT_LLM_RESPONSE_CONTRACT_INVALID";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChatConfig {
    model: String,
    messages: Vec<MessageConfig>,
    #[serde(default = "empty_object")]
    parameters: Value,
    response: ResponseConfig,
    #[serde(default = "default_max_request_bytes")]
    max_request_bytes: usize,
}

fn empty_object() -> Value {
    json!({})
}

fn default_max_request_bytes() -> usize {
    DEFAULT_MAX_CHAT_REQUEST_BYTES
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MessageConfig {
    role: ChatRole,
    parts: Vec<MessagePart>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum MessagePart {
    /// Authored instruction text. This is the only inline form allowed in a
    /// system message.
    Text { text: String },
    /// A resolved, authored prompt declaration.
    Prompt { prompt: Identifier },
    /// A runtime value serialized as JSON and explicitly labelled untrusted.
    Data { input: Identifier },
    /// A runtime string used only as a user-message image URL.
    ImageUrl {
        input: Identifier,
        #[serde(default)]
        optional: bool,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case", deny_unknown_fields)]
enum ResponseConfig {
    Text,
    Json { schema: Value },
}

/// The vNext chat leaf. Prompt files are resolved before this operation is
/// registered, so runtime execution never performs filesystem access.
#[derive(Clone)]
pub struct ChatOperation {
    models: ModelRegistry,
    definitions: BTreeMap<Identifier, Value>,
    prompts: BTreeMap<Identifier, String>,
}

impl ChatOperation {
    pub fn new(
        models: ModelRegistry,
        definitions: BTreeMap<Identifier, Value>,
        prompts: BTreeMap<Identifier, String>,
    ) -> Self {
        Self {
            models,
            definitions,
            prompts,
        }
    }

    fn parse_config(config: &Value) -> Result<ChatConfig, OperationError> {
        let config: ChatConfig = serde_json::from_value(config.clone())
            .map_err(|_| operation_error(CHAT_CONFIG_INVALID, "ai.chat config is invalid"))?;
        if config.model.trim().is_empty()
            || !config.parameters.is_object()
            || config.max_request_bytes == 0
            || config.max_request_bytes > MAX_CHAT_REQUEST_BYTES
        {
            return Err(operation_error(
                CHAT_CONFIG_INVALID,
                "ai.chat config is invalid",
            ));
        }
        Ok(config)
    }

    fn validate_messages(
        &self,
        config: &ChatConfig,
        inputs: &BTreeMap<Identifier, ValueType>,
    ) -> Result<bool, OperationError> {
        if config.messages.is_empty() {
            return Err(operation_error(
                CHAT_MESSAGES_INVALID,
                "ai.chat requires at least one non-empty message",
            ));
        }

        let mut referenced_inputs = BTreeSet::new();
        let mut has_images = false;
        for message in &config.messages {
            if message.parts.is_empty() {
                return Err(operation_error(
                    CHAT_MESSAGES_INVALID,
                    "ai.chat requires at least one non-empty message",
                ));
            }
            for part in &message.parts {
                match part {
                    MessagePart::Text { text } => {
                        if text.trim().is_empty() {
                            return Err(operation_error(
                                CHAT_MESSAGES_INVALID,
                                "ai.chat authored message text must not be blank",
                            ));
                        }
                    }
                    MessagePart::Prompt { prompt } => {
                        if !self.prompts.contains_key(prompt) {
                            return Err(operation_error(
                                CHAT_PROMPT_NOT_FOUND,
                                "ai.chat references an undeclared prompt",
                            ));
                        }
                    }
                    MessagePart::Data { input } => {
                        if message.role != ChatRole::User {
                            return Err(operation_error(
                                CHAT_MESSAGES_INVALID,
                                "ai.chat runtime data is allowed only in user messages",
                            ));
                        }
                        referenced_inputs.insert(input.clone());
                    }
                    MessagePart::ImageUrl { input, optional } => {
                        if message.role != ChatRole::User {
                            return Err(operation_error(
                                CHAT_MESSAGES_INVALID,
                                "ai.chat image URLs are allowed only in user messages",
                            ));
                        }
                        let Some(input_type) = inputs.get(input) else {
                            return Err(operation_error(
                                CHAT_INPUT_CONTRACT_INVALID,
                                "ai.chat message input does not match with bindings",
                            ));
                        };
                        let expected = if *optional {
                            ValueType::Union(vec![ValueType::String, ValueType::Null])
                        } else {
                            ValueType::String
                        };
                        if !input_type.is_assignable_to(&expected) {
                            return Err(operation_error(
                                CHAT_INPUT_CONTRACT_INVALID,
                                "ai.chat image URL input must be a string or optional null",
                            ));
                        }
                        referenced_inputs.insert(input.clone());
                        has_images = true;
                    }
                }
            }
        }
        if referenced_inputs.len() != inputs.len()
            || referenced_inputs
                .iter()
                .any(|name| !inputs.contains_key(name))
        {
            return Err(operation_error(
                CHAT_INPUT_CONTRACT_INVALID,
                "ai.chat message inputs must exactly match with bindings",
            ));
        }
        Ok(has_images)
    }

    fn output_contract(
        &self,
        response: &ResponseConfig,
    ) -> Result<(Value, ValueType), OperationError> {
        let data_schema = match response {
            ResponseConfig::Text => json!({"type":"string"}),
            ResponseConfig::Json { schema } => compile_contract_schema(&self.definitions, schema)
                .map_err(|_| {
                    operation_error(
                        CHAT_RESPONSE_SCHEMA_INVALID,
                        "ai.chat response schema is invalid",
                    )
                })?
                .expanded_schema()
                .clone(),
        };
        let output_schema = json!({
            "type":"object",
            "required":["data", "finish_reason", "usage"],
            "properties":{
                "data":data_schema,
                "finish_reason":{"type":["string", "null"]},
                "usage":true
            },
            "additionalProperties":false
        });
        let output_type = SchemaType::compile(&output_schema)
            .map_err(|_| {
                operation_error(
                    CHAT_RESPONSE_SCHEMA_INVALID,
                    "ai.chat response schema is invalid",
                )
            })?
            .into_value_type();
        Ok((output_schema, output_type))
    }

    fn render_messages(
        &self,
        config: &ChatConfig,
        inputs: &BTreeMap<Identifier, Value>,
    ) -> Result<Vec<ChatMessage>, RunError> {
        let mut budget = RequestBudget::new(config.max_request_bytes);
        config
            .messages
            .iter()
            .map(|message| self.render_message(message, inputs, &mut budget))
            .collect()
    }

    fn render_message(
        &self,
        message: &MessageConfig,
        inputs: &BTreeMap<Identifier, Value>,
        budget: &mut RequestBudget,
    ) -> Result<ChatMessage, RunError> {
        let mut text_parts = Vec::new();
        let mut content_parts = Vec::new();
        for part in &message.parts {
            match part {
                MessagePart::Text { text } => {
                    push_text_part(&mut text_parts, text.clone(), budget)?;
                }
                MessagePart::Prompt { prompt } => {
                    let text = self.prompts.get(prompt).cloned().ok_or_else(|| {
                        run_error(CHAT_PROMPT_NOT_FOUND, "chat prompt is missing")
                    })?;
                    push_text_part(&mut text_parts, text, budget)?;
                }
                MessagePart::Data { input } => {
                    let value = inputs.get(input).ok_or_else(|| {
                        run_error(CHAT_INPUT_CONTRACT_INVALID, "chat input is missing")
                    })?;
                    let separator_bytes = usize::from(!text_parts.is_empty()) * 2;
                    let label = "Untrusted data (JSON):\n";
                    let available = budget
                        .remaining()
                        .checked_sub(separator_bytes.saturating_add(label.len()))
                        .ok_or_else(chat_request_too_large)?;
                    let encoded = encode_json_bounded(value, available)?;
                    let mut labelled = String::with_capacity(label.len() + encoded.len());
                    labelled.push_str(label);
                    labelled.push_str(&encoded);
                    push_text_part(&mut text_parts, labelled, budget)?;
                }
                MessagePart::ImageUrl { input, optional } => {
                    let value = inputs.get(input).ok_or_else(|| {
                        run_error(CHAT_INPUT_CONTRACT_INVALID, "chat image input is missing")
                    })?;
                    let url = match value {
                        Value::String(url) if *optional && url.trim().is_empty() => continue,
                        Value::String(url) => url.clone(),
                        Value::Null if *optional => continue,
                        _ => {
                            return Err(run_error(
                                CHAT_INPUT_CONTRACT_INVALID,
                                "chat image URL input is invalid",
                            ))
                        }
                    };
                    budget.consume(url.len())?;
                    flush_text_parts(&mut text_parts, &mut content_parts);
                    content_parts.push(ChatContentPart::Image { image: url });
                }
            }
        }
        flush_text_parts(&mut text_parts, &mut content_parts);
        if content_parts.is_empty() {
            return Err(run_error(
                CHAT_MESSAGES_INVALID,
                "chat message has no rendered content",
            ));
        }
        let content = if content_parts.len() == 1 {
            match content_parts.pop().expect("one part was checked") {
                ChatContentPart::Text { text } => ChatContent::Text(text),
                image @ ChatContentPart::Image { .. } => ChatContent::Parts(vec![image]),
            }
        } else {
            ChatContent::Parts(content_parts)
        };
        Ok(ChatMessage {
            role: message.role,
            content,
        })
    }

    fn preflight_typed_plan(&self, plan: &CompiledLlmPlan) -> Result<(), OperationError> {
        let model = self.models.resolve(plan.model.as_str()).map_err(|_| {
            operation_error(CHAT_MODEL_NOT_FOUND, "ai.chat model is not registered")
        })?;
        model
            .validate_parameters(plan.parameters.value())
            .map_err(|_| {
                operation_error(CHAT_PARAMETERS_INVALID, "ai.chat parameters are invalid")
            })?;
        if !plan.capabilities.is_subset(&model.capabilities()) {
            return Err(operation_error(
                CHAT_VISION_REQUIRED,
                "ai.chat model capabilities differ from the compiled plan",
            ));
        }
        Ok(())
    }

    fn render_typed_messages(
        &self,
        plan: &CompiledLlmPlan,
        call: &EvaluatedCall,
    ) -> Result<Vec<ChatMessage>, RunError> {
        let mut messages = Vec::new();
        let limits = RuntimeMessageLimits::new(
            plan.limits.max_messages,
            plan.limits.max_message_bytes,
            plan.limits.max_image_url_bytes,
            plan.limits.max_request_bytes,
        );
        for source in &plan.message_sources {
            match source {
                MessageSourcePlan::Dynamic { value, .. } => {
                    let value = call.dependencies.get(value).ok_or_else(|| {
                        run_error(
                            CHAT_INPUT_CONTRACT_INVALID,
                            "verified dynamic message dependency is missing",
                        )
                    })?;
                    if matches!(value, Value::Array(values) if values.is_empty()) {
                        continue;
                    }
                    let remaining_messages =
                        plan.limits.max_messages.saturating_sub(messages.len());
                    let current_usage = validate_runtime_message_budget(&messages, limits)
                        .map_err(runtime_message_error)?;
                    // A non-empty source adds one comma to the existing JSON
                    // array, while its own surrounding brackets disappear.
                    let remaining_source_bytes = if messages.is_empty() {
                        plan.limits.max_request_bytes
                    } else {
                        plan.limits
                            .max_request_bytes
                            .saturating_sub(current_usage.total_bytes)
                            .saturating_add(1)
                    };
                    let dynamic = parse_dynamic_messages_bounded(
                        value,
                        RuntimeMessageLimits::new(
                            remaining_messages,
                            plan.limits.max_message_bytes,
                            plan.limits.max_image_url_bytes,
                            remaining_source_bytes,
                        ),
                    )
                    .and_then(dynamic_messages_into_runtime)
                    .map_err(runtime_message_error)?;
                    messages.extend(dynamic);
                    validate_runtime_message_budget(&messages, limits)
                        .map_err(runtime_message_error)?;
                }
                MessageSourcePlan::Authored { role, content } => {
                    if messages.len() >= plan.limits.max_messages {
                        return Err(chat_request_too_large());
                    }
                    let role = planned_role_to_runtime(*role);
                    let mut atoms = Vec::with_capacity(content.len());
                    let mut remaining_content_bytes = plan.limits.max_message_bytes;
                    let mut has_pending_text = false;
                    for atom in content {
                        let separator_bytes = usize::from(
                            has_pending_text
                                && matches!(
                                    atom,
                                    CompiledContentAtom::Template { .. }
                                        | CompiledContentAtom::RuntimeText { .. }
                                ),
                        ) * 2;
                        remaining_content_bytes = remaining_content_bytes
                            .checked_sub(separator_bytes)
                            .ok_or_else(chat_request_too_large)?;
                        let rendered =
                            self.render_typed_atom(plan, call, atom, remaining_content_bytes)?;
                        let payload_bytes = match &rendered {
                            RenderedContentAtom::Text(text) => {
                                has_pending_text = true;
                                text.len()
                            }
                            RenderedContentAtom::Image(Some(image)) => {
                                has_pending_text = false;
                                image.len()
                            }
                            RenderedContentAtom::Image(None) => 0,
                        };
                        remaining_content_bytes = remaining_content_bytes
                            .checked_sub(payload_bytes)
                            .ok_or_else(chat_request_too_large)?;
                        atoms.push(rendered);
                    }
                    let message =
                        build_authored_message(role, atoms).map_err(runtime_message_error)?;
                    messages.push(message);
                    validate_runtime_message_budget(&messages, limits)
                        .map_err(runtime_message_error)?;
                }
            }
        }
        validate_runtime_messages(&messages, limits).map_err(runtime_message_error)?;
        Ok(messages
            .into_iter()
            .map(runtime_message_to_provider)
            .collect())
    }

    fn render_typed_atom(
        &self,
        plan: &CompiledLlmPlan,
        call: &EvaluatedCall,
        atom: &CompiledContentAtom,
        max_atom_bytes: usize,
    ) -> Result<RenderedContentAtom, RunError> {
        match atom {
            CompiledContentAtom::Template {
                template_id,
                bindings,
            } => {
                let template = plan.templates.get(template_id).ok_or_else(|| {
                    run_error(
                        CHAT_PROMPT_NOT_FOUND,
                        "verified LLM template is missing from its plan",
                    )
                })?;
                let data = bindings
                    .iter()
                    .map(|(name, value)| {
                        call.dependencies
                            .get(value)
                            .map(|value| (name.as_str(), value))
                            .ok_or_else(|| {
                                run_error(
                                    CHAT_INPUT_CONTRACT_INVALID,
                                    "verified LLM template dependency is missing",
                                )
                            })
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()?;
                if !serialized_json_within_limit(&data, plan.limits.max_template_context_bytes) {
                    return Err(chat_request_too_large());
                }
                let rendered = template
                    .compiled
                    .render_bounded(
                        &data,
                        plan.limits.max_template_output_bytes.min(max_atom_bytes),
                    )
                    .map_err(|error| run_error(error.code(), error.message()))?;
                Ok(RenderedContentAtom::Text(rendered))
            }
            CompiledContentAtom::RuntimeText { value } => {
                let text = call
                    .dependencies
                    .get(value)
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        run_error(
                            CHAT_INPUT_CONTRACT_INVALID,
                            "verified LLM runtime text dependency is not a string",
                        )
                    })?;
                if text.len() > max_atom_bytes {
                    return Err(chat_request_too_large());
                }
                Ok(RenderedContentAtom::Text(text.to_string()))
            }
            CompiledContentAtom::Image { value } => {
                let value = call.dependencies.get(value).ok_or_else(|| {
                    run_error(
                        CHAT_INPUT_CONTRACT_INVALID,
                        "verified LLM image dependency is missing",
                    )
                })?;
                let image = match value {
                    Value::Null => None,
                    Value::String(image)
                        if image.len() <= plan.limits.max_image_url_bytes
                            && image.len() <= max_atom_bytes =>
                    {
                        Some(image.clone())
                    }
                    Value::String(_) => return Err(chat_request_too_large()),
                    _ => {
                        return Err(run_error(
                            CHAT_INPUT_CONTRACT_INVALID,
                            "verified LLM image dependency is not a string or null",
                        ))
                    }
                };
                Ok(RenderedContentAtom::Image(image))
            }
        }
    }
}

#[async_trait]
impl Operation for ChatOperation {
    fn uses(&self) -> &'static str {
        AI_CHAT_USES
    }

    fn compile(
        &self,
        config: &Value,
        inputs: &BTreeMap<Identifier, ValueType>,
    ) -> Result<CompiledOperationContract, OperationError> {
        let config = Self::parse_config(config)?;
        let model = self.models.resolve(&config.model).map_err(|_| {
            operation_error(CHAT_MODEL_NOT_FOUND, "ai.chat model is not registered")
        })?;
        model.validate_parameters(&config.parameters).map_err(|_| {
            operation_error(CHAT_PARAMETERS_INVALID, "ai.chat parameters are invalid")
        })?;
        let has_images = self.validate_messages(&config, inputs)?;
        if has_images && !model.capabilities().contains(&ModelCapability::Vision) {
            return Err(operation_error(
                CHAT_VISION_REQUIRED,
                "ai.chat message requires a vision-capable model",
            ));
        }
        let (output_schema, output_type) = self.output_contract(&config.response)?;

        Ok(CompiledOperationContract {
            output_schema,
            output_type,
            effect: OperationEffect::ExternalModel,
            idempotent: false,
        })
    }

    fn preflight_plan(
        &self,
        plan: &CallPlan,
        _inputs: &BTreeMap<Identifier, ValueType>,
    ) -> Result<(), OperationError> {
        let CallPlan::Llm(plan) = plan else {
            return Err(operation_error(
                CHAT_CONFIG_INVALID,
                "ai.chat requires an LLM CallPlan",
            ));
        };
        self.preflight_typed_plan(plan)
    }

    async fn execute_plan(
        &self,
        plan: &CallPlan,
        call: EvaluatedCall,
        context: OperationContext,
    ) -> Result<Value, RunError> {
        let CallPlan::Llm(plan) = plan else {
            return Err(run_error(
                CHAT_CONFIG_INVALID,
                "ai.chat requires an LLM CallPlan",
            ));
        };
        self.preflight_typed_plan(plan)
            .map_err(|error| run_error(error.code(), error.message()))?;
        let model = self
            .models
            .resolve(plan.model.as_str())
            .map_err(|_| run_error(CHAT_MODEL_NOT_FOUND, "ai.chat model is not registered"))?;
        let messages = self.render_typed_messages(plan, &call)?;
        if provider_messages_require_vision(&messages)
            && !model.capabilities().contains(&ModelCapability::Vision)
        {
            return Err(run_error(
                CHAT_VISION_REQUIRED,
                "ai.chat rendered message requires a vision-capable model",
            ));
        }
        if !serialized_json_within_limit(plan.parameters.value(), plan.limits.max_request_bytes) {
            return Err(chat_request_too_large());
        }
        let request = ChatRequest {
            messages,
            parameters: plan.parameters.value().clone(),
        };
        if !model.request_body_within_limit(&request, plan.limits.max_request_bytes) {
            return Err(chat_request_too_large());
        }

        let stream = model.stream_chat(request);
        tokio::pin!(stream);
        let mut stream = tokio::select! {
            result = &mut stream => result?,
            _ = context.control.stopped() => return Err(stopped_error(&context)),
            _ = sleep(context.control.remaining()) => return Err(RunError::operation_timeout()),
        };
        let mut text = String::new();
        let mut finish_reason = None;
        let mut usage = None;
        let max_text_bytes = model.max_accumulated_text_bytes();
        loop {
            let chunk = tokio::select! {
                chunk = stream.next() => chunk,
                _ = context.control.stopped() => return Err(stopped_error(&context)),
                _ = sleep(context.control.remaining()) => return Err(RunError::operation_timeout()),
            };
            let Some(chunk) = chunk else {
                break;
            };
            let chunk = chunk?;
            if text.len().saturating_add(chunk.text.len()) > max_text_bytes {
                return Err(model_response_too_large());
            }
            text.push_str(&chunk.text);
            if chunk.finish_reason.is_some() {
                finish_reason = chunk.finish_reason;
            }
            if chunk.usage.is_some() {
                usage = chunk.usage;
            }
        }
        let data = match &plan.response {
            ValidatedResponseContract::Text => Value::String(text),
            ValidatedResponseContract::Json { data } => {
                let value: Value = serde_json::from_str(&text).map_err(|_| {
                    run_error(
                        CHAT_RESPONSE_JSON_INVALID,
                        "chat response is not valid JSON",
                    )
                })?;
                let validator = crate::schema::compile_schema_2020(&data.schema).map_err(|_| {
                    run_error(
                        CHAT_RESPONSE_SCHEMA_INVALID,
                        "chat response schema is invalid",
                    )
                })?;
                if !validator.is_valid(&value) {
                    return Err(run_error(
                        CHAT_RESPONSE_CONTRACT_INVALID,
                        "chat response does not match its schema",
                    ));
                }
                value
            }
        };
        Ok(json!({
            "data": data,
            "finish_reason": finish_reason,
            "usage": usage,
        }))
    }

    async fn execute(
        &self,
        config: &Value,
        inputs: BTreeMap<Identifier, Value>,
        context: OperationContext,
    ) -> Result<Value, RunError> {
        let config =
            Self::parse_config(config).map_err(|error| run_error(error.code(), error.message()))?;
        let model = self
            .models
            .resolve(&config.model)
            .map_err(|_| run_error(CHAT_MODEL_NOT_FOUND, "ai.chat model is not registered"))?;
        let messages = self.render_messages(&config, &inputs)?;
        let request = ChatRequest {
            messages,
            parameters: config.parameters.clone(),
        };

        let stream = model.stream_chat(request);
        tokio::pin!(stream);
        let mut stream = tokio::select! {
            result = &mut stream => result?,
            _ = context.control.stopped() => return Err(stopped_error(&context)),
            _ = sleep(context.control.remaining()) => return Err(RunError::operation_timeout()),
        };

        let mut text = String::new();
        let mut finish_reason = None;
        let mut usage = None;
        let max_text_bytes = model.max_accumulated_text_bytes();
        loop {
            let chunk = tokio::select! {
                chunk = stream.next() => chunk,
                _ = context.control.stopped() => return Err(stopped_error(&context)),
                _ = sleep(context.control.remaining()) => return Err(RunError::operation_timeout()),
            };
            let Some(chunk) = chunk else {
                break;
            };
            let chunk = chunk?;
            if text.len().saturating_add(chunk.text.len()) > max_text_bytes {
                return Err(model_response_too_large());
            }
            if !chunk.text.is_empty() {
                text.push_str(&chunk.text);
            }
            if chunk.finish_reason.is_some() {
                finish_reason = chunk.finish_reason;
            }
            if chunk.usage.is_some() {
                usage = chunk.usage;
            }
        }

        let data = match &config.response {
            ResponseConfig::Text => Value::String(text),
            ResponseConfig::Json { schema } => {
                let data: Value = serde_json::from_str(&text).map_err(|_| {
                    run_error(
                        CHAT_RESPONSE_JSON_INVALID,
                        "chat response is not valid JSON",
                    )
                })?;
                let contract =
                    compile_contract_schema(&self.definitions, schema).map_err(|_| {
                        run_error(
                            CHAT_RESPONSE_SCHEMA_INVALID,
                            "chat response schema is invalid",
                        )
                    })?;
                if !contract.validator().is_valid(&data) {
                    return Err(run_error(
                        CHAT_RESPONSE_CONTRACT_INVALID,
                        "chat response does not match its schema",
                    ));
                }
                data
            }
        };
        Ok(json!({
            "data": data,
            "finish_reason": finish_reason,
            "usage": usage,
        }))
    }
}

fn planned_role_to_runtime(role: PlannedRole) -> RuntimeRole {
    match role {
        PlannedRole::System => RuntimeRole::System,
        PlannedRole::User => RuntimeRole::User,
        PlannedRole::Assistant => RuntimeRole::Assistant,
    }
}

fn runtime_message_to_provider(message: RuntimeMessage) -> ChatMessage {
    let role = match message.role() {
        RuntimeRole::System => ChatRole::System,
        RuntimeRole::User => ChatRole::User,
        RuntimeRole::Assistant => ChatRole::Assistant,
    };
    let content = match message.into_content() {
        RuntimeContent::Text(text) => ChatContent::Text(text),
        RuntimeContent::Parts(parts) => ChatContent::Parts(
            parts
                .into_iter()
                .map(|part| match part {
                    RuntimeContentPart::Text { text } => ChatContentPart::Text { text },
                    RuntimeContentPart::Image { image } => ChatContentPart::Image { image },
                })
                .collect(),
        ),
    };
    ChatMessage { role, content }
}

fn provider_messages_require_vision(messages: &[ChatMessage]) -> bool {
    messages.iter().any(|message| {
        matches!(
            &message.content,
            ChatContent::Parts(parts)
                if parts
                    .iter()
                    .any(|part| matches!(part, ChatContentPart::Image { .. }))
        )
    })
}

fn runtime_message_error(error: super::runtime_message::RuntimeMessageError) -> RunError {
    run_error(error.code(), error.message())
}

fn flush_text_parts(text_parts: &mut Vec<String>, content_parts: &mut Vec<ChatContentPart>) {
    if !text_parts.is_empty() {
        content_parts.push(ChatContentPart::Text {
            text: std::mem::take(text_parts).join("\n\n"),
        });
    }
}

fn push_text_part(
    text_parts: &mut Vec<String>,
    text: String,
    budget: &mut RequestBudget,
) -> Result<(), RunError> {
    let separator_bytes = usize::from(!text_parts.is_empty()) * 2;
    budget.consume(separator_bytes.saturating_add(text.len()))?;
    text_parts.push(text);
    Ok(())
}

struct RequestBudget {
    remaining: usize,
}

impl RequestBudget {
    fn new(max_bytes: usize) -> Self {
        Self {
            remaining: max_bytes,
        }
    }

    fn remaining(&self) -> usize {
        self.remaining
    }

    fn consume(&mut self, bytes: usize) -> Result<(), RunError> {
        self.remaining = self
            .remaining
            .checked_sub(bytes)
            .ok_or_else(chat_request_too_large)?;
        Ok(())
    }
}

struct BoundedJsonWriter {
    bytes: Vec<u8>,
    max_bytes: usize,
    exceeded: bool,
}

impl BoundedJsonWriter {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(max_bytes.min(4_096)),
            max_bytes,
            exceeded: false,
        }
    }
}

impl Write for BoundedJsonWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if buffer.len() > self.max_bytes.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "chat request exceeds configured limit",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encode_json_bounded(value: &Value, max_bytes: usize) -> Result<String, RunError> {
    let mut writer = BoundedJsonWriter::new(max_bytes);
    if serde_json::to_writer(&mut writer, value).is_err() {
        return if writer.exceeded {
            Err(chat_request_too_large())
        } else {
            Err(run_error(
                CHAT_INPUT_CONTRACT_INVALID,
                "chat input is invalid",
            ))
        };
    }
    String::from_utf8(writer.bytes).map_err(|_| {
        run_error(
            CHAT_INPUT_CONTRACT_INVALID,
            "chat input is not valid UTF-8 JSON",
        )
    })
}

fn chat_request_too_large() -> RunError {
    run_error(
        CHAT_DATA_TOO_LARGE,
        "chat request exceeds its aggregate byte limit",
    )
}

fn operation_error(code: &'static str, message: &'static str) -> OperationError {
    OperationError::new(code, message)
}

fn run_error(code: &'static str, message: &'static str) -> RunError {
    RunError::operation(code, message)
}

fn stopped_error(context: &OperationContext) -> RunError {
    context
        .control
        .stop_reason()
        .map(RunError::stopped)
        .unwrap_or_else(|| RunError::stopped(StopReason::Interrupted))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_trait::async_trait;
    use futures::stream;
    use serde_json::{json, Value};

    use super::{ChatOperation, AI_CHAT_USES};
    use crate::{
        dsl::{
            vnext::{
                ir::{OperationId, TypedContract, ValueId},
                operation::{EvaluatedCall, Operation, OperationContext},
                plan::{
                    CallPlan, CompiledContentAtom, CompiledLlmPlan, CompiledTemplateId,
                    MessageSourcePlan, PlannedRole, PlannedTemplate, ResolvedModelId,
                    ResolvedRequestLimits, TemplateProfileVersion, TemplateProvenance,
                    ValidatedModelParameters, ValidatedResponseContract,
                },
                shape::DynamicMessageShapeProof,
                template::compile_template,
                types::{SchemaType, ValueType},
                value::{Identifier, LocalInputPath},
            },
            DslPath,
        },
        resources::models::{
            ChatChunk, ChatMessage, ChatModel, ChatRequest, ChatRole, ChatStream, ModelCapability,
            ModelRegistry,
        },
        runtime::{stop_pair, ExecutionControl, RunError},
    };

    #[derive(Debug)]
    struct FakeModel {
        response: String,
        requests: Arc<Mutex<Vec<ChatRequest>>>,
        vision: bool,
    }

    #[async_trait]
    impl ChatModel for FakeModel {
        fn capabilities(&self) -> BTreeSet<ModelCapability> {
            self.vision
                .then_some(ModelCapability::Vision)
                .into_iter()
                .collect()
        }

        fn validate_parameters(&self, parameters: &Value) -> Result<(), crate::dsl::CompileError> {
            if parameters.is_object() {
                Ok(())
            } else {
                Err(crate::dsl::CompileError::new("BAD", "bad"))
            }
        }

        async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError> {
            self.requests.lock().unwrap().push(request);
            Ok(Box::pin(stream::iter(vec![Ok(ChatChunk {
                text: self.response.clone(),
                finish_reason: Some("stop".to_string()),
                usage: Some(json!({"tokens":3})),
            })])))
        }
    }

    fn id(value: &str) -> Identifier {
        Identifier::parse(value).unwrap()
    }

    fn fake_operation(
        response: &str,
        vision: bool,
    ) -> (ChatOperation, Arc<Mutex<Vec<ChatRequest>>>) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut models = ModelRegistry::default();
        models
            .register(
                "chat",
                FakeModel {
                    response: response.to_string(),
                    requests: Arc::clone(&requests),
                    vision,
                },
            )
            .unwrap();
        (
            ChatOperation::new(
                models,
                BTreeMap::new(),
                BTreeMap::from([(id("system"), "System instruction".to_string())]),
            ),
            requests,
        )
    }

    fn text_config() -> Value {
        json!({
            "model":"chat",
            "messages":[
                {"role":"system","parts":[{"kind":"prompt","prompt":"system"}]},
                {"role":"user","parts":[
                    {"kind":"text","text":"Analyze the following value."},
                    {"kind":"data","input":"question"}
                ]}
            ],
            "parameters":{},
            "response":{"format":"text"}
        })
    }

    fn context() -> OperationContext {
        let (_controller, stop) = stop_pair();
        OperationContext::new(
            "run_1",
            OperationId::authored("/workflow/chat").unwrap(),
            1,
            ExecutionControl::new(stop, Duration::from_secs(1)),
        )
    }

    fn value_id(name: &str) -> ValueId {
        ValueId::output(format!("/workflow/chat/{name}")).unwrap()
    }

    fn typed_contract(schema: Value) -> TypedContract {
        let value_type = SchemaType::compile(&schema).unwrap().into_value_type();
        TypedContract { schema, value_type }
    }

    fn catalog_template(
        name: &str,
        source: &str,
        slot_signature: BTreeMap<Identifier, ValueType>,
    ) -> (CompiledTemplateId, PlannedTemplate) {
        let prompt_id = id(name);
        (
            CompiledTemplateId::catalog(&prompt_id),
            PlannedTemplate {
                provenance: TemplateProvenance::Catalog {
                    prompt_id,
                    asset_hash: "00".repeat(32),
                },
                compiled: compile_template(source).unwrap(),
                slot_signature,
                profile_version: TemplateProfileVersion::V1,
            },
        )
    }

    fn inline_template(
        name: &str,
        source: &str,
        slot_signature: BTreeMap<Identifier, ValueType>,
    ) -> (CompiledTemplateId, PlannedTemplate) {
        let path = DslPath::root().child_key(name);
        (
            CompiledTemplateId::inline(&path, 0),
            PlannedTemplate {
                provenance: TemplateProvenance::Inline {
                    dsl_path: path,
                    source_hash: "11".repeat(32),
                },
                compiled: compile_template(source).unwrap(),
                slot_signature,
                profile_version: TemplateProfileVersion::V1,
            },
        )
    }

    fn llm_plan(
        local_inputs: BTreeMap<Identifier, ValueId>,
        message_sources: Vec<MessageSourcePlan>,
        templates: BTreeMap<CompiledTemplateId, PlannedTemplate>,
        parameters: Value,
        response: ValidatedResponseContract,
        capabilities: BTreeSet<ModelCapability>,
        max_request_bytes: usize,
    ) -> CallPlan {
        let data_schema = match &response {
            ValidatedResponseContract::Text => json!({"type":"string"}),
            ValidatedResponseContract::Json { data } => data.schema.clone(),
        };
        let output_contract = typed_contract(json!({
            "type":"object",
            "required":["data", "finish_reason", "usage"],
            "properties":{
                "data":data_schema,
                "finish_reason":{"type":["string", "null"]},
                "usage":true
            },
            "additionalProperties":false
        }));
        let atom_limit = max_request_bytes.min(16 * 1024);
        CallPlan::Llm(CompiledLlmPlan {
            model: ResolvedModelId::parse("chat").unwrap(),
            local_inputs,
            message_sources,
            templates,
            parameters: ValidatedModelParameters::new(parameters).unwrap(),
            response,
            output_contract,
            capabilities,
            limits: ResolvedRequestLimits {
                max_messages: 16,
                max_message_bytes: atom_limit,
                max_image_url_bytes: atom_limit,
                max_request_bytes,
                max_template_context_bytes: 256 * 1024,
                max_template_output_bytes: atom_limit,
            },
        })
    }

    fn evaluated_call(values: Vec<(Identifier, ValueId, Value)>) -> EvaluatedCall {
        let mut inputs = BTreeMap::new();
        let mut dependencies = BTreeMap::new();
        for (name, value_id, value) in values {
            inputs.insert(name, value.clone());
            dependencies.insert(value_id, value);
        }
        EvaluatedCall {
            inputs,
            dependencies,
        }
    }

    #[tokio::test]
    async fn typed_plan_preserves_static_dynamic_and_authored_message_order_and_bytes() {
        let (operation, requests) = fake_operation("done", false);
        let history = value_id("history");
        let question = value_id("question");
        let (system_template_id, system_template) =
            catalog_template("typed_system", "System instruction.", BTreeMap::new());
        let plan = llm_plan(
            BTreeMap::from([
                (id("history"), history.clone()),
                (id("question"), question.clone()),
            ]),
            vec![
                MessageSourcePlan::Authored {
                    role: PlannedRole::System,
                    content: vec![CompiledContentAtom::Template {
                        template_id: system_template_id.clone(),
                        bindings: BTreeMap::new(),
                    }],
                },
                MessageSourcePlan::Dynamic {
                    source: LocalInputPath::parse("inputs.history").unwrap(),
                    value: history.clone(),
                    proven_shape: DynamicMessageShapeProof {
                        requires_vision: false,
                    },
                },
                MessageSourcePlan::Authored {
                    role: PlannedRole::User,
                    content: vec![CompiledContentAtom::RuntimeText {
                        value: question.clone(),
                    }],
                },
            ],
            BTreeMap::from([(system_template_id, system_template)]),
            json!({}),
            ValidatedResponseContract::Text,
            BTreeSet::new(),
            16 * 1024,
        );

        operation
            .execute_plan(
                &plan,
                evaluated_call(vec![
                    (
                        id("history"),
                        history,
                        json!([
                            {"role":"user", "content":r#"prior {"raw":true}"#},
                            {"role":"assistant", "content":"answer exactly"}
                        ]),
                    ),
                    (id("question"), question, json!(r#"What is "x"?"#)),
                ]),
                context(),
            )
            .await
            .unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(
            requests[0].messages,
            vec![
                ChatMessage::from_text(ChatRole::System, "System instruction."),
                ChatMessage::from_text(ChatRole::User, r#"prior {"raw":true}"#),
                ChatMessage::from_text(ChatRole::Assistant, "answer exactly"),
                ChatMessage::from_text(ChatRole::User, r#"What is "x"?"#),
            ]
        );
    }

    #[tokio::test]
    async fn typed_plan_renders_restricted_template_bindings_without_escaping_or_unstable_json() {
        let (operation, requests) = fake_operation("done", false);
        let name = value_id("name");
        let context_value = value_id("context");
        let tags = value_id("tags");
        let (user_template_id, user_template) = inline_template(
            "typed_user",
            "Hello {{name}}. Context={{json context}}. Tags:{{#each tags as |tag|}}[{{tag}}]{{/each}}",
            BTreeMap::from([
                (id("name"), ValueType::String),
                (
                    id("context"),
                    typed_contract(json!({
                        "type":"object",
                        "additionalProperties":{"type":"integer"}
                    }))
                    .value_type,
                ),
                (
                    id("tags"),
                    typed_contract(json!({"type":"array", "items":{"type":"string"}}))
                        .value_type,
                ),
            ]),
        );
        let plan = llm_plan(
            BTreeMap::from([
                (id("name"), name.clone()),
                (id("context"), context_value.clone()),
                (id("tags"), tags.clone()),
            ]),
            vec![MessageSourcePlan::Authored {
                role: PlannedRole::User,
                content: vec![CompiledContentAtom::Template {
                    template_id: user_template_id.clone(),
                    bindings: BTreeMap::from([
                        (id("name"), name.clone()),
                        (id("context"), context_value.clone()),
                        (id("tags"), tags.clone()),
                    ]),
                }],
            }],
            BTreeMap::from([(user_template_id, user_template)]),
            json!({}),
            ValidatedResponseContract::Text,
            BTreeSet::new(),
            16 * 1024,
        );

        operation
            .execute_plan(
                &plan,
                evaluated_call(vec![
                    (id("name"), name, json!("<Ada>&")),
                    (id("context"), context_value, json!({"z":2, "a":1})),
                    (id("tags"), tags, json!(["one", "two"])),
                ]),
                context(),
            )
            .await
            .unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(
            requests[0].messages,
            vec![ChatMessage::from_text(
                ChatRole::User,
                r#"Hello <Ada>&. Context={"a":1,"z":2}. Tags:[one][two]"#,
            )]
        );
    }

    #[tokio::test]
    async fn template_context_has_a_separate_preclone_budget() {
        let (operation, requests) = fake_operation("done", false);
        let context_value = value_id("large_context");
        let context_type = typed_contract(json!({
            "type":"object",
            "required":["small", "padding"],
            "properties":{
                "small":{"type":"string"},
                "padding":{"type":"string"}
            },
            "additionalProperties":false
        }))
        .value_type;
        let (template_id, template) = inline_template(
            "context_budget",
            "{{context.small}}",
            BTreeMap::from([(id("context"), context_type)]),
        );
        let plan = llm_plan(
            BTreeMap::from([(id("context"), context_value.clone())]),
            vec![MessageSourcePlan::Authored {
                role: PlannedRole::User,
                content: vec![CompiledContentAtom::Template {
                    template_id: template_id.clone(),
                    bindings: BTreeMap::from([(id("context"), context_value.clone())]),
                }],
            }],
            BTreeMap::from([(template_id, template)]),
            json!({}),
            ValidatedResponseContract::Text,
            BTreeSet::new(),
            16 * 1024,
        );

        operation
            .execute_plan(
                &plan,
                evaluated_call(vec![(
                    id("context"),
                    context_value.clone(),
                    json!({"small":"ok", "padding":"x".repeat(100_000)}),
                )]),
                context(),
            )
            .await
            .unwrap();
        assert_eq!(
            requests.lock().unwrap()[0].messages,
            vec![ChatMessage::from_text(ChatRole::User, "ok")]
        );

        let error = operation
            .execute_plan(
                &plan,
                evaluated_call(vec![(
                    id("context"),
                    context_value,
                    json!({"small":"ok", "padding":"x".repeat(300_000)}),
                )]),
                context(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), "VNEXT_LLM_REQUEST_TOO_LARGE");
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn typed_plan_omits_null_images_and_rejects_blank_present_images() {
        let (operation, requests) = fake_operation("done", true);
        let question = value_id("image_question");
        let image = value_id("image");
        let plan = llm_plan(
            BTreeMap::from([
                (id("question"), question.clone()),
                (id("image"), image.clone()),
            ]),
            vec![MessageSourcePlan::Authored {
                role: PlannedRole::User,
                content: vec![
                    CompiledContentAtom::RuntimeText {
                        value: question.clone(),
                    },
                    CompiledContentAtom::Image {
                        value: image.clone(),
                    },
                ],
            }],
            BTreeMap::new(),
            json!({}),
            ValidatedResponseContract::Text,
            BTreeSet::from([ModelCapability::Vision]),
            16 * 1024,
        );

        operation
            .execute_plan(
                &plan,
                evaluated_call(vec![
                    (id("question"), question.clone(), json!("Inspect this.")),
                    (id("image"), image.clone(), Value::Null),
                ]),
                context(),
            )
            .await
            .unwrap();
        assert_eq!(
            requests.lock().unwrap()[0].messages,
            vec![ChatMessage::from_text(ChatRole::User, "Inspect this.")]
        );

        let error = operation
            .execute_plan(
                &plan,
                evaluated_call(vec![
                    (id("question"), question, json!("Inspect this.")),
                    (id("image"), image, json!("   ")),
                ]),
                context(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), "VNEXT_LLM_CONTENT_INVALID");
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn typed_plan_rechecks_dynamic_images_against_runtime_model_capabilities() {
        let (operation, requests) = fake_operation("done", false);
        let history = value_id("dynamic_image_history");
        let plan = llm_plan(
            BTreeMap::from([(id("history"), history.clone())]),
            vec![MessageSourcePlan::Dynamic {
                source: LocalInputPath::parse("inputs.history").unwrap(),
                value: history.clone(),
                proven_shape: DynamicMessageShapeProof {
                    requires_vision: false,
                },
            }],
            BTreeMap::new(),
            json!({}),
            ValidatedResponseContract::Text,
            BTreeSet::new(),
            16 * 1024,
        );

        let error = operation
            .execute_plan(
                &plan,
                evaluated_call(vec![(
                    id("history"),
                    history,
                    json!([{
                        "role": "user",
                        "content": [{"image": "https://example.test/report.png"}]
                    }]),
                )]),
                context(),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code(), "VNEXT_LLM_VISION_REQUIRED");
        assert!(requests.lock().unwrap().is_empty());
    }

    #[test]
    fn empty_dynamic_source_is_a_zero_byte_noop_at_the_exact_message_boundary() {
        let (operation, _) = fake_operation("done", false);
        let question = value_id("exact_question");
        let history = value_id("empty_history");
        let mut plan = llm_plan(
            BTreeMap::from([
                (id("question"), question.clone()),
                (id("history"), history.clone()),
            ]),
            vec![
                MessageSourcePlan::Authored {
                    role: PlannedRole::User,
                    content: vec![CompiledContentAtom::RuntimeText {
                        value: question.clone(),
                    }],
                },
                MessageSourcePlan::Dynamic {
                    source: LocalInputPath::parse("inputs.history").unwrap(),
                    value: history.clone(),
                    proven_shape: DynamicMessageShapeProof {
                        requires_vision: false,
                    },
                },
            ],
            BTreeMap::new(),
            json!({}),
            ValidatedResponseContract::Text,
            BTreeSet::new(),
            16 * 1024,
        );
        let exact = serde_json::to_vec(&json!([{"role":"user", "content":"q"}]))
            .unwrap()
            .len();
        let CallPlan::Llm(plan) = &mut plan else {
            unreachable!()
        };
        plan.limits.max_message_bytes = exact;
        plan.limits.max_image_url_bytes = exact;
        plan.limits.max_request_bytes = exact;
        plan.limits.max_template_output_bytes = exact;

        let rendered = operation
            .render_typed_messages(
                plan,
                &evaluated_call(vec![
                    (id("question"), question, json!("q")),
                    (id("history"), history, json!([])),
                ]),
            )
            .unwrap();
        assert_eq!(rendered, vec![ChatMessage::from_text(ChatRole::User, "q")]);
    }

    #[test]
    fn multiple_dynamic_sources_share_one_preallocation_budget() {
        let (operation, _) = fake_operation("done", false);
        let first = value_id("first_history");
        let second = value_id("second_history");
        let first_value = json!([{"role":"user", "content":"a".repeat(40)}]);
        let second_value = json!([{"role":"user", "content":"b".repeat(40)}]);
        let single_bytes = serde_json::to_vec(&first_value).unwrap().len();
        let mut plan = llm_plan(
            BTreeMap::from([(id("first"), first.clone()), (id("second"), second.clone())]),
            vec![
                MessageSourcePlan::Dynamic {
                    source: LocalInputPath::parse("inputs.first").unwrap(),
                    value: first.clone(),
                    proven_shape: DynamicMessageShapeProof {
                        requires_vision: false,
                    },
                },
                MessageSourcePlan::Dynamic {
                    source: LocalInputPath::parse("inputs.second").unwrap(),
                    value: second.clone(),
                    proven_shape: DynamicMessageShapeProof {
                        requires_vision: false,
                    },
                },
            ],
            BTreeMap::new(),
            json!({}),
            ValidatedResponseContract::Text,
            BTreeSet::new(),
            single_bytes + 8,
        );
        let CallPlan::Llm(plan) = &mut plan else {
            unreachable!()
        };
        plan.limits.max_message_bytes = single_bytes;
        plan.limits.max_image_url_bytes = single_bytes;

        let error = operation
            .render_typed_messages(
                plan,
                &evaluated_call(vec![
                    (id("first"), first, first_value),
                    (id("second"), second, second_value),
                ]),
            )
            .unwrap_err();
        assert_eq!(error.code(), "VNEXT_LLM_REQUEST_TOO_LARGE");
    }

    #[tokio::test]
    async fn typed_plan_enforces_provider_request_aggregate_size_including_parameters() {
        let (operation, requests) = fake_operation("done", false);
        let question = value_id("small_question");
        let plan = llm_plan(
            BTreeMap::from([(id("question"), question.clone())]),
            vec![MessageSourcePlan::Authored {
                role: PlannedRole::User,
                content: vec![CompiledContentAtom::RuntimeText {
                    value: question.clone(),
                }],
            }],
            BTreeMap::new(),
            json!({"provider_option":"x".repeat(100)}),
            ValidatedResponseContract::Text,
            BTreeSet::new(),
            128,
        );

        let error = operation
            .execute_plan(
                &plan,
                evaluated_call(vec![(id("question"), question, json!("U"))]),
                context(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), "VNEXT_LLM_REQUEST_TOO_LARGE");
        assert!(requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn typed_plan_parses_and_validates_structured_json_response() {
        let response_schema = json!({
            "type":"object",
            "required":["answer"],
            "properties":{"answer":{"type":"string"}},
            "additionalProperties":false
        });
        let response = ValidatedResponseContract::Json {
            data: typed_contract(response_schema),
        };
        let question = value_id("json_question");
        let plan = llm_plan(
            BTreeMap::from([(id("question"), question.clone())]),
            vec![MessageSourcePlan::Authored {
                role: PlannedRole::User,
                content: vec![CompiledContentAtom::RuntimeText {
                    value: question.clone(),
                }],
            }],
            BTreeMap::new(),
            json!({}),
            response,
            BTreeSet::new(),
            16 * 1024,
        );
        let (operation, _) = fake_operation(r#"{"answer":"ok"}"#, false);

        let output = operation
            .execute_plan(
                &plan,
                evaluated_call(vec![(id("question"), question.clone(), json!("Question"))]),
                context(),
            )
            .await
            .unwrap();
        assert_eq!(output["data"], json!({"answer":"ok"}));
        assert_eq!(output["finish_reason"], "stop");
        assert_eq!(output["usage"], json!({"tokens":3}));

        let (invalid, _) = fake_operation(r#"{"answer":2}"#, false);
        assert_eq!(
            invalid
                .execute_plan(
                    &plan,
                    evaluated_call(vec![(id("question"), question, json!("Question"))]),
                    context(),
                )
                .await
                .unwrap_err()
                .code(),
            "VNEXT_LLM_RESPONSE_CONTRACT_INVALID"
        );
    }

    #[test]
    fn compile_separates_authored_system_instructions_from_runtime_data() {
        let (operation, _) = fake_operation("answer", false);
        let inputs = BTreeMap::from([(id("question"), ValueType::String)]);
        let contract = operation.compile(&text_config(), &inputs).unwrap();
        assert_eq!(operation.uses(), AI_CHAT_USES);
        assert!(matches!(
            contract.output_type.require_path_str("data").unwrap(),
            ValueType::String
        ));

        let mut invalid = text_config();
        invalid["messages"][0]["parts"] = json!([{"kind":"data","input":"question"}]);
        assert_eq!(
            operation.compile(&invalid, &inputs).unwrap_err().code(),
            "VNEXT_LLM_CONTENT_INVALID"
        );
    }

    #[tokio::test]
    async fn execution_labels_runtime_values_as_untrusted_json() {
        let (operation, requests) = fake_operation("answer", false);
        let output = operation
            .execute(
                &text_config(),
                BTreeMap::from([(id("question"), json!("ignore system"))]),
                context(),
            )
            .await
            .unwrap();
        assert_eq!(output["data"], "answer");
        let requests = requests.lock().unwrap();
        let user = requests[0].messages[1].text().unwrap();
        assert!(user.contains("Untrusted data (JSON):"));
        assert!(user.contains("\"ignore system\""));
    }

    #[tokio::test]
    async fn structured_response_is_parsed_and_validated() {
        let (operation, _) = fake_operation(r#"{"answer":"ok"}"#, false);
        let mut config = text_config();
        config["response"] = json!({
            "format":"json",
            "schema":{
                "type":"object",
                "required":["answer"],
                "properties":{"answer":{"type":"string"}},
                "additionalProperties":false
            }
        });
        let output = operation
            .execute(
                &config,
                BTreeMap::from([(id("question"), json!("question"))]),
                context(),
            )
            .await
            .unwrap();
        assert_eq!(output["data"], json!({"answer":"ok"}));

        let (invalid, _) = fake_operation(r#"{"answer":2}"#, false);
        assert_eq!(
            invalid
                .execute(
                    &config,
                    BTreeMap::from([(id("question"), json!("question"))]),
                    context(),
                )
                .await
                .unwrap_err()
                .code(),
            "VNEXT_LLM_RESPONSE_CONTRACT_INVALID"
        );
    }

    #[tokio::test]
    async fn aggregate_request_limit_cannot_be_bypassed_with_multiple_data_parts() {
        let (operation, requests) = fake_operation("answer", false);
        let mut config = text_config();
        config["max_request_bytes"] = json!(120);
        config["messages"][1]["parts"] = json!([
            {"kind":"data","input":"question"},
            {"kind":"data","input":"evidence"}
        ]);
        let error = operation
            .execute(
                &config,
                BTreeMap::from([
                    (id("question"), json!("q".repeat(40))),
                    (id("evidence"), json!("e".repeat(40))),
                ]),
                context(),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code(), "VNEXT_LLM_REQUEST_TOO_LARGE");
        assert!(requests.lock().unwrap().is_empty());
    }
}
