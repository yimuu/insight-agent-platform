use super::{
    merge_generation_parameters, normalize_provider_stream, permanent, rejected,
    retryable_after_dispatch, validate_wire_descriptor, InstalledModelAdapterDescriptor,
    ModelAdapterCancelOutcome, ModelAdapterCancelRequest, ModelAdapterExecutionRequest,
    ModelAdapterFailure, ModelAdapterHostError, ModelProviderAdapter, ModelProviderWireConnector,
    ModelProviderWireEvent, ModelProviderWireProtocol, ModelProviderWireRequest,
    NormalizedFrameBuilder, NormalizedModelStream, ProviderEventCodec,
    ANTHROPIC_MESSAGES_ADAPTER_NAME,
};
use async_trait::async_trait;
use insight_platform_contracts::{canonical_digest, ClosedJsonValue, ValueRef};
use insight_platform_models::{
    AccountingQuality, CanonicalAssistantMessage, CanonicalFinishReason, CanonicalMessagePart,
    CanonicalMessageRole, CanonicalModelResponse, ModelObservation, ModelToolIntent, ModelUsage,
    NormalizedModelDelta, NormalizedModelFrame,
};
use serde_json::{Map, Value};
use std::{collections::BTreeMap, sync::Arc};

/// Anthropic Messages API adapter for protocol version `2023-06-01`.
pub struct AnthropicMessagesAdapter {
    descriptor: InstalledModelAdapterDescriptor,
    connector: Arc<dyn ModelProviderWireConnector>,
}

impl AnthropicMessagesAdapter {
    pub fn new(
        descriptor: InstalledModelAdapterDescriptor,
        connector: Arc<dyn ModelProviderWireConnector>,
    ) -> Result<Self, ModelAdapterHostError> {
        validate_wire_descriptor(&descriptor, ANTHROPIC_MESSAGES_ADAPTER_NAME)?;
        Ok(Self {
            descriptor,
            connector,
        })
    }
}

#[async_trait]
impl ModelProviderAdapter for AnthropicMessagesAdapter {
    fn descriptor(&self) -> InstalledModelAdapterDescriptor {
        self.descriptor.clone()
    }

    async fn invoke(
        &self,
        request: ModelAdapterExecutionRequest,
    ) -> Result<NormalizedModelStream, ModelAdapterFailure> {
        let body = anthropic_request_body(&request)?;
        let wire = ModelProviderWireRequest::build(
            ModelProviderWireProtocol::AnthropicMessages,
            &request,
            body,
        )?;
        let upstream = self.connector.open(wire).await?;
        Ok(normalize_provider_stream(
            upstream,
            AnthropicMessagesCodec::new(&request),
            request.provider.request_limits.maximum_response_bytes,
        ))
    }

    async fn cancel(
        &self,
        request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        self.connector
            .cancel(ModelProviderWireProtocol::AnthropicMessages, request)
            .await
    }
}

fn anthropic_request_body(
    request: &ModelAdapterExecutionRequest,
) -> Result<Value, ModelAdapterFailure> {
    if request.profile.usage.reports_cost
        || request.profile.usage.reports_reasoning_tokens
        || !request.profile.usage.provider_reports_usage
        || (request
            .request
            .response_contract
            .structured_schema
            .is_some()
            && !request.profile.structured_output.native)
    {
        return Err(rejected("anthropic_messages_profile_not_supported"));
    }

    let mut system = Vec::new();
    let mut messages = Vec::new();
    let mut saw_conversation_message = false;
    for message in &request.request.messages {
        if message.role == CanonicalMessageRole::Platform {
            if saw_conversation_message {
                return Err(rejected("anthropic_messages_system_order"));
            }
            for part in &message.parts {
                let CanonicalMessagePart::Text(text) = part else {
                    return Err(rejected("anthropic_messages_artifact_not_supported"));
                };
                system.push(serde_json::json!({"type": "text", "text": text}));
            }
            continue;
        }
        saw_conversation_message = true;

        if message.role == CanonicalMessageRole::Tool {
            let mut content = Vec::new();
            for part in &message.parts {
                let CanonicalMessagePart::ToolResult(result) = part else {
                    return Err(rejected("anthropic_messages_message_not_supported"));
                };
                let ValueRef::Inline { value } = &result.value else {
                    return Err(rejected("anthropic_messages_artifact_not_supported"));
                };
                content.push(serde_json::json!({
                    "type": "tool_result",
                    "tool_use_id": result.call_id,
                    "content": serde_json::to_string(value)
                        .map_err(|_| rejected("anthropic_messages_tool_result_not_json"))?,
                }));
            }
            messages.push(serde_json::json!({"role": "user", "content": content}));
            continue;
        }

        let role = match message.role {
            CanonicalMessageRole::User => "user",
            CanonicalMessageRole::Assistant => "assistant",
            CanonicalMessageRole::Platform | CanonicalMessageRole::Tool => {
                unreachable!("platform and tool messages are handled above")
            }
        };
        let mut content = Vec::new();
        for part in &message.parts {
            let CanonicalMessagePart::Text(text) = part else {
                return Err(rejected("anthropic_messages_artifact_not_supported"));
            };
            content.push(serde_json::json!({"type": "text", "text": text}));
        }
        messages.push(serde_json::json!({"role": role, "content": content}));
    }
    if messages.is_empty() {
        return Err(rejected("anthropic_messages_missing_user_message"));
    }

    let mut body = Map::from_iter([
        (
            "model".to_owned(),
            Value::String(request.profile.model_identity.value.clone()),
        ),
        (
            "max_tokens".to_owned(),
            Value::from(request.request.max_output_tokens),
        ),
        ("messages".to_owned(), Value::Array(messages)),
        ("stream".to_owned(), Value::Bool(true)),
    ]);
    if !system.is_empty() {
        body.insert("system".to_owned(), Value::Array(system));
    }

    if !request.request.tools.is_empty() {
        body.insert(
            "tools".to_owned(),
            Value::Array(
                request
                    .request
                    .tools
                    .iter()
                    .map(|tool| {
                        serde_json::json!({
                            "name": tool.projected_name,
                            "description": "Published platform capability",
                            "input_schema": tool.input_schema.schema,
                            "strict": true,
                        })
                    })
                    .collect(),
            ),
        );
        body.insert(
            "tool_choice".to_owned(),
            serde_json::json!({
                "type": "auto",
                "disable_parallel_tool_use": !request.profile.tools.parallel,
            }),
        );
    }

    if let Some(schema) = &request.request.response_contract.structured_schema {
        body.insert(
            "output_config".to_owned(),
            serde_json::json!({
                "format": {
                    "type": "json_schema",
                    "schema": schema.schema,
                }
            }),
        );
    }
    merge_generation_parameters(
        &mut body,
        &request.request.generation_parameters.value,
        &[
            "temperature",
            "top_p",
            "top_k",
            "stop_sequences",
            "thinking",
        ],
    )?;
    Ok(Value::Object(body))
}

enum AnthropicContentBlock {
    Text {
        value: String,
        stopped: bool,
    },
    Tool {
        call_id: String,
        name: String,
        arguments: String,
        stopped: bool,
    },
    HiddenReasoning {
        stopped: bool,
    },
}

impl AnthropicContentBlock {
    fn stopped(&self) -> bool {
        match self {
            Self::Text { stopped, .. }
            | Self::Tool { stopped, .. }
            | Self::HiddenReasoning { stopped } => *stopped,
        }
    }

    fn mark_stopped(&mut self) -> Result<(), ModelAdapterFailure> {
        let stopped = match self {
            Self::Text { stopped, .. }
            | Self::Tool { stopped, .. }
            | Self::HiddenReasoning { stopped } => stopped,
        };
        if *stopped {
            return Err(permanent("anthropic_messages_duplicate_block_stop"));
        }
        *stopped = true;
        Ok(())
    }
}

struct AnthropicMessagesCodec {
    request: ModelAdapterExecutionRequest,
    frames: NormalizedFrameBuilder,
    provider_message_id: Option<String>,
    actual_model_identity: Option<String>,
    input_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    stop_reason: Option<String>,
    blocks: BTreeMap<u64, AnthropicContentBlock>,
    terminal: bool,
}

impl AnthropicMessagesCodec {
    fn new(request: &ModelAdapterExecutionRequest) -> Self {
        Self {
            request: request.clone(),
            frames: NormalizedFrameBuilder::new(request),
            provider_message_id: None,
            actual_model_identity: None,
            input_tokens: None,
            cached_input_tokens: None,
            output_tokens: None,
            stop_reason: None,
            blocks: BTreeMap::new(),
            terminal: false,
        }
    }

    fn message_start(&mut self, data: &Value) -> Result<(), ModelAdapterFailure> {
        if self.provider_message_id.is_some() {
            return Err(permanent("anthropic_messages_duplicate_start"));
        }
        ensure_keys(data, &["type", "message"])?;
        let message = data
            .get("message")
            .ok_or_else(|| permanent("anthropic_messages_invalid_start"))?;
        ensure_keys(
            message,
            &[
                "id",
                "type",
                "role",
                "model",
                "content",
                "stop_reason",
                "stop_sequence",
                "usage",
                "container",
                "context_management",
            ],
        )?;
        if message.get("type").and_then(Value::as_str) != Some("message")
            || message.get("role").and_then(Value::as_str) != Some("assistant")
            || !message
                .get("content")
                .and_then(Value::as_array)
                .is_some_and(Vec::is_empty)
        {
            return Err(permanent("anthropic_messages_invalid_start"));
        }
        let usage = message
            .get("usage")
            .ok_or_else(|| permanent("anthropic_messages_missing_usage"))?;
        ensure_usage_keys(usage)?;
        self.provider_message_id = Some(required_string(message, "id")?.to_owned());
        self.actual_model_identity = Some(required_string(message, "model")?.to_owned());
        self.input_tokens = Some(required_u64(usage, "input_tokens")?);
        self.cached_input_tokens = if self.request.profile.usage.reports_cached_input_tokens {
            Some(required_u64(usage, "cache_read_input_tokens")?)
        } else {
            None
        };
        Ok(())
    }

    fn block_start(
        &mut self,
        data: &Value,
    ) -> Result<Option<NormalizedModelFrame>, ModelAdapterFailure> {
        self.require_started()?;
        ensure_keys(data, &["type", "index", "content_block"])?;
        let index = required_u64(data, "index")?;
        let block = data
            .get("content_block")
            .ok_or_else(|| permanent("anthropic_messages_invalid_block"))?;
        let (content, live) = match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                ensure_keys(block, &["type", "text", "citations"])?;
                let initial = optional_string(block, "text")?.unwrap_or_default();
                let live = if initial.is_empty() {
                    None
                } else {
                    Some(
                        self.frames
                            .live(NormalizedModelDelta::Text(initial.clone()))?,
                    )
                };
                (
                    AnthropicContentBlock::Text {
                        value: initial,
                        stopped: false,
                    },
                    live,
                )
            }
            Some("tool_use") => {
                ensure_keys(block, &["type", "id", "name", "input"])?;
                let name = required_string(block, "name")?;
                if !self
                    .request
                    .request
                    .tools
                    .iter()
                    .any(|tool| tool.projected_name == name)
                {
                    return Err(permanent("anthropic_messages_unknown_tool"));
                }
                if !block
                    .get("input")
                    .and_then(Value::as_object)
                    .is_some_and(Map::is_empty)
                {
                    return Err(permanent("anthropic_messages_nonempty_initial_tool_input"));
                }
                (
                    AnthropicContentBlock::Tool {
                        call_id: required_string(block, "id")?.to_owned(),
                        name: name.to_owned(),
                        arguments: String::new(),
                        stopped: false,
                    },
                    None,
                )
            }
            Some("thinking") => {
                ensure_keys(block, &["type", "thinking", "signature"])?;
                (
                    AnthropicContentBlock::HiddenReasoning { stopped: false },
                    None,
                )
            }
            Some("redacted_thinking") => {
                ensure_keys(block, &["type", "data"])?;
                (
                    AnthropicContentBlock::HiddenReasoning { stopped: false },
                    None,
                )
            }
            _ => return Err(permanent("anthropic_messages_unknown_block")),
        };
        if self.blocks.insert(index, content).is_some() {
            return Err(permanent("anthropic_messages_duplicate_block"));
        }
        Ok(live)
    }

    fn block_delta(
        &mut self,
        data: &Value,
    ) -> Result<Option<NormalizedModelFrame>, ModelAdapterFailure> {
        ensure_keys(data, &["type", "index", "delta"])?;
        let index = required_u64(data, "index")?;
        let delta = data
            .get("delta")
            .ok_or_else(|| permanent("anthropic_messages_invalid_delta"))?;
        let block = self
            .blocks
            .get_mut(&index)
            .ok_or_else(|| permanent("anthropic_messages_unknown_block"))?;
        if block.stopped() {
            return Err(permanent("anthropic_messages_delta_after_block_stop"));
        }
        match (block, delta.get("type").and_then(Value::as_str)) {
            (AnthropicContentBlock::Text { value, .. }, Some("text_delta")) => {
                ensure_keys(delta, &["type", "text"])?;
                let fragment = required_string(delta, "text")?.to_owned();
                value.push_str(&fragment);
                Ok(Some(
                    self.frames.live(NormalizedModelDelta::Text(fragment))?,
                ))
            }
            (
                AnthropicContentBlock::Tool {
                    call_id,
                    name,
                    arguments,
                    ..
                },
                Some("input_json_delta"),
            ) => {
                ensure_keys(delta, &["type", "partial_json"])?;
                let fragment = required_string(delta, "partial_json")?.to_owned();
                arguments.push_str(&fragment);
                Ok(Some(self.frames.live(
                    NormalizedModelDelta::ToolArguments {
                        call_id: call_id.clone(),
                        projected_tool_name: name.clone(),
                        fragment,
                    },
                )?))
            }
            (AnthropicContentBlock::HiddenReasoning { .. }, Some("thinking_delta")) => {
                ensure_keys(delta, &["type", "thinking"])?;
                Ok(None)
            }
            (AnthropicContentBlock::HiddenReasoning { .. }, Some("signature_delta")) => {
                ensure_keys(delta, &["type", "signature"])?;
                Ok(None)
            }
            (AnthropicContentBlock::Text { .. }, Some("citations_delta")) => {
                ensure_keys(delta, &["type", "citation"])?;
                Ok(None)
            }
            _ => Err(permanent("anthropic_messages_delta_type_mismatch")),
        }
    }

    fn message_delta(&mut self, data: &Value) -> Result<(), ModelAdapterFailure> {
        self.require_started()?;
        if self.stop_reason.is_some() {
            return Err(permanent("anthropic_messages_duplicate_message_delta"));
        }
        ensure_keys(data, &["type", "delta", "usage", "context_management"])?;
        let delta = data
            .get("delta")
            .ok_or_else(|| permanent("anthropic_messages_invalid_message_delta"))?;
        ensure_keys(delta, &["stop_reason", "stop_sequence"])?;
        self.stop_reason = Some(required_string(delta, "stop_reason")?.to_owned());
        let usage = data
            .get("usage")
            .ok_or_else(|| permanent("anthropic_messages_missing_usage"))?;
        ensure_usage_keys(usage)?;
        self.output_tokens = Some(required_u64(usage, "output_tokens")?);
        Ok(())
    }

    fn completed(&mut self) -> Result<NormalizedModelFrame, ModelAdapterFailure> {
        self.require_started()?;
        if self.terminal
            || self.blocks.values().any(|block| !block.stopped())
            || self.stop_reason.is_none()
        {
            return Err(permanent("anthropic_messages_invalid_terminal"));
        }
        self.terminal = true;

        let mut text = String::new();
        let mut tool_intents = Vec::new();
        let mut digest_blocks = Vec::new();
        for (index, block) in &self.blocks {
            match block {
                AnthropicContentBlock::Text { value, .. } => {
                    text.push_str(value);
                    digest_blocks.push(serde_json::json!({
                        "index": index,
                        "type": "text",
                        "text": value,
                    }));
                }
                AnthropicContentBlock::Tool {
                    call_id,
                    name,
                    arguments,
                    ..
                } => {
                    let projection = self
                        .request
                        .request
                        .tools
                        .iter()
                        .find(|tool| tool.projected_name == *name)
                        .ok_or_else(|| permanent("anthropic_messages_unknown_tool"))?;
                    let argument_value: Value = serde_json::from_str(arguments)
                        .map_err(|_| permanent("anthropic_messages_invalid_tool_arguments"))?;
                    tool_intents.push(ModelToolIntent {
                        call_id: call_id.clone(),
                        projected_tool_name: name.clone(),
                        arguments: ClosedJsonValue::build(
                            projection.input_schema.canonical_digest.clone(),
                            argument_value.clone(),
                        )
                        .map_err(|_| permanent("anthropic_messages_invalid_tool_arguments"))?,
                    });
                    digest_blocks.push(serde_json::json!({
                        "index": index,
                        "type": "tool_use",
                        "id": call_id,
                        "name": name,
                        "input": argument_value,
                    }));
                }
                AnthropicContentBlock::HiddenReasoning { .. } => {
                    digest_blocks.push(serde_json::json!({
                        "index": index,
                        "type": "hidden_reasoning",
                    }));
                }
            }
        }

        let stop_reason = self
            .stop_reason
            .as_deref()
            .ok_or_else(|| permanent("anthropic_messages_invalid_terminal"))?;
        let finish_reason = match (stop_reason, tool_intents.is_empty()) {
            ("end_turn" | "stop_sequence", true) => CanonicalFinishReason::Completed,
            ("tool_use", false) => CanonicalFinishReason::ToolUse,
            ("max_tokens" | "model_context_window_exceeded", _) => {
                return Err(permanent("anthropic_messages_incomplete"));
            }
            ("refusal", _) => return Err(permanent("anthropic_messages_content_filtered")),
            _ => return Err(permanent("anthropic_messages_unknown_finish_reason")),
        };

        let structured_output = if tool_intents.is_empty() {
            if let Some(schema) = &self.request.request.response_contract.structured_schema {
                let value = serde_json::from_str(&text)
                    .map_err(|_| permanent("anthropic_messages_invalid_structured_output"))?;
                Some(
                    ClosedJsonValue::build(schema.canonical_digest.clone(), value)
                        .map_err(|_| permanent("anthropic_messages_invalid_structured_output"))?,
                )
            } else {
                None
            }
        } else {
            None
        };
        let message = if structured_output.is_none() && !text.is_empty() {
            Some(CanonicalAssistantMessage {
                parts: vec![CanonicalMessagePart::Text(text)],
                classification: self.request.request.classification,
            })
        } else {
            None
        };
        let input_tokens = self
            .input_tokens
            .ok_or_else(|| permanent("anthropic_messages_missing_usage"))?;
        let output_tokens = self
            .output_tokens
            .ok_or_else(|| permanent("anthropic_messages_missing_usage"))?;
        let actual_model_identity = self
            .actual_model_identity
            .clone()
            .ok_or_else(|| permanent("anthropic_messages_invalid_terminal"))?;
        let response_digest = digest_value(&serde_json::json!({
            "id": self.provider_message_id,
            "type": "message",
            "role": "assistant",
            "model": actual_model_identity,
            "content": digest_blocks,
            "stop_reason": stop_reason,
            "usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read_input_tokens": self.cached_input_tokens,
            },
        }))?;

        self.frames.terminal(CanonicalModelResponse {
            schema_version: 1,
            message,
            structured_output,
            tool_intents,
            finish_reason,
            usage: ModelUsage {
                input_tokens: Some(input_tokens),
                output_tokens: Some(output_tokens),
                cached_input_tokens: self.cached_input_tokens,
                reasoning_tokens: None,
                provider_reported_cost: None,
                accounting_quality: AccountingQuality::ProviderReported,
            },
            observation: ModelObservation {
                request_sent: true,
                provider_response_digest: Some(response_digest),
                actual_model_identity: Some(actual_model_identity),
                model_fingerprint: None,
                possible_duplicate_charge: false,
                stream_delta_count: self.frames.delta_count(),
                stream_bytes: self.frames.delta_bytes(),
            },
        })
    }

    fn require_started(&self) -> Result<(), ModelAdapterFailure> {
        if self.provider_message_id.is_none() {
            return Err(permanent("anthropic_messages_event_before_start"));
        }
        Ok(())
    }
}

impl ProviderEventCodec for AnthropicMessagesCodec {
    fn accept(
        &mut self,
        event: ModelProviderWireEvent,
    ) -> Result<Option<NormalizedModelFrame>, ModelAdapterFailure> {
        if self.terminal {
            return Err(permanent("anthropic_messages_event_after_terminal"));
        }
        let data_type = event
            .data
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| permanent("anthropic_messages_missing_event_type"))?;
        if event.event_name != data_type {
            return Err(permanent("anthropic_messages_event_type_mismatch"));
        }
        match data_type {
            "message_start" => {
                self.message_start(&event.data)?;
                Ok(None)
            }
            "content_block_start" => self.block_start(&event.data),
            "content_block_delta" => self.block_delta(&event.data),
            "content_block_stop" => {
                ensure_keys(&event.data, &["type", "index"])?;
                let index = required_u64(&event.data, "index")?;
                self.blocks
                    .get_mut(&index)
                    .ok_or_else(|| permanent("anthropic_messages_unknown_block"))?
                    .mark_stopped()?;
                Ok(None)
            }
            "message_delta" => {
                self.message_delta(&event.data)?;
                Ok(None)
            }
            "message_stop" => {
                ensure_keys(&event.data, &["type"])?;
                self.completed().map(Some)
            }
            "ping" => {
                ensure_keys(&event.data, &["type"])?;
                Ok(None)
            }
            "error" => {
                ensure_keys(&event.data, &["type", "error"])?;
                let error = event
                    .data
                    .get("error")
                    .ok_or_else(|| permanent("anthropic_messages_invalid_error"))?;
                ensure_keys(error, &["type", "message"])?;
                match required_string(error, "type")? {
                    "overloaded_error" | "rate_limit_error" | "api_error" => {
                        Err(retryable_after_dispatch(
                            "anthropic_messages_provider_unavailable",
                            self.request.request.deadline,
                        ))
                    }
                    _ => Err(permanent("anthropic_messages_provider_error")),
                }
            }
            _ => Err(permanent("anthropic_messages_unknown_event")),
        }
    }

    fn missing_terminal(&self) -> ModelAdapterFailure {
        retryable_after_dispatch(
            "anthropic_messages_missing_terminal",
            self.request.request.deadline,
        )
    }
}

fn ensure_usage_keys(value: &Value) -> Result<(), ModelAdapterFailure> {
    ensure_keys(
        value,
        &[
            "input_tokens",
            "output_tokens",
            "cache_creation_input_tokens",
            "cache_read_input_tokens",
            "server_tool_use",
            "cache_creation",
        ],
    )
}

fn ensure_keys(value: &Value, allowed: &[&str]) -> Result<(), ModelAdapterFailure> {
    let object = value
        .as_object()
        .ok_or_else(|| permanent("anthropic_messages_invalid_object"))?;
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(permanent("anthropic_messages_unknown_field"));
    }
    Ok(())
}

fn required_string<'a>(value: &'a Value, name: &str) -> Result<&'a str, ModelAdapterFailure> {
    value
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && !value.contains('\0'))
        .ok_or_else(|| permanent("anthropic_messages_invalid_field"))
}

fn optional_string(value: &Value, name: &str) -> Result<Option<String>, ModelAdapterFailure> {
    match value.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.contains('\0') => Ok(Some(value.clone())),
        Some(_) => Err(permanent("anthropic_messages_invalid_field")),
    }
}

fn required_u64(value: &Value, name: &str) -> Result<u64, ModelAdapterFailure> {
    value
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| permanent("anthropic_messages_invalid_usage"))
}

fn digest_value(
    value: &Value,
) -> Result<insight_platform_contracts::Sha256Digest, ModelAdapterFailure> {
    canonical_digest(value)
        .map_err(|_| permanent("anthropic_messages_invalid_terminal"))?
        .parse()
        .map_err(|_| permanent("anthropic_messages_invalid_terminal"))
}
