use super::{
    normalize_provider_stream, permanent, rejected, retryable_after_dispatch,
    validate_wire_descriptor, InstalledModelAdapterDescriptor, ModelAdapterCancelOutcome,
    ModelAdapterCancelRequest, ModelAdapterExecutionRequest, ModelAdapterFailure,
    ModelAdapterHostError, ModelProviderAdapter, ModelProviderWireConnector,
    ModelProviderWireEvent, ModelProviderWireProtocol, ModelProviderWireRequest,
    NormalizedFrameBuilder, NormalizedModelStream, ProviderEventCodec,
    OPENAI_RESPONSES_ADAPTER_NAME,
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

/// OpenAI Responses API adapter. HTTP, TLS, egress and credential handling stay in the connector.
pub struct OpenAiResponsesAdapter {
    descriptor: InstalledModelAdapterDescriptor,
    connector: Arc<dyn ModelProviderWireConnector>,
}

impl OpenAiResponsesAdapter {
    pub fn new(
        descriptor: InstalledModelAdapterDescriptor,
        connector: Arc<dyn ModelProviderWireConnector>,
    ) -> Result<Self, ModelAdapterHostError> {
        validate_wire_descriptor(&descriptor, OPENAI_RESPONSES_ADAPTER_NAME)?;
        Ok(Self {
            descriptor,
            connector,
        })
    }
}

#[async_trait]
impl ModelProviderAdapter for OpenAiResponsesAdapter {
    fn descriptor(&self) -> InstalledModelAdapterDescriptor {
        self.descriptor.clone()
    }

    async fn invoke(
        &self,
        request: ModelAdapterExecutionRequest,
    ) -> Result<NormalizedModelStream, ModelAdapterFailure> {
        let body = openai_request_body(&request)?;
        let wire = ModelProviderWireRequest::build(
            ModelProviderWireProtocol::OpenAiResponses,
            &request,
            body,
        )?;
        let upstream = self.connector.open(wire).await?;
        Ok(normalize_provider_stream(
            upstream,
            OpenAiResponsesCodec::new(&request),
            request.provider.request_limits.maximum_response_bytes,
        ))
    }

    async fn cancel(
        &self,
        request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        self.connector
            .cancel(ModelProviderWireProtocol::OpenAiResponses, request)
            .await
    }
}

fn openai_request_body(
    request: &ModelAdapterExecutionRequest,
) -> Result<Value, ModelAdapterFailure> {
    if !request.request.artifact_inputs.is_empty()
        || request.profile.usage.reports_cost
        || !request.profile.usage.provider_reports_usage
        || (request
            .request
            .response_contract
            .structured_schema
            .is_some()
            && !request.profile.structured_output.native)
    {
        return Err(rejected("openai_responses_profile_not_supported"));
    }

    let mut input = Vec::new();
    for message in &request.request.messages {
        if message.role == CanonicalMessageRole::Tool {
            for part in &message.parts {
                let CanonicalMessagePart::ToolResult(result) = part else {
                    return Err(rejected("openai_responses_message_not_supported"));
                };
                let ValueRef::Inline { value } = &result.value else {
                    return Err(rejected("openai_responses_artifact_not_supported"));
                };
                input.push(serde_json::json!({
                    "type": "function_call_output",
                    "call_id": result.call_id,
                    "output": serde_json::to_string(value)
                        .map_err(|_| rejected("openai_responses_tool_result_not_json"))?,
                }));
            }
            continue;
        }

        let role = match message.role {
            CanonicalMessageRole::Platform => "developer",
            CanonicalMessageRole::User => "user",
            CanonicalMessageRole::Assistant => "assistant",
            CanonicalMessageRole::Tool => unreachable!("tool messages are handled above"),
        };
        let content_type = if message.role == CanonicalMessageRole::Assistant {
            "output_text"
        } else {
            "input_text"
        };
        let mut content = Vec::new();
        for part in &message.parts {
            let CanonicalMessagePart::Text(text) = part else {
                return Err(rejected("openai_responses_artifact_not_supported"));
            };
            content.push(serde_json::json!({"type": content_type, "text": text}));
        }
        input.push(serde_json::json!({"role": role, "content": content}));
    }

    let mut body = Map::from_iter([
        (
            "model".to_owned(),
            Value::String(request.profile.model_identity.value.clone()),
        ),
        ("input".to_owned(), Value::Array(input)),
        (
            "max_output_tokens".to_owned(),
            Value::from(request.request.max_output_tokens),
        ),
        ("stream".to_owned(), Value::Bool(true)),
        ("store".to_owned(), Value::Bool(false)),
    ]);

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
                            "type": "function",
                            "name": tool.projected_name,
                            "description": "Published platform capability",
                            "parameters": tool.input_schema.schema,
                            "strict": true,
                        })
                    })
                    .collect(),
            ),
        );
        body.insert("tool_choice".to_owned(), Value::String("auto".to_owned()));
        body.insert(
            "parallel_tool_calls".to_owned(),
            Value::Bool(request.profile.tools.parallel),
        );
    }

    if let Some(schema) = &request.request.response_contract.structured_schema {
        body.insert(
            "text".to_owned(),
            serde_json::json!({
                "format": {
                    "type": "json_schema",
                    "name": "platform_output",
                    "schema": schema.schema,
                    "strict": true,
                }
            }),
        );
    }
    merge_generation_parameters(
        &mut body,
        &request.request.generation_parameters.value,
        &["temperature", "top_p", "reasoning", "service_tier"],
    )?;
    Ok(Value::Object(body))
}

pub(crate) fn merge_generation_parameters(
    body: &mut Map<String, Value>,
    parameters: &Value,
    allowed: &[&str],
) -> Result<(), ModelAdapterFailure> {
    let object = parameters
        .as_object()
        .ok_or_else(|| rejected("model_generation_parameters_not_object"))?;
    for (name, value) in object {
        if !allowed.contains(&name.as_str()) || body.contains_key(name) {
            return Err(rejected("model_generation_parameter_not_supported"));
        }
        body.insert(name.clone(), value.clone());
    }
    Ok(())
}

struct OpenAiResponsesCodec {
    request: ModelAdapterExecutionRequest,
    frames: NormalizedFrameBuilder,
    function_items: BTreeMap<String, (String, String)>,
    terminal: bool,
}

impl OpenAiResponsesCodec {
    fn new(request: &ModelAdapterExecutionRequest) -> Self {
        Self {
            request: request.clone(),
            frames: NormalizedFrameBuilder::new(request),
            function_items: BTreeMap::new(),
            terminal: false,
        }
    }

    fn completed(&mut self, response: Value) -> Result<NormalizedModelFrame, ModelAdapterFailure> {
        if self.terminal || response.get("status").and_then(Value::as_str) != Some("completed") {
            return Err(permanent("openai_responses_invalid_terminal"));
        }
        ensure_keys(
            &response,
            &[
                "id",
                "object",
                "created_at",
                "status",
                "background",
                "error",
                "incomplete_details",
                "instructions",
                "max_output_tokens",
                "max_tool_calls",
                "model",
                "output",
                "parallel_tool_calls",
                "previous_response_id",
                "prompt_cache_key",
                "prompt_cache_retention",
                "reasoning",
                "safety_identifier",
                "service_tier",
                "store",
                "system_fingerprint",
                "temperature",
                "text",
                "tool_choice",
                "tools",
                "top_logprobs",
                "top_p",
                "truncation",
                "usage",
                "user",
                "metadata",
            ],
        )?;
        self.terminal = true;
        let response_digest = digest_value(&response)?;
        let actual_model_identity = required_string(&response, "model")?.to_owned();
        let output = response
            .get("output")
            .and_then(Value::as_array)
            .ok_or_else(|| permanent("openai_responses_invalid_output"))?;
        let mut text = String::new();
        let mut tool_intents = Vec::new();

        for item in output {
            match item.get("type").and_then(Value::as_str) {
                Some("message") => {
                    ensure_keys(item, &["id", "type", "status", "role", "content"])?;
                    if item.get("role").and_then(Value::as_str) != Some("assistant") {
                        return Err(permanent("openai_responses_invalid_message"));
                    }
                    let content = item
                        .get("content")
                        .and_then(Value::as_array)
                        .ok_or_else(|| permanent("openai_responses_invalid_message"))?;
                    for part in content {
                        match part.get("type").and_then(Value::as_str) {
                            Some("output_text") => {
                                ensure_keys(part, &["type", "text", "annotations", "logprobs"])?;
                                text.push_str(required_string(part, "text")?);
                            }
                            Some("refusal") => {
                                return Err(permanent("openai_responses_content_filtered"));
                            }
                            _ => return Err(permanent("openai_responses_unknown_content")),
                        }
                    }
                }
                Some("function_call") => {
                    ensure_keys(
                        item,
                        &["id", "type", "status", "call_id", "name", "arguments"],
                    )?;
                    let name = required_string(item, "name")?;
                    let call_id = required_string(item, "call_id")?;
                    let projection = self
                        .request
                        .request
                        .tools
                        .iter()
                        .find(|tool| tool.projected_name == name)
                        .ok_or_else(|| permanent("openai_responses_unknown_tool"))?;
                    let arguments: Value =
                        serde_json::from_str(required_string(item, "arguments")?)
                            .map_err(|_| permanent("openai_responses_invalid_tool_arguments"))?;
                    tool_intents.push(ModelToolIntent {
                        call_id: call_id.to_owned(),
                        projected_tool_name: name.to_owned(),
                        arguments: ClosedJsonValue::build(
                            projection.input_schema.canonical_digest.clone(),
                            arguments,
                        )
                        .map_err(|_| permanent("openai_responses_invalid_tool_arguments"))?,
                    });
                }
                Some("reasoning") => {
                    ensure_keys(
                        item,
                        &[
                            "id",
                            "type",
                            "status",
                            "summary",
                            "content",
                            "encrypted_content",
                        ],
                    )?;
                }
                _ => return Err(permanent("openai_responses_unknown_output")),
            }
        }

        let structured_output =
            if let Some(schema) = &self.request.request.response_contract.structured_schema {
                let value = serde_json::from_str(&text)
                    .map_err(|_| permanent("openai_responses_invalid_structured_output"))?;
                Some(
                    ClosedJsonValue::build(schema.canonical_digest.clone(), value)
                        .map_err(|_| permanent("openai_responses_invalid_structured_output"))?,
                )
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
        let finish_reason = if tool_intents.is_empty() {
            CanonicalFinishReason::Completed
        } else {
            CanonicalFinishReason::ToolUse
        };
        let usage = response
            .get("usage")
            .ok_or_else(|| permanent("openai_responses_missing_usage"))?;
        ensure_keys(
            usage,
            &[
                "input_tokens",
                "input_tokens_details",
                "output_tokens",
                "output_tokens_details",
                "total_tokens",
            ],
        )?;
        let input_tokens = required_u64(usage, "input_tokens")?;
        let output_tokens = required_u64(usage, "output_tokens")?;
        let cached_input_tokens = if self.request.profile.usage.reports_cached_input_tokens {
            let details = usage
                .get("input_tokens_details")
                .ok_or_else(|| permanent("openai_responses_missing_usage"))?;
            ensure_keys(details, &["cached_tokens"])?;
            Some(required_u64(details, "cached_tokens")?)
        } else {
            None
        };
        let reasoning_tokens = if self.request.profile.usage.reports_reasoning_tokens {
            let details = usage
                .get("output_tokens_details")
                .ok_or_else(|| permanent("openai_responses_missing_usage"))?;
            ensure_keys(details, &["reasoning_tokens"])?;
            Some(required_u64(details, "reasoning_tokens")?)
        } else {
            None
        };
        let model_fingerprint = optional_string(&response, "system_fingerprint")?;

        self.frames.terminal(CanonicalModelResponse {
            schema_version: 1,
            message,
            structured_output,
            tool_intents,
            finish_reason,
            usage: ModelUsage {
                input_tokens: Some(input_tokens),
                output_tokens: Some(output_tokens),
                cached_input_tokens,
                reasoning_tokens,
                provider_reported_cost: None,
                accounting_quality: AccountingQuality::ProviderReported,
            },
            observation: ModelObservation {
                request_sent: true,
                provider_response_digest: Some(response_digest),
                actual_model_identity: Some(actual_model_identity),
                model_fingerprint,
                possible_duplicate_charge: false,
                stream_delta_count: self.frames.delta_count(),
                stream_bytes: self.frames.delta_bytes(),
            },
        })
    }
}

impl ProviderEventCodec for OpenAiResponsesCodec {
    fn accept(
        &mut self,
        event: ModelProviderWireEvent,
    ) -> Result<Option<NormalizedModelFrame>, ModelAdapterFailure> {
        if self.terminal {
            return Err(permanent("openai_responses_event_after_terminal"));
        }
        let data_type = event
            .data
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| permanent("openai_responses_missing_event_type"))?;
        if event.event_name != data_type {
            return Err(permanent("openai_responses_event_type_mismatch"));
        }
        match data_type {
            "response.created" | "response.in_progress" => {
                ensure_keys(&event.data, &["type", "sequence_number", "response"])?;
                Ok(None)
            }
            "response.content_part.added" | "response.content_part.done" => {
                ensure_keys(
                    &event.data,
                    &[
                        "type",
                        "sequence_number",
                        "item_id",
                        "output_index",
                        "content_index",
                        "part",
                    ],
                )?;
                Ok(None)
            }
            "response.output_item.done" => {
                ensure_keys(
                    &event.data,
                    &["type", "sequence_number", "output_index", "item"],
                )?;
                Ok(None)
            }
            "response.output_item.added" => {
                ensure_keys(
                    &event.data,
                    &["type", "sequence_number", "output_index", "item"],
                )?;
                let item = event
                    .data
                    .get("item")
                    .ok_or_else(|| permanent("openai_responses_invalid_output_item"))?;
                match item.get("type").and_then(Value::as_str) {
                    Some("function_call") => {
                        ensure_keys(
                            item,
                            &["id", "type", "status", "call_id", "name", "arguments"],
                        )?;
                        let name = required_string(item, "name")?;
                        if !self
                            .request
                            .request
                            .tools
                            .iter()
                            .any(|tool| tool.projected_name == name)
                        {
                            return Err(permanent("openai_responses_unknown_tool"));
                        }
                        let previous = self.function_items.insert(
                            required_string(item, "id")?.to_owned(),
                            (
                                required_string(item, "call_id")?.to_owned(),
                                name.to_owned(),
                            ),
                        );
                        if previous.is_some() {
                            return Err(permanent("openai_responses_duplicate_output_item"));
                        }
                    }
                    Some("message") | Some("reasoning") => {}
                    _ => return Err(permanent("openai_responses_unknown_output")),
                }
                Ok(None)
            }
            "response.output_text.delta" => {
                ensure_keys(
                    &event.data,
                    &[
                        "type",
                        "sequence_number",
                        "item_id",
                        "output_index",
                        "content_index",
                        "delta",
                        "logprobs",
                    ],
                )?;
                Ok(Some(self.frames.live(NormalizedModelDelta::Text(
                    required_string(&event.data, "delta")?.to_owned(),
                ))?))
            }
            "response.output_text.done" => {
                ensure_keys(
                    &event.data,
                    &[
                        "type",
                        "sequence_number",
                        "item_id",
                        "output_index",
                        "content_index",
                        "text",
                        "logprobs",
                    ],
                )?;
                Ok(None)
            }
            "response.output_text.annotation.added" => {
                ensure_keys(
                    &event.data,
                    &[
                        "type",
                        "sequence_number",
                        "item_id",
                        "output_index",
                        "content_index",
                        "annotation_index",
                        "annotation",
                    ],
                )?;
                Ok(None)
            }
            "response.function_call_arguments.delta" => {
                ensure_keys(
                    &event.data,
                    &[
                        "type",
                        "sequence_number",
                        "item_id",
                        "output_index",
                        "delta",
                    ],
                )?;
                let item_id = required_string(&event.data, "item_id")?;
                let (call_id, name) = self
                    .function_items
                    .get(item_id)
                    .ok_or_else(|| permanent("openai_responses_unknown_function_item"))?;
                Ok(Some(self.frames.live(
                    NormalizedModelDelta::ToolArguments {
                        call_id: call_id.clone(),
                        projected_tool_name: name.clone(),
                        fragment: required_string(&event.data, "delta")?.to_owned(),
                    },
                )?))
            }
            "response.function_call_arguments.done" => {
                ensure_keys(
                    &event.data,
                    &[
                        "type",
                        "sequence_number",
                        "item_id",
                        "output_index",
                        "arguments",
                    ],
                )?;
                Ok(None)
            }
            "response.reasoning_summary_part.added"
            | "response.reasoning_summary_part.done"
            | "response.reasoning_summary_text.delta"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_text.delta"
            | "response.reasoning_text.done" => {
                // Hidden reasoning never crosses the normalized/public stream boundary.
                Ok(None)
            }
            "response.completed" => {
                ensure_keys(&event.data, &["type", "sequence_number", "response"])?;
                let response = event
                    .data
                    .get("response")
                    .cloned()
                    .ok_or_else(|| permanent("openai_responses_missing_terminal"))?;
                self.completed(response).map(Some)
            }
            "error" | "response.failed" | "response.incomplete" => {
                Err(permanent("openai_responses_provider_error"))
            }
            _ => Err(permanent("openai_responses_unknown_event")),
        }
    }

    fn missing_terminal(&self) -> ModelAdapterFailure {
        retryable_after_dispatch(
            "openai_responses_missing_terminal",
            self.request.request.deadline,
        )
    }
}

fn ensure_keys(value: &Value, allowed: &[&str]) -> Result<(), ModelAdapterFailure> {
    let object = value
        .as_object()
        .ok_or_else(|| permanent("openai_responses_invalid_object"))?;
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(permanent("openai_responses_unknown_field"));
    }
    Ok(())
}

fn required_string<'a>(value: &'a Value, name: &str) -> Result<&'a str, ModelAdapterFailure> {
    value
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && !value.contains('\0'))
        .ok_or_else(|| permanent("openai_responses_invalid_field"))
}

fn optional_string(value: &Value, name: &str) -> Result<Option<String>, ModelAdapterFailure> {
    match value.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.is_empty() && !value.contains('\0') => {
            Ok(Some(value.clone()))
        }
        Some(_) => Err(permanent("openai_responses_invalid_field")),
    }
}

fn required_u64(value: &Value, name: &str) -> Result<u64, ModelAdapterFailure> {
    value
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| permanent("openai_responses_invalid_usage"))
}

fn digest_value(
    value: &Value,
) -> Result<insight_platform_contracts::Sha256Digest, ModelAdapterFailure> {
    canonical_digest(value)
        .map_err(|_| permanent("openai_responses_invalid_terminal"))?
        .parse()
        .map_err(|_| permanent("openai_responses_invalid_terminal"))
}
