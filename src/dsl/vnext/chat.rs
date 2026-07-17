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
        model_response_too_large, ChatContent, ChatContentPart, ChatMessage, ChatRequest, ChatRole,
        ImageUrl, ModelCapability, ModelRegistry,
    },
    runtime::{RunError, StopReason},
};

use super::{
    operation::{
        CompiledOperationContract, Operation, OperationContext, OperationEffect, OperationError,
    },
    schema::compile_contract_schema,
    types::{SchemaType, ValueType},
    value::Identifier,
};

pub const AI_CHAT_USES: &str = "ai.chat";
pub const DEFAULT_MAX_CHAT_REQUEST_BYTES: usize = 262_144;
pub const MAX_CHAT_REQUEST_BYTES: usize = 1_048_576;

const CHAT_CONFIG_INVALID: &str = "VNEXT_CHAT_CONFIG_INVALID";
const CHAT_MODEL_NOT_FOUND: &str = "VNEXT_CHAT_MODEL_NOT_FOUND";
const CHAT_PARAMETERS_INVALID: &str = "VNEXT_CHAT_PARAMETERS_INVALID";
const CHAT_MESSAGES_INVALID: &str = "VNEXT_CHAT_MESSAGES_INVALID";
const CHAT_INPUT_CONTRACT_INVALID: &str = "VNEXT_CHAT_INPUT_CONTRACT_INVALID";
const CHAT_PROMPT_NOT_FOUND: &str = "VNEXT_CHAT_PROMPT_NOT_FOUND";
const CHAT_VISION_REQUIRED: &str = "VNEXT_CHAT_VISION_REQUIRED";
const CHAT_RESPONSE_SCHEMA_INVALID: &str = "VNEXT_CHAT_RESPONSE_SCHEMA_INVALID";
const CHAT_DATA_TOO_LARGE: &str = "VNEXT_CHAT_DATA_TOO_LARGE";
const CHAT_RESPONSE_JSON_INVALID: &str = "VNEXT_CHAT_RESPONSE_JSON_INVALID";
const CHAT_RESPONSE_CONTRACT_INVALID: &str = "VNEXT_CHAT_RESPONSE_CONTRACT_INVALID";

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
                    content_parts.push(ChatContentPart::ImageUrl {
                        image_url: ImageUrl { url },
                    });
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
                image @ ChatContentPart::ImageUrl { .. } => ChatContent::Parts(vec![image]),
            }
        } else {
            ChatContent::Parts(content_parts)
        };
        Ok(ChatMessage {
            role: message.role,
            content,
        })
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
        dsl::vnext::{
            ir::OperationId,
            operation::{Operation, OperationContext},
            types::ValueType,
            value::Identifier,
        },
        resources::models::{
            ChatChunk, ChatModel, ChatRequest, ChatStream, ModelCapability, ModelRegistry,
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
            "VNEXT_CHAT_MESSAGES_INVALID"
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
            "VNEXT_CHAT_RESPONSE_CONTRACT_INVALID"
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

        assert_eq!(error.code(), "VNEXT_CHAT_DATA_TOO_LARGE");
        assert!(requests.lock().unwrap().is_empty());
    }
}
