use std::{collections::BTreeSet, sync::Arc, time::Instant};

use async_trait::async_trait;
use futures::StreamExt;
use handlebars::{RenderError, RenderErrorReason};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::sleep;

mod dynamic;

use dynamic::{CompiledDynamicMessages, DynamicMessageEntryConfig};

use crate::{
    dsl::{
        compiled::{
            CompiledNode, NextPolicy, NodeCompilation, NodeControl, NodeEnvelopeRules, NodeOutcome,
            NodeTransition,
        },
        compiler::{CompileContext, TemplateProgram},
        CompileError, EmitPolicy,
    },
    nodes::registry::{NodeExecutor, NodeType},
    observability::{elapsed_ms, json_size_bytes},
    resources::models::{
        model_response_too_large, ChatContent, ChatContentPart, ChatMessage, ChatModel,
        ChatRequest, ChatRole, ImageUrl, ModelCapability,
    },
    runtime::{ExecutionControl, RunContext, RunError},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChatConfig {
    model: String,
    messages: Vec<Value>,
    #[serde(default = "empty_object")]
    parameters: Value,
}

fn empty_object() -> Value {
    json!({})
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MessageConfig {
    role: ChatRole,
    content: MessageContentConfig,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MessageContentConfig {
    Text(String),
    TemplateRef(TemplateRefConfig),
    Parts(Vec<MessagePartConfig>),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TemplateRefConfig {
    template_ref: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TextSourceConfig {
    Text(String),
    TemplateRef(TemplateRefConfig),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum MessagePartConfig {
    Text {
        text: TextSourceConfig,
    },
    ImageUrl {
        image_url: ImageUrlConfig,
        #[serde(default)]
        optional: bool,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImageUrlConfig {
    url: String,
}

#[derive(Debug)]
enum CompiledMessageContent {
    Text(TemplateProgram),
    Parts(Vec<CompiledMessagePart>),
}

#[derive(Debug)]
enum CompiledMessagePart {
    Text(TemplateProgram),
    ImageUrl {
        template: TemplateProgram,
        optional: bool,
    },
}

#[derive(Debug)]
struct CompiledMessage {
    role: ChatRole,
    content: CompiledMessageContent,
}

enum CompiledMessageEntry {
    Static(CompiledMessage),
    Dynamic(CompiledDynamicMessages),
}

struct CompiledChat {
    model: Arc<dyn ChatModel>,
    messages: Vec<CompiledMessageEntry>,
    parameters: Value,
}

#[derive(Debug, Clone, Copy)]
pub struct ChatNode;

impl NodeType for ChatNode {
    fn kind(&self) -> &'static str {
        "core.chat"
    }

    fn compile(
        &self,
        node_id: &str,
        config: Value,
        context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        let config: ChatConfig = serde_json::from_value(config).map_err(|error| {
            CompileError::new(
                "NODE_CONFIG_INVALID",
                format!("invalid core.chat config for node '{node_id}': {error}"),
            )
        })?;
        if config.messages.is_empty() {
            return Err(CompileError::new(
                "CHAT_MESSAGES_REQUIRED",
                format!("chat node '{node_id}' must define at least one message"),
            ));
        }
        if !config.parameters.is_object() {
            return Err(CompileError::new(
                "CHAT_PARAMETERS_INVALID",
                format!("chat node '{node_id}' parameters must be an object"),
            ));
        }
        let model = context.models().resolve(&config.model)?;
        model.validate_parameters(&config.parameters)?;

        let mut references = BTreeSet::new();
        let mut has_images = false;
        let mut messages = Vec::with_capacity(config.messages.len());
        for (entry_index, entry) in config.messages.into_iter().enumerate() {
            if entry
                .as_object()
                .is_some_and(|object| object.contains_key("from"))
            {
                let config: DynamicMessageEntryConfig =
                    serde_json::from_value(entry).map_err(|_| {
                        CompileError::new(
                            "CHAT_DYNAMIC_MESSAGES_CONFIG_INVALID",
                            format!(
                                "chat node '{node_id}' dynamic message entry {entry_index} has invalid configuration"
                            ),
                        )
                    })?;
                let dynamic = CompiledDynamicMessages::compile(config.from, node_id, entry_index)?;
                if let Some(reference) = dynamic.reference() {
                    references.insert(reference.to_string());
                }
                has_images |= dynamic.requires_vision();
                messages.push(CompiledMessageEntry::Dynamic(dynamic));
            } else {
                let message: MessageConfig = serde_json::from_value(entry).map_err(|error| {
                    CompileError::new(
                        "NODE_CONFIG_INVALID",
                        format!(
                            "invalid core.chat message {entry_index} for node '{node_id}': {error}"
                        ),
                    )
                })?;
                let (message, message_references, message_has_images) =
                    compile_static_message(message, context, node_id, entry_index)?;
                references.extend(message_references);
                has_images |= message_has_images;
                messages.push(CompiledMessageEntry::Static(message));
            }
        }

        if has_images && !model.capabilities().contains(&ModelCapability::Vision) {
            return Err(CompileError::new(
                "MODEL_CAPABILITY_REQUIRED",
                format!(
                    "chat node '{node_id}' uses image content but model '{}' lacks vision capability",
                    config.model
                ),
            ));
        }

        Ok(NodeCompilation {
            body: Arc::new(CompiledChat {
                model,
                messages,
                parameters: config.parameters,
            }),
            edges: Vec::new(),
            references,
            control: NodeControl::Ordinary,
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Required,
                allows_content_emit: true,
            },
        })
    }
}

fn compile_static_message(
    message: MessageConfig,
    context: &mut CompileContext<'_>,
    node_id: &str,
    message_index: usize,
) -> Result<(CompiledMessage, BTreeSet<String>, bool), CompileError> {
    let mut references = BTreeSet::new();
    let mut has_images = false;
    let content = match message.content {
        MessageContentConfig::Text(source) => {
            let template = compile_text_source(
                TextSourceConfig::Text(source),
                context,
                node_id,
                &format!("messages[{message_index}].content"),
            )?;
            references.extend(template.references.iter().cloned());
            CompiledMessageContent::Text(template)
        }
        MessageContentConfig::TemplateRef(source) => {
            let template = compile_text_source(
                TextSourceConfig::TemplateRef(source),
                context,
                node_id,
                &format!("messages[{message_index}].content"),
            )?;
            references.extend(template.references.iter().cloned());
            CompiledMessageContent::Text(template)
        }
        MessageContentConfig::Parts(parts) => {
            if parts.is_empty() {
                return Err(CompileError::new(
                    "CHAT_CONTENT_PARTS_REQUIRED",
                    format!(
                        "chat node '{node_id}' message {message_index} must contain at least one part"
                    ),
                ));
            }
            let mut compiled_parts = Vec::with_capacity(parts.len());
            for (part_index, part) in parts.into_iter().enumerate() {
                match part {
                    MessagePartConfig::Text { text } => {
                        let template = compile_text_source(
                            text,
                            context,
                            node_id,
                            &format!("messages[{message_index}].parts[{part_index}].text"),
                        )?;
                        references.extend(template.references.iter().cloned());
                        compiled_parts.push(CompiledMessagePart::Text(template));
                    }
                    MessagePartConfig::ImageUrl {
                        image_url,
                        optional,
                    } => {
                        let template = compile_text_source(
                            TextSourceConfig::Text(image_url.url),
                            context,
                            node_id,
                            &format!("messages[{message_index}].parts[{part_index}].image_url.url"),
                        )?;
                        references.extend(template.references.iter().cloned());
                        has_images = true;
                        compiled_parts.push(CompiledMessagePart::ImageUrl { template, optional });
                    }
                }
            }
            CompiledMessageContent::Parts(compiled_parts)
        }
    };
    Ok((
        CompiledMessage {
            role: message.role,
            content,
        },
        references,
        has_images,
    ))
}

fn compile_text_source(
    source: TextSourceConfig,
    context: &mut CompileContext<'_>,
    node_id: &str,
    field: &str,
) -> Result<TemplateProgram, CompileError> {
    match source {
        TextSourceConfig::Text(source) => context.compile_inline_template(node_id, field, &source),
        TextSourceConfig::TemplateRef(source) => {
            context.compile_prompt_ref(node_id, field, &source.template_ref)
        }
    }
}

#[async_trait]
impl NodeExecutor for ChatNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        if let Some(reason) = control.stop_reason() {
            return Err(RunError::stopped(reason));
        }
        let body = node.body::<CompiledChat>()?;
        let data = context.template_data();
        let mut messages = Vec::new();
        for entry in &body.messages {
            match entry {
                CompiledMessageEntry::Static(message) => {
                    messages.push(message.render(context, &data)?);
                }
                CompiledMessageEntry::Dynamic(dynamic) => {
                    messages.extend(dynamic.expand(context)?);
                }
            }
        }
        if messages.is_empty() {
            return Err(RunError::new(
                "CHAT_MESSAGES_EMPTY",
                "chat messages are empty after dynamic sources were expanded",
            ));
        }
        let request = ChatRequest {
            messages,
            parameters: body.parameters.clone(),
        };

        let messages_count = request.messages.len();
        let image_parts_count = request
            .messages
            .iter()
            .map(|message| message.image_urls().len())
            .sum::<usize>();
        let parameters_keys_count = request
            .parameters
            .as_object()
            .map_or(0, |parameters| parameters.len());
        tracing::info!(
            event_name = "chat.request",
            run_id = context.metadata().run_id.as_str(),
            request_id = context.metadata().request_id.as_str(),
            agent_id = context.metadata().agent_id.as_str(),
            agent_version = context.metadata().agent_version.as_str(),
            node_id = node.id.as_str(),
            messages_count,
            image_parts_count,
            parameters_keys_count,
            "chat request metadata"
        );
        let chat_started = Instant::now();

        let stream_future = body.model.stream_chat(request);
        tokio::pin!(stream_future);
        let mut stream = tokio::select! {
            result = &mut stream_future => result?,
            _ = control.stopped() => return Err(stopped_error(control)),
            _ = sleep(control.remaining()) => return Err(RunError::timeout()),
        };

        let mut text = String::new();
        let mut finish_reason = None;
        let mut usage = None;
        let mut chunks_count = 0_usize;
        let mut text_bytes = 0_usize;
        let max_accumulated_text_bytes = body.model.max_accumulated_text_bytes();
        loop {
            let chunk = tokio::select! {
                chunk = stream.next() => chunk,
                _ = control.stopped() => return Err(stopped_error(control)),
                _ = sleep(control.remaining()) => return Err(RunError::timeout()),
            };
            let Some(chunk) = chunk else {
                break;
            };
            let chunk = chunk?;
            chunks_count += 1;
            text_bytes = text_bytes.saturating_add(chunk.text.len());
            if !chunk.text.is_empty() {
                if text.len().saturating_add(chunk.text.len()) > max_accumulated_text_bytes {
                    return Err(model_response_too_large());
                }
                text.push_str(&chunk.text);
                if node.emit == EmitPolicy::Content {
                    control.emit_content(chunk.text).await?;
                }
            }
            if chunk.finish_reason.is_some() {
                finish_reason = chunk.finish_reason;
            }
            if chunk.usage.is_some() {
                usage = chunk.usage;
            }
        }

        let usage_bytes = usage.as_ref().map_or(0, json_size_bytes);
        tracing::info!(
            event_name = "chat.response",
            run_id = context.metadata().run_id.as_str(),
            request_id = context.metadata().request_id.as_str(),
            agent_id = context.metadata().agent_id.as_str(),
            agent_version = context.metadata().agent_version.as_str(),
            node_id = node.id.as_str(),
            chunks_count,
            text_bytes,
            usage_bytes,
            finish_reason_present = finish_reason.is_some(),
            elapsed_ms = elapsed_ms(chat_started),
            "chat response metadata"
        );

        Ok(NodeOutcome {
            output: json!({
                "text": text,
                "finish_reason": finish_reason,
                "usage": usage,
            }),
            transition: NodeTransition::Next,
        })
    }
}

impl CompiledMessage {
    fn render(&self, context: &RunContext, data: &Value) -> Result<ChatMessage, RunError> {
        let content = match &self.content {
            CompiledMessageContent::Text(template) => {
                ChatContent::Text(render_template(context, template, data)?)
            }
            CompiledMessageContent::Parts(parts) => {
                let mut rendered = Vec::with_capacity(parts.len());
                for part in parts {
                    match part {
                        CompiledMessagePart::Text(template) => {
                            rendered.push(ChatContentPart::Text {
                                text: render_template(context, template, data)?,
                            });
                        }
                        CompiledMessagePart::ImageUrl { template, optional } => {
                            match render_raw_template(context, template, data) {
                                Ok(url) if *optional && url.trim().is_empty() => {}
                                Ok(url) => rendered.push(ChatContentPart::ImageUrl {
                                    image_url: ImageUrl { url },
                                }),
                                Err(error)
                                    if *optional
                                        && matches!(
                                            error.reason(),
                                            RenderErrorReason::MissingVariable(_)
                                        ) => {}
                                Err(error) => return Err(template_render_error(template, error)),
                            }
                        }
                    }
                }
                if rendered.is_empty() {
                    return Err(RunError::new(
                        "CHAT_CONTENT_PARTS_EMPTY",
                        "chat message has no content parts after optional parts were omitted",
                    ));
                }
                ChatContent::Parts(rendered)
            }
        };
        Ok(ChatMessage {
            role: self.role,
            content,
        })
    }
}

fn render_template(
    context: &RunContext,
    template: &TemplateProgram,
    data: &Value,
) -> Result<String, RunError> {
    render_raw_template(context, template, data)
        .map_err(|error| template_render_error(template, error))
}

fn render_raw_template(
    context: &RunContext,
    template: &TemplateProgram,
    data: &Value,
) -> Result<String, RenderError> {
    context.templates().render(&template.name, data)
}

fn template_render_error(template: &TemplateProgram, error: RenderError) -> RunError {
    RunError::new(
        "TEMPLATE_RENDER_FAILED",
        format!("failed to render template '{}': {error}", template.name),
    )
}

fn stopped_error(control: &ExecutionControl) -> RunError {
    control
        .stop_reason()
        .map(RunError::stopped)
        .unwrap_or_else(|| RunError::new("RUN_STOPPED", "run stopped"))
}
