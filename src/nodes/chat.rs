use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::sleep;

use crate::{
    dsl::{
        compiled::{
            CompiledNode, NextPolicy, NodeCompilation, NodeEnvelopeRules, NodeOutcome,
            NodeTransition,
        },
        compiler::{CompileContext, TemplateProgram},
        CompileError, EmitPolicy,
    },
    nodes::registry::{NodeExecutor, NodeType},
    resources::models::{
        ChatContent, ChatContentPart, ChatMessage, ChatModel, ChatRequest, ChatRole, ImageUrl,
        ModelCapability,
    },
    runtime::{ExecutionControl, RunContext, RunError},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChatConfig {
    model: String,
    messages: Vec<MessageConfig>,
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
    Parts(Vec<MessagePartConfig>),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum MessagePartConfig {
    Text { text: String },
    ImageUrl { image_url: ImageUrlConfig },
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
    ImageUrl(TemplateProgram),
}

#[derive(Debug)]
struct CompiledMessage {
    role: ChatRole,
    content: CompiledMessageContent,
}

struct CompiledChat {
    model: Arc<dyn ChatModel>,
    messages: Vec<CompiledMessage>,
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
        for (message_index, message) in config.messages.into_iter().enumerate() {
            let content = match message.content {
                MessageContentConfig::Text(source) => {
                    let template = context.compile_inline_template(
                        node_id,
                        &format!("messages[{message_index}].content"),
                        &source,
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
                        let (field, source, image) = match part {
                            MessagePartConfig::Text { text } => ("text", text, false),
                            MessagePartConfig::ImageUrl { image_url } => {
                                ("image_url.url", image_url.url, true)
                            }
                        };
                        let template = context.compile_inline_template(
                            node_id,
                            &format!("messages[{message_index}].parts[{part_index}].{field}"),
                            &source,
                        )?;
                        references.extend(template.references.iter().cloned());
                        if image {
                            has_images = true;
                            compiled_parts.push(CompiledMessagePart::ImageUrl(template));
                        } else {
                            compiled_parts.push(CompiledMessagePart::Text(template));
                        }
                    }
                    CompiledMessageContent::Parts(compiled_parts)
                }
            };
            messages.push(CompiledMessage {
                role: message.role,
                content,
            });
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
            terminal: false,
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Required,
                allows_content_emit: true,
            },
        })
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
        let messages = body
            .messages
            .iter()
            .map(|message| message.render(context, &data))
            .collect::<Result<Vec<_>, _>>()?;
        let request = ChatRequest {
            messages,
            parameters: body.parameters.clone(),
        };

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
            if !chunk.text.is_empty() {
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
            CompiledMessageContent::Parts(parts) => ChatContent::Parts(
                parts
                    .iter()
                    .map(|part| match part {
                        CompiledMessagePart::Text(template) => Ok(ChatContentPart::Text {
                            text: render_template(context, template, data)?,
                        }),
                        CompiledMessagePart::ImageUrl(template) => Ok(ChatContentPart::ImageUrl {
                            image_url: ImageUrl {
                                url: render_template(context, template, data)?,
                            },
                        }),
                    })
                    .collect::<Result<Vec<_>, RunError>>()?,
            ),
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
    context
        .templates()
        .render(&template.name, data)
        .map_err(|error| {
            RunError::new(
                "TEMPLATE_RENDER_FAILED",
                format!("failed to render template '{}': {error}", template.name),
            )
        })
}

fn stopped_error(control: &ExecutionControl) -> RunError {
    control
        .stop_reason()
        .map(RunError::stopped)
        .unwrap_or_else(|| RunError::new("RUN_STOPPED", "run stopped"))
}
