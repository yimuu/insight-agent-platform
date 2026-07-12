use std::collections::BTreeSet;

use serde::Deserialize;
use serde_json::Value;

use crate::{
    dsl::{references::is_dsl_identifier, CompileError},
    resources::models::{ChatContent, ChatContentPart, ChatMessage, ChatRole, ImageUrl},
    runtime::{RunContext, RunError},
};

pub(super) const DEFAULT_MAX_MESSAGES: usize = 50;
pub(super) const DEFAULT_MAX_BYTES: usize = 262_144;

fn default_max_messages() -> usize {
    DEFAULT_MAX_MESSAGES
}

fn default_max_bytes() -> usize {
    DEFAULT_MAX_BYTES
}

fn default_allowed_content() -> Vec<String> {
    vec!["text".to_string()]
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DynamicMessageEntryConfig {
    pub(super) from: DynamicMessagesConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DynamicMessagesConfig {
    pub(super) path: String,
    #[serde(default)]
    pub(super) optional: bool,
    #[serde(default = "default_max_messages")]
    pub(super) max_messages: usize,
    #[serde(default = "default_max_bytes")]
    pub(super) max_bytes: usize,
    #[serde(default = "default_allowed_content")]
    pub(super) allowed_content: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DynamicContentKind {
    Text,
    ImageUrl,
}

#[derive(Debug)]
pub(super) struct CompiledDynamicMessages {
    path: DynamicSourcePath,
    optional: bool,
    max_messages: usize,
    max_bytes: usize,
    allowed_content: BTreeSet<DynamicContentKind>,
}

impl CompiledDynamicMessages {
    pub(super) fn compile(
        config: DynamicMessagesConfig,
        node_id: &str,
        entry_index: usize,
    ) -> Result<Self, CompileError> {
        if config.max_messages == 0 || config.max_bytes == 0 {
            return Err(config_invalid(
                node_id,
                entry_index,
                "limits must be positive",
            ));
        }
        if config.allowed_content.is_empty() {
            return Err(config_invalid(
                node_id,
                entry_index,
                "allowed_content must not be empty",
            ));
        }
        let mut allowed_content = BTreeSet::new();
        for kind in config.allowed_content {
            let kind = match kind.as_str() {
                "text" => DynamicContentKind::Text,
                "image_url" => DynamicContentKind::ImageUrl,
                _ => {
                    return Err(config_invalid(
                        node_id,
                        entry_index,
                        "allowed_content contains an unsupported kind",
                    ));
                }
            };
            allowed_content.insert(kind);
        }
        Ok(Self {
            path: DynamicSourcePath::parse(config.path, node_id, entry_index)?,
            optional: config.optional,
            max_messages: config.max_messages,
            max_bytes: config.max_bytes,
            allowed_content,
        })
    }

    pub(super) fn reference(&self) -> Option<&str> {
        match &self.path {
            DynamicSourcePath::Input { .. } => None,
            DynamicSourcePath::NodeOutput { node_id, .. } => Some(node_id),
        }
    }

    pub(super) fn requires_vision(&self) -> bool {
        self.allowed_content.contains(&DynamicContentKind::ImageUrl)
    }

    pub(super) fn expand(&self, context: &RunContext) -> Result<Vec<ChatMessage>, RunError> {
        let Some(source) = self.path.resolve(context) else {
            return if self.optional {
                Ok(Vec::new())
            } else {
                Err(self.source_missing())
            };
        };
        let array = source.as_array().ok_or_else(|| self.invalid_source())?;
        let bytes = serde_json::to_vec(source)
            .map_err(|_| self.invalid_source())?
            .len();
        if bytes > self.max_bytes {
            return Err(self.too_large());
        }
        if array.len() > self.max_messages {
            return Err(self.limit_exceeded());
        }

        array
            .iter()
            .enumerate()
            .map(|(message_index, value)| {
                let message: DynamicMessage = serde_json::from_value(value.clone())
                    .map_err(|_| self.invalid_message(message_index, "has invalid shape"))?;
                self.convert(message, message_index)
            })
            .collect()
    }

    fn convert(
        &self,
        message: DynamicMessage,
        message_index: usize,
    ) -> Result<ChatMessage, RunError> {
        if message.role == ChatRole::System {
            return Err(self.invalid_message(message_index, "uses the system role"));
        }
        let content = match message.content {
            DynamicContent::Text(text) => {
                self.require_kind(DynamicContentKind::Text, message_index, None)?;
                ChatContent::Text(text)
            }
            DynamicContent::Parts(parts) => {
                if parts.is_empty() {
                    return Err(self.invalid_message(message_index, "has no content parts"));
                }
                let mut converted = Vec::with_capacity(parts.len());
                for (part_index, part) in parts.into_iter().enumerate() {
                    match part {
                        DynamicPart::Text { text } => {
                            self.require_kind(
                                DynamicContentKind::Text,
                                message_index,
                                Some(part_index),
                            )?;
                            converted.push(ChatContentPart::Text { text });
                        }
                        DynamicPart::ImageUrl { image_url } => {
                            self.require_kind(
                                DynamicContentKind::ImageUrl,
                                message_index,
                                Some(part_index),
                            )?;
                            if message.role != ChatRole::User {
                                return Err(self.invalid_part(
                                    message_index,
                                    part_index,
                                    "image_url is allowed only for user messages",
                                ));
                            }
                            if image_url.url.trim().is_empty() {
                                return Err(self.invalid_part(
                                    message_index,
                                    part_index,
                                    "image_url must not be blank",
                                ));
                            }
                            converted.push(ChatContentPart::ImageUrl {
                                image_url: ImageUrl { url: image_url.url },
                            });
                        }
                    }
                }
                ChatContent::Parts(converted)
            }
        };
        Ok(ChatMessage {
            role: message.role,
            content,
        })
    }

    fn require_kind(
        &self,
        kind: DynamicContentKind,
        message_index: usize,
        part_index: Option<usize>,
    ) -> Result<(), RunError> {
        if self.allowed_content.contains(&kind) {
            return Ok(());
        }
        match part_index {
            Some(part_index) => {
                Err(self.invalid_part(message_index, part_index, "content kind is not allowed"))
            }
            None => Err(self.invalid_message(message_index, "content kind is not allowed")),
        }
    }

    fn source_missing(&self) -> RunError {
        RunError::new(
            "CHAT_DYNAMIC_MESSAGES_SOURCE_MISSING",
            format!(
                "dynamic message source '{}' is missing",
                self.path.canonical()
            ),
        )
    }

    fn invalid_source(&self) -> RunError {
        RunError::new(
            "CHAT_DYNAMIC_MESSAGES_INVALID",
            format!(
                "dynamic message source '{}' must be an array",
                self.path.canonical()
            ),
        )
    }

    fn limit_exceeded(&self) -> RunError {
        RunError::new(
            "CHAT_DYNAMIC_MESSAGES_LIMIT_EXCEEDED",
            format!(
                "dynamic message source '{}' exceeds max_messages {}",
                self.path.canonical(),
                self.max_messages
            ),
        )
    }

    fn too_large(&self) -> RunError {
        RunError::new(
            "CHAT_DYNAMIC_MESSAGES_TOO_LARGE",
            format!(
                "dynamic message source '{}' exceeds max_bytes {}",
                self.path.canonical(),
                self.max_bytes
            ),
        )
    }

    fn invalid_message(&self, message_index: usize, rule: &str) -> RunError {
        RunError::new(
            "CHAT_DYNAMIC_MESSAGES_INVALID",
            format!(
                "dynamic message source '{}' message {} {rule}",
                self.path.canonical(),
                message_index
            ),
        )
    }

    fn invalid_part(&self, message_index: usize, part_index: usize, rule: &str) -> RunError {
        RunError::new(
            "CHAT_DYNAMIC_MESSAGES_INVALID",
            format!(
                "dynamic message source '{}' message {} part {} {rule}",
                self.path.canonical(),
                message_index,
                part_index
            ),
        )
    }
}

#[derive(Debug)]
enum DynamicSourcePath {
    Input {
        canonical: String,
        fields: Vec<String>,
    },
    NodeOutput {
        canonical: String,
        node_id: String,
        fields: Vec<String>,
    },
}

impl DynamicSourcePath {
    fn parse(value: String, node_id: &str, entry_index: usize) -> Result<Self, CompileError> {
        let segments = value.split('.').collect::<Vec<_>>();
        if !segments.iter().all(|segment| is_dsl_identifier(segment)) {
            return Err(path_invalid(node_id, entry_index));
        }
        match segments.as_slice() {
            ["input", fields @ ..] if !fields.is_empty() => Ok(Self::Input {
                canonical: value.clone(),
                fields: fields.iter().map(|field| (*field).to_string()).collect(),
            }),
            ["nodes", source_node, "output", fields @ ..] => Ok(Self::NodeOutput {
                canonical: value.clone(),
                node_id: (*source_node).to_string(),
                fields: fields.iter().map(|field| (*field).to_string()).collect(),
            }),
            _ => Err(path_invalid(node_id, entry_index)),
        }
    }

    fn canonical(&self) -> &str {
        match self {
            Self::Input { canonical, .. } | Self::NodeOutput { canonical, .. } => canonical,
        }
    }

    fn resolve<'a>(&self, context: &'a RunContext) -> Option<&'a Value> {
        let (mut current, fields) = match self {
            Self::Input { fields, .. } => (context.input(), fields),
            Self::NodeOutput {
                node_id, fields, ..
            } => (context.node_output(node_id)?, fields),
        };
        for field in fields {
            current = current.as_object()?.get(field)?;
        }
        Some(current)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DynamicMessage {
    role: ChatRole,
    content: DynamicContent,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum DynamicContent {
    Text(String),
    Parts(Vec<DynamicPart>),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum DynamicPart {
    Text { text: String },
    ImageUrl { image_url: DynamicImageUrl },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DynamicImageUrl {
    url: String,
}

fn config_invalid(node_id: &str, entry_index: usize, rule: &str) -> CompileError {
    CompileError::new(
        "CHAT_DYNAMIC_MESSAGES_CONFIG_INVALID",
        format!("chat node '{node_id}' dynamic message entry {entry_index} {rule}"),
    )
}

fn path_invalid(node_id: &str, entry_index: usize) -> CompileError {
    CompileError::new(
        "CHAT_DYNAMIC_MESSAGES_PATH_INVALID",
        format!("chat node '{node_id}' dynamic message entry {entry_index} has an invalid path"),
    )
}
