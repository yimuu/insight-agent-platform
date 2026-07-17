use std::{fmt, io::Write};

use serde::Serialize;
use serde_json::Value;

pub const LLM_CONTENT_INVALID: &str = "VNEXT_LLM_CONTENT_INVALID";
pub const LLM_DYNAMIC_MESSAGE_INVALID: &str = "VNEXT_LLM_DYNAMIC_MESSAGE_INVALID";
pub const LLM_DYNAMIC_ROLE_FORBIDDEN: &str = "VNEXT_LLM_DYNAMIC_ROLE_FORBIDDEN";
pub const LLM_MESSAGE_ORDER_INVALID: &str = "VNEXT_LLM_MESSAGE_ORDER_INVALID";
pub const LLM_REQUEST_TOO_LARGE: &str = "VNEXT_LLM_REQUEST_TOO_LARGE";

const CONTENT_INVALID_MESSAGE: &str = "LLM message content is invalid";
const DYNAMIC_MESSAGE_INVALID_MESSAGE: &str = "dynamic LLM message is invalid";
const DYNAMIC_ROLE_FORBIDDEN_MESSAGE: &str = "dynamic LLM message role is forbidden";
const MESSAGE_ORDER_INVALID_MESSAGE: &str = "LLM message order is invalid";
const REQUEST_TOO_LARGE_MESSAGE: &str = "LLM message request exceeds its configured limit";

/// A safe, body-free error returned while constructing or validating runtime
/// messages. Indices identify the invalid location without retaining message
/// text, image URLs, or other caller-provided values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMessageError {
    code: &'static str,
    message: &'static str,
    message_index: Option<usize>,
    part_index: Option<usize>,
}

impl RuntimeMessageError {
    fn content_invalid() -> Self {
        Self::new(LLM_CONTENT_INVALID, CONTENT_INVALID_MESSAGE)
    }

    fn dynamic_invalid(message_index: usize, part_index: Option<usize>) -> Self {
        Self {
            code: LLM_DYNAMIC_MESSAGE_INVALID,
            message: DYNAMIC_MESSAGE_INVALID_MESSAGE,
            message_index: Some(message_index),
            part_index,
        }
    }

    fn dynamic_role_forbidden(message_index: usize) -> Self {
        Self {
            code: LLM_DYNAMIC_ROLE_FORBIDDEN,
            message: DYNAMIC_ROLE_FORBIDDEN_MESSAGE,
            message_index: Some(message_index),
            part_index: None,
        }
    }

    fn message_order_invalid() -> Self {
        Self::new(LLM_MESSAGE_ORDER_INVALID, MESSAGE_ORDER_INVALID_MESSAGE)
    }

    fn request_too_large() -> Self {
        Self::new(LLM_REQUEST_TOO_LARGE, REQUEST_TOO_LARGE_MESSAGE)
    }

    fn new(code: &'static str, message: &'static str) -> Self {
        Self {
            code,
            message,
            message_index: None,
            part_index: None,
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &'static str {
        self.message
    }

    pub fn message_index(&self) -> Option<usize> {
        self.message_index
    }

    pub fn part_index(&self) -> Option<usize> {
        self.part_index
    }
}

impl fmt::Display for RuntimeMessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)?;
        if let Some(message_index) = self.message_index {
            write!(formatter, " at message index {message_index}")?;
        }
        if let Some(part_index) = self.part_index {
            write!(formatter, ", part index {part_index}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RuntimeMessageError {}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeRole {
    System,
    User,
    Assistant,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum RuntimeContent {
    Text(String),
    Parts(Vec<RuntimeContentPart>),
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum RuntimeContentPart {
    Text { text: String },
    Image { image: String },
}

/// A provider-neutral message. Fields are private so callers must construct a
/// role-correlated, non-empty value through the checked constructors below.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RuntimeMessage {
    role: RuntimeRole,
    content: RuntimeContent,
}

impl RuntimeMessage {
    pub fn new(role: RuntimeRole, content: RuntimeContent) -> Result<Self, RuntimeMessageError> {
        validate_content(role, &content)?;
        Ok(Self { role, content })
    }

    pub fn role(&self) -> RuntimeRole {
        self.role
    }

    pub fn content(&self) -> &RuntimeContent {
        &self.content
    }

    pub fn into_content(self) -> RuntimeContent {
        self.content
    }
}

/// The role-correlated, caller-controlled message type. It deliberately has no
/// system variant. Use `parse_dynamic_messages` at the untrusted JSON boundary;
/// manually constructed values are validated again during conversion.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum DynamicMessage {
    User { content: DynamicUserContent },
    Assistant { content: DynamicTextContent },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum DynamicUserContent {
    Text(String),
    Parts(Vec<DynamicUserContentPart>),
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum DynamicTextContent {
    Text(String),
    Parts(Vec<DynamicTextPart>),
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum DynamicUserContentPart {
    Text { text: String },
    Image { image: String },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DynamicTextPart {
    pub text: String,
}

impl DynamicMessage {
    pub fn role(&self) -> RuntimeRole {
        match self {
            Self::User { .. } => RuntimeRole::User,
            Self::Assistant { .. } => RuntimeRole::Assistant,
        }
    }

    fn into_runtime(self) -> Result<RuntimeMessage, RuntimeMessageError> {
        let (role, content) = match self {
            Self::User { content } => (RuntimeRole::User, user_content_into_runtime(content)?),
            Self::Assistant { content } => {
                (RuntimeRole::Assistant, text_content_into_runtime(content)?)
            }
        };
        RuntimeMessage::new(role, content)
    }
}

/// A rendered authored content atom. Text has already passed through the
/// restricted template renderer. A nullable image uses `None` to mean omission;
/// an empty or whitespace-only `Some` value is always invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenderedContentAtom {
    Text(String),
    Image(Option<String>),
}

/// Canonicalizes only authored content. Adjacent text atoms are joined by
/// exactly the inserted byte sequence `\n\n`; existing bytes are not trimmed or
/// normalized. A present image breaks the text run, while an omitted nullable
/// image contributes no atom.
pub fn canonicalize_authored_content(
    role: RuntimeRole,
    atoms: impl IntoIterator<Item = RenderedContentAtom>,
) -> Result<RuntimeContent, RuntimeMessageError> {
    let mut pending_text = Vec::new();
    let mut parts = Vec::new();
    let mut saw_image = false;

    for atom in atoms {
        match atom {
            RenderedContentAtom::Text(text) => {
                require_non_blank(&text).map_err(|_| RuntimeMessageError::content_invalid())?;
                pending_text.push(text);
            }
            RenderedContentAtom::Image(image) => {
                if role != RuntimeRole::User {
                    return Err(RuntimeMessageError::content_invalid());
                }
                let Some(image) = image else {
                    continue;
                };
                require_non_blank(&image).map_err(|_| RuntimeMessageError::content_invalid())?;
                flush_authored_text(&mut pending_text, &mut parts);
                parts.push(RuntimeContentPart::Image { image });
                saw_image = true;
            }
        }
    }
    flush_authored_text(&mut pending_text, &mut parts);

    if parts.is_empty() {
        return Err(RuntimeMessageError::content_invalid());
    }
    if !saw_image && parts.len() == 1 {
        let RuntimeContentPart::Text { text } =
            parts.pop().expect("one authored text part was checked")
        else {
            unreachable!("saw_image is false only for text content")
        };
        Ok(RuntimeContent::Text(text))
    } else {
        Ok(RuntimeContent::Parts(parts))
    }
}

pub fn build_authored_message(
    role: RuntimeRole,
    atoms: impl IntoIterator<Item = RenderedContentAtom>,
) -> Result<RuntimeMessage, RuntimeMessageError> {
    RuntimeMessage::new(role, canonicalize_authored_content(role, atoms)?)
}

/// Strictly parses a caller-provided `DynamicMessage[]`. Every message and part
/// is a closed object; user content may contain text or image parts, while
/// assistant content may contain text parts only. Empty arrays and blank values
/// fail without retaining their bodies in the error.
pub fn parse_dynamic_messages(value: &Value) -> Result<Vec<DynamicMessage>, RuntimeMessageError> {
    let messages = value
        .as_array()
        .ok_or_else(|| RuntimeMessageError::dynamic_invalid(0, None))?;
    messages
        .iter()
        .enumerate()
        .map(|(index, message)| parse_dynamic_message(message, index))
        .collect()
}

/// Applies all clone-relevant request budgets to the borrowed JSON value
/// before allocating owned dynamic messages.
pub fn parse_dynamic_messages_bounded(
    value: &Value,
    limits: RuntimeMessageLimits,
) -> Result<Vec<DynamicMessage>, RuntimeMessageError> {
    let messages = value
        .as_array()
        .ok_or_else(|| RuntimeMessageError::dynamic_invalid(0, None))?;
    if messages.len() > limits.max_messages {
        return Err(RuntimeMessageError::request_too_large());
    }
    serialized_len_bounded(messages, limits.max_total_bytes)?;
    for message in messages {
        serialized_len_bounded(message, limits.max_message_bytes)?;
        validate_dynamic_image_value_sizes(message, limits.max_image_bytes)?;
    }
    parse_dynamic_messages(value)
}

fn validate_dynamic_image_value_sizes(
    message: &Value,
    max_image_bytes: usize,
) -> Result<(), RuntimeMessageError> {
    let Some(parts) = message
        .as_object()
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
    else {
        return Ok(());
    };
    if parts.iter().any(|part| {
        part.as_object()
            .and_then(|part| part.get("image"))
            .and_then(Value::as_str)
            .is_some_and(|image| image.len() > max_image_bytes)
    }) {
        return Err(RuntimeMessageError::request_too_large());
    }
    Ok(())
}

/// Converts already parsed or manually constructed dynamic messages without
/// changing message order, content representation, part order, or string bytes.
pub fn dynamic_messages_into_runtime(
    messages: Vec<DynamicMessage>,
) -> Result<Vec<RuntimeMessage>, RuntimeMessageError> {
    messages
        .into_iter()
        .enumerate()
        .map(|(index, message)| {
            message.into_runtime().map_err(|mut error| {
                error.message_index = Some(index);
                error
            })
        })
        .collect()
}

fn parse_dynamic_message(
    value: &Value,
    message_index: usize,
) -> Result<DynamicMessage, RuntimeMessageError> {
    let object = value
        .as_object()
        .ok_or_else(|| RuntimeMessageError::dynamic_invalid(message_index, None))?;
    if object.len() != 2 || !object.contains_key("role") || !object.contains_key("content") {
        return Err(RuntimeMessageError::dynamic_invalid(message_index, None));
    }
    let role = object
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(|| RuntimeMessageError::dynamic_invalid(message_index, None))?;
    let content = object
        .get("content")
        .expect("closed dynamic message content was checked");

    match role {
        "user" => Ok(DynamicMessage::User {
            content: parse_dynamic_user_content(content, message_index)?,
        }),
        "assistant" => Ok(DynamicMessage::Assistant {
            content: parse_dynamic_text_content(content, message_index)?,
        }),
        _ => Err(RuntimeMessageError::dynamic_role_forbidden(message_index)),
    }
}

fn parse_dynamic_user_content(
    value: &Value,
    message_index: usize,
) -> Result<DynamicUserContent, RuntimeMessageError> {
    if let Some(text) = value.as_str() {
        require_dynamic_non_blank(text, message_index, None)?;
        return Ok(DynamicUserContent::Text(text.to_string()));
    }
    let parts = value
        .as_array()
        .filter(|parts| !parts.is_empty())
        .ok_or_else(|| RuntimeMessageError::dynamic_invalid(message_index, None))?;
    let parts = parts
        .iter()
        .enumerate()
        .map(|(part_index, part)| {
            let object = part
                .as_object()
                .filter(|object| object.len() == 1)
                .ok_or_else(|| {
                    RuntimeMessageError::dynamic_invalid(message_index, Some(part_index))
                })?;
            if let Some(text) = object.get("text").and_then(Value::as_str) {
                require_dynamic_non_blank(text, message_index, Some(part_index))?;
                Ok(DynamicUserContentPart::Text {
                    text: text.to_string(),
                })
            } else if let Some(image) = object.get("image").and_then(Value::as_str) {
                require_dynamic_non_blank(image, message_index, Some(part_index))?;
                Ok(DynamicUserContentPart::Image {
                    image: image.to_string(),
                })
            } else {
                Err(RuntimeMessageError::dynamic_invalid(
                    message_index,
                    Some(part_index),
                ))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DynamicUserContent::Parts(parts))
}

fn parse_dynamic_text_content(
    value: &Value,
    message_index: usize,
) -> Result<DynamicTextContent, RuntimeMessageError> {
    if let Some(text) = value.as_str() {
        require_dynamic_non_blank(text, message_index, None)?;
        return Ok(DynamicTextContent::Text(text.to_string()));
    }
    let parts = value
        .as_array()
        .filter(|parts| !parts.is_empty())
        .ok_or_else(|| RuntimeMessageError::dynamic_invalid(message_index, None))?;
    let parts = parts
        .iter()
        .enumerate()
        .map(|(part_index, part)| {
            let object = part
                .as_object()
                .filter(|object| object.len() == 1)
                .ok_or_else(|| {
                    RuntimeMessageError::dynamic_invalid(message_index, Some(part_index))
                })?;
            let text = object.get("text").and_then(Value::as_str).ok_or_else(|| {
                RuntimeMessageError::dynamic_invalid(message_index, Some(part_index))
            })?;
            require_dynamic_non_blank(text, message_index, Some(part_index))?;
            Ok(DynamicTextPart {
                text: text.to_string(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DynamicTextContent::Parts(parts))
}

fn require_dynamic_non_blank(
    value: &str,
    message_index: usize,
    part_index: Option<usize>,
) -> Result<(), RuntimeMessageError> {
    require_non_blank(value)
        .map_err(|_| RuntimeMessageError::dynamic_invalid(message_index, part_index))
}

fn require_non_blank(value: &str) -> Result<(), ()> {
    if value.trim().is_empty() {
        Err(())
    } else {
        Ok(())
    }
}

fn user_content_into_runtime(
    content: DynamicUserContent,
) -> Result<RuntimeContent, RuntimeMessageError> {
    match content {
        DynamicUserContent::Text(text) => {
            require_non_blank(&text).map_err(|_| RuntimeMessageError::content_invalid())?;
            Ok(RuntimeContent::Text(text))
        }
        DynamicUserContent::Parts(parts) if !parts.is_empty() => Ok(RuntimeContent::Parts(
            parts
                .into_iter()
                .map(|part| match part {
                    DynamicUserContentPart::Text { text } => {
                        require_non_blank(&text)
                            .map_err(|_| RuntimeMessageError::content_invalid())?;
                        Ok(RuntimeContentPart::Text { text })
                    }
                    DynamicUserContentPart::Image { image } => {
                        require_non_blank(&image)
                            .map_err(|_| RuntimeMessageError::content_invalid())?;
                        Ok(RuntimeContentPart::Image { image })
                    }
                })
                .collect::<Result<Vec<_>, RuntimeMessageError>>()?,
        )),
        DynamicUserContent::Parts(_) => Err(RuntimeMessageError::content_invalid()),
    }
}

fn text_content_into_runtime(
    content: DynamicTextContent,
) -> Result<RuntimeContent, RuntimeMessageError> {
    match content {
        DynamicTextContent::Text(text) => {
            require_non_blank(&text).map_err(|_| RuntimeMessageError::content_invalid())?;
            Ok(RuntimeContent::Text(text))
        }
        DynamicTextContent::Parts(parts) if !parts.is_empty() => Ok(RuntimeContent::Parts(
            parts
                .into_iter()
                .map(|part| {
                    require_non_blank(&part.text)
                        .map_err(|_| RuntimeMessageError::content_invalid())?;
                    Ok(RuntimeContentPart::Text { text: part.text })
                })
                .collect::<Result<Vec<_>, RuntimeMessageError>>()?,
        )),
        DynamicTextContent::Parts(_) => Err(RuntimeMessageError::content_invalid()),
    }
}

fn flush_authored_text(pending_text: &mut Vec<String>, parts: &mut Vec<RuntimeContentPart>) {
    if !pending_text.is_empty() {
        parts.push(RuntimeContentPart::Text {
            text: std::mem::take(pending_text).join("\n\n"),
        });
    }
}

fn validate_content(
    role: RuntimeRole,
    content: &RuntimeContent,
) -> Result<(), RuntimeMessageError> {
    match content {
        RuntimeContent::Text(text) => {
            require_non_blank(text).map_err(|_| RuntimeMessageError::content_invalid())
        }
        RuntimeContent::Parts(parts) if parts.is_empty() => {
            Err(RuntimeMessageError::content_invalid())
        }
        RuntimeContent::Parts(parts) => {
            for part in parts {
                match part {
                    RuntimeContentPart::Text { text } => require_non_blank(text)
                        .map_err(|_| RuntimeMessageError::content_invalid())?,
                    RuntimeContentPart::Image { image } => {
                        if role != RuntimeRole::User {
                            return Err(RuntimeMessageError::content_invalid());
                        }
                        require_non_blank(image)
                            .map_err(|_| RuntimeMessageError::content_invalid())?;
                    }
                }
            }
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeMessageLimits {
    max_messages: usize,
    max_message_bytes: usize,
    max_image_bytes: usize,
    max_total_bytes: usize,
}

impl RuntimeMessageLimits {
    pub const fn new(
        max_messages: usize,
        max_message_bytes: usize,
        max_image_bytes: usize,
        max_total_bytes: usize,
    ) -> Self {
        Self {
            max_messages,
            max_message_bytes,
            max_image_bytes,
            max_total_bytes,
        }
    }

    pub const fn max_messages(self) -> usize {
        self.max_messages
    }

    pub const fn max_message_bytes(self) -> usize {
        self.max_message_bytes
    }

    pub const fn max_image_bytes(self) -> usize {
        self.max_image_bytes
    }

    pub const fn max_total_bytes(self) -> usize {
        self.max_total_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeMessageUsage {
    pub message_count: usize,
    /// Exact canonical JSON byte length of the provider-neutral message array.
    pub total_bytes: usize,
    /// Largest canonical JSON byte length of any individual message.
    pub largest_message_bytes: usize,
}

/// Revalidates the resource budget of a partially or fully assembled request.
/// Unlike [`validate_runtime_messages`], this deliberately does not require a
/// final user message, so callers can apply the same exact limits after every
/// source expansion instead of first materializing an oversized intermediate
/// list.
pub fn validate_runtime_message_budget(
    messages: &[RuntimeMessage],
    limits: RuntimeMessageLimits,
) -> Result<RuntimeMessageUsage, RuntimeMessageError> {
    if messages.len() > limits.max_messages {
        return Err(RuntimeMessageError::request_too_large());
    }

    let mut largest_message_bytes = 0;
    for message in messages {
        validate_content(message.role, &message.content)?;
        validate_image_sizes(&message.content, limits.max_image_bytes)?;
        let message_bytes = serialized_len_bounded(message, limits.max_message_bytes)?;
        largest_message_bytes = largest_message_bytes.max(message_bytes);
    }

    let total_bytes = serialized_len_bounded(messages, limits.max_total_bytes)?;
    Ok(RuntimeMessageUsage {
        message_count: messages.len(),
        total_bytes,
        largest_message_bytes,
    })
}

/// Revalidates the fully assembled request immediately before provider
/// lowering. System messages must form a prefix, at least one user message must
/// exist, and the final message must be user. Byte budgets use the exact JSON
/// representation of these provider-neutral messages, including structural
/// overhead but excluding provider/model-specific request fields.
pub fn validate_runtime_messages(
    messages: &[RuntimeMessage],
    limits: RuntimeMessageLimits,
) -> Result<RuntimeMessageUsage, RuntimeMessageError> {
    let usage = validate_runtime_message_budget(messages, limits)?;
    let mut saw_non_system = false;
    let mut saw_user = false;
    for message in messages {
        match message.role {
            RuntimeRole::System if saw_non_system => {
                return Err(RuntimeMessageError::message_order_invalid())
            }
            RuntimeRole::System => {}
            RuntimeRole::User => {
                saw_non_system = true;
                saw_user = true;
            }
            RuntimeRole::Assistant => saw_non_system = true,
        }
    }
    if !saw_user
        || !matches!(
            messages.last().map(RuntimeMessage::role),
            Some(RuntimeRole::User)
        )
    {
        return Err(RuntimeMessageError::message_order_invalid());
    }
    Ok(usage)
}

fn validate_image_sizes(
    content: &RuntimeContent,
    max_image_bytes: usize,
) -> Result<(), RuntimeMessageError> {
    let RuntimeContent::Parts(parts) = content else {
        return Ok(());
    };
    if parts.iter().any(|part| {
        matches!(
            part,
            RuntimeContentPart::Image { image } if image.len() > max_image_bytes
        )
    }) {
        return Err(RuntimeMessageError::request_too_large());
    }
    Ok(())
}

fn serialized_len_bounded<T: Serialize + ?Sized>(
    value: &T,
    limit: usize,
) -> Result<usize, RuntimeMessageError> {
    let mut writer = CountingWriter::new(limit);
    if serde_json::to_writer(&mut writer, value).is_err() {
        return Err(RuntimeMessageError::request_too_large());
    }
    Ok(writer.written)
}

struct CountingWriter {
    written: usize,
    limit: usize,
}

impl CountingWriter {
    fn new(limit: usize) -> Self {
        Self { written: 0, limit }
    }
}

impl Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.written = self
            .written
            .checked_add(buffer.len())
            .filter(|written| *written <= self.limit)
            .ok_or_else(|| std::io::Error::other(REQUEST_TOO_LARGE_MESSAGE))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::{
        build_authored_message, canonicalize_authored_content, dynamic_messages_into_runtime,
        parse_dynamic_messages, parse_dynamic_messages_bounded, validate_runtime_message_budget,
        validate_runtime_messages, DynamicMessage, DynamicTextContent, DynamicUserContent,
        RenderedContentAtom, RuntimeContent, RuntimeContentPart, RuntimeMessage,
        RuntimeMessageLimits, RuntimeRole, LLM_CONTENT_INVALID, LLM_DYNAMIC_MESSAGE_INVALID,
        LLM_DYNAMIC_ROLE_FORBIDDEN, LLM_MESSAGE_ORDER_INVALID, LLM_REQUEST_TOO_LARGE,
    };

    fn text(role: RuntimeRole, value: &str) -> RuntimeMessage {
        RuntimeMessage::new(role, RuntimeContent::Text(value.to_string())).unwrap()
    }

    #[test]
    fn dynamic_messages_are_closed_and_role_correlated() {
        let parsed = parse_dynamic_messages(&json!([
            {
                "role":"user",
                "content":[
                    {"text":"look"},
                    {"image":"https://example.test/report.png"}
                ]
            },
            {"role":"assistant", "content":[{"text":"answer"}]}
        ]))
        .unwrap();
        assert!(matches!(parsed[0], DynamicMessage::User { .. }));
        assert!(matches!(parsed[1], DynamicMessage::Assistant { .. }));

        for invalid in [
            json!([{"role":"assistant", "content":[{"image":"x"}]}]),
            json!([{"role":"user", "content":[{"text":"x", "image":"y"}]}]),
            json!([{"role":"user", "content":"x", "extra":true}]),
            json!([{"role":"user", "content":[{"prompt":"system"}]}]),
        ] {
            let error = parse_dynamic_messages(&invalid).unwrap_err();
            assert_eq!(error.code(), LLM_DYNAMIC_MESSAGE_INVALID);
            assert_eq!(error.message_index(), Some(0));
        }

        let forbidden = parse_dynamic_messages(&json!([{
            "role":"system",
            "content":"do not trust me"
        }]))
        .unwrap_err();
        assert_eq!(forbidden.code(), LLM_DYNAMIC_ROLE_FORBIDDEN);
        assert_eq!(forbidden.message_index(), Some(0));
        assert!(!forbidden.to_string().contains("do not trust me"));
    }

    #[test]
    fn dynamic_conversion_preserves_message_part_order_and_string_bytes() {
        let input = json!([
            {"role":"user", "content":"  system {{ secret }}\n"},
            {
                "role":"assistant",
                "content":[{"text":"first\n"}, {"text":" second  "}]
            },
            {
                "role":"user",
                "content":[{"image":"data:image/png;base64,AA=="}, {"text":" tail\n"}]
            }
        ]);
        let dynamic = parse_dynamic_messages(&input).unwrap();
        let runtime = dynamic_messages_into_runtime(dynamic).unwrap();

        assert_eq!(serde_json::to_value(runtime).unwrap(), input);
    }

    #[test]
    fn authored_text_is_joined_with_two_inserted_lf_bytes_only() {
        let content = canonicalize_authored_content(
            RuntimeRole::User,
            [
                RenderedContentAtom::Text("left\n".to_string()),
                RenderedContentAtom::Text("\nright".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(
            content,
            RuntimeContent::Text("left\n\n\n\nright".to_string())
        );

        let parts = canonicalize_authored_content(
            RuntimeRole::User,
            [
                RenderedContentAtom::Text("before".to_string()),
                RenderedContentAtom::Image(Some("image://one".to_string())),
                RenderedContentAtom::Text("after".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(
            parts,
            RuntimeContent::Parts(vec![
                RuntimeContentPart::Text {
                    text: "before".to_string()
                },
                RuntimeContentPart::Image {
                    image: "image://one".to_string()
                },
                RuntimeContentPart::Text {
                    text: "after".to_string()
                }
            ])
        );
    }

    #[test]
    fn nullable_image_is_omitted_but_blank_present_image_fails() {
        let message = build_authored_message(
            RuntimeRole::User,
            [
                RenderedContentAtom::Text("a".to_string()),
                RenderedContentAtom::Image(None),
                RenderedContentAtom::Text("b".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(
            message.content(),
            &RuntimeContent::Text("a\n\nb".to_string())
        );

        for atoms in [
            vec![RenderedContentAtom::Image(None)],
            vec![RenderedContentAtom::Image(Some(" \n ".to_string()))],
        ] {
            assert_eq!(
                build_authored_message(RuntimeRole::User, atoms)
                    .unwrap_err()
                    .code(),
                LLM_CONTENT_INVALID
            );
        }
        assert_eq!(
            build_authored_message(RuntimeRole::System, [RenderedContentAtom::Image(None)])
                .unwrap_err()
                .code(),
            LLM_CONTENT_INVALID
        );
    }

    #[test]
    fn blank_dynamic_and_authored_text_fail_without_rewriting_nonblank_values() {
        for input in [
            json!([{"role":"user", "content":" \n "}]),
            json!([{"role":"user", "content":[]}]),
            json!([{"role":"user", "content":[{"text":"\t"}]}]),
            json!([{"role":"user", "content":[{"image":" "}]}]),
        ] {
            assert_eq!(
                parse_dynamic_messages(&input).unwrap_err().code(),
                LLM_DYNAMIC_MESSAGE_INVALID
            );
        }
        assert_eq!(
            canonicalize_authored_content(
                RuntimeRole::User,
                [RenderedContentAtom::Text(" \t ".to_string())]
            )
            .unwrap_err()
            .code(),
            LLM_CONTENT_INVALID
        );
    }

    #[test]
    fn manually_constructed_dynamic_values_are_revalidated() {
        for message in [
            DynamicMessage::User {
                content: DynamicUserContent::Parts(Vec::new()),
            },
            DynamicMessage::Assistant {
                content: DynamicTextContent::Text(" ".to_string()),
            },
        ] {
            assert_eq!(
                dynamic_messages_into_runtime(vec![message])
                    .unwrap_err()
                    .code(),
                LLM_CONTENT_INVALID
            );
        }
    }

    #[test]
    fn runtime_validation_enforces_system_prefix_and_final_user() {
        let valid = vec![
            text(RuntimeRole::System, "s1"),
            text(RuntimeRole::System, "s2"),
            text(RuntimeRole::User, "q1"),
            text(RuntimeRole::Assistant, "a1"),
            text(RuntimeRole::User, "q2"),
        ];
        validate_runtime_messages(&valid, RuntimeMessageLimits::new(5, 1_024, 1_024, 4_096))
            .unwrap();

        for invalid in [
            vec![
                text(RuntimeRole::User, "q"),
                text(RuntimeRole::System, "late"),
                text(RuntimeRole::User, "q2"),
            ],
            vec![
                text(RuntimeRole::User, "q"),
                text(RuntimeRole::Assistant, "a"),
            ],
            vec![text(RuntimeRole::System, "system only")],
            Vec::new(),
        ] {
            assert_eq!(
                validate_runtime_messages(
                    &invalid,
                    RuntimeMessageLimits::new(10, 1_024, 1_024, 4_096),
                )
                .unwrap_err()
                .code(),
                LLM_MESSAGE_ORDER_INVALID
            );
        }
    }

    #[test]
    fn partial_budget_validation_does_not_require_a_final_user() {
        let partial = vec![
            text(RuntimeRole::System, "system"),
            text(RuntimeRole::Assistant, "history"),
        ];
        let exact = serde_json::to_vec(&partial).unwrap().len();
        let usage = validate_runtime_message_budget(
            &partial,
            RuntimeMessageLimits::new(2, exact, exact, exact),
        )
        .unwrap();
        assert_eq!(usage.total_bytes, exact);
        assert_eq!(
            validate_runtime_messages(&partial, RuntimeMessageLimits::new(2, exact, exact, exact),)
                .unwrap_err()
                .code(),
            LLM_MESSAGE_ORDER_INVALID
        );
    }

    #[test]
    fn runtime_validation_enforces_exact_message_and_total_json_byte_limits() {
        let messages = vec![
            text(RuntimeRole::System, "system"),
            text(RuntimeRole::User, "question"),
        ];
        let per_message = messages
            .iter()
            .map(|message| serde_json::to_vec(message).unwrap().len())
            .collect::<Vec<_>>();
        let largest = *per_message.iter().max().unwrap();
        let total = serde_json::to_vec(&messages).unwrap().len();

        let usage = validate_runtime_messages(
            &messages,
            RuntimeMessageLimits::new(messages.len(), largest, total, total),
        )
        .unwrap();
        assert_eq!(usage.message_count, 2);
        assert_eq!(usage.largest_message_bytes, largest);
        assert_eq!(usage.total_bytes, total);

        for limits in [
            RuntimeMessageLimits::new(1, largest, total, total),
            RuntimeMessageLimits::new(2, largest - 1, total, total),
            RuntimeMessageLimits::new(2, largest, total, total - 1),
        ] {
            assert_eq!(
                validate_runtime_messages(&messages, limits)
                    .unwrap_err()
                    .code(),
                LLM_REQUEST_TOO_LARGE
            );
        }
    }

    #[test]
    fn runtime_constructor_rejects_image_for_non_user_roles() {
        for role in [RuntimeRole::System, RuntimeRole::Assistant] {
            let error = RuntimeMessage::new(
                role,
                RuntimeContent::Parts(vec![RuntimeContentPart::Image {
                    image: "image://forbidden".to_string(),
                }]),
            )
            .unwrap_err();
            assert_eq!(error.code(), LLM_CONTENT_INVALID);
        }
    }

    #[test]
    fn runtime_validation_enforces_the_image_byte_limit() {
        let messages = vec![
            text(RuntimeRole::System, "system"),
            RuntimeMessage::new(
                RuntimeRole::User,
                RuntimeContent::Parts(vec![RuntimeContentPart::Image {
                    image: "四".to_string(),
                }]),
            )
            .unwrap(),
        ];
        let total = serde_json::to_vec(&messages).unwrap().len();
        validate_runtime_messages(&messages, RuntimeMessageLimits::new(2, total, 3, total))
            .unwrap();
        assert_eq!(
            validate_runtime_messages(&messages, RuntimeMessageLimits::new(2, total, 2, total),)
                .unwrap_err()
                .code(),
            LLM_REQUEST_TOO_LARGE
        );
    }

    #[test]
    fn dynamic_root_must_be_an_array_but_an_empty_source_is_valid() {
        assert_eq!(
            parse_dynamic_messages(&Value::Array(Vec::new())).unwrap(),
            []
        );
        assert_eq!(
            parse_dynamic_messages(&json!({"role":"user", "content":"q"}))
                .unwrap_err()
                .code(),
            LLM_DYNAMIC_MESSAGE_INVALID
        );
    }

    #[test]
    fn dynamic_budgets_are_checked_before_owned_message_construction() {
        let input = json!([{
            "role":"user",
            "content":[{"image":"https://example.test/oversized.png"}]
        }]);
        let exact_message = serde_json::to_vec(&input[0]).unwrap().len();
        let exact_total = serde_json::to_vec(&input).unwrap().len();

        parse_dynamic_messages_bounded(
            &input,
            RuntimeMessageLimits::new(1, exact_message, 64, exact_total),
        )
        .unwrap();
        for limits in [
            RuntimeMessageLimits::new(0, exact_message, 64, exact_total),
            RuntimeMessageLimits::new(1, exact_message - 1, 64, exact_total),
            RuntimeMessageLimits::new(1, exact_message, 8, exact_total),
            RuntimeMessageLimits::new(1, exact_message, 64, exact_total - 1),
        ] {
            assert_eq!(
                parse_dynamic_messages_bounded(&input, limits)
                    .unwrap_err()
                    .code(),
                LLM_REQUEST_TOO_LARGE
            );
        }
    }
}
