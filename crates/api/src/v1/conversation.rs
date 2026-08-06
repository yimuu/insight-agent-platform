//! Conversation HTTP wire helpers.

use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

use insight_runtime::{AgentInvocation, FileRef};

use super::RunDto;

#[cfg(test)]
pub(crate) const DEFAULT_MESSAGE_PAGE_SIZE: u32 = 50;
#[cfg(test)]
pub(crate) const MAX_MESSAGE_PAGE_SIZE: u32 = 200;
const MAX_CURSOR_BYTES: usize = 1024;
const BASE64_URL_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConversationPrincipal {
    pub tenant_id: String,
    pub user_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateConversationRequest {
    pub agent_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppendConversationMessageRequest {
    pub query: String,
    /// Recognized only so the Conversation surface can return its dedicated
    /// protocol error instead of collapsing managed history into a generic
    /// JSON-shape rejection. Client history is never copied into the Run.
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub messages: Option<Value>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub files: Option<Vec<FileRef>>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub inputs: Option<Map<String, Value>>,
}

fn deserialize_non_null_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)?
        .ok_or_else(|| D::Error::custom("null is not allowed"))
        .map(Some)
}

impl AppendConversationMessageRequest {
    pub(crate) const fn has_client_history(&self) -> bool {
        self.messages.is_some()
    }

    pub(crate) fn into_invocation(self) -> AgentInvocation {
        AgentInvocation {
            query: Some(self.query),
            messages: None,
            files: self.files,
            inputs: self.inputs,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveConversationRequest {}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationMessagesQuery {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}

impl ConversationMessagesQuery {
    pub(crate) fn validated_limit_with(
        &self,
        default: u32,
        max: u32,
    ) -> Result<u32, ConversationRequestError> {
        if default == 0 || max == 0 || default > max {
            return Err(ConversationRequestError::InvalidPageLimit);
        }
        let limit = self.limit.unwrap_or(default);
        (1..=max)
            .contains(&limit)
            .then_some(limit)
            .ok_or(ConversationRequestError::InvalidPageLimit)
    }

    pub(crate) fn decoded_cursor(
        &self,
    ) -> Result<Option<ConversationMessageCursor>, ConversationRequestError> {
        self.cursor
            .as_deref()
            .map(ConversationMessageCursor::decode)
            .transpose()
    }
}

/// Decoded form of the opaque HTTP cursor. Storage adapters use these exact
/// two values for a stable `(message_order, message_id)` seek.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationMessageCursor {
    pub message_order: i64,
    pub message_id: String,
}

impl ConversationMessageCursor {
    pub fn encode(&self) -> String {
        let payload =
            serde_json::to_vec(self).expect("conversation cursor wire serialization is infallible");
        base64_url_encode(&payload)
    }

    pub(crate) fn decode(encoded: &str) -> Result<Self, ConversationRequestError> {
        if encoded.is_empty() || encoded.len() > MAX_CURSOR_BYTES {
            return Err(ConversationRequestError::InvalidCursor);
        }
        let payload = base64_url_decode(encoded).ok_or(ConversationRequestError::InvalidCursor)?;
        let cursor: Self = serde_json::from_slice(&payload)
            .map_err(|_| ConversationRequestError::InvalidCursor)?;
        if cursor.message_order < 1
            || cursor.message_id.is_empty()
            || cursor.message_id.len() > 256
            || cursor
                .message_id
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
        {
            return Err(ConversationRequestError::InvalidCursor);
        }
        Ok(cursor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConversationRequestError {
    InvalidCursor,
    InvalidPageLimit,
}

/// Cursor page returned by the HTTP surface. The runtime/storage cursor never
/// leaks as a structured client-controlled object.
#[derive(Debug, Clone, Serialize)]
pub struct ConversationMessagePageDto<T> {
    pub messages: Vec<T>,
    pub next_cursor: Option<String>,
}

impl<T> ConversationMessagePageDto<T> {
    pub fn new(messages: Vec<T>, next_cursor: Option<ConversationMessageCursor>) -> Self {
        Self {
            messages,
            next_cursor: next_cursor.map(|cursor| cursor.encode()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ConversationTurnDto<T> {
    pub user_message: T,
    pub run: RunDto,
    pub replayed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ConversationDeleteDto {
    pub deleted: bool,
}

fn base64_url_encode(input: &[u8]) -> String {
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or_default();
        let third = chunk.get(2).copied().unwrap_or_default();
        output.push(BASE64_URL_ALPHABET[usize::from(first >> 2)] as char);
        output
            .push(BASE64_URL_ALPHABET[usize::from(((first & 0b11) << 4) | (second >> 4))] as char);
        if chunk.len() > 1 {
            output.push(
                BASE64_URL_ALPHABET[usize::from(((second & 0b1111) << 2) | (third >> 6))] as char,
            );
        }
        if chunk.len() > 2 {
            output.push(BASE64_URL_ALPHABET[usize::from(third & 0b11_1111)] as char);
        }
    }
    output
}

fn base64_url_decode(input: &str) -> Option<Vec<u8>> {
    if input.len() % 4 == 1 || !input.is_ascii() {
        return None;
    }
    let mut accumulator = 0_u32;
    let mut bits = 0_u8;
    let mut output = Vec::with_capacity(input.len() * 3 / 4);
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        };
        accumulator = (accumulator << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push((accumulator >> bits) as u8);
            accumulator &= (1_u32 << bits).saturating_sub(1);
        }
    }
    if accumulator != 0 || base64_url_encode(&output) != input {
        return None;
    }
    Some(output)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        AppendConversationMessageRequest, ArchiveConversationRequest, ConversationMessageCursor,
        ConversationMessagePageDto, ConversationMessagesQuery, ConversationRequestError,
        CreateConversationRequest, DEFAULT_MESSAGE_PAGE_SIZE, MAX_MESSAGE_PAGE_SIZE,
    };

    #[test]
    fn cursor_round_trip_is_opaque_canonical_base64url() {
        let cursor = ConversationMessageCursor {
            message_order: 9_223_372,
            message_id: "msg_01JZ-example".to_owned(),
        };
        let encoded = cursor.encode();
        assert!(!encoded.contains('='));
        assert!(!encoded.contains('{'));
        assert_eq!(ConversationMessageCursor::decode(&encoded), Ok(cursor));
        assert_eq!(
            ConversationMessageCursor::decode(&format!("{encoded}=")),
            Err(ConversationRequestError::InvalidCursor)
        );
        assert_eq!(
            ConversationMessageCursor::decode("eyJtZXNzYWdlX29yZGVyIjowLCJtZXNzYWdlX2lkIjoibSJ9"),
            Err(ConversationRequestError::InvalidCursor)
        );
    }

    #[test]
    fn page_limit_defaults_and_stays_bounded() {
        assert_eq!(
            ConversationMessagesQuery::default()
                .validated_limit_with(DEFAULT_MESSAGE_PAGE_SIZE, MAX_MESSAGE_PAGE_SIZE)
                .unwrap(),
            DEFAULT_MESSAGE_PAGE_SIZE
        );
        assert_eq!(
            ConversationMessagesQuery {
                cursor: None,
                limit: Some(MAX_MESSAGE_PAGE_SIZE),
            }
            .validated_limit_with(DEFAULT_MESSAGE_PAGE_SIZE, MAX_MESSAGE_PAGE_SIZE)
            .unwrap(),
            MAX_MESSAGE_PAGE_SIZE
        );
        assert_eq!(
            ConversationMessagesQuery {
                cursor: None,
                limit: Some(0),
            }
            .validated_limit_with(DEFAULT_MESSAGE_PAGE_SIZE, MAX_MESSAGE_PAGE_SIZE),
            Err(ConversationRequestError::InvalidPageLimit)
        );
    }

    #[test]
    fn conversation_mutation_bodies_strictly_reject_unknown_fields() {
        assert!(serde_json::from_value::<CreateConversationRequest>(
            json!({"agent_id": "agent-a"})
        )
        .is_ok());
        assert!(serde_json::from_value::<CreateConversationRequest>(
            json!({"agent_id": "agent-a", "tenant_id": "forged"})
        )
        .is_err());
        assert!(serde_json::from_value::<AppendConversationMessageRequest>(
            json!({"query": "hello", "files": [{"file_id":"file-a"}], "inputs": {"style":"short"}})
        )
        .is_ok());
        for alias in ["content", "message", "input", "payload"] {
            assert!(
                serde_json::from_value::<AppendConversationMessageRequest>(json!({
                    "query":"hello",
                    (alias):"must-not-be-accepted"
                }))
                .is_err()
            );
        }
        let managed = serde_json::from_value::<AppendConversationMessageRequest>(json!({
            "query":"hello",
            "messages":[]
        }))
        .unwrap();
        assert!(managed.has_client_history());
        for field in ["messages", "files", "inputs"] {
            assert!(
                serde_json::from_value::<AppendConversationMessageRequest>(json!({
                    "query":"hello",
                    (field):null
                }))
                .is_err()
            );
        }
        assert!(serde_json::from_value::<ArchiveConversationRequest>(json!({})).is_ok());
        assert!(serde_json::from_value::<ArchiveConversationRequest>(
            json!({"delete_messages": true})
        )
        .is_err());
    }

    #[test]
    fn page_exposes_only_an_opaque_next_cursor() {
        let page = ConversationMessagePageDto::new(
            vec![json!({"message_id": "msg-2"})],
            Some(ConversationMessageCursor {
                message_order: 2,
                message_id: "msg-2".to_owned(),
            }),
        );
        let value = serde_json::to_value(page).unwrap();
        assert_eq!(value["messages"][0]["message_id"], "msg-2");
        assert!(value["next_cursor"].is_string());
        assert!(!value.to_string().contains("message_order"));
    }
}
