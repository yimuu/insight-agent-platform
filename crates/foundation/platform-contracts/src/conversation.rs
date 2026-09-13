//! Workspace conversations reference Run authority; they never own execution state or body copies.
use crate::{ClosedJsonSchema, ExactDeploymentRef, ResourceId, Sha256Digest, UtcTimestamp};
use serde::{Deserialize, Serialize};
pub const MAX_CONVERSATIONS_PER_TENANT: i64 = 1000;
pub const MAX_CONVERSATION_TURNS: u32 = 128;
pub const MAX_CONVERSATION_MESSAGE_BYTES: usize = 16_384;
pub const MAX_CONVERSATION_HISTORY_BYTES: usize = 262_144;
pub const MAX_CONVERSATION_TITLE_BYTES: usize = 160;
pub const MAX_CONVERSATION_PAGE_SIZE: u16 = 50;
pub const MAX_CONVERSATION_FIELD_BYTES: usize = 128;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationViewV1 {
    pub schema_version: u32,
    pub conversation_id: ResourceId,
    pub agent_id: ResourceId,
    pub agent_deployment: ExactDeploymentRef,
    pub input_field: String,
    pub input_schema_digest: Sha256Digest,
    pub title: String,
    pub created_by: ResourceId,
    pub version: u64,
    pub turn_count: u32,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationTurnViewV1 {
    pub schema_version: u32,
    pub conversation_id: ResourceId,
    pub ordinal: u32,
    pub created_at: UtcTimestamp,
    pub run_id: ResourceId,
    pub history_through: u32,
    pub conversation_version: u64,
}
/// Only direct closed schemas are accepted. No inferred field or client-supplied mapping.
pub fn conversation_input_field(
    input: &ClosedJsonSchema,
    output: &ClosedJsonSchema,
) -> Option<String> {
    input.validate().ok()?;
    output.validate().ok()?;
    let i = &input.schema;
    let o = &output.schema;
    let properties = i.get("properties")?.as_object()?;
    if i.get("type")?.as_str()? != "object"
        || properties.len() != 1
        || i.get("additionalProperties")?.as_bool()?
        || o.get("type")?.as_str()? != "object"
        || o.get("additionalProperties")?.as_bool()?
    {
        return None;
    }
    let (key, value) = properties.iter().next()?;
    if key.is_empty()
        || key.len() > MAX_CONVERSATION_FIELD_BYTES
        || key.chars().any(char::is_control)
    {
        return None;
    }
    if value.get("type")?.as_str()? != "string"
        || i.get("required")?.as_array()? != &vec![serde_json::json!(key)]
        || o.get("properties")?.get("answer")?.get("type")?.as_str()? != "string"
        || !o
            .get("required")?
            .as_array()?
            .contains(&serde_json::json!("answer"))
    {
        return None;
    }
    Some(key.clone())
}
pub fn validate_conversation_message(message: &str) -> bool {
    !message.trim().is_empty()
        && message.len() <= MAX_CONVERSATION_MESSAGE_BYTES
        && !message.contains('\0')
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_ambiguous_or_optional_input_and_non_text_answer() {
        let schema = |mut v: serde_json::Value| {
            v["$schema"] = serde_json::json!("https://json-schema.org/draft/2020-12/schema");
            for property in v["properties"].as_object_mut().unwrap().values_mut() {
                property["minLength"] = serde_json::json!(1);
                property["maxLength"] = serde_json::json!(256);
                property["x-platform-max-bytes"] = serde_json::json!(1024);
                property["x-platform-classification"] = serde_json::json!("internal");
            }
            ClosedJsonSchema::build(v).unwrap()
        };
        let input = schema(
            serde_json::json!({"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false}),
        );
        let output = schema(
            serde_json::json!({"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}),
        );
        assert_eq!(
            conversation_input_field(&input, &output).as_deref(),
            Some("message")
        );
        let mut wrong = input.clone();
        wrong.schema["required"] = serde_json::json!([]);
        assert_eq!(conversation_input_field(&wrong, &output), None);
        assert!(!validate_conversation_message("  "));
        assert!(!validate_conversation_message(
            &"x".repeat(MAX_CONVERSATION_MESSAGE_BYTES + 1)
        ));
    }
}
