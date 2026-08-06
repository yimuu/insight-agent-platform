//! Public Agent invocation wire and deterministic normalization.

use std::collections::{BTreeSet, HashSet};

use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::{json, Map, Value};

use insight_engine::plan::{PlanInputContract, PlanType};

use crate::catalog::{AgentInputError, PublishedAgent};

const RESERVED_INPUTS: [&str; 3] = ["query", "messages", "files"];
const MAX_CLIENT_HISTORY_MESSAGES: usize = 256;
const MAX_CLIENT_HISTORY_BYTES: usize = 2 * 1_024 * 1_024;
const MAX_CLIENT_HISTORY_ESTIMATED_TOKENS: usize = 256 * 1_024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileRef {
    pub file_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationFile {
    pub file_id: String,
    pub filename: String,
    pub media_type: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum MessageContentPart {
    Text { text: String },
    File { file: FileRef },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    pub role: MessageRole,
    pub content: Vec<MessageContentPart>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentInvocation {
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub query: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub messages: Option<Vec<Message>>,
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

impl AgentInvocation {
    pub fn file_refs(&self) -> &[FileRef] {
        self.files.as_deref().unwrap_or_default()
    }

    pub(crate) fn referenced_file_ids(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut ids = Vec::new();
        for file in self.file_refs() {
            if seen.insert(file.file_id.clone()) {
                ids.push(file.file_id.clone());
            }
        }
        for message in self.messages.as_deref().unwrap_or_default() {
            for part in &message.content {
                if let MessageContentPart::File { file } = part {
                    if seen.insert(file.file_id.clone()) {
                        ids.push(file.file_id.clone());
                    }
                }
            }
        }
        ids
    }

    pub(crate) fn canonical_user_content(&self) -> Result<Value, AgentInputError> {
        let query = self.query.as_ref().ok_or_else(|| {
            AgentInputError::new(
                "CONVERSATION_INPUT_INVALID",
                "conversation invocation requires query",
            )
        })?;
        let mut content = vec![json!({"text": query})];
        content.extend(
            self.file_refs()
                .iter()
                .map(|file| json!({"file":{"file_id":file.file_id}})),
        );
        Ok(Value::Array(content))
    }

    pub(crate) fn validate_base(&self) -> Result<(), AgentInputError> {
        if self
            .query
            .as_ref()
            .is_some_and(|query| query.len() > 1_048_576)
        {
            return Err(AgentInputError::new(
                "INVOCATION_INVALID",
                "query exceeds the platform request bound",
            ));
        }
        if let Some(messages) = &self.messages {
            validate_messages(messages)?;
        }
        let mut file_ids = HashSet::new();
        for file in self.file_refs() {
            if file.file_id.is_empty()
                || file.file_id.len() > 256
                || file
                    .file_id
                    .chars()
                    .any(|character| character.is_control() || character.is_whitespace())
                || !file_ids.insert(file.file_id.as_str())
            {
                return Err(AgentInputError::new(
                    "INVOCATION_INVALID",
                    "file references must contain unique valid file IDs",
                ));
            }
        }
        if self.inputs.as_ref().is_some_and(|inputs| {
            RESERVED_INPUTS
                .iter()
                .any(|reserved| inputs.contains_key(*reserved))
        }) {
            return Err(AgentInputError::new(
                "INVOCATION_INVALID",
                "reserved invocation fields cannot appear inside inputs",
            ));
        }
        Ok(())
    }
}

fn validate_messages(messages: &[Message]) -> Result<(), AgentInputError> {
    if messages.len() > MAX_CLIENT_HISTORY_MESSAGES {
        return Err(AgentInputError::new(
            "INVOCATION_INVALID",
            "client history exceeds the platform message-count bound",
        ));
    }
    let mut character_count = 0_usize;
    for message in messages {
        if message.content.is_empty() {
            return Err(AgentInputError::new(
                "INVOCATION_INVALID",
                "history messages require at least one content part",
            ));
        }
        for part in &message.content {
            match part {
                MessageContentPart::Text { text } => {
                    character_count = character_count.saturating_add(text.chars().count());
                }
                MessageContentPart::File { file } => validate_file_id(&file.file_id)?,
            }
        }
    }
    let encoded = serde_jcs::to_vec(messages).map_err(|_| {
        AgentInputError::new("INVOCATION_INVALID", "client history is not canonical JSON")
    })?;
    if encoded.len() > MAX_CLIENT_HISTORY_BYTES
        || character_count.div_ceil(4) > MAX_CLIENT_HISTORY_ESTIMATED_TOKENS
    {
        return Err(AgentInputError::new(
            "INVOCATION_INVALID",
            "client history exceeds the platform byte or token budget",
        ));
    }
    Ok(())
}

fn validate_file_id(file_id: &str) -> Result<(), AgentInputError> {
    if file_id.is_empty()
        || file_id.len() > 256
        || file_id
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(AgentInputError::new(
            "INVOCATION_INVALID",
            "file references must contain valid file IDs",
        ));
    }
    Ok(())
}

pub(crate) fn referenced_file_ids_in_messages(messages: &[Value]) -> Vec<String> {
    let mut ids = Vec::new();
    for file_id in messages
        .iter()
        .filter_map(Value::as_object)
        .filter_map(|message| message.get("content"))
        .filter_map(Value::as_array)
        .flatten()
        .filter_map(Value::as_object)
        .filter_map(|part| part.get("file"))
        .filter_map(Value::as_object)
        .filter_map(|file| file.get("file_id"))
        .filter_map(Value::as_str)
    {
        if !ids.iter().any(|existing| existing == file_id) {
            ids.push(file_id.to_owned());
        }
    }
    ids
}

impl PublishedAgent {
    /// Converts one public invocation into the exact normalized AgentInput.
    /// File metadata must already have been resolved and authorized by the
    /// File repository in the same order as the public FileRef list.
    pub fn normalize_invocation(
        &self,
        invocation: AgentInvocation,
        managed_messages: Option<Vec<Value>>,
        resolved_files: Vec<InvocationFile>,
    ) -> Result<insight_engine::RuntimeValue, AgentInputError> {
        invocation.validate_base()?;
        let declared = declared_inputs(self.plan().metadata().input_contract())?;

        if invocation.query.is_some() && !declared.contains("query") {
            return Err(AgentInputError::new(
                "AGENT_INPUT_UNKNOWN_FIELD",
                "agent does not declare query input",
            ));
        }
        if invocation.messages.is_some() && !declared.contains("messages") {
            return Err(AgentInputError::new(
                "CLIENT_HISTORY_UNSUPPORTED",
                "agent does not accept client-supplied history",
            ));
        }
        if invocation.files.is_some() && !declared.contains("files") {
            return Err(AgentInputError::new(
                "AGENT_INPUT_UNKNOWN_FIELD",
                "agent does not declare files input",
            ));
        }
        if managed_messages.is_some() && invocation.messages.is_some() {
            return Err(AgentInputError::new(
                "CONVERSATION_HISTORY_MANAGED",
                "conversation history is managed by the platform",
            ));
        }

        let requested_files = invocation.file_refs();
        if requested_files.len() != resolved_files.len()
            || requested_files
                .iter()
                .zip(&resolved_files)
                .any(|(requested, resolved)| requested.file_id != resolved.file_id)
        {
            return Err(AgentInputError::new(
                "FILE_NOT_READY",
                "resolved files do not match the invocation",
            ));
        }

        let managed_messages = managed_messages
            .map(|messages| {
                messages
                    .into_iter()
                    .map(|message| {
                        serde_json::from_value::<Message>(message).map_err(|_| {
                            AgentInputError::new(
                                "CONVERSATION_INPUT_INVALID",
                                "managed conversation history is invalid",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .and_then(|messages| {
                        validate_messages(&messages)?;
                        Ok(messages)
                    })
            })
            .transpose()?;
        let AgentInvocation {
            query,
            messages,
            files,
            inputs,
        } = invocation;
        let mut candidate = inputs.unwrap_or_default();
        if let Some(query) = query {
            candidate.insert("query".to_owned(), Value::String(query));
        }
        if let Some(messages) = managed_messages.or(messages) {
            candidate.insert(
                "messages".to_owned(),
                serde_json::to_value(messages).expect("Message is JSON"),
            );
        }
        if files.is_some() {
            candidate.insert(
                "files".to_owned(),
                Value::Array(
                    resolved_files
                        .into_iter()
                        .map(|file| serde_json::to_value(file).expect("InvocationFile is JSON"))
                        .collect(),
                ),
            );
        }
        self.normalize_input(Value::Object(candidate))
    }

    pub fn public_invocation_schema(&self) -> Value {
        invocation_schema(self.public_input_schema(), false)
    }

    pub fn public_conversation_message_schema(&self) -> Option<Value> {
        self.supports_conversations()
            .then(|| invocation_schema(self.public_input_schema(), true))
    }

    pub fn supports_conversations(&self) -> bool {
        let contract = self.plan().metadata().input_contract();
        let PlanType::Object { properties, .. } = contract.accepted_type() else {
            return false;
        };
        properties
            .get("query")
            .is_some_and(|query| query.required && query.value_type.string_constraints().is_some())
            && properties
                .get("messages")
                .is_some_and(|messages| messages.value_type.array_constraints().is_some())
    }

    pub fn accepts_client_history(&self) -> bool {
        declared_inputs(self.plan().metadata().input_contract())
            .is_ok_and(|inputs| inputs.contains("messages"))
    }

    pub fn accepts_files(&self) -> bool {
        declared_inputs(self.plan().metadata().input_contract())
            .is_ok_and(|inputs| inputs.contains("files"))
    }
}

fn declared_inputs(contract: &PlanInputContract) -> Result<BTreeSet<&str>, AgentInputError> {
    let PlanType::Object { properties, .. } = contract.accepted_type() else {
        return Err(AgentInputError::new(
            "AGENT_INPUT_INVALID",
            "agent input contract must be an object",
        ));
    };
    Ok(properties.keys().map(String::as_str).collect())
}

fn invocation_schema(mut input_schema: Value, conversation: bool) -> Value {
    let Value::Object(input) = &mut input_schema else {
        return json!({"type":"object","additionalProperties":false,"properties":{}});
    };
    let mut properties = input
        .remove("properties")
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let required = input
        .remove("required")
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default();
    let required: BTreeSet<String> = required
        .into_iter()
        .filter_map(|value| value.as_str().map(str::to_owned))
        .collect();
    let mut root_properties = Map::new();
    let mut root_required = Vec::new();
    for name in RESERVED_INPUTS {
        let Some(mut schema) = properties.remove(name) else {
            continue;
        };
        if conversation && name == "messages" {
            continue;
        }
        if name == "files" {
            if let Some(object) = schema.as_object_mut() {
                object.insert("items".to_owned(), json!({"$ref":"#/$defs/FileRef"}));
            }
        } else if name == "messages" {
            if let Some(object) = schema.as_object_mut() {
                object.insert("items".to_owned(), json!({"$ref":"#/$defs/Message"}));
            }
        }
        if required.contains(name)
            && !schema
                .as_object()
                .is_some_and(|schema| schema.contains_key("default"))
        {
            root_required.push(Value::String(name.to_owned()));
        }
        root_properties.insert(name.to_owned(), schema);
    }

    let business_required = required
        .iter()
        .filter(|name| !RESERVED_INPUTS.contains(&name.as_str()))
        .filter(|name| {
            !properties
                .get(*name)
                .and_then(Value::as_object)
                .is_some_and(|schema| schema.contains_key("default"))
        })
        .cloned()
        .map(Value::String)
        .collect::<Vec<_>>();
    let mut business = json!({
        "type":"object",
        "additionalProperties":false,
        "properties":properties
    });
    if !business_required.is_empty() {
        business
            .as_object_mut()
            .expect("business schema is an object")
            .insert("required".to_owned(), Value::Array(business_required));
        root_required.push(Value::String("inputs".to_owned()));
    }
    root_properties.insert(
        "inputs".to_owned(),
        json!({"$ref":"#/$defs/BusinessInputs"}),
    );

    let mut root = json!({
        "type":"object",
        "additionalProperties":false,
        "properties":root_properties,
        "$defs": {
            "BusinessInputs": business,
            "FileRef": {
                "type":"object",
                "additionalProperties":false,
                "required":["file_id"],
                "properties":{"file_id":{
                    "type":"string",
                    "minLength":1,
                    "maxLength":256,
                    "pattern":"^[^\\s\\u0000-\\u001f\\u007f-\\u009f]+$"
                }}
            },
            "Message": {
                "type":"object",
                "additionalProperties":false,
                "required":["role","content"],
                "properties": {
                    "role":{"type":"string","enum":["user","assistant"]},
                    "content": {
                        "type":"array",
                        "minItems":1,
                        "items": {
                            "oneOf":[
                                {
                                    "type":"object",
                                    "additionalProperties":false,
                                    "required":["text"],
                                    "properties":{"text":{"type":"string"}}
                                },
                                {
                                    "type":"object",
                                    "additionalProperties":false,
                                    "required":["file"],
                                    "properties":{"file":{"$ref":"#/$defs/FileRef"}}
                                }
                            ]
                        }
                    }
                }
            }
        }
    });
    if !root_required.is_empty() {
        root.as_object_mut()
            .expect("invocation schema is an object")
            .insert("required".to_owned(), Value::Array(root_required));
    }
    root
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{invocation_schema, AgentInvocation};

    #[test]
    fn invocation_wire_is_strict_and_has_no_legacy_aliases() {
        assert!(serde_json::from_value::<AgentInvocation>(json!({
            "query":"hello",
            "messages":[],
            "files":[],
            "inputs":{"style":"concise"}
        }))
        .is_ok());
        for alias in ["content", "message", "input", "payload"] {
            assert!(serde_json::from_value::<AgentInvocation>(json!({
                "query":"hello",
                (alias):"legacy"
            }))
            .is_err());
        }
        for field in ["query", "messages", "files", "inputs"] {
            assert!(serde_json::from_value::<AgentInvocation>(json!({
                (field): null
            }))
            .is_err());
        }
    }

    #[test]
    fn checked_in_base_schema_accepts_every_canonical_sample() {
        let schema: serde_json::Value =
            serde_json::from_str(workspace_asset_str!("schemas/agent-invocation-v1.json")).unwrap();
        let samples: serde_json::Value = serde_json::from_str(workspace_asset_str!(
            "schemas/agent-invocation-v1.samples.json"
        ))
        .unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        for (name, sample) in samples.as_object().unwrap() {
            assert!(
                validator.is_valid(sample),
                "canonical AgentInvocation sample '{name}' must satisfy the base schema"
            );
        }
        assert!(!validator.is_valid(&json!({"message":{"text":"legacy"}})));
        assert!(!validator.is_valid(&json!({"files":[{"file_id":"bad id"}]})));
    }

    #[test]
    fn invocation_and_conversation_schemas_derive_the_exact_same_contract() {
        let input = json!({
            "type":"object",
            "additionalProperties":false,
            "required":["query","messages","files","style","optional_note"],
            "properties":{
                "query":{"type":"string","minLength":1},
                "messages":{"type":"array","default":[]},
                "files":{"type":"array","default":[],"maxItems":10},
                "style":{"type":"string","enum":["concise","detailed"]},
                "optional_note":{"type":"string","default":""}
            }
        });
        let stateless = invocation_schema(input.clone(), false);
        assert_eq!(stateless["required"], json!(["query", "inputs"]));
        assert_eq!(
            stateless["$defs"]["BusinessInputs"]["required"],
            json!(["style"])
        );
        assert_eq!(
            stateless["properties"]["files"]["items"]["$ref"],
            "#/$defs/FileRef"
        );
        let conversation = invocation_schema(input, true);
        assert!(conversation["properties"].get("messages").is_none());
        assert_eq!(conversation["required"], json!(["query", "inputs"]));
        for alias in ["content", "message", "input", "payload"] {
            assert!(stateless["properties"].get(alias).is_none());
            assert!(conversation["properties"].get(alias).is_none());
        }
    }

    #[test]
    fn invocation_rejects_duplicate_files_and_reserved_business_fields() {
        let duplicate: AgentInvocation = serde_json::from_value(json!({
            "files":[{"file_id":"file_a"},{"file_id":"file_a"}]
        }))
        .unwrap();
        assert_eq!(
            duplicate.validate_base().unwrap_err().code(),
            "INVOCATION_INVALID"
        );

        let reserved: AgentInvocation = serde_json::from_value(json!({
            "inputs":{"query":"hidden"}
        }))
        .unwrap();
        assert_eq!(
            reserved.validate_base().unwrap_err().code(),
            "INVOCATION_INVALID"
        );
    }

    #[test]
    fn invocation_rejects_unsafe_history_roles_parts_and_empty_content() {
        for value in [
            json!({"messages":[{"role":"system","content":[{"text":"x"}]}]}),
            json!({"messages":[{"role":"user","content":[{"image_url":"https://example.invalid/x"}]}]}),
            json!({"messages":[{"role":"assistant","content":[]}]}),
        ] {
            let decoded = serde_json::from_value::<AgentInvocation>(value);
            if let Ok(decoded) = decoded {
                assert_eq!(
                    decoded.validate_base().unwrap_err().code(),
                    "INVOCATION_INVALID"
                );
            }
        }
    }
}
