//! In-process evidence that a Task response was validated against its frozen schema.
//!
//! This evidence is not a wire DTO or an authorization decision. The Task command still checks
//! current permissions, generation, deadline and first-winner state in its transaction.

use insight_platform_contracts::{
    canonical_digest, parse_strict_json, ArtifactRef, ClosedJsonSchema, JsonLimits, Sha256Digest,
    ValueRef,
};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::{error::Error, fmt, io};

const TASK_INPUT_LIMITS: JsonLimits = JsonLimits {
    max_bytes: 65_536,
    max_depth: 32,
    max_items_per_array: 4_096,
    max_properties_per_object: 1_024,
    max_string_bytes: 65_536,
};

/// Constructible only by validating the actual inline value or exact Artifact bytes.
///
/// Deliberately has no serialization or deserialization implementation: a caller-provided digest
/// must never stand in for instance validation.
#[derive(Clone)]
pub struct ValidatedTaskInput {
    schema_digest: Sha256Digest,
    content_digest: Sha256Digest,
    binding: InputBinding,
}

#[derive(Clone)]
enum InputBinding {
    Inline,
    Artifact(ArtifactRef),
}

impl fmt::Debug for ValidatedTaskInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedTaskInput")
            .finish_non_exhaustive()
    }
}

/// A fixed, body-free rejection for malformed, oversized, mismatched or schema-invalid input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidTaskInput;

impl fmt::Display for InvalidTaskInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("task input is invalid")
    }
}

impl Error for InvalidTaskInput {}

impl ValidatedTaskInput {
    pub fn validate_inline(
        schema: &ClosedJsonSchema,
        value: &Value,
    ) -> Result<Self, InvalidTaskInput> {
        let bytes = bounded_inline_bytes(value)?;
        let parsed = validate_document(schema, &bytes)?;
        if &parsed != value {
            return Err(InvalidTaskInput);
        }
        Ok(Self {
            schema_digest: schema.canonical_digest.clone(),
            content_digest: value_digest(value)?,
            binding: InputBinding::Inline,
        })
    }

    pub fn validate_interaction_inline(
        schema: &insight_platform_contracts::InteractionSchemaDocument,
        value: &Value,
    ) -> Result<Self, InvalidTaskInput> {
        schema.validate().map_err(|_| InvalidTaskInput)?;
        if schema.profile == insight_platform_contracts::MCP_FORM_SCHEMA_PROFILE_ID {
            let bytes = bounded_inline_bytes(value)?;
            let parsed =
                parse_strict_json(&bytes, TASK_INPUT_LIMITS).map_err(|_| InvalidTaskInput)?;
            schema
                .validate_mcp_form_instance(&parsed)
                .map_err(|_| InvalidTaskInput)?;
            Ok(Self {
                schema_digest: schema.canonical_digest.clone(),
                content_digest: value_digest(value)?,
                binding: InputBinding::Inline,
            })
        } else {
            let schema = ClosedJsonSchema {
                schema_version: schema.schema_version,
                profile: schema.profile.clone(),
                schema: schema.schema.clone(),
                canonical_digest: schema.canonical_digest.clone(),
            };
            Self::validate_inline(&schema, value)
        }
    }

    pub fn validate_artifact(
        schema: &ClosedJsonSchema,
        artifact: &ArtifactRef,
        bytes: &[u8],
    ) -> Result<Self, InvalidTaskInput> {
        artifact.validate().map_err(|_| InvalidTaskInput)?;
        if bytes.len() > TASK_INPUT_LIMITS.max_bytes
            || artifact.byte_length() != bytes.len() as u64
            || artifact.content_digest() != &bytes_digest(bytes)?
        {
            return Err(InvalidTaskInput);
        }
        validate_document(schema, bytes)?;
        Ok(Self {
            schema_digest: schema.canonical_digest.clone(),
            // Artifact identity is over the exact stored bytes, not normalized JSON.
            content_digest: artifact.content_digest().clone(),
            binding: InputBinding::Artifact(artifact.clone()),
        })
    }

    pub fn schema_digest(&self) -> &Sha256Digest {
        &self.schema_digest
    }

    pub fn content_digest(&self) -> &Sha256Digest {
        &self.content_digest
    }

    pub fn matches_response(
        &self,
        schema_digest: &Sha256Digest,
        content_digest: &Sha256Digest,
        value: &ValueRef,
    ) -> bool {
        if schema_digest != &self.schema_digest || content_digest != &self.content_digest {
            return false;
        }
        match (&self.binding, value) {
            (InputBinding::Inline, ValueRef::Inline { value }) => {
                let Ok(bytes) = bounded_inline_bytes(value) else {
                    return false;
                };
                let Ok(parsed) = parse_strict_json(&bytes, TASK_INPUT_LIMITS) else {
                    return false;
                };
                &parsed == value
                    && value_digest(value).is_ok_and(|actual| actual == self.content_digest)
            }
            (InputBinding::Artifact(validated), ValueRef::Artifact { artifact }) => {
                validated == artifact
            }
            _ => false,
        }
    }
}

fn validate_document(schema: &ClosedJsonSchema, bytes: &[u8]) -> Result<Value, InvalidTaskInput> {
    let value = parse_strict_json(bytes, TASK_INPUT_LIMITS).map_err(|_| InvalidTaskInput)?;
    schema
        .validate_instance(&value)
        .map_err(|_| InvalidTaskInput)?;
    Ok(value)
}

fn value_digest(value: &Value) -> Result<Sha256Digest, InvalidTaskInput> {
    canonical_digest(value)
        .map_err(|_| InvalidTaskInput)?
        .parse()
        .map_err(|_| InvalidTaskInput)
}

fn bytes_digest(bytes: &[u8]) -> Result<Sha256Digest, InvalidTaskInput> {
    let hex: String = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("sha256:{hex}")
        .parse()
        .map_err(|_| InvalidTaskInput)
}

fn bounded_inline_bytes(value: &Value) -> Result<Vec<u8>, InvalidTaskInput> {
    // An in-memory Value need not have come from a bounded transport. Guard recursion and traversal
    // before serializing; the shared strict parser remains the JSON validation authority.
    let mut remaining_nodes = TASK_INPUT_LIMITS.max_bytes;
    check_value_shape(value, 1, &mut remaining_nodes)?;
    let mut writer = BoundedJsonWriter(Vec::new());
    serde_json::to_writer(&mut writer, value).map_err(|_| InvalidTaskInput)?;
    Ok(writer.0)
}

fn check_value_shape(
    value: &Value,
    depth: usize,
    remaining_nodes: &mut usize,
) -> Result<(), InvalidTaskInput> {
    if depth > TASK_INPUT_LIMITS.max_depth {
        return Err(InvalidTaskInput);
    }
    *remaining_nodes = remaining_nodes.checked_sub(1).ok_or(InvalidTaskInput)?;
    match value {
        Value::Array(items) => {
            if items.len() > TASK_INPUT_LIMITS.max_items_per_array {
                return Err(InvalidTaskInput);
            }
            for item in items {
                check_value_shape(item, depth + 1, remaining_nodes)?;
            }
        }
        Value::Object(properties) => {
            if properties.len() > TASK_INPUT_LIMITS.max_properties_per_object {
                return Err(InvalidTaskInput);
            }
            for (key, item) in properties {
                if key.len() > TASK_INPUT_LIMITS.max_string_bytes {
                    return Err(InvalidTaskInput);
                }
                check_value_shape(item, depth + 1, remaining_nodes)?;
            }
        }
        Value::String(text) if text.len() > TASK_INPUT_LIMITS.max_string_bytes => {
            return Err(InvalidTaskInput);
        }
        _ => {}
    }
    Ok(())
}

struct BoundedJsonWriter(Vec<u8>);

impl io::Write for BoundedJsonWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) > TASK_INPUT_LIMITS.max_bytes {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::DataClassification;
    use serde_json::json;

    fn schema() -> ClosedJsonSchema {
        ClosedJsonSchema::build(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "count": {"type": "integer", "minimum": 0, "maximum": 3},
                "name": {"type": "string", "minLength": 1, "maxLength": 16, "x-platform-max-bytes": 64}
            },
            "required": ["count", "name"],
            "additionalProperties": false
        }))
        .unwrap()
    }

    fn artifact(bytes: &[u8]) -> ArtifactRef {
        ArtifactRef::new(
            "art_0198f1c3-8f49-7c3e-b1f3-773c28367b7e".parse().unwrap(),
            bytes_digest(bytes).unwrap(),
            bytes.len() as u64,
            "application/json",
            DataClassification::Confidential,
            Some("response.json".to_owned()),
        )
        .unwrap()
    }

    #[test]
    fn inline_evidence_binds_actual_content_and_frozen_schema() {
        let schema = schema();
        let value = json!({"count": 2, "name": "answer"});
        let evidence = ValidatedTaskInput::validate_inline(&schema, &value).unwrap();
        let digest = value_digest(&value).unwrap();
        assert_eq!(evidence.content_digest(), &digest);
        assert_eq!(evidence.schema_digest(), &schema.canonical_digest);
        assert!(evidence.matches_response(
            &schema.canonical_digest,
            &digest,
            &ValueRef::Inline {
                value: value.clone()
            }
        ));
        assert!(!evidence.matches_response(
            &schema.canonical_digest,
            &digest,
            &ValueRef::Inline {
                value: json!({"count": 3, "name": "answer"})
            }
        ));
        let wrong = value_digest(&json!({})).unwrap();
        assert!(!evidence.matches_response(
            &wrong,
            &digest,
            &ValueRef::Inline {
                value: value.clone()
            }
        ));
        assert!(!evidence.matches_response(
            &schema.canonical_digest,
            &wrong,
            &ValueRef::Inline { value }
        ));
    }

    #[test]
    fn both_branches_run_actual_instance_validation() {
        let schema = schema();
        for invalid in [
            json!({"count": 4, "name": "answer"}),
            json!({"count": 2}),
            json!({"count": "2", "name": "answer"}),
            json!({"count": 2, "name": "answer", "extra": true}),
        ] {
            assert_eq!(
                ValidatedTaskInput::validate_inline(&schema, &invalid).unwrap_err(),
                InvalidTaskInput
            );
            let bytes = serde_json::to_vec(&invalid).unwrap();
            assert_eq!(
                ValidatedTaskInput::validate_artifact(&schema, &artifact(&bytes), &bytes)
                    .unwrap_err(),
                InvalidTaskInput
            );
        }
    }

    #[test]
    fn artifact_uses_exact_raw_digest_length_and_complete_reference() {
        let schema = schema();
        let bytes = br#"{ "name": "answer", "count": 2 }"#;
        let reference = artifact(bytes);
        let evidence = ValidatedTaskInput::validate_artifact(&schema, &reference, bytes).unwrap();
        let parsed: Value = serde_json::from_slice(bytes).unwrap();
        assert_ne!(evidence.content_digest(), &value_digest(&parsed).unwrap());
        assert_eq!(evidence.content_digest(), reference.content_digest());
        assert!(evidence.matches_response(
            &schema.canonical_digest,
            reference.content_digest(),
            &ValueRef::Artifact {
                artifact: reference.clone()
            }
        ));
        let mut wire = serde_json::to_value(&reference).unwrap();
        for (key, changed) in [
            (
                "artifact_id",
                json!("art_0198f1c3-8f49-7c3e-b1f3-773c28367b7f"),
            ),
            ("classification", json!("restricted")),
            ("media_type", json!("text/plain")),
            ("display_name", json!("other.json")),
            ("byte_length", json!(bytes.len() + 1)),
        ] {
            let previous = wire[key].clone();
            wire[key] = changed;
            let changed: ArtifactRef = serde_json::from_value(wire.clone()).unwrap();
            assert!(!evidence.matches_response(
                &schema.canonical_digest,
                reference.content_digest(),
                &ValueRef::Artifact { artifact: changed }
            ));
            wire[key] = previous;
        }
        assert!(ValidatedTaskInput::validate_artifact(
            &schema,
            &reference,
            br#"{"name":"answer","count":2}"#
        )
        .is_err());
        let tampered = br#"{ "name": "answer", "count": 3 }"#;
        assert_eq!(bytes.len(), tampered.len());
        assert!(ValidatedTaskInput::validate_artifact(&schema, &reference, tampered).is_err());
        wire["byte_length"] = json!(bytes.len() + 1);
        let wrong_length = serde_json::from_value(wire).unwrap();
        assert!(ValidatedTaskInput::validate_artifact(&schema, &wrong_length, bytes).is_err());
    }

    #[test]
    fn equal_content_digest_cannot_switch_inline_and_artifact_branches() {
        let schema = schema();
        let value = json!({"count": 2, "name": "answer"});
        let bytes = insight_platform_contracts::canonical_json(&value).unwrap();
        let reference = artifact(&bytes);
        let inline = ValidatedTaskInput::validate_inline(&schema, &value).unwrap();
        let stored = ValidatedTaskInput::validate_artifact(&schema, &reference, &bytes).unwrap();
        assert_eq!(inline.content_digest(), stored.content_digest());
        assert!(!inline.matches_response(
            &schema.canonical_digest,
            stored.content_digest(),
            &ValueRef::Artifact {
                artifact: reference
            }
        ));
        assert!(!stored.matches_response(
            &schema.canonical_digest,
            inline.content_digest(),
            &ValueRef::Inline { value }
        ));
    }

    #[test]
    fn artifact_json_rejects_duplicates_trailing_values_and_invalid_utf8() {
        let schema = schema();
        for bytes in [
            br#"{"count":1,"count":2,"name":"secret"}"#.as_slice(),
            br#"{"count":1,"\u0063ount":2,"name":"secret"}"#.as_slice(),
            br#"{"count":2,"name":"secret"} {}"#.as_slice(),
            b"\xff".as_slice(),
            br#"{"count":9007199254740992,"name":"secret"}"#.as_slice(),
        ] {
            let error = ValidatedTaskInput::validate_artifact(&schema, &artifact(bytes), bytes)
                .unwrap_err();
            assert_eq!(error, InvalidTaskInput);
            assert_eq!(error.to_string(), "task input is invalid");
            assert_eq!(format!("{error:?}"), "InvalidTaskInput");
            assert!(error.source().is_none());
        }
    }

    #[test]
    fn forged_schema_envelopes_cannot_issue_evidence() {
        let value = json!({"count": 2, "name": "answer"});
        let bytes = serde_json::to_vec(&value).unwrap();
        let valid = schema();
        let mut invalid = valid.clone();
        invalid.schema["properties"]["count"]["maximum"] = json!(100);
        assert!(ValidatedTaskInput::validate_inline(&invalid, &value).is_err());
        assert!(
            ValidatedTaskInput::validate_artifact(&invalid, &artifact(&bytes), &bytes).is_err()
        );
        invalid = valid;
        invalid.profile = "future.schema/1".to_owned();
        assert!(ValidatedTaskInput::validate_inline(&invalid, &value).is_err());
    }

    #[test]
    fn json_limits_apply_before_schema_validation_and_inline_serialization_is_bounded() {
        let schema = schema();
        let oversized = json!({"count": 2, "name": "x".repeat(65_536)});
        assert!(ValidatedTaskInput::validate_inline(&schema, &oversized).is_err());
        let bytes = serde_json::to_vec(&oversized).unwrap();
        assert!(ValidatedTaskInput::validate_artifact(&schema, &artifact(&bytes), &bytes).is_err());
        let too_many_items = Value::Array(vec![Value::Null; 4_097]);
        let too_many_properties = Value::Object(
            (0..1_025)
                .map(|index| (index.to_string(), Value::Null))
                .collect(),
        );
        let mut too_deep = Value::Null;
        for _ in 0..32 {
            too_deep = Value::Array(vec![too_deep]);
        }
        for value in [too_many_items, too_many_properties, too_deep] {
            assert!(bounded_inline_bytes(&value).is_err());
            let bytes = serde_json::to_vec(&value).unwrap();
            assert!(
                ValidatedTaskInput::validate_artifact(&schema, &artifact(&bytes), &bytes).is_err()
            );
        }
        // The strings individually fit; their escaped serialized representation does not.
        assert!(bounded_inline_bytes(&json!({"name": "\n".repeat(40_000)})).is_err());
        assert!(bounded_inline_bytes(
            &json!({"left": "x".repeat(40_000), "right": "x".repeat(40_000)})
        )
        .is_err());
    }

    #[test]
    fn schema_valid_instances_still_obey_task_container_and_depth_limits() {
        let array =
            json!({"type":"array","minItems":0,"maxItems":5_000,"items":{"type":"boolean"}});
        let mut nested_schema = json!({"type":"boolean"});
        let mut nested_value = json!(true);
        for _ in 0..31 {
            nested_schema = json!({"type":"array","minItems":0,"maxItems":1,"items":nested_schema});
            nested_value = json!([nested_value]);
        }
        let object_properties: serde_json::Map<String, Value> = (0..1_025)
            .map(|index| (index.to_string(), json!({"type":"boolean"})))
            .collect();
        let object_values: serde_json::Map<String, Value> = (0..1_025)
            .map(|index| (index.to_string(), json!(true)))
            .collect();
        for (property, value) in [
            (array, Value::Array(vec![json!(true); 4_097])),
            (nested_schema, nested_value),
            (
                json!({"type":"object","properties":object_properties,"required":[],"additionalProperties":false}),
                Value::Object(object_values),
            ),
        ] {
            let schema = ClosedJsonSchema::build(json!({
                "$schema":"https://json-schema.org/draft/2020-12/schema",
                "type":"object","properties":{"input":property},
                "required":["input"],"additionalProperties":false
            }))
            .unwrap();
            let value = json!({"input":value});
            schema.validate_instance(&value).unwrap();
            assert!(ValidatedTaskInput::validate_inline(&schema, &value).is_err());
            let bytes = serde_json::to_vec(&value).unwrap();
            assert!(bytes.len() < TASK_INPUT_LIMITS.max_bytes);
            assert!(
                ValidatedTaskInput::validate_artifact(&schema, &artifact(&bytes), &bytes).is_err()
            );
        }
    }
}
