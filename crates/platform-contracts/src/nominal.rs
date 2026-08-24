use crate::{
    id::{ResourceKind, RESOURCE_KIND_DESCRIPTORS},
    json::MAX_SAFE_JSON_INTEGER,
    registry::{
        ApiProblemCode, DataClassification, FailureClass, FailureSource, PlatformFailureCode,
        Retryability,
    },
    types::{MAX_ARTIFACT_BYTES, MAX_FIELD_ERRORS, MAX_OPAQUE_CURSOR_BYTES, MAX_SAFE_TEXT_BYTES},
};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;

pub const NOMINAL_REFERENCE_PREFIX: &str = "urn:insight:platform:v1:nominal:";

pub fn nominal_schemas() -> BTreeMap<&'static str, Value> {
    BTreeMap::from([
        ("ApiProblem", api_problem_schema()),
        ("ArtifactRef", artifact_ref_schema()),
        ("DecimalMoney", decimal_money_schema()),
        ("Digest", digest_schema()),
        ("Failure", failure_schema()),
        ("OpaqueListCursor", cursor_schema("OpaqueListCursor")),
        (
            "OpaqueRunEventCursor",
            cursor_schema("OpaqueRunEventCursor"),
        ),
        ("UtcTimestamp", utc_timestamp_schema()),
        ("UuidV7Id", resource_id_schema()),
    ])
}

pub fn nominal_schema_files() -> BTreeMap<&'static str, (&'static str, Value)> {
    BTreeMap::from([
        (
            "ApiProblem",
            (
                "schemas/nominal/api-problem.schema.json",
                api_problem_schema(),
            ),
        ),
        (
            "ArtifactRef",
            (
                "schemas/nominal/artifact-ref.schema.json",
                artifact_ref_schema(),
            ),
        ),
        (
            "DecimalMoney",
            (
                "schemas/nominal/decimal-money.schema.json",
                decimal_money_schema(),
            ),
        ),
        (
            "Digest",
            ("schemas/nominal/digest.schema.json", digest_schema()),
        ),
        (
            "Failure",
            ("schemas/nominal/failure.schema.json", failure_schema()),
        ),
        (
            "OpaqueListCursor",
            (
                "schemas/nominal/opaque-list-cursor.schema.json",
                cursor_schema("OpaqueListCursor"),
            ),
        ),
        (
            "OpaqueRunEventCursor",
            (
                "schemas/nominal/opaque-run-event-cursor.schema.json",
                cursor_schema("OpaqueRunEventCursor"),
            ),
        ),
        (
            "UtcTimestamp",
            (
                "schemas/nominal/utc-timestamp.schema.json",
                utc_timestamp_schema(),
            ),
        ),
        (
            "UuidV7Id",
            (
                "schemas/nominal/uuid-v7-id.schema.json",
                resource_id_schema(),
            ),
        ),
    ])
}

pub fn canonical_schema_digest(schema: &Value) -> String {
    let bytes = serde_jcs::to_vec(schema).expect("nominal schema is canonicalizable");
    let digest = Sha256::digest(bytes);
    format!("sha256:{}", lowercase_hex(&digest))
}

pub fn pinned_nominal_reference(name: &str) -> Option<String> {
    nominal_schemas().get(name).map(|schema| {
        format!(
            "{NOMINAL_REFERENCE_PREFIX}{name}@{}",
            canonical_schema_digest(schema)
        )
    })
}

pub fn is_known_pinned_nominal_reference(reference: &str) -> bool {
    schema_for_pinned_nominal_reference(reference).is_some()
}

pub(crate) fn schema_for_pinned_nominal_reference(reference: &str) -> Option<Value> {
    let (name, supplied_digest) = reference
        .strip_prefix(NOMINAL_REFERENCE_PREFIX)
        .and_then(|rest| rest.rsplit_once('@'))?;
    nominal_schemas()
        .get(name)
        .filter(|schema| canonical_schema_digest(schema) == supplied_digest)
        .cloned()
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn schema_document(name: &str, body: Value) -> Value {
    let mut object = body.as_object().expect("schema body is an object").clone();
    object.insert(
        "$schema".to_owned(),
        Value::String("https://json-schema.org/draft/2020-12/schema".to_owned()),
    );
    object.insert(
        "$id".to_owned(),
        Value::String(format!("urn:insight:platform:v1:schema:{name}")),
    );
    object.insert("title".to_owned(), Value::String(name.to_owned()));
    Value::Object(object)
}

fn string_schema(minimum: u64, maximum: u64, max_bytes: u64) -> Value {
    json!({
        "type": "string",
        "minLength": minimum,
        "maxLength": maximum,
        "x-platform-max-bytes": max_bytes
    })
}

fn nullable(schema: Value) -> Value {
    json!({"oneOf": [schema, {"type": "null"}]})
}

fn enum_values<T>(items: &[T], to_wire: impl Fn(&T) -> &'static str) -> Vec<&'static str> {
    items.iter().map(to_wire).collect()
}

fn resource_pattern(prefixes: &[&str]) -> String {
    format!(
        "^({})_[0-9a-f]{{8}}-[0-9a-f]{{4}}-7[0-9a-f]{{3}}-[89ab][0-9a-f]{{3}}-[0-9a-f]{{12}}$",
        prefixes.join("|")
    )
}

fn resource_id_schema() -> Value {
    schema_document(
        "UuidV7Id",
        json!({
            "type": "string",
            "description": "Known resource prefix plus canonical lowercase RFC 9562 UUIDv7.",
            "pattern": resource_pattern(
                &RESOURCE_KIND_DESCRIPTORS
                    .iter()
                    .map(|descriptor| descriptor.prefix)
                    .collect::<Vec<_>>()
            ),
            "minLength": 39,
            "maxLength": 48,
            "x-platform-max-bytes": 48
        }),
    )
}

fn digest_body() -> Value {
    json!({
        "type": "string",
        "pattern": "^sha256:[0-9a-f]{64}$",
        "minLength": 71,
        "maxLength": 71,
        "x-platform-max-bytes": 71
    })
}

fn digest_schema() -> Value {
    schema_document("Digest", digest_body())
}

fn utc_timestamp_schema() -> Value {
    schema_document(
        "UtcTimestamp",
        json!({
            "type": "string",
            "description": "UTC RFC 3339 timestamp with exactly six fractional digits.",
            "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}Z$",
            "minLength": 27,
            "maxLength": 27,
            "x-platform-max-bytes": 27
        }),
    )
}

fn decimal_money_schema() -> Value {
    schema_document(
        "DecimalMoney",
        json!({
            "type": "object",
            "properties": {
                "currency": {
                    "type": "string",
                    "pattern": "^[A-Z]{3}$",
                    "minLength": 3,
                    "maxLength": 3,
                    "x-platform-max-bytes": 3
                },
                "minor_units": {"type": "integer", "minimum": -(MAX_SAFE_JSON_INTEGER as i64), "maximum": MAX_SAFE_JSON_INTEGER},
                "scale": {"type": "integer", "minimum": 0, "maximum": 18}
            },
            "required": ["currency", "minor_units", "scale"],
            "additionalProperties": false
        }),
    )
}

fn cursor_schema(name: &str) -> Value {
    schema_document(
        name,
        json!({
            "type": "string",
            "description": "Opaque token. The nominal schema name is its non-interchangeable purpose.",
            "pattern": "^[\\u0020-\\u007e]+$",
            "minLength": 1,
            "maxLength": MAX_OPAQUE_CURSOR_BYTES,
            "x-platform-max-bytes": MAX_OPAQUE_CURSOR_BYTES
        }),
    )
}

fn artifact_ref_body() -> Value {
    json!({
        "type": "object",
        "properties": {
            "artifact_id": {
                "type": "string",
                "pattern": resource_pattern(&[ResourceKind::Artifact.descriptor().prefix]),
                "minLength": 40,
                "maxLength": 40,
                "x-platform-max-bytes": 40
            },
            "content_digest": digest_body(),
            "byte_length": {"type": "integer", "minimum": 0, "maximum": MAX_ARTIFACT_BYTES},
            "media_type": string_schema(1, 255, 255),
            "classification": {
                "type": "string",
                "enum": enum_values(DataClassification::ALL, |value| value.as_str()),
                "minLength": 1,
                "maxLength": 32,
                "x-platform-max-bytes": 32
            },
            "display_name": nullable(string_schema(1, 255, 1_020))
        },
        "required": [
            "artifact_id", "content_digest", "byte_length", "media_type", "classification",
            "display_name"
        ],
        "additionalProperties": false
    })
}

fn artifact_ref_schema() -> Value {
    schema_document("ArtifactRef", artifact_ref_body())
}

fn failure_code_schema() -> Value {
    json!({
        "oneOf": [
            {
                "type": "object",
                "properties": {
                    "kind": {"type": "string", "const": "platform", "minLength": 8, "maxLength": 8, "x-platform-max-bytes": 8},
                    "code": {
                        "type": "string",
                        "enum": enum_values(PlatformFailureCode::ALL, |value| value.as_str()),
                        "minLength": 1,
                        "maxLength": 64,
                        "x-platform-max-bytes": 64
                    }
                },
                "required": ["kind", "code"],
                "additionalProperties": false
            },
            {
                "type": "object",
                "properties": {
                    "kind": {"type": "string", "const": "declared", "minLength": 8, "maxLength": 8, "x-platform-max-bytes": 8},
                    "interface_revision_id": {
                        "type": "string",
                        "pattern": resource_pattern(&[
                            ResourceKind::AgentInterfaceRevision.descriptor().prefix,
                            ResourceKind::CapabilityInterfaceRevision.descriptor().prefix
                        ]),
                        "minLength": 42,
                        "maxLength": 43,
                        "x-platform-max-bytes": 43
                    },
                    "code": {
                        "type": "string",
                        "pattern": "^[a-z][a-z0-9_]{0,63}$",
                        "minLength": 1,
                        "maxLength": 64,
                        "x-platform-max-bytes": 64
                    }
                },
                "required": ["kind", "interface_revision_id", "code"],
                "additionalProperties": false
            }
        ]
    })
}

fn failure_schema() -> Value {
    schema_document(
        "Failure",
        json!({
            "type": "object",
            "properties": {
                "code": failure_code_schema(),
                "class": {
                    "type": "string",
                    "enum": enum_values(FailureClass::ALL, |value| value.as_str()),
                    "minLength": 1,
                    "maxLength": 32,
                    "x-platform-max-bytes": 32
                },
                "retryability": {
                    "type": "string",
                    "enum": enum_values(Retryability::ALL, |value| value.as_str()),
                    "minLength": 1,
                    "maxLength": 32,
                    "x-platform-max-bytes": 32
                },
                "safe_message": nullable(string_schema(0, 4_096, MAX_SAFE_TEXT_BYTES as u64)),
                "details_ref": nullable(artifact_ref_body()),
                "source": {
                    "type": "string",
                    "enum": enum_values(FailureSource::ALL, |value| value.as_str()),
                    "minLength": 1,
                    "maxLength": 32,
                    "x-platform-max-bytes": 32
                }
            },
            "required": ["code", "class", "retryability", "safe_message", "details_ref", "source"],
            "additionalProperties": false
        }),
    )
}

fn field_error_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "field": string_schema(1, 512, 2_048),
            "code": {
                "type": "string",
                "pattern": "^[a-z][a-z0-9_]{0,63}$",
                "minLength": 1,
                "maxLength": 64,
                "x-platform-max-bytes": 64
            },
            "safe_message": nullable(string_schema(0, 4_096, MAX_SAFE_TEXT_BYTES as u64))
        },
        "required": ["field", "code", "safe_message"],
        "additionalProperties": false
    })
}

fn api_problem_schema() -> Value {
    schema_document(
        "ApiProblem",
        json!({
            "type": "object",
            "properties": {
                "type_uri": string_schema(1, 2_048, 8_192),
                "title": string_schema(1, 4_096, MAX_SAFE_TEXT_BYTES as u64),
                "status": {"type": "integer", "minimum": 400, "maximum": 599},
                "code": {
                    "type": "string",
                    "enum": enum_values(ApiProblemCode::ALL, |value| value.as_str()),
                    "minLength": 1,
                    "maxLength": 64,
                    "x-platform-max-bytes": 64
                },
                "detail": nullable(string_schema(0, 4_096, MAX_SAFE_TEXT_BYTES as u64)),
                "request_id": {
                    "type": "string",
                    "pattern": resource_pattern(&[ResourceKind::ServerRequest.descriptor().prefix]),
                    "minLength": 40,
                    "maxLength": 40,
                    "x-platform-max-bytes": 40
                },
                "retryable": {"type": "boolean"},
                "retry_after_ms": nullable(json!({"type": "integer", "minimum": 0, "maximum": 86_400_000_u64})),
                "field_errors": {
                    "type": "array",
                    "items": field_error_schema(),
                    "minItems": 0,
                    "maxItems": MAX_FIELD_ERRORS
                }
            },
            "required": [
                "type_uri", "title", "status", "code", "detail", "request_id", "retryable",
                "retry_after_ms", "field_errors"
            ],
            "additionalProperties": false
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_nominal_schema_has_a_stable_exact_reference() {
        for (name, schema) in nominal_schemas() {
            jsonschema::options()
                .with_draft(jsonschema::Draft::Draft202012)
                .build(&schema)
                .unwrap_or_else(|failure| panic!("{name} schema does not compile: {failure}"));
            let reference = pinned_nominal_reference(name).unwrap();
            assert!(reference.ends_with(&canonical_schema_digest(&schema)));
            assert!(is_known_pinned_nominal_reference(&reference));
        }
        assert!(!is_known_pinned_nominal_reference(
            "urn:insight:platform:v1:nominal:Digest@sha256:0000000000000000000000000000000000000000000000000000000000000000"
        ));
    }
}
