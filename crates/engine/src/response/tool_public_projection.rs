//! Pure projection of a frozen tool publication policy.
//!
//! This module is deliberately detached from workers, repositories, and live
//! publication. It accepts only complete, already schema-validated model
//! arguments and a completed executor result. Consequently there is no API
//! which could attempt string-level redaction of a partial JSON argument
//! delta. A producer may forward raw Provider deltas only when
//! [`WorkflowToolPublicProjection::raw_argument_deltas_authorized`] returns
//! `true`.

use std::collections::BTreeSet;

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::{
    resource_policy::{ToolPublicArguments, ToolPublicPolicy},
    schema::{compile_schema_2020, JsonSchemaValidator},
};

use super::{
    validate_bounded_public_json, WorkflowPublicResultError, WorkflowToolContent,
    WorkflowToolResult, MAX_PUBLIC_LABEL_BYTES, MAX_WORKFLOW_PUBLIC_JSON_BYTES,
};

const MAX_FROZEN_TOOL_PUBLIC_POLICY_BYTES: usize = 256 * 1_024;

/// Completed public argument projections for both protocol branches.
///
/// `workflow_started_arguments` is the optional object carried by
/// `workflow.tool.started`. `standard_function_call_arguments` is a canonical
/// JSON string and is present only for the `arguments: all` branch which may
/// produce standard Responses function-call items and `arguments.done`.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowToolCompletedArgumentsProjection {
    workflow_started_arguments: Option<Value>,
    standard_function_call_arguments: Option<String>,
}

impl WorkflowToolCompletedArgumentsProjection {
    pub fn workflow_started_arguments(&self) -> Option<&Value> {
        self.workflow_started_arguments.as_ref()
    }

    pub fn standard_function_call_arguments(&self) -> Option<&str> {
        self.standard_function_call_arguments.as_deref()
    }
}

/// A fail-closed, executable view of one frozen `effective_public_policy`.
///
/// Construction re-decodes the canonical linker evidence, rechecks its
/// cross-field invariant, and compiles the frozen public result schema. The
/// original descriptor or current registry is never consulted at runtime.
#[derive(Debug, Clone)]
pub struct WorkflowToolPublicProjection {
    call: bool,
    arguments: ToolPublicArguments,
    result_validator: Option<JsonSchemaValidator>,
}

impl WorkflowToolPublicProjection {
    /// Decodes exactly the normalized `effective_public_policy` stored in a
    /// Deployment Revision. Missing defaults, duplicate fields, unknown
    /// members, invalid schemas, and contradictory authorization fail closed.
    pub fn from_frozen_effective_policy(
        frozen_policy: &Value,
    ) -> Result<Self, WorkflowPublicResultError> {
        let encoded = serde_jcs::to_vec(frozen_policy).map_err(|_| invalid_policy())?;
        if encoded.len() > MAX_FROZEN_TOOL_PUBLIC_POLICY_BYTES {
            return Err(invalid_policy());
        }

        let policy = serde_json::from_value::<ToolPublicPolicy>(frozen_policy.clone())
            .map_err(|_| invalid_policy())?;
        let normalized = serde_json::to_value(&policy).map_err(|_| invalid_policy())?;
        if &normalized != frozen_policy {
            return Err(invalid_policy());
        }
        validate_policy_invariants(&policy)?;

        let result_validator = policy
            .result_schema
            .as_ref()
            .map(|schema| {
                if !normalized_closed_result_schema(schema) {
                    return Err("frozen public result schema is not closed".to_owned());
                }
                compile_schema_2020(schema)
            })
            .transpose()
            .map_err(|_| invalid_policy())?;

        Ok(Self {
            call: policy.call,
            arguments: policy.arguments,
            result_validator,
        })
    }

    /// Whether the workflow-level tool call metadata is public at all.
    pub const fn call_authorized(&self) -> bool {
        self.call
    }

    /// Whether a completed result has a frozen caller-visible schema.
    pub const fn result_authorized(&self) -> bool {
        self.result_validator.is_some()
    }

    /// Whether standard Responses function-call item, delta, and done events
    /// are authorized. An exhaustive field list deliberately remains false.
    pub fn standard_function_call_events_authorized(&self) -> bool {
        self.call && matches!(self.arguments, ToolPublicArguments::All)
    }

    /// Whether an exact raw Provider argument delta may be published.
    ///
    /// No delta projection method exists for private/field policies: those
    /// modes must buffer until the complete validated object is available.
    pub fn raw_argument_deltas_authorized(&self) -> bool {
        self.standard_function_call_events_authorized()
    }

    /// Projects one complete, model-visible, schema-validated argument object.
    ///
    /// The caller must pass the object before server-only protected context is
    /// injected. This method never accepts partial JSON. All returned values
    /// are rechecked against public inline size and structural bounds.
    pub fn project_validated_completed_arguments(
        &self,
        validated_arguments: &Value,
    ) -> Result<WorkflowToolCompletedArgumentsProjection, WorkflowPublicResultError> {
        if matches!(self.arguments, ToolPublicArguments::Private) {
            return Ok(WorkflowToolCompletedArgumentsProjection {
                workflow_started_arguments: None,
                standard_function_call_arguments: None,
            });
        }
        let arguments = validated_arguments
            .as_object()
            .ok_or_else(invalid_arguments)?;

        let workflow_started_arguments = match &self.arguments {
            ToolPublicArguments::Private => unreachable!("private arguments returned above"),
            ToolPublicArguments::All => Some(validated_arguments.clone()),
            ToolPublicArguments::Fields(fields) => {
                let projected = fields
                    .iter()
                    .filter_map(|field| {
                        arguments
                            .get(field)
                            .map(|value| (field.clone(), value.clone()))
                    })
                    .collect::<Map<_, _>>();
                Some(Value::Object(projected))
            }
        };

        if let Some(arguments) = &workflow_started_arguments {
            validate_bounded_public_json(arguments, MAX_WORKFLOW_PUBLIC_JSON_BYTES)?;
        }

        let standard_function_call_arguments = if self.standard_function_call_events_authorized() {
            Some(
                serde_jcs::to_string(
                    workflow_started_arguments
                        .as_ref()
                        .expect("arguments: all always projects a complete object"),
                )
                .map_err(|_| invalid_arguments())?,
            )
        } else {
            None
        };

        Ok(WorkflowToolCompletedArgumentsProjection {
            workflow_started_arguments,
            standard_function_call_arguments,
        })
    }

    /// Projects a completed safe result into terminal/event tool content.
    ///
    /// A private result policy returns `None` without inspecting the executor
    /// value. A public result is revalidated against the exact frozen schema.
    /// Plain public objects become `output_json`; an explicitly tagged content
    /// object (or exact `{ "content": [...] }` envelope) is decoded through
    /// the closed [`WorkflowToolContent`] union. Thus image, file, and audio
    /// content can only carry an integrity-checked `ArtifactRef` and never
    /// inline bytes.
    pub fn project_validated_completed_result(
        &self,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        validated_public_result: &Value,
    ) -> Result<Option<WorkflowToolResult>, WorkflowPublicResultError> {
        let Some(validator) = &self.result_validator else {
            return Ok(None);
        };
        if !self.call || !validator.is_valid(validated_public_result) {
            return Err(invalid_result());
        }
        validate_bounded_public_json(validated_public_result, MAX_WORKFLOW_PUBLIC_JSON_BYTES)?;

        let content = decode_public_result_content(validated_public_result)?;
        WorkflowToolResult::new(call_id, tool_name, content)
            .map(Some)
            .map_err(|_| invalid_result())
    }
}

fn validate_policy_invariants(policy: &ToolPublicPolicy) -> Result<(), WorkflowPublicResultError> {
    if !policy.call
        && (!matches!(policy.arguments, ToolPublicArguments::Private)
            || policy.result_schema.is_some())
    {
        return Err(invalid_policy());
    }
    if let ToolPublicArguments::Fields(fields) = &policy.arguments {
        if fields.is_empty() || fields.iter().any(|field| !valid_public_field(field)) {
            return Err(invalid_policy());
        }
    }
    Ok(())
}

fn valid_public_field(field: &str) -> bool {
    !field.is_empty()
        && field.len() <= MAX_PUBLIC_LABEL_BYTES
        && !field.chars().any(char::is_control)
}

fn normalized_closed_result_schema(schema: &Value) -> bool {
    let Some(document) = schema.as_object() else {
        return false;
    };
    if document.get("$schema").and_then(Value::as_str)
        != Some("https://json-schema.org/draft/2020-12/schema")
        || !document.get("$defs").is_some_and(Value::is_object)
        || contains_unsafe_schema_reference(schema)
    {
        return false;
    }

    let mut observed_references = BTreeSet::new();
    closed_object_schema_facts(schema, schema, &mut observed_references) == (true, true)
        && safe_public_value_schema(schema, schema, &mut BTreeSet::new())
}

/// Conservative public-Schema subset. Runtime byte/depth limits bound actual
/// values, while this proof prevents an unconstrained nested object/array from
/// carrying fields that the declared public contract never named.
fn safe_public_value_schema(
    schema: &Value,
    root: &Value,
    observed_references: &mut BTreeSet<String>,
) -> bool {
    let Value::Object(object) = schema else {
        // `false` accepts no value and is safe; `true` accepts arbitrary
        // nested data and cannot be used as a public projection contract.
        return schema == &Value::Bool(false);
    };

    if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
        let Some(pointer) = reference.strip_prefix('#') else {
            return false;
        };
        if !observed_references.insert(reference.to_owned()) {
            return false;
        }
        let safe = root
            .pointer(pointer)
            .is_some_and(|target| safe_public_value_schema(target, root, observed_references));
        observed_references.remove(reference);
        return safe;
    }

    if let Some(value) = object.get("const") {
        return validate_bounded_public_json(value, MAX_WORKFLOW_PUBLIC_JSON_BYTES).is_ok();
    }
    if let Some(values) = object.get("enum") {
        return values.as_array().is_some_and(|values| {
            !values.is_empty()
                && values.iter().all(|value| {
                    validate_bounded_public_json(value, MAX_WORKFLOW_PUBLIC_JSON_BYTES).is_ok()
                })
        });
    }

    for keyword in ["oneOf", "anyOf"] {
        if let Some(branches) = object.get(keyword) {
            return branches.as_array().is_some_and(|branches| {
                !branches.is_empty()
                    && branches
                        .iter()
                        .all(|branch| safe_public_value_schema(branch, root, observed_references))
            });
        }
    }
    // Proving closure assembled across multiple allOf branches requires a
    // full unevaluated-properties evaluator. Reject it here instead of
    // accidentally treating individually open branches as a safe projection.
    if object.contains_key("allOf") || !object.contains_key("type") {
        return false;
    }

    let types = match object.get("type") {
        Some(Value::String(kind)) => vec![kind.as_str()],
        Some(Value::Array(kinds)) => {
            let kinds = kinds.iter().map(Value::as_str).collect::<Option<Vec<_>>>();
            let Some(kinds) = kinds else {
                return false;
            };
            if kinds.is_empty() {
                return false;
            }
            kinds
        }
        _ => return false,
    };
    if types.iter().any(|kind| {
        !matches!(
            *kind,
            "null" | "boolean" | "integer" | "number" | "string" | "object" | "array"
        )
    }) {
        return false;
    }

    let object_safe = !types.contains(&"object")
        || (object.get("additionalProperties") == Some(&Value::Bool(false))
            || object.get("unevaluatedProperties") == Some(&Value::Bool(false)))
            && object
                .get("patternProperties")
                .is_none_or(|patterns| patterns.as_object().is_some_and(Map::is_empty))
            && object.get("properties").is_none_or(|properties| {
                properties.as_object().is_some_and(|properties| {
                    properties.values().all(|property| {
                        safe_public_value_schema(property, root, observed_references)
                    })
                })
            });

    let array_safe = !types.contains(&"array")
        || object.get("items").is_some_and(|items| {
            safe_public_value_schema(items, root, observed_references)
                && object.get("prefixItems").is_none_or(|prefix| {
                    prefix.as_array().is_some_and(|items| {
                        items
                            .iter()
                            .all(|item| safe_public_value_schema(item, root, observed_references))
                    })
                })
        });
    object_safe && array_safe
}

fn closed_object_schema_facts(
    schema: &Value,
    root: &Value,
    observed_references: &mut BTreeSet<String>,
) -> (bool, bool) {
    let Some(object) = schema.as_object() else {
        return (false, false);
    };
    let direct_object = object.get("type").and_then(Value::as_str) == Some("object");
    let direct_closed = object.get("additionalProperties") == Some(&Value::Bool(false))
        || object.get("unevaluatedProperties") == Some(&Value::Bool(false));
    let referenced = object
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|reference| {
            let pointer = reference.strip_prefix('#')?;
            if !observed_references.insert(reference.to_owned()) {
                return None;
            }
            let facts =
                closed_object_schema_facts(root.pointer(pointer)?, root, observed_references);
            observed_references.remove(reference);
            Some(facts)
        })
        .unwrap_or((false, false));

    (direct_object || referenced.0, direct_closed || referenced.1)
}

fn contains_unsafe_schema_reference(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object.contains_key("$dynamicRef")
                || object.contains_key("$recursiveRef")
                || object.get("$ref").is_some_and(|reference| {
                    !reference.as_str().is_some_and(|reference| {
                        reference
                            .strip_prefix("#/$defs/")
                            .is_some_and(|name| !name.is_empty() && !name.contains('/'))
                    })
                })
                || object.values().any(contains_unsafe_schema_reference)
        }
        Value::Array(values) => values.iter().any(contains_unsafe_schema_reference),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExplicitContentEnvelope {
    content: Vec<WorkflowToolContent>,
}

fn decode_public_result_content(
    validated_public_result: &Value,
) -> Result<Vec<WorkflowToolContent>, WorkflowPublicResultError> {
    let object = validated_public_result
        .as_object()
        .ok_or_else(invalid_result)?;

    if object.len() == 1 && object.contains_key("content") {
        return serde_json::from_value::<ExplicitContentEnvelope>(validated_public_result.clone())
            .map(|envelope| envelope.content)
            .map_err(|_| invalid_result());
    }

    if object
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(is_explicit_content_type)
    {
        return serde_json::from_value::<WorkflowToolContent>(validated_public_result.clone())
            .map(|content| vec![content])
            .map_err(|_| invalid_result());
    }

    WorkflowToolContent::output_json(validated_public_result.clone())
        .map(|content| vec![content])
        .map_err(|_| invalid_result())
}

fn is_explicit_content_type(value: &str) -> bool {
    matches!(
        value,
        "output_text" | "output_json" | "output_image" | "output_file" | "output_audio"
    )
}

fn invalid_policy() -> WorkflowPublicResultError {
    WorkflowPublicResultError::new("frozen workflow tool public policy is invalid")
}

fn invalid_arguments() -> WorkflowPublicResultError {
    WorkflowPublicResultError::new("completed workflow tool public arguments are invalid")
}

fn invalid_result() -> WorkflowPublicResultError {
    WorkflowPublicResultError::new("completed workflow tool public result is invalid")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::{json, Value};

    use crate::resource_policy::{ToolPublicArguments, ToolPublicPolicy};

    use super::WorkflowToolPublicProjection;

    fn normalized_schema(properties: Value, required: &[&str]) -> Value {
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {},
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": false
        })
    }

    fn frozen(call: bool, arguments: ToolPublicArguments, result_schema: Option<Value>) -> Value {
        serde_json::to_value(ToolPublicPolicy {
            call,
            arguments,
            result_schema,
        })
        .unwrap()
    }

    #[test]
    fn policy_matrix_keeps_private_fields_and_all_on_distinct_protocol_branches() {
        let private = WorkflowToolPublicProjection::from_frozen_effective_policy(&frozen(
            false,
            ToolPublicArguments::Private,
            None,
        ))
        .unwrap();
        assert!(!private.call_authorized());
        assert!(!private.result_authorized());
        assert!(!private.standard_function_call_events_authorized());
        assert!(!private.raw_argument_deltas_authorized());
        let projection = private
            .project_validated_completed_arguments(&json!("private values are not inspected"))
            .unwrap();
        assert_eq!(projection.workflow_started_arguments(), None);
        assert_eq!(projection.standard_function_call_arguments(), None);

        let metadata_only = WorkflowToolPublicProjection::from_frozen_effective_policy(&frozen(
            true,
            ToolPublicArguments::Private,
            None,
        ))
        .unwrap();
        assert!(metadata_only.call_authorized());
        assert!(!metadata_only.standard_function_call_events_authorized());
        assert!(!metadata_only.raw_argument_deltas_authorized());

        let fields = WorkflowToolPublicProjection::from_frozen_effective_policy(&frozen(
            true,
            ToolPublicArguments::Fields(BTreeSet::from(["query".to_owned()])),
            None,
        ))
        .unwrap();
        assert!(fields.call_authorized());
        assert!(!fields.standard_function_call_events_authorized());
        assert!(!fields.raw_argument_deltas_authorized());
        let projection = fields
            .project_validated_completed_arguments(&json!({
                "query": "WBC",
                "private_context": {"credential": "never publish"}
            }))
            .unwrap();
        assert_eq!(
            projection.workflow_started_arguments(),
            Some(&json!({"query": "WBC"}))
        );
        assert_eq!(projection.standard_function_call_arguments(), None);

        let all = WorkflowToolPublicProjection::from_frozen_effective_policy(&frozen(
            true,
            ToolPublicArguments::All,
            None,
        ))
        .unwrap();
        assert!(all.call_authorized());
        assert!(all.standard_function_call_events_authorized());
        assert!(all.raw_argument_deltas_authorized());
        let projection = all
            .project_validated_completed_arguments(&json!({"query": "WBC"}))
            .unwrap();
        assert_eq!(
            projection.workflow_started_arguments(),
            Some(&json!({"query": "WBC"}))
        );
        assert_eq!(
            projection.standard_function_call_arguments(),
            Some(r#"{"query":"WBC"}"#)
        );
    }

    #[test]
    fn contradictory_or_noncanonical_frozen_policies_fail_closed() {
        for policy in [
            frozen(false, ToolPublicArguments::All, None),
            frozen(
                false,
                ToolPublicArguments::Fields(BTreeSet::from(["query".to_owned()])),
                None,
            ),
            frozen(
                false,
                ToolPublicArguments::Private,
                Some(normalized_schema(json!({}), &[])),
            ),
            frozen(true, ToolPublicArguments::Fields(BTreeSet::new()), None),
            json!({"call": true, "arguments": "private"}),
            json!({
                "call": true,
                "arguments": ["query", "query"],
                "result": null
            }),
            json!({
                "call": true,
                "arguments": "private",
                "result": null,
                "future": true
            }),
            frozen(true, ToolPublicArguments::Private, Some(json!(true))),
            frozen(
                true,
                ToolPublicArguments::Private,
                Some(json!({
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "$defs": {},
                    "type": "object"
                })),
            ),
            frozen(
                true,
                ToolPublicArguments::Private,
                Some(json!({
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "$defs": {},
                    "$dynamicRef": "#node",
                    "type": "object",
                    "additionalProperties": false
                })),
            ),
            frozen(
                true,
                ToolPublicArguments::Private,
                Some(normalized_schema(
                    json!({"nested": {"type": "object"}}),
                    &["nested"],
                )),
            ),
            frozen(
                true,
                ToolPublicArguments::Private,
                Some(normalized_schema(json!({"anything": {}}), &["anything"])),
            ),
            frozen(
                true,
                ToolPublicArguments::Private,
                Some(normalized_schema(
                    json!({"items": {"type": "array"}}),
                    &["items"],
                )),
            ),
        ] {
            let error =
                WorkflowToolPublicProjection::from_frozen_effective_policy(&policy).unwrap_err();
            assert_eq!(error.code(), "WORKFLOW_PUBLIC_RESULT_INVALID");
        }
    }

    #[test]
    fn field_projection_only_exposes_complete_selected_values_and_enforces_bounds() {
        let projection = WorkflowToolPublicProjection::from_frozen_effective_policy(&frozen(
            true,
            ToolPublicArguments::Fields(BTreeSet::from(["items".to_owned()])),
            None,
        ))
        .unwrap();

        assert!(projection
            .project_validated_completed_arguments(&json!(["not", "an", "object"]))
            .is_err());
        assert!(projection
            .project_validated_completed_arguments(&json!({
                "items": (0..4_096).collect::<Vec<_>>()
            }))
            .is_err());
        assert!(!projection.raw_argument_deltas_authorized());
    }

    #[test]
    fn private_results_are_not_inspected_and_public_json_is_schema_checked() {
        let private = WorkflowToolPublicProjection::from_frozen_effective_policy(&frozen(
            true,
            ToolPublicArguments::Private,
            None,
        ))
        .unwrap();
        assert!(private
            .project_validated_completed_result(
                "call_1",
                "lookup",
                &json!({"raw_secret": "ignored"})
            )
            .unwrap()
            .is_none());

        let public = WorkflowToolPublicProjection::from_frozen_effective_policy(&frozen(
            true,
            ToolPublicArguments::Private,
            Some(normalized_schema(
                json!({"indicator": {"type": "string"}}),
                &["indicator"],
            )),
        ))
        .unwrap();
        assert!(public.result_authorized());
        assert!(public
            .project_validated_completed_result("call_1", "lookup", &json!({"indicator": 7}))
            .is_err());
        let result = public
            .project_validated_completed_result("call_1", "lookup", &json!({"indicator": "WBC"}))
            .unwrap()
            .unwrap();
        assert_eq!(result.call_id(), "call_1");
        assert_eq!(result.tool_name(), "lookup");
        assert_eq!(result.content().len(), 1);
        assert_eq!(
            result.content()[0].json(),
            Some(&json!({"indicator": "WBC"}))
        );
    }

    #[test]
    fn explicit_typed_content_is_closed_bounded_and_binary_requires_artifact_ref() {
        let text_policy = WorkflowToolPublicProjection::from_frozen_effective_policy(&frozen(
            true,
            ToolPublicArguments::Private,
            Some(normalized_schema(
                json!({
                    "type": {"const": "output_text"},
                    "text": {"type": "string"}
                }),
                &["type", "text"],
            )),
        ))
        .unwrap();
        let text = text_policy
            .project_validated_completed_result(
                "call_text",
                "summarize",
                &json!({"type": "output_text", "text": "safe"}),
            )
            .unwrap()
            .unwrap();
        assert_eq!(text.content()[0].text(), Some("safe"));

        let image_policy = WorkflowToolPublicProjection::from_frozen_effective_policy(&frozen(
            true,
            ToolPublicArguments::Private,
            Some(normalized_schema(
                json!({
                    "type": {"const": "output_image"},
                    "artifact": {
                        "type": "object",
                        "properties": {
                            "artifact_id": {"type": "string"},
                            "content_hash": {"type": "string"},
                            "size_bytes": {"type": "integer"},
                            "media_type": {"type": "string"}
                        },
                        "required": ["artifact_id", "content_hash", "size_bytes", "media_type"],
                        "additionalProperties": false
                    },
                    "base64": {"type": "string"}
                }),
                &["type"],
            )),
        ))
        .unwrap();
        assert!(image_policy
            .project_validated_completed_result(
                "call_image",
                "render",
                &json!({"type": "output_image", "base64": "aGVsbG8="}),
            )
            .is_err());

        let image = image_policy
            .project_validated_completed_result(
                "call_image",
                "render",
                &json!({
                    "type": "output_image",
                    "artifact": {
                        "artifact_id": "artifact_image",
                        "content_hash": concat!(
                            "sha256:",
                            "0000000000000000000000000000000000000000000000000000000000000000"
                        ),
                        "size_bytes": 12,
                        "media_type": "image/png"
                    }
                }),
            )
            .unwrap()
            .unwrap();
        assert!(image.content()[0].artifact().is_some());
    }

    #[test]
    fn explicit_content_envelope_enforces_part_and_inline_limits() {
        let policy = WorkflowToolPublicProjection::from_frozen_effective_policy(&frozen(
            true,
            ToolPublicArguments::Private,
            Some(normalized_schema(
                json!({
                    "content": {
                        "type": "array",
                        "items": {
                            "oneOf": [
                                {
                                    "type": "object",
                                    "properties": {
                                        "type": {"const": "output_text"},
                                        "text": {"type": "string"}
                                    },
                                    "required": ["type", "text"],
                                    "additionalProperties": false
                                },
                                {
                                    "type": "object",
                                    "properties": {
                                        "type": {"const": "output_json"},
                                        "json": {
                                            "type": "object",
                                            "properties": {"status": {"type": "string"}},
                                            "required": ["status"],
                                            "additionalProperties": false
                                        }
                                    },
                                    "required": ["type", "json"],
                                    "additionalProperties": false
                                }
                            ]
                        }
                    }
                }),
                &["content"],
            )),
        ))
        .unwrap();
        let result = policy
            .project_validated_completed_result(
                "call_many",
                "compose",
                &json!({
                    "content": [
                        {"type": "output_text", "text": "safe"},
                        {"type": "output_json", "json": {"status": "ok"}}
                    ]
                }),
            )
            .unwrap()
            .unwrap();
        assert_eq!(result.content().len(), 2);

        let oversized = json!({
            "content": (0..129)
                .map(|index| json!({"type": "output_text", "text": index.to_string()}))
                .collect::<Vec<_>>()
        });
        assert!(policy
            .project_validated_completed_result("call_many", "compose", &oversized)
            .is_err());
    }
}
