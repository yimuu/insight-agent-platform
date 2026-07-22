//! Pure projection of a frozen first-class retrieval publication policy.
//!
//! This module has no worker, repository, or live-broker dependency. It never
//! derives public results from a provider/model output: callers must supply an
//! explicit `public_candidate`, and private policy does not inspect that value.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::{
    resource_policy::RetrievalPublicPolicy,
    schema::{compile_schema_2020, JsonSchemaValidator},
};

use super::{
    WorkflowPublicResultError, WorkflowRetrieval, WorkflowRetrievalResult,
    MAX_WORKFLOW_RETRIEVAL_RESULTS,
};

const MAX_FROZEN_RETRIEVAL_PUBLIC_POLICY_BYTES: usize = 256 * 1024;
const MAX_PUBLIC_RETRIEVAL_AGGREGATE_BYTES: usize = 1024 * 1024;
const MAX_PUBLIC_RETRIEVAL_ITEM_BYTES: usize = 128 * 1024;
const MAX_PUBLIC_RETRIEVAL_DEPTH: usize = 32;
const MAX_PUBLIC_RETRIEVAL_ITEM_VALUES: usize = 8_192;
const MAX_PUBLIC_RETRIEVAL_AGGREGATE_VALUES: usize = 65_536;
const MAX_FROZEN_QUERY_FIELD_BYTES: usize = 128;

/// Executable, fail-closed view of one frozen retrieval publication contract.
///
/// `frozen_query_field` comes from the same frozen descriptor binding as the
/// effective policy; it is not rediscovered from a current registry entry.
#[derive(Debug, Clone)]
pub struct WorkflowRetrievalPublicProjection {
    query: bool,
    query_field: String,
    result_validator: Option<JsonSchemaValidator>,
}

impl WorkflowRetrievalPublicProjection {
    /// Re-decodes and proves exact canonical linker evidence.
    ///
    /// Missing defaults, unknown members, non-normalized schemas, unsafe
    /// references, and open nested public objects all fail closed.
    pub fn from_frozen_effective_policy(
        frozen_policy: &Value,
        frozen_query_field: &str,
    ) -> Result<Self, WorkflowPublicResultError> {
        if !valid_query_field(frozen_query_field) {
            return Err(invalid_policy());
        }
        let encoded = serde_jcs::to_vec(frozen_policy).map_err(|_| invalid_policy())?;
        if encoded.len() > MAX_FROZEN_RETRIEVAL_PUBLIC_POLICY_BYTES {
            return Err(invalid_policy());
        }
        let policy = serde_json::from_value::<RetrievalPublicPolicy>(frozen_policy.clone())
            .map_err(|_| invalid_policy())?;
        let normalized = serde_json::to_value(&policy).map_err(|_| invalid_policy())?;
        if &normalized != frozen_policy {
            return Err(invalid_policy());
        }

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
            query: policy.query,
            query_field: frozen_query_field.to_owned(),
            result_validator,
        })
    }

    pub const fn query_authorized(&self) -> bool {
        self.query
    }

    pub const fn result_authorized(&self) -> bool {
        self.result_validator.is_some()
    }

    /// Builds the complete caller-visible retrieval projection.
    ///
    /// `validated_model_input` must be the closed, model-visible object before
    /// any server-only context is added. `public_candidate` is an explicit
    /// array whose entries are checked independently against the exact frozen
    /// public result schema and then decoded through the closed
    /// [`WorkflowRetrievalResult`] wire type.
    pub fn project_validated_completed(
        &self,
        retrieval_id: impl Into<String>,
        validated_model_input: &Value,
        public_candidate: Option<&Value>,
    ) -> Result<Option<WorkflowRetrieval>, WorkflowPublicResultError> {
        // This branch must precede all access to input/candidate. It is the
        // non-interference guarantee for a fully private descriptor policy.
        if !self.query && self.result_validator.is_none() {
            return Ok(None);
        }

        let query = if self.query {
            Some(
                validated_model_input
                    .as_object()
                    .and_then(|input| input.get(&self.query_field))
                    .and_then(Value::as_str)
                    .ok_or_else(invalid_input)?
                    .to_owned(),
            )
        } else {
            None
        };

        let results = match &self.result_validator {
            None => Vec::new(),
            Some(validator) => {
                let candidate = public_candidate.ok_or_else(invalid_result)?;
                let values = candidate.as_array().ok_or_else(invalid_result)?;
                if values.len() > MAX_WORKFLOW_RETRIEVAL_RESULTS {
                    return Err(invalid_result());
                }
                validate_public_json_bounds(
                    candidate,
                    MAX_PUBLIC_RETRIEVAL_AGGREGATE_BYTES,
                    MAX_PUBLIC_RETRIEVAL_DEPTH,
                    MAX_PUBLIC_RETRIEVAL_AGGREGATE_VALUES,
                )?;

                let mut observed_ids = BTreeSet::new();
                let mut results = Vec::with_capacity(values.len());
                for value in values {
                    if !validator.is_valid(value) {
                        return Err(invalid_result());
                    }
                    validate_public_json_bounds(
                        value,
                        MAX_PUBLIC_RETRIEVAL_ITEM_BYTES,
                        MAX_PUBLIC_RETRIEVAL_DEPTH,
                        MAX_PUBLIC_RETRIEVAL_ITEM_VALUES,
                    )?;
                    let result = serde_json::from_value::<WorkflowRetrievalResult>(value.clone())
                        .map_err(|_| invalid_result())?;
                    if !observed_ids.insert(result.id().to_owned()) {
                        return Err(invalid_result());
                    }
                    results.push(result);
                }
                results
            }
        };

        let retrieval =
            WorkflowRetrieval::new(retrieval_id, query, results).map_err(|_| invalid_result())?;
        let wire = serde_json::to_value(&retrieval).map_err(|_| invalid_result())?;
        validate_public_json_bounds(
            &wire,
            MAX_PUBLIC_RETRIEVAL_AGGREGATE_BYTES,
            MAX_PUBLIC_RETRIEVAL_DEPTH,
            MAX_PUBLIC_RETRIEVAL_AGGREGATE_VALUES,
        )?;
        Ok(Some(retrieval))
    }

    /// Revalidates a durable, already-public projection against the exact
    /// frozen policy used by the worker.
    ///
    /// This path intentionally has no access to model input or the executor's
    /// public candidate. It is used at the repository boundary and while
    /// rebuilding a terminal snapshot to prove that an immutable public row
    /// still has the authorized shape. Equality with the original query and
    /// candidate is established before the worker result is committed.
    pub fn validate_frozen_completed(
        &self,
        expected_retrieval_id: &str,
        retrieval: &WorkflowRetrieval,
    ) -> Result<(), WorkflowPublicResultError> {
        if self.query || self.result_validator.is_some() {
            if retrieval.retrieval_id() != expected_retrieval_id
                || retrieval.query().is_some() != self.query
                || (self.result_validator.is_none() && !retrieval.results().is_empty())
            {
                return Err(invalid_result());
            }
        } else {
            // A fully private policy must be represented by absence of a
            // projection, never by an empty public object.
            return Err(invalid_result());
        }

        let mut observed_ids = BTreeSet::new();
        if let Some(validator) = &self.result_validator {
            for result in retrieval.results() {
                let value = serde_json::to_value(result).map_err(|_| invalid_result())?;
                if !validator.is_valid(&value) || !observed_ids.insert(result.id().to_owned()) {
                    return Err(invalid_result());
                }
                validate_public_json_bounds(
                    &value,
                    MAX_PUBLIC_RETRIEVAL_ITEM_BYTES,
                    MAX_PUBLIC_RETRIEVAL_DEPTH,
                    MAX_PUBLIC_RETRIEVAL_ITEM_VALUES,
                )?;
            }
        }
        let wire = serde_json::to_value(retrieval).map_err(|_| invalid_result())?;
        validate_public_json_bounds(
            &wire,
            MAX_PUBLIC_RETRIEVAL_AGGREGATE_BYTES,
            MAX_PUBLIC_RETRIEVAL_DEPTH,
            MAX_PUBLIC_RETRIEVAL_AGGREGATE_VALUES,
        )
    }
}

fn valid_query_field(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_FROZEN_QUERY_FIELD_BYTES || !value.is_ascii() {
        return false;
    }
    let mut characters = value.chars();
    matches!(characters.next(), Some(first) if first.is_ascii_alphabetic() || first == '_')
        && characters
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
}

fn validate_public_json_bounds(
    value: &Value,
    max_bytes: usize,
    max_depth: usize,
    max_values: usize,
) -> Result<(), WorkflowPublicResultError> {
    let encoded = serde_jcs::to_vec(value).map_err(|_| invalid_result())?;
    if encoded.len() > max_bytes {
        return Err(invalid_result());
    }
    let mut stack = vec![(value, 0_usize)];
    let mut values = 0_usize;
    while let Some((current, depth)) = stack.pop() {
        values = values.saturating_add(1);
        if values > max_values || depth > max_depth {
            return Err(invalid_result());
        }
        match current {
            Value::Array(items) => {
                stack.extend(items.iter().map(|item| (item, depth.saturating_add(1))))
            }
            Value::Object(object) => {
                stack.extend(object.values().map(|item| (item, depth.saturating_add(1))))
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    Ok(())
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

    let mut observed = BTreeSet::new();
    closed_object_schema_facts(schema, schema, &mut observed) == (true, true)
        && safe_public_value_schema(schema, schema, &mut BTreeSet::new())
}

fn safe_public_value_schema(
    schema: &Value,
    root: &Value,
    observed_references: &mut BTreeSet<String>,
) -> bool {
    let Value::Object(object) = schema else {
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
        return serde_jcs::to_vec(value).is_ok();
    }
    if let Some(values) = object.get("enum") {
        return values.as_array().is_some_and(|values| {
            !values.is_empty() && values.iter().all(|value| serde_jcs::to_vec(value).is_ok())
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
    if object.contains_key("allOf") || !object.contains_key("type") {
        return false;
    }
    let types = match object.get("type") {
        Some(Value::String(kind)) => vec![kind.as_str()],
        Some(Value::Array(kinds)) => {
            let Some(kinds) = kinds.iter().map(Value::as_str).collect::<Option<Vec<_>>>() else {
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
                .is_none_or(|patterns| patterns.as_object().is_some_and(|p| p.is_empty()))
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

fn invalid_policy() -> WorkflowPublicResultError {
    WorkflowPublicResultError::new("frozen workflow retrieval public policy is invalid")
}

fn invalid_input() -> WorkflowPublicResultError {
    WorkflowPublicResultError::new("completed workflow retrieval public input is invalid")
}

fn invalid_result() -> WorkflowPublicResultError {
    WorkflowPublicResultError::new("completed workflow retrieval public result is invalid")
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use crate::resource_policy::RetrievalPublicPolicy;

    use super::{WorkflowRetrievalPublicProjection, MAX_WORKFLOW_RETRIEVAL_RESULTS};

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

    fn basic_result_schema() -> Value {
        normalized_schema(
            json!({
                "id": {"type": "string"},
                "title": {"type": "string"},
                "uri": {"type": "string"},
                "score": {"type": "number"},
                "snippet": {"type": "string"},
                "metadata": {
                    "type": "object",
                    "properties": {"source": {"type": "string"}},
                    "additionalProperties": false
                }
            }),
            &["id"],
        )
    }

    fn frozen(query: bool, result_schema: Option<Value>) -> Value {
        serde_json::to_value(RetrievalPublicPolicy {
            query,
            result_schema,
        })
        .unwrap()
    }

    #[test]
    fn fully_private_policy_does_not_inspect_input_or_candidate() {
        let projection = WorkflowRetrievalPublicProjection::from_frozen_effective_policy(
            &frozen(false, None),
            "query",
        )
        .unwrap();
        assert!(!projection.query_authorized());
        assert!(!projection.result_authorized());
        let candidate = json!("not even a result array");
        assert!(projection
            .project_validated_completed("ret_1", &json!("not an input object"), Some(&candidate),)
            .unwrap()
            .is_none());
    }

    #[test]
    fn query_and_result_authorization_are_independent() {
        let query_only = WorkflowRetrievalPublicProjection::from_frozen_effective_policy(
            &frozen(true, None),
            "query",
        )
        .unwrap();
        let retrieval = query_only
            .project_validated_completed(
                "ret_query",
                &json!({"query": "WBC"}),
                Some(&json!("private result is ignored")),
            )
            .unwrap()
            .unwrap();
        assert_eq!(retrieval.query(), Some("WBC"));
        assert!(retrieval.results().is_empty());

        let result_only = WorkflowRetrievalPublicProjection::from_frozen_effective_policy(
            &frozen(false, Some(basic_result_schema())),
            "query",
        )
        .unwrap();
        let retrieval = result_only
            .project_validated_completed(
                "ret_result",
                &json!("private input is ignored"),
                Some(&json!([{"id": "doc_1", "metadata": {"source": "kb"}}])),
            )
            .unwrap()
            .unwrap();
        assert_eq!(retrieval.query(), None);
        assert_eq!(retrieval.results()[0].id(), "doc_1");
    }

    #[test]
    fn both_authorizations_project_only_explicit_candidate() {
        let projection = WorkflowRetrievalPublicProjection::from_frozen_effective_policy(
            &frozen(true, Some(basic_result_schema())),
            "question",
        )
        .unwrap();
        let candidate = json!([{
            "id": "doc_1",
            "title": "Public title",
            "score": 0.92,
            "metadata": {"source": "medical-kb"}
        }]);
        let retrieval = projection
            .project_validated_completed(
                "ret_1",
                &json!({"question": "WBC", "credential": "private"}),
                Some(&candidate),
            )
            .unwrap()
            .unwrap();
        assert_eq!(retrieval.query(), Some("WBC"));
        assert_eq!(retrieval.results()[0].title(), Some("Public title"));
        let encoded = serde_json::to_string(&retrieval).unwrap();
        assert!(!encoded.contains("raw_secret"));
        assert!(!encoded.contains("credential"));
    }

    #[test]
    fn contradictory_noncanonical_or_unsafe_frozen_contracts_fail_closed() {
        for policy in [
            json!({"query": false}),
            json!({"query": false, "result": null, "future": true}),
            frozen(false, Some(json!(true))),
            frozen(
                false,
                Some(json!({
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "$defs": {},
                    "type": "object",
                    "properties": {"metadata": {"type": "object"}},
                    "additionalProperties": false
                })),
            ),
            frozen(
                false,
                Some(json!({
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "$defs": {},
                    "$ref": "https://example.invalid/public.json"
                })),
            ),
        ] {
            let error =
                WorkflowRetrievalPublicProjection::from_frozen_effective_policy(&policy, "query")
                    .unwrap_err();
            assert_eq!(error.code(), "WORKFLOW_PUBLIC_RESULT_INVALID");
        }
        assert!(
            WorkflowRetrievalPublicProjection::from_frozen_effective_policy(
                &frozen(false, None),
                "not a field",
            )
            .is_err()
        );
    }

    #[test]
    fn schema_and_closed_wire_reject_missing_candidate_wrong_types_and_unknown_fields() {
        let projection = WorkflowRetrievalPublicProjection::from_frozen_effective_policy(
            &frozen(false, Some(basic_result_schema())),
            "query",
        )
        .unwrap();
        assert!(projection
            .project_validated_completed("ret_1", &json!({}), None)
            .is_err());
        for candidate in [
            json!({"id": "not-an-array"}),
            json!([{"id": 7}]),
            json!([{"id": "doc_1", "raw_document": "private"}]),
            json!([{"id": "doc_1", "metadata": {"unknown": true}}]),
        ] {
            assert!(projection
                .project_validated_completed("ret_1", &json!({}), Some(&candidate))
                .is_err());
        }
    }

    #[test]
    fn exact_result_limit_is_accepted_and_n_plus_one_is_rejected() {
        let projection = WorkflowRetrievalPublicProjection::from_frozen_effective_policy(
            &frozen(false, Some(basic_result_schema())),
            "query",
        )
        .unwrap();
        let exact = (0..MAX_WORKFLOW_RETRIEVAL_RESULTS)
            .map(|index| json!({"id": format!("doc_{index}")}))
            .collect::<Vec<_>>();
        let retrieval = projection
            .project_validated_completed("ret_n", &json!({}), Some(&json!(exact)))
            .unwrap()
            .unwrap();
        assert_eq!(retrieval.results().len(), MAX_WORKFLOW_RETRIEVAL_RESULTS);

        let too_many = (0..=MAX_WORKFLOW_RETRIEVAL_RESULTS)
            .map(|index| json!({"id": format!("doc_{index}")}))
            .collect::<Vec<_>>();
        assert!(projection
            .project_validated_completed("ret_n1", &json!({}), Some(&json!(too_many)))
            .is_err());
    }

    #[test]
    fn duplicate_result_identity_and_per_field_bounds_fail_closed() {
        let projection = WorkflowRetrievalPublicProjection::from_frozen_effective_policy(
            &frozen(true, Some(basic_result_schema())),
            "query",
        )
        .unwrap();
        assert!(projection
            .project_validated_completed(
                "ret_dup",
                &json!({"query": "WBC"}),
                Some(&json!([{"id": "doc_1"}, {"id": "doc_1"}])),
            )
            .is_err());
        assert!(projection
            .project_validated_completed(
                "ret_query",
                &json!({"query": "x".repeat(16 * 1024 + 1)}),
                Some(&json!([])),
            )
            .is_err());
        assert!(projection
            .project_validated_completed(
                "ret_title",
                &json!({"query": "WBC"}),
                Some(&json!([{"id": "doc_1", "title": "x".repeat(4 * 1024 + 1)}])),
            )
            .is_err());
    }

    #[test]
    fn aggregate_jcs_is_bounded_to_one_mibibyte() {
        let projection = WorkflowRetrievalPublicProjection::from_frozen_effective_policy(
            &frozen(false, Some(basic_result_schema())),
            "query",
        )
        .unwrap();
        let oversized = (0..17)
            .map(|index| {
                json!({
                    "id": format!("doc_{index}"),
                    "snippet": "x".repeat(64 * 1024)
                })
            })
            .collect::<Vec<_>>();
        assert!(projection
            .project_validated_completed("ret_large", &json!({}), Some(&json!(oversized)))
            .is_err());
    }

    #[test]
    fn deeply_nested_public_metadata_exceeding_the_structural_limit_is_rejected() {
        let mut nested_schema = json!({"type": "string"});
        let mut nested_value = json!("leaf");
        for _ in 0..34 {
            nested_schema = json!({
                "type": "object",
                "properties": {"child": nested_schema},
                "required": ["child"],
                "additionalProperties": false
            });
            nested_value = json!({"child": nested_value});
        }
        let policy = frozen(
            false,
            Some(normalized_schema(
                json!({
                    "id": {"type": "string"},
                    "metadata": nested_schema
                }),
                &["id", "metadata"],
            )),
        );
        let projection =
            WorkflowRetrievalPublicProjection::from_frozen_effective_policy(&policy, "query")
                .unwrap();
        let candidate = json!([{"id": "doc_deep", "metadata": nested_value}]);
        assert!(projection
            .project_validated_completed("ret_deep", &json!({}), Some(&candidate))
            .is_err());
    }

    #[test]
    fn artifact_is_a_typed_closed_reference_never_inline_binary() {
        let artifact_schema = normalized_schema(
            json!({
                "id": {"type": "string"},
                "artifact": {
                    "type": "object",
                    "properties": {
                        "artifact_id": {"type": "string"},
                        "content_hash": {"type": "string"},
                        "size_bytes": {"type": "integer", "minimum": 0},
                        "media_type": {"type": ["string", "null"]}
                    },
                    "required": ["artifact_id", "content_hash", "size_bytes", "media_type"],
                    "additionalProperties": false
                }
            }),
            &["id", "artifact"],
        );
        let projection = WorkflowRetrievalPublicProjection::from_frozen_effective_policy(
            &frozen(false, Some(artifact_schema)),
            "query",
        )
        .unwrap();
        for invalid in [
            json!([{"id": "doc_1", "artifact": "base64:aGVsbG8="}]),
            json!([{
                "id": "doc_1",
                "artifact": {
                    "artifact_id": "artifact_1",
                    "content_hash": "not-a-content-hash",
                    "size_bytes": 5,
                    "media_type": "text/plain"
                }
            }]),
            json!([{
                "id": "doc_1",
                "artifact": {
                    "artifact_id": "artifact_1",
                    "content_hash": concat!("sha256:", "0000000000000000000000000000000000000000000000000000000000000000"),
                    "size_bytes": 5,
                    "media_type": "text/plain",
                    "base64": "aGVsbG8="
                }
            }]),
        ] {
            assert!(projection
                .project_validated_completed("ret_artifact", &json!({}), Some(&invalid))
                .is_err());
        }

        let valid = json!([{
            "id": "doc_1",
            "artifact": {
                "artifact_id": "artifact_1",
                "content_hash": concat!("sha256:", "0000000000000000000000000000000000000000000000000000000000000000"),
                "size_bytes": 5,
                "media_type": "text/plain"
            }
        }]);
        let retrieval = projection
            .project_validated_completed("ret_artifact", &json!({}), Some(&valid))
            .unwrap()
            .unwrap();
        assert_eq!(retrieval.results()[0].artifact().unwrap().size_bytes(), 5);
    }
}
