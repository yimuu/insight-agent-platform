use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use serde_json::Value;

use super::{schema::compile_contract_schema, value::Identifier};

pub const INPUT_NORMALIZATION_INVALID: &str = "VNEXT_INPUT_NORMALIZATION_INVALID";
pub const INPUT_VALUE_INVALID: &str = "VNEXT_INPUT_VALUE_INVALID";

#[derive(Debug, Clone, PartialEq)]
enum MissingPolicy {
    Default(Value),
    Null,
}

/// A compile-time-derived, idempotent materializer for missing top-level input
/// properties. It never rewrites a value supplied by the caller.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct InputNormalizer {
    policies: BTreeMap<String, MissingPolicy>,
}

/// The authored input schema and the schema used after missing values have
/// been materialized. The latter differs only by making materializable fields
/// required.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledInputNormalization {
    pub normalizer: InputNormalizer,
    pub normalized_schema: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputNormalizationError {
    code: &'static str,
    message: &'static str,
}

impl InputNormalizationError {
    fn contract() -> Self {
        Self {
            code: INPUT_NORMALIZATION_INVALID,
            message: "input normalization contract is invalid",
        }
    }

    fn value() -> Self {
        Self {
            code: INPUT_VALUE_INVALID,
            message: "input value cannot be normalized",
        }
    }

    pub fn code(self) -> &'static str {
        self.code
    }

    pub fn message(self) -> &'static str {
        self.message
    }
}

impl fmt::Display for InputNormalizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl Error for InputNormalizationError {}

impl InputNormalizer {
    /// Derives missing-value policies from an authored top-level object
    /// contract. A property default wins; otherwise an optional property is
    /// materialized as null only when its resolved schema explicitly admits
    /// null.
    pub fn compile(
        definitions: &BTreeMap<Identifier, Value>,
        schema: &Value,
    ) -> Result<CompiledInputNormalization, InputNormalizationError> {
        let object = schema
            .as_object()
            .ok_or_else(InputNormalizationError::contract)?;
        if object.get("type").and_then(Value::as_str) != Some("object") {
            return Err(InputNormalizationError::contract());
        }
        let properties = match object.get("properties") {
            Some(value) => value
                .as_object()
                .ok_or_else(InputNormalizationError::contract)?,
            None => {
                return Ok(CompiledInputNormalization {
                    normalizer: Self::default(),
                    normalized_schema: schema.clone(),
                });
            }
        };
        let mut required = match object.get("required") {
            Some(Value::Array(values)) => values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_string)
                        .ok_or_else(InputNormalizationError::contract)
                })
                .collect::<Result<BTreeSet<_>, _>>()?,
            Some(_) => return Err(InputNormalizationError::contract()),
            None => BTreeSet::new(),
        };

        let mut policies = BTreeMap::new();
        for (name, property_schema) in properties {
            let contract = compile_contract_schema(definitions, property_schema)
                .map_err(|_| InputNormalizationError::contract())?;
            let policy = if let Some(default) = property_schema.get("default") {
                if !contract.validator().is_valid(default) {
                    return Err(InputNormalizationError::contract());
                }
                Some(MissingPolicy::Default(default.clone()))
            } else if !required.contains(name) && explicitly_allows_null(contract.expanded_schema())
            {
                Some(MissingPolicy::Null)
            } else {
                None
            };
            if let Some(policy) = policy {
                required.insert(name.clone());
                policies.insert(name.clone(), policy);
            }
        }

        let mut normalized_schema = schema.clone();
        if !policies.is_empty() {
            normalized_schema
                .as_object_mut()
                .expect("top-level input object was checked")
                .insert(
                    "required".to_string(),
                    Value::Array(required.into_iter().map(Value::String).collect()),
                );
        }
        Ok(CompiledInputNormalization {
            normalizer: Self { policies },
            normalized_schema,
        })
    }

    pub fn normalize(&self, mut input: Value) -> Result<Value, InputNormalizationError> {
        let object = input
            .as_object_mut()
            .ok_or_else(InputNormalizationError::value)?;
        for (name, policy) in &self.policies {
            if object.contains_key(name) {
                continue;
            }
            let value = match policy {
                MissingPolicy::Default(value) => value.clone(),
                MissingPolicy::Null => Value::Null,
            };
            object.insert(name.clone(), value);
        }
        Ok(input)
    }

    pub fn is_noop(&self) -> bool {
        self.policies.is_empty()
    }
}

fn explicitly_allows_null(schema: &Value) -> bool {
    let Some(object) = schema.as_object() else {
        return false;
    };
    let typed_null = match object.get("type") {
        Some(Value::String(value)) => value == "null",
        Some(Value::Array(values)) => values.iter().any(|value| value.as_str() == Some("null")),
        _ => false,
    };
    typed_null
        || object.get("const").is_some_and(Value::is_null)
        || object
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(Value::is_null))
        || ["oneOf", "anyOf"].into_iter().any(|keyword| {
            object
                .get(keyword)
                .and_then(Value::as_array)
                .is_some_and(|variants| variants.iter().any(explicitly_allows_null))
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{InputNormalizer, INPUT_VALUE_INVALID};

    fn compile(schema: serde_json::Value) -> super::CompiledInputNormalization {
        InputNormalizer::compile(&BTreeMap::new(), &schema).unwrap()
    }

    #[test]
    fn materializes_defaults_nullable_optional_fields_and_required_set() {
        let compiled = compile(json!({
            "type":"object",
            "required":["question"],
            "properties":{
                "question":{"type":"string"},
                "messages":{"type":"array","items":{"type":"string"},"default":[]},
                "image_url":{"oneOf":[{"type":"string"},{"type":"null"}]},
                "optional_text":{"type":"string"}
            },
            "additionalProperties":false
        }));

        assert_eq!(
            compiled.normalized_schema["required"],
            json!(["image_url", "messages", "question"])
        );
        assert_eq!(
            compiled
                .normalizer
                .normalize(json!({"question":"hello"}))
                .unwrap(),
            json!({"question":"hello","messages":[],"image_url":null})
        );
    }

    #[test]
    fn preserves_explicit_values_and_is_idempotent() {
        let compiled = compile(json!({
            "type":"object",
            "properties":{
                "messages":{"type":"array","items":{"type":"string"},"default":[]},
                "image_url":{"type":["string","null"]}
            },
            "additionalProperties":false
        }));
        let explicit = json!({"messages":["kept"],"image_url":"https://example.test/a.png"});
        let once = compiled.normalizer.normalize(explicit.clone()).unwrap();
        let twice = compiled.normalizer.normalize(once.clone()).unwrap();
        assert_eq!(once, explicit);
        assert_eq!(twice, once);
    }

    #[test]
    fn rejects_non_object_values_and_invalid_defaults() {
        let compiled = compile(json!({"type":"object","properties":{}}));
        let error = compiled.normalizer.normalize(json!([])).unwrap_err();
        assert_eq!(error.code(), INPUT_VALUE_INVALID);

        assert!(InputNormalizer::compile(
            &BTreeMap::new(),
            &json!({
                "type":"object",
                "properties":{"messages":{"type":"array","default":"not-an-array"}}
            })
        )
        .is_err());
    }
}
