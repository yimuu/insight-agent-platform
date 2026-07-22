use serde_json::Value;

#[derive(Clone, Debug)]
pub struct JsonSchemaValidator {
    document: Value,
    inner: jsonschema::Validator,
}

pub fn compile_schema(schema: &Value) -> Result<JsonSchemaValidator, String> {
    compile_schema_with_draft(schema, SupportedDraft::Draft7)
}

pub fn compile_schema_2020(schema: &Value) -> Result<JsonSchemaValidator, String> {
    compile_schema_with_draft(schema, SupportedDraft::Draft202012)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupportedDraft {
    Draft7,
    Draft202012,
}

fn compile_schema_with_draft(
    schema: &Value,
    draft: SupportedDraft,
) -> Result<JsonSchemaValidator, String> {
    validate_schema_policy(schema, draft)?;
    let jsonschema_draft = match draft {
        SupportedDraft::Draft7 => jsonschema::Draft::Draft7,
        SupportedDraft::Draft202012 => jsonschema::Draft::Draft202012,
    };
    let inner = jsonschema::options()
        .with_draft(jsonschema_draft)
        .build(schema)
        .map_err(|error| error.to_string())?;
    Ok(JsonSchemaValidator {
        document: schema.clone(),
        inner,
    })
}

impl JsonSchemaValidator {
    pub fn document(&self) -> &Value {
        &self.document
    }

    pub fn is_valid(&self, value: &Value) -> bool {
        self.inner.is_valid(value)
    }
}

fn validate_schema_policy(value: &Value, draft: SupportedDraft) -> Result<(), String> {
    match value {
        Value::Object(object) => {
            if let Some(schema_value) = object.get("$schema") {
                let schema_uri = schema_value
                    .as_str()
                    .ok_or_else(|| "$schema must be a string".to_string())?;
                validate_schema_uri(schema_uri, draft)?;
            }
            if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                validate_reference(reference)?;
            }
            for value in object.values() {
                validate_schema_policy(value, draft)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_schema_policy(value, draft)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn validate_schema_uri(uri: &str, draft: SupportedDraft) -> Result<(), String> {
    let accepted = match draft {
        SupportedDraft::Draft7 => matches!(
            uri,
            "http://json-schema.org/draft-07/schema#"
                | "https://json-schema.org/draft-07/schema#"
                | "http://json-schema.org/draft-07/schema"
                | "https://json-schema.org/draft-07/schema"
        ),
        SupportedDraft::Draft202012 => matches!(
            uri,
            "http://json-schema.org/draft/2020-12/schema"
                | "https://json-schema.org/draft/2020-12/schema"
                | "http://json-schema.org/draft/2020-12/schema#"
                | "https://json-schema.org/draft/2020-12/schema#"
        ),
    };
    if accepted {
        Ok(())
    } else {
        let expected = match draft {
            SupportedDraft::Draft7 => "Draft 7",
            SupportedDraft::Draft202012 => "Draft 2020-12",
        };
        Err(format!(
            "unsupported JSON Schema draft '{uri}'; only {expected} is supported"
        ))
    }
}

fn validate_reference(reference: &str) -> Result<(), String> {
    if reference.starts_with('#') {
        Ok(())
    } else {
        Err("external JSON Schema references are not supported".to_string())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{compile_schema, compile_schema_2020};

    #[test]
    fn validates_basic_object_schema() {
        let schema = json!({
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": {"type": "string", "minLength": 1}
            },
            "additionalProperties": false
        });
        let validator = compile_schema(&schema).unwrap();

        assert_eq!(validator.document(), &schema);
        assert!(validator.is_valid(&json!({"name": "demo"})));
        assert!(!validator.is_valid(&json!({})));
        assert!(!validator.is_valid(&json!({"name": "demo", "extra": true})));
    }

    #[test]
    fn missing_schema_uses_draft7_tuple_items_behavior() {
        let validator = compile_schema(&json!({
            "type": "array",
            "items": [{"type": "string"}],
            "additionalItems": false
        }))
        .unwrap();

        assert!(validator.is_valid(&json!(["ok"])));
        assert!(!validator.is_valid(&json!(["ok", "extra"])));
    }

    #[test]
    fn accepts_explicit_draft7_schema_uris() {
        for uri in [
            "http://json-schema.org/draft-07/schema#",
            "https://json-schema.org/draft-07/schema#",
        ] {
            let validator = compile_schema(&json!({
                "$schema": uri,
                "type": "object",
                "required": ["id"],
                "properties": {"id": {"type": "string"}}
            }))
            .unwrap();

            assert!(validator.is_valid(&json!({"id": "agent"})));
            assert!(!validator.is_valid(&json!({"id": 1})));
        }
    }

    #[test]
    fn rejects_non_draft7_schema_uri() {
        let error = compile_schema(&json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object"
        }))
        .unwrap_err();

        assert!(error.contains("unsupported JSON Schema draft"));
    }

    #[test]
    fn rejects_non_string_schema_uri() {
        let error = compile_schema(&json!({
            "$schema": 7,
            "type": "object"
        }))
        .unwrap_err();

        assert_eq!(error, "$schema must be a string");
    }

    #[test]
    fn allows_internal_refs() {
        let validator = compile_schema(&json!({
            "type": "object",
            "definitions": {
                "name": {"type": "string", "minLength": 1}
            },
            "required": ["name"],
            "properties": {
                "name": {"$ref": "#/definitions/name"}
            }
        }))
        .unwrap();

        assert!(validator.is_valid(&json!({"name": "alice"})));
        assert!(!validator.is_valid(&json!({"name": ""})));
    }

    #[test]
    fn rejects_external_refs_before_upstream_resolution() {
        for reference in [
            "https://example.invalid/schema.json",
            "http://example.invalid/schema.json",
            "file:///tmp/schema.json",
            "schemas/shared.json#/defs/name",
        ] {
            let error = compile_schema(&json!({"$ref": reference})).unwrap_err();
            assert!(error.contains("external JSON Schema references are not supported"));
        }
    }

    #[test]
    fn compiles_draft_2020_12_without_changing_the_general_draft7_helper() {
        let schema = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "array",
            "prefixItems": [{"type": "string"}],
            "items": false
        });

        let validator = compile_schema_2020(&schema).unwrap();
        assert!(validator.is_valid(&json!(["ok"])));
        assert!(!validator.is_valid(&json!(["ok", "extra"])));
        assert!(compile_schema(&schema).is_err());
    }

    #[test]
    fn draft_2020_12_rejects_other_declared_dialects_and_external_refs() {
        assert!(compile_schema_2020(&json!({
            "$schema": "https://json-schema.org/draft-07/schema#",
            "type": "object"
        }))
        .is_err());
        assert!(compile_schema_2020(&json!({"$ref": "https://example.invalid/schema"})).is_err());
    }
}
