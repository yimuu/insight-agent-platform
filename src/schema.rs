use serde_json::Value;

#[derive(Clone, Debug)]
pub struct JsonSchemaValidator {
    inner: jsonschema::Validator,
}

pub fn compile_schema(schema: &Value) -> Result<JsonSchemaValidator, String> {
    validate_schema_policy(schema)?;
    let inner = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft7)
        .build(schema)
        .map_err(|error| error.to_string())?;
    Ok(JsonSchemaValidator { inner })
}

impl JsonSchemaValidator {
    pub fn is_valid(&self, value: &Value) -> bool {
        self.inner.is_valid(value)
    }
}

fn validate_schema_policy(value: &Value) -> Result<(), String> {
    match value {
        Value::Object(object) => {
            if let Some(schema_uri) = object.get("$schema").and_then(Value::as_str) {
                validate_schema_uri(schema_uri)?;
            }
            if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                validate_reference(reference)?;
            }
            for value in object.values() {
                validate_schema_policy(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_schema_policy(value)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn validate_schema_uri(uri: &str) -> Result<(), String> {
    match uri {
        "http://json-schema.org/draft-07/schema#" | "https://json-schema.org/draft-07/schema#" => {
            Ok(())
        }
        _ => Err(format!(
            "unsupported JSON Schema draft '{uri}'; only Draft 7 is supported"
        )),
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

    use super::compile_schema;

    #[test]
    fn validates_basic_object_schema() {
        let validator = compile_schema(&json!({
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": {"type": "string", "minLength": 1}
            },
            "additionalProperties": false
        }))
        .unwrap();

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
}
