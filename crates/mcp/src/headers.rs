use std::collections::{BTreeMap, BTreeSet};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::Value;

const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;
const MAX_HEADER_PLANS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ToolHeader {
    suffix: String,
    path: Vec<String>,
    primitive: Primitive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Primitive {
    String,
    Integer,
    Boolean,
}

/// Validated extraction plan for `x-mcp-header` annotations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolHeaderPlan {
    headers: Vec<ToolHeader>,
}

impl ToolHeaderPlan {
    pub fn from_schema(schema: &Value) -> Result<Self, ToolHeaderError> {
        if !schema.is_object() {
            return Err(ToolHeaderError::SchemaRoot);
        }
        let mut headers = Vec::new();
        let mut names = BTreeSet::new();
        walk_schema(schema, &mut Vec::new(), false, &mut headers, &mut names)?;
        if headers.len() > MAX_HEADER_PLANS {
            return Err(ToolHeaderError::TooMany);
        }
        Ok(Self { headers })
    }

    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    /// Extracts already-validated primitive values from call arguments.
    pub fn extract(&self, arguments: &Value) -> Result<BTreeMap<String, String>, ToolHeaderError> {
        let object = arguments
            .as_object()
            .ok_or(ToolHeaderError::ArgumentsNotObject)?;
        let mut output = BTreeMap::new();
        for header in &self.headers {
            let mut current = object.get(&header.path[0]);
            for segment in &header.path[1..] {
                current = current
                    .and_then(Value::as_object)
                    .and_then(|object| object.get(segment));
            }
            let Some(value) = current else {
                continue;
            };
            if value.is_null() {
                continue;
            }
            let value = match header.primitive {
                Primitive::String => value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or(ToolHeaderError::ArgumentType)?,
                Primitive::Integer => {
                    let value = value.as_i64().ok_or(ToolHeaderError::ArgumentType)?;
                    if !(-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&value) {
                        return Err(ToolHeaderError::UnsafeInteger);
                    }
                    value.to_string()
                }
                Primitive::Boolean => value
                    .as_bool()
                    .map(|value| value.to_string())
                    .ok_or(ToolHeaderError::ArgumentType)?,
            };
            output.insert(
                format!("Mcp-Param-{}", header.suffix),
                encode_header_value(&value),
            );
        }
        Ok(output)
    }
}

fn walk_schema(
    schema: &Value,
    path: &mut Vec<String>,
    annotation_allowed: bool,
    headers: &mut Vec<ToolHeader>,
    names: &mut BTreeSet<String>,
) -> Result<(), ToolHeaderError> {
    let object = schema.as_object().ok_or(ToolHeaderError::PropertySchema)?;
    if let Some(annotation) = object.get("x-mcp-header") {
        if !annotation_allowed || path.is_empty() {
            return Err(ToolHeaderError::UnreachableAnnotation);
        }
        let suffix = annotation
            .as_str()
            .filter(|value| valid_token(value))
            .ok_or(ToolHeaderError::HeaderName)?;
        if !names.insert(suffix.to_ascii_lowercase()) {
            return Err(ToolHeaderError::DuplicateHeaderName);
        }
        let primitive = match object.get("type").and_then(Value::as_str) {
            Some("string") => Primitive::String,
            Some("integer") => Primitive::Integer,
            Some("boolean") => Primitive::Boolean,
            _ => return Err(ToolHeaderError::NonPrimitive),
        };
        headers.push(ToolHeader {
            suffix: suffix.to_owned(),
            path: path.clone(),
            primitive,
        });
    }

    for (keyword, value) in object {
        if keyword == "x-mcp-header" {
            continue;
        }
        if keyword == "properties" {
            let properties = value.as_object().ok_or(ToolHeaderError::Properties)?;
            for (name, child) in properties {
                path.push(name.clone());
                walk_schema(child, path, true, headers, names)?;
                path.pop();
            }
        } else if contains_annotation(value) {
            return Err(ToolHeaderError::UnreachableAnnotation);
        }
    }
    Ok(())
}

fn contains_annotation(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object.contains_key("x-mcp-header") || object.values().any(contains_annotation)
        }
        Value::Array(values) => values.iter().any(contains_annotation),
        _ => false,
    }
}

fn valid_token(value: &str) -> bool {
    !value.is_empty()
        && value.is_ascii()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

/// Encodes unsafe HTTP field values using the MCP Base64 sentinel.
pub fn encode_header_value(value: &str) -> String {
    let sentinel = value.starts_with("=?base64?") && value.ends_with("?=");
    let safe = !value.is_empty()
        && value.trim_matches([' ', '\t']) == value
        && value
            .bytes()
            .all(|byte| byte == b'\t' || (0x20..=0x7e).contains(&byte));
    if safe && !sentinel {
        value.to_owned()
    } else {
        format!("=?base64?{}?=", STANDARD.encode(value.as_bytes()))
    }
}

/// Decodes the MCP Base64 sentinel used by mirrored name and parameter
/// headers. Plain visible ASCII values are returned unchanged.
pub fn decode_header_value(value: &str) -> Result<String, ToolHeaderError> {
    if let Some(encoded) = value
        .strip_prefix("=?base64?")
        .and_then(|value| value.strip_suffix("?="))
    {
        let bytes = STANDARD
            .decode(encoded)
            .map_err(|_| ToolHeaderError::HeaderValue)?;
        let decoded = String::from_utf8(bytes).map_err(|_| ToolHeaderError::HeaderValue)?;
        if decoded.is_empty() || decoded.chars().any(char::is_control) {
            return Err(ToolHeaderError::HeaderValue);
        }
        return Ok(decoded);
    }
    if value.is_empty()
        || value.trim_matches([' ', '\t']) != value
        || !value
            .bytes()
            .all(|byte| byte == b'\t' || (0x20..=0x7e).contains(&byte))
    {
        return Err(ToolHeaderError::HeaderValue);
    }
    Ok(value.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolHeaderError {
    SchemaRoot,
    PropertySchema,
    Properties,
    UnreachableAnnotation,
    HeaderName,
    HeaderValue,
    DuplicateHeaderName,
    NonPrimitive,
    TooMany,
    ArgumentsNotObject,
    ArgumentType,
    UnsafeInteger,
}

impl std::fmt::Display for ToolHeaderError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("invalid MCP tool header contract")
    }
}

impl std::error::Error for ToolHeaderError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidToolHeader {
    pub tool_name: String,
    pub reason: ToolHeaderError,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolHeaderRejection<T> {
    pub accepted: Vec<T>,
    pub rejected: Vec<InvalidToolHeader>,
}

impl<T> ToolHeaderRejection<T> {
    pub fn filter_by(
        tools: impl IntoIterator<Item = T>,
        name: impl Fn(&T) -> &str,
        schema: impl Fn(&T) -> &Value,
    ) -> Self {
        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        for tool in tools {
            match ToolHeaderPlan::from_schema(schema(&tool)) {
                Ok(_) => accepted.push(tool),
                Err(reason) => rejected.push(InvalidToolHeader {
                    tool_name: name(&tool).to_owned(),
                    reason,
                }),
            }
        }
        Self { accepted, rejected }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn extracts_nested_primitive_headers_and_encodes_unsafe_values() {
        let plan = ToolHeaderPlan::from_schema(&json!({
            "type":"object",
            "properties":{
                "region":{"type":"string","x-mcp-header":"Region"},
                "routing":{"type":"object","properties":{
                    "attempt":{"type":"integer","x-mcp-header":"Attempt"},
                    "enabled":{"type":"boolean","x-mcp-header":"Enabled"}
                }}
            }
        }))
        .unwrap();
        let headers = plan
            .extract(&json!({
                "region":"Hello, 世界",
                "routing":{"attempt":42,"enabled":true}
            }))
            .unwrap();
        assert_eq!(
            headers["Mcp-Param-Region"],
            "=?base64?SGVsbG8sIOS4lueVjA==?="
        );
        assert_eq!(headers["Mcp-Param-Attempt"], "42");
        assert_eq!(headers["Mcp-Param-Enabled"], "true");
    }

    #[test]
    fn rejects_unreachable_duplicate_and_non_primitive_annotations() {
        for schema in [
            json!({"type":"object","items":{"x-mcp-header":"Bad","type":"string"}}),
            json!({"type":"object","properties":{
                "a":{"type":"string","x-mcp-header":"Route"},
                "b":{"type":"string","x-mcp-header":"route"}
            }}),
            json!({"type":"object","properties":{
                "a":{"type":"number","x-mcp-header":"Route"}
            }}),
            json!({"type":"object","oneOf":[{
                "properties":{"a":{"type":"string","x-mcp-header":"Route"}}
            }]}),
        ] {
            assert!(ToolHeaderPlan::from_schema(&schema).is_err());
        }
    }

    #[test]
    fn sentinel_and_whitespace_are_encoded() {
        assert_eq!(encode_header_value("safe"), "safe");
        assert_eq!(encode_header_value(" padded "), "=?base64?IHBhZGRlZCA=?=");
        assert_eq!(
            encode_header_value("=?base64?literal?="),
            "=?base64?PT9iYXNlNjQ/bGl0ZXJhbD89?="
        );
    }

    #[test]
    fn mirrored_header_values_round_trip_and_reject_invalid_sentinels() {
        for value in ["plain", " leading", "你好"] {
            assert_eq!(
                decode_header_value(&encode_header_value(value)).unwrap(),
                value
            );
        }
        assert!(decode_header_value("=?base64?***?=").is_err());
        assert!(decode_header_value(" line").is_err());
    }
}
