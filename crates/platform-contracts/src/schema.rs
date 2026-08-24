use crate::{canonical_digest, Sha256Digest};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

pub const CLOSED_SCHEMA_PROFILE_ID: &str = "insight.closed-json-schema/1";
pub const MCP_FORM_SCHEMA_PROFILE_ID: &str = "mcp.form-json-schema/2025-11-25";
pub const CLOSED_SCHEMA_DOCUMENT_VERSION: u32 = 1;
pub const MAX_CLOSED_SCHEMA_BYTES: usize = 262_144;

/// Immutable validation snapshot for the platform closed JSON Schema profile.
///
/// A digest without this document is not enough to validate a value at a trust boundary.  The
/// complete document is therefore stored in a published ResourceVersion while downstream durable
/// admissions may freeze only `canonical_digest` after resolving the exact revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosedJsonSchema {
    pub schema_version: u32,
    pub profile: String,
    pub schema: Value,
    pub canonical_digest: Sha256Digest,
}

impl ClosedJsonSchema {
    pub fn build(schema: Value) -> Result<Self, SchemaProfileError> {
        validate_closed_schema_document_size(&schema)?;
        validate_closed_schema(&schema)?;
        Ok(Self {
            schema_version: CLOSED_SCHEMA_DOCUMENT_VERSION,
            profile: CLOSED_SCHEMA_PROFILE_ID.to_owned(),
            canonical_digest: schema_digest(&schema)?,
            schema,
        })
    }

    pub fn validate(&self) -> Result<(), SchemaProfileError> {
        if self.schema_version != CLOSED_SCHEMA_DOCUMENT_VERSION
            || self.profile != CLOSED_SCHEMA_PROFILE_ID
        {
            return Err(SchemaProfileError::new(
                "$",
                "closed_schema_binding",
                "closed schema version or profile is invalid",
            ));
        }
        validate_closed_schema_document_size(&self.schema)?;
        if schema_digest(&self.schema)? != self.canonical_digest {
            return Err(SchemaProfileError::new(
                "$",
                "closed_schema_digest",
                "closed schema canonical digest is invalid",
            ));
        }
        validate_closed_schema(&self.schema)
    }

    pub fn validate_instance(&self, value: &Value) -> Result<(), SchemaProfileError> {
        self.validate()?;
        let nominal_schema = self
            .schema
            .get("$ref")
            .and_then(Value::as_str)
            .and_then(crate::nominal::schema_for_pinned_nominal_reference);
        let validation_schema = nominal_schema.as_ref().unwrap_or(&self.schema);
        let validator = jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .build(validation_schema)
            .map_err(|_| {
                SchemaProfileError::new(
                    "$",
                    "closed_schema_compile",
                    "closed schema cannot be compiled by the local validator",
                )
            })?;
        if validator.is_valid(value) {
            Ok(())
        } else {
            Err(SchemaProfileError::new(
                "$",
                "closed_schema_instance",
                "value does not satisfy the exact closed schema",
            ))
        }
    }
}

/// Kept as a domain-neutral spelling for Model and other internal consumers.
pub type ClosedSchemaDocument = ClosedJsonSchema;

fn validate_closed_schema_document_size(schema: &Value) -> Result<(), SchemaProfileError> {
    let bytes = serde_json::to_vec(schema)
        .map_err(|_| {
            SchemaProfileError::new(
                "$",
                "closed_schema_serialization",
                "closed schema cannot be serialized",
            )
        })?
        .len();
    if bytes > MAX_CLOSED_SCHEMA_BYTES {
        return Err(SchemaProfileError::new(
            "$",
            "closed_schema_size",
            "closed schema exceeds its hard byte limit",
        ));
    }
    Ok(())
}

/// Canonical, closed schema carried by a human Interaction.
///
/// Keeping the document with its digest lets the public Task projection render the exact form
/// that the backend requested while the digest remains the compact binding used by RunValue and
/// wake contracts. Backend-provided presentation text is deliberately not part of this type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteractionSchemaDocument {
    pub schema_version: u32,
    pub profile: String,
    pub schema: Value,
    pub canonical_digest: Sha256Digest,
}

impl InteractionSchemaDocument {
    pub fn build(schema: Value) -> Result<Self, SchemaProfileError> {
        validate_closed_schema(&schema)?;
        let canonical_digest = schema_digest(&schema)?;
        Ok(Self {
            schema_version: 1,
            profile: CLOSED_SCHEMA_PROFILE_ID.to_owned(),
            schema,
            canonical_digest,
        })
    }

    pub fn validate(&self) -> Result<(), SchemaProfileError> {
        if self.schema_version != 1 || schema_digest(&self.schema)? != self.canonical_digest {
            return Err(SchemaProfileError::new(
                "$",
                "interaction_schema_binding",
                "interaction schema metadata or canonical digest is invalid",
            ));
        }
        match self.profile.as_str() {
            CLOSED_SCHEMA_PROFILE_ID => validate_closed_schema(&self.schema),
            MCP_FORM_SCHEMA_PROFILE_ID => validate_mcp_form_schema(&self.schema),
            _ => Err(SchemaProfileError::new(
                "$/profile",
                "interaction_schema_profile",
                "interaction schema profile is not supported",
            )),
        }
    }

    pub fn build_mcp_form(schema: Value) -> Result<Self, SchemaProfileError> {
        validate_mcp_form_schema(&schema)?;
        let canonical_digest = schema_digest(&schema)?;
        Ok(Self {
            schema_version: 1,
            profile: MCP_FORM_SCHEMA_PROFILE_ID.to_owned(),
            schema,
            canonical_digest,
        })
    }

    pub fn validate_mcp_form_instance(&self, value: &Value) -> Result<(), SchemaProfileError> {
        if self.profile != MCP_FORM_SCHEMA_PROFILE_ID {
            return Err(SchemaProfileError::new(
                "$/profile",
                "interaction_schema_profile",
                "interaction schema is not an MCP form schema",
            ));
        }
        self.validate()?;
        validate_mcp_form_instance(&self.schema, value)
    }
}

const MAX_MCP_FORM_PROPERTIES: usize = 64;
const MAX_MCP_FORM_ENUM_VALUES: usize = 128;
const MAX_MCP_FORM_STRING_BYTES: usize = 8_192;

pub fn validate_mcp_form_schema(schema: &Value) -> Result<(), SchemaProfileError> {
    let serialized_bytes = serde_json::to_vec(schema)
        .map_err(|_| SchemaProfileError::new("$", "mcp_form_schema", "schema is invalid"))?
        .len();
    if serialized_bytes > 65_536 {
        return Err(SchemaProfileError::new(
            "$",
            "mcp_form_schema_bound",
            "MCP form schema is too large",
        ));
    }
    let root = schema
        .as_object()
        .ok_or_else(|| SchemaProfileError::new("$", "mcp_form_root", "schema must be an object"))?;
    require_only_keys(
        root,
        &["$schema", "type", "properties", "required"],
        "$",
        "mcp_form_keyword",
    )?;
    if root.get("type").and_then(Value::as_str) != Some("object") {
        return Err(SchemaProfileError::new(
            "$/type",
            "mcp_form_root_type",
            "MCP form schema root must have type object",
        ));
    }
    if root.get("$schema").is_some_and(|dialect| {
        dialect.as_str() != Some("https://json-schema.org/draft/2020-12/schema")
            && dialect.as_str() != Some("https://json-schema.org/draft/2020-12/schema#")
    }) {
        return Err(SchemaProfileError::new(
            "$/$schema",
            "mcp_form_dialect",
            "MCP form schema dialect is not supported",
        ));
    }
    let properties = root
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            SchemaProfileError::new(
                "$/properties",
                "mcp_form_properties",
                "MCP form schema must declare properties",
            )
        })?;
    if properties.len() > MAX_MCP_FORM_PROPERTIES {
        return Err(SchemaProfileError::new(
            "$/properties",
            "mcp_form_properties_bound",
            "MCP form has too many properties",
        ));
    }
    for (name, definition) in properties {
        if name.is_empty()
            || name.len() > 128
            || name.chars().any(char::is_control)
            || sensitive_form_term(name)
        {
            return Err(SchemaProfileError::new(
                format!("$/properties/{name}"),
                "mcp_form_property_name",
                "MCP form property name is unsafe or invalid",
            ));
        }
        validate_mcp_primitive_schema(definition, &format!("$/properties/{name}"))?;
    }
    let required = root
        .get("required")
        .map(|value| {
            value.as_array().ok_or_else(|| {
                SchemaProfileError::new(
                    "$/required",
                    "mcp_form_required_type",
                    "required must be an array",
                )
            })
        })
        .transpose()?
        .cloned()
        .unwrap_or_default();
    let mut unique = BTreeSet::new();
    for name in required {
        let name = name.as_str().ok_or_else(|| {
            SchemaProfileError::new(
                "$/required",
                "mcp_form_required_entry",
                "required entries must be strings",
            )
        })?;
        if !properties.contains_key(name) || !unique.insert(name.to_owned()) {
            return Err(SchemaProfileError::new(
                "$/required",
                "mcp_form_required_entry",
                "required entries must be unique declared properties",
            ));
        }
    }
    Ok(())
}

fn validate_mcp_primitive_schema(value: &Value, path: &str) -> Result<(), SchemaProfileError> {
    let node = value.as_object().ok_or_else(|| {
        SchemaProfileError::new(
            path,
            "mcp_form_property",
            "property schema must be an object",
        )
    })?;
    for key in ["title", "description"] {
        if node.get(key).is_some_and(|value| {
            value.as_str().is_none_or(|text| {
                text.len() > MAX_MCP_FORM_STRING_BYTES
                    || text.chars().any(char::is_control)
                    || sensitive_form_term(text)
            })
        }) {
            return Err(SchemaProfileError::new(
                format!("{path}/{key}"),
                "mcp_form_presentation",
                "MCP form presentation text is unsafe or invalid",
            ));
        }
    }
    match node.get("type").and_then(Value::as_str) {
        Some("boolean") => {
            require_only_keys(
                node,
                &["type", "title", "description", "default"],
                path,
                "mcp_form_boolean_keyword",
            )?;
            if node.get("default").is_some_and(|value| !value.is_boolean()) {
                return Err(SchemaProfileError::new(
                    path,
                    "mcp_form_default",
                    "boolean default is invalid",
                ));
            }
        }
        Some("number") | Some("integer") => {
            require_only_keys(
                node,
                &[
                    "type",
                    "title",
                    "description",
                    "minimum",
                    "maximum",
                    "default",
                ],
                path,
                "mcp_form_number_keyword",
            )?;
            for key in ["minimum", "maximum", "default"] {
                if node.get(key).is_some_and(|value| !value.is_number()) {
                    return Err(SchemaProfileError::new(
                        path,
                        "mcp_form_number",
                        "numeric constraint is invalid",
                    ));
                }
            }
            if node
                .get("minimum")
                .and_then(Value::as_f64)
                .zip(node.get("maximum").and_then(Value::as_f64))
                .is_some_and(|(min, max)| min > max)
            {
                return Err(SchemaProfileError::new(
                    path,
                    "mcp_form_number_bounds",
                    "minimum exceeds maximum",
                ));
            }
            if node.get("type").and_then(Value::as_str) == Some("integer")
                && ["minimum", "maximum", "default"]
                    .iter()
                    .filter_map(|key| node.get(*key))
                    .any(|value| value.as_i64().is_none() && value.as_u64().is_none())
            {
                return Err(SchemaProfileError::new(
                    path,
                    "mcp_form_integer",
                    "integer constraints must be integers",
                ));
            }
        }
        Some("string") => validate_mcp_string_schema(node, path)?,
        Some("array") => validate_mcp_multi_select_schema(node, path)?,
        _ => {
            return Err(SchemaProfileError::new(
                format!("{path}/type"),
                "mcp_form_type",
                "MCP form properties must use a supported primitive or enum schema",
            ))
        }
    }
    Ok(())
}

fn validate_mcp_string_schema(
    node: &Map<String, Value>,
    path: &str,
) -> Result<(), SchemaProfileError> {
    let enum_shape =
        node.contains_key("enum") || node.contains_key("oneOf") || node.contains_key("enumNames");
    if enum_shape {
        require_only_keys(
            node,
            &[
                "type",
                "title",
                "description",
                "enum",
                "enumNames",
                "oneOf",
                "default",
            ],
            path,
            "mcp_form_enum_keyword",
        )?;
        let values = mcp_enum_values(node, path)?;
        if node
            .get("default")
            .is_some_and(|value| value.as_str().is_none_or(|value| !values.contains(value)))
        {
            return Err(SchemaProfileError::new(
                path,
                "mcp_form_default",
                "enum default is invalid",
            ));
        }
        return Ok(());
    }
    require_only_keys(
        node,
        &[
            "type",
            "title",
            "description",
            "minLength",
            "maxLength",
            "format",
            "default",
        ],
        path,
        "mcp_form_string_keyword",
    )?;
    let minimum = optional_u64(node, "minLength", path)?;
    let maximum = optional_u64(node, "maxLength", path)?;
    if maximum.is_some_and(|value| value as usize > MAX_MCP_FORM_STRING_BYTES)
        || minimum.zip(maximum).is_some_and(|(min, max)| min > max)
    {
        return Err(SchemaProfileError::new(
            path,
            "mcp_form_string_bounds",
            "string bounds are invalid",
        ));
    }
    if node.get("format").is_some_and(|value| {
        !matches!(value.as_str(), Some("uri" | "email" | "date" | "date-time"))
    }) {
        return Err(SchemaProfileError::new(
            path,
            "mcp_form_string_format",
            "string format is invalid",
        ));
    }
    if node.get("default").is_some_and(|value| {
        value.as_str().is_none_or(|value| {
            value.len() > MAX_MCP_FORM_STRING_BYTES
                || !string_instance_in_bounds(node, value)
                || !valid_mcp_string_format(node.get("format").and_then(Value::as_str), value)
        })
    }) {
        return Err(SchemaProfileError::new(
            path,
            "mcp_form_default",
            "string default is invalid",
        ));
    }
    Ok(())
}

fn validate_mcp_multi_select_schema(
    node: &Map<String, Value>,
    path: &str,
) -> Result<(), SchemaProfileError> {
    require_only_keys(
        node,
        &[
            "type",
            "title",
            "description",
            "minItems",
            "maxItems",
            "items",
            "default",
        ],
        path,
        "mcp_form_multi_keyword",
    )?;
    let items = node
        .get("items")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            SchemaProfileError::new(
                format!("{path}/items"),
                "mcp_form_multi_items",
                "multi-select items are required",
            )
        })?;
    require_only_keys(
        items,
        &["type", "enum", "anyOf"],
        &format!("{path}/items"),
        "mcp_form_multi_items_keyword",
    )?;
    if items
        .get("type")
        .is_some_and(|value| value.as_str() != Some("string"))
    {
        return Err(SchemaProfileError::new(
            format!("{path}/items/type"),
            "mcp_form_multi_items",
            "multi-select items must be strings",
        ));
    }
    let values = mcp_enum_values(items, &format!("{path}/items"))?;
    let minimum = optional_u64(node, "minItems", path)?.unwrap_or(0);
    let maximum = optional_u64(node, "maxItems", path)?.unwrap_or(values.len() as u64);
    if minimum > maximum || maximum as usize > values.len() {
        return Err(SchemaProfileError::new(
            path,
            "mcp_form_multi_bounds",
            "multi-select bounds are invalid",
        ));
    }
    if let Some(default) = node.get("default") {
        let default = default.as_array().ok_or_else(|| {
            SchemaProfileError::new(path, "mcp_form_default", "multi-select default is invalid")
        })?;
        if default.len() < minimum as usize
            || default.len() > maximum as usize
            || default
                .iter()
                .filter_map(Value::as_str)
                .collect::<BTreeSet<_>>()
                .len()
                != default.len()
            || default
                .iter()
                .any(|value| value.as_str().is_none_or(|value| !values.contains(value)))
        {
            return Err(SchemaProfileError::new(
                path,
                "mcp_form_default",
                "multi-select default is invalid",
            ));
        }
    }
    Ok(())
}

fn mcp_enum_values(
    node: &Map<String, Value>,
    path: &str,
) -> Result<BTreeSet<String>, SchemaProfileError> {
    let candidates = if let Some(values) = node.get("enum") {
        if node.contains_key("oneOf") {
            return Err(SchemaProfileError::new(
                path,
                "mcp_form_enum_shape",
                "enum and oneOf cannot be combined",
            ));
        }
        values
            .as_array()
            .ok_or_else(|| {
                SchemaProfileError::new(path, "mcp_form_enum_values", "enum must be an array")
            })?
            .iter()
            .map(|value| value.as_str().map(ToOwned::to_owned))
            .collect::<Option<Vec<_>>>()
    } else if let Some(values) = node.get("oneOf").or_else(|| node.get("anyOf")) {
        values.as_array().and_then(|values| {
            values
                .iter()
                .map(|value| {
                    let option = value.as_object()?;
                    if !option
                        .keys()
                        .all(|key| matches!(key.as_str(), "const" | "title"))
                        || option.len() != 2
                        || option.get("title").and_then(Value::as_str).is_none()
                    {
                        return None;
                    }
                    option.get("const")?.as_str().map(ToOwned::to_owned)
                })
                .collect::<Option<Vec<_>>>()
        })
    } else {
        None
    }
    .ok_or_else(|| {
        SchemaProfileError::new(path, "mcp_form_enum_values", "enum values are invalid")
    })?;
    if candidates.is_empty()
        || candidates.len() > MAX_MCP_FORM_ENUM_VALUES
        || candidates.iter().any(|value| {
            value.is_empty() || value.len() > 512 || value.chars().any(char::is_control)
        })
    {
        return Err(SchemaProfileError::new(
            path,
            "mcp_form_enum_values",
            "enum values are invalid",
        ));
    }
    let candidate_count = candidates.len();
    let values = candidates.into_iter().collect::<BTreeSet<_>>();
    if values.is_empty()
        || values.len() > MAX_MCP_FORM_ENUM_VALUES
        || values.len() != candidate_count
    {
        return Err(SchemaProfileError::new(
            path,
            "mcp_form_enum_values",
            "enum values must be unique",
        ));
    }
    if let Some(names) = node.get("enumNames") {
        let names = names.as_array().ok_or_else(|| {
            SchemaProfileError::new(path, "mcp_form_enum_names", "enumNames must be an array")
        })?;
        if names.len() != values.len()
            || names.iter().any(|name| {
                name.as_str().is_none_or(|name| {
                    name.is_empty() || name.len() > 512 || name.chars().any(char::is_control)
                })
            })
        {
            return Err(SchemaProfileError::new(
                path,
                "mcp_form_enum_names",
                "enumNames are invalid",
            ));
        }
    }
    Ok(values)
}

fn validate_mcp_form_instance(schema: &Value, value: &Value) -> Result<(), SchemaProfileError> {
    let properties = schema["properties"]
        .as_object()
        .expect("validated MCP schema");
    let object = value.as_object().ok_or_else(|| {
        SchemaProfileError::new(
            "$",
            "mcp_form_instance_type",
            "form response must be an object",
        )
    })?;
    if object.keys().any(|name| !properties.contains_key(name)) {
        return Err(SchemaProfileError::new(
            "$",
            "mcp_form_instance_property",
            "form response contains an unknown property",
        ));
    }
    for required in schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        if !object.contains_key(required) {
            return Err(SchemaProfileError::new(
                format!("$/{required}"),
                "mcp_form_instance_required",
                "required form property is missing",
            ));
        }
    }
    for (name, value) in object {
        validate_mcp_primitive_instance(&properties[name], value, &format!("$/{name}"))?;
    }
    Ok(())
}

fn validate_mcp_primitive_instance(
    schema: &Value,
    value: &Value,
    path: &str,
) -> Result<(), SchemaProfileError> {
    let node = schema.as_object().expect("validated MCP property");
    let valid = match node.get("type").and_then(Value::as_str) {
        Some("boolean") => value.is_boolean(),
        Some("integer") => value.as_i64().is_some() || value.as_u64().is_some(),
        Some("number") => value.is_number(),
        Some("string") => value.as_str().is_some_and(|text| {
            let enum_values = if node.contains_key("enum") || node.contains_key("oneOf") {
                mcp_enum_values(node, path).ok()
            } else {
                None
            };
            string_instance_in_bounds(node, text)
                && enum_values
                    .as_ref()
                    .is_none_or(|values| values.contains(text))
                && valid_mcp_string_format(node.get("format").and_then(Value::as_str), text)
        }),
        Some("array") => value.as_array().is_some_and(|items| {
            let values = node
                .get("items")
                .and_then(Value::as_object)
                .and_then(|items| mcp_enum_values(items, path).ok());
            let minimum = node.get("minItems").and_then(Value::as_u64).unwrap_or(0) as usize;
            let maximum = node.get("maxItems").and_then(Value::as_u64).map_or_else(
                || values.as_ref().map_or(0, BTreeSet::len),
                |value| value as usize,
            );
            items.len() >= minimum
                && items.len() <= maximum
                && items
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<BTreeSet<_>>()
                    .len()
                    == items.len()
                && values.is_some_and(|values| {
                    items
                        .iter()
                        .all(|item| item.as_str().is_some_and(|item| values.contains(item)))
                })
        }),
        _ => false,
    };
    if !valid || !numeric_instance_in_bounds(node, value) {
        return Err(SchemaProfileError::new(
            path,
            "mcp_form_instance",
            "form response does not match the requested schema",
        ));
    }
    Ok(())
}

fn string_instance_in_bounds(node: &Map<String, Value>, text: &str) -> bool {
    let characters = text.chars().count() as u64;
    characters >= node.get("minLength").and_then(Value::as_u64).unwrap_or(0)
        && characters
            <= node
                .get("maxLength")
                .and_then(Value::as_u64)
                .unwrap_or(MAX_MCP_FORM_STRING_BYTES as u64)
        && text.len() <= MAX_MCP_FORM_STRING_BYTES
}

fn numeric_instance_in_bounds(node: &Map<String, Value>, value: &Value) -> bool {
    let Some(actual) = value.as_f64() else {
        return true;
    };
    node.get("minimum")
        .and_then(Value::as_f64)
        .is_none_or(|minimum| actual >= minimum)
        && node
            .get("maximum")
            .and_then(Value::as_f64)
            .is_none_or(|maximum| actual <= maximum)
}

fn valid_mcp_string_format(format: Option<&str>, value: &str) -> bool {
    match format {
        None => true,
        Some("uri") => value.contains(":") && !value.chars().any(char::is_whitespace),
        Some("email") => value
            .split_once('@')
            .is_some_and(|(left, right)| !left.is_empty() && right.contains('.')),
        Some("date") => chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok(),
        Some("date-time") => chrono::DateTime::parse_from_rfc3339(value).is_ok(),
        _ => false,
    }
}

fn optional_u64(
    node: &Map<String, Value>,
    key: &str,
    path: &str,
) -> Result<Option<u64>, SchemaProfileError> {
    node.get(key)
        .map(|value| {
            value.as_u64().ok_or_else(|| {
                SchemaProfileError::new(
                    format!("{path}/{key}"),
                    "mcp_form_bound",
                    "bound must be a non-negative integer",
                )
            })
        })
        .transpose()
}

fn require_only_keys(
    node: &Map<String, Value>,
    allowed: &[&str],
    path: &str,
    code: &'static str,
) -> Result<(), SchemaProfileError> {
    if let Some(key) = node.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(SchemaProfileError::new(
            format!("{path}/{key}"),
            code,
            "keyword is outside the MCP form profile",
        ));
    }
    Ok(())
}

fn sensitive_form_term(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase().replace(['-', ' '], "_");
    [
        "password",
        "passphrase",
        "api_key",
        "apikey",
        "access_token",
        "refresh_token",
        "secret",
        "credential",
        "credit_card",
        "card_number",
        "cvv",
        "bank_account",
        "private_key",
        "social_security",
    ]
    .iter()
    .any(|term| normalized.contains(term))
}

fn schema_digest(schema: &Value) -> Result<Sha256Digest, SchemaProfileError> {
    canonical_digest(schema)
        .map_err(|_| {
            SchemaProfileError::new(
                "$",
                "interaction_schema_canonicalization",
                "interaction schema cannot be canonicalized",
            )
        })?
        .parse()
        .map_err(|_| {
            SchemaProfileError::new(
                "$",
                "interaction_schema_digest",
                "interaction schema digest is invalid",
            )
        })
}

pub const ALLOWED_SCHEMA_KEYWORDS: &[&str] = &[
    "$schema",
    "$id",
    "$defs",
    "$ref",
    "type",
    "title",
    "description",
    "properties",
    "required",
    "additionalProperties",
    "items",
    "minItems",
    "maxItems",
    "uniqueItems",
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "enum",
    "const",
    "oneOf",
    "x-platform-max-bytes",
    "x-platform-classification",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaProfileError {
    pub path: String,
    pub code: &'static str,
    pub message: String,
}

impl SchemaProfileError {
    fn new(path: impl Into<String>, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for SchemaProfileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} at {}: {}",
            self.code, self.path, self.message
        )
    }
}

impl Error for SchemaProfileError {}

pub fn validate_closed_schema(schema: &Value) -> Result<(), SchemaProfileError> {
    let root = schema.as_object().ok_or_else(|| {
        SchemaProfileError::new(
            "$",
            "schema_root_not_object",
            "schema root must be an object",
        )
    })?;
    let object_root = root.get("type") == Some(&Value::String("object".to_owned()));
    let nominal_root = root
        .get("$ref")
        .and_then(Value::as_str)
        .is_some_and(crate::nominal::is_known_pinned_nominal_reference);
    if !object_root && !nominal_root {
        return Err(SchemaProfileError::new(
            "$",
            "schema_root_type",
            "schema root must declare type object or an exact pinned platform nominal reference",
        ));
    }
    if root.get("$schema")
        != Some(&Value::String(
            "https://json-schema.org/draft/2020-12/schema".to_owned(),
        ))
    {
        return Err(SchemaProfileError::new(
            "$/$schema",
            "schema_dialect_required",
            "root must declare JSON Schema 2020-12",
        ));
    }

    validate_schema_node(root, "$", true)?;
    validate_local_references(root)?;
    Ok(())
}

fn validate_schema_node(
    node: &Map<String, Value>,
    path: &str,
    _is_root: bool,
) -> Result<(), SchemaProfileError> {
    for keyword in node.keys() {
        if !ALLOWED_SCHEMA_KEYWORDS.contains(&keyword.as_str()) {
            return Err(SchemaProfileError::new(
                format!("{path}/{keyword}"),
                "schema_unknown_keyword",
                format!("keyword {keyword:?} is outside {CLOSED_SCHEMA_PROFILE_ID}"),
            ));
        }
    }

    if node.get("x-platform-classification").is_some_and(|value| {
        !matches!(
            value.as_str(),
            Some("public" | "internal" | "confidential" | "restricted")
        )
    }) {
        return Err(SchemaProfileError::new(
            format!("{path}/x-platform-classification"),
            "schema_classification",
            "field classification must use the closed platform registry",
        ));
    }

    if let Some(reference) = node.get("$ref") {
        let reference = reference.as_str().ok_or_else(|| {
            SchemaProfileError::new(
                format!("{path}/$ref"),
                "schema_ref_type",
                "$ref must be a string",
            )
        })?;
        if !is_allowed_reference(reference) {
            return Err(SchemaProfileError::new(
                format!("{path}/$ref"),
                "schema_ref_forbidden",
                "only local $defs or digest-pinned platform nominal references are allowed",
            ));
        }
    }

    if node.contains_key("type") && !node["type"].is_string() {
        return Err(SchemaProfileError::new(
            format!("{path}/type"),
            "schema_type_shape",
            "type must be one string; nullable values use an explicit oneOf null branch",
        ));
    }

    match node.get("type").and_then(Value::as_str) {
        Some("object") => validate_object(node, path)?,
        Some("array") => {
            require_nonnegative_bound(node, "minItems", path)?;
            require_positive_bound(node, "maxItems", path)?;
            if node
                .get("minItems")
                .and_then(Value::as_u64)
                .zip(node.get("maxItems").and_then(Value::as_u64))
                .is_some_and(|(minimum, maximum)| minimum > maximum)
            {
                return Err(SchemaProfileError::new(
                    path,
                    "schema_bounds_order",
                    "minItems cannot exceed maxItems",
                ));
            }
            let items = node
                .get("items")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    SchemaProfileError::new(
                        format!("{path}/items"),
                        "schema_items_required",
                        "array must declare one item schema",
                    )
                })?;
            validate_schema_node(items, &format!("{path}/items"), false)?;
        }
        Some("string") => {
            require_nonnegative_bound(node, "minLength", path)?;
            require_positive_bound(node, "maxLength", path)?;
            require_positive_bound(node, "x-platform-max-bytes", path)?;
        }
        Some("integer") | Some("number") => validate_numeric_bounds(node, path)?,
        Some("null") | Some("boolean") | None => {}
        Some(other) => {
            return Err(SchemaProfileError::new(
                format!("{path}/type"),
                "schema_type_forbidden",
                format!("type {other:?} is not in the profile"),
            ));
        }
    }

    if let Some(definitions) = node.get("$defs") {
        let definitions = definitions.as_object().ok_or_else(|| {
            SchemaProfileError::new(
                format!("{path}/$defs"),
                "schema_defs_type",
                "$defs must be an object",
            )
        })?;
        for (name, definition) in definitions {
            let definition = definition.as_object().ok_or_else(|| {
                SchemaProfileError::new(
                    format!("{path}/$defs/{name}"),
                    "schema_definition_type",
                    "definition must be a schema object",
                )
            })?;
            validate_schema_node(definition, &format!("{path}/$defs/{name}"), false)?;
        }
    }

    if let Some(branches) = node.get("oneOf") {
        validate_tagged_union(branches, path)?;
    }
    Ok(())
}

/// Capability publication adds mandatory field metadata to the single platform schema profile.
pub fn validate_capability_interface_schema(
    schema: &ClosedJsonSchema,
) -> Result<(), SchemaProfileError> {
    schema.validate()?;
    require_capability_property_metadata(
        schema.schema.as_object().ok_or_else(|| {
            SchemaProfileError::new(
                "$",
                "schema_root_not_object",
                "schema root must be an object",
            )
        })?,
        "$",
    )
}

fn require_capability_property_metadata(
    node: &Map<String, Value>,
    path: &str,
) -> Result<(), SchemaProfileError> {
    if let Some(properties) = node.get("properties").and_then(Value::as_object) {
        for (name, property) in properties {
            let property = property.as_object().ok_or_else(|| {
                SchemaProfileError::new(
                    format!("{path}/properties/{name}"),
                    "schema_property_type",
                    "property must be a schema object",
                )
            })?;
            let description = property.get("description").and_then(Value::as_str);
            if description.is_none_or(|value| {
                value.is_empty() || value.len() > 4_096 || value.chars().any(char::is_control)
            }) {
                return Err(SchemaProfileError::new(
                    format!("{path}/properties/{name}/description"),
                    "schema_property_description",
                    "Capability field requires a bounded non-empty description",
                ));
            }
            if !matches!(
                property
                    .get("x-platform-classification")
                    .and_then(Value::as_str),
                Some("public" | "internal" | "confidential" | "restricted")
            ) {
                return Err(SchemaProfileError::new(
                    format!("{path}/properties/{name}/x-platform-classification"),
                    "schema_property_classification",
                    "Capability field requires a closed data classification",
                ));
            }
            require_capability_property_metadata(property, &format!("{path}/properties/{name}"))?;
        }
    }
    if let Some(definitions) = node.get("$defs").and_then(Value::as_object) {
        for (name, definition) in definitions {
            if let Some(definition) = definition.as_object() {
                require_capability_property_metadata(definition, &format!("{path}/$defs/{name}"))?;
            }
        }
    }
    if let Some(items) = node.get("items").and_then(Value::as_object) {
        require_capability_property_metadata(items, &format!("{path}/items"))?;
    }
    if let Some(branches) = node.get("oneOf").and_then(Value::as_array) {
        for (index, branch) in branches.iter().enumerate() {
            if let Some(branch) = branch.as_object() {
                require_capability_property_metadata(branch, &format!("{path}/oneOf/{index}"))?;
            }
        }
    }
    Ok(())
}

fn validate_object(node: &Map<String, Value>, path: &str) -> Result<(), SchemaProfileError> {
    if node.get("additionalProperties") != Some(&Value::Bool(false)) {
        return Err(SchemaProfileError::new(
            format!("{path}/additionalProperties"),
            "schema_object_open",
            "every object must explicitly set additionalProperties to false",
        ));
    }
    let properties = node
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            SchemaProfileError::new(
                format!("{path}/properties"),
                "schema_properties_required",
                "object must declare properties",
            )
        })?;
    let required = node
        .get("required")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            SchemaProfileError::new(
                format!("{path}/required"),
                "schema_required_required",
                "object must explicitly declare required, including an empty list",
            )
        })?;
    let mut required_names = BTreeSet::new();
    for value in required {
        let name = value.as_str().ok_or_else(|| {
            SchemaProfileError::new(
                format!("{path}/required"),
                "schema_required_type",
                "required entries must be strings",
            )
        })?;
        if !properties.contains_key(name) || !required_names.insert(name) {
            return Err(SchemaProfileError::new(
                format!("{path}/required"),
                "schema_required_invalid",
                "required entries must be unique declared properties",
            ));
        }
    }
    for (name, property) in properties {
        let property = property.as_object().ok_or_else(|| {
            SchemaProfileError::new(
                format!("{path}/properties/{name}"),
                "schema_property_type",
                "property must be a schema object",
            )
        })?;
        validate_schema_node(property, &format!("{path}/properties/{name}"), false)?;
    }
    Ok(())
}

fn validate_numeric_bounds(
    node: &Map<String, Value>,
    path: &str,
) -> Result<(), SchemaProfileError> {
    let has_lower = node.contains_key("minimum") || node.contains_key("exclusiveMinimum");
    let has_upper = node.contains_key("maximum") || node.contains_key("exclusiveMaximum");
    if !has_lower || !has_upper {
        return Err(SchemaProfileError::new(
            path,
            "schema_numeric_unbounded",
            "number and integer schemas require explicit lower and upper bounds",
        ));
    }
    for keyword in [
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
    ] {
        if node.get(keyword).is_some_and(|value| !value.is_number()) {
            return Err(SchemaProfileError::new(
                format!("{path}/{keyword}"),
                "schema_numeric_bound_type",
                "numeric bound must be a JSON number",
            ));
        }
    }
    Ok(())
}

fn require_nonnegative_bound(
    node: &Map<String, Value>,
    keyword: &'static str,
    path: &str,
) -> Result<(), SchemaProfileError> {
    if node.get(keyword).and_then(Value::as_u64).is_none() {
        return Err(SchemaProfileError::new(
            format!("{path}/{keyword}"),
            "schema_bound_required",
            format!("{keyword} must be a non-negative integer"),
        ));
    }
    Ok(())
}

fn require_positive_bound(
    node: &Map<String, Value>,
    keyword: &'static str,
    path: &str,
) -> Result<(), SchemaProfileError> {
    if node
        .get(keyword)
        .and_then(Value::as_u64)
        .is_none_or(|value| value == 0)
    {
        return Err(SchemaProfileError::new(
            format!("{path}/{keyword}"),
            "schema_bound_required",
            format!("{keyword} must be a positive integer"),
        ));
    }
    Ok(())
}

fn validate_tagged_union(branches: &Value, path: &str) -> Result<(), SchemaProfileError> {
    let branches = branches.as_array().ok_or_else(|| {
        SchemaProfileError::new(
            format!("{path}/oneOf"),
            "schema_union_type",
            "oneOf must be an array",
        )
    })?;
    if branches.len() < 2 {
        return Err(SchemaProfileError::new(
            format!("{path}/oneOf"),
            "schema_union_size",
            "oneOf requires at least two branches",
        ));
    }
    let null_branches = branches
        .iter()
        .filter(|branch| branch.get("type") == Some(&Value::String("null".to_owned())))
        .count();
    if null_branches == 1 && branches.len() == 2 {
        for (index, branch) in branches.iter().enumerate() {
            let branch = branch.as_object().ok_or_else(|| {
                SchemaProfileError::new(
                    format!("{path}/oneOf/{index}"),
                    "schema_union_branch_type",
                    "nullable branch must be a schema object",
                )
            })?;
            validate_schema_node(branch, &format!("{path}/oneOf/{index}"), false)?;
        }
        return Ok(());
    }
    let mut candidates: Option<BTreeMap<String, BTreeSet<String>>> = None;
    for (index, branch) in branches.iter().enumerate() {
        let branch = branch.as_object().ok_or_else(|| {
            SchemaProfileError::new(
                format!("{path}/oneOf/{index}"),
                "schema_union_branch_type",
                "union branch must be a schema object",
            )
        })?;
        validate_schema_node(branch, &format!("{path}/oneOf/{index}"), false)?;
        let properties = branch
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                SchemaProfileError::new(
                    format!("{path}/oneOf/{index}/properties"),
                    "schema_union_discriminator",
                    "tagged union branch must be a closed object",
                )
            })?;
        let required = branch
            .get("required")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                SchemaProfileError::new(
                    format!("{path}/oneOf/{index}/required"),
                    "schema_union_discriminator",
                    "tagged union branch must require its discriminator",
                )
            })?;
        let required = required
            .iter()
            .filter_map(Value::as_str)
            .collect::<BTreeSet<_>>();
        let branch_candidates = properties
            .iter()
            .filter_map(|(name, property)| {
                let constant = property.get("const")?.as_str()?;
                required
                    .contains(name.as_str())
                    .then(|| (name.clone(), constant.to_owned()))
            })
            .collect::<BTreeMap<_, _>>();
        if branch_candidates.is_empty() {
            return Err(SchemaProfileError::new(
                format!("{path}/oneOf/{index}"),
                "schema_union_discriminator",
                "branch requires a string const discriminator",
            ));
        }
        match &mut candidates {
            None => {
                candidates = Some(
                    branch_candidates
                        .into_iter()
                        .map(|(name, value)| (name, BTreeSet::from([value])))
                        .collect(),
                );
            }
            Some(existing) => {
                existing.retain(|name, values| {
                    branch_candidates
                        .get(name)
                        .is_some_and(|value| values.insert(value.clone()))
                });
            }
        }
    }
    let valid = candidates.is_some_and(|items| {
        items.len() == 1
            && items
                .values()
                .next()
                .is_some_and(|values| values.len() == branches.len())
    });
    if !valid {
        return Err(SchemaProfileError::new(
            format!("{path}/oneOf"),
            "schema_union_discriminator",
            "oneOf must have exactly one common required string discriminator with distinct const values",
        ));
    }
    Ok(())
}

fn is_allowed_reference(reference: &str) -> bool {
    reference.starts_with("#/$defs/")
        || crate::nominal::is_known_pinned_nominal_reference(reference)
}

fn validate_local_references(root: &Map<String, Value>) -> Result<(), SchemaProfileError> {
    let definitions = root.get("$defs").and_then(Value::as_object);
    let mut graph = BTreeMap::<String, BTreeSet<String>>::new();
    if let Some(definitions) = definitions {
        for (name, definition) in definitions {
            let mut references = BTreeSet::new();
            collect_local_references(definition, &mut references);
            for reference in &references {
                if !definitions.contains_key(reference) {
                    return Err(SchemaProfileError::new(
                        format!("$/$defs/{name}"),
                        "schema_ref_missing",
                        format!("local definition {reference:?} does not exist"),
                    ));
                }
            }
            graph.insert(name.clone(), references);
        }
    }

    let mut active = BTreeSet::new();
    let mut complete = BTreeSet::new();
    for name in graph.keys() {
        visit_reference(name, &graph, &mut active, &mut complete)?;
    }
    Ok(())
}

fn collect_local_references(value: &Value, references: &mut BTreeSet<String>) {
    match value {
        Value::Object(object) => {
            if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                if let Some(name) = reference.strip_prefix("#/$defs/") {
                    references.insert(name.to_owned());
                }
            }
            for child in object.values() {
                collect_local_references(child, references);
            }
        }
        Value::Array(array) => {
            for child in array {
                collect_local_references(child, references);
            }
        }
        _ => {}
    }
}

fn visit_reference(
    name: &str,
    graph: &BTreeMap<String, BTreeSet<String>>,
    active: &mut BTreeSet<String>,
    complete: &mut BTreeSet<String>,
) -> Result<(), SchemaProfileError> {
    if complete.contains(name) {
        return Ok(());
    }
    if !active.insert(name.to_owned()) {
        return Err(SchemaProfileError::new(
            format!("$/$defs/{name}"),
            "schema_ref_cycle",
            "recursive local references are forbidden",
        ));
    }
    for target in graph.get(name).into_iter().flatten() {
        visit_reference(target, graph, active, complete)?;
    }
    active.remove(name);
    complete.insert(name.to_owned());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_schema() -> Value {
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "urn:fixture:input",
            "type": "object",
            "properties": {
                "count": {"type": "integer", "minimum": 0, "maximum": 100},
                "name": {
                    "type": "string",
                    "minLength": 0,
                    "maxLength": 32,
                    "x-platform-max-bytes": 128
                }
            },
            "required": ["count"],
            "additionalProperties": false
        })
    }

    #[test]
    fn accepts_closed_bounded_schema() {
        validate_closed_schema(&valid_schema()).unwrap();
        let document = InteractionSchemaDocument::build(valid_schema()).unwrap();
        document.validate().unwrap();
    }

    #[test]
    fn closed_schema_document_binds_profile_digest_and_instances() {
        let document = ClosedJsonSchema::build(valid_schema()).unwrap();
        document.validate_instance(&json!({"count": 7})).unwrap();
        assert!(document.validate_instance(&json!({"count": -1})).is_err());

        let mut forged = document.clone();
        forged.canonical_digest =
            "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                .parse()
                .unwrap();
        assert_eq!(forged.validate().unwrap_err().code, "closed_schema_digest");

        let mut wrong_profile = document;
        wrong_profile.profile = "vendor.schema/1".to_owned();
        assert_eq!(
            wrong_profile.validate().unwrap_err().code,
            "closed_schema_binding"
        );
    }

    #[test]
    fn capability_schema_requires_closed_field_metadata() {
        let schema = ClosedJsonSchema::build(valid_schema()).unwrap();
        assert_eq!(
            validate_capability_interface_schema(&schema)
                .unwrap_err()
                .code,
            "schema_property_description"
        );

        let mut annotated = valid_schema();
        for property in annotated["properties"]
            .as_object_mut()
            .unwrap()
            .values_mut()
        {
            property["description"] = json!("bounded fixture field");
            property["x-platform-classification"] = json!("internal");
        }
        validate_capability_interface_schema(&ClosedJsonSchema::build(annotated).unwrap()).unwrap();
    }

    #[test]
    fn mcp_form_profile_is_closed_bounded_non_secret_and_validates_responses() {
        let document = InteractionSchemaDocument::build_mcp_form(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "count": {"type": "integer", "minimum": 1, "maximum": 3},
                "enabled": {"type": "boolean"},
                "region": {"type": "string", "enum": ["cn", "us"]},
                "labels": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["a", "b"]},
                    "minItems": 1,
                    "maxItems": 2
                }
            },
            "required": ["count", "region"]
        }))
        .unwrap();
        document
            .validate_mcp_form_instance(&json!({
                "count": 2,
                "enabled": true,
                "region": "cn",
                "labels": ["a"]
            }))
            .unwrap();
        assert!(document
            .validate_mcp_form_instance(&json!({"count": 4, "region": "cn"}))
            .is_err());
        assert!(document
            .validate_mcp_form_instance(&json!({"count": 2, "region": "eu"}))
            .is_err());
        assert!(document
            .validate_mcp_form_instance(&json!({"count": 2, "region": "cn", "extra": true}))
            .is_err());

        assert!(InteractionSchemaDocument::build_mcp_form(json!({
            "type": "object",
            "properties": {
                "access_token": {"type": "string", "maxLength": 128}
            }
        }))
        .is_err());
        assert!(InteractionSchemaDocument::build_mcp_form(json!({
            "type": "object",
            "properties": {
                "region": {"type": "string", "enum": ["cn", "cn"]}
            }
        }))
        .is_err());
    }

    #[test]
    fn rejects_open_objects_unknown_keywords_and_unbounded_scalars() {
        let mut schema = valid_schema();
        schema["additionalProperties"] = Value::Bool(true);
        assert_eq!(
            validate_closed_schema(&schema).unwrap_err().code,
            "schema_object_open"
        );

        let mut schema = valid_schema();
        schema["pattern"] = Value::String(".*".to_owned());
        assert_eq!(
            validate_closed_schema(&schema).unwrap_err().code,
            "schema_unknown_keyword"
        );

        let mut schema = valid_schema();
        schema["properties"]["count"] = json!({"type": "integer"});
        assert_eq!(
            validate_closed_schema(&schema).unwrap_err().code,
            "schema_numeric_unbounded"
        );
    }

    #[test]
    fn rejects_recursive_local_references() {
        let schema = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"node": {"$ref": "#/$defs/node"}},
            "required": [],
            "additionalProperties": false,
            "$defs": {
                "node": {
                    "type": "object",
                    "properties": {"next": {"$ref": "#/$defs/node"}},
                    "required": [],
                    "additionalProperties": false
                }
            }
        });
        assert_eq!(
            validate_closed_schema(&schema).unwrap_err().code,
            "schema_ref_cycle"
        );
    }

    #[test]
    fn accepts_only_registered_exact_nominal_schema_digests() {
        let mut schema = valid_schema();
        schema["properties"]["digest"] = serde_json::json!({
            "$ref": crate::nominal::pinned_nominal_reference("Digest").unwrap()
        });
        validate_closed_schema(&schema).unwrap();

        schema["properties"]["digest"]["$ref"] = Value::String(
            "urn:insight:platform:v1:nominal:Digest@sha256:0000000000000000000000000000000000000000000000000000000000000000"
                .to_owned(),
        );
        assert_eq!(
            validate_closed_schema(&schema).unwrap_err().code,
            "schema_ref_forbidden"
        );
    }

    #[test]
    fn root_nominal_schema_resolves_and_validates_exact_instances() {
        let schema = ClosedJsonSchema::build(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$ref": crate::nominal::pinned_nominal_reference("Digest").unwrap()
        }))
        .unwrap();
        assert!(schema
            .validate_instance(&json!(
                "sha256:0000000000000000000000000000000000000000000000000000000000000000"
            ))
            .is_ok());
        assert!(schema.validate_instance(&json!("sha256:short")).is_err());
    }
}
