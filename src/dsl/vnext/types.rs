use std::{collections::BTreeMap, error::Error, fmt};

use serde_json::{Map, Value};

pub const SCHEMA_INVALID: &str = "VNEXT_SCHEMA_INVALID";
pub const SCHEMA_TYPE_UNSUPPORTED: &str = "VNEXT_SCHEMA_TYPE_UNSUPPORTED";
pub const SCHEMA_COMBINATOR_UNSUPPORTED: &str = "VNEXT_SCHEMA_COMBINATOR_UNSUPPORTED";
pub const SCHEMA_KEYWORD_UNSUPPORTED: &str = "VNEXT_SCHEMA_KEYWORD_UNSUPPORTED";
pub const SCHEMA_ENUM_INVALID: &str = "VNEXT_SCHEMA_ENUM_INVALID";
pub const TYPE_PATH_INVALID: &str = "VNEXT_TYPE_PATH_INVALID";
pub const TYPE_PATH_FIELD_NOT_FOUND: &str = "VNEXT_TYPE_PATH_FIELD_NOT_FOUND";
pub const TYPE_PATH_EXPECTED_OBJECT: &str = "VNEXT_TYPE_PATH_EXPECTED_OBJECT";
pub const TYPE_PATH_EXPECTED_ARRAY: &str = "VNEXT_TYPE_PATH_EXPECTED_ARRAY";
pub const TYPE_PATH_OPTIONAL_ACCESS: &str = "VNEXT_TYPE_PATH_OPTIONAL_ACCESS";
pub const TYPE_PATH_UNSAFE_UNION_ACCESS: &str = "VNEXT_TYPE_PATH_UNSAFE_UNION_ACCESS";
pub const TYPE_NARROWING_REQUIRES_UNION: &str = "VNEXT_TYPE_NARROWING_REQUIRES_UNION";
pub const TYPE_NARROWING_LITERAL_REQUIRED: &str = "VNEXT_TYPE_NARROWING_LITERAL_REQUIRED";
pub const TYPE_NARROWING_NO_MATCH: &str = "VNEXT_TYPE_NARROWING_NO_MATCH";
pub const TYPE_NOT_ASSIGNABLE: &str = "VNEXT_TYPE_NOT_ASSIGNABLE";
pub const TYPE_UNIFY_EMPTY: &str = "VNEXT_TYPE_UNIFY_EMPTY";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeError {
    code: &'static str,
    message: String,
}

impl TypeError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for TypeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for TypeError {}

#[derive(Debug, Clone, PartialEq)]
pub struct PropertyType {
    pub value_type: ValueType,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectType {
    pub properties: BTreeMap<String, PropertyType>,
    /// `None` represents `additionalProperties: false`; `Some(Any)` represents
    /// the JSON Schema default of unrestricted additional properties.
    pub additional_properties: Option<Box<ValueType>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArrayType {
    pub items: Box<ValueType>,
    pub min_items: usize,
}

/// The complete, deliberately safe run metadata surface exposed to authored
/// vNext expressions. Keep this contract in one place so lowering, IR
/// verification, and the runtime value cannot silently diverge.
pub(crate) fn safe_run_metadata_type() -> ValueType {
    ValueType::Object(ObjectType {
        properties: [
            "id",
            "request_id",
            "agent_id",
            "agent_version",
            "started_at",
        ]
        .into_iter()
        .map(|name| {
            (
                name.to_string(),
                PropertyType {
                    value_type: ValueType::String,
                    required: true,
                },
            )
        })
        .collect(),
        additional_properties: None,
    })
}

/// The conservative static type understood by the vNext compiler.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueType {
    Never,
    Any,
    Null,
    Boolean,
    Integer,
    Number,
    String,
    Literal(Value),
    Array(ArrayType),
    Object(ObjectType),
    Union(Vec<ValueType>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaType {
    value_type: ValueType,
}

impl SchemaType {
    pub fn compile(schema: &Value) -> Result<Self, TypeError> {
        Ok(Self {
            value_type: compile_schema(schema, "$")?,
        })
    }

    pub fn value_type(&self) -> &ValueType {
        &self.value_type
    }

    pub fn into_value_type(self) -> ValueType {
        self.value_type
    }
}

fn compile_schema(schema: &Value, location: &str) -> Result<ValueType, TypeError> {
    match schema {
        Value::Bool(true) => Ok(ValueType::Any),
        Value::Bool(false) => Ok(ValueType::Never),
        Value::Object(object) => compile_schema_object(object, location),
        _ => Err(TypeError::new(
            SCHEMA_INVALID,
            format!("schema at '{location}' must be an object or boolean"),
        )),
    }
}

fn compile_schema_object(
    object: &Map<String, Value>,
    location: &str,
) -> Result<ValueType, TypeError> {
    if object.contains_key("$ref") {
        return Err(TypeError::new(
            SCHEMA_COMBINATOR_UNSUPPORTED,
            format!("schema at '{location}' contains an unresolved $ref"),
        ));
    }
    // These keywords change object/array shape, conditional presence, or
    // reference resolution. Treating them as annotations would make static
    // path and assignability claims weaker than the runtime contract.
    for keyword in [
        "$dynamicRef",
        "$dynamicAnchor",
        "$recursiveRef",
        "$recursiveAnchor",
        "additionalItems",
        "definitions",
        "dependencies",
        "patternProperties",
        "propertyNames",
        "unevaluatedProperties",
        "dependentRequired",
        "minProperties",
        "maxProperties",
        "prefixItems",
        "unevaluatedItems",
    ] {
        if object.contains_key(keyword) {
            return Err(TypeError::new(
                SCHEMA_KEYWORD_UNSUPPORTED,
                format!(
                    "schema keyword '{keyword}' at '{location}' is outside the typed schema profile"
                ),
            ));
        }
    }
    for keyword in ["allOf", "not", "if", "then", "else", "dependentSchemas"] {
        if object.contains_key(keyword) {
            return Err(TypeError::new(
                SCHEMA_COMBINATOR_UNSUPPORTED,
                format!("schema keyword '{keyword}' at '{location}' is not supported"),
            ));
        }
    }

    let one_of = object.get("oneOf");
    let any_of = object.get("anyOf");
    if one_of.is_some() && any_of.is_some() {
        return Err(TypeError::new(
            SCHEMA_INVALID,
            format!("schema at '{location}' cannot define both oneOf and anyOf"),
        ));
    }
    if let Some(combinator) = one_of.or(any_of) {
        let incompatible_sibling = [
            "type",
            "enum",
            "const",
            "properties",
            "required",
            "additionalProperties",
            "items",
            "minItems",
        ]
        .into_iter()
        .find(|keyword| object.contains_key(*keyword));
        if let Some(keyword) = incompatible_sibling {
            return Err(TypeError::new(
                SCHEMA_COMBINATOR_UNSUPPORTED,
                format!(
                    "schema at '{location}' cannot combine oneOf/anyOf with shape keyword '{keyword}'"
                ),
            ));
        }
        let variants = combinator.as_array().ok_or_else(|| {
            TypeError::new(
                SCHEMA_INVALID,
                format!("oneOf/anyOf at '{location}' must be an array"),
            )
        })?;
        if variants.is_empty() {
            return Err(TypeError::new(
                SCHEMA_INVALID,
                format!("oneOf/anyOf at '{location}' must not be empty"),
            ));
        }
        return Ok(normalize_union(
            variants
                .iter()
                .enumerate()
                .map(|(index, variant)| {
                    compile_schema(variant, &format!("{location}.variant[{index}]"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ));
    }

    if object.contains_key("enum") && object.contains_key("const") {
        return Err(TypeError::new(
            SCHEMA_ENUM_INVALID,
            format!("schema at '{location}' cannot define both enum and const"),
        ));
    }

    let declared_types = declared_types(object, location)?;
    if let Some(values) = object.get("enum") {
        let values = values.as_array().ok_or_else(|| {
            TypeError::new(
                SCHEMA_ENUM_INVALID,
                format!("enum at '{location}' must be an array"),
            )
        })?;
        if values.is_empty() {
            return Err(TypeError::new(
                SCHEMA_ENUM_INVALID,
                format!("enum at '{location}' must not be empty"),
            ));
        }
        return compile_enum(values, declared_types.as_deref(), location);
    }
    if let Some(value) = object.get("const") {
        return compile_enum(
            std::slice::from_ref(value),
            declared_types.as_deref(),
            location,
        );
    }

    let inferred = if declared_types.is_none() {
        if object.contains_key("properties")
            || object.contains_key("required")
            || object.contains_key("additionalProperties")
        {
            Some(vec!["object".to_string()])
        } else if object.contains_key("items") || object.contains_key("minItems") {
            Some(vec!["array".to_string()])
        } else {
            None
        }
    } else {
        declared_types
    };

    let Some(types) = inferred else {
        return Ok(ValueType::Any);
    };
    let compiled = types
        .iter()
        .map(|kind| compile_named_type(kind, object, location))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(normalize_union(compiled))
}

fn declared_types(
    object: &Map<String, Value>,
    location: &str,
) -> Result<Option<Vec<String>>, TypeError> {
    let Some(value) = object.get("type") else {
        return Ok(None);
    };
    match value {
        Value::String(kind) => Ok(Some(vec![kind.clone()])),
        Value::Array(kinds) if !kinds.is_empty() => {
            let mut parsed = Vec::with_capacity(kinds.len());
            for kind in kinds {
                let kind = kind.as_str().ok_or_else(|| {
                    TypeError::new(
                        SCHEMA_INVALID,
                        format!("type array at '{location}' must contain only strings"),
                    )
                })?;
                if !parsed.iter().any(|existing| existing == kind) {
                    parsed.push(kind.to_string());
                }
            }
            Ok(Some(parsed))
        }
        _ => Err(TypeError::new(
            SCHEMA_INVALID,
            format!("type at '{location}' must be a string or non-empty string array"),
        )),
    }
}

fn compile_enum(
    values: &[Value],
    declared_types: Option<&[String]>,
    location: &str,
) -> Result<ValueType, TypeError> {
    let mut literals = Vec::with_capacity(values.len());
    for value in values {
        if value.is_array() || value.is_object() {
            return Err(TypeError::new(
                SCHEMA_ENUM_INVALID,
                format!("enum at '{location}' supports only scalar JSON values"),
            ));
        }
        let literal = ValueType::Literal(value.clone());
        if let Some(declared_types) = declared_types {
            let matches = declared_types.iter().any(|kind| {
                compile_named_type(kind, &Map::new(), location)
                    .is_ok_and(|declared| literal.is_assignable_to(&declared))
            });
            if !matches {
                return Err(TypeError::new(
                    SCHEMA_ENUM_INVALID,
                    format!("enum value at '{location}' does not match its declared type"),
                ));
            }
        }
        literals.push(literal);
    }
    Ok(normalize_union(literals))
}

fn compile_named_type(
    kind: &str,
    object: &Map<String, Value>,
    location: &str,
) -> Result<ValueType, TypeError> {
    match kind {
        "null" => Ok(ValueType::Null),
        "boolean" => Ok(ValueType::Boolean),
        "integer" => Ok(ValueType::Integer),
        "number" => Ok(ValueType::Number),
        "string" => Ok(ValueType::String),
        "array" => compile_array(object, location),
        "object" => compile_object(object, location),
        _ => Err(TypeError::new(
            SCHEMA_TYPE_UNSUPPORTED,
            format!("schema type '{kind}' at '{location}' is not supported"),
        )),
    }
}

fn compile_array(object: &Map<String, Value>, location: &str) -> Result<ValueType, TypeError> {
    let items = object.get("items").ok_or_else(|| {
        TypeError::new(
            SCHEMA_INVALID,
            format!("array schema at '{location}' must define items"),
        )
    })?;
    let min_items = match object.get("minItems") {
        Some(value) => value
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| {
                TypeError::new(
                    SCHEMA_INVALID,
                    format!("minItems at '{location}' must be a non-negative integer"),
                )
            })?,
        None => 0,
    };
    Ok(ValueType::Array(ArrayType {
        items: Box::new(compile_schema(items, &format!("{location}.items"))?),
        min_items,
    }))
}

fn compile_object(object: &Map<String, Value>, location: &str) -> Result<ValueType, TypeError> {
    let empty_properties = Map::new();
    let property_schemas = match object.get("properties") {
        Some(value) => value.as_object().ok_or_else(|| {
            TypeError::new(
                SCHEMA_INVALID,
                format!("properties at '{location}' must be an object"),
            )
        })?,
        None => &empty_properties,
    };

    let mut required = Vec::new();
    if let Some(values) = object.get("required") {
        let values = values.as_array().ok_or_else(|| {
            TypeError::new(
                SCHEMA_INVALID,
                format!("required at '{location}' must be an array"),
            )
        })?;
        for value in values {
            let value = value.as_str().ok_or_else(|| {
                TypeError::new(
                    SCHEMA_INVALID,
                    format!("required at '{location}' must contain only strings"),
                )
            })?;
            if required.iter().any(|required| required == value) {
                return Err(TypeError::new(
                    SCHEMA_INVALID,
                    format!("required property '{value}' at '{location}' is duplicated"),
                ));
            }
            required.push(value.to_string());
        }
    }

    let mut properties = BTreeMap::new();
    for (name, schema) in property_schemas {
        properties.insert(
            name.clone(),
            PropertyType {
                value_type: compile_schema(schema, &format!("{location}.properties.{name}"))?,
                required: required.contains(name),
            },
        );
    }
    if let Some(undeclared) = required
        .iter()
        .find(|property| !properties.contains_key(*property))
    {
        return Err(TypeError::new(
            SCHEMA_INVALID,
            format!(
                "required property '{undeclared}' at '{location}' has no declared property schema"
            ),
        ));
    }

    let additional_properties = match object.get("additionalProperties") {
        None | Some(Value::Bool(true)) => Some(Box::new(ValueType::Any)),
        Some(Value::Bool(false)) => None,
        Some(schema @ Value::Object(_)) => Some(Box::new(compile_schema(
            schema,
            &format!("{location}.additionalProperties"),
        )?)),
        Some(_) => {
            return Err(TypeError::new(
                SCHEMA_INVALID,
                format!("additionalProperties at '{location}' must be a boolean or schema object"),
            ))
        }
    };

    Ok(ValueType::Object(ObjectType {
        properties,
        additional_properties,
    }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticPath {
    canonical: String,
    /// JSON Pointer-decoded tokens. Their meaning is type-directed: object
    /// tokens are exact property names, while array tokens must be canonical
    /// non-negative decimal indexes.
    segments: Vec<String>,
}

impl StaticPath {
    /// Parses a slash-separated relative JSON Pointer suffix. `~0` and `~1`
    /// are decoded; a path that already came from `ValuePath::fields()` should
    /// instead use `from_decoded_segments` to avoid decoding twice.
    pub fn parse(value: impl Into<String>) -> Result<Self, TypeError> {
        let value = value.into();
        if value.len() > 512 {
            return Err(TypeError::new(
                TYPE_PATH_INVALID,
                "static type path must contain at most 512 bytes",
            ));
        }
        let segments = if value.is_empty() {
            Vec::new()
        } else {
            value
                .split('/')
                .map(|segment| decode_pointer_token(segment, &value))
                .collect::<Result<Vec<_>, _>>()?
        };
        Self::from_decoded_segments(segments)
    }

    /// Builds a static path from JSON Pointer-decoded `ValuePath` fields.
    /// Object keys are retained byte-for-byte, including dots, hyphens,
    /// slashes, empty strings, and numeric-looking keys.
    pub fn from_decoded_segments<I, S>(segments: I) -> Result<Self, TypeError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let segments = segments
            .into_iter()
            .map(|segment| segment.as_ref().to_string())
            .collect::<Vec<_>>();
        let canonical = segments
            .iter()
            .map(|segment| encode_pointer_token(segment))
            .collect::<Vec<_>>()
            .join("/");
        if canonical.len() > 512 {
            return Err(TypeError::new(
                TYPE_PATH_INVALID,
                "decoded static type path exceeds 512 encoded bytes",
            ));
        }
        Ok(Self {
            canonical,
            segments,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    pub fn segments(&self) -> &[String] {
        &self.segments
    }
}

fn decode_pointer_token(token: &str, path: &str) -> Result<String, TypeError> {
    let mut decoded = String::with_capacity(token.len());
    let mut characters = token.chars();
    while let Some(character) = characters.next() {
        if character != '~' {
            decoded.push(character);
            continue;
        }
        match characters.next() {
            Some('0') => decoded.push('~'),
            Some('1') => decoded.push('/'),
            _ => {
                return Err(TypeError::new(
                    TYPE_PATH_INVALID,
                    format!("static type path '{path}' contains an invalid JSON Pointer escape"),
                ))
            }
        }
    }
    Ok(decoded)
}

fn encode_pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

fn canonical_array_index(token: &str) -> Option<usize> {
    if token == "0" {
        return Some(0);
    }
    if token.is_empty()
        || token.starts_with('0')
        || !token.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    token.parse().ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PathPresence {
    Required,
    Optional,
    UnsafeUnion,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PathResolution {
    pub value_type: ValueType,
    pub presence: PathPresence,
}

impl ValueType {
    pub fn resolve_path(&self, path: &StaticPath) -> Result<PathResolution, TypeError> {
        let mut resolution = PathResolution {
            value_type: self.clone(),
            presence: PathPresence::Required,
        };
        for segment in path.segments() {
            let next = resolve_segment(&resolution.value_type, segment, path.as_str())?;
            resolution = PathResolution {
                value_type: next.value_type,
                presence: resolution.presence.max(next.presence),
            };
        }
        Ok(resolution)
    }

    pub fn resolve_path_str(&self, path: &str) -> Result<PathResolution, TypeError> {
        self.resolve_path(&StaticPath::parse(path)?)
    }

    /// Resolves JSON Pointer-decoded fields, such as those returned by
    /// `ValuePath::fields()`, without interpreting or decoding object keys.
    pub fn resolve_decoded_segments<I, S>(&self, segments: I) -> Result<PathResolution, TypeError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.resolve_path(&StaticPath::from_decoded_segments(segments)?)
    }

    pub fn require_path(&self, path: &StaticPath) -> Result<ValueType, TypeError> {
        let resolution = self.resolve_path(path)?;
        match resolution.presence {
            PathPresence::Required => Ok(resolution.value_type),
            PathPresence::Optional => Err(TypeError::new(
                TYPE_PATH_OPTIONAL_ACCESS,
                format!(
                    "path '{}' is not present for every valid value",
                    path.as_str()
                ),
            )),
            PathPresence::UnsafeUnion => Err(TypeError::new(
                TYPE_PATH_UNSAFE_UNION_ACCESS,
                format!(
                    "path '{}' is available only in some union variants",
                    path.as_str()
                ),
            )),
        }
    }

    pub fn require_path_str(&self, path: &str) -> Result<ValueType, TypeError> {
        let path = StaticPath::parse(path)?;
        self.require_path(&path)
    }

    /// Requires a path supplied as already-decoded fields to be present for
    /// every valid input value.
    pub fn require_decoded_segments<I, S>(&self, segments: I) -> Result<ValueType, TypeError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let path = StaticPath::from_decoded_segments(segments)?;
        self.require_path(&path)
    }

    pub fn narrow_discriminator(
        &self,
        field: &str,
        expected: &Value,
    ) -> Result<ValueType, TypeError> {
        if expected.is_array() || expected.is_object() {
            return Err(TypeError::new(
                TYPE_NARROWING_LITERAL_REQUIRED,
                "union discriminator narrowing requires a scalar JSON literal",
            ));
        }
        let ValueType::Union(variants) = self else {
            return Err(TypeError::new(
                TYPE_NARROWING_REQUIRES_UNION,
                "discriminator narrowing requires a union type",
            ));
        };

        let matches = variants
            .iter()
            .filter(|variant| variant_matches_discriminator(variant, field, expected))
            .cloned()
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return Err(TypeError::new(
                TYPE_NARROWING_NO_MATCH,
                format!("no union variant matches discriminator field '{field}'"),
            ));
        }
        Ok(normalize_union(matches))
    }

    pub fn is_assignable_to(&self, target: &ValueType) -> bool {
        if self == target || matches!(self, ValueType::Never) || matches!(target, ValueType::Any) {
            return true;
        }
        if matches!(self, ValueType::Any) || matches!(target, ValueType::Never) {
            return false;
        }
        if let ValueType::Union(variants) = self {
            return variants
                .iter()
                .all(|variant| variant.is_assignable_to(target));
        }
        if let ValueType::Union(variants) = target {
            return variants
                .iter()
                .any(|variant| self.is_assignable_to(variant));
        }

        match (self, target) {
            (ValueType::Integer, ValueType::Number) => true,
            (ValueType::Literal(value), target) => literal_type(value).is_assignable_to(target),
            (ValueType::Array(source), ValueType::Array(target)) => {
                source.min_items >= target.min_items && source.items.is_assignable_to(&target.items)
            }
            (ValueType::Object(source), ValueType::Object(target)) => {
                object_is_assignable(source, target)
            }
            _ => false,
        }
    }

    pub fn ensure_assignable_to(&self, target: &ValueType) -> Result<(), TypeError> {
        if self.is_assignable_to(target) {
            Ok(())
        } else {
            Err(TypeError::new(
                TYPE_NOT_ASSIGNABLE,
                format!(
                    "source type '{}' is not assignable to target type '{}'",
                    self.kind_name(),
                    target.kind_name()
                ),
            ))
        }
    }

    pub fn unify(types: impl IntoIterator<Item = ValueType>) -> Result<ValueType, TypeError> {
        let mut types = types.into_iter();
        let Some(first) = types.next() else {
            return Err(TypeError::new(
                TYPE_UNIFY_EMPTY,
                "cannot unify an empty type collection",
            ));
        };
        Ok(types.fold(first, unify_pair))
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::Any => "any",
            Self::Null => "null",
            Self::Boolean => "boolean",
            Self::Integer => "integer",
            Self::Number => "number",
            Self::String => "string",
            Self::Literal(_) => "literal",
            Self::Array(_) => "array",
            Self::Object(_) => "object",
            Self::Union(_) => "union",
        }
    }
}

fn resolve_segment(
    value_type: &ValueType,
    segment: &str,
    canonical: &str,
) -> Result<PathResolution, TypeError> {
    if let ValueType::Union(variants) = value_type {
        let mut resolved = Vec::new();
        let mut missing = false;
        for variant in variants {
            match resolve_segment(variant, segment, canonical) {
                Ok(value) => resolved.push(value),
                Err(_) => missing = true,
            }
        }
        if resolved.is_empty() {
            return Err(segment_error(value_type, segment, canonical));
        }
        let presence = if missing {
            PathPresence::UnsafeUnion
        } else {
            resolved
                .iter()
                .map(|value| value.presence)
                .max()
                .unwrap_or(PathPresence::Required)
        };
        return Ok(PathResolution {
            value_type: ValueType::unify(resolved.into_iter().map(|value| value.value_type))?,
            presence,
        });
    }

    match value_type {
        ValueType::Object(object) => {
            if let Some(property) = object.properties.get(segment) {
                Ok(PathResolution {
                    value_type: property.value_type.clone(),
                    presence: if property.required {
                        PathPresence::Required
                    } else {
                        PathPresence::Optional
                    },
                })
            } else if let Some(additional) = &object.additional_properties {
                Ok(PathResolution {
                    value_type: additional.as_ref().clone(),
                    presence: PathPresence::Optional,
                })
            } else {
                Err(TypeError::new(
                    TYPE_PATH_FIELD_NOT_FOUND,
                    format!("field '{segment}' does not exist while resolving path '{canonical}'"),
                ))
            }
        }
        ValueType::Array(array) => {
            let index = canonical_array_index(segment).ok_or_else(|| {
                TypeError::new(
                    TYPE_PATH_INVALID,
                    format!(
                        "array token '{segment}' in path '{canonical}' must be a canonical non-negative decimal index"
                    ),
                )
            })?;
            Ok(PathResolution {
                value_type: array.items.as_ref().clone(),
                presence: if index < array.min_items {
                    PathPresence::Required
                } else {
                    PathPresence::Optional
                },
            })
        }
        ValueType::Any => Ok(PathResolution {
            value_type: ValueType::Any,
            presence: PathPresence::Optional,
        }),
        _ => Err(segment_error(value_type, segment, canonical)),
    }
}

fn segment_error(value_type: &ValueType, segment: &str, canonical: &str) -> TypeError {
    TypeError::new(
        TYPE_PATH_EXPECTED_OBJECT,
        format!(
            "cannot resolve token '{segment}' from type '{}' while resolving path '{canonical}'",
            value_type.kind_name()
        ),
    )
}

fn variant_matches_discriminator(variant: &ValueType, field: &str, expected: &Value) -> bool {
    let ValueType::Object(object) = variant else {
        return false;
    };
    let Some(property) = object.properties.get(field) else {
        return false;
    };
    property.required && ValueType::Literal(expected.clone()).is_assignable_to(&property.value_type)
}

fn literal_type(value: &Value) -> ValueType {
    match value {
        Value::Null => ValueType::Null,
        Value::Bool(_) => ValueType::Boolean,
        Value::Number(number) if number.is_i64() || number.is_u64() => ValueType::Integer,
        Value::Number(_) => ValueType::Number,
        Value::String(_) => ValueType::String,
        Value::Array(_) | Value::Object(_) => ValueType::Literal(value.clone()),
    }
}

fn object_is_assignable(source: &ObjectType, target: &ObjectType) -> bool {
    for (name, target_property) in &target.properties {
        match source.properties.get(name) {
            Some(source_property) => {
                if target_property.required && !source_property.required {
                    return false;
                }
                if !source_property
                    .value_type
                    .is_assignable_to(&target_property.value_type)
                {
                    return false;
                }
            }
            None if target_property.required => return false,
            None => {
                if source
                    .additional_properties
                    .as_deref()
                    .is_some_and(|additional| {
                        !additional.is_assignable_to(&target_property.value_type)
                    })
                {
                    return false;
                }
            }
        }
    }

    for (name, source_property) in &source.properties {
        if target.properties.contains_key(name) {
            continue;
        }
        let Some(target_additional) = &target.additional_properties else {
            return false;
        };
        if !source_property
            .value_type
            .is_assignable_to(target_additional)
        {
            return false;
        }
    }

    match (&source.additional_properties, &target.additional_properties) {
        (Some(source), Some(target)) => source.is_assignable_to(target),
        (Some(_), None) => false,
        (None, _) => true,
    }
}

fn unify_pair(left: ValueType, right: ValueType) -> ValueType {
    if left.is_assignable_to(&right) {
        right
    } else if right.is_assignable_to(&left) {
        left
    } else {
        normalize_union(vec![left, right])
    }
}

fn normalize_union(types: Vec<ValueType>) -> ValueType {
    let mut flattened = Vec::new();
    for value_type in types {
        match value_type {
            ValueType::Never => {}
            ValueType::Any => return ValueType::Any,
            ValueType::Union(variants) => flattened.extend(variants),
            value_type => flattened.push(value_type),
        }
    }
    if flattened.is_empty() {
        return ValueType::Never;
    }

    let mut unique = Vec::new();
    for value_type in flattened {
        if !unique.contains(&value_type) {
            unique.push(value_type);
        }
    }
    let mut retained = Vec::new();
    for (index, candidate) in unique.iter().enumerate() {
        let subsumed = unique.iter().enumerate().any(|(other_index, other)| {
            index != other_index
                && candidate.is_assignable_to(other)
                && !other.is_assignable_to(candidate)
        });
        if !subsumed {
            retained.push(candidate.clone());
        }
    }
    match retained.len() {
        0 => ValueType::Never,
        1 => retained.pop().expect("one retained union type"),
        _ => ValueType::Union(retained),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        PathPresence, SchemaType, StaticPath, TypeError, ValueType, SCHEMA_COMBINATOR_UNSUPPORTED,
        SCHEMA_INVALID, SCHEMA_KEYWORD_UNSUPPORTED, TYPE_NARROWING_NO_MATCH, TYPE_NOT_ASSIGNABLE,
        TYPE_PATH_FIELD_NOT_FOUND, TYPE_PATH_INVALID, TYPE_PATH_OPTIONAL_ACCESS,
        TYPE_PATH_UNSAFE_UNION_ACCESS,
    };

    fn compile(schema: serde_json::Value) -> ValueType {
        SchemaType::compile(&schema).unwrap().into_value_type()
    }

    fn assert_code(result: Result<ValueType, TypeError>, expected: &str) {
        assert_eq!(result.unwrap_err().code(), expected);
    }

    #[test]
    fn resolves_required_optional_and_misspelled_object_fields() {
        let value_type = compile(json!({
            "type":"object",
            "required":["profile"],
            "properties":{
                "profile":{
                    "type":"object",
                    "required":["name"],
                    "properties":{
                        "name":{"type":"string"},
                        "nickname":{"type":"string"}
                    },
                    "additionalProperties":false
                }
            },
            "additionalProperties":false
        }));

        assert_eq!(
            value_type.require_path_str("profile/name").unwrap(),
            ValueType::String
        );
        let optional = value_type.resolve_path_str("profile/nickname").unwrap();
        assert_eq!(optional.presence, PathPresence::Optional);
        assert_code(
            value_type.require_path_str("profile/nickname"),
            TYPE_PATH_OPTIONAL_ACCESS,
        );
        assert_code(
            value_type.require_path_str("profile/nmae"),
            TYPE_PATH_FIELD_NOT_FOUND,
        );
    }

    #[test]
    fn preserves_arbitrary_object_keys_and_decoded_value_path_fields() {
        let value_type = compile(json!({
            "type":"object",
            "required":["display-name","nested/key","0"],
            "properties":{
                "display-name":{"type":"string"},
                "0":{"type":"boolean"},
                "nested/key":{
                    "type":"array",
                    "minItems":1,
                    "items":{
                        "type":"object",
                        "required":["display-name"],
                        "properties":{"display-name":{"type":"integer"}},
                        "additionalProperties":false
                    }
                }
            },
            "additionalProperties":false
        }));

        assert_eq!(
            value_type.require_path_str("display-name").unwrap(),
            ValueType::String
        );
        assert_eq!(
            value_type.require_decoded_segments(["0"]).unwrap(),
            ValueType::Boolean
        );

        let pointer_path = StaticPath::parse("nested~1key/0/display-name").unwrap();
        assert_eq!(
            pointer_path.segments(),
            &["nested/key", "0", "display-name"]
        );
        assert_eq!(
            value_type.require_path(&pointer_path).unwrap(),
            ValueType::Integer
        );

        let fields = vec![
            "nested/key".to_string(),
            "0".to_string(),
            "display-name".to_string(),
        ];
        assert_eq!(
            value_type.require_decoded_segments(&fields).unwrap(),
            ValueType::Integer
        );
        assert_eq!(
            StaticPath::from_decoded_segments(&fields).unwrap().as_str(),
            "nested~1key/0/display-name"
        );
        assert_eq!(
            StaticPath::parse("bad~2key").unwrap_err().code(),
            TYPE_PATH_INVALID
        );
    }

    #[test]
    fn resolves_fixed_array_indexes_and_tracks_min_items() {
        let value_type = compile(json!({
            "type":"object",
            "required":["items"],
            "properties":{
                "items":{
                    "type":"array",
                    "minItems":1,
                    "items":{
                        "type":"object",
                        "required":["id"],
                        "properties":{"id":{"type":"integer"}},
                        "additionalProperties":false
                    }
                }
            },
            "additionalProperties":false
        }));

        assert_eq!(
            value_type.require_path_str("items/0/id").unwrap(),
            ValueType::Integer
        );
        assert_eq!(
            value_type.resolve_path_str("items/1/id").unwrap().presence,
            PathPresence::Optional
        );
        for path in [
            "items/index/id",
            "items/*/id",
            "items/01/id",
            "items/+1/id",
            "items/-/id",
            "items/184467440737095516160/id",
        ] {
            assert_code(value_type.require_path_str(path), TYPE_PATH_INVALID);
        }
    }

    #[test]
    fn discriminated_union_requires_narrowing_before_variant_access() {
        let result_type = compile(json!({
            "oneOf":[
                {
                    "type":"object",
                    "required":["status","value"],
                    "properties":{
                        "status":{"enum":["ok"]},
                        "value":{
                            "type":"object",
                            "required":["answer"],
                            "properties":{"answer":{"type":"string"}},
                            "additionalProperties":false
                        }
                    },
                    "additionalProperties":false
                },
                {
                    "type":"object",
                    "required":["status","error"],
                    "properties":{
                        "status":{"enum":["error"]},
                        "error":{
                            "type":"object",
                            "required":["code"],
                            "properties":{"code":{"type":"string"}},
                            "additionalProperties":false
                        }
                    },
                    "additionalProperties":false
                }
            ]
        }));

        assert_eq!(
            result_type.resolve_path_str("status").unwrap().presence,
            PathPresence::Required
        );
        assert_code(
            result_type.require_path_str("value/answer"),
            TYPE_PATH_UNSAFE_UNION_ACCESS,
        );

        let ok = result_type
            .narrow_discriminator("status", &json!("ok"))
            .unwrap();
        assert_eq!(
            ok.require_path_str("value/answer").unwrap(),
            ValueType::String
        );
        assert_eq!(
            result_type
                .narrow_discriminator("status", &json!("missing"))
                .unwrap_err()
                .code(),
            TYPE_NARROWING_NO_MATCH
        );
    }

    #[test]
    fn assignability_is_directional_and_respects_required_properties() {
        assert!(ValueType::Integer.is_assignable_to(&ValueType::Number));
        assert!(!ValueType::Number.is_assignable_to(&ValueType::Integer));
        assert_eq!(
            ValueType::Number
                .ensure_assignable_to(&ValueType::Integer)
                .unwrap_err()
                .code(),
            TYPE_NOT_ASSIGNABLE
        );

        let required = compile(json!({
            "type":"object",
            "required":["answer"],
            "properties":{"answer":{"type":"string"}},
            "additionalProperties":false
        }));
        let optional = compile(json!({
            "type":"object",
            "properties":{"answer":{"type":"string"}},
            "additionalProperties":false
        }));
        assert!(required.is_assignable_to(&optional));
        assert!(!optional.is_assignable_to(&required));

        let open_source = compile(json!({
            "type":"object",
            "additionalProperties":true
        }));
        let typed_optional_target = compile(json!({
            "type":"object",
            "properties":{"answer":{"type":"string"}},
            "additionalProperties":true
        }));
        assert!(
            !open_source.is_assignable_to(&typed_optional_target),
            "an open source may provide the target's optional named field with the wrong type"
        );
    }

    #[test]
    fn unify_selects_safe_supertype_or_preserves_a_union() {
        assert_eq!(
            ValueType::unify([ValueType::Integer, ValueType::Number]).unwrap(),
            ValueType::Number
        );
        let heterogeneous = ValueType::unify([ValueType::String, ValueType::Boolean]).unwrap();
        assert!(matches!(heterogeneous, ValueType::Union(ref values) if values.len() == 2));
    }

    #[test]
    fn rejects_invalid_schema_shapes_with_stable_codes() {
        for schema in [
            json!({"type":"array"}),
            json!({"type":"object","required":["missing"],"properties":{}}),
            json!({"oneOf":[]}),
        ] {
            assert_eq!(
                SchemaType::compile(&schema).unwrap_err().code(),
                SCHEMA_INVALID
            );
        }
    }

    #[test]
    fn rejects_shape_keywords_outside_the_typed_schema_profile() {
        for schema in [
            json!({"$dynamicRef":"#node"}),
            json!({"$dynamicAnchor":"node"}),
            json!({"$recursiveRef":"#"}),
            json!({"type":"object","propertyNames":{"pattern":"^x"}}),
            json!({"type":"object","patternProperties":{"^x":{"type":"string"}}}),
            json!({"type":"object","unevaluatedProperties":false}),
            json!({"type":"object","dependentRequired":{"a":["b"]}}),
            json!({"type":"object","minProperties":1}),
            json!({"type":"array","prefixItems":[{"type":"string"}]}),
            json!({"type":"array","items":{"type":"string"},"unevaluatedItems":false}),
        ] {
            assert_eq!(
                SchemaType::compile(&schema).unwrap_err().code(),
                SCHEMA_KEYWORD_UNSUPPORTED
            );
        }
    }

    #[test]
    fn rejects_unmodeled_combinators_and_shape_changing_union_siblings() {
        for schema in [
            json!({"allOf":[{"type":"string"}]}),
            json!({"not":{"type":"null"}}),
            json!({"if":{"type":"string"}}),
            json!({"then":{"type":"string"}}),
            json!({"else":{"type":"string"}}),
            json!({"dependentSchemas":{"a":{"required":["b"]}}}),
            json!({
                "oneOf":[{"type":"object"},{"type":"null"}],
                "properties":{"answer":{"type":"string"}}
            }),
        ] {
            assert_eq!(
                SchemaType::compile(&schema).unwrap_err().code(),
                SCHEMA_COMBINATOR_UNSUPPORTED
            );
        }
    }
}
