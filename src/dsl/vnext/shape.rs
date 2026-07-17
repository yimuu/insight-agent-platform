use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use serde_json::{Map, Value};

use super::types::ValueType;

pub const SCHEMA_SHAPE_INVALID: &str = "VNEXT_SCHEMA_SHAPE_INVALID";
pub const SCHEMA_SHAPE_UNSUPPORTED: &str = "VNEXT_SCHEMA_SHAPE_UNSUPPORTED";
pub const SCHEMA_SHAPE_PATH_INVALID: &str = "VNEXT_SCHEMA_SHAPE_PATH_INVALID";
pub const SCHEMA_SHAPE_PATH_FIELD_NOT_FOUND: &str = "VNEXT_SCHEMA_SHAPE_PATH_FIELD_NOT_FOUND";
pub const SCHEMA_SHAPE_PATH_EXPECTED_CONTAINER: &str = "VNEXT_SCHEMA_SHAPE_PATH_EXPECTED_CONTAINER";
pub const SCHEMA_SHAPE_PATH_OPTIONAL_ACCESS: &str = "VNEXT_SCHEMA_SHAPE_PATH_OPTIONAL_ACCESS";
pub const SCHEMA_SHAPE_PATH_UNSAFE_UNION_ACCESS: &str =
    "VNEXT_SCHEMA_SHAPE_PATH_UNSAFE_UNION_ACCESS";
pub const DYNAMIC_MESSAGE_SHAPE_INVALID: &str = "VNEXT_DYNAMIC_MESSAGE_SHAPE_INVALID";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShapeError {
    code: &'static str,
    message: String,
}

impl ShapeError {
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

impl fmt::Display for ShapeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ShapeError {}

#[derive(Debug, Clone, PartialEq)]
pub struct ShapeProperty {
    pub shape: SchemaShape,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectShape {
    pub properties: BTreeMap<String, ShapeProperty>,
    /// `None` is a closed object. `Some(Any)` is JSON Schema's default open
    /// object contract.
    pub additional_properties: Option<Box<SchemaShape>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArrayShape {
    pub items: Box<SchemaShape>,
}

/// A value-refinement-free structural view of an expanded JSON Schema.
///
/// Unlike `ValueType`, this representation deliberately does not retain
/// `minLength`, `minItems`, formats, patterns, or other runtime refinements.
/// It does retain closed objects, required properties, unions, literal
/// discriminators, and array item shapes so correlated message unions remain
/// provable.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaShape {
    Never,
    Any,
    Null,
    Boolean,
    Integer,
    Number,
    String,
    Literal(Value),
    Array(ArrayShape),
    Object(ObjectShape),
    Union(Vec<SchemaShape>),
}

impl SchemaShape {
    /// Compiles an already expanded JSON Schema into its structural shape.
    /// Unresolved `$ref` and structural combinators outside this profile are
    /// rejected instead of being approximated unsafely.
    pub fn compile(schema: &Value) -> Result<Self, ShapeError> {
        compile_shape(schema, "$")
    }

    /// Reconstructs the structural facts retained by one SSA `ValueType`.
    /// Runtime refinements such as `ArrayType::min_items` intentionally do
    /// not participate in this representation or dynamic-message proof.
    pub fn from_value_type(value_type: &ValueType) -> Self {
        match value_type {
            ValueType::Never => Self::Never,
            ValueType::Any => Self::Any,
            ValueType::Null => Self::Null,
            ValueType::Boolean => Self::Boolean,
            ValueType::Integer => Self::Integer,
            ValueType::Number => Self::Number,
            ValueType::String => Self::String,
            ValueType::Literal(value) => literal_shape(value),
            ValueType::Array(array) => Self::Array(ArrayShape {
                items: Box::new(Self::from_value_type(&array.items)),
            }),
            ValueType::Object(object) => Self::Object(ObjectShape {
                properties: object
                    .properties
                    .iter()
                    .map(|(name, property)| {
                        (
                            name.clone(),
                            ShapeProperty {
                                shape: Self::from_value_type(&property.value_type),
                                required: property.required,
                            },
                        )
                    })
                    .collect(),
                additional_properties: object
                    .additional_properties
                    .as_ref()
                    .map(|shape| Box::new(Self::from_value_type(shape))),
            }),
            ValueType::Union(variants) => normalize_union(
                variants
                    .iter()
                    .map(Self::from_value_type)
                    .collect::<Vec<_>>(),
            ),
        }
    }

    /// Resolves already JSON-Pointer-decoded path segments and requires the
    /// complete path to exist for every value admitted by this shape.
    pub fn resolve_path(&self, segments: &[String]) -> Result<SchemaShape, ShapeError> {
        let mut resolution = ShapePathResolution {
            shape: self.clone(),
            presence: ShapePathPresence::Required,
        };
        for segment in segments {
            let next = resolve_shape_segment(&resolution.shape, segment)?;
            resolution = ShapePathResolution {
                shape: next.shape,
                presence: resolution.presence.max(next.presence),
            };
        }
        match resolution.presence {
            ShapePathPresence::Required => Ok(resolution.shape),
            ShapePathPresence::Optional => Err(ShapeError::new(
                SCHEMA_SHAPE_PATH_OPTIONAL_ACCESS,
                "schema shape path is not present for every valid value",
            )),
            ShapePathPresence::UnsafeUnion => Err(ShapeError::new(
                SCHEMA_SHAPE_PATH_UNSAFE_UNION_ACCESS,
                "schema shape path is available only in some union variants",
            )),
        }
    }

    pub fn is_assignable_to(&self, target: &SchemaShape) -> bool {
        if self == target || matches!(self, Self::Never) || matches!(target, Self::Any) {
            return true;
        }
        if matches!(self, Self::Any) || matches!(target, Self::Never) {
            return false;
        }
        if let Self::Union(variants) = self {
            return variants
                .iter()
                .all(|variant| variant.is_assignable_to(target));
        }
        if let Self::Union(variants) = target {
            return variants
                .iter()
                .any(|variant| self.is_assignable_to(variant));
        }

        match (self, target) {
            (Self::Integer, Self::Number) => true,
            (Self::Literal(value), target) => scalar_literal_kind(value).is_assignable_to(target),
            (Self::Array(source), Self::Array(target)) => {
                source.items.is_assignable_to(&target.items)
            }
            (Self::Object(source), Self::Object(target)) => object_is_assignable(source, target),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ShapePathPresence {
    Required,
    Optional,
    UnsafeUnion,
}

#[derive(Debug, Clone, PartialEq)]
struct ShapePathResolution {
    shape: SchemaShape,
    presence: ShapePathPresence,
}

/// Evidence saved in a compiled dynamic message source. The verifier must
/// recompute this proof from the source shape rather than trusting serialized
/// input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DynamicMessageShapeProof {
    pub requires_vision: bool,
}

pub fn prove_dynamic_message_array(
    source: &SchemaShape,
) -> Result<DynamicMessageShapeProof, ShapeError> {
    let canonical = canonical_dynamic_message_array_shape();
    if !source.is_assignable_to(&canonical) {
        return Err(ShapeError::new(
            DYNAMIC_MESSAGE_SHAPE_INVALID,
            "source schema shape is not assignable to DynamicMessage[]",
        ));
    }

    let requires_vision = match source {
        SchemaShape::Never => false,
        SchemaShape::Array(array) => message_shape_allows_image(&array.items),
        _ => false,
    };
    Ok(DynamicMessageShapeProof { requires_vision })
}

fn compile_shape(schema: &Value, location: &str) -> Result<SchemaShape, ShapeError> {
    match schema {
        Value::Bool(true) => Ok(SchemaShape::Any),
        Value::Bool(false) => Ok(SchemaShape::Never),
        Value::Object(object) => compile_shape_object(object, location),
        _ => Err(ShapeError::new(
            SCHEMA_SHAPE_INVALID,
            format!("schema at '{location}' must be an object or boolean"),
        )),
    }
}

fn compile_shape_object(
    object: &Map<String, Value>,
    location: &str,
) -> Result<SchemaShape, ShapeError> {
    if object.contains_key("$ref") {
        return Err(ShapeError::new(
            SCHEMA_SHAPE_UNSUPPORTED,
            format!("schema at '{location}' contains an unresolved $ref"),
        ));
    }

    for keyword in [
        "$dynamicRef",
        "$recursiveRef",
        "additionalItems",
        "allOf",
        "dependencies",
        "dependentSchemas",
        "if",
        "not",
        "patternProperties",
        "prefixItems",
        "propertyNames",
        "then",
        "else",
        "unevaluatedItems",
        "unevaluatedProperties",
    ] {
        if object.contains_key(keyword) {
            return Err(ShapeError::new(
                SCHEMA_SHAPE_UNSUPPORTED,
                format!(
                    "schema keyword '{keyword}' at '{location}' is outside the structural shape profile"
                ),
            ));
        }
    }

    let one_of = object.get("oneOf");
    let any_of = object.get("anyOf");
    if one_of.is_some() && any_of.is_some() {
        return Err(ShapeError::new(
            SCHEMA_SHAPE_INVALID,
            format!("schema at '{location}' cannot define both oneOf and anyOf"),
        ));
    }
    if let Some(combinator) = one_of.or(any_of) {
        if let Some(keyword) = [
            "type",
            "enum",
            "const",
            "properties",
            "required",
            "additionalProperties",
            "items",
        ]
        .into_iter()
        .find(|keyword| object.contains_key(*keyword))
        {
            return Err(ShapeError::new(
                SCHEMA_SHAPE_UNSUPPORTED,
                format!(
                    "schema at '{location}' cannot combine oneOf/anyOf with structural keyword '{keyword}'"
                ),
            ));
        }
        let variants = combinator.as_array().ok_or_else(|| {
            ShapeError::new(
                SCHEMA_SHAPE_INVALID,
                format!("oneOf/anyOf at '{location}' must be an array"),
            )
        })?;
        if variants.is_empty() {
            return Err(ShapeError::new(
                SCHEMA_SHAPE_INVALID,
                format!("oneOf/anyOf at '{location}' must not be empty"),
            ));
        }
        return Ok(normalize_union(
            variants
                .iter()
                .enumerate()
                .map(|(index, variant)| {
                    compile_shape(variant, &format!("{location}.variant[{index}]"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ));
    }

    if object.contains_key("enum") && object.contains_key("const") {
        return Err(ShapeError::new(
            SCHEMA_SHAPE_INVALID,
            format!("schema at '{location}' cannot define both enum and const"),
        ));
    }

    let declared = declared_types(object, location)?;
    if let Some(value) = object.get("const") {
        let shape = literal_shape(value);
        ensure_literal_matches_declared(&shape, declared.as_deref(), location)?;
        return Ok(shape);
    }
    if let Some(values) = object.get("enum") {
        let values = values.as_array().ok_or_else(|| {
            ShapeError::new(
                SCHEMA_SHAPE_INVALID,
                format!("enum at '{location}' must be an array"),
            )
        })?;
        if values.is_empty() {
            return Err(ShapeError::new(
                SCHEMA_SHAPE_INVALID,
                format!("enum at '{location}' must not be empty"),
            ));
        }
        let shapes = values
            .iter()
            .map(|value| {
                let shape = literal_shape(value);
                ensure_literal_matches_declared(&shape, declared.as_deref(), location)?;
                Ok(shape)
            })
            .collect::<Result<Vec<_>, ShapeError>>()?;
        return Ok(normalize_union(shapes));
    }

    let inferred = if declared.is_some() {
        declared
    } else if object.contains_key("properties")
        || object.contains_key("required")
        || object.contains_key("additionalProperties")
    {
        Some(vec!["object".to_string()])
    } else if object.contains_key("items") || object.contains_key("minItems") {
        Some(vec!["array".to_string()])
    } else {
        None
    };

    let Some(types) = inferred else {
        return Ok(SchemaShape::Any);
    };
    Ok(normalize_union(
        types
            .iter()
            .map(|kind| compile_named_shape(kind, object, location))
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn declared_types(
    object: &Map<String, Value>,
    location: &str,
) -> Result<Option<Vec<String>>, ShapeError> {
    let Some(value) = object.get("type") else {
        return Ok(None);
    };
    match value {
        Value::String(kind) => Ok(Some(vec![kind.clone()])),
        Value::Array(kinds) if !kinds.is_empty() => {
            let mut parsed = Vec::with_capacity(kinds.len());
            for kind in kinds {
                let kind = kind.as_str().ok_or_else(|| {
                    ShapeError::new(
                        SCHEMA_SHAPE_INVALID,
                        format!("type array at '{location}' must contain only strings"),
                    )
                })?;
                if !parsed.iter().any(|existing| existing == kind) {
                    parsed.push(kind.to_string());
                }
            }
            Ok(Some(parsed))
        }
        _ => Err(ShapeError::new(
            SCHEMA_SHAPE_INVALID,
            format!("type at '{location}' must be a string or non-empty string array"),
        )),
    }
}

fn ensure_literal_matches_declared(
    shape: &SchemaShape,
    declared: Option<&[String]>,
    location: &str,
) -> Result<(), ShapeError> {
    let Some(declared) = declared else {
        return Ok(());
    };
    let matches = declared.iter().any(|kind| {
        compile_named_shape(kind, &Map::new(), location)
            .is_ok_and(|target| shape.is_assignable_to(&target))
    });
    if matches {
        Ok(())
    } else {
        Err(ShapeError::new(
            SCHEMA_SHAPE_INVALID,
            format!("enum/const value at '{location}' does not match its declared type"),
        ))
    }
}

fn compile_named_shape(
    kind: &str,
    object: &Map<String, Value>,
    location: &str,
) -> Result<SchemaShape, ShapeError> {
    match kind {
        "null" => Ok(SchemaShape::Null),
        "boolean" => Ok(SchemaShape::Boolean),
        "integer" => Ok(SchemaShape::Integer),
        "number" => Ok(SchemaShape::Number),
        "string" => Ok(SchemaShape::String),
        "array" => compile_array_shape(object, location),
        "object" => compile_object_shape(object, location),
        _ => Err(ShapeError::new(
            SCHEMA_SHAPE_INVALID,
            format!("schema type '{kind}' at '{location}' is not supported"),
        )),
    }
}

fn compile_array_shape(
    object: &Map<String, Value>,
    location: &str,
) -> Result<SchemaShape, ShapeError> {
    let items = object
        .get("items")
        .map(|items| compile_shape(items, &format!("{location}.items")))
        .transpose()?
        .unwrap_or(SchemaShape::Any);
    Ok(SchemaShape::Array(ArrayShape {
        items: Box::new(items),
    }))
}

fn compile_object_shape(
    object: &Map<String, Value>,
    location: &str,
) -> Result<SchemaShape, ShapeError> {
    let empty_properties = Map::new();
    let property_schemas = match object.get("properties") {
        Some(value) => value.as_object().ok_or_else(|| {
            ShapeError::new(
                SCHEMA_SHAPE_INVALID,
                format!("properties at '{location}' must be an object"),
            )
        })?,
        None => &empty_properties,
    };

    let mut required = BTreeSet::new();
    if let Some(values) = object.get("required") {
        let values = values.as_array().ok_or_else(|| {
            ShapeError::new(
                SCHEMA_SHAPE_INVALID,
                format!("required at '{location}' must be an array"),
            )
        })?;
        for value in values {
            let name = value.as_str().ok_or_else(|| {
                ShapeError::new(
                    SCHEMA_SHAPE_INVALID,
                    format!("required at '{location}' must contain only strings"),
                )
            })?;
            if !required.insert(name.to_string()) {
                return Err(ShapeError::new(
                    SCHEMA_SHAPE_INVALID,
                    format!("required property at '{location}' is duplicated"),
                ));
            }
        }
    }

    let mut properties = BTreeMap::new();
    for (name, schema) in property_schemas {
        properties.insert(
            name.clone(),
            ShapeProperty {
                shape: compile_shape(schema, &format!("{location}.properties.{name}"))?,
                required: required.contains(name),
            },
        );
    }
    if required
        .iter()
        .any(|required| !properties.contains_key(required))
    {
        return Err(ShapeError::new(
            SCHEMA_SHAPE_UNSUPPORTED,
            format!("required properties at '{location}' must have explicit property schemas"),
        ));
    }

    let additional_properties = match object.get("additionalProperties") {
        None | Some(Value::Bool(true)) => Some(Box::new(SchemaShape::Any)),
        Some(Value::Bool(false)) => None,
        Some(schema @ Value::Object(_)) => Some(Box::new(compile_shape(
            schema,
            &format!("{location}.additionalProperties"),
        )?)),
        Some(_) => {
            return Err(ShapeError::new(
                SCHEMA_SHAPE_INVALID,
                format!("additionalProperties at '{location}' must be a boolean or schema"),
            ))
        }
    };

    Ok(SchemaShape::Object(ObjectShape {
        properties,
        additional_properties,
    }))
}

fn literal_shape(value: &Value) -> SchemaShape {
    match value {
        Value::Null => SchemaShape::Null,
        Value::Bool(_) | Value::Number(_) | Value::String(_) => SchemaShape::Literal(value.clone()),
        Value::Array(values) => {
            let items = if values.is_empty() {
                SchemaShape::Never
            } else {
                normalize_union(values.iter().map(literal_shape).collect())
            };
            SchemaShape::Array(ArrayShape {
                items: Box::new(items),
            })
        }
        Value::Object(values) => SchemaShape::Object(ObjectShape {
            properties: values
                .iter()
                .map(|(name, value)| {
                    (
                        name.clone(),
                        ShapeProperty {
                            shape: literal_shape(value),
                            required: true,
                        },
                    )
                })
                .collect(),
            additional_properties: None,
        }),
    }
}

fn scalar_literal_kind(value: &Value) -> SchemaShape {
    match value {
        Value::Null => SchemaShape::Null,
        Value::Bool(_) => SchemaShape::Boolean,
        Value::Number(number) if number.is_i64() || number.is_u64() => SchemaShape::Integer,
        Value::Number(_) => SchemaShape::Number,
        Value::String(_) => SchemaShape::String,
        Value::Array(_) | Value::Object(_) => literal_shape(value),
    }
}

fn resolve_shape_segment(
    shape: &SchemaShape,
    segment: &str,
) -> Result<ShapePathResolution, ShapeError> {
    if let SchemaShape::Union(variants) = shape {
        let mut resolved = Vec::new();
        let mut missing = false;
        for variant in variants {
            match resolve_shape_segment(variant, segment) {
                Ok(value) => resolved.push(value),
                Err(_) => missing = true,
            }
        }
        if resolved.is_empty() {
            return Err(ShapeError::new(
                SCHEMA_SHAPE_PATH_EXPECTED_CONTAINER,
                "schema shape path cannot be resolved through any union variant",
            ));
        }
        let presence = if missing {
            ShapePathPresence::UnsafeUnion
        } else {
            resolved
                .iter()
                .map(|value| value.presence)
                .max()
                .unwrap_or(ShapePathPresence::Required)
        };
        return Ok(ShapePathResolution {
            shape: normalize_union(resolved.into_iter().map(|value| value.shape).collect()),
            presence,
        });
    }

    match shape {
        SchemaShape::Object(object) => {
            if let Some(property) = object.properties.get(segment) {
                Ok(ShapePathResolution {
                    shape: property.shape.clone(),
                    presence: if property.required {
                        ShapePathPresence::Required
                    } else {
                        ShapePathPresence::Optional
                    },
                })
            } else if let Some(additional) = &object.additional_properties {
                Ok(ShapePathResolution {
                    shape: additional.as_ref().clone(),
                    presence: ShapePathPresence::Optional,
                })
            } else {
                Err(ShapeError::new(
                    SCHEMA_SHAPE_PATH_FIELD_NOT_FOUND,
                    "field does not exist in the closed schema shape",
                ))
            }
        }
        SchemaShape::Array(array) => {
            if canonical_array_index(segment).is_none() {
                return Err(ShapeError::new(
                    SCHEMA_SHAPE_PATH_INVALID,
                    "array shape paths require a canonical non-negative decimal index",
                ));
            }
            Ok(ShapePathResolution {
                shape: array.items.as_ref().clone(),
                // SchemaShape intentionally erases minItems, so no fixed index
                // can be proven present using structural facts alone.
                presence: ShapePathPresence::Optional,
            })
        }
        SchemaShape::Any => Ok(ShapePathResolution {
            shape: SchemaShape::Any,
            presence: ShapePathPresence::Optional,
        }),
        SchemaShape::Never => Ok(ShapePathResolution {
            shape: SchemaShape::Never,
            presence: ShapePathPresence::Required,
        }),
        _ => Err(ShapeError::new(
            SCHEMA_SHAPE_PATH_EXPECTED_CONTAINER,
            "schema shape path requires an object or array container",
        )),
    }
}

fn canonical_array_index(segment: &str) -> Option<usize> {
    if segment.is_empty()
        || (segment.len() > 1 && segment.starts_with('0'))
        || !segment.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    segment.parse().ok()
}

fn object_is_assignable(source: &ObjectShape, target: &ObjectShape) -> bool {
    for (name, target_property) in &target.properties {
        match source.properties.get(name) {
            Some(source_property) => {
                if target_property.required && !source_property.required {
                    return false;
                }
                if !source_property
                    .shape
                    .is_assignable_to(&target_property.shape)
                {
                    return false;
                }
            }
            None if target_property.required => return false,
            None => {
                if source
                    .additional_properties
                    .as_deref()
                    .is_some_and(|additional| !additional.is_assignable_to(&target_property.shape))
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
        if !source_property.shape.is_assignable_to(target_additional) {
            return false;
        }
    }

    match (&source.additional_properties, &target.additional_properties) {
        (Some(source), Some(target)) => source.is_assignable_to(target),
        (Some(_), None) => false,
        (None, _) => true,
    }
}

fn normalize_union(shapes: Vec<SchemaShape>) -> SchemaShape {
    let mut flattened = Vec::new();
    for shape in shapes {
        match shape {
            SchemaShape::Never => {}
            SchemaShape::Any => return SchemaShape::Any,
            SchemaShape::Union(variants) => flattened.extend(variants),
            shape => flattened.push(shape),
        }
    }
    if flattened.is_empty() {
        return SchemaShape::Never;
    }

    let mut unique = Vec::new();
    for shape in flattened {
        if !unique.contains(&shape) {
            unique.push(shape);
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
        0 => SchemaShape::Never,
        1 => retained.pop().expect("one retained shape"),
        _ => SchemaShape::Union(retained),
    }
}

fn property(shape: SchemaShape) -> ShapeProperty {
    ShapeProperty {
        shape,
        required: true,
    }
}

fn closed_object<const N: usize>(fields: [(&str, SchemaShape); N]) -> SchemaShape {
    SchemaShape::Object(ObjectShape {
        properties: fields
            .into_iter()
            .map(|(name, shape)| (name.to_string(), property(shape)))
            .collect(),
        additional_properties: None,
    })
}

fn text_part_shape() -> SchemaShape {
    closed_object([("text", SchemaShape::String)])
}

fn image_part_shape() -> SchemaShape {
    closed_object([("image", SchemaShape::String)])
}

fn user_message_shape() -> SchemaShape {
    let part = normalize_union(vec![text_part_shape(), image_part_shape()]);
    closed_object([
        (
            "role",
            SchemaShape::Literal(Value::String("user".to_string())),
        ),
        (
            "content",
            normalize_union(vec![
                SchemaShape::String,
                SchemaShape::Array(ArrayShape {
                    items: Box::new(part),
                }),
            ]),
        ),
    ])
}

fn assistant_message_shape() -> SchemaShape {
    closed_object([
        (
            "role",
            SchemaShape::Literal(Value::String("assistant".to_string())),
        ),
        (
            "content",
            normalize_union(vec![
                SchemaShape::String,
                SchemaShape::Array(ArrayShape {
                    items: Box::new(text_part_shape()),
                }),
            ]),
        ),
    ])
}

fn canonical_dynamic_message_array_shape() -> SchemaShape {
    SchemaShape::Array(ArrayShape {
        items: Box::new(normalize_union(vec![
            user_message_shape(),
            assistant_message_shape(),
        ])),
    })
}

fn message_shape_allows_image(shape: &SchemaShape) -> bool {
    any_reachable_variant(shape, &|message| {
        if !message.is_assignable_to(&user_message_shape()) {
            return false;
        }
        let SchemaShape::Object(message) = message else {
            return false;
        };
        message
            .properties
            .get("content")
            .is_some_and(|content| content_shape_allows_image(&content.shape))
    })
}

fn content_shape_allows_image(shape: &SchemaShape) -> bool {
    any_reachable_variant(shape, &|content| {
        let SchemaShape::Array(parts) = content else {
            return false;
        };
        any_reachable_variant(&parts.items, &|part| {
            !matches!(part, SchemaShape::Never) && part.is_assignable_to(&image_part_shape())
        })
    })
}

fn any_reachable_variant(shape: &SchemaShape, predicate: &impl Fn(&SchemaShape) -> bool) -> bool {
    match shape {
        SchemaShape::Never => false,
        SchemaShape::Union(variants) => variants
            .iter()
            .any(|variant| any_reachable_variant(variant, predicate)),
        shape => predicate(shape),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use crate::dsl::vnext::types::SchemaType;

    use super::{
        prove_dynamic_message_array, ArrayShape, DynamicMessageShapeProof, SchemaShape,
        DYNAMIC_MESSAGE_SHAPE_INVALID, SCHEMA_SHAPE_PATH_FIELD_NOT_FOUND,
        SCHEMA_SHAPE_PATH_INVALID, SCHEMA_SHAPE_PATH_OPTIONAL_ACCESS,
        SCHEMA_SHAPE_PATH_UNSAFE_UNION_ACCESS,
    };

    fn compile(schema: Value) -> SchemaShape {
        SchemaShape::compile(&schema).unwrap()
    }

    fn message(role: &str, content: Value) -> Value {
        json!({
            "type":"object",
            "required":["role","content"],
            "properties":{
                "role":{"const":role},
                "content":content
            },
            "additionalProperties":false
        })
    }

    fn text_part() -> Value {
        json!({
            "type":"object",
            "required":["text"],
            "properties":{"text":{"type":"string"}},
            "additionalProperties":false
        })
    }

    fn image_part() -> Value {
        json!({
            "type":"object",
            "required":["image"],
            "properties":{"image":{"type":"string"}},
            "additionalProperties":false
        })
    }

    fn messages(items: Value) -> SchemaShape {
        compile(json!({"type":"array","items":items}))
    }

    #[test]
    fn ignores_value_refinements_but_preserves_closed_correlated_unions() {
        let shape = messages(json!({
            "oneOf":[
                message("user", json!({
                    "oneOf":[
                        {"type":"string","minLength":1,"pattern":".+"},
                        {"type":"array","minItems":1,"items":image_part()}
                    ]
                })),
                message("assistant", json!({
                    "type":"array",
                    "minItems":1,
                    "items":text_part()
                }))
            ]
        }));

        assert_eq!(
            prove_dynamic_message_array(&shape).unwrap(),
            DynamicMessageShapeProof {
                requires_vision: true
            }
        );
    }

    #[test]
    fn narrower_text_only_and_user_only_shapes_are_valid_without_nominal_markers() {
        let assistant_text = messages(message("assistant", json!({"type":"string"})));
        assert_eq!(
            prove_dynamic_message_array(&assistant_text).unwrap(),
            DynamicMessageShapeProof {
                requires_vision: false
            }
        );

        let user_text_parts =
            messages(message("user", json!({"type":"array","items":text_part()})));
        assert!(
            !prove_dynamic_message_array(&user_text_parts)
                .unwrap()
                .requires_vision
        );
    }

    #[test]
    fn rejects_open_nullable_any_and_unknown_role_sources() {
        let cases = [
            json!({"type":"array"}),
            json!({"type":"array","items":true}),
            json!({"type":"array","items":{"type":["object","null"]}}),
            json!({
                "type":"array",
                "items":{
                    "type":"object",
                    "required":["role","content"],
                    "properties":{
                        "role":{"const":"tool"},
                        "content":{"type":"string"}
                    }
                }
            }),
            json!({"type":"array","items":{"type":"array","items":text_part()}}),
        ];

        for schema in cases {
            let error = prove_dynamic_message_array(&compile(schema)).unwrap_err();
            assert_eq!(error.code(), DYNAMIC_MESSAGE_SHAPE_INVALID);
        }
    }

    #[test]
    fn rejects_missing_fields_and_open_content_parts() {
        let missing_content = messages(json!({
            "type":"object",
            "required":["role"],
            "properties":{"role":{"const":"user"}},
            "additionalProperties":false
        }));
        assert!(prove_dynamic_message_array(&missing_content).is_err());

        let open_part = messages(message(
            "user",
            json!({
                "type":"array",
                "items":{
                    "type":"object",
                    "required":["text"],
                    "properties":{"text":{"type":"string"}}
                }
            }),
        ));
        assert!(prove_dynamic_message_array(&open_part).is_err());
    }

    #[test]
    fn rejects_uncorrelated_role_union_with_images_and_assistant_images() {
        let uncorrelated = messages(json!({
            "type":"object",
            "required":["role","content"],
            "properties":{
                "role":{"enum":["user","assistant"]},
                "content":{"type":"array","items":image_part()}
            },
            "additionalProperties":false
        }));
        assert!(prove_dynamic_message_array(&uncorrelated).is_err());

        let assistant_image = messages(message(
            "assistant",
            json!({"type":"array","items":image_part()}),
        ));
        assert!(prove_dynamic_message_array(&assistant_image).is_err());
    }

    #[test]
    fn empty_array_bottom_shape_is_a_valid_non_vision_subtype() {
        let shape = SchemaShape::Array(ArrayShape {
            items: Box::new(SchemaShape::Never),
        });
        assert_eq!(
            prove_dynamic_message_array(&shape).unwrap(),
            DynamicMessageShapeProof {
                requires_vision: false
            }
        );
    }

    #[test]
    fn resolves_only_required_object_paths_and_keeps_union_results_correlated() {
        let input = compile(json!({
            "type":"object",
            "required":["history"],
            "properties":{
                "history":{
                    "type":"array",
                    "items":{
                        "oneOf":[
                            message("user", json!({"type":"string"})),
                            message("assistant", json!({"type":"string"}))
                        ]
                    }
                },
                "optional_note":{"type":"string"}
            },
            "additionalProperties":false
        }));

        let history = input.resolve_path(&["history".to_string()]).unwrap();
        assert!(matches!(history, SchemaShape::Array(_)));
        assert_eq!(
            input
                .resolve_path(&["optional_note".to_string()])
                .unwrap_err()
                .code(),
            SCHEMA_SHAPE_PATH_OPTIONAL_ACCESS
        );
        assert_eq!(
            input
                .resolve_path(&["missing".to_string()])
                .unwrap_err()
                .code(),
            SCHEMA_SHAPE_PATH_FIELD_NOT_FOUND
        );

        let messages = match history {
            SchemaShape::Array(array) => array.items,
            _ => unreachable!(),
        };
        assert!(matches!(
            messages.resolve_path(&["content".to_string()]).unwrap(),
            SchemaShape::String
        ));
    }

    #[test]
    fn path_resolution_rejects_union_gaps_and_unproven_array_indexes() {
        let union_with_gap = compile(json!({
            "oneOf":[
                message("user", json!({"type":"string"})),
                {
                    "type":"object",
                    "required":["role"],
                    "properties":{"role":{"const":"assistant"}},
                    "additionalProperties":false
                }
            ]
        }));
        assert_eq!(
            union_with_gap
                .resolve_path(&["content".to_string()])
                .unwrap_err()
                .code(),
            SCHEMA_SHAPE_PATH_UNSAFE_UNION_ACCESS
        );

        let array = compile(json!({
            "type":"array",
            "minItems":10,
            "items":{"type":"string"}
        }));
        assert_eq!(
            array.resolve_path(&["0".to_string()]).unwrap_err().code(),
            SCHEMA_SHAPE_PATH_OPTIONAL_ACCESS
        );
        assert_eq!(
            array.resolve_path(&["01".to_string()]).unwrap_err().code(),
            SCHEMA_SHAPE_PATH_INVALID
        );
    }

    #[test]
    fn value_type_roundtrip_preserves_correlated_message_shape_and_proof() {
        let schema = json!({
            "type":"array",
            "minItems":7,
            "items":{
                "oneOf":[
                    message("user", json!({
                        "oneOf":[
                            {"type":"string","minLength":1},
                            {"type":"array","minItems":1,"items":image_part()}
                        ]
                    })),
                    message("assistant", json!({
                        "type":"array",
                        "minItems":3,
                        "items":text_part()
                    }))
                ]
            }
        });
        let direct = SchemaShape::compile(&schema).unwrap();
        let value_type = SchemaType::compile(&schema).unwrap().into_value_type();
        let reconstructed = SchemaShape::from_value_type(&value_type);

        assert_eq!(reconstructed, direct);
        assert_eq!(
            prove_dynamic_message_array(&reconstructed).unwrap(),
            DynamicMessageShapeProof {
                requires_vision: true
            }
        );
    }
}
