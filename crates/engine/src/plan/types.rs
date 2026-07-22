use std::{collections::BTreeMap, error::Error, fmt};

use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

pub const PLAN_TYPE_WIRE_VERSION: u32 = 2;
pub const PLAN_TYPE_REGEX_ENGINE_VERSION: &str = "regex-1.13.0";
pub const PLAN_TYPE_UNION_EMPTY: &str = "ENGINE_PLAN_TYPE_UNION_EMPTY";
pub const PLAN_TYPE_WIRE_INVALID: &str = "ENGINE_PLAN_TYPE_WIRE_INVALID";
pub const PLAN_TYPE_CANONICALIZATION_FAILED: &str = "ENGINE_PLAN_TYPE_CANONICALIZATION_FAILED";
pub const PLAN_TYPE_JSON_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

const MAX_SAFE_JSON_INTEGER: u64 = (1_u64 << 53) - 1;
const MAX_REGEX_PATTERN_BYTES: usize = 4 * 1024;
const MAX_REGEX_COMPILED_BYTES: usize = 1024 * 1024;
const PLAN_TYPE_HASH_DOMAIN: &[u8] = b"insight.plan_type.v2\0";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanTypeError {
    code: &'static str,
    message: String,
}

impl PlanTypeError {
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

impl fmt::Display for PlanTypeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for PlanTypeError {}

/// A closed, immutable type algebra for Canonical Typed Plan ports and values.
///
/// `StringRefined` and `ArrayBounded` are the normalized in-memory forms for
/// refinements added by the v2 wire contract. The existing `String` and
/// `Array` variants remain the unique canonical forms for an unconstrained
/// string and an array without an upper bound, respectively. On the wire both
/// pairs use the natural `string`/`array` tags and carry every constraint
/// explicitly; their Rust-only variant names never leak into a Plan document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanType {
    Never,
    Any,
    Null,
    Boolean,
    Integer,
    Number,
    String,
    StringRefined {
        min_length: u64,
        max_length: Option<u64>,
        pattern: Option<String>,
        enum_values: Option<Vec<Value>>,
    },
    Literal {
        value: Value,
    },
    Array {
        items: Box<PlanType>,
        min_items: u64,
    },
    ArrayBounded {
        items: Box<PlanType>,
        min_items: u64,
        max_items: u64,
    },
    Object {
        properties: BTreeMap<String, PlanProperty>,
        /// `None` is a closed object. `Some(Any)` accepts arbitrary additional
        /// values; any other type constrains them.
        additional_properties: Option<Box<PlanType>>,
    },
    Union {
        variants: Vec<PlanType>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlanProperty {
    pub value_type: PlanType,
    pub required: bool,
}

impl PlanProperty {
    pub fn new(value_type: PlanType, required: bool) -> Result<Self, PlanTypeError> {
        Ok(Self {
            value_type: value_type.normalize()?,
            required,
        })
    }

    fn normalize(self) -> Result<Self, PlanTypeError> {
        Self::new(self.value_type, self.required)
    }
}

impl<'de> Deserialize<'de> for PlanProperty {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            value_type: PlanType,
            required: bool,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.value_type, wire.required).map_err(serde::de::Error::custom)
    }
}

/// A nullable field which is nevertheless required to be present on the wire.
/// This distinguishes the v2 contract from silently defaulting an omitted v1
/// field to `None`.
#[derive(Debug, Clone)]
struct RequiredNullable<T> {
    present: bool,
    value: Option<T>,
}

impl<T> RequiredNullable<T> {
    fn present(value: Option<T>) -> Self {
        Self {
            present: true,
            value,
        }
    }

    fn into_required(self, label: &str) -> Result<Option<T>, PlanTypeError> {
        if !self.present {
            return Err(PlanTypeError::new(
                PLAN_TYPE_WIRE_INVALID,
                format!("v2 plan type wire is missing required field '{label}'"),
            ));
        }
        Ok(self.value)
    }
}

impl<T> Default for RequiredNullable<T> {
    fn default() -> Self {
        Self {
            present: false,
            value: None,
        }
    }
}

impl<T> Serialize for RequiredNullable<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.value.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for RequiredNullable<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::present(Option::<T>::deserialize(deserializer)?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum PlanTypeWire {
    Never {},
    Any {},
    Null {},
    Boolean {},
    Integer {},
    Number {},
    String {
        min_length: u64,
        #[serde(default)]
        max_length: RequiredNullable<u64>,
        #[serde(default)]
        pattern: RequiredNullable<String>,
        #[serde(rename = "enum", default)]
        enum_values: RequiredNullable<Vec<Value>>,
    },
    Literal {
        value: Value,
    },
    Array {
        items: Box<PlanType>,
        min_items: u64,
        #[serde(default)]
        max_items: RequiredNullable<u64>,
    },
    Object {
        properties: BTreeMap<String, PlanProperty>,
        #[serde(default)]
        additional_properties: RequiredNullable<Box<PlanType>>,
    },
    Union {
        variants: Vec<PlanType>,
    },
}

impl Serialize for PlanType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let normalized = self
            .normalized()
            .map_err(<S::Error as serde::ser::Error>::custom)?;
        let wire = match normalized {
            Self::Never => PlanTypeWire::Never {},
            Self::Any => PlanTypeWire::Any {},
            Self::Null => PlanTypeWire::Null {},
            Self::Boolean => PlanTypeWire::Boolean {},
            Self::Integer => PlanTypeWire::Integer {},
            Self::Number => PlanTypeWire::Number {},
            Self::String => PlanTypeWire::String {
                min_length: 0,
                max_length: RequiredNullable::present(None),
                pattern: RequiredNullable::present(None),
                enum_values: RequiredNullable::present(None),
            },
            Self::StringRefined {
                min_length,
                max_length,
                pattern,
                enum_values,
            } => PlanTypeWire::String {
                min_length,
                max_length: RequiredNullable::present(max_length),
                pattern: RequiredNullable::present(pattern),
                enum_values: RequiredNullable::present(enum_values),
            },
            Self::Literal { value } => PlanTypeWire::Literal { value },
            Self::Array { items, min_items } => PlanTypeWire::Array {
                items,
                min_items,
                max_items: RequiredNullable::present(None),
            },
            Self::ArrayBounded {
                items,
                min_items,
                max_items,
            } => PlanTypeWire::Array {
                items,
                min_items,
                max_items: RequiredNullable::present(Some(max_items)),
            },
            Self::Object {
                properties,
                additional_properties,
            } => PlanTypeWire::Object {
                properties,
                additional_properties: RequiredNullable::present(additional_properties),
            },
            Self::Union { variants } => PlanTypeWire::Union { variants },
        };
        wire.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PlanType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PlanTypeWire::deserialize(deserializer)?;
        let value: Result<Self, PlanTypeError> = match wire {
            PlanTypeWire::Never {} => Ok(Self::Never),
            PlanTypeWire::Any {} => Ok(Self::Any),
            PlanTypeWire::Null {} => Ok(Self::Null),
            PlanTypeWire::Boolean {} => Ok(Self::Boolean),
            PlanTypeWire::Integer {} => Ok(Self::Integer),
            PlanTypeWire::Number {} => Ok(Self::Number),
            PlanTypeWire::String {
                min_length,
                max_length,
                pattern,
                enum_values,
            } => (|| -> Result<Self, PlanTypeError> {
                Self::string(
                    min_length,
                    max_length.into_required("max_length")?,
                    pattern.into_required("pattern")?,
                    enum_values.into_required("enum")?,
                )
            })(),
            PlanTypeWire::Literal { value } => Self::literal(value),
            PlanTypeWire::Array {
                items,
                min_items,
                max_items,
            } => (|| -> Result<Self, PlanTypeError> {
                Self::array(*items, min_items, max_items.into_required("max_items")?)
            })(),
            PlanTypeWire::Object {
                properties,
                additional_properties,
            } => (|| -> Result<Self, PlanTypeError> {
                Self::Object {
                    properties,
                    additional_properties: additional_properties
                        .into_required("additional_properties")?,
                }
                .normalize()
            })(),
            PlanTypeWire::Union { variants } => Self::Union { variants }.normalize(),
        };
        value.map_err(serde::de::Error::custom)
    }
}

impl PlanType {
    /// Canonical runtime contract for the only workflow failure value that
    /// Catch and `all_settled` may turn into authored data.
    pub fn safe_error() -> Result<Self, PlanTypeError> {
        Ok(Self::Object {
            properties: BTreeMap::from([
                ("code".to_owned(), PlanProperty::new(Self::String, true)?),
                (
                    "kind".to_owned(),
                    PlanProperty::new(
                        Self::literal(Value::String("safe_error".to_owned()))?,
                        true,
                    )?,
                ),
                ("message".to_owned(), PlanProperty::new(Self::String, true)?),
            ]),
            additional_properties: None,
        })
    }

    /// Constructs a canonical string contract. `enum_values` are JSON values
    /// so forged non-string values can be rejected rather than coerced.
    pub fn string(
        min_length: u64,
        max_length: Option<u64>,
        pattern: Option<String>,
        enum_values: Option<Vec<Value>>,
    ) -> Result<Self, PlanTypeError> {
        Self::StringRefined {
            min_length,
            max_length,
            pattern,
            enum_values,
        }
        .normalize()
    }

    /// Constructs a canonical array contract with optional upper bound.
    pub fn array(
        items: PlanType,
        min_items: u64,
        max_items: Option<u64>,
    ) -> Result<Self, PlanTypeError> {
        match max_items {
            Some(max_items) => Self::ArrayBounded {
                items: Box::new(items),
                min_items,
                max_items,
            },
            None => Self::Array {
                items: Box::new(items),
                min_items,
            },
        }
        .normalize()
    }

    pub fn literal(value: Value) -> Result<Self, PlanTypeError> {
        Self::Literal { value }.normalize()
    }

    /// Constructs the least union supertype and returns it in canonical form.
    /// Empty input is rejected; use `Never` when the empty type is intended.
    pub fn union(variants: impl IntoIterator<Item = PlanType>) -> Result<Self, PlanTypeError> {
        let variants = variants.into_iter().collect::<Vec<_>>();
        if variants.is_empty() {
            return Err(empty_union_error());
        }
        Self::Union { variants }.normalize()
    }

    pub fn unify(types: impl IntoIterator<Item = PlanType>) -> Result<Self, PlanTypeError> {
        Self::union(types)
    }

    pub fn normalized(&self) -> Result<Self, PlanTypeError> {
        self.clone().normalize()
    }

    /// Projects the canonical Plan type into an equivalent Draft 2020-12
    /// schema. This is the only schema used by API admission and structured
    /// worker adapters; PlanType remains the execution authority.
    pub fn json_schema(&self) -> Result<Value, PlanTypeError> {
        let normalized = self.normalized()?;
        Ok(json_schema_for(&normalized))
    }

    /// Self-contained root schema document suitable for publication and
    /// `jsonschema` compilation.
    pub fn json_schema_document(&self) -> Result<Value, PlanTypeError> {
        let schema = self.json_schema()?;
        Ok(match schema {
            Value::Object(mut object) => {
                object.insert(
                    "$schema".to_owned(),
                    Value::String(PLAN_TYPE_JSON_SCHEMA_DIALECT.to_owned()),
                );
                Value::Object(object)
            }
            Value::Bool(value) => serde_json::json!({
                "$schema": PLAN_TYPE_JSON_SCHEMA_DIALECT,
                "allOf": [value]
            }),
            _ => unreachable!("PlanType schema projection returns only object or boolean"),
        })
    }

    pub fn normalize(self) -> Result<Self, PlanTypeError> {
        match self {
            Self::Never
            | Self::Any
            | Self::Null
            | Self::Boolean
            | Self::Integer
            | Self::Number
            | Self::String => Ok(self),
            Self::StringRefined {
                min_length,
                max_length,
                pattern,
                enum_values,
            } => normalize_string(min_length, max_length, pattern, enum_values),
            Self::Literal { value } => Ok(Self::Literal {
                value: normalize_literal(value)?,
            }),
            Self::Array { items, min_items } => normalize_array(*items, min_items, None),
            Self::ArrayBounded {
                items,
                min_items,
                max_items,
            } => normalize_array(*items, min_items, Some(max_items)),
            Self::Object {
                properties,
                additional_properties,
            } => {
                let mut normalized_properties = BTreeMap::new();
                for (name, property) in properties {
                    let property = property.normalize()?;
                    if property.value_type == Self::Never {
                        if property.required {
                            return Ok(Self::Never);
                        }
                        continue;
                    }
                    normalized_properties.insert(name, property);
                }
                let additional_properties = match additional_properties {
                    Some(value_type) => match value_type.normalize()? {
                        Self::Never => None,
                        value_type => Some(Box::new(value_type)),
                    },
                    None => None,
                };
                Ok(Self::Object {
                    properties: normalized_properties,
                    additional_properties,
                })
            }
            Self::Union { variants } => normalize_union(variants),
        }
    }

    /// Returns the canonical string refinements for string types.
    #[allow(clippy::type_complexity)]
    pub fn string_constraints(&self) -> Option<(u64, Option<u64>, Option<&str>, Option<&[Value]>)> {
        match self {
            Self::String => Some((0, None, None, None)),
            Self::StringRefined {
                min_length,
                max_length,
                pattern,
                enum_values,
            } => Some((
                *min_length,
                *max_length,
                pattern.as_deref(),
                enum_values.as_deref(),
            )),
            _ => None,
        }
    }

    /// Returns item type and cardinality bounds for array types.
    pub fn array_constraints(&self) -> Option<(&PlanType, u64, Option<u64>)> {
        match self {
            Self::Array { items, min_items } => Some((items, *min_items, None)),
            Self::ArrayBounded {
                items,
                min_items,
                max_items,
            } => Some((items, *min_items, Some(*max_items))),
            _ => None,
        }
    }

    /// Validates a concrete runtime JSON value against this closed type
    /// contract. An invalid/forged contract is an error; a well-formed value
    /// outside the contract returns `Ok(false)`.
    pub fn accepts_literal(&self, value: &Value) -> Result<bool, PlanTypeError> {
        let target = self.normalized()?;
        let literal = Self::literal(value.clone())?;
        Ok(literal.is_assignable_normalized_to(&target))
    }

    /// Directional assignment: every value accepted by `self` must also be
    /// accepted by `target`. Invalid raw enum construction fails closed.
    pub fn is_assignable_to(&self, target: &Self) -> bool {
        let (Ok(source), Ok(target)) = (self.normalized(), target.normalized()) else {
            return false;
        };
        source.is_assignable_normalized_to(&target)
    }

    fn is_assignable_normalized_to(&self, target: &Self) -> bool {
        if self == target || matches!(self, Self::Never) || matches!(target, Self::Any) {
            return true;
        }
        if matches!(self, Self::Any) || matches!(target, Self::Never) {
            return false;
        }
        if let Self::Union { variants } = self {
            return variants
                .iter()
                .all(|variant| variant.is_assignable_normalized_to(target));
        }
        if let Self::Union { variants } = target {
            if let Some(values) = finite_string_values(self) {
                return values.iter().all(|value| {
                    variants.iter().any(|variant| {
                        Self::Literal {
                            value: value.clone(),
                        }
                        .is_assignable_normalized_to(variant)
                    })
                });
            }
            return variants
                .iter()
                .any(|variant| self.is_assignable_normalized_to(variant));
        }

        if let (Some(source), Some(target)) = (string_contract(self), string_contract(target)) {
            return string_is_assignable(source, target);
        }
        if let (Some(source), Some(target)) = (array_contract(self), array_contract(target)) {
            return source.min_items >= target.min_items
                && upper_bound_is_narrower(source.max_items, target.max_items)
                && source.items.is_assignable_normalized_to(target.items);
        }

        match (self, target) {
            (Self::Integer, Self::Number) => true,
            (Self::Literal { value }, target) => literal_is_assignable_to(value, target),
            (
                Self::Object {
                    properties: source_properties,
                    additional_properties: source_additional,
                },
                Self::Object {
                    properties: target_properties,
                    additional_properties: target_additional,
                },
            ) => object_is_assignable(
                source_properties,
                source_additional.as_deref(),
                target_properties,
                target_additional.as_deref(),
            ),
            _ => false,
        }
    }

    /// RFC 8785 bytes of the recursively normalized v2 wire representation.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, PlanTypeError> {
        let normalized = self.normalized()?;
        canonical_wire_bytes(&normalized)
    }

    /// Domain-separated SHA-256 of `canonical_bytes`, encoded as
    /// `sha256:<lowercase hex>`.
    pub fn canonical_hash(&self) -> Result<String, PlanTypeError> {
        let bytes = self.canonical_bytes()?;
        let mut hasher = Sha256::new();
        hasher.update(PLAN_TYPE_HASH_DOMAIN);
        hasher.update(bytes);
        let digest = hasher.finalize();
        let mut encoded = String::with_capacity("sha256:".len() + digest.len() * 2);
        encoded.push_str("sha256:");
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in digest {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        Ok(encoded)
    }
}

fn json_schema_for(value_type: &PlanType) -> Value {
    match value_type {
        PlanType::Never => Value::Bool(false),
        PlanType::Any => Value::Bool(true),
        PlanType::Null => serde_json::json!({"type": "null"}),
        PlanType::Boolean => serde_json::json!({"type": "boolean"}),
        PlanType::Integer => serde_json::json!({"type": "integer"}),
        PlanType::Number => serde_json::json!({"type": "number"}),
        PlanType::String => serde_json::json!({"type": "string"}),
        PlanType::StringRefined {
            min_length,
            max_length,
            pattern,
            enum_values,
        } => {
            let mut schema = Map::from_iter([
                ("type".to_owned(), Value::String("string".to_owned())),
                ("minLength".to_owned(), Value::Number((*min_length).into())),
            ]);
            if let Some(max_length) = max_length {
                schema.insert("maxLength".to_owned(), Value::Number((*max_length).into()));
            }
            if let Some(pattern) = pattern {
                schema.insert("pattern".to_owned(), Value::String(pattern.clone()));
                schema.insert(
                    "x-insight-regex-engine".to_owned(),
                    Value::String(PLAN_TYPE_REGEX_ENGINE_VERSION.to_owned()),
                );
            }
            if let Some(values) = enum_values {
                schema.insert("enum".to_owned(), Value::Array(values.clone()));
            }
            Value::Object(schema)
        }
        PlanType::Literal { value } => serde_json::json!({"const": value}),
        PlanType::Array { items, min_items } => serde_json::json!({
            "type": "array",
            "items": json_schema_for(items),
            "minItems": min_items,
        }),
        PlanType::ArrayBounded {
            items,
            min_items,
            max_items,
        } => serde_json::json!({
            "type": "array",
            "items": json_schema_for(items),
            "minItems": min_items,
            "maxItems": max_items,
        }),
        PlanType::Object {
            properties,
            additional_properties,
        } => {
            let property_schemas = properties
                .iter()
                .map(|(name, property)| (name.clone(), json_schema_for(&property.value_type)))
                .collect::<Map<_, _>>();
            let required = properties
                .iter()
                .filter(|(_, property)| property.required)
                .map(|(name, _)| Value::String(name.clone()))
                .collect::<Vec<_>>();
            let mut schema = Map::from_iter([
                ("type".to_owned(), Value::String("object".to_owned())),
                ("properties".to_owned(), Value::Object(property_schemas)),
                ("required".to_owned(), Value::Array(required)),
                (
                    "additionalProperties".to_owned(),
                    additional_properties
                        .as_deref()
                        .map(json_schema_for)
                        .unwrap_or(Value::Bool(false)),
                ),
            ]);
            // `required: []` is valid but omitting it produces a smaller public
            // schema without changing semantics.
            if schema.get("required") == Some(&Value::Array(Vec::new())) {
                schema.remove("required");
            }
            Value::Object(schema)
        }
        PlanType::Union { variants } => serde_json::json!({
            "anyOf": variants.iter().map(json_schema_for).collect::<Vec<_>>()
        }),
    }
}

#[derive(Clone, Copy)]
struct StringContract<'a> {
    min_length: u64,
    max_length: Option<u64>,
    pattern: Option<&'a str>,
    enum_values: Option<&'a [Value]>,
}

#[derive(Clone, Copy)]
struct ArrayContract<'a> {
    items: &'a PlanType,
    min_items: u64,
    max_items: Option<u64>,
}

fn string_contract(value: &PlanType) -> Option<StringContract<'_>> {
    let (min_length, max_length, pattern, enum_values) = value.string_constraints()?;
    Some(StringContract {
        min_length,
        max_length,
        pattern,
        enum_values,
    })
}

fn array_contract(value: &PlanType) -> Option<ArrayContract<'_>> {
    let (items, min_items, max_items) = value.array_constraints()?;
    Some(ArrayContract {
        items,
        min_items,
        max_items,
    })
}

fn empty_union_error() -> PlanTypeError {
    PlanTypeError::new(
        PLAN_TYPE_UNION_EMPTY,
        "plan type Union must contain at least one variant; use Never for the empty type",
    )
}

fn validate_safe_bound(value: u64, label: &str) -> Result<(), PlanTypeError> {
    if value > MAX_SAFE_JSON_INTEGER {
        return Err(PlanTypeError::new(
            PLAN_TYPE_WIRE_INVALID,
            format!("{label} exceeds the canonical JSON safe-integer range"),
        ));
    }
    Ok(())
}

fn compile_pattern(pattern: &str) -> Result<Regex, PlanTypeError> {
    if pattern.len() > MAX_REGEX_PATTERN_BYTES {
        return Err(PlanTypeError::new(
            PLAN_TYPE_WIRE_INVALID,
            "string pattern exceeds the bounded regex source size",
        ));
    }
    RegexBuilder::new(pattern)
        .size_limit(MAX_REGEX_COMPILED_BYTES)
        .build()
        .map_err(|_| {
            PlanTypeError::new(
                PLAN_TYPE_WIRE_INVALID,
                "string pattern is not a valid bounded Rust regex",
            )
        })
}

fn normalize_string(
    min_length: u64,
    max_length: Option<u64>,
    pattern: Option<String>,
    enum_values: Option<Vec<Value>>,
) -> Result<PlanType, PlanTypeError> {
    validate_safe_bound(min_length, "string min_length")?;
    if let Some(max_length) = max_length {
        validate_safe_bound(max_length, "string max_length")?;
        if min_length > max_length {
            return Err(PlanTypeError::new(
                PLAN_TYPE_WIRE_INVALID,
                "string min_length must not exceed max_length",
            ));
        }
    }

    let pattern = pattern.filter(|value| !value.is_empty());
    let compiled_pattern = pattern.as_deref().map(compile_pattern).transpose()?;
    let enum_values = match enum_values {
        Some(values) => {
            let count = u64::try_from(values.len()).unwrap_or(u64::MAX);
            validate_safe_bound(count, "string enum length")?;
            if values.is_empty() {
                return Ok(PlanType::Never);
            }
            let mut keyed = Vec::with_capacity(values.len());
            for value in values {
                let value = normalize_literal(value)?;
                let Some(text) = value.as_str() else {
                    return Err(PlanTypeError::new(
                        PLAN_TYPE_WIRE_INVALID,
                        "string enum contains a non-string value",
                    ));
                };
                if !string_value_matches(text, min_length, max_length, compiled_pattern.as_ref()) {
                    return Err(PlanTypeError::new(
                        PLAN_TYPE_WIRE_INVALID,
                        "string enum value violates its enclosing string constraints",
                    ));
                }
                keyed.push((canonical_value_bytes(&value)?, value));
            }
            keyed.sort_by(|left, right| left.0.cmp(&right.0));
            keyed.dedup_by(|left, right| left.0 == right.0);
            Some(
                keyed
                    .into_iter()
                    .map(|(_, value)| value)
                    .collect::<Vec<_>>(),
            )
        }
        None => None,
    };

    if min_length == 0 && max_length.is_none() && pattern.is_none() && enum_values.is_none() {
        Ok(PlanType::String)
    } else {
        Ok(PlanType::StringRefined {
            min_length,
            max_length,
            pattern,
            enum_values,
        })
    }
}

fn normalize_array(
    items: PlanType,
    min_items: u64,
    max_items: Option<u64>,
) -> Result<PlanType, PlanTypeError> {
    validate_safe_bound(min_items, "array min_items")?;
    if let Some(max_items) = max_items {
        validate_safe_bound(max_items, "array max_items")?;
        if min_items > max_items {
            return Err(PlanTypeError::new(
                PLAN_TYPE_WIRE_INVALID,
                "array min_items must not exceed max_items",
            ));
        }
    }
    let items = items.normalize()?;
    if min_items > 0 && items == PlanType::Never {
        return Ok(PlanType::Never);
    }
    // With an uninhabited item type only the empty array exists, regardless of
    // a redundant upper bound. Keep one canonical representation for it.
    if items == PlanType::Never {
        return Ok(PlanType::Array {
            items: Box::new(items),
            min_items: 0,
        });
    }
    match max_items {
        Some(max_items) => Ok(PlanType::ArrayBounded {
            items: Box::new(items),
            min_items,
            max_items,
        }),
        None => Ok(PlanType::Array {
            items: Box::new(items),
            min_items,
        }),
    }
}

fn validate_literal(value: &Value) -> Result<(), PlanTypeError> {
    match value {
        Value::Number(number) => {
            let unsafe_unsigned = number
                .as_u64()
                .is_some_and(|value| value > MAX_SAFE_JSON_INTEGER);
            let unsafe_signed = number.as_i64().is_some_and(|value| {
                value < -(MAX_SAFE_JSON_INTEGER as i64) || value > MAX_SAFE_JSON_INTEGER as i64
            });
            if unsafe_unsigned || unsafe_signed {
                return Err(PlanTypeError::new(
                    PLAN_TYPE_WIRE_INVALID,
                    "literal integer exceeds the canonical JSON safe-integer range",
                ));
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_literal(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_literal(value)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::String(_) => {}
    }
    Ok(())
}

fn normalize_literal(value: Value) -> Result<Value, PlanTypeError> {
    validate_literal(&value)?;
    let bytes = canonical_value_bytes(&value)?;
    let canonical = serde_json::from_slice(&bytes).map_err(|error| {
        PlanTypeError::new(
            PLAN_TYPE_CANONICALIZATION_FAILED,
            format!("failed to decode canonical plan literal: {error}"),
        )
    })?;
    validate_literal(&canonical)?;
    Ok(canonical)
}

fn canonical_value_bytes(value: &Value) -> Result<Vec<u8>, PlanTypeError> {
    serde_jcs::to_vec(value).map_err(|error| {
        PlanTypeError::new(
            PLAN_TYPE_CANONICALIZATION_FAILED,
            format!("failed to canonicalize plan literal: {error}"),
        )
    })
}

fn normalize_union(variants: Vec<PlanType>) -> Result<PlanType, PlanTypeError> {
    if variants.is_empty() {
        return Err(empty_union_error());
    }

    let mut flattened = Vec::new();
    for variant in variants {
        push_union_variant(variant, &mut flattened)?;
    }
    if flattened.iter().any(|variant| variant == &PlanType::Any) {
        return Ok(PlanType::Any);
    }
    flattened.retain(|variant| variant != &PlanType::Never);
    if flattened.is_empty() {
        return Ok(PlanType::Never);
    }

    let mut keyed = flattened
        .into_iter()
        .map(|variant| Ok((canonical_wire_bytes(&variant)?, variant)))
        .collect::<Result<Vec<_>, PlanTypeError>>()?;
    keyed.sort_by(|left, right| left.0.cmp(&right.0));
    keyed.dedup_by(|left, right| left.0 == right.0);
    let unique = keyed
        .into_iter()
        .map(|(_, variant)| variant)
        .collect::<Vec<_>>();

    let mut retained = Vec::new();
    for (index, candidate) in unique.iter().enumerate() {
        let strictly_subsumed = unique.iter().enumerate().any(|(other_index, other)| {
            index != other_index
                && candidate.is_assignable_normalized_to(other)
                && !other.is_assignable_normalized_to(candidate)
        });
        if !strictly_subsumed {
            retained.push(candidate.clone());
        }
    }

    match retained.len() {
        0 => Ok(PlanType::Never),
        1 => Ok(retained.pop().expect("one retained plan type")),
        _ => Ok(PlanType::Union { variants: retained }),
    }
}

fn push_union_variant(
    variant: PlanType,
    flattened: &mut Vec<PlanType>,
) -> Result<(), PlanTypeError> {
    match variant {
        PlanType::Union { variants } => {
            if variants.is_empty() {
                return Err(empty_union_error());
            }
            for variant in variants {
                push_union_variant(variant, flattened)?;
            }
        }
        variant => flattened.push(variant.normalize()?),
    }
    Ok(())
}

fn canonical_wire_bytes(value_type: &PlanType) -> Result<Vec<u8>, PlanTypeError> {
    serde_jcs::to_vec(value_type).map_err(|error| {
        PlanTypeError::new(
            PLAN_TYPE_CANONICALIZATION_FAILED,
            format!("failed to canonicalize plan type: {error}"),
        )
    })
}

fn string_value_matches(
    value: &str,
    min_length: u64,
    max_length: Option<u64>,
    pattern: Option<&Regex>,
) -> bool {
    let length = u64::try_from(value.chars().count()).unwrap_or(u64::MAX);
    length >= min_length
        && max_length.is_none_or(|maximum| length <= maximum)
        && pattern.is_none_or(|pattern| pattern.is_match(value))
}

fn string_contract_accepts(value: &str, target: StringContract<'_>) -> bool {
    let Ok(pattern) = target.pattern.map(compile_pattern).transpose() else {
        return false;
    };
    if !string_value_matches(
        value,
        target.min_length,
        target.max_length,
        pattern.as_ref(),
    ) {
        return false;
    }
    target.enum_values.is_none_or(|values| {
        values
            .iter()
            .any(|candidate| candidate.as_str() == Some(value))
    })
}

fn string_is_assignable(source: StringContract<'_>, target: StringContract<'_>) -> bool {
    if let Some(values) = source.enum_values {
        return values.iter().all(|value| {
            value
                .as_str()
                .is_some_and(|value| string_contract_accepts(value, target))
        });
    }
    if target.enum_values.is_some() {
        return false;
    }
    source.min_length >= target.min_length
        && upper_bound_is_narrower(source.max_length, target.max_length)
        && match target.pattern {
            None => true,
            Some(target_pattern) => source.pattern == Some(target_pattern),
        }
}

fn upper_bound_is_narrower(source: Option<u64>, target: Option<u64>) -> bool {
    match (source, target) {
        (_, None) => true,
        (Some(source), Some(target)) => source <= target,
        (None, Some(_)) => false,
    }
}

fn finite_string_values(value_type: &PlanType) -> Option<&[Value]> {
    match value_type {
        PlanType::StringRefined {
            enum_values: Some(values),
            ..
        } => Some(values),
        _ => None,
    }
}

fn literal_is_assignable_to(value: &Value, target: &PlanType) -> bool {
    if let (Value::String(value), Some(target)) = (value, string_contract(target)) {
        return string_contract_accepts(value, target);
    }
    if let (Value::Array(values), Some(target)) = (value, array_contract(target)) {
        let length = u64::try_from(values.len()).unwrap_or(u64::MAX);
        return length >= target.min_items
            && target.max_items.is_none_or(|maximum| length <= maximum)
            && values.iter().all(|value| {
                PlanType::Literal {
                    value: value.clone(),
                }
                .is_assignable_normalized_to(target.items)
            });
    }
    match (value, target) {
        (Value::Null, PlanType::Null) => true,
        (Value::Bool(_), PlanType::Boolean) => true,
        (Value::Number(number), PlanType::Integer) => {
            number.is_i64()
                || number.is_u64()
                || number.as_f64().is_some_and(|value| value.fract() == 0.0)
        }
        (Value::Number(_), PlanType::Number) => true,
        (
            Value::Object(values),
            PlanType::Object {
                properties,
                additional_properties,
            },
        ) => {
            for (name, property) in properties {
                match values.get(name) {
                    Some(value) => {
                        if !(PlanType::Literal {
                            value: value.clone(),
                        })
                        .is_assignable_normalized_to(&property.value_type)
                        {
                            return false;
                        }
                    }
                    None if property.required => return false,
                    None => {}
                }
            }
            values.iter().all(|(name, value)| {
                properties.contains_key(name)
                    || additional_properties.as_deref().is_some_and(|target| {
                        (PlanType::Literal {
                            value: value.clone(),
                        })
                        .is_assignable_normalized_to(target)
                    })
            })
        }
        _ => false,
    }
}

fn object_is_assignable(
    source_properties: &BTreeMap<String, PlanProperty>,
    source_additional: Option<&PlanType>,
    target_properties: &BTreeMap<String, PlanProperty>,
    target_additional: Option<&PlanType>,
) -> bool {
    for (name, target_property) in target_properties {
        match source_properties.get(name) {
            Some(source_property) => {
                if target_property.required && !source_property.required {
                    return false;
                }
                if !source_property
                    .value_type
                    .is_assignable_normalized_to(&target_property.value_type)
                {
                    return false;
                }
            }
            None if target_property.required => return false,
            None => {
                if source_additional.is_some_and(|additional| {
                    !additional.is_assignable_normalized_to(&target_property.value_type)
                }) {
                    return false;
                }
            }
        }
    }

    for (name, source_property) in source_properties {
        if target_properties.contains_key(name) {
            continue;
        }
        let Some(target_additional) = target_additional else {
            return false;
        };
        if !source_property
            .value_type
            .is_assignable_normalized_to(target_additional)
        {
            return false;
        }
    }

    match (source_additional, target_additional) {
        (Some(source), Some(target)) => source.is_assignable_normalized_to(target),
        (Some(_), None) => false,
        (None, _) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn property(value_type: PlanType, required: bool) -> PlanProperty {
        PlanProperty::new(value_type, required).unwrap()
    }

    fn object(
        properties: impl IntoIterator<Item = (&'static str, PlanProperty)>,
        additional_properties: Option<PlanType>,
    ) -> PlanType {
        PlanType::Object {
            properties: properties
                .into_iter()
                .map(|(name, property)| (name.to_string(), property))
                .collect(),
            additional_properties: additional_properties.map(Box::new),
        }
        .normalize()
        .unwrap()
    }

    fn string_wire() -> Value {
        json!({
            "type": "string",
            "min_length": 0,
            "max_length": null,
            "pattern": null,
            "enum": null
        })
    }

    #[test]
    fn serde_round_trip_normalizes_nested_unions() {
        let value = json!({
            "type": "union",
            "variants": [
                string_wire(),
                {
                    "type": "union",
                    "variants": [
                        { "type": "boolean" },
                        string_wire()
                    ]
                }
            ]
        });
        let decoded: PlanType = serde_json::from_value(value).unwrap();
        let expected = PlanType::union([PlanType::Boolean, PlanType::String]).unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(
            serde_json::from_value::<PlanType>(serde_json::to_value(&decoded).unwrap()).unwrap(),
            decoded
        );
    }

    #[test]
    fn map_union_and_enum_reordering_have_identical_canonical_contracts() {
        let first_object = object(
            [
                ("zeta", property(PlanType::Integer, true)),
                (
                    "alpha",
                    property(
                        PlanType::string(
                            1,
                            Some(8),
                            Some("^[a-z]+$".to_owned()),
                            Some(vec![json!("beta"), json!("alpha"), json!("beta")]),
                        )
                        .unwrap(),
                        false,
                    ),
                ),
            ],
            None,
        );
        let second_object = object(
            [
                (
                    "alpha",
                    property(
                        PlanType::string(
                            1,
                            Some(8),
                            Some("^[a-z]+$".to_owned()),
                            Some(vec![json!("alpha"), json!("beta")]),
                        )
                        .unwrap(),
                        false,
                    ),
                ),
                ("zeta", property(PlanType::Integer, true)),
            ],
            None,
        );
        assert_eq!(
            first_object.canonical_bytes().unwrap(),
            second_object.canonical_bytes().unwrap()
        );
        assert_eq!(
            first_object.canonical_hash().unwrap(),
            second_object.canonical_hash().unwrap()
        );

        let first_union = PlanType::Union {
            variants: vec![
                PlanType::String,
                PlanType::Union {
                    variants: vec![PlanType::Boolean, PlanType::String],
                },
            ],
        };
        let second_union = PlanType::Union {
            variants: vec![PlanType::Boolean, PlanType::String],
        };
        assert_eq!(
            first_union.canonical_bytes().unwrap(),
            second_union.canonical_bytes().unwrap()
        );
    }

    #[test]
    fn canonical_type_wire_is_a_v2_golden_contract() {
        let value_type = PlanType::array(
            PlanType::string(
                1,
                Some(5),
                Some("^[a-z]+$".to_owned()),
                Some(vec![json!("beta"), json!("alpha"), json!("alpha")]),
            )
            .unwrap(),
            1,
            Some(2),
        )
        .unwrap();
        assert_eq!(PLAN_TYPE_WIRE_VERSION, 2);
        assert_eq!(PLAN_TYPE_REGEX_ENGINE_VERSION, "regex-1.13.0");
        assert_eq!(
            String::from_utf8(value_type.canonical_bytes().unwrap()).unwrap(),
            r#"{"items":{"enum":["alpha","beta"],"max_length":5,"min_length":1,"pattern":"^[a-z]+$","type":"string"},"max_items":2,"min_items":1,"type":"array"}"#
        );
        assert_eq!(
            value_type.canonical_hash().unwrap(),
            "sha256:f10c945a56ac750867c165716a32f303e1e5e34fc411129e21b8196016a18fca"
        );
    }

    #[test]
    fn string_assignability_is_directional_and_regex_is_conservative() {
        let wide = PlanType::string(1, Some(20), None, None).unwrap();
        let narrow = PlanType::string(3, Some(8), None, None).unwrap();
        assert!(narrow.is_assignable_to(&wide));
        assert!(!wide.is_assignable_to(&narrow));
        assert!(narrow.is_assignable_to(&PlanType::String));
        assert!(!PlanType::String.is_assignable_to(&narrow));

        let same_pattern_wide =
            PlanType::string(1, Some(20), Some("^[a-z]+$".to_owned()), None).unwrap();
        let same_pattern_narrow =
            PlanType::string(3, Some(8), Some("^[a-z]+$".to_owned()), None).unwrap();
        let different_pattern =
            PlanType::string(3, Some(8), Some("^[a-z]{3,8}$".to_owned()), None).unwrap();
        assert!(same_pattern_narrow.is_assignable_to(&same_pattern_wide));
        assert!(!different_pattern.is_assignable_to(&same_pattern_wide));

        let finite = PlanType::string(
            0,
            None,
            Some("^[a-z]+$".to_owned()),
            Some(vec![json!("alpha"), json!("beta")]),
        )
        .unwrap();
        assert!(finite.is_assignable_to(&same_pattern_wide));
    }

    #[test]
    fn enum_is_typed_validated_sorted_and_directional() {
        let ab = PlanType::string(
            1,
            Some(5),
            Some("^[a-z]+$".to_owned()),
            Some(vec![json!("beta"), json!("alpha"), json!("beta")]),
        )
        .unwrap();
        let abc = PlanType::string(
            1,
            Some(5),
            Some("^[a-z]+$".to_owned()),
            Some(vec![json!("gamma"), json!("beta"), json!("alpha")]),
        )
        .unwrap();
        assert!(ab.is_assignable_to(&abc));
        assert!(!abc.is_assignable_to(&ab));
        assert!(PlanType::string(0, None, None, Some(Vec::new()))
            .is_ok_and(|value| value == PlanType::Never));
        assert!(PlanType::string(0, None, None, Some(vec![json!(1)])).is_err());
        assert!(PlanType::string(3, None, None, Some(vec![json!("no")])).is_err());
        assert!(PlanType::string(
            0,
            None,
            Some("^[a-z]+$".to_owned()),
            Some(vec![json!("UPPER")]),
        )
        .is_err());
    }

    #[test]
    fn array_assignability_and_literals_respect_both_bounds_recursively() {
        let nested = PlanType::array(
            PlanType::string(2, Some(4), Some("^[a-z]+$".to_owned()), None).unwrap(),
            1,
            Some(2),
        )
        .unwrap();
        let wide = PlanType::array(PlanType::String, 0, Some(4)).unwrap();
        assert!(nested.is_assignable_to(&wide));
        assert!(!wide.is_assignable_to(&nested));
        assert!(nested.accepts_literal(&json!(["ab", "cde"])).unwrap());
        assert!(!nested.accepts_literal(&json!([])).unwrap());
        assert!(!nested.accepts_literal(&json!(["ab", "cd", "ef"])).unwrap());
        assert!(!nested.accepts_literal(&json!(["A!"])).unwrap());

        let object = object([("names", property(nested.clone(), true))], None);
        assert!(object.accepts_literal(&json!({"names": ["ab"]})).unwrap());
        assert!(!object.accepts_literal(&json!({"names": ["x"]})).unwrap());
    }

    #[test]
    fn primitive_union_and_object_assignment_remain_directional() {
        assert!(PlanType::Never.is_assignable_to(&PlanType::String));
        assert!(PlanType::String.is_assignable_to(&PlanType::Any));
        assert!(!PlanType::Any.is_assignable_to(&PlanType::String));
        assert!(PlanType::Integer.is_assignable_to(&PlanType::Number));
        assert!(!PlanType::Number.is_assignable_to(&PlanType::Integer));

        let union = PlanType::unify([PlanType::String, PlanType::Boolean]).unwrap();
        assert!(PlanType::String.is_assignable_to(&union));
        assert!(!union.is_assignable_to(&PlanType::String));

        let required = object([("answer", property(PlanType::String, true))], None);
        let optional = object([("answer", property(PlanType::String, false))], None);
        assert!(required.is_assignable_to(&optional));
        assert!(!optional.is_assignable_to(&required));
    }

    #[test]
    fn constraints_fail_closed_at_every_construction_boundary() {
        let unsafe_bound = MAX_SAFE_JSON_INTEGER + 1;
        assert!(PlanType::string(unsafe_bound, None, None, None).is_err());
        assert!(PlanType::string(5, Some(4), None, None).is_err());
        assert!(PlanType::string(0, None, Some("[".to_owned()), None).is_err());
        assert!(
            PlanType::string(0, None, Some("a".repeat(MAX_REGEX_PATTERN_BYTES + 1)), None,)
                .is_err()
        );
        assert!(PlanType::array(PlanType::String, unsafe_bound, None).is_err());
        assert!(PlanType::array(PlanType::String, 2, Some(1)).is_err());

        let forged = PlanType::StringRefined {
            min_length: 0,
            max_length: None,
            pattern: Some("[".to_owned()),
            enum_values: None,
        };
        assert!(!forged.is_assignable_to(&PlanType::String));
        assert!(serde_json::to_value(&forged).is_err());
    }

    #[test]
    fn v1_or_partial_v2_wire_and_unknown_fields_are_rejected() {
        let cases = [
            json!({ "type": "string" }),
            json!({
                "type": "string",
                "min_length": 0,
                "max_length": null,
                "pattern": null
            }),
            json!({
                "type": "string",
                "min_length": 0,
                "max_length": null,
                "pattern": null,
                "enum": null,
                "future_field": true
            }),
            json!({
                "type": "array",
                "items": string_wire(),
                "min_items": 0
            }),
            json!({
                "type": "array",
                "items": string_wire(),
                "min_items": 0,
                "max_items": null,
                "future_field": true
            }),
            json!({
                "type": "object",
                "properties": {}
            }),
        ];
        for value in cases {
            assert!(
                serde_json::from_value::<PlanType>(value.clone()).is_err(),
                "accepted invalid wire {value}"
            );
        }
    }

    #[test]
    fn literals_remain_canonical_and_safe_integer_bounded() {
        let integer = PlanType::literal(json!(3)).unwrap();
        let integral_float = PlanType::literal(json!(3.0)).unwrap();
        assert_eq!(integer, integral_float);
        assert!(integer.is_assignable_to(&PlanType::Integer));
        assert!(integer.is_assignable_to(&PlanType::Number));
        assert!(PlanType::literal(json!(9_007_199_254_740_992.0)).is_err());
        assert_eq!(
            PlanType::union([]).unwrap_err().code(),
            PLAN_TYPE_UNION_EMPTY
        );
    }
}
