use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeSet, error::Error, fmt};

pub const MAX_SAFE_JSON_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonLimits {
    pub max_bytes: usize,
    pub max_depth: usize,
    pub max_properties_per_object: usize,
    pub max_items_per_array: usize,
    pub max_string_bytes: usize,
}

impl JsonLimits {
    pub const CONTRACT_FIXTURE: Self = Self {
        max_bytes: 1_048_576,
        max_depth: 32,
        max_properties_per_object: 1_024,
        max_items_per_array: 4_096,
        max_string_bytes: 262_144,
    };

    fn validate(self) -> Result<(), StrictJsonError> {
        if self.max_bytes == 0
            || self.max_depth == 0
            || self.max_properties_per_object == 0
            || self.max_items_per_array == 0
            || self.max_string_bytes == 0
        {
            return Err(StrictJsonError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrictJsonError {
    InvalidLimits,
    DocumentTooLarge { actual: usize, maximum: usize },
    InvalidJson(String),
    Canonicalization(String),
}

impl fmt::Display for StrictJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits => formatter.write_str("strict JSON limits must be positive"),
            Self::DocumentTooLarge { actual, maximum } => {
                write!(
                    formatter,
                    "JSON document is {actual} bytes; maximum is {maximum}"
                )
            }
            Self::InvalidJson(message) => {
                write!(formatter, "strict JSON rejected input: {message}")
            }
            Self::Canonicalization(message) => {
                write!(formatter, "JCS canonicalization failed: {message}")
            }
        }
    }
}

impl Error for StrictJsonError {}

pub fn parse_strict_json(input: &[u8], limits: JsonLimits) -> Result<Value, StrictJsonError> {
    limits.validate()?;
    if input.len() > limits.max_bytes {
        return Err(StrictJsonError::DocumentTooLarge {
            actual: input.len(),
            maximum: limits.max_bytes,
        });
    }

    let mut deserializer = serde_json::Deserializer::from_slice(input);
    let value = StrictValueSeed {
        limits: &limits,
        depth: 1,
    }
    .deserialize(&mut deserializer)
    .map_err(|failure| StrictJsonError::InvalidJson(failure.to_string()))?;
    deserializer
        .end()
        .map_err(|failure| StrictJsonError::InvalidJson(failure.to_string()))?;
    Ok(value)
}

pub fn canonical_json(value: &Value) -> Result<Vec<u8>, StrictJsonError> {
    serde_jcs::to_vec(value)
        .map_err(|failure| StrictJsonError::Canonicalization(failure.to_string()))
}

pub fn canonical_digest(value: &Value) -> Result<String, StrictJsonError> {
    let canonical = canonical_json(value)?;
    let digest = Sha256::digest(canonical);
    Ok(format!("sha256:{}", lowercase_hex(&digest)))
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

struct StrictValueSeed<'a> {
    limits: &'a JsonLimits,
    depth: usize,
}

impl StrictValueSeed<'_> {
    fn child(&self) -> Result<Self, String> {
        let depth = self.depth + 1;
        if depth > self.limits.max_depth {
            return Err(format!(
                "nesting depth {depth} exceeds maximum {}",
                self.limits.max_depth
            ));
        }
        Ok(Self {
            limits: self.limits,
            depth,
        })
    }
}

impl<'de> DeserializeSeed<'de> for StrictValueSeed<'_> {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if self.depth > self.limits.max_depth {
            return Err(de::Error::custom(format!(
                "nesting depth {} exceeds maximum {}",
                self.depth, self.limits.max_depth
            )));
        }
        deserializer.deserialize_any(StrictValueVisitor { seed: self })
    }
}

struct StrictValueVisitor<'a> {
    seed: StrictValueSeed<'a>,
}

impl<'de> Visitor<'de> for StrictValueVisitor<'_> {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.unsigned_abs() > MAX_SAFE_JSON_INTEGER {
            return Err(E::custom("integer is outside the interoperable JSON range"));
        }
        Ok(Value::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value > MAX_SAFE_JSON_INTEGER {
            return Err(E::custom("integer is outside the interoperable JSON range"));
        }
        Ok(Value::Number(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if !value.is_finite() {
            return Err(E::custom("NaN and Infinity are forbidden"));
        }
        if value.fract() == 0.0 && value.abs() > MAX_SAFE_JSON_INTEGER as f64 {
            return Err(E::custom("integer is outside the interoperable JSON range"));
        }
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("number cannot be represented as finite JSON"))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Null)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Null)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_string(value.to_owned())
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_string(value.to_owned())
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.len() > self.seed.limits.max_string_bytes {
            return Err(E::custom(format!(
                "string is {} bytes; maximum is {}",
                value.len(),
                self.seed.limits.max_string_bytes
            )));
        }
        Ok(Value::String(value))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) =
            sequence.next_element_seed(self.seed.child().map_err(de::Error::custom)?)?
        {
            if values.len() == self.seed.limits.max_items_per_array {
                return Err(de::Error::custom(format!(
                    "array exceeds maximum {} items",
                    self.seed.limits.max_items_per_array
                )));
            }
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        let mut keys = BTreeSet::new();
        while let Some(key) = object.next_key::<String>()? {
            if key.len() > self.seed.limits.max_string_bytes {
                return Err(de::Error::custom("object key exceeds string byte limit"));
            }
            if !keys.insert(key.clone()) {
                return Err(de::Error::custom(format!("duplicate object key {key:?}")));
            }
            if values.len() == self.seed.limits.max_properties_per_object {
                return Err(de::Error::custom(format!(
                    "object exceeds maximum {} properties",
                    self.seed.limits.max_properties_per_object
                )));
            }
            let value = object.next_value_seed(self.seed.child().map_err(de::Error::custom)?)?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejects_duplicates_before_building_a_value() {
        let failure =
            parse_strict_json(br#"{"a":1,"a":2}"#, JsonLimits::CONTRACT_FIXTURE).unwrap_err();
        assert!(failure.to_string().contains("duplicate object key"));
    }

    #[test]
    fn enforces_container_and_document_limits() {
        let limits = JsonLimits {
            max_bytes: 16,
            max_depth: 2,
            max_properties_per_object: 1,
            max_items_per_array: 1,
            max_string_bytes: 2,
        };
        assert!(parse_strict_json(br#"{"a":1,"b":2}"#, limits).is_err());
        assert!(parse_strict_json(br#"[[1]]"#, limits).is_err());
        assert!(parse_strict_json(br#""abc""#, limits).is_err());
    }

    #[test]
    fn rejects_invalid_utf8_non_finite_and_unrepresentable_numbers() {
        for input in [
            &b"\xff"[..],
            &b"NaN"[..],
            &b"Infinity"[..],
            &b"1e400"[..],
            &b"18446744073709551616"[..],
        ] {
            assert!(
                parse_strict_json(input, JsonLimits::CONTRACT_FIXTURE).is_err(),
                "accepted invalid numeric/UTF-8 input: {:?}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn computes_rfc_8785_canonical_bytes_and_prefixed_digest() {
        let value = json!({"z": 1, "a": [true, null]});
        assert_eq!(
            canonical_json(&value).unwrap(),
            br#"{"a":[true,null],"z":1}"#
        );
        let digest = canonical_digest(&value).unwrap();
        assert!(digest.starts_with("sha256:"));
        assert_eq!(digest.len(), 71);
    }
}
