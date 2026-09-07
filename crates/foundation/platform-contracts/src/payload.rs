//! Versioned, bounded canonical JSON at durable boundaries.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use sha2::{Digest as _, Sha256};
use std::{error::Error, fmt};

pub const DEFAULT_PAYLOAD_LIMIT: usize = 1_048_576;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadError {
    InvalidInput(String),
}
impl fmt::Display for PayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(f, "invalid payload: {message}"),
        }
    }
}
impl Error for PayloadError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypedPayload {
    pub schema_version: i32,
    pub value: Value,
    pub digest: String,
}
impl TypedPayload {
    pub fn new<T: Serialize>(schema_version: i32, source: &T) -> Result<Self, PayloadError> {
        Self::with_limit(schema_version, source, DEFAULT_PAYLOAD_LIMIT)
    }

    pub fn with_limit<T: Serialize>(
        schema_version: i32,
        source: &T,
        maximum_bytes: usize,
    ) -> Result<Self, PayloadError> {
        if schema_version <= 0 {
            return Err(PayloadError::InvalidInput(
                "payload schema version must be positive".to_owned(),
            ));
        }
        let value = serde_json::to_value(source)
            .map_err(|failure| PayloadError::InvalidInput(failure.to_string()))?;
        let Value::Object(mut object) = value else {
            return Err(PayloadError::InvalidInput(
                "typed payload must serialize to a JSON object".to_owned(),
            ));
        };
        if object.contains_key("schema_version") {
            return Err(PayloadError::InvalidInput(
                "typed payload source must not define schema_version".to_owned(),
            ));
        }
        object.insert(
            "schema_version".to_owned(),
            Value::Number(Number::from(schema_version)),
        );
        let value = Value::Object(object);
        let canonical = serde_jcs::to_vec(&value)
            .map_err(|failure| PayloadError::InvalidInput(failure.to_string()))?;
        if canonical.len() > maximum_bytes {
            return Err(PayloadError::InvalidInput(format!(
                "typed payload is {} bytes; maximum is {maximum_bytes}",
                canonical.len()
            )));
        }
        Ok(Self {
            schema_version,
            value,
            digest: format!(
                "sha256:{}",
                Sha256::digest(&canonical)
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            ),
        })
    }

    pub fn empty(schema_version: i32) -> Result<Self, PayloadError> {
        Self::new(schema_version, &Map::<String, Value>::new())
    }

    pub fn from_versioned<T: Serialize>(
        schema_version: i32,
        source: &T,
        maximum_bytes: usize,
    ) -> Result<Self, PayloadError> {
        if schema_version <= 0 {
            return Err(PayloadError::InvalidInput(
                "payload schema version must be positive".to_owned(),
            ));
        }
        let value = serde_json::to_value(source)
            .map_err(|failure| PayloadError::InvalidInput(failure.to_string()))?;
        let Value::Object(object) = &value else {
            return Err(PayloadError::InvalidInput(
                "versioned payload must serialize to a JSON object".to_owned(),
            ));
        };
        if object.get("schema_version").and_then(Value::as_i64) != Some(i64::from(schema_version)) {
            return Err(PayloadError::InvalidInput(
                "embedded and relational schema versions disagree".to_owned(),
            ));
        }
        let canonical = serde_jcs::to_vec(&value)
            .map_err(|failure| PayloadError::InvalidInput(failure.to_string()))?;
        if canonical.len() > maximum_bytes {
            return Err(PayloadError::InvalidInput(format!(
                "typed payload is {} bytes; maximum is {maximum_bytes}",
                canonical.len()
            )));
        }
        Ok(Self {
            schema_version,
            value,
            digest: format!(
                "sha256:{}",
                Sha256::digest(&canonical)
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            ),
        })
    }
}
