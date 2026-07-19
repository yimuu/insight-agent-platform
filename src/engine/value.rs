use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::Value;

use super::{ArtifactId, ContentHash, ModelError};

const VALUE_CANONICALIZATION_FAILED: &str = "ENGINE_VALUE_CANONICALIZATION_FAILED";
const VALUE_INTEGRITY_FAILED: &str = "ENGINE_VALUE_INTEGRITY_FAILED";
const ARTIFACT_INVALID: &str = "ENGINE_ARTIFACT_INVALID";

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InlineValueRef {
    value: Value,
    content_hash: ContentHash,
    canonical_bytes: u64,
}

impl InlineValueRef {
    pub fn new(value: Value) -> Result<Self, ModelError> {
        let canonical = serde_jcs::to_vec(&value).map_err(|_| {
            ModelError::new(
                VALUE_CANONICALIZATION_FAILED,
                "inline value could not be canonicalized",
            )
        })?;
        let canonical_bytes = u64::try_from(canonical.len()).map_err(|_| {
            ModelError::new(
                VALUE_CANONICALIZATION_FAILED,
                "inline value size exceeds the supported range",
            )
        })?;
        Ok(Self {
            value,
            content_hash: ContentHash::from_bytes(&canonical),
            canonical_bytes,
        })
    }

    pub fn value(&self) -> &Value {
        &self.value
    }

    pub fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }

    pub fn canonical_bytes(&self) -> u64 {
        self.canonical_bytes
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InlineValueRefWire {
    value: Value,
    content_hash: ContentHash,
    canonical_bytes: u64,
}

impl<'de> Deserialize<'de> for InlineValueRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = InlineValueRefWire::deserialize(deserializer)?;
        let value = Self::new(wire.value).map_err(D::Error::custom)?;
        if value.content_hash != wire.content_hash || value.canonical_bytes != wire.canonical_bytes
        {
            return Err(D::Error::custom(ModelError::new(
                VALUE_INTEGRITY_FAILED,
                "inline value metadata does not match its canonical content",
            )));
        }
        Ok(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactRef {
    artifact_id: ArtifactId,
    content_hash: ContentHash,
    size_bytes: u64,
    media_type: Option<String>,
}

impl ArtifactRef {
    pub fn new(
        artifact_id: ArtifactId,
        content_hash: ContentHash,
        size_bytes: u64,
        media_type: Option<String>,
    ) -> Result<Self, ModelError> {
        if media_type.as_ref().is_some_and(|value| {
            value.is_empty()
                || value.len() > 255
                || value.chars().any(|character| character.is_control())
        }) {
            return Err(ModelError::new(
                ARTIFACT_INVALID,
                "artifact media type must be non-empty, bounded, and body-free",
            ));
        }
        Ok(Self {
            artifact_id,
            content_hash,
            size_bytes,
            media_type,
        })
    }

    pub fn artifact_id(&self) -> &ArtifactId {
        &self.artifact_id
    }

    pub fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn media_type(&self) -> Option<&str> {
        self.media_type.as_deref()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactRefWire {
    artifact_id: ArtifactId,
    content_hash: ContentHash,
    size_bytes: u64,
    media_type: Option<String>,
}

impl<'de> Deserialize<'de> for ArtifactRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ArtifactRefWire::deserialize(deserializer)?;
        Self::new(
            wire.artifact_id,
            wire.content_hash,
            wire.size_bytes,
            wire.media_type,
        )
        .map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "storage", rename_all = "snake_case")]
pub enum ValueRef {
    Inline(InlineValueRef),
    Artifact(ArtifactRef),
}

impl ValueRef {
    pub fn inline(value: Value) -> Result<Self, ModelError> {
        InlineValueRef::new(value).map(Self::Inline)
    }

    pub fn content_hash(&self) -> &ContentHash {
        match self {
            Self::Inline(value) => value.content_hash(),
            Self::Artifact(value) => value.content_hash(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn inline_hash_uses_canonical_json_order() {
        let first = ValueRef::inline(json!({"a": 1, "b": 2})).unwrap();
        let second = ValueRef::inline(json!({"b": 2, "a": 1})).unwrap();
        assert_eq!(first.content_hash(), second.content_hash());
    }

    #[test]
    fn inline_value_rejects_tampered_hash_and_size_on_deserialization() {
        let value = InlineValueRef::new(json!({"answer": 42})).unwrap();
        let encoded = serde_json::to_value(&value).unwrap();
        assert_eq!(
            serde_json::from_value::<InlineValueRef>(encoded.clone()).unwrap(),
            value
        );

        let mut wrong_hash = encoded.clone();
        wrong_hash["content_hash"] = json!(ContentHash::from_bytes(b"different").as_str());
        assert!(serde_json::from_value::<InlineValueRef>(wrong_hash).is_err());

        let mut wrong_size = encoded;
        wrong_size["canonical_bytes"] = json!(999);
        assert!(serde_json::from_value::<InlineValueRef>(wrong_size).is_err());
    }

    #[test]
    fn artifact_metadata_rejects_control_characters() {
        let result = ArtifactRef::new(
            ArtifactId::new("artifact_fixed").unwrap(),
            ContentHash::from_bytes(b"artifact"),
            8,
            Some("text/plain\nsecret".to_string()),
        );
        assert_eq!(result.unwrap_err().code(), ARTIFACT_INVALID);
    }

    #[test]
    fn artifact_metadata_is_revalidated_on_deserialization() {
        let artifact = ArtifactRef::new(
            ArtifactId::new("artifact_fixed").unwrap(),
            ContentHash::from_bytes(b"artifact"),
            8,
            Some("text/plain".to_string()),
        )
        .unwrap();
        let mut encoded = serde_json::to_value(&artifact).unwrap();
        encoded["media_type"] = json!("text/plain\nsecret");
        assert!(serde_json::from_value::<ArtifactRef>(encoded).is_err());
    }
}
