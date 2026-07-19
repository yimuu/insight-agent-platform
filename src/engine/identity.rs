use std::{fmt, fmt::Write as _, str::FromStr};

use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::ModelError;

pub const ID_MAX_BYTES: usize = 256;
pub const SEMANTIC_ID_MAX_BYTES: usize = 128;
pub const DYNAMIC_KEY_MAX_BYTES: usize = 512;

const ID_INVALID: &str = "ENGINE_ID_INVALID";
const COUNTER_INVALID: &str = "ENGINE_COUNTER_INVALID";
const COUNTER_OVERFLOW: &str = "ENGINE_COUNTER_OVERFLOW";
const IDENTITY_HASH_DOMAIN: &[u8] = b"insight-agent/identity/v1";

fn framed_identity_hash(kind: &str, parts: &[&str]) -> ContentHash {
    let mut bytes = Vec::new();
    for part in std::iter::once(IDENTITY_HASH_DOMAIN)
        .chain(std::iter::once(kind.as_bytes()))
        .chain(parts.iter().map(|part| part.as_bytes()))
    {
        let length = u64::try_from(part.len())
            .expect("supported Rust targets cannot address more than u64::MAX bytes");
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(part);
    }
    ContentHash::from_bytes(&bytes)
}

fn validate_opaque_id(kind: &str, value: &str) -> Result<(), ModelError> {
    if value.is_empty()
        || value.len() > ID_MAX_BYTES
        || value.chars().any(|character| {
            character.is_control()
                || character.is_whitespace()
                || character == '/'
                || character == '\\'
        })
    {
        return Err(ModelError::new(
            ID_INVALID,
            format!("{kind} must be a non-empty, bounded opaque identifier"),
        ));
    }
    Ok(())
}

fn validate_semantic_id(kind: &str, value: &str) -> Result<(), ModelError> {
    if value.is_empty() || value.len() > SEMANTIC_ID_MAX_BYTES {
        return Err(ModelError::new(
            ID_INVALID,
            format!("{kind} must be a non-empty, bounded semantic identifier"),
        ));
    }
    let mut characters = value.chars();
    let first = characters.next().expect("empty semantic ID was rejected");
    if !(first == '_' || first.is_ascii_alphabetic())
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err(ModelError::new(
            ID_INVALID,
            format!("{kind} must match [A-Za-z_][A-Za-z0-9_]*"),
        ));
    }
    Ok(())
}

fn validate_dynamic_key(value: &str) -> Result<(), ModelError> {
    if value.is_empty()
        || value.len() > DYNAMIC_KEY_MAX_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(ModelError::new(
            ID_INVALID,
            "dynamic key must be non-empty, bounded, and contain no control characters",
        ));
    }
    Ok(())
}

macro_rules! string_id {
    ($name:ident, $label:literal, $validator:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ModelError> {
                let value = value.into();
                $validator($label, &value)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl FromStr for $name {
            type Err = ModelError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = ModelError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(D::Error::custom)
            }
        }
    };
}

string_id!(
    DefinitionRevisionId,
    "definition revision ID",
    validate_opaque_id
);
string_id!(
    DeploymentRevisionId,
    "deployment revision ID",
    validate_opaque_id
);
string_id!(RunId, "run ID", validate_opaque_id);
string_id!(ScopeInstanceId, "scope instance ID", validate_opaque_id);
string_id!(ActivationId, "activation ID", validate_opaque_id);
string_id!(EffectId, "effect ID", validate_opaque_id);
string_id!(ForkGroupId, "fork group ID", validate_opaque_id);
string_id!(ControlTokenId, "control token ID", validate_opaque_id);
string_id!(ArtifactId, "artifact ID", validate_opaque_id);
string_id!(NodeId, "node ID", validate_semantic_id);
string_id!(PortId, "port ID", validate_semantic_id);
string_id!(LegId, "leg ID", validate_semantic_id);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DynamicKey(String);

impl DynamicKey {
    pub fn new(value: impl Into<String>) -> Result<Self, ModelError> {
        let value = value.into();
        validate_dynamic_key(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DynamicKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for DynamicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for DynamicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentHash(String);

impl ContentHash {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let digest = Sha256::digest(bytes);
        let mut encoded = String::with_capacity("sha256:".len() + digest.len() * 2);
        encoded.push_str("sha256:");
        for byte in digest {
            write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
        }
        Self(encoded)
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, ModelError> {
        let value = value.into();
        let Some(hex) = value.strip_prefix("sha256:") else {
            return Err(ModelError::new(
                ID_INVALID,
                "content hash must use the sha256:<hex> form",
            ));
        };
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ModelError::new(
                ID_INVALID,
                "content hash must contain 64 lowercase hexadecimal digits",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for ContentHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ContentHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(D::Error::custom)
    }
}

macro_rules! uuid_constructor {
    ($type:ident, $prefix:literal) => {
        impl $type {
            pub fn random() -> Self {
                Self(format!(concat!($prefix, "{}"), Uuid::new_v4().simple()))
            }
        }
    };
}

uuid_constructor!(RunId, "run_");
uuid_constructor!(ActivationId, "activation_");
uuid_constructor!(ForkGroupId, "fork_");
uuid_constructor!(ControlTokenId, "token_");

impl EffectId {
    pub fn for_activation(run_id: &RunId, activation_id: &ActivationId) -> Self {
        let hash = framed_identity_hash("effect", &[run_id.as_str(), activation_id.as_str()]);
        Self(format!("effect_{}", &hash.as_str()["sha256:".len()..]))
    }
}

impl ScopeInstanceId {
    pub fn root() -> Self {
        Self("scope_root".to_string())
    }

    pub fn derive(parent: &Self, owner: &NodeId, discriminator: &str) -> Result<Self, ModelError> {
        if discriminator.is_empty()
            || discriminator.len() > DYNAMIC_KEY_MAX_BYTES
            || discriminator.chars().any(char::is_control)
        {
            return Err(ModelError::new(
                ID_INVALID,
                "scope discriminator must be non-empty, bounded, and body-free",
            ));
        }
        let hash = framed_identity_hash("scope", &[parent.as_str(), owner.as_str(), discriminator]);
        Ok(Self(format!("scope_{}", &hash.as_str()["sha256:".len()..])))
    }
}

macro_rules! monotonic_counter {
    ($name:ident, $inner:ty, $label:literal) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name($inner);

        impl $name {
            pub const FIRST: Self = Self(1);

            pub fn new(value: $inner) -> Result<Self, ModelError> {
                if value == 0 {
                    return Err(ModelError::new(
                        COUNTER_INVALID,
                        concat!($label, " must start at one"),
                    ));
                }
                Ok(Self(value))
            }

            pub fn get(self) -> $inner {
                self.0
            }

            pub fn next(self) -> Result<Self, ModelError> {
                self.0.checked_add(1).map(Self).ok_or_else(|| {
                    ModelError::new(COUNTER_OVERFLOW, concat!($label, " overflowed"))
                })
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_u64(u64::from(self.0))
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = <$inner>::deserialize(deserializer)?;
                Self::new(value).map_err(D::Error::custom)
            }
        }
    };
}

monotonic_counter!(AttemptNo, u32, "attempt number");
monotonic_counter!(LeaseEpoch, u64, "lease epoch");
monotonic_counter!(Generation, u32, "run generation");

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn semantic_ids_are_validated_on_construction_and_deserialization() {
        assert_eq!(
            NodeId::new("analyze_report").unwrap().as_str(),
            "analyze_report"
        );
        assert_eq!(NodeId::new("1-invalid").unwrap_err().code(), ID_INVALID);
        assert!(serde_json::from_value::<NodeId>(json!("bad/id")).is_err());
    }

    #[test]
    fn generated_effect_and_scope_ids_are_stable() {
        let run = RunId::new("run_fixed").unwrap();
        let activation = ActivationId::new("activation_fixed").unwrap();
        let first = EffectId::for_activation(&run, &activation);
        let second = EffectId::for_activation(&run, &activation);
        assert_eq!(first, second);

        let owner = NodeId::new("map_items").unwrap();
        let child_a = ScopeInstanceId::derive(&ScopeInstanceId::root(), &owner, "item:a").unwrap();
        let child_b = ScopeInstanceId::derive(&ScopeInstanceId::root(), &owner, "item:a").unwrap();
        assert_eq!(child_a, child_b);
    }

    #[test]
    fn deterministic_ids_use_boundary_safe_framing() {
        let first_effect = EffectId::for_activation(
            &RunId::new("a:b").unwrap(),
            &ActivationId::new("c").unwrap(),
        );
        let second_effect = EffectId::for_activation(
            &RunId::new("a").unwrap(),
            &ActivationId::new("b:c").unwrap(),
        );
        assert_ne!(first_effect, second_effect);

        let first_scope = ScopeInstanceId::derive(
            &ScopeInstanceId::new("a:b").unwrap(),
            &NodeId::new("c").unwrap(),
            "d",
        )
        .unwrap();
        let second_scope = ScopeInstanceId::derive(
            &ScopeInstanceId::new("a").unwrap(),
            &NodeId::new("b").unwrap(),
            "c:d",
        )
        .unwrap();
        assert_ne!(first_scope, second_scope);
    }

    #[test]
    fn monotonic_counters_reject_zero_and_overflow() {
        assert_eq!(AttemptNo::new(0).unwrap_err().code(), COUNTER_INVALID);
        assert!(serde_json::from_value::<LeaseEpoch>(json!(0)).is_err());
        assert_eq!(
            AttemptNo::new(u32::MAX).unwrap().next().unwrap_err().code(),
            COUNTER_OVERFLOW
        );
    }

    #[test]
    fn content_hash_has_a_strict_round_trip() {
        let hash = ContentHash::from_bytes(b"stable");
        let encoded = serde_json::to_value(&hash).unwrap();
        assert_eq!(
            serde_json::from_value::<ContentHash>(encoded).unwrap(),
            hash
        );
        assert!(ContentHash::parse("sha256:ABC").is_err());
    }
}
