use std::{collections::BTreeSet, fmt, fmt::Write as _, str::FromStr};

use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use super::{PlanError, PLAN_STABLE_ID_COLLISION, PLAN_WIRE_INVALID};
use crate::NodeId;

const PLAN_ID_MAX_BYTES: usize = 128;
const SOURCE_ID_MAX_BYTES: usize = 512;
const VERSION_MAX_BYTES: usize = 128;
const STABLE_NODE_ID_DOMAIN: &[u8] = b"insight-agent/canonical-plan/generated-node-id/v1";

fn validate_plan_identifier(kind: &str, value: &str) -> Result<(), PlanError> {
    if value.is_empty() || value.len() > PLAN_ID_MAX_BYTES {
        return Err(PlanError::new(
            PLAN_WIRE_INVALID,
            format!("{kind} must be a non-empty identifier of at most {PLAN_ID_MAX_BYTES} bytes"),
        ));
    }
    let mut bytes = value.bytes();
    let first = bytes.next().expect("empty identifier was rejected");
    if !(first == b'_' || first.is_ascii_alphabetic())
        || !bytes.all(|byte| {
            byte == b'_'
                || byte == b'-'
                || byte == b'.'
                || byte == b':'
                || byte.is_ascii_alphanumeric()
        })
    {
        return Err(PlanError::new(
            PLAN_WIRE_INVALID,
            format!(
                "{kind} must start with an ASCII letter or underscore and contain only ASCII letters, digits, '_', '-', '.', or ':'"
            ),
        ));
    }
    Ok(())
}

fn validate_source_identifier(kind: &str, value: &str) -> Result<(), PlanError> {
    if value.is_empty() || value.len() > SOURCE_ID_MAX_BYTES || value.chars().any(char::is_control)
    {
        return Err(PlanError::new(
            PLAN_WIRE_INVALID,
            format!("{kind} must be non-empty, bounded, and contain no control characters"),
        ));
    }
    Ok(())
}

macro_rules! string_value {
    ($name:ident, $label:literal, $validator:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, PlanError> {
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
            type Err = PlanError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = PlanError;

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

string_value!(ControlPortId, "control port ID", validate_plan_identifier);
string_value!(DataPortId, "data port ID", validate_plan_identifier);
string_value!(ControlEdgeId, "control edge ID", validate_plan_identifier);
string_value!(DataBindingId, "data binding ID", validate_plan_identifier);
string_value!(PhiBindingId, "Phi binding ID", validate_plan_identifier);
string_value!(ScopeId, "scope ID", validate_plan_identifier);
string_value!(PolicyId, "policy ID", validate_plan_identifier);
string_value!(BranchCaseId, "branch case ID", validate_plan_identifier);
string_value!(PortName, "port name", validate_plan_identifier);
string_value!(SecretRef, "secret reference", validate_plan_identifier);
string_value!(
    SourceDocumentId,
    "source document ID",
    validate_source_identifier
);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VersionTag(String);

impl VersionTag {
    pub fn new(value: impl Into<String>) -> Result<Self, PlanError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > VERSION_MAX_BYTES
            || value
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
        {
            return Err(PlanError::new(
                PLAN_WIRE_INVALID,
                "version tag must be non-empty, bounded, and contain no whitespace or control characters",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for VersionTag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for VersionTag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for VersionTag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

/// Deterministic compiler-node ID allocator with an explicit collision set.
///
/// Every field is independently length-framed before hashing, so ambiguous
/// concatenations cannot alias. Callers should reserve authored IDs before
/// allocating compiler-owned IDs.
#[derive(Debug, Clone, Default)]
pub struct StableNodeIdGenerator {
    occupied: BTreeSet<NodeId>,
}

impl StableNodeIdGenerator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_reserved(ids: impl IntoIterator<Item = NodeId>) -> Result<Self, PlanError> {
        let mut generator = Self::new();
        for id in ids {
            generator.reserve(id)?;
        }
        Ok(generator)
    }

    pub fn reserve(&mut self, id: NodeId) -> Result<(), PlanError> {
        if !self.occupied.insert(id.clone()) {
            return Err(PlanError::new(
                PLAN_STABLE_ID_COLLISION,
                format!("node ID collision for '{id}'"),
            ));
        }
        Ok(())
    }

    pub fn compiler_node_id(
        &mut self,
        stable_parent: &NodeId,
        semantic_role: &str,
        arm_or_leg_id: Option<&str>,
    ) -> Result<NodeId, PlanError> {
        validate_component("semantic role", semantic_role)?;
        if let Some(member) = arm_or_leg_id {
            validate_component("arm/leg ID", member)?;
        }

        let mut hasher = Sha256::new();
        for part in [
            STABLE_NODE_ID_DOMAIN,
            b"node".as_slice(),
            stable_parent.as_str().as_bytes(),
            semantic_role.as_bytes(),
            arm_or_leg_id.unwrap_or("").as_bytes(),
        ] {
            let length = u64::try_from(part.len())
                .expect("supported Rust targets cannot address more than u64::MAX bytes");
            hasher.update(length.to_be_bytes());
            hasher.update(part);
        }
        let digest = hasher.finalize();
        let mut value = String::with_capacity(4 + digest.len() * 2);
        value.push_str("gen_");
        for byte in digest {
            write!(&mut value, "{byte:02x}").expect("writing to a String cannot fail");
        }
        let id = NodeId::new(value).map_err(|error| {
            PlanError::new(
                PLAN_WIRE_INVALID,
                format!("generated node ID failed validation: {error}"),
            )
        })?;
        self.reserve(id.clone())?;
        Ok(id)
    }

    pub fn contains(&self, id: &NodeId) -> bool {
        self.occupied.contains(id)
    }
}

fn validate_component(label: &str, value: &str) -> Result<(), PlanError> {
    if value.is_empty() || value.len() > PLAN_ID_MAX_BYTES || value.chars().any(char::is_control) {
        return Err(PlanError::new(
            PLAN_WIRE_INVALID,
            format!("stable ID {label} must be non-empty, bounded, and contain no controls"),
        ));
    }
    Ok(())
}
