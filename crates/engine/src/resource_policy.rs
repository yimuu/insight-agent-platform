use std::collections::BTreeSet;

use serde::{de::Error as _, ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// Closed authorization vocabulary for model-visible tool arguments.
///
/// `All` is deliberately distinct from an exhaustive field list: only `All`
/// authorizes raw Provider argument deltas. `Fields` authorizes a completed,
/// schema-validated projection and therefore serializes as an ordered JSON
/// array rather than an open object.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ToolPublicArguments {
    #[default]
    Private,
    All,
    Fields(BTreeSet<String>),
}

impl Serialize for ToolPublicArguments {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Private => serializer.serialize_str("private"),
            Self::All => serializer.serialize_str("all"),
            Self::Fields(fields) => {
                let mut sequence = serializer.serialize_seq(Some(fields.len()))?;
                for field in fields {
                    sequence.serialize_element(field)?;
                }
                sequence.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ToolPublicArguments {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::String(mode) if mode == "private" => Ok(Self::Private),
            Value::String(mode) if mode == "all" => Ok(Self::All),
            Value::Array(values) => {
                let fields = values
                    .into_iter()
                    .map(|value| {
                        value.as_str().map(str::to_owned).ok_or_else(|| {
                            D::Error::custom("public argument fields must be strings")
                        })
                    })
                    .collect::<Result<BTreeSet<_>, _>>()?;
                Ok(Self::Fields(fields))
            }
            _ => Err(D::Error::custom(
                "public arguments must be 'private', 'all', or a field list",
            )),
        }
    }
}

/// Tool-side half of the public response authorization contract.
///
/// The default is fully private. `progress` and `result` are independent,
/// self-contained closed JSON Schemas for safe public values; neither is the
/// executor's raw output schema. The Agent-side `llm.publish` decision is
/// applied separately by the deployment linker.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolPublicPolicy {
    #[serde(default)]
    pub call: bool,
    #[serde(default)]
    pub arguments: ToolPublicArguments,
    #[serde(default, rename = "progress", skip_serializing_if = "Option::is_none")]
    pub progress_schema: Option<Value>,
    #[serde(default, rename = "result")]
    pub result_schema: Option<Value>,
}

impl ToolPublicPolicy {
    pub fn private() -> Self {
        Self::default()
    }

    pub fn is_fully_private(&self) -> bool {
        !self.call
            && matches!(self.arguments, ToolPublicArguments::Private)
            && self.progress_schema.is_none()
            && self.result_schema.is_none()
    }
}

/// Retrieval-side half of the caller-visible publication contract.
///
/// Both fields are private by default. `result` is the closed schema for one
/// public retrieval result, not the raw provider result and not the complete
/// model-facing output schema.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrievalPublicPolicy {
    #[serde(default)]
    pub query: bool,
    #[serde(default, rename = "result")]
    pub result_schema: Option<Value>,
}

impl RetrievalPublicPolicy {
    pub fn private() -> Self {
        Self::default()
    }

    pub fn is_fully_private(&self) -> bool {
        !self.query && self.result_schema.is_none()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ToolPublicArguments, ToolPublicPolicy};

    #[test]
    fn absent_progress_preserves_the_pre_extension_canonical_wire() {
        let old_wire = json!({
            "call": true,
            "arguments": "private",
            "result": null
        });
        let decoded = serde_json::from_value::<ToolPublicPolicy>(old_wire.clone()).unwrap();
        assert_eq!(
            decoded,
            ToolPublicPolicy {
                call: true,
                arguments: ToolPublicArguments::Private,
                progress_schema: None,
                result_schema: None,
            }
        );
        assert_eq!(serde_json::to_value(decoded).unwrap(), old_wire);
    }
}
