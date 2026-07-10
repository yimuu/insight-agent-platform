use std::{collections::BTreeMap, fmt, time::Duration};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use super::CompileError;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawAgent {
    pub version: u32,
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub input: RawInput,
    #[serde(default)]
    pub prompts: BTreeMap<String, String>,
    pub entry: String,
    pub nodes: BTreeMap<String, RawNode>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawInput {
    pub schema: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawNode {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub next: Option<String>,
    #[serde(default)]
    pub emit: EmitPolicy,
    #[serde(default)]
    pub timeout: Option<DurationSpec>,
    pub config: Value,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EmitPolicy {
    #[default]
    None,
    Content,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationSpec(Duration);

impl DurationSpec {
    pub fn get(self) -> Duration {
        self.0
    }
}

impl<'de> Deserialize<'de> for DurationSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let duration = humantime::parse_duration(&value).map_err(de::Error::custom)?;
        if duration.is_zero() {
            return Err(de::Error::custom("duration must be greater than zero"));
        }
        Ok(Self(duration))
    }
}

impl Serialize for DurationSpec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&humantime::format_duration(self.0).to_string())
    }
}

impl fmt::Display for DurationSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        humantime::format_duration(self.0).fmt(formatter)
    }
}

pub fn parse_raw_agent(yaml: &str) -> Result<RawAgent, CompileError> {
    let agent: RawAgent =
        serde_yaml::from_str(yaml).map_err(|error| CompileError::yaml(error.to_string()))?;
    if agent.version != 1 {
        return Err(CompileError::unsupported_version(agent.version));
    }
    Ok(agent)
}
