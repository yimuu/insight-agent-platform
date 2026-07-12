use std::{collections::BTreeMap, fmt, time::Duration};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
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
        let duration = parse_formal_duration(&value).map_err(serde::de::Error::custom)?;
        Ok(Self(duration))
    }
}

impl Serialize for DurationSpec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format_formal_duration(self.0))
    }
}

impl fmt::Display for DurationSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&format_formal_duration(self.0))
    }
}

fn parse_formal_duration(value: &str) -> Result<Duration, String> {
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 1_u64)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000_u64)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60_000_u64)
    } else {
        return Err("duration must match a positive integer followed by ms, s, or m".to_string());
    };
    if number.is_empty()
        || number.starts_with('0')
        || !number.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("duration must match a positive integer followed by ms, s, or m".to_string());
    }
    let amount = number
        .parse::<u64>()
        .map_err(|_| "duration is too large".to_string())?;
    let millis = amount
        .checked_mul(multiplier)
        .ok_or_else(|| "duration is too large".to_string())?;
    Ok(Duration::from_millis(millis))
}

fn format_formal_duration(duration: Duration) -> String {
    let millis = duration.as_millis();
    if millis.is_multiple_of(60_000) {
        format!("{}m", millis / 60_000)
    } else if millis.is_multiple_of(1_000) {
        format!("{}s", millis / 1_000)
    } else {
        format!("{millis}ms")
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
