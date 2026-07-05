use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::providers::ModelType;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgentConfig {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub model: ModelConfig,
    pub input: InputConfig,
    #[serde(default)]
    pub prompts: BTreeMap<String, String>,
    pub steps: Vec<StepConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModelConfig {
    pub provider: String,
    #[serde(default, rename = "type")]
    pub model_type: ModelType,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub options: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InputConfig {
    pub schema: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StepConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: StepKind,
    #[serde(default)]
    pub prompt_ref: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub system_prompt_ref: Option<String>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub image_input: Option<String>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub tool: Option<String>,
    #[serde(default)]
    pub args: Value,
}

impl StepConfig {
    pub fn prompt_source(&self) -> Option<PromptSource<'_>> {
        match (&self.prompt_ref, &self.prompt) {
            (Some(prompt_ref), None) => Some(PromptSource::Ref(prompt_ref)),
            (None, Some(prompt)) => Some(PromptSource::Inline(prompt)),
            _ => None,
        }
    }

    pub fn system_prompt_source(&self) -> Option<PromptSource<'_>> {
        match (&self.system_prompt_ref, &self.system_prompt) {
            (Some(prompt_ref), None) => Some(PromptSource::Ref(prompt_ref)),
            (None, Some(prompt)) => Some(PromptSource::Inline(prompt)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromptSource<'a> {
    Ref(&'a str),
    Inline(&'a str),
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    Prompt,
    Text,
    Llm,
    Tool,
}
