use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndOutcomeKind {
    Success,
    Failure,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    pub data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum TerminalOutcome {
    Success { output: RunOutput },
    Failure { error: WorkflowError },
}

impl TerminalOutcome {
    pub fn kind(&self) -> EndOutcomeKind {
        match self {
            Self::Success { .. } => EndOutcomeKind::Success,
            Self::Failure { .. } => EndOutcomeKind::Failure,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    Workflow,
    Node,
    Timeout,
    Infrastructure,
}

impl FailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Workflow => "workflow",
            Self::Node => "node",
            Self::Timeout => "timeout",
            Self::Infrastructure => "infrastructure",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "workflow" => Some(Self::Workflow),
            "node" => Some(Self::Node),
            "timeout" => Some(Self::Timeout),
            "infrastructure" => Some(Self::Infrastructure),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunFailure {
    pub kind: FailureKind,
    pub code: String,
    pub message: String,
}
