use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunEventKind {
    RunStarted,
    StepStarted,
    TokenDelta,
    ToolCallStarted,
    ToolCallCompleted,
    StepCompleted,
    RunCompleted,
    Error,
}

impl RunEventKind {
    pub fn as_sse_name(self) -> &'static str {
        match self {
            Self::RunStarted => "run_started",
            Self::StepStarted => "step_started",
            Self::TokenDelta => "token_delta",
            Self::ToolCallStarted => "tool_call_started",
            Self::ToolCallCompleted => "tool_call_completed",
            Self::StepCompleted => "step_completed",
            Self::RunCompleted => "run_completed",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEvent {
    pub kind: RunEventKind,
    pub run_id: String,
    pub agent_id: String,
    pub step_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub payload: Value,
}
