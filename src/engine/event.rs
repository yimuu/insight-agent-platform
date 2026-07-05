use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::response::CODE_OK;

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
    #[serde(rename = "event")]
    pub event: RunEventKind,
    pub run_id: String,
    pub agent_id: String,
    pub step_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub content: String,
    pub result: Value,
    #[serde(skip)]
    pub code: i32,
    #[serde(skip)]
    pub message: String,
}

impl RunEvent {
    pub fn ok(
        event: RunEventKind,
        run_id: String,
        agent_id: String,
        step_id: Option<String>,
        content: impl Into<String>,
        result: Value,
    ) -> Self {
        Self {
            event,
            run_id,
            agent_id,
            step_id,
            timestamp: Utc::now(),
            content: content.into(),
            result,
            code: CODE_OK,
            message: "ok".to_string(),
        }
    }

    pub fn error(
        run_id: String,
        agent_id: String,
        step_id: Option<String>,
        code: i32,
        message: impl Into<String>,
    ) -> Self {
        Self {
            event: RunEventKind::Error,
            run_id,
            agent_id,
            step_id,
            timestamp: Utc::now(),
            content: String::new(),
            result: Value::Null,
            code,
            message: message.into(),
        }
    }
}
