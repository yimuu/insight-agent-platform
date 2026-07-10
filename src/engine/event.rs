use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::response::CODE_OK;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunEventType {
    #[serde(rename = "run.started")]
    RunStarted,
    #[serde(rename = "step.started")]
    StepStarted,
    #[serde(rename = "thinking.delta")]
    ThinkingDelta,
    #[serde(rename = "content.delta")]
    ContentDelta,
    #[serde(rename = "tool_call.started")]
    ToolCallStarted,
    #[serde(rename = "tool_call.completed")]
    ToolCallCompleted,
    #[serde(rename = "step.completed")]
    StepCompleted,
    #[serde(rename = "step.failed")]
    StepFailed,
    #[serde(rename = "run.completed")]
    RunCompleted,
    #[serde(rename = "run.failed")]
    RunFailed,
    #[serde(rename = "run.cancelled")]
    RunCancelled,
}

impl RunEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RunStarted => "run.started",
            Self::StepStarted => "step.started",
            Self::ThinkingDelta => "thinking.delta",
            Self::ContentDelta => "content.delta",
            Self::ToolCallStarted => "tool_call.started",
            Self::ToolCallCompleted => "tool_call.completed",
            Self::StepCompleted => "step.completed",
            Self::StepFailed => "step.failed",
            Self::RunCompleted => "run.completed",
            Self::RunFailed => "run.failed",
            Self::RunCancelled => "run.cancelled",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEvent {
    #[serde(rename = "type")]
    pub event_type: RunEventType,
    pub seq: u64,
    pub request_id: String,
    pub run_id: String,
    pub agent_id: String,
    #[serde(rename = "time")]
    pub timestamp: DateTime<Utc>,
    pub code: i32,
    pub message: String,
    pub data: Value,
    #[serde(skip)]
    pub step_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RunEventScope {
    pub request_id: String,
    pub run_id: String,
    pub agent_id: String,
    pub step_id: Option<String>,
}

impl RunEvent {
    pub fn ok(event_type: RunEventType, seq: u64, scope: RunEventScope, data: Value) -> Self {
        Self {
            event_type,
            seq,
            request_id: scope.request_id,
            run_id: scope.run_id,
            agent_id: scope.agent_id,
            step_id: scope.step_id,
            timestamp: Utc::now(),
            code: CODE_OK,
            message: "ok".to_string(),
            data,
        }
    }

    pub fn failed(
        event_type: RunEventType,
        seq: u64,
        scope: RunEventScope,
        code: i32,
        message: impl Into<String>,
    ) -> Self {
        let step_id_data = scope
            .step_id
            .as_ref()
            .map(|value| json!({ "step_id": value, "status": "failed" }))
            .unwrap_or_else(|| json!({ "status": "failed" }));

        Self {
            event_type,
            seq,
            request_id: scope.request_id,
            run_id: scope.run_id,
            agent_id: scope.agent_id,
            step_id: scope.step_id,
            timestamp: Utc::now(),
            code,
            message: message.into(),
            data: step_id_data,
        }
    }
}
