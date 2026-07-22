use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const EVENT_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunEventType {
    #[serde(rename = "run.created")]
    RunCreated,
    #[serde(rename = "run.started")]
    RunStarted,
    #[serde(rename = "operation.started")]
    OperationStarted,
    #[serde(rename = "operation.completed")]
    OperationCompleted,
    #[serde(rename = "operation.failed")]
    OperationFailed,
    #[serde(rename = "run.completed")]
    RunCompleted,
    #[serde(rename = "run.failed")]
    RunFailed,
    #[serde(rename = "run.cancelled")]
    RunCancelled,
    #[serde(rename = "run.interrupted")]
    RunInterrupted,
}

impl RunEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RunCreated => "run.created",
            Self::RunStarted => "run.started",
            Self::OperationStarted => "operation.started",
            Self::OperationCompleted => "operation.completed",
            Self::OperationFailed => "operation.failed",
            Self::RunCompleted => "run.completed",
            Self::RunFailed => "run.failed",
            Self::RunCancelled => "run.cancelled",
            Self::RunInterrupted => "run.interrupted",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "run.created" => Some(Self::RunCreated),
            "run.started" => Some(Self::RunStarted),
            "operation.started" => Some(Self::OperationStarted),
            "operation.completed" => Some(Self::OperationCompleted),
            "operation.failed" => Some(Self::OperationFailed),
            "run.completed" => Some(Self::RunCompleted),
            "run.failed" => Some(Self::RunFailed),
            "run.cancelled" => Some(Self::RunCancelled),
            "run.interrupted" => Some(Self::RunInterrupted),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunEventScope {
    pub request_id: String,
    pub run_id: String,
    pub agent_id: String,
    pub agent_version: String,
}

impl RunEventScope {
    pub fn for_run(
        request_id: impl Into<String>,
        run_id: impl Into<String>,
        agent_id: impl Into<String>,
        agent_version: impl Into<String>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            run_id: run_id.into(),
            agent_id: agent_id.into(),
            agent_version: agent_version.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunEvent {
    pub schema_version: u32,
    #[serde(rename = "type")]
    pub event_type: RunEventType,
    pub seq: u64,
    pub request_id: String,
    pub run_id: String,
    pub agent_id: String,
    pub agent_version: String,
    #[serde(rename = "time")]
    pub timestamp: DateTime<Utc>,
    pub code: String,
    pub message: String,
    pub data: Value,
}

impl RunEvent {
    pub fn ok(event_type: RunEventType, seq: u64, scope: RunEventScope, data: Value) -> Self {
        Self::ok_at(event_type, seq, scope, Utc::now(), data)
    }

    pub fn ok_at(
        event_type: RunEventType,
        seq: u64,
        scope: RunEventScope,
        timestamp: DateTime<Utc>,
        data: Value,
    ) -> Self {
        Self::new(event_type, seq, scope, timestamp, "OK", "ok", data)
    }

    pub fn error(
        event_type: RunEventType,
        seq: u64,
        scope: RunEventScope,
        code: impl Into<String>,
        message: impl Into<String>,
        data: Value,
    ) -> Self {
        Self::error_at(event_type, seq, scope, Utc::now(), code, message, data)
    }

    pub fn error_at(
        event_type: RunEventType,
        seq: u64,
        scope: RunEventScope,
        timestamp: DateTime<Utc>,
        code: impl Into<String>,
        message: impl Into<String>,
        data: Value,
    ) -> Self {
        Self::new(event_type, seq, scope, timestamp, code, message, data)
    }

    fn new(
        event_type: RunEventType,
        seq: u64,
        scope: RunEventScope,
        timestamp: DateTime<Utc>,
        code: impl Into<String>,
        message: impl Into<String>,
        data: Value,
    ) -> Self {
        Self {
            schema_version: EVENT_SCHEMA_VERSION,
            event_type,
            seq,
            request_id: scope.request_id,
            run_id: scope.run_id,
            agent_id: scope.agent_id,
            agent_version: scope.agent_version,
            timestamp,
            code: code.into(),
            message: message.into(),
            data,
        }
    }
}
