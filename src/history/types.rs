use std::{error::Error, fmt};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::dsl::compiled::RunOutput;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Created,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "created" => Some(Self::Created),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "interrupted" => Some(Self::Interrupted),
            _ => None,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunAttachment {
    Attached,
    Detached,
}

impl RunAttachment {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Attached => "attached",
            Self::Detached => "detached",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "attached" => Some(Self::Attached),
            "detached" => Some(Self::Detached),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunRecord {
    pub run_id: String,
    pub request_id: String,
    pub agent_id: String,
    pub agent_version: String,
    pub attachment: RunAttachment,
    pub status: RunStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub input_summary: Value,
    pub output: Option<RunOutput>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunSummary {
    pub run_id: String,
    pub request_id: String,
    pub agent_id: String,
    pub agent_version: String,
    pub attachment: RunAttachment,
    pub status: RunStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl From<&RunRecord> for RunSummary {
    fn from(record: &RunRecord) -> Self {
        Self {
            run_id: record.run_id.clone(),
            request_id: record.request_id.clone(),
            agent_id: record.agent_id.clone(),
            agent_version: record.agent_version.clone(),
            attachment: record.attachment,
            status: record.status,
            started_at: record.started_at,
            ended_at: record.ended_at,
            updated_at: record.updated_at,
            error_code: record.error_code.clone(),
            error_message: record.error_message.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewRun {
    pub run_id: String,
    pub request_id: String,
    pub agent_id: String,
    pub agent_version: String,
    pub attachment: RunAttachment,
    pub created_at: DateTime<Utc>,
    pub input_summary: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeOutputRecord {
    pub run_id: String,
    pub node_id: String,
    pub output: Value,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TerminalUpdate {
    pub run_id: String,
    pub status: RunStatus,
    pub ended_at: DateTime<Utc>,
    pub output: Option<RunOutput>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl TerminalUpdate {
    pub fn new(
        run_id: impl Into<String>,
        status: RunStatus,
        ended_at: DateTime<Utc>,
        output: Option<RunOutput>,
        error_code: Option<String>,
        error_message: Option<String>,
    ) -> Result<Self, HistoryTypeError> {
        if !status.is_terminal() {
            return Err(HistoryTypeError::new(
                "TERMINAL_STATUS_REQUIRED",
                format!("status '{}' is not terminal", status.as_str()),
            ));
        }
        Ok(Self {
            run_id: run_id.into(),
            status,
            ended_at,
            output,
            error_code,
            error_message,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryTypeError {
    code: &'static str,
    message: String,
}

impl HistoryTypeError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Display for HistoryTypeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for HistoryTypeError {}

pub fn summarize_input(input: &Value) -> Value {
    let mut keys = input
        .as_object()
        .map(|object| object.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    keys.sort();
    let serialized_bytes = serde_json::to_vec(input).map_or(0, |bytes| bytes.len());
    json!({
        "keys": keys,
        "serialized_bytes": serialized_bytes,
    })
}
