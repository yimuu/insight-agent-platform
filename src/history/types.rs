use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::outcome::{RunFailure, RunOutput};

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
    #[serde(flatten)]
    pub lifecycle: RunLifecycle,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub input_summary: Value,
}

impl RunRecord {
    pub fn status(&self) -> RunStatus {
        self.lifecycle.status()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunSummary {
    pub run_id: String,
    pub request_id: String,
    pub agent_id: String,
    pub agent_version: String,
    pub attachment: RunAttachment,
    #[serde(flatten)]
    pub lifecycle: RunSummaryLifecycle,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl RunSummary {
    pub fn status(&self) -> RunStatus {
        match self.lifecycle {
            RunSummaryLifecycle::Created => RunStatus::Created,
            RunSummaryLifecycle::Running => RunStatus::Running,
            RunSummaryLifecycle::Completed => RunStatus::Completed,
            RunSummaryLifecycle::Failed { .. } => RunStatus::Failed,
            RunSummaryLifecycle::Cancelled { .. } => RunStatus::Cancelled,
            RunSummaryLifecycle::Interrupted { .. } => RunStatus::Interrupted,
        }
    }
}

impl From<&RunRecord> for RunSummary {
    fn from(record: &RunRecord) -> Self {
        Self {
            run_id: record.run_id.clone(),
            request_id: record.request_id.clone(),
            agent_id: record.agent_id.clone(),
            agent_version: record.agent_version.clone(),
            attachment: record.attachment,
            lifecycle: RunSummaryLifecycle::from(&record.lifecycle),
            started_at: record.started_at,
            ended_at: record.ended_at,
            updated_at: record.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RunLifecycle {
    Created,
    Running,
    Completed { output: RunOutput },
    Failed { error: RunFailure },
    Cancelled { error: StopError },
    Interrupted { error: StopError },
}

impl RunLifecycle {
    pub fn status(&self) -> RunStatus {
        match self {
            Self::Created => RunStatus::Created,
            Self::Running => RunStatus::Running,
            Self::Completed { .. } => RunStatus::Completed,
            Self::Failed { .. } => RunStatus::Failed,
            Self::Cancelled { .. } => RunStatus::Cancelled,
            Self::Interrupted { .. } => RunStatus::Interrupted,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StopError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RunSummaryLifecycle {
    Created,
    Running,
    Completed,
    Failed { error: RunFailure },
    Cancelled { error: StopError },
    Interrupted { error: StopError },
}

impl From<&RunLifecycle> for RunSummaryLifecycle {
    fn from(lifecycle: &RunLifecycle) -> Self {
        match lifecycle {
            RunLifecycle::Created => Self::Created,
            RunLifecycle::Running => Self::Running,
            RunLifecycle::Completed { .. } => Self::Completed,
            RunLifecycle::Failed { error } => Self::Failed {
                error: error.clone(),
            },
            RunLifecycle::Cancelled { error } => Self::Cancelled {
                error: error.clone(),
            },
            RunLifecycle::Interrupted { error } => Self::Interrupted {
                error: error.clone(),
            },
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

#[derive(Debug, Clone, PartialEq)]
pub enum RunTerminal {
    Completed { output: RunOutput },
    Failed { error: RunFailure },
    Cancelled { error: StopError },
    Interrupted { error: StopError },
}

impl RunTerminal {
    pub fn status(&self) -> RunStatus {
        match self {
            Self::Completed { .. } => RunStatus::Completed,
            Self::Failed { .. } => RunStatus::Failed,
            Self::Cancelled { .. } => RunStatus::Cancelled,
            Self::Interrupted { .. } => RunStatus::Interrupted,
        }
    }

    pub fn output(&self) -> Option<&RunOutput> {
        match self {
            Self::Completed { output } => Some(output),
            _ => None,
        }
    }

    pub fn failure(&self) -> Option<&RunFailure> {
        match self {
            Self::Failed { error } => Some(error),
            _ => None,
        }
    }

    pub fn stop_error(&self) -> Option<&StopError> {
        match self {
            Self::Cancelled { error } | Self::Interrupted { error } => Some(error),
            _ => None,
        }
    }

    pub fn error_code(&self) -> Option<&str> {
        self.failure()
            .map(|error| error.code.as_str())
            .or_else(|| self.stop_error().map(|error| error.code.as_str()))
    }

    pub fn error_message(&self) -> Option<&str> {
        self.failure()
            .map(|error| error.message.as_str())
            .or_else(|| self.stop_error().map(|error| error.message.as_str()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TerminalUpdate {
    pub run_id: String,
    pub ended_at: DateTime<Utc>,
    pub terminal: RunTerminal,
}

impl TerminalUpdate {
    pub fn new(run_id: impl Into<String>, ended_at: DateTime<Utc>, terminal: RunTerminal) -> Self {
        Self {
            run_id: run_id.into(),
            ended_at,
            terminal,
        }
    }

    pub fn status(&self) -> RunStatus {
        self.terminal.status()
    }
}

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
