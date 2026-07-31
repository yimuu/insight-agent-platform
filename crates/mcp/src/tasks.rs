//! Official `io.modelcontextprotocol/tasks` extension wire contracts.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{InputRequest, InputResponse, MetaMap};

pub const MCP_TASKS_EXTENSION_ID: &str = "io.modelcontextprotocol/tasks";
const MAX_TASK_ID_BYTES: usize = 8 * 1024;
const MAX_STATUS_MESSAGE_BYTES: usize = 4 * 1024;
const MAX_INPUTS: usize = 32;
const MAX_TTL_MS: u64 = 30 * 24 * 60 * 60 * 1_000;
const MAX_POLL_INTERVAL_MS: u64 = 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Working,
    InputRequired,
    Completed,
    Cancelled,
    Failed,
}

impl TaskStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Task {
    #[serde(rename = "taskId")]
    pub task_id: String,
    pub status: TaskStatus,
    #[serde(
        rename = "statusMessage",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub status_message: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
    #[serde(rename = "lastUpdatedAt")]
    pub last_updated_at: DateTime<Utc>,
    #[serde(rename = "ttlMs")]
    pub ttl_ms: Option<u64>,
    #[serde(
        rename = "pollIntervalMs",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub poll_interval_ms: Option<u64>,
}

impl Task {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.task_id.is_empty()
            || self.task_id.len() > MAX_TASK_ID_BYTES
            || self.task_id.chars().any(char::is_control)
            || self.status_message.as_ref().is_some_and(|message| {
                message.is_empty()
                    || message.len() > MAX_STATUS_MESSAGE_BYTES
                    || message.chars().any(char::is_control)
            })
            || self.created_at > self.last_updated_at
            || self.ttl_ms.is_some_and(|ttl| ttl == 0 || ttl > MAX_TTL_MS)
            || self
                .poll_interval_ms
                .is_some_and(|interval| interval == 0 || interval > MAX_POLL_INTERVAL_MS)
        {
            Err("invalid MCP task")
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTaskResult {
    #[serde(rename = "resultType")]
    pub result_type: String,
    #[serde(flatten)]
    pub task: Task,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

impl CreateTaskResult {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.result_type != "task" {
            return Err("invalid MCP create-task result");
        }
        self.task.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetTaskResult {
    #[serde(rename = "resultType")]
    pub result_type: String,
    #[serde(flatten)]
    pub task: Task,
    #[serde(
        rename = "inputRequests",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub input_requests: Option<BTreeMap<String, InputRequest>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

impl GetTaskResult {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.task.validate()?;
        let inputs_valid = self.input_requests.as_ref().is_some_and(|inputs| {
            !inputs.is_empty()
                && inputs.len() <= MAX_INPUTS
                && inputs.keys().all(|key| {
                    !key.is_empty() && key.len() <= 128 && !key.chars().any(char::is_control)
                })
        });
        let result_valid = self.result.as_ref().is_some_and(Value::is_object);
        let error_valid = self.error.as_ref().is_some_and(Value::is_object);
        let shape_valid = match self.task.status {
            TaskStatus::Working | TaskStatus::Cancelled => {
                self.input_requests.is_none() && self.result.is_none() && self.error.is_none()
            }
            TaskStatus::InputRequired => {
                inputs_valid && self.result.is_none() && self.error.is_none()
            }
            TaskStatus::Completed => {
                self.input_requests.is_none() && result_valid && self.error.is_none()
            }
            TaskStatus::Failed => {
                self.input_requests.is_none() && self.result.is_none() && error_valid
            }
        };
        if self.result_type == "complete" && shape_valid {
            Ok(())
        } else {
            Err("invalid MCP task result")
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateTaskParams {
    #[serde(rename = "taskId")]
    pub task_id: String,
    #[serde(rename = "inputResponses")]
    pub input_responses: BTreeMap<String, InputResponse>,
}

impl UpdateTaskParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        if valid_task_id(&self.task_id)
            && !self.input_responses.is_empty()
            && self.input_responses.len() <= MAX_INPUTS
            && self.input_responses.keys().all(|key| {
                !key.is_empty() && key.len() <= 128 && !key.chars().any(char::is_control)
            })
        {
            Ok(())
        } else {
            Err("invalid MCP task update")
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAcknowledgement {
    #[serde(rename = "resultType")]
    pub result_type: String,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetaMap>,
}

impl TaskAcknowledgement {
    pub fn complete() -> Self {
        Self {
            result_type: "complete".to_owned(),
            metadata: None,
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.result_type == "complete" {
            Ok(())
        } else {
            Err("invalid MCP task acknowledgement")
        }
    }
}

pub fn valid_task_id(task_id: &str) -> bool {
    !task_id.is_empty()
        && task_id.len() <= MAX_TASK_ID_BYTES
        && !task_id.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn task(status: TaskStatus) -> Task {
        Task {
            task_id: "opaque-task".to_owned(),
            status,
            status_message: None,
            created_at: "2026-07-30T10:00:00Z".parse().unwrap(),
            last_updated_at: "2026-07-30T10:00:01Z".parse().unwrap(),
            ttl_ms: Some(60_000),
            poll_interval_ms: Some(250),
        }
    }

    #[test]
    fn status_specific_task_shapes_are_closed() {
        let completed = GetTaskResult {
            result_type: "complete".to_owned(),
            task: task(TaskStatus::Completed),
            input_requests: None,
            result: Some(json!({"content": [], "isError": false})),
            error: None,
            metadata: None,
        };
        completed.validate().unwrap();
        let mut invalid = completed.clone();
        invalid.error = Some(json!({"code": -32603}));
        assert!(invalid.validate().is_err());
        assert!(serde_json::from_value::<CreateTaskResult>(json!({
            "resultType": "task",
            "taskId": "task",
            "status": "working",
            "createdAt": "2026-07-30T10:00:00Z",
            "lastUpdatedAt": "2026-07-30T10:00:00Z",
            "ttlMs": 1000,
            "unexpected": true
        }))
        .is_err());
    }
}
