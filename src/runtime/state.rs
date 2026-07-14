use serde::Serialize;
use tokio::sync::Mutex;

use crate::{history::types::RunStatus, outcome::RunOutput};

use super::RunError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Pending,
    Ready,
    Running,
    Succeeded,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchState {
    Pending,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchFailureKind {
    Workflow,
    Node,
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BranchError {
    pub kind: BranchFailureKind,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BranchResult {
    Succeeded {
        terminal_node_id: String,
        output: RunOutput,
    },
    Failed {
        terminal_node_id: String,
        error: BranchError,
    },
}

pub struct RunState {
    status: Mutex<RunStatus>,
}

impl RunState {
    pub fn new() -> Self {
        Self {
            status: Mutex::new(RunStatus::Created),
        }
    }

    pub async fn start(&self) -> Result<(), RunError> {
        let mut status = self.status.lock().await;
        if *status != RunStatus::Created {
            return Err(RunError::new(
                "RUN_STATE_INVALID",
                format!("cannot start run in '{}' state", status.as_str()),
            ));
        }
        *status = RunStatus::Running;
        Ok(())
    }

    pub async fn try_terminal(&self, next: RunStatus) -> Result<bool, RunError> {
        if !next.is_terminal() {
            return Err(RunError::new(
                "RUN_STATE_INVALID",
                format!("status '{}' is not terminal", next.as_str()),
            ));
        }
        let mut status = self.status.lock().await;
        if status.is_terminal() {
            return Ok(false);
        }
        *status = next;
        Ok(true)
    }

    pub async fn status(&self) -> RunStatus {
        *self.status.lock().await
    }
}

impl Default for RunState {
    fn default() -> Self {
        Self::new()
    }
}
