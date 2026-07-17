use tokio::sync::Mutex;

use crate::history::types::RunStatus;

use super::RunError;

/// In-process lifecycle guard for one Run.
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
            return Err(RunError::infrastructure(
                "RUN_STATE_INVALID",
                format!("cannot start run in '{}' state", status.as_str()),
            ));
        }
        *status = RunStatus::Running;
        Ok(())
    }

    pub async fn try_terminal(&self, next: RunStatus) -> Result<bool, RunError> {
        if !next.is_terminal() {
            return Err(RunError::infrastructure(
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
