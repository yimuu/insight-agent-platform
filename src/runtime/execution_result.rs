use crate::outcome::TerminalOutcome;

use super::RunError;

/// Result of executing one verified workflow, before durable terminal mapping.
#[derive(Debug, Clone, PartialEq)]
pub enum RunExecutionResult {
    Ended(TerminalOutcome),
    Failed(RunError),
    Stopped(RunError),
}
