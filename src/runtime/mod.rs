pub mod attachment;
pub mod context;
pub mod control;
pub mod coordinator;
pub mod service;
pub mod state;

use std::{error::Error, fmt};

pub use attachment::{AttachedRun, RunSubscription};
pub use context::{RunContext, RunMetadata};
pub use control::{stop_pair, ExecutionControl, StopController, StopReason, StopSignal};
pub use coordinator::RunCoordinator;
pub use service::{
    CompiledAgentRegistry, RequestMetadata, RunService, RunServiceConfig, ServiceError,
};
pub use state::RunState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunError {
    code: &'static str,
    message: String,
}

impl RunError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn stopped(reason: StopReason) -> Self {
        match reason {
            StopReason::Cancelled => Self::new("RUN_CANCELLED", "run cancelled"),
            StopReason::Interrupted => Self::new("RUN_INTERRUPTED", "run interrupted"),
            StopReason::TimedOut => Self::new("RUN_TIMEOUT", "run timed out"),
        }
    }

    pub fn timeout() -> Self {
        Self::new("NODE_TIMEOUT", "node execution timed out")
    }
}

impl fmt::Display for RunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for RunError {}
