pub mod attachment;
pub mod context;
pub mod control;
pub mod coordinator;
pub mod execution;
pub mod service;
pub mod state;

use std::{error::Error, fmt};

pub use attachment::{AttachedRun, RunSubscription};
pub use context::{RunContext, RunMetadata};
pub use control::{stop_pair, ExecutionControl, StopController, StopReason, StopSignal};
pub use coordinator::RunCoordinator;
pub use execution::{execute_node, ExecutionLimiter, NodeExecutionFailure, NodeExecutionResult};
pub use service::{
    CompiledAgentRegistry, RequestMetadata, RunService, RunServiceConfig, ServiceError,
};
pub use state::{BranchError, BranchResult, RunState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunErrorKind {
    Node,
    Stop,
    Infrastructure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunError {
    code: &'static str,
    message: String,
    kind: RunErrorKind,
}

impl RunError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            kind: RunErrorKind::Node,
        }
    }

    pub fn infrastructure(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            kind: RunErrorKind::Infrastructure,
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn kind(&self) -> RunErrorKind {
        self.kind
    }

    pub fn stopped(reason: StopReason) -> Self {
        let (code, message) = match reason {
            StopReason::Cancelled => ("RUN_CANCELLED", "run cancelled"),
            StopReason::Interrupted => ("RUN_INTERRUPTED", "run interrupted"),
            StopReason::TimedOut => ("RUN_TIMEOUT", "run timed out"),
        };
        Self {
            code,
            message: message.to_string(),
            kind: RunErrorKind::Stop,
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
