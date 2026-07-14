pub mod attachment;
pub mod context;
pub mod control;
pub mod coordinator;
pub mod execution;
pub mod scheduler;
pub mod service;
pub mod state;

use std::{error::Error, fmt};

pub use attachment::{AttachedRun, RunSubscription};
pub use context::{RunContext, RunMetadata};
pub use control::{stop_pair, ExecutionControl, StopController, StopReason, StopSignal};
pub use coordinator::RunCoordinator;
pub(crate) use execution::execute_node_with_cancellation;
pub use execution::{execute_node, ExecutionLimiter, NodeExecutionFailure, NodeExecutionResult};
pub use scheduler::{Scheduler, SchedulerResult};
pub use service::{
    CompiledAgentRegistry, RequestMetadata, RunService, RunServiceConfig, ServiceError,
};
pub use state::{BranchError, BranchFailureKind, BranchResult, BranchState, NodeState, RunState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunErrorKind {
    Node,
    Timeout,
    Stop,
    Infrastructure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunError {
    code: &'static str,
    message: String,
    kind: RunErrorKind,
    stop_reason: Option<StopReason>,
}

impl RunError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            kind: RunErrorKind::Node,
            stop_reason: None,
        }
    }

    pub fn infrastructure(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            kind: RunErrorKind::Infrastructure,
            stop_reason: None,
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

    pub fn stop_reason(&self) -> Option<StopReason> {
        self.stop_reason
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
            stop_reason: Some(reason),
        }
    }

    pub fn timeout() -> Self {
        Self {
            code: "NODE_TIMEOUT",
            message: "node execution timed out".to_string(),
            kind: RunErrorKind::Timeout,
            stop_reason: None,
        }
    }
}

impl fmt::Display for RunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for RunError {}
