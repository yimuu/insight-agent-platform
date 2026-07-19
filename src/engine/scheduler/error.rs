use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

pub const SCHEDULER_FACT_MISSING: &str = "ENGINE_SCHEDULER_FACT_MISSING";
pub const SCHEDULER_FACT_INCONSISTENT: &str = "ENGINE_SCHEDULER_FACT_INCONSISTENT";
pub const SCHEDULER_VALUE_TYPE_MISMATCH: &str = "ENGINE_SCHEDULER_VALUE_TYPE_MISMATCH";
pub const SCHEDULER_EXPRESSION_INVALID: &str = "ENGINE_SCHEDULER_EXPRESSION_INVALID";
pub const SCHEDULER_GRAPH_INVALID: &str = "ENGINE_SCHEDULER_GRAPH_INVALID";
pub const SCHEDULER_ID_INVALID: &str = "ENGINE_SCHEDULER_ID_INVALID";
pub const SCHEDULER_DYNAMIC_KEY_DUPLICATE: &str = "ENGINE_SCHEDULER_DYNAMIC_KEY_DUPLICATE";
pub const SCHEDULER_LOOP_BUDGET_EXCEEDED: &str = "ENGINE_SCHEDULER_LOOP_BUDGET_EXCEEDED";

pub const SCHEDULER_PUBLIC_EXPRESSION_FAILURE: &str = "SCHEDULER_EXPRESSION_FAILURE";
pub const SCHEDULER_PUBLIC_DYNAMIC_KEY_DUPLICATE: &str = "SCHEDULER_DYNAMIC_KEY_DUPLICATE";
pub const SCHEDULER_PUBLIC_LOOP_BUDGET_EXCEEDED: &str = "SCHEDULER_LOOP_BUDGET_EXCEEDED";
pub const SCHEDULER_PUBLIC_INVARIANT_FAILURE: &str = "SCHEDULER_INVARIANT_FAILURE";

/// Closed durable classification for a planner failure. The planner's human
/// message is deliberately not persisted: it can mention implementation
/// details and is not required to replay the terminal decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerPlanningFailure {
    ExpressionInvalid,
    ValueTypeMismatch,
    DynamicKeyDuplicate,
    LoopBudgetExceeded,
    FactMissing,
    FactInconsistent,
    GraphInvalid,
    IdInvalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerPlanningFailureKind {
    Workflow,
    Invariant,
}

impl SchedulerPlanningFailure {
    pub fn from_error(error: &SchedulerError) -> Self {
        match error.code() {
            SCHEDULER_EXPRESSION_INVALID => Self::ExpressionInvalid,
            SCHEDULER_VALUE_TYPE_MISMATCH => Self::ValueTypeMismatch,
            SCHEDULER_DYNAMIC_KEY_DUPLICATE => Self::DynamicKeyDuplicate,
            SCHEDULER_LOOP_BUDGET_EXCEEDED => Self::LoopBudgetExceeded,
            SCHEDULER_FACT_MISSING => Self::FactMissing,
            SCHEDULER_FACT_INCONSISTENT => Self::FactInconsistent,
            SCHEDULER_GRAPH_INVALID => Self::GraphInvalid,
            SCHEDULER_ID_INVALID => Self::IdInvalid,
            // SchedulerError construction is module-private, but defaulting a
            // future code to invariant is the fail-closed behavior.
            _ => Self::FactInconsistent,
        }
    }

    pub fn kind(self) -> SchedulerPlanningFailureKind {
        match self {
            Self::ExpressionInvalid
            | Self::ValueTypeMismatch
            | Self::DynamicKeyDuplicate
            | Self::LoopBudgetExceeded => SchedulerPlanningFailureKind::Workflow,
            Self::FactMissing | Self::FactInconsistent | Self::GraphInvalid | Self::IdInvalid => {
                SchedulerPlanningFailureKind::Invariant
            }
        }
    }

    pub fn internal_code(self) -> &'static str {
        match self {
            Self::ExpressionInvalid => SCHEDULER_EXPRESSION_INVALID,
            Self::ValueTypeMismatch => SCHEDULER_VALUE_TYPE_MISMATCH,
            Self::DynamicKeyDuplicate => SCHEDULER_DYNAMIC_KEY_DUPLICATE,
            Self::LoopBudgetExceeded => SCHEDULER_LOOP_BUDGET_EXCEEDED,
            Self::FactMissing => SCHEDULER_FACT_MISSING,
            Self::FactInconsistent => SCHEDULER_FACT_INCONSISTENT,
            Self::GraphInvalid => SCHEDULER_GRAPH_INVALID,
            Self::IdInvalid => SCHEDULER_ID_INVALID,
        }
    }

    pub fn public_code(self) -> &'static str {
        match self {
            Self::ExpressionInvalid | Self::ValueTypeMismatch => {
                SCHEDULER_PUBLIC_EXPRESSION_FAILURE
            }
            Self::DynamicKeyDuplicate => SCHEDULER_PUBLIC_DYNAMIC_KEY_DUPLICATE,
            Self::LoopBudgetExceeded => SCHEDULER_PUBLIC_LOOP_BUDGET_EXCEEDED,
            Self::FactMissing | Self::FactInconsistent | Self::GraphInvalid | Self::IdInvalid => {
                SCHEDULER_PUBLIC_INVARIANT_FAILURE
            }
        }
    }
}

/// Stable, body-free scheduler planning error.
///
/// The planner never embeds task output, run input, expression values, or
/// secret material in this error. Callers can therefore persist the code and
/// message as internal diagnostics without leaking workflow data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerError {
    code: &'static str,
    message: String,
}

impl SchedulerError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
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
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for SchedulerError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planning_failure_taxonomy_is_closed_and_fail_closed() {
        for (code, expected, kind) in [
            (
                SCHEDULER_EXPRESSION_INVALID,
                SchedulerPlanningFailure::ExpressionInvalid,
                SchedulerPlanningFailureKind::Workflow,
            ),
            (
                SCHEDULER_VALUE_TYPE_MISMATCH,
                SchedulerPlanningFailure::ValueTypeMismatch,
                SchedulerPlanningFailureKind::Workflow,
            ),
            (
                SCHEDULER_DYNAMIC_KEY_DUPLICATE,
                SchedulerPlanningFailure::DynamicKeyDuplicate,
                SchedulerPlanningFailureKind::Workflow,
            ),
            (
                SCHEDULER_LOOP_BUDGET_EXCEEDED,
                SchedulerPlanningFailure::LoopBudgetExceeded,
                SchedulerPlanningFailureKind::Workflow,
            ),
            (
                SCHEDULER_FACT_MISSING,
                SchedulerPlanningFailure::FactMissing,
                SchedulerPlanningFailureKind::Invariant,
            ),
            (
                SCHEDULER_FACT_INCONSISTENT,
                SchedulerPlanningFailure::FactInconsistent,
                SchedulerPlanningFailureKind::Invariant,
            ),
            (
                SCHEDULER_GRAPH_INVALID,
                SchedulerPlanningFailure::GraphInvalid,
                SchedulerPlanningFailureKind::Invariant,
            ),
            (
                SCHEDULER_ID_INVALID,
                SchedulerPlanningFailure::IdInvalid,
                SchedulerPlanningFailureKind::Invariant,
            ),
        ] {
            let failure = SchedulerPlanningFailure::from_error(&SchedulerError::new(code, "body"));
            assert_eq!(failure, expected);
            assert_eq!(failure.kind(), kind);
            assert_eq!(failure.internal_code(), code);
            assert!(!failure.public_code().starts_with("ENGINE_"));
        }

        let future = SchedulerPlanningFailure::from_error(&SchedulerError::new(
            "ENGINE_SCHEDULER_FUTURE_CODE",
            "body",
        ));
        assert_eq!(future, SchedulerPlanningFailure::FactInconsistent);
        assert_eq!(future.kind(), SchedulerPlanningFailureKind::Invariant);
    }
}
