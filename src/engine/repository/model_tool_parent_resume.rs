use super::RepositoryErrorExt as _;

use std::{collections::BTreeSet, fmt};

use chrono::{DateTime, Utc};

use crate::engine::{worker::ModelContinuationTurn, EffectEvidence};

use super::RepositoryError;

const MAX_PARENT_CONTINUATION_TURNS: usize = 1_024;
const MAX_PARENT_CONTINUATION_CALLS: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParentTaskClaimClass {
    InitialExecute,
    ActivateCheckpointed,
    ContinueReady,
    FinalizeLeaseLoss,
    Acknowledge,
    Ineligible,
}

impl ParentTaskClaimClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::InitialExecute => "initial_execute",
            Self::ActivateCheckpointed => "activate_checkpointed",
            Self::ContinueReady => "continue_ready",
            Self::FinalizeLeaseLoss => "finalize_lease_loss",
            Self::Acknowledge => "acknowledge",
            Self::Ineligible => "ineligible",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LatestParentModelCall {
    pub(crate) model_call_no: u32,
    pub(crate) task_id: String,
    pub(crate) lease_epoch: u64,
    pub(crate) fencing_token: String,
    pub(crate) call_status: String,
    pub(crate) finish_reason: Option<String>,
    pub(crate) execution_status: Option<String>,
    pub(crate) continuation_status: Option<String>,
}

impl LatestParentModelCall {
    pub(crate) fn is_tool_checkpoint(&self) -> bool {
        self.call_status == "completed" && self.finish_reason.as_deref() == Some("tool_calls")
    }

    pub(crate) fn is_waiting_tools(&self) -> bool {
        self.execution_status.as_deref() == Some("active")
            && self.continuation_status.as_deref() == Some("waiting_tools")
    }

    pub(crate) fn is_checkpointed(&self) -> bool {
        self.is_tool_checkpoint()
            && self.execution_status.as_deref() == Some("checkpointed")
            && self.continuation_status.as_deref() == Some("checkpointed")
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.is_tool_checkpoint()
            && matches!(
                (
                    self.execution_status.as_deref(),
                    self.continuation_status.as_deref()
                ),
                (Some("succeeded"), Some("ready_continue"))
                    | (Some("failed"), Some("ready_failed"))
                    | (Some("cancelled"), Some("ready_cancelled"))
            )
    }
}

pub(crate) fn classify_parent_task_claim(
    task_state: &str,
    attempt_lifecycle: &str,
    effect_evidence: EffectEvidence,
    latest: Option<&LatestParentModelCall>,
) -> ParentTaskClaimClass {
    if latest.is_some_and(LatestParentModelCall::is_waiting_tools) {
        return ParentTaskClaimClass::Ineligible;
    }
    if task_state == "published" {
        return ParentTaskClaimClass::Acknowledge;
    }
    match (task_state, attempt_lifecycle, effect_evidence, latest) {
        ("pending", "leased", EffectEvidence::NotStarted, None) => {
            ParentTaskClaimClass::InitialExecute
        }
        ("claimed", "running", EffectEvidence::Started, Some(latest))
            if latest.is_checkpointed() =>
        {
            ParentTaskClaimClass::ActivateCheckpointed
        }
        ("pending", "running", EffectEvidence::Started, Some(latest)) if latest.is_ready() => {
            ParentTaskClaimClass::ContinueReady
        }
        ("claimed", _, _, _) => ParentTaskClaimClass::FinalizeLeaseLoss,
        _ => ParentTaskClaimClass::Ineligible,
    }
}

/// Exact durable state required to resume one already-running parent LLM
/// Attempt after a model/tool round.  It deliberately carries the original
/// operation deadline; reclaiming scheduler authority must never reset the
/// Attempt budget.
#[derive(Clone, PartialEq, Eq)]
pub enum ModelToolParentResume {
    /// The Provider completion and complete call batch are durable, but the
    /// queue activation transaction did not commit before the worker stopped.
    ActivateCheckpointed {
        model_call_no: u32,
        operation_deadline: DateTime<Utc>,
    },
    /// Every tool in all completed rounds succeeded.  The transcript is exact
    /// provider input for `next_model_call_no` and contains no mutable-catalog
    /// evidence.
    ReadyContinue {
        completed_model_call_no: u32,
        next_model_call_no: u32,
        operation_deadline: DateTime<Utc>,
        turns: Vec<ModelContinuationTurn>,
    },
    /// At least one tool failed durably. `effect_outcome_unknown` is retained
    /// as a closed classification, never as a raw worker error body.
    ReadyFailed {
        completed_model_call_no: u32,
        operation_deadline: DateTime<Utc>,
        effect_outcome_unknown: bool,
    },
    ReadyCancelled {
        completed_model_call_no: u32,
        operation_deadline: DateTime<Utc>,
    },
}

impl ModelToolParentResume {
    pub(crate) fn activate_checkpointed(
        model_call_no: u32,
        operation_deadline: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        if model_call_no == 0 {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self::ActivateCheckpointed {
            model_call_no,
            operation_deadline,
        })
    }

    pub(crate) fn ready_continue(
        turns: Vec<ModelContinuationTurn>,
        operation_deadline: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        if turns.is_empty() || turns.len() > MAX_PARENT_CONTINUATION_TURNS {
            return Err(RepositoryError::invalid_data());
        }
        let mut call_ids = BTreeSet::new();
        let mut call_count = 0usize;
        for (index, turn) in turns.iter().enumerate() {
            let expected_model_call_no = u32::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(1))
                .ok_or_else(RepositoryError::invalid_data)?;
            if turn.model_call_no() != expected_model_call_no {
                return Err(RepositoryError::invalid_data());
            }
            call_count = call_count
                .checked_add(turn.calls().len())
                .ok_or_else(RepositoryError::invalid_data)?;
            if call_count > MAX_PARENT_CONTINUATION_CALLS
                || turn
                    .calls()
                    .iter()
                    .any(|call| !call_ids.insert(call.call_id()))
            {
                return Err(RepositoryError::invalid_data());
            }
        }
        let completed_model_call_no = turns
            .last()
            .map(ModelContinuationTurn::model_call_no)
            .ok_or_else(RepositoryError::invalid_data)?;
        let next_model_call_no = completed_model_call_no
            .checked_add(1)
            .ok_or_else(RepositoryError::invalid_data)?;
        Ok(Self::ReadyContinue {
            completed_model_call_no,
            next_model_call_no,
            operation_deadline,
            turns,
        })
    }

    pub(crate) fn ready_failed(
        completed_model_call_no: u32,
        operation_deadline: DateTime<Utc>,
        effect_outcome_unknown: bool,
    ) -> Result<Self, RepositoryError> {
        if completed_model_call_no == 0 {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self::ReadyFailed {
            completed_model_call_no,
            operation_deadline,
            effect_outcome_unknown,
        })
    }

    pub(crate) fn ready_cancelled(
        completed_model_call_no: u32,
        operation_deadline: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        if completed_model_call_no == 0 {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self::ReadyCancelled {
            completed_model_call_no,
            operation_deadline,
        })
    }

    pub fn operation_deadline(&self) -> DateTime<Utc> {
        match self {
            Self::ActivateCheckpointed {
                operation_deadline, ..
            }
            | Self::ReadyContinue {
                operation_deadline, ..
            }
            | Self::ReadyFailed {
                operation_deadline, ..
            }
            | Self::ReadyCancelled {
                operation_deadline, ..
            } => *operation_deadline,
        }
    }

    pub fn checkpointed_model_call_no(&self) -> Option<u32> {
        match self {
            Self::ActivateCheckpointed { model_call_no, .. } => Some(*model_call_no),
            Self::ReadyContinue { .. } | Self::ReadyFailed { .. } | Self::ReadyCancelled { .. } => {
                None
            }
        }
    }

    pub fn completed_model_call_no(&self) -> Option<u32> {
        match self {
            Self::ActivateCheckpointed { .. } => None,
            Self::ReadyContinue {
                completed_model_call_no,
                ..
            }
            | Self::ReadyFailed {
                completed_model_call_no,
                ..
            }
            | Self::ReadyCancelled {
                completed_model_call_no,
                ..
            } => Some(*completed_model_call_no),
        }
    }

    pub fn next_model_call_no(&self) -> Option<u32> {
        match self {
            Self::ReadyContinue {
                next_model_call_no, ..
            } => Some(*next_model_call_no),
            Self::ActivateCheckpointed { .. }
            | Self::ReadyFailed { .. }
            | Self::ReadyCancelled { .. } => None,
        }
    }

    pub fn turns(&self) -> &[ModelContinuationTurn] {
        match self {
            Self::ReadyContinue { turns, .. } => turns,
            Self::ActivateCheckpointed { .. }
            | Self::ReadyFailed { .. }
            | Self::ReadyCancelled { .. } => &[],
        }
    }

    pub fn effect_outcome_unknown(&self) -> bool {
        matches!(
            self,
            Self::ReadyFailed {
                effect_outcome_unknown: true,
                ..
            }
        )
    }
}

impl fmt::Debug for ModelToolParentResume {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ActivateCheckpointed {
                model_call_no,
                operation_deadline,
            } => formatter
                .debug_struct("ActivateCheckpointed")
                .field("model_call_no", model_call_no)
                .field("operation_deadline", operation_deadline)
                .finish(),
            Self::ReadyContinue {
                completed_model_call_no,
                next_model_call_no,
                operation_deadline,
                turns,
            } => formatter
                .debug_struct("ReadyContinue")
                .field("completed_model_call_no", completed_model_call_no)
                .field("next_model_call_no", next_model_call_no)
                .field("operation_deadline", operation_deadline)
                .field("turn_count", &turns.len())
                .field(
                    "call_count",
                    &turns.iter().map(|turn| turn.calls().len()).sum::<usize>(),
                )
                .finish(),
            Self::ReadyFailed {
                completed_model_call_no,
                operation_deadline,
                effect_outcome_unknown,
            } => formatter
                .debug_struct("ReadyFailed")
                .field("completed_model_call_no", completed_model_call_no)
                .field("operation_deadline", operation_deadline)
                .field("effect_outcome_unknown", effect_outcome_unknown)
                .finish(),
            Self::ReadyCancelled {
                completed_model_call_no,
                operation_deadline,
            } => formatter
                .debug_struct("ReadyCancelled")
                .field("completed_model_call_no", completed_model_call_no)
                .field("operation_deadline", operation_deadline)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use serde_json::json;

    use crate::engine::worker::{ModelContinuationTurn, ModelToolCall, ModelToolResult};

    use super::ModelToolParentResume;

    fn turn(model_call_no: u32, call_id: &str) -> ModelContinuationTurn {
        ModelContinuationTurn::new(
            model_call_no,
            None,
            vec![
                ModelToolCall::new(0, call_id, "lookup", json!({"query": model_call_no})).unwrap(),
            ],
            vec![ModelToolResult::new(call_id, json!({"answer": model_call_no})).unwrap()],
        )
        .unwrap()
    }

    #[test]
    fn ready_continuation_derives_the_next_round_and_hides_bodies_from_debug() {
        let deadline = Utc::now() + Duration::minutes(1);
        let resume = ModelToolParentResume::ready_continue(
            vec![turn(1, "call_a"), turn(2, "call_b")],
            deadline,
        )
        .unwrap();

        assert_eq!(resume.completed_model_call_no(), Some(2));
        assert_eq!(resume.next_model_call_no(), Some(3));
        assert_eq!(resume.turns().len(), 2);
        assert_eq!(resume.operation_deadline(), deadline);
        let debug = format!("{resume:?}");
        assert!(debug.contains("turn_count: 2"));
        assert!(!debug.contains("query"));
        assert!(!debug.contains("answer"));
        assert!(!debug.contains("call_a"));
    }

    #[test]
    fn ready_continuation_rejects_round_gaps_and_cross_round_call_id_reuse() {
        let deadline = Utc::now();
        assert!(ModelToolParentResume::ready_continue(vec![turn(2, "call_a")], deadline).is_err());
        assert!(ModelToolParentResume::ready_continue(
            vec![turn(1, "call_a"), turn(2, "call_a")],
            deadline,
        )
        .is_err());
    }

    #[test]
    fn closed_resume_variants_reject_zero_model_call_numbers() {
        let deadline = Utc::now();
        assert!(ModelToolParentResume::activate_checkpointed(0, deadline).is_err());
        assert!(ModelToolParentResume::ready_failed(0, deadline, false).is_err());
        assert!(ModelToolParentResume::ready_cancelled(0, deadline).is_err());

        let failed = ModelToolParentResume::ready_failed(1, deadline, true).unwrap();
        assert!(failed.effect_outcome_unknown());
        assert!(failed.turns().is_empty());
    }
}
