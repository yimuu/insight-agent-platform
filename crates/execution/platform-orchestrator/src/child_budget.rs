//! Child duration starts at the database-observed atomic admission time.

use crate::{ChildBudget, OrchestratorError};
use chrono::{DateTime, Duration, Utc};
use insight_platform_plan::ChildBudgetLimit;

pub fn validate_child_budget_limit(limit: &ChildBudgetLimit) -> Result<(), OrchestratorError> {
    if limit.maximum_duration_milliseconds == 0
        || limit.maximum_model_tokens == 0
        || limit.maximum_capability_calls == 0
        || limit.maximum_artifact_bytes == 0
        || limit.maximum_descendant_runs == 0
    {
        return Err(OrchestratorError::InvalidChildBudget);
    }
    child_duration(limit)?;
    Ok(())
}

fn child_duration(limit: &ChildBudgetLimit) -> Result<Duration, OrchestratorError> {
    let milliseconds = i64::try_from(limit.maximum_duration_milliseconds)
        .map_err(|_| OrchestratorError::InvalidChildBudget)?;
    Duration::try_milliseconds(milliseconds).ok_or(OrchestratorError::InvalidChildBudget)
}

pub fn derive_child_budget(
    limit: &ChildBudgetLimit,
    admitted_at: DateTime<Utc>,
    inherited_deadline: DateTime<Utc>,
) -> Result<ChildBudget, OrchestratorError> {
    validate_child_budget_limit(limit)?;
    if inherited_deadline <= admitted_at {
        return Err(OrchestratorError::InvalidChildBudget);
    }
    let deadline = admitted_at
        .checked_add_signed(child_duration(limit)?)
        .ok_or(OrchestratorError::InvalidChildBudget)?
        .min(inherited_deadline);
    Ok(ChildBudget {
        deadline,
        maximum_model_tokens: limit.maximum_model_tokens,
        maximum_capability_calls: limit.maximum_capability_calls,
        maximum_artifact_bytes: limit.maximum_artifact_bytes,
        maximum_descendant_runs: limit.maximum_descendant_runs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(milliseconds: u64) -> ChildBudgetLimit {
        ChildBudgetLimit {
            maximum_duration_milliseconds: milliseconds,
            maximum_model_tokens: 1_000,
            maximum_capability_calls: 10,
            maximum_artifact_bytes: 1_048_576,
            maximum_descendant_runs: 8,
        }
    }

    #[test]
    fn admission_uses_full_duration_and_never_extends_parent_deadline() {
        let now = DateTime::from_timestamp(1_700_000_000, 123_456_000).unwrap();
        let parent = now + Duration::seconds(30);
        assert_eq!(
            derive_child_budget(&limit(100), now, parent)
                .unwrap()
                .deadline,
            now + Duration::milliseconds(100)
        );
        assert_eq!(
            derive_child_budget(&limit(1), now, parent)
                .unwrap()
                .deadline,
            now + Duration::milliseconds(1)
        );
        let capped = now + Duration::milliseconds(37);
        assert_eq!(
            derive_child_budget(&limit(100), now, capped)
                .unwrap()
                .deadline,
            capped
        );
        for expired in [now, now - Duration::microseconds(1)] {
            assert_eq!(
                derive_child_budget(&limit(100), now, expired),
                Err(OrchestratorError::InvalidChildBudget)
            );
        }
    }

    #[test]
    fn admission_rejects_zero_and_overflow_without_panicking() {
        let now = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let parent = now + Duration::seconds(30);
        for milliseconds in [0, u64::MAX] {
            assert_eq!(
                derive_child_budget(&limit(milliseconds), now, parent),
                Err(OrchestratorError::InvalidChildBudget)
            );
        }
        assert_eq!(
            derive_child_budget(
                &limit(100),
                DateTime::<Utc>::MAX_UTC - Duration::milliseconds(1),
                DateTime::<Utc>::MAX_UTC,
            ),
            Err(OrchestratorError::InvalidChildBudget)
        );
    }
}
