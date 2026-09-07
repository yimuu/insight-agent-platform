//! Exact references for a committed external result awaiting ordinary controller settlement.
//! This is continuation evidence, not permission to invoke the external owner again.
use crate::{ExactRunValueRef, OrchestratorError};
use insight_platform_contracts::{ResourceId, ResourceKind};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalLeafCompletion {
    pub source_orchestration_job_id: ResourceId,
    pub owner: ExternalLeafCompletionOwner,
    pub output: ExactRunValueRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalLeafCompletionOwner {
    Child {
        child_run_id: ResourceId,
        child_link_id: ResourceId,
    },
    Context {
        context_query_id: ResourceId,
        context_job_id: ResourceId,
    },
    Model {
        model_turn_id: ResourceId,
        model_job_id: ResourceId,
    },
    Capability {
        invocation_id: ResourceId,
        invocation_job_id: ResourceId,
    },
}

impl ExternalLeafCompletion {
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        let valid_owner = match &self.owner {
            ExternalLeafCompletionOwner::Child {
                child_run_id,
                child_link_id,
            } => {
                child_run_id.kind() == ResourceKind::Run
                    && child_link_id.kind() == ResourceKind::ChildRunLink
            }
            ExternalLeafCompletionOwner::Context {
                context_query_id,
                context_job_id,
            } => {
                context_query_id.kind() == ResourceKind::ContextQuery
                    && context_job_id.kind() == ResourceKind::Job
            }
            ExternalLeafCompletionOwner::Model {
                model_turn_id,
                model_job_id,
            } => {
                model_turn_id.kind() == ResourceKind::ModelTurn
                    && model_job_id.kind() == ResourceKind::Job
            }
            ExternalLeafCompletionOwner::Capability {
                invocation_id,
                invocation_job_id,
            } => {
                invocation_id.kind() == ResourceKind::CapabilityInvocation
                    && invocation_job_id.kind() == ResourceKind::Job
            }
        };
        if !valid_owner
            || self.source_orchestration_job_id.kind() != ResourceKind::Job
            || self.output.value_id.kind() != ResourceKind::RunValue
        {
            return Err(OrchestratorError::InvalidRunAdmission);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OrchestrationJobPayload;
    use insight_platform_contracts::{SchedulingLane, TypedPayload};

    fn payload() -> OrchestrationJobPayload {
        OrchestrationJobPayload {
            bindings_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            node_execution_id: "nod_0198f1c3-9a00-7c3e-b1f3-773c28366f17".parse().unwrap(),
            root_scope_id: "scp_0198f1c3-9a00-7c3e-b1f3-773c28366f17".parse().unwrap(),
            retry_backoff_milliseconds: 1,
            wake_contract: None,
            convergence_failure: None,
            model_tool_continuation: None,
            external_leaf_completion: Some(ExternalLeafCompletion {
                source_orchestration_job_id: "job_0198f1c3-9a00-7c3e-b1f3-773c28366f17"
                    .parse()
                    .unwrap(),
                owner: ExternalLeafCompletionOwner::Child {
                    child_run_id: "run_0198f1c3-9a00-7c3e-b1f3-773c28366f17".parse().unwrap(),
                    child_link_id: "crun_0198f1c3-9a00-7c3e-b1f3-773c28366f17".parse().unwrap(),
                },
                output: ExactRunValueRef {
                    value_id: "val_0198f1c3-9a00-7c3e-b1f3-773c28366f17".parse().unwrap(),
                    schema_digest: format!("sha256:{}", "b".repeat(64)).parse().unwrap(),
                    content_digest: format!("sha256:{}", "c".repeat(64)).parse().unwrap(),
                },
            }),
        }
    }

    #[test]
    fn current_codec_requires_exact_version_fields_and_digest() {
        let payload = payload();
        let encoded = payload.to_payload().unwrap();
        assert_eq!(encoded.schema_version, 2);
        assert_eq!(
            OrchestrationJobPayload::from_payload(&encoded).unwrap(),
            payload
        );
        assert!(OrchestrationJobPayload::from_payload(
            &TypedPayload::with_limit(1, &payload, 262_144).unwrap()
        )
        .is_err());
        let mut missing = encoded.value.clone();
        missing
            .as_object_mut()
            .unwrap()
            .remove("external_leaf_completion");
        let missing = TypedPayload::from_versioned(2, &missing, 262_144).unwrap();
        assert!(OrchestrationJobPayload::from_payload(&missing).is_err());
        let mut corrupted = encoded;
        corrupted.digest = format!("sha256:{}", "d".repeat(64));
        assert!(OrchestrationJobPayload::from_payload(&corrupted).is_err());
    }

    #[test]
    fn only_committed_outcomes_use_the_control_lane_and_owners_are_nominal() {
        let mut payload = payload();
        assert_eq!(payload.scheduling_lane(), SchedulingLane::RestrictedControl);
        payload
            .external_leaf_completion
            .as_mut()
            .unwrap()
            .source_orchestration_job_id = payload.node_execution_id.clone();
        assert!(payload.to_payload().is_err());
        payload.external_leaf_completion = None;
        assert_eq!(payload.scheduling_lane(), SchedulingLane::Business);
        assert!(payload.to_payload().is_ok());
    }
}
