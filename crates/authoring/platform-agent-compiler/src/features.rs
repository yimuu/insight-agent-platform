//! Pure feature derivation. Exact deployment projections are rechecked by Registry storage.
use crate::{AgentCompilerError, RequiredAgentFeature, ResolvedAgentBindings};
use insight_platform_contracts::{
    AgentSlotTargetInputV1, ExactDeploymentRef, ResourceId, MAX_AGENT_DEPLOYMENT_FEATURE_EVIDENCE,
};
use insight_platform_plan::{RuntimeNode, RuntimePlan};
use std::collections::{BTreeMap, BTreeSet};

pub fn feature_deployments(
    bindings: &ResolvedAgentBindings,
) -> Result<Vec<ExactDeploymentRef>, AgentCompilerError> {
    let mut exact: BTreeMap<ResourceId, ExactDeploymentRef> = BTreeMap::new();
    for slot in &bindings.slots {
        let targets = match &slot.target {
            AgentSlotTargetInputV1::Capability { candidates, .. }
            | AgentSlotTargetInputV1::ChildAgent { candidates, .. } => candidates.as_slice(),
            AgentSlotTargetInputV1::Context { binding } => {
                std::slice::from_ref(&binding.context_deployment)
            }
            AgentSlotTargetInputV1::Model { .. } | AgentSlotTargetInputV1::Skill { .. } => &[],
        };
        for target in targets {
            if exact
                .insert(target.deployment_id.clone(), target.clone())
                .is_some_and(|previous| previous != *target)
            {
                return Err(AgentCompilerError::binding(
                    "conflicting exact feature deployment",
                ));
            }
        }
    }
    if exact.len() > MAX_AGENT_DEPLOYMENT_FEATURE_EVIDENCE {
        return Err(AgentCompilerError::binding(
            "deployment feature evidence exceeds its bound",
        ));
    }
    Ok(exact.into_values().collect())
}

pub(crate) fn required_features(
    plan: &RuntimePlan,
    bindings: &ResolvedAgentBindings,
) -> Result<Vec<RequiredAgentFeature>, AgentCompilerError> {
    let expected = feature_deployments(bindings)?;
    if bindings.deployment_features.len() != expected.len()
        || bindings
            .deployment_features
            .iter()
            .zip(&expected)
            .any(|(actual, target)| actual.validate().is_err() || actual.deployment != *target)
    {
        return Err(AgentCompilerError::binding(
            "exact deployment feature evidence is missing, duplicated, unordered or unrelated",
        ));
    }
    let mut required = BTreeSet::new();
    for node in plan.nodes.values() {
        match node {
            RuntimeNode::ModelLoop { .. } => {
                required.insert(RequiredAgentFeature::Model);
            }
            RuntimeNode::ContextQuery { .. } => {
                required.insert(RequiredAgentFeature::Context);
            }
            _ => {}
        }
    }
    for evidence in &bindings.deployment_features {
        required.extend(evidence.required_features.iter().copied());
    }
    Ok(required.into_iter().collect())
}
