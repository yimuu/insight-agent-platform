use insight_platform_api::{
    product::{
        AgentAuthoringModelBindingV1, AgentAuthoringProfileV1, DEFAULT_AGENT_MODEL_REFERENCE,
    },
    resource::ResourceApplicationError,
};
use insight_platform_contracts::{ExactDeploymentRef, ExactPolicyBinding};

pub(super) fn build(
    environment: String,
    execution_profile: ExactPolicyBinding,
    selection_policy: ExactPolicyBinding,
    model: Option<ExactDeploymentRef>,
) -> Result<AgentAuthoringProfileV1, ResourceApplicationError> {
    let models = model
        .into_iter()
        .map(|deployment| AgentAuthoringModelBindingV1 {
            alias: DEFAULT_AGENT_MODEL_REFERENCE.to_owned(),
            deployment,
            selection_policy: selection_policy.clone(),
        })
        .collect();
    AgentAuthoringProfileV1::build_for_installation(environment, execution_profile, models)
        .map_err(|_| ResourceApplicationError::Internal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_agent_compiler::{compile_agent, AgentCompilerInput};
    use insight_platform_contracts::{ExactVersionRef, ResourceKind};
    use serde_json::json;

    #[test]
    fn installed_default_reference_reaches_the_shared_compiler() {
        let digest = || format!("sha256:{}", "a".repeat(64)).parse().unwrap();
        let policy = || ExactPolicyBinding {
            revision: ExactVersionRef::new(
                crate::new_id(ResourceKind::PolicyRevision).unwrap(),
                digest(),
            )
            .unwrap(),
            deployment: ExactDeploymentRef::new(
                crate::new_id(ResourceKind::PolicyDeployment).unwrap(),
                digest(),
            )
            .unwrap(),
        };
        let execution = policy();
        let selection = policy();
        let model = ExactDeploymentRef::new(
            crate::new_id(ResourceKind::ModelDeployment).unwrap(),
            digest(),
        )
        .unwrap();
        let profile = build(
            "development".into(),
            execution.clone(),
            selection.clone(),
            Some(model.clone()),
        )
        .unwrap();
        profile.validate().unwrap();
        let selected = &profile.models[0];
        assert_eq!(selected.alias, "project/default");
        assert_eq!(selected.deployment, model);
        let schema = |property: &str| json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{property:{"type":"string","minLength":1,"maxLength":256,"x-platform-max-bytes":1024}},"required":[property],"additionalProperties":false});
        let manifest = json!({"apiVersion":"insight.platform/v1","kind":"Agent","metadata":{"name":"installed-model-chat"},"spec":{"execution":{"kind":"model_chat"},"instructions":"Return one JSON object with answer set to the input message.","model":{"ref":selected.alias},"input":{"schema":"input.json","classification":"internal"},"output":{"schema":"output.json"}}});
        let input: AgentCompilerInput = serde_json::from_value(json!({
            "plan_bytes": null,
            "manifest_bytes":serde_json::to_vec(&manifest).unwrap(),
            "input_schema_bytes":serde_json::to_vec(&schema("message")).unwrap(),
            "output_schema_bytes":serde_json::to_vec(&schema("answer")).unwrap(),
            "profile":{
                "default_deadline_seconds":profile.default_deadline_seconds,
                "default_environment":profile.default_environment,
                "policy_versions":profile.policy_versions,
                "deployment_policies":profile.deployment_policies,
                "execution_profile":profile.execution_profile,
                "model_loop":profile.model_loop
            },
            "bindings":{"model":{"manifest_ref":selected.alias,"deployment":selected.deployment,"selection_policy":selected.selection_policy},"slots":[]}
        })).unwrap();
        compile_agent(input).expect("the Gateway's emitted reference must compile unchanged");
        assert!(build("development".into(), execution, selection, None)
            .unwrap()
            .models
            .is_empty());
    }
}
