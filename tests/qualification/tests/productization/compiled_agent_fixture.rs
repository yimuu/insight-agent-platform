//! Test authoring drafts use the actual compiler and public Artifact lifecycle.
use super::*;
use insight_platform_agent_compiler::{
    agent_interface_contract_digest, compile_source_bundle, AgentCompileResponseV1,
    AgentCompilerProfile, AgentSourceBundleV1, AgentSourceFilesV1, ArtifactAuthority,
    ModelLoopCompilerLimits, ResolvedAgentBindings,
};
use insight_platform_contracts::{ArtifactPurpose, ArtifactState, ClosedJsonSchema};
use std::collections::BTreeMap;

pub(super) fn fixture_schema_documents(schemas: &[&Value]) -> Value {
    let mut documents = BTreeMap::new();
    for value in schemas {
        let schema: ClosedJsonSchema = serde_json::from_value((*value).clone()).unwrap();
        let schema = insight_platform_contracts::ClosedValueSchema::try_from(schema).unwrap();
        documents.insert(schema.canonical_digest.clone(), schema);
    }
    serde_json::to_value(documents).unwrap()
}

pub(super) fn compile_agent_apply_fixture(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    plan: &Value,
    manifest: &mut Value,
) {
    assert_eq!(
        plan["plan_version"], 6,
        "fixture authors must supply the current IR; no old reader"
    );
    assert!(
        plan["schema_documents"].is_object(),
        "fixture authors explicitly supply frozen internal schemas"
    );
    let spec = &manifest["create"]["document"]["spec"];
    let input: ClosedJsonSchema = serde_json::from_value(spec["input_schema"].clone()).unwrap();
    let output: ClosedJsonSchema = serde_json::from_value(spec["output_schema"].clone()).unwrap();
    let name = spec["authoring_name"].as_str().unwrap().to_owned();
    let binding = &manifest["deployment"]["closure"]["bindings"];
    let profile = AgentCompilerProfile {
        default_deadline_seconds: spec["default_deadline_seconds"]
            .as_u64()
            .unwrap()
            .try_into()
            .unwrap(),
        default_environment: manifest["deployment"]["environment"]
            .as_str()
            .unwrap()
            .into(),
        policy_versions: serde_json::from_value(spec["policy_versions"].clone()).unwrap(),
        deployment_policies: serde_json::from_value(binding["policies"].clone()).unwrap(),
        execution_profile: serde_json::from_value(binding["execution_profile"].clone()).unwrap(),
        model_loop: ModelLoopCompilerLimits {
            maximum_rounds: 16,
            maximum_capability_calls: 16,
            maximum_parallel_calls_per_round: 4,
            token_budget: 4096,
        },
    };
    let mut bindings = ResolvedAgentBindings {
        model: None,
        deployment_features: Vec::new(),
        slots: serde_json::from_value(binding["slots"].clone())
            .expect("fixture bindings are pure authoring inputs; Context has no snapshot identity"),
    };
    if let Some(request) =
        insight_platform_registry::authoring::exact_feature_request(&bindings.slots)
            .expect("bounded exact query")
    {
        let (client, base, token) =
            super::native_and_remote_capability::raw_management_client(project);
        let response = client
            .post(format!("{base}/v1/agent-authoring-bindings:resolve"))
            .bearer_auth(token)
            .json(&request)
            .send()
            .expect("actual feature resolver transport");
        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "actual exact feature resolver failed"
        );
        let resolved = response
            .json::<insight_platform_registry::authoring::ResolveAgentBindingsResponseV1>()
            .expect("owning feature response");
        bindings.deployment_features =
            insight_platform_registry::authoring::resolved_feature_evidence(&resolved, &request)
                .expect("actual exact feature evidence");
    }
    let mut authored_plan = plan.clone();
    authored_plan["interface_contract_digest"] =
        serde_json::to_value(agent_interface_contract_digest(&input, &output).unwrap()).unwrap();
    let source = json!({
        "apiVersion":"insight.platform/v1", "kind":"Agent",
        "metadata":{"name":name,"displayName":manifest["create"]["display_name"]},
        "spec":{
            "execution":{"kind":"full_plan","plan":"plan.json"},
            "input":{"schema":"input.schema.json","classification":spec["input_classification"]},
            "output":{"schema":"output.schema.json"},
            "limits":{"deadlineSeconds":profile.default_deadline_seconds},
            "publish":{"environment":profile.default_environment},
            "instructions":spec["author_instructions"],
        }
    });
    let bundle = AgentSourceBundleV1 {
        schema_version: 1,
        compiler_semantic_identity: insight_platform_agent_compiler::compiler_semantic_identity(),
        compile_policy_inputs_digest: AgentSourceBundleV1::policy_digest(&profile),
        sources: AgentSourceFilesV1 {
            manifest_path: "agent.json".into(),
            files: BTreeMap::from([
                ("agent.json".into(), serde_json::to_string(&source).unwrap()),
                (
                    "plan.json".into(),
                    serde_json::to_string(&authored_plan).unwrap(),
                ),
                (
                    "input.schema.json".into(),
                    serde_json::to_string(&input.schema).unwrap(),
                ),
                (
                    "output.schema.json".into(),
                    serde_json::to_string(&output.schema).unwrap(),
                ),
            ]),
        },
        profile,
        bindings,
    };
    let AgentCompileResponseV1::Compiled { compilation } = compile_source_bundle(bundle.clone())
    else {
        panic!(
            "current fixture compilation failed for {name}: {:?}",
            compile_source_bundle(bundle)
        );
    };
    let upload = |bytes: &[u8], purpose: ArtifactPurpose, display: &str| {
        let path = fixture.join(display);
        fs::write(&path, bytes).expect("bounded authoring fixture file");
        let report = upload_artifact(insight, project, &path, purpose.as_str(), display);
        let actual = run_json(
            insight,
            &[
                "artifact",
                "get",
                report["artifact_id"].as_str().unwrap(),
                "--path",
                project.to_str().unwrap(),
            ],
        );
        assert_eq!(actual["state"], "ready");
        assert_eq!(
            actual["content"]["content_digest"],
            report["content_digest"]
        );
        let actual_purpose: ArtifactPurpose =
            serde_json::from_value(actual["purpose"].clone()).unwrap();
        let actual_state: ArtifactState = serde_json::from_value(actual["state"].clone()).unwrap();
        let actual_content: insight_platform_contracts::ArtifactRef =
            serde_json::from_value(actual["content"].clone()).unwrap();
        assert_eq!(actual_purpose, purpose);
        assert_eq!(actual_state, ArtifactState::Ready);
        assert_eq!(
            actual_content,
            serde_json::from_value(artifact_ref(&report, display)).unwrap()
        );
        ArtifactAuthority {
            purpose: actual_purpose,
            state: actual_state,
            artifact: actual_content,
        }
    };
    let authoring = upload(
        &compilation.source_bundle_bytes,
        ArtifactPurpose::AuthoringDocument,
        compilation
            .compiled
            .resource_intent
            .authoring_artifact
            .display_name
            .as_deref()
            .unwrap(),
    );
    let typed_plan = upload(
        &compilation.compiled.typed_plan_bytes,
        ArtifactPurpose::TypedPlan,
        compilation
            .compiled
            .resource_intent
            .typed_plan_artifact
            .display_name
            .as_deref()
            .unwrap(),
    );
    let document = compilation.materialize(&authoring, &typed_plan).unwrap();
    manifest["create"]["document"] = serde_json::to_value(document).unwrap();
    manifest["publish"]["interface_content_digest"] =
        serde_json::to_value(&compilation.compiled.resource_intent.contract_digest).unwrap();
    manifest["publish"]["plan_content_digest"] =
        serde_json::to_value(&compilation.compiled.typed_plan_digest).unwrap();
    manifest["publish"]["artifact_id"] =
        serde_json::to_value(typed_plan.artifact.artifact_id()).unwrap();
    let deployment = &compilation.compiled.deployment_intent;
    manifest["deployment"]["environment"] = json!(deployment.environment);
    manifest["deployment"]["closure"]["bindings"] = json!({"entry_node_id":deployment.entry_node_id,"entry_node_kind":deployment.entry_node_kind,"slots":deployment.slots,"policies":deployment.policies,"execution_profile":deployment.execution_profile});
}
