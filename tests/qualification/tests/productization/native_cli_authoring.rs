//! Actual user-facing authoring, publication, update and execution through the public Gateway.
use super::*;
use insight_platform_api::resource::{DeploymentViewV1, ResourceVersionViewV1, ResourceViewV1};

fn get<T: serde::de::DeserializeOwned>(client: &Client, base: &str, token: &str, path: &str) -> T {
    let response = client
        .get(format!("{base}{path}"))
        .bearer_auth(token)
        .send()
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "public authoring read {path}"
    );
    response.json().expect("owning public authoring DTO")
}

fn exact_publication(project: &Path, agent_id: &str) -> (ResourceViewV1, ResourceVersionViewV1) {
    let (client, _, token) = raw_runtime_client(project);
    let profile: Value =
        serde_json::from_slice(&fs::read(project.join(".insight/runtime/profile.json")).unwrap())
            .unwrap();
    let base = format!(
        "http://127.0.0.1:{}",
        profile["ports"]["gateway_management"].as_u64().unwrap()
    );
    let resource: ResourceViewV1 = get(&client, &base, &token, &format!("/v1/agents/{agent_id}"));
    resource.validate().unwrap();
    let deployment_id = resource
        .active_deployment_id
        .as_ref()
        .expect("published active deployment");
    let deployment: DeploymentViewV1 = get(
        &client,
        &base,
        &token,
        &format!("/v1/agents/{agent_id}/deployments/{deployment_id}"),
    );
    deployment.validate().unwrap();
    let version: ResourceVersionViewV1 = get(
        &client,
        &base,
        &token,
        &format!(
            "/v1/agents/{agent_id}/versions/{}",
            deployment.resource_version_id
        ),
    );
    version.validate().unwrap();
    let ResourceDocument::Agent(spec) = &version.payload.document else {
        panic!("Agent Plan revision document");
    };
    assert_ne!(
        spec.authoring_package.artifact.artifact_id(),
        &spec.typed_plan_artifact_id
    );
    assert_eq!(
        version.artifact_id.as_ref(),
        Some(&spec.typed_plan_artifact_id),
        "public publication binds Plan bytes; source stays in the authoring package"
    );
    assert_eq!(version.content_digest, spec.typed_plan_digest);
    assert_eq!(resource.resource_id, version.resource_id);
    (resource, version)
}

pub(super) fn verify(insight: &Path, project: &Path) {
    let directory = project.join("native-cli-authoring");
    fs::create_dir(&directory).expect("new dedicated CLI authoring directory");
    let corpus = workspace_root().join("contracts/product-experience/agent-compiler/v2");
    fs::copy(
        corpus.join("schema-message.json"),
        directory.join("schema-message.json"),
    )
    .unwrap();
    let mut source: Value =
        serde_json::from_slice(&fs::read(corpus.join("deterministic.json")).unwrap()).unwrap();
    source["metadata"]["name"] = json!("native-cli-authoring");
    source["metadata"]["displayName"] = json!("Native CLI publication");
    source["spec"]["input"]["schema"] = json!("native-cli-authoring/schema-message.json");
    source["spec"]["output"]["schema"] = json!("native-cli-authoring/schema-message.json");
    let file = directory.join("agent.json");
    fs::write(&file, serde_json::to_vec_pretty(&source).unwrap()).unwrap();
    let path = project.to_str().unwrap();
    let input = file.to_str().unwrap();
    let validated = run_json(
        insight,
        &[
            "agent", "validate", "--file", input, "--output", "json", "--path", path,
        ],
    );
    assert_eq!(validated["agent_name"], "native-cli-authoring");
    let created = run_json(
        insight,
        &[
            "agent", "publish", "--file", input, "--output", "json", "--path", path,
        ],
    );
    assert_eq!(created["state"], "ready");
    let agent_id = created["agent_id"].as_str().unwrap();
    let (_, first) = exact_publication(project, agent_id);
    source["metadata"]["displayName"] = json!("Native CLI publication updated");
    fs::write(&file, serde_json::to_vec_pretty(&source).unwrap()).unwrap();
    let updated = run_json(
        insight,
        &[
            "agent", "publish", "--file", input, "--output", "json", "--path", path,
        ],
    );
    assert_eq!(updated["agent_id"], agent_id);
    assert_eq!(updated["state"], "ready");
    assert_eq!(updated["unchanged"], false);
    let (resource, second) = exact_publication(project, agent_id);
    assert_ne!(first.resource_version_id, second.resource_version_id);
    assert!(second.revision_no > first.revision_no);
    assert_eq!(
        resource.draft.display_name,
        "Native CLI publication updated"
    );
    let ResourceDocument::Agent(first_spec) = &first.payload.document else {
        unreachable!()
    };
    let ResourceDocument::Agent(second_spec) = &second.payload.document else {
        unreachable!()
    };
    assert_eq!(first_spec.contract_digest, second_spec.contract_digest);
    assert_eq!(first_spec.typed_plan_digest, second_spec.typed_plan_digest);
    assert_ne!(
        first_spec.authoring_package.artifact.content_digest(),
        second_spec.authoring_package.artifact.content_digest()
    );
    source["metadata"]["displayName"] = json!("Native CLI publication C");
    fs::write(&file, serde_json::to_vec_pretty(&source).unwrap()).unwrap();
    let third = run_json(
        insight,
        &[
            "agent", "publish", "--file", input, "--output", "json", "--path", path,
        ],
    );
    assert_eq!(third["agent_id"], agent_id);
    let (_, third_version) = exact_publication(project, agent_id);
    source["metadata"]["displayName"] = json!("Native CLI publication updated");
    fs::write(&file, serde_json::to_vec_pretty(&source).unwrap()).unwrap();
    let repeated = run_json(
        insight,
        &[
            "agent", "publish", "--file", input, "--output", "json", "--path", path,
        ],
    );
    assert_eq!(repeated["agent_id"], agent_id);
    assert_eq!(repeated["unchanged"], false);
    let (_, repeated_version) = exact_publication(project, agent_id);
    assert!(repeated_version.revision_no > third_version.revision_no);
    let ResourceDocument::Agent(repeated_spec) = &repeated_version.payload.document else {
        unreachable!()
    };
    assert_eq!(
        repeated_spec.authoring_package.artifact.content_digest(),
        second_spec.authoring_package.artifact.content_digest()
    );
    let unchanged = run_json(
        insight,
        &[
            "agent", "publish", "--file", input, "--output", "json", "--path", path,
        ],
    );
    assert_eq!(unchanged["unchanged"], true);
    assert_eq!(unchanged["agent_id"], agent_id);
    let run = run_json(
        insight,
        &[
            "agent",
            "run",
            "native-cli-authoring",
            "--input",
            "{\"message\":\"actual CLI echo\"}",
            "--output",
            "json",
            "--path",
            path,
        ],
    );
    assert_eq!(run["run_state"], "succeeded");
    assert_eq!(
        run["result"],
        json!({"kind":"inline", "value":{"message":"actual CLI echo"}})
    );
    let result = run_json(
        insight,
        &[
            "agent",
            "result",
            run["run_id"].as_str().unwrap(),
            "--output",
            "json",
            "--path",
            path,
        ],
    );
    assert_eq!(result["result"], run["result"]);
    fs::write(project.join(".insight/runtime/logs/native-cli-authoring.json"), serde_json::to_vec_pretty(&json!({"kind":"insight.native-cli-authoring/v1", "new_publication":true,"updated_same_agent":true,"repeated_a_b_c_b":true,"unchanged_replay":true,"plan_artifact_bound":true,"source_artifact_separate":true,"run_succeeded":true,"agent_id":agent_id,"first_plan_revision_id":first.resource_version_id,"updated_plan_revision_id":repeated_version.resource_version_id,"run_id":run["run_id"]})).unwrap()).unwrap();
}

#[test]
#[ignore = "targeted actual CLI publication; also invoked by the full native journey"]
fn actual_cli_publication_and_update() {
    let project = env::var(PROJECT_ENV).expect("explicit task-owned native project");
    let insight = env::var(INSIGHT_BIN_ENV).expect("actual built CLI");
    verify(Path::new(&insight), Path::new(&project));
}
