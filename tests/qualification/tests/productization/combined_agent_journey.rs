//! One public authoring/Run journey across the actual domain workers.
use super::*;

fn read_fixture(fixture: &Path, name: &str) -> Value {
    serde_json::from_slice(
        &fs::read(fixture.join(name)).expect("published fixture manifest exists"),
    )
    .expect("manifest is JSON")
}

fn apply(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    plan: &Value,
    manifest: &mut Value,
) -> Value {
    compile_agent_apply_fixture(insight, project, fixture, plan, manifest);
    let name = manifest["create"]["document"]["spec"]["authoring_name"]
        .as_str()
        .unwrap();
    let path = write_canonical(fixture, &format!("{name}.apply.json"), manifest);
    run_json(
        insight,
        &[
            "apply",
            "--file",
            path.to_str().unwrap(),
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    )
}

fn read_public(client: &Client, base: &str, token: &str, path: &str) -> Value {
    let response = client
        .get(format!("{base}/v1/{path}"))
        .bearer_auth(token)
        .header("accept", "application/json")
        .send()
        .expect("public journey read completes");
    assert_eq!(response.status(), StatusCode::OK, "public read {path}");
    response.json().expect("public read is JSON")
}

pub(super) fn run(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    model_manifest: &Value,
    replacement: &mut RestartedOrchestrationWorker,
) -> Value {
    let context_manifest = read_fixture(fixture, "cited-context-agent.apply.json");
    let capability_manifest = read_fixture(fixture, "capability-agent.apply.json");
    let context_spec = &context_manifest["create"]["document"]["spec"];
    let model_spec = &model_manifest["create"]["document"]["spec"];
    let context_bindings = &context_manifest["deployment"]["closure"]["bindings"];
    let model_bindings = &model_manifest["deployment"]["closure"]["bindings"];
    let capability_bindings = &capability_manifest["deployment"]["closure"]["bindings"];
    let input_schema = context_spec["input_schema"].clone();
    let observation_schema = context_spec["output_schema"].clone();
    let result_schema = model_spec["output_schema"].clone();
    let approval_schema = serde_json::to_value(
        insight_platform_contracts::ClosedJsonSchema::build(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object",
            "properties": {"approved": {"type": "boolean", "const": true}},
            "required": ["approved"], "additionalProperties": false,
        }))
        .unwrap(),
    )
    .unwrap();
    let input_digest = input_schema["canonical_digest"].as_str().unwrap();
    let result_digest = result_schema["canonical_digest"].as_str().unwrap();
    let observation_digest = observation_schema["canonical_digest"].as_str().unwrap();
    let approval_digest = approval_schema["canonical_digest"].as_str().unwrap();
    let port = |node: &str, name: &str, digest: &str| json!({"source":"node_output", "producer_node_id":node,"port_id":name,"schema_digest":digest});
    let schema_documents = fixture_schema_documents(&[
        &input_schema,
        &observation_schema,
        &result_schema,
        &approval_schema,
    ]);

    let mut child = model_manifest.clone();
    child["create"]["display_name"] = json!("Combined journey typed result child");
    child["create"]["document"]["spec"]["authoring_name"] = json!("combined-typed-result-child");
    child["create"]["document"]["spec"]["input_schema"] = result_schema.clone();
    child["deployment"]["closure"]["bindings"]["slots"] = json!([]);
    let child_plan = json!({
        "plan_version":6,"schema_documents":schema_documents,"interface_contract_digest":CONTRACT_DIGEST,"entry_node_id":"start","dependency_slots":{},
        "nodes":{"start":{"kind":"start","next":"finish"},"finish":{"kind":"return","value":{"source":"run_input","schema_digest":result_digest}}}
    });
    let child_report = apply(insight, project, fixture, &child_plan, &mut child);
    let child_bindings = &child["deployment"]["closure"]["bindings"];
    let child_closure = json!({
        "interface":exact_version(published_version(&child_report,"agent_interface_revision")),
        "plan":exact_version(published_version(&child_report,"agent_plan_revision")),
        "entry_node_id":child_bindings["entry_node_id"],"entry_node_kind":child_bindings["entry_node_kind"],
        "slots":child_bindings["slots"],"policies":child_bindings["policies"],"execution_profile":child_bindings["execution_profile"],
    });
    let child_exact = json!({"deployment_id":child_report["deployment_id"],"resource_kind":"agent_deployment",
        "deployment_digest":canonical_digest(&json!({"schema_version":1,"resource_kind":"agent","bindings":child_closure}))});
    let native_slot = capability_bindings["slots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|slot| slot["slot_id"] == "native")
        .unwrap()
        .clone();
    let context_slot = context_bindings["slots"][0].clone();
    let model_slot = model_bindings["slots"][0].clone();
    let child_requirement = canonical_digest(
        &json!({"input_schema":result_digest,"output_schema":result_digest,"kind":"child_agent"}),
    );
    let child_slot = json!({"slot_id":"child_worker","requirement_digest":child_requirement,
        "target":{"kind":"child_agent","candidates":[child_exact],"selection_policy":model_slot["target"]["selection_policy"]}});
    let plan = json!({
        "plan_version":6,"schema_documents":schema_documents,"interface_contract_digest":CONTRACT_DIGEST,"entry_node_id":"start",
        "dependency_slots":{
            "catalog":{"kind":"context","requirement_digest":context_slot["requirement_digest"]},
            "primary_model":{"kind":"model","requirement_digest":model_slot["requirement_digest"]},
            "native":{"kind":"capability","requirement_digest":native_slot["requirement_digest"]},
            "child_worker":{"kind":"child_agent","requirement_digest":child_requirement}},
        "nodes":{
            "start":{"kind":"start","next":"retrieve"},
            "retrieve":{"kind":"context_query","context_slot_id":"catalog","request":{"source":"run_input","schema_digest":input_digest},
                "result":port("retrieve","items",observation_digest),"maximum_items":1,"resume":"model"},
            "model":{"kind":"model_loop","model_slot_id":"primary_model","skill_slot_ids":[],"capability_slot_ids":["native"],
                "input":port("retrieve","items",observation_digest),"model_route":null,"output":port("model","response",result_digest),
                "maximum_rounds":2,"maximum_capability_calls":1,"maximum_parallel_calls_per_round":1,"token_budget":4096,"resume":"approve"},
            "approve":{"kind":"human_task","definition":{"kind":"interaction","interaction_kind":"form",
                "eligibility_rule":insight_platform_contracts::TaskEligibilityRule::AnyAuthorized,
                "eligible_principal_rule_digest":insight_platform_contracts::TaskEligibilityRule::AnyAuthorized.canonical_digest().unwrap(),
                "safe_prompt_key":"combined_journey_approval"},
                "response":port("approve","approval",approval_digest),"timeout_milliseconds":240000,"resume":"child"},
            "child":{"kind":"child_agent_call","child_agent_slot_id":"child_worker","input":port("model","response",result_digest),
                "candidate_route":null,"output":port("child","result",result_digest),
                "budget":{"maximum_duration_milliseconds":60000,"maximum_model_tokens":1000,"maximum_capability_calls":1,"maximum_artifact_bytes":1048576,"maximum_descendant_runs":1},
                "cancellation_policy":"cascade_and_wait","attempt_limit":1,"retry_backoff_milliseconds":100,"resume":"finish"},
            "finish":{"kind":"return","value":port("child","result",result_digest)}
        }
    });
    let mut parent = model_manifest.clone();
    parent["create"]["display_name"] = json!("Retrieval tool approval child journey");
    parent["create"]["document"]["spec"]["authoring_name"] = json!("combined-agent-journey");
    parent["create"]["document"]["spec"]["input_schema"] = input_schema.clone();
    parent["deployment"]["closure"]["bindings"]["slots"] =
        json!([context_slot, model_slot, native_slot, child_slot]);
    // Refer to the actual published policies once; the compiler remains the definition authority.
    let mut revisions = std::collections::BTreeMap::new();
    let mut policies = std::collections::BTreeMap::new();
    for manifest in [&context_manifest, model_manifest, &capability_manifest] {
        for revision in manifest["create"]["document"]["spec"]["policy_versions"]
            .as_array()
            .unwrap()
        {
            revisions.insert(
                revision["revision_id"].as_str().unwrap().to_owned(),
                revision.clone(),
            );
        }
        for policy in manifest["deployment"]["closure"]["bindings"]["policies"]
            .as_array()
            .unwrap()
        {
            policies.insert(
                policy["revision"]["revision_id"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
                policy.clone(),
            );
        }
    }
    parent["create"]["document"]["spec"]["policy_versions"] =
        json!(revisions.into_values().collect::<Vec<_>>());
    parent["deployment"]["closure"]["bindings"]["policies"] =
        json!(policies.into_values().collect::<Vec<_>>());
    let report = apply(insight, project, fixture, &plan, &mut parent);
    let request = json!({"agent_id":report["resource_id"],
        "input":{"classification":"internal","schema_digest":input_digest,"value":{"kind":"inline","value":{"message":"retrieve and approve the cited tool result"}}},
        "deadline":(Utc::now()+Duration::minutes(5)).to_rfc3339_opts(SecondsFormat::Micros,true)});
    let (client, base, token) = raw_runtime_client(project);
    let created = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth(&token)
        .header("idempotency-key", "combined-agent-journey")
        .json(&request)
        .send()
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let created: Value = created.json().unwrap();
    let run_id = created["run_id"].as_str().unwrap();
    let deadline = Instant::now() + StdDuration::from_secs(120);
    let task = loop {
        let tasks = read_public(
            &client,
            &base,
            &token,
            &format!("tasks?run_id={run_id}&state=pending&page_size=10"),
        );
        if let Some(task) = tasks["items"].as_array().unwrap().first() {
            break task.clone();
        }
        let run = read_public(&client, &base, &token, &format!("runs/{run_id}"));
        assert!(
            !matches!(
                run["state"].as_str(),
                Some("failed" | "cancelled" | "timed_out")
            ),
            "combined Run terminated before approval: {run}"
        );
        assert!(
            Instant::now() < deadline,
            "combined Run did not reach approval: {run}"
        );
        thread::sleep(StdDuration::from_millis(100));
    };
    let task_id = task["task_id"].as_str().unwrap();
    let pending = run_json(
        insight,
        &["task", "get", task_id, "--path", project.to_str().unwrap()],
    );
    assert_eq!(pending["response_schema_digest"], approval_digest);
    assert_eq!(
        read_public(
            &client,
            &base,
            &token,
            &format!("runs/{run_id}/children?page_size=10")
        )["items"],
        json!([])
    );
    let before =
        fs::read_to_string(project.join(".insight/runtime/logs/remote-fixture-model.jsonl"))
            .unwrap();
    replacement
        .terminate()
        .expect("actual orchestration worker stops during durable approval wait");
    *replacement = restart_orchestration_worker(insight, project);
    let recovered = run_json(
        insight,
        &["task", "get", task_id, "--path", project.to_str().unwrap()],
    );
    assert_eq!(
        recovered, pending,
        "restart preserves the exact Task and ETag"
    );
    let approval_input = json!({"classification":"internal","schema_digest":approval_digest,"value":{"kind":"inline","value":{"approved":true}}});
    let submit = || {
        let response = client
            .post(format!("{base}/v1/tasks/{task_id}:submit-input"))
            .bearer_auth(&token)
            .header("accept", "application/json")
            .header("idempotency-key", "combined-durable-approval")
            .header("if-match", pending["etag"].as_str().unwrap())
            .json(&approval_input)
            .send()
            .expect("public Task submission completes");
        assert_eq!(response.status(), StatusCode::OK);
        response.json::<Value>().expect("Task response is JSON")
    };
    let responded = submit();
    assert_eq!(responded["state"], "responded");
    assert_eq!(
        submit(),
        responded,
        "second HTTP POST replays the same Receipt despite its old ETag"
    );
    let records = run_json_lines(
        insight,
        &[
            "run",
            "watch",
            run_id,
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(records.last().unwrap()["run"]["state"], "succeeded");
    let result = run_json(
        insight,
        &["run", "result", run_id, "--path", project.to_str().unwrap()],
    );
    assert_eq!(result["schema_digest"], result_digest);
    assert_eq!(
        result["value"],
        json!({"kind":"inline","value":{"answer":"cited tool result approved"}})
    );
    let children = read_public(
        &client,
        &base,
        &token,
        &format!("runs/{run_id}/children?page_size=10"),
    );
    assert_eq!(children["items"].as_array().unwrap().len(), 1);
    let child = &children["items"][0];
    assert_eq!(child["child_state"], "succeeded");
    assert_eq!(child["parent_plan_node_key"], "child");
    let values = read_public(
        &client,
        &base,
        &token,
        &format!("runs/{run_id}/values?page_size=50"),
    );
    assert!(
        values["next_cursor"].is_null(),
        "fixture values fit one bounded page"
    );
    let observation = values["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["schema_digest"] == observation_digest)
        .expect("retrieval persisted its typed output");
    let observed = read_public(
        &client,
        &base,
        &token,
        &format!(
            "runs/{run_id}/values/{}/content",
            observation["value_id"].as_str().unwrap()
        ),
    );
    assert_eq!(
        observed["value"]["value"]["items"][0]["citation"]["strength"],
        "observation_only"
    );
    let after =
        fs::read_to_string(project.join(".insight/runtime/logs/remote-fixture-model.jsonl"))
            .unwrap();
    assert_eq!(
        before, after,
        "approval recovery cannot redispatch the completed model/tool effect"
    );
    let traces: Vec<Value> = after
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(traces.iter().filter(|v|v["kind"]=="combined_tool_intent"&&v["context_citation_present"]==true).count(),1);
    assert_eq!(
        traces
            .iter()
            .filter(|v| v["kind"] == "combined_tool_result" && v["tool_result_matches"] == true)
            .count(),
        1
    );
    let proof = json!({"schema_version":1,"kind":"insight.public-combined-agent-journey/v1","qualification_run_id":qualification_run_id(),
        "run_id":run_id,"task_id":task_id,"child_run_id":child["child_run_id"],"result":result,
        "retrieval_value_id":observation["value_id"],"actual_orchestration_restart":true,"model_requests_before_restart":2,"model_requests_after_restart":0,
        "public_authoring":true,"public_task_receipt_replay":true,"database_business_state_mutation":false});
    fs::write(
        project.join(".insight/runtime/logs/combined-agent-journey.json"),
        canonical_bytes(&proof),
    )
    .unwrap();
    proof
}
