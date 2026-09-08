//! Real Native work must continue to drain under the development polling profile.
use super::*;
use native_and_remote_capability::{
    apply_policy, provision_capability_quotas, publish_capability, value_schema,
};

pub(super) fn verify(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    authoring: &Value,
    qualification: &Value,
) {
    let selection = apply_policy(
        insight,
        project,
        fixture,
        "native-progress-selection",
        "selection",
        Some((
            "selection",
            json!({"schema_version": 1, "mode": "only_candidate", "route_schema_digest": null}),
        )),
        authoring,
        qualification,
    );
    let execution = apply_policy(
        insight,
        project,
        fixture,
        "native-progress-execution",
        "execution",
        None,
        authoring,
        qualification,
    );
    let config: Value = serde_json::from_slice(
        &fs::read(project.join(".insight/runtime/config/capability-native.json")).unwrap(),
    )
    .unwrap();
    let adapter = &config["installed_adapters"][0];
    let schema = value_schema("message");
    let schema_digest = schema["canonical_digest"].as_str().unwrap();
    let (deployment, _) = publish_capability(
        insight,
        project,
        fixture,
        "native_progress_echo",
        "pure",
        "intrinsic",
        "native",
        json!({"kind": "native", "contract": {
            "adapter_id": adapter["adapter_id"], "adapter_version": adapter["adapter_version"],
            "module_digest": adapter["module_digest"], "entrypoint_id": adapter["entrypoint_id"], "worker_protocol_version": 1
        }}),
        json!({"kind": "native", "binding": {
            "worker_manifest_digest": canonical_digest(&config["worker_manifest"]), "adapter_module_digest": adapter["module_digest"]
        }}),
        &schema,
        &schema,
        authoring,
        qualification,
        vec![execution.revision.clone()],
    );
    provision_capability_quotas(&[(&deployment, "capability_native")]);
    let contract = canonical_digest(&json!({"agent": "native-development-progress"}));
    let requirement = canonical_digest(&json!({"slot": "native-progress"}));
    let output = json!({"source": "node_output", "producer_node_id": "native", "port_id": "result", "schema_digest": schema_digest});
    let plan = json!({
        "plan_version": 6, "schema_documents": {}, "interface_contract_digest": contract, "entry_node_id": "start",
        "dependency_slots": {"native": {"kind": "capability", "requirement_digest": requirement}},
        "nodes": {
            "start": {"kind": "start", "next": "native"},
            "native": {"kind": "capability_call", "capability_slot_id": "native", "input": {"source": "run_input", "schema_digest": schema_digest},
                "candidate_route": null, "output": output, "attempt_limit": 1, "retry_backoff_milliseconds": 100, "resume": "finish"},
            "finish": {"kind": "return", "value": output}
        }
    });
    let plan_path = write_canonical(fixture, "native-progress-plan.json", &plan);
    let uploaded = upload_artifact(
        insight,
        project,
        &plan_path,
        "typed_plan",
        "native-progress-plan.json",
    );
    let mut manifest = json!({
        "schema_version": 1, "kind": "insight.platform.apply/v1", "resource_noun": "agents",
        "create": {"display_name": "Native development progress", "document": {"resource_kind": "agent", "spec": agent_resource_spec(
            "native-development-progress", vec![], json!({
                "authoring_package": {"artifact": authoring, "manifest_digest": authoring["content_digest"]},
                "contract_digest": contract, "dependency_versions": [], "policy_versions": [selection.revision, execution.revision],
                "input_schema": schema, "output_schema": schema, "error_schema": value_schema("error"),
                "typed_plan_artifact_id": uploaded["artifact_id"], "typed_plan_digest": uploaded["content_digest"]
            }))}},
        "publish": {"kind": "agent", "revision_no": 1, "interface_content_digest": contract, "plan_content_digest": uploaded["content_digest"], "artifact_id": uploaded["artifact_id"]},
        "deployment": {"environment": "local", "closure": {"resource_kind": "agent", "bindings": {
            "entry_node_id": "start", "entry_node_kind": "start",
            "slots": [{"slot_id": "native", "requirement_digest": requirement,
                "target": {"kind": "capability", "candidates": [deployment], "selection_policy": selection.binding}}],
            "policies": [], "execution_profile": execution.binding
        }}}
    });
    compile_agent_apply_fixture(insight, project, fixture, &plan, &mut manifest);
    let path = write_canonical(fixture, "native-progress-agent.apply.json", &manifest);
    let published = run_json(
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
    );

    // Use the public Gateway, with one shared deadline for admission, progress and result reads.
    let (client, base, token) = raw_runtime_client(project);
    let began = Instant::now();
    let deadline = began + StdDuration::from_secs(120);
    let remaining = || {
        deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
            .expect("50 Native Runs must drain within 120 seconds")
            .min(StdDuration::from_secs(10))
    };
    let mut pending = std::collections::BTreeMap::new();
    for index in 0..50 {
        let message = format!("native-progress-{index}");
        let response = client.post(format!("{base}/v1/runs"))
            .bearer_auth(&token).header("idempotency-key", format!("native-progress-{index}"))
            .timeout(remaining()).json(&json!({
                "agent_id": published["resource_id"],
                "input": {"classification": "internal", "schema_digest": schema_digest, "value": {"kind": "inline", "value": {"message": message}}},
                "deadline": (Utc::now() + Duration::minutes(5)).to_rfc3339_opts(SecondsFormat::Micros, true)
            })).send().expect("public Native Run admission");
        assert_eq!(response.status(), StatusCode::CREATED);
        let run: Value = response.json().expect("public Native Run identity");
        assert!(
            pending
                .insert(run["run_id"].as_str().unwrap().to_owned(), message)
                .is_none(),
            "each input has an independent Run"
        );
    }
    while !pending.is_empty() {
        let ids: Vec<_> = pending.keys().cloned().collect();
        for id in ids {
            let response = client
                .get(format!("{base}/v1/runs/{id}"))
                .bearer_auth(&token)
                .timeout(remaining())
                .send()
                .expect("public Native Run progress");
            assert_eq!(response.status(), StatusCode::OK);
            let run: Value = response.json().expect("public Native Run state");
            assert_eq!(run["run_id"], id);
            match run["state"].as_str().unwrap() {
                "queued" | "running" | "waiting" => continue,
                "succeeded" => {}
                state => panic!("Native progress Run ended in {state}"),
            }
            let response = client
                .get(format!("{base}/v1/runs/{id}/result"))
                .bearer_auth(&token)
                .timeout(remaining())
                .send()
                .expect("public Native result");
            assert_eq!(response.status(), StatusCode::OK);
            let result: Value = response.json().expect("public Native typed result");
            assert_eq!(result["schema_digest"], schema_digest);
            assert_eq!(
                result["value"],
                json!({"kind": "inline", "value": {"message": pending.remove(&id).unwrap()}})
            );
        }
        if !pending.is_empty() {
            thread::sleep(remaining().min(StdDuration::from_millis(100)));
        }
    }
    assert!(began.elapsed() < StdDuration::from_secs(120));
    println!(
        "50 distinct Native Capability Runs completed through the public Gateway in {:.3}s",
        began.elapsed().as_secs_f64()
    );
}
