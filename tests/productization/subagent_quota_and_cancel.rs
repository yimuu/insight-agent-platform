use super::*;
use sqlx::{postgres::PgPoolOptions, Row};

const DATABASE_URL: &str = "postgres://insight:insight@127.0.0.1:5432/insight_platform";

pub(super) struct SubagentEvidence {
    pub run_id: String,
    console_passed: bool,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
}

impl SubagentEvidence {
    pub(super) fn mark_console_passed(&mut self) {
        self.console_passed = true;
        self.finished_at = Utc::now();
    }

    pub(super) fn report(&self, revision: &str) -> Value {
        let check = |id: &str, status: &str, evidence: &str| json!({"id": id, "status": status, "evidence": evidence});
        let console = if self.console_passed {
            check(
                "console",
                "passed",
                "the real Console read the exact succeeded parent Run and its child.started/child.completed durable timeline from the fresh authority",
            )
        } else {
            check(
                "console",
                "not_run",
                "set PLATFORM_PRODUCTIZATION_CONSOLE_BROWSER=true to inspect the exact Subagent parent Run in the real Console",
            )
        };
        json!({
            "schema_version": 1,
            "report_kind": "insight.productization.scenario-report/v1",
            "scenario_id": "subagent-quota-and-cancel",
            "contract_profile": "insight.platform/v1",
            "profile": "starter",
            "automation_layer": "P2",
            "source_revision": revision,
            "environment": {
                "os": env::consts::OS,
                "architecture": env::consts::ARCH,
                "fresh_profile": true,
            },
            "started_at": self.started_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "finished_at": self.finished_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "status": if self.console_passed { "passed" } else { "incomplete" },
            "entrypoints": [
                check("cli", "passed", "public apply/run create/watch/get/result/cancel commands exercised succeeded, cancelled and quota-rejected parent Runs and exact child Runs"),
                check("http_fixture", "passed", "independent raw /v1 reads verified the succeeded parent and child projections after the durable child transaction"),
                console,
            ],
            "assertions": [
                check("typed_child_run", "passed", "a ChildAgentCall selected one exact Agent Deployment, created a distinct typed child Run and returned its exact Inline output to the parent"),
                check("quota_reservation", "passed", "the PostgreSQL authority atomically retained one child link and one root descendant reservation for the succeeded parent"),
                check("cancel_propagated", "passed", "public parent cancellation converged both the parent and its waiting child to cancelled under cascade_and_wait"),
            ],
            "failure_probes": [
                check("child_late_result", "passed", "after cancellation, the child timer became due but the exact child projection version and cancelled terminal state remained unchanged"),
                check("quota_exhaustion", "passed", "a child budget requesting the complete hard descendant allowance failed the parent without creating a partial child link or descendant reservation"),
            ],
        })
    }
}

pub(super) fn run(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    schema_digest: &str,
    agent_manifest: &Value,
    child_agent_report: &Value,
    qualification_ref: &Value,
) -> SubagentEvidence {
    let started_at = Utc::now();
    let selection = json!({
        "schema_version": 1,
        "mode": "only_candidate",
        "route_schema_digest": null,
    });
    let selection_digest = canonical_digest(&selection);
    let selection_manifest = json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": "policies",
        "create": {
            "display_name": "Productization only-candidate child selection",
            "document": {
                "resource_kind": "policy",
                "spec": {
                    "authoring_package": agent_manifest["create"]["document"]["spec"]["authoring_package"],
                    "contract_digest": selection_digest,
                    "dependency_versions": [],
                    "policy_versions": [],
                    "policy_kind": "selection",
                    "rules_digest": selection_digest,
                    "selection": selection,
                    "scheduling": null,
                    "retention": null,
                    "model_safety": null,
                    "model_budget": null,
                    "model_public_projection": null,
                    "mcp_protocol": null,
                    "mcp_auth": null,
                    "sandbox_isolation": null,
                    "sandbox_resource": null,
                    "sandbox_network": null,
                    "sandbox_artifact_io": null,
                    "sandbox_secret_resolution": null
                }
            }
        },
        "publish": {
            "kind": "single",
            "revision_no": 1,
            "content_digest": selection_digest,
            "artifact_id": null
        },
        "deployment": {
            "environment": "local",
            "closure": {
                "resource_kind": "policy",
                "bindings": {
                    "applicability_digest": canonical_digest(&json!({"environment": "local", "purpose": "child_selection"})),
                    "qualification_evidence": qualification_ref
                }
            }
        }
    });
    let selection_path = write_canonical(
        fixture,
        "child-selection-policy.apply.json",
        &selection_manifest,
    );
    let selection_report = run_json(
        insight,
        &[
            "apply",
            "--file",
            selection_path.to_str().unwrap(),
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let selection_revision = exact_version(published_version(&selection_report, "policy_revision"));
    let selection_closure = json!({
        "policy_revision": selection_revision,
        "applicability_digest": selection_manifest["deployment"]["closure"]["bindings"]["applicability_digest"],
        "qualification_evidence": qualification_ref,
    });
    let selection_binding = json!({
        "deployment": {
            "deployment_id": selection_report["deployment_id"],
            "resource_kind": "policy_deployment",
            "deployment_digest": canonical_digest(&json!({"schema_version": 1, "resource_kind": "policy", "bindings": selection_closure})),
        },
        "revision": selection_revision,
    });

    let success_parent = apply_parent(
        insight,
        project,
        fixture,
        "subagent-success",
        schema_digest,
        agent_manifest,
        child_agent_report,
        &agent_manifest["deployment"]["closure"]["bindings"],
        &selection_binding,
        2,
    );
    let success_run = create_run(
        insight,
        project,
        fixture,
        "subagent-success",
        &success_parent,
        schema_digest,
    );
    let success_run_id = success_run["run_id"]
        .as_str()
        .expect("Subagent success Run ID");
    let success_watch = run_json_lines(
        insight,
        &[
            "run",
            "watch",
            success_run_id,
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(success_watch.last().unwrap()["run"]["state"], "succeeded");
    assert!(success_watch.iter().any(
        |record| record["kind"] == "event" && record["event"]["event_type"] == "child.started"
    ));
    assert!(success_watch
        .iter()
        .any(|record| record["kind"] == "event"
            && record["event"]["event_type"] == "child.completed"));
    let success_child_id = wait_for_child_link(success_run_id).0;
    let child = run_json(
        insight,
        &[
            "run",
            "get",
            &success_child_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(child["state"], "succeeded");
    let result = run_json(
        insight,
        &[
            "run",
            "result",
            success_run_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(
        result["value"],
        json!({"kind": "inline", "value": {"message": "subagent-success"}})
    );
    prove_raw_http_read(project, success_run_id);
    prove_child_reservation(success_run_id, 1, 1);

    let (wait_child, wait_child_bindings) =
        apply_waiting_child(insight, project, fixture, schema_digest, agent_manifest);
    let cancel_parent = apply_parent(
        insight,
        project,
        fixture,
        "subagent-cancel",
        schema_digest,
        agent_manifest,
        &wait_child,
        &wait_child_bindings,
        &selection_binding,
        2,
    );
    let cancel_run = create_run(
        insight,
        project,
        fixture,
        "subagent-cancel",
        &cancel_parent,
        schema_digest,
    );
    let cancel_run_id = cancel_run["run_id"]
        .as_str()
        .expect("Subagent cancel Run ID");
    let (cancel_child_id, _) = wait_for_child_link(cancel_run_id);
    let cancelled = run_json(
        insight,
        &[
            "run",
            "cancel",
            cancel_run_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert!(matches!(
        cancelled["state"].as_str(),
        Some("cancelling" | "cancelled")
    ));
    wait_for_run_state(insight, project, cancel_run_id, "cancelled");
    let child_terminal = wait_for_run_state(insight, project, &cancel_child_id, "cancelled");
    let child_version = child_terminal["version"]
        .as_u64()
        .expect("cancelled child version");
    thread::sleep(StdDuration::from_millis(900));
    let child_after_timer = run_json(
        insight,
        &[
            "run",
            "get",
            &cancel_child_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(child_after_timer["state"], "cancelled");
    assert_eq!(child_after_timer["version"], child_version);

    let quota_parent = apply_parent(
        insight,
        project,
        fixture,
        "subagent-quota",
        schema_digest,
        agent_manifest,
        child_agent_report,
        &agent_manifest["deployment"]["closure"]["bindings"],
        &selection_binding,
        500,
    );
    let quota_run = create_run(
        insight,
        project,
        fixture,
        "subagent-quota",
        &quota_parent,
        schema_digest,
    );
    let quota_run_id = quota_run["run_id"].as_str().expect("Subagent quota Run ID");
    wait_for_run_state(insight, project, quota_run_id, "failed");
    prove_child_reservation(quota_run_id, 0, 0);

    SubagentEvidence {
        run_id: success_run_id.to_owned(),
        console_passed: false,
        started_at,
        finished_at: Utc::now(),
    }
}

fn apply_waiting_child(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    schema_digest: &str,
    agent_manifest: &Value,
) -> (Value, Value) {
    let plan = json!({
        "plan_version": 5,
        "interface_contract_digest": CONTRACT_DIGEST,
        "entry_node_id": "start",
        "dependency_slots": {},
        "nodes": {
            "start": {"kind": "start", "next": "wait"},
            "wait": {"kind": "timer_wait", "delay_milliseconds": 500, "resume": "finish"},
            "finish": {"kind": "return", "value": {"source": "run_input", "schema_digest": schema_digest}}
        }
    });
    let path = write_canonical(fixture, "subagent-wait-child-plan.json", &plan);
    let upload = upload_artifact(
        insight,
        project,
        &path,
        "typed_plan",
        "subagent-wait-child-plan.json",
    );
    let mut manifest = agent_manifest.clone();
    manifest["create"]["display_name"] = json!("Subagent cancellation child");
    manifest["create"]["document"]["spec"]["typed_plan_artifact_id"] =
        upload["artifact_id"].clone();
    manifest["create"]["document"]["spec"]["typed_plan_digest"] = upload["content_digest"].clone();
    manifest["publish"]["plan_content_digest"] = upload["content_digest"].clone();
    manifest["publish"]["artifact_id"] = upload["artifact_id"].clone();
    let manifest_path = write_canonical(fixture, "subagent-wait-child.apply.json", &manifest);
    let deployment_bindings = manifest["deployment"]["closure"]["bindings"].clone();
    let report = run_json(
        insight,
        &[
            "apply",
            "--file",
            manifest_path.to_str().unwrap(),
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    (report, deployment_bindings)
}

#[allow(clippy::too_many_arguments)]
fn apply_parent(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    name: &str,
    schema_digest: &str,
    agent_manifest: &Value,
    child_report: &Value,
    child_bindings: &Value,
    selection_binding: &Value,
    descendants: u32,
) -> Value {
    let requirement_digest = canonical_digest(
        &json!({"kind": "child_agent", "input_schema_digest": schema_digest, "output_schema_digest": schema_digest}),
    );
    let child_closure = json!({
        "interface": exact_version(published_version(
            child_report,
            "agent_interface_revision",
        )),
        "plan": exact_version(published_version(child_report, "agent_plan_revision")),
        "entry_node_id": child_bindings["entry_node_id"],
        "entry_node_kind": child_bindings["entry_node_kind"],
        "slots": child_bindings["slots"],
        "policies": child_bindings["policies"],
        "execution_profile": child_bindings["execution_profile"],
    });
    let child_deployment = json!({
        "deployment_id": child_report["deployment_id"],
        "resource_kind": "agent_deployment",
        "deployment_digest": canonical_digest(&json!({"schema_version": 1, "resource_kind": "agent", "bindings": child_closure})),
    });
    let target = json!({"kind": "child_agent", "candidates": [child_deployment], "selection_policy": selection_binding});
    let slot = json!({
        "slot_id": "child_worker",
        "requirement_digest": requirement_digest,
        "target": target,
    });
    let plan = json!({
        "plan_version": 5,
        "interface_contract_digest": CONTRACT_DIGEST,
        "entry_node_id": "start",
        "dependency_slots": {"child_worker": {"kind": "child_agent", "requirement_digest": requirement_digest}},
        "nodes": {
            "start": {"kind": "start", "next": "child"},
            "child": {
                "kind": "child_agent_call",
                "child_agent_slot_id": "child_worker",
                "input": {"source": "run_input", "schema_digest": schema_digest},
                "candidate_route": null,
                "output": {"source": "node_output", "producer_node_id": "child", "port_id": "response", "schema_digest": schema_digest},
                "budget": {"maximum_duration_milliseconds": 60000, "maximum_model_tokens": 1000, "maximum_capability_calls": 10, "maximum_artifact_bytes": 1048576, "maximum_descendant_runs": descendants},
                "cancellation_policy": "cascade_and_wait",
                "attempt_limit": 1,
                "retry_backoff_milliseconds": 100,
                "resume": "finish"
            },
            "finish": {"kind": "return", "value": {"source": "node_output", "producer_node_id": "child", "port_id": "response", "schema_digest": schema_digest}}
        }
    });
    let plan_path = write_canonical(fixture, &format!("{name}-plan.json"), &plan);
    let upload = upload_artifact(
        insight,
        project,
        &plan_path,
        "typed_plan",
        &format!("{name}-plan.json"),
    );
    let mut manifest = agent_manifest.clone();
    manifest["create"]["display_name"] = json!(format!("Productization {name}"));
    manifest["create"]["document"]["spec"]["typed_plan_artifact_id"] =
        upload["artifact_id"].clone();
    manifest["create"]["document"]["spec"]["typed_plan_digest"] = upload["content_digest"].clone();
    manifest["create"]["document"]["spec"]["policy_versions"]
        .as_array_mut()
        .unwrap()
        .push(selection_binding["revision"].clone());
    manifest["publish"]["plan_content_digest"] = upload["content_digest"].clone();
    manifest["publish"]["artifact_id"] = upload["artifact_id"].clone();
    manifest["deployment"]["closure"]["bindings"]["entry_node_kind"] = json!("start");
    manifest["deployment"]["closure"]["bindings"]["slots"] = json!([slot]);
    manifest["deployment"]["closure"]["bindings"]["policies"]
        .as_array_mut()
        .unwrap()
        .push(selection_binding.clone());
    let path = write_canonical(fixture, &format!("{name}.apply.json"), &manifest);
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

fn create_run(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    name: &str,
    agent: &Value,
    schema_digest: &str,
) -> Value {
    let request = json!({
        "agent_id": agent["resource_id"],
        "input": {"classification": "internal", "schema_digest": schema_digest, "value": {"kind": "inline", "value": {"message": name}}},
        "deadline": (Utc::now() + Duration::minutes(5)).to_rfc3339_opts(SecondsFormat::Micros, true),
    });
    let path = write_canonical(fixture, &format!("{name}-run.json"), &request);
    run_json(
        insight,
        &[
            "run",
            "create",
            "--file",
            path.to_str().unwrap(),
            "--path",
            project.to_str().unwrap(),
        ],
    )
}

fn wait_for_run_state(insight: &Path, project: &Path, run_id: &str, expected: &str) -> Value {
    let deadline = Instant::now() + StdDuration::from_secs(30);
    loop {
        let run = run_json(
            insight,
            &["run", "get", run_id, "--path", project.to_str().unwrap()],
        );
        if run["state"] == expected {
            return run;
        }
        assert!(
            Instant::now() < deadline,
            "Run {run_id} did not reach {expected}: {run}"
        );
        thread::sleep(StdDuration::from_millis(50));
    }
}

fn wait_for_child_link(parent_run_id: &str) -> (String, String) {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let pool = PgPoolOptions::new().max_connections(1).connect(DATABASE_URL).await.unwrap();
        let deadline = Instant::now() + StdDuration::from_secs(30);
        loop {
            if let Some(row) = sqlx::query("SELECT related_run_id, node_id FROM insight_platform.run_nodes WHERE run_id = $1 AND record_kind = 'child_run_link'")
                .bind(parent_run_id).fetch_optional(&pool).await.unwrap() {
                return (row.get::<String, _>("related_run_id"), row.get::<String, _>("node_id"));
            }
            assert!(Instant::now() < deadline, "parent Run {parent_run_id} did not create a child link");
            tokio::time::sleep(StdDuration::from_millis(50)).await;
        }
    })
}

fn prove_child_reservation(parent_run_id: &str, expected_links: i64, expected_descendants: i32) {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let pool = PgPoolOptions::new().max_connections(1).connect(DATABASE_URL).await.unwrap();
        let links: i64 = sqlx::query_scalar("SELECT count(*) FROM insight_platform.run_nodes WHERE run_id = $1 AND record_kind = 'child_run_link'").bind(parent_run_id).fetch_one(&pool).await.unwrap();
        let descendants: i32 = sqlx::query_scalar("SELECT descendant_count FROM insight_platform.runs WHERE run_id = $1").bind(parent_run_id).fetch_one(&pool).await.unwrap();
        assert_eq!(links, expected_links);
        assert_eq!(descendants, expected_descendants);
    });
}
