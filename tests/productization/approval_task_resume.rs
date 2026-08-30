use super::*;

pub(super) struct ApprovalTaskEvidence {
    pub run_id: String,
    pub console_passed: bool,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
}

impl ApprovalTaskEvidence {
    pub fn report(&self, revision: &str) -> Value {
        let check = |id: &str, status: &str, evidence: &str| json!({"id": id, "status": status, "evidence": evidence});
        let console = if self.console_passed {
            check(
                "console",
                "passed",
                "a headless Chromium journey used the static Console through a transparent same-origin loopback proxy against the same fresh Gateway/PostgreSQL authority, discovered a waiting Task through SSE, submitted typed input, observed the terminal Run, and proved reload clears browser-only credentials and projections",
            )
        } else {
            check(
                "console",
                "not_run",
                "set PLATFORM_PRODUCTIZATION_CONSOLE_BROWSER=true to run the real Gateway browser journey",
            )
        };
        let status = if self.console_passed {
            "passed"
        } else {
            "incomplete"
        };
        json!({
            "schema_version": 1,
            "report_kind": "insight.productization.scenario-report/v1",
            "scenario_id": "approval-task-resume",
            "contract_profile": "insight.platform/v1",
            "profile": "base",
            "automation_layer": "P2",
            "source_revision": revision,
            "environment": {
                "os": env::consts::OS,
                "architecture": env::consts::ARCH,
                "fresh_profile": true,
            },
            "started_at": self.started_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "finished_at": self.finished_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "status": status,
            "entrypoints": [
                check("cli", "passed", "public insight apply/run/watch/task/result commands completed against a fresh base profile"),
                check("http_fixture", "passed", "an independent raw /v1 Task mutation exercised the stale ETag and distinct Receipt fence"),
                console,
            ],
            "assertions": [
                check("task_waiting", "passed", "the Run reached waiting and exposed one pending interaction_form Task with the exact owner and schema"),
                check("first_winner", "passed", "the first submit-input won; exact replay returned the same Task and a distinct stale mutation was rejected"),
                check("run_resumed", "passed", "durable SSE observed interaction.resolved and the Run reached succeeded with the submitted value"),
            ],
            "failure_probes": [
                check("task_replay", "passed", "repeating the same CLI journal returned the exact same responded Task authority"),
                check("stale_task_fence", "passed", "the original Task ETag with a new Receipt returned closed 409 invalid_state_transition"),
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
    console_authorities: (&str, &str, &str, &str),
) -> ApprovalTaskEvidence {
    let (deterministic_run_id, timer_signal_run_id, subagent_run_id, artifact_id) =
        console_authorities;
    let started_at = Utc::now();
    let human_task_plan = json!({
        "plan_version": 5,
        "interface_contract_digest": CONTRACT_DIGEST,
        "entry_node_id": "start",
        "dependency_slots": {},
        "nodes": {
            "start": {"kind": "start", "next": "collect_input"},
            "collect_input": {
                "kind": "human_task",
                "definition": {
                    "kind": "interaction",
                    "interaction_kind": "form",
                    "eligible_principal_rule_digest": canonical_digest(&json!({
                        "rule": "local_developer",
                    })),
                    "safe_prompt_key": "provide_message",
                },
                "response": {
                    "source": "node_output",
                    "producer_node_id": "collect_input",
                    "port_id": "response",
                    "schema_digest": schema_digest,
                },
                "timeout_milliseconds": 240_000,
                "resume": "finish",
            },
            "finish": {
                "kind": "return",
                "value": {
                    "source": "node_output",
                    "producer_node_id": "collect_input",
                    "port_id": "response",
                    "schema_digest": schema_digest,
                },
            },
        },
    });
    let human_task_plan_path = write_canonical(fixture, "human-task-plan.json", &human_task_plan);
    let human_task_plan_upload = upload_artifact(
        insight,
        project,
        &human_task_plan_path,
        "typed_plan",
        "human-task-plan.json",
    );
    let mut human_task_manifest = agent_manifest.clone();
    human_task_manifest["create"]["display_name"] = json!("Deterministic human task agent");
    human_task_manifest["create"]["document"]["spec"]["typed_plan_artifact_id"] =
        human_task_plan_upload["artifact_id"].clone();
    human_task_manifest["create"]["document"]["spec"]["typed_plan_digest"] =
        human_task_plan_upload["content_digest"].clone();
    human_task_manifest["publish"]["plan_content_digest"] =
        human_task_plan_upload["content_digest"].clone();
    human_task_manifest["publish"]["artifact_id"] = human_task_plan_upload["artifact_id"].clone();
    let human_task_manifest_path =
        write_canonical(fixture, "human-task-agent.apply.json", &human_task_manifest);
    let human_task_agent = run_json(
        insight,
        &[
            "apply",
            "--file",
            human_task_manifest_path.to_str().unwrap(),
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );

    let human_task_request = json!({
        "agent_id": human_task_agent["resource_id"],
        "input": {
            "classification": "internal",
            "schema_digest": schema_digest,
            "value": {"kind": "inline", "value": {"message": "before task"}},
        },
        "deadline": (Utc::now() + Duration::minutes(5))
            .to_rfc3339_opts(SecondsFormat::Micros, true),
    });
    let human_task_request_path =
        write_canonical(fixture, "human-task-run.json", &human_task_request);
    let human_task_run = run_json(
        insight,
        &[
            "run",
            "create",
            "--file",
            human_task_request_path.to_str().unwrap(),
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let run_id = human_task_run["run_id"]
        .as_str()
        .expect("Human Task Run ID")
        .to_owned();
    let mut watcher = Command::new(insight)
        .args([
            "run",
            "watch",
            &run_id,
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Human Task Run watcher starts");
    let stdout = watcher.stdout.take().expect("watcher stdout is piped");
    let mut lines = BufReader::new(stdout).lines();
    let mut watch_records = Vec::new();
    let task_id = loop {
        let line = lines
            .next()
            .unwrap_or_else(|| panic!("Run watch ended before interaction.required"))
            .expect("Run watch line is readable");
        let record: Value = serde_json::from_str(&line).expect("Run watch line is closed JSON");
        let task_id = (record["kind"] == "event"
            && record["event"]["event_type"] == "interaction.required")
            .then(|| record["event"]["data"]["source_id"].as_str())
            .flatten()
            .map(str::to_owned);
        watch_records.push(record);
        if let Some(task_id) = task_id {
            break task_id;
        }
    };

    let waiting_run = run_json(
        insight,
        &["run", "get", &run_id, "--path", project.to_str().unwrap()],
    );
    assert_eq!(waiting_run["state"], "waiting");
    let pending_task = run_json(
        insight,
        &["task", "get", &task_id, "--path", project.to_str().unwrap()],
    );
    assert_eq!(pending_task["task_kind"], "interaction_form");
    assert_eq!(pending_task["state"], "pending");
    assert_eq!(pending_task["owner"]["run_id"], run_id);
    assert_eq!(pending_task["response_schema_digest"], schema_digest);
    let pending_task_etag = pending_task["etag"]
        .as_str()
        .expect("pending Task ETag")
        .to_owned();

    let task_response = json!({
        "classification": "internal",
        "schema_digest": schema_digest,
        "value": {"kind": "inline", "value": {"message": "after task"}},
    });
    let task_response_path = write_canonical(fixture, "human-task-response.json", &task_response);
    let responded_task = run_json(
        insight,
        &[
            "task",
            "submit-input",
            &task_id,
            "--file",
            task_response_path.to_str().unwrap(),
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(responded_task["state"], "responded");
    let replayed_task = run_json(
        insight,
        &[
            "task",
            "submit-input",
            &task_id,
            "--file",
            task_response_path.to_str().unwrap(),
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(replayed_task, responded_task);
    prove_stale_task_fence(project, &task_id, &pending_task_etag, &task_response);

    for line in lines {
        watch_records.push(
            serde_json::from_str(&line.expect("Run watch line is readable"))
                .expect("Run watch line is closed JSON"),
        );
    }
    let watch_status = watcher.wait().expect("Run watcher status is readable");
    assert!(watch_status.success(), "Human Task Run watch failed");
    let terminal = watch_records.last().expect("Human Task terminal record");
    assert_eq!(terminal["kind"], "terminal");
    assert_eq!(terminal["run"]["state"], "succeeded");
    assert!(watch_records.iter().any(|record| {
        record["kind"] == "event"
            && record["event"]["event_type"] == "interaction.resolved"
            && record["event"]["data"]["source_id"] == task_id
    }));
    let human_task_result = run_json(
        insight,
        &[
            "run",
            "result",
            &run_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(
        human_task_result["value"],
        json!({"kind": "inline", "value": {"message": "after task"}})
    );
    assert_eq!(human_task_result["schema_digest"], schema_digest);

    let console_passed =
        if env::var("PLATFORM_PRODUCTIZATION_CONSOLE_BROWSER").as_deref() == Ok("true") {
            run_real_gateway_console_journey(
                insight,
                project,
                fixture,
                schema_digest,
                human_task_agent["resource_id"]
                    .as_str()
                    .expect("Human Task Agent ID"),
                (
                    deterministic_run_id,
                    timer_signal_run_id,
                    subagent_run_id,
                    artifact_id,
                ),
            );
            true
        } else {
            false
        };

    ApprovalTaskEvidence {
        run_id,
        console_passed,
        started_at,
        finished_at: Utc::now(),
    }
}

fn run_real_gateway_console_journey(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    schema_digest: &str,
    agent_id: &str,
    authorities: (&str, &str, &str, &str),
) {
    let (deterministic_run_id, timer_signal_run_id, subagent_run_id, artifact_id) = authorities;
    let request = json!({
        "agent_id": agent_id,
        "input": {
            "classification": "internal",
            "schema_digest": schema_digest,
            "value": {"kind": "inline", "value": {"message": "before browser task"}},
        },
        "deadline": (Utc::now() + Duration::minutes(5))
            .to_rfc3339_opts(SecondsFormat::Micros, true),
    });
    let request_path = write_canonical(fixture, "console-human-task-run.json", &request);
    let created = run_json(
        insight,
        &[
            "run",
            "create",
            "--file",
            request_path.to_str().unwrap(),
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let run_id = created["run_id"]
        .as_str()
        .expect("Console Human Task Run ID")
        .to_owned();
    let mut watcher = Command::new(insight)
        .args([
            "run",
            "watch",
            &run_id,
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Console Human Task Run watcher starts");
    let stdout = watcher
        .stdout
        .take()
        .expect("Console watcher stdout is piped");
    let mut lines = BufReader::new(stdout).lines();
    let mut watch_records = Vec::new();
    let task_id = loop {
        let line = lines
            .next()
            .unwrap_or_else(|| panic!("Console Run watch ended before interaction.required"))
            .expect("Console Run watch line is readable");
        let record: Value =
            serde_json::from_str(&line).expect("Console Run watch line is closed JSON");
        let task_id = (record["kind"] == "event"
            && record["event"]["event_type"] == "interaction.required")
            .then(|| record["event"]["data"]["source_id"].as_str())
            .flatten()
            .map(str::to_owned);
        watch_records.push(record);
        if let Some(task_id) = task_id {
            break task_id;
        }
    };

    let response = json!({
        "classification": "internal",
        "schema_digest": schema_digest,
        "value": {"kind": "inline", "value": {"message": "after task"}},
    });
    let (_, gateway_origin, token) = raw_runtime_client(project);
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let node = env::var("PLATFORM_PRODUCTIZATION_NODE_BIN").unwrap_or_else(|_| "node".to_owned());
    let output = Command::new(node)
        .arg(workspace.join("web/console/tests/real-gateway-journey.mjs"))
        .env("INSIGHT_CONSOLE_GATEWAY_ORIGIN", gateway_origin)
        .env("INSIGHT_CONSOLE_ACCESS_TOKEN", token)
        .env("INSIGHT_CONSOLE_RUN_ID", &run_id)
        .env("INSIGHT_CONSOLE_TASK_ID", &task_id)
        .env("INSIGHT_CONSOLE_DETERMINISTIC_RUN_ID", deterministic_run_id)
        .env("INSIGHT_CONSOLE_TIMER_SIGNAL_RUN_ID", timer_signal_run_id)
        .env("INSIGHT_CONSOLE_SUBAGENT_RUN_ID", subagent_run_id)
        .env("INSIGHT_CONSOLE_ARTIFACT_ID", artifact_id)
        .env(
            "INSIGHT_CONSOLE_TASK_RESPONSE",
            serde_json::to_string(&response).expect("Console Task response is JSON"),
        )
        .output()
        .expect("real Gateway Console browser journey starts");
    assert!(
        output.status.success(),
        "real Gateway Console browser journey failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let browser_evidence: Value = serde_json::from_slice(&output.stdout)
        .expect("real Gateway Console browser evidence is closed JSON");
    assert_eq!(
        browser_evidence["kind"],
        "insight.console.real-gateway-journey/v1"
    );
    assert_eq!(browser_evidence["status"], "passed");
    assert_eq!(browser_evidence["run_id"], run_id);
    assert_eq!(browser_evidence["task_id"], task_id);
    assert_eq!(
        browser_evidence["deterministic_run_id"],
        deterministic_run_id
    );
    assert_eq!(browser_evidence["timer_signal_run_id"], timer_signal_run_id);
    assert_eq!(browser_evidence["subagent_run_id"], subagent_run_id);
    assert_eq!(browser_evidence["artifact_id"], artifact_id);

    for line in lines {
        watch_records.push(
            serde_json::from_str(&line.expect("Console Run watch line is readable"))
                .expect("Console Run watch line is closed JSON"),
        );
    }
    assert!(
        watcher
            .wait()
            .expect("Console Run watcher status is readable")
            .success(),
        "Console Human Task Run watch failed"
    );
    assert_eq!(
        watch_records.last().expect("Console Run terminal record")["run"]["state"],
        "succeeded"
    );
    let result = run_json(
        insight,
        &[
            "run",
            "result",
            &run_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(
        result["value"],
        json!({"kind": "inline", "value": {"message": "after task"}})
    );
}

fn prove_stale_task_fence(project: &Path, task_id: &str, stale_etag: &str, input: &Value) {
    let (client, base_url, token) = raw_runtime_client(project);
    let response = client
        .post(format!("{base_url}/v1/tasks/{task_id}:submit-input"))
        .bearer_auth(token)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .header("idempotency-key", "productization-stale-task-fence")
        .header("if-match", stale_etag)
        .json(input)
        .send()
        .expect("stale Task mutation completes with a public Problem");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        response
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store, private, max-age=0")
    );
    assert!(response.headers().contains_key("trace-id"));
    let problem: Value = response.json().expect("stale Task Problem is JSON");
    assert_eq!(problem["status"], 409);
    assert_eq!(problem["code"], "invalid_state_transition");
    assert_eq!(problem["retryable"], false);
}
