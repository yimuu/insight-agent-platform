use super::*;

pub(super) struct TimerSignalEvidence {
    pub run_id: String,
    console_passed: bool,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
}

impl TimerSignalEvidence {
    pub fn mark_console_passed(&mut self) {
        self.console_passed = true;
        self.finished_at = Utc::now();
    }

    pub fn report(&self, revision: &str) -> Value {
        let check = |id: &str, status: &str, evidence: &str| json!({"id": id, "status": status, "evidence": evidence});
        let console = if self.console_passed {
            check(
                "console",
                "passed",
                "the same real headless Chromium session read this exact Timer/Signal Run ID from the fresh Gateway/PostgreSQL authority and verified its succeeded Inline result after replacement-Worker recovery",
            )
        } else {
            check(
                "console",
                "not_run",
                "set PLATFORM_PRODUCTIZATION_CONSOLE_BROWSER=true to read the exact Timer/Signal Run through the real Console",
            )
        };
        json!({
            "schema_version": 1,
            "report_kind": "insight.productization.scenario-report/v1",
            "scenario_id": "timer-signal-restart-recovery",
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
            "status": "incomplete",
            "entrypoints": [
                check("cli", "passed", "public insight artifact/apply/run watch/result commands completed on the fresh base authority"),
                check("http_fixture", "passed", "raw public /v1 signal submission and exact Receipt replay both returned the closed 204 contract"),
                console,
            ],
            "assertions": [
                check("restart_resume", "passed", "the accepted signal durably moved the Run to running with a ready continuation while the Orchestration Worker was absent, then an exact replacement Worker reached succeeded"),
                check("event_sequence", "passed", "public polling observed distinct TimerWait and SignalWait Run projection versions, then bounded SSE reached one terminal succeeded projection"),
                check("no_duplicate_effect", "passed", "replaying the exact signal Receipt did not create a second Run transition"),
            ],
            "failure_probes": [
                check("worker_kill_before_commit", "passed", "the Worker was terminated before signal submission; the owner transaction consumed the signal and retained a durable ready continuation without a Worker"),
                check("stale_job_fence", "not_run", "a checked stale lease-token commit requires a controlled Worker fault hook not exposed by this P2 public fixture"),
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
    replacement: RestartedOrchestrationWorker,
) -> (TimerSignalEvidence, RestartedOrchestrationWorker) {
    let started_at = Utc::now();
    let plan = json!({
        "plan_version": 5,
        "interface_contract_digest": CONTRACT_DIGEST,
        "entry_node_id": "start",
        "dependency_slots": {},
        "nodes": {
            "start": {"kind": "start", "next": "delay"},
            "delay": {
                "kind": "timer_wait",
                "delay_milliseconds": 250,
                "resume": "await_release"
            },
            "await_release": {
                "kind": "signal_wait",
                "signal_key": "release",
                "payload": null,
                "timeout_milliseconds": 120000,
                "resume": "finish"
            },
            "finish": {
                "kind": "return",
                "value": {"source": "run_input", "schema_digest": schema_digest}
            }
        }
    });
    let plan_path = write_canonical(fixture, "timer-signal-plan.json", &plan);
    let plan_upload = upload_artifact(
        insight,
        project,
        &plan_path,
        "typed_plan",
        "timer-signal-plan.json",
    );
    let mut manifest = agent_manifest.clone();
    manifest["create"]["display_name"] = json!("Timer signal recovery agent");
    manifest["create"]["document"]["spec"]["typed_plan_artifact_id"] =
        plan_upload["artifact_id"].clone();
    manifest["create"]["document"]["spec"]["typed_plan_digest"] =
        plan_upload["content_digest"].clone();
    manifest["publish"]["plan_content_digest"] = plan_upload["content_digest"].clone();
    manifest["publish"]["artifact_id"] = plan_upload["artifact_id"].clone();
    let manifest_path = write_canonical(fixture, "timer-signal-agent.apply.json", &manifest);
    let agent = run_json(
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
    let request = json!({
        "agent_id": agent["resource_id"],
        "input": {
            "classification": "internal",
            "schema_digest": schema_digest,
            "value": {"kind": "inline", "value": {"message": "resume after signal"}}
        },
        "deadline": (Utc::now() + Duration::minutes(5))
            .to_rfc3339_opts(SecondsFormat::Micros, true),
    });
    let request_path = write_canonical(fixture, "timer-signal-run.json", &request);
    let run = run_json(
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
    let run_id = run["run_id"].as_str().expect("timer signal Run ID");

    let wait_deadline = Instant::now() + StdDuration::from_secs(15);
    let first_wait_version = loop {
        let view = run_json(
            insight,
            &["run", "get", run_id, "--path", project.to_str().unwrap()],
        );
        if view["state"] == "waiting" {
            break view["version"].as_u64().expect("timer wait Run version");
        }
        assert!(
            Instant::now() < wait_deadline,
            "timer signal Run did not reach waiting"
        );
        thread::sleep(StdDuration::from_millis(50));
    };
    let signal_wait_deadline = Instant::now() + StdDuration::from_secs(15);
    loop {
        let view = run_json(
            insight,
            &["run", "get", run_id, "--path", project.to_str().unwrap()],
        );
        if view["state"] == "waiting"
            && view["version"]
                .as_u64()
                .is_some_and(|version| version > first_wait_version)
        {
            break;
        }
        assert!(
            Instant::now() < signal_wait_deadline,
            "timer signal Run did not advance from TimerWait to SignalWait"
        );
        thread::sleep(StdDuration::from_millis(50));
    }
    drop(replacement);

    let (client, base_url, token) = raw_runtime_client(project);
    let receipt = format!("productization-signal-{run_id}");
    for _ in 0..2 {
        let response = client
            .post(format!("{base_url}/v1/runs/{run_id}/signals/release"))
            .bearer_auth(&token)
            .header("accept", "application/json")
            .header("content-type", "application/json")
            .header("idempotency-key", &receipt)
            .json(&json!({"payload": null}))
            .send()
            .expect("raw signal request completes");
        let status = response.status();
        let has_trace_id = response.headers().contains_key("trace-id");
        let body = response.text().expect("raw signal response is readable");
        assert_eq!(status, StatusCode::NO_CONTENT, "signal response: {body}");
        assert!(has_trace_id);
    }
    let waiting = run_json(
        insight,
        &["run", "get", run_id, "--path", project.to_str().unwrap()],
    );
    assert_eq!(waiting["state"], "running");

    let replacement = restart_orchestration_worker(insight, project);
    let watched = run_json_lines(
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
    let terminal = watched.last().expect("timer signal terminal watch record");
    assert_eq!(terminal["kind"], "terminal");
    assert_eq!(terminal["run"]["state"], "succeeded");
    let result = run_json(
        insight,
        &["run", "result", run_id, "--path", project.to_str().unwrap()],
    );
    assert_eq!(
        result["value"],
        json!({"kind": "inline", "value": {"message": "resume after signal"}})
    );

    (
        TimerSignalEvidence {
            run_id: run_id.to_owned(),
            console_passed: false,
            started_at,
            finished_at: Utc::now(),
        },
        replacement,
    )
}
