use chrono::{Duration, SecondsFormat, Utc};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    env,
    fmt::Write as _,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader},
    net::TcpStream,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration as StdDuration, Instant},
};
use tempfile::TempDir;

const PROJECT_ENV: &str = "PLATFORM_PRODUCTIZATION_PROJECT";
const INSIGHT_BIN_ENV: &str = "PLATFORM_INSIGHT_BIN";
const CONTRACT_DIGEST: &str =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct RestartedOrchestrationWorker(Child);

impl Drop for RestartedOrchestrationWorker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn process_is_running(pid: u64) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn stop_orchestration_worker(project: &Path) {
    let process_state: Value = serde_json::from_slice(
        &fs::read(project.join(".insight/runtime/processes.json"))
            .expect("runtime process state is readable"),
    )
    .expect("runtime process state is closed JSON");
    let pid = process_state["processes"]["orchestration"]["pid"]
        .as_u64()
        .expect("orchestration PID");
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .is_ok_and(|status| status.success()),
        "orchestration worker accepts TERM"
    );
    let deadline = Instant::now() + StdDuration::from_secs(15);
    while process_is_running(pid) && Instant::now() < deadline {
        thread::sleep(StdDuration::from_millis(50));
    }
    assert!(
        !process_is_running(pid),
        "orchestration worker did not stop"
    );
}

fn restart_orchestration_worker(insight: &Path, project: &Path) -> RestartedOrchestrationWorker {
    let workspace = insight
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .expect("insight binary is inside workspace target directory");
    let runtime = project.join(".insight/runtime");
    let profile: Value = serde_json::from_slice(
        &fs::read(runtime.join("profile.json")).expect("runtime profile is readable"),
    )
    .expect("runtime profile is closed JSON");
    let port = profile["ports"]["orchestration_observability"]
        .as_u64()
        .and_then(|value| u16::try_from(value).ok())
        .expect("orchestration observability port");
    let config_digest = profile["config_digests"]["orchestration"]
        .as_str()
        .expect("orchestration config digest");
    let tls = runtime.join("tls");
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(runtime.join("logs/orchestration-restart.log"))
        .expect("restart log is writable");
    let stderr = log.try_clone().expect("restart log can be cloned");
    let mut child = Command::new(workspace.join("target/release/platform-orchestration-worker"))
        .env(
            "PLATFORM_ORCHESTRATION_WORKER_CONFIG",
            runtime.join("config/orchestration.json"),
        )
        .env("PLATFORM_ORCHESTRATION_WORKER_CONFIG_DIGEST", config_digest)
        .env(
            "PLATFORM_ORCHESTRATION_WORKER_DATABASE_URL",
            "postgres://insight:insight@127.0.0.1:5432/insight_platform",
        )
        .env(
            "PLATFORM_ORCHESTRATION_WORKER_ARTIFACT_CA_PATH",
            tls.join("ca.pem"),
        )
        .env(
            "PLATFORM_ORCHESTRATION_WORKER_ARTIFACT_CERT_PATH",
            tls.join("orchestration-client.pem"),
        )
        .env(
            "PLATFORM_ORCHESTRATION_WORKER_ARTIFACT_KEY_PATH",
            tls.join("orchestration-client-key.pem"),
        )
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("replacement orchestration worker starts");
    let address = format!("127.0.0.1:{port}");
    let deadline = Instant::now() + StdDuration::from_secs(30);
    loop {
        if TcpStream::connect(&address).is_ok() {
            break;
        }
        if let Some(status) = child.try_wait().expect("replacement status is readable") {
            panic!("replacement orchestration worker exited before readiness: {status}");
        }
        assert!(
            Instant::now() < deadline,
            "replacement orchestration worker did not become ready"
        );
        thread::sleep(StdDuration::from_millis(50));
    }
    RestartedOrchestrationWorker(child)
}

fn canonical_bytes(value: &Value) -> Vec<u8> {
    serde_jcs::to_vec(value).expect("fixture is canonicalizable")
}

fn digest_bytes(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(&mut encoded, "{byte:02x}").expect("String writes cannot fail");
    }
    format!("sha256:{encoded}")
}

fn canonical_digest(value: &Value) -> String {
    digest_bytes(&canonical_bytes(value))
}

fn run_json(insight: &Path, arguments: &[&str]) -> Value {
    let output = Command::new(insight)
        .args(arguments)
        .output()
        .expect("insight command starts");
    assert!(
        output.status.success(),
        "insight {:?} failed\nstdout:\n{}\nstderr:\n{}",
        arguments,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "insight {:?} did not emit one JSON document: {error}\nstdout:\n{}\nstderr:\n{}",
            arguments,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn run_json_lines(insight: &Path, arguments: &[&str]) -> Vec<Value> {
    let output = Command::new(insight)
        .args(arguments)
        .output()
        .expect("insight command starts");
    assert!(
        output.status.success(),
        "insight {:?} failed\nstdout:\n{}\nstderr:\n{}",
        arguments,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("insight JSON Lines are UTF-8");
    let records = stdout
        .lines()
        .map(|line| {
            serde_json::from_str(line).unwrap_or_else(|error| {
                panic!(
                    "insight {:?} emitted invalid JSON Line: {error}\nline:\n{line}\nstderr:\n{}",
                    arguments,
                    String::from_utf8_lossy(&output.stderr)
                )
            })
        })
        .collect::<Vec<_>>();
    assert!(
        !records.is_empty(),
        "insight {:?} emitted no JSON Lines\nstderr:\n{}",
        arguments,
        String::from_utf8_lossy(&output.stderr)
    );
    records
}

fn run_failure(insight: &Path, arguments: &[&str]) -> String {
    let output = Command::new(insight)
        .args(arguments)
        .output()
        .expect("insight command starts");
    assert!(
        !output.status.success(),
        "insight {:?} unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        arguments,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.is_empty(),
        "failed insight {:?} emitted a success document:\n{}",
        arguments,
        String::from_utf8_lossy(&output.stdout)
    );
    String::from_utf8(output.stderr).expect("insight failure is UTF-8")
}

fn write_canonical(directory: &Path, name: &str, value: &Value) -> std::path::PathBuf {
    let path = directory.join(name);
    fs::write(&path, canonical_bytes(value)).expect("fixture artifact is writable");
    path
}

fn upload_artifact(
    insight: &Path,
    project: &Path,
    source: &Path,
    purpose: &str,
    display_name: &str,
) -> Value {
    run_json(
        insight,
        &[
            "artifact",
            "upload",
            "--file",
            source.to_str().unwrap(),
            "--purpose",
            purpose,
            "--classification",
            "internal",
            "--media-type",
            "application/json",
            "--display-name",
            display_name,
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    )
}

fn artifact_ref(report: &Value, display_name: &str) -> Value {
    json!({
        "artifact_id": report["artifact_id"],
        "content_digest": report["content_digest"],
        "byte_length": report["byte_length"],
        "media_type": report["media_type"],
        "classification": "internal",
        "display_name": display_name,
    })
}

fn published_version<'a>(report: &'a Value, kind: &str) -> &'a Value {
    report["published_versions"]
        .as_array()
        .expect("published_versions array")
        .iter()
        .find(|version| version["resource_kind"] == kind)
        .unwrap_or_else(|| panic!("apply report omitted {kind}"))
}

fn exact_version(version: &Value) -> Value {
    json!({
        "revision_id": version["resource_version_id"],
        "resource_kind": version["resource_kind"],
        "semantic_digest": version["content_digest"],
    })
}

#[test]
fn public_cli_deterministic_first_run() {
    let (Ok(project), Ok(insight)) = (env::var(PROJECT_ENV), env::var(INSIGHT_BIN_ENV)) else {
        eprintln!("{PROJECT_ENV} or {INSIGHT_BIN_ENV} is unset; productization P2 journey skipped");
        return;
    };
    let project = Path::new(&project);
    let insight = Path::new(&insight);
    assert!(project.join(".insight/project.json").is_file());
    assert!(insight.is_file());

    let fixture = TempDir::new().expect("fixture directory");
    let schema_document = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "message": {
                "type": "string",
                "minLength": 0,
                "maxLength": 1024,
                "x-platform-max-bytes": 4096
            }
        },
        "required": ["message"],
        "additionalProperties": false,
    });
    let schema_digest = canonical_digest(&schema_document);
    let closed_schema = json!({
        "schema_version": 1,
        "profile": "insight.closed-json-schema/1",
        "schema": schema_document,
        "canonical_digest": schema_digest,
    });
    let plan = json!({
        "plan_version": 5,
        "interface_contract_digest": CONTRACT_DIGEST,
        "entry_node_id": "start",
        "dependency_slots": {},
        "nodes": {
            "start": {"kind": "start", "next": "finish"},
            "finish": {
                "kind": "return",
                "value": {"source": "run_input", "schema_digest": schema_digest},
            },
        },
    });
    let plan_path = write_canonical(fixture.path(), "typed-plan.json", &plan);
    let authoring_path = write_canonical(
        fixture.path(),
        "agent-authoring.json",
        &json!({"schema_version": 1, "kind": "deterministic-echo-agent"}),
    );
    let qualification_path = write_canonical(
        fixture.path(),
        "qualification.json",
        &json!({"schema_version": 1, "kind": "local-development-qualification"}),
    );

    let plan_upload = upload_artifact(
        insight,
        project,
        &plan_path,
        "typed_plan",
        "typed-plan.json",
    );
    assert_eq!(plan_upload["content_digest"], canonical_digest(&plan));
    let operation_id = plan_upload["operation_id"]
        .as_str()
        .expect("Artifact upload Operation ID");
    let operation = run_json(
        insight,
        &[
            "operation",
            "wait",
            operation_id,
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(operation["operation_id"], operation_id);
    assert_eq!(operation["state"], "succeeded");

    let plan_artifact_id = plan_upload["artifact_id"]
        .as_str()
        .expect("Plan Artifact ID");
    let artifact = run_json(
        insight,
        &[
            "artifact",
            "get",
            plan_artifact_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(artifact["artifact_id"], plan_artifact_id);
    assert_eq!(artifact["state"], "ready");
    assert_eq!(
        artifact["content"]["content_digest"],
        canonical_digest(&plan)
    );

    let downloaded_plan_path = fixture.path().join("downloaded-plan.json");
    let download = run_json(
        insight,
        &[
            "artifact",
            "read",
            plan_artifact_id,
            "--output",
            downloaded_plan_path.to_str().unwrap(),
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(download["artifact_id"], plan_artifact_id);
    assert_eq!(download["content_digest"], canonical_digest(&plan));
    assert_eq!(
        fs::read(&downloaded_plan_path).expect("downloaded Artifact is readable"),
        canonical_bytes(&plan)
    );
    let failure = run_failure(
        insight,
        &[
            "artifact",
            "read",
            plan_artifact_id,
            "--output",
            downloaded_plan_path.to_str().unwrap(),
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert!(
        failure.contains("already exists"),
        "no-clobber failure was not explicit: {failure}"
    );
    assert_eq!(
        fs::read(&downloaded_plan_path).expect("existing download remains readable"),
        canonical_bytes(&plan)
    );
    let authoring_upload = upload_artifact(
        insight,
        project,
        &authoring_path,
        "authoring_document",
        "agent-authoring.json",
    );
    let qualification_upload = upload_artifact(
        insight,
        project,
        &qualification_path,
        "diagnostic",
        "qualification.json",
    );
    let authoring_ref = artifact_ref(&authoring_upload, "agent-authoring.json");
    let qualification_ref = artifact_ref(&qualification_upload, "qualification.json");

    let scheduling = json!({
        "version": 1,
        "weight": 1,
        "burst": 2,
        "aging_rounds": 2,
    });
    let policy_contract_digest = canonical_digest(&json!({
        "kind": "local_execution_profile",
        "scheduling": scheduling,
    }));
    let applicability_digest = canonical_digest(&json!({"environment": "local"}));
    let policy_manifest = json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": "policies",
        "create": {
            "display_name": "Deterministic local execution profile",
            "document": {
                "resource_kind": "policy",
                "spec": {
                    "authoring_package": {
                        "artifact": authoring_ref,
                        "manifest_digest": authoring_upload["content_digest"],
                    },
                    "contract_digest": policy_contract_digest,
                    "dependency_versions": [],
                    "policy_versions": [],
                    "policy_kind": "scheduling",
                    "rules_digest": canonical_digest(&scheduling),
                    "selection": null,
                    "scheduling": scheduling,
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
                    "sandbox_secret_resolution": null,
                },
            },
        },
        "publish": {
            "kind": "single",
            "revision_no": 1,
            "content_digest": policy_contract_digest,
            "artifact_id": null,
        },
        "deployment": {
            "environment": "local",
            "closure": {
                "resource_kind": "policy",
                "bindings": {
                    "applicability_digest": applicability_digest,
                    "qualification_evidence": qualification_ref,
                },
            },
        },
    });
    let policy_manifest_path =
        write_canonical(fixture.path(), "policy.apply.json", &policy_manifest);
    let policy_report = run_json(
        insight,
        &[
            "apply",
            "--file",
            policy_manifest_path.to_str().unwrap(),
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let policy_version = published_version(&policy_report, "policy_revision");
    let policy_revision = exact_version(policy_version);
    let policy_closure = json!({
        "policy_revision": policy_revision,
        "applicability_digest": applicability_digest,
        "qualification_evidence": qualification_ref,
    });
    let policy_binding = json!({
        "deployment": {
            "deployment_id": policy_report["deployment_id"],
            "resource_kind": "policy_deployment",
            "deployment_digest": canonical_digest(&json!({
                "schema_version": 1,
                "resource_kind": "policy",
                "bindings": policy_closure,
            })),
        },
        "revision": policy_revision,
    });

    let agent_manifest = json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": "agents",
        "create": {
            "display_name": "Deterministic echo agent",
            "document": {
                "resource_kind": "agent",
                "spec": {
                    "authoring_package": {
                        "artifact": authoring_ref,
                        "manifest_digest": authoring_upload["content_digest"],
                    },
                    "contract_digest": CONTRACT_DIGEST,
                    "dependency_versions": [],
                    "policy_versions": [policy_revision],
                    "input_schema": closed_schema,
                    "output_schema": closed_schema,
                    "error_schema": closed_schema,
                    "typed_plan_artifact_id": plan_upload["artifact_id"],
                    "typed_plan_digest": plan_upload["content_digest"],
                },
            },
        },
        "publish": {
            "kind": "agent",
            "revision_no": 1,
            "interface_content_digest": CONTRACT_DIGEST,
            "plan_content_digest": plan_upload["content_digest"],
            "artifact_id": plan_upload["artifact_id"],
        },
        "deployment": {
            "environment": "local",
            "closure": {
                "resource_kind": "agent",
                "bindings": {
                    "entry_node_id": "start",
                    "entry_node_kind": "start",
                    "slots": [],
                    "policies": [policy_binding],
                    "execution_profile": policy_binding,
                },
            },
        },
    });
    let agent_manifest_path = write_canonical(fixture.path(), "agent.apply.json", &agent_manifest);
    let agent_report = run_json(
        insight,
        &[
            "apply",
            "--file",
            agent_manifest_path.to_str().unwrap(),
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(
        published_version(&agent_report, "agent_plan_revision")["content_digest"],
        plan_upload["content_digest"]
    );

    let run_request = json!({
        "agent_id": agent_report["resource_id"],
        "input": {
            "classification": "internal",
            "schema_digest": schema_digest,
            "value": {"kind": "inline", "value": {"message": "hello"}},
        },
        "deadline": (Utc::now() + Duration::minutes(5))
            .to_rfc3339_opts(SecondsFormat::Micros, true),
    });
    let run_request_path = write_canonical(fixture.path(), "run.json", &run_request);
    let run = run_json(
        insight,
        &[
            "run",
            "create",
            "--file",
            run_request_path.to_str().unwrap(),
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let run_id = run["run_id"].as_str().expect("Run ID");
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
    let terminal = watched.last().expect("terminal watch record");
    assert_eq!(terminal["kind"], "terminal");
    assert_eq!(terminal["run"]["state"], "succeeded");
    let result = run_json(
        insight,
        &["run", "result", run_id, "--path", project.to_str().unwrap()],
    );
    assert_eq!(
        result["value"],
        json!({"kind": "inline", "value": {"message": "hello"}})
    );
    assert_eq!(result["schema_digest"], schema_digest);

    stop_orchestration_worker(project);
    let restart_request = json!({
        "agent_id": agent_report["resource_id"],
        "input": {
            "classification": "internal",
            "schema_digest": schema_digest,
            "value": {"kind": "inline", "value": {"message": "after restart"}},
        },
        "deadline": (Utc::now() + Duration::minutes(5))
            .to_rfc3339_opts(SecondsFormat::Micros, true),
    });
    let restart_request_path =
        write_canonical(fixture.path(), "restart-run.json", &restart_request);
    let restart_run = run_json(
        insight,
        &[
            "run",
            "create",
            "--file",
            restart_request_path.to_str().unwrap(),
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let restart_run_id = restart_run["run_id"].as_str().expect("restart Run ID");
    let queued = run_json(
        insight,
        &[
            "run",
            "get",
            restart_run_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(queued["state"], "queued");

    let _replacement = restart_orchestration_worker(insight, project);
    let watched = run_json_lines(
        insight,
        &[
            "run",
            "watch",
            restart_run_id,
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let terminal = watched.last().expect("restart terminal watch record");
    assert_eq!(terminal["kind"], "terminal");
    assert_eq!(terminal["run"]["state"], "succeeded");
    let result = run_json(
        insight,
        &[
            "run",
            "result",
            restart_run_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(
        result["value"],
        json!({"kind": "inline", "value": {"message": "after restart"}})
    );
    assert_eq!(result["schema_digest"], schema_digest);

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
    let human_task_plan_path =
        write_canonical(fixture.path(), "human-task-plan.json", &human_task_plan);
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
    let human_task_manifest_path = write_canonical(
        fixture.path(),
        "human-task-agent.apply.json",
        &human_task_manifest,
    );
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
        write_canonical(fixture.path(), "human-task-run.json", &human_task_request);
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
    let human_task_run_id = human_task_run["run_id"]
        .as_str()
        .expect("Human Task Run ID");
    let mut watcher = Command::new(insight)
        .args([
            "run",
            "watch",
            human_task_run_id,
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
        &[
            "run",
            "get",
            human_task_run_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(waiting_run["state"], "waiting");
    let pending_task = run_json(
        insight,
        &["task", "get", &task_id, "--path", project.to_str().unwrap()],
    );
    assert_eq!(pending_task["task_kind"], "interaction_form");
    assert_eq!(pending_task["state"], "pending");
    assert_eq!(pending_task["owner"]["run_id"], human_task_run_id);
    assert_eq!(pending_task["response_schema_digest"], schema_digest);

    let task_response = json!({
        "classification": "internal",
        "schema_digest": schema_digest,
        "value": {"kind": "inline", "value": {"message": "after task"}},
    });
    let task_response_path =
        write_canonical(fixture.path(), "human-task-response.json", &task_response);
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
            human_task_run_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(
        human_task_result["value"],
        json!({"kind": "inline", "value": {"message": "after task"}})
    );
    assert_eq!(human_task_result["schema_digest"], schema_digest);
}
