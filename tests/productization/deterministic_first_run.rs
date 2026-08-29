use chrono::{Duration, SecondsFormat, Utc};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{env, fmt::Write as _, fs, path::Path, process::Command};
use tempfile::TempDir;

const PROJECT_ENV: &str = "PLATFORM_PRODUCTIZATION_PROJECT";
const INSIGHT_BIN_ENV: &str = "PLATFORM_INSIGHT_BIN";
const CONTRACT_DIGEST: &str =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

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
}
