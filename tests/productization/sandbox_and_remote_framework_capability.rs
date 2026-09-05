use super::*;

const SANDBOX_EVIDENCE_ENV: &str = "PLATFORM_PRODUCTIZATION_SANDBOX_EVIDENCE";
const SANDBOX_ENVIRONMENT_ENV: &str = "PLATFORM_PRODUCTIZATION_SANDBOX_ENVIRONMENT";

pub(super) struct SandboxFrameworkEvidence {
    pub run_id: String,
    pub console_passed: bool,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
    runtime_contract_digest: String,
    package_image: String,
    opensandbox_evidence_digest: String,
}

impl SandboxFrameworkEvidence {
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
                "the real headless Chromium Console read the exact successful remote LangGraph Capability Run and its bounded typed result",
            )
        } else {
            check(
                "console",
                "not_run",
                "set PLATFORM_PRODUCTIZATION_CONSOLE_BROWSER=true to read the exact sandbox/framework Run",
            )
        };
        json!({
            "schema_version": 1,
            "report_kind": "insight.productization.scenario-report/v1",
            "scenario_id": "sandbox-and-remote-framework-capability",
            "contract_profile": "insight.platform/v1",
            "profile": "starter+remote-capability,sandbox",
            "qualification_run_id": qualification_run_id(),
            "actual_profile": actual_productization_profile(),
            "profile_digest": productization_profile_digest(),
            "evidence_inputs": {"opensandbox_qualification": self.opensandbox_evidence_digest},
            "automation_layer": "P3",
            "source_revision": revision,
            "environment": {"os": env::consts::OS, "architecture": env::consts::ARCH, "fresh_profile": true},
            "started_at": self.started_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "finished_at": self.finished_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "status": if self.console_passed { "passed" } else { "incomplete" },
            "entrypoints": [
                check("cli", "passed", "public apply/run/watch/result commands completed for the exact remote framework Capability after same-revision OpenSandbox qualification"),
                check("http_fixture", "passed", "the bounded LangGraph.js reference service was reached only through the exact Egress Deployment"),
                console,
            ],
            "assertions": [
                check("sandbox_limit", "passed", &format!("real OpenSandbox Kubernetes L3 enforced the exact runtime contract {} for package {}", self.runtime_contract_digest, self.package_image)),
                check("remote_framework_result", "passed", "the exact LangGraph.js StateGraph returned a bounded typed Inline result through the public Capability path"),
                check("no_platform_database_access", "passed", "the isolated Sandbox package boundary passed and the remote framework process received no database or platform-internal credential environment"),
            ],
            "failure_probes": [
                check("sandbox_limit_rejection", "passed", "the real OpenSandbox deadline phase committed the bounded timeout terminal and cleaned the physical candidate"),
                check("framework_network_rejection", "passed", "removing the exact Egress endpoint catalog rejected the framework call before fixture dispatch"),
            ],
        })
    }
}

struct QualifiedSandboxEvidence {
    runtime_contract_digest: String,
    package_image: String,
    raw_digest: String,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
}

fn exact_source_revision() -> String {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(workspace_root())
        .output()
        .expect("Git revision is readable");
    assert!(output.status.success(), "Git revision lookup succeeds");
    let revision = String::from_utf8(output.stdout)
        .expect("Git revision is UTF-8")
        .trim()
        .to_owned();
    assert_eq!(revision.len(), 40, "Git revision is exact");
    revision
}

fn exact_oci_subject(value: &str) -> bool {
    if value.is_empty() || value.len() > 255 || value.contains('@') {
        return false;
    }
    let Some((registry, path)) = value.split_once('/') else {
        return false;
    };
    let registry_valid = !registry.is_empty()
        && registry.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-' | b':')
        });
    let path_valid = !path.is_empty()
        && path.split('/').all(|part| {
            !part.is_empty()
                && part.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'.' | b'_' | b'-')
                })
        });
    registry_valid && path_valid
}

fn exact_oci_digest_reference(value: &str) -> bool {
    let Some((subject, digest)) = value.rsplit_once('@') else {
        return false;
    };
    !subject.is_empty()
        && subject.len() <= 255
        && subject.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b'-' | b'/' | b':')
        })
        && exact_evidence_digest(digest)
}

fn exact_oci_repository(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && !value.contains('@')
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b'-' | b'/' | b':')
        })
}

fn validate_release_candidate(value: &Value) {
    if value.is_null() {
        return;
    }
    let candidate = value
        .as_object()
        .expect("release_candidate is null or a closed object");
    let expected = [
        "release_bundle_digest",
        "runtime",
        "sandbox_runner",
        "console",
    ];
    assert_eq!(
        candidate.len(),
        expected.len(),
        "release_candidate is closed"
    );
    assert!(expected.iter().all(|field| candidate.contains_key(*field)));
    assert!(
        candidate["release_bundle_digest"]
            .as_str()
            .is_some_and(exact_evidence_digest),
        "release candidate bundle digest is exact"
    );
    let mut repository_root: Option<&str> = None;
    for (name, suffix) in [
        ("runtime", "/platform-runtime"),
        ("sandbox_runner", "/platform-sandbox-runner"),
        ("console", "/platform-console"),
    ] {
        let component = candidate[name]
            .as_object()
            .unwrap_or_else(|| panic!("release_candidate.{name} is an object"));
        let fields = ["subject", "index_digest", "platform", "platform_digest"];
        assert_eq!(
            component.len(),
            fields.len(),
            "release_candidate.{name} is closed"
        );
        assert!(fields.iter().all(|field| component.contains_key(*field)));
        let subject = component["subject"]
            .as_str()
            .unwrap_or_else(|| panic!("release_candidate.{name}.subject is a string"));
        assert!(
            exact_oci_subject(subject) && subject.ends_with(suffix),
            "release_candidate.{name}.subject is the expected untagged OCI subject"
        );
        let root = subject
            .strip_suffix(suffix)
            .expect("validated candidate component suffix");
        if let Some(expected_root) = repository_root {
            assert_eq!(
                root, expected_root,
                "candidate components share one repository root"
            );
        } else {
            repository_root = Some(root);
        }
        assert!(
            component["index_digest"]
                .as_str()
                .is_some_and(exact_evidence_digest),
            "release_candidate.{name}.index_digest is exact"
        );
        assert_eq!(component["platform"], "linux/amd64");
        assert!(
            component["platform_digest"]
                .as_str()
                .is_some_and(exact_evidence_digest),
            "release_candidate.{name}.platform_digest is exact"
        );
    }
}

fn validate_bootstrap_environment(evidence: &Value) {
    let path = env::var(SANDBOX_ENVIRONMENT_ENV).unwrap_or_else(|_| {
        panic!("{SANDBOX_ENVIRONMENT_ENV} is required for the sandbox scenario")
    });
    let path = Path::new(&path);
    let metadata =
        fs::symlink_metadata(path).expect("OpenSandbox bootstrap environment metadata is readable");
    assert!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "OpenSandbox bootstrap environment must be a regular, non-symlink file"
    );
    let bytes = fs::read(path).expect("OpenSandbox bootstrap environment is readable");
    assert_eq!(
        evidence["bootstrap_environment_digest"],
        digest_bytes(&bytes),
        "OpenSandbox evidence binds the exact bootstrap environment bytes"
    );
    let environment: Value =
        serde_json::from_slice(&bytes).expect("OpenSandbox bootstrap environment is closed JSON");
    let object = environment
        .as_object()
        .expect("OpenSandbox bootstrap environment is an object");
    let expected = [
        "schema_version",
        "kind",
        "production",
        "git_commit",
        "platform_image_digest",
        "platform_image_repository",
        "platform_image_identity",
        "sandbox_runner_image_digest",
        "sandbox_runner_image_repository",
        "sandbox_runner_image_identity",
        "deployment_config_digest",
        "generated_at",
        "cluster_name",
        "kubeconfig",
    ];
    assert_eq!(
        object.len(),
        expected.len(),
        "bootstrap environment is closed"
    );
    assert!(expected.iter().all(|field| object.contains_key(*field)));
    assert_eq!(environment["schema_version"], 2);
    assert_eq!(
        environment["kind"],
        "insight.platform/kind-local-mechanics/v2"
    );
    assert_eq!(environment["production"], false);
    assert_eq!(environment["git_commit"], exact_source_revision());
    for field in [
        "platform_image_digest",
        "sandbox_runner_image_digest",
        "deployment_config_digest",
    ] {
        assert!(
            environment[field]
                .as_str()
                .is_some_and(exact_evidence_digest),
            "bootstrap environment {field} is exact"
        );
    }
    let identity_fields = [
        "kind",
        "repository",
        "reference",
        "config_digest",
        "index_digest",
        "platform",
        "platform_digest",
    ];
    for (label, repository_field, digest_field, identity_field, candidate_field) in [
        (
            "platform",
            "platform_image_repository",
            "platform_image_digest",
            "platform_image_identity",
            "runtime",
        ),
        (
            "Sandbox runner",
            "sandbox_runner_image_repository",
            "sandbox_runner_image_digest",
            "sandbox_runner_image_identity",
            "sandbox_runner",
        ),
    ] {
        let repository = environment[repository_field]
            .as_str()
            .unwrap_or_else(|| panic!("bootstrap {label} image repository is a string"));
        assert!(
            exact_oci_repository(repository),
            "bootstrap {label} image repository is valid"
        );
        let identity = environment[identity_field]
            .as_object()
            .unwrap_or_else(|| panic!("bootstrap {label} image identity is an object"));
        assert_eq!(
            identity.len(),
            identity_fields.len(),
            "bootstrap {label} image identity is closed"
        );
        assert!(identity_fields
            .iter()
            .all(|field| identity.contains_key(*field)));
        assert_eq!(identity["repository"], repository);
        assert!(identity["config_digest"]
            .as_str()
            .is_some_and(exact_evidence_digest));
        assert!(matches!(
            identity["platform"].as_str(),
            Some("linux/amd64" | "linux/arm64")
        ));
        assert_eq!(identity["platform_digest"], environment[digest_field]);
        assert!(identity["platform_digest"]
            .as_str()
            .is_some_and(exact_evidence_digest));
        assert_eq!(
            identity["reference"],
            format!(
                "{repository}@{}",
                identity["platform_digest"].as_str().unwrap()
            )
        );
        match identity["kind"].as_str() {
            Some("source_oci_manifest") => {
                assert!(evidence["release_candidate"].is_null());
                assert!(identity["index_digest"].is_null());
            }
            Some("signed_release_candidate") => {
                let candidate = evidence["release_candidate"]
                    .as_object()
                    .expect("signed bootstrap image has release candidate evidence");
                assert!(identity["index_digest"]
                    .as_str()
                    .is_some_and(exact_evidence_digest));
                let component = candidate[candidate_field]
                    .as_object()
                    .unwrap_or_else(|| panic!("release candidate {label} identity is an object"));
                for (candidate_field, identity_field) in [
                    ("subject", "repository"),
                    ("index_digest", "index_digest"),
                    ("platform", "platform"),
                    ("platform_digest", "platform_digest"),
                ] {
                    assert_eq!(component[candidate_field], identity[identity_field]);
                }
            }
            _ => panic!("bootstrap {label} image identity kind is invalid"),
        }
    }
    assert_eq!(
        environment["platform_image_identity"]["kind"],
        environment["sandbox_runner_image_identity"]["kind"],
        "bootstrap runtime and Sandbox runner identity classes agree"
    );
    assert_eq!(
        environment["platform_image_identity"]["platform"],
        environment["sandbox_runner_image_identity"]["platform"],
        "bootstrap runtime and Sandbox runner platforms agree"
    );
    assert_eq!(
        environment["platform_image_digest"], evidence["platform_image_digest"],
        "bootstrap and OpenSandbox evidence bind the same platform image"
    );
    assert_eq!(
        environment["cluster_name"], evidence["environment"]["cluster_name"],
        "bootstrap and OpenSandbox evidence bind the same cluster"
    );
    let generated_at = environment["generated_at"]
        .as_str()
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .expect("bootstrap generated_at is RFC3339")
        .with_timezone(&Utc);
    let raw_started_at = chrono::DateTime::parse_from_rfc3339(
        evidence["started_at"]
            .as_str()
            .expect("OpenSandbox started_at is a string"),
    )
    .expect("OpenSandbox started_at is RFC3339")
    .with_timezone(&Utc);
    let bootstrap_age = raw_started_at.signed_duration_since(generated_at);
    assert!(
        bootstrap_age >= Duration::zero() && bootstrap_age <= Duration::hours(3),
        "raw OpenSandbox qualification must start within three hours after bootstrap"
    );
    for field in ["cluster_name", "kubeconfig"] {
        assert!(environment[field]
            .as_str()
            .is_some_and(|value| !value.is_empty() && value.len() <= 4096));
    }
}

fn qualified_sandbox_evidence() -> QualifiedSandboxEvidence {
    let path = env::var(SANDBOX_EVIDENCE_ENV)
        .unwrap_or_else(|_| panic!("{SANDBOX_EVIDENCE_ENV} is required for the sandbox scenario"));
    let path = Path::new(&path);
    let metadata = fs::symlink_metadata(path).expect("OpenSandbox evidence metadata is readable");
    assert!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "OpenSandbox evidence must be a regular, non-symlink file"
    );
    let evidence_bytes = fs::read(path).expect("OpenSandbox evidence is readable");
    let evidence: Value =
        serde_json::from_slice(&evidence_bytes).expect("OpenSandbox evidence is closed JSON");
    assert_eq!(
        evidence_bytes,
        canonical_bytes(&evidence),
        "OpenSandbox evidence must use canonical JSON bytes"
    );
    let object = evidence
        .as_object()
        .expect("OpenSandbox evidence is an object");
    let expected_fields = [
        "bootstrap_environment_digest",
        "checks",
        "environment",
        "finished_at",
        "package_image",
        "platform_image_digest",
        "qualification_run_id",
        "qualifier",
        "release_candidate",
        "report_kind",
        "runtime_contract_digest",
        "sandbox_chart_digest",
        "schema_version",
        "source_revision",
        "started_at",
        "status",
    ];
    assert_eq!(object.len(), expected_fields.len());
    assert!(expected_fields
        .iter()
        .all(|field| object.contains_key(*field)));
    assert_eq!(evidence["schema_version"], 1);
    assert_eq!(
        evidence["report_kind"],
        "insight.productization.opensandbox-qualification/v1"
    );
    assert_eq!(evidence["status"], "passed");
    assert_eq!(evidence["environment"]["fresh_cluster"], true);
    let environment = evidence["environment"]
        .as_object()
        .expect("OpenSandbox evidence environment is closed");
    assert_eq!(environment.len(), 4);
    for field in ["os", "architecture", "fresh_cluster", "cluster_name"] {
        assert!(environment.contains_key(field));
    }
    for field in ["os", "architecture", "cluster_name"] {
        assert!(environment[field]
            .as_str()
            .is_some_and(|value| !value.is_empty() && value.len() <= 128));
    }
    assert_eq!(
        evidence["qualifier"],
        "scripts/qualify-platform-sandbox-l3.sh"
    );
    assert_eq!(
        evidence["source_revision"],
        exact_source_revision(),
        "OpenSandbox qualification must identify the same exact source revision"
    );
    let raw_qualification_run_id = evidence["qualification_run_id"]
        .as_str()
        .expect("OpenSandbox qualification run ID is a string");
    assert!(
        exact_evidence_digest(raw_qualification_run_id),
        "OpenSandbox qualification run ID is exact"
    );
    assert_eq!(
        raw_qualification_run_id,
        qualification_run_id(),
        "scenario reports must use the raw OpenSandbox qualification run ID"
    );
    let started_at = chrono::DateTime::parse_from_rfc3339(
        evidence["started_at"]
            .as_str()
            .expect("OpenSandbox started_at is a string"),
    )
    .expect("OpenSandbox started_at is RFC3339")
    .with_timezone(&Utc);
    let finished_at = chrono::DateTime::parse_from_rfc3339(
        evidence["finished_at"]
            .as_str()
            .expect("OpenSandbox finished_at is a string"),
    )
    .expect("OpenSandbox finished_at is RFC3339")
    .with_timezone(&Utc);
    assert!(
        finished_at >= started_at,
        "OpenSandbox qualification time window is ordered"
    );
    let expected_checks = [
        "opensandbox_lifecycle",
        "current_runtime_contract",
        "direct_and_disabled_network",
        "package_process_isolation",
        "deadline_limit_enforced",
        "dispatcher_recovery",
    ];
    let checks = evidence["checks"]
        .as_array()
        .expect("OpenSandbox evidence checks are an array");
    assert_eq!(checks.len(), expected_checks.len());
    for (check, expected) in checks.iter().zip(expected_checks) {
        let object = check
            .as_object()
            .expect("OpenSandbox evidence check is an object");
        assert_eq!(object.len(), 2, "OpenSandbox evidence check is closed");
        assert!(object.contains_key("id") && object.contains_key("status"));
        assert_eq!(check["id"], expected);
        assert_eq!(check["status"], "passed");
    }
    let runtime_contract_digest = evidence["runtime_contract_digest"]
        .as_str()
        .expect("OpenSandbox runtime contract digest")
        .to_owned();
    let package_image = evidence["package_image"]
        .as_str()
        .expect("OpenSandbox exact package image")
        .to_owned();
    assert!(
        exact_evidence_digest(&runtime_contract_digest),
        "OpenSandbox runtime contract digest is exact"
    );
    assert!(
        exact_oci_digest_reference(&package_image),
        "OpenSandbox package image is an exact OCI digest reference"
    );
    for field in [
        "bootstrap_environment_digest",
        "platform_image_digest",
        "sandbox_chart_digest",
    ] {
        let value = evidence[field]
            .as_str()
            .unwrap_or_else(|| panic!("{field} is an exact digest"));
        assert!(exact_evidence_digest(value), "{field} is an exact digest");
    }
    validate_release_candidate(&evidence["release_candidate"]);
    validate_bootstrap_environment(&evidence);
    QualifiedSandboxEvidence {
        runtime_contract_digest,
        package_image,
        raw_digest: digest_bytes(&evidence_bytes),
        started_at,
        finished_at,
    }
}

struct RemoteFrameworkEnvironment {
    broker: FixtureChild,
    fixture: FixtureChild,
    config_path: std::path::PathBuf,
    original_config: Vec<u8>,
    trace_path: std::path::PathBuf,
}

impl Drop for RemoteFrameworkEnvironment {
    fn drop(&mut self) {
        let _ = self.broker.terminate();
        let _ = self.fixture.terminate();
    }
}

fn start_framework_environment(
    _insight: &Path,
    project: &Path,
    fixture_directory: &Path,
    deployment: &Value,
    backend_contract_digest: &str,
    endpoint: &Value,
    policies: &std::collections::BTreeMap<&str, native_and_remote_capability::PolicyAuthority>,
) -> RemoteFrameworkEnvironment {
    let workspace = workspace_root();
    let runtime = project.join(".insight/runtime");
    let tls = runtime.join("tls");
    let authority_config_path = runtime.join("config/egress-broker.json");
    let config_path = fixture_directory.join("egress-broker-framework.json");
    let original_config = fs::read(&authority_config_path).expect("Egress config read");
    let mut config: Value = serde_json::from_slice(&original_config).expect("Egress config JSON");
    config["capability_http_endpoints"] = json!([{
        "schema_version": 1,
        "capability_deployment": deployment,
        "backend_contract_digest": backend_contract_digest,
        "effect": "pure",
        "idempotency_kind": "intrinsic",
        "method": "post",
        "endpoint": endpoint,
        "endpoint_identity_digest": canonical_digest(endpoint),
        "network_policy": policies["network"].revision,
        "tls_policy": policies["tls"].revision,
        "trust_policy": policies["trust"].revision,
        "secret_bindings": [],
        "credential_injections": [],
        "limits": {"maximum_request_bytes": 4096, "maximum_response_bytes": 4096, "maximum_diagnostic_bytes": 1024,
            "connect_timeout_milliseconds": 500, "first_byte_timeout_milliseconds": 1000, "idle_timeout_milliseconds": 1000, "total_timeout_milliseconds": 3000},
        "development_loopback": true,
        "trusted_root_pem": fs::read_to_string(tls.join("ca.pem")).expect("local CA")
    }]);
    let address = config["observability_listen_address"]
        .as_str()
        .expect("Egress observability address")
        .to_owned();
    if TcpStream::connect(&address).is_ok() {
        stop_role(project, "egress-broker");
    }
    let trace_path = runtime.join(format!(
        "logs/remote-fixture-framework-{}.jsonl",
        uuid::Uuid::now_v7()
    ));
    let node = env::var("PLATFORM_PRODUCTIZATION_NODE_BIN").unwrap_or_else(|_| "node".to_owned());
    let mut fixture = Command::new(node);
    fixture
        .arg(workspace.join("examples/productization/langgraph-reference/server.mjs"))
        .env_clear()
        .env(
            "INSIGHT_REMOTE_FIXTURE_PORT",
            endpoint["port"].as_u64().unwrap().to_string(),
        )
        .env(
            "INSIGHT_REMOTE_FIXTURE_CERT_PATH",
            tls.join("egress-broker.pem"),
        )
        .env(
            "INSIGHT_REMOTE_FIXTURE_KEY_PATH",
            tls.join("egress-broker-key.pem"),
        )
        .env("INSIGHT_REMOTE_FIXTURE_TRACE_PATH", &trace_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut fixture = FixtureChild::new(fixture.spawn().expect("remote framework fixture starts"));
    let ready = BufReader::new(fixture.child_mut().stdout.take().unwrap())
        .lines()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&ready).unwrap()["status"],
        "ready"
    );
    let mut broker = FixtureChild::new(native_and_remote_capability::spawn_egress(
        workspace,
        project,
        &config_path,
        &config,
    ));
    native_and_remote_capability::wait_egress(broker.child_mut(), &address);
    RemoteFrameworkEnvironment {
        broker,
        fixture,
        config_path,
        original_config,
        trace_path,
    }
}

fn publish_framework_agent(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    authoring_ref: &Value,
    deployment: &Value,
    policies: &std::collections::BTreeMap<&str, native_and_remote_capability::PolicyAuthority>,
    schema: &Value,
) -> String {
    let schema_digest = schema["canonical_digest"].as_str().unwrap();
    let contract_digest = canonical_digest(&json!({"agent": "remote-framework-capability"}));
    let requirement_digest = canonical_digest(&json!({"slot": "framework"}));
    let plan = json!({
        "plan_version": 5,
        "interface_contract_digest": contract_digest,
        "entry_node_id": "start",
        "dependency_slots": {"framework": {"kind": "capability", "requirement_digest": requirement_digest}},
        "nodes": {
            "start": {"kind": "start", "next": "framework"},
            "framework": {"kind": "capability_call", "capability_slot_id": "framework",
                "input": {"source": "run_input", "schema_digest": schema_digest}, "candidate_route": null,
                "output": {"source": "node_output", "producer_node_id": "framework", "port_id": "result", "schema_digest": schema_digest},
                "attempt_limit": 1, "retry_backoff_milliseconds": 100, "resume": "finish"},
            "finish": {"kind": "return", "value": {"source": "node_output", "producer_node_id": "framework", "port_id": "result", "schema_digest": schema_digest}}
        }
    });
    let plan_path = write_canonical(fixture, "remote-framework-agent-plan.json", &plan);
    let plan_upload = upload_artifact(
        insight,
        project,
        &plan_path,
        "typed_plan",
        "remote-framework-agent-plan.json",
    );
    let manifest = json!({
        "schema_version": 1, "kind": "insight.platform.apply/v1", "resource_noun": "agents",
        "create": {"display_name": "Remote framework Capability agent", "document": {"resource_kind": "agent", "spec": agent_resource_spec(
            "remote-framework-capability-agent", vec![], json!({
            "authoring_package": {"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]},
            "contract_digest": contract_digest, "dependency_versions": [],
            "policy_versions": policies.values().map(|policy| policy.revision.clone()).collect::<Vec<_>>(),
            "input_schema": schema, "output_schema": schema,
            "error_schema": native_and_remote_capability::value_schema("error"),
            "typed_plan_artifact_id": plan_upload["artifact_id"], "typed_plan_digest": plan_upload["content_digest"]
        }))}},
        "publish": {"kind": "agent", "revision_no": 1, "interface_content_digest": contract_digest,
            "plan_content_digest": plan_upload["content_digest"], "artifact_id": plan_upload["artifact_id"]},
        "deployment": {"environment": "local", "closure": {"resource_kind": "agent", "bindings": {
            "entry_node_id": "start", "entry_node_kind": "start",
            "slots": [{"slot_id": "framework", "requirement_digest": requirement_digest,
                "target": {"kind": "capability", "candidates": [deployment], "selection_policy": policies["selection"].binding}}],
            "policies": [], "execution_profile": policies["execution"].binding
        }}}
    });
    let path = write_canonical(fixture, "remote-framework-agent.apply.json", &manifest);
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
    )["resource_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn trace_records(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).expect("framework trace is closed JSON"))
        .collect()
}

pub(super) fn run(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    authoring_ref: &Value,
    qualification_ref: &Value,
) -> SandboxFrameworkEvidence {
    let sandbox = qualified_sandbox_evidence();
    let selection =
        json!({"schema_version": 1, "mode": "only_candidate", "route_schema_digest": null});
    let mut policies = std::collections::BTreeMap::new();
    for (key, role, kind, specialized) in [
        (
            "selection",
            "framework-selection",
            "selection",
            Some(("selection", selection)),
        ),
        ("execution", "framework-execution", "execution", None),
        ("network", "framework-network", "network", None),
        ("tls", "framework-tls", "tls", None),
        ("trust", "framework-trust", "trust", None),
    ] {
        policies.insert(
            key,
            native_and_remote_capability::apply_policy(
                insight,
                project,
                fixture,
                role,
                kind,
                specialized,
                authoring_ref,
                qualification_ref,
            ),
        );
    }
    let remote_config: Value = serde_json::from_slice(
        &fs::read(project.join(".insight/runtime/config/capability-remote.json")).unwrap(),
    )
    .unwrap();
    let http = &remote_config["installed_http_codecs"][0];
    let http_contract = json!({"kind": "http", "contract": {
        "method": "post", "protocol_contract_digest": http["protocol_contract_digest"],
        "request_mapping_digest": http["request_mapping_digest"], "response_mapping_digest": http["response_mapping_digest"],
        "error_mapping_digest": http["error_mapping_digest"], "idempotency_header": null
    }});
    let backend_contract_digest = canonical_digest(&http_contract);
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let endpoint =
        json!({"scheme": "https", "host": "localhost", "port": port, "base_path": "/v1/framework"});
    let http_binding = json!({"kind": "http", "binding": {
        "codec": {"schema_version": 1, "backend_kind": "http", "codec_id": http["codec_id"], "codec_version": http["codec_version"],
            "module_digest": http["module_digest"], "worker_protocol_version": http["worker_protocol_version"], "descriptor_digest": http["descriptor_digest"]},
        "worker_manifest_digest": canonical_digest(&remote_config["worker_manifest"]),
        "endpoint": endpoint, "endpoint_identity_digest": canonical_digest(&endpoint),
        "network_policy": policies["network"].revision, "tls_policy": policies["tls"].revision,
        "trust_policy": policies["trust"].revision
    }});
    let schema = native_and_remote_capability::value_schema("message");
    let remote_policies = vec![
        policies["network"].revision.clone(),
        policies["tls"].revision.clone(),
        policies["trust"].revision.clone(),
    ];
    let (framework_deployment, _) = native_and_remote_capability::publish_capability(
        insight,
        project,
        fixture,
        "remote_framework",
        "pure",
        "intrinsic",
        "http",
        http_contract,
        http_binding,
        &schema,
        &schema,
        authoring_ref,
        qualification_ref,
        remote_policies,
    );
    native_and_remote_capability::provision_capability_quotas(&[(
        &framework_deployment,
        "capability_remote",
    )]);
    let mut remote = start_framework_environment(
        insight,
        project,
        fixture,
        &framework_deployment,
        &backend_contract_digest,
        &endpoint,
        &policies,
    );
    let agent_id = publish_framework_agent(
        insight,
        project,
        fixture,
        authoring_ref,
        &framework_deployment,
        &policies,
        &schema,
    );
    let schema_digest = schema["canonical_digest"].as_str().unwrap();
    let run_id = native_and_remote_capability::create_run(
        insight,
        project,
        fixture,
        &agent_id,
        schema_digest,
        "framework-success",
        "bounded request",
    );
    assert_eq!(
        native_and_remote_capability::terminal(insight, project, &run_id)["state"],
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
    assert_eq!(result["schema_digest"], schema_digest);
    assert_eq!(
        result["value"]["value"]["message"],
        "langgraph: bounded request"
    );
    let requests = trace_records(&remote.trace_path);
    assert_eq!(
        requests
            .iter()
            .filter(|record| record["kind"] == "langgraph_capability_request")
            .count(),
        1
    );
    assert!(requests.iter().all(|record| {
        record["kind"] != "langgraph_capability_request"
            || (record["database_environment_present"] == false
                && record["framework"] == "langgraph-js"
                && record["framework_version"] == "1.4.13")
    }));

    let workspace = workspace_root();
    let address = serde_json::from_slice::<Value>(&remote.original_config).unwrap()
        ["observability_listen_address"]
        .as_str()
        .unwrap()
        .to_owned();
    remote
        .broker
        .terminate()
        .expect("replacement Egress terminates before the framework rejection probe");
    let mut deny_config: Value = serde_json::from_slice(&remote.original_config).unwrap();
    deny_config["capability_http_endpoints"] = json!([]);
    remote.broker = FixtureChild::new(native_and_remote_capability::spawn_egress(
        workspace,
        project,
        &remote.config_path,
        &deny_config,
    ));
    native_and_remote_capability::wait_egress(remote.broker.child_mut(), &address);
    let rejected_id = native_and_remote_capability::create_run(
        insight,
        project,
        fixture,
        &agent_id,
        schema_digest,
        "framework-rejected",
        "network rejection",
    );
    assert_eq!(
        native_and_remote_capability::terminal(insight, project, &rejected_id)["state"],
        "failed"
    );
    assert_eq!(
        trace_records(&remote.trace_path)
            .iter()
            .filter(|record| record["kind"] == "langgraph_capability_request")
            .count(),
        1,
        "Egress rejection must happen before remote framework dispatch"
    );
    drop(remote);
    let finished_at = Utc::now();
    assert!(
        finished_at >= sandbox.finished_at,
        "sandbox scenario report must cover the raw OpenSandbox qualification window"
    );
    SandboxFrameworkEvidence {
        run_id,
        console_passed: false,
        started_at: sandbox.started_at,
        finished_at,
        runtime_contract_digest: sandbox.runtime_contract_digest,
        package_image: sandbox.package_image,
        opensandbox_evidence_digest: sandbox.raw_digest,
    }
}
