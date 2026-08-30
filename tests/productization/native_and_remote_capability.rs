use super::*;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const DATABASE_URL: &str = "postgres://insight:insight@127.0.0.1:5432/insight_platform";

#[derive(Clone)]
pub(super) struct PolicyAuthority {
    pub revision: Value,
    pub binding: Value,
}

pub(super) struct CapabilityEvidence {
    pub run_id: String,
    pub console_passed: bool,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
}

impl CapabilityEvidence {
    pub(super) fn mark_console_passed(&mut self) {
        self.console_passed = true;
    }

    pub(super) fn report(&self, revision: &str) -> Value {
        let check = |id: &str, status: &str, evidence: &str| json!({"id": id, "status": status, "evidence": evidence});
        let console = if self.console_passed {
            check(
                "console",
                "passed",
                "the real headless Chromium Console read the exact successful native-to-remote Capability Run",
            )
        } else {
            check(
                "console",
                "not_run",
                "set PLATFORM_PRODUCTIZATION_CONSOLE_BROWSER=true to read the exact Capability Run",
            )
        };
        json!({
            "schema_version": 1,
            "report_kind": "insight.productization.scenario-report/v1",
            "scenario_id": "native-and-remote-capability",
            "contract_profile": "insight.platform/v1",
            "profile": "full",
            "automation_layer": "P3",
            "source_revision": revision,
            "environment": {"os": env::consts::OS, "architecture": env::consts::ARCH, "fresh_profile": true},
            "started_at": self.started_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "finished_at": self.finished_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "status": if self.console_passed { "passed" } else { "incomplete" },
            "entrypoints": [
                check("cli", "passed", "public apply/run/watch/result commands completed against the fresh full profile"),
                check("http_fixture", "passed", "the exact installed HTTP Capability closure reached the bounded local TLS fixture through Egress"),
                console,
            ],
            "assertions": [
                check("typed_result", "passed", "Native echo output became the exact typed input of the remote JSON Capability and the Agent returned the typed result"),
                check("exact_deployment", "passed", "both CapabilityCall nodes froze and executed their exact Interface Deployment closures"),
                check("remote_effect_key", "passed", "the durable remote Invocation retained non-empty exact effect and idempotency key digests"),
            ],
            "failure_probes": [
                check("remote_unknown_outcome", "passed", "a non-idempotent remote first-byte timeout failed closed without a fabricated result"),
                check("egress_rejection", "passed", "the same exact remote Deployment failed before dispatch after the installed endpoint catalog was removed"),
            ],
        })
    }
}

fn closed_schema(schema: Value) -> Value {
    json!({
        "schema_version": 1,
        "profile": "insight.closed-json-schema/1",
        "canonical_digest": canonical_digest(&schema),
        "schema": schema,
    })
}

pub(super) fn value_schema(field: &str) -> Value {
    closed_schema(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            field: {
                "description": "Bounded productization Capability value.",
                "type": "string",
                "minLength": 1,
                "maxLength": 256,
                "x-platform-max-bytes": 1024,
                "x-platform-classification": "internal"
            }
        },
        "required": [field],
        "additionalProperties": false
    }))
}

pub(super) fn apply_policy(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    role: &str,
    kind: &str,
    specialized: Option<(&str, Value)>,
    authoring_ref: &Value,
    qualification_ref: &Value,
) -> PolicyAuthority {
    let rules_digest = specialized.as_ref().map_or_else(
        || canonical_digest(&json!({"capability_policy_role": role})),
        |(_, value)| canonical_digest(value),
    );
    let contract_digest = canonical_digest(&json!({"capability_policy_role": role, "kind": kind}));
    let mut documents = serde_json::Map::from_iter([
        ("selection".to_owned(), Value::Null),
        ("scheduling".to_owned(), Value::Null),
        ("retention".to_owned(), Value::Null),
        ("model_safety".to_owned(), Value::Null),
        ("model_budget".to_owned(), Value::Null),
        ("model_public_projection".to_owned(), Value::Null),
        ("mcp_protocol".to_owned(), Value::Null),
        ("mcp_auth".to_owned(), Value::Null),
        ("sandbox_isolation".to_owned(), Value::Null),
        ("sandbox_resource".to_owned(), Value::Null),
        ("sandbox_network".to_owned(), Value::Null),
        ("sandbox_artifact_io".to_owned(), Value::Null),
        ("sandbox_secret_resolution".to_owned(), Value::Null),
    ]);
    if let Some((field, value)) = specialized {
        documents.insert(field.to_owned(), value);
    }
    let mut spec = serde_json::Map::from_iter([
        (
            "authoring_package".to_owned(),
            json!({"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]}),
        ),
        ("contract_digest".to_owned(), json!(contract_digest)),
        ("dependency_versions".to_owned(), json!([])),
        ("policy_versions".to_owned(), json!([])),
        ("policy_kind".to_owned(), json!(kind)),
        ("rules_digest".to_owned(), json!(rules_digest)),
    ]);
    spec.extend(documents);
    let applicability_digest =
        canonical_digest(&json!({"capability_policy_role": role, "environment": "local"}));
    let manifest = json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": "policies",
        "create": {"display_name": format!("Capability {role} policy"), "document": {"resource_kind": "policy", "spec": spec}},
        "publish": {"kind": "single", "revision_no": 1, "content_digest": rules_digest, "artifact_id": null},
        "deployment": {"environment": "local", "closure": {"resource_kind": "policy", "bindings": {
            "applicability_digest": applicability_digest,
            "qualification_evidence": qualification_ref
        }}}
    });
    let path = write_canonical(
        fixture,
        &format!("capability-{role}-policy.apply.json"),
        &manifest,
    );
    let report = run_json(
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
    let revision = exact_version(published_version(&report, "policy_revision"));
    let closure = json!({
        "policy_revision": revision,
        "applicability_digest": applicability_digest,
        "qualification_evidence": qualification_ref,
    });
    PolicyAuthority {
        revision: revision.clone(),
        binding: json!({
            "deployment": {
                "deployment_id": report["deployment_id"],
                "resource_kind": "policy_deployment",
                "deployment_digest": canonical_digest(&json!({"schema_version": 1, "resource_kind": "policy", "bindings": closure})),
            },
            "revision": revision,
        }),
    }
}

pub(super) fn raw_management_client(project: &Path) -> (Client, String, String) {
    let profile: Value = serde_json::from_slice(
        &fs::read(project.join(".insight/runtime/profile.json")).expect("runtime profile"),
    )
    .expect("runtime profile JSON");
    let port = profile["ports"]["gateway_management"]
        .as_u64()
        .and_then(|value| u16::try_from(value).ok())
        .expect("Management Gateway port");
    let token = fs::read_to_string(project.join(".insight/identity/developer-access-token.jwt"))
        .expect("developer token")
        .trim()
        .to_owned();
    let client = Client::builder()
        .redirect(Policy::none())
        .no_proxy()
        .timeout(StdDuration::from_secs(10))
        .build()
        .expect("Management client");
    (client, format!("http://127.0.0.1:{port}"), token)
}

fn publish_capability_interface(
    insight: &Path,
    project: &Path,
    name: &str,
    manifest: &Value,
) -> Value {
    let (client, base_url, token) = raw_management_client(project);
    let receipt = |step: &str| format!("capability-{name}-interface-{step}");
    let created = client
        .post(format!("{base_url}/v1/capabilities"))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .header("idempotency-key", receipt("create"))
        .json(&manifest["create"])
        .send()
        .expect("raw Capability Interface create");
    assert_eq!(created.status(), StatusCode::CREATED);
    let create_etag = created
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .expect("created Capability ETag")
        .to_owned();
    let created: Value = created.json().expect("created Capability JSON");
    let resource_id = created["resource_id"]
        .as_str()
        .expect("Capability Interface ID");
    let validation = client
        .post(format!(
            "{base_url}/v1/capabilities/{resource_id}/draft:validate"
        ))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .header("idempotency-key", receipt("validate"))
        .header("if-match", create_etag)
        .body(Vec::new())
        .send()
        .expect("raw Capability validation");
    assert_eq!(validation.status(), StatusCode::ACCEPTED);
    let validation: Value = validation.json().expect("Capability validation Operation");
    let operation_id = validation["operation_id"].as_str().unwrap();
    assert_eq!(
        run_json(
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
        )["state"],
        "succeeded"
    );
    let validated = client
        .get(format!("{base_url}/v1/capabilities/{resource_id}"))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .send()
        .expect("validated Capability read");
    assert_eq!(validated.status(), StatusCode::OK);
    let validated_etag = validated
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .unwrap()
        .to_owned();
    let published = client
        .post(format!(
            "{base_url}/v1/capabilities/{resource_id}/draft:publish"
        ))
        .bearer_auth(token)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .header("idempotency-key", receipt("publish"))
        .header("if-match", validated_etag)
        .json(&manifest["publish"])
        .send()
        .expect("raw Capability publish");
    assert_eq!(published.status(), StatusCode::OK);
    let publish_etag = published
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .unwrap()
        .to_owned();
    let published: Value = published.json().expect("published Capability JSON");
    let version = published["published_versions"]
        .as_array()
        .and_then(|versions| versions.first())
        .expect("Capability published Version");
    json!({
        "resource_id": resource_id,
        "validation_operation_id": operation_id,
        "published_versions": [{
            "resource_version_id": version["resource_version_id"],
            "resource_kind": "capability_interface_revision",
            "content_digest": version["content_digest"]
        }],
        "deployment_id": null,
        "active_deployment_id": null,
        "final_resource_etag": publish_etag
    })
}

pub(super) fn publish_capability(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    name: &str,
    effect: &str,
    idempotency: &str,
    backend_kind: &str,
    backend_contract: Value,
    backend_binding: Value,
    input_schema: &Value,
    output_schema: &Value,
    authoring_ref: &Value,
    qualification_ref: &Value,
    policy_versions: Vec<Value>,
) -> (Value, Value) {
    let interface_contract_digest = canonical_digest(&json!({"capability_interface": name}));
    let interface_manifest = json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": "capabilities",
        "create": {"display_name": format!("{name} Capability"), "document": {"resource_kind": "capability_interface", "spec": {
            "authoring_package": {"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]},
            "contract_digest": interface_contract_digest,
            "dependency_versions": [],
            "policy_versions": policy_versions,
            "qualified_name": format!("productization.{name}"),
            "input_schema": input_schema,
            "output_schema": output_schema,
            "error_schema": value_schema("error"),
            "artifacts": {"ports": []},
            "data_policy": {"maximum_input_classification": "internal", "maximum_output_classification": "internal", "allowed_regions": ["local"], "declassification_policy": null},
            "execution_limits": {"maximum_input_bytes": 4096, "maximum_output_bytes": 4096, "maximum_artifacts": 0, "maximum_execution_milliseconds": 10000},
            "effect": effect,
            "idempotency": idempotency,
            "cancellation": "best_effort",
            "progress": {"mode": "none", "schema_digest": null, "max_events": 0, "max_bytes_per_event": 0, "minimum_interval_milliseconds": 0, "durability": "none"}
        }}},
        "publish": {"kind": "single", "revision_no": 1, "content_digest": interface_contract_digest, "artifact_id": null},
        "deployment": null
    });
    let interface_report =
        publish_capability_interface(insight, project, name, &interface_manifest);
    let interface_revision = exact_version(published_version(
        &interface_report,
        "capability_interface_revision",
    ));

    let implementation_contract_digest =
        canonical_digest(&json!({"capability_implementation": name}));
    let backend_contract_digest = canonical_digest(&backend_contract);
    let implementation_manifest = json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": "capability-implementations",
        "create": {"display_name": format!("{name} Capability Implementation"), "document": {"resource_kind": "capability_implementation", "spec": {
            "authoring_package": {"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]},
            "contract_digest": implementation_contract_digest,
            "dependency_versions": [interface_revision],
            "policy_versions": [],
            "interface_revision": interface_revision,
            "backend_kind": backend_kind,
            "backend_contract": backend_contract,
            "backend_contract_digest": backend_contract_digest,
            "credential_requirements": [],
            "backend_limits": {"maximum_request_bytes": 4096, "maximum_response_bytes": 4096, "maximum_diagnostic_bytes": 1024,
                "connect_timeout_milliseconds": 500, "first_byte_timeout_milliseconds": 1000, "idle_timeout_milliseconds": 1000, "total_timeout_milliseconds": 3000},
            "features": {"deferred": backend_kind == "sandbox", "input_required": false, "callback": false, "poll": false, "progress": false,
                "cancellation": true, "max_remote_state_bytes": if backend_kind == "sandbox" { 4096 } else { 0 }, "max_poll_count": 0}
        }}},
        "publish": {"kind": "single", "revision_no": 1, "content_digest": implementation_contract_digest, "artifact_id": null},
        "deployment": null
    });
    let implementation_path = write_canonical(
        fixture,
        &format!("{name}-capability-implementation.apply.json"),
        &implementation_manifest,
    );
    let implementation_report = run_json(
        insight,
        &[
            "apply",
            "--file",
            implementation_path.to_str().unwrap(),
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let implementation_revision = exact_version(published_version(
        &implementation_report,
        "capability_implementation_revision",
    ));
    let bindings = json!({
        "implementation": implementation_revision,
        "interface": interface_revision,
        "backend": backend_binding,
        "secret_bindings": [],
        "policies": policy_versions,
        "conformance_evidence": qualification_ref,
    });
    let closure = json!({"resource_kind": "capability_interface", "bindings": bindings});
    let (client, base_url, token) = raw_management_client(project);
    let resource_id = interface_report["resource_id"]
        .as_str()
        .expect("Capability ID");
    let response = client
        .post(format!("{base_url}/v1/capabilities/{resource_id}/deployments"))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .header("idempotency-key", format!("capability-{name}-deploy"))
        .header(
            "if-match",
            interface_report["final_resource_etag"]
                .as_str()
                .expect("Capability ETag"),
        )
        .json(&json!({"resource_version_id": interface_revision["revision_id"], "environment": "local", "closure": closure}))
        .send()
        .expect("Capability Deployment create");
    assert_eq!(response.status(), StatusCode::CREATED);
    let deployment_view: Value = response.json().expect("Capability Deployment JSON");
    let resource = client
        .get(format!("{base_url}/v1/capabilities/{resource_id}"))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .send()
        .expect("Capability read");
    assert_eq!(resource.status(), StatusCode::OK);
    let etag = resource
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .expect("Capability deployment ETag")
        .to_owned();
    let deployment_id = deployment_view["deployment_id"]
        .as_str()
        .expect("Capability Deployment ID");
    let activated = client
        .post(format!(
            "{base_url}/v1/capabilities/{resource_id}/deployments/{deployment_id}:activate"
        ))
        .bearer_auth(token)
        .header("accept", "application/json")
        .header("idempotency-key", format!("capability-{name}-activate"))
        .header("if-match", etag)
        .body(Vec::new())
        .send()
        .expect("Capability activation");
    assert_eq!(activated.status(), StatusCode::OK);
    let deployment = json!({
        "deployment_id": deployment_view["deployment_id"],
        "resource_kind": "capability_deployment",
        "deployment_digest": canonical_digest(&json!({"schema_version": 1, "resource_kind": "capability_interface", "bindings": bindings})),
    });
    (deployment, interface_revision)
}

pub(super) fn provision_capability_quotas(deployments: &[(&Value, &str)]) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Capability quota runtime")
        .block_on(async {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(DATABASE_URL)
                .await
                .expect("Capability quota database");
            let tenant_id: String = sqlx::query_scalar(
                "SELECT tenant_id FROM insight_platform.tenants ORDER BY tenant_id LIMIT 1",
            )
            .fetch_one(&pool)
            .await
            .expect("fresh tenant");
            let payload = json!({"profile": "productization_full"});
            let payload_digest = canonical_digest(&payload);
            for work_class in ["capability_native", "capability_remote"] {
                sqlx::query(
                    r#"INSERT INTO insight_platform.quota_accounts
                       (tenant_id, quota_account_id, scope_kind, scope_id, work_class, metric, limit_value, payload_schema_version, payload, payload_digest)
                       VALUES ($1, $2, 'tenant', $1, $3, 'durable_quota.work_class_concurrent_operations', 8, 1, $4, $5)
                       ON CONFLICT (tenant_id, scope_kind, scope_id, work_class, metric) DO NOTHING"#,
                )
                .bind(&tenant_id)
                .bind(format!("qac_{}", Uuid::now_v7()))
                .bind(work_class)
                .bind(&payload)
                .bind(&payload_digest)
                .execute(&pool)
                .await
                .expect("Capability tenant quota");
            }
            for (deployment, work_class) in deployments {
                sqlx::query(
                    r#"INSERT INTO insight_platform.quota_accounts
                       (tenant_id, quota_account_id, scope_kind, scope_id, work_class, metric, limit_value, payload_schema_version, payload, payload_digest)
                       VALUES ($1, $2, 'capability_deployment', $3, $4, 'durable_quota.capability_concurrent_invocations', 32, 1, $5, $6)
                       ON CONFLICT (tenant_id, scope_kind, scope_id, work_class, metric) DO NOTHING"#,
                )
                .bind(&tenant_id)
                .bind(format!("qac_{}", Uuid::now_v7()))
                .bind(deployment["deployment_id"].as_str().unwrap())
                .bind(work_class)
                .bind(&payload)
                .bind(&payload_digest)
                .execute(&pool)
                .await
                .expect("Capability Deployment quota");
            }
        });
}

struct RemoteCapabilityEnvironment {
    broker: Child,
    fixture: Child,
    config_path: std::path::PathBuf,
    original_config: Vec<u8>,
}

impl Drop for RemoteCapabilityEnvironment {
    fn drop(&mut self) {
        let _ = self.broker.kill();
        let _ = self.broker.wait();
        let _ = self.fixture.kill();
        let _ = self.fixture.wait();
        fs::write(&self.config_path, &self.original_config).expect("Egress config restore");
    }
}

pub(super) fn spawn_egress(
    workspace: &Path,
    project: &Path,
    config_path: &Path,
    config: &Value,
) -> Child {
    let runtime = project.join(".insight/runtime");
    let tls = runtime.join("tls");
    let config_digest = canonical_digest(config);
    fs::write(config_path, canonical_bytes(config)).expect("Egress config write");
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(runtime.join("logs/egress-broker-capability-restart.log"))
        .expect("Egress log");
    let stderr = log.try_clone().expect("Egress log clone");
    Command::new(workspace.join("target/release/platform-egress-broker"))
        .env("PLATFORM_EGRESS_BROKER_CONFIG", config_path)
        .env("PLATFORM_EGRESS_BROKER_CONFIG_DIGEST", config_digest)
        .env(
            "PLATFORM_EGRESS_BROKER_AUTHORITY_CA_PATH",
            tls.join("ca.pem"),
        )
        .env(
            "PLATFORM_EGRESS_BROKER_AUTHORITY_CERT_PATH",
            tls.join("egress-broker-client.pem"),
        )
        .env(
            "PLATFORM_EGRESS_BROKER_AUTHORITY_KEY_PATH",
            tls.join("egress-broker-client-key.pem"),
        )
        .env("PLATFORM_EGRESS_BROKER_CLIENT_CA_PATH", tls.join("ca.pem"))
        .env(
            "PLATFORM_EGRESS_BROKER_CERT_PATH",
            tls.join("egress-broker.pem"),
        )
        .env(
            "PLATFORM_EGRESS_BROKER_KEY_PATH",
            tls.join("egress-broker-key.pem"),
        )
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("replacement Egress starts")
}

pub(super) fn wait_egress(child: &mut Child, address: &str) {
    let deadline = Instant::now() + StdDuration::from_secs(30);
    while TcpStream::connect(address).is_err() {
        if let Some(status) = child.try_wait().expect("Egress status") {
            panic!("replacement Egress exited before readiness: {status}");
        }
        assert!(Instant::now() < deadline, "replacement Egress readiness");
        thread::sleep(StdDuration::from_millis(50));
    }
}

fn start_remote_environment(
    insight: &Path,
    project: &Path,
    deployment: &Value,
    backend_contract_digest: &str,
    endpoint: &Value,
    policies: &std::collections::BTreeMap<&str, PolicyAuthority>,
) -> RemoteCapabilityEnvironment {
    let workspace = insight
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .unwrap();
    let runtime = project.join(".insight/runtime");
    let tls = runtime.join("tls");
    let config_path = runtime.join("config/egress-broker.json");
    let original_config = fs::read(&config_path).expect("Egress config read");
    let mut config: Value = serde_json::from_slice(&original_config).expect("Egress config JSON");
    config["capability_http_endpoints"] = json!([{
        "schema_version": 1,
        "capability_deployment": deployment,
        "backend_contract_digest": backend_contract_digest,
        "effect": "non_idempotent_write",
        "idempotency_kind": "none",
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
    let port = endpoint["port"].as_u64().unwrap().to_string();
    let node = env::var("PLATFORM_PRODUCTIZATION_NODE_BIN").unwrap_or_else(|_| "node".to_owned());
    let mut fixture = Command::new(node)
        .arg(workspace.join("examples/productization/remote-fixture.mjs"))
        .env("INSIGHT_REMOTE_FIXTURE_PORT", port)
        .env(
            "INSIGHT_REMOTE_FIXTURE_CERT_PATH",
            tls.join("egress-broker.pem"),
        )
        .env(
            "INSIGHT_REMOTE_FIXTURE_KEY_PATH",
            tls.join("egress-broker-key.pem"),
        )
        .env(
            "INSIGHT_REMOTE_FIXTURE_TRACE_PATH",
            runtime.join("logs/remote-fixture-capability.jsonl"),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("remote Capability fixture starts");
    let ready = BufReader::new(fixture.stdout.take().unwrap())
        .lines()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&ready).unwrap()["status"],
        "ready"
    );
    let mut broker = spawn_egress(workspace, project, &config_path, &config);
    wait_egress(&mut broker, &address);
    RemoteCapabilityEnvironment {
        broker,
        fixture,
        config_path,
        original_config,
    }
}

pub(super) fn create_run(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    agent_id: &str,
    schema_digest: &str,
    name: &str,
    message: &str,
) -> String {
    let request = json!({
        "agent_id": agent_id,
        "input": {"classification": "internal", "schema_digest": schema_digest, "value": {"kind": "inline", "value": {"message": message}}},
        "deadline": (Utc::now() + Duration::minutes(5)).to_rfc3339_opts(SecondsFormat::Micros, true),
    });
    let path = write_canonical(fixture, &format!("capability-{name}-run.json"), &request);
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
    )["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

pub(super) fn terminal(insight: &Path, project: &Path, run_id: &str) -> Value {
    run_json_lines(
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
    )
    .last()
    .expect("Capability terminal")["run"]
        .clone()
}

fn await_reconciliation(run_id: &str, deployment_id: &str) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(DATABASE_URL)
                .await
                .unwrap();
            for _ in 0..80 {
                let row: Option<(String, Option<String>, Option<String>, bool)> = sqlx::query_as(
                    "SELECT state, effect_key_digest, payload->'admission'->>'idempotency_key_digest', payload->'reconciliation' IS NOT NULL \
                     FROM insight_platform.invocations WHERE run_id = $1 AND deployment_id = $2",
                )
                .bind(run_id)
                .bind(deployment_id)
                .fetch_optional(&pool)
                .await
                .unwrap();
                if let Some((state, effect_key, idempotency_key, has_reconciliation)) = row {
                    if state == "reconciliation_required" {
                        assert!(has_reconciliation);
                        assert!(effect_key.is_some_and(|digest| digest.starts_with("sha256:")));
                        assert!(
                            idempotency_key.is_some_and(|digest| digest.starts_with("sha256:"))
                        );
                        return;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            panic!("remote Capability Invocation did not require reconciliation");
        });
}

pub(super) fn run(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    authoring_ref: &Value,
    qualification_ref: &Value,
) -> CapabilityEvidence {
    let started_at = Utc::now();
    let selection =
        json!({"schema_version": 1, "mode": "only_candidate", "route_schema_digest": null});
    let mut policies = std::collections::BTreeMap::new();
    for (role, kind, specialized) in [
        ("selection", "selection", Some(("selection", selection))),
        ("execution", "execution", None),
        ("network", "network", None),
        ("tls", "tls", None),
        ("trust", "trust", None),
    ] {
        policies.insert(
            role,
            apply_policy(
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
    let native_config: Value = serde_json::from_slice(
        &fs::read(project.join(".insight/runtime/config/capability-native.json")).unwrap(),
    )
    .unwrap();
    let remote_config: Value = serde_json::from_slice(
        &fs::read(project.join(".insight/runtime/config/capability-remote.json")).unwrap(),
    )
    .unwrap();
    let native_adapter = &native_config["installed_adapters"][0];
    let native_contract = json!({"kind": "native", "contract": {
        "adapter_id": native_adapter["adapter_id"], "adapter_version": native_adapter["adapter_version"],
        "module_digest": native_adapter["module_digest"], "entrypoint_id": native_adapter["entrypoint_id"], "worker_protocol_version": 1
    }});
    let native_binding = json!({"kind": "native", "binding": {
        "worker_manifest_digest": canonical_digest(&native_config["worker_manifest"]),
        "adapter_module_digest": native_adapter["module_digest"]
    }});
    let schema = value_schema("message");
    let policy_versions = vec![policies["execution"].revision.clone()];
    let (native_deployment, _) = publish_capability(
        insight,
        project,
        fixture,
        "native_echo",
        "pure",
        "intrinsic",
        "native",
        native_contract,
        native_binding,
        &schema,
        &schema,
        authoring_ref,
        qualification_ref,
        policy_versions,
    );

    let http = &remote_config["installed_http_codecs"][0];
    let http_contract = json!({"kind": "http", "contract": {
        "method": "post", "protocol_contract_digest": http["protocol_contract_digest"],
        "request_mapping_digest": http["request_mapping_digest"], "response_mapping_digest": http["response_mapping_digest"],
        "error_mapping_digest": http["error_mapping_digest"], "idempotency_header": null
    }});
    let http_contract_digest = canonical_digest(&http_contract);
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let endpoint = json!({"scheme": "https", "host": "localhost", "port": port, "base_path": "/v1/capability"});
    let http_binding = json!({"kind": "http", "binding": {
        "codec": {"schema_version": 1, "backend_kind": "http", "codec_id": http["codec_id"], "codec_version": http["codec_version"],
            "module_digest": http["module_digest"], "worker_protocol_version": http["worker_protocol_version"], "descriptor_digest": http["descriptor_digest"]},
        "worker_manifest_digest": canonical_digest(&remote_config["worker_manifest"]),
        "endpoint": endpoint, "endpoint_identity_digest": canonical_digest(&endpoint),
        "network_policy": policies["network"].revision, "tls_policy": policies["tls"].revision, "trust_policy": policies["trust"].revision
    }});
    let remote_policies = vec![
        policies["network"].revision.clone(),
        policies["tls"].revision.clone(),
        policies["trust"].revision.clone(),
    ];
    let (remote_deployment, _) = publish_capability(
        insight,
        project,
        fixture,
        "remote_http",
        "non_idempotent_write",
        "none",
        "http",
        http_contract,
        http_binding,
        &schema,
        &schema,
        authoring_ref,
        qualification_ref,
        remote_policies,
    );
    provision_capability_quotas(&[
        (&native_deployment, "capability_native"),
        (&remote_deployment, "capability_remote"),
    ]);

    let mut remote = start_remote_environment(
        insight,
        project,
        &remote_deployment,
        &http_contract_digest,
        &endpoint,
        &policies,
    );
    let schema_digest = schema["canonical_digest"].as_str().unwrap();
    let contract_digest = canonical_digest(&json!({"agent": "native-and-remote-capability"}));
    let native_requirement = canonical_digest(&json!({"slot": "native"}));
    let remote_requirement = canonical_digest(&json!({"slot": "remote"}));
    let plan = json!({
        "plan_version": 5, "interface_contract_digest": contract_digest, "entry_node_id": "start",
        "dependency_slots": {
            "native": {"kind": "capability", "requirement_digest": native_requirement},
            "remote": {"kind": "capability", "requirement_digest": remote_requirement}
        },
        "nodes": {
            "start": {"kind": "start", "next": "native"},
            "native": {"kind": "capability_call", "capability_slot_id": "native", "input": {"source": "run_input", "schema_digest": schema_digest},
                "candidate_route": null, "output": {"source": "node_output", "producer_node_id": "native", "port_id": "result", "schema_digest": schema_digest},
                "attempt_limit": 1, "retry_backoff_milliseconds": 100, "resume": "remote"},
            "remote": {"kind": "capability_call", "capability_slot_id": "remote", "input": {"source": "node_output", "producer_node_id": "native", "port_id": "result", "schema_digest": schema_digest},
                "candidate_route": null, "output": {"source": "node_output", "producer_node_id": "remote", "port_id": "result", "schema_digest": schema_digest},
                "attempt_limit": 1, "retry_backoff_milliseconds": 100, "resume": "finish"},
            "finish": {"kind": "return", "value": {"source": "node_output", "producer_node_id": "remote", "port_id": "result", "schema_digest": schema_digest}}
        }
    });
    let plan_path = write_canonical(fixture, "capability-agent-plan.json", &plan);
    let plan_upload = upload_artifact(
        insight,
        project,
        &plan_path,
        "typed_plan",
        "capability-agent-plan.json",
    );
    let agent_manifest = json!({
        "schema_version": 1, "kind": "insight.platform.apply/v1", "resource_noun": "agents",
        "create": {"display_name": "Native and remote Capability agent", "document": {"resource_kind": "agent", "spec": {
            "authoring_package": {"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]},
            "contract_digest": contract_digest, "dependency_versions": [], "policy_versions": policies.values().map(|p| p.revision.clone()).collect::<Vec<_>>(),
            "input_schema": schema, "output_schema": schema, "error_schema": value_schema("error"),
            "typed_plan_artifact_id": plan_upload["artifact_id"], "typed_plan_digest": plan_upload["content_digest"]
        }}},
        "publish": {"kind": "agent", "revision_no": 1, "interface_content_digest": contract_digest, "plan_content_digest": plan_upload["content_digest"], "artifact_id": plan_upload["artifact_id"]},
        "deployment": {"environment": "local", "closure": {"resource_kind": "agent", "bindings": {
            "entry_node_id": "start", "entry_node_kind": "start",
            "slots": [
                {"slot_id": "native", "requirement_digest": native_requirement, "target": {"kind": "capability", "candidates": [native_deployment], "selection_policy": policies["selection"].binding}},
                {"slot_id": "remote", "requirement_digest": remote_requirement, "target": {"kind": "capability", "candidates": [remote_deployment], "selection_policy": policies["selection"].binding}}
            ],
            "policies": [], "execution_profile": policies["execution"].binding
        }}}
    });
    let agent_path = write_canonical(fixture, "capability-agent.apply.json", &agent_manifest);
    let agent_report = run_json(
        insight,
        &[
            "apply",
            "--file",
            agent_path.to_str().unwrap(),
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let agent_id = agent_report["resource_id"].as_str().unwrap();

    let run_id = create_run(
        insight,
        project,
        fixture,
        agent_id,
        schema_digest,
        "success",
        "capability round trip",
    );
    assert_eq!(terminal(insight, project, &run_id)["state"], "succeeded");
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
    assert_eq!(result["value"]["value"]["message"], "capability round trip");

    let unknown_id = create_run(
        insight,
        project,
        fixture,
        agent_id,
        schema_digest,
        "unknown",
        "fixture-capability-timeout",
    );
    await_reconciliation(
        &unknown_id,
        remote_deployment["deployment_id"].as_str().unwrap(),
    );
    assert_ne!(
        run_json(
            insight,
            &[
                "run",
                "get",
                &unknown_id,
                "--path",
                project.to_str().unwrap(),
            ],
        )["state"],
        "succeeded"
    );

    let workspace = insight
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .unwrap();
    let address: String = serde_json::from_slice::<Value>(&remote.original_config).unwrap()
        ["observability_listen_address"]
        .as_str()
        .unwrap()
        .to_owned();
    let _ = remote.broker.kill();
    let _ = remote.broker.wait();
    let mut deny_config: Value = serde_json::from_slice(&remote.original_config).unwrap();
    deny_config["capability_http_endpoints"] = json!([]);
    remote.broker = spawn_egress(workspace, project, &remote.config_path, &deny_config);
    wait_egress(&mut remote.broker, &address);
    let rejected_id = create_run(
        insight,
        project,
        fixture,
        agent_id,
        schema_digest,
        "rejected",
        "egress rejection",
    );
    assert_eq!(terminal(insight, project, &rejected_id)["state"], "failed");

    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let pool = PgPoolOptions::new().max_connections(1).connect(DATABASE_URL).await.unwrap();
        let rows: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT deployment_id, effect_key_digest, payload->'admission'->>'idempotency_key_digest' FROM insight_platform.invocations WHERE run_id = $1 ORDER BY created_at",
        )
        .bind(&run_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, native_deployment["deployment_id"]);
        assert_eq!(rows[1].0, remote_deployment["deployment_id"]);
        assert!(rows[1].1.as_deref().is_some_and(|digest| digest.starts_with("sha256:")));
        assert!(rows[1].2.as_deref().is_some_and(|digest| digest.starts_with("sha256:")));
    });

    drop(remote);
    CapabilityEvidence {
        run_id,
        console_passed: false,
        started_at,
        finished_at: Utc::now(),
    }
}
