use super::*;
use insight_platform_contracts::{
    ExactDeploymentRef, ExactSecretBindingRef, McpAuthorizationPrincipalKind, PrincipalKind,
    ResourceId, ResourceKind, SecretBindingPayload, SecretPurpose, SecretResolutionPolicy,
};
use insight_platform_mcp_host::{McpAuthorizationBindingRecord, NewMcpAuthorizationBinding};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const DATABASE_URL: &str = "postgres://insight:insight@127.0.0.1:5432/insight_platform";

pub(super) struct McpEvidence {
    pub run_id: String,
    pub console_passed: bool,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
    _remote_worker: Child,
}

impl McpEvidence {
    pub(super) fn mark_console_passed(&mut self) {
        self.console_passed = true;
    }

    pub(super) fn report(&self, revision: &str) -> Value {
        let check = |id: &str, status: &str, evidence: &str| json!({"id": id, "status": status, "evidence": evidence});
        json!({
            "schema_version": 1,
            "report_kind": "insight.productization.scenario-report/v1",
            "scenario_id": "remote-mcp-tool-and-resource",
            "contract_profile": "insight.platform/v1",
            "profile": "full",
            "automation_layer": "P3",
            "source_revision": revision,
            "environment": {"os": env::consts::OS, "architecture": env::consts::ARCH, "fresh_profile": true},
            "started_at": self.started_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "finished_at": self.finished_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "status": if self.console_passed { "passed" } else { "incomplete" },
            "entrypoints": [
                check("cli", "passed", "public apply, discovery Operation, Run watch and result commands completed"),
                check("http_fixture", "passed", "MCP initialize, tools/list, resources/list and tools/call traversed Egress to the bounded TLS fixture"),
                if self.console_passed {
                    check("console", "passed", "real headless Chromium read the exact successful MCP Run")
                } else {
                    check("console", "not_run", "set PLATFORM_PRODUCTIZATION_CONSOLE_BROWSER=true to read the exact MCP Run")
                },
            ],
            "assertions": [
                check("discovery_snapshot", "passed", "public discovery committed one exact immutable snapshot"),
                check("bounded_projection", "passed", "the verified descriptor Artifact contains the exact bounded Tool and Resource and no authorization material"),
                check("mcp_result", "passed", "the MCP Capability returned the typed fixture Tool result through the Agent Run"),
            ],
            "failure_probes": [
                check("mcp_tls_rejection", "passed", "an untrusted root rejected the MCP endpoint before application dispatch"),
                check("mcp_remote_error", "passed", "a bounded JSON-RPC error failed the Run without fabricating a successful result"),
            ],
        })
    }
}

struct McpEnvironment {
    broker: Child,
    fixture: Child,
    config_path: std::path::PathBuf,
    original_config: Vec<u8>,
}

impl Drop for McpEnvironment {
    fn drop(&mut self) {
        let _ = self.broker.kill();
        let _ = self.broker.wait();
        let _ = self.fixture.kill();
        let _ = self.fixture.wait();
        fs::write(&self.config_path, &self.original_config).expect("Egress config restore");
    }
}

fn new_id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7()).expect("UUIDv7 resource ID")
}

fn endpoint(host: &str, port: u16, base_path: &str) -> Value {
    json!({"scheme": "https", "host": host, "port": port, "base_path": base_path})
}

fn oauth_endpoint(host: &str, base_path: &str) -> Value {
    let endpoint = endpoint(host, 443, base_path);
    json!({"endpoint": endpoint, "endpoint_identity_digest": canonical_digest(&endpoint)})
}

fn protocol_document() -> Value {
    let limits = json!({
        "maximum_request_bytes": 16384,
        "maximum_response_bytes": 65536,
        "maximum_metadata_entries": 32,
        "maximum_progress_events": 32,
        "maximum_pages": 16,
        "minimum_poll_milliseconds": 10,
        "maximum_poll_milliseconds": 5000
    });
    json!({
        "schema_version": 1,
        "offered_versions": ["2025-11-25"],
        "transport_features": {"streamable_http_get": true, "streamable_http_sse": true, "resumable_stream": true, "session_affinity": true},
        "client_capabilities": {"elicitation_form": false, "elicitation_url": false, "tasks_elicitation_create": false, "sampling": false, "roots": false},
        "allowed_server_capabilities": {"tools": true, "resources": true, "prompts": false, "logging": false, "tasks": false, "subscriptions": false},
        "experimental_features": [],
        "method_limits": {"tools_call": limits, "resources_list": limits, "resources_read": limits},
        "metadata_policy": {"maximum_server_name_bytes": 128, "maximum_server_version_bytes": 64, "maximum_instruction_bytes": 4096, "maximum_object_name_bytes": 128, "maximum_description_bytes": 8192, "maximum_icon_bytes": 1048576}
    })
}

fn auth_document(provider_id: &ResourceId, resource_endpoint: &Value) -> Value {
    json!({
        "schema_version": 1,
        "issuer": oauth_endpoint("auth.fixture.test", "/"),
        "authorization_endpoint": oauth_endpoint("auth.fixture.test", "/oauth/authorize"),
        "token_endpoint": oauth_endpoint("auth.fixture.test", "/oauth/token"),
        "client_id": "insight-productization",
        "client_authentication": "none",
        "client_credential_purpose": null,
        "pkce_secret_provider_id": provider_id,
        "token_secret_provider_id": provider_id,
        "redirect_uri": oauth_endpoint("platform.fixture.test", "/v1/mcp/oauth/callback"),
        "resource_indicator": {"endpoint": resource_endpoint, "endpoint_identity_digest": canonical_digest(resource_endpoint)},
        "allowed_scopes": ["resources.read", "tools.call"],
        "maximum_token_response_bytes": 65536,
        "connect_timeout_milliseconds": 5000,
        "total_timeout_milliseconds": 30000,
        "maximum_clock_skew_seconds": 60
    })
}

fn server_limits() -> Value {
    json!({
        "maximum_message_bytes": 65536,
        "maximum_response_bytes": 1048576,
        "maximum_headers": 32,
        "maximum_sse_event_bytes": 8192,
        "maximum_in_flight": 8,
        "maximum_connections": 4,
        "maximum_sessions": 4,
        "maximum_session_milliseconds": 3600000,
        "idle_timeout_milliseconds": 30000,
        "initialize_timeout_milliseconds": 30000,
        "request_timeout_milliseconds": 60000,
        "total_timeout_milliseconds": 120000
    })
}

fn publish_server(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    authoring_ref: &Value,
    qualification_ref: &Value,
    endpoint: &Value,
    policies: &std::collections::BTreeMap<&str, native_and_remote_capability::PolicyAuthority>,
) -> (Value, Value) {
    let contract_digest = canonical_digest(&json!({"productization": "remote-mcp-server"}));
    let bindings = json!({
        "server_identity_digest": canonical_digest(endpoint),
        "transport": {"kind": "streamable_http", "binding": {
            "endpoint": endpoint,
            "endpoint_identity_digest": canonical_digest(endpoint),
            "network_policy": policies["network"].revision,
            "tls_policy": policies["tls"].revision
        }},
        "protocol_policy": policies["protocol"].revision,
        "trust_policy": policies["trust"].revision,
        "auth_policy": policies["auth"].revision,
        "secret_bindings": [],
        "conformance_evidence": qualification_ref
    });
    let manifest = json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": "mcp-servers",
        "create": {"display_name": "Productization remote MCP server", "document": {"resource_kind": "mcp_server", "spec": {
            "authoring_package": {"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]},
            "contract_digest": contract_digest,
            "dependency_versions": [],
            "policy_versions": [policies["protocol"].revision, policies["auth"].revision],
            "transport": "streamable_http",
            "protocol_policy": policies["protocol"].revision,
            "deployment_credential_requirements": [],
            "authorization_credential_purpose": "mcp.oauth.token",
            "limits": server_limits()
        }}},
        "publish": {"kind": "single", "revision_no": 1, "content_digest": contract_digest, "artifact_id": null},
        "deployment": {"environment": "local", "closure": {"resource_kind": "mcp_server", "bindings": bindings}}
    });
    let path = write_canonical(fixture, "remote-mcp-server.apply.json", &manifest);
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
    let revision = exact_version(published_version(&report, "mcp_server_revision"));
    let mut exact_bindings = bindings;
    exact_bindings["server_revision"] = revision.clone();
    let deployment = json!({
        "deployment_id": report["deployment_id"],
        "resource_kind": "mcp_deployment",
        "deployment_digest": canonical_digest(&json!({"schema_version": 1, "resource_kind": "mcp_server", "bindings": exact_bindings}))
    });
    (report, deployment)
}

fn seed_authorization(
    deployment: &Value,
    audience_digest: &str,
    provider_id: &ResourceId,
) -> String {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let pool = PgPoolOptions::new().max_connections(1).connect(DATABASE_URL).await.unwrap();
        let (tenant, principal, principal_kind, generation): (String, String, String, i64) = sqlx::query_as(
            "SELECT tenant_id, principal_id, principal_kind, generation FROM insight_platform.tenant_principals WHERE state='active' AND principal_kind='agent_author' ORDER BY created_at LIMIT 1"
        ).fetch_one(&pool).await.unwrap();
        let tenant_id: ResourceId = tenant.parse().unwrap();
        let principal_id: ResourceId = principal.parse().unwrap();
        let principal_identity_kind: PrincipalKind = principal_kind.parse().unwrap();
        let secret_binding_id = new_id(ResourceKind::SecretBinding);
        let purpose: SecretPurpose = "mcp.oauth.token".parse().unwrap();
        let pinned_digest: insight_platform_contracts::Sha256Digest = canonical_digest(&json!({"productization": "mcp-token-version"})).parse().unwrap();
        let resolution_policy = SecretResolutionPolicy::Pinned { opaque_version_identity_digest: pinned_digest };
        let exact_secret = ExactSecretBindingRef::build(secret_binding_id.clone(), 1, provider_id.clone(), purpose.clone(), resolution_policy.clone()).unwrap();
        let secret_payload = SecretBindingPayload { provider_id: provider_id.clone(), resolution_policy };
        let mut secret_value = serde_json::to_value(&secret_payload).unwrap();
        secret_value
            .as_object_mut()
            .unwrap()
            .insert("schema_version".to_owned(), json!(1));
        let secret_digest = canonical_digest(&secret_value);
        sqlx::query("INSERT INTO insight_platform.secret_bindings (tenant_id, secret_binding_id, purpose, provider, state, opaque_reference_ciphertext, key_id, reference_digest, payload_schema_version, payload, payload_digest) VALUES ($1,$2,$3,$4,'active',$5,'productization-fixture',$6,1,$7,$8)")
            .bind(&tenant).bind(secret_binding_id.to_string()).bind(purpose.as_str()).bind(provider_id.to_string())
            .bind(vec![1_u8]).bind(canonical_digest(&json!({"fixture": "anonymous"}))).bind(&secret_value).bind(secret_digest).execute(&pool).await.unwrap();
        let authorization_id = new_id(ResourceKind::McpAuthorizationBinding);
        let exact_deployment = ExactDeploymentRef::new(
            deployment["deployment_id"].as_str().unwrap().parse().unwrap(),
            deployment["deployment_digest"].as_str().unwrap().parse().unwrap(),
        ).unwrap();
        let authorization = McpAuthorizationBindingRecord::create(NewMcpAuthorizationBinding {
            tenant_id,
            authorization_binding_id: authorization_id.clone(),
            mcp_deployment: exact_deployment,
            principal_kind: McpAuthorizationPrincipalKind::PerUser,
            principal_id,
            principal_identity_kind,
            principal_binding_generation: u64::try_from(generation).unwrap(),
            audience_identity_digest: audience_digest.parse().unwrap(),
            granted_scopes: vec!["resources.read".to_owned(), "tools.call".to_owned()],
            token_secret_binding: exact_secret,
            expires_at: Utc::now() + Duration::hours(4),
        }, Utc::now()).unwrap();
        let value = serde_json::to_value(&authorization).unwrap();
        let digest = canonical_digest(&value);
        sqlx::query("INSERT INTO insight_platform.resources (tenant_id, resource_id, resource_kind, lifecycle_state, gate_state, version, payload_schema_version, payload, payload_digest) VALUES ($1,$2,'mcp_authorization_binding','active','enabled',1,1,$3,$4)")
            .bind(&tenant).bind(authorization_id.to_string()).bind(value).bind(digest).execute(&pool).await.unwrap();
        authorization_id.to_string()
    })
}

fn start_environment(
    insight: &Path,
    project: &Path,
    deployment: &Value,
    endpoint: &Value,
    policies: &std::collections::BTreeMap<&str, native_and_remote_capability::PolicyAuthority>,
) -> McpEnvironment {
    let workspace = insight
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .unwrap();
    let runtime = project.join(".insight/runtime");
    let tls = runtime.join("tls");
    let config_path = runtime.join("config/egress-broker.json");
    let original_config = fs::read(&config_path).unwrap();
    let mut config: Value = serde_json::from_slice(&original_config).unwrap();
    config["mcp_streamable_http_endpoints"] = json!([{
        "schema_version": 1,
        "deployment": deployment,
        "endpoint": endpoint,
        "endpoint_identity_digest": canonical_digest(endpoint),
        "server_identity_digest": canonical_digest(endpoint),
        "protocol_policy": policies["protocol"].revision,
        "network_policy": policies["network"].revision,
        "tls_policy": policies["tls"].revision,
        "trust_policy": policies["trust"].revision,
        "auth_policy": policies["auth"].revision,
        "token_credential_purpose": "mcp.oauth.token",
        "trusted_root_pem": fs::read_to_string(tls.join("ca.pem")).unwrap(),
        "development_loopback": true,
        "development_anonymous": true
    }]);
    let address = config["observability_listen_address"]
        .as_str()
        .unwrap()
        .to_owned();
    if TcpStream::connect(&address).is_ok() {
        stop_role(project, "egress-broker");
    }
    let node = env::var("PLATFORM_PRODUCTIZATION_NODE_BIN").unwrap_or_else(|_| "node".to_owned());
    let mut fixture = Command::new(node)
        .arg(workspace.join("examples/productization/remote-fixture.mjs"))
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
        .env(
            "INSIGHT_REMOTE_FIXTURE_TRACE_PATH",
            runtime.join("logs/remote-fixture-mcp.jsonl"),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let ready = BufReader::new(fixture.stdout.take().unwrap())
        .lines()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&ready).unwrap()["status"],
        "ready"
    );
    let mut broker =
        native_and_remote_capability::spawn_egress(workspace, project, &config_path, &config);
    native_and_remote_capability::wait_egress(&mut broker, &address);
    McpEnvironment {
        broker,
        fixture,
        config_path,
        original_config,
    }
}

fn discover(insight: &Path, project: &Path, server: &Value, authorization_id: &str) -> Value {
    let (client, base_url, token) = native_and_remote_capability::raw_management_client(project);
    let deadline = (Utc::now() + Duration::minutes(9)).to_rfc3339_opts(SecondsFormat::Micros, true);
    let response = client.post(format!("{base_url}/v1/mcp-servers/{}/deployments/{}:discover", server["resource_id"].as_str().unwrap(), server["deployment_id"].as_str().unwrap()))
        .bearer_auth(token).header("accept", "application/json").header("content-type", "application/json")
        .header("idempotency-key", "productization-mcp-discovery")
        .json(&json!({"schema_version": 1, "authorization_binding_id": authorization_id, "deadline": deadline}))
        .send().unwrap();
    if response.status() != StatusCode::ACCEPTED {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        let database = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(DATABASE_URL)
                    .await
                    .unwrap();
                sqlx::query_as::<_, (String, String, String, String, String, i64, Value)>(
                    "SELECT job_id, job_kind, work_class, owner_kind, owner_id, version, payload FROM insight_platform.jobs WHERE job_kind='mcp_discovery' ORDER BY created_at DESC LIMIT 1",
                )
                .fetch_optional(&pool)
                .await
                .unwrap()
            });
        panic!("MCP discovery returned {status}: {body}; durable Job={database:?}");
    }
    let operation: Value = response.json().unwrap();
    let operation_id = operation["operation_id"].as_str().unwrap();
    let terminal = run_json(
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
    assert_eq!(
        terminal["state"], "succeeded",
        "MCP discovery failed: {terminal}"
    );
    terminal
}

fn discovery_snapshot(insight: &Path, project: &Path, fixture: &Path) -> Value {
    let payload = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let pool = PgPoolOptions::new().max_connections(1).connect(DATABASE_URL).await.unwrap();
            sqlx::query_scalar::<_, Value>("SELECT payload FROM insight_platform.resources WHERE resource_kind='mcp_discovery_snapshot' AND lifecycle_state='active' AND gate_state='enabled' ORDER BY created_at DESC LIMIT 1")
                .fetch_one(&pool).await.unwrap()
        });
    let snapshot = &payload["snapshot"];
    assert_eq!(snapshot["negotiated_version"], "2025-11-25");
    assert_eq!(snapshot["negotiated_capabilities"]["tools"], true);
    assert_eq!(snapshot["negotiated_capabilities"]["resources"], true);
    let artifact = &snapshot["objects_artifact"];
    let output = fixture.join("remote-mcp-discovery.json");
    let read = run_json(
        insight,
        &[
            "artifact",
            "read",
            artifact["artifact_id"].as_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(read["content_digest"], artifact["content_digest"]);
    let bytes = fs::read(&output).unwrap();
    assert_eq!(
        u64::try_from(bytes.len()).unwrap(),
        artifact["byte_length"].as_u64().unwrap()
    );
    let descriptor: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(descriptor["tools"][0]["name"], "fixture_lookup");
    assert_eq!(
        descriptor["resources"][0]["uri"],
        "mcp://insight.fixture/productization"
    );
    let serialized = String::from_utf8(bytes).unwrap();
    for forbidden in ["authorization_binding_id", "secret_binding_id", "token"] {
        assert!(!serialized.contains(forbidden));
    }
    payload
}

fn install_exact_mcp_codec(
    insight: &Path,
    project: &Path,
    backend_contract: &Value,
    schema_digest: &str,
    protocol: &Value,
    objects_digest: &Value,
) -> (Value, Child) {
    let workspace = insight
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .unwrap();
    let runtime = project.join(".insight/runtime");
    let tls = runtime.join("tls");
    let config_path = runtime.join("config/capability-remote.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&config_path).unwrap()).unwrap();
    let existing = config["installed_mcp_codecs"][0].clone();
    let descriptor_digest = canonical_digest(&json!({
        "contract": backend_contract,
        "domain": "insight.platform/v1/installed-capability-codec-descriptor",
        "schema_version": 1
    }));
    config["installed_mcp_codecs"] = json!([{
        "codec_id": existing["codec_id"], "codec_version": existing["codec_version"],
        "module_digest": existing["module_digest"], "worker_protocol_version": existing["worker_protocol_version"],
        "descriptor_digest": descriptor_digest, "remote_tool_name": "fixture_lookup",
        "remote_input_schema_digest": schema_digest, "output_mapping_digest": existing["output_mapping_digest"],
        "protocol_profile_id": protocol["revision_id"], "protocol_profile_digest": protocol["semantic_digest"],
        "discovery_semantic_evidence_digest": objects_digest
    }]);
    config["worker_manifest"]["adapter_runtime_digest"] = json!(canonical_digest(&json!({
        "schema_version": 1,
        "http": config["installed_http_codecs"],
        "grpc": config["installed_grpc_codecs"],
        "mcp": config["installed_mcp_codecs"]
    })));
    stop_role(project, "capability-remote");
    fs::write(&config_path, canonical_bytes(&config)).unwrap();
    let digest = canonical_digest(&config);
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(runtime.join("logs/capability-remote-mcp-restart.log"))
        .unwrap();
    let stderr = log.try_clone().unwrap();
    let mut child =
        Command::new(workspace.join("target/release/platform-capability-remote-worker"))
            .env("PLATFORM_CAPABILITY_REMOTE_WORKER_CONFIG", &config_path)
            .env("PLATFORM_CAPABILITY_REMOTE_WORKER_CONFIG_DIGEST", digest)
            .env(
                "PLATFORM_CAPABILITY_REMOTE_WORKER_DATABASE_URL",
                DATABASE_URL,
            )
            .env(
                "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_CA_PATH",
                tls.join("ca.pem"),
            )
            .env(
                "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_CERT_PATH",
                tls.join("capability-remote-client.pem"),
            )
            .env(
                "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_KEY_PATH",
                tls.join("capability-remote-client-key.pem"),
            )
            .env(
                "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_CA_PATH",
                tls.join("ca.pem"),
            )
            .env(
                "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_CERT_PATH",
                tls.join("capability-remote-client.pem"),
            )
            .env(
                "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_KEY_PATH",
                tls.join("capability-remote-client-key.pem"),
            )
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr))
            .spawn()
            .unwrap();
    let address = config["observability_listen_address"].as_str().unwrap();
    let deadline = Instant::now() + StdDuration::from_secs(30);
    while TcpStream::connect(address).is_err() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "replacement MCP Capability worker exited"
        );
        assert!(
            Instant::now() < deadline,
            "replacement MCP Capability worker readiness"
        );
        thread::sleep(StdDuration::from_millis(50));
    }
    (config, child)
}

fn publish_agent(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    deployment: &Value,
    schema: &Value,
    policies: &std::collections::BTreeMap<&str, native_and_remote_capability::PolicyAuthority>,
    authoring_ref: &Value,
) -> String {
    let schema_digest = schema["canonical_digest"].as_str().unwrap();
    let contract_digest = canonical_digest(&json!({"agent": "remote-mcp-tool"}));
    let requirement_digest = canonical_digest(&json!({"slot": "mcp"}));
    let plan = json!({
        "plan_version": 5, "interface_contract_digest": contract_digest, "entry_node_id": "start",
        "dependency_slots": {"mcp": {"kind": "capability", "requirement_digest": requirement_digest}},
        "nodes": {
            "start": {"kind": "start", "next": "mcp"},
            "mcp": {"kind": "capability_call", "capability_slot_id": "mcp",
                "input": {"source": "run_input", "schema_digest": schema_digest}, "candidate_route": null,
                "output": {"source": "node_output", "producer_node_id": "mcp", "port_id": "result", "schema_digest": schema_digest},
                "attempt_limit": 1, "retry_backoff_milliseconds": 100, "resume": "finish"},
            "finish": {"kind": "return", "value": {"source": "node_output", "producer_node_id": "mcp", "port_id": "result", "schema_digest": schema_digest}}
        }
    });
    let plan_path = write_canonical(fixture, "remote-mcp-agent-plan.json", &plan);
    let plan_upload = upload_artifact(
        insight,
        project,
        &plan_path,
        "typed_plan",
        "remote-mcp-agent-plan.json",
    );
    let manifest = json!({
        "schema_version": 1, "kind": "insight.platform.apply/v1", "resource_noun": "agents",
        "create": {"display_name": "Remote MCP Tool agent", "document": {"resource_kind": "agent", "spec": {
            "authoring_package": {"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]},
            "contract_digest": contract_digest, "dependency_versions": [],
            "policy_versions": policies.values().map(|policy| policy.revision.clone()).collect::<Vec<_>>(),
            "input_schema": schema, "output_schema": schema, "error_schema": native_and_remote_capability::value_schema("error"),
            "typed_plan_artifact_id": plan_upload["artifact_id"], "typed_plan_digest": plan_upload["content_digest"]
        }}},
        "publish": {"kind": "agent", "revision_no": 1, "interface_content_digest": contract_digest,
            "plan_content_digest": plan_upload["content_digest"], "artifact_id": plan_upload["artifact_id"]},
        "deployment": {"environment": "local", "closure": {"resource_kind": "agent", "bindings": {
            "entry_node_id": "start", "entry_node_kind": "start",
            "slots": [{"slot_id": "mcp", "requirement_digest": requirement_digest, "target": {"kind": "capability",
                "candidates": [deployment], "selection_policy": policies["selection"].binding}}],
            "policies": [], "execution_profile": policies["execution"].binding
        }}}
    });
    let path = write_canonical(fixture, "remote-mcp-agent.apply.json", &manifest);
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

pub(super) fn run(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    authoring_ref: &Value,
    qualification_ref: &Value,
) -> McpEvidence {
    let started_at = Utc::now();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let endpoint = endpoint("localhost", port, "/mcp");
    let provider_id = new_id(ResourceKind::SecretProvider);
    let protocol = protocol_document();
    let auth = auth_document(&provider_id, &endpoint);
    let selection =
        json!({"schema_version": 1, "mode": "only_candidate", "route_schema_digest": null});
    let mut policies = std::collections::BTreeMap::new();
    for (role, kind, specialized) in [
        ("protocol", "protocol", Some(("mcp_protocol", protocol))),
        ("auth", "mcp_auth", Some(("mcp_auth", auth))),
        ("selection", "selection", Some(("selection", selection))),
        ("execution", "execution", None),
        ("network", "network", None),
        ("tls", "tls", None),
        ("trust", "trust", None),
    ] {
        policies.insert(
            role,
            native_and_remote_capability::apply_policy(
                insight,
                project,
                fixture,
                &format!("mcp-{role}"),
                kind,
                specialized,
                authoring_ref,
                qualification_ref,
            ),
        );
    }
    let (server, deployment) = publish_server(
        insight,
        project,
        fixture,
        authoring_ref,
        qualification_ref,
        &endpoint,
        &policies,
    );
    let authorization_id = seed_authorization(
        &deployment,
        canonical_digest(&endpoint).as_str(),
        &provider_id,
    );
    let mut environment = start_environment(insight, project, &deployment, &endpoint, &policies);
    let _operation = discover(insight, project, &server, &authorization_id);
    let snapshot_record = discovery_snapshot(insight, project, fixture);
    let snapshot = &snapshot_record["snapshot"];
    let schema = native_and_remote_capability::value_schema("message");
    let schema_digest = schema["canonical_digest"].as_str().unwrap();
    let descriptor: Value =
        serde_json::from_slice(&fs::read(fixture.join("remote-mcp-discovery.json")).unwrap())
            .unwrap();
    let remote_input_schema_digest = canonical_digest(&descriptor["tools"][0]["inputSchema"]);
    let original_remote: Value = serde_json::from_slice(
        &fs::read(project.join(".insight/runtime/config/capability-remote.json")).unwrap(),
    )
    .unwrap();
    let output_mapping =
        original_remote["installed_mcp_codecs"][0]["output_mapping_digest"].clone();
    let backend_contract = json!({"kind": "mcp", "contract": {
        "remote_tool_name": "fixture_lookup", "remote_input_schema_digest": remote_input_schema_digest,
        "output_mapping_digest": output_mapping, "protocol_profile": snapshot["protocol_profile"],
        "discovery_semantic_evidence_digest": snapshot["objects_digest"],
        "supports_task": false, "supports_progress": true
    }});
    let (remote_config, remote_worker) = install_exact_mcp_codec(
        insight,
        project,
        &backend_contract,
        &remote_input_schema_digest,
        &snapshot["protocol_profile"],
        &snapshot["objects_digest"],
    );
    let codec = &remote_config["installed_mcp_codecs"][0];
    let backend_binding = json!({"kind": "mcp", "binding": {
        "codec": {"schema_version": 1, "backend_kind": "mcp", "codec_id": codec["codec_id"],
            "codec_version": codec["codec_version"], "module_digest": codec["module_digest"],
            "worker_protocol_version": codec["worker_protocol_version"], "descriptor_digest": codec["descriptor_digest"]},
        "worker_manifest_digest": canonical_digest(&remote_config["worker_manifest"]),
        "mcp_deployment": deployment, "discovery_snapshot_id": snapshot["snapshot_id"],
        "discovery_snapshot_digest": snapshot["canonical_digest"], "authorization_policy": policies["auth"].revision
    }});
    let (capability_deployment, _) = native_and_remote_capability::publish_capability(
        insight,
        project,
        fixture,
        "remote_mcp",
        "read_only",
        "intrinsic",
        "mcp",
        backend_contract,
        backend_binding,
        &schema,
        &schema,
        authoring_ref,
        qualification_ref,
        vec![
            policies["auth"].revision.clone(),
            policies["protocol"].revision.clone(),
        ],
    );
    native_and_remote_capability::provision_capability_quotas(&[(
        &capability_deployment,
        "capability_remote",
    )]);
    let agent_id = publish_agent(
        insight,
        project,
        fixture,
        &capability_deployment,
        &schema,
        &policies,
        authoring_ref,
    );
    let run_id = native_and_remote_capability::create_run(
        insight,
        project,
        fixture,
        &agent_id,
        schema_digest,
        "mcp-success",
        "mcp round trip",
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
    assert_eq!(result["value"]["value"]["message"], "mcp round trip");

    let remote_error_id = native_and_remote_capability::create_run(
        insight,
        project,
        fixture,
        &agent_id,
        schema_digest,
        "mcp-remote-error",
        "fixture-mcp-remote-error",
    );
    assert_eq!(
        native_and_remote_capability::terminal(insight, project, &remote_error_id)["state"],
        "failed"
    );

    let trace_path = project.join(".insight/runtime/logs/remote-fixture-mcp.jsonl");
    let calls_before = fs::read_to_string(&trace_path)
        .unwrap()
        .lines()
        .filter(|line| line.contains("\"rpc_method\":\"tools/call\""))
        .count();
    let _ = environment.broker.kill();
    let _ = environment.broker.wait();
    let mut wrong_tls: Value =
        serde_json::from_slice(&fs::read(&environment.config_path).unwrap()).unwrap();
    wrong_tls["mcp_streamable_http_endpoints"][0]["trusted_root_pem"] = json!(fs::read_to_string(
        project.join(".insight/runtime/tls/security-authority.pem")
    )
    .unwrap());
    let workspace = insight
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .unwrap();
    let address = wrong_tls["observability_listen_address"]
        .as_str()
        .unwrap()
        .to_owned();
    environment.broker = native_and_remote_capability::spawn_egress(
        workspace,
        project,
        &environment.config_path,
        &wrong_tls,
    );
    native_and_remote_capability::wait_egress(&mut environment.broker, &address);
    let tls_id = native_and_remote_capability::create_run(
        insight,
        project,
        fixture,
        &agent_id,
        schema_digest,
        "mcp-tls",
        "mcp tls rejected",
    );
    assert_eq!(
        native_and_remote_capability::terminal(insight, project, &tls_id)["state"],
        "failed"
    );
    let calls_after = fs::read_to_string(&trace_path)
        .unwrap()
        .lines()
        .filter(|line| line.contains("\"rpc_method\":\"tools/call\""))
        .count();
    assert_eq!(calls_after, calls_before);

    McpEvidence {
        run_id,
        console_passed: false,
        started_at,
        finished_at: Utc::now(),
        _remote_worker: remote_worker,
    }
}
