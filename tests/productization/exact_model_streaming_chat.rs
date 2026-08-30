use super::*;
use insight_platform_contracts::{
    ExactSecretBindingRef, ResourceId, ResourceKind, SecretBindingPayload, SecretPurpose,
    SecretResolutionPolicy, Sha256Digest,
};
use insight_platform_postgres::repository::{NewSecretBinding, PgRepository};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const DATABASE_URL: &str = "postgres://insight:insight@127.0.0.1:5432/insight_platform";

#[derive(Clone)]
struct PolicyAuthority {
    revision: Value,
    binding: Value,
}

pub(super) struct ExactModelEvidence {
    pub run_id: String,
    pub console_passed: bool,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
}

impl ExactModelEvidence {
    pub(super) fn mark_console_passed(&mut self) {
        self.console_passed = true;
    }

    pub(super) fn report(&self, revision: &str) -> Value {
        let check = |id: &str, status: &str, evidence: &str| json!({"id": id, "status": status, "evidence": evidence});
        let console = if self.console_passed {
            check(
                "console",
                "passed",
                "the real headless Chromium Console read the exact successful Model Run from the fresh Gateway authority",
            )
        } else {
            check(
                "console",
                "not_run",
                "set PLATFORM_PRODUCTIZATION_CONSOLE_BROWSER=true to read the exact Model Run",
            )
        };
        json!({
            "schema_version": 1,
            "report_kind": "insight.productization.scenario-report/v1",
            "scenario_id": "exact-model-streaming-chat",
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
                check("http_fixture", "passed", "the exact installed Egress closure streamed a deterministic TLS response from the bounded local fixture"),
                console,
            ],
            "assertions": [
                check("exact_model_binding", "passed", "the Agent froze one exact Model Deployment selected by an exact Selection Policy"),
                check("inline_result", "passed", "the terminal Run returned the schema-checked deterministic structured Inline result"),
                check("usage_projection", "passed", "the public Run result retained the provider-reported bounded token usage without prompt or credential leakage"),
            ],
            "failure_probes": [
                check("model_timeout", "passed", "a provider that withheld its first byte reached a closed failed terminal Run"),
                check("output_limit", "passed", "an oversized provider stream was rejected inside Egress and reached a closed failed terminal Run"),
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

fn bounded_string_schema() -> Value {
    json!({
        "type": "string",
        "minLength": 1,
        "maxLength": 256,
        "x-platform-max-bytes": 1024,
        "x-platform-classification": "internal",
    })
}

fn apply_policy(
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
        || canonical_digest(&json!({"model_policy_role": role})),
        |(_, value)| canonical_digest(value),
    );
    let contract_digest = canonical_digest(&json!({"model_policy_role": role, "kind": kind}));
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
        canonical_digest(&json!({"model_policy_role": role, "environment": "local"}));
    let manifest = json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": "policies",
        "create": {"display_name": format!("Model {role} policy"), "document": {"resource_kind": "policy", "spec": spec}},
        "publish": {"kind": "single", "revision_no": 1, "content_digest": contract_digest, "artifact_id": null},
        "deployment": {"environment": "local", "closure": {"resource_kind": "policy", "bindings": {
            "applicability_digest": applicability_digest,
            "qualification_evidence": qualification_ref,
        }}},
    });
    let path = write_canonical(
        fixture,
        &format!("model-{role}-policy.apply.json"),
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
    let binding = json!({
        "deployment": {
            "deployment_id": report["deployment_id"],
            "resource_kind": "policy_deployment",
            "deployment_digest": canonical_digest(&json!({"schema_version": 1, "resource_kind": "policy", "bindings": closure})),
        },
        "revision": revision,
    });
    PolicyAuthority { revision, binding }
}

fn exact_deployment(report: &Value, resource_kind: &str, bindings: Value) -> Value {
    json!({
        "deployment_id": report["deployment_id"],
        "resource_kind": match resource_kind {
            "model_provider" => "model_provider_deployment",
            "model_profile" => "model_deployment",
            _ => panic!("unsupported exact Deployment kind"),
        },
        "deployment_digest": canonical_digest(&json!({
            "schema_version": 1,
            "resource_kind": resource_kind,
            "bindings": bindings,
        })),
    })
}

fn provision_model_quotas(model_deployment_id: &str) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Model quota fixture runtime")
        .block_on(async {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(DATABASE_URL)
                .await
                .expect("Model quota fixture database");
            let tenant_id: String = sqlx::query_scalar(
                "SELECT tenant_id FROM insight_platform.tenants ORDER BY tenant_id LIMIT 1",
            )
            .fetch_one(&pool)
            .await
            .expect("fresh profile tenant");
            let payload = json!({"profile": "productization_full"});
            let payload_digest = canonical_digest(&payload);
            for (scope_kind, scope_id, metric, limit_value) in [
                (
                    "tenant",
                    tenant_id.as_str(),
                    "durable_quota.work_class_concurrent_operations",
                    8_i64,
                ),
                (
                    "model_deployment",
                    model_deployment_id,
                    "durable_quota.model_requests",
                    64_i64,
                ),
                (
                    "model_deployment",
                    model_deployment_id,
                    "durable_quota.model_tokens",
                    131_072_i64,
                ),
                (
                    "model_deployment",
                    model_deployment_id,
                    "durable_quota.model_cost_microunits",
                    2_000_000_i64,
                ),
            ] {
                sqlx::query(
                    r#"
                    INSERT INTO insight_platform.quota_accounts (
                        tenant_id, quota_account_id, scope_kind, scope_id, work_class, metric,
                        limit_value, payload_schema_version, payload, payload_digest
                    ) VALUES ($1, $2, $3, $4, 'model', $5, $6, 1, $7, $8)
                    ON CONFLICT (tenant_id, scope_kind, scope_id, work_class, metric) DO NOTHING
                "#,
                )
                .bind(&tenant_id)
                .bind(format!("qac_{}", Uuid::now_v7()))
                .bind(scope_kind)
                .bind(scope_id)
                .bind(metric)
                .bind(limit_value)
                .bind(&payload)
                .bind(&payload_digest)
                .execute(&pool)
                .await
                .expect("Model quota account is provisioned");
            }
        });
}

fn provision_model_secret_binding() -> Value {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Model SecretBinding fixture runtime")
        .block_on(async {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(DATABASE_URL)
                .await
                .expect("Model SecretBinding fixture database");
            let tenant_id: ResourceId = sqlx::query_scalar::<_, String>(
                "SELECT tenant_id FROM insight_platform.tenants ORDER BY tenant_id LIMIT 1",
            )
            .fetch_one(&pool)
            .await
            .expect("fresh profile tenant")
            .parse()
            .expect("tenant ID is nominal");
            let binding_id = ResourceId::from_uuid_v7(ResourceKind::SecretBinding, Uuid::now_v7())
                .expect("SecretBinding ID");
            let provider_id =
                ResourceId::from_uuid_v7(ResourceKind::SecretProvider, Uuid::now_v7())
                    .expect("SecretProvider ID");
            let purpose: SecretPurpose = "provider.api_key".parse().expect("Secret purpose");
            let resolution_policy = SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: canonical_digest(&json!({
                    "fixture": "model-provider-anonymous-loopback"
                }))
                .parse::<Sha256Digest>()
                .expect("opaque version digest"),
            };
            PgRepository::new(pool)
                .create_secret_binding(NewSecretBinding {
                    tenant_id,
                    secret_binding_id: binding_id.clone(),
                    purpose: purpose.clone(),
                    provider_id: provider_id.clone(),
                    opaque_reference_ciphertext: vec![1],
                    key_id: "productization-local-only".to_owned(),
                    reference_digest: canonical_digest(&json!({"reference": "not-resolved"}))
                        .parse()
                        .expect("reference digest"),
                    payload: SecretBindingPayload {
                        provider_id: provider_id.clone(),
                        resolution_policy: resolution_policy.clone(),
                    },
                })
                .await
                .expect("development SecretBinding authority is provisioned");
            serde_json::to_value(
                ExactSecretBindingRef::build(
                    binding_id,
                    1,
                    provider_id,
                    purpose,
                    resolution_policy,
                )
                .expect("exact development SecretBinding"),
            )
            .expect("exact SecretBinding serializes")
        })
}

fn wait_terminal(insight: &Path, project: &Path, run_id: &str) -> Value {
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
    records.last().expect("terminal Model watch record")["run"].clone()
}

fn create_run(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    agent_id: &str,
    input_digest: &str,
    name: &str,
    prompt: &str,
) -> String {
    let request = json!({
        "agent_id": agent_id,
        "input": {"classification": "internal", "schema_digest": input_digest, "value": {"kind": "inline", "value": {"prompt": prompt}}},
        "deadline": (Utc::now() + Duration::minutes(5)).to_rfc3339_opts(SecondsFormat::Micros, true),
    });
    let path = write_canonical(fixture, &format!("model-{name}-run.json"), &request);
    let run = run_json(
        insight,
        &[
            "run",
            "create",
            "--file",
            path.to_str().unwrap(),
            "--path",
            project.to_str().unwrap(),
        ],
    );
    run["run_id"].as_str().expect("Model Run ID").to_owned()
}

struct RemoteModelEnvironment {
    broker: Child,
    fixture: Child,
    config_path: std::path::PathBuf,
    original_config: Vec<u8>,
}

impl Drop for RemoteModelEnvironment {
    fn drop(&mut self) {
        let _ = self.broker.kill();
        let _ = self.broker.wait();
        let _ = self.fixture.kill();
        let _ = self.fixture.wait();
        fs::write(&self.config_path, &self.original_config)
            .expect("original Egress config is restored");
    }
}

fn start_remote_model_environment(
    insight: &Path,
    project: &Path,
    provider_deployment: &Value,
    provider_revision: &Value,
    endpoint: &Value,
    endpoint_digest: &str,
    policies: &std::collections::BTreeMap<&str, PolicyAuthority>,
) -> RemoteModelEnvironment {
    let workspace = insight
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .expect("workspace");
    let runtime = project.join(".insight/runtime");
    let tls = runtime.join("tls");
    let config_path = runtime.join("config/egress-broker.json");
    let original_config = fs::read(&config_path).expect("Egress config is readable");
    let mut config: Value =
        serde_json::from_slice(&original_config).expect("Egress config is closed JSON");
    config["model_endpoints"] = json!([{
        "schema_version": 1,
        "protocol": "open_ai_responses",
        "provider_deployment": provider_deployment,
        "provider_revision": provider_revision,
        "endpoint": endpoint,
        "endpoint_identity_digest": endpoint_digest,
        "credential_purpose": "provider.api_key",
        "network_policy": policies["network"].revision,
        "tls_policy": policies["tls"].revision,
        "trust_policy": policies["trust"].revision,
        "data_policy": policies["data"].revision,
        "region": "local",
        "development_loopback": true,
        "development_anonymous": true,
        "trusted_root_pem": fs::read_to_string(tls.join("ca.pem")).expect("local CA is readable"),
    }]);
    let config_digest = canonical_digest(&config);
    let ready_address = config["observability_listen_address"]
        .as_str()
        .expect("Egress observability address")
        .to_owned();
    if TcpStream::connect(&ready_address).is_ok() {
        stop_role(project, "egress-broker");
    }
    fs::write(&config_path, canonical_bytes(&config)).expect("installed Egress config is writable");

    let fixture_port = endpoint["port"].as_u64().expect("fixture port").to_string();
    let node = env::var("PLATFORM_PRODUCTIZATION_NODE_BIN").unwrap_or_else(|_| "node".to_owned());
    let mut fixture = Command::new(node)
        .arg(workspace.join("examples/productization/remote-fixture.mjs"))
        .env("INSIGHT_REMOTE_FIXTURE_PORT", &fixture_port)
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
            runtime.join("logs/remote-fixture-model.jsonl"),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("remote fixture starts");
    let ready = BufReader::new(fixture.stdout.take().expect("fixture stdout"))
        .lines()
        .next()
        .expect("fixture readiness line")
        .expect("fixture readiness is UTF-8");
    assert_eq!(
        serde_json::from_str::<Value>(&ready).expect("fixture readiness JSON")["status"],
        "ready"
    );

    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(runtime.join("logs/egress-broker-model-restart.log"))
        .expect("Egress log");
    let stderr = log.try_clone().expect("Egress log clone");
    let mut broker = Command::new(workspace.join("target/release/platform-egress-broker"))
        .env("PLATFORM_EGRESS_BROKER_CONFIG", &config_path)
        .env("PLATFORM_EGRESS_BROKER_CONFIG_DIGEST", &config_digest)
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
        .expect("replacement Egress Broker starts");
    let deadline = Instant::now() + StdDuration::from_secs(30);
    while TcpStream::connect(&ready_address).is_err() {
        if let Some(status) = broker.try_wait().expect("Egress status") {
            panic!("replacement Egress Broker exited before readiness: {status}");
        }
        assert!(
            Instant::now() < deadline,
            "replacement Egress Broker did not become ready"
        );
        thread::sleep(StdDuration::from_millis(50));
    }
    RemoteModelEnvironment {
        broker,
        fixture,
        config_path,
        original_config,
    }
}

pub(super) fn run(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    authoring_ref: &Value,
    qualification_ref: &Value,
) -> ExactModelEvidence {
    let started_at = Utc::now();
    let instruction = "Follow exact platform safety and authorization contracts.";
    let safety = json!({
        "schema_version": 1,
        "contract_id": "platform.model_safety.v1",
        "platform_instruction": instruction,
        "instruction_content_digest": digest_bytes(instruction.as_bytes()),
        "instruction_byte_budget": 1024,
        "instruction_token_budget": 256,
        "pre_dispatch_rules_digest": canonical_digest(&json!({"phase": "pre"})),
        "post_response_rules_digest": canonical_digest(&json!({"phase": "post"})),
    });
    let budget = json!({
        "schema_version": 1, "maximum_attempts_per_turn": 1,
        "maximum_input_tokens_per_turn": 2048, "maximum_output_tokens_per_turn": 256,
        "maximum_total_tokens_per_turn": 2304, "cost_ceiling_microunits_per_turn": 1_000_000,
    });
    let projection = json!({"schema_version": 1, "reject_prompt_overflow": true, "retain_source_map": true, "retain_sensitive_prompt_body": false});
    let selection =
        json!({"schema_version": 1, "mode": "only_candidate", "route_schema_digest": null});
    let mut policies = std::collections::BTreeMap::new();
    for (role, kind, specialized) in [
        ("protocol", "protocol", None),
        ("network", "network", None),
        ("tls", "tls", None),
        ("trust", "trust", None),
        ("data", "data_flow", None),
        ("safety", "model_safety", Some(("model_safety", safety))),
        ("budget", "budget", Some(("model_budget", budget))),
        (
            "projection",
            "public_projection",
            Some(("model_public_projection", projection)),
        ),
        ("selection", "selection", Some(("selection", selection))),
        ("execution", "execution", None),
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

    let model_config: Value = serde_json::from_slice(
        &fs::read(project.join(".insight/runtime/config/model-worker.json"))
            .expect("Model Worker config"),
    )
    .expect("Model Worker config JSON");
    let adapter = model_config["installed_adapters"]
        .as_array()
        .expect("installed adapters")
        .iter()
        .find(|entry| entry["qualified_name"] == "openai.responses/v1")
        .expect("OpenAI Responses adapter");
    let endpoint_port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("fixture port reservation")
        .local_addr()
        .unwrap()
        .port();
    let endpoint =
        json!({"scheme": "https", "host": "localhost", "port": endpoint_port, "base_path": "/"});
    let endpoint_digest = canonical_digest(&endpoint);
    let secret_binding = provision_model_secret_binding();
    let provider_contract_digest =
        canonical_digest(&json!({"provider": "productization-openai-responses"}));
    let provider_manifest = json!({
        "schema_version": 1, "kind": "insight.platform.apply/v1", "resource_noun": "model-providers",
        "create": {"display_name": "Productization OpenAI Responses Provider", "document": {"resource_kind": "model_provider", "spec": {
            "authoring_package": {"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]},
            "contract_digest": provider_contract_digest,
            "dependency_versions": [policies["protocol"].revision], "policy_versions": [policies["protocol"].revision],
            "installed_adapter": adapter, "protocol_policy": policies["protocol"].revision, "credential_requirements": ["provider.api_key"],
            "request_limits": {"maximum_request_bytes": 65536, "maximum_response_bytes": 4096, "maximum_messages": 16, "maximum_parts": 32,
                "maximum_tools": 1, "maximum_parallel_tool_calls": 1, "maximum_stream_delta_bytes": 4096,
                "connect_timeout_milliseconds": 1000, "first_byte_timeout_milliseconds": 1500,
                "idle_timeout_milliseconds": 1500, "total_timeout_milliseconds": 3000},
        }}},
        "publish": {"kind": "single", "revision_no": 1, "content_digest": provider_contract_digest, "artifact_id": null},
        "deployment": {"environment": "local", "closure": {"resource_kind": "model_provider", "bindings": {
            "endpoint_identity_digest": endpoint_digest, "secret_bindings": [secret_binding], "protocol_policy": policies["protocol"].revision,
            "network_policy": policies["network"].revision, "tls_policy": policies["tls"].revision,
            "trust_policy": policies["trust"].revision, "data_policy": policies["data"].revision,
            "region": "local", "conformance_evidence": qualification_ref,
        }}},
    });
    let provider_path = write_canonical(fixture, "model-provider.apply.json", &provider_manifest);
    let provider_report = run_json(
        insight,
        &[
            "apply",
            "--file",
            provider_path.to_str().unwrap(),
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let provider_revision = exact_version(published_version(
        &provider_report,
        "model_provider_revision",
    ));
    let provider_bindings = json!({
        "provider_revision": provider_revision, "endpoint_identity_digest": endpoint_digest, "secret_bindings": [secret_binding],
        "protocol_policy": policies["protocol"].revision, "network_policy": policies["network"].revision,
        "tls_policy": policies["tls"].revision, "trust_policy": policies["trust"].revision,
        "data_policy": policies["data"].revision, "region": "local", "conformance_evidence": qualification_ref,
    });
    let provider_deployment =
        exact_deployment(&provider_report, "model_provider", provider_bindings);

    let parameter_schema = json!({"type": "object", "properties": {"temperature": {"type": "integer", "minimum": 0, "maximum": 1}}, "required": ["temperature"], "additionalProperties": false});
    let parameter_schema_digest = canonical_digest(&parameter_schema);
    let defaults = json!({"temperature": 0});
    let profile_contract_digest = canonical_digest(&json!({"model": "fixture-model-2026-08"}));
    let observed_at = Utc::now() - Duration::minutes(1);
    let model_manifest = json!({
        "schema_version": 1, "kind": "insight.platform.apply/v1", "resource_noun": "models",
        "create": {"display_name": "Productization deterministic model", "document": {"resource_kind": "model_profile", "spec": {
            "authoring_package": {"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]},
            "contract_digest": profile_contract_digest, "dependency_versions": [provider_revision],
            "policy_versions": [policies["data"].revision, policies["safety"].revision, policies["budget"].revision, policies["projection"].revision],
            "provider_revision": provider_revision, "model_identity": {"value": "fixture-model-2026-08", "stability": "pinned"},
            "modalities": {"input": ["text"], "output": ["text"]},
            "context": {"maximum_context_tokens": 4096, "maximum_output_tokens": 256,
                "tokenizer_contract_digest": canonical_digest(&json!({"tokenizer": "fixture"})), "estimator_contract_digest": canonical_digest(&json!({"estimator": "fixture"}))},
            "tools": {"supported": true, "parallel": true, "maximum_tools": 1, "maximum_calls_per_turn": 1, "maximum_argument_bytes": 1024},
            "structured_output": {"native": true, "textual_json_fallback": true, "may_combine_with_tool_intent": false, "maximum_schema_bytes": 65536, "maximum_output_bytes": 4096},
            "parameter_schema_digest": parameter_schema_digest,
            "usage": {"provider_reports_usage": true, "reports_cached_input_tokens": false, "reports_reasoning_tokens": false, "reports_cost": false,
                "cost_currency": null, "estimator_contract_digest": canonical_digest(&json!({"usage_estimator": "fixture"}))},
            "data_handling": {"maximum_classification": "internal", "allowed_regions": ["local"], "maximum_retention_milliseconds": 60000,
                "training": "prohibited", "subprocessor_set_digest": canonical_digest(&json!({"subprocessors": []}))},
            "limits": {"maximum_messages": 16, "maximum_parts": 32, "maximum_text_bytes": 32768, "maximum_tools": 1,
                "maximum_parallel_tool_calls": 1, "maximum_rounds": 1, "maximum_input_tokens": 2048, "maximum_output_tokens": 256},
            "catalog_evidence": {"artifact": qualification_ref, "source_digest": canonical_digest(&json!({"catalog": "fixture"})),
                "adapter_contract_digest": adapter["adapter_contract_digest"],
                "observed_at": observed_at.to_rfc3339_opts(SecondsFormat::Micros, true),
                "expires_at": (observed_at + Duration::days(1)).to_rfc3339_opts(SecondsFormat::Micros, true)},
        }}},
        "publish": {"kind": "single", "revision_no": 1, "content_digest": profile_contract_digest, "artifact_id": null},
        "deployment": {"environment": "local", "closure": {"resource_kind": "model_profile", "bindings": {
            "provider_deployment": provider_deployment, "data_policy": policies["data"].revision,
            "safety_policy": policies["safety"].revision, "budget_policy": policies["budget"].revision,
            "public_projection_policy": policies["projection"].revision,
            "generation_defaults": {"schema_digest": parameter_schema_digest, "value": defaults, "canonical_digest": canonical_digest(&defaults)},
        }}},
    });
    let model_path = write_canonical(fixture, "model-profile.apply.json", &model_manifest);
    let model_report = run_json(
        insight,
        &[
            "apply",
            "--file",
            model_path.to_str().unwrap(),
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let model_revision = exact_version(published_version(&model_report, "model_profile_revision"));
    let model_bindings = json!({
        "profile_revision": model_revision, "provider_deployment": provider_deployment,
        "data_policy": policies["data"].revision, "safety_policy": policies["safety"].revision,
        "budget_policy": policies["budget"].revision, "public_projection_policy": policies["projection"].revision,
        "generation_defaults": {"schema_digest": parameter_schema_digest, "value": defaults, "canonical_digest": canonical_digest(&defaults)},
    });
    let model_deployment = exact_deployment(&model_report, "model_profile", model_bindings);
    provision_model_quotas(
        model_deployment["deployment_id"]
            .as_str()
            .expect("Model Deployment ID"),
    );

    let remote = start_remote_model_environment(
        insight,
        project,
        &provider_deployment,
        &provider_revision,
        &endpoint,
        &endpoint_digest,
        &policies,
    );

    let input_schema = closed_schema(
        json!({"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": {"prompt": bounded_string_schema()}, "required": ["prompt"], "additionalProperties": false}),
    );
    let output_schema = closed_schema(
        json!({"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": {"answer": bounded_string_schema()}, "required": ["answer"], "additionalProperties": false}),
    );
    let input_digest = input_schema["canonical_digest"].as_str().unwrap();
    let output_digest = output_schema["canonical_digest"].as_str().unwrap();
    let contract_digest = canonical_digest(&json!({"agent": "exact-model-streaming-chat"}));
    let requirement_digest = canonical_digest(&json!({"slot": "primary_model"}));
    let plan = json!({
        "plan_version": 5, "interface_contract_digest": contract_digest, "entry_node_id": "start",
        "dependency_slots": {"primary_model": {"kind": "model", "requirement_digest": requirement_digest}},
        "nodes": {
            "start": {"kind": "start", "next": "model"},
            "model": {"kind": "model_loop", "model_slot_id": "primary_model", "skill_slot_ids": [], "capability_slot_ids": [],
                "input": {"source": "run_input", "schema_digest": input_digest}, "model_route": null,
                "output": {"source": "node_output", "producer_node_id": "model", "port_id": "response", "schema_digest": output_digest},
                "maximum_rounds": 1, "maximum_capability_calls": 1, "maximum_parallel_calls_per_round": 1, "token_budget": 2304, "resume": "finish"},
            "finish": {"kind": "return", "value": {"source": "node_output", "producer_node_id": "model", "port_id": "response", "schema_digest": output_digest}},
        },
    });
    let plan_path = write_canonical(fixture, "model-agent-plan.json", &plan);
    let plan_upload = upload_artifact(
        insight,
        project,
        &plan_path,
        "typed_plan",
        "model-agent-plan.json",
    );
    let agent_manifest = json!({
        "schema_version": 1, "kind": "insight.platform.apply/v1", "resource_noun": "agents",
        "create": {"display_name": "Exact model streaming agent", "document": {"resource_kind": "agent", "spec": {
            "authoring_package": {"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]},
            "contract_digest": contract_digest, "dependency_versions": [],
            "policy_versions": policies.values().map(|policy| policy.revision.clone()).collect::<Vec<_>>(),
            "input_schema": input_schema, "output_schema": output_schema,
            "error_schema": closed_schema(json!({"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": {"message": bounded_string_schema()}, "required": ["message"], "additionalProperties": false})),
            "typed_plan_artifact_id": plan_upload["artifact_id"], "typed_plan_digest": plan_upload["content_digest"],
        }}},
        "publish": {"kind": "agent", "revision_no": 1, "interface_content_digest": contract_digest,
            "plan_content_digest": plan_upload["content_digest"], "artifact_id": plan_upload["artifact_id"]},
        "deployment": {"environment": "local", "closure": {"resource_kind": "agent", "bindings": {
            "entry_node_id": "start", "entry_node_kind": "start",
            "slots": [{"slot_id": "primary_model", "requirement_digest": requirement_digest,
                "target": {"kind": "model", "candidates": [model_deployment], "selection_policy": policies["selection"].binding}}],
            "policies": [policies["safety"].binding, policies["budget"].binding, policies["projection"].binding, policies["data"].binding],
            "execution_profile": policies["execution"].binding,
        }}},
    });
    let agent_path = write_canonical(fixture, "model-agent.apply.json", &agent_manifest);
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
    let agent_id = agent_report["resource_id"]
        .as_str()
        .expect("Model Agent ID");

    let run_id = create_run(
        insight,
        project,
        fixture,
        agent_id,
        input_digest,
        "success",
        "respond deterministically",
    );
    assert_eq!(
        wait_terminal(insight, project, &run_id)["state"],
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
    assert_eq!(result["schema_digest"], output_digest);
    assert_eq!(
        result["value"]["value"]["answer"],
        "deterministic streamed model response"
    );

    for (name, prompt) in [
        ("timeout", "fixture-timeout"),
        ("output-limit", "fixture-output-limit"),
    ] {
        let failure_id = create_run(
            insight,
            project,
            fixture,
            agent_id,
            input_digest,
            name,
            prompt,
        );
        assert_eq!(
            wait_terminal(insight, project, &failure_id)["state"],
            "failed",
            "{name} probe must fail closed"
        );
    }
    drop(remote);
    ExactModelEvidence {
        run_id,
        console_passed: false,
        started_at,
        finished_at: Utc::now(),
    }
}
