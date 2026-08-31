use super::*;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const DATABASE_URL: &str = "postgres://insight:insight@127.0.0.1:5432/insight_platform";

pub(super) struct ContextRetrievalEvidence {
    pub run_id: String,
    dataset_id: String,
    generation_id: String,
    console_passed: bool,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
}

impl ContextRetrievalEvidence {
    pub(super) fn mark_console_passed(&mut self) {
        self.console_passed = true;
    }

    pub(super) fn report(&self, revision: &str) -> Value {
        let check = |id: &str, status: &str, evidence: &str| json!({"id": id, "status": status, "evidence": evidence});
        let console = if self.console_passed {
            check(
                "console",
                "passed",
                "the real headless Chromium Console read the exact terminal Context Run and displayed the deterministic cited content through the fresh Gateway authority",
            )
        } else {
            check(
                "console",
                "not_run",
                "set PLATFORM_PRODUCTIZATION_CONSOLE_BROWSER=true to read the exact Context Run through the real Console",
            )
        };
        json!({
            "schema_version": 1,
            "report_kind": "insight.productization.scenario-report/v1",
            "scenario_id": "context-retrieval-and-citation",
            "contract_profile": "insight.platform/v1",
            "profile": "starter+context",
            "automation_layer": "P3",
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
                check("cli", "passed", "public insight apply/operation/run/watch/result commands completed against the fresh starter+context profile"),
                check("http_fixture", "passed", "raw /v1 triggered the Dataset build, read its typed Operation result and exact immutable generation, and exercised invalid build admission"),
                console,
            ],
            "assertions": [
                check("dataset_generation", "passed", &format!("Dataset {} published exact immutable generation {} discovered only through its successful Operation", self.dataset_id, self.generation_id)),
                check("citation_projection", "passed", "the terminal public Run result contained the deterministic Context item, observation_only citation, exact Dataset generation and content digest"),
                check("exact_context_binding", "passed", "the Agent Deployment froze the exact Context Deployment, PinAtRunAdmission Dataset and distinct authorization/ranking policy revisions"),
            ],
            "failure_probes": [
                check("dataset_build_failure", "passed", "an expired public Dataset build request failed closed before creating an Operation or Dataset target"),
                check("citation_policy_rejection", "passed", "an exact-only Context Interface rejected the Native adapter's observation_only citation and the owning Run reached failed without a result"),
            ],
        })
    }
}

#[derive(Clone)]
struct PolicyAuthority {
    revision: Value,
    binding: Value,
}

fn policy_manifest(
    role: &str,
    policy_kind: &str,
    authoring_ref: &Value,
    qualification_ref: &Value,
) -> Value {
    let rules_digest = canonical_digest(&json!({"context_policy_role": role}));
    let contract_digest = canonical_digest(&json!({
        "context_policy_role": role,
        "policy_kind": policy_kind,
    }));
    let applicability_digest =
        canonical_digest(&json!({"context_policy_role": role, "environment": "local"}));
    json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": "policies",
        "create": {
            "display_name": format!("Context {role} policy"),
            "document": {
                "resource_kind": "policy",
                "spec": {
                    "authoring_package": {
                        "artifact": authoring_ref,
                        "manifest_digest": authoring_ref["content_digest"],
                    },
                    "contract_digest": contract_digest,
                    "dependency_versions": [],
                    "policy_versions": [],
                    "policy_kind": policy_kind,
                    "rules_digest": rules_digest,
                    "selection": null,
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
                    "sandbox_secret_resolution": null,
                },
            },
        },
        "publish": {
            "kind": "single",
            "revision_no": 1,
            "content_digest": contract_digest,
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
    })
}

fn apply_policy(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    role: &str,
    policy_kind: &str,
    authoring_ref: &Value,
    qualification_ref: &Value,
) -> PolicyAuthority {
    let manifest = policy_manifest(role, policy_kind, authoring_ref, qualification_ref);
    let path = write_canonical(
        fixture,
        &format!("context-{role}-policy.apply.json"),
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
        "applicability_digest": manifest["deployment"]["closure"]["bindings"]["applicability_digest"],
        "qualification_evidence": qualification_ref,
    });
    let binding = json!({
        "deployment": {
            "deployment_id": report["deployment_id"],
            "resource_kind": "policy_deployment",
            "deployment_digest": canonical_digest(&json!({
                "schema_version": 1,
                "resource_kind": "policy",
                "bindings": closure,
            })),
        },
        "revision": revision,
    });
    PolicyAuthority { revision, binding }
}

fn string_schema(maximum: u32) -> Value {
    json!({
        "type": "string",
        "minLength": 1,
        "maxLength": maximum,
        "x-platform-max-bytes": maximum * 4,
    })
}

fn digest_schema() -> Value {
    string_schema(71)
}

fn exact_version_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "revision_id": string_schema(128),
            "resource_kind": string_schema(64),
            "semantic_digest": digest_schema(),
        },
        "required": ["revision_id", "resource_kind", "semantic_digest"],
        "additionalProperties": false,
    })
}

fn exact_deployment_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "deployment_id": string_schema(128),
            "resource_kind": string_schema(64),
            "deployment_digest": digest_schema(),
        },
        "required": ["deployment_id", "resource_kind", "deployment_digest"],
        "additionalProperties": false,
    })
}

fn dataset_view_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "kind": {"const": "generation"},
            "exact": {
                "type": "object",
                "properties": {
                    "dataset_id": string_schema(128),
                    "generation_id": string_schema(128),
                    "generation_digest": digest_schema(),
                },
                "required": ["dataset_id", "generation_id", "generation_digest"],
                "additionalProperties": false,
            },
        },
        "required": ["kind", "exact"],
        "additionalProperties": false,
    })
}

fn context_output_schema_document() -> Value {
    let dataset_view = dataset_view_schema();
    let citation = json!({
        "type": "object",
        "properties": {
            "context_deployment": exact_deployment_schema(),
            "interface_revision": exact_version_schema(),
            "dataset_view": dataset_view.clone(),
            "locator": {
                "type": "object",
                "properties": {
                    "kind": {"const": "remote_opaque"},
                    "locator_digest": digest_schema(),
                },
                "required": ["kind", "locator_digest"],
                "additionalProperties": false,
            },
            "strength": {"const": "observation_only"},
            "content_digest": digest_schema(),
            "observed_at": string_schema(64),
            "display_label": string_schema(256),
        },
        "required": ["context_deployment", "interface_revision", "dataset_view", "locator", "strength", "content_digest", "observed_at", "display_label"],
        "additionalProperties": false,
    });
    let item = json!({
        "type": "object",
        "properties": {
            "item_id": string_schema(128),
            "source_item_identity_digest": digest_schema(),
            "content": {
                "type": "object",
                "properties": {"kind": {"const": "inline"}, "value": string_schema(4096)},
                "required": ["kind", "value"],
                "additionalProperties": false,
            },
            "structured_fields": {
                "type": "object",
                "properties": {
                    "schema_digest": digest_schema(),
                    "value": {"type": "object", "properties": {}, "required": [], "additionalProperties": false},
                    "canonical_digest": digest_schema(),
                },
                "required": ["schema_digest", "value", "canonical_digest"],
                "additionalProperties": false,
            },
            "score": {
                "type": "object",
                "properties": {
                    "millionths": {"type": "integer", "minimum": -1000000, "maximum": 1000000},
                    "score_domain_digest": digest_schema(),
                },
                "required": ["millionths", "score_domain_digest"],
                "additionalProperties": false,
            },
            "classification": {"const": "internal"},
            "citation": citation,
            "authorization_evidence_digest": digest_schema(),
        },
        "required": ["item_id", "source_item_identity_digest", "content", "structured_fields", "score", "classification", "citation", "authorization_evidence_digest"],
        "additionalProperties": false,
    });
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "schema_version": {"type": "integer", "minimum": 1, "maximum": 1},
            "observation_id": string_schema(128),
            "context_query_id": string_schema(128),
            "dataset_view": dataset_view,
            "normalized_query_digest": digest_schema(),
            "items": {"type": "array", "items": item, "minItems": 1, "maxItems": 8},
            "next_cursor_digest": {"oneOf": [digest_schema(), {"type": "null"}]},
            "evidence": {
                "type": "object",
                "properties": {
                    "backend_request_digest": digest_schema(),
                    "backend_response_digest": digest_schema(),
                    "authorization_evidence_digest": digest_schema(),
                    "ranking_evidence_digest": digest_schema(),
                    "candidate_count": {"type": "integer", "minimum": 0, "maximum": 1000},
                    "rejected_count": {"type": "integer", "minimum": 0, "maximum": 1000},
                    "truncated": {"type": "boolean"},
                },
                "required": ["backend_request_digest", "backend_response_digest", "authorization_evidence_digest", "ranking_evidence_digest", "candidate_count", "rejected_count", "truncated"],
                "additionalProperties": false,
            },
            "observed_at": string_schema(64),
            "total_bytes": {"type": "integer", "minimum": 0, "maximum": 1048576},
        },
        "required": ["schema_version", "observation_id", "context_query_id", "dataset_view", "normalized_query_digest", "items", "next_cursor_digest", "evidence", "observed_at", "total_bytes"],
        "additionalProperties": false,
    })
}

fn context_query_schema_document() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"message": string_schema(1024)},
        "required": ["message"],
        "additionalProperties": false,
    })
}

fn closed_schema(schema: Value) -> Value {
    let canonical_digest = canonical_digest(&schema);
    json!({
        "schema_version": 1,
        "profile": "insight.closed-json-schema/1",
        "schema": schema,
        "canonical_digest": canonical_digest,
    })
}

fn publish_context_interface(
    insight: &Path,
    project: &Path,
    name: &str,
    manifest: &Value,
) -> Value {
    let (client, base_url, token) = raw_management_client(project);
    let receipt = |step: &str| format!("context-{name}-interface-{step}");
    let created = client
        .post(format!("{base_url}/v1/contexts"))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .header("idempotency-key", receipt("create"))
        .json(&manifest["create"])
        .send()
        .expect("raw Context Interface create completes");
    assert_eq!(created.status(), StatusCode::CREATED);
    assert!(created.headers().contains_key("trace-id"));
    let create_etag = created
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .expect("created Context Interface ETag")
        .to_owned();
    let created: Value = created.json().expect("created Context Interface is JSON");
    let resource_id = created["resource_id"]
        .as_str()
        .expect("Context Interface Resource ID");

    let validation = client
        .post(format!(
            "{base_url}/v1/contexts/{resource_id}/draft:validate"
        ))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .header("idempotency-key", receipt("validate"))
        .header("if-match", create_etag)
        .body(Vec::new())
        .send()
        .expect("raw Context Interface validation starts");
    assert_eq!(validation.status(), StatusCode::ACCEPTED);
    let validation: Value = validation
        .json()
        .expect("Context Interface validation Operation is JSON");
    let operation_id = validation["operation_id"]
        .as_str()
        .expect("Context Interface validation Operation ID");
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
    assert_eq!(operation["state"], "succeeded");

    let validated = client
        .get(format!("{base_url}/v1/contexts/{resource_id}"))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .send()
        .expect("validated Context Interface read completes");
    assert_eq!(validated.status(), StatusCode::OK);
    let validated_etag = validated
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .expect("validated Context Interface ETag")
        .to_owned();
    let validated: Value = validated
        .json()
        .expect("validated Context Interface is JSON");
    assert!(validated["draft"]["validation"].is_object());

    let published = client
        .post(format!(
            "{base_url}/v1/contexts/{resource_id}/draft:publish"
        ))
        .bearer_auth(token)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .header("idempotency-key", receipt("publish"))
        .header("if-match", validated_etag)
        .json(&manifest["publish"])
        .send()
        .expect("raw Context Interface publish completes");
    assert_eq!(published.status(), StatusCode::OK);
    let publish_etag = published
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .expect("published Context Interface ETag")
        .to_owned();
    let published: Value = published
        .json()
        .expect("published Context Interface is JSON");
    assert_eq!(published["resource_id"], resource_id);
    assert_eq!(published["etag"], publish_etag);
    let published_versions = published["published_versions"]
        .as_array()
        .expect("Context Interface published_versions array");
    assert_eq!(published_versions.len(), 1);
    let version = &published_versions[0];
    assert!(version["resource_version_id"]
        .as_str()
        .is_some_and(|value| value.starts_with("xirev_")));
    json!({
        "resource_id": resource_id,
        "validation_operation_id": operation_id,
        "published_versions": [{
            "resource_version_id": version["resource_version_id"],
            "resource_kind": "context_source_interface_revision",
            "content_digest": version["content_digest"],
        }],
        "deployment_id": null,
        "active_deployment_id": null,
        "final_resource_etag": publish_etag,
    })
}

fn raw_management_client(project: &Path) -> (Client, String, String) {
    let (client, _, token) = raw_runtime_client(project);
    let profile: Value = serde_json::from_slice(
        &fs::read(project.join(".insight/runtime/profile.json"))
            .expect("runtime profile is readable"),
    )
    .expect("runtime profile is closed JSON");
    let management_port = profile["ports"]["gateway_management"]
        .as_u64()
        .and_then(|value| u16::try_from(value).ok())
        .expect("Management Gateway port");
    let base_url = format!("http://127.0.0.1:{management_port}");
    (client, base_url, token)
}

#[test]
fn context_observation_output_schema_is_closed_and_instance_valid() {
    insight_platform_contracts::ClosedJsonSchema::build(context_query_schema_document())
        .expect("Context query schema is closed");
    let schema =
        insight_platform_contracts::ClosedJsonSchema::build(context_output_schema_document())
            .expect("Context observation output schema is closed");
    assert_eq!(
        schema.canonical_digest.as_str(),
        canonical_digest(&schema.schema)
    );
}

fn apply_context(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    name: &str,
    allowed_strength: &str,
    authoring_ref: &Value,
    qualification_ref: &Value,
    policies: &std::collections::BTreeMap<&str, PolicyAuthority>,
    runtime_config: &Value,
) -> (Value, Value) {
    let context_contract_digest = canonical_digest(&json!({"context_interface": name}));
    let score_domain_digest = canonical_digest(&json!({"score_domain": name}));
    let interface_manifest = json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": "contexts",
        "create": {
            "display_name": format!("{name} Context Interface"),
            "document": {
                "resource_kind": "context_source_interface",
                "spec": {
                    "authoring_package": {"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]},
                    "contract_digest": context_contract_digest,
                    "dependency_versions": [],
                    "policy_versions": [policies["entitlement"].revision, policies["cache"].revision],
                    "query_schema_digest": canonical_digest(&context_query_schema_document()),
                    "filter_schema_digest": canonical_digest(&json!({"schema": "context-filter"})),
                    "item_schema_digest": canonical_digest(&json!({"schema": "context-item"})),
                    "observation_schema_digest": canonical_digest(&context_output_schema_document()),
                    "allowed_consistency": ["pin_at_run_admission", "external_observation"],
                    "citation": {
                        "allowed_strengths": [allowed_strength],
                        "locator_kinds": ["remote_opaque"],
                        "require_content_digest": true,
                        "maximum_display_label_bytes": 256,
                    },
                    "pagination": {"maximum_page_size": 8, "maximum_cursor_bytes": 1024, "cursor_ttl_milliseconds": 60000},
                    "ranking": {"score_domain_digest": score_domain_digest, "reranker_contract_digest": null, "maximum_candidates": 8},
                    "data_policy": {
                        "maximum_classification": "internal",
                        "allowed_regions": ["local"],
                        "entitlement_policy": policies["entitlement"].revision,
                        "cache_policy": policies["cache"].revision,
                        "maximum_retention_milliseconds": 60000,
                    },
                    "limits": {
                        "maximum_query_bytes": 4096,
                        "maximum_filter_bytes": 4096,
                        "maximum_item_bytes": 65536,
                        "maximum_total_bytes": 1048576,
                        "maximum_items": 8,
                        "maximum_fan_out": 1,
                    },
                },
            },
        },
        "publish": {"kind": "single", "revision_no": 1, "content_digest": context_contract_digest, "artifact_id": null},
        "deployment": null,
    });
    let interface_report = publish_context_interface(insight, project, name, &interface_manifest);
    let interface_revision = exact_version(published_version(
        &interface_report,
        "context_source_interface_revision",
    ));

    let implementation_contract_digest = canonical_digest(&json!({"context_implementation": name}));
    let implementation_manifest = json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": "context-implementations",
        "create": {
            "display_name": format!("{name} Native Context Implementation"),
            "document": {
                "resource_kind": "context_source_implementation",
                "spec": {
                    "authoring_package": {"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]},
                    "contract_digest": implementation_contract_digest,
                    "dependency_versions": [interface_revision],
                    "policy_versions": [],
                    "interface_revision": interface_revision,
                    "backend_kind": "native_catalog",
                    "contract": {
                        "backend": {"kind": "native_catalog", "adapter_contract_digest": runtime_config["native_catalog"]["adapter_contract_digest"]},
                        "credential_requirements": [],
                        "limits": {
                            "maximum_request_bytes": 65536,
                            "maximum_response_bytes": 1048576,
                            "maximum_candidates": 8,
                            "maximum_remote_state_bytes": 0,
                            "maximum_poll_count": 0,
                            "total_timeout_milliseconds": 30000,
                        },
                    },
                },
            },
        },
        "publish": {"kind": "single", "revision_no": 1, "content_digest": implementation_contract_digest, "artifact_id": null},
        "deployment": null,
    });
    let implementation_path = write_canonical(
        fixture,
        &format!("{name}-context-implementation.apply.json"),
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
        "context_source_implementation_revision",
    ));

    let closure = json!({
        "resource_kind": "context_source_interface",
        "bindings": {
            "implementation": implementation_revision,
            "interface": interface_revision,
            "required_worker_manifest_digest": canonical_digest(&runtime_config["worker_manifest"]),
            "backend": {"kind": "native_catalog", "installed_adapter_digest": runtime_config["native_catalog"]["installed_adapter_digest"]},
            "secret_bindings": [],
            "network_policy": null,
            "tls_policy": null,
            "trust_policy": null,
            "parser_policy": policies["parser"].revision,
            "chunker_policy": policies["chunker"].revision,
            "embedding_model_deployment": null,
            "ranking_policy": policies["ranking"].revision,
            "data_policy": policies["data"].revision,
            "conformance_evidence": qualification_ref,
        },
    });
    let (client, base_url, token) = raw_management_client(project);
    let resource_id = interface_report["resource_id"]
        .as_str()
        .expect("Context Interface Resource ID");
    let response = client
        .post(format!("{base_url}/v1/contexts/{resource_id}/deployments"))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .header("idempotency-key", format!("context-{name}-deploy"))
        .header(
            "if-match",
            interface_report["final_resource_etag"]
                .as_str()
                .expect("published Context Resource ETag"),
        )
        .json(&json!({
            "resource_version_id": interface_revision["revision_id"],
            "environment": "local",
            "closure": closure,
        }))
        .send()
        .expect("raw Context Deployment create completes");
    assert_eq!(response.status(), StatusCode::CREATED);
    let deployment_view: Value = response.json().expect("Context Deployment is JSON");
    assert_eq!(deployment_view["resource_id"], resource_id);

    let resource = client
        .get(format!("{base_url}/v1/contexts/{resource_id}"))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .send()
        .expect("Context Resource read after Deployment completes");
    assert_eq!(resource.status(), StatusCode::OK);
    let resource_etag = resource
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .expect("Context Resource ETag")
        .to_owned();
    let deployment_id = deployment_view["deployment_id"]
        .as_str()
        .expect("Context Deployment ID");
    let activated = client
        .post(format!(
            "{base_url}/v1/contexts/{resource_id}/deployments/{deployment_id}:activate"
        ))
        .bearer_auth(token)
        .header("accept", "application/json")
        .header("idempotency-key", format!("context-{name}-activate"))
        .header("if-match", resource_etag)
        .body(Vec::new())
        .send()
        .expect("raw Context Deployment activation completes");
    assert_eq!(activated.status(), StatusCode::OK);
    let activated: Value = activated
        .json()
        .expect("activated Context Resource is JSON");
    assert_eq!(activated["gate_state"], "enabled");
    let deployment = json!({
        "deployment_id": deployment_view["deployment_id"],
        "resource_kind": "context_deployment",
        "deployment_digest": deployment_view["closure_digest"],
    });
    (interface_report, deployment)
}

fn apply_context_agent(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    name: &str,
    context_deployment: &Value,
    consistency: Value,
    authoring_ref: &Value,
    policies: &std::collections::BTreeMap<&str, PolicyAuthority>,
) -> (Value, String, String) {
    let input_schema_document = context_query_schema_document();
    let output_schema_document = context_output_schema_document();
    let input_schema = closed_schema(input_schema_document);
    let output_schema = closed_schema(output_schema_document);
    let input_digest = input_schema["canonical_digest"]
        .as_str()
        .expect("Context Agent input schema digest")
        .to_owned();
    let output_digest = output_schema["canonical_digest"]
        .as_str()
        .expect("Context Agent output schema digest")
        .to_owned();
    let interface_contract_digest = canonical_digest(&json!({"context_agent": name}));
    let requirement_digest = canonical_digest(&json!({"context_slot": name}));
    let plan = json!({
        "plan_version": 5,
        "interface_contract_digest": interface_contract_digest,
        "entry_node_id": "start",
        "dependency_slots": {
            "catalog": {"kind": "context", "requirement_digest": requirement_digest},
        },
        "nodes": {
            "start": {"kind": "start", "next": "catalog"},
            "catalog": {
                "kind": "context_query",
                "context_slot_id": "catalog",
                "request": {"source": "run_input", "schema_digest": input_digest},
                "result": {"source": "node_output", "producer_node_id": "catalog", "port_id": "items", "schema_digest": output_digest},
                "maximum_items": 1,
                "resume": "finish",
            },
            "finish": {
                "kind": "return",
                "value": {"source": "node_output", "producer_node_id": "catalog", "port_id": "items", "schema_digest": output_digest},
            },
        },
    });
    let plan_path = write_canonical(fixture, &format!("{name}-context-agent-plan.json"), &plan);
    let plan_upload = upload_artifact(
        insight,
        project,
        &plan_path,
        "typed_plan",
        &format!("{name}-context-agent-plan.json"),
    );
    let ordered_policy_versions = [
        "entitlement",
        "cache",
        "parser",
        "chunker",
        "ranking",
        "data",
        "authorization",
        "execution",
    ]
    .map(|role| policies[role].revision.clone());
    let manifest = json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": "agents",
        "create": {
            "display_name": format!("{name} Context Agent"),
            "document": {
                "resource_kind": "agent",
                "spec": {
                    "authoring_package": {"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]},
                    "contract_digest": interface_contract_digest,
                    "dependency_versions": [],
                    "policy_versions": ordered_policy_versions,
                    "input_schema": input_schema,
                    "output_schema": output_schema,
                    "error_schema": closed_schema(json!({
                        "$schema": "https://json-schema.org/draft/2020-12/schema",
                        "type": "object",
                        "properties": {"message": string_schema(1024)},
                        "required": ["message"],
                        "additionalProperties": false,
                    })),
                    "typed_plan_artifact_id": plan_upload["artifact_id"],
                    "typed_plan_digest": plan_upload["content_digest"],
                },
            },
        },
        "publish": {
            "kind": "agent",
            "revision_no": 1,
            "interface_content_digest": interface_contract_digest,
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
                    "slots": [{
                        "slot_id": "catalog",
                        "requirement_digest": requirement_digest,
                        "target": {
                            "kind": "context",
                            "binding": {
                                "context_deployment": context_deployment,
                                "consistency": consistency,
                                "allowed_projection": [],
                                "authorization_policy": policies["authorization"].revision,
                                "ranking_policy": policies["ranking"].revision,
                            },
                        },
                    }],
                    "policies": [
                        policies["authorization"].binding,
                        policies["ranking"].binding,
                        policies["parser"].binding,
                        policies["chunker"].binding,
                        policies["data"].binding,
                    ],
                    "execution_profile": policies["execution"].binding,
                },
            },
        },
    });
    let manifest_path = write_canonical(
        fixture,
        &format!("{name}-context-agent.apply.json"),
        &manifest,
    );
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
    (report, input_digest, output_digest)
}

fn create_context_run(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    name: &str,
    agent_id: &str,
    input_digest: &str,
) -> String {
    let request = json!({
        "agent_id": agent_id,
        "input": {
            "classification": "internal",
            "schema_digest": input_digest,
            "value": {"kind": "inline", "value": {"message": "retrieve local context"}},
        },
        "deadline": (Utc::now() + Duration::minutes(5)).to_rfc3339_opts(SecondsFormat::Micros, true),
    });
    let request_path = write_canonical(fixture, &format!("{name}-context-run.json"), &request);
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
    run["run_id"].as_str().expect("Context Run ID").to_owned()
}

fn provision_context_query_quotas(context_deployment_id: &str) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Context quota fixture runtime");
    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(DATABASE_URL)
            .await
            .expect("Context quota fixture database");
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
                4_i64,
            ),
            (
                "context_deployment",
                context_deployment_id,
                "durable_quota.context_queries",
                128_i64,
            ),
            (
                "context_deployment",
                context_deployment_id,
                "durable_quota.context_result_bytes",
                67_108_864_i64,
            ),
        ] {
            sqlx::query(
                r#"
                INSERT INTO insight_platform.quota_accounts (
                    tenant_id, quota_account_id, scope_kind, scope_id, work_class, metric,
                    limit_value, payload_schema_version, payload, payload_digest
                ) VALUES ($1, $2, $3, $4, 'context', $5, $6, 1, $7, $8)
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
            .expect("Context quota account is provisioned");
        }
        let exact_count: i64 = sqlx::query_scalar(
            r#"
            SELECT count(*) FROM insight_platform.quota_accounts
            WHERE tenant_id = $1 AND work_class = 'context' AND (
                (scope_kind = 'tenant' AND scope_id = $1
                 AND metric = 'durable_quota.work_class_concurrent_operations')
                OR
                (scope_kind = 'context_deployment' AND scope_id = $2
                 AND metric IN ('durable_quota.context_queries',
                                'durable_quota.context_result_bytes'))
            )
            "#,
        )
        .bind(&tenant_id)
        .bind(context_deployment_id)
        .fetch_one(&pool)
        .await
        .expect("Context quota closure is readable");
        assert_eq!(exact_count, 3, "Context quota closure is exact");
    });
}

pub(super) fn run(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    authoring_ref: &Value,
    qualification_ref: &Value,
) -> ContextRetrievalEvidence {
    let started_at = Utc::now();
    let runtime_config: Value = serde_json::from_slice(
        &fs::read(project.join(".insight/runtime/config/context-native.json"))
            .expect("Native Context runtime config is readable"),
    )
    .expect("Native Context runtime config is closed JSON");
    let mut policies = std::collections::BTreeMap::new();
    for (role, kind) in [
        ("entitlement", "authorization"),
        ("cache", "authorization"),
        ("parser", "parser"),
        ("chunker", "chunker"),
        ("ranking", "ranking"),
        ("data", "data_flow"),
        ("authorization", "authorization"),
        ("execution", "execution"),
    ] {
        policies.insert(
            role,
            apply_policy(
                insight,
                project,
                fixture,
                role,
                kind,
                authoring_ref,
                qualification_ref,
            ),
        );
    }

    let (context_report, context_deployment) = apply_context(
        insight,
        project,
        fixture,
        "cited",
        "observation_only",
        authoring_ref,
        qualification_ref,
        &policies,
        &runtime_config,
    );
    let context_id = context_report["resource_id"]
        .as_str()
        .expect("Context Resource ID");
    let context_deployment_id = context_deployment["deployment_id"]
        .as_str()
        .expect("Context Deployment ID");
    provision_context_query_quotas(context_deployment_id);
    let (client, base_url, token) = raw_management_client(project);
    let invalid = client
        .post(format!(
            "{base_url}/v1/contexts/{context_id}/deployments/{context_deployment_id}:build-dataset"
        ))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .header("idempotency-key", "context-expired-dataset-build")
        .json(&json!({
            "schema_version": 1,
            "dataset_id": null,
            "deadline": (Utc::now() - Duration::seconds(1)).to_rfc3339_opts(SecondsFormat::Micros, true),
        }))
        .send()
        .expect("invalid Dataset build request completes");
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    let problem: Value = invalid
        .json()
        .expect("invalid Dataset build Problem is JSON");
    assert_eq!(problem["code"], "invalid_request");

    let build = client
        .post(format!(
            "{base_url}/v1/contexts/{context_id}/deployments/{context_deployment_id}:build-dataset"
        ))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .header("idempotency-key", "context-cited-dataset-build")
        .json(&json!({
            "schema_version": 1,
            "dataset_id": null,
            "deadline": (Utc::now() + Duration::minutes(10)).to_rfc3339_opts(SecondsFormat::Micros, true),
        }))
        .send()
        .expect("Dataset build request completes");
    assert_eq!(build.status(), StatusCode::ACCEPTED);
    let build: Value = build.json().expect("Dataset build Operation is JSON");
    assert_eq!(build["kind"], "context_dataset_build");
    let operation_id = build["operation_id"]
        .as_str()
        .expect("Dataset build Operation ID");
    let dataset_id = build["target"]["context_dataset_id"]
        .as_str()
        .expect("Dataset build target ID")
        .to_owned();
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
    assert_eq!(operation["state"], "succeeded");
    assert_eq!(operation["result"]["kind"], "context_dataset_generation");
    let generation_id = operation["result"]["generation_id"]
        .as_str()
        .expect("Dataset generation ID")
        .to_owned();
    let generation = client
        .get(format!(
            "{base_url}/v1/context-datasets/{dataset_id}/versions/{generation_id}"
        ))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .send()
        .expect("exact Dataset generation read completes");
    assert_eq!(generation.status(), StatusCode::OK);
    let generation: Value = generation.json().expect("Dataset generation is JSON");
    assert_eq!(generation["resource_version_id"], generation_id);
    assert_eq!(
        generation["content_digest"],
        operation["result"]["result_digest"]
    );
    assert_eq!(
        generation["payload"]["document"]["spec"]["generation"]["created_by_operation_id"],
        operation_id
    );

    let (agent, input_digest, output_digest) = apply_context_agent(
        insight,
        project,
        fixture,
        "cited",
        &context_deployment,
        json!({"mode": "pin_at_run_admission", "dataset_id": dataset_id}),
        authoring_ref,
        &policies,
    );
    let agent_id = agent["resource_id"].as_str().expect("Context Agent ID");
    let run_id = create_context_run(insight, project, fixture, "cited", agent_id, &input_digest);
    let watched = run_json_lines(
        insight,
        &[
            "run",
            "watch",
            &run_id,
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(watched.last().unwrap()["run"]["state"], "succeeded");
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
    let observation = &result["value"]["value"];
    assert_eq!(
        observation["items"][0]["content"]["value"],
        "local deterministic context item"
    );
    assert_eq!(
        observation["items"][0]["citation"]["strength"],
        "observation_only"
    );
    assert_eq!(
        observation["items"][0]["citation"]["dataset_view"]["exact"]["dataset_id"],
        dataset_id
    );
    assert_eq!(
        observation["items"][0]["citation"]["dataset_view"]["exact"]["generation_id"],
        generation_id
    );
    assert_eq!(
        observation["items"][0]["citation"]["content_digest"],
        canonical_digest(&json!("local deterministic context item"))
    );

    let agent_deployment_id = agent["deployment_id"]
        .as_str()
        .expect("Context Agent Deployment ID");
    let deployment = client
        .get(format!(
            "{base_url}/v1/agents/{agent_id}/deployments/{agent_deployment_id}"
        ))
        .bearer_auth(&token)
        .header("accept", "application/json")
        .send()
        .expect("exact Agent Deployment read completes");
    assert_eq!(deployment.status(), StatusCode::OK);
    let deployment: Value = deployment.json().expect("Agent Deployment is JSON");
    let binding = &deployment["closure"]["bindings"]["slots"][0]["target"]["binding"];
    assert_eq!(binding["owner_agent_deployment_id"], agent_deployment_id);
    assert_eq!(binding["context_deployment"], context_deployment);
    assert_eq!(binding["consistency"]["dataset_id"], dataset_id);
    assert_ne!(binding["authorization_policy"], binding["ranking_policy"]);

    let (_, rejecting_context_deployment) = apply_context(
        insight,
        project,
        fixture,
        "exact-only",
        "exact",
        authoring_ref,
        qualification_ref,
        &policies,
        &runtime_config,
    );
    provision_context_query_quotas(
        rejecting_context_deployment["deployment_id"]
            .as_str()
            .expect("rejecting Context Deployment ID"),
    );
    let (rejecting_agent, rejecting_input_digest, _) = apply_context_agent(
        insight,
        project,
        fixture,
        "exact-only",
        &rejecting_context_deployment,
        json!({"mode": "external_observation"}),
        authoring_ref,
        &policies,
    );
    let rejecting_run_id = create_context_run(
        insight,
        project,
        fixture,
        "exact-only",
        rejecting_agent["resource_id"]
            .as_str()
            .expect("rejecting Context Agent ID"),
        &rejecting_input_digest,
    );
    let rejected = run_json_lines(
        insight,
        &[
            "run",
            "watch",
            &rejecting_run_id,
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(rejected.last().unwrap()["run"]["state"], "failed");
    let rejected = run_json(
        insight,
        &[
            "run",
            "get",
            &rejecting_run_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(rejected["state"], "failed");
    assert_eq!(rejected["output_value_id"], Value::Null);

    ContextRetrievalEvidence {
        run_id,
        dataset_id,
        generation_id,
        console_passed: false,
        started_at,
        finished_at: Utc::now(),
    }
}
