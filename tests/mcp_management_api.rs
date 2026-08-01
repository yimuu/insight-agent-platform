use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    response::Response,
    Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::Utc;
use insight_api::v1::{
    build_mcp_management_router, LocalMcpRevisionReadiness, McpDraftDocument,
    McpManagementApiState, McpManagementPolicy, McpManifestSignerPolicy, McpRevisionAuthoring,
    OperatorAuth, MCP_SERVER_DISCOVER, MCP_SERVER_PUBLISH, MCP_SERVER_READ, MCP_SERVER_WRITE,
};
use insight_durable::{
    ClaimMcpDiscoveriesCommand, CompleteMcpDiscoveryCommand, CompleteMcpDiscoveryResult,
    McpManagementDurableRepository,
};
use insight_storage::SqliteDurableRepository;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tower::ServiceExt;

const SQLITE_SCHEMA: &str = include_str!("../database/durable/sqlite/schema.sql");

#[derive(Debug)]
struct FixtureAuthoring;

#[async_trait]
impl McpRevisionAuthoring for FixtureAuthoring {
    async fn snapshot_definition_prompts(
        &self,
        _server_id: &str,
        draft: &McpDraftDocument,
        _snapshot: &insight_durable::McpDiscoverySnapshot,
    ) -> Result<BTreeMap<String, Value>, ()> {
        Ok(draft
            .imports
            .prompts
            .iter()
            .filter(|prompt| prompt.definition_arguments.is_some())
            .map(|prompt| {
                (
                    prompt.remote.clone(),
                    json!({
                        "resultType":"complete",
                        "messages":[{"role":"user","content":{"type":"text","text":"frozen definition prompt"}}]
                    }),
                )
            })
            .collect())
    }
}

fn hash(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!(
        "sha256:{}",
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

fn canonical_hash(value: &Value) -> String {
    hash(&serde_jcs::to_vec(value).unwrap())
}

fn all_capabilities() -> BTreeSet<String> {
    BTreeSet::from([
        MCP_SERVER_READ.to_owned(),
        MCP_SERVER_WRITE.to_owned(),
        MCP_SERVER_DISCOVER.to_owned(),
        MCP_SERVER_PUBLISH.to_owned(),
    ])
}

async fn fixture(
    capabilities: BTreeSet<String>,
    trusted_manifest_signers: BTreeMap<String, McpManifestSignerPolicy>,
) -> (tempfile::TempDir, Arc<SqliteDurableRepository>, Router) {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("management.sqlite3");
    File::create(&path).unwrap();
    let options = SqliteConnectOptions::from_str(path.to_str().unwrap())
        .unwrap()
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::raw_sql(SQLITE_SCHEMA).execute(&pool).await.unwrap();
    pool.close().await;
    let repository = Arc::new(SqliteDurableRepository::connect_path(&path).await.unwrap());
    let auth = OperatorAuth::new([(
        "operator-token".to_owned(),
        "operator-a".to_owned(),
        capabilities,
    )])
    .unwrap();
    let policy = McpManagementPolicy {
        preferred_protocol: "2026-07-28".to_owned(),
        legacy_protocols: BTreeSet::new(),
        allowed_secret_refs: BTreeSet::from(["ENGINEERING_MCP_TOKEN".to_owned()]),
        resolvable_secret_refs: BTreeSet::from(["ENGINEERING_MCP_TOKEN".to_owned()]),
        stdio_profile_fingerprints: BTreeMap::new(),
        stdio_profile_allowed_parameters: BTreeMap::new(),
        allow_loopback_development: false,
        trusted_manifest_signers,
        manifest_max_validity: Duration::from_secs(86_400),
        max_pending_discoveries: 8,
        max_request_bytes: 1024 * 1024,
        max_response_bytes: 16 * 1024 * 1024,
        max_sse_line_bytes: 65_536,
        max_sse_event_bytes: 1024 * 1024,
        max_content_items: 128,
        max_catalog_items: 4096,
        policy_fingerprint: hash(b"policy"),
        cursor_signing_key: [7; 32],
    };
    let app = build_mcp_management_router(McpManagementApiState {
        auth,
        repository: repository.clone(),
        policy: Arc::new(policy),
        authoring: Arc::new(FixtureAuthoring),
        readiness: Arc::new(LocalMcpRevisionReadiness),
    });
    (temporary, repository, app)
}

fn request(
    method: &str,
    uri: &str,
    token: Option<&str>,
    request_id: Option<&str>,
    if_match: Option<&str>,
    body: Value,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if let Some(request_id) = request_id {
        builder = builder.header("x-request-id", request_id);
    }
    if let Some(if_match) = if_match {
        builder = builder.header(header::IF_MATCH, if_match);
    }
    builder
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn empty_request(method: &str, uri: &str, request_id: &str, if_match: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, "Bearer operator-token")
        .header("x-request-id", request_id)
        .header(header::IF_MATCH, if_match)
        .body(Body::empty())
        .unwrap()
}

async fn response_value(response: Response) -> Value {
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn management_auth_strict_contract_cursor_and_rejection_audit_fail_closed() {
    let (temporary, _repository, app) = fixture(all_capabilities(), BTreeMap::new()).await;
    let unauthorized = app
        .clone()
        .oneshot(request(
            "GET",
            "/v1/admin/mcp/servers",
            Some("ordinary-api-token"),
            None,
            None,
            json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    let hidden = app
        .clone()
        .oneshot(request(
            "GET",
            "/v1/admin/mcp/servers/private",
            Some("ordinary-api-token"),
            None,
            None,
            json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(hidden.status(), StatusCode::NOT_FOUND);

    let created = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/mcp/servers",
            Some("operator-token"),
            Some("create-one"),
            None,
            json!({"server_id":"one","display_name":"One","draft":{}}),
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    assert_eq!(created.headers()[header::ETAG], "\"server-1\"");
    assert_eq!(
        created.headers()[header::CACHE_CONTROL],
        "private, no-store"
    );
    let replay = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/mcp/servers",
            Some("operator-token"),
            Some("create-one"),
            None,
            json!({"server_id":"one","display_name":"One","draft":{}}),
        ))
        .await
        .unwrap();
    assert_eq!(replay.headers()["idempotent-replayed"], "true");

    let missing_match = app
        .clone()
        .oneshot(request(
            "PUT",
            "/v1/admin/mcp/servers/one/draft",
            Some("operator-token"),
            Some("draft-one"),
            None,
            json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(missing_match.status(), StatusCode::PRECONDITION_REQUIRED);
    for uri in [
        "/v1/admin/mcp/servers?limit=0",
        "/v1/admin/mcp/servers?unknown=true",
        "/v1/admin/mcp/servers?cursor=Zm9yZ2Vk.invalid",
    ] {
        assert_eq!(
            app.clone()
                .oneshot(request(
                    "GET",
                    uri,
                    Some("operator-token"),
                    None,
                    None,
                    json!({})
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }

    let secret_marker = "must-not-enter-audit";
    let unknown = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/mcp/servers",
            Some("operator-token"),
            Some("rejected-request"),
            None,
            json!({"server_id":"bad","display_name":"Bad","unknown":secret_marker}),
        ))
        .await
        .unwrap();
    assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);

    let bad_hash = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/mcp/servers",
            Some("operator-token"),
            Some("bad-hash"),
            None,
            json!({"server_id":"bad-hash","display_name":"Bad hash","draft":{"imports":{"tools":[{
                "remote":"search","candidate_schema_hash":"sha256:bad","as":"search","effect":"mutating",
                "idempotency":"unknown","cancellation":"not_supported","approval":"always","input_required":"denied","tasks":"denied"
            }],"resources":{"allow":[]},"prompts":[]}}}),
        ))
        .await
        .unwrap();
    assert_eq!(bad_hash.status(), StatusCode::BAD_REQUEST);

    let bodyful_delete = app
        .clone()
        .oneshot(request(
            "DELETE",
            "/v1/admin/mcp/servers/one/active-revision",
            Some("operator-token"),
            Some("bodyful-delete"),
            Some("\"server-1\""),
            json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(bodyful_delete.status(), StatusCode::BAD_REQUEST);

    let path = temporary.path().join("management.sqlite3");
    let options = SqliteConnectOptions::from_str(path.to_str().unwrap())
        .unwrap()
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let audit_rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT actor_id,request_id_hash,result_code FROM mcp_management_audit_events
         WHERE event_kind='mcp.management.rejected' ORDER BY audit_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(audit_rows.iter().any(|row| {
        row.0 == "operator-a" && row.1 == hash(b"rejected-request") && row.2 == "http_400"
    }));
    let audit_dump = format!("{audit_rows:?}");
    assert!(!audit_dump.contains(secret_marker));
    assert!(!audit_dump.contains("rejected-request"));
    pool.close().await;

    let (_temporary, _repository, read_only) = fixture(
        BTreeSet::from([MCP_SERVER_READ.to_owned()]),
        BTreeMap::new(),
    )
    .await;
    assert_eq!(
        read_only
            .oneshot(request(
                "POST",
                "/v1/admin/mcp/servers",
                Some("operator-token"),
                Some("forbidden"),
                None,
                json!({"server_id":"nope","display_name":"Nope"})
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn explicit_discovery_import_publish_and_lifecycle_are_end_to_end() {
    let (_temporary, repository, app) = fixture(all_capabilities(), BTreeMap::new()).await;
    let created = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/mcp/servers",
            Some("operator-token"),
            Some("lifecycle-create"),
            None,
            json!({
                "server_id":"engineering","display_name":"Engineering",
                "draft":{
                    "transport":{"type":"streamable_http","endpoint":"https://mcp.example.test/mcp"},
                    "discovery":{"type":"live_service_account"},"authorization":{"type":"none"},
                    "protocol":{"preferred":"2026-07-28","legacy_fallback":[]},
                    "imports":{"tools":[],"resources":{"allow":[]},"prompts":[]},"limits":{}
                }
            }),
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);

    let discovery = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/mcp/servers/engineering/discoveries",
            Some("operator-token"),
            Some("discover"),
            Some("\"draft-1\""),
            json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(discovery.status(), StatusCode::ACCEPTED);
    let location = discovery.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .to_owned();
    let discovery_id = response_value(discovery).await["data"]["discovery_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let claim = repository
        .claim_mcp_discoveries(ClaimMcpDiscoveriesCommand {
            worker_id: "api-conformance".to_owned(),
            now: Utc::now(),
            lease_expires_at: Utc::now() + chrono::Duration::seconds(30),
            limit: 1,
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    let input_schema = json!({"type":"object","additionalProperties":false,"properties":{"query":{"type":"string"}},"required":["query"]});
    let output_schema = json!({"type":"object","additionalProperties":false,"properties":{"result":{"type":"string"}},"required":["result"]});
    let schema_hash = canonical_hash(&json!({"input":input_schema,"output":output_schema}));
    let snapshot = json!({
        "capabilities":{"tools":{},"resources":{},"prompts":{}},
        "server_info":{"name":"fixture","version":"1.0.0"},"selected_protocol":"2026-07-28",
        "tools":[{"remote":"search","input_schema":input_schema,"output_schema":output_schema,"schema_hash":schema_hash,"byte_count":128,"importable":true}],
        "resources":[{"uri":"https://mcp.example.test/docs/one","name":"one","mimeType":"text/plain"}],
        "resource_templates":[{"uriTemplate":"https://mcp.example.test/docs/{id}","name":"by-id","mimeType":"text/plain"}],
        "prompts":[{"name":"summarize","arguments":[]}],
        "rejections":{"tools":[{"identity":"unsafe-tool","code":"schema_not_closed"}],"resources":[],"resource_templates":[],"prompts":[]},
        "source":"fixture"
    });
    repository
        .complete_mcp_discovery(CompleteMcpDiscoveryCommand {
            discovery_id: discovery_id.clone(),
            claim_token: claim.claim_token,
            now: Utc::now(),
            result: CompleteMcpDiscoveryResult::Succeeded {
                catalog_fingerprint: canonical_hash(&snapshot),
                snapshot_document: snapshot,
            },
        })
        .await
        .unwrap();

    let candidate_tools = app
        .clone()
        .oneshot(request(
            "GET",
            &format!("{location}/tools"),
            Some("operator-token"),
            None,
            None,
            json!({}),
        ))
        .await
        .unwrap();
    let candidate_tools = response_value(candidate_tools).await;
    assert_eq!(
        candidate_tools["data"]["items"].as_array().unwrap().len(),
        2
    );
    assert_eq!(candidate_tools["data"]["items"][1]["importable"], false);

    let preview = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/mcp/servers/engineering/tool-import-previews",
            Some("operator-token"),
            None,
            None,
            json!({"discovery_id":discovery_id,"selection":{"mode":"all"},"alias_prefix":"engineering_"}),
        ))
        .await
        .unwrap();
    let tool_items = response_value(preview).await["data"]["items"].clone();
    assert_eq!(tool_items.as_array().unwrap().len(), 1);

    let tools = app
        .clone()
        .oneshot(request(
            "PUT",
            "/v1/admin/mcp/servers/engineering/draft/imports/tools",
            Some("operator-token"),
            Some("tools"),
            Some("\"draft-1\""),
            json!({"discovery_id":discovery_id,"items":tool_items}),
        ))
        .await
        .unwrap();
    assert_eq!(tools.headers()[header::ETAG], "\"draft-2\"");
    let resources = app
        .clone()
        .oneshot(request(
            "PUT",
            "/v1/admin/mcp/servers/engineering/draft/imports/resources",
            Some("operator-token"),
            Some("resources"),
            Some("\"draft-2\""),
            json!({"discovery_id":discovery_id,"items":[{"uri_pattern":"https://mcp.example.test/docs/*","mime_allowlist":["text/plain"],"max_content_bytes":4096}]}),
        ))
        .await
        .unwrap();
    assert_eq!(resources.headers()[header::ETAG], "\"draft-3\"");
    let prompts = app
        .clone()
        .oneshot(request(
            "PUT",
            "/v1/admin/mcp/servers/engineering/draft/imports/prompts",
            Some("operator-token"),
            Some("prompts"),
            Some("\"draft-3\""),
            json!({"discovery_id":discovery_id,"items":[{"remote":"summarize","allow_user_invocation":true,"definition_arguments":{}}]}),
        ))
        .await
        .unwrap();
    assert_eq!(prompts.headers()[header::ETAG], "\"draft-4\"");

    let validation = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/mcp/servers/engineering/validations",
            Some("operator-token"),
            Some("validation"),
            Some("\"draft-4\""),
            json!({"discovery_id":discovery_id}),
        ))
        .await
        .unwrap();
    assert_eq!(validation.status(), StatusCode::CREATED);
    let validation = response_value(validation).await;
    assert_eq!(validation["data"]["valid"], true);
    let validation_id = validation["data"]["validation_id"].as_str().unwrap();
    let published = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/mcp/servers/engineering/revisions",
            Some("operator-token"),
            Some("publish"),
            Some("\"draft-4\""),
            json!({"draft_version":4,"discovery_id":discovery_id,"validation_id":validation_id}),
        ))
        .await
        .unwrap();
    assert_eq!(published.status(), StatusCode::CREATED);
    let published = response_value(published).await;
    let revision_id = published["data"]["revision_id"].as_str().unwrap();
    assert_eq!(
        published["data"]["document"]["bindings"]["tools"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        published["data"]["document"]["bindings"]["prompts"]["definition_results"]["summarize"]
            ["messages"][0]["content"]["text"],
        "frozen definition prompt"
    );

    let activated = app
        .clone()
        .oneshot(request(
            "PUT",
            "/v1/admin/mcp/servers/engineering/active-revision",
            Some("operator-token"),
            Some("activate"),
            Some("\"server-1\""),
            json!({"revision_id":revision_id}),
        ))
        .await
        .unwrap();
    assert_eq!(activated.headers()[header::ETAG], "\"server-2\"");
    let disabled = app
        .clone()
        .oneshot(empty_request(
            "DELETE",
            "/v1/admin/mcp/servers/engineering/active-revision",
            "disable",
            "\"server-2\"",
        ))
        .await
        .unwrap();
    assert_eq!(disabled.headers()[header::ETAG], "\"server-3\"");
    let retired = app
        .oneshot(request(
            "POST",
            "/v1/admin/mcp/servers/engineering/retirement",
            Some("operator-token"),
            Some("retire"),
            Some("\"server-3\""),
            json!({"reason_code":"decommissioned"}),
        ))
        .await
        .unwrap();
    assert_eq!(retired.headers()[header::ETAG], "\"server-4\"");
}

#[tokio::test]
async fn signed_manifest_is_verified_and_only_safe_summary_is_returned() {
    use ring::{
        rand::SystemRandom,
        signature::{Ed25519KeyPair, KeyPair},
    };

    let key_document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    let key_pair = Ed25519KeyPair::from_pkcs8(key_document.as_ref()).unwrap();
    let (_temporary, _repository, app) = fixture(
        all_capabilities(),
        BTreeMap::from([(
            "release".to_owned(),
            McpManifestSignerPolicy {
                public_key: key_pair.public_key().as_ref().to_vec(),
            },
        )]),
    )
    .await;
    assert_eq!(
        app.clone()
            .oneshot(request(
                "POST",
                "/v1/admin/mcp/servers",
                Some("operator-token"),
                Some("manifest-server"),
                None,
                json!({"server_id":"manifest","display_name":"Manifest","draft":{}})
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED
    );
    let issued_at = Utc::now() - chrono::Duration::minutes(1);
    let expires_at = Utc::now() + chrono::Duration::hours(1);
    let mut payload = json!({
        "server_id":"manifest","protocol":{"preferred":"2026-07-28"},
        "discover":{"capabilities":{"tools":{}},"serverInfo":{"name":"fixture","version":"1.0.0"}},
        "tools":[{"name":"search","inputSchema":{"type":"object","additionalProperties":false}}],
        "resources":[],"resource_templates":[],"prompts":[],"issued_at":issued_at,"expires_at":expires_at
    });
    let content_hash = canonical_hash(&payload);
    payload["content_hash"] = json!(content_hash);
    let canonical = serde_jcs::to_vec(&payload).unwrap();
    let signature = key_pair.sign(&canonical);
    let upload = json!({
        "format":"jcs-ed25519-v1","payload":URL_SAFE_NO_PAD.encode(&canonical),
        "key_id":"release","signature":URL_SAFE_NO_PAD.encode(signature.as_ref())
    });
    let accepted = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/mcp/servers/manifest/manifests",
            Some("operator-token"),
            Some("manifest-valid"),
            None,
            upload.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::CREATED);
    let safe = response_value(accepted).await;
    assert!(safe["data"].get("payload").is_none());
    assert!(safe["data"].get("signature").is_none());
    assert_eq!(safe["data"]["content_hash"], content_hash);

    let mut tampered = canonical;
    let index = tampered.iter().position(|byte| *byte == b'f').unwrap();
    tampered[index] = b'g';
    let rejected = app
        .oneshot(request(
            "POST",
            "/v1/admin/mcp/servers/manifest/manifests",
            Some("operator-token"),
            Some("manifest-tampered"),
            None,
            json!({"format":"jcs-ed25519-v1","payload":URL_SAFE_NO_PAD.encode(tampered),"key_id":"release","signature":upload["signature"]}),
        ))
        .await
        .unwrap();
    assert!(matches!(
        rejected.status(),
        StatusCode::BAD_REQUEST | StatusCode::FORBIDDEN
    ));
}
