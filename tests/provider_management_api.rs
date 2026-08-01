use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    Router,
};
use insight_agent_platform::resources::provider_management::{
    ProviderManagementRuntime, ProviderManagementRuntimeConfig,
};
use insight_api::v1::{
    build_provider_management_router, LocalProviderRevisionReadiness, OperatorAuth,
    ProviderManagementApiState, ProviderManagementPolicy, PROVIDER_ACTIVATE, PROVIDER_DISCOVER,
    PROVIDER_PUBLISH, PROVIDER_READ, PROVIDER_RETIRE, PROVIDER_SUSPEND, PROVIDER_TEST,
    PROVIDER_WRITE,
};
use insight_resources::models::{ModelRegistry, ModelSelector};
use insight_storage::SqliteDurableRepository;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tower::ServiceExt;

const SQLITE_SCHEMA: &str = include_str!("../database/durable/sqlite/schema.sql");

fn hash(bytes: &[u8]) -> String {
    format!(
        "sha256:{}",
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

fn capabilities() -> BTreeSet<String> {
    BTreeSet::from(
        [
            PROVIDER_READ,
            PROVIDER_WRITE,
            PROVIDER_DISCOVER,
            PROVIDER_TEST,
            PROVIDER_PUBLISH,
            PROVIDER_ACTIVATE,
            PROVIDER_SUSPEND,
            PROVIDER_RETIRE,
        ]
        .map(str::to_owned),
    )
}

async fn fixture() -> (tempfile::TempDir, Arc<SqliteDurableRepository>, Router) {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("provider-management.sqlite3");
    File::create(&path).unwrap();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::from_str(path.to_str().unwrap())
                .unwrap()
                .foreign_keys(true),
        )
        .await
        .unwrap();
    sqlx::raw_sql(SQLITE_SCHEMA).execute(&pool).await.unwrap();
    pool.close().await;
    let repository = Arc::new(SqliteDurableRepository::connect_path(&path).await.unwrap());
    let auth = OperatorAuth::new([(
        "operator-token".to_owned(),
        "operator-a".to_owned(),
        capabilities(),
    )])
    .unwrap();
    let policy = ProviderManagementPolicy {
        installed_adapters: BTreeMap::from([("open_ai_compatible".to_owned(), "2.1.0".to_owned())]),
        allowed_secret_refs: BTreeSet::from(["secret://environment/COMPANY_LLM_TOKEN".to_owned()]),
        resolvable_secret_refs: BTreeSet::from([
            "secret://environment/COMPANY_LLM_TOKEN".to_owned()
        ]),
        templates: Vec::new(),
        allow_loopback_development: false,
        max_models: 32,
        max_connect_timeout_ms: 30_000,
        max_request_timeout_ms: 600_000,
        max_pending_operations: 4,
        require_connection_test_for_publish: false,
        policy_fingerprint: hash(b"provider-policy"),
        cursor_signing_key: [8; 32],
    };
    let app = build_provider_management_router(ProviderManagementApiState {
        auth,
        repository: repository.clone(),
        policy: Arc::new(policy),
        readiness: Arc::new(LocalProviderRevisionReadiness),
    });
    (temporary, repository, app)
}

fn request(
    method: &str,
    uri: &str,
    request_id: Option<&str>,
    if_match: Option<&str>,
    body: Option<Value>,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, "Bearer operator-token");
    if let Some(request_id) = request_id {
        builder = builder.header("x-request-id", request_id);
    }
    if let Some(if_match) = if_match {
        builder = builder.header(header::IF_MATCH, if_match);
    }
    match body {
        Some(body) => builder
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    }
}

async fn json_body(response: axum::response::Response) -> Value {
    serde_json::from_slice(
        &to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap()
}

fn create_body() -> Value {
    json!({
        "provider_id":"company-llm","display_name":"Company LLM","adapter_type":"open_ai_compatible",
        "draft":{
            "adapter":{"type":"open_ai_compatible"},"endpoint":"https://llm.company.test/v1",
            "credential":{"type":"none"},
            "transport":{"tls":"required","redirects":"deny","connect_timeout_ms":5000,"request_timeout_ms":120000},
            "models":[{"id":"chat-v1","input":["text"],"capabilities":["complete","streaming"],"provenance":{"type":"adapter_verified"}}]
        }
    })
}

#[tokio::test]
async fn provider_http_lifecycle_is_authenticated_cas_idempotent_and_body_free() {
    let (temporary, repository, app) = fixture().await;

    let unauthorized = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/admin/providers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let created = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/providers",
            Some("create-1"),
            None,
            Some(create_body()),
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    assert_eq!(created.headers()[header::ETAG], "\"provider-1\"");
    assert_eq!(
        created.headers()[header::CACHE_CONTROL],
        "private, no-store"
    );
    let replayed = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/providers",
            Some("create-1"),
            None,
            Some(create_body()),
        ))
        .await
        .unwrap();
    assert_eq!(replayed.status(), StatusCode::CREATED);
    assert_eq!(replayed.headers()["idempotent-replayed"], "true");

    let validation = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/providers/company-llm/validations",
            Some("validate-1"),
            None,
            Some(json!({"draft_version":1})),
        ))
        .await
        .unwrap();
    assert_eq!(validation.status(), StatusCode::ACCEPTED);
    let validation_id = json_body(validation).await["data"]["validation_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let published = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/providers/company-llm/revisions",
            Some("publish-1"),
            None,
            Some(json!({"draft_version":1,"validation_id":validation_id})),
        ))
        .await
        .unwrap();
    assert_eq!(published.status(), StatusCode::CREATED);
    let revision_id = json_body(published).await["data"]["revision_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let activated = app
        .clone()
        .oneshot(request(
            "PUT",
            "/v1/admin/providers/company-llm/active-revision",
            Some("activate-1"),
            Some("\"provider-1\""),
            Some(json!({"revision_id":revision_id})),
        ))
        .await
        .unwrap();
    assert_eq!(activated.status(), StatusCode::OK);
    assert_eq!(activated.headers()[header::ETAG], "\"provider-2\"");
    let models = ModelRegistry::default();
    let mut provider_runtime = ProviderManagementRuntime::start(
        repository.clone(),
        models.clone(),
        ProviderManagementRuntimeConfig {
            enabled: true,
            workers: 1,
            poll_interval: Duration::from_millis(20),
            lease_duration: Duration::from_secs(30),
            retention: chrono::Duration::days(1),
            max_response_bytes: 1024 * 1024,
            allow_loopback_development: false,
        },
    )
    .await
    .unwrap();
    let selector = ModelSelector::new("company-llm", "chat-v1").unwrap();
    assert!(models.resolve(&selector).is_ok());

    let nonempty_delete = app
        .clone()
        .oneshot(request(
            "DELETE",
            "/v1/admin/providers/company-llm/active-revision",
            Some("deactivate-invalid"),
            Some("\"provider-2\""),
            Some(json!({})),
        ))
        .await
        .unwrap();
    assert_eq!(nonempty_delete.status(), StatusCode::BAD_REQUEST);

    let suspended = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/providers/company-llm/suspension",
            Some("suspend-1"),
            Some("\"provider-2\""),
            Some(json!({"reason_code":"incident"})),
        ))
        .await
        .unwrap();
    assert_eq!(suspended.status(), StatusCode::OK);
    assert_eq!(suspended.headers()[header::ETAG], "\"provider-3\"");
    let deadline = Instant::now() + Duration::from_secs(2);
    while models.resolve(&selector).is_ok() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        models.resolve(&selector).is_err(),
        "generation polling must converge even when no outbox notification is consumed"
    );
    let resumed = app
        .clone()
        .oneshot(request(
            "DELETE",
            "/v1/admin/providers/company-llm/suspension",
            Some("resume-1"),
            Some("\"provider-3\""),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resumed.status(), StatusCode::OK);
    assert_eq!(resumed.headers()[header::ETAG], "\"provider-4\"");
    let deadline = Instant::now() + Duration::from_secs(2);
    while models.resolve(&selector).is_err() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(models.resolve(&selector).is_ok());
    provider_runtime.shutdown().await;

    let audit_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::from_str(
                temporary
                    .path()
                    .join("provider-management.sqlite3")
                    .to_str()
                    .unwrap(),
            )
            .unwrap()
            .foreign_keys(true),
        )
        .await
        .unwrap();
    let invalid_capabilities: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM provider_management_audit_events
         WHERE capability NOT IN('provider.read','provider.write','provider.discover','provider.test',
                                 'provider.publish','provider.activate','provider.suspend','provider.retire')",
    )
    .fetch_one(&audit_pool)
    .await
    .unwrap();
    assert_eq!(invalid_capabilities, 0);
    let safe_evidence: String = sqlx::query_scalar(
        "SELECT COALESCE(group_concat(event_kind || ':' || capability || ':' || request_id_hash || ':' || COALESCE(before_hash,'') || ':' || COALESCE(after_hash,'')), '') || ':' || COALESCE((SELECT group_concat(safe_payload) FROM provider_management_outbox), '') FROM provider_management_audit_events",
    )
    .fetch_one(&audit_pool)
    .await
    .unwrap();
    for forbidden in [
        "COMPANY_LLM_TOKEN",
        "llm.company.test",
        "chat-v1",
        "secret://",
    ] {
        assert!(!safe_evidence.contains(forbidden));
    }
    for expected in [
        "provider.write",
        "provider.publish",
        "provider.activate",
        "provider.suspend",
    ] {
        assert!(safe_evidence.contains(expected));
    }
    audit_pool.close().await;
}

#[tokio::test]
async fn provider_http_rejects_secret_values_unknown_fields_and_unbounded_probe_payloads() {
    let (temporary, _repository, app) = fixture().await;
    let duplicate_key = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/admin/providers")
                .header(header::AUTHORIZATION, "Bearer operator-token")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-request-id", "duplicate-provider-key")
                .body(Body::from(
                    r#"{"provider_id":"first","provider_id":"second","display_name":"Duplicate","adapter_type":"open_ai_compatible","draft":{"adapter":{"type":"open_ai_compatible"},"endpoint":"https://example.test/v1","credential":{"type":"none"},"models":[]}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate_key.status(), StatusCode::BAD_REQUEST);

    let audit_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::from_str(
                temporary
                    .path()
                    .join("provider-management.sqlite3")
                    .to_str()
                    .unwrap(),
            )
            .unwrap()
            .foreign_keys(true),
        )
        .await
        .unwrap();
    let rejected: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM provider_management_audit_events
         WHERE event_kind='provider.request_rejected' AND result_code='http_400'
           AND capability='provider.write'",
    )
    .fetch_one(&audit_pool)
    .await
    .unwrap();
    assert_eq!(rejected, 1);
    audit_pool.close().await;

    let mut body = create_body();
    body["draft"]["credential"] = json!({"type":"bearer","value":"secret"});
    let response = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/providers",
            Some("bad-secret"),
            None,
            Some(body),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let mut body = create_body();
    body["auto_import"] = json!(true);
    let response = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/providers",
            Some("bad-auto"),
            None,
            Some(body),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let created = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/providers",
            Some("create-probe"),
            None,
            Some(create_body()),
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let response = app
        .oneshot(request(
            "POST",
            "/v1/admin/providers/company-llm/connection-tests",
            Some("bad-probe"),
            None,
            Some(json!({"draft_version":1,"mode":"canary","prompt":"arbitrary"})),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
