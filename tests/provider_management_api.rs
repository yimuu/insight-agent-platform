#[path = "support/database.rs"]
mod database;

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
    managed_openai_adapter_manifest_digest, ManagedProviderRevisionReadiness,
    ProviderManagementRuntime, ProviderManagementRuntimeConfig,
};
use insight_api::v1::{
    build_provider_management_router, OperatorAuth, ProviderManagementApiState,
    ProviderManagementPolicy, PROVIDER_ACTIVATE, PROVIDER_DISCOVER, PROVIDER_PUBLISH,
    PROVIDER_READ, PROVIDER_RETIRE, PROVIDER_SUSPEND, PROVIDER_TEST, PROVIDER_WRITE,
};
use insight_durable::ProviderManagementDurableRepository;
use insight_resources::models::{ModelRegistry, ModelSelector};
use insight_storage::{PostgresDurableRepository, SqliteDurableRepository};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{
    postgres::PgPoolOptions,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    AssertSqlSafe,
};
use tower::ServiceExt;
use uuid::Uuid;

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

fn management_router(repository: Arc<dyn ProviderManagementDurableRepository>) -> Router {
    let auth = OperatorAuth::new([(
        "operator-token".to_owned(),
        "operator-a".to_owned(),
        capabilities(),
    )])
    .unwrap();
    let policy = ProviderManagementPolicy {
        installed_adapters: BTreeMap::from([("open_ai_compatible".to_owned(), "2.1.0".to_owned())]),
        adapter_worker_versions: BTreeMap::from([(
            "open_ai_compatible".to_owned(),
            insight_resources::openai_chat::OPENAI_CHAT_WORKER_VERSION.to_owned(),
        )]),
        adapter_manifest_digests: BTreeMap::from([(
            "open_ai_compatible".to_owned(),
            managed_openai_adapter_manifest_digest(),
        )]),
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
        repository,
        policy: Arc::new(policy),
        readiness: Arc::new(ManagedProviderRevisionReadiness::new(
            ProviderManagementRuntimeConfig {
                enabled: true,
                workers: 1,
                poll_interval: Duration::from_secs(1),
                lease_duration: Duration::from_secs(30),
                retention: chrono::Duration::days(1),
                max_response_bytes: 1024 * 1024,
                allow_loopback_development: false,
                allowed_secret_names: BTreeSet::new(),
            },
        )),
    });
    app
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
    let app = management_router(repository.clone());
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
    assert_eq!(unauthorized.headers()["x-content-type-options"], "nosniff");

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
    assert_eq!(created.headers()["x-content-type-options"], "nosniff");
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
    let runtime_config = ProviderManagementRuntimeConfig {
        enabled: true,
        workers: 1,
        poll_interval: Duration::from_millis(20),
        lease_duration: Duration::from_secs(30),
        retention: chrono::Duration::days(1),
        max_response_bytes: 1024 * 1024,
        allow_loopback_development: false,
        allowed_secret_names: BTreeSet::new(),
    };
    let models_a = ModelRegistry::default();
    let models_b = ModelRegistry::default();
    let mut provider_runtime_a = ProviderManagementRuntime::start(
        repository.clone(),
        models_a.clone(),
        runtime_config.clone(),
    )
    .await
    .unwrap();
    let mut provider_runtime_b = ProviderManagementRuntime::start(
        repository.clone(),
        models_b.clone(),
        runtime_config.clone(),
    )
    .await
    .unwrap();
    let selector = ModelSelector::new("company-llm", "chat-v1").unwrap();
    assert!(models_a.resolve(&selector).is_ok());
    assert!(models_b.resolve(&selector).is_ok());
    let binding_hash = models_a
        .deployment_identity(&selector)
        .unwrap()
        .binding_hash()
        .to_owned();
    assert_eq!(
        models_b
            .deployment_identity(&selector)
            .unwrap()
            .binding_hash(),
        binding_hash
    );

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
    while (models_a.resolve(&selector).is_ok() || models_b.resolve(&selector).is_ok())
        && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        models_a.resolve(&selector).is_err() && models_b.resolve(&selector).is_err(),
        "every runtime must converge by generation polling even without notifications"
    );
    assert!(models_a.resolve_versioned(&selector, &binding_hash).is_ok());
    assert!(models_b.resolve_versioned(&selector, &binding_hash).is_ok());
    provider_runtime_b.shutdown().await;
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
    while models_a.resolve(&selector).is_err() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(models_a.resolve(&selector).is_ok());
    let models_restarted = ModelRegistry::default();
    let mut provider_runtime_restarted = ProviderManagementRuntime::start(
        repository.clone(),
        models_restarted.clone(),
        runtime_config,
    )
    .await
    .unwrap();
    assert!(models_restarted.resolve(&selector).is_ok());
    assert!(models_restarted
        .resolve_versioned(&selector, &binding_hash)
        .is_ok());
    provider_runtime_a.shutdown().await;
    provider_runtime_restarted.shutdown().await;

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
async fn postgres_notifications_converge_two_provider_runtimes_and_restart_recovers_exact_archive()
{
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set TEST_POSTGRES_URL for Provider multi-runtime conformance"
        );
        return;
    };
    let schema = format!("provider_runtime_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    database::provision_postgres_schema(&control).await;
    let repository = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    let app = management_router(repository.clone());

    let created = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/providers",
            Some("pg-create"),
            None,
            Some(create_body()),
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let validation = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/providers/company-llm/validations",
            Some("pg-validate"),
            None,
            Some(json!({"draft_version":1})),
        ))
        .await
        .unwrap();
    let validation_id = json_body(validation).await["data"]["validation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let published = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/providers/company-llm/revisions",
            Some("pg-publish"),
            None,
            Some(json!({"draft_version":1,"validation_id":validation_id})),
        ))
        .await
        .unwrap();
    let revision_id = json_body(published).await["data"]["revision_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let activated = app
        .clone()
        .oneshot(request(
            "PUT",
            "/v1/admin/providers/company-llm/active-revision",
            Some("pg-activate"),
            Some("\"provider-1\""),
            Some(json!({"revision_id":revision_id})),
        ))
        .await
        .unwrap();
    assert_eq!(activated.status(), StatusCode::OK);

    let runtime_config = ProviderManagementRuntimeConfig {
        enabled: true,
        workers: 1,
        poll_interval: Duration::from_secs(30),
        lease_duration: Duration::from_secs(30),
        retention: chrono::Duration::days(1),
        max_response_bytes: 1024 * 1024,
        allow_loopback_development: false,
        allowed_secret_names: BTreeSet::new(),
    };
    let selector = ModelSelector::new("company-llm", "chat-v1").unwrap();
    let models_a = ModelRegistry::default();
    let models_b = ModelRegistry::default();
    let mut runtime_a = ProviderManagementRuntime::start(
        repository.clone(),
        models_a.clone(),
        runtime_config.clone(),
    )
    .await
    .unwrap();
    let mut runtime_b = ProviderManagementRuntime::start(
        repository.clone(),
        models_b.clone(),
        runtime_config.clone(),
    )
    .await
    .unwrap();
    let exact_binding_hash = models_a
        .deployment_identity(&selector)
        .unwrap()
        .binding_hash()
        .to_owned();
    assert!(models_b
        .resolve_versioned(&selector, &exact_binding_hash)
        .is_ok());

    let suspended = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/providers/company-llm/suspension",
            Some("pg-suspend"),
            Some("\"provider-2\""),
            Some(json!({"reason_code":"notification-test"})),
        ))
        .await
        .unwrap();
    assert_eq!(suspended.status(), StatusCode::OK);
    let deadline = Instant::now() + Duration::from_secs(3);
    while (models_a.resolve(&selector).is_ok() || models_b.resolve(&selector).is_ok())
        && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(models_a.resolve(&selector).is_err());
    assert!(models_b.resolve(&selector).is_err());
    assert!(models_a
        .resolve_versioned(&selector, &exact_binding_hash)
        .is_ok());
    runtime_b.shutdown().await;

    let resumed = app
        .oneshot(request(
            "DELETE",
            "/v1/admin/providers/company-llm/suspension",
            Some("pg-resume"),
            Some("\"provider-3\""),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resumed.status(), StatusCode::OK);
    let deadline = Instant::now() + Duration::from_secs(3);
    while models_a.resolve(&selector).is_err() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(models_a.resolve(&selector).is_ok());

    let restarted_models = ModelRegistry::default();
    let mut restarted = ProviderManagementRuntime::start(
        repository.clone(),
        restarted_models.clone(),
        runtime_config,
    )
    .await
    .unwrap();
    assert!(restarted_models.resolve(&selector).is_ok());
    assert!(restarted_models
        .resolve_versioned(&selector, &exact_binding_hash)
        .is_ok());
    runtime_a.shutdown().await;
    restarted.shutdown().await;
    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
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
