#[path = "support/database.rs"]
mod database;

use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    response::Response,
    Router,
};
use futures::StreamExt;
use insight_agent_platform::{
    api::v1::{build_router, ApiAuth, ApiState},
    catalog::{
        compile_agent_dir, compile_enabled_agents, deploy_agents, DeployedAgent,
        DeploymentPersistencePolicy, LeafDeploymentResolver, ResolvedLeafDeployment,
    },
    dsl::CompileError,
    engine::{
        plan::LeafTaskDescriptor,
        repository::{RepositoryError, SqliteDurableRepository, StorageLocator},
        ArtifactRef, ArtifactStoreDeploymentContract, ContentHash, LeafTaskKind,
        LocalContentAddressedArtifactStore, SubflowContractRegistry, WorkerArtifactStore,
        WorkerExecutorRegistry,
    },
    runtime::{
        DeployedAgentCatalog, ProductionRunRepository, RunService, RunServiceConfig,
        TerminalOnlyRunConfig, TerminalOnlyStore,
    },
};
use insight_durable::{CompleteFileCommand, CreateFileCommand, FileDurableRepository, FileQuery};
use insight_engine::{
    file_store::{AuthorizedFile, AuthorizedFileUrl, FileAdmissionAuthority, FileAuthorityError},
    RunId,
};
use insight_storage::{
    terminal_artifact_staging_id, ScopedArtifactReference, TerminalArtifactSourceKind,
    TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE,
};
use serde_json::{json, Value};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    SqlitePool,
};
use tempfile::TempDir;
use tower::ServiceExt;

const AGENT_ID: &str = "terminal_native_passthrough";
const FULL_AGENT_ID: &str = "full_native_passthrough";
const FULL_SUBFLOW_PARENT_AGENT_ID: &str = "full_subflow_parent";
const FULL_SUBFLOW_CHILD_AGENT_ID: &str = "full_subflow_child";
const TENANT_ID: &str = "tenant-terminal-e2e";
const USER_ID: &str = "user-terminal-e2e";
const MAX_HTTP_BODY_BYTES: usize = 2 * 1024 * 1024;

fn text_content(text: &str) -> Value {
    json!([{"text": text}])
}

fn decode_artifact_reference(
    store: &dyn WorkerArtifactStore,
    reference: &str,
) -> (ArtifactRef, StorageLocator) {
    if let Ok(reference) = ScopedArtifactReference::parse(reference) {
        return (reference.artifact().clone(), reference.locator().unwrap());
    }
    let artifact: ArtifactRef = serde_json::from_str(reference).unwrap();
    let locator = store.storage_locator(&artifact).unwrap();
    (artifact, locator)
}

const TERMINAL_NATIVE_PASSTHROUGH: &str = r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: terminal_native_passthrough
  name: Terminal native passthrough
  description: Scheduler-native terminal-only Conversation fixture.
execution:
  persistence_mode: terminal_only
inputs:
  query: string
  messages: {type: "Message[]", default: []}
  files: {type: "File[]", default: []}
output: string
workflow:
  steps:
    - return: $query
"#;

const FULL_NATIVE_PASSTHROUGH: &str = r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: full_native_passthrough
  name: Full native passthrough
  description: Scheduler-native full-persistence Conversation fixture.
execution:
  persistence_mode: full
inputs:
  query: string
  messages: {type: "Message[]", default: []}
  files: {type: "File[]", default: []}
output: string
workflow:
  steps:
    - return: $query
"#;

struct NativeOnlyResolver;

impl LeafDeploymentResolver for NativeOnlyResolver {
    fn resolve_leaf(
        &self,
        _kind: LeafTaskKind,
        _descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        Err(CompileError::new(
            "TEST_UNEXPECTED_LEAF",
            "the native passthrough fixture must not contain worker leaves",
        ))
    }
}

struct DelayedSummaryArtifactStore {
    inner: Arc<dyn WorkerArtifactStore>,
    delay: Duration,
}

struct ConversationFileAuthority {
    public_ready: AtomicBool,
}

impl ConversationFileAuthority {
    fn new() -> Self {
        Self {
            public_ready: AtomicBool::new(true),
        }
    }

    fn tombstone_public_file(&self) {
        self.public_ready.store(false, Ordering::Release);
    }

    fn authorized(file_id: &str) -> AuthorizedFile {
        AuthorizedFile::from_storage_adapter(
            file_id.to_owned(),
            "scan.png".to_owned(),
            "image/png".to_owned(),
            8,
            format!("conversation-test/{file_id}"),
            "etag-conversation-file".to_owned(),
            Some("version-conversation-file".to_owned()),
        )
    }

    fn public_not_found() -> FileAuthorityError {
        FileAuthorityError::from_storage_adapter(
            "FILE_NOT_FOUND",
            "File was not found for this principal",
        )
    }
}

#[async_trait]
impl FileAdmissionAuthority for ConversationFileAuthority {
    async fn resolve_files(
        &self,
        _tenant_id: &str,
        _user_id: &str,
        file_ids: &[String],
    ) -> Result<Vec<AuthorizedFile>, FileAuthorityError> {
        if !self.public_ready.load(Ordering::Acquire) && !file_ids.is_empty() {
            return Err(Self::public_not_found());
        }
        Ok(file_ids
            .iter()
            .map(|file_id| Self::authorized(file_id))
            .collect())
    }

    async fn resolve_conversation_files(
        &self,
        _conversation_id: &str,
        _tenant_id: &str,
        _user_id: &str,
        current_file_ids: &[String],
        history_file_ids: &[String],
    ) -> Result<Vec<AuthorizedFile>, FileAuthorityError> {
        if !self.public_ready.load(Ordering::Acquire) && !current_file_ids.is_empty() {
            return Err(Self::public_not_found());
        }
        Ok(current_file_ids
            .iter()
            .chain(history_file_ids)
            .map(|file_id| Self::authorized(file_id))
            .collect())
    }

    async fn resolve_run_files(
        &self,
        _run_id: &RunId,
        file_ids: &[String],
    ) -> Result<Vec<AuthorizedFile>, FileAuthorityError> {
        Ok(file_ids
            .iter()
            .map(|file_id| Self::authorized(file_id))
            .collect())
    }

    async fn read_file(
        &self,
        _file: &AuthorizedFile,
        _max_bytes: usize,
    ) -> Result<Vec<u8>, FileAuthorityError> {
        unreachable!("the passthrough fixture does not dispatch file bytes")
    }

    async fn presign_file_read(
        &self,
        _file: &AuthorizedFile,
    ) -> Result<AuthorizedFileUrl, FileAuthorityError> {
        unreachable!("the passthrough fixture does not dispatch file URLs")
    }
}

#[async_trait]
impl WorkerArtifactStore for DelayedSummaryArtifactStore {
    fn inline_threshold_bytes(&self) -> usize {
        self.inner.inline_threshold_bytes()
    }

    fn deployment_contract(&self) -> ArtifactStoreDeploymentContract {
        self.inner.deployment_contract()
    }

    fn storage_locator(&self, artifact: &ArtifactRef) -> Result<StorageLocator, RepositoryError> {
        self.inner.storage_locator(artifact)
    }

    async fn put_and_verify(
        &self,
        artifact: &ArtifactRef,
        bytes: &[u8],
    ) -> Result<(ContentHash, u64), RepositoryError> {
        let summary = serde_json::from_slice::<Value>(bytes)
            .ok()
            .and_then(|value| {
                value
                    .get("scope")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .is_some_and(|scope| scope.contains(":conversation-summary:"));
        if summary {
            tokio::time::sleep(self.delay).await;
        }
        self.inner.put_and_verify(artifact, bytes).await
    }

    async fn read_and_verify(
        &self,
        artifact: &ArtifactRef,
        locator: &StorageLocator,
        max_bytes: usize,
    ) -> Result<Vec<u8>, RepositoryError> {
        self.inner
            .read_and_verify(artifact, locator, max_bytes)
            .await
    }

    async fn delete(
        &self,
        artifact: &ArtifactRef,
        locator: &StorageLocator,
    ) -> Result<(), RepositoryError> {
        self.inner.delete(artifact, locator).await
    }
}

struct TerminalHttpFixture {
    app: Router,
    service: RunService,
    repository: Arc<SqliteDurableRepository>,
    file_authority: Arc<ConversationFileAuthority>,
    inspection: SqlitePool,
    artifact_store: Arc<dyn WorkerArtifactStore>,
    _root: TempDir,
}

impl TerminalHttpFixture {
    async fn start() -> Self {
        Self::start_with_summary_policy(100, 8).await
    }

    async fn start_full_only() -> Self {
        Self::start_with_runtime_options(100, 8, false).await
    }

    async fn start_with_summary_policy(
        summary_trigger_messages: u32,
        recent_context_messages: u32,
    ) -> Self {
        Self::start_with_runtime_options(summary_trigger_messages, recent_context_messages, true)
            .await
    }

    async fn start_with_runtime_options(
        summary_trigger_messages: u32,
        recent_context_messages: u32,
        terminal_only_enabled: bool,
    ) -> Self {
        Self::start_with_runtime_options_and_summary_artifact_delay(
            summary_trigger_messages,
            recent_context_messages,
            terminal_only_enabled,
            Duration::ZERO,
        )
        .await
    }

    async fn start_with_runtime_options_and_summary_artifact_delay(
        summary_trigger_messages: u32,
        recent_context_messages: u32,
        terminal_only_enabled: bool,
        summary_artifact_delay: Duration,
    ) -> Self {
        let root = tempfile::tempdir().unwrap();
        let agent_directory = root.path().join("agent");
        std::fs::create_dir_all(&agent_directory).unwrap();
        std::fs::write(
            agent_directory.join("agent.yaml"),
            TERMINAL_NATIVE_PASSTHROUGH,
        )
        .unwrap();
        let published = Arc::new(compile_agent_dir(&agent_directory).unwrap());
        let terminal_deployed = Arc::new(
            DeployedAgent::publish_with_persistence_policy(
                published,
                &NativeOnlyResolver,
                SubflowContractRegistry::new(),
                DeploymentPersistencePolicy::terminal_only(false, Duration::from_secs(10)).unwrap(),
            )
            .unwrap(),
        );
        let full_agent_directory = root.path().join("full-agent");
        std::fs::create_dir_all(&full_agent_directory).unwrap();
        std::fs::write(
            full_agent_directory.join("agent.yaml"),
            FULL_NATIVE_PASSTHROUGH,
        )
        .unwrap();
        let full_published = Arc::new(compile_agent_dir(&full_agent_directory).unwrap());
        let full_deployed = Arc::new(
            DeployedAgent::publish_with_persistence_policy(
                full_published,
                &NativeOnlyResolver,
                SubflowContractRegistry::new(),
                DeploymentPersistencePolicy::full(),
            )
            .unwrap(),
        );
        let subflow_catalog_directory = root.path().join("subflow-catalog");
        let subflow_child_directory = subflow_catalog_directory.join(FULL_SUBFLOW_CHILD_AGENT_ID);
        std::fs::create_dir_all(&subflow_child_directory).unwrap();
        std::fs::write(
            subflow_child_directory.join("agent.yaml"),
            format!(
                r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: {FULL_SUBFLOW_CHILD_AGENT_ID}
  name: Full subflow child
  description: Child used to prove full Conversation subflow rejection.
execution:
  persistence_mode: full
inputs: {{query: string}}
output: string
workflow:
  steps:
    - return: $query
"#
            ),
        )
        .unwrap();
        let subflow_child = compile_agent_dir(&subflow_child_directory).unwrap();
        let subflow_parent_directory = subflow_catalog_directory.join(FULL_SUBFLOW_PARENT_AGENT_ID);
        std::fs::create_dir_all(&subflow_parent_directory).unwrap();
        std::fs::write(
            subflow_parent_directory.join("agent.yaml"),
            format!(
                r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: {FULL_SUBFLOW_PARENT_AGENT_ID}
  name: Full subflow parent
  description: Full parent whose Conversation admission must be rejected.
execution:
  persistence_mode: full
inputs:
  query: string
  messages: {{type: "Message[]", default: []}}
output: string
workflow:
  steps:
    - id: child_answer
      type: call
      definition_revision: {}
      interface_version: child-v1
      input: {{query: $query}}
      response: string
    - return: $child_answer
"#,
                subflow_child.definition_revision_id()
            ),
        )
        .unwrap();
        let subflow_enabled = BTreeSet::from([
            FULL_SUBFLOW_CHILD_AGENT_ID.to_owned(),
            FULL_SUBFLOW_PARENT_AGENT_ID.to_owned(),
        ]);
        let subflow_catalog =
            compile_enabled_agents(&subflow_catalog_directory, &subflow_enabled).unwrap();
        let mut deployed = vec![terminal_deployed, full_deployed];
        deployed.extend(deploy_agents(&subflow_catalog, &NativeOnlyResolver).unwrap());
        let agents = DeployedAgentCatalog::new(deployed).unwrap();

        let database_path = root.path().join("terminal.sqlite3");
        database::provision_sqlite_database(&database_path).await;
        let repository = Arc::new(
            SqliteDurableRepository::connect_path(&database_path)
                .await
                .unwrap(),
        );
        let production_repository: Arc<dyn ProductionRunRepository> = repository.clone();
        let terminal_store: Arc<dyn TerminalOnlyStore> = repository.clone();
        let base_artifact_store: Arc<dyn WorkerArtifactStore> = Arc::new(
            LocalContentAddressedArtifactStore::open(root.path().join("objects"), 4)
                .await
                .unwrap(),
        );
        let artifact_store: Arc<dyn WorkerArtifactStore> = if summary_artifact_delay.is_zero() {
            base_artifact_store
        } else {
            Arc::new(DelayedSummaryArtifactStore {
                inner: base_artifact_store,
                delay: summary_artifact_delay,
            })
        };
        let mut service_config = RunServiceConfig::single_process_development(8, 2, 1, 32);
        service_config.pump_interval = Duration::from_millis(10);
        service_config.run_timeout = Duration::from_secs(10);
        service_config.terminal_barrier_timeout = Duration::from_secs(2);
        service_config.outbound_write_timeout = Duration::from_secs(2);
        service_config = service_config.with_graph_persistence_policy(
            DeploymentPersistencePolicy::terminal_only(false, Duration::from_secs(10)).unwrap(),
        );
        let service = RunService::start_with_artifact_store(
            agents,
            production_repository,
            WorkerExecutorRegistry::new(),
            Arc::clone(&artifact_store),
            service_config,
        )
        .await
        .unwrap();
        let file_authority = Arc::new(ConversationFileAuthority::new());
        service
            .set_file_admission_authority(file_authority.clone())
            .unwrap();
        let conversation_config = TerminalOnlyRunConfig {
            owner_lease: Duration::from_secs(30),
            owner_heartbeat: Duration::from_secs(5),
            terminal_commit_retry: Duration::from_secs(1),
            run_timeout: Duration::from_secs(10),
            max_concurrent_runs: 8,
            conversations_enabled: true,
            inline_content_max_bytes: 256,
            message_page_size_default: 1,
            message_page_size_max: 2,
            recent_context_messages,
            summary_trigger_messages,
            summary_trigger_tokens: 100_000,
            run_retention: Duration::from_secs(24 * 60 * 60),
            conversation_retention: Duration::from_secs(24 * 60 * 60),
            terminal_barrier_timeout: Duration::from_secs(2),
            outbound_write_timeout: Duration::from_secs(2),
        };
        if terminal_only_enabled {
            service
                .enable_terminal_only(
                    terminal_store,
                    "http://terminal-only.test".to_owned(),
                    conversation_config,
                )
                .await
                .unwrap();
        } else {
            service
                .enable_conversations(terminal_store, conversation_config)
                .unwrap();
        }

        let inspection = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&database_path)
                    .create_if_missing(false)
                    .foreign_keys(true),
            )
            .await
            .unwrap();
        let app = build_router(ApiState {
            service: service.clone(),
            auth: ApiAuth::disabled(),
            sse_keep_alive_interval: Duration::from_secs(30),
            readiness_probe_timeout: Duration::from_secs(1),
        });
        Self {
            app,
            service,
            repository,
            file_authority,
            inspection,
            artifact_store,
            _root: root,
        }
    }

    async fn shutdown(self) {
        self.service.shutdown(Duration::from_secs(2)).await.unwrap();
        self.inspection.close().await;
    }
}

fn conversation_read(path: impl Into<String>, tenant_id: &str, user_id: &str) -> Request<Body> {
    Request::get(path.into())
        .header("x-tenant-id", tenant_id)
        .header("x-user-id", user_id)
        .body(Body::empty())
        .unwrap()
}

fn conversation_write(
    method: Method,
    path: impl Into<String>,
    tenant_id: &str,
    user_id: &str,
    request_id: &str,
    body: Value,
) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path.into())
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-tenant-id", tenant_id)
        .header("x-user-id", user_id)
        .header("x-request-id", request_id)
        .header("idempotency-key", request_id)
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn conversation_delete(
    path: impl Into<String>,
    tenant_id: &str,
    user_id: &str,
    request_id: &str,
) -> Request<Body> {
    Request::builder()
        .method(Method::DELETE)
        .uri(path.into())
        .header("x-tenant-id", tenant_id)
        .header("x-user-id", user_id)
        .header("x-request-id", request_id)
        .body(Body::empty())
        .unwrap()
}

async fn send(app: &Router, request: Request<Body>) -> Response {
    app.clone().oneshot(request).await.unwrap()
}

async fn json_body(response: Response) -> Value {
    let bytes = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn message_page(
    app: &Router,
    conversation_id: &str,
    tenant_id: &str,
    user_id: &str,
    query: &str,
) -> (StatusCode, Value) {
    let response = send(
        app,
        conversation_read(
            format!("/v1/conversations/{conversation_id}/messages{query}"),
            tenant_id,
            user_id,
        ),
    )
    .await;
    let status = response.status();
    (status, json_body(response).await)
}

async fn wait_for_assistant(app: &Router, conversation_id: &str, run_id: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (status, page) =
            message_page(app, conversation_id, TENANT_ID, USER_ID, "?limit=2").await;
        assert_eq!(status, StatusCode::OK, "{page}");
        if page["data"]["messages"].as_array().is_some_and(|messages| {
            messages
                .iter()
                .any(|message| message["role"] == "assistant" && message["run_id"] == run_id)
        }) {
            return page;
        }
        assert!(
            Instant::now() < deadline,
            "Conversation Run {run_id} did not publish an assistant message: {page}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn prometheus_sample(metrics: &str, sample: &str) -> u64 {
    metrics
        .lines()
        .find_map(|line| {
            line.strip_prefix(sample)
                .and_then(|value| value.strip_prefix(' '))
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or_else(|| panic!("missing Prometheus sample {sample}: {metrics}"))
}

async fn wait_for_terminal_run(app: &Router, run_id: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let response = send(
            app,
            Request::get(format!("/v1/runs/{run_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        if matches!(
            body["data"]["status"].as_str(),
            Some("completed" | "failed" | "cancelled" | "interrupted" | "timed_out")
        ) {
            return body["data"].clone();
        }
        assert!(
            Instant::now() < deadline,
            "terminal-only Run {run_id} did not finish: {body}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn assert_terminal_capability(value: &Value) {
    assert_eq!(value["persistence_mode"], "terminal_only");
    assert_eq!(value["recovery_capability"], "none");
    assert_eq!(value["event_replay"], false);
    if value.get("id").is_some() {
        assert_eq!(value["volatile_waits_enabled"], false);
        assert_eq!(value["wait_recovery"], "none");
    }
}

#[tokio::test]
async fn active_terminal_admission_is_tenant_scoped_and_historical_routes_fail_closed() {
    let fixture = TerminalHttpFixture::start().await;
    let terminal_revision = fixture
        .service
        .agents()
        .get(AGENT_ID)
        .unwrap()
        .deployment_revision_id()
        .as_str()
        .to_owned();
    let full_revision = fixture
        .service
        .agents()
        .get(FULL_AGENT_ID)
        .unwrap()
        .deployment_revision_id()
        .as_str()
        .to_owned();
    let pinned_body = json!({"query": "pinned"});
    let pinned_path = format!("/v1/agents/{AGENT_ID}/deployments/{terminal_revision}/runs");

    // Historical Deployment admission was removed by the managed-Agent
    // clean cut. Ordinary clients can only resolve the durable active head.
    let historical_terminal = send(
        &fixture.app,
        Request::post(&pinned_path)
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-tenant-id", "tenant-pinned-a")
            .header("x-request-id", "historical-terminal-request")
            .body(Body::from(serde_json::to_vec(&pinned_body).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(historical_terminal.status(), StatusCode::NOT_FOUND);

    let active_path = format!("/v1/agents/{AGENT_ID}/runs");

    let tenant_a = send(
        &fixture.app,
        Request::post(&active_path)
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-tenant-id", "tenant-pinned-a")
            .header("x-request-id", "same-pinned-request")
            .header("idempotency-key", "same-pinned-operation")
            .body(Body::from(serde_json::to_vec(&pinned_body).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(tenant_a.status(), StatusCode::ACCEPTED);
    let run_a = tenant_a.headers()["x-run-id"].to_str().unwrap().to_owned();
    let tenant_b = send(
        &fixture.app,
        Request::post(&active_path)
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-tenant-id", "tenant-pinned-b")
            .header("x-request-id", "same-pinned-request")
            .header("idempotency-key", "same-pinned-operation")
            .body(Body::from(serde_json::to_vec(&pinned_body).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(tenant_b.status(), StatusCode::ACCEPTED);
    let run_b = tenant_b.headers()["x-run-id"].to_str().unwrap().to_owned();
    assert_ne!(run_a, run_b, "idempotency key is tenant-scoped");
    assert_eq!(
        send(
            &fixture.app,
            Request::get(format!("/v1/runs/{run_a}"))
                .header("x-tenant-id", "tenant-pinned-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );

    let full_pinned_path = format!("/v1/agents/{FULL_AGENT_ID}/deployments/{full_revision}/runs");
    let rejected_full = send(
        &fixture.app,
        Request::post(&full_pinned_path)
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-tenant-id", "tenant-unbound")
            .header("x-request-id", "full-pinned-nondefault")
            .body(Body::from(serde_json::to_vec(&pinned_body).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(rejected_full.status(), StatusCode::NOT_FOUND);
    for (path, expected_status) in [
        (
            format!("/v1/agents/{FULL_AGENT_ID}/runs"),
            StatusCode::ACCEPTED,
        ),
        (
            format!("/v1/agents/{FULL_AGENT_ID}/runs/stream"),
            StatusCode::OK,
        ),
    ] {
        let admitted = send(
            &fixture.app,
            Request::post(path)
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-tenant-id", "tenant-unbound")
                .header("x-request-id", "full-unbound-nondefault")
                .body(Body::from(serde_json::to_vec(&pinned_body).unwrap()))
                .unwrap(),
        )
        .await;
        assert_eq!(admitted.status(), expected_status);
        let admitted_run_id = admitted.headers()["x-run-id"].to_str().unwrap().to_owned();
        assert_eq!(
            send(
                &fixture.app,
                Request::get(format!("/v1/runs/{admitted_run_id}"))
                    .header("x-tenant-id", "tenant-unbound")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .status(),
            StatusCode::OK
        );
        assert_eq!(
            send(
                &fixture.app,
                Request::get(format!("/v1/runs/{admitted_run_id}"))
                    .header("x-tenant-id", "another-tenant")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
        let _ = to_bytes(admitted.into_body(), MAX_HTTP_BODY_BYTES)
            .await
            .unwrap();
    }

    let default_full = send(
        &fixture.app,
        Request::post(format!("/v1/agents/{FULL_AGENT_ID}/runs"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-request-id", "full-pinned-default")
            .body(Body::from(serde_json::to_vec(&pinned_body).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(default_full.status(), StatusCode::ACCEPTED);
    let full_run_id = default_full.headers()["x-run-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        send(
            &fixture.app,
            Request::get(format!("/v1/runs/{full_run_id}"))
                .header("x-tenant-id", "tenant-unbound")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &fixture.app,
            Request::delete(format!("/v1/runs/{full_run_id}"))
                .header("x-tenant-id", "tenant-unbound")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        fixture
            .service
            .run_persistence_capability("tenant-unbound", &full_run_id)
            .await
            .unwrap_err()
            .code(),
        "RUN_NOT_FOUND"
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn privacy_delete_cancels_unconsumed_terminal_and_full_attached_streams() {
    let fixture = TerminalHttpFixture::start().await;
    for (agent_id, suffix) in [(AGENT_ID, "terminal"), (FULL_AGENT_ID, "full")] {
        let tenant_id = format!("tenant-stream-delete-{suffix}");
        let user_id = format!("user-stream-delete-{suffix}");
        let marker = format!("private-unconsumed-stream-marker-{suffix}");
        let created = send(
            &fixture.app,
            conversation_write(
                Method::POST,
                "/v1/conversations",
                &tenant_id,
                &user_id,
                &format!("create-stream-delete-{suffix}"),
                json!({"agent_id": agent_id}),
            ),
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let conversation_id = json_body(created).await["data"]["conversation_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let stream = send(
            &fixture.app,
            conversation_write(
                Method::POST,
                format!("/v1/conversations/{conversation_id}/messages/stream"),
                &tenant_id,
                &user_id,
                &format!("turn-stream-delete-{suffix}"),
                json!({"query": marker}),
            ),
        )
        .await;
        assert_eq!(stream.status(), StatusCode::OK);

        let deleted = tokio::time::timeout(
            Duration::from_secs(2),
            send(
                &fixture.app,
                conversation_delete(
                    format!("/v1/conversations/{conversation_id}"),
                    &tenant_id,
                    &user_id,
                    &format!("delete-stream-{suffix}"),
                ),
            ),
        )
        .await
        .expect("privacy DELETE was blocked by an unconsumed attached stream");
        assert_eq!(deleted.status(), StatusCode::OK);
        assert_eq!(json_body(deleted).await["data"]["deleted"], true);

        let bytes = tokio::time::timeout(
            Duration::from_secs(1),
            to_bytes(stream.into_body(), MAX_HTTP_BODY_BYTES),
        )
        .await
        .expect("cancelled Conversation stream did not reach EOF")
        .unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(
            !body.contains(&marker),
            "private output remained observable after DELETE: {body}"
        );
    }

    fixture.shutdown().await;
}

#[tokio::test]
async fn privacy_delete_cancels_unconsumed_finite_bodies_and_redacts_future_reads() {
    let fixture = TerminalHttpFixture::start().await;
    for (agent_id, suffix) in [(AGENT_ID, "terminal"), (FULL_AGENT_ID, "full")] {
        let marker = format!("private-finite-response-marker-{suffix}");
        let created = send(
            &fixture.app,
            conversation_write(
                Method::POST,
                "/v1/conversations",
                TENANT_ID,
                USER_ID,
                &format!("create-finite-response-{suffix}"),
                json!({"agent_id": agent_id}),
            ),
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let conversation_id = json_body(created).await["data"]["conversation_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let turn = send(
            &fixture.app,
            conversation_write(
                Method::POST,
                format!("/v1/conversations/{conversation_id}/messages"),
                TENANT_ID,
                USER_ID,
                &format!("turn-finite-response-{suffix}"),
                json!({"query": marker}),
            ),
        )
        .await;
        assert_eq!(turn.status(), StatusCode::ACCEPTED);
        let run_id = turn.headers()["x-run-id"].to_str().unwrap().to_owned();
        let _ = json_body(turn).await;
        let _ = wait_for_assistant(&fixture.app, &conversation_id, &run_id).await;

        let held_messages = send(
            &fixture.app,
            conversation_read(
                format!("/v1/conversations/{conversation_id}/messages?limit=2"),
                TENANT_ID,
                USER_ID,
            ),
        )
        .await;
        assert_eq!(held_messages.status(), StatusCode::OK);
        let held_run = send(
            &fixture.app,
            Request::get(format!("/v1/runs/{run_id}"))
                .header("x-tenant-id", TENANT_ID)
                .header("x-user-id", USER_ID)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(held_run.status(), StatusCode::OK);
        let held_content_visibility = fixture
            .service
            .acquire_run_content_response_visibility(TENANT_ID, Some(USER_ID), &run_id)
            .await
            .unwrap()
            .expect("Conversation-bound content surface did not receive a privacy read lease");

        let app = fixture.app.clone();
        let delete_request = conversation_delete(
            format!("/v1/conversations/{conversation_id}"),
            TENANT_ID,
            USER_ID,
            &format!("delete-finite-response-{suffix}"),
        );
        let mut deletion = tokio::spawn(async move { send(&app, delete_request).await });
        let deleted = tokio::time::timeout(Duration::from_secs(2), &mut deletion)
            .await
            .expect("unconsumed finite response bodies blocked privacy DELETE")
            .unwrap();
        assert!(
            held_content_visibility.is_cancelled(),
            "privacy DELETE did not cancel an idle content visibility lease"
        );
        drop(held_content_visibility);
        assert_eq!(deleted.status(), StatusCode::OK);
        assert_eq!(json_body(deleted).await["data"]["deleted"], true);

        let held_messages = to_bytes(held_messages.into_body(), MAX_HTTP_BODY_BYTES)
            .await
            .unwrap();
        assert!(
            held_messages.is_empty(),
            "an authorized message body remained deliverable after privacy DELETE"
        );
        assert!(!String::from_utf8_lossy(&held_messages).contains(&marker));
        let held_run = to_bytes(held_run.into_body(), MAX_HTTP_BODY_BYTES)
            .await
            .unwrap();
        assert!(
            held_run.is_empty(),
            "an authorized Run body remained deliverable after privacy DELETE"
        );
        assert!(!String::from_utf8_lossy(&held_run).contains(&marker));

        assert_eq!(
            send(
                &fixture.app,
                conversation_read(
                    format!("/v1/conversations/{conversation_id}/messages"),
                    TENANT_ID,
                    USER_ID,
                ),
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );

        let after_delete = send(
            &fixture.app,
            Request::get(format!("/v1/runs/{run_id}"))
                .header("x-tenant-id", TENANT_ID)
                .header("x-user-id", USER_ID)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        if agent_id == FULL_AGENT_ID {
            assert_eq!(after_delete.status(), StatusCode::OK);
            assert!(
                !json_body(after_delete).await.to_string().contains(&marker),
                "full Run redaction exposed deleted Conversation content"
            );
        } else {
            assert_eq!(after_delete.status(), StatusCode::NOT_FOUND);
        }
    }

    fixture.shutdown().await;
}

#[tokio::test]
async fn full_conversation_visibility_guard_fences_privacy_delete_and_checks_principal() {
    let fixture = TerminalHttpFixture::start_full_only().await;
    let conversation = fixture
        .service
        .create_conversation(
            "tenant-full-guard",
            "user-full-guard",
            FULL_AGENT_ID,
            "create-full-guard",
        )
        .await
        .unwrap();
    let turn = fixture
        .service
        .create_conversation_detached_turn(
            &conversation.conversation_id,
            "tenant-full-guard",
            "user-full-guard",
            "turn-full-guard",
            "request-full-guard",
            serde_json::from_value(json!({"query": "private full terminal"})).unwrap(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let run = fixture
            .service
            .get_run_for_principal(
                "tenant-full-guard",
                Some("user-full-guard"),
                &turn.run.run_id,
            )
            .await
            .unwrap();
        if run.status().is_terminal() {
            break;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(fixture
        .service
        .acquire_visible_full_conversation_run(
            &conversation.conversation_id,
            "tenant-other",
            "user-full-guard",
            &turn.run.run_id,
        )
        .await
        .is_err());
    assert!(fixture
        .service
        .acquire_visible_full_conversation_run(
            &conversation.conversation_id,
            "tenant-full-guard",
            "user-other",
            &turn.run.run_id,
        )
        .await
        .is_err());

    let guard = fixture
        .service
        .acquire_visible_full_conversation_run(
            &conversation.conversation_id,
            "tenant-full-guard",
            "user-full-guard",
            &turn.run.run_id,
        )
        .await
        .unwrap();
    let service = fixture.service.clone();
    let conversation_id = conversation.conversation_id.clone();
    let mut deletion = tokio::spawn(async move {
        service
            .delete_conversation(&conversation_id, "tenant-full-guard", "user-full-guard")
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut deletion)
            .await
            .is_err(),
        "full privacy DELETE must wait for terminal SSE publication"
    );
    drop(guard);
    assert!(deletion.await.unwrap().unwrap());
    assert!(fixture
        .service
        .acquire_visible_full_conversation_run(
            &conversation.conversation_id,
            "tenant-full-guard",
            "user-full-guard",
            &turn.run.run_id,
        )
        .await
        .is_err());

    fixture.shutdown().await;
}

#[tokio::test]
async fn terminal_only_http_conversation_lifecycle_is_isolated_idempotent_and_cursor_paged() {
    let fixture = TerminalHttpFixture::start().await;

    let discovery = send(
        &fixture.app,
        Request::get(format!("/v1/agents/{AGENT_ID}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(discovery.status(), StatusCode::OK);
    assert_terminal_capability(&json_body(discovery).await["data"]);

    let standalone = send(
        &fixture.app,
        Request::post(format!("/v1/agents/{AGENT_ID}/runs"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-request-id", "standalone-terminal-run")
            .body(Body::from(
                serde_json::to_vec(&json!({"query": "standalone"})).unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(standalone.status(), StatusCode::ACCEPTED);
    let standalone_run_id = standalone.headers()["x-run-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let standalone_body = json_body(standalone).await;
    assert_terminal_capability(&standalone_body["data"]);
    let terminal_standalone = wait_for_terminal_run(&fixture.app, &standalone_run_id).await;
    assert_eq!(
        terminal_standalone["status"], "completed",
        "{terminal_standalone}"
    );
    assert_terminal_capability(&terminal_standalone);

    let tenant_standalone = send(
        &fixture.app,
        Request::post(format!("/v1/agents/{AGENT_ID}/runs"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-request-id", "standalone-terminal-run-tenant")
            .header("x-tenant-id", TENANT_ID)
            .body(Body::from(
                serde_json::to_vec(&json!({"query": "tenant standalone"})).unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(tenant_standalone.status(), StatusCode::ACCEPTED);
    let tenant_standalone_id = tenant_standalone.headers()["x-run-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        send(
            &fixture.app,
            Request::get(format!("/v1/runs/{tenant_standalone_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &fixture.app,
            Request::get(format!("/v1/runs/{tenant_standalone_id}"))
                .header("x-tenant-id", TENANT_ID)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::OK
    );

    let pause = send(
        &fixture.app,
        Request::post(format!("/v1/runs/{standalone_run_id}/pause"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(pause.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        json_body(pause).await,
        json!({
            "error": {
                "code": "RUN_CAPABILITY_UNAVAILABLE",
                "message": "this run does not persist recovery checkpoints"
            }
        })
    );

    let created = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            "/v1/conversations",
            TENANT_ID,
            USER_ID,
            "conversation-create",
            json!({"agent_id": AGENT_ID}),
        ),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);
    assert_eq!(created.headers()["x-request-id"], "conversation-create");
    let created_body = json_body(created).await;
    let conversation_id = created_body["data"]["conversation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(created_body["data"]["tenant_id"], TENANT_ID);
    assert_eq!(created_body["data"]["user_id"], USER_ID);
    assert_eq!(created_body["data"]["agent_id"], AGENT_ID);

    let fetched = send(
        &fixture.app,
        conversation_read(
            format!("/v1/conversations/{conversation_id}"),
            TENANT_ID,
            USER_ID,
        ),
    )
    .await;
    assert_eq!(fetched.status(), StatusCode::OK);
    assert_eq!(fetched.headers()[header::CACHE_CONTROL], "no-store");
    assert_eq!(
        json_body(fetched).await["data"]["conversation_id"],
        conversation_id
    );

    let wrong_tenant = send(
        &fixture.app,
        conversation_read(
            format!("/v1/conversations/{conversation_id}"),
            "tenant-other",
            USER_ID,
        ),
    )
    .await;
    assert_eq!(wrong_tenant.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        json_body(wrong_tenant).await["code"],
        "CONVERSATION_NOT_FOUND"
    );
    let (wrong_user_status, wrong_user) = message_page(
        &fixture.app,
        &conversation_id,
        TENANT_ID,
        "user-other",
        "?limit=2",
    )
    .await;
    assert_eq!(wrong_user_status, StatusCode::NOT_FOUND);
    assert_eq!(wrong_user["code"], "CONVERSATION_NOT_FOUND");

    let large_message = format!("large-object-message:{}", "x".repeat(700));
    let first_turn = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            "conversation-turn-1",
            json!({"query": large_message}),
        ),
    )
    .await;
    assert_eq!(first_turn.status(), StatusCode::ACCEPTED);
    let first_run_id = first_turn.headers()["x-run-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let first_message_id = first_turn.headers()["x-message-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let first_turn_body = json_body(first_turn).await;
    assert_eq!(
        first_turn_body["data"]["user_message"]["content"],
        text_content(&large_message)
    );
    assert_eq!(
        first_turn_body["data"]["user_message"]["message_id"],
        first_message_id
    );
    assert_eq!(first_turn_body["data"]["replayed"], false);
    assert_terminal_capability(&first_turn_body["data"]["run"]);

    let first_page = wait_for_assistant(&fixture.app, &conversation_id, &first_run_id).await;
    let first_messages = first_page["data"]["messages"].as_array().unwrap();
    assert_eq!(first_messages.len(), 2);
    let first_user = first_messages
        .iter()
        .find(|message| message["role"] == "user")
        .unwrap();
    let first_assistant = first_messages
        .iter()
        .find(|message| message["role"] == "assistant")
        .unwrap();
    assert_eq!(first_user["content"], text_content(&large_message));
    assert_eq!(first_assistant["content"], text_content(&large_message));
    assert!(first_user.get("storage").is_none());
    assert!(first_assistant.get("storage").is_none());
    assert!(
        first_user["message_order"].as_i64().unwrap()
            < first_assistant["message_order"].as_i64().unwrap()
    );

    let custom_tenant_run_without_tenant = send(
        &fixture.app,
        Request::get(format!("/v1/runs/{first_run_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        custom_tenant_run_without_tenant.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &fixture.app,
            Request::get(format!("/v1/runs/{first_run_id}"))
                .header("x-tenant-id", TENANT_ID)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    let custom_tenant_run = send(
        &fixture.app,
        Request::get(format!("/v1/runs/{first_run_id}"))
            .header("x-tenant-id", TENANT_ID)
            .header("x-user-id", USER_ID)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(custom_tenant_run.status(), StatusCode::OK);
    let custom_tenant_run = json_body(custom_tenant_run).await;
    assert_eq!(custom_tenant_run["data"]["status"], "completed");
    assert_eq!(custom_tenant_run["data"]["output"]["data"], large_message);
    assert_terminal_capability(&custom_tenant_run["data"]);
    let wrong_tenant_run = send(
        &fixture.app,
        Request::get(format!("/v1/runs/{first_run_id}"))
            .header("x-tenant-id", "tenant-other")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(wrong_tenant_run.status(), StatusCode::NOT_FOUND);
    let wrong_user_run = send(
        &fixture.app,
        Request::get(format!("/v1/runs/{first_run_id}"))
            .header("x-tenant-id", TENANT_ID)
            .header("x-user-id", "user-other")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(wrong_user_run.status(), StatusCode::NOT_FOUND);

    let custom_tenant_pause = send(
        &fixture.app,
        Request::post(format!("/v1/runs/{first_run_id}/pause"))
            .header("x-tenant-id", TENANT_ID)
            .header("x-user-id", USER_ID)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        custom_tenant_pause.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        json_body(custom_tenant_pause).await["error"]["code"],
        "RUN_CAPABILITY_UNAVAILABLE"
    );
    let custom_tenant_cancel = send(
        &fixture.app,
        Request::builder()
            .method(Method::DELETE)
            .uri(format!("/v1/runs/{first_run_id}"))
            .header("x-tenant-id", TENANT_ID)
            .header("x-user-id", USER_ID)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(custom_tenant_cancel.status(), StatusCode::OK);
    assert_eq!(
        json_body(custom_tenant_cancel).await["data"]["output"]["data"],
        large_message
    );

    let replay = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            "conversation-turn-1",
            json!({"query": large_message}),
        ),
    )
    .await;
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    assert_eq!(replay.headers()["x-run-id"], first_run_id);
    assert_eq!(replay.headers()["x-message-id"], first_message_id);
    let replay_body = json_body(replay).await;
    assert_eq!(replay_body["data"]["replayed"], true);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM conversation_messages WHERE conversation_id=?"
        )
        .bind(&conversation_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        2
    );

    let second_turn = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            "conversation-turn-2",
            json!({"query": "second turn"}),
        ),
    )
    .await;
    assert_eq!(second_turn.status(), StatusCode::ACCEPTED);
    let second_run_id = second_turn.headers()["x-run-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let second_turn_body = json_body(second_turn).await;
    assert_terminal_capability(&second_turn_body["data"]["run"]);
    wait_for_assistant(&fixture.app, &conversation_id, &second_run_id).await;

    let (first_page_status, first_cursor_page) = message_page(
        &fixture.app,
        &conversation_id,
        TENANT_ID,
        USER_ID,
        "?limit=2",
    )
    .await;
    assert_eq!(first_page_status, StatusCode::OK);
    let cursor = first_cursor_page["data"]["next_cursor"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(!cursor.contains('{'));
    assert!(!cursor.contains('='));
    let (second_page_status, second_cursor_page) = message_page(
        &fixture.app,
        &conversation_id,
        TENANT_ID,
        USER_ID,
        &format!("?limit=2&cursor={cursor}"),
    )
    .await;
    assert_eq!(second_page_status, StatusCode::OK);
    assert!(second_cursor_page["data"]["next_cursor"].is_null());
    let message_ids = first_cursor_page["data"]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .chain(
            second_cursor_page["data"]["messages"]
                .as_array()
                .unwrap()
                .iter(),
        )
        .map(|message| message["message_id"].as_str().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(message_ids.len(), 4);

    let archived = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/archive"),
            TENANT_ID,
            USER_ID,
            "conversation-archive",
            json!({}),
        ),
    )
    .await;
    assert_eq!(archived.status(), StatusCode::OK);
    assert!(!json_body(archived).await["data"]["archived_at"].is_null());
    let archived_turn = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            "conversation-turn-after-archive",
            json!({"query": "must be rejected"}),
        ),
    )
    .await;
    assert_eq!(archived_turn.status(), StatusCode::CONFLICT);
    assert_eq!(
        json_body(archived_turn).await["code"],
        "CONVERSATION_ARCHIVED"
    );

    let deleted = send(
        &fixture.app,
        conversation_delete(
            format!("/v1/conversations/{conversation_id}"),
            TENANT_ID,
            USER_ID,
            "conversation-privacy-delete",
        ),
    )
    .await;
    assert_eq!(deleted.status(), StatusCode::OK);
    assert_eq!(json_body(deleted).await["data"]["deleted"], true);
    let after_delete = send(
        &fixture.app,
        conversation_read(
            format!("/v1/conversations/{conversation_id}"),
            TENANT_ID,
            USER_ID,
        ),
    )
    .await;
    assert_eq!(after_delete.status(), StatusCode::NOT_FOUND);
    let (messages_after_delete_status, messages_after_delete) = message_page(
        &fixture.app,
        &conversation_id,
        TENANT_ID,
        USER_ID,
        "?limit=2",
    )
    .await;
    assert_eq!(messages_after_delete_status, StatusCode::NOT_FOUND);
    assert_eq!(messages_after_delete["code"], "CONVERSATION_NOT_FOUND");
    let redacted_run = send(
        &fixture.app,
        Request::get(format!("/v1/runs/{first_run_id}"))
            .header("x-tenant-id", TENANT_ID)
            .header("x-user-id", USER_ID)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(redacted_run.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM conversation_messages WHERE conversation_id=?"
        )
        .bind(&conversation_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        0
    );

    fixture.shutdown().await;
}

async fn exercise_conversation_history_file_binding_after_public_tombstone(
    agent_id: &str,
    label: &str,
) {
    let fixture = TerminalHttpFixture::start().await;
    let file_id = format!("file_conversation_history_{label}");
    let now = chrono::Utc::now();
    let file_query = FileQuery {
        file_id: file_id.clone(),
        tenant_id: TENANT_ID.to_owned(),
        user_id: USER_ID.to_owned(),
    };
    fixture
        .repository
        .create_file(CreateFileCommand {
            file_id: file_id.clone(),
            tenant_id: TENANT_ID.to_owned(),
            user_id: USER_ID.to_owned(),
            filename: "scan.png".to_owned(),
            media_type: "image/png".to_owned(),
            expected_size_bytes: 8,
            checksum_sha256: None,
            object_key: format!("conversation-test/{file_id}"),
            idempotency_key: Some(format!("conversation-history-file-{label}")),
            request_hash: format!("sha256:{}", "a".repeat(64)),
            created_at: now,
            upload_expires_at: now + chrono::Duration::hours(1),
        })
        .await
        .unwrap();
    fixture
        .repository
        .complete_file(CompleteFileCommand {
            query: file_query.clone(),
            actual_size_bytes: 8,
            object_etag: "etag-conversation-file".to_owned(),
            object_version_id: Some("version-conversation-file".to_owned()),
            ready_at: now,
        })
        .await
        .unwrap();

    let created = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            "/v1/conversations",
            TENANT_ID,
            USER_ID,
            &format!("conversation-file-create-{label}"),
            json!({"agent_id": agent_id}),
        ),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);
    let conversation_id = json_body(created).await["data"]["conversation_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let first = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            &format!("conversation-file-turn-1-{label}"),
            json!({"query":"inspect", "files":[{"file_id":&file_id}]}),
        ),
    )
    .await;
    assert_eq!(first.status(), StatusCode::ACCEPTED);
    let first_run_id = first.headers()["x-run-id"].to_str().unwrap().to_owned();
    let first_body = json_body(first).await;
    assert_eq!(
        first_body["data"]["user_message"]["content"],
        json!([{"text":"inspect"},{"file":{"file_id":&file_id}}])
    );
    wait_for_assistant(&fixture.app, &conversation_id, &first_run_id).await;

    fixture
        .repository
        .begin_file_delete(file_query, chrono::Utc::now())
        .await
        .unwrap();
    fixture.file_authority.tombstone_public_file();

    let second = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            &format!("conversation-file-turn-2-{label}"),
            json!({"query":"follow up"}),
        ),
    )
    .await;
    if second.status() != StatusCode::ACCEPTED {
        panic!(
            "unexpected second-turn response: {}",
            json_body(second).await
        );
    }
    let second_run_id = second.headers()["x-run-id"].to_str().unwrap().to_owned();
    wait_for_assistant(&fixture.app, &conversation_id, &second_run_id).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM file_bindings
             WHERE file_id=? AND target_kind='run' AND released_at IS NULL"
        )
        .bind(&file_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        2,
        "the replayed history File must be frozen into the second Run"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM file_bindings
             WHERE file_id=? AND target_kind='conversation' AND released_at IS NULL"
        )
        .bind(&file_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        1
    );

    let explicit_reattach = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            &format!("conversation-file-turn-3-{label}"),
            json!({"query":"reattach", "files":[{"file_id":&file_id}]}),
        ),
    )
    .await;
    assert_eq!(explicit_reattach.status(), StatusCode::NOT_FOUND);
    assert_eq!(json_body(explicit_reattach).await["code"], "FILE_NOT_FOUND");

    fixture.shutdown().await;
}

#[tokio::test]
async fn conversation_history_reuses_immutable_file_binding_after_public_tombstone() {
    exercise_conversation_history_file_binding_after_public_tombstone(AGENT_ID, "terminal").await;
    exercise_conversation_history_file_binding_after_public_tombstone(FULL_AGENT_ID, "full").await;
}

#[tokio::test]
async fn attached_terminal_sse_is_emitted_only_after_result_and_assistant_commit() {
    let fixture = TerminalHttpFixture::start().await;
    let created = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            "/v1/conversations",
            TENANT_ID,
            USER_ID,
            "stream-conversation-create",
            json!({"agent_id": AGENT_ID}),
        ),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);
    let conversation_id = json_body(created).await["data"]["conversation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let streamed_message = format!("streamed-large-object:{}", "z".repeat(700));
    let response = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages/stream"),
            TENANT_ID,
            USER_ID,
            "conversation-stream-turn",
            json!({"query": streamed_message}),
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers()[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));
    assert_eq!(
        response.headers()[header::CACHE_CONTROL],
        "no-store, no-transform"
    );
    let run_id = response.headers()["x-run-id"].to_str().unwrap().to_owned();
    let message_id = response.headers()["x-message-id"]
        .to_str()
        .unwrap()
        .to_owned();

    let mut stream = response.into_body().into_data_stream();
    let mut bytes = Vec::new();
    let mut observed_terminal = false;
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
            assert!(bytes.len() <= MAX_HTTP_BODY_BYTES);
            if !observed_terminal
                && String::from_utf8_lossy(&bytes).contains("event: run.lifecycle.completed")
            {
                observed_terminal = true;
                let terminal_state = sqlx::query_scalar::<_, String>(
                    "SELECT terminal_state FROM terminal_run_results WHERE run_id=?",
                )
                .bind(&run_id)
                .fetch_one(&fixture.inspection)
                .await
                .unwrap();
                assert_eq!(terminal_state, "succeeded");
                let assistant_count = sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM conversation_messages
                     WHERE conversation_id=? AND run_id=? AND role='assistant'",
                )
                .bind(&conversation_id)
                .bind(&run_id)
                .fetch_one(&fixture.inspection)
                .await
                .unwrap();
                assert_eq!(assistant_count, 1);
            }
        }
    })
    .await
    .expect("attached terminal-only response did not reach terminal EOF");
    assert!(observed_terminal);
    let raw = String::from_utf8(bytes).unwrap();
    assert!(raw.lines().all(|line| !line.starts_with("id:")));
    let terminal = raw
        .split_terminator("\n\n")
        .find_map(|frame| {
            let event = frame
                .lines()
                .find_map(|line| line.strip_prefix("event: "))?;
            (event == "run.lifecycle.completed").then(|| {
                let data = frame
                    .lines()
                    .find_map(|line| line.strip_prefix("data: "))
                    .unwrap();
                serde_json::from_str::<Value>(data).unwrap()
            })
        })
        .expect("terminal run.lifecycle.completed frame");
    assert_eq!(terminal["run"]["result"], streamed_message);

    let page = wait_for_assistant(&fixture.app, &conversation_id, &run_id).await;
    let messages = page["data"]["messages"].as_array().unwrap();
    let user = messages
        .iter()
        .find(|message| message["role"] == "user")
        .unwrap();
    let assistant = messages
        .iter()
        .find(|message| message["role"] == "assistant")
        .unwrap();
    assert_eq!(user["message_id"], message_id);
    assert_eq!(user["content"], text_content(&streamed_message));
    assert_eq!(assistant["content"], text_content(&streamed_message));

    fixture.shutdown().await;
}

#[tokio::test]
async fn full_persistence_conversation_is_atomic_idempotent_streamable_and_tenant_isolated() {
    let fixture = TerminalHttpFixture::start().await;
    let created = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            "/v1/conversations",
            TENANT_ID,
            USER_ID,
            "full-conversation-create",
            json!({"agent_id": FULL_AGENT_ID}),
        ),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);
    let conversation_id = json_body(created).await["data"]["conversation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (frozen_mode, frozen_deployment_revision) = sqlx::query_as::<_, (String, String)>(
        "SELECT persistence_mode,deployment_revision_id
             FROM conversations WHERE conversation_id=?",
    )
    .bind(&conversation_id)
    .fetch_one(&fixture.inspection)
    .await
    .unwrap();
    assert_eq!(frozen_mode, "full");
    assert!(!frozen_deployment_revision.is_empty());

    let first = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            "full-turn-1",
            json!({"query": "durable hello"}),
        ),
    )
    .await;
    if first.status() != StatusCode::ACCEPTED {
        panic!("unexpected first-turn response: {}", json_body(first).await);
    }
    assert_eq!(first.status(), StatusCode::ACCEPTED);
    let run_id = first.headers()["x-run-id"].to_str().unwrap().to_owned();
    let message_id = first.headers()["x-message-id"].to_str().unwrap().to_owned();
    let first_body = json_body(first).await;
    assert_eq!(first_body["data"]["replayed"], false);
    assert_eq!(first_body["data"]["run"]["persistence_mode"], "full");
    assert_eq!(
        first_body["data"]["run"]["recovery_capability"],
        "restart_only"
    );
    assert_eq!(first_body["data"]["run"]["event_replay"], true);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT deployment_revision_id FROM workflow_runs WHERE run_id=?"
        )
        .bind(&run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        frozen_deployment_revision
    );
    let page = wait_for_assistant(&fixture.app, &conversation_id, &run_id).await;
    let messages = page["data"]["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(
        messages
            .iter()
            .find(|message| message["role"] == "assistant")
            .unwrap()["content"],
        text_content("durable hello")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM full_conversation_turns
             WHERE tenant_id=? AND admission_id LIKE 'idem_%' AND run_id=?"
        )
        .bind(TENANT_ID)
        .bind(&run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM terminal_run_admissions WHERE run_id=?")
            .bind(&run_id)
            .fetch_one(&fixture.inspection)
            .await
            .unwrap(),
        0
    );
    let (small_assistant_inline, small_assistant_ref) =
        sqlx::query_as::<_, (Option<String>, Option<String>)>(
            "SELECT content_inline,content_ref
             FROM conversation_messages
             WHERE conversation_id=? AND run_id=? AND role='assistant'",
        )
        .bind(&conversation_id)
        .bind(&run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap();
    assert!(small_assistant_inline.is_some());
    assert!(small_assistant_ref.is_none());

    let replay = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            "full-turn-1",
            json!({"query": "durable hello"}),
        ),
    )
    .await;
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    assert_eq!(replay.headers()["x-run-id"], run_id);
    assert_eq!(replay.headers()["x-message-id"], message_id);
    let replay = json_body(replay).await;
    assert_eq!(replay["data"]["replayed"], true);
    assert_eq!(
        replay["data"]["user_message"]["content"],
        text_content("durable hello")
    );
    assert_eq!(replay["data"]["run"]["recovery_capability"], "restart_only");
    assert_eq!(replay["data"]["run"]["event_replay"], true);
    let conflicting_replay = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            "full-turn-1",
            json!({"query": "must conflict"}),
        ),
    )
    .await;
    assert_eq!(conflicting_replay.status(), StatusCode::CONFLICT);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM conversation_messages WHERE conversation_id=?"
        )
        .bind(&conversation_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        2
    );

    let wrong_tenant = send(
        &fixture.app,
        Request::get(format!("/v1/runs/{run_id}"))
            .header("x-tenant-id", "tenant-other")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(wrong_tenant.status(), StatusCode::NOT_FOUND);
    for user_id in [None, Some("user-other")] {
        let request = Request::get(format!("/v1/runs/{run_id}")).header("x-tenant-id", TENANT_ID);
        let request = match user_id {
            Some(user_id) => request.header("x-user-id", user_id),
            None => request,
        };
        assert_eq!(
            send(&fixture.app, request.body(Body::empty()).unwrap())
                .await
                .status(),
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        send(
            &fixture.app,
            Request::delete(format!("/v1/runs/{run_id}"))
                .header("x-tenant-id", TENANT_ID)
                .header("x-user-id", "user-other")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    for path in [
        format!("/v1/runs/{run_id}/artifacts/artifact_private"),
        format!("/v1/runs/{run_id}/execution-graph"),
        format!("/v1/runs/{run_id}/trace"),
    ] {
        assert_eq!(
            send(
                &fixture.app,
                Request::get(path)
                    .header("x-tenant-id", TENANT_ID)
                    .header("x-user-id", "user-other")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
    }
    for path in [
        format!("/v1/runs/{run_id}/pause"),
        format!("/v1/runs/{run_id}/resume"),
        format!("/v1/runs/{run_id}/redrive"),
        format!("/v1/runs/{run_id}/fork"),
        format!("/v1/runs/{run_id}/migrate"),
        format!("/v1/runs/{run_id}/continue-as-new"),
        format!("/v1/runs/{run_id}/signals/private"),
    ] {
        assert_eq!(
            send(
                &fixture.app,
                Request::post(path)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-tenant-id", TENANT_ID)
                    .header("x-user-id", "user-other")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
    }
    let authorized = send(
        &fixture.app,
        Request::get(format!("/v1/runs/{run_id}"))
            .header("x-tenant-id", TENANT_ID)
            .header("x-user-id", USER_ID)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(authorized.status(), StatusCode::OK);
    let authorized = json_body(authorized).await;
    assert_eq!(authorized["data"]["status"], "completed");
    assert_eq!(authorized["data"]["persistence_mode"], "full");
    assert_eq!(authorized["data"]["recovery_capability"], "restart_only");
    assert_eq!(authorized["data"]["event_replay"], true);
    assert_eq!(
        send(
            &fixture.app,
            Request::get(format!("/v1/runs/{run_id}"))
                .header("x-tenant-id", TENANT_ID)
                .header("x-user-id", "user-other")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &fixture.app,
            Request::get(format!("/v1/runs/{run_id}/execution-graph"))
                .header("x-tenant-id", TENANT_ID)
                .header("x-user-id", "user-other")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &fixture.app,
            Request::get(format!("/v1/runs/{run_id}/execution-graph"))
                .header("x-tenant-id", TENANT_ID)
                .header("x-user-id", USER_ID)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    for surface in [
        "pause",
        "resume",
        "signals/conversation-probe",
        "redrive",
        "fork",
        "migrate",
        "continue-as-new",
    ] {
        let unsupported = send(
            &fixture.app,
            Request::post(format!("/v1/runs/{run_id}/{surface}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-tenant-id", TENANT_ID)
                .header("x-user-id", USER_ID)
                .header(
                    "x-request-id",
                    format!("full-conversation-{}", surface.replace('/', "-")),
                )
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert_eq!(
            unsupported.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{surface}"
        );
        assert!(
            unsupported.headers().get(header::RETRY_AFTER).is_none(),
            "{surface}"
        );
        let unsupported = json_body(unsupported).await;
        assert_eq!(
            unsupported["error"]["code"], "RUN_CAPABILITY_UNAVAILABLE",
            "{surface}"
        );
        assert_eq!(
            unsupported["error"]["message"],
            "Conversation-bound full Runs do not support recovery lineage",
            "{surface}"
        );
    }

    let large_message = "durable-large-".repeat(80);
    let large = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            "full-turn-large",
            json!({"query": large_message}),
        ),
    )
    .await;
    assert_eq!(large.status(), StatusCode::ACCEPTED);
    let large_run_id = large.headers()["x-run-id"].to_str().unwrap().to_owned();
    let large_page = wait_for_assistant(&fixture.app, &conversation_id, &large_run_id).await;
    assert!(large_page["data"]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|message| {
            message["role"] == "user" && message["content"] == text_content(&large_message)
        }));
    let (inline, reference) = sqlx::query_as::<_, (Option<String>, Option<String>)>(
        "SELECT content_inline,content_ref
         FROM conversation_messages
         WHERE conversation_id=? AND run_id=? AND role='user'",
    )
    .bind(&conversation_id)
    .bind(&large_run_id)
    .fetch_one(&fixture.inspection)
    .await
    .unwrap();
    assert!(inline.is_none());
    assert!(reference.is_some());
    let (assistant_inline, assistant_reference, assistant_hash) =
        sqlx::query_as::<_, (Option<String>, Option<String>, String)>(
            "SELECT content_inline,content_ref,content_hash
             FROM conversation_messages
             WHERE conversation_id=? AND run_id=? AND role='assistant'",
        )
        .bind(&conversation_id)
        .bind(&large_run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap();
    assert!(assistant_inline.is_none());
    let assistant_reference = assistant_reference.expect("large assistant output was not scoped");
    assert_eq!(
        assistant_hash,
        ContentHash::from_bytes(&serde_jcs::to_vec(&text_content(&large_message)).unwrap())
            .as_str()
    );
    let (assistant_artifact, assistant_locator) =
        decode_artifact_reference(fixture.artifact_store.as_ref(), &assistant_reference);
    fixture
        .artifact_store
        .read_and_verify(&assistant_artifact, &assistant_locator, 2 * 1024 * 1024)
        .await
        .expect("large assistant object was not readable before privacy deletion");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM terminal_artifact_staging
             WHERE source_kind='user_message' AND source_id=(
                 SELECT message_id FROM conversation_messages
                 WHERE conversation_id=? AND run_id=? AND role='user'
             )"
        )
        .bind(&conversation_id)
        .bind(&large_run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM terminal_artifact_staging
             WHERE source_kind='assistant_message' AND source_id=?"
        )
        .bind(&large_run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        0
    );

    let streamed = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages/stream"),
            TENANT_ID,
            USER_ID,
            "full-turn-stream",
            json!({"query": "durable stream"}),
        ),
    )
    .await;
    assert_eq!(streamed.status(), StatusCode::OK);
    let streamed_run_id = streamed.headers()["x-run-id"].to_str().unwrap().to_owned();
    let bytes = tokio::time::timeout(
        Duration::from_secs(5),
        to_bytes(streamed.into_body(), MAX_HTTP_BODY_BYTES),
    )
    .await
    .expect("full Conversation SSE did not reach EOF")
    .unwrap();
    let raw = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(raw.contains("event: run.lifecycle.completed"), "{raw}");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT lifecycle FROM workflow_runs WHERE run_id=?")
            .bind(&streamed_run_id)
            .fetch_one(&fixture.inspection)
            .await
            .unwrap(),
        "succeeded"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM conversation_messages
             WHERE conversation_id=? AND run_id=? AND role='assistant'"
        )
        .bind(&conversation_id)
        .bind(&streamed_run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        1
    );
    let replayed_stream = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages/stream"),
            TENANT_ID,
            USER_ID,
            "full-turn-stream",
            json!({"query": "durable stream"}),
        ),
    )
    .await;
    assert_eq!(replayed_stream.status(), StatusCode::OK);
    assert_eq!(replayed_stream.headers()["x-run-id"], streamed_run_id);
    let replayed_stream = tokio::time::timeout(
        Duration::from_secs(5),
        to_bytes(replayed_stream.into_body(), MAX_HTTP_BODY_BYTES),
    )
    .await
    .expect("full Conversation replay SSE did not reach EOF")
    .unwrap();
    let replayed_stream = String::from_utf8(replayed_stream.to_vec()).unwrap();
    assert!(
        replayed_stream.contains("event: run.lifecycle.completed"),
        "{replayed_stream}"
    );
    assert!(
        !replayed_stream.contains("event: run.lifecycle.running"),
        "terminal replay must not synthesize running: {replayed_stream}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM conversation_messages
             WHERE conversation_id=? AND run_id=?"
        )
        .bind(&conversation_id)
        .bind(&streamed_run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        2
    );

    let deleted = send(
        &fixture.app,
        conversation_delete(
            format!("/v1/conversations/{conversation_id}"),
            TENANT_ID,
            USER_ID,
            "full-conversation-delete",
        ),
    )
    .await;
    assert_eq!(deleted.status(), StatusCode::OK);
    assert_eq!(json_body(deleted).await["data"]["deleted"], true);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if fixture
            .artifact_store
            .read_and_verify(&assistant_artifact, &assistant_locator, 2 * 1024 * 1024)
            .await
            .is_err()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "privacy deletion did not erase the scoped assistant object"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let redacted = send(
        &fixture.app,
        Request::get(format!("/v1/runs/{run_id}"))
            .header("x-tenant-id", TENANT_ID)
            .header("x-user-id", USER_ID)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(redacted.status(), StatusCode::OK);
    assert!(json_body(redacted).await["data"]["output"]["data"].is_null());
    assert_eq!(
        send(
            &fixture.app,
            Request::get(format!("/v1/runs/{run_id}"))
                .header("x-tenant-id", "tenant-other")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM full_conversation_turns WHERE conversation_id=?"
        )
        .bind(&conversation_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        3
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn full_conversation_retention_drains_multiple_bounded_batches_per_cycle() {
    let fixture = TerminalHttpFixture::start_full_only().await;
    let expired_at = chrono::Utc::now() - chrono::Duration::days(2);
    let input_hash = ContentHash::from_bytes(b"full-retention-batch")
        .as_str()
        .to_owned();
    let mut transaction = fixture.inspection.begin().await.unwrap();
    for index in 0..205_u32 {
        sqlx::query(
            "INSERT INTO conversations (
                 conversation_id,tenant_id,user_id,agent_id,persistence_mode,
                 deployment_revision_id,created_at,archived_at
             ) VALUES (?,?,?,?,'full',?,?,NULL)",
        )
        .bind(format!("full-retention-conversation-{index}"))
        .bind(TENANT_ID)
        .bind(USER_ID)
        .bind(FULL_AGENT_ID)
        .bind("full-retention-deployment")
        .bind(expired_at)
        .execute(&mut *transaction)
        .await
        .unwrap();

        let run_id = format!("full-retention-run-{index}");
        sqlx::query(
            "INSERT INTO terminal_run_admissions (
                 run_id,tenant_id,admission_id,request_id,agent_id,definition_revision_id,
                 deployment_revision_id,conversation_id,user_message_id,input_ref,
                 input_hash,selected_context_hash,owner_instance_id,owner_epoch,accepted_at
             ) VALUES (?,?,?,?,?,?,?,NULL,NULL,NULL,?,NULL,?,1,?)",
        )
        .bind(&run_id)
        .bind(TENANT_ID)
        .bind(format!("full-retention-admission-{index}"))
        .bind(format!("full-retention-request-{index}"))
        .bind(AGENT_ID)
        .bind("full-retention-definition")
        .bind("full-retention-deployment")
        .bind(&input_hash)
        .bind("full-retention-owner")
        .bind(expired_at)
        .execute(&mut *transaction)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO terminal_run_results (
                 run_id,terminal_state,response_id,output_ref,output_hash,error_code,
                 usage_json,started_at,terminal_at
             ) VALUES (?,'succeeded',?,NULL,NULL,NULL,NULL,?,?)",
        )
        .bind(run_id)
        .bind(format!("full-retention-response-{index}"))
        .bind(expired_at)
        .bind(expired_at + chrono::Duration::seconds(1))
        .execute(&mut *transaction)
        .await
        .unwrap();
    }
    transaction.commit().await.unwrap();

    let maintenance_now = chrono::Utc::now();
    let mut deletion_rows = Vec::new();
    let mut staging_rows = Vec::new();
    let mut object_probes = Vec::new();
    for index in 0..205_u32 {
        let deletion_envelope = json!({
            "kind": "insight_terminal_object",
            "scope": format!("tenant:{TENANT_ID}:retention-delete:{index}"),
            "value": format!("delete object {index}"),
        });
        let deletion_bytes = serde_jcs::to_vec(&deletion_envelope).unwrap();
        let deletion_artifact = fixture
            .artifact_store
            .artifact_for_bytes(
                &deletion_bytes,
                Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE.to_owned()),
            )
            .unwrap();
        fixture
            .artifact_store
            .put_and_verify(&deletion_artifact, &deletion_bytes)
            .await
            .unwrap();
        let deletion_locator = fixture
            .artifact_store
            .storage_locator(&deletion_artifact)
            .unwrap();
        deletion_rows.push((
            format!("full-retention-deletion-job-{index}"),
            serde_json::to_string(&deletion_artifact).unwrap(),
            deletion_artifact.content_hash().as_str().to_owned(),
            format!("full-retention-deletion-source-{index}"),
        ));
        if index == 0 || index == 204 {
            object_probes.push((deletion_artifact, deletion_locator));
        }

        let staging_envelope = json!({
            "kind": "insight_terminal_object",
            "scope": format!("tenant:{TENANT_ID}:retention-stage:{index}"),
            "value": format!("staging object {index}"),
        });
        let staging_bytes = serde_jcs::to_vec(&staging_envelope).unwrap();
        let staging_artifact = fixture
            .artifact_store
            .artifact_for_bytes(
                &staging_bytes,
                Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE.to_owned()),
            )
            .unwrap();
        fixture
            .artifact_store
            .put_and_verify(&staging_artifact, &staging_bytes)
            .await
            .unwrap();
        let staging_locator = fixture
            .artifact_store
            .storage_locator(&staging_artifact)
            .unwrap();
        let source_id = format!("full-retention-staging-source-{index}");
        staging_rows.push((
            terminal_artifact_staging_id(
                TENANT_ID,
                TerminalArtifactSourceKind::AssistantMessage,
                &source_id,
            ),
            serde_json::to_string(&staging_artifact).unwrap(),
            staging_artifact.content_hash().as_str().to_owned(),
            source_id,
        ));
        if index == 0 || index == 204 {
            object_probes.push((staging_artifact, staging_locator));
        }
    }
    let mut transaction = fixture.inspection.begin().await.unwrap();
    for (job_id, content_ref, content_hash, source_id) in deletion_rows {
        sqlx::query(
            "INSERT INTO terminal_content_deletion_jobs (
                 deletion_job_id,tenant_id,content_ref,content_hash,source_kind,source_id,
                 job_state,available_at,claim_token,claimed_by,claim_expires_at,attempts,created_at
             ) VALUES (?,?,?,?,'conversation_retention',?,'pending',?,NULL,NULL,NULL,0,?)",
        )
        .bind(job_id)
        .bind(TENANT_ID)
        .bind(content_ref)
        .bind(content_hash)
        .bind(source_id)
        .bind(maintenance_now)
        .bind(maintenance_now)
        .execute(&mut *transaction)
        .await
        .unwrap();
    }
    for (staging_id, content_ref, content_hash, source_id) in staging_rows {
        sqlx::query(
            "INSERT INTO terminal_artifact_staging (
                 staging_id,tenant_id,content_ref,content_hash,source_kind,source_id,
                 staging_state,available_at,claim_token,claimed_by,claim_expires_at,
                 attempts,created_at
             ) VALUES (?,?,?,?,'assistant_message',?,'pending',?,NULL,NULL,NULL,0,?)",
        )
        .bind(staging_id)
        .bind(TENANT_ID)
        .bind(content_ref)
        .bind(content_hash)
        .bind(source_id)
        .bind(maintenance_now)
        .bind(maintenance_now)
        .execute(&mut *transaction)
        .await
        .unwrap();
    }
    transaction.commit().await.unwrap();

    fixture
        .service
        .run_full_conversation_maintenance_once()
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM conversations
             WHERE conversation_id LIKE 'full-retention-conversation-%'"
        )
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM terminal_run_admissions
             WHERE run_id LIKE 'full-retention-run-%'"
        )
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM terminal_content_deletion_jobs
             WHERE deletion_job_id LIKE 'full-retention-deletion-job-%'"
        )
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM terminal_artifact_staging
             WHERE source_id LIKE 'full-retention-staging-source-%'"
        )
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        0
    );
    for (artifact, locator) in object_probes {
        assert!(
            fixture
                .artifact_store
                .read_and_verify(&artifact, &locator, 1024)
                .await
                .is_err(),
            "maintenance must delete every probed object across multiple batches"
        );
    }
    let metrics = fixture.service.prometheus_metrics();
    assert!(
        prometheus_sample(
            &metrics,
            "conversation_retention_deleted_total{kind=\"conversation\",persistence_mode=\"full\"}"
        ) >= 205
    );
    assert!(
        prometheus_sample(
            &metrics,
            "conversation_retention_deleted_total{kind=\"run\",persistence_mode=\"full\"}"
        ) >= 205
    );
    assert!(
        prometheus_sample(
            &metrics,
            "conversation_retention_batches_total{persistence_mode=\"full\"}"
        ) >= 3
    );
    assert_eq!(
        prometheus_sample(
            &metrics,
            "conversation_retention_backlog_pending{persistence_mode=\"full\"}"
        ),
        0
    );
    for queue in ["content_deletion", "artifact_staging"] {
        assert!(
            prometheus_sample(
                &metrics,
                &format!(
                    "conversation_maintenance_batches_total{{queue=\"{queue}\",persistence_mode=\"full\"}}"
                )
            ) >= 3,
            "{queue} did not drain more than one bounded batch: {metrics}"
        );
        assert_eq!(
            prometheus_sample(
                &metrics,
                &format!(
                    "conversation_maintenance_backlog_pending{{queue=\"{queue}\",persistence_mode=\"full\"}}"
                )
            ),
            0,
            "{queue} reported backlog after a complete bounded drain"
        );
    }

    fixture.shutdown().await;
}

#[tokio::test]
async fn full_conversation_summary_worker_is_non_blocking_and_locally_coalesced() {
    let fixture = TerminalHttpFixture::start_with_runtime_options_and_summary_artifact_delay(
        2,
        1,
        false,
        Duration::from_secs(2),
    )
    .await;
    let initial_metrics = fixture.service.prometheus_metrics();
    for sample in [
        "conversation_messages_total{role=\"user\",persistence_mode=\"full\"}",
        "conversation_messages_total{role=\"assistant\",persistence_mode=\"full\"}",
        "conversation_summary_jobs_active{persistence_mode=\"full\"}",
        "conversation_summary_jobs_total{result=\"succeeded\",persistence_mode=\"full\"}",
        "conversation_summary_jobs_total{result=\"failed\",persistence_mode=\"full\"}",
    ] {
        assert_eq!(
            prometheus_sample(&initial_metrics, sample),
            0,
            "feature-off metrics must expose a stable zero sample"
        );
    }

    let created = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            "/v1/conversations",
            TENANT_ID,
            USER_ID,
            "full-background-summary-create",
            json!({"agent_id": FULL_AGENT_ID}),
        ),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);
    let conversation_id = json_body(created).await["data"]["conversation_id"]
        .as_str()
        .unwrap()
        .to_owned();

    for turn in 1..=2 {
        let started = Instant::now();
        let response = send(
            &fixture.app,
            conversation_write(
                Method::POST,
                format!("/v1/conversations/{conversation_id}/messages"),
                TENANT_ID,
                USER_ID,
                &format!("full-background-summary-turn-{turn}"),
                json!({"query": format!("background summary message {turn}")}),
            ),
        )
        .await;
        let elapsed = started.elapsed();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(
            elapsed < Duration::from_secs(1),
            "turn admission joined a delayed summary worker for {elapsed:?}"
        );
        let run_id = response.headers()["x-run-id"].to_str().unwrap().to_owned();
        wait_for_assistant(&fixture.app, &conversation_id, &run_id).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if prometheus_sample(
                    &fixture.service.prometheus_metrics(),
                    "conversation_summary_jobs_active{persistence_mode=\"full\"}",
                ) == 1
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the coalesced summary worker did not become active");
        assert_eq!(
            prometheus_sample(
                &fixture.service.prometheus_metrics(),
                "conversation_summary_jobs_active{persistence_mode=\"full\"}"
            ),
            1,
            "repeated admission/terminal triggers must coalesce locally"
        );
    }

    let deadline = Instant::now() + Duration::from_secs(7);
    loop {
        let metrics = fixture.service.prometheus_metrics();
        let active = prometheus_sample(
            &metrics,
            "conversation_summary_jobs_active{persistence_mode=\"full\"}",
        );
        let assistant = prometheus_sample(
            &metrics,
            "conversation_messages_total{role=\"assistant\",persistence_mode=\"full\"}",
        );
        if active == 0 && assistant == 2 {
            assert_eq!(
                prometheus_sample(
                    &metrics,
                    "conversation_messages_total{role=\"user\",persistence_mode=\"full\"}"
                ),
                2
            );
            assert!(
                prometheus_sample(
                    &metrics,
                    "conversation_summary_jobs_total{result=\"succeeded\",persistence_mode=\"full\"}"
                ) >= 1
            );
            assert_eq!(
                prometheus_sample(
                    &metrics,
                    "conversation_summary_jobs_total{result=\"failed\",persistence_mode=\"full\"}"
                ),
                0
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "full summary worker did not drain: {metrics}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM conversation_summary_jobs WHERE conversation_id=?"
        )
        .bind(&conversation_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        0,
        "exact-token release must retire the durable claim"
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn below_threshold_full_turns_do_not_mutate_durable_summary_claims() {
    let fixture = TerminalHttpFixture::start_with_runtime_options(100, 8, false).await;
    sqlx::query("CREATE TABLE summary_job_mutation_audit (operation TEXT NOT NULL)")
        .execute(&fixture.inspection)
        .await
        .unwrap();
    for statement in [
        "CREATE TRIGGER audit_summary_job_insert
         AFTER INSERT ON conversation_summary_jobs
         BEGIN
           INSERT INTO summary_job_mutation_audit(operation) VALUES ('insert');
         END",
        "CREATE TRIGGER audit_summary_job_update
         AFTER UPDATE ON conversation_summary_jobs
         BEGIN
           INSERT INTO summary_job_mutation_audit(operation) VALUES ('update');
         END",
        "CREATE TRIGGER audit_summary_job_delete
         AFTER DELETE ON conversation_summary_jobs
         BEGIN
           INSERT INTO summary_job_mutation_audit(operation) VALUES ('delete');
         END",
    ] {
        sqlx::query(statement)
            .execute(&fixture.inspection)
            .await
            .unwrap();
    }

    let created = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            "/v1/conversations",
            TENANT_ID,
            USER_ID,
            "full-summary-preflight-create",
            json!({"agent_id": FULL_AGENT_ID}),
        ),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);
    let conversation_id = json_body(created).await["data"]["conversation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    for turn in 1..=3 {
        let response = send(
            &fixture.app,
            conversation_write(
                Method::POST,
                format!("/v1/conversations/{conversation_id}/messages"),
                TENANT_ID,
                USER_ID,
                &format!("full-summary-preflight-turn-{turn}"),
                json!({"query": format!("small message {turn}")}),
            ),
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let run_id = response.headers()["x-run-id"].to_str().unwrap().to_owned();
        wait_for_assistant(&fixture.app, &conversation_id, &run_id).await;
    }

    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let metrics = fixture.service.prometheus_metrics();
        let active = prometheus_sample(
            &metrics,
            "conversation_summary_jobs_active{persistence_mode=\"full\"}",
        );
        let assistants = prometheus_sample(
            &metrics,
            "conversation_messages_total{role=\"assistant\",persistence_mode=\"full\"}",
        );
        if active == 0 && assistants == 3 {
            assert_eq!(
                prometheus_sample(
                    &metrics,
                    "conversation_summary_jobs_total{result=\"succeeded\",persistence_mode=\"full\"}"
                ),
                0
            );
            assert_eq!(
                prometheus_sample(
                    &metrics,
                    "conversation_summary_jobs_total{result=\"failed\",persistence_mode=\"full\"}"
                ),
                0
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "below-threshold summary preflight did not settle: {metrics}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM summary_job_mutation_audit")
            .fetch_one(&fixture.inspection)
            .await
            .unwrap(),
        0,
        "below-threshold turns must perform no durable summary claim write"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM conversation_summary_jobs WHERE conversation_id=?"
        )
        .bind(&conversation_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        0
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn full_conversation_surface_works_when_terminal_only_runtime_is_disabled() {
    let fixture = TerminalHttpFixture::start_full_only().await;

    let expired = fixture
        .service
        .create_conversation(
            TENANT_ID,
            USER_ID,
            FULL_AGENT_ID,
            "full-only-retention-create",
        )
        .await
        .unwrap();
    let expired_value = json!("full-only-retention-object");
    let expired_envelope = json!({
        "kind": "insight_terminal_object",
        "scope": format!("tenant:{TENANT_ID}:conversation-message:expired"),
        "value": expired_value,
    });
    let expired_bytes = serde_jcs::to_vec(&expired_envelope).unwrap();
    let expired_artifact = fixture
        .artifact_store
        .artifact_for_bytes(
            &expired_bytes,
            Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE.to_owned()),
        )
        .unwrap();
    fixture
        .artifact_store
        .put_and_verify(&expired_artifact, &expired_bytes)
        .await
        .unwrap();
    let expired_locator = fixture
        .artifact_store
        .storage_locator(&expired_artifact)
        .unwrap();
    let expired_ref = serde_json::to_string(&expired_artifact).unwrap();
    let expired_at = chrono::Utc::now() - chrono::Duration::days(2);
    sqlx::query("UPDATE conversations SET created_at=? WHERE conversation_id=?")
        .bind(expired_at)
        .bind(&expired.conversation_id)
        .execute(&fixture.inspection)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO conversation_messages (
             message_id,conversation_id,message_order,role,run_id,
             content_inline,content_ref,content_hash,created_at
         ) VALUES (?,?,1,'user',NULL,NULL,?,?,?)",
    )
    .bind("message-full-only-retention")
    .bind(&expired.conversation_id)
    .bind(&expired_ref)
    .bind(
        ContentHash::from_bytes(&serde_jcs::to_vec(&expired_value).unwrap())
            .as_str()
            .to_owned(),
    )
    .bind(expired_at)
    .execute(&fixture.inspection)
    .await
    .unwrap();

    let expired_terminal_run = "run-full-only-retention";
    sqlx::query(
        "INSERT INTO terminal_run_admissions (
             run_id,tenant_id,admission_id,request_id,agent_id,definition_revision_id,
             deployment_revision_id,conversation_id,user_message_id,input_ref,
             input_hash,selected_context_hash,owner_instance_id,owner_epoch,accepted_at
         ) VALUES (?,?,?,?,?,?,?,NULL,NULL,NULL,?,NULL,?,1,?)",
    )
    .bind(expired_terminal_run)
    .bind(TENANT_ID)
    .bind("admission-full-only-retention")
    .bind("request-full-only-retention")
    .bind(AGENT_ID)
    .bind("definition-full-only-retention")
    .bind("deployment-full-only-retention")
    .bind(ContentHash::from_bytes(b"expired-terminal-input").as_str())
    .bind("retired-terminal-owner")
    .bind(expired_at)
    .execute(&fixture.inspection)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO terminal_run_results (
             run_id,terminal_state,response_id,output_ref,output_hash,error_code,
             usage_json,started_at,terminal_at
         ) VALUES (?,'succeeded',?,NULL,NULL,NULL,NULL,?,?)",
    )
    .bind(expired_terminal_run)
    .bind("resp-full-only-retention")
    .bind(expired_at)
    .bind(expired_at + chrono::Duration::seconds(1))
    .execute(&fixture.inspection)
    .await
    .unwrap();

    fixture
        .service
        .run_full_conversation_maintenance_once()
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let conversation_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM conversations WHERE conversation_id=?",
        )
        .bind(&expired.conversation_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap();
        let terminal_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM terminal_run_admissions WHERE run_id=?",
        )
        .bind(expired_terminal_run)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap();
        let deletion_jobs =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM terminal_content_deletion_jobs")
                .fetch_one(&fixture.inspection)
                .await
                .unwrap();
        let object_deleted = fixture
            .artifact_store
            .read_and_verify(&expired_artifact, &expired_locator, 1024)
            .await
            .is_err();
        if conversation_count == 0 && terminal_count == 0 && deletion_jobs == 0 && object_deleted {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "full-only maintenance did not apply bounded Conversation/Run retention: \
             conversations={conversation_count}, terminal_runs={terminal_count}, \
             deletion_jobs={deletion_jobs}, object_deleted={object_deleted}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let created = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            "/v1/conversations",
            TENANT_ID,
            USER_ID,
            "full-only-create",
            json!({"agent_id": FULL_AGENT_ID}),
        ),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);
    let conversation_id = json_body(created).await["data"]["conversation_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let fetched = send(
        &fixture.app,
        conversation_read(
            format!("/v1/conversations/{conversation_id}"),
            TENANT_ID,
            USER_ID,
        ),
    )
    .await;
    assert_eq!(fetched.status(), StatusCode::OK);
    assert_eq!(json_body(fetched).await["data"]["agent_id"], FULL_AGENT_ID);

    let large_content = format!("full-only-durable-large:{}", "q".repeat(700));
    let turn = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            "full-only-turn",
            json!({"query": large_content}),
        ),
    )
    .await;
    assert_eq!(turn.status(), StatusCode::ACCEPTED);
    let run_id = turn.headers()["x-run-id"].to_str().unwrap().to_owned();
    let page = wait_for_assistant(&fixture.app, &conversation_id, &run_id).await;
    assert_eq!(page["data"]["messages"].as_array().unwrap().len(), 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM terminal_run_admissions")
            .fetch_one(&fixture.inspection)
            .await
            .unwrap(),
        0
    );
    let refs = sqlx::query_scalar::<_, String>(
        "SELECT content_ref FROM conversation_messages
         WHERE conversation_id=? AND run_id=? AND content_ref IS NOT NULL
         ORDER BY message_order",
    )
    .bind(&conversation_id)
    .bind(&run_id)
    .fetch_all(&fixture.inspection)
    .await
    .unwrap();
    assert_eq!(
        refs.len(),
        2,
        "full-only large user and assistant must use refs"
    );
    let (assistant_inline, assistant_ref, assistant_hash) =
        sqlx::query_as::<_, (Option<String>, Option<String>, String)>(
            "SELECT content_inline,content_ref,content_hash
             FROM conversation_messages
             WHERE conversation_id=? AND run_id=? AND role='assistant'",
        )
        .bind(&conversation_id)
        .bind(&run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap();
    assert!(assistant_inline.is_none());
    assert!(assistant_ref.is_some());
    assert_eq!(
        assistant_hash,
        ContentHash::from_bytes(&serde_jcs::to_vec(&text_content(&large_content)).unwrap())
            .as_str()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM terminal_artifact_staging
             WHERE source_kind='assistant_message' AND source_id=?",
        )
        .bind(&run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        0
    );
    let referenced_artifacts = refs
        .iter()
        .map(|reference| decode_artifact_reference(fixture.artifact_store.as_ref(), reference))
        .collect::<Vec<_>>();

    let orphan_envelope = json!({
        "kind": "insight_terminal_object",
        "scope": format!("tenant:{TENANT_ID}:full-only-orphan"),
        "value": "orphan",
    });
    let orphan_bytes = serde_jcs::to_vec(&orphan_envelope).unwrap();
    let orphan_artifact = fixture
        .artifact_store
        .artifact_for_bytes(
            &orphan_bytes,
            Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE.to_owned()),
        )
        .unwrap();
    fixture
        .artifact_store
        .put_and_verify(&orphan_artifact, &orphan_bytes)
        .await
        .unwrap();
    let orphan_ref = serde_json::to_string(&orphan_artifact).unwrap();
    let orphan_source_id = "full-only-orphan-source";
    let orphan_staging_id = terminal_artifact_staging_id(
        TENANT_ID,
        TerminalArtifactSourceKind::AssistantMessage,
        orphan_source_id,
    );
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO terminal_artifact_staging (
             staging_id,tenant_id,content_ref,content_hash,source_kind,source_id,
             staging_state,available_at,created_at
         ) VALUES (?,?,?,?,?,?,'pending',?,?)",
    )
    .bind(&orphan_staging_id)
    .bind(TENANT_ID)
    .bind(&orphan_ref)
    .bind(orphan_artifact.content_hash().as_str())
    .bind("assistant_message")
    .bind(orphan_source_id)
    .bind(now)
    .bind(now)
    .execute(&fixture.inspection)
    .await
    .unwrap();
    let orphan_locator = fixture
        .artifact_store
        .storage_locator(&orphan_artifact)
        .unwrap();
    fixture
        .service
        .run_full_conversation_maintenance_once()
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let stage_exists = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM terminal_artifact_staging WHERE staging_id=?",
        )
        .bind(&orphan_staging_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap()
            != 0;
        let object_exists = fixture
            .artifact_store
            .read_and_verify(&orphan_artifact, &orphan_locator, 1024)
            .await
            .is_ok();
        if !stage_exists && !object_exists {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "full-only maintenance did not collect an orphan staging object"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let archived = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/archive"),
            TENANT_ID,
            USER_ID,
            "full-only-archive",
            json!({}),
        ),
    )
    .await;
    assert_eq!(archived.status(), StatusCode::OK);
    assert!(!json_body(archived).await["data"]["archived_at"].is_null());

    let deleted = send(
        &fixture.app,
        conversation_delete(
            format!("/v1/conversations/{conversation_id}"),
            TENANT_ID,
            USER_ID,
            "full-only-delete",
        ),
    )
    .await;
    assert_eq!(deleted.status(), StatusCode::OK);
    assert_eq!(json_body(deleted).await["data"]["deleted"], true);
    fixture
        .service
        .run_full_conversation_maintenance_once()
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let deletion_jobs =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM terminal_content_deletion_jobs")
                .fetch_one(&fixture.inspection)
                .await
                .unwrap();
        let all_deleted =
            futures::future::join_all(referenced_artifacts.iter().map(|(artifact, locator)| {
                fixture
                    .artifact_store
                    .read_and_verify(artifact, locator, 2 * 1024 * 1024)
            }))
            .await
            .into_iter()
            .all(|result| result.is_err());
        if deletion_jobs == 0 && all_deleted {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "full-only maintenance did not drain privacy deletion jobs"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let redacted_run = send(
        &fixture.app,
        Request::get(format!("/v1/runs/{run_id}"))
            .header("x-tenant-id", TENANT_ID)
            .header("x-user-id", USER_ID)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(redacted_run.status(), StatusCode::OK);
    let redacted_run = json_body(redacted_run).await;
    assert!(redacted_run["data"]["output"]["data"].is_null());
    assert!(
        !redacted_run.to_string().contains("full-only-durable-large"),
        "deleted Conversation payload leaked through Run GET"
    );
    let execution_graph = send(
        &fixture.app,
        Request::get(format!("/v1/runs/{run_id}/execution-graph"))
            .header("x-tenant-id", TENANT_ID)
            .header("x-user-id", USER_ID)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(execution_graph.status(), StatusCode::OK);
    assert!(
        !String::from_utf8(
            to_bytes(execution_graph.into_body(), MAX_HTTP_BODY_BYTES)
                .await
                .unwrap()
                .to_vec()
        )
        .unwrap()
        .contains("full-only-durable-large"),
        "deleted Conversation payload leaked through execution graph"
    );
    let trace = send(
        &fixture.app,
        Request::get(format!("/v1/runs/{run_id}/trace"))
            .header("x-tenant-id", TENANT_ID)
            .header("x-user-id", USER_ID)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(trace.status(), StatusCode::NOT_FOUND);
    let artifact = send(
        &fixture.app,
        Request::get(format!(
            "/v1/runs/{run_id}/artifacts/artifact_deleted_probe"
        ))
        .header("x-tenant-id", TENANT_ID)
        .header("x-user-id", USER_ID)
        .body(Body::empty())
        .unwrap(),
    )
    .await;
    assert_eq!(artifact.status(), StatusCode::NOT_FOUND);

    for path in [
        format!("/v1/runs/{run_id}/pause"),
        format!("/v1/runs/{run_id}/resume"),
        format!("/v1/runs/{run_id}/redrive"),
        format!("/v1/runs/{run_id}/fork"),
        format!("/v1/runs/{run_id}/migrate"),
        format!("/v1/runs/{run_id}/continue-as-new"),
        format!("/v1/runs/{run_id}/signals/deleted-probe"),
    ] {
        let hidden = send(
            &fixture.app,
            Request::post(path)
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-tenant-id", TENANT_ID)
                .header("x-user-id", USER_ID)
                .header("x-request-id", "full-only-deleted-control")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert_eq!(hidden.status(), StatusCode::NOT_FOUND);
        assert!(
            !String::from_utf8(
                to_bytes(hidden.into_body(), MAX_HTTP_BODY_BYTES)
                    .await
                    .unwrap()
                    .to_vec()
            )
            .unwrap()
            .contains("full-only-durable-large"),
            "deleted Conversation payload leaked through a Run control surface"
        );
    }

    let cancelled_tombstone = send(
        &fixture.app,
        Request::delete(format!("/v1/runs/{run_id}"))
            .header("x-tenant-id", TENANT_ID)
            .header("x-user-id", USER_ID)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(cancelled_tombstone.status(), StatusCode::OK);
    let cancelled_tombstone = json_body(cancelled_tombstone).await;
    assert!(cancelled_tombstone["data"]["output"]["data"].is_null());
    assert!(
        !cancelled_tombstone
            .to_string()
            .contains("full-only-durable-large"),
        "deleted Conversation payload leaked through Run cancellation"
    );
    assert_eq!(
        send(
            &fixture.app,
            conversation_read(
                format!("/v1/conversations/{conversation_id}"),
                TENANT_ID,
                USER_ID,
            ),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn full_conversation_large_assistant_stage_and_terminal_result_commit_atomically() {
    let fixture = TerminalHttpFixture::start().await;
    let created = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            "/v1/conversations",
            TENANT_ID,
            USER_ID,
            "full-atomic-stage-create",
            json!({"agent_id": FULL_AGENT_ID}),
        ),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);
    let conversation_id = json_body(created).await["data"]["conversation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    sqlx::query(
        "CREATE TRIGGER test_fail_full_assistant_insert
         BEFORE INSERT ON conversation_messages
         WHEN NEW.role='assistant'
         BEGIN
           SELECT RAISE(FAIL, 'injected assistant insert failure');
         END",
    )
    .execute(&fixture.inspection)
    .await
    .unwrap();

    let large_content = format!("atomic-assistant:{}", "a".repeat(700));
    let turn = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            "full-atomic-stage-turn",
            json!({"query": large_content.clone()}),
        ),
    )
    .await;
    assert_eq!(turn.status(), StatusCode::ACCEPTED);
    let accepted_run_id = turn.headers()["x-run-id"].to_str().unwrap().to_owned();
    let accepted_message_id = turn.headers()["x-message-id"].to_str().unwrap().to_owned();
    let turn_body = json_body(turn).await;
    assert_eq!(turn_body["data"]["replayed"], false);
    assert_eq!(turn_body["data"]["run"]["run_id"], accepted_run_id);
    assert_eq!(
        turn_body["data"]["user_message"]["message_id"],
        accepted_message_id
    );

    let replay = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            "full-atomic-stage-turn",
            json!({"query": large_content}),
        ),
    )
    .await;
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    assert_eq!(replay.headers()["x-run-id"], accepted_run_id);
    assert_eq!(replay.headers()["x-message-id"], accepted_message_id);
    let replay_body = json_body(replay).await;
    assert_eq!(replay_body["data"]["replayed"], true);
    assert_eq!(replay_body["data"]["run"]["run_id"], accepted_run_id);
    assert_eq!(
        replay_body["data"]["user_message"]["message_id"],
        accepted_message_id
    );

    let (run_id, message_id) = sqlx::query_as::<_, (String, String)>(
        "SELECT run_id,user_message_id FROM full_conversation_turns
         WHERE tenant_id=? AND admission_id LIKE 'idem_%' AND run_id=? AND conversation_id=?",
    )
    .bind(TENANT_ID)
    .bind(&accepted_run_id)
    .bind(&conversation_id)
    .fetch_one(&fixture.inspection)
    .await
    .unwrap();
    assert_eq!(run_id, accepted_run_id);
    assert_eq!(message_id, accepted_message_id);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let staged = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM terminal_artifact_staging
             WHERE source_kind='assistant_message' AND source_id=?",
        )
        .bind(&run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap();
        if staged == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "large assistant artifact was never staged before terminal commit"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (lifecycle, terminal_event_id, output_payload_id, output_artifact_id) =
        sqlx::query_as::<_, (String, Option<String>, Option<String>, Option<String>)>(
            "SELECT lifecycle,terminal_event_id,output_payload_id,output_artifact_id
             FROM workflow_runs WHERE run_id=?",
        )
        .bind(&run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap();
    assert_ne!(lifecycle, "succeeded");
    assert!(terminal_event_id.is_none());
    assert!(output_payload_id.is_none());
    assert!(output_artifact_id.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM conversation_messages
             WHERE conversation_id=? AND run_id=? AND role='assistant'",
        )
        .bind(&conversation_id)
        .bind(&run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        0
    );

    sqlx::query("DROP TRIGGER test_fail_full_assistant_insert")
        .execute(&fixture.inspection)
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        fixture.service.reconcile_startup().await.unwrap();
        let lifecycle =
            sqlx::query_scalar::<_, String>("SELECT lifecycle FROM workflow_runs WHERE run_id=?")
                .bind(&run_id)
                .fetch_one(&fixture.inspection)
                .await
                .unwrap();
        if lifecycle == "succeeded" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Run did not recover its rolled-back terminal commit; last lifecycle: {lifecycle}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (terminal_event_id, output_payload_id, output_artifact_id) =
        sqlx::query_as::<_, (Option<String>, Option<String>, Option<String>)>(
            "SELECT terminal_event_id,output_payload_id,output_artifact_id
             FROM workflow_runs WHERE run_id=?",
        )
        .bind(&run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap();
    assert!(terminal_event_id.is_some());
    assert_ne!(output_payload_id.is_some(), output_artifact_id.is_some());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM conversation_messages
             WHERE conversation_id=? AND run_id=? AND role='assistant'",
        )
        .bind(&conversation_id)
        .bind(&run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM terminal_artifact_staging
             WHERE source_kind='assistant_message' AND source_id=?",
        )
        .bind(&run_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        0
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn full_conversation_rejects_subflow_lineage_before_durable_turn_admission() {
    let fixture = TerminalHttpFixture::start_full_only().await;
    let created = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            "/v1/conversations",
            TENANT_ID,
            USER_ID,
            "full-subflow-create",
            json!({"agent_id": FULL_SUBFLOW_PARENT_AGENT_ID}),
        ),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);
    let conversation_id = json_body(created).await["data"]["conversation_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let rejected = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            "full-subflow-turn",
            json!({"query": "must not create a child lineage"}),
        ),
    )
    .await;
    assert_eq!(rejected.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        json_body(rejected).await["error"]["code"],
        "RUN_CAPABILITY_UNAVAILABLE"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM conversation_messages WHERE conversation_id=?"
        )
        .bind(&conversation_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM full_conversation_turns WHERE conversation_id=?"
        )
        .bind(&conversation_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        0
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn full_persistence_conversation_uses_latest_summary_and_only_post_boundary_recent_messages()
{
    let fixture = TerminalHttpFixture::start_with_summary_policy(4, 2).await;
    let created = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            "/v1/conversations",
            TENANT_ID,
            USER_ID,
            "full-summary-create",
            json!({"agent_id": FULL_AGENT_ID}),
        ),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);
    let conversation_id = json_body(created).await["data"]["conversation_id"]
        .as_str()
        .unwrap()
        .to_owned();

    for turn in 1..=3 {
        let response = send(
            &fixture.app,
            conversation_write(
                Method::POST,
                format!("/v1/conversations/{conversation_id}/messages"),
                TENANT_ID,
                USER_ID,
                &format!("full-summary-turn-{turn}"),
                json!({"query": format!("summary message {turn}")}),
            ),
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let run_id = response.headers()["x-run-id"].to_str().unwrap().to_owned();
        wait_for_assistant(&fixture.app, &conversation_id, &run_id).await;
        if turn >= 2 {
            let expected_boundary = i64::from((turn - 1) * 2);
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let boundary = sqlx::query_scalar::<_, Option<i64>>(
                    "SELECT MAX(through_message_order)
                     FROM conversation_summaries WHERE conversation_id=?",
                )
                .bind(&conversation_id)
                .fetch_one(&fixture.inspection)
                .await
                .unwrap();
                if boundary == Some(expected_boundary) {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "summary boundary did not reach {expected_boundary}: {boundary:?}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM conversation_summaries WHERE conversation_id=?"
        )
        .bind(&conversation_id)
        .fetch_one(&fixture.inspection)
        .await
        .unwrap(),
        2
    );

    let fourth = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            "full-summary-turn-4",
            json!({"query": "summary message 4"}),
        ),
    )
    .await;
    assert_eq!(fourth.status(), StatusCode::ACCEPTED);
    let fourth_run_id = fourth.headers()["x-run-id"].to_str().unwrap().to_owned();
    let input = sqlx::query_scalar::<_, String>(
        "SELECT p.inline_value
         FROM workflow_runs r
         JOIN payloads p ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id
         WHERE r.run_id=?",
    )
    .bind(&fourth_run_id)
    .fetch_one(&fixture.inspection)
    .await
    .unwrap();
    let input: Value = serde_json::from_str(&input).unwrap();
    let messages = input["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0]["role"], "assistant");
    assert!(messages[0]["content"][0]["text"]
        .as_str()
        .unwrap()
        .starts_with("Conversation summary: "));
    assert!(messages[1..].iter().all(|message| {
        message["content"][0]["text"]
            .as_str()
            .is_some_and(|text| text.contains("summary message 3"))
    }));
    wait_for_assistant(&fixture.app, &conversation_id, &fourth_run_id).await;

    let (latest_boundary, latest_ref, latest_hash) = sqlx::query_as::<_, (i64, String, String)>(
        "SELECT through_message_order,summary_ref,summary_hash
             FROM conversation_summaries
             WHERE conversation_id=?
             ORDER BY through_message_order DESC LIMIT 1",
    )
    .bind(&conversation_id)
    .fetch_one(&fixture.inspection)
    .await
    .unwrap();
    let (latest_artifact, latest_locator) =
        decode_artifact_reference(fixture.artifact_store.as_ref(), &latest_ref);
    fixture
        .artifact_store
        .delete(&latest_artifact, &latest_locator)
        .await
        .unwrap();
    let latest_message_order = sqlx::query_scalar::<_, i64>(
        "SELECT MAX(message_order) FROM conversation_messages WHERE conversation_id=?",
    )
    .bind(&conversation_id)
    .fetch_one(&fixture.inspection)
    .await
    .unwrap();
    assert!(latest_message_order > latest_boundary);
    sqlx::query(
        "INSERT INTO conversation_summaries (
             conversation_id,through_message_order,summary_ref,summary_hash,
             model_revision,created_at
         ) VALUES (?,?,?,?,?,?)",
    )
    .bind(&conversation_id)
    .bind(latest_message_order)
    .bind(&latest_ref)
    .bind(&latest_hash)
    .bind("missing-object-test")
    .bind(chrono::Utc::now())
    .execute(&fixture.inspection)
    .await
    .unwrap();
    let mut expected_fallback_orders = sqlx::query_scalar::<_, i64>(
        "SELECT message_order FROM conversation_messages
         WHERE conversation_id=?
         ORDER BY message_order DESC LIMIT 2",
    )
    .bind(&conversation_id)
    .fetch_all(&fixture.inspection)
    .await
    .unwrap();
    expected_fallback_orders.reverse();

    let fifth = send(
        &fixture.app,
        conversation_write(
            Method::POST,
            format!("/v1/conversations/{conversation_id}/messages"),
            TENANT_ID,
            USER_ID,
            "full-summary-turn-missing-object",
            json!({"query": "summary missing fallback"}),
        ),
    )
    .await;
    assert_eq!(fifth.status(), StatusCode::ACCEPTED);
    let fifth_run_id = fifth.headers()["x-run-id"].to_str().unwrap().to_owned();
    let input = sqlx::query_scalar::<_, String>(
        "SELECT p.inline_value
         FROM workflow_runs r
         JOIN payloads p ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id
         WHERE r.run_id=?",
    )
    .bind(&fifth_run_id)
    .fetch_one(&fixture.inspection)
    .await
    .unwrap();
    let input: Value = serde_json::from_str(&input).unwrap();
    let fallback = input["messages"].as_array().unwrap();
    assert_eq!(fallback.len(), expected_fallback_orders.len());
    assert!(fallback.iter().all(|message| {
        matches!(message["role"].as_str(), Some("user" | "assistant"))
            && message["content"]
                .as_array()
                .is_some_and(|content| !content.is_empty())
    }));
    wait_for_assistant(&fixture.app, &conversation_id, &fifth_run_id).await;

    fixture.shutdown().await;
}
