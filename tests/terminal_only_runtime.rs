use std::{
    fs::File,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use insight_dsl::{CompileOptions, GraphAuthorDocument, GraphDocumentId};
use insight_engine::{
    artifact_store::{ArtifactStoreDeploymentContract, WorkerArtifactStore},
    author::CompileError,
    history::types::RunLifecycle,
    plan::{LeafTaskDescriptor, LeafTaskKind, SubflowContractRegistry},
    repository::{RepositoryError, StorageLocator},
    run_stream::{
        LiveRunStreamBrokerCapability, LiveRunStreamBrokerError, LiveRunStreamCloseOutcome,
        LiveRunStreamPublication, LiveRunStreamPublishOutcome, LiveRunStreamSeal,
        LiveRunStreamSubscriber,
    },
    worker::WorkerExecutorRegistry,
    ArtifactId, ArtifactRef, ContentHash, DefinitionRevisionId, RunId,
};
use insight_runtime::{
    catalog::{
        DeployedAgent, DeploymentPersistencePolicy, LeafDeploymentResolver, PublishedAgent,
        ResolvedLeafDeployment,
    },
    AgentInvocation, DeployedAgentCatalog, InMemoryLiveRunStreamBroker, LiveRunStreamBroker,
    TerminalOnlyRunConfig, TerminalOnlyRunEngine, TerminalOnlyStore,
};
use insight_storage::{artifact_store::LocalContentAddressedArtifactStore, *};
use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;

const SQLITE_SCHEMA: &str = include_str!("../database/durable/sqlite/schema.sql");

async fn provision_sqlite_database(path: &Path) {
    File::create(path).unwrap();
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::raw_sql(SQLITE_SCHEMA).execute(&pool).await.unwrap();
    pool.close().await;
}

struct NoLeafResolver;

impl LeafDeploymentResolver for NoLeafResolver {
    fn resolve_leaf(
        &self,
        _kind: LeafTaskKind,
        _descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        unreachable!("the native terminal-only fixtures contain no leaf task")
    }
}

struct Fixture {
    _temporary: TempDir,
    repository: Arc<SqliteDurableRepository>,
    artifact_store: Arc<dyn WorkerArtifactStore>,
    engine: TerminalOnlyRunEngine,
}

impl Fixture {
    async fn new(source: &str, allow_volatile_waits: bool) -> Self {
        Self::new_with(
            source,
            allow_volatile_waits,
            Arc::new(InMemoryLiveRunStreamBroker::new(32, 8).unwrap()),
            terminal_config(),
        )
        .await
    }

    async fn new_with(
        source: &str,
        allow_volatile_waits: bool,
        broker: Arc<dyn LiveRunStreamBroker>,
        config: TerminalOnlyRunConfig,
    ) -> Self {
        Self::new_with_store_wrapper(source, allow_volatile_waits, broker, config, |store| store)
            .await
    }

    async fn new_with_store_wrapper<F>(
        source: &str,
        allow_volatile_waits: bool,
        broker: Arc<dyn LiveRunStreamBroker>,
        config: TerminalOnlyRunConfig,
        wrap_store: F,
    ) -> Self
    where
        F: FnOnce(Arc<dyn WorkerArtifactStore>) -> Arc<dyn WorkerArtifactStore>,
    {
        Self::new_with_wrappers(
            source,
            allow_volatile_waits,
            broker,
            config,
            |repository| -> Arc<dyn TerminalOnlyStore> { repository },
            wrap_store,
        )
        .await
    }

    async fn new_with_terminal_store_wrapper<F>(
        source: &str,
        allow_volatile_waits: bool,
        broker: Arc<dyn LiveRunStreamBroker>,
        config: TerminalOnlyRunConfig,
        wrap_store: F,
    ) -> Self
    where
        F: FnOnce(Arc<SqliteDurableRepository>) -> Arc<dyn TerminalOnlyStore>,
    {
        Self::new_with_wrappers(
            source,
            allow_volatile_waits,
            broker,
            config,
            wrap_store,
            |artifact_store| artifact_store,
        )
        .await
    }

    async fn new_with_wrappers<FS, FA>(
        source: &str,
        allow_volatile_waits: bool,
        broker: Arc<dyn LiveRunStreamBroker>,
        config: TerminalOnlyRunConfig,
        wrap_store: FS,
        wrap_artifact_store: FA,
    ) -> Self
    where
        FS: FnOnce(Arc<SqliteDurableRepository>) -> Arc<dyn TerminalOnlyStore>,
        FA: FnOnce(Arc<dyn WorkerArtifactStore>) -> Arc<dyn WorkerArtifactStore>,
    {
        let temporary = tempfile::tempdir().unwrap();
        provision_sqlite_database(&temporary.path().join("terminal.sqlite")).await;
        let repository = Arc::new(
            SqliteDurableRepository::connect_path(&temporary.path().join("terminal.sqlite"))
                .await
                .unwrap(),
        );
        let graph = GraphAuthorDocument::from_structured_source(
            GraphDocumentId::new("terminal_fixture_graph").unwrap(),
            source,
            CompileOptions::new(
                DefinitionRevisionId::new("terminal_fixture_definition").unwrap(),
                "terminal-fixture/agent.yaml",
                source,
            ),
        )
        .unwrap();
        let published = Arc::new(
            PublishedAgent::from_verified_graph(
                "terminal_fixture",
                "Terminal fixture",
                "terminal-only runtime fixture",
                graph,
            )
            .unwrap(),
        );
        let deployed = Arc::new(
            DeployedAgent::publish_with_persistence_policy(
                published,
                &NoLeafResolver,
                SubflowContractRegistry::new(),
                DeploymentPersistencePolicy::terminal_only(
                    allow_volatile_waits,
                    Duration::from_secs(3),
                )
                .unwrap(),
            )
            .unwrap(),
        );
        let agents = DeployedAgentCatalog::new(vec![deployed]).unwrap();
        let store = wrap_store(repository.clone());
        let artifact_store: Arc<dyn WorkerArtifactStore> = Arc::new(
            LocalContentAddressedArtifactStore::open(temporary.path().join("objects"), 64)
                .await
                .unwrap(),
        );
        let artifact_store = wrap_artifact_store(artifact_store);
        let engine = TerminalOnlyRunEngine::start(
            store,
            agents,
            Arc::new(WorkerExecutorRegistry::new()),
            Arc::clone(&artifact_store),
            broker,
            "http://127.0.0.1:0".to_owned(),
            config,
        )
        .await
        .unwrap();
        Self {
            _temporary: temporary,
            repository,
            artifact_store,
            engine,
        }
    }

    async fn wait_terminal(
        &self,
        tenant_id: &str,
        user_id: Option<&str>,
        run_id: &str,
    ) -> insight_engine::history::types::RunRecord {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let run = self
                    .engine
                    .get_run_for_principal(tenant_id, user_id, run_id)
                    .await
                    .unwrap();
                if run.status().is_terminal() {
                    return run;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("terminal-only fixture did not reach terminal state")
    }
}

struct AdmissionResponseLossState {
    remaining_losses: AtomicUsize,
    admission_delegations: AtomicUsize,
    result_commits: AtomicUsize,
    hang_heartbeat: AtomicBool,
}

impl AdmissionResponseLossState {
    fn new(remaining_losses: usize) -> Self {
        Self {
            remaining_losses: AtomicUsize::new(remaining_losses),
            admission_delegations: AtomicUsize::new(0),
            result_commits: AtomicUsize::new(0),
            hang_heartbeat: AtomicBool::new(false),
        }
    }

    fn drop_response(&self) -> bool {
        self.remaining_losses
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
    }
}

struct AmbiguousAdmissionStore {
    inner: Arc<dyn TerminalOnlyStore>,
    state: Arc<AdmissionResponseLossState>,
}

fn ambiguous_storage_failure() -> RepositoryError {
    insight_engine::repository::adapter::repository_error(
        insight_engine::repository::REPOSITORY_STORAGE_FAILURE,
        "injected response loss after authoritative admission commit",
    )
}

#[async_trait]
impl TerminalRunStore for AmbiguousAdmissionStore {
    async fn register_runtime_instance(
        &self,
        command: RegisterRuntimeInstance,
    ) -> Result<RuntimeInstanceLease, RepositoryError> {
        self.inner.register_runtime_instance(command).await
    }

    async fn heartbeat_runtime_instance(
        &self,
        command: HeartbeatRuntimeInstance,
    ) -> Result<OwnerLeaseHeartbeat, RepositoryError> {
        if self.state.hang_heartbeat.load(Ordering::Acquire) {
            std::future::pending().await
        }
        self.inner.heartbeat_runtime_instance(command).await
    }

    async fn check_runtime_owner(
        &self,
        query: OwnerLeaseQuery,
    ) -> Result<OwnerLeaseStatus, RepositoryError> {
        self.inner.check_runtime_owner(query).await
    }

    async fn unregister_runtime_instance(
        &self,
        owner: RuntimeOwner,
    ) -> Result<bool, RepositoryError> {
        self.inner.unregister_runtime_instance(owner).await
    }

    async fn admit_terminal_run(
        &self,
        command: NewTerminalRunAdmission,
    ) -> Result<AdmissionOutcome, RepositoryError> {
        self.state
            .admission_delegations
            .fetch_add(1, Ordering::Relaxed);
        let outcome = self.inner.admit_terminal_run(command).await?;
        if self.state.drop_response() {
            Err(ambiguous_storage_failure())
        } else {
            Ok(outcome)
        }
    }

    async fn get_terminal_run(
        &self,
        query: TerminalRunQuery,
    ) -> Result<Option<TerminalRunView>, RepositoryError> {
        self.inner.get_terminal_run(query).await
    }

    async fn get_terminal_run_by_request(
        &self,
        query: TerminalRunRequestQuery,
    ) -> Result<Option<TerminalRunView>, RepositoryError> {
        self.inner.get_terminal_run_by_request(query).await
    }

    async fn commit_terminal_result(
        &self,
        command: NewTerminalRunResult,
    ) -> Result<TerminalCommitOutcome, RepositoryError> {
        self.state.result_commits.fetch_add(1, Ordering::Relaxed);
        self.inner.commit_terminal_result(command).await
    }

    async fn delete_terminal_runs_before(
        &self,
        retention: BoundedRetention,
    ) -> Result<RetentionDeleteOutcome, RepositoryError> {
        self.inner.delete_terminal_runs_before(retention).await
    }
}

#[async_trait]
impl TerminalContentDeletionStore for AmbiguousAdmissionStore {
    async fn claim_content_deletion_jobs(
        &self,
        command: ClaimContentDeletionJobs,
    ) -> Result<Vec<TerminalContentDeletionClaim>, RepositoryError> {
        self.inner.claim_content_deletion_jobs(command).await
    }

    async fn ack_content_deletion_job(
        &self,
        command: AckContentDeletionJob,
    ) -> Result<bool, RepositoryError> {
        self.inner.ack_content_deletion_job(command).await
    }
}

#[async_trait]
impl TerminalArtifactStagingStore for AmbiguousAdmissionStore {
    async fn stage_terminal_artifact(
        &self,
        command: NewTerminalArtifactStage,
    ) -> Result<StageTerminalArtifactOutcome, RepositoryError> {
        self.inner.stage_terminal_artifact(command).await
    }

    async fn claim_terminal_artifact_stages(
        &self,
        command: ClaimTerminalArtifactStages,
    ) -> Result<Vec<TerminalArtifactStageClaim>, RepositoryError> {
        self.inner.claim_terminal_artifact_stages(command).await
    }

    async fn resolve_terminal_artifact_stage(
        &self,
        command: ResolveTerminalArtifactStage,
    ) -> Result<TerminalArtifactStageDisposition, RepositoryError> {
        self.inner.resolve_terminal_artifact_stage(command).await
    }

    async fn ack_terminal_artifact_stage(
        &self,
        command: AckTerminalArtifactStage,
    ) -> Result<bool, RepositoryError> {
        self.inner.ack_terminal_artifact_stage(command).await
    }
}

#[async_trait]
impl ConversationStore for AmbiguousAdmissionStore {
    async fn create_conversation(
        &self,
        command: NewConversation,
    ) -> Result<CreateConversationOutcome, RepositoryError> {
        self.inner.create_conversation(command).await
    }

    async fn get_conversation(
        &self,
        query: ConversationQuery,
    ) -> Result<Option<Conversation>, RepositoryError> {
        self.inner.get_conversation(query).await
    }

    async fn create_conversation_turn(
        &self,
        command: NewConversationTurn,
    ) -> Result<ConversationTurnOutcome, RepositoryError> {
        self.inner.create_conversation_turn(command).await
    }

    async fn get_terminal_conversation_turn(
        &self,
        query: FullConversationTurnQuery,
    ) -> Result<Option<ConversationTurnOutcome>, RepositoryError> {
        self.inner.get_terminal_conversation_turn(query).await
    }

    async fn get_full_conversation_turn(
        &self,
        query: FullConversationTurnQuery,
    ) -> Result<Option<FullConversationTurn>, RepositoryError> {
        self.inner.get_full_conversation_turn(query).await
    }

    async fn full_conversation_run_tenant(
        &self,
        run_id: &RunId,
    ) -> Result<Option<String>, RepositoryError> {
        self.inner.full_conversation_run_tenant(run_id).await
    }

    async fn full_conversation_run_user(
        &self,
        run_id: &RunId,
    ) -> Result<Option<String>, RepositoryError> {
        self.inner.full_conversation_run_user(run_id).await
    }

    async fn full_conversation_run_conversation_id(
        &self,
        run_id: &RunId,
    ) -> Result<Option<String>, RepositoryError> {
        self.inner
            .full_conversation_run_conversation_id(run_id)
            .await
    }

    async fn full_conversation_run_is_deleted(
        &self,
        run_id: &RunId,
    ) -> Result<Option<bool>, RepositoryError> {
        self.inner.full_conversation_run_is_deleted(run_id).await
    }

    async fn list_full_conversation_run_ids(
        &self,
        query: ConversationQuery,
    ) -> Result<Vec<RunId>, RepositoryError> {
        self.inner.list_full_conversation_run_ids(query).await
    }

    async fn commit_conversation_turn(
        &self,
        command: CommitConversationTurn,
    ) -> Result<ConversationTerminalCommitOutcome, RepositoryError> {
        self.inner.commit_conversation_turn(command).await
    }

    async fn page_conversation_messages(
        &self,
        query: MessagePageQuery,
    ) -> Result<Option<ConversationMessagePage>, RepositoryError> {
        self.inner.page_conversation_messages(query).await
    }

    async fn archive_conversation(
        &self,
        query: ConversationQuery,
        archived_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<ArchiveOutcome, RepositoryError> {
        self.inner.archive_conversation(query, archived_at).await
    }

    async fn delete_conversation(
        &self,
        query: ConversationQuery,
    ) -> Result<PrivacyDeleteOutcome, RepositoryError> {
        self.inner.delete_conversation(query).await
    }

    async fn put_conversation_summary(
        &self,
        summary: NewConversationSummary,
    ) -> Result<SummaryOutcome, RepositoryError> {
        self.inner.put_conversation_summary(summary).await
    }

    async fn try_claim_conversation_summary_job(
        &self,
        command: ClaimConversationSummaryJob,
    ) -> Result<bool, RepositoryError> {
        self.inner.try_claim_conversation_summary_job(command).await
    }

    async fn release_conversation_summary_job(
        &self,
        command: ReleaseConversationSummaryJob,
    ) -> Result<bool, RepositoryError> {
        self.inner.release_conversation_summary_job(command).await
    }

    async fn latest_conversation_summary(
        &self,
        query: ConversationQuery,
    ) -> Result<Option<ConversationSummary>, RepositoryError> {
        self.inner.latest_conversation_summary(query).await
    }

    async fn load_conversation_context(
        &self,
        query: ConversationContextQuery,
    ) -> Result<Option<ConversationContext>, RepositoryError> {
        self.inner.load_conversation_context(query).await
    }

    async fn delete_conversations_before(
        &self,
        retention: BoundedRetention,
    ) -> Result<ConversationRetentionOutcome, RepositoryError> {
        self.inner.delete_conversations_before(retention).await
    }
}

#[derive(Default)]
struct PutCrashState {
    crash_next_put: AtomicBool,
    deleted: AtomicUsize,
    artifact: Mutex<Option<ArtifactRef>>,
}

struct PanicAfterSuccessfulPutStore {
    inner: Arc<dyn WorkerArtifactStore>,
    state: Arc<PutCrashState>,
}

#[async_trait]
impl WorkerArtifactStore for PanicAfterSuccessfulPutStore {
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
        let stored = self.inner.put_and_verify(artifact, bytes).await?;
        *self
            .state
            .artifact
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(artifact.clone());
        if self.state.crash_next_put.swap(false, Ordering::AcqRel) {
            panic!("failpoint: hard crash after object put and before terminal metadata commit");
        }
        Ok(stored)
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
        self.inner.delete(artifact, locator).await?;
        self.state.deleted.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn terminal_config() -> TerminalOnlyRunConfig {
    TerminalOnlyRunConfig {
        owner_lease: Duration::from_secs(3),
        owner_heartbeat: Duration::from_secs(1),
        terminal_commit_retry: Duration::from_secs(1),
        run_timeout: Duration::from_secs(2),
        max_concurrent_runs: 8,
        conversations_enabled: true,
        inline_content_max_bytes: 256,
        message_page_size_default: 2,
        message_page_size_max: 20,
        recent_context_messages: 4,
        summary_trigger_messages: 6,
        summary_trigger_tokens: 256,
        run_retention: Duration::from_secs(24 * 60 * 60),
        conversation_retention: Duration::from_secs(24 * 60 * 60),
        terminal_barrier_timeout: Duration::from_secs(1),
        outbound_write_timeout: Duration::from_secs(1),
    }
}

struct FailingSubscribeBroker;

#[async_trait]
impl LiveRunStreamBroker for FailingSubscribeBroker {
    fn deployment_capability(&self) -> LiveRunStreamBrokerCapability {
        LiveRunStreamBrokerCapability::SingleProcess
    }

    async fn subscribe(
        &self,
        _run_id: RunId,
    ) -> Result<Box<dyn LiveRunStreamSubscriber>, LiveRunStreamBrokerError> {
        Err(LiveRunStreamBrokerError::new(
            "TEST_SUBSCRIBE_FAILED",
            "test subscriber failed",
        ))
    }

    fn publish(&self, _publication: LiveRunStreamPublication) -> LiveRunStreamPublishOutcome {
        LiveRunStreamPublishOutcome::NoSubscriber
    }

    fn seal(&self, _seal: LiveRunStreamSeal) -> LiveRunStreamPublishOutcome {
        LiveRunStreamPublishOutcome::NoSubscriber
    }

    fn close_run(&self, _run_id: &RunId) -> LiveRunStreamCloseOutcome {
        LiveRunStreamCloseOutcome::default()
    }
}

const PASSTHROUGH_AGENT: &str = r#"
api_version: insight.agent/v1
kind: agent
inputs:
  query: string
  messages: {type: "Message[]", default: []}
output: string
workflow:
  steps:
    - return: $query
"#;

const CONTEXT_AGENT: &str = r#"
api_version: insight.agent/v1
kind: agent
inputs:
  query: string
  messages: {type: "Message[]", default: []}
output: "Message[]"
workflow:
  steps:
    - return: $messages
"#;

fn invocation(value: Value) -> AgentInvocation {
    let query = value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| serde_jcs::to_string(&value).unwrap());
    AgentInvocation {
        query: Some(query),
        messages: None,
        files: None,
        inputs: None,
    }
}

fn empty_invocation() -> AgentInvocation {
    AgentInvocation {
        query: None,
        messages: None,
        files: None,
        inputs: None,
    }
}

fn invocation_query(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| serde_jcs::to_string(value).unwrap())
}

fn text_content(value: &Value) -> Value {
    json!([{"text": invocation_query(value)}])
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

#[test]
fn terminal_run_timeout_must_fit_inside_owner_lease() {
    let mut config = terminal_config();
    config.run_timeout = config.owner_lease + Duration::from_millis(1);
    let error = config.validate().unwrap_err();
    assert_eq!(error.code(), "RUN_SERVICE_CONFIG_INVALID");
}

#[tokio::test]
async fn conversation_turns_are_atomic_idempotent_ordered_and_private() {
    let fixture = Fixture::new(PASSTHROUGH_AGENT, false).await;
    let conversation = fixture
        .engine
        .create_conversation("tenant-a", "user-a", "terminal_fixture", "request-create")
        .await
        .unwrap();
    let replay = fixture
        .engine
        .create_conversation("tenant-a", "user-a", "terminal_fixture", "request-create")
        .await
        .unwrap();
    assert_eq!(replay.conversation_id, conversation.conversation_id);

    let large_content = json!({"text": "x".repeat(512)});
    let turn = fixture
        .engine
        .create_conversation_detached_invocation(
            &conversation.conversation_id,
            "tenant-a",
            "user-a",
            "request-turn-1",
            "request-trace-turn-1",
            invocation(large_content.clone()),
        )
        .await
        .unwrap();
    let terminal = fixture
        .wait_terminal("tenant-a", Some("user-a"), &turn.run.run_id)
        .await;
    match terminal.lifecycle {
        RunLifecycle::Completed { output } => {
            assert_eq!(output.data, invocation_query(&large_content))
        }
        lifecycle => panic!("unexpected lifecycle: {lifecycle:?}"),
    }

    let replayed = fixture
        .engine
        .create_conversation_detached_invocation(
            &conversation.conversation_id,
            "tenant-a",
            "user-a",
            "request-turn-1",
            "another-request-trace",
            invocation(large_content.clone()),
        )
        .await
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.run.run_id, turn.run.run_id);
    assert_eq!(
        replayed.user_message.message_id,
        turn.user_message.message_id
    );
    assert_eq!(replayed.user_message.content, text_content(&large_content));

    let page = fixture
        .engine
        .list_conversation_messages(
            &conversation.conversation_id,
            "tenant-a",
            "user-a",
            None,
            20,
        )
        .await
        .unwrap();
    assert_eq!(page.messages.len(), 2);
    assert_eq!(
        page.messages[0].role,
        insight_storage::ConversationRole::Assistant
    );
    assert_eq!(
        page.messages[1].role,
        insight_storage::ConversationRole::User
    );
    assert_eq!(page.messages[0].content, text_content(&large_content));
    assert_eq!(page.messages[1].content, text_content(&large_content));
    let metrics = fixture.engine.prometheus_metrics();
    for required in [
        "terminal_run_active ",
        "terminal_run_admissions_total ",
        "terminal_run_results_total{terminal_state=\"succeeded\"} ",
        "conversation_messages_total{role=\"user\",persistence_mode=\"terminal_only\"} ",
        "conversation_messages_total{role=\"assistant\",persistence_mode=\"terminal_only\"} ",
        "conversation_context_messages{persistence_mode=\"terminal_only\"} ",
        "conversation_context_tokens{persistence_mode=\"terminal_only\"} ",
    ] {
        assert!(metrics.contains(required), "missing metric: {required}");
    }
    for forbidden in [
        "tenant-a",
        "user-a",
        conversation.conversation_id.as_str(),
        turn.run.run_id.as_str(),
    ] {
        assert!(
            !metrics.contains(forbidden),
            "metrics must not contain request-scoped identity: {forbidden}"
        );
    }
    assert!(fixture
        .engine
        .get_conversation(&conversation.conversation_id, "tenant-a", "other-user",)
        .await
        .is_err());

    fixture
        .engine
        .archive_conversation(&conversation.conversation_id, "tenant-a", "user-a")
        .await
        .unwrap();
    let archived = fixture
        .engine
        .create_conversation_detached_invocation(
            &conversation.conversation_id,
            "tenant-a",
            "user-a",
            "request-turn-after-archive",
            "request-trace-after-archive",
            invocation(json!("blocked")),
        )
        .await
        .unwrap_err();
    assert_eq!(archived.code(), "CONVERSATION_ARCHIVED");

    assert!(fixture
        .engine
        .delete_conversation(&conversation.conversation_id, "tenant-a", "user-a",)
        .await
        .unwrap());
    assert!(fixture
        .engine
        .get_conversation(&conversation.conversation_id, "tenant-a", "user-a",)
        .await
        .is_err());
    fixture
        .engine
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn volatile_timer_executes_in_memory_and_owner_reset_derives_interrupted() {
    let source = r#"
api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: delay
      wait: {duration_ms: 20}
    - return: done
"#;
    let fixture = Fixture::new(source, true).await;
    let completed = fixture
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            empty_invocation(),
            "timer-admission".to_owned(),
            "request-timer".to_owned(),
        )
        .await
        .unwrap();
    let terminal = fixture
        .wait_terminal("default", None, &completed.run_id)
        .await;
    assert!(matches!(terminal.lifecycle, RunLifecycle::Completed { .. }));

    let long_source = source.replace("duration_ms: 20", "duration_ms: 1500");
    let long = Fixture::new(&long_source, true).await;
    let admitted = long
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            empty_invocation(),
            "interrupted-admission".to_owned(),
            "request-interrupted".to_owned(),
        )
        .await
        .unwrap();
    let restarted =
        SqliteDurableRepository::connect_path(&long._temporary.path().join("terminal.sqlite"))
            .await
            .unwrap();
    let view = restarted
        .get_terminal_run(TerminalRunQuery {
            tenant_id: "default".to_owned(),
            run_id: RunId::new(admitted.run_id).unwrap(),
            observed_at: chrono::Utc::now(),
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.state, TerminalRunDerivedState::Interrupted);
    assert!(view.result.is_none());

    fixture
        .engine
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
    long.engine.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn same_request_replay_precedes_active_capacity_rejection() {
    let source = r#"
api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: delay
      wait: {duration_ms: 1500}
    - return: done
"#;
    let mut config = terminal_config();
    config.max_concurrent_runs = 1;
    let fixture = Fixture::new_with(
        source,
        true,
        Arc::new(InMemoryLiveRunStreamBroker::new(32, 8).unwrap()),
        config,
    )
    .await;
    let first = fixture
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            empty_invocation(),
            "capacity-replay".to_owned(),
            "capacity-request-1".to_owned(),
        )
        .await
        .unwrap();
    assert_eq!(fixture.engine.active_run_count(), 1);
    let replay = fixture
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            empty_invocation(),
            "capacity-replay".to_owned(),
            "capacity-request-2".to_owned(),
        )
        .await
        .unwrap();
    assert_eq!(replay.run_id, first.run_id);
    let capacity = fixture
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            empty_invocation(),
            "capacity-new".to_owned(),
            "capacity-request-3".to_owned(),
        )
        .await
        .unwrap_err();
    assert_eq!(capacity.code(), "RUN_CAPACITY_EXCEEDED");
    fixture
        .engine
        .shutdown(Duration::from_secs(3))
        .await
        .unwrap();
}

#[tokio::test]
async fn ambiguous_admission_response_replays_exact_authority_and_executes_once() {
    let source = r#"
api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - return: done
"#;
    let state = Arc::new(AdmissionResponseLossState::new(1));
    let fixture = Fixture::new_with_terminal_store_wrapper(
        source,
        false,
        Arc::new(InMemoryLiveRunStreamBroker::new(32, 8).unwrap()),
        terminal_config(),
        {
            let state = Arc::clone(&state);
            move |repository| {
                let inner: Arc<dyn TerminalOnlyStore> = repository;
                Arc::new(AmbiguousAdmissionStore { inner, state })
            }
        },
    )
    .await;

    let admitted = fixture
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            empty_invocation(),
            "ambiguous-then-replay".to_owned(),
            "ambiguous-trace-1".to_owned(),
        )
        .await
        .unwrap();
    let terminal = fixture
        .wait_terminal("default", None, &admitted.run_id)
        .await;
    assert!(matches!(terminal.lifecycle, RunLifecycle::Completed { .. }));
    assert_eq!(state.admission_delegations.load(Ordering::Relaxed), 2);
    assert_eq!(state.result_commits.load(Ordering::Relaxed), 1);

    let replay = fixture
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            empty_invocation(),
            "ambiguous-then-replay".to_owned(),
            "ambiguous-trace-2".to_owned(),
        )
        .await
        .unwrap();
    assert_eq!(replay.run_id, admitted.run_id);
    assert_eq!(state.admission_delegations.load(Ordering::Relaxed), 2);
    assert_eq!(state.result_commits.load(Ordering::Relaxed), 1);
    fixture
        .engine
        .shutdown(Duration::from_secs(3))
        .await
        .unwrap();
}

#[tokio::test]
async fn repeated_ambiguous_admission_response_retires_owner_and_authority_interrupts() {
    let source = r#"
api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - return: done
"#;
    let state = Arc::new(AdmissionResponseLossState::new(2));
    let fixture = Fixture::new_with_terminal_store_wrapper(
        source,
        false,
        Arc::new(InMemoryLiveRunStreamBroker::new(32, 8).unwrap()),
        terminal_config(),
        {
            let state = Arc::clone(&state);
            move |repository| {
                let inner: Arc<dyn TerminalOnlyStore> = repository;
                Arc::new(AmbiguousAdmissionStore { inner, state })
            }
        },
    )
    .await;
    let retired_owner = fixture.engine.owner();
    let error = fixture
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            empty_invocation(),
            "ambiguous-twice".to_owned(),
            "ambiguous-twice-trace".to_owned(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "TERMINAL_ONLY_UNAVAILABLE");
    assert_eq!(state.admission_delegations.load(Ordering::Relaxed), 2);
    assert_eq!(state.result_commits.load(Ordering::Relaxed), 0);
    assert!(!fixture.engine.is_ready());

    let view = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let view = fixture
                .repository
                .get_terminal_run_by_request(TerminalRunRequestQuery {
                    tenant_id: "default".to_owned(),
                    admission_id: "ambiguous-twice".to_owned(),
                    observed_at: chrono::Utc::now(),
                })
                .await
                .unwrap()
                .expect("the first delegated call committed authority");
            if view.state == TerminalRunDerivedState::Interrupted {
                break view;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("ambiguous admission authority did not become interrupted");
    assert_eq!(view.admission.owner, retired_owner);
    assert!(view.result.is_none());

    tokio::time::timeout(Duration::from_secs(3), async {
        while !fixture.engine.is_ready() || fixture.engine.owner() == retired_owner {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("terminal owner did not recover after retiring ambiguous authority");
    fixture
        .engine
        .shutdown(Duration::from_secs(3))
        .await
        .unwrap();
}

#[tokio::test]
async fn hanging_heartbeat_stops_admission_and_cancels_active_at_confirmed_lease_threshold() {
    let source = r#"
api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: delay
      wait: {duration_ms: 2500}
    - return: done
"#;
    let state = Arc::new(AdmissionResponseLossState::new(0));
    let mut config = terminal_config();
    config.run_timeout = Duration::from_secs(3);
    let fixture = Fixture::new_with_terminal_store_wrapper(
        source,
        true,
        Arc::new(InMemoryLiveRunStreamBroker::new(32, 8).unwrap()),
        config,
        {
            let state = Arc::clone(&state);
            move |repository| {
                let inner: Arc<dyn TerminalOnlyStore> = repository;
                Arc::new(AmbiguousAdmissionStore { inner, state })
            }
        },
    )
    .await;
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    fixture
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            empty_invocation(),
            "heartbeat-hangs".to_owned(),
            "heartbeat-hangs-trace".to_owned(),
        )
        .await
        .unwrap();
    assert_eq!(fixture.engine.active_run_count(), 1);
    tokio::time::pause();
    state.hang_heartbeat.store(true, Ordering::Release);

    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    let metrics = fixture.engine.prometheus_metrics();
    assert!(!fixture.engine.is_ready());
    assert_eq!(fixture.engine.active_run_count(), 0, "{metrics}");
    assert!(metrics.contains("terminal_runtime_owner_lease_expired_total 1"));
    assert!(metrics.contains("terminal_run_interrupted_total{reason=\"owner_lost\"} 1"));
    let rejected = fixture
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            empty_invocation(),
            "heartbeat-hangs-new".to_owned(),
            "heartbeat-hangs-new-trace".to_owned(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            rejected.code(),
            "RUN_SERVICE_STOPPING" | "TERMINAL_ONLY_UNAVAILABLE"
        ),
        "lost-owner admission must fail closed, got {}",
        rejected.code()
    );

    tokio::time::resume();
    fixture
        .engine
        .shutdown(Duration::from_secs(3))
        .await
        .unwrap();
}

#[tokio::test]
async fn lost_owner_is_fenced_then_replaced_and_interrupted_replay_is_attachable() {
    let source = r#"
api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: delay
      wait: {duration_ms: 1500}
    - return: done
"#;
    let fixture = Fixture::new(source, true).await;
    let old_owner = fixture.engine.owner();
    let admitted = fixture
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            empty_invocation(),
            "request-owner-loss".to_owned(),
            "request-owner-loss-trace".to_owned(),
        )
        .await
        .unwrap();

    // Opening the SQLite repository models a process restart and deliberately
    // clears the ephemeral owner table.
    let _reset =
        SqliteDurableRepository::connect_path(&fixture._temporary.path().join("terminal.sqlite"))
            .await
            .unwrap();
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if fixture.engine.owner() != old_owner && fixture.engine.is_ready() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owner was not re-registered");
    let new_owner = fixture.engine.owner();
    assert_ne!(new_owner.instance_id, old_owner.instance_id);
    assert!(new_owner.owner_epoch > old_owner.owner_epoch);

    let interrupted = fixture
        .engine
        .get_run("default", &admitted.run_id)
        .await
        .unwrap();
    assert!(matches!(
        interrupted.lifecycle,
        RunLifecycle::Interrupted { .. }
    ));
    let run_id = RunId::new(admitted.run_id.clone()).unwrap();
    let late_commit = fixture
        .repository
        .commit_terminal_result(NewTerminalRunResult {
            run_id: run_id.clone(),
            owner: old_owner,
            terminal_state: TerminalState::Succeeded,
            response_id: format!("resp_{}", run_id.as_str()),
            output_ref: None,
            output_hash: None,
            error_code: None,
            usage_json: None,
            tool_results: Vec::new(),
            started_at: chrono::Utc::now(),
            terminal_at: chrono::Utc::now(),
        })
        .await
        .unwrap_err();
    assert_eq!(late_commit.code(), TERMINAL_RUN_OWNER_LEASE_LOST);

    let mut replay = fixture
        .engine
        .create_attached_invocation(
            "default",
            "terminal_fixture",
            empty_invocation(),
            "request-owner-loss".to_owned(),
            "request-owner-loss-replay-trace".to_owned(),
        )
        .await
        .unwrap();
    let snapshot = replay.subscription.recv_terminal().await.unwrap();
    assert!(snapshot.run().to_string().contains("RUN_INTERRUPTED"));

    let fresh = fixture
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            empty_invocation(),
            "request-after-owner-loss".to_owned(),
            "request-after-owner-loss-trace".to_owned(),
        )
        .await
        .unwrap();
    assert_ne!(fresh.run_id, admitted.run_id);
    fixture
        .engine
        .shutdown(Duration::from_secs(3))
        .await
        .unwrap();
}

#[tokio::test]
async fn subscribe_failure_still_executes_and_releases_active_capacity() {
    let mut config = terminal_config();
    config.max_concurrent_runs = 1;
    let fixture = Fixture::new_with(
        PASSTHROUGH_AGENT,
        false,
        Arc::new(FailingSubscribeBroker),
        config,
    )
    .await;
    let error = fixture
        .engine
        .create_attached_invocation(
            "default",
            "terminal_fixture",
            invocation(json!("first")),
            "subscribe-failure".to_owned(),
            "request-subscribe-failure".to_owned(),
        )
        .await;
    let error = match error {
        Ok(_) => panic!("subscriber failure unexpectedly returned an attachment"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "TERMINAL_ONLY_UNAVAILABLE");
    tokio::time::timeout(Duration::from_secs(3), async {
        while fixture.engine.active_run_count() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("failed subscription left an active Run");

    let replay = fixture
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            invocation(json!("first")),
            "subscribe-failure".to_owned(),
            "request-subscribe-failure-replay".to_owned(),
        )
        .await
        .unwrap();
    let replay = fixture.wait_terminal("default", None, &replay.run_id).await;
    assert!(matches!(replay.lifecycle, RunLifecycle::Completed { .. }));
    let next = fixture
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            invocation(json!("second")),
            "after-subscribe-failure".to_owned(),
            "request-after-subscribe-failure".to_owned(),
        )
        .await
        .unwrap();
    let next = fixture.wait_terminal("default", None, &next.run_id).await;
    assert!(matches!(next.lifecycle, RunLifecycle::Completed { .. }));
    fixture
        .engine
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn identical_private_content_uses_distinct_scoped_objects() {
    let fixture = Fixture::new(PASSTHROUGH_AGENT, false).await;
    let first = fixture
        .engine
        .create_conversation("tenant-a", "user-a", "terminal_fixture", "create-first")
        .await
        .unwrap();
    let second = fixture
        .engine
        .create_conversation("tenant-a", "user-a", "terminal_fixture", "create-second")
        .await
        .unwrap();
    let third = fixture
        .engine
        .create_conversation("tenant-b", "user-b", "terminal_fixture", "create-third")
        .await
        .unwrap();
    let content = json!({"private": "same".repeat(256)});
    let first_turn = fixture
        .engine
        .create_conversation_detached_invocation(
            &first.conversation_id,
            "tenant-a",
            "user-a",
            "turn-first",
            "trace-first",
            invocation(content.clone()),
        )
        .await
        .unwrap();
    let second_turn = fixture
        .engine
        .create_conversation_detached_invocation(
            &second.conversation_id,
            "tenant-a",
            "user-a",
            "turn-second",
            "trace-second",
            invocation(content.clone()),
        )
        .await
        .unwrap();
    let third_turn = fixture
        .engine
        .create_conversation_detached_invocation(
            &third.conversation_id,
            "tenant-b",
            "user-b",
            "turn-third",
            "trace-third",
            invocation(content.clone()),
        )
        .await
        .unwrap();
    fixture
        .wait_terminal("tenant-a", Some("user-a"), &first_turn.run.run_id)
        .await;
    fixture
        .wait_terminal("tenant-a", Some("user-a"), &second_turn.run.run_id)
        .await;
    fixture
        .wait_terminal("tenant-b", Some("user-b"), &third_turn.run.run_id)
        .await;

    let first_page = fixture
        .repository
        .page_conversation_messages(MessagePageQuery {
            conversation: ConversationQuery {
                conversation_id: first.conversation_id.clone(),
                tenant_id: "tenant-a".to_owned(),
                user_id: "user-a".to_owned(),
            },
            before: None,
            limit: 20,
        })
        .await
        .unwrap()
        .unwrap();
    let second_page = fixture
        .repository
        .page_conversation_messages(MessagePageQuery {
            conversation: ConversationQuery {
                conversation_id: second.conversation_id.clone(),
                tenant_id: "tenant-a".to_owned(),
                user_id: "user-a".to_owned(),
            },
            before: None,
            limit: 20,
        })
        .await
        .unwrap()
        .unwrap();
    let third_page = fixture
        .repository
        .page_conversation_messages(MessagePageQuery {
            conversation: ConversationQuery {
                conversation_id: third.conversation_id.clone(),
                tenant_id: "tenant-b".to_owned(),
                user_id: "user-b".to_owned(),
            },
            before: None,
            limit: 20,
        })
        .await
        .unwrap()
        .unwrap();
    let first_refs = first_page
        .messages
        .iter()
        .filter_map(|message| match &message.content {
            ConversationContent::Ref(reference) => Some(reference),
            ConversationContent::Inline(_) => None,
        })
        .collect::<Vec<_>>();
    let second_refs = second_page
        .messages
        .iter()
        .filter_map(|message| match &message.content {
            ConversationContent::Ref(reference) => Some(reference),
            ConversationContent::Inline(_) => None,
        })
        .collect::<Vec<_>>();
    let third_refs = third_page
        .messages
        .iter()
        .filter_map(|message| match &message.content {
            ConversationContent::Ref(reference) => Some(reference),
            ConversationContent::Inline(_) => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(first_refs.len(), 2);
    assert_eq!(second_refs.len(), 2);
    assert_eq!(third_refs.len(), 2);
    assert!(first_refs
        .iter()
        .all(|first_ref| !second_refs.contains(first_ref)));
    assert!(first_refs
        .iter()
        .chain(second_refs.iter())
        .all(|reference| !third_refs.contains(reference)));
    let raw_hash = ContentHash::from_bytes(&serde_jcs::to_vec(&content).unwrap());
    for reference in first_refs
        .iter()
        .chain(second_refs.iter())
        .chain(third_refs.iter())
    {
        let (artifact, _) = decode_artifact_reference(fixture.artifact_store.as_ref(), reference);
        assert_ne!(
            artifact.content_hash(),
            &raw_hash,
            "terminal object collided with an unscoped/full-runtime value"
        );
    }

    fixture
        .engine
        .delete_conversation(&first.conversation_id, "tenant-a", "user-a")
        .await
        .unwrap();
    let surviving = fixture
        .engine
        .list_conversation_messages(&second.conversation_id, "tenant-a", "user-a", None, 20)
        .await
        .unwrap();
    assert_eq!(surviving.messages.len(), 2);
    assert!(surviving
        .messages
        .iter()
        .all(|message| message.content == text_content(&content)));
    fixture
        .engine
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn rolling_summary_is_flat_bounded_and_missing_summary_falls_back_to_recent() {
    let mut config = terminal_config();
    config.summary_trigger_tokens = 100_000;
    let fixture = Fixture::new_with(
        CONTEXT_AGENT,
        false,
        Arc::new(InMemoryLiveRunStreamBroker::new(32, 8).unwrap()),
        config,
    )
    .await;
    let conversation = fixture
        .engine
        .create_conversation(
            "tenant-summary",
            "user-summary",
            "terminal_fixture",
            "create-summary",
        )
        .await
        .unwrap();
    let mut last_context = Value::Null;
    for index in 0..18 {
        let turn = fixture
            .engine
            .create_conversation_detached_invocation(
                &conversation.conversation_id,
                "tenant-summary",
                "user-summary",
                &format!("turn-summary-{index}"),
                &format!("trace-summary-{index}"),
                invocation(json!(format!("message-{index}-{}", "x".repeat(32)))),
            )
            .await
            .unwrap();
        let terminal = fixture
            .wait_terminal("tenant-summary", Some("user-summary"), &turn.run.run_id)
            .await;
        last_context = match terminal.lifecycle {
            RunLifecycle::Completed { output } => output.data,
            lifecycle => panic!("unexpected lifecycle: {lifecycle:?}"),
        };
        let token_estimate = serde_jcs::to_vec(&last_context)
            .unwrap()
            .len()
            .saturating_add(3)
            / 4;
        assert!(
            token_estimate <= config.summary_trigger_tokens,
            "context exceeded hard token budget: {token_estimate}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let query = ConversationQuery {
        conversation_id: conversation.conversation_id.clone(),
        tenant_id: "tenant-summary".to_owned(),
        user_id: "user-summary".to_owned(),
    };
    let _advanced_summary = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(summary) = fixture
                .repository
                .latest_conversation_summary(query.clone())
                .await
                .unwrap()
            {
                if summary.through_message_order >= 8 {
                    break summary;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("rolling summary did not advance through multiple cycles");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let active = fixture
                .engine
                .prometheus_metrics()
                .lines()
                .find(|line| {
                    line.starts_with(
                        "conversation_summary_jobs_active{persistence_mode=\"terminal_only\"} ",
                    )
                })
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(u64::MAX);
            if active == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("rolling summary worker did not quiesce before missing-object injection");
    let summary_text = last_context
        .as_array()
        .and_then(|messages| {
            messages.iter().find_map(|message| {
                message["content"][0]["text"]
                    .as_str()
                    .and_then(|text| text.strip_prefix("Conversation summary: "))
            })
        })
        .expect("context did not contain the rolling summary");
    let summary_value: Value = serde_json::from_str(summary_text).unwrap();
    assert_eq!(
        summary_value.get("kind").and_then(Value::as_str),
        Some("conversation_summary")
    );
    assert!(summary_value.get("previous").is_none());
    assert!(summary_value
        .get("entries")
        .and_then(Value::as_array)
        .is_some_and(|entries| entries.len() <= 64));

    let summary = fixture
        .repository
        .latest_conversation_summary(query.clone())
        .await
        .unwrap()
        .expect("latest rolling summary disappeared");
    let (artifact, locator) =
        decode_artifact_reference(fixture.artifact_store.as_ref(), &summary.summary_ref);
    fixture
        .artifact_store
        .delete(&artifact, &locator)
        .await
        .unwrap();
    let missing_summary_bytes = b"terminal-summary-object-that-was-never-written";
    let missing_summary_artifact = ArtifactRef::new(
        ArtifactId::new("artifact_terminal_missing_summary").unwrap(),
        ContentHash::from_bytes(missing_summary_bytes),
        u64::try_from(missing_summary_bytes.len()).unwrap(),
        Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE.to_owned()),
    )
    .unwrap();
    let missing_summary_ref = serde_json::to_string(&missing_summary_artifact).unwrap();
    let inspection = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(fixture._temporary.path().join("terminal.sqlite"))
                .create_if_missing(false)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    let latest_message_order = sqlx::query_scalar::<_, i64>(
        "SELECT MAX(message_order) FROM conversation_messages WHERE conversation_id=?",
    )
    .bind(&conversation.conversation_id)
    .fetch_one(&inspection)
    .await
    .unwrap();
    assert!(latest_message_order > summary.through_message_order);
    sqlx::query(
        "INSERT INTO conversation_summaries (
             conversation_id,through_message_order,summary_ref,summary_hash,
             model_revision,created_at
         ) VALUES (?,?,?,?,?,?)",
    )
    .bind(&conversation.conversation_id)
    .bind(latest_message_order)
    .bind(missing_summary_ref)
    .bind(missing_summary_artifact.content_hash().as_str())
    .bind("missing-object-test")
    .bind(chrono::Utc::now())
    .execute(&inspection)
    .await
    .unwrap();
    inspection.close().await;
    let mut expected_messages = fixture
        .engine
        .list_conversation_messages(
            &conversation.conversation_id,
            "tenant-summary",
            "user-summary",
            None,
            config.recent_context_messages,
        )
        .await
        .unwrap()
        .messages
        .into_iter()
        .map(|message| {
            json!({
                "role": match message.role {
                    ConversationRole::User => "user",
                    ConversationRole::Assistant => "assistant",
                },
                "content": message.content,
            })
        })
        .collect::<Vec<_>>();
    expected_messages.reverse();
    let fallback = fixture
        .engine
        .create_conversation_detached_invocation(
            &conversation.conversation_id,
            "tenant-summary",
            "user-summary",
            "turn-after-missing-summary",
            "trace-after-missing-summary",
            invocation(json!("still-available")),
        )
        .await
        .unwrap();
    let fallback = fixture
        .wait_terminal("tenant-summary", Some("user-summary"), &fallback.run.run_id)
        .await;
    let fallback_context = match fallback.lifecycle {
        RunLifecycle::Completed { output } => output.data,
        lifecycle => panic!("summary failure blocked turn: {lifecycle:?}"),
    };
    let fallback_messages = fallback_context
        .as_array()
        .expect("missing summary fallback did not include messages")
        .clone();
    assert!(
        !fallback_messages.is_empty()
            && fallback_messages
                == expected_messages[expected_messages.len() - fallback_messages.len()..],
        "missing latest summary must reload the full-history recent page and retain its exact contiguous suffix after token budgeting"
    );
    assert!(fixture
        .engine
        .prometheus_metrics()
        .lines()
        .find(|line| {
            line.starts_with(
                "conversation_summary_jobs_total{result=\"failed\",persistence_mode=\"terminal_only\"} ",
            )
        })
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|value| value > 0));
    fixture
        .engine
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn context_token_budget_selects_a_contiguous_recent_message_suffix() {
    let mut config = terminal_config();
    config.summary_trigger_messages = 100;
    config.recent_context_messages = 8;
    config.summary_trigger_tokens = 256;
    let fixture = Fixture::new_with(
        CONTEXT_AGENT,
        false,
        Arc::new(InMemoryLiveRunStreamBroker::new(32, 8).unwrap()),
        config,
    )
    .await;
    let conversation = fixture
        .engine
        .create_conversation(
            "tenant-context-suffix",
            "user-context-suffix",
            "terminal_fixture",
            "context-suffix-create",
        )
        .await
        .unwrap();
    for (request_id, content) in [
        ("context-suffix-small", json!("small")),
        (
            "context-suffix-oversized",
            json!(format!("oversized:{}", "x".repeat(2_000))),
        ),
    ] {
        let turn = fixture
            .engine
            .create_conversation_detached_invocation(
                &conversation.conversation_id,
                "tenant-context-suffix",
                "user-context-suffix",
                request_id,
                request_id,
                invocation(content),
            )
            .await
            .unwrap();
        fixture
            .wait_terminal(
                "tenant-context-suffix",
                Some("user-context-suffix"),
                &turn.run.run_id,
            )
            .await;
    }
    let mut durable_messages = fixture
        .engine
        .list_conversation_messages(
            &conversation.conversation_id,
            "tenant-context-suffix",
            "user-context-suffix",
            None,
            config.recent_context_messages,
        )
        .await
        .unwrap()
        .messages
        .into_iter()
        .map(|message| {
            json!({
                "role": match message.role {
                    ConversationRole::User => "user",
                    ConversationRole::Assistant => "assistant",
                },
                "content": message.content,
            })
        })
        .collect::<Vec<_>>();
    durable_messages.reverse();
    let probe = fixture
        .engine
        .create_conversation_detached_invocation(
            &conversation.conversation_id,
            "tenant-context-suffix",
            "user-context-suffix",
            "context-suffix-probe",
            "context-suffix-probe-trace",
            invocation(json!("probe")),
        )
        .await
        .unwrap();
    let probe = fixture
        .wait_terminal(
            "tenant-context-suffix",
            Some("user-context-suffix"),
            &probe.run.run_id,
        )
        .await;
    let context = match probe.lifecycle {
        RunLifecycle::Completed { output } => output.data,
        lifecycle => panic!("unexpected lifecycle: {lifecycle:?}"),
    };
    let selected_messages = context.as_array().unwrap().clone();
    assert_eq!(
        selected_messages,
        durable_messages[durable_messages.len() - selected_messages.len()..],
        "context budgeting must stop at the first oversized newer message"
    );
    fixture
        .engine
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn completed_runs_do_not_accumulate_background_join_handles() {
    let fixture = Fixture::new(PASSTHROUGH_AGENT, false).await;
    for index in 0..128 {
        let run = fixture
            .engine
            .create_detached_invocation(
                "default",
                "terminal_fixture",
                invocation(json!(index.to_string())),
                format!("fast-admission-{index}"),
                format!("request-fast-{index}"),
            )
            .await
            .unwrap();
        fixture.wait_terminal("default", None, &run.run_id).await;
    }
    assert_eq!(fixture.engine.active_run_count(), 0);
    assert_eq!(
        fixture.engine.tracked_background_task_count().await,
        4,
        "only heartbeat, retention, deletion, and artifact-staging pumps may be retained"
    );
    fixture
        .engine
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn crash_after_object_put_before_metadata_commit_is_durably_collected() {
    let state = Arc::new(PutCrashState::default());
    state.crash_next_put.store(true, Ordering::Release);
    let wrapper_state = Arc::clone(&state);
    let fixture = Fixture::new_with_store_wrapper(
        PASSTHROUGH_AGENT,
        false,
        Arc::new(InMemoryLiveRunStreamBroker::new(32, 8).unwrap()),
        terminal_config(),
        move |inner| {
            Arc::new(PanicAfterSuccessfulPutStore {
                inner,
                state: wrapper_state,
            })
        },
    )
    .await;
    let admitted = fixture
        .engine
        .create_detached_invocation(
            "default",
            "terminal_fixture",
            invocation(json!("put-then-crash")),
            "put-then-crash".to_owned(),
            "request-put-then-crash".to_owned(),
        )
        .await
        .unwrap();
    let artifact = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(artifact) = state
                .artifact
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
            {
                break artifact;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("put failpoint did not execute");
    assert!(
        fixture
            .repository
            .get_terminal_run(TerminalRunQuery {
                tenant_id: "default".to_owned(),
                run_id: RunId::new(admitted.run_id).unwrap(),
                observed_at: chrono::Utc::now(),
            })
            .await
            .unwrap()
            .unwrap()
            .result
            .is_none(),
        "the failpoint must fire before terminal metadata authority commits"
    );
    assert_eq!(
        fixture
            .engine
            .run_artifact_staging_batch_at(chrono::Utc::now() + chrono::Duration::minutes(5))
            .await
            .unwrap(),
        1
    );
    assert_eq!(state.deleted.load(Ordering::Acquire), 1);
    let locator = fixture.artifact_store.storage_locator(&artifact).unwrap();
    assert!(
        fixture
            .artifact_store
            .read_and_verify(&artifact, &locator, 1024 * 1024)
            .await
            .is_err(),
        "the staged object survived orphan GC"
    );
    assert_eq!(
        fixture
            .engine
            .run_artifact_staging_batch_at(chrono::Utc::now() + chrono::Duration::minutes(10))
            .await
            .unwrap(),
        0,
        "acknowledged orphan staging must not be claimed twice"
    );
    fixture
        .engine
        .shutdown(Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn artifact_staging_drain_catches_up_more_than_one_bounded_batch() {
    let fixture = Fixture::new(PASSTHROUGH_AGENT, false).await;
    let staging_batches = |metrics: &str| {
        metrics
            .lines()
            .find(|line| {
                line.starts_with(
                    "conversation_maintenance_batches_total{queue=\"artifact_staging\",persistence_mode=\"terminal_only\"} ",
                )
            })
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap()
    };
    let created_at = chrono::Utc::now() - chrono::Duration::minutes(5);
    for index in 0..101_u32 {
        let bytes = format!("terminal-maintenance-catch-up-{index}").into_bytes();
        let artifact = ArtifactRef::new(
            ArtifactId::new(format!("artifact_terminal_maintenance_{index}")).unwrap(),
            ContentHash::from_bytes(&bytes),
            u64::try_from(bytes.len()).unwrap(),
            Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE.to_owned()),
        )
        .unwrap();
        fixture
            .artifact_store
            .put_and_verify(&artifact, &bytes)
            .await
            .unwrap();
        fixture
            .repository
            .stage_terminal_artifact(NewTerminalArtifactStage {
                tenant_id: "tenant-maintenance".to_owned(),
                content_ref: serde_json::to_string(&artifact).unwrap(),
                content_hash: artifact.content_hash().clone(),
                source_kind: TerminalArtifactSourceKind::RunOutput,
                source_id: format!("maintenance-source-{index}"),
                available_at: created_at,
                created_at,
            })
            .await
            .unwrap();
    }
    let batches_before = staging_batches(&fixture.engine.prometheus_metrics());

    assert_eq!(
        fixture
            .engine
            .run_artifact_staging_drain_at(chrono::Utc::now())
            .await
            .unwrap(),
        101
    );
    let metrics = fixture.engine.prometheus_metrics();
    assert!(
        staging_batches(&metrics) - batches_before >= 2,
        "101 staging rows must require more than one bounded claim batch"
    );
    assert!(metrics.contains(
        "conversation_maintenance_backlog_pending{queue=\"artifact_staging\",persistence_mode=\"terminal_only\"} 0"
    ));
    let oldest_age = metrics
        .lines()
        .find(|line| {
            line.starts_with(
                "conversation_maintenance_oldest_age_seconds{queue=\"artifact_staging\",persistence_mode=\"terminal_only\"} ",
            )
        })
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap();
    assert!(oldest_age >= 300);
    fixture
        .engine
        .shutdown(Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn privacy_delete_quiesces_an_active_conversation_before_removal() {
    let source = r#"
api_version: insight.agent/v1
kind: agent
inputs:
  query: string
  messages: {type: "Message[]", default: []}
output: string
workflow:
  steps:
    - id: delay
      wait: {duration_ms: 1000}
    - return: $query
"#;
    let fixture = Fixture::new(source, true).await;
    let conversation = fixture
        .engine
        .create_conversation(
            "tenant-delete",
            "user-delete",
            "terminal_fixture",
            "create-delete",
        )
        .await
        .unwrap();
    let _turn = fixture
        .engine
        .create_conversation_detached_invocation(
            &conversation.conversation_id,
            "tenant-delete",
            "user-delete",
            "turn-delete",
            "trace-delete",
            invocation(json!({"private": "delete while active"})),
        )
        .await
        .unwrap();
    assert!(fixture.engine.active_run_count() > 0);
    assert!(fixture
        .engine
        .delete_conversation(
            &conversation.conversation_id,
            "tenant-delete",
            "user-delete",
        )
        .await
        .unwrap());
    assert_eq!(fixture.engine.active_run_count(), 0);
    assert!(fixture.engine.is_ready());
    assert!(fixture
        .engine
        .get_conversation(
            &conversation.conversation_id,
            "tenant-delete",
            "user-delete",
        )
        .await
        .is_err());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(fixture
        .engine
        .get_conversation(
            &conversation.conversation_id,
            "tenant-delete",
            "user-delete",
        )
        .await
        .is_err());
    fixture
        .engine
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn retention_skips_an_active_conversation_until_terminal_commit() {
    let source = r#"
api_version: insight.agent/v1
kind: agent
inputs:
  query: string
  messages: {type: "Message[]", default: []}
output: string
workflow:
  steps:
    - id: delay
      wait: {duration_ms: 1000}
    - return: $query
"#;
    let fixture = Fixture::new(source, true).await;
    let conversation = fixture
        .engine
        .create_conversation(
            "tenant-retention",
            "user-retention",
            "terminal_fixture",
            "create-retention",
        )
        .await
        .unwrap();
    let turn = fixture
        .engine
        .create_conversation_detached_invocation(
            &conversation.conversation_id,
            "tenant-retention",
            "user-retention",
            "turn-retention",
            "trace-retention",
            invocation(json!({"private": "retain while active"})),
        )
        .await
        .unwrap();
    assert!(fixture.engine.active_run_count() > 0);

    let active_retention = fixture
        .repository
        .delete_conversations_before(BoundedRetention {
            before: chrono::Utc::now() + chrono::Duration::hours(1),
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(
        active_retention.deleted, 0,
        "retention must preserve the FK target while a terminal turn is active"
    );

    let terminal = fixture
        .wait_terminal("tenant-retention", Some("user-retention"), &turn.run.run_id)
        .await;
    assert!(
        matches!(terminal.lifecycle, RunLifecycle::Completed { .. }),
        "the terminal commit must succeed after the concurrent retention pass"
    );
    let completed_retention = fixture
        .repository
        .delete_conversations_before(BoundedRetention {
            before: chrono::Utc::now() + chrono::Duration::hours(1),
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(completed_retention.deleted, 1);
    fixture
        .engine
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn terminal_conversation_visibility_guard_fences_delete_and_checks_principal() {
    let fixture = Fixture::new(PASSTHROUGH_AGENT, false).await;
    let conversation = fixture
        .engine
        .create_conversation(
            "tenant-guard",
            "user-guard",
            "terminal_fixture",
            "create-guard",
        )
        .await
        .unwrap();
    let turn = fixture
        .engine
        .create_conversation_detached_invocation(
            &conversation.conversation_id,
            "tenant-guard",
            "user-guard",
            "turn-guard",
            "trace-guard",
            invocation(json!({"private": "terminal visibility"})),
        )
        .await
        .unwrap();
    fixture
        .wait_terminal("tenant-guard", Some("user-guard"), &turn.run.run_id)
        .await;

    assert!(fixture
        .engine
        .acquire_conversation_visibility_guard(
            "tenant-other",
            "user-guard",
            turn.run.run_id.as_str(),
        )
        .await
        .is_err());
    assert!(fixture
        .engine
        .acquire_conversation_visibility_guard(
            "tenant-guard",
            "user-other",
            turn.run.run_id.as_str(),
        )
        .await
        .is_err());

    let guard = fixture
        .engine
        .acquire_conversation_visibility_guard(
            "tenant-guard",
            "user-guard",
            turn.run.run_id.as_str(),
        )
        .await
        .unwrap();
    let engine = fixture.engine.clone();
    let conversation_id = conversation.conversation_id.clone();
    let mut deletion = tokio::spawn(async move {
        engine
            .delete_conversation(&conversation_id, "tenant-guard", "user-guard")
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut deletion)
            .await
            .is_err(),
        "privacy DELETE must wait until terminal-frame publication releases its guard"
    );
    drop(guard);
    assert!(deletion.await.unwrap().unwrap());

    assert!(fixture
        .engine
        .acquire_conversation_visibility_guard(
            "tenant-guard",
            "user-guard",
            turn.run.run_id.as_str(),
        )
        .await
        .is_err());
    fixture
        .engine
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}
