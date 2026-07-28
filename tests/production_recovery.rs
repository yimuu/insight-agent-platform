#[path = "support/database.rs"]
mod database;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use insight_agent_platform::{
    catalog::{
        compile_enabled_agents, deploy_agents, LeafDeploymentResolver, ResolvedLeafDeployment,
    },
    dsl::CompileError,
    engine::{
        plan::LeafTaskDescriptor, repository::PostgresDurableRepository, EffectEvidence,
        LeafTaskExecutor, LeafTaskKind, LocalContentAddressedArtifactStore, RuntimeValue,
        SchedulerTaskKind, TaskExecutionRequest, TaskExecutionResult, VersionTag,
        WorkerExecutionContext, WorkerExecutorRegistry, WorkerFailure,
    },
    history::types::RunStatus,
    runtime::{
        DeployedAgentCatalog, ForkRecoveryOptions, MigrationNodeMappingRequest,
        ProductionRunRepository, RecoveryOperation, RecoveryRequestMetadata, RecoveryReusePolicy,
        RequestMetadata, RunService, RunServiceConfig,
    },
};
use insight_api::v1::{build_router, ApiAuth, ApiState};
use serde_json::{json, Value};
use sqlx::{
    postgres::PgPoolOptions, sqlite::SqliteConnectOptions, AssertSqlSafe, ConnectOptions,
    SqlitePool,
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use uuid::Uuid;

const ASYNC_TRANSITION_TIMEOUT: Duration = Duration::from_secs(10);

struct NoLeafResolver;

async fn shared_artifact_store(
    root: std::path::PathBuf,
    namespace: &str,
) -> Arc<LocalContentAddressedArtifactStore> {
    Arc::new(
        LocalContentAddressedArtifactStore::open_shared(root, 64 * 1024, namespace)
            .await
            .unwrap(),
    )
}

impl LeafDeploymentResolver for NoLeafResolver {
    fn resolve_leaf(
        &self,
        _kind: LeafTaskKind,
        _descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        ResolvedLeafDeployment::new(VersionTag::new("unused-worker").unwrap(), json!({}))
    }
}

struct CountingExecutor {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl LeafTaskExecutor for CountingExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let output = request.outputs().first().expect("fixture output");
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("recovery-answer")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

fn write_worker_agent(root: &std::path::Path, id: &str, implementation: &str, output: &str) {
    let directory = root.join(id);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join("agent.yaml"),
        format!(
            r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: {id}
  name: {id}
  description: Recovery reuse fixture.
inputs:
  question: string
output: {output}
workflow:
  steps:
    - id: answer
      type: action
      call: {implementation}
      inputs:
        question: $question
      response: {output}
    - return: $answer
"#,
        ),
    )
    .unwrap();
}

fn reuse_workers(calls: Arc<AtomicUsize>) -> WorkerExecutorRegistry {
    let mut workers = WorkerExecutorRegistry::new();
    for implementation in ["fixture.reuse", "fixture.reuse.v2", "fixture.mapped"] {
        workers
            .register(
                SchedulerTaskKind::Action,
                implementation,
                VersionTag::new("1").unwrap(),
                VersionTag::new("unused-worker").unwrap(),
                Arc::new(CountingExecutor {
                    calls: Arc::clone(&calls),
                }),
            )
            .unwrap();
    }
    workers
}

fn write_mapped_migration_agent(
    root: &std::path::Path,
    id: &str,
    answer_node: &str,
    review_node: &str,
    signal: &str,
) {
    let directory = root.join(id);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join("agent.yaml"),
        format!(
            r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: {id}
  name: {id}
  description: Explicit mapped migrate fixture.
types:
  Approval:
    fields:
      decision: {{type: string, enum: [approved, rejected]}}
inputs:
  question: string
output: Approval
workflow:
  steps:
    - id: {answer_node}
      type: action
      call: fixture.mapped
      inputs:
        question: $question
      response: string
    - id: {review_node}
      human_task:
        signal: {signal}
        request: Review the mapped migration result
        response: Approval
    - return: ${review_node}
"#,
        ),
    )
    .unwrap();
}

fn mapped_migration_catalog(root: &std::path::Path) -> DeployedAgentCatalog {
    let enabled = BTreeSet::from(["mapped_v1".to_owned(), "mapped_v2".to_owned()]);
    let published = compile_enabled_agents(root, &enabled).unwrap();
    let deployed = deploy_agents(&published, &NoLeafResolver).unwrap();
    DeployedAgentCatalog::new(deployed).unwrap()
}

struct ReuseFixture {
    _temporary: tempfile::TempDir,
    service: RunService,
    control: SqlitePool,
    calls: Arc<AtomicUsize>,
    compatible_revision: String,
    incompatible_revision: String,
}

async fn setup_reuse_sqlite() -> ReuseFixture {
    let temporary = tempfile::tempdir().unwrap();
    let agents_root = temporary.path().join("agents");
    std::fs::create_dir_all(&agents_root).unwrap();
    write_worker_agent(&agents_root, "reuse_v1", "fixture.reuse", "string");
    write_worker_agent(&agents_root, "reuse_v2", "fixture.reuse.v2", "string");
    write_worker_agent(
        &agents_root,
        "reuse_incompatible",
        "fixture.reuse.v2",
        "integer",
    );
    let enabled = BTreeSet::from([
        "reuse_v1".to_owned(),
        "reuse_v2".to_owned(),
        "reuse_incompatible".to_owned(),
    ]);
    let published = compile_enabled_agents(&agents_root, &enabled).unwrap();
    let deployed = deploy_agents(&published, &NoLeafResolver).unwrap();
    let compatible_revision = deployed
        .iter()
        .find(|agent| agent.published().metadata().id == "reuse_v2")
        .unwrap()
        .deployment_revision_id()
        .as_str()
        .to_owned();
    let incompatible_revision = deployed
        .iter()
        .find(|agent| agent.published().metadata().id == "reuse_incompatible")
        .unwrap()
        .deployment_revision_id()
        .as_str()
        .to_owned();
    let agents = DeployedAgentCatalog::new(deployed).unwrap();
    let database = temporary.path().join("reuse.sqlite");
    database::provision_sqlite_database(&database).await;
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let service = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        reuse_workers(Arc::clone(&calls)),
        RunServiceConfig::single_process_development(16, 2, 1, 32),
    )
    .await
    .unwrap();
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(database)
            .create_if_missing(false)
            .disable_statement_logging(),
    )
    .await
    .unwrap();
    ReuseFixture {
        _temporary: temporary,
        service,
        control,
        calls,
        compatible_revision,
        incompatible_revision,
    }
}

fn write_agent(root: &std::path::Path, id: &str, signal: &str) {
    let directory = root.join(id);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join("agent.yaml"),
        format!(
            r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: {id}
  name: {id}
  description: Durable recovery fixture.
types:
  Approval:
    fields:
      decision: {{type: string, enum: [approved, rejected]}}
inputs:
  label: string
output: Approval
workflow:
  steps:
    - id: review
      human_task:
        signal: {signal}
        request: Review this recovery fixture
        response: Approval
    - return: $review
"#,
        ),
    )
    .unwrap();
}

fn recovery_catalog(agents_root: &std::path::Path) -> DeployedAgentCatalog {
    let enabled = BTreeSet::from(["recovery_v1".to_owned(), "recovery_v2".to_owned()]);
    let published = compile_enabled_agents(agents_root, &enabled).unwrap();
    let deployed = deploy_agents(&published, &NoLeafResolver).unwrap();
    DeployedAgentCatalog::new(deployed).unwrap()
}

async fn setup() -> (tempfile::TempDir, RunService) {
    let temporary = tempfile::tempdir().unwrap();
    let agents_root = temporary.path().join("agents");
    std::fs::create_dir_all(&agents_root).unwrap();
    write_agent(&agents_root, "recovery_v1", "approve_v1");
    write_agent(&agents_root, "recovery_v2", "approve_v2");
    let agents = recovery_catalog(&agents_root);
    let database_path = temporary.path().join("recovery.sqlite");
    database::provision_sqlite_database(&database_path).await;
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database_path,
        )
        .await
        .unwrap(),
    );
    let service = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        RunServiceConfig::single_process_development(16, 2, 1, 32),
    )
    .await
    .unwrap();
    (temporary, service)
}

fn recovery_request(
    request_id: &str,
    expected_source_projection_version: u64,
) -> RecoveryRequestMetadata {
    RecoveryRequestMetadata {
        request_id: request_id.to_owned(),
        expected_source_projection_version,
        reuse_policy: RecoveryReusePolicy::Reexecute,
    }
}

fn reuse_request(
    request_id: &str,
    expected_source_projection_version: u64,
) -> RecoveryRequestMetadata {
    RecoveryRequestMetadata {
        request_id: request_id.to_owned(),
        expected_source_projection_version,
        reuse_policy: RecoveryReusePolicy::ReuseCompatible,
    }
}

fn signal_wait_mapping(source: &str, target: &str) -> MigrationNodeMappingRequest {
    MigrationNodeMappingRequest {
        source_node_id: source.to_owned(),
        target_node_id: target.to_owned(),
        ports: None,
        rebuild_signal_wait: true,
        rebuild_timer: false,
    }
}

async fn wait_terminal(
    service: &RunService,
    run_id: &str,
) -> insight_agent_platform::history::types::RunRecord {
    let deadline = tokio::time::Instant::now() + ASYNC_TRANSITION_TIMEOUT;
    loop {
        let current = service.get_run(run_id).await.unwrap();
        if current.status().is_terminal() {
            return current;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Run did not reach terminal within {ASYNC_TRANSITION_TIMEOUT:?}; last status: {:?}",
            current.status()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn waiting_source(
    service: &RunService,
    label: &str,
) -> insight_agent_platform::history::types::RunRecord {
    let created = service
        .create_detached(
            "recovery_v1",
            json!({"label": label}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + ASYNC_TRANSITION_TIMEOUT;
    loop {
        let waiting = service
            .list_human_tasks("recovery-reviewer", Vec::new(), 100)
            .await
            .unwrap()
            .iter()
            .any(|item| item.run_id().as_str() == created.run_id);
        if waiting {
            let current = service.get_run(&created.run_id).await.unwrap();
            assert_eq!(
                current.status(),
                RunStatus::Running,
                "source Run exposed a human task outside its active/waiting lifecycle"
            );
            return current;
        }
        let current = service.get_run(&created.run_id).await.unwrap();
        assert!(
            !current.status().is_terminal(),
            "source Run reached {:?} before exposing its human task",
            current.status()
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "source Run did not reach its durable human wait within \
             {ASYNC_TRANSITION_TIMEOUT:?}; last status: {:?}",
            current.status()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn completed_source(
    service: &RunService,
    label: &str,
) -> insight_agent_platform::history::types::RunRecord {
    let created = waiting_source(service, label).await;
    let human_task_deadline = tokio::time::Instant::now() + ASYNC_TRANSITION_TIMEOUT;
    let item = loop {
        let item = service
            .list_human_tasks("recovery-reviewer", Vec::new(), 100)
            .await
            .unwrap()
            .into_iter()
            .find(|item| item.run_id().as_str() == created.run_id);
        if let Some(item) = item {
            break item;
        }
        assert!(
            tokio::time::Instant::now() < human_task_deadline,
            "source Run did not expose its human task within {ASYNC_TRANSITION_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let claim = service
        .claim_human_task(
            item.work_item_id().as_str(),
            "recovery-reviewer",
            Vec::new(),
            &format!("claim-{label}"),
        )
        .await
        .unwrap();
    service
        .complete_human_task(
            item.work_item_id().as_str(),
            "recovery-reviewer",
            Vec::new(),
            &format!("message-{label}"),
            claim.claim_fence(),
            json!({"decision": "approved"}),
        )
        .await
        .unwrap();
    let signalled = service.get_run(&created.run_id).await.unwrap();
    if signalled.status().is_terminal() {
        return signalled;
    };
    let terminal_deadline = tokio::time::Instant::now() + ASYNC_TRANSITION_TIMEOUT;
    loop {
        let current = service.get_run(&created.run_id).await.unwrap();
        if current.status().is_terminal() {
            return current;
        }
        assert!(
            tokio::time::Instant::now() < terminal_deadline,
            "source Run did not reach terminal within {ASYNC_TRANSITION_TIMEOUT:?}; last status: {:?}",
            current.status()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn sqlite_service_redrive_and_fork_use_closed_server_derived_recovery_evidence() {
    let (_temporary, service) = setup().await;
    let source = completed_source(&service, "terminal-source").await;
    assert_eq!(source.status(), RunStatus::Completed);

    let redrive = service
        .redrive(
            &source.run_id,
            recovery_request("redrive-request-1", source.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(redrive.operation, RecoveryOperation::Redrive);
    assert_eq!(redrive.candidates_created, 0);
    assert_eq!(redrive.target.status(), RunStatus::Running);
    assert_eq!(redrive.target.agent_version, source.agent_version);

    let redrive_replay = service
        .redrive(
            &source.run_id,
            recovery_request("redrive-request-1", source.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(redrive_replay.target.run_id, redrive.target.run_id);

    let fork = service
        .fork(
            &source.run_id,
            recovery_request("fork-request-1", source.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(fork.operation, RecoveryOperation::Fork);
    assert_eq!(fork.candidates_created, 0);
    assert_eq!(fork.target.status(), RunStatus::Running);
    assert_eq!(fork.target.agent_version, source.agent_version);

    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn sqlite_waiting_recovery_target_reaches_durable_deadline_after_service_restart() {
    let temporary = tempfile::tempdir().unwrap();
    let agents_root = temporary.path().join("agents");
    std::fs::create_dir_all(&agents_root).unwrap();
    write_agent(&agents_root, "recovery_v1", "approve_v1");
    write_agent(&agents_root, "recovery_v2", "approve_v2");
    let database = temporary.path().join("recovery-deadline.sqlite");
    let timeout = Duration::from_millis(750);
    database::provision_sqlite_database(&database).await;
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let service = RunService::start(
        recovery_catalog(&agents_root),
        repository as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        RunServiceConfig::single_process_development(16, 2, 1, 32).with_run_timeout(timeout),
    )
    .await
    .unwrap();
    let source = completed_source(&service, "deadline-source").await;
    let redrive = service
        .redrive(
            &source.run_id,
            recovery_request("deadline-redrive", source.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(redrive.target.status(), RunStatus::Running);
    let target_run_id = redrive.target.run_id.clone();
    service.shutdown(Duration::from_secs(1)).await.unwrap();
    drop(service);

    let control = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    let deadline_before_restart: String =
        sqlx::query_scalar("SELECT deadline_at FROM workflow_runs WHERE run_id=?")
            .bind(&target_run_id)
            .fetch_one(&control)
            .await
            .unwrap();
    assert!(!deadline_before_restart.is_empty());

    let restarted_repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let restarted = RunService::start(
        recovery_catalog(&agents_root),
        restarted_repository as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        RunServiceConfig::single_process_development(16, 2, 1, 32).with_run_timeout(timeout),
    )
    .await
    .unwrap();
    let wait_until = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        let current = restarted.get_run(&target_run_id).await.unwrap();
        if current.status() == RunStatus::Failed {
            break;
        }
        assert!(
            tokio::time::Instant::now() < wait_until,
            "restarted recovery target did not reach its durable deadline"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let row = sqlx::query_as::<_, (String, String, i64)>(
        "SELECT r.lifecycle,r.deadline_at,
                julianday(json_extract(e.safe_payload,'$.run_deadline_at')) =
                    julianday(r.deadline_at)
         FROM workflow_runs r JOIN execution_events e
           ON e.run_id=r.run_id AND e.kind='run.created'
         WHERE r.run_id=?",
    )
    .bind(&target_run_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(row.0, "timed_out");
    assert_eq!(row.1, deadline_before_restart);
    assert_eq!(row.2, 1);
    restarted.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn sqlite_public_redrive_and_fork_reuse_override_revision_and_cas_are_closed() {
    let fixture = setup_reuse_sqlite().await;
    let source = fixture
        .service
        .create_detached(
            "reuse_v1",
            json!({"question": "source"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    let source = wait_terminal(&fixture.service, &source.run_id).await;
    assert_eq!(source.status(), RunStatus::Completed);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);

    let redrive = fixture
        .service
        .redrive(
            &source.run_id,
            reuse_request("sqlite-reuse-redrive", source.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(redrive.candidates_created, 1);
    assert_eq!(
        wait_terminal(&fixture.service, &redrive.target.run_id)
            .await
            .status(),
        RunStatus::Completed
    );
    assert_eq!(
        fixture.calls.load(Ordering::SeqCst),
        1,
        "a materialized Redrive candidate cannot call the worker"
    );
    let replay = fixture
        .service
        .redrive(
            &source.run_id,
            reuse_request("sqlite-reuse-redrive", source.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(replay.target.run_id, redrive.target.run_id);

    let checkpoint_id = sqlx::query_scalar::<_, String>(
        "SELECT checkpoint_id FROM scheduler_checkpoints WHERE run_id=?
         ORDER BY scheduler_projection_version DESC,checkpoint_id DESC LIMIT 1",
    )
    .bind(&source.run_id)
    .fetch_one(&fixture.control)
    .await
    .unwrap();
    let fork = fixture
        .service
        .fork_with_options(
            &source.run_id,
            ForkRecoveryOptions {
                target_deployment_revision_id: Some(fixture.compatible_revision.clone()),
                checkpoint_id: Some(checkpoint_id.clone()),
                input_override: Some(json!({"question": "overridden"})),
            },
            reuse_request("sqlite-selected-fork", source.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(fork.candidates_created, 1);
    assert_eq!(fork.target.agent_id, "reuse_v2");
    assert_eq!(
        wait_terminal(&fixture.service, &fork.target.run_id)
            .await
            .status(),
        RunStatus::Completed
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
    let stored_input = sqlx::query_scalar::<_, String>(
        "SELECT p.inline_value FROM workflow_runs r JOIN payloads p
           ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id WHERE r.run_id=?",
    )
    .bind(&fork.target.run_id)
    .fetch_one(&fixture.control)
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&stored_input).unwrap(),
        json!({"question": "overridden"})
    );

    let incompatible = fixture
        .service
        .fork_with_options(
            &source.run_id,
            ForkRecoveryOptions {
                target_deployment_revision_id: Some(fixture.incompatible_revision.clone()),
                checkpoint_id: Some(checkpoint_id.clone()),
                input_override: None,
            },
            reuse_request("sqlite-incompatible-fork", source.projection_version),
        )
        .await
        .unwrap_err();
    assert_eq!(incompatible.code(), "FORK_REVISION_INCOMPATIBLE");

    let conflicting_replay = fixture
        .service
        .fork_with_options(
            &source.run_id,
            ForkRecoveryOptions {
                target_deployment_revision_id: Some(fixture.compatible_revision.clone()),
                checkpoint_id: Some(checkpoint_id),
                input_override: Some(json!({"question": "different-intent"})),
            },
            reuse_request("sqlite-selected-fork", source.projection_version),
        )
        .await
        .unwrap_err();
    assert_eq!(conflicting_replay.code(), "RECOVERY_CONFLICT");

    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_service_continue_and_two_phase_migrate_are_retry_safe() {
    let (_temporary, service) = setup().await;

    let continue_source = waiting_source(&service, "continue-source").await;
    let paused = service.pause(&continue_source.run_id).await.unwrap();
    let continued = service
        .continue_as_new(
            &paused.run_id,
            json!({"label": "next-generation"}),
            recovery_request("continue-request-1", paused.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(continued.operation, RecoveryOperation::ContinueAsNew);
    assert_eq!(continued.source.status(), RunStatus::Cancelled);
    assert_eq!(continued.target.status(), RunStatus::Running);
    assert_eq!(continued.candidates_created, 0);
    let continued_replay = service
        .continue_as_new(
            &paused.run_id,
            json!({"label": "next-generation"}),
            recovery_request("continue-request-1", paused.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(continued_replay.target.run_id, continued.target.run_id);

    let migrate_source = waiting_source(&service, "migrate-source").await;
    let paused = service.pause(&migrate_source.run_id).await.unwrap();
    let migrated = service
        .migrate(
            &paused.run_id,
            "recovery_v2",
            json!({"label": "replacement"}),
            vec![signal_wait_mapping("review", "review")],
            recovery_request("migrate-request-1", paused.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(migrated.operation, RecoveryOperation::Migrate);
    assert_eq!(migrated.source.status(), RunStatus::Cancelled);
    assert_eq!(migrated.target.status(), RunStatus::Running);
    assert_eq!(migrated.target.agent_id, "recovery_v2");
    assert_eq!(migrated.candidates_created, 0);
    let migrated_replay = service
        .migrate(
            &paused.run_id,
            "recovery_v2",
            json!({"label": "replacement"}),
            vec![signal_wait_mapping("review", "review")],
            recovery_request("migrate-request-1", paused.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(migrated_replay.target.run_id, migrated.target.run_id);

    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn sqlite_service_migrate_replays_frozen_pending_intent_after_alias_advance() {
    let temporary = tempfile::tempdir().unwrap();
    let agents_root = temporary.path().join("agents");
    std::fs::create_dir_all(&agents_root).unwrap();
    write_agent(&agents_root, "recovery_v1", "approve_v1");
    write_agent(&agents_root, "recovery_v2", "approve_v2_old");
    let old_agents = recovery_catalog(&agents_root);
    let old_target_revision = old_agents
        .get("recovery_v2")
        .unwrap()
        .deployment_revision_id()
        .as_str()
        .to_owned();
    let database = temporary.path().join("migration-crash-replay.sqlite");
    database::provision_sqlite_database(&database).await;
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let service = RunService::start(
        old_agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        RunServiceConfig::single_process_development(16, 2, 1, 32),
    )
    .await
    .unwrap();
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .create_if_missing(false)
            .disable_statement_logging(),
    )
    .await
    .unwrap();

    let source = waiting_source(&service, "sqlite-migration-crash").await;
    let paused = service.pause(&source.run_id).await.unwrap();
    sqlx::query(
        "CREATE TRIGGER simulate_migration_crash_after_begin
         BEFORE INSERT ON workflow_runs
         WHEN NEW.lineage_kind='migrate'
         BEGIN
           SELECT RAISE(ABORT, 'simulated crash after migration begin');
         END",
    )
    .execute(&control)
    .await
    .unwrap();
    let crashed = service
        .migrate(
            &paused.run_id,
            "recovery_v2",
            json!({"label": "sqlite-replacement"}),
            vec![signal_wait_mapping("review", "review")],
            recovery_request("sqlite-migration-crash-replay", paused.projection_version),
        )
        .await
        .unwrap_err();
    assert_eq!(crashed.code(), "RUN_SERVICE_UNAVAILABLE");
    let pending: String =
        sqlx::query_scalar("SELECT intent_state FROM run_migration_intents WHERE run_id=?")
            .bind(&paused.run_id)
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(pending, "pending");

    service.shutdown(Duration::from_secs(1)).await.unwrap();
    drop(service);
    drop(repository);
    sqlx::query("DROP TRIGGER simulate_migration_crash_after_begin")
        .execute(&control)
        .await
        .unwrap();

    // The same public alias now resolves to a different immutable deployment.
    write_agent(&agents_root, "recovery_v2", "approve_v2_new");
    let new_agents = recovery_catalog(&agents_root);
    let new_target_revision = new_agents
        .get("recovery_v2")
        .unwrap()
        .deployment_revision_id()
        .as_str()
        .to_owned();
    assert_ne!(new_target_revision, old_target_revision);
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let service = RunService::start(
        new_agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        RunServiceConfig::single_process_development(16, 2, 1, 32),
    )
    .await
    .unwrap();

    let changed_input = service
        .migrate(
            &paused.run_id,
            "recovery_v2",
            json!({"label": "changed-after-begin"}),
            vec![signal_wait_mapping("review", "review")],
            recovery_request("sqlite-migration-crash-replay", paused.projection_version),
        )
        .await
        .unwrap_err();
    assert_eq!(changed_input.code(), "RECOVERY_CONFLICT");
    let changed_mapping = service
        .migrate(
            &paused.run_id,
            "recovery_v2",
            json!({"label": "sqlite-replacement"}),
            Vec::new(),
            recovery_request("sqlite-migration-crash-replay", paused.projection_version),
        )
        .await
        .unwrap_err();
    assert_eq!(changed_mapping.code(), "RECOVERY_CONFLICT");

    let migrated = service
        .migrate(
            &paused.run_id,
            "recovery_v2",
            json!({"label": "sqlite-replacement"}),
            vec![signal_wait_mapping("review", "review")],
            recovery_request("sqlite-migration-crash-replay", paused.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(migrated.target.agent_version, old_target_revision);
    assert_ne!(migrated.target.agent_version, new_target_revision);
    assert_eq!(migrated.source.status(), RunStatus::Cancelled);

    service.shutdown(Duration::from_secs(1)).await.unwrap();
    control.close().await;
}

async fn response_json(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap()).unwrap()
}

#[tokio::test]
async fn sqlite_service_and_api_map_unsafe_redrive_to_requires_fork_without_side_effects() {
    let fixture = setup_reuse_sqlite().await;
    let source = fixture
        .service
        .create_detached(
            "reuse_v1",
            json!({"question": "unsafe redrive source"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    let source = wait_terminal(&fixture.service, &source.run_id).await;
    assert_eq!(source.status(), RunStatus::Completed);

    let persisted_effect = sqlx::query_as::<_, (String, String)>(
        "SELECT effect_idempotency,effect_evidence FROM node_activations
         WHERE run_id=? AND execution_kind='worker'",
    )
    .bind(&source.run_id)
    .fetch_one(&fixture.control)
    .await
    .unwrap();
    assert_eq!(
        persisted_effect,
        ("non_idempotent".into(), "committed".into())
    );
    sqlx::query(
        "UPDATE node_activations SET effect_evidence='started'
         WHERE run_id=? AND execution_kind='worker'",
    )
    .bind(&source.run_id)
    .execute(&fixture.control)
    .await
    .unwrap();

    let before = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT (SELECT COUNT(*) FROM workflow_runs),
                (SELECT COUNT(*) FROM node_attempts),
                (SELECT COUNT(*) FROM recovery_effect_roots)",
    )
    .fetch_one(&fixture.control)
    .await
    .unwrap();

    let service_error = fixture
        .service
        .redrive(
            &source.run_id,
            recovery_request("unsafe-redrive-service", source.projection_version),
        )
        .await
        .unwrap_err();
    assert_eq!(service_error.code(), "REDRIVE_REQUIRES_FORK");

    let app = build_router(ApiState {
        service: fixture.service.clone(),
        auth: ApiAuth::disabled(),
        sse_keep_alive_interval: Duration::from_secs(1),
        readiness_probe_timeout: Duration::from_secs(1),
    });
    let response = app
        .oneshot(
            Request::post(format!("/v1/runs/{}/redrive", source.run_id))
                .header("content-type", "application/json")
                .header("x-request-id", "unsafe-redrive-api")
                .body(Body::from(
                    json!({
                        "expected_projection_version": source.projection_version,
                        "reuse_policy": "reexecute"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = response_json(response).await;
    assert_eq!(body["code"], "REDRIVE_REQUIRES_FORK");
    assert_eq!(body["message"], "redrive requires a fork");
    let encoded = body.to_string();
    for private_detail in ["started", "unknown", "non_idempotent", "effect_id"] {
        assert!(!encoded.contains(private_detail));
    }

    let after = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT (SELECT COUNT(*) FROM workflow_runs),
                (SELECT COUNT(*) FROM node_attempts),
                (SELECT COUNT(*) FROM recovery_effect_roots)",
    )
    .fetch_one(&fixture.control)
    .await
    .unwrap();
    assert_eq!(
        after, before,
        "blocked Redrive must not create durable target state"
    );

    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
    fixture.control.close().await;
}

async fn wait_for_human_task_on_run(service: &RunService, run_id: &str) {
    let deadline = tokio::time::Instant::now() + ASYNC_TRANSITION_TIMEOUT;
    loop {
        if service
            .list_human_tasks("mapped-reviewer", Vec::new(), 100)
            .await
            .unwrap()
            .iter()
            .any(|item| item.run_id().as_str() == run_id)
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "mapped migration Run did not reach its durable wait"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn post_mapped_migrate(
    app: &Router,
    source_run_id: &str,
    expected_projection_version: u64,
    request_id: &str,
    mappings: Value,
) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::post(format!("/v1/runs/{source_run_id}/migrate"))
                .header("content-type", "application/json")
                .header("x-request-id", request_id)
                .body(Body::from(
                    json!({
                        "expected_projection_version": expected_projection_version,
                        "reuse_policy": "reuse_compatible",
                        "target_agent_id": "mapped_v2",
                        "input": {"question": "mapped source"},
                        "mappings": mappings,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
}

fn mapped_leaf_mapping() -> Value {
    json!({
        "source_node_id": "answer_v1",
        "target_node_id": "answer_v2",
        "rebuild_signal_wait": false,
        "rebuild_timer": false
    })
}

fn mapped_wait_mapping() -> Value {
    json!({
        "source_node_id": "review_v1",
        "target_node_id": "review_v2",
        "rebuild_signal_wait": true,
        "rebuild_timer": false
    })
}

#[tokio::test]
async fn formal_recovery_routes_require_stable_identity_and_reject_low_level_proofs() {
    let (_temporary, service) = setup().await;
    let source = completed_source(&service, "router-source").await;
    let app = build_router(ApiState {
        service: service.clone(),
        auth: ApiAuth::disabled(),
        sse_keep_alive_interval: Duration::from_secs(1),
        readiness_probe_timeout: Duration::from_secs(1),
    });
    let uri = format!("/v1/runs/{}/redrive", source.run_id);
    let body = json!({
        "expected_projection_version": source.projection_version,
        "reuse_policy": "reexecute"
    });

    let missing_identity = app
        .clone()
        .oneshot(
            Request::post(&uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing_identity.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_json(missing_identity).await["code"],
        "RECOVERY_REQUEST_INVALID"
    );

    let forged = app
        .clone()
        .oneshot(
            Request::post(&uri)
                .header("content-type", "application/json")
                .header("x-request-id", "router-redrive-1")
                .body(Body::from(
                    json!({
                        "expected_projection_version": source.projection_version,
                        "reuse_policy": "reexecute",
                        "reuse_candidates": [{"forged": true}],
                        "checkpoint_hash": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forged.status(), StatusCode::BAD_REQUEST);

    let first = app
        .clone()
        .oneshot(
            Request::post(&uri)
                .header("content-type", "application/json")
                .header("x-request-id", "router-redrive-1")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first = response_json(first).await;
    assert_eq!(first["data"]["reuse_policy"], "reexecute");
    assert_eq!(first["data"]["candidates_created"], 0);
    let target_run_id = first["data"]["target"]["run_id"].as_str().unwrap();

    let replay = app
        .clone()
        .oneshot(
            Request::post(&uri)
                .header("content-type", "application/json")
                .header("x-request-id", "router-redrive-1")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(
        response_json(replay).await["data"]["target"]["run_id"],
        target_run_id
    );
    let reused_identity_with_different_intent = app
        .clone()
        .oneshot(
            Request::post(&uri)
                .header("content-type", "application/json")
                .header("x-request-id", "router-redrive-1")
                .body(Body::from(
                    json!({
                        "expected_projection_version": source.projection_version + 1,
                        "reuse_policy": "reexecute"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        reused_identity_with_different_intent.status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        response_json(reused_identity_with_different_intent).await["code"],
        "RECOVERY_CONFLICT"
    );

    let fork = app
        .clone()
        .oneshot(
            Request::post(format!("/v1/runs/{}/fork", source.run_id))
                .header("content-type", "application/json")
                .header("x-request-id", "router-fork-1")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(fork.status(), StatusCode::OK);
    assert_eq!(response_json(fork).await["data"]["operation"], "fork");

    let selected_fork = app
        .clone()
        .oneshot(
            Request::post(format!("/v1/runs/{}/fork", source.run_id))
                .header("content-type", "application/json")
                .header("x-request-id", "router-fork-selected")
                .body(Body::from(
                    json!({
                        "expected_projection_version": source.projection_version,
                        "reuse_policy": "reexecute",
                        "target_deployment_revision_id": source.agent_version,
                        "input": {"label": "router-fork-override"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(selected_fork.status(), StatusCode::OK);
    let selected_fork = response_json(selected_fork).await;
    assert_eq!(selected_fork["data"]["operation"], "fork");
    assert_eq!(selected_fork["data"]["target"]["agent_id"], "recovery_v1");

    let continue_source = waiting_source(&service, "router-continue").await;
    let paused = service.pause(&continue_source.run_id).await.unwrap();
    let continued = app
        .clone()
        .oneshot(
            Request::post(format!("/v1/runs/{}/continue-as-new", paused.run_id))
                .header("content-type", "application/json")
                .header("x-request-id", "router-continue-1")
                .body(Body::from(
                    json!({
                        "expected_projection_version": paused.projection_version,
                        "reuse_policy": "reexecute",
                        "input": {"label": "router-next-generation"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(continued.status(), StatusCode::OK);
    assert_eq!(
        response_json(continued).await["data"]["operation"],
        "continue_as_new"
    );

    let migrate_source = waiting_source(&service, "router-migrate").await;
    let paused = service.pause(&migrate_source.run_id).await.unwrap();
    let migrated = app
        .oneshot(
            Request::post(format!("/v1/runs/{}/migrate", paused.run_id))
                .header("content-type", "application/json")
                .header("x-request-id", "router-migrate-1")
                .body(Body::from(
                    json!({
                        "expected_projection_version": paused.projection_version,
                        "reuse_policy": "reexecute",
                        "target_agent_id": "recovery_v2",
                        "input": {"label": "router-replacement"},
                        "mappings": [{
                            "source_node_id": "review",
                            "target_node_id": "review",
                            "rebuild_signal_wait": true,
                            "rebuild_timer": false
                        }]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(migrated.status(), StatusCode::OK);
    let migrated = response_json(migrated).await;
    assert_eq!(migrated["data"]["operation"], "migrate");
    assert_eq!(migrated["data"]["target"]["agent_id"], "recovery_v2");

    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn sqlite_formal_migrate_derives_cross_revision_mapping_and_reuses_renamed_leaf() {
    let temporary = tempfile::tempdir().unwrap();
    let agents_root = temporary.path().join("agents");
    std::fs::create_dir_all(&agents_root).unwrap();
    write_mapped_migration_agent(
        &agents_root,
        "mapped_v1",
        "answer_v1",
        "review_v1",
        "approve_v1",
    );
    write_mapped_migration_agent(
        &agents_root,
        "mapped_v2",
        "answer_v2",
        "review_v2",
        "approve_v2",
    );
    let database = temporary.path().join("mapped-migrate.sqlite");
    database::provision_sqlite_database(&database).await;
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let service = RunService::start(
        mapped_migration_catalog(&agents_root),
        repository as Arc<dyn ProductionRunRepository>,
        reuse_workers(Arc::clone(&calls)),
        RunServiceConfig::single_process_development(16, 2, 1, 32),
    )
    .await
    .unwrap();
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .create_if_missing(false)
            .disable_statement_logging(),
    )
    .await
    .unwrap();
    let app = build_router(ApiState {
        service: service.clone(),
        auth: ApiAuth::disabled(),
        sse_keep_alive_interval: Duration::from_secs(1),
        readiness_probe_timeout: Duration::from_secs(1),
    });

    let source = service
        .create_detached(
            "mapped_v1",
            json!({"question": "mapped source"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_human_task_on_run(&service, &source.run_id).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let paused = service.pause(&source.run_id).await.unwrap();

    let missing = post_mapped_migrate(
        &app,
        &paused.run_id,
        paused.projection_version,
        "mapped-missing",
        json!([]),
    )
    .await;
    assert_eq!(missing.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response_json(missing).await["code"],
        "MIGRATION_MAPPING_INCOMPATIBLE"
    );

    let incompatible = post_mapped_migrate(
        &app,
        &paused.run_id,
        paused.projection_version,
        "mapped-incompatible",
        json!([{
            "source_node_id": "answer_v1",
            "target_node_id": "answer_v2",
            "ports": {"question": "question"},
            "rebuild_signal_wait": false,
            "rebuild_timer": false
        }, mapped_wait_mapping()]),
    )
    .await;
    assert_eq!(incompatible.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let uncovered_wait = post_mapped_migrate(
        &app,
        &paused.run_id,
        paused.projection_version,
        "mapped-uncovered-wait",
        json!([mapped_leaf_mapping()]),
    )
    .await;
    assert_eq!(uncovered_wait.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let pre_handoff = sqlx::query_as::<_, (Option<String>, i64)>(
        "SELECT termination_intent_reason,
                (SELECT COUNT(*) FROM run_migration_intents WHERE run_id=r.run_id)
         FROM workflow_runs r WHERE run_id=?",
    )
    .bind(&paused.run_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(pre_handoff, (None, 0));

    let migrated = post_mapped_migrate(
        &app,
        &paused.run_id,
        paused.projection_version,
        "mapped-success",
        json!([mapped_leaf_mapping(), mapped_wait_mapping()]),
    )
    .await;
    assert_eq!(migrated.status(), StatusCode::OK);
    let migrated = response_json(migrated).await;
    assert_eq!(migrated["data"]["candidates_created"], 1);
    let target_run_id = migrated["data"]["target"]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    wait_for_human_task_on_run(&service, &target_run_id).await;
    let candidate_decision = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT candidate_state,rejection_reason FROM run_reuse_candidates
         WHERE run_id=? AND target_node_id='answer_v2'",
    )
    .bind(&target_run_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(candidate_decision, ("materialized".to_owned(), None));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_activations
             WHERE run_id=? AND node_id='answer_v2' AND lifecycle='succeeded'
               AND reused_from_run_id=?",
        )
        .bind(&target_run_id)
        .bind(&paused.run_id)
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM run_reuse_candidates
             WHERE run_id=? AND target_node_id='answer_v2' AND candidate_state='materialized'",
        )
        .bind(&target_run_id)
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );

    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn postgres_formal_migrate_derives_cross_revision_mapping_and_reuses_renamed_leaf() {
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        return;
    };
    let temporary = tempfile::tempdir().unwrap();
    let agents_root = temporary.path().join("agents");
    std::fs::create_dir_all(&agents_root).unwrap();
    write_mapped_migration_agent(
        &agents_root,
        "mapped_v1",
        "answer_v1",
        "review_v1",
        "approve_v1",
    );
    write_mapped_migration_agent(
        &agents_root,
        "mapped_v2",
        "answer_v2",
        "review_v2",
        "approve_v2",
    );

    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    let schema = format!("production_mapped_migrate_{}", Uuid::new_v4().simple());
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    database::provision_postgres_url(&scoped_url).await;
    let repository = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let artifact_store = shared_artifact_store(
        temporary.path().join("artifacts"),
        "mapped-migration-recovery",
    )
    .await;
    let service = RunService::start_with_artifact_store(
        mapped_migration_catalog(&agents_root),
        repository.clone() as Arc<dyn ProductionRunRepository>,
        reuse_workers(Arc::clone(&calls)),
        artifact_store,
        RunServiceConfig::production(16, 2, 1, 32),
    )
    .await
    .unwrap();
    let app = build_router(ApiState {
        service: service.clone(),
        auth: ApiAuth::disabled(),
        sse_keep_alive_interval: Duration::from_secs(1),
        readiness_probe_timeout: Duration::from_secs(1),
    });

    let source = service
        .create_detached(
            "mapped_v1",
            json!({"question": "mapped source"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_human_task_on_run(&service, &source.run_id).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let paused = service.pause(&source.run_id).await.unwrap();

    let missing = post_mapped_migrate(
        &app,
        &paused.run_id,
        paused.projection_version,
        "pg-mapped-missing",
        json!([]),
    )
    .await;
    assert_eq!(missing.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response_json(missing).await["code"],
        "MIGRATION_MAPPING_INCOMPATIBLE"
    );

    let incompatible = post_mapped_migrate(
        &app,
        &paused.run_id,
        paused.projection_version,
        "pg-mapped-incompatible",
        json!([{
            "source_node_id": "answer_v1",
            "target_node_id": "answer_v2",
            "ports": {"question": "question"},
            "rebuild_signal_wait": false,
            "rebuild_timer": false
        }, mapped_wait_mapping()]),
    )
    .await;
    assert_eq!(incompatible.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let uncovered_wait = post_mapped_migrate(
        &app,
        &paused.run_id,
        paused.projection_version,
        "pg-mapped-uncovered-wait",
        json!([mapped_leaf_mapping()]),
    )
    .await;
    assert_eq!(uncovered_wait.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let pre_handoff = sqlx::query_as::<_, (Option<String>, i64)>(
        "SELECT termination_intent_reason,
                (SELECT COUNT(*) FROM run_migration_intents WHERE run_id=r.run_id)
         FROM workflow_runs r WHERE run_id=$1",
    )
    .bind(&paused.run_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(pre_handoff, (None, 0));

    let migrated = post_mapped_migrate(
        &app,
        &paused.run_id,
        paused.projection_version,
        "pg-mapped-success",
        json!([mapped_leaf_mapping(), mapped_wait_mapping()]),
    )
    .await;
    assert_eq!(migrated.status(), StatusCode::OK);
    let migrated = response_json(migrated).await;
    assert_eq!(migrated["data"]["candidates_created"], 1);
    let target_run_id = migrated["data"]["target"]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    wait_for_human_task_on_run(&service, &target_run_id).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_activations
             WHERE run_id=$1 AND node_id='answer_v2' AND lifecycle='succeeded'
               AND reused_from_run_id=$2",
        )
        .bind(&target_run_id)
        .bind(&paused.run_id)
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM run_reuse_candidates
             WHERE run_id=$1 AND target_node_id='answer_v2'
               AND candidate_state='materialized'",
        )
        .bind(&target_run_id)
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );

    service.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(app);
    drop(service);
    drop(repository);
    control.close().await;
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn postgres_service_redrive_migrate_and_continue_preserve_durable_lineage() {
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        return;
    };
    let temporary = tempfile::tempdir().unwrap();
    let agents_root = temporary.path().join("agents");
    std::fs::create_dir_all(&agents_root).unwrap();
    write_agent(&agents_root, "recovery_v1", "approve_v1");
    write_agent(&agents_root, "recovery_v2", "approve_v2");
    let enabled = BTreeSet::from(["recovery_v1".to_owned(), "recovery_v2".to_owned()]);
    let published = compile_enabled_agents(&agents_root, &enabled).unwrap();
    let deployed = deploy_agents(&published, &NoLeafResolver).unwrap();
    let agents = DeployedAgentCatalog::new(deployed).unwrap();

    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    let schema = format!("production_recovery_{}", Uuid::new_v4().simple());
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    database::provision_postgres_url(&scoped_url).await;
    let repository = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    let artifact_store = shared_artifact_store(
        temporary.path().join("artifacts"),
        "lineage-production-recovery",
    )
    .await;
    let service = RunService::start_with_artifact_store(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        artifact_store,
        RunServiceConfig::production(16, 2, 1, 32),
    )
    .await
    .unwrap();

    let source = completed_source(&service, "pg-redrive").await;
    let redrive = service
        .redrive(
            &source.run_id,
            recovery_request("pg-redrive-request", source.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(redrive.target.agent_version, source.agent_version);
    assert_eq!(redrive.candidates_created, 0);

    let source = waiting_source(&service, "pg-continue").await;
    let paused = service.pause(&source.run_id).await.unwrap();
    let continued = service
        .continue_as_new(
            &paused.run_id,
            json!({"label": "pg-next-generation"}),
            recovery_request("pg-continue-request", paused.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(continued.source.status(), RunStatus::Cancelled);
    assert_eq!(continued.target.agent_version, paused.agent_version);

    let source = waiting_source(&service, "pg-migrate").await;
    let paused = service.pause(&source.run_id).await.unwrap();
    let migrated = service
        .migrate(
            &paused.run_id,
            "recovery_v2",
            json!({"label": "pg-replacement"}),
            vec![signal_wait_mapping("review", "review")],
            recovery_request("pg-migrate-request", paused.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(migrated.source.status(), RunStatus::Cancelled);
    assert_eq!(migrated.target.agent_id, "recovery_v2");
    assert_ne!(migrated.target.agent_version, paused.agent_version);

    service.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(service);
    drop(repository);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn postgres_stale_runtime_migrate_uses_the_durable_target_head() {
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        return;
    };
    let temporary = tempfile::tempdir().unwrap();
    let agents_root = temporary.path().join("agents");
    std::fs::create_dir_all(&agents_root).unwrap();
    write_agent(&agents_root, "recovery_v1", "approve_v1");
    write_agent(&agents_root, "recovery_v2", "approve_v2_old");
    let stale_agents = recovery_catalog(&agents_root);
    let old_target_revision = stale_agents
        .get("recovery_v2")
        .unwrap()
        .deployment_revision_id()
        .as_str()
        .to_owned();

    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    let schema = format!(
        "production_migration_durable_head_{}",
        Uuid::new_v4().simple()
    );
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(2)
        .connect(&scoped_url)
        .await
        .unwrap();
    database::provision_postgres_schema(&control).await;
    let stale_repository = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    let artifact_store = shared_artifact_store(
        temporary.path().join("artifacts"),
        "durable-head-migration-recovery",
    )
    .await;
    let stale_service = RunService::start_with_artifact_store(
        stale_agents,
        stale_repository.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        artifact_store.clone(),
        RunServiceConfig::production(16, 2, 1, 32),
    )
    .await
    .unwrap();

    // A second runtime publishes a new deployment after the stale runtime has
    // already loaded its process-local catalog.
    write_agent(&agents_root, "recovery_v2", "approve_v2_new");
    let fresh_agents = recovery_catalog(&agents_root);
    let new_target_revision = fresh_agents
        .get("recovery_v2")
        .unwrap()
        .deployment_revision_id()
        .as_str()
        .to_owned();
    assert_ne!(new_target_revision, old_target_revision);
    let fresh_repository = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    let fresh_service = RunService::start_with_artifact_store(
        fresh_agents,
        fresh_repository.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        artifact_store,
        RunServiceConfig::production(16, 2, 1, 32),
    )
    .await
    .unwrap();

    let source = waiting_source(&stale_service, "postgres-stale-migration-head").await;
    let paused = stale_service.pause(&source.run_id).await.unwrap();
    let migrated = stale_service
        .migrate(
            &paused.run_id,
            "recovery_v2",
            json!({"label": "resolved-from-durable-head"}),
            vec![signal_wait_mapping("review", "review")],
            recovery_request("postgres-stale-migration-head", paused.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(migrated.target.agent_version, new_target_revision);
    assert_ne!(migrated.target.agent_version, old_target_revision);
    let frozen_target: String = sqlx::query_scalar(
        "SELECT target_deployment_revision_id FROM run_migration_intents WHERE run_id=$1",
    )
    .bind(&paused.run_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(frozen_target, new_target_revision);

    stale_service
        .shutdown(Duration::from_secs(2))
        .await
        .unwrap();
    fresh_service
        .shutdown(Duration::from_secs(2))
        .await
        .unwrap();
    drop(stale_service);
    drop(fresh_service);
    drop(stale_repository);
    drop(fresh_repository);
    control.close().await;
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn postgres_service_migrate_replays_frozen_pending_intent_after_alias_advance() {
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        return;
    };
    let temporary = tempfile::tempdir().unwrap();
    let agents_root = temporary.path().join("agents");
    std::fs::create_dir_all(&agents_root).unwrap();
    write_agent(&agents_root, "recovery_v1", "approve_v1");
    write_agent(&agents_root, "recovery_v2", "approve_v2_old");
    let old_agents = recovery_catalog(&agents_root);
    let old_target_revision = old_agents
        .get("recovery_v2")
        .unwrap()
        .deployment_revision_id()
        .as_str()
        .to_owned();

    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    let schema = format!(
        "production_migration_crash_replay_{}",
        Uuid::new_v4().simple()
    );
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(2)
        .connect(&scoped_url)
        .await
        .unwrap();
    database::provision_postgres_schema(&control).await;
    let repository = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    let artifact_store = shared_artifact_store(
        temporary.path().join("artifacts"),
        "frozen-intent-migration-recovery",
    )
    .await;
    let service = RunService::start_with_artifact_store(
        old_agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        artifact_store.clone(),
        RunServiceConfig::production(16, 2, 1, 32),
    )
    .await
    .unwrap();

    let source = waiting_source(&service, "postgres-migration-crash").await;
    let paused = service.pause(&source.run_id).await.unwrap();
    sqlx::query(
        "ALTER TABLE workflow_runs
         ADD CONSTRAINT simulate_migration_crash_after_begin
         CHECK (lineage_kind IS DISTINCT FROM 'migrate')",
    )
    .execute(&control)
    .await
    .unwrap();
    let crashed = service
        .migrate(
            &paused.run_id,
            "recovery_v2",
            json!({"label": "postgres-replacement"}),
            vec![signal_wait_mapping("review", "review")],
            recovery_request("postgres-migration-crash-replay", paused.projection_version),
        )
        .await
        .unwrap_err();
    assert_eq!(crashed.code(), "RUN_SERVICE_UNAVAILABLE");
    let pending: String =
        sqlx::query_scalar("SELECT intent_state FROM run_migration_intents WHERE run_id=$1")
            .bind(&paused.run_id)
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(pending, "pending");

    service.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(service);
    drop(repository);
    sqlx::query(
        "ALTER TABLE workflow_runs
         DROP CONSTRAINT simulate_migration_crash_after_begin",
    )
    .execute(&control)
    .await
    .unwrap();

    write_agent(&agents_root, "recovery_v2", "approve_v2_new");
    let new_agents = recovery_catalog(&agents_root);
    let new_target_revision = new_agents
        .get("recovery_v2")
        .unwrap()
        .deployment_revision_id()
        .as_str()
        .to_owned();
    assert_ne!(new_target_revision, old_target_revision);
    let repository = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    let service = RunService::start_with_artifact_store(
        new_agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        artifact_store,
        RunServiceConfig::production(16, 2, 1, 32),
    )
    .await
    .unwrap();

    let changed_input = service
        .migrate(
            &paused.run_id,
            "recovery_v2",
            json!({"label": "changed-after-begin"}),
            vec![signal_wait_mapping("review", "review")],
            recovery_request("postgres-migration-crash-replay", paused.projection_version),
        )
        .await
        .unwrap_err();
    assert_eq!(changed_input.code(), "RECOVERY_CONFLICT");
    let changed_mapping = service
        .migrate(
            &paused.run_id,
            "recovery_v2",
            json!({"label": "postgres-replacement"}),
            Vec::new(),
            recovery_request("postgres-migration-crash-replay", paused.projection_version),
        )
        .await
        .unwrap_err();
    assert_eq!(changed_mapping.code(), "RECOVERY_CONFLICT");

    let migrated = service
        .migrate(
            &paused.run_id,
            "recovery_v2",
            json!({"label": "postgres-replacement"}),
            vec![signal_wait_mapping("review", "review")],
            recovery_request("postgres-migration-crash-replay", paused.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(migrated.target.agent_version, old_target_revision);
    assert_ne!(migrated.target.agent_version, new_target_revision);
    assert_eq!(migrated.source.status(), RunStatus::Cancelled);

    service.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(service);
    drop(repository);
    control.close().await;
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn postgres_public_redrive_and_selected_fork_reuse_override_and_cas_are_durable() {
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        return;
    };
    let temporary = tempfile::tempdir().unwrap();
    let agents_root = temporary.path().join("agents");
    std::fs::create_dir_all(&agents_root).unwrap();
    write_worker_agent(&agents_root, "reuse_v1", "fixture.reuse", "string");
    let enabled = BTreeSet::from(["reuse_v1".to_owned()]);
    let published = compile_enabled_agents(&agents_root, &enabled).unwrap();
    let deployed = deploy_agents(&published, &NoLeafResolver).unwrap();
    let agents = DeployedAgentCatalog::new(deployed).unwrap();

    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    let schema = format!("production_reuse_{}", Uuid::new_v4().simple());
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    database::provision_postgres_url(&scoped_url).await;
    let repository = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let artifact_store =
        shared_artifact_store(temporary.path().join("artifacts"), "reuse-recovery").await;
    let service = RunService::start_with_artifact_store(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        reuse_workers(Arc::clone(&calls)),
        artifact_store,
        RunServiceConfig::production(16, 2, 1, 32),
    )
    .await
    .unwrap();

    let source = service
        .create_detached(
            "reuse_v1",
            json!({"question": "postgres-source"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    let source = wait_terminal(&service, &source.run_id).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let redrive = service
        .redrive(
            &source.run_id,
            reuse_request("pg-closed-redrive", source.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(redrive.candidates_created, 1);
    assert_eq!(
        wait_terminal(&service, &redrive.target.run_id)
            .await
            .status(),
        RunStatus::Completed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let checkpoint_id = sqlx::query_scalar::<_, String>(
        "SELECT checkpoint_id FROM scheduler_checkpoints WHERE run_id=$1
         ORDER BY scheduler_projection_version DESC,checkpoint_id DESC LIMIT 1",
    )
    .bind(&source.run_id)
    .fetch_one(&control)
    .await
    .unwrap();
    let options = ForkRecoveryOptions {
        target_deployment_revision_id: None,
        checkpoint_id: Some(checkpoint_id.clone()),
        input_override: Some(json!({"question": "postgres-override"})),
    };
    let fork = service
        .fork_with_options(
            &source.run_id,
            options.clone(),
            reuse_request("pg-selected-fork", source.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(fork.candidates_created, 1);
    assert_eq!(
        wait_terminal(&service, &fork.target.run_id).await.status(),
        RunStatus::Completed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let replay = service
        .fork_with_options(
            &source.run_id,
            options,
            reuse_request("pg-selected-fork", source.projection_version),
        )
        .await
        .unwrap();
    assert_eq!(replay.target.run_id, fork.target.run_id);
    let stale = service
        .redrive(
            &source.run_id,
            reuse_request(
                "pg-stale-redrive",
                source.projection_version.saturating_add(1),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(stale.code(), "RECOVERY_CONFLICT");
    let conflict = service
        .fork_with_options(
            &source.run_id,
            ForkRecoveryOptions {
                target_deployment_revision_id: None,
                checkpoint_id: Some(checkpoint_id),
                input_override: Some(json!({"question": "postgres-different"})),
            },
            reuse_request("pg-selected-fork", source.projection_version),
        )
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), "RECOVERY_CONFLICT");

    service.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(service);
    drop(repository);
    control.close().await;
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
