use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use insight_agent_platform::{
    catalog_v3::{
        compile_v3_agent_dir, DeployedV3Agent, LeafDeploymentResolver, ResolvedLeafDeployment,
    },
    dsl::CompileError,
    engine::{
        plan::LeafTaskDescriptor,
        repository::{DurableRepository, SchedulerDurableRepository},
        scheduler::TaskOutcomeFact,
        EffectEvidence, LeafTaskExecutor, LeafTaskKind, LocalContentAddressedArtifactStore,
        RuntimeValue, SchedulerTaskKind, SubflowContractRegistry, TaskExecutionRequest,
        TaskExecutionResult, VersionTag, WorkerArtifactStore, WorkerExecutionContext,
        WorkerExecutorRegistry, WorkerFailure, WorkerFailureClass,
    },
    history::types::{RunAttachment, RunStatus},
    runtime::{
        DeployedAgentCatalog, ProductionRunRepository, RequestMetadata, RunService,
        RunServiceConfig,
    },
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

const WORKER_VERSION: &str = "fixture-worker-1";

struct FixtureResolver;

impl LeafDeploymentResolver for FixtureResolver {
    fn resolve_leaf(
        &self,
        kind: LeafTaskKind,
        descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        ResolvedLeafDeployment::new(
            VersionTag::new(WORKER_VERSION).unwrap(),
            json!({
                "kind": kind.name(),
                "implementation": descriptor.implementation,
            }),
        )
    }
}

struct FixtureExecutor;

#[async_trait]
impl LeafTaskExecutor for FixtureExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        let output = request.outputs().first().expect("fixture output contract");
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("answered")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

struct PanicsOnceExecutor {
    calls: AtomicUsize,
}

#[async_trait]
impl LeafTaskExecutor for PanicsOnceExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("executor panic payload must never escape the worker boundary");
        }
        let output = request.outputs().first().expect("fixture output contract");
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("recovered worker pump")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

fn deployed_catalog() -> (DeployedAgentCatalog, String) {
    let directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/v3/runtime_agent");
    let published = Arc::new(compile_v3_agent_dir(&directory).unwrap());
    let deployed = Arc::new(
        DeployedV3Agent::publish(published, &FixtureResolver, SubflowContractRegistry::new())
            .unwrap(),
    );
    let revision = deployed.deployment_revision_id().as_str().to_owned();
    (DeployedAgentCatalog::new(vec![deployed]).unwrap(), revision)
}

fn workers() -> WorkerExecutorRegistry {
    let mut workers = WorkerExecutorRegistry::new();
    workers
        .register(
            SchedulerTaskKind::Action,
            "fixture.answer",
            VersionTag::new("1").unwrap(),
            VersionTag::new(WORKER_VERSION).unwrap(),
            Arc::new(FixtureExecutor),
        )
        .unwrap();
    workers
}

fn panics_once_workers() -> WorkerExecutorRegistry {
    let mut workers = WorkerExecutorRegistry::new();
    workers
        .register(
            SchedulerTaskKind::Action,
            "fixture.answer",
            VersionTag::new("1").unwrap(),
            VersionTag::new(WORKER_VERSION).unwrap(),
            Arc::new(PanicsOnceExecutor {
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
    workers
}

fn config(pump_interval: Duration) -> RunServiceConfig {
    let mut config = RunServiceConfig::single_process_development(16, 4, 2, 64);
    config.scheduler_lease_seconds = 30;
    config.task_claim_seconds = 30;
    config.scheduler_action_budget = 128;
    config.pump_interval = pump_interval;
    config.run_timeout = Duration::from_secs(60);
    config.artifact_orphan_retention = Duration::from_secs(60);
    config.artifact_reference_retention = Duration::from_secs(60);
    config.artifact_gc_interval = Duration::from_secs(60);
    config.artifact_deletion_claim_seconds = 30;
    config.public_event_nonterminal_retention = Duration::from_secs(60);
    config.public_event_prune_interval = Duration::from_secs(60);
    config
}

fn production_config(pump_interval: Duration) -> RunServiceConfig {
    let mut config = RunServiceConfig::production(16, 4, 2, 64);
    config.scheduler_lease_seconds = 30;
    config.task_claim_seconds = 30;
    config.scheduler_action_budget = 128;
    config.pump_interval = pump_interval;
    config.run_timeout = Duration::from_secs(60);
    config.artifact_orphan_retention = Duration::from_secs(60);
    config.artifact_reference_retention = Duration::from_secs(60);
    config.artifact_gc_interval = Duration::from_secs(60);
    config.artifact_deletion_claim_seconds = 30;
    config.public_event_nonterminal_retention = Duration::from_secs(60);
    config.public_event_prune_interval = Duration::from_secs(60);
    config
}

async fn wait_for_terminal(service: &RunService, run_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let record = service.get_run(run_id).await.unwrap();
        if record.status().is_terminal() {
            assert_eq!(record.status(), RunStatus::Completed);
            return;
        }
        assert!(Instant::now() < deadline, "Run did not become terminal");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn production_sqlite_is_rejected_at_the_library_boundary_before_catalog_writes() {
    let (agents, _) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::in_memory()
            .await
            .unwrap(),
    );

    let error = RunService::start(
        agents.clone(),
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        RunServiceConfig::production(16, 4, 2, 64),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), "PLATFORM_PRODUCTION_REQUIRES_POSTGRES");
    let stored = repository.load_versioned_plan_catalog().await.unwrap();
    assert!(stored.plans().is_empty());
    assert!(stored.heads().is_empty());

    let service = RunService::start(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap();
    let stored = repository.load_versioned_plan_catalog().await.unwrap();
    assert_eq!(stored.plans().len(), 1);
    assert_eq!(stored.heads().len(), 1);
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn production_artifact_store_gate_precedes_publication_and_binds_shared_identity() {
    let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
        eprintln!("skipping real PostgreSQL production Artifact-store gate test");
        return;
    };
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let schema = format!("v3_artifact_authority_{}", &suffix[..16]);
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let repository = Arc::new(
        insight_agent_platform::engine::repository::PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    repository.initialize_schema().await.unwrap();

    let missing = RunService::start(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        production_config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap_err();
    assert_eq!(
        missing.code(),
        "PLATFORM_PRODUCTION_REQUIRES_ARTIFACT_STORE"
    );
    let stored = repository.load_versioned_plan_catalog().await.unwrap();
    assert!(stored.plans().is_empty());
    assert!(stored.heads().is_empty());

    let artifact_directory = tempfile::tempdir().unwrap();
    let local = Arc::new(
        LocalContentAddressedArtifactStore::open(artifact_directory.path().join("local"), 1)
            .await
            .unwrap(),
    );
    let local_error = RunService::start_with_artifact_store(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        local,
        production_config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap_err();
    assert_eq!(
        local_error.code(),
        "PLATFORM_PRODUCTION_REQUIRES_SHARED_ARTIFACT_STORE"
    );
    let stored = repository.load_versioned_plan_catalog().await.unwrap();
    assert!(stored.plans().is_empty());
    assert!(stored.heads().is_empty());

    let shared_root = artifact_directory.path().join("shared");
    let shared = Arc::new(
        LocalContentAddressedArtifactStore::open_shared(shared_root.clone(), 1, "production")
            .await
            .unwrap(),
    );
    let first = RunService::start_with_artifact_store(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        shared.clone(),
        production_config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap();
    let same_root = Arc::new(
        LocalContentAddressedArtifactStore::open_shared(
            shared_root.join("..").join("shared"),
            1,
            "production",
        )
        .await
        .unwrap(),
    );
    let second = RunService::start_with_artifact_store(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        same_root,
        production_config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap();
    let before_conflict = repository.load_versioned_plan_catalog().await.unwrap();

    let different_root = Arc::new(
        LocalContentAddressedArtifactStore::open_shared(
            artifact_directory.path().join("different"),
            1,
            "production",
        )
        .await
        .unwrap(),
    );
    assert_ne!(
        shared.deployment_contract().store_id(),
        different_root.deployment_contract().store_id()
    );
    let conflict = RunService::start_with_artifact_store(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        different_root,
        production_config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap_err();
    assert_eq!(
        conflict.code(),
        "PLATFORM_ARTIFACT_STORE_AUTHORITY_CONFLICT"
    );
    let after_conflict = repository.load_versioned_plan_catalog().await.unwrap();
    assert_eq!(after_conflict.plans().len(), before_conflict.plans().len());
    assert_eq!(after_conflict.heads().len(), before_conflict.heads().len());

    second.shutdown(Duration::from_secs(1)).await.unwrap();
    first.shutdown(Duration::from_secs(1)).await.unwrap();
    drop(repository);
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn executor_panic_is_durable_and_does_not_kill_the_only_worker_pump() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory
        .path()
        .join("runtime-terminal-task-failure.sqlite");
    let (agents, _) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let mut service_config = config(Duration::from_millis(5));
    service_config.max_concurrent_operations = 1;
    service_config.max_concurrent_operations_per_run = 1;
    let service = RunService::start(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        panics_once_workers(),
        service_config,
    )
    .await
    .unwrap();

    let mut first = service
        .create_attached(
            "runtime_fixture",
            json!({"question": "panic once"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let record = service.get_run(&first.run_id).await.unwrap();
        if record.status().is_terminal() {
            assert_eq!(record.status(), RunStatus::Failed);
            let insight_agent_platform::history::types::RunLifecycle::Failed { error } =
                record.lifecycle
            else {
                unreachable!("terminal status was checked above")
            };
            assert_eq!(error.code, "SCHEDULER_INTERNAL_FAILURE");
            break;
        }
        assert!(Instant::now() < deadline, "panicked task was stranded");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let run_id = insight_agent_platform::engine::RunId::new(first.run_id.clone()).unwrap();
    let facts = repository.load_scheduler_facts(&run_id).await.unwrap();
    let failure = facts
        .task_outcomes()
        .values()
        .find_map(|outcome| match outcome {
            TaskOutcomeFact::Failed { failure } => Some(failure),
            TaskOutcomeFact::Succeeded { .. } => None,
        })
        .expect("panic must commit one durable task failure");
    assert_eq!(failure.code(), "WORKER_EXECUTOR_PANICKED");
    assert_eq!(failure.class(), WorkerFailureClass::EffectOutcomeUnknown);

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    let public_failures = sqlx::query_as::<_, (String, String, i64, i64)>(
        "SELECT outbox.event_kind,outbox.causation_event_id,event.seq,outbox.public_ordinal
         FROM public_event_outbox outbox
         JOIN execution_events event
           ON event.run_id=outbox.run_id AND event.event_id=outbox.causation_event_id
         WHERE outbox.run_id=? AND outbox.event_kind IN ('operation.failed','run.failed')
         ORDER BY event.seq,outbox.public_ordinal",
    )
    .bind(&first.run_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(public_failures.len(), 2, "{public_failures:?}");
    assert_eq!(public_failures[0].0, "operation.failed");
    assert_eq!(public_failures[1].0, "run.failed");
    assert_ne!(public_failures[0].1, public_failures[1].1);
    assert!(public_failures[0].2 < public_failures[1].2);
    assert_eq!((public_failures[0].3, public_failures[1].3), (40, 50));

    use insight_agent_platform::events::protocol::RunEventType;
    let mut public_sequence = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), first.subscription.recv())
            .await
            .unwrap()
            .unwrap();
        let terminal = event.event_type == RunEventType::RunFailed;
        public_sequence.push(event.event_type);
        if terminal {
            break;
        }
    }
    assert_eq!(
        public_sequence,
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::OperationStarted,
            RunEventType::OperationFailed,
            RunEventType::RunFailed,
        ]
    );
    assert_eq!(
        first.subscription.recv().await.unwrap_err().code(),
        "SUBSCRIPTION_TERMINAL"
    );

    // With one configured worker, success here proves the same supervised
    // pump survived the panic and retained runtime capacity.
    let second = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "worker still alive"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_terminal(&service, &second.run_id).await;
    pool.close().await;
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn sqlite_restart_resumes_nonterminal_run_and_preserves_public_identity() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime.sqlite");
    let (agents, revision) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let first = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        workers(),
        config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap();
    let created = first
        .create_detached(
            "runtime_fixture",
            json!({"question": "resume me"}),
            RequestMetadata {
                request_id: Some("request-restart-1".to_owned()),
            },
        )
        .await
        .unwrap();
    assert_eq!(created.status(), RunStatus::Running);
    assert_eq!(created.request_id, "request-restart-1");
    assert_eq!(created.attachment, RunAttachment::Detached);
    assert_eq!(created.agent_version, revision);
    let run_id = created.run_id.clone();
    first.shutdown(Duration::from_secs(1)).await.unwrap();
    drop(first);

    let (agents, _) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let second = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        workers(),
        config(Duration::from_millis(5)),
    )
    .await
    .unwrap();
    wait_for_terminal(&second, &run_id).await;
    let completed = second.get_run(&run_id).await.unwrap();
    assert_eq!(completed.request_id, "request-restart-1");
    assert_eq!(completed.attachment, RunAttachment::Detached);
    assert_eq!(completed.agent_version, revision);
    let execution_graph = second.execution_graph(&run_id).await.unwrap();
    assert!(!execution_graph.nodes().is_empty());
    let trace = second.trace_overlay(&run_id).await.unwrap();
    assert_eq!(trace.graph_document_id(), execution_graph.document_id());
    assert!(!trace.activations().is_empty());
    second.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn attached_subscription_delivers_one_terminal_event_then_reaches_eof() {
    let (agents, revision) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::in_memory()
            .await
            .unwrap(),
    );
    let service = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        workers(),
        config(Duration::from_millis(5)),
    )
    .await
    .unwrap();
    let mut attached = service
        .create_attached(
            "runtime_fixture",
            json!({"question": "public-event-secret-marker"}),
            RequestMetadata {
                request_id: Some("request-stream-1".to_owned()),
            },
        )
        .await
        .unwrap();
    let mut events = Vec::new();
    for _ in 0..16 {
        let event = tokio::time::timeout(Duration::from_secs(2), attached.subscription.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.request_id, "request-stream-1");
        assert_eq!(event.agent_version, revision);
        let encoded = serde_json::to_string(&event).unwrap();
        assert!(!encoded.contains("public-event-secret-marker"));
        assert!(!encoded.contains("answered"));
        let terminal = matches!(
            event.event_type,
            insight_agent_platform::events::protocol::RunEventType::RunCompleted
                | insight_agent_platform::events::protocol::RunEventType::RunFailed
                | insight_agent_platform::events::protocol::RunEventType::RunCancelled
                | insight_agent_platform::events::protocol::RunEventType::RunInterrupted
        );
        events.push(event);
        if terminal {
            break;
        }
    }
    use insight_agent_platform::events::protocol::RunEventType;
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::OperationStarted,
            RunEventType::OperationCompleted,
            RunEventType::RunCompleted,
        ]
    );
    assert_eq!(events.last().unwrap().code, "OK");
    assert_eq!(
        attached.subscription.recv().await.unwrap_err().code(),
        "SUBSCRIPTION_TERMINAL"
    );
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn postgres_attached_public_lifecycle_is_ordered_private_and_replay_idempotent() {
    let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
        return;
    };
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let schema = format!("v3_public_lifecycle_{}", &suffix[..16]);
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    let version: String = sqlx::query_scalar("SHOW server_version_num")
        .fetch_one(&admin)
        .await
        .unwrap();
    assert!((160_000..170_000).contains(&version.parse::<u32>().unwrap()));
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let repository = Arc::new(
        insight_agent_platform::engine::repository::PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    repository.initialize_schema().await.unwrap();
    let control = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&scoped_url)
        .await
        .unwrap();

    let (agents, revision) = deployed_catalog();
    let artifact_directory = tempfile::tempdir().unwrap();
    let artifact_store = Arc::new(
        LocalContentAddressedArtifactStore::open_shared(
            artifact_directory.path().join("objects"),
            1,
            "public_lifecycle",
        )
        .await
        .unwrap(),
    );
    let service = RunService::start_with_artifact_store(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        artifact_store,
        production_config(Duration::from_millis(5)),
    )
    .await
    .unwrap();
    let mut attached = service
        .create_attached(
            "runtime_fixture",
            json!({"question": "postgres-public-secret-marker"}),
            RequestMetadata {
                request_id: Some("request-postgres-public-lifecycle".to_owned()),
            },
        )
        .await
        .unwrap();
    use insight_agent_platform::events::protocol::RunEventType;
    let mut live_kinds = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), attached.subscription.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.agent_version, revision);
        let encoded = serde_json::to_string(&event).unwrap();
        assert!(!encoded.contains("postgres-public-secret-marker"));
        assert!(!encoded.contains("answered"));
        let terminal = event.event_type == RunEventType::RunCompleted;
        live_kinds.push(event.event_type);
        if terminal {
            break;
        }
    }
    assert_eq!(
        live_kinds,
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::OperationStarted,
            RunEventType::OperationCompleted,
            RunEventType::RunCompleted,
        ]
    );
    assert_eq!(
        attached.subscription.recv().await.unwrap_err().code(),
        "SUBSCRIPTION_TERMINAL"
    );

    let rows = sqlx::query_as::<_, (String, String, String, i64, i32)>(
        "SELECT outbox.event_kind,outbox.public_event_id,outbox.safe_envelope::text,
                event.seq,outbox.public_ordinal
         FROM public_event_outbox outbox
         JOIN execution_events event
           ON event.run_id=outbox.run_id AND event.event_id=outbox.causation_event_id
         WHERE outbox.run_id=$1
         ORDER BY event.seq,outbox.public_ordinal",
    )
    .bind(&attached.run_id)
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(
        rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
        vec![
            "run.created",
            "run.started",
            "operation.started",
            "operation.completed",
            "run.completed",
        ]
    );
    assert_eq!(
        rows.iter()
            .map(|row| row.1.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        rows.len()
    );
    assert!(rows.iter().all(|row| {
        !row.2.contains("postgres-public-secret-marker") && !row.2.contains("answered")
    }));
    let authoritative_ids = rows.iter().map(|row| row.1.clone()).collect::<Vec<_>>();

    service.reconcile_startup().await.unwrap();
    let replay_ids = sqlx::query_scalar::<_, String>(
        "SELECT public_event_id FROM public_event_outbox WHERE run_id=$1
         ORDER BY public_event_id",
    )
    .bind(&attached.run_id)
    .fetch_all(&control)
    .await
    .unwrap();
    let mut expected_ids = authoritative_ids;
    expected_ids.sort();
    assert_eq!(replay_ids, expected_ids);

    service.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(service);
    drop(repository);
    control.close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn cancel_is_durable_and_idempotently_visible_through_get() {
    let (agents, revision) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::in_memory()
            .await
            .unwrap(),
    );
    let service = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        workers(),
        config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap();
    let created = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "cancel me"}),
            RequestMetadata {
                request_id: Some("request-cancel-1".to_owned()),
            },
        )
        .await
        .unwrap();
    assert_eq!(created.status(), RunStatus::Running);
    let cancelled = service.cancel(&created.run_id).await.unwrap();
    assert_eq!(cancelled.status(), RunStatus::Cancelled);
    assert_eq!(cancelled.request_id, "request-cancel-1");
    assert_eq!(cancelled.agent_version, revision);
    assert_eq!(
        service.get_run(&created.run_id).await.unwrap().status(),
        RunStatus::Cancelled
    );
    assert_eq!(
        service.cancel(&created.run_id).await.unwrap().status(),
        RunStatus::Cancelled
    );
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn max_concurrent_runs_bounds_nonterminal_runs_and_reopens_after_drain() {
    let (agents, _) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::in_memory()
            .await
            .unwrap(),
    );
    let mut service_config = config(Duration::from_secs(3_600));
    service_config.max_concurrent_runs = 1;
    let service = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        workers(),
        service_config,
    )
    .await
    .unwrap();

    let first = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "hold the only slot"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), RunStatus::Running);

    let capacity = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "must wait"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(capacity.code(), "RUN_CAPACITY_EXCEEDED");

    assert_eq!(
        service.cancel(&first.run_id).await.unwrap().status(),
        RunStatus::Cancelled
    );
    let admitted = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "slot reopened"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    assert_eq!(admitted.status(), RunStatus::Running);

    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn production_public_event_pruner_keeps_terminal_and_expires_nonterminal_rows() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime-public-retention.sqlite");
    let (agents, _) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let mut service_config = config(Duration::from_millis(5));
    service_config.public_event_nonterminal_retention = Duration::from_secs(1);
    service_config.public_event_prune_interval = Duration::from_millis(5);
    let service = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        workers(),
        service_config,
    )
    .await
    .unwrap();
    let created = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "exercise public retention"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_terminal(&service, &created.run_id).await;

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    let publication_deadline = Instant::now() + Duration::from_secs(5);
    let initial_nonterminal = loop {
        let (terminal, published_nonterminal, total_nonterminal, missing_deadline) =
            sqlx::query_as::<_, (i64, i64, i64, i64)>(
                "SELECT
                    SUM(CASE WHEN is_terminal=1 AND publish_state='published'
                        AND retain_until IS NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN is_terminal=0 AND publish_state='published'
                        THEN 1 ELSE 0 END),
                    SUM(CASE WHEN is_terminal=0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN is_terminal=0 AND publish_state='published'
                        AND retain_until IS NULL THEN 1 ELSE 0 END)
                 FROM public_event_outbox WHERE run_id=?",
            )
            .bind(&created.run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        if terminal == 1 && published_nonterminal > 0 && published_nonterminal == total_nonterminal
        {
            assert_eq!(missing_deadline, 0);
            break total_nonterminal;
        }
        assert!(
            Instant::now() < publication_deadline,
            "public outbox did not reach its published retention boundary"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(initial_nonterminal > 0);

    let prune_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (terminal, nonterminal) = sqlx::query_as::<_, (i64, i64)>(
            "SELECT
                SUM(CASE WHEN is_terminal=1 AND publish_state='published' THEN 1 ELSE 0 END),
                SUM(CASE WHEN is_terminal=0 THEN 1 ELSE 0 END)
             FROM public_event_outbox WHERE run_id=?",
        )
        .bind(&created.run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(terminal, 1, "terminal public authority must remain durable");
        if nonterminal == 0 {
            break;
        }
        assert!(
            Instant::now() < prune_deadline,
            "expired nonterminal public events were not pruned"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    pool.close().await;
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn production_worker_externalizes_large_output_and_commits_reference() {
    use insight_agent_platform::engine::{
        repository::{ArtifactDurableRepository, ReleaseRunArtifactRetentionCommand},
        RunId, TransitionKey, TransitionOutcome,
    };

    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime-artifact.sqlite");
    let (agents, _) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let object_root = directory.path().join("objects");
    let store = Arc::new(
        LocalContentAddressedArtifactStore::open(object_root.clone(), 1)
            .await
            .unwrap(),
    );
    let mut service_config = config(Duration::from_millis(5));
    service_config.artifact_gc_interval = Duration::from_millis(5);
    service_config.artifact_reference_retention = Duration::from_secs(60);
    let service = RunService::start_with_artifact_store(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        store.clone(),
        service_config,
    )
    .await
    .unwrap();
    let created = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "store the answer"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_terminal(&service, &created.run_id).await;

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let content_hash = loop {
        let row = sqlx::query_as::<_, (String, String, i64)>(
            "SELECT a.artifact_state,a.content_hash,
                    (SELECT COUNT(*) FROM artifact_retention_releases rr WHERE rr.run_id=a.run_id)
             FROM artifacts a WHERE a.run_id=?",
        )
        .bind(&created.run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        if row.0 == "referenced" && row.2 == 1 {
            break row.1;
        }
        assert!(
            Instant::now() < deadline,
            "terminal Artifact retention was not registered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let hash = content_hash.strip_prefix("sha256:").unwrap();
    let object_path = object_root.join(&hash[..2]).join(hash);
    assert!(object_path.exists());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT artifact_state FROM artifacts WHERE run_id=?")
            .bind(&created.run_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "referenced",
        "a caller clock must not bypass the database retention deadline"
    );

    let run_id = RunId::new(created.run_id.clone()).unwrap();
    let release_key = TransitionKey::derive(
        "production.v3.run-service",
        &["artifact.retention.release", run_id.as_str()],
    )
    .unwrap();
    assert!(matches!(
        repository
            .release_run_artifact_retention(
                release_key.clone(),
                ReleaseRunArtifactRetentionCommand::new(run_id.clone(), 60).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
    assert_eq!(
        repository
            .release_run_artifact_retention(
                release_key,
                ReleaseRunArtifactRetentionCommand::new(run_id, 61).unwrap(),
            )
            .await
            .unwrap_err()
            .code(),
        "ENGINE_REPOSITORY_INTENT_CONFLICT"
    );
    assert!(
        sqlx::query_scalar::<_, i64>(
            "SELECT
                (SELECT COUNT(*) FROM scheduler_values
                 WHERE run_id=? AND storage_kind='artifact') +
                (SELECT COUNT(*) FROM scheduler_occurrence_values
                 WHERE run_id=? AND storage_kind='artifact')",
        )
        .bind(&created.run_id)
        .bind(&created.run_id)
        .fetch_one(&pool)
        .await
        .unwrap()
            > 0
    );

    sqlx::query(
        "UPDATE artifact_retention_releases
         SET retain_until=STRFTIME('%Y-%m-%dT%H:%M:%fZ','now','-1 second')
         WHERE run_id=?",
    )
    .bind(&created.run_id)
    .execute(&pool)
    .await
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let state =
            sqlx::query_scalar::<_, String>("SELECT artifact_state FROM artifacts WHERE run_id=?")
                .bind(&created.run_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        if state == "deleted" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "referenced Artifact was not collected after its durable deadline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!object_path.exists());
    pool.close().await;
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn production_artifact_gc_deletes_verified_unreferenced_object() {
    use insight_agent_platform::engine::repository::{
        ArtifactDurableRepository, StageArtifactCommand, VerifyArtifactCommand,
    };

    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime-artifact-gc.sqlite");
    let (agents, _) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let object_root = directory.path().join("objects");
    let store = Arc::new(
        LocalContentAddressedArtifactStore::open(object_root.clone(), 1)
            .await
            .unwrap(),
    );
    let mut service_config = config(Duration::from_millis(5));
    service_config.artifact_orphan_retention = Duration::from_secs(1);
    service_config.artifact_gc_interval = Duration::from_millis(5);
    let service = RunService::start_with_artifact_store(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        store.clone(),
        service_config,
    )
    .await
    .unwrap();
    let run = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "create a GC authority Run"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_terminal(&service, &run.run_id).await;
    let run_id = insight_agent_platform::engine::RunId::new(run.run_id.clone()).unwrap();

    let bytes = b"verified-but-never-referenced";
    let artifact = store
        .artifact_for_bytes(bytes, Some("application/octet-stream".to_owned()))
        .unwrap();
    let locator = store.storage_locator(&artifact).unwrap();
    let hash_hex = artifact
        .content_hash()
        .as_str()
        .strip_prefix("sha256:")
        .unwrap();
    let object_path = object_root.join(&hash_hex[..2]).join(hash_hex);
    repository
        .stage_artifact(StageArtifactCommand::new(
            run_id.clone(),
            artifact.clone(),
            locator,
            None,
        ))
        .await
        .unwrap();
    let (hash, size) = store.put_and_verify(&artifact, bytes).await.unwrap();
    repository
        .verify_artifact(VerifyArtifactCommand::new(
            run_id,
            artifact.artifact_id().clone(),
            hash,
            size,
        ))
        .await
        .unwrap();

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let state = sqlx::query_scalar::<_, String>(
            "SELECT artifact_state FROM artifacts WHERE run_id=? AND artifact_id=?",
        )
        .bind(&run.run_id)
        .bind(artifact.artifact_id().as_str())
        .fetch_one(&pool)
        .await
        .unwrap();
        if state == "deleted" {
            break;
        }
        assert!(Instant::now() < deadline, "artifact GC did not settle");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!object_path.exists());
    pool.close().await;
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn postgres_background_pumps_externalize_prune_and_gc_across_shared_store_restart() {
    use insight_agent_platform::engine::repository::{
        ArtifactDurableRepository, StageArtifactCommand, VerifyArtifactCommand,
    };

    let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
        eprintln!("skipping real PostgreSQL Artifact/public-event background-pump test");
        return;
    };
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let schema = format!("v3_background_artifact_{}", &suffix[..16]);
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let repository = Arc::new(
        insight_agent_platform::engine::repository::PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    repository.initialize_schema().await.unwrap();
    let control = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();

    let artifact_directory = tempfile::tempdir().unwrap();
    let object_root = artifact_directory.path().join("objects");
    let store = Arc::new(
        LocalContentAddressedArtifactStore::open_shared(
            object_root.clone(),
            1,
            "pg_background_pumps",
        )
        .await
        .unwrap(),
    );
    let mut first_config = production_config(Duration::from_millis(5));
    first_config.artifact_gc_interval = Duration::from_millis(5);
    first_config.public_event_nonterminal_retention = Duration::from_secs(1);
    first_config.public_event_prune_interval = Duration::from_secs(3_600);
    let first = RunService::start_with_artifact_store(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        store.clone(),
        first_config,
    )
    .await
    .unwrap();
    let created = first
        .create_detached(
            "runtime_fixture",
            json!({"question": "postgres background Artifact evidence"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_terminal(&first, &created.run_id).await;

    let deadline = Instant::now() + Duration::from_secs(10);
    let referenced_hash = loop {
        let artifact = sqlx::query_as::<_, (String, String, i64)>(
            "SELECT a.artifact_state,a.content_hash,
                    (SELECT COUNT(*) FROM artifact_retention_releases rr
                     WHERE rr.run_id=a.run_id)
             FROM artifacts a WHERE a.run_id=$1",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap();
        let public = sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT
                COUNT(*) FILTER (WHERE is_terminal AND publish_state='published'),
                COUNT(*) FILTER (WHERE NOT is_terminal),
                COUNT(*) FILTER (WHERE NOT is_terminal AND publish_state='published')
             FROM public_event_outbox WHERE run_id=$1",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap();
        if artifact.0 == "referenced"
            && artifact.2 == 1
            && public.0 == 1
            && public.1 > 0
            && public.1 == public.2
        {
            break artifact.1;
        }
        assert!(
            Instant::now() < deadline,
            "PostgreSQL background pumps did not publish and reference terminal output"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let referenced_hex = referenced_hash.strip_prefix("sha256:").unwrap();
    let referenced_path = object_root.join(&referenced_hex[..2]).join(referenced_hex);
    assert!(referenced_path.is_file());
    assert!(
        sqlx::query_scalar::<_, i64>(
            "SELECT
                (SELECT COUNT(*) FROM scheduler_values
                 WHERE run_id=$1 AND storage_kind='artifact') +
                (SELECT COUNT(*) FROM scheduler_occurrence_values
                 WHERE run_id=$1 AND storage_kind='artifact')",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap()
            > 0
    );

    first.shutdown(Duration::from_secs(1)).await.unwrap();

    let restarted_repository = Arc::new(
        insight_agent_platform::engine::repository::PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    let restarted_store = Arc::new(
        LocalContentAddressedArtifactStore::open_shared(
            object_root.join("..").join("objects"),
            1,
            "pg_background_pumps",
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        store.deployment_contract(),
        restarted_store.deployment_contract()
    );
    let mut restarted_config = production_config(Duration::from_millis(5));
    restarted_config.artifact_orphan_retention = Duration::from_secs(1);
    restarted_config.artifact_gc_interval = Duration::from_millis(5);
    restarted_config.public_event_nonterminal_retention = Duration::from_secs(1);
    restarted_config.public_event_prune_interval = Duration::from_millis(5);
    let restarted = RunService::start_with_artifact_store(
        deployed_catalog().0,
        restarted_repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        restarted_store.clone(),
        restarted_config,
    )
    .await
    .unwrap();

    let prune_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (terminal, nonterminal) = sqlx::query_as::<_, (i64, i64)>(
            "SELECT
                COUNT(*) FILTER (WHERE is_terminal AND publish_state='published'),
                COUNT(*) FILTER (WHERE NOT is_terminal)
             FROM public_event_outbox WHERE run_id=$1",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(terminal, 1, "terminal public authority must remain durable");
        if nonterminal == 0 {
            break;
        }
        assert!(
            Instant::now() < prune_deadline,
            "PostgreSQL nonterminal public events were not pruned after restart"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(referenced_path.is_file());

    let run_id = insight_agent_platform::engine::RunId::new(created.run_id.clone()).unwrap();
    let orphan_bytes = b"postgres-verified-but-never-referenced";
    let orphan = restarted_store
        .artifact_for_bytes(orphan_bytes, Some("application/octet-stream".to_owned()))
        .unwrap();
    let orphan_locator = restarted_store.storage_locator(&orphan).unwrap();
    let orphan_hex = orphan
        .content_hash()
        .as_str()
        .strip_prefix("sha256:")
        .unwrap();
    let orphan_path = object_root.join(&orphan_hex[..2]).join(orphan_hex);
    restarted_repository
        .stage_artifact(StageArtifactCommand::new(
            run_id,
            orphan.clone(),
            orphan_locator,
            None,
        ))
        .await
        .unwrap();
    let (orphan_hash, orphan_size) = restarted_store
        .put_and_verify(&orphan, orphan_bytes)
        .await
        .unwrap();
    restarted_repository
        .verify_artifact(VerifyArtifactCommand::new(
            insight_agent_platform::engine::RunId::new(created.run_id.clone()).unwrap(),
            orphan.artifact_id().clone(),
            orphan_hash,
            orphan_size,
        ))
        .await
        .unwrap();
    assert!(orphan_path.is_file());
    sqlx::query(
        "UPDATE artifacts
         SET created_at=CURRENT_TIMESTAMP-INTERVAL '10 seconds'
         WHERE run_id=$1 AND artifact_id=$2",
    )
    .bind(&created.run_id)
    .bind(orphan.artifact_id().as_str())
    .execute(&control)
    .await
    .unwrap();

    let gc_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let state = sqlx::query_scalar::<_, String>(
            "SELECT artifact_state FROM artifacts WHERE run_id=$1 AND artifact_id=$2",
        )
        .bind(&created.run_id)
        .bind(orphan.artifact_id().as_str())
        .fetch_one(&control)
        .await
        .unwrap();
        if state == "deleted" {
            break;
        }
        assert!(
            Instant::now() < gc_deadline,
            "PostgreSQL background Artifact GC did not delete the orphan"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!orphan_path.exists());

    sqlx::query(
        "UPDATE artifact_retention_releases
         SET retain_until=CURRENT_TIMESTAMP-INTERVAL '1 second'
         WHERE run_id=$1",
    )
    .bind(&created.run_id)
    .execute(&control)
    .await
    .unwrap();
    let referenced_gc_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let state = sqlx::query_scalar::<_, String>(
            "SELECT artifact_state FROM artifacts
             WHERE run_id=$1 AND content_hash=$2",
        )
        .bind(&created.run_id)
        .bind(&referenced_hash)
        .fetch_one(&control)
        .await
        .unwrap();
        if state == "deleted" {
            break;
        }
        assert!(
            Instant::now() < referenced_gc_deadline,
            "PostgreSQL referenced Artifact was not collected after its durable deadline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!referenced_path.exists());

    restarted.shutdown(Duration::from_secs(1)).await.unwrap();
    control.close().await;
    drop(restarted_repository);
    drop(repository);
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
