use std::{sync::Arc, time::Duration};

use insight_agent_platform::{
    catalog_v3::{
        compile_v3_agent_dir, DeployedV3Agent, LeafDeploymentResolver, ResolvedLeafDeployment,
    },
    dsl::{
        v3::{compile_source, CompileOptions},
        CompileError,
    },
    engine::{
        plan::LeafTaskDescriptor,
        repository::{
            ActivationAdmissionCommand, ActivationDurableRepository, CreateRunCommand,
            DurableRepository, PlanInstallOutcome, PostgresDurableRepository,
            PublicEventOutboxRepository, ReceiveSignalCommand, SqliteDurableRepository,
            VersionedPlan, REPOSITORY_CONFIGURATION_INVALID, REPOSITORY_INTENT_CONFLICT,
        },
        ActivationId, AdmissionState, DefinitionRevisionId, DeploymentRevisionId,
        ExecutionEventContext, ExecutionEventPayload, ExecutionKind, IntentHash, LeafTaskKind,
        NodeId, PendingExecutionEvent, PublicEventPayload, RunId, RunLifecycle, ScopeInstanceId,
        SemanticHash, SignalId, SubflowContractRegistry, TerminationReason, TransitionKey,
        TransitionOutcome, VersionTag, WorkerExecutorRegistry,
    },
    runtime::{
        DeployedAgentCatalog, ProductionRunRepository, RequestMetadata, RunService,
        RunServiceConfig,
    },
};
use serde_json::{json, Value};
use sqlx::{
    postgres::PgPoolOptions, sqlite::SqliteConnectOptions, AssertSqlSafe, ConnectOptions, PgPool,
    SqlitePool,
};
use uuid::Uuid;

const PLAN_SOURCE: &str = r#"api_version: insight.agent/v3
kind: agent
inputs:
  question: string
output: string
workflow:
  steps:
    - return: fixed
"#;

fn verified_plan(label: &str) -> insight_agent_platform::engine::Plan {
    compile_source(
        PLAN_SOURCE,
        CompileOptions::new(
            DefinitionRevisionId::new(format!("definition_revision_{label}")).unwrap(),
            format!("{label}.yaml"),
            PLAN_SOURCE,
        ),
    )
    .unwrap()
}

fn versioned_plan(label: &str, resolved_bindings: Value, worker_contracts: Value) -> VersionedPlan {
    VersionedPlan::from_verified_plan(
        format!("definition_{label}"),
        format!("agent_{label}"),
        format!("Repository contract {label}"),
        DeploymentRevisionId::new(format!("deployment_revision_{label}")).unwrap(),
        "expression-3.0.0",
        json!({"author": "structured-v3"}),
        &verified_plan(label),
        json!({"return": "descriptor-v1"}),
        resolved_bindings,
        worker_contracts,
    )
    .unwrap()
}

fn key(label: &str) -> TransitionKey {
    TransitionKey::derive("repository.path.independent.contract", &[label]).unwrap()
}

struct NoLeafResolver;

impl LeafDeploymentResolver for NoLeafResolver {
    fn resolve_leaf(
        &self,
        _kind: LeafTaskKind,
        _descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        ResolvedLeafDeployment::new(VersionTag::new("unused-worker").unwrap(), json!({}))
    }
}

#[derive(serde::Serialize)]
struct TypedPublicEventIntent<'a> {
    payload: &'a PublicEventPayload,
}

#[derive(serde::Serialize)]
struct TypedRunTransitionIntent<'a> {
    run_id: &'a RunId,
    expected_projection_version: u64,
    expected_lifecycle: RunLifecycle,
    expected_admission: AdmissionState,
    next_lifecycle: RunLifecycle,
    next_admission: AdmissionState,
    termination_intent_reason: Option<TerminationReason>,
    terminal: Option<()>,
    event: &'a PendingExecutionEvent,
    public_event: Option<TypedPublicEventIntent<'a>>,
}

/// Makes a deliberately non-authoritative in-memory `Plan` without relying on
/// its private field order. The public accessor locates the semantic hash
/// within this exclusively borrowed value; the shared borrow ends before the
/// slot is replaced. This is test-only evidence that downstream authority
/// boundaries re-run `Plan::verify` rather than trusting the Rust type alone.
fn forge_in_memory_semantic_hash(plan: &mut insight_agent_platform::engine::Plan) {
    let plan_start = std::ptr::from_mut(plan).cast::<u8>();
    let hash_address = {
        let hash = plan.semantic_hash();
        std::ptr::from_ref(hash).cast::<u8>()
    };
    let hash_offset = unsafe { hash_address.offset_from(plan_start.cast_const()) };
    let hash_slot = unsafe { plan_start.offset(hash_offset).cast::<SemanticHash>() };
    let forged = SemanticHash::parse(format!("sha256:{}", "0".repeat(64))).unwrap();
    assert_ne!(plan.semantic_hash(), &forged);
    let original = unsafe { std::ptr::replace(hash_slot, forged) };
    drop(original);
}

async fn isolated_postgres_repository(
) -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("V3_TEST_POSTGRES_URL").ok()?;
    let schema = format!("repository_contract_{}", Uuid::new_v4().simple());
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
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();
    Some((repository, control, admin, schema))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_concurrent_writers_commit_once_replay_exactly_and_keep_one_event_authority() {
    let Some((repository, control, admin, schema)) = isolated_postgres_repository().await else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set V3_TEST_POSTGRES_URL for the PostgreSQL repository contract"
        );
        return;
    };

    let plan = versioned_plan(
        "postgres_concurrent_authority",
        json!({"model": "fixed"}),
        json!({"worker": "worker-v1"}),
    );
    assert_eq!(
        repository.install_versioned_plan(&plan).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let run_id = RunId::new("run_postgres_concurrent_authority").unwrap();
    let create_key = key("postgres.concurrent.create");
    let create = CreateRunCommand::new(
        run_id.clone(),
        &plan,
        json!({"question": "same authoritative input"}),
    )
    .unwrap();

    const WRITERS: usize = 8;
    let barrier = Arc::new(tokio::sync::Barrier::new(WRITERS + 1));
    let mut tasks = Vec::with_capacity(WRITERS);
    for _ in 0..WRITERS {
        let repository = repository.clone();
        let barrier = barrier.clone();
        let create_key = create_key.clone();
        let create = create.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            repository.create_run(create_key, create).await
        }));
    }
    barrier.wait().await;

    let mut committed = 0;
    let mut replayed = 0;
    let mut authoritative = None;
    for task in tasks {
        let outcome = task.await.unwrap().unwrap();
        let receipt = match outcome {
            TransitionOutcome::Committed { result } => {
                committed += 1;
                result
            }
            TransitionOutcome::ExactReplay { authoritative } => {
                replayed += 1;
                authoritative
            }
            outcome => panic!("unexpected concurrent create outcome: {outcome:?}"),
        };
        if let Some(expected) = authoritative.as_ref() {
            assert_eq!(&receipt, expected);
        } else {
            authoritative = Some(receipt);
        }
    }
    assert_eq!(committed, 1);
    assert_eq!(replayed, WRITERS - 1);
    assert!(authoritative.unwrap().public_event_id().is_some());

    let activation_id = ActivationId::new("activation_postgres_concurrent_signal").unwrap();
    assert!(matches!(
        repository
            .admit_activation(
                key("postgres.concurrent.admit"),
                ActivationAdmissionCommand::new(
                    run_id.clone(),
                    ScopeInstanceId::root(),
                    0,
                    activation_id.clone(),
                    NodeId::new("postgres_concurrent_wait").unwrap(),
                    "postgres-concurrent-wait",
                    ExecutionKind::DurableWait,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));

    let signal = ReceiveSignalCommand::new(
        run_id.clone(),
        SignalId::new("signal_postgres_concurrent").unwrap(),
        "message-postgres-concurrent",
        "continue",
        activation_id,
        json!({"approved": true}),
    )
    .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(WRITERS + 1));
    let mut tasks = Vec::with_capacity(WRITERS);
    for _ in 0..WRITERS {
        let repository = repository.clone();
        let barrier = barrier.clone();
        let signal = signal.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            repository.receive_signal(signal).await
        }));
    }
    barrier.wait().await;

    let mut committed = 0;
    let mut replayed = 0;
    let mut authoritative = None;
    for task in tasks {
        let outcome = task.await.unwrap().unwrap();
        let receipt = match outcome {
            TransitionOutcome::Committed { result } => {
                committed += 1;
                result
            }
            TransitionOutcome::ExactReplay { authoritative } => {
                replayed += 1;
                authoritative
            }
            outcome => panic!("unexpected concurrent signal outcome: {outcome:?}"),
        };
        if let Some(expected) = authoritative.as_ref() {
            assert_eq!(&receipt, expected);
        } else {
            authoritative = Some(receipt);
        }
    }
    assert_eq!(committed, 1);
    assert_eq!(replayed, WRITERS - 1);

    let event_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution_events WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(
        event_count, 3,
        "create, admission, and signal each append once"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(DISTINCT seq) FROM execution_events WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        event_count
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT next_event_seq FROM workflow_runs WHERE run_id=$1",)
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        event_count + 1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM public_event_outbox WHERE run_id=$1",)
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        1,
        "only the typed Run-created projection is public"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM signals_inbox WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        1
    );

    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_repository_clones_serialize_one_transition_and_preserve_typed_public_output() {
    let repository = SqliteDurableRepository::in_memory().await.unwrap();
    let plan = versioned_plan(
        "sqlite_serialized_authority",
        json!({"model": "fixed"}),
        json!({"worker": "worker-v1"}),
    );
    repository.install_versioned_plan(&plan).await.unwrap();
    let run_id = RunId::new("run_sqlite_serialized_authority").unwrap();
    let create_key = key("sqlite.serialized.create");
    let create = CreateRunCommand::new(
        run_id.clone(),
        &plan,
        json!({"question": "same authoritative input"}),
    )
    .unwrap();

    const WRITERS: usize = 8;
    let barrier = Arc::new(tokio::sync::Barrier::new(WRITERS + 1));
    let mut tasks = Vec::with_capacity(WRITERS);
    for _ in 0..WRITERS {
        let repository = repository.clone();
        let barrier = barrier.clone();
        let create_key = create_key.clone();
        let create = create.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            repository.create_run(create_key, create).await
        }));
    }
    barrier.wait().await;

    let mut committed = 0;
    let mut replayed = 0;
    let mut authoritative = None;
    for task in tasks {
        let outcome = task.await.unwrap().unwrap();
        let receipt = match outcome {
            TransitionOutcome::Committed { result } => {
                committed += 1;
                result
            }
            TransitionOutcome::ExactReplay { authoritative } => {
                replayed += 1;
                authoritative
            }
            outcome => panic!("unexpected SQLite create outcome: {outcome:?}"),
        };
        if let Some(expected) = authoritative.as_ref() {
            assert_eq!(&receipt, expected);
        } else {
            authoritative = Some(receipt);
        }
    }
    assert_eq!(committed, 1);
    assert_eq!(replayed, WRITERS - 1);
    assert_eq!(
        repository
            .load_run(&run_id)
            .await
            .unwrap()
            .unwrap()
            .next_event_seq(),
        2
    );

    assert_eq!(
        repository
            .create_run(
                create_key,
                CreateRunCommand::new(run_id, &plan, json!({"question": "different intent"}),)
                    .unwrap(),
            )
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_INTENT_CONFLICT
    );
    let claims = repository
        .claim_public_events("sqlite-contract-dispatcher", 30, 8)
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(
        claims[0].safe_envelope().payload(),
        &PublicEventPayload::RunCreated
    );
}

#[tokio::test]
async fn run_transition_intent_serialization_keeps_closed_typed_events() {
    let temporary = tempfile::tempdir().unwrap();
    let agent = temporary.path().join("typed-intent-agent");
    std::fs::create_dir_all(&agent).unwrap();
    std::fs::write(
        agent.join("agent.yaml"),
        r#"api_version: insight.agent/v3
kind: agent
metadata:
  id: repository_typed_intent
  name: Repository typed intent
  description: Freezes the closed transition intent wire contract.
inputs:
  question: string
output: string
workflow:
  steps:
    - return: $question
"#,
    )
    .unwrap();
    let published = Arc::new(compile_v3_agent_dir(&agent).unwrap());
    let deployed = Arc::new(
        DeployedV3Agent::publish(published, &NoLeafResolver, SubflowContractRegistry::new())
            .unwrap(),
    );

    let database = temporary.path().join("typed-intent.sqlite");
    let repository = Arc::new(
        SqliteDurableRepository::connect_path(&database)
            .await
            .unwrap(),
    );
    let service = RunService::start(
        DeployedAgentCatalog::new(vec![deployed]).unwrap(),
        repository as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        RunServiceConfig::single_process_development(8, 1, 1, 32),
    )
    .await
    .unwrap();
    let run = service
        .create_detached(
            "repository_typed_intent",
            json!({"question": "typed"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    service.shutdown(Duration::from_secs(1)).await.unwrap();

    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .create_if_missing(false)
            .disable_statement_logging(),
    )
    .await
    .unwrap();
    let (stored_intent_hash, stored_execution_payload) = sqlx::query_as::<_, (String, String)>(
        "SELECT intent_hash,safe_payload FROM execution_events
             WHERE run_id=? AND kind='run.lifecycle_changed'
               AND json_extract(safe_payload,'$.lifecycle')='active'",
    )
    .bind(&run.run_id)
    .fetch_one(&control)
    .await
    .unwrap();
    let stored_public_envelope = sqlx::query_scalar::<_, String>(
        "SELECT safe_envelope FROM public_event_outbox
         WHERE run_id=? AND event_kind='run.started'",
    )
    .bind(&run.run_id)
    .fetch_one(&control)
    .await
    .unwrap();

    let run_id = RunId::new(run.run_id).unwrap();
    let event = PendingExecutionEvent::new(
        ExecutionEventContext::for_run(run_id.clone()),
        ExecutionEventPayload::RunLifecycleChanged {
            lifecycle: RunLifecycle::Active,
        },
    )
    .unwrap();
    let public_payload = PublicEventPayload::RunStarted;
    let typed_command = TypedRunTransitionIntent {
        run_id: &run_id,
        expected_projection_version: 0,
        expected_lifecycle: RunLifecycle::Created,
        expected_admission: AdmissionState::Open,
        next_lifecycle: RunLifecycle::Active,
        next_admission: AdmissionState::Open,
        termination_intent_reason: None,
        terminal: None,
        event: &event,
        public_event: Some(TypedPublicEventIntent {
            payload: &public_payload,
        }),
    };
    let typed_command_wire = serde_json::to_value(&typed_command).unwrap();

    assert_eq!(
        IntentHash::from_serializable(&typed_command)
            .unwrap()
            .as_str(),
        stored_intent_hash.as_str(),
        "the committed RunTransitionCommand hash must cover a PendingExecutionEvent and a typed PublicEventPayload"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&stored_execution_payload).unwrap(),
        typed_command_wire["event"]["payload"]
    );
    assert_eq!(
        serde_json::from_str::<Value>(&stored_public_envelope).unwrap()["payload"],
        typed_command_wire["public_event"]["payload"]
    );
    assert!(typed_command_wire.get("safe_event_payload").is_none());
    assert!(typed_command_wire["public_event"]
        .get("event_kind")
        .is_none());

    let mut raw_event_fallback = typed_command_wire.clone();
    let typed_event = raw_event_fallback
        .as_object_mut()
        .unwrap()
        .remove("event")
        .unwrap();
    raw_event_fallback["safe_event_payload"] = typed_event["payload"].clone();
    assert_ne!(
        IntentHash::from_serializable(&raw_event_fallback)
            .unwrap()
            .as_str(),
        stored_intent_hash.as_str(),
        "a raw safe_event_payload fallback must not reproduce the committed intent"
    );

    let mut string_kind_fallback = typed_command_wire;
    string_kind_fallback["public_event"] = json!({"event_kind": "run.started"});
    assert_ne!(
        IntentHash::from_serializable(&string_kind_fallback)
            .unwrap()
            .as_str(),
        stored_intent_hash.as_str(),
        "a String event kind fallback must not reproduce the committed intent"
    );
}

#[test]
fn repository_authority_hashes_are_verified_and_content_derived() {
    let plan = verified_plan("derived_authority");
    let first = VersionedPlan::from_verified_plan(
        "definition_derived_authority",
        "agent_derived_authority",
        "Derived authority",
        DeploymentRevisionId::new("deployment_revision_derived_authority").unwrap(),
        "expression-3.0.0",
        json!({"author": "structured-v3"}),
        &plan,
        json!({"return": "descriptor-v1"}),
        serde_json::from_str(r#"{"temperature":0,"model":"fixed"}"#).unwrap(),
        serde_json::from_str(r#"{"version":1,"implementation":"worker"}"#).unwrap(),
    )
    .unwrap();
    let reordered = VersionedPlan::from_verified_plan(
        "definition_derived_authority",
        "agent_derived_authority",
        "Derived authority",
        DeploymentRevisionId::new("deployment_revision_derived_authority").unwrap(),
        "expression-3.0.0",
        json!({"author": "structured-v3"}),
        &plan,
        json!({"return": "descriptor-v1"}),
        serde_json::from_str(r#"{"model":"fixed","temperature":0}"#).unwrap(),
        serde_json::from_str(r#"{"implementation":"worker","version":1}"#).unwrap(),
    )
    .unwrap();
    let changed = VersionedPlan::from_verified_plan(
        "definition_derived_authority",
        "agent_derived_authority",
        "Derived authority",
        DeploymentRevisionId::new("deployment_revision_derived_authority").unwrap(),
        "expression-3.0.0",
        json!({"author": "structured-v3"}),
        &plan,
        json!({"return": "descriptor-v1"}),
        json!({"model": "fixed", "temperature": 1}),
        json!({"implementation": "worker", "version": 1}),
    )
    .unwrap();

    assert_eq!(first.plan_hash().as_str(), plan.semantic_hash().as_str());
    assert_eq!(first.plan_hash(), reordered.plan_hash());
    assert_eq!(first.binding_hash(), reordered.binding_hash());
    assert_ne!(first.binding_hash(), changed.binding_hash());

    let mut forged_plan = plan;
    forge_in_memory_semantic_hash(&mut forged_plan);
    assert!(forged_plan.verify().is_err());
    assert_eq!(
        VersionedPlan::from_verified_plan(
            "definition_forged_authority",
            "agent_forged_authority",
            "Forged authority",
            DeploymentRevisionId::new("deployment_revision_forged_authority").unwrap(),
            "expression-3.0.0",
            json!({"author": "structured-v3"}),
            &forged_plan,
            json!({"return": "descriptor-v1"}),
            json!({"model": "fixed"}),
            json!({"worker": "worker-v1"}),
        )
        .unwrap_err()
        .code(),
        REPOSITORY_CONFIGURATION_INVALID,
        "from_verified_plan must call Plan::verify even for an in-memory Plan value"
    );
}
