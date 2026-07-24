use std::sync::Arc;

use insight_dsl::{compile_source, CompileOptions};
use insight_durable::{
    ActivationAdmissionCommand, ActivationDurableRepository, CreateRunCommand, DurableRepository,
    PlanInstallOutcome, PublicEventOutboxRepository, ReceiveSignalCommand, VersionedPlan,
};
use insight_engine::{
    repository::REPOSITORY_INTENT_CONFLICT, ActivationId, DefinitionRevisionId,
    DeploymentRevisionId, ExecutionKind, NodeId, PublicEventPayload, RunId, ScopeInstanceId,
    SignalId, TransitionKey, TransitionOutcome,
};
use insight_storage::{PostgresDurableRepository, SqliteDurableRepository};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use uuid::Uuid;

const PLAN_SOURCE: &str = r#"api_version: insight.agent/v1
kind: agent
inputs:
  question: string
output: string
workflow:
  steps:
    - return: fixed
"#;

fn verified_plan(label: &str) -> insight_engine::Plan {
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
        json!({"author": "structured"}),
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

async fn isolated_postgres_repository(
) -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("TEST_POSTGRES_URL").ok()?;
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
            "CI must set TEST_POSTGRES_URL for the PostgreSQL repository contract"
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
