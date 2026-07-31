mod support;

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use insight_dsl::{compile_source, CompileOptions};
use insight_durable::{
    model::adapter as model_adapter, CreateMcpInteractionCommand, CreateRunCommand,
    DurableRepository, McpInteractionDisposition, McpInteractionDurableRepository,
    McpInteractionId, McpInteractionListFilter, McpInteractionOutcome, McpInteractionPrincipal,
    McpInteractionRequest, McpInteractionState, McpSecretCiphertext, PlanInstallOutcome,
    ResolveMcpInteractionCommand, RunTransitionCommand, TransitionMcpInteractionCommand,
    VersionedPlan,
};
use insight_engine::{
    run_stream::RUN_STREAM_PROTOCOL_VERSION, AdmissionState, ContentHash, DefinitionRevisionId,
    DeploymentRevisionId, ExecutionEventContext, ExecutionEventPayload, PendingExecutionEvent,
    PublicEventPayload, RunId, RunLifecycle, TransitionKey, TransitionOutcome,
};
use insight_storage::{PostgresDurableRepository, SqliteDurableRepository};
use serde_json::json;
use sqlx::{
    postgres::PgPoolOptions,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    AssertSqlSafe, PgPool,
};
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

fn now() -> DateTime<Utc> {
    "2026-07-30T12:00:00Z".parse().unwrap()
}

fn versioned_plan(label: &str) -> VersionedPlan {
    let plan = compile_source(
        PLAN_SOURCE,
        CompileOptions::new(
            DefinitionRevisionId::new(format!("definition_revision_{label}")).unwrap(),
            format!("{label}.yaml"),
            PLAN_SOURCE,
        ),
    )
    .unwrap();
    VersionedPlan::from_verified_plan(
        format!("definition_{label}"),
        format!("agent_{label}"),
        format!("MCP interaction repository contract {label}"),
        DeploymentRevisionId::new(format!("deployment_revision_{label}")).unwrap(),
        "expression-3.0.0",
        json!({"author": "structured"}),
        &plan,
        json!({"return": "descriptor-v1"}),
        json!({"model": "fixed"}),
        json!({"worker": "worker-v1"}),
    )
    .unwrap()
}

fn key(label: &str) -> TransitionKey {
    TransitionKey::derive("mcp.interaction.repository.contract", &[label]).unwrap()
}

async fn create_run<R: DurableRepository>(repository: &R, label: &str) -> RunId {
    let plan = versioned_plan(label);
    assert_eq!(
        repository.install_versioned_plan(&plan).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let run_id = RunId::new(format!("run_{label}")).unwrap();
    assert!(matches!(
        repository
            .create_run(
                key(&format!("{label}.create")),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"question": "safe"})).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    run_id
}

async fn prepare_completing_run<R: DurableRepository>(repository: &R, label: &str, run_id: &RunId) {
    let active = model_adapter::run_transition_nonterminal(
        run_id.clone(),
        0,
        RunLifecycle::Created,
        AdmissionState::Open,
        RunLifecycle::Active,
        AdmissionState::Open,
        PendingExecutionEvent::new(
            ExecutionEventContext::for_run(run_id.clone()),
            ExecutionEventPayload::RunLifecycleChanged {
                lifecycle: RunLifecycle::Active,
            },
        )
        .unwrap(),
        None,
    )
    .unwrap();
    assert!(matches!(
        repository
            .commit_run_transition(key(&format!("{label}.active")), active)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));

    let completing = model_adapter::run_transition_nonterminal(
        run_id.clone(),
        1,
        RunLifecycle::Active,
        AdmissionState::Open,
        RunLifecycle::Completing,
        AdmissionState::Draining,
        PendingExecutionEvent::new(
            ExecutionEventContext::for_run(run_id.clone()),
            ExecutionEventPayload::RunLifecycleChanged {
                lifecycle: RunLifecycle::Completing,
            },
        )
        .unwrap(),
        None,
    )
    .unwrap();
    assert!(matches!(
        repository
            .commit_run_transition(key(&format!("{label}.completing")), completing)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
}

fn terminal_success_command(run_id: &RunId) -> RunTransitionCommand {
    model_adapter::run_transition_terminal_success(
        run_id.clone(),
        2,
        json!({"answer": "complete"}),
        PendingExecutionEvent::new(
            ExecutionEventContext::for_run(run_id.clone()),
            ExecutionEventPayload::RunLifecycleChanged {
                lifecycle: RunLifecycle::Succeeded,
            },
        )
        .unwrap(),
        model_adapter::public_event_intent(PublicEventPayload::RunCompleted),
    )
    .unwrap()
}

async fn complete_run<R: DurableRepository>(repository: &R, label: &str, run_id: &RunId) {
    prepare_completing_run(repository, label, run_id).await;
    assert!(matches!(
        repository
            .commit_run_transition(
                key(&format!("{label}.completed")),
                terminal_success_command(run_id),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
}

fn create_interaction(label: &str, run_id: &RunId, generation: u32) -> CreateMcpInteractionCommand {
    let separator = match generation {
        1 => ':',
        2 => '-',
        3 => '.',
        _ => '_',
    };
    CreateMcpInteractionCommand::new(
        McpInteractionId::new(format!("interaction_{label}{separator}{generation}")).unwrap(),
        McpInteractionPrincipal::new("tenant-a", "user-a").unwrap(),
        run_id.as_str(),
        "operation-a",
        "server-a",
        "a".repeat(64),
        "elicitation-a",
        generation,
        McpInteractionRequest::Form {
            message: "Choose a safe value".to_owned(),
            requested_schema: json!({
                "type": "object",
                "properties": {
                    "answer": {"type": "string", "minLength": 1, "maxLength": 32}
                },
                "required": ["answer"],
                "additionalProperties": false
            }),
        },
        McpSecretCiphertext::new(format!("enc:v1:request-{label}-{generation}")).unwrap(),
        "b".repeat(64),
        now() + Duration::minutes(10),
        now(),
    )
    .unwrap()
}

async fn exercise_repository<R>(repository: R, label: &str)
where
    R: DurableRepository + McpInteractionDurableRepository + Clone + Send + Sync + 'static,
{
    let run_id = create_run(&repository, label).await;
    let create = create_interaction(label, &run_id, 1);
    let interaction_id = create.interaction().interaction_id().clone();

    assert!(matches!(
        repository
            .create_mcp_interaction(create.clone())
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository.create_mcp_interaction(create).await.unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));

    let interaction = repository
        .load_mcp_interaction(&interaction_id)
        .await
        .unwrap()
        .unwrap();
    let principal = interaction.principal().clone();
    let accept = ResolveMcpInteractionCommand::new(
        &interaction,
        principal.clone(),
        "accept-first-winner",
        interaction.version(),
        McpInteractionDisposition::Accept,
        Some(&json!({"answer": "yes"})),
        Some(McpSecretCiphertext::new("enc:v1:response-accepted").unwrap()),
        Some("c".repeat(64)),
        now() + Duration::seconds(1),
    )
    .unwrap();
    let cancel = ResolveMcpInteractionCommand::new(
        &interaction,
        principal.clone(),
        "cancel-first-winner",
        interaction.version(),
        McpInteractionDisposition::Cancel,
        None,
        None,
        None,
        now() + Duration::seconds(1),
    )
    .unwrap();

    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let accept_task = {
        let repository = repository.clone();
        let barrier = barrier.clone();
        let command = accept.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            repository.resolve_mcp_interaction(command).await
        })
    };
    let cancel_task = {
        let repository = repository.clone();
        let barrier = barrier.clone();
        let command = cancel.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            repository.resolve_mcp_interaction(command).await
        })
    };
    barrier.wait().await;
    let accept_outcome = accept_task.await.unwrap().unwrap();
    let cancel_outcome = cancel_task.await.unwrap().unwrap();
    assert_eq!(
        [
            matches!(&accept_outcome, TransitionOutcome::Committed { .. }),
            matches!(&cancel_outcome, TransitionOutcome::Committed { .. }),
        ]
        .into_iter()
        .filter(|committed| *committed)
        .count(),
        1
    );
    assert_eq!(
        [
            matches!(&accept_outcome, TransitionOutcome::StateConflict),
            matches!(&cancel_outcome, TransitionOutcome::StateConflict),
        ]
        .into_iter()
        .filter(|conflict| *conflict)
        .count(),
        1
    );
    let winner_replay = if matches!(&accept_outcome, TransitionOutcome::Committed { .. }) {
        repository.resolve_mcp_interaction(accept).await.unwrap()
    } else {
        repository.resolve_mcp_interaction(cancel).await.unwrap()
    };
    assert!(matches!(
        winner_replay,
        TransitionOutcome::ExactReplay { .. }
    ));

    let authoritative = repository
        .load_mcp_interaction(&interaction_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(authoritative.version(), 2);
    assert!(matches!(
        authoritative.state(),
        McpInteractionState::Responded | McpInteractionState::Closed
    ));
    let secret = repository
        .load_mcp_interaction_secret(&interaction_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        secret.response_secret.is_some(),
        authoritative.state() == McpInteractionState::Responded
    );
    assert!(!format!("{secret:?}").contains("response-accepted"));

    let second_create = create_interaction(label, &run_id, 2);
    let second_id = second_create.interaction().interaction_id().clone();
    let second = match repository
        .create_mcp_interaction(second_create)
        .await
        .unwrap()
    {
        TransitionOutcome::Committed { result } => result,
        outcome => panic!("unexpected create outcome: {outcome:?}"),
    };
    let wrong_principal = ResolveMcpInteractionCommand::new(
        &second,
        McpInteractionPrincipal::new("tenant-a", "user-b").unwrap(),
        "wrong-principal",
        second.version(),
        McpInteractionDisposition::Cancel,
        None,
        None,
        None,
        now() + Duration::seconds(2),
    )
    .unwrap();
    assert!(matches!(
        repository
            .resolve_mcp_interaction(wrong_principal)
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    ));
    let accept_second = ResolveMcpInteractionCommand::new(
        &second,
        principal.clone(),
        "accept-for-retry",
        second.version(),
        McpInteractionDisposition::Accept,
        Some(&json!({"answer": "continue"})),
        Some(McpSecretCiphertext::new("enc:v1:response-for-retry").unwrap()),
        Some("d".repeat(64)),
        now() + Duration::seconds(3),
    )
    .unwrap();
    let responded = match repository
        .resolve_mcp_interaction(accept_second)
        .await
        .unwrap()
    {
        TransitionOutcome::Committed { result } => result,
        outcome => panic!("unexpected resolve outcome: {outcome:?}"),
    };
    assert_eq!(responded.state(), McpInteractionState::Responded);
    assert_eq!(responded.outcome(), Some(McpInteractionOutcome::Accepted));
    assert!(repository
        .list_mcp_interactions_ready_for_retry(16)
        .await
        .unwrap()
        .iter()
        .any(|item| item.interaction_id() == &second_id));

    let retrying = match repository
        .transition_mcp_interaction(
            TransitionMcpInteractionCommand::begin_retry(
                second_id.clone(),
                "begin-retry",
                responded.version(),
                now() + Duration::seconds(4),
            )
            .unwrap(),
        )
        .await
        .unwrap()
    {
        TransitionOutcome::Committed { result } => result,
        outcome => panic!("unexpected begin retry outcome: {outcome:?}"),
    };
    assert_eq!(retrying.state(), McpInteractionState::Retrying);

    let closed = match repository
        .transition_mcp_interaction(
            TransitionMcpInteractionCommand::close(
                second_id,
                "finish-retry",
                retrying.version(),
                McpInteractionOutcome::RetryCompleted,
                now() + Duration::seconds(5),
            )
            .unwrap(),
        )
        .await
        .unwrap()
    {
        TransitionOutcome::Committed { result } => result,
        outcome => panic!("unexpected close outcome: {outcome:?}"),
    };
    assert_eq!(closed.state(), McpInteractionState::Closed);
    assert_eq!(
        closed.outcome(),
        Some(McpInteractionOutcome::RetryCompleted)
    );

    let listed = repository
        .list_mcp_interactions(
            &principal,
            &McpInteractionListFilter {
                run_id: Some(run_id.as_str().to_owned()),
                state: None,
                after_interaction_id: None,
            },
            16,
        )
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);

    let terminal_create = create_interaction(label, &run_id, 3);
    let terminal_create_replay = terminal_create.clone();
    let terminal_interaction_id = terminal_create.interaction().interaction_id().clone();
    assert!(matches!(
        repository
            .create_mcp_interaction(terminal_create)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));

    complete_run(&repository, label, &run_id).await;

    let terminal_interaction = repository
        .load_mcp_interaction(&terminal_interaction_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal_interaction.state(), McpInteractionState::Closed);
    assert_eq!(
        terminal_interaction.outcome(),
        Some(McpInteractionOutcome::RunTerminal)
    );
    assert_eq!(terminal_interaction.version(), 2);
    let preserved = repository
        .load_mcp_interaction(closed.interaction_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(preserved.state(), McpInteractionState::Closed);
    assert_eq!(
        preserved.outcome(),
        Some(McpInteractionOutcome::RetryCompleted)
    );
    assert_eq!(preserved.version(), closed.version());

    let snapshot = repository
        .load_run_stream_snapshot(&run_id)
        .await
        .unwrap()
        .unwrap();
    let summaries = snapshot.run()["interactions"].as_array().unwrap();
    assert_eq!(summaries.len(), 3);
    assert!(summaries.windows(2).all(|pair| {
        pair[0]["interaction_id"].as_str().unwrap().as_bytes()
            < pair[1]["interaction_id"].as_str().unwrap().as_bytes()
    }));
    let terminal_summary = summaries
        .iter()
        .find(|summary| {
            summary["interaction_id"].as_str() == Some(terminal_interaction_id.as_str())
        })
        .unwrap();
    assert_eq!(terminal_summary["state"], "closed");
    assert_eq!(terminal_summary["outcome"], "run_terminal");
    let encoded = serde_json::to_string(snapshot.run()).unwrap();
    for forbidden in [
        "Choose a safe value",
        "requested_schema",
        "response-for-retry",
        "request_secret",
    ] {
        assert!(!encoded.contains(forbidden));
    }
    let hash_projection = json!({
        "protocol": RUN_STREAM_PROTOCOL_VERSION,
        "run_id": run_id.as_str(),
        "terminal_kind": snapshot.terminal_kind().as_str(),
        "run": snapshot.run(),
        "public_item_manifest": snapshot.public_item_manifest(),
    });
    assert_eq!(
        snapshot.snapshot_hash(),
        &ContentHash::from_bytes(&serde_jcs::to_vec(&hash_projection).unwrap())
    );

    assert!(matches!(
        repository
            .create_mcp_interaction(terminal_create_replay)
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
    assert!(matches!(
        repository
            .create_mcp_interaction(create_interaction(label, &run_id, 4))
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    ));
}

async fn isolated_postgres_repository(
) -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("TEST_POSTGRES_URL").ok()?;
    let schema = format!("mcp_interactions_{}", Uuid::new_v4().simple());
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
    support::provision_postgres_schema(&control).await;
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    Some((repository, control, admin, schema))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_mcp_interaction_is_first_winner_idempotent_and_retryable() {
    let (_temporary, repository): (_, SqliteDurableRepository) =
        support::temporary_sqlite_repository().await;
    exercise_repository(repository, "mcp_sqlite").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_mcp_interaction_create_replay_precedes_the_per_run_limit() {
    let (temporary, repository): (_, SqliteDurableRepository) =
        support::temporary_sqlite_repository().await;
    let run_id = create_run(&repository, "mcp_sqlite_create_limit").await;
    let first = create_interaction("mcp_sqlite_create_limit", &run_id, 1);
    let replay = first.clone();
    assert!(matches!(
        repository.create_mcp_interaction(first).await.unwrap(),
        TransitionOutcome::Committed { .. }
    ));

    let control = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(temporary.path().join("durable.sqlite3"))
                .foreign_keys(true),
        )
        .await
        .unwrap();
    sqlx::query(
        "WITH digits(d) AS (
             VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)
         ), numbers(n) AS (
             SELECT ones.d + 10*tens.d + 100*hundreds.d + 1000*thousands.d
             FROM digits ones CROSS JOIN digits tens
             CROSS JOIN digits hundreds CROSS JOIN digits thousands
         )
         INSERT INTO mcp_interactions(
             interaction_id,tenant_id,user_id,run_id,operation_id,server_id,binding_hash,
             logical_request_key,generation,request_json,interaction_state,outcome,
             interaction_version,deadline,created_at,updated_at,closed_at,creation_intent_hash
         )
         SELECT printf('interaction.limit-%04d',n),'tenant-a','user-a',?,
                'operation-limit','server-a',?,printf('request-limit-%04d',n),n,?,
                'requested',NULL,1,'2026-07-30T12:10:00.000000Z',
                '2026-07-30T12:00:00.000000Z','2026-07-30T12:00:00.000000Z',NULL,?
         FROM numbers WHERE n BETWEEN 2 AND 1024 ORDER BY n",
    )
    .bind(run_id.as_str())
    .bind("a".repeat(64))
    .bind(
        serde_json::to_string(&McpInteractionRequest::Approval {
            message: "count-only fixture".to_owned(),
            effect: "read_only".to_owned(),
        })
        .unwrap(),
    )
    .bind(format!("sha256:{}", "c".repeat(64)))
    .execute(&control)
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM mcp_interactions WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        1_024
    );

    assert!(matches!(
        repository.create_mcp_interaction(replay).await.unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
    assert!(matches!(
        repository
            .create_mcp_interaction(create_interaction(
                "mcp_sqlite_create_limit",
                &run_id,
                1_025,
            ))
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM mcp_interactions WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        1_024
    );
    control.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_mcp_interaction_create_racing_terminalization_has_one_lock_order() {
    let (_temporary, repository): (_, SqliteDurableRepository) =
        support::temporary_sqlite_repository().await;
    let label = "mcp_sqlite_create_terminal_race";
    let run_id = create_run(&repository, label).await;
    prepare_completing_run(&repository, label, &run_id).await;
    let create = create_interaction(label, &run_id, 1);
    let interaction_id = create.interaction().interaction_id().clone();
    let barrier = Arc::new(tokio::sync::Barrier::new(3));

    let create_task = {
        let repository = repository.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            repository.create_mcp_interaction(create).await
        })
    };
    let terminal_task = {
        let repository = repository.clone();
        let barrier = barrier.clone();
        let run_id = run_id.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            repository
                .commit_run_transition(
                    key("mcp_sqlite_create_terminal_race.completed"),
                    terminal_success_command(&run_id),
                )
                .await
        })
    };
    barrier.wait().await;
    let create_outcome = create_task.await.unwrap().unwrap();
    let terminal_outcome = terminal_task.await.unwrap().unwrap();
    assert!(matches!(
        terminal_outcome,
        TransitionOutcome::Committed { .. }
    ));

    let snapshot = repository
        .load_run_stream_snapshot(&run_id)
        .await
        .unwrap()
        .unwrap();
    let summaries = snapshot.run()["interactions"].as_array().unwrap();
    match create_outcome {
        TransitionOutcome::Committed { .. } => {
            let authoritative = repository
                .load_mcp_interaction(&interaction_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(authoritative.state(), McpInteractionState::Closed);
            assert_eq!(
                authoritative.outcome(),
                Some(McpInteractionOutcome::RunTerminal)
            );
            assert_eq!(summaries.len(), 1);
            assert_eq!(
                summaries[0]["interaction_id"].as_str(),
                Some(interaction_id.as_str())
            );
        }
        TransitionOutcome::StateConflict => {
            assert!(repository
                .load_mcp_interaction(&interaction_id)
                .await
                .unwrap()
                .is_none());
            assert!(summaries.is_empty());
        }
        outcome => panic!("unexpected create-versus-terminal outcome: {outcome:?}"),
    }
    assert!(matches!(
        repository
            .create_mcp_interaction(create_interaction(label, &run_id, 2))
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_mcp_interaction_is_first_winner_idempotent_and_retryable() {
    let Some((repository, control, admin, schema)) = isolated_postgres_repository().await else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set TEST_POSTGRES_URL for PostgreSQL MCP interaction conformance"
        );
        return;
    };
    exercise_repository(repository, "mcp_postgres").await;
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
