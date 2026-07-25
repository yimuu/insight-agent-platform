//! Public PostgreSQL repository conformance at the durable admission boundary.

mod support;

use insight_dsl::{compile_source, CompileOptions};
use insight_durable::{
    ActivationAdmissionCommand, ActivationDurableRepository, ContinueAsNewCommand,
    CreateRunCommand, DurableRepository, PlanInstallOutcome, ProjectionAudit,
    ProjectionDurableRepository, ProjectionSubject, ReceiveSignalCommand,
    RecoveryDurableRepository, VersionedPlan,
};
use insight_engine::{
    plan::{
        AuthorFormat, DataBinding, DataBindingId, DataPort, DataPortId, Node, NodeKind,
        PlanBuilder, PlanInputContract, PlanMetadata, PlanType, PortDirection, PortName,
        ReturnDescriptor, ScopeId, ScopeMetadata, ValueSource, VersionTag,
    },
    repository::REPOSITORY_DATA_INVALID,
    ActivationId, ContentHash, DefinitionRevisionId, DeploymentRevisionId, ExecutionKind, NodeId,
    RunId, RunLifecycle, ScopeInstanceId, SignalId, TransitionKey, TransitionOutcome,
};
use insight_storage::PostgresDurableRepository;
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use uuid::Uuid;

fn verified_plan() -> insight_engine::Plan {
    let revision = DefinitionRevisionId::new("definition_revision_pg_v1").unwrap();
    let return_id = insight_engine::NodeId::new("return_node").unwrap();
    let root_id = ScopeId::new("root_scope").unwrap();
    let value_input = DataPortId::new("return_value").unwrap();
    let safe_error = PlanType::safe_error().unwrap();
    let mut builder = PlanBuilder::new(PlanMetadata::new(
        revision,
        VersionTag::new("compiler-3.0.0").unwrap(),
        AuthorFormat::Programmatic,
        return_id.clone(),
        PlanInputContract::new(PlanType::Any),
        PlanType::Any,
        safe_error,
    ));
    builder
        .add_scope(ScopeMetadata::root(root_id.clone()))
        .add_node(Node::new(
            return_id.clone(),
            root_id,
            NodeKind::Return(ReturnDescriptor {
                value_input: value_input.clone(),
            }),
        ))
        .add_data_port(DataPort::new(
            value_input.clone(),
            return_id,
            PortName::new("value").unwrap(),
            PortDirection::Input,
            PlanType::Any,
            true,
        ))
        .add_data_binding(DataBinding::new(
            DataBindingId::new("bind_return").unwrap(),
            ValueSource::RunInput { path: vec![] },
            value_input,
        ));
    builder.build().unwrap()
}

fn versioned_plan() -> VersionedPlan {
    VersionedPlan::from_verified_plan(
        "definition_pg_smoke",
        "agent_pg_smoke",
        "PostgreSQL public repository smoke",
        DeploymentRevisionId::new("deployment_revision_pg_v1").unwrap(),
        "expression-3.0.0",
        json!({"format": "structured"}),
        &verified_plan(),
        json!({"return": "descriptor-v1"}),
        json!({"model": "fixed"}),
        json!({"worker": "worker-v1"}),
    )
    .unwrap()
}

fn normalized_input_versioned_plan() -> (insight_engine::Plan, VersionedPlan) {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs:
  question: string
  messages: {type: "Message[]", default: []}
output: string
workflow:
  steps:
    - return: fixed
"#;
    let plan = compile_source(
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("pg_normalized_recovery_input").unwrap(),
            "pg-normalized-recovery-input.yaml",
            source,
        ),
    )
    .unwrap();
    let versioned = VersionedPlan::from_verified_plan(
        "definition_pg_normalized_recovery_input",
        "agent_pg_normalized_recovery_input",
        "PostgreSQL normalized recovery input",
        DeploymentRevisionId::new("deployment_pg_normalized_recovery_input").unwrap(),
        "expression-3.0.0",
        json!({"author": "structured"}),
        &plan,
        json!({}),
        json!([]),
        json!([]),
    )
    .unwrap();
    (plan, versioned)
}

fn key(label: &str) -> TransitionKey {
    TransitionKey::derive("repository.pg.test", &[label]).unwrap()
}

async fn isolated_repository() -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("TEST_POSTGRES_URL").ok()?;
    let schema = format!("repository_{}", Uuid::new_v4().simple());
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

#[tokio::test]
async fn postgres_continue_as_new_rejects_unnormalized_target_input_before_commit() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    let (plan, versioned) = normalized_input_versioned_plan();
    repository.install_versioned_plan(&versioned).await.unwrap();

    let source = RunId::new("run_pg_continue_normalized_source").unwrap();
    let normalized = plan
        .metadata()
        .input_contract()
        .normalize(json!({"question": "source"}))
        .unwrap();
    assert!(matches!(
        repository
            .create_run(
                key("continue.normalized.source"),
                CreateRunCommand::new(source.clone(), &versioned, normalized).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    sqlx::query(
        "UPDATE workflow_runs
         SET lifecycle='active', admission_state='paused', projection_version=2
         WHERE run_id=$1",
    )
    .bind(source.as_str())
    .execute(&control)
    .await
    .unwrap();

    let rejected_target = RunId::new("run_pg_continue_missing_frozen_default").unwrap();
    let rejected = repository
        .continue_as_new(
            key("continue.missing-default"),
            ContinueAsNewCommand::new(
                source.clone(),
                rejected_target.clone(),
                2,
                json!({"question": "target"}),
                vec![],
            )
            .unwrap(),
        )
        .await;
    assert!(matches!(rejected, Ok(TransitionOutcome::StateConflict)));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM workflow_runs WHERE run_id=$1")
            .bind(rejected_target.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        0
    );

    let accepted_target = RunId::new("run_pg_continue_normalized_target").unwrap();
    let normalized = plan
        .metadata()
        .input_contract()
        .normalize(json!({"question": "target"}))
        .unwrap();
    assert!(matches!(
        repository
            .continue_as_new(
                key("continue.normalized-target"),
                ContinueAsNewCommand::new(source, accepted_target.clone(), 2, normalized, vec![],)
                    .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let stored = sqlx::query_scalar::<_, Value>(
        "SELECT p.inline_value
         FROM workflow_runs r JOIN payloads p
           ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id
         WHERE r.run_id=$1",
    )
    .bind(accepted_target.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(stored["messages"], json!([]));

    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

/// Runs locally only when a disposable PostgreSQL URL is supplied. CI makes
/// that URL mandatory through the separate PostgreSQL sentinel test.
#[tokio::test]
async fn postgres_public_admission_and_projection_repair_are_serialized() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    repository.check_health().await.unwrap();

    let plan = versioned_plan();
    assert_eq!(
        repository.install_versioned_plan(&plan).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    assert_eq!(
        repository.install_versioned_plan(&plan).await.unwrap(),
        PlanInstallOutcome::AlreadyInstalled
    );

    let run_id = RunId::new("run_pg_repository_smoke").unwrap();
    let create_key = key("create");
    let create = CreateRunCommand::new(run_id.clone(), &plan, json!({"input": 1})).unwrap();
    assert!(matches!(
        repository
            .create_run(create_key.clone(), create.clone())
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository.create_run(create_key, create).await.unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));

    let projection = repository.load_run(&run_id).await.unwrap().unwrap();
    assert_eq!(projection.lifecycle(), RunLifecycle::Created);
    assert_eq!(
        projection.definition_revision_id(),
        plan.definition_revision_id()
    );
    assert_eq!(
        projection.deployment_revision_id(),
        plan.deployment_revision_id()
    );

    let subject = ProjectionSubject::run(&run_id);
    assert!(matches!(
        repository
            .audit_projection(&run_id, &subject)
            .await
            .unwrap(),
        ProjectionAudit::Match { .. }
    ));
    sqlx::query("UPDATE workflow_runs SET lifecycle='active' WHERE run_id=$1")
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert!(matches!(
        repository
            .audit_projection(&run_id, &subject)
            .await
            .unwrap(),
        ProjectionAudit::Mismatch { .. }
    ));

    let first_repository = repository.clone();
    let second_repository = repository.clone();
    let first_run = run_id.clone();
    let second_run = run_id.clone();
    let first_subject = subject.clone();
    let second_subject = subject.clone();
    let (first, second) = tokio::join!(
        first_repository.repair_projection(&first_run, &first_subject),
        second_repository.repair_projection(&second_run, &second_subject),
    );
    let repaired_count = u8::from(first.unwrap().repaired()) + u8::from(second.unwrap().repaired());
    assert_eq!(repaired_count, 1, "row lock did not serialize repair");
    assert!(repository
        .audit_projection(&run_id, &subject)
        .await
        .unwrap()
        .is_match());
    assert_eq!(
        repository
            .load_run(&run_id)
            .await
            .unwrap()
            .unwrap()
            .lifecycle(),
        RunLifecycle::Created
    );

    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_concurrent_events_allocate_one_gapless_per_run_sequence_and_rollback_cleanly() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set TEST_POSTGRES_URL for the per-Run sequence gate"
        );
        return;
    };

    let plan = versioned_plan();
    repository.install_versioned_plan(&plan).await.unwrap();
    let run_id = RunId::new("run_pg_concurrent_event_sequence").unwrap();
    let create = CreateRunCommand::new(run_id.clone(), &plan, json!({"input": 1})).unwrap();
    assert!(matches!(
        repository
            .create_run(key("sequence.create"), create)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));

    let activation_id = ActivationId::new("activation_pg_sequence_wait").unwrap();
    let admission = ActivationAdmissionCommand::new(
        run_id.clone(),
        ScopeInstanceId::root(),
        0,
        activation_id.clone(),
        NodeId::new("sequence_wait").unwrap(),
        "sequence-wait",
        ExecutionKind::DurableWait,
    )
    .unwrap();
    assert!(matches!(
        repository
            .admit_activation(key("sequence.admit"), admission)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));

    const CONCURRENT_TRANSITIONS: usize = 8;
    let first_concurrent_seq: i64 =
        sqlx::query_scalar("SELECT next_event_seq FROM workflow_runs WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(CONCURRENT_TRANSITIONS + 1));
    let mut tasks = Vec::with_capacity(CONCURRENT_TRANSITIONS);
    for ordinal in 0..CONCURRENT_TRANSITIONS {
        let repository = repository.clone();
        let barrier = barrier.clone();
        let command = ReceiveSignalCommand::new(
            run_id.clone(),
            SignalId::new(format!("signal_pg_sequence_{ordinal}")).unwrap(),
            format!("message-pg-sequence-{ordinal}"),
            "sequence",
            activation_id.clone(),
            json!({"ordinal": ordinal}),
        )
        .unwrap();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            repository.receive_signal(command).await
        }));
    }
    barrier.wait().await;
    for task in tasks {
        assert!(matches!(
            task.await.unwrap().unwrap(),
            TransitionOutcome::Committed { .. }
        ));
    }

    let concurrent_seqs = sqlx::query_scalar::<_, i64>(
        "SELECT seq FROM execution_events
         WHERE run_id=$1 AND seq >= $2 ORDER BY seq",
    )
    .bind(run_id.as_str())
    .bind(first_concurrent_seq)
    .fetch_all(&control)
    .await
    .unwrap();
    let expected_concurrent_seqs = (first_concurrent_seq
        ..first_concurrent_seq + i64::try_from(CONCURRENT_TRANSITIONS).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(concurrent_seqs, expected_concurrent_seqs);
    assert!(concurrent_seqs.windows(2).all(|pair| pair[1] > pair[0]));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(DISTINCT seq) FROM execution_events WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution_events WHERE run_id=$1",)
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap()
    );
    let next_after_concurrency = first_concurrent_seq
        + i64::try_from(CONCURRENT_TRANSITIONS).expect("test transition count fits i64");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT next_event_seq FROM workflow_runs WHERE run_id=$1",)
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        next_after_concurrency
    );

    let event_count_before_fault: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_events WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap();
    let signal_count_before_fault: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM signals_inbox WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap();
    let payload_count_before_fault: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM payloads WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap();
    let checkpoint_count_before_fault: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM projection_checkpoint_batches WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap();

    sqlx::query(
        "CREATE FUNCTION fail_sequence_checkpoint_insert_fn() RETURNS trigger
         LANGUAGE plpgsql AS $body$
         BEGIN
           RAISE EXCEPTION 'fault after sequence and event allocation';
         END
         $body$",
    )
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_sequence_checkpoint_insert
         BEFORE INSERT ON projection_checkpoint_batches
         FOR EACH ROW
         EXECUTE FUNCTION fail_sequence_checkpoint_insert_fn()",
    )
    .execute(&control)
    .await
    .unwrap();

    let failed_command = ReceiveSignalCommand::new(
        run_id.clone(),
        SignalId::new("signal_pg_sequence_fault").unwrap(),
        "message-pg-sequence-fault",
        "sequence",
        activation_id,
        json!({"fault": true}),
    )
    .unwrap();
    assert!(repository
        .receive_signal(failed_command.clone())
        .await
        .is_err());
    sqlx::query("DROP TRIGGER fail_sequence_checkpoint_insert ON projection_checkpoint_batches")
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION fail_sequence_checkpoint_insert_fn()")
        .execute(&control)
        .await
        .unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT next_event_seq FROM workflow_runs WHERE run_id=$1",)
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        next_after_concurrency
    );
    for (query, expected) in [
        (
            "SELECT COUNT(*) FROM execution_events WHERE run_id=$1",
            event_count_before_fault,
        ),
        (
            "SELECT COUNT(*) FROM signals_inbox WHERE run_id=$1",
            signal_count_before_fault,
        ),
        (
            "SELECT COUNT(*) FROM payloads WHERE run_id=$1",
            payload_count_before_fault,
        ),
        (
            "SELECT COUNT(*) FROM projection_checkpoint_batches WHERE run_id=$1",
            checkpoint_count_before_fault,
        ),
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(query)
                .bind(run_id.as_str())
                .fetch_one(&control)
                .await
                .unwrap(),
            expected
        );
    }

    assert!(matches!(
        repository.receive_signal(failed_command).await.unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let final_seqs = sqlx::query_scalar::<_, i64>(
        "SELECT seq FROM execution_events WHERE run_id=$1 ORDER BY seq",
    )
    .bind(run_id.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    let final_event_count = i64::try_from(final_seqs.len()).unwrap();
    assert_eq!(final_seqs, (1..=final_event_count).collect::<Vec<_>>());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT next_event_seq FROM workflow_runs WHERE run_id=$1",)
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        final_event_count + 1
    );

    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_execution_event_authority_is_closed_immutable_and_schema_checked() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    let plan = versioned_plan();
    repository.install_versioned_plan(&plan).await.unwrap();
    let run_id = RunId::new("run_pg_event_authority").unwrap();
    let create_key = key("event.authority.pg.create");
    let create = CreateRunCommand::new(run_id.clone(), &plan, json!({"input": 1})).unwrap();
    assert!(matches!(
        repository
            .create_run(create_key.clone(), create.clone())
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));

    assert!(sqlx::query(
        "INSERT INTO execution_events (
            run_id,seq,event_id,schema_version,kind,transition_key,intent_hash,
            projection_version_after,safe_payload,occurred_at
         ) VALUES ($1,2,'event_pg_unknown_schema',999,'projection.mutated',$2,$3,0,'{}'::jsonb,CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(key("event.authority.pg.unknown").as_str())
    .bind(ContentHash::from_bytes(b"pg-unknown-schema").as_str())
    .execute(&control)
    .await
    .is_err());

    assert!(sqlx::query(
        "INSERT INTO execution_events (
            run_id,seq,event_id,schema_version,kind,transition_key,intent_hash,
            projection_version_after,safe_payload,occurred_at
         ) VALUES ($1,2,'event_pg_unknown_kind',2,'future.kind',$2,$3,0,'{}'::jsonb,CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(key("event.authority.pg.unknown-kind").as_str())
    .bind(ContentHash::from_bytes(b"pg-unknown-kind").as_str())
    .execute(&control)
    .await
    .is_err());

    let canonical = json!({
        "schema_version": 1,
        "subject_count": 0,
        "manifest_hash": ContentHash::from_bytes(b"pg-empty-projection-manifest").as_str(),
        "subjects": [],
    });
    assert!(sqlx::query(
        "INSERT INTO execution_events (
            run_id,seq,event_id,schema_version,kind,transition_key,intent_hash,
            projection_version_after,safe_payload,occurred_at,projection_ledger_batch
         ) VALUES ($1,2,'event_pg_prefilled_ledger',2,'projection.mutated',$2,$3,0,
                   '{\"type\":\"projection_mutated\",\"mutation\":\"signal_received\"}'::jsonb,
                   CURRENT_TIMESTAMP,$4)",
    )
    .bind(run_id.as_str())
    .bind(key("event.authority.pg.prefilled").as_str())
    .bind(ContentHash::from_bytes(b"pg-prefilled-ledger").as_str())
    .bind(&canonical)
    .execute(&control)
    .await
    .is_err());

    for (seq, event_id, transition, intent) in [
        (
            2_i64,
            "event_pg_ledger_once",
            key("event.authority.pg.once"),
            ContentHash::from_bytes(b"pg-ledger-once"),
        ),
        (
            3_i64,
            "event_pg_ledger_extra",
            key("event.authority.pg.extra"),
            ContentHash::from_bytes(b"pg-ledger-extra"),
        ),
    ] {
        sqlx::query(
            "INSERT INTO execution_events (
                run_id,seq,event_id,schema_version,kind,transition_key,intent_hash,
                projection_version_after,safe_payload,occurred_at
             ) VALUES ($1,$2,$3,2,'projection.mutated',$4,$5,0,'{}'::jsonb,CURRENT_TIMESTAMP)",
        )
        .bind(run_id.as_str())
        .bind(seq)
        .bind(event_id)
        .bind(transition.as_str())
        .bind(intent.as_str())
        .execute(&control)
        .await
        .unwrap();
    }

    assert_eq!(
        sqlx::query(
            "UPDATE execution_events SET projection_ledger_batch=$1
             WHERE run_id=$2 AND event_id='event_pg_ledger_once'",
        )
        .bind(&canonical)
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    assert!(sqlx::query(
        "UPDATE execution_events SET projection_ledger_batch=$1
         WHERE run_id=$2 AND event_id='event_pg_ledger_once'",
    )
    .bind(&canonical)
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .is_err());
    let missing_field = json!({
        "schema_version": 1,
        "subject_count": 0,
        "manifest_hash": ContentHash::from_bytes(b"pg-missing-field").as_str(),
    });
    assert!(sqlx::query(
        "UPDATE execution_events SET projection_ledger_batch=$1
         WHERE run_id=$2 AND event_id='event_pg_ledger_extra'",
    )
    .bind(missing_field)
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .is_err());
    assert!(sqlx::query(
        "UPDATE execution_events SET projection_ledger_batch=NULL
         WHERE run_id=$1 AND event_id='event_pg_ledger_extra'",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .is_err());
    // JSONB canonicalizes duplicate lexical keys before the trigger sees the
    // value. A last-wins duplicate that changes a required value must still be
    // rejected by the closed stored-object contract.
    let duplicate_key = format!(
        "{{\"schema_version\":1,\"schema_version\":2,\"subject_count\":0,\"manifest_hash\":\"{}\",\"subjects\":[]}}",
        ContentHash::from_bytes(b"pg-duplicate-key").as_str()
    );
    assert!(sqlx::query(
        "UPDATE execution_events SET projection_ledger_batch=$1::jsonb
         WHERE run_id=$2 AND event_id='event_pg_ledger_extra'",
    )
    .bind(duplicate_key)
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .is_err());
    let mut extra = canonical.clone();
    extra["future_field"] = json!(true);
    assert!(sqlx::query(
        "UPDATE execution_events SET projection_ledger_batch=$1
         WHERE run_id=$2 AND event_id='event_pg_ledger_extra'",
    )
    .bind(extra)
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .is_err());
    assert!(sqlx::query(
        "UPDATE execution_events SET kind='forged'
         WHERE run_id=$1 AND event_id='event_pg_ledger_once'",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .is_err());

    // Simulate legacy/corrupt rows after bypassing the database rewrite guard.
    // Exact replay must decode the complete typed event before using it.
    let original_payload = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT safe_payload FROM execution_events WHERE run_id=$1 AND transition_key=$2",
    )
    .bind(run_id.as_str())
    .bind(create_key.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::raw_sql("DROP TRIGGER execution_event_projection_ledger_immutable ON execution_events;")
        .execute(&control)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE execution_events SET safe_payload='{\"type\":\"activation_ready\"}'::jsonb
         WHERE run_id=$1 AND transition_key=$2",
    )
    .bind(run_id.as_str())
    .bind(create_key.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert_eq!(
        repository
            .create_run(create_key.clone(), create.clone())
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_DATA_INVALID
    );
    sqlx::query(
        "UPDATE execution_events SET kind='activation.ready',
             safe_payload='{\"type\":\"activation_ready\"}'::jsonb
         WHERE run_id=$1 AND transition_key=$2",
    )
    .bind(run_id.as_str())
    .bind(create_key.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert_eq!(
        repository
            .create_run(create_key.clone(), create.clone())
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_DATA_INVALID
    );
    sqlx::query(
        "UPDATE execution_events SET kind='run.created',safe_payload=$1
         WHERE run_id=$2 AND transition_key=$3",
    )
    .bind(original_payload)
    .bind(run_id.as_str())
    .bind(create_key.as_str())
    .execute(&control)
    .await
    .unwrap();
    sqlx::raw_sql(
        "ALTER TABLE execution_events DROP CONSTRAINT execution_events_schema_version_supported;",
    )
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE execution_events SET schema_version=999 WHERE run_id=$1 AND transition_key=$2",
    )
    .bind(run_id.as_str())
    .bind(create_key.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert_eq!(
        repository
            .audit_projection(&run_id, &ProjectionSubject::run(&run_id))
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_DATA_INVALID
    );
    assert_eq!(
        repository
            .create_run(create_key, create)
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_DATA_INVALID
    );

    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}
