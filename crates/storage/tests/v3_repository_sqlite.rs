//! Public SQLite repository conformance at the durable-v3 admission boundary.
//!
//! These tests deliberately do not reconstruct the repository's private Run
//! transition commands. Scheduler/Activation tests own state-machine coverage;
//! this file proves only the public plan, Run admission, and projection APIs.

use insight_dsl::v3::{compile_source, CompileOptions};
use insight_durable::{
    ContinueAsNewCommand, CreateRunCommand, DurableRepository, PlanInstallOutcome,
    PlanPublicationOutcome, ProjectionAudit, ProjectionDurableRepository, ProjectionSubject,
    PublishVersionedPlanCommand, RecoveryDurableRepository, VersionedPlan,
};
use insight_engine::{
    plan::{
        AuthorFormat, DataBinding, DataBindingId, DataPort, DataPortId, Node, NodeKind,
        PlanBuilder, PlanInputContract, PlanMetadata, PlanType, PortDirection, PortName,
        ReturnDescriptor, ScopeId, ScopeMetadata, ValueSource, VersionTag,
    },
    repository::{
        REPOSITORY_DATA_INVALID, REPOSITORY_INTENT_CONFLICT, REPOSITORY_PLAN_CONFLICT,
        REPOSITORY_STORAGE_FAILURE,
    },
    AdmissionState, ContentHash, DefinitionRevisionId, DeploymentRevisionId, ExecutionEventId,
    RunId, RunLifecycle, TransitionKey, TransitionOutcome,
};
use insight_storage::SqliteDurableRepository;
use serde_json::{json, Value};
use sqlx::{sqlite::SqliteConnectOptions, Row, SqlitePool};

fn key(label: &str) -> TransitionKey {
    TransitionKey::derive("repository.test", &[label]).unwrap()
}

fn verified_plan() -> insight_engine::Plan {
    let revision = DefinitionRevisionId::new("definition_revision_v1").unwrap();
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

fn plan_with_binding(binding: Value) -> VersionedPlan {
    let canonical_plan = verified_plan();
    let worker_contracts = json!({"worker": "worker-v1"});
    let expected_binding_hash = ContentHash::from_bytes(
        &serde_jcs::to_vec(&json!({
            "schema_version": 1,
            "resolved_bindings": &binding,
            "worker_contracts": &worker_contracts,
        }))
        .unwrap(),
    );
    let versioned = VersionedPlan::from_verified_plan(
        "definition_checkout",
        "agent_checkout",
        "Checkout agent",
        DeploymentRevisionId::new("deployment_revision_v1").unwrap(),
        "expression-3.0.0",
        json!({"author": "structured", "version": 3}),
        &canonical_plan,
        json!({"return": "descriptor-v1"}),
        binding,
        worker_contracts,
    )
    .unwrap();
    assert_eq!(
        versioned.plan_hash().as_str(),
        canonical_plan.semantic_hash().as_str()
    );
    assert_eq!(versioned.binding_hash(), &expected_binding_hash);
    versioned
}

fn graph_plan(
    definition_id: &str,
    agent_id: &str,
    deployment_revision_id: &str,
    binding: Value,
) -> VersionedPlan {
    VersionedPlan::from_verified_plan(
        definition_id,
        agent_id,
        format!("Graph plan {agent_id}"),
        DeploymentRevisionId::new(deployment_revision_id).unwrap(),
        "expression-3.0.0",
        json!({"authoring_mode": "graph"}),
        &verified_plan(),
        json!({"return": "descriptor-v1"}),
        binding,
        json!([]),
    )
    .unwrap()
}

async fn file_repository() -> (tempfile::TempDir, SqliteDurableRepository, SqlitePool) {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("durable-v3.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    let inspection = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    (directory, repository, inspection)
}

async fn create_run(
    repository: &SqliteDurableRepository,
    plan: &VersionedPlan,
    label: &str,
) -> RunId {
    let run_id = RunId::new(label).unwrap();
    let outcome = repository
        .create_run(
            key(&format!("{label}.create")),
            CreateRunCommand::new(run_id.clone(), plan, json!({"question": "safe"})).unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(outcome, TransitionOutcome::Committed { .. }));
    run_id
}

fn normalized_input_plan() -> (insight_engine::Plan, VersionedPlan) {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs:
  question: string
  messages: {type: "Message[]", default: []}
  image_url: {type: string, optional: true}
output: string
workflow:
  steps:
    - return: fixed
"#;
    let plan = compile_source(
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("normalized_repository_input").unwrap(),
            "normalized-repository-input.yaml",
            source,
        ),
    )
    .unwrap();
    let versioned = VersionedPlan::from_verified_plan(
        "definition_normalized_input",
        "agent_normalized_input",
        "Normalized input",
        DeploymentRevisionId::new("deployment_normalized_input").unwrap(),
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

#[tokio::test]
async fn sqlite_continue_as_new_rejects_unnormalized_target_input_before_commit() {
    let (_directory, repository, inspection) = file_repository().await;
    let (plan, versioned) = normalized_input_plan();
    repository.install_versioned_plan(&versioned).await.unwrap();

    let source = RunId::new("run_continue_normalized_source").unwrap();
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
         WHERE run_id=?",
    )
    .bind(source.as_str())
    .execute(&inspection)
    .await
    .unwrap();

    let rejected_target = RunId::new("run_continue_missing_frozen_default").unwrap();
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
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM workflow_runs WHERE run_id=?")
            .bind(rejected_target.as_str())
            .fetch_one(&inspection)
            .await
            .unwrap(),
        0
    );

    let accepted_target = RunId::new("run_continue_normalized_target").unwrap();
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
    let stored = sqlx::query_scalar::<_, String>(
        "SELECT p.inline_value
         FROM workflow_runs r JOIN payloads p
           ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id
         WHERE r.run_id=?",
    )
    .bind(accepted_target.as_str())
    .fetch_one(&inspection)
    .await
    .unwrap();
    let stored: Value = serde_json::from_str(&stored).unwrap();
    assert_eq!(stored["messages"], json!([]));
}

fn empty_projection_ledger(extra: bool) -> Value {
    let mut value = json!({
        "schema_version": 1,
        "subject_count": 0,
        "manifest_hash": ContentHash::from_bytes(b"empty-projection-manifest").as_str(),
        "subjects": [],
    });
    if extra {
        value["future_field"] = json!(true);
    }
    value
}

#[tokio::test]
async fn execution_event_rows_are_closed_immutable_authorities_and_unknown_schema_fails_closed() {
    let (_directory, repository, inspection) = file_repository().await;
    let plan = plan_with_binding(json!({"model": "model-fixed"}));
    repository.install_versioned_plan(&plan).await.unwrap();
    let run_id = RunId::new("run_event_authority_sqlite").unwrap();
    let create_key = key("event.authority.sqlite.create");
    let create = CreateRunCommand::new(run_id.clone(), &plan, json!({"input": 1})).unwrap();
    assert!(matches!(
        repository
            .create_run(create_key.clone(), create.clone())
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));

    let unsupported_insert = sqlx::query(
        "INSERT INTO execution_events (
            run_id,seq,event_id,schema_version,kind,transition_key,intent_hash,
            projection_version_after,safe_payload,occurred_at
         ) VALUES (?,2,'event_unknown_schema',999,'projection.mutated',?,?,0,'{}',CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(key("event.authority.sqlite.unknown").as_str())
    .bind(ContentHash::from_bytes(b"unknown-schema").as_str())
    .execute(&inspection)
    .await;
    assert!(unsupported_insert.is_err());

    assert!(sqlx::query(
        "INSERT INTO execution_events (
            run_id,seq,event_id,schema_version,kind,transition_key,intent_hash,
            projection_version_after,safe_payload,occurred_at
         ) VALUES (?,2,'event_unknown_kind',2,'future.kind',?,?,0,'{}',CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(key("event.authority.sqlite.unknown-kind").as_str())
    .bind(ContentHash::from_bytes(b"unknown-kind").as_str())
    .execute(&inspection)
    .await
    .is_err());

    let canonical = serde_jcs::to_string(&empty_projection_ledger(false)).unwrap();
    assert!(sqlx::query(
        "INSERT INTO execution_events (
            run_id,seq,event_id,schema_version,kind,transition_key,intent_hash,
            projection_version_after,safe_payload,occurred_at,projection_ledger_batch
         ) VALUES (?,2,'event_prefilled_ledger',2,'projection.mutated',?,?,0,
                   '{\"type\":\"projection_mutated\",\"mutation\":\"signal_received\"}',
                   CURRENT_TIMESTAMP,?)",
    )
    .bind(run_id.as_str())
    .bind(key("event.authority.sqlite.prefilled").as_str())
    .bind(ContentHash::from_bytes(b"prefilled-ledger").as_str())
    .bind(&canonical)
    .execute(&inspection)
    .await
    .is_err());

    for (seq, event_id, transition, intent) in [
        (
            2_i64,
            "event_ledger_once",
            key("event.authority.sqlite.once"),
            ContentHash::from_bytes(b"ledger-once"),
        ),
        (
            3_i64,
            "event_ledger_extra",
            key("event.authority.sqlite.extra"),
            ContentHash::from_bytes(b"ledger-extra"),
        ),
    ] {
        sqlx::query(
            "INSERT INTO execution_events (
                run_id,seq,event_id,schema_version,kind,transition_key,intent_hash,
                projection_version_after,safe_payload,occurred_at
             ) VALUES (?,?,?,2,'projection.mutated',?,?,0,'{}',CURRENT_TIMESTAMP)",
        )
        .bind(run_id.as_str())
        .bind(seq)
        .bind(event_id)
        .bind(transition.as_str())
        .bind(intent.as_str())
        .execute(&inspection)
        .await
        .unwrap();
    }

    assert_eq!(
        sqlx::query(
            "UPDATE execution_events SET projection_ledger_batch=?
             WHERE run_id=? AND event_id='event_ledger_once'",
        )
        .bind(&canonical)
        .bind(run_id.as_str())
        .execute(&inspection)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    assert!(sqlx::query(
        "UPDATE execution_events SET projection_ledger_batch=?
         WHERE run_id=? AND event_id='event_ledger_once'",
    )
    .bind(&canonical)
    .bind(run_id.as_str())
    .execute(&inspection)
    .await
    .is_err());
    let missing_field = json!({
        "schema_version": 1,
        "subject_count": 0,
        "manifest_hash": ContentHash::from_bytes(b"missing-field").as_str(),
    });
    assert!(sqlx::query(
        "UPDATE execution_events SET projection_ledger_batch=?
         WHERE run_id=? AND event_id='event_ledger_extra'",
    )
    .bind(serde_jcs::to_string(&missing_field).unwrap())
    .bind(run_id.as_str())
    .execute(&inspection)
    .await
    .is_err());
    let duplicate_key = format!(
        "{{\"manifest_hash\":\"{}\",\"schema_version\":1,\"subject_count\":0,\"subject_count\":0,\"subjects\":[]}}",
        ContentHash::from_bytes(b"duplicate-key").as_str()
    );
    assert!(sqlx::query(
        "UPDATE execution_events SET projection_ledger_batch=?
         WHERE run_id=? AND event_id='event_ledger_extra'",
    )
    .bind(duplicate_key)
    .bind(run_id.as_str())
    .execute(&inspection)
    .await
    .is_err());
    assert!(sqlx::query(
        "UPDATE execution_events SET projection_ledger_batch=NULL
         WHERE run_id=? AND event_id='event_ledger_extra'",
    )
    .bind(run_id.as_str())
    .execute(&inspection)
    .await
    .is_err());
    assert!(sqlx::query(
        "UPDATE execution_events SET projection_ledger_batch=?
         WHERE run_id=? AND event_id='event_ledger_extra'",
    )
    .bind(serde_jcs::to_string(&empty_projection_ledger(true)).unwrap())
    .bind(run_id.as_str())
    .execute(&inspection)
    .await
    .is_err());
    assert!(sqlx::query(
        "UPDATE execution_events SET kind='forged' WHERE run_id=? AND event_id='event_ledger_once'",
    )
    .bind(run_id.as_str())
    .execute(&inspection)
    .await
    .is_err());

    // Simulate a legacy/corrupt snapshot that bypassed database guards. The
    // shared persisted-row decoder must reject it before exact replay uses it.
    let original_payload = sqlx::query_scalar::<_, String>(
        "SELECT safe_payload FROM execution_events WHERE run_id=? AND transition_key=?",
    )
    .bind(run_id.as_str())
    .bind(create_key.as_str())
    .fetch_one(&inspection)
    .await
    .unwrap();
    sqlx::raw_sql("DROP TRIGGER execution_event_projection_ledger_immutable;")
        .execute(&inspection)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE execution_events SET safe_payload='{\"type\":\"activation_ready\"}'
         WHERE run_id=? AND transition_key=?",
    )
    .bind(run_id.as_str())
    .bind(create_key.as_str())
    .execute(&inspection)
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
             safe_payload='{\"type\":\"activation_ready\"}'
         WHERE run_id=? AND transition_key=?",
    )
    .bind(run_id.as_str())
    .bind(create_key.as_str())
    .execute(&inspection)
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
        "UPDATE execution_events SET kind='run.created',safe_payload=?
         WHERE run_id=? AND transition_key=?",
    )
    .bind(original_payload)
    .bind(run_id.as_str())
    .bind(create_key.as_str())
    .execute(&inspection)
    .await
    .unwrap();
    sqlx::raw_sql("DROP TRIGGER execution_event_schema_version_update_supported;")
        .execute(&inspection)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE execution_events SET schema_version=999 WHERE run_id=? AND transition_key=?",
    )
    .bind(run_id.as_str())
    .bind(create_key.as_str())
    .execute(&inspection)
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
}

#[tokio::test]
async fn public_plan_install_and_run_admission_are_idempotent_and_revision_pinned() {
    let (_directory, repository, inspection) = file_repository().await;
    repository.check_health().await.unwrap();

    let plan = plan_with_binding(json!({"model": "model-fixed"}));
    assert_eq!(
        repository.install_versioned_plan(&plan).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    assert_eq!(
        repository.install_versioned_plan(&plan).await.unwrap(),
        PlanInstallOutcome::AlreadyInstalled
    );

    let run_id = RunId::new("run_repository_create").unwrap();
    let transition_key = key("create.idempotent");
    let command = CreateRunCommand::new(run_id.clone(), &plan, json!({"b": 2, "a": 1})).unwrap();
    let first = repository
        .create_run(transition_key.clone(), command.clone())
        .await
        .unwrap();
    let receipt = first.committed_result().cloned().unwrap();
    assert_eq!(receipt.event_seq(), 1);
    assert_eq!(receipt.projection_version(), 0);
    ExecutionEventId::parse(receipt.event_id()).unwrap();
    assert_eq!(
        repository
            .create_run(transition_key.clone(), command)
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay {
            authoritative: receipt
        }
    );

    let different_intent = CreateRunCommand::new(run_id.clone(), &plan, json!({"a": 9})).unwrap();
    assert_eq!(
        repository
            .create_run(transition_key, different_intent)
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_INTENT_CONFLICT
    );

    let projection = repository.load_run(&run_id).await.unwrap().unwrap();
    assert_eq!(projection.lifecycle(), RunLifecycle::Created);
    assert_eq!(projection.admission(), AdmissionState::Open);
    assert_eq!(
        projection.definition_revision_id(),
        plan.definition_revision_id()
    );
    assert_eq!(
        projection.deployment_revision_id(),
        plan.deployment_revision_id()
    );

    let row = sqlx::query(
        "SELECT r.plan_hash, r.binding_hash, p.inline_value
         FROM workflow_runs r
         JOIN payloads p ON p.run_id = r.run_id AND p.payload_id = r.input_payload_id
         WHERE r.run_id = ?",
    )
    .bind(run_id.as_str())
    .fetch_one(&inspection)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("plan_hash"), plan.plan_hash().as_str());
    assert_eq!(
        row.get::<String, _>("binding_hash"),
        plan.binding_hash().as_str()
    );
    assert_eq!(
        serde_json::from_str::<Value>(&row.get::<String, _>("inline_value")).unwrap(),
        json!({"a": 1, "b": 2})
    );
}

#[tokio::test]
async fn cross_runtime_stale_subflow_publication_does_not_install_parent() {
    let (directory, repository_a, _inspection) = file_repository().await;
    let database = directory.path().join("durable-v3.sqlite");
    let repository_b = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();

    let child_v1 = graph_plan(
        "definition_sqlite_publication_child",
        "agent_sqlite_publication_child",
        "deployment_sqlite_publication_child_v1",
        json!([]),
    );
    assert_eq!(
        repository_a
            .publish_versioned_plan(
                PublishVersionedPlanCommand::new(child_v1.clone(), vec![]).unwrap()
            )
            .await
            .unwrap(),
        PlanPublicationOutcome::Published(PlanInstallOutcome::Installed)
    );
    let snapshot = repository_a.load_versioned_plan_catalog().await.unwrap();
    let child_v1_head = snapshot
        .heads()
        .iter()
        .find(|head| head.agent_id() == child_v1.agent_id())
        .unwrap()
        .clone();

    let parent = graph_plan(
        "definition_sqlite_publication_parent",
        "agent_sqlite_publication_parent",
        "deployment_sqlite_publication_parent_v1",
        json!([{
            "node_id": "child",
            "binding": {
                "adapter": "durable_subflow",
                "definition_revision_id": child_v1.definition_revision_id(),
                "deployment_revision_id": child_v1.deployment_revision_id(),
                "plan_hash": child_v1.plan_hash(),
                "binding_hash": child_v1.binding_hash(),
                "interface_version": "sqlite-publication-v1"
            }
        }]),
    );
    let stale_parent = PublishVersionedPlanCommand::new(parent.clone(), vec![child_v1_head])
        .expect("snapshot head covers the parent's exact durable subflow pin");

    // A separate repository instance advances the same child route. The
    // process-local writer mutexes are intentionally unrelated.
    let child_v2 = graph_plan(
        "definition_sqlite_publication_child",
        "agent_sqlite_publication_child",
        "deployment_sqlite_publication_child_v2",
        json!([{"binding_revision": 2}]),
    );
    assert!(matches!(
        repository_b
            .publish_versioned_plan(
                PublishVersionedPlanCommand::new(child_v2.clone(), vec![]).unwrap()
            )
            .await
            .unwrap(),
        PlanPublicationOutcome::Published(_)
    ));

    assert_eq!(
        repository_a
            .publish_versioned_plan(stale_parent)
            .await
            .unwrap(),
        PlanPublicationOutcome::DependencyHeadChanged
    );
    let final_catalog = repository_a.load_versioned_plan_catalog().await.unwrap();
    assert!(final_catalog
        .heads()
        .iter()
        .all(|head| head.agent_id() != parent.agent_id()));
    assert!(final_catalog
        .plans()
        .iter()
        .all(|plan| plan.deployment_revision_id() != parent.deployment_revision_id()));
    assert_eq!(
        final_catalog
            .heads()
            .iter()
            .find(|head| head.agent_id() == child_v2.agent_id())
            .unwrap()
            .deployment_revision_id(),
        child_v2.deployment_revision_id()
    );
}

#[tokio::test]
async fn public_projection_api_audits_repairs_and_fails_closed_on_corrupt_authority() {
    let (_directory, repository, inspection) = file_repository().await;
    let plan = plan_with_binding(json!({"model": "model-fixed"}));
    repository.install_versioned_plan(&plan).await.unwrap();
    let run_id = create_run(&repository, &plan, "run_projection_repair").await;
    let subject = ProjectionSubject::run(&run_id);

    assert!(matches!(
        repository
            .audit_projection(&run_id, &subject)
            .await
            .unwrap(),
        ProjectionAudit::Match { .. }
    ));

    sqlx::query("UPDATE workflow_runs SET lifecycle='active' WHERE run_id=?")
        .bind(run_id.as_str())
        .execute(&inspection)
        .await
        .unwrap();
    assert!(matches!(
        repository
            .audit_projection(&run_id, &subject)
            .await
            .unwrap(),
        ProjectionAudit::Mismatch { .. }
    ));

    sqlx::raw_sql(
        "CREATE TRIGGER block_projection_repair
         BEFORE UPDATE OF lifecycle ON workflow_runs
         FOR EACH ROW WHEN OLD.lifecycle='active' AND NEW.lifecycle='created'
         BEGIN SELECT RAISE(ABORT, 'repair blocked'); END;",
    )
    .execute(&inspection)
    .await
    .unwrap();
    assert_eq!(
        repository
            .repair_projection(&run_id, &subject)
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_STORAGE_FAILURE
    );
    sqlx::raw_sql("DROP TRIGGER block_projection_repair")
        .execute(&inspection)
        .await
        .unwrap();

    assert!(repository
        .repair_projection(&run_id, &subject)
        .await
        .unwrap()
        .repaired());
    assert!(repository
        .audit_projection(&run_id, &subject)
        .await
        .unwrap()
        .is_match());
    assert!(!repository
        .repair_projection(&run_id, &subject)
        .await
        .unwrap()
        .repaired());

    sqlx::query(
        "UPDATE projection_checkpoints SET checkpoint_schema_version=999
         WHERE run_id=? AND subject_kind='run'",
    )
    .bind(run_id.as_str())
    .execute(&inspection)
    .await
    .unwrap();
    assert!(matches!(
        repository
            .create_run(
                key("run_projection_repair.create"),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"question": "safe"})).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));

    // The immutable event ledger is the rebuild/replay authority. Corrupting
    // that authority (after deliberately removing its database guard) must
    // fail closed even though checkpoint materialization is merely a cache.
    sqlx::raw_sql("DROP TRIGGER execution_event_projection_ledger_immutable")
        .execute(&inspection)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE execution_events
         SET projection_ledger_batch=json_set(projection_ledger_batch,'$.manifest_hash',?)
         WHERE run_id=? AND transition_key=?",
    )
    .bind(ContentHash::from_bytes(b"corrupt-projection-ledger").as_str())
    .bind(run_id.as_str())
    .bind(key("run_projection_repair.create").as_str())
    .execute(&inspection)
    .await
    .unwrap();
    assert_eq!(
        repository
            .create_run(
                key("run_projection_repair.create"),
                CreateRunCommand::new(run_id, &plan, json!({"question": "safe"})).unwrap(),
            )
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_DATA_INVALID
    );
}

#[tokio::test]
async fn exact_admission_replay_uses_embedded_ledger_when_materialization_is_missing() {
    let (_directory, repository, inspection) = file_repository().await;
    let plan = plan_with_binding(json!({"model": "model-fixed"}));
    repository.install_versioned_plan(&plan).await.unwrap();
    let run_id = create_run(&repository, &plan, "run_projection_missing_manifest").await;

    sqlx::raw_sql("PRAGMA foreign_keys=OFF")
        .execute(&inspection)
        .await
        .unwrap();
    sqlx::query("DELETE FROM projection_checkpoints WHERE run_id=?")
        .bind(run_id.as_str())
        .execute(&inspection)
        .await
        .unwrap();
    sqlx::query("DELETE FROM projection_checkpoint_batches WHERE run_id=?")
        .bind(run_id.as_str())
        .execute(&inspection)
        .await
        .unwrap();
    sqlx::raw_sql("PRAGMA foreign_keys=ON")
        .execute(&inspection)
        .await
        .unwrap();

    assert!(repository
        .audit_projection(&run_id, &ProjectionSubject::run(&run_id))
        .await
        .unwrap()
        .is_match());
    assert!(matches!(
        repository
            .create_run(
                key("run_projection_missing_manifest.create"),
                CreateRunCommand::new(run_id, &plan, json!({"question": "safe"})).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
}

#[tokio::test]
async fn public_repository_errors_are_stable_and_body_free() {
    let repository = SqliteDurableRepository::in_memory().await.unwrap();
    let plan = plan_with_binding(json!({"model": "model-fixed"}));
    repository.install_versioned_plan(&plan).await.unwrap();

    let secret = "do-not-leak-this-secret";
    let conflicting = plan_with_binding(json!({"model": secret}));
    let error = repository
        .install_versioned_plan(&conflicting)
        .await
        .unwrap_err();
    assert_eq!(error.code(), REPOSITORY_PLAN_CONFLICT);
    assert!(!error.to_string().contains(secret));
    assert!(!format!("{error:?}").contains(secret));
}
