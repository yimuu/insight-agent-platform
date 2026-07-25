use std::{collections::BTreeSet, sync::Arc, time::Duration};

use insight_agent_platform::{
    catalog::{
        compile_agent_dir, compile_enabled_agents, deploy_agents, LeafDeploymentResolver,
        ResolvedLeafDeployment,
    },
    dsl::CompileError,
    engine::{
        plan::{LeafTaskDescriptor, LeafTaskKind},
        repository::SqliteDurableRepository,
        VersionTag, WorkerExecutorRegistry,
    },
    history::types::RunStatus,
    runtime::{
        DeployedAgentCatalog, ProductionRunRepository, RequestMetadata, RunService,
        RunServiceConfig,
    },
};
use serde_json::json;
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, Row, SqlitePool};

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

fn write_agents(root: &std::path::Path) {
    let child = root.join("child");
    std::fs::create_dir_all(&child).unwrap();
    std::fs::write(
        child.join("agent.yaml"),
        r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: child
  name: Child
  description: Durable child used by the production subflow gate.
inputs: {question: string}
output: string
workflow:
  steps:
    - return: $question
"#,
    )
    .unwrap();
    let revision = compile_agent_dir(&child)
        .unwrap()
        .definition_revision_id()
        .clone();

    let parent = root.join("parent");
    std::fs::create_dir_all(&parent).unwrap();
    std::fs::write(
        parent.join("agent.yaml"),
        format!(
            r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: parent
  name: Parent
  description: Production parent with an immutable child pin.
inputs: {{question: string}}
output: string
workflow:
  steps:
    - id: child_answer
      type: call
      definition_revision: {revision}
      interface_version: child-v1
      input: {{question: $question}}
      response: string
    - return: $child_answer
"#,
        ),
    )
    .unwrap();
}

async fn wait_for_success(service: &RunService, run_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let record = service.get_run(run_id).await.unwrap();
        if record.status().is_terminal() {
            assert_eq!(record.status(), RunStatus::Completed);
            return;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn production_catalog_discovers_pins_and_recovers_parent_child_subflow() {
    let temporary = tempfile::tempdir().unwrap();
    let agents_root = temporary.path().join("agents");
    std::fs::create_dir_all(&agents_root).unwrap();
    write_agents(&agents_root);
    let enabled = BTreeSet::from(["child".to_owned(), "parent".to_owned()]);
    let published = compile_enabled_agents(&agents_root, &enabled).unwrap();
    let deployed = deploy_agents(&published, &NoLeafResolver).unwrap();
    let agents = DeployedAgentCatalog::new(deployed).unwrap();

    let database = temporary.path().join("subflow.sqlite");
    database::provision_sqlite_database(&database).await;
    let repository = Arc::new(
        SqliteDurableRepository::connect_path(&database)
            .await
            .unwrap(),
    );
    let service = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        RunServiceConfig::single_process_development(8, 4, 2, 32),
    )
    .await
    .unwrap();
    let parent = service
        .create_detached(
            "parent",
            json!({"question": "durable child result"}),
            RequestMetadata {
                request_id: Some("production-subflow-1".to_owned()),
            },
        )
        .await
        .unwrap();
    wait_for_success(&service, &parent.run_id).await;

    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .create_if_missing(false)
            .disable_statement_logging(),
    )
    .await
    .unwrap();
    let child = sqlx::query(
        "SELECT run_id,lifecycle FROM workflow_runs WHERE parent_run_id=? AND lineage_kind='subflow'",
    )
    .bind(&parent.run_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(child.get::<String, _>("lifecycle"), "succeeded");
    let invocation =
        sqlx::query("SELECT invocation_state FROM scheduler_subflow_invocations WHERE run_id=?")
            .bind(&parent.run_id)
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(invocation.get::<String, _>("invocation_state"), "completed");
    let open_scopes: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM scope_instances WHERE run_id IN (?,?) AND (lifecycle<>'settled' OR admission_state<>'closed' OR admitted_children<>settled_children)",
    )
    .bind(&parent.run_id)
    .bind(child.get::<String, _>("run_id"))
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(open_scopes, 0);

    service.shutdown(Duration::from_secs(1)).await.unwrap();
}
#[path = "support/database.rs"]
mod database;
