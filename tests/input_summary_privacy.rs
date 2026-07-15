use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use handlebars::Handlebars;
use insight_agent_platform::{
    dsl::{
        compiled::{
            CompiledAgent, CompiledNode, ExecutionPlan, NodeCompilation, NodeControl, NodeOutcome,
            NodeTransition,
        },
        compiler::CompileContext,
        CompileError, EmitPolicy,
    },
    events::hub::{EventHub, EventHubConfig},
    history::{
        postgres::PostgresRunRepository,
        repository::RunRepository,
        sqlite::SqliteRunRepository,
        types::{RunRecord, RunStatus},
    },
    nodes::registry::{NodeExecutor, NodeExecutorRegistry, NodeType},
    outcome::{RunOutput, TerminalOutcome},
    runtime::{
        CompiledAgentRegistry, ExecutionControl, RequestMetadata, RunContext, RunError, RunService,
        RunServiceConfig,
    },
    schema::compile_schema,
};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, types::Json, AssertSqlSafe, PgPool, SqlitePool};
use tempfile::tempdir;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

const PRIVACY_AGENT_ID: &str = "input-summary-privacy";
const PLAIN_SECRET: &str = "plain-input-secret-18f4c792";
const NESTED_SECRET: &str = "nested-input-secret-29a6d8e3";
const ESCAPED_SECRET: &str = "escaped-input-secret-\"quote\\slash\nline-3bc7e1f5";
const TEST_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
const POSTGRES_PROBE_TIMEOUT: Duration = Duration::from_millis(500);
const SERVICE_SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

struct FixedOutputNode;

impl NodeType for FixedOutputNode {
    fn kind(&self) -> &'static str {
        "test.input_summary_privacy"
    }

    fn compile(
        &self,
        _node_id: &str,
        _config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        Err(CompileError::new(
            "TEST_ONLY",
            "input-summary privacy test nodes are constructed directly",
        ))
    }
}

#[async_trait]
impl NodeExecutor for FixedOutputNode {
    async fn execute(
        &self,
        _node: &CompiledNode,
        _context: &RunContext,
        _control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        Ok(NodeOutcome {
            output: json!({"fixed": true}),
            transition: NodeTransition::End(TerminalOutcome::Success {
                output: RunOutput {
                    content: Some("fixed output".to_string()),
                    format: Some("text".to_string()),
                    data: json!({"fixed": true}),
                },
            }),
        })
    }
}

fn privacy_input() -> Value {
    json!({
        "plain": PLAIN_SECRET,
        "nested": {
            "token": NESTED_SECRET,
        },
        "escaped": ESCAPED_SECRET,
        "enabled": true,
        "count": 7,
        "items": [1, 2, 3],
        "nothing": null,
    })
}

fn expected_input_summary(input: &Value) -> Value {
    json!({
        "keys": ["count", "enabled", "escaped", "items", "nested", "nothing", "plain"],
        "serialized_bytes": serde_json::to_vec(input).unwrap().len(),
    })
}

fn assert_exact_summary(actual: &Value, expected: &Value) {
    assert_eq!(actual, expected);
    let object = actual
        .as_object()
        .expect("input summary must be a JSON object");
    assert_eq!(
        object.len(),
        2,
        "input summary must have exactly two fields"
    );
    assert!(object.contains_key("keys"));
    assert!(object.contains_key("serialized_bytes"));
}

fn assert_raw_summary_contains_no_secrets(raw: &str) {
    for sentinel in [PLAIN_SECRET, NESTED_SECRET, ESCAPED_SECRET] {
        assert!(
            !raw.contains(sentinel),
            "raw input summary contains a secret sentinel"
        );
    }

    let escaped = serde_json::to_string(ESCAPED_SECRET).unwrap();
    let escaped = escaped
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap();
    assert!(
        !raw.contains(escaped),
        "raw input summary contains a JSON-escaped secret sentinel"
    );
}

fn privacy_agent() -> Arc<CompiledAgent> {
    let node = CompiledNode {
        id: "fixed".to_string(),
        kind: "test.input_summary_privacy".to_string(),
        next: None,
        emit: EmitPolicy::None,
        timeout: TEST_OPERATION_TIMEOUT,
        body: Arc::new(()),
        edges: Vec::new(),
        references: BTreeSet::new(),
        control: NodeControl::Ordinary,
    };
    let nodes = BTreeMap::from([(node.id.clone(), node)]);

    Arc::new(CompiledAgent {
        id: PRIVACY_AGENT_ID.to_string(),
        name: "Input summary privacy test agent".to_string(),
        description: "Produces a fixed result without reading its input".to_string(),
        version_hash: "sha256:input-summary-privacy".to_string(),
        input_schema: Arc::new(
            compile_schema(&json!({
                "type": "object",
                "required": [
                    "plain", "nested", "escaped", "enabled", "count", "items", "nothing"
                ],
                "additionalProperties": false,
                "properties": {
                    "plain": {"type": "string"},
                    "nested": {
                        "type": "object",
                        "required": ["token"],
                        "additionalProperties": false,
                        "properties": {
                            "token": {"type": "string"},
                        },
                    },
                    "escaped": {"type": "string"},
                    "enabled": {"type": "boolean"},
                    "count": {"type": "integer"},
                    "items": {
                        "type": "array",
                        "items": {"type": "integer"},
                    },
                    "nothing": {"type": "null"},
                },
            }))
            .unwrap(),
        ),
        entry: "fixed".to_string(),
        execution_plan: ExecutionPlan::sequential("fixed", nodes.keys().cloned()),
        nodes,
        templates: Arc::new(Handlebars::new()),
    })
}

fn privacy_service(repository: Arc<dyn RunRepository>) -> RunService {
    let events = EventHub::new(
        Arc::clone(&repository),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 16,
            journal_batch_size: 4,
            operation_timeout: TEST_OPERATION_TIMEOUT,
        },
    );
    let agents = CompiledAgentRegistry::new(vec![privacy_agent()]).unwrap();
    let mut executors = NodeExecutorRegistry::default();
    executors.register(FixedOutputNode).unwrap();

    RunService::new(
        agents,
        executors,
        repository,
        events,
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 1,
            max_parallel_branches_per_run: 1,
            run_timeout: TEST_OPERATION_TIMEOUT,
        },
    )
    .unwrap()
}

async fn wait_for_completed(service: &RunService, run_id: &str) -> RunRecord {
    timeout(TEST_OPERATION_TIMEOUT, async {
        loop {
            let record = service.get_run(run_id).await.unwrap();
            if record.status() == RunStatus::Completed {
                return record;
            }
            assert!(
                !record.status().is_terminal(),
                "privacy test run reached unexpected terminal status {:?}",
                record.status()
            );
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("privacy test run did not complete before the deadline")
}

async fn shutdown_service(service: &RunService) {
    timeout(
        TEST_OPERATION_TIMEOUT,
        service.shutdown(SERVICE_SHUTDOWN_GRACE),
    )
    .await
    .expect("privacy test service shutdown exceeded the outer deadline")
    .unwrap();
}

fn postgres_database_url() -> Option<String> {
    let database_url = std::env::var("RUN_HISTORY_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    if std::env::var_os("CI").is_some() && database_url.is_none() {
        panic!("RUN_HISTORY_POSTGRES_URL is required in CI");
    }
    if database_url.is_none() {
        eprintln!(
            "skipping postgres input-summary privacy test: RUN_HISTORY_POSTGRES_URL is not set"
        );
    }
    database_url
}

struct PostgresPrivacySchema {
    admin: PgPool,
    database_url: String,
    schema: String,
}

impl PostgresPrivacySchema {
    async fn create(database_url: &str) -> Self {
        let schema = format!("input_summary_privacy_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(database_url)
            .await
            .unwrap();
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        Self {
            admin,
            database_url: database_url.to_string(),
            schema,
        }
    }

    fn url(&self, application_name: &str) -> String {
        let separator = if self.database_url.contains('?') {
            '&'
        } else {
            '?'
        };
        format!(
            "{}{separator}options=-csearch_path%3D{}&application_name={application_name}",
            self.database_url, self.schema
        )
    }

    async fn scoped_read_only_pool(&self, application_name: &str) -> PgPool {
        PgPoolOptions::new()
            .max_connections(1)
            .after_connect(|connection, _metadata| {
                Box::pin(async move {
                    sqlx::query("SET default_transaction_read_only = on")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(&self.url(application_name))
            .await
            .unwrap()
    }

    async fn cleanup(self) {
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await
        .unwrap();
        self.admin.close().await;
    }
}

#[tokio::test]
async fn sqlite_run_service_persists_only_shape_metadata_in_raw_row() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("input-summary-privacy.db");
    let database_url = format!("sqlite://{}?mode=rwc", path.display());
    let repository = Arc::new(SqliteRunRepository::connect(&database_url).await.unwrap());
    let repository_trait: Arc<dyn RunRepository> = repository.clone();
    let service = privacy_service(repository_trait);
    let input = privacy_input();
    let expected = expected_input_summary(&input);

    let created = timeout(
        TEST_OPERATION_TIMEOUT,
        service.create_detached(PRIVACY_AGENT_ID, input, RequestMetadata::default()),
    )
    .await
    .expect("privacy test run creation exceeded the deadline")
    .unwrap();
    assert_exact_summary(&created.input_summary, &expected);

    let completed = wait_for_completed(&service, &created.run_id).await;
    assert_exact_summary(&completed.input_summary, &expected);

    let reconstructed = timeout(TEST_OPERATION_TIMEOUT, repository.get_run(&created.run_id))
        .await
        .expect("privacy test run reconstruction exceeded the deadline")
        .unwrap()
        .expect("privacy test run disappeared");
    assert_exact_summary(&reconstructed.input_summary, &expected);

    let inspector = SqlitePool::connect(&database_url).await.unwrap();
    let (raw_summary, storage_type, native_type): (String, String, String) = timeout(
        TEST_OPERATION_TIMEOUT,
        sqlx::query_as(
            "SELECT input_summary, typeof(input_summary), json_type(input_summary)
             FROM runs WHERE run_id = ?",
        )
        .bind(&created.run_id)
        .fetch_one(&inspector),
    )
    .await
    .expect("raw SQLite input-summary query exceeded the deadline")
    .unwrap();
    assert_eq!(storage_type, "text");
    assert_eq!(native_type, "object");
    let decoded: Value = serde_json::from_str(&raw_summary).unwrap();
    assert_exact_summary(&decoded, &expected);
    assert_raw_summary_contains_no_secrets(&raw_summary);

    shutdown_service(&service).await;
    inspector.close().await;
    drop(service);
    drop(repository);
}

#[tokio::test]
async fn postgres_run_service_persists_only_shape_metadata_in_raw_jsonb_row() {
    let Some(database_url) = postgres_database_url() else {
        return;
    };

    let schema = PostgresPrivacySchema::create(&database_url).await;
    let application_name = format!("input_summary_privacy_{}", Uuid::new_v4().simple());
    let (repository, owner) = PostgresRunRepository::connect_owned(
        &schema.url(&application_name),
        TEST_OPERATION_TIMEOUT,
        POSTGRES_PROBE_TIMEOUT,
    )
    .await
    .unwrap();
    let repository = Arc::new(repository);
    let repository_trait: Arc<dyn RunRepository> = repository.clone();
    let service = privacy_service(repository_trait);
    let input = privacy_input();
    let expected = expected_input_summary(&input);

    let created = timeout(
        TEST_OPERATION_TIMEOUT,
        service.create_detached(PRIVACY_AGENT_ID, input, RequestMetadata::default()),
    )
    .await
    .expect("PostgreSQL privacy test run creation exceeded the deadline")
    .unwrap();
    assert_exact_summary(&created.input_summary, &expected);

    let completed = wait_for_completed(&service, &created.run_id).await;
    assert_exact_summary(&completed.input_summary, &expected);

    let reconstructed = timeout(TEST_OPERATION_TIMEOUT, repository.get_run(&created.run_id))
        .await
        .expect("PostgreSQL privacy test run reconstruction exceeded the deadline")
        .unwrap()
        .expect("PostgreSQL privacy test run disappeared");
    assert_exact_summary(&reconstructed.input_summary, &expected);
    assert_eq!(created.input_summary, completed.input_summary);
    assert_eq!(completed.input_summary, reconstructed.input_summary);

    let inspector = schema
        .scoped_read_only_pool("input_summary_raw_reader")
        .await;
    let read_only: String = sqlx::query_scalar("SELECT current_setting('transaction_read_only')")
        .fetch_one(&inspector)
        .await
        .unwrap();
    assert_eq!(read_only, "on");
    let (stored, raw_summary, native_type): (Json<Value>, String, String) = timeout(
        TEST_OPERATION_TIMEOUT,
        sqlx::query_as(
            "SELECT input_summary, input_summary::text, jsonb_typeof(input_summary)
             FROM runs WHERE run_id = $1",
        )
        .bind(&created.run_id)
        .fetch_one(&inspector),
    )
    .await
    .expect("raw PostgreSQL input-summary query exceeded the deadline")
    .unwrap();
    assert_eq!(native_type, "object");
    assert_exact_summary(&stored.0, &expected);
    let decoded: Value = serde_json::from_str(&raw_summary).unwrap();
    assert_exact_summary(&decoded, &expected);
    assert_raw_summary_contains_no_secrets(&raw_summary);

    shutdown_service(&service).await;
    inspector.close().await;
    drop(service);
    drop(repository);
    timeout(TEST_OPERATION_TIMEOUT, owner.release())
        .await
        .expect("PostgreSQL owner release exceeded the deadline")
        .unwrap();
    schema.cleanup().await;
}
