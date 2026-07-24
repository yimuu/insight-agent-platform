use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use insight_agent_platform::{
    catalog::{DeployedAgent, LeafDeploymentResolver, PublishedAgent, ResolvedLeafDeployment},
    dsl::{
        CompileError, CompileOptions, GraphAuthorDocument, GraphDocumentId, GraphSemanticEdit,
        GraphSemanticEditBatch, NodeView, ViewDocument,
    },
    engine::{
        plan::{LeafTaskDescriptor, SubflowContractRegistry},
        repository::{
            CreateRunCommand, DurableRepository, PostgresDurableRepository, SqliteDurableRepository,
        },
        DefinitionRevisionId, EffectEvidence, LeafTaskExecutor, LeafTaskKind,
        LocalContentAddressedArtifactStore, RunId, RuntimeValue, SchedulerTaskKind,
        TaskExecutionRequest, TaskExecutionResult, TransitionKey, TransitionOutcome, VersionTag,
        WorkerExecutionContext, WorkerExecutorRegistry, WorkerFailure,
    },
    history::types::RunStatus,
    runtime::{
        DeployedAgentCatalog, ProductionRunRepository, RequestMetadata, RunService,
        RunServiceConfig,
    },
};
use insight_api::v1::{build_router, ApiAuth, ApiState};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use uuid::Uuid;

const AGENT_ID: &str = "graph_product";
const RESTART_AGENT_ID: &str = "graph_product_restart";
const MULTIRUNTIME_AGENT_ID: &str = "graph_product_multiruntime";
const TAKEOVER_CHILD_AGENT_ID: &str = "graph_product_takeover_child";
const TAKEOVER_PARENT_AGENT_ID: &str = "graph_product_takeover_parent";
const PUBLICATION_CHILD_AGENT_ID: &str = "graph_product_publication_child";
const PUBLICATION_PARENT_AGENT_ID: &str = "graph_product_publication_parent";
const ADMIN_TOKEN: &str = "graph-product-admin";
const WORKER_VERSION: &str = "graph-product-worker-1";
const CHANGED_WORKER_VERSION: &str = "graph-product-worker-current-but-not-pinned";

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

struct ChangedFixtureResolver;

impl LeafDeploymentResolver for ChangedFixtureResolver {
    fn resolve_leaf(
        &self,
        kind: LeafTaskKind,
        descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        ResolvedLeafDeployment::new(
            VersionTag::new(CHANGED_WORKER_VERSION).unwrap(),
            json!({
                "kind": kind.name(),
                "implementation": descriptor.implementation,
                "alias_revision": "changed-after-publication",
            }),
        )
    }
}

#[derive(Default)]
struct PublicationGate {
    reached: tokio::sync::Notify,
    released: Mutex<bool>,
    release: Condvar,
}

impl PublicationGate {
    async fn wait_until_reached(&self) {
        self.reached.notified().await;
    }

    fn release(&self) {
        let mut released = self.released.lock().unwrap();
        *released = true;
        self.release.notify_all();
    }
}

struct BlockingFixtureResolver {
    gate: Arc<PublicationGate>,
}

impl LeafDeploymentResolver for BlockingFixtureResolver {
    fn resolve_leaf(
        &self,
        kind: LeafTaskKind,
        descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        if descriptor.implementation == "fixture.publication_gate" {
            self.gate.reached.notify_one();
            let mut released = self.gate.released.lock().unwrap();
            while !*released {
                released = self.gate.release.wait(released).unwrap();
            }
        }
        FixtureResolver.resolve_leaf(kind, descriptor)
    }
}

struct FixtureExecutor;

#[async_trait]
impl LeafTaskExecutor for FixtureExecutor {
    fn live_response_capable(&self) -> bool {
        true
    }

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
                RuntimeValue::new(json!("graph answer")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

fn graph() -> GraphAuthorDocument {
    let source = include_str!("fixtures/dsl/linear.yaml");
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("graph_product_canvas").unwrap(),
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("graph_product_definition_revision_1").unwrap(),
            "graph-product/agent.yaml",
            source,
        ),
    )
    .unwrap()
}

fn graph_with_unavailable_worker() -> GraphAuthorDocument {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: unavailable_answer
      type: action
      call: fixture.unavailable
      inputs: {question: $question}
      response: string
    - return: $unavailable_answer
"#;
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("graph_product_unavailable_worker_canvas").unwrap(),
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("graph_product_unavailable_worker_revision").unwrap(),
            "graph-product-unavailable-worker/agent.yaml",
            source,
        ),
    )
    .unwrap()
}

fn public_streaming_graph() -> GraphAuthorDocument {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: answer
      type: llm
      model: fixture_model
      stream: true
      publish: true
      messages:
        - role: user
          content: [{text: $question}]
      response: string
    - return: $answer
"#;
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("graph_product_public_streaming_canvas").unwrap(),
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("graph_product_public_streaming_revision").unwrap(),
            "graph-product-public-streaming/agent.yaml",
            source,
        ),
    )
    .unwrap()
}

fn topology_edited_graph() -> GraphAuthorDocument {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: answer
      if: "true"
      then:
        - id: primary_answer
          type: action
          call: fixture.answer
          inputs: {question: $question}
          response: string
        - yield: $primary_answer
      else:
        - id: fallback_answer
          type: action
          call: fixture.answer
          inputs: {question: $question}
          response: string
        - yield: $fallback_answer
    - return: $answer
"#;
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("graph_product_canvas").unwrap(),
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("graph_product_definition_revision_2").unwrap(),
            "graph-product/agent.yaml",
            source,
        ),
    )
    .unwrap()
}

/// Replaces every explicit topology collection inside one atomic edit batch.
/// No intermediate delete-only graph is ever verified or persisted.
fn topology_edits(from: &GraphAuthorDocument, to: &GraphAuthorDocument) -> Vec<GraphSemanticEdit> {
    let mut edits = Vec::new();
    edits.extend(
        from.nodes()
            .iter()
            .map(|value| GraphSemanticEdit::DeleteNode {
                node_id: value.id().clone(),
            }),
    );
    edits.extend(
        from.ports()
            .control()
            .iter()
            .map(|value| GraphSemanticEdit::DeleteControlPort {
                port_id: value.id().clone(),
            }),
    );
    edits.extend(
        from.ports()
            .data()
            .iter()
            .map(|value| GraphSemanticEdit::DeleteDataPort {
                port_id: value.id().clone(),
            }),
    );
    edits.extend(
        from.edges()
            .control()
            .iter()
            .map(|value| GraphSemanticEdit::DeleteControlEdge {
                edge_id: value.id().clone(),
            }),
    );
    edits.extend(
        from.bindings()
            .data()
            .iter()
            .map(|value| GraphSemanticEdit::DeleteDataBinding {
                binding_id: value.id().clone(),
            }),
    );
    edits.extend(
        from.bindings()
            .phi()
            .iter()
            .map(|value| GraphSemanticEdit::DeletePhiBinding {
                binding_id: value.id().clone(),
            }),
    );
    edits.extend(
        from.scopes()
            .iter()
            .map(|value| GraphSemanticEdit::DeleteScope {
                scope_id: value.id().clone(),
            }),
    );
    edits.extend(
        from.policies()
            .iter()
            .map(|value| GraphSemanticEdit::DeletePolicy {
                policy_id: value.id().clone(),
            }),
    );
    edits.extend(
        to.nodes()
            .iter()
            .cloned()
            .map(|node| GraphSemanticEdit::UpsertNode { node }),
    );
    edits.extend(
        to.ports()
            .control()
            .iter()
            .cloned()
            .map(|port| GraphSemanticEdit::UpsertControlPort { port }),
    );
    edits.extend(
        to.ports()
            .data()
            .iter()
            .cloned()
            .map(|port| GraphSemanticEdit::UpsertDataPort { port }),
    );
    edits.extend(
        to.edges()
            .control()
            .iter()
            .cloned()
            .map(|edge| GraphSemanticEdit::UpsertControlEdge { edge }),
    );
    edits.extend(
        to.bindings()
            .data()
            .iter()
            .cloned()
            .map(|binding| GraphSemanticEdit::UpsertDataBinding { binding }),
    );
    edits.extend(
        to.bindings()
            .phi()
            .iter()
            .cloned()
            .map(|binding| GraphSemanticEdit::UpsertPhiBinding { binding }),
    );
    edits.extend(
        to.scopes()
            .iter()
            .cloned()
            .map(|scope| GraphSemanticEdit::UpsertScope { scope }),
    );
    edits.extend(
        to.policies()
            .iter()
            .cloned()
            .map(|policy| GraphSemanticEdit::UpsertPolicy { policy }),
    );
    edits
}

fn restart_graph() -> GraphAuthorDocument {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: reply
      wait: {signal: reply, response: string}
    - return: $reply
"#;
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("graph_product_restart_canvas").unwrap(),
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("graph_product_restart_definition_revision_1").unwrap(),
            "graph-product-restart/agent.yaml",
            source,
        ),
    )
    .unwrap()
}

fn multiruntime_graph(revision: u8) -> GraphAuthorDocument {
    let source = format!(
        r#"api_version: insight.agent/v1
kind: agent
inputs: {{}}
output: string
workflow:
  steps:
    - id: reply_{revision}
      wait: {{signal: reply_{revision}, response: string}}
    - return: $reply_{revision}
"#
    );
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new(format!("graph_product_multiruntime_canvas_{revision}")).unwrap(),
        &source,
        CompileOptions::new(
            DefinitionRevisionId::new(format!(
                "graph_product_multiruntime_definition_revision_{revision}"
            ))
            .unwrap(),
            format!("graph-product-multiruntime/revision-{revision}.yaml"),
            &source,
        ),
    )
    .unwrap()
}

fn takeover_child_graph() -> GraphAuthorDocument {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: takeover_reply
      wait: {signal: takeover_reply, response: string}
    - return: $takeover_reply
"#;
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("graph_product_takeover_child_canvas").unwrap(),
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("graph_product_takeover_child_definition_revision_1")
                .unwrap(),
            "graph-product-takeover/child.yaml",
            source,
        ),
    )
    .unwrap()
}

fn takeover_parent_graph(child_definition_revision: &str) -> GraphAuthorDocument {
    let source = format!(
        r#"api_version: insight.agent/v1
kind: agent
inputs: {{question: string}}
output: string
workflow:
  steps:
    - id: child_answer
      type: call
      definition_revision: {child_definition_revision}
      interface_version: takeover-child-v1
      input: {{question: $question}}
      response: string
    - return: $child_answer
"#
    );
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("graph_product_takeover_parent_canvas").unwrap(),
        &source,
        CompileOptions::new(
            DefinitionRevisionId::new("graph_product_takeover_parent_definition_revision_1")
                .unwrap(),
            "graph-product-takeover/parent.yaml",
            &source,
        ),
    )
    .unwrap()
}

fn publication_child_graph() -> GraphAuthorDocument {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: child_answer
      type: action
      call: fixture.answer
      inputs: {question: $question}
      response: string
    - return: $child_answer
"#;
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("graph_product_publication_child_canvas").unwrap(),
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("graph_product_publication_child_definition_revision_1")
                .unwrap(),
            "graph-product-publication/child.yaml",
            source,
        ),
    )
    .unwrap()
}

fn publication_parent_graph(child_definition_revision: &str) -> GraphAuthorDocument {
    let source = format!(
        r#"api_version: insight.agent/v1
kind: agent
inputs: {{question: string}}
output: string
workflow:
  steps:
    - id: publication_gate
      type: action
      call: fixture.publication_gate
      inputs: {{question: $question}}
      response: string
    - id: child_answer
      type: call
      definition_revision: {child_definition_revision}
      interface_version: publication-child-v1
      input: {{question: $question}}
      response: string
    - return: $child_answer
"#
    );
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("graph_product_publication_parent_canvas").unwrap(),
        &source,
        CompileOptions::new(
            DefinitionRevisionId::new("graph_product_publication_parent_definition_revision_1")
                .unwrap(),
            "graph-product-publication/parent.yaml",
            &source,
        ),
    )
    .unwrap()
}

fn frozen_subflow_deployment(
    catalog: &insight_agent_platform::engine::repository::VersionedPlanCatalog,
    parent_deployment_revision_id: &str,
) -> String {
    let plan = catalog
        .plans()
        .iter()
        .find(|plan| plan.deployment_revision_id().as_str() == parent_deployment_revision_id)
        .expect("published parent deployment is durable");
    let document = serde_json::to_value(plan).unwrap();
    document["resolved_bindings"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|entry| {
            let binding = entry.get("binding")?;
            (binding.get("adapter")?.as_str()? == "durable_subflow").then(|| {
                binding["deployment_revision_id"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
        })
        .expect("parent deployment freezes one durable subflow")
}

fn conflicting_supplied_catalog() -> DeployedAgentCatalog {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - return: supplied-built-in-must-not-win
"#;
    let graph = GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("graph_product_conflicting_builtin_canvas").unwrap(),
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("graph_product_conflicting_builtin_revision").unwrap(),
            "graph-product-conflicting-builtin/agent.yaml",
            source,
        ),
    )
    .unwrap();
    let published = Arc::new(
        PublishedAgent::from_verified_graph(
            RESTART_AGENT_ID,
            "Conflicting supplied built-in",
            "This metadata must not replace the durable Graph head",
            graph,
        )
        .unwrap(),
    );
    let deployed = Arc::new(
        DeployedAgent::publish(published, &FixtureResolver, SubflowContractRegistry::new())
            .unwrap(),
    );
    DeployedAgentCatalog::new(vec![deployed]).unwrap()
}

fn workers() -> WorkerExecutorRegistry {
    let mut workers = WorkerExecutorRegistry::new();
    for worker_version in [WORKER_VERSION, CHANGED_WORKER_VERSION] {
        workers
            .register(
                SchedulerTaskKind::Action,
                "fixture.answer",
                VersionTag::new("1").unwrap(),
                VersionTag::new(worker_version).unwrap(),
                Arc::new(FixtureExecutor),
            )
            .unwrap();
        workers
            .register(
                SchedulerTaskKind::Action,
                "fixture.publication_gate",
                VersionTag::new("1").unwrap(),
                VersionTag::new(worker_version).unwrap(),
                Arc::new(FixtureExecutor),
            )
            .unwrap();
        workers
            .register(
                SchedulerTaskKind::Llm,
                "core.llm",
                VersionTag::new("2").unwrap(),
                VersionTag::new(worker_version).unwrap(),
                Arc::new(FixtureExecutor),
            )
            .unwrap();
    }
    workers
}

fn config() -> RunServiceConfig {
    RunServiceConfig::production(16, 4, 2, 64).with_run_timeout(Duration::from_secs(30))
}

fn single_process_development_config() -> RunServiceConfig {
    RunServiceConfig::single_process_development(16, 4, 2, 64)
        .with_run_timeout(Duration::from_secs(30))
}

async fn shared_artifact_store(
    root: PathBuf,
    namespace: &str,
) -> Arc<LocalContentAddressedArtifactStore> {
    Arc::new(
        LocalContentAddressedArtifactStore::open_shared(root, 64 * 1024, namespace)
            .await
            .unwrap(),
    )
}

async fn response_json(response: axum::response::Response) -> Value {
    serde_json::from_slice(
        &to_bytes(response.into_body(), 64 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap()
}

async fn assert_discovery_version(app: &Router, agent_id: &str, expected_version: &str) {
    let listed = app
        .clone()
        .oneshot(
            authorized(Request::get("/v1/agents"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(listed.status(), StatusCode::OK);
    let listed = response_json(listed).await;
    let listed_agent = listed["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|agent| agent["id"] == agent_id)
        .expect("durable Agent head must appear in discovery");
    assert_eq!(listed_agent["version"], expected_version);
    assert_eq!(
        listed_agent["streaming"],
        json!({
            "protocol": "response-stream/v1",
            "transport": "sse",
            "live_only": true,
            "sources": []
        })
    );
    assert_eq!(
        listed_agent["output_schema"],
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "string"
        })
    );

    let loaded = app
        .clone()
        .oneshot(
            authorized(Request::get(format!("/v1/agents/{agent_id}")))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(loaded.status(), StatusCode::OK);
    let loaded = response_json(loaded).await;
    assert_eq!(loaded["data"]["version"], expected_version);
    assert_eq!(&loaded["data"], listed_agent);
}

fn authorized(builder: axum::http::request::Builder) -> axum::http::request::Builder {
    builder.header("authorization", format!("Bearer {ADMIN_TOKEN}"))
}

async fn wait_for_completed(service: &RunService, run_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let run = service.get_run(run_id).await.unwrap();
        if run.status().is_terminal() {
            assert_eq!(run.status(), RunStatus::Completed);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "Graph-authored Run did not complete"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn exercise_graph_product(
    repository: Arc<dyn ProductionRunRepository>,
    service_config: RunServiceConfig,
) {
    let artifact_root = tempfile::tempdir().unwrap();
    let artifact_store = shared_artifact_store(
        artifact_root.path().join("objects"),
        "graph-product-exercise",
    )
    .await;
    let service = RunService::start_with_artifact_store_and_graph_publication(
        conflicting_supplied_catalog(),
        Arc::clone(&repository),
        workers(),
        artifact_store.clone(),
        Arc::new(FixtureResolver),
        service_config,
    )
    .await
    .unwrap();
    let app = build_router(ApiState {
        service: service.clone(),
        auth: ApiAuth::bearer_token(ADMIN_TOKEN),
        sse_keep_alive_interval: Duration::from_secs(1),
        readiness_probe_timeout: Duration::from_secs(1),
    });
    let graph = graph();
    let graph_bytes = graph.encode_json().unwrap();

    let encoded_graph = String::from_utf8(graph_bytes.clone()).unwrap();
    let duplicate_key_graph = encoded_graph.replacen(
        "\"schema_version\":2",
        "\"schema_version\":2,\"schema_version\":2",
        1,
    );
    let rejected_duplicate = app
        .clone()
        .oneshot(
            authorized(Request::post(format!(
                "/v1/graph-agents/{AGENT_ID}/revisions"
            )))
            .header("content-type", "application/json")
            .body(Body::from(duplicate_key_graph))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rejected_duplicate.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_json(rejected_duplicate).await["code"],
        "GRAPH_DOCUMENT_INVALID"
    );

    let unauthorized = app
        .clone()
        .oneshot(
            Request::post(format!("/v1/graph-agents/{AGENT_ID}/revisions"))
                .header("content-type", "application/json")
                .body(Body::from(graph_bytes.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let published = app
        .clone()
        .oneshot(
            authorized(Request::post(format!(
                "/v1/graph-agents/{AGENT_ID}/revisions"
            )))
            .header("content-type", "application/json")
            .body(Body::from(graph_bytes))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(published.status(), StatusCode::CREATED);
    let published = response_json(published).await;
    let definition_revision_id = published["data"]["definition_revision_id"]
        .as_str()
        .unwrap();
    let deployment_revision_id = published["data"]["deployment_revision_id"]
        .as_str()
        .unwrap();
    assert_eq!(
        published["data"]["graph_document_id"],
        "graph_product_canvas"
    );
    assert_eq!(
        published["data"]["semantic_hash"],
        graph.semantic_hash().as_str()
    );

    // Resolver output is only a deployment proposal. A Graph publication
    // whose exact worker is absent from this runtime must fail before it can
    // install an immutable archive row or replace the last executable head.
    let catalog_before_unavailable = repository.load_versioned_plan_catalog().await.unwrap();
    let unavailable = service
        .publish_graph(AGENT_ID, graph_with_unavailable_worker())
        .await
        .unwrap_err();
    assert_eq!(unavailable.code(), "GRAPH_PUBLICATION_INVALID");
    let catalog_after_unavailable = repository.load_versioned_plan_catalog().await.unwrap();
    assert_eq!(
        catalog_after_unavailable.heads(),
        catalog_before_unavailable.heads()
    );
    assert_eq!(
        catalog_after_unavailable.plans(),
        catalog_before_unavailable.plans()
    );
    assert!(catalog_after_unavailable.plans().iter().all(|plan| {
        plan.definition_revision_id().as_str() != "graph_product_unavailable_worker_revision"
    }));
    let old_alias = service
        .create_detached(
            AGENT_ID,
            json!({"question": "old executable head remains admitted"}),
            RequestMetadata {
                request_id: Some("graph-worker-rejection-old-alias".to_owned()),
            },
        )
        .await
        .unwrap();
    assert_eq!(old_alias.agent_version, deployment_revision_id);
    wait_for_completed(&service, &old_alias.run_id).await;

    let revision = app
        .clone()
        .oneshot(
            authorized(Request::get(format!(
                "/v1/graph-agents/{AGENT_ID}/revisions/{definition_revision_id}"
            )))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(revision.status(), StatusCode::OK);
    let revision = response_json(revision).await;
    let revision =
        GraphAuthorDocument::decode_json(&serde_json::to_vec(&revision["data"]).unwrap()).unwrap();
    assert_eq!(revision, graph);

    let topology_target = topology_edited_graph();
    assert_eq!(
        graph.metadata().entry_node_id(),
        topology_target.metadata().entry_node_id()
    );
    let edited_revision_id = "graph_product_definition_revision_2";
    let edit_batch = GraphSemanticEditBatch::new(
        graph.semantic_hash().clone(),
        DefinitionRevisionId::new(edited_revision_id).unwrap(),
        topology_edits(&graph, &topology_target),
    )
    .unwrap();
    let edited = app
        .clone()
        .oneshot(
            authorized(Request::post(format!(
                "/v1/graph-agents/{AGENT_ID}/revisions/{definition_revision_id}/semantic-edits"
            )))
            .header("content-type", "application/json")
            .body(Body::from(edit_batch.encode_json().unwrap()))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(edited.status(), StatusCode::CREATED);
    let edited = response_json(edited).await;
    assert_eq!(edited["data"]["definition_revision_id"], edited_revision_id);
    assert_eq!(
        edited["data"]["semantic_hash"],
        topology_target.semantic_hash().as_str()
    );
    let edited_semantic_hash = insight_agent_platform::engine::plan::SemanticHash::parse(
        edited["data"]["semantic_hash"].as_str().unwrap(),
    )
    .unwrap();
    let edited_graph = service
        .graph_author(AGENT_ID, edited_revision_id)
        .await
        .unwrap();
    assert_eq!(
        edited_graph.semantic_hash(),
        topology_target.semantic_hash()
    );
    assert_eq!(edited_graph.nodes(), topology_target.nodes());
    assert_eq!(edited_graph.ports(), topology_target.ports());
    assert_eq!(edited_graph.edges(), topology_target.edges());
    assert_eq!(edited_graph.bindings(), topology_target.bindings());
    assert_eq!(edited_graph.scopes(), topology_target.scopes());

    // The source archive remains immutable and the new revision has no
    // inherited View document.
    assert_eq!(
        service
            .graph_author(AGENT_ID, definition_revision_id)
            .await
            .unwrap(),
        graph
    );
    let missing_new_view = app
        .clone()
        .oneshot(
            authorized(Request::get(format!(
                "/v1/graph-agents/{AGENT_ID}/revisions/{edited_revision_id}/view"
            )))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing_new_view.status(), StatusCode::NOT_FOUND);

    // A stale base may compile locally, but the repository route CAS rejects
    // it before installing the proposed target revision.
    let stale_revision_id = "graph_product_definition_revision_stale";
    let stale_batch = GraphSemanticEditBatch::new(
        graph.semantic_hash().clone(),
        DefinitionRevisionId::new(stale_revision_id).unwrap(),
        topology_edits(&graph, &topology_target),
    )
    .unwrap();
    let stale = app
        .clone()
        .oneshot(
            authorized(Request::post(format!(
                "/v1/graph-agents/{AGENT_ID}/revisions/{definition_revision_id}/semantic-edits"
            )))
            .header("content-type", "application/json")
            .body(Body::from(stale_batch.encode_json().unwrap()))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    assert_eq!(response_json(stale).await["code"], "GRAPH_EDIT_CONFLICT");
    assert_eq!(
        service
            .graph_author(AGENT_ID, stale_revision_id)
            .await
            .unwrap_err()
            .code(),
        "GRAPH_DOCUMENT_NOT_FOUND"
    );

    // Full-plan verification also rolls back an invalid grouped edit.
    let invalid_revision_id = "graph_product_definition_revision_invalid";
    let invalid_batch = GraphSemanticEditBatch::new(
        edited_semantic_hash,
        DefinitionRevisionId::new(invalid_revision_id).unwrap(),
        vec![GraphSemanticEdit::DeleteNode {
            node_id: edited_graph.metadata().entry_node_id().clone(),
        }],
    )
    .unwrap();
    let invalid = app
        .clone()
        .oneshot(
            authorized(Request::post(format!(
                "/v1/graph-agents/{AGENT_ID}/revisions/{edited_revision_id}/semantic-edits"
            )))
            .header("content-type", "application/json")
            .body(Body::from(invalid_batch.encode_json().unwrap()))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response_json(invalid).await["code"], "GRAPH_EDIT_INVALID");
    assert_eq!(
        service
            .graph_author(AGENT_ID, invalid_revision_id)
            .await
            .unwrap_err()
            .code(),
        "GRAPH_DOCUMENT_NOT_FOUND"
    );

    let created = create_pinned_run(&app, deployment_revision_id, "first question").await;
    let run_id = created["data"]["run_id"].as_str().unwrap().to_owned();
    wait_for_completed(&service, &run_id).await;

    let execution_graph = app
        .clone()
        .oneshot(
            authorized(Request::get(format!("/v1/runs/{run_id}/execution-graph")))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(execution_graph.status(), StatusCode::OK);
    let execution_graph = response_json(execution_graph).await;
    let loaded_graph =
        GraphAuthorDocument::decode_json(&serde_json::to_vec(&execution_graph["data"]).unwrap())
            .unwrap();
    assert_eq!(loaded_graph.semantic_hash(), graph.semantic_hash());
    assert_eq!(loaded_graph.nodes(), graph.nodes());

    let trace = app
        .clone()
        .oneshot(
            authorized(Request::get(format!("/v1/runs/{run_id}/trace")))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(trace.status(), StatusCode::OK);
    let trace = response_json(trace).await;
    assert_eq!(trace["data"]["run_id"], run_id);
    assert_eq!(trace["data"]["graph_document_id"], "graph_product_canvas");
    assert!(!trace["data"]["activations"].as_array().unwrap().is_empty());

    let mut view = ViewDocument::new(graph.document_id().clone());
    view.set_node(graph.nodes()[0].id().clone(), NodeView::at(10.0, 20.0))
        .unwrap();
    let saved_view = put_view(&app, definition_revision_id, 0, &view).await;
    assert_eq!(saved_view.status(), StatusCode::OK);
    let saved_view = response_json(saved_view).await;
    assert_eq!(saved_view["data"]["version"], 1);
    let loaded_view = app
        .clone()
        .oneshot(
            authorized(Request::get(format!(
                "/v1/graph-agents/{AGENT_ID}/revisions/{definition_revision_id}/view"
            )))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(loaded_view.status(), StatusCode::OK);
    assert_eq!(response_json(loaded_view).await["data"]["version"], 1);

    let mut competing = view.clone();
    competing
        .set_node(graph.nodes()[0].id().clone(), NodeView::at(80.0, 90.0))
        .unwrap();
    let conflict = put_view(&app, definition_revision_id, 0, &competing).await;
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    assert_eq!(response_json(conflict).await["code"], "GRAPH_VIEW_CONFLICT");

    let damaged = app
        .clone()
        .oneshot(
            authorized(Request::put(format!(
                "/v1/graph-agents/{AGENT_ID}/revisions/{definition_revision_id}/view?expected_version=1"
            )))
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(damaged.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response_json(damaged).await["code"], "GRAPH_VIEW_INVALID");

    let second = create_pinned_run(&app, deployment_revision_id, "after bad view").await;
    let second_run_id = second["data"]["run_id"].as_str().unwrap();
    wait_for_completed(&service, second_run_id).await;
    let still_same_graph = service.execution_graph(second_run_id).await.unwrap();
    assert_eq!(still_same_graph.semantic_hash(), graph.semantic_hash());

    let restart_publication = service
        .publish_graph(RESTART_AGENT_ID, restart_graph())
        .await
        .unwrap();
    assert_eq!(
        service
            .agents()
            .get(RESTART_AGENT_ID)
            .unwrap()
            .deployment_revision_id()
            .as_str(),
        restart_publication.deployment_revision_id
    );
    let restart_run = service
        .create_detached_from_deployment(
            RESTART_AGENT_ID,
            &restart_publication.deployment_revision_id,
            json!({}),
            RequestMetadata {
                request_id: Some("graph-product-restart-run".to_owned()),
            },
        )
        .await
        .unwrap();
    let metadata_before = service
        .agents()
        .get(RESTART_AGENT_ID)
        .unwrap()
        .published()
        .metadata()
        .clone();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        service.get_run(&restart_run.run_id).await.unwrap().status(),
        RunStatus::Running
    );
    service.shutdown(Duration::from_secs(2)).await.unwrap();

    let unavailable = RunService::start_with_artifact_store_and_graph_publication(
        DeployedAgentCatalog::default(),
        Arc::clone(&repository),
        WorkerExecutorRegistry::new(),
        artifact_store.clone(),
        Arc::new(ChangedFixtureResolver),
        service_config,
    )
    .await;
    match unavailable {
        Ok(_) => panic!("startup accepted a durable deployment without its pinned worker"),
        Err(error) => assert_eq!(error.code(), "RUN_DEPLOYMENT_UNAVAILABLE"),
    }

    let restarted = RunService::start_with_artifact_store_and_graph_publication(
        conflicting_supplied_catalog(),
        repository,
        workers(),
        artifact_store,
        Arc::new(ChangedFixtureResolver),
        service_config,
    )
    .await
    .unwrap();
    let restored = restarted.agents().get(RESTART_AGENT_ID).unwrap();
    assert_eq!(
        restored.deployment_revision_id().as_str(),
        restart_publication.deployment_revision_id
    );
    assert_eq!(restored.published().metadata(), &metadata_before);

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match restarted
            .signal(
                &restart_run.run_id,
                "reply",
                "graph-product-restart-signal",
                json!("resumed after restart"),
            )
            .await
        {
            Ok(_) => break,
            Err(error) if error.code() == "SIGNAL_NOT_WAITING" => {
                assert!(
                    Instant::now() < deadline,
                    "restored signal wait never opened"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => panic!("restored signal failed: {error}"),
        }
    }
    wait_for_completed(&restarted, &restart_run.run_id).await;
    restarted.shutdown(Duration::from_secs(2)).await.unwrap();
}

async fn create_pinned_run(app: &Router, deployment_revision_id: &str, question: &str) -> Value {
    let response = app
        .clone()
        .oneshot(
            authorized(Request::post(format!(
                "/v1/agents/{AGENT_ID}/deployments/{deployment_revision_id}/runs"
            )))
            .header("content-type", "application/json")
            .body(Body::from(json!({"question": question}).to_string()))
            .unwrap(),
        )
        .await
        .unwrap();
    if response.status() != StatusCode::ACCEPTED {
        let status = response.status();
        let body = response_json(response).await;
        panic!("pinned Graph Run returned {status}: {body}");
    }
    response_json(response).await
}

async fn put_view(
    app: &Router,
    definition_revision_id: &str,
    expected_version: u64,
    view: &ViewDocument,
) -> axum::response::Response {
    app.clone()
        .oneshot(
            authorized(Request::put(format!(
                "/v1/graph-agents/{AGENT_ID}/revisions/{definition_revision_id}/view?expected_version={expected_version}"
            )))
            .header("content-type", "application/json")
            .body(Body::from(view.encode_json().unwrap()))
            .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn sqlite_graph_author_publish_run_trace_and_view_is_a_product_flow() {
    let repository = Arc::new(SqliteDurableRepository::in_memory().await.unwrap());
    exercise_graph_product(
        repository as Arc<dyn ProductionRunRepository>,
        single_process_development_config(),
    )
    .await;
}

#[tokio::test]
async fn sqlite_peer_discovery_reads_each_durable_graph_head_without_run_admission() {
    let repository = Arc::new(SqliteDurableRepository::in_memory().await.unwrap());
    let service_a = RunService::start_with_graph_publication(
        DeployedAgentCatalog::default(),
        repository.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        Arc::new(FixtureResolver),
        single_process_development_config(),
    )
    .await
    .unwrap();
    let service_b = RunService::start_with_graph_publication(
        DeployedAgentCatalog::default(),
        repository as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        Arc::new(FixtureResolver),
        single_process_development_config(),
    )
    .await
    .unwrap();
    let app_b = build_router(ApiState {
        service: service_b.clone(),
        auth: ApiAuth::bearer_token(ADMIN_TOKEN),
        sse_keep_alive_interval: Duration::from_secs(1),
        readiness_probe_timeout: Duration::from_secs(1),
    });

    let first = service_a
        .publish_graph(MULTIRUNTIME_AGENT_ID, multiruntime_graph(1))
        .await
        .unwrap();
    assert!(service_b.agents().get(MULTIRUNTIME_AGENT_ID).is_none());
    assert_discovery_version(&app_b, MULTIRUNTIME_AGENT_ID, &first.deployment_revision_id).await;

    let second = service_a
        .publish_graph(MULTIRUNTIME_AGENT_ID, multiruntime_graph(2))
        .await
        .unwrap();
    assert_discovery_version(
        &app_b,
        MULTIRUNTIME_AGENT_ID,
        &second.deployment_revision_id,
    )
    .await;

    service_a.shutdown(Duration::from_secs(2)).await.unwrap();
    service_b.shutdown(Duration::from_secs(2)).await.unwrap();
}

#[tokio::test]
async fn postgres_graph_author_product_contract_matches_sqlite() {
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        return;
    };
    let schema = format!("graph_product_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let repository = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    repository.initialize_schema().await.unwrap();

    exercise_graph_product(repository as Arc<dyn ProductionRunRepository>, config()).await;

    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_production_graph_publication_rejects_public_streaming_without_shared_broker() {
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        return;
    };
    let schema = format!("graph_public_stream_gate_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let repository = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    repository.initialize_schema().await.unwrap();
    let artifact_directory = tempfile::tempdir().unwrap();
    let artifact_store = shared_artifact_store(
        artifact_directory.path().join("shared"),
        "graph-public-stream-gate",
    )
    .await;
    let service = RunService::start_with_artifact_store_and_graph_publication(
        DeployedAgentCatalog::default(),
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        artifact_store,
        Arc::new(FixtureResolver),
        config(),
    )
    .await
    .unwrap();
    let before = repository.load_versioned_plan_catalog().await.unwrap();

    let error = service
        .publish_graph("graph_product_public_streaming", public_streaming_graph())
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        "PLATFORM_PRODUCTION_REQUIRES_SHARED_LIVE_RESPONSE_BROKER"
    );
    let after = repository.load_versioned_plan_catalog().await.unwrap();
    assert_eq!(after.plans().len(), before.plans().len());
    assert_eq!(after.heads().len(), before.heads().len());

    service.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(repository);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn postgres_live_runtimes_admit_new_runs_from_the_durable_graph_head() {
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        return;
    };
    let suffix = Uuid::new_v4().simple().to_string();
    let schema = format!("graph_route_{}", &suffix[..16]);
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let repository_a = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    repository_a.initialize_schema().await.unwrap();
    let repository_b = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    let artifact_root = tempfile::tempdir().unwrap();
    let artifact_store = shared_artifact_store(
        artifact_root.path().join("objects"),
        "graph-product-live-runtimes",
    )
    .await;
    let service_a = RunService::start_with_artifact_store_and_graph_publication(
        DeployedAgentCatalog::default(),
        repository_a.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        artifact_store.clone(),
        Arc::new(FixtureResolver),
        config(),
    )
    .await
    .unwrap();
    let service_b = RunService::start_with_artifact_store_and_graph_publication(
        DeployedAgentCatalog::default(),
        repository_b.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        artifact_store,
        Arc::new(FixtureResolver),
        config(),
    )
    .await
    .unwrap();
    let app_b = build_router(ApiState {
        service: service_b.clone(),
        auth: ApiAuth::bearer_token(ADMIN_TOKEN),
        sse_keep_alive_interval: Duration::from_secs(1),
        readiness_probe_timeout: Duration::from_secs(1),
    });

    let first = service_a
        .publish_graph(MULTIRUNTIME_AGENT_ID, multiruntime_graph(1))
        .await
        .unwrap();
    assert!(service_b.agents().get(MULTIRUNTIME_AGENT_ID).is_none());
    assert_discovery_version(&app_b, MULTIRUNTIME_AGENT_ID, &first.deployment_revision_id).await;
    let admitted_on_b = service_b
        .create_detached(
            MULTIRUNTIME_AGENT_ID,
            json!({}),
            RequestMetadata {
                request_id: Some("graph-route-first-on-b".to_owned()),
            },
        )
        .await
        .unwrap();
    assert_eq!(admitted_on_b.agent_version, first.deployment_revision_id);
    assert!(service_b
        .agents()
        .get_deployment(&admitted_on_b.agent_version)
        .is_some());

    let first_snapshot = repository_a.load_versioned_plan_catalog().await.unwrap();
    let first_head = first_snapshot
        .heads()
        .iter()
        .find(|head| head.agent_id() == MULTIRUNTIME_AGENT_ID)
        .unwrap()
        .clone();
    let first_plan = first_snapshot
        .plans()
        .iter()
        .find(|plan| plan.deployment_revision_id() == first_head.deployment_revision_id())
        .unwrap()
        .clone();
    let second = service_a
        .publish_graph(MULTIRUNTIME_AGENT_ID, multiruntime_graph(2))
        .await
        .unwrap();
    assert_discovery_version(
        &app_b,
        MULTIRUNTIME_AGENT_ID,
        &second.deployment_revision_id,
    )
    .await;

    // A snapshot head is only a proposal. The create transaction must reject
    // it after a concurrent publication advanced the durable route.
    let stale_run_id = RunId::new(format!("run_graph_route_stale_{suffix}")).unwrap();
    let stale = repository_a
        .create_run(
            TransitionKey::derive("test.graph-route", &[stale_run_id.as_str()]).unwrap(),
            CreateRunCommand::new(stale_run_id.clone(), &first_plan, json!({}))
                .unwrap()
                .with_expected_publication_head(first_head)
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(stale, TransitionOutcome::StateConflict));
    assert!(repository_a
        .load_run(&stale_run_id)
        .await
        .unwrap()
        .is_none());

    let refreshed_on_b = service_b
        .create_detached(
            MULTIRUNTIME_AGENT_ID,
            json!({}),
            RequestMetadata {
                request_id: Some("graph-route-refreshed-on-b".to_owned()),
            },
        )
        .await
        .unwrap();
    assert_eq!(refreshed_on_b.agent_version, second.deployment_revision_id);

    let (third, fourth) = tokio::join!(
        service_a.publish_graph(MULTIRUNTIME_AGENT_ID, multiruntime_graph(3)),
        service_b.publish_graph(MULTIRUNTIME_AGENT_ID, multiruntime_graph(4)),
    );
    third.unwrap();
    fourth.unwrap();
    let final_snapshot = repository_a.load_versioned_plan_catalog().await.unwrap();
    let final_deployment = final_snapshot
        .heads()
        .iter()
        .find(|head| head.agent_id() == MULTIRUNTIME_AGENT_ID)
        .unwrap()
        .deployment_revision_id()
        .as_str()
        .to_owned();
    assert_discovery_version(&app_b, MULTIRUNTIME_AGENT_ID, &final_deployment).await;
    let admitted_on_a = service_a
        .create_detached(
            MULTIRUNTIME_AGENT_ID,
            json!({}),
            RequestMetadata {
                request_id: Some("graph-route-final-on-a".to_owned()),
            },
        )
        .await
        .unwrap();
    let admitted_on_b = service_b
        .create_detached(
            MULTIRUNTIME_AGENT_ID,
            json!({}),
            RequestMetadata {
                request_id: Some("graph-route-final-on-b".to_owned()),
            },
        )
        .await
        .unwrap();
    assert_eq!(admitted_on_a.agent_version, final_deployment);
    assert_eq!(admitted_on_b.agent_version, final_deployment);

    service_a.shutdown(Duration::from_secs(2)).await.unwrap();
    service_b.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(service_a);
    drop(service_b);
    drop(repository_a);
    drop(repository_b);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_subflow_publication_rejects_stale_snapshot_and_keeps_old_parent_frozen() {
    // Build both author documents even on developer machines that do not run
    // the optional real-PostgreSQL contract test.
    let child_graph = publication_child_graph();
    let parent_graph =
        publication_parent_graph("graph_product_publication_child_definition_revision_1");
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        return;
    };
    let suffix = Uuid::new_v4().simple().to_string();
    let schema = format!("graph_publication_cas_{}", &suffix[..16]);
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let repository_a = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    repository_a.initialize_schema().await.unwrap();
    let repository_b = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    let gate = Arc::new(PublicationGate::default());
    let artifact_root = tempfile::tempdir().unwrap();
    let artifact_store = shared_artifact_store(
        artifact_root.path().join("objects"),
        "graph-product-publication-cas",
    )
    .await;
    let service_a = RunService::start_with_artifact_store_and_graph_publication(
        DeployedAgentCatalog::default(),
        repository_a.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        artifact_store.clone(),
        Arc::new(BlockingFixtureResolver {
            gate: Arc::clone(&gate),
        }),
        config(),
    )
    .await
    .unwrap();
    let service_b = RunService::start_with_artifact_store_and_graph_publication(
        DeployedAgentCatalog::default(),
        repository_b.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        artifact_store,
        Arc::new(ChangedFixtureResolver),
        config(),
    )
    .await
    .unwrap();
    let child_v1 = service_a
        .publish_graph(PUBLICATION_CHILD_AGENT_ID, child_graph.clone())
        .await
        .unwrap();
    assert_eq!(
        child_v1.definition_revision_id,
        "graph_product_publication_child_definition_revision_1"
    );

    // Runtime B started before the child existed. Publishing the first parent
    // must restore the child selected by the durable head, rather than infer
    // candidates from B's process-local current catalog.
    let parent_v1 = service_b
        .publish_graph(PUBLICATION_PARENT_AGENT_ID, parent_graph.clone())
        .await
        .unwrap();
    let before_stale = repository_a.load_versioned_plan_catalog().await.unwrap();
    assert_eq!(
        frozen_subflow_deployment(&before_stale, &parent_v1.deployment_revision_id),
        child_v1.deployment_revision_id
    );
    let parent_plan_count = before_stale
        .plans()
        .iter()
        .filter(|plan| plan.agent_id() == PUBLICATION_PARENT_AGENT_ID)
        .count();

    let stale_service = service_a.clone();
    let stale_graph = parent_graph.clone();
    let runtime = tokio::runtime::Handle::current();
    let stale_publication = tokio::task::spawn_blocking(move || {
        runtime.block_on(async move {
            stale_service
                .publish_graph(PUBLICATION_PARENT_AGENT_ID, stale_graph)
                .await
        })
    });
    tokio::time::timeout(Duration::from_secs(10), gate.wait_until_reached())
        .await
        .expect("stale parent publication did not reach its gated leaf resolver");

    // The gate is reached after Runtime A has selected child v1 from its
    // durable snapshot, but before the repository publication transaction.
    // Runtime B now advances the same child definition to a new deployment.
    let child_v2_result = tokio::time::timeout(
        Duration::from_secs(30),
        service_b.publish_graph(PUBLICATION_CHILD_AGENT_ID, child_graph),
    )
    .await;
    gate.release();
    let child_v2 = child_v2_result
        .expect("new child publication did not finish")
        .unwrap();
    assert_eq!(
        child_v2.definition_revision_id,
        child_v1.definition_revision_id
    );
    assert_ne!(
        child_v2.deployment_revision_id,
        child_v1.deployment_revision_id
    );
    let stale_error = tokio::time::timeout(Duration::from_secs(10), stale_publication)
        .await
        .expect("stale parent publication did not finish")
        .unwrap()
        .unwrap_err();
    assert_eq!(stale_error.code(), "GRAPH_PUBLICATION_CONFLICT");

    let after_stale = repository_a.load_versioned_plan_catalog().await.unwrap();
    assert_eq!(
        after_stale
            .plans()
            .iter()
            .filter(|plan| plan.agent_id() == PUBLICATION_PARENT_AGENT_ID)
            .count(),
        parent_plan_count,
        "stale proposal installed a parent archive row"
    );
    assert_eq!(
        after_stale
            .heads()
            .iter()
            .find(|head| head.agent_id() == PUBLICATION_PARENT_AGENT_ID)
            .unwrap()
            .deployment_revision_id()
            .as_str(),
        parent_v1.deployment_revision_id
    );
    assert_eq!(
        frozen_subflow_deployment(&after_stale, &parent_v1.deployment_revision_id),
        child_v1.deployment_revision_id,
        "advancing the child retargeted an already published parent"
    );

    let parent_v2 = service_b
        .publish_graph(PUBLICATION_PARENT_AGENT_ID, parent_graph)
        .await
        .unwrap();
    assert_ne!(
        parent_v2.deployment_revision_id,
        parent_v1.deployment_revision_id
    );
    let final_catalog = repository_a.load_versioned_plan_catalog().await.unwrap();
    assert_eq!(
        frozen_subflow_deployment(&final_catalog, &parent_v1.deployment_revision_id),
        child_v1.deployment_revision_id
    );
    assert_eq!(
        frozen_subflow_deployment(&final_catalog, &parent_v2.deployment_revision_id),
        child_v2.deployment_revision_id
    );

    service_a.shutdown(Duration::from_secs(2)).await.unwrap();
    service_b.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(service_a);
    drop(service_b);
    drop(repository_a);
    drop(repository_b);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_stale_runtime_restores_exact_subflow_closure_and_takes_over_run() {
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        return;
    };
    let suffix = Uuid::new_v4().simple().to_string();
    let schema = format!("graph_takeover_{}", &suffix[..16]);
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let repository_a = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    repository_a.initialize_schema().await.unwrap();
    let repository_b = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    let control = PgPoolOptions::new()
        .max_connections(2)
        .connect(&scoped_url)
        .await
        .unwrap();
    let artifact_root = tempfile::tempdir().unwrap();
    let artifact_store = shared_artifact_store(
        artifact_root.path().join("objects"),
        "graph-product-takeover",
    )
    .await;

    // B starts before either immutable deployment exists. Its long pump
    // interval makes the later online restore deterministic in this test.
    let mut stale_runtime_config = config();
    stale_runtime_config.max_concurrent_runs = 2;
    stale_runtime_config.pump_interval = Duration::from_secs(60 * 60);
    let service_b = RunService::start_with_artifact_store_and_graph_publication(
        DeployedAgentCatalog::default(),
        repository_b.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        artifact_store.clone(),
        Arc::new(FixtureResolver),
        stale_runtime_config,
    )
    .await
    .unwrap();
    let service_a = RunService::start_with_artifact_store_and_graph_publication(
        DeployedAgentCatalog::default(),
        repository_a.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        artifact_store,
        Arc::new(FixtureResolver),
        config(),
    )
    .await
    .unwrap();

    let child_graph = takeover_child_graph();
    let child = service_a
        .publish_graph(TAKEOVER_CHILD_AGENT_ID, child_graph.clone())
        .await
        .unwrap();
    let parent_graph = takeover_parent_graph(&child.definition_revision_id);
    let parent = service_a
        .publish_graph(TAKEOVER_PARENT_AGENT_ID, parent_graph.clone())
        .await
        .unwrap();
    assert!(service_b
        .agents()
        .get_deployment(&child.deployment_revision_id)
        .is_none());
    assert!(service_b
        .agents()
        .get_deployment(&parent.deployment_revision_id)
        .is_none());

    let parent_run = service_a
        .create_detached(
            TAKEOVER_PARENT_AGENT_ID,
            json!({"question": "created on runtime A"}),
            RequestMetadata {
                request_id: Some("graph-takeover-parent-on-a".to_owned()),
            },
        )
        .await
        .unwrap();
    assert_eq!(parent_run.agent_version, parent.deployment_revision_id);

    let deadline = Instant::now() + Duration::from_secs(10);
    let (child_run_id, child_run_deployment): (String, String) = loop {
        let row = sqlx::query_as::<_, (String, String)>(
            "SELECT r.run_id,r.deployment_revision_id
             FROM workflow_runs r
             JOIN scheduler_wait_registrations w ON w.run_id=r.run_id
             WHERE r.parent_run_id=$1 AND r.lineage_kind='subflow'
               AND w.signal_name='takeover_reply' AND w.winner_kind IS NULL",
        )
        .bind(&parent_run.run_id)
        .fetch_optional(&control)
        .await
        .unwrap();
        if let Some(row) = row {
            break row;
        }
        assert!(
            Instant::now() < deadline,
            "runtime A did not durably reach the child signal wait"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(child_run_deployment, child.deployment_revision_id);

    service_a.shutdown(Duration::from_secs(2)).await.unwrap();

    // A read on stale B must restore the exact parent and its frozen child
    // dependency from one durable catalog snapshot before any scheduler work.
    let restored_graph = service_b.execution_graph(&parent_run.run_id).await.unwrap();
    assert_eq!(restored_graph.document_id(), parent_graph.document_id());
    assert!(service_b
        .agents()
        .get_deployment(&parent.deployment_revision_id)
        .is_some());
    assert!(service_b
        .agents()
        .get_deployment(&child.deployment_revision_id)
        .is_some());

    service_b.reconcile_startup().await.unwrap();
    service_b
        .signal(
            &child_run_id,
            "takeover_reply",
            "graph-takeover-signal-on-b",
            json!("completed by runtime B"),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        service_b.reconcile_startup().await.unwrap();
        let record = service_b.get_run(&parent_run.run_id).await.unwrap();
        if record.status().is_terminal() {
            assert_eq!(record.status(), RunStatus::Completed);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "runtime B did not complete the Run created by runtime A"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        service_b.get_run(&child_run_id).await.unwrap().status(),
        RunStatus::Completed
    );

    // Both takeover reservations must be released at terminal. With a limit
    // of two, two new waiting Runs are admitted and the third is rejected.
    let waiting_one = service_b
        .create_detached_from_deployment(
            TAKEOVER_CHILD_AGENT_ID,
            &child.deployment_revision_id,
            json!({"question": "capacity one"}),
            RequestMetadata {
                request_id: Some("graph-takeover-capacity-one".to_owned()),
            },
        )
        .await
        .unwrap();
    let waiting_two = service_b
        .create_detached(
            TAKEOVER_CHILD_AGENT_ID,
            json!({"question": "capacity two"}),
            RequestMetadata {
                request_id: Some("graph-takeover-capacity-two".to_owned()),
            },
        )
        .await
        .unwrap();
    let capacity = service_b
        .create_detached(
            TAKEOVER_CHILD_AGENT_ID,
            json!({"question": "capacity full"}),
            RequestMetadata {
                request_id: Some("graph-takeover-capacity-full".to_owned()),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(capacity.code(), "RUN_CAPACITY_EXCEEDED");
    service_b.cancel(&waiting_one.run_id).await.unwrap();
    service_b.cancel(&waiting_two.run_id).await.unwrap();

    service_b.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(service_a);
    drop(service_b);
    drop(repository_a);
    drop(repository_b);
    control.close().await;
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
