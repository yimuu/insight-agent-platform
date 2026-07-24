#[macro_use]
#[path = "../../../tests/support/workspace_assets.rs"]
mod workspace_assets;

use insight_dsl::{
    CompileOptions, GraphAuthorDocument, GraphDocumentId, GraphSurfaceRepository, NodeView,
    ViewDocument,
};
use insight_durable::{CreateRunCommand, DurableRepository, VersionedPlan};
use insight_engine::{
    DefinitionRevisionId, DeploymentRevisionId, RunId, TransitionKey, TransitionOutcome,
};
use insight_storage::{PostgresDurableRepository, SqliteDurableRepository};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe};
use uuid::Uuid;

fn graph() -> GraphAuthorDocument {
    let source = workspace_asset_str!("tests/fixtures/dsl/linear.yaml");
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("canvas_graph_repository").unwrap(),
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("canvas_definition_revision_1").unwrap(),
            "canvas/agent.yaml",
            source,
        ),
    )
    .unwrap()
}

fn versioned(graph: &GraphAuthorDocument) -> VersionedPlan {
    VersionedPlan::from_verified_graph(
        "canvas_definition",
        "canvas_agent",
        "Canvas agent",
        DeploymentRevisionId::new("canvas_deployment_revision_1").unwrap(),
        "cel-0.14+match-jcs-v1+value-jcs-v1",
        graph,
        json!([]),
        json!([]),
        json!([]),
    )
    .unwrap()
}

#[tokio::test]
async fn graph_author_view_and_trace_have_one_durable_authority_each() {
    let repository = SqliteDurableRepository::in_memory().await.unwrap();
    let graph = graph();
    let versioned = versioned(&graph);
    repository.install_versioned_plan(&versioned).await.unwrap();

    let loaded = repository
        .load_graph_author(
            "canvas_definition",
            graph.metadata().definition_revision_id(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded, graph);
    assert_eq!(loaded.semantic_hash(), graph.semantic_hash());

    let mut view = ViewDocument::new(graph.document_id().clone());
    view.set_node(graph.nodes()[0].id().clone(), NodeView::at(10.0, 20.0))
        .unwrap();
    let created = repository
        .save_graph_view("canvas_definition", &graph, 0, &view)
        .await
        .unwrap();
    assert!(matches!(
        created,
        TransitionOutcome::Committed { ref result } if result.version() == 1
    ));
    assert!(matches!(
        repository
            .save_graph_view("canvas_definition", &graph, 0, &view)
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { ref authoritative }
            if authoritative.version() == 1
    ));

    let mut updated = view.clone();
    updated
        .set_node(graph.nodes()[0].id().clone(), NodeView::at(50.0, 80.0))
        .unwrap();
    assert!(matches!(
        repository
            .save_graph_view("canvas_definition", &graph, 1, &updated)
            .await
            .unwrap(),
        TransitionOutcome::Committed { ref result } if result.version() == 2
    ));
    assert!(matches!(
        repository
            .save_graph_view("canvas_definition", &graph, 1, &view)
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    ));
    let loaded_view = repository
        .load_graph_view(
            "canvas_definition",
            graph.metadata().definition_revision_id(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded_view.version(), 2);
    assert_eq!(loaded_view.document(), &updated);
    assert_eq!(loaded.semantic_hash(), graph.semantic_hash());

    let run_id = RunId::new("canvas_trace_run_1").unwrap();
    repository
        .create_run(
            TransitionKey::derive("canvas.test", &["create-run"]).unwrap(),
            CreateRunCommand::new(run_id.clone(), &versioned, json!({"question": "hello"}))
                .unwrap(),
        )
        .await
        .unwrap();
    let trace = repository
        .load_trace_overlay(graph.document_id(), &run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(trace.graph_document_id(), graph.document_id());
    assert_eq!(trace.run_id(), &run_id);
    assert!(trace.activations().is_empty());
    assert!(repository
        .load_trace_overlay(&GraphDocumentId::new("different_canvas").unwrap(), &run_id,)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn postgres_graph_author_view_and_trace_contract_matches_sqlite() {
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        return;
    };
    let schema = format!("graph_surface_{}", Uuid::new_v4().simple());
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
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();

    let graph = graph();
    let versioned = versioned(&graph);
    repository.install_versioned_plan(&versioned).await.unwrap();
    assert_eq!(
        repository
            .load_graph_author(
                "canvas_definition",
                graph.metadata().definition_revision_id()
            )
            .await
            .unwrap()
            .unwrap(),
        graph
    );

    let mut view = ViewDocument::new(graph.document_id().clone());
    view.set_node(graph.nodes()[0].id().clone(), NodeView::at(1.0, 2.0))
        .unwrap();
    assert!(matches!(
        repository
            .save_graph_view("canvas_definition", &graph, 0, &view)
            .await
            .unwrap(),
        TransitionOutcome::Committed { ref result } if result.version() == 1
    ));
    assert_eq!(
        repository
            .load_graph_view(
                "canvas_definition",
                graph.metadata().definition_revision_id()
            )
            .await
            .unwrap()
            .unwrap()
            .document(),
        &view
    );

    let mut first_view = view.clone();
    first_view
        .set_node(graph.nodes()[0].id().clone(), NodeView::at(3.0, 4.0))
        .unwrap();
    let mut second_view = view.clone();
    second_view
        .set_node(graph.nodes()[0].id().clone(), NodeView::at(5.0, 6.0))
        .unwrap();
    let first_repository = repository.clone();
    let second_repository = repository.clone();
    let (first, second) = tokio::join!(
        first_repository.save_graph_view("canvas_definition", &graph, 1, &first_view),
        second_repository.save_graph_view("canvas_definition", &graph, 1, &second_view),
    );
    let outcomes = [first.unwrap(), second.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, TransitionOutcome::Committed { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, TransitionOutcome::StateConflict))
            .count(),
        1
    );

    let run_id = RunId::new("canvas_trace_run_pg_1").unwrap();
    repository
        .create_run(
            TransitionKey::derive("canvas.pg.test", &["create-run"]).unwrap(),
            CreateRunCommand::new(run_id.clone(), &versioned, json!({"question": "hello"}))
                .unwrap(),
        )
        .await
        .unwrap();
    let trace = repository
        .load_trace_overlay(graph.document_id(), &run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(trace.graph_document_id(), graph.document_id());
    assert!(trace.activations().is_empty());

    drop(repository);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}
