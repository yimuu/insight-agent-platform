//! Durable repository contract for the canvas authoring surface.
//!
//! Graph author semantics reuse the immutable
//! `workflow_definition_revisions.author_document` authority installed beside
//! the Canonical Plan. Only mutable [`ViewDocument`] state needs a separate
//! table. [`TraceOverlay`] is reconstructed on demand from activation
//! projections and is never persisted as another execution truth.

use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use sqlx::Row;

use crate::engine::{
    repository::{PostgresDurableRepository, RepositoryError, SqliteDurableRepository},
    ActivationId, DefinitionRevisionId, DeploymentRevisionId, NodeId, Plan, RunId,
    TransitionOutcome,
};

use super::graph::{
    ActivationTrace, GraphAuthorDocument, GraphDocumentId, TraceActivationState, TraceOverlay,
    ViewDocument,
};

impl crate::engine::repository::VersionedPlan {
    /// Publication constructor that cannot pair one GraphAuthorDocument with
    /// an unrelated Canonical Plan. The immutable revision stores the explicit
    /// graph wire and the Plan rebuilt from that exact document.
    #[allow(clippy::too_many_arguments)]
    pub fn from_verified_graph(
        definition_id: impl Into<String>,
        agent_id: impl Into<String>,
        display_name: impl Into<String>,
        deployment_revision_id: DeploymentRevisionId,
        expression_engine_version: impl Into<String>,
        graph: &GraphAuthorDocument,
        descriptor_contracts: Value,
        resolved_bindings: Value,
        worker_contracts: Value,
    ) -> Result<Self, RepositoryError> {
        let bytes = graph
            .encode_json()
            .map_err(|_| RepositoryError::invalid_data())?;
        let author_document =
            serde_json::from_slice(&bytes).map_err(|_| RepositoryError::canonicalization())?;
        Self::from_verified_plan(
            definition_id,
            agent_id,
            display_name,
            deployment_revision_id,
            expression_engine_version,
            author_document,
            graph.plan(),
            descriptor_contracts,
            resolved_bindings,
            worker_contracts,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StoredGraphView {
    version: u64,
    document: ViewDocument,
}

impl StoredGraphView {
    fn new(version: u64, document: ViewDocument) -> Self {
        Self { version, document }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn document(&self) -> &ViewDocument {
        &self.document
    }

    pub fn into_document(self) -> ViewDocument {
        self.document
    }
}

/// Persistence boundary consumed by the formal Graph Author product service.
///
/// `expected_version == 0` creates the first View. Subsequent writes use the
/// last returned version. An exact retry is `ExactReplay`; a competing
/// write is `StateConflict` and cannot silently overwrite another editor.
#[async_trait]
pub trait GraphSurfaceRepository: Send + Sync {
    async fn load_graph_author(
        &self,
        definition_id: &str,
        definition_revision_id: &DefinitionRevisionId,
    ) -> Result<Option<GraphAuthorDocument>, RepositoryError>;

    async fn save_graph_view(
        &self,
        definition_id: &str,
        graph: &GraphAuthorDocument,
        expected_version: u64,
        view: &ViewDocument,
    ) -> Result<TransitionOutcome<StoredGraphView>, RepositoryError>;

    async fn load_graph_view(
        &self,
        definition_id: &str,
        definition_revision_id: &DefinitionRevisionId,
    ) -> Result<Option<StoredGraphView>, RepositoryError>;

    async fn load_trace_overlay(
        &self,
        graph_document_id: &GraphDocumentId,
        run_id: &RunId,
    ) -> Result<Option<TraceOverlay>, RepositoryError>;
}

fn invalid_data<T>() -> Result<T, RepositoryError> {
    Err(RepositoryError::invalid_data())
}

fn graph_from_values(
    author_document: Value,
    canonical_plan: Value,
    plan_hash: &str,
    expected_revision: &DefinitionRevisionId,
) -> Result<Option<GraphAuthorDocument>, RepositoryError> {
    if author_document
        .get("authoring_mode")
        .and_then(Value::as_str)
        != Some("graph")
    {
        return Ok(None);
    }
    let author_bytes =
        serde_json::to_vec(&author_document).map_err(|_| RepositoryError::invalid_data())?;
    let graph = GraphAuthorDocument::decode_json(&author_bytes)
        .map_err(|_| RepositoryError::invalid_data())?;
    let plan_bytes =
        serde_json::to_vec(&canonical_plan).map_err(|_| RepositoryError::invalid_data())?;
    let plan = Plan::decode_json(&plan_bytes).map_err(|_| RepositoryError::invalid_data())?;
    if graph.metadata().definition_revision_id() != expected_revision
        || graph.plan() != &plan
        || graph.semantic_hash().as_str() != plan_hash
    {
        return invalid_data();
    }
    Ok(Some(graph))
}

fn execution_graph_from_values(
    author_document: Value,
    canonical_plan: Value,
    plan_hash: &str,
    expected_revision: &DefinitionRevisionId,
) -> Result<GraphAuthorDocument, RepositoryError> {
    if let Some(graph) = graph_from_values(
        author_document,
        canonical_plan.clone(),
        plan_hash,
        expected_revision,
    )? {
        return Ok(graph);
    }
    let plan_bytes =
        serde_json::to_vec(&canonical_plan).map_err(|_| RepositoryError::invalid_data())?;
    let plan = Plan::decode_json(&plan_bytes).map_err(|_| RepositoryError::invalid_data())?;
    if plan.metadata().definition_revision_id() != expected_revision
        || plan.semantic_hash().as_str() != plan_hash
    {
        return Err(RepositoryError::invalid_data());
    }
    GraphAuthorDocument::from_execution_plan(plan).map_err(|_| RepositoryError::invalid_data())
}

fn decode_sqlite_execution_graph_row(
    row: &sqlx::sqlite::SqliteRow,
    revision: &DefinitionRevisionId,
) -> Result<GraphAuthorDocument, RepositoryError> {
    let author = row
        .try_get::<String, _>("author_document")
        .map_err(|_| RepositoryError::invalid_data())?;
    let plan = row
        .try_get::<String, _>("canonical_plan")
        .map_err(|_| RepositoryError::invalid_data())?;
    let hash = row
        .try_get::<String, _>("plan_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    execution_graph_from_values(
        serde_json::from_str(&author).map_err(|_| RepositoryError::invalid_data())?,
        serde_json::from_str(&plan).map_err(|_| RepositoryError::invalid_data())?,
        &hash,
        revision,
    )
}

fn decode_postgres_execution_graph_row(
    row: &sqlx::postgres::PgRow,
    revision: &DefinitionRevisionId,
) -> Result<GraphAuthorDocument, RepositoryError> {
    execution_graph_from_values(
        row.try_get("author_document")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("canonical_plan")
            .map_err(|_| RepositoryError::invalid_data())?,
        &row.try_get::<String, _>("plan_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
        revision,
    )
}

fn decode_sqlite_graph_row(
    row: &sqlx::sqlite::SqliteRow,
    revision: &DefinitionRevisionId,
) -> Result<Option<GraphAuthorDocument>, RepositoryError> {
    let author: String = row
        .try_get("author_document")
        .map_err(|_| RepositoryError::invalid_data())?;
    let plan: String = row
        .try_get("canonical_plan")
        .map_err(|_| RepositoryError::invalid_data())?;
    let hash: String = row
        .try_get("plan_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    graph_from_values(
        serde_json::from_str(&author).map_err(|_| RepositoryError::invalid_data())?,
        serde_json::from_str(&plan).map_err(|_| RepositoryError::invalid_data())?,
        &hash,
        revision,
    )
}

fn decode_postgres_graph_row(
    row: &sqlx::postgres::PgRow,
    revision: &DefinitionRevisionId,
) -> Result<Option<GraphAuthorDocument>, RepositoryError> {
    let author = row
        .try_get::<Value, _>("author_document")
        .map_err(|_| RepositoryError::invalid_data())?;
    let plan = row
        .try_get::<Value, _>("canonical_plan")
        .map_err(|_| RepositoryError::invalid_data())?;
    let hash = row
        .try_get::<String, _>("plan_hash")
        .map_err(|_| RepositoryError::invalid_data())?;
    graph_from_values(author, plan, &hash, revision)
}

fn decode_view_json(value: &[u8], version: i64) -> Result<StoredGraphView, RepositoryError> {
    let version = u64::try_from(version)
        .ok()
        .filter(|version| *version > 0)
        .ok_or_else(RepositoryError::invalid_data)?;
    let document = ViewDocument::decode_json(value).map_err(|_| RepositoryError::invalid_data())?;
    Ok(StoredGraphView::new(version, document))
}

fn trace_state(value: &str) -> Result<TraceActivationState, RepositoryError> {
    match value {
        "created" => Ok(TraceActivationState::Created),
        "ready" => Ok(TraceActivationState::Ready),
        "leased" => Ok(TraceActivationState::Leased),
        "running" => Ok(TraceActivationState::Running),
        "retry_wait" => Ok(TraceActivationState::RetryWait),
        "waiting" => Ok(TraceActivationState::Waiting),
        "terminating" => Ok(TraceActivationState::Terminating),
        "succeeded" => Ok(TraceActivationState::Succeeded),
        "failed" => Ok(TraceActivationState::Failed),
        "cancelled" => Ok(TraceActivationState::Cancelled),
        "timed_out" => Ok(TraceActivationState::TimedOut),
        _ => Err(RepositoryError::invalid_data()),
    }
}

fn attempt(value: Option<i64>) -> Result<Option<u32>, RepositoryError> {
    value
        .map(|value| {
            u32::try_from(value)
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(RepositoryError::invalid_data)
        })
        .transpose()
}

#[async_trait]
impl GraphSurfaceRepository for SqliteDurableRepository {
    async fn load_graph_author(
        &self,
        definition_id: &str,
        definition_revision_id: &DefinitionRevisionId,
    ) -> Result<Option<GraphAuthorDocument>, RepositoryError> {
        let row = sqlx::query(
            "SELECT author_document, canonical_plan, plan_hash
             FROM workflow_definition_revisions
             WHERE definition_id = ? AND definition_revision_id = ?",
        )
        .bind(definition_id)
        .bind(definition_revision_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| decode_sqlite_graph_row(&row, definition_revision_id))
            .transpose()
            .map(Option::flatten)
    }

    async fn save_graph_view(
        &self,
        definition_id: &str,
        graph: &GraphAuthorDocument,
        expected_version: u64,
        view: &ViewDocument,
    ) -> Result<TransitionOutcome<StoredGraphView>, RepositoryError> {
        graph
            .validate()
            .and_then(|()| view.validate_against(graph))
            .map_err(|_| RepositoryError::invalid_data())?;
        let revision = graph.metadata().definition_revision_id();
        let encoded = view
            .encode_json()
            .map_err(|_| RepositoryError::invalid_data())?;
        let encoded = String::from_utf8(encoded).map_err(|_| RepositoryError::invalid_data())?;
        let expected = i64::try_from(expected_version)
            .map_err(|_| RepositoryError::invalid_configuration())?;
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;

        let author_row = sqlx::query(
            "SELECT author_document, canonical_plan, plan_hash
             FROM workflow_definition_revisions
             WHERE definition_id = ? AND definition_revision_id = ?",
        )
        .bind(definition_id)
        .bind(revision.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(author_row) = author_row else {
            return Ok(TransitionOutcome::StateConflict);
        };
        let Some(stored_graph) = decode_sqlite_graph_row(&author_row, revision)? else {
            return Ok(TransitionOutcome::StateConflict);
        };
        if &stored_graph != graph {
            return Ok(TransitionOutcome::StateConflict);
        }

        let row = sqlx::query(
            "SELECT view_version, view_document FROM graph_view_documents
             WHERE definition_id = ? AND definition_revision_id = ?",
        )
        .bind(definition_id)
        .bind(revision.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let outcome = match row {
            Some(row) => {
                let current = decode_view_json(
                    row.try_get::<String, _>("view_document")
                        .map_err(|_| RepositoryError::invalid_data())?
                        .as_bytes(),
                    row.try_get::<i64, _>("view_version")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?;
                if current.document == *view
                    && (current.version == expected_version
                        || current.version == expected_version.saturating_add(1))
                {
                    TransitionOutcome::ExactReplay {
                        authoritative: current,
                    }
                } else if current.version != expected_version {
                    TransitionOutcome::StateConflict
                } else {
                    let next = expected
                        .checked_add(1)
                        .ok_or_else(RepositoryError::invalid_configuration)?;
                    sqlx::query(
                        "UPDATE graph_view_documents
                         SET view_version = ?, view_document = ?, updated_at = CURRENT_TIMESTAMP
                         WHERE definition_id = ? AND definition_revision_id = ?
                           AND view_version = ?",
                    )
                    .bind(next)
                    .bind(&encoded)
                    .bind(definition_id)
                    .bind(revision.as_str())
                    .bind(expected)
                    .execute(&mut *transaction)
                    .await
                    .map_err(RepositoryError::storage)?;
                    TransitionOutcome::Committed {
                        result: StoredGraphView::new(
                            u64::try_from(next).expect("positive view version"),
                            view.clone(),
                        ),
                    }
                }
            }
            None if expected_version == 0 => {
                sqlx::query(
                    "INSERT INTO graph_view_documents (
                        definition_id, definition_revision_id, graph_document_id,
                        view_version, view_document, updated_at
                     ) VALUES (?, ?, ?, 1, ?, CURRENT_TIMESTAMP)",
                )
                .bind(definition_id)
                .bind(revision.as_str())
                .bind(graph.document_id().as_str())
                .bind(&encoded)
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
                TransitionOutcome::Committed {
                    result: StoredGraphView::new(1, view.clone()),
                }
            }
            None => TransitionOutcome::StateConflict,
        };
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(outcome)
    }

    async fn load_graph_view(
        &self,
        definition_id: &str,
        definition_revision_id: &DefinitionRevisionId,
    ) -> Result<Option<StoredGraphView>, RepositoryError> {
        let row = sqlx::query(
            "SELECT v.view_version, v.view_document,
                    r.author_document, r.canonical_plan, r.plan_hash
             FROM graph_view_documents v
             JOIN workflow_definition_revisions r
               ON r.definition_id = v.definition_id
              AND r.definition_revision_id = v.definition_revision_id
             WHERE v.definition_id = ? AND v.definition_revision_id = ?",
        )
        .bind(definition_id)
        .bind(definition_revision_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let Some(graph) = decode_sqlite_graph_row(&row, definition_revision_id)? else {
            return invalid_data();
        };
        let stored = decode_view_json(
            row.try_get::<String, _>("view_document")
                .map_err(|_| RepositoryError::invalid_data())?
                .as_bytes(),
            row.try_get::<i64, _>("view_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        stored
            .document
            .validate_against(&graph)
            .map_err(|_| RepositoryError::invalid_data())?;
        Ok(Some(stored))
    }

    async fn load_trace_overlay(
        &self,
        graph_document_id: &GraphDocumentId,
        run_id: &RunId,
    ) -> Result<Option<TraceOverlay>, RepositoryError> {
        let row = sqlx::query(
            "SELECT r.definition_revision_id,
                    d.author_document, d.canonical_plan, d.plan_hash
             FROM workflow_runs r
             JOIN workflow_definition_revisions d
               ON d.definition_id = r.definition_id
              AND d.definition_revision_id = r.definition_revision_id
             WHERE r.run_id = ?",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let revision = DefinitionRevisionId::new(
            row.try_get::<String, _>("definition_revision_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let graph = decode_sqlite_execution_graph_row(&row, &revision)?;
        if graph.document_id() != graph_document_id {
            return Ok(None);
        }
        let rows = sqlx::query(
            "SELECT activation_id, node_id, last_attempt_no, lifecycle
             FROM node_activations WHERE run_id = ? ORDER BY activation_id",
        )
        .bind(run_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut overlay = TraceOverlay::new(graph_document_id.clone(), run_id.clone());
        for row in rows {
            overlay
                .add_activation(ActivationTrace::new(
                    ActivationId::new(
                        row.try_get::<String, _>("activation_id")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )
                    .map_err(|_| RepositoryError::invalid_data())?,
                    NodeId::new(
                        row.try_get::<String, _>("node_id")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )
                    .map_err(|_| RepositoryError::invalid_data())?,
                    attempt(
                        row.try_get::<Option<i64>, _>("last_attempt_no")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )?,
                    trace_state(
                        &row.try_get::<String, _>("lifecycle")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )?,
                ))
                .map_err(|_| RepositoryError::invalid_data())?;
        }
        overlay
            .validate_against(&graph)
            .map_err(|_| RepositoryError::invalid_data())?;
        Ok(Some(overlay))
    }
}

#[async_trait]
impl GraphSurfaceRepository for PostgresDurableRepository {
    async fn load_graph_author(
        &self,
        definition_id: &str,
        definition_revision_id: &DefinitionRevisionId,
    ) -> Result<Option<GraphAuthorDocument>, RepositoryError> {
        let row = sqlx::query(
            "SELECT author_document, canonical_plan, plan_hash
             FROM workflow_definition_revisions
             WHERE definition_id = $1 AND definition_revision_id = $2",
        )
        .bind(definition_id)
        .bind(definition_revision_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| decode_postgres_graph_row(&row, definition_revision_id))
            .transpose()
            .map(Option::flatten)
    }

    async fn save_graph_view(
        &self,
        definition_id: &str,
        graph: &GraphAuthorDocument,
        expected_version: u64,
        view: &ViewDocument,
    ) -> Result<TransitionOutcome<StoredGraphView>, RepositoryError> {
        graph
            .validate()
            .and_then(|()| view.validate_against(graph))
            .map_err(|_| RepositoryError::invalid_data())?;
        let revision = graph.metadata().definition_revision_id();
        let encoded_bytes = view
            .encode_json()
            .map_err(|_| RepositoryError::invalid_data())?;
        let encoded = serde_json::from_slice::<Value>(&encoded_bytes)
            .map_err(|_| RepositoryError::invalid_data())?;
        let expected = i64::try_from(expected_version)
            .map_err(|_| RepositoryError::invalid_configuration())?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let author_row = sqlx::query(
            "SELECT author_document, canonical_plan, plan_hash
             FROM workflow_definition_revisions
             WHERE definition_id = $1 AND definition_revision_id = $2 FOR UPDATE",
        )
        .bind(definition_id)
        .bind(revision.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(author_row) = author_row else {
            return Ok(TransitionOutcome::StateConflict);
        };
        let Some(stored_graph) = decode_postgres_graph_row(&author_row, revision)? else {
            return Ok(TransitionOutcome::StateConflict);
        };
        if &stored_graph != graph {
            return Ok(TransitionOutcome::StateConflict);
        }

        let row = sqlx::query(
            "SELECT view_version, view_document FROM graph_view_documents
             WHERE definition_id = $1 AND definition_revision_id = $2 FOR UPDATE",
        )
        .bind(definition_id)
        .bind(revision.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let outcome = match row {
            Some(row) => {
                let stored_json = row
                    .try_get::<Value, _>("view_document")
                    .map_err(|_| RepositoryError::invalid_data())?;
                let stored_bytes = serde_json::to_vec(&stored_json)
                    .map_err(|_| RepositoryError::invalid_data())?;
                let current = decode_view_json(
                    &stored_bytes,
                    row.try_get::<i64, _>("view_version")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?;
                if current.document == *view
                    && (current.version == expected_version
                        || current.version == expected_version.saturating_add(1))
                {
                    TransitionOutcome::ExactReplay {
                        authoritative: current,
                    }
                } else if current.version != expected_version {
                    TransitionOutcome::StateConflict
                } else {
                    let next = expected
                        .checked_add(1)
                        .ok_or_else(RepositoryError::invalid_configuration)?;
                    sqlx::query(
                        "UPDATE graph_view_documents
                         SET view_version = $1, view_document = $2, updated_at = CURRENT_TIMESTAMP
                         WHERE definition_id = $3 AND definition_revision_id = $4
                           AND view_version = $5",
                    )
                    .bind(next)
                    .bind(&encoded)
                    .bind(definition_id)
                    .bind(revision.as_str())
                    .bind(expected)
                    .execute(&mut *transaction)
                    .await
                    .map_err(RepositoryError::storage)?;
                    TransitionOutcome::Committed {
                        result: StoredGraphView::new(
                            u64::try_from(next).expect("positive view version"),
                            view.clone(),
                        ),
                    }
                }
            }
            None if expected_version == 0 => {
                sqlx::query(
                    "INSERT INTO graph_view_documents (
                        definition_id, definition_revision_id, graph_document_id,
                        view_version, view_document, updated_at
                     ) VALUES ($1, $2, $3, 1, $4, CURRENT_TIMESTAMP)",
                )
                .bind(definition_id)
                .bind(revision.as_str())
                .bind(graph.document_id().as_str())
                .bind(&encoded)
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
                TransitionOutcome::Committed {
                    result: StoredGraphView::new(1, view.clone()),
                }
            }
            None => TransitionOutcome::StateConflict,
        };
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(outcome)
    }

    async fn load_graph_view(
        &self,
        definition_id: &str,
        definition_revision_id: &DefinitionRevisionId,
    ) -> Result<Option<StoredGraphView>, RepositoryError> {
        let row = sqlx::query(
            "SELECT v.view_version, v.view_document,
                    r.author_document, r.canonical_plan, r.plan_hash
             FROM graph_view_documents v
             JOIN workflow_definition_revisions r
               ON r.definition_id = v.definition_id
              AND r.definition_revision_id = v.definition_revision_id
             WHERE v.definition_id = $1 AND v.definition_revision_id = $2",
        )
        .bind(definition_id)
        .bind(definition_revision_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let Some(graph) = decode_postgres_graph_row(&row, definition_revision_id)? else {
            return invalid_data();
        };
        let view_json = row
            .try_get::<Value, _>("view_document")
            .map_err(|_| RepositoryError::invalid_data())?;
        let view_bytes =
            serde_json::to_vec(&view_json).map_err(|_| RepositoryError::invalid_data())?;
        let stored = decode_view_json(
            &view_bytes,
            row.try_get::<i64, _>("view_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?;
        stored
            .document
            .validate_against(&graph)
            .map_err(|_| RepositoryError::invalid_data())?;
        Ok(Some(stored))
    }

    async fn load_trace_overlay(
        &self,
        graph_document_id: &GraphDocumentId,
        run_id: &RunId,
    ) -> Result<Option<TraceOverlay>, RepositoryError> {
        let row = sqlx::query(
            "SELECT r.definition_revision_id,
                    d.author_document, d.canonical_plan, d.plan_hash
             FROM workflow_runs r
             JOIN workflow_definition_revisions d
               ON d.definition_id = r.definition_id
              AND d.definition_revision_id = r.definition_revision_id
             WHERE r.run_id = $1",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let revision = DefinitionRevisionId::new(
            row.try_get::<String, _>("definition_revision_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        let graph = decode_postgres_execution_graph_row(&row, &revision)?;
        if graph.document_id() != graph_document_id {
            return Ok(None);
        }
        let rows = sqlx::query(
            "SELECT activation_id, node_id, last_attempt_no, lifecycle
             FROM node_activations WHERE run_id = $1 ORDER BY activation_id",
        )
        .bind(run_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut overlay = TraceOverlay::new(graph_document_id.clone(), run_id.clone());
        for row in rows {
            overlay
                .add_activation(ActivationTrace::new(
                    ActivationId::new(
                        row.try_get::<String, _>("activation_id")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )
                    .map_err(|_| RepositoryError::invalid_data())?,
                    NodeId::new(
                        row.try_get::<String, _>("node_id")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )
                    .map_err(|_| RepositoryError::invalid_data())?,
                    attempt(
                        row.try_get::<Option<i32>, _>("last_attempt_no")
                            .map_err(|_| RepositoryError::invalid_data())?
                            .map(i64::from),
                    )?,
                    trace_state(
                        &row.try_get::<String, _>("lifecycle")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )?,
                ))
                .map_err(|_| RepositoryError::invalid_data())?;
        }
        overlay
            .validate_against(&graph)
            .map_err(|_| RepositoryError::invalid_data())?;
        Ok(Some(overlay))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_projection_covers_every_durable_activation_lifecycle_without_lossy_aliases() {
        let cases = [
            ("created", TraceActivationState::Created),
            ("ready", TraceActivationState::Ready),
            ("leased", TraceActivationState::Leased),
            ("running", TraceActivationState::Running),
            ("retry_wait", TraceActivationState::RetryWait),
            ("waiting", TraceActivationState::Waiting),
            ("terminating", TraceActivationState::Terminating),
            ("succeeded", TraceActivationState::Succeeded),
            ("failed", TraceActivationState::Failed),
            ("cancelled", TraceActivationState::Cancelled),
            ("timed_out", TraceActivationState::TimedOut),
        ];
        for (wire, expected) in cases {
            assert_eq!(trace_state(wire).unwrap(), expected);
        }
        assert!(trace_state("unknown").is_err());
        assert_eq!(attempt(None).unwrap(), None);
        assert_eq!(attempt(Some(1)).unwrap(), Some(1));
        assert!(attempt(Some(0)).is_err());
    }
}
