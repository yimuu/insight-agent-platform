//! Durable repository contract for the canvas authoring surface.
//!
//! Graph author semantics reuse the immutable
//! `workflow_definition_revisions.author_document` authority installed beside
//! the Canonical Plan. Only mutable [`ViewDocument`] state needs a separate
//! table. [`TraceOverlay`] is reconstructed on demand from activation
//! projections and is never persisted as another execution truth.

use async_trait::async_trait;
use insight_engine::{repository::RepositoryError, DefinitionRevisionId, RunId, TransitionOutcome};
use serde::Serialize;

use super::graph::{GraphAuthorDocument, GraphDocumentId, TraceOverlay, ViewDocument};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StoredGraphView {
    version: u64,
    document: ViewDocument,
}

impl StoredGraphView {
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

/// Workspace-internal constructors used by storage adapters.
#[doc(hidden)]
pub mod adapter {
    use super::{StoredGraphView, ViewDocument};

    pub fn stored_graph_view(version: u64, document: ViewDocument) -> StoredGraphView {
        StoredGraphView { version, document }
    }
}
