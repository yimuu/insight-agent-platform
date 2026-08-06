//! PostgreSQL and SQLite adapters for the backend-neutral terminal-only
//! storage contracts owned by `insight-durable`.

mod postgres;
mod sqlite;
pub(crate) mod staging_postgres;
pub(crate) mod staging_sqlite;

use insight_engine::{repository::RepositoryError, ArtifactRef};

pub use insight_durable::terminal_store::*;

fn invalid_data() -> RepositoryError {
    insight_engine::repository::adapter::invalid_data()
}

fn artifact_from_content_ref(content_ref: &str) -> Result<ArtifactRef, RepositoryError> {
    if let Ok(reference) = ScopedArtifactReference::parse(content_ref) {
        return Ok(reference.artifact().clone());
    }
    serde_json::from_str(content_ref).map_err(|_| invalid_data())
}
