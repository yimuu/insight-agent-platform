//! PostgreSQL and SQLite adapters for the backend-neutral terminal-only
//! storage contracts owned by `insight-durable`.

mod postgres;
mod sqlite;
pub(crate) mod staging_postgres;
pub(crate) mod staging_sqlite;

use insight_engine::repository::RepositoryError;

pub use insight_durable::terminal_store::*;

fn invalid_data() -> RepositoryError {
    insight_engine::repository::adapter::invalid_data()
}
