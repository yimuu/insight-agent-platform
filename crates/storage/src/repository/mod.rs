mod artifact_adapter;
mod error;
mod human_task_adapter;
mod ingress_adapter;
pub mod migration_manifest;
mod model;
mod model_tool_parent_resume;
mod postgres;
mod postgres_activation;
mod postgres_control;
mod postgres_model_tool_queue;
mod postgres_projection;
mod postgres_recovery;
mod postgres_retrieval_publication;
mod postgres_scheduler;
mod public_outbox_adapter;
#[cfg(test)]
mod recovery_repository;
#[cfg(test)]
mod retrieval_safety_tests;
mod sqlite;
mod sqlite_activation;
mod sqlite_control;
mod sqlite_model_tool_queue;
mod sqlite_projection;
mod sqlite_recovery;
mod sqlite_retrieval_publication;
mod sqlite_scheduler;

pub use postgres::PostgresDurableRepository;
pub use sqlite::SqliteDurableRepository;

pub(crate) use error::RepositoryErrorExt;

// The SQL adapters were historically sibling modules of these durable
// contracts. Keep their internal imports compact while the public owner of
// every contract remains `insight-durable`.
pub(crate) use insight_durable::*;
