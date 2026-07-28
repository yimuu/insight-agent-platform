mod artifact_adapter;
mod error;
mod human_task_adapter;
mod ingress_adapter;
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
pub mod schema_contract;
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

pub(crate) fn database_time(value: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_micros(value.timestamp_micros())
        .expect("a valid DateTime always has a representable microsecond timestamp")
}

// The SQL adapters were historically sibling modules of these durable
// contracts. Keep their internal imports compact while the public owner of
// every contract remains `insight-durable`.
pub(crate) use insight_durable::*;

#[cfg(test)]
mod database_time_tests {
    use super::database_time;

    #[test]
    fn database_time_matches_sql_timestamp_precision() {
        let value = chrono::DateTime::parse_from_rfc3339("2026-07-23T18:35:37.123456789Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        assert_eq!(
            database_time(value).to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            "2026-07-23T18:35:37.123456000Z"
        );
    }
}
