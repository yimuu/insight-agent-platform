use insight_engine::repository::{
    RepositoryError, REPOSITORY_CONSTRAINT_CONFLICT, REPOSITORY_STORAGE_FAILURE,
};

use insight_engine::repository::adapter as repository_adapter;

pub(crate) trait RepositoryErrorExt {
    fn new(code: &'static str, message: &'static str) -> Self;
    fn storage(error: sqlx::Error) -> Self;
    fn storage_unavailable() -> Self;
    fn canonicalization() -> Self;
    fn invalid_configuration() -> Self;
    fn invalid_data() -> Self;
    fn intent_conflict() -> Self;
    fn activation_not_found() -> Self;
    fn run_migrating() -> Self;
    fn schema_not_initialized() -> Self;
    fn schema_contract_mismatch() -> Self;
    fn schema_backend_mismatch() -> Self;
    fn redrive_requires_fork() -> Self;
}

impl RepositoryErrorExt for RepositoryError {
    fn new(code: &'static str, message: &'static str) -> Self {
        repository_adapter::repository_error(code, message)
    }

    fn storage(error: sqlx::Error) -> Self {
        if error
            .as_database_error()
            .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
        {
            return repository_adapter::repository_error(
                REPOSITORY_CONSTRAINT_CONFLICT,
                "durable repository constraint conflict",
            );
        }
        repository_adapter::repository_error(
            REPOSITORY_STORAGE_FAILURE,
            "durable repository operation failed",
        )
    }

    fn storage_unavailable() -> Self {
        repository_adapter::repository_error(
            REPOSITORY_STORAGE_FAILURE,
            "durable repository operation failed",
        )
    }

    fn canonicalization() -> Self {
        repository_adapter::canonicalization()
    }

    fn invalid_configuration() -> Self {
        repository_adapter::invalid_configuration()
    }

    fn invalid_data() -> Self {
        repository_adapter::invalid_data()
    }

    fn intent_conflict() -> Self {
        repository_adapter::intent_conflict()
    }

    fn activation_not_found() -> Self {
        repository_adapter::activation_not_found()
    }

    fn run_migrating() -> Self {
        repository_adapter::run_migrating()
    }

    fn schema_not_initialized() -> Self {
        repository_adapter::repository_error(
            super::schema_contract::DATABASE_SCHEMA_NOT_INITIALIZED,
            "durable database Schema is not initialized",
        )
    }

    fn schema_contract_mismatch() -> Self {
        repository_adapter::repository_error(
            super::schema_contract::DATABASE_SCHEMA_CONTRACT_MISMATCH,
            "durable database Schema contract does not match this service",
        )
    }

    fn schema_backend_mismatch() -> Self {
        repository_adapter::repository_error(
            super::schema_contract::DATABASE_SCHEMA_BACKEND_MISMATCH,
            "durable database Schema backend does not match this repository",
        )
    }

    fn redrive_requires_fork() -> Self {
        repository_adapter::redrive_requires_fork()
    }
}
