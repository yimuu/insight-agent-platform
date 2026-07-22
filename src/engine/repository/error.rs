pub use insight_engine::repository::{
    RepositoryError, StorageLocator, REPOSITORY_ACTIVATION_NOT_FOUND,
    REPOSITORY_ARTIFACT_STORE_CONFLICT, REPOSITORY_CANONICALIZATION_FAILED,
    REPOSITORY_CONFIGURATION_INVALID, REPOSITORY_CONSTRAINT_CONFLICT, REPOSITORY_DATA_INVALID,
    REPOSITORY_INTENT_CONFLICT, REPOSITORY_MIGRATION_FAILED, REPOSITORY_PLAN_CONFLICT,
    REPOSITORY_REDRIVE_REQUIRES_FORK, REPOSITORY_RUN_MIGRATING, REPOSITORY_RUN_NOT_FOUND,
    REPOSITORY_SCHEDULER_ACTION_UNSUPPORTED, REPOSITORY_SCHEDULER_CRASH_INJECTED,
    REPOSITORY_STORAGE_FAILURE,
};

use insight_engine::repository::adapter as repository_adapter;

pub(crate) trait RepositoryErrorExt {
    fn new(code: &'static str, message: &'static str) -> Self;
    fn storage(error: sqlx::Error) -> Self;
    fn canonicalization() -> Self;
    fn invalid_configuration() -> Self;
    fn invalid_data() -> Self;
    fn scheduler_crash_injected() -> Self;
    fn intent_conflict() -> Self;
    fn activation_not_found() -> Self;
    fn run_migrating() -> Self;
    fn migration_failed() -> Self;
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

    fn canonicalization() -> Self {
        repository_adapter::canonicalization()
    }

    fn invalid_configuration() -> Self {
        repository_adapter::invalid_configuration()
    }

    fn invalid_data() -> Self {
        repository_adapter::invalid_data()
    }

    fn scheduler_crash_injected() -> Self {
        repository_adapter::scheduler_crash_injected()
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

    fn migration_failed() -> Self {
        repository_adapter::migration_failed()
    }

    fn redrive_requires_fork() -> Self {
        repository_adapter::redrive_requires_fork()
    }
}
