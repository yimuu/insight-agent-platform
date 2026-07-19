use std::{error::Error, fmt};

pub const REPOSITORY_CONFIGURATION_INVALID: &str = "ENGINE_REPOSITORY_CONFIGURATION_INVALID";
pub const REPOSITORY_CANONICALIZATION_FAILED: &str = "ENGINE_REPOSITORY_CANONICALIZATION_FAILED";
pub const REPOSITORY_PLAN_CONFLICT: &str = "ENGINE_REPOSITORY_PLAN_CONFLICT";
pub const REPOSITORY_RUN_NOT_FOUND: &str = "ENGINE_REPOSITORY_RUN_NOT_FOUND";
pub const REPOSITORY_RUN_MIGRATING: &str = "RUN_MIGRATING";
pub const REPOSITORY_ACTIVATION_NOT_FOUND: &str = "ENGINE_REPOSITORY_ACTIVATION_NOT_FOUND";
pub const REPOSITORY_CONSTRAINT_CONFLICT: &str = "ENGINE_REPOSITORY_CONSTRAINT_CONFLICT";
pub const REPOSITORY_INTENT_CONFLICT: &str = "ENGINE_REPOSITORY_INTENT_CONFLICT";
pub const REPOSITORY_DATA_INVALID: &str = "ENGINE_REPOSITORY_DATA_INVALID";
pub const REPOSITORY_STORAGE_FAILURE: &str = "ENGINE_REPOSITORY_STORAGE_FAILURE";
pub const REPOSITORY_MIGRATION_FAILED: &str = "ENGINE_REPOSITORY_MIGRATION_FAILED";
pub const REPOSITORY_ARTIFACT_STORE_CONFLICT: &str = "ENGINE_REPOSITORY_ARTIFACT_STORE_CONFLICT";
pub const REPOSITORY_SCHEDULER_ACTION_UNSUPPORTED: &str =
    "ENGINE_REPOSITORY_SCHEDULER_ACTION_UNSUPPORTED";
pub const REPOSITORY_SCHEDULER_CRASH_INJECTED: &str = "ENGINE_REPOSITORY_SCHEDULER_CRASH_INJECTED";
pub const REPOSITORY_REDRIVE_REQUIRES_FORK: &str = "ENGINE_REDRIVE_REQUIRES_FORK";

/// Stable, body-free repository failure.
///
/// Database diagnostics are deliberately not retained: constraint details can
/// contain IDs or payload fragments and must not cross the durable-kernel API.
#[derive(Clone, PartialEq, Eq)]
pub struct RepositoryError {
    code: &'static str,
    message: &'static str,
}

impl RepositoryError {
    pub(crate) const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &'static str {
        self.message
    }

    pub(crate) fn storage(error: sqlx::Error) -> Self {
        if error
            .as_database_error()
            .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
        {
            return Self::new(
                REPOSITORY_CONSTRAINT_CONFLICT,
                "durable repository constraint conflict",
            );
        }
        Self::new(
            REPOSITORY_STORAGE_FAILURE,
            "durable repository operation failed",
        )
    }

    pub(crate) const fn canonicalization() -> Self {
        Self::new(
            REPOSITORY_CANONICALIZATION_FAILED,
            "repository intent could not be canonicalized",
        )
    }

    pub(crate) const fn invalid_configuration() -> Self {
        Self::new(
            REPOSITORY_CONFIGURATION_INVALID,
            "durable repository configuration is invalid",
        )
    }

    pub(crate) const fn invalid_data() -> Self {
        Self::new(
            REPOSITORY_DATA_INVALID,
            "durable repository data is invalid",
        )
    }

    pub(crate) const fn scheduler_crash_injected() -> Self {
        Self::new(
            REPOSITORY_SCHEDULER_CRASH_INJECTED,
            "scheduler crash injection interrupted execution",
        )
    }

    pub(crate) const fn intent_conflict() -> Self {
        Self::new(
            REPOSITORY_INTENT_CONFLICT,
            "transition key is already bound to a different canonical intent",
        )
    }

    pub(crate) const fn activation_not_found() -> Self {
        Self::new(
            REPOSITORY_ACTIVATION_NOT_FOUND,
            "durable activation was not found",
        )
    }

    pub(crate) const fn run_migrating() -> Self {
        Self::new(
            REPOSITORY_RUN_MIGRATING,
            "workflow run is migrating and no longer accepts signals",
        )
    }

    pub(crate) const fn migration_failed() -> Self {
        Self::new(
            REPOSITORY_MIGRATION_FAILED,
            "durable PostgreSQL migration authority is invalid",
        )
    }

    pub(crate) const fn redrive_requires_fork() -> Self {
        Self::new(
            REPOSITORY_REDRIVE_REQUIRES_FORK,
            "redrive is unsafe for a non-idempotent effect and requires a fork",
        )
    }
}

impl fmt::Debug for RepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RepositoryError")
            .field("code", &self.code)
            .field("message", &self.message)
            .finish()
    }
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for RepositoryError {}
