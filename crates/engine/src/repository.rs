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

const MAX_STORAGE_LOCATOR_BYTES: usize = 16 * 1024;

/// Stable, body-free repository failure.
///
/// Adapter diagnostics are deliberately not retained: database and I/O error
/// details can contain identifiers or payload fragments and must not cross a
/// repository or object-store port.
#[derive(Clone, PartialEq, Eq)]
pub struct RepositoryError {
    code: &'static str,
    message: &'static str,
}

impl RepositoryError {
    const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &'static str {
        self.message
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

/// A storage locator is intentionally neither serializable nor printable.
///
/// Locators can contain bucket topology or short-lived credentials. They are
/// persisted privately and returned only to object-store adapters; they have
/// no representation in public execution events.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageLocator(String);

impl StorageLocator {
    pub fn new(value: impl Into<String>) -> Result<Self, RepositoryError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_STORAGE_LOCATOR_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(RepositoryError::invalid_configuration());
        }
        Ok(Self(value))
    }

    pub fn expose_to_storage_adapter(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for StorageLocator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StorageLocator(<redacted>)")
    }
}

/// Workspace-internal constructors used by durable and storage adapters.
#[doc(hidden)]
pub mod adapter {
    use super::{
        RepositoryError, StorageLocator, REPOSITORY_ACTIVATION_NOT_FOUND,
        REPOSITORY_INTENT_CONFLICT, REPOSITORY_MIGRATION_FAILED, REPOSITORY_REDRIVE_REQUIRES_FORK,
        REPOSITORY_RUN_MIGRATING, REPOSITORY_SCHEDULER_CRASH_INJECTED,
    };

    pub const fn repository_error(code: &'static str, message: &'static str) -> RepositoryError {
        RepositoryError::new(code, message)
    }

    pub const fn canonicalization() -> RepositoryError {
        RepositoryError::canonicalization()
    }

    pub const fn invalid_configuration() -> RepositoryError {
        RepositoryError::invalid_configuration()
    }

    pub const fn invalid_data() -> RepositoryError {
        RepositoryError::invalid_data()
    }

    pub const fn scheduler_crash_injected() -> RepositoryError {
        RepositoryError::new(
            REPOSITORY_SCHEDULER_CRASH_INJECTED,
            "scheduler crash injection interrupted execution",
        )
    }

    pub const fn intent_conflict() -> RepositoryError {
        RepositoryError::new(
            REPOSITORY_INTENT_CONFLICT,
            "transition key is already bound to a different canonical intent",
        )
    }

    pub const fn activation_not_found() -> RepositoryError {
        RepositoryError::new(
            REPOSITORY_ACTIVATION_NOT_FOUND,
            "durable activation was not found",
        )
    }

    pub const fn run_migrating() -> RepositoryError {
        RepositoryError::new(
            REPOSITORY_RUN_MIGRATING,
            "workflow run is migrating and no longer accepts signals",
        )
    }

    pub const fn migration_failed() -> RepositoryError {
        RepositoryError::new(
            REPOSITORY_MIGRATION_FAILED,
            "durable PostgreSQL migration authority is invalid",
        )
    }

    pub const fn redrive_requires_fork() -> RepositoryError {
        RepositoryError::new(
            REPOSITORY_REDRIVE_REQUIRES_FORK,
            "redrive is unsafe for a non-idempotent effect and requires a fork",
        )
    }

    pub fn storage_locator_from_validated_parts(value: String) -> StorageLocator {
        StorageLocator(value)
    }
}
