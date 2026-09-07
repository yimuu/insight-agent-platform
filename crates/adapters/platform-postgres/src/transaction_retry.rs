//! Only explicit PostgreSQL transaction aborts permit bounded replay of an allocated command.
use crate::repository::RepositoryError;
pub(crate) const MAXIMUM_ATTEMPTS: u32 = 8;
pub(crate) fn is_retryable_postgres_transaction_abort(failure: &RepositoryError) -> bool {
    matches!(failure, RepositoryError::Database(sqlx::Error::Database(database))
        if matches!(database.code().as_deref(), Some("40001" | "40P01")))
}
