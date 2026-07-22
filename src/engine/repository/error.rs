use insight_engine::repository::adapter as repository_adapter;

use super::RepositoryError;

pub(crate) trait RepositoryErrorExt {
    fn canonicalization() -> Self;
    fn invalid_configuration() -> Self;
    fn invalid_data() -> Self;
}

impl RepositoryErrorExt for RepositoryError {
    fn canonicalization() -> Self {
        repository_adapter::canonicalization()
    }

    fn invalid_configuration() -> Self {
        repository_adapter::invalid_configuration()
    }

    fn invalid_data() -> Self {
        repository_adapter::invalid_data()
    }
}
