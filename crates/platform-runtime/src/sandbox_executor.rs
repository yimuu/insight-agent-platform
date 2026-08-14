//! Controller-side PostgreSQL adapter for the dedicated Sandbox Executor claim port.

use async_trait::async_trait;
use insight_platform_postgres::repository::{PgRepository, RepositoryError};
use insight_platform_sandbox::{ClaimSandboxJobs, ClaimedSandboxJob, SandboxClaimAuthority};

#[derive(Clone)]
pub struct PgSandboxClaimAuthority {
    repository: PgRepository,
}

impl PgSandboxClaimAuthority {
    pub fn new(repository: PgRepository) -> Self {
        Self { repository }
    }
}

#[async_trait]
impl SandboxClaimAuthority for PgSandboxClaimAuthority {
    async fn claim_sandbox_jobs(
        &self,
        command: ClaimSandboxJobs,
    ) -> Result<Vec<ClaimedSandboxJob>, insight_platform_sandbox::SandboxClaimFailure> {
        self.repository
            .claim_sandbox_jobs(command)
            .await
            .map_err(classify_repository_failure)
    }
}

fn classify_repository_failure(
    failure: RepositoryError,
) -> insight_platform_sandbox::SandboxClaimFailure {
    use insight_platform_sandbox::SandboxClaimFailure;
    match failure {
        RepositoryError::Database(_) => SandboxClaimFailure::Unavailable,
        RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired => SandboxClaimFailure::FirstWinnerLost,
        RepositoryError::InvalidInput(_)
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict
        | RepositoryError::CorruptRow(_) => SandboxClaimFailure::InvariantViolation,
    }
}
