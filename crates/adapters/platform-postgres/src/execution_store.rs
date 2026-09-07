//! PostgreSQL implementation of the application execution port.
use crate::repository::{PgRepository, RepositoryError};
use async_trait::async_trait;
use insight_platform_contracts::CommandOutcome;
use insight_platform_jobs::store::*;
use insight_platform_orchestrator::store::*;
use insight_platform_runtime::*;
#[async_trait]
impl OrchestrationGenerationStore for PgRepository {
    async fn start_generation(
        &self,
        command: StartOrchestrationJob,
    ) -> Result<CommandOutcome<JobRecord>, GenerationStoreFailure> {
        let mut transaction = self
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        match transaction.start_orchestration_job(command).await {
            Ok(record) => {
                transaction
                    .commit()
                    .await
                    .map_err(classify_repository_failure)?;
                Ok(record)
            }
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                Err(classify_repository_failure(failure))
            }
        }
    }

    async fn heartbeat_generation(
        &self,
        command: HeartbeatJob,
    ) -> Result<JobRecord, GenerationStoreFailure> {
        let mut transaction = self
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        match transaction.heartbeat_orchestration_job(command).await {
            Ok(record) => {
                transaction
                    .commit()
                    .await
                    .map_err(classify_repository_failure)?;
                Ok(record)
            }
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                Err(classify_repository_failure(failure))
            }
        }
    }
}
fn classify_repository_failure(failure: RepositoryError) -> GenerationStoreFailure {
    match failure {
        RepositoryError::Database(_) | RepositoryError::CapacityUnavailable => {
            GenerationStoreFailure::Unavailable
        }
        RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired => GenerationStoreFailure::FenceLost,
        RepositoryError::InvalidInput(_)
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict
        | RepositoryError::PublicHistoryGap { .. }
        | RepositoryError::CorruptRow(_)
        | RepositoryError::InvalidPersistedObject(_) => GenerationStoreFailure::InvariantViolation,
    }
}
