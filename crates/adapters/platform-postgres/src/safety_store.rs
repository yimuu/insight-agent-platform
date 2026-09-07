//! PostgreSQL implementation of the application safety port.
use crate::repository::{PgRepository, RepositoryError};
use async_trait::async_trait;
use insight_platform_artifacts::store::*;
use insight_platform_jobs::store::*;
use insight_platform_orchestrator::store::*;
use insight_platform_runtime::*;
#[async_trait]
impl OrchestrationSafetyStore for PgRepository {
    type Error = RepositoryError;

    async fn drive_expired_jobs(
        &self,
        command: DriveExpiredOrchestrationJobs,
    ) -> Result<SafetyScanPage<RecoveredOrchestrationJob>, Self::Error> {
        let mut transaction = self.begin_scheduler_transaction().await?;
        let page = transaction
            .drive_expired_orchestration_jobs(command)
            .await?;
        transaction.commit().await?;
        Ok(page)
    }

    async fn drive_due_retries(
        &self,
        command: DriveDueOrchestrationRetries,
    ) -> Result<SafetyScanPage<PromotedOrchestrationRetry>, Self::Error> {
        let mut transaction = self.begin_scheduler_transaction().await?;
        let page = transaction.drive_due_orchestration_retries(command).await?;
        transaction.commit().await?;
        Ok(page)
    }

    async fn drive_due_waits(
        &self,
        command: DriveDueOrchestrationWaits,
    ) -> Result<SafetyScanPage<WokenOrchestrationJob>, Self::Error> {
        let mut transaction = self.begin_scheduler_transaction().await?;
        let page = transaction.drive_due_orchestration_waits(command).await?;
        transaction.commit().await?;
        Ok(page)
    }

    async fn drive_convergence(
        &self,
        command: DriveOrchestrationConvergence,
    ) -> Result<SafetyScanPage<ConvergedOrchestrationRun>, Self::Error> {
        PgRepository::drive_orchestration_convergence(self, command).await
    }

    async fn drive_expired_tasks(
        &self,
        command: DriveExpiredOrchestrationTasks,
    ) -> Result<SafetyScanPage<ResolvedOrchestrationTask>, Self::Error> {
        let mut transaction = self.begin_scheduler_transaction().await?;
        let page = transaction
            .drive_expired_orchestration_tasks(command)
            .await?;
        transaction.commit().await?;
        Ok(page)
    }

    async fn drive_terminal_children(
        &self,
        command: DriveTerminalChildRuns,
    ) -> Result<SafetyScanPage<ResolvedOrchestrationChildRun>, Self::Error> {
        let mut transaction = self.begin_scheduler_transaction().await?;
        let resolved = transaction.drive_terminal_child_runs(command).await?;
        transaction.commit().await?;
        Ok(resolved)
    }

    async fn drive_child_cancellations(
        &self,
        command: DriveChildRunCancellations,
    ) -> Result<SafetyScanPage<CancellingOrchestrationChildRun>, Self::Error> {
        let mut transaction = self.begin_scheduler_transaction().await?;
        let cancelling = transaction.drive_child_run_cancellations(command).await?;
        transaction.commit().await?;
        Ok(cancelling)
    }
}
#[async_trait]
impl ArtifactSafetyStore for PgRepository {
    type Error = RepositoryError;

    async fn drive_expired_artifact_jobs(
        &self,
        command: DriveExpiredArtifactJobs,
    ) -> Result<SafetyScanPage<RecoveredArtifactJob>, Self::Error> {
        PgRepository::drive_expired_artifact_jobs(self, command).await
    }
}
