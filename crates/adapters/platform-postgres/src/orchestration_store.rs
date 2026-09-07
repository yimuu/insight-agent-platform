//! PostgreSQL implementation of the application orchestration port.
use crate::repository::{PgRepository, RepositoryError};
use async_trait::async_trait;
use insight_platform_orchestrator::store::*;
use insight_platform_runtime::*;
#[async_trait]
impl OrchestrationClaimStore for PgRepository {
    type Error = RepositoryError;

    async fn claim_orchestration_jobs(
        &self,
        command: ClaimOrchestrationJobs,
    ) -> Result<Vec<ClaimedOrchestrationJob>, Self::Error> {
        let mut transaction = self.begin_orchestration_claim_transaction().await?;
        let claimed = transaction.claim_orchestration_jobs(command).await?;
        transaction.commit().await?;
        Ok(claimed)
    }
}
