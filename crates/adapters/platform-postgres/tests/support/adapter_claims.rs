//! Exercise bounded scheduler rounds; a single production poll owns one partition.
use insight_platform_context::ClaimContextJobs;
use insight_platform_contracts::Sha256Digest;
use insight_platform_invocations::ClaimCapabilityJobs;
use insight_platform_jobs::store::JobRecord;
use insight_platform_models::ClaimModelJobs;
use insight_platform_postgres::repository::ClaimJobs;
use insight_platform_postgres::repository::{PgRepository, RepositoryError};
use insight_platform_postgres::{
    capability_execution_repository::ClaimedCapabilityExecution,
    context_query_repository::ClaimedContextExecution,
    model_turn_repository::ClaimedModelExecution,
};
#[allow(dead_code)]
#[async_trait::async_trait]
pub trait AdapterClaimRounds {
    async fn claim_model_jobs_current_rounds(
        &self,
        command: ClaimModelJobs,
    ) -> Result<Vec<ClaimedModelExecution>, RepositoryError>;
    async fn claim_capability_jobs_current_rounds(
        &self,
        command: ClaimCapabilityJobs,
    ) -> Result<Vec<ClaimedCapabilityExecution>, RepositoryError>;
    async fn claim_context_jobs_current_rounds(
        &self,
        command: ClaimContextJobs,
    ) -> Result<Vec<ClaimedContextExecution>, RepositoryError>;
    async fn claim_dataset_jobs_current_rounds(
        &self,
        command: ClaimJobs,
    ) -> Result<Vec<JobRecord>, RepositoryError>;
    async fn claim_dataset_sources_current_rounds(
        &self,
        command: ClaimJobs,
        sources: &[Sha256Digest],
    ) -> Result<Vec<JobRecord>, RepositoryError>;
}
#[async_trait::async_trait]
impl AdapterClaimRounds for PgRepository {
    async fn claim_dataset_jobs_current_rounds(
        &self,
        command: ClaimJobs,
    ) -> Result<Vec<JobRecord>, RepositoryError> {
        for _ in 0..(usize::from(insight_platform_contracts::SCHEDULER_PARTITION_COUNT) * 8) {
            let batch = self
                .claim_context_dataset_build_jobs(command.clone())
                .await?;
            if !batch.is_empty() {
                return Ok(batch);
            }
            tokio::task::yield_now().await;
        }
        Ok(vec![])
    }
    async fn claim_dataset_sources_current_rounds(
        &self,
        command: ClaimJobs,
        sources: &[Sha256Digest],
    ) -> Result<Vec<JobRecord>, RepositoryError> {
        for _ in 0..(usize::from(insight_platform_contracts::SCHEDULER_PARTITION_COUNT) * 8) {
            let batch = self
                .claim_context_dataset_build_jobs_for_sources(command.clone(), sources)
                .await?;
            if !batch.is_empty() {
                return Ok(batch);
            }
            tokio::task::yield_now().await;
        }
        Ok(vec![])
    }
    async fn claim_model_jobs_current_rounds(
        &self,
        command: ClaimModelJobs,
    ) -> Result<Vec<ClaimedModelExecution>, RepositoryError> {
        for _ in 0..(usize::from(insight_platform_contracts::SCHEDULER_PARTITION_COUNT) * 8) {
            let batch = self.claim_model_jobs(command.clone()).await?;
            if !batch.is_empty() {
                return Ok(batch);
            }
            tokio::task::yield_now().await;
        }
        Ok(vec![])
    }
    async fn claim_capability_jobs_current_rounds(
        &self,
        command: ClaimCapabilityJobs,
    ) -> Result<Vec<ClaimedCapabilityExecution>, RepositoryError> {
        for _ in 0..(usize::from(insight_platform_contracts::SCHEDULER_PARTITION_COUNT) * 8) {
            let batch = self.claim_capability_jobs(command.clone()).await?;
            if !batch.is_empty() {
                return Ok(batch);
            }
            tokio::task::yield_now().await;
        }
        Ok(vec![])
    }
    async fn claim_context_jobs_current_rounds(
        &self,
        command: ClaimContextJobs,
    ) -> Result<Vec<ClaimedContextExecution>, RepositoryError> {
        for _ in 0..(usize::from(insight_platform_contracts::SCHEDULER_PARTITION_COUNT) * 8) {
            let batch = self.claim_context_jobs(command.clone()).await?;
            if !batch.is_empty() {
                return Ok(batch);
            }
            tokio::task::yield_now().await;
        }
        Ok(vec![])
    }
}
