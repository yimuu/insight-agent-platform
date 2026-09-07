//! Finite production-style claim rounds, retaining the selected transaction for assertions.
use insight_platform_contracts::SCHEDULER_PARTITION_COUNT;
use insight_platform_orchestrator::store::{ClaimOrchestrationJobs, ClaimedOrchestrationJob};
use insight_platform_postgres::repository::{
    PgRepository, PgSchedulerTransaction, RepositoryError,
};
use std::{future::Future, pin::Pin};

type ClaimFixtureResult =
    Result<(PgSchedulerTransaction, Vec<ClaimedOrchestrationJob>), RepositoryError>;

pub fn begin_orchestration_claim_fixture(
    repository: &PgRepository,
    command: ClaimOrchestrationJobs,
) -> Pin<Box<dyn Future<Output = ClaimFixtureResult> + Send + '_>> {
    // Match the old async-trait helper's boxed future: the large kernel test must not
    // inline a complete production claim future at every sequential assertion site.
    Box::pin(async move {
        let maximum_rounds = usize::from(SCHEDULER_PARTITION_COUNT) * 8;
        for round in 0..maximum_rounds {
            let mut transaction = repository.begin_orchestration_claim_transaction().await?;
            let batch = match transaction.claim_orchestration_jobs(command.clone()).await {
                Ok(batch) => batch,
                Err(failure) => {
                    transaction.rollback().await?;
                    return Err(failure);
                }
            };
            if !batch.is_empty() || round + 1 == maximum_rounds {
                return Ok((transaction, batch));
            }
            // Empty partitions still advance durable fairness. A new transaction must obtain
            // fresh physical hints; reusing this transaction would keep its first partition.
            transaction.commit().await?;
            tokio::task::yield_now().await;
        }
        unreachable!("bounded claim rounds return the last still-open transaction")
    })
}
