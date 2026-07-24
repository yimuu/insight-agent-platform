use async_trait::async_trait;
use serde_json::Value;

use insight_dsl::GraphSurfaceRepository;
use insight_engine::{NodeId, RunId, TransitionKey};

use crate::{
    ClaimSchedulerRunCommand, FencedSchedulerRunCommand, HumanTaskDurableRepository,
    PublicEventOutboxRepository, RecoveryDurableRepository, RepositoryError,
    RuntimeIngressDurableRepository, SchedulerDurableRepository,
};

/// Declares whether a Run repository can uphold the ownership and fencing
/// contracts required by a production runtime.
///
/// The default is deliberately fail-closed so a new repository implementation
/// cannot become production-capable merely by implementing the data methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunRepositoryCapability {
    SingleProcessOnly,
    Production,
}

#[async_trait]
pub trait ProductionRunRepository:
    SchedulerDurableRepository
    + PublicEventOutboxRepository
    + RuntimeIngressDurableRepository
    + RecoveryDurableRepository
    + HumanTaskDurableRepository
    + GraphSurfaceRepository
{
    fn run_repository_capability(&self) -> RunRepositoryCapability {
        RunRepositoryCapability::SingleProcessOnly
    }

    async fn check_production_health(&self) -> Result<(), RepositoryError>;

    /// Load the immutable canonical input through the Run's payload authority.
    /// Public history queries must not depend on a live scheduler checkpoint.
    async fn load_production_run_input(
        &self,
        run_id: &RunId,
    ) -> Result<Option<Value>, RepositoryError>;

    async fn claim_production_scheduler_run(
        &self,
        transition_key: TransitionKey,
        command: ClaimSchedulerRunCommand,
    ) -> Result<Option<FencedSchedulerRunCommand>, RepositoryError>;

    async fn release_production_scheduler_run(
        &self,
        transition_key: TransitionKey,
        fence: FencedSchedulerRunCommand,
    ) -> Result<(), RepositoryError>;

    /// Read-only evidence used to reject incomplete public migration mappings
    /// before `begin_migration` can write a source termination intent.
    async fn load_pending_migration_waits(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<PendingMigrationWait>, RepositoryError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingMigrationWait {
    node_id: NodeId,
    has_signal: bool,
    has_timer: bool,
}

impl PendingMigrationWait {
    fn new(node_id: NodeId, has_signal: bool, has_timer: bool) -> Result<Self, RepositoryError> {
        if !has_signal && !has_timer {
            return Err(insight_engine::repository::adapter::invalid_data());
        }
        Ok(Self {
            node_id,
            has_signal,
            has_timer,
        })
    }
}

/// Workspace-internal bridge for concrete storage implementations and runtime
/// validation. It intentionally does not become part of the root facade.
#[doc(hidden)]
pub mod adapter {
    use insight_engine::NodeId;

    use super::{PendingMigrationWait, RepositoryError};

    pub fn pending_migration_wait(
        node_id: NodeId,
        has_signal: bool,
        has_timer: bool,
    ) -> Result<PendingMigrationWait, RepositoryError> {
        PendingMigrationWait::new(node_id, has_signal, has_timer)
    }

    pub fn pending_migration_wait_parts(wait: &PendingMigrationWait) -> (&NodeId, bool, bool) {
        (&wait.node_id, wait.has_signal, wait.has_timer)
    }
}
