use async_trait::async_trait;

use crate::RepositoryError;
use crate::RepositoryErrorExt as _;

pub const WORK_NOTIFY_CHANNEL_PREFIX: &str = "iap_work_";

/// Payload-free durable work categories. Notifications are hints only: a
/// consumer must always re-run the authoritative bounded repository query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkClass {
    SchedulerTask,
    ModelToolTask,
    RuntimeIngress,
    PublicEvent,
    Recovery,
    /// Transaction-coalesced all-class hint, also used by maintenance work.
    Maintenance,
}

impl WorkClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SchedulerTask => "scheduler_task",
            Self::ModelToolTask => "model_tool_task",
            Self::RuntimeIngress => "runtime_ingress",
            Self::PublicEvent => "public_event",
            Self::Recovery => "recovery",
            Self::Maintenance => "maintenance",
        }
    }

    pub fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "scheduler_task" => Ok(Self::SchedulerTask),
            "model_tool_task" => Ok(Self::ModelToolTask),
            "runtime_ingress" => Ok(Self::RuntimeIngress),
            "public_event" => Ok(Self::PublicEvent),
            "recovery" => Ok(Self::Recovery),
            "maintenance" => Ok(Self::Maintenance),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

/// Backend notification stream. Messages may be duplicated, coalesced,
/// reordered or lost and therefore never carry execution authority.
#[async_trait]
pub trait WorkNotificationStream: Send {
    async fn recv(&mut self) -> Result<WorkClass, RepositoryError>;
}

/// Backend-neutral source of low-latency work wakeup hints.
///
/// PostgreSQL exposes a cross-process LISTEN/NOTIFY stream. SQLite may return
/// `None`; its single-process runtime still has local wakeups and safety polls.
#[async_trait]
pub trait WorkWakeupRepository: Send + Sync {
    async fn open_work_notification_stream(
        &self,
    ) -> Result<Option<Box<dyn WorkNotificationStream>>, RepositoryError>;

    /// Publish one lossy cross-process hint after authoritative work has
    /// already committed. Implementations may coalesce or discard the hint;
    /// consumers must still use their safety scan for correctness.
    async fn publish_work_notification(&self, _work: WorkClass) -> Result<(), RepositoryError> {
        Ok(())
    }
}
