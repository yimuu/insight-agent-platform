use insight_platform_contracts::{ResourceId, ResourceKind};
use insight_platform_jobs::store::SafetyScanShard;
use insight_platform_orchestrator::store::{
    DriveOrchestrationConvergence, OrchestrationConvergenceSlot,
};
pub fn command() -> DriveOrchestrationConvergence {
    fn id(kind: ResourceKind) -> ResourceId {
        format!("{}_{}", kind.descriptor().prefix, uuid::Uuid::now_v7())
            .parse()
            .unwrap()
    }
    DriveOrchestrationConvergence {
        shard: SafetyScanShard { index: 0, count: 1 },
        after: None,
        limit: 16,
        slots: (0..16)
            .map(|_| OrchestrationConvergenceSlot {
                quota_entry_ids: (0..4).map(|_| id(ResourceKind::QuotaLedgerEntry)).collect(),
                run_event_id: id(ResourceKind::Event),
                run_outbox_id: id(ResourceKind::OutboxEvent),
                node_event_id: id(ResourceKind::Event),
                node_outbox_id: id(ResourceKind::OutboxEvent),
                node_cancelling_event_id: id(ResourceKind::Event),
                node_cancelling_outbox_id: id(ResourceKind::OutboxEvent),
                scope_closing_event_id: id(ResourceKind::Event),
                scope_closing_outbox_id: id(ResourceKind::OutboxEvent),
                scope_terminal_event_id: id(ResourceKind::Event),
                scope_terminal_outbox_id: id(ResourceKind::OutboxEvent),
                job_event_id: id(ResourceKind::Event),
                job_outbox_id: id(ResourceKind::OutboxEvent),
            })
            .collect(),
    }
}
