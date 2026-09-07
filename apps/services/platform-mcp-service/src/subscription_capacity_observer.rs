use insight_platform_mcp_service::subscription_driver::McpSubscriptionCapacity;
use insight_platform_observability::{
    OperationalCapacityMetric, OperationalCapacitySnapshot, OperationalCapacitySource,
};
use std::sync::Arc;

struct McpSubscriptionCapacityObservation {
    owner: McpSubscriptionCapacity,
}

impl OperationalCapacitySource for McpSubscriptionCapacityObservation {
    fn snapshot(&self) -> OperationalCapacitySnapshot {
        OperationalCapacitySnapshot::new(
            u64::try_from(self.owner.maximum()).unwrap_or(u64::MAX),
            u64::try_from(self.owner.available()).unwrap_or(u64::MAX),
        )
        .expect("MCP subscription semaphore preserves its configured capacity")
    }
}

pub(crate) fn subscription_capacity_metric(
    owner: McpSubscriptionCapacity,
) -> OperationalCapacityMetric {
    let source: Arc<dyn OperationalCapacitySource> =
        Arc::new(McpSubscriptionCapacityObservation { owner });
    OperationalCapacityMetric::new("subscription_jobs", source)
}
