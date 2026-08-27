use insight_platform_mcp_service::McpDiscoveryCapacity;
use insight_platform_observability::{
    OperationalCapacityMetric, OperationalCapacitySnapshot, OperationalCapacitySource,
};
use std::sync::Arc;

struct McpDiscoveryCapacityObservation {
    owner: McpDiscoveryCapacity,
}

impl OperationalCapacitySource for McpDiscoveryCapacityObservation {
    fn snapshot(&self) -> OperationalCapacitySnapshot {
        OperationalCapacitySnapshot::new(
            u64::try_from(self.owner.maximum()).unwrap_or(u64::MAX),
            u64::try_from(self.owner.available()).unwrap_or(u64::MAX),
        )
        .expect("MCP discovery semaphore preserves its configured capacity")
    }
}

pub(crate) fn discovery_capacity_metric(owner: McpDiscoveryCapacity) -> OperationalCapacityMetric {
    let source: Arc<dyn OperationalCapacitySource> =
        Arc::new(McpDiscoveryCapacityObservation { owner });
    OperationalCapacityMetric::new("discovery_jobs", source)
}
