use insight_platform_mcp_rpc::{McpRequestCapacity, McpRequestCapacitySnapshot};
use insight_platform_observability::{
    OperationalCapacityMetric, OperationalCapacitySnapshot, OperationalCapacitySource,
};
use std::sync::Arc;

struct McpRequestCapacityObservation {
    owner: McpRequestCapacity,
}

impl OperationalCapacitySource for McpRequestCapacityObservation {
    fn snapshot(&self) -> OperationalCapacitySnapshot {
        let McpRequestCapacitySnapshot {
            maximum_in_flight,
            available,
        } = self.owner.snapshot();
        OperationalCapacitySnapshot::new(
            u64::try_from(maximum_in_flight).unwrap_or(u64::MAX),
            u64::try_from(available).unwrap_or(u64::MAX),
        )
        .expect("MCP request semaphore preserves its configured capacity")
    }
}

pub(crate) fn request_capacity_metric(owner: McpRequestCapacity) -> OperationalCapacityMetric {
    let source: Arc<dyn OperationalCapacitySource> =
        Arc::new(McpRequestCapacityObservation { owner });
    OperationalCapacityMetric::new("rpc_requests", source)
}
