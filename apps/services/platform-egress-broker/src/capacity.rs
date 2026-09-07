use insight_platform_egress::EgressCapacitySnapshot;
use insight_platform_egress_rpc::{
    EgressMcpSubscriptionBridge, EgressMcpSubscriptionBridgeCapacitySnapshot,
};
use insight_platform_observability::{
    OperationalCapacityMetric, OperationalCapacitySnapshot, OperationalCapacitySource,
};
use insight_platform_secret_broker::SecretBrokerCapacitySnapshot;
use std::sync::Arc;

struct EgressCapacityObservation<T> {
    owner: Arc<T>,
    snapshot: fn(&T) -> EgressCapacitySnapshot,
}

impl<T> OperationalCapacitySource for EgressCapacityObservation<T>
where
    T: Send + Sync + 'static,
{
    fn snapshot(&self) -> OperationalCapacitySnapshot {
        let snapshot = (self.snapshot)(&self.owner);
        operational(snapshot.maximum_in_flight, snapshot.available)
    }
}

struct SecretCapacityObservation<T> {
    owner: Arc<T>,
    snapshot: fn(&T) -> SecretBrokerCapacitySnapshot,
}

impl<T> OperationalCapacitySource for SecretCapacityObservation<T>
where
    T: Send + Sync + 'static,
{
    fn snapshot(&self) -> OperationalCapacitySnapshot {
        let snapshot = (self.snapshot)(&self.owner);
        operational(snapshot.maximum_in_flight, snapshot.available)
    }
}

#[derive(Clone, Copy)]
enum BridgeLane {
    Pending,
    Active,
}

struct BridgeCapacityObservation {
    owner: Arc<EgressMcpSubscriptionBridge>,
    lane: BridgeLane,
}

impl OperationalCapacitySource for BridgeCapacityObservation {
    fn snapshot(&self) -> OperationalCapacitySnapshot {
        let EgressMcpSubscriptionBridgeCapacitySnapshot {
            maximum_pending,
            pending_available,
            maximum_active,
            active_available,
        } = self.owner.capacity_snapshot();
        match self.lane {
            BridgeLane::Pending => operational(maximum_pending, pending_available),
            BridgeLane::Active => operational(maximum_active, active_available),
        }
    }
}

fn operational(maximum: usize, available: usize) -> OperationalCapacitySnapshot {
    OperationalCapacitySnapshot::new(
        u64::try_from(maximum).unwrap_or(u64::MAX),
        u64::try_from(available).unwrap_or(u64::MAX),
    )
    .expect("Egress semaphore preserves its configured capacity")
}

pub(crate) fn egress_capacity_metric<T>(
    resource: &'static str,
    owner: Arc<T>,
    snapshot: fn(&T) -> EgressCapacitySnapshot,
) -> OperationalCapacityMetric
where
    T: Send + Sync + 'static,
{
    let source: Arc<dyn OperationalCapacitySource> =
        Arc::new(EgressCapacityObservation { owner, snapshot });
    OperationalCapacityMetric::new(resource, source)
}

pub(crate) fn secret_capacity_metric<T>(
    resource: &'static str,
    owner: Arc<T>,
    snapshot: fn(&T) -> SecretBrokerCapacitySnapshot,
) -> OperationalCapacityMetric
where
    T: Send + Sync + 'static,
{
    let source: Arc<dyn OperationalCapacitySource> =
        Arc::new(SecretCapacityObservation { owner, snapshot });
    OperationalCapacityMetric::new(resource, source)
}

pub(crate) fn bridge_capacity_metrics(
    owner: Arc<EgressMcpSubscriptionBridge>,
) -> [OperationalCapacityMetric; 2] {
    [
        OperationalCapacityMetric::new(
            "mcp_subscription_pending",
            Arc::new(BridgeCapacityObservation {
                owner: Arc::clone(&owner),
                lane: BridgeLane::Pending,
            }),
        ),
        OperationalCapacityMetric::new(
            "mcp_subscription_active",
            Arc::new(BridgeCapacityObservation {
                owner,
                lane: BridgeLane::Active,
            }),
        ),
    ]
}
