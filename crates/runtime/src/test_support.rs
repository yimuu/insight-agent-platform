//! Cross-layer test harness hooks for the root package.

use insight_engine::{events::protocol::RunEvent, PublicEventEnvelope, RunId};

use crate::v3_service::{self, RunService, RunSubscription, ServiceError};

pub async fn run_event(
    service: &RunService,
    run_id: &RunId,
    envelope: &PublicEventEnvelope,
) -> Result<RunEvent, ServiceError> {
    v3_service::test_run_event(service, run_id, envelope).await
}

pub fn subscription(service: &RunService, run_id: &RunId) -> RunSubscription {
    v3_service::test_subscription(service, run_id)
}

pub async fn deliver_published_public_event(
    service: &RunService,
    public_event_id: &str,
) -> Result<(), ServiceError> {
    v3_service::test_deliver_published_public_event(service, public_event_id).await
}

pub async fn flush_public_events(service: &RunService) -> Result<(), ServiceError> {
    v3_service::test_flush_public_events(service).await
}

pub fn has_live_subscription(service: &RunService, run_id: &RunId) -> bool {
    v3_service::test_has_live_subscription(service, run_id)
}
