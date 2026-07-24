//! Workspace-internal hooks used by the root cross-layer conformance harness.
//!
//! This module is intentionally absent from the root compatibility facade. It
//! exposes orchestration seams that cannot live in a lower crate without
//! introducing a forbidden runtime/storage dependency, while keeping Cargo
//! features reserved for actual build capabilities.

use insight_engine::{events::protocol::RunEvent, PublicEventEnvelope, RunId};

use crate::run_service::{self, RunService, RunSubscription, ServiceError};

pub async fn run_event(
    service: &RunService,
    run_id: &RunId,
    envelope: &PublicEventEnvelope,
) -> Result<RunEvent, ServiceError> {
    run_service::test_run_event(service, run_id, envelope).await
}

pub fn subscription(service: &RunService, run_id: &RunId) -> RunSubscription {
    run_service::test_subscription(service, run_id)
}

pub async fn deliver_published_public_event(
    service: &RunService,
    public_event_id: &str,
) -> Result<(), ServiceError> {
    run_service::test_deliver_published_public_event(service, public_event_id).await
}

pub async fn flush_public_events(service: &RunService) -> Result<(), ServiceError> {
    run_service::test_flush_public_events(service).await
}

pub fn has_live_subscription(service: &RunService, run_id: &RunId) -> bool {
    run_service::test_has_live_subscription(service, run_id)
}
