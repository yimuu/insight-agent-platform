//! Bounded delivery orchestration through narrow authority and transport ports.
//! A transport confirmation is never a business completion or a Job state transition.

use async_trait::async_trait;
use insight_platform_contracts::{
    ClaimDueCommittedEvents, ClaimedCommittedEvent, CommittedEventNoticeV1, OutboxBacklogWatermark,
    OutboxClaimFence, OutboxFailureCode, OutboxSettlement,
};
use std::{error::Error, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxDeliveryError {
    InvalidCommand,
    Unavailable,
    Incompatible,
}
impl fmt::Display for OutboxDeliveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidCommand => "invalid outbox command",
            Self::Unavailable => "outbox authority unavailable",
            Self::Incompatible => "outbox contract incompatible",
        })
    }
}
impl Error for OutboxDeliveryError {}

#[async_trait]
pub trait OutboxDeliveryStore: Send + Sync {
    async fn claim_due_committed_events(
        &self,
        command: ClaimDueCommittedEvents,
    ) -> Result<Vec<ClaimedCommittedEvent>, OutboxDeliveryError>;
    /// False means the claim has expired or another generation now owns it.
    async fn settle_committed_event(
        &self,
        fence: &OutboxClaimFence,
        settlement: OutboxSettlement,
    ) -> Result<bool, OutboxDeliveryError>;
    async fn observe_outbox_backlog(
        &self,
        maximum_scan_rows: u32,
    ) -> Result<OutboxBacklogWatermark, OutboxDeliveryError>;
}

#[async_trait]
pub trait CommittedEventPublisher: Send + Sync {
    /// Success requires a durable server ACK from the exact contracted stream.
    async fn publish(&self, notice: &CommittedEventNoticeV1) -> Result<(), OutboxFailureCode>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OutboxDrainReport {
    pub published: u16,
    pub retry: u16,
    pub incompatible: u16,
    pub fence_lost: u16,
}

/// The caller reserves local capacity before claim. Publication is sequential, so the process
/// contract must fit maximum_claims * publish_timeout plus settlement inside its lease.
pub async fn drain_committed_events<S: OutboxDeliveryStore, P: CommittedEventPublisher>(
    store: &S,
    publisher: &P,
    command: ClaimDueCommittedEvents,
    retry_delay_milliseconds: u64,
) -> Result<OutboxDrainReport, OutboxDeliveryError> {
    command
        .validate()
        .map_err(|_| OutboxDeliveryError::InvalidCommand)?;
    OutboxSettlement::Retry {
        failure: OutboxFailureCode::TransportUnavailable,
        delay_milliseconds: retry_delay_milliseconds,
    }
    .validate()
    .map_err(|_| OutboxDeliveryError::InvalidCommand)?;
    let claims = store.claim_due_committed_events(command.clone()).await?;
    if claims.len() > usize::from(command.maximum_claims) {
        return Err(OutboxDeliveryError::Incompatible);
    }
    let mut report = OutboxDrainReport::default();
    for claim in claims {
        let settlement = if claim.validate().is_err() {
            OutboxSettlement::Incompatible
        } else {
            match publisher.publish(&claim.notice).await {
                Ok(()) => OutboxSettlement::Published,
                Err(OutboxFailureCode::ContractIncompatible) => OutboxSettlement::Incompatible,
                Err(failure) => OutboxSettlement::Retry {
                    failure,
                    delay_milliseconds: retry_delay_milliseconds,
                },
            }
        };
        if !store
            .settle_committed_event(&claim.fence, settlement)
            .await?
        {
            report.fence_lost += 1;
            continue;
        }
        match settlement {
            OutboxSettlement::Published => report.published += 1,
            OutboxSettlement::Retry { .. } => report.retry += 1,
            OutboxSettlement::Incompatible => report.incompatible += 1,
        }
    }
    Ok(report)
}
