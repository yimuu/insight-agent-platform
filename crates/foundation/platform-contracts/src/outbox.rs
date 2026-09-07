//! Versioned, bounded committed-event delivery contracts. No business payload crosses this boundary.

use crate::{ResourceId, ResourceKind, TraceId, UtcTimestamp, MAX_SAFE_JSON_INTEGER};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

pub const COMMITTED_EVENT_NOTICE_VERSION: u16 = 1;
pub const MAX_COMMITTED_EVENT_NOTICE_BYTES: usize = 4096;
pub const MAX_OUTBOX_CLAIM_BATCH: u16 = 64;
pub const MAX_OUTBOX_LEASE_MILLISECONDS: u64 = 120_000;
pub const MAX_OUTBOX_RETRY_MILLISECONDS: u64 = 3_600_000;
pub const MAX_OUTBOX_BACKLOG_SCAN: u32 = 1_000_000;

/// An authority point-read hint. Deliberately excludes original Event type, payload and data refs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedEventNoticeV1 {
    pub schema_version: u16,
    pub tenant_id: ResourceId,
    pub event_id: ResourceId,
    pub aggregate_id: ResourceId,
    pub aggregate_version: Option<u64>,
    pub run_id: Option<ResourceId>,
    pub public_sequence: Option<u64>,
    pub trace_id: TraceId,
    pub occurred_at: UtcTimestamp,
}

impl CommittedEventNoticeV1 {
    pub fn validate(&self) -> Result<(), OutboxContractError> {
        if self.schema_version != COMMITTED_EVENT_NOTICE_VERSION
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.event_id.kind() != ResourceKind::Event
            || self
                .aggregate_version
                .is_some_and(|v| v == 0 || v > MAX_SAFE_JSON_INTEGER)
            || self
                .run_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::Run)
            || self
                .public_sequence
                .is_some_and(|v| v == 0 || v > MAX_SAFE_JSON_INTEGER || self.run_id.is_none())
        {
            return Err(OutboxContractError);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxClaimFence {
    pub tenant_id: ResourceId,
    pub outbox_id: ResourceId,
    pub event_id: ResourceId,
    pub process_generation: ResourceId,
    pub epoch: u64,
}

impl OutboxClaimFence {
    pub fn validate(&self) -> Result<(), OutboxContractError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.event_id.kind() != ResourceKind::Event
            || self.process_generation.kind() != ResourceKind::WorkerProcessGeneration
            || self.epoch == 0
            || self.epoch > i64::MAX as u64
        {
            return Err(OutboxContractError);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ClaimedCommittedEvent {
    pub fence: OutboxClaimFence,
    pub notice: CommittedEventNoticeV1,
    pub publish_attempts: u32,
}

impl ClaimedCommittedEvent {
    pub fn validate(&self) -> Result<(), OutboxContractError> {
        self.fence.validate()?;
        self.notice.validate()?;
        if self.fence.tenant_id != self.notice.tenant_id
            || self.fence.event_id != self.notice.event_id
        {
            return Err(OutboxContractError);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ClaimDueCommittedEvents {
    pub process_generation: ResourceId,
    pub maximum_claims: u16,
    pub lease_milliseconds: u64,
}

impl ClaimDueCommittedEvents {
    pub fn validate(&self) -> Result<(), OutboxContractError> {
        if self.process_generation.kind() != ResourceKind::WorkerProcessGeneration
            || self.maximum_claims == 0
            || self.maximum_claims > MAX_OUTBOX_CLAIM_BATCH
            || self.lease_milliseconds == 0
            || self.lease_milliseconds > MAX_OUTBOX_LEASE_MILLISECONDS
        {
            return Err(OutboxContractError);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxFailureCode {
    TransportUnavailable,
    ConfirmationInvalid,
    ContractIncompatible,
}

impl OutboxFailureCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TransportUnavailable => "outbox_transport_unavailable",
            Self::ConfirmationInvalid => "outbox_confirmation_invalid",
            Self::ContractIncompatible => "outbox_contract_incompatible",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OutboxSettlement {
    Published,
    Retry {
        failure: OutboxFailureCode,
        delay_milliseconds: u64,
    },
    Incompatible,
}

impl OutboxSettlement {
    pub fn validate(self) -> Result<(), OutboxContractError> {
        if let Self::Retry {
            delay_milliseconds, ..
        } = self
        {
            if delay_milliseconds == 0 || delay_milliseconds > MAX_OUTBOX_RETRY_MILLISECONDS {
                return Err(OutboxContractError);
            }
        }
        Ok(())
    }
}

/// Counts all undelivered obligations, including incompatible records. The query stops at limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxBacklogWatermark {
    pub observed_undelivered: u32,
    pub scan_limit: u32,
    pub limit_reached: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxContractError;
impl fmt::Display for OutboxContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid committed-event delivery contract")
    }
}
impl Error for OutboxContractError {}

#[cfg(test)]
mod tests {
    use super::*;
    fn notice() -> CommittedEventNoticeV1 {
        CommittedEventNoticeV1 {
            schema_version: 1,
            tenant_id: "ten_0198f1c3-8f49-7c3e-b1f3-773c28367b94".parse().unwrap(),
            event_id: "evt_0198f1c3-8f49-7c3e-b1f3-773c28367b94".parse().unwrap(),
            aggregate_id: "run_0198f1c3-8f49-7c3e-b1f3-773c28367b94".parse().unwrap(),
            aggregate_version: Some(1),
            run_id: None,
            public_sequence: None,
            trace_id: "0123456789abcdef0123456789abcdef".parse().unwrap(),
            occurred_at: "2026-09-06T00:00:00.000000Z".parse().unwrap(),
        }
    }
    #[test]
    fn projection_cannot_accept_a_body_or_an_unsupported_schema() {
        let mut value = serde_json::to_value(notice()).unwrap();
        value["payload"] = serde_json::json!({"secret": "not deliverable"});
        assert!(serde_json::from_value::<CommittedEventNoticeV1>(value).is_err());
        let mut current = notice();
        current.schema_version = 2;
        assert!(current.validate().is_err());
    }
    #[test]
    fn sequence_needs_exact_run_and_fence_cannot_switch_events() {
        let mut current = notice();
        current.public_sequence = Some(1);
        assert!(current.validate().is_err());
        current.run_id = Some(current.aggregate_id.clone());
        assert!(current.validate().is_ok());
        let mut claim = ClaimedCommittedEvent {
            fence: OutboxClaimFence {
                tenant_id: current.tenant_id.clone(),
                outbox_id: "obx_0198f1c3-8f49-7c3e-b1f3-773c28367b94".parse().unwrap(),
                event_id: current.event_id.clone(),
                process_generation: "wrk_0198f1c3-8f49-7c3e-b1f3-773c28367b94".parse().unwrap(),
                epoch: 1,
            },
            notice: current,
            publish_attempts: 0,
        };
        assert!(claim.validate().is_ok());
        claim.fence.event_id = "evt_0198f1c3-8f49-7c3e-b1f3-773c28367b95".parse().unwrap();
        assert!(claim.validate().is_err());
    }
}
