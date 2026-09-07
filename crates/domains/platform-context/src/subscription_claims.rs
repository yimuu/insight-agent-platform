//! Physical allocation for shared Context subscription Job claims. The store
//! selects durable owners; callers supply capacity and new mutation identities.
use insight_platform_contracts::{
    ResourceId, ResourceKind, Sha256Digest, WorkClass, WorkerManifest,
};
use insight_platform_jobs::LeasePolicy;
use std::collections::BTreeSet;
pub const CONTEXT_SUBSCRIPTION_REFRESH_MAX_BATCH: u16 = 64;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextSubscriptionRefreshClaimSlot {
    pub lease_token_digest: Sha256Digest,
    pub quota_reservation_id: ResourceId,
    pub quota_reserve_entry_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
}
#[derive(Debug, Clone)]
pub struct ClaimContextSubscriptionRefreshJobs {
    pub worker_process_generation_id: ResourceId,
    pub worker_manifest_digest: Sha256Digest,
    pub worker_manifest: WorkerManifest,
    pub slots: Vec<ContextSubscriptionRefreshClaimSlot>,
    pub lease_policy: LeasePolicy,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextSubscriptionClaimError {
    InvalidCapacity,
    InvalidIdentity,
    InvalidManifest,
}
impl std::fmt::Display for ContextSubscriptionClaimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Context subscription claim: {self:?}")
    }
}
impl std::error::Error for ContextSubscriptionClaimError {}
impl ClaimContextSubscriptionRefreshJobs {
    pub fn validate(&self) -> Result<(), ContextSubscriptionClaimError> {
        use ContextSubscriptionClaimError::*;
        self.worker_manifest
            .validate()
            .map_err(|_| InvalidManifest)?;
        if self.worker_manifest.work_class != WorkClass::Context
            || self
                .worker_manifest
                .canonical_digest()
                .map_err(|_| InvalidManifest)?
                != self.worker_manifest_digest
        {
            return Err(InvalidManifest);
        }
        if self.slots.is_empty()
            || self.slots.len() > usize::from(CONTEXT_SUBSCRIPTION_REFRESH_MAX_BATCH)
            || self.lease_policy.requested_milliseconds == 0
            || self.lease_policy.requested_milliseconds
                > self.lease_policy.hard_maximum_milliseconds
        {
            return Err(InvalidCapacity);
        }
        if self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration {
            return Err(InvalidIdentity);
        }
        let mut ids = BTreeSet::new();
        let mut tokens = BTreeSet::new();
        for slot in &self.slots {
            if slot.quota_reservation_id.kind() != ResourceKind::UsageReservation
                || slot.quota_reserve_entry_id.kind() != ResourceKind::QuotaLedgerEntry
                || slot.event_id.kind() != ResourceKind::Event
                || slot.outbox_id.kind() != ResourceKind::OutboxEvent
                || !tokens.insert(slot.lease_token_digest.clone())
            {
                return Err(InvalidIdentity);
            }
            for id in [
                &slot.quota_reservation_id,
                &slot.quota_reserve_entry_id,
                &slot.event_id,
                &slot.outbox_id,
            ] {
                if !ids.insert(id.clone()) {
                    return Err(InvalidIdentity);
                }
            }
        }
        Ok(())
    }
}
