//! Artifact recovery command and read ports, independent of a physical database.
use crate::ArtifactWorkError;
use insight_platform_contracts::{ResourceId, ResourceKind};
use insight_platform_jobs::store::{
    validate_safety_scan_request, JobRecord, SafetyScanCursor, SafetyScanShard,
};
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub struct ArtifactRecoverySlot {
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
}
impl ArtifactRecoverySlot {
    pub fn validate(&self) -> Result<(), ArtifactWorkError> {
        if self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.event_id.to_string() == self.outbox_id.to_string()
        {
            return Err(ArtifactWorkError::InvalidCommand);
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DriveExpiredArtifactJobs {
    pub shard: SafetyScanShard,
    pub after: Option<SafetyScanCursor>,
    pub limit: u16,
    pub slots: Vec<ArtifactRecoverySlot>,
}
impl DriveExpiredArtifactJobs {
    pub fn validate(
        &self,
        maximum_batch: u16,
        maximum_shards: u16,
    ) -> Result<(), ArtifactWorkError> {
        validate_safety_scan_request(
            self.shard,
            self.after.as_ref(),
            ResourceKind::Job,
            self.limit,
            self.slots.len(),
            maximum_batch,
            maximum_shards,
        )
        .map_err(|_| ArtifactWorkError::InvalidCommand)?;
        let mut identities = BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            for identity in [&slot.event_id, &slot.outbox_id] {
                if !identities.insert(identity.to_string()) {
                    return Err(ArtifactWorkError::InvalidCommand);
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct RecoveredArtifactJob {
    pub job: JobRecord,
    pub payload_kind: String,
    pub artifact_version: Option<u64>,
    pub blob_version: Option<u64>,
    pub operation_version: Option<u64>,
}
