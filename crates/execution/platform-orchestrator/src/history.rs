//! Public Run replay and retention boundaries. Event sequence numbers are never
//! recycled. The Run stores the last removed sequence as a monotonic replay floor.

use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    PublicRunEventType, ResourceId, ResourceKind, Sha256Digest, TraceId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub mod retirement;

pub const PUBLIC_RUN_EVENT_PAGE_VERSION: u32 = 1;
pub const MAX_PUBLIC_RUN_EVENT_PAGE: u16 = 128;
pub const MAX_PUBLIC_RUN_EVENT_PURGE_BATCH: u16 = 1000;
pub const MAX_RUN_HISTORY_HOLDS: usize = 32;
pub const MAX_HISTORY_RETENTION_SECONDS: u64 = 315_576_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryRetentionError {
    InvalidPolicy,
    InvalidHold,
}

/// Deployment-signed maintenance configuration. It sets minimum retention,
/// never clears a current Run hold or authorizes disclosure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRetentionPolicy {
    pub schema_version: u32,
    pub public_event_minimum_seconds: u64,
    pub audit_event_minimum_seconds: u64,
    pub receipt_minimum_seconds: u64,
    pub published_outbox_minimum_seconds: u64,
    pub cleanup_minimum_seconds: u64,
}
impl HistoryRetentionPolicy {
    pub fn validate(&self) -> Result<(), HistoryRetentionError> {
        if self.schema_version != 2
            || [
                self.public_event_minimum_seconds,
                self.audit_event_minimum_seconds,
                self.receipt_minimum_seconds,
                self.published_outbox_minimum_seconds,
                self.cleanup_minimum_seconds,
            ]
            .iter()
            .any(|value| !(1..=MAX_HISTORY_RETENTION_SECONDS).contains(value))
        {
            return Err(HistoryRetentionError::InvalidPolicy);
        }
        Ok(())
    }
}

/// One current hold authority on the existing Run root. Keys are canonical
/// placement command digests, so a release cannot replace another hold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunHistoryHolds {
    pub schema_version: u32,
    pub holds: BTreeMap<Sha256Digest, RunHistoryHold>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunHistoryHold {
    pub reason_evidence_digest: Sha256Digest,
    pub placed_by: ResourceId,
    pub placed_at: DateTime<Utc>,
}
impl Default for RunHistoryHolds {
    fn default() -> Self {
        Self {
            schema_version: 1,
            holds: BTreeMap::new(),
        }
    }
}
impl RunHistoryHolds {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), HistoryRetentionError> {
        if self.schema_version != 1
            || self.holds.len() > MAX_RUN_HISTORY_HOLDS
            || self.holds.values().any(|hold| {
                hold.placed_by.kind() != ResourceKind::Principal || hold.placed_at > now
            })
        {
            return Err(HistoryRetentionError::InvalidHold);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "position", rename_all = "snake_case", deny_unknown_fields)]
pub enum PublicRunReadPosition {
    Initial,
    AfterSequence { sequence: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicRunEventRecord {
    pub event_id: ResourceId,
    pub trace_id: TraceId,
    pub sequence: u64,
    pub event_type: PublicRunEventType,
    pub source_id: ResourceId,
    pub source_projection_version: u64,
    pub safe_summary: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicRunEventPage {
    pub schema_version: u32,
    pub events: Vec<PublicRunEventRecord>,
    pub replay_floor: u64,
    pub high_water_sequence: u64,
    pub started_after_sequence: u64,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicReplayError {
    CorruptWatermarks,
    CursorAhead,
    HistoryGap { replay_floor: u64 },
}

impl PublicRunReadPosition {
    pub fn resolve(
        self,
        replay_floor: u64,
        high_water_sequence: u64,
    ) -> Result<u64, PublicReplayError> {
        if replay_floor > high_water_sequence || high_water_sequence > i64::MAX as u64 {
            return Err(PublicReplayError::CorruptWatermarks);
        }
        match self {
            Self::Initial => Ok(replay_floor),
            Self::AfterSequence { sequence } if sequence < replay_floor => {
                Err(PublicReplayError::HistoryGap { replay_floor })
            }
            Self::AfterSequence { sequence } if sequence > high_water_sequence => {
                Err(PublicReplayError::CursorAhead)
            }
            Self::AfterSequence { sequence } => Ok(sequence),
        }
    }
}

/// A restricted maintenance operation freezes its policy time and target sequence.
/// The target makes a retry after an uncertain commit idempotent; a batch budget
/// only bounds each transaction and does not enlarge the authorized prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PurgePublicRunEventPrefix {
    pub tenant_id: ResourceId,
    pub run_id: ResourceId,
    pub through_sequence: u64,
    pub maximum_events: u16,
}

impl PurgePublicRunEventPrefix {
    pub fn validate_at(&self, _now: DateTime<Utc>) -> Result<(), PublicReplayError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.run_id.kind() != ResourceKind::Run
            || self.through_sequence > i64::MAX as u64
            || self.maximum_events == 0
            || self.maximum_events > MAX_PUBLIC_RUN_EVENT_PURGE_BATCH
        {
            return Err(PublicReplayError::CorruptWatermarks);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicRunRetentionOutcome {
    pub previous_floor: u64,
    pub replay_floor: u64,
    pub deleted_events: u16,
    pub target_reached: bool,
}

#[derive(Debug, Clone)]
pub struct PlaceRunHistoryHold {
    pub audit: insight_platform_contracts::CommandAudit,
    pub run_id: ResourceId,
    pub expected_run_version: u64,
    pub reason_evidence_digest: Sha256Digest,
}
#[derive(Debug, Clone)]
pub struct ReleaseRunHistoryHold {
    pub audit: insight_platform_contracts::CommandAudit,
    pub run_id: ResourceId,
    pub expected_run_version: u64,
    pub hold_key: Sha256Digest,
    pub release_evidence_digest: Sha256Digest,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunHistoryHoldOutcome {
    pub run_id: ResourceId,
    pub run_version: u64,
    pub holds: RunHistoryHolds,
}
impl PlaceRunHistoryHold {
    pub fn request_digest(&self) -> Result<Sha256Digest, HistoryRetentionError> {
        insight_platform_contracts::canonical_digest(&serde_json::json!({"operation":"run.history_hold.place","version":1,"tenant_id":self.audit.tenant_id,"run_id":self.run_id,"expected_run_version":self.expected_run_version,"reason_evidence_digest":self.reason_evidence_digest})).map_err(|_|HistoryRetentionError::InvalidHold)?.parse().map_err(|_|HistoryRetentionError::InvalidHold)
    }
}
impl ReleaseRunHistoryHold {
    pub fn request_digest(&self) -> Result<Sha256Digest, HistoryRetentionError> {
        insight_platform_contracts::canonical_digest(&serde_json::json!({"operation":"run.history_hold.release","version":1,"tenant_id":self.audit.tenant_id,"run_id":self.run_id,"expected_run_version":self.expected_run_version,"hold_key":self.hold_key,"release_evidence_digest":self.release_evidence_digest})).map_err(|_|HistoryRetentionError::InvalidHold)?.parse().map_err(|_|HistoryRetentionError::InvalidHold)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn initial_and_still_valid_old_cursor_have_distinct_semantics() {
        assert_eq!(PublicRunReadPosition::Initial.resolve(8, 13), Ok(8));
        assert_eq!(
            PublicRunReadPosition::AfterSequence { sequence: 7 }.resolve(8, 13),
            Err(PublicReplayError::HistoryGap { replay_floor: 8 })
        );
        assert_eq!(
            PublicRunReadPosition::AfterSequence { sequence: 8 }.resolve(8, 13),
            Ok(8)
        );
        assert_eq!(
            PublicRunReadPosition::AfterSequence { sequence: 14 }.resolve(8, 13),
            Err(PublicReplayError::CursorAhead)
        );
        assert_eq!(PublicRunReadPosition::Initial.resolve(13, 13), Ok(13));
    }
}

pub const MAX_HISTORY_RETENTION_RUN_SCAN: u16 = 128;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRetentionRunKey {
    pub tenant_id: ResourceId,
    pub run_id: ResourceId,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRetentionCursor {
    pub schema_version: u32,
    pub creation_cutoff: DateTime<Utc>,
    pub upper: HistoryRetentionRunKey,
    pub after: HistoryRetentionRunKey,
}
#[derive(Debug, Clone)]
pub struct ScanHistoryRetentionRuns {
    pub cursor: Option<HistoryRetentionCursor>,
    pub maximum_runs: u16,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRetentionRunCandidate {
    pub tenant_id: ResourceId,
    pub run_id: ResourceId,
    pub through_sequence: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRetentionRunPage {
    pub candidates: Vec<HistoryRetentionRunCandidate>,
    pub next_cursor: Option<HistoryRetentionCursor>,
}
impl ScanHistoryRetentionRuns {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), HistoryRetentionError> {
        let valid_key = |key: &HistoryRetentionRunKey| {
            key.tenant_id.kind() == ResourceKind::Tenant && key.run_id.kind() == ResourceKind::Run
        };
        if !(1..=MAX_HISTORY_RETENTION_RUN_SCAN).contains(&self.maximum_runs)
            || self.cursor.as_ref().is_some_and(|cursor| {
                cursor.schema_version != 1
                    || cursor.creation_cutoff > now
                    || !valid_key(&cursor.upper)
                    || !valid_key(&cursor.after)
                    || (&cursor.after.tenant_id, &cursor.after.run_id)
                        > (&cursor.upper.tenant_id, &cursor.upper.run_id)
            })
        {
            return Err(HistoryRetentionError::InvalidPolicy);
        }
        Ok(())
    }
}
