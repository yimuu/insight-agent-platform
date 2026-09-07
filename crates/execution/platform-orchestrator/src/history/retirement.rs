//! Safe metadata and decisions for bounded history retirement. No command body,
//! Secret material, object locator, or transport credential crosses this port.
use super::{HistoryRetentionPolicy, MAX_HISTORY_RETENTION_RUN_SCAN};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{ResourceId, ResourceKind};
use serde::{Deserialize, Serialize};

pub const HISTORY_RETIREMENT_VERSION: u32 = 1;
pub const MAX_HISTORY_OWNER_ROOTS: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryRetirementLane {
    Receipt,
    EventDelivery,
    OAuthTask,
}
impl HistoryRetirementLane {
    pub const ALL: [Self; 3] = [Self::Receipt, Self::EventDelivery, Self::OAuthTask];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Receipt => "receipt",
            Self::EventDelivery => "event_delivery",
            Self::OAuthTask => "oauth_task",
        }
    }
    pub fn accepts(self, id: &ResourceId) -> bool {
        id.kind()
            == match self {
                Self::Receipt => ResourceKind::Receipt,
                Self::EventDelivery => ResourceKind::Event,
                Self::OAuthTask => ResourceKind::Interaction,
            }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRecordKey {
    pub tenant_id: ResourceId,
    pub record_id: ResourceId,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRetirementCursor {
    pub schema_version: u32,
    pub lane: HistoryRetirementLane,
    pub creation_cutoff: DateTime<Utc>,
    pub upper: HistoryRecordKey,
    pub after: HistoryRecordKey,
}
#[derive(Debug, Clone)]
pub struct ScanHistoryRetirement {
    pub lane: HistoryRetirementLane,
    pub cursor: Option<HistoryRetirementCursor>,
    pub maximum_records: u16,
}
impl ScanHistoryRetirement {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), &'static str> {
        if !(1..=MAX_HISTORY_RETENTION_RUN_SCAN).contains(&self.maximum_records) {
            return Err("invalid retirement page bound");
        }
        if let Some(cursor) = &self.cursor {
            let key =
                |key: &HistoryRecordKey| (key.tenant_id.to_string(), key.record_id.to_string());
            if cursor.schema_version != HISTORY_RETIREMENT_VERSION
                || cursor.lane != self.lane
                || cursor.creation_cutoff > now
                || key(&cursor.after) > key(&cursor.upper)
                || [&cursor.after, &cursor.upper].iter().any(|key| {
                    key.tenant_id.kind() != ResourceKind::Tenant
                        || !self.lane.accepts(&key.record_id)
                })
            {
                return Err("invalid retirement cursor");
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct HistoryRetirementPage {
    pub candidates: Vec<HistoryRecordKey>,
    pub next_cursor: Option<HistoryRetirementCursor>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryRetainedReason {
    Window,
    ActiveEffect,
    UnknownEffect,
    Delivery,
    Hold,
    Reference,
    UnknownScope,
    Busy,
    Corrupt,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryRetirementOutcome {
    Retained(HistoryRetainedReason),
    Retired {
        receipts: u16,
        events: u16,
        outbox: u16,
        tasks: u16,
        jobs: u16,
    },
    AlreadyAbsent,
}

/// Complete current Receipt scope registry. The response reference is also
/// checked: collection-scoped creates are tied to their actual created owner.
pub fn known_receipt_scope(kind: &str, scope: &str) -> bool {
    if kind == "command"
        && insight_platform_contracts::PrincipalKind::ALL
            .iter()
            .any(|kind| scope == format!("tenant_principal_{}", kind.as_str()))
    {
        return true;
    }
    match kind {
        "command" => matches!(
            scope,
            "artifact"
                | "artifact_link"
                | "artifact_collection"
                | "capability_invocation"
                | "context_query"
                | "context_deployment"
                | "interaction"
                | "job"
                | "mcp_oauth_task"
                | "mcp_operation"
                | "mcp_deployment"
                | "model_turn"
                | "resource"
                | "resource_collection"
                | "run"
                | "run_admission"
                | "secret_binding"
                | "tenant"
        ),
        "job_commit" => scope == "job",
        "callback" => matches!(scope, "job" | "mcp_oauth_task" | "mcp_subscription"),
        _ => false,
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryReceiptWindow {
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub expires_at: DateTime<Utc>,
    pub claim_expires_at: Option<DateTime<Utc>>,
    pub owner_deadline: Option<DateTime<Utc>>,
}
impl HistoryReceiptWindow {
    pub fn ends_at(&self, policy: &HistoryRetentionPolicy) -> Option<DateTime<Utc>> {
        policy.validate().ok()?;
        let completed = self.completed_at?;
        if completed < self.created_at || self.expires_at <= self.created_at {
            return None;
        }
        Some(
            completed
                .checked_add_signed(Duration::seconds(policy.receipt_minimum_seconds as i64))?
                .max(self.expires_at)
                .max(self.claim_expires_at.unwrap_or(completed))
                .max(self.owner_deadline.unwrap_or(completed)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn policy() -> HistoryRetentionPolicy {
        HistoryRetentionPolicy {
            schema_version: 2,
            public_event_minimum_seconds: 1,
            audit_event_minimum_seconds: 1,
            receipt_minimum_seconds: 30,
            published_outbox_minimum_seconds: 1,
            cleanup_minimum_seconds: 1,
        }
    }
    #[test]
    fn receipt_window_covers_completion_claim_and_real_owner_deadline() {
        let now = Utc::now();
        let mut window = HistoryReceiptWindow {
            created_at: now,
            completed_at: Some(now + Duration::seconds(1)),
            expires_at: now + Duration::seconds(5),
            claim_expires_at: None,
            owner_deadline: None,
        };
        assert_eq!(window.ends_at(&policy()), Some(now + Duration::seconds(31)));
        window.claim_expires_at = Some(now + Duration::seconds(60));
        assert_eq!(window.ends_at(&policy()), Some(now + Duration::seconds(60)));
        window.owner_deadline = Some(now + Duration::seconds(120));
        assert_eq!(
            window.ends_at(&policy()),
            Some(now + Duration::seconds(120))
        );
        window.completed_at = None;
        assert_eq!(window.ends_at(&policy()), None);
        let mut unsupported = policy();
        unsupported.schema_version = 1;
        assert!(unsupported.validate().is_err());
    }
    #[test]
    fn every_dynamic_principal_scope_is_closed_and_unknown_receipts_are_preserved() {
        for kind in insight_platform_contracts::PrincipalKind::ALL {
            assert!(known_receipt_scope(
                "command",
                &format!("tenant_principal_{}", kind.as_str())
            ));
        }
        assert!(!known_receipt_scope(
            "command",
            "tenant_principal_future_admin"
        ));
        assert!(!known_receipt_scope("callback", "resource"));
        assert!(!known_receipt_scope("future_receipt", "job"));
        for scope in ["job", "mcp_subscription", "mcp_oauth_task"] {
            assert!(known_receipt_scope("callback", scope));
        }
    }
}
