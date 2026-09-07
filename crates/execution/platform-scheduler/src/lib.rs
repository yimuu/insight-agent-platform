//! Bounded scheduling policy validation for the shared, partitioned Job authority.
//! PostgreSQL owns candidate enumeration, cursors and persisted per-tenant credit.
//! The partitioned module makes the only current admission/accounting decision.

pub mod partitioned;
use insight_platform_contracts::{canonical_digest, ResourceId, ResourceKind, Sha256Digest};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerHardLimits {
    pub maximum_deficit: u64,
    pub maximum_tenants: u16,
    pub maximum_window_per_tenant: u16,
    pub maximum_batch: u16,
}

impl SchedulerHardLimits {
    pub fn validate(&self) -> Result<(), ScheduleError> {
        if self.maximum_deficit == 0
            || self.maximum_tenants == 0
            || self.maximum_window_per_tenant == 0
            || self.maximum_batch == 0
        {
            return Err(ScheduleError::InvalidPolicy);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantSchedulingPolicyBinding {
    pub tenant_id: ResourceId,
    pub policy_version_id: ResourceId,
    pub policy_version_digest: Sha256Digest,
    pub rules_digest: Sha256Digest,
    pub weight: u16,
    pub burst: u16,
    pub aging_rounds: u16,
}

impl TenantSchedulingPolicyBinding {
    pub fn validate(&self) -> Result<(), ScheduleError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.policy_version_id.kind() != ResourceKind::PolicyRevision
            || self.weight == 0
            || self.burst == 0
            || self.aging_rounds == 0
        {
            return Err(ScheduleError::InvalidPolicy);
        }
        let document = serde_json::json!({
            "version": 1,
            "weight": self.weight,
            "burst": self.burst,
            "aging_rounds": self.aging_rounds,
        });
        let digest: Sha256Digest = canonical_digest(&document)
            .map_err(|_| ScheduleError::Canonicalization)?
            .parse()
            .map_err(|_| ScheduleError::Canonicalization)?;
        if digest != self.rules_digest {
            return Err(ScheduleError::PolicyDigestMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleError {
    InvalidPolicy,
    PolicyDigestMismatch,
    Canonicalization,
}
impl fmt::Display for ScheduleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidPolicy => "scheduling policy is invalid",
            Self::PolicyDigestMismatch => "scheduling policy digest differs from its frozen rules",
            Self::Canonicalization => "scheduling policy cannot be canonicalized",
        })
    }
}
impl Error for ScheduleError {}
