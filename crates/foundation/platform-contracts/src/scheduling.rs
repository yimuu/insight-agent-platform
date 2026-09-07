//! Stable scheduling identities and persistent cursor contracts. Jobs remain the work authority.

use crate::{ResourceId, ResourceKind, Sha256Digest, WorkClass};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{error::Error, fmt};

pub const SCHEDULER_STATE_VERSION: u32 = 1;
pub const SCHEDULER_PARTITION_MAPPING_VERSION: u32 = 1;
pub const SCHEDULER_PARTITION_COUNT: u16 = 256;
pub const SCHEDULER_PARTITION_DOMAIN: &[u8] = b"insight.scheduler.partition.v1\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchedulerPartitionId(pub u8);

impl SchedulerPartitionId {
    pub fn for_tenant(tenant: &ResourceId) -> Result<Self, SchedulingContractError> {
        if tenant.kind() != ResourceKind::Tenant {
            return Err(SchedulingContractError::WrongIdentity);
        }
        let mut digest = Sha256::new();
        digest.update(SCHEDULER_PARTITION_DOMAIN);
        digest.update(tenant.uuid().as_bytes());
        Ok(Self(digest.finalize()[0]))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulingLane {
    Business,
    RestrictedControl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimMode {
    NewAttempt,
    Continuation,
}

impl ClaimMode {
    pub const fn admitted_attempt_cost(self) -> u64 {
        match self {
            Self::NewAttempt => 1,
            Self::Continuation => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum SchedulingPolicyBinding {
    Unbound,
    Bound {
        policy_version_id: ResourceId,
        policy_version_digest: Sha256Digest,
        rules_digest: Sha256Digest,
    },
}

impl SchedulingPolicyBinding {
    pub fn validate(&self) -> Result<(), SchedulingContractError> {
        if let Self::Bound {
            policy_version_id, ..
        } = self
        {
            if policy_version_id.kind() != ResourceKind::PolicyRevision {
                return Err(SchedulingContractError::WrongIdentity);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobCreationKey {
    pub created_at: DateTime<Utc>,
    pub job_id: ResourceId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobSweepContinuation {
    pub creation_cutoff: DateTime<Utc>,
    pub upper_bound: JobCreationKey,
    pub after: Option<JobCreationKey>,
}

impl JobSweepContinuation {
    pub fn validate(&self) -> Result<(), SchedulingContractError> {
        if self.upper_bound.job_id.kind() != ResourceKind::Job
            || self.upper_bound.created_at >= self.creation_cutoff
            || self.after.as_ref().is_some_and(|after| {
                after.job_id.kind() != ResourceKind::Job || after > &self.upper_bound
            })
        {
            return Err(SchedulingContractError::InvalidCursor);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerPartitionState {
    pub schema_version: u32,
    pub work_class: WorkClass,
    pub partition_id: SchedulerPartitionId,
    pub current_round: u64,
    pub tenant_upper_bound: Option<ResourceId>,
    pub cursor_tenant_id: Option<ResourceId>,
}

impl SchedulerPartitionState {
    pub fn validate(&self) -> Result<(), SchedulingContractError> {
        if self.schema_version != SCHEDULER_STATE_VERSION || self.current_round > i64::MAX as u64 {
            return Err(SchedulingContractError::InvalidState);
        }
        for tenant in self
            .tenant_upper_bound
            .iter()
            .chain(self.cursor_tenant_id.iter())
        {
            if tenant.kind() != ResourceKind::Tenant
                || SchedulerPartitionId::for_tenant(tenant)? != self.partition_id
            {
                return Err(SchedulingContractError::WrongIdentity);
            }
        }
        if self.cursor_tenant_id.as_ref().is_some_and(|cursor| {
            self.tenant_upper_bound
                .as_ref()
                .is_none_or(|upper| cursor > upper)
        }) {
            return Err(SchedulingContractError::InvalidCursor);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantSchedulerState {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub work_class: WorkClass,
    pub partition_id: SchedulerPartitionId,
    pub policy: SchedulingPolicyBinding,
    pub deficit: u64,
    pub earliest_eligible_round: u64,
    pub credited_round: Option<u64>,
    pub last_served_round: Option<u64>,
    pub successful_claims: u64,
    pub job_sweep: Option<JobSweepContinuation>,
}

impl TenantSchedulerState {
    pub fn validate(&self, maximum_deficit: u64) -> Result<(), SchedulingContractError> {
        self.policy.validate()?;
        if self.schema_version != SCHEDULER_STATE_VERSION
            || SchedulerPartitionId::for_tenant(&self.tenant_id)? != self.partition_id
            || self.deficit > maximum_deficit
            || self.deficit > i64::MAX as u64
            || self.earliest_eligible_round > i64::MAX as u64
            || self
                .credited_round
                .is_some_and(|round| round > i64::MAX as u64)
            || self
                .last_served_round
                .is_some_and(|round| round > i64::MAX as u64)
            || self.successful_claims > i64::MAX as u64
            || (matches!(self.policy, SchedulingPolicyBinding::Unbound) && self.deficit != 0)
        {
            return Err(SchedulingContractError::InvalidState);
        }
        if let Some(sweep) = &self.job_sweep {
            sweep.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingContractError {
    WrongIdentity,
    InvalidCursor,
    InvalidState,
}
impl fmt::Display for SchedulingContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "scheduling contract: {self:?}")
    }
}
impl Error for SchedulingContractError {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partition_uses_uuid_bytes_not_nominal_text_or_worker_count() {
        let tenant: ResourceId = "ten_01951f3d-7b80-7b81-8d22-841bcc458faa".parse().unwrap();
        let mut input = SCHEDULER_PARTITION_DOMAIN.to_vec();
        input.extend_from_slice(&[
            0x01, 0x95, 0x1f, 0x3d, 0x7b, 0x80, 0x7b, 0x81, 0x8d, 0x22, 0x84, 0x1b, 0xcc, 0x45,
            0x8f, 0xaa,
        ]);
        assert_eq!(
            SchedulerPartitionId::for_tenant(&tenant).unwrap().0,
            Sha256::digest(input)[0]
        );
        let job = ResourceId::from_uuid_v7(ResourceKind::Job, tenant.uuid()).unwrap();
        assert_eq!(
            SchedulerPartitionId::for_tenant(&job),
            Err(SchedulingContractError::WrongIdentity)
        );
    }

    #[test]
    fn continuation_does_not_spend_another_attempt() {
        assert_eq!(ClaimMode::Continuation.admitted_attempt_cost(), 0);
        assert_eq!(ClaimMode::NewAttempt.admitted_attempt_cost(), 1);
    }
}
