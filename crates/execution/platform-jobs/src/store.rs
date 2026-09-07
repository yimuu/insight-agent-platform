//! Durable Job command and read projections, independent of a storage driver.
use crate::JobError;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    ResourceId, ResourceKind, SchedulerPriority, Sha256Digest, TraceIdentityV1, TypedPayload,
};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobRecord {
    pub tenant_id: String,
    pub job_id: String,
    pub job_kind: String,
    pub work_class: String,
    pub owner_kind: String,
    pub owner_id: String,
    pub trace: TraceIdentityV1,
    pub invocation_id: Option<String>,
    pub run_id: Option<String>,
    pub node_id: Option<String>,
    pub state: String,
    pub version: i64,
    pub attempt_no: i32,
    pub attempt_limit: i32,
    pub lease_epoch: i64,
    pub worker_id: Option<String>,
    pub lease_token_digest: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub scheduled_at: DateTime<Utc>,
    pub retry_at: Option<DateTime<Utc>>,
    pub deadline: DateTime<Utc>,
    pub priority: SchedulerPriority,
    pub wake_kind: Option<String>,
    pub wake_state: Option<String>,
    pub wake_generation: i64,
    pub request_digest: String,
    pub result_digest: Option<String>,
    pub effect_key_digest: Option<String>,
    pub quota_reservation_id: Option<String>,
    pub payload: TypedPayload,
    pub execution_requirement: insight_platform_contracts::ExecutionRequirement,
    pub attempt_build_digest: Option<Sha256Digest>,
    pub started_at: Option<DateTime<Utc>>,
    pub terminal_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
#[derive(Debug, Clone)]
pub struct JobCommandFence {
    pub tenant_id: String,
    pub job_id: String,
    pub worker_id: ResourceId,
    pub lease_epoch: i64,
    pub expected_job_version: i64,
    pub lease_token_digest: Sha256Digest,
}
#[derive(Debug, Clone)]
pub struct HeartbeatJob {
    pub fence: JobCommandFence,
    pub lease_milliseconds: i64,
}

impl JobCommandFence {
    pub fn validate(&self) -> Result<(), JobError> {
        let tenant: ResourceId = self
            .tenant_id
            .parse()
            .map_err(|_| JobError::InvalidProjection)?;
        let job: ResourceId = self
            .job_id
            .parse()
            .map_err(|_| JobError::InvalidProjection)?;
        if tenant.kind() != ResourceKind::Tenant
            || job.kind() != ResourceKind::Job
            || self.worker_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_epoch <= 0
            || self.expected_job_version <= 0
        {
            return Err(JobError::InvalidProjection);
        }
        Ok(())
    }

    /// Converts the addressed storage command to the existing pure lease decision fence.
    pub fn decision_fence(&self) -> Result<crate::JobFence, JobError> {
        self.validate()?;
        Ok(crate::JobFence {
            expected_version: u64::try_from(self.expected_job_version)
                .map_err(|_| JobError::InvalidProjection)?,
            worker_process_generation_id: self.worker_id.clone(),
            lease_generation: u64::try_from(self.lease_epoch)
                .map_err(|_| JobError::InvalidProjection)?,
            token_digest: self.lease_token_digest.clone(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafetyScanShard {
    pub index: u16,
    pub count: u16,
}
impl SafetyScanShard {
    pub const fn whole() -> Self {
        Self { index: 0, count: 1 }
    }

    pub fn validate(self, maximum_shards: u16) -> Result<(), JobError> {
        if self.count == 0 || self.count > maximum_shards || self.index >= self.count {
            return Err(JobError::InvalidProjection);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafetyScanCursor {
    pub sort_at: DateTime<Utc>,
    pub tenant_id: ResourceId,
    pub item_id: ResourceId,
}
impl SafetyScanCursor {
    pub fn validate(&self, item_kind: ResourceKind) -> Result<(), JobError> {
        if self.tenant_id.kind() != ResourceKind::Tenant || self.item_id.kind() != item_kind {
            return Err(JobError::InvalidProjection);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct SafetyScanPage<T> {
    pub records: Vec<T>,
    /// One safe diagnostic per invalid scanned object, bounded by the scan request.
    /// It records no successful mutation and never replaces the persisted object.
    pub diagnostics: Vec<SafeScanDiagnostic>,
    pub next_cursor: Option<SafetyScanCursor>,
    pub exhausted: bool,
}
impl<T> SafetyScanPage<T> {
    pub fn with_diagnostics(mut self, diagnostics: Vec<SafeScanDiagnostic>) -> Self {
        self.diagnostics = diagnostics;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SafetyScanPhase {
    JobDecode,
    OwnerDecode,
    OwnerValidation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SafetyScanDiagnosticCode {
    InvalidPersistedObject,
}

/// Operational evidence only: no payload, free-form error, cursor authority or repair instruction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeScanDiagnostic {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub item_id: ResourceId,
    pub phase: SafetyScanPhase,
    pub code: SafetyScanDiagnosticCode,
}

impl SafeScanDiagnostic {
    pub fn validate(&self) -> Result<(), JobError> {
        if self.schema_version != 1 || self.tenant_id.kind() != ResourceKind::Tenant {
            return Err(JobError::InvalidProjection);
        }
        Ok(())
    }
}
impl<T> Deref for SafetyScanPage<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.records
    }
}
impl<T> IntoIterator for SafetyScanPage<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}
impl<'a, T> IntoIterator for &'a SafetyScanPage<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.iter()
    }
}

pub fn validate_safety_scan_request(
    shard: SafetyScanShard,
    after: Option<&SafetyScanCursor>,
    cursor_item_kind: ResourceKind,
    limit: u16,
    slot_count: usize,
    maximum_batch: u16,
    maximum_shards: u16,
) -> Result<(), JobError> {
    shard.validate(maximum_shards)?;
    if let Some(cursor) = after {
        cursor.validate(cursor_item_kind)?;
    }
    if limit == 0 || limit > maximum_batch || slot_count != usize::from(limit) {
        return Err(JobError::InvalidProjection);
    }
    Ok(())
}

pub const MAX_JOB_LEASE_MILLISECONDS: i64 = 120_000;
