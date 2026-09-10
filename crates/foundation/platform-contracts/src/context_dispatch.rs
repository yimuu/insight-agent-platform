//! Metadata-only authorization for a current Remote Context physical attempt.
use crate::{ResourceId, ResourceKind, Sha256Digest};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextDispatchAuthorizationV1 {
    pub schema_version: u16,
    pub tenant_id: ResourceId,
    pub context_query_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub physical_attempt: u32,
    pub lease_generation: u64,
    pub lease_token_digest: Sha256Digest,
    pub admission_digest: Sha256Digest,
    pub request_metadata_digest: Sha256Digest,
    pub input_content_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

impl ContextDispatchAuthorizationV1 {
    pub fn validate_at(&self, now: DateTime<Utc>) -> bool {
        self.schema_version == 1
            && self.tenant_id.kind() == ResourceKind::Tenant
            && self.context_query_id.kind() == ResourceKind::ContextQuery
            && self.job_id.kind() == ResourceKind::Job
            && self.worker_process_generation_id.kind() == ResourceKind::WorkerProcessGeneration
            && self.physical_attempt > 0
            && self.lease_generation > 0
            && self.deadline > now
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextDispatchAuthorizationError {
    Rejected,
    Unavailable,
}

/// Short-lived read authorization; outcome writes retain the original Job CAS fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextDispatchPermitV1 {
    pub schema_version: u16,
    pub request_digest: Sha256Digest,
    pub valid_until: DateTime<Utc>,
}

impl ContextDispatchPermitV1 {
    pub fn validate_for(
        &self,
        request: &ContextDispatchAuthorizationV1,
        now: DateTime<Utc>,
    ) -> bool {
        self.schema_version == 1
            && request.validate_at(now)
            && self.valid_until > now
            && self.valid_until <= request.deadline
            && serde_json::to_value(request)
                .ok()
                .and_then(|value| crate::canonical_digest(&value).ok())
                .as_deref()
                == Some(self.request_digest.as_str())
    }
}
