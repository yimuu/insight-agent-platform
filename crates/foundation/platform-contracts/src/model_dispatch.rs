//! Metadata-only current authorization for a ModelWorker's exact physical dispatch.
//! The durable ModelTurn/Job and frozen closure remain the owning facts in PostgreSQL.

use crate::{
    DataRegion, ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef, ResourceId,
    ResourceKind, Sha256Digest, MAX_MODEL_CREDENTIAL_REQUIREMENTS,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDispatchAuthorizationV1 {
    pub schema_version: u16,
    pub tenant_id: ResourceId,
    pub model_turn_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub admission_digest: Sha256Digest,
    pub model_request_digest: Sha256Digest,
    pub provider_deployment: ExactDeploymentRef,
    pub provider_revision: ExactVersionRef,
    pub endpoint_identity_digest: Sha256Digest,
    pub secret_bindings: Vec<ExactSecretBindingRef>,
    pub network_policy: ExactVersionRef,
    pub tls_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub data_policy: ExactVersionRef,
    pub region: DataRegion,
    pub adapter_qualified_name: String,
    pub maximum_request_bytes: u32,
    pub maximum_response_bytes: u32,
    pub connect_timeout_milliseconds: u64,
    pub total_timeout_milliseconds: u64,
    pub deadline: DateTime<Utc>,
}

impl ModelDispatchAuthorizationV1 {
    pub fn validate_at(&self, now: DateTime<Utc>) -> bool {
        self.schema_version == 1
            && self.tenant_id.kind() == ResourceKind::Tenant
            && self.model_turn_id.kind() == ResourceKind::ModelTurn
            && self.job_id.kind() == ResourceKind::Job
            && self.worker_process_generation_id.kind() == ResourceKind::WorkerProcessGeneration
            && self.attempt_no > 0
            && self.lease_generation > 0
            && self.deadline > now
            && self.maximum_request_bytes > 0
            && self.maximum_request_bytes <= crate::MAX_MODEL_REQUEST_BYTES
            && self.maximum_response_bytes > 0
            && self.maximum_response_bytes <= crate::MAX_MODEL_RESPONSE_BYTES
            && self.connect_timeout_milliseconds > 0
            && self.connect_timeout_milliseconds < self.total_timeout_milliseconds
            && self.provider_deployment.resource_kind == ResourceKind::ModelProviderDeployment
            && self.provider_deployment.validate().is_ok()
            && self.provider_revision.resource_kind == ResourceKind::ModelProviderRevision
            && self.provider_revision.validate().is_ok()
            && !self.secret_bindings.is_empty()
            && self.secret_bindings.len() <= MAX_MODEL_CREDENTIAL_REQUIREMENTS
            && self
                .secret_bindings
                .iter()
                .all(|binding| binding.validate().is_ok())
            && self
                .secret_bindings
                .iter()
                .map(|binding| &binding.secret_binding_id)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == self.secret_bindings.len()
            && [
                &self.network_policy,
                &self.tls_policy,
                &self.trust_policy,
                &self.data_policy,
            ]
            .iter()
            .all(|policy| {
                policy.resource_kind == ResourceKind::PolicyRevision && policy.validate().is_ok()
            })
            && !self.adapter_qualified_name.is_empty()
            && self.adapter_qualified_name.len() <= crate::MAX_MODEL_ADAPTER_NAME_BYTES
            && self.adapter_qualified_name.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/')
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelDispatchAuthorizationError {
    Rejected,
    Unavailable,
}

/// Short-lived read authorization. It is not a one-shot dispatch token or an execution result.
/// The receiver checks the request identity and expiry before opening the physical transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDispatchPermitV1 {
    pub schema_version: u16,
    pub request_digest: Sha256Digest,
    pub valid_until: DateTime<Utc>,
}

impl ModelDispatchPermitV1 {
    pub fn validate_for(&self, request: &ModelDispatchAuthorizationV1, now: DateTime<Utc>) -> bool {
        self.schema_version == 1
            && self.valid_until > now
            && self.valid_until <= request.deadline
            && serde_json::to_value(request)
                .ok()
                .and_then(|value| crate::canonical_digest(&value).ok())
                .as_deref()
                == Some(self.request_digest.as_str())
    }
}
