//! Current, credential-free model connection observations. This is not durable Model execution.
use crate::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const MODEL_PROBE_DEADLINE_SECONDS: i64 = 30;
pub const MODEL_PROBE_CONNECT_MILLISECONDS: u64 = 5_000;
pub const MODEL_PROBE_MAXIMUM_REQUEST_BYTES: usize = 4096;
pub const MODEL_PROBE_MAXIMUM_RESPONSE_BYTES: usize = 65_536;
pub const MODEL_PROBE_MAXIMUM_OUTPUT_TOKENS: u32 = 32;
pub const MODEL_PROBE_MAXIMUM_IN_FLIGHT: usize = 4;
pub const MODEL_PROBE_MAXIMUM_METADATA_BYTES: usize = 65_536;
pub const MODEL_PROBE_PROMPT: &str = "Reply with OK.";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConnectionProbeRequestV1 {
    pub schema_version: u16,
    pub installation_digest: Sha256Digest,
    pub model_deployment: ExactDeploymentRef,
}
impl ModelConnectionProbeRequestV1 {
    pub fn validate(&self) -> bool {
        self.schema_version == 1 && exact(&self.model_deployment, ResourceKind::ModelDeployment)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelConnectionOutcome {
    ResponseReceived,
    CredentialsRejected,
    ModelUnavailable,
    RateLimited,
    ProviderUnavailable,
    InvalidResponse,
    TimedOut,
    TransportUnavailable,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConnectionObservationV1 {
    pub schema_version: u16,
    pub model_deployment: ExactDeploymentRef,
    pub provider_deployment: ExactDeploymentRef,
    pub model_identity: ProviderModelIdentity,
    pub protocol: ModelProviderWireProtocol,
    pub observed_at: UtcTimestamp,
    pub outcome: ModelConnectionOutcome,
}
impl ModelConnectionObservationV1 {
    pub fn validate(&self) -> bool {
        self.schema_version == 1
            && exact(&self.model_deployment, ResourceKind::ModelDeployment)
            && exact(
                &self.provider_deployment,
                ResourceKind::ModelProviderDeployment,
            )
            && self.model_identity.validate().is_ok()
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelConnectionError {
    Rejected,
    NotFound,
    Conflict,
    Unavailable,
}

/// Authenticated Gateway metadata only. The Security owner resolves the target from current PG facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConnectionProbeAuthorizationV1 {
    pub schema_version: u16,
    pub request_id: ResourceId,
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub installation_digest: Sha256Digest,
    pub model_deployment: ExactDeploymentRef,
    pub environment: String,
    pub deadline: UtcTimestamp,
}
impl ModelConnectionProbeAuthorizationV1 {
    pub fn deadline_at(&self) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(self.deadline.as_str())
            .expect("validated nominal time")
            .with_timezone(&Utc)
    }
    pub fn validate_at(&self, now: DateTime<Utc>) -> bool {
        self.schema_version == 1
            && self.request_id.kind() == ResourceKind::ServerRequest
            && self.tenant_id.kind() == ResourceKind::Tenant
            && self.principal_id.kind() == ResourceKind::Principal
            && exact(&self.model_deployment, ResourceKind::ModelDeployment)
            && valid_environment(&self.environment)
            && self.deadline_at() > now
            && self.deadline_at() <= now + chrono::Duration::seconds(MODEL_PROBE_DEADLINE_SECONDS)
    }
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ModelConnectionError> {
        digest(self)
    }
}

/// A bounded projection of already-published definitions, not a second authority for those facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConnectionTargetV1 {
    pub schema_version: u16,
    pub model_deployment: ExactDeploymentRef,
    pub profile_revision: ExactVersionRef,
    pub provider_deployment: ExactDeploymentRef,
    pub provider: ModelProviderDeploymentClosure,
    pub model_identity: ProviderModelIdentity,
    pub protocol: ModelProviderWireProtocol,
    pub installed_adapter: InstalledModelAdapter,
    pub request_limits: ProviderRequestLimits,
    pub maximum_output_tokens: u32,
    pub maximum_input_text_bytes: u32,
    pub credential_generation: u64,
}
impl ModelConnectionTargetV1 {
    pub fn validate(&self) -> bool {
        self.schema_version == 1
            && exact(&self.model_deployment, ResourceKind::ModelDeployment)
            && exact(
                &self.provider_deployment,
                ResourceKind::ModelProviderDeployment,
            )
            && self.profile_revision.resource_kind == ResourceKind::ModelProfileRevision
            && self.profile_revision.validate().is_ok()
            && DeploymentClosure::ModelProvider(self.provider.clone())
                .validate()
                .is_ok()
            && TypedPayload::new(1, &DeploymentClosure::ModelProvider(self.provider.clone()))
                .ok()
                .is_some_and(|p| p.digest == self.provider_deployment.deployment_digest.as_str())
            && self.provider.secret_bindings.len() == 1
            && self.provider.secret_bindings[0].purpose.as_str() == MODEL_API_KEY_PURPOSE
            && self.credential_generation >= self.provider.secret_bindings[0].binding_generation
            && self.provider.secret_bindings[0].permits_resolved_generation(
                &self.provider.secret_bindings[0].secret_binding_id,
                &self.provider.secret_bindings[0].purpose,
                self.credential_generation,
            )
            && self.model_identity.validate().is_ok()
            && self.installed_adapter.validate().is_ok()
            && self.installed_adapter.qualified_name == self.protocol.qualified_name()
            && self.installed_adapter.adapter_contract_digest
                == self.protocol.adapter_contract_digest()
            && self.request_limits.validate().is_ok()
            && self.maximum_output_tokens > 0
            && self.maximum_output_tokens <= MODEL_PROBE_MAXIMUM_OUTPUT_TOKENS
            && self.maximum_input_text_bytes >= MODEL_PROBE_PROMPT.len() as u32
    }
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ModelConnectionError> {
        if !self.validate() {
            return Err(ModelConnectionError::Rejected);
        }
        digest(self)
    }
    pub fn observation(
        &self,
        outcome: ModelConnectionOutcome,
        now: DateTime<Utc>,
    ) -> ModelConnectionObservationV1 {
        ModelConnectionObservationV1 {
            schema_version: 1,
            model_deployment: self.model_deployment.clone(),
            provider_deployment: self.provider_deployment.clone(),
            model_identity: self.model_identity.clone(),
            protocol: self.protocol,
            observed_at: UtcTimestamp::from_datetime(now),
            outcome,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConnectionProbePermitV1 {
    pub schema_version: u16,
    pub request_digest: Sha256Digest,
    pub target_digest: Sha256Digest,
    pub target: ModelConnectionTargetV1,
    pub valid_until: UtcTimestamp,
}
impl ModelConnectionProbePermitV1 {
    pub fn validate_for(
        &self,
        request: &ModelConnectionProbeAuthorizationV1,
        now: DateTime<Utc>,
    ) -> bool {
        let until = DateTime::parse_from_rfc3339(self.valid_until.as_str())
            .expect("validated nominal time")
            .with_timezone(&Utc);
        self.schema_version == 1
            && request.validate_at(now)
            && until > now
            && until <= request.deadline_at()
            && request.canonical_digest().as_ref() == Ok(&self.request_digest)
            && self.target.canonical_digest().as_ref() == Ok(&self.target_digest)
            && self.target.model_deployment == request.model_deployment
    }
}

fn exact(reference: &ExactDeploymentRef, kind: ResourceKind) -> bool {
    reference.resource_kind == kind && reference.validate().is_ok()
}
fn valid_environment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'))
}
fn digest(value: &impl Serialize) -> Result<Sha256Digest, ModelConnectionError> {
    let value = serde_json::to_value(value).map_err(|_| ModelConnectionError::Rejected)?;
    if canonical_json(&value)
        .map_err(|_| ModelConnectionError::Rejected)?
        .len()
        > MODEL_PROBE_MAXIMUM_METADATA_BYTES
    {
        return Err(ModelConnectionError::Rejected);
    }
    canonical_digest(&value)
        .map_err(|_| ModelConnectionError::Rejected)?
        .parse()
        .map_err(|_| ModelConnectionError::Rejected)
}
