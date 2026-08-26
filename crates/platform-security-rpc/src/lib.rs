//! Versioned internal gRPC boundary between the Egress Broker and SecretBinding authority.
//!
//! The client implements only the two security ports needed by the Secret Broker. The server
//! delegates only those same ports. Production TLS configuration verifies the private CA; the
//! interceptor below additionally authorizes exactly the Egress Broker URI SAN before decoding a
//! request body.

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, CommandAudit, ExactSecretBindingRef, JsonLimits,
    PrincipalKind, ResourceId, SecretBindingPayload, SecretBindingState, SecretPurpose,
    Sha256Digest, TraceIdentityV1,
};
use insight_platform_rpc_trace::{require_trace_interceptor, PropagateTrace};
use insight_platform_security::{
    EncryptedOpaqueReference, PreparedSecretBindingAuthority,
    PreparedSecretBindingRegistrationDisposition, PreparedSecretBindingRegistrationError,
    PreparedSecretBindingRegistrationOutcome, RegisterPreparedSecretBinding,
    SecretBindingResolutionAuthority, SecretBindingResolutionError, SecretBindingResolutionRecord,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{error::Error, fmt, sync::Arc};
use tonic::{Request, Response, Status};
use x509_parser::{extensions::GeneralName, parse_x509_certificate};

pub mod proto {
    tonic::include_proto!("insight.platform.v1");
}

use proto::{
    security_secret_authority_service_client::SecuritySecretAuthorityServiceClient,
    security_secret_authority_service_server::SecuritySecretAuthorityService,
    ClosedSecurityEnvelope,
};

pub const SECURITY_INTERNAL_RPC_SCHEMA_VERSION: u32 = 1;
pub const EGRESS_BROKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/egress-broker";
pub const MAX_SECURITY_INTERNAL_RPC_BYTES_HARD: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecurityInternalRpcLimits {
    maximum_message_bytes: usize,
}

impl SecurityInternalRpcLimits {
    pub fn new(maximum_message_bytes: usize) -> Result<Self, SecurityRpcError> {
        if !(1..=MAX_SECURITY_INTERNAL_RPC_BYTES_HARD).contains(&maximum_message_bytes) {
            return Err(SecurityRpcError::InvalidConfiguration);
        }
        Ok(Self {
            maximum_message_bytes,
        })
    }

    pub const fn maximum_message_bytes(self) -> usize {
        self.maximum_message_bytes
    }
}

impl Default for SecurityInternalRpcLimits {
    fn default() -> Self {
        Self {
            maximum_message_bytes: MAX_SECURITY_INTERNAL_RPC_BYTES_HARD,
        }
    }
}

/// Endpoint-role authorization after tonic/rustls has verified the client certificate chain.
#[derive(Debug, Clone, Copy, Default)]
pub struct EgressBrokerWorkloadIdentity;

impl tonic::service::Interceptor for EgressBrokerWorkloadIdentity {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        let leaf = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        require_exact_workload_uri(leaf.as_ref(), EGRESS_BROKER_WORKLOAD_IDENTITY)?;
        require_trace_interceptor(request)
    }
}

fn require_exact_workload_uri(certificate: &[u8], expected: &str) -> Result<(), Status> {
    let (remainder, certificate) = parse_x509_certificate(certificate)
        .map_err(|_| Status::unauthenticated("client certificate is invalid"))?;
    if !remainder.is_empty() {
        return Err(Status::unauthenticated("client certificate is invalid"));
    }
    let alternative_names = certificate
        .subject_alternative_name()
        .map_err(|_| Status::unauthenticated("client certificate identity is invalid"))?
        .ok_or_else(|| Status::permission_denied("workload identity is not authorized"))?;
    let mut uris = alternative_names
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            GeneralName::URI(uri) => Some(*uri),
            _ => None,
        });
    if uris.next() != Some(expected) || uris.next().is_some() {
        return Err(Status::permission_denied(
            "workload identity is not authorized",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LoadSecretBindingRequest {
    schema_version: u32,
    tenant_id: ResourceId,
    secret_binding_id: ResourceId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretBindingResolutionWire {
    schema_version: u32,
    tenant_id: ResourceId,
    secret_binding_id: ResourceId,
    purpose: SecretPurpose,
    provider_id: ResourceId,
    state: SecretBindingState,
    generation: u64,
    encrypted_reference_base64: String,
    key_id: String,
    reference_digest: Sha256Digest,
    payload: SecretBindingPayload,
}

impl TryFrom<SecretBindingResolutionRecord> for SecretBindingResolutionWire {
    type Error = SecurityRpcError;

    fn try_from(value: SecretBindingResolutionRecord) -> Result<Self, Self::Error> {
        value
            .validate()
            .map_err(|_| SecurityRpcError::InvalidEnvelope)?;
        Ok(Self {
            schema_version: 1,
            tenant_id: value.tenant_id,
            secret_binding_id: value.secret_binding_id,
            purpose: value.purpose,
            provider_id: value.provider_id,
            state: value.state,
            generation: value.generation,
            encrypted_reference_base64: BASE64.encode(value.encrypted_reference.as_bytes()),
            key_id: value.key_id,
            reference_digest: value.reference_digest,
            payload: value.payload,
        })
    }
}

impl TryFrom<SecretBindingResolutionWire> for SecretBindingResolutionRecord {
    type Error = SecurityRpcError;

    fn try_from(value: SecretBindingResolutionWire) -> Result<Self, Self::Error> {
        if value.schema_version != 1 {
            return Err(SecurityRpcError::InvalidEnvelope);
        }
        let encrypted = BASE64
            .decode(value.encrypted_reference_base64)
            .map_err(|_| SecurityRpcError::InvalidEnvelope)?;
        let record = Self {
            tenant_id: value.tenant_id,
            secret_binding_id: value.secret_binding_id,
            purpose: value.purpose,
            provider_id: value.provider_id,
            state: value.state,
            generation: value.generation,
            encrypted_reference: EncryptedOpaqueReference::new(encrypted)
                .map_err(|_| SecurityRpcError::InvalidEnvelope)?,
            key_id: value.key_id,
            reference_digest: value.reference_digest,
            payload: value.payload,
        };
        record
            .validate()
            .map_err(|_| SecurityRpcError::InvalidEnvelope)?;
        Ok(record)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandAuditWire {
    trace: TraceIdentityV1,
    tenant_id: ResourceId,
    principal_id: ResourceId,
    principal_kind: PrincipalKind,
    receipt_id: ResourceId,
    event_id: ResourceId,
    outbox_id: ResourceId,
    idempotency_key_digest: Sha256Digest,
    request_digest: Sha256Digest,
    receipt_expires_at: DateTime<Utc>,
}

impl From<CommandAudit> for CommandAuditWire {
    fn from(value: CommandAudit) -> Self {
        Self {
            trace: value.trace,
            tenant_id: value.tenant_id,
            principal_id: value.principal_id,
            principal_kind: value.principal_kind,
            receipt_id: value.receipt_id,
            event_id: value.event_id,
            outbox_id: value.outbox_id,
            idempotency_key_digest: value.idempotency_key_digest,
            request_digest: value.request_digest,
            receipt_expires_at: value.receipt_expires_at,
        }
    }
}

impl From<CommandAuditWire> for CommandAudit {
    fn from(value: CommandAuditWire) -> Self {
        Self {
            trace: value.trace,
            tenant_id: value.tenant_id,
            principal_id: value.principal_id,
            principal_kind: value.principal_kind,
            receipt_id: value.receipt_id,
            event_id: value.event_id,
            outbox_id: value.outbox_id,
            idempotency_key_digest: value.idempotency_key_digest,
            request_digest: value.request_digest,
            receipt_expires_at: value.receipt_expires_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterPreparedSecretBindingWire {
    schema_version: u32,
    audit: CommandAuditWire,
    preparation_digest: Sha256Digest,
    secret_binding_id: ResourceId,
    purpose: SecretPurpose,
    provider_id: ResourceId,
    encrypted_reference_base64: String,
    key_id: String,
    reference_digest: Sha256Digest,
    opaque_version_identity_digest: Sha256Digest,
    provider_storage_evidence_digest: Sha256Digest,
}

impl From<RegisterPreparedSecretBinding> for RegisterPreparedSecretBindingWire {
    fn from(value: RegisterPreparedSecretBinding) -> Self {
        Self {
            schema_version: 1,
            audit: value.audit.into(),
            preparation_digest: value.preparation_digest,
            secret_binding_id: value.secret_binding_id,
            purpose: value.purpose,
            provider_id: value.provider_id,
            encrypted_reference_base64: BASE64.encode(value.encrypted_reference.as_bytes()),
            key_id: value.key_id,
            reference_digest: value.reference_digest,
            opaque_version_identity_digest: value.opaque_version_identity_digest,
            provider_storage_evidence_digest: value.provider_storage_evidence_digest,
        }
    }
}

impl TryFrom<RegisterPreparedSecretBindingWire> for RegisterPreparedSecretBinding {
    type Error = SecurityRpcError;

    fn try_from(value: RegisterPreparedSecretBindingWire) -> Result<Self, Self::Error> {
        if value.schema_version != 1 {
            return Err(SecurityRpcError::InvalidEnvelope);
        }
        let encrypted = BASE64
            .decode(value.encrypted_reference_base64)
            .map_err(|_| SecurityRpcError::InvalidEnvelope)?;
        Ok(Self {
            audit: value.audit.into(),
            preparation_digest: value.preparation_digest,
            secret_binding_id: value.secret_binding_id,
            purpose: value.purpose,
            provider_id: value.provider_id,
            encrypted_reference: EncryptedOpaqueReference::new(encrypted)
                .map_err(|_| SecurityRpcError::InvalidEnvelope)?,
            key_id: value.key_id,
            reference_digest: value.reference_digest,
            opaque_version_identity_digest: value.opaque_version_identity_digest,
            provider_storage_evidence_digest: value.provider_storage_evidence_digest,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RegistrationDispositionWire {
    Applied,
    Replayed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedRegistrationOutcomeWire {
    schema_version: u32,
    disposition: RegistrationDispositionWire,
    exact_binding: ExactSecretBindingRef,
}

impl From<PreparedSecretBindingRegistrationOutcome> for PreparedRegistrationOutcomeWire {
    fn from(value: PreparedSecretBindingRegistrationOutcome) -> Self {
        Self {
            schema_version: 1,
            disposition: match value.disposition {
                PreparedSecretBindingRegistrationDisposition::Applied => {
                    RegistrationDispositionWire::Applied
                }
                PreparedSecretBindingRegistrationDisposition::Replayed => {
                    RegistrationDispositionWire::Replayed
                }
            },
            exact_binding: value.exact_binding,
        }
    }
}

impl TryFrom<PreparedRegistrationOutcomeWire> for PreparedSecretBindingRegistrationOutcome {
    type Error = SecurityRpcError;

    fn try_from(value: PreparedRegistrationOutcomeWire) -> Result<Self, Self::Error> {
        if value.schema_version != 1 || value.exact_binding.validate().is_err() {
            return Err(SecurityRpcError::InvalidEnvelope);
        }
        Ok(Self {
            disposition: match value.disposition {
                RegistrationDispositionWire::Applied => {
                    PreparedSecretBindingRegistrationDisposition::Applied
                }
                RegistrationDispositionWire::Replayed => {
                    PreparedSecretBindingRegistrationDisposition::Replayed
                }
            },
            exact_binding: value.exact_binding,
        })
    }
}

#[derive(Clone)]
pub struct SecuritySecretAuthorityGrpcClient {
    client: TracedSecuritySecretAuthorityServiceClient,
    limits: SecurityInternalRpcLimits,
}

impl SecuritySecretAuthorityGrpcClient {
    pub fn new(channel: tonic::transport::Channel, limits: SecurityInternalRpcLimits) -> Self {
        let maximum = limits.maximum_message_bytes();
        Self {
            client: SecuritySecretAuthorityServiceClient::with_interceptor(channel, PropagateTrace)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            limits,
        }
    }
}

type TracedSecuritySecretAuthorityServiceClient = SecuritySecretAuthorityServiceClient<
    tonic::service::interceptor::InterceptedService<tonic::transport::Channel, PropagateTrace>,
>;

#[async_trait]
impl SecretBindingResolutionAuthority for SecuritySecretAuthorityGrpcClient {
    async fn load_for_resolution(
        &self,
        tenant_id: &ResourceId,
        secret_binding_id: &ResourceId,
    ) -> Result<SecretBindingResolutionRecord, SecretBindingResolutionError> {
        let request = LoadSecretBindingRequest {
            schema_version: 1,
            tenant_id: tenant_id.clone(),
            secret_binding_id: secret_binding_id.clone(),
        };
        let mut client = self.client.clone();
        let response = client
            .load_secret_binding(Request::new(
                encode(&request, self.limits)
                    .map_err(|_| SecretBindingResolutionError::InvalidEvidence)?,
            ))
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                    SecretBindingResolutionError::Unavailable
                }
                tonic::Code::NotFound => SecretBindingResolutionError::NotFound,
                _ => SecretBindingResolutionError::InvalidEvidence,
            })?;
        let wire: SecretBindingResolutionWire = decode(response.into_inner(), self.limits)
            .map_err(|_| SecretBindingResolutionError::InvalidEvidence)?;
        let record = SecretBindingResolutionRecord::try_from(wire)
            .map_err(|_| SecretBindingResolutionError::InvalidEvidence)?;
        if &record.tenant_id != tenant_id || &record.secret_binding_id != secret_binding_id {
            return Err(SecretBindingResolutionError::InvalidEvidence);
        }
        Ok(record)
    }
}

#[async_trait]
impl PreparedSecretBindingAuthority for SecuritySecretAuthorityGrpcClient {
    async fn register_prepared(
        &self,
        command: RegisterPreparedSecretBinding,
    ) -> Result<PreparedSecretBindingRegistrationOutcome, PreparedSecretBindingRegistrationError>
    {
        let expected_binding = command
            .exact_binding()
            .map_err(|_| PreparedSecretBindingRegistrationError::Rejected)?;
        let wire = RegisterPreparedSecretBindingWire::from(command);
        let mut client = self.client.clone();
        let response = client
            .register_prepared_secret_binding(Request::new(
                encode(&wire, self.limits)
                    .map_err(|_| PreparedSecretBindingRegistrationError::Rejected)?,
            ))
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                    PreparedSecretBindingRegistrationError::TemporarilyUnavailable
                }
                _ => PreparedSecretBindingRegistrationError::Rejected,
            })?;
        let wire: PreparedRegistrationOutcomeWire = decode(response.into_inner(), self.limits)
            .map_err(|_| PreparedSecretBindingRegistrationError::Rejected)?;
        let outcome = PreparedSecretBindingRegistrationOutcome::try_from(wire)
            .map_err(|_| PreparedSecretBindingRegistrationError::Rejected)?;
        if outcome.exact_binding != expected_binding {
            return Err(PreparedSecretBindingRegistrationError::Rejected);
        }
        Ok(outcome)
    }
}

pub struct SecuritySecretAuthorityGrpcService<R, W> {
    resolution: Arc<R>,
    registration: Arc<W>,
    limits: SecurityInternalRpcLimits,
}

impl<R, W> SecuritySecretAuthorityGrpcService<R, W> {
    pub fn new(
        resolution: Arc<R>,
        registration: Arc<W>,
        limits: SecurityInternalRpcLimits,
    ) -> Self {
        Self {
            resolution,
            registration,
            limits,
        }
    }
}

#[tonic::async_trait]
impl<R, W> SecuritySecretAuthorityService for SecuritySecretAuthorityGrpcService<R, W>
where
    R: SecretBindingResolutionAuthority + 'static,
    W: PreparedSecretBindingAuthority + 'static,
{
    async fn load_secret_binding(
        &self,
        request: Request<ClosedSecurityEnvelope>,
    ) -> Result<Response<ClosedSecurityEnvelope>, Status> {
        let request: LoadSecretBindingRequest = decode(request.into_inner(), self.limits)?;
        if request.schema_version != 1 {
            return Err(Status::invalid_argument("invalid SecretBinding request"));
        }
        let record = self
            .resolution
            .load_for_resolution(&request.tenant_id, &request.secret_binding_id)
            .await
            .map_err(|failure| match failure {
                SecretBindingResolutionError::Unavailable => {
                    Status::unavailable("SecretBinding authority unavailable")
                }
                SecretBindingResolutionError::NotFound => {
                    Status::not_found("SecretBinding not found")
                }
                SecretBindingResolutionError::InvalidEvidence => {
                    Status::failed_precondition("SecretBinding authority evidence is invalid")
                }
            })?;
        if record.tenant_id != request.tenant_id
            || record.secret_binding_id != request.secret_binding_id
        {
            return Err(Status::failed_precondition(
                "SecretBinding authority evidence is invalid",
            ));
        }
        let wire = SecretBindingResolutionWire::try_from(record)?;
        Ok(Response::new(encode(&wire, self.limits)?))
    }

    async fn register_prepared_secret_binding(
        &self,
        request: Request<ClosedSecurityEnvelope>,
    ) -> Result<Response<ClosedSecurityEnvelope>, Status> {
        let wire: RegisterPreparedSecretBindingWire = decode(request.into_inner(), self.limits)?;
        let command = RegisterPreparedSecretBinding::try_from(wire)?;
        command
            .validate_at(Utc::now())
            .map_err(|_| Status::invalid_argument("invalid prepared SecretBinding command"))?;
        let expected_binding = command
            .exact_binding()
            .map_err(|_| Status::invalid_argument("invalid prepared SecretBinding command"))?;
        let outcome =
            self.registration.register_prepared(command).await.map_err(
                |failure| match failure {
                    PreparedSecretBindingRegistrationError::TemporarilyUnavailable => {
                        Status::unavailable("SecretBinding authority unavailable")
                    }
                    PreparedSecretBindingRegistrationError::Rejected => {
                        Status::failed_precondition("prepared SecretBinding command rejected")
                    }
                },
            )?;
        if outcome.exact_binding != expected_binding {
            return Err(Status::failed_precondition(
                "prepared SecretBinding authority evidence is invalid",
            ));
        }
        Ok(Response::new(encode(
            &PreparedRegistrationOutcomeWire::from(outcome),
            self.limits,
        )?))
    }
}

fn encode<T: Serialize>(
    value: &T,
    limits: SecurityInternalRpcLimits,
) -> Result<ClosedSecurityEnvelope, SecurityRpcError> {
    let canonical_json = serde_jcs::to_vec(value).map_err(|_| SecurityRpcError::InvalidEnvelope)?;
    if canonical_json.is_empty() || canonical_json.len() > limits.maximum_message_bytes {
        return Err(SecurityRpcError::InvalidEnvelope);
    }
    let parsed: serde_json::Value =
        serde_json::from_slice(&canonical_json).map_err(|_| SecurityRpcError::InvalidEnvelope)?;
    let payload_digest: Sha256Digest = canonical_digest(&parsed)
        .map_err(|_| SecurityRpcError::InvalidEnvelope)?
        .parse()
        .map_err(|_| SecurityRpcError::InvalidEnvelope)?;
    Ok(ClosedSecurityEnvelope {
        schema_version: SECURITY_INTERNAL_RPC_SCHEMA_VERSION,
        canonical_json,
        payload_digest: payload_digest.to_string(),
    })
}

fn decode<T: DeserializeOwned>(
    envelope: ClosedSecurityEnvelope,
    limits: SecurityInternalRpcLimits,
) -> Result<T, SecurityRpcError> {
    if envelope.schema_version != SECURITY_INTERNAL_RPC_SCHEMA_VERSION
        || envelope.canonical_json.is_empty()
        || envelope.canonical_json.len() > limits.maximum_message_bytes
    {
        return Err(SecurityRpcError::InvalidEnvelope);
    }
    let expected: Sha256Digest = envelope
        .payload_digest
        .parse()
        .map_err(|_| SecurityRpcError::InvalidEnvelope)?;
    let parsed = parse_strict_json(
        &envelope.canonical_json,
        JsonLimits {
            max_bytes: limits.maximum_message_bytes,
            max_depth: 32,
            max_properties_per_object: 64,
            max_items_per_array: 64,
            max_string_bytes: limits.maximum_message_bytes,
        },
    )
    .map_err(|_| SecurityRpcError::InvalidEnvelope)?;
    let actual: Sha256Digest = canonical_digest(&parsed)
        .map_err(|_| SecurityRpcError::InvalidEnvelope)?
        .parse()
        .map_err(|_| SecurityRpcError::InvalidEnvelope)?;
    if actual != expected
        || serde_jcs::to_vec(&parsed).map_err(|_| SecurityRpcError::InvalidEnvelope)?
            != envelope.canonical_json
    {
        return Err(SecurityRpcError::InvalidEnvelope);
    }
    serde_json::from_value(parsed).map_err(|_| SecurityRpcError::InvalidEnvelope)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityRpcError {
    InvalidConfiguration,
    InvalidEnvelope,
}

impl fmt::Display for SecurityRpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "Security RPC configuration is invalid",
            Self::InvalidEnvelope => "Security RPC envelope is invalid",
        })
    }
}

impl Error for SecurityRpcError {}

impl From<SecurityRpcError> for Status {
    fn from(_: SecurityRpcError) -> Self {
        Status::invalid_argument("invalid Security RPC envelope")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use insight_platform_contracts::{
        ResourceKind, SecretResolutionPolicy, TraceFlags, TraceIdentityV1,
    };
    use insight_platform_rpc_trace::{scope_trace, RpcTraceContext};
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose, SanType,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::oneshot;
    use tonic::transport::{
        server::TcpIncoming, Certificate, ClientTlsConfig, Endpoint, Identity, Server,
        ServerTlsConfig,
    };

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn purpose() -> SecretPurpose {
        "mcp.oauth.token".parse().unwrap()
    }

    fn rpc_trace() -> RpcTraceContext {
        RpcTraceContext::start(TraceIdentityV1::generate(), TraceFlags::NotSampled).unwrap()
    }

    fn resolution_record(
        tenant_id: ResourceId,
        secret_binding_id: ResourceId,
        provider_id: ResourceId,
    ) -> SecretBindingResolutionRecord {
        let policy = SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: digest('a'),
        };
        SecretBindingResolutionRecord {
            tenant_id,
            secret_binding_id,
            purpose: purpose(),
            provider_id: provider_id.clone(),
            state: SecretBindingState::Active,
            generation: 1,
            encrypted_reference: EncryptedOpaqueReference::new(b"ciphertext-only".to_vec())
                .unwrap(),
            key_id: "kms://fixture/key".to_owned(),
            reference_digest: digest('b'),
            payload: SecretBindingPayload {
                provider_id,
                resolution_policy: policy,
            },
        }
    }

    fn registration_command(
        tenant_id: ResourceId,
        secret_binding_id: ResourceId,
        provider_id: ResourceId,
    ) -> RegisterPreparedSecretBinding {
        let preparation_digest = digest('c');
        let mut command = RegisterPreparedSecretBinding {
            audit: CommandAudit {
                trace: insight_platform_contracts::TraceIdentityV1::generate(),
                tenant_id,
                principal_id: id(ResourceKind::Principal),
                principal_kind: PrincipalKind::ServiceIdentity,
                receipt_id: id(ResourceKind::Receipt),
                event_id: id(ResourceKind::Event),
                outbox_id: id(ResourceKind::OutboxEvent),
                idempotency_key_digest: preparation_digest.clone(),
                request_digest: digest('0'),
                receipt_expires_at: Utc::now() + Duration::hours(1),
            },
            preparation_digest,
            secret_binding_id,
            purpose: purpose(),
            provider_id,
            encrypted_reference: EncryptedOpaqueReference::new(b"prepared-ciphertext".to_vec())
                .unwrap(),
            key_id: "kms://fixture/key".to_owned(),
            reference_digest: digest('d'),
            opaque_version_identity_digest: digest('e'),
            provider_storage_evidence_digest: digest('f'),
        };
        command.audit.request_digest = command.semantic_request_digest().unwrap();
        command
    }

    struct RecordingAuthority {
        record: SecretBindingResolutionRecord,
        resolution_calls: AtomicUsize,
        registration_calls: AtomicUsize,
        drift_registration_outcome: AtomicBool,
    }

    #[async_trait]
    impl SecretBindingResolutionAuthority for RecordingAuthority {
        async fn load_for_resolution(
            &self,
            tenant_id: &ResourceId,
            secret_binding_id: &ResourceId,
        ) -> Result<SecretBindingResolutionRecord, SecretBindingResolutionError> {
            self.resolution_calls.fetch_add(1, Ordering::AcqRel);
            if tenant_id != &self.record.tenant_id
                || secret_binding_id != &self.record.secret_binding_id
            {
                return Err(SecretBindingResolutionError::NotFound);
            }
            Ok(self.record.clone())
        }
    }

    #[async_trait]
    impl PreparedSecretBindingAuthority for RecordingAuthority {
        async fn register_prepared(
            &self,
            command: RegisterPreparedSecretBinding,
        ) -> Result<PreparedSecretBindingRegistrationOutcome, PreparedSecretBindingRegistrationError>
        {
            self.registration_calls.fetch_add(1, Ordering::AcqRel);
            command
                .validate_at(Utc::now())
                .map_err(|_| PreparedSecretBindingRegistrationError::Rejected)?;
            let mut exact_binding = command
                .exact_binding()
                .map_err(|_| PreparedSecretBindingRegistrationError::Rejected)?;
            if self.drift_registration_outcome.load(Ordering::Acquire) {
                exact_binding.secret_binding_id = id(ResourceKind::SecretBinding);
            }
            Ok(PreparedSecretBindingRegistrationOutcome {
                disposition: PreparedSecretBindingRegistrationDisposition::Applied,
                exact_binding,
            })
        }
    }

    struct MtlsFixture {
        ca_pem: String,
        server_certificate_pem: String,
        server_key_pem: String,
        egress_certificate_pem: String,
        egress_key_pem: String,
        wrong_certificate_pem: String,
        wrong_key_pem: String,
    }

    fn mtls_fixture() -> MtlsFixture {
        let mut ca_parameters = CertificateParams::default();
        ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_parameters.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let ca = CertifiedIssuer::self_signed(ca_parameters, KeyPair::generate().unwrap()).unwrap();
        let issue = |subject_alt_names, extended_key_usage| {
            let mut parameters = CertificateParams::default();
            parameters.subject_alt_names = subject_alt_names;
            parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            parameters.extended_key_usages = vec![extended_key_usage];
            let key = KeyPair::generate().unwrap();
            let certificate = parameters.signed_by(&key, &ca).unwrap();
            (certificate.pem(), key.serialize_pem())
        };
        let (server_certificate_pem, server_key_pem) = issue(
            vec![SanType::DnsName("localhost".try_into().unwrap())],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let (egress_certificate_pem, egress_key_pem) = issue(
            vec![SanType::URI(
                EGRESS_BROKER_WORKLOAD_IDENTITY.try_into().unwrap(),
            )],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        let (wrong_certificate_pem, wrong_key_pem) = issue(
            vec![SanType::URI(
                "spiffe://insight.platform/workload/model-worker"
                    .try_into()
                    .unwrap(),
            )],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        MtlsFixture {
            ca_pem: ca.pem(),
            server_certificate_pem,
            server_key_pem,
            egress_certificate_pem,
            egress_key_pem,
            wrong_certificate_pem,
            wrong_key_pem,
        }
    }

    async fn channel(
        endpoint: &str,
        fixture: &MtlsFixture,
        certificate: &str,
        key: &str,
    ) -> tonic::transport::Channel {
        Endpoint::from_shared(endpoint.to_owned())
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .domain_name("localhost")
                    .ca_certificate(Certificate::from_pem(&fixture.ca_pem))
                    .identity(Identity::from_pem(certificate, key)),
            )
            .unwrap()
            .connect()
            .await
            .unwrap()
    }

    #[test]
    fn envelope_is_canonical_bounded_and_digest_bound() {
        let limits = SecurityInternalRpcLimits::default();
        let request = LoadSecretBindingRequest {
            schema_version: 1,
            tenant_id: id(ResourceKind::Tenant),
            secret_binding_id: id(ResourceKind::SecretBinding),
        };
        let envelope = encode(&request, limits).unwrap();
        let decoded: LoadSecretBindingRequest = decode(envelope.clone(), limits).unwrap();
        assert_eq!(decoded.tenant_id, request.tenant_id);

        let mut whitespace_tamper = envelope.clone();
        whitespace_tamper.canonical_json.push(b' ');
        assert!(matches!(
            decode::<LoadSecretBindingRequest>(whitespace_tamper, limits),
            Err(SecurityRpcError::InvalidEnvelope)
        ));

        let mut digest_tamper = envelope;
        digest_tamper.payload_digest = digest('9').to_string();
        assert!(matches!(
            decode::<LoadSecretBindingRequest>(digest_tamper, limits),
            Err(SecurityRpcError::InvalidEnvelope)
        ));
    }

    #[tokio::test]
    async fn real_mtls_allows_exact_egress_and_rejects_other_ca_valid_role() {
        let fixture = mtls_fixture();
        let tenant_id = id(ResourceKind::Tenant);
        let secret_binding_id = id(ResourceKind::SecretBinding);
        let provider_id = id(ResourceKind::SecretProvider);
        let authority = Arc::new(RecordingAuthority {
            record: resolution_record(
                tenant_id.clone(),
                secret_binding_id.clone(),
                provider_id.clone(),
            ),
            resolution_calls: AtomicUsize::new(0),
            registration_calls: AtomicUsize::new(0),
            drift_registration_outcome: AtomicBool::new(false),
        });
        let limits = SecurityInternalRpcLimits::default();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        let service = proto::security_secret_authority_service_server::SecuritySecretAuthorityServiceServer::new(
            SecuritySecretAuthorityGrpcService::new(
                Arc::clone(&authority),
                Arc::clone(&authority),
                limits,
            ),
        )
        .max_encoding_message_size(limits.maximum_message_bytes())
        .max_decoding_message_size(limits.maximum_message_bytes());
        let service = tonic::service::interceptor::InterceptedService::new(
            service,
            EgressBrokerWorkloadIdentity,
        );
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(
                &fixture.server_certificate_pem,
                &fixture.server_key_pem,
            ))
            .client_ca_root(Certificate::from_pem(&fixture.ca_pem));
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)
                .unwrap()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_receiver.await;
                })
                .await
                .unwrap();
        });
        let endpoint = format!("https://localhost:{}", address.port());

        let accepted_channel = channel(
            &endpoint,
            &fixture,
            &fixture.egress_certificate_pem,
            &fixture.egress_key_pem,
        )
        .await;
        let mut missing_trace_client =
            SecuritySecretAuthorityServiceClient::new(accepted_channel.clone());
        let missing_trace = missing_trace_client
            .load_secret_binding(Request::new(
                encode(
                    &LoadSecretBindingRequest {
                        schema_version: 1,
                        tenant_id: tenant_id.clone(),
                        secret_binding_id: secret_binding_id.clone(),
                    },
                    limits,
                )
                .unwrap(),
            ))
            .await
            .unwrap_err();
        assert_eq!(missing_trace.code(), tonic::Code::InvalidArgument);
        let client = SecuritySecretAuthorityGrpcClient::new(accepted_channel, limits);
        let resolved = scope_trace(
            rpc_trace(),
            client.load_for_resolution(&tenant_id, &secret_binding_id),
        )
        .await
        .unwrap();
        assert_eq!(resolved.tenant_id, tenant_id);
        assert_eq!(resolved.secret_binding_id, secret_binding_id);
        assert_eq!(authority.resolution_calls.load(Ordering::Acquire), 1);

        let command = registration_command(
            tenant_id.clone(),
            id(ResourceKind::SecretBinding),
            provider_id,
        );
        let expected = command.exact_binding().unwrap();
        let registered = scope_trace(rpc_trace(), client.register_prepared(command))
            .await
            .unwrap();
        assert_eq!(registered.exact_binding, expected);
        assert_eq!(authority.registration_calls.load(Ordering::Acquire), 1);

        authority
            .drift_registration_outcome
            .store(true, Ordering::Release);
        let rejected_drift = scope_trace(
            rpc_trace(),
            client.register_prepared(registration_command(
                tenant_id.clone(),
                id(ResourceKind::SecretBinding),
                expected.provider_id,
            )),
        )
        .await
        .unwrap_err();
        assert_eq!(
            rejected_drift,
            PreparedSecretBindingRegistrationError::Rejected
        );
        assert_eq!(authority.registration_calls.load(Ordering::Acquire), 2);

        let wrong_channel = channel(
            &endpoint,
            &fixture,
            &fixture.wrong_certificate_pem,
            &fixture.wrong_key_pem,
        )
        .await;
        let mut wrong_client = SecuritySecretAuthorityServiceClient::new(wrong_channel);
        let rejected = wrong_client
            .load_secret_binding(Request::new(
                encode(
                    &LoadSecretBindingRequest {
                        schema_version: 1,
                        tenant_id,
                        secret_binding_id,
                    },
                    limits,
                )
                .unwrap(),
            ))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);
        assert_eq!(authority.resolution_calls.load(Ordering::Acquire), 1);
        assert_eq!(authority.registration_calls.load(Ordering::Acquire), 2);

        drop(client);
        drop(missing_trace_client);
        drop(wrong_client);
        shutdown_sender.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }
}
