//! Closed internal gRPC boundary between Capability Workers and the independent MCP Host.
//!
//! The wire contains only bounded JCS for nominal MCP contracts and outcomes. TLS chain validation
//! belongs to tonic/rustls composition; the server interceptor additionally authorizes one exact
//! Capability Worker URI SAN before any envelope is decoded.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_context::{
    ContextSubscriptionExecutionError, ContextSubscriptionRefreshAttempt,
    ContextSubscriptionRefreshBackend, ContextSubscriptionRefreshResponse,
};
use insight_platform_contracts::{
    checked_in_hard_limit_profile, parse_strict_json, JsonLimits, Sha256Digest,
};
use insight_platform_mcp_host::{
    McpHostClient, McpHostError, McpHostExecutionContract, McpOperationOutcome,
    McpOperationRequest, McpRemoteTaskCancelOutcome,
};
use insight_platform_rpc_trace::{
    request_with_current_trace, require_trace_interceptor, scope_trace, trace_context,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{error::Error, fmt, sync::Arc};
use tonic::{Request, Response, Status};
use x509_parser::{extensions::GeneralName, parse_x509_certificate};

pub mod proto {
    tonic::include_proto!("insight.platform.v1");
}

use proto::{
    mcp_host_execution_service_client::McpHostExecutionServiceClient,
    mcp_host_execution_service_server::McpHostExecutionService, ClosedMcpHostEnvelope,
};
use proto::{
    mcp_resource_refresh_service_client::McpResourceRefreshServiceClient,
    mcp_resource_refresh_service_server::McpResourceRefreshService,
};

pub const MCP_HOST_INTERNAL_RPC_SCHEMA_VERSION: u32 = 1;
pub const CAPABILITY_WORKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/capability-worker";
pub const CONTEXT_WORKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/context-worker";
const EXECUTE_OPERATION: &str = "mcp_host.execute.v1";
const EXECUTE_OUTCOME: &str = "mcp_host.execute_outcome.v1";
const CANCEL_OPERATION: &str = "mcp_host.cancel_remote_task.v1";
const CANCEL_OUTCOME: &str = "mcp_host.cancel_remote_task_outcome.v1";
const REFRESH_RESOURCES_OPERATION: &str = "mcp_host.refresh_resources.v1";
const REFRESH_RESOURCES_OUTCOME: &str = "mcp_host.refresh_resources_outcome.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpHostInternalRpcLimits {
    maximum_message_bytes: usize,
    maximum_json_depth: usize,
    maximum_properties_per_object: usize,
    maximum_items_per_array: usize,
}

impl McpHostInternalRpcLimits {
    pub fn new(maximum_message_bytes: usize) -> Result<Self, McpHostRpcError> {
        let profile = checked_in_hard_limit_profile();
        let maximum_contract_bytes = usize::try_from(
            profile
                .model_context_mcp
                .request_bytes
                .hard_max
                .min(profile.model_context_mcp.response_bytes.hard_max),
        )
        .map_err(|_| McpHostRpcError::InvalidConfiguration)?;
        if !(4_096..=maximum_contract_bytes).contains(&maximum_message_bytes) {
            return Err(McpHostRpcError::InvalidConfiguration);
        }
        Ok(Self {
            maximum_message_bytes,
            maximum_json_depth: usize::try_from(profile.api.json_depth.hard_max)
                .map_err(|_| McpHostRpcError::InvalidConfiguration)?,
            maximum_properties_per_object: usize::try_from(profile.api.json_properties.hard_max)
                .map_err(|_| McpHostRpcError::InvalidConfiguration)?,
            maximum_items_per_array: usize::try_from(profile.api.json_items.hard_max)
                .map_err(|_| McpHostRpcError::InvalidConfiguration)?,
        })
    }

    pub const fn maximum_message_bytes(self) -> usize {
        self.maximum_message_bytes
    }

    fn json_limits(self) -> JsonLimits {
        JsonLimits {
            max_bytes: self.maximum_message_bytes,
            max_depth: self.maximum_json_depth,
            max_properties_per_object: self.maximum_properties_per_object,
            max_items_per_array: self.maximum_items_per_array,
            max_string_bytes: self.maximum_message_bytes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecuteWire {
    schema_version: u32,
    contract: McpHostExecutionContract,
    request: McpOperationRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelWire {
    schema_version: u32,
    contract: McpHostExecutionContract,
    request: McpOperationRequest,
    deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RefreshResourcesWire {
    schema_version: u32,
    attempt: ContextSubscriptionRefreshAttempt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "status",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum RefreshResourcesWireOutcome {
    Succeeded(ContextSubscriptionRefreshResponse),
    Failed(ContextSubscriptionExecutionFailureCode),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ContextSubscriptionExecutionFailureCode {
    InvalidAttempt,
    InvalidEvidence,
    InvalidResponse,
    Rejected,
    Unavailable,
    CompletionUncertain,
    Canonicalization,
}

impl From<ContextSubscriptionExecutionError> for ContextSubscriptionExecutionFailureCode {
    fn from(value: ContextSubscriptionExecutionError) -> Self {
        match value {
            ContextSubscriptionExecutionError::InvalidAttempt => Self::InvalidAttempt,
            ContextSubscriptionExecutionError::InvalidEvidence => Self::InvalidEvidence,
            ContextSubscriptionExecutionError::InvalidResponse => Self::InvalidResponse,
            ContextSubscriptionExecutionError::Rejected => Self::Rejected,
            ContextSubscriptionExecutionError::Unavailable => Self::Unavailable,
            ContextSubscriptionExecutionError::CompletionUncertain => Self::CompletionUncertain,
            ContextSubscriptionExecutionError::Canonicalization => Self::Canonicalization,
        }
    }
}

impl From<ContextSubscriptionExecutionFailureCode> for ContextSubscriptionExecutionError {
    fn from(value: ContextSubscriptionExecutionFailureCode) -> Self {
        match value {
            ContextSubscriptionExecutionFailureCode::InvalidAttempt => Self::InvalidAttempt,
            ContextSubscriptionExecutionFailureCode::InvalidEvidence => Self::InvalidEvidence,
            ContextSubscriptionExecutionFailureCode::InvalidResponse => Self::InvalidResponse,
            ContextSubscriptionExecutionFailureCode::Rejected => Self::Rejected,
            ContextSubscriptionExecutionFailureCode::Unavailable => Self::Unavailable,
            ContextSubscriptionExecutionFailureCode::CompletionUncertain => {
                Self::CompletionUncertain
            }
            ContextSubscriptionExecutionFailureCode::Canonicalization => Self::Canonicalization,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "status",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum WireOutcome<T> {
    Succeeded(T),
    Failed(McpHostFailureCode),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum McpHostFailureCode {
    AuthorityUnavailable,
    CompletionUnknown,
    InvalidAuthorization,
    InvalidDiscovery,
    InvalidExecutionContract,
    InvalidOperation,
    InvalidOutcome,
    InvalidSession,
    InvalidSubscription,
    WrongTransport,
    Canonicalization,
}

impl From<McpHostError> for McpHostFailureCode {
    fn from(value: McpHostError) -> Self {
        match value {
            McpHostError::AuthorityUnavailable => Self::AuthorityUnavailable,
            McpHostError::CompletionUnknown => Self::CompletionUnknown,
            McpHostError::InvalidAuthorization => Self::InvalidAuthorization,
            McpHostError::InvalidDiscovery => Self::InvalidDiscovery,
            McpHostError::InvalidExecutionContract => Self::InvalidExecutionContract,
            McpHostError::InvalidOperation => Self::InvalidOperation,
            McpHostError::InvalidOutcome => Self::InvalidOutcome,
            McpHostError::InvalidSession => Self::InvalidSession,
            McpHostError::InvalidSubscription => Self::InvalidSubscription,
            McpHostError::WrongTransport => Self::WrongTransport,
            McpHostError::Canonicalization => Self::Canonicalization,
        }
    }
}

impl From<McpHostFailureCode> for McpHostError {
    fn from(value: McpHostFailureCode) -> Self {
        match value {
            McpHostFailureCode::AuthorityUnavailable => Self::AuthorityUnavailable,
            McpHostFailureCode::CompletionUnknown => Self::CompletionUnknown,
            McpHostFailureCode::InvalidAuthorization => Self::InvalidAuthorization,
            McpHostFailureCode::InvalidDiscovery => Self::InvalidDiscovery,
            McpHostFailureCode::InvalidExecutionContract => Self::InvalidExecutionContract,
            McpHostFailureCode::InvalidOperation => Self::InvalidOperation,
            McpHostFailureCode::InvalidOutcome => Self::InvalidOutcome,
            McpHostFailureCode::InvalidSession => Self::InvalidSession,
            McpHostFailureCode::InvalidSubscription => Self::InvalidSubscription,
            McpHostFailureCode::WrongTransport => Self::WrongTransport,
            McpHostFailureCode::Canonicalization => Self::Canonicalization,
        }
    }
}

#[derive(Clone)]
pub struct McpHostGrpcClient {
    client: McpHostExecutionServiceClient<tonic::transport::Channel>,
    limits: McpHostInternalRpcLimits,
}

impl McpHostGrpcClient {
    pub fn new(channel: tonic::transport::Channel, limits: McpHostInternalRpcLimits) -> Self {
        let maximum = limits.maximum_message_bytes();
        Self {
            client: McpHostExecutionServiceClient::new(channel)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            limits,
        }
    }
}

#[async_trait]
impl McpHostClient for McpHostGrpcClient {
    async fn execute(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
    ) -> Result<McpOperationOutcome, McpHostError> {
        contract.validate_canonical_at(Utc::now())?;
        request.validate_for(contract, Utc::now())?;
        let envelope = encode_envelope(
            EXECUTE_OPERATION,
            &ExecuteWire {
                schema_version: 1,
                contract: contract.clone(),
                request: request.clone(),
            },
            self.limits,
        )
        .map_err(|_| McpHostError::Canonicalization)?;
        let mut client = self.client.clone();
        let rpc_request =
            request_with_current_trace(envelope).map_err(|_| McpHostError::CompletionUnknown)?;
        let response = client
            .execute(rpc_request)
            .await
            .map_err(|_| McpHostError::CompletionUnknown)?
            .into_inner();
        match decode_envelope::<WireOutcome<McpOperationOutcome>>(
            response,
            EXECUTE_OUTCOME,
            self.limits,
        )
        .map_err(|_| McpHostError::InvalidOutcome)?
        {
            WireOutcome::Succeeded(outcome) => {
                outcome.validate_for(request, contract, Utc::now())?;
                Ok(outcome)
            }
            WireOutcome::Failed(failure) => Err(failure.into()),
        }
    }

    async fn cancel_remote_task(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
        deadline: DateTime<Utc>,
    ) -> Result<McpRemoteTaskCancelOutcome, McpHostError> {
        let envelope = encode_envelope(
            CANCEL_OPERATION,
            &CancelWire {
                schema_version: 1,
                contract: contract.clone(),
                request: request.clone(),
                deadline,
            },
            self.limits,
        )
        .map_err(|_| McpHostError::Canonicalization)?;
        let mut client = self.client.clone();
        let request =
            request_with_current_trace(envelope).map_err(|_| McpHostError::CompletionUnknown)?;
        let response = client
            .cancel_remote_task(request)
            .await
            .map_err(|_| McpHostError::CompletionUnknown)?
            .into_inner();
        match decode_envelope::<WireOutcome<McpRemoteTaskCancelOutcome>>(
            response,
            CANCEL_OUTCOME,
            self.limits,
        )
        .map_err(|_| McpHostError::InvalidOutcome)?
        {
            WireOutcome::Succeeded(outcome) => Ok(outcome),
            WireOutcome::Failed(failure) => Err(failure.into()),
        }
    }
}

#[derive(Clone)]
pub struct McpResourceRefreshGrpcClient {
    client: McpResourceRefreshServiceClient<tonic::transport::Channel>,
    limits: McpHostInternalRpcLimits,
}

impl McpResourceRefreshGrpcClient {
    pub fn new(channel: tonic::transport::Channel, limits: McpHostInternalRpcLimits) -> Self {
        let maximum = limits.maximum_message_bytes();
        Self {
            client: McpResourceRefreshServiceClient::new(channel)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            limits,
        }
    }
}

#[async_trait]
impl ContextSubscriptionRefreshBackend for McpResourceRefreshGrpcClient {
    async fn refresh_subscription_resources(
        &self,
        attempt: ContextSubscriptionRefreshAttempt,
    ) -> Result<ContextSubscriptionRefreshResponse, ContextSubscriptionExecutionError> {
        attempt.validate_at(Utc::now())?;
        let envelope = encode_envelope(
            REFRESH_RESOURCES_OPERATION,
            &RefreshResourcesWire {
                schema_version: 1,
                attempt: attempt.clone(),
            },
            self.limits,
        )
        .map_err(|_| ContextSubscriptionExecutionError::Canonicalization)?;
        let mut client = self.client.clone();
        let request = request_with_current_trace(envelope)
            .map_err(|_| ContextSubscriptionExecutionError::CompletionUncertain)?;
        let response = client
            .refresh_resources(request)
            .await
            .map_err(|_| ContextSubscriptionExecutionError::CompletionUncertain)?
            .into_inner();
        match decode_envelope::<RefreshResourcesWireOutcome>(
            response,
            REFRESH_RESOURCES_OUTCOME,
            self.limits,
        )
        .map_err(|_| ContextSubscriptionExecutionError::InvalidResponse)?
        {
            RefreshResourcesWireOutcome::Succeeded(response) => {
                response.validate_for(&attempt, Utc::now())?;
                Ok(response)
            }
            RefreshResourcesWireOutcome::Failed(failure) => Err(failure.into()),
        }
    }
}

pub struct McpResourceRefreshGrpcService<B> {
    backend: Arc<B>,
    limits: McpHostInternalRpcLimits,
}

impl<B> McpResourceRefreshGrpcService<B> {
    pub fn new(backend: Arc<B>, limits: McpHostInternalRpcLimits) -> Self {
        Self { backend, limits }
    }
}

#[tonic::async_trait]
impl<B> McpResourceRefreshService for McpResourceRefreshGrpcService<B>
where
    B: ContextSubscriptionRefreshBackend + 'static,
{
    async fn refresh_resources(
        &self,
        request: Request<ClosedMcpHostEnvelope>,
    ) -> Result<Response<ClosedMcpHostEnvelope>, Status> {
        let trace = trace_context(&request)?;
        scope_trace(trace, async {
            let wire: RefreshResourcesWire = decode_envelope(
                request.into_inner(),
                REFRESH_RESOURCES_OPERATION,
                self.limits,
            )?;
            if wire.schema_version != 1 || wire.attempt.validate_at(Utc::now()).is_err() {
                return Err(Status::invalid_argument(
                    "invalid MCP Resource Refresh request",
                ));
            }
            let outcome = match self
                .backend
                .refresh_subscription_resources(wire.attempt)
                .await
            {
                Ok(response) => RefreshResourcesWireOutcome::Succeeded(response),
                Err(failure) => RefreshResourcesWireOutcome::Failed(failure.into()),
            };
            Ok(Response::new(encode_envelope(
                REFRESH_RESOURCES_OUTCOME,
                &outcome,
                self.limits,
            )?))
        })
        .await
    }
}

pub struct McpHostGrpcService<C> {
    host: Arc<C>,
    limits: McpHostInternalRpcLimits,
}

impl<C> McpHostGrpcService<C> {
    pub fn new(host: Arc<C>, limits: McpHostInternalRpcLimits) -> Self {
        Self { host, limits }
    }
}

#[tonic::async_trait]
impl<C> McpHostExecutionService for McpHostGrpcService<C>
where
    C: McpHostClient + 'static,
{
    async fn execute(
        &self,
        request: Request<ClosedMcpHostEnvelope>,
    ) -> Result<Response<ClosedMcpHostEnvelope>, Status> {
        let trace = trace_context(&request)?;
        scope_trace(trace, async {
            let wire: ExecuteWire =
                decode_envelope(request.into_inner(), EXECUTE_OPERATION, self.limits)?;
            if wire.schema_version != 1 {
                return Err(Status::invalid_argument("invalid MCP Host request"));
            }
            let outcome = match self.host.execute(&wire.contract, &wire.request).await {
                Ok(outcome) => WireOutcome::Succeeded(outcome),
                Err(failure) => WireOutcome::Failed(failure.into()),
            };
            Ok(Response::new(encode_envelope(
                EXECUTE_OUTCOME,
                &outcome,
                self.limits,
            )?))
        })
        .await
    }

    async fn cancel_remote_task(
        &self,
        request: Request<ClosedMcpHostEnvelope>,
    ) -> Result<Response<ClosedMcpHostEnvelope>, Status> {
        let trace = trace_context(&request)?;
        scope_trace(trace, async {
            let wire: CancelWire =
                decode_envelope(request.into_inner(), CANCEL_OPERATION, self.limits)?;
            if wire.schema_version != 1 {
                return Err(Status::invalid_argument("invalid MCP Host cancel request"));
            }
            let outcome = match self
                .host
                .cancel_remote_task(&wire.contract, &wire.request, wire.deadline)
                .await
            {
                Ok(outcome) => WireOutcome::Succeeded(outcome),
                Err(failure) => WireOutcome::Failed(failure.into()),
            };
            Ok(Response::new(encode_envelope(
                CANCEL_OUTCOME,
                &outcome,
                self.limits,
            )?))
        })
        .await
    }
}

/// Authorizes the exact Capability Worker URI SAN before decoding the request body.
#[derive(Debug, Clone, Copy, Default)]
pub struct CapabilityWorkerWorkloadIdentity;

impl tonic::service::Interceptor for CapabilityWorkerWorkloadIdentity {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        let leaf = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        require_exact_workload_uri(leaf.as_ref(), CAPABILITY_WORKER_WORKLOAD_IDENTITY)?;
        require_trace_interceptor(request)
    }
}

/// Authorizes the exact Context Worker URI SAN before decoding a Resource Refresh request.
#[derive(Debug, Clone, Copy, Default)]
pub struct ContextWorkerWorkloadIdentity;

impl tonic::service::Interceptor for ContextWorkerWorkloadIdentity {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        let leaf = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        require_exact_workload_uri(leaf.as_ref(), CONTEXT_WORKER_WORKLOAD_IDENTITY)?;
        require_trace_interceptor(request)
    }
}

fn require_exact_workload_uri(certificate: &[u8], expected: &str) -> Result<(), Status> {
    let (remainder, certificate) = parse_x509_certificate(certificate)
        .map_err(|_| Status::unauthenticated("client certificate is invalid"))?;
    if !remainder.is_empty() {
        return Err(Status::unauthenticated("client certificate is invalid"));
    }
    let names = certificate
        .subject_alternative_name()
        .map_err(|_| Status::unauthenticated("client certificate identity is invalid"))?
        .ok_or_else(|| Status::permission_denied("workload identity is not authorized"))?;
    let uris = names
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            GeneralName::URI(uri) => Some(*uri),
            _ => None,
        })
        .collect::<Vec<_>>();
    if uris.as_slice() != [expected] {
        return Err(Status::permission_denied(
            "workload identity is not authorized",
        ));
    }
    Ok(())
}

fn encode_envelope<T: Serialize>(
    operation: &str,
    value: &T,
    limits: McpHostInternalRpcLimits,
) -> Result<ClosedMcpHostEnvelope, McpHostRpcError> {
    if !valid_operation(operation) {
        return Err(McpHostRpcError::InvalidEnvelope);
    }
    let metadata_jcs = serde_jcs::to_vec(value).map_err(|_| McpHostRpcError::InvalidEnvelope)?;
    if metadata_jcs.is_empty() || metadata_jcs.len() > limits.maximum_message_bytes {
        return Err(McpHostRpcError::InvalidEnvelope);
    }
    let metadata_digest = envelope_digest(operation, &metadata_jcs);
    Ok(ClosedMcpHostEnvelope {
        schema_version: MCP_HOST_INTERNAL_RPC_SCHEMA_VERSION,
        operation: operation.to_owned(),
        metadata_jcs,
        metadata_digest: metadata_digest.to_string(),
    })
}

fn decode_envelope<T: DeserializeOwned>(
    envelope: ClosedMcpHostEnvelope,
    operation: &str,
    limits: McpHostInternalRpcLimits,
) -> Result<T, McpHostRpcError> {
    if envelope.schema_version != MCP_HOST_INTERNAL_RPC_SCHEMA_VERSION
        || envelope.operation != operation
        || envelope.metadata_jcs.is_empty()
        || envelope.metadata_jcs.len() > limits.maximum_message_bytes
        || envelope.metadata_digest.parse::<Sha256Digest>().ok()
            != Some(envelope_digest(operation, &envelope.metadata_jcs))
    {
        return Err(McpHostRpcError::InvalidEnvelope);
    }
    let value = parse_strict_json(&envelope.metadata_jcs, limits.json_limits())
        .map_err(|_| McpHostRpcError::InvalidEnvelope)?;
    if serde_jcs::to_vec(&value).ok().as_deref() != Some(envelope.metadata_jcs.as_slice()) {
        return Err(McpHostRpcError::InvalidEnvelope);
    }
    serde_json::from_value(value).map_err(|_| McpHostRpcError::InvalidEnvelope)
}

fn envelope_digest(operation: &str, bytes: &[u8]) -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(b"insight.platform/v1/mcp-host-rpc\0");
    hasher.update(operation.as_bytes());
    hasher.update(b"\0");
    hasher.update(bytes);
    let encoded = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("sha256:{encoded}")
        .parse()
        .expect("SHA-256 output is a valid digest")
}

fn valid_operation(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpHostRpcError {
    InvalidConfiguration,
    InvalidEnvelope,
}

impl fmt::Display for McpHostRpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "MCP Host RPC configuration is invalid",
            Self::InvalidEnvelope => "MCP Host RPC envelope is invalid",
        })
    }
}

impl Error for McpHostRpcError {}

impl From<McpHostRpcError> for Status {
    fn from(_: McpHostRpcError) -> Self {
        Status::invalid_argument("invalid MCP Host RPC envelope")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{TraceFlags, TraceIdentityV1};
    use insight_platform_rpc_trace::{request_with_trace, RpcTraceContext};
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose, SanType,
    };
    use tonic::transport::{
        server::TcpIncoming, Certificate, ClientTlsConfig, Endpoint, Identity, Server,
        ServerTlsConfig,
    };

    struct UnreachableHost;

    #[async_trait]
    impl McpHostClient for UnreachableHost {
        async fn execute(
            &self,
            _contract: &McpHostExecutionContract,
            _request: &McpOperationRequest,
        ) -> Result<McpOperationOutcome, McpHostError> {
            unreachable!("malformed fixture must be rejected before host execution")
        }
    }

    #[test]
    fn envelope_is_canonical_bounded_and_digest_bound() {
        let limits = McpHostInternalRpcLimits::new(4_096).unwrap();
        let envelope = encode_envelope(
            EXECUTE_OPERATION,
            &serde_json::json!({"schema_version": 1, "z": 2, "a": 1}),
            limits,
        )
        .unwrap();
        assert_eq!(
            envelope.metadata_jcs,
            br#"{"a":1,"schema_version":1,"z":2}"#
        );
        let decoded: serde_json::Value =
            decode_envelope(envelope.clone(), EXECUTE_OPERATION, limits).unwrap();
        assert_eq!(decoded["a"], 1);

        let mut tampered = envelope.clone();
        tampered.metadata_jcs[0] = b'[';
        assert!(decode_envelope::<serde_json::Value>(tampered, EXECUTE_OPERATION, limits).is_err());
        assert!(decode_envelope::<serde_json::Value>(envelope, CANCEL_OPERATION, limits).is_err());
        assert!(McpHostInternalRpcLimits::new(128).is_err());
    }

    #[test]
    fn workload_identity_accepts_only_one_exact_authorized_uri() {
        let mut ca_parameters = CertificateParams::default();
        ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_parameters.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
        ];
        let ca = CertifiedIssuer::self_signed(ca_parameters, KeyPair::generate().unwrap()).unwrap();
        let issue = |uris: &[&str]| {
            let mut parameters = CertificateParams::default();
            parameters.subject_alt_names = uris
                .iter()
                .map(|uri| SanType::URI((*uri).try_into().unwrap()))
                .collect();
            parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
            let key = KeyPair::generate().unwrap();
            parameters.signed_by(&key, &ca).unwrap().der().to_vec()
        };
        assert!(require_exact_workload_uri(
            &issue(&[CAPABILITY_WORKER_WORKLOAD_IDENTITY]),
            CAPABILITY_WORKER_WORKLOAD_IDENTITY,
        )
        .is_ok());
        assert!(require_exact_workload_uri(
            &issue(&["spiffe://insight.platform/workload/model-worker"]),
            CAPABILITY_WORKER_WORKLOAD_IDENTITY,
        )
        .is_err());
        assert!(require_exact_workload_uri(
            &issue(&[
                CAPABILITY_WORKER_WORKLOAD_IDENTITY,
                "spiffe://insight.platform/workload/model-worker",
            ]),
            CAPABILITY_WORKER_WORKLOAD_IDENTITY,
        )
        .is_err());
        assert!(require_exact_workload_uri(
            &issue(&[CONTEXT_WORKER_WORKLOAD_IDENTITY]),
            CONTEXT_WORKER_WORKLOAD_IDENTITY,
        )
        .is_ok());
        assert!(require_exact_workload_uri(
            &issue(&[CAPABILITY_WORKER_WORKLOAD_IDENTITY]),
            CONTEXT_WORKER_WORKLOAD_IDENTITY,
        )
        .is_err());
    }

    #[test]
    fn refresh_failure_codes_are_closed_and_round_trip() {
        for failure in [
            ContextSubscriptionExecutionError::InvalidAttempt,
            ContextSubscriptionExecutionError::InvalidEvidence,
            ContextSubscriptionExecutionError::InvalidResponse,
            ContextSubscriptionExecutionError::Rejected,
            ContextSubscriptionExecutionError::Unavailable,
            ContextSubscriptionExecutionError::CompletionUncertain,
            ContextSubscriptionExecutionError::Canonicalization,
        ] {
            let wire = ContextSubscriptionExecutionFailureCode::from(failure);
            assert_eq!(ContextSubscriptionExecutionError::from(wire), failure);
        }
    }

    #[tokio::test]
    async fn real_mtls_requires_trace_and_rejects_other_workloads_before_decode() {
        let mut ca_parameters = CertificateParams::default();
        ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_parameters.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
        ];
        let ca = CertifiedIssuer::self_signed(ca_parameters, KeyPair::generate().unwrap()).unwrap();
        let issue = |sans: Vec<SanType>, usage| {
            let mut parameters = CertificateParams::default();
            parameters.subject_alt_names = sans;
            parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            parameters.extended_key_usages = vec![usage];
            let key = KeyPair::generate().unwrap();
            let certificate = parameters.signed_by(&key, &ca).unwrap();
            (certificate.pem(), key.serialize_pem())
        };
        let (server_certificate, server_key) = issue(
            vec![SanType::DnsName("mcp-host.test".try_into().unwrap())],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let (capability_certificate, capability_key) = issue(
            vec![SanType::URI(
                CAPABILITY_WORKER_WORKLOAD_IDENTITY.try_into().unwrap(),
            )],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        let (model_certificate, model_key) = issue(
            vec![SanType::URI(
                "spiffe://insight.platform/workload/model-worker"
                    .try_into()
                    .unwrap(),
            )],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        let limits = McpHostInternalRpcLimits::new(65_536).unwrap();
        let service = proto::mcp_host_execution_service_server::McpHostExecutionServiceServer::new(
            McpHostGrpcService::new(Arc::new(UnreachableHost), limits),
        );
        let service = tonic::service::interceptor::InterceptedService::new(
            service,
            CapabilityWorkerWorkloadIdentity,
        );
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let ca_pem = ca.pem();
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .identity(Identity::from_pem(&server_certificate, &server_key))
                        .client_ca_root(Certificate::from_pem(ca_pem.clone())),
                )
                .unwrap()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_receiver.await;
                }),
        );
        let connect = |certificate: String, key: String| {
            let ca_pem = ca_pem.clone();
            async move {
                let channel = Endpoint::from_shared(format!("https://{address}"))
                    .unwrap()
                    .tls_config(
                        ClientTlsConfig::new()
                            .domain_name("mcp-host.test")
                            .ca_certificate(Certificate::from_pem(ca_pem))
                            .identity(Identity::from_pem(certificate, key)),
                    )
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
                proto::mcp_host_execution_service_client::McpHostExecutionServiceClient::new(
                    channel,
                )
            }
        };
        let malformed = ClosedMcpHostEnvelope {
            schema_version: 1,
            operation: EXECUTE_OPERATION.to_owned(),
            metadata_jcs: b"{}".to_vec(),
            metadata_digest: envelope_digest(EXECUTE_OPERATION, b"{}").to_string(),
        };
        let mut capability = connect(capability_certificate, capability_key).await;
        let missing_trace = capability
            .execute(Request::new(malformed.clone()))
            .await
            .unwrap_err();
        assert_eq!(missing_trace.code(), tonic::Code::InvalidArgument);
        let trace =
            RpcTraceContext::start(TraceIdentityV1::generate(), TraceFlags::NotSampled).unwrap();
        let malformed_status = capability
            .execute(request_with_trace(malformed.clone(), trace).unwrap())
            .await
            .unwrap_err();
        assert_eq!(malformed_status.code(), tonic::Code::InvalidArgument);
        let model_status = connect(model_certificate, model_key)
            .await
            .execute(Request::new(malformed))
            .await
            .unwrap_err();
        assert_eq!(model_status.code(), tonic::Code::PermissionDenied);
        let _ = shutdown_sender.send(());
        server.await.unwrap().unwrap();
    }
}
