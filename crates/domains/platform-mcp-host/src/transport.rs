use super::{
    McpOperationContinuation, McpOperationOutcome, McpRemoteTaskCancelOutcome, McpTransportFailure,
    PreparedMcpSubscription, SensitiveMcpNotificationWire,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    CanonicalHttpEndpoint, ClosedJsonValue, ExactDeploymentRef, ExactSecretBindingRef,
    ExactVersionRef, McpClientCapabilities, McpNegotiatedCapabilities, PublishedMcpMethod,
    ResourceId, Sha256Digest,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpStreamableHttpSubscriptionNotification {
    pub tenant_id: ResourceId,
    pub subscription_id: ResourceId,
    pub authorization_generation: u64,
    pub session_generation: u64,
    pub event_generation: u64,
    pub event_key_digest: Sha256Digest,
    pub wire: SensitiveMcpNotificationWire,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpStreamableHttpSubscriptionTermination {
    pub tenant_id: ResourceId,
    pub subscription_id: ResourceId,
    pub authorization_generation: u64,
    pub session_generation: u64,
    pub worker_process_generation_id: ResourceId,
    pub observed_at: DateTime<Utc>,
    pub failure: McpTransportFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpStreamableHttpSubscriptionSinkError {
    Rejected,
    Saturated,
    Unavailable,
}

/// Host-owned boundary from the live credential-bearing Egress stream to durable notification and
/// session-loss authorities. Egress may supply wire bytes and exact generation identities, but it
/// cannot allocate Receipt/Event IDs or mutate PostgreSQL state.
#[async_trait]
pub trait McpStreamableHttpSubscriptionSink: Send + Sync {
    async fn ingest_notification(
        &self,
        notification: McpStreamableHttpSubscriptionNotification,
    ) -> Result<(), McpStreamableHttpSubscriptionSinkError>;

    async fn report_termination(
        &self,
        termination: McpStreamableHttpSubscriptionTermination,
    ) -> Result<(), McpStreamableHttpSubscriptionSinkError>;
}

/// Credential-free request handed to the role-scoped Streamable HTTP connector.
///
/// `secret_binding` is exact non-secret authority metadata resolved inside the connector's secret
/// broker. There is deliberately no field capable of carrying an access token, cookie, redirect
/// target, arbitrary URI or caller-provided header.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpStreamableHttpRequest {
    pub tenant_id: ResourceId,
    pub deployment: ExactDeploymentRef,
    pub endpoint: CanonicalHttpEndpoint,
    pub endpoint_identity_digest: Sha256Digest,
    pub server_identity_digest: Sha256Digest,
    pub protocol_policy: ExactVersionRef,
    pub network_policy: ExactVersionRef,
    pub tls_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub auth_policy: Option<ExactVersionRef>,
    pub authorization_binding_id: ResourceId,
    pub authorization_generation: u64,
    pub principal_binding_generation: u64,
    pub token_secret_binding: ExactSecretBindingRef,
    pub protocol_version: String,
    pub client_capabilities: McpClientCapabilities,
    pub negotiated_capabilities: McpNegotiatedCapabilities,
    pub operation_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub physical_attempt: u32,
    pub discovery_snapshot_id: ResourceId,
    pub discovery_snapshot_digest: Sha256Digest,
    pub method: PublishedMcpMethod,
    pub params: ClosedJsonValue,
    pub task_requested: bool,
    pub continuation: Option<McpOperationContinuation>,
    pub task_limits: Option<McpRemoteTaskLimits>,
    pub idempotency_key_digest: Sha256Digest,
    /// Original Invocation deadline bound into the encrypted continuation.
    pub deadline: DateTime<Utc>,
    /// Current bounded transport deadline, which may extend into the cancellation cleanup window.
    pub transport_deadline: DateTime<Utc>,
    pub maximum_request_bytes: u32,
    pub maximum_response_bytes: u32,
    pub maximum_headers: u16,
    pub maximum_sse_event_bytes: u32,
    pub maximum_progress_events: u32,
    pub idle_timeout_milliseconds: u64,
    pub initialize_timeout_milliseconds: u64,
    pub request_timeout_milliseconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpRemoteTaskLimits {
    pub maximum_get_request_bytes: u32,
    pub maximum_get_response_bytes: u32,
    pub maximum_result_request_bytes: u32,
    pub maximum_result_response_bytes: u32,
    pub maximum_cancel_request_bytes: Option<u32>,
    pub maximum_cancel_response_bytes: Option<u32>,
    pub maximum_get_progress_events: u32,
    pub maximum_result_progress_events: u32,
    pub maximum_cancel_progress_events: Option<u32>,
    pub minimum_poll_milliseconds: u64,
    pub maximum_poll_milliseconds: u64,
}

#[async_trait]
pub trait McpStreamableHttpConnector: Send + Sync {
    async fn execute(
        &self,
        request: McpStreamableHttpRequest,
    ) -> Result<McpOperationOutcome, McpTransportFailure>;

    async fn cancel_remote_task(
        &self,
        _request: McpStreamableHttpRequest,
    ) -> Result<McpRemoteTaskCancelOutcome, McpTransportFailure> {
        Err(contract_mismatch())
    }
}

/// Exact credential-free request used by the Streamable HTTP egress role to establish and own a
/// durable MCP Resource subscription connection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpStreamableHttpSubscriptionRequest {
    pub tenant_id: ResourceId,
    pub deployment: ExactDeploymentRef,
    pub endpoint: CanonicalHttpEndpoint,
    pub endpoint_identity_digest: Sha256Digest,
    pub server_identity_digest: Sha256Digest,
    pub protocol_policy: ExactVersionRef,
    pub network_policy: ExactVersionRef,
    pub tls_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub auth_policy: Option<ExactVersionRef>,
    pub authorization_binding_id: ResourceId,
    pub authorization_generation: u64,
    pub principal_binding_generation: u64,
    pub token_secret_binding: ExactSecretBindingRef,
    pub protocol_version: String,
    pub client_capabilities: McpClientCapabilities,
    pub negotiated_capabilities: McpNegotiatedCapabilities,
    pub subscription_id: ResourceId,
    pub binding_digest: Sha256Digest,
    pub session_generation: u64,
    pub worker_process_generation_id: ResourceId,
    pub resource_uri: String,
    pub resource_uri_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
    pub maximum_message_bytes: u32,
    pub maximum_response_bytes: u32,
    pub maximum_headers: u16,
    pub maximum_sse_event_bytes: u32,
    pub idle_timeout_milliseconds: u64,
    pub initialize_timeout_milliseconds: u64,
    pub request_timeout_milliseconds: u64,
    pub maximum_session_milliseconds: u64,
}

#[async_trait]
pub trait McpStreamableHttpSubscriptionConnector: Send + Sync {
    async fn establish_subscription(
        &self,
        request: McpStreamableHttpSubscriptionRequest,
    ) -> Result<PreparedMcpSubscription, McpTransportFailure>;
}

pub fn contract_mismatch() -> McpTransportFailure {
    McpTransportFailure::RejectedBeforeDispatch(super::SafeMcpFailure {
        safe_code: "mcp_transport_contract_mismatch".to_owned(),
        safe_message: "MCP transport does not match the exact Deployment".to_owned(),
        evidence_digest: super::static_digest("mcp_transport_contract_mismatch"),
    })
}
