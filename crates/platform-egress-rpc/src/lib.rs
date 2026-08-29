//! Versioned internal gRPC boundary for the independently deployed Egress Broker.
//!
//! The wire separates strict canonical metadata from a raw, digest-bound payload so bounded
//! binary Capability messages do not expand into JSON arrays. PostgreSQL identities, arbitrary
//! URLs, caller headers and plaintext credentials are absent from this adapter; the concrete
//! Egress implementation still resolves exact process-installed catalogs and late Secret values.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use futures::{stream, Stream, StreamExt};
use insight_platform_capability_adapters::{
    CapabilityAdapterFailure, CapabilityAdapterFailureClass, CapabilityTransportCancelOutcome,
    CapabilityTransportCancelRequest, GrpcNetworkTransport, GrpcTransportRequest,
    GrpcTransportResponse, HttpNetworkTransport, HttpTransportRequest, HttpTransportResponse,
};
use insight_platform_context::{
    RemoteContextFailure, RemoteContextSearchConnector, RemoteContextSearchRequest,
    RemoteContextSearchResponse,
};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ResourceKind, SecretResolutionPolicy,
    Sha256Digest,
};
use insight_platform_mcp_host::{
    AuthorizedMcpOAuthPkceCleanup, McpDiscoveryTransportConnector, McpDiscoveryTransportRequest,
    McpDiscoveryTransportResponse, McpOAuthAuthorizedGrant, McpOAuthCredentialBroker,
    McpOAuthCredentialBrokerError, McpOAuthExchangeContract, McpOAuthPkceSecretCleaner,
    McpOAuthPkceSecretCleanupDisposition, McpOAuthPkceSecretCleanupError, McpOperationOutcome,
    McpRemoteTaskCancelOutcome, McpResourceRefreshConnector, McpResourceRefreshTransportEvidence,
    McpResourceRefreshTransportRequest, McpStreamableHttpConnector, McpStreamableHttpRequest,
    McpStreamableHttpSubscriptionConnector, McpStreamableHttpSubscriptionNotification,
    McpStreamableHttpSubscriptionRequest, McpStreamableHttpSubscriptionSink,
    McpStreamableHttpSubscriptionSinkError, McpStreamableHttpSubscriptionTermination,
    McpSubscriptionActivation, McpTransportFailure, PreparedMcpSubscription, SafeMcpFailure,
    SensitiveMcpNotificationWire, SensitiveOAuthValue, MAX_MCP_OAUTH_CODE_BYTES,
    MCP_OAUTH_PKCE_SECRET_PURPOSE,
};
use insight_platform_model_adapters::{
    ModelAdapterCancelOutcome, ModelAdapterCancelRequest, ModelAdapterFailure,
    ModelAdapterFailureClass, ModelProviderWireConnector, ModelProviderWireEvent,
    ModelProviderWireProtocol, ModelProviderWireRequest, ModelProviderWireStream,
};
use insight_platform_rpc_trace::{
    require_trace_interceptor, scope_trace, trace_context, PropagateTrace,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    pin::Pin,
    sync::Arc,
};
use tokio::sync::{mpsc, Mutex, OwnedSemaphorePermit, Semaphore};
use tonic::{Request, Response, Status};
use x509_parser::{extensions::GeneralName, parse_x509_certificate};

pub mod proto {
    tonic::include_proto!("insight.platform.v1");
}

use proto::{
    egress_broker_service_client::EgressBrokerServiceClient,
    egress_broker_service_server::EgressBrokerService, ClosedEgressEnvelope,
};

pub const EGRESS_INTERNAL_RPC_SCHEMA_VERSION: u32 = 1;
pub const MODEL_WORKER_WORKLOAD_IDENTITY: &str = "spiffe://insight.platform/workload/model-worker";
pub const CAPABILITY_WORKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/capability-worker";
pub const MCP_HOST_WORKLOAD_IDENTITY: &str = "spiffe://insight.platform/workload/mcp-host";
pub const MCP_DISCOVERY_WORKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/mcp-discovery-worker";
pub const MCP_SUBSCRIPTION_WORKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/mcp-subscription-worker";
pub const MCP_CLEANUP_WORKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/mcp-cleanup-worker";
pub const MCP_CALLBACK_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/mcp-callback-api";
pub const CONTEXT_WORKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/context-worker";
pub const MAX_EGRESS_METADATA_BYTES_HARD: usize = 1_048_576;
pub const MAX_EGRESS_PAYLOAD_BYTES_HARD: usize = 64 * 1_048_576;
pub const MAX_EGRESS_RPC_MESSAGE_BYTES_HARD: usize =
    MAX_EGRESS_METADATA_BYTES_HARD + MAX_EGRESS_PAYLOAD_BYTES_HARD + 8_192;

const OPEN_MODEL_PROVIDER: &str = "model_provider.open/v1";
const MODEL_PROVIDER_FRAME: &str = "model_provider.frame/v1";
const CANCEL_MODEL_PROVIDER: &str = "model_provider.cancel/v1";
const MODEL_PROVIDER_CANCEL_OUTCOME: &str = "model_provider.cancel_outcome/v1";
const ROUND_TRIP_CAPABILITY_HTTP: &str = "capability_http.round_trip/v1";
const CAPABILITY_HTTP_OUTCOME: &str = "capability_http.outcome/v1";
const CANCEL_CAPABILITY_HTTP: &str = "capability_http.cancel/v1";
const CAPABILITY_HTTP_CANCEL_OUTCOME: &str = "capability_http.cancel_outcome/v1";
const UNARY_CAPABILITY_GRPC: &str = "capability_grpc.unary/v1";
const CAPABILITY_GRPC_OUTCOME: &str = "capability_grpc.outcome/v1";
const CANCEL_CAPABILITY_GRPC: &str = "capability_grpc.cancel/v1";
const CAPABILITY_GRPC_CANCEL_OUTCOME: &str = "capability_grpc.cancel_outcome/v1";
const QUERY_REMOTE_CONTEXT: &str = "remote_context.query/v1";
const REMOTE_CONTEXT_OUTCOME: &str = "remote_context.outcome/v1";
const EXCHANGE_MCP_OAUTH_AUTHORIZATION_CODE: &str = "mcp_oauth.exchange_authorization_code/v1";
const MCP_OAUTH_AUTHORIZATION_CODE_OUTCOME: &str = "mcp_oauth.authorization_code_outcome/v1";
const DELETE_MCP_OAUTH_PKCE_SECRET: &str = "mcp_oauth.delete_pkce_secret/v1";
const MCP_OAUTH_PKCE_SECRET_DELETE_OUTCOME: &str = "mcp_oauth.pkce_secret_delete_outcome/v1";
const EXECUTE_MCP_STREAMABLE_HTTP: &str = "mcp_streamable_http.execute/v1";
const MCP_STREAMABLE_HTTP_OUTCOME: &str = "mcp_streamable_http.outcome/v1";
const DISCOVER_MCP_STREAMABLE_HTTP: &str = "mcp_streamable_http.discover/v1";
const MCP_STREAMABLE_HTTP_DISCOVERY_OUTCOME: &str = "mcp_streamable_http.discovery_outcome/v1";
const REFRESH_MCP_RESOURCES: &str = "mcp_resource_refresh.execute/v1";
const MCP_RESOURCE_REFRESH_OUTCOME: &str = "mcp_resource_refresh.outcome/v1";
const CANCEL_MCP_REMOTE_TASK: &str = "mcp_streamable_http.cancel_remote_task/v1";
const MCP_REMOTE_TASK_CANCEL_OUTCOME: &str = "mcp_streamable_http.cancel_outcome/v1";
const ESTABLISH_MCP_STREAMABLE_HTTP_SUBSCRIPTION: &str =
    "mcp_streamable_http.establish_subscription/v1";
const MCP_STREAMABLE_HTTP_SUBSCRIPTION_PREPARED: &str =
    "mcp_streamable_http.subscription_prepared/v1";
const ACTIVATE_MCP_STREAMABLE_HTTP_SUBSCRIPTION: &str =
    "mcp_streamable_http.activate_subscription/v1";
const MCP_STREAMABLE_HTTP_SUBSCRIPTION_NOTIFICATION: &str =
    "mcp_streamable_http.subscription_notification/v1";
const MCP_STREAMABLE_HTTP_SUBSCRIPTION_TERMINATION: &str =
    "mcp_streamable_http.subscription_termination/v1";

pub const MAX_EGRESS_PENDING_MCP_SUBSCRIPTIONS_HARD: usize = 4_096;
pub const MAX_EGRESS_ACTIVE_MCP_SUBSCRIPTIONS_HARD: usize = 65_536;
pub const MAX_EGRESS_MCP_SUBSCRIPTION_EVENT_BUFFER_HARD: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EgressInternalRpcLimits {
    maximum_metadata_bytes: usize,
    maximum_payload_bytes: usize,
}

impl EgressInternalRpcLimits {
    pub fn new(
        maximum_metadata_bytes: usize,
        maximum_payload_bytes: usize,
    ) -> Result<Self, EgressRpcError> {
        if !(1..=MAX_EGRESS_METADATA_BYTES_HARD).contains(&maximum_metadata_bytes)
            || !(1..=MAX_EGRESS_PAYLOAD_BYTES_HARD).contains(&maximum_payload_bytes)
        {
            return Err(EgressRpcError::InvalidConfiguration);
        }
        Ok(Self {
            maximum_metadata_bytes,
            maximum_payload_bytes,
        })
    }

    pub const fn maximum_message_bytes(self) -> usize {
        self.maximum_metadata_bytes + self.maximum_payload_bytes + 8_192
    }
}

impl Default for EgressInternalRpcLimits {
    fn default() -> Self {
        Self {
            maximum_metadata_bytes: MAX_EGRESS_METADATA_BYTES_HARD,
            maximum_payload_bytes: MAX_EGRESS_PAYLOAD_BYTES_HARD,
        }
    }
}

/// Bounded in-memory bridge for the two-step subscription handoff. It is transport state only:
/// PostgreSQL remains the subscription/session authority and can rebuild any lost bridge state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EgressMcpSubscriptionBridgeLimits {
    pub maximum_pending: usize,
    pub maximum_active: usize,
    pub event_buffer_capacity: usize,
}

impl EgressMcpSubscriptionBridgeLimits {
    pub fn validate(self) -> Result<(), EgressRpcError> {
        if !(1..=MAX_EGRESS_PENDING_MCP_SUBSCRIPTIONS_HARD).contains(&self.maximum_pending)
            || !(1..=MAX_EGRESS_ACTIVE_MCP_SUBSCRIPTIONS_HARD).contains(&self.maximum_active)
            || !(1..=MAX_EGRESS_MCP_SUBSCRIPTION_EVENT_BUFFER_HARD)
                .contains(&self.event_buffer_capacity)
        {
            return Err(EgressRpcError::InvalidConfiguration);
        }
        Ok(())
    }
}

impl Default for EgressMcpSubscriptionBridgeLimits {
    fn default() -> Self {
        Self {
            maximum_pending: 256,
            maximum_active: 4_096,
            event_buffer_capacity: 64,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct McpSubscriptionRouteKey {
    tenant_id: String,
    subscription_id: String,
    authorization_generation: u64,
    session_generation: u64,
}

impl McpSubscriptionRouteKey {
    fn from_request(request: &McpStreamableHttpSubscriptionRequest) -> Self {
        Self {
            tenant_id: request.tenant_id.to_string(),
            subscription_id: request.subscription_id.to_string(),
            authorization_generation: request.authorization_generation,
            session_generation: request.session_generation,
        }
    }

    fn from_notification(notification: &McpStreamableHttpSubscriptionNotification) -> Self {
        Self {
            tenant_id: notification.tenant_id.to_string(),
            subscription_id: notification.subscription_id.to_string(),
            authorization_generation: notification.authorization_generation,
            session_generation: notification.session_generation,
        }
    }

    fn from_termination(termination: &McpStreamableHttpSubscriptionTermination) -> Self {
        Self {
            tenant_id: termination.tenant_id.to_string(),
            subscription_id: termination.subscription_id.to_string(),
            authorization_generation: termination.authorization_generation,
            session_generation: termination.session_generation,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedMcpSubscriptionWire {
    schema_version: u32,
    request_digest: Sha256Digest,
    established: insight_platform_mcp_host::EstablishedMcpSubscription,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivateMcpSubscriptionWire {
    schema_version: u32,
    request_digest: Sha256Digest,
    tenant_id: insight_platform_contracts::ResourceId,
    subscription_id: insight_platform_contracts::ResourceId,
    authorization_generation: u64,
    session_generation: u64,
}

impl ActivateMcpSubscriptionWire {
    fn route_key(&self) -> McpSubscriptionRouteKey {
        McpSubscriptionRouteKey {
            tenant_id: self.tenant_id.to_string(),
            subscription_id: self.subscription_id.to_string(),
            authorization_generation: self.authorization_generation,
            session_generation: self.session_generation,
        }
    }

    fn validate(&self) -> Result<(), EgressRpcError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.subscription_id.kind() != ResourceKind::McpOperation
            || self.authorization_generation == 0
            || self.session_generation == 0
        {
            return Err(EgressRpcError::InvalidEnvelope);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpSubscriptionNotificationMetadata {
    schema_version: u32,
    tenant_id: insight_platform_contracts::ResourceId,
    subscription_id: insight_platform_contracts::ResourceId,
    authorization_generation: u64,
    session_generation: u64,
    event_generation: u64,
    event_key_digest: Sha256Digest,
    received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpSubscriptionTerminationMetadata {
    schema_version: u32,
    tenant_id: insight_platform_contracts::ResourceId,
    subscription_id: insight_platform_contracts::ResourceId,
    authorization_generation: u64,
    session_generation: u64,
    worker_process_generation_id: insight_platform_contracts::ResourceId,
    observed_at: DateTime<Utc>,
    failure: McpTransportFailure,
}

struct PendingMcpSubscription {
    route: McpSubscriptionRouteKey,
    request_digest: Sha256Digest,
    expires_at: DateTime<Utc>,
    activation: Box<dyn McpSubscriptionActivation>,
    _pending_slot: OwnedSemaphorePermit,
    active_slot: OwnedSemaphorePermit,
}

struct ActiveMcpSubscriptionRoute {
    sender: mpsc::Sender<ClosedEgressEnvelope>,
    _active_slot: OwnedSemaphorePermit,
}

/// Process-local event bridge used by the Egress connector and internal gRPC service. A caller
/// first obtains encrypted session evidence, durably commits Ready, then opens the activation
/// stream. No notification can be emitted before that stream route exists.
pub struct EgressMcpSubscriptionBridge {
    limits: EgressInternalRpcLimits,
    bridge_limits: EgressMcpSubscriptionBridgeLimits,
    pending_slots: Arc<Semaphore>,
    active_slots: Arc<Semaphore>,
    reserved_routes: Mutex<BTreeSet<McpSubscriptionRouteKey>>,
    active: Mutex<BTreeMap<McpSubscriptionRouteKey, ActiveMcpSubscriptionRoute>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EgressMcpSubscriptionBridgeCapacitySnapshot {
    pub maximum_pending: usize,
    pub pending_available: usize,
    pub maximum_active: usize,
    pub active_available: usize,
}

impl EgressMcpSubscriptionBridge {
    pub fn new(
        limits: EgressInternalRpcLimits,
        bridge_limits: EgressMcpSubscriptionBridgeLimits,
    ) -> Result<Self, EgressRpcError> {
        bridge_limits.validate()?;
        Ok(Self {
            limits,
            bridge_limits,
            pending_slots: Arc::new(Semaphore::new(bridge_limits.maximum_pending)),
            active_slots: Arc::new(Semaphore::new(bridge_limits.maximum_active)),
            reserved_routes: Mutex::new(BTreeSet::new()),
            active: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn sink(self: &Arc<Self>) -> Arc<dyn McpStreamableHttpSubscriptionSink> {
        self.clone()
    }

    pub fn capacity_snapshot(&self) -> EgressMcpSubscriptionBridgeCapacitySnapshot {
        EgressMcpSubscriptionBridgeCapacitySnapshot {
            maximum_pending: self.bridge_limits.maximum_pending,
            pending_available: self.pending_slots.available_permits(),
            maximum_active: self.bridge_limits.maximum_active,
            active_available: self.active_slots.available_permits(),
        }
    }

    async fn establish(
        &self,
        connector: &dyn McpStreamableHttpSubscriptionConnector,
        request: McpStreamableHttpSubscriptionRequest,
    ) -> Result<(PreparedMcpSubscriptionWire, PendingMcpSubscription), McpTransportFailure> {
        let request_digest = typed_digest(&request)
            .map_err(|_| mcp_rpc_rejected("mcp_egress_subscription_request_invalid"))?;
        let route = McpSubscriptionRouteKey::from_request(&request);
        let pending_slot = Arc::clone(&self.pending_slots)
            .try_acquire_owned()
            .map_err(|_| mcp_rpc_retryable("mcp_egress_subscription_pending_capacity"))?;
        let active_slot = Arc::clone(&self.active_slots)
            .try_acquire_owned()
            .map_err(|_| mcp_rpc_retryable("mcp_egress_subscription_active_capacity"))?;
        {
            let mut routes = self.reserved_routes.lock().await;
            if !routes.insert(route.clone()) {
                return Err(mcp_rpc_uncertain(
                    "mcp_egress_subscription_generation_already_reserved",
                    request_digest,
                ));
            }
        }

        let prepared = match connector.establish_subscription(request).await {
            Ok(prepared) => prepared,
            Err(failure) => {
                self.reserved_routes.lock().await.remove(&route);
                return Err(failure);
            }
        };
        let (established, activation) = prepared.into_parts();
        let expires_at = established.expires_at;
        Ok((
            PreparedMcpSubscriptionWire {
                schema_version: 1,
                request_digest: request_digest.clone(),
                established,
            },
            PendingMcpSubscription {
                route,
                request_digest,
                expires_at,
                activation,
                _pending_slot: pending_slot,
                active_slot,
            },
        ))
    }

    async fn activate(
        &self,
        pending: PendingMcpSubscription,
        request: ActivateMcpSubscriptionWire,
        sender: mpsc::Sender<ClosedEgressEnvelope>,
    ) -> Result<(), Status> {
        request
            .validate()
            .map_err(|_| Status::invalid_argument("invalid MCP subscription activation"))?;
        if pending.request_digest != request.request_digest
            || pending.route != request.route_key()
            || pending.expires_at <= Utc::now()
        {
            self.reserved_routes.lock().await.remove(&pending.route);
            return Err(Status::failed_precondition(
                "MCP subscription activation does not match prepared state",
            ));
        }
        let route = pending.route.clone();
        let mut active = self.active.lock().await;
        if active.contains_key(&route) {
            return Err(Status::failed_precondition(
                "MCP subscription route is already active",
            ));
        }
        active.insert(
            route,
            ActiveMcpSubscriptionRoute {
                sender,
                _active_slot: pending.active_slot,
            },
        );
        drop(active);
        pending.activation.activate().await;
        Ok(())
    }

    async fn remove_active(&self, route: &McpSubscriptionRouteKey) {
        self.active.lock().await.remove(route);
        self.reserved_routes.lock().await.remove(route);
    }

    async fn discard_pending(&self, pending: PendingMcpSubscription) {
        self.reserved_routes.lock().await.remove(&pending.route);
    }
}

#[async_trait]
impl McpStreamableHttpSubscriptionSink for EgressMcpSubscriptionBridge {
    async fn ingest_notification(
        &self,
        notification: McpStreamableHttpSubscriptionNotification,
    ) -> Result<(), McpStreamableHttpSubscriptionSinkError> {
        let route = McpSubscriptionRouteKey::from_notification(&notification);
        let sender = self
            .active
            .lock()
            .await
            .get(&route)
            .map(|active| active.sender.clone())
            .ok_or(McpStreamableHttpSubscriptionSinkError::Rejected)?;
        let metadata = McpSubscriptionNotificationMetadata {
            schema_version: 1,
            tenant_id: notification.tenant_id,
            subscription_id: notification.subscription_id,
            authorization_generation: notification.authorization_generation,
            session_generation: notification.session_generation,
            event_generation: notification.event_generation,
            event_key_digest: notification.event_key_digest,
            received_at: notification.received_at,
        };
        let envelope = encode_metadata_payload(
            &metadata,
            notification.wire.into_bytes(),
            MCP_STREAMABLE_HTTP_SUBSCRIPTION_NOTIFICATION,
            self.limits,
        )
        .map_err(|_| McpStreamableHttpSubscriptionSinkError::Rejected)?;
        if sender.try_send(envelope).is_err() {
            self.remove_active(&route).await;
            return Err(McpStreamableHttpSubscriptionSinkError::Saturated);
        }
        Ok(())
    }

    async fn report_termination(
        &self,
        termination: McpStreamableHttpSubscriptionTermination,
    ) -> Result<(), McpStreamableHttpSubscriptionSinkError> {
        termination
            .failure
            .validate_wire_shape()
            .map_err(|_| McpStreamableHttpSubscriptionSinkError::Rejected)?;
        let route = McpSubscriptionRouteKey::from_termination(&termination);
        let sender = self
            .active
            .lock()
            .await
            .get(&route)
            .map(|active| active.sender.clone())
            .ok_or(McpStreamableHttpSubscriptionSinkError::Rejected)?;
        let metadata = McpSubscriptionTerminationMetadata {
            schema_version: 1,
            tenant_id: termination.tenant_id,
            subscription_id: termination.subscription_id,
            authorization_generation: termination.authorization_generation,
            session_generation: termination.session_generation,
            worker_process_generation_id: termination.worker_process_generation_id,
            observed_at: termination.observed_at,
            failure: termination.failure,
        };
        let envelope = encode_metadata(
            &metadata,
            MCP_STREAMABLE_HTTP_SUBSCRIPTION_TERMINATION,
            self.limits,
        )
        .map_err(|_| McpStreamableHttpSubscriptionSinkError::Rejected)?;
        let sent = sender.try_send(envelope).is_ok();
        self.remove_active(&route).await;
        if sent {
            Ok(())
        } else {
            Err(McpStreamableHttpSubscriptionSinkError::Saturated)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressCallerRole {
    ModelWorker,
    CapabilityWorker,
    ContextWorker,
    McpDiscoveryWorker,
    McpSubscriptionWorker,
    McpCleanupWorker,
    McpCallback,
    McpHost,
}

impl EgressCallerRole {
    fn from_uri(uri: &str) -> Option<Self> {
        match uri {
            MODEL_WORKER_WORKLOAD_IDENTITY => Some(Self::ModelWorker),
            CAPABILITY_WORKER_WORKLOAD_IDENTITY => Some(Self::CapabilityWorker),
            CONTEXT_WORKER_WORKLOAD_IDENTITY => Some(Self::ContextWorker),
            MCP_DISCOVERY_WORKER_WORKLOAD_IDENTITY => Some(Self::McpDiscoveryWorker),
            MCP_SUBSCRIPTION_WORKER_WORKLOAD_IDENTITY => Some(Self::McpSubscriptionWorker),
            MCP_CLEANUP_WORKER_WORKLOAD_IDENTITY => Some(Self::McpCleanupWorker),
            MCP_CALLBACK_WORKLOAD_IDENTITY => Some(Self::McpCallback),
            MCP_HOST_WORKLOAD_IDENTITY => Some(Self::McpHost),
            _ => None,
        }
    }
}

/// Authenticates one exact closed workload URI SAN before any request body is decoded.
#[derive(Debug, Clone, Copy, Default)]
pub struct EgressCallerWorkloadIdentity;

impl tonic::service::Interceptor for EgressCallerWorkloadIdentity {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        let leaf = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        let role = closed_workload_role(leaf.as_ref())?;
        request.extensions_mut().insert(role);
        require_trace_interceptor(request)
    }
}

fn closed_workload_role(certificate: &[u8]) -> Result<EgressCallerRole, Status> {
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
    let role = uris
        .next()
        .and_then(EgressCallerRole::from_uri)
        .ok_or_else(|| Status::permission_denied("workload identity is not authorized"))?;
    if uris.next().is_some() {
        return Err(Status::permission_denied(
            "workload identity is not authorized",
        ));
    }
    Ok(role)
}

fn require_role<T>(request: &Request<T>, expected: EgressCallerRole) -> Result<(), Status> {
    if request.extensions().get::<EgressCallerRole>() == Some(&expected) {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "workload role cannot invoke this Egress operation",
        ))
    }
}

fn remote_context_rpc_failure(code: &str, retryable: bool) -> RemoteContextFailure {
    RemoteContextFailure {
        code: code.to_owned(),
        class: if retryable {
            insight_platform_context::RemoteContextFailureClass::RetryableBeforeDispatch
        } else {
            insight_platform_context::RemoteContextFailureClass::RejectedBeforeDispatch
        },
        safe_message: "Remote Context Egress RPC failed before dispatch".to_owned(),
        dispatch_evidence_digest: None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum UnaryOutcome<T, F> {
    Succeeded(T),
    Failed(F),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum McpOAuthCredentialFailureWire {
    Rejected,
    TemporarilyUnavailable,
    ExchangeUncertain,
}

impl From<McpOAuthCredentialBrokerError> for McpOAuthCredentialFailureWire {
    fn from(value: McpOAuthCredentialBrokerError) -> Self {
        match value {
            McpOAuthCredentialBrokerError::Rejected => Self::Rejected,
            McpOAuthCredentialBrokerError::TemporarilyUnavailable => Self::TemporarilyUnavailable,
            McpOAuthCredentialBrokerError::ExchangeUncertain => Self::ExchangeUncertain,
        }
    }
}

impl From<McpOAuthCredentialFailureWire> for McpOAuthCredentialBrokerError {
    fn from(value: McpOAuthCredentialFailureWire) -> Self {
        match value {
            McpOAuthCredentialFailureWire::Rejected => Self::Rejected,
            McpOAuthCredentialFailureWire::TemporarilyUnavailable => Self::TemporarilyUnavailable,
            McpOAuthCredentialFailureWire::ExchangeUncertain => Self::ExchangeUncertain,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum McpOAuthPkceCleanupFailureWire {
    Rejected,
    TemporarilyUnavailable,
    OutcomeUncertain,
}

impl From<McpOAuthPkceSecretCleanupError> for McpOAuthPkceCleanupFailureWire {
    fn from(value: McpOAuthPkceSecretCleanupError) -> Self {
        match value {
            McpOAuthPkceSecretCleanupError::Rejected => Self::Rejected,
            McpOAuthPkceSecretCleanupError::TemporarilyUnavailable => Self::TemporarilyUnavailable,
            McpOAuthPkceSecretCleanupError::OutcomeUncertain => Self::OutcomeUncertain,
        }
    }
}

impl From<McpOAuthPkceCleanupFailureWire> for McpOAuthPkceSecretCleanupError {
    fn from(value: McpOAuthPkceCleanupFailureWire) -> Self {
        match value {
            McpOAuthPkceCleanupFailureWire::Rejected => Self::Rejected,
            McpOAuthPkceCleanupFailureWire::TemporarilyUnavailable => Self::TemporarilyUnavailable,
            McpOAuthPkceCleanupFailureWire::OutcomeUncertain => Self::OutcomeUncertain,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum ModelStreamFrame {
    Event(ModelProviderWireEvent),
    Failed(ModelAdapterFailure),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressRpcDependencyOutcome {
    Success,
    Failure,
}

/// Receives only the result of an actual Egress Broker gRPC transport operation. Request metadata,
/// endpoint, provider, tenant/resource identity, payload and error details never cross this port.
pub trait EgressRpcDependencyObserver: Send + Sync {
    fn observe(&self, outcome: EgressRpcDependencyOutcome);
}

#[derive(Debug)]
struct NoopEgressRpcDependencyObserver;

impl EgressRpcDependencyObserver for NoopEgressRpcDependencyObserver {
    fn observe(&self, _outcome: EgressRpcDependencyOutcome) {}
}

#[derive(Clone)]
pub struct EgressBrokerGrpcClient {
    client: TracedEgressBrokerServiceClient,
    limits: EgressInternalRpcLimits,
    mcp_subscription_sink: Option<Arc<dyn McpStreamableHttpSubscriptionSink>>,
    dependency_observer: Arc<dyn EgressRpcDependencyObserver>,
}

impl EgressBrokerGrpcClient {
    pub fn new(channel: tonic::transport::Channel, limits: EgressInternalRpcLimits) -> Self {
        Self::new_with_observer(channel, limits, Arc::new(NoopEgressRpcDependencyObserver))
    }

    pub fn new_with_observer(
        channel: tonic::transport::Channel,
        limits: EgressInternalRpcLimits,
        dependency_observer: Arc<dyn EgressRpcDependencyObserver>,
    ) -> Self {
        let maximum = limits.maximum_message_bytes();
        Self {
            client: EgressBrokerServiceClient::with_interceptor(channel, PropagateTrace)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            limits,
            mcp_subscription_sink: None,
            dependency_observer,
        }
    }

    pub fn with_mcp_subscription_sink(
        mut self,
        sink: Arc<dyn McpStreamableHttpSubscriptionSink>,
    ) -> Self {
        self.mcp_subscription_sink = Some(sink);
        self
    }
}

fn observe_egress_rpc(observer: &Arc<dyn EgressRpcDependencyObserver>, success: bool) {
    observer.observe(if success {
        EgressRpcDependencyOutcome::Success
    } else {
        EgressRpcDependencyOutcome::Failure
    });
}

type TracedEgressBrokerServiceClient = EgressBrokerServiceClient<
    tonic::service::interceptor::InterceptedService<tonic::transport::Channel, PropagateTrace>,
>;

#[async_trait]
impl ModelProviderWireConnector for EgressBrokerGrpcClient {
    async fn open(
        &self,
        request: ModelProviderWireRequest,
    ) -> Result<ModelProviderWireStream, ModelAdapterFailure> {
        request.validate_at(Utc::now())?;
        let deadline = request.deadline;
        let envelope = encode_model_request(request, self.limits)
            .map_err(|_| model_rpc_failure("model_egress_rpc_request_invalid", false, deadline))?;
        let mut client = self.client.clone();
        let response = client.open_model_provider(Request::new(envelope)).await;
        observe_egress_rpc(&self.dependency_observer, response.is_ok());
        let incoming = response
            .map_err(|status| model_status_failure(status, deadline))?
            .into_inner();
        let limits = self.limits;
        let dependency_observer = Arc::clone(&self.dependency_observer);
        let stream = stream::unfold(
            (incoming, false, dependency_observer),
            move |(mut incoming, terminal, dependency_observer)| async move {
                if terminal {
                    return None;
                }
                let response = incoming.message().await;
                observe_egress_rpc(&dependency_observer, response.is_ok());
                match response {
                    Ok(Some(envelope)) => match decode_metadata::<ModelStreamFrame>(
                        envelope,
                        MODEL_PROVIDER_FRAME,
                        limits,
                    ) {
                        Ok(ModelStreamFrame::Event(event)) => {
                            Some((Ok(event), (incoming, false, dependency_observer)))
                        }
                        Ok(ModelStreamFrame::Failed(failure))
                            if failure.validate_wire_shape(deadline, Utc::now()).is_ok() =>
                        {
                            Some((Err(failure), (incoming, true, dependency_observer)))
                        }
                        Ok(ModelStreamFrame::Failed(_)) => Some((
                            Err(model_rpc_failure(
                                "model_egress_rpc_response_invalid",
                                true,
                                deadline,
                            )),
                            (incoming, true, dependency_observer),
                        )),
                        Err(_) => Some((
                            Err(model_rpc_failure(
                                "model_egress_rpc_response_invalid",
                                true,
                                deadline,
                            )),
                            (incoming, true, dependency_observer),
                        )),
                    },
                    Ok(None) => None,
                    Err(status) => Some((
                        Err(model_status_failure(status, deadline)),
                        (incoming, true, dependency_observer),
                    )),
                }
            },
        );
        Ok(Box::pin(stream))
    }

    async fn cancel(
        &self,
        protocol: ModelProviderWireProtocol,
        request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        request.validate_shape_at(Utc::now()).map_err(|_| {
            model_rpc_failure("model_egress_rpc_cancel_invalid", false, request.deadline)
        })?;
        let deadline = request.deadline;
        let envelope = encode_metadata(&(protocol, request), CANCEL_MODEL_PROVIDER, self.limits)
            .map_err(|_| model_rpc_failure("model_egress_rpc_cancel_invalid", false, deadline))?;
        let mut client = self.client.clone();
        let response = client.cancel_model_provider(Request::new(envelope)).await;
        observe_egress_rpc(&self.dependency_observer, response.is_ok());
        let response = response.map_err(|status| model_status_failure(status, deadline))?;
        match decode_metadata::<UnaryOutcome<ModelAdapterCancelOutcome, ModelAdapterFailure>>(
            response.into_inner(),
            MODEL_PROVIDER_CANCEL_OUTCOME,
            self.limits,
        )
        .map_err(|_| model_rpc_failure("model_egress_rpc_response_invalid", true, deadline))?
        {
            UnaryOutcome::Succeeded(outcome) => Ok(outcome),
            UnaryOutcome::Failed(failure)
                if failure.validate_wire_shape(deadline, Utc::now()).is_ok() =>
            {
                Err(failure)
            }
            UnaryOutcome::Failed(_) => Err(model_rpc_failure(
                "model_egress_rpc_response_invalid",
                true,
                deadline,
            )),
        }
    }
}

#[async_trait]
impl HttpNetworkTransport for EgressBrokerGrpcClient {
    async fn round_trip(
        &self,
        request: HttpTransportRequest,
    ) -> Result<HttpTransportResponse, CapabilityAdapterFailure> {
        request
            .validate_at(Utc::now())
            .map_err(|_| capability_rpc_failure("capability_egress_rpc_request_invalid", false))?;
        let response_limits = request.limits;
        let envelope = encode_http_request(request, self.limits)
            .map_err(|_| capability_rpc_failure("capability_egress_rpc_request_invalid", false))?;
        let mut client = self.client.clone();
        let response = client
            .round_trip_capability_http(Request::new(envelope))
            .await;
        observe_egress_rpc(&self.dependency_observer, response.is_ok());
        let response = response.map_err(capability_status_failure)?;
        match decode_http_outcome(response.into_inner(), self.limits)? {
            UnaryOutcome::Succeeded(response) => {
                response.validate(response_limits).map_err(|_| {
                    capability_rpc_failure("capability_egress_rpc_response_invalid", false)
                })?;
                Ok(response)
            }
            UnaryOutcome::Failed(failure) => Err(failure),
        }
    }

    async fn cancel(
        &self,
        request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        capability_cancel(
            self.client.clone(),
            Arc::clone(&self.dependency_observer),
            request,
            CANCEL_CAPABILITY_HTTP,
            CAPABILITY_HTTP_CANCEL_OUTCOME,
            self.limits,
            |client, request| Box::pin(client.cancel_capability_http(request)),
        )
        .await
    }
}

#[async_trait]
impl GrpcNetworkTransport for EgressBrokerGrpcClient {
    async fn unary(
        &self,
        request: GrpcTransportRequest,
    ) -> Result<GrpcTransportResponse, CapabilityAdapterFailure> {
        request
            .validate_at(Utc::now())
            .map_err(|_| capability_rpc_failure("capability_egress_rpc_request_invalid", false))?;
        let response_limits = request.limits;
        let envelope = encode_grpc_request(request, self.limits)
            .map_err(|_| capability_rpc_failure("capability_egress_rpc_request_invalid", false))?;
        let mut client = self.client.clone();
        let response = client.unary_capability_grpc(Request::new(envelope)).await;
        observe_egress_rpc(&self.dependency_observer, response.is_ok());
        let response = response.map_err(capability_status_failure)?;
        match decode_grpc_outcome(response.into_inner(), self.limits)? {
            UnaryOutcome::Succeeded(response) => {
                response.validate(response_limits).map_err(|_| {
                    capability_rpc_failure("capability_egress_rpc_response_invalid", false)
                })?;
                Ok(response)
            }
            UnaryOutcome::Failed(failure) => Err(failure),
        }
    }

    async fn cancel(
        &self,
        request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        capability_cancel(
            self.client.clone(),
            Arc::clone(&self.dependency_observer),
            request,
            CANCEL_CAPABILITY_GRPC,
            CAPABILITY_GRPC_CANCEL_OUTCOME,
            self.limits,
            |client, request| Box::pin(client.cancel_capability_grpc(request)),
        )
        .await
    }
}

#[async_trait]
impl RemoteContextSearchConnector for EgressBrokerGrpcClient {
    async fn query(
        &self,
        request: RemoteContextSearchRequest,
    ) -> Result<RemoteContextSearchResponse, RemoteContextFailure> {
        request
            .validate_at(Utc::now())
            .map_err(|_| remote_context_rpc_failure("context_egress_rpc_request_invalid", false))?;
        let envelope = encode_metadata(&request, QUERY_REMOTE_CONTEXT, self.limits)
            .map_err(|_| remote_context_rpc_failure("context_egress_rpc_request_invalid", false))?;
        let mut client = self.client.clone();
        let response = client.query_remote_context(Request::new(envelope)).await;
        observe_egress_rpc(&self.dependency_observer, response.is_ok());
        let response = response
            .map_err(|_| remote_context_rpc_failure("context_egress_rpc_unavailable", true))?;
        match decode_metadata::<UnaryOutcome<RemoteContextSearchResponse, RemoteContextFailure>>(
            response.into_inner(),
            REMOTE_CONTEXT_OUTCOME,
            self.limits,
        )
        .map_err(|_| remote_context_rpc_failure("context_egress_rpc_response_invalid", true))?
        {
            UnaryOutcome::Succeeded(response)
                if response.validate_for(&request, Utc::now()).is_ok() =>
            {
                Ok(response)
            }
            UnaryOutcome::Succeeded(_) => Err(remote_context_rpc_failure(
                "context_egress_rpc_response_invalid",
                true,
            )),
            UnaryOutcome::Failed(failure) if failure.validate().is_ok() => Err(failure),
            UnaryOutcome::Failed(_) => Err(remote_context_rpc_failure(
                "context_egress_rpc_response_invalid",
                true,
            )),
        }
    }
}

#[async_trait]
impl McpOAuthCredentialBroker for EgressBrokerGrpcClient {
    async fn exchange_authorization_code(
        &self,
        contract: &McpOAuthExchangeContract,
        authorization_code: SensitiveOAuthValue,
        now: DateTime<Utc>,
    ) -> Result<McpOAuthAuthorizedGrant, McpOAuthCredentialBrokerError> {
        contract
            .validate_at(now)
            .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
        let envelope = encode_metadata_payload(
            contract,
            authorization_code.as_bytes().to_vec(),
            EXCHANGE_MCP_OAUTH_AUTHORIZATION_CODE,
            self.limits,
        )
        .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
        let mut client = self.client.clone();
        let response = client
            .exchange_mcp_o_auth_authorization_code(Request::new(envelope))
            .await;
        observe_egress_rpc(&self.dependency_observer, response.is_ok());
        let response = response.map_err(mcp_oauth_status_failure)?;
        let outcome: UnaryOutcome<McpOAuthAuthorizedGrant, McpOAuthCredentialFailureWire> =
            decode_metadata(
                response.into_inner(),
                MCP_OAUTH_AUTHORIZATION_CODE_OUTCOME,
                self.limits,
            )
            .map_err(|_| McpOAuthCredentialBrokerError::ExchangeUncertain)?;
        match outcome {
            UnaryOutcome::Succeeded(grant) => {
                grant
                    .validate_for_binding(&contract.binding, now)
                    .map_err(|_| McpOAuthCredentialBrokerError::ExchangeUncertain)?;
                Ok(grant)
            }
            UnaryOutcome::Failed(failure) => Err(failure.into()),
        }
    }
}

#[async_trait]
impl McpOAuthPkceSecretCleaner for EgressBrokerGrpcClient {
    async fn delete_exact(
        &self,
        authorization: &AuthorizedMcpOAuthPkceCleanup,
    ) -> Result<McpOAuthPkceSecretCleanupDisposition, McpOAuthPkceSecretCleanupError> {
        validate_cleanup_authorization(authorization)
            .map_err(|_| McpOAuthPkceSecretCleanupError::Rejected)?;
        let envelope = encode_metadata(authorization, DELETE_MCP_OAUTH_PKCE_SECRET, self.limits)
            .map_err(|_| McpOAuthPkceSecretCleanupError::Rejected)?;
        let mut client = self.client.clone();
        let response = client
            .delete_mcp_o_auth_pkce_secret(Request::new(envelope))
            .await;
        observe_egress_rpc(&self.dependency_observer, response.is_ok());
        let response = response.map_err(mcp_oauth_cleanup_status_failure)?;
        match decode_metadata::<
            UnaryOutcome<McpOAuthPkceSecretCleanupDisposition, McpOAuthPkceCleanupFailureWire>,
        >(
            response.into_inner(),
            MCP_OAUTH_PKCE_SECRET_DELETE_OUTCOME,
            self.limits,
        )
        .map_err(|_| McpOAuthPkceSecretCleanupError::OutcomeUncertain)?
        {
            UnaryOutcome::Succeeded(disposition) => Ok(disposition),
            UnaryOutcome::Failed(failure) => Err(failure.into()),
        }
    }
}

#[async_trait]
impl McpDiscoveryTransportConnector for EgressBrokerGrpcClient {
    async fn discover(
        &self,
        request: McpDiscoveryTransportRequest,
    ) -> Result<McpDiscoveryTransportResponse, McpTransportFailure> {
        request
            .validate_at(Utc::now())
            .map_err(|_| mcp_rpc_rejected("mcp_discovery_rpc_request_invalid"))?;
        let request_identity = request.request_digest.clone();
        let envelope = encode_metadata(&request, DISCOVER_MCP_STREAMABLE_HTTP, self.limits)
            .map_err(|_| mcp_rpc_rejected("mcp_discovery_rpc_request_invalid"))?;
        let mut client = self.client.clone();
        let response = client
            .discover_mcp_streamable_http(Request::new(envelope))
            .await;
        observe_egress_rpc(&self.dependency_observer, response.is_ok());
        let response =
            response.map_err(|status| mcp_rpc_status_failure(status, request_identity.clone()))?;
        let outcome =
            decode_mcp_discovery_outcome(response.into_inner(), self.limits).map_err(|_| {
                mcp_rpc_uncertain(
                    "mcp_discovery_rpc_response_invalid",
                    request_identity.clone(),
                )
            })?;
        match outcome {
            UnaryOutcome::Succeeded(response) => {
                response.validate_for(&request).map_err(|_| {
                    mcp_rpc_uncertain("mcp_discovery_rpc_response_invalid", request_identity)
                })?;
                Ok(response)
            }
            UnaryOutcome::Failed(failure) => {
                failure.validate_wire_shape().map_err(|_| {
                    mcp_rpc_uncertain("mcp_discovery_rpc_response_invalid", request_identity)
                })?;
                Err(failure)
            }
        }
    }
}

#[async_trait]
impl McpStreamableHttpConnector for EgressBrokerGrpcClient {
    async fn execute(
        &self,
        request: McpStreamableHttpRequest,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        let request_identity = request.idempotency_key_digest.clone();
        let envelope = encode_metadata(&request, EXECUTE_MCP_STREAMABLE_HTTP, self.limits)
            .map_err(|_| mcp_rpc_rejected("mcp_egress_rpc_request_invalid"))?;
        let mut client = self.client.clone();
        let response = client
            .execute_mcp_streamable_http(Request::new(envelope))
            .await;
        observe_egress_rpc(&self.dependency_observer, response.is_ok());
        let response =
            response.map_err(|status| mcp_rpc_status_failure(status, request_identity.clone()))?;
        let outcome: UnaryOutcome<McpOperationOutcome, McpTransportFailure> = decode_metadata(
            response.into_inner(),
            MCP_STREAMABLE_HTTP_OUTCOME,
            self.limits,
        )
        .map_err(|_| {
            mcp_rpc_uncertain("mcp_egress_rpc_response_invalid", request_identity.clone())
        })?;
        match outcome {
            UnaryOutcome::Succeeded(outcome) => {
                outcome
                    .validate_streamable_wire_shape(&request, Utc::now())
                    .map_err(|_| {
                        mcp_rpc_uncertain("mcp_egress_rpc_response_invalid", request_identity)
                    })?;
                Ok(outcome)
            }
            UnaryOutcome::Failed(failure) => {
                failure.validate_wire_shape().map_err(|_| {
                    mcp_rpc_uncertain("mcp_egress_rpc_response_invalid", request_identity)
                })?;
                Err(failure)
            }
        }
    }

    async fn cancel_remote_task(
        &self,
        request: McpStreamableHttpRequest,
    ) -> Result<McpRemoteTaskCancelOutcome, McpTransportFailure> {
        let request_identity = request.idempotency_key_digest.clone();
        let envelope = encode_metadata(&request, CANCEL_MCP_REMOTE_TASK, self.limits)
            .map_err(|_| mcp_rpc_rejected("mcp_egress_rpc_cancel_invalid"))?;
        let mut client = self.client.clone();
        let response = client.cancel_mcp_remote_task(Request::new(envelope)).await;
        observe_egress_rpc(&self.dependency_observer, response.is_ok());
        let response =
            response.map_err(|status| mcp_rpc_status_failure(status, request_identity.clone()))?;
        match decode_metadata::<UnaryOutcome<McpRemoteTaskCancelOutcome, McpTransportFailure>>(
            response.into_inner(),
            MCP_REMOTE_TASK_CANCEL_OUTCOME,
            self.limits,
        )
        .map_err(|_| {
            mcp_rpc_uncertain(
                "mcp_egress_rpc_cancel_response_invalid",
                request_identity.clone(),
            )
        })? {
            UnaryOutcome::Succeeded(McpRemoteTaskCancelOutcome::Accepted) => {
                Ok(McpRemoteTaskCancelOutcome::Accepted)
            }
            UnaryOutcome::Failed(failure) => {
                failure.validate_wire_shape().map_err(|_| {
                    mcp_rpc_uncertain("mcp_egress_rpc_cancel_response_invalid", request_identity)
                })?;
                Err(failure)
            }
        }
    }
}

#[async_trait]
impl McpResourceRefreshConnector for EgressBrokerGrpcClient {
    async fn refresh_resources(
        &self,
        request: McpResourceRefreshTransportRequest,
    ) -> Result<McpResourceRefreshTransportEvidence, McpTransportFailure> {
        request
            .validate_at(Utc::now())
            .map_err(|_| mcp_rpc_rejected("mcp_resource_refresh_rpc_request_invalid"))?;
        let request_identity = request.execution_identity_digest.clone();
        let envelope = encode_metadata(&request, REFRESH_MCP_RESOURCES, self.limits)
            .map_err(|_| mcp_rpc_rejected("mcp_resource_refresh_rpc_request_invalid"))?;
        let mut client = self.client.clone();
        let response = client.refresh_mcp_resources(Request::new(envelope)).await;
        observe_egress_rpc(&self.dependency_observer, response.is_ok());
        let response =
            response.map_err(|status| mcp_rpc_status_failure(status, request_identity.clone()))?;
        match decode_metadata::<
            UnaryOutcome<McpResourceRefreshTransportEvidence, McpTransportFailure>,
        >(
            response.into_inner(),
            MCP_RESOURCE_REFRESH_OUTCOME,
            self.limits,
        )
        .map_err(|_| {
            mcp_rpc_uncertain(
                "mcp_resource_refresh_rpc_response_invalid",
                request_identity.clone(),
            )
        })? {
            UnaryOutcome::Succeeded(evidence)
                if evidence.execution_identity_digest == request.execution_identity_digest
                    && evidence.request_digest == request.request_digest
                    && evidence.observed_at <= request.deadline
                    && evidence.resource_count <= request.maximum_resources
                    && evidence.item_count <= request.maximum_items
                    && evidence.byte_count <= request.maximum_total_bytes =>
            {
                Ok(evidence)
            }
            UnaryOutcome::Succeeded(_) => Err(mcp_rpc_uncertain(
                "mcp_resource_refresh_rpc_response_invalid",
                request_identity,
            )),
            UnaryOutcome::Failed(failure) => {
                failure.validate_wire_shape().map_err(|_| {
                    mcp_rpc_uncertain(
                        "mcp_resource_refresh_rpc_response_invalid",
                        request_identity,
                    )
                })?;
                Err(failure)
            }
        }
    }
}

#[async_trait]
impl McpStreamableHttpSubscriptionConnector for EgressBrokerGrpcClient {
    async fn establish_subscription(
        &self,
        request: McpStreamableHttpSubscriptionRequest,
    ) -> Result<PreparedMcpSubscription, McpTransportFailure> {
        let sink = self
            .mcp_subscription_sink
            .as_ref()
            .cloned()
            .ok_or_else(|| mcp_rpc_rejected("mcp_egress_subscription_sink_not_installed"))?;
        let request_digest = typed_digest(&request)
            .map_err(|_| mcp_rpc_rejected("mcp_egress_subscription_request_invalid"))?;
        let envelope = encode_metadata(
            &request,
            ESTABLISH_MCP_STREAMABLE_HTTP_SUBSCRIPTION,
            self.limits,
        )
        .map_err(|_| mcp_rpc_rejected("mcp_egress_subscription_request_invalid"))?;
        let (request_sender, request_receiver) = mpsc::channel(2);
        request_sender
            .send(envelope)
            .await
            .map_err(|_| mcp_rpc_rejected("mcp_egress_subscription_request_invalid"))?;
        let request_stream = tokio_stream::wrappers::ReceiverStream::new(request_receiver);
        let mut client = self.client.clone();
        let response_stream = client
            .stream_mcp_streamable_http_subscription(Request::new(request_stream))
            .await;
        observe_egress_rpc(&self.dependency_observer, response_stream.is_ok());
        let mut response_stream = response_stream
            .map_err(|_| {
                mcp_rpc_uncertain(
                    "mcp_egress_subscription_establish_rpc_uncertain",
                    request_digest.clone(),
                )
            })?
            .into_inner();
        let response = response_stream.message().await;
        observe_egress_rpc(&self.dependency_observer, response.is_ok());
        let response = response
            .map_err(|_| {
                mcp_rpc_uncertain(
                    "mcp_egress_subscription_establish_response_invalid",
                    request_digest.clone(),
                )
            })?
            .ok_or_else(|| {
                mcp_rpc_uncertain(
                    "mcp_egress_subscription_establish_response_missing",
                    request_digest.clone(),
                )
            })?;
        let outcome: UnaryOutcome<PreparedMcpSubscriptionWire, McpTransportFailure> =
            decode_metadata(
                response,
                MCP_STREAMABLE_HTTP_SUBSCRIPTION_PREPARED,
                self.limits,
            )
            .map_err(|_| {
                mcp_rpc_uncertain(
                    "mcp_egress_subscription_establish_response_invalid",
                    request_digest.clone(),
                )
            })?;
        match outcome {
            UnaryOutcome::Succeeded(prepared)
                if prepared.schema_version == 1 && prepared.request_digest == request_digest =>
            {
                Ok(PreparedMcpSubscription::new(
                    prepared.established,
                    Box::new(EgressRpcMcpSubscriptionActivation {
                        request_sender,
                        response_stream,
                        limits: self.limits,
                        sink,
                        request,
                        request_digest,
                        dependency_observer: Arc::clone(&self.dependency_observer),
                    }),
                ))
            }
            UnaryOutcome::Succeeded(_) => Err(mcp_rpc_uncertain(
                "mcp_egress_subscription_establish_response_invalid",
                request_digest,
            )),
            UnaryOutcome::Failed(failure) => {
                failure.validate_wire_shape().map_err(|_| {
                    mcp_rpc_uncertain(
                        "mcp_egress_subscription_establish_response_invalid",
                        request_digest,
                    )
                })?;
                Err(failure)
            }
        }
    }
}

struct EgressRpcMcpSubscriptionActivation {
    request_sender: mpsc::Sender<ClosedEgressEnvelope>,
    response_stream: tonic::Streaming<ClosedEgressEnvelope>,
    limits: EgressInternalRpcLimits,
    sink: Arc<dyn McpStreamableHttpSubscriptionSink>,
    request: McpStreamableHttpSubscriptionRequest,
    request_digest: Sha256Digest,
    dependency_observer: Arc<dyn EgressRpcDependencyObserver>,
}

#[async_trait]
impl McpSubscriptionActivation for EgressRpcMcpSubscriptionActivation {
    async fn activate(mut self: Box<Self>) {
        tokio::spawn(async move {
            if let Err(failure) = self.drive().await {
                let _ = self
                    .sink
                    .report_termination(McpStreamableHttpSubscriptionTermination {
                        tenant_id: self.request.tenant_id.clone(),
                        subscription_id: self.request.subscription_id.clone(),
                        authorization_generation: self.request.authorization_generation,
                        session_generation: self.request.session_generation,
                        worker_process_generation_id: self
                            .request
                            .worker_process_generation_id
                            .clone(),
                        observed_at: Utc::now(),
                        failure,
                    })
                    .await;
            }
        });
    }
}

impl EgressRpcMcpSubscriptionActivation {
    async fn drive(&mut self) -> Result<(), McpTransportFailure> {
        let activation = ActivateMcpSubscriptionWire {
            schema_version: 1,
            request_digest: self.request_digest.clone(),
            tenant_id: self.request.tenant_id.clone(),
            subscription_id: self.request.subscription_id.clone(),
            authorization_generation: self.request.authorization_generation,
            session_generation: self.request.session_generation,
        };
        let envelope = encode_metadata(
            &activation,
            ACTIVATE_MCP_STREAMABLE_HTTP_SUBSCRIPTION,
            self.limits,
        )
        .map_err(|_| mcp_rpc_rejected("mcp_egress_subscription_activation_invalid"))?;
        self.request_sender.send(envelope).await.map_err(|_| {
            mcp_rpc_uncertain(
                "mcp_egress_subscription_activation_rpc_uncertain",
                self.request_digest.clone(),
            )
        })?;
        loop {
            let response = self.response_stream.message().await;
            observe_egress_rpc(&self.dependency_observer, response.is_ok());
            let envelope = response.map_err(|_| {
                mcp_rpc_uncertain(
                    "mcp_egress_subscription_stream_rpc_uncertain",
                    self.request_digest.clone(),
                )
            })?;
            let Some(envelope) = envelope else {
                return Err(mcp_rpc_uncertain(
                    "mcp_egress_subscription_stream_closed",
                    self.request_digest.clone(),
                ));
            };
            match envelope.operation.as_str() {
                MCP_STREAMABLE_HTTP_SUBSCRIPTION_NOTIFICATION => {
                    let (metadata, payload): (McpSubscriptionNotificationMetadata, Vec<u8>) =
                        decode_metadata_payload(
                            envelope,
                            MCP_STREAMABLE_HTTP_SUBSCRIPTION_NOTIFICATION,
                            self.limits,
                        )
                        .map_err(|_| {
                            mcp_rpc_uncertain(
                                "mcp_egress_subscription_notification_invalid",
                                self.request_digest.clone(),
                            )
                        })?;
                    if metadata.schema_version != 1
                        || metadata.tenant_id != self.request.tenant_id
                        || metadata.subscription_id != self.request.subscription_id
                        || metadata.authorization_generation
                            != self.request.authorization_generation
                        || metadata.session_generation != self.request.session_generation
                        || metadata.event_generation == 0
                    {
                        return Err(mcp_rpc_uncertain(
                            "mcp_egress_subscription_notification_invalid",
                            self.request_digest.clone(),
                        ));
                    }
                    let wire = SensitiveMcpNotificationWire::new(payload).map_err(|_| {
                        mcp_rpc_uncertain(
                            "mcp_egress_subscription_notification_invalid",
                            self.request_digest.clone(),
                        )
                    })?;
                    self.sink
                        .ingest_notification(McpStreamableHttpSubscriptionNotification {
                            tenant_id: metadata.tenant_id,
                            subscription_id: metadata.subscription_id,
                            authorization_generation: metadata.authorization_generation,
                            session_generation: metadata.session_generation,
                            event_generation: metadata.event_generation,
                            event_key_digest: metadata.event_key_digest,
                            wire,
                            received_at: metadata.received_at,
                        })
                        .await
                        .map_err(|_| {
                            mcp_rpc_uncertain(
                                "mcp_egress_subscription_host_sink_unavailable",
                                self.request_digest.clone(),
                            )
                        })?;
                }
                MCP_STREAMABLE_HTTP_SUBSCRIPTION_TERMINATION => {
                    let metadata: McpSubscriptionTerminationMetadata = decode_metadata(
                        envelope,
                        MCP_STREAMABLE_HTTP_SUBSCRIPTION_TERMINATION,
                        self.limits,
                    )
                    .map_err(|_| {
                        mcp_rpc_uncertain(
                            "mcp_egress_subscription_termination_invalid",
                            self.request_digest.clone(),
                        )
                    })?;
                    if metadata.schema_version != 1
                        || metadata.tenant_id != self.request.tenant_id
                        || metadata.subscription_id != self.request.subscription_id
                        || metadata.authorization_generation
                            != self.request.authorization_generation
                        || metadata.session_generation != self.request.session_generation
                        || metadata.worker_process_generation_id
                            != self.request.worker_process_generation_id
                        || metadata.failure.validate_wire_shape().is_err()
                    {
                        return Err(mcp_rpc_uncertain(
                            "mcp_egress_subscription_termination_invalid",
                            self.request_digest.clone(),
                        ));
                    }
                    self.sink
                        .report_termination(McpStreamableHttpSubscriptionTermination {
                            tenant_id: metadata.tenant_id,
                            subscription_id: metadata.subscription_id,
                            authorization_generation: metadata.authorization_generation,
                            session_generation: metadata.session_generation,
                            worker_process_generation_id: metadata.worker_process_generation_id,
                            observed_at: metadata.observed_at,
                            failure: metadata.failure,
                        })
                        .await
                        .map_err(|_| {
                            mcp_rpc_uncertain(
                                "mcp_egress_subscription_host_sink_unavailable",
                                self.request_digest.clone(),
                            )
                        })?;
                    return Ok(());
                }
                _ => {
                    return Err(mcp_rpc_uncertain(
                        "mcp_egress_subscription_stream_frame_invalid",
                        self.request_digest.clone(),
                    ))
                }
            }
        }
    }
}

async fn capability_cancel(
    mut client: TracedEgressBrokerServiceClient,
    dependency_observer: Arc<dyn EgressRpcDependencyObserver>,
    request: CapabilityTransportCancelRequest,
    request_operation: &'static str,
    response_operation: &'static str,
    limits: EgressInternalRpcLimits,
    invoke: impl for<'a> FnOnce(
        &'a mut TracedEgressBrokerServiceClient,
        Request<ClosedEgressEnvelope>,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<ClosedEgressEnvelope>, Status>>
                + Send
                + 'a,
        >,
    >,
) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
    request
        .validate_at(Utc::now())
        .map_err(|_| capability_rpc_failure("capability_egress_rpc_cancel_invalid", false))?;
    let envelope = encode_metadata(&request, request_operation, limits)
        .map_err(|_| capability_rpc_failure("capability_egress_rpc_cancel_invalid", false))?;
    let response = invoke(&mut client, Request::new(envelope)).await;
    observe_egress_rpc(&dependency_observer, response.is_ok());
    let response = response.map_err(capability_status_failure)?;
    match decode_metadata::<
        UnaryOutcome<CapabilityTransportCancelOutcome, CapabilityAdapterFailure>,
    >(response.into_inner(), response_operation, limits)
    .map_err(|_| capability_rpc_failure("capability_egress_rpc_response_invalid", false))?
    {
        UnaryOutcome::Succeeded(outcome) => Ok(outcome),
        UnaryOutcome::Failed(failure) if failure.validate().is_ok() => Err(failure),
        UnaryOutcome::Failed(_) => Err(capability_rpc_failure(
            "capability_egress_rpc_response_invalid",
            false,
        )),
    }
}

pub struct EgressBrokerGrpcService<M, H, G> {
    model: Arc<M>,
    http: Arc<H>,
    grpc: Arc<G>,
    remote_context: Option<Arc<dyn RemoteContextSearchConnector>>,
    mcp_oauth: Option<Arc<dyn McpOAuthCredentialBroker>>,
    mcp_oauth_pkce_cleaner: Option<Arc<dyn McpOAuthPkceSecretCleaner>>,
    mcp_discovery: Option<Arc<dyn McpDiscoveryTransportConnector>>,
    mcp_streamable_http: Option<Arc<dyn McpStreamableHttpConnector>>,
    mcp_resource_refresh: Option<Arc<dyn McpResourceRefreshConnector>>,
    mcp_streamable_http_subscription: Option<Arc<dyn McpStreamableHttpSubscriptionConnector>>,
    mcp_subscription_bridge: Option<Arc<EgressMcpSubscriptionBridge>>,
    limits: EgressInternalRpcLimits,
}

impl<M, H, G> EgressBrokerGrpcService<M, H, G> {
    pub fn new(model: Arc<M>, http: Arc<H>, grpc: Arc<G>, limits: EgressInternalRpcLimits) -> Self {
        Self {
            model,
            http,
            grpc,
            remote_context: None,
            mcp_oauth: None,
            mcp_oauth_pkce_cleaner: None,
            mcp_discovery: None,
            mcp_streamable_http: None,
            mcp_resource_refresh: None,
            mcp_streamable_http_subscription: None,
            mcp_subscription_bridge: None,
            limits,
        }
    }

    pub fn with_remote_context(mut self, connector: Arc<dyn RemoteContextSearchConnector>) -> Self {
        self.remote_context = Some(connector);
        self
    }

    pub fn with_mcp_oauth(
        mut self,
        broker: Arc<dyn McpOAuthCredentialBroker>,
        cleaner: Arc<dyn McpOAuthPkceSecretCleaner>,
    ) -> Self {
        self.mcp_oauth = Some(broker);
        self.mcp_oauth_pkce_cleaner = Some(cleaner);
        self
    }

    pub fn with_mcp_streamable_http(
        mut self,
        connector: Arc<dyn McpStreamableHttpConnector>,
    ) -> Self {
        self.mcp_streamable_http = Some(connector);
        self
    }

    pub fn with_mcp_discovery(
        mut self,
        connector: Arc<dyn McpDiscoveryTransportConnector>,
    ) -> Self {
        self.mcp_discovery = Some(connector);
        self
    }

    pub fn with_mcp_resource_refresh(
        mut self,
        connector: Arc<dyn McpResourceRefreshConnector>,
    ) -> Self {
        self.mcp_resource_refresh = Some(connector);
        self
    }

    pub fn with_mcp_streamable_http_subscription(
        mut self,
        connector: Arc<dyn McpStreamableHttpSubscriptionConnector>,
        bridge: Arc<EgressMcpSubscriptionBridge>,
    ) -> Self {
        self.mcp_streamable_http_subscription = Some(connector);
        self.mcp_subscription_bridge = Some(bridge);
        self
    }
}

#[tonic::async_trait]
impl<M, H, G> EgressBrokerService for EgressBrokerGrpcService<M, H, G>
where
    M: ModelProviderWireConnector + 'static,
    H: HttpNetworkTransport + 'static,
    G: GrpcNetworkTransport + 'static,
{
    type OpenModelProviderStream =
        Pin<Box<dyn Stream<Item = Result<ClosedEgressEnvelope, Status>> + Send + 'static>>;
    type StreamMcpStreamableHttpSubscriptionStream =
        Pin<Box<dyn Stream<Item = Result<ClosedEgressEnvelope, Status>> + Send + 'static>>;

    async fn open_model_provider(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<Self::OpenModelProviderStream>, Status> {
        require_role(&request, EgressCallerRole::ModelWorker)?;
        let trace = trace_context(&request)?;
        let request = decode_model_request(request.into_inner(), self.limits)?;
        let limits = self.limits;
        let stream: Self::OpenModelProviderStream =
            match scope_trace(trace, self.model.open(request)).await {
                Ok(upstream) => Box::pin(stream::unfold(
                    (upstream, false),
                    move |(mut upstream, terminal)| async move {
                        if terminal {
                            return None;
                        }
                        match upstream.next().await {
                            Some(Ok(event)) => Some((
                                encode_metadata(
                                    &ModelStreamFrame::Event(event),
                                    MODEL_PROVIDER_FRAME,
                                    limits,
                                )
                                .map_err(Status::from),
                                (upstream, false),
                            )),
                            Some(Err(failure)) => Some((
                                encode_metadata(
                                    &ModelStreamFrame::Failed(failure),
                                    MODEL_PROVIDER_FRAME,
                                    limits,
                                )
                                .map_err(Status::from),
                                (upstream, true),
                            )),
                            None => None,
                        }
                    },
                )),
                Err(failure) => Box::pin(stream::once(async move {
                    encode_metadata(
                        &ModelStreamFrame::Failed(failure),
                        MODEL_PROVIDER_FRAME,
                        limits,
                    )
                    .map_err(Status::from)
                })),
            };
        Ok(Response::new(stream))
    }

    async fn cancel_model_provider(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        require_role(&request, EgressCallerRole::ModelWorker)?;
        let trace = trace_context(&request)?;
        let (protocol, cancel): (ModelProviderWireProtocol, ModelAdapterCancelRequest) =
            decode_metadata(request.into_inner(), CANCEL_MODEL_PROVIDER, self.limits)?;
        cancel
            .validate_shape_at(Utc::now())
            .map_err(|_| Status::invalid_argument("invalid Model Provider cancel"))?;
        let outcome = match scope_trace(trace, self.model.cancel(protocol, cancel)).await {
            Ok(outcome) => UnaryOutcome::Succeeded(outcome),
            Err(failure) => UnaryOutcome::Failed(failure),
        };
        Ok(Response::new(encode_metadata(
            &outcome,
            MODEL_PROVIDER_CANCEL_OUTCOME,
            self.limits,
        )?))
    }

    async fn round_trip_capability_http(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        require_role(&request, EgressCallerRole::CapabilityWorker)?;
        let trace = trace_context(&request)?;
        let request = decode_http_request(request.into_inner(), self.limits)?;
        let outcome = match scope_trace(trace, self.http.round_trip(request)).await {
            Ok(response) => UnaryOutcome::Succeeded(response),
            Err(failure) => UnaryOutcome::Failed(failure),
        };
        Ok(Response::new(encode_http_outcome(outcome, self.limits)?))
    }

    async fn cancel_capability_http(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        require_role(&request, EgressCallerRole::CapabilityWorker)?;
        let trace = trace_context(&request)?;
        let request: CapabilityTransportCancelRequest =
            decode_metadata(request.into_inner(), CANCEL_CAPABILITY_HTTP, self.limits)?;
        request
            .validate_at(Utc::now())
            .map_err(|_| Status::invalid_argument("invalid Capability HTTP cancel"))?;
        let outcome = match scope_trace(trace, self.http.cancel(request)).await {
            Ok(outcome) => UnaryOutcome::Succeeded(outcome),
            Err(failure) => UnaryOutcome::Failed(failure),
        };
        Ok(Response::new(encode_metadata(
            &outcome,
            CAPABILITY_HTTP_CANCEL_OUTCOME,
            self.limits,
        )?))
    }

    async fn unary_capability_grpc(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        require_role(&request, EgressCallerRole::CapabilityWorker)?;
        let trace = trace_context(&request)?;
        let request = decode_grpc_request(request.into_inner(), self.limits)?;
        let outcome = match scope_trace(trace, self.grpc.unary(request)).await {
            Ok(response) => UnaryOutcome::Succeeded(response),
            Err(failure) => UnaryOutcome::Failed(failure),
        };
        Ok(Response::new(encode_grpc_outcome(outcome, self.limits)?))
    }

    async fn cancel_capability_grpc(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        require_role(&request, EgressCallerRole::CapabilityWorker)?;
        let trace = trace_context(&request)?;
        let request: CapabilityTransportCancelRequest =
            decode_metadata(request.into_inner(), CANCEL_CAPABILITY_GRPC, self.limits)?;
        request
            .validate_at(Utc::now())
            .map_err(|_| Status::invalid_argument("invalid Capability gRPC cancel"))?;
        let outcome = match scope_trace(trace, self.grpc.cancel(request)).await {
            Ok(outcome) => UnaryOutcome::Succeeded(outcome),
            Err(failure) => UnaryOutcome::Failed(failure),
        };
        Ok(Response::new(encode_metadata(
            &outcome,
            CAPABILITY_GRPC_CANCEL_OUTCOME,
            self.limits,
        )?))
    }

    async fn query_remote_context(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        require_role(&request, EgressCallerRole::ContextWorker)?;
        let trace = trace_context(&request)?;
        let connector = self
            .remote_context
            .as_ref()
            .ok_or_else(|| Status::unavailable("Remote Context connector is not installed"))?;
        let request: RemoteContextSearchRequest =
            decode_metadata(request.into_inner(), QUERY_REMOTE_CONTEXT, self.limits)?;
        request
            .validate_at(Utc::now())
            .map_err(|_| Status::invalid_argument("invalid Remote Context request"))?;
        let outcome = match scope_trace(trace, connector.query(request)).await {
            Ok(response) => UnaryOutcome::Succeeded(response),
            Err(failure) => {
                failure.validate().map_err(|_| {
                    Status::failed_precondition(
                        "Remote Context connector returned invalid failure evidence",
                    )
                })?;
                UnaryOutcome::Failed(failure)
            }
        };
        Ok(Response::new(encode_metadata(
            &outcome,
            REMOTE_CONTEXT_OUTCOME,
            self.limits,
        )?))
    }

    async fn exchange_mcp_o_auth_authorization_code(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        require_role(&request, EgressCallerRole::McpCallback)?;
        let trace = trace_context(&request)?;
        let broker = self
            .mcp_oauth
            .as_ref()
            .ok_or_else(|| Status::unavailable("MCP OAuth broker is not installed"))?;
        let (contract, code): (McpOAuthExchangeContract, Vec<u8>) = decode_metadata_payload(
            request.into_inner(),
            EXCHANGE_MCP_OAUTH_AUTHORIZATION_CODE,
            self.limits,
        )?;
        let now = Utc::now();
        contract
            .validate_at(now)
            .map_err(|_| Status::invalid_argument("invalid MCP OAuth exchange contract"))?;
        let code = SensitiveOAuthValue::from_decoded(code, MAX_MCP_OAUTH_CODE_BYTES)
            .map_err(|_| Status::invalid_argument("invalid MCP OAuth authorization code"))?;
        let outcome = match scope_trace(
            trace,
            broker.exchange_authorization_code(&contract, code, now),
        )
        .await
        {
            Ok(grant) => {
                grant
                    .validate_for_binding(&contract.binding, now)
                    .map_err(|_| {
                        Status::failed_precondition("MCP OAuth broker returned invalid evidence")
                    })?;
                UnaryOutcome::Succeeded(grant)
            }
            Err(failure) => UnaryOutcome::Failed(McpOAuthCredentialFailureWire::from(failure)),
        };
        Ok(Response::new(encode_metadata(
            &outcome,
            MCP_OAUTH_AUTHORIZATION_CODE_OUTCOME,
            self.limits,
        )?))
    }

    async fn delete_mcp_o_auth_pkce_secret(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        require_role(&request, EgressCallerRole::McpCleanupWorker)?;
        let trace = trace_context(&request)?;
        let cleaner = self
            .mcp_oauth_pkce_cleaner
            .as_ref()
            .ok_or_else(|| Status::unavailable("MCP OAuth PKCE cleaner is not installed"))?;
        let authorization: AuthorizedMcpOAuthPkceCleanup = decode_metadata(
            request.into_inner(),
            DELETE_MCP_OAUTH_PKCE_SECRET,
            self.limits,
        )?;
        validate_cleanup_authorization(&authorization)
            .map_err(|_| Status::invalid_argument("invalid MCP OAuth PKCE cleanup"))?;
        let outcome = match scope_trace(trace, cleaner.delete_exact(&authorization)).await {
            Ok(disposition) => UnaryOutcome::Succeeded(disposition),
            Err(failure) => UnaryOutcome::Failed(McpOAuthPkceCleanupFailureWire::from(failure)),
        };
        Ok(Response::new(encode_metadata(
            &outcome,
            MCP_OAUTH_PKCE_SECRET_DELETE_OUTCOME,
            self.limits,
        )?))
    }

    async fn execute_mcp_streamable_http(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        require_role(&request, EgressCallerRole::McpHost)?;
        let trace = trace_context(&request)?;
        let connector = self
            .mcp_streamable_http
            .as_ref()
            .ok_or_else(|| Status::unavailable("MCP Streamable HTTP connector is not installed"))?;
        let request: McpStreamableHttpRequest = decode_metadata(
            request.into_inner(),
            EXECUTE_MCP_STREAMABLE_HTTP,
            self.limits,
        )?;
        let outcome = match scope_trace(trace, connector.execute(request)).await {
            Ok(outcome) => UnaryOutcome::Succeeded(outcome),
            Err(failure) => {
                failure.validate_wire_shape().map_err(|_| {
                    Status::failed_precondition("MCP connector returned invalid failure evidence")
                })?;
                UnaryOutcome::Failed(failure)
            }
        };
        Ok(Response::new(encode_metadata(
            &outcome,
            MCP_STREAMABLE_HTTP_OUTCOME,
            self.limits,
        )?))
    }

    async fn discover_mcp_streamable_http(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        require_role(&request, EgressCallerRole::McpDiscoveryWorker)?;
        let trace = trace_context(&request)?;
        let connector = self
            .mcp_discovery
            .as_ref()
            .ok_or_else(|| Status::unavailable("MCP discovery connector is not installed"))?;
        let request: McpDiscoveryTransportRequest = decode_metadata(
            request.into_inner(),
            DISCOVER_MCP_STREAMABLE_HTTP,
            self.limits,
        )?;
        request
            .validate_at(Utc::now())
            .map_err(|_| Status::invalid_argument("invalid MCP discovery request"))?;
        let outcome = match scope_trace(trace, connector.discover(request.clone())).await {
            Ok(response) => {
                response.validate_for(&request).map_err(|_| {
                    Status::failed_precondition("MCP discovery connector returned invalid response")
                })?;
                UnaryOutcome::Succeeded(response)
            }
            Err(failure) => {
                failure.validate_wire_shape().map_err(|_| {
                    Status::failed_precondition(
                        "MCP discovery connector returned invalid failure evidence",
                    )
                })?;
                UnaryOutcome::Failed(failure)
            }
        };
        Ok(Response::new(encode_mcp_discovery_outcome(
            outcome,
            self.limits,
        )?))
    }

    async fn refresh_mcp_resources(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        require_role(&request, EgressCallerRole::McpHost)?;
        let trace = trace_context(&request)?;
        let connector = self.mcp_resource_refresh.as_ref().ok_or_else(|| {
            Status::unavailable("MCP Resource Refresh connector is not installed")
        })?;
        let request: McpResourceRefreshTransportRequest =
            decode_metadata(request.into_inner(), REFRESH_MCP_RESOURCES, self.limits)?;
        request
            .validate_at(Utc::now())
            .map_err(|_| Status::invalid_argument("invalid MCP Resource Refresh request"))?;
        let outcome = match scope_trace(trace, connector.refresh_resources(request.clone())).await {
            Ok(evidence)
                if evidence.execution_identity_digest == request.execution_identity_digest
                    && evidence.request_digest == request.request_digest
                    && evidence.observed_at <= request.deadline
                    && evidence.resource_count <= request.maximum_resources
                    && evidence.item_count <= request.maximum_items
                    && evidence.byte_count <= request.maximum_total_bytes =>
            {
                UnaryOutcome::Succeeded(evidence)
            }
            Ok(_) => {
                return Err(Status::failed_precondition(
                    "MCP Resource Refresh connector returned invalid evidence",
                ))
            }
            Err(failure) => {
                failure.validate_wire_shape().map_err(|_| {
                    Status::failed_precondition(
                        "MCP Resource Refresh connector returned invalid failure evidence",
                    )
                })?;
                UnaryOutcome::Failed(failure)
            }
        };
        Ok(Response::new(encode_metadata(
            &outcome,
            MCP_RESOURCE_REFRESH_OUTCOME,
            self.limits,
        )?))
    }

    async fn cancel_mcp_remote_task(
        &self,
        request: Request<ClosedEgressEnvelope>,
    ) -> Result<Response<ClosedEgressEnvelope>, Status> {
        require_role(&request, EgressCallerRole::McpHost)?;
        let trace = trace_context(&request)?;
        let connector = self
            .mcp_streamable_http
            .as_ref()
            .ok_or_else(|| Status::unavailable("MCP Streamable HTTP connector is not installed"))?;
        let request: McpStreamableHttpRequest =
            decode_metadata(request.into_inner(), CANCEL_MCP_REMOTE_TASK, self.limits)?;
        let outcome = match scope_trace(trace, connector.cancel_remote_task(request)).await {
            Ok(outcome) => UnaryOutcome::Succeeded(outcome),
            Err(failure) => {
                failure.validate_wire_shape().map_err(|_| {
                    Status::failed_precondition("MCP connector returned invalid failure evidence")
                })?;
                UnaryOutcome::Failed(failure)
            }
        };
        Ok(Response::new(encode_metadata(
            &outcome,
            MCP_REMOTE_TASK_CANCEL_OUTCOME,
            self.limits,
        )?))
    }

    async fn stream_mcp_streamable_http_subscription(
        &self,
        request: Request<tonic::Streaming<ClosedEgressEnvelope>>,
    ) -> Result<Response<Self::StreamMcpStreamableHttpSubscriptionStream>, Status> {
        require_role(&request, EgressCallerRole::McpSubscriptionWorker)?;
        let trace = trace_context(&request)?;
        let connector = self
            .mcp_streamable_http_subscription
            .as_ref()
            .ok_or_else(|| {
                Status::unavailable("MCP Streamable HTTP subscription connector is not installed")
            })?;
        let bridge = self.mcp_subscription_bridge.as_ref().ok_or_else(|| {
            Status::unavailable("MCP Streamable HTTP subscription bridge is not installed")
        })?;
        let mut inbound = request.into_inner();
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), inbound.message())
            .await
            .map_err(|_| Status::deadline_exceeded("MCP subscription establish frame timed out"))?
            .map_err(|_| Status::invalid_argument("invalid MCP subscription establish stream"))?
            .ok_or_else(|| {
                Status::invalid_argument("MCP subscription establish frame is required")
            })?;
        let request: McpStreamableHttpSubscriptionRequest = decode_metadata(
            first,
            ESTABLISH_MCP_STREAMABLE_HTTP_SUBSCRIPTION,
            self.limits,
        )?;
        let request_deadline = request.deadline;
        let (sender, receiver) = mpsc::channel(bridge.bridge_limits.event_buffer_capacity);
        let pending = match scope_trace(trace, bridge.establish(connector.as_ref(), request)).await
        {
            Ok((prepared, pending)) => {
                sender
                    .send(encode_metadata(
                        &UnaryOutcome::<PreparedMcpSubscriptionWire, McpTransportFailure>::Succeeded(
                            prepared,
                        ),
                        MCP_STREAMABLE_HTTP_SUBSCRIPTION_PREPARED,
                        self.limits,
                    )?)
                    .await
                    .map_err(|_| Status::cancelled("MCP subscription stream closed"))?;
                pending
            }
            Err(failure) => {
                failure.validate_wire_shape().map_err(|_| {
                    Status::failed_precondition(
                        "MCP subscription connector returned invalid failure evidence",
                    )
                })?;
                sender
                    .send(encode_metadata(
                        &UnaryOutcome::<PreparedMcpSubscriptionWire, McpTransportFailure>::Failed(
                            failure,
                        ),
                        MCP_STREAMABLE_HTTP_SUBSCRIPTION_PREPARED,
                        self.limits,
                    )?)
                    .await
                    .map_err(|_| Status::cancelled("MCP subscription stream closed"))?;
                drop(sender);
                let stream = stream::unfold(receiver, |mut receiver| async move {
                    receiver
                        .recv()
                        .await
                        .map(|envelope| (Ok(envelope), receiver))
                });
                return Ok(Response::new(Box::pin(stream)));
            }
        };
        let bridge = Arc::clone(bridge);
        let activation_sender = sender.clone();
        let limits = self.limits;
        tokio::spawn(scope_trace(trace, async move {
            let remaining = (request_deadline - Utc::now())
                .to_std()
                .unwrap_or_default()
                .min(std::time::Duration::from_secs(30));
            let activation = if remaining.is_zero() {
                None
            } else {
                tokio::time::timeout(remaining, inbound.message())
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .flatten()
            };
            let Some(activation) = activation else {
                bridge.discard_pending(pending).await;
                return;
            };
            let activation = decode_metadata::<ActivateMcpSubscriptionWire>(
                activation,
                ACTIVATE_MCP_STREAMABLE_HTTP_SUBSCRIPTION,
                limits,
            );
            match activation {
                Ok(activation) => {
                    let _ = bridge
                        .activate(pending, activation, activation_sender)
                        .await;
                }
                Err(_) => bridge.discard_pending(pending).await,
            }
        }));
        drop(sender);
        let stream = stream::unfold(receiver, |mut receiver| async move {
            receiver
                .recv()
                .await
                .map(|envelope| (Ok(envelope), receiver))
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

fn encode_model_request(
    mut request: ModelProviderWireRequest,
    limits: EgressInternalRpcLimits,
) -> Result<ClosedEgressEnvelope, EgressRpcError> {
    let payload =
        serde_jcs::to_vec(&request.request_body).map_err(|_| EgressRpcError::InvalidEnvelope)?;
    request.request_body = serde_json::Value::Null;
    encode_metadata_payload(&request, payload, OPEN_MODEL_PROVIDER, limits)
}

fn decode_model_request(
    envelope: ClosedEgressEnvelope,
    limits: EgressInternalRpcLimits,
) -> Result<ModelProviderWireRequest, EgressRpcError> {
    let (mut request, payload): (ModelProviderWireRequest, Vec<u8>) =
        decode_metadata_payload(envelope, OPEN_MODEL_PROVIDER, limits)?;
    if !request.request_body.is_null() {
        return Err(EgressRpcError::InvalidEnvelope);
    }
    request.request_body = decode_canonical_payload_json(&payload, limits)?;
    request
        .validate_at(Utc::now())
        .map_err(|_| EgressRpcError::InvalidEnvelope)?;
    Ok(request)
}

fn encode_http_request(
    mut request: HttpTransportRequest,
    limits: EgressInternalRpcLimits,
) -> Result<ClosedEgressEnvelope, EgressRpcError> {
    let payload = std::mem::take(&mut request.body);
    encode_metadata_payload(&request, payload, ROUND_TRIP_CAPABILITY_HTTP, limits)
}

fn decode_http_request(
    envelope: ClosedEgressEnvelope,
    limits: EgressInternalRpcLimits,
) -> Result<HttpTransportRequest, EgressRpcError> {
    let (mut request, payload): (HttpTransportRequest, Vec<u8>) =
        decode_metadata_payload(envelope, ROUND_TRIP_CAPABILITY_HTTP, limits)?;
    if !request.body.is_empty() {
        return Err(EgressRpcError::InvalidEnvelope);
    }
    request.body = payload;
    request
        .validate_at(Utc::now())
        .map_err(|_| EgressRpcError::InvalidEnvelope)?;
    Ok(request)
}

fn encode_grpc_request(
    mut request: GrpcTransportRequest,
    limits: EgressInternalRpcLimits,
) -> Result<ClosedEgressEnvelope, EgressRpcError> {
    let payload = std::mem::take(&mut request.message);
    encode_metadata_payload(&request, payload, UNARY_CAPABILITY_GRPC, limits)
}

fn decode_grpc_request(
    envelope: ClosedEgressEnvelope,
    limits: EgressInternalRpcLimits,
) -> Result<GrpcTransportRequest, EgressRpcError> {
    let (mut request, payload): (GrpcTransportRequest, Vec<u8>) =
        decode_metadata_payload(envelope, UNARY_CAPABILITY_GRPC, limits)?;
    if !request.message.is_empty() {
        return Err(EgressRpcError::InvalidEnvelope);
    }
    request.message = payload;
    request
        .validate_at(Utc::now())
        .map_err(|_| EgressRpcError::InvalidEnvelope)?;
    Ok(request)
}

fn encode_http_outcome(
    outcome: UnaryOutcome<HttpTransportResponse, CapabilityAdapterFailure>,
    limits: EgressInternalRpcLimits,
) -> Result<ClosedEgressEnvelope, EgressRpcError> {
    match outcome {
        UnaryOutcome::Succeeded(mut response) => {
            let payload = std::mem::take(&mut response.body);
            encode_metadata_payload(
                &UnaryOutcome::<HttpTransportResponse, CapabilityAdapterFailure>::Succeeded(
                    response,
                ),
                payload,
                CAPABILITY_HTTP_OUTCOME,
                limits,
            )
        }
        UnaryOutcome::Failed(failure) => encode_metadata_payload(
            &UnaryOutcome::<HttpTransportResponse, CapabilityAdapterFailure>::Failed(failure),
            Vec::new(),
            CAPABILITY_HTTP_OUTCOME,
            limits,
        ),
    }
}

fn encode_mcp_discovery_outcome(
    outcome: UnaryOutcome<McpDiscoveryTransportResponse, McpTransportFailure>,
    limits: EgressInternalRpcLimits,
) -> Result<ClosedEgressEnvelope, EgressRpcError> {
    match outcome {
        UnaryOutcome::Succeeded(mut response) => {
            let payload = std::mem::take(&mut response.descriptor_bytes);
            encode_metadata_payload(
                &UnaryOutcome::<McpDiscoveryTransportResponse, McpTransportFailure>::Succeeded(
                    response,
                ),
                payload,
                MCP_STREAMABLE_HTTP_DISCOVERY_OUTCOME,
                limits,
            )
        }
        UnaryOutcome::Failed(failure) => encode_metadata_payload(
            &UnaryOutcome::<McpDiscoveryTransportResponse, McpTransportFailure>::Failed(failure),
            Vec::new(),
            MCP_STREAMABLE_HTTP_DISCOVERY_OUTCOME,
            limits,
        ),
    }
}

fn decode_mcp_discovery_outcome(
    envelope: ClosedEgressEnvelope,
    limits: EgressInternalRpcLimits,
) -> Result<UnaryOutcome<McpDiscoveryTransportResponse, McpTransportFailure>, EgressRpcError> {
    let (outcome, payload) = decode_metadata_payload::<
        UnaryOutcome<McpDiscoveryTransportResponse, McpTransportFailure>,
    >(envelope, MCP_STREAMABLE_HTTP_DISCOVERY_OUTCOME, limits)?;
    match outcome {
        UnaryOutcome::Succeeded(mut response) if response.descriptor_bytes.is_empty() => {
            response.descriptor_bytes = payload;
            Ok(UnaryOutcome::Succeeded(response))
        }
        UnaryOutcome::Failed(failure)
            if payload.is_empty() && failure.validate_wire_shape().is_ok() =>
        {
            Ok(UnaryOutcome::Failed(failure))
        }
        _ => Err(EgressRpcError::InvalidEnvelope),
    }
}

fn decode_http_outcome(
    envelope: ClosedEgressEnvelope,
    limits: EgressInternalRpcLimits,
) -> Result<UnaryOutcome<HttpTransportResponse, CapabilityAdapterFailure>, CapabilityAdapterFailure>
{
    let (outcome, payload) = decode_metadata_payload::<
        UnaryOutcome<HttpTransportResponse, CapabilityAdapterFailure>,
    >(envelope, CAPABILITY_HTTP_OUTCOME, limits)
    .map_err(|_| capability_rpc_failure("capability_egress_rpc_response_invalid", false))?;
    match outcome {
        UnaryOutcome::Succeeded(mut response) if response.body.is_empty() => {
            response.body = payload;
            Ok(UnaryOutcome::Succeeded(response))
        }
        UnaryOutcome::Failed(failure) if payload.is_empty() && failure.validate().is_ok() => {
            Ok(UnaryOutcome::Failed(failure))
        }
        _ => Err(capability_rpc_failure(
            "capability_egress_rpc_response_invalid",
            false,
        )),
    }
}

fn encode_grpc_outcome(
    outcome: UnaryOutcome<GrpcTransportResponse, CapabilityAdapterFailure>,
    limits: EgressInternalRpcLimits,
) -> Result<ClosedEgressEnvelope, EgressRpcError> {
    match outcome {
        UnaryOutcome::Succeeded(mut response) => {
            let payload = std::mem::take(&mut response.message);
            encode_metadata_payload(
                &UnaryOutcome::<GrpcTransportResponse, CapabilityAdapterFailure>::Succeeded(
                    response,
                ),
                payload,
                CAPABILITY_GRPC_OUTCOME,
                limits,
            )
        }
        UnaryOutcome::Failed(failure) => encode_metadata_payload(
            &UnaryOutcome::<GrpcTransportResponse, CapabilityAdapterFailure>::Failed(failure),
            Vec::new(),
            CAPABILITY_GRPC_OUTCOME,
            limits,
        ),
    }
}

fn decode_grpc_outcome(
    envelope: ClosedEgressEnvelope,
    limits: EgressInternalRpcLimits,
) -> Result<UnaryOutcome<GrpcTransportResponse, CapabilityAdapterFailure>, CapabilityAdapterFailure>
{
    let (outcome, payload) = decode_metadata_payload::<
        UnaryOutcome<GrpcTransportResponse, CapabilityAdapterFailure>,
    >(envelope, CAPABILITY_GRPC_OUTCOME, limits)
    .map_err(|_| capability_rpc_failure("capability_egress_rpc_response_invalid", false))?;
    match outcome {
        UnaryOutcome::Succeeded(mut response) if response.message.is_empty() => {
            response.message = payload;
            Ok(UnaryOutcome::Succeeded(response))
        }
        UnaryOutcome::Failed(failure) if payload.is_empty() && failure.validate().is_ok() => {
            Ok(UnaryOutcome::Failed(failure))
        }
        _ => Err(capability_rpc_failure(
            "capability_egress_rpc_response_invalid",
            false,
        )),
    }
}

fn encode_metadata<T: Serialize>(
    value: &T,
    operation: &'static str,
    limits: EgressInternalRpcLimits,
) -> Result<ClosedEgressEnvelope, EgressRpcError> {
    encode_metadata_payload(value, Vec::new(), operation, limits)
}

fn typed_digest<T: Serialize>(value: &T) -> Result<Sha256Digest, EgressRpcError> {
    let value = serde_json::to_value(value).map_err(|_| EgressRpcError::InvalidEnvelope)?;
    canonical_digest(&value)
        .map_err(|_| EgressRpcError::InvalidEnvelope)?
        .parse()
        .map_err(|_| EgressRpcError::InvalidEnvelope)
}

fn decode_metadata<T: DeserializeOwned>(
    envelope: ClosedEgressEnvelope,
    operation: &'static str,
    limits: EgressInternalRpcLimits,
) -> Result<T, EgressRpcError> {
    let (value, payload) = decode_metadata_payload(envelope, operation, limits)?;
    if !payload.is_empty() {
        return Err(EgressRpcError::InvalidEnvelope);
    }
    Ok(value)
}

fn encode_metadata_payload<T: Serialize>(
    value: &T,
    payload: Vec<u8>,
    operation: &'static str,
    limits: EgressInternalRpcLimits,
) -> Result<ClosedEgressEnvelope, EgressRpcError> {
    let canonical_metadata_json =
        serde_jcs::to_vec(value).map_err(|_| EgressRpcError::InvalidEnvelope)?;
    if canonical_metadata_json.is_empty()
        || canonical_metadata_json.len() > limits.maximum_metadata_bytes
        || payload.len() > limits.maximum_payload_bytes
    {
        return Err(EgressRpcError::InvalidEnvelope);
    }
    let metadata: serde_json::Value = serde_json::from_slice(&canonical_metadata_json)
        .map_err(|_| EgressRpcError::InvalidEnvelope)?;
    let metadata_digest: Sha256Digest = canonical_digest(&metadata)
        .map_err(|_| EgressRpcError::InvalidEnvelope)?
        .parse()
        .map_err(|_| EgressRpcError::InvalidEnvelope)?;
    let payload_digest = raw_digest(&payload)?;
    let envelope_digest = combined_digest(operation, &metadata_digest, &payload_digest)?;
    Ok(ClosedEgressEnvelope {
        schema_version: EGRESS_INTERNAL_RPC_SCHEMA_VERSION,
        operation: operation.to_owned(),
        canonical_metadata_json,
        payload,
        payload_digest: payload_digest.to_string(),
        envelope_digest: envelope_digest.to_string(),
    })
}

fn decode_metadata_payload<T: DeserializeOwned>(
    envelope: ClosedEgressEnvelope,
    operation: &'static str,
    limits: EgressInternalRpcLimits,
) -> Result<(T, Vec<u8>), EgressRpcError> {
    if envelope.schema_version != EGRESS_INTERNAL_RPC_SCHEMA_VERSION
        || envelope.operation != operation
        || envelope.canonical_metadata_json.is_empty()
        || envelope.canonical_metadata_json.len() > limits.maximum_metadata_bytes
        || envelope.payload.len() > limits.maximum_payload_bytes
    {
        return Err(EgressRpcError::InvalidEnvelope);
    }
    let metadata = parse_strict_json(
        &envelope.canonical_metadata_json,
        JsonLimits {
            max_bytes: limits.maximum_metadata_bytes,
            max_depth: 64,
            max_properties_per_object: 512,
            max_items_per_array: 4_096,
            max_string_bytes: limits.maximum_metadata_bytes,
        },
    )
    .map_err(|_| EgressRpcError::InvalidEnvelope)?;
    if serde_jcs::to_vec(&metadata).map_err(|_| EgressRpcError::InvalidEnvelope)?
        != envelope.canonical_metadata_json
    {
        return Err(EgressRpcError::InvalidEnvelope);
    }
    let metadata_digest: Sha256Digest = canonical_digest(&metadata)
        .map_err(|_| EgressRpcError::InvalidEnvelope)?
        .parse()
        .map_err(|_| EgressRpcError::InvalidEnvelope)?;
    let payload_digest: Sha256Digest = envelope
        .payload_digest
        .parse()
        .map_err(|_| EgressRpcError::InvalidEnvelope)?;
    let actual_payload_digest = raw_digest(&envelope.payload)?;
    let expected_envelope_digest: Sha256Digest = envelope
        .envelope_digest
        .parse()
        .map_err(|_| EgressRpcError::InvalidEnvelope)?;
    if payload_digest != actual_payload_digest
        || combined_digest(operation, &metadata_digest, &payload_digest)?
            != expected_envelope_digest
    {
        return Err(EgressRpcError::InvalidEnvelope);
    }
    let value = serde_json::from_value(metadata).map_err(|_| EgressRpcError::InvalidEnvelope)?;
    Ok((value, envelope.payload))
}

fn decode_canonical_payload_json(
    payload: &[u8],
    limits: EgressInternalRpcLimits,
) -> Result<serde_json::Value, EgressRpcError> {
    if payload.is_empty() || payload.len() > limits.maximum_payload_bytes {
        return Err(EgressRpcError::InvalidEnvelope);
    }
    let value = parse_strict_json(
        payload,
        JsonLimits {
            max_bytes: limits.maximum_payload_bytes,
            max_depth: 128,
            max_properties_per_object: payload.len(),
            max_items_per_array: payload.len(),
            max_string_bytes: limits.maximum_payload_bytes,
        },
    )
    .map_err(|_| EgressRpcError::InvalidEnvelope)?;
    if serde_jcs::to_vec(&value).map_err(|_| EgressRpcError::InvalidEnvelope)? != payload {
        return Err(EgressRpcError::InvalidEnvelope);
    }
    Ok(value)
}

fn raw_digest(bytes: &[u8]) -> Result<Sha256Digest, EgressRpcError> {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").map_err(|_| EgressRpcError::InvalidEnvelope)?;
    }
    encoded.parse().map_err(|_| EgressRpcError::InvalidEnvelope)
}

fn combined_digest(
    operation: &str,
    metadata_digest: &Sha256Digest,
    payload_digest: &Sha256Digest,
) -> Result<Sha256Digest, EgressRpcError> {
    canonical_digest(&serde_json::json!({
        "metadata_digest": metadata_digest,
        "operation": operation,
        "payload_digest": payload_digest,
        "schema_version": EGRESS_INTERNAL_RPC_SCHEMA_VERSION,
    }))
    .map_err(|_| EgressRpcError::InvalidEnvelope)?
    .parse()
    .map_err(|_| EgressRpcError::InvalidEnvelope)
}

fn model_status_failure(status: Status, deadline: DateTime<Utc>) -> ModelAdapterFailure {
    let retryable = matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::ResourceExhausted
    );
    model_rpc_failure("model_egress_rpc_unavailable", retryable, deadline)
}

fn model_rpc_failure(code: &str, retryable: bool, deadline: DateTime<Utc>) -> ModelAdapterFailure {
    let now = Utc::now();
    let retry_at = now + Duration::milliseconds(100);
    let can_retry = retryable && retry_at < deadline;
    ModelAdapterFailure {
        class: if can_retry {
            ModelAdapterFailureClass::RetryableBeforeDispatch
        } else if retryable {
            ModelAdapterFailureClass::Permanent
        } else {
            ModelAdapterFailureClass::RejectedBeforeDispatch
        },
        safe_code: code.to_owned(),
        safe_message: "Model Egress RPC failed".to_owned(),
        evidence_digest: safe_evidence(code),
        request_sent: false,
        retry_at: can_retry.then_some(retry_at),
    }
}

fn capability_status_failure(status: Status) -> CapabilityAdapterFailure {
    capability_rpc_failure(
        "capability_egress_rpc_unavailable",
        matches!(
            status.code(),
            tonic::Code::Unavailable
                | tonic::Code::DeadlineExceeded
                | tonic::Code::ResourceExhausted
        ),
    )
}

fn capability_rpc_failure(code: &str, retryable: bool) -> CapabilityAdapterFailure {
    CapabilityAdapterFailure {
        class: if retryable {
            CapabilityAdapterFailureClass::RetryableBeforeDispatch
        } else {
            CapabilityAdapterFailureClass::RejectedBeforeDispatch
        },
        safe_code: code.to_owned(),
        safe_message: "Capability Egress RPC failed".to_owned(),
        evidence_digest: safe_evidence(code),
        external_identity_digest: None,
    }
}

fn mcp_oauth_status_failure(status: Status) -> McpOAuthCredentialBrokerError {
    match status.code() {
        tonic::Code::InvalidArgument
        | tonic::Code::FailedPrecondition
        | tonic::Code::Unauthenticated
        | tonic::Code::PermissionDenied => McpOAuthCredentialBrokerError::Rejected,
        _ => McpOAuthCredentialBrokerError::ExchangeUncertain,
    }
}

fn mcp_oauth_cleanup_status_failure(status: Status) -> McpOAuthPkceSecretCleanupError {
    match status.code() {
        tonic::Code::InvalidArgument
        | tonic::Code::FailedPrecondition
        | tonic::Code::Unauthenticated
        | tonic::Code::PermissionDenied => McpOAuthPkceSecretCleanupError::Rejected,
        _ => McpOAuthPkceSecretCleanupError::OutcomeUncertain,
    }
}

fn validate_cleanup_authorization(
    authorization: &AuthorizedMcpOAuthPkceCleanup,
) -> Result<(), EgressRpcError> {
    if authorization.tenant_id.kind() != ResourceKind::Tenant
        || authorization.task_id.kind() != ResourceKind::Interaction
        || authorization.secret_binding.validate().is_err()
        || authorization.secret_binding.purpose.as_str() != MCP_OAUTH_PKCE_SECRET_PURPOSE
        || !matches!(
            authorization.secret_binding.resolution_policy,
            SecretResolutionPolicy::Pinned { .. }
        )
    {
        return Err(EgressRpcError::InvalidEnvelope);
    }
    Ok(())
}

fn mcp_rpc_status_failure(status: Status, request_identity: Sha256Digest) -> McpTransportFailure {
    match status.code() {
        tonic::Code::InvalidArgument
        | tonic::Code::FailedPrecondition
        | tonic::Code::Unauthenticated
        | tonic::Code::PermissionDenied => mcp_rpc_rejected("mcp_egress_rpc_rejected"),
        _ => mcp_rpc_uncertain("mcp_egress_rpc_completion_unobserved", request_identity),
    }
}

fn mcp_rpc_rejected(code: &str) -> McpTransportFailure {
    McpTransportFailure::RejectedBeforeDispatch(SafeMcpFailure {
        safe_code: code.to_owned(),
        safe_message: "MCP Egress RPC rejected the request".to_owned(),
        evidence_digest: safe_evidence(code),
    })
}

fn mcp_rpc_retryable(code: &str) -> McpTransportFailure {
    McpTransportFailure::RetryableBeforeDispatch(SafeMcpFailure {
        safe_code: code.to_owned(),
        safe_message: "MCP Egress RPC is temporarily unavailable".to_owned(),
        evidence_digest: safe_evidence(code),
    })
}

fn mcp_rpc_uncertain(code: &str, request_identity: Sha256Digest) -> McpTransportFailure {
    McpTransportFailure::PostDispatchUncertain {
        failure: SafeMcpFailure {
            safe_code: code.to_owned(),
            safe_message: "MCP Egress RPC completion could not be observed".to_owned(),
            evidence_digest: safe_evidence(code),
        },
        // The exact broker request identity is the only stable physical identity available when
        // the RPC response (which normally carries the DNS-pinned remote identity) is lost.
        external_identity_digest: request_identity,
    }
}

fn safe_evidence(code: &str) -> Sha256Digest {
    canonical_digest(&serde_json::json!({
        "domain": code,
        "schema_version": 1,
    }))
    .expect("static Egress RPC evidence is canonical")
    .parse()
    .expect("canonical digest is SHA-256")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressRpcError {
    InvalidConfiguration,
    InvalidEnvelope,
}

impl fmt::Display for EgressRpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "Egress RPC configuration is invalid",
            Self::InvalidEnvelope => "Egress RPC envelope is invalid",
        })
    }
}

impl Error for EgressRpcError {}

impl From<EgressRpcError> for Status {
    fn from(_: EgressRpcError) -> Self {
        Status::invalid_argument("invalid Egress RPC envelope")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::egress_broker_service_server::EgressBrokerServiceServer;
    use insight_platform_capability_adapters::CapabilityTransportRequestIdentity;
    use insight_platform_contracts::{
        CanonicalHttpEndpoint, CapabilityBackendKind, CapabilityEndpointScheme, ExactDeploymentRef,
        ExactSecretBindingRef, ExactVersionRef, McpClientCapabilities, McpNegotiatedCapabilities,
        ResourceId, ResourceKind, SecretPurpose, SecretResolutionPolicy, TraceFlags,
        TraceIdentityV1, MCP_PROTOCOL_BASELINE,
    };
    use insight_platform_rpc_trace::{request_with_trace, RpcTraceContext};
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose, SanType,
    };
    use tonic::transport::{
        server::TcpIncoming, Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server,
        ServerTlsConfig,
    };

    #[derive(Default)]
    struct RecordingDependencyObserver {
        outcomes: std::sync::Mutex<Vec<EgressRpcDependencyOutcome>>,
    }

    impl EgressRpcDependencyObserver for RecordingDependencyObserver {
        fn observe(&self, outcome: EgressRpcDependencyOutcome) {
            self.outcomes.lock().unwrap().push(outcome);
        }
    }

    impl RecordingDependencyObserver {
        fn outcomes(&self) -> Vec<EgressRpcDependencyOutcome> {
            self.outcomes.lock().unwrap().clone()
        }
    }

    fn traced_request<T>(message: T) -> Request<T> {
        request_with_trace(
            message,
            RpcTraceContext::start(TraceIdentityV1::generate(), TraceFlags::NotSampled).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn envelope_binds_operation_metadata_and_raw_payload() {
        let limits = EgressInternalRpcLimits::new(4_096, 4_096).unwrap();
        let envelope = encode_metadata_payload(
            &serde_json::json!({"schema_version": 1, "value": "closed"}),
            vec![0, 1, 2, 255],
            ROUND_TRIP_CAPABILITY_HTTP,
            limits,
        )
        .unwrap();
        let (metadata, payload): (serde_json::Value, Vec<u8>) =
            decode_metadata_payload(envelope.clone(), ROUND_TRIP_CAPABILITY_HTTP, limits).unwrap();
        assert_eq!(metadata["value"], "closed");
        assert_eq!(payload, vec![0, 1, 2, 255]);

        let mut tampered = envelope.clone();
        tampered.payload[0] = 9;
        assert!(decode_metadata_payload::<serde_json::Value>(
            tampered,
            ROUND_TRIP_CAPABILITY_HTTP,
            limits
        )
        .is_err());
        assert!(decode_metadata_payload::<serde_json::Value>(
            envelope,
            UNARY_CAPABILITY_GRPC,
            limits
        )
        .is_err());
    }

    #[test]
    fn discovery_response_keeps_descriptor_bytes_in_the_raw_payload_lane() {
        let limits = EgressInternalRpcLimits::new(4_096, 4_096).unwrap();
        let descriptor_bytes =
            br#"{"prompts":[],"resources":[],"schema_version":1,"tools":[]}"#.to_vec();
        let response = McpDiscoveryTransportResponse {
            schema_version: 1,
            request_digest: digest('1'),
            negotiated_version: "2025-11-25".to_owned(),
            negotiated_capabilities: insight_platform_contracts::McpNegotiatedCapabilities {
                tools: false,
                resources: false,
                prompts: false,
                logging: false,
                tasks: false,
                tasks_list: false,
                tasks_cancel: false,
                tasks_tools_call: false,
                elicitation: false,
                sampling: false,
                roots: false,
                subscriptions: false,
            },
            descriptor_digest: raw_digest(&descriptor_bytes).unwrap(),
            descriptor_count: 0,
            descriptor_bytes: descriptor_bytes.clone(),
            observed_at: Utc::now(),
        };
        let envelope =
            encode_mcp_discovery_outcome(UnaryOutcome::Succeeded(response.clone()), limits)
                .unwrap();
        assert_eq!(envelope.payload, descriptor_bytes);
        let metadata: serde_json::Value =
            serde_json::from_slice(&envelope.canonical_metadata_json).unwrap();
        assert_eq!(
            metadata
                .pointer("/value/descriptor_bytes")
                .and_then(serde_json::Value::as_str),
            Some("")
        );
        let UnaryOutcome::Succeeded(decoded) =
            decode_mcp_discovery_outcome(envelope, limits).unwrap()
        else {
            panic!("discovery response unexpectedly decoded as a failure")
        };
        assert_eq!(decoded, response);
    }

    #[test]
    fn worker_rejects_malformed_egress_failure_frames() {
        let limits = EgressInternalRpcLimits::new(4_096, 4_096).unwrap();
        let malformed_capability = CapabilityAdapterFailure {
            class: CapabilityAdapterFailureClass::Uncertain,
            safe_code: "malformed_failure".to_owned(),
            safe_message: "Missing required external identity".to_owned(),
            evidence_digest: digest('a'),
            external_identity_digest: None,
        };
        let envelope =
            encode_http_outcome(UnaryOutcome::Failed(malformed_capability), limits).unwrap();
        let decoded = decode_http_outcome(envelope, limits).unwrap_err();
        assert_eq!(decoded.safe_code, "capability_egress_rpc_response_invalid");

        let deadline = Utc::now() + Duration::minutes(1);
        let malformed_model = ModelAdapterFailure {
            class: ModelAdapterFailureClass::RetryableAfterDispatch,
            safe_code: "malformed_failure".to_owned(),
            safe_message: "Missing dispatch evidence".to_owned(),
            evidence_digest: digest('b'),
            request_sent: false,
            retry_at: Some(Utc::now() + Duration::seconds(1)),
        };
        assert!(malformed_model
            .validate_wire_shape(deadline, Utc::now())
            .is_err());
    }

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn capability_cancel_request() -> CapabilityTransportCancelRequest {
        CapabilityTransportCancelRequest {
            identity: CapabilityTransportRequestIdentity {
                backend_kind: CapabilityBackendKind::Http,
                tenant_id: id(ResourceKind::Tenant),
                invocation_id: id(ResourceKind::CapabilityInvocation),
                job_id: id(ResourceKind::Job),
                worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration),
                capability_deployment_id: id(ResourceKind::CapabilityDeployment),
                capability_deployment_digest: digest('a'),
                physical_attempt: 1,
                lease_generation: 1,
            },
            deadline: Utc::now() + Duration::minutes(1),
        }
    }

    fn exact_version(kind: ResourceKind, character: char) -> ExactVersionRef {
        ExactVersionRef::new(id(kind), digest(character)).unwrap()
    }

    fn subscription_request() -> McpStreamableHttpSubscriptionRequest {
        let endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: "mcp.example.com".to_owned(),
            port: 443,
            base_path: "/mcp".to_owned(),
        };
        McpStreamableHttpSubscriptionRequest {
            tenant_id: id(ResourceKind::Tenant),
            deployment: ExactDeploymentRef::new(id(ResourceKind::McpDeployment), digest('a'))
                .unwrap(),
            endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
            endpoint,
            server_identity_digest: digest('b'),
            protocol_policy: exact_version(ResourceKind::PolicyRevision, 'c'),
            network_policy: exact_version(ResourceKind::PolicyRevision, 'd'),
            tls_policy: exact_version(ResourceKind::PolicyRevision, 'e'),
            trust_policy: exact_version(ResourceKind::PolicyRevision, 'f'),
            auth_policy: Some(exact_version(ResourceKind::PolicyRevision, '1')),
            authorization_binding_id: id(ResourceKind::McpAuthorizationBinding),
            authorization_generation: 2,
            principal_binding_generation: 3,
            token_secret_binding: ExactSecretBindingRef::build(
                id(ResourceKind::SecretBinding),
                4,
                id(ResourceKind::SecretProvider),
                "mcp_access_token".parse().unwrap(),
                SecretResolutionPolicy::Pinned {
                    opaque_version_identity_digest: digest('2'),
                },
            )
            .unwrap(),
            protocol_version: MCP_PROTOCOL_BASELINE.to_owned(),
            client_capabilities: McpClientCapabilities {
                elicitation_form: false,
                elicitation_url: false,
                tasks_elicitation_create: false,
                sampling: false,
                roots: false,
            },
            negotiated_capabilities: McpNegotiatedCapabilities {
                tools: false,
                resources: true,
                prompts: false,
                logging: false,
                tasks: false,
                tasks_list: false,
                tasks_cancel: false,
                tasks_tools_call: false,
                elicitation: false,
                sampling: false,
                roots: false,
                subscriptions: true,
            },
            subscription_id: id(ResourceKind::McpOperation),
            binding_digest: digest('3'),
            session_generation: 5,
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration),
            resource_uri: "mcp://catalog.example/items/42".to_owned(),
            resource_uri_digest: digest('4'),
            deadline: Utc::now() + Duration::minutes(5),
            maximum_message_bytes: 16_384,
            maximum_response_bytes: 16_384,
            maximum_headers: 32,
            maximum_sse_event_bytes: 8_192,
            idle_timeout_milliseconds: 2_000,
            initialize_timeout_milliseconds: 2_000,
            request_timeout_milliseconds: 5_000,
            maximum_session_milliseconds: 300_000,
        }
    }

    struct FixtureSubscriptionConnector {
        sink: Arc<dyn McpStreamableHttpSubscriptionSink>,
    }

    struct FixtureSubscriptionActivation {
        sink: Arc<dyn McpStreamableHttpSubscriptionSink>,
        request: McpStreamableHttpSubscriptionRequest,
    }

    #[async_trait]
    impl McpSubscriptionActivation for FixtureSubscriptionActivation {
        async fn activate(self: Box<Self>) {
            self.sink
                .ingest_notification(McpStreamableHttpSubscriptionNotification {
                    tenant_id: self.request.tenant_id.clone(),
                    subscription_id: self.request.subscription_id.clone(),
                    authorization_generation: self.request.authorization_generation,
                    session_generation: self.request.session_generation,
                    event_generation: 1,
                    event_key_digest: digest('5'),
                    wire: SensitiveMcpNotificationWire::new(
                        br#"{"jsonrpc":"2.0","method":"notifications/resources/list_changed"}"#
                            .to_vec(),
                    )
                    .unwrap(),
                    received_at: Utc::now(),
                })
                .await
                .unwrap();
            self.sink
                .report_termination(McpStreamableHttpSubscriptionTermination {
                    tenant_id: self.request.tenant_id.clone(),
                    subscription_id: self.request.subscription_id.clone(),
                    authorization_generation: self.request.authorization_generation,
                    session_generation: self.request.session_generation,
                    worker_process_generation_id: self.request.worker_process_generation_id.clone(),
                    observed_at: Utc::now(),
                    failure: mcp_rpc_retryable("fixture_subscription_closed"),
                })
                .await
                .unwrap();
        }
    }

    #[async_trait]
    impl McpStreamableHttpSubscriptionConnector for FixtureSubscriptionConnector {
        async fn establish_subscription(
            &self,
            request: McpStreamableHttpSubscriptionRequest,
        ) -> Result<PreparedMcpSubscription, McpTransportFailure> {
            let established = insight_platform_mcp_host::EstablishedMcpSubscription {
                transport_kind: insight_platform_contracts::McpTransportKind::StreamableHttp,
                binding_digest: request.binding_digest.clone(),
                encrypted_opaque_session: insight_platform_mcp_host::EncryptedMcpState {
                    scheme: "aes-256-gcm".to_owned(),
                    ciphertext: vec![7; 32],
                    key_id: "fixture-key".to_owned(),
                    key_reference_digest: digest('6'),
                    plaintext_digest: digest('7'),
                },
                established_at: Utc::now(),
                expires_at: request.deadline,
                evidence_digest: digest('8'),
            };
            Ok(PreparedMcpSubscription::new(
                established,
                Box::new(FixtureSubscriptionActivation {
                    sink: Arc::clone(&self.sink),
                    request,
                }),
            ))
        }
    }

    #[tokio::test]
    async fn subscription_bridge_emits_nothing_until_exact_activation_then_streams_terminally() {
        let rpc_limits = EgressInternalRpcLimits::new(65_536, 1_048_576).unwrap();
        let bridge = Arc::new(
            EgressMcpSubscriptionBridge::new(
                rpc_limits,
                EgressMcpSubscriptionBridgeLimits {
                    maximum_pending: 2,
                    maximum_active: 2,
                    event_buffer_capacity: 2,
                },
            )
            .unwrap(),
        );
        let connector = FixtureSubscriptionConnector {
            sink: bridge.sink(),
        };
        assert_eq!(bridge.capacity_snapshot().pending_available, 2);
        assert_eq!(bridge.capacity_snapshot().active_available, 2);
        let request = subscription_request();
        let (prepared, pending) = bridge.establish(&connector, request.clone()).await.unwrap();
        assert_eq!(bridge.capacity_snapshot().pending_available, 1);
        assert_eq!(bridge.capacity_snapshot().active_available, 1);
        assert!(bridge.active.lock().await.is_empty());

        let (sender, mut receiver) = mpsc::channel(2);
        bridge
            .activate(
                pending,
                ActivateMcpSubscriptionWire {
                    schema_version: 1,
                    request_digest: prepared.request_digest,
                    tenant_id: request.tenant_id,
                    subscription_id: request.subscription_id,
                    authorization_generation: request.authorization_generation,
                    session_generation: request.session_generation,
                },
                sender,
            )
            .await
            .unwrap();
        let notification = receiver.recv().await.unwrap();
        assert_eq!(
            notification.operation,
            MCP_STREAMABLE_HTTP_SUBSCRIPTION_NOTIFICATION
        );
        let termination = receiver.recv().await.unwrap();
        assert_eq!(
            termination.operation,
            MCP_STREAMABLE_HTTP_SUBSCRIPTION_TERMINATION
        );
        assert!(receiver.recv().await.is_none());
        assert!(bridge.active.lock().await.is_empty());
    }

    struct FixtureModel;

    #[async_trait]
    impl ModelProviderWireConnector for FixtureModel {
        async fn open(
            &self,
            _request: ModelProviderWireRequest,
        ) -> Result<ModelProviderWireStream, ModelAdapterFailure> {
            Ok(Box::pin(stream::empty()))
        }

        async fn cancel(
            &self,
            _protocol: ModelProviderWireProtocol,
            _request: ModelAdapterCancelRequest,
        ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
            Ok(ModelAdapterCancelOutcome::Unsupported)
        }
    }

    struct FixtureHttp;

    #[async_trait]
    impl HttpNetworkTransport for FixtureHttp {
        async fn round_trip(
            &self,
            _request: HttpTransportRequest,
        ) -> Result<HttpTransportResponse, CapabilityAdapterFailure> {
            unreachable!("fixture only exercises cancel authorization")
        }

        async fn cancel(
            &self,
            _request: CapabilityTransportCancelRequest,
        ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
            insight_platform_rpc_trace::current_trace()
                .expect("Egress handler installs the received trace for dependencies");
            Ok(CapabilityTransportCancelOutcome::Accepted)
        }
    }

    struct FixtureGrpc;

    #[async_trait]
    impl GrpcNetworkTransport for FixtureGrpc {
        async fn unary(
            &self,
            _request: GrpcTransportRequest,
        ) -> Result<GrpcTransportResponse, CapabilityAdapterFailure> {
            unreachable!("fixture only exercises HTTP cancel authorization")
        }

        async fn cancel(
            &self,
            _request: CapabilityTransportCancelRequest,
        ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
            Ok(CapabilityTransportCancelOutcome::Accepted)
        }
    }

    struct FixtureRemoteContext;

    #[async_trait]
    impl RemoteContextSearchConnector for FixtureRemoteContext {
        async fn query(
            &self,
            _request: RemoteContextSearchRequest,
        ) -> Result<RemoteContextSearchResponse, RemoteContextFailure> {
            unreachable!("role gate fixture does not dispatch Remote Context")
        }
    }

    struct FixtureOAuth;

    #[async_trait]
    impl McpOAuthCredentialBroker for FixtureOAuth {
        async fn exchange_authorization_code(
            &self,
            _contract: &McpOAuthExchangeContract,
            _authorization_code: SensitiveOAuthValue,
            _now: DateTime<Utc>,
        ) -> Result<McpOAuthAuthorizedGrant, McpOAuthCredentialBrokerError> {
            Err(McpOAuthCredentialBrokerError::Rejected)
        }
    }

    struct FixturePkceCleaner;

    #[async_trait]
    impl McpOAuthPkceSecretCleaner for FixturePkceCleaner {
        async fn delete_exact(
            &self,
            _authorization: &AuthorizedMcpOAuthPkceCleanup,
        ) -> Result<McpOAuthPkceSecretCleanupDisposition, McpOAuthPkceSecretCleanupError> {
            Ok(McpOAuthPkceSecretCleanupDisposition::Deleted)
        }
    }

    struct MtlsFixture {
        ca_pem: String,
        server_certificate_pem: String,
        server_key_pem: String,
        capability_certificate_pem: String,
        capability_key_pem: String,
        model_certificate_pem: String,
        model_key_pem: String,
        mcp_certificate_pem: String,
        mcp_key_pem: String,
        discovery_certificate_pem: String,
        discovery_key_pem: String,
        subscription_certificate_pem: String,
        subscription_key_pem: String,
        cleanup_certificate_pem: String,
        cleanup_key_pem: String,
        callback_certificate_pem: String,
        callback_key_pem: String,
        context_certificate_pem: String,
        context_key_pem: String,
        unknown_certificate_pem: String,
        unknown_key_pem: String,
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
            vec![SanType::DnsName("egress.test".try_into().unwrap())],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let client = |uri: &str| {
            issue(
                vec![SanType::URI(uri.try_into().unwrap())],
                ExtendedKeyUsagePurpose::ClientAuth,
            )
        };
        let (capability_certificate_pem, capability_key_pem) =
            client(CAPABILITY_WORKER_WORKLOAD_IDENTITY);
        let (model_certificate_pem, model_key_pem) = client(MODEL_WORKER_WORKLOAD_IDENTITY);
        let (mcp_certificate_pem, mcp_key_pem) = client(MCP_HOST_WORKLOAD_IDENTITY);
        let (discovery_certificate_pem, discovery_key_pem) =
            client(MCP_DISCOVERY_WORKER_WORKLOAD_IDENTITY);
        let (subscription_certificate_pem, subscription_key_pem) =
            client(MCP_SUBSCRIPTION_WORKER_WORKLOAD_IDENTITY);
        let (cleanup_certificate_pem, cleanup_key_pem) =
            client(MCP_CLEANUP_WORKER_WORKLOAD_IDENTITY);
        let (callback_certificate_pem, callback_key_pem) = client(MCP_CALLBACK_WORKLOAD_IDENTITY);
        let (context_certificate_pem, context_key_pem) = client(CONTEXT_WORKER_WORKLOAD_IDENTITY);
        let (unknown_certificate_pem, unknown_key_pem) =
            client("spiffe://insight.platform/workload/api");
        MtlsFixture {
            ca_pem: ca.pem(),
            server_certificate_pem,
            server_key_pem,
            capability_certificate_pem,
            capability_key_pem,
            model_certificate_pem,
            model_key_pem,
            mcp_certificate_pem,
            mcp_key_pem,
            discovery_certificate_pem,
            discovery_key_pem,
            subscription_certificate_pem,
            subscription_key_pem,
            cleanup_certificate_pem,
            cleanup_key_pem,
            callback_certificate_pem,
            callback_key_pem,
            context_certificate_pem,
            context_key_pem,
            unknown_certificate_pem,
            unknown_key_pem,
        }
    }

    async fn connect_channel(
        address: std::net::SocketAddr,
        fixture: &MtlsFixture,
        certificate: &str,
        key: &str,
    ) -> Channel {
        let endpoint = Endpoint::from_shared(format!("https://{address}"))
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .domain_name("egress.test")
                    .ca_certificate(Certificate::from_pem(fixture.ca_pem.clone()))
                    .identity(Identity::from_pem(certificate, key)),
            )
            .unwrap();
        endpoint.connect().await.unwrap()
    }

    async fn connect(
        address: std::net::SocketAddr,
        fixture: &MtlsFixture,
        certificate: &str,
        key: &str,
    ) -> EgressBrokerServiceClient<Channel> {
        EgressBrokerServiceClient::new(connect_channel(address, fixture, certificate, key).await)
    }

    #[tokio::test]
    async fn client_observes_an_actual_transport_failure_without_request_metadata() {
        let observer = Arc::new(RecordingDependencyObserver::default());
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let client = EgressBrokerGrpcClient::new_with_observer(
            channel,
            EgressInternalRpcLimits::new(65_536, 1_048_576).unwrap(),
            observer.clone(),
        );

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            HttpNetworkTransport::cancel(&client, capability_cancel_request()),
        )
        .await
        .unwrap();

        assert!(result.is_err());
        assert_eq!(
            observer.outcomes(),
            vec![EgressRpcDependencyOutcome::Failure]
        );
    }

    #[tokio::test]
    async fn real_mtls_and_method_role_gate_reject_confused_deputies() {
        let fixture = mtls_fixture();
        let limits = EgressInternalRpcLimits::new(65_536, 1_048_576).unwrap();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let service = EgressBrokerServiceServer::new(
            EgressBrokerGrpcService::new(
                Arc::new(FixtureModel),
                Arc::new(FixtureHttp),
                Arc::new(FixtureGrpc),
                limits,
            )
            .with_mcp_oauth(Arc::new(FixtureOAuth), Arc::new(FixturePkceCleaner))
            .with_remote_context(Arc::new(FixtureRemoteContext)),
        );
        let service = tonic::service::interceptor::InterceptedService::new(
            service,
            EgressCallerWorkloadIdentity,
        );
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .identity(Identity::from_pem(
                            &fixture.server_certificate_pem,
                            &fixture.server_key_pem,
                        ))
                        .client_ca_root(Certificate::from_pem(fixture.ca_pem.clone())),
                )
                .unwrap()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_receiver.await;
                }),
        );

        let request = capability_cancel_request();
        let envelope = encode_metadata(&request, CANCEL_CAPABILITY_HTTP, limits).unwrap();
        let mut capability = connect(
            address,
            &fixture,
            &fixture.capability_certificate_pem,
            &fixture.capability_key_pem,
        )
        .await;
        assert_eq!(
            capability
                .cancel_capability_http(Request::new(envelope.clone()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        let outcome: UnaryOutcome<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> =
            decode_metadata(
                capability
                    .cancel_capability_http(traced_request(envelope.clone()))
                    .await
                    .unwrap()
                    .into_inner(),
                CAPABILITY_HTTP_CANCEL_OUTCOME,
                limits,
            )
            .unwrap();
        assert!(matches!(
            outcome,
            UnaryOutcome::Succeeded(CapabilityTransportCancelOutcome::Accepted)
        ));

        let observer = Arc::new(RecordingDependencyObserver::default());
        let observed_capability = EgressBrokerGrpcClient::new_with_observer(
            connect_channel(
                address,
                &fixture,
                &fixture.capability_certificate_pem,
                &fixture.capability_key_pem,
            )
            .await,
            limits,
            observer.clone(),
        );
        assert_eq!(
            scope_trace(
                RpcTraceContext::start(TraceIdentityV1::generate(), TraceFlags::NotSampled)
                    .unwrap(),
                HttpNetworkTransport::cancel(&observed_capability, capability_cancel_request()),
            )
            .await
            .unwrap(),
            CapabilityTransportCancelOutcome::Accepted
        );
        assert_eq!(
            observer.outcomes(),
            vec![EgressRpcDependencyOutcome::Success]
        );

        let remote_envelope = encode_metadata(
            &serde_json::json!({"schema_version": 1}),
            QUERY_REMOTE_CONTEXT,
            limits,
        )
        .unwrap();
        assert_eq!(
            capability
                .query_remote_context(traced_request(remote_envelope.clone()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let mut context = connect(
            address,
            &fixture,
            &fixture.context_certificate_pem,
            &fixture.context_key_pem,
        )
        .await;
        assert_eq!(
            context
                .query_remote_context(traced_request(remote_envelope))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );

        let mut model = connect(
            address,
            &fixture,
            &fixture.model_certificate_pem,
            &fixture.model_key_pem,
        )
        .await;
        assert_eq!(
            model
                .cancel_capability_http(traced_request(envelope.clone()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        let oauth_authorization = AuthorizedMcpOAuthPkceCleanup {
            tenant_id: id(ResourceKind::Tenant),
            task_id: id(ResourceKind::Interaction),
            secret_binding: ExactSecretBindingRef::build(
                id(ResourceKind::SecretBinding),
                1,
                id(ResourceKind::SecretProvider),
                MCP_OAUTH_PKCE_SECRET_PURPOSE
                    .parse::<SecretPurpose>()
                    .unwrap(),
                SecretResolutionPolicy::Pinned {
                    opaque_version_identity_digest: digest('f'),
                },
            )
            .unwrap(),
        };
        let oauth_envelope =
            encode_metadata(&oauth_authorization, DELETE_MCP_OAUTH_PKCE_SECRET, limits).unwrap();
        assert_eq!(
            capability
                .delete_mcp_o_auth_pkce_secret(traced_request(oauth_envelope.clone()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let mut mcp = connect(
            address,
            &fixture,
            &fixture.mcp_certificate_pem,
            &fixture.mcp_key_pem,
        )
        .await;
        assert_eq!(
            mcp.delete_mcp_o_auth_pkce_secret(traced_request(oauth_envelope.clone()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let mut cleanup = connect(
            address,
            &fixture,
            &fixture.cleanup_certificate_pem,
            &fixture.cleanup_key_pem,
        )
        .await;
        let outcome: UnaryOutcome<
            McpOAuthPkceSecretCleanupDisposition,
            McpOAuthPkceCleanupFailureWire,
        > = decode_metadata(
            cleanup
                .delete_mcp_o_auth_pkce_secret(traced_request(oauth_envelope))
                .await
                .unwrap()
                .into_inner(),
            MCP_OAUTH_PKCE_SECRET_DELETE_OUTCOME,
            limits,
        )
        .unwrap();
        assert!(matches!(
            outcome,
            UnaryOutcome::Succeeded(McpOAuthPkceSecretCleanupDisposition::Deleted)
        ));

        let oauth_exchange_envelope = encode_metadata(
            &serde_json::json!({"schema_version": 1}),
            EXCHANGE_MCP_OAUTH_AUTHORIZATION_CODE,
            limits,
        )
        .unwrap();
        assert_eq!(
            mcp.exchange_mcp_o_auth_authorization_code(traced_request(
                oauth_exchange_envelope.clone()
            ))
            .await
            .unwrap_err()
            .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            cleanup
                .exchange_mcp_o_auth_authorization_code(traced_request(
                    oauth_exchange_envelope.clone()
                ))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let mut callback = connect(
            address,
            &fixture,
            &fixture.callback_certificate_pem,
            &fixture.callback_key_pem,
        )
        .await;
        assert_eq!(
            callback
                .exchange_mcp_o_auth_authorization_code(traced_request(oauth_exchange_envelope))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );

        let mcp_operation_envelope = encode_metadata(
            &serde_json::json!({"schema_version": 1}),
            EXECUTE_MCP_STREAMABLE_HTTP,
            limits,
        )
        .unwrap();
        assert_eq!(
            capability
                .execute_mcp_streamable_http(traced_request(mcp_operation_envelope.clone()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            cleanup
                .execute_mcp_streamable_http(traced_request(mcp_operation_envelope.clone()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            mcp.execute_mcp_streamable_http(traced_request(mcp_operation_envelope))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );

        let discovery_envelope = encode_metadata(
            &serde_json::json!({"schema_version": 1}),
            DISCOVER_MCP_STREAMABLE_HTTP,
            limits,
        )
        .unwrap();
        assert_eq!(
            mcp.discover_mcp_streamable_http(traced_request(discovery_envelope.clone()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let mut discovery = connect(
            address,
            &fixture,
            &fixture.discovery_certificate_pem,
            &fixture.discovery_key_pem,
        )
        .await;
        assert_eq!(
            discovery
                .discover_mcp_streamable_http(traced_request(discovery_envelope))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            discovery
                .execute_mcp_streamable_http(traced_request(
                    encode_metadata(
                        &serde_json::json!({"schema_version": 1}),
                        EXECUTE_MCP_STREAMABLE_HTTP,
                        limits,
                    )
                    .unwrap()
                ))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        let mcp_cancel_envelope = encode_metadata(
            &serde_json::json!({"schema_version": 1}),
            CANCEL_MCP_REMOTE_TASK,
            limits,
        )
        .unwrap();
        assert_eq!(
            capability
                .cancel_mcp_remote_task(traced_request(mcp_cancel_envelope.clone()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            mcp.cancel_mcp_remote_task(traced_request(mcp_cancel_envelope))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );

        let subscription_envelope = encode_metadata(
            &serde_json::json!({"schema_version": 1}),
            ESTABLISH_MCP_STREAMABLE_HTTP_SUBSCRIPTION,
            limits,
        )
        .unwrap();
        assert_eq!(
            capability
                .stream_mcp_streamable_http_subscription(traced_request(stream::iter(vec![
                    subscription_envelope.clone(),
                ])))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            mcp.stream_mcp_streamable_http_subscription(traced_request(stream::iter(vec![
                subscription_envelope.clone(),
            ])))
            .await
            .unwrap_err()
            .code(),
            tonic::Code::PermissionDenied
        );
        let mut subscription = connect(
            address,
            &fixture,
            &fixture.subscription_certificate_pem,
            &fixture.subscription_key_pem,
        )
        .await;
        assert_eq!(
            subscription
                .stream_mcp_streamable_http_subscription(traced_request(stream::iter(vec![
                    subscription_envelope,
                ])))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );

        let mut unknown = connect(
            address,
            &fixture,
            &fixture.unknown_certificate_pem,
            &fixture.unknown_key_pem,
        )
        .await;
        assert_eq!(
            unknown
                .cancel_capability_http(traced_request(envelope))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        drop(capability);
        drop(model);
        drop(mcp);
        drop(discovery);
        drop(callback);
        drop(unknown);
        let _ = shutdown_sender.send(());
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
