use super::{
    McpHostExecutionContract, McpHostTransport, McpOperationContinuation, McpOperationOutcome,
    McpOperationRequest, McpRemoteTaskCancelOutcome, McpResourceSubscriptionBinding,
    McpSubscriptionTransport, McpTransportFailure, PreparedMcpSubscription,
    SensitiveMcpNotificationWire,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    CanonicalHttpEndpoint, ClosedJsonValue, ExactDeploymentRef, ExactSecretBindingRef,
    ExactVersionRef, McpClientCapabilities, McpNegotiatedCapabilities, McpTransportBinding,
    McpTransportKind, PublishedMcpMethod, ResourceId, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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

pub struct StreamableHttpMcpTransport {
    connector: Arc<dyn McpStreamableHttpConnector>,
}

impl StreamableHttpMcpTransport {
    pub fn new(connector: Arc<dyn McpStreamableHttpConnector>) -> Self {
        Self { connector }
    }
}

#[async_trait]
impl McpHostTransport for StreamableHttpMcpTransport {
    fn kind(&self) -> McpTransportKind {
        McpTransportKind::StreamableHttp
    }

    async fn execute(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        self.connector
            .execute(streamable_http_request(
                contract,
                request,
                request.deadline,
            )?)
            .await
    }

    async fn cancel_remote_task(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
        deadline: DateTime<Utc>,
    ) -> Result<McpRemoteTaskCancelOutcome, McpTransportFailure> {
        self.connector
            .cancel_remote_task(streamable_http_request(contract, request, deadline)?)
            .await
    }
}

fn streamable_http_request(
    contract: &McpHostExecutionContract,
    request: &McpOperationRequest,
    transport_deadline: DateTime<Utc>,
) -> Result<McpStreamableHttpRequest, McpTransportFailure> {
    let McpTransportBinding::StreamableHttp {
        endpoint,
        endpoint_identity_digest,
        network_policy,
        tls_policy,
    } = &contract.deployment_closure.transport;
    let method_limits = contract
        .protocol_profile
        .method_limits
        .get(&request.method)
        .ok_or_else(contract_mismatch)?;
    let task_limits = remote_task_limits(contract, request)?;
    Ok(McpStreamableHttpRequest {
        tenant_id: request.tenant_id.clone(),
        deployment: contract.deployment.clone(),
        endpoint: endpoint.clone(),
        endpoint_identity_digest: endpoint_identity_digest.clone(),
        server_identity_digest: contract.deployment_closure.server_identity_digest.clone(),
        protocol_policy: contract.server.protocol_policy.clone(),
        network_policy: network_policy.clone(),
        tls_policy: tls_policy.clone(),
        trust_policy: contract.deployment_closure.trust_policy.clone(),
        auth_policy: contract.deployment_closure.auth_policy.clone(),
        authorization_binding_id: contract.authorization.authorization_binding_id.clone(),
        authorization_generation: contract.authorization.generation,
        principal_binding_generation: contract.authorization.principal_binding_generation,
        token_secret_binding: contract.authorization.token_secret_binding.clone(),
        protocol_version: contract.discovery.negotiated_version.clone(),
        client_capabilities: contract.protocol_profile.client_capabilities.clone(),
        negotiated_capabilities: contract.discovery.negotiated_capabilities.clone(),
        operation_id: request.mcp_operation_id.clone(),
        invocation_id: request.invocation_id.clone(),
        job_id: request.job_id.clone(),
        physical_attempt: request.physical_attempt,
        discovery_snapshot_id: contract.discovery.snapshot_id.clone(),
        discovery_snapshot_digest: contract.discovery.canonical_digest.clone(),
        method: request.method,
        params: request.params.clone(),
        task_requested: request.task_requested,
        continuation: request.continuation.clone(),
        task_limits,
        idempotency_key_digest: request.idempotency_key_digest.clone(),
        deadline: request.deadline,
        transport_deadline,
        maximum_request_bytes: method_limits
            .maximum_request_bytes
            .min(contract.server.limits.maximum_message_bytes),
        maximum_response_bytes: method_limits
            .maximum_response_bytes
            .min(contract.server.limits.maximum_response_bytes),
        maximum_headers: contract.server.limits.maximum_headers,
        maximum_sse_event_bytes: contract.server.limits.maximum_sse_event_bytes,
        maximum_progress_events: method_limits.maximum_progress_events,
        idle_timeout_milliseconds: contract.server.limits.idle_timeout_milliseconds,
        initialize_timeout_milliseconds: contract.server.limits.initialize_timeout_milliseconds,
        request_timeout_milliseconds: contract.server.limits.request_timeout_milliseconds,
    })
}

/// Broker request for managed stdio. It contains exact immutable package/runtime/policy identities
/// and protocol data only; there is no executable path, shell string, PID, namespace or host path.
#[cfg(any())]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpManagedRunnerRequest {
    pub tenant_id: ResourceId,
    pub deployment: ExactDeploymentRef,
    pub package: ExactVersionRef,
    pub runtime: ExactVersionRef,
    pub profile: ExactVersionRef,
    pub isolation: SandboxIsolationClass,
    pub isolation_policy: ExactVersionRef,
    pub resource_policy: ExactVersionRef,
    pub artifact_io_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub auth_policy: Option<ExactVersionRef>,
    pub authorization_binding_id: ResourceId,
    pub authorization_generation: u64,
    pub principal_binding_generation: u64,
    pub token_secret_binding: ExactSecretBindingRef,
    pub protocol_policy: ExactVersionRef,
    pub protocol_version: String,
    pub client_capabilities: McpClientCapabilities,
    pub negotiated_capabilities: McpNegotiatedCapabilities,
    pub operation_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub physical_attempt: u32,
    pub discovery_snapshot_id: ResourceId,
    pub discovery_snapshot_digest: Sha256Digest,
    pub method: PublishedMcpMethod,
    pub params: ClosedJsonValue,
    pub task_requested: bool,
    pub continuation: Option<McpOperationContinuation>,
    pub task_limits: Option<McpRemoteTaskLimits>,
    pub idempotency_key_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
    pub maximum_request_bytes: u32,
    pub maximum_response_bytes: u32,
    pub idle_timeout_milliseconds: u64,
    pub initialize_timeout_milliseconds: u64,
    pub request_timeout_milliseconds: u64,
}

#[cfg(any())]
impl McpManagedRunnerRequest {
    /// Builds the exact post-claim Runner request. The logical operation is frozen at Sandbox
    /// admission; WorkerProcessGeneration and lease generation come only from the Sandbox claim.
    pub fn from_operation(
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
    ) -> Result<Self, McpTransportFailure> {
        let now = Utc::now();
        contract
            .validate_canonical_at(now)
            .map_err(|_| contract_mismatch())?;
        request
            .validate_for(contract, now)
            .map_err(|_| contract_mismatch())?;
        let McpTransportBinding::ManagedStdio {
            package,
            runtime,
            profile,
            isolation,
            isolation_policy,
            resource_policy,
            artifact_io_policy,
        } = &contract.deployment_closure.transport
        else {
            return Err(contract_mismatch());
        };
        if *isolation != SandboxIsolationClass::MicroVm {
            return Err(contract_mismatch());
        }
        let method_limits = contract
            .protocol_profile
            .method_limits
            .get(&request.method)
            .ok_or_else(contract_mismatch)?;
        let task_limits = remote_task_limits(contract, request)?;
        Ok(Self {
            tenant_id: request.tenant_id.clone(),
            deployment: contract.deployment.clone(),
            package: package.clone(),
            runtime: runtime.clone(),
            profile: profile.clone(),
            isolation: *isolation,
            isolation_policy: isolation_policy.clone(),
            resource_policy: resource_policy.clone(),
            artifact_io_policy: artifact_io_policy.clone(),
            trust_policy: contract.deployment_closure.trust_policy.clone(),
            auth_policy: contract.deployment_closure.auth_policy.clone(),
            authorization_binding_id: contract.authorization.authorization_binding_id.clone(),
            authorization_generation: contract.authorization.generation,
            principal_binding_generation: contract.authorization.principal_binding_generation,
            token_secret_binding: contract.authorization.token_secret_binding.clone(),
            protocol_policy: contract.server.protocol_policy.clone(),
            protocol_version: contract.discovery.negotiated_version.clone(),
            client_capabilities: contract.protocol_profile.client_capabilities.clone(),
            negotiated_capabilities: contract.discovery.negotiated_capabilities.clone(),
            operation_id: request.mcp_operation_id.clone(),
            invocation_id: request.invocation_id.clone(),
            job_id: request.job_id.clone(),
            worker_process_generation_id: request.worker_process_generation_id.clone(),
            lease_generation: request.lease_generation,
            physical_attempt: request.physical_attempt,
            discovery_snapshot_id: contract.discovery.snapshot_id.clone(),
            discovery_snapshot_digest: contract.discovery.canonical_digest.clone(),
            method: request.method,
            params: request.params.clone(),
            task_requested: request.task_requested,
            continuation: request.continuation.clone(),
            task_limits,
            idempotency_key_digest: request.idempotency_key_digest.clone(),
            deadline: request.deadline,
            maximum_request_bytes: method_limits
                .maximum_request_bytes
                .min(contract.server.limits.maximum_message_bytes),
            maximum_response_bytes: method_limits
                .maximum_response_bytes
                .min(contract.server.limits.maximum_response_bytes),
            idle_timeout_milliseconds: contract.server.limits.idle_timeout_milliseconds,
            initialize_timeout_milliseconds: contract.server.limits.initialize_timeout_milliseconds,
            request_timeout_milliseconds: contract.server.limits.request_timeout_milliseconds,
        })
    }
}

#[cfg(any())]
#[async_trait]
pub trait ManagedMcpRunnerBroker: Send + Sync {
    async fn execute(
        &self,
        request: McpManagedRunnerRequest,
    ) -> Result<McpOperationOutcome, McpTransportFailure>;
}

#[cfg(any())]
pub struct ManagedStdioMcpTransport {
    broker: Arc<dyn ManagedMcpRunnerBroker>,
}

#[cfg(any())]
impl ManagedStdioMcpTransport {
    pub fn new(broker: Arc<dyn ManagedMcpRunnerBroker>) -> Self {
        Self { broker }
    }
}

#[cfg(any())]
#[async_trait]
impl McpHostTransport for ManagedStdioMcpTransport {
    fn kind(&self) -> McpTransportKind {
        McpTransportKind::ManagedStdio
    }

    async fn execute(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        self.broker
            .execute(McpManagedRunnerRequest::from_operation(contract, request)?)
            .await
    }
}

fn remote_task_limits(
    contract: &McpHostExecutionContract,
    request: &McpOperationRequest,
) -> Result<Option<McpRemoteTaskLimits>, McpTransportFailure> {
    if !request.task_requested && request.continuation.is_none() {
        return Ok(None);
    }
    let get = contract
        .protocol_profile
        .method_limits
        .get(&PublishedMcpMethod::TasksGet)
        .ok_or_else(contract_mismatch)?;
    let result = contract
        .protocol_profile
        .method_limits
        .get(&PublishedMcpMethod::TasksResult)
        .ok_or_else(contract_mismatch)?;
    let cancel = contract
        .discovery
        .negotiated_capabilities
        .tasks_cancel
        .then(|| {
            contract
                .protocol_profile
                .method_limits
                .get(&PublishedMcpMethod::TasksCancel)
                .ok_or_else(contract_mismatch)
        })
        .transpose()?;
    Ok(Some(McpRemoteTaskLimits {
        maximum_get_request_bytes: get
            .maximum_request_bytes
            .min(contract.server.limits.maximum_message_bytes),
        maximum_get_response_bytes: get
            .maximum_response_bytes
            .min(contract.server.limits.maximum_response_bytes),
        maximum_result_request_bytes: result
            .maximum_request_bytes
            .min(contract.server.limits.maximum_message_bytes),
        maximum_result_response_bytes: result
            .maximum_response_bytes
            .min(contract.server.limits.maximum_response_bytes),
        maximum_cancel_request_bytes: cancel.map(|limits| {
            limits
                .maximum_request_bytes
                .min(contract.server.limits.maximum_message_bytes)
        }),
        maximum_cancel_response_bytes: cancel.map(|limits| {
            limits
                .maximum_response_bytes
                .min(contract.server.limits.maximum_response_bytes)
        }),
        maximum_get_progress_events: get.maximum_progress_events,
        maximum_result_progress_events: result.maximum_progress_events,
        maximum_cancel_progress_events: cancel.map(|limits| limits.maximum_progress_events),
        minimum_poll_milliseconds: get.minimum_poll_milliseconds,
        maximum_poll_milliseconds: get.maximum_poll_milliseconds,
    }))
}

fn contract_mismatch() -> McpTransportFailure {
    McpTransportFailure::RejectedBeforeDispatch(super::SafeMcpFailure {
        safe_code: "mcp_transport_contract_mismatch".to_owned(),
        safe_message: "MCP transport does not match the exact Deployment".to_owned(),
        evidence_digest: super::static_digest("mcp_transport_contract_mismatch"),
    })
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

pub struct StreamableHttpMcpSubscriptionTransport {
    connector: Arc<dyn McpStreamableHttpSubscriptionConnector>,
}

impl StreamableHttpMcpSubscriptionTransport {
    pub fn new(connector: Arc<dyn McpStreamableHttpSubscriptionConnector>) -> Self {
        Self { connector }
    }
}

#[async_trait]
impl McpSubscriptionTransport for StreamableHttpMcpSubscriptionTransport {
    fn kind(&self) -> McpTransportKind {
        McpTransportKind::StreamableHttp
    }

    async fn establish(
        &self,
        contract: &McpHostExecutionContract,
        binding: &McpResourceSubscriptionBinding,
        session_generation: u64,
        worker_process_generation_id: &ResourceId,
        deadline: DateTime<Utc>,
    ) -> Result<PreparedMcpSubscription, McpTransportFailure> {
        let McpTransportBinding::StreamableHttp {
            endpoint,
            endpoint_identity_digest,
            network_policy,
            tls_policy,
        } = &contract.deployment_closure.transport;
        binding
            .validate_for_execution_contract_at(contract, Utc::now())
            .map_err(|_| contract_mismatch())?;
        self.connector
            .establish_subscription(McpStreamableHttpSubscriptionRequest {
                tenant_id: binding.tenant_id.clone(),
                deployment: contract.deployment.clone(),
                endpoint: endpoint.clone(),
                endpoint_identity_digest: endpoint_identity_digest.clone(),
                server_identity_digest: contract.deployment_closure.server_identity_digest.clone(),
                protocol_policy: contract.server.protocol_policy.clone(),
                network_policy: network_policy.clone(),
                tls_policy: tls_policy.clone(),
                trust_policy: contract.deployment_closure.trust_policy.clone(),
                auth_policy: contract.deployment_closure.auth_policy.clone(),
                authorization_binding_id: contract.authorization.authorization_binding_id.clone(),
                authorization_generation: contract.authorization.generation,
                principal_binding_generation: contract.authorization.principal_binding_generation,
                token_secret_binding: contract.authorization.token_secret_binding.clone(),
                protocol_version: contract.discovery.negotiated_version.clone(),
                client_capabilities: contract.protocol_profile.client_capabilities.clone(),
                negotiated_capabilities: contract.discovery.negotiated_capabilities.clone(),
                subscription_id: binding.subscription_id.clone(),
                binding_digest: binding.canonical_digest.clone(),
                session_generation,
                worker_process_generation_id: worker_process_generation_id.clone(),
                resource_uri: binding.resource_uri.clone(),
                resource_uri_digest: binding.resource_uri_digest.clone(),
                deadline,
                maximum_message_bytes: contract.server.limits.maximum_message_bytes,
                maximum_response_bytes: contract.server.limits.maximum_response_bytes,
                maximum_headers: contract.server.limits.maximum_headers,
                maximum_sse_event_bytes: contract.server.limits.maximum_sse_event_bytes,
                idle_timeout_milliseconds: contract.server.limits.idle_timeout_milliseconds,
                initialize_timeout_milliseconds: contract
                    .server
                    .limits
                    .initialize_timeout_milliseconds,
                request_timeout_milliseconds: contract.server.limits.request_timeout_milliseconds,
                maximum_session_milliseconds: contract.server.limits.maximum_session_milliseconds,
            })
            .await
    }
}
