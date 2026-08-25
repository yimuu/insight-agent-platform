use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_context::{
    ContextSubscriptionExecutionError, ContextSubscriptionRefreshAttempt,
    ContextSubscriptionRefreshBackend, ContextSubscriptionRefreshCause,
    ContextSubscriptionRefreshEvidence, ContextSubscriptionRefreshResponse,
    CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION, MAX_CONTEXT_SUBSCRIPTION_REFRESH_BYTES,
    MAX_CONTEXT_SUBSCRIPTION_REFRESH_ITEMS, MAX_CONTEXT_SUBSCRIPTION_REFRESH_RESOURCES,
};
use insight_platform_contracts::{
    CanonicalHttpEndpoint, ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef,
    McpClientCapabilities, McpMethodLimits, McpNegotiatedCapabilities, McpTransportBinding,
    PublishedMcpMethod, ResourceId, ResourceKind, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    McpHostExecutionContract, McpSubscriptionRecord, McpSubscriptionState, McpTransportFailure,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpResourceRefreshMethodLimits {
    pub maximum_request_bytes: u32,
    pub maximum_response_bytes: u32,
    pub maximum_pages: u16,
}

impl From<McpMethodLimits> for McpResourceRefreshMethodLimits {
    fn from(value: McpMethodLimits) -> Self {
        Self {
            maximum_request_bytes: value.maximum_request_bytes,
            maximum_response_bytes: value.maximum_response_bytes,
            maximum_pages: value.maximum_pages,
        }
    }
}

/// Credential-free Host→Egress request for a fenced Context refresh attempt. The Context caller
/// cannot choose a method, endpoint, header or credential; Host derives all of them from the
/// current subscription and exact published MCP execution closure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpResourceRefreshTransportRequest {
    pub schema_version: u32,
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
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub attempt_number: u32,
    pub discovery_snapshot_id: ResourceId,
    pub discovery_snapshot_digest: Sha256Digest,
    pub execution_identity_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub resource_uri: String,
    pub resource_uri_digest: Sha256Digest,
    pub cause: ContextSubscriptionRefreshCause,
    pub deadline: DateTime<Utc>,
    pub list_limits: Option<McpResourceRefreshMethodLimits>,
    pub read_limits: McpResourceRefreshMethodLimits,
    pub maximum_resources: u32,
    pub maximum_items: u32,
    pub maximum_total_bytes: u64,
    pub maximum_headers: u16,
    pub maximum_sse_event_bytes: u32,
    pub idle_timeout_milliseconds: u64,
    pub initialize_timeout_milliseconds: u64,
    pub request_timeout_milliseconds: u64,
}

impl McpResourceRefreshTransportRequest {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ContextSubscriptionExecutionError> {
        let policies = [
            &self.protocol_policy,
            &self.network_policy,
            &self.tls_policy,
            &self.trust_policy,
        ];
        let full_reconcile = matches!(
            self.cause,
            ContextSubscriptionRefreshCause::FullReconcile { .. }
        );
        if self.schema_version != CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.deployment.resource_kind != ResourceKind::McpDeployment
            || self.deployment.validate().is_err()
            || self.endpoint.validate().is_err()
            || self.endpoint.canonical_digest().as_ref() != Ok(&self.endpoint_identity_digest)
            || policies.iter().any(|policy| {
                policy.resource_kind != ResourceKind::PolicyRevision || policy.validate().is_err()
            })
            || self.auth_policy.as_ref().is_none_or(|policy| {
                policy.resource_kind != ResourceKind::PolicyRevision || policy.validate().is_err()
            })
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.authorization_generation == 0
            || self.principal_binding_generation == 0
            || self.token_secret_binding.validate().is_err()
            || !self.negotiated_capabilities.resources
            || self.subscription_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation == 0
            || self.attempt_number == 0
            || self.discovery_snapshot_id.kind() != ResourceKind::McpDiscoverySnapshot
            || self.resource_uri.is_empty()
            || self.resource_uri.len() > 8_192
            || self.resource_uri.chars().any(char::is_control)
            || self.deadline <= now
            || self.list_limits.is_some() != full_reconcile
            || !valid_refresh_limits(self.read_limits)
            || self
                .list_limits
                .is_some_and(|limits| !valid_refresh_limits(limits))
            || self.maximum_resources == 0
            || self.maximum_resources > MAX_CONTEXT_SUBSCRIPTION_REFRESH_RESOURCES
            || self.maximum_items == 0
            || self.maximum_items > MAX_CONTEXT_SUBSCRIPTION_REFRESH_ITEMS
            || self.maximum_total_bytes == 0
            || self.maximum_total_bytes > MAX_CONTEXT_SUBSCRIPTION_REFRESH_BYTES
            || self.maximum_headers == 0
            || self.maximum_sse_event_bytes == 0
            || self.idle_timeout_milliseconds == 0
            || self.initialize_timeout_milliseconds == 0
            || self.request_timeout_milliseconds == 0
        {
            return Err(ContextSubscriptionExecutionError::InvalidAttempt);
        }
        Ok(())
    }
}

fn valid_refresh_limits(limits: McpResourceRefreshMethodLimits) -> bool {
    limits.maximum_request_bytes > 0
        && limits.maximum_response_bytes > 0
        && limits.maximum_pages > 0
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpResourceRefreshTransportEvidence {
    pub schema_version: u32,
    pub execution_identity_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub response_digest: Sha256Digest,
    pub resource_set_digest: Sha256Digest,
    pub resource_count: u32,
    pub item_count: u32,
    pub byte_count: u64,
    pub remote_revision: Option<String>,
    pub cursor: Option<String>,
    pub observed_at: DateTime<Utc>,
}

#[async_trait]
pub trait McpResourceRefreshConnector: Send + Sync {
    async fn refresh_resources(
        &self,
        request: McpResourceRefreshTransportRequest,
    ) -> Result<McpResourceRefreshTransportEvidence, McpTransportFailure>;
}

pub struct StreamableHttpMcpResourceRefreshProtocol {
    connector: Arc<dyn McpResourceRefreshConnector>,
}

impl StreamableHttpMcpResourceRefreshProtocol {
    pub fn new(connector: Arc<dyn McpResourceRefreshConnector>) -> Self {
        Self { connector }
    }
}

#[async_trait]
impl McpResourceRefreshProtocol for StreamableHttpMcpResourceRefreshProtocol {
    async fn refresh_resources(
        &self,
        attempt: &ContextSubscriptionRefreshAttempt,
        resolved: &ResolvedContextSubscriptionRefresh,
    ) -> Result<ContextSubscriptionRefreshResponse, ContextSubscriptionExecutionError> {
        let request = resource_refresh_transport_request(attempt, resolved)?;
        let evidence = self
            .connector
            .refresh_resources(request)
            .await
            .map_err(map_transport_failure)?;
        let response = ContextSubscriptionRefreshResponse::Completed {
            evidence: ContextSubscriptionRefreshEvidence {
                schema_version: evidence.schema_version,
                execution_identity_digest: evidence.execution_identity_digest,
                request_digest: evidence.request_digest,
                response_digest: evidence.response_digest,
                resource_set_digest: evidence.resource_set_digest,
                resource_count: evidence.resource_count,
                item_count: evidence.item_count,
                byte_count: evidence.byte_count,
                remote_revision: evidence.remote_revision,
                cursor: evidence.cursor,
                observed_at: evidence.observed_at,
            },
        };
        response.validate_for(attempt, Utc::now())?;
        Ok(response)
    }
}

fn resource_refresh_transport_request(
    attempt: &ContextSubscriptionRefreshAttempt,
    resolved: &ResolvedContextSubscriptionRefresh,
) -> Result<McpResourceRefreshTransportRequest, ContextSubscriptionExecutionError> {
    resolved.validate_for(attempt)?;
    let contract = &resolved.contract;
    let McpTransportBinding::StreamableHttp {
        endpoint,
        endpoint_identity_digest,
        network_policy,
        tls_policy,
    } = &contract.deployment_closure.transport;
    let read_limits = contract
        .protocol_profile
        .method_limits
        .get(&PublishedMcpMethod::ResourcesRead)
        .copied()
        .ok_or(ContextSubscriptionExecutionError::Rejected)?;
    let list_limits = matches!(
        attempt.request.cause,
        ContextSubscriptionRefreshCause::FullReconcile { .. }
    )
    .then(|| {
        contract
            .protocol_profile
            .method_limits
            .get(&PublishedMcpMethod::ResourcesList)
            .copied()
            .ok_or(ContextSubscriptionExecutionError::Rejected)
    })
    .transpose()?;
    let request = McpResourceRefreshTransportRequest {
        schema_version: CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION,
        tenant_id: attempt.request.tenant_id.clone(),
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
        subscription_id: attempt.request.subscription_id.clone(),
        job_id: attempt.job_id.clone(),
        worker_process_generation_id: attempt.worker_process_generation_id.clone(),
        lease_generation: attempt.job_fence.lease_generation,
        attempt_number: attempt.attempt_number,
        discovery_snapshot_id: contract.discovery.snapshot_id.clone(),
        discovery_snapshot_digest: contract.discovery.canonical_digest.clone(),
        execution_identity_digest: attempt.execution_identity_digest()?,
        request_digest: attempt.request.request_digest.clone(),
        resource_uri: attempt.request.resource_uri.clone(),
        resource_uri_digest: attempt.request.resource_uri_digest.clone(),
        cause: attempt.request.cause.clone(),
        deadline: attempt.request.deadline,
        list_limits: list_limits.map(Into::into),
        read_limits: read_limits.into(),
        maximum_resources: MAX_CONTEXT_SUBSCRIPTION_REFRESH_RESOURCES,
        maximum_items: MAX_CONTEXT_SUBSCRIPTION_REFRESH_ITEMS,
        maximum_total_bytes: MAX_CONTEXT_SUBSCRIPTION_REFRESH_BYTES,
        maximum_headers: contract.server.limits.maximum_headers,
        maximum_sse_event_bytes: contract.server.limits.maximum_sse_event_bytes,
        idle_timeout_milliseconds: contract.server.limits.idle_timeout_milliseconds,
        initialize_timeout_milliseconds: contract.server.limits.initialize_timeout_milliseconds,
        request_timeout_milliseconds: contract.server.limits.request_timeout_milliseconds,
    };
    request.validate_at(Utc::now())?;
    Ok(request)
}

fn map_transport_failure(failure: McpTransportFailure) -> ContextSubscriptionExecutionError {
    match failure {
        McpTransportFailure::RetryableBeforeDispatch(_) => {
            ContextSubscriptionExecutionError::Unavailable
        }
        McpTransportFailure::PostDispatchUncertain { .. } => {
            ContextSubscriptionExecutionError::CompletionUncertain
        }
        McpTransportFailure::RejectedBeforeDispatch(_)
        | McpTransportFailure::Permanent(_)
        | McpTransportFailure::ReauthorizationRequired { .. } => {
            ContextSubscriptionExecutionError::Rejected
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedContextSubscriptionRefresh {
    pub subscription: McpSubscriptionRecord,
    pub contract: McpHostExecutionContract,
}

impl ResolvedContextSubscriptionRefresh {
    pub fn validate_for(
        &self,
        attempt: &ContextSubscriptionRefreshAttempt,
    ) -> Result<(), ContextSubscriptionExecutionError> {
        let now = Utc::now();
        attempt.validate_at(now)?;
        self.subscription
            .validate_at(now)
            .map_err(|_| ContextSubscriptionExecutionError::Rejected)?;
        self.contract
            .validate_canonical_at(now)
            .map_err(|_| ContextSubscriptionExecutionError::Rejected)?;
        let binding = &self.subscription.payload.binding;
        let request = &attempt.request;
        if self.subscription.tenant_id != request.tenant_id
            || self.subscription.subscription_id != request.subscription_id
            || self.subscription.state != McpSubscriptionState::Active
            || binding.context_deployment != request.context_deployment
            || binding.mcp_deployment != request.mcp_deployment
            || binding.discovery_snapshot_id != request.discovery_snapshot_id
            || binding.discovery_snapshot_digest != request.discovery_snapshot_digest
            || binding.authorization_generation != request.authorization_generation
            || self.subscription.payload.session.generation != request.session_generation
            || binding.resource_uri != request.resource_uri
            || binding.resource_uri_digest != request.resource_uri_digest
            || binding
                .validate_for_execution_contract_at(&self.contract, now)
                .is_err()
        {
            return Err(ContextSubscriptionExecutionError::Rejected);
        }
        Ok(())
    }
}

#[async_trait]
pub trait ContextSubscriptionRefreshResolver: Send + Sync {
    async fn resolve_context_subscription_refresh(
        &self,
        attempt: &ContextSubscriptionRefreshAttempt,
    ) -> Result<ResolvedContextSubscriptionRefresh, ContextSubscriptionExecutionError>;
}

#[async_trait]
pub trait McpResourceRefreshProtocol: Send + Sync {
    async fn refresh_resources(
        &self,
        attempt: &ContextSubscriptionRefreshAttempt,
        resolved: &ResolvedContextSubscriptionRefresh,
    ) -> Result<ContextSubscriptionRefreshResponse, ContextSubscriptionExecutionError>;
}

pub struct McpResourceRefreshHost<R, P> {
    resolver: Arc<R>,
    protocol: Arc<P>,
}

impl<R, P> McpResourceRefreshHost<R, P> {
    pub fn new(resolver: Arc<R>, protocol: Arc<P>) -> Self {
        Self { resolver, protocol }
    }
}

#[async_trait]
impl<R, P> ContextSubscriptionRefreshBackend for McpResourceRefreshHost<R, P>
where
    R: ContextSubscriptionRefreshResolver + 'static,
    P: McpResourceRefreshProtocol + 'static,
{
    async fn refresh_subscription_resources(
        &self,
        attempt: ContextSubscriptionRefreshAttempt,
    ) -> Result<ContextSubscriptionRefreshResponse, ContextSubscriptionExecutionError> {
        attempt.validate_at(Utc::now())?;
        let resolved = self
            .resolver
            .resolve_context_subscription_refresh(&attempt)
            .await?;
        resolved.validate_for(&attempt)?;
        let response = self.protocol.refresh_resources(&attempt, &resolved).await?;
        response.validate_for(&attempt, Utc::now())?;
        Ok(response)
    }
}
