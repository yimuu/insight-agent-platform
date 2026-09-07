use async_trait::async_trait;
use chrono::Utc;
use insight_platform_context::{
    ContextSubscriptionExecutionError, ContextSubscriptionRefreshAttempt,
    ContextSubscriptionRefreshCause, ContextSubscriptionRefreshEvidence,
    ContextSubscriptionRefreshResponse, CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION,
    MAX_CONTEXT_SUBSCRIPTION_REFRESH_BYTES, MAX_CONTEXT_SUBSCRIPTION_REFRESH_ITEMS,
    MAX_CONTEXT_SUBSCRIPTION_REFRESH_RESOURCES,
};
use insight_platform_contracts::{McpTransportBinding, PublishedMcpMethod};
use std::sync::Arc;

use insight_platform_mcp_host::*;

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
    resolved.validate_for(Utc::now(), attempt)?;
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
