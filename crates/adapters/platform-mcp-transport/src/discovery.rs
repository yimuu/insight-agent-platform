use async_trait::async_trait;
use chrono::Utc;
use insight_platform_contracts::{McpTransportBinding, McpTransportKind};
use std::sync::Arc;

use insight_platform_mcp_host::*;

pub struct StreamableHttpMcpDiscoveryTransport {
    connector: Arc<dyn McpDiscoveryTransportConnector>,
}

impl StreamableHttpMcpDiscoveryTransport {
    pub fn new(connector: Arc<dyn McpDiscoveryTransportConnector>) -> Self {
        Self { connector }
    }
}

#[async_trait]
impl McpDiscoveryTransport for StreamableHttpMcpDiscoveryTransport {
    fn kind(&self) -> McpTransportKind {
        McpTransportKind::StreamableHttp
    }

    async fn discover(
        &self,
        contract: &McpDiscoveryExecutionContract,
        request: &McpDiscoveryRequest,
    ) -> Result<McpDiscoveryCandidate, McpTransportFailure> {
        let transport_request = discovery_transport_request(contract, request)?;
        let response = self.connector.discover(transport_request.clone()).await?;
        response.validate_for(&transport_request).map_err(|_| {
            McpTransportFailure::Permanent(SafeMcpFailure {
                safe_code: "mcp_discovery_egress_response_invalid".to_owned(),
                safe_message: "MCP discovery response failed boundary validation".to_owned(),
                evidence_digest: static_digest("mcp_discovery_egress_response_invalid"),
            })
        })?;
        McpDiscoveryCandidate::build(
            response.negotiated_version,
            response.negotiated_capabilities,
            response.descriptor_bytes,
            response.descriptor_count,
            response.observed_at,
            request.deadline.min(contract.authorization.expires_at),
            contract,
        )
        .map_err(|_| {
            McpTransportFailure::Permanent(SafeMcpFailure {
                safe_code: "mcp_discovery_egress_response_invalid".to_owned(),
                safe_message: "MCP discovery response failed boundary validation".to_owned(),
                evidence_digest: static_digest("mcp_discovery_egress_response_invalid"),
            })
        })
    }
}

fn discovery_transport_request(
    contract: &McpDiscoveryExecutionContract,
    request: &McpDiscoveryRequest,
) -> Result<McpDiscoveryTransportRequest, McpTransportFailure> {
    contract
        .validate_at(Utc::now())
        .map_err(|_| invalid_discovery_transport_contract())?;
    request
        .validate_for(contract, Utc::now())
        .map_err(|_| invalid_discovery_transport_contract())?;
    let McpTransportBinding::StreamableHttp {
        endpoint,
        endpoint_identity_digest,
        network_policy,
        tls_policy,
    } = &contract.deployment_closure.transport;
    let maximum_descriptor_bytes = u64::from(contract.server.limits.maximum_response_bytes);
    let mut transport = McpDiscoveryTransportRequest {
        schema_version: 1,
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
        offered_versions: contract.protocol_profile.offered_versions.to_vec(),
        client_capabilities: contract.protocol_profile.client_capabilities.clone(),
        operation_id: request.operation_id.clone(),
        job_id: request.job_id.clone(),
        worker_process_generation_id: request.worker_process_generation_id.clone(),
        lease_generation: request.lease_generation,
        physical_attempt: request.physical_attempt,
        request_digest: placeholder_digest().map_err(|_| invalid_discovery_transport_contract())?,
        deadline: request.deadline,
        maximum_descriptor_bytes,
        maximum_descriptor_count: MAX_MCP_DISCOVERY_DESCRIPTOR_COUNT,
        maximum_pages_per_kind: MAX_MCP_DISCOVERY_PAGES_PER_KIND,
        maximum_request_bytes: contract.server.limits.maximum_message_bytes,
        maximum_response_bytes: contract.server.limits.maximum_response_bytes,
        maximum_headers: contract.server.limits.maximum_headers,
        maximum_sse_event_bytes: contract.server.limits.maximum_sse_event_bytes,
        idle_timeout_milliseconds: contract.server.limits.idle_timeout_milliseconds,
        initialize_timeout_milliseconds: contract.server.limits.initialize_timeout_milliseconds,
        request_timeout_milliseconds: contract.server.limits.request_timeout_milliseconds,
    };
    transport.request_digest = digest_without_field(&transport, "request_digest")
        .map_err(|_| invalid_discovery_transport_contract())?;
    transport
        .validate_at(Utc::now())
        .map_err(|_| invalid_discovery_transport_contract())?;
    Ok(transport)
}

fn invalid_discovery_transport_contract() -> McpTransportFailure {
    McpTransportFailure::Permanent(SafeMcpFailure {
        safe_code: "mcp_discovery_transport_contract_invalid".to_owned(),
        safe_message: "MCP discovery transport contract is invalid".to_owned(),
        evidence_digest: static_digest("mcp_discovery_transport_contract_invalid"),
    })
}
