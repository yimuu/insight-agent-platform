//! MCP operation ABI and transport semantics. Tenant policy never becomes a worker capability.
use crate::{
    McpDiscoveryAdmission, McpDiscoveryExecutionContract, McpHostError, McpHostExecutionContract,
    McpResourceSubscriptionBinding,
};
use insight_platform_contracts::{
    canonical_digest, DomainOperationRequirements, ExecutionRequirement, McpTransportKind,
    Sha256Digest, WorkerExecutionCapabilities, WorkerExecutionCapability,
    EXECUTION_REQUIREMENT_VERSION, MCP_PROTOCOL_BASELINE,
};
use serde_json::{json, Value};
pub const SUPPORTED_SEMANTIC_PROTOCOLS: &[&str] = &[MCP_PROTOCOL_BASELINE];
fn digest(value: Value) -> Sha256Digest {
    canonical_digest(&value)
        .expect("bounded MCP execution contract")
        .parse()
        .expect("canonical SHA-256")
}
fn abi(operation: &str) -> Sha256Digest {
    digest(json!({"owner":"mcp","operation":operation,"interpreter_version":1}))
}
pub fn adapter_identity(
    transport: McpTransportKind,
    offered_versions: &[String],
) -> Result<Sha256Digest, McpHostError> {
    if offered_versions.is_empty()
        || offered_versions.len() > SUPPORTED_SEMANTIC_PROTOCOLS.len()
        || !offered_versions.windows(2).all(|pair| pair[0] < pair[1])
        || offered_versions
            .iter()
            .any(|version| !SUPPORTED_SEMANTIC_PROTOCOLS.contains(&version.as_str()))
    {
        return Err(McpHostError::InvalidDiscovery);
    }
    Ok(digest(
        json!({"protocol":"mcp","transport":transport,"supported_semantic_protocols":offered_versions}),
    ))
}
fn requirement(operation: &str, adapter: Sha256Digest, policy: Value) -> ExecutionRequirement {
    ExecutionRequirement::DomainOperation {
        operation_abi_identity: abi(operation),
        requirements: DomainOperationRequirements::Adapter {
            protocol_adapter_identity: adapter,
            policy_inputs_digest: digest(policy),
        },
    }
}
impl McpDiscoveryAdmission {
    pub fn execution_requirement(
        &self,
        execution: &McpDiscoveryExecutionContract,
    ) -> Result<ExecutionRequirement, McpHostError> {
        if self.mcp_deployment != execution.deployment {
            return Err(McpHostError::InvalidDiscovery);
        }
        self.execution_requirement_for_protocol(&execution.server, &execution.protocol_profile)
    }
    pub fn execution_requirement_for_protocol(
        &self,
        server: &insight_platform_contracts::McpServerExecutionContract,
        protocol: &insight_platform_contracts::McpProtocolPolicyDocument,
    ) -> Result<ExecutionRequirement, McpHostError> {
        self.validate()?;
        server
            .validate()
            .map_err(|_| McpHostError::InvalidDiscovery)?;
        protocol
            .validate()
            .map_err(|_| McpHostError::InvalidDiscovery)?;
        if self.server_revision != server.revision
            || self.protocol_profile != server.protocol_policy
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        let adapter = adapter_identity(server.transport, &protocol.offered_versions)?;
        Ok(requirement(
            "discovery-payload-v1",
            adapter,
            json!({"protocol_profile":self.protocol_profile,
            "authorization_context_digest":self.authorization_context_digest,"authorization_generation":self.authorization_generation,
            "mcp_deployment":self.mcp_deployment,"server_revision":self.server_revision,"artifact_policy":self.artifact_policy}),
        ))
    }
}
impl McpResourceSubscriptionBinding {
    pub fn execution_requirement(
        &self,
        execution: &McpHostExecutionContract,
    ) -> Result<ExecutionRequirement, McpHostError> {
        if self.mcp_deployment != execution.deployment
            || self.protocol_profile != execution.server.protocol_policy
            || self.transport_kind != execution.server.transport
        {
            return Err(McpHostError::InvalidSubscription);
        }
        self.execution_requirement_for_protocol(&execution.protocol_profile)
    }
    pub fn execution_requirement_for_protocol(
        &self,
        protocol: &insight_platform_contracts::McpProtocolPolicyDocument,
    ) -> Result<ExecutionRequirement, McpHostError> {
        self.validate_canonical()?;
        protocol
            .validate()
            .map_err(|_| McpHostError::InvalidSubscription)?;
        let adapter = adapter_identity(self.transport_kind, &protocol.offered_versions)?;
        Ok(requirement(
            "subscription-payload-v1",
            adapter,
            json!({"protocol_profile":self.protocol_profile,
            "authorization_context_digest":self.authorization_context_digest,"authorization_generation":self.authorization_generation,
            "mcp_deployment":self.mcp_deployment,"context_deployment":self.context_deployment,
            "scope_digest":self.scope_digest,"transport_binding_digest":self.transport_binding_digest,
            "discovery_snapshot_digest":self.discovery_snapshot_digest}),
        ))
    }
}
pub fn execution_capabilities() -> WorkerExecutionCapabilities {
    let adapter = adapter_identity(
        McpTransportKind::StreamableHttp,
        &[MCP_PROTOCOL_BASELINE.to_owned()],
    )
    .expect("closed MCP baseline");
    WorkerExecutionCapabilities {
        schema_version: EXECUTION_REQUIREMENT_VERSION,
        capabilities: ["discovery-payload-v1", "subscription-payload-v1"]
            .into_iter()
            .map(|operation| WorkerExecutionCapability::DomainOperation {
                operation_abi_identity: abi(operation),
                adapter: Some(adapter.clone()),
            })
            .collect(),
    }
}
pub fn discovery_execution_capabilities() -> WorkerExecutionCapabilities {
    let mut catalog = execution_capabilities();
    catalog.capabilities.truncate(1);
    catalog
}
pub fn subscription_execution_capabilities() -> WorkerExecutionCapabilities {
    let mut catalog = execution_capabilities();
    catalog.capabilities.remove(0);
    catalog
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn protocol_semantics_are_closed_and_tenant_independent() {
        execution_capabilities().validate().unwrap();
        assert!(
            adapter_identity(McpTransportKind::StreamableHttp, &["2099-01-01".to_owned()]).is_err()
        );
        assert!(adapter_identity(McpTransportKind::StreamableHttp, &[]).is_err());
    }
}
