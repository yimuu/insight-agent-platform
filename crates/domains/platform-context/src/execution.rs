//! Frozen Context operation requirements; mutable wake/progress facts do not alter compatibility.
use crate::{
    ContextDatasetBuildJobPayload, ContextQueryError, ContextSubscriptionRefreshJobPayload,
};
use insight_platform_contracts::{
    canonical_digest, DomainOperationRequirements, ExecutionRequirement, Sha256Digest,
    WorkerExecutionCapability,
};
use serde_json::{json, Value};
fn digest(value: Value) -> Sha256Digest {
    canonical_digest(&value)
        .expect("bounded Context execution contract")
        .parse()
        .expect("canonical SHA-256")
}
fn abi(operation: &str) -> Sha256Digest {
    digest(json!({"owner":"context", "operation":operation,"interpreter_version":1}))
}
fn refresh_adapter() -> Sha256Digest {
    digest(
        json!({"protocol":"context-subscription-refresh-rpc","schema_version":crate::CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION}),
    )
}
pub fn dataset_execution_capability(
    adapter_contract_digest: &Sha256Digest,
) -> WorkerExecutionCapability {
    WorkerExecutionCapability::DomainOperation {
        operation_abi_identity: abi("dataset-build-payload-v1"),
        adapter: Some(adapter_contract_digest.clone()),
    }
}
pub fn subscription_execution_capability() -> WorkerExecutionCapability {
    WorkerExecutionCapability::DomainOperation {
        operation_abi_identity: abi("subscription-refresh-payload-v1"),
        adapter: Some(refresh_adapter()),
    }
}
impl ContextDatasetBuildJobPayload {
    pub fn execution_requirement(&self) -> Result<ExecutionRequirement, ContextQueryError> {
        self.validate_for_owner(&self.dataset_id)?;
        Ok(ExecutionRequirement::DomainOperation {
            operation_abi_identity: abi("dataset-build-payload-v1"),
            requirements: DomainOperationRequirements::Adapter {
                protocol_adapter_identity: self.source_binding.adapter_contract_digest.clone(),
                policy_inputs_digest: digest(json!({"context_deployment":self.context_deployment,
                    "parser_profile":self.parser_profile,"chunker_profile":self.chunker_profile,
                    "embedding_model_deployment":self.embedding_model_deployment,"ranking_profile":self.ranking_profile,
                    "data_policy":self.data_policy,"artifact_stages":self.artifact_stages})),
            },
        })
    }
}
impl ContextSubscriptionRefreshJobPayload {
    pub fn execution_requirement(&self) -> Result<ExecutionRequirement, ContextQueryError> {
        if self.schema_version != crate::CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION
            || self
                .request
                .canonical_request_digest()
                .map_err(|_| ContextQueryError::InvalidJob)?
                != self.request.request_digest
        {
            return Err(ContextQueryError::InvalidJob);
        }
        let request = &self.request;
        Ok(ExecutionRequirement::DomainOperation {
            operation_abi_identity: abi("subscription-refresh-payload-v1"),
            requirements: DomainOperationRequirements::Adapter {
                protocol_adapter_identity: refresh_adapter(),
                policy_inputs_digest: digest(
                    json!({"context_deployment":request.context_deployment,
                    "mcp_deployment":request.mcp_deployment,"discovery_snapshot_id":request.discovery_snapshot_id,
                    "discovery_snapshot_digest":request.discovery_snapshot_digest,"resource_uri_digest":request.resource_uri_digest,
                    "authorization_generation":request.authorization_generation}),
                ),
            },
        })
    }
}
