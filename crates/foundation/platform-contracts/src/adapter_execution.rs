//! Stable adapter semantics. Publication manifest digests and executable build
//! identities are provenance, not adapter compatibility keys.
use crate::{
    canonical_digest, CapabilityBackendBinding, CapabilityBackendContract, ContextBackendBinding,
    ContextBackendContract, DomainOperationRequirements, ExecutionCompatibilityError,
    ExecutionRequirement, InstalledCapabilityCodecRef, InstalledModelAdapter, Sha256Digest,
    WorkerExecutionCapability,
};
use serde_json::{json, Value};

fn digest(value: Value) -> Result<Sha256Digest, ExecutionCompatibilityError> {
    canonical_digest(&value)
        .map_err(|_| ExecutionCompatibilityError::InvalidRequirement)?
        .parse()
        .map_err(|_| ExecutionCompatibilityError::InvalidRequirement)
}
fn capability(
    operation: &str,
    adapter: Value,
) -> Result<WorkerExecutionCapability, ExecutionCompatibilityError> {
    Ok(WorkerExecutionCapability::DomainOperation {
        operation_abi_identity: digest(json!({"contract":operation,"abi":1}))?,
        adapter: Some(digest(adapter)?),
    })
}
/// Freezes policy inputs separately from the installed semantic capability. A
/// worker cannot turn a Program capability into permission for physical I/O.
pub fn adapter_execution_requirement(
    capability: &WorkerExecutionCapability,
    policy_inputs_digest: Sha256Digest,
) -> Result<ExecutionRequirement, ExecutionCompatibilityError> {
    let WorkerExecutionCapability::DomainOperation {
        operation_abi_identity,
        adapter: Some(adapter),
    } = capability
    else {
        return Err(ExecutionCompatibilityError::InvalidRequirement);
    };
    Ok(ExecutionRequirement::DomainOperation {
        operation_abi_identity: operation_abi_identity.clone(),
        requirements: DomainOperationRequirements::Adapter {
            protocol_adapter_identity: adapter.clone(),
            policy_inputs_digest,
        },
    })
}

pub fn model_adapter_execution_capability(
    adapter: &InstalledModelAdapter,
) -> Result<WorkerExecutionCapability, ExecutionCompatibilityError> {
    adapter
        .validate()
        .map_err(|_| ExecutionCompatibilityError::InvalidRequirement)?;
    model_adapter_execution_capability_from_contract(
        &adapter.qualified_name,
        &adapter.adapter_contract_digest,
    )
}
pub fn model_adapter_execution_capability_from_contract(
    qualified_name: &str,
    contract: &Sha256Digest,
) -> Result<WorkerExecutionCapability, ExecutionCompatibilityError> {
    if qualified_name.is_empty()
        || qualified_name.len() > crate::MAX_MODEL_ADAPTER_NAME_BYTES
        || !qualified_name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'/'))
    {
        return Err(ExecutionCompatibilityError::InvalidRequirement);
    }
    capability(
        "model.adapter.execute",
        json!({"qualified_name":qualified_name,"adapter_contract_digest":contract}),
    )
}

pub fn native_capability_adapter_execution_capability(
    module: &Sha256Digest,
) -> Result<WorkerExecutionCapability, ExecutionCompatibilityError> {
    capability(
        "capability.native.execute",
        json!({"adapter_module_digest":module}),
    )
}

/// `validate_for` at admission proves this descriptor matches the complete
/// immutable backend contract. Installation proves it is an implemented codec.
pub fn remote_capability_adapter_execution_capability(
    codec: &InstalledCapabilityCodecRef,
) -> Result<WorkerExecutionCapability, ExecutionCompatibilityError> {
    if codec.schema_version != crate::INSTALLED_CAPABILITY_CODEC_MANIFEST_VERSION
        || codec.worker_protocol_version != crate::WORKER_PROTOCOL_VERSION
        || codec.codec_id.is_empty()
        || codec.codec_id.len() > 128
        || codec.codec_version.is_empty()
        || codec.codec_version.len() > 128
        || !matches!(
            codec.backend_kind,
            crate::CapabilityBackendKind::Http
                | crate::CapabilityBackendKind::Grpc
                | crate::CapabilityBackendKind::Mcp
        )
    {
        return Err(ExecutionCompatibilityError::InvalidRequirement);
    }
    capability(
        "capability.remote.execute",
        serde_json::to_value(codec).map_err(|_| ExecutionCompatibilityError::InvalidRequirement)?,
    )
}

pub fn capability_adapter_execution_capability(
    binding: &CapabilityBackendBinding,
    contract: &CapabilityBackendContract,
    features: &crate::CapabilityBackendFeatures,
) -> Result<Option<WorkerExecutionCapability>, ExecutionCompatibilityError> {
    contract
        .validate(features)
        .map_err(|_| ExecutionCompatibilityError::InvalidRequirement)?;
    binding
        .validate_for(contract)
        .map_err(|_| ExecutionCompatibilityError::InvalidRequirement)?;
    match binding {
        CapabilityBackendBinding::Native {
            adapter_module_digest,
            ..
        } => native_capability_adapter_execution_capability(adapter_module_digest).map(Some),
        CapabilityBackendBinding::Http { codec, .. }
        | CapabilityBackendBinding::Grpc { codec, .. }
        | CapabilityBackendBinding::Mcp { codec, .. } => {
            codec
                .validate_for(contract)
                .map_err(|_| ExecutionCompatibilityError::InvalidRequirement)?;
            remote_capability_adapter_execution_capability(codec).map(Some)
        }
        // Sandbox has its own frozen runtime/package/profile execution boundary.
        CapabilityBackendBinding::Sandbox { .. } => Ok(None),
    }
}

pub fn native_context_adapter_execution_capability(
    contract: &Sha256Digest,
    installed: &Sha256Digest,
) -> Result<WorkerExecutionCapability, ExecutionCompatibilityError> {
    capability(
        "context.query.execute",
        json!({"kind":"native_catalog","adapter_contract_digest":contract,"installed_adapter_digest":installed}),
    )
}
pub fn remote_context_adapter_execution_capability(
    protocol: &Sha256Digest,
    mapping: &Sha256Digest,
) -> Result<WorkerExecutionCapability, ExecutionCompatibilityError> {
    capability(
        "context.query.execute",
        json!({"kind":"remote_search","protocol_contract_digest":protocol,"result_mapping_digest":mapping}),
    )
}
pub fn context_adapter_execution_capability(
    binding: &ContextBackendBinding,
    contract: &ContextBackendContract,
) -> Result<WorkerExecutionCapability, ExecutionCompatibilityError> {
    binding
        .validate()
        .map_err(|_| ExecutionCompatibilityError::InvalidRequirement)?;
    contract
        .validate()
        .map_err(|_| ExecutionCompatibilityError::InvalidRequirement)?;
    match (binding, contract) {
        (
            ContextBackendBinding::NativeCatalog {
                installed_adapter_digest,
            },
            ContextBackendContract::NativeCatalog {
                adapter_contract_digest,
            },
        ) => native_context_adapter_execution_capability(
            adapter_contract_digest,
            installed_adapter_digest,
        ),
        (
            ContextBackendBinding::RemoteSearch { .. },
            ContextBackendContract::RemoteSearch {
                protocol_contract_digest,
                result_mapping_digest,
            },
        ) => remote_context_adapter_execution_capability(
            protocol_contract_digest,
            result_mapping_digest,
        ),
        // These describe existing domain contracts. Production installed catalogs
        // declare only implemented backends; describing a requirement grants no I/O.
        (
            ContextBackendBinding::ManagedIndex { .. },
            ContextBackendContract::ManagedIndex {
                query_contract_digest,
                result_contract_digest,
            },
        ) => capability(
            "context.query.execute",
            json!({"kind":"managed_index","query_contract_digest":query_contract_digest,"result_contract_digest":result_contract_digest}),
        ),
        (
            ContextBackendBinding::SqlCatalog { dialect, .. },
            ContextBackendContract::SqlCatalog {
                dialect: contract_dialect,
                catalog_projection_digest,
            },
        ) if dialect == contract_dialect => capability(
            "context.query.execute",
            json!({"kind":"sql_catalog","dialect":dialect,"catalog_projection_digest":catalog_projection_digest}),
        ),
        (
            ContextBackendBinding::McpResources { .. },
            ContextBackendContract::McpResources {
                resource_contract_digest,
                ..
            },
        ) => capability(
            "context.query.execute",
            json!({"kind":"mcp_resources","resource_contract_digest":resource_contract_digest}),
        ),
        (
            ContextBackendBinding::ArtifactCollection { .. },
            ContextBackendContract::ArtifactCollection {
                collection_contract_digest,
            },
        ) => capability(
            "context.query.execute",
            json!({"kind":"artifact_collection","collection_contract_digest":collection_contract_digest}),
        ),
        _ => Err(ExecutionCompatibilityError::UnsupportedRequirement),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sha(c: char) -> Sha256Digest {
        format!("sha256:{}", c.to_string().repeat(64))
            .parse()
            .unwrap()
    }
    #[test]
    fn rebuild_does_not_change_model_semantics_but_contract_change_does() {
        let mut adapter = InstalledModelAdapter {
            qualified_name: "builtin.model".into(),
            worker_manifest_digest: sha('1'),
            adapter_contract_digest: sha('2'),
        };
        let original = model_adapter_execution_capability(&adapter).unwrap();
        adapter.worker_manifest_digest = sha('3');
        assert_eq!(
            original,
            model_adapter_execution_capability(&adapter).unwrap()
        );
        adapter.adapter_contract_digest = sha('4');
        assert_ne!(
            original,
            model_adapter_execution_capability(&adapter).unwrap()
        );
    }
    #[test]
    fn policy_is_frozen_but_does_not_enumerate_tenant_capabilities() {
        let capability = native_capability_adapter_execution_capability(&sha('1')).unwrap();
        let first = adapter_execution_requirement(&capability, sha('2')).unwrap();
        let second = adapter_execution_requirement(&capability, sha('3')).unwrap();
        assert!(capability.supports(&first) && capability.supports(&second));
        assert_ne!(
            first.canonical_digest().unwrap(),
            second.canonical_digest().unwrap()
        );
        assert!(!native_capability_adapter_execution_capability(&sha('4'))
            .unwrap()
            .supports(&first));
    }
    #[test]
    fn program_capability_cannot_authorize_an_adapter() {
        let program = WorkerExecutionCapability::Program {
            program_semantic_identity: sha('1'),
            ir_abi_version: 6,
        };
        assert!(adapter_execution_requirement(&program, sha('2')).is_err());
        let adapter = native_context_adapter_execution_capability(&sha('2'), &sha('3')).unwrap();
        assert!(!program.supports(&adapter_execution_requirement(&adapter, sha('4')).unwrap()));
    }
}
