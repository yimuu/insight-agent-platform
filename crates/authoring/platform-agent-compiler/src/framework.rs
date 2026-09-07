//! Data-only export contract for a deliberately limited framework integration.
//! This is an explicit platform-annotated export, not an arbitrary LangGraph program reader.
use insight_platform_contracts::{ClosedJsonSchema, ClosedValueSchema, Sha256Digest};
use insight_platform_plan::{PlanNodeKey, RuntimeDependencySlot, RuntimeNode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const FRAMEWORK_EXPORT_VERSION: u32 = 1;
pub const MAX_FRAMEWORK_EXPORT_BYTES: usize = 1_048_576;
/// The only accepted graph/state semantics. There are no host closures, reducers,
/// framework checkpoints, dynamic graph construction, or implicit state mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameworkExportDialect {
    LangGraphStaticTypedPortsV1,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationRecoveryGranularity {
    PlatformNodes,
    CapabilityInvocation,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticFrameworkExportV1 {
    pub schema_version: u32,
    pub dialect: FrameworkExportDialect,
    pub adapter_semantic_identity: Sha256Digest,
    pub entry_node_id: PlanNodeKey,
    // Reuse the sole owning typed node vocabulary; the export author must supply
    // explicit port, control-scope, deadline, retry and cancellation semantics.
    pub nodes: BTreeMap<PlanNodeKey, RuntimeNode>,
    pub dependency_slots: BTreeMap<String, RuntimeDependencySlot>,
    pub schema_documents: BTreeMap<Sha256Digest, ClosedValueSchema>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameworkImportRequestV1 {
    pub schema_version: u32,
    pub name: String,
    pub display_name: String,
    pub input_schema: ClosedJsonSchema,
    pub output_schema: ClosedJsonSchema,
    pub input_classification: insight_platform_contracts::DataClassification,
    pub profile: crate::AgentCompilerProfile,
    pub bindings: crate::ResolvedAgentBindings,
    pub graph: StaticFrameworkExportV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameworkImportResultV1 {
    pub schema_version: u32,
    pub recovery_granularity: IntegrationRecoveryGranularity,
    pub adapter_semantic_identity: Sha256Digest,
    pub source_bundle: crate::AgentSourceBundleV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameworkImportError {
    InvalidExport,
    UnsupportedSemantics,
    CompilationRejected,
    LimitExceeded,
}

pub fn framework_adapter_semantic_identity() -> Sha256Digest {
    insight_platform_contracts::canonical_digest(&serde_json::json!({"adapter":"langgraph-static-typed-ports","semantic_version":1,"state":"single_assignment_exact_ports","checkpoint_owner":"platform_run","ir_abi":6})).expect("static adapter descriptor").parse().expect("canonical digest")
}
impl StaticFrameworkExportV1 {
    pub fn lower(
        &self,
        interface_contract_digest: Sha256Digest,
    ) -> Result<insight_platform_plan::RuntimePlan, FrameworkImportError> {
        if self.schema_version != FRAMEWORK_EXPORT_VERSION
            || self.adapter_semantic_identity != framework_adapter_semantic_identity()
        {
            return Err(FrameworkImportError::UnsupportedSemantics);
        }
        if self.nodes.is_empty()
            || self.nodes.len() > 1024
            || self.dependency_slots.len() > 64
            || self.schema_documents.len() > insight_platform_plan::MAX_PLAN_SCHEMA_DOCUMENTS
            || serde_json::to_vec(self)
                .map_err(|_| FrameworkImportError::InvalidExport)?
                .len()
                > MAX_FRAMEWORK_EXPORT_BYTES
        {
            return Err(FrameworkImportError::LimitExceeded);
        }
        Ok(insight_platform_plan::RuntimePlan {
            plan_version: 6,
            interface_contract_digest,
            entry_node_id: self.entry_node_id.clone(),
            nodes: self.nodes.clone(),
            dependency_slots: self.dependency_slots.clone(),
            schema_documents: self.schema_documents.clone(),
        })
    }
}
/// Generate the shared authoring representation while retaining the original export
/// as source. Registry validation will independently lower those same frozen bytes.
pub fn import_framework(
    request: FrameworkImportRequestV1,
) -> Result<FrameworkImportResultV1, FrameworkImportError> {
    if request.schema_version != 1
        || serde_json::to_vec(&request)
            .map_err(|_| FrameworkImportError::InvalidExport)?
            .len()
            > crate::MAX_AGENT_COMPILER_REQUEST_BYTES
    {
        return Err(FrameworkImportError::LimitExceeded);
    }
    request
        .input_schema
        .validate()
        .map_err(|_| FrameworkImportError::InvalidExport)?;
    request
        .output_schema
        .validate()
        .map_err(|_| FrameworkImportError::InvalidExport)?;
    let manifest = serde_json::json!({"apiVersion":"insight.platform/v1","kind":"Agent","metadata":{"name":request.name,"displayName":request.display_name},"spec":{"execution":{"kind":"framework_graph","plan":"framework.json"},"input":{"schema":"input.json","classification":request.input_classification},"output":{"schema":"output.json"}}});
    let json = |value: serde_json::Value| -> Result<String, FrameworkImportError> {
        String::from_utf8(
            insight_platform_contracts::canonical_json(&value)
                .map_err(|_| FrameworkImportError::InvalidExport)?,
        )
        .map_err(|_| FrameworkImportError::InvalidExport)
    };
    let source_bundle = crate::AgentSourceBundleV1 {
        schema_version: crate::AGENT_SOURCE_BUNDLE_VERSION,
        compiler_semantic_identity: crate::compiler_semantic_identity(),
        compile_policy_inputs_digest: crate::AgentSourceBundleV1::policy_digest(&request.profile),
        profile: request.profile,
        bindings: request.bindings,
        sources: crate::AgentSourceFilesV1 {
            manifest_path: "agent.json".to_owned(),
            files: BTreeMap::from([
                ("agent.json".to_owned(), json(manifest)?),
                ("input.json".to_owned(), json(request.input_schema.schema)?),
                (
                    "output.json".to_owned(),
                    json(request.output_schema.schema)?,
                ),
                (
                    "framework.json".to_owned(),
                    json(
                        serde_json::to_value(&request.graph)
                            .map_err(|_| FrameworkImportError::InvalidExport)?,
                    )?,
                ),
            ]),
        },
    };
    if !matches!(
        crate::compile_source_bundle(source_bundle.clone()),
        crate::AgentCompileResponseV1::Compiled { .. }
    ) {
        return Err(FrameworkImportError::CompilationRejected);
    }
    Ok(FrameworkImportResultV1 {
        schema_version: 1,
        recovery_granularity: IntegrationRecoveryGranularity::PlatformNodes,
        adapter_semantic_identity: framework_adapter_semantic_identity(),
        source_bundle,
    })
}
