//! Pure reconstruction of authoring evidence. Storage, authorization and lease fencing belong
//! to the Registry application and its Artifact reader, never to this compiler.

use insight_platform_contracts::{ExecutionRequirement, ResourceDocument, Sha256Digest};
use serde::{Deserialize, Serialize};

use crate::{AgentBoundaryErrorCode, AgentCompilationV1, AgentSourceBundleV1, ArtifactAuthority};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCompilationInputIdentityV1 {
    pub schema_version: u32,
    pub source_bundle_digest: Sha256Digest,
    pub compiler_semantic_identity: Sha256Digest,
    pub compile_policy_inputs_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCompilationEvidenceV1 {
    pub schema_version: u32,
    pub input: AgentCompilationInputIdentityV1,
    pub manifest_digest: Sha256Digest,
    pub typed_plan_digest: Sha256Digest,
    pub contract_digest: Sha256Digest,
    pub program_requirement: ExecutionRequirement,
    pub deployment_features: Vec<insight_platform_contracts::AgentDeploymentFeaturesV1>,
}

impl AgentCompilationInputIdentityV1 {
    pub fn validate(&self) -> Result<(), AgentBoundaryErrorCode> {
        if self.schema_version != 1 {
            return Err(AgentBoundaryErrorCode::AuthoringVersionUnsupported);
        }
        Ok(())
    }
}

impl AgentCompilationEvidenceV1 {
    pub fn validate(&self) -> Result<(), AgentBoundaryErrorCode> {
        self.input.validate()?;
        if self.deployment_features.len()
            > insight_platform_contracts::MAX_AGENT_DEPLOYMENT_FEATURE_EVIDENCE
            || self
                .deployment_features
                .iter()
                .any(|item| item.validate().is_err())
            || self
                .deployment_features
                .windows(2)
                .any(|pair| pair[0].deployment.deployment_id >= pair[1].deployment.deployment_id)
        {
            return Err(AgentBoundaryErrorCode::AgentCompileFailed);
        }
        if self.schema_version != 1 {
            return Err(AgentBoundaryErrorCode::AuthoringVersionUnsupported);
        }
        match &self.program_requirement {
            ExecutionRequirement::Program {
                definition_digest,
                program_semantic_identity,
                ir_abi_version,
            } if definition_digest == &self.typed_plan_digest
                && insight_platform_plan::execution::program_semantic_identity(*ir_abi_version)
                    .as_ref()
                    == Ok(program_semantic_identity) =>
            {
                Ok(())
            }
            _ => Err(AgentBoundaryErrorCode::AgentCompileFailed),
        }
    }
}

/// Inspect the actual immutable Artifact bytes, never a client-supplied digest envelope.
/// Canonical bytes are required so the package identity has precisely one representation.
pub fn inspect_frozen_source_bundle(
    bytes: &[u8],
) -> Result<AgentCompilationInputIdentityV1, AgentBoundaryErrorCode> {
    let bundle: AgentSourceBundleV1 = crate::boundary::parse_boundary(bytes)?;
    let canonical = bundle.canonical_bytes()?;
    if canonical != bytes {
        return Err(AgentBoundaryErrorCode::SourceBundleInvalid);
    }
    Ok(AgentCompilationInputIdentityV1 {
        schema_version: 1,
        source_bundle_digest: crate::digest_bytes(bytes)
            .map_err(|_| AgentBoundaryErrorCode::CompilerInternal)?,
        compiler_semantic_identity: bundle.compiler_semantic_identity,
        compile_policy_inputs_digest: bundle.compile_policy_inputs_digest,
    })
}

/// Recompile and compare the complete Agent document and exact Plan bytes. Ready Artifact
/// authority is supplied by the authorized reader and must be rechecked by the commit fence.
pub fn validate_frozen_agent_artifacts(
    document: &ResourceDocument,
    source: &ArtifactAuthority,
    source_bytes: &[u8],
    plan: &ArtifactAuthority,
    plan_bytes: &[u8],
) -> Result<AgentCompilationEvidenceV1, AgentBoundaryErrorCode> {
    let input = inspect_frozen_source_bundle(source_bytes)?;
    let bundle: AgentSourceBundleV1 = crate::boundary::parse_boundary(source_bytes)?;
    let deployment_features = bundle.bindings.deployment_features.clone();
    let compilation: AgentCompilationV1 = crate::boundary::compile_validated_bundle(bundle)?;
    if compilation.source_bundle_digest != input.source_bundle_digest
        || compilation.compiled.typed_plan_bytes != plan_bytes
        || &compilation
            .materialize(source, plan)
            .map_err(|_| AgentBoundaryErrorCode::AgentBindingNotReady)?
            != document
    {
        return Err(AgentBoundaryErrorCode::AgentCompileFailed);
    }
    let plan: insight_platform_plan::RuntimePlan = crate::boundary::parse_boundary(plan_bytes)?;
    let program_requirement = insight_platform_plan::execution::program_execution_requirement(
        compilation.compiled.typed_plan_digest.clone(),
        plan.plan_version,
    )
    .map_err(|_| AgentBoundaryErrorCode::AgentCompileFailed)?;
    let evidence = AgentCompilationEvidenceV1 {
        schema_version: 1,
        input,
        manifest_digest: compilation.compiled.manifest_digest,
        typed_plan_digest: compilation.compiled.typed_plan_digest,
        contract_digest: compilation.compiled.resource_intent.contract_digest,
        program_requirement,
        deployment_features,
    };
    evidence.validate()?;
    Ok(evidence)
}
