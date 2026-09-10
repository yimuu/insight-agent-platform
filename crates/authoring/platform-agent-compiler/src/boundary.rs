//! Owning, versioned native/WASM and persisted Agent authoring boundary.
//!
//! Source files are data. No path in this module is opened or resolved through the host.

use std::collections::BTreeMap;

use insight_platform_contracts::{canonical_digest, canonical_json, Sha256Digest};
use serde::{Deserialize, Serialize};

use crate::{AgentCompilerProfile, CompiledAgent, ResolvedAgentBindings};

pub const AGENT_SOURCE_BUNDLE_VERSION: u32 = 1;
pub const MAX_AGENT_SOURCE_FILES: usize = 64;
pub const MAX_AGENT_SOURCE_PATH_BYTES: usize = 256;
pub const MAX_AGENT_SOURCE_FILE_BYTES: usize = 1_048_576;
pub const MAX_AGENT_SOURCE_TOTAL_BYTES: usize = 4_194_304;
pub const MAX_AGENT_COMPILER_REQUEST_BYTES: usize = 8_388_608;
pub const MAX_AGENT_COMPILER_RESPONSE_BYTES: usize = 16_777_216;
pub const MAX_AGENT_COMPILER_DIAGNOSTICS: usize = 32;

/// This identity describes parser/defaulting/lowering/canonicalization semantics, never a build.
pub fn compiler_semantic_identity() -> Sha256Digest {
    canonical_digest(&serde_json::json!({
        "dialect": "insight.agent-authoring",
        "semantic_version": 7,
        "source_bundle_version": AGENT_SOURCE_BUNDLE_VERSION,
        "runtime_plan_version": 6
    }))
    .expect("static compiler identity is canonical JSON")
    .parse()
    .expect("canonical digest is a nominal digest")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSourceFilesV1 {
    pub manifest_path: String,
    pub files: BTreeMap<String, String>,
}

impl AgentSourceFilesV1 {
    pub fn validate(&self) -> Result<(), AgentBoundaryErrorCode> {
        if self.files.is_empty()
            || self.files.len() > MAX_AGENT_SOURCE_FILES
            || !self.files.contains_key(&self.manifest_path)
        {
            return Err(AgentBoundaryErrorCode::SourceBundleInvalid);
        }
        let mut total = 0usize;
        for (path, contents) in &self.files {
            crate::validate_relative_reference(path, "source")
                .map_err(|_| AgentBoundaryErrorCode::SourceBundleInvalid)?;
            if path.len() > MAX_AGENT_SOURCE_PATH_BYTES {
                return Err(AgentBoundaryErrorCode::CompilerLimitExceeded);
            }
            total = total
                .checked_add(path.len())
                .and_then(|size| size.checked_add(contents.len()))
                .ok_or(AgentBoundaryErrorCode::CompilerLimitExceeded)?;
            if contents.len() > MAX_AGENT_SOURCE_FILE_BYTES || total > MAX_AGENT_SOURCE_TOTAL_BYTES
            {
                return Err(AgentBoundaryErrorCode::CompilerLimitExceeded);
            }
        }
        Ok(())
    }
}

/// Unfrozen authoring input. The core chooses its own installed semantic identity and freezes it.
/// This convenience request is never Registry validation evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentAuthoringRequestV1 {
    pub schema_version: u32,
    pub sources: AgentSourceFilesV1,
    pub profile: AgentCompilerProfile,
    pub bindings: ResolvedAgentBindings,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestInspectionRequestV1 {
    pub schema_version: u32,
    pub manifest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentManifestInspectionResponseV1 {
    Inspected {
        schema_version: u32,
        resolution: crate::AgentManifestResolution,
    },
    Rejected {
        diagnostics: Vec<AgentCompilerDiagnosticV1>,
    },
}

/// Complete input for recompilation. The Artifact containing this value is distinct from the IR.
/// Resource publication keeps the existing authoring manifest digest alongside the bundle digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSourceBundleV1 {
    pub schema_version: u32,
    pub compiler_semantic_identity: Sha256Digest,
    pub compile_policy_inputs_digest: Sha256Digest,
    pub sources: AgentSourceFilesV1,
    pub profile: AgentCompilerProfile,
    pub bindings: ResolvedAgentBindings,
}

impl AgentSourceBundleV1 {
    pub fn policy_digest(profile: &AgentCompilerProfile) -> Sha256Digest {
        canonical_digest(&serde_json::to_value(profile).expect("compiler policy is serializable"))
            .expect("compiler policy is canonical JSON")
            .parse()
            .expect("canonical digest is a nominal digest")
    }

    pub fn validate(&self) -> Result<(), AgentBoundaryErrorCode> {
        if self.schema_version != AGENT_SOURCE_BUNDLE_VERSION {
            return Err(AgentBoundaryErrorCode::AuthoringVersionUnsupported);
        }
        if self.compiler_semantic_identity != compiler_semantic_identity() {
            return Err(AgentBoundaryErrorCode::CompilerSemanticUnsupported);
        }
        if self.compile_policy_inputs_digest != Self::policy_digest(&self.profile) {
            return Err(AgentBoundaryErrorCode::CompilePolicyMismatch);
        }
        self.sources.validate()?;
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, AgentBoundaryErrorCode> {
        self.validate()?;
        canonical_json(
            &serde_json::to_value(self).map_err(|_| AgentBoundaryErrorCode::CompilerInternal)?,
        )
        .map_err(|_| AgentBoundaryErrorCode::CompilerInternal)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentBoundaryErrorCode {
    SourceBundleInvalid,
    SourceReferenceMissing,
    AuthoringVersionUnsupported,
    CompilerSemanticUnsupported,
    CompilePolicyMismatch,
    CompilerLimitExceeded,
    AgentManifestInvalid,
    AgentReferenceMissing,
    AgentBindingNotReady,
    AgentCompileFailed,
    CompilerInternal,
}

impl AgentBoundaryErrorCode {
    pub const ALL: &'static [Self] = &[
        Self::SourceBundleInvalid,
        Self::SourceReferenceMissing,
        Self::AuthoringVersionUnsupported,
        Self::CompilerSemanticUnsupported,
        Self::CompilePolicyMismatch,
        Self::CompilerLimitExceeded,
        Self::AgentManifestInvalid,
        Self::AgentReferenceMissing,
        Self::AgentBindingNotReady,
        Self::AgentCompileFailed,
        Self::CompilerInternal,
    ];
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSourceLocationV1 {
    pub file: String,
    pub source_pointer: String,
    pub line: u32,
    pub column: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCompilerDiagnosticV1 {
    pub code: AgentBoundaryErrorCode,
    pub location: Option<AgentSourceLocationV1>,
    pub safe_detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCompilationV1 {
    pub schema_version: u32,
    pub compiler_semantic_identity: Sha256Digest,
    pub compile_policy_inputs_digest: Sha256Digest,
    pub source_bundle_bytes: Vec<u8>,
    pub source_bundle_digest: Sha256Digest,
    pub source_map_bytes: Vec<u8>,
    pub source_map_digest: Sha256Digest,
    pub compiled: CompiledAgent,
}

impl AgentCompilationV1 {
    /// Manifest identity and complete publication-input identity have different meanings.
    pub fn materialize(
        &self,
        authoring: &crate::ArtifactAuthority,
        typed_plan: &crate::ArtifactAuthority,
    ) -> Result<insight_platform_contracts::ResourceDocument, crate::AgentCompilerError> {
        if authoring.artifact.content_digest() != &self.source_bundle_digest {
            return Err(crate::AgentCompilerError::binding(
                "authoring source bundle identity differs",
            ));
        }
        let mut document = self
            .compiled
            .resource_intent
            .materialize(authoring, typed_plan)?;
        let insight_platform_contracts::ResourceDocument::Agent(agent) = &mut document else {
            return Err(crate::AgentCompilerError::compile(
                "compiler produced a non-Agent resource",
            ));
        };
        agent.authoring_package.manifest_digest = self.compiled.manifest_digest.clone();
        Ok(document)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentCompileResponseV1 {
    Compiled {
        compilation: Box<AgentCompilationV1>,
    },
    Rejected {
        diagnostics: Vec<AgentCompilerDiagnosticV1>,
    },
}

pub fn compile_source_bundle(bundle: AgentSourceBundleV1) -> AgentCompileResponseV1 {
    let source = bundle.clone();
    match compile_validated_bundle(bundle) {
        Ok(compilation) => bounded_response(AgentCompileResponseV1::Compiled {
            compilation: Box::new(compilation),
        }),
        Err(code) => {
            let mut response = rejection(code);
            if let AgentCompileResponseV1::Rejected { diagnostics } = &mut response {
                diagnostics[0].location =
                    crate::source_map::diagnostic_location(&source.sources, code);
            }
            bounded_response(response)
        }
    }
}

pub(crate) fn compile_validated_bundle(
    bundle: AgentSourceBundleV1,
) -> Result<AgentCompilationV1, AgentBoundaryErrorCode> {
    bundle.validate()?;
    let manifest = bundle
        .sources
        .files
        .get(&bundle.sources.manifest_path)
        .ok_or(AgentBoundaryErrorCode::SourceReferenceMissing)?;
    let resolution = crate::inspect_manifest(manifest.as_bytes()).map_err(compiler_error_code)?;
    let mut expected = vec![
        bundle.sources.manifest_path.as_str(),
        resolution.input_schema_path.as_str(),
        resolution.output_schema_path.as_str(),
    ];
    if let Some(path) = &resolution.plan_path {
        expected.push(path);
    }
    if bundle
        .sources
        .files
        .keys()
        .any(|path| !expected.contains(&path.as_str()))
    {
        return Err(AgentBoundaryErrorCode::SourceBundleInvalid);
    }
    let source = |path: &str| {
        bundle
            .sources
            .files
            .get(path)
            .map(|text| text.as_bytes().to_vec())
            .ok_or(AgentBoundaryErrorCode::SourceReferenceMissing)
    };
    let mut compiled = crate::compile_agent(crate::AgentCompilerInput {
        plan_bytes: resolution.plan_path.as_deref().map(source).transpose()?,
        manifest_bytes: manifest.as_bytes().to_vec(),
        input_schema_bytes: source(&resolution.input_schema_path)?,
        output_schema_bytes: source(&resolution.output_schema_path)?,
        profile: bundle.profile.clone(),
        bindings: bundle.bindings.clone(),
    })
    .map_err(compiler_error_code)?;
    let source_map_bytes =
        crate::source_map::build_source_map(&bundle, &compiled)?.canonical_bytes()?;
    let source_map_digest = crate::digest_bytes(&source_map_bytes)
        .map_err(|_| AgentBoundaryErrorCode::CompilerInternal)?;
    let source_bundle_bytes = bundle.canonical_bytes()?;
    let source_bundle_digest = crate::digest_bytes(&source_bundle_bytes)
        .map_err(|_| AgentBoundaryErrorCode::CompilerInternal)?;
    compiled.resource_intent.authoring_artifact.content_digest = source_bundle_digest.clone();
    compiled.resource_intent.authoring_artifact.byte_length = source_bundle_bytes.len() as u64;
    Ok(AgentCompilationV1 {
        schema_version: 1,
        compiler_semantic_identity: bundle.compiler_semantic_identity,
        compile_policy_inputs_digest: bundle.compile_policy_inputs_digest,
        source_bundle_bytes,
        source_bundle_digest,
        source_map_bytes,
        source_map_digest,
        compiled,
    })
}

pub(crate) fn compiler_error_code(error: crate::AgentCompilerError) -> AgentBoundaryErrorCode {
    match error.code() {
        crate::AgentCompilerErrorCode::AgentManifestInvalid => {
            AgentBoundaryErrorCode::AgentManifestInvalid
        }
        crate::AgentCompilerErrorCode::AgentReferenceMissing => {
            AgentBoundaryErrorCode::AgentReferenceMissing
        }
        crate::AgentCompilerErrorCode::AgentBindingNotReady => {
            AgentBoundaryErrorCode::AgentBindingNotReady
        }
        crate::AgentCompilerErrorCode::AgentCompileFailed => {
            AgentBoundaryErrorCode::AgentCompileFailed
        }
    }
}

pub(crate) fn rejection(code: AgentBoundaryErrorCode) -> AgentCompileResponseV1 {
    AgentCompileResponseV1::Rejected {
        diagnostics: vec![AgentCompilerDiagnosticV1 {
            code,
            location: None,
            safe_detail: "The source package cannot be compiled under its exact compiler contract."
                .to_owned(),
        }],
    }
}

pub(crate) fn bounded_response(response: AgentCompileResponseV1) -> AgentCompileResponseV1 {
    if matches!(&response, AgentCompileResponseV1::Rejected { diagnostics } if diagnostics.len() > MAX_AGENT_COMPILER_DIAGNOSTICS)
        || serde_json::to_vec(&response).map_or(true, |bytes| {
            bytes.len() > MAX_AGENT_COMPILER_RESPONSE_BYTES
        })
    {
        rejection(AgentBoundaryErrorCode::CompilerLimitExceeded)
    } else {
        response
    }
}

/// The same strict and bounded transport entry point is used by native and WASM clients.
pub fn compile_request_bytes(input: &[u8]) -> Vec<u8> {
    let response = match parse_boundary::<AgentSourceBundleV1>(input) {
        Ok(bundle) => compile_source_bundle(bundle),
        Err(code) => rejection(code),
    };
    serde_json::to_vec(&bounded_response(response))
        .expect("closed compiler response is serializable")
}

/// Canonical digest utility for transport DTO integrity; it performs no authoring decisions.
pub fn canonical_digest_request_bytes(input: &[u8]) -> Vec<u8> {
    let value = parse_boundary::<serde_json::Value>(input);
    let result = value.and_then(|value| {
        canonical_digest(&value).map_err(|_| AgentBoundaryErrorCode::CompilerInternal)
    });
    serde_json::to_vec(&result).expect("bounded digest result is serializable")
}

pub(crate) fn parse_boundary<T: serde::de::DeserializeOwned>(
    input: &[u8],
) -> Result<T, AgentBoundaryErrorCode> {
    if input.len() > MAX_AGENT_COMPILER_REQUEST_BYTES {
        return Err(AgentBoundaryErrorCode::CompilerLimitExceeded);
    }
    insight_platform_contracts::parse_strict_json(
        input,
        insight_platform_contracts::JsonLimits {
            max_bytes: MAX_AGENT_COMPILER_REQUEST_BYTES,
            max_depth: 32,
            max_properties_per_object: 1_024,
            max_items_per_array: 4_096,
            max_string_bytes: MAX_AGENT_SOURCE_FILE_BYTES,
        },
    )
    .map_err(|_| AgentBoundaryErrorCode::SourceBundleInvalid)
    .and_then(|value| {
        serde_json::from_value(value).map_err(|_| AgentBoundaryErrorCode::SourceBundleInvalid)
    })
}

pub fn compile_authoring_request_bytes(input: &[u8]) -> Vec<u8> {
    let response = match parse_boundary::<AgentAuthoringRequestV1>(input) {
        Ok(request) if request.schema_version == 1 => compile_source_bundle(AgentSourceBundleV1 {
            schema_version: AGENT_SOURCE_BUNDLE_VERSION,
            compiler_semantic_identity: compiler_semantic_identity(),
            compile_policy_inputs_digest: AgentSourceBundleV1::policy_digest(&request.profile),
            sources: request.sources,
            profile: request.profile,
            bindings: request.bindings,
        }),
        Ok(_) => rejection(AgentBoundaryErrorCode::AuthoringVersionUnsupported),
        Err(code) => rejection(code),
    };
    serde_json::to_vec(&bounded_response(response))
        .expect("closed compiler response is serializable")
}

pub fn inspect_manifest_request_bytes(input: &[u8]) -> Vec<u8> {
    let result = parse_boundary::<AgentManifestInspectionRequestV1>(input).and_then(|request| {
        if request.schema_version != 1 {
            return Err(AgentBoundaryErrorCode::AuthoringVersionUnsupported);
        }
        crate::inspect_manifest(request.manifest.as_bytes()).map_err(compiler_error_code)
    });
    let response = match result {
        Ok(resolution) => AgentManifestInspectionResponseV1::Inspected {
            schema_version: 1,
            resolution,
        },
        Err(code) => {
            let AgentCompileResponseV1::Rejected { diagnostics } = rejection(code) else {
                unreachable!()
            };
            AgentManifestInspectionResponseV1::Rejected { diagnostics }
        }
    };
    serde_json::to_vec(&response).expect("bounded manifest inspection is serializable")
}
