//! Source-only editor preflight. This result grants no binding or publication authority.
use crate::{AgentBoundaryErrorCode, AgentManifestInspectionResponseV1, AgentSourceFilesV1};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSourceInspectionRequestV1 {
    pub schema_version: u32,
    pub sources: AgentSourceFilesV1,
}

/// Validate every fact decidable from the captured source files, without resolving dependencies.
pub fn inspect_source_files(
    sources: &AgentSourceFilesV1,
) -> Result<crate::AgentManifestResolution, AgentBoundaryErrorCode> {
    sources.validate()?;
    let manifest = &sources.files[&sources.manifest_path];
    let resolution = crate::inspect_manifest(manifest.as_bytes())
        .map_err(crate::boundary::compiler_error_code)?;
    let mut expected = vec![
        sources.manifest_path.as_str(),
        resolution.input_schema_path.as_str(),
        resolution.output_schema_path.as_str(),
    ];
    if let Some(path) = &resolution.plan_path {
        expected.push(path);
    }
    if sources
        .files
        .keys()
        .any(|path| !expected.contains(&path.as_str()))
    {
        return Err(AgentBoundaryErrorCode::SourceBundleInvalid);
    }
    let source = |path: &str| {
        sources
            .files
            .get(path)
            .ok_or(AgentBoundaryErrorCode::SourceReferenceMissing)
    };
    let input = crate::compile_schema(
        source(&resolution.input_schema_path)?.as_bytes(),
        "input schema",
    )
    .map_err(crate::boundary::compiler_error_code)?;
    let output = crate::compile_schema(
        source(&resolution.output_schema_path)?.as_bytes(),
        "output schema",
    )
    .map_err(crate::boundary::compiler_error_code)?;
    match resolution.execution_kind {
        crate::AgentExecutionKind::Deterministic => {
            if input.canonical_digest != output.canonical_digest {
                return Err(AgentBoundaryErrorCode::AgentCompileFailed);
            }
        }
        crate::AgentExecutionKind::ModelChat => {}
        crate::AgentExecutionKind::FullPlan | crate::AgentExecutionKind::FrameworkGraph => {
            let path = resolution
                .plan_path
                .as_ref()
                .ok_or(AgentBoundaryErrorCode::SourceReferenceMissing)?;
            let contract = crate::agent_interface_contract_digest(&input, &output)
                .map_err(crate::boundary::compiler_error_code)?;
            let mut plan = crate::parse_authored_plan(
                source(path)?.as_bytes(),
                resolution.execution_kind,
                &contract,
            )
            .map_err(crate::boundary::compiler_error_code)?;
            let error =
                crate::compiler_error_schema().map_err(crate::boundary::compiler_error_code)?;
            crate::validate_authored_plan(&mut plan, &input, &output, &error)
                .map_err(crate::boundary::compiler_error_code)?;
        }
    }
    Ok(resolution)
}

pub fn inspect_agent_sources(
    request: AgentSourceInspectionRequestV1,
) -> AgentManifestInspectionResponseV1 {
    let result = if request.schema_version != 1 {
        Err(AgentBoundaryErrorCode::AuthoringVersionUnsupported)
    } else {
        inspect_source_files(&request.sources)
    };
    match result {
        Ok(resolution) => AgentManifestInspectionResponseV1::Inspected {
            schema_version: 1,
            resolution,
        },
        Err(code) => inspection_rejected(code, Some(&request.sources)),
    }
}
fn inspection_rejected(
    code: AgentBoundaryErrorCode,
    sources: Option<&AgentSourceFilesV1>,
) -> AgentManifestInspectionResponseV1 {
    let crate::AgentCompileResponseV1::Rejected { mut diagnostics } =
        crate::boundary::rejection(code)
    else {
        unreachable!()
    };
    if let Some(sources) = sources {
        diagnostics[0].location = crate::source_map::diagnostic_location(sources, code);
    }
    AgentManifestInspectionResponseV1::Rejected { diagnostics }
}
pub fn inspect_source_request_bytes(input: &[u8]) -> Vec<u8> {
    let response = match crate::boundary::parse_boundary::<AgentSourceInspectionRequestV1>(input) {
        Ok(request) => inspect_agent_sources(request),
        Err(code) => inspection_rejected(code, None),
    };
    let bytes = serde_json::to_vec(&response).expect("source inspection is serializable");
    if bytes.len() > crate::MAX_AGENT_COMPILER_RESPONSE_BYTES {
        return serde_json::to_vec(&inspection_rejected(
            AgentBoundaryErrorCode::CompilerLimitExceeded,
            None,
        ))
        .expect("bounded rejection");
    }
    bytes
}

/// Generated editor wire contract; aggregate UTF-8 budgets remain enforced by the owner.
pub fn agent_source_inspection_request_schema() -> serde_json::Value {
    use serde_json::json;
    json!({"$schema":"https://json-schema.org/draft/2020-12/schema","title":"AgentSourceInspectionRequestV1","type":"object","additionalProperties":false,"required":["schema_version","sources"],"properties":{
        "schema_version":{"const":1},"sources":{"type":"object","additionalProperties":false,"required":["manifest_path","files"],"properties":{
            "manifest_path":{"type":"string","minLength":1,"maxLength":crate::MAX_AGENT_SOURCE_PATH_BYTES},
            "files":{"type":"object","minProperties":1,"maxProperties":crate::MAX_AGENT_SOURCE_FILES,"propertyNames":{"type":"string","minLength":1,"maxLength":crate::MAX_AGENT_SOURCE_PATH_BYTES},"additionalProperties":{"type":"string","maxLength":crate::MAX_AGENT_SOURCE_FILE_BYTES}}
        }}
    }})
}
pub fn agent_source_inspection_response_schema() -> serde_json::Value {
    use serde_json::json;
    let path =
        json!({"type":"string","minLength":1,"maxLength":crate::MAX_AGENT_SOURCE_PATH_BYTES});
    let resolution = json!({"type":"object","additionalProperties":false,"required":["execution_kind","model_ref","input_schema_path","output_schema_path","plan_path"],"properties":{
        "execution_kind":{"enum":[crate::AgentExecutionKind::Deterministic,crate::AgentExecutionKind::ModelChat,crate::AgentExecutionKind::FullPlan,crate::AgentExecutionKind::FrameworkGraph]},
        "model_ref":{"type":["string","null"],"maxLength":256},"input_schema_path":path,"output_schema_path":path,"plan_path":{"anyOf":[path,{"type":"null"}]}
    }});
    let location = json!({"type":"object","additionalProperties":false,"required":["file","source_pointer","line","column"],"properties":{"file":path,"source_pointer":{"type":"string","maxLength":crate::MAX_AGENT_SOURCE_POINTER_BYTES},"line":{"type":"integer","minimum":1,"maximum":crate::MAX_AGENT_SOURCE_FILE_BYTES},"column":{"type":"integer","minimum":1,"maximum":crate::MAX_AGENT_SOURCE_FILE_BYTES}}});
    json!({"$schema":"https://json-schema.org/draft/2020-12/schema","title":"AgentSourceInspectionResponseV1","oneOf":[
        {"type":"object","additionalProperties":false,"required":["outcome","schema_version","resolution"],"properties":{"outcome":{"const":"inspected"},"schema_version":{"const":1},"resolution":resolution}},
        {"type":"object","additionalProperties":false,"required":["outcome","diagnostics"],"properties":{"outcome":{"const":"rejected"},"diagnostics":{"type":"array","minItems":1,"maxItems":crate::MAX_AGENT_COMPILER_DIAGNOSTICS,"items":{"type":"object","additionalProperties":false,"required":["code","location","safe_detail"],"properties":{"code":{"enum":AgentBoundaryErrorCode::ALL},"location":{"anyOf":[location,{"type":"null"}]},"safe_detail":{"type":"string","maxLength":2048}}}}}}
    ]})
}
