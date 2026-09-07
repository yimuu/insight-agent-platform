//! Local file-system adapter for the shared, pure Agent compiler.
use insight_platform_agent_compiler::{AgentCompilerError, MAX_AGENT_MANIFEST_BYTES};
use insight_platform_contracts::MAX_CLOSED_SCHEMA_BYTES;
use std::{
    fs,
    path::{Component, Path, PathBuf},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedAgentProject {
    pub manifest_path: PathBuf,
    pub plan_bytes: Option<Vec<u8>>,
    pub manifest_bytes: Vec<u8>,
    pub input_schema_bytes: Vec<u8>,
    pub output_schema_bytes: Vec<u8>,
}

pub fn load_project_sources(
    project_root: &Path,
    manifest_path: &Path,
) -> Result<LoadedAgentProject, AgentCompilerError> {
    let canonical_root = fs::canonicalize(project_root).map_err(|error| {
        AgentCompilerError::reference(format!("cannot resolve project root: {error}"))
    })?;
    if !canonical_root.is_dir() {
        return Err(AgentCompilerError::reference(
            "project root is not a directory",
        ));
    }
    let canonical_manifest = safe_project_file(
        &canonical_root,
        manifest_path,
        MAX_AGENT_MANIFEST_BYTES,
        "manifest",
    )?;
    let manifest_bytes =
        read_bounded_file(&canonical_manifest, MAX_AGENT_MANIFEST_BYTES, "manifest")?;
    let manifest = insight_platform_agent_compiler::inspect_manifest(&manifest_bytes)?;
    let input_path = Path::new(&manifest.input_schema_path);
    let output_path = Path::new(&manifest.output_schema_path);
    let canonical_input = safe_project_file(
        &canonical_root,
        input_path,
        MAX_CLOSED_SCHEMA_BYTES,
        "input schema",
    )?;
    let canonical_output = safe_project_file(
        &canonical_root,
        output_path,
        MAX_CLOSED_SCHEMA_BYTES,
        "output schema",
    )?;
    let plan_bytes = manifest
        .plan_path
        .as_deref()
        .map(|relative| {
            let path = safe_project_file(
                &canonical_root,
                Path::new(relative),
                MAX_AGENT_MANIFEST_BYTES,
                "full Plan",
            )?;
            read_bounded_file(&path, MAX_AGENT_MANIFEST_BYTES, "full Plan")
        })
        .transpose()?;
    Ok(LoadedAgentProject {
        plan_bytes,
        manifest_path: canonical_manifest,
        manifest_bytes,
        input_schema_bytes: read_bounded_file(
            &canonical_input,
            MAX_CLOSED_SCHEMA_BYTES,
            "input schema",
        )?,
        output_schema_bytes: read_bounded_file(
            &canonical_output,
            MAX_CLOSED_SCHEMA_BYTES,
            "output schema",
        )?,
    })
}

fn safe_project_file(
    canonical_root: &Path,
    relative: &Path,
    maximum_bytes: usize,
    name: &str,
) -> Result<PathBuf, AgentCompilerError> {
    let rendered = relative.to_string_lossy();
    insight_platform_agent_compiler::validate_relative_reference(&rendered, name)?;
    let mut candidate = canonical_root.to_path_buf();
    for component in relative.components() {
        match component {
            Component::Normal(part) => candidate.push(part),
            Component::CurDir => continue,
            _ => {
                return Err(AgentCompilerError::reference(format!(
                    "{name} path escapes the project root"
                )))
            }
        }
        let metadata = fs::symlink_metadata(&candidate).map_err(|error| {
            AgentCompilerError::reference(format!("cannot inspect {name}: {error}"))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(AgentCompilerError::reference(format!(
                "{name} path contains a symbolic link"
            )));
        }
    }
    let canonical = fs::canonicalize(&candidate).map_err(|error| {
        AgentCompilerError::reference(format!("cannot resolve {name}: {error}"))
    })?;
    let metadata = fs::metadata(&canonical).map_err(|error| {
        AgentCompilerError::reference(format!("cannot inspect {name}: {error}"))
    })?;
    if !canonical.starts_with(canonical_root)
        || !metadata.is_file()
        || metadata.len() > u64::try_from(maximum_bytes).unwrap_or(u64::MAX)
    {
        return Err(AgentCompilerError::reference(format!(
            "{name} is outside the project, not a file, or exceeds its byte limit"
        )));
    }
    Ok(canonical)
}

fn read_bounded_file(
    path: &Path,
    maximum_bytes: usize,
    name: &str,
) -> Result<Vec<u8>, AgentCompilerError> {
    let bytes = fs::read(path)
        .map_err(|error| AgentCompilerError::reference(format!("cannot read {name}: {error}")))?;
    if bytes.len() > maximum_bytes {
        return Err(AgentCompilerError::reference(format!(
            "{name} exceeds its byte limit"
        )));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_agent_compiler::AgentCompilerErrorCode;
    use tempfile::tempdir;
    const SCHEMA: &str = r#"{
        "$schema":"https://json-schema.org/draft/2020-12/schema",
        "type":"object",
        "properties":{"message":{"type":"string","minLength":1,"maxLength":128,"x-platform-max-bytes":512}},
        "required":["message"],
        "additionalProperties":false
    }"#;
    fn deterministic_yaml() -> Vec<u8> {
        br#"apiVersion: insight.platform/v1
kind: Agent
metadata:
  name: echo-agent
spec:
  execution:
    kind: deterministic
  input:
    schema: schemas/io.json
    classification: internal
  output:
    schema: schemas/io.json
"#
        .to_vec()
    }
    #[test]
    fn project_loader_rejects_parent_absolute_and_symlink_paths() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("schemas")).unwrap();
        fs::write(directory.path().join("agent.yaml"), deterministic_yaml()).unwrap();
        fs::write(directory.path().join("schemas/io.json"), SCHEMA).unwrap();
        let loaded = load_project_sources(directory.path(), Path::new("agent.yaml")).unwrap();
        assert_eq!(loaded.input_schema_bytes, SCHEMA.as_bytes());

        assert_eq!(
            load_project_sources(directory.path(), Path::new("../agent.yaml"))
                .unwrap_err()
                .code(),
            AgentCompilerErrorCode::AgentManifestInvalid
        );
        assert_eq!(
            load_project_sources(directory.path(), &directory.path().join("agent.yaml"))
                .unwrap_err()
                .code(),
            AgentCompilerErrorCode::AgentManifestInvalid
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            fs::remove_file(directory.path().join("schemas/io.json")).unwrap();
            let outside = tempdir().unwrap();
            fs::write(outside.path().join("io.json"), SCHEMA).unwrap();
            symlink(
                outside.path().join("io.json"),
                directory.path().join("schemas/io.json"),
            )
            .unwrap();
            assert_eq!(
                load_project_sources(directory.path(), Path::new("agent.yaml"))
                    .unwrap_err()
                    .code(),
                AgentCompilerErrorCode::AgentReferenceMissing
            );
        }
    }
}
