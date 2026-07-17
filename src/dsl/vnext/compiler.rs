use std::{collections::BTreeMap, fmt, fs, path::Path, sync::Arc};

use sha2::{Digest, Sha256};

use crate::{
    dsl::CompileError,
    resources::{actions::ActionRegistry, models::ModelRegistry},
    schema::{compile_schema_2020, JsonSchemaValidator},
};

use super::{
    chat::ChatOperation,
    ir::WorkflowIr,
    lower::{lower_workflow, CallContractResolver, ResolvedCallContract},
    operation::{ActionCallOperation, OperationRegistry},
    raw::{parse_workflow, PromptDeclaration, RawWorkflow},
    value::Identifier,
};

pub const MAX_AGENT_SOURCE_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_PROMPT_BYTES: usize = 1024 * 1024;
pub const MAX_TOTAL_PROMPT_BYTES: usize = 4 * 1024 * 1024;

/// Fully compiled, self-contained vNext Agent. File-backed prompts have already
/// been resolved; runtime execution performs no DSL parsing or filesystem I/O.
#[derive(Clone)]
pub struct CompiledWorkflow {
    pub version_hash: String,
    pub ir: Arc<WorkflowIr>,
    operations: OperationRegistry,
    input_validator: Arc<JsonSchemaValidator>,
    output_validator: Arc<JsonSchemaValidator>,
}

impl CompiledWorkflow {
    pub fn input_validator(&self) -> &JsonSchemaValidator {
        &self.input_validator
    }

    pub fn output_validator(&self) -> &JsonSchemaValidator {
        &self.output_validator
    }

    pub(crate) fn operations(&self) -> &OperationRegistry {
        &self.operations
    }

    pub(crate) fn input_validator_arc(&self) -> Arc<JsonSchemaValidator> {
        Arc::clone(&self.input_validator)
    }

    pub(crate) fn output_validator_arc(&self) -> Arc<JsonSchemaValidator> {
        Arc::clone(&self.output_validator)
    }
}

impl fmt::Debug for CompiledWorkflow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompiledWorkflow")
            .field("id", &self.ir.metadata.id)
            .field("version_hash", &self.version_hash)
            .field(
                "operation_uses",
                &self.operations.names().collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct WorkflowCompiler {
    models: ModelRegistry,
    actions: ActionRegistry,
    extensions: OperationRegistry,
}

impl WorkflowCompiler {
    pub fn new(models: ModelRegistry, actions: ActionRegistry) -> Self {
        Self {
            models,
            actions,
            extensions: OperationRegistry::default(),
        }
    }

    pub fn with_extensions(
        models: ModelRegistry,
        actions: ActionRegistry,
        extensions: OperationRegistry,
    ) -> Self {
        Self {
            models,
            actions,
            extensions,
        }
    }

    pub fn compile_dir(&self, root: &Path) -> Result<CompiledWorkflow, CompileError> {
        let path = root.join("agent.yaml");
        let metadata = fs::metadata(&path).map_err(|_| {
            CompileError::new(
                "VNEXT_AGENT_READ_FAILED",
                "failed to read the vNext agent document",
            )
        })?;
        if metadata.len() > MAX_AGENT_SOURCE_BYTES as u64 {
            return Err(CompileError::new(
                "VNEXT_AGENT_SOURCE_TOO_LARGE",
                "vNext agent document exceeds its byte limit",
            ));
        }
        let source = fs::read_to_string(path).map_err(|_| {
            CompileError::new(
                "VNEXT_AGENT_READ_FAILED",
                "failed to read the vNext agent document",
            )
        })?;
        self.compile_source(root, &source)
    }

    pub fn compile_source(
        &self,
        root: &Path,
        source: &str,
    ) -> Result<CompiledWorkflow, CompileError> {
        if source.len() > MAX_AGENT_SOURCE_BYTES {
            return Err(CompileError::new(
                "VNEXT_AGENT_SOURCE_TOO_LARGE",
                "vNext agent document exceeds its byte limit",
            ));
        }
        let raw = parse_workflow(source).map_err(|_| {
            CompileError::new(
                "VNEXT_AGENT_PARSE_FAILED",
                "failed to parse the vNext agent document",
            )
        })?;
        self.compile_raw(root, raw)
    }

    pub fn compile_raw(
        &self,
        root: &Path,
        mut raw: RawWorkflow,
    ) -> Result<CompiledWorkflow, CompileError> {
        let prompts = resolve_prompts(root, &raw.prompts)?;
        raw.prompts = prompts
            .iter()
            .map(|(name, text)| (name.clone(), PromptDeclaration::Inline(text.clone())))
            .collect();

        let mut operations = self.extensions.clone();
        operations
            .register(ActionCallOperation::new(self.actions.clone()))
            .map_err(operation_registration_error)?;
        operations
            .register(ChatOperation::new(
                self.models.clone(),
                raw.definitions.clone(),
                prompts,
            ))
            .map_err(operation_registration_error)?;

        let resolver = RegistryResolver(&operations);
        let ir = lower_workflow(&raw, &resolver).map_err(|errors| {
            let error = errors
                .first()
                .expect("lowering returns at least one error when it fails");
            CompileError::new(error.code(), error.message())
        })?;
        let input_validator = compile_schema_2020(&ir.input.schema).map_err(|_| {
            CompileError::new(
                "VNEXT_INPUT_SCHEMA_INVALID",
                "compiled vNext input schema is invalid",
            )
        })?;
        let output_validator = compile_schema_2020(&ir.output.schema).map_err(|_| {
            CompileError::new(
                "VNEXT_OUTPUT_SCHEMA_INVALID",
                "compiled vNext output schema is invalid",
            )
        })?;
        let version_hash = workflow_hash(&raw)?;

        Ok(CompiledWorkflow {
            version_hash,
            ir: Arc::new(ir),
            operations,
            input_validator: Arc::new(input_validator),
            output_validator: Arc::new(output_validator),
        })
    }
}

struct RegistryResolver<'a>(&'a OperationRegistry);

impl CallContractResolver for RegistryResolver<'_> {
    fn resolve_call(
        &self,
        uses: &str,
        config: &serde_json::Value,
        inputs: &BTreeMap<Identifier, super::types::ValueType>,
    ) -> Result<ResolvedCallContract, String> {
        let operation = self
            .0
            .resolve(uses)
            .map_err(|error| error.code().to_string())?;
        let contract = operation
            .compile(config, inputs)
            .map_err(|error| error.code().to_string())?;
        Ok(ResolvedCallContract {
            output_schema: contract.output_schema,
            output_type: contract.output_type,
        })
    }
}

fn operation_registration_error(error: super::operation::OperationError) -> CompileError {
    CompileError::new(error.code(), error.message())
}

fn resolve_prompts(
    root: &Path,
    declarations: &BTreeMap<Identifier, PromptDeclaration>,
) -> Result<BTreeMap<Identifier, String>, CompileError> {
    let canonical_root = root.canonicalize().map_err(|_| {
        CompileError::new(
            "VNEXT_AGENT_PATH_INVALID",
            "vNext agent directory is invalid",
        )
    })?;
    let prompts = declarations
        .iter()
        .map(|(name, declaration)| {
            let text = match declaration {
                PromptDeclaration::Inline(text) => text.clone(),
                PromptDeclaration::File(relative) => {
                    let path = root.join(relative);
                    let canonical = path.canonicalize().map_err(|_| {
                        CompileError::new(
                            "VNEXT_PROMPT_READ_FAILED",
                            "failed to resolve a vNext prompt file",
                        )
                    })?;
                    if !canonical.starts_with(&canonical_root) {
                        return Err(CompileError::new(
                            "VNEXT_PROMPT_PATH_ESCAPE",
                            "vNext prompt file must stay inside the agent directory",
                        ));
                    }
                    let metadata = fs::metadata(&canonical).map_err(|_| {
                        CompileError::new(
                            "VNEXT_PROMPT_READ_FAILED",
                            "failed to read a vNext prompt file",
                        )
                    })?;
                    if metadata.len() > MAX_PROMPT_BYTES as u64 {
                        return Err(CompileError::new(
                            "VNEXT_PROMPT_TOO_LARGE",
                            "vNext prompt exceeds its byte limit",
                        ));
                    }
                    fs::read_to_string(canonical).map_err(|_| {
                        CompileError::new(
                            "VNEXT_PROMPT_READ_FAILED",
                            "failed to read a vNext prompt file",
                        )
                    })?
                }
            };
            if text.len() > MAX_PROMPT_BYTES {
                return Err(CompileError::new(
                    "VNEXT_PROMPT_TOO_LARGE",
                    "vNext prompt exceeds its byte limit",
                ));
            }
            Ok((name.clone(), text))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    if prompts
        .values()
        .try_fold(0usize, |total, prompt| total.checked_add(prompt.len()))
        .is_none_or(|total| total > MAX_TOTAL_PROMPT_BYTES)
    {
        return Err(CompileError::new(
            "VNEXT_PROMPTS_TOO_LARGE",
            "vNext prompts exceed their aggregate byte limit",
        ));
    }
    Ok(prompts)
}

fn workflow_hash(raw: &RawWorkflow) -> Result<String, CompileError> {
    use std::fmt::Write as _;

    let bytes = serde_json::to_vec(raw).map_err(|_| {
        CompileError::new(
            "VNEXT_AGENT_HASH_FAILED",
            "failed to normalize the vNext agent document",
        )
    })?;
    let digest = Sha256::digest(bytes);
    let mut hash = String::with_capacity("sha256:".len() + digest.len() * 2);
    hash.push_str("sha256:");
    for byte in digest {
        write!(&mut hash, "{byte:02x}").expect("writing to a string cannot fail");
    }
    Ok(hash)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::WorkflowCompiler;
    use crate::resources::{actions::ActionRegistry, models::ModelRegistry};

    const SOURCE: &str = r#"
api_version: insight.agent/v2
kind: agent
metadata: {id: fixture, name: Fixture}
schema_dialect: https://json-schema.org/draft/2020-12/schema
prompts:
  system: {file: prompts/system.md}
input:
  schema:
    type: object
    required: [question]
    properties: {question: {type: string}}
    additionalProperties: false
output:
  data_schema:
    type: object
    required: [answer]
    properties: {answer: {type: string}}
    additionalProperties: false
workflow:
  result:
    return:
      data:
        object:
          answer: {from: input.question}
"#;

    #[test]
    fn compiles_one_self_contained_workflow_and_hashes_resolved_prompts() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("prompts")).unwrap();
        fs::write(directory.path().join("agent.yaml"), SOURCE).unwrap();
        fs::write(directory.path().join("prompts/system.md"), "first").unwrap();
        let compiler = WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default());

        let first = compiler.compile_dir(directory.path()).unwrap();
        assert!(first.input_validator().is_valid(&serde_json::json!({
            "question":"answer"
        })));
        assert!(first
            .output_validator()
            .is_valid(&serde_json::json!({"answer":"answer"})));
        assert_eq!(first.ir.prompts.values().next().unwrap().text, "first");

        fs::write(directory.path().join("prompts/system.md"), "second").unwrap();
        let second = compiler.compile_dir(directory.path()).unwrap();
        assert_ne!(first.version_hash, second.version_hash);
    }

    #[test]
    fn rejects_prompt_path_escape_without_reading_outside_content() {
        let parent = tempdir().unwrap();
        let directory = parent.path().join("agent");
        fs::create_dir(&directory).unwrap();
        fs::write(parent.path().join("secret.md"), "do-not-leak").unwrap();
        let source = SOURCE.replace("prompts/system.md", "../secret.md");
        let error = WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default())
            .compile_source(&directory, &source)
            .unwrap_err();
        assert_eq!(error.code(), "VNEXT_PROMPT_PATH_ESCAPE");
        assert!(!error.to_string().contains("do-not-leak"));
    }

    #[test]
    fn rejects_oversized_source_and_prompt_before_parsing_or_loading_them() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("prompts")).unwrap();
        fs::write(
            directory.path().join("prompts/system.md"),
            vec![b'x'; super::MAX_PROMPT_BYTES + 1],
        )
        .unwrap();
        let compiler = WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default());
        let error = compiler
            .compile_source(directory.path(), SOURCE)
            .unwrap_err();
        assert_eq!(error.code(), "VNEXT_PROMPT_TOO_LARGE");

        let oversized = "x".repeat(super::MAX_AGENT_SOURCE_BYTES + 1);
        let error = compiler
            .compile_source(directory.path(), &oversized)
            .unwrap_err();
        assert_eq!(error.code(), "VNEXT_AGENT_SOURCE_TOO_LARGE");
    }
}
