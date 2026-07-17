use std::{collections::BTreeMap, fmt, fs, path::Path, sync::Arc};

use handlebars::{
    template::{BlockParam, HelperTemplate, Parameter, TemplateElement},
    Template,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    dsl::{CompileError, DslPath, SourceSpan},
    resources::{actions::ActionRegistry, models::ModelRegistry},
    schema::{compile_schema_2020, JsonSchemaValidator},
};

use super::{
    chat::ChatOperation,
    ir::{OperationKind, Region, TypedContract, WorkflowIr},
    lower::{lower_workflow, ResolvedActionContract, ResolvedModelContract, ResourceResolver},
    operation::{ActionCallOperation, OperationRegistry},
    plan::{CallPlan, PlannedTemplate, ResolvedModelId, TemplateProfileVersion},
    raw::{
        parse_workflow, ParallelBranch, PromptDeclaration, RawWorkflow, SpannedRawDocument, Step,
        SwitchCase, SwitchDefault, WorkflowBody,
    },
    schema::compile_contract_schema,
    template::{compile_template, CompiledTemplate, TemplateAccessKind, TemplatePathSegment},
    types::ValueType,
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
}

impl WorkflowCompiler {
    pub fn new(models: ModelRegistry, actions: ActionRegistry) -> Self {
        Self { models, actions }
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
        let document = parse_workflow(source).map_err(CompileError::from)?;
        self.compile_spanned(root, document)
    }

    pub fn compile_raw(
        &self,
        root: &Path,
        raw: RawWorkflow,
    ) -> Result<CompiledWorkflow, CompileError> {
        let diagnostics = CompileDiagnosticContext::new(&raw);
        self.compile_raw_inner(root, raw)
            .map_err(|error| diagnostics.decorate(error))
    }

    fn compile_spanned(
        &self,
        root: &Path,
        document: SpannedRawDocument,
    ) -> Result<CompiledWorkflow, CompileError> {
        let (raw, source_map) = document.into_parts();
        let diagnostics = CompileDiagnosticContext::new(&raw);
        self.compile_raw_inner(root, raw)
            .map_err(|error| diagnostics.decorate(attach_source_span(error, &source_map)))
    }

    fn compile_raw_inner(
        &self,
        root: &Path,
        mut raw: RawWorkflow,
    ) -> Result<CompiledWorkflow, CompileError> {
        let prompts = resolve_prompts(root, &raw.prompts)?;
        raw.prompts = prompts
            .iter()
            .map(|(name, text)| (name.clone(), PromptDeclaration::Inline(text.clone())))
            .collect();

        let mut operations = OperationRegistry::default();
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

        let resolver = RegistryResolver {
            models: &self.models,
            actions: &self.actions,
        };
        let ir = lower_workflow(&raw, &resolver).map_err(|errors| {
            let error = errors
                .first()
                .expect("lowering returns at least one error when it fails");
            let exposed_code = if error.code() == super::lower::LOWER_SEMANTIC_INVALID {
                error.cause_code().unwrap_or(error.code())
            } else {
                error.code()
            };
            let compile_error = CompileError::new(exposed_code, error.message());
            let compile_error = error
                .decoded_template_span()
                .map_or(compile_error.clone(), |span| {
                    compile_error.with_decoded_template_span(span)
                });
            error.location().map_or(compile_error.clone(), |location| {
                compile_error.with_path(authored_location_to_dsl_path(&raw, location))
            })
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
        let version_hash = workflow_hash(&raw, &ir)?;

        Ok(CompiledWorkflow {
            version_hash,
            ir: Arc::new(ir),
            operations,
            input_validator: Arc::new(input_validator),
            output_validator: Arc::new(output_validator),
        })
    }
}

struct CompileDiagnosticContext {
    agent_id: String,
    step_owners: Vec<(DslPath, String)>,
}

impl CompileDiagnosticContext {
    fn new(raw: &RawWorkflow) -> Self {
        let mut step_owners = Vec::new();
        collect_step_owners(
            &raw.workflow.steps,
            &DslPath::root().child_key("workflow"),
            &mut step_owners,
        );
        Self {
            agent_id: raw.metadata.id.as_str().to_string(),
            step_owners,
        }
    }

    fn decorate(&self, error: CompileError) -> CompileError {
        let step_id = error.path().and_then(|path| {
            self.step_owners
                .iter()
                .filter(|(step_path, _)| path.segments().starts_with(step_path.segments()))
                .max_by_key(|(step_path, _)| step_path.segments().len())
                .map(|(_, step_id)| step_id.clone())
        });
        let error = error.with_agent_id(&self.agent_id);
        step_id.map_or(error.clone(), |step_id| error.with_step_id(step_id))
    }
}

fn collect_step_owners(
    steps: &[Step],
    authored_region_path: &DslPath,
    owners: &mut Vec<(DslPath, String)>,
) {
    for (step_index, step) in steps.iter().enumerate() {
        let step_path = authored_region_path
            .child_key("steps")
            .child_index(step_index);
        owners.push((step_path.clone(), step_id(step).as_str().to_string()));
        match step {
            Step::Parallel { branches, .. } => {
                for (name, branch) in branches {
                    collect_step_owners(
                        &branch.steps,
                        &step_path.child_key("branches").child_key(name.as_str()),
                        owners,
                    );
                }
            }
            Step::Switch { cases, default, .. } => {
                for (case_index, case) in cases.iter().enumerate() {
                    collect_step_owners(
                        &case.steps,
                        &step_path.child_key("cases").child_index(case_index),
                        owners,
                    );
                }
                collect_step_owners(&default.steps, &step_path.child_key("default"), owners);
            }
            Step::Llm { .. } | Step::Action { .. } => {}
        }
    }
}

fn attach_source_span(
    error: CompileError,
    source_map: &BTreeMap<DslPath, SourceSpan>,
) -> CompileError {
    if error.span().is_some() {
        return error;
    }
    let Some(span) = error
        .path()
        .and_then(|path| nearest_source_span(source_map, path))
    else {
        return error;
    };
    error.with_span(span)
}

fn nearest_source_span(
    source_map: &BTreeMap<DslPath, SourceSpan>,
    path: &DslPath,
) -> Option<SourceSpan> {
    (0..=path.segments().len()).rev().find_map(|length| {
        let ancestor = DslPath::from_segments(path.segments()[..length].iter().cloned());
        source_map.get(&ancestor).copied()
    })
}

fn authored_location_to_dsl_path(raw: &RawWorkflow, location: &str) -> DslPath {
    let tokens = if location.starts_with('/') {
        location
            .split('/')
            .filter(|token| !token.is_empty())
            .collect::<Vec<_>>()
    } else {
        location
            .split('.')
            .filter(|token| !token.is_empty())
            .collect::<Vec<_>>()
    };
    let mut path = DslPath::root();
    let mut cursor = LocationCursor::Root;
    for (token_index, token) in tokens.iter().copied().enumerate() {
        cursor = match cursor {
            LocationCursor::Root if token == "workflow" => {
                path = path.child_key("workflow");
                LocationCursor::Workflow(&raw.workflow)
            }
            LocationCursor::Root => {
                path = append_location_token(path, token);
                LocationCursor::Generic
            }
            LocationCursor::Workflow(workflow) if token == "steps" => {
                path = path.child_key("steps");
                LocationCursor::Steps(&workflow.steps)
            }
            LocationCursor::Workflow(_) if token == "result" => {
                path = path.child_key("result");
                LocationCursor::Generic
            }
            LocationCursor::Workflow(workflow) => match find_step(&workflow.steps, token) {
                Some((index, step)) => {
                    path = path.child_key("steps").child_index(index);
                    LocationCursor::Step(step)
                }
                None => {
                    path = append_location_token(path, token);
                    LocationCursor::Generic
                }
            },
            LocationCursor::Steps(steps) => match find_step(steps, token) {
                Some((index, step)) => {
                    path = path.child_index(index);
                    LocationCursor::Step(step)
                }
                None => {
                    path = append_location_token(path, token);
                    LocationCursor::Generic
                }
            },
            LocationCursor::Step(Step::Parallel { branches, .. }) if token == "branches" => {
                path = path.child_key("branches");
                LocationCursor::Branches(branches)
            }
            LocationCursor::Step(Step::Switch { cases, .. }) if token == "cases" => {
                path = path.child_key("cases");
                LocationCursor::Cases(cases)
            }
            LocationCursor::Step(Step::Switch { default, .. }) if token == "default" => {
                path = path.child_key("default");
                LocationCursor::Default(default)
            }
            LocationCursor::Step(_) => {
                path = append_location_token(path, token);
                LocationCursor::Generic
            }
            LocationCursor::Branches(branches) => {
                match branches
                    .iter()
                    .find(|(name, _)| name.as_str() == stable_identity_token(token))
                {
                    Some((name, branch)) => {
                        path = path.child_key(name.as_str());
                        LocationCursor::Branch(branch)
                    }
                    None => {
                        path = append_location_token(path, token);
                        LocationCursor::Generic
                    }
                }
            }
            LocationCursor::Branch(branch) if token == "steps" => {
                path = path.child_key("steps");
                LocationCursor::Steps(&branch.steps)
            }
            LocationCursor::Branch(_) if token == "result" => {
                path = path.child_key("result");
                LocationCursor::Generic
            }
            LocationCursor::Branch(branch) => match find_step(&branch.steps, token) {
                Some((index, step)) => {
                    path = path.child_key("steps").child_index(index);
                    LocationCursor::Step(step)
                }
                None => {
                    path = append_location_token(path, token);
                    LocationCursor::Generic
                }
            },
            LocationCursor::Cases(cases) => match cases
                .iter()
                .enumerate()
                .rfind(|(_, case)| case.id.as_str() == stable_identity_token(token))
            {
                Some((index, case)) => {
                    path = path.child_index(index);
                    LocationCursor::Case(case)
                }
                None => {
                    path = append_location_token(path, token);
                    LocationCursor::Generic
                }
            },
            LocationCursor::Case(case) if token == "steps" => {
                path = path.child_key("steps");
                LocationCursor::Steps(&case.steps)
            }
            LocationCursor::Case(_) if token == "result" => {
                path = path.child_key("result");
                LocationCursor::Generic
            }
            LocationCursor::Case(case) => match find_step(&case.steps, token) {
                Some((index, step)) => {
                    path = path.child_key("steps").child_index(index);
                    LocationCursor::Step(step)
                }
                None => {
                    path = append_location_token(path, token);
                    LocationCursor::Generic
                }
            },
            LocationCursor::Default(default)
                if default.id.as_str() == stable_identity_token(token) =>
            {
                if token_index + 1 == tokens.len() {
                    path = path.child_key("id");
                }
                LocationCursor::Default(default)
            }
            LocationCursor::Default(default) if token == "steps" => {
                path = path.child_key("steps");
                LocationCursor::Steps(&default.steps)
            }
            LocationCursor::Default(_) if token == "result" => {
                path = path.child_key("result");
                LocationCursor::Generic
            }
            LocationCursor::Default(default) => match find_step(&default.steps, token) {
                Some((index, step)) => {
                    path = path.child_key("steps").child_index(index);
                    LocationCursor::Step(step)
                }
                None => {
                    path = append_location_token(path, token);
                    LocationCursor::Generic
                }
            },
            LocationCursor::Generic => {
                path = append_location_token(path, token);
                LocationCursor::Generic
            }
        };
    }
    path
}

#[derive(Clone, Copy)]
enum LocationCursor<'a> {
    Root,
    Workflow(&'a WorkflowBody),
    Steps(&'a [Step]),
    Step(&'a Step),
    Branches(&'a BTreeMap<super::value::Identifier, ParallelBranch>),
    Branch(&'a ParallelBranch),
    Cases(&'a [SwitchCase]),
    Case(&'a SwitchCase),
    Default(&'a SwitchDefault),
    Generic,
}

fn find_step<'a>(steps: &'a [Step], token: &str) -> Option<(usize, &'a Step)> {
    let token = stable_identity_token(token);
    steps
        .iter()
        .enumerate()
        .rfind(|(_, step)| step_id(step).as_str() == token)
}

fn step_id(step: &Step) -> &super::value::Identifier {
    match step {
        Step::Llm { id, .. }
        | Step::Action { id, .. }
        | Step::Parallel { id, .. }
        | Step::Switch { id, .. } => id,
    }
}

fn stable_identity_token(token: &str) -> &str {
    token.split(['#', '@']).next().unwrap_or(token)
}

fn append_location_token(path: DslPath, token: &str) -> DslPath {
    token
        .parse::<usize>()
        .map_or_else(|_| path.child_key(token), |index| path.child_index(index))
}

struct RegistryResolver<'a> {
    models: &'a ModelRegistry,
    actions: &'a ActionRegistry,
}

impl ResourceResolver for RegistryResolver<'_> {
    fn resolve_action(&self, action_id: &str) -> Result<ResolvedActionContract, String> {
        let action = self
            .actions
            .resolve(action_id)
            .map_err(|error| error.code().to_string())?;
        let descriptor = action.descriptor();
        Ok(ResolvedActionContract {
            identity: action.identity().clone(),
            input: compile_resource_contract(&descriptor.input_schema)?,
            output: compile_resource_contract(&descriptor.output_schema)?,
        })
    }

    fn resolve_model(
        &self,
        model: &str,
        parameters: &serde_json::Value,
    ) -> Result<ResolvedModelContract, String> {
        let resolved = self
            .models
            .resolve(model)
            .map_err(|error| error.code().to_string())?;
        resolved
            .validate_parameters(parameters)
            .map_err(|error| error.code().to_string())?;
        Ok(ResolvedModelContract {
            id: ResolvedModelId::parse(model)?,
            capabilities: resolved.capabilities(),
        })
    }
}

fn compile_resource_contract(schema: &serde_json::Value) -> Result<TypedContract, String> {
    let mut root = schema.clone();
    let definitions = match &mut root {
        serde_json::Value::Object(object) => object
            .remove("$defs")
            .map(|definitions| {
                let serde_json::Value::Object(definitions) = definitions else {
                    return Err("ACTION_SCHEMA_DEFS_INVALID".to_string());
                };
                definitions
                    .into_iter()
                    .map(|(name, schema)| {
                        Identifier::parse(name)
                            .map(|name| (name, schema))
                            .map_err(|_| "ACTION_SCHEMA_DEFS_INVALID".to_string())
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()
            })
            .transpose()?
            .unwrap_or_default(),
        _ => BTreeMap::new(),
    };
    let bundle =
        compile_contract_schema(&definitions, &root).map_err(|error| error.code().to_string())?;
    let value_type = super::types::SchemaType::compile(bundle.expanded_schema())
        .map_err(|error| error.code().to_string())?
        .into_value_type();
    Ok(TypedContract {
        schema: bundle.validator_document().clone(),
        value_type,
    })
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
    let prompts_path = DslPath::root().child_key("prompts");
    let mut total_bytes = 0usize;
    let mut prompts = BTreeMap::new();
    for (name, declaration) in declarations {
        let prompt_path = prompts_path
            .child_key(name.as_str())
            .child_key(match declaration {
                PromptDeclaration::Inline(_) => "inline",
                PromptDeclaration::File(_) => "file",
            });
        let text = resolve_prompt_text(root, &canonical_root, declaration).map_err(|error| {
            if error.path().is_some() {
                error
            } else {
                error.with_path(prompt_path.clone())
            }
        })?;
        total_bytes = total_bytes
            .checked_add(text.len())
            .filter(|total| *total <= MAX_TOTAL_PROMPT_BYTES)
            .ok_or_else(|| {
                CompileError::new(
                    "VNEXT_PROMPTS_TOO_LARGE",
                    "vNext prompts exceed their aggregate byte limit",
                )
                .with_path(prompts_path.clone())
            })?;
        compile_template(&text).map_err(|error| {
            let compile_error = CompileError::new(
                "VNEXT_LLM_TEMPLATE_INVALID",
                "vNext prompt does not satisfy the restricted template profile",
            )
            .with_path(prompt_path.clone());
            error.decoded_span().map_or(compile_error.clone(), |span| {
                compile_error.with_decoded_template_span(span)
            })
        })?;
        prompts.insert(name.clone(), text);
    }
    Ok(prompts)
}

fn resolve_prompt_text(
    root: &Path,
    canonical_root: &Path,
    declaration: &PromptDeclaration,
) -> Result<String, CompileError> {
    let text = match declaration {
        PromptDeclaration::Inline(text) => text.clone(),
        PromptDeclaration::File(relative) => {
            if Path::new(relative)
                .extension()
                .and_then(|value| value.to_str())
                != Some("md")
            {
                return Err(CompileError::new(
                    "VNEXT_PROMPT_FILE_INVALID",
                    "vNext prompt files must use the .md extension",
                ));
            }
            let path = root.join(relative);
            let canonical = path.canonicalize().map_err(|_| {
                CompileError::new(
                    "VNEXT_PROMPT_READ_FAILED",
                    "failed to resolve a vNext prompt file",
                )
            })?;
            if !canonical.starts_with(canonical_root) {
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
            if !metadata.is_file() {
                return Err(CompileError::new(
                    "VNEXT_PROMPT_FILE_INVALID",
                    "vNext prompt path must resolve to a regular file",
                ));
            }
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
    if text.starts_with('\u{feff}') || text.contains('\0') || text.trim().is_empty() {
        return Err(CompileError::new(
            "VNEXT_PROMPT_CONTENT_INVALID",
            "vNext prompt content is empty or contains forbidden bytes",
        ));
    }
    Ok(text)
}

fn workflow_hash(raw: &RawWorkflow, ir: &WorkflowIr) -> Result<String, CompileError> {
    use std::fmt::Write as _;

    let bytes = serde_jcs::to_vec(raw).map_err(|_| agent_hash_error())?;
    let mut hasher = Sha256::new();
    hash_component(&mut hasher, b"insight.agent.content-hash/v2");
    hash_component(&mut hasher, &bytes);
    for (prompt_id, prompt) in &ir.prompts {
        hash_component(&mut hasher, b"catalog-template");
        hash_component(&mut hasher, prompt_id.as_str().as_bytes());
        let identity = template_identity_bytes(&prompt.compiled, None, prompt.profile_version)?;
        hash_component(&mut hasher, &identity);
    }
    hash_region_templates(&mut hasher, &ir.root)?;
    let digest = hasher.finalize();
    let mut hash = String::with_capacity("sha256:".len() + digest.len() * 2);
    hash.push_str("sha256:");
    for byte in digest {
        write!(&mut hash, "{byte:02x}").expect("writing to a string cannot fail");
    }
    Ok(hash)
}

fn hash_region_templates(hasher: &mut Sha256, region: &Region) -> Result<(), CompileError> {
    for operation in &region.operations {
        match &operation.kind {
            OperationKind::Call(call) => {
                let CallPlan::Llm(plan) = &call.plan else {
                    continue;
                };
                for (template_id, planned) in &plan.templates {
                    hash_component(hasher, b"planned-template");
                    hash_component(hasher, region.id.path().as_str().as_bytes());
                    hash_component(hasher, operation.id.path().as_str().as_bytes());
                    hash_component(hasher, template_id.as_str().as_bytes());
                    let identity = planned_template_identity_bytes(planned)?;
                    hash_component(hasher, &identity);
                }
            }
            OperationKind::Parallel(parallel) => {
                for branch in parallel.branches.values() {
                    hash_region_templates(hasher, branch)?;
                }
            }
            OperationKind::Branch(branch) => {
                for case in &branch.cases {
                    hash_region_templates(hasher, &case.region)?;
                }
                hash_region_templates(hasher, &branch.default.region)?;
            }
            OperationKind::Const { .. }
            | OperationKind::Project { .. }
            | OperationKind::Object { .. }
            | OperationKind::Array { .. }
            | OperationKind::Template { .. }
            | OperationKind::Phi(_) => {}
        }
    }
    Ok(())
}

fn planned_template_identity_bytes(planned: &PlannedTemplate) -> Result<Vec<u8>, CompileError> {
    template_identity_bytes(
        &planned.compiled,
        Some(&planned.slot_signature),
        planned.profile_version,
    )
}

/// A versioned, implementation-independent template identity.
///
/// Rust `Debug` and dependency serialization are intentionally excluded: all
/// persisted bytes are owned here and changing this format requires a version
/// bump.
fn template_identity_bytes(
    compiled: &CompiledTemplate,
    slot_signature: Option<&BTreeMap<Identifier, ValueType>>,
    profile: TemplateProfileVersion,
) -> Result<Vec<u8>, CompileError> {
    let mut identity = Vec::new();
    encode_bytes(&mut identity, b"insight.llm-template.identity/v1");
    encode_bytes(&mut identity, profile.as_str().as_bytes());
    encode_bytes(&mut identity, compiled.source().as_bytes());
    encode_template_ast(&mut identity, compiled.ast())?;
    encode_template_accesses(&mut identity, compiled);

    encode_count(&mut identity, compiled.slots().len());
    for slot in compiled.slots() {
        encode_bytes(&mut identity, slot.as_str().as_bytes());
    }

    match slot_signature {
        None => identity.push(0),
        Some(slot_signature) => {
            identity.push(1);
            encode_count(&mut identity, slot_signature.len());
            for (slot, value_type) in slot_signature {
                encode_bytes(&mut identity, slot.as_str().as_bytes());
                encode_value_type(&mut identity, value_type);
            }
        }
    }
    Ok(identity)
}

fn encode_template_ast(output: &mut Vec<u8>, template: &Template) -> Result<(), CompileError> {
    encode_count(output, template.elements.len());
    for element in &template.elements {
        encode_template_element(output, element)?;
    }
    Ok(())
}

fn encode_template_element(
    output: &mut Vec<u8>,
    element: &TemplateElement,
) -> Result<(), CompileError> {
    match element {
        TemplateElement::RawString(text) => {
            output.push(0);
            encode_bytes(output, text.as_bytes());
        }
        TemplateElement::Expression(helper) => {
            output.push(1);
            encode_helper_template(output, helper)?;
        }
        TemplateElement::HelperBlock(helper) => {
            output.push(2);
            encode_helper_template(output, helper)?;
        }
        TemplateElement::HtmlExpression(_)
        | TemplateElement::DecoratorExpression(_)
        | TemplateElement::DecoratorBlock(_)
        | TemplateElement::PartialExpression(_)
        | TemplateElement::PartialBlock(_)
        | TemplateElement::Comment(_) => return Err(agent_hash_error()),
        _ => return Err(agent_hash_error()),
    }
    Ok(())
}

fn encode_helper_template(
    output: &mut Vec<u8>,
    helper: &HelperTemplate,
) -> Result<(), CompileError> {
    encode_parameter(output, &helper.name)?;
    encode_count(output, helper.params.len());
    for parameter in &helper.params {
        encode_parameter(output, parameter)?;
    }

    let mut hash = helper.hash.iter().collect::<Vec<_>>();
    hash.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
    encode_count(output, hash.len());
    for (name, parameter) in hash {
        encode_bytes(output, name.as_bytes());
        encode_parameter(output, parameter)?;
    }

    match &helper.block_param {
        None => output.push(0),
        Some(BlockParam::Single(parameter)) => {
            output.push(1);
            encode_parameter(output, parameter)?;
        }
        Some(BlockParam::Pair((left, right))) => {
            output.push(2);
            encode_parameter(output, left)?;
            encode_parameter(output, right)?;
        }
        Some(_) => return Err(agent_hash_error()),
    }
    encode_optional_template(output, helper.template.as_ref())?;
    encode_optional_template(output, helper.inverse.as_ref())?;
    output.push(u8::from(helper.block));
    output.push(u8::from(helper.chain));
    Ok(())
}

fn encode_parameter(output: &mut Vec<u8>, parameter: &Parameter) -> Result<(), CompileError> {
    match parameter {
        Parameter::Name(name) => {
            output.push(0);
            encode_bytes(output, name.as_bytes());
        }
        Parameter::Path(_) => {
            output.push(1);
            let path = parameter.as_name().ok_or_else(agent_hash_error)?;
            encode_bytes(output, path.as_bytes());
        }
        Parameter::Literal(value) => {
            output.push(2);
            encode_json_value(output, value);
        }
        Parameter::Subexpression(expression) => {
            output.push(3);
            encode_template_element(output, &expression.element)?;
        }
        _ => return Err(agent_hash_error()),
    }
    Ok(())
}

fn encode_optional_template(
    output: &mut Vec<u8>,
    template: Option<&Template>,
) -> Result<(), CompileError> {
    match template {
        None => output.push(0),
        Some(template) => {
            output.push(1);
            encode_template_ast(output, template)?;
        }
    }
    Ok(())
}

fn encode_template_accesses(output: &mut Vec<u8>, compiled: &CompiledTemplate) {
    encode_count(output, compiled.accesses().len());
    for access in compiled.accesses() {
        output.push(match access.kind {
            TemplateAccessKind::Scalar => 0,
            TemplateAccessKind::Json => 1,
            TemplateAccessKind::Each => 2,
        });
        encode_bytes(output, access.path.root.as_str().as_bytes());
        encode_count(output, access.path.segments.len());
        for segment in &access.path.segments {
            match segment {
                TemplatePathSegment::Field(field) => {
                    output.push(0);
                    encode_bytes(output, field.as_str().as_bytes());
                }
                TemplatePathSegment::EachItem => output.push(1),
            }
        }
    }
}

fn encode_value_type(output: &mut Vec<u8>, value_type: &ValueType) {
    match value_type {
        ValueType::Never => output.push(0),
        ValueType::Any => output.push(1),
        ValueType::Null => output.push(2),
        ValueType::Boolean => output.push(3),
        ValueType::Integer => output.push(4),
        ValueType::Number => output.push(5),
        ValueType::String => output.push(6),
        ValueType::Literal(value) => {
            output.push(7);
            encode_json_value(output, value);
        }
        ValueType::Array(array) => {
            output.push(8);
            encode_count(output, array.min_items);
            encode_value_type(output, &array.items);
        }
        ValueType::Object(object) => {
            output.push(9);
            encode_count(output, object.properties.len());
            for (name, property) in &object.properties {
                encode_bytes(output, name.as_bytes());
                output.push(u8::from(property.required));
                encode_value_type(output, &property.value_type);
            }
            match &object.additional_properties {
                None => output.push(0),
                Some(additional) => {
                    output.push(1);
                    encode_value_type(output, additional);
                }
            }
        }
        ValueType::Union(variants) => {
            output.push(10);
            encode_count(output, variants.len());
            for variant in variants {
                encode_value_type(output, variant);
            }
        }
    }
}

fn encode_json_value(output: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => output.push(0),
        Value::Bool(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        Value::Number(value) => {
            output.push(2);
            encode_bytes(output, value.to_string().as_bytes());
        }
        Value::String(value) => {
            output.push(3);
            encode_bytes(output, value.as_bytes());
        }
        Value::Array(values) => {
            output.push(4);
            encode_count(output, values.len());
            for value in values {
                encode_json_value(output, value);
            }
        }
        Value::Object(fields) => {
            output.push(5);
            let mut fields = fields.iter().collect::<Vec<_>>();
            fields.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
            encode_count(output, fields.len());
            for (name, value) in fields {
                encode_bytes(output, name.as_bytes());
                encode_json_value(output, value);
            }
        }
    }
}

fn encode_count(output: &mut Vec<u8>, value: usize) {
    output.extend_from_slice(&(value as u64).to_be_bytes());
}

fn encode_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    encode_count(output, bytes.len());
    output.extend_from_slice(bytes);
}

fn agent_hash_error() -> CompileError {
    CompileError::new(
        "VNEXT_AGENT_HASH_FAILED",
        "failed to normalize the vNext agent document",
    )
}

fn hash_component(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
    };

    use serde_json::{Map, Value};
    use tempfile::tempdir;

    use super::{template_identity_bytes, workflow_hash, WorkflowCompiler};
    use crate::{
        dsl::vnext::{
            ir::OperationKind,
            lower::{
                lower_workflow, ResolvedActionContract, ResolvedModelContract, ResourceResolver,
            },
            plan::{CallPlan, ResolvedModelId, TemplateProfileVersion},
            raw::parse_workflow,
            template::compile_template,
            types::ValueType,
            value::Identifier,
        },
        resources::{
            actions::ActionRegistry,
            models::{ModelCapability, ModelRegistry},
        },
    };

    struct HashOnlyResolver;

    impl ResourceResolver for HashOnlyResolver {
        fn resolve_action(&self, _action_id: &str) -> Result<ResolvedActionContract, String> {
            Err("actions are not used by this hash fixture".to_string())
        }

        fn resolve_model(
            &self,
            model: &str,
            _parameters: &Value,
        ) -> Result<ResolvedModelContract, String> {
            Ok(ResolvedModelContract {
                id: ResolvedModelId::parse(model)?,
                capabilities: BTreeSet::<ModelCapability>::new(),
            })
        }
    }

    fn source_slice(source: &str, span: crate::dsl::SourceSpan) -> &str {
        &source[span.byte_start() as usize..span.byte_end() as usize]
    }

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
        assert_eq!(
            first.ir.prompts.values().next().unwrap().compiled.source(),
            "first"
        );

        let repeated = compiler.compile_dir(directory.path()).unwrap();
        assert_eq!(first.version_hash, repeated.version_hash);
        assert!(first.version_hash.starts_with("sha256:"));
        assert_eq!(first.version_hash.len(), 71);

        fs::write(directory.path().join("prompts/system.md"), "second").unwrap();
        let second = compiler.compile_dir(directory.path()).unwrap();
        assert_ne!(first.version_hash, second.version_hash);
    }

    #[test]
    fn workflow_hash_canonicalizes_json_object_map_order() {
        let directory = tempdir().unwrap();
        let source = SOURCE.replace(
            "  system: {file: prompts/system.md}",
            "  system: {inline: system}",
        );
        let left = source.replace(
            "properties: {question: {type: string}}",
            "properties: {question: {type: string}, context: {type: integer}}",
        );
        let right = source.replace(
            "properties: {question: {type: string}}",
            "properties: {context: {type: integer}, question: {type: string}}",
        );
        let compiler = WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default());

        let left = compiler.compile_source(directory.path(), &left).unwrap();
        let right = compiler.compile_source(directory.path(), &right).unwrap();

        assert_eq!(left.version_hash, right.version_hash);
    }

    #[test]
    fn template_identity_canonicalizes_literal_object_map_order() {
        let compiled = compile_template("{{ json value }}").unwrap();
        let value = Identifier::parse("value").unwrap();
        let mut left_fields = Map::new();
        left_fields.insert("a".to_string(), Value::String("first".to_string()));
        left_fields.insert("b".to_string(), Value::from(2));
        let mut right_fields = Map::new();
        right_fields.insert("b".to_string(), Value::from(2));
        right_fields.insert("a".to_string(), Value::String("first".to_string()));
        let left_signature = BTreeMap::from([(
            value.clone(),
            ValueType::Literal(Value::Object(left_fields)),
        )]);
        let right_signature =
            BTreeMap::from([(value, ValueType::Literal(Value::Object(right_fields)))]);

        let left =
            template_identity_bytes(&compiled, Some(&left_signature), TemplateProfileVersion::V1)
                .unwrap();
        let right = template_identity_bytes(
            &compiled,
            Some(&right_signature),
            TemplateProfileVersion::V1,
        )
        .unwrap();

        assert_eq!(left, right);
    }

    #[test]
    fn template_identity_includes_source_bytes_raw_ast_and_profile() {
        let spaced = compile_template("{{ value }}").unwrap();
        let compact = compile_template("{{value}}").unwrap();
        assert_eq!(spaced.ast().elements, compact.ast().elements);
        assert_eq!(spaced.accesses(), compact.accesses());
        let spaced = template_identity_bytes(&spaced, None, TemplateProfileVersion::V1).unwrap();
        let compact = template_identity_bytes(&compact, None, TemplateProfileVersion::V1).unwrap();
        assert_ne!(spaced, compact, "exact source bytes are identity-bearing");
        assert!(spaced
            .windows(TemplateProfileVersion::V1.as_str().len())
            .any(|window| window == TemplateProfileVersion::V1.as_str().as_bytes()));

        let raw_left = compile_template("{{{{raw}}}}{{ value }}{{{{/raw}}}}").unwrap();
        let raw_right = compile_template("{{{{raw}}}}{{ other }}{{{{/raw}}}}").unwrap();
        let raw_left =
            template_identity_bytes(&raw_left, None, TemplateProfileVersion::V1).unwrap();
        let raw_right =
            template_identity_bytes(&raw_right, None, TemplateProfileVersion::V1).unwrap();
        assert_ne!(raw_left, raw_right, "raw block bodies are identity-bearing");
    }

    #[test]
    fn template_identity_includes_static_slot_types() {
        let compiled = compile_template("{{ value }}").unwrap();
        let value = Identifier::parse("value").unwrap();
        let string_signature = BTreeMap::from([(value.clone(), ValueType::String)]);
        let integer_signature = BTreeMap::from([(value, ValueType::Integer)]);

        let string_identity = template_identity_bytes(
            &compiled,
            Some(&string_signature),
            TemplateProfileVersion::V1,
        )
        .unwrap();
        let integer_identity = template_identity_bytes(
            &compiled,
            Some(&integer_signature),
            TemplateProfileVersion::V1,
        )
        .unwrap();

        assert_ne!(string_identity, integer_identity);
    }

    #[test]
    fn workflow_hash_includes_inline_template_slot_signature_from_typed_ir() {
        let source = r#"
api_version: insight.agent/v2
kind: agent
metadata: {id: inline_hash, name: Inline Hash}
schema_dialect: https://json-schema.org/draft/2020-12/schema
prompts: {}
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
  steps:
    - kind: llm
      id: answer
      model: hash_model
      inputs:
        question: {from: input.question}
      messages:
        - role: user
          content: {text: "Question: {{question}}"}
      response: {format: text}
  result:
    return:
      data:
        object:
          answer: {from: steps.answer.output.data}
"#;
        let (raw, _) = parse_workflow(source).unwrap().into_parts();
        let mut ir = lower_workflow(&raw, &HashOnlyResolver).unwrap();
        let original = workflow_hash(&raw, &ir).unwrap();

        let plan = ir
            .root
            .operations
            .iter_mut()
            .find_map(|operation| match &mut operation.kind {
                OperationKind::Call(call) => match &mut call.plan {
                    CallPlan::Llm(plan) => Some(plan),
                    CallPlan::Action(_) => None,
                },
                _ => None,
            })
            .expect("fixture lowers one LLM plan");
        let template = plan
            .templates
            .values_mut()
            .find(|template| !template.slot_signature.is_empty())
            .expect("inline template has one typed slot");
        assert_eq!(
            template
                .slot_signature
                .get(&Identifier::parse("question").unwrap()),
            Some(&ValueType::String)
        );
        template
            .slot_signature
            .insert(Identifier::parse("question").unwrap(), ValueType::Integer);

        let mutated = workflow_hash(&raw, &ir).unwrap();
        assert_ne!(original, mutated);
    }

    #[test]
    fn aggregate_prompt_limit_fails_before_later_declarations_are_read() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("prompts")).unwrap();
        for name in ["a", "b", "c", "d"] {
            fs::write(
                directory.path().join(format!("prompts/{name}.md")),
                vec![b'x'; super::MAX_PROMPT_BYTES],
            )
            .unwrap();
        }
        fs::write(directory.path().join("prompts/e.md"), "x").unwrap();
        let source = SOURCE.replace(
            "  system: {file: prompts/system.md}",
            r#"  a: {file: prompts/a.md}
  b: {file: prompts/b.md}
  c: {file: prompts/c.md}
  d: {file: prompts/d.md}
  e: {file: prompts/e.md}
  z_missing: {file: prompts/missing.md}"#,
        );

        let error = WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default())
            .compile_source(directory.path(), &source)
            .unwrap_err();

        assert_eq!(error.code(), "VNEXT_PROMPTS_TOO_LARGE");
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.prompts")
        );
        assert_eq!(error.agent_id(), Some("fixture"));
        assert_eq!(error.step_id(), None);
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

    #[test]
    fn rejects_non_markdown_and_non_regular_prompt_assets() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("prompts")).unwrap();
        fs::write(directory.path().join("prompts/system.txt"), "instruction").unwrap();
        let source = SOURCE.replace("prompts/system.md", "prompts/system.txt");
        let compiler = WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default());
        let error = compiler
            .compile_source(directory.path(), &source)
            .unwrap_err();
        assert_eq!(error.code(), "VNEXT_PROMPT_FILE_INVALID");

        fs::create_dir(directory.path().join("prompts/directory.md")).unwrap();
        let source = SOURCE.replace("prompts/system.md", "prompts/directory.md");
        let error = compiler
            .compile_source(directory.path(), &source)
            .unwrap_err();
        assert_eq!(error.code(), "VNEXT_PROMPT_FILE_INVALID");
    }

    #[test]
    fn rejects_unsafe_or_unsupported_prompt_content_without_echoing_it() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("prompts")).unwrap();
        let compiler = WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default());

        for content in ["", "  \n", "\u{feff}secret", "secret\0body"] {
            fs::write(directory.path().join("prompts/system.md"), content).unwrap();
            let error = compiler
                .compile_source(directory.path(), SOURCE)
                .unwrap_err();
            assert_eq!(error.code(), "VNEXT_PROMPT_CONTENT_INVALID");
            assert!(!error.to_string().contains("secret"));
        }

        fs::write(
            directory.path().join("prompts/system.md"),
            "{{#if secret}}forbidden{{/if}}",
        )
        .unwrap();
        let error = compiler
            .compile_source(directory.path(), SOURCE)
            .unwrap_err();
        assert_eq!(error.code(), "VNEXT_LLM_TEMPLATE_INVALID");
        assert!(!error.to_string().contains("secret"));
    }

    #[test]
    fn file_prompt_template_errors_keep_authored_and_decoded_coordinates_separate() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("prompts")).unwrap();
        let prompt = "第一行\n第二行 {{#if secret}}do-not-render{{/if}}";
        fs::write(directory.path().join("prompts/system.md"), prompt).unwrap();

        let error = WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default())
            .compile_source(directory.path(), SOURCE)
            .unwrap_err();

        assert_eq!(error.code(), "VNEXT_LLM_TEMPLATE_INVALID");
        assert_eq!(error.agent_id(), Some("fixture"));
        assert_eq!(error.step_id(), None);
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.prompts.system.file")
        );
        assert_eq!(
            source_slice(SOURCE, error.span().unwrap()),
            "prompts/system.md"
        );
        let decoded = error.decoded_template_span().unwrap();
        assert_eq!(decoded.byte_start(), prompt.find("{{#if").unwrap() as u64);
        assert_eq!((decoded.line_start(), decoded.column_start()), (2, 5));
        assert!(!error.to_string().contains("secret"));
        assert!(!error.to_string().contains("do-not-render"));
    }

    #[test]
    fn inline_block_prompt_errors_use_decoded_scalar_coordinates() {
        let directory = tempdir().unwrap();
        let source = SOURCE.replace(
            "  system: {file: prompts/system.md}",
            r#"  system:
    inline: |
      第一行
      第二行 {{#if secret}}do-not-render{{/if}}"#,
        );
        let decoded_prompt = "第一行\n第二行 {{#if secret}}do-not-render{{/if}}\n";

        let error = WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default())
            .compile_source(directory.path(), &source)
            .unwrap_err();

        assert_eq!(error.code(), "VNEXT_LLM_TEMPLATE_INVALID");
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.prompts.system.inline")
        );
        let authored = error.span().unwrap();
        let authored_source = source_slice(&source, authored);
        assert!(
            authored_source.starts_with('|'),
            "unexpected authored span: {authored_source:?}"
        );
        let decoded = error.decoded_template_span().unwrap();
        assert_eq!(
            decoded.byte_start(),
            decoded_prompt.find("{{#if").unwrap() as u64
        );
        assert_eq!((decoded.line_start(), decoded.column_start()), (2, 5));
        assert!(authored.line_start() > decoded.line_start());
        assert!(!error.to_string().contains("secret"));
        assert!(!error.to_string().contains("do-not-render"));
    }

    #[test]
    fn single_inline_message_template_error_stops_at_authored_content() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("prompts")).unwrap();
        fs::write(directory.path().join("prompts/system.md"), "system").unwrap();
        let source = SOURCE.replace(
            "workflow:\n  result:",
            r#"workflow:
  steps:
    - kind: llm
      id: ask
      model: unavailable
      messages:
        - role: user
          content:
            text: |-
              第一行
              第二行 {{#if secret}}do-not-render{{/if}}
      response: {format: text}
  result:"#,
        );

        let error = WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default())
            .compile_source(directory.path(), &source)
            .unwrap_err();

        assert_eq!(error.code(), "VNEXT_LLM_TEMPLATE_INVALID");
        assert_eq!(error.agent_id(), Some("fixture"));
        assert_eq!(error.step_id(), Some("ask"));
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.workflow.steps[0].messages[0].content")
        );
        assert!(source_slice(&source, error.span().unwrap()).starts_with("text: |-"));
        let decoded = error.decoded_template_span().unwrap();
        assert_eq!((decoded.line_start(), decoded.column_start()), (2, 5));
        assert!(!error.to_string().contains("secret"));
        assert!(!error.to_string().contains("do-not-render"));
    }

    #[test]
    fn nested_default_parts_template_error_maps_to_authored_indexes() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("prompts")).unwrap();
        fs::write(directory.path().join("prompts/system.md"), "system").unwrap();
        let source = SOURCE.replace(
            "workflow:\n  result:",
            r#"workflow:
  steps:
    - kind: parallel
      id: fanout
      settle: all
      max_concurrency: 2
      branches:
        left:
          output_schema: {}
          steps:
            - kind: switch
              id: route
              output_schema: {}
              cases:
                - id: active
                  when: {cel: "true"}
                  result: {return: {literal: null}}
              default:
                id: fallback
                steps:
                  - kind: llm
                    id: ask
                    model: unavailable
                    messages:
                      - role: user
                        content:
                          - {text: safe}
                          - text: |-
                              第一行
                              第二行 {{#if secret}}do-not-render{{/if}}
                    response: {format: text}
                result: {return: {literal: null}}
          result: {return: {literal: null}}
        right:
          output_schema: {}
          result: {return: {literal: null}}
  result:"#,
        );

        let error = WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default())
            .compile_source(directory.path(), &source)
            .unwrap_err();

        assert_eq!(error.code(), "VNEXT_LLM_TEMPLATE_INVALID");
        assert_eq!(error.agent_id(), Some("fixture"));
        assert_eq!(error.step_id(), Some("ask"));
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some(
                "$.workflow.steps[0].branches.left.steps[0].default.steps[0].messages[0].content[1]"
            )
        );
        assert!(source_slice(&source, error.span().unwrap()).starts_with("text: |-"));
        let decoded = error.decoded_template_span().unwrap();
        assert_eq!((decoded.line_start(), decoded.column_start()), (2, 5));
        assert!(!error.to_string().contains("secret"));
        assert!(!error.to_string().contains("do-not-render"));
    }

    #[test]
    fn parse_diagnostics_reach_compile_errors_without_source_body() {
        let directory = tempdir().unwrap();
        let source = SOURCE.replace(
            "metadata: {id: fixture, name: Fixture}",
            "metadata: {id: fixture, name: Fixture, secret_field: do-not-render}",
        );
        let error = WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default())
            .compile_source(directory.path(), &source)
            .unwrap_err();

        assert_eq!(error.code(), "VNEXT_AGENT_PARSE_FAILED");
        assert_eq!(error.agent_id(), None);
        assert_eq!(error.step_id(), None);
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.metadata.secret_field")
        );
        assert_eq!(source_slice(&source, error.span().unwrap()), "secret_field");
        assert_eq!(error.message(), "failed to parse the vNext agent document");
        assert!(!error.to_string().contains("do-not-render"));
        assert!(!error.to_string().contains("secret_field"));
    }

    #[test]
    fn semantic_and_lower_diagnostics_resolve_authored_paths_and_spans() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("prompts")).unwrap();
        fs::write(directory.path().join("prompts/system.md"), "system").unwrap();
        let compiler = WorkflowCompiler::new(ModelRegistry::default(), ActionRegistry::default());

        let semantic = SOURCE.replace(
            "workflow:\n  result:",
            r#"workflow:
  steps:
    - kind: llm
      id: ask
      model: unavailable
      inputs:
        unused: {literal: do-not-render}
      messages:
        - {role: system, content: system}
        - {role: user, content: {text: hello}}
      response: {format: text}
  result:"#,
        );
        let error = compiler
            .compile_source(directory.path(), &semantic)
            .unwrap_err();
        assert_eq!(error.code(), "VNEXT_LLM_INPUT_UNUSED");
        assert_eq!(error.agent_id(), Some("fixture"));
        assert_eq!(error.step_id(), Some("ask"));
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.workflow.steps[0].inputs.unused")
        );
        assert_eq!(
            source_slice(&semantic, error.span().unwrap()),
            "{literal: do-not-render}"
        );
        assert!(!error.to_string().contains("do-not-render"));

        let lower = SOURCE.replace(
            "https://json-schema.org/draft/2020-12/schema",
            "urn:unsupported",
        );
        let error = compiler
            .compile_source(directory.path(), &lower)
            .unwrap_err();
        assert_eq!(
            error.code(),
            super::super::lower::LOWER_SCHEMA_DIALECT_INVALID
        );
        assert_eq!(error.agent_id(), Some("fixture"));
        assert_eq!(error.step_id(), None);
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.schema_dialect")
        );
        assert_eq!(
            source_slice(&lower, error.span().unwrap()),
            "urn:unsupported"
        );

        let slash_location = SOURCE.replace(
            "workflow:\n  result:",
            r#"workflow:
  steps:
    - kind: llm
      id: ask
      model: unavailable
      messages:
        - {role: system, content: system}
        - {role: user, content: {text: hello}}
      response: {format: text}
  result:"#,
        );
        let error = compiler
            .compile_source(directory.path(), &slash_location)
            .unwrap_err();
        assert_eq!(error.code(), super::super::lower::LLM_MODEL_NOT_FOUND);
        assert_eq!(error.agent_id(), Some("fixture"));
        assert_eq!(error.step_id(), Some("ask"));
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.workflow.steps[0]")
        );
        let span = source_slice(&slash_location, error.span().unwrap());
        assert!(
            span.starts_with("- kind: llm") || span.starts_with("kind: llm"),
            "unexpected step span: {span:?}"
        );
        assert!(span.ends_with("{format: text}"));
    }
}
