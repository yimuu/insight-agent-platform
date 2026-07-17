use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use handlebars::Template;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    dsl::{DslPath, SourceSpan},
    resources::{actions::ActionDescriptorIdentity, models::ModelCapability},
};

use super::{
    ir::{
        self, Branch, BranchCase, BranchDefault, Call, CelProgram, CompiledPrompt, IrValueType,
        Operation, OperationId, OperationKind, Parallel, ParameterSource, Phi, Region, RegionId,
        RegionKind, RegionParameter, RootReturn as IrRootReturn, Terminator, TypedContract,
        ValueDefinition, ValueId, WorkflowIr,
    },
    message::{
        AuthoredContentAtom, AuthoredContentExpr, AuthoredMessageTemplate, AuthoredRole,
        MessageListExpr, MessageSource, ResponseConfig,
    },
    plan::{
        CallPlan, CallTarget, CompiledActionPlan, CompiledContentAtom, CompiledLlmPlan,
        CompiledTemplateId, MessageSourcePlan, PlannedRole, PlannedTemplate, ResolvedModelId,
        ResolvedRequestLimits, TemplateProfileVersion, TemplateProvenance,
        ValidatedModelParameters, ValidatedResponseContract,
    },
    predicate::analyze_predicate,
    raw::{
        BlockResult, ParallelBranch, ParallelSettle, Predicate, PromptDeclaration, RawWorkflow,
        RootResult, Step, SwitchCase, SwitchDefault,
    },
    schema::compile_contract_schema,
    semantics::validate_workflow_semantics,
    shape::{prove_dynamic_message_array, SchemaShape},
    template::{compile_template, CompiledTemplate, TemplateAccessKind, TemplatePathSegment},
    types::{
        safe_run_metadata_type, ArrayType, ObjectType, PropertyType, SchemaType, StaticPath,
        ValueType,
    },
    value::{
        Identifier, LocalInputPath, LocalInputRef, TemplateExpr, ValueExpr, ValuePath,
        ValuePathRoot,
    },
};

pub const LOWER_SEMANTIC_INVALID: &str = "VNEXT_LOWER_SEMANTIC_INVALID";
pub const LOWER_SCHEMA_DIALECT_INVALID: &str = "VNEXT_LOWER_SCHEMA_DIALECT_INVALID";
pub const LOWER_SCHEMA_INVALID: &str = "VNEXT_LOWER_SCHEMA_INVALID";
pub const LOWER_CALL_CONTRACT_INVALID: &str = "VNEXT_LOWER_CALL_CONTRACT_INVALID";
pub const LOWER_SOURCE_INVALID: &str = "VNEXT_LOWER_SOURCE_INVALID";
pub const LOWER_PATH_INVALID: &str = "VNEXT_LOWER_PATH_INVALID";
pub const LOWER_TYPE_MISMATCH: &str = "VNEXT_LOWER_TYPE_MISMATCH";
pub const LOWER_CEL_INVALID: &str = "VNEXT_LOWER_CEL_INVALID";
pub const LOWER_TEMPLATE_INVALID: &str = "VNEXT_LLM_TEMPLATE_INVALID";
pub const LOWER_PROMPT_UNRESOLVED: &str = "VNEXT_LLM_PROMPT_NOT_FOUND";
pub const LOWER_IDENTITY_INVALID: &str = "VNEXT_LOWER_IDENTITY_INVALID";
pub const LOWER_LIMIT_EXCEEDED: &str = "VNEXT_LOWER_LIMIT_EXCEEDED";
pub const LOWER_IR_INVALID: &str = "VNEXT_LOWER_IR_INVALID";
pub const LLM_MODEL_NOT_FOUND: &str = "VNEXT_LLM_MODEL_NOT_FOUND";
pub const LLM_PARAMETERS_INVALID: &str = "VNEXT_LLM_PARAMETERS_INVALID";
pub const LLM_TEMPLATE_BINDING_INVALID: &str = "VNEXT_LLM_TEMPLATE_BINDING_INVALID";
pub const LLM_SYSTEM_RUNTIME_INPUT_FORBIDDEN: &str = "VNEXT_LLM_SYSTEM_RUNTIME_INPUT_FORBIDDEN";
pub const LLM_MESSAGE_SOURCE_TYPE_INVALID: &str = "VNEXT_LLM_MESSAGE_SOURCE_TYPE_INVALID";
pub const LLM_VISION_REQUIRED: &str = "VNEXT_LLM_VISION_REQUIRED";
pub const LLM_RESPONSE_CONFIG_INVALID: &str = "VNEXT_LLM_RESPONSE_CONFIG_INVALID";
pub const ACTION_NOT_FOUND: &str = "VNEXT_ACTION_NOT_FOUND";
pub const ACTION_INPUT_CONTRACT_INVALID: &str = "VNEXT_ACTION_INPUT_CONTRACT_INVALID";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LowerError {
    code: &'static str,
    message: String,
    location: Option<String>,
    cause_code: Option<&'static str>,
    decoded_template_span: Option<SourceSpan>,
}

impl LowerError {
    fn new(code: &'static str, message: impl Into<String>, location: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            location: Some(location.into()),
            cause_code: None,
            decoded_template_span: None,
        }
    }

    fn caused_by(mut self, cause_code: &'static str) -> Self {
        self.cause_code = Some(cause_code);
        self
    }

    fn with_decoded_template_span(mut self, span: Option<SourceSpan>) -> Self {
        self.decoded_template_span = span;
        self
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn location(&self) -> Option<&str> {
        self.location.as_deref()
    }

    pub fn cause_code(&self) -> Option<&'static str> {
        self.cause_code
    }

    pub fn decoded_template_span(&self) -> Option<SourceSpan> {
        self.decoded_template_span
    }
}

impl fmt::Display for LowerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(location) = &self.location {
            write!(formatter, "{} at {location}: {}", self.code, self.message)
        } else {
            write!(formatter, "{}: {}", self.code, self.message)
        }
    }
}

impl Error for LowerError {}

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedActionContract {
    pub identity: ActionDescriptorIdentity,
    pub input: TypedContract,
    pub output: TypedContract,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedModelContract {
    pub id: ResolvedModelId,
    pub capabilities: BTreeSet<ModelCapability>,
}

/// Compile-time-only boundary to the two closed leaf resource registries.
pub trait ResourceResolver {
    fn resolve_action(&self, action_id: &str) -> Result<ResolvedActionContract, String>;

    fn resolve_model(
        &self,
        model: &str,
        parameters: &Value,
    ) -> Result<ResolvedModelContract, String>;
}

pub type LowerResult<T> = Result<T, Vec<LowerError>>;

pub fn lower_workflow<R: ResourceResolver>(
    workflow: &RawWorkflow,
    resolver: &R,
) -> LowerResult<WorkflowIr> {
    if let Err(errors) = validate_workflow_semantics(workflow) {
        return Err(errors
            .into_iter()
            .map(|error| {
                LowerError::new(
                    LOWER_SEMANTIC_INVALID,
                    "workflow semantic validation failed",
                    error.location(),
                )
                .caused_by(error.code())
                .with_decoded_template_span(error.decoded_template_span())
            })
            .collect());
    }
    if !is_draft_2020_12(&workflow.schema_dialect) {
        return Err(vec![LowerError::new(
            LOWER_SCHEMA_DIALECT_INVALID,
            "workflow schema dialect must be Draft 2020-12",
            "schema_dialect",
        )]);
    }

    let lowerer = Lowerer { workflow, resolver };
    let ir = lowerer.lower().map_err(|error| vec![error])?;
    if let Err(errors) = ir::validate(&ir) {
        return Err(errors
            .into_iter()
            .map(|error| {
                let location = error
                    .operation
                    .as_ref()
                    .map(ToString::to_string)
                    .or_else(|| error.region.as_ref().map(ToString::to_string))
                    .unwrap_or_else(|| "workflow".to_string());
                LowerError::new(
                    LOWER_IR_INVALID,
                    format!(
                        "lowered workflow failed typed IR verification: {}",
                        error.message
                    ),
                    location,
                )
            })
            .collect());
    }
    Ok(ir)
}

fn is_draft_2020_12(dialect: &str) -> bool {
    dialect == "https://json-schema.org/draft/2020-12/schema"
}

type OneResult<T> = Result<T, LowerError>;

#[derive(Debug, Clone)]
struct TypedValue {
    id: ValueId,
    value_type: ValueType,
}

#[derive(Debug, Clone)]
struct RegionEnvironment {
    input: Option<TypedValue>,
    run: Option<TypedValue>,
    scope: BTreeMap<Identifier, TypedValue>,
    steps: BTreeMap<Identifier, TypedValue>,
}

struct ChildRegionSpec<'a> {
    runtime_path: &'a str,
    authored_path: &'a DslPath,
    kind: RegionKind,
    steps: &'a [Step],
    result: &'a BlockResult,
    result_contract: TypedContract,
}

struct Lowerer<'a, R> {
    workflow: &'a RawWorkflow,
    resolver: &'a R,
}

impl<R: ResourceResolver> Lowerer<'_, R> {
    fn lower(&self) -> OneResult<WorkflowIr> {
        let input = self.compile_contract(&self.workflow.input.schema, "input.schema")?;
        let output =
            self.compile_contract(&self.workflow.output.data_schema, "output.data_schema")?;
        let prompts = self.compile_prompts()?;

        let root_path = "/workflow";
        let input_value = TypedValue {
            id: self.parameter_value_id(root_path, 0)?,
            value_type: input.value_type.clone(),
        };
        let run_value = TypedValue {
            id: self.parameter_value_id(root_path, 1)?,
            value_type: safe_run_metadata_type(),
        };
        let parameters = vec![
            RegionParameter {
                name: identifier("input", root_path)?,
                value: data_definition(&input_value),
                source: ParameterSource::WorkflowInput,
            },
            RegionParameter {
                name: identifier("run", root_path)?,
                value: data_definition(&run_value),
                source: ParameterSource::RunMetadata,
            },
        ];
        let mut environment = RegionEnvironment {
            input: Some(input_value),
            run: Some(run_value),
            scope: BTreeMap::new(),
            steps: BTreeMap::new(),
        };
        let mut operations = Vec::new();
        let authored_root = DslPath::root().child_key("workflow");
        self.lower_steps(
            root_path,
            &authored_root,
            &self.workflow.workflow.steps,
            &mut operations,
            &mut environment,
        )?;
        let terminator = self.lower_root_result(
            &self.workflow.workflow.result,
            &output.value_type,
            &mut operations,
            &environment,
        )?;

        Ok(WorkflowIr {
            metadata: self.workflow.metadata.clone(),
            input,
            output: output.clone(),
            prompts,
            errors: self.workflow.errors.clone(),
            root: Region {
                id: self.region_id(root_path)?,
                kind: RegionKind::Workflow,
                parameters,
                operations,
                result: output,
                terminator: Some(terminator),
            },
        })
    }

    fn compile_prompts(&self) -> OneResult<BTreeMap<Identifier, CompiledPrompt>> {
        self.workflow
            .prompts
            .iter()
            .map(|(name, declaration)| match declaration {
                PromptDeclaration::Inline(text) => Ok((
                    name.clone(),
                    CompiledPrompt {
                        provenance: TemplateProvenance::Catalog {
                            prompt_id: name.clone(),
                            asset_hash: sha256_label(text.as_bytes()),
                        },
                        compiled: compile_template(text).map_err(|error| {
                            LowerError::new(
                                LOWER_TEMPLATE_INVALID,
                                "prompt does not satisfy the restricted template profile",
                                format!("prompts.{name}.inline"),
                            )
                            .caused_by(error.code())
                            .with_decoded_template_span(error.decoded_span())
                        })?,
                        profile_version: TemplateProfileVersion::V1,
                    },
                )),
                PromptDeclaration::File(_) => Err(LowerError::new(
                    LOWER_PROMPT_UNRESOLVED,
                    "file-backed prompt must be resolved before lowering",
                    format!("prompts.{name}"),
                )),
            })
            .collect()
    }

    fn compile_contract(&self, schema: &Value, location: &str) -> OneResult<TypedContract> {
        let bundle =
            compile_contract_schema(&self.workflow.definitions, schema).map_err(|error| {
                LowerError::new(
                    LOWER_SCHEMA_INVALID,
                    "workflow contract schema could not be compiled",
                    location,
                )
                .caused_by(error.code())
            })?;
        let schema_type = SchemaType::compile(bundle.expanded_schema()).map_err(|error| {
            LowerError::new(
                LOWER_SCHEMA_INVALID,
                "workflow contract is outside the supported static schema subset",
                location,
            )
            .caused_by(error.code())
        })?;
        Ok(TypedContract {
            schema: bundle.validator_document().clone(),
            value_type: schema_type.into_value_type(),
        })
    }

    fn lower_steps(
        &self,
        region_path: &str,
        authored_region_path: &DslPath,
        steps: &[Step],
        operations: &mut Vec<Operation>,
        environment: &mut RegionEnvironment,
    ) -> OneResult<()> {
        for (step_index, step) in steps.iter().enumerate() {
            let id = step_id(step);
            let step_path = format!("{region_path}/{}", id.as_str());
            let authored_step_path = authored_region_path
                .child_key("steps")
                .child_index(step_index);
            let output = match step {
                Step::Llm {
                    model,
                    inputs,
                    messages,
                    parameters,
                    response,
                    ..
                } => self.lower_llm(
                    &step_path,
                    &authored_step_path,
                    model,
                    inputs,
                    messages,
                    parameters,
                    response,
                    operations,
                    environment,
                )?,
                Step::Action { call, inputs, .. } => {
                    self.lower_action(&step_path, call, inputs, operations, environment)?
                }
                Step::Parallel {
                    inputs,
                    settle,
                    max_concurrency,
                    branches,
                    ..
                } => self.lower_parallel(
                    &step_path,
                    &authored_step_path,
                    inputs,
                    *settle,
                    *max_concurrency,
                    branches,
                    operations,
                    environment,
                )?,
                Step::Switch {
                    inputs,
                    output_schema,
                    cases,
                    default,
                    ..
                } => self.lower_switch(
                    &step_path,
                    &authored_step_path,
                    inputs,
                    output_schema,
                    cases,
                    default,
                    operations,
                    environment,
                )?,
            };
            environment.steps.insert(id.clone(), output);
        }
        Ok(())
    }

    fn lower_action(
        &self,
        step_path: &str,
        action_id: &str,
        inputs: &BTreeMap<Identifier, ValueExpr>,
        operations: &mut Vec<Operation>,
        environment: &RegionEnvironment,
    ) -> OneResult<TypedValue> {
        let mut ordinal = 0;
        let inputs = self.lower_named_inputs_with_ordinal(
            step_path,
            inputs,
            &mut ordinal,
            operations,
            environment,
        )?;
        let object_type = ValueType::Object(ObjectType {
            properties: inputs
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_string(),
                        PropertyType {
                            value_type: value.value_type.clone(),
                            required: true,
                        },
                    )
                })
                .collect(),
            additional_properties: None,
        });
        let input_object = self.emit_expression(
            step_path,
            &mut ordinal,
            operations,
            OperationKind::Object {
                fields: inputs
                    .iter()
                    .map(|(name, value)| (name.as_str().to_string(), value.id.clone()))
                    .collect(),
            },
            object_type,
        )?;
        let resolved = self.resolver.resolve_action(action_id).map_err(|cause| {
            let code = if cause == "ACTION_NOT_FOUND" {
                ACTION_NOT_FOUND
            } else {
                LOWER_CALL_CONTRACT_INVALID
            };
            LowerError::new(code, "action contract resolution failed", step_path)
        })?;
        if !input_object
            .value_type
            .is_assignable_to(&resolved.input.value_type)
        {
            return Err(LowerError::new(
                ACTION_INPUT_CONTRACT_INVALID,
                "action input object is not assignable to its declared contract",
                step_path,
            ));
        }

        let output = TypedValue {
            id: self.authored_output_id(step_path)?,
            value_type: resolved.output.value_type.clone(),
        };
        let input_name = identifier("input", step_path)?;
        operations.push(Operation {
            id: self.authored_operation_id(step_path)?,
            output: data_definition(&output),
            kind: OperationKind::Call(Box::new(Call {
                target: CallTarget::ActionCall,
                inputs: BTreeMap::from([(input_name, input_object.id.clone())]),
                plan: CallPlan::Action(CompiledActionPlan {
                    action_id: resolved.identity.id,
                    descriptor_version: resolved.identity.version,
                    descriptor_hash: resolved.identity.descriptor_hash,
                    input_object: input_object.id,
                    input_contract: resolved.input,
                    output_contract: resolved.output,
                }),
            })),
        });
        Ok(output)
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_llm(
        &self,
        step_path: &str,
        authored_step_path: &DslPath,
        model: &str,
        inputs: &BTreeMap<Identifier, ValueExpr>,
        messages: &MessageListExpr,
        parameters: &serde_json::Map<String, Value>,
        response: &ResponseConfig,
        operations: &mut Vec<Operation>,
        environment: &RegionEnvironment,
    ) -> OneResult<TypedValue> {
        let mut ordinal = 0;
        let local_inputs = self.lower_named_inputs_with_ordinal(
            step_path,
            inputs,
            &mut ordinal,
            operations,
            environment,
        )?;
        let parameters_value = Value::Object(parameters.clone());
        let resolved_model = self
            .resolver
            .resolve_model(model, &parameters_value)
            .map_err(|cause| {
                let code = if cause == "MODEL_NOT_FOUND" {
                    LLM_MODEL_NOT_FOUND
                } else {
                    LLM_PARAMETERS_INVALID
                };
                LowerError::new(code, "LLM resource resolution failed", step_path)
            })?;
        let parameters = ValidatedModelParameters::new(parameters_value).map_err(|_| {
            LowerError::new(
                LLM_PARAMETERS_INVALID,
                "LLM parameters must be a static object",
                step_path,
            )
        })?;
        let mut templates = BTreeMap::new();
        let mut consumed = BTreeSet::new();
        let message_sources = self.lower_message_list(
            step_path,
            authored_step_path,
            messages,
            &local_inputs,
            &mut ordinal,
            operations,
            &mut templates,
            &mut consumed,
        )?;
        let declared = local_inputs.keys().cloned().collect::<BTreeSet<_>>();
        if consumed != declared {
            return Err(LowerError::new(
                LLM_TEMPLATE_BINDING_INVALID,
                "LLM inputs must be consumed exactly by messages or template slots",
                step_path,
            ));
        }
        let requires_vision = message_sources.iter().any(|source| match source {
            MessageSourcePlan::Authored { content, .. } => content
                .iter()
                .any(|atom| matches!(atom, CompiledContentAtom::Image { .. })),
            MessageSourcePlan::Dynamic { proven_shape, .. } => proven_shape.requires_vision,
        });
        if requires_vision
            && !resolved_model
                .capabilities
                .contains(&ModelCapability::Vision)
        {
            return Err(LowerError::new(
                LLM_VISION_REQUIRED,
                "LLM messages require a vision-capable model",
                step_path,
            ));
        }
        let (response, output_contract) = self.compile_llm_response(response, step_path)?;
        let output = TypedValue {
            id: self.authored_output_id(step_path)?,
            value_type: output_contract.value_type.clone(),
        };
        let input_ids = input_ids(&local_inputs);
        operations.push(Operation {
            id: self.authored_operation_id(step_path)?,
            output: data_definition(&output),
            kind: OperationKind::Call(Box::new(Call {
                target: CallTarget::AiChat,
                inputs: input_ids.clone(),
                plan: CallPlan::Llm(CompiledLlmPlan {
                    model: resolved_model.id,
                    local_inputs: input_ids,
                    message_sources,
                    templates,
                    parameters,
                    response,
                    output_contract,
                    capabilities: resolved_model.capabilities,
                    limits: ResolvedRequestLimits::new(
                        128, 65_536, 32_768, 262_144, 262_144, 262_144,
                    )
                    .expect("compiler request limits must satisfy the frozen platform profile"),
                }),
            })),
        });
        Ok(output)
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_message_list(
        &self,
        step_path: &str,
        authored_step_path: &DslPath,
        messages: &MessageListExpr,
        local_inputs: &BTreeMap<Identifier, TypedValue>,
        ordinal: &mut u32,
        operations: &mut Vec<Operation>,
        templates: &mut BTreeMap<CompiledTemplateId, PlannedTemplate>,
        consumed: &mut BTreeSet<Identifier>,
    ) -> OneResult<Vec<MessageSourcePlan>> {
        match messages {
            MessageListExpr::Dynamic(reference) => Ok(vec![self.lower_dynamic_message_source(
                step_path,
                reference,
                local_inputs,
                ordinal,
                operations,
                consumed,
            )?]),
            MessageListExpr::Sources(sources) => sources
                .iter()
                .enumerate()
                .map(|(source_index, source)| match source {
                    MessageSource::Dynamic(reference) => self.lower_dynamic_message_source(
                        step_path,
                        reference,
                        local_inputs,
                        ordinal,
                        operations,
                        consumed,
                    ),
                    MessageSource::Authored(message) => self.lower_authored_message(
                        step_path,
                        authored_step_path,
                        source_index,
                        message,
                        local_inputs,
                        ordinal,
                        operations,
                        templates,
                        consumed,
                    ),
                })
                .collect(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_dynamic_message_source(
        &self,
        step_path: &str,
        reference: &LocalInputRef,
        local_inputs: &BTreeMap<Identifier, TypedValue>,
        ordinal: &mut u32,
        operations: &mut Vec<Operation>,
        consumed: &mut BTreeSet<Identifier>,
    ) -> OneResult<MessageSourcePlan> {
        let value = self.lower_local_input_ref(
            step_path,
            &reference.from,
            local_inputs,
            ordinal,
            operations,
            consumed,
        )?;
        let shape = SchemaShape::from_value_type(&value.value_type);
        let proven_shape = prove_dynamic_message_array(&shape).map_err(|error| {
            LowerError::new(
                LLM_MESSAGE_SOURCE_TYPE_INVALID,
                "dynamic LLM message source has an invalid structural contract",
                step_path,
            )
            .caused_by(error.code())
        })?;
        Ok(MessageSourcePlan::Dynamic {
            source: reference.from.clone(),
            value: value.id,
            proven_shape,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_authored_message(
        &self,
        step_path: &str,
        authored_step_path: &DslPath,
        source_index: usize,
        message: &AuthoredMessageTemplate,
        local_inputs: &BTreeMap<Identifier, TypedValue>,
        ordinal: &mut u32,
        operations: &mut Vec<Operation>,
        templates: &mut BTreeMap<CompiledTemplateId, PlannedTemplate>,
        consumed: &mut BTreeSet<Identifier>,
    ) -> OneResult<MessageSourcePlan> {
        let role = match message.role {
            AuthoredRole::System => PlannedRole::System,
            AuthoredRole::User => PlannedRole::User,
            AuthoredRole::Assistant => PlannedRole::Assistant,
        };
        let content_path = authored_step_path
            .child_key("messages")
            .child_index(source_index)
            .child_key("content");
        let mut content = Vec::with_capacity(message.content.atoms().len());
        match &message.content {
            AuthoredContentExpr::Single(atom) => content.push(self.lower_content_atom(
                step_path,
                &content_path,
                role,
                atom,
                local_inputs,
                ordinal,
                operations,
                templates,
                consumed,
            )?),
            AuthoredContentExpr::Parts(atoms) => {
                for (atom_index, atom) in atoms.iter().enumerate() {
                    content.push(self.lower_content_atom(
                        step_path,
                        &content_path.child_index(atom_index),
                        role,
                        atom,
                        local_inputs,
                        ordinal,
                        operations,
                        templates,
                        consumed,
                    )?);
                }
            }
        }
        Ok(MessageSourcePlan::Authored { role, content })
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_content_atom(
        &self,
        step_path: &str,
        authored_path: &DslPath,
        role: PlannedRole,
        atom: &AuthoredContentAtom,
        local_inputs: &BTreeMap<Identifier, TypedValue>,
        ordinal: &mut u32,
        operations: &mut Vec<Operation>,
        templates: &mut BTreeMap<CompiledTemplateId, PlannedTemplate>,
        consumed: &mut BTreeSet<Identifier>,
    ) -> OneResult<CompiledContentAtom> {
        match atom {
            AuthoredContentAtom::Prompt(prompt_id) => {
                let declaration = self.workflow.prompts.get(prompt_id).ok_or_else(|| {
                    LowerError::new(
                        LOWER_PROMPT_UNRESOLVED,
                        "LLM content references an undeclared prompt",
                        step_path,
                    )
                })?;
                let PromptDeclaration::Inline(source) = declaration else {
                    return Err(LowerError::new(
                        LOWER_PROMPT_UNRESOLVED,
                        "file-backed prompt must be resolved before lowering",
                        step_path,
                    ));
                };
                let compiled = compile_template(source).map_err(|error| {
                    LowerError::new(
                        LOWER_TEMPLATE_INVALID,
                        "LLM prompt does not satisfy the restricted template profile",
                        step_path,
                    )
                    .caused_by(error.code())
                    .with_decoded_template_span(error.decoded_span())
                })?;
                let template_id = CompiledTemplateId::catalog(prompt_id);
                let (bindings, slot_signature) =
                    self.bind_llm_template(step_path, role, &compiled, local_inputs, consumed)?;
                templates
                    .entry(template_id.clone())
                    .or_insert_with(|| PlannedTemplate {
                        provenance: TemplateProvenance::Catalog {
                            prompt_id: prompt_id.clone(),
                            asset_hash: sha256_label(source.as_bytes()),
                        },
                        compiled,
                        slot_signature,
                        profile_version: TemplateProfileVersion::V1,
                    });
                Ok(CompiledContentAtom::Template {
                    template_id,
                    bindings,
                })
            }
            AuthoredContentAtom::InlineText(source) => {
                let compiled = compile_template(source).map_err(|error| {
                    LowerError::new(
                        LOWER_TEMPLATE_INVALID,
                        "inline LLM text does not satisfy the restricted template profile",
                        step_path,
                    )
                    .caused_by(error.code())
                    .with_decoded_template_span(error.decoded_span())
                })?;
                let template_id = CompiledTemplateId::inline(authored_path, 0);
                let (bindings, slot_signature) =
                    self.bind_llm_template(step_path, role, &compiled, local_inputs, consumed)?;
                templates.insert(
                    template_id.clone(),
                    PlannedTemplate {
                        provenance: TemplateProvenance::Inline {
                            dsl_path: authored_path.clone(),
                            source_hash: sha256_label(source.as_bytes()),
                        },
                        compiled,
                        slot_signature,
                        profile_version: TemplateProfileVersion::V1,
                    },
                );
                Ok(CompiledContentAtom::Template {
                    template_id,
                    bindings,
                })
            }
            AuthoredContentAtom::RuntimeText(reference) => {
                if role != PlannedRole::User {
                    return Err(LowerError::new(
                        LLM_SYSTEM_RUNTIME_INPUT_FORBIDDEN,
                        "runtime LLM text is allowed only in authored user messages",
                        step_path,
                    ));
                }
                let value = self.lower_local_input_ref(
                    step_path,
                    &reference.from,
                    local_inputs,
                    ordinal,
                    operations,
                    consumed,
                )?;
                self.require_assignable(&value.value_type, &ValueType::String, step_path)?;
                Ok(CompiledContentAtom::RuntimeText { value: value.id })
            }
            AuthoredContentAtom::Image(reference) => {
                if role != PlannedRole::User {
                    return Err(LowerError::new(
                        LLM_SYSTEM_RUNTIME_INPUT_FORBIDDEN,
                        "LLM images are allowed only in authored user messages",
                        step_path,
                    ));
                }
                let value = self.lower_local_input_ref(
                    step_path,
                    &reference.from,
                    local_inputs,
                    ordinal,
                    operations,
                    consumed,
                )?;
                let expected = ValueType::Union(vec![ValueType::String, ValueType::Null]);
                self.require_assignable(&value.value_type, &expected, step_path)?;
                Ok(CompiledContentAtom::Image { value: value.id })
            }
        }
    }

    fn bind_llm_template(
        &self,
        step_path: &str,
        role: PlannedRole,
        compiled: &CompiledTemplate,
        local_inputs: &BTreeMap<Identifier, TypedValue>,
        consumed: &mut BTreeSet<Identifier>,
    ) -> OneResult<(
        BTreeMap<Identifier, ValueId>,
        BTreeMap<Identifier, ValueType>,
    )> {
        if role != PlannedRole::User && !compiled.slots().is_empty() {
            return Err(LowerError::new(
                LLM_SYSTEM_RUNTIME_INPUT_FORBIDDEN,
                "authored system and assistant templates cannot read runtime inputs",
                step_path,
            ));
        }
        self.validate_template_accesses(step_path, compiled, local_inputs)?;
        let mut bindings = BTreeMap::new();
        let mut signature = BTreeMap::new();
        for slot in compiled.slots() {
            let value = local_inputs.get(slot).ok_or_else(|| {
                LowerError::new(
                    LLM_TEMPLATE_BINDING_INVALID,
                    "LLM template slot has no same-named local input",
                    step_path,
                )
            })?;
            consumed.insert(slot.clone());
            bindings.insert(slot.clone(), value.id.clone());
            signature.insert(slot.clone(), value.value_type.clone());
        }
        Ok((bindings, signature))
    }

    fn validate_template_accesses(
        &self,
        step_path: &str,
        compiled: &CompiledTemplate,
        local_inputs: &BTreeMap<Identifier, TypedValue>,
    ) -> OneResult<()> {
        for access in compiled.accesses() {
            let root = local_inputs.get(&access.path.root).ok_or_else(|| {
                LowerError::new(
                    LLM_TEMPLATE_BINDING_INVALID,
                    "LLM template references an undeclared local input",
                    step_path,
                )
            })?;
            let value_type = template_access_type(&root.value_type, &access.path.segments)
                .map_err(|cause| {
                    LowerError::new(
                        LLM_TEMPLATE_BINDING_INVALID,
                        "LLM template access is not guaranteed by its input contract",
                        step_path,
                    )
                    .caused_by(cause)
                })?;
            match access.kind {
                TemplateAccessKind::Scalar if is_template_scalar(&value_type) => {}
                TemplateAccessKind::Json => {
                    if prove_dynamic_message_array(&SchemaShape::from_value_type(&value_type))
                        .is_ok()
                    {
                        return Err(LowerError::new(
                            LLM_TEMPLATE_BINDING_INVALID,
                            "dynamic message arrays cannot be rendered through a template",
                            step_path,
                        ));
                    }
                }
                TemplateAccessKind::Each if is_static_array(&value_type) => {
                    if prove_dynamic_message_array(&SchemaShape::from_value_type(&value_type))
                        .is_ok()
                    {
                        return Err(LowerError::new(
                            LLM_TEMPLATE_BINDING_INVALID,
                            "dynamic message arrays cannot be rendered through a template",
                            step_path,
                        ));
                    }
                }
                TemplateAccessKind::Scalar | TemplateAccessKind::Each => {
                    return Err(LowerError::new(
                        LLM_TEMPLATE_BINDING_INVALID,
                        "LLM template slot is not valid for its syntactic use",
                        step_path,
                    ));
                }
            }
        }
        Ok(())
    }

    fn lower_local_input_ref(
        &self,
        step_path: &str,
        path: &LocalInputPath,
        local_inputs: &BTreeMap<Identifier, TypedValue>,
        ordinal: &mut u32,
        operations: &mut Vec<Operation>,
        consumed: &mut BTreeSet<Identifier>,
    ) -> OneResult<TypedValue> {
        let source = local_inputs.get(path.binding()).ok_or_else(|| {
            LowerError::new(
                LLM_TEMPLATE_BINDING_INVALID,
                "LLM local input reference is not declared by this node",
                step_path,
            )
        })?;
        consumed.insert(path.binding().clone());
        if path.fields().is_empty() {
            return Ok(source.clone());
        }
        let static_path = StaticPath::from_decoded_segments(path.fields()).map_err(|error| {
            LowerError::new(
                LOWER_PATH_INVALID,
                "LLM local input path is not canonical",
                step_path,
            )
            .caused_by(error.code())
        })?;
        let value_type = source
            .value_type
            .require_path(&static_path)
            .map_err(|error| {
                LowerError::new(
                    LOWER_PATH_INVALID,
                    "LLM local input path is not guaranteed by its source contract",
                    step_path,
                )
                .caused_by(error.code())
            })?;
        self.emit_expression(
            step_path,
            ordinal,
            operations,
            OperationKind::Project {
                source: source.id.clone(),
                path: static_path,
            },
            value_type,
        )
    }

    fn compile_llm_response(
        &self,
        response: &ResponseConfig,
        step_path: &str,
    ) -> OneResult<(ValidatedResponseContract, TypedContract)> {
        let (response, data_schema) = match response {
            ResponseConfig::Text => (ValidatedResponseContract::Text, json!({"type":"string"})),
            ResponseConfig::Json { schema } => {
                let bundle = compile_contract_schema(&self.workflow.definitions, schema).map_err(
                    |error| {
                        LowerError::new(
                            LLM_RESPONSE_CONFIG_INVALID,
                            "LLM JSON response schema could not be compiled",
                            step_path,
                        )
                        .caused_by(error.code())
                    },
                )?;
                let schema_type =
                    SchemaType::compile(bundle.expanded_schema()).map_err(|error| {
                        LowerError::new(
                            LLM_RESPONSE_CONFIG_INVALID,
                            "LLM JSON response schema is outside the static schema profile",
                            step_path,
                        )
                        .caused_by(error.code())
                    })?;
                if matches!(schema_type.value_type(), ValueType::Any | ValueType::Never) {
                    return Err(LowerError::new(
                        LLM_RESPONSE_CONFIG_INVALID,
                        "LLM JSON response schema must define a concrete data type",
                        step_path,
                    ));
                }
                let data = TypedContract {
                    schema: bundle.validator_document().clone(),
                    value_type: schema_type.into_value_type(),
                };
                (
                    ValidatedResponseContract::Json { data },
                    bundle.expanded_schema().clone(),
                )
            }
        };
        let output_schema = json!({
            "type":"object",
            "required":["data", "finish_reason", "usage"],
            "properties":{
                "data":data_schema,
                "finish_reason":{"type":["string", "null"]},
                "usage":true
            },
            "additionalProperties":false
        });
        let output = self.compile_contract(&output_schema, step_path)?;
        Ok((response, output))
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_parallel(
        &self,
        step_path: &str,
        authored_step_path: &DslPath,
        inputs: &BTreeMap<Identifier, ValueExpr>,
        settle: ParallelSettle,
        max_concurrency: Option<usize>,
        branches: &BTreeMap<Identifier, ParallelBranch>,
        operations: &mut Vec<Operation>,
        environment: &RegionEnvironment,
    ) -> OneResult<TypedValue> {
        let inputs = self.lower_named_inputs(step_path, inputs, operations, environment)?;
        let mut lowered_branches = BTreeMap::new();
        let mut properties = BTreeMap::new();
        for (name, branch) in branches {
            let child_path = format!("{step_path}/branches/{}", name.as_str());
            let authored_child_path = authored_step_path
                .child_key("branches")
                .child_key(name.as_str());
            let result = self.compile_contract(
                &branch.output_schema,
                &format!("{child_path}/output_schema"),
            )?;
            let child = self.lower_child_region(
                ChildRegionSpec {
                    runtime_path: &child_path,
                    authored_path: &authored_child_path,
                    kind: RegionKind::ParallelBranch { name: name.clone() },
                    steps: &branch.steps,
                    result: &branch.result,
                    result_contract: result.clone(),
                },
                &inputs,
            )?;
            properties.insert(
                name.as_str().to_string(),
                PropertyType {
                    value_type: match settle {
                        ParallelSettle::All => result.value_type,
                        ParallelSettle::AllSettled => ir::settled_type(result.value_type),
                    },
                    required: true,
                },
            );
            lowered_branches.insert(name.clone(), child);
        }
        let output = TypedValue {
            id: self.authored_output_id(step_path)?,
            value_type: ValueType::Object(ObjectType {
                properties,
                additional_properties: None,
            }),
        };
        operations.push(Operation {
            id: self.authored_operation_id(step_path)?,
            output: data_definition(&output),
            kind: OperationKind::Parallel(Parallel {
                inputs: input_ids(&inputs),
                settle,
                max_concurrency,
                branches: lowered_branches,
            }),
        });
        Ok(output)
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_switch(
        &self,
        step_path: &str,
        authored_step_path: &DslPath,
        inputs: &BTreeMap<Identifier, ValueExpr>,
        output_schema: &Value,
        cases: &[SwitchCase],
        default: &SwitchDefault,
        operations: &mut Vec<Operation>,
        environment: &RegionEnvironment,
    ) -> OneResult<TypedValue> {
        let inputs = self.lower_named_inputs(step_path, inputs, operations, environment)?;
        let input_types = inputs
            .iter()
            .map(|(name, value)| (name.clone(), value.value_type.clone()))
            .collect::<BTreeMap<_, _>>();
        let result = self.compile_contract(output_schema, &format!("{step_path}/output_schema"))?;
        let result_type = result.value_type.clone();
        let mut lowered_cases = Vec::with_capacity(cases.len());
        let mut incomings = Vec::with_capacity(cases.len() + 1);
        for (case_index, case) in cases.iter().enumerate() {
            let Predicate::Cel(source) = &case.when;
            let child_path = format!("{step_path}/cases/{}", case.id.as_str());
            let authored_child_path = authored_step_path
                .child_key("cases")
                .child_index(case_index);
            let analysis = analyze_predicate(source, &input_types).map_err(|_| {
                LowerError::new(
                    LOWER_CEL_INVALID,
                    "switch case contains an invalid typed CEL predicate",
                    child_path.clone(),
                )
            })?;
            let narrowed_inputs = inputs
                .iter()
                .map(|(name, value)| {
                    (
                        name.clone(),
                        TypedValue {
                            id: value.id.clone(),
                            value_type: analysis
                                .narrowed_scope
                                .get(name)
                                .cloned()
                                .expect("predicate analysis preserves every scope binding"),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let region = self.lower_child_region(
                ChildRegionSpec {
                    runtime_path: &child_path,
                    authored_path: &authored_child_path,
                    kind: RegionKind::SwitchArm {
                        name: case.id.clone(),
                        is_default: false,
                    },
                    steps: &case.steps,
                    result: &case.result,
                    result_contract: result.clone(),
                },
                &narrowed_inputs,
            )?;
            incomings.push(region.id.clone());
            lowered_cases.push(BranchCase {
                id: case.id.clone(),
                predicate: CelProgram {
                    source: source.clone(),
                },
                region,
            });
        }
        let default_path = format!("{step_path}/default/{}", default.id.as_str());
        let authored_default_path = authored_step_path.child_key("default");
        let default_region = self.lower_child_region(
            ChildRegionSpec {
                runtime_path: &default_path,
                authored_path: &authored_default_path,
                kind: RegionKind::SwitchArm {
                    name: default.id.clone(),
                    is_default: true,
                },
                steps: &default.steps,
                result: &default.result,
                result_contract: result,
            },
            &inputs,
        )?;
        incomings.push(default_region.id.clone());

        let branch_id = self.authored_operation_id(step_path)?;
        let token_id = self.control_value_id(step_path)?;
        operations.push(Operation {
            id: branch_id.clone(),
            output: ValueDefinition {
                id: token_id.clone(),
                value_type: IrValueType::Control {
                    result_type: result_type.clone(),
                },
            },
            kind: OperationKind::Branch(Box::new(Branch {
                inputs: input_ids(&inputs),
                cases: lowered_cases,
                default: BranchDefault {
                    id: default.id.clone(),
                    region: default_region,
                },
            })),
        });

        let output = TypedValue {
            id: self.phi_value_id(step_path)?,
            value_type: result_type,
        };
        operations.push(Operation {
            id: self.phi_operation_id(step_path)?,
            output: data_definition(&output),
            kind: OperationKind::Phi(Phi {
                branch: branch_id,
                token: token_id,
                incomings,
            }),
        });
        Ok(output)
    }

    fn lower_child_region(
        &self,
        spec: ChildRegionSpec<'_>,
        captures: &BTreeMap<Identifier, TypedValue>,
    ) -> OneResult<Region> {
        let ChildRegionSpec {
            runtime_path: region_path,
            authored_path: authored_region_path,
            kind,
            steps,
            result,
            result_contract,
        } = spec;
        let (parameters, scope) = self.capture_parameters(region_path, captures)?;
        let mut environment = RegionEnvironment {
            input: None,
            run: None,
            scope,
            steps: BTreeMap::new(),
        };
        let mut operations = Vec::new();
        self.lower_steps(
            region_path,
            authored_region_path,
            steps,
            &mut operations,
            &mut environment,
        )?;
        let terminator = match result {
            BlockResult::Return(expression) => {
                let expression_path = format!("{region_path}/result");
                let mut ordinal = 0;
                let value = self.lower_expression(
                    expression,
                    &expression_path,
                    &mut ordinal,
                    &mut operations,
                    &environment,
                )?;
                self.require_assignable(
                    &value.value_type,
                    &result_contract.value_type,
                    &expression_path,
                )?;
                Terminator::RegionYield { value: value.id }
            }
            BlockResult::Raise(error) => Terminator::Raise {
                error: error.clone(),
            },
        };
        Ok(Region {
            id: self.region_id(region_path)?,
            kind,
            parameters,
            operations,
            result: result_contract,
            terminator: Some(terminator),
        })
    }

    fn capture_parameters(
        &self,
        region_path: &str,
        captures: &BTreeMap<Identifier, TypedValue>,
    ) -> OneResult<(Vec<RegionParameter>, BTreeMap<Identifier, TypedValue>)> {
        let mut parameters = Vec::with_capacity(captures.len());
        let mut scope = BTreeMap::new();
        for (index, (name, source)) in captures.iter().enumerate() {
            let ordinal = u16::try_from(index).map_err(|_| {
                LowerError::new(
                    LOWER_LIMIT_EXCEEDED,
                    "region capture count exceeds the stable identity limit",
                    region_path,
                )
            })?;
            let captured = TypedValue {
                id: self.parameter_value_id(region_path, ordinal)?,
                value_type: source.value_type.clone(),
            };
            parameters.push(RegionParameter {
                name: name.clone(),
                value: data_definition(&captured),
                source: ParameterSource::Capture {
                    source: source.id.clone(),
                },
            });
            scope.insert(name.clone(), captured);
        }
        Ok((parameters, scope))
    }

    fn lower_root_result(
        &self,
        result: &RootResult,
        output_type: &ValueType,
        operations: &mut Vec<Operation>,
        environment: &RegionEnvironment,
    ) -> OneResult<Terminator> {
        match result {
            RootResult::Raise(error) => Ok(Terminator::Raise {
                error: error.clone(),
            }),
            RootResult::Return(result) => {
                let expression_path = "/workflow/result";
                let mut ordinal = 0;
                let content = result
                    .content
                    .as_ref()
                    .map(|expression| {
                        let value = self.lower_expression(
                            expression,
                            expression_path,
                            &mut ordinal,
                            operations,
                            environment,
                        )?;
                        self.require_assignable(
                            &value.value_type,
                            &ValueType::String,
                            "workflow.result.content",
                        )?;
                        Ok(value.id)
                    })
                    .transpose()?;
                let data = self.lower_expression(
                    &result.data,
                    expression_path,
                    &mut ordinal,
                    operations,
                    environment,
                )?;
                self.require_assignable(&data.value_type, output_type, "workflow.result.data")?;
                Ok(Terminator::WorkflowReturn(IrRootReturn {
                    content,
                    format: result.format,
                    data: data.id,
                }))
            }
        }
    }

    fn lower_named_inputs(
        &self,
        expression_path: &str,
        inputs: &BTreeMap<Identifier, ValueExpr>,
        operations: &mut Vec<Operation>,
        environment: &RegionEnvironment,
    ) -> OneResult<BTreeMap<Identifier, TypedValue>> {
        let mut ordinal = 0;
        self.lower_named_inputs_with_ordinal(
            expression_path,
            inputs,
            &mut ordinal,
            operations,
            environment,
        )
    }

    fn lower_named_inputs_with_ordinal(
        &self,
        expression_path: &str,
        inputs: &BTreeMap<Identifier, ValueExpr>,
        ordinal: &mut u32,
        operations: &mut Vec<Operation>,
        environment: &RegionEnvironment,
    ) -> OneResult<BTreeMap<Identifier, TypedValue>> {
        inputs
            .iter()
            .map(|(name, expression)| {
                Ok((
                    name.clone(),
                    self.lower_expression(
                        expression,
                        expression_path,
                        ordinal,
                        operations,
                        environment,
                    )?,
                ))
            })
            .collect()
    }

    fn lower_expression(
        &self,
        expression: &ValueExpr,
        expression_path: &str,
        ordinal: &mut u32,
        operations: &mut Vec<Operation>,
        environment: &RegionEnvironment,
    ) -> OneResult<TypedValue> {
        match expression {
            ValueExpr::Literal(value) => self.emit_expression(
                expression_path,
                ordinal,
                operations,
                OperationKind::Const {
                    value: value.clone(),
                },
                infer_json_type(value),
            ),
            ValueExpr::From(path) => {
                self.lower_from(path, expression_path, ordinal, operations, environment)
            }
            ValueExpr::Object(fields) => {
                let mut lowered = BTreeMap::new();
                let mut properties = BTreeMap::new();
                for (name, expression) in fields {
                    let value = self.lower_expression(
                        expression,
                        expression_path,
                        ordinal,
                        operations,
                        environment,
                    )?;
                    properties.insert(
                        name.clone(),
                        PropertyType {
                            value_type: value.value_type.clone(),
                            required: true,
                        },
                    );
                    lowered.insert(name.clone(), value.id);
                }
                self.emit_expression(
                    expression_path,
                    ordinal,
                    operations,
                    OperationKind::Object { fields: lowered },
                    ValueType::Object(ObjectType {
                        properties,
                        additional_properties: None,
                    }),
                )
            }
            ValueExpr::Array(items) => {
                let mut lowered = Vec::with_capacity(items.len());
                let mut item_types = Vec::with_capacity(items.len());
                for expression in items {
                    let value = self.lower_expression(
                        expression,
                        expression_path,
                        ordinal,
                        operations,
                        environment,
                    )?;
                    item_types.push(value.value_type);
                    lowered.push(value.id);
                }
                let item_type = if item_types.is_empty() {
                    ValueType::Never
                } else {
                    ValueType::unify(item_types).map_err(|error| {
                        LowerError::new(
                            LOWER_TYPE_MISMATCH,
                            "array expression item types could not be unified",
                            expression_path,
                        )
                        .caused_by(error.code())
                    })?
                };
                self.emit_expression(
                    expression_path,
                    ordinal,
                    operations,
                    OperationKind::Array { items: lowered },
                    ValueType::Array(ArrayType {
                        items: Box::new(item_type),
                        min_items: items.len(),
                    }),
                )
            }
            ValueExpr::Template(template) => {
                self.lower_template(template, expression_path, ordinal, operations, environment)
            }
        }
    }

    fn lower_template(
        &self,
        template: &TemplateExpr,
        expression_path: &str,
        ordinal: &mut u32,
        operations: &mut Vec<Operation>,
        environment: &RegionEnvironment,
    ) -> OneResult<TypedValue> {
        Template::compile(&template.text).map_err(|_| {
            LowerError::new(
                LOWER_TEMPLATE_INVALID,
                "template contains invalid Handlebars syntax",
                expression_path,
            )
        })?;
        let mut bindings = BTreeMap::new();
        for (name, expression) in &template.bindings {
            let value = self.lower_expression(
                expression,
                expression_path,
                ordinal,
                operations,
                environment,
            )?;
            bindings.insert(name.clone(), value.id);
        }
        self.emit_expression(
            expression_path,
            ordinal,
            operations,
            OperationKind::Template {
                text: template.text.clone(),
                bindings,
            },
            ValueType::String,
        )
    }

    fn lower_from(
        &self,
        path: &ValuePath,
        expression_path: &str,
        ordinal: &mut u32,
        operations: &mut Vec<Operation>,
        environment: &RegionEnvironment,
    ) -> OneResult<TypedValue> {
        if matches!(path.root(), ValuePathRoot::Scope) && path.fields().is_empty() {
            let mut fields = BTreeMap::new();
            let mut properties = BTreeMap::new();
            for (name, value) in &environment.scope {
                fields.insert(name.as_str().to_string(), value.id.clone());
                properties.insert(
                    name.as_str().to_string(),
                    PropertyType {
                        value_type: value.value_type.clone(),
                        required: true,
                    },
                );
            }
            return self.emit_expression(
                expression_path,
                ordinal,
                operations,
                OperationKind::Object { fields },
                ValueType::Object(ObjectType {
                    properties,
                    additional_properties: None,
                }),
            );
        }

        let (source, segments) = match path.root() {
            ValuePathRoot::Input => (
                environment.input.as_ref().ok_or_else(|| {
                    LowerError::new(
                        LOWER_SOURCE_INVALID,
                        "workflow input is not visible in this lexical region",
                        expression_path,
                    )
                })?,
                path.fields(),
            ),
            ValuePathRoot::Run => (
                environment.run.as_ref().ok_or_else(|| {
                    LowerError::new(
                        LOWER_SOURCE_INVALID,
                        "run metadata is not visible in this lexical region",
                        expression_path,
                    )
                })?,
                path.fields(),
            ),
            ValuePathRoot::StepOutput { step } => (
                environment.steps.get(step).ok_or_else(|| {
                    LowerError::new(
                        LOWER_SOURCE_INVALID,
                        "step output is not visible in this lexical region",
                        expression_path,
                    )
                })?,
                path.fields(),
            ),
            ValuePathRoot::Scope => {
                let (binding, remaining) = path.fields().split_first().ok_or_else(|| {
                    LowerError::new(
                        LOWER_SOURCE_INVALID,
                        "scope projection is missing its capture name",
                        expression_path,
                    )
                })?;
                let source = environment
                    .scope
                    .iter()
                    .find_map(|(name, value)| (name.as_str() == binding).then_some(value))
                    .ok_or_else(|| {
                        LowerError::new(
                            LOWER_SOURCE_INVALID,
                            "scope capture is not visible in this lexical region",
                            expression_path,
                        )
                    })?;
                (source, remaining)
            }
        };
        let static_path = StaticPath::from_decoded_segments(segments).map_err(|error| {
            LowerError::new(
                LOWER_PATH_INVALID,
                "value path could not be represented as a static path",
                expression_path,
            )
            .caused_by(error.code())
        })?;
        let value_type = source
            .value_type
            .require_path(&static_path)
            .map_err(|error| {
                LowerError::new(
                    LOWER_PATH_INVALID,
                    "value path is not guaranteed by its source contract",
                    expression_path,
                )
                .caused_by(error.code())
            })?;
        self.emit_expression(
            expression_path,
            ordinal,
            operations,
            OperationKind::Project {
                source: source.id.clone(),
                path: static_path,
            },
            value_type,
        )
    }

    fn emit_expression(
        &self,
        expression_path: &str,
        ordinal: &mut u32,
        operations: &mut Vec<Operation>,
        kind: OperationKind,
        value_type: ValueType,
    ) -> OneResult<TypedValue> {
        let stable_ordinal = u16::try_from(*ordinal).map_err(|_| {
            LowerError::new(
                LOWER_LIMIT_EXCEEDED,
                "expression count exceeds the stable identity limit",
                expression_path,
            )
        })?;
        *ordinal += 1;
        let output = TypedValue {
            id: self.expression_value_id(expression_path, stable_ordinal)?,
            value_type,
        };
        operations.push(Operation {
            id: self.expression_operation_id(expression_path, stable_ordinal)?,
            output: data_definition(&output),
            kind,
        });
        Ok(output)
    }

    fn require_assignable(
        &self,
        actual: &ValueType,
        expected: &ValueType,
        location: &str,
    ) -> OneResult<()> {
        if actual.is_assignable_to(expected) {
            Ok(())
        } else {
            Err(LowerError::new(
                LOWER_TYPE_MISMATCH,
                "expression type is not assignable to its declared contract",
                location,
            ))
        }
    }

    fn region_id(&self, path: &str) -> OneResult<RegionId> {
        RegionId::new(path).map_err(|_| identity_error(path))
    }

    fn authored_operation_id(&self, path: &str) -> OneResult<OperationId> {
        OperationId::authored(path).map_err(|_| identity_error(path))
    }

    fn expression_operation_id(&self, path: &str, ordinal: u16) -> OneResult<OperationId> {
        OperationId::expression(path, ordinal).map_err(|_| identity_error(path))
    }

    fn phi_operation_id(&self, path: &str) -> OneResult<OperationId> {
        OperationId::phi(path).map_err(|_| identity_error(path))
    }

    fn parameter_value_id(&self, path: &str, ordinal: u16) -> OneResult<ValueId> {
        ValueId::parameter(path, ordinal).map_err(|_| identity_error(path))
    }

    fn authored_output_id(&self, path: &str) -> OneResult<ValueId> {
        ValueId::output(path).map_err(|_| identity_error(path))
    }

    fn expression_value_id(&self, path: &str, ordinal: u16) -> OneResult<ValueId> {
        ValueId::expression(path, ordinal).map_err(|_| identity_error(path))
    }

    fn control_value_id(&self, path: &str) -> OneResult<ValueId> {
        ValueId::control(path).map_err(|_| identity_error(path))
    }

    fn phi_value_id(&self, path: &str) -> OneResult<ValueId> {
        ValueId::phi(path).map_err(|_| identity_error(path))
    }
}

fn identity_error(location: &str) -> LowerError {
    LowerError::new(
        LOWER_IDENTITY_INVALID,
        "lowering could not construct a stable slash-qualified identity",
        location,
    )
}

fn identifier(value: &str, location: &str) -> OneResult<Identifier> {
    Identifier::parse(value).map_err(|_| identity_error(location))
}

fn sha256_label(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity("sha256:".len() + digest.len() * 2);
    encoded.push_str("sha256:");
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a string cannot fail");
    }
    encoded
}

fn template_access_type(
    root: &ValueType,
    segments: &[TemplatePathSegment],
) -> Result<ValueType, &'static str> {
    let mut current = root.clone();
    for segment in segments {
        current = match segment {
            TemplatePathSegment::Field(field) => {
                let path = StaticPath::from_decoded_segments([field.as_str()])
                    .map_err(|_| LOWER_TYPE_MISMATCH)?;
                current
                    .require_path(&path)
                    .map_err(|_| LOWER_TYPE_MISMATCH)?
            }
            TemplatePathSegment::EachItem => template_array_item(&current)?,
        };
    }
    Ok(current)
}

fn template_array_item(value_type: &ValueType) -> Result<ValueType, &'static str> {
    match value_type {
        ValueType::Array(array) => Ok((*array.items).clone()),
        ValueType::Union(variants) => ValueType::unify(
            variants
                .iter()
                .map(template_array_item)
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(|_| LOWER_TYPE_MISMATCH),
        _ => Err(LOWER_TYPE_MISMATCH),
    }
}

fn is_static_array(value_type: &ValueType) -> bool {
    template_array_item(value_type).is_ok()
}

fn is_template_scalar(value_type: &ValueType) -> bool {
    value_type.is_assignable_to(&ValueType::Union(vec![
        ValueType::String,
        ValueType::Number,
        ValueType::Boolean,
    ]))
}

fn step_id(step: &Step) -> &Identifier {
    match step {
        Step::Llm { id, .. }
        | Step::Action { id, .. }
        | Step::Parallel { id, .. }
        | Step::Switch { id, .. } => id,
    }
}

fn data_definition(value: &TypedValue) -> ValueDefinition {
    ValueDefinition {
        id: value.id.clone(),
        value_type: IrValueType::Data(value.value_type.clone()),
    }
}

fn input_ids(values: &BTreeMap<Identifier, TypedValue>) -> BTreeMap<Identifier, ValueId> {
    values
        .iter()
        .map(|(name, value)| (name.clone(), value.id.clone()))
        .collect()
}

fn infer_json_type(value: &Value) -> ValueType {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            ValueType::Literal(value.clone())
        }
        Value::Array(values) => {
            let items = if values.is_empty() {
                ValueType::Never
            } else {
                ValueType::unify(values.iter().map(infer_json_type))
                    .expect("non-empty JSON arrays always provide a type")
            };
            ValueType::Array(ArrayType {
                items: Box::new(items),
                min_items: values.len(),
            })
        }
        Value::Object(values) => ValueType::Object(ObjectType {
            properties: values
                .iter()
                .map(|(name, value)| {
                    (
                        name.clone(),
                        PropertyType {
                            value_type: infer_json_type(value),
                            required: true,
                        },
                    )
                })
                .collect(),
            additional_properties: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use semver::Version;
    use serde_json::{json, Value};

    use super::{
        lower_workflow, ResolvedActionContract, ResolvedModelContract, ResourceResolver,
        LOWER_CEL_INVALID, LOWER_SCHEMA_DIALECT_INVALID, LOWER_SCHEMA_INVALID,
        LOWER_SEMANTIC_INVALID, LOWER_TEMPLATE_INVALID, LOWER_TYPE_MISMATCH,
    };
    use crate::{
        dsl::vnext::{
            ir::{
                self, IrValueType, OperationKind, OperationRole, Region, RegionKind, TypedContract,
                ValueRole,
            },
            message::{
                AuthoredContentAtom, AuthoredContentExpr, AuthoredMessageTemplate, AuthoredRole,
                MessageListExpr, MessageSource, ResponseConfig,
            },
            plan::{CallPlan, CallTarget, ResolvedModelId, TemplateProvenance},
            raw::{
                ApiVersion, BlockResult, DocumentKind, InputContract, Metadata, OutputContract,
                ParallelBranch, ParallelSettle, Predicate, RawWorkflow, RootResult, RootReturn,
                Step, SwitchCase, SwitchDefault, WorkflowBody,
            },
            types::{
                safe_run_metadata_type, ObjectType, PropertyType, ValueType,
                SCHEMA_KEYWORD_UNSUPPORTED,
            },
            value::{Identifier, TemplateExpr, ValueExpr, ValuePath},
        },
        resources::actions::ActionDescriptorIdentity,
    };

    fn id(value: &str) -> Identifier {
        Identifier::parse(value).unwrap()
    }

    fn from(path: &str) -> ValueExpr {
        ValueExpr::From(ValuePath::parse(path).unwrap())
    }

    fn scope_value_schema() -> Value {
        json!({
            "type":"object",
            "required":["value"],
            "properties":{"value":{"$ref":"#/$defs/Text"}},
            "additionalProperties":false
        })
    }

    fn safe_error_schema() -> Value {
        ir::safe_branch_error_schema()
    }

    fn settled_scope_schema() -> Value {
        json!({
            "oneOf":[
                {
                    "type":"object",
                    "required":["status","value"],
                    "properties":{
                        "status":{"const":"ok"},
                        "value":{"$ref":"#/$defs/ScopeValue"}
                    },
                    "additionalProperties":false
                },
                {
                    "type":"object",
                    "required":["status","error"],
                    "properties":{
                        "status":{"const":"error"},
                        "error":safe_error_schema()
                    },
                    "additionalProperties":false
                }
            ]
        })
    }

    fn aggregate_schema() -> Value {
        json!({
            "type":"object",
            "required":["left","right"],
            "properties":{
                "left":settled_scope_schema(),
                "right":settled_scope_schema()
            },
            "additionalProperties":false
        })
    }

    fn definitions() -> BTreeMap<Identifier, Value> {
        BTreeMap::from([
            (id("Text"), json!({"type":"string"})),
            (id("ScopeValue"), scope_value_schema()),
        ])
    }

    fn workflow(steps: Vec<Step>, result: RootResult, output_schema: Value) -> RawWorkflow {
        RawWorkflow {
            api_version: ApiVersion::V2,
            kind: DocumentKind::Agent,
            metadata: Metadata {
                id: id("lowering_fixture"),
                name: "Lowering fixture".to_string(),
                description: String::new(),
            },
            schema_dialect: "https://json-schema.org/draft/2020-12/schema".to_string(),
            definitions: definitions(),
            prompts: BTreeMap::new(),
            errors: BTreeMap::new(),
            input: InputContract {
                schema: json!({
                    "type":"object",
                    "required":["question"],
                    "properties":{"question":{"$ref":"#/$defs/Text"}},
                    "additionalProperties":false
                }),
            },
            output: OutputContract {
                data_schema: output_schema,
            },
            workflow: WorkflowBody { steps, result },
        }
    }

    fn inline_llm(id_value: &str, content: AuthoredContentExpr) -> Step {
        Step::Llm {
            id: id(id_value),
            model: "test.model".to_string(),
            inputs: BTreeMap::new(),
            messages: MessageListExpr::Sources(vec![MessageSource::Authored(
                AuthoredMessageTemplate {
                    role: AuthoredRole::User,
                    content,
                },
            )]),
            parameters: serde_json::Map::new(),
            response: ResponseConfig::Text,
        }
    }

    fn collect_inline_template_paths(region: &Region, paths: &mut Vec<String>) {
        for operation in &region.operations {
            match &operation.kind {
                OperationKind::Call(call) => {
                    if let CallPlan::Llm(plan) = &call.plan {
                        paths.extend(plan.templates.values().filter_map(|template| {
                            let TemplateProvenance::Inline { dsl_path, .. } = &template.provenance
                            else {
                                return None;
                            };
                            Some(dsl_path.to_string())
                        }));
                    }
                }
                OperationKind::Parallel(parallel) => {
                    for child in parallel.branches.values() {
                        collect_inline_template_paths(child, paths);
                    }
                }
                OperationKind::Branch(branch) => {
                    for case in &branch.cases {
                        collect_inline_template_paths(&case.region, paths);
                    }
                    collect_inline_template_paths(&branch.default.region, paths);
                }
                _ => {}
            }
        }
    }

    fn returning(value: ValueExpr) -> BlockResult {
        BlockResult::Return(value)
    }

    #[derive(Debug, Clone, Copy)]
    struct EchoResolver;

    impl ResourceResolver for EchoResolver {
        fn resolve_action(&self, action_id: &str) -> Result<ResolvedActionContract, String> {
            if action_id != "test.echo" {
                return Err("resolver details must not escape lowering".to_string());
            }
            let input_schema = json!({
                "type":"object",
                "required":["value"],
                "properties":{"value":{"type":"string"}},
                "additionalProperties":false
            });
            let input_type = ValueType::Object(ObjectType {
                properties: BTreeMap::from([(
                    "value".to_string(),
                    PropertyType {
                        value_type: ValueType::String,
                        required: true,
                    },
                )]),
                additional_properties: None,
            });
            Ok(ResolvedActionContract {
                identity: ActionDescriptorIdentity {
                    id: "test.echo".to_string(),
                    version: Version::new(1, 0, 0),
                    descriptor_hash: "ab".repeat(32),
                },
                input: TypedContract {
                    schema: input_schema,
                    value_type: input_type,
                },
                output: TypedContract {
                    schema: json!({"type":"string"}),
                    value_type: ValueType::String,
                },
            })
        }

        fn resolve_model(
            &self,
            model: &str,
            _parameters: &Value,
        ) -> Result<ResolvedModelContract, String> {
            if model != "test.model" {
                return Err("resolver details must not escape lowering".to_string());
            }
            Ok(ResolvedModelContract {
                id: ResolvedModelId::parse("test.model@v1").unwrap(),
                capabilities: BTreeSet::new(),
            })
        }
    }

    fn valid_nested_workflow() -> RawWorkflow {
        let route = Step::Switch {
            id: id("route"),
            inputs: BTreeMap::from([(id("value"), from("scope.value"))]),
            output_schema: json!({"$ref":"#/$defs/ScopeValue"}),
            cases: vec![SwitchCase {
                id: id("nonempty"),
                when: Predicate::Cel("scope.value != ''".to_string()),
                steps: vec![Step::Action {
                    id: id("echo"),
                    call: "test.echo".to_string(),
                    inputs: BTreeMap::from([(id("value"), from("scope.value"))]),
                }],
                result: returning(ValueExpr::Object(BTreeMap::from([(
                    "value".to_string(),
                    from("steps.echo.output"),
                )]))),
            }],
            default: SwitchDefault {
                id: id("fallback"),
                steps: Vec::new(),
                result: returning(from("scope")),
            },
        };
        let fanout = Step::Parallel {
            id: id("fanout"),
            inputs: BTreeMap::from([(id("value"), from("input.question"))]),
            settle: ParallelSettle::AllSettled,
            max_concurrency: Some(2),
            branches: BTreeMap::from([
                (
                    id("left"),
                    ParallelBranch {
                        output_schema: json!({"$ref":"#/$defs/ScopeValue"}),
                        steps: vec![route],
                        result: returning(from("steps.route.output")),
                    },
                ),
                (
                    id("right"),
                    ParallelBranch {
                        output_schema: json!({"$ref":"#/$defs/ScopeValue"}),
                        steps: Vec::new(),
                        result: returning(from("scope")),
                    },
                ),
            ]),
        };
        let output_schema = json!({
            "type":"object",
            "required":["display-name"],
            "properties":{"display-name":aggregate_schema()},
            "additionalProperties":false
        });
        workflow(
            vec![fanout],
            RootResult::Return(RootReturn {
                content: None,
                format: None,
                data: ValueExpr::Object(BTreeMap::from([(
                    "display-name".to_string(),
                    from("steps.fanout.output"),
                )])),
            }),
            output_schema,
        )
    }

    #[test]
    fn lowers_nested_parallel_switch_scope_objects_and_stable_ids() {
        let workflow = valid_nested_workflow();
        let first = lower_workflow(&workflow, &EchoResolver).unwrap();
        let second = lower_workflow(&workflow, &EchoResolver).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.root.result, first.output);
        assert_eq!(first.root.id.path().as_str(), "/workflow");
        assert_eq!(
            first.root.operations[0].id.path().as_str(),
            "/workflow/fanout"
        );
        assert_eq!(
            first.root.operations[0].id.role(),
            OperationRole::Expression(0)
        );

        let parallel_operation = first
            .root
            .operations
            .iter()
            .find(|operation| matches!(operation.kind, OperationKind::Parallel(_)))
            .unwrap();
        assert_eq!(parallel_operation.id.path().as_str(), "/workflow/fanout");
        assert_eq!(parallel_operation.id.role(), OperationRole::Authored);
        let IrValueType::Data(ValueType::Object(aggregate)) = &parallel_operation.output.value_type
        else {
            panic!("parallel must produce a typed aggregate")
        };
        for branch in ["left", "right"] {
            assert!(matches!(
                aggregate.properties[branch].value_type,
                ValueType::Union(ref variants) if variants.len() == 2
            ));
        }

        let OperationKind::Parallel(parallel) = &parallel_operation.kind else {
            unreachable!()
        };
        let left = &parallel.branches[&id("left")];
        assert_eq!(left.result.schema["$ref"], json!("#/$defs/ScopeValue"));
        assert!(left.result.schema["$defs"].is_object());
        assert_eq!(left.id.path().as_str(), "/workflow/fanout/branches/left");
        assert!(matches!(
            left.kind,
            RegionKind::ParallelBranch { ref name } if name == &id("left")
        ));
        let branch_operation = left
            .operations
            .iter()
            .find(|operation| matches!(operation.kind, OperationKind::Branch(_)))
            .unwrap();
        let phi_operation = left
            .operations
            .iter()
            .find(|operation| matches!(operation.kind, OperationKind::Phi(_)))
            .unwrap();
        assert_eq!(
            branch_operation.id.path().as_str(),
            "/workflow/fanout/branches/left/route"
        );
        assert_eq!(phi_operation.id.path(), branch_operation.id.path());
        assert_eq!(phi_operation.id.role(), OperationRole::Phi);
        assert_eq!(phi_operation.output.id.role(), ValueRole::PhiOutput);

        let OperationKind::Branch(branch) = &branch_operation.kind else {
            unreachable!()
        };
        assert_eq!(branch.cases[0].region.result, branch.default.region.result);
        assert_eq!(
            branch.cases[0].region.result.schema["$ref"],
            json!("#/$defs/ScopeValue")
        );
        assert_eq!(
            branch.cases[0].region.id.path().as_str(),
            "/workflow/fanout/branches/left/route/cases/nonempty"
        );
        let action_call = branch.cases[0]
            .region
            .operations
            .iter()
            .find(|operation| matches!(operation.kind, OperationKind::Call(_)))
            .expect("switch arm must contain the lowered echo action");
        let OperationKind::Call(call) = &action_call.kind else {
            unreachable!()
        };
        assert_eq!(call.target, CallTarget::ActionCall);
        let CallPlan::Action(plan) = &call.plan else {
            panic!("echo must lower to a typed action plan")
        };
        assert_eq!(plan.action_id, "test.echo");
        assert_eq!(plan.descriptor_version, Version::new(1, 0, 0));
        assert_eq!(plan.descriptor_hash, "ab".repeat(32));
        assert_eq!(
            call.inputs,
            BTreeMap::from([(id("input"), plan.input_object.clone())])
        );
        assert_eq!(
            call.plan.dependencies(),
            BTreeSet::from([plan.input_object.clone()])
        );
        let input_object = branch.cases[0]
            .region
            .operations
            .iter()
            .find(|operation| operation.output.id == plan.input_object)
            .expect("action input object must be materialized as SSA");
        let OperationKind::Object { fields } = &input_object.kind else {
            panic!("action input must come from one object expression")
        };
        assert_eq!(fields.len(), 1);
        assert!(fields.contains_key("value"));
        assert_eq!(
            input_object.output.value_type,
            IrValueType::Data(plan.input_contract.value_type.clone())
        );
        assert_eq!(
            branch.default.region.id.path().as_str(),
            "/workflow/fanout/branches/left/route/default/fallback"
        );
        assert!(branch
            .default
            .region
            .operations
            .iter()
            .any(|operation| { matches!(operation.kind, OperationKind::Object { .. }) }));
        assert!(first
            .output
            .value_type
            .require_path_str("display-name/left/status")
            .is_ok());
    }

    #[test]
    fn inline_template_provenance_uses_exact_authored_single_and_nested_parts_paths() {
        let simple = workflow(
            vec![inline_llm(
                "answer",
                AuthoredContentExpr::Single(AuthoredContentAtom::InlineText(
                    "single template".to_string(),
                )),
            )],
            RootResult::Return(RootReturn {
                content: None,
                format: None,
                data: from("steps.answer.output.data"),
            }),
            json!({"$ref":"#/$defs/Text"}),
        );
        let ir = lower_workflow(&simple, &EchoResolver).unwrap();
        let mut paths = Vec::new();
        collect_inline_template_paths(&ir.root, &mut paths);
        assert_eq!(
            paths,
            ["$.workflow.steps[0].messages[0].content".to_string()]
        );

        let mut nested = valid_nested_workflow();
        let Step::Parallel { branches, .. } = &mut nested.workflow.steps[0] else {
            unreachable!()
        };
        let left = branches.get_mut(&id("left")).unwrap();
        let Step::Switch { cases, default, .. } = &mut left.steps[0] else {
            unreachable!()
        };
        cases[0].steps[0] = inline_llm(
            "echo",
            AuthoredContentExpr::Single(AuthoredContentAtom::InlineText(
                "case template".to_string(),
            )),
        );
        cases[0].result = returning(ValueExpr::Object(BTreeMap::from([(
            "value".to_string(),
            from("steps.echo.output.data"),
        )])));
        default.steps.push(inline_llm(
            "fallback_message",
            AuthoredContentExpr::Parts(vec![
                AuthoredContentAtom::InlineText("first part".to_string()),
                AuthoredContentAtom::InlineText("second part".to_string()),
            ]),
        ));

        let ir = lower_workflow(&nested, &EchoResolver).unwrap();
        let mut paths = Vec::new();
        collect_inline_template_paths(&ir.root, &mut paths);
        paths.sort();
        assert_eq!(
            paths,
            [
                "$.workflow.steps[0].branches.left.steps[0].cases[0].steps[0].messages[0].content",
                "$.workflow.steps[0].branches.left.steps[0].default.steps[0].messages[0].content[0]",
                "$.workflow.steps[0].branches.left.steps[0].default.steps[0].messages[0].content[1]",
            ]
        );
        assert_eq!(ir::validate(&ir), Ok(()));
    }

    #[test]
    fn semantic_template_failures_keep_decoded_coordinates_through_lowering() {
        let workflow = workflow(
            vec![inline_llm(
                "answer",
                AuthoredContentExpr::Single(AuthoredContentAtom::InlineText(
                    "第一行\n第二行 {{#if secret}}do-not-render{{/if}}".to_string(),
                )),
            )],
            RootResult::Return(RootReturn {
                content: None,
                format: None,
                data: from("input.question"),
            }),
            json!({"$ref":"#/$defs/Text"}),
        );

        let error = &lower_workflow(&workflow, &EchoResolver).unwrap_err()[0];
        assert_eq!(error.code(), LOWER_SEMANTIC_INVALID);
        assert_eq!(error.cause_code(), Some(LOWER_TEMPLATE_INVALID));
        assert_eq!(
            error.location(),
            Some("workflow.steps.answer.messages.0.content")
        );
        let decoded = error.decoded_template_span().unwrap();
        assert_eq!((decoded.line_start(), decoded.column_start()), (2, 5));
        assert!(!error.to_string().contains("secret"));
    }

    #[test]
    fn rejects_non_draft_2020_12_schema_dialect() {
        let mut workflow = valid_nested_workflow();
        workflow.schema_dialect = "http://json-schema.org/draft-07/schema#".to_string();

        let errors = lower_workflow(&workflow, &EchoResolver).unwrap_err();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code(), LOWER_SCHEMA_DIALECT_INVALID);
        assert_eq!(errors[0].location(), Some("schema_dialect"));
    }

    #[test]
    fn rejects_unmodeled_shape_keyword_in_authored_contract() {
        let mut workflow = valid_nested_workflow();
        workflow.input.schema = json!({
            "type":"object",
            "required":["question"],
            "properties":{"question":{"type":"string"}},
            "patternProperties":{"^private_":{"type":"string"}},
            "additionalProperties":false
        });

        let error = &lower_workflow(&workflow, &EchoResolver).unwrap_err()[0];
        assert_eq!(error.code(), LOWER_SCHEMA_INVALID);
        assert_eq!(error.cause_code(), Some(SCHEMA_KEYWORD_UNSUPPORTED));
        assert_eq!(error.location(), Some("input.schema"));
    }

    #[test]
    fn semantic_validation_stops_forward_and_cross_region_references() {
        let forward = workflow(
            vec![
                Step::Action {
                    id: id("first"),
                    call: "test.echo".to_string(),
                    inputs: BTreeMap::from([(id("value"), from("steps.later.output"))]),
                },
                Step::Action {
                    id: id("later"),
                    call: "test.echo".to_string(),
                    inputs: BTreeMap::from([(id("value"), from("input.question"))]),
                },
            ],
            RootResult::Return(RootReturn {
                content: None,
                format: None,
                data: ValueExpr::Literal(json!(null)),
            }),
            json!({}),
        );
        let errors = lower_workflow(&forward, &EchoResolver).unwrap_err();
        assert_eq!(errors[0].code(), LOWER_SEMANTIC_INVALID);
        assert_eq!(
            errors[0].cause_code(),
            Some("VNEXT_STEP_REFERENCE_NOT_VISIBLE")
        );

        let cross_region = workflow(
            vec![Step::Parallel {
                id: id("fanout"),
                inputs: BTreeMap::new(),
                settle: ParallelSettle::All,
                max_concurrency: Some(2),
                branches: BTreeMap::from([
                    (
                        id("left"),
                        ParallelBranch {
                            output_schema: json!({"type":"null"}),
                            steps: vec![Step::Action {
                                id: id("local"),
                                call: "test.echo".to_string(),
                                inputs: BTreeMap::from([(
                                    id("value"),
                                    ValueExpr::Literal(json!("left")),
                                )]),
                            }],
                            result: returning(ValueExpr::Literal(json!(null))),
                        },
                    ),
                    (
                        id("right"),
                        ParallelBranch {
                            output_schema: json!({"type":"string"}),
                            steps: Vec::new(),
                            result: returning(from("steps.local.output")),
                        },
                    ),
                ]),
            }],
            RootResult::Return(RootReturn {
                content: None,
                format: None,
                data: ValueExpr::Literal(json!(null)),
            }),
            json!({}),
        );
        let errors = lower_workflow(&cross_region, &EchoResolver).unwrap_err();
        assert_eq!(errors[0].code(), LOWER_SEMANTIC_INVALID);
        assert_eq!(
            errors[0].cause_code(),
            Some("VNEXT_STEP_REFERENCE_NOT_VISIBLE")
        );
    }

    #[test]
    fn rejects_child_result_type_mismatch_without_echoing_literal_data() {
        let workflow = workflow(
            vec![Step::Parallel {
                id: id("fanout"),
                inputs: BTreeMap::new(),
                settle: ParallelSettle::All,
                max_concurrency: Some(2),
                branches: BTreeMap::from([
                    (
                        id("left"),
                        ParallelBranch {
                            output_schema: json!({"type":"string"}),
                            steps: Vec::new(),
                            result: returning(ValueExpr::Literal(json!(947_311))),
                        },
                    ),
                    (
                        id("right"),
                        ParallelBranch {
                            output_schema: json!({"type":"string"}),
                            steps: Vec::new(),
                            result: returning(ValueExpr::Literal(json!("ok"))),
                        },
                    ),
                ]),
            }],
            RootResult::Return(RootReturn {
                content: None,
                format: None,
                data: ValueExpr::Literal(json!(null)),
            }),
            json!({}),
        );
        let error = &lower_workflow(&workflow, &EchoResolver).unwrap_err()[0];
        assert_eq!(error.code(), LOWER_TYPE_MISMATCH);
        assert!(!error.to_string().contains("947311"));
    }

    #[test]
    fn rejects_bad_cel_before_building_branch_ir() {
        let workflow = workflow(
            vec![Step::Switch {
                id: id("route"),
                inputs: BTreeMap::from([(id("value"), from("input.question"))]),
                output_schema: json!({"$ref":"#/$defs/Text"}),
                cases: vec![SwitchCase {
                    id: id("broken"),
                    when: Predicate::Cel("scope.value +".to_string()),
                    steps: Vec::new(),
                    result: returning(from("scope.value")),
                }],
                default: SwitchDefault {
                    id: id("fallback"),
                    steps: Vec::new(),
                    result: returning(from("scope.value")),
                },
            }],
            RootResult::Return(RootReturn {
                content: None,
                format: None,
                data: from("steps.route.output"),
            }),
            json!({"$ref":"#/$defs/Text"}),
        );
        let error = &lower_workflow(&workflow, &EchoResolver).unwrap_err()[0];
        assert_eq!(error.code(), LOWER_CEL_INVALID);
        assert!(!error.to_string().contains("scope.value +"));
    }

    #[test]
    fn rejects_non_boolean_and_unknown_cel_references_before_ir() {
        for source in [
            "scope.value",
            "input.question == 'yes'",
            "scope.missing == 'yes'",
        ] {
            let workflow = workflow(
                vec![Step::Switch {
                    id: id("route"),
                    inputs: BTreeMap::from([(id("value"), from("input.question"))]),
                    output_schema: json!({"$ref":"#/$defs/Text"}),
                    cases: vec![SwitchCase {
                        id: id("checked"),
                        when: Predicate::Cel(source.to_string()),
                        steps: Vec::new(),
                        result: returning(from("scope.value")),
                    }],
                    default: SwitchDefault {
                        id: id("fallback"),
                        steps: Vec::new(),
                        result: returning(from("scope.value")),
                    },
                }],
                RootResult::Return(RootReturn {
                    content: None,
                    format: None,
                    data: from("steps.route.output"),
                }),
                json!({"$ref":"#/$defs/Text"}),
            );
            let error = &lower_workflow(&workflow, &EchoResolver).unwrap_err()[0];
            assert_eq!(error.code(), LOWER_CEL_INVALID);
            assert!(!error.to_string().contains(source));
        }
    }

    #[test]
    fn status_predicate_narrows_all_settled_value_inside_its_arm() {
        let fanout = Step::Parallel {
            id: id("fanout"),
            inputs: BTreeMap::from([(id("question"), from("input.question"))]),
            settle: ParallelSettle::AllSettled,
            max_concurrency: Some(2),
            branches: BTreeMap::from([
                (
                    id("left"),
                    ParallelBranch {
                        output_schema: json!({"$ref":"#/$defs/Text"}),
                        steps: Vec::new(),
                        result: returning(from("scope.question")),
                    },
                ),
                (
                    id("right"),
                    ParallelBranch {
                        output_schema: json!({"$ref":"#/$defs/Text"}),
                        steps: Vec::new(),
                        result: returning(from("scope.question")),
                    },
                ),
            ]),
        };
        let route = Step::Switch {
            id: id("route"),
            inputs: BTreeMap::from([(id("candidate"), from("steps.fanout.output.left"))]),
            output_schema: json!({"$ref":"#/$defs/Text"}),
            cases: vec![SwitchCase {
                id: id("success"),
                when: Predicate::Cel("scope.candidate.status == 'ok'".to_string()),
                steps: Vec::new(),
                result: returning(from("scope.candidate.value")),
            }],
            default: SwitchDefault {
                id: id("fallback"),
                steps: Vec::new(),
                result: returning(ValueExpr::Literal(json!("unavailable"))),
            },
        };
        let workflow = workflow(
            vec![fanout, route],
            RootResult::Return(RootReturn {
                content: None,
                format: None,
                data: from("steps.route.output"),
            }),
            json!({"$ref":"#/$defs/Text"}),
        );

        let ir = lower_workflow(&workflow, &EchoResolver).unwrap();
        assert!(ir::validate(&ir).is_ok());
        let branch = ir
            .root
            .operations
            .iter()
            .find_map(|operation| match &operation.kind {
                OperationKind::Branch(branch) => Some(branch),
                _ => None,
            })
            .expect("expected lowered Branch");
        let captured = &branch.cases[0].region.parameters[0].value.value_type;
        let IrValueType::Data(captured) = captured else {
            panic!("switch captures must be data")
        };
        assert_eq!(
            captured.require_decoded_segments(["value"]).unwrap(),
            ValueType::String
        );
    }

    #[test]
    fn rejects_invalid_template_during_lowering() {
        let workflow = workflow(
            Vec::new(),
            RootResult::Return(RootReturn {
                content: None,
                format: None,
                data: ValueExpr::Template(TemplateExpr {
                    text: "{{#if broken}}".to_string(),
                    bindings: BTreeMap::new(),
                }),
            }),
            json!({"$ref":"#/$defs/Text"}),
        );

        let error = &lower_workflow(&workflow, &EchoResolver).unwrap_err()[0];
        assert_eq!(error.code(), LOWER_TEMPLATE_INVALID);
        assert!(!error.to_string().contains("{{#if broken}}"));
    }

    #[test]
    fn run_metadata_static_contract_matches_the_runtime_surface() {
        let workflow = workflow(
            Vec::new(),
            RootResult::Return(RootReturn {
                content: None,
                format: None,
                data: from("run.request_id"),
            }),
            json!({"$ref":"#/$defs/Text"}),
        );

        let ir = lower_workflow(&workflow, &EchoResolver).unwrap();
        assert_eq!(
            ir.root.parameters[1].value.value_type,
            IrValueType::Data(safe_run_metadata_type())
        );
        assert!(ir::validate(&ir).is_ok());
    }

    #[test]
    fn rejects_root_output_mismatch() {
        let workflow = workflow(
            Vec::new(),
            RootResult::Return(RootReturn {
                content: None,
                format: None,
                data: ValueExpr::Literal(json!(false)),
            }),
            json!({"type":"string"}),
        );
        let error = &lower_workflow(&workflow, &EchoResolver).unwrap_err()[0];
        assert_eq!(error.code(), LOWER_TYPE_MISMATCH);
        assert_eq!(error.location(), Some("workflow.result.data"));
    }

    #[test]
    fn scalar_literal_satisfies_an_exact_const_contract() {
        let workflow = workflow(
            Vec::new(),
            RootResult::Return(RootReturn {
                content: None,
                format: None,
                data: ValueExpr::Literal(json!("ok")),
            }),
            json!({"const":"ok"}),
        );

        let ir = lower_workflow(&workflow, &EchoResolver).unwrap();
        assert_eq!(ir.output.value_type, ValueType::Literal(json!("ok")));
    }

    #[test]
    fn lowers_empty_expression_and_literal_arrays_with_bottom_items() {
        for expression in [ValueExpr::Array(Vec::new()), ValueExpr::Literal(json!([]))] {
            let workflow = workflow(
                Vec::new(),
                RootResult::Return(RootReturn {
                    content: None,
                    format: None,
                    data: expression,
                }),
                json!({"type":"array","items":{"type":"string"}}),
            );

            let ir = lower_workflow(&workflow, &EchoResolver).unwrap();
            let value_type = ir
                .root
                .operations
                .iter()
                .find_map(|operation| match &operation.output.value_type {
                    IrValueType::Data(ValueType::Array(array)) => Some(array),
                    _ => None,
                })
                .expect("empty array expression must be lowered");
            assert_eq!(value_type.items.as_ref(), &ValueType::Never);
            assert_eq!(value_type.min_items, 0);
            assert!(ir::validate(&ir).is_ok());
        }
    }
}
