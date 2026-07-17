use std::{collections::BTreeMap, error::Error, fmt};

use handlebars::Template;
use serde_json::Value;

use super::{
    ir::{
        self, Branch, BranchCase, BranchDefault, Call, CelProgram, CompiledPrompt, IrValueType,
        Operation, OperationId, OperationKind, Parallel, ParameterSource, Phi, Region, RegionId,
        RegionKind, RegionParameter, RootReturn as IrRootReturn, Terminator, TypedContract,
        ValueDefinition, ValueId, WorkflowIr,
    },
    predicate::analyze_predicate,
    raw::{
        BlockResult, ParallelBranch, ParallelSettle, Predicate, PromptDeclaration, RawWorkflow,
        RootResult, Step, SwitchCase, SwitchDefault,
    },
    schema::compile_contract_schema,
    semantics::validate_workflow_semantics,
    types::{
        safe_run_metadata_type, ArrayType, ObjectType, PropertyType, SchemaType, StaticPath,
        ValueType,
    },
    value::{Identifier, TemplateExpr, ValueExpr, ValuePath, ValuePathRoot},
};

pub const LOWER_SEMANTIC_INVALID: &str = "VNEXT_LOWER_SEMANTIC_INVALID";
pub const LOWER_SCHEMA_DIALECT_INVALID: &str = "VNEXT_LOWER_SCHEMA_DIALECT_INVALID";
pub const LOWER_SCHEMA_INVALID: &str = "VNEXT_LOWER_SCHEMA_INVALID";
pub const LOWER_CALL_CONTRACT_INVALID: &str = "VNEXT_LOWER_CALL_CONTRACT_INVALID";
pub const LOWER_SOURCE_INVALID: &str = "VNEXT_LOWER_SOURCE_INVALID";
pub const LOWER_PATH_INVALID: &str = "VNEXT_LOWER_PATH_INVALID";
pub const LOWER_TYPE_MISMATCH: &str = "VNEXT_LOWER_TYPE_MISMATCH";
pub const LOWER_CEL_INVALID: &str = "VNEXT_LOWER_CEL_INVALID";
pub const LOWER_TEMPLATE_INVALID: &str = "VNEXT_LOWER_TEMPLATE_INVALID";
pub const LOWER_PROMPT_UNRESOLVED: &str = "VNEXT_LOWER_PROMPT_UNRESOLVED";
pub const LOWER_IDENTITY_INVALID: &str = "VNEXT_LOWER_IDENTITY_INVALID";
pub const LOWER_LIMIT_EXCEEDED: &str = "VNEXT_LOWER_LIMIT_EXCEEDED";
pub const LOWER_IR_INVALID: &str = "VNEXT_LOWER_IR_INVALID";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LowerError {
    code: &'static str,
    message: &'static str,
    location: Option<String>,
    cause_code: Option<&'static str>,
}

impl LowerError {
    fn new(code: &'static str, message: &'static str, location: impl Into<String>) -> Self {
        Self {
            code,
            message,
            location: Some(location.into()),
            cause_code: None,
        }
    }

    fn caused_by(mut self, cause_code: &'static str) -> Self {
        self.cause_code = Some(cause_code);
        self
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &'static str {
        self.message
    }

    pub fn location(&self) -> Option<&str> {
        self.location.as_deref()
    }

    pub fn cause_code(&self) -> Option<&'static str> {
        self.cause_code
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

/// A registry-resolved operation output contract. `output_schema` is retained
/// as a runtime contract by the operation registry; `output_type` is the named
/// conservative type used by lowering and the IR verifier.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedCallContract {
    pub output_schema: Value,
    pub output_type: ValueType,
}

/// Minimal boundary between structured lowering and an operation registry.
pub trait CallContractResolver {
    fn resolve_call(
        &self,
        uses: &str,
        config: &Value,
        inputs: &BTreeMap<Identifier, ValueType>,
    ) -> Result<ResolvedCallContract, String>;
}

pub type LowerResult<T> = Result<T, Vec<LowerError>>;

pub fn lower_workflow<R: CallContractResolver>(
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
                    "lowered workflow failed typed IR verification",
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

struct Lowerer<'a, R> {
    workflow: &'a RawWorkflow,
    resolver: &'a R,
}

impl<R: CallContractResolver> Lowerer<'_, R> {
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
        self.lower_steps(
            root_path,
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
                PromptDeclaration::Inline(text) => {
                    Ok((name.clone(), CompiledPrompt { text: text.clone() }))
                }
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

    fn compile_value_type(&self, schema: &Value, location: &str) -> OneResult<ValueType> {
        self.compile_contract(schema, location)
            .map(|contract| contract.value_type)
    }

    fn lower_steps(
        &self,
        region_path: &str,
        steps: &[Step],
        operations: &mut Vec<Operation>,
        environment: &mut RegionEnvironment,
    ) -> OneResult<()> {
        for step in steps {
            let id = step_id(step);
            let step_path = format!("{region_path}/{}", id.as_str());
            let output = match step {
                Step::Operation {
                    uses,
                    inputs,
                    config,
                    ..
                } => self.lower_call(&step_path, uses, inputs, config, operations, environment)?,
                Step::Parallel {
                    inputs,
                    settle,
                    max_concurrency,
                    branches,
                    ..
                } => self.lower_parallel(
                    &step_path,
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

    fn lower_call(
        &self,
        step_path: &str,
        uses: &str,
        inputs: &BTreeMap<Identifier, ValueExpr>,
        config: &Value,
        operations: &mut Vec<Operation>,
        environment: &RegionEnvironment,
    ) -> OneResult<TypedValue> {
        let inputs = self.lower_named_inputs(step_path, inputs, operations, environment)?;
        let input_types = inputs
            .iter()
            .map(|(name, value)| (name.clone(), value.value_type.clone()))
            .collect::<BTreeMap<_, _>>();
        let resolved = self
            .resolver
            .resolve_call(uses, config, &input_types)
            .map_err(|_| {
                LowerError::new(
                    LOWER_CALL_CONTRACT_INVALID,
                    "operation contract resolution failed",
                    step_path,
                )
            })?;
        let schema_type = self
            .compile_value_type(&resolved.output_schema, step_path)
            .map_err(|error| {
                let cause = error.cause_code().unwrap_or(error.code());
                LowerError::new(
                    LOWER_CALL_CONTRACT_INVALID,
                    "operation output schema could not be compiled",
                    step_path,
                )
                .caused_by(cause)
            })?;
        if !types_equivalent(&schema_type, &resolved.output_type) {
            return Err(LowerError::new(
                LOWER_CALL_CONTRACT_INVALID,
                "operation output schema and named output type disagree",
                step_path,
            ));
        }

        let output = TypedValue {
            id: self.authored_output_id(step_path)?,
            value_type: resolved.output_type,
        };
        operations.push(Operation {
            id: self.authored_operation_id(step_path)?,
            output: data_definition(&output),
            kind: OperationKind::Call(Call {
                uses: uses.to_string(),
                inputs: input_ids(&inputs),
                config: config.clone(),
            }),
        });
        Ok(output)
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_parallel(
        &self,
        step_path: &str,
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
            let result = self.compile_contract(
                &branch.output_schema,
                &format!("{child_path}/output_schema"),
            )?;
            let child = self.lower_child_region(
                &child_path,
                RegionKind::ParallelBranch { name: name.clone() },
                &branch.steps,
                &branch.result,
                result.clone(),
                &inputs,
            )?;
            properties.insert(
                name.as_str().to_string(),
                PropertyType {
                    value_type: match settle {
                        ParallelSettle::All => result.value_type,
                        ParallelSettle::AllSettled => settled_type(result.value_type),
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
        for case in cases {
            let Predicate::Cel(source) = &case.when;
            let child_path = format!("{step_path}/cases/{}", case.id.as_str());
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
                &child_path,
                RegionKind::SwitchArm {
                    name: case.id.clone(),
                    is_default: false,
                },
                &case.steps,
                &case.result,
                result.clone(),
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
        let default_region = self.lower_child_region(
            &default_path,
            RegionKind::SwitchArm {
                name: default.id.clone(),
                is_default: true,
            },
            &default.steps,
            &default.result,
            result,
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
        region_path: &str,
        kind: RegionKind,
        steps: &[Step],
        result: &BlockResult,
        result_contract: TypedContract,
        captures: &BTreeMap<Identifier, TypedValue>,
    ) -> OneResult<Region> {
        let (parameters, scope) = self.capture_parameters(region_path, captures)?;
        let mut environment = RegionEnvironment {
            input: None,
            run: None,
            scope,
            steps: BTreeMap::new(),
        };
        let mut operations = Vec::new();
        self.lower_steps(region_path, steps, &mut operations, &mut environment)?;
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
        inputs
            .iter()
            .map(|(name, expression)| {
                Ok((
                    name.clone(),
                    self.lower_expression(
                        expression,
                        expression_path,
                        &mut ordinal,
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
                    ValueType::Any
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
            ValueExpr::Prompt(prompt) => self.emit_expression(
                expression_path,
                ordinal,
                operations,
                OperationKind::Prompt {
                    prompt: prompt.clone(),
                },
                ValueType::String,
            ),
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

fn step_id(step: &Step) -> &Identifier {
    match step {
        Step::Operation { id, .. } | Step::Parallel { id, .. } | Step::Switch { id, .. } => id,
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

fn types_equivalent(left: &ValueType, right: &ValueType) -> bool {
    left.is_assignable_to(right) && right.is_assignable_to(left)
}

fn infer_json_type(value: &Value) -> ValueType {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            ValueType::Literal(value.clone())
        }
        Value::Array(values) => {
            let items = if values.is_empty() {
                ValueType::Any
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

fn settled_type(value_type: ValueType) -> ValueType {
    let ok = object_type([
        (
            "status",
            ValueType::Literal(Value::String("ok".to_string())),
        ),
        ("value", value_type),
    ]);
    let safe_error = object_type([
        ("category", ValueType::String),
        ("code", ValueType::String),
        ("retryable", ValueType::Boolean),
        ("origin", ValueType::String),
    ]);
    let error = object_type([
        (
            "status",
            ValueType::Literal(Value::String("error".to_string())),
        ),
        ("error", safe_error),
    ]);
    ValueType::Union(vec![ok, error])
}

fn object_type<const N: usize>(fields: [(&str, ValueType); N]) -> ValueType {
    ValueType::Object(ObjectType {
        properties: fields
            .into_iter()
            .map(|(name, value_type)| {
                (
                    name.to_string(),
                    PropertyType {
                        value_type,
                        required: true,
                    },
                )
            })
            .collect(),
        additional_properties: None,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::{json, Value};

    use super::{
        lower_workflow, CallContractResolver, ResolvedCallContract, LOWER_CEL_INVALID,
        LOWER_SCHEMA_DIALECT_INVALID, LOWER_SCHEMA_INVALID, LOWER_SEMANTIC_INVALID,
        LOWER_TEMPLATE_INVALID, LOWER_TYPE_MISMATCH,
    };
    use crate::dsl::vnext::{
        ir::{self, IrValueType, OperationKind, OperationRole, RegionKind, ValueRole},
        raw::{
            ApiVersion, BlockResult, DocumentKind, InputContract, Metadata, OutputContract,
            ParallelBranch, ParallelSettle, Predicate, RawWorkflow, RootResult, RootReturn, Step,
            SwitchCase, SwitchDefault, WorkflowBody,
        },
        types::{safe_run_metadata_type, ValueType, SCHEMA_KEYWORD_UNSUPPORTED},
        value::{Identifier, TemplateExpr, ValueExpr, ValuePath},
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
        json!({
            "type":"object",
            "required":["category","code","retryable","origin"],
            "properties":{
                "category":{"type":"string"},
                "code":{"type":"string"},
                "retryable":{"type":"boolean"},
                "origin":{"type":"string"}
            },
            "additionalProperties":false
        })
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

    fn returning(value: ValueExpr) -> BlockResult {
        BlockResult::Return(value)
    }

    #[derive(Debug, Clone, Copy)]
    struct EchoResolver;

    impl CallContractResolver for EchoResolver {
        fn resolve_call(
            &self,
            uses: &str,
            _config: &Value,
            inputs: &BTreeMap<Identifier, ValueType>,
        ) -> Result<ResolvedCallContract, String> {
            if uses != "test.echo"
                || inputs.len() != 1
                || inputs.get(&id("value")) != Some(&ValueType::String)
            {
                return Err("resolver details must not escape lowering".to_string());
            }
            Ok(ResolvedCallContract {
                output_schema: json!({"$ref":"#/$defs/Text"}),
                output_type: ValueType::String,
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
                steps: vec![Step::Operation {
                    id: id("echo"),
                    uses: "test.echo".to_string(),
                    inputs: BTreeMap::from([(id("value"), from("scope.value"))]),
                    config: json!({"mode":"typed"}),
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
                Step::Operation {
                    id: id("first"),
                    uses: "test.echo".to_string(),
                    inputs: BTreeMap::from([(id("value"), from("steps.later.output"))]),
                    config: json!({}),
                },
                Step::Operation {
                    id: id("later"),
                    uses: "test.echo".to_string(),
                    inputs: BTreeMap::from([(id("value"), from("input.question"))]),
                    config: json!({}),
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
                            steps: vec![Step::Operation {
                                id: id("local"),
                                uses: "test.echo".to_string(),
                                inputs: BTreeMap::from([(
                                    id("value"),
                                    ValueExpr::Literal(json!("left")),
                                )]),
                                config: json!({}),
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
}
