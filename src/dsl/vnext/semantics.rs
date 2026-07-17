use std::{collections::BTreeSet, fmt};

use super::{
    raw::{BlockResult, ParallelBranch, RawWorkflow, RootResult, Step, SwitchCase, SwitchDefault},
    value::{Identifier, ValueExpr, ValuePathRoot},
};

pub const MAX_STATIC_PARALLEL_CONCURRENCY: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SemanticErrorCode {
    DuplicateStepId,
    DuplicateSwitchCaseId,
    ParallelBranchCountInvalid,
    ParallelConcurrencyInvalid,
    StepReferenceNotVisible,
    ScopeBindingNotDeclared,
    RootReferenceNotVisible,
    PromptNotDeclared,
    ErrorNotDeclared,
    RootFormatRequired,
    RootFormatWithoutContent,
}

impl SemanticErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DuplicateStepId => "VNEXT_STEP_ID_DUPLICATE",
            Self::DuplicateSwitchCaseId => "VNEXT_SWITCH_CASE_ID_DUPLICATE",
            Self::ParallelBranchCountInvalid => "VNEXT_PARALLEL_BRANCH_COUNT_INVALID",
            Self::ParallelConcurrencyInvalid => "VNEXT_PARALLEL_CONCURRENCY_INVALID",
            Self::StepReferenceNotVisible => "VNEXT_STEP_REFERENCE_NOT_VISIBLE",
            Self::ScopeBindingNotDeclared => "VNEXT_SCOPE_BINDING_NOT_DECLARED",
            Self::RootReferenceNotVisible => "VNEXT_ROOT_REFERENCE_NOT_VISIBLE",
            Self::PromptNotDeclared => "VNEXT_PROMPT_NOT_DECLARED",
            Self::ErrorNotDeclared => "VNEXT_ERROR_NOT_DECLARED",
            Self::RootFormatRequired => "VNEXT_ROOT_FORMAT_REQUIRED",
            Self::RootFormatWithoutContent => "VNEXT_ROOT_FORMAT_WITHOUT_CONTENT",
        }
    }

    fn message(self) -> &'static str {
        match self {
            Self::DuplicateStepId => "step ids must be unique within one region",
            Self::DuplicateSwitchCaseId => {
                "switch case and default ids must be unique within one switch"
            }
            Self::ParallelBranchCountInvalid => "parallel must declare at least two branches",
            Self::ParallelConcurrencyInvalid => {
                "parallel max_concurrency is outside the static limit"
            }
            Self::StepReferenceNotVisible => {
                "step output reference is not a completed prior sibling in this region"
            }
            Self::ScopeBindingNotDeclared => {
                "scope reference is not declared by the owning block with bindings"
            }
            Self::RootReferenceNotVisible => {
                "input and run references are visible only in the workflow root"
            }
            Self::PromptNotDeclared => "value expression references an undeclared prompt",
            Self::ErrorNotDeclared => "result references an undeclared workflow error",
            Self::RootFormatRequired => "root return content requires a format",
            Self::RootFormatWithoutContent => "root return format requires content",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticError {
    code: SemanticErrorCode,
    location: String,
}

impl SemanticError {
    fn new(code: SemanticErrorCode, location: impl Into<String>) -> Self {
        Self {
            code,
            location: location.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code.as_str()
    }

    pub fn message(&self) -> &'static str {
        self.code.message()
    }

    pub fn location(&self) -> &str {
        &self.location
    }
}

impl fmt::Display for SemanticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} at {}: {}",
            self.code(),
            self.location,
            self.message()
        )
    }
}

impl std::error::Error for SemanticError {}

pub fn validate_workflow_semantics(workflow: &RawWorkflow) -> Result<(), Vec<SemanticError>> {
    let mut validator = Validator {
        prompts: workflow.prompts.keys().cloned().collect(),
        declared_errors: workflow.errors.keys().cloned().collect(),
        errors: Vec::new(),
    };
    let root_scope = BTreeSet::new();
    let completed = validator.validate_steps(
        &workflow.workflow.steps,
        &root_scope,
        RegionVisibility::Root,
        "workflow",
    );
    validator.validate_root_result(&workflow.workflow.result, &completed, &root_scope);

    if validator.errors.is_empty() {
        Ok(())
    } else {
        Err(validator.errors)
    }
}

struct Validator {
    prompts: BTreeSet<Identifier>,
    declared_errors: BTreeSet<Identifier>,
    errors: Vec<SemanticError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegionVisibility {
    Root,
    Child,
}

impl Validator {
    fn validate_steps(
        &mut self,
        steps: &[Step],
        scope: &BTreeSet<Identifier>,
        visibility: RegionVisibility,
        region: &str,
    ) -> BTreeSet<Identifier> {
        let mut completed = BTreeSet::new();
        for step in steps {
            let id = step_id(step);
            let location = format!("{region}.steps.{id}");
            if completed.contains(id) {
                self.error(SemanticErrorCode::DuplicateStepId, &location);
            }

            match step {
                Step::Operation { inputs, .. } => {
                    self.validate_bindings(
                        inputs.values(),
                        &completed,
                        scope,
                        visibility,
                        &location,
                    );
                }
                Step::Parallel {
                    inputs,
                    max_concurrency,
                    branches,
                    ..
                } => {
                    self.validate_bindings(
                        inputs.values(),
                        &completed,
                        scope,
                        visibility,
                        &location,
                    );
                    self.validate_parallel_shape(branches, *max_concurrency, &location);
                    let child_scope = inputs.keys().cloned().collect::<BTreeSet<_>>();
                    for (branch_id, branch) in branches {
                        self.validate_parallel_branch(
                            branch,
                            &child_scope,
                            &format!("{location}.branches.{branch_id}"),
                        );
                    }
                }
                Step::Switch {
                    inputs,
                    cases,
                    default,
                    ..
                } => {
                    self.validate_bindings(
                        inputs.values(),
                        &completed,
                        scope,
                        visibility,
                        &location,
                    );
                    let child_scope = inputs.keys().cloned().collect::<BTreeSet<_>>();
                    self.validate_switch(cases, default, &child_scope, &location);
                }
            }
            completed.insert(id.clone());
        }
        completed
    }

    fn validate_parallel_shape(
        &mut self,
        branches: &std::collections::BTreeMap<Identifier, ParallelBranch>,
        max_concurrency: Option<usize>,
        location: &str,
    ) {
        if branches.len() < 2 {
            self.error(SemanticErrorCode::ParallelBranchCountInvalid, location);
        }
        if max_concurrency
            .is_some_and(|value| value == 0 || value > MAX_STATIC_PARALLEL_CONCURRENCY)
        {
            self.error(SemanticErrorCode::ParallelConcurrencyInvalid, location);
        }
    }

    fn validate_parallel_branch(
        &mut self,
        branch: &ParallelBranch,
        scope: &BTreeSet<Identifier>,
        location: &str,
    ) {
        let completed =
            self.validate_steps(&branch.steps, scope, RegionVisibility::Child, location);
        self.validate_block_result(&branch.result, &completed, scope, location);
    }

    fn validate_switch(
        &mut self,
        cases: &[SwitchCase],
        default: &SwitchDefault,
        scope: &BTreeSet<Identifier>,
        location: &str,
    ) {
        let mut ids = BTreeSet::new();
        for case in cases {
            let case_location = format!("{location}.cases.{}", case.id);
            if !ids.insert(case.id.clone()) {
                self.error(SemanticErrorCode::DuplicateSwitchCaseId, &case_location);
            }
            let completed =
                self.validate_steps(&case.steps, scope, RegionVisibility::Child, &case_location);
            self.validate_block_result(&case.result, &completed, scope, &case_location);
        }

        let default_location = format!("{location}.default.{}", default.id);
        if !ids.insert(default.id.clone()) {
            self.error(SemanticErrorCode::DuplicateSwitchCaseId, &default_location);
        }
        let completed = self.validate_steps(
            &default.steps,
            scope,
            RegionVisibility::Child,
            &default_location,
        );
        self.validate_block_result(&default.result, &completed, scope, &default_location);
    }

    fn validate_bindings<'a>(
        &mut self,
        values: impl IntoIterator<Item = &'a ValueExpr>,
        completed: &BTreeSet<Identifier>,
        scope: &BTreeSet<Identifier>,
        visibility: RegionVisibility,
        location: &str,
    ) {
        for value in values {
            self.validate_value(value, completed, scope, visibility, location);
        }
    }

    fn validate_value(
        &mut self,
        expression: &ValueExpr,
        completed: &BTreeSet<Identifier>,
        scope: &BTreeSet<Identifier>,
        visibility: RegionVisibility,
        location: &str,
    ) {
        match expression {
            ValueExpr::Literal(_) => {}
            ValueExpr::From(path) => match path.root() {
                ValuePathRoot::StepOutput { step } if !completed.contains(step) => {
                    self.error(SemanticErrorCode::StepReferenceNotVisible, location);
                }
                ValuePathRoot::Scope => {
                    if let Some(binding) = path.fields().first() {
                        if !scope.iter().any(|declared| declared.as_str() == binding) {
                            self.error(SemanticErrorCode::ScopeBindingNotDeclared, location);
                        }
                    }
                }
                ValuePathRoot::Input | ValuePathRoot::Run
                    if visibility == RegionVisibility::Child =>
                {
                    self.error(SemanticErrorCode::RootReferenceNotVisible, location);
                }
                ValuePathRoot::Input | ValuePathRoot::Run | ValuePathRoot::StepOutput { .. } => {}
            },
            ValueExpr::Object(fields) => {
                self.validate_bindings(fields.values(), completed, scope, visibility, location);
            }
            ValueExpr::Array(values) => {
                self.validate_bindings(values, completed, scope, visibility, location);
            }
            ValueExpr::Template(template) => {
                self.validate_bindings(
                    template.bindings.values(),
                    completed,
                    scope,
                    visibility,
                    location,
                );
            }
            ValueExpr::Prompt(prompt) => {
                if !self.prompts.contains(prompt) {
                    self.error(SemanticErrorCode::PromptNotDeclared, location);
                }
            }
        }
    }

    fn validate_block_result(
        &mut self,
        result: &BlockResult,
        completed: &BTreeSet<Identifier>,
        scope: &BTreeSet<Identifier>,
        location: &str,
    ) {
        match result {
            BlockResult::Return(value) => {
                self.validate_value(
                    value,
                    completed,
                    scope,
                    RegionVisibility::Child,
                    &format!("{location}.result"),
                );
            }
            BlockResult::Raise(error) => {
                self.validate_error(error, &format!("{location}.result"));
            }
        }
    }

    fn validate_root_result(
        &mut self,
        result: &RootResult,
        completed: &BTreeSet<Identifier>,
        scope: &BTreeSet<Identifier>,
    ) {
        match result {
            RootResult::Return(result) => {
                match (&result.content, result.format) {
                    (Some(_), None) => {
                        self.error(SemanticErrorCode::RootFormatRequired, "workflow.result")
                    }
                    (None, Some(_)) => self.error(
                        SemanticErrorCode::RootFormatWithoutContent,
                        "workflow.result",
                    ),
                    (Some(_), Some(_)) | (None, None) => {}
                }
                if let Some(content) = &result.content {
                    self.validate_value(
                        content,
                        completed,
                        scope,
                        RegionVisibility::Root,
                        "workflow.result.content",
                    );
                }
                self.validate_value(
                    &result.data,
                    completed,
                    scope,
                    RegionVisibility::Root,
                    "workflow.result.data",
                );
            }
            RootResult::Raise(error) => self.validate_error(error, "workflow.result"),
        }
    }

    fn validate_error(&mut self, error: &Identifier, location: &str) {
        if !self.declared_errors.contains(error) {
            self.error(SemanticErrorCode::ErrorNotDeclared, location);
        }
    }

    fn error(&mut self, code: SemanticErrorCode, location: impl Into<String>) {
        self.errors.push(SemanticError::new(code, location));
    }
}

fn step_id(step: &Step) -> &Identifier {
    match step {
        Step::Operation { id, .. } | Step::Parallel { id, .. } | Step::Switch { id, .. } => id,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{validate_workflow_semantics, MAX_STATIC_PARALLEL_CONCURRENCY};
    use crate::dsl::vnext::{
        raw::{
            ApiVersion, BlockResult, DocumentKind, ErrorCategory, ErrorDeclaration, InputContract,
            Metadata, OutputContract, OutputFormat, ParallelBranch, ParallelSettle,
            PromptDeclaration, RawWorkflow, RootResult, RootReturn, Step, SwitchCase,
            SwitchDefault, WorkflowBody,
        },
        value::{Identifier, TemplateExpr, ValueExpr, ValuePath},
    };

    fn id(value: &str) -> Identifier {
        Identifier::parse(value).unwrap()
    }

    fn source(value: &str) -> ValueExpr {
        ValueExpr::From(ValuePath::parse(value).unwrap())
    }

    fn operation(name: &str, inputs: BTreeMap<Identifier, ValueExpr>) -> Step {
        Step::Operation {
            id: id(name),
            uses: "test.operation".to_string(),
            inputs,
            config: json!({}),
        }
    }

    fn returning(value: ValueExpr) -> BlockResult {
        BlockResult::Return(value)
    }

    fn branch(steps: Vec<Step>, result: BlockResult) -> ParallelBranch {
        ParallelBranch {
            output_schema: json!({}),
            steps,
            result,
        }
    }

    fn workflow(steps: Vec<Step>, result: RootResult) -> RawWorkflow {
        RawWorkflow {
            api_version: ApiVersion::V2,
            kind: DocumentKind::Agent,
            metadata: Metadata {
                id: id("test_agent"),
                name: "Test Agent".to_string(),
                description: String::new(),
            },
            schema_dialect: "https://json-schema.org/draft/2020-12/schema".to_string(),
            definitions: BTreeMap::new(),
            prompts: BTreeMap::from([(
                id("system"),
                PromptDeclaration::Inline("system prompt".to_string()),
            )]),
            errors: BTreeMap::from([(
                id("declared"),
                ErrorDeclaration {
                    category: ErrorCategory::Workflow,
                    code: "WORKFLOW_DECLARED".to_string(),
                    public_message: "declared failure".to_string(),
                },
            )]),
            input: InputContract { schema: json!({}) },
            output: OutputContract {
                data_schema: json!({}),
            },
            workflow: WorkflowBody { steps, result },
        }
    }

    fn root_return(value: ValueExpr) -> RootResult {
        RootResult::Return(RootReturn {
            content: None,
            format: None,
            data: value,
        })
    }

    fn error_codes(workflow: &RawWorkflow) -> Vec<&'static str> {
        validate_workflow_semantics(workflow)
            .unwrap_err()
            .iter()
            .map(|error| error.code())
            .collect()
    }

    #[test]
    fn accepts_local_predecessors_and_explicit_child_scope_capture() {
        let prepare = operation(
            "prepare",
            BTreeMap::from([
                (id("question"), source("input.question")),
                (id("run_id"), source("run.id")),
                (id("prompt"), ValueExpr::Prompt(id("system"))),
            ]),
        );
        let parallel = Step::Parallel {
            id: id("fanout"),
            inputs: BTreeMap::from([(id("captured"), source("steps.prepare.output"))]),
            settle: ParallelSettle::AllSettled,
            max_concurrency: Some(2),
            branches: BTreeMap::from([
                (
                    id("left"),
                    branch(
                        vec![operation(
                            "analyze",
                            BTreeMap::from([(id("value"), source("scope.captured"))]),
                        )],
                        returning(source("steps.analyze.output")),
                    ),
                ),
                (
                    id("right"),
                    branch(Vec::new(), returning(source("scope.captured"))),
                ),
            ]),
        };
        let workflow = workflow(
            vec![prepare, parallel],
            root_return(source("steps.fanout.output")),
        );

        assert_eq!(validate_workflow_semantics(&workflow), Ok(()));
    }

    #[test]
    fn rejects_forward_and_unknown_root_result_references() {
        let forward = operation(
            "first",
            BTreeMap::from([(id("value"), source("steps.later.output"))]),
        );
        let workflow = workflow(
            vec![forward, operation("later", BTreeMap::new())],
            root_return(source("steps.missing.output")),
        );

        assert_eq!(
            error_codes(&workflow),
            vec![
                "VNEXT_STEP_REFERENCE_NOT_VISIBLE",
                "VNEXT_STEP_REFERENCE_NOT_VISIBLE"
            ]
        );
    }

    #[test]
    fn rejects_parent_step_reference_from_child_result() {
        let parallel = Step::Parallel {
            id: id("fanout"),
            inputs: BTreeMap::new(),
            settle: ParallelSettle::AllSettled,
            max_concurrency: None,
            branches: BTreeMap::from([
                (
                    id("left"),
                    branch(Vec::new(), returning(source("steps.prepare.output"))),
                ),
                (
                    id("right"),
                    branch(Vec::new(), returning(ValueExpr::Literal(json!(null)))),
                ),
            ]),
        };
        let workflow = workflow(
            vec![operation("prepare", BTreeMap::new()), parallel],
            root_return(source("steps.fanout.output")),
        );

        assert_eq!(
            error_codes(&workflow),
            vec!["VNEXT_STEP_REFERENCE_NOT_VISIBLE"]
        );
    }

    #[test]
    fn rejects_cross_branch_reference() {
        let parallel = Step::Parallel {
            id: id("fanout"),
            inputs: BTreeMap::new(),
            settle: ParallelSettle::AllSettled,
            max_concurrency: None,
            branches: BTreeMap::from([
                (
                    id("left"),
                    branch(
                        vec![operation("only_left", BTreeMap::new())],
                        returning(source("steps.only_left.output")),
                    ),
                ),
                (
                    id("right"),
                    branch(Vec::new(), returning(source("steps.only_left.output"))),
                ),
            ]),
        };
        let workflow = workflow(vec![parallel], root_return(source("steps.fanout.output")));

        assert_eq!(
            error_codes(&workflow),
            vec!["VNEXT_STEP_REFERENCE_NOT_VISIBLE"]
        );
    }

    #[test]
    fn rejects_input_and_run_references_inside_child_regions() {
        let parallel = Step::Parallel {
            id: id("fanout"),
            inputs: BTreeMap::new(),
            settle: ParallelSettle::AllSettled,
            max_concurrency: None,
            branches: BTreeMap::from([
                (
                    id("left"),
                    branch(
                        vec![operation(
                            "work",
                            BTreeMap::from([
                                (id("question"), source("input.question")),
                                (id("run_id"), source("run.id")),
                            ]),
                        )],
                        returning(source("steps.work.output")),
                    ),
                ),
                (
                    id("right"),
                    branch(Vec::new(), returning(ValueExpr::Literal(json!(null)))),
                ),
            ]),
        };
        let workflow = workflow(vec![parallel], root_return(source("steps.fanout.output")));

        assert_eq!(
            error_codes(&workflow),
            vec![
                "VNEXT_ROOT_REFERENCE_NOT_VISIBLE",
                "VNEXT_ROOT_REFERENCE_NOT_VISIBLE"
            ]
        );
    }

    #[test]
    fn recursively_rejects_unknown_prompt_and_scope_binding() {
        let nested = ValueExpr::Object(BTreeMap::from([(
            "items".to_string(),
            ValueExpr::Array(vec![ValueExpr::Template(TemplateExpr {
                text: "{{value}}".to_string(),
                bindings: BTreeMap::from([(id("value"), ValueExpr::Prompt(id("unknown_prompt")))]),
            })]),
        )]));
        let left_branch = branch(
            vec![operation(
                "child",
                BTreeMap::from([(id("value"), source("scope.not_captured"))]),
            )],
            returning(ValueExpr::Literal(json!(null))),
        );
        let parallel = Step::Parallel {
            id: id("fanout"),
            inputs: BTreeMap::new(),
            settle: ParallelSettle::AllSettled,
            max_concurrency: None,
            branches: BTreeMap::from([
                (id("left"), left_branch),
                (
                    id("right"),
                    branch(Vec::new(), returning(ValueExpr::Literal(json!(null)))),
                ),
            ]),
        };
        let workflow = workflow(
            vec![
                operation("prepare", BTreeMap::from([(id("nested"), nested)])),
                parallel,
            ],
            root_return(source("steps.fanout.output")),
        );

        assert_eq!(
            error_codes(&workflow),
            vec![
                "VNEXT_PROMPT_NOT_DECLARED",
                "VNEXT_SCOPE_BINDING_NOT_DECLARED"
            ]
        );
    }

    #[test]
    fn rejects_undeclared_root_and_child_errors() {
        let parallel = Step::Parallel {
            id: id("fanout"),
            inputs: BTreeMap::new(),
            settle: ParallelSettle::AllSettled,
            max_concurrency: None,
            branches: BTreeMap::from([
                (
                    id("left"),
                    branch(Vec::new(), BlockResult::Raise(id("missing_child"))),
                ),
                (
                    id("right"),
                    branch(Vec::new(), BlockResult::Raise(id("declared"))),
                ),
            ]),
        };
        let workflow = workflow(vec![parallel], RootResult::Raise(id("missing_root")));

        assert_eq!(
            error_codes(&workflow),
            vec!["VNEXT_ERROR_NOT_DECLARED", "VNEXT_ERROR_NOT_DECLARED"]
        );
    }

    #[test]
    fn rejects_duplicate_ids_and_invalid_parallel_shape() {
        let duplicate_switch = Step::Switch {
            id: id("route"),
            inputs: BTreeMap::new(),
            output_schema: json!({}),
            cases: vec![SwitchCase {
                id: id("same"),
                when: crate::dsl::vnext::raw::Predicate::Cel("true".to_string()),
                steps: Vec::new(),
                result: returning(ValueExpr::Literal(json!(true))),
            }],
            default: SwitchDefault {
                id: id("same"),
                steps: Vec::new(),
                result: returning(ValueExpr::Literal(json!(false))),
            },
        };
        let invalid_parallel = Step::Parallel {
            id: id("parallel"),
            inputs: BTreeMap::new(),
            settle: ParallelSettle::All,
            max_concurrency: Some(MAX_STATIC_PARALLEL_CONCURRENCY + 1),
            branches: BTreeMap::from([(
                id("only"),
                branch(Vec::new(), returning(ValueExpr::Literal(json!(null)))),
            )]),
        };
        let workflow = workflow(
            vec![
                operation("duplicate", BTreeMap::new()),
                operation("duplicate", BTreeMap::new()),
                duplicate_switch,
                invalid_parallel,
            ],
            root_return(ValueExpr::Literal(json!(null))),
        );

        assert_eq!(
            error_codes(&workflow),
            vec![
                "VNEXT_STEP_ID_DUPLICATE",
                "VNEXT_SWITCH_CASE_ID_DUPLICATE",
                "VNEXT_PARALLEL_BRANCH_COUNT_INVALID",
                "VNEXT_PARALLEL_CONCURRENCY_INVALID"
            ]
        );
    }

    #[test]
    fn allows_same_local_step_id_in_disjoint_branches() {
        let parallel = Step::Parallel {
            id: id("fanout"),
            inputs: BTreeMap::new(),
            settle: ParallelSettle::All,
            max_concurrency: Some(2),
            branches: BTreeMap::from([
                (
                    id("left"),
                    branch(
                        vec![operation("work", BTreeMap::new())],
                        returning(source("steps.work.output")),
                    ),
                ),
                (
                    id("right"),
                    branch(
                        vec![operation("work", BTreeMap::new())],
                        returning(source("steps.work.output")),
                    ),
                ),
            ]),
        };
        let workflow = workflow(vec![parallel], root_return(source("steps.fanout.output")));

        assert_eq!(validate_workflow_semantics(&workflow), Ok(()));
    }

    #[test]
    fn validates_root_return_content_and_format_as_one_structure() {
        let content_without_format = workflow(
            Vec::new(),
            RootResult::Return(RootReturn {
                content: Some(ValueExpr::Literal(json!("answer"))),
                format: None,
                data: ValueExpr::Literal(json!({})),
            }),
        );
        assert_eq!(
            error_codes(&content_without_format),
            vec!["VNEXT_ROOT_FORMAT_REQUIRED"]
        );

        let format_without_content = workflow(
            Vec::new(),
            RootResult::Return(RootReturn {
                content: None,
                format: Some(OutputFormat::Markdown),
                data: ValueExpr::Literal(json!({})),
            }),
        );
        assert_eq!(
            error_codes(&format_without_content),
            vec!["VNEXT_ROOT_FORMAT_WITHOUT_CONTENT"]
        );
    }
}
