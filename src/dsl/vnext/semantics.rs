use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use crate::dsl::SourceSpan;

use super::{
    message::{
        AuthoredContentAtom, AuthoredMessageTemplate, AuthoredRole, MessageListExpr, MessageSource,
    },
    predicate::referenced_scope_bindings,
    raw::{
        BlockResult, ParallelBranch, Predicate, PromptDeclaration, RawWorkflow, RootResult, Step,
        SwitchCase, SwitchDefault,
    },
    template::compile_template,
    value::{Identifier, LocalInputRef, ValueExpr, ValuePathRoot},
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
    ControlInputUnused,
    LlmInputNotDeclared,
    LlmInputUnused,
    PromptNotDeclared,
    LlmSystemPrefixInvalid,
    LlmSystemRuntimeInputForbidden,
    LlmTemplateInvalid,
    ErrorCodeInvalid,
    ErrorPublicMessageInvalid,
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
            Self::ControlInputUnused => "VNEXT_CONTROL_INPUT_UNUSED",
            Self::LlmInputNotDeclared => "VNEXT_LLM_INPUT_NOT_DECLARED",
            Self::LlmInputUnused => "VNEXT_LLM_INPUT_UNUSED",
            Self::PromptNotDeclared => "VNEXT_LLM_PROMPT_NOT_FOUND",
            Self::LlmSystemPrefixInvalid => "VNEXT_LLM_SYSTEM_PREFIX_INVALID",
            Self::LlmSystemRuntimeInputForbidden => "VNEXT_LLM_SYSTEM_RUNTIME_INPUT_FORBIDDEN",
            Self::LlmTemplateInvalid => "VNEXT_LLM_TEMPLATE_INVALID",
            Self::ErrorCodeInvalid => "VNEXT_ERROR_CODE_INVALID",
            Self::ErrorPublicMessageInvalid => "VNEXT_ERROR_PUBLIC_MESSAGE_INVALID",
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
            Self::ControlInputUnused => {
                "parallel or switch input is not consumed by its owned subtree"
            }
            Self::LlmInputNotDeclared => {
                "LLM message or template references an undeclared local input"
            }
            Self::LlmInputUnused => "LLM input is not consumed by messages or a template slot",
            Self::PromptNotDeclared => "LLM message references an undeclared prompt",
            Self::LlmSystemPrefixInvalid => {
                "authored system messages must form one contiguous prefix"
            }
            Self::LlmSystemRuntimeInputForbidden => {
                "authored system and assistant content cannot read runtime inputs"
            }
            Self::LlmTemplateInvalid => "LLM text does not satisfy the restricted template profile",
            Self::ErrorCodeInvalid => {
                "workflow error code does not satisfy the closed SafeBranchError profile"
            }
            Self::ErrorPublicMessageInvalid => {
                "workflow public error message is empty or exceeds its configured limit"
            }
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
    decoded_template_span: Option<SourceSpan>,
}

impl SemanticError {
    fn new(code: SemanticErrorCode, location: impl Into<String>) -> Self {
        Self {
            code,
            location: location.into(),
            decoded_template_span: None,
        }
    }

    fn with_decoded_template_span(mut self, span: Option<SourceSpan>) -> Self {
        self.decoded_template_span = span;
        self
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

    pub fn decoded_template_span(&self) -> Option<SourceSpan> {
        self.decoded_template_span
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
        prompts: workflow.prompts.clone(),
        declared_errors: workflow.errors.keys().cloned().collect(),
        errors: Vec::new(),
    };
    for (name, declaration) in &workflow.errors {
        if !super::raw::is_valid_error_code(&declaration.code) {
            validator.error(
                SemanticErrorCode::ErrorCodeInvalid,
                format!("errors.{}.code", name.as_str()),
            );
        }
        if !super::raw::is_valid_error_public_message(&declaration.public_message) {
            validator.error(
                SemanticErrorCode::ErrorPublicMessageInvalid,
                format!("errors.{}.public_message", name.as_str()),
            );
        }
    }
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
    prompts: BTreeMap<Identifier, PromptDeclaration>,
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
                Step::Llm {
                    inputs, messages, ..
                } => {
                    self.validate_bindings(
                        inputs.values(),
                        &completed,
                        scope,
                        visibility,
                        &location,
                    );
                    self.validate_llm(inputs, messages, &location);
                }
                Step::Action { inputs, .. } => {
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
                    self.validate_control_input_usage(
                        inputs,
                        collect_parallel_scope_reads(branches),
                        &location,
                    );
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
                    self.validate_control_input_usage(
                        inputs,
                        collect_switch_scope_reads(cases, default),
                        &location,
                    );
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

    fn validate_control_input_usage(
        &mut self,
        inputs: &BTreeMap<Identifier, ValueExpr>,
        consumed: BTreeSet<Identifier>,
        location: &str,
    ) {
        for input in inputs.keys().filter(|input| !consumed.contains(*input)) {
            self.error(
                SemanticErrorCode::ControlInputUnused,
                format!("{location}.inputs.{input}"),
            );
        }
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

    fn validate_llm(
        &mut self,
        inputs: &BTreeMap<Identifier, ValueExpr>,
        messages: &MessageListExpr,
        location: &str,
    ) {
        let declared = inputs.keys().cloned().collect::<BTreeSet<_>>();
        let mut consumed = BTreeSet::new();
        // File prompts are resolved to inline declarations by WorkflowCompiler
        // before semantic lowering. Keep direct RawWorkflow validation
        // conservative instead of falsely reporting inputs unused when their
        // file source has not been loaded yet.
        let mut usage_complete = true;

        match messages {
            MessageListExpr::Dynamic(reference) => self.validate_local_input_ref(
                reference,
                &declared,
                &mut consumed,
                &format!("{location}.messages"),
            ),
            MessageListExpr::Sources(sources) => {
                let mut system_prefix_open = true;
                for (index, source) in sources.iter().enumerate() {
                    let source_location = format!("{location}.messages.{index}");
                    match source {
                        MessageSource::Dynamic(reference) => {
                            system_prefix_open = false;
                            self.validate_local_input_ref(
                                reference,
                                &declared,
                                &mut consumed,
                                &source_location,
                            );
                        }
                        MessageSource::Authored(message) => {
                            if message.role == AuthoredRole::System {
                                if !system_prefix_open {
                                    self.error(
                                        SemanticErrorCode::LlmSystemPrefixInvalid,
                                        &source_location,
                                    );
                                }
                            } else {
                                system_prefix_open = false;
                            }
                            self.validate_authored_message(
                                message,
                                &declared,
                                &mut consumed,
                                &mut usage_complete,
                                &source_location,
                            );
                        }
                    }
                }
            }
        }

        if usage_complete {
            for input in declared.difference(&consumed) {
                self.error(
                    SemanticErrorCode::LlmInputUnused,
                    format!("{location}.inputs.{input}"),
                );
            }
        }
    }

    fn validate_authored_message(
        &mut self,
        message: &AuthoredMessageTemplate,
        declared: &BTreeSet<Identifier>,
        consumed: &mut BTreeSet<Identifier>,
        usage_complete: &mut bool,
        location: &str,
    ) {
        let content_location = format!("{location}.content");
        for (index, atom) in message.content.atoms().iter().enumerate() {
            let atom_location = match &message.content {
                super::message::AuthoredContentExpr::Single(_) => content_location.clone(),
                super::message::AuthoredContentExpr::Parts(_) => {
                    format!("{content_location}.{index}")
                }
            };
            match atom {
                AuthoredContentAtom::Prompt(prompt) => {
                    let declaration = self.prompts.get(prompt).cloned();
                    match declaration {
                        None => {
                            self.error(SemanticErrorCode::PromptNotDeclared, &atom_location);
                            *usage_complete = false;
                        }
                        Some(PromptDeclaration::Inline(source)) => {
                            if !self.validate_llm_template(
                                &source,
                                message.role,
                                declared,
                                consumed,
                                &atom_location,
                            ) {
                                *usage_complete = false;
                            }
                        }
                        Some(PromptDeclaration::File(_)) => {
                            *usage_complete = false;
                        }
                    }
                }
                AuthoredContentAtom::InlineText(source) => {
                    if !self.validate_llm_template(
                        source,
                        message.role,
                        declared,
                        consumed,
                        &atom_location,
                    ) {
                        *usage_complete = false;
                    }
                }
                AuthoredContentAtom::RuntimeText(reference) => {
                    if message.role != AuthoredRole::User {
                        self.error(
                            SemanticErrorCode::LlmSystemRuntimeInputForbidden,
                            &atom_location,
                        );
                    }
                    self.validate_local_input_ref(reference, declared, consumed, &atom_location);
                }
                AuthoredContentAtom::Image(reference) => {
                    if message.role != AuthoredRole::User {
                        self.error(
                            SemanticErrorCode::LlmSystemRuntimeInputForbidden,
                            &atom_location,
                        );
                    }
                    self.validate_local_input_ref(reference, declared, consumed, &atom_location);
                }
            }
        }
    }

    fn validate_llm_template(
        &mut self,
        source: &str,
        role: AuthoredRole,
        declared: &BTreeSet<Identifier>,
        consumed: &mut BTreeSet<Identifier>,
        location: &str,
    ) -> bool {
        let template = match compile_template(source) {
            Ok(template) => template,
            Err(error) => {
                self.errors.push(
                    SemanticError::new(SemanticErrorCode::LlmTemplateInvalid, location)
                        .with_decoded_template_span(error.decoded_span()),
                );
                return false;
            }
        };
        if role != AuthoredRole::User && !template.slots().is_empty() {
            self.error(SemanticErrorCode::LlmSystemRuntimeInputForbidden, location);
        }
        for slot in template.slots() {
            self.validate_local_binding(slot, declared, consumed, location);
        }
        true
    }

    fn validate_local_input_ref(
        &mut self,
        reference: &LocalInputRef,
        declared: &BTreeSet<Identifier>,
        consumed: &mut BTreeSet<Identifier>,
        location: &str,
    ) {
        self.validate_local_binding(reference.from.binding(), declared, consumed, location);
    }

    fn validate_local_binding(
        &mut self,
        binding: &Identifier,
        declared: &BTreeSet<Identifier>,
        consumed: &mut BTreeSet<Identifier>,
        location: &str,
    ) {
        if declared.contains(binding) {
            consumed.insert(binding.clone());
        } else {
            self.error(SemanticErrorCode::LlmInputNotDeclared, location);
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

fn collect_parallel_scope_reads(
    branches: &BTreeMap<Identifier, ParallelBranch>,
) -> BTreeSet<Identifier> {
    let mut consumed = BTreeSet::new();
    for branch in branches.values() {
        collect_region_scope_reads(&branch.steps, &branch.result, &mut consumed);
    }
    consumed
}

fn collect_switch_scope_reads(
    cases: &[SwitchCase],
    default: &SwitchDefault,
) -> BTreeSet<Identifier> {
    let mut consumed = BTreeSet::new();
    for case in cases {
        let Predicate::Cel(source) = &case.when;
        if let Ok(bindings) = referenced_scope_bindings(source) {
            consumed.extend(bindings);
        }
        collect_region_scope_reads(&case.steps, &case.result, &mut consumed);
    }
    collect_region_scope_reads(&default.steps, &default.result, &mut consumed);
    consumed
}

/// Collects reads of the region's own `scope` while respecting ownership.
/// Inputs of a nested control step are evaluated in this region and therefore
/// count; that nested step's branches/arms have a new scope and are not walked.
fn collect_region_scope_reads(
    steps: &[Step],
    result: &BlockResult,
    consumed: &mut BTreeSet<Identifier>,
) {
    for step in steps {
        for value in step_inputs(step).values() {
            collect_value_scope_reads(value, consumed);
        }
    }
    if let BlockResult::Return(value) = result {
        collect_value_scope_reads(value, consumed);
    }
}

fn step_inputs(step: &Step) -> &BTreeMap<Identifier, ValueExpr> {
    match step {
        Step::Llm { inputs, .. }
        | Step::Action { inputs, .. }
        | Step::Parallel { inputs, .. }
        | Step::Switch { inputs, .. } => inputs,
    }
}

fn collect_value_scope_reads(expression: &ValueExpr, consumed: &mut BTreeSet<Identifier>) {
    match expression {
        ValueExpr::Literal(_) => {}
        ValueExpr::From(path) => {
            if path.root() == &ValuePathRoot::Scope {
                if let Some(binding) = path
                    .fields()
                    .first()
                    .and_then(|field| Identifier::parse(field).ok())
                {
                    consumed.insert(binding);
                }
            }
        }
        ValueExpr::Object(fields) => {
            for value in fields.values() {
                collect_value_scope_reads(value, consumed);
            }
        }
        ValueExpr::Array(values) => {
            for value in values {
                collect_value_scope_reads(value, consumed);
            }
        }
        ValueExpr::Template(template) => {
            for value in template.bindings.values() {
                collect_value_scope_reads(value, consumed);
            }
        }
    }
}

fn step_id(step: &Step) -> &Identifier {
    match step {
        Step::Llm { id, .. }
        | Step::Action { id, .. }
        | Step::Parallel { id, .. }
        | Step::Switch { id, .. } => id,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{validate_workflow_semantics, MAX_STATIC_PARALLEL_CONCURRENCY};
    use crate::dsl::vnext::{
        message::{MessageListExpr, ResponseConfig},
        raw::{
            ApiVersion, BlockResult, DocumentKind, ErrorCategory, ErrorDeclaration, InputContract,
            Metadata, OutputContract, OutputFormat, ParallelBranch, ParallelSettle, Predicate,
            PromptDeclaration, RawWorkflow, RootResult, RootReturn, Step, SwitchCase,
            SwitchDefault, WorkflowBody,
        },
        value::{Identifier, ValueExpr, ValuePath},
    };

    fn id(value: &str) -> Identifier {
        Identifier::parse(value).unwrap()
    }

    fn source(value: &str) -> ValueExpr {
        ValueExpr::From(ValuePath::parse(value).unwrap())
    }

    fn action(name: &str, inputs: BTreeMap<Identifier, ValueExpr>) -> Step {
        Step::Action {
            id: id(name),
            call: "test.action".to_string(),
            inputs,
        }
    }

    fn llm(name: &str, inputs: BTreeMap<Identifier, ValueExpr>, messages: &str) -> Step {
        Step::Llm {
            id: id(name),
            model: "general_chat".to_string(),
            inputs,
            messages: yaml_serde::from_str::<MessageListExpr>(messages).unwrap(),
            parameters: serde_json::Map::new(),
            response: ResponseConfig::Text,
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
        let prepare = action(
            "prepare",
            BTreeMap::from([
                (id("question"), source("input.question")),
                (id("run_id"), source("run.id")),
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
                        vec![action(
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
    fn control_inputs_are_consumed_by_predicates_and_nested_owned_subtrees() {
        let nested = Step::Parallel {
            id: id("nested"),
            inputs: BTreeMap::from([(id("inner"), source("scope.outer"))]),
            settle: ParallelSettle::All,
            max_concurrency: None,
            branches: BTreeMap::from([
                (
                    id("a"),
                    branch(Vec::new(), returning(source("scope.inner"))),
                ),
                (
                    id("b"),
                    branch(Vec::new(), returning(source("scope.inner"))),
                ),
            ]),
        };
        let parallel = Step::Parallel {
            id: id("fanout"),
            inputs: BTreeMap::from([(id("outer"), source("input.outer"))]),
            settle: ParallelSettle::All,
            max_concurrency: None,
            branches: BTreeMap::from([
                (
                    id("left"),
                    branch(vec![nested], returning(source("steps.nested.output"))),
                ),
                (
                    id("right"),
                    branch(Vec::new(), returning(ValueExpr::Literal(json!(null)))),
                ),
            ]),
        };
        let route = Step::Switch {
            id: id("route"),
            inputs: BTreeMap::from([(id("selected"), source("steps.fanout.output"))]),
            output_schema: json!({}),
            cases: vec![SwitchCase {
                id: id("matched"),
                when: Predicate::Cel("size(scope.selected) >= 0".to_string()),
                steps: Vec::new(),
                result: returning(ValueExpr::Literal(json!("matched"))),
            }],
            default: SwitchDefault {
                id: id("fallback"),
                steps: Vec::new(),
                result: returning(ValueExpr::Literal(json!("fallback"))),
            },
        };
        let workflow = workflow(
            vec![parallel, route],
            root_return(source("steps.route.output")),
        );

        assert_eq!(validate_workflow_semantics(&workflow), Ok(()));
    }

    #[test]
    fn rejects_a_control_input_never_read_by_its_owned_subtree() {
        let parallel = Step::Parallel {
            id: id("fanout"),
            inputs: BTreeMap::from([(id("unused"), source("input.value"))]),
            settle: ParallelSettle::All,
            max_concurrency: None,
            branches: BTreeMap::from([
                (
                    id("left"),
                    branch(Vec::new(), returning(ValueExpr::Literal(json!(1)))),
                ),
                (
                    id("right"),
                    branch(Vec::new(), returning(ValueExpr::Literal(json!(2)))),
                ),
            ]),
        };
        let workflow = workflow(vec![parallel], root_return(source("steps.fanout.output")));

        assert_eq!(error_codes(&workflow), vec!["VNEXT_CONTROL_INPUT_UNUSED"]);
    }

    #[test]
    fn rejects_forward_and_unknown_root_result_references() {
        let forward = action(
            "first",
            BTreeMap::from([(id("value"), source("steps.later.output"))]),
        );
        let workflow = workflow(
            vec![forward, action("later", BTreeMap::new())],
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
            vec![action("prepare", BTreeMap::new()), parallel],
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
                        vec![action("only_left", BTreeMap::new())],
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
                        vec![action(
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
        let unknown_prompt = llm(
            "prepare",
            BTreeMap::new(),
            "- {role: user, content: unknown_prompt}",
        );
        let left_branch = branch(
            vec![action(
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
            vec![unknown_prompt, parallel],
            root_return(source("steps.fanout.output")),
        );

        assert_eq!(
            error_codes(&workflow),
            vec![
                "VNEXT_LLM_PROMPT_NOT_FOUND",
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
                action("duplicate", BTreeMap::new()),
                action("duplicate", BTreeMap::new()),
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
                        vec![action("work", BTreeMap::new())],
                        returning(source("steps.work.output")),
                    ),
                ),
                (
                    id("right"),
                    branch(
                        vec![action("work", BTreeMap::new())],
                        returning(source("steps.work.output")),
                    ),
                ),
            ]),
        };
        let workflow = workflow(vec![parallel], root_return(source("steps.fanout.output")));

        assert_eq!(validate_workflow_semantics(&workflow), Ok(()));
    }

    #[test]
    fn accepts_llm_inputs_consumed_by_dynamic_messages_templates_and_images() {
        let step = llm(
            "answer",
            BTreeMap::from([
                (id("history"), source("input.history")),
                (id("question"), source("input.question")),
                (id("image_url"), source("input.image_url")),
            ]),
            r#"
- {role: system, content: system}
- {from: inputs.history}
- role: user
  content:
    - answer_prompt
    - {image: {from: inputs.image_url}}
"#,
        );
        let mut workflow = workflow(vec![step], root_return(source("steps.answer.output.data")));
        workflow.prompts.insert(
            id("answer_prompt"),
            PromptDeclaration::Inline("Question: {{ question }}".to_string()),
        );

        assert_eq!(validate_workflow_semantics(&workflow), Ok(()));
    }

    #[test]
    fn rejects_undeclared_local_references_slots_and_unused_inputs() {
        let step = llm(
            "answer",
            BTreeMap::from([(id("unused"), source("input.unused"))]),
            r#"
- {role: user, content: {from: inputs.missing}}
- {role: user, content: slot_prompt}
"#,
        );
        let mut workflow = workflow(vec![step], root_return(source("steps.answer.output.data")));
        workflow.prompts.insert(
            id("slot_prompt"),
            PromptDeclaration::Inline("{{ absent }}".to_string()),
        );

        assert_eq!(
            error_codes(&workflow),
            vec![
                "VNEXT_LLM_INPUT_NOT_DECLARED",
                "VNEXT_LLM_INPUT_NOT_DECLARED",
                "VNEXT_LLM_INPUT_UNUSED"
            ]
        );
    }

    #[test]
    fn rejects_non_prefix_system_messages_and_runtime_content_outside_user_role() {
        let step = llm(
            "answer",
            BTreeMap::from([
                (id("assistant_text"), source("input.assistant_text")),
                (id("assistant_image"), source("input.assistant_image")),
                (id("system_slot"), source("input.system_slot")),
            ]),
            r#"
- {role: user, content: {text: begin}}
- {role: system, content: slotted_system}
- {role: assistant, content: {from: inputs.assistant_text}}
- role: assistant
  content:
    - {image: {from: inputs.assistant_image}}
"#,
        );
        let mut workflow = workflow(vec![step], root_return(source("steps.answer.output.data")));
        workflow.prompts.insert(
            id("slotted_system"),
            PromptDeclaration::Inline("{{ system_slot }}".to_string()),
        );

        assert_eq!(
            error_codes(&workflow),
            vec![
                "VNEXT_LLM_SYSTEM_PREFIX_INVALID",
                "VNEXT_LLM_SYSTEM_RUNTIME_INPUT_FORBIDDEN",
                "VNEXT_LLM_SYSTEM_RUNTIME_INPUT_FORBIDDEN",
                "VNEXT_LLM_SYSTEM_RUNTIME_INPUT_FORBIDDEN"
            ]
        );
    }

    #[test]
    fn rejects_invalid_restricted_inline_templates_without_echoing_source() {
        let step = llm(
            "answer",
            BTreeMap::new(),
            "- {role: user, content: bad_template}",
        );
        let mut workflow = workflow(vec![step], root_return(source("steps.answer.output.data")));
        workflow.prompts.insert(
            id("bad_template"),
            PromptDeclaration::Inline("{{#if secret}}do-not-render{{/if}}".to_string()),
        );

        let errors = validate_workflow_semantics(&workflow).unwrap_err();
        assert_eq!(errors[0].code(), "VNEXT_LLM_TEMPLATE_INVALID");
        assert_eq!(
            errors[0].location(),
            "workflow.steps.answer.messages.0.content"
        );
        let decoded = errors[0].decoded_template_span().unwrap();
        assert_eq!((decoded.line_start(), decoded.column_start()), (1, 1));
        assert!(!errors[0].to_string().contains("do-not-render"));
    }

    #[test]
    fn template_error_locations_preserve_parts_indexes_and_decoded_coordinates() {
        let step = llm(
            "answer",
            BTreeMap::new(),
            r#"
- role: user
  content:
    - {text: safe}
    - {text: "第一行\n第二行 {{#if secret}}do-not-render{{/if}}"}
"#,
        );
        let workflow = workflow(vec![step], root_return(source("steps.answer.output.data")));

        let errors = validate_workflow_semantics(&workflow).unwrap_err();
        assert_eq!(errors[0].code(), "VNEXT_LLM_TEMPLATE_INVALID");
        assert_eq!(
            errors[0].location(),
            "workflow.steps.answer.messages.0.content.1"
        );
        let decoded = errors[0].decoded_template_span().unwrap();
        assert_eq!((decoded.line_start(), decoded.column_start()), (2, 5));
        assert!(!errors[0].to_string().contains("secret"));
        assert!(!errors[0].to_string().contains("do-not-render"));
    }

    #[test]
    fn rejects_programmatically_forged_error_declarations_with_stable_locations() {
        let mut invalid_code = workflow(Vec::new(), RootResult::Raise(id("declared")));
        invalid_code.errors.get_mut(&id("declared")).unwrap().code = "lowercase-secret".to_string();
        let errors = validate_workflow_semantics(&invalid_code).unwrap_err();
        assert_eq!(errors[0].code(), "VNEXT_ERROR_CODE_INVALID");
        assert_eq!(errors[0].location(), "errors.declared.code");
        assert!(!errors[0].to_string().contains("lowercase-secret"));

        let mut invalid_message = workflow(Vec::new(), RootResult::Raise(id("declared")));
        invalid_message
            .errors
            .get_mut(&id("declared"))
            .unwrap()
            .public_message = "x".repeat(super::super::raw::ERROR_PUBLIC_MESSAGE_MAX_CHARS + 1);
        let errors = validate_workflow_semantics(&invalid_message).unwrap_err();
        assert_eq!(errors[0].code(), "VNEXT_ERROR_PUBLIC_MESSAGE_INVALID");
        assert_eq!(errors[0].location(), "errors.declared.public_message");
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
