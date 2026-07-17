//! Typed Region/SSA target for the vNext workflow compiler.
//!
//! The IR preserves every runtime-relevant authored contract while replacing
//! source expressions and structured blocks with explicit, typed values. It is
//! deliberately independent from the runtime scheduler: `validate` is a pure
//! verifier that rejects malformed plans before execution.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    dsl::{DslPath, DslPathSegment},
    resources::models::ModelCapability,
    schema::compile_schema_2020,
};

use super::{
    plan::{
        CallPlan, CallTarget, CompiledContentAtom, CompiledLlmPlan, CompiledTemplateId,
        MessageSourcePlan, PlannedRole, PlannedTemplate, TemplateProfileVersion,
        TemplateProvenance, ValidatedResponseContract,
    },
    predicate::analyze_predicate,
    raw::{
        is_valid_error_code, is_valid_error_public_message, ErrorDeclaration, Metadata,
        ERROR_CODE_MAX_CHARS,
    },
    schema::compile_contract_schema,
    shape::{prove_dynamic_message_array, SchemaShape},
    template::{compile_template, CompiledTemplate, TemplateAccessKind, TemplatePathSegment},
    types::{
        safe_run_metadata_type, ArrayType, ObjectType, PropertyType, SchemaType, StaticPath,
        ValueType,
    },
    value::Identifier,
};

pub use super::raw::{ErrorCategory, OutputFormat, ParallelSettle};

pub const SAFE_BRANCH_ORIGIN_MAX_BYTES: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StablePath(String);

impl StablePath {
    pub fn parse(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        if value.len() < 2
            || value.len() > SAFE_BRANCH_ORIGIN_MAX_BYTES
            || !value.starts_with('/')
            || value.ends_with('/')
        {
            return Err("stable path must be a non-root slash-qualified path".to_string());
        }
        for segment in value[1..].split('/') {
            Identifier::parse(segment).map_err(|_| {
                format!("stable path '{value}' contains invalid segment '{segment}'")
            })?;
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn is_descendant_of(&self, parent: &StablePath) -> bool {
        self.0
            .strip_prefix(parent.as_str())
            .is_some_and(|suffix| suffix.starts_with('/') && suffix.len() > 1)
    }
}

impl fmt::Display for StablePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RegionId(StablePath);

impl RegionId {
    pub fn new(path: impl Into<String>) -> Result<Self, String> {
        StablePath::parse(path).map(Self)
    }

    pub fn path(&self) -> &StablePath {
        &self.0
    }
}

impl fmt::Display for RegionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OperationRole {
    Authored,
    Expression(u16),
    Phi,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OperationId {
    path: StablePath,
    role: OperationRole,
}

impl OperationId {
    pub fn authored(path: impl Into<String>) -> Result<Self, String> {
        Self::new(path, OperationRole::Authored)
    }

    pub fn expression(path: impl Into<String>, ordinal: u16) -> Result<Self, String> {
        Self::new(path, OperationRole::Expression(ordinal))
    }

    pub fn phi(path: impl Into<String>) -> Result<Self, String> {
        Self::new(path, OperationRole::Phi)
    }

    pub fn new(path: impl Into<String>, role: OperationRole) -> Result<Self, String> {
        let operation = Self {
            path: StablePath::parse(path)?,
            role,
        };
        if operation.to_string().len() > SAFE_BRANCH_ORIGIN_MAX_BYTES {
            return Err("stable operation origin exceeds its byte limit".to_string());
        }
        Ok(operation)
    }

    pub fn path(&self) -> &StablePath {
        &self.path
    }

    pub fn role(&self) -> OperationRole {
        self.role
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}#{:?}", self.path, self.role)
    }
}

pub(crate) fn is_safe_branch_origin(value: &str) -> bool {
    if value.is_empty()
        || value.len() > SAFE_BRANCH_ORIGIN_MAX_BYTES
        || value.chars().any(char::is_control)
    {
        return false;
    }
    let (path, role) = value
        .rsplit_once('#')
        .map_or((value, None), |(path, role)| (path, Some(role)));
    if StablePath::parse(path).is_err() {
        return false;
    }
    match role {
        None | Some("Authored" | "Phi") => true,
        Some(role) => role
            .strip_prefix("Expression(")
            .and_then(|value| value.strip_suffix(')'))
            .and_then(|value| value.parse::<u16>().ok())
            .is_some(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ValueRole {
    Parameter(u16),
    AuthoredOutput,
    ExpressionOutput(u16),
    BranchControl,
    PhiOutput,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueId {
    path: StablePath,
    role: ValueRole,
}

impl ValueId {
    pub fn parameter(path: impl Into<String>, ordinal: u16) -> Result<Self, String> {
        Self::new(path, ValueRole::Parameter(ordinal))
    }

    pub fn output(path: impl Into<String>) -> Result<Self, String> {
        Self::new(path, ValueRole::AuthoredOutput)
    }

    pub fn expression(path: impl Into<String>, ordinal: u16) -> Result<Self, String> {
        Self::new(path, ValueRole::ExpressionOutput(ordinal))
    }

    pub fn control(path: impl Into<String>) -> Result<Self, String> {
        Self::new(path, ValueRole::BranchControl)
    }

    pub fn phi(path: impl Into<String>) -> Result<Self, String> {
        Self::new(path, ValueRole::PhiOutput)
    }

    pub fn new(path: impl Into<String>, role: ValueRole) -> Result<Self, String> {
        Ok(Self {
            path: StablePath::parse(path)?,
            role,
        })
    }

    pub fn path(&self) -> &StablePath {
        &self.path
    }

    pub fn role(&self) -> ValueRole {
        self.role
    }
}

impl fmt::Display for ValueId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}#{:?}", self.path, self.role)
    }
}

/// Control values never cross an authored data boundary. The result type is
/// carried only so a matching Phi can be verified without weakening the JSON
/// value type system.
#[derive(Debug, Clone, PartialEq)]
pub enum IrValueType {
    Data(ValueType),
    Control { result_type: ValueType },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValueDefinition {
    pub id: ValueId,
    pub value_type: IrValueType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypedContract {
    /// Self-contained Draft 2020-12 validator document retained for runtime
    /// validation and normalized plan hashing.
    pub schema: Value,
    /// Conservative compiler type derived from `schema` after ref resolution.
    pub value_type: ValueType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledPrompt {
    /// Catalog identity and content hash are retained across persistence.
    pub provenance: TemplateProvenance,
    /// Restricted template AST compiled once before IR construction.
    pub compiled: CompiledTemplate,
    pub profile_version: TemplateProfileVersion,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowIr {
    pub metadata: Metadata,
    pub input: TypedContract,
    pub output: TypedContract,
    pub prompts: BTreeMap<Identifier, CompiledPrompt>,
    pub errors: BTreeMap<Identifier, ErrorDeclaration>,
    pub root: Region,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParameterSource {
    WorkflowInput,
    RunMetadata,
    Capture { source: ValueId },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RegionParameter {
    pub name: Identifier,
    pub value: ValueDefinition,
    pub source: ParameterSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionKind {
    Workflow,
    ParallelBranch { name: Identifier },
    SwitchArm { name: Identifier, is_default: bool },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Region {
    pub id: RegionId,
    pub kind: RegionKind,
    pub parameters: Vec<RegionParameter>,
    pub operations: Vec<Operation>,
    pub result: TypedContract,
    /// Optional only so partially lowered IR can be rejected by the verifier.
    pub terminator: Option<Terminator>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Operation {
    pub id: OperationId,
    pub output: ValueDefinition,
    pub kind: OperationKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OperationKind {
    Const {
        value: Value,
    },
    Project {
        source: ValueId,
        path: StaticPath,
    },
    Object {
        fields: BTreeMap<String, ValueId>,
    },
    Array {
        items: Vec<ValueId>,
    },
    Template {
        text: String,
        bindings: BTreeMap<Identifier, ValueId>,
    },
    Call(Box<Call>),
    Parallel(Parallel),
    Branch(Box<Branch>),
    Phi(Phi),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Call {
    pub target: CallTarget,
    pub inputs: BTreeMap<Identifier, ValueId>,
    pub plan: CallPlan,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Parallel {
    pub inputs: BTreeMap<Identifier, ValueId>,
    pub settle: ParallelSettle,
    pub max_concurrency: Option<usize>,
    pub branches: BTreeMap<Identifier, Region>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CelProgram {
    /// Stage 4 may replace this source-preserving form with compiled bytecode,
    /// but ordered case identity and source remain part of the normalized plan.
    pub source: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BranchCase {
    pub id: Identifier,
    pub predicate: CelProgram,
    pub region: Region,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BranchDefault {
    pub id: Identifier,
    pub region: Region,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Branch {
    pub inputs: BTreeMap<Identifier, ValueId>,
    /// Authored order is semantically significant: the first true case wins.
    pub cases: Vec<BranchCase>,
    pub default: BranchDefault,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Phi {
    pub branch: OperationId,
    pub token: ValueId,
    /// Case regions in authored order followed by the default region.
    pub incomings: Vec<RegionId>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RootReturn {
    pub content: Option<ValueId>,
    pub format: Option<OutputFormat>,
    pub data: ValueId,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Terminator {
    RegionYield { value: ValueId },
    WorkflowReturn(RootReturn),
    Raise { error: Identifier },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ValidationCode {
    RootMustBeWorkflow,
    DuplicateRegionId,
    DuplicateOperationId,
    DuplicateValueId,
    DuplicateParameterName,
    MissingTerminator,
    WrongTerminator,
    UnknownValue,
    UseBeforeDefinition,
    ValueEscapesRegion,
    TypeMismatch,
    InternalControlUse,
    InvalidStableIdentity,
    InvalidOperationRole,
    InvalidValueRole,
    InvalidRegionKind,
    InvalidRootParameter,
    InvalidCapture,
    InvalidParallel,
    InvalidBranch,
    InvalidPredicate,
    InvalidPhi,
    InvalidContract,
    InvalidTemplate,
    InvalidCallPlan,
    InvalidErrorDeclaration,
    UndeclaredError,
    InvalidRootReturn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub code: ValidationCode,
    pub message: String,
    pub region: Option<RegionId>,
    pub operation: Option<OperationId>,
    pub value: Option<ValueId>,
}

fn expanded_contract_schema(document: &Value) -> Result<Value, ()> {
    let mut definitions = BTreeMap::new();
    let authored = match document {
        Value::Object(object) => {
            let mut authored = object.clone();
            let injected_definitions = authored.remove("$defs");
            if let Some(definitions_value) = &injected_definitions {
                let definitions_object = definitions_value.as_object().ok_or(())?;
                for (name, schema) in definitions_object {
                    definitions.insert(Identifier::parse(name).map_err(|_| ())?, schema.clone());
                }
            }
            if injected_definitions.is_some()
                && authored.len() == 1
                && authored
                    .get("allOf")
                    .and_then(Value::as_array)
                    .is_some_and(|items| items.len() == 1 && items[0].is_boolean())
            {
                authored
                    .remove("allOf")
                    .and_then(|value| value.as_array().cloned())
                    .and_then(|mut values| values.pop())
                    .ok_or(())?
            } else {
                Value::Object(authored)
            }
        }
        Value::Bool(_) => document.clone(),
        _ => return Err(()),
    };
    compile_contract_schema(&definitions, &authored)
        .map(|bundle| bundle.expanded_schema().clone())
        .map_err(|_| ())
}

pub fn validate(ir: &WorkflowIr) -> Result<(), Vec<ValidationError>> {
    let mut validator = Validator::new(ir);
    validator.collect_region(&ir.root);
    if !matches!(ir.root.kind, RegionKind::Workflow) {
        validator.error(
            ValidationCode::RootMustBeWorkflow,
            "root region must have Workflow kind",
            Some(ir.root.id.clone()),
            None,
            None,
        );
    }
    validator.validate_contract(&ir.input, "workflow input", Some(ir.root.id.clone()));
    validator.validate_contract(&ir.output, "workflow output", Some(ir.root.id.clone()));
    validator.validate_error_declarations();
    validator.validate_prompts();
    if ir.root.result.schema != ir.output.schema
        || !types_equivalent(&ir.root.result.value_type, &ir.output.value_type)
    {
        validator.error(
            ValidationCode::TypeMismatch,
            "root result contract must equal the public output data contract",
            Some(ir.root.id.clone()),
            None,
            None,
        );
    }
    validator.validate_region(&ir.root, true);

    if validator.validation_errors.is_empty() {
        Ok(())
    } else {
        Err(validator.validation_errors)
    }
}

#[derive(Debug, Clone)]
struct KnownValue {
    owner: RegionId,
}

#[derive(Debug, Clone)]
struct BranchInfo {
    token: ValueId,
    result_type: ValueType,
    incomings: Vec<RegionId>,
    phi_count: usize,
}

struct Validator<'a> {
    ir: &'a WorkflowIr,
    validation_errors: Vec<ValidationError>,
    regions: BTreeSet<RegionId>,
    operations: BTreeSet<OperationId>,
    values: BTreeMap<ValueId, KnownValue>,
}

impl<'a> Validator<'a> {
    fn new(ir: &'a WorkflowIr) -> Self {
        Self {
            ir,
            validation_errors: Vec::new(),
            regions: BTreeSet::new(),
            operations: BTreeSet::new(),
            values: BTreeMap::new(),
        }
    }

    fn validate_contract(
        &mut self,
        contract: &TypedContract,
        label: &str,
        region: Option<RegionId>,
    ) {
        if compile_schema_2020(&contract.schema).is_err() {
            self.error(
                ValidationCode::InvalidContract,
                format!("{label} contract is not a valid Draft 2020-12 schema"),
                region,
                None,
                None,
            );
            return;
        }
        let static_schema = match expanded_contract_schema(&contract.schema) {
            Ok(schema) => schema,
            Err(()) => {
                self.error(
                    ValidationCode::InvalidContract,
                    format!("{label} contract could not be safely resolved"),
                    region,
                    None,
                    None,
                );
                return;
            }
        };
        match SchemaType::compile(&static_schema) {
            Ok(schema_type) if types_equivalent(schema_type.value_type(), &contract.value_type) => {
            }
            Ok(_) => self.error(
                ValidationCode::TypeMismatch,
                format!("{label} schema and static value type disagree"),
                region,
                None,
                None,
            ),
            Err(_) => self.error(
                ValidationCode::InvalidContract,
                format!("{label} schema is outside the supported static schema subset"),
                region,
                None,
                None,
            ),
        }
    }

    fn validate_prompts(&mut self) {
        for (prompt_id, prompt) in &self.ir.prompts {
            let valid_provenance = matches!(
                &prompt.provenance,
                TemplateProvenance::Catalog {
                    prompt_id: provenance_id,
                    asset_hash,
                } if provenance_id == prompt_id
                    && asset_hash == &sha256_label(prompt.compiled.source().as_bytes())
            );
            if !valid_provenance {
                self.error(
                    ValidationCode::InvalidTemplate,
                    format!(
                        "compiled prompt '{prompt_id}' must retain matching catalog provenance"
                    ),
                    Some(self.ir.root.id.clone()),
                    None,
                    None,
                );
            }
            if prompt.compiled.source().trim().is_empty()
                || !matches!(prompt.profile_version, TemplateProfileVersion::V1)
            {
                self.error(
                    ValidationCode::InvalidTemplate,
                    format!("compiled prompt '{prompt_id}' has invalid template metadata"),
                    Some(self.ir.root.id.clone()),
                    None,
                    None,
                );
            }
        }
    }

    fn validate_error_declarations(&mut self) {
        for declaration in self.ir.errors.values() {
            if !is_valid_error_code(&declaration.code)
                || !is_valid_error_public_message(&declaration.public_message)
            {
                self.error(
                    ValidationCode::InvalidErrorDeclaration,
                    "workflow error declaration does not satisfy its bounded closed profile",
                    Some(self.ir.root.id.clone()),
                    None,
                    None,
                );
            }
        }
    }

    fn collect_region(&mut self, region: &Region) {
        if !self.regions.insert(region.id.clone()) {
            self.error(
                ValidationCode::DuplicateRegionId,
                format!("duplicate region id '{}'", region.id),
                Some(region.id.clone()),
                None,
                None,
            );
        }
        for parameter in &region.parameters {
            self.collect_value(region, None, &parameter.value);
        }
        for operation in &region.operations {
            if !self.operations.insert(operation.id.clone()) {
                self.error(
                    ValidationCode::DuplicateOperationId,
                    format!("duplicate operation id '{}'", operation.id),
                    Some(region.id.clone()),
                    Some(operation.id.clone()),
                    None,
                );
            }
            self.collect_value(region, Some(operation), &operation.output);
            match &operation.kind {
                OperationKind::Parallel(parallel) => {
                    for child in parallel.branches.values() {
                        self.collect_region(child);
                    }
                }
                OperationKind::Branch(branch) => {
                    for case in &branch.cases {
                        self.collect_region(&case.region);
                    }
                    self.collect_region(&branch.default.region);
                }
                OperationKind::Const { .. }
                | OperationKind::Project { .. }
                | OperationKind::Object { .. }
                | OperationKind::Array { .. }
                | OperationKind::Template { .. }
                | OperationKind::Call(_)
                | OperationKind::Phi(_) => {}
            }
        }
    }

    fn collect_value(
        &mut self,
        region: &Region,
        operation: Option<&Operation>,
        value: &ValueDefinition,
    ) {
        if self
            .values
            .insert(
                value.id.clone(),
                KnownValue {
                    owner: region.id.clone(),
                },
            )
            .is_some()
        {
            self.error(
                ValidationCode::DuplicateValueId,
                format!("duplicate value id '{}'", value.id),
                Some(region.id.clone()),
                operation.map(|value| value.id.clone()),
                Some(value.id.clone()),
            );
        }
    }

    fn validate_region(&mut self, region: &Region, is_root: bool) {
        self.validate_contract(&region.result, "region result", Some(region.id.clone()));
        let mut visible = BTreeMap::new();
        let mut parameter_names = BTreeSet::new();
        let mut workflow_inputs = 0;
        let mut run_inputs = 0;

        for parameter in &region.parameters {
            if !parameter_names.insert(parameter.name.clone()) {
                self.error(
                    ValidationCode::DuplicateParameterName,
                    format!("duplicate region parameter '{}'", parameter.name),
                    Some(region.id.clone()),
                    None,
                    Some(parameter.value.id.clone()),
                );
            }
            if parameter.value.id.path() != region.id.path()
                || !matches!(parameter.value.id.role(), ValueRole::Parameter(_))
            {
                self.error(
                    ValidationCode::InvalidStableIdentity,
                    "region parameter value must use its region path and Parameter role",
                    Some(region.id.clone()),
                    None,
                    Some(parameter.value.id.clone()),
                );
            }
            if !matches!(parameter.value.value_type, IrValueType::Data(_)) {
                self.error(
                    ValidationCode::InternalControlUse,
                    "region parameters must be data values",
                    Some(region.id.clone()),
                    None,
                    Some(parameter.value.id.clone()),
                );
            }

            match (&parameter.source, is_root) {
                (ParameterSource::WorkflowInput, true) => {
                    workflow_inputs += 1;
                    if let IrValueType::Data(value_type) = &parameter.value.value_type {
                        if !types_equivalent(value_type, &self.ir.input.value_type) {
                            self.error(
                                ValidationCode::TypeMismatch,
                                "workflow input parameter type does not match input contract",
                                Some(region.id.clone()),
                                None,
                                Some(parameter.value.id.clone()),
                            );
                        }
                    }
                }
                (ParameterSource::RunMetadata, true) => {
                    run_inputs += 1;
                    if let IrValueType::Data(value_type) = &parameter.value.value_type {
                        if !types_equivalent(value_type, &safe_run_metadata_type()) {
                            self.error(
                                ValidationCode::TypeMismatch,
                                "safe run parameter type does not match runtime metadata contract",
                                Some(region.id.clone()),
                                None,
                                Some(parameter.value.id.clone()),
                            );
                        }
                    }
                }
                (ParameterSource::Capture { .. }, false) => {}
                _ => self.error(
                    ValidationCode::InvalidRootParameter,
                    if is_root {
                        "root parameters must originate from WorkflowInput or RunMetadata"
                    } else {
                        "child parameters must originate from an explicit Capture"
                    },
                    Some(region.id.clone()),
                    None,
                    Some(parameter.value.id.clone()),
                ),
            }
            visible.insert(
                parameter.value.id.clone(),
                parameter.value.value_type.clone(),
            );
        }

        if is_root && (workflow_inputs != 1 || run_inputs != 1) {
            self.error(
                ValidationCode::InvalidRootParameter,
                "workflow region must define exactly one input and one safe run parameter",
                Some(region.id.clone()),
                None,
                None,
            );
        }

        let mut branches = BTreeMap::<OperationId, BranchInfo>::new();
        for (operation_index, operation) in region.operations.iter().enumerate() {
            let previous_operation = operation_index
                .checked_sub(1)
                .and_then(|index| region.operations.get(index));
            if !operation.id.path().is_descendant_of(region.id.path()) {
                self.error(
                    ValidationCode::InvalidStableIdentity,
                    "operation stable path must be below its containing region",
                    Some(region.id.clone()),
                    Some(operation.id.clone()),
                    None,
                );
            }
            self.validate_output_identity(region, operation);

            match &operation.kind {
                OperationKind::Const { value } => {
                    self.validate_const(region, operation, value);
                }
                OperationKind::Project { source, path } => {
                    self.validate_project(region, operation, source, path, &visible);
                }
                OperationKind::Object { fields } => {
                    self.validate_object(region, operation, fields, &visible);
                }
                OperationKind::Array { items } => {
                    self.validate_array(region, operation, items, &visible);
                }
                OperationKind::Template { text, bindings } => {
                    self.validate_template(region, operation, text, bindings, &visible);
                }
                OperationKind::Call(call) => {
                    self.validate_call(region, operation, call, previous_operation, &visible);
                }
                OperationKind::Parallel(parallel) => {
                    self.validate_parallel(region, operation, parallel, &visible);
                }
                OperationKind::Branch(branch) => {
                    if let Some(info) = self.validate_branch(region, operation, branch, &visible) {
                        branches.insert(operation.id.clone(), info);
                    }
                }
                OperationKind::Phi(phi) => {
                    self.validate_phi(region, operation, phi, &visible, &mut branches);
                }
            }

            visible.insert(
                operation.output.id.clone(),
                operation.output.value_type.clone(),
            );
        }

        for (branch, info) in branches {
            if info.phi_count != 1 {
                self.error(
                    ValidationCode::InvalidPhi,
                    format!("branch '{branch}' must be consumed by exactly one Phi"),
                    Some(region.id.clone()),
                    Some(branch),
                    None,
                );
            }
        }

        self.validate_terminator(region, is_root, &visible);
    }

    fn validate_output_identity(&mut self, region: &Region, operation: &Operation) {
        let expected = match (&operation.kind, operation.id.role()) {
            (
                OperationKind::Const { .. }
                | OperationKind::Project { .. }
                | OperationKind::Object { .. }
                | OperationKind::Array { .. }
                | OperationKind::Template { .. },
                OperationRole::Expression(ordinal),
            ) => Some(ValueRole::ExpressionOutput(ordinal)),
            (OperationKind::Call(_) | OperationKind::Parallel(_), OperationRole::Authored) => {
                Some(ValueRole::AuthoredOutput)
            }
            (OperationKind::Branch(_), OperationRole::Authored) => Some(ValueRole::BranchControl),
            (OperationKind::Phi(_), OperationRole::Phi) => Some(ValueRole::PhiOutput),
            _ => None,
        };
        let Some(expected) = expected else {
            self.error(
                ValidationCode::InvalidOperationRole,
                "operation kind does not match its stable synthetic role",
                Some(region.id.clone()),
                Some(operation.id.clone()),
                Some(operation.output.id.clone()),
            );
            return;
        };
        if operation.output.id.path() != operation.id.path()
            || operation.output.id.role() != expected
        {
            self.error(
                ValidationCode::InvalidValueRole,
                "operation output path/role does not match its producer",
                Some(region.id.clone()),
                Some(operation.id.clone()),
                Some(operation.output.id.clone()),
            );
        }
    }

    fn validate_const(&mut self, region: &Region, operation: &Operation, value: &Value) {
        let Some(output) = self.output_data(region, operation) else {
            return;
        };
        let actual = infer_json_type(value);
        if empty_array_bottom_was_widened(&actual, output) {
            self.error(
                ValidationCode::TypeMismatch,
                "empty constant array output must retain the never item type",
                Some(region.id.clone()),
                Some(operation.id.clone()),
                Some(operation.output.id.clone()),
            );
            return;
        }
        self.require_assignable(region, operation, &actual, output, "constant output");
    }

    fn validate_project(
        &mut self,
        region: &Region,
        operation: &Operation,
        source: &ValueId,
        path: &StaticPath,
        visible: &BTreeMap<ValueId, IrValueType>,
    ) {
        let source_type = self.require_data_use(region, operation, source, visible);
        let output = self.output_data(region, operation);
        let (Some(source_type), Some(output)) = (source_type, output) else {
            return;
        };
        match source_type.require_path(path) {
            Ok(actual) => {
                self.require_assignable(region, operation, &actual, output, "project output")
            }
            Err(error) => self.error(
                ValidationCode::TypeMismatch,
                format!("invalid project '{}': {error}", path.as_str()),
                Some(region.id.clone()),
                Some(operation.id.clone()),
                Some(source.clone()),
            ),
        }
    }

    fn validate_object(
        &mut self,
        region: &Region,
        operation: &Operation,
        fields: &BTreeMap<String, ValueId>,
        visible: &BTreeMap<ValueId, IrValueType>,
    ) {
        let mut properties = BTreeMap::new();
        let mut complete = true;
        for (name, value) in fields {
            if let Some(value_type) = self.require_data_use(region, operation, value, visible) {
                properties.insert(
                    name.clone(),
                    PropertyType {
                        value_type: value_type.clone(),
                        required: true,
                    },
                );
            } else {
                complete = false;
            }
        }
        if complete {
            let actual = ValueType::Object(ObjectType {
                properties,
                additional_properties: None,
            });
            if let Some(output) = self.output_data(region, operation) {
                self.require_assignable(region, operation, &actual, output, "object output");
            }
        }
    }

    fn validate_array(
        &mut self,
        region: &Region,
        operation: &Operation,
        items: &[ValueId],
        visible: &BTreeMap<ValueId, IrValueType>,
    ) {
        let mut item_types = Vec::with_capacity(items.len());
        for item in items {
            if let Some(value_type) = self.require_data_use(region, operation, item, visible) {
                item_types.push(value_type.clone());
            }
        }
        if item_types.len() == items.len() {
            let item_type = if item_types.is_empty() {
                ValueType::Never
            } else {
                ValueType::unify(item_types).expect("non-empty types were supplied")
            };
            let actual = ValueType::Array(ArrayType {
                items: Box::new(item_type),
                min_items: items.len(),
            });
            if let Some(output) = self.output_data(region, operation) {
                if empty_array_bottom_was_widened(&actual, output) {
                    self.error(
                        ValidationCode::TypeMismatch,
                        "empty array output must retain the never item type",
                        Some(region.id.clone()),
                        Some(operation.id.clone()),
                        Some(operation.output.id.clone()),
                    );
                    return;
                }
                self.require_assignable(region, operation, &actual, output, "array output");
            }
        }
    }

    fn validate_template(
        &mut self,
        region: &Region,
        operation: &Operation,
        text: &str,
        bindings: &BTreeMap<Identifier, ValueId>,
        visible: &BTreeMap<ValueId, IrValueType>,
    ) {
        let compiled = compile_template(text).ok();
        let mut signature = BTreeMap::new();
        for (name, value) in bindings {
            if let Some(value_type) = self.require_data_use(region, operation, value, visible) {
                signature.insert(name.clone(), value_type.clone());
            }
        }
        let valid = compiled.as_ref().is_some_and(|compiled| {
            compiled.slots().iter().eq(bindings.keys())
                && template_accesses_match_types(compiled, &signature)
        });
        if !valid {
            self.error(
                ValidationCode::InvalidTemplate,
                "Template must use the restricted profile with exact string-typed bindings",
                Some(region.id.clone()),
                Some(operation.id.clone()),
                Some(operation.output.id.clone()),
            );
        }
        if let Some(output) = self.output_data(region, operation) {
            self.require_assignable(
                region,
                operation,
                &ValueType::String,
                output,
                "template output",
            );
        }
    }

    fn validate_call(
        &mut self,
        region: &Region,
        operation: &Operation,
        call: &Call,
        previous_operation: Option<&Operation>,
        visible: &BTreeMap<ValueId, IrValueType>,
    ) {
        if call.target != call.plan.target() {
            self.invalid_call(
                region,
                operation,
                "Call target does not match its typed plan variant",
                None,
            );
        }

        let mut dependencies = call.plan.dependencies();
        dependencies.extend(call.inputs.values().cloned());
        let mut dependency_types = BTreeMap::new();
        for value in dependencies {
            if let Some(value_type) = self.require_data_use(region, operation, &value, visible) {
                dependency_types.insert(value, value_type.clone());
            }
        }

        self.validate_contract(
            call.plan.output_contract(),
            "Call plan output",
            Some(region.id.clone()),
        );
        let operation_output = self.output_data(region, operation).cloned();
        if let Some(operation_output) = operation_output {
            if !types_equivalent(&call.plan.output_contract().value_type, &operation_output) {
                self.invalid_call(
                    region,
                    operation,
                    "Call plan output contract must equal the operation output type",
                    Some(operation.output.id.clone()),
                );
            }
        }

        match &call.plan {
            CallPlan::Action(plan) => self.validate_action_call(
                region,
                operation,
                call,
                plan,
                previous_operation,
                visible,
            ),
            CallPlan::Llm(plan) => {
                self.validate_llm_call(region, operation, call, plan, &dependency_types)
            }
        }
    }

    fn validate_action_call(
        &mut self,
        region: &Region,
        operation: &Operation,
        call: &Call,
        plan: &super::plan::CompiledActionPlan,
        previous_operation: Option<&Operation>,
        visible: &BTreeMap<ValueId, IrValueType>,
    ) {
        let input_name = Identifier::parse("input").expect("input is a stable identifier");
        if call.inputs.len() != 1
            || call.inputs.get(&input_name) != Some(&plan.input_object)
            || !matches!(
                &plan.input_contract.value_type,
                ValueType::Object(object) if object.additional_properties.is_none()
            )
        {
            self.invalid_call(
                region,
                operation,
                "Action Call.inputs must contain exactly the typed input object",
                Some(plan.input_object.clone()),
            );
        }

        let input_producer_fields = previous_operation.and_then(|producer| {
            let same_step = producer.id.path() == operation.id.path();
            let is_expression = matches!(producer.id.role(), OperationRole::Expression(_));
            match &producer.kind {
                OperationKind::Object { fields }
                    if producer.output.id == plan.input_object
                        && same_step
                        && is_expression =>
                {
                    Some((producer, fields))
                }
                _ => {
                    self.invalid_call(
                        region,
                        operation,
                        "Action input_object must be the immediately preceding same-step SSA Object output",
                        Some(plan.input_object.clone()),
                    );
                    None
                }
            }
        });

        if previous_operation.is_none() {
            self.invalid_call(
                region,
                operation,
                "Action input_object must be the immediately preceding same-step SSA Object output",
                Some(plan.input_object.clone()),
            );
        }

        if let Some((producer, fields)) = input_producer_fields {
            let mut properties = BTreeMap::new();
            let mut complete = true;
            for (name, value) in fields {
                match visible.get(value) {
                    Some(IrValueType::Data(value_type)) => {
                        properties.insert(
                            name.clone(),
                            PropertyType {
                                value_type: value_type.clone(),
                                required: true,
                            },
                        );
                    }
                    _ => complete = false,
                }
            }

            if complete {
                let constructed_type = ValueType::Object(ObjectType {
                    properties,
                    additional_properties: None,
                });
                match &producer.output.value_type {
                    IrValueType::Data(declared_type)
                        if types_equivalent(&constructed_type, declared_type) => {}
                    _ => self.invalid_call(
                        region,
                        operation,
                        "Action input Object output type must exactly describe its constructed fields",
                        Some(plan.input_object.clone()),
                    ),
                }
                if !constructed_type.is_assignable_to(&plan.input_contract.value_type) {
                    self.invalid_call(
                        region,
                        operation,
                        "Action input Object fields must satisfy the frozen input contract",
                        Some(plan.input_object.clone()),
                    );
                }
            }
        }

        if plan.action_id.trim().is_empty()
            || plan.action_id.trim() != plan.action_id
            || plan.descriptor_version.to_string().trim().is_empty()
            || !valid_sha256_hex(&plan.descriptor_hash)
        {
            self.invalid_call(
                region,
                operation,
                "Action plan must retain a canonical id, SemVer, and descriptor hash",
                None,
            );
        }

        self.validate_contract(
            &plan.input_contract,
            "Action plan input",
            Some(region.id.clone()),
        );
        self.validate_contract(
            &plan.output_contract,
            "Action plan output",
            Some(region.id.clone()),
        );
        if let Some(IrValueType::Data(actual)) = visible.get(&plan.input_object) {
            if !actual.is_assignable_to(&plan.input_contract.value_type) {
                self.invalid_call(
                    region,
                    operation,
                    "Action input object type must be assignable to its frozen input contract",
                    Some(plan.input_object.clone()),
                );
            }
        }
    }

    fn validate_llm_call(
        &mut self,
        region: &Region,
        operation: &Operation,
        call: &Call,
        plan: &CompiledLlmPlan,
        dependency_types: &BTreeMap<ValueId, ValueType>,
    ) {
        if call.inputs != plan.local_inputs {
            self.invalid_call(
                region,
                operation,
                "LLM Call.inputs must exactly equal its local input capture map",
                None,
            );
        }
        if plan.message_sources.is_empty() {
            self.invalid_call(
                region,
                operation,
                "LLM plan must contain at least one ordered message source",
                None,
            );
        }
        if !plan.parameters.value().is_object() || !plan.limits.is_valid() {
            self.invalid_call(
                region,
                operation,
                "LLM parameters and resolved request limits are invalid",
                None,
            );
        }
        let authored_message_count = plan
            .message_sources
            .iter()
            .filter(|source| matches!(source, MessageSourcePlan::Authored { .. }))
            .count();
        if authored_message_count > plan.limits.max_messages {
            self.invalid_call(
                region,
                operation,
                "static authored messages exceed the resolved message-count limit",
                None,
            );
        }

        match &plan.response {
            ValidatedResponseContract::Text => {
                self.validate_contract(
                    &plan.output_contract,
                    "LLM text response",
                    Some(region.id.clone()),
                );
            }
            ValidatedResponseContract::Json { data } => {
                self.validate_contract(data, "LLM JSON response", Some(region.id.clone()));
                let capability = crate::resources::models::planned_structured_output_capability(
                    &plan.capabilities,
                );
                if capability.is_none() {
                    self.invalid_call(
                        region,
                        operation,
                        "structured LLM responses must select exactly one output capability",
                        Some(operation.output.id.clone()),
                    );
                }
                if capability == Some(ModelCapability::JsonObjectOutput)
                    && !matches!(&data.value_type, ValueType::Object(_))
                {
                    self.invalid_call(
                        region,
                        operation,
                        "json_object_output requires a top-level object response",
                        Some(operation.output.id.clone()),
                    );
                }
            }
        }
        if !llm_output_contract_matches(&plan.response, &plan.output_contract) {
            self.invalid_call(
                region,
                operation,
                "LLM output contract must equal its direct business response contract",
                Some(operation.output.id.clone()),
            );
        }

        for (template_id, template) in &plan.templates {
            self.validate_planned_template(region, operation, template_id, template);
        }

        let mut system_prefix = true;
        let mut has_dynamic_source = false;
        let mut has_authored_user = false;
        for source in &plan.message_sources {
            match source {
                MessageSourcePlan::Authored { role, content } => {
                    if content.is_empty() {
                        self.invalid_call(
                            region,
                            operation,
                            "authored LLM message content must not be empty",
                            None,
                        );
                    }
                    if matches!(role, PlannedRole::System) {
                        if !system_prefix {
                            self.invalid_call(
                                region,
                                operation,
                                "authored system messages must form one static prefix",
                                None,
                            );
                        }
                    } else {
                        system_prefix = false;
                    }
                    if matches!(role, PlannedRole::User) {
                        has_authored_user = true;
                    }

                    for atom in content {
                        self.validate_content_atom(
                            region,
                            operation,
                            *role,
                            atom,
                            plan,
                            dependency_types,
                        );
                    }
                }
                MessageSourcePlan::Dynamic {
                    source,
                    value,
                    proven_shape,
                } => {
                    system_prefix = false;
                    has_dynamic_source = true;
                    let Some(base) = plan.local_inputs.get(source.binding()) else {
                        self.invalid_call(
                            region,
                            operation,
                            "dynamic message source must originate from a declared local input",
                            Some(value.clone()),
                        );
                        continue;
                    };
                    if !self.is_exact_local_projection(
                        region,
                        operation,
                        value,
                        base,
                        source.fields(),
                    ) {
                        self.invalid_call(
                            region,
                            operation,
                            "dynamic message source path does not match its SSA projection",
                            Some(value.clone()),
                        );
                    }
                    let Some(value_type) = dependency_types.get(value) else {
                        continue;
                    };
                    match prove_dynamic_message_array(&SchemaShape::from_value_type(value_type)) {
                        Ok(recomputed) => {
                            if &recomputed != proven_shape {
                                self.invalid_call(
                                    region,
                                    operation,
                                    "dynamic message shape proof does not match its SSA type",
                                    Some(value.clone()),
                                );
                            }
                            if recomputed.requires_vision
                                && !plan.capabilities.contains(&ModelCapability::Vision)
                            {
                                self.invalid_call(
                                    region,
                                    operation,
                                    "dynamic image messages require model Vision capability",
                                    Some(value.clone()),
                                );
                            }
                        }
                        Err(_) => self.invalid_call(
                            region,
                            operation,
                            "dynamic message source is not a closed role-correlated Message array",
                            Some(value.clone()),
                        ),
                    }
                }
            }
        }

        if !has_dynamic_source && !has_authored_user {
            self.invalid_call(
                region,
                operation,
                "fully static LLM messages must contain a user message",
                None,
            );
        }
        if !matches!(
            plan.message_sources.last(),
            Some(MessageSourcePlan::Dynamic { .. })
                | Some(MessageSourcePlan::Authored {
                    role: PlannedRole::User,
                    ..
                })
        ) {
            self.invalid_call(
                region,
                operation,
                "LLM message plan must end with a user message or runtime-validated dynamic source",
                None,
            );
        }
    }

    fn validate_planned_template(
        &mut self,
        region: &Region,
        operation: &Operation,
        template_id: &CompiledTemplateId,
        template: &PlannedTemplate,
    ) {
        if !matches!(template.profile_version, TemplateProfileVersion::V1)
            || !template
                .compiled
                .slots()
                .iter()
                .eq(template.slot_signature.keys())
            || !template_accesses_match_signature(template)
        {
            self.invalid_call(
                region,
                operation,
                "planned template slots do not match its compiled restricted AST",
                None,
            );
        }
        for value_type in template.slot_signature.values() {
            if prove_dynamic_message_array(&SchemaShape::from_value_type(value_type)).is_ok() {
                self.invalid_call(
                    region,
                    operation,
                    "dynamic Message arrays cannot enter ordinary template slots",
                    None,
                );
            }
        }

        let valid_provenance = match &template.provenance {
            TemplateProvenance::Catalog {
                prompt_id,
                asset_hash,
            } => {
                let expected_id = CompiledTemplateId::catalog(prompt_id);
                let catalog_matches = self.ir.prompts.get(prompt_id).is_some_and(|prompt| {
                    prompt.provenance == template.provenance
                        && prompt.compiled == template.compiled
                        && prompt.profile_version == template.profile_version
                });
                asset_hash == &sha256_label(template.compiled.source().as_bytes())
                    && template_id == &expected_id
                    && catalog_matches
            }
            TemplateProvenance::Inline {
                dsl_path,
                source_hash,
            } => {
                let prefix = format!("inline:{dsl_path}:");
                let canonical_ordinal = template_id
                    .as_str()
                    .strip_prefix(&prefix)
                    .and_then(|ordinal| ordinal.parse::<usize>().ok())
                    .is_some_and(|ordinal| template_id.as_str() == format!("{prefix}{ordinal}"));
                source_hash == &sha256_label(template.compiled.source().as_bytes())
                    && canonical_ordinal
                    && is_authored_inline_template_path(dsl_path)
            }
        };
        if !valid_provenance {
            self.invalid_call(
                region,
                operation,
                "planned template provenance is missing or does not match the prompt catalog",
                None,
            );
        }
    }

    fn validate_content_atom(
        &mut self,
        region: &Region,
        operation: &Operation,
        role: PlannedRole,
        atom: &CompiledContentAtom,
        plan: &CompiledLlmPlan,
        dependency_types: &BTreeMap<ValueId, ValueType>,
    ) {
        match atom {
            CompiledContentAtom::Template {
                template_id,
                bindings,
            } => {
                let Some(template) = plan.templates.get(template_id) else {
                    self.invalid_call(
                        region,
                        operation,
                        "content references a template absent from the typed plan",
                        None,
                    );
                    return;
                };
                if !bindings.keys().eq(template.slot_signature.keys()) {
                    self.invalid_call(
                        region,
                        operation,
                        "template bindings must exactly match the compiled slot signature",
                        None,
                    );
                }
                for (slot, expected) in &template.slot_signature {
                    let Some(value) = bindings.get(slot) else {
                        continue;
                    };
                    if dependency_types
                        .get(value)
                        .is_some_and(|actual| !types_equivalent(actual, expected))
                    {
                        self.invalid_call(
                            region,
                            operation,
                            "template binding type differs from its frozen slot signature",
                            Some(value.clone()),
                        );
                    }
                    match plan.local_inputs.get(slot) {
                        Some(base)
                            if self
                                .is_local_value_or_projection(region, operation, value, base) => {}
                        _ => self.invalid_call(
                            region,
                            operation,
                            "template binding must originate from its named local input",
                            Some(value.clone()),
                        ),
                    }
                }
                if matches!(role, PlannedRole::System | PlannedRole::Assistant)
                    && !bindings.is_empty()
                {
                    self.invalid_call(
                        region,
                        operation,
                        "system and assistant authored messages must be static",
                        None,
                    );
                }
            }
            CompiledContentAtom::RuntimeText { value } => {
                if matches!(role, PlannedRole::System | PlannedRole::Assistant) {
                    self.invalid_call(
                        region,
                        operation,
                        "system and assistant authored messages cannot read runtime text",
                        Some(value.clone()),
                    );
                }
                if dependency_types
                    .get(value)
                    .is_some_and(|actual| !actual.is_assignable_to(&ValueType::String))
                {
                    self.invalid_call(
                        region,
                        operation,
                        "runtime text content must have string type",
                        Some(value.clone()),
                    );
                }
                if !self.is_from_any_local_input(region, operation, value, &plan.local_inputs) {
                    self.invalid_call(
                        region,
                        operation,
                        "runtime text content must originate from an LLM local input",
                        Some(value.clone()),
                    );
                }
            }
            CompiledContentAtom::Image { value } => {
                if !matches!(role, PlannedRole::User) {
                    self.invalid_call(
                        region,
                        operation,
                        "only authored user messages may contain images",
                        Some(value.clone()),
                    );
                }
                let image_type = ValueType::Union(vec![ValueType::String, ValueType::Null]);
                if dependency_types
                    .get(value)
                    .is_some_and(|actual| !actual.is_assignable_to(&image_type))
                {
                    self.invalid_call(
                        region,
                        operation,
                        "image content must have string or nullable string type",
                        Some(value.clone()),
                    );
                }
                if !plan.capabilities.contains(&ModelCapability::Vision) {
                    self.invalid_call(
                        region,
                        operation,
                        "authored image content requires model Vision capability",
                        Some(value.clone()),
                    );
                }
                if !self.is_from_any_local_input(region, operation, value, &plan.local_inputs) {
                    self.invalid_call(
                        region,
                        operation,
                        "image content must originate from an LLM local input",
                        Some(value.clone()),
                    );
                }
            }
        }
    }

    fn is_from_any_local_input(
        &self,
        region: &Region,
        operation: &Operation,
        value: &ValueId,
        local_inputs: &BTreeMap<Identifier, ValueId>,
    ) -> bool {
        local_inputs
            .values()
            .any(|base| self.is_local_value_or_projection(region, operation, value, base))
    }

    fn is_local_value_or_projection(
        &self,
        region: &Region,
        operation: &Operation,
        value: &ValueId,
        base: &ValueId,
    ) -> bool {
        if value == base {
            return true;
        }
        region
            .operations
            .iter()
            .take_while(|candidate| candidate.id != operation.id)
            .find(|candidate| &candidate.output.id == value)
            .is_some_and(|producer| {
                matches!(
                    &producer.kind,
                    OperationKind::Project { source, .. } if source == base
                )
            })
    }

    fn is_exact_local_projection(
        &self,
        region: &Region,
        operation: &Operation,
        value: &ValueId,
        base: &ValueId,
        fields: &[String],
    ) -> bool {
        if fields.is_empty() {
            return value == base;
        }
        region
            .operations
            .iter()
            .take_while(|candidate| candidate.id != operation.id)
            .find(|candidate| &candidate.output.id == value)
            .is_some_and(|producer| {
                matches!(
                    &producer.kind,
                    OperationKind::Project { source, path }
                        if source == base && path.segments() == fields
                )
            })
    }

    fn invalid_call(
        &mut self,
        region: &Region,
        operation: &Operation,
        message: impl Into<String>,
        value: Option<ValueId>,
    ) {
        self.error(
            ValidationCode::InvalidCallPlan,
            message,
            Some(region.id.clone()),
            Some(operation.id.clone()),
            value,
        );
    }

    fn validate_parallel(
        &mut self,
        parent: &Region,
        operation: &Operation,
        parallel: &Parallel,
        visible: &BTreeMap<ValueId, IrValueType>,
    ) {
        for value in parallel.inputs.values() {
            self.require_data_use(parent, operation, value, visible);
        }
        let output = self.output_data(parent, operation).cloned();

        if parallel.branches.len() < 2 {
            self.error(
                ValidationCode::InvalidParallel,
                "Parallel must contain at least two branches",
                Some(parent.id.clone()),
                Some(operation.id.clone()),
                None,
            );
        }
        if parallel
            .max_concurrency
            .is_some_and(|limit| limit == 0 || limit > parallel.branches.len())
        {
            self.error(
                ValidationCode::InvalidParallel,
                "max_concurrency must be between one and the branch count",
                Some(parent.id.clone()),
                Some(operation.id.clone()),
                None,
            );
        }

        let mut properties = BTreeMap::new();
        for (name, child) in &parallel.branches {
            match &child.kind {
                RegionKind::ParallelBranch { name: child_name } if child_name == name => {}
                _ => self.error(
                    ValidationCode::InvalidRegionKind,
                    format!("parallel child region must identify branch '{name}'"),
                    Some(child.id.clone()),
                    Some(operation.id.clone()),
                    None,
                ),
            }
            self.validate_child_path(parent, operation, child);
            self.validate_child_boundary(parent, operation, &parallel.inputs, child, visible, None);
            self.validate_region(child, false);

            let value_type = match parallel.settle {
                ParallelSettle::All => child.result.value_type.clone(),
                ParallelSettle::AllSettled => settled_type(child.result.value_type.clone()),
            };
            properties.insert(
                name.as_str().to_string(),
                PropertyType {
                    value_type,
                    required: true,
                },
            );
        }

        if let Some(output) = output {
            let expected = ValueType::Object(ObjectType {
                properties,
                additional_properties: None,
            });
            if !types_equivalent(&expected, &output) {
                self.error(
                    ValidationCode::TypeMismatch,
                    "Parallel output type must be the exact typed branch aggregate",
                    Some(parent.id.clone()),
                    Some(operation.id.clone()),
                    Some(operation.output.id.clone()),
                );
            }
        }
    }

    fn validate_branch(
        &mut self,
        parent: &Region,
        operation: &Operation,
        branch: &Branch,
        visible: &BTreeMap<ValueId, IrValueType>,
    ) -> Option<BranchInfo> {
        let mut input_types = BTreeMap::new();
        for (name, value) in &branch.inputs {
            if let Some(value_type) = self.require_data_use(parent, operation, value, visible) {
                input_types.insert(name.clone(), value_type.clone());
            }
        }
        if branch.cases.is_empty() {
            self.error(
                ValidationCode::InvalidBranch,
                "Branch must preserve at least one ordered case before its default",
                Some(parent.id.clone()),
                Some(operation.id.clone()),
                None,
            );
        }

        let result_type = match &operation.output.value_type {
            IrValueType::Control { result_type } => result_type.clone(),
            IrValueType::Data(_) => {
                self.error(
                    ValidationCode::InvalidBranch,
                    "Branch must define one internal control token",
                    Some(parent.id.clone()),
                    Some(operation.id.clone()),
                    Some(operation.output.id.clone()),
                );
                return None;
            }
        };
        let result_contract = &branch.default.region.result;
        if !types_equivalent(&result_contract.value_type, &result_type) {
            self.error(
                ValidationCode::TypeMismatch,
                "Branch control type must equal its switch result contract type",
                Some(parent.id.clone()),
                Some(operation.id.clone()),
                Some(operation.output.id.clone()),
            );
        }

        let mut names = BTreeSet::new();
        let mut incomings = Vec::with_capacity(branch.cases.len() + 1);
        for case in &branch.cases {
            let analysis = if input_types.len() == branch.inputs.len() {
                match analyze_predicate(&case.predicate.source, &input_types) {
                    Ok(analysis) => Some(analysis),
                    Err(error) => {
                        self.error(
                            ValidationCode::InvalidPredicate,
                            format!(
                                "switch case '{}' has an invalid typed CEL predicate: {error}",
                                case.id
                            ),
                            Some(case.region.id.clone()),
                            Some(operation.id.clone()),
                            None,
                        );
                        None
                    }
                }
            } else {
                None
            };
            if !names.insert(case.id.clone()) {
                self.error(
                    ValidationCode::InvalidBranch,
                    format!("duplicate switch arm id '{}'", case.id),
                    Some(case.region.id.clone()),
                    Some(operation.id.clone()),
                    None,
                );
            }
            match &case.region.kind {
                RegionKind::SwitchArm {
                    name,
                    is_default: false,
                } if name == &case.id => {}
                _ => self.error(
                    ValidationCode::InvalidRegionKind,
                    format!("case region must identify non-default arm '{}'", case.id),
                    Some(case.region.id.clone()),
                    Some(operation.id.clone()),
                    None,
                ),
            }
            self.validate_branch_child(
                parent,
                operation,
                &branch.inputs,
                &case.region,
                result_contract,
                visible,
                analysis.as_ref().map(|analysis| &analysis.narrowed_scope),
            );
            incomings.push(case.region.id.clone());
        }

        if !names.insert(branch.default.id.clone()) {
            self.error(
                ValidationCode::InvalidBranch,
                format!("default arm id '{}' duplicates a case", branch.default.id),
                Some(branch.default.region.id.clone()),
                Some(operation.id.clone()),
                None,
            );
        }
        match &branch.default.region.kind {
            RegionKind::SwitchArm {
                name,
                is_default: true,
            } if name == &branch.default.id => {}
            _ => self.error(
                ValidationCode::InvalidRegionKind,
                format!(
                    "default region must identify default arm '{}'",
                    branch.default.id
                ),
                Some(branch.default.region.id.clone()),
                Some(operation.id.clone()),
                None,
            ),
        }
        self.validate_branch_child(
            parent,
            operation,
            &branch.inputs,
            &branch.default.region,
            result_contract,
            visible,
            (input_types.len() == branch.inputs.len()).then_some(&input_types),
        );
        incomings.push(branch.default.region.id.clone());

        Some(BranchInfo {
            token: operation.output.id.clone(),
            result_type,
            incomings,
            phi_count: 0,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_branch_child(
        &mut self,
        parent: &Region,
        operation: &Operation,
        inputs: &BTreeMap<Identifier, ValueId>,
        child: &Region,
        result_contract: &TypedContract,
        visible: &BTreeMap<ValueId, IrValueType>,
        expected_capture_types: Option<&BTreeMap<Identifier, ValueType>>,
    ) {
        self.validate_child_path(parent, operation, child);
        self.validate_child_boundary(
            parent,
            operation,
            inputs,
            child,
            visible,
            expected_capture_types,
        );
        if !child
            .result
            .value_type
            .is_assignable_to(&result_contract.value_type)
        {
            self.error(
                ValidationCode::TypeMismatch,
                "switch arm result is not assignable to the Branch merge type",
                Some(child.id.clone()),
                Some(operation.id.clone()),
                None,
            );
        }
        if child.result.schema != result_contract.schema
            || !types_equivalent(&child.result.value_type, &result_contract.value_type)
        {
            self.error(
                ValidationCode::TypeMismatch,
                "switch arms must preserve one exact result contract",
                Some(child.id.clone()),
                Some(operation.id.clone()),
                None,
            );
        }
        self.validate_region(child, false);
    }

    fn validate_phi(
        &mut self,
        region: &Region,
        operation: &Operation,
        phi: &Phi,
        visible: &BTreeMap<ValueId, IrValueType>,
        branches: &mut BTreeMap<OperationId, BranchInfo>,
    ) {
        let control_type = self.require_control_use(region, operation, &phi.token, visible);
        let Some(info) = branches.get_mut(&phi.branch) else {
            self.error(
                ValidationCode::InvalidPhi,
                "Phi must reference a preceding Branch in the same region",
                Some(region.id.clone()),
                Some(operation.id.clone()),
                Some(phi.token.clone()),
            );
            return;
        };
        if phi.token != info.token {
            self.error(
                ValidationCode::InvalidPhi,
                "Phi token does not belong to its referenced Branch",
                Some(region.id.clone()),
                Some(operation.id.clone()),
                Some(phi.token.clone()),
            );
        }
        if phi.incomings != info.incomings {
            self.error(
                ValidationCode::InvalidPhi,
                "Phi incoming regions must match ordered cases followed by default",
                Some(region.id.clone()),
                Some(operation.id.clone()),
                None,
            );
        }
        if control_type.is_some_and(|value_type| !types_equivalent(value_type, &info.result_type)) {
            self.error(
                ValidationCode::TypeMismatch,
                "Phi token result type differs from its Branch merge type",
                Some(region.id.clone()),
                Some(operation.id.clone()),
                Some(phi.token.clone()),
            );
        }
        match &operation.output.value_type {
            IrValueType::Data(value_type) if types_equivalent(value_type, &info.result_type) => {}
            _ => self.error(
                ValidationCode::TypeMismatch,
                "Phi output must be data with the Branch merge type",
                Some(region.id.clone()),
                Some(operation.id.clone()),
                Some(operation.output.id.clone()),
            ),
        }
        info.phi_count += 1;
    }

    fn validate_child_path(&mut self, parent: &Region, operation: &Operation, child: &Region) {
        if !child.id.path().is_descendant_of(operation.id.path()) {
            self.error(
                ValidationCode::InvalidStableIdentity,
                "child region path must be below its owning structured operation",
                Some(parent.id.clone()),
                Some(operation.id.clone()),
                None,
            );
        }
    }

    fn validate_child_boundary(
        &mut self,
        parent: &Region,
        operation: &Operation,
        inputs: &BTreeMap<Identifier, ValueId>,
        child: &Region,
        visible: &BTreeMap<ValueId, IrValueType>,
        expected_capture_types: Option<&BTreeMap<Identifier, ValueType>>,
    ) {
        let parameters = child
            .parameters
            .iter()
            .map(|parameter| (parameter.name.clone(), parameter))
            .collect::<BTreeMap<_, _>>();
        if parameters.len() != child.parameters.len() || parameters.len() != inputs.len() {
            self.error(
                ValidationCode::InvalidCapture,
                "child parameters must exactly match the owning step's named inputs",
                Some(child.id.clone()),
                Some(operation.id.clone()),
                None,
            );
        }

        for (name, source) in inputs {
            let source_type = self.require_data_use(parent, operation, source, visible);
            let Some(parameter) = parameters.get(name) else {
                self.error(
                    ValidationCode::InvalidCapture,
                    format!("child is missing scope capture '{name}'"),
                    Some(child.id.clone()),
                    Some(operation.id.clone()),
                    Some(source.clone()),
                );
                continue;
            };
            match &parameter.source {
                ParameterSource::Capture {
                    source: parameter_source,
                } if parameter_source == source => {}
                _ => self.error(
                    ValidationCode::InvalidCapture,
                    format!("child capture '{name}' is bound to the wrong parent value"),
                    Some(child.id.clone()),
                    Some(operation.id.clone()),
                    Some(parameter.value.id.clone()),
                ),
            }
            if let (Some(source_type), IrValueType::Data(parameter_type)) =
                (source_type, &parameter.value.value_type)
            {
                let expected_type = expected_capture_types
                    .and_then(|expected| expected.get(name))
                    .unwrap_or(source_type);
                if !expected_type.is_assignable_to(source_type) {
                    self.error(
                        ValidationCode::TypeMismatch,
                        format!("child capture '{name}' is not a safe narrowing of its source"),
                        Some(child.id.clone()),
                        Some(operation.id.clone()),
                        Some(parameter.value.id.clone()),
                    );
                }
                if !types_equivalent(expected_type, parameter_type) {
                    self.error(
                        ValidationCode::TypeMismatch,
                        format!(
                            "child capture '{name}' does not match its predicate-narrowed type"
                        ),
                        Some(child.id.clone()),
                        Some(operation.id.clone()),
                        Some(parameter.value.id.clone()),
                    );
                }
            }
        }

        for parameter in &child.parameters {
            if !inputs.contains_key(&parameter.name) {
                self.error(
                    ValidationCode::InvalidCapture,
                    format!(
                        "child declares undeclared scope capture '{}'",
                        parameter.name
                    ),
                    Some(child.id.clone()),
                    Some(operation.id.clone()),
                    Some(parameter.value.id.clone()),
                );
            }
        }
    }

    fn validate_terminator(
        &mut self,
        region: &Region,
        is_root: bool,
        visible: &BTreeMap<ValueId, IrValueType>,
    ) {
        let Some(terminator) = &region.terminator else {
            self.error(
                ValidationCode::MissingTerminator,
                "region has no terminator",
                Some(region.id.clone()),
                None,
                None,
            );
            return;
        };
        match terminator {
            Terminator::WorkflowReturn(root_return) if is_root => {
                self.validate_root_return(region, root_return, visible);
            }
            Terminator::RegionYield { value } if !is_root => {
                if let Some(value_type) =
                    self.require_data_use_at_terminator(region, value, visible)
                {
                    if !value_type.is_assignable_to(&region.result.value_type) {
                        self.error(
                            ValidationCode::TypeMismatch,
                            "RegionYield value is not assignable to the region result type",
                            Some(region.id.clone()),
                            None,
                            Some(value.clone()),
                        );
                    }
                }
            }
            Terminator::Raise { error } => {
                if !self.ir.errors.contains_key(error) {
                    self.error(
                        ValidationCode::UndeclaredError,
                        format!("Raise references undeclared workflow error '{error}'"),
                        Some(region.id.clone()),
                        None,
                        None,
                    );
                }
            }
            Terminator::WorkflowReturn(_) | Terminator::RegionYield { .. } => self.error(
                ValidationCode::WrongTerminator,
                if is_root {
                    "workflow region must end with WorkflowReturn or Raise"
                } else {
                    "child region must end with RegionYield or Raise"
                },
                Some(region.id.clone()),
                None,
                None,
            ),
        }
    }

    fn validate_root_return(
        &mut self,
        region: &Region,
        root_return: &RootReturn,
        visible: &BTreeMap<ValueId, IrValueType>,
    ) {
        if root_return.content.is_some() != root_return.format.is_some() {
            self.error(
                ValidationCode::InvalidRootReturn,
                "root content and format must either both be present or both be absent",
                Some(region.id.clone()),
                None,
                root_return.content.clone(),
            );
        }
        if let Some(content) = &root_return.content {
            if let Some(content_type) =
                self.require_data_use_at_terminator(region, content, visible)
            {
                if !content_type.is_assignable_to(&ValueType::String) {
                    self.error(
                        ValidationCode::TypeMismatch,
                        "root content must be a string",
                        Some(region.id.clone()),
                        None,
                        Some(content.clone()),
                    );
                }
            }
        }
        if let Some(data_type) =
            self.require_data_use_at_terminator(region, &root_return.data, visible)
        {
            if !data_type.is_assignable_to(&self.ir.output.value_type)
                || !data_type.is_assignable_to(&region.result.value_type)
            {
                self.error(
                    ValidationCode::TypeMismatch,
                    "root data is not assignable to the public output contract",
                    Some(region.id.clone()),
                    None,
                    Some(root_return.data.clone()),
                );
            }
        }
    }

    fn output_data<'b>(
        &mut self,
        region: &Region,
        operation: &'b Operation,
    ) -> Option<&'b ValueType> {
        match &operation.output.value_type {
            IrValueType::Data(value_type) => Some(value_type),
            IrValueType::Control { .. } => {
                self.error(
                    ValidationCode::InternalControlUse,
                    "only Branch may define an internal control token",
                    Some(region.id.clone()),
                    Some(operation.id.clone()),
                    Some(operation.output.id.clone()),
                );
                None
            }
        }
    }

    fn require_data_use<'b>(
        &mut self,
        region: &Region,
        operation: &Operation,
        value: &ValueId,
        visible: &'b BTreeMap<ValueId, IrValueType>,
    ) -> Option<&'b ValueType> {
        match self.check_use(region, Some(operation), value, visible)? {
            IrValueType::Data(value_type) => Some(value_type),
            IrValueType::Control { .. } => {
                self.error(
                    ValidationCode::InternalControlUse,
                    "Branch control token cannot be used as authored data",
                    Some(region.id.clone()),
                    Some(operation.id.clone()),
                    Some(value.clone()),
                );
                None
            }
        }
    }

    fn require_control_use<'b>(
        &mut self,
        region: &Region,
        operation: &Operation,
        value: &ValueId,
        visible: &'b BTreeMap<ValueId, IrValueType>,
    ) -> Option<&'b ValueType> {
        match self.check_use(region, Some(operation), value, visible)? {
            IrValueType::Control { result_type } => Some(result_type),
            IrValueType::Data(_) => {
                self.error(
                    ValidationCode::InvalidPhi,
                    "Phi must consume an internal Branch control token",
                    Some(region.id.clone()),
                    Some(operation.id.clone()),
                    Some(value.clone()),
                );
                None
            }
        }
    }

    fn require_data_use_at_terminator<'b>(
        &mut self,
        region: &Region,
        value: &ValueId,
        visible: &'b BTreeMap<ValueId, IrValueType>,
    ) -> Option<&'b ValueType> {
        match self.check_use(region, None, value, visible)? {
            IrValueType::Data(value_type) => Some(value_type),
            IrValueType::Control { .. } => {
                self.error(
                    ValidationCode::InternalControlUse,
                    "Branch control token cannot escape through a region terminator",
                    Some(region.id.clone()),
                    None,
                    Some(value.clone()),
                );
                None
            }
        }
    }

    fn check_use<'b>(
        &mut self,
        region: &Region,
        operation: Option<&Operation>,
        value: &ValueId,
        visible: &'b BTreeMap<ValueId, IrValueType>,
    ) -> Option<&'b IrValueType> {
        if let Some(value_type) = visible.get(value) {
            return Some(value_type);
        }
        match self.values.get(value) {
            Some(definition) if definition.owner == region.id => self.error(
                ValidationCode::UseBeforeDefinition,
                format!("value '{value}' does not dominate this use"),
                Some(region.id.clone()),
                operation.map(|value| value.id.clone()),
                Some(value.clone()),
            ),
            Some(_) => self.error(
                ValidationCode::ValueEscapesRegion,
                format!("value '{value}' belongs to another region"),
                Some(region.id.clone()),
                operation.map(|value| value.id.clone()),
                Some(value.clone()),
            ),
            None => self.error(
                ValidationCode::UnknownValue,
                format!("value '{value}' is not defined"),
                Some(region.id.clone()),
                operation.map(|value| value.id.clone()),
                Some(value.clone()),
            ),
        }
        None
    }

    fn require_assignable(
        &mut self,
        region: &Region,
        operation: &Operation,
        actual: &ValueType,
        expected: &ValueType,
        subject: &str,
    ) {
        if !actual.is_assignable_to(expected) {
            self.error(
                ValidationCode::TypeMismatch,
                format!(
                    "{subject} type '{}' is not assignable to '{}'",
                    actual.kind_name(),
                    expected.kind_name()
                ),
                Some(region.id.clone()),
                Some(operation.id.clone()),
                Some(operation.output.id.clone()),
            );
        }
    }

    fn error(
        &mut self,
        code: ValidationCode,
        message: impl Into<String>,
        region: Option<RegionId>,
        operation: Option<OperationId>,
        value: Option<ValueId>,
    ) {
        self.validation_errors.push(ValidationError {
            code,
            message: message.into(),
            region,
            operation,
            value,
        });
    }
}

fn types_equivalent(left: &ValueType, right: &ValueType) -> bool {
    left.is_assignable_to(right) && right.is_assignable_to(left)
}

fn llm_output_contract_matches(
    response: &ValidatedResponseContract,
    output: &TypedContract,
) -> bool {
    match response {
        ValidatedResponseContract::Text => types_equivalent(&output.value_type, &ValueType::String),
        ValidatedResponseContract::Json { data } => output == data,
    }
}

fn valid_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
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

fn is_authored_inline_template_path(path: &DslPath) -> bool {
    let [DslPathSegment::Key(workflow), DslPathSegment::Key(steps), DslPathSegment::Index(_), rest @ ..] =
        path.segments()
    else {
        return false;
    };
    if workflow != "workflow" || steps != "steps" {
        return false;
    }

    let mut remaining = rest;
    loop {
        match remaining {
            [DslPathSegment::Key(messages), DslPathSegment::Index(_), DslPathSegment::Key(content)]
                if messages == "messages" && content == "content" =>
            {
                return true;
            }
            [DslPathSegment::Key(messages), DslPathSegment::Index(_), DslPathSegment::Key(content), DslPathSegment::Index(_)]
                if messages == "messages" && content == "content" =>
            {
                return true;
            }
            [DslPathSegment::Key(branches), DslPathSegment::Key(_), DslPathSegment::Key(steps), DslPathSegment::Index(_), rest @ ..]
                if branches == "branches" && steps == "steps" =>
            {
                remaining = rest;
            }
            [DslPathSegment::Key(cases), DslPathSegment::Index(_), DslPathSegment::Key(steps), DslPathSegment::Index(_), rest @ ..]
                if cases == "cases" && steps == "steps" =>
            {
                remaining = rest;
            }
            [DslPathSegment::Key(default), DslPathSegment::Key(steps), DslPathSegment::Index(_), rest @ ..]
                if default == "default" && steps == "steps" =>
            {
                remaining = rest;
            }
            _ => return false,
        }
    }
}

fn template_accesses_match_signature(template: &PlannedTemplate) -> bool {
    template_accesses_match_types(&template.compiled, &template.slot_signature)
}

fn template_accesses_match_types(
    template: &CompiledTemplate,
    signature: &BTreeMap<Identifier, ValueType>,
) -> bool {
    template.accesses().iter().all(|access| {
        let Some(mut value_type) = signature.get(&access.path.root).cloned() else {
            return false;
        };
        for segment in &access.path.segments {
            value_type = match segment {
                TemplatePathSegment::Field(field) => {
                    let Ok(resolved) = value_type.require_decoded_segments([field.as_str()]) else {
                        return false;
                    };
                    resolved
                }
                TemplatePathSegment::EachItem => {
                    let Some(item) = template_array_item_type(&value_type) else {
                        return false;
                    };
                    item
                }
            };
        }
        match access.kind {
            TemplateAccessKind::Scalar => template_scalar_type(&value_type),
            TemplateAccessKind::Json => !matches!(value_type, ValueType::Any | ValueType::Never),
            TemplateAccessKind::Each => template_array_item_type(&value_type).is_some(),
        }
    })
}

fn template_array_item_type(value_type: &ValueType) -> Option<ValueType> {
    match value_type {
        ValueType::Array(array) => Some(array.items.as_ref().clone()),
        ValueType::Union(variants) => {
            let items = variants
                .iter()
                .map(template_array_item_type)
                .collect::<Option<Vec<_>>>()?;
            ValueType::unify(items).ok()
        }
        _ => None,
    }
}

fn template_scalar_type(value_type: &ValueType) -> bool {
    match value_type {
        ValueType::String | ValueType::Literal(Value::String(_)) => true,
        ValueType::Literal(
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_),
        ) => false,
        ValueType::Union(variants) => variants.iter().all(template_scalar_type),
        ValueType::Never
        | ValueType::Any
        | ValueType::Null
        | ValueType::Boolean
        | ValueType::Integer
        | ValueType::Number
        | ValueType::Array(_)
        | ValueType::Object(_) => false,
    }
}

fn empty_array_bottom_was_widened(actual: &ValueType, declared: &ValueType) -> bool {
    let ValueType::Array(actual) = actual else {
        return false;
    };
    if actual.min_items != 0 || !matches!(actual.items.as_ref(), ValueType::Never) {
        return false;
    }
    !matches!(
        declared,
        ValueType::Array(array)
            if array.min_items == 0 && matches!(array.items.as_ref(), ValueType::Never)
    )
}

fn infer_json_type(value: &Value) -> ValueType {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            ValueType::Literal(value.clone())
        }
        Value::Array(values) => {
            let item_type = if values.is_empty() {
                ValueType::Never
            } else {
                ValueType::unify(values.iter().map(infer_json_type))
                    .expect("non-empty JSON array was supplied")
            };
            ValueType::Array(ArrayType {
                items: Box::new(item_type),
                min_items: values.len(),
            })
        }
        Value::Object(values) => {
            let mut properties = BTreeMap::new();
            for (name, value) in values {
                properties.insert(
                    name.clone(),
                    PropertyType {
                        value_type: infer_json_type(value),
                        required: true,
                    },
                );
            }
            ValueType::Object(ObjectType {
                properties,
                additional_properties: None,
            })
        }
    }
}

pub(crate) fn safe_branch_error_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["category", "code", "retryable", "origin"],
        "properties": {
            "category": {
                "type": "string",
                "enum": ["workflow", "operation", "timeout"]
            },
            "code": {
                "type": "string",
                "minLength": 1,
                "maxLength": ERROR_CODE_MAX_CHARS,
                "pattern": "^[A-Z][A-Z0-9_]*$"
            },
            "retryable": {"type": "boolean"},
            "origin": {
                "type": "string",
                "minLength": 1,
                "maxLength": SAFE_BRANCH_ORIGIN_MAX_BYTES
            }
        },
        "additionalProperties": false
    })
}

pub(crate) fn safe_branch_error_type() -> ValueType {
    SchemaType::compile(&safe_branch_error_schema())
        .expect("the frozen SafeBranchError schema is inside the static type profile")
        .into_value_type()
}

pub(crate) fn settled_type(value_type: ValueType) -> ValueType {
    let ok = object_type([
        (
            "status",
            ValueType::Literal(Value::String("ok".to_string())),
        ),
        ("value", value_type),
    ]);
    let error = object_type([
        (
            "status",
            ValueType::Literal(Value::String("error".to_string())),
        ),
        ("error", safe_branch_error_type()),
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
    use serde_json::json;

    use super::*;
    use crate::dsl::vnext::{raw::ErrorCategory, template::compile_template};

    const SYSTEM_PROMPT_SOURCE: &str = "Analyze the question.";

    fn identifier(value: &str) -> Identifier {
        Identifier::parse(value).unwrap()
    }

    fn region_id(path: &str) -> RegionId {
        RegionId::new(path).unwrap()
    }

    fn authored_operation(path: &str) -> OperationId {
        OperationId::authored(path).unwrap()
    }

    fn expression_operation(path: &str, ordinal: u16) -> OperationId {
        OperationId::expression(path, ordinal).unwrap()
    }

    fn data(id: ValueId, value_type: ValueType) -> ValueDefinition {
        ValueDefinition {
            id,
            value_type: IrValueType::Data(value_type),
        }
    }

    fn expression_value(path: &str, ordinal: u16, value_type: ValueType) -> ValueDefinition {
        data(ValueId::expression(path, ordinal).unwrap(), value_type)
    }

    fn string_contract() -> TypedContract {
        TypedContract {
            schema: json!({"type":"string"}),
            value_type: ValueType::String,
        }
    }

    fn text_llm_contract() -> TypedContract {
        string_contract()
    }

    fn system_provenance() -> TemplateProvenance {
        TemplateProvenance::Catalog {
            prompt_id: identifier("system"),
            asset_hash: sha256_label(SYSTEM_PROMPT_SOURCE.as_bytes()),
        }
    }

    fn compiled_system_prompt() -> CompiledPrompt {
        CompiledPrompt {
            provenance: system_provenance(),
            compiled: compile_template(SYSTEM_PROMPT_SOURCE).unwrap(),
            profile_version: TemplateProfileVersion::V1,
        }
    }

    fn llm_plan(question: ValueId) -> CompiledLlmPlan {
        let template_id = CompiledTemplateId::catalog(&identifier("system"));
        CompiledLlmPlan {
            model: super::super::plan::ResolvedModelId::parse("general_chat").unwrap(),
            local_inputs: BTreeMap::from([(identifier("question"), question.clone())]),
            message_sources: vec![
                MessageSourcePlan::Authored {
                    role: PlannedRole::System,
                    content: vec![CompiledContentAtom::Template {
                        template_id: template_id.clone(),
                        bindings: BTreeMap::new(),
                    }],
                },
                MessageSourcePlan::Authored {
                    role: PlannedRole::User,
                    content: vec![CompiledContentAtom::RuntimeText { value: question }],
                },
            ],
            templates: BTreeMap::from([(
                template_id,
                PlannedTemplate {
                    provenance: system_provenance(),
                    compiled: compile_template(SYSTEM_PROMPT_SOURCE).unwrap(),
                    slot_signature: BTreeMap::new(),
                    profile_version: TemplateProfileVersion::V1,
                },
            )]),
            parameters: super::super::plan::ValidatedModelParameters::new(json!({})).unwrap(),
            response: ValidatedResponseContract::Text,
            output_contract: text_llm_contract(),
            capabilities: BTreeSet::new(),
            limits: super::super::plan::ResolvedRequestLimits {
                max_messages: 32,
                max_message_bytes: 16 * 1024,
                max_image_url_bytes: 4 * 1024,
                max_request_bytes: 64 * 1024,
                max_template_context_bytes: 64 * 1024,
                max_template_output_bytes: 16 * 1024,
            },
        }
    }

    fn capture_region(
        path: &str,
        kind: RegionKind,
        name: &str,
        source: ValueId,
        value_type: ValueType,
    ) -> Region {
        let captured = ValueId::parameter(path, 0).unwrap();
        Region {
            id: region_id(path),
            kind,
            parameters: vec![RegionParameter {
                name: identifier(name),
                value: data(captured.clone(), value_type.clone()),
                source: ParameterSource::Capture { source },
            }],
            operations: Vec::new(),
            result: TypedContract {
                schema: json!({"type": "string"}),
                value_type,
            },
            terminator: Some(Terminator::RegionYield { value: captured }),
        }
    }

    fn valid_ir() -> WorkflowIr {
        let input_type = object_type([("question", ValueType::String)]);
        let evidence_type = ValueType::Array(ArrayType {
            items: Box::new(ValueType::String),
            min_items: 2,
        });
        let output_type = object_type([
            ("answer", ValueType::String),
            ("evidence", evidence_type.clone()),
        ]);
        let output_schema = json!({
            "type": "object",
            "properties": {
                "answer": {"type": "string"},
                "evidence": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 2
                }
            },
            "required": ["answer", "evidence"],
            "additionalProperties": false
        });

        let input_value = ValueId::parameter("/workflow", 0).unwrap();
        let run_value = ValueId::parameter("/workflow", 1).unwrap();
        let question = ValueId::expression("/workflow/analyze", 0).unwrap();
        let chat_result = ValueId::output("/workflow/analyze").unwrap();
        let answer = chat_result.clone();
        let branch_token = ValueId::control("/workflow/selected").unwrap();
        let selected = ValueId::phi("/workflow/selected").unwrap();
        let evidence = ValueId::expression("/workflow/result", 0).unwrap();
        let result = ValueId::expression("/workflow/result", 1).unwrap();

        let parallel_branches = BTreeMap::from([
            (
                identifier("risk"),
                capture_region(
                    "/workflow/perspectives/branches/risk",
                    RegionKind::ParallelBranch {
                        name: identifier("risk"),
                    },
                    "question",
                    answer.clone(),
                    ValueType::String,
                ),
            ),
            (
                identifier("technical"),
                capture_region(
                    "/workflow/perspectives/branches/technical",
                    RegionKind::ParallelBranch {
                        name: identifier("technical"),
                    },
                    "question",
                    answer.clone(),
                    ValueType::String,
                ),
            ),
        ]);
        let parallel_type = ValueType::Object(ObjectType {
            properties: BTreeMap::from([
                (
                    "risk".to_string(),
                    PropertyType {
                        value_type: settled_type(ValueType::String),
                        required: true,
                    },
                ),
                (
                    "technical".to_string(),
                    PropertyType {
                        value_type: settled_type(ValueType::String),
                        required: true,
                    },
                ),
            ]),
            additional_properties: None,
        });

        let case = capture_region(
            "/workflow/selected/cases/available",
            RegionKind::SwitchArm {
                name: identifier("available"),
                is_default: false,
            },
            "answer",
            answer.clone(),
            ValueType::String,
        );
        let default = capture_region(
            "/workflow/selected/default/fallback",
            RegionKind::SwitchArm {
                name: identifier("fallback"),
                is_default: true,
            },
            "answer",
            answer.clone(),
            ValueType::String,
        );
        let incoming_case = case.id.clone();
        let incoming_default = default.id.clone();

        WorkflowIr {
            metadata: Metadata {
                id: identifier("researcher"),
                name: "Researcher".to_string(),
                description: "typed IR fixture".to_string(),
            },
            input: TypedContract {
                schema: json!({
                    "type": "object",
                    "properties": {"question": {"type": "string"}},
                    "required": ["question"],
                    "additionalProperties": false
                }),
                value_type: input_type.clone(),
            },
            output: TypedContract {
                schema: output_schema.clone(),
                value_type: output_type.clone(),
            },
            prompts: BTreeMap::from([(identifier("system"), compiled_system_prompt())]),
            errors: BTreeMap::from([(
                identifier("unavailable"),
                ErrorDeclaration {
                    category: ErrorCategory::Workflow,
                    code: "WORKFLOW_UNAVAILABLE".to_string(),
                    public_message: "No result is available.".to_string(),
                },
            )]),
            root: Region {
                id: region_id("/workflow"),
                kind: RegionKind::Workflow,
                parameters: vec![
                    RegionParameter {
                        name: identifier("input"),
                        value: data(input_value.clone(), input_type),
                        source: ParameterSource::WorkflowInput,
                    },
                    RegionParameter {
                        name: identifier("run"),
                        value: data(run_value, safe_run_metadata_type()),
                        source: ParameterSource::RunMetadata,
                    },
                ],
                operations: vec![
                    Operation {
                        id: expression_operation("/workflow/analyze", 0),
                        output: expression_value("/workflow/analyze", 0, ValueType::String),
                        kind: OperationKind::Project {
                            source: input_value,
                            path: StaticPath::parse("question").unwrap(),
                        },
                    },
                    Operation {
                        id: expression_operation("/workflow/analyze", 1),
                        output: expression_value("/workflow/analyze", 1, ValueType::String),
                        kind: OperationKind::Template {
                            text: "{{ question }}".to_string(),
                            bindings: BTreeMap::from([(identifier("question"), question.clone())]),
                        },
                    },
                    Operation {
                        id: authored_operation("/workflow/analyze"),
                        output: data(chat_result.clone(), text_llm_contract().value_type),
                        kind: OperationKind::Call(Box::new(Call {
                            target: CallTarget::AiChat,
                            inputs: BTreeMap::from([(identifier("question"), question.clone())]),
                            plan: CallPlan::Llm(llm_plan(question)),
                        })),
                    },
                    Operation {
                        id: expression_operation("/workflow/constants", 0),
                        output: expression_value("/workflow/constants", 0, ValueType::String),
                        kind: OperationKind::Const {
                            value: json!("fallback"),
                        },
                    },
                    Operation {
                        id: expression_operation("/workflow/arbitrary_object", 0),
                        output: expression_value("/workflow/arbitrary_object", 0, ValueType::Any),
                        kind: OperationKind::Object {
                            fields: BTreeMap::from([("display-name".to_string(), answer.clone())]),
                        },
                    },
                    Operation {
                        id: authored_operation("/workflow/perspectives"),
                        output: data(
                            ValueId::output("/workflow/perspectives").unwrap(),
                            parallel_type,
                        ),
                        kind: OperationKind::Parallel(Parallel {
                            inputs: BTreeMap::from([(identifier("question"), answer.clone())]),
                            settle: ParallelSettle::AllSettled,
                            max_concurrency: Some(2),
                            branches: parallel_branches,
                        }),
                    },
                    Operation {
                        id: authored_operation("/workflow/selected"),
                        output: ValueDefinition {
                            id: branch_token.clone(),
                            value_type: IrValueType::Control {
                                result_type: ValueType::String,
                            },
                        },
                        kind: OperationKind::Branch(Box::new(Branch {
                            inputs: BTreeMap::from([(identifier("answer"), answer.clone())]),
                            cases: vec![BranchCase {
                                id: identifier("available"),
                                predicate: CelProgram {
                                    source: "scope.answer != ''".to_string(),
                                },
                                region: case,
                            }],
                            default: BranchDefault {
                                id: identifier("fallback"),
                                region: default,
                            },
                        })),
                    },
                    Operation {
                        id: OperationId::phi("/workflow/selected").unwrap(),
                        output: data(selected.clone(), ValueType::String),
                        kind: OperationKind::Phi(Phi {
                            branch: authored_operation("/workflow/selected"),
                            token: branch_token,
                            incomings: vec![incoming_case, incoming_default],
                        }),
                    },
                    Operation {
                        id: expression_operation("/workflow/result", 0),
                        output: expression_value("/workflow/result", 0, evidence_type),
                        kind: OperationKind::Array {
                            items: vec![answer, selected.clone()],
                        },
                    },
                    Operation {
                        id: expression_operation("/workflow/result", 1),
                        output: expression_value("/workflow/result", 1, output_type.clone()),
                        kind: OperationKind::Object {
                            fields: BTreeMap::from([
                                ("answer".to_string(), selected.clone()),
                                ("evidence".to_string(), evidence),
                            ]),
                        },
                    },
                ],
                result: TypedContract {
                    schema: output_schema,
                    value_type: output_type,
                },
                terminator: Some(Terminator::WorkflowReturn(RootReturn {
                    content: Some(selected),
                    format: Some(OutputFormat::Markdown),
                    data: result,
                })),
            },
        }
    }

    fn call_mut(ir: &mut WorkflowIr) -> &mut Call {
        ir.root
            .operations
            .iter_mut()
            .find_map(|operation| match &mut operation.kind {
                OperationKind::Call(call) => Some(call),
                _ => None,
            })
            .expect("fixture contains a Call")
    }

    fn action_ir() -> WorkflowIr {
        let mut ir = valid_ir();
        let input_contract = ir.input.clone();
        let input_object = ValueId::expression("/workflow/analyze", 1).unwrap();
        ir.root.operations[1].output.value_type =
            IrValueType::Data(input_contract.value_type.clone());
        ir.root.operations[1].kind = OperationKind::Object {
            fields: BTreeMap::from([(
                "question".to_string(),
                ValueId::expression("/workflow/analyze", 0).unwrap(),
            )]),
        };
        let call_operation = ir
            .root
            .operations
            .iter_mut()
            .find(|operation| matches!(operation.kind, OperationKind::Call(_)))
            .unwrap();
        call_operation.output.value_type = IrValueType::Data(ValueType::String);
        let OperationKind::Call(call) = &mut call_operation.kind else {
            unreachable!()
        };
        **call = Call {
            target: CallTarget::ActionCall,
            inputs: BTreeMap::from([(identifier("input"), input_object.clone())]),
            plan: CallPlan::Action(super::super::plan::CompiledActionPlan {
                action_id: "example.action".to_string(),
                descriptor_version: semver::Version::parse("1.2.3").unwrap(),
                descriptor_hash: "ab".repeat(32),
                input_object,
                input_contract,
                output_contract: string_contract(),
            }),
        };
        ir
    }

    fn error_codes(result: Result<(), Vec<ValidationError>>) -> BTreeSet<ValidationCode> {
        result
            .expect_err("IR should be rejected")
            .into_iter()
            .map(|error| error.code)
            .collect()
    }

    #[test]
    fn accepts_complete_typed_ir_and_arbitrary_object_keys() {
        assert_eq!(validate(&valid_ir()), Ok(()));
        assert_eq!(validate(&action_ir()), Ok(()));
    }

    #[test]
    fn llm_output_contract_is_the_exact_direct_response_contract() {
        let text = ValidatedResponseContract::Text;
        assert!(llm_output_contract_matches(&text, &string_contract()));
        assert!(llm_output_contract_matches(
            &text,
            &TypedContract {
                schema: json!({"type":"string", "minLength":1}),
                value_type: ValueType::String,
            },
        ));
        assert!(!llm_output_contract_matches(
            &text,
            &TypedContract {
                schema: json!({"type":"number"}),
                value_type: ValueType::Number,
            },
        ));

        let schema = json!({
            "type":"object",
            "properties":{"answer":{"type":"string"}},
            "required":["answer"],
            "additionalProperties":false
        });
        let data = TypedContract {
            value_type: SchemaType::compile(&schema).unwrap().into_value_type(),
            schema,
        };
        let structured = ValidatedResponseContract::Json { data: data.clone() };
        assert!(llm_output_contract_matches(&structured, &data));

        let widened_schema = json!({"type":"object"});
        let widened = TypedContract {
            value_type: SchemaType::compile(&widened_schema)
                .unwrap()
                .into_value_type(),
            schema: widened_schema,
        };
        assert!(!llm_output_contract_matches(&structured, &widened));
    }

    #[test]
    fn safe_branch_error_schema_is_closed_refined_and_matches_the_ir_type() {
        let schema = safe_branch_error_schema();
        let schema_type = SchemaType::compile(&schema).unwrap().into_value_type();
        assert!(types_equivalent(&schema_type, &safe_branch_error_type()));

        let validator = compile_schema_2020(&schema).unwrap();
        for category in ["workflow", "operation", "timeout"] {
            assert!(validator.is_valid(&json!({
                "category": category,
                "code": "OPERATION_TIMEOUT",
                "retryable": false,
                "origin": "/workflow/parallel/branches/technical/call#Authored"
            })));
        }
        for invalid in [
            json!({
                "category":"internal",
                "code":"VALID",
                "retryable":false,
                "origin":"/workflow/call#Authored"
            }),
            json!({
                "category":"operation",
                "code":"lowercase",
                "retryable":false,
                "origin":"/workflow/call#Authored"
            }),
            json!({
                "category":"operation",
                "code":"VALID",
                "retryable":false,
                "origin":"x".repeat(SAFE_BRANCH_ORIGIN_MAX_BYTES + 1)
            }),
            json!({
                "category":"operation",
                "code":"VALID",
                "retryable":false,
                "origin":"/workflow/call#Authored",
                "message":"must stay private"
            }),
        ] {
            assert!(!validator.is_valid(&invalid));
        }
    }

    #[test]
    fn rejects_forged_error_declarations_and_oversized_stable_origins() {
        let mut invalid_code = valid_ir();
        invalid_code
            .errors
            .get_mut(&identifier("unavailable"))
            .unwrap()
            .code = "lowercase".to_string();
        assert!(
            error_codes(validate(&invalid_code)).contains(&ValidationCode::InvalidErrorDeclaration)
        );

        let mut invalid_message = valid_ir();
        invalid_message
            .errors
            .get_mut(&identifier("unavailable"))
            .unwrap()
            .public_message = "x".repeat(super::super::raw::ERROR_PUBLIC_MESSAGE_MAX_CHARS + 1);
        assert!(error_codes(validate(&invalid_message))
            .contains(&ValidationCode::InvalidErrorDeclaration));

        let oversized = format!("/{}", "a".repeat(SAFE_BRANCH_ORIGIN_MAX_BYTES));
        assert!(RegionId::new(&oversized).is_err());
        assert!(!is_safe_branch_origin(&oversized));
        assert!(is_safe_branch_origin(
            "/workflow/parallel/branches/technical/call#Expression(12)"
        ));
    }

    #[test]
    fn rejects_call_target_input_output_and_dependency_forgery() {
        let mut wrong_target = valid_ir();
        call_mut(&mut wrong_target).target = CallTarget::ActionCall;
        assert!(error_codes(validate(&wrong_target)).contains(&ValidationCode::InvalidCallPlan));

        let mut wrong_inputs = valid_ir();
        call_mut(&mut wrong_inputs).inputs.clear();
        assert!(error_codes(validate(&wrong_inputs)).contains(&ValidationCode::InvalidCallPlan));

        let mut wrong_output = valid_ir();
        let CallPlan::Llm(plan) = &mut call_mut(&mut wrong_output).plan else {
            unreachable!()
        };
        plan.output_contract = TypedContract {
            schema: json!({"type":"number"}),
            value_type: ValueType::Number,
        };
        assert!(error_codes(validate(&wrong_output)).contains(&ValidationCode::InvalidCallPlan));

        let mut unknown_dependency = valid_ir();
        let CallPlan::Llm(plan) = &mut call_mut(&mut unknown_dependency).plan else {
            unreachable!()
        };
        let MessageSourcePlan::Authored { content, .. } = &mut plan.message_sources[1] else {
            unreachable!()
        };
        content[0] = CompiledContentAtom::RuntimeText {
            value: ValueId::output("/workflow/missing").unwrap(),
        };
        assert!(error_codes(validate(&unknown_dependency)).contains(&ValidationCode::UnknownValue));
    }

    #[test]
    fn rejects_forged_llm_message_template_and_capability_contracts() {
        let mut valid_template_binding = valid_ir();
        let CallPlan::Llm(plan) = &mut call_mut(&mut valid_template_binding).plan else {
            unreachable!()
        };
        let template_path = crate::dsl::DslPath::root()
            .child_key("workflow")
            .child_key("steps")
            .child_index(0)
            .child_key("messages")
            .child_index(1)
            .child_key("content");
        let inline_id = CompiledTemplateId::inline(&template_path, 0);
        plan.templates.insert(
            inline_id.clone(),
            PlannedTemplate {
                provenance: TemplateProvenance::Inline {
                    dsl_path: template_path,
                    source_hash: sha256_label("{{ question }}".as_bytes()),
                },
                compiled: compile_template("{{ question }}").unwrap(),
                slot_signature: BTreeMap::from([(identifier("question"), ValueType::String)]),
                profile_version: TemplateProfileVersion::V1,
            },
        );
        let MessageSourcePlan::Authored { content, .. } = &mut plan.message_sources[1] else {
            unreachable!()
        };
        content[0] = CompiledContentAtom::Template {
            template_id: inline_id.clone(),
            bindings: BTreeMap::from([(
                identifier("question"),
                ValueId::expression("/workflow/analyze", 0).unwrap(),
            )]),
        };
        assert_eq!(validate(&valid_template_binding), Ok(()));

        let mut stable_id_pseudo_path = valid_template_binding.clone();
        let CallPlan::Llm(plan) = &mut call_mut(&mut stable_id_pseudo_path).plan else {
            unreachable!()
        };
        let mut template = plan.templates.remove(&inline_id).unwrap();
        let pseudo_path = crate::dsl::DslPath::root()
            .child_key("workflow")
            .child_key("analyze")
            .child_key("messages")
            .child_index(1)
            .child_key("content");
        let pseudo_id = CompiledTemplateId::inline(&pseudo_path, 0);
        let TemplateProvenance::Inline { dsl_path, .. } = &mut template.provenance else {
            unreachable!()
        };
        *dsl_path = pseudo_path;
        plan.templates.insert(pseudo_id.clone(), template);
        let MessageSourcePlan::Authored { content, .. } = &mut plan.message_sources[1] else {
            unreachable!()
        };
        let CompiledContentAtom::Template { template_id, .. } = &mut content[0] else {
            unreachable!()
        };
        *template_id = pseudo_id;
        assert!(error_codes(validate(&stable_id_pseudo_path))
            .contains(&ValidationCode::InvalidCallPlan));

        let mut forged_source_hash = valid_template_binding.clone();
        let CallPlan::Llm(plan) = &mut call_mut(&mut forged_source_hash).plan else {
            unreachable!()
        };
        let TemplateProvenance::Inline { source_hash, .. } =
            &mut plan.templates.get_mut(&inline_id).unwrap().provenance
        else {
            unreachable!()
        };
        *source_hash = "sha256:forged".to_string();
        assert!(
            error_codes(validate(&forged_source_hash)).contains(&ValidationCode::InvalidCallPlan)
        );

        let mut missing_binding = valid_template_binding.clone();
        let CallPlan::Llm(plan) = &mut call_mut(&mut missing_binding).plan else {
            unreachable!()
        };
        let MessageSourcePlan::Authored { content, .. } = &mut plan.message_sources[1] else {
            unreachable!()
        };
        let CompiledContentAtom::Template { bindings, .. } = &mut content[0] else {
            unreachable!()
        };
        bindings.clear();
        assert!(error_codes(validate(&missing_binding)).contains(&ValidationCode::InvalidCallPlan));

        let mut empty_messages = valid_ir();
        let CallPlan::Llm(plan) = &mut call_mut(&mut empty_messages).plan else {
            unreachable!()
        };
        plan.message_sources.clear();
        assert!(error_codes(validate(&empty_messages)).contains(&ValidationCode::InvalidCallPlan));

        let mut invalid_limits = valid_ir();
        let CallPlan::Llm(plan) = &mut call_mut(&mut invalid_limits).plan else {
            unreachable!()
        };
        plan.limits.max_messages = 0;
        assert!(error_codes(validate(&invalid_limits)).contains(&ValidationCode::InvalidCallPlan));

        let mut too_many_static_messages = valid_ir();
        let CallPlan::Llm(plan) = &mut call_mut(&mut too_many_static_messages).plan else {
            unreachable!()
        };
        plan.limits.max_messages = 1;
        assert!(error_codes(validate(&too_many_static_messages))
            .contains(&ValidationCode::InvalidCallPlan));

        let mut late_system = valid_ir();
        let CallPlan::Llm(plan) = &mut call_mut(&mut late_system).plan else {
            unreachable!()
        };
        let user = plan.message_sources[1].clone();
        plan.message_sources.push(MessageSourcePlan::Authored {
            role: PlannedRole::System,
            content: vec![CompiledContentAtom::Template {
                template_id: CompiledTemplateId::catalog(&identifier("system")),
                bindings: BTreeMap::new(),
            }],
        });
        plan.message_sources.push(user);
        assert!(error_codes(validate(&late_system)).contains(&ValidationCode::InvalidCallPlan));

        let mut missing_template = valid_ir();
        let CallPlan::Llm(plan) = &mut call_mut(&mut missing_template).plan else {
            unreachable!()
        };
        plan.templates.clear();
        assert!(error_codes(validate(&missing_template)).contains(&ValidationCode::InvalidCallPlan));

        let mut image_without_vision = valid_ir();
        let CallPlan::Llm(plan) = &mut call_mut(&mut image_without_vision).plan else {
            unreachable!()
        };
        let MessageSourcePlan::Authored { content, .. } = &mut plan.message_sources[1] else {
            unreachable!()
        };
        content.push(CompiledContentAtom::Image {
            value: ValueId::expression("/workflow/analyze", 0).unwrap(),
        });
        assert!(
            error_codes(validate(&image_without_vision)).contains(&ValidationCode::InvalidCallPlan)
        );
        assert!(!template_scalar_type(&ValueType::Null));
        assert!(!template_scalar_type(&ValueType::Literal(Value::Null)));
    }

    #[test]
    fn rejects_forged_dynamic_message_proofs_and_non_local_content() {
        let history_json = json!([{
            "role":"user",
            "content":[{"text":"prior context"}]
        }]);
        let history_type = infer_json_type(&history_json);
        let history = ValueId::expression("/workflow/history", 0).unwrap();
        let mut valid_dynamic = valid_ir();
        valid_dynamic.root.operations.insert(
            2,
            Operation {
                id: expression_operation("/workflow/history", 0),
                output: expression_value("/workflow/history", 0, history_type.clone()),
                kind: OperationKind::Const {
                    value: history_json,
                },
            },
        );
        let call = call_mut(&mut valid_dynamic);
        call.inputs.insert(identifier("history"), history.clone());
        let CallPlan::Llm(plan) = &mut call.plan else {
            unreachable!()
        };
        plan.local_inputs
            .insert(identifier("history"), history.clone());
        plan.message_sources.push(MessageSourcePlan::Dynamic {
            source: super::super::value::LocalInputPath::parse("inputs.history").unwrap(),
            value: history,
            proven_shape: prove_dynamic_message_array(&SchemaShape::from_value_type(&history_type))
                .unwrap(),
        });
        assert_eq!(validate(&valid_dynamic), Ok(()));

        let mut forged_proof = valid_dynamic.clone();
        let CallPlan::Llm(plan) = &mut call_mut(&mut forged_proof).plan else {
            unreachable!()
        };
        let MessageSourcePlan::Dynamic { proven_shape, .. } =
            plan.message_sources.last_mut().unwrap()
        else {
            unreachable!()
        };
        proven_shape.requires_vision = true;
        assert!(error_codes(validate(&forged_proof)).contains(&ValidationCode::InvalidCallPlan));

        let mut invalid_dynamic = valid_ir();
        let CallPlan::Llm(plan) = &mut call_mut(&mut invalid_dynamic).plan else {
            unreachable!()
        };
        plan.message_sources.push(MessageSourcePlan::Dynamic {
            source: super::super::value::LocalInputPath::parse("inputs.question").unwrap(),
            value: ValueId::expression("/workflow/analyze", 0).unwrap(),
            proven_shape: super::super::shape::DynamicMessageShapeProof {
                requires_vision: false,
            },
        });
        assert!(error_codes(validate(&invalid_dynamic)).contains(&ValidationCode::InvalidCallPlan));

        let mut non_local = valid_ir();
        let foreign = ValueId::expression("/workflow/analyze", 1).unwrap();
        let CallPlan::Llm(plan) = &mut call_mut(&mut non_local).plan else {
            unreachable!()
        };
        let MessageSourcePlan::Authored { content, .. } = &mut plan.message_sources[1] else {
            unreachable!()
        };
        content[0] = CompiledContentAtom::RuntimeText { value: foreign };
        assert!(error_codes(validate(&non_local)).contains(&ValidationCode::InvalidCallPlan));
    }

    #[test]
    fn rejects_forged_action_identity_object_and_contracts() {
        let mut bad_hash = action_ir();
        let CallPlan::Action(plan) = &mut call_mut(&mut bad_hash).plan else {
            unreachable!()
        };
        plan.descriptor_hash = "not-a-sha256".to_string();
        assert!(error_codes(validate(&bad_hash)).contains(&ValidationCode::InvalidCallPlan));

        let mut wrong_input_name = action_ir();
        let call = call_mut(&mut wrong_input_name);
        let value = call.inputs.remove(&identifier("input")).unwrap();
        call.inputs.insert(identifier("payload"), value);
        assert!(error_codes(validate(&wrong_input_name)).contains(&ValidationCode::InvalidCallPlan));

        let mut wrong_contract = action_ir();
        let CallPlan::Action(plan) = &mut call_mut(&mut wrong_contract).plan else {
            unreachable!()
        };
        plan.input_contract = string_contract();
        assert!(error_codes(validate(&wrong_contract)).contains(&ValidationCode::InvalidCallPlan));
    }

    #[test]
    fn rejects_action_plan_that_borrows_an_earlier_compatible_object() {
        let mut ir = action_ir();
        let borrowed_input = ValueId::expression("/workflow/borrowed_input", 0).unwrap();
        ir.root.operations.insert(
            1,
            Operation {
                id: expression_operation("/workflow/borrowed_input", 0),
                output: data(borrowed_input.clone(), ir.input.value_type.clone()),
                kind: OperationKind::Object {
                    fields: BTreeMap::from([(
                        "question".to_string(),
                        ValueId::expression("/workflow/analyze", 0).unwrap(),
                    )]),
                },
            },
        );
        let call = call_mut(&mut ir);
        call.inputs
            .insert(identifier("input"), borrowed_input.clone());
        let CallPlan::Action(plan) = &mut call.plan else {
            unreachable!()
        };
        plan.input_object = borrowed_input;

        assert!(error_codes(validate(&ir)).contains(&ValidationCode::InvalidCallPlan));
    }

    #[test]
    fn rejects_action_input_object_with_a_widened_declared_field_type() {
        let mut ir = action_ir();
        ir.root.operations[0].output.value_type = IrValueType::Data(ValueType::Integer);
        ir.root.operations[0].kind = OperationKind::Const { value: json!(1) };
        ir.root.operations[1].output.value_type =
            IrValueType::Data(object_type([("question", ValueType::Number)]));
        let CallPlan::Action(plan) = &mut call_mut(&mut ir).plan else {
            unreachable!()
        };
        plan.input_contract = TypedContract {
            schema: json!({
                "type": "object",
                "properties": {"question": {"type": "number"}},
                "required": ["question"],
                "additionalProperties": false
            }),
            value_type: object_type([("question", ValueType::Number)]),
        };

        assert!(error_codes(validate(&ir)).contains(&ValidationCode::InvalidCallPlan));
    }

    #[test]
    fn rejects_forged_request_limits_above_every_absolute_platform_ceiling() {
        for dimension in 0..6 {
            let mut ir = valid_ir();
            let CallPlan::Llm(plan) = &mut call_mut(&mut ir).plan else {
                unreachable!()
            };
            match dimension {
                0 => plan.limits.max_messages = usize::MAX,
                1 => plan.limits.max_message_bytes = usize::MAX,
                2 => plan.limits.max_image_url_bytes = usize::MAX,
                3 => plan.limits.max_request_bytes = usize::MAX,
                4 => plan.limits.max_template_context_bytes = usize::MAX,
                5 => plan.limits.max_template_output_bytes = usize::MAX,
                _ => unreachable!(),
            }
            assert!(error_codes(validate(&ir)).contains(&ValidationCode::InvalidCallPlan));
        }
    }

    #[test]
    fn validates_region_schema_constraints_and_rejects_forged_static_types() {
        let mut ir = valid_ir();
        let OperationKind::Parallel(parallel) = &mut ir.root.operations[5].kind else {
            panic!("expected Parallel")
        };
        let risk = parallel.branches.get_mut(&identifier("risk")).unwrap();
        risk.result.schema = json!({"type": "string", "minLength": 8});
        assert_eq!(validate(&ir), Ok(()));

        let OperationKind::Parallel(parallel) = &mut ir.root.operations[5].kind else {
            unreachable!()
        };
        parallel
            .branches
            .get_mut(&identifier("risk"))
            .unwrap()
            .result
            .value_type = ValueType::Number;
        assert!(error_codes(validate(&ir)).contains(&ValidationCode::TypeMismatch));
    }

    #[test]
    fn rejects_missing_terminator() {
        let mut ir = valid_ir();
        ir.root.terminator = None;

        assert!(error_codes(validate(&ir)).contains(&ValidationCode::MissingTerminator));
    }

    #[test]
    fn rejects_use_before_definition() {
        let mut ir = valid_ir();
        ir.root.operations.swap(0, 3);

        assert!(error_codes(validate(&ir)).contains(&ValidationCode::UseBeforeDefinition));
    }

    #[test]
    fn rejects_invalid_template_text_in_forged_ir() {
        let mut ir = valid_ir();
        let OperationKind::Template { text, .. } = &mut ir.root.operations[1].kind else {
            panic!("expected Template")
        };
        *text = "{{#if broken}}".to_string();

        assert!(error_codes(validate(&ir)).contains(&ValidationCode::InvalidTemplate));
    }

    #[test]
    fn rejects_run_parameter_type_that_differs_from_runtime_contract() {
        let mut ir = valid_ir();
        ir.root.parameters[1].value.value_type = IrValueType::Data(ValueType::Any);

        assert!(error_codes(validate(&ir)).contains(&ValidationCode::TypeMismatch));
    }

    #[test]
    fn rejects_non_boolean_and_unknown_field_predicates_in_forged_ir() {
        for source in ["scope.answer", "scope.missing == 'yes'"] {
            let mut ir = valid_ir();
            let OperationKind::Branch(branch) = &mut ir.root.operations[6].kind else {
                panic!("expected Branch")
            };
            branch.cases[0].predicate.source = source.to_string();

            assert!(error_codes(validate(&ir)).contains(&ValidationCode::InvalidPredicate));
        }
    }

    #[test]
    fn rejects_duplicate_region_operation_and_value_identities() {
        let mut duplicate_region = valid_ir();
        let OperationKind::Parallel(parallel) = &mut duplicate_region.root.operations[5].kind
        else {
            panic!("expected Parallel")
        };
        let technical_id = parallel.branches[&identifier("technical")].id.clone();
        parallel.branches.get_mut(&identifier("risk")).unwrap().id = technical_id;
        assert!(
            error_codes(validate(&duplicate_region)).contains(&ValidationCode::DuplicateRegionId)
        );

        let mut duplicate_operation = valid_ir();
        duplicate_operation.root.operations[1].id =
            duplicate_operation.root.operations[0].id.clone();
        assert!(error_codes(validate(&duplicate_operation))
            .contains(&ValidationCode::DuplicateOperationId));

        let mut duplicate_value = valid_ir();
        duplicate_value.root.operations[1].output.id =
            duplicate_value.root.operations[0].output.id.clone();
        assert!(error_codes(validate(&duplicate_value)).contains(&ValidationCode::DuplicateValueId));
    }

    #[test]
    fn rejects_value_that_escapes_its_lexical_region() {
        let mut ir = valid_ir();
        let root_answer = ValueId::output("/workflow/analyze").unwrap();
        let OperationKind::Parallel(parallel) = &mut ir.root.operations[5].kind else {
            panic!("expected Parallel")
        };
        parallel
            .branches
            .get_mut(&identifier("risk"))
            .unwrap()
            .terminator = Some(Terminator::RegionYield { value: root_answer });

        assert!(error_codes(validate(&ir)).contains(&ValidationCode::ValueEscapesRegion));
    }

    #[test]
    fn rejects_child_capture_bound_to_a_different_parent_value() {
        let mut ir = valid_ir();
        let wrong_parent_value = ValueId::expression("/workflow/analyze", 0).unwrap();
        let OperationKind::Parallel(parallel) = &mut ir.root.operations[5].kind else {
            panic!("expected Parallel")
        };
        parallel
            .branches
            .get_mut(&identifier("risk"))
            .unwrap()
            .parameters[0]
            .source = ParameterSource::Capture {
            source: wrong_parent_value,
        };

        assert!(error_codes(validate(&ir)).contains(&ValidationCode::InvalidCapture));
    }

    #[test]
    fn rejects_wrong_terminator_for_workflow() {
        let mut ir = valid_ir();
        let data = ValueId::expression("/workflow/result", 1).unwrap();
        ir.root.terminator = Some(Terminator::RegionYield { value: data });

        assert!(error_codes(validate(&ir)).contains(&ValidationCode::WrongTerminator));
    }

    #[test]
    fn rejects_undeclared_raise() {
        let mut ir = valid_ir();
        let OperationKind::Parallel(parallel) = &mut ir.root.operations[5].kind else {
            panic!("expected Parallel")
        };
        parallel
            .branches
            .get_mut(&identifier("risk"))
            .unwrap()
            .terminator = Some(Terminator::Raise {
            error: identifier("missing_error"),
        });

        assert!(error_codes(validate(&ir)).contains(&ValidationCode::UndeclaredError));
    }

    #[test]
    fn rejects_control_token_as_object_data() {
        let mut ir = valid_ir();
        let control = ValueId::control("/workflow/selected").unwrap();
        let OperationKind::Object { fields } = &mut ir.root.operations[9].kind else {
            panic!("expected Object")
        };
        fields.insert("answer".to_string(), control);

        assert!(error_codes(validate(&ir)).contains(&ValidationCode::InternalControlUse));
    }

    #[test]
    fn rejects_phi_with_reordered_incomings() {
        let mut ir = valid_ir();
        let OperationKind::Phi(phi) = &mut ir.root.operations[7].kind else {
            panic!("expected Phi")
        };
        phi.incomings.reverse();

        assert!(error_codes(validate(&ir)).contains(&ValidationCode::InvalidPhi));
    }

    #[test]
    fn rejects_malformed_branch_and_phi_shapes() {
        let mut empty_branch = valid_ir();
        let OperationKind::Branch(branch) = &mut empty_branch.root.operations[6].kind else {
            panic!("expected Branch")
        };
        branch.cases.clear();
        assert!(error_codes(validate(&empty_branch)).contains(&ValidationCode::InvalidBranch));

        let mut duplicate_arm = valid_ir();
        let OperationKind::Branch(branch) = &mut duplicate_arm.root.operations[6].kind else {
            unreachable!()
        };
        branch.default.id = branch.cases[0].id.clone();
        assert!(error_codes(validate(&duplicate_arm)).contains(&ValidationCode::InvalidBranch));

        let mut foreign_branch = valid_ir();
        let OperationKind::Phi(phi) = &mut foreign_branch.root.operations[7].kind else {
            unreachable!()
        };
        phi.branch = authored_operation("/workflow/analyze");
        assert!(error_codes(validate(&foreign_branch)).contains(&ValidationCode::InvalidPhi));

        let mut foreign_token = valid_ir();
        let OperationKind::Phi(phi) = &mut foreign_token.root.operations[7].kind else {
            unreachable!()
        };
        phi.token = ValueId::control("/workflow/foreign").unwrap();
        assert!(error_codes(validate(&foreign_token)).contains(&ValidationCode::InvalidPhi));
    }

    #[test]
    fn rejects_parallel_concurrency_above_branch_count() {
        let mut ir = valid_ir();
        let OperationKind::Parallel(parallel) = &mut ir.root.operations[5].kind else {
            panic!("expected Parallel")
        };
        parallel.max_concurrency = Some(3);

        assert!(error_codes(validate(&ir)).contains(&ValidationCode::InvalidParallel));
    }

    #[test]
    fn verifier_requires_bottom_items_for_empty_arrays() {
        let mut ir = valid_ir();
        ir.root.operations.push(Operation {
            id: expression_operation("/workflow/empty", 0),
            output: expression_value(
                "/workflow/empty",
                0,
                ValueType::Array(ArrayType {
                    items: Box::new(ValueType::Never),
                    min_items: 0,
                }),
            ),
            kind: OperationKind::Array { items: Vec::new() },
        });
        assert_eq!(validate(&ir), Ok(()));

        {
            let IrValueType::Data(ValueType::Array(array)) =
                &mut ir.root.operations.last_mut().unwrap().output.value_type
            else {
                unreachable!()
            };
            *array.items = ValueType::Any;
        }
        assert!(error_codes(validate(&ir)).contains(&ValidationCode::TypeMismatch));

        let IrValueType::Data(ValueType::Array(array)) =
            &mut ir.root.operations.last_mut().unwrap().output.value_type
        else {
            unreachable!()
        };
        *array.items = ValueType::Never;
        ir.root.operations.push(Operation {
            id: expression_operation("/workflow/empty_literal", 0),
            output: expression_value(
                "/workflow/empty_literal",
                0,
                ValueType::Array(ArrayType {
                    items: Box::new(ValueType::Any),
                    min_items: 0,
                }),
            ),
            kind: OperationKind::Const { value: json!([]) },
        });
        assert!(error_codes(validate(&ir)).contains(&ValidationCode::TypeMismatch));

        assert_eq!(
            infer_json_type(&json!([])),
            ValueType::Array(ArrayType {
                items: Box::new(ValueType::Never),
                min_items: 0,
            })
        );
    }
}
