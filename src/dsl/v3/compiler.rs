use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::{
    dsl::CompileError,
    engine::{
        plan::{
            expression::{
                analyze_cel_expression, analyze_match_program, analyze_value_program, MatchProgram,
                MatchValue, TemplatePart, ValueProgram,
            },
            AuthorFormat, BranchCase, BranchCaseId, BranchDescriptor, CatchFailureKind,
            CollectDescriptor, CollectSource, ControlEdge, ControlEdgeId, ControlPort,
            ControlPortId, DataBinding, DataBindingId, DataPort, DataPortId, DescriptorValue,
            ErrorBoundaryDescriptor, ExpressionLanguage, ForkDescriptor, ForkLegDescriptor,
            JoinDescriptor, LeafTaskDescriptor, LoopDescriptor, LoopFlavor as PlanLoopFlavor,
            MapDescriptor, MergeDescriptor, Node, NodeKind, PhiBinding, PhiBindingId, Plan,
            PlanBuilder, PlanJoinMode, PlanMetadata, PlanProperty, PlanType, PortDirection,
            PortName, PureExpression, RaiseDescriptor, ReturnDescriptor, ScopeId, ScopeKind,
            ScopeMetadata, SourceDocumentId, SourceMap, SourcePosition, SourceSpan,
            StableNodeIdGenerator, SubflowCallDescriptor, TimerDescriptor, ValueSource, VersionTag,
            WaitSignalDescriptor, CEL_EXPRESSION_ENGINE_VERSION, LITERAL_EXPRESSION_ENGINE_VERSION,
            MATCH_EXPRESSION_ENGINE_VERSION, VALUE_EXPRESSION_ENGINE_VERSION,
        },
        ContentHash, DefinitionRevisionId, LegId, NodeId,
    },
};

use super::{
    ast::{
        AuthorTypeContract, CallStep, ContentPart, HumanTaskStep, IfStep, ImageUrlContent,
        LeafKind, LeafStep, LlmContract, LoopFlavor as AuthorLoopFlavor, LoopStep, MapStep,
        MessageExpr, MessageRole, ParallelSettle, ParallelStep, Step, StructuredAuthorDocument,
        TextContent, TryStep, ValueExpr, WaitKind, WaitStep,
    },
    expression::{
        canonical_json, compile_condition, compile_numeric, fold_static_match, literal_type,
        RestrictedExpression,
    },
    raw,
    template::compile_template,
    validate, DESCRIPTOR_CONTRACT_BLOCKED, EXPRESSION_ENGINE_BLOCKED, INVALID_CONTROL_FLOW,
    INVALID_DOCUMENT, INVALID_REFERENCE, INVALID_TYPE, PROMPT_RESOURCE_BLOCKED,
};

pub const V3_COMPILER_VERSION: &str = "structured-v3-advanced-3";

const V3_LLM_DESCRIPTOR_VERSION: &str = "2";
const V3_LEAF_DESCRIPTOR_VERSION: &str = "1";

#[derive(Debug, Clone)]
pub struct CompileOptions {
    pub definition_revision_id: DefinitionRevisionId,
    pub source_id: String,
    pub source_hash: ContentHash,
    pub source_end: (u64, u32, u32),
    pub prompt_files: BTreeMap<String, String>,
}

impl CompileOptions {
    pub fn new(
        definition_revision_id: DefinitionRevisionId,
        source_id: impl Into<String>,
        source: &str,
    ) -> Self {
        let (line, column) = source_end(source);
        Self {
            definition_revision_id,
            source_id: source_id.into(),
            source_hash: ContentHash::from_bytes(source.as_bytes()),
            source_end: (source.len() as u64, line, column),
            prompt_files: BTreeMap::new(),
        }
    }

    pub fn with_prompt_file(mut self, path: impl Into<String>, content: impl Into<String>) -> Self {
        self.prompt_files.insert(path.into(), content.into());
        self
    }
}

pub fn compile_source(source: &str, options: CompileOptions) -> Result<Plan, CompileError> {
    let raw = raw::parse(source)?;
    let document = validate(raw)?;
    compile(document, options)
}

pub fn compile(
    document: StructuredAuthorDocument,
    options: CompileOptions,
) -> Result<Plan, CompileError> {
    GraphCompiler::new(document, options)?.compile()
}

#[derive(Debug, Clone)]
struct Symbol {
    source: ValueSource,
    value_type: PlanType,
    producer_port: Option<DataPortId>,
}

type Environment = BTreeMap<String, Symbol>;

#[derive(Debug, Clone)]
struct ControlPoint(ControlPortId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum AbruptExit {
    Return,
    Raise,
}

#[derive(Debug)]
enum BlockExit {
    Continue(Option<ControlPoint>),
    Yield {
        control: Option<ControlPoint>,
        value: ValueExpr,
        environment: Environment,
    },
    LoopContinue {
        control: Option<ControlPoint>,
        value: ValueExpr,
        environment: Environment,
    },
    LoopBreak {
        control: Option<ControlPoint>,
        value: ValueExpr,
        environment: Environment,
    },
    /// Every reachable path exits the current structured block. A set is
    /// required because different authored branch arms may Return or Raise.
    Abrupt(BTreeSet<AbruptExit>),
}

fn abrupt(exit: AbruptExit) -> BlockExit {
    BlockExit::Abrupt(BTreeSet::from([exit]))
}

#[derive(Debug, Clone)]
struct ResolvedValue {
    source: ValueSource,
    value_type: PlanType,
    producer_port: Option<DataPortId>,
}

#[derive(Debug, Default)]
struct LlmReferenceUsage {
    all: BTreeSet<String>,
    absence_aware: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct PendingScope {
    id: ScopeId,
    parent: Option<ScopeId>,
    owner: Option<NodeId>,
    kind: ScopeKind,
    captures: BTreeSet<DataPortId>,
}

struct GraphCompiler {
    document: StructuredAuthorDocument,
    options: CompileOptions,
    root_scope: ScopeId,
    generator: StableNodeIdGenerator,
    entry: Option<NodeId>,
    nodes: Vec<Node>,
    control_ports: Vec<ControlPort>,
    data_ports: Vec<DataPort>,
    control_edges: Vec<ControlEdge>,
    data_bindings: Vec<DataBinding>,
    phi_bindings: Vec<PhiBinding>,
    scopes: BTreeMap<ScopeId, PendingScope>,
    pure_expression_sequence: usize,
}

impl GraphCompiler {
    fn new(
        document: StructuredAuthorDocument,
        options: CompileOptions,
    ) -> Result<Self, CompileError> {
        let authored = authored_node_ids(&document.steps)
            .into_iter()
            .map(|value| node_id(&value))
            .collect::<Result<Vec<_>, _>>()?;
        let generator = StableNodeIdGenerator::with_reserved(authored).map_err(plan_error)?;
        let root_scope = scope_id("scope_root")?;
        let scopes = BTreeMap::from([(
            root_scope.clone(),
            PendingScope {
                id: root_scope.clone(),
                parent: None,
                owner: None,
                kind: ScopeKind::Root,
                captures: BTreeSet::new(),
            },
        )]);
        Ok(Self {
            document,
            options,
            root_scope,
            generator,
            entry: None,
            nodes: Vec::new(),
            control_ports: Vec::new(),
            data_ports: Vec::new(),
            control_edges: Vec::new(),
            data_bindings: Vec::new(),
            phi_bindings: Vec::new(),
            scopes,
            pure_expression_sequence: 0,
        })
    }

    fn compile(mut self) -> Result<Plan, CompileError> {
        let input_type = lower_author_type(&self.document.input_type)?;
        let output_type = lower_author_type(&self.document.output_type)?;
        for contract in self.document.types.values() {
            lower_author_type(contract)?;
        }

        let mut environment = Environment::new();
        let mut input_defaults = BTreeMap::new();
        for (name, declaration) in &self.document.inputs {
            let value_type = lower_author_type(&declaration.value_type)?;
            if let Some(value) = &declaration.default {
                if !value_type.accepts_literal(value).unwrap_or(false) {
                    return Err(CompileError::new(
                        INVALID_TYPE,
                        format!(
                            "input default for '{name}' does not satisfy its fully constrained type"
                        ),
                    ));
                }
                input_defaults.insert(name.clone(), value.clone());
            }
            environment.insert(
                name.clone(),
                Symbol {
                    source: if declaration.optional {
                        ValueSource::OptionalRunInput {
                            path: vec![name.clone()],
                        }
                    } else {
                        ValueSource::RunInput {
                            path: vec![name.clone()],
                        }
                    },
                    value_type,
                    producer_port: None,
                },
            );
        }

        let steps = self.document.steps.clone();
        let root_scope = self.root_scope.clone();
        let exit = self.compile_block(
            &steps,
            None,
            &root_scope,
            &mut environment,
            "workflow",
            BlockContext::Root,
        )?;
        if !matches!(exit, BlockExit::Abrupt(_)) {
            return Err(CompileError::new(
                INVALID_CONTROL_FLOW,
                "workflow must end every reachable path with return or raise",
            ));
        }
        let entry = self.entry.clone().ok_or_else(|| {
            CompileError::new(
                INVALID_CONTROL_FLOW,
                "workflow did not produce an entry node",
            )
        })?;

        let metadata = PlanMetadata::new(
            self.options.definition_revision_id.clone(),
            version(V3_COMPILER_VERSION)?,
            AuthorFormat::Structured,
            entry,
            crate::engine::plan::PlanInputContract::new(input_type).with_defaults(input_defaults),
            output_type,
            safe_error_type()?,
        );
        let source_map = self.complete_source_map()?;
        let mut builder = PlanBuilder::new(metadata);
        for node in self.nodes {
            builder.add_node(node);
        }
        for port in self.control_ports {
            builder.add_control_port(port);
        }
        for port in self.data_ports {
            builder.add_data_port(port);
        }
        for edge in self.control_edges {
            builder.add_control_edge(edge);
        }
        for binding in self.data_bindings {
            builder.add_data_binding(binding);
        }
        for phi in self.phi_bindings {
            builder.add_phi_binding(phi);
        }
        for scope in self.scopes.into_values() {
            let value = match (scope.parent, scope.owner) {
                (None, None) => ScopeMetadata::root(scope.id),
                (Some(parent), Some(owner)) => {
                    ScopeMetadata::child(scope.id, parent, owner, scope.kind, scope.captures)
                }
                _ => {
                    return Err(CompileError::new(
                        INVALID_CONTROL_FLOW,
                        "compiler constructed an incomplete scope",
                    ));
                }
            };
            builder.add_scope(value);
        }
        builder.set_source_map(source_map);
        builder.build().map_err(plan_error)
    }

    #[allow(clippy::too_many_arguments)]
    fn compile_block(
        &mut self,
        steps: &[Step],
        incoming: Option<ControlPoint>,
        scope: &ScopeId,
        environment: &mut Environment,
        stable_context: &str,
        block_context: BlockContext,
    ) -> Result<BlockExit, CompileError> {
        let mut control = incoming;
        for (index, step) in steps.iter().enumerate() {
            let is_last = index + 1 == steps.len();
            match step {
                Step::Leaf(step) => {
                    control = Some(self.compile_leaf(step, control, scope, environment)?);
                }
                Step::Wait(step) => {
                    control = Some(self.compile_wait(step, control, scope, environment)?);
                }
                Step::HumanTask(step) => {
                    control = Some(self.compile_human_task(step, control, scope, environment)?);
                }
                Step::Call(step) => {
                    control = Some(self.compile_call(step, control, scope, environment)?);
                }
                Step::If(step) => {
                    match self.compile_if(step, control, scope, environment, stable_context)? {
                        BlockExit::Continue(next) => control = next,
                        exit @ (BlockExit::Yield { .. }
                        | BlockExit::LoopContinue { .. }
                        | BlockExit::LoopBreak { .. }
                        | BlockExit::Abrupt(_)) => {
                            if !is_last {
                                return Err(unreachable_after_terminator());
                            }
                            return Ok(exit);
                        }
                    }
                }
                Step::Parallel(step) => {
                    control = Some(self.compile_parallel(
                        step,
                        control,
                        scope,
                        environment,
                        stable_context,
                    )?);
                }
                Step::Map(step) => {
                    control = Some(self.compile_map(
                        step,
                        control,
                        scope,
                        environment,
                        stable_context,
                    )?);
                }
                Step::Loop(step) => {
                    control = Some(self.compile_loop(
                        step,
                        control,
                        scope,
                        environment,
                        stable_context,
                    )?);
                }
                Step::Try(step) => {
                    match self.compile_try(step, control, scope, environment, stable_context)? {
                        BlockExit::Continue(next) => control = next,
                        exit @ (BlockExit::Yield { .. }
                        | BlockExit::LoopContinue { .. }
                        | BlockExit::LoopBreak { .. }
                        | BlockExit::Abrupt(_)) => {
                            if !is_last {
                                return Err(unreachable_after_terminator());
                            }
                            return Ok(exit);
                        }
                    }
                }
                Step::Yield(value) => {
                    if matches!(block_context, BlockContext::Root) {
                        return Err(CompileError::new(
                            INVALID_CONTROL_FLOW,
                            "yield is only valid as a structured if arm or parallel leg terminator",
                        ));
                    }
                    if !is_last {
                        return Err(unreachable_after_terminator());
                    }
                    return Ok(if matches!(block_context, BlockContext::LoopBody) {
                        BlockExit::LoopContinue {
                            control,
                            value: value.clone(),
                            environment: environment.clone(),
                        }
                    } else {
                        BlockExit::Yield {
                            control,
                            value: value.clone(),
                            environment: environment.clone(),
                        }
                    });
                }
                Step::Continue(value) | Step::Break(value) => {
                    if !matches!(block_context, BlockContext::LoopBody) {
                        return Err(CompileError::new(
                            INVALID_CONTROL_FLOW,
                            "break and continue are only valid as loop body terminators",
                        ));
                    }
                    if !is_last {
                        return Err(unreachable_after_terminator());
                    }
                    let exit = if matches!(step, Step::Continue(_)) {
                        BlockExit::LoopContinue {
                            control,
                            value: value.clone(),
                            environment: environment.clone(),
                        }
                    } else {
                        BlockExit::LoopBreak {
                            control,
                            value: value.clone(),
                            environment: environment.clone(),
                        }
                    };
                    return Ok(exit);
                }
                Step::Return(value) => {
                    if matches!(
                        block_context,
                        BlockContext::ParallelLeg | BlockContext::MapBody | BlockContext::LoopBody
                    ) {
                        return Err(CompileError::new(
                            INVALID_CONTROL_FLOW,
                            "return cannot escape a parallel/map/loop child scope; use its typed terminator",
                        ));
                    }
                    if !is_last {
                        return Err(unreachable_after_terminator());
                    }
                    self.compile_return(value, control, scope, environment, stable_context)?;
                    return Ok(abrupt(AbruptExit::Return));
                }
                Step::Raise(value) => {
                    if !is_last {
                        return Err(unreachable_after_terminator());
                    }
                    self.compile_raise(value, control, scope, environment, stable_context)?;
                    return Ok(abrupt(AbruptExit::Raise));
                }
            }
        }
        Ok(BlockExit::Continue(control))
    }

    fn compile_leaf(
        &mut self,
        step: &LeafStep,
        incoming: Option<ControlPoint>,
        scope: &ScopeId,
        environment: &mut Environment,
    ) -> Result<ControlPoint, CompileError> {
        self.validate_leaf_value_contracts(step, environment)?;
        let id = node_id(&step.id)?;
        let output_type = lower_author_type(step.output_type.as_ref().ok_or_else(|| {
            CompileError::new(
                DESCRIPTOR_CONTRACT_BLOCKED,
                "leaf output is owned by its versioned descriptor registry; contextual descriptor linking is required before Plan lowering",
            )
        })?)?;
        let mut public_configuration = descriptor_object(&step.configuration)?;
        let mut reference_configuration = step.configuration.clone();
        if let Some(llm) = &step.llm {
            public_configuration.remove("messages");
            if let Value::Object(configuration) = &mut reference_configuration {
                configuration.remove("messages");
            }
            public_configuration.insert("message_program".to_owned(), compile_message_program(llm));
            public_configuration.insert("stream".to_owned(), DescriptorValue::Boolean(llm.stream));
            public_configuration
                .insert("publish".to_owned(), DescriptorValue::Boolean(llm.publish));
            public_configuration.insert(
                "tools".to_owned(),
                DescriptorValue::Array(
                    llm.tools
                        .iter()
                        .cloned()
                        .map(DescriptorValue::String)
                        .collect(),
                ),
            );
            public_configuration.insert(
                "tool_choice".to_owned(),
                DescriptorValue::String(llm.tool_choice.as_str().to_owned()),
            );
            public_configuration.insert(
                "tool_limits".to_owned(),
                DescriptorValue::Object(BTreeMap::from([
                    (
                        "max_calls".to_owned(),
                        DescriptorValue::Integer(i64::from(llm.tool_limits.max_calls)),
                    ),
                    (
                        "max_rounds".to_owned(),
                        DescriptorValue::Integer(i64::from(llm.tool_limits.max_rounds)),
                    ),
                ])),
            );
        }
        let configuration_references = collect_configuration_references(&reference_configuration)?;
        let mut references = configuration_references.clone();
        let llm_usage = match &step.llm {
            Some(llm) => self.collect_llm_references(llm)?,
            None => LlmReferenceUsage::default(),
        };
        references.extend(llm_usage.all.iter().cloned());
        let absence_aware = llm_usage
            .absence_aware
            .difference(&configuration_references)
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut bindings_config = BTreeMap::new();
        let mut optional_bindings_config = Vec::new();
        let mut dependencies = Vec::new();
        for reference in references {
            let resolved = self.resolve_reference(&reference, environment)?;
            let optional = matches!(&resolved.source, ValueSource::OptionalRunInput { .. });
            if optional && !absence_aware.contains(&reference) {
                return Err(CompileError::new(
                    INVALID_REFERENCE,
                    format!(
                        "optional input '${reference}' is used where absence is not supported; provide a default or use it only as an image_url part"
                    ),
                ));
            }
            let input = self.add_data_port(
                &id,
                &format!("input_{}", dependencies.len()),
                PortDirection::Input,
                resolved.value_type.clone(),
                !optional,
            )?;
            self.capture_value(scope, &resolved)?;
            self.add_data_binding(
                resolved.source,
                input.clone(),
                &format!("leaf:{id}:{reference}"),
            )?;
            if optional {
                optional_bindings_config.push(DescriptorValue::String(reference.clone()));
            }
            bindings_config.insert(reference, DescriptorValue::String(input.to_string()));
            dependencies.push(input);
        }
        if !bindings_config.is_empty() {
            public_configuration.insert(
                "runtime_bindings".to_owned(),
                DescriptorValue::Object(bindings_config),
            );
        }
        if !optional_bindings_config.is_empty() {
            public_configuration.insert(
                "optional_runtime_bindings".to_owned(),
                DescriptorValue::Array(optional_bindings_config),
            );
        }
        if !self.document.prompts.is_empty() && step.kind == LeafKind::Llm {
            let prompt_catalog = self
                .document
                .prompts
                .iter()
                .map(|(id, value)| -> Result<_, CompileError> {
                    let (content, source_path) = match value {
                        super::ast::PromptDeclaration::Inline(value) => (value.clone(), None),
                        super::ast::PromptDeclaration::File(value) => {
                            let content = self.options.prompt_files.get(value).ok_or_else(|| {
                                CompileError::new(
                                    PROMPT_RESOURCE_BLOCKED,
                                    format!("prompt file '{value}' was not resolved into this immutable compilation revision"),
                                )
                            })?;
                            (content.clone(), Some(value.clone()))
                        }
                    };
                    compile_template(&content).map_err(|_| {
                        CompileError::new(
                            PROMPT_RESOURCE_BLOCKED,
                            format!("prompt '{id}' does not satisfy the restricted v3 template profile"),
                        )
                    })?;
                    let mut descriptor = BTreeMap::from([
                        (
                            "content".to_owned(),
                            DescriptorValue::String(content.clone()),
                        ),
                        (
                            "content_hash".to_owned(),
                            DescriptorValue::String(ContentHash::from_bytes(content.as_bytes()).to_string()),
                        ),
                        (
                            "template_profile".to_owned(),
                            DescriptorValue::String("v3-restricted-handlebars-1".to_owned()),
                        ),
                    ]);
                    if let Some(source_path) = source_path {
                        descriptor.insert(
                            "source_path".to_owned(),
                            DescriptorValue::String(source_path),
                        );
                    }
                    Ok((id.clone(), DescriptorValue::Object(descriptor)))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            public_configuration.insert(
                "prompt_catalog".to_owned(),
                DescriptorValue::Object(prompt_catalog),
            );
        }
        let descriptor = LeafTaskDescriptor::new(
            step.implementation.clone(),
            version(match step.kind {
                LeafKind::Llm => V3_LLM_DESCRIPTOR_VERSION,
                LeafKind::Action | LeafKind::Retrieval | LeafKind::Http | LeafKind::Tool => {
                    V3_LEAF_DESCRIPTOR_VERSION
                }
            })?,
            public_configuration,
        );
        let kind = match step.kind {
            LeafKind::Llm => NodeKind::LlmTask(descriptor),
            LeafKind::Action => NodeKind::ActionTask(descriptor),
            LeafKind::Retrieval => NodeKind::RetrievalTask(descriptor),
            LeafKind::Http => NodeKind::HttpTask(descriptor),
            LeafKind::Tool => NodeKind::ToolTask(descriptor),
        };
        self.add_node(id.clone(), scope.clone(), kind)?;
        self.connect_incoming(incoming, &id)?;
        let output = self.add_data_port(
            &id,
            "result",
            PortDirection::Output,
            output_type.clone(),
            false,
        )?;
        let control = self.add_control_port(&id, "out", PortDirection::Output)?;
        environment.insert(
            step.id.clone(),
            Symbol {
                source: ValueSource::Port {
                    port_id: output.clone(),
                },
                value_type: output_type,
                producer_port: Some(output),
            },
        );
        Ok(ControlPoint(control))
    }

    fn collect_llm_references(
        &self,
        contract: &LlmContract,
    ) -> Result<LlmReferenceUsage, CompileError> {
        let mut required = BTreeSet::new();
        let mut image = BTreeSet::new();
        for message in &contract.messages {
            match message {
                MessageExpr::Splice(path) => {
                    required.insert(path.source());
                }
                MessageExpr::Message { content, .. } => {
                    for part in content {
                        match part {
                            ContentPart::Text(TextContent::PromptRef(prompt_id)) => {
                                let source = self.prompt_source(prompt_id)?;
                                let template = compile_template(source).map_err(|_| {
                                    CompileError::new(
                                        PROMPT_RESOURCE_BLOCKED,
                                        format!("prompt '{prompt_id}' does not satisfy the restricted v3 template profile"),
                                    )
                                })?;
                                required.extend(template.slots().iter().cloned());
                            }
                            ContentPart::Text(TextContent::ValueRef(path)) => {
                                required.insert(path.source());
                            }
                            ContentPart::ImageUrl(ImageUrlContent::ValueRef(path)) => {
                                image.insert(path.source());
                            }
                            ContentPart::Text(TextContent::Template(template)) => {
                                let template = compile_template(&template.source).map_err(|_| {
                                    CompileError::new(
                                        INVALID_REFERENCE,
                                        "inline message template does not satisfy the restricted v3 template profile",
                                    )
                                })?;
                                required.extend(template.slots().iter().cloned());
                            }
                            ContentPart::Text(TextContent::Literal(_))
                            | ContentPart::ImageUrl(ImageUrlContent::Literal(_)) => {}
                        }
                    }
                }
            }
        }
        let mut all = required.clone();
        all.extend(image.iter().cloned());
        Ok(LlmReferenceUsage {
            all,
            absence_aware: image.difference(&required).cloned().collect(),
        })
    }

    fn prompt_source(&self, prompt_id: &str) -> Result<&str, CompileError> {
        match self.document.prompts.get(prompt_id) {
            Some(super::ast::PromptDeclaration::Inline(value)) => Ok(value),
            Some(super::ast::PromptDeclaration::File(path)) => self
                .options
                .prompt_files
                .get(path)
                .map(String::as_str)
                .ok_or_else(|| {
                    CompileError::new(
                        PROMPT_RESOURCE_BLOCKED,
                        format!("prompt file '{path}' was not resolved into this immutable compilation revision"),
                    )
                }),
            None => Err(CompileError::new(
                PROMPT_RESOURCE_BLOCKED,
                format!("prompt '{prompt_id}' is not declared"),
            )),
        }
    }

    fn validate_leaf_value_contracts(
        &self,
        step: &LeafStep,
        environment: &Environment,
    ) -> Result<(), CompileError> {
        if step.kind != LeafKind::Llm {
            return Ok(());
        }
        let llm = step
            .llm
            .as_ref()
            .expect("validated LLM leaf carries its typed message contract");
        for message in &llm.messages {
            match message {
                MessageExpr::Splice(reference) => {
                    let resolved = self.resolve_reference(&reference.source(), environment)?;
                    if !is_message_array(&resolved.value_type)? {
                        return Err(CompileError::new(
                            INVALID_TYPE,
                            "a scalar entry in llm.messages must reference Message[] for one-level splice",
                        ));
                    }
                }
                MessageExpr::Message { content, .. } => {
                    for part in content {
                        match part {
                            ContentPart::Text(TextContent::ValueRef(reference)) => {
                                let resolved =
                                    self.resolve_reference(&reference.source(), environment)?;
                                if !resolved.value_type.is_assignable_to(&PlanType::String) {
                                    return Err(CompileError::new(
                                        INVALID_TYPE,
                                        "text $reference must have non-null string type",
                                    ));
                                }
                            }
                            ContentPart::Text(TextContent::Template(template)) => {
                                for reference in &template.references {
                                    let resolved =
                                        self.resolve_reference(&reference.source(), environment)?;
                                    if !resolved.value_type.is_assignable_to(&PlanType::String) {
                                        return Err(CompileError::new(
                                        INVALID_TYPE,
                                        "text template references must have non-null string type",
                                    ));
                                    }
                                }
                            }
                            ContentPart::ImageUrl(ImageUrlContent::ValueRef(reference)) => {
                                let resolved =
                                    self.resolve_reference(&reference.source(), environment)?;
                                if !string_or_nullable_string(&resolved.value_type) {
                                    return Err(CompileError::new(
                                        INVALID_TYPE,
                                        "image_url reference must have string or string|null type",
                                    ));
                                }
                            }
                            ContentPart::Text(
                                TextContent::PromptRef(_) | TextContent::Literal(_),
                            )
                            | ContentPart::ImageUrl(ImageUrlContent::Literal(_)) => {}
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn compile_human_task(
        &mut self,
        step: &HumanTaskStep,
        incoming: Option<ControlPoint>,
        scope: &ScopeId,
        environment: &mut Environment,
    ) -> Result<ControlPoint, CompileError> {
        let payload_type = lower_author_type(&step.payload_type)?;
        if step.claim_lease_ms == 0 || step.claim_lease_ms > 30 * 24 * 60 * 60 * 1_000 {
            return Err(CompileError::new(
                INVALID_CONTROL_FLOW,
                "human_task claim_lease_ms must be between one millisecond and thirty days",
            ));
        }
        let mut assignees = step.assignees.clone();
        assignees.sort();
        assignees.dedup();
        let mut candidate_groups = step.candidate_groups.clone();
        candidate_groups.sort();
        candidate_groups.dedup();
        let id = node_id(&step.id)?;
        let request = self.resolve_value_for_node(&step.request, &id, scope, environment, false)?;
        self.capture_value(scope, &request)?;
        let request_input = self.add_data_port(
            &id,
            "request",
            PortDirection::Input,
            request.value_type.clone(),
            true,
        )?;
        self.add_data_binding(
            request.source,
            request_input.clone(),
            &format!("human_task:{id}:request"),
        )?;
        self.add_node(
            id.clone(),
            scope.clone(),
            NodeKind::HumanTask(crate::engine::plan::HumanTaskDescriptor {
                completion_signal: step.signal_name.clone(),
                request_input,
                request_type: request.value_type,
                response_type: payload_type.clone(),
                assignees,
                candidate_groups,
                claim_lease_ms: step.claim_lease_ms,
            }),
        )?;
        self.connect_incoming(incoming, &id)?;
        let payload = self.add_data_port(
            &id,
            "response",
            PortDirection::Output,
            payload_type.clone(),
            false,
        )?;
        let output = self.add_control_port(&id, "out", PortDirection::Output)?;
        environment.insert(
            step.id.clone(),
            Symbol {
                source: ValueSource::Port {
                    port_id: payload.clone(),
                },
                value_type: payload_type,
                producer_port: Some(payload),
            },
        );
        Ok(ControlPoint(output))
    }

    fn compile_call(
        &mut self,
        step: &CallStep,
        incoming: Option<ControlPoint>,
        scope: &ScopeId,
        environment: &mut Environment,
    ) -> Result<ControlPoint, CompileError> {
        let id = node_id(&step.id)?;
        let invocation_scope_id = scope_id(&stable_id(
            "scope",
            &format!("subflow:{}:invocation", step.id),
        ))?;
        let mut interface_inputs = BTreeMap::new();
        for (name, value) in &step.input {
            let input = self.resolve_subflow_input(value, &id, scope, environment)?;
            let required = !matches!(&input.source, ValueSource::OptionalRunInput { .. });
            self.capture_value(scope, &input)?;
            let input_port =
                self.add_data_port(&id, name, PortDirection::Input, input.value_type, required)?;
            self.add_data_binding(
                input.source,
                input_port.clone(),
                &format!("subflow:{}:{name}", step.id),
            )?;
            interface_inputs.insert(PortName::new(name.clone()).map_err(plan_error)?, input_port);
        }
        self.add_node(
            id.clone(),
            scope.clone(),
            NodeKind::SubflowCall(SubflowCallDescriptor {
                definition_revision_id: DefinitionRevisionId::new(step.definition_revision.clone())
                    .map_err(model_error)?,
                interface_version: version(&step.interface_version)?,
                invocation_scope_id: invocation_scope_id.clone(),
                inputs: interface_inputs,
                timeout_ms: step.timeout_ms,
            }),
        )?;
        if self
            .scopes
            .insert(
                invocation_scope_id.clone(),
                PendingScope {
                    id: invocation_scope_id,
                    parent: Some(scope.clone()),
                    owner: Some(id.clone()),
                    kind: ScopeKind::Subflow {
                        call_node_id: id.clone(),
                    },
                    captures: BTreeSet::new(),
                },
            )
            .is_some()
        {
            return Err(CompileError::new(
                INVALID_CONTROL_FLOW,
                format!(
                    "subflow '{}' produced a duplicate invocation scope",
                    step.id
                ),
            ));
        }
        self.connect_incoming(incoming, &id)?;
        let output_type = lower_author_type(&step.output_type)?;
        let result = self.add_data_port(
            &id,
            "result",
            PortDirection::Output,
            output_type.clone(),
            false,
        )?;
        let output = self.add_control_port(&id, "out", PortDirection::Output)?;
        environment.insert(
            step.id.clone(),
            Symbol {
                source: ValueSource::Port {
                    port_id: result.clone(),
                },
                value_type: output_type,
                producer_port: Some(result),
            },
        );
        Ok(ControlPoint(output))
    }

    fn resolve_subflow_input(
        &mut self,
        value: &ValueExpr,
        evaluating_node: &NodeId,
        scope: &ScopeId,
        environment: &Environment,
    ) -> Result<ResolvedValue, CompileError> {
        if let ValueExpr::Reference(reference) = value {
            return self.resolve_reference(&reference.source(), environment);
        }
        self.resolve_value_for_node(value, evaluating_node, scope, environment, false)
    }

    fn compile_map(
        &mut self,
        step: &MapStep,
        incoming: Option<ControlPoint>,
        scope: &ScopeId,
        environment: &mut Environment,
        stable_context: &str,
    ) -> Result<ControlPoint, CompileError> {
        let map_id = node_id(&step.id)?;
        let collect_id = self.generated_node(&map_id, "collect", None)?;
        if let Some(key_field) = &step.key_field {
            reject_duplicate_static_map_keys(&step.items, key_field)?;
        }
        let items =
            self.compile_array_expression(&step.items, &map_id, scope, environment, "map_items")?;
        let Some((item_type, _, _)) = items.result_type.array_constraints() else {
            return Err(CompileError::new(
                INVALID_TYPE,
                "map items must have one concrete typed array contract",
            ));
        };
        if let Some(key_field) = &step.key_field {
            require_stable_map_key(item_type, key_field)?;
        }
        let item_type = item_type.clone();
        let item_port = self.add_data_port(
            &map_id,
            "item",
            PortDirection::Output,
            item_type.clone(),
            false,
        )?;
        let body_output = self.add_control_port(&map_id, "body", PortDirection::Output)?;
        let empty_output = self.add_control_port(&map_id, "empty", PortDirection::Output)?;
        let body_scope = scope_id(&stable_id("scope", &format!("map:{}:body", step.id)))?;
        let placeholder_yield = DataPortId::new(stable_id(
            "data_port",
            &format!("{}:map_yield_placeholder", step.id),
        ))
        .map_err(plan_error)?;
        self.add_node(
            map_id.clone(),
            scope.clone(),
            NodeKind::Map(MapDescriptor {
                items,
                body_scope_id: body_scope.clone(),
                item_port: item_port.clone(),
                yield_port: placeholder_yield,
                max_concurrency: step.max_concurrency,
            }),
        )?;
        self.connect_incoming(incoming, &map_id)?;
        self.scopes.insert(
            body_scope.clone(),
            PendingScope {
                id: body_scope.clone(),
                parent: Some(scope.clone()),
                owner: Some(map_id.clone()),
                kind: ScopeKind::MapBody {
                    map_node_id: map_id.clone(),
                },
                captures: BTreeSet::from([item_port.clone()]),
            },
        );
        let mut body_environment = environment.clone();
        body_environment.insert(
            step.item_name.clone(),
            Symbol {
                source: ValueSource::Port {
                    port_id: item_port.clone(),
                },
                value_type: item_type,
                producer_port: Some(item_port.clone()),
            },
        );
        let exit = self.compile_block(
            &step.steps,
            Some(ControlPoint(body_output)),
            &body_scope,
            &mut body_environment,
            &format!("{stable_context}.{}.body", step.id),
            BlockContext::MapBody,
        )?;
        let BlockExit::Yield {
            control,
            value,
            environment: yield_environment,
        } = exit
        else {
            return Err(CompileError::new(
                INVALID_CONTROL_FLOW,
                "every normal map body path must end with one typed yield",
            ));
        };
        let control = control.ok_or_else(|| {
            CompileError::new(
                INVALID_CONTROL_FLOW,
                "map yield must follow a value-producing body node",
            )
        })?;
        let yielded = self.resolve_value(&value, &yield_environment)?;
        let yield_port = yielded.producer_port.ok_or_else(|| {
            CompileError::new(
                INVALID_CONTROL_FLOW,
                "map yield must publish an output produced inside the body",
            )
        })?;
        let producer_scope = self
            .data_ports
            .iter()
            .find(|port| port.id() == &yield_port)
            .and_then(|port| {
                self.nodes
                    .iter()
                    .find(|node| node.id() == port.owner())
                    .map(|node| node.scope_id().clone())
            });
        if producer_scope.as_ref() != Some(&body_scope) {
            return Err(CompileError::new(
                INVALID_CONTROL_FLOW,
                "map yield cannot publish a captured value from outside its body",
            ));
        }
        let map_position = self
            .nodes
            .iter()
            .position(|node| node.id() == &map_id)
            .expect("map node was inserted before compiling its body");
        let items = match self.nodes[map_position].kind() {
            NodeKind::Map(descriptor) => descriptor.items.clone(),
            _ => unreachable!("map position changed kind"),
        };
        self.nodes[map_position] = Node::new(
            map_id.clone(),
            scope.clone(),
            NodeKind::Map(MapDescriptor {
                items,
                body_scope_id: body_scope,
                item_port,
                yield_port: yield_port.clone(),
                max_concurrency: step.max_concurrency,
            }),
        );

        let body_input = self.add_control_port(&collect_id, "body", PortDirection::Input)?;
        let empty_input = self.add_control_port(&collect_id, "empty", PortDirection::Input)?;
        self.add_control_edge(
            control.0,
            body_input.clone(),
            &format!("map:{}:body_collect", step.id),
        )?;
        self.add_control_edge(
            empty_output.clone(),
            empty_input.clone(),
            &format!("map:{}:empty_collect", step.id),
        )?;
        let collect_output = self.add_control_port(&collect_id, "out", PortDirection::Output)?;
        let result_type = PlanType::Array {
            items: Box::new(yielded.value_type),
            min_items: 0,
        };
        let result = self.add_data_port(
            &collect_id,
            "result",
            PortDirection::Output,
            result_type.clone(),
            false,
        )?;
        self.add_node(
            collect_id.clone(),
            scope.clone(),
            NodeKind::Collect(CollectDescriptor {
                source: CollectSource::DynamicMap {
                    map_node_id: map_id,
                    key_field: step.key_field.clone(),
                    empty_output,
                    body_input,
                    empty_input,
                },
                output_port: result.clone(),
            }),
        )?;
        environment.insert(
            step.id.clone(),
            Symbol {
                source: ValueSource::Port {
                    port_id: result.clone(),
                },
                value_type: result_type,
                producer_port: Some(result),
            },
        );
        Ok(ControlPoint(collect_output))
    }

    fn compile_loop(
        &mut self,
        step: &LoopStep,
        incoming: Option<ControlPoint>,
        scope: &ScopeId,
        environment: &mut Environment,
        stable_context: &str,
    ) -> Result<ControlPoint, CompileError> {
        if step.flavor == AuthorLoopFlavor::Agent && !contains_agent_operation(&step.steps) {
            return Err(CompileError::new(
                INVALID_CONTROL_FLOW,
                "agent_loop must contain an llm, retrieval, tool, or fixed subflow call",
            ));
        }
        let loop_id = node_id(&step.id)?;
        let collect_id = self.generated_node(&loop_id, "collect", None)?;
        let initial =
            self.resolve_value_for_node(&step.initial, &loop_id, scope, environment, false)?;
        if initial.value_type == PlanType::Never || initial.value_type == PlanType::Any {
            return Err(CompileError::new(
                INVALID_TYPE,
                "loop initial state must have one concrete non-Never type",
            ));
        }
        self.capture_value(scope, &initial)?;
        let state_type = initial.value_type.clone();
        let initial_input = self.add_data_port(
            &loop_id,
            "initial",
            PortDirection::Input,
            state_type.clone(),
            true,
        )?;
        self.add_data_binding(
            initial.source,
            initial_input.clone(),
            &format!("loop:{}:initial", step.id),
        )?;
        let state_port = self.add_data_port(
            &loop_id,
            "state",
            PortDirection::Output,
            state_type.clone(),
            false,
        )?;
        let continue_input = self.add_control_port(&loop_id, "continue", PortDirection::Input)?;
        let body_output = self.add_control_port(&loop_id, "body", PortDirection::Output)?;
        let completed_output =
            self.add_control_port(&loop_id, "completed", PortDirection::Output)?;
        let exit_condition =
            self.compile_branch_expression(&step.until, "until", &loop_id, scope, environment)?;
        self.add_node(
            loop_id.clone(),
            scope.clone(),
            NodeKind::Loop(LoopDescriptor {
                flavor: match step.flavor {
                    AuthorLoopFlavor::Workflow => PlanLoopFlavor::Workflow,
                    AuthorLoopFlavor::Agent => PlanLoopFlavor::Agent,
                },
                continue_input: continue_input.clone(),
                body_output: body_output.clone(),
                completed_output: completed_output.clone(),
                exit_condition,
                max_iterations: step.max_iterations,
                deadline_ms: step.deadline_ms,
            }),
        )?;
        self.connect_incoming(incoming, &loop_id)?;
        let body_scope = scope_id(&stable_id("scope", &format!("loop:{}:body", step.id)))?;
        self.scopes.insert(
            body_scope.clone(),
            PendingScope {
                id: body_scope.clone(),
                parent: Some(scope.clone()),
                owner: Some(loop_id.clone()),
                kind: ScopeKind::LoopBody {
                    loop_node_id: loop_id.clone(),
                },
                captures: BTreeSet::from([state_port.clone()]),
            },
        );
        let mut body_environment = environment.clone();
        body_environment.insert(
            step.state_name.clone(),
            Symbol {
                source: ValueSource::Port {
                    port_id: state_port.clone(),
                },
                value_type: state_type.clone(),
                producer_port: Some(state_port.clone()),
            },
        );
        let exit = self.compile_block(
            &step.steps,
            Some(ControlPoint(body_output)),
            &body_scope,
            &mut body_environment,
            &format!("{stable_context}.{}.body", step.id),
            BlockContext::LoopBody,
        )?;
        let (control, value, yield_environment, is_break) = match exit {
            BlockExit::LoopContinue {
                control,
                value,
                environment,
            } => (control, value, environment, false),
            BlockExit::LoopBreak {
                control,
                value,
                environment,
            } => (control, value, environment, true),
            _ => {
                return Err(CompileError::new(
                    INVALID_CONTROL_FLOW,
                    "loop body must end with typed yield/continue or break",
                ));
            }
        };
        let control = control.ok_or_else(|| {
            CompileError::new(
                INVALID_CONTROL_FLOW,
                "loop terminator must follow a value-producing body node",
            )
        })?;
        let yielded = self.resolve_value(&value, &yield_environment)?;
        if !yielded.value_type.is_assignable_to(&state_type) {
            return Err(CompileError::new(
                INVALID_TYPE,
                "loop yield/continue/break value must satisfy the initial state type",
            ));
        }
        let yield_port = yielded.producer_port.ok_or_else(|| {
            CompileError::new(
                INVALID_CONTROL_FLOW,
                "loop state transition must publish an output produced inside the body",
            )
        })?;
        let producer_scope = self
            .data_ports
            .iter()
            .find(|port| port.id() == &yield_port)
            .and_then(|port| {
                self.nodes
                    .iter()
                    .find(|node| node.id() == port.owner())
                    .map(|node| node.scope_id().clone())
            });
        if producer_scope.as_ref() != Some(&body_scope) {
            return Err(CompileError::new(
                INVALID_CONTROL_FLOW,
                "loop state transition cannot publish a captured outer value",
            ));
        }

        let completed_input =
            self.add_control_port(&collect_id, "completed", PortDirection::Input)?;
        self.add_control_edge(
            completed_output,
            completed_input.clone(),
            &format!("loop:{}:completed_collect", step.id),
        )?;
        let break_input = if is_break {
            let input = self.add_control_port(&collect_id, "break", PortDirection::Input)?;
            self.add_control_edge(
                control.0,
                input.clone(),
                &format!("loop:{}:break_collect", step.id),
            )?;
            Some(input)
        } else {
            self.add_control_edge(
                control.0,
                continue_input,
                &format!("loop:{}:continue", step.id),
            )?;
            None
        };
        let collect_output = self.add_control_port(&collect_id, "out", PortDirection::Output)?;
        let result = self.add_data_port(
            &collect_id,
            "result",
            PortDirection::Output,
            state_type.clone(),
            false,
        )?;
        self.add_node(
            collect_id.clone(),
            scope.clone(),
            NodeKind::Collect(CollectDescriptor {
                source: CollectSource::Loop {
                    loop_node_id: loop_id,
                    initial_input,
                    state_port,
                    yield_port,
                    completed_input,
                    break_input,
                },
                output_port: result.clone(),
            }),
        )?;
        environment.insert(
            step.id.clone(),
            Symbol {
                source: ValueSource::Port {
                    port_id: result.clone(),
                },
                value_type: state_type,
                producer_port: Some(result),
            },
        );
        Ok(ControlPoint(collect_output))
    }

    fn compile_try(
        &mut self,
        step: &TryStep,
        incoming: Option<ControlPoint>,
        scope: &ScopeId,
        environment: &mut Environment,
        stable_context: &str,
    ) -> Result<BlockExit, CompileError> {
        let boundary_id = node_id(&step.id)?;
        let protected_scope = scope_id(&stable_id(
            "scope",
            &format!("error_boundary:{}:protected", step.id),
        ))?;
        let handler_scope = scope_id(&stable_id(
            "scope",
            &format!("error_boundary:{}:handler", step.id),
        ))?;
        let finalizer_scope = (!step.finally_steps.is_empty())
            .then(|| {
                scope_id(&stable_id(
                    "scope",
                    &format!("error_boundary:{}:finalizer", step.id),
                ))
            })
            .transpose()?;
        let protected_output =
            self.add_control_port(&boundary_id, "protected", PortDirection::Output)?;
        let handler_output =
            self.add_control_port(&boundary_id, "handler", PortDirection::Output)?;
        let finalizer_output = finalizer_scope
            .as_ref()
            .map(|_| self.add_control_port(&boundary_id, "finalizer", PortDirection::Output))
            .transpose()?;
        // Completion ports are allocated before compiling child blocks so the
        // boundary can be installed early (captures need its error-port owner).
        // Ports for authored-abrupt blocks are removed once the closed block
        // exits are known.
        let protected_completion_port =
            self.add_control_port(&boundary_id, "protected_completed", PortDirection::Input)?;
        let handler_completion_port =
            self.add_control_port(&boundary_id, "handler_completed", PortDirection::Input)?;
        let finalizer_completion_port = finalizer_scope
            .as_ref()
            .map(|_| {
                self.add_control_port(&boundary_id, "finalizer_completed", PortDirection::Input)
            })
            .transpose()?;
        let completed_port =
            self.add_control_port(&boundary_id, "completed", PortDirection::Output)?;
        let error_type = safe_error_type()?;
        let error_port = self.add_data_port(
            &boundary_id,
            "safe_business_failure",
            PortDirection::Output,
            error_type.clone(),
            false,
        )?;
        self.add_node(
            boundary_id.clone(),
            scope.clone(),
            NodeKind::ErrorBoundary(ErrorBoundaryDescriptor {
                protected_scope_id: protected_scope.clone(),
                handler_scope_id: handler_scope.clone(),
                finalizer_scope_id: finalizer_scope.clone(),
                catch_kind: CatchFailureKind::SafeBusinessFailure,
                protected_output: protected_output.clone(),
                handler_output: handler_output.clone(),
                finalizer_output: finalizer_output.clone(),
                protected_completed_input: Some(protected_completion_port.clone()),
                handler_completed_input: Some(handler_completion_port.clone()),
                finalizer_completed_input: finalizer_completion_port.clone(),
                completed_output: Some(completed_port.clone()),
                error_port: error_port.clone(),
            }),
        )?;
        self.connect_incoming(incoming, &boundary_id)?;
        self.scopes.insert(
            protected_scope.clone(),
            PendingScope {
                id: protected_scope.clone(),
                parent: Some(scope.clone()),
                owner: Some(boundary_id.clone()),
                kind: ScopeKind::ErrorProtected {
                    boundary_node_id: boundary_id.clone(),
                },
                captures: BTreeSet::new(),
            },
        );
        self.scopes.insert(
            handler_scope.clone(),
            PendingScope {
                id: handler_scope.clone(),
                parent: Some(scope.clone()),
                owner: Some(boundary_id.clone()),
                kind: ScopeKind::ErrorHandler {
                    boundary_node_id: boundary_id.clone(),
                },
                captures: BTreeSet::from([error_port.clone()]),
            },
        );
        if let Some(finalizer_scope) = &finalizer_scope {
            self.scopes.insert(
                finalizer_scope.clone(),
                PendingScope {
                    id: finalizer_scope.clone(),
                    parent: Some(scope.clone()),
                    owner: Some(boundary_id.clone()),
                    kind: ScopeKind::ErrorFinalizer {
                        boundary_node_id: boundary_id.clone(),
                    },
                    captures: BTreeSet::new(),
                },
            );
        }

        let mut protected_environment = environment.clone();
        let protected_exit = self.compile_block(
            &step.protected_steps,
            Some(ControlPoint(protected_output.clone())),
            &protected_scope,
            &mut protected_environment,
            &format!("{stable_context}.{}.try", step.id),
            BlockContext::IfArm,
        )?;
        let (protected_completed_input, protected_normal, protected_abrupt) = match protected_exit {
            BlockExit::Continue(Some(control)) => {
                self.add_control_edge(
                    control.0,
                    protected_completion_port.clone(),
                    &format!("try:{}:protected_completed", step.id),
                )?;
                (
                    Some(protected_completion_port.clone()),
                    true,
                    BTreeSet::new(),
                )
            }
            BlockExit::Abrupt(exits) => (None, false, exits),
            BlockExit::Continue(None) => {
                return Err(CompileError::new(
                    INVALID_CONTROL_FLOW,
                    "try protected block has no correlated control completion",
                ));
            }
            BlockExit::Yield { .. }
            | BlockExit::LoopContinue { .. }
            | BlockExit::LoopBreak { .. } => {
                return Err(CompileError::new(
                    INVALID_CONTROL_FLOW,
                    "yield/break/continue cannot cross a try protected boundary",
                ));
            }
        };

        let mut handler_environment = environment.clone();
        handler_environment.insert(
            step.error_name.clone(),
            Symbol {
                source: ValueSource::Port {
                    port_id: error_port.clone(),
                },
                value_type: error_type,
                producer_port: Some(error_port.clone()),
            },
        );
        let handler_exit = self.compile_block(
            &step.handler_steps,
            Some(ControlPoint(handler_output.clone())),
            &handler_scope,
            &mut handler_environment,
            &format!("{stable_context}.{}.catch", step.id),
            BlockContext::IfArm,
        )?;
        let (handler_completed_input, handler_normal, handler_abrupt) = match handler_exit {
            BlockExit::Continue(Some(control)) => {
                self.add_control_edge(
                    control.0,
                    handler_completion_port.clone(),
                    &format!("try:{}:handler_completed", step.id),
                )?;
                (Some(handler_completion_port.clone()), true, BTreeSet::new())
            }
            BlockExit::Abrupt(exits) => (None, false, exits),
            BlockExit::Continue(None) => {
                return Err(CompileError::new(
                    INVALID_CONTROL_FLOW,
                    "safe_business_failure handler has no correlated control completion",
                ));
            }
            BlockExit::Yield { .. }
            | BlockExit::LoopContinue { .. }
            | BlockExit::LoopBreak { .. } => {
                return Err(CompileError::new(
                    INVALID_CONTROL_FLOW,
                    "yield/break/continue cannot cross a catch boundary",
                ));
            }
        };

        let (finalizer_completed_input, finalizer_normal, finalizer_abrupt) =
            if let Some(finalizer_scope) = &finalizer_scope {
                let output = finalizer_output
                    .clone()
                    .expect("finalizer scope owns an output");
                let completion = finalizer_completion_port
                    .clone()
                    .expect("finalizer scope owns an allocated completion port");
                let mut finalizer_environment = environment.clone();
                let finally_exit = self.compile_block(
                    &step.finally_steps,
                    Some(ControlPoint(output)),
                    finalizer_scope,
                    &mut finalizer_environment,
                    &format!("{stable_context}.{}.finally", step.id),
                    BlockContext::IfArm,
                )?;
                match finally_exit {
                    BlockExit::Continue(Some(control)) => {
                        self.add_control_edge(
                            control.0,
                            completion.clone(),
                            &format!("try:{}:finalizer_completed", step.id),
                        )?;
                        (Some(completion), true, BTreeSet::new())
                    }
                    BlockExit::Abrupt(exits) => (None, false, exits),
                    BlockExit::Continue(None) => {
                        return Err(CompileError::new(
                            INVALID_CONTROL_FLOW,
                            "finally has no correlated control completion",
                        ));
                    }
                    BlockExit::Yield { .. }
                    | BlockExit::LoopContinue { .. }
                    | BlockExit::LoopBreak { .. } => {
                        return Err(CompileError::new(
                            INVALID_CONTROL_FLOW,
                            "yield/break/continue cannot cross a finally boundary",
                        ));
                    }
                }
            } else {
                (None, true, BTreeSet::new())
            };

        let pre_finalizer_normal = protected_normal || handler_normal;
        let boundary_normal = pre_finalizer_normal && finalizer_normal;
        let completed_output = boundary_normal.then_some(completed_port.clone());

        let mut unused_ports = BTreeSet::new();
        if protected_completed_input.is_none() {
            unused_ports.insert(protected_completion_port);
        }
        if handler_completed_input.is_none() {
            unused_ports.insert(handler_completion_port);
        }
        if finalizer_scope.is_some() && finalizer_completed_input.is_none() {
            unused_ports.insert(
                finalizer_completion_port
                    .clone()
                    .expect("finalizer completion port was allocated"),
            );
        }
        if completed_output.is_none() {
            unused_ports.insert(completed_port);
        }
        self.control_ports
            .retain(|port| !unused_ports.contains(port.id()));

        let boundary_position = self
            .nodes
            .iter()
            .position(|node| node.id() == &boundary_id)
            .expect("ErrorBoundary was installed before its child blocks");
        self.nodes[boundary_position] = Node::new(
            boundary_id,
            scope.clone(),
            NodeKind::ErrorBoundary(ErrorBoundaryDescriptor {
                protected_scope_id: protected_scope,
                handler_scope_id: handler_scope,
                finalizer_scope_id: finalizer_scope,
                catch_kind: CatchFailureKind::SafeBusinessFailure,
                protected_output,
                handler_output,
                finalizer_output,
                protected_completed_input,
                handler_completed_input,
                finalizer_completed_input,
                completed_output: completed_output.clone(),
                error_port,
            }),
        );

        if let Some(output) = completed_output {
            return Ok(BlockExit::Continue(Some(ControlPoint(output))));
        }
        if !finalizer_abrupt.is_empty() {
            return Ok(BlockExit::Abrupt(finalizer_abrupt));
        }
        let mut exits = protected_abrupt;
        exits.extend(handler_abrupt);
        if exits.is_empty() {
            return Err(CompileError::new(
                INVALID_CONTROL_FLOW,
                "try/catch/finally has neither a normal nor an authored abrupt exit",
            ));
        }
        Ok(BlockExit::Abrupt(exits))
    }

    fn compile_wait(
        &mut self,
        step: &WaitStep,
        incoming: Option<ControlPoint>,
        scope: &ScopeId,
        environment: &mut Environment,
    ) -> Result<ControlPoint, CompileError> {
        let id = node_id(&step.id)?;
        let kind = match &step.kind {
            WaitKind::Signal { name, payload_type } => NodeKind::WaitSignal(WaitSignalDescriptor {
                signal_name: name.clone(),
                payload_type: lower_author_type(payload_type)?,
            }),
            WaitKind::Timer { duration_ms } => {
                let expression =
                    self.compile_numeric_expression(duration_ms, &id, scope, environment)?;
                NodeKind::Timer(TimerDescriptor {
                    delay_ms: expression,
                })
            }
        };
        self.add_node(id.clone(), scope.clone(), kind)?;
        self.connect_incoming(incoming, &id)?;
        if let WaitKind::Signal { payload_type, .. } = &step.kind {
            let output_type = lower_author_type(payload_type)?;
            let output = self.add_data_port(
                &id,
                "payload",
                PortDirection::Output,
                output_type.clone(),
                false,
            )?;
            environment.insert(
                step.id.clone(),
                Symbol {
                    source: ValueSource::Port {
                        port_id: output.clone(),
                    },
                    value_type: output_type,
                    producer_port: Some(output),
                },
            );
        }
        let output = self.add_control_port(&id, "out", PortDirection::Output)?;
        Ok(ControlPoint(output))
    }

    fn compile_numeric_expression(
        &mut self,
        source: &ValueExpr,
        node: &NodeId,
        scope: &ScopeId,
        environment: &Environment,
    ) -> Result<PureExpression, CompileError> {
        match compile_numeric(source)? {
            RestrictedExpression::Literal { value, value_type } => Ok(PureExpression::new(
                ExpressionLanguage::Literal,
                version(LITERAL_EXPRESSION_ENGINE_VERSION)?,
                canonical_json(&value)?,
                value_type,
            )),
            RestrictedExpression::BareName(name) => {
                let resolved = self.resolve_reference(&name, environment)?;
                if !resolved.value_type.is_assignable_to(&PlanType::Number) {
                    return Err(CompileError::new(
                        INVALID_TYPE,
                        "timer duration reference must be a non-null number",
                    ));
                }
                self.capture_value(scope, &resolved)?;
                let input = self.add_data_port(
                    node,
                    "duration_ms",
                    PortDirection::Input,
                    resolved.value_type.clone(),
                    true,
                )?;
                self.add_data_binding(resolved.source, input.clone(), &format!("timer:{node}"))?;
                Ok(PureExpression::new(
                    ExpressionLanguage::Cel,
                    version(CEL_EXPRESSION_ENGINE_VERSION)?,
                    name.clone(),
                    resolved.value_type,
                )
                .with_dependency(name, input))
            }
        }
    }

    fn compile_array_expression(
        &mut self,
        source: &ValueExpr,
        node: &NodeId,
        scope: &ScopeId,
        environment: &Environment,
        dependency_name: &str,
    ) -> Result<PureExpression, CompileError> {
        let resolved = self.resolve_value_for_node(source, node, scope, environment, false)?;
        let result_type = resolved.value_type.clone();
        if result_type.array_constraints().is_none() {
            return Err(CompileError::new(
                INVALID_TYPE,
                "dynamic collection input must have one concrete array type",
            ));
        }
        match resolved.source {
            ValueSource::Literal { value } => Ok(PureExpression::new(
                ExpressionLanguage::Literal,
                version(LITERAL_EXPRESSION_ENGINE_VERSION)?,
                canonical_json(&value)?,
                result_type,
            )),
            ValueSource::Expression { expression } => Ok(expression),
            source @ (ValueSource::RunInput { .. } | ValueSource::Port { .. }) => {
                self.capture_value(
                    scope,
                    &ResolvedValue {
                        source: source.clone(),
                        value_type: result_type.clone(),
                        producer_port: match &source {
                            ValueSource::Port { port_id } => Some(port_id.clone()),
                            _ => None,
                        },
                    },
                )?;
                let input = self.add_data_port(
                    node,
                    dependency_name,
                    PortDirection::Input,
                    result_type.clone(),
                    true,
                )?;
                self.add_data_binding(
                    source,
                    input.clone(),
                    &format!("array:{node}:{dependency_name}"),
                )?;
                Ok(PureExpression::new(
                    ExpressionLanguage::Cel,
                    version(CEL_EXPRESSION_ENGINE_VERSION)?,
                    dependency_name,
                    result_type,
                )
                .with_dependency(dependency_name, input))
            }
            ValueSource::OptionalRunInput { .. } => Err(CompileError::new(
                INVALID_REFERENCE,
                "optional collection input requires a default before iteration",
            )),
        }
    }

    fn compile_if(
        &mut self,
        step: &IfStep,
        incoming: Option<ControlPoint>,
        scope: &ScopeId,
        environment: &mut Environment,
        stable_context: &str,
    ) -> Result<BlockExit, CompileError> {
        let branch_id = node_id(&step.id)?;
        let mut cases = Vec::new();
        let mut authored_arms: Vec<(String, Option<&str>, &[Step])> = Vec::new();
        authored_arms.push((
            "then".to_owned(),
            Some(step.condition.as_str()),
            &step.then_steps,
        ));
        for arm in &step.elif {
            authored_arms.push((arm.id.clone(), Some(arm.condition.as_str()), &arm.steps));
        }
        authored_arms.push((
            "else".to_owned(),
            None,
            step.else_steps.as_deref().unwrap_or(&[]),
        ));

        for (case, condition, _) in &authored_arms {
            let case_id = branch_case_id(case)?;
            let output = self.add_control_port(&branch_id, case, PortDirection::Output)?;
            let case = if let Some(condition) = condition {
                BranchCase::when(
                    case_id,
                    self.compile_branch_expression(
                        condition,
                        case,
                        &branch_id,
                        scope,
                        environment,
                    )?,
                    output,
                )
            } else {
                BranchCase::otherwise(case_id, output)
            };
            cases.push(case);
        }
        self.add_node(
            branch_id.clone(),
            scope.clone(),
            NodeKind::Branch(BranchDescriptor {
                cases: cases.clone(),
            }),
        )?;
        self.connect_incoming(incoming, &branch_id)?;

        struct ArmResult {
            case_id: BranchCaseId,
            exit: BlockExit,
        }
        let mut results = Vec::new();
        for ((case, _, steps), descriptor) in authored_arms.iter().zip(&cases) {
            let arm_scope = if has_executable_node(steps) {
                let id = scope_id(&stable_id("scope", &format!("branch:{}:{case}", step.id)))?;
                self.scopes.insert(
                    id.clone(),
                    PendingScope {
                        id: id.clone(),
                        parent: Some(scope.clone()),
                        owner: Some(branch_id.clone()),
                        kind: ScopeKind::BranchArm {
                            branch_node_id: branch_id.clone(),
                            case_id: descriptor.case_id.clone(),
                        },
                        captures: BTreeSet::new(),
                    },
                );
                id
            } else {
                scope.clone()
            };
            let mut arm_environment = environment.clone();
            let context = format!("{stable_context}.{}.{}", step.id, case);
            let exit = self.compile_block(
                steps,
                Some(ControlPoint(descriptor.output_port.clone())),
                &arm_scope,
                &mut arm_environment,
                &context,
                BlockContext::IfArm,
            )?;
            results.push(ArmResult {
                case_id: descriptor.case_id.clone(),
                exit,
            });
        }

        let has_yield = results
            .iter()
            .any(|value| matches!(value.exit, BlockExit::Yield { .. }));
        if has_yield && step.else_steps.is_none() {
            return Err(CompileError::new(
                INVALID_CONTROL_FLOW,
                "a value-producing if must declare else",
            ));
        }
        if has_yield
            && results
                .iter()
                .any(|value| matches!(value.exit, BlockExit::Continue(_)))
        {
            return Err(CompileError::new(
                INVALID_CONTROL_FLOW,
                "every non-terminating path of a value-producing if must end in yield",
            ));
        }

        let mut abrupt_exits = BTreeSet::new();
        let continuing = results
            .into_iter()
            .filter_map(|value| match &value.exit {
                BlockExit::Abrupt(exits) => {
                    abrupt_exits.extend(exits.iter().copied());
                    None
                }
                _ => Some(value),
            })
            .collect::<Vec<_>>();
        if continuing.is_empty() {
            return Ok(BlockExit::Abrupt(abrupt_exits));
        }
        let merge_id = self.generated_node(&branch_id, "merge", None)?;
        let mut arms = BTreeMap::new();
        let mut phi_sources = BTreeMap::new();
        let mut yield_types = Vec::new();
        for arm in continuing {
            let input =
                self.add_control_port(&merge_id, arm.case_id.as_str(), PortDirection::Input)?;
            let (control, yielded) = match arm.exit {
                BlockExit::Continue(control) => (control, None),
                BlockExit::Yield {
                    control,
                    value,
                    environment,
                } => (
                    control,
                    Some(self.resolve_value_for_node(
                        &value,
                        &merge_id,
                        scope,
                        &environment,
                        true,
                    )?),
                ),
                BlockExit::LoopContinue { .. } | BlockExit::LoopBreak { .. } => {
                    return Err(CompileError::new(
                        INVALID_CONTROL_FLOW,
                        "loop control cannot cross an if arm in this structured profile",
                    ));
                }
                BlockExit::Abrupt(_) => unreachable!("abrupt arms were filtered"),
            };
            let control = control.ok_or_else(|| {
                CompileError::new(
                    INVALID_CONTROL_FLOW,
                    "an empty root arm cannot reach its correlated merge",
                )
            })?;
            self.add_control_edge(
                control.0,
                input.clone(),
                &format!("if:{}:{}", step.id, arm.case_id),
            )?;
            arms.insert(arm.case_id.clone(), input);
            if let Some(value) = yielded {
                yield_types.push(value.value_type.clone());
                phi_sources.insert(arm.case_id, value.source);
            }
        }
        let output = self.add_control_port(&merge_id, "out", PortDirection::Output)?;
        self.add_node(
            merge_id.clone(),
            scope.clone(),
            NodeKind::Merge(MergeDescriptor {
                branch_node_id: branch_id,
                arms,
                output_port: output.clone(),
            }),
        )?;
        if has_yield {
            let value_type = PlanType::unify(yield_types)
                .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))?;
            let phi_output = self.add_data_port(
                &merge_id,
                "value",
                PortDirection::Output,
                value_type.clone(),
                false,
            )?;
            let phi_id = phi_id(&format!("if:{}:phi", step.id))?;
            self.phi_bindings.push(PhiBinding::new(
                phi_id,
                merge_id,
                phi_output.clone(),
                phi_sources,
            ));
            environment.insert(
                step.id.clone(),
                Symbol {
                    source: ValueSource::Port {
                        port_id: phi_output.clone(),
                    },
                    value_type,
                    producer_port: Some(phi_output),
                },
            );
        }
        Ok(BlockExit::Continue(Some(ControlPoint(output))))
    }

    fn compile_branch_expression(
        &mut self,
        source: &str,
        condition_context: &str,
        branch: &NodeId,
        scope: &ScopeId,
        environment: &Environment,
    ) -> Result<PureExpression, CompileError> {
        let compiled = compile_condition(source)?;
        let mut expression = PureExpression::new(
            ExpressionLanguage::Cel,
            version(CEL_EXPRESSION_ENGINE_VERSION)?,
            compiled.source.clone(),
            PlanType::Boolean,
        );
        let mut dependency_types = BTreeMap::new();
        for (index, name) in compiled.dependencies.iter().enumerate() {
            let resolved = self.resolve_reference(name, environment)?;
            self.capture_value(scope, &resolved)?;
            let port = self.add_data_port(
                branch,
                &format!("condition_{condition_context}_{index}"),
                PortDirection::Input,
                resolved.value_type.clone(),
                true,
            )?;
            self.add_data_binding(
                resolved.source,
                port.clone(),
                &format!("condition:{branch}:{condition_context}:{index}:{name}"),
            )?;
            dependency_types.insert(name.clone(), resolved.value_type);
            expression = expression.with_dependency(name.clone(), port);
        }
        let analysis =
            analyze_cel_expression(&compiled.source, &dependency_types).map_err(|error| {
                CompileError::new(
                    INVALID_TYPE,
                    format!("if/elif condition is outside the fixed typed CEL profile: {error}"),
                )
            })?;
        if !analysis.result_type.is_assignable_to(&PlanType::Boolean) {
            return Err(CompileError::new(
                INVALID_TYPE,
                "if/elif condition must have a statically non-null Boolean result",
            ));
        }
        if analysis.references.is_empty() {
            let program = cel::Program::compile(&compiled.source)
                .expect("condition parser and typed analysis already accepted this source");
            let value = program.execute(&cel::Context::default()).map_err(|error| {
                CompileError::new(
                    INVALID_TYPE,
                    format!("constant CEL condition cannot be evaluated: {error}"),
                )
            })?;
            let cel::Value::Bool(value) = value else {
                return Err(CompileError::new(
                    INVALID_TYPE,
                    "constant if/elif condition must evaluate to Boolean",
                ));
            };
            expression.source = value.to_string();
        }
        Ok(expression)
    }

    fn compile_parallel(
        &mut self,
        step: &ParallelStep,
        incoming: Option<ControlPoint>,
        scope: &ScopeId,
        environment: &mut Environment,
        stable_context: &str,
    ) -> Result<ControlPoint, CompileError> {
        let fork_id = node_id(&step.id)?;
        let join_id = self.generated_node(&fork_id, "join", None)?;
        let collect_id = self.generated_node(&fork_id, "collect", None)?;
        // Leg bodies are compiled before the descriptor can be finalized, but
        // the Fork remains the control entry/owner, never the first leg leaf.
        if self.entry.is_none() {
            self.entry = Some(fork_id.clone());
        }
        let mode = match step.settle {
            ParallelSettle::AllSuccess => PlanJoinMode::AllSuccess,
            ParallelSettle::AllSettled => PlanJoinMode::AllSettled,
        };
        let mut fork_legs = Vec::new();
        let mut join_legs = BTreeMap::new();
        let mut leg_types = Vec::new();
        let mut compiled_legs = Vec::new();
        for leg in &step.legs {
            let leg_id = LegId::new(leg.id.clone()).map_err(model_error)?;
            let output = self.add_control_port(&fork_id, &leg.id, PortDirection::Output)?;
            let input = self.add_control_port(&join_id, &leg.id, PortDirection::Input)?;
            let leg_scope = scope_id(&stable_id(
                "scope",
                &format!("parallel:{}:{}", step.id, leg.id),
            ))?;
            self.scopes.insert(
                leg_scope.clone(),
                PendingScope {
                    id: leg_scope.clone(),
                    parent: Some(scope.clone()),
                    owner: Some(fork_id.clone()),
                    kind: ScopeKind::ForkLeg {
                        fork_node_id: fork_id.clone(),
                        leg_id: leg_id.clone(),
                    },
                    captures: BTreeSet::new(),
                },
            );
            compiled_legs.push((leg, leg_id, output, input, leg_scope));
        }
        // The Fork descriptor needs yield ports, so compile leg bodies before
        // materializing the Fork node itself. Graph ordering is non-semantic.
        for (leg, leg_id, output, input, leg_scope) in compiled_legs {
            let mut leg_environment = environment.clone();
            let context = format!("{stable_context}.{}.{}", step.id, leg.id);
            let exit = self.compile_block(
                &leg.steps,
                Some(ControlPoint(output.clone())),
                &leg_scope,
                &mut leg_environment,
                &context,
                BlockContext::ParallelLeg,
            )?;
            let BlockExit::Yield {
                control,
                value,
                environment: yield_environment,
            } = exit
            else {
                return Err(CompileError::new(
                    INVALID_CONTROL_FLOW,
                    "every normal parallel leg path must end with one typed yield",
                ));
            };
            let control = control.ok_or_else(|| {
                CompileError::new(
                    INVALID_CONTROL_FLOW,
                    "parallel yield must follow a value-producing node in its leg",
                )
            })?;
            let value = self.resolve_value(&value, &yield_environment)?;
            let yield_port = value.producer_port.ok_or_else(|| {
                CompileError::new(
                    INVALID_CONTROL_FLOW,
                    "parallel yield must publish an output produced inside the leg",
                )
            })?;
            let producer_scope = self
                .data_ports
                .iter()
                .find(|port| port.id() == &yield_port)
                .and_then(|port| {
                    self.nodes
                        .iter()
                        .find(|node| node.id() == port.owner())
                        .map(|node| node.scope_id().clone())
                });
            if producer_scope.as_ref() != Some(&leg_scope) {
                return Err(CompileError::new(
                    INVALID_CONTROL_FLOW,
                    "parallel yield cannot publish a captured value from outside its leg",
                ));
            }
            self.add_control_edge(
                control.0,
                input.clone(),
                &format!("parallel:{}:{}", step.id, leg.id),
            )?;
            leg_types.push((leg.id.clone(), value.value_type));
            fork_legs.push(ForkLegDescriptor {
                leg_id: leg_id.clone(),
                scope_id: leg_scope,
                output_port: output,
                yield_port,
            });
            join_legs.insert(leg_id, input);
        }
        self.add_node(
            fork_id.clone(),
            scope.clone(),
            NodeKind::Fork(ForkDescriptor {
                legs: fork_legs,
                join_mode: mode,
            }),
        )?;
        self.connect_incoming(incoming, &fork_id)?;
        let join_output = self.add_control_port(&join_id, "out", PortDirection::Output)?;
        self.add_node(
            join_id.clone(),
            scope.clone(),
            NodeKind::Join(JoinDescriptor {
                fork_node_id: fork_id.clone(),
                mode,
                legs: join_legs,
                output_port: join_output.clone(),
            }),
        )?;
        let collect_input = self.add_control_port(&collect_id, "in", PortDirection::Input)?;
        self.add_control_edge(
            join_output,
            collect_input,
            &format!("parallel:{}:join_collect", step.id),
        )?;
        let collect_output = self.add_control_port(&collect_id, "out", PortDirection::Output)?;
        let result_type = parallel_result_type(&leg_types, mode, &safe_error_type()?)?;
        let result_port = self.add_data_port(
            &collect_id,
            "result",
            PortDirection::Output,
            result_type.clone(),
            false,
        )?;
        self.add_node(
            collect_id.clone(),
            scope.clone(),
            NodeKind::Collect(CollectDescriptor {
                source: CollectSource::StaticFork {
                    fork_node_id: fork_id,
                    join_node_id: join_id,
                    mode,
                },
                output_port: result_port.clone(),
            }),
        )?;
        environment.insert(
            step.id.clone(),
            Symbol {
                source: ValueSource::Port {
                    port_id: result_port.clone(),
                },
                value_type: result_type,
                producer_port: Some(result_port),
            },
        );
        Ok(ControlPoint(collect_output))
    }

    fn compile_return(
        &mut self,
        value: &ValueExpr,
        incoming: Option<ControlPoint>,
        scope: &ScopeId,
        environment: &Environment,
        stable_context: &str,
    ) -> Result<(), CompileError> {
        let expected = lower_author_type(&self.document.output_type)?;
        let parent = stable_parent(stable_context)?;
        let id = self.generated_node(&parent, "return", Some(stable_context))?;
        let value = self.resolve_value_for_node(value, &id, scope, environment, false)?;
        if !value.value_type.is_assignable_to(&expected) {
            return Err(CompileError::new(
                INVALID_TYPE,
                "return value does not satisfy workflow output",
            ));
        }
        self.capture_value(scope, &value)?;
        let input = self.add_data_port(&id, "value", PortDirection::Input, expected, true)?;
        self.add_node(
            id.clone(),
            scope.clone(),
            NodeKind::Return(ReturnDescriptor {
                value_input: input.clone(),
            }),
        )?;
        self.connect_incoming(incoming, &id)?;
        self.add_data_binding(value.source, input, &format!("return:{stable_context}"))?;
        Ok(())
    }

    fn compile_raise(
        &mut self,
        value: &ValueExpr,
        incoming: Option<ControlPoint>,
        scope: &ScopeId,
        environment: &Environment,
        stable_context: &str,
    ) -> Result<(), CompileError> {
        let parent = stable_parent(stable_context)?;
        let id = self.generated_node(&parent, "raise", Some(stable_context))?;
        let resolved = if let ValueExpr::ErrorRef(error_id) = value {
            if let Some(error) = self.document.errors.get(error_id) {
                let literal = serde_json::json!({
                    "kind": "safe_error",
                    "code": error.code,
                    "message": error.public_message,
                });
                ResolvedValue {
                    value_type: literal_type(&literal)?,
                    source: ValueSource::Literal { value: literal },
                    producer_port: None,
                }
            } else {
                return Err(CompileError::new(
                    INVALID_REFERENCE,
                    "raise references an undeclared error",
                ));
            }
        } else {
            self.resolve_value_for_node(value, &id, scope, environment, false)?
        };
        let error_type = safe_error_type()?;
        if !resolved.value_type.is_assignable_to(&error_type) {
            return Err(CompileError::new(
                INVALID_TYPE,
                "raise must name a declared error or produce the safe error contract",
            ));
        }
        self.capture_value(scope, &resolved)?;
        let input = self.add_data_port(&id, "error", PortDirection::Input, error_type, true)?;
        self.add_node(
            id.clone(),
            scope.clone(),
            NodeKind::Raise(RaiseDescriptor {
                error_input: input.clone(),
            }),
        )?;
        self.connect_incoming(incoming, &id)?;
        self.add_data_binding(resolved.source, input, &format!("raise:{stable_context}"))?;
        Ok(())
    }

    fn resolve_value_for_node(
        &mut self,
        value: &ValueExpr,
        evaluating_node: &NodeId,
        scope: &ScopeId,
        environment: &Environment,
        arm_correlated: bool,
    ) -> Result<ResolvedValue, CompileError> {
        let value = fold_static_match(value)?;
        let resolved = match &value {
            ValueExpr::Match(value) => self.compile_match_expression(
                value,
                evaluating_node,
                scope,
                environment,
                arm_correlated,
            ),
            ValueExpr::Reference(reference) if !reference.segments.is_empty() => self
                .compile_value_expression(
                    &value,
                    evaluating_node,
                    scope,
                    environment,
                    arm_correlated,
                ),
            ValueExpr::Array(_) | ValueExpr::Object(_) | ValueExpr::Template(_) => {
                match self.resolve_value(&value, environment) {
                    Ok(value) => Ok(value),
                    Err(error) if error.code() == EXPRESSION_ENGINE_BLOCKED => self
                        .compile_value_expression(
                            &value,
                            evaluating_node,
                            scope,
                            environment,
                            arm_correlated,
                        ),
                    Err(error) => Err(error),
                }
            }
            _ => self.resolve_value(&value, environment),
        }?;
        if matches!(&resolved.source, ValueSource::OptionalRunInput { .. }) {
            return Err(CompileError::new(
                INVALID_REFERENCE,
                "optional input is used where absence is not supported; provide a default first",
            ));
        }
        Ok(resolved)
    }

    fn compile_value_expression(
        &mut self,
        value: &ValueExpr,
        evaluating_node: &NodeId,
        scope: &ScopeId,
        environment: &Environment,
        arm_correlated: bool,
    ) -> Result<ResolvedValue, CompileError> {
        let expression_sequence = self.pure_expression_sequence;
        self.pure_expression_sequence += 1;
        let mut dependency_sequence = 0_usize;
        let mut dependency_types = BTreeMap::new();
        let mut dependency_ports = BTreeMap::new();
        let program = self.compile_value_program(
            value,
            evaluating_node,
            scope,
            environment,
            arm_correlated,
            expression_sequence,
            &mut dependency_sequence,
            &mut dependency_types,
            &mut dependency_ports,
        )?;
        let value_type = analyze_value_program(&program, &dependency_types).map_err(|error| {
            CompileError::new(
                INVALID_TYPE,
                format!("natural value is outside the fixed typed profile: {error}"),
            )
        })?;
        let source = serde_jcs::to_string(&program).map_err(|error| {
            CompileError::new(
                INVALID_TYPE,
                format!("natural value cannot be canonicalized: {error}"),
            )
        })?;
        let mut expression = PureExpression::new(
            ExpressionLanguage::Value,
            version(VALUE_EXPRESSION_ENGINE_VERSION)?,
            source,
            value_type.clone(),
        );
        for (name, port) in dependency_ports {
            expression = expression.with_dependency(name, port);
        }
        Ok(ResolvedValue {
            source: ValueSource::Expression { expression },
            value_type,
            producer_port: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn compile_value_program(
        &mut self,
        value: &ValueExpr,
        evaluating_node: &NodeId,
        scope: &ScopeId,
        environment: &Environment,
        arm_correlated: bool,
        expression_sequence: usize,
        dependency_sequence: &mut usize,
        dependency_types: &mut BTreeMap<String, PlanType>,
        dependency_ports: &mut BTreeMap<String, DataPortId>,
    ) -> Result<ValueProgram, CompileError> {
        match fold_static_match(value)? {
            ValueExpr::Reference(reference) => {
                let symbol = environment.get(&reference.root).ok_or_else(|| {
                    CompileError::new(
                        INVALID_REFERENCE,
                        format!(
                            "reference '${}' is not visible in this lexical scope",
                            reference.source()
                        ),
                    )
                })?;
                let resolved = ResolvedValue {
                    source: symbol.source.clone(),
                    value_type: symbol.value_type.clone(),
                    producer_port: symbol.producer_port.clone(),
                };
                self.capture_expression_dependency(scope, &resolved, arm_correlated)?;
                let index = *dependency_sequence;
                *dependency_sequence += 1;
                let name = format!("d{index}");
                let port = match &resolved.source {
                    ValueSource::Port { port_id } => port_id.clone(),
                    ValueSource::RunInput { .. } | ValueSource::Literal { .. } => {
                        let input = self.add_data_port(
                            evaluating_node,
                            &format!("value_{expression_sequence}_{index}"),
                            PortDirection::Input,
                            resolved.value_type.clone(),
                            true,
                        )?;
                        self.add_data_binding(
                            resolved.source,
                            input.clone(),
                            &format!("value:{evaluating_node}:{expression_sequence}:{index}"),
                        )?;
                        input
                    }
                    ValueSource::OptionalRunInput { .. } => {
                        return Err(CompileError::new(
                            INVALID_REFERENCE,
                            "optional input cannot be used inside a value expression without a default",
                        ));
                    }
                    ValueSource::Expression { .. } => {
                        return Err(CompileError::new(
                            EXPRESSION_ENGINE_BLOCKED,
                            "natural value dependencies must name a lexical port or RunInput",
                        ));
                    }
                };
                dependency_types.insert(name.clone(), resolved.value_type);
                dependency_ports.insert(name.clone(), port);
                Ok(ValueProgram::Dependency {
                    name,
                    path: reference.segments,
                })
            }
            ValueExpr::Literal(value) => Ok(ValueProgram::Literal { value }),
            ValueExpr::Array(values) => Ok(ValueProgram::Array {
                items: values
                    .iter()
                    .map(|value| {
                        self.compile_value_program(
                            value,
                            evaluating_node,
                            scope,
                            environment,
                            arm_correlated,
                            expression_sequence,
                            dependency_sequence,
                            dependency_types,
                            dependency_ports,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            ValueExpr::Object(values) => Ok(ValueProgram::Object {
                fields: values
                    .iter()
                    .map(|(key, value)| {
                        Ok((
                            key.clone(),
                            self.compile_value_program(
                                value,
                                evaluating_node,
                                scope,
                                environment,
                                arm_correlated,
                                expression_sequence,
                                dependency_sequence,
                                dependency_types,
                                dependency_ports,
                            )?,
                        ))
                    })
                    .collect::<Result<BTreeMap<_, _>, CompileError>>()?,
            }),
            ValueExpr::Template(template) => {
                let mut parts = Vec::new();
                let mut cursor = 0_usize;
                for reference in &template.references {
                    let open = template.source[cursor..]
                        .find("{{")
                        .map(|offset| cursor + offset)
                        .expect("validated template reference has an opening delimiter");
                    let close = template.source[open + 2..]
                        .find("}}")
                        .map(|offset| open + 2 + offset)
                        .expect("validated template reference has a closing delimiter");
                    if cursor < open {
                        parts.push(TemplatePart::Text {
                            text: template.source[cursor..open].to_owned(),
                        });
                    }
                    parts.push(TemplatePart::Value {
                        value: Box::new(self.compile_value_program(
                            &ValueExpr::Reference(reference.clone()),
                            evaluating_node,
                            scope,
                            environment,
                            arm_correlated,
                            expression_sequence,
                            dependency_sequence,
                            dependency_types,
                            dependency_ports,
                        )?),
                    });
                    cursor = close + 2;
                }
                if cursor < template.source.len() {
                    parts.push(TemplatePart::Text {
                        text: template.source[cursor..].to_owned(),
                    });
                }
                Ok(ValueProgram::Template { parts })
            }
            ValueExpr::Match(_) => Err(CompileError::new(
                EXPRESSION_ENGINE_BLOCKED,
                "match must be the enclosing pure value expression",
            )),
            ValueExpr::ErrorRef(_) => Err(CompileError::new(
                INVALID_REFERENCE,
                "declared errors are only valid in raise",
            )),
        }
    }

    fn compile_match_expression(
        &mut self,
        value: &super::ast::MatchExpr,
        evaluating_node: &NodeId,
        scope: &ScopeId,
        environment: &Environment,
        arm_correlated: bool,
    ) -> Result<ResolvedValue, CompileError> {
        let expression_sequence = self.pure_expression_sequence;
        self.pure_expression_sequence += 1;
        let mut dependency_sequence = 0_usize;
        let mut dependency_types = BTreeMap::new();
        let mut dependency_ports = BTreeMap::new();
        let selector = self.compile_match_value(
            &value.selector,
            evaluating_node,
            scope,
            environment,
            arm_correlated,
            expression_sequence,
            &mut dependency_sequence,
            &mut dependency_types,
            &mut dependency_ports,
        )?;
        let cases = value
            .cases
            .iter()
            .map(|(case, value)| {
                Ok((
                    case.clone(),
                    self.compile_match_value(
                        value,
                        evaluating_node,
                        scope,
                        environment,
                        arm_correlated,
                        expression_sequence,
                        &mut dependency_sequence,
                        &mut dependency_types,
                        &mut dependency_ports,
                    )?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, CompileError>>()?;
        let default = self.compile_match_value(
            &value.default,
            evaluating_node,
            scope,
            environment,
            arm_correlated,
            expression_sequence,
            &mut dependency_sequence,
            &mut dependency_types,
            &mut dependency_ports,
        )?;
        let program = MatchProgram {
            selector,
            cases,
            default,
        };
        let value_type = analyze_match_program(&program, &dependency_types).map_err(|error| {
            CompileError::new(
                INVALID_TYPE,
                format!("match is outside the fixed typed value profile: {error}"),
            )
        })?;
        let source = serde_jcs::to_string(&program).map_err(|error| {
            CompileError::new(
                INVALID_TYPE,
                format!("match cannot be canonicalized: {error}"),
            )
        })?;
        let mut expression = PureExpression::new(
            ExpressionLanguage::Match,
            version(MATCH_EXPRESSION_ENGINE_VERSION)?,
            source,
            value_type.clone(),
        );
        for (name, port) in dependency_ports {
            expression = expression.with_dependency(name, port);
        }
        Ok(ResolvedValue {
            source: ValueSource::Expression { expression },
            value_type,
            producer_port: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn compile_match_value(
        &mut self,
        value: &ValueExpr,
        evaluating_node: &NodeId,
        scope: &ScopeId,
        environment: &Environment,
        arm_correlated: bool,
        expression_sequence: usize,
        dependency_sequence: &mut usize,
        dependency_types: &mut BTreeMap<String, PlanType>,
        dependency_ports: &mut BTreeMap<String, DataPortId>,
    ) -> Result<MatchValue, CompileError> {
        match fold_static_match(value)? {
            ValueExpr::Reference(reference) => {
                let resolved = self.resolve_reference(&reference.source(), environment)?;
                self.capture_expression_dependency(scope, &resolved, arm_correlated)?;
                let index = *dependency_sequence;
                *dependency_sequence += 1;
                let name = format!("d{index}");
                let port = match &resolved.source {
                    ValueSource::Port { port_id } => port_id.clone(),
                    ValueSource::RunInput { .. } | ValueSource::Literal { .. } => {
                        let input = self.add_data_port(
                            evaluating_node,
                            &format!("match_{expression_sequence}_{index}"),
                            PortDirection::Input,
                            resolved.value_type.clone(),
                            true,
                        )?;
                        self.add_data_binding(
                            resolved.source,
                            input.clone(),
                            &format!("match:{evaluating_node}:{expression_sequence}:{index}"),
                        )?;
                        input
                    }
                    ValueSource::OptionalRunInput { .. } => {
                        return Err(CompileError::new(
                            INVALID_REFERENCE,
                            "optional input cannot be used inside match without a default",
                        ));
                    }
                    ValueSource::Expression { .. } => {
                        return Err(CompileError::new(
                            EXPRESSION_ENGINE_BLOCKED,
                            "match dependency composition requires an explicitly nested match value",
                        ));
                    }
                };
                dependency_types.insert(name.clone(), resolved.value_type);
                dependency_ports.insert(name.clone(), port);
                Ok(MatchValue::Dependency { name })
            }
            ValueExpr::Literal(value) => Ok(MatchValue::Literal { value }),
            ValueExpr::Array(values) => Ok(MatchValue::Literal {
                value: Value::Array(
                    values
                        .iter()
                        .map(static_literal_value)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            }),
            ValueExpr::Object(values) => Ok(MatchValue::Literal {
                value: Value::Object(
                    values
                        .iter()
                        .map(|(key, value)| Ok((key.clone(), static_literal_value(value)?)))
                        .collect::<Result<Map<_, _>, CompileError>>()?,
                ),
            }),
            ValueExpr::Match(value) => {
                let selector = self.compile_match_value(
                    &value.selector,
                    evaluating_node,
                    scope,
                    environment,
                    arm_correlated,
                    expression_sequence,
                    dependency_sequence,
                    dependency_types,
                    dependency_ports,
                )?;
                let cases = value
                    .cases
                    .iter()
                    .map(|(case, value)| {
                        Ok((
                            case.clone(),
                            self.compile_match_value(
                                value,
                                evaluating_node,
                                scope,
                                environment,
                                arm_correlated,
                                expression_sequence,
                                dependency_sequence,
                                dependency_types,
                                dependency_ports,
                            )?,
                        ))
                    })
                    .collect::<Result<BTreeMap<_, _>, CompileError>>()?;
                let default = self.compile_match_value(
                    &value.default,
                    evaluating_node,
                    scope,
                    environment,
                    arm_correlated,
                    expression_sequence,
                    dependency_sequence,
                    dependency_types,
                    dependency_ports,
                )?;
                Ok(MatchValue::Match {
                    selector: Box::new(selector),
                    cases,
                    default: Box::new(default),
                })
            }
            ValueExpr::Template(_) => Err(CompileError::new(
                EXPRESSION_ENGINE_BLOCKED,
                "template values require the separately versioned Template expression engine",
            )),
            ValueExpr::ErrorRef(_) => Err(CompileError::new(
                INVALID_REFERENCE,
                "declared errors are only valid in raise",
            )),
        }
    }

    fn capture_expression_dependency(
        &mut self,
        target_scope: &ScopeId,
        value: &ResolvedValue,
        arm_correlated: bool,
    ) -> Result<(), CompileError> {
        let Some(port) = &value.producer_port else {
            return Ok(());
        };
        let source_scope = self
            .data_ports
            .iter()
            .find(|candidate| candidate.id() == port)
            .and_then(|candidate| {
                self.nodes
                    .iter()
                    .find(|node| node.id() == candidate.owner())
                    .map(|node| node.scope_id().clone())
            })
            .ok_or_else(|| {
                CompileError::new(INVALID_REFERENCE, "match dependency port is missing")
            })?;
        if arm_correlated && self.scope_is_ancestor(target_scope, &source_scope) {
            return Ok(());
        }
        self.capture_value(target_scope, value)
    }

    fn scope_is_ancestor(&self, ancestor: &ScopeId, descendant: &ScopeId) -> bool {
        let mut cursor = Some(descendant.clone());
        while let Some(scope_id) = cursor {
            if &scope_id == ancestor {
                return true;
            }
            cursor = self
                .scopes
                .get(&scope_id)
                .and_then(|scope| scope.parent.clone());
        }
        false
    }

    fn resolve_value(
        &self,
        value: &ValueExpr,
        environment: &Environment,
    ) -> Result<ResolvedValue, CompileError> {
        match fold_static_match(value)? {
            ValueExpr::Reference(reference) => {
                self.resolve_reference(&reference.source(), environment)
            }
            ValueExpr::Literal(value) => Ok(ResolvedValue {
                value_type: literal_type(&value)?,
                source: ValueSource::Literal { value },
                producer_port: None,
            }),
            ValueExpr::Array(values) => {
                let mut result = Vec::with_capacity(values.len());
                for value in values {
                    let value = self.resolve_value(&value, environment)?;
                    let ValueSource::Literal { value } = value.source else {
                        return Err(CompileError::new(
                            EXPRESSION_ENGINE_BLOCKED,
                            "dynamic array construction requires a published typed Array expression engine",
                        ));
                    };
                    result.push(value);
                }
                let value = Value::Array(result);
                Ok(ResolvedValue {
                    value_type: literal_type(&value)?,
                    source: ValueSource::Literal { value },
                    producer_port: None,
                })
            }
            ValueExpr::Object(values) => {
                let mut result = Map::new();
                for (key, value) in values {
                    let value = self.resolve_value(&value, environment)?;
                    let ValueSource::Literal { value } = value.source else {
                        return Err(CompileError::new(
                            EXPRESSION_ENGINE_BLOCKED,
                            "dynamic object construction requires a published typed Object expression engine",
                        ));
                    };
                    result.insert(key, value);
                }
                let value = Value::Object(result);
                Ok(ResolvedValue {
                    value_type: literal_type(&value)?,
                    source: ValueSource::Literal { value },
                    producer_port: None,
                })
            }
            ValueExpr::Template(_) => Err(CompileError::new(
                EXPRESSION_ENGINE_BLOCKED,
                "text templates require a published typed Template expression engine",
            )),
            ValueExpr::Match(_) => Err(CompileError::new(
                EXPRESSION_ENGINE_BLOCKED,
                "dynamic match is not published by Canonical Plan wire v1",
            )),
            ValueExpr::ErrorRef(_) => Err(CompileError::new(
                INVALID_REFERENCE,
                "declared errors are only valid in raise",
            )),
        }
    }

    fn resolve_reference(
        &self,
        reference: &str,
        environment: &Environment,
    ) -> Result<ResolvedValue, CompileError> {
        let mut parts = reference.split('.');
        let root = parts.next().expect("validated references are non-empty");
        let symbol = environment.get(root).ok_or_else(|| {
            CompileError::new(
                INVALID_REFERENCE,
                format!("reference '${reference}' is not visible in this lexical scope"),
            )
        })?;
        let path = parts.collect::<Vec<_>>();
        if path.is_empty() {
            return Ok(ResolvedValue {
                source: symbol.source.clone(),
                value_type: symbol.value_type.clone(),
                producer_port: symbol.producer_port.clone(),
            });
        }
        if symbol.producer_port.is_some() {
            return Err(CompileError::new(
                EXPRESSION_ENGINE_BLOCKED,
                "projecting a field from a step output requires the versioned Project expression engine, which Plan wire v1 has not published",
            ));
        }
        let value_type = project_type(&symbol.value_type, &path)?;
        let (input_path, optional) = match &symbol.source {
            ValueSource::RunInput { path } => (path, false),
            ValueSource::OptionalRunInput { path } => (path, true),
            _ => {
                return Err(CompileError::new(
                    INVALID_REFERENCE,
                    "field projection source is not a run input",
                ));
            }
        };
        let mut projected = input_path.clone();
        projected.extend(path.iter().map(|value| (*value).to_owned()));
        Ok(ResolvedValue {
            source: if optional {
                ValueSource::OptionalRunInput { path: projected }
            } else {
                ValueSource::RunInput { path: projected }
            },
            value_type,
            producer_port: None,
        })
    }

    fn capture_value(
        &mut self,
        target_scope: &ScopeId,
        value: &ResolvedValue,
    ) -> Result<(), CompileError> {
        let Some(port) = &value.producer_port else {
            return Ok(());
        };
        let source_scope = self
            .data_ports
            .iter()
            .find(|candidate| candidate.id() == port)
            .and_then(|candidate| {
                self.nodes
                    .iter()
                    .find(|node| node.id() == candidate.owner())
                    .map(|node| node.scope_id().clone())
            })
            .ok_or_else(|| {
                CompileError::new(INVALID_REFERENCE, "capture source port is missing")
            })?;
        let mut cursor = target_scope.clone();
        while cursor != source_scope {
            let scope = self.scopes.get_mut(&cursor).ok_or_else(|| {
                CompileError::new(INVALID_REFERENCE, "capture target scope is missing")
            })?;
            scope.captures.insert(port.clone());
            cursor = scope.parent.clone().ok_or_else(|| {
                CompileError::new(
                    INVALID_REFERENCE,
                    "reference crosses an unrelated lexical scope",
                )
            })?;
        }
        Ok(())
    }

    fn add_node(&mut self, id: NodeId, scope: ScopeId, kind: NodeKind) -> Result<(), CompileError> {
        if self.entry.is_none() {
            self.entry = Some(id.clone());
        }
        self.nodes.push(Node::new(id, scope, kind));
        Ok(())
    }

    fn connect_incoming(
        &mut self,
        incoming: Option<ControlPoint>,
        node: &NodeId,
    ) -> Result<(), CompileError> {
        if let Some(incoming) = incoming {
            let input = self.add_control_port(node, "in", PortDirection::Input)?;
            self.add_control_edge(incoming.0, input, &format!("control:{node}"))?;
        } else if self.entry.as_ref().is_some_and(|entry| entry != node) {
            return Err(CompileError::new(
                INVALID_CONTROL_FLOW,
                "compiler attempted to create a second entry path",
            ));
        }
        Ok(())
    }

    fn add_control_port(
        &mut self,
        owner: &NodeId,
        name: &str,
        direction: PortDirection,
    ) -> Result<ControlPortId, CompileError> {
        let id = ControlPortId::new(stable_id(
            "control_port",
            &format!("{owner}:{direction:?}:{name}"),
        ))
        .map_err(plan_error)?;
        self.control_ports.push(ControlPort::new(
            id.clone(),
            owner.clone(),
            port_name(name)?,
            direction,
        ));
        Ok(id)
    }

    fn add_data_port(
        &mut self,
        owner: &NodeId,
        name: &str,
        direction: PortDirection,
        value_type: PlanType,
        required: bool,
    ) -> Result<DataPortId, CompileError> {
        let id = DataPortId::new(stable_id(
            "data_port",
            &format!("{owner}:{direction:?}:{name}"),
        ))
        .map_err(plan_error)?;
        self.data_ports.push(DataPort::new(
            id.clone(),
            owner.clone(),
            port_name(name)?,
            direction,
            value_type,
            required,
        ));
        Ok(id)
    }

    fn add_control_edge(
        &mut self,
        from: ControlPortId,
        to: ControlPortId,
        semantic: &str,
    ) -> Result<(), CompileError> {
        let id = ControlEdgeId::new(stable_id("control_edge", semantic)).map_err(plan_error)?;
        self.control_edges.push(ControlEdge::new(id, from, to));
        Ok(())
    }

    fn add_data_binding(
        &mut self,
        source: ValueSource,
        to: DataPortId,
        semantic: &str,
    ) -> Result<(), CompileError> {
        let id = DataBindingId::new(stable_id("data_binding", semantic)).map_err(plan_error)?;
        self.data_bindings.push(DataBinding::new(id, source, to));
        Ok(())
    }

    fn generated_node(
        &mut self,
        parent: &NodeId,
        role: &str,
        member: Option<&str>,
    ) -> Result<NodeId, CompileError> {
        self.generator
            .compiler_node_id(parent, role, member)
            .map_err(plan_error)
    }

    fn complete_source_map(&self) -> Result<SourceMap, CompileError> {
        let source_id =
            SourceDocumentId::new(self.options.source_id.clone()).map_err(plan_error)?;
        let span = SourceSpan::new(
            source_id.clone(),
            SourcePosition::new(0, 1, 1),
            SourcePosition::new(
                self.options.source_end.0,
                self.options.source_end.1,
                self.options.source_end.2,
            ),
        );
        let mut source_map = SourceMap::authored(source_id, self.options.source_hash.clone());
        for node in &self.nodes {
            source_map.insert_node(node.id().clone(), span.clone());
        }
        for port in &self.control_ports {
            source_map.insert_control_port(port.id().clone(), span.clone());
        }
        for port in &self.data_ports {
            source_map.insert_data_port(port.id().clone(), span.clone());
        }
        for edge in &self.control_edges {
            source_map.insert_control_edge(edge.id().clone(), span.clone());
        }
        for binding in &self.data_bindings {
            source_map.insert_data_binding(binding.id().clone(), span.clone());
        }
        for phi in &self.phi_bindings {
            source_map.insert_phi_binding(phi.id().clone(), span.clone());
        }
        for scope in self.scopes.values() {
            source_map.insert_scope(scope.id.clone(), span.clone());
        }
        Ok(source_map)
    }
}

#[derive(Debug, Clone, Copy)]
enum BlockContext {
    Root,
    IfArm,
    ParallelLeg,
    MapBody,
    LoopBody,
}

fn static_literal_value(value: &ValueExpr) -> Result<Value, CompileError> {
    match fold_static_match(value)? {
        ValueExpr::Literal(value) => Ok(value),
        ValueExpr::Array(values) => values
            .iter()
            .map(static_literal_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        ValueExpr::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), static_literal_value(value)?)))
            .collect::<Result<Map<_, _>, CompileError>>()
            .map(Value::Object),
        ValueExpr::Reference(_)
        | ValueExpr::Template(_)
        | ValueExpr::Match(_)
        | ValueExpr::ErrorRef(_) => Err(CompileError::new(
            EXPRESSION_ENGINE_BLOCKED,
            "dynamic composite match arms require their own published expression algebra",
        )),
    }
}

fn lower_author_type(contract: &AuthorTypeContract) -> Result<PlanType, CompileError> {
    let mut shape = contract.shape.clone();
    for (path, constraints) in &contract.constraints {
        apply_type_constraints(&mut shape, path, constraints)?;
    }
    shape
        .normalized()
        .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))
}

fn require_stable_map_key(item_type: &PlanType, key_field: &str) -> Result<(), CompileError> {
    let PlanType::Object {
        properties,
        additional_properties,
    } = item_type
    else {
        return Err(CompileError::new(
            INVALID_TYPE,
            "map item type must be a closed object with a stable string key",
        ));
    };
    let valid_key = properties.get(key_field).is_some_and(|property| {
        property.required
            && property.value_type != PlanType::Never
            && property.value_type.is_assignable_to(&PlanType::String)
    });
    if additional_properties.is_some() || !valid_key {
        return Err(CompileError::new(
            INVALID_TYPE,
            "map key must name a required non-null string field on the closed item type",
        ));
    }
    Ok(())
}

fn reject_duplicate_static_map_keys(
    items: &ValueExpr,
    key_field: &str,
) -> Result<(), CompileError> {
    let Ok(Value::Array(items)) = static_literal_value(items) else {
        return Ok(());
    };
    let mut keys = BTreeSet::new();
    for item in items {
        let Some(key) = item
            .as_object()
            .and_then(|object| object.get(key_field))
            .and_then(Value::as_str)
        else {
            continue;
        };
        if !keys.insert(key.to_owned()) {
            return Err(CompileError::new(
                INVALID_CONTROL_FLOW,
                format!("static map items contain duplicate key '{key}'"),
            ));
        }
    }
    Ok(())
}

fn contains_agent_operation(steps: &[Step]) -> bool {
    steps.iter().any(|step| match step {
        Step::Leaf(leaf) => matches!(
            leaf.kind,
            LeafKind::Llm | LeafKind::Retrieval | LeafKind::Tool
        ),
        Step::Call(_) => true,
        Step::If(value) => {
            contains_agent_operation(&value.then_steps)
                || value
                    .elif
                    .iter()
                    .any(|arm| contains_agent_operation(&arm.steps))
                || value
                    .else_steps
                    .as_deref()
                    .is_some_and(contains_agent_operation)
        }
        Step::Parallel(value) => value
            .legs
            .iter()
            .any(|leg| contains_agent_operation(&leg.steps)),
        Step::Map(value) => contains_agent_operation(&value.steps),
        Step::Loop(value) => contains_agent_operation(&value.steps),
        Step::Try(value) => {
            contains_agent_operation(&value.protected_steps)
                || contains_agent_operation(&value.handler_steps)
                || contains_agent_operation(&value.finally_steps)
        }
        Step::Wait(_)
        | Step::HumanTask(_)
        | Step::Yield(_)
        | Step::Break(_)
        | Step::Continue(_)
        | Step::Return(_)
        | Step::Raise(_) => false,
    })
}

fn apply_type_constraints(
    value_type: &mut PlanType,
    path: &[String],
    constraints: &super::ast::TypeConstraints,
) -> Result<(), CompileError> {
    if path.is_empty() {
        if let PlanType::Union { variants } = value_type {
            let mut applied = false;
            for variant in variants.iter_mut() {
                if matches!(variant, PlanType::Null) {
                    continue;
                }
                apply_type_constraints_here(variant, constraints)?;
                applied = true;
            }
            return if applied {
                Ok(())
            } else {
                Err(CompileError::new(
                    INVALID_TYPE,
                    "constraints cannot target a null-only union",
                ))
            };
        }
        return apply_type_constraints_here(value_type, constraints);
    }
    match (&path[0][..], value_type) {
        ("[]", PlanType::Array { items, .. } | PlanType::ArrayBounded { items, .. }) => {
            apply_type_constraints(items, &path[1..], constraints)
        }
        (field, PlanType::Object { properties, .. }) => {
            let property = properties.get_mut(field).ok_or_else(|| {
                CompileError::new(INVALID_TYPE, "constraint path references a missing field")
            })?;
            apply_type_constraints(&mut property.value_type, &path[1..], constraints)
        }
        (_, PlanType::Union { variants }) => {
            let mut applied = false;
            for variant in variants.iter_mut() {
                if matches!(variant, PlanType::Null) {
                    continue;
                }
                apply_type_constraints(variant, path, constraints)?;
                applied = true;
            }
            if applied {
                Ok(())
            } else {
                Err(CompileError::new(
                    INVALID_TYPE,
                    "constraint path cannot be applied to a null-only union",
                ))
            }
        }
        _ => Err(CompileError::new(
            INVALID_TYPE,
            "constraint path does not match its declared type",
        )),
    }
}

fn apply_type_constraints_here(
    value_type: &mut PlanType,
    constraints: &super::ast::TypeConstraints,
) -> Result<(), CompileError> {
    let has_array = constraints.min_items.is_some() || constraints.max_items.is_some();
    let has_string = constraints.min_length.is_some()
        || constraints.max_length.is_some()
        || constraints.pattern.is_some()
        || constraints.enum_values.is_some();
    if has_array && has_string {
        return Err(CompileError::new(
            INVALID_TYPE,
            "array and string constraints cannot target the same type",
        ));
    }
    if has_array {
        let Some((items, current_minimum, current_maximum)) = value_type.array_constraints() else {
            return Err(CompileError::new(
                INVALID_TYPE,
                "min_items/max_items can only constrain an array",
            ));
        };
        *value_type = PlanType::array(
            items.clone(),
            constraints.min_items.unwrap_or(current_minimum),
            constraints.max_items.or(current_maximum),
        )
        .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))?;
    } else if has_string {
        let Some((current_minimum, current_maximum, current_pattern, current_enum)) =
            value_type.string_constraints()
        else {
            return Err(CompileError::new(
                INVALID_TYPE,
                "string bounds, pattern, and enum can only constrain a string",
            ));
        };
        *value_type = PlanType::string(
            constraints.min_length.unwrap_or(current_minimum),
            constraints.max_length.or(current_maximum),
            constraints
                .pattern
                .clone()
                .or_else(|| current_pattern.map(str::to_owned)),
            constraints
                .enum_values
                .clone()
                .or_else(|| current_enum.map(<[Value]>::to_vec)),
        )
        .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))?;
    }
    Ok(())
}

fn parallel_result_type(
    legs: &[(String, PlanType)],
    mode: PlanJoinMode,
    error_type: &PlanType,
) -> Result<PlanType, CompileError> {
    let mut properties = BTreeMap::new();
    for (id, value_type) in legs {
        let value_type = match mode {
            PlanJoinMode::AllSuccess => value_type.clone(),
            PlanJoinMode::AllSettled => {
                let ok = PlanType::Object {
                    properties: BTreeMap::from([
                        (
                            "kind".to_owned(),
                            PlanProperty::new(literal_type(&Value::String("ok".to_owned()))?, true)
                                .map_err(|failure| {
                                    CompileError::new(INVALID_TYPE, failure.to_string())
                                })?,
                        ),
                        (
                            "value".to_owned(),
                            PlanProperty::new(value_type.clone(), true).map_err(|failure| {
                                CompileError::new(INVALID_TYPE, failure.to_string())
                            })?,
                        ),
                    ]),
                    additional_properties: None,
                };
                let error = PlanType::Object {
                    properties: BTreeMap::from([
                        (
                            "error".to_owned(),
                            PlanProperty::new(error_type.clone(), true).map_err(|failure| {
                                CompileError::new(INVALID_TYPE, failure.to_string())
                            })?,
                        ),
                        (
                            "kind".to_owned(),
                            PlanProperty::new(
                                literal_type(&Value::String("error".to_owned()))?,
                                true,
                            )
                            .map_err(|failure| {
                                CompileError::new(INVALID_TYPE, failure.to_string())
                            })?,
                        ),
                    ]),
                    additional_properties: None,
                };
                PlanType::union([ok, error])
                    .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))?
            }
        };
        properties.insert(
            id.clone(),
            PlanProperty::new(value_type, true)
                .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))?,
        );
    }
    Ok(PlanType::Object {
        properties,
        additional_properties: None,
    })
}

fn safe_error_type() -> Result<PlanType, CompileError> {
    PlanType::safe_error().map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))
}

fn message_plan_type() -> Result<PlanType, CompileError> {
    let text = PlanType::Object {
        properties: BTreeMap::from([(
            "text".to_owned(),
            PlanProperty::new(PlanType::String, true)
                .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))?,
        )]),
        additional_properties: None,
    };
    let image = PlanType::Object {
        properties: BTreeMap::from([(
            "image_url".to_owned(),
            PlanProperty::new(PlanType::String, true)
                .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))?,
        )]),
        additional_properties: None,
    };
    let content = PlanType::Array {
        items: Box::new(
            PlanType::union([text, image])
                .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))?,
        ),
        min_items: 0,
    };
    let variants = ["user", "assistant"].map(|role| -> Result<PlanType, CompileError> {
        Ok(PlanType::Object {
            properties: BTreeMap::from([
                (
                    "content".to_owned(),
                    PlanProperty::new(content.clone(), true)
                        .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))?,
                ),
                (
                    "role".to_owned(),
                    PlanProperty::new(literal_type(&Value::String(role.to_owned()))?, true)
                        .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))?,
                ),
            ]),
            additional_properties: None,
        })
    });
    PlanType::union(variants.into_iter().collect::<Result<Vec<_>, _>>()?)
        .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))
}

fn is_message_array(value_type: &PlanType) -> Result<bool, CompileError> {
    let Some((items, _, _)) = value_type.array_constraints() else {
        return Ok(false);
    };
    Ok(items == &message_plan_type()?)
}

fn string_or_nullable_string(value_type: &PlanType) -> bool {
    value_type.string_constraints().is_some()
        || matches!(
            value_type,
            PlanType::Union { variants }
                if variants.len() == 2
                    && variants.iter().any(|variant| variant == &PlanType::Null)
                    && variants.iter().any(|variant| variant.string_constraints().is_some())
        )
}

fn project_type(value_type: &PlanType, path: &[&str]) -> Result<PlanType, CompileError> {
    let mut current = value_type;
    for field in path {
        let PlanType::Object { properties, .. } = current else {
            return Err(CompileError::new(
                INVALID_REFERENCE,
                "reference field projection requires an object",
            ));
        };
        current = &properties
            .get(*field)
            .ok_or_else(|| {
                CompileError::new(
                    INVALID_REFERENCE,
                    format!("reference projects unknown field '{field}'"),
                )
            })?
            .value_type;
    }
    Ok(current.clone())
}

fn collect_configuration_references(value: &Value) -> Result<BTreeSet<String>, CompileError> {
    let mut references = BTreeSet::new();
    collect_references(value, &mut references)?;
    Ok(references)
}

fn collect_references(
    value: &Value,
    references: &mut BTreeSet<String>,
) -> Result<(), CompileError> {
    match value {
        Value::String(value) => {
            if let Some(reference) = value.strip_prefix('$') {
                references.insert(reference.to_owned());
                return Ok(());
            }
            let mut rest = value.as_str();
            while let Some(open) = rest.find("{{") {
                let after = &rest[open + 2..];
                let close = after.find("}}").ok_or_else(|| {
                    CompileError::new(INVALID_REFERENCE, "unterminated text template")
                })?;
                references.insert(after[..close].trim().to_owned());
                rest = &after[close + 2..];
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_references(value, references)?;
            }
        }
        Value::Object(values) => {
            if values.len() == 1
                && values.keys().next().is_some_and(|key| {
                    matches!(
                        key.as_str(),
                        "from" | "literal" | "object" | "array" | "template"
                    )
                })
            {
                return Err(CompileError::new(
                    INVALID_REFERENCE,
                    "legacy value wrappers are forbidden in v3 natural YAML",
                ));
            }
            for value in values.values() {
                collect_references(value, references)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn descriptor_object(value: &Value) -> Result<BTreeMap<String, DescriptorValue>, CompileError> {
    let Value::Object(values) = value else {
        return Err(CompileError::new(
            INVALID_DOCUMENT,
            "leaf configuration must be an object",
        ));
    };
    values
        .iter()
        .map(|(key, value)| Ok((key.clone(), descriptor_value(value)?)))
        .collect()
}

fn compile_message_program(contract: &LlmContract) -> DescriptorValue {
    DescriptorValue::Array(
        contract
            .messages
            .iter()
            .map(|message| match message {
                MessageExpr::Splice(path) => DescriptorValue::Object(BTreeMap::from([
                    (
                        "kind".to_owned(),
                        DescriptorValue::String("message_splice".to_owned()),
                    ),
                    ("path".to_owned(), DescriptorValue::String(path.source())),
                ])),
                MessageExpr::Message { role, content } => {
                    DescriptorValue::Object(BTreeMap::from([
                        (
                            "content".to_owned(),
                            DescriptorValue::Array(
                                content
                                    .iter()
                                    .map(|part| match part {
                                        ContentPart::Text(value) => {
                                            let (kind, value, references) = match value {
                                                TextContent::PromptRef(value) => {
                                                    ("prompt_ref", value.clone(), Vec::new())
                                                }
                                                TextContent::ValueRef(value) => {
                                                    ("value_ref", value.source(), Vec::new())
                                                }
                                                TextContent::Template(value) => (
                                                    "template",
                                                    value.source.clone(),
                                                    value
                                                        .references
                                                        .iter()
                                                        .map(|path| {
                                                            DescriptorValue::String(path.source())
                                                        })
                                                        .collect(),
                                                ),
                                                TextContent::Literal(value) => {
                                                    ("literal", value.clone(), Vec::new())
                                                }
                                            };
                                            DescriptorValue::Object(BTreeMap::from([
                                                (
                                                    "kind".to_owned(),
                                                    DescriptorValue::String(kind.to_owned()),
                                                ),
                                                (
                                                    "references".to_owned(),
                                                    DescriptorValue::Array(references),
                                                ),
                                                ("text".to_owned(), DescriptorValue::String(value)),
                                            ]))
                                        }
                                        ContentPart::ImageUrl(value) => {
                                            let (kind, value) = match value {
                                                ImageUrlContent::ValueRef(value) => {
                                                    ("value_ref", value.source())
                                                }
                                                ImageUrlContent::Literal(value) => {
                                                    ("literal", value.clone())
                                                }
                                            };
                                            DescriptorValue::Object(BTreeMap::from([
                                                (
                                                    "image_url".to_owned(),
                                                    DescriptorValue::String(value),
                                                ),
                                                (
                                                    "kind".to_owned(),
                                                    DescriptorValue::String(kind.to_owned()),
                                                ),
                                            ]))
                                        }
                                    })
                                    .collect(),
                            ),
                        ),
                        (
                            "kind".to_owned(),
                            DescriptorValue::String("message".to_owned()),
                        ),
                        (
                            "role".to_owned(),
                            DescriptorValue::String(
                                match role {
                                    MessageRole::System => "system",
                                    MessageRole::User => "user",
                                    MessageRole::Assistant => "assistant",
                                }
                                .to_owned(),
                            ),
                        ),
                    ]))
                }
            })
            .collect(),
    )
}

fn descriptor_value(value: &Value) -> Result<DescriptorValue, CompileError> {
    match value {
        Value::Null => Ok(DescriptorValue::Null),
        Value::Bool(value) => Ok(DescriptorValue::Boolean(*value)),
        Value::Number(value) => Ok(value.as_i64().map_or_else(
            || DescriptorValue::Number(value.clone()),
            DescriptorValue::Integer,
        )),
        Value::String(value) => Ok(DescriptorValue::String(value.clone())),
        Value::Array(values) => values
            .iter()
            .map(descriptor_value)
            .collect::<Result<Vec<_>, _>>()
            .map(DescriptorValue::Array),
        Value::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), descriptor_value(value)?)))
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(DescriptorValue::Object),
    }
}

fn authored_node_ids(steps: &[Step]) -> Vec<String> {
    let mut values = Vec::new();
    for step in steps {
        match step {
            Step::Leaf(step) => values.push(step.id.clone()),
            Step::Wait(step) => values.push(step.id.clone()),
            Step::HumanTask(step) => values.push(step.id.clone()),
            Step::Call(step) => values.push(step.id.clone()),
            Step::If(step) => {
                values.push(step.id.clone());
                values.extend(authored_node_ids(&step.then_steps));
                for arm in &step.elif {
                    values.extend(authored_node_ids(&arm.steps));
                }
                if let Some(steps) = &step.else_steps {
                    values.extend(authored_node_ids(steps));
                }
            }
            Step::Parallel(step) => {
                values.push(step.id.clone());
                for leg in &step.legs {
                    values.extend(authored_node_ids(&leg.steps));
                }
            }
            Step::Map(step) => {
                values.push(step.id.clone());
                values.extend(authored_node_ids(&step.steps));
            }
            Step::Loop(step) => {
                values.push(step.id.clone());
                values.extend(authored_node_ids(&step.steps));
            }
            Step::Try(step) => {
                values.push(step.id.clone());
                values.extend(authored_node_ids(&step.protected_steps));
                values.extend(authored_node_ids(&step.handler_steps));
                values.extend(authored_node_ids(&step.finally_steps));
            }
            Step::Yield(_)
            | Step::Break(_)
            | Step::Continue(_)
            | Step::Return(_)
            | Step::Raise(_) => {}
        }
    }
    values
}

fn has_executable_node(steps: &[Step]) -> bool {
    steps.iter().any(|step| !matches!(step, Step::Yield(_)))
}

fn source_end(source: &str) -> (u32, u32) {
    let mut line = 1_u32;
    let mut column = 1_u32;
    for character in source.chars() {
        if character == '\n' {
            line = line.saturating_add(1);
            column = 1;
        } else {
            column = column.saturating_add(1);
        }
    }
    (line, column)
}

fn stable_id(domain: &str, semantic: &str) -> String {
    let mut hasher = Sha256::new();
    for value in [domain.as_bytes(), semantic.as_bytes()] {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }
    let digest = hasher.finalize();
    let mut result = String::from("v3_");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        result.push(char::from(HEX[usize::from(byte >> 4)]));
        result.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    result
}

fn stable_parent(value: &str) -> Result<NodeId, CompileError> {
    node_id(&stable_id("terminal_parent", value))
}

fn node_id(value: &str) -> Result<NodeId, CompileError> {
    NodeId::new(value.to_owned()).map_err(model_error)
}

fn scope_id(value: &str) -> Result<ScopeId, CompileError> {
    ScopeId::new(value.to_owned()).map_err(plan_error)
}

fn branch_case_id(value: &str) -> Result<BranchCaseId, CompileError> {
    BranchCaseId::new(value.to_owned()).map_err(plan_error)
}

fn port_name(value: &str) -> Result<PortName, CompileError> {
    PortName::new(value.to_owned()).map_err(plan_error)
}

fn phi_id(semantic: &str) -> Result<PhiBindingId, CompileError> {
    PhiBindingId::new(stable_id("phi", semantic)).map_err(plan_error)
}

fn version(value: &str) -> Result<VersionTag, CompileError> {
    VersionTag::new(value.to_owned()).map_err(plan_error)
}

fn plan_error(error: crate::engine::plan::PlanError) -> CompileError {
    CompileError::new(error.code(), error.message().to_owned())
}

fn model_error(error: crate::engine::ModelError) -> CompileError {
    CompileError::new(INVALID_DOCUMENT, error.to_string())
}

fn unreachable_after_terminator() -> CompileError {
    CompileError::new(
        INVALID_CONTROL_FLOW,
        "yield, return, and raise must be the final step of their structured block",
    )
}
