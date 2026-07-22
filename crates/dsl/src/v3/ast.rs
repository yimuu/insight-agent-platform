use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::{CompileError, DslPath};
use insight_engine::plan::{PlanProperty, PlanType};

use super::{
    raw::RawDocument, API_VERSION, DOCUMENT_KIND, INVALID_CONTROL_FLOW, INVALID_DOCUMENT,
    INVALID_STEP, INVALID_TYPE,
};

#[derive(Debug, Clone, PartialEq)]
pub struct StructuredAuthorDocument {
    pub metadata: Option<Metadata>,
    pub prompts: BTreeMap<String, PromptDeclaration>,
    pub errors: BTreeMap<String, ErrorDeclaration>,
    pub types: BTreeMap<String, AuthorTypeContract>,
    pub inputs: BTreeMap<String, InputDeclaration>,
    pub input_type: AuthorTypeContract,
    pub output_type: AuthorTypeContract,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub id: String,
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptDeclaration {
    Inline(String),
    File(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorDeclaration {
    pub code: String,
    pub public_message: String,
}

/// Full author contract retained until the Plan-lowering boundary. `shape`
/// provides ordinary assignability while path-keyed constraints preserve
/// contracts that Canonical Plan wire v1 cannot represent yet.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthorTypeContract {
    pub shape: PlanType,
    pub constraints: BTreeMap<Vec<String>, TypeConstraints>,
    pub nominal: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TypeConstraints {
    pub min_items: Option<u64>,
    pub max_items: Option<u64>,
    pub min_length: Option<u64>,
    pub max_length: Option<u64>,
    pub pattern: Option<String>,
    pub enum_values: Option<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InputDeclaration {
    pub value_type: AuthorTypeContract,
    pub optional: bool,
    pub default: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    Leaf(LeafStep),
    If(IfStep),
    Parallel(ParallelStep),
    Map(MapStep),
    Loop(LoopStep),
    Call(CallStep),
    Try(TryStep),
    HumanTask(HumanTaskStep),
    Wait(WaitStep),
    Yield(ValueExpr),
    Break(ValueExpr),
    Continue(ValueExpr),
    Return(ValueExpr),
    Raise(ValueExpr),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafKind {
    Llm,
    Action,
    Retrieval,
    Http,
    Tool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LeafStep {
    pub id: String,
    pub kind: LeafKind,
    pub implementation: String,
    /// Inert author configuration. Runtime values remain `$name` references;
    /// the compiler extracts them into explicit typed data ports/bindings.
    pub configuration: Value,
    /// LLM always declares this. Other leaf kinds may inherit their output
    /// from a versioned descriptor registry at contextual linking.
    pub output_type: Option<AuthorTypeContract>,
    pub llm: Option<LlmContract>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmContract {
    pub messages: Vec<MessageExpr>,
    pub stream: bool,
    pub publish: bool,
    pub tools: Vec<String>,
    pub tool_choice: LlmToolChoice,
    pub tool_limits: LlmToolLimits,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LlmToolChoice {
    Auto,
    Required,
    Tool(String),
}

impl LlmToolChoice {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Auto => "auto",
            Self::Required => "required",
            Self::Tool(tool) => tool,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LlmToolLimits {
    pub max_rounds: u32,
    pub max_calls: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageExpr {
    Splice(ValuePath),
    Message {
        role: MessageRole,
        content: Vec<ContentPart>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageRole {
    System,
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentPart {
    Text(TextContent),
    ImageUrl(ImageUrlContent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextContent {
    PromptRef(String),
    ValueRef(ValuePath),
    Template(TextTemplate),
    Literal(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageUrlContent {
    ValueRef(ValuePath),
    Literal(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct IfStep {
    pub id: String,
    pub condition: String,
    pub then_steps: Vec<Step>,
    pub elif: Vec<ElifArm>,
    pub else_steps: Option<Vec<Step>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ElifArm {
    pub id: String,
    pub condition: String,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelSettle {
    AllSuccess,
    AllSettled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParallelStep {
    pub id: String,
    pub settle: ParallelSettle,
    pub legs: Vec<ParallelLeg>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParallelLeg {
    pub id: String,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MapStep {
    pub id: String,
    pub items: ValueExpr,
    /// Optional stable business key. When omitted, durable identity is the
    /// canonical input ordinal and therefore intentionally order-sensitive.
    pub key_field: Option<String>,
    pub item_name: String,
    pub max_concurrency: Option<u32>,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopFlavor {
    Workflow,
    Agent,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LoopStep {
    pub id: String,
    pub flavor: LoopFlavor,
    pub initial: ValueExpr,
    pub state_name: String,
    pub until: String,
    pub max_iterations: Option<u32>,
    pub deadline_ms: Option<u64>,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CallStep {
    pub id: String,
    pub definition_revision: String,
    pub interface_version: String,
    pub input: BTreeMap<String, ValueExpr>,
    pub output_type: AuthorTypeContract,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TryStep {
    pub id: String,
    pub protected_steps: Vec<Step>,
    pub error_name: String,
    pub handler_steps: Vec<Step>,
    pub finally_steps: Vec<Step>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HumanTaskStep {
    pub id: String,
    pub signal_name: String,
    pub payload_type: AuthorTypeContract,
    pub request: ValueExpr,
    pub assignees: Vec<String>,
    pub candidate_groups: Vec<String>,
    pub claim_lease_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WaitStep {
    pub id: String,
    pub kind: WaitKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WaitKind {
    Signal {
        name: String,
        payload_type: AuthorTypeContract,
    },
    Timer {
        duration_ms: ValueExpr,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueExpr {
    Reference(ValuePath),
    Literal(Value),
    Array(Vec<ValueExpr>),
    Object(BTreeMap<String, ValueExpr>),
    Template(TextTemplate),
    Match(MatchExpr),
    ErrorRef(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ValuePath {
    pub root: String,
    pub segments: Vec<String>,
}

impl ValuePath {
    pub fn parse(source: &str) -> Result<Self, CompileError> {
        validate_reference_path(source)?;
        let mut parts = source.split('.');
        Ok(Self {
            root: parts
                .next()
                .expect("validated path is non-empty")
                .to_owned(),
            segments: parts.map(str::to_owned).collect(),
        })
    }

    pub fn source(&self) -> String {
        std::iter::once(self.root.as_str())
            .chain(self.segments.iter().map(String::as_str))
            .collect::<Vec<_>>()
            .join(".")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextTemplate {
    pub source: String,
    pub references: Vec<ValuePath>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchExpr {
    pub selector: Box<ValueExpr>,
    /// Ordered for deterministic diagnostics and canonical lowering. YAML map
    /// order is not semantic, so validation canonicalizes cases by key.
    pub cases: BTreeMap<String, ValueExpr>,
    pub default: Box<ValueExpr>,
}

pub fn validate(raw: RawDocument) -> Result<StructuredAuthorDocument, CompileError> {
    if raw.api_version != API_VERSION || raw.kind != DOCUMENT_KIND {
        return Err(error(
            INVALID_DOCUMENT,
            "document must declare api_version insight.agent/v3 and kind agent",
            DslPath::root(),
        ));
    }

    let metadata = raw
        .metadata
        .map(|value| {
            validate_identifier(&value.id, "metadata id")?;
            if value.name.trim().is_empty() {
                return Err(CompileError::new(
                    INVALID_DOCUMENT,
                    "metadata name must be non-empty",
                ));
            }
            Ok(Metadata {
                id: value.id,
                name: value.name,
                description: value.description,
            })
        })
        .transpose()?;
    let prompts = raw
        .prompts
        .iter()
        .map(|(id, value)| {
            validate_identifier(id, "prompt id")?;
            let declaration = as_object(value, "prompt must declare inline or file")?;
            let prompt = if let Some(value) = declaration.get("inline") {
                exact_keys(declaration, &["inline"])?;
                PromptDeclaration::Inline(string(value, "inline prompt")?.to_owned())
            } else if let Some(value) = declaration.get("file") {
                exact_keys(declaration, &["file"])?;
                let path = string(value, "prompt file")?;
                if path.starts_with('/') || path.split('/').any(|part| part == "..") {
                    return Err(CompileError::new(
                        INVALID_DOCUMENT,
                        "prompt file must be a relative path without parent traversal",
                    ));
                }
                PromptDeclaration::File(path.to_owned())
            } else {
                return Err(CompileError::new(
                    INVALID_DOCUMENT,
                    "prompt must declare exactly one of inline or file",
                ));
            };
            Ok((id.clone(), prompt))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let errors = raw
        .errors
        .iter()
        .map(|(id, value)| {
            validate_identifier(id, "error id")?;
            let declaration = as_object(value, "error declaration must be an object")?;
            exact_keys(declaration, &["category", "code", "public_message"])?;
            if string(required(declaration, "category")?, "error category")? != "workflow" {
                return Err(CompileError::new(
                    INVALID_DOCUMENT,
                    "v3 Core error category must be workflow",
                ));
            }
            let code = string(required(declaration, "code")?, "error code")?;
            if !valid_error_code(code) {
                return Err(CompileError::new(
                    INVALID_DOCUMENT,
                    "error code must use bounded uppercase ASCII and underscores",
                ));
            }
            let public_message = string(
                required(declaration, "public_message")?,
                "error public_message",
            )?;
            if public_message.trim().is_empty() || public_message.chars().count() > 512 {
                return Err(CompileError::new(
                    INVALID_DOCUMENT,
                    "error public_message must be non-empty and at most 512 characters",
                ));
            }
            Ok((
                id.clone(),
                ErrorDeclaration {
                    code: code.to_owned(),
                    public_message: public_message.to_owned(),
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;

    let resolver = TypeResolver::new(&raw.types)?;
    let types = resolver.resolve_all()?;
    let mut inputs = BTreeMap::new();
    let mut properties = BTreeMap::new();
    let mut input_constraints = BTreeMap::new();
    for (name, wire) in raw.inputs {
        validate_identifier(&name, "input name")?;
        let declaration = parse_input(&wire, &resolver).map_err(|value| {
            value.with_path(DslPath::root().child_key("inputs").child_key(&name))
        })?;
        properties.insert(
            name.clone(),
            PlanProperty::new(
                declaration.value_type.shape.clone(),
                !declaration.optional && declaration.default.is_none(),
            )
            .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))?,
        );
        for (path, constraints) in &declaration.value_type.constraints {
            let mut qualified = Vec::with_capacity(path.len() + 1);
            qualified.push(name.clone());
            qualified.extend(path.iter().cloned());
            input_constraints.insert(qualified, constraints.clone());
        }
        inputs.insert(name, declaration);
    }
    let input_type = AuthorTypeContract {
        shape: PlanType::Object {
            properties,
            additional_properties: None,
        }
        .normalized()
        .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))?,
        constraints: input_constraints,
        nominal: None,
    };
    let output_type = resolver.resolve_type_value(&raw.output)?;
    let mut steps = raw
        .workflow
        .steps
        .iter()
        .enumerate()
        .map(|(index, value)| {
            parse_step(value, &resolver, &prompts).map_err(|failure| {
                failure.with_path(
                    DslPath::root()
                        .child_key("workflow")
                        .child_key("steps")
                        .child_index(index),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    bind_declared_errors(&mut steps, &errors);
    if steps.is_empty() {
        return Err(error(
            INVALID_DOCUMENT,
            "workflow.steps must contain at least one step",
            DslPath::root().child_key("workflow").child_key("steps"),
        ));
    }
    validate_global_ids(&steps, inputs.keys())?;

    Ok(StructuredAuthorDocument {
        metadata,
        prompts,
        errors,
        types,
        inputs,
        input_type,
        output_type,
        steps,
    })
}

fn parse_input(
    value: &Value,
    resolver: &TypeResolver<'_>,
) -> Result<InputDeclaration, CompileError> {
    if value.is_string() {
        return Ok(InputDeclaration {
            value_type: resolver.resolve_type_value(value)?,
            optional: false,
            default: None,
        });
    }
    let object = as_object(value, "input declaration must be a type string or object")?;
    exact_keys(
        object,
        &[
            "type",
            "optional",
            "default",
            "min_items",
            "max_items",
            "min_length",
            "max_length",
            "pattern",
            "enum",
        ],
    )?;
    let mut value_type = resolver.resolve_type_value(required(object, "type")?)?;
    let optional = object.get("optional").map_or(Ok(false), |value| {
        value
            .as_bool()
            .ok_or_else(|| CompileError::new(INVALID_TYPE, "input optional must be a boolean"))
    })?;
    let default = object.get("default").cloned();
    if optional && default.is_some() {
        return Err(CompileError::new(
            INVALID_TYPE,
            "input default and optional: true are mutually exclusive",
        ));
    }
    if let Some(value) = &default {
        reject_dynamic_default(value)?;
        let actual = literal_type(value)?;
        if !actual.is_assignable_to(&value_type.shape) {
            return Err(CompileError::new(
                INVALID_TYPE,
                "input default does not satisfy its declared type",
            ));
        }
    }
    let declared_constraints = parse_constraints(object)?;
    if declared_constraints != TypeConstraints::default() {
        value_type
            .constraints
            .insert(Vec::new(), declared_constraints);
    }
    Ok(InputDeclaration {
        value_type,
        optional,
        default,
    })
}

fn parse_step(
    value: &Value,
    resolver: &TypeResolver<'_>,
    prompts: &BTreeMap<String, PromptDeclaration>,
) -> Result<Step, CompileError> {
    let object = as_object(value, "workflow step must be an object")?;
    if object.len() == 1 {
        if let Some(value) = object.get("yield") {
            return Ok(Step::Yield(parse_value_expr(value)?));
        }
        if let Some(value) = object.get("return") {
            return Ok(Step::Return(parse_value_expr(value)?));
        }
        if let Some(value) = object.get("raise") {
            return Ok(Step::Raise(parse_value_expr(value)?));
        }
        if let Some(value) = object.get("break") {
            return Ok(Step::Break(parse_value_expr(value)?));
        }
        if let Some(value) = object.get("continue") {
            return Ok(Step::Continue(parse_value_expr(value)?));
        }
    }
    if object.contains_key("switch")
        || object.get("type").and_then(Value::as_str) == Some("switch")
        || object.contains_key("kind")
        || object.contains_key("result")
    {
        return Err(CompileError::new(
            INVALID_STEP,
            "legacy switch, kind, child result, and core.* syntax is not part of v3",
        ));
    }
    if object.contains_key("if") {
        return parse_if(object, resolver, prompts).map(Step::If);
    }
    if object.contains_key("parallel") {
        return parse_parallel(object, resolver, prompts).map(Step::Parallel);
    }
    if object.contains_key("map") {
        return parse_map(object, resolver, prompts).map(Step::Map);
    }
    if object.contains_key("loop") {
        return parse_loop(object, resolver, prompts, LoopFlavor::Workflow).map(Step::Loop);
    }
    if object.contains_key("agent_loop") {
        return parse_loop(object, resolver, prompts, LoopFlavor::Agent).map(Step::Loop);
    }
    if object.contains_key("try") {
        return parse_try(object, resolver, prompts).map(Step::Try);
    }
    if object.contains_key("human_task") {
        return parse_human_task(object, resolver).map(Step::HumanTask);
    }
    if object.contains_key("wait") {
        return parse_wait(object, resolver).map(Step::Wait);
    }
    if object.get("type").and_then(Value::as_str) == Some("call") {
        return parse_call(object, resolver).map(Step::Call);
    }
    if object.contains_key("type") {
        return parse_leaf(object, resolver, prompts).map(Step::Leaf);
    }
    Err(CompileError::new(
        INVALID_STEP,
        "step must be a v3 leaf, structured control, durable wait/call, or explicit terminator",
    ))
}

fn parse_leaf(
    object: &Map<String, Value>,
    resolver: &TypeResolver<'_>,
    prompts: &BTreeMap<String, PromptDeclaration>,
) -> Result<LeafStep, CompileError> {
    let kind = string(required(object, "type")?, "leaf type")?;
    let id = identifier(required(object, "id")?, "leaf id")?;
    let (kind, implementation, allowed) = match kind {
        "llm" => (
            LeafKind::Llm,
            "core.llm".to_owned(),
            &[
                "id",
                "type",
                "model",
                "messages",
                "stream",
                "publish",
                "tools",
                "tool_choice",
                "tool_limits",
                "parameters",
                "response",
            ][..],
        ),
        "action" => (
            LeafKind::Action,
            string(required(object, "call")?, "action call")?.to_owned(),
            &["id", "type", "call", "inputs", "response"][..],
        ),
        "retrieval" => (
            LeafKind::Retrieval,
            string(required(object, "retrieval")?, "retrieval resource")?.to_owned(),
            &["id", "type", "retrieval", "inputs", "publish", "response"][..],
        ),
        "http" => (
            LeafKind::Http,
            "core.http".to_owned(),
            &["id", "type", "method", "url", "headers", "body", "response"][..],
        ),
        "tool" => (
            LeafKind::Tool,
            "core.tool".to_owned(),
            &["id", "type", "tool", "arguments", "response"][..],
        ),
        value if value.starts_with("core.") => {
            return Err(CompileError::new(
                INVALID_STEP,
                "core.* node tags are forbidden in the v3 author surface",
            ));
        }
        _ => {
            return Err(CompileError::new(
                INVALID_STEP,
                "leaf type must be llm, action, retrieval, http, or tool",
            ));
        }
    };
    exact_keys(object, allowed)?;
    let llm = match kind {
        LeafKind::Llm => {
            string(required(object, "model")?, "llm model")?;
            Some(parse_llm_contract(object, prompts)?)
        }
        LeafKind::Action => {
            if let Some(inputs) = object.get("inputs") {
                object_value(inputs, "action inputs")?;
            }
            None
        }
        LeafKind::Retrieval => {
            object_value(required(object, "inputs")?, "retrieval inputs")?;
            if let Some(publish) = object.get("publish") {
                boolean(publish, "retrieval publish")?;
            }
            None
        }
        LeafKind::Http => {
            string(required(object, "method")?, "http method")?;
            required(object, "url")?;
            None
        }
        LeafKind::Tool => {
            string(required(object, "tool")?, "tool name")?;
            if let Some(arguments) = object.get("arguments") {
                object_value(arguments, "tool arguments")?;
            }
            None
        }
    };
    let output_type = match kind {
        LeafKind::Llm => Some(resolver.resolve_type_value(required(object, "response")?)?),
        LeafKind::Action | LeafKind::Retrieval | LeafKind::Http | LeafKind::Tool => object
            .get("response")
            .map(|value| resolver.resolve_type_value(value))
            .transpose()?,
    };
    let mut configuration = object.clone();
    for key in ["id", "type", "response"] {
        configuration.remove(key);
    }
    if kind == LeafKind::Retrieval {
        configuration
            .entry("publish".to_owned())
            .or_insert(Value::Bool(false));
    }
    Ok(LeafStep {
        id,
        kind,
        implementation,
        configuration: Value::Object(configuration),
        output_type,
        llm,
    })
}

fn parse_if(
    object: &Map<String, Value>,
    resolver: &TypeResolver<'_>,
    prompts: &BTreeMap<String, PromptDeclaration>,
) -> Result<IfStep, CompileError> {
    exact_keys(object, &["id", "if", "then", "elif", "else"])?;
    let id = identifier(required(object, "id")?, "if id")?;
    let condition = string(required(object, "if")?, "if condition")?.to_owned();
    let then_steps = parse_steps(required(object, "then")?, resolver, prompts)?;
    let mut elif = Vec::new();
    if let Some(value) = object.get("elif") {
        let values = value
            .as_array()
            .ok_or_else(|| CompileError::new(INVALID_STEP, "elif must be an ordered list"))?;
        for value in values {
            let arm = as_object(value, "elif arm must be an object")?;
            exact_keys(arm, &["id", "when", "then"])?;
            elif.push(ElifArm {
                id: identifier(required(arm, "id")?, "elif id")?,
                condition: string(required(arm, "when")?, "elif condition")?.to_owned(),
                steps: parse_steps(required(arm, "then")?, resolver, prompts)?,
            });
        }
    }
    let else_steps = object
        .get("else")
        .map(|value| parse_steps(value, resolver, prompts))
        .transpose()?;
    Ok(IfStep {
        id,
        condition,
        then_steps,
        elif,
        else_steps,
    })
}

fn parse_parallel(
    object: &Map<String, Value>,
    resolver: &TypeResolver<'_>,
    prompts: &BTreeMap<String, PromptDeclaration>,
) -> Result<ParallelStep, CompileError> {
    exact_keys(object, &["id", "settle", "parallel"])?;
    let id = identifier(required(object, "id")?, "parallel id")?;
    let settle = match object.get("settle").and_then(Value::as_str) {
        None | Some("all_success") => ParallelSettle::AllSuccess,
        Some("all_settled") => ParallelSettle::AllSettled,
        Some(_) => {
            return Err(CompileError::new(
                INVALID_STEP,
                "parallel settle must be all_success or all_settled",
            ));
        }
    };
    let legs_object = object_value(required(object, "parallel")?, "parallel legs")?;
    if legs_object.is_empty() {
        return Err(CompileError::new(
            INVALID_STEP,
            "parallel must declare at least one stable leg",
        ));
    }
    let mut legs = Vec::new();
    for (id, value) in legs_object {
        validate_identifier(id, "parallel leg id")?;
        legs.push(ParallelLeg {
            id: id.clone(),
            steps: parse_steps(value, resolver, prompts)?,
        });
    }
    Ok(ParallelStep { id, settle, legs })
}

fn parse_map(
    object: &Map<String, Value>,
    resolver: &TypeResolver<'_>,
    prompts: &BTreeMap<String, PromptDeclaration>,
) -> Result<MapStep, CompileError> {
    exact_keys(object, &["id", "map"])?;
    let id = identifier(required(object, "id")?, "map id")?;
    let map = object_value(required(object, "map")?, "map declaration")?;
    exact_keys(map, &["items", "key", "as", "max_concurrency", "steps"])?;
    let key_field = map
        .get("key")
        .map(|value| identifier(value, "map key field"))
        .transpose()?;
    let item_name = map
        .get("as")
        .map(|value| identifier(value, "map item name"))
        .transpose()?
        .unwrap_or_else(|| "item".to_owned());
    let max_concurrency = map
        .get("max_concurrency")
        .map(|value| positive_u32(value, "map max_concurrency"))
        .transpose()?;
    Ok(MapStep {
        id,
        items: parse_value_expr(required(map, "items")?)?,
        key_field,
        item_name,
        max_concurrency,
        steps: parse_steps(required(map, "steps")?, resolver, prompts)?,
    })
}

fn parse_loop(
    object: &Map<String, Value>,
    resolver: &TypeResolver<'_>,
    prompts: &BTreeMap<String, PromptDeclaration>,
    flavor: LoopFlavor,
) -> Result<LoopStep, CompileError> {
    let field = match flavor {
        LoopFlavor::Workflow => "loop",
        LoopFlavor::Agent => "agent_loop",
    };
    exact_keys(object, &["id", field])?;
    let id = identifier(required(object, "id")?, "loop id")?;
    let declaration = object_value(required(object, field)?, "loop declaration")?;
    exact_keys(
        declaration,
        &[
            "initial",
            "as",
            "until",
            "max_iterations",
            "deadline_ms",
            "steps",
        ],
    )?;
    let max_iterations = declaration
        .get("max_iterations")
        .map(|value| positive_u32(value, "loop max_iterations"))
        .transpose()?;
    let deadline_ms = declaration
        .get("deadline_ms")
        .map(|value| positive_u64(value, "loop deadline_ms"))
        .transpose()?;
    if max_iterations.is_none() && deadline_ms.is_none() {
        return Err(CompileError::new(
            INVALID_CONTROL_FLOW,
            "loop and agent_loop require max_iterations and/or deadline_ms",
        ));
    }
    let until = match required(declaration, "until")? {
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        _ => {
            return Err(CompileError::new(
                INVALID_STEP,
                "loop until must be a CEL string or boolean literal",
            ));
        }
    };
    Ok(LoopStep {
        id,
        flavor,
        initial: parse_value_expr(required(declaration, "initial")?)?,
        state_name: declaration
            .get("as")
            .map(|value| identifier(value, "loop state name"))
            .transpose()?
            .unwrap_or_else(|| "state".to_owned()),
        until,
        max_iterations,
        deadline_ms,
        steps: parse_steps(required(declaration, "steps")?, resolver, prompts)?,
    })
}

fn parse_call(
    object: &Map<String, Value>,
    resolver: &TypeResolver<'_>,
) -> Result<CallStep, CompileError> {
    exact_keys(
        object,
        &[
            "id",
            "type",
            "definition_revision",
            "interface_version",
            "input",
            "response",
            "request",
            "timeout_ms",
        ],
    )?;
    let input = parse_value_expr(required(object, "input")?)?;
    let ValueExpr::Object(input) = input else {
        return Err(CompileError::new(
            INVALID_STEP,
            "call input must be an object whose fields are the child interface inputs",
        ));
    };
    for name in input.keys() {
        validate_identifier(name, "call input name")?;
    }
    Ok(CallStep {
        id: identifier(required(object, "id")?, "call id")?,
        definition_revision: string(
            required(object, "definition_revision")?,
            "call definition_revision",
        )?
        .to_owned(),
        interface_version: string(
            required(object, "interface_version")?,
            "call interface_version",
        )?
        .to_owned(),
        input,
        output_type: resolver.resolve_type_value(required(object, "response")?)?,
        timeout_ms: object
            .get("timeout_ms")
            .map(|value| positive_u64(value, "call timeout_ms"))
            .transpose()?
            .unwrap_or(5 * 60 * 1_000),
    })
}

fn parse_try(
    object: &Map<String, Value>,
    resolver: &TypeResolver<'_>,
    prompts: &BTreeMap<String, PromptDeclaration>,
) -> Result<TryStep, CompileError> {
    exact_keys(object, &["id", "try", "catch", "finally"])?;
    let catch = object_value(required(object, "catch")?, "catch declaration")?;
    exact_keys(catch, &["safe_business_failure"])?;
    let safe = object_value(
        required(catch, "safe_business_failure")?,
        "safe_business_failure handler",
    )?;
    exact_keys(safe, &["as", "steps"])?;
    let protected_steps = parse_steps(required(object, "try")?, resolver, prompts)?;
    let handler_steps = parse_steps(required(safe, "steps")?, resolver, prompts)?;
    if protected_steps.is_empty() || handler_steps.is_empty() {
        return Err(CompileError::new(
            INVALID_CONTROL_FLOW,
            "try and safe_business_failure handler must each contain at least one step",
        ));
    }
    Ok(TryStep {
        id: identifier(required(object, "id")?, "try id")?,
        protected_steps,
        error_name: safe
            .get("as")
            .map(|value| identifier(value, "caught error name"))
            .transpose()?
            .unwrap_or_else(|| "error".to_owned()),
        handler_steps,
        finally_steps: object
            .get("finally")
            .map(|value| parse_steps(value, resolver, prompts))
            .transpose()?
            .unwrap_or_default(),
    })
}

fn parse_human_task(
    object: &Map<String, Value>,
    resolver: &TypeResolver<'_>,
) -> Result<HumanTaskStep, CompileError> {
    exact_keys(object, &["id", "human_task"])?;
    let task = object_value(required(object, "human_task")?, "human_task declaration")?;
    exact_keys(
        task,
        &[
            "signal",
            "request",
            "response",
            "assignees",
            "candidate_groups",
            "claim_lease_ms",
        ],
    )?;
    Ok(HumanTaskStep {
        id: identifier(required(object, "id")?, "human_task id")?,
        signal_name: string(required(task, "signal")?, "human_task signal")?.to_owned(),
        payload_type: resolver.resolve_type_value(required(task, "response")?)?,
        request: parse_value_expr(required(task, "request")?)?,
        assignees: optional_string_list(task.get("assignees"), "human_task assignees")?,
        candidate_groups: optional_string_list(
            task.get("candidate_groups"),
            "human_task candidate_groups",
        )?,
        claim_lease_ms: task
            .get("claim_lease_ms")
            .map(|value| {
                value.as_u64().ok_or_else(|| {
                    CompileError::new(
                        INVALID_CONTROL_FLOW,
                        "human_task claim_lease_ms must be a positive integer",
                    )
                })
            })
            .transpose()?
            .unwrap_or(5 * 60 * 1_000),
    })
}

fn optional_string_list(
    value: Option<&Value>,
    label: &'static str,
) -> Result<Vec<String>, CompileError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let values = value.as_array().ok_or_else(|| {
        CompileError::new(INVALID_CONTROL_FLOW, "human task assignment must be a list")
    })?;
    values
        .iter()
        .map(|value| string(value, label).map(ToOwned::to_owned))
        .collect()
}

fn parse_wait(
    object: &Map<String, Value>,
    resolver: &TypeResolver<'_>,
) -> Result<WaitStep, CompileError> {
    exact_keys(object, &["id", "wait"])?;
    let id = identifier(required(object, "id")?, "wait id")?;
    let wait = object_value(required(object, "wait")?, "wait declaration")?;
    let kind = if wait.contains_key("signal") {
        exact_keys(wait, &["signal", "response"])?;
        WaitKind::Signal {
            name: string(required(wait, "signal")?, "signal name")?.to_owned(),
            payload_type: resolver.resolve_type_value(required(wait, "response")?)?,
        }
    } else if wait.contains_key("duration_ms") {
        exact_keys(wait, &["duration_ms"])?;
        WaitKind::Timer {
            duration_ms: parse_value_expr(required(wait, "duration_ms")?)?,
        }
    } else {
        return Err(CompileError::new(
            INVALID_STEP,
            "wait must declare either signal+response or duration_ms",
        ));
    };
    Ok(WaitStep { id, kind })
}

fn parse_steps(
    value: &Value,
    resolver: &TypeResolver<'_>,
    prompts: &BTreeMap<String, PromptDeclaration>,
) -> Result<Vec<Step>, CompileError> {
    value
        .as_array()
        .ok_or_else(|| CompileError::new(INVALID_STEP, "structured block must be a step list"))?
        .iter()
        .map(|value| parse_step(value, resolver, prompts))
        .collect()
}

fn positive_u32(value: &Value, label: &str) -> Result<u32, CompileError> {
    let value = positive_u64(value, label)?;
    u32::try_from(value)
        .map_err(|_| CompileError::new(INVALID_STEP, format!("{label} exceeds u32")))
}

fn positive_u64(value: &Value, label: &str) -> Result<u64, CompileError> {
    value
        .as_u64()
        .filter(|value| *value > 0)
        .ok_or_else(|| CompileError::new(INVALID_STEP, format!("{label} must be positive")))
}

pub fn parse_value_expr(value: &Value) -> Result<ValueExpr, CompileError> {
    if let Some(source) = value.as_str() {
        if let Some(name) = source.strip_prefix('$') {
            return Ok(ValueExpr::Reference(ValuePath::parse(name)?));
        }
        if source.contains("{{") || source.contains("}}") {
            return Ok(ValueExpr::Template(parse_template(source)?));
        }
        return Ok(ValueExpr::Literal(value.clone()));
    }
    if let Some(values) = value.as_array() {
        return values
            .iter()
            .map(parse_value_expr)
            .collect::<Result<Vec<_>, _>>()
            .map(ValueExpr::Array);
    }
    if let Some(object) = value.as_object() {
        if object.len() == 1
            && object.keys().next().is_some_and(|key| {
                matches!(
                    key.as_str(),
                    "from" | "literal" | "object" | "array" | "template"
                )
            })
        {
            return Err(CompileError::new(
                INVALID_STEP,
                "legacy value wrappers are forbidden in v3 natural YAML",
            ));
        }
        if object.contains_key("match") {
            exact_keys(object, &["match", "cases", "default"])?;
            let cases = object_value(required(object, "cases")?, "match cases")?;
            if cases.is_empty() {
                return Err(CompileError::new(
                    INVALID_STEP,
                    "match must declare at least one pure value case",
                ));
            }
            return Ok(ValueExpr::Match(MatchExpr {
                selector: Box::new(parse_value_expr(required(object, "match")?)?),
                cases: cases
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), parse_value_expr(value)?)))
                    .collect::<Result<_, CompileError>>()?,
                default: Box::new(parse_value_expr(required(object, "default")?)?),
            }));
        }
        return object
            .iter()
            .map(|(key, value)| Ok((key.clone(), parse_value_expr(value)?)))
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(ValueExpr::Object);
    }
    Ok(ValueExpr::Literal(value.clone()))
}

fn bind_declared_errors(steps: &mut [Step], errors: &BTreeMap<String, ErrorDeclaration>) {
    for step in steps {
        match step {
            Step::Raise(value) => {
                if let ValueExpr::Literal(Value::String(id)) = value {
                    if errors.contains_key(id) {
                        *value = ValueExpr::ErrorRef(id.clone());
                    }
                }
            }
            Step::If(value) => {
                bind_declared_errors(&mut value.then_steps, errors);
                for arm in &mut value.elif {
                    bind_declared_errors(&mut arm.steps, errors);
                }
                if let Some(steps) = &mut value.else_steps {
                    bind_declared_errors(steps, errors);
                }
            }
            Step::Parallel(value) => {
                for leg in &mut value.legs {
                    bind_declared_errors(&mut leg.steps, errors);
                }
            }
            Step::Map(value) => bind_declared_errors(&mut value.steps, errors),
            Step::Loop(value) => bind_declared_errors(&mut value.steps, errors),
            Step::Try(value) => {
                bind_declared_errors(&mut value.protected_steps, errors);
                bind_declared_errors(&mut value.handler_steps, errors);
                bind_declared_errors(&mut value.finally_steps, errors);
            }
            Step::Leaf(_)
            | Step::Call(_)
            | Step::HumanTask(_)
            | Step::Wait(_)
            | Step::Yield(_)
            | Step::Break(_)
            | Step::Continue(_)
            | Step::Return(_) => {}
        }
    }
}

fn parse_template(source: &str) -> Result<TextTemplate, CompileError> {
    let mut rest = source;
    let mut references = Vec::new();
    loop {
        let Some(open) = rest.find("{{") else {
            if rest.contains("}}") {
                return Err(CompileError::new(
                    INVALID_STEP,
                    "text template contains an unmatched closing delimiter",
                ));
            }
            break;
        };
        if rest[..open].contains("}}") {
            return Err(CompileError::new(
                INVALID_STEP,
                "text template contains an unmatched closing delimiter",
            ));
        }
        let after = &rest[open + 2..];
        let close = after.find("}}").ok_or_else(|| {
            CompileError::new(
                INVALID_STEP,
                "text template contains an unmatched opening delimiter",
            )
        })?;
        references.push(ValuePath::parse(after[..close].trim())?);
        rest = &after[close + 2..];
    }
    Ok(TextTemplate {
        source: source.to_owned(),
        references,
    })
}

fn contract(shape: PlanType) -> AuthorTypeContract {
    AuthorTypeContract {
        shape,
        constraints: BTreeMap::new(),
        nominal: None,
    }
}

fn message_contract() -> AuthorTypeContract {
    let text = PlanType::Object {
        properties: BTreeMap::from([(
            "text".to_owned(),
            PlanProperty::new(PlanType::String, true).expect("built-in Message text is valid"),
        )]),
        additional_properties: None,
    };
    let image = PlanType::Object {
        properties: BTreeMap::from([(
            "image_url".to_owned(),
            PlanProperty::new(PlanType::String, true).expect("built-in Message image URL is valid"),
        )]),
        additional_properties: None,
    };
    let content = PlanType::Array {
        items: Box::new(
            PlanType::union([text, image]).expect("built-in Message content union is valid"),
        ),
        min_items: 0,
    };
    let variants = ["user", "assistant"].map(|role| PlanType::Object {
        properties: BTreeMap::from([
            (
                "content".to_owned(),
                PlanProperty::new(content.clone(), true)
                    .expect("built-in Message content is valid"),
            ),
            (
                "role".to_owned(),
                PlanProperty::new(
                    PlanType::literal(Value::String(role.to_owned()))
                        .expect("built-in Message role is valid"),
                    true,
                )
                .expect("built-in Message role property is valid"),
            ),
        ]),
        additional_properties: None,
    });
    AuthorTypeContract {
        shape: PlanType::union(variants).expect("built-in Message union is valid"),
        constraints: BTreeMap::new(),
        nominal: Some("Message".to_owned()),
    }
}

fn parse_constraints(object: &Map<String, Value>) -> Result<TypeConstraints, CompileError> {
    let integer = |key: &str| -> Result<Option<u64>, CompileError> {
        object
            .get(key)
            .map(|value| {
                value.as_u64().ok_or_else(|| {
                    CompileError::new(
                        INVALID_TYPE,
                        format!("type constraint {key} must be a non-negative integer"),
                    )
                })
            })
            .transpose()
    };
    let min_items = integer("min_items")?;
    let max_items = integer("max_items")?;
    let min_length = integer("min_length")?;
    let max_length = integer("max_length")?;
    if min_items.zip(max_items).is_some_and(|(min, max)| min > max)
        || min_length
            .zip(max_length)
            .is_some_and(|(min, max)| min > max)
    {
        return Err(CompileError::new(
            INVALID_TYPE,
            "type constraint minimum cannot exceed its maximum",
        ));
    }
    let pattern = object
        .get("pattern")
        .map(|value| string(value, "type pattern").map(str::to_owned))
        .transpose()?;
    let enum_values = object
        .get("enum")
        .map(|value| {
            value.as_array().cloned().ok_or_else(|| {
                CompileError::new(INVALID_TYPE, "type enum must be a non-empty array")
            })
        })
        .transpose()?;
    if enum_values.as_ref().is_some_and(Vec::is_empty) {
        return Err(CompileError::new(
            INVALID_TYPE,
            "type enum must be a non-empty array",
        ));
    }
    Ok(TypeConstraints {
        min_items,
        max_items,
        min_length,
        max_length,
        pattern,
        enum_values,
    })
}

fn reject_dynamic_default(value: &Value) -> Result<(), CompileError> {
    match value {
        Value::String(value)
            if value.starts_with('$') || value.contains("{{") || value.contains("}}") =>
        {
            Err(CompileError::new(
                INVALID_TYPE,
                "input defaults must be static and cannot contain references or templates",
            ))
        }
        Value::Array(values) => {
            for value in values {
                reject_dynamic_default(value)?;
            }
            Ok(())
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
                    INVALID_TYPE,
                    "legacy value wrappers are forbidden in v3 defaults",
                ));
            }
            for value in values.values() {
                reject_dynamic_default(value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn parse_llm_contract(
    object: &Map<String, Value>,
    prompts: &BTreeMap<String, PromptDeclaration>,
) -> Result<LlmContract, CompileError> {
    const DEFAULT_MAX_ROUNDS: u32 = 8;
    const DEFAULT_MAX_CALLS: u32 = 32;

    if let Some(parameters) = object.get("parameters") {
        let parameters = object_value(parameters, "llm parameters")?;
        if parameters.contains_key("stream") {
            return Err(CompileError::new(
                INVALID_STEP,
                "llm stream is a top-level execution field and cannot appear in parameters",
            ));
        }
    }

    let stream = object
        .get("stream")
        .map(|value| boolean(value, "llm stream"))
        .transpose()?
        .unwrap_or(true);
    let publish = object
        .get("publish")
        .map(|value| boolean(value, "llm publish"))
        .transpose()?
        .unwrap_or(false);

    let mut seen_tools = BTreeSet::new();
    let tools = object
        .get("tools")
        .map(|value| {
            value
                .as_array()
                .ok_or_else(|| {
                    CompileError::new(INVALID_STEP, "llm tools must be an ordered list")
                })?
                .iter()
                .map(|value| {
                    let tool = identifier(value, "llm tool name")?;
                    if !seen_tools.insert(tool.clone()) {
                        return Err(CompileError::new(
                            INVALID_STEP,
                            format!("llm tools contains duplicate tool '{tool}'"),
                        ));
                    }
                    Ok(tool)
                })
                .collect::<Result<Vec<_>, CompileError>>()
        })
        .transpose()?
        .unwrap_or_default();

    let tool_choice = match object
        .get("tool_choice")
        .map(|value| string(value, "llm tool_choice"))
        .transpose()?
        .unwrap_or("auto")
    {
        "auto" => LlmToolChoice::Auto,
        "required" => LlmToolChoice::Required,
        tool => {
            validate_identifier(tool, "llm tool_choice")?;
            if !seen_tools.contains(tool) {
                return Err(CompileError::new(
                    INVALID_STEP,
                    format!("llm tool_choice '{tool}' is not present in tools"),
                ));
            }
            LlmToolChoice::Tool(tool.to_owned())
        }
    };
    if matches!(&tool_choice, LlmToolChoice::Required) && tools.is_empty() {
        return Err(CompileError::new(
            INVALID_STEP,
            "llm tool_choice required needs at least one declared tool",
        ));
    }

    let tool_limits = object
        .get("tool_limits")
        .map(|value| -> Result<LlmToolLimits, CompileError> {
            let limits = object_value(value, "llm tool_limits")?;
            exact_keys(limits, &["max_rounds", "max_calls"])?;
            Ok(LlmToolLimits {
                max_rounds: limits
                    .get("max_rounds")
                    .map(|value| positive_u32(value, "llm tool_limits.max_rounds"))
                    .transpose()?
                    .unwrap_or(DEFAULT_MAX_ROUNDS),
                max_calls: limits
                    .get("max_calls")
                    .map(|value| positive_u32(value, "llm tool_limits.max_calls"))
                    .transpose()?
                    .unwrap_or(DEFAULT_MAX_CALLS),
            })
        })
        .transpose()?
        .unwrap_or(LlmToolLimits {
            max_rounds: DEFAULT_MAX_ROUNDS,
            max_calls: DEFAULT_MAX_CALLS,
        });

    Ok(LlmContract {
        messages: parse_messages(required(object, "messages")?, prompts)?,
        stream,
        publish,
        tools,
        tool_choice,
        tool_limits,
    })
}

fn parse_messages(
    value: &Value,
    prompts: &BTreeMap<String, PromptDeclaration>,
) -> Result<Vec<MessageExpr>, CompileError> {
    let messages = value
        .as_array()
        .ok_or_else(|| CompileError::new(INVALID_STEP, "llm messages must be an ordered list"))?;
    let mut parsed = Vec::new();
    for message in messages {
        if let Some(reference) = message.as_str().and_then(|value| value.strip_prefix('$')) {
            parsed.push(MessageExpr::Splice(ValuePath::parse(reference)?));
            continue;
        }
        let message = object_value(message, "message must be {role, content} or $MessageList")?;
        exact_keys(message, &["role", "content"])?;
        let role = match string(required(message, "role")?, "message role")? {
            "system" => MessageRole::System,
            "user" => MessageRole::User,
            "assistant" => MessageRole::Assistant,
            _ => {
                return Err(CompileError::new(
                    INVALID_STEP,
                    "authored message role must be system, user, or assistant",
                ));
            }
        };
        let content = required(message, "content")?.as_array().ok_or_else(|| {
            CompileError::new(INVALID_STEP, "message content must be an ordered part list")
        })?;
        let mut parsed_content = Vec::new();
        for part in content {
            let part = object_value(part, "content part must be a single-key object")?;
            if part.len() != 1 {
                return Err(CompileError::new(
                    INVALID_STEP,
                    "content part must contain exactly one text or image_url field",
                ));
            }
            if let Some(text) = part.get("text") {
                let text = string(text, "text content")?;
                if let Some(reference) = text.strip_prefix('$') {
                    parsed_content.push(ContentPart::Text(TextContent::ValueRef(
                        ValuePath::parse(reference)?,
                    )));
                } else if prompts.contains_key(text) {
                    parsed_content.push(ContentPart::Text(TextContent::PromptRef(text.to_owned())));
                } else if text.contains("{{") || text.contains("}}") {
                    parsed_content.push(ContentPart::Text(TextContent::Template(parse_template(
                        text,
                    )?)));
                } else {
                    parsed_content.push(ContentPart::Text(TextContent::Literal(text.to_owned())));
                }
            } else if let Some(image) = part.get("image_url") {
                let image = string(image, "image_url content")?;
                if let Some(reference) = image.strip_prefix('$') {
                    parsed_content.push(ContentPart::ImageUrl(ImageUrlContent::ValueRef(
                        ValuePath::parse(reference)?,
                    )));
                } else if image.contains("{{") || image.contains("}}") {
                    return Err(CompileError::new(
                        INVALID_STEP,
                        "image_url does not support text templates",
                    ));
                } else {
                    parsed_content.push(ContentPart::ImageUrl(ImageUrlContent::Literal(
                        image.to_owned(),
                    )));
                }
            } else {
                return Err(CompileError::new(
                    INVALID_STEP,
                    "content part must contain text or image_url",
                ));
            }
        }
        parsed.push(MessageExpr::Message {
            role,
            content: parsed_content,
        });
    }
    Ok(parsed)
}

fn validate_reference_path(value: &str) -> Result<(), CompileError> {
    if value.is_empty()
        || value
            .split('.')
            .any(|part| validate_identifier(part, "reference").is_err())
    {
        Err(CompileError::new(
            INVALID_STEP,
            "runtime reference must be $name or $name.field using stable identifiers",
        ))
    } else {
        Ok(())
    }
}

fn valid_error_code(value: &str) -> bool {
    let mut bytes = value.bytes();
    value.len() <= 128
        && bytes.next().is_some_and(|byte| byte.is_ascii_uppercase())
        && bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn validate_global_ids<'a>(
    steps: &'a [Step],
    inputs: impl Iterator<Item = &'a String>,
) -> Result<(), CompileError> {
    let mut seen = inputs.cloned().collect::<BTreeSet<_>>();
    walk_ids(steps, &mut |id| {
        if !seen.insert(id.to_owned()) {
            return Err(CompileError::new(
                INVALID_STEP,
                format!("authored id '{id}' is duplicated or shadows an input"),
            ));
        }
        Ok(())
    })
}

fn walk_ids(
    steps: &[Step],
    visitor: &mut impl FnMut(&str) -> Result<(), CompileError>,
) -> Result<(), CompileError> {
    for step in steps {
        match step {
            Step::Leaf(value) => visitor(&value.id)?,
            Step::Wait(value) => visitor(&value.id)?,
            Step::HumanTask(value) => visitor(&value.id)?,
            Step::Call(value) => visitor(&value.id)?,
            Step::If(value) => {
                visitor(&value.id)?;
                let mut arm_ids = BTreeSet::from(["then".to_owned(), "else".to_owned()]);
                for arm in &value.elif {
                    validate_identifier(&arm.id, "elif id")?;
                    if !arm_ids.insert(arm.id.clone()) {
                        return Err(CompileError::new(
                            INVALID_STEP,
                            "elif ids must be unique and cannot be then or else",
                        ));
                    }
                    walk_ids(&arm.steps, visitor)?;
                }
                walk_ids(&value.then_steps, visitor)?;
                if let Some(steps) = &value.else_steps {
                    walk_ids(steps, visitor)?;
                }
            }
            Step::Parallel(value) => {
                visitor(&value.id)?;
                for leg in &value.legs {
                    walk_ids(&leg.steps, visitor)?;
                }
            }
            Step::Map(value) => {
                visitor(&value.id)?;
                walk_ids(&value.steps, visitor)?;
            }
            Step::Loop(value) => {
                visitor(&value.id)?;
                walk_ids(&value.steps, visitor)?;
            }
            Step::Try(value) => {
                visitor(&value.id)?;
                walk_ids(&value.protected_steps, visitor)?;
                walk_ids(&value.handler_steps, visitor)?;
                walk_ids(&value.finally_steps, visitor)?;
            }
            Step::Yield(_)
            | Step::Break(_)
            | Step::Continue(_)
            | Step::Return(_)
            | Step::Raise(_) => {}
        }
    }
    Ok(())
}

struct TypeResolver<'a> {
    declarations: &'a BTreeMap<String, Value>,
}

impl<'a> TypeResolver<'a> {
    fn new(declarations: &'a BTreeMap<String, Value>) -> Result<Self, CompileError> {
        for name in declarations.keys() {
            validate_identifier(name, "type name")?;
            if name == "Message" {
                return Err(CompileError::new(
                    INVALID_TYPE,
                    "Message is a platform type and must not be redefined by an Agent",
                ));
            }
            if !name
                .chars()
                .next()
                .is_some_and(|character| character.is_ascii_uppercase())
            {
                return Err(CompileError::new(
                    INVALID_TYPE,
                    "named types must begin with an uppercase ASCII letter",
                ));
            }
        }
        Ok(Self { declarations })
    }

    fn resolve_all(&self) -> Result<BTreeMap<String, AuthorTypeContract>, CompileError> {
        self.declarations
            .keys()
            .map(|name| Ok((name.clone(), self.resolve_named(name, &mut Vec::new())?)))
            .collect()
    }

    fn resolve_type_value(&self, value: &Value) -> Result<AuthorTypeContract, CompileError> {
        let source = value
            .as_str()
            .ok_or_else(|| CompileError::new(INVALID_TYPE, "type expression must be a string"))?;
        self.resolve_source(source, &mut Vec::new())
    }

    fn resolve_source(
        &self,
        source: &str,
        stack: &mut Vec<String>,
    ) -> Result<AuthorTypeContract, CompileError> {
        if source.trim() != source || source.is_empty() {
            return Err(CompileError::new(
                INVALID_TYPE,
                "type expression must be a canonical non-empty string",
            ));
        }
        if let Some(item) = source.strip_suffix("[]") {
            let item = self.resolve_source(item, stack)?;
            let constraints = item
                .constraints
                .into_iter()
                .map(|(mut path, value)| {
                    path.insert(0, "[]".to_owned());
                    (path, value)
                })
                .collect();
            return Ok(AuthorTypeContract {
                shape: PlanType::Array {
                    items: Box::new(item.shape),
                    min_items: 0,
                },
                constraints,
                nominal: None,
            });
        }
        match source {
            "string" => Ok(contract(PlanType::String)),
            "boolean" => Ok(contract(PlanType::Boolean)),
            "integer" => Ok(contract(PlanType::Integer)),
            "number" => Ok(contract(PlanType::Number)),
            "null" => Ok(contract(PlanType::Null)),
            "any" => Ok(contract(PlanType::Any)),
            "Message" => Ok(message_contract()),
            name => self.resolve_named(name, stack),
        }
    }

    fn resolve_named(
        &self,
        name: &str,
        stack: &mut Vec<String>,
    ) -> Result<AuthorTypeContract, CompileError> {
        if stack.iter().any(|value| value == name) {
            return Err(CompileError::new(
                INVALID_TYPE,
                "recursive named types are not part of the v3 Core type profile",
            ));
        }
        if name == "Message" {
            return Ok(message_contract());
        }
        let declaration = self.declarations.get(name).ok_or_else(|| {
            CompileError::new(INVALID_TYPE, format!("unknown named type '{name}'"))
        })?;
        stack.push(name.to_owned());
        let result = if let Some(alias) = declaration.as_str() {
            self.resolve_source(alias, stack)
        } else {
            let object = as_object(declaration, "named type must be an alias or object")?;
            if object.contains_key("fields") {
                exact_keys(object, &["fields"])?;
                let fields = object_value(required(object, "fields")?, "type fields")?;
                let mut properties = BTreeMap::new();
                let mut constraints = BTreeMap::new();
                for (field, declaration) in fields {
                    validate_identifier(field, "field name")?;
                    let (mut value_type, field_constraints) =
                        if let Some(source) = declaration.as_str() {
                            (
                                self.resolve_source(source, stack)?,
                                TypeConstraints::default(),
                            )
                        } else {
                            let declaration = as_object(declaration, "field declaration")?;
                            exact_keys(
                                declaration,
                                &[
                                    "type",
                                    "min_items",
                                    "max_items",
                                    "min_length",
                                    "max_length",
                                    "pattern",
                                    "enum",
                                ],
                            )?;
                            let value_type = declaration
                                .get("type")
                                .and_then(Value::as_str)
                                .ok_or_else(|| {
                                    CompileError::new(INVALID_TYPE, "field type must be a string")
                                })?;
                            (
                                self.resolve_source(value_type, stack)?,
                                parse_constraints(declaration)?,
                            )
                        };
                    if field_constraints != TypeConstraints::default() {
                        value_type.constraints.insert(Vec::new(), field_constraints);
                    }
                    for (path, value) in value_type.constraints {
                        let mut qualified = Vec::with_capacity(path.len() + 1);
                        qualified.push(field.clone());
                        qualified.extend(path);
                        constraints.insert(qualified, value);
                    }
                    properties.insert(
                        field.clone(),
                        PlanProperty::new(value_type.shape, true).map_err(|failure| {
                            CompileError::new(INVALID_TYPE, failure.to_string())
                        })?,
                    );
                }
                Ok(AuthorTypeContract {
                    shape: PlanType::Object {
                        properties,
                        additional_properties: None,
                    },
                    constraints,
                    nominal: Some(name.to_owned()),
                })
            } else {
                exact_keys(
                    object,
                    &[
                        "type",
                        "min_items",
                        "max_items",
                        "min_length",
                        "max_length",
                        "pattern",
                        "enum",
                    ],
                )?;
                let source = string(required(object, "type")?, "type alias")?;
                let mut value = self.resolve_source(source, stack)?;
                let constraints = parse_constraints(object)?;
                if constraints != TypeConstraints::default() {
                    if let (PlanType::Array { min_items, .. }, Some(minimum)) =
                        (&mut value.shape, constraints.min_items)
                    {
                        *min_items = minimum;
                    }
                    value.constraints.insert(Vec::new(), constraints);
                }
                value.nominal = Some(name.to_owned());
                Ok(value)
            }
        };
        stack.pop();
        let mut result = result?;
        result.shape = result
            .shape
            .normalized()
            .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))?;
        if result.nominal.is_none() {
            result.nominal = Some(name.to_owned());
        }
        Ok(result)
    }
}

fn literal_type(value: &Value) -> Result<PlanType, CompileError> {
    PlanType::literal(value.clone())
        .map_err(|failure| CompileError::new(INVALID_TYPE, failure.to_string()))
}

fn validate_identifier(value: &str, label: &str) -> Result<(), CompileError> {
    let mut bytes = value.bytes();
    let valid = value.len() <= 64
        && bytes
            .next()
            .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
        && bytes.all(|byte| byte == b'_' || byte == b'-' || byte.is_ascii_alphanumeric());
    if valid {
        Ok(())
    } else {
        Err(CompileError::new(
            INVALID_DOCUMENT,
            format!("{label} must be a stable ASCII identifier of at most 64 bytes"),
        ))
    }
}

fn identifier(value: &Value, label: &str) -> Result<String, CompileError> {
    let value = string(value, label)?;
    validate_identifier(value, label)?;
    Ok(value.to_owned())
}

fn string<'a>(value: &'a Value, label: &str) -> Result<&'a str, CompileError> {
    value
        .as_str()
        .ok_or_else(|| CompileError::new(INVALID_STEP, format!("{label} must be a string")))
}

fn boolean(value: &Value, label: &str) -> Result<bool, CompileError> {
    value
        .as_bool()
        .ok_or_else(|| CompileError::new(INVALID_STEP, format!("{label} must be a boolean")))
}

fn as_object<'a>(
    value: &'a Value,
    message: &'static str,
) -> Result<&'a Map<String, Value>, CompileError> {
    value
        .as_object()
        .ok_or_else(|| CompileError::new(INVALID_STEP, message))
}

fn object_value<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>, CompileError> {
    value
        .as_object()
        .ok_or_else(|| CompileError::new(INVALID_STEP, format!("{label} must be an object")))
}

fn required<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a Value, CompileError> {
    object.get(field).ok_or_else(|| {
        CompileError::new(INVALID_STEP, format!("required field '{field}' is missing"))
    })
}

fn exact_keys(object: &Map<String, Value>, allowed: &[&str]) -> Result<(), CompileError> {
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(CompileError::new(
            INVALID_STEP,
            format!("unknown v3 field '{key}'"),
        ));
    }
    Ok(())
}

fn error(code: &'static str, message: &'static str, path: DslPath) -> CompileError {
    CompileError::new(code, message).with_path(path)
}

#[cfg(test)]
mod tests {
    use super::{parse_value_expr, validate, Step, ValueExpr};
    use crate::v3::raw::parse;
    use serde_json::json;

    #[test]
    fn match_is_a_pure_value_and_cannot_contain_steps() {
        assert!(matches!(
            parse_value_expr(&json!({
                "match": "follow_up",
                "cases": {"follow_up": "conversation"},
                "default": "report"
            }))
            .unwrap(),
            ValueExpr::Match(_)
        ));
        assert!(parse_value_expr(&json!({
            "match": "follow_up",
            "cases": {"follow_up": "conversation"},
            "default": "report",
            "steps": []
        }))
        .is_err());
    }

    #[test]
    fn validates_clean_break_root_and_leaf_surface() {
        let raw = parse(
            r#"api_version: insight.agent/v3
kind: agent
inputs:
  question: string
output: string
workflow:
  steps:
    - id: answer
      type: action
      call: fixture.answer
      inputs: {question: $question}
      response: string
    - return: $answer
"#,
        )
        .unwrap();
        let document = validate(raw).unwrap();
        assert!(matches!(document.steps[0], Step::Leaf(_)));
    }
}
