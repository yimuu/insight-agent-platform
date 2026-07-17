use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{
    de::Error as _, ser::SerializeMap as _, Deserialize, Deserializer, Serialize, Serializer,
};
use serde_json::{Map, Value};
use yaml_rust2::{
    parser::{Event, MarkedEventReceiver, Parser},
    scanner::{Marker, Scanner, TScalarStyle, TokenType},
};

use crate::dsl::{DslParseError, DslPath, SourceSpan};

use super::{
    message::{MessageListExpr, ResponseConfig},
    value::{Identifier, ValueExpr},
};

pub const PARSE_ERROR_CODE: &str = "VNEXT_AGENT_PARSE_FAILED";
const PARSE_ERROR_MESSAGE: &str = "failed to parse the vNext agent document";
pub const LLM_RESPONSE_CONFIG_INVALID: &str = "VNEXT_LLM_RESPONSE_CONFIG_INVALID";
const LLM_RESPONSE_CONFIG_INVALID_MESSAGE: &str = "LLM response configuration is invalid";
pub const ERROR_CODE_MAX_CHARS: usize = 128;
pub const ERROR_PUBLIC_MESSAGE_MAX_CHARS: usize = 512;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum ApiVersion {
    #[serde(rename = "insight.agent/v2")]
    V2,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DocumentKind {
    Agent,
}

/// The root authored vNext workflow document.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RawWorkflow {
    pub api_version: ApiVersion,
    pub kind: DocumentKind,
    pub metadata: Metadata,
    pub schema_dialect: String,
    #[serde(default, rename = "$defs")]
    pub definitions: BTreeMap<Identifier, Value>,
    #[serde(default)]
    pub prompts: BTreeMap<Identifier, PromptDeclaration>,
    #[serde(default)]
    pub errors: BTreeMap<Identifier, ErrorDeclaration>,
    pub input: InputContract,
    pub output: OutputContract,
    pub workflow: WorkflowBody,
}

/// A parsed workflow plus authored source locations. The source map is kept out
/// of runtime IR and events; it exists only for compile diagnostics and
/// controlled developer tooling.
#[derive(Debug, Clone, PartialEq)]
pub struct SpannedRawDocument {
    pub raw_ast: RawWorkflow,
    pub source_map: BTreeMap<DslPath, SourceSpan>,
}

impl SpannedRawDocument {
    pub fn into_parts(self) -> (RawWorkflow, BTreeMap<DslPath, SourceSpan>) {
        (self.raw_ast, self.source_map)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub id: Identifier,
    pub name: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct InputContract {
    pub schema: Value,
}

/// The schema applies to the stable RunOutput `data` field. The platform owns
/// the outer content/format/data envelope.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OutputContract {
    pub data_schema: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptDeclaration {
    Inline(String),
    File(String),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PromptDeclarationWire {
    Inline(InlinePromptWire),
    File(FilePromptWire),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InlinePromptWire {
    inline: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePromptWire {
    file: String,
}

impl<'de> Deserialize<'de> for PromptDeclaration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match PromptDeclarationWire::deserialize(deserializer)? {
            PromptDeclarationWire::Inline(value) => Self::Inline(value.inline),
            PromptDeclarationWire::File(value) => Self::File(value.file),
        })
    }
}

impl Serialize for PromptDeclaration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Inline(value) => serialize_single_entry(serializer, "inline", value),
            Self::File(value) => serialize_single_entry(serializer, "file", value),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    Workflow,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ErrorDeclaration {
    pub category: ErrorCategory,
    #[serde(deserialize_with = "deserialize_error_code")]
    pub code: String,
    #[serde(deserialize_with = "deserialize_error_public_message")]
    pub public_message: String,
}

pub fn is_valid_error_code(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_uppercase())
        && value.chars().count() <= ERROR_CODE_MAX_CHARS
        && bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

pub fn is_valid_error_public_message(value: &str) -> bool {
    !value.trim().is_empty()
        && !value.contains('\0')
        && value.chars().count() <= ERROR_PUBLIC_MESSAGE_MAX_CHARS
}

fn deserialize_error_code<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if is_valid_error_code(&value) {
        Ok(value)
    } else {
        Err(D::Error::custom(
            "workflow error code does not satisfy its closed profile",
        ))
    }
}

fn deserialize_error_public_message<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if is_valid_error_public_message(&value) {
        Ok(value)
    } else {
        Err(D::Error::custom(
            "workflow public error message does not satisfy its bounded profile",
        ))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkflowBody {
    #[serde(default)]
    pub steps: Vec<Step>,
    pub result: RootResult,
}

/// LLM, action, parallel, and switch are deliberately one internally tagged
/// union. Every variant is a closed authored contract; the generic internal
/// operation/config surface is intentionally not part of this AST.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Step {
    Llm {
        id: Identifier,
        model: String,
        #[serde(default)]
        inputs: BTreeMap<Identifier, ValueExpr>,
        messages: MessageListExpr,
        #[serde(default)]
        parameters: Map<String, Value>,
        response: ResponseConfig,
    },
    Action {
        id: Identifier,
        call: String,
        #[serde(default)]
        inputs: BTreeMap<Identifier, ValueExpr>,
    },
    Parallel {
        id: Identifier,
        #[serde(default)]
        inputs: BTreeMap<Identifier, ValueExpr>,
        settle: ParallelSettle,
        #[serde(default)]
        max_concurrency: Option<usize>,
        branches: BTreeMap<Identifier, ParallelBranch>,
    },
    Switch {
        id: Identifier,
        #[serde(default)]
        inputs: BTreeMap<Identifier, ValueExpr>,
        output_schema: Value,
        cases: Vec<SwitchCase>,
        default: SwitchDefault,
    },
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParallelSettle {
    /// Every branch must succeed; the first settleable failure propagates.
    All,
    /// Every branch settles into a typed `Result<T, BranchError>` envelope.
    AllSettled,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ParallelBranch {
    pub output_schema: Value,
    #[serde(default)]
    pub steps: Vec<Step>,
    pub result: BlockResult,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Cel(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PredicateWire {
    cel: String,
}

impl<'de> Deserialize<'de> for Predicate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        PredicateWire::deserialize(deserializer).map(|value| Self::Cel(value.cel))
    }
}

impl Serialize for Predicate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Cel(value) => serialize_single_entry(serializer, "cel", value),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SwitchCase {
    pub id: Identifier,
    pub when: Predicate,
    #[serde(default)]
    pub steps: Vec<Step>,
    pub result: BlockResult,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SwitchDefault {
    pub id: Identifier,
    #[serde(default)]
    pub steps: Vec<Step>,
    pub result: BlockResult,
}

/// A child block has exactly one normal return or authored workflow raise.
#[derive(Debug, Clone, PartialEq)]
pub enum BlockResult {
    Return(ValueExpr),
    Raise(Identifier),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum BlockResultWire {
    Return(BlockReturnWire),
    Raise(BlockRaiseWire),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BlockReturnWire {
    r#return: ValueExpr,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BlockRaiseWire {
    raise: Identifier,
}

impl<'de> Deserialize<'de> for BlockResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match BlockResultWire::deserialize(deserializer)? {
            BlockResultWire::Return(value) => Self::Return(value.r#return),
            BlockResultWire::Raise(value) => Self::Raise(value.raise),
        })
    }
}

impl Serialize for BlockResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Return(value) => serialize_single_entry(serializer, "return", value),
            Self::Raise(value) => serialize_single_entry(serializer, "raise", value),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputFormat {
    Text,
    Markdown,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RootReturn {
    #[serde(default)]
    pub content: Option<ValueExpr>,
    #[serde(default)]
    pub format: Option<OutputFormat>,
    pub data: ValueExpr,
}

/// The workflow root ends in one public success return or authored failure.
#[derive(Debug, Clone, PartialEq)]
pub enum RootResult {
    Return(RootReturn),
    Raise(Identifier),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RootResultWire {
    Return(RootReturnWire),
    Raise(RootRaiseWire),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RootReturnWire {
    r#return: RootReturn,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RootRaiseWire {
    raise: Identifier,
}

impl<'de> Deserialize<'de> for RootResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match RootResultWire::deserialize(deserializer)? {
            RootResultWire::Return(value) => Self::Return(value.r#return),
            RootResultWire::Raise(value) => Self::Raise(value.raise),
        })
    }
}

impl Serialize for RootResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Return(value) => serialize_single_entry(serializer, "return", value),
            Self::Raise(value) => serialize_single_entry(serializer, "raise", value),
        }
    }
}

fn serialize_single_entry<S, T>(
    serializer: S,
    key: &'static str,
    value: &T,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize + ?Sized,
{
    let mut map = serializer.serialize_map(Some(1))?;
    map.serialize_entry(key, value)?;
    map.end()
}

pub fn parse_workflow(source: &str) -> Result<SpannedRawDocument, DslParseError> {
    let raw_ast = match yaml_serde::from_str(source) {
        Ok(raw_ast) => raw_ast,
        Err(error) => {
            return Err(match SpannedSyntax::parse(source) {
                Ok(syntax) => parse_error(source, error, &syntax),
                Err(syntax_error) if syntax_error.path().is_some_and(|path| !path.is_root()) => {
                    syntax_error
                }
                Err(_) => basic_parse_error(source, error),
            });
        }
    };
    let syntax = SpannedSyntax::parse(source)?;
    Ok(SpannedRawDocument {
        raw_ast,
        source_map: syntax.source_map,
    })
}

fn parse_error(source: &str, error: yaml_serde::Error, syntax: &SpannedSyntax) -> DslParseError {
    let rendered = error.to_string();
    let base_path = serde_error_path_from_rendered(&rendered).unwrap_or_default();
    let message = serde_error_message(&rendered);

    let error_byte = error.location().map(|location| location.index());
    let (path, span) = if let Some(field) = diagnostic_field(message, "unknown field `") {
        let field_path = syntax
            .field_path_under(&base_path, &field, error_byte)
            .unwrap_or_else(|| base_path.child_key(field));
        let span = syntax
            .key_spans
            .get(&field_path)
            .or_else(|| syntax.source_map.get(&field_path))
            .copied();
        (field_path, span)
    } else if let Some(field) = diagnostic_field(message, "duplicate field `") {
        let field_path = base_path.child_key(field);
        let span = syntax
            .key_spans
            .get(&field_path)
            .or_else(|| syntax.source_map.get(&field_path))
            .copied();
        (field_path, span)
    } else if diagnostic_field(message, "missing field `").is_some() {
        let span = syntax.source_map.get(&base_path).copied();
        (base_path, span)
    } else if message.contains("did not match any variant of untagged enum") {
        if let Some(path) = syntax.invalid_union_path(&base_path) {
            let span = syntax.source_map.get(&path).copied();
            (path, span)
        } else {
            error
                .location()
                .and_then(|location| syntax.most_specific_path_at_byte(location.index()))
                .filter(|(path, _)| {
                    path.segments().starts_with(base_path.segments())
                        && path.segments().len() >= base_path.segments().len()
                })
                .map_or_else(
                    || {
                        (
                            base_path.clone(),
                            syntax.source_map.get(&base_path).copied(),
                        )
                    },
                    |(path, span)| (path, Some(span)),
                )
        }
    } else if message.contains("unknown variant `") {
        let discriminator = if base_path.segments().last().is_some_and(
            |segment| matches!(segment, crate::dsl::DslPathSegment::Key(key) if key == "kind"),
        ) {
            base_path.clone()
        } else {
            base_path.child_key("kind")
        };
        if let Some(span) = syntax.source_map.get(&discriminator).copied() {
            (discriminator, Some(span))
        } else if let Some((path, span)) = error
            .location()
            .and_then(|location| syntax.most_specific_path_at_byte(location.index()))
        {
            (path, Some(span))
        } else {
            (
                base_path.clone(),
                syntax.source_map.get(&base_path).copied(),
            )
        }
    } else {
        let span = syntax.source_map.get(&base_path).copied().or_else(|| {
            error
                .location()
                .map(|location| SourceSpan::point(source, location.index()))
        });
        (base_path, span)
    };

    if let Some(response_path) = invalid_llm_response_path(source) {
        let response_span = nearest_syntax_span(syntax, &response_path).or(span);
        return DslParseError::new(
            LLM_RESPONSE_CONFIG_INVALID,
            LLM_RESPONSE_CONFIG_INVALID_MESSAGE,
        )
        .at(response_path, response_span);
    }

    let response_path = path_contains_key(&path, "response")
        || error_byte
            .and_then(|byte| syntax.most_specific_path_at_byte(byte))
            .is_some_and(|(path, _)| path_contains_key(&path, "response"));
    let (code, safe_message) = if response_path {
        (
            LLM_RESPONSE_CONFIG_INVALID,
            LLM_RESPONSE_CONFIG_INVALID_MESSAGE,
        )
    } else {
        (PARSE_ERROR_CODE, PARSE_ERROR_MESSAGE)
    };
    DslParseError::new(code, safe_message).at(path, span)
}

fn path_contains_key(path: &DslPath, expected: &str) -> bool {
    path.segments()
        .iter()
        .any(|segment| matches!(segment, crate::dsl::DslPathSegment::Key(key) if key == expected))
}

fn nearest_syntax_span(syntax: &SpannedSyntax, path: &DslPath) -> Option<SourceSpan> {
    (0..=path.segments().len()).rev().find_map(|length| {
        let ancestor = DslPath::from_segments(path.segments()[..length].iter().cloned());
        syntax.source_map.get(&ancestor).copied()
    })
}

fn invalid_llm_response_path(source: &str) -> Option<DslPath> {
    let document = yaml_serde::from_str::<Value>(source).ok()?;
    let steps = document.get("workflow")?.get("steps")?.as_array()?;
    invalid_response_in_steps(
        steps,
        &DslPath::root().child_key("workflow").child_key("steps"),
    )
}

fn invalid_response_in_steps(steps: &[Value], base: &DslPath) -> Option<DslPath> {
    for (index, step) in steps.iter().enumerate() {
        let step_path = base.child_index(index);
        let Some(step) = step.as_object() else {
            continue;
        };
        match step.get("kind").and_then(Value::as_str) {
            Some("llm") => {
                let response_path = step_path.child_key("response");
                let Some(response) = step.get("response").and_then(Value::as_object) else {
                    return Some(response_path);
                };
                let format = response.get("format").and_then(Value::as_str);
                let valid = matches!(format, Some("text")) && response.len() == 1
                    || matches!(format, Some("json"))
                        && response.len() == 2
                        && response.contains_key("schema");
                if !valid {
                    if let Some(unknown) = response
                        .keys()
                        .find(|key| !matches!(key.as_str(), "format" | "schema"))
                    {
                        return Some(response_path.child_key(unknown));
                    }
                    return Some(response_path);
                }
            }
            Some("parallel") => {
                if let Some(branches) = step.get("branches").and_then(Value::as_object) {
                    for (branch, body) in branches {
                        let Some(children) = body.get("steps").and_then(Value::as_array) else {
                            continue;
                        };
                        let children_path = step_path
                            .child_key("branches")
                            .child_key(branch)
                            .child_key("steps");
                        if let Some(path) = invalid_response_in_steps(children, &children_path) {
                            return Some(path);
                        }
                    }
                }
            }
            Some("switch") => {
                if let Some(cases) = step.get("cases").and_then(Value::as_array) {
                    for (case_index, case) in cases.iter().enumerate() {
                        let Some(children) = case.get("steps").and_then(Value::as_array) else {
                            continue;
                        };
                        let children_path = step_path
                            .child_key("cases")
                            .child_index(case_index)
                            .child_key("steps");
                        if let Some(path) = invalid_response_in_steps(children, &children_path) {
                            return Some(path);
                        }
                    }
                }
                if let Some(children) = step
                    .get("default")
                    .and_then(|default| default.get("steps"))
                    .and_then(Value::as_array)
                {
                    let children_path = step_path.child_key("default").child_key("steps");
                    if let Some(path) = invalid_response_in_steps(children, &children_path) {
                        return Some(path);
                    }
                }
            }
            _ => {}
        }
    }
    None
}

fn basic_parse_error(source: &str, error: yaml_serde::Error) -> DslParseError {
    let path = serde_error_path_from_rendered(&error.to_string()).unwrap_or_default();
    let span = error
        .location()
        .map(|location| SourceSpan::point(source, location.index()));
    DslParseError::new(PARSE_ERROR_CODE, PARSE_ERROR_MESSAGE).at(path, span)
}

fn serde_error_path_from_rendered(rendered: &str) -> Option<DslPath> {
    let (path, _) = rendered.split_once(": ")?;
    DslPath::from_serde_path(path)
}

fn serde_error_message(rendered: &str) -> &str {
    rendered
        .split_once(": ")
        .map_or(rendered, |(_, message)| message)
}

fn diagnostic_field(message: &str, prefix: &str) -> Option<String> {
    let rest = message.strip_prefix(prefix)?;
    let (field, _) = rest.split_once('`')?;
    (!field.is_empty()).then(|| field.to_string())
}

#[derive(Debug)]
struct SpannedSyntax {
    source_map: BTreeMap<DslPath, SourceSpan>,
    key_spans: BTreeMap<DslPath, SourceSpan>,
}

impl SpannedSyntax {
    fn parse(source: &str) -> Result<Self, DslParseError> {
        let index = SourceIndex::new(source);
        let lexical = LexicalIndex::scan(source, &index)?;
        let mut receiver = EventCollector::default();
        let mut parser = Parser::new_from_str(source);
        if let Err(error) = parser.load(&mut receiver, true) {
            let byte = index.byte_at_marker(error.marker());
            return Err(safe_syntax_error(source, DslPath::root(), byte));
        }

        let root = SyntaxParser::new(&receiver.events, &lexical, source, &index).parse()?;
        let mut source_map = BTreeMap::from([(DslPath::root(), SourceSpan::document(source))]);
        let mut key_spans = BTreeMap::new();
        collect_source_map(
            source,
            &index,
            &root,
            &DslPath::root(),
            &mut source_map,
            &mut key_spans,
        )?;
        Ok(Self {
            source_map,
            key_spans,
        })
    }

    fn most_specific_path_at_byte(&self, byte: usize) -> Option<(DslPath, SourceSpan)> {
        self.source_map
            .iter()
            .filter(|(_, span)| span.byte_start() <= byte as u64 && (byte as u64) < span.byte_end())
            .max_by_key(|(path, span)| {
                (
                    path.segments().len(),
                    std::cmp::Reverse(span.byte_end().saturating_sub(span.byte_start())),
                )
            })
            .map(|(path, span)| (path.clone(), *span))
    }

    fn field_path_under(
        &self,
        parent: &DslPath,
        field: &str,
        error_byte: Option<usize>,
    ) -> Option<DslPath> {
        self.key_spans
            .iter()
            .filter(|(path, _)| {
                path.segments().starts_with(parent.segments())
                    && path.segments().last().is_some_and(|segment| {
                        matches!(segment, crate::dsl::DslPathSegment::Key(key) if key == field)
                    })
            })
            .min_by_key(|(path, span)| {
                let distance = error_byte.map_or(0, |byte| {
                    let byte = byte as u64;
                    if byte < span.byte_start() {
                        span.byte_start() - byte
                    } else {
                        byte.saturating_sub(span.byte_end())
                    }
                });
                (distance, std::cmp::Reverse(path.segments().len()))
            })
            .map(|(path, _)| path.clone())
    }

    fn invalid_union_path(&self, base: &DslPath) -> Option<DslPath> {
        let mut fields_by_parent = BTreeMap::<DslPath, BTreeSet<&str>>::new();
        for path in self.key_spans.keys().filter(|path| {
            path.segments().starts_with(base.segments()) && !path.segments().is_empty()
        }) {
            let Some(crate::dsl::DslPathSegment::Key(field)) = path.segments().last() else {
                continue;
            };
            let parent = DslPath::from_segments(
                path.segments()[..path.segments().len() - 1].iter().cloned(),
            );
            fields_by_parent
                .entry(parent)
                .or_default()
                .insert(field.as_str());
        }
        fields_by_parent
            .into_iter()
            .filter(|(parent, _)| parent.segments().starts_with(base.segments()))
            .filter(|(parent, fields)| {
                let prompt_declaration = parent.segments().len() == 2
                    && matches!(
                        &parent.segments()[0],
                        crate::dsl::DslPathSegment::Key(key) if key == "prompts"
                    )
                    && fields.contains("inline")
                    && fields.contains("file");
                let authored_result = parent.segments().last().is_some_and(|segment| {
                    matches!(segment, crate::dsl::DslPathSegment::Key(key) if key == "result")
                }) && fields.contains("return")
                    && fields.contains("raise");
                prompt_declaration || authored_result
            })
            .min_by_key(|(parent, _)| parent.segments().len())
            .map(|(parent, _)| parent)
    }
}

#[derive(Debug)]
struct SourceIndex {
    char_to_byte: Vec<usize>,
    char_to_position: Vec<(u32, u32)>,
}

impl SourceIndex {
    fn new(source: &str) -> Self {
        let mut char_to_byte = Vec::with_capacity(source.chars().count() + 1);
        let mut char_to_position = Vec::with_capacity(char_to_byte.capacity());
        let mut line = 1u32;
        let mut column = 1u32;
        for (byte, character) in source.char_indices() {
            char_to_byte.push(byte);
            char_to_position.push((line, column));
            if character == '\n' {
                line = line.saturating_add(1);
                column = 1;
            } else {
                column = column.saturating_add(1);
            }
        }
        char_to_byte.push(source.len());
        char_to_position.push((line, column));
        Self {
            char_to_byte,
            char_to_position,
        }
    }

    fn byte(&self, character: usize) -> usize {
        self.char_to_byte
            .get(character)
            .copied()
            .unwrap_or_else(|| *self.char_to_byte.last().expect("source index has EOF"))
    }

    fn character_at_marker(&self, marker: &Marker) -> usize {
        let line = u32::try_from(marker.line()).unwrap_or(u32::MAX);
        let column = u32::try_from(marker.col())
            .unwrap_or(u32::MAX)
            .saturating_add(1);
        self.char_to_position
            .binary_search(&(line, column))
            .unwrap_or_else(|_| marker.index().min(self.char_to_byte.len() - 1))
    }

    fn byte_at_marker(&self, marker: &Marker) -> usize {
        self.byte(self.character_at_marker(marker))
    }

    fn span(&self, start: usize, end: usize) -> SourceSpan {
        let last = self.char_to_byte.len() - 1;
        let start = start.min(last);
        let end = end.clamp(start, last);
        let (line_start, column_start) = self.char_to_position[start];
        let (line_end, column_end) = self.char_to_position[end];
        SourceSpan::new(
            self.char_to_byte[start] as u64,
            self.char_to_byte[end] as u64,
            line_start,
            column_start,
            line_end,
            column_end,
        )
    }
}

#[derive(Debug, Default)]
struct LexicalIndex {
    scalar_ends: BTreeMap<usize, VecDeque<usize>>,
}

impl LexicalIndex {
    fn scan(source: &str, index: &SourceIndex) -> Result<Self, DslParseError> {
        let mut scanner = Scanner::new(source.chars());
        let tokens = scanner.by_ref().collect::<Vec<_>>();
        if let Some(error) = scanner.get_error() {
            return Err(safe_syntax_error(
                source,
                DslPath::root(),
                index.byte_at_marker(error.marker()),
            ));
        }

        let mut scalar_ends = BTreeMap::<usize, VecDeque<usize>>::new();
        for (token_index, token) in tokens.iter().enumerate() {
            if !matches!(token.1, TokenType::Scalar(..) | TokenType::Alias(..)) {
                continue;
            }
            let end = tokens.get(token_index + 1).map_or_else(
                || source.chars().count(),
                |next| index.character_at_marker(&next.0),
            );
            scalar_ends
                .entry(index.character_at_marker(&token.0))
                .or_default()
                .push_back(end);
        }
        Ok(Self { scalar_ends })
    }

    fn scalar_end(&self, start: usize) -> Option<usize> {
        self.scalar_ends
            .get(&start)
            .and_then(|ends| ends.front())
            .copied()
    }
}

#[derive(Debug, Default)]
struct EventCollector {
    events: Vec<(Event, Marker)>,
}

impl MarkedEventReceiver for EventCollector {
    fn on_event(&mut self, event: Event, marker: Marker) {
        self.events.push((event, marker));
    }
}

#[derive(Debug)]
struct SyntaxNode {
    start: usize,
    end: usize,
    kind: SyntaxKind,
}

#[derive(Debug)]
enum SyntaxKind {
    Scalar(String),
    Alias,
    Sequence(Vec<SyntaxNode>),
    Mapping(Vec<SyntaxEntry>),
}

#[derive(Debug)]
struct SyntaxEntry {
    key: SyntaxNode,
    value: SyntaxNode,
}

struct SyntaxParser<'a> {
    events: &'a [(Event, Marker)],
    position: usize,
    lexical: &'a LexicalIndex,
    source: &'a str,
    index: &'a SourceIndex,
}

impl<'a> SyntaxParser<'a> {
    fn new(
        events: &'a [(Event, Marker)],
        lexical: &'a LexicalIndex,
        source: &'a str,
        index: &'a SourceIndex,
    ) -> Self {
        Self {
            events,
            position: 0,
            lexical,
            source,
            index,
        }
    }

    fn parse(mut self) -> Result<SyntaxNode, DslParseError> {
        self.expect_wrapper(|event| matches!(event, Event::StreamStart))?;
        self.expect_wrapper(|event| matches!(event, Event::DocumentStart))?;
        let root = self.parse_node(0)?;
        self.expect_wrapper(|event| matches!(event, Event::DocumentEnd))?;
        match self.events.get(self.position) {
            Some((Event::StreamEnd, _)) if self.position + 1 == self.events.len() => Ok(root),
            Some((Event::DocumentStart, marker)) => Err(safe_syntax_error(
                self.source,
                DslPath::root(),
                self.index.byte_at_marker(marker),
            )),
            Some((_, marker)) => Err(safe_syntax_error(
                self.source,
                DslPath::root(),
                self.index.byte_at_marker(marker),
            )),
            None => Err(safe_syntax_error(
                self.source,
                DslPath::root(),
                self.source.len(),
            )),
        }
    }

    fn expect_wrapper(
        &mut self,
        predicate: impl FnOnce(&Event) -> bool,
    ) -> Result<(), DslParseError> {
        let Some((event, marker)) = self.events.get(self.position) else {
            return Err(safe_syntax_error(
                self.source,
                DslPath::root(),
                self.source.len(),
            ));
        };
        if !predicate(event) {
            return Err(safe_syntax_error(
                self.source,
                DslPath::root(),
                self.index.byte_at_marker(marker),
            ));
        }
        self.position += 1;
        Ok(())
    }

    fn parse_node(&mut self, depth: usize) -> Result<SyntaxNode, DslParseError> {
        if depth > 256 {
            let byte = self
                .events
                .get(self.position)
                .map_or(self.source.len(), |(_, marker)| {
                    self.index.byte_at_marker(marker)
                });
            return Err(safe_syntax_error(self.source, DslPath::root(), byte));
        }
        let Some((event, marker)) = self.events.get(self.position).cloned() else {
            return Err(safe_syntax_error(
                self.source,
                DslPath::root(),
                self.source.len(),
            ));
        };
        self.position += 1;
        let event_start = self.index.character_at_marker(&marker);
        let start = event_start;
        match event {
            Event::Scalar(value, style, ..) => {
                let start = match style {
                    TScalarStyle::Literal => self.block_scalar_start(event_start, '|'),
                    TScalarStyle::Folded => self.block_scalar_start(event_start, '>'),
                    _ => event_start,
                };
                let end = self.scalar_end(start);
                Ok(SyntaxNode {
                    start,
                    end,
                    kind: SyntaxKind::Scalar(value),
                })
            }
            Event::Alias(_) => {
                let end = self.scalar_end(start);
                Ok(SyntaxNode {
                    start,
                    end,
                    kind: SyntaxKind::Alias,
                })
            }
            Event::SequenceStart(..) => {
                let mut items = Vec::new();
                while !matches!(
                    self.events.get(self.position),
                    Some((Event::SequenceEnd, _))
                ) {
                    items.push(self.parse_node(depth + 1)?);
                }
                let (_, end_marker) = self.events[self.position].clone();
                self.position += 1;
                let end = self.container_end(
                    self.index.character_at_marker(&end_marker),
                    ']',
                    items.last().map_or(start, |item| item.end),
                );
                Ok(SyntaxNode {
                    start,
                    end,
                    kind: SyntaxKind::Sequence(items),
                })
            }
            Event::MappingStart(..) => {
                let mut entries = Vec::new();
                while !matches!(self.events.get(self.position), Some((Event::MappingEnd, _))) {
                    let key = self.parse_node(depth + 1)?;
                    if matches!(
                        self.events.get(self.position),
                        Some((Event::MappingEnd, _)) | None
                    ) {
                        let byte = self.index.byte(key.start);
                        return Err(safe_syntax_error(self.source, DslPath::root(), byte));
                    }
                    let value = self.parse_node(depth + 1)?;
                    entries.push(SyntaxEntry { key, value });
                }
                let (_, end_marker) = self.events[self.position].clone();
                self.position += 1;
                let end = self.container_end(
                    self.index.character_at_marker(&end_marker),
                    '}',
                    entries.last().map_or(start, |entry| entry.value.end),
                );
                let start = entries
                    .first()
                    .map_or(start, |entry| start.min(entry.key.start));
                Ok(SyntaxNode {
                    start,
                    end,
                    kind: SyntaxKind::Mapping(entries),
                })
            }
            _ => Err(safe_syntax_error(
                self.source,
                DslPath::root(),
                self.index.byte(start),
            )),
        }
    }

    fn scalar_end(&self, start: usize) -> usize {
        let hint = self.lexical.scalar_end(start).unwrap_or_else(|| {
            self.events
                .get(self.position)
                .map_or(self.source.chars().count(), |(_, marker)| {
                    self.index.character_at_marker(marker)
                })
        });
        trim_node_end(self.source, self.index.byte(start), self.index.byte(hint))
            .and_then(|byte| self.index.char_at_byte(byte))
            .unwrap_or(hint)
    }

    fn container_end(&self, marker: usize, closing: char, block_end: usize) -> usize {
        let byte = self.index.byte(marker);
        if self.source[byte..].starts_with(closing) {
            marker.saturating_add(1)
        } else {
            block_end
        }
    }

    fn block_scalar_start(&self, event_start: usize, indicator: char) -> usize {
        let event_byte = self.index.byte(event_start);
        block_scalar_indicator_start(self.source, event_byte, indicator)
            .and_then(|byte| self.index.char_at_byte(byte))
            .unwrap_or(event_start)
    }
}

impl SourceIndex {
    fn char_at_byte(&self, byte: usize) -> Option<usize> {
        self.char_to_byte.binary_search(&byte).ok()
    }
}

fn block_scalar_indicator_start(source: &str, event_byte: usize, indicator: char) -> Option<usize> {
    if source[event_byte..].starts_with(indicator) {
        return Some(event_byte);
    }

    let mut next_line_start = source[..event_byte].rfind('\n').map_or(0, |byte| byte + 1);
    while next_line_start > 0 {
        let line_end = next_line_start - 1;
        let line_start = source[..line_end].rfind('\n').map_or(0, |byte| byte + 1);
        let line = source[line_start..line_end].trim_end_matches('\r');
        if let Some(offset) = line.rfind(indicator) {
            let suffix = &line[offset + indicator.len_utf8()..];
            let comment = suffix.char_indices().find_map(|(byte, character)| {
                (character == '#'
                    && (byte == 0
                        || suffix[..byte]
                            .chars()
                            .next_back()
                            .is_some_and(char::is_whitespace)))
                .then_some(byte)
            });
            let header_suffix = &suffix[..comment.unwrap_or(suffix.len())];
            if header_suffix.chars().all(|character| {
                character.is_ascii_whitespace()
                    || matches!(character, '+' | '-')
                    || matches!(character, '1'..='9')
            }) {
                return Some(line_start + offset);
            }
        }
        if !line.trim().is_empty() {
            break;
        }
        next_line_start = line_start;
    }
    None
}

fn trim_node_end(source: &str, start: usize, end: usize) -> Option<usize> {
    let mut slice = source.get(start..end)?;
    if matches!(slice.chars().next(), Some('"' | '\'')) {
        let quote = slice.chars().next()?;
        let mut escaped = false;
        let mut characters = slice.char_indices().skip(1).peekable();
        while let Some((offset, character)) = characters.next() {
            if quote == '"' {
                if character == quote && !escaped {
                    return Some(start + offset + character.len_utf8());
                }
                escaped = character == '\\' && !escaped;
                if character != '\\' {
                    escaped = false;
                }
            } else if character == quote {
                if characters.peek().is_some_and(|(_, next)| *next == quote) {
                    characters.next();
                } else {
                    return Some(start + offset + character.len_utf8());
                }
            }
        }
    }

    if !matches!(slice.chars().next(), Some('|' | '>')) {
        for (offset, character) in slice.char_indices() {
            if character == '#'
                && (offset == 0
                    || slice[..offset]
                        .chars()
                        .next_back()
                        .is_some_and(char::is_whitespace))
            {
                slice = &slice[..offset];
                break;
            }
        }
    }
    Some(start + slice.trim_end_matches(char::is_whitespace).len())
}

#[allow(clippy::too_many_arguments)]
fn collect_source_map(
    source: &str,
    index: &SourceIndex,
    node: &SyntaxNode,
    path: &DslPath,
    source_map: &mut BTreeMap<DslPath, SourceSpan>,
    key_spans: &mut BTreeMap<DslPath, SourceSpan>,
) -> Result<(), DslParseError> {
    match &node.kind {
        SyntaxKind::Scalar(_) | SyntaxKind::Alias => {}
        SyntaxKind::Sequence(items) => {
            for (item_index, item) in items.iter().enumerate() {
                let item_path = path.child_index(item_index);
                source_map.insert(item_path.clone(), node_span(index, item));
                collect_source_map(source, index, item, &item_path, source_map, key_spans)?;
            }
        }
        SyntaxKind::Mapping(entries) => {
            let mut seen = BTreeSet::new();
            for entry in entries {
                let SyntaxKind::Scalar(key) = &entry.key.kind else {
                    return Err(safe_syntax_error(
                        source,
                        path.clone(),
                        index.byte(entry.key.start),
                    ));
                };
                let value_path = path.child_key(key);
                let key_span = node_span(index, &entry.key);
                if !seen.insert(key.clone()) {
                    return Err(DslParseError::new(PARSE_ERROR_CODE, PARSE_ERROR_MESSAGE)
                        .at(value_path, Some(key_span)));
                }
                key_spans.insert(value_path.clone(), key_span);
                source_map.insert(value_path.clone(), node_span(index, &entry.value));
                collect_source_map(
                    source,
                    index,
                    &entry.value,
                    &value_path,
                    source_map,
                    key_spans,
                )?;
            }
        }
    }
    Ok(())
}

fn node_span(index: &SourceIndex, node: &SyntaxNode) -> SourceSpan {
    index.span(node.start, node.end)
}

fn safe_syntax_error(source: &str, path: DslPath, byte: usize) -> DslParseError {
    DslParseError::new(PARSE_ERROR_CODE, PARSE_ERROR_MESSAGE)
        .at(path, Some(SourceSpan::point(source, byte)))
}

#[cfg(test)]
mod tests {
    use super::super::value::{Identifier, ValueExpr};
    use super::{parse_workflow, ParallelSettle, RootResult, Step};

    fn source_slice(source: &str, span: crate::dsl::SourceSpan) -> &str {
        &source[span.byte_start() as usize..span.byte_end() as usize]
    }

    const VALID: &str = r#"
api_version: insight.agent/v2
kind: agent
metadata:
  id: parallel_researcher
  name: Parallel Researcher
  description: Typed vNext fixture.
schema_dialect: https://json-schema.org/draft/2020-12/schema
$defs:
  Perspective: {type: object}
prompts:
  system:
    inline: You are concise.
errors:
  all_failed:
    category: workflow
    code: WORKFLOW_ALL_FAILED
    public_message: Every branch failed.
input:
  schema:
    type: object
    required: [question]
output:
  data_schema: {type: object}
workflow:
  steps:
    - kind: llm
      id: prepare
      model: general_chat
      inputs:
        question: {from: input.question}
      messages:
        - {role: system, content: system}
        - {role: user, content: {from: inputs.question}}
      parameters: {temperature: 0.2}
      response: {format: text}

    - kind: action
      id: measure
      call: example.text_metrics
      inputs:
        text: {from: steps.prepare.output.data}

    - kind: parallel
      id: perspectives
      inputs:
        question: {from: steps.prepare.output.data}
      settle: all_settled
      max_concurrency: 2
      branches:
        technical:
          output_schema: {type: object}
          steps:
            - kind: llm
              id: analyze
              model: general_chat
              inputs:
                question: {from: scope.question}
              messages:
                - {role: system, content: system}
                - {role: user, content: {from: inputs.question}}
              response: {format: text}
          result:
            return: {from: steps.analyze.output.data}
        risk:
          output_schema: {type: object}
          result:
            raise: all_failed

    - kind: switch
      id: selected
      inputs:
        results: {from: steps.perspectives.output}
      output_schema: {}
      cases:
        - id: available
          when:
            cel: "scope.results.summary.ok > 0"
          result:
            return: {from: scope.results}
      default:
        id: fallback
        result:
          return: {literal: null}
  result:
    return:
      content: {from: steps.selected.output.answer}
      format: markdown
      data:
        object:
          answer: {from: steps.selected.output.answer}
          count: {literal: 2}
"#;

    #[test]
    fn parses_complete_workflow_with_all_step_variants() {
        let document = parse_workflow(VALID).unwrap();
        let workflow = document.raw_ast;

        assert_eq!(workflow.metadata.id.as_str(), "parallel_researcher");
        assert_eq!(workflow.workflow.steps.len(), 4);
        assert!(matches!(workflow.workflow.steps[0], Step::Llm { .. }));
        assert!(matches!(workflow.workflow.steps[1], Step::Action { .. }));
        assert!(matches!(
            workflow.workflow.steps[2],
            Step::Parallel {
                settle: ParallelSettle::AllSettled,
                ..
            }
        ));
        assert!(matches!(workflow.workflow.steps[3], Step::Switch { .. }));
        assert!(matches!(workflow.workflow.result, RootResult::Return(_)));

        let Step::Llm { inputs, .. } = &workflow.workflow.steps[0] else {
            unreachable!();
        };
        assert!(matches!(
            inputs[&Identifier::parse("question").unwrap()],
            ValueExpr::From(_)
        ));
        let document_span = document.source_map[&crate::dsl::DslPath::root()];
        assert_eq!(document_span.byte_start(), 0);
        assert_eq!(document_span.byte_end(), VALID.len() as u64);
    }

    #[test]
    fn records_value_and_sequence_item_spans_through_the_complete_tree() {
        let document = parse_workflow(VALID).unwrap();
        let paths = [
            crate::dsl::DslPath::root()
                .child_key("metadata")
                .child_key("id"),
            crate::dsl::DslPath::root()
                .child_key("workflow")
                .child_key("steps")
                .child_index(0),
            crate::dsl::DslPath::root()
                .child_key("workflow")
                .child_key("steps")
                .child_index(0)
                .child_key("messages")
                .child_index(1)
                .child_key("content")
                .child_key("from"),
            crate::dsl::DslPath::root()
                .child_key("workflow")
                .child_key("steps")
                .child_index(2)
                .child_key("branches")
                .child_key("technical")
                .child_key("steps")
                .child_index(0)
                .child_key("id"),
        ];
        for path in &paths {
            let span = document
                .source_map
                .get(path)
                .unwrap_or_else(|| panic!("missing source span for {path}"));
            assert!(span.byte_end() > span.byte_start(), "empty span for {path}");
        }
        assert_eq!(
            source_slice(VALID, document.source_map[&paths[0]]),
            "parallel_researcher"
        );
        assert_eq!(
            source_slice(VALID, document.source_map[&paths[2]]),
            "inputs.question"
        );
        assert_eq!(
            source_slice(VALID, document.source_map[&paths[3]]),
            "analyze"
        );

        let unicode = VALID.replace("Parallel Researcher", "并行研究员");
        let document = parse_workflow(&unicode).unwrap();
        let name = crate::dsl::DslPath::root()
            .child_key("metadata")
            .child_key("name");
        let span = document.source_map[&name];
        assert_eq!(source_slice(&unicode, span), "并行研究员");
        assert_eq!(
            span.byte_end() - span.byte_start(),
            "并行研究员".len() as u64
        );
    }

    #[test]
    fn records_exact_authored_span_for_unicode_block_scalars() {
        let source = VALID.replace(
            "    inline: You are concise.",
            "    inline: |\n      第一行\n      第二行 {{ question }}",
        );
        let document = parse_workflow(&source).unwrap();
        let inline = crate::dsl::DslPath::root()
            .child_key("prompts")
            .child_key("system")
            .child_key("inline");
        let span = document.source_map[&inline];

        assert_eq!(
            source_slice(&source, span),
            "|\n      第一行\n      第二行 {{ question }}"
        );
        assert_eq!(
            span.byte_start(),
            source.find("|\n      第一行").unwrap() as u64
        );
    }

    #[test]
    fn parses_both_parallel_settle_policies() {
        assert_eq!(
            yaml_serde::from_str::<ParallelSettle>("all").unwrap(),
            ParallelSettle::All
        );
        assert_eq!(
            yaml_serde::from_str::<ParallelSettle>("all_settled").unwrap(),
            ParallelSettle::AllSettled
        );
    }

    #[test]
    fn response_union_shape_errors_use_the_stable_llm_configuration_code() {
        for invalid_response in [
            "response: {format: text, schema: {type: string}}",
            "response: {format: json}",
            "response: {format: text, unknown: do-not-render}",
        ] {
            let source = VALID.replacen("response: {format: text}", invalid_response, 1);
            let error = parse_workflow(&source).unwrap_err();

            assert_eq!(
                error.code(),
                super::LLM_RESPONSE_CONFIG_INVALID,
                "response={invalid_response}, path={:?}",
                error.path()
            );
            assert_eq!(error.message(), super::LLM_RESPONSE_CONFIG_INVALID_MESSAGE);
            assert!(error
                .path()
                .is_some_and(|path| super::path_contains_key(path, "response")));
            assert!(!error.to_string().contains("do-not-render"));
        }
    }

    #[test]
    fn workflow_error_codes_and_public_messages_use_bounded_closed_profiles() {
        let exact_code = format!("A{}", "B".repeat(super::ERROR_CODE_MAX_CHARS - 1));
        let exact_message = "界".repeat(super::ERROR_PUBLIC_MESSAGE_MAX_CHARS);
        let exact = VALID
            .replace("WORKFLOW_ALL_FAILED", &exact_code)
            .replace("Every branch failed.", &exact_message);
        parse_workflow(&exact).expect("exact error declaration limits must remain accepted");

        for invalid_code in [
            "\"\"".to_string(),
            "lowercase".to_string(),
            "_LEADING_UNDERSCORE".to_string(),
            "A-DASH".to_string(),
            format!("A{}", "B".repeat(super::ERROR_CODE_MAX_CHARS)),
        ] {
            let source = VALID.replace(
                "WORKFLOW_ALL_FAILED",
                &format!("{invalid_code} # do-not-render-error-code"),
            );
            let error = parse_workflow(&source).unwrap_err();
            assert_eq!(error.code(), super::PARSE_ERROR_CODE);
            assert_eq!(error.message(), super::PARSE_ERROR_MESSAGE);
            assert!(!error.to_string().contains("do-not-render"));
        }

        for invalid_message in [
            "\"   \"".to_string(),
            format!(
                "\"{}\"",
                "x".repeat(super::ERROR_PUBLIC_MESSAGE_MAX_CHARS + 1)
            ),
        ] {
            let source = VALID.replace("Every branch failed.", &invalid_message);
            let error = parse_workflow(&source).unwrap_err();
            assert_eq!(error.code(), super::PARSE_ERROR_CODE);
            assert_eq!(error.message(), super::PARSE_ERROR_MESSAGE);
        }
    }

    #[test]
    fn rejects_unknown_fields_at_root_step_and_child_boundaries() {
        let root_unknown = VALID.replace("kind: agent", "kind: agent\nunknown_root_contract: true");
        assert!(parse_workflow(&root_unknown).is_err());

        let step_unknown = VALID.replace(
            "model: general_chat",
            "model: general_chat\n      next: forbidden",
        );
        assert!(parse_workflow(&step_unknown).is_err());

        let child_unknown = VALID.replace(
            "output_schema: {type: object}",
            "output_schema: {type: object}\n          next: forbidden",
        );
        assert!(parse_workflow(&child_unknown).is_err());

        let mixed_business_policy = VALID.replace(
            "settle: all_settled",
            "settle: all_settled\n      require: {min_ok: 1}",
        );
        assert!(parse_workflow(&mixed_business_policy).is_err());
    }

    #[test]
    fn rejects_the_complete_legacy_authored_control_flow_grammar() {
        let mut legacy_documents = vec![
            (
                "entry",
                VALID.replacen("workflow:\n", "entry: prepare\nworkflow:\n", 1),
            ),
            (
                "nodes",
                VALID.replacen("workflow:\n", "nodes: {}\nworkflow:\n", 1),
            ),
            (
                "next",
                VALID.replacen(
                    "model: general_chat",
                    "model: general_chat\n      next: selected",
                    1,
                ),
            ),
        ];
        for legacy_type in [
            "core.fork",
            "core.join",
            "core.branch_end",
            "core.condition",
            "core.select",
            "core.end",
        ] {
            legacy_documents.push((
                legacy_type,
                VALID.replacen("model: general_chat", &format!("type: {legacy_type}"), 1),
            ));
        }

        for (legacy_feature, document) in legacy_documents {
            assert!(
                parse_workflow(&document).is_err(),
                "legacy authored feature '{legacy_feature}' must not enter the v2 AST"
            );
        }
    }

    #[test]
    fn rejects_removed_generic_calls_aliases_and_message_pseudo_operators() {
        let removed = [
            (
                "operation",
                VALID.replacen("kind: llm", "kind: operation", 1),
            ),
            (
                "uses",
                VALID.replacen(
                    "model: general_chat",
                    "model: general_chat\n      uses: ai.chat",
                    1,
                ),
            ),
            (
                "config",
                VALID.replacen(
                    "parameters: {temperature: 0.2}",
                    "parameters: {temperature: 0.2}\n      config: {}",
                    1,
                ),
            ),
            (
                "with",
                VALID.replacen("      inputs:\n", "      with:\n", 1),
            ),
            (
                "parts",
                VALID.replacen(
                    "- {role: system, content: system}",
                    "- {role: system, content: system, parts: []}",
                    1,
                ),
            ),
            (
                "spread",
                VALID.replacen(
                    "      messages:\n",
                    "      messages:\n        - {spread: history}\n",
                    1,
                ),
            ),
            (
                "concat",
                VALID.replacen(
                    "      messages:\n",
                    "      messages:\n        - {concat: []}\n",
                    1,
                ),
            ),
            (
                "prompt expression",
                VALID.replacen(
                    "text: {from: steps.prepare.output.data}",
                    "text: {prompt: system}",
                    1,
                ),
            ),
        ];

        for (feature, document) in removed {
            assert!(
                parse_workflow(&document).is_err(),
                "removed authored feature '{feature}' must not enter the v2 AST"
            );
        }
    }

    #[test]
    fn requires_an_explicit_schema_dialect() {
        let missing = VALID.replace(
            "schema_dialect: https://json-schema.org/draft/2020-12/schema\n",
            "",
        );
        let error = parse_workflow(&missing).unwrap_err();
        assert_eq!(error.code(), super::PARSE_ERROR_CODE);
        assert!(error.path().is_some_and(crate::dsl::DslPath::is_root));
        assert!(error.span().is_some());
        assert_eq!(error.to_string(), super::PARSE_ERROR_MESSAGE);
    }

    #[test]
    fn rejects_dynamic_paths_through_the_full_document_parser() {
        let dynamic = VALID.replace("{from: input.question}", "{from: 'steps[chosen].output'}");
        let error = parse_workflow(&dynamic).unwrap_err();
        assert_eq!(error.code(), super::PARSE_ERROR_CODE);
        assert!(error.path().is_some_and(|path| !path.is_root()));
        assert!(error.span().is_some());
        assert!(!error.to_string().contains("steps[chosen]"));
    }

    #[test]
    fn rejects_implicit_value_and_missing_block_result_shapes() {
        let implicit = VALID.replace(
            "question: {from: input.question}",
            "question: input.question",
        );
        assert!(parse_workflow(&implicit).is_err());

        let missing_result =
            VALID.replace("          result:\n            raise: all_failed\n", "");
        assert!(parse_workflow(&missing_result).is_err());
    }

    #[test]
    fn parse_errors_carry_safe_structured_paths_and_minimal_spans() {
        let source = VALID.replace(
            "model: general_chat",
            "model: general_chat\n      secret_unknown_field: do-not-render",
        );
        let error = parse_workflow(&source).unwrap_err();

        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.workflow.steps")
        );
        let span = error.span().unwrap();
        assert!(span.byte_end() - span.byte_start() <= 4);
        assert!(span.line_start() > 1);
        assert_eq!(error.to_string(), super::PARSE_ERROR_MESSAGE);
        assert!(!error.to_string().contains("do-not-render"));
        assert!(!error.to_string().contains("secret_unknown_field"));
    }

    #[test]
    fn unknown_fields_and_union_discriminators_use_the_smallest_authored_span() {
        let unknown = VALID.replacen(
            "model: general_chat",
            "model: general_chat\n      secret_unknown_field: do-not-render",
            1,
        );
        let error = parse_workflow(&unknown).unwrap_err();
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.workflow.steps[0].secret_unknown_field")
        );
        assert_eq!(
            source_slice(&unknown, error.span().unwrap()),
            "secret_unknown_field"
        );
        assert_eq!(error.to_string(), super::PARSE_ERROR_MESSAGE);
        assert!(!error.to_string().contains("do-not-render"));

        let invalid_kind = VALID.replacen("kind: llm", "kind: mystery", 1);
        let error = parse_workflow(&invalid_kind).unwrap_err();
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.workflow.steps[0].kind")
        );
        assert_eq!(
            source_slice(&invalid_kind, error.span().unwrap()),
            "mystery"
        );
        assert!(!error.to_string().contains("mystery"));

        let invalid_union = VALID.replacen(
            "    inline: You are concise.",
            "    inline: You are concise.\n    file: prompts/secret.md",
            1,
        );
        let error = parse_workflow(&invalid_union).unwrap_err();
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.prompts.system")
        );
        assert!(error.span().unwrap().byte_end() > error.span().unwrap().byte_start());
        assert!(!error.to_string().contains("secret.md"));
    }

    #[test]
    fn rejects_yaml_and_json_duplicate_keys_at_the_second_key_span() {
        let yaml = VALID.replacen("kind: agent", "kind: agent\nkind: agent", 1);
        let error = parse_workflow(&yaml).unwrap_err();
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.kind")
        );
        assert_eq!(source_slice(&yaml, error.span().unwrap()), "kind");

        let raw = parse_workflow(VALID).unwrap().raw_ast;
        let json = serde_json::to_string(&raw).unwrap();
        let duplicate = json.replacen(
            "\"kind\":\"agent\"",
            "\"kind\":\"agent\",\"kind\":\"agent\"",
            1,
        );
        let error = parse_workflow(&duplicate).unwrap_err();
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.kind")
        );
        assert_eq!(source_slice(&duplicate, error.span().unwrap()), "\"kind\"");
        assert_eq!(error.to_string(), super::PARSE_ERROR_MESSAGE);
    }

    #[test]
    fn json_unknown_fields_and_union_discriminators_have_exact_safe_locations() {
        let raw = parse_workflow(VALID).unwrap().raw_ast;
        let json = serde_json::to_string(&raw).unwrap();
        let unknown = json.replacen(
            "\"description\":",
            "\"secret_unknown\":\"do-not-render\",\"description\":",
            1,
        );
        let error = parse_workflow(&unknown).unwrap_err();
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.metadata.secret_unknown")
        );
        assert_eq!(
            source_slice(&unknown, error.span().unwrap()),
            "\"secret_unknown\""
        );
        assert!(!error.to_string().contains("do-not-render"));

        let invalid_kind = json.replacen("\"kind\":\"llm\"", "\"kind\":\"mystery\"", 1);
        let error = parse_workflow(&invalid_kind).unwrap_err();
        assert_eq!(
            error.path().map(ToString::to_string).as_deref(),
            Some("$.workflow.steps[0].kind")
        );
        assert_eq!(
            source_slice(&invalid_kind, error.span().unwrap()),
            "\"mystery\""
        );
        assert!(!error.to_string().contains("mystery"));
    }

    #[test]
    fn the_same_parser_accepts_json_and_records_its_document_span() {
        let raw = parse_workflow(VALID).unwrap().raw_ast;
        let json = serde_json::to_string(&raw).unwrap();
        let document = parse_workflow(&json).unwrap();

        assert_eq!(document.raw_ast, raw);
        assert_eq!(
            document.source_map[&crate::dsl::DslPath::root()].byte_end(),
            json.len() as u64
        );
        let nested = crate::dsl::DslPath::root()
            .child_key("workflow")
            .child_key("steps")
            .child_index(0)
            .child_key("id");
        assert_eq!(
            source_slice(&json, document.source_map[&nested]),
            "\"prepare\""
        );
    }
}
