use std::collections::{BTreeMap, BTreeSet};

use serde::{de::Error as _, Deserialize, Deserializer};
use serde_json::{json, Map, Value};

use super::{
    message::{
        AuthoredContentAtom, AuthoredContentExpr, AuthoredMessageTemplate, AuthoredRole,
        MessageListExpr, MessageSource, ResponseConfig,
    },
    raw::{
        ApiVersion, BlockResult, DocumentKind, ErrorDeclaration, InputContract, Metadata,
        OutputContract, ParallelBranch, ParallelSettle, Predicate, PromptDeclaration, RawWorkflow,
        RootResult, RootReturn, Step, SwitchCase, SwitchDefault, WorkflowBody,
    },
    template::compile_template,
    value::{Identifier, LocalInputPath, LocalInputRef, TemplateExpr, ValueExpr, ValuePath},
};

const DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";
const LEGACY_VALUE_KEYS: [&str; 5] = ["from", "literal", "object", "array", "template"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TypeExpr {
    String,
    Number,
    Integer,
    Boolean,
    Named(Identifier),
    Array(Box<TypeExpr>),
}

impl TypeExpr {
    fn parse(source: &str) -> Result<Self, String> {
        if source.trim() != source || source.is_empty() {
            return Err("type expression must be a non-empty canonical scalar".to_string());
        }
        if let Some(item) = source.strip_suffix("[]") {
            if item.ends_with("[]") {
                return Self::parse(item).map(|item| Self::Array(Box::new(item)));
            }
            return Self::parse(item).map(|item| Self::Array(Box::new(item)));
        }
        match source {
            "string" => Ok(Self::String),
            "number" => Ok(Self::Number),
            "integer" => Ok(Self::Integer),
            "boolean" => Ok(Self::Boolean),
            _ => {
                let name = Identifier::parse(source)?;
                if !is_pascal_case(name.as_str()) {
                    return Err(format!("named type '{name}' must use PascalCase"));
                }
                Ok(Self::Named(name))
            }
        }
    }
}

impl<'de> Deserialize<'de> for TypeExpr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let source = String::deserialize(deserializer)?;
        Self::parse(&source).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum FieldDeclaration {
    Simple(TypeExpr),
    Full(FieldOptions),
}

#[derive(Debug, Clone, Default)]
enum DefaultDeclaration {
    #[default]
    Missing,
    Value(Value),
}

impl DefaultDeclaration {
    fn is_present(&self) -> bool {
        matches!(self, Self::Value(_))
    }

    fn value(&self) -> Option<&Value> {
        match self {
            Self::Missing => None,
            Self::Value(value) => Some(value),
        }
    }
}

impl<'de> Deserialize<'de> for DefaultDeclaration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Value::deserialize(deserializer).map(Self::Value)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldOptions {
    r#type: TypeExpr,
    #[serde(default)]
    optional: bool,
    #[serde(default)]
    default: DefaultDeclaration,
    #[serde(default)]
    min_items: Option<usize>,
    #[serde(default)]
    max_items: Option<usize>,
    #[serde(default)]
    min_length: Option<usize>,
    #[serde(default)]
    max_length: Option<usize>,
    #[serde(default)]
    pattern: Option<String>,
    #[serde(default, rename = "enum")]
    enum_values: Option<Vec<Value>>,
}

impl FieldDeclaration {
    fn options(&self) -> FieldOptions {
        match self {
            Self::Simple(r#type) => FieldOptions {
                r#type: r#type.clone(),
                optional: false,
                default: DefaultDeclaration::Missing,
                min_items: None,
                max_items: None,
                min_length: None,
                max_length: None,
                pattern: None,
                enum_values: None,
            },
            Self::Full(options) => options.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum TypeDeclaration {
    Object(ObjectTypeDeclaration),
    Alias(AliasTypeDeclaration),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectTypeDeclaration {
    fields: BTreeMap<Identifier, FieldDeclaration>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasTypeDeclaration {
    r#type: TypeExpr,
    #[serde(default)]
    min_items: Option<usize>,
    #[serde(default)]
    max_items: Option<usize>,
    #[serde(default)]
    min_length: Option<usize>,
    #[serde(default)]
    max_length: Option<usize>,
    #[serde(default)]
    pattern: Option<String>,
    #[serde(default, rename = "enum")]
    enum_values: Option<Vec<Value>>,
}

#[derive(Debug, Clone, Copy)]
struct ConstraintOptions<'a> {
    min_items: Option<usize>,
    max_items: Option<usize>,
    min_length: Option<usize>,
    max_length: Option<usize>,
    pattern: Option<&'a str>,
    enum_values: Option<&'a [Value]>,
}

impl FieldOptions {
    fn constraints(&self) -> ConstraintOptions<'_> {
        ConstraintOptions {
            min_items: self.min_items,
            max_items: self.max_items,
            min_length: self.min_length,
            max_length: self.max_length,
            pattern: self.pattern.as_deref(),
            enum_values: self.enum_values.as_deref(),
        }
    }
}

impl AliasTypeDeclaration {
    fn constraints(&self) -> ConstraintOptions<'_> {
        ConstraintOptions {
            min_items: self.min_items,
            max_items: self.max_items,
            min_length: self.min_length,
            max_length: self.max_length,
            pattern: self.pattern.as_deref(),
            enum_values: self.enum_values.as_deref(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthoredWorkflow {
    api_version: ApiVersion,
    kind: DocumentKind,
    metadata: Metadata,
    #[serde(default)]
    types: BTreeMap<Identifier, TypeDeclaration>,
    #[serde(default)]
    prompts: BTreeMap<Identifier, PromptDeclaration>,
    #[serde(default)]
    errors: BTreeMap<Identifier, ErrorDeclaration>,
    inputs: BTreeMap<Identifier, FieldDeclaration>,
    output: TypeExpr,
    workflow: AuthoredWorkflowBody,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthoredWorkflowBody {
    #[serde(default)]
    steps: Vec<AuthoredStep>,
    result: AuthoredRootResult,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum AuthoredStep {
    Llm {
        id: Identifier,
        model: String,
        messages: AuthoredMessageList,
        #[serde(default)]
        parameters: Map<String, Value>,
        response: TypeExpr,
    },
    Action {
        id: Identifier,
        call: String,
        #[serde(default)]
        inputs: BTreeMap<Identifier, Value>,
    },
    Parallel {
        id: Identifier,
        #[serde(default)]
        inputs: BTreeMap<Identifier, Value>,
        settle: ParallelSettle,
        #[serde(default)]
        max_concurrency: Option<usize>,
        branches: BTreeMap<Identifier, AuthoredParallelBranch>,
    },
    Switch {
        id: Identifier,
        #[serde(default)]
        inputs: BTreeMap<Identifier, Value>,
        output: TypeExpr,
        cases: Vec<AuthoredSwitchCase>,
        default: AuthoredSwitchDefault,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthoredParallelBranch {
    output: TypeExpr,
    #[serde(default)]
    steps: Vec<AuthoredStep>,
    result: AuthoredBlockResult,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthoredSwitchCase {
    id: Identifier,
    when: Predicate,
    #[serde(default)]
    steps: Vec<AuthoredStep>,
    result: AuthoredBlockResult,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthoredSwitchDefault {
    id: Identifier,
    #[serde(default)]
    steps: Vec<AuthoredStep>,
    result: AuthoredBlockResult,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthoredReturn {
    r#return: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthoredRaise {
    raise: Identifier,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum AuthoredBlockResult {
    Return(AuthoredReturn),
    Raise(AuthoredRaise),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum AuthoredRootResult {
    Return(AuthoredReturn),
    Raise(AuthoredRaise),
}

#[derive(Debug, Deserialize)]
struct AuthoredMessageList(Vec<AuthoredMessageSource>);

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum AuthoredMessageSource {
    Dynamic(String),
    Authored(AuthoredMessage),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthoredMessage {
    role: AuthoredRole,
    content: Vec<AuthoredContentPart>,
}

#[derive(Debug)]
enum AuthoredContentPart {
    Text(String),
    ImageUrl(String),
}

impl<'de> Deserialize<'de> for AuthoredContentPart {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| D::Error::custom("content part must be a closed single-key object"))?;
        if object.len() != 1 {
            return Err(D::Error::custom(
                "content part must contain exactly one of text or image_url",
            ));
        }
        let (key, value) = object.iter().next().expect("one key was checked");
        let value = value
            .as_str()
            .ok_or_else(|| D::Error::custom("content part value must be a string scalar"))?;
        match key.as_str() {
            "text" => Ok(Self::Text(value.to_string())),
            "image_url" => Ok(Self::ImageUrl(value.to_string())),
            _ => Err(D::Error::custom(
                "content part key must be text or image_url",
            )),
        }
    }
}

pub(crate) fn deserialize_workflow<'de, D>(deserializer: D) -> Result<RawWorkflow, D::Error>
where
    D: Deserializer<'de>,
{
    AuthoredWorkflow::deserialize(deserializer)?
        .into_raw()
        .map_err(D::Error::custom)
}

impl AuthoredWorkflow {
    fn into_raw(self) -> Result<RawWorkflow, String> {
        validate_snake_case_names(self.inputs.keys(), "input")?;
        for name in self.types.keys() {
            if name.as_str() == "Message" {
                return Err("Message is a reserved platform type and cannot be redefined".into());
            }
            if !is_pascal_case(name.as_str()) {
                return Err(format!("custom type '{name}' must use PascalCase"));
            }
        }

        let mut definitions = BTreeMap::from([(
            Identifier::parse("Message").expect("platform type is canonical"),
            message_schema(),
        )]);
        for (name, declaration) in &self.types {
            definitions.insert(
                name.clone(),
                type_declaration_schema(declaration, &self.types)?,
            );
        }

        let input_schema = object_contract_schema(&self.inputs, &self.types)?;
        let output_schema = type_schema(&self.output);
        let root_inputs = self.inputs.keys().cloned().collect::<BTreeSet<_>>();
        let mut environment = LexicalEnvironment::root(root_inputs);
        let mut converter = WorkflowConverter {
            prompts: &self.prompts,
            types: &self.types,
        };
        let steps = converter.convert_steps(self.workflow.steps, &mut environment)?;
        let result = converter.convert_root_result(self.workflow.result, &environment)?;

        Ok(RawWorkflow {
            api_version: self.api_version,
            kind: self.kind,
            metadata: self.metadata,
            schema_dialect: DIALECT.to_string(),
            definitions,
            prompts: self.prompts,
            errors: self.errors,
            input: InputContract {
                schema: input_schema,
            },
            output: OutputContract {
                data_schema: output_schema,
            },
            workflow: WorkflowBody { steps, result },
        })
    }
}

fn validate_snake_case_names<'a>(
    names: impl IntoIterator<Item = &'a Identifier>,
    category: &str,
) -> Result<(), String> {
    for name in names {
        if !is_snake_case(name.as_str()) {
            return Err(format!("{category} '{name}' must use snake_case"));
        }
    }
    Ok(())
}

fn is_pascal_case(value: &str) -> bool {
    value
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_uppercase())
        && !value.contains('_')
}

fn is_snake_case(value: &str) -> bool {
    let mut chars = value.chars();
    chars
        .next()
        .is_some_and(|character| character.is_ascii_lowercase())
        && chars.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
}

fn object_contract_schema(
    fields: &BTreeMap<Identifier, FieldDeclaration>,
    types: &BTreeMap<Identifier, TypeDeclaration>,
) -> Result<Value, String> {
    let mut properties = Map::new();
    let mut required = Vec::new();
    for (name, declaration) in fields {
        let options = declaration.options();
        validate_field_options(&options, name.as_str())?;
        let schema = field_schema(&options, types)?;
        if !options.optional && !options.default.is_present() {
            required.push(Value::String(name.as_str().to_string()));
        }
        properties.insert(name.as_str().to_string(), schema);
    }
    Ok(json!({
        "type": "object",
        "required": required,
        "properties": properties,
        "additionalProperties": false
    }))
}

fn type_declaration_schema(
    declaration: &TypeDeclaration,
    types: &BTreeMap<Identifier, TypeDeclaration>,
) -> Result<Value, String> {
    match declaration {
        TypeDeclaration::Object(object) => {
            validate_snake_case_names(object.fields.keys(), "field")?;
            if object.fields.values().any(|field| {
                let options = field.options();
                options.optional || options.default.is_present()
            }) {
                return Err(
                    "default and optional are currently supported only for top-level inputs"
                        .to_string(),
                );
            }
            object_contract_schema(&object.fields, types)
        }
        TypeDeclaration::Alias(alias) => {
            let mut schema = type_schema(&alias.r#type);
            apply_constraints(&mut schema, &alias.r#type, types, alias.constraints())?;
            Ok(schema)
        }
    }
}

fn field_schema(
    options: &FieldOptions,
    types: &BTreeMap<Identifier, TypeDeclaration>,
) -> Result<Value, String> {
    let mut schema = type_schema(&options.r#type);
    apply_constraints(&mut schema, &options.r#type, types, options.constraints())?;
    if options.optional {
        schema = json!({"anyOf": [schema, {"type": "null"}]});
    }
    if let Some(default) = options.default.value() {
        validate_static_default(default)?;
        let object = schema
            .as_object_mut()
            .ok_or_else(|| "generated field schema is not an object".to_string())?;
        object.insert("default".to_string(), default.clone());
    }
    Ok(schema)
}

fn validate_static_default(value: &Value) -> Result<(), String> {
    match value {
        Value::String(value) if is_runtime_reference_scalar(value) => {
            Err("input defaults are static and cannot contain runtime references".to_string())
        }
        Value::Array(items) => {
            for item in items {
                validate_static_default(item)?;
            }
            Ok(())
        }
        Value::Object(object) => {
            if object.len() == 1
                && object
                    .keys()
                    .next()
                    .is_some_and(|key| LEGACY_VALUE_KEYS.contains(&key.as_str()))
            {
                return Err(
                    "legacy value wrappers from/literal/object/array/template are forbidden"
                        .to_string(),
                );
            }
            for value in object.values() {
                validate_static_default(value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_field_options(options: &FieldOptions, field: &str) -> Result<(), String> {
    if options.optional && options.default.is_present() {
        return Err(format!(
            "field '{field}' cannot combine default with optional: true"
        ));
    }
    if options
        .min_items
        .zip(options.max_items)
        .is_some_and(|(min, max)| min > max)
    {
        return Err(format!(
            "field '{field}' has min_items greater than max_items"
        ));
    }
    if options
        .min_length
        .zip(options.max_length)
        .is_some_and(|(min, max)| min > max)
    {
        return Err(format!(
            "field '{field}' has min_length greater than max_length"
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolvedTypeKind {
    String,
    Number,
    Integer,
    Boolean,
    Array,
    Object,
    Unknown,
}

impl ResolvedTypeKind {
    fn is_scalar(self) -> bool {
        matches!(
            self,
            Self::String | Self::Number | Self::Integer | Self::Boolean
        )
    }
}

fn resolve_type_kind(
    expression: &TypeExpr,
    types: &BTreeMap<Identifier, TypeDeclaration>,
) -> ResolvedTypeKind {
    fn resolve(
        expression: &TypeExpr,
        types: &BTreeMap<Identifier, TypeDeclaration>,
        active: &mut BTreeSet<Identifier>,
    ) -> ResolvedTypeKind {
        match expression {
            TypeExpr::String => ResolvedTypeKind::String,
            TypeExpr::Number => ResolvedTypeKind::Number,
            TypeExpr::Integer => ResolvedTypeKind::Integer,
            TypeExpr::Boolean => ResolvedTypeKind::Boolean,
            TypeExpr::Array(_) => ResolvedTypeKind::Array,
            TypeExpr::Named(name) if name.as_str() == "Message" => ResolvedTypeKind::Object,
            TypeExpr::Named(name) => {
                if !active.insert(name.clone()) {
                    return ResolvedTypeKind::Unknown;
                }
                let resolved = match types.get(name) {
                    Some(TypeDeclaration::Object(_)) => ResolvedTypeKind::Object,
                    Some(TypeDeclaration::Alias(alias)) => resolve(&alias.r#type, types, active),
                    None => ResolvedTypeKind::Unknown,
                };
                active.remove(name);
                resolved
            }
        }
    }

    resolve(expression, types, &mut BTreeSet::new())
}

fn apply_constraints(
    schema: &mut Value,
    declared_type: &TypeExpr,
    types: &BTreeMap<Identifier, TypeDeclaration>,
    constraints: ConstraintOptions<'_>,
) -> Result<(), String> {
    let ConstraintOptions {
        min_items,
        max_items,
        min_length,
        max_length,
        pattern,
        enum_values,
    } = constraints;
    if min_items.zip(max_items).is_some_and(|(min, max)| min > max) {
        return Err("min_items must not exceed max_items".to_string());
    }
    if min_length
        .zip(max_length)
        .is_some_and(|(min, max)| min > max)
    {
        return Err("min_length must not exceed max_length".to_string());
    }
    let resolved_type = resolve_type_kind(declared_type, types);
    let has_array_constraint = min_items.is_some() || max_items.is_some();
    if has_array_constraint
        && !matches!(
            resolved_type,
            ResolvedTypeKind::Array | ResolvedTypeKind::Unknown
        )
    {
        return Err("min_items and max_items require an array type".to_string());
    }
    let has_string_constraint = min_length.is_some() || max_length.is_some() || pattern.is_some();
    if has_string_constraint
        && !matches!(
            resolved_type,
            ResolvedTypeKind::String | ResolvedTypeKind::Unknown
        )
    {
        return Err("min_length, max_length, and pattern require type string".to_string());
    }
    if enum_values.is_some()
        && resolved_type != ResolvedTypeKind::Unknown
        && !resolved_type.is_scalar()
    {
        return Err("enum requires a primitive scalar type".to_string());
    }
    let object = schema
        .as_object_mut()
        .ok_or_else(|| "generated type schema is not an object".to_string())?;
    if let Some(value) = min_items {
        object.insert("minItems".to_string(), json!(value));
    }
    if let Some(value) = max_items {
        object.insert("maxItems".to_string(), json!(value));
    }
    if let Some(value) = min_length {
        object.insert("minLength".to_string(), json!(value));
    }
    if let Some(value) = max_length {
        object.insert("maxLength".to_string(), json!(value));
    }
    if let Some(value) = pattern {
        object.insert("pattern".to_string(), json!(value));
    }
    if let Some(values) = enum_values {
        if values.is_empty() {
            return Err("enum constraint must not be empty".to_string());
        }
        object.insert("enum".to_string(), Value::Array(values.to_vec()));
    }
    Ok(())
}

fn type_schema(expression: &TypeExpr) -> Value {
    match expression {
        TypeExpr::String => json!({"type": "string"}),
        TypeExpr::Number => json!({"type": "number"}),
        TypeExpr::Integer => json!({"type": "integer"}),
        TypeExpr::Boolean => json!({"type": "boolean"}),
        TypeExpr::Named(name) => json!({"$ref": format!("#/$defs/{name}")}),
        TypeExpr::Array(item) => json!({"type": "array", "items": type_schema(item)}),
    }
}

fn resolves_to_string(
    expression: &TypeExpr,
    types: &BTreeMap<Identifier, TypeDeclaration>,
) -> bool {
    fn resolve(
        expression: &TypeExpr,
        types: &BTreeMap<Identifier, TypeDeclaration>,
        active: &mut BTreeSet<Identifier>,
    ) -> bool {
        match expression {
            TypeExpr::String => true,
            TypeExpr::Number | TypeExpr::Integer | TypeExpr::Boolean | TypeExpr::Array(_) => false,
            TypeExpr::Named(name) => {
                if !active.insert(name.clone()) {
                    return false;
                }
                let is_string = types
                    .get(name)
                    .is_some_and(|declaration| match declaration {
                        TypeDeclaration::Alias(alias) => resolve(&alias.r#type, types, active),
                        TypeDeclaration::Object(_) => false,
                    });
                active.remove(name);
                is_string
            }
        }
    }

    resolve(expression, types, &mut BTreeSet::new())
}

fn message_schema() -> Value {
    let text_part = json!({
        "type": "object",
        "required": ["text"],
        "properties": {"text": {"type": "string", "minLength": 1}},
        "additionalProperties": false
    });
    let image_part = json!({
        "type": "object",
        "required": ["image_url"],
        "properties": {"image_url": {"type": "string", "minLength": 1}},
        "additionalProperties": false
    });
    json!({
        "oneOf": [
            {
                "type": "object",
                "required": ["role", "content"],
                "properties": {
                    "role": {"const": "user"},
                    "content": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"oneOf": [text_part.clone(), image_part]}
                    }
                },
                "additionalProperties": false
            },
            {
                "type": "object",
                "required": ["role", "content"],
                "properties": {
                    "role": {"const": "assistant"},
                    "content": {"type": "array", "minItems": 1, "items": text_part}
                },
                "additionalProperties": false
            }
        ]
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BaseNamespace {
    Input,
    Scope,
}

#[derive(Debug, Clone)]
struct LexicalEnvironment {
    base_namespace: BaseNamespace,
    base: BTreeSet<Identifier>,
    steps: BTreeSet<Identifier>,
}

impl LexicalEnvironment {
    fn root(inputs: BTreeSet<Identifier>) -> Self {
        Self {
            base_namespace: BaseNamespace::Input,
            base: inputs,
            steps: BTreeSet::new(),
        }
    }

    fn child(captures: BTreeSet<Identifier>) -> Self {
        Self {
            base_namespace: BaseNamespace::Scope,
            base: captures,
            steps: BTreeSet::new(),
        }
    }

    fn resolve(&self, source: &str) -> Result<ValuePath, String> {
        let (root, fields) = parse_lexical_reference(source)?;
        let base_visible = self.base.contains(&root);
        let step_visible = self.steps.contains(&root);
        let run_visible = self.base_namespace == BaseNamespace::Input && root.as_str() == "run";
        let candidates =
            usize::from(base_visible) + usize::from(step_visible) + usize::from(run_visible);
        match candidates {
            0 => {
                return Err(format!(
                    "runtime reference '{source}' is not defined in the current lexical scope"
                ))
            }
            1 => {}
            _ => {
                return Err(format!(
                    "runtime reference '{source}' is ambiguous in the current lexical scope"
                ))
            }
        }

        let mut canonical = if step_visible {
            format!("steps.{root}.output")
        } else if run_visible {
            "run".to_string()
        } else {
            match self.base_namespace {
                BaseNamespace::Input => format!("input.{root}"),
                BaseNamespace::Scope => format!("scope.{root}"),
            }
        };
        for field in fields {
            canonical.push('.');
            canonical.push_str(field.as_str());
        }
        ValuePath::parse(canonical)
    }

    fn insert_step(&mut self, id: Identifier) -> Result<(), String> {
        if self.base.contains(&id)
            || (self.base_namespace == BaseNamespace::Input && id.as_str() == "run")
        {
            return Err(format!(
                "step id '{id}' conflicts with another visible lexical symbol"
            ));
        }
        if !self.steps.insert(id.clone()) {
            return Err(format!("step id '{id}' is duplicated in its region"));
        }
        Ok(())
    }
}

fn parse_lexical_reference(source: &str) -> Result<(Identifier, Vec<Identifier>), String> {
    let Some(path) = source.strip_prefix('$') else {
        return Err("runtime reference must start with '$'".to_string());
    };
    if path.is_empty() || path.len() > 512 || path.contains(['#', '[', ']']) {
        return Err(format!("runtime reference '{source}' is not canonical"));
    }
    let mut segments = path.split('.');
    let root = Identifier::parse(
        segments
            .next()
            .expect("split always returns at least one segment"),
    )?;
    let fields = segments
        .map(Identifier::parse)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((root, fields))
}

fn is_runtime_reference_scalar(source: &str) -> bool {
    source.starts_with('$') && parse_lexical_reference(source).is_ok()
}

struct WorkflowConverter<'a> {
    prompts: &'a BTreeMap<Identifier, PromptDeclaration>,
    types: &'a BTreeMap<Identifier, TypeDeclaration>,
}

impl WorkflowConverter<'_> {
    fn convert_steps(
        &mut self,
        steps: Vec<AuthoredStep>,
        environment: &mut LexicalEnvironment,
    ) -> Result<Vec<Step>, String> {
        let mut converted = Vec::with_capacity(steps.len());
        for step in steps {
            let id = authored_step_id(&step).clone();
            if !is_snake_case(id.as_str()) {
                return Err(format!("step id '{id}' must use snake_case"));
            }
            let normalized = match step {
                AuthoredStep::Llm {
                    id,
                    model,
                    messages,
                    parameters,
                    response,
                } => {
                    let (inputs, messages) = self.convert_messages(messages, environment)?;
                    let response = match &response {
                        TypeExpr::String => ResponseConfig::Text,
                        TypeExpr::Named(_) if resolves_to_string(&response, self.types) => {
                            ResponseConfig::TextSchema {
                                schema: type_schema(&response),
                            }
                        }
                        _ => ResponseConfig::Json {
                            schema: type_schema(&response),
                        },
                    };
                    Step::Llm {
                        id,
                        model,
                        inputs,
                        messages,
                        parameters,
                        response,
                    }
                }
                AuthoredStep::Action { id, call, inputs } => Step::Action {
                    id,
                    call,
                    inputs: convert_named_values(inputs, environment)?,
                },
                AuthoredStep::Parallel {
                    id,
                    inputs,
                    settle,
                    max_concurrency,
                    branches,
                } => {
                    let inputs = convert_named_values(inputs, environment)?;
                    validate_snake_case_names(inputs.keys(), "control input")?;
                    let captures = inputs.keys().cloned().collect::<BTreeSet<_>>();
                    let branches = branches
                        .into_iter()
                        .map(|(name, branch)| {
                            if !is_snake_case(name.as_str()) {
                                return Err(format!(
                                    "parallel branch '{name}' must use snake_case"
                                ));
                            }
                            let mut child = LexicalEnvironment::child(captures.clone());
                            let steps = self.convert_steps(branch.steps, &mut child)?;
                            let result = self.convert_block_result(branch.result, &child)?;
                            Ok((
                                name,
                                ParallelBranch {
                                    output_schema: type_schema(&branch.output),
                                    steps,
                                    result,
                                },
                            ))
                        })
                        .collect::<Result<BTreeMap<_, _>, String>>()?;
                    Step::Parallel {
                        id,
                        inputs,
                        settle,
                        max_concurrency,
                        branches,
                    }
                }
                AuthoredStep::Switch {
                    id,
                    inputs,
                    output,
                    cases,
                    default,
                } => {
                    let inputs = convert_named_values(inputs, environment)?;
                    validate_snake_case_names(inputs.keys(), "control input")?;
                    let captures = inputs.keys().cloned().collect::<BTreeSet<_>>();
                    let cases = cases
                        .into_iter()
                        .map(|case| {
                            let mut child = LexicalEnvironment::child(captures.clone());
                            let steps = self.convert_steps(case.steps, &mut child)?;
                            let result = self.convert_block_result(case.result, &child)?;
                            Ok(SwitchCase {
                                id: case.id,
                                when: case.when,
                                steps,
                                result,
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?;
                    let mut child = LexicalEnvironment::child(captures);
                    let default_steps = self.convert_steps(default.steps, &mut child)?;
                    let default_result = self.convert_block_result(default.result, &child)?;
                    Step::Switch {
                        id,
                        inputs,
                        output_schema: type_schema(&output),
                        cases,
                        default: SwitchDefault {
                            id: default.id,
                            steps: default_steps,
                            result: default_result,
                        },
                    }
                }
            };
            environment.insert_step(id)?;
            converted.push(normalized);
        }
        Ok(converted)
    }

    fn convert_block_result(
        &self,
        result: AuthoredBlockResult,
        environment: &LexicalEnvironment,
    ) -> Result<BlockResult, String> {
        match result {
            AuthoredBlockResult::Return(value) => {
                convert_value(value.r#return, environment).map(BlockResult::Return)
            }
            AuthoredBlockResult::Raise(value) => Ok(BlockResult::Raise(value.raise)),
        }
    }

    fn convert_root_result(
        &self,
        result: AuthoredRootResult,
        environment: &LexicalEnvironment,
    ) -> Result<RootResult, String> {
        match result {
            AuthoredRootResult::Return(value) => Ok(RootResult::Return(RootReturn {
                content: None,
                format: None,
                data: convert_value(value.r#return, environment)?,
            })),
            AuthoredRootResult::Raise(value) => Ok(RootResult::Raise(value.raise)),
        }
    }

    fn convert_messages(
        &self,
        messages: AuthoredMessageList,
        environment: &LexicalEnvironment,
    ) -> Result<(BTreeMap<Identifier, ValueExpr>, MessageListExpr), String> {
        if messages.0.is_empty() {
            return Err("messages must contain at least one ordered source".to_string());
        }
        let mut captures = CaptureBuilder::default();
        let mut sources = Vec::with_capacity(messages.0.len());
        for source in messages.0 {
            match source {
                AuthoredMessageSource::Dynamic(reference) => {
                    let path = environment.resolve(&reference)?;
                    let local = captures.capture(ValueExpr::From(path))?;
                    sources.push(MessageSource::Dynamic(local));
                }
                AuthoredMessageSource::Authored(message) => {
                    if message.content.is_empty() {
                        return Err("authored message content must not be empty".to_string());
                    }
                    let mut atoms = Vec::with_capacity(message.content.len());
                    for part in message.content {
                        match part {
                            AuthoredContentPart::Text(text) => {
                                atoms.push(self.convert_text_part(
                                    &text,
                                    message.role,
                                    environment,
                                    &mut captures,
                                )?);
                            }
                            AuthoredContentPart::ImageUrl(image_url) => {
                                if message.role != AuthoredRole::User {
                                    return Err(
                                        "authored images are allowed only in user messages"
                                            .to_string(),
                                    );
                                }
                                let expression = if is_runtime_reference_scalar(&image_url) {
                                    ValueExpr::From(environment.resolve(&image_url)?)
                                } else {
                                    ValueExpr::Literal(Value::String(image_url))
                                };
                                let local = captures.capture(expression)?;
                                atoms.push(AuthoredContentAtom::Image(local));
                            }
                        }
                    }
                    sources.push(MessageSource::Authored(AuthoredMessageTemplate {
                        role: message.role,
                        content: AuthoredContentExpr::Parts(atoms),
                    }));
                }
            }
        }
        Ok((captures.inputs, MessageListExpr::Sources(sources)))
    }

    fn convert_text_part(
        &self,
        text: &str,
        _role: AuthoredRole,
        environment: &LexicalEnvironment,
        captures: &mut CaptureBuilder,
    ) -> Result<AuthoredContentAtom, String> {
        if is_runtime_reference_scalar(text) {
            let path = environment.resolve(text)?;
            return captures
                .capture(ValueExpr::From(path))
                .map(AuthoredContentAtom::RuntimeText);
        }

        if let Ok(prompt) = Identifier::parse(text) {
            if let Some(declaration) = self.prompts.get(&prompt) {
                if let PromptDeclaration::Inline(source) = declaration {
                    capture_template_slots(source, environment, captures)?;
                }
                return Ok(AuthoredContentAtom::Prompt(prompt));
            }
        }
        capture_template_slots(text, environment, captures)?;
        Ok(AuthoredContentAtom::InlineText(text.to_string()))
    }
}

fn authored_step_id(step: &AuthoredStep) -> &Identifier {
    match step {
        AuthoredStep::Llm { id, .. }
        | AuthoredStep::Action { id, .. }
        | AuthoredStep::Parallel { id, .. }
        | AuthoredStep::Switch { id, .. } => id,
    }
}

fn convert_named_values(
    values: BTreeMap<Identifier, Value>,
    environment: &LexicalEnvironment,
) -> Result<BTreeMap<Identifier, ValueExpr>, String> {
    validate_snake_case_names(values.keys(), "input")?;
    values
        .into_iter()
        .map(|(name, value)| Ok((name, convert_value(value, environment)?)))
        .collect()
}

#[derive(Default)]
struct CaptureBuilder {
    inputs: BTreeMap<Identifier, ValueExpr>,
    next: usize,
}

impl CaptureBuilder {
    fn capture(&mut self, expression: ValueExpr) -> Result<LocalInputRef, String> {
        let name = loop {
            let candidate = Identifier::parse(format!("__value_{}", self.next))?;
            self.next = self
                .next
                .checked_add(1)
                .ok_or_else(|| "LLM capture count overflowed".to_string())?;
            if !self.inputs.contains_key(&candidate) {
                break candidate;
            }
        };
        self.inputs.insert(name.clone(), expression);
        local_ref(&name)
    }

    fn capture_slot(&mut self, name: &Identifier, expression: ValueExpr) -> Result<(), String> {
        match self.inputs.get(name) {
            Some(existing) if existing == &expression => Ok(()),
            Some(_) => Err(format!(
                "template slot '{name}' resolves to conflicting lexical values"
            )),
            None => {
                self.inputs.insert(name.clone(), expression);
                Ok(())
            }
        }
    }
}

fn local_ref(binding: &Identifier) -> Result<LocalInputRef, String> {
    LocalInputPath::parse(format!("inputs.{binding}")).map(|from| LocalInputRef { from })
}

fn capture_template_slots(
    source: &str,
    environment: &LexicalEnvironment,
    captures: &mut CaptureBuilder,
) -> Result<(), String> {
    let Ok(compiled) = compile_template(source) else {
        // The compiler/lowerer owns template diagnostics because it can attach
        // the authored DSL path and decoded template coordinates.
        return Ok(());
    };
    for slot in compiled.slots() {
        let path = environment.resolve(&format!("${slot}"))?;
        captures.capture_slot(slot, ValueExpr::From(path))?;
    }
    Ok(())
}

fn convert_value(value: Value, environment: &LexicalEnvironment) -> Result<ValueExpr, String> {
    match value {
        Value::String(source) if is_runtime_reference_scalar(&source) => {
            environment.resolve(&source).map(ValueExpr::From)
        }
        Value::String(source) => {
            let compiled = match compile_template(&source) {
                Ok(compiled) => compiled,
                Err(_) if source.contains("{{") => {
                    return Ok(ValueExpr::Template(TemplateExpr {
                        text: source,
                        bindings: BTreeMap::new(),
                    }));
                }
                Err(_) => return Ok(ValueExpr::Literal(Value::String(source))),
            };
            if compiled.slots().is_empty() {
                return Ok(ValueExpr::Literal(Value::String(source)));
            }
            let bindings = compiled
                .slots()
                .iter()
                .map(|slot| {
                    let path = environment.resolve(&format!("${slot}"))?;
                    Ok((slot.clone(), ValueExpr::From(path)))
                })
                .collect::<Result<BTreeMap<_, _>, String>>()?;
            Ok(ValueExpr::Template(TemplateExpr {
                text: source,
                bindings,
            }))
        }
        Value::Array(items) => items
            .into_iter()
            .map(|item| convert_value(item, environment))
            .collect::<Result<Vec<_>, _>>()
            .map(ValueExpr::Array),
        Value::Object(object) => {
            if object.len() == 1
                && object
                    .keys()
                    .next()
                    .is_some_and(|key| LEGACY_VALUE_KEYS.contains(&key.as_str()))
            {
                return Err(
                    "legacy value wrappers from/literal/object/array/template are forbidden"
                        .to_string(),
                );
            }
            object
                .into_iter()
                .map(|(name, value)| Ok((name, convert_value(value, environment)?)))
                .collect::<Result<BTreeMap<_, _>, String>>()
                .map(ValueExpr::Object)
        }
        scalar => Ok(ValueExpr::Literal(scalar)),
    }
}

/// File-backed prompts become inline declarations before lowering. This pass
/// resolves their free template slots through the same lexical environment as
/// authored inline text and materializes only the captures actually used.
pub(crate) fn materialize_resolved_prompt_bindings(
    workflow: &mut RawWorkflow,
) -> Result<(), String> {
    let prompts = workflow.prompts.clone();
    let input_names = workflow
        .input
        .schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| {
            properties
                .keys()
                .map(Identifier::parse)
                .collect::<Result<BTreeSet<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    let mut environment = LexicalEnvironment::root(input_names);
    materialize_steps(&mut workflow.workflow.steps, &prompts, &mut environment)
}

fn materialize_steps(
    steps: &mut [Step],
    prompts: &BTreeMap<Identifier, PromptDeclaration>,
    environment: &mut LexicalEnvironment,
) -> Result<(), String> {
    for step in steps {
        let id = match step {
            Step::Llm {
                id,
                inputs,
                messages,
                ..
            } => {
                if let MessageListExpr::Sources(sources) = messages {
                    for source in sources {
                        let MessageSource::Authored(message) = source else {
                            continue;
                        };
                        for atom in message.content.atoms() {
                            let AuthoredContentAtom::Prompt(prompt_id) = atom else {
                                continue;
                            };
                            let PromptDeclaration::Inline(source) = prompts
                                .get(prompt_id)
                                .ok_or_else(|| format!("prompt '{prompt_id}' is not declared"))?
                            else {
                                return Err(format!(
                                    "prompt '{prompt_id}' was not resolved before binding"
                                ));
                            };
                            let compiled = compile_template(source).map_err(|_| {
                                format!("prompt '{prompt_id}' has an invalid template")
                            })?;
                            for slot in compiled.slots() {
                                let expression =
                                    ValueExpr::From(environment.resolve(&format!("${slot}"))?);
                                match inputs.get(slot) {
                                    Some(existing) if existing == &expression => {}
                                    Some(_) => {
                                        return Err(format!(
                                            "prompt slot '{slot}' conflicts with an LLM capture"
                                        ))
                                    }
                                    None => {
                                        inputs.insert(slot.clone(), expression);
                                    }
                                }
                            }
                        }
                    }
                }
                id.clone()
            }
            Step::Action { id, .. } => id.clone(),
            Step::Parallel {
                id,
                inputs,
                branches,
                ..
            } => {
                let captures = inputs.keys().cloned().collect::<BTreeSet<_>>();
                for branch in branches.values_mut() {
                    let mut child = LexicalEnvironment::child(captures.clone());
                    materialize_steps(&mut branch.steps, prompts, &mut child)?;
                }
                id.clone()
            }
            Step::Switch {
                id,
                inputs,
                cases,
                default,
                ..
            } => {
                let captures = inputs.keys().cloned().collect::<BTreeSet<_>>();
                for case in cases {
                    let mut child = LexicalEnvironment::child(captures.clone());
                    materialize_steps(&mut case.steps, prompts, &mut child)?;
                }
                let mut child = LexicalEnvironment::child(captures);
                materialize_steps(&mut default.steps, prompts, &mut child)?;
                id.clone()
            }
        };
        environment.insert_step(id)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{message_schema, TypeExpr};
    use crate::dsl::vnext::raw::parse_workflow;

    const MINIMAL: &str = r#"
api_version: insight.agent/v2
kind: agent
metadata:
  id: concise
  name: Concise
inputs:
  question: string
  messages:
    type: Message[]
    default: []
output: string
prompts:
  answer:
    inline: "Answer {{ question }}"
workflow:
  steps:
    - id: answer
      type: llm
      model: general_chat
      messages:
        - $messages
        - role: user
          content:
            - text: answer
      response: string
  result:
    return: $answer
"#;

    #[test]
    fn parses_type_expressions() {
        assert!(matches!(
            TypeExpr::parse("string").unwrap(),
            TypeExpr::String
        ));
        assert!(matches!(
            TypeExpr::parse("Message[]").unwrap(),
            TypeExpr::Array(_)
        ));
        assert!(TypeExpr::parse("message").is_err());
        assert!(TypeExpr::parse("Bad_Name").is_err());
    }

    #[test]
    fn platform_message_schema_uses_single_key_parts() {
        let rendered = message_schema().to_string();
        assert!(rendered.contains("image_url"));
        assert!(!rendered.contains("\"type\":\"text\""));
    }

    #[test]
    fn normalizes_new_author_surface_to_internal_contracts() {
        let raw = parse_workflow(MINIMAL).unwrap().raw_ast;
        assert!(raw
            .definitions
            .keys()
            .any(|name| name.as_str() == "Message"));
        let super::Step::Llm { inputs, .. } = &raw.workflow.steps[0] else {
            panic!("expected LLM")
        };
        assert!(inputs.keys().any(|name| name.as_str() == "question"));
    }

    #[test]
    fn rejects_deleted_author_aliases_and_tagged_parts() {
        for source in [
            MINIMAL.replace("return: $answer", "return: {from: input.question}"),
            MINIMAL.replace("- text: answer", "- type: text\n              text: answer"),
            MINIMAL.replace("type: llm", "kind: llm"),
        ] {
            assert!(
                parse_workflow(&source).is_err(),
                "source should be rejected"
            );
        }
    }

    #[test]
    fn rejects_runtime_references_and_legacy_wrappers_inside_defaults() {
        let cases = [
            MINIMAL.replace(
                "question: string",
                "question:\n    type: string\n    default: $missing",
            ),
            MINIMAL.replace(
                "question: string",
                "question:\n    type: string\n    default: {literal: x}",
            ),
            MINIMAL.replace(
                "question: string",
                "question:\n    type: string[]\n    default: [{nested: {from: x}}]",
            ),
        ];

        for source in cases {
            assert!(
                parse_workflow(&source).is_err(),
                "static defaults must not reopen runtime or legacy value syntax"
            );
        }
    }
}
