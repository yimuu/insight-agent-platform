use std::{collections::BTreeSet, io::Write};

use handlebars::template::{BlockParam, HelperTemplate, Parameter, TemplateElement};
use handlebars::{
    no_escape, Context, Handlebars, Helper, HelperDef, HelperResult, Output, RenderContext,
    Template,
};
use serde::Serialize;
use serde_json::Value;

use crate::dsl::SourceSpan;

use super::value::Identifier;

pub const TEMPLATE_INVALID: &str = "VNEXT_LLM_TEMPLATE_INVALID";
pub const TEMPLATE_RENDER_INVALID: &str = "VNEXT_LLM_TEMPLATE_RENDER_INVALID";
pub const TEMPLATE_OUTPUT_TOO_LARGE: &str = "VNEXT_LLM_REQUEST_TOO_LARGE";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledTemplate {
    source: String,
    ast: Template,
    slots: BTreeSet<Identifier>,
    accesses: Vec<TemplateAccess>,
}

impl CompiledTemplate {
    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn ast(&self) -> &Template {
        &self.ast
    }

    pub fn slots(&self) -> &BTreeSet<Identifier> {
        &self.slots
    }

    pub fn accesses(&self) -> &[TemplateAccess] {
        &self.accesses
    }

    pub fn render<T: Serialize>(&self, bindings: &T) -> Result<String, TemplateProfileError> {
        self.render_bounded(bindings, usize::MAX)
    }

    pub fn render_bounded<T: Serialize>(
        &self,
        bindings: &T,
        max_bytes: usize,
    ) -> Result<String, TemplateProfileError> {
        let mut registry = Handlebars::new();
        registry.set_strict_mode(true);
        registry.register_escape_fn(no_escape);
        registry.register_helper("json", Box::new(StableJsonHelper));
        registry.register_template("template", self.ast.clone());
        let mut output = BoundedTemplateOutput::new(max_bytes);
        if registry
            .render_to_write("template", bindings, &mut output)
            .is_err()
        {
            return Err(if output.exceeded {
                TemplateProfileError::too_large()
            } else {
                TemplateProfileError::render()
            });
        }
        String::from_utf8(output.bytes).map_err(|_| TemplateProfileError::render())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateAccessKind {
    Scalar,
    Json,
    Each,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplatePathSegment {
    Field(Identifier),
    EachItem,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplatePath {
    pub root: Identifier,
    pub segments: Vec<TemplatePathSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateAccess {
    pub path: TemplatePath,
    pub kind: TemplateAccessKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateProfileError {
    code: &'static str,
    message: &'static str,
    decoded_span: Option<SourceSpan>,
}

impl TemplateProfileError {
    fn invalid() -> Self {
        Self {
            code: TEMPLATE_INVALID,
            message: "template does not satisfy the restricted LLM template profile",
            decoded_span: None,
        }
    }

    fn invalid_at(source: &str, byte: usize) -> Self {
        Self::invalid().at_decoded_byte(source, byte)
    }

    fn render() -> Self {
        Self {
            code: TEMPLATE_RENDER_INVALID,
            message: "template rendering failed",
            decoded_span: None,
        }
    }

    fn too_large() -> Self {
        Self {
            code: TEMPLATE_OUTPUT_TOO_LARGE,
            message: "template rendering exceeds its configured byte limit",
            decoded_span: None,
        }
    }

    fn at_decoded_byte(mut self, source: &str, byte: usize) -> Self {
        if self.decoded_span.is_none() {
            self.decoded_span = Some(SourceSpan::point(source, byte));
        }
        self
    }

    fn at_decoded_position(mut self, source: &str, line: usize, column: usize) -> Self {
        if self.decoded_span.is_none() {
            self.decoded_span = Some(SourceSpan::point(
                source,
                decoded_byte_at(source, line, column),
            ));
        }
        self
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &'static str {
        self.message
    }

    /// Location in the decoded template source, independent from any outer
    /// YAML/JSON scalar or prompt asset location.
    pub fn decoded_span(&self) -> Option<SourceSpan> {
        self.decoded_span
    }
}

impl std::fmt::Display for TemplateProfileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for TemplateProfileError {}

struct BoundedTemplateOutput {
    bytes: Vec<u8>,
    max_bytes: usize,
    exceeded: bool,
}

impl BoundedTemplateOutput {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(max_bytes.min(4_096)),
            max_bytes,
            exceeded: false,
        }
    }
}

impl Write for BoundedTemplateOutput {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if buffer.len() > self.max_bytes.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "template output exceeds configured limit",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn compile_template(source: &str) -> Result<CompiledTemplate, TemplateProfileError> {
    if source.trim().is_empty() {
        return Err(TemplateProfileError::invalid_at(source, 0));
    }
    if let Some(byte) = source.find('\0') {
        return Err(TemplateProfileError::invalid_at(source, byte));
    }
    validate_delimiters(source)?;
    let ast = Template::compile(source).map_err(|error| {
        error.pos().map_or_else(
            || TemplateProfileError::invalid_at(source, 0),
            |(line, column)| {
                TemplateProfileError::invalid().at_decoded_position(source, line, column)
            },
        )
    })?;
    let mut slots = BTreeSet::new();
    let mut accesses = Vec::new();
    validate_elements(source, &ast, &mut Vec::new(), &mut slots, &mut accesses)?;
    Ok(CompiledTemplate {
        source: source.to_string(),
        ast,
        slots,
        accesses,
    })
}

fn validate_delimiters(source: &str) -> Result<(), TemplateProfileError> {
    let mut cursor = 0;
    let mut raw_start = None;
    while let Some(relative) = source[cursor..].find("{{") {
        let start = cursor + relative;
        let remaining = &source[start..];
        if raw_start.is_some() {
            if remaining.starts_with("{{{{/raw}}}}") {
                raw_start = None;
                cursor = start + "{{{{/raw}}}}".len();
            } else if remaining.starts_with("{{{{raw}}}}") || remaining.starts_with("{{{{") {
                return Err(TemplateProfileError::invalid_at(source, start));
            } else {
                cursor = start + 2;
            }
            continue;
        }

        if start > 0 && source.as_bytes()[start - 1] == b'\\' {
            return Err(TemplateProfileError::invalid_at(source, start - 1));
        }
        if remaining.starts_with("{{{{raw}}}}") {
            raw_start = Some(start);
            cursor = start + "{{{{raw}}}}".len();
            continue;
        }
        if remaining.starts_with("{{{{")
            || remaining.starts_with("{{{")
            || remaining.starts_with("{{&")
            || remaining.starts_with("{{~")
            || remaining.starts_with("{{!")
        {
            return Err(TemplateProfileError::invalid_at(source, start));
        }
        let Some(end) = remaining.find("}}") else {
            return Err(TemplateProfileError::invalid_at(source, start));
        };
        if remaining[..end].ends_with('~') {
            return Err(TemplateProfileError::invalid_at(source, start));
        }
        cursor = start + end + 2;
    }
    if let Some(start) = raw_start {
        return Err(TemplateProfileError::invalid_at(source, start));
    }
    Ok(())
}

fn validate_elements(
    source: &str,
    template: &Template,
    aliases: &mut Vec<(Identifier, TemplatePath)>,
    slots: &mut BTreeSet<Identifier>,
    accesses: &mut Vec<TemplateAccess>,
) -> Result<(), TemplateProfileError> {
    for (index, element) in template.elements.iter().enumerate() {
        let result = match element {
            TemplateElement::RawString(_) => Ok(()),
            TemplateElement::Expression(helper) => {
                validate_expression(helper, aliases, slots, accesses)
            }
            TemplateElement::HelperBlock(helper) => match helper.name.as_name() {
                Some("each") => validate_each(source, helper, aliases, slots, accesses),
                Some("raw") => validate_raw(helper),
                _ => Err(TemplateProfileError::invalid()),
            },
            TemplateElement::HtmlExpression(_)
            | TemplateElement::DecoratorExpression(_)
            | TemplateElement::DecoratorBlock(_)
            | TemplateElement::PartialExpression(_)
            | TemplateElement::PartialBlock(_)
            | TemplateElement::Comment(_) => Err(TemplateProfileError::invalid()),
            _ => Err(TemplateProfileError::invalid()),
        };
        if let Err(error) = result {
            let error = match template.mapping.get(index) {
                Some(mapping) => error.at_decoded_position(source, mapping.0, mapping.1),
                None => error.at_decoded_byte(source, 0),
            };
            return Err(error);
        }
    }
    Ok(())
}

fn validate_raw(helper: &HelperTemplate) -> Result<(), TemplateProfileError> {
    if !helper.block
        || helper.chain
        || !helper.params.is_empty()
        || !helper.hash.is_empty()
        || helper.block_param.is_some()
        || helper.inverse.is_some()
    {
        return Err(TemplateProfileError::invalid());
    }
    let body = helper
        .template
        .as_ref()
        .ok_or_else(TemplateProfileError::invalid)?;
    if body
        .elements
        .iter()
        .all(|element| matches!(element, TemplateElement::RawString(_)))
    {
        Ok(())
    } else {
        Err(TemplateProfileError::invalid())
    }
}

fn validate_expression(
    helper: &HelperTemplate,
    aliases: &[(Identifier, TemplatePath)],
    slots: &mut BTreeSet<Identifier>,
    accesses: &mut Vec<TemplateAccess>,
) -> Result<(), TemplateProfileError> {
    if helper.block
        || helper.chain
        || helper.block_param.is_some()
        || helper.template.is_some()
        || helper.inverse.is_some()
        || !helper.hash.is_empty()
    {
        return Err(TemplateProfileError::invalid());
    }
    let name = helper
        .name
        .as_name()
        .ok_or_else(TemplateProfileError::invalid)?;
    let (path, kind) = if helper.params.is_empty() {
        (
            resolve_path(name, aliases, slots)?,
            TemplateAccessKind::Scalar,
        )
    } else if name == "json" && helper.params.len() == 1 {
        let path = parameter_path(&helper.params[0])?;
        (
            resolve_path(path, aliases, slots)?,
            TemplateAccessKind::Json,
        )
    } else {
        return Err(TemplateProfileError::invalid());
    };
    accesses.push(TemplateAccess { path, kind });
    Ok(())
}

fn validate_each(
    source: &str,
    helper: &HelperTemplate,
    aliases: &mut Vec<(Identifier, TemplatePath)>,
    slots: &mut BTreeSet<Identifier>,
    accesses: &mut Vec<TemplateAccess>,
) -> Result<(), TemplateProfileError> {
    if helper.name.as_name() != Some("each")
        || !helper.block
        || helper.chain
        || helper.params.len() != 1
        || !helper.hash.is_empty()
        || helper.inverse.is_some()
    {
        return Err(TemplateProfileError::invalid());
    }
    let collection = parameter_path(&helper.params[0])?;
    let collection = resolve_path(collection, aliases, slots)?;
    accesses.push(TemplateAccess {
        path: collection.clone(),
        kind: TemplateAccessKind::Each,
    });
    let alias = match helper.block_param.as_ref() {
        Some(BlockParam::Single(parameter)) => Identifier::parse(parameter_path(parameter)?)
            .map_err(|_| TemplateProfileError::invalid())?,
        _ => return Err(TemplateProfileError::invalid()),
    };
    let body = helper
        .template
        .as_ref()
        .ok_or_else(TemplateProfileError::invalid)?;
    let mut item_path = collection;
    item_path.segments.push(TemplatePathSegment::EachItem);
    aliases.push((alias, item_path));
    let result = validate_elements(source, body, aliases, slots, accesses);
    aliases.pop();
    result
}

fn decoded_byte_at(source: &str, line: usize, column: usize) -> usize {
    let target_line = line.max(1);
    let target_column = column.max(1);
    let mut current_line = 1usize;
    let mut current_column = 1usize;

    for (byte, character) in source.char_indices() {
        if current_line == target_line {
            if current_column >= target_column || character == '\n' {
                return byte;
            }
        } else if current_line > target_line {
            return byte;
        }

        if character == '\n' {
            current_line = current_line.saturating_add(1);
            current_column = 1;
        } else {
            current_column = current_column.saturating_add(1);
        }
    }
    source.len()
}

fn parameter_path(parameter: &Parameter) -> Result<&str, TemplateProfileError> {
    match parameter {
        Parameter::Name(value) => Ok(value),
        Parameter::Path(_) => parameter
            .as_name()
            .ok_or_else(TemplateProfileError::invalid),
        Parameter::Literal(_) | Parameter::Subexpression(_) => Err(TemplateProfileError::invalid()),
        _ => Err(TemplateProfileError::invalid()),
    }
}

fn resolve_path(
    path: &str,
    aliases: &[(Identifier, TemplatePath)],
    slots: &mut BTreeSet<Identifier>,
) -> Result<TemplatePath, TemplateProfileError> {
    if path.is_empty()
        || path.starts_with('.')
        || path.starts_with('@')
        || path.contains('/')
        || path.contains('[')
        || path.contains(']')
    {
        return Err(TemplateProfileError::invalid());
    }
    let segments = path.split('.').collect::<Vec<_>>();
    let root = Identifier::parse(segments[0]).map_err(|_| TemplateProfileError::invalid())?;
    let fields = segments[1..]
        .iter()
        .map(|segment| {
            Identifier::parse(*segment)
                .map(TemplatePathSegment::Field)
                .map_err(|_| TemplateProfileError::invalid())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if let Some((_, alias_path)) = aliases.iter().rev().find(|(alias, _)| alias == &root) {
        let mut resolved = alias_path.clone();
        resolved.segments.extend(fields);
        Ok(resolved)
    } else {
        slots.insert(root.clone());
        Ok(TemplatePath {
            root,
            segments: fields,
        })
    }
}

#[derive(Clone, Copy)]
struct StableJsonHelper;

impl HelperDef for StableJsonHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        helper: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        output: &mut dyn Output,
    ) -> HelperResult {
        let value = helper
            .param(0)
            .ok_or(handlebars::RenderErrorReason::ParamNotFoundForIndex(
                "json", 0,
            ))?
            .value();
        let encoded = serde_json::to_string(&canonical_json(value))
            .expect("serde_json::Value always has a deterministic JSON representation");
        output.write(&encoded)?;
        Ok(())
    }
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonical_json).collect()),
        Value::Object(fields) => {
            let mut ordered = fields.iter().collect::<Vec<_>>();
            ordered.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
            Value::Object(
                ordered
                    .into_iter()
                    .map(|(name, value)| (name.clone(), canonical_json(value)))
                    .collect(),
            )
        }
        scalar => scalar.clone(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        compile_template, TemplateAccessKind, TemplatePathSegment, TEMPLATE_INVALID,
        TEMPLATE_OUTPUT_TOO_LARGE, TEMPLATE_RENDER_INVALID,
    };

    #[test]
    fn compiles_slots_json_each_and_raw_blocks() {
        let template = compile_template(
            "{{ question }}\n{{ json details }}\n{{#each items as |item|}}{{ item.name }}{{/each}}\n{{{{raw}}}}{{ literal }}{{{{/raw}}}}",
        )
        .unwrap();
        assert_eq!(
            template
                .slots()
                .iter()
                .map(|slot| slot.as_str())
                .collect::<Vec<_>>(),
            vec!["details", "items", "question"]
        );
        assert_eq!(template.accesses().len(), 4);
        assert_eq!(template.accesses()[0].kind, TemplateAccessKind::Scalar);
        assert_eq!(template.accesses()[1].kind, TemplateAccessKind::Json);
        assert_eq!(template.accesses()[2].kind, TemplateAccessKind::Each);
        assert_eq!(
            template.accesses()[3].path.segments,
            [
                TemplatePathSegment::EachItem,
                TemplatePathSegment::Field(
                    super::Identifier::parse("name").expect("name is an identifier")
                ),
            ]
        );
        let rendered = template
            .render(&json!({
                "question": "<unsafe>& {{still_literal}}",
                "details": {"b": 2, "a": 1},
                "items": [{"name": "A"}, {"name": "B"}],
            }))
            .unwrap();
        assert!(rendered.contains("<unsafe>& {{still_literal}}"));
        assert!(rendered.contains(r#"{"a":1,"b":2}"#));
        assert!(rendered.contains("AB"));
        assert!(rendered.ends_with("{{ literal }}"));
    }

    #[test]
    fn rejects_unapproved_handlebars_features_and_escapes() {
        for source in [
            "{{{ value }}}",
            "{{& value}}",
            "\\{{ value }}",
            "{{#if value}}x{{/if}}",
            "{{lookup value key}}",
            "{{> partial}}",
            "{{../parent}}",
            "{{@index}}",
            "{{! comment}}",
            "{{~ value }}",
            "{{ value ~}}",
            "{{#each items}}{{this}}{{/each}}",
            "{{#each items as |item|}}{{else}}x{{/each}}",
            "{{{{raw}}}}x{{{{raw}}}}y{{{{/raw}}}}{{{{/raw}}}}",
        ] {
            let error = compile_template(source).unwrap_err();
            assert_eq!(error.code(), TEMPLATE_INVALID, "source: {source}");
            assert!(error.decoded_span().is_some(), "source: {source}");
            assert!(!error.message().contains(source));
        }
    }

    #[test]
    fn compile_errors_report_decoded_unicode_coordinates_without_body() {
        let source = "第一行\n第二行 {{#if secret}}do-not-render{{/if}}";
        let error = compile_template(source).unwrap_err();
        let span = error.decoded_span().unwrap();

        assert_eq!(span.byte_start(), source.find("{{#if").unwrap() as u64);
        assert_eq!((span.line_start(), span.column_start()), (2, 5));
        assert_eq!(
            error.message(),
            "template does not satisfy the restricted LLM template profile"
        );
        assert!(!error.to_string().contains("secret"));
        assert!(!error.to_string().contains("do-not-render"));
    }

    #[test]
    fn handlebars_parse_errors_report_decoded_source_coordinates() {
        let source = "第一行\n{{#each items as |item|}}x{{/if}}do-not-render";
        let error = compile_template(source).unwrap_err();
        let span = error.decoded_span().unwrap();
        let expected_byte = source.find("{{/if}}").unwrap();

        assert_eq!(span.byte_start(), expected_byte as u64);
        assert_eq!((span.line_start(), span.column_start()), (2, 27));
        assert!(!error.to_string().contains("do-not-render"));
    }

    #[test]
    fn missing_runtime_slot_is_body_free() {
        let template = compile_template("{{ value }}").unwrap();
        let error = template.render(&json!({})).unwrap_err();
        assert_eq!(error.code(), TEMPLATE_RENDER_INVALID);
        assert_eq!(error.message(), "template rendering failed");
    }

    #[test]
    fn rendering_stops_at_the_exact_byte_limit() {
        let template = compile_template("{{ value }}").unwrap();
        assert_eq!(
            template.render_bounded(&json!({"value":"四"}), 3).unwrap(),
            "四"
        );
        let error = template
            .render_bounded(&json!({"value":"四"}), 2)
            .unwrap_err();
        assert_eq!(error.code(), TEMPLATE_OUTPUT_TOO_LARGE);
    }
}
