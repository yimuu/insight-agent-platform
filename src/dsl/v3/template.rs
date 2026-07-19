//! Restricted, deterministic template profile used by v3 messages and
//! immutable Markdown prompt assets.
//!
//! Templates are compiled while publishing a Definition Revision. Runtime
//! rendering reparses only the already-pinned source under this exact profile;
//! it never loads files or enables arbitrary Handlebars helpers.

use std::{collections::BTreeSet, io::Write};

use handlebars::template::{BlockParam, HelperTemplate, Parameter, TemplateElement};
use handlebars::{
    no_escape, Context, Handlebars, Helper, HelperDef, HelperResult, Output, RenderContext,
    Template,
};
use serde::Serialize;

pub const TEMPLATE_INVALID: &str = "DSL_V3_TEMPLATE_INVALID";
pub const TEMPLATE_RENDER_INVALID: &str = "DSL_V3_TEMPLATE_RENDER_INVALID";
pub const TEMPLATE_OUTPUT_TOO_LARGE: &str = "DSL_V3_TEMPLATE_OUTPUT_TOO_LARGE";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledTemplate {
    source: String,
    ast: Template,
    slots: BTreeSet<String>,
    accesses: Vec<TemplateAccess>,
}

impl CompiledTemplate {
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Root lexical names required by this template. An access such as
    /// `result.answer` therefore contributes the single slot `result`.
    pub fn slots(&self) -> &BTreeSet<String> {
        &self.slots
    }

    pub fn accesses(&self) -> &[TemplateAccess] {
        &self.accesses
    }

    pub fn render_bounded<T: Serialize>(
        &self,
        bindings: &T,
        max_bytes: usize,
    ) -> Result<String, TemplateError> {
        let mut registry = Handlebars::new();
        registry.set_strict_mode(true);
        registry.register_escape_fn(no_escape);
        registry.register_helper("json", Box::new(StableJsonHelper));
        registry.register_template("template", self.ast.clone());
        let mut output = BoundedOutput::new(max_bytes);
        if registry
            .render_to_write("template", bindings, &mut output)
            .is_err()
        {
            return Err(if output.exceeded {
                TemplateError::too_large()
            } else {
                TemplateError::render()
            });
        }
        String::from_utf8(output.bytes).map_err(|_| TemplateError::render())
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
    Field(String),
    EachItem,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplatePath {
    root: String,
    segments: Vec<TemplatePathSegment>,
}

impl TemplatePath {
    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn segments(&self) -> &[TemplatePathSegment] {
        &self.segments
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateAccess {
    path: TemplatePath,
    kind: TemplateAccessKind,
}

impl TemplateAccess {
    pub fn path(&self) -> &TemplatePath {
        &self.path
    }

    pub fn kind(&self) -> TemplateAccessKind {
        self.kind
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateError {
    code: &'static str,
    message: &'static str,
}

impl TemplateError {
    fn invalid() -> Self {
        Self {
            code: TEMPLATE_INVALID,
            message: "template does not satisfy the restricted v3 profile",
        }
    }

    fn render() -> Self {
        Self {
            code: TEMPLATE_RENDER_INVALID,
            message: "template rendering failed",
        }
    }

    fn too_large() -> Self {
        Self {
            code: TEMPLATE_OUTPUT_TOO_LARGE,
            message: "template rendering exceeds its configured byte limit",
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl std::fmt::Display for TemplateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for TemplateError {}

pub fn compile_template(source: &str) -> Result<CompiledTemplate, TemplateError> {
    if source.trim().is_empty() || source.contains('\0') {
        return Err(TemplateError::invalid());
    }
    validate_delimiters(source)?;
    let ast = Template::compile(source).map_err(|_| TemplateError::invalid())?;
    let mut slots = BTreeSet::new();
    let mut accesses = Vec::new();
    validate_elements(&ast, &mut Vec::new(), &mut slots, &mut accesses)?;
    Ok(CompiledTemplate {
        source: source.to_owned(),
        ast,
        slots,
        accesses,
    })
}

fn validate_delimiters(source: &str) -> Result<(), TemplateError> {
    let mut cursor = 0;
    let mut raw_open = false;
    while let Some(relative) = source[cursor..].find("{{") {
        let start = cursor + relative;
        let remaining = &source[start..];
        if raw_open {
            if remaining.starts_with("{{{{/raw}}}}") {
                raw_open = false;
                cursor = start + "{{{{/raw}}}}".len();
            } else if remaining.starts_with("{{{{") {
                return Err(TemplateError::invalid());
            } else {
                cursor = start + 2;
            }
            continue;
        }
        if start > 0 && source.as_bytes()[start - 1] == b'\\' {
            return Err(TemplateError::invalid());
        }
        if remaining.starts_with("{{{{raw}}}}") {
            raw_open = true;
            cursor = start + "{{{{raw}}}}".len();
            continue;
        }
        if remaining.starts_with("{{{{")
            || remaining.starts_with("{{{")
            || remaining.starts_with("{{&")
            || remaining.starts_with("{{~")
            || remaining.starts_with("{{!")
        {
            return Err(TemplateError::invalid());
        }
        let Some(end) = remaining.find("}}") else {
            return Err(TemplateError::invalid());
        };
        if remaining[..end].ends_with('~') {
            return Err(TemplateError::invalid());
        }
        cursor = start + end + 2;
    }
    if raw_open {
        return Err(TemplateError::invalid());
    }
    Ok(())
}

fn validate_elements(
    template: &Template,
    aliases: &mut Vec<(String, TemplatePath)>,
    slots: &mut BTreeSet<String>,
    accesses: &mut Vec<TemplateAccess>,
) -> Result<(), TemplateError> {
    for element in &template.elements {
        match element {
            TemplateElement::RawString(_) => {}
            TemplateElement::Expression(helper) => {
                validate_expression(helper, aliases, slots, accesses)?;
            }
            TemplateElement::HelperBlock(helper) => match helper.name.as_name() {
                Some("each") => validate_each(helper, aliases, slots, accesses)?,
                Some("raw") => validate_raw(helper)?,
                _ => return Err(TemplateError::invalid()),
            },
            TemplateElement::HtmlExpression(_)
            | TemplateElement::DecoratorExpression(_)
            | TemplateElement::DecoratorBlock(_)
            | TemplateElement::PartialExpression(_)
            | TemplateElement::PartialBlock(_)
            | TemplateElement::Comment(_) => return Err(TemplateError::invalid()),
            _ => return Err(TemplateError::invalid()),
        }
    }
    Ok(())
}

fn validate_raw(helper: &HelperTemplate) -> Result<(), TemplateError> {
    if !helper.block
        || helper.chain
        || !helper.params.is_empty()
        || !helper.hash.is_empty()
        || helper.block_param.is_some()
        || helper.inverse.is_some()
    {
        return Err(TemplateError::invalid());
    }
    let body = helper
        .template
        .as_ref()
        .ok_or_else(TemplateError::invalid)?;
    if body
        .elements
        .iter()
        .all(|element| matches!(element, TemplateElement::RawString(_)))
    {
        Ok(())
    } else {
        Err(TemplateError::invalid())
    }
}

fn validate_expression(
    helper: &HelperTemplate,
    aliases: &[(String, TemplatePath)],
    slots: &mut BTreeSet<String>,
    accesses: &mut Vec<TemplateAccess>,
) -> Result<(), TemplateError> {
    if helper.block
        || helper.chain
        || helper.block_param.is_some()
        || helper.template.is_some()
        || helper.inverse.is_some()
        || !helper.hash.is_empty()
    {
        return Err(TemplateError::invalid());
    }
    let name = helper.name.as_name().ok_or_else(TemplateError::invalid)?;
    let (path, kind) = if helper.params.is_empty() {
        (
            resolve_path(name, aliases, slots)?,
            TemplateAccessKind::Scalar,
        )
    } else if name == "json" && helper.params.len() == 1 {
        (
            resolve_path(parameter_path(&helper.params[0])?, aliases, slots)?,
            TemplateAccessKind::Json,
        )
    } else {
        return Err(TemplateError::invalid());
    };
    accesses.push(TemplateAccess { path, kind });
    Ok(())
}

fn validate_each(
    helper: &HelperTemplate,
    aliases: &mut Vec<(String, TemplatePath)>,
    slots: &mut BTreeSet<String>,
    accesses: &mut Vec<TemplateAccess>,
) -> Result<(), TemplateError> {
    if !helper.block
        || helper.chain
        || helper.params.len() != 1
        || !helper.hash.is_empty()
        || helper.inverse.is_some()
    {
        return Err(TemplateError::invalid());
    }
    let collection = resolve_path(parameter_path(&helper.params[0])?, aliases, slots)?;
    accesses.push(TemplateAccess {
        path: collection.clone(),
        kind: TemplateAccessKind::Each,
    });
    let alias = match helper.block_param.as_ref() {
        Some(BlockParam::Single(parameter)) => {
            let alias = parameter_path(parameter)?;
            validate_identifier(alias)?;
            alias.to_owned()
        }
        _ => return Err(TemplateError::invalid()),
    };
    let body = helper
        .template
        .as_ref()
        .ok_or_else(TemplateError::invalid)?;
    let mut item_path = collection;
    item_path.segments.push(TemplatePathSegment::EachItem);
    aliases.push((alias, item_path));
    let result = validate_elements(body, aliases, slots, accesses);
    aliases.pop();
    result
}

fn parameter_path(parameter: &Parameter) -> Result<&str, TemplateError> {
    match parameter {
        Parameter::Name(value) => Ok(value),
        Parameter::Path(_) => parameter.as_name().ok_or_else(TemplateError::invalid),
        Parameter::Literal(_) | Parameter::Subexpression(_) => Err(TemplateError::invalid()),
        _ => Err(TemplateError::invalid()),
    }
}

fn resolve_path(
    path: &str,
    aliases: &[(String, TemplatePath)],
    slots: &mut BTreeSet<String>,
) -> Result<TemplatePath, TemplateError> {
    if path.is_empty()
        || path.starts_with('.')
        || path.starts_with('@')
        || path.contains('/')
        || path.contains('[')
        || path.contains(']')
    {
        return Err(TemplateError::invalid());
    }
    let segments = path.split('.').collect::<Vec<_>>();
    validate_identifier(segments[0])?;
    let fields = segments[1..]
        .iter()
        .map(|segment| {
            validate_identifier(segment)?;
            Ok(TemplatePathSegment::Field((*segment).to_owned()))
        })
        .collect::<Result<Vec<_>, TemplateError>>()?;
    if let Some((_, alias_path)) = aliases.iter().rev().find(|(alias, _)| alias == segments[0]) {
        let mut resolved = alias_path.clone();
        resolved.segments.extend(fields);
        Ok(resolved)
    } else {
        slots.insert(segments[0].to_owned());
        Ok(TemplatePath {
            root: segments[0].to_owned(),
            segments: fields,
        })
    }
}

fn validate_identifier(value: &str) -> Result<(), TemplateError> {
    let mut characters = value.chars();
    if value.len() > 128
        || !matches!(characters.next(), Some(first) if first.is_ascii_alphabetic() || first == '_')
        || characters.any(|character| !(character.is_ascii_alphanumeric() || character == '_'))
    {
        return Err(TemplateError::invalid());
    }
    Ok(())
}

struct BoundedOutput {
    bytes: Vec<u8>,
    max_bytes: usize,
    exceeded: bool,
}

impl BoundedOutput {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(max_bytes.min(4_096)),
            max_bytes,
            exceeded: false,
        }
    }
}

impl Write for BoundedOutput {
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
        let encoded = serde_jcs::to_string(value).map_err(|_| {
            handlebars::RenderErrorReason::Other("canonical JSON rendering failed".to_owned())
        })?;
        output.write(&encoded)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{compile_template, TemplateAccessKind, TEMPLATE_INVALID};

    #[test]
    fn profile_collects_root_slots_and_renders_scalar_json_and_each() {
        let template = compile_template(
            "{{ question }}\n{{ json details }}\n{{#each items as |item|}}{{ item.name }}{{/each}}",
        )
        .unwrap();
        assert_eq!(
            template
                .slots()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["details", "items", "question"]
        );
        assert_eq!(template.accesses()[1].kind(), TemplateAccessKind::Json);
        assert_eq!(
            template
                .render_bounded(
                    &json!({
                        "question": "<untrusted>{{still_literal}}",
                        "details": {"b": 2, "a": 1},
                        "items": [{"name": "A"}, {"name": "B"}],
                    }),
                    1024,
                )
                .unwrap(),
            "<untrusted>{{still_literal}}\n{\"a\":1,\"b\":2}\nAB"
        );
    }

    #[test]
    fn profile_rejects_dynamic_helpers_parent_access_and_unbounded_features() {
        for source in [
            "{{{ value }}}",
            "{{#if value}}x{{/if}}",
            "{{lookup value key}}",
            "{{../parent}}",
            "{{@index}}",
            "{{! comment}}",
        ] {
            assert_eq!(
                compile_template(source).unwrap_err().code(),
                TEMPLATE_INVALID
            );
        }
    }

    #[test]
    fn bounded_render_and_strict_missing_slot_fail_closed() {
        let template = compile_template("{{ value }}").unwrap();
        assert!(template.render_bounded(&json!({}), 16).is_err());
        assert!(template.render_bounded(&json!({"value": "四"}), 2).is_err());
        assert_eq!(
            template.render_bounded(&json!({"value": "四"}), 3).unwrap(),
            "四"
        );
    }
}
