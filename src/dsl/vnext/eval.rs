use std::{collections::BTreeMap, fmt, io::Write};

use handlebars::{no_escape, Handlebars, Template};
use serde_json::{Map, Value};

use super::value::{Identifier, TemplateExpr, ValueExpr, ValuePath, ValuePathRoot};

pub const DEFAULT_MAX_TEMPLATE_OUTPUT_BYTES: usize = 262_144;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvalErrorCode {
    LimitInvalid,
    SourceMissing,
    SourceTypeMismatch,
    PromptMissing,
    TemplateInvalid,
    TemplateRenderFailed,
    TemplateOutputTooLarge,
}

impl EvalErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LimitInvalid => "EVAL_LIMIT_INVALID",
            Self::SourceMissing => "EVAL_SOURCE_MISSING",
            Self::SourceTypeMismatch => "EVAL_SOURCE_TYPE_MISMATCH",
            Self::PromptMissing => "EVAL_PROMPT_MISSING",
            Self::TemplateInvalid => "EVAL_TEMPLATE_INVALID",
            Self::TemplateRenderFailed => "EVAL_TEMPLATE_RENDER_FAILED",
            Self::TemplateOutputTooLarge => "EVAL_TEMPLATE_OUTPUT_TOO_LARGE",
        }
    }

    fn message(self) -> &'static str {
        match self {
            Self::LimitInvalid => "template output byte limit must be positive",
            Self::SourceMissing => "value source is missing",
            Self::SourceTypeMismatch => "value source traversal requires an object or array",
            Self::PromptMissing => "prompt is not defined",
            Self::TemplateInvalid => "template is invalid",
            Self::TemplateRenderFailed => "template rendering failed",
            Self::TemplateOutputTooLarge => "template output exceeds the configured byte limit",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvalError {
    code: EvalErrorCode,
}

impl EvalError {
    fn new(code: EvalErrorCode) -> Self {
        Self { code }
    }

    pub fn code(&self) -> &'static str {
        self.code.as_str()
    }

    pub fn message(&self) -> &'static str {
        self.code.message()
    }
}

impl fmt::Display for EvalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code(), self.message())
    }
}

impl std::error::Error for EvalError {}

/// The complete runtime namespace available to a value expression.
///
/// Step map values are their outputs directly. The `output` segment is part of
/// the authored path grammar and is not represented by another runtime wrapper.
#[derive(Debug, Clone, Copy)]
pub struct EvalEnvironment<'a> {
    pub input: &'a Value,
    pub run: &'a Value,
    pub scope: &'a Value,
    pub steps: &'a BTreeMap<Identifier, Value>,
    pub prompts: &'a BTreeMap<Identifier, String>,
}

impl<'a> EvalEnvironment<'a> {
    pub fn new(
        input: &'a Value,
        run: &'a Value,
        scope: &'a Value,
        steps: &'a BTreeMap<Identifier, Value>,
        prompts: &'a BTreeMap<Identifier, String>,
    ) -> Self {
        Self {
            input,
            run,
            scope,
            steps,
            prompts,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ValueEvaluator {
    max_template_output_bytes: usize,
}

impl ValueEvaluator {
    pub fn new(max_template_output_bytes: usize) -> Result<Self, EvalError> {
        if max_template_output_bytes == 0 {
            return Err(EvalError::new(EvalErrorCode::LimitInvalid));
        }
        Ok(Self {
            max_template_output_bytes,
        })
    }

    pub fn max_template_output_bytes(self) -> usize {
        self.max_template_output_bytes
    }

    pub fn evaluate(
        &self,
        expression: &ValueExpr,
        environment: &EvalEnvironment<'_>,
    ) -> Result<Value, EvalError> {
        match expression {
            ValueExpr::Literal(value) => Ok(value.clone()),
            ValueExpr::From(path) => resolve_path(path, environment).cloned(),
            ValueExpr::Object(fields) => fields
                .iter()
                .map(|(name, value)| Ok((name.clone(), self.evaluate(value, environment)?)))
                .collect::<Result<Map<_, _>, _>>()
                .map(Value::Object),
            ValueExpr::Array(values) => values
                .iter()
                .map(|value| self.evaluate(value, environment))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array),
            ValueExpr::Prompt(name) => environment
                .prompts
                .get(name)
                .cloned()
                .map(Value::String)
                .ok_or_else(|| EvalError::new(EvalErrorCode::PromptMissing)),
            ValueExpr::Template(template) => self.render_template(template, environment),
        }
    }

    fn render_template(
        &self,
        expression: &TemplateExpr,
        environment: &EvalEnvironment<'_>,
    ) -> Result<Value, EvalError> {
        let bindings = expression
            .bindings
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.as_str().to_string(),
                    self.evaluate(value, environment)?,
                ))
            })
            .collect::<Result<Map<_, _>, EvalError>>()?;

        let template = Template::compile(&expression.text)
            .map_err(|_| EvalError::new(EvalErrorCode::TemplateInvalid))?;
        let mut handlebars = Handlebars::new();
        handlebars.set_strict_mode(true);
        handlebars.register_escape_fn(no_escape);
        handlebars.register_template("value", template);

        let mut output = BoundedWriter::new(self.max_template_output_bytes);
        let rendered = handlebars.render_to_write("value", &Value::Object(bindings), &mut output);
        if output.exceeded() {
            return Err(EvalError::new(EvalErrorCode::TemplateOutputTooLarge));
        }
        rendered.map_err(|_| EvalError::new(EvalErrorCode::TemplateRenderFailed))?;
        String::from_utf8(output.into_bytes())
            .map(Value::String)
            .map_err(|_| EvalError::new(EvalErrorCode::TemplateRenderFailed))
    }
}

impl Default for ValueEvaluator {
    fn default() -> Self {
        Self {
            max_template_output_bytes: DEFAULT_MAX_TEMPLATE_OUTPUT_BYTES,
        }
    }
}

fn resolve_path<'environment>(
    path: &ValuePath,
    environment: &EvalEnvironment<'environment>,
) -> Result<&'environment Value, EvalError> {
    let mut value = match path.root() {
        ValuePathRoot::Input => environment.input,
        ValuePathRoot::Run => environment.run,
        ValuePathRoot::Scope => environment.scope,
        ValuePathRoot::StepOutput { step } => environment
            .steps
            .get(step)
            .ok_or_else(|| EvalError::new(EvalErrorCode::SourceMissing))?,
    };

    for field in path.fields() {
        value = match value {
            Value::Object(object) => object
                .get(field)
                .ok_or_else(|| EvalError::new(EvalErrorCode::SourceMissing))?,
            Value::Array(array) => {
                let index = parse_array_index(field)
                    .ok_or_else(|| EvalError::new(EvalErrorCode::SourceTypeMismatch))?;
                array
                    .get(index)
                    .ok_or_else(|| EvalError::new(EvalErrorCode::SourceMissing))?
            }
            _ => return Err(EvalError::new(EvalErrorCode::SourceTypeMismatch)),
        };
    }
    Ok(value)
}

fn parse_array_index(segment: &str) -> Option<usize> {
    if segment == "0" {
        return Some(0);
    }
    if segment.starts_with('0')
        || segment.is_empty()
        || !segment.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    segment.parse().ok()
}

struct BoundedWriter {
    bytes: Vec<u8>,
    max_bytes: usize,
    exceeded: bool,
}

impl BoundedWriter {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(max_bytes.min(4_096)),
            max_bytes,
            exceeded: false,
        }
    }

    fn exceeded(&self) -> bool {
        self.exceeded
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedWriter {
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::{json, Value};

    use super::{EvalEnvironment, ValueEvaluator};
    use crate::dsl::vnext::value::{Identifier, TemplateExpr, ValueExpr, ValuePath};

    fn id(value: &str) -> Identifier {
        Identifier::parse(value).unwrap()
    }

    fn from(path: &str) -> ValueExpr {
        ValueExpr::From(ValuePath::parse(path).unwrap())
    }

    struct Fixture {
        input: Value,
        run: Value,
        scope: Value,
        steps: BTreeMap<Identifier, Value>,
        prompts: BTreeMap<Identifier, String>,
    }

    impl Fixture {
        fn environment(&self) -> EvalEnvironment<'_> {
            EvalEnvironment::new(
                &self.input,
                &self.run,
                &self.scope,
                &self.steps,
                &self.prompts,
            )
        }
    }

    fn fixture() -> Fixture {
        Fixture {
            input: json!({
                "null": null,
                "bool": true,
                "number": 7,
                "string": "visible",
                "array": [1, false, null],
                "object": {"nested": "value"},
                "secret": "must-not-leak"
            }),
            run: json!({"id": "run_1"}),
            scope: json!({"item": {"id": "doc_1"}, "index": 3}),
            steps: BTreeMap::from([(id("lookup"), json!({"count": 2, "ok": true}))]),
            prompts: BTreeMap::from([(id("system"), "system prompt".to_string())]),
        }
    }

    #[test]
    fn preserves_every_json_type_through_from_and_recursive_containers() {
        let expression = ValueExpr::Object(BTreeMap::from([
            ("null".to_string(), from("input.null")),
            ("bool".to_string(), from("input.bool")),
            ("number".to_string(), from("input.number")),
            ("string".to_string(), from("input.string")),
            ("array".to_string(), from("input.array")),
            ("object".to_string(), from("input.object")),
            (
                "recursive".to_string(),
                ValueExpr::Array(vec![
                    from("steps.lookup.output.count"),
                    ValueExpr::Literal(json!({"exact": false})),
                ]),
            ),
        ]));
        let fixture = fixture();

        let value = ValueEvaluator::default()
            .evaluate(&expression, &fixture.environment())
            .unwrap();

        assert_eq!(
            value,
            json!({
                "null": null,
                "bool": true,
                "number": 7,
                "string": "visible",
                "array": [1, false, null],
                "object": {"nested": "value"},
                "recursive": [2, {"exact": false}]
            })
        );
    }

    #[test]
    fn resolves_only_the_declared_runtime_roots_and_scope() {
        let fixture = fixture();
        let evaluator = ValueEvaluator::default();

        assert_eq!(
            evaluator
                .evaluate(&from("scope.item.id"), &fixture.environment())
                .unwrap(),
            json!("doc_1")
        );
        assert_eq!(
            evaluator
                .evaluate(&from("run.id"), &fixture.environment())
                .unwrap(),
            json!("run_1")
        );
        assert_eq!(
            evaluator
                .evaluate(&from("steps.lookup.output.ok"), &fixture.environment())
                .unwrap(),
            json!(true)
        );
    }

    #[test]
    fn missing_and_non_object_sources_fail_with_body_free_errors() {
        let fixture = fixture();
        let evaluator = ValueEvaluator::default();

        let missing = evaluator
            .evaluate(&from("input.absent"), &fixture.environment())
            .unwrap_err();
        assert_eq!(missing.code(), "EVAL_SOURCE_MISSING");
        assert_eq!(missing.message(), "value source is missing");

        let invalid = evaluator
            .evaluate(&from("input.string.field"), &fixture.environment())
            .unwrap_err();
        assert_eq!(invalid.code(), "EVAL_SOURCE_TYPE_MISMATCH");
        assert_eq!(
            invalid.message(),
            "value source traversal requires an object or array"
        );
        assert!(!missing.to_string().contains("must-not-leak"));
        assert!(!invalid.to_string().contains("visible"));
    }

    #[test]
    fn resolves_arbitrary_object_keys_and_fixed_array_indices() {
        let fixture = Fixture {
            input: json!({"items": [{"display-name": "Ada", "a/b": true}]}),
            ..fixture()
        };

        assert_eq!(
            ValueEvaluator::default()
                .evaluate(&from("input#/items/0/display-name"), &fixture.environment())
                .unwrap(),
            json!("Ada")
        );
        assert_eq!(
            ValueEvaluator::default()
                .evaluate(&from("input#/items/0/a~1b"), &fixture.environment())
                .unwrap(),
            json!(true)
        );

        for path in ["input#/items/00", "input#/items/-1", "input#/items/4"] {
            assert!(ValueEvaluator::default()
                .evaluate(&from(path), &fixture.environment())
                .is_err());
        }
    }

    #[test]
    fn prompt_lookup_returns_raw_text_and_missing_prompt_is_stable() {
        let fixture = fixture();
        let evaluator = ValueEvaluator::default();

        assert_eq!(
            evaluator
                .evaluate(&ValueExpr::Prompt(id("system")), &fixture.environment())
                .unwrap(),
            json!("system prompt")
        );
        let error = evaluator
            .evaluate(&ValueExpr::Prompt(id("missing")), &fixture.environment())
            .unwrap_err();
        assert_eq!(error.code(), "EVAL_PROMPT_MISSING");
        assert_eq!(error.message(), "prompt is not defined");
    }

    #[test]
    fn template_receives_only_explicit_evaluated_bindings() {
        let fixture = fixture();
        let evaluator = ValueEvaluator::default();
        let explicit = ValueExpr::Template(TemplateExpr {
            text: "{{visible}}/{{count}}".to_string(),
            bindings: BTreeMap::from([
                (id("visible"), from("input.string")),
                (id("count"), from("steps.lookup.output.count")),
            ]),
        });

        assert_eq!(
            evaluator
                .evaluate(&explicit, &fixture.environment())
                .unwrap(),
            json!("visible/2")
        );

        let isolated = ValueExpr::Template(TemplateExpr {
            text: "{{input.secret}}".to_string(),
            bindings: BTreeMap::new(),
        });
        let error = evaluator
            .evaluate(&isolated, &fixture.environment())
            .unwrap_err();
        assert_eq!(error.code(), "EVAL_TEMPLATE_RENDER_FAILED");
        assert_eq!(error.message(), "template rendering failed");
        assert!(!error.to_string().contains("must-not-leak"));
    }

    #[test]
    fn template_output_limit_is_exact_and_checked_while_rendering() {
        let fixture = fixture();
        let expression = ValueExpr::Template(TemplateExpr {
            text: "{{value}}".to_string(),
            bindings: BTreeMap::from([(id("value"), ValueExpr::Literal(json!("1234")))]),
        });

        assert_eq!(
            ValueEvaluator::new(4)
                .unwrap()
                .evaluate(&expression, &fixture.environment())
                .unwrap(),
            json!("1234")
        );
        let error = ValueEvaluator::new(3)
            .unwrap()
            .evaluate(&expression, &fixture.environment())
            .unwrap_err();
        assert_eq!(error.code(), "EVAL_TEMPLATE_OUTPUT_TOO_LARGE");
        assert_eq!(
            error.message(),
            "template output exceeds the configured byte limit"
        );
    }

    #[test]
    fn invalid_template_and_zero_limit_have_stable_errors() {
        let fixture = fixture();
        let invalid = ValueExpr::Template(TemplateExpr {
            text: "{{#if value}}".to_string(),
            bindings: BTreeMap::new(),
        });
        let error = ValueEvaluator::default()
            .evaluate(&invalid, &fixture.environment())
            .unwrap_err();
        assert_eq!(error.code(), "EVAL_TEMPLATE_INVALID");
        assert_eq!(error.message(), "template is invalid");

        let error = ValueEvaluator::new(0).unwrap_err();
        assert_eq!(error.code(), "EVAL_LIMIT_INVALID");
        assert_eq!(
            error.message(),
            "template output byte limit must be positive"
        );
    }
}
