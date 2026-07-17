use std::{collections::BTreeMap, fmt, str::FromStr};

use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// A stable DSL identifier used for steps, branches, bindings, prompts, and errors.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Identifier(String);

impl Identifier {
    pub fn parse(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        let mut characters = value.chars();
        let Some(first) = characters.next() else {
            return Err("identifier must not be empty".to_string());
        };
        if !(first.is_ascii_alphabetic() || first == '_')
            || !characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return Err(format!(
                "identifier '{value}' must match [A-Za-z_][A-Za-z0-9_]*"
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for Identifier {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl<'de> Deserialize<'de> for Identifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(D::Error::custom)
    }
}

impl Serialize for Identifier {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

/// The statically resolvable root of a value path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValuePathRoot {
    Input,
    Scope,
    Run,
    StepOutput { step: Identifier },
}

/// A canonical, computed-access-free path used by `ValueExpr::From`.
///
/// Identifier-shaped object keys may use dot shorthand. Arbitrary object keys
/// and fixed array indices use a JSON Pointer suffix, for example
/// `input#/items/0/display-name` or `steps.lookup.output#/data/answer`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValuePath {
    canonical: String,
    root: ValuePathRoot,
    fields: Vec<String>,
}

impl ValuePath {
    pub fn parse(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        if value.is_empty() || value.len() > 512 {
            return Err("value path must contain between 1 and 512 bytes".to_string());
        }
        if value.trim() != value {
            return Err("value path must not contain leading or trailing whitespace".to_string());
        }

        let (dot_path, pointer) = match value.split_once('#') {
            Some((dot_path, pointer)) if pointer.starts_with('/') => (dot_path, Some(pointer)),
            Some(_) => {
                return Err(
                    "value path JSON Pointer suffix must start with '/' after '#'".to_string(),
                )
            }
            None => (value.as_str(), None),
        };

        let segments = dot_path.split('.').collect::<Vec<_>>();
        if segments.iter().any(|segment| segment.is_empty()) {
            return Err("value path must use non-empty dot-separated segments".to_string());
        }

        let (root, field_segments) = match segments.as_slice() {
            ["input", fields @ ..] => (ValuePathRoot::Input, fields),
            ["scope", fields @ ..] => (ValuePathRoot::Scope, fields),
            ["run", fields @ ..] => (ValuePathRoot::Run, fields),
            ["steps", step, "output", fields @ ..] => (
                ValuePathRoot::StepOutput {
                    step: Identifier::parse(*step)?,
                },
                fields,
            ),
            _ => {
                return Err(
                    "value path must use input, scope, run, or steps.<step_id>.output".to_string(),
                )
            }
        };
        let mut fields = field_segments
            .iter()
            .map(|field| Identifier::parse(*field).map(|field| field.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(pointer) = pointer {
            fields.extend(parse_json_pointer(pointer)?);
        }

        Ok(Self {
            canonical: value,
            root,
            fields,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    pub fn root(&self) -> &ValuePathRoot {
        &self.root
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    pub fn referenced_step(&self) -> Option<&Identifier> {
        match &self.root {
            ValuePathRoot::StepOutput { step } => Some(step),
            ValuePathRoot::Input | ValuePathRoot::Scope | ValuePathRoot::Run => None,
        }
    }
}

fn parse_json_pointer(pointer: &str) -> Result<Vec<String>, String> {
    debug_assert!(pointer.starts_with('/'));
    pointer[1..]
        .split('/')
        .map(|segment| {
            let mut decoded = String::with_capacity(segment.len());
            let mut characters = segment.chars();
            while let Some(character) = characters.next() {
                if character != '~' {
                    decoded.push(character);
                    continue;
                }
                match characters.next() {
                    Some('0') => decoded.push('~'),
                    Some('1') => decoded.push('/'),
                    _ => {
                        return Err(
                            "value path JSON Pointer contains an invalid '~' escape".to_string()
                        )
                    }
                }
            }
            Ok(decoded)
        })
        .collect()
}

impl fmt::Display for ValuePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.canonical)
    }
}

impl FromStr for ValuePath {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl<'de> Deserialize<'de> for ValuePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(D::Error::custom)
    }
}

impl Serialize for ValuePath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.canonical)
    }
}

/// An explicitly bound string template. Templates never receive the global
/// workflow context implicitly.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TemplateExpr {
    pub text: String,
    #[serde(default)]
    pub bindings: BTreeMap<Identifier, ValueExpr>,
}

/// A typed runtime value expression. The externally tagged representation keeps
/// literal data distinct from references and string rendering.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueExpr {
    Literal(Value),
    From(ValuePath),
    Object(BTreeMap<String, ValueExpr>),
    Array(Vec<ValueExpr>),
    Template(TemplateExpr),
    Prompt(Identifier),
}

impl<'de> Deserialize<'de> for ValueExpr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Value::deserialize(deserializer)?;
        let object = raw.as_object().ok_or_else(|| {
            D::Error::custom(
                "value expression must be an object with exactly one of literal, from, object, array, template, or prompt",
            )
        })?;
        if object.len() != 1 {
            return Err(D::Error::custom(
                "value expression must contain exactly one expression key",
            ));
        }
        let (kind, value) = object.iter().next().expect("length was checked");
        match kind.as_str() {
            "literal" => Ok(Self::Literal(value.clone())),
            "from" => serde_json::from_value(value.clone())
                .map(Self::From)
                .map_err(D::Error::custom),
            "object" => serde_json::from_value(value.clone())
                .map(Self::Object)
                .map_err(D::Error::custom),
            "array" => serde_json::from_value(value.clone())
                .map(Self::Array)
                .map_err(D::Error::custom),
            "template" => serde_json::from_value(value.clone())
                .map(Self::Template)
                .map_err(D::Error::custom),
            "prompt" => serde_json::from_value(value.clone())
                .map(Self::Prompt)
                .map_err(D::Error::custom),
            _ => Err(D::Error::custom(format!(
                "unknown value expression kind '{kind}'"
            ))),
        }
    }
}

impl Serialize for ValueExpr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap as _;

        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            Self::Literal(value) => map.serialize_entry("literal", value)?,
            Self::From(value) => map.serialize_entry("from", value)?,
            Self::Object(value) => map.serialize_entry("object", value)?,
            Self::Array(value) => map.serialize_entry("array", value)?,
            Self::Template(value) => map.serialize_entry("template", value)?,
            Self::Prompt(value) => map.serialize_entry("prompt", value)?,
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::{Identifier, ValueExpr, ValuePath, ValuePathRoot};
    use serde_json::json;

    #[test]
    fn parses_all_value_expression_variants_without_type_coercion() {
        let expression: ValueExpr = yaml_serde::from_str(
            r#"
object:
  exact: {from: steps.analyze.output.data}
  count: {literal: 2}
  enabled: {literal: true}
  values:
    array:
      - {literal: null}
      - {prompt: system}
  label:
    template:
      text: "Result {{ name }}"
      bindings:
        name: {from: input.name}
"#,
        )
        .unwrap();

        let ValueExpr::Object(fields) = expression else {
            panic!("expected an object expression");
        };
        assert_eq!(fields["count"], ValueExpr::Literal(json!(2)));
        assert_eq!(fields["enabled"], ValueExpr::Literal(json!(true)));
        let ValueExpr::From(path) = &fields["exact"] else {
            panic!("expected a typed source path");
        };
        assert_eq!(path.as_str(), "steps.analyze.output.data");
        assert_eq!(
            path.referenced_step().map(Identifier::as_str),
            Some("analyze")
        );
    }

    #[test]
    fn value_path_accepts_only_canonical_static_roots() {
        let input = ValuePath::parse("input.question").unwrap();
        assert_eq!(input.root(), &ValuePathRoot::Input);
        assert_eq!(input.fields()[0], "question");

        let scope = ValuePath::parse("scope").unwrap();
        assert_eq!(scope.root(), &ValuePathRoot::Scope);

        let output = ValuePath::parse("steps.analyze.output.data.answer").unwrap();
        assert_eq!(
            output.referenced_step().map(Identifier::as_str),
            Some("analyze")
        );
        assert_eq!(output.fields(), ["data", "answer"]);

        let pointer = ValuePath::parse("input#/items/0/display-name/a~1b/tilde~0key").unwrap();
        assert_eq!(
            pointer.fields(),
            ["items", "0", "display-name", "a/b", "tilde~key"]
        );
    }

    #[test]
    fn rejects_dynamic_or_non_canonical_paths() {
        for path in [
            "steps[node].output",
            "steps.${node}.output",
            "steps.node[\"output\"]",
            "steps.node",
            "steps.node.result",
            "nodes.node.output",
            "input.items.0",
            "input#items/0",
            "input#/bad~2escape",
            "input..question",
            " input.question",
        ] {
            let source = format!("from: '{path}'");
            assert!(
                yaml_serde::from_str::<ValueExpr>(&source).is_err(),
                "path '{path}' should be rejected"
            );
        }
    }

    #[test]
    fn rejects_unknown_fields_and_implicit_shapes() {
        for source in [
            "template: {text: hello, unknown: true}",
            "template: {text: hello, bindings: {bad-name: {literal: 1}}}",
            "{from: input.question, literal: fallback}",
            "input.question",
            "object: {question: input.question}",
        ] {
            assert!(
                yaml_serde::from_str::<ValueExpr>(source).is_err(),
                "expression '{source}' should be rejected"
            );
        }
    }

    #[test]
    fn object_expression_allows_arbitrary_json_property_names() {
        let expression: ValueExpr = yaml_serde::from_str(
            r#"object:
  display-name: {literal: Ada}
  "": {literal: empty}
"#,
        )
        .unwrap();
        let ValueExpr::Object(fields) = expression else {
            panic!("expected object expression");
        };
        assert_eq!(fields["display-name"], ValueExpr::Literal(json!("Ada")));
        assert_eq!(fields[""], ValueExpr::Literal(json!("empty")));
    }
}
