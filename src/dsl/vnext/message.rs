use std::fmt;

use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};

use super::value::{Identifier, LocalInputPath, LocalInputRef};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthoredRole {
    System,
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoredContentAtom {
    Prompt(Identifier),
    InlineText(String),
    RuntimeText(LocalInputRef),
    Image(LocalInputRef),
}

impl AuthoredContentAtom {
    fn parse(value: Value) -> Result<Self, String> {
        match value {
            Value::String(prompt) => Identifier::parse(prompt).map(Self::Prompt),
            Value::Object(object) => parse_content_object(object),
            _ => Err(
                "content atom must be a Prompt ID or a closed text, from, or image object"
                    .to_string(),
            ),
        }
    }
}

impl Serialize for AuthoredContentAtom {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap as _;

        match self {
            Self::Prompt(prompt) => prompt.serialize(serializer),
            Self::InlineText(text) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("text", text)?;
                map.end()
            }
            Self::RuntimeText(reference) => reference.serialize(serializer),
            Self::Image(reference) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("image", reference)?;
                map.end()
            }
        }
    }
}

fn parse_content_object(object: Map<String, Value>) -> Result<AuthoredContentAtom, String> {
    if object.len() != 1 {
        return Err("content object must contain exactly one discriminator".to_string());
    }
    let (kind, value) = object.into_iter().next().expect("one entry was checked");
    match kind.as_str() {
        "text" => value
            .as_str()
            .map(|text| AuthoredContentAtom::InlineText(text.to_string()))
            .ok_or_else(|| "inline content text must be a string".to_string()),
        "from" => serde_json::from_value::<LocalInputPath>(value)
            .map(|from| AuthoredContentAtom::RuntimeText(LocalInputRef { from }))
            .map_err(|error| error.to_string()),
        "image" => serde_json::from_value::<LocalInputRef>(value)
            .map(AuthoredContentAtom::Image)
            .map_err(|error| error.to_string()),
        _ => Err(format!("unknown authored content discriminator '{kind}'")),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoredContentExpr {
    Single(AuthoredContentAtom),
    Parts(Vec<AuthoredContentAtom>),
}

impl AuthoredContentExpr {
    pub fn atoms(&self) -> &[AuthoredContentAtom] {
        match self {
            Self::Single(atom) => std::slice::from_ref(atom),
            Self::Parts(atoms) => atoms,
        }
    }
}

impl<'de> Deserialize<'de> for AuthoredContentExpr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::Array(values) => {
                if values.is_empty() {
                    return Err(D::Error::custom("authored content list must not be empty"));
                }
                values
                    .into_iter()
                    .map(AuthoredContentAtom::parse)
                    .collect::<Result<Vec<_>, _>>()
                    .map(Self::Parts)
                    .map_err(D::Error::custom)
            }
            value => {
                let atom = AuthoredContentAtom::parse(value).map_err(D::Error::custom)?;
                if matches!(atom, AuthoredContentAtom::Image(_)) {
                    return Err(D::Error::custom(
                        "image content must appear in a content atom list",
                    ));
                }
                Ok(Self::Single(atom))
            }
        }
    }
}

impl Serialize for AuthoredContentExpr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Single(atom) => atom.serialize(serializer),
            Self::Parts(atoms) => atoms.serialize(serializer),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AuthoredMessageTemplate {
    pub role: AuthoredRole,
    pub content: AuthoredContentExpr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageSource {
    Authored(AuthoredMessageTemplate),
    Dynamic(LocalInputRef),
}

impl MessageSource {
    fn parse(value: Value) -> Result<Self, String> {
        let object = value
            .as_object()
            .ok_or_else(|| "message source must be an object".to_string())?;
        if object.len() == 1 && object.contains_key("from") {
            return serde_json::from_value(value)
                .map(Self::Dynamic)
                .map_err(|error| error.to_string());
        }
        serde_json::from_value(value)
            .map(Self::Authored)
            .map_err(|error| error.to_string())
    }
}

impl Serialize for MessageSource {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Authored(message) => message.serialize(serializer),
            Self::Dynamic(reference) => reference.serialize(serializer),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageListExpr {
    Dynamic(LocalInputRef),
    Sources(Vec<MessageSource>),
}

impl MessageListExpr {
    pub fn sources(&self) -> Option<&[MessageSource]> {
        match self {
            Self::Dynamic(_) => None,
            Self::Sources(sources) => Some(sources),
        }
    }
}

impl<'de> Deserialize<'de> for MessageListExpr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::Object(_) => serde_json::from_value(value)
                .map(Self::Dynamic)
                .map_err(D::Error::custom),
            Value::Array(values) => {
                if values.is_empty() {
                    return Err(D::Error::custom("messages source list must not be empty"));
                }
                values
                    .into_iter()
                    .map(MessageSource::parse)
                    .collect::<Result<Vec<_>, _>>()
                    .map(Self::Sources)
                    .map_err(D::Error::custom)
            }
            _ => Err(D::Error::custom(
                "messages must be a local input reference or non-empty source list",
            )),
        }
    }
}

impl Serialize for MessageListExpr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Dynamic(reference) => reference.serialize(serializer),
            Self::Sources(sources) => sources.serialize(serializer),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResponseConfig {
    Text,
    TextSchema { schema: Value },
    Json { schema: Value },
}

impl<'de> Deserialize<'de> for ResponseConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let mut object = value
            .as_object()
            .cloned()
            .ok_or_else(|| D::Error::custom("LLM response must be a closed object"))?;
        let format = object
            .remove("format")
            .and_then(|value| value.as_str().map(str::to_string))
            .ok_or_else(|| D::Error::custom("LLM response format must be a string"))?;
        match format.as_str() {
            "text" if object.is_empty() => Ok(Self::Text),
            "text" if object.len() == 1 && object.contains_key("schema") => Ok(Self::TextSchema {
                schema: object.remove("schema").expect("schema key was checked"),
            }),
            "json" if object.len() == 1 && object.contains_key("schema") => Ok(Self::Json {
                schema: object.remove("schema").expect("schema key was checked"),
            }),
            "text" | "json" => Err(D::Error::custom(
                "LLM response fields do not match the selected format",
            )),
            _ => Err(D::Error::custom("unknown LLM response format")),
        }
    }
}

impl Serialize for ResponseConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap as _;

        let mut map = serializer.serialize_map(Some(match self {
            Self::Text => 1,
            Self::TextSchema { .. } | Self::Json { .. } => 2,
        }))?;
        match self {
            Self::Text => map.serialize_entry("format", "text")?,
            Self::TextSchema { schema } => {
                map.serialize_entry("format", "text")?;
                map.serialize_entry("schema", schema)?;
            }
            Self::Json { schema } => {
                map.serialize_entry("format", "json")?;
                map.serialize_entry("schema", schema)?;
            }
        }
        map.end()
    }
}

impl fmt::Display for AuthoredRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::System => "system",
            Self::User => "user",
            Self::Assistant => "assistant",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AuthoredContentAtom, AuthoredContentExpr, AuthoredRole, MessageListExpr, MessageSource,
        ResponseConfig,
    };

    #[test]
    fn parses_prompt_biased_content_and_dynamic_sources() {
        let messages: MessageListExpr = yaml_serde::from_str(
            r#"
- role: system
  content: system
- {from: inputs.history}
- role: user
  content:
    - health_advice
    - {text: The current question follows.}
    - {from: inputs.question}
    - image: {from: inputs.image_url}
"#,
        )
        .unwrap();
        let MessageListExpr::Sources(sources) = messages else {
            panic!("expected authored source list");
        };
        assert_eq!(sources.len(), 3);
        let MessageSource::Authored(system) = &sources[0] else {
            panic!("expected authored system message");
        };
        assert_eq!(system.role, AuthoredRole::System);
        assert!(matches!(
            system.content,
            AuthoredContentExpr::Single(AuthoredContentAtom::Prompt(_))
        ));
        let MessageSource::Dynamic(history) = &sources[1] else {
            panic!("expected dynamic history");
        };
        assert_eq!(history.from.binding().as_str(), "history");
        let MessageSource::Authored(user) = &sources[2] else {
            panic!("expected authored user message");
        };
        assert_eq!(user.content.atoms().len(), 4);
    }

    #[test]
    fn parses_direct_message_list_reference_and_closed_response() {
        let messages: MessageListExpr = yaml_serde::from_str("from: inputs.messages").unwrap();
        assert!(matches!(messages, MessageListExpr::Dynamic(_)));
        let response: ResponseConfig = yaml_serde::from_str("format: text").unwrap();
        assert!(matches!(response, ResponseConfig::Text));
        let response: ResponseConfig = yaml_serde::from_str(
            "format: json\nschema: {type: object, additionalProperties: false}",
        )
        .unwrap();
        assert!(matches!(response, ResponseConfig::Json { .. }));
        let response: ResponseConfig =
            yaml_serde::from_str("format: text\nschema: {type: string, minLength: 1}").unwrap();
        assert!(matches!(
            response,
            ResponseConfig::TextSchema { schema }
                if schema == serde_json::json!({"type": "string", "minLength": 1})
        ));
    }

    #[test]
    fn rejects_aliases_implicit_text_nested_lists_and_open_objects() {
        for source in [
            "[]",
            "spread: history",
            "concat: []",
            "'{{ history }}'",
            "- '...history'",
            "- role: user\n  content: literal prose",
            "- role: user\n  content: []",
            "- role: user\n  content: [[system]]",
            "- role: user\n  content: {prompt: system}",
            "- role: user\n  content: {text: ok, from: inputs.question}",
            "- role: user\n  content: {from: input.question}",
            "- role: user\n  content: {image: {from: inputs.image_url}}",
            "- role: user\n  content: system\n  unknown: true",
            "- {from: inputs.history, role: user}",
        ] {
            assert!(
                yaml_serde::from_str::<MessageListExpr>(source).is_err(),
                "source should be rejected: {source}"
            );
        }
    }

    #[test]
    fn quoted_scalar_is_still_a_prompt_identifier() {
        let content: AuthoredContentExpr = yaml_serde::from_str("'system'").unwrap();
        assert!(matches!(
            content,
            AuthoredContentExpr::Single(AuthoredContentAtom::Prompt(_))
        ));
        assert!(yaml_serde::from_str::<AuthoredContentExpr>("'literal prose'").is_err());
    }
}
