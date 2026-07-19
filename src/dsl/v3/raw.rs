use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::Value;
use yaml_rust2::{
    parser::{Event, MarkedEventReceiver, Parser},
    scanner::Marker,
};

use crate::dsl::{DslParseError, DslPath, SourceSpan};

use super::{DUPLICATE_KEY, PARSE_FAILED};

/// Inert v3 wire document. No field in this layer has execution semantics.
/// Validation into the closed author AST is a separate, mandatory step.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RawDocument {
    pub api_version: String,
    pub kind: String,
    #[serde(default)]
    pub metadata: Option<RawMetadata>,
    #[serde(default)]
    pub types: BTreeMap<String, Value>,
    #[serde(default)]
    pub prompts: BTreeMap<String, Value>,
    #[serde(default)]
    pub errors: BTreeMap<String, Value>,
    pub inputs: BTreeMap<String, Value>,
    pub output: Value,
    pub workflow: RawWorkflow,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawMetadata {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RawWorkflow {
    pub steps: Vec<Value>,
}

/// Parse one YAML/JSON document while rejecting duplicate mapping keys before
/// Serde is allowed to materialize a map. This matters for security-sensitive
/// control fields such as `if`, `else`, `settle`, and `return`.
pub fn parse(source: &str) -> Result<RawDocument, DslParseError> {
    reject_duplicate_keys(source)?;
    yaml_serde::from_str(source).map_err(|error| {
        let path = error_path(&error.to_string()).unwrap_or_default();
        let span = error
            .location()
            .map(|location| SourceSpan::point(source, location.index()));
        DslParseError::new(PARSE_FAILED, "failed to parse the v3 agent document").at(path, span)
    })
}

#[derive(Default)]
struct Collector {
    events: Vec<(Event, Marker)>,
}

impl MarkedEventReceiver for Collector {
    fn on_event(&mut self, event: Event, marker: Marker) {
        self.events.push((event, marker));
    }
}

fn reject_duplicate_keys(source: &str) -> Result<(), DslParseError> {
    let mut collector = Collector::default();
    let mut parser = Parser::new_from_str(source);
    parser.load(&mut collector, true).map_err(|error| {
        DslParseError::new(PARSE_FAILED, "failed to parse the v3 agent document").at(
            DslPath::root(),
            Some(SourceSpan::point(
                source,
                marker_byte(source, error.marker()),
            )),
        )
    })?;

    let mut cursor = EventCursor {
        source,
        events: &collector.events,
        position: 0,
    };
    cursor.expect(|event| matches!(event, Event::StreamStart))?;
    cursor.expect(|event| matches!(event, Event::DocumentStart))?;
    cursor.read_node(&DslPath::root(), 0)?;
    cursor.expect(|event| matches!(event, Event::DocumentEnd))?;
    cursor.expect(|event| matches!(event, Event::StreamEnd))?;
    if cursor.position != cursor.events.len() {
        return Err(cursor.error(DslPath::root(), source.len()));
    }
    Ok(())
}

struct EventCursor<'a> {
    source: &'a str,
    events: &'a [(Event, Marker)],
    position: usize,
}

impl EventCursor<'_> {
    fn expect(&mut self, predicate: impl FnOnce(&Event) -> bool) -> Result<(), DslParseError> {
        let Some((event, marker)) = self.events.get(self.position) else {
            return Err(self.error(DslPath::root(), self.source.len()));
        };
        if !predicate(event) {
            return Err(self.error(DslPath::root(), marker_byte(self.source, marker)));
        }
        self.position += 1;
        Ok(())
    }

    fn read_node(&mut self, path: &DslPath, depth: usize) -> Result<Option<String>, DslParseError> {
        if depth > 256 {
            return Err(self.error(path.clone(), self.current_byte()));
        }
        let Some((event, marker)) = self.events.get(self.position).cloned() else {
            return Err(self.error(path.clone(), self.source.len()));
        };
        self.position += 1;
        match event {
            Event::Scalar(value, _, _, tag) => {
                if tag.is_some() {
                    return Err(self.error(path.clone(), marker_byte(self.source, &marker)));
                }
                Ok(Some(value))
            }
            Event::SequenceStart(_, tag) => {
                if tag.is_some() {
                    return Err(self.error(path.clone(), marker_byte(self.source, &marker)));
                }
                let mut index = 0;
                while !matches!(
                    self.events.get(self.position),
                    Some((Event::SequenceEnd, _))
                ) {
                    self.read_node(&path.child_index(index), depth + 1)?;
                    index += 1;
                }
                self.position += 1;
                Ok(None)
            }
            Event::MappingStart(_, tag) => {
                if tag.is_some() {
                    return Err(self.error(path.clone(), marker_byte(self.source, &marker)));
                }
                let mut keys = BTreeSet::new();
                while !matches!(self.events.get(self.position), Some((Event::MappingEnd, _))) {
                    let key_byte = self.current_byte();
                    let Some(key) = self.read_node(path, depth + 1)? else {
                        return Err(self.error(path.clone(), key_byte));
                    };
                    let key_path = path.child_key(key.clone());
                    if !keys.insert(key) {
                        return Err(DslParseError::new(
                            DUPLICATE_KEY,
                            "v3 documents must not contain duplicate mapping keys",
                        )
                        .at(key_path, Some(SourceSpan::point(self.source, key_byte))));
                    }
                    self.read_node(&key_path, depth + 1)?;
                }
                self.position += 1;
                Ok(None)
            }
            Event::Alias(_) => Err(self.error(path.clone(), marker_byte(self.source, &marker))),
            _ => Err(self.error(path.clone(), marker_byte(self.source, &marker))),
        }
    }

    fn current_byte(&self) -> usize {
        self.events
            .get(self.position)
            .map_or(self.source.len(), |(_, marker)| {
                marker_byte(self.source, marker)
            })
    }

    fn error(&self, path: DslPath, byte: usize) -> DslParseError {
        DslParseError::new(PARSE_FAILED, "failed to parse the v3 agent document")
            .at(path, Some(SourceSpan::point(self.source, byte)))
    }
}

fn marker_byte(source: &str, marker: &Marker) -> usize {
    // yaml-rust2 marker indices count Unicode scalar values. Translate them to
    // byte offsets before constructing the public byte span.
    source
        .char_indices()
        .nth(marker.index())
        .map_or(source.len(), |(byte, _)| byte)
}

fn error_path(rendered: &str) -> Option<DslPath> {
    let (path, _) = rendered.split_once(": ")?;
    DslPath::from_serde_path(path)
}

#[cfg(test)]
mod tests {
    use super::parse;
    use crate::dsl::v3::DUPLICATE_KEY;

    #[test]
    fn duplicate_keys_are_rejected_before_materialization() {
        let error = parse(
            "api_version: insight.agent/v3\nkind: agent\nkind: agent\ninputs: {}\noutput: string\nworkflow: {steps: []}\n",
        )
        .unwrap_err();
        assert_eq!(error.code(), DUPLICATE_KEY);
        assert_eq!(error.path().unwrap().to_string(), "$.kind");
    }
}
