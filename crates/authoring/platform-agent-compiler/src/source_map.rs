//! Immutable, derived source provenance. This is a diagnostic artifact, never execution state.
use insight_platform_contracts::{canonical_json, Sha256Digest};
use insight_platform_plan::PlanNodeKey;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const AGENT_SOURCE_MAP_VERSION: u32 = 1;
pub const MAX_AGENT_SOURCE_MAP_ENTRIES: usize = 32_768;
pub const MAX_AGENT_SOURCE_MAP_BYTES: usize = 4_194_304;
pub const MAX_AGENT_SOURCE_POINTER_BYTES: usize = 1_024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentSourceTargetV1 {
    Node {
        node_id: PlanNodeKey,
        ir_pointer: String,
    },
    Port {
        node_id: PlanNodeKey,
        ir_pointer: String,
    },
    Expression {
        node_id: PlanNodeKey,
        ir_pointer: String,
    },
}
impl AgentSourceTargetV1 {
    pub fn node_id(&self) -> &PlanNodeKey {
        match self {
            Self::Node { node_id, .. }
            | Self::Port { node_id, .. }
            | Self::Expression { node_id, .. } => node_id,
        }
    }
    pub fn ir_pointer(&self) -> &str {
        match self {
            Self::Node { ir_pointer, .. }
            | Self::Port { ir_pointer, .. }
            | Self::Expression { ir_pointer, .. } => ir_pointer,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSourceSpanV1 {
    pub file: String,
    pub source_pointer: String,
    /// One-based parser source line and Unicode scalar column.
    pub line: u32,
    pub column: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSourceMapEntryV1 {
    pub target: AgentSourceTargetV1,
    pub source: AgentSourceSpanV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSourceMapV1 {
    pub schema_version: u32,
    pub compiler_semantic_identity: Sha256Digest,
    pub source_files_digest: Sha256Digest,
    pub typed_plan_digest: Sha256Digest,
    pub entries: Vec<AgentSourceMapEntryV1>,
}
impl AgentSourceMapV1 {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, crate::AgentBoundaryErrorCode> {
        use crate::AgentBoundaryErrorCode::{
            CompilerInternal, CompilerLimitExceeded, SourceBundleInvalid,
        };
        if self.schema_version != AGENT_SOURCE_MAP_VERSION
            || self.compiler_semantic_identity != crate::compiler_semantic_identity()
            || self.entries.is_empty()
        {
            return Err(SourceBundleInvalid);
        }
        if self.entries.len() > MAX_AGENT_SOURCE_MAP_ENTRIES {
            return Err(CompilerLimitExceeded);
        }
        let mut targets = BTreeSet::new();
        for entry in &self.entries {
            if !targets.insert(entry.target.ir_pointer())
                || entry.source.line == 0
                || entry.source.column == 0
                || entry.source.line as usize > crate::MAX_AGENT_SOURCE_FILE_BYTES
                || entry.source.column as usize > crate::MAX_AGENT_SOURCE_FILE_BYTES
                || PlanNodeKey::new(entry.target.node_id().as_str().to_owned()).is_err()
                || entry.source.file.len() > crate::MAX_AGENT_SOURCE_PATH_BYTES
                || !valid_pointer(entry.target.ir_pointer())
                || !valid_pointer(&entry.source.source_pointer)
                || crate::validate_relative_reference(&entry.source.file, "source map").is_err()
            {
                return Err(SourceBundleInvalid);
            }
        }
        let bytes = canonical_json(&serde_json::to_value(self).map_err(|_| CompilerInternal)?)
            .map_err(|_| CompilerInternal)?;
        if bytes.len() > MAX_AGENT_SOURCE_MAP_BYTES {
            return Err(CompilerLimitExceeded);
        }
        Ok(bytes)
    }
}
fn valid_pointer(value: &str) -> bool {
    if value.len() > MAX_AGENT_SOURCE_POINTER_BYTES
        || (!value.is_empty() && !value.starts_with('/'))
        || value.chars().any(char::is_control)
    {
        return false;
    }
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'~' && !matches!(bytes.next(), Some(b'0' | b'1')) {
            return false;
        }
    }
    true
}

fn escape(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}
fn positions(
    file: &str,
    text: &str,
) -> Result<std::collections::BTreeMap<String, AgentSourceSpanV1>, crate::AgentBoundaryErrorCode> {
    use crate::{AgentBoundaryErrorCode, YamlEventCollector};
    use yaml_rust2::parser::{Event, Parser};
    let mut collector = YamlEventCollector::default();
    Parser::new_from_str(text)
        .load(&mut collector, true)
        .map_err(|_| AgentBoundaryErrorCode::SourceBundleInvalid)?;
    struct Cursor<'a> {
        events: &'a [(Event, yaml_rust2::scanner::Marker)],
        next: usize,
        file: &'a str,
        result: std::collections::BTreeMap<String, AgentSourceSpanV1>,
    }
    impl Cursor<'_> {
        fn node(&mut self, pointer: String, depth: usize) -> Result<(), AgentBoundaryErrorCode> {
            if depth > 32
                || !valid_pointer(&pointer)
                || self.result.len() >= MAX_AGENT_SOURCE_MAP_ENTRIES
            {
                return Err(AgentBoundaryErrorCode::CompilerLimitExceeded);
            }
            let (event, mut marker) = self
                .events
                .get(self.next)
                .cloned()
                .ok_or(AgentBoundaryErrorCode::SourceBundleInvalid)?;
            self.next += 1;
            // YAML block mappings emit their start at the first colon; the first key token is the actual beginning.
            if matches!(event, Event::MappingStart(..)) {
                if let Some((_, first)) = self.events.get(self.next) {
                    if first.index() < marker.index() {
                        marker = *first;
                    }
                }
            }
            let location = AgentSourceSpanV1 {
                file: self.file.into(),
                source_pointer: pointer.clone(),
                line: u32::try_from(marker.line())
                    .map_err(|_| AgentBoundaryErrorCode::CompilerLimitExceeded)?,
                column: u32::try_from(marker.col() + 1)
                    .map_err(|_| AgentBoundaryErrorCode::CompilerLimitExceeded)?,
            };
            if self.result.insert(pointer.clone(), location).is_some() {
                return Err(AgentBoundaryErrorCode::SourceBundleInvalid);
            }
            match event {
                Event::Scalar(..) => {}
                Event::MappingStart(..) => {
                    while !matches!(self.events.get(self.next), Some((Event::MappingEnd, _))) {
                        let Some((Event::Scalar(key, ..), _)) = self.events.get(self.next) else {
                            return Err(AgentBoundaryErrorCode::SourceBundleInvalid);
                        };
                        let child = format!("{pointer}/{}", escape(key));
                        self.next += 1;
                        self.node(child, depth + 1)?;
                    }
                    self.next += 1;
                }
                Event::SequenceStart(..) => {
                    let mut index = 0usize;
                    while !matches!(self.events.get(self.next), Some((Event::SequenceEnd, _))) {
                        self.node(format!("{pointer}/{index}"), depth + 1)?;
                        index += 1;
                    }
                    self.next += 1;
                }
                _ => return Err(AgentBoundaryErrorCode::SourceBundleInvalid),
            }
            Ok(())
        }
    }
    let next = collector
        .events
        .iter()
        .position(|(event, _)| matches!(event, Event::DocumentStart))
        .ok_or(AgentBoundaryErrorCode::SourceBundleInvalid)?
        + 1;
    let mut cursor = Cursor {
        events: &collector.events,
        next,
        file,
        result: Default::default(),
    };
    cursor.node(String::new(), 0)?;
    Ok(cursor.result)
}

pub(crate) fn build_source_map(
    bundle: &crate::AgentSourceBundleV1,
    compiled: &crate::CompiledAgent,
) -> Result<AgentSourceMapV1, crate::AgentBoundaryErrorCode> {
    use crate::AgentBoundaryErrorCode::{
        CompilerInternal, CompilerLimitExceeded, SourceBundleInvalid,
    };
    use insight_platform_plan::{ExactDataPortRef, RuntimePlan, TypedExpressionProgram};
    use serde_json::Value;
    let resolution =
        crate::inspect_manifest(bundle.sources.files[&bundle.sources.manifest_path].as_bytes())
            .map_err(|_| SourceBundleInvalid)?;
    let (file, full) = match resolution.plan_path {
        Some(path) => (path, true),
        None => (bundle.sources.manifest_path.clone(), false),
    };
    let source_positions = positions(
        &file,
        bundle.sources.files.get(&file).ok_or(SourceBundleInvalid)?,
    )?;
    let plan: RuntimePlan =
        serde_json::from_slice(&compiled.typed_plan_bytes).map_err(|_| CompilerInternal)?;
    let plan_json: Value =
        serde_json::from_slice(&compiled.typed_plan_bytes).map_err(|_| CompilerInternal)?;
    struct Entries<'a> {
        positions: &'a std::collections::BTreeMap<String, AgentSourceSpanV1>,
        full: bool,
        entries: Vec<AgentSourceMapEntryV1>,
    }
    impl Entries<'_> {
        fn add(
            &mut self,
            target: AgentSourceTargetV1,
        ) -> Result<(), crate::AgentBoundaryErrorCode> {
            if self.entries.len() >= MAX_AGENT_SOURCE_MAP_ENTRIES {
                return Err(CompilerLimitExceeded);
            }
            let pointer = if self.full {
                target.ir_pointer()
            } else {
                "/spec/execution"
            };
            let source = self
                .positions
                .get(pointer)
                .ok_or(SourceBundleInvalid)?
                .clone();
            self.entries.push(AgentSourceMapEntryV1 { target, source });
            Ok(())
        }
        fn walk(
            &mut self,
            node: &PlanNodeKey,
            pointer: String,
            value: &Value,
            depth: usize,
        ) -> Result<(), crate::AgentBoundaryErrorCode> {
            if depth > 32 || !valid_pointer(&pointer) {
                return Err(CompilerLimitExceeded);
            }
            if value.is_object()
                && serde_json::from_value::<ExactDataPortRef>(value.clone()).is_ok()
            {
                self.add(AgentSourceTargetV1::Port {
                    node_id: node.clone(),
                    ir_pointer: pointer,
                })?;
                return Ok(());
            }
            if let Ok(program) = serde_json::from_value::<TypedExpressionProgram>(value.clone()) {
                for index in 0..program.instructions.len() {
                    self.add(AgentSourceTargetV1::Expression {
                        node_id: node.clone(),
                        ir_pointer: format!("{pointer}/instructions/{index}"),
                    })?;
                }
            }
            match value {
                Value::Object(map) => {
                    for (key, child) in map {
                        if map.get("op").and_then(Value::as_str) == Some("literal")
                            && key == "value"
                        {
                            continue;
                        }
                        self.walk(node, format!("{pointer}/{}", escape(key)), child, depth + 1)?;
                    }
                }
                Value::Array(values) => {
                    for (index, child) in values.iter().enumerate() {
                        self.walk(node, format!("{pointer}/{index}"), child, depth + 1)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
    }
    let mut entries = Entries {
        positions: &source_positions,
        full,
        entries: Vec::new(),
    };
    for (node_id, node) in &plan.nodes {
        let pointer = format!("/nodes/{}", escape(node_id.as_str()));
        entries.add(AgentSourceTargetV1::Node {
            node_id: node_id.clone(),
            ir_pointer: pointer.clone(),
        })?;
        entries.walk(
            node_id,
            pointer,
            &serde_json::to_value(node).map_err(|_| CompilerInternal)?,
            0,
        )?;
    }
    for entry in &entries.entries {
        let expected = format!("/nodes/{}", escape(entry.target.node_id().as_str()));
        if !plan.nodes.contains_key(entry.target.node_id())
            || !(entry.target.ir_pointer() == expected
                || entry
                    .target
                    .ir_pointer()
                    .starts_with(&format!("{expected}/")))
            || plan_json.pointer(entry.target.ir_pointer()).is_none()
        {
            return Err(SourceBundleInvalid);
        }
    }
    let result = AgentSourceMapV1 {
        schema_version: AGENT_SOURCE_MAP_VERSION,
        compiler_semantic_identity: crate::compiler_semantic_identity(),
        source_files_digest: insight_platform_contracts::canonical_digest(
            &serde_json::to_value(&bundle.sources).map_err(|_| CompilerInternal)?,
        )
        .map_err(|_| CompilerInternal)?
        .parse()
        .map_err(|_| CompilerInternal)?,
        typed_plan_digest: compiled.typed_plan_digest.clone(),
        entries: entries.entries,
    };
    result.canonical_bytes()?;
    Ok(result)
}

pub(crate) fn diagnostic_location(
    sources: &crate::AgentSourceFilesV1,
    code: crate::AgentBoundaryErrorCode,
) -> Option<crate::AgentSourceLocationV1> {
    use crate::AgentBoundaryErrorCode;
    use yaml_rust2::parser::Parser;
    if sources.validate().is_err() {
        return None;
    }
    for (file, source) in &sources.files {
        let mut collector = crate::YamlEventCollector::default();
        if let Err(error) = Parser::new_from_str(source).load(&mut collector, true) {
            let marker = error.marker();
            return Some(crate::AgentSourceLocationV1 {
                file: file.clone(),
                source_pointer: String::new(),
                line: marker.line().try_into().ok()?,
                column: (marker.col() + 1).try_into().ok()?,
            });
        }
    }
    let manifest = &sources.files[&sources.manifest_path];
    let resolution = crate::inspect_manifest(manifest.as_bytes());
    if let Ok(resolution) = &resolution {
        for file in [
            &resolution.input_schema_path,
            &resolution.output_schema_path,
        ] {
            let source = sources.files.get(file)?;
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(source) {
                if let Err(error) = insight_platform_contracts::ClosedJsonSchema::build(value) {
                    let source_positions = positions(file, source).ok()?;
                    let pointer = error.path.strip_prefix('$').unwrap_or("");
                    let location = source_positions
                        .get(pointer)
                        .or_else(|| source_positions.get(""))?;
                    return Some(crate::AgentSourceLocationV1 {
                        file: location.file.clone(),
                        source_pointer: location.source_pointer.clone(),
                        line: location.line,
                        column: location.column,
                    });
                }
            }
        }
    }
    let (file, pointer) = match (
        code,
        resolution
            .as_ref()
            .ok()
            .and_then(|value| value.plan_path.as_ref()),
    ) {
        (AgentBoundaryErrorCode::AgentCompileFailed, Some(file)) => (file.as_str(), "/nodes"),
        (AgentBoundaryErrorCode::AgentManifestInvalid, _) => (sources.manifest_path.as_str(), ""),
        (AgentBoundaryErrorCode::AgentCompileFailed, _) => {
            (sources.manifest_path.as_str(), "/spec/execution")
        }
        _ => return None,
    };
    let source_positions = positions(file, sources.files.get(file)?).ok()?;
    let location = source_positions.get(pointer)?;
    Some(crate::AgentSourceLocationV1 {
        file: location.file.clone(),
        source_pointer: location.source_pointer.clone(),
        line: location.line,
        column: location.column,
    })
}

impl AgentSourceMapV1 {
    /// A map is evidence only for these exact source files and typed Plan bytes.
    pub fn validate_for(
        &self,
        bundle: &crate::AgentSourceBundleV1,
        compiled: &crate::CompiledAgent,
    ) -> Result<(), crate::AgentBoundaryErrorCode> {
        self.canonical_bytes()?;
        if self != &build_source_map(bundle, compiled)? {
            return Err(crate::AgentBoundaryErrorCode::SourceBundleInvalid);
        }
        Ok(())
    }
}
pub fn agent_source_map_schema() -> serde_json::Value {
    use serde_json::json;
    let mut digest = insight_platform_contracts::nominal_schemas()
        .remove("Digest")
        .expect("owning Digest schema");
    digest.as_object_mut().unwrap().remove("$id");
    let pointer = json!({"type":"string","maxLength":MAX_AGENT_SOURCE_POINTER_BYTES,"pattern":"^(?:/(?:[^~]|~[01])*)?$"});
    let target = json!({"type":"object","additionalProperties":false,"required":["kind","node_id","ir_pointer"],"properties":{"kind":{"enum":["node","port","expression"]},"node_id":{"type":"string","minLength":1,"maxLength":128,"pattern":"^[A-Za-z0-9][A-Za-z0-9_.:-]*$"},"ir_pointer":pointer}});
    let source = json!({"type":"object","additionalProperties":false,"required":["file","source_pointer","line","column"],"properties":{"file":{"type":"string","minLength":1,"maxLength":crate::MAX_AGENT_SOURCE_PATH_BYTES},"source_pointer":pointer,"line":{"type":"integer","minimum":1,"maximum":crate::MAX_AGENT_SOURCE_FILE_BYTES},"column":{"type":"integer","minimum":1,"maximum":crate::MAX_AGENT_SOURCE_FILE_BYTES}}});
    json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"urn:insight:platform:v1:agent-source-map-v1","type":"object","additionalProperties":false,"x-insight-max-canonical-bytes":MAX_AGENT_SOURCE_MAP_BYTES,"required":["schema_version","compiler_semantic_identity","source_files_digest","typed_plan_digest","entries"],"properties":{"schema_version":{"const":AGENT_SOURCE_MAP_VERSION},"compiler_semantic_identity":{"const":crate::compiler_semantic_identity()},"source_files_digest":digest,"typed_plan_digest":digest,"entries":{"type":"array","minItems":1,"maxItems":MAX_AGENT_SOURCE_MAP_ENTRIES,"items":{"type":"object","additionalProperties":false,"required":["target","source"],"properties":{"target":target,"source":source}}}}})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parser_spans_are_real_unicode_scalar_positions() {
        let source = "{\"title\":\"你好🌍\",\"nodes\":{\"entry\":{\"kind\":\"start\"}}}";
        let actual = positions("plan.json", source).unwrap();
        let entry = &actual["/nodes/entry"];
        let byte = source.find("{\"kind\"").unwrap();
        assert_eq!(
            (entry.line, entry.column),
            (1, source[..byte].chars().count() as u32 + 1)
        );
        assert_ne!(
            entry.column,
            byte as u32 + 1,
            "UTF-8 byte offsets are not columns"
        );
        let yaml = "nodes:\n  entry:\n    kind: start\n";
        let actual = positions("plan.yaml", yaml).unwrap();
        assert_eq!(
            (actual["/nodes/entry"].line, actual["/nodes/entry"].column),
            (3, 5)
        );
        assert!(positions("plan.json", "{\"a\":0,\"a\":1}").is_err());
    }
}
