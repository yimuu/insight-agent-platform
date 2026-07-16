use std::io::{self, Write};

use serde::Deserialize;
use serde_json::Value;

use crate::{
    dsl::{references::is_dsl_identifier, CompileError},
    runtime::{RunContext, RunError},
};

pub(super) const MAX_JSON_CONTENT_BYTES: usize = 262_144;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonContentConfig {
    path: String,
    max_bytes: usize,
}

#[derive(Debug)]
pub(super) struct CompiledJsonContent {
    canonical: String,
    source_node_id: String,
    fields: Vec<String>,
    max_bytes: usize,
    location: String,
}

impl CompiledJsonContent {
    pub(super) fn compile(
        config: Value,
        node_id: &str,
        message_index: usize,
        part_index: usize,
    ) -> Result<Self, CompileError> {
        let location = format!("chat node '{node_id}' message {message_index} part {part_index}");
        let config: JsonContentConfig = serde_json::from_value(config).map_err(|_| {
            CompileError::new(
                "CHAT_JSON_CONTENT_CONFIG_INVALID",
                format!("{location} has invalid JSON content configuration"),
            )
        })?;
        if config.max_bytes == 0 || config.max_bytes > MAX_JSON_CONTENT_BYTES {
            return Err(CompileError::new(
                "CHAT_JSON_CONTENT_CONFIG_INVALID",
                format!("{location} JSON max_bytes must be between 1 and {MAX_JSON_CONTENT_BYTES}"),
            ));
        }

        let segments = config.path.split('.').collect::<Vec<_>>();
        let ["nodes", source_node_id, "output", fields @ ..] = segments.as_slice() else {
            return Err(path_invalid(&location));
        };
        if !is_dsl_identifier(source_node_id)
            || !fields.iter().all(|field| is_dsl_identifier(field))
        {
            return Err(path_invalid(&location));
        }
        let source_node_id = (*source_node_id).to_string();
        let fields = fields.iter().map(|field| (*field).to_string()).collect();

        Ok(Self {
            canonical: config.path,
            source_node_id,
            fields,
            max_bytes: config.max_bytes,
            location,
        })
    }

    pub(super) fn reference(&self) -> &str {
        &self.source_node_id
    }

    pub(super) fn render(&self, context: &RunContext) -> Result<String, RunError> {
        let mut value = context
            .node_output(&self.source_node_id)
            .ok_or_else(|| self.source_missing())?;
        for field in &self.fields {
            value = value
                .as_object()
                .and_then(|object| object.get(field))
                .ok_or_else(|| self.source_missing())?;
        }

        let mut writer = BoundedJsonWriter::new(self.max_bytes);
        if serde_json::to_writer(&mut writer, value).is_err() {
            return if writer.exceeded() {
                Err(self.too_large())
            } else {
                Err(self.serialization_failed())
            };
        }
        String::from_utf8(writer.into_bytes()).map_err(|_| self.serialization_failed())
    }

    fn source_missing(&self) -> RunError {
        RunError::new(
            "CHAT_JSON_CONTENT_SOURCE_MISSING",
            format!(
                "{} JSON source '{}' is missing",
                self.location, self.canonical
            ),
        )
    }

    fn too_large(&self) -> RunError {
        RunError::new(
            "CHAT_JSON_CONTENT_TOO_LARGE",
            format!(
                "{} JSON source '{}' exceeds max_bytes {}",
                self.location, self.canonical, self.max_bytes
            ),
        )
    }

    fn serialization_failed(&self) -> RunError {
        RunError::new(
            "CHAT_JSON_CONTENT_SERIALIZATION_FAILED",
            format!(
                "{} JSON source '{}' could not be serialized",
                self.location, self.canonical
            ),
        )
    }
}

fn path_invalid(location: &str) -> CompileError {
    CompileError::new(
        "CHAT_JSON_CONTENT_PATH_INVALID",
        format!("{location} JSON path must use nodes.<node_id>.output with optional object fields"),
    )
}

struct BoundedJsonWriter {
    bytes: Vec<u8>,
    max_bytes: usize,
    exceeded: bool,
}

impl BoundedJsonWriter {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::new(),
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

impl Write for BoundedJsonWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > self.max_bytes.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("JSON content exceeds its byte limit"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
