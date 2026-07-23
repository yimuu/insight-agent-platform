//! Public `response-stream/v1` protocol and its transient delivery contracts.
//!
//! The types in this module deliberately separate two representations:
//!
//! - [`ResponseStreamEvent`] is the closed, serializable caller contract;
//! - [`LiveResponsePublication`] is an internal, non-serializable envelope
//!   carrying Attempt and item-local ordering authority.
//!
//! Concrete in-memory and shared broker adapters live above this crate. The
//! shared bounded queue primitive remains here so every adapter applies the
//! same ordering, gap, seal, and byte-limit rules.

use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    fmt,
    sync::Mutex,
};

use async_trait::async_trait;
use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use tokio::sync::Notify;

use crate::{ActivationId, ArtifactRef, AttemptNo, RunId};

mod durable;
mod retrieval_public_projection;
mod tool_public_projection;

pub use durable::{DurableResponseSnapshot, ResponseTerminalKind, ResponseUsageStatus};
pub use retrieval_public_projection::WorkflowRetrievalPublicProjection;
pub use tool_public_projection::{
    WorkflowToolCompletedArgumentsProjection, WorkflowToolPublicProjection,
};

pub const RESPONSE_STREAM_PROTOCOL_VERSION: &str = "response-stream/v1";

const LIVE_RESPONSE_CONFIG_INVALID: &str = "LIVE_RESPONSE_CONFIG_INVALID";
const LIVE_RESPONSE_STREAM_CLOSED: &str = "LIVE_RESPONSE_STREAM_CLOSED";
const LIVE_RESPONSE_IDENTITY_INVALID: &str = "LIVE_RESPONSE_IDENTITY_INVALID";
const LIVE_RESPONSE_FUNCTION_CALL_INVALID: &str = "LIVE_RESPONSE_FUNCTION_CALL_INVALID";
const MAX_PUBLIC_LABEL_BYTES: usize = 256;
pub const MAX_FUNCTION_CALL_ARGUMENT_BYTES: usize = 256 * 1_024;
const MAX_FUNCTION_CALL_ARGUMENT_DEPTH: usize = 64;
const MAX_FUNCTION_CALL_ARGUMENT_VALUES: usize = 16_384;
const WORKFLOW_PUBLIC_RESULT_INVALID: &str = "WORKFLOW_PUBLIC_RESULT_INVALID";
const MAX_WORKFLOW_PUBLIC_TEXT_BYTES: usize = 64 * 1_024;
const MAX_WORKFLOW_PUBLIC_JSON_BYTES: usize = 64 * 1_024;
const MAX_WORKFLOW_PUBLIC_JSON_DEPTH: usize = 32;
const MAX_WORKFLOW_PUBLIC_JSON_VALUES: usize = 4_096;
const MAX_WORKFLOW_PUBLIC_JSON_STRING_BYTES: usize = 16 * 1_024;
const MAX_WORKFLOW_TOOL_CONTENT_PARTS: usize = 128;
const MAX_WORKFLOW_RETRIEVAL_RESULTS: usize = 256;
const MAX_WORKFLOW_RETRIEVAL_QUERY_BYTES: usize = 16 * 1_024;
const MAX_WORKFLOW_RETRIEVAL_TITLE_BYTES: usize = 4 * 1_024;
const MAX_WORKFLOW_RETRIEVAL_URI_BYTES: usize = 8 * 1_024;
const MAX_WORKFLOW_RETRIEVAL_SNIPPET_BYTES: usize = 64 * 1_024;
const MAX_WORKFLOW_RETRIEVAL_METADATA_BYTES: usize = 16 * 1_024;
const MAX_WORKFLOW_RETRIEVAL_METADATA_ENTRIES: usize = 128;

/// Exact public event set frozen by `response-stream/v1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResponseStreamEventType {
    ResponseCreated,
    ResponseInProgress,
    ResponseOutputItemAdded,
    ResponseContentPartAdded,
    ResponseOutputTextDelta,
    ResponseOutputTextDone,
    ResponseContentPartDone,
    ResponseFunctionCallArgumentsDelta,
    ResponseFunctionCallArgumentsDone,
    ResponseOutputItemDone,
    ResponseFileSearchCallInProgress,
    ResponseFileSearchCallSearching,
    ResponseFileSearchCallCompleted,
    ResponseCompleted,
    ResponseFailed,
    Error,
    WorkflowToolStarted,
    WorkflowToolCompleted,
    WorkflowToolFailed,
    WorkflowRetrievalCompleted,
    WorkflowStreamGap,
    WorkflowResponseTimedOut,
    WorkflowResponseCancelled,
    WorkflowResponseInterrupted,
}

impl ResponseStreamEventType {
    pub const ALL: [Self; 24] = [
        Self::ResponseCreated,
        Self::ResponseInProgress,
        Self::ResponseOutputItemAdded,
        Self::ResponseContentPartAdded,
        Self::ResponseOutputTextDelta,
        Self::ResponseOutputTextDone,
        Self::ResponseContentPartDone,
        Self::ResponseFunctionCallArgumentsDelta,
        Self::ResponseFunctionCallArgumentsDone,
        Self::ResponseOutputItemDone,
        Self::ResponseFileSearchCallInProgress,
        Self::ResponseFileSearchCallSearching,
        Self::ResponseFileSearchCallCompleted,
        Self::ResponseCompleted,
        Self::ResponseFailed,
        Self::Error,
        Self::WorkflowToolStarted,
        Self::WorkflowToolCompleted,
        Self::WorkflowToolFailed,
        Self::WorkflowRetrievalCompleted,
        Self::WorkflowStreamGap,
        Self::WorkflowResponseTimedOut,
        Self::WorkflowResponseCancelled,
        Self::WorkflowResponseInterrupted,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResponseCreated => "response.created",
            Self::ResponseInProgress => "response.in_progress",
            Self::ResponseOutputItemAdded => "response.output_item.added",
            Self::ResponseContentPartAdded => "response.content_part.added",
            Self::ResponseOutputTextDelta => "response.output_text.delta",
            Self::ResponseOutputTextDone => "response.output_text.done",
            Self::ResponseContentPartDone => "response.content_part.done",
            Self::ResponseFunctionCallArgumentsDelta => "response.function_call_arguments.delta",
            Self::ResponseFunctionCallArgumentsDone => "response.function_call_arguments.done",
            Self::ResponseOutputItemDone => "response.output_item.done",
            Self::ResponseFileSearchCallInProgress => "response.file_search_call.in_progress",
            Self::ResponseFileSearchCallSearching => "response.file_search_call.searching",
            Self::ResponseFileSearchCallCompleted => "response.file_search_call.completed",
            Self::ResponseCompleted => "response.completed",
            Self::ResponseFailed => "response.failed",
            Self::Error => "error",
            Self::WorkflowToolStarted => "workflow.tool.started",
            Self::WorkflowToolCompleted => "workflow.tool.completed",
            Self::WorkflowToolFailed => "workflow.tool.failed",
            Self::WorkflowRetrievalCompleted => "workflow.retrieval.completed",
            Self::WorkflowStreamGap => "workflow.stream.gap",
            Self::WorkflowResponseTimedOut => "workflow.response.timed_out",
            Self::WorkflowResponseCancelled => "workflow.response.cancelled",
            Self::WorkflowResponseInterrupted => "workflow.response.interrupted",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|event_type| event_type.as_str() == value)
    }

    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::ResponseCompleted
                | Self::ResponseFailed
                | Self::WorkflowResponseTimedOut
                | Self::WorkflowResponseCancelled
                | Self::WorkflowResponseInterrupted
        )
    }
}

impl Serialize for ResponseStreamEventType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ResponseStreamEventType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).ok_or_else(|| D::Error::custom("unknown response-stream/v1 event type"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseObjectKind {
    Response,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseStatus {
    InProgress,
    Completed,
    Failed,
    Cancelled,
    Incomplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseItemStatus {
    InProgress,
    Completed,
    Failed,
    Incomplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseRole {
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResponseContentPart {
    OutputText {
        text: String,
        #[serde(default)]
        annotations: Vec<Value>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResponseOutputItem {
    Message {
        id: String,
        status: ResponseItemStatus,
        role: ResponseRole,
        content: Vec<ResponseContentPart>,
    },
    FunctionCall {
        id: String,
        status: ResponseItemStatus,
        call_id: String,
        name: String,
        arguments: String,
    },
    FileSearchCall {
        id: String,
        status: ResponseItemStatus,
        #[serde(default)]
        queries: Vec<String>,
        #[serde(default)]
        results: Vec<Value>,
    },
}

impl ResponseOutputItem {
    pub fn id(&self) -> &str {
        match self {
            Self::Message { id, .. }
            | Self::FunctionCall { id, .. }
            | Self::FileSearchCall { id, .. } => id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicResponseError {
    pub code: String,
    pub message: String,
    pub param: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseUsageInputDetails {
    pub cached_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseUsageOutputDetails {
    pub reasoning_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseUsage {
    pub input_tokens: u64,
    pub input_tokens_details: ResponseUsageInputDetails,
    pub output_tokens: u64,
    pub output_tokens_details: ResponseUsageOutputDetails,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicResponse {
    pub id: String,
    pub object: ResponseObjectKind,
    pub status: ResponseStatus,
    #[serde(default)]
    pub output: Vec<ResponseOutputItem>,
    pub usage: Option<ResponseUsage>,
    pub error: Option<PublicResponseError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowUsageStatus {
    Complete,
    Partial,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowPublicError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStopReason {
    Cancelled,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowCompleted {
    pub run_id: String,
    pub result: Value,
    #[serde(default)]
    pub tool_results: Vec<WorkflowToolResult>,
    #[serde(default)]
    pub retrievals: Vec<WorkflowRetrieval>,
    pub usage_status: WorkflowUsageStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowFailure {
    pub run_id: String,
    pub error: WorkflowPublicError,
    #[serde(default)]
    pub tool_results: Vec<WorkflowToolResult>,
    #[serde(default)]
    pub retrievals: Vec<WorkflowRetrieval>,
    pub usage_status: WorkflowUsageStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowStopped {
    pub run_id: String,
    pub reason: WorkflowStopReason,
    #[serde(default)]
    pub tool_results: Vec<WorkflowToolResult>,
    #[serde(default)]
    pub retrievals: Vec<WorkflowRetrieval>,
    pub usage_status: WorkflowUsageStatus,
}

/// Validation failure for a caller-visible tool or retrieval result.
///
/// The error deliberately has one stable public code and a body-free message:
/// rejected provider or executor values must not be reflected into logs or a
/// response stream while the safe public projection is being built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowPublicResultError {
    message: &'static str,
}

impl WorkflowPublicResultError {
    const fn new(message: &'static str) -> Self {
        Self { message }
    }

    pub const fn code(&self) -> &'static str {
        WORKFLOW_PUBLIC_RESULT_INVALID
    }

    pub const fn message(&self) -> &'static str {
        self.message
    }
}

impl fmt::Display for WorkflowPublicResultError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl Error for WorkflowPublicResultError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum WorkflowToolContentWire {
    #[serde(rename = "output_text")]
    Text { text: String },
    #[serde(rename = "output_json")]
    Json { json: Value },
    #[serde(rename = "output_image")]
    Image { artifact: ArtifactRef },
    #[serde(rename = "output_file")]
    File { artifact: ArtifactRef },
    #[serde(rename = "output_audio")]
    Audio { artifact: ArtifactRef },
}

/// Closed public content union for a completed workflow tool call.
///
/// The inner representation is private so in-process producers cannot bypass
/// the same limits enforced when a durable terminal snapshot is decoded.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowToolContent {
    wire: WorkflowToolContentWire,
}

impl WorkflowToolContent {
    pub fn output_text(text: impl Into<String>) -> Result<Self, WorkflowPublicResultError> {
        let text = text.into();
        validate_bounded_public_string(
            &text,
            MAX_WORKFLOW_PUBLIC_TEXT_BYTES,
            "workflow tool text must be non-empty and bounded",
        )?;
        Ok(Self {
            wire: WorkflowToolContentWire::Text { text },
        })
    }

    pub fn output_json(json: Value) -> Result<Self, WorkflowPublicResultError> {
        validate_bounded_public_json(&json, MAX_WORKFLOW_PUBLIC_JSON_BYTES)?;
        Ok(Self {
            wire: WorkflowToolContentWire::Json { json },
        })
    }

    pub fn output_image(artifact: ArtifactRef) -> Self {
        Self {
            wire: WorkflowToolContentWire::Image { artifact },
        }
    }

    pub fn output_file(artifact: ArtifactRef) -> Self {
        Self {
            wire: WorkflowToolContentWire::File { artifact },
        }
    }

    pub fn output_audio(artifact: ArtifactRef) -> Self {
        Self {
            wire: WorkflowToolContentWire::Audio { artifact },
        }
    }

    pub fn text(&self) -> Option<&str> {
        match &self.wire {
            WorkflowToolContentWire::Text { text } => Some(text),
            WorkflowToolContentWire::Json { .. }
            | WorkflowToolContentWire::Image { .. }
            | WorkflowToolContentWire::File { .. }
            | WorkflowToolContentWire::Audio { .. } => None,
        }
    }

    pub fn json(&self) -> Option<&Value> {
        match &self.wire {
            WorkflowToolContentWire::Json { json } => Some(json),
            WorkflowToolContentWire::Text { .. }
            | WorkflowToolContentWire::Image { .. }
            | WorkflowToolContentWire::File { .. }
            | WorkflowToolContentWire::Audio { .. } => None,
        }
    }

    pub fn artifact(&self) -> Option<&ArtifactRef> {
        match &self.wire {
            WorkflowToolContentWire::Image { artifact }
            | WorkflowToolContentWire::File { artifact }
            | WorkflowToolContentWire::Audio { artifact } => Some(artifact),
            WorkflowToolContentWire::Text { .. } | WorkflowToolContentWire::Json { .. } => None,
        }
    }
}

impl Serialize for WorkflowToolContent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.wire.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for WorkflowToolContent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match WorkflowToolContentWire::deserialize(deserializer)? {
            WorkflowToolContentWire::Text { text } => {
                Self::output_text(text).map_err(D::Error::custom)
            }
            WorkflowToolContentWire::Json { json } => {
                Self::output_json(json).map_err(D::Error::custom)
            }
            WorkflowToolContentWire::Image { artifact } => Ok(Self::output_image(artifact)),
            WorkflowToolContentWire::File { artifact } => Ok(Self::output_file(artifact)),
            WorkflowToolContentWire::Audio { artifact } => Ok(Self::output_audio(artifact)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WorkflowToolResult {
    call_id: String,
    tool_name: String,
    content: Vec<WorkflowToolContent>,
}

impl WorkflowToolResult {
    pub fn new(
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        content: Vec<WorkflowToolContent>,
    ) -> Result<Self, WorkflowPublicResultError> {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        if !valid_public_label(&call_id) || !valid_public_label(&tool_name) {
            return Err(WorkflowPublicResultError::new(
                "workflow tool identities must be stable public labels",
            ));
        }
        if content.len() > MAX_WORKFLOW_TOOL_CONTENT_PARTS {
            return Err(WorkflowPublicResultError::new(
                "workflow tool content exceeds the public part limit",
            ));
        }
        Ok(Self {
            call_id,
            tool_name,
            content,
        })
    }

    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }

    pub fn content(&self) -> &[WorkflowToolContent] {
        &self.content
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowToolResultWire {
    call_id: String,
    tool_name: String,
    content: Vec<WorkflowToolContent>,
}

impl<'de> Deserialize<'de> for WorkflowToolResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = WorkflowToolResultWire::deserialize(deserializer)?;
        Self::new(wire.call_id, wire.tool_name, wire.content).map_err(D::Error::custom)
    }
}

/// Bounded object-valued metadata already projected by a retrieval policy.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
#[serde(transparent)]
pub struct WorkflowRetrievalMetadata {
    entries: BTreeMap<String, Value>,
}

impl WorkflowRetrievalMetadata {
    pub fn new(entries: BTreeMap<String, Value>) -> Result<Self, WorkflowPublicResultError> {
        if entries.len() > MAX_WORKFLOW_RETRIEVAL_METADATA_ENTRIES
            || entries.keys().any(|key| {
                key.is_empty()
                    || key.len() > MAX_PUBLIC_LABEL_BYTES
                    || key.chars().any(char::is_control)
            })
        {
            return Err(WorkflowPublicResultError::new(
                "workflow retrieval metadata keys must be non-empty and bounded",
            ));
        }
        let value = Value::Object(entries.clone().into_iter().collect());
        validate_bounded_public_json(&value, MAX_WORKFLOW_RETRIEVAL_METADATA_BYTES)?;
        Ok(Self { entries })
    }

    pub fn entries(&self) -> &BTreeMap<String, Value> {
        &self.entries
    }
}

impl<'de> Deserialize<'de> for WorkflowRetrievalMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = BTreeMap::<String, Value>::deserialize(deserializer)?;
        Self::new(entries).map_err(D::Error::custom)
    }
}

/// One closed, caller-visible retrieval result.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WorkflowRetrievalResult {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snippet: Option<String>,
    metadata: WorkflowRetrievalMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact: Option<ArtifactRef>,
}

impl WorkflowRetrievalResult {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: impl Into<String>,
        title: Option<String>,
        uri: Option<String>,
        score: Option<f64>,
        snippet: Option<String>,
        metadata: WorkflowRetrievalMetadata,
        artifact: Option<ArtifactRef>,
    ) -> Result<Self, WorkflowPublicResultError> {
        let id = id.into();
        if !valid_public_label(&id) {
            return Err(WorkflowPublicResultError::new(
                "workflow retrieval result ID must be a stable public label",
            ));
        }
        validate_optional_public_string(
            title.as_deref(),
            MAX_WORKFLOW_RETRIEVAL_TITLE_BYTES,
            "workflow retrieval title must be non-empty and bounded",
        )?;
        validate_optional_public_string(
            snippet.as_deref(),
            MAX_WORKFLOW_RETRIEVAL_SNIPPET_BYTES,
            "workflow retrieval snippet must be non-empty and bounded",
        )?;
        if uri.as_deref().is_some_and(|uri| {
            uri.is_empty()
                || uri.len() > MAX_WORKFLOW_RETRIEVAL_URI_BYTES
                || uri
                    .chars()
                    .any(|character| character.is_control() || character.is_whitespace())
        }) {
            return Err(WorkflowPublicResultError::new(
                "workflow retrieval URI must be non-empty, bounded, and whitespace-free",
            ));
        }
        if score.is_some_and(|score| !score.is_finite()) {
            return Err(WorkflowPublicResultError::new(
                "workflow retrieval score must be finite",
            ));
        }
        Ok(Self {
            id,
            title,
            uri,
            score,
            snippet,
            metadata,
            artifact,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    pub fn uri(&self) -> Option<&str> {
        self.uri.as_deref()
    }

    pub fn score(&self) -> Option<f64> {
        self.score
    }

    pub fn snippet(&self) -> Option<&str> {
        self.snippet.as_deref()
    }

    pub fn metadata(&self) -> &WorkflowRetrievalMetadata {
        &self.metadata
    }

    pub fn artifact(&self) -> Option<&ArtifactRef> {
        self.artifact.as_ref()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowRetrievalResultWire {
    id: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    uri: Option<String>,
    #[serde(default)]
    score: Option<f64>,
    #[serde(default)]
    snippet: Option<String>,
    #[serde(default)]
    metadata: WorkflowRetrievalMetadata,
    #[serde(default)]
    artifact: Option<ArtifactRef>,
}

impl<'de> Deserialize<'de> for WorkflowRetrievalResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = WorkflowRetrievalResultWire::deserialize(deserializer)?;
        Self::new(
            wire.id,
            wire.title,
            wire.uri,
            wire.score,
            wire.snippet,
            wire.metadata,
            wire.artifact,
        )
        .map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WorkflowRetrieval {
    retrieval_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<String>,
    results: Vec<WorkflowRetrievalResult>,
}

impl WorkflowRetrieval {
    pub fn new(
        retrieval_id: impl Into<String>,
        query: Option<String>,
        results: Vec<WorkflowRetrievalResult>,
    ) -> Result<Self, WorkflowPublicResultError> {
        let retrieval_id = retrieval_id.into();
        if !valid_public_label(&retrieval_id) {
            return Err(WorkflowPublicResultError::new(
                "workflow retrieval ID must be a stable public label",
            ));
        }
        validate_optional_public_string(
            query.as_deref(),
            MAX_WORKFLOW_RETRIEVAL_QUERY_BYTES,
            "workflow retrieval query must be non-empty and bounded",
        )?;
        if results.len() > MAX_WORKFLOW_RETRIEVAL_RESULTS {
            return Err(WorkflowPublicResultError::new(
                "workflow retrieval exceeds the public result limit",
            ));
        }
        Ok(Self {
            retrieval_id,
            query,
            results,
        })
    }

    pub fn retrieval_id(&self) -> &str {
        &self.retrieval_id
    }

    pub fn query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    pub fn results(&self) -> &[WorkflowRetrievalResult] {
        &self.results
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowRetrievalWire {
    retrieval_id: String,
    #[serde(default)]
    query: Option<String>,
    results: Vec<WorkflowRetrievalResult>,
}

impl<'de> Deserialize<'de> for WorkflowRetrieval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = WorkflowRetrievalWire::deserialize(deserializer)?;
        Self::new(wire.retrieval_id, wire.query, wire.results).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStreamGapAction {
    DiscardProvisionalItem,
}

/// Closed public wire envelope. Internal Attempt, activation, model-call and
/// item-local sequence fields have no representation in this enum.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ResponseStreamEvent {
    #[serde(rename = "response.created")]
    ResponseCreated {
        sequence_number: u64,
        response: PublicResponse,
    },
    #[serde(rename = "response.in_progress")]
    ResponseInProgress {
        sequence_number: u64,
        response: PublicResponse,
    },
    #[serde(rename = "response.output_item.added")]
    ResponseOutputItemAdded {
        sequence_number: u64,
        output_index: u32,
        item: ResponseOutputItem,
    },
    #[serde(rename = "response.content_part.added")]
    ResponseContentPartAdded {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
        content_index: u32,
        part: ResponseContentPart,
    },
    #[serde(rename = "response.output_text.delta")]
    ResponseOutputTextDelta {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
        content_index: u32,
        delta: String,
    },
    #[serde(rename = "response.output_text.done")]
    ResponseOutputTextDone {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
        content_index: u32,
        text: String,
    },
    #[serde(rename = "response.content_part.done")]
    ResponseContentPartDone {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
        content_index: u32,
        part: ResponseContentPart,
    },
    #[serde(rename = "response.function_call_arguments.delta")]
    ResponseFunctionCallArgumentsDelta {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
        delta: String,
    },
    #[serde(rename = "response.function_call_arguments.done")]
    ResponseFunctionCallArgumentsDone {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
        name: String,
        arguments: String,
    },
    #[serde(rename = "response.output_item.done")]
    ResponseOutputItemDone {
        sequence_number: u64,
        output_index: u32,
        item: ResponseOutputItem,
    },
    #[serde(rename = "response.file_search_call.in_progress")]
    ResponseFileSearchCallInProgress {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
    },
    #[serde(rename = "response.file_search_call.searching")]
    ResponseFileSearchCallSearching {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
    },
    #[serde(rename = "response.file_search_call.completed")]
    ResponseFileSearchCallCompleted {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
    },
    #[serde(rename = "response.completed")]
    ResponseCompleted {
        sequence_number: u64,
        response: PublicResponse,
        workflow: WorkflowCompleted,
    },
    #[serde(rename = "response.failed")]
    ResponseFailed {
        sequence_number: u64,
        response: PublicResponse,
        workflow: WorkflowFailure,
    },
    #[serde(rename = "error")]
    Error {
        sequence_number: u64,
        code: String,
        message: String,
        param: Option<String>,
    },
    #[serde(rename = "workflow.tool.started")]
    WorkflowToolStarted {
        sequence_number: u64,
        call_id: String,
        tool_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        arguments: Option<Value>,
    },
    #[serde(rename = "workflow.tool.completed")]
    WorkflowToolCompleted {
        sequence_number: u64,
        call_id: String,
        tool_name: String,
        content: Vec<WorkflowToolContent>,
    },
    #[serde(rename = "workflow.tool.failed")]
    WorkflowToolFailed {
        sequence_number: u64,
        call_id: String,
        tool_name: String,
        error: WorkflowPublicError,
    },
    #[serde(rename = "workflow.retrieval.completed")]
    WorkflowRetrievalCompleted {
        sequence_number: u64,
        retrieval_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        query: Option<String>,
        results: Vec<WorkflowRetrievalResult>,
    },
    #[serde(rename = "workflow.stream.gap")]
    WorkflowStreamGap {
        sequence_number: u64,
        item_id: String,
        attempt_no: u32,
        missing_from: u64,
        missing_to: Option<u64>,
        unknown_tail: bool,
        action: WorkflowStreamGapAction,
    },
    #[serde(rename = "workflow.response.timed_out")]
    WorkflowResponseTimedOut {
        sequence_number: u64,
        response: PublicResponse,
        workflow: WorkflowFailure,
    },
    #[serde(rename = "workflow.response.cancelled")]
    WorkflowResponseCancelled {
        sequence_number: u64,
        response: PublicResponse,
        workflow: WorkflowStopped,
    },
    #[serde(rename = "workflow.response.interrupted")]
    WorkflowResponseInterrupted {
        sequence_number: u64,
        response: PublicResponse,
        workflow: WorkflowStopped,
    },
}

impl ResponseStreamEvent {
    pub const fn event_type(&self) -> ResponseStreamEventType {
        match self {
            Self::ResponseCreated { .. } => ResponseStreamEventType::ResponseCreated,
            Self::ResponseInProgress { .. } => ResponseStreamEventType::ResponseInProgress,
            Self::ResponseOutputItemAdded { .. } => {
                ResponseStreamEventType::ResponseOutputItemAdded
            }
            Self::ResponseContentPartAdded { .. } => {
                ResponseStreamEventType::ResponseContentPartAdded
            }
            Self::ResponseOutputTextDelta { .. } => {
                ResponseStreamEventType::ResponseOutputTextDelta
            }
            Self::ResponseOutputTextDone { .. } => ResponseStreamEventType::ResponseOutputTextDone,
            Self::ResponseContentPartDone { .. } => {
                ResponseStreamEventType::ResponseContentPartDone
            }
            Self::ResponseFunctionCallArgumentsDelta { .. } => {
                ResponseStreamEventType::ResponseFunctionCallArgumentsDelta
            }
            Self::ResponseFunctionCallArgumentsDone { .. } => {
                ResponseStreamEventType::ResponseFunctionCallArgumentsDone
            }
            Self::ResponseOutputItemDone { .. } => ResponseStreamEventType::ResponseOutputItemDone,
            Self::ResponseFileSearchCallInProgress { .. } => {
                ResponseStreamEventType::ResponseFileSearchCallInProgress
            }
            Self::ResponseFileSearchCallSearching { .. } => {
                ResponseStreamEventType::ResponseFileSearchCallSearching
            }
            Self::ResponseFileSearchCallCompleted { .. } => {
                ResponseStreamEventType::ResponseFileSearchCallCompleted
            }
            Self::ResponseCompleted { .. } => ResponseStreamEventType::ResponseCompleted,
            Self::ResponseFailed { .. } => ResponseStreamEventType::ResponseFailed,
            Self::Error { .. } => ResponseStreamEventType::Error,
            Self::WorkflowToolStarted { .. } => ResponseStreamEventType::WorkflowToolStarted,
            Self::WorkflowToolCompleted { .. } => ResponseStreamEventType::WorkflowToolCompleted,
            Self::WorkflowToolFailed { .. } => ResponseStreamEventType::WorkflowToolFailed,
            Self::WorkflowRetrievalCompleted { .. } => {
                ResponseStreamEventType::WorkflowRetrievalCompleted
            }
            Self::WorkflowStreamGap { .. } => ResponseStreamEventType::WorkflowStreamGap,
            Self::WorkflowResponseTimedOut { .. } => {
                ResponseStreamEventType::WorkflowResponseTimedOut
            }
            Self::WorkflowResponseCancelled { .. } => {
                ResponseStreamEventType::WorkflowResponseCancelled
            }
            Self::WorkflowResponseInterrupted { .. } => {
                ResponseStreamEventType::WorkflowResponseInterrupted
            }
        }
    }

    pub const fn sequence_number(&self) -> u64 {
        match self {
            Self::ResponseCreated {
                sequence_number, ..
            }
            | Self::ResponseInProgress {
                sequence_number, ..
            }
            | Self::ResponseOutputItemAdded {
                sequence_number, ..
            }
            | Self::ResponseContentPartAdded {
                sequence_number, ..
            }
            | Self::ResponseOutputTextDelta {
                sequence_number, ..
            }
            | Self::ResponseOutputTextDone {
                sequence_number, ..
            }
            | Self::ResponseContentPartDone {
                sequence_number, ..
            }
            | Self::ResponseFunctionCallArgumentsDelta {
                sequence_number, ..
            }
            | Self::ResponseFunctionCallArgumentsDone {
                sequence_number, ..
            }
            | Self::ResponseOutputItemDone {
                sequence_number, ..
            }
            | Self::ResponseFileSearchCallInProgress {
                sequence_number, ..
            }
            | Self::ResponseFileSearchCallSearching {
                sequence_number, ..
            }
            | Self::ResponseFileSearchCallCompleted {
                sequence_number, ..
            }
            | Self::ResponseCompleted {
                sequence_number, ..
            }
            | Self::ResponseFailed {
                sequence_number, ..
            }
            | Self::Error {
                sequence_number, ..
            }
            | Self::WorkflowToolStarted {
                sequence_number, ..
            }
            | Self::WorkflowToolCompleted {
                sequence_number, ..
            }
            | Self::WorkflowToolFailed {
                sequence_number, ..
            }
            | Self::WorkflowRetrievalCompleted {
                sequence_number, ..
            }
            | Self::WorkflowStreamGap {
                sequence_number, ..
            }
            | Self::WorkflowResponseTimedOut {
                sequence_number, ..
            }
            | Self::WorkflowResponseCancelled {
                sequence_number, ..
            }
            | Self::WorkflowResponseInterrupted {
                sequence_number, ..
            } => *sequence_number,
        }
    }

    pub const fn is_terminal(&self) -> bool {
        self.event_type().is_terminal()
    }
}

/// Internal identity of one durable model output item. It is intentionally not
/// serializable; only `item_id`, `output_index`, and selected safe fields are
/// projected into public `response.*` events by the dispatcher.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LiveResponseItemIdentity {
    run_id: RunId,
    activation_id: ActivationId,
    attempt_no: AttemptNo,
    model_call_no: u32,
    item_id: String,
    output_index: u32,
}

impl LiveResponseItemIdentity {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_id: RunId,
        activation_id: ActivationId,
        attempt_no: AttemptNo,
        model_call_no: u32,
        item_id: impl Into<String>,
        output_index: u32,
    ) -> Result<Self, LiveResponseBrokerError> {
        let item_id = item_id.into();
        if model_call_no == 0 || !valid_public_label(&item_id) {
            return Err(LiveResponseBrokerError::new(
                LIVE_RESPONSE_IDENTITY_INVALID,
                "live response item identity is invalid",
            ));
        }
        Ok(Self {
            run_id,
            activation_id,
            attempt_no,
            model_call_no,
            item_id,
            output_index,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn attempt_no(&self) -> AttemptNo {
        self.attempt_no
    }

    pub fn model_call_no(&self) -> u32 {
        self.model_call_no
    }

    pub fn item_id(&self) -> &str {
        &self.item_id
    }

    pub fn output_index(&self) -> u32 {
        self.output_index
    }
}

impl fmt::Debug for LiveResponseItemIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveResponseItemIdentity")
            .field("run_id", &self.run_id)
            .field("activation_id", &self.activation_id)
            .field("attempt_no", &self.attempt_no)
            .field("model_call_no", &self.model_call_no)
            .field("item_id", &self.item_id)
            .field("output_index", &self.output_index)
            .finish()
    }
}

/// Internal identity of one best-effort workflow observation source.
///
/// Unlike [`LiveResponseItemIdentity`], this identity deliberately has no
/// model-call, item, or output-index fields. Workflow tool and retrieval
/// observations are not durable Response output items and therefore cannot
/// participate in item seals, gaps, or the terminal manifest barrier.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LiveWorkflowObservationIdentity {
    run_id: RunId,
    activation_id: ActivationId,
    attempt_no: AttemptNo,
    source_id: String,
}

impl LiveWorkflowObservationIdentity {
    pub fn new(
        run_id: RunId,
        activation_id: ActivationId,
        attempt_no: AttemptNo,
        source_id: impl Into<String>,
    ) -> Result<Self, LiveResponseBrokerError> {
        let source_id = source_id.into();
        if !valid_public_label(&source_id) {
            return Err(LiveResponseBrokerError::new(
                LIVE_RESPONSE_IDENTITY_INVALID,
                "live workflow observation identity is invalid",
            ));
        }
        Ok(Self {
            run_id,
            activation_id,
            attempt_no,
            source_id,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn attempt_no(&self) -> AttemptNo {
        self.attempt_no
    }

    /// Stable producer-defined identity within an Attempt, such as a durable
    /// tool-call or retrieval identifier. It is never exposed on public wire.
    pub fn source_id(&self) -> &str {
        &self.source_id
    }
}

impl fmt::Debug for LiveWorkflowObservationIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveWorkflowObservationIdentity")
            .field("run_id", &self.run_id)
            .field("activation_id", &self.activation_id)
            .field("attempt_no", &self.attempt_no)
            .field("source_id", &self.source_id)
            .finish()
    }
}

/// Closed internal source identity for live response publication.
///
/// The variants are intentionally not interchangeable: `response.*` payloads
/// require [`Self::OutputItem`], while public workflow observations require
/// [`Self::WorkflowObservation`]. [`LiveResponsePublication`] enforces that
/// contract at construction and PostgreSQL decode boundaries.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LiveResponseSourceIdentity {
    OutputItem(LiveResponseItemIdentity),
    WorkflowObservation(LiveWorkflowObservationIdentity),
}

impl LiveResponseSourceIdentity {
    pub fn run_id(&self) -> &RunId {
        match self {
            Self::OutputItem(identity) => identity.run_id(),
            Self::WorkflowObservation(identity) => identity.run_id(),
        }
    }

    pub fn output_item(&self) -> Option<&LiveResponseItemIdentity> {
        match self {
            Self::OutputItem(identity) => Some(identity),
            Self::WorkflowObservation(_) => None,
        }
    }

    pub fn workflow_observation(&self) -> Option<&LiveWorkflowObservationIdentity> {
        match self {
            Self::OutputItem(_) => None,
            Self::WorkflowObservation(identity) => Some(identity),
        }
    }
}

impl From<LiveResponseItemIdentity> for LiveResponseSourceIdentity {
    fn from(identity: LiveResponseItemIdentity) -> Self {
        Self::OutputItem(identity)
    }
}

impl From<LiveWorkflowObservationIdentity> for LiveResponseSourceIdentity {
    fn from(identity: LiveWorkflowObservationIdentity) -> Self {
        Self::WorkflowObservation(identity)
    }
}

impl fmt::Debug for LiveResponseSourceIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputItem(identity) => {
                formatter.debug_tuple("OutputItem").field(identity).finish()
            }
            Self::WorkflowObservation(identity) => formatter
                .debug_tuple("WorkflowObservation")
                .field(identity)
                .finish(),
        }
    }
}

/// Safe, already-authorized public projection before connection sequencing.
/// This type has a body-free custom `Debug` implementation and intentionally
/// has no `Serialize` implementation.
#[derive(Clone, PartialEq)]
pub enum LiveResponsePayload {
    OutputItemAdded {
        item: ResponseOutputItem,
    },
    ContentPartAdded {
        content_index: u32,
        part: ResponseContentPart,
    },
    OutputTextDelta {
        content_index: u32,
        delta: String,
    },
    OutputTextDone {
        content_index: u32,
        text: String,
    },
    ContentPartDone {
        content_index: u32,
        part: ResponseContentPart,
    },
    FunctionCallArgumentsDelta {
        delta: String,
    },
    FunctionCallArgumentsDone {
        name: String,
        arguments: String,
    },
    OutputItemDone {
        item: ResponseOutputItem,
    },
    FileSearchCallInProgress,
    FileSearchCallSearching,
    FileSearchCallCompleted,
    ToolStarted {
        call_id: String,
        tool_name: String,
        arguments: Option<Value>,
    },
    ToolCompleted {
        call_id: String,
        tool_name: String,
        content: Vec<WorkflowToolContent>,
    },
    ToolFailed {
        call_id: String,
        tool_name: String,
        error: WorkflowPublicError,
    },
    RetrievalCompleted {
        retrieval_id: String,
        query: Option<String>,
        results: Vec<WorkflowRetrievalResult>,
    },
}

impl LiveResponsePayload {
    pub const fn event_type(&self) -> ResponseStreamEventType {
        match self {
            Self::OutputItemAdded { .. } => ResponseStreamEventType::ResponseOutputItemAdded,
            Self::ContentPartAdded { .. } => ResponseStreamEventType::ResponseContentPartAdded,
            Self::OutputTextDelta { .. } => ResponseStreamEventType::ResponseOutputTextDelta,
            Self::OutputTextDone { .. } => ResponseStreamEventType::ResponseOutputTextDone,
            Self::ContentPartDone { .. } => ResponseStreamEventType::ResponseContentPartDone,
            Self::FunctionCallArgumentsDelta { .. } => {
                ResponseStreamEventType::ResponseFunctionCallArgumentsDelta
            }
            Self::FunctionCallArgumentsDone { .. } => {
                ResponseStreamEventType::ResponseFunctionCallArgumentsDone
            }
            Self::OutputItemDone { .. } => ResponseStreamEventType::ResponseOutputItemDone,
            Self::FileSearchCallInProgress => {
                ResponseStreamEventType::ResponseFileSearchCallInProgress
            }
            Self::FileSearchCallSearching => {
                ResponseStreamEventType::ResponseFileSearchCallSearching
            }
            Self::FileSearchCallCompleted => {
                ResponseStreamEventType::ResponseFileSearchCallCompleted
            }
            Self::ToolStarted { .. } => ResponseStreamEventType::WorkflowToolStarted,
            Self::ToolCompleted { .. } => ResponseStreamEventType::WorkflowToolCompleted,
            Self::ToolFailed { .. } => ResponseStreamEventType::WorkflowToolFailed,
            Self::RetrievalCompleted { .. } => ResponseStreamEventType::WorkflowRetrievalCompleted,
        }
    }

    const fn requires_output_item_source(&self) -> bool {
        matches!(
            self,
            Self::OutputItemAdded { .. }
                | Self::ContentPartAdded { .. }
                | Self::OutputTextDelta { .. }
                | Self::OutputTextDone { .. }
                | Self::ContentPartDone { .. }
                | Self::FunctionCallArgumentsDelta { .. }
                | Self::FunctionCallArgumentsDone { .. }
                | Self::OutputItemDone { .. }
                | Self::FileSearchCallInProgress
                | Self::FileSearchCallSearching
                | Self::FileSearchCallCompleted
        )
    }

    const fn requires_workflow_observation_source(&self) -> bool {
        matches!(
            self,
            Self::ToolStarted { .. }
                | Self::ToolCompleted { .. }
                | Self::ToolFailed { .. }
                | Self::RetrievalCompleted { .. }
        )
    }
}

impl fmt::Debug for LiveResponsePayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveResponsePayload")
            .field("event_type", &self.event_type())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq)]
pub struct LiveResponsePublication {
    source: LiveResponseSourceIdentity,
    local_sequence: u64,
    payload: LiveResponsePayload,
}

impl LiveResponsePublication {
    /// Constructs one durable Response output-item publication.
    ///
    /// Workflow tool and retrieval payloads are rejected; use
    /// [`Self::new_workflow_observation`] for those observations.
    pub fn new(
        identity: LiveResponseItemIdentity,
        local_sequence: u64,
        payload: LiveResponsePayload,
    ) -> Result<Self, LiveResponseBrokerError> {
        Self::from_source(identity.into(), local_sequence, payload)
    }

    /// Constructs one best-effort workflow tool or retrieval observation.
    /// Response output-item payloads are rejected.
    pub fn new_workflow_observation(
        identity: LiveWorkflowObservationIdentity,
        local_sequence: u64,
        payload: LiveResponsePayload,
    ) -> Result<Self, LiveResponseBrokerError> {
        Self::from_source(identity.into(), local_sequence, payload)
    }

    pub(crate) fn from_source(
        source: LiveResponseSourceIdentity,
        local_sequence: u64,
        payload: LiveResponsePayload,
    ) -> Result<Self, LiveResponseBrokerError> {
        let source_matches = match &source {
            LiveResponseSourceIdentity::OutputItem(identity) => {
                payload.requires_output_item_source()
                    && match &payload {
                        LiveResponsePayload::OutputItemAdded { item }
                        | LiveResponsePayload::OutputItemDone { item } => {
                            item.id() == identity.item_id()
                        }
                        _ => true,
                    }
            }
            LiveResponseSourceIdentity::WorkflowObservation(_) => {
                payload.requires_workflow_observation_source()
            }
        };
        if !source_matches {
            return Err(LiveResponseBrokerError::new(
                LIVE_RESPONSE_IDENTITY_INVALID,
                "live response payload does not match its source identity",
            ));
        }
        Ok(Self {
            source,
            local_sequence,
            payload,
        })
    }

    pub fn source(&self) -> &LiveResponseSourceIdentity {
        &self.source
    }

    pub fn output_item_identity(&self) -> Option<&LiveResponseItemIdentity> {
        self.source.output_item()
    }

    pub fn workflow_observation_identity(&self) -> Option<&LiveWorkflowObservationIdentity> {
        self.source.workflow_observation()
    }

    pub fn run_id(&self) -> &RunId {
        self.source.run_id()
    }

    pub fn local_sequence(&self) -> u64 {
        self.local_sequence
    }

    pub fn payload_type(&self) -> ResponseStreamEventType {
        self.payload.event_type()
    }

    pub(crate) fn payload(&self) -> &LiveResponsePayload {
        &self.payload
    }

    fn public_wire_bytes(&self) -> usize {
        serde_json::to_vec(&self.clone().into_public_event(u64::MAX))
            .map(|encoded| encoded.len())
            .unwrap_or(usize::MAX)
    }

    /// Applies the connection-local sequence and drops all internal ordering
    /// and execution identity fields from the public wire value.
    pub fn into_public_event(self, sequence_number: u64) -> ResponseStreamEvent {
        let LiveResponsePublication {
            source, payload, ..
        } = self;
        match (source, payload) {
            (
                LiveResponseSourceIdentity::OutputItem(identity),
                LiveResponsePayload::OutputItemAdded { item },
            ) => ResponseStreamEvent::ResponseOutputItemAdded {
                sequence_number,
                output_index: identity.output_index,
                item,
            },
            (
                LiveResponseSourceIdentity::OutputItem(identity),
                LiveResponsePayload::ContentPartAdded {
                    content_index,
                    part,
                },
            ) => ResponseStreamEvent::ResponseContentPartAdded {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
                content_index,
                part,
            },
            (
                LiveResponseSourceIdentity::OutputItem(identity),
                LiveResponsePayload::OutputTextDelta {
                    content_index,
                    delta,
                },
            ) => ResponseStreamEvent::ResponseOutputTextDelta {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
                content_index,
                delta,
            },
            (
                LiveResponseSourceIdentity::OutputItem(identity),
                LiveResponsePayload::OutputTextDone {
                    content_index,
                    text,
                },
            ) => ResponseStreamEvent::ResponseOutputTextDone {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
                content_index,
                text,
            },
            (
                LiveResponseSourceIdentity::OutputItem(identity),
                LiveResponsePayload::ContentPartDone {
                    content_index,
                    part,
                },
            ) => ResponseStreamEvent::ResponseContentPartDone {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
                content_index,
                part,
            },
            (
                LiveResponseSourceIdentity::OutputItem(identity),
                LiveResponsePayload::FunctionCallArgumentsDelta { delta },
            ) => ResponseStreamEvent::ResponseFunctionCallArgumentsDelta {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
                delta,
            },
            (
                LiveResponseSourceIdentity::OutputItem(identity),
                LiveResponsePayload::FunctionCallArgumentsDone { name, arguments },
            ) => ResponseStreamEvent::ResponseFunctionCallArgumentsDone {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
                name,
                arguments,
            },
            (
                LiveResponseSourceIdentity::OutputItem(identity),
                LiveResponsePayload::OutputItemDone { item },
            ) => ResponseStreamEvent::ResponseOutputItemDone {
                sequence_number,
                output_index: identity.output_index,
                item,
            },
            (
                LiveResponseSourceIdentity::OutputItem(identity),
                LiveResponsePayload::FileSearchCallInProgress,
            ) => ResponseStreamEvent::ResponseFileSearchCallInProgress {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
            },
            (
                LiveResponseSourceIdentity::OutputItem(identity),
                LiveResponsePayload::FileSearchCallSearching,
            ) => ResponseStreamEvent::ResponseFileSearchCallSearching {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
            },
            (
                LiveResponseSourceIdentity::OutputItem(identity),
                LiveResponsePayload::FileSearchCallCompleted,
            ) => ResponseStreamEvent::ResponseFileSearchCallCompleted {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
            },
            (
                LiveResponseSourceIdentity::WorkflowObservation(_),
                LiveResponsePayload::ToolStarted {
                    call_id,
                    tool_name,
                    arguments,
                },
            ) => ResponseStreamEvent::WorkflowToolStarted {
                sequence_number,
                call_id,
                tool_name,
                arguments,
            },
            (
                LiveResponseSourceIdentity::WorkflowObservation(_),
                LiveResponsePayload::ToolCompleted {
                    call_id,
                    tool_name,
                    content,
                },
            ) => ResponseStreamEvent::WorkflowToolCompleted {
                sequence_number,
                call_id,
                tool_name,
                content,
            },
            (
                LiveResponseSourceIdentity::WorkflowObservation(_),
                LiveResponsePayload::ToolFailed {
                    call_id,
                    tool_name,
                    error,
                },
            ) => ResponseStreamEvent::WorkflowToolFailed {
                sequence_number,
                call_id,
                tool_name,
                error,
            },
            (
                LiveResponseSourceIdentity::WorkflowObservation(_),
                LiveResponsePayload::RetrievalCompleted {
                    retrieval_id,
                    query,
                    results,
                },
            ) => ResponseStreamEvent::WorkflowRetrievalCompleted {
                sequence_number,
                retrieval_id,
                query,
                results,
            },
            _ => unreachable!("LiveResponsePublication validates source and payload pairing"),
        }
    }
}

impl fmt::Debug for LiveResponsePublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveResponsePublication")
            .field("source", &self.source)
            .field("local_sequence", &self.local_sequence)
            .field("event_type", &self.payload.event_type())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveResponseSealStatus {
    Completed,
    Incomplete,
}

#[derive(Clone, PartialEq, Eq)]
pub struct LiveResponseSeal {
    identity: LiveResponseItemIdentity,
    last_local_sequence: Option<u64>,
    status: LiveResponseSealStatus,
}

impl LiveResponseSeal {
    pub fn new(
        identity: LiveResponseItemIdentity,
        last_local_sequence: Option<u64>,
        status: LiveResponseSealStatus,
    ) -> Self {
        Self {
            identity,
            last_local_sequence,
            status,
        }
    }

    pub fn identity(&self) -> &LiveResponseItemIdentity {
        &self.identity
    }

    pub fn last_local_sequence(&self) -> Option<u64> {
        self.last_local_sequence
    }

    pub fn status(&self) -> LiveResponseSealStatus {
        self.status
    }
}

impl fmt::Debug for LiveResponseSeal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveResponseSeal")
            .field("identity", &self.identity)
            .field("last_local_sequence", &self.last_local_sequence)
            .field("status", &self.status)
            .finish()
    }
}

/// Canonical four-frame publication plan for one already-completed model
/// function-call item.
///
/// Durable activation code should use this builder instead of assembling
/// function-call frames independently. The plan fixes item status, arguments,
/// local sequence numbers, and the matching completed seal as one contract.
#[derive(Clone, PartialEq)]
pub struct CompletedFunctionCallPublication {
    publications: [LiveResponsePublication; 4],
    seal: LiveResponseSeal,
}

impl CompletedFunctionCallPublication {
    pub const LAST_LOCAL_SEQUENCE: u64 = 3;

    pub fn build(
        identity: LiveResponseItemIdentity,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments_jcs: impl Into<String>,
    ) -> Result<Self, LiveResponseBrokerError> {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        let arguments_jcs = arguments_jcs.into();
        if !valid_public_label(&call_id) || !valid_public_label(&tool_name) {
            return Err(invalid_completed_function_call());
        }
        validate_function_call_arguments(&arguments_jcs)?;

        let item_id = identity.item_id().to_owned();
        let added_item = ResponseOutputItem::FunctionCall {
            id: item_id.clone(),
            status: ResponseItemStatus::InProgress,
            call_id: call_id.clone(),
            name: tool_name.clone(),
            arguments: String::new(),
        };
        let completed_item = ResponseOutputItem::FunctionCall {
            id: item_id,
            status: ResponseItemStatus::Completed,
            call_id,
            name: tool_name.clone(),
            arguments: arguments_jcs.clone(),
        };
        let publications = [
            LiveResponsePublication::new(
                identity.clone(),
                0,
                LiveResponsePayload::OutputItemAdded { item: added_item },
            )?,
            LiveResponsePublication::new(
                identity.clone(),
                1,
                LiveResponsePayload::FunctionCallArgumentsDelta {
                    delta: arguments_jcs.clone(),
                },
            )?,
            LiveResponsePublication::new(
                identity.clone(),
                2,
                LiveResponsePayload::FunctionCallArgumentsDone {
                    name: tool_name,
                    arguments: arguments_jcs,
                },
            )?,
            LiveResponsePublication::new(
                identity.clone(),
                Self::LAST_LOCAL_SEQUENCE,
                LiveResponsePayload::OutputItemDone {
                    item: completed_item,
                },
            )?,
        ];
        let seal = LiveResponseSeal::new(
            identity,
            Some(Self::LAST_LOCAL_SEQUENCE),
            LiveResponseSealStatus::Completed,
        );
        Ok(Self { publications, seal })
    }

    pub fn publications(&self) -> &[LiveResponsePublication; 4] {
        &self.publications
    }

    pub fn seal(&self) -> &LiveResponseSeal {
        &self.seal
    }

    pub fn into_parts(self) -> ([LiveResponsePublication; 4], LiveResponseSeal) {
        (self.publications, self.seal)
    }
}

impl fmt::Debug for CompletedFunctionCallPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompletedFunctionCallPublication")
            .field("identity", self.seal.identity())
            .field("publication_count", &self.publications.len())
            .field("last_local_sequence", &Self::LAST_LOCAL_SEQUENCE)
            .field("seal_status", &self.seal.status())
            .finish()
    }
}

/// Completion-only tail for a function item whose `added` frame and exact
/// Provider argument fragments were already published provisionally. The
/// durable checkpoint supplies the final seal watermark, so this builder can
/// be replayed by another runtime without retaining any transient fragment.
#[derive(Clone, PartialEq)]
pub struct CompletedFunctionCallTailPublication {
    publications: [LiveResponsePublication; 2],
    seal: LiveResponseSeal,
}

impl CompletedFunctionCallTailPublication {
    pub fn build(
        identity: LiveResponseItemIdentity,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments_jcs: impl Into<String>,
        seal_index: u64,
    ) -> Result<Self, LiveResponseBrokerError> {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        let arguments_jcs = arguments_jcs.into();
        if seal_index < 3 || !valid_public_label(&call_id) || !valid_public_label(&tool_name) {
            return Err(invalid_completed_function_call());
        }
        validate_function_call_arguments(&arguments_jcs)?;
        let done_sequence = seal_index
            .checked_sub(1)
            .ok_or_else(invalid_completed_function_call)?;
        let completed_item = ResponseOutputItem::FunctionCall {
            id: identity.item_id().to_owned(),
            status: ResponseItemStatus::Completed,
            call_id,
            name: tool_name.clone(),
            arguments: arguments_jcs.clone(),
        };
        let publications = [
            LiveResponsePublication::new(
                identity.clone(),
                done_sequence,
                LiveResponsePayload::FunctionCallArgumentsDone {
                    name: tool_name,
                    arguments: arguments_jcs,
                },
            )?,
            LiveResponsePublication::new(
                identity.clone(),
                seal_index,
                LiveResponsePayload::OutputItemDone {
                    item: completed_item,
                },
            )?,
        ];
        let seal = LiveResponseSeal::new(
            identity,
            Some(seal_index),
            LiveResponseSealStatus::Completed,
        );
        Ok(Self { publications, seal })
    }

    pub fn into_parts(self) -> ([LiveResponsePublication; 2], LiveResponseSeal) {
        (self.publications, self.seal)
    }
}

impl fmt::Debug for CompletedFunctionCallTailPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompletedFunctionCallTailPublication")
            .field("identity", self.seal.identity())
            .field("publication_count", &self.publications.len())
            .field("last_local_sequence", &self.seal.last_local_sequence())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct LiveResponseGap {
    identity: LiveResponseItemIdentity,
    missing_from: u64,
    missing_to: Option<u64>,
    unknown_tail: bool,
}

impl LiveResponseGap {
    pub fn known(
        identity: LiveResponseItemIdentity,
        missing_from: u64,
        missing_to: u64,
    ) -> Result<Self, LiveResponseBrokerError> {
        if missing_from > missing_to {
            return Err(LiveResponseBrokerError::new(
                LIVE_RESPONSE_IDENTITY_INVALID,
                "live response gap range is invalid",
            ));
        }
        Ok(Self {
            identity,
            missing_from,
            missing_to: Some(missing_to),
            unknown_tail: false,
        })
    }

    pub fn unknown_tail(identity: LiveResponseItemIdentity, missing_from: u64) -> Self {
        Self {
            identity,
            missing_from,
            missing_to: None,
            unknown_tail: true,
        }
    }

    pub fn identity(&self) -> &LiveResponseItemIdentity {
        &self.identity
    }

    pub fn missing_from(&self) -> u64 {
        self.missing_from
    }

    pub fn missing_to(&self) -> Option<u64> {
        self.missing_to
    }

    pub fn has_unknown_tail(&self) -> bool {
        self.unknown_tail
    }

    pub fn into_public_event(self, sequence_number: u64) -> ResponseStreamEvent {
        ResponseStreamEvent::WorkflowStreamGap {
            sequence_number,
            item_id: self.identity.item_id,
            attempt_no: self.identity.attempt_no.get(),
            missing_from: self.missing_from,
            missing_to: self.missing_to,
            unknown_tail: self.unknown_tail,
            action: WorkflowStreamGapAction::DiscardProvisionalItem,
        }
    }

    fn merge(&mut self, other: &Self) {
        self.missing_from = self.missing_from.min(other.missing_from);
        if self.unknown_tail || other.unknown_tail {
            self.missing_to = None;
            self.unknown_tail = true;
        } else {
            self.missing_to = self.missing_to.max(other.missing_to);
        }
    }

    fn can_merge(&self, other: &Self) -> bool {
        if self.identity != other.identity {
            return false;
        }
        let (Some(left_to), Some(right_to)) = (self.missing_to, other.missing_to) else {
            return true;
        };
        self.missing_from <= right_to.saturating_add(1)
            && other.missing_from <= left_to.saturating_add(1)
    }

    fn widen_to_unknown_tail(&mut self, other: &Self) {
        self.missing_from = self.missing_from.min(other.missing_from);
        self.missing_to = None;
        self.unknown_tail = true;
    }
}

impl fmt::Debug for LiveResponseGap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveResponseGap")
            .field("identity", &self.identity)
            .field("missing_from", &self.missing_from)
            .field("missing_to", &self.missing_to)
            .field("unknown_tail", &self.unknown_tail)
            .finish()
    }
}

pub enum LiveResponseDelivery {
    Publication(LiveResponsePublication),
    Gap(LiveResponseGap),
    Seal(LiveResponseSeal),
}

impl fmt::Debug for LiveResponseDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Publication(publication) => publication.fmt(formatter),
            Self::Gap(gap) => gap.fmt(formatter),
            Self::Seal(seal) => seal.fmt(formatter),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveResponsePublishOutcome {
    Enqueued,
    EnqueuedAfterGap,
    /// A workflow observation was retained after one or more producer-local
    /// indices were skipped. No output-item gap is synthesized.
    EnqueuedAfterBestEffortLoss,
    DroppedWithGap,
    DroppedOversizeWithGap,
    /// A workflow observation was dropped by a bounded live-only queue. It is
    /// not terminal authority and therefore creates no output-item gap.
    DroppedBestEffort,
    SealEnqueued,
    SealExactReplay,
    NoSubscriber,
    RunClosed,
    RejectedOutOfOrder,
    RejectedAfterSeal,
    SealConflict,
    ControlQueueFull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LiveResponseCloseOutcome {
    unknown_tail_gaps: usize,
    omitted_unknown_tail_gaps: usize,
}

impl LiveResponseCloseOutcome {
    pub fn unknown_tail_gaps(self) -> usize {
        self.unknown_tail_gaps
    }

    pub fn omitted_unknown_tail_gaps(self) -> usize {
        self.omitted_unknown_tail_gaps
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveResponseBrokerError {
    code: &'static str,
    message: &'static str,
}

impl LiveResponseBrokerError {
    pub const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &'static str {
        self.message
    }
}

impl fmt::Display for LiveResponseBrokerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl Error for LiveResponseBrokerError {}

#[async_trait]
pub trait LiveResponseSubscriber: Send {
    fn run_id(&self) -> &RunId;

    async fn recv(&mut self) -> Result<LiveResponseDelivery, LiveResponseBrokerError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveResponseBrokerCapability {
    SingleProcess,
    Shared,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveResponseByteLimits {
    pub max_frame_bytes: usize,
    pub max_item_bytes: usize,
    pub max_run_bytes: usize,
}

impl LiveResponseByteLimits {
    pub fn new(
        max_frame_bytes: usize,
        max_item_bytes: usize,
        max_run_bytes: usize,
    ) -> Result<Self, LiveResponseBrokerError> {
        if max_frame_bytes == 0
            || max_item_bytes < max_frame_bytes
            || max_run_bytes < max_item_bytes
        {
            return Err(LiveResponseBrokerError::new(
                LIVE_RESPONSE_CONFIG_INVALID,
                "live response byte limits are invalid",
            ));
        }
        Ok(Self {
            max_frame_bytes,
            max_item_bytes,
            max_run_bytes,
        })
    }
}

impl Default for LiveResponseByteLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 4 * 1_024,
            max_item_bytes: 4 * 1_024 * 1_024,
            max_run_bytes: 16 * 1_024 * 1_024,
        }
    }
}

/// Transient, live-only response transport. `publish` and `seal` are
/// deliberately non-blocking: observation backpressure must never block or
/// fail the durable worker effect.
#[async_trait]
pub trait LiveResponseBroker: Send + Sync {
    fn deployment_capability(&self) -> LiveResponseBrokerCapability;

    async fn check_readiness(
        &self,
        _readiness_timeout: std::time::Duration,
    ) -> Result<(), LiveResponseBrokerError> {
        Ok(())
    }

    async fn shutdown(&self, _grace: std::time::Duration) -> Result<(), LiveResponseBrokerError> {
        Ok(())
    }

    async fn subscribe(
        &self,
        run_id: RunId,
    ) -> Result<Box<dyn LiveResponseSubscriber>, LiveResponseBrokerError>;

    fn publish(&self, publication: LiveResponsePublication) -> LiveResponsePublishOutcome;

    fn seal(&self, seal: LiveResponseSeal) -> LiveResponsePublishOutcome;

    fn close_run(&self, run_id: &RunId) -> LiveResponseCloseOutcome;
}

/// Workspace-internal hooks shared by concrete live-response adapters.
///
/// This module is public only because Rust crate boundaries prevent
/// `pub(crate)` access from the runtime and storage adapter crates. It is not
/// part of the root compatibility facade.
#[doc(hidden)]
pub mod adapter {
    use super::*;

    /// Reconstructs one validated private publication from a transport wire.
    pub fn publication_from_source(
        source: LiveResponseSourceIdentity,
        local_sequence: u64,
        payload: LiveResponsePayload,
    ) -> Result<LiveResponsePublication, LiveResponseBrokerError> {
        LiveResponsePublication::from_source(source, local_sequence, payload)
    }

    /// Borrows the already-authorized payload for a concrete transport codec.
    pub fn publication_payload(publication: &LiveResponsePublication) -> &LiveResponsePayload {
        publication.payload()
    }

    /// Reconstructs and validates one durable terminal response snapshot.
    #[allow(clippy::too_many_arguments)]
    pub fn durable_response_snapshot_new(
        response_id: String,
        terminal_kind: ResponseTerminalKind,
        response: Value,
        workflow: Value,
        public_item_manifest: Value,
        usage: Option<Value>,
        usage_status: ResponseUsageStatus,
        snapshot_hash: crate::ContentHash,
    ) -> Result<DurableResponseSnapshot, crate::repository::RepositoryError> {
        DurableResponseSnapshot::new(
            response_id,
            terminal_kind,
            response,
            workflow,
            public_item_manifest,
            usage,
            usage_status,
            snapshot_hash,
        )
    }

    /// Parses one closed durable response terminal discriminator.
    pub fn response_terminal_kind_parse(
        value: &str,
    ) -> Result<ResponseTerminalKind, crate::repository::RepositoryError> {
        ResponseTerminalKind::parse(value)
    }

    /// Parses one closed durable response usage discriminator.
    pub fn response_usage_status_parse(
        value: &str,
    ) -> Result<ResponseUsageStatus, crate::repository::RepositoryError> {
        ResponseUsageStatus::parse(value)
    }

    pub struct RunQueue {
        run_id: RunId,
        body_capacity: usize,
        control_capacity: usize,
        byte_limits: LiveResponseByteLimits,
        state: Mutex<RunQueueState>,
        notify: Notify,
    }

    #[derive(Default)]
    struct RunQueueState {
        body: VecDeque<LiveResponsePublication>,
        controls: VecDeque<QueueControl>,
        item_cursors: BTreeMap<LiveResponseItemIdentity, ItemCursor>,
        observation_cursors: BTreeMap<LiveWorkflowObservationIdentity, ObservationCursor>,
        observed_bytes: usize,
        size_exhausted: bool,
        closed: bool,
    }

    #[derive(Clone)]
    enum QueueControl {
        Gap(LiveResponseGap),
        Seal(LiveResponseSeal),
    }

    #[derive(Clone, Default)]
    struct ItemCursor {
        next_local_sequence: u64,
        sequence_exhausted: bool,
        observed_bytes: usize,
        size_exhausted: bool,
        seal: Option<LiveResponseSeal>,
    }

    #[derive(Clone, Default)]
    struct ObservationCursor {
        next_local_sequence: u64,
        sequence_exhausted: bool,
        observed_bytes: usize,
        size_exhausted: bool,
    }

    impl ObservationCursor {
        fn expected(&self) -> Option<u64> {
            (!self.sequence_exhausted).then_some(self.next_local_sequence)
        }

        fn observe(&mut self, sequence: u64) {
            match sequence.checked_add(1) {
                Some(next) => self.next_local_sequence = next,
                None => self.sequence_exhausted = true,
            }
        }
    }

    impl ItemCursor {
        fn expected(&self) -> Option<u64> {
            (!self.sequence_exhausted).then_some(self.next_local_sequence)
        }

        fn observed_last(&self) -> Option<u64> {
            if self.sequence_exhausted {
                Some(u64::MAX)
            } else {
                self.next_local_sequence.checked_sub(1)
            }
        }

        fn observe(&mut self, sequence: u64) {
            match sequence.checked_add(1) {
                Some(next) => self.next_local_sequence = next,
                None => self.sequence_exhausted = true,
            }
        }
    }

    impl RunQueue {
        pub fn new_with_limits(
            run_id: RunId,
            body_capacity: usize,
            control_capacity: usize,
            byte_limits: LiveResponseByteLimits,
        ) -> Self {
            Self {
                run_id,
                body_capacity,
                control_capacity,
                byte_limits,
                state: Mutex::new(RunQueueState::default()),
                notify: Notify::new(),
            }
        }

        pub fn publish(&self, publication: LiveResponsePublication) -> LiveResponsePublishOutcome {
            if publication.run_id() != &self.run_id {
                return LiveResponsePublishOutcome::NoSubscriber;
            }
            let source = publication.source().clone();
            let local_sequence = publication.local_sequence();
            let mut state = lock(&self.state);
            if state.closed {
                return LiveResponsePublishOutcome::RunClosed;
            }
            match source {
                LiveResponseSourceIdentity::OutputItem(identity) => {
                    self.publish_output_item(&mut state, publication, identity, local_sequence)
                }
                LiveResponseSourceIdentity::WorkflowObservation(identity) => self
                    .publish_workflow_observation(
                        &mut state,
                        publication,
                        identity,
                        local_sequence,
                    ),
            }
        }

        fn publish_output_item(
            &self,
            state: &mut RunQueueState,
            publication: LiveResponsePublication,
            identity: LiveResponseItemIdentity,
            local_sequence: u64,
        ) -> LiveResponsePublishOutcome {
            let cursor = state
                .item_cursors
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            if cursor.seal.is_some() {
                return LiveResponsePublishOutcome::RejectedAfterSeal;
            }
            let Some(expected) = cursor.expected() else {
                return LiveResponsePublishOutcome::RejectedOutOfOrder;
            };
            if local_sequence < expected {
                return LiveResponsePublishOutcome::RejectedOutOfOrder;
            }

            let frame_bytes = publication.public_wire_bytes();
            let item_observed_bytes = cursor.observed_bytes.saturating_add(frame_bytes);
            let run_observed_bytes = state.observed_bytes.saturating_add(frame_bytes);
            let item_size_exhausted = cursor.size_exhausted
                || frame_bytes > self.byte_limits.max_frame_bytes
                || item_observed_bytes > self.byte_limits.max_item_bytes;
            let run_size_exhausted =
                state.size_exhausted || run_observed_bytes > self.byte_limits.max_run_bytes;
            let body_full = state.body.len() >= self.body_capacity;
            let must_drop = body_full || item_size_exhausted || run_size_exhausted;
            let gap = if must_drop {
                Some(
                    LiveResponseGap::known(identity.clone(), expected, local_sequence)
                        .expect("the observed sequence is never below the expected sequence"),
                )
            } else if local_sequence > expected {
                Some(
                    LiveResponseGap::known(identity.clone(), expected, local_sequence - 1)
                        .expect("a skipped sequence always forms a non-empty range"),
                )
            } else {
                None
            };

            let mut controls = state.controls.clone();
            if gap
                .as_ref()
                .is_some_and(|gap| !enqueue_gap(&mut controls, self.control_capacity, gap.clone()))
            {
                return LiveResponsePublishOutcome::ControlQueueFull;
            }

            let mut next_cursor = cursor;
            next_cursor.observe(local_sequence);
            next_cursor.observed_bytes = item_observed_bytes;
            next_cursor.size_exhausted = item_size_exhausted;
            state.observed_bytes = run_observed_bytes;
            state.size_exhausted = run_size_exhausted;
            state.controls = controls;
            state.item_cursors.insert(identity, next_cursor);
            let outcome = if must_drop {
                LiveResponsePublishOutcome::DroppedWithGap
            } else {
                state.body.push_back(publication);
                if gap.is_some() {
                    LiveResponsePublishOutcome::EnqueuedAfterGap
                } else {
                    LiveResponsePublishOutcome::Enqueued
                }
            };
            self.notify.notify_one();
            outcome
        }

        fn publish_workflow_observation(
            &self,
            state: &mut RunQueueState,
            publication: LiveResponsePublication,
            identity: LiveWorkflowObservationIdentity,
            local_sequence: u64,
        ) -> LiveResponsePublishOutcome {
            let cursor = state
                .observation_cursors
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            let Some(expected) = cursor.expected() else {
                return LiveResponsePublishOutcome::RejectedOutOfOrder;
            };
            if local_sequence < expected {
                return LiveResponsePublishOutcome::RejectedOutOfOrder;
            }

            let frame_bytes = publication.public_wire_bytes();
            let source_observed_bytes = cursor.observed_bytes.saturating_add(frame_bytes);
            let run_observed_bytes = state.observed_bytes.saturating_add(frame_bytes);
            let source_size_exhausted = cursor.size_exhausted
                || frame_bytes > self.byte_limits.max_frame_bytes
                || source_observed_bytes > self.byte_limits.max_item_bytes;
            let run_size_exhausted =
                state.size_exhausted || run_observed_bytes > self.byte_limits.max_run_bytes;
            let must_drop = state.body.len() >= self.body_capacity
                || source_size_exhausted
                || run_size_exhausted;

            let mut next_cursor = cursor;
            next_cursor.observe(local_sequence);
            next_cursor.observed_bytes = source_observed_bytes;
            next_cursor.size_exhausted = source_size_exhausted;
            state.observed_bytes = run_observed_bytes;
            state.size_exhausted = run_size_exhausted;
            state.observation_cursors.insert(identity, next_cursor);

            if must_drop {
                return LiveResponsePublishOutcome::DroppedBestEffort;
            }
            state.body.push_back(publication);
            self.notify.notify_one();
            if local_sequence > expected {
                LiveResponsePublishOutcome::EnqueuedAfterBestEffortLoss
            } else {
                LiveResponsePublishOutcome::Enqueued
            }
        }

        /// Records producer-side loss for an observation that cannot fit another
        /// transient transport envelope. No public output-item gap is created.
        pub fn discard_workflow_observation(
            &self,
            identity: LiveWorkflowObservationIdentity,
            local_sequence: u64,
        ) -> LiveResponsePublishOutcome {
            if identity.run_id() != &self.run_id {
                return LiveResponsePublishOutcome::NoSubscriber;
            }
            let mut state = lock(&self.state);
            if state.closed {
                return LiveResponsePublishOutcome::RunClosed;
            }
            let cursor = state
                .observation_cursors
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            let Some(expected) = cursor.expected() else {
                return LiveResponsePublishOutcome::RejectedOutOfOrder;
            };
            if local_sequence < expected {
                return LiveResponsePublishOutcome::RejectedOutOfOrder;
            }
            let mut next_cursor = cursor;
            next_cursor.observe(local_sequence);
            state.observation_cursors.insert(identity, next_cursor);
            LiveResponsePublishOutcome::DroppedBestEffort
        }

        pub fn seal(&self, seal: LiveResponseSeal) -> LiveResponsePublishOutcome {
            if seal.identity().run_id() != &self.run_id {
                return LiveResponsePublishOutcome::NoSubscriber;
            }
            let identity = seal.identity().clone();
            let mut state = lock(&self.state);
            if state.closed {
                return LiveResponsePublishOutcome::RunClosed;
            }
            let cursor = state
                .item_cursors
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            if let Some(existing) = &cursor.seal {
                return if existing == &seal {
                    LiveResponsePublishOutcome::SealExactReplay
                } else {
                    LiveResponsePublishOutcome::SealConflict
                };
            }
            let observed_last = cursor.observed_last();
            if seal.last_local_sequence < observed_last {
                return LiveResponsePublishOutcome::SealConflict;
            }

            let mut controls = state.controls.clone();
            if let Some(last) = seal.last_local_sequence {
                let missing_from = observed_last.map_or(0, |observed| observed.saturating_add(1));
                if observed_last.is_none_or(|observed| last > observed) {
                    let gap = LiveResponseGap::known(identity.clone(), missing_from, last)
                        .expect("a seal beyond observed data always forms a valid gap");
                    if !enqueue_gap(&mut controls, self.control_capacity, gap) {
                        return LiveResponsePublishOutcome::ControlQueueFull;
                    }
                }
            }
            if controls.len() >= self.control_capacity {
                return LiveResponsePublishOutcome::ControlQueueFull;
            }
            controls.push_back(QueueControl::Seal(seal.clone()));
            let mut next_cursor = cursor;
            next_cursor.seal = Some(seal);
            state.controls = controls;
            state.item_cursors.insert(identity, next_cursor);
            drop(state);
            self.notify.notify_one();
            LiveResponsePublishOutcome::SealEnqueued
        }

        pub fn discard_with_gap(
            &self,
            identity: LiveResponseItemIdentity,
            local_sequence: u64,
        ) -> LiveResponsePublishOutcome {
            if identity.run_id() != &self.run_id {
                return LiveResponsePublishOutcome::NoSubscriber;
            }
            let mut state = lock(&self.state);
            if state.closed {
                return LiveResponsePublishOutcome::RunClosed;
            }
            let cursor = state
                .item_cursors
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            if cursor.seal.is_some() {
                return LiveResponsePublishOutcome::RejectedAfterSeal;
            }
            let Some(expected) = cursor.expected() else {
                return LiveResponsePublishOutcome::RejectedOutOfOrder;
            };
            if local_sequence < expected {
                return LiveResponsePublishOutcome::RejectedOutOfOrder;
            }
            let gap = LiveResponseGap::known(identity.clone(), expected, local_sequence)
                .expect("the discarded sequence is never below the expected sequence");
            let mut controls = state.controls.clone();
            if !enqueue_gap(&mut controls, self.control_capacity, gap) {
                return LiveResponsePublishOutcome::ControlQueueFull;
            }
            let mut next_cursor = cursor;
            next_cursor.observe(local_sequence);
            state.controls = controls;
            state.item_cursors.insert(identity, next_cursor);
            drop(state);
            self.notify.notify_one();
            LiveResponsePublishOutcome::DroppedWithGap
        }

        pub fn discard_seal_with_gap(
            &self,
            identity: LiveResponseItemIdentity,
        ) -> LiveResponsePublishOutcome {
            if identity.run_id() != &self.run_id {
                return LiveResponsePublishOutcome::NoSubscriber;
            }
            let mut state = lock(&self.state);
            if state.closed {
                return LiveResponsePublishOutcome::RunClosed;
            }
            let cursor = state
                .item_cursors
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            if cursor.seal.is_some() {
                return LiveResponsePublishOutcome::RejectedAfterSeal;
            }
            let missing_from = cursor.expected().unwrap_or(u64::MAX);
            let gap = LiveResponseGap::unknown_tail(identity, missing_from);
            let mut controls = state.controls.clone();
            if !enqueue_gap(&mut controls, self.control_capacity, gap) {
                return LiveResponsePublishOutcome::ControlQueueFull;
            }
            state.controls = controls;
            drop(state);
            self.notify.notify_one();
            LiveResponsePublishOutcome::DroppedWithGap
        }

        pub fn accept_gap(&self, gap: LiveResponseGap) -> LiveResponsePublishOutcome {
            if gap.identity().run_id() != &self.run_id {
                return LiveResponsePublishOutcome::NoSubscriber;
            }
            let identity = gap.identity().clone();
            let mut state = lock(&self.state);
            if state.closed {
                return LiveResponsePublishOutcome::RunClosed;
            }
            let cursor = state
                .item_cursors
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            if cursor.seal.is_some() {
                return LiveResponsePublishOutcome::RejectedAfterSeal;
            }
            let mut controls = state.controls.clone();
            if !enqueue_gap(&mut controls, self.control_capacity, gap.clone()) {
                return LiveResponsePublishOutcome::ControlQueueFull;
            }
            let mut next_cursor = cursor;
            if let Some(missing_to) = gap.missing_to() {
                if next_cursor
                    .expected()
                    .is_some_and(|expected| missing_to >= expected)
                {
                    next_cursor.observe(missing_to);
                }
            }
            state.controls = controls;
            state.item_cursors.insert(identity, next_cursor);
            drop(state);
            self.notify.notify_one();
            LiveResponsePublishOutcome::EnqueuedAfterGap
        }

        pub async fn recv(&self) -> Result<LiveResponseDelivery, LiveResponseBrokerError> {
            loop {
                let notified = self.notify.notified();
                {
                    let mut state = lock(&self.state);
                    if let Some(delivery) = next_delivery(&mut state) {
                        return Ok(delivery);
                    }
                    if state.closed {
                        return Err(LiveResponseBrokerError::new(
                            LIVE_RESPONSE_STREAM_CLOSED,
                            "live response stream is closed",
                        ));
                    }
                }
                notified.await;
            }
        }

        pub fn close(&self) -> LiveResponseCloseOutcome {
            let mut state = lock(&self.state);
            if state.closed {
                return LiveResponseCloseOutcome::default();
            }
            let mut controls = state.controls.clone();
            let mut outcome = LiveResponseCloseOutcome::default();
            for (identity, cursor) in &state.item_cursors {
                if cursor.seal.is_some() {
                    continue;
                }
                let missing_from = cursor.expected().unwrap_or(u64::MAX);
                let gap = LiveResponseGap::unknown_tail(identity.clone(), missing_from);
                if enqueue_gap(&mut controls, self.control_capacity, gap) {
                    outcome.unknown_tail_gaps += 1;
                } else {
                    outcome.omitted_unknown_tail_gaps += 1;
                }
            }
            state.controls = controls;
            state.closed = true;
            drop(state);
            self.notify.notify_waiters();
            outcome
        }
    }

    fn enqueue_gap(
        controls: &mut VecDeque<QueueControl>,
        capacity: usize,
        gap: LiveResponseGap,
    ) -> bool {
        if let Some(existing) = controls.iter_mut().find_map(|control| match control {
            QueueControl::Gap(existing) if existing.can_merge(&gap) => Some(existing),
            QueueControl::Gap(_) | QueueControl::Seal(_) => None,
        }) {
            existing.merge(&gap);
            return true;
        }
        if controls.len() < capacity {
            controls.push_back(QueueControl::Gap(gap));
            return true;
        }
        // Once the priority queue is saturated, preserving a fabricated broad
        // known range would be incorrect when received indices lie between two
        // gaps. Collapse only this item's pending evidence to an explicit unknown
        // tail; the caller will discard the provisional item and use the durable
        // terminal snapshot as authority.
        if let Some(existing) = controls.iter_mut().find_map(|control| match control {
            QueueControl::Gap(existing) if existing.identity == gap.identity => Some(existing),
            QueueControl::Gap(_) | QueueControl::Seal(_) => None,
        }) {
            existing.widen_to_unknown_tail(&gap);
            return true;
        }
        false
    }

    fn next_delivery(state: &mut RunQueueState) -> Option<LiveResponseDelivery> {
        if let Some(position) = state
            .controls
            .iter()
            .position(|control| matches!(control, QueueControl::Gap(_)))
        {
            let QueueControl::Gap(gap) = state.controls.remove(position)? else {
                unreachable!("the selected control is a gap")
            };
            return Some(LiveResponseDelivery::Gap(gap));
        }

        if let Some(position) = state.controls.iter().position(|control| {
            let QueueControl::Seal(seal) = control else {
                return false;
            };
            !state
                .body
                .iter()
                .any(|publication| publication.output_item_identity() == Some(seal.identity()))
        }) {
            let QueueControl::Seal(seal) = state.controls.remove(position)? else {
                unreachable!("the selected control is a seal")
            };
            return Some(LiveResponseDelivery::Seal(seal));
        }

        state
            .body
            .pop_front()
            .map(LiveResponseDelivery::Publication)
    }
}

fn validate_function_call_arguments(arguments_jcs: &str) -> Result<(), LiveResponseBrokerError> {
    if arguments_jcs.is_empty() || arguments_jcs.len() > MAX_FUNCTION_CALL_ARGUMENT_BYTES {
        return Err(invalid_completed_function_call());
    }
    let value: Value =
        serde_json::from_str(arguments_jcs).map_err(|_| invalid_completed_function_call())?;
    if !value.is_object() {
        return Err(invalid_completed_function_call());
    }
    let canonical = serde_jcs::to_string(&value).map_err(|_| invalid_completed_function_call())?;
    if canonical != arguments_jcs {
        return Err(invalid_completed_function_call());
    }

    let mut stack = vec![(&value, 0_usize)];
    let mut observed_values = 0_usize;
    while let Some((current, depth)) = stack.pop() {
        observed_values = observed_values.saturating_add(1);
        if observed_values > MAX_FUNCTION_CALL_ARGUMENT_VALUES
            || depth > MAX_FUNCTION_CALL_ARGUMENT_DEPTH
        {
            return Err(invalid_completed_function_call());
        }
        match current {
            Value::Array(values) => {
                stack.extend(values.iter().map(|value| (value, depth.saturating_add(1))));
            }
            Value::Object(values) => {
                stack.extend(
                    values
                        .values()
                        .map(|value| (value, depth.saturating_add(1))),
                );
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    Ok(())
}

fn invalid_completed_function_call() -> LiveResponseBrokerError {
    LiveResponseBrokerError::new(
        LIVE_RESPONSE_FUNCTION_CALL_INVALID,
        "completed function-call publication is invalid",
    )
}

fn validate_optional_public_string(
    value: Option<&str>,
    max_bytes: usize,
    message: &'static str,
) -> Result<(), WorkflowPublicResultError> {
    match value {
        Some(value) => validate_bounded_public_string(value, max_bytes, message),
        None => Ok(()),
    }
}

fn validate_bounded_public_string(
    value: &str,
    max_bytes: usize,
    message: &'static str,
) -> Result<(), WorkflowPublicResultError> {
    if value.is_empty() || value.len() > max_bytes {
        return Err(WorkflowPublicResultError::new(message));
    }
    Ok(())
}

fn validate_bounded_public_json(
    value: &Value,
    max_bytes: usize,
) -> Result<(), WorkflowPublicResultError> {
    let encoded = serde_jcs::to_vec(value).map_err(|_| {
        WorkflowPublicResultError::new("workflow public JSON must be canonicalizable")
    })?;
    if encoded.len() > max_bytes {
        return Err(WorkflowPublicResultError::new(
            "workflow public JSON exceeds the inline byte limit",
        ));
    }

    let mut stack = vec![(value, 0_usize)];
    let mut observed_values = 0_usize;
    while let Some((current, depth)) = stack.pop() {
        observed_values = observed_values.saturating_add(1);
        if observed_values > MAX_WORKFLOW_PUBLIC_JSON_VALUES
            || depth > MAX_WORKFLOW_PUBLIC_JSON_DEPTH
        {
            return Err(WorkflowPublicResultError::new(
                "workflow public JSON exceeds the structural limit",
            ));
        }
        match current {
            Value::String(string) if string.len() > MAX_WORKFLOW_PUBLIC_JSON_STRING_BYTES => {
                return Err(WorkflowPublicResultError::new(
                    "workflow public JSON contains an oversized string",
                ));
            }
            Value::Array(values) => {
                stack.extend(values.iter().map(|value| (value, depth.saturating_add(1))));
            }
            Value::Object(values) => {
                if values.keys().any(|key| {
                    key.is_empty()
                        || key.len() > MAX_PUBLIC_LABEL_BYTES
                        || key.chars().any(char::is_control)
                }) {
                    return Err(WorkflowPublicResultError::new(
                        "workflow public JSON contains an invalid object key",
                    ));
                }
                stack.extend(
                    values
                        .values()
                        .map(|value| (value, depth.saturating_add(1))),
                );
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    Ok(())
}

fn valid_public_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_PUBLIC_LABEL_BYTES
        && !value.chars().any(|character| {
            character.is_control() || character.is_whitespace() || matches!(character, '/' | '\\')
        })
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArtifactId, ContentHash};
    use serde_json::json;

    fn run(value: &str) -> RunId {
        RunId::new(value).unwrap()
    }

    fn identity(run_id: &str) -> LiveResponseItemIdentity {
        LiveResponseItemIdentity::new(
            run(run_id),
            ActivationId::new("activation_answer").unwrap(),
            AttemptNo::FIRST,
            1,
            "msg_answer",
            0,
        )
        .unwrap()
    }

    fn workflow_identity(run_id: &str, source_id: &str) -> LiveWorkflowObservationIdentity {
        LiveWorkflowObservationIdentity::new(
            run(run_id),
            ActivationId::new("activation_workflow_observation").unwrap(),
            AttemptNo::FIRST,
            source_id,
        )
        .unwrap()
    }

    fn delta(
        identity: LiveResponseItemIdentity,
        sequence: u64,
        text: &str,
    ) -> LiveResponsePublication {
        LiveResponsePublication::new(
            identity,
            sequence,
            LiveResponsePayload::OutputTextDelta {
                content_index: 0,
                delta: text.to_owned(),
            },
        )
        .unwrap()
    }

    fn tool_started(
        identity: LiveWorkflowObservationIdentity,
        sequence: u64,
        call_id: &str,
    ) -> LiveResponsePublication {
        LiveResponsePublication::new_workflow_observation(
            identity,
            sequence,
            LiveResponsePayload::ToolStarted {
                call_id: call_id.to_owned(),
                tool_name: "lookup".to_owned(),
                arguments: Some(json!({"published": true})),
            },
        )
        .unwrap()
    }

    fn artifact(value: &str, media_type: &str) -> ArtifactRef {
        ArtifactRef::new(
            ArtifactId::new(value).unwrap(),
            ContentHash::from_bytes(value.as_bytes()),
            4,
            Some(media_type.to_owned()),
        )
        .unwrap()
    }

    fn sample_response(status: ResponseStatus) -> PublicResponse {
        PublicResponse {
            id: "resp_schema".to_owned(),
            object: ResponseObjectKind::Response,
            status,
            output: vec![
                ResponseOutputItem::Message {
                    id: "msg_schema".to_owned(),
                    status: ResponseItemStatus::Completed,
                    role: ResponseRole::Assistant,
                    content: vec![ResponseContentPart::OutputText {
                        text: "complete".to_owned(),
                        annotations: vec![json!({"kind": "citation"})],
                    }],
                },
                ResponseOutputItem::FunctionCall {
                    id: "fn_schema".to_owned(),
                    status: ResponseItemStatus::Completed,
                    call_id: "call_schema".to_owned(),
                    name: "lookup".to_owned(),
                    arguments: r#"{"indicator":"WBC"}"#.to_owned(),
                },
                ResponseOutputItem::FileSearchCall {
                    id: "search_schema".to_owned(),
                    status: ResponseItemStatus::Completed,
                    queries: vec!["WBC".to_owned()],
                    results: vec![json!({"document_id": "doc_schema"})],
                },
            ],
            usage: Some(ResponseUsage {
                input_tokens: 11,
                input_tokens_details: ResponseUsageInputDetails { cached_tokens: 3 },
                output_tokens: 7,
                output_tokens_details: ResponseUsageOutputDetails {
                    reasoning_tokens: 2,
                },
                total_tokens: 18,
            }),
            error: (status == ResponseStatus::Failed).then(|| PublicResponseError {
                code: "MODEL_FAILED".to_owned(),
                message: "model request failed".to_owned(),
                param: None,
            }),
        }
    }

    fn sample_workflow_completed() -> WorkflowCompleted {
        let artifact = artifact("artifact_schema", "image/png");
        let tool_result = WorkflowToolResult::new(
            "call_schema",
            "lookup",
            vec![
                WorkflowToolContent::output_text("tool text").unwrap(),
                WorkflowToolContent::output_json(json!({"ok": true})).unwrap(),
                WorkflowToolContent::output_image(artifact.clone()),
                WorkflowToolContent::output_file(artifact.clone()),
                WorkflowToolContent::output_audio(artifact.clone()),
            ],
        )
        .unwrap();
        let retrieval_result = WorkflowRetrievalResult::new(
            "result_schema",
            Some("Lab handbook".to_owned()),
            Some("https://example.test/lab".to_owned()),
            Some(0.95),
            Some("Reference range".to_owned()),
            WorkflowRetrievalMetadata::new(BTreeMap::from([(
                "source".to_owned(),
                json!("handbook"),
            )]))
            .unwrap(),
            Some(artifact),
        )
        .unwrap();
        let retrieval =
            WorkflowRetrieval::new("ret_schema", Some("WBC".to_owned()), vec![retrieval_result])
                .unwrap();
        WorkflowCompleted {
            run_id: "run_schema".to_owned(),
            result: json!({"answer": "complete"}),
            tool_results: vec![tool_result],
            retrievals: vec![retrieval],
            usage_status: WorkflowUsageStatus::Complete,
        }
    }

    fn vendored_standard_event_samples() -> Vec<ResponseStreamEvent> {
        let empty_response = PublicResponse {
            id: "resp_schema".to_owned(),
            object: ResponseObjectKind::Response,
            status: ResponseStatus::InProgress,
            output: Vec::new(),
            usage: None,
            error: None,
        };
        let empty_part = ResponseContentPart::OutputText {
            text: String::new(),
            annotations: Vec::new(),
        };
        vec![
            ResponseStreamEvent::ResponseCreated {
                sequence_number: 0,
                response: empty_response.clone(),
            },
            ResponseStreamEvent::ResponseInProgress {
                sequence_number: 1,
                response: empty_response,
            },
            ResponseStreamEvent::ResponseOutputItemAdded {
                sequence_number: 2,
                output_index: 0,
                item: ResponseOutputItem::Message {
                    id: "msg_schema".to_owned(),
                    status: ResponseItemStatus::InProgress,
                    role: ResponseRole::Assistant,
                    content: Vec::new(),
                },
            },
            ResponseStreamEvent::ResponseContentPartAdded {
                sequence_number: 3,
                item_id: "msg_schema".to_owned(),
                output_index: 0,
                content_index: 0,
                part: empty_part,
            },
            ResponseStreamEvent::ResponseOutputTextDelta {
                sequence_number: 4,
                item_id: "msg_schema".to_owned(),
                output_index: 0,
                content_index: 0,
                delta: "partial".to_owned(),
            },
            ResponseStreamEvent::ResponseOutputTextDone {
                sequence_number: 5,
                item_id: "msg_schema".to_owned(),
                output_index: 0,
                content_index: 0,
                text: "complete".to_owned(),
            },
            ResponseStreamEvent::ResponseContentPartDone {
                sequence_number: 6,
                item_id: "msg_schema".to_owned(),
                output_index: 0,
                content_index: 0,
                part: ResponseContentPart::OutputText {
                    text: "complete".to_owned(),
                    annotations: Vec::new(),
                },
            },
            ResponseStreamEvent::ResponseFunctionCallArgumentsDelta {
                sequence_number: 7,
                item_id: "fn_schema".to_owned(),
                output_index: 1,
                delta: r#"{"indicator":"#.to_owned(),
            },
            ResponseStreamEvent::ResponseFunctionCallArgumentsDone {
                sequence_number: 8,
                item_id: "fn_schema".to_owned(),
                output_index: 1,
                name: "lookup".to_owned(),
                arguments: r#"{"indicator":"WBC"}"#.to_owned(),
            },
            ResponseStreamEvent::ResponseOutputItemDone {
                sequence_number: 9,
                output_index: 1,
                item: ResponseOutputItem::FunctionCall {
                    id: "fn_schema".to_owned(),
                    status: ResponseItemStatus::Completed,
                    call_id: "call_schema".to_owned(),
                    name: "lookup".to_owned(),
                    arguments: r#"{"indicator":"WBC"}"#.to_owned(),
                },
            },
            ResponseStreamEvent::ResponseFileSearchCallInProgress {
                sequence_number: 10,
                item_id: "search_schema".to_owned(),
                output_index: 2,
            },
            ResponseStreamEvent::ResponseFileSearchCallSearching {
                sequence_number: 11,
                item_id: "search_schema".to_owned(),
                output_index: 2,
            },
            ResponseStreamEvent::ResponseFileSearchCallCompleted {
                sequence_number: 12,
                item_id: "search_schema".to_owned(),
                output_index: 2,
            },
            ResponseStreamEvent::ResponseCompleted {
                sequence_number: 13,
                response: sample_response(ResponseStatus::Completed),
                workflow: sample_workflow_completed(),
            },
            ResponseStreamEvent::ResponseFailed {
                sequence_number: 14,
                response: sample_response(ResponseStatus::Failed),
                workflow: WorkflowFailure {
                    run_id: "run_schema".to_owned(),
                    error: WorkflowPublicError {
                        code: "WORKFLOW_FAILED".to_owned(),
                        message: "workflow failed".to_owned(),
                    },
                    tool_results: Vec::new(),
                    retrievals: Vec::new(),
                    usage_status: WorkflowUsageStatus::Partial,
                },
            },
            ResponseStreamEvent::Error {
                sequence_number: 15,
                code: "STREAM_ERROR".to_owned(),
                message: "stream failed".to_owned(),
                param: None,
            },
        ]
    }

    #[test]
    fn vendored_openai_streaming_snapshot_is_pinned_to_the_v1_standard_event_contract() {
        let snapshot: Value = serde_json::from_str(workspace_asset_str!(
            "schemas/vendor/openai-responses-streaming-2026-07-19.snapshot.json"
        ))
        .unwrap();
        assert_eq!(
            snapshot["protocol_binding"],
            RESPONSE_STREAM_PROTOCOL_VERSION
        );
        assert_eq!(snapshot["captured_at"], "2026-07-19");
        assert_eq!(
            snapshot["source"],
            "https://developers.openai.com/api/docs/guides/streaming-responses"
        );

        let snapshot_types = snapshot["standard_events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event_type| event_type.as_str().unwrap())
            .collect::<Vec<_>>();
        let protocol_standard_types = ResponseStreamEventType::ALL
            .into_iter()
            .map(ResponseStreamEventType::as_str)
            .filter(|event_type| !event_type.starts_with("workflow."))
            .collect::<Vec<_>>();
        assert_eq!(snapshot_types, protocol_standard_types);

        let validator = crate::schema::compile_schema_2020(&snapshot)
            .expect("vendored Draft 2020-12 schema must compile");
        let samples = vendored_standard_event_samples();
        assert_eq!(samples.len(), protocol_standard_types.len());
        for (sample, expected_type) in samples.iter().zip(protocol_standard_types) {
            assert_eq!(sample.event_type().as_str(), expected_type);
            let encoded = serde_json::to_value(sample).unwrap();
            assert!(
                validator.is_valid(&encoded),
                "real {expected_type} serialization must match the vendored schema"
            );

            let mut missing_required = encoded.clone();
            missing_required
                .as_object_mut()
                .unwrap()
                .remove("sequence_number");
            assert!(
                !validator.is_valid(&missing_required),
                "{expected_type} must reject a missing required field"
            );

            let mut wrong_type = encoded.clone();
            wrong_type["sequence_number"] = json!("not-an-integer");
            assert!(
                !validator.is_valid(&wrong_type),
                "{expected_type} must reject a wrong field type"
            );

            let mut unknown_field = encoded;
            unknown_field["unexpected"] = json!(true);
            assert!(
                !validator.is_valid(&unknown_field),
                "{expected_type} must reject unknown fields"
            );
        }

        let platform_extension = ResponseStreamEvent::WorkflowToolStarted {
            sequence_number: 16,
            call_id: "call_schema".to_owned(),
            tool_name: "lookup".to_owned(),
            arguments: None,
        };
        assert!(!validator.is_valid(&serde_json::to_value(platform_extension).unwrap()));

        let terminal_extensions = &snapshot["platform_extensions"]["terminal_fields"];
        assert_eq!(
            terminal_extensions["response.completed"],
            json!(["workflow"])
        );
        assert_eq!(terminal_extensions["response.failed"], json!(["workflow"]));
    }

    #[test]
    fn event_type_set_is_exact_and_rejects_unknown_names() {
        let names = ResponseStreamEventType::ALL
            .into_iter()
            .map(ResponseStreamEventType::as_str)
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "response.created",
                "response.in_progress",
                "response.output_item.added",
                "response.content_part.added",
                "response.output_text.delta",
                "response.output_text.done",
                "response.content_part.done",
                "response.function_call_arguments.delta",
                "response.function_call_arguments.done",
                "response.output_item.done",
                "response.file_search_call.in_progress",
                "response.file_search_call.searching",
                "response.file_search_call.completed",
                "response.completed",
                "response.failed",
                "error",
                "workflow.tool.started",
                "workflow.tool.completed",
                "workflow.tool.failed",
                "workflow.retrieval.completed",
                "workflow.stream.gap",
                "workflow.response.timed_out",
                "workflow.response.cancelled",
                "workflow.response.interrupted",
            ]
        );
        for event_type in ResponseStreamEventType::ALL {
            let encoded = serde_json::to_value(event_type).unwrap();
            assert_eq!(encoded, json!(event_type.as_str()));
            assert_eq!(
                serde_json::from_value::<ResponseStreamEventType>(encoded).unwrap(),
                event_type
            );
        }
        assert!(serde_json::from_value::<ResponseStreamEventType>(json!("run.completed")).is_err());
        assert!(serde_json::from_value::<ResponseStreamEventType>(json!(
            "workflow.tool_result.done"
        ))
        .is_err());
        assert!(!ResponseStreamEventType::Error.is_terminal());
        assert!(ResponseStreamEventType::ResponseCompleted.is_terminal());
    }

    #[test]
    fn publication_projects_only_the_closed_public_fields() {
        let publication = delta(identity("run_private"), 41, "visible answer");
        let debug = format!("{publication:?}");
        assert!(!debug.contains("visible answer"));
        let event = publication.into_public_event(7);
        let encoded = serde_json::to_value(&event).unwrap();
        assert_eq!(
            encoded,
            json!({
                "type": "response.output_text.delta",
                "sequence_number": 7,
                "item_id": "msg_answer",
                "output_index": 0,
                "content_index": 0,
                "delta": "visible answer"
            })
        );
        let object = encoded.as_object().unwrap();
        for internal in [
            "run_id",
            "activation_id",
            "attempt_no",
            "model_call_no",
            "local_sequence",
            "id",
        ] {
            assert!(!object.contains_key(internal), "leaked {internal}");
        }
        let mut unknown = encoded;
        unknown["node_id"] = json!("answer");
        assert!(serde_json::from_value::<ResponseStreamEvent>(unknown).is_err());
    }

    #[test]
    fn publication_source_union_rejects_mismatches_and_hides_workflow_identity() {
        let item = identity("run_source_contract");
        assert!(LiveResponsePublication::new(
            item,
            0,
            LiveResponsePayload::ToolStarted {
                call_id: "call_wrong_source".to_owned(),
                tool_name: "lookup".to_owned(),
                arguments: None,
            },
        )
        .is_err());

        let observation = workflow_identity("run_source_contract", "tool_call_source");
        assert!(LiveResponsePublication::new_workflow_observation(
            observation.clone(),
            0,
            LiveResponsePayload::OutputTextDelta {
                content_index: 0,
                delta: "wrong source".to_owned(),
            },
        )
        .is_err());

        let publication = tool_started(observation, 0, "call_public");
        let debug = format!("{publication:?}");
        assert!(!debug.contains("published"));
        let encoded = serde_json::to_value(publication.into_public_event(9)).unwrap();
        assert_eq!(
            encoded,
            json!({
                "type": "workflow.tool.started",
                "sequence_number": 9,
                "call_id": "call_public",
                "tool_name": "lookup",
                "arguments": {"published": true}
            })
        );
        for internal in [
            "run_id",
            "activation_id",
            "attempt_no",
            "source_id",
            "model_call_no",
            "item_id",
            "output_index",
            "local_sequence",
        ] {
            assert!(!encoded.as_object().unwrap().contains_key(internal));
        }
    }

    #[test]
    fn completed_function_call_builder_freezes_exact_frames_sequences_and_seal() {
        let arguments = r#"{"city":"shanghai","units":"metric"}"#;
        let plan = CompletedFunctionCallPublication::build(
            identity("run_completed_function_call"),
            "call_weather",
            "weather",
            arguments,
        )
        .unwrap();
        assert_eq!(CompletedFunctionCallPublication::LAST_LOCAL_SEQUENCE, 3);
        assert_eq!(plan.publications().len(), 4);
        assert_eq!(
            plan.publications()
                .iter()
                .map(LiveResponsePublication::local_sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(plan.seal().last_local_sequence(), Some(3));
        assert_eq!(plan.seal().status(), LiveResponseSealStatus::Completed);
        assert_eq!(plan.seal().identity().item_id(), "msg_answer");
        assert!(!format!("{plan:?}").contains("shanghai"));

        let events = plan
            .publications()
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, publication)| {
                serde_json::to_value(publication.into_public_event(10 + index as u64)).unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            events,
            vec![
                json!({
                    "type": "response.output_item.added",
                    "sequence_number": 10,
                    "output_index": 0,
                    "item": {
                        "type": "function_call",
                        "id": "msg_answer",
                        "status": "in_progress",
                        "call_id": "call_weather",
                        "name": "weather",
                        "arguments": ""
                    }
                }),
                json!({
                    "type": "response.function_call_arguments.delta",
                    "sequence_number": 11,
                    "item_id": "msg_answer",
                    "output_index": 0,
                    "delta": arguments
                }),
                json!({
                    "type": "response.function_call_arguments.done",
                    "sequence_number": 12,
                    "item_id": "msg_answer",
                    "output_index": 0,
                    "name": "weather",
                    "arguments": arguments
                }),
                json!({
                    "type": "response.output_item.done",
                    "sequence_number": 13,
                    "output_index": 0,
                    "item": {
                        "type": "function_call",
                        "id": "msg_answer",
                        "status": "completed",
                        "call_id": "call_weather",
                        "name": "weather",
                        "arguments": arguments
                    }
                }),
            ]
        );
    }

    #[test]
    fn completed_function_call_tail_uses_the_durable_fragment_watermark_without_replaying() {
        let arguments = r#"{"city":"shanghai"}"#;
        let tail = CompletedFunctionCallTailPublication::build(
            identity("run_completed_function_call_tail"),
            "call_weather",
            "weather",
            arguments,
            5,
        )
        .unwrap();
        assert!(!format!("{tail:?}").contains("shanghai"));
        let (frames, seal) = tail.into_parts();
        assert_eq!(
            frames
                .iter()
                .map(LiveResponsePublication::local_sequence)
                .collect::<Vec<_>>(),
            vec![4, 5],
        );
        assert_eq!(
            frames
                .iter()
                .map(LiveResponsePublication::payload_type)
                .collect::<Vec<_>>(),
            vec![
                ResponseStreamEventType::ResponseFunctionCallArgumentsDone,
                ResponseStreamEventType::ResponseOutputItemDone,
            ],
        );
        assert_eq!(seal.last_local_sequence(), Some(5));
        assert_eq!(seal.status(), LiveResponseSealStatus::Completed);
        assert!(CompletedFunctionCallTailPublication::build(
            identity("run_invalid_function_call_tail"),
            "call_weather",
            "weather",
            arguments,
            2,
        )
        .is_err());
    }

    #[test]
    fn completed_function_call_builder_rejects_non_jcs_non_object_and_oversize_arguments() {
        for invalid in [r#"{"z":1,"a":2}"#, r#"{"a": 1}"#, r#"[1,2,3]"#, "null"] {
            let error = CompletedFunctionCallPublication::build(
                identity("run_invalid_function_call"),
                "call_invalid",
                "lookup",
                invalid,
            )
            .unwrap_err();
            assert_eq!(error.code(), LIVE_RESPONSE_FUNCTION_CALL_INVALID);
            assert!(!format!("{error:?}").contains(invalid));
        }

        assert!(CompletedFunctionCallPublication::build(
            identity("run_invalid_function_identity"),
            "call with spaces",
            "lookup",
            "{}",
        )
        .is_err());

        let oversized = serde_jcs::to_string(&json!({
            "value": "x".repeat(MAX_FUNCTION_CALL_ARGUMENT_BYTES)
        }))
        .unwrap();
        assert!(oversized.len() > MAX_FUNCTION_CALL_ARGUMENT_BYTES);
        let error = CompletedFunctionCallPublication::build(
            identity("run_oversize_function_call"),
            "call_oversize",
            "lookup",
            oversized,
        )
        .unwrap_err();
        assert_eq!(error.code(), LIVE_RESPONSE_FUNCTION_CALL_INVALID);
    }

    #[test]
    fn terminal_tool_and_retrieval_results_use_closed_typed_envelopes() {
        let image = serde_json::to_value(artifact("art_image", "image/png")).unwrap();
        let file = serde_json::to_value(artifact("art_file", "application/pdf")).unwrap();
        let audio = serde_json::to_value(artifact("art_audio", "audio/mpeg")).unwrap();
        let workflow: WorkflowCompleted = serde_json::from_value(json!({
            "run_id": "run_typed_terminal",
            "result": {"answer": "ok"},
            "tool_results": [{
                "call_id": "call_lookup",
                "tool_name": "lookup",
                "content": [
                    {"type": "output_text", "text": "safe text"},
                    {"type": "output_json", "json": {"score": 0.9}},
                    {"type": "output_image", "artifact": image},
                    {"type": "output_file", "artifact": file},
                    {"type": "output_audio", "artifact": audio}
                ]
            }],
            "retrievals": [{
                "retrieval_id": "ret_lookup",
                "query": "published query",
                "results": [{
                    "id": "doc_1",
                    "title": "source",
                    "uri": "https://example.test/source/1",
                    "score": 0.92,
                    "snippet": "safe excerpt",
                    "metadata": {"collection": "public"},
                    "artifact": serde_json::to_value(artifact("art_source", "text/plain")).unwrap()
                }]
            }],
            "usage_status": "complete"
        }))
        .unwrap();
        assert_eq!(workflow.tool_results[0].call_id(), "call_lookup");
        assert_eq!(
            workflow.tool_results[0].content()[1].json(),
            Some(&json!({"score": 0.9}))
        );
        assert_eq!(workflow.retrievals[0].retrieval_id(), "ret_lookup");
        assert_eq!(workflow.retrievals[0].results()[0].id(), "doc_1");
        assert_eq!(workflow.retrievals[0].results()[0].score(), Some(0.92));
        assert_eq!(workflow.tool_results[0].content().len(), 5);

        let mut encoded = serde_json::to_value(workflow).unwrap();
        encoded["tool_results"][0]["raw_provider_payload"] = json!("private");
        assert!(serde_json::from_value::<WorkflowCompleted>(encoded).is_err());
    }

    #[test]
    fn terminal_public_results_reject_unknown_and_wrong_variants() {
        let base = json!({
            "run_id": "run_invalid_terminal",
            "result": {"answer": "ok"},
            "tool_results": [{
                "call_id": "call_lookup",
                "tool_name": "lookup",
                "content": [{"type": "output_text", "text": "safe"}]
            }],
            "retrievals": [{
                "retrieval_id": "ret_lookup",
                "results": [{"id": "doc_1", "metadata": {}}]
            }],
            "usage_status": "complete"
        });

        let mut unknown_variant = base.clone();
        unknown_variant["tool_results"][0]["content"][0]["type"] = json!("output_video");
        assert!(serde_json::from_value::<WorkflowCompleted>(unknown_variant).is_err());

        let mut inline_binary = base.clone();
        inline_binary["tool_results"][0]["content"][0] = json!({
            "type": "output_image",
            "base64": "aGVsbG8="
        });
        assert!(serde_json::from_value::<WorkflowCompleted>(inline_binary).is_err());

        let mut unknown_retrieval_field = base.clone();
        unknown_retrieval_field["retrievals"][0]["results"][0]["raw_document"] = json!("private");
        assert!(serde_json::from_value::<WorkflowCompleted>(unknown_retrieval_field).is_err());

        let mut wrong_retrieval_shape = base;
        wrong_retrieval_shape["retrievals"][0]["results"][0] = json!("doc_1");
        assert!(serde_json::from_value::<WorkflowCompleted>(wrong_retrieval_shape).is_err());
    }

    #[test]
    fn public_result_constructors_enforce_identity_score_and_inline_bounds() {
        assert!(WorkflowToolResult::new("not stable", "lookup", Vec::new()).is_err());
        assert!(
            WorkflowToolContent::output_text("x".repeat(MAX_WORKFLOW_PUBLIC_TEXT_BYTES + 1))
                .is_err()
        );
        assert!(WorkflowToolContent::output_json(json!({
            "payload": "x".repeat(MAX_WORKFLOW_PUBLIC_JSON_BYTES)
        }))
        .is_err());

        let metadata = WorkflowRetrievalMetadata::default();
        assert!(WorkflowRetrievalResult::new(
            "doc_1",
            None,
            None,
            Some(f64::NAN),
            None,
            metadata,
            None,
        )
        .is_err());
        assert!(WorkflowRetrieval::new(
            "ret_1",
            Some("x".repeat(MAX_WORKFLOW_RETRIEVAL_QUERY_BYTES + 1)),
            Vec::new(),
        )
        .is_err());

        let oversized_metadata = BTreeMap::from([(
            "public".to_owned(),
            json!("x".repeat(MAX_WORKFLOW_RETRIEVAL_METADATA_BYTES)),
        )]);
        assert!(WorkflowRetrievalMetadata::new(oversized_metadata).is_err());
    }

    #[test]
    fn closed_event_envelopes_reject_unknown_fields() {
        let response = PublicResponse {
            id: "resp_1".to_owned(),
            object: ResponseObjectKind::Response,
            status: ResponseStatus::InProgress,
            output: Vec::new(),
            usage: None,
            error: None,
        };
        let event = ResponseStreamEvent::ResponseCreated {
            sequence_number: 0,
            response,
        };
        let mut encoded = serde_json::to_value(event).unwrap();
        encoded["unexpected"] = json!(true);
        assert!(serde_json::from_value::<ResponseStreamEvent>(encoded).is_err());

        let tool = json!({
            "type": "workflow.tool.failed",
            "sequence_number": 3,
            "call_id": "call_1",
            "tool_name": "lookup",
            "error": {"code": "LOOKUP_FAILED", "message": "lookup failed", "raw": "secret"}
        });
        assert!(serde_json::from_value::<ResponseStreamEvent>(tool).is_err());
    }
}
