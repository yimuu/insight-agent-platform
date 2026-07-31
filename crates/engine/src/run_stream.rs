//! Public `run-stream/v1` protocol and its transient delivery contracts.
//!
//! The types in this module deliberately separate two representations:
//!
//! - [`RunStreamEvent`] is the closed, serializable caller contract;
//! - [`LiveRunStreamPublication`] is an internal, non-serializable envelope
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
use serde::{de::Error as _, ser::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use tokio::sync::Notify;

use crate::{ActivationId, ArtifactRef, AttemptNo, RunId};

mod durable;
mod retrieval_public_projection;
mod tool_public_projection;

pub use durable::{DurableRunStreamSnapshot, RunTerminalKind, RunUsageStatus};
pub use retrieval_public_projection::RunRetrievalPublicProjection;
pub use tool_public_projection::{RunToolCompletedArgumentsProjection, RunToolPublicProjection};

pub const RUN_STREAM_PROTOCOL_VERSION: &str = "run-stream/v1";

const LIVE_RUN_STREAM_CONFIG_INVALID: &str = "LIVE_RUN_STREAM_CONFIG_INVALID";
const LIVE_RUN_STREAM_STREAM_CLOSED: &str = "LIVE_RUN_STREAM_STREAM_CLOSED";
const LIVE_RUN_STREAM_IDENTITY_INVALID: &str = "LIVE_RUN_STREAM_IDENTITY_INVALID";
const LIVE_RUN_STREAM_FUNCTION_CALL_INVALID: &str = "LIVE_RUN_STREAM_FUNCTION_CALL_INVALID";
const MAX_PUBLIC_LABEL_BYTES: usize = 256;
const MAX_PUBLIC_MESSAGE_BYTES: usize = 512;
pub const MAX_FUNCTION_CALL_ARGUMENT_BYTES: usize = 256 * 1_024;
const MAX_FUNCTION_CALL_ARGUMENT_DEPTH: usize = 64;
const MAX_FUNCTION_CALL_ARGUMENT_VALUES: usize = 16_384;
const RUN_PUBLIC_RESULT_INVALID: &str = "RUN_PUBLIC_RESULT_INVALID";
const MAX_RUN_PUBLIC_TEXT_BYTES: usize = 64 * 1_024;
const MAX_RUN_PUBLIC_JSON_BYTES: usize = 64 * 1_024;
const MAX_RUN_PUBLIC_JSON_DEPTH: usize = 32;
const MAX_RUN_PUBLIC_JSON_VALUES: usize = 4_096;
const MAX_RUN_PUBLIC_JSON_STRING_BYTES: usize = 16 * 1_024;
const MAX_RUN_TOOL_CONTENT_PARTS: usize = 128;
const MAX_RUN_RETRIEVAL_RESULTS: usize = 256;
const MAX_RUN_RETRIEVAL_QUERY_BYTES: usize = 16 * 1_024;
const MAX_RUN_RETRIEVAL_TITLE_BYTES: usize = 4 * 1_024;
const MAX_RUN_RETRIEVAL_URI_BYTES: usize = 8 * 1_024;
const MAX_RUN_RETRIEVAL_SNIPPET_BYTES: usize = 64 * 1_024;
const MAX_RUN_RETRIEVAL_METADATA_BYTES: usize = 16 * 1_024;
const MAX_RUN_RETRIEVAL_METADATA_ENTRIES: usize = 128;

/// Exact public event set frozen by `run-stream/v1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RunStreamEventType {
    RunLifecycleCreated,
    RunLifecycleRunning,
    RunOutputItemAdded,
    RunOutputContentPartAdded,
    RunOutputTextDelta,
    RunOutputTextDone,
    RunOutputContentPartDone,
    RunOutputFunctionCallArgumentsDelta,
    RunOutputFunctionCallArgumentsDone,
    RunOutputItemDone,
    RunOutputFileSearchCallInProgress,
    RunOutputFileSearchCallSearching,
    RunOutputFileSearchCallCompleted,
    RunLifecycleCompleted,
    RunLifecycleFailed,
    RunStreamError,
    RunToolStarted,
    RunToolProgress,
    RunToolCompleted,
    RunToolFailed,
    RunRetrievalCompleted,
    RunStreamGap,
    RunLifecycleTimedOut,
    RunLifecycleCancelled,
    RunLifecycleInterrupted,
}

impl RunStreamEventType {
    pub const ALL: [Self; 25] = [
        Self::RunLifecycleCreated,
        Self::RunLifecycleRunning,
        Self::RunOutputItemAdded,
        Self::RunOutputContentPartAdded,
        Self::RunOutputTextDelta,
        Self::RunOutputTextDone,
        Self::RunOutputContentPartDone,
        Self::RunOutputFunctionCallArgumentsDelta,
        Self::RunOutputFunctionCallArgumentsDone,
        Self::RunOutputItemDone,
        Self::RunOutputFileSearchCallInProgress,
        Self::RunOutputFileSearchCallSearching,
        Self::RunOutputFileSearchCallCompleted,
        Self::RunLifecycleCompleted,
        Self::RunLifecycleFailed,
        Self::RunStreamError,
        Self::RunToolStarted,
        Self::RunToolProgress,
        Self::RunToolCompleted,
        Self::RunToolFailed,
        Self::RunRetrievalCompleted,
        Self::RunStreamGap,
        Self::RunLifecycleTimedOut,
        Self::RunLifecycleCancelled,
        Self::RunLifecycleInterrupted,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RunLifecycleCreated => "run.lifecycle.created",
            Self::RunLifecycleRunning => "run.lifecycle.running",
            Self::RunOutputItemAdded => "run.output.item.added",
            Self::RunOutputContentPartAdded => "run.output.content_part.added",
            Self::RunOutputTextDelta => "run.output.text.delta",
            Self::RunOutputTextDone => "run.output.text.done",
            Self::RunOutputContentPartDone => "run.output.content_part.done",
            Self::RunOutputFunctionCallArgumentsDelta => "run.output.function_call.arguments.delta",
            Self::RunOutputFunctionCallArgumentsDone => "run.output.function_call.arguments.done",
            Self::RunOutputItemDone => "run.output.item.done",
            Self::RunOutputFileSearchCallInProgress => "run.output.file_search_call.in_progress",
            Self::RunOutputFileSearchCallSearching => "run.output.file_search_call.searching",
            Self::RunOutputFileSearchCallCompleted => "run.output.file_search_call.completed",
            Self::RunLifecycleCompleted => "run.lifecycle.completed",
            Self::RunLifecycleFailed => "run.lifecycle.failed",
            Self::RunStreamError => "run.stream.error",
            Self::RunToolStarted => "run.tool.started",
            Self::RunToolProgress => "run.tool.progress",
            Self::RunToolCompleted => "run.tool.completed",
            Self::RunToolFailed => "run.tool.failed",
            Self::RunRetrievalCompleted => "run.retrieval.completed",
            Self::RunStreamGap => "run.stream.gap",
            Self::RunLifecycleTimedOut => "run.lifecycle.timed_out",
            Self::RunLifecycleCancelled => "run.lifecycle.cancelled",
            Self::RunLifecycleInterrupted => "run.lifecycle.interrupted",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|event_type| event_type.as_str() == value)
    }

    pub const fn is_run_terminal(self) -> bool {
        matches!(
            self,
            Self::RunLifecycleCompleted
                | Self::RunLifecycleFailed
                | Self::RunLifecycleTimedOut
                | Self::RunLifecycleCancelled
                | Self::RunLifecycleInterrupted
        )
    }

    pub const fn ends_stream(self) -> bool {
        self.is_run_terminal() || matches!(self, Self::RunStreamError)
    }
}

impl Serialize for RunStreamEventType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RunStreamEventType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).ok_or_else(|| D::Error::custom("unknown run-stream/v1 event type"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunObjectKind {
    Run,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Created,
    Running,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    Interrupted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOutputItemStatus {
    InProgress,
    Completed,
    Failed,
    Incomplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOutputRole {
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunOutputContentPart {
    OutputText {
        text: String,
        #[serde(default)]
        annotations: Vec<Value>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunOutputItem {
    Message {
        id: String,
        status: RunOutputItemStatus,
        role: RunOutputRole,
        content: Vec<RunOutputContentPart>,
    },
    FunctionCall {
        id: String,
        status: RunOutputItemStatus,
        call_id: String,
        name: String,
        arguments: String,
    },
    FileSearchCall {
        id: String,
        status: RunOutputItemStatus,
        #[serde(default)]
        queries: Vec<String>,
        #[serde(default)]
        results: Vec<Value>,
    },
}

impl RunOutputItem {
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
pub struct RunUsageInputDetails {
    pub cached_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunUsageOutputDetails {
    pub reasoning_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunUsage {
    pub input_tokens: u64,
    pub input_tokens_details: RunUsageInputDetails,
    pub output_tokens: u64,
    pub output_tokens_details: RunUsageOutputDetails,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunPublicError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunInitialSnapshot {
    pub id: String,
    pub object: RunObjectKind,
    pub status: RunStatus,
    pub output: Vec<RunOutputItem>,
    pub usage: Option<RunUsage>,
}

impl RunInitialSnapshot {
    pub fn new(run_id: impl Into<String>, status: RunStatus) -> Result<Self, &'static str> {
        if !matches!(status, RunStatus::Created | RunStatus::Running) {
            return Err("initial run snapshot status must be created or running");
        }
        let id = run_id.into();
        if !valid_public_label(&id) {
            return Err("run snapshot ID must be a stable public label");
        }
        Ok(Self {
            id,
            object: RunObjectKind::Run,
            status,
            output: Vec::new(),
            usage: None,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunInitialSnapshotWire {
    id: String,
    object: RunObjectKind,
    status: RunStatus,
    output: Vec<RunOutputItem>,
    usage: Option<RunUsage>,
}

impl<'de> Deserialize<'de> for RunInitialSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RunInitialSnapshotWire::deserialize(deserializer)?;
        if wire.object != RunObjectKind::Run
            || !matches!(wire.status, RunStatus::Created | RunStatus::Running)
            || !wire.output.is_empty()
            || wire.usage.is_some()
            || !valid_public_label(&wire.id)
        {
            return Err(D::Error::custom("invalid initial run snapshot"));
        }
        Ok(Self {
            id: wire.id,
            object: wire.object,
            status: wire.status,
            output: wire.output,
            usage: wire.usage,
        })
    }
}

/// Returns one bounded, body-free explanation for stable infrastructure
/// failure codes. Unknown codes remain intentionally generic.
pub fn public_failure_message(code: &str) -> &'static str {
    match code {
        "LLM_PROVIDER_AUTHENTICATION_FAILED" => "model provider authentication failed",
        "LLM_PROVIDER_PERMISSION_DENIED" => "model provider denied access",
        "LLM_PROVIDER_CONNECTION_FAILED" => "failed to connect to model provider",
        "LLM_PROVIDER_REQUEST_TIMEOUT" => "model provider request timed out",
        "LLM_PROVIDER_REQUEST_REJECTED" => "model provider rejected the request",
        "LLM_PROVIDER_RATE_LIMITED" => "model provider rate limit exceeded",
        "LLM_PROVIDER_UNAVAILABLE" => "model provider is unavailable",
        "LLM_PROVIDER_STREAM_FAILED" => "model provider stream failed",
        "LLM_PROVIDER_RESPONSE_INVALID" => "model provider returned an invalid response",
        "LLM_PROVIDER_RESPONSE_TOO_LARGE" => "model provider response exceeded the size limit",
        "LLM_PROVIDER_FAILED" => "model provider request failed",
        _ => "run failed",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunCompletedSnapshot {
    pub id: String,
    pub object: RunObjectKind,
    pub status: RunStatus,
    #[serde(default)]
    pub output: Vec<RunOutputItem>,
    pub result: Value,
    #[serde(default)]
    pub tool_results: Vec<RunToolResult>,
    #[serde(default)]
    pub retrievals: Vec<RunRetrieval>,
    pub usage: Option<RunUsage>,
    pub usage_status: RunUsageStatus,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunCompletedSnapshotWire {
    id: String,
    object: RunObjectKind,
    status: RunStatus,
    #[serde(default)]
    output: Vec<RunOutputItem>,
    result: Value,
    #[serde(default)]
    tool_results: Vec<RunToolResult>,
    #[serde(default)]
    retrievals: Vec<RunRetrieval>,
    usage: Option<RunUsage>,
    usage_status: RunUsageStatus,
}

impl<'de> Deserialize<'de> for RunCompletedSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RunCompletedSnapshotWire::deserialize(deserializer)?;
        let run = Self {
            id: wire.id,
            object: wire.object,
            status: wire.status,
            output: wire.output,
            result: wire.result,
            tool_results: wire.tool_results,
            retrievals: wire.retrievals,
            usage: wire.usage,
            usage_status: wire.usage_status,
        };
        validate_completed_run(&run).map_err(D::Error::custom)?;
        Ok(run)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunFailedSnapshot {
    pub id: String,
    pub object: RunObjectKind,
    pub status: RunStatus,
    #[serde(default)]
    pub output: Vec<RunOutputItem>,
    pub error: RunPublicError,
    #[serde(default)]
    pub tool_results: Vec<RunToolResult>,
    #[serde(default)]
    pub retrievals: Vec<RunRetrieval>,
    pub usage: Option<RunUsage>,
    pub usage_status: RunUsageStatus,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunFailedSnapshotWire {
    id: String,
    object: RunObjectKind,
    status: RunStatus,
    #[serde(default)]
    output: Vec<RunOutputItem>,
    error: RunPublicError,
    #[serde(default)]
    tool_results: Vec<RunToolResult>,
    #[serde(default)]
    retrievals: Vec<RunRetrieval>,
    usage: Option<RunUsage>,
    usage_status: RunUsageStatus,
}

impl<'de> Deserialize<'de> for RunFailedSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RunFailedSnapshotWire::deserialize(deserializer)?;
        if !matches!(wire.status, RunStatus::Failed | RunStatus::TimedOut) {
            return Err(D::Error::custom(
                "failed run snapshot status must be failed or timed_out",
            ));
        }
        let expected_status = wire.status;
        let run = Self {
            id: wire.id,
            object: wire.object,
            status: wire.status,
            output: wire.output,
            error: wire.error,
            tool_results: wire.tool_results,
            retrievals: wire.retrievals,
            usage: wire.usage,
            usage_status: wire.usage_status,
        };
        validate_failed_run(&run, expected_status).map_err(D::Error::custom)?;
        Ok(run)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunStoppedSnapshot {
    pub id: String,
    pub object: RunObjectKind,
    pub status: RunStatus,
    #[serde(default)]
    pub output: Vec<RunOutputItem>,
    #[serde(default)]
    pub tool_results: Vec<RunToolResult>,
    #[serde(default)]
    pub retrievals: Vec<RunRetrieval>,
    pub usage: Option<RunUsage>,
    pub usage_status: RunUsageStatus,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunStoppedSnapshotWire {
    id: String,
    object: RunObjectKind,
    status: RunStatus,
    #[serde(default)]
    output: Vec<RunOutputItem>,
    #[serde(default)]
    tool_results: Vec<RunToolResult>,
    #[serde(default)]
    retrievals: Vec<RunRetrieval>,
    usage: Option<RunUsage>,
    usage_status: RunUsageStatus,
}

impl<'de> Deserialize<'de> for RunStoppedSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RunStoppedSnapshotWire::deserialize(deserializer)?;
        if !matches!(wire.status, RunStatus::Cancelled | RunStatus::Interrupted) {
            return Err(D::Error::custom(
                "stopped run snapshot status must be cancelled or interrupted",
            ));
        }
        let expected_status = wire.status;
        let run = Self {
            id: wire.id,
            object: wire.object,
            status: wire.status,
            output: wire.output,
            tool_results: wire.tool_results,
            retrievals: wire.retrievals,
            usage: wire.usage,
            usage_status: wire.usage_status,
        };
        validate_stopped_run(&run, expected_status).map_err(D::Error::custom)?;
        Ok(run)
    }
}

/// Validation failure for a caller-visible tool or retrieval result.
///
/// The error deliberately has one stable public code and a body-free message:
/// rejected provider or executor values must not be reflected into logs or a
/// Run stream while the safe public projection is being built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunPublicResultError {
    message: &'static str,
}

impl RunPublicResultError {
    const fn new(message: &'static str) -> Self {
        Self { message }
    }

    pub const fn code(&self) -> &'static str {
        RUN_PUBLIC_RESULT_INVALID
    }

    pub const fn message(&self) -> &'static str {
        self.message
    }
}

impl fmt::Display for RunPublicResultError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl Error for RunPublicResultError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum RunToolContentWire {
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum RunToolProgressContentWire {
    #[serde(rename = "output_text")]
    Text { text: String },
    #[serde(rename = "output_json")]
    Json { json: Value },
}

/// Closed public content union for one live-only run tool progress update.
///
/// Progress deliberately excludes artifact-bearing variants because a
/// best-effort observation cannot establish durable artifact authority.
#[derive(Debug, Clone, PartialEq)]
pub struct RunToolProgressContent {
    wire: RunToolProgressContentWire,
}

impl RunToolProgressContent {
    pub fn output_text(text: impl Into<String>) -> Result<Self, RunPublicResultError> {
        let text = text.into();
        validate_bounded_public_string(
            &text,
            MAX_RUN_PUBLIC_TEXT_BYTES,
            "run tool progress text must be non-empty and bounded",
        )?;
        Ok(Self {
            wire: RunToolProgressContentWire::Text { text },
        })
    }

    pub fn output_json(json: Value) -> Result<Self, RunPublicResultError> {
        validate_bounded_public_json(&json, MAX_RUN_PUBLIC_JSON_BYTES)?;
        Ok(Self {
            wire: RunToolProgressContentWire::Json { json },
        })
    }

    pub fn text(&self) -> Option<&str> {
        match &self.wire {
            RunToolProgressContentWire::Text { text } => Some(text),
            RunToolProgressContentWire::Json { .. } => None,
        }
    }

    pub fn json(&self) -> Option<&Value> {
        match &self.wire {
            RunToolProgressContentWire::Json { json } => Some(json),
            RunToolProgressContentWire::Text { .. } => None,
        }
    }
}

impl Serialize for RunToolProgressContent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.wire.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RunToolProgressContent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match RunToolProgressContentWire::deserialize(deserializer)? {
            RunToolProgressContentWire::Text { text } => {
                Self::output_text(text).map_err(D::Error::custom)
            }
            RunToolProgressContentWire::Json { json } => {
                Self::output_json(json).map_err(D::Error::custom)
            }
        }
    }
}

/// Closed public content union for a completed run tool call.
///
/// The inner representation is private so in-process producers cannot bypass
/// the same limits enforced when a durable terminal snapshot is decoded.
#[derive(Debug, Clone, PartialEq)]
pub struct RunToolContent {
    wire: RunToolContentWire,
}

impl RunToolContent {
    pub fn output_text(text: impl Into<String>) -> Result<Self, RunPublicResultError> {
        let text = text.into();
        validate_bounded_public_string(
            &text,
            MAX_RUN_PUBLIC_TEXT_BYTES,
            "run tool text must be non-empty and bounded",
        )?;
        Ok(Self {
            wire: RunToolContentWire::Text { text },
        })
    }

    pub fn output_json(json: Value) -> Result<Self, RunPublicResultError> {
        validate_bounded_public_json(&json, MAX_RUN_PUBLIC_JSON_BYTES)?;
        Ok(Self {
            wire: RunToolContentWire::Json { json },
        })
    }

    pub fn output_image(artifact: ArtifactRef) -> Self {
        Self {
            wire: RunToolContentWire::Image { artifact },
        }
    }

    pub fn output_file(artifact: ArtifactRef) -> Self {
        Self {
            wire: RunToolContentWire::File { artifact },
        }
    }

    pub fn output_audio(artifact: ArtifactRef) -> Self {
        Self {
            wire: RunToolContentWire::Audio { artifact },
        }
    }

    pub fn text(&self) -> Option<&str> {
        match &self.wire {
            RunToolContentWire::Text { text } => Some(text),
            RunToolContentWire::Json { .. }
            | RunToolContentWire::Image { .. }
            | RunToolContentWire::File { .. }
            | RunToolContentWire::Audio { .. } => None,
        }
    }

    pub fn json(&self) -> Option<&Value> {
        match &self.wire {
            RunToolContentWire::Json { json } => Some(json),
            RunToolContentWire::Text { .. }
            | RunToolContentWire::Image { .. }
            | RunToolContentWire::File { .. }
            | RunToolContentWire::Audio { .. } => None,
        }
    }

    pub fn artifact(&self) -> Option<&ArtifactRef> {
        match &self.wire {
            RunToolContentWire::Image { artifact }
            | RunToolContentWire::File { artifact }
            | RunToolContentWire::Audio { artifact } => Some(artifact),
            RunToolContentWire::Text { .. } | RunToolContentWire::Json { .. } => None,
        }
    }
}

impl Serialize for RunToolContent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.wire.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RunToolContent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match RunToolContentWire::deserialize(deserializer)? {
            RunToolContentWire::Text { text } => Self::output_text(text).map_err(D::Error::custom),
            RunToolContentWire::Json { json } => Self::output_json(json).map_err(D::Error::custom),
            RunToolContentWire::Image { artifact } => Ok(Self::output_image(artifact)),
            RunToolContentWire::File { artifact } => Ok(Self::output_file(artifact)),
            RunToolContentWire::Audio { artifact } => Ok(Self::output_audio(artifact)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunToolResult {
    call_id: String,
    tool_name: String,
    content: Vec<RunToolContent>,
}

impl RunToolResult {
    pub fn new(
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        content: Vec<RunToolContent>,
    ) -> Result<Self, RunPublicResultError> {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        if !valid_public_label(&call_id) || !valid_public_label(&tool_name) {
            return Err(RunPublicResultError::new(
                "run tool identities must be stable public labels",
            ));
        }
        if content.len() > MAX_RUN_TOOL_CONTENT_PARTS {
            return Err(RunPublicResultError::new(
                "run tool content exceeds the public part limit",
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

    pub fn content(&self) -> &[RunToolContent] {
        &self.content
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunToolResultWire {
    call_id: String,
    tool_name: String,
    content: Vec<RunToolContent>,
}

impl<'de> Deserialize<'de> for RunToolResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RunToolResultWire::deserialize(deserializer)?;
        Self::new(wire.call_id, wire.tool_name, wire.content).map_err(D::Error::custom)
    }
}

/// Bounded object-valued metadata already projected by a retrieval policy.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
#[serde(transparent)]
pub struct RunRetrievalMetadata {
    entries: BTreeMap<String, Value>,
}

impl RunRetrievalMetadata {
    pub fn new(entries: BTreeMap<String, Value>) -> Result<Self, RunPublicResultError> {
        if entries.len() > MAX_RUN_RETRIEVAL_METADATA_ENTRIES
            || entries.keys().any(|key| {
                key.is_empty()
                    || key.len() > MAX_PUBLIC_LABEL_BYTES
                    || key.chars().any(char::is_control)
            })
        {
            return Err(RunPublicResultError::new(
                "run retrieval metadata keys must be non-empty and bounded",
            ));
        }
        let value = Value::Object(entries.clone().into_iter().collect());
        validate_bounded_public_json(&value, MAX_RUN_RETRIEVAL_METADATA_BYTES)?;
        Ok(Self { entries })
    }

    pub fn entries(&self) -> &BTreeMap<String, Value> {
        &self.entries
    }
}

impl<'de> Deserialize<'de> for RunRetrievalMetadata {
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
pub struct RunRetrievalResult {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snippet: Option<String>,
    metadata: RunRetrievalMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact: Option<ArtifactRef>,
}

impl RunRetrievalResult {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: impl Into<String>,
        title: Option<String>,
        uri: Option<String>,
        score: Option<f64>,
        snippet: Option<String>,
        metadata: RunRetrievalMetadata,
        artifact: Option<ArtifactRef>,
    ) -> Result<Self, RunPublicResultError> {
        let id = id.into();
        if !valid_public_label(&id) {
            return Err(RunPublicResultError::new(
                "run retrieval result ID must be a stable public label",
            ));
        }
        validate_optional_public_string(
            title.as_deref(),
            MAX_RUN_RETRIEVAL_TITLE_BYTES,
            "run retrieval title must be non-empty and bounded",
        )?;
        validate_optional_public_string(
            snippet.as_deref(),
            MAX_RUN_RETRIEVAL_SNIPPET_BYTES,
            "run retrieval snippet must be non-empty and bounded",
        )?;
        if uri.as_deref().is_some_and(|uri| {
            uri.is_empty()
                || uri.len() > MAX_RUN_RETRIEVAL_URI_BYTES
                || uri
                    .chars()
                    .any(|character| character.is_control() || character.is_whitespace())
        }) {
            return Err(RunPublicResultError::new(
                "run retrieval URI must be non-empty, bounded, and whitespace-free",
            ));
        }
        if score.is_some_and(|score| !score.is_finite()) {
            return Err(RunPublicResultError::new(
                "run retrieval score must be finite",
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

    pub fn metadata(&self) -> &RunRetrievalMetadata {
        &self.metadata
    }

    pub fn artifact(&self) -> Option<&ArtifactRef> {
        self.artifact.as_ref()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunRetrievalResultWire {
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
    metadata: RunRetrievalMetadata,
    #[serde(default)]
    artifact: Option<ArtifactRef>,
}

impl<'de> Deserialize<'de> for RunRetrievalResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RunRetrievalResultWire::deserialize(deserializer)?;
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
pub struct RunRetrieval {
    retrieval_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<String>,
    results: Vec<RunRetrievalResult>,
}

impl RunRetrieval {
    pub fn new(
        retrieval_id: impl Into<String>,
        query: Option<String>,
        results: Vec<RunRetrievalResult>,
    ) -> Result<Self, RunPublicResultError> {
        let retrieval_id = retrieval_id.into();
        if !valid_public_label(&retrieval_id) {
            return Err(RunPublicResultError::new(
                "run retrieval ID must be a stable public label",
            ));
        }
        validate_optional_public_string(
            query.as_deref(),
            MAX_RUN_RETRIEVAL_QUERY_BYTES,
            "run retrieval query must be non-empty and bounded",
        )?;
        if results.len() > MAX_RUN_RETRIEVAL_RESULTS {
            return Err(RunPublicResultError::new(
                "run retrieval exceeds the public result limit",
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

    pub fn results(&self) -> &[RunRetrievalResult] {
        &self.results
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunRetrievalWire {
    retrieval_id: String,
    #[serde(default)]
    query: Option<String>,
    results: Vec<RunRetrievalResult>,
}

impl<'de> Deserialize<'de> for RunRetrieval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RunRetrievalWire::deserialize(deserializer)?;
        Self::new(wire.retrieval_id, wire.query, wire.results).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStreamGapAction {
    DiscardProvisionalItem,
}

/// Closed public wire envelope. Internal Attempt, activation, model-call and
/// item-local sequence fields have no representation in this enum.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum RunStreamEvent {
    #[serde(rename = "run.lifecycle.created")]
    RunLifecycleCreated {
        sequence_number: u64,
        #[serde(
            serialize_with = "serialize_created_run",
            deserialize_with = "deserialize_created_run"
        )]
        run: RunInitialSnapshot,
    },
    #[serde(rename = "run.lifecycle.running")]
    RunLifecycleRunning {
        sequence_number: u64,
        #[serde(
            serialize_with = "serialize_running_run",
            deserialize_with = "deserialize_running_run"
        )]
        run: RunInitialSnapshot,
    },
    #[serde(rename = "run.output.item.added")]
    RunOutputItemAdded {
        sequence_number: u64,
        output_index: u32,
        item: RunOutputItem,
    },
    #[serde(rename = "run.output.content_part.added")]
    RunOutputContentPartAdded {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
        content_index: u32,
        part: RunOutputContentPart,
    },
    #[serde(rename = "run.output.text.delta")]
    RunOutputTextDelta {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
        content_index: u32,
        delta: String,
    },
    #[serde(rename = "run.output.text.done")]
    RunOutputTextDone {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
        content_index: u32,
        text: String,
    },
    #[serde(rename = "run.output.content_part.done")]
    RunOutputContentPartDone {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
        content_index: u32,
        part: RunOutputContentPart,
    },
    #[serde(rename = "run.output.function_call.arguments.delta")]
    RunOutputFunctionCallArgumentsDelta {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
        delta: String,
    },
    #[serde(rename = "run.output.function_call.arguments.done")]
    RunOutputFunctionCallArgumentsDone {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
        name: String,
        arguments: String,
    },
    #[serde(rename = "run.output.item.done")]
    RunOutputItemDone {
        sequence_number: u64,
        output_index: u32,
        item: RunOutputItem,
    },
    #[serde(rename = "run.output.file_search_call.in_progress")]
    RunOutputFileSearchCallInProgress {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
    },
    #[serde(rename = "run.output.file_search_call.searching")]
    RunOutputFileSearchCallSearching {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
    },
    #[serde(rename = "run.output.file_search_call.completed")]
    RunOutputFileSearchCallCompleted {
        sequence_number: u64,
        item_id: String,
        output_index: u32,
    },
    #[serde(rename = "run.lifecycle.completed")]
    RunLifecycleCompleted {
        sequence_number: u64,
        #[serde(
            serialize_with = "serialize_completed_run",
            deserialize_with = "deserialize_completed_run"
        )]
        run: RunCompletedSnapshot,
    },
    #[serde(rename = "run.lifecycle.failed")]
    RunLifecycleFailed {
        sequence_number: u64,
        #[serde(
            serialize_with = "serialize_failed_run",
            deserialize_with = "deserialize_failed_run"
        )]
        run: RunFailedSnapshot,
    },
    #[serde(rename = "run.stream.error")]
    RunStreamError {
        sequence_number: u64,
        #[serde(
            serialize_with = "serialize_run_stream_error_code",
            deserialize_with = "deserialize_run_stream_error_code"
        )]
        code: String,
        #[serde(
            serialize_with = "serialize_run_stream_error_message",
            deserialize_with = "deserialize_run_stream_error_message"
        )]
        message: String,
    },
    #[serde(rename = "run.tool.started")]
    RunToolStarted {
        sequence_number: u64,
        call_id: String,
        tool_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        arguments: Option<Value>,
    },
    #[serde(rename = "run.tool.progress")]
    RunToolProgress {
        sequence_number: u64,
        call_id: String,
        tool_name: String,
        #[serde(
            serialize_with = "serialize_run_tool_progress_content",
            deserialize_with = "deserialize_run_tool_progress_content"
        )]
        content: Vec<RunToolProgressContent>,
    },
    #[serde(rename = "run.tool.completed")]
    RunToolCompleted {
        sequence_number: u64,
        call_id: String,
        tool_name: String,
        duration_ms: u64,
        content: Vec<RunToolContent>,
    },
    #[serde(rename = "run.tool.failed")]
    RunToolFailed {
        sequence_number: u64,
        call_id: String,
        tool_name: String,
        duration_ms: u64,
        error: RunPublicError,
    },
    #[serde(rename = "run.retrieval.completed")]
    RunRetrievalCompleted {
        sequence_number: u64,
        retrieval_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        query: Option<String>,
        results: Vec<RunRetrievalResult>,
    },
    #[serde(rename = "run.stream.gap")]
    RunStreamGap {
        sequence_number: u64,
        item_id: String,
        attempt_no: u32,
        missing_from: u64,
        missing_to: Option<u64>,
        unknown_tail: bool,
        action: RunStreamGapAction,
    },
    #[serde(rename = "run.lifecycle.timed_out")]
    RunLifecycleTimedOut {
        sequence_number: u64,
        #[serde(
            serialize_with = "serialize_timed_out_run",
            deserialize_with = "deserialize_timed_out_run"
        )]
        run: RunFailedSnapshot,
    },
    #[serde(rename = "run.lifecycle.cancelled")]
    RunLifecycleCancelled {
        sequence_number: u64,
        #[serde(
            serialize_with = "serialize_cancelled_run",
            deserialize_with = "deserialize_cancelled_run"
        )]
        run: RunStoppedSnapshot,
    },
    #[serde(rename = "run.lifecycle.interrupted")]
    RunLifecycleInterrupted {
        sequence_number: u64,
        #[serde(
            serialize_with = "serialize_interrupted_run",
            deserialize_with = "deserialize_interrupted_run"
        )]
        run: RunStoppedSnapshot,
    },
}

fn validate_initial_run(
    run: &RunInitialSnapshot,
    expected_status: RunStatus,
) -> Result<(), &'static str> {
    if run.object != RunObjectKind::Run
        || run.status != expected_status
        || !valid_public_label(&run.id)
        || !run.output.is_empty()
        || run.usage.is_some()
    {
        return Err("initial run snapshot does not match its lifecycle event");
    }
    Ok(())
}

fn validate_terminal_run_common(
    id: &str,
    object: RunObjectKind,
    status: RunStatus,
    expected_status: RunStatus,
    usage: Option<&RunUsage>,
    usage_status: RunUsageStatus,
) -> Result<(), &'static str> {
    if object != RunObjectKind::Run
        || status != expected_status
        || !valid_public_label(id)
        || (usage_status == RunUsageStatus::Complete) != usage.is_some()
    {
        return Err("terminal run snapshot does not match its lifecycle event");
    }
    Ok(())
}

macro_rules! initial_run_serde {
    ($serialize:ident, $deserialize:ident, $status:expr) => {
        fn $serialize<S>(run: &RunInitialSnapshot, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            validate_initial_run(run, $status).map_err(S::Error::custom)?;
            run.serialize(serializer)
        }

        fn $deserialize<'de, D>(deserializer: D) -> Result<RunInitialSnapshot, D::Error>
        where
            D: Deserializer<'de>,
        {
            let run = RunInitialSnapshot::deserialize(deserializer)?;
            validate_initial_run(&run, $status).map_err(D::Error::custom)?;
            Ok(run)
        }
    };
}

initial_run_serde!(
    serialize_created_run,
    deserialize_created_run,
    RunStatus::Created
);
initial_run_serde!(
    serialize_running_run,
    deserialize_running_run,
    RunStatus::Running
);

fn valid_run_stream_error_code(code: &str) -> bool {
    code.len() > "RUN_STREAM_".len()
        && code.len() <= 128
        && code.starts_with("RUN_STREAM_")
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn serialize_run_stream_error_code<S>(code: &String, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if !valid_run_stream_error_code(code) {
        return Err(S::Error::custom("invalid Run stream error code"));
    }
    code.serialize(serializer)
}

fn deserialize_run_stream_error_code<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let code = String::deserialize(deserializer)?;
    if !valid_run_stream_error_code(&code) {
        return Err(D::Error::custom("invalid Run stream error code"));
    }
    Ok(code)
}

fn valid_run_stream_error_message(message: &str) -> bool {
    !message.is_empty()
        && message.len() <= MAX_PUBLIC_MESSAGE_BYTES
        && !message.chars().any(char::is_control)
}

fn serialize_run_stream_error_message<S>(message: &String, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if !valid_run_stream_error_message(message) {
        return Err(S::Error::custom("invalid Run stream error message"));
    }
    message.serialize(serializer)
}

fn deserialize_run_stream_error_message<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let message = String::deserialize(deserializer)?;
    if !valid_run_stream_error_message(&message) {
        return Err(D::Error::custom("invalid Run stream error message"));
    }
    Ok(message)
}

fn validate_completed_run(run: &RunCompletedSnapshot) -> Result<(), &'static str> {
    validate_terminal_run_common(
        &run.id,
        run.object,
        run.status,
        RunStatus::Completed,
        run.usage.as_ref(),
        run.usage_status,
    )
}

fn serialize_completed_run<S>(run: &RunCompletedSnapshot, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    validate_completed_run(run).map_err(S::Error::custom)?;
    run.serialize(serializer)
}

fn deserialize_completed_run<'de, D>(deserializer: D) -> Result<RunCompletedSnapshot, D::Error>
where
    D: Deserializer<'de>,
{
    let run = RunCompletedSnapshot::deserialize(deserializer)?;
    validate_completed_run(&run).map_err(D::Error::custom)?;
    Ok(run)
}

fn validate_failed_run(
    run: &RunFailedSnapshot,
    expected_status: RunStatus,
) -> Result<(), &'static str> {
    validate_terminal_run_common(
        &run.id,
        run.object,
        run.status,
        expected_status,
        run.usage.as_ref(),
        run.usage_status,
    )?;
    if !valid_public_label(&run.error.code)
        || run.error.message.is_empty()
        || run.error.message.len() > MAX_PUBLIC_LABEL_BYTES
        || run.error.message.chars().any(char::is_control)
    {
        return Err("terminal run error is not a safe public error");
    }
    Ok(())
}

macro_rules! failed_run_serde {
    ($serialize:ident, $deserialize:ident, $status:expr) => {
        fn $serialize<S>(run: &RunFailedSnapshot, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            validate_failed_run(run, $status).map_err(S::Error::custom)?;
            run.serialize(serializer)
        }

        fn $deserialize<'de, D>(deserializer: D) -> Result<RunFailedSnapshot, D::Error>
        where
            D: Deserializer<'de>,
        {
            let run = RunFailedSnapshot::deserialize(deserializer)?;
            validate_failed_run(&run, $status).map_err(D::Error::custom)?;
            Ok(run)
        }
    };
}

failed_run_serde!(
    serialize_failed_run,
    deserialize_failed_run,
    RunStatus::Failed
);
failed_run_serde!(
    serialize_timed_out_run,
    deserialize_timed_out_run,
    RunStatus::TimedOut
);

fn validate_stopped_run(
    run: &RunStoppedSnapshot,
    expected_status: RunStatus,
) -> Result<(), &'static str> {
    validate_terminal_run_common(
        &run.id,
        run.object,
        run.status,
        expected_status,
        run.usage.as_ref(),
        run.usage_status,
    )
}

macro_rules! stopped_run_serde {
    ($serialize:ident, $deserialize:ident, $status:expr) => {
        fn $serialize<S>(run: &RunStoppedSnapshot, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            validate_stopped_run(run, $status).map_err(S::Error::custom)?;
            run.serialize(serializer)
        }

        fn $deserialize<'de, D>(deserializer: D) -> Result<RunStoppedSnapshot, D::Error>
        where
            D: Deserializer<'de>,
        {
            let run = RunStoppedSnapshot::deserialize(deserializer)?;
            validate_stopped_run(&run, $status).map_err(D::Error::custom)?;
            Ok(run)
        }
    };
}

stopped_run_serde!(
    serialize_cancelled_run,
    deserialize_cancelled_run,
    RunStatus::Cancelled
);
stopped_run_serde!(
    serialize_interrupted_run,
    deserialize_interrupted_run,
    RunStatus::Interrupted
);

fn serialize_run_tool_progress_content<S>(
    content: &[RunToolProgressContent],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if content.is_empty() || content.len() > MAX_RUN_TOOL_CONTENT_PARTS {
        return Err(S::Error::custom(
            "run tool progress content must be non-empty and bounded",
        ));
    }
    content.serialize(serializer)
}

fn deserialize_run_tool_progress_content<'de, D>(
    deserializer: D,
) -> Result<Vec<RunToolProgressContent>, D::Error>
where
    D: Deserializer<'de>,
{
    let content = Vec::<RunToolProgressContent>::deserialize(deserializer)?;
    if content.is_empty() || content.len() > MAX_RUN_TOOL_CONTENT_PARTS {
        return Err(D::Error::custom(
            "run tool progress content must be non-empty and bounded",
        ));
    }
    Ok(content)
}

impl RunStreamEvent {
    pub const fn event_type(&self) -> RunStreamEventType {
        match self {
            Self::RunLifecycleCreated { .. } => RunStreamEventType::RunLifecycleCreated,
            Self::RunLifecycleRunning { .. } => RunStreamEventType::RunLifecycleRunning,
            Self::RunOutputItemAdded { .. } => RunStreamEventType::RunOutputItemAdded,
            Self::RunOutputContentPartAdded { .. } => RunStreamEventType::RunOutputContentPartAdded,
            Self::RunOutputTextDelta { .. } => RunStreamEventType::RunOutputTextDelta,
            Self::RunOutputTextDone { .. } => RunStreamEventType::RunOutputTextDone,
            Self::RunOutputContentPartDone { .. } => RunStreamEventType::RunOutputContentPartDone,
            Self::RunOutputFunctionCallArgumentsDelta { .. } => {
                RunStreamEventType::RunOutputFunctionCallArgumentsDelta
            }
            Self::RunOutputFunctionCallArgumentsDone { .. } => {
                RunStreamEventType::RunOutputFunctionCallArgumentsDone
            }
            Self::RunOutputItemDone { .. } => RunStreamEventType::RunOutputItemDone,
            Self::RunOutputFileSearchCallInProgress { .. } => {
                RunStreamEventType::RunOutputFileSearchCallInProgress
            }
            Self::RunOutputFileSearchCallSearching { .. } => {
                RunStreamEventType::RunOutputFileSearchCallSearching
            }
            Self::RunOutputFileSearchCallCompleted { .. } => {
                RunStreamEventType::RunOutputFileSearchCallCompleted
            }
            Self::RunLifecycleCompleted { .. } => RunStreamEventType::RunLifecycleCompleted,
            Self::RunLifecycleFailed { .. } => RunStreamEventType::RunLifecycleFailed,
            Self::RunStreamError { .. } => RunStreamEventType::RunStreamError,
            Self::RunToolStarted { .. } => RunStreamEventType::RunToolStarted,
            Self::RunToolProgress { .. } => RunStreamEventType::RunToolProgress,
            Self::RunToolCompleted { .. } => RunStreamEventType::RunToolCompleted,
            Self::RunToolFailed { .. } => RunStreamEventType::RunToolFailed,
            Self::RunRetrievalCompleted { .. } => RunStreamEventType::RunRetrievalCompleted,
            Self::RunStreamGap { .. } => RunStreamEventType::RunStreamGap,
            Self::RunLifecycleTimedOut { .. } => RunStreamEventType::RunLifecycleTimedOut,
            Self::RunLifecycleCancelled { .. } => RunStreamEventType::RunLifecycleCancelled,
            Self::RunLifecycleInterrupted { .. } => RunStreamEventType::RunLifecycleInterrupted,
        }
    }

    pub const fn sequence_number(&self) -> u64 {
        match self {
            Self::RunLifecycleCreated {
                sequence_number, ..
            }
            | Self::RunLifecycleRunning {
                sequence_number, ..
            }
            | Self::RunOutputItemAdded {
                sequence_number, ..
            }
            | Self::RunOutputContentPartAdded {
                sequence_number, ..
            }
            | Self::RunOutputTextDelta {
                sequence_number, ..
            }
            | Self::RunOutputTextDone {
                sequence_number, ..
            }
            | Self::RunOutputContentPartDone {
                sequence_number, ..
            }
            | Self::RunOutputFunctionCallArgumentsDelta {
                sequence_number, ..
            }
            | Self::RunOutputFunctionCallArgumentsDone {
                sequence_number, ..
            }
            | Self::RunOutputItemDone {
                sequence_number, ..
            }
            | Self::RunOutputFileSearchCallInProgress {
                sequence_number, ..
            }
            | Self::RunOutputFileSearchCallSearching {
                sequence_number, ..
            }
            | Self::RunOutputFileSearchCallCompleted {
                sequence_number, ..
            }
            | Self::RunLifecycleCompleted {
                sequence_number, ..
            }
            | Self::RunLifecycleFailed {
                sequence_number, ..
            }
            | Self::RunStreamError {
                sequence_number, ..
            }
            | Self::RunToolStarted {
                sequence_number, ..
            }
            | Self::RunToolProgress {
                sequence_number, ..
            }
            | Self::RunToolCompleted {
                sequence_number, ..
            }
            | Self::RunToolFailed {
                sequence_number, ..
            }
            | Self::RunRetrievalCompleted {
                sequence_number, ..
            }
            | Self::RunStreamGap {
                sequence_number, ..
            }
            | Self::RunLifecycleTimedOut {
                sequence_number, ..
            }
            | Self::RunLifecycleCancelled {
                sequence_number, ..
            }
            | Self::RunLifecycleInterrupted {
                sequence_number, ..
            } => *sequence_number,
        }
    }

    pub const fn is_run_terminal(&self) -> bool {
        self.event_type().is_run_terminal()
    }

    pub const fn ends_stream(&self) -> bool {
        self.event_type().ends_stream()
    }
}

/// Internal identity of one durable model output item. It is intentionally not
/// serializable; only `item_id`, `output_index`, and selected safe fields are
/// projected into public `run.output.*` events by the dispatcher.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LiveRunStreamItemIdentity {
    run_id: RunId,
    activation_id: ActivationId,
    attempt_no: AttemptNo,
    model_call_no: u32,
    item_id: String,
    output_index: u32,
}

impl LiveRunStreamItemIdentity {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_id: RunId,
        activation_id: ActivationId,
        attempt_no: AttemptNo,
        model_call_no: u32,
        item_id: impl Into<String>,
        output_index: u32,
    ) -> Result<Self, LiveRunStreamBrokerError> {
        let item_id = item_id.into();
        if model_call_no == 0 || !valid_public_label(&item_id) {
            return Err(LiveRunStreamBrokerError::new(
                LIVE_RUN_STREAM_IDENTITY_INVALID,
                "live Run stream item identity is invalid",
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

impl fmt::Debug for LiveRunStreamItemIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveRunStreamItemIdentity")
            .field("run_id", &self.run_id)
            .field("activation_id", &self.activation_id)
            .field("attempt_no", &self.attempt_no)
            .field("model_call_no", &self.model_call_no)
            .field("item_id", &self.item_id)
            .field("output_index", &self.output_index)
            .finish()
    }
}

/// Internal identity of one best-effort run observation source.
///
/// Unlike [`LiveRunStreamItemIdentity`], this identity deliberately has no
/// model-call, item, or output-index fields. Workflow tool and retrieval
/// observations are not durable Response output items and therefore cannot
/// participate in item seals, gaps, or the terminal manifest barrier.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LiveRunObservationIdentity {
    run_id: RunId,
    activation_id: ActivationId,
    attempt_no: AttemptNo,
    source_id: String,
}

impl LiveRunObservationIdentity {
    pub fn new(
        run_id: RunId,
        activation_id: ActivationId,
        attempt_no: AttemptNo,
        source_id: impl Into<String>,
    ) -> Result<Self, LiveRunStreamBrokerError> {
        let source_id = source_id.into();
        if !valid_public_label(&source_id) {
            return Err(LiveRunStreamBrokerError::new(
                LIVE_RUN_STREAM_IDENTITY_INVALID,
                "live run observation identity is invalid",
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

impl fmt::Debug for LiveRunObservationIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveRunObservationIdentity")
            .field("run_id", &self.run_id)
            .field("activation_id", &self.activation_id)
            .field("attempt_no", &self.attempt_no)
            .field("source_id", &self.source_id)
            .finish()
    }
}

/// Closed internal source identity for live Run stream publication.
///
/// The variants are intentionally not interchangeable: `response.*` payloads
/// require [`Self::OutputItem`], while public run observations require
/// [`Self::RunObservation`]. [`LiveRunStreamPublication`] enforces that
/// contract at construction and PostgreSQL decode boundaries.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LiveRunStreamSourceIdentity {
    OutputItem(LiveRunStreamItemIdentity),
    RunObservation(LiveRunObservationIdentity),
}

impl LiveRunStreamSourceIdentity {
    pub fn run_id(&self) -> &RunId {
        match self {
            Self::OutputItem(identity) => identity.run_id(),
            Self::RunObservation(identity) => identity.run_id(),
        }
    }

    pub fn output_item(&self) -> Option<&LiveRunStreamItemIdentity> {
        match self {
            Self::OutputItem(identity) => Some(identity),
            Self::RunObservation(_) => None,
        }
    }

    pub fn run_observation(&self) -> Option<&LiveRunObservationIdentity> {
        match self {
            Self::OutputItem(_) => None,
            Self::RunObservation(identity) => Some(identity),
        }
    }
}

impl From<LiveRunStreamItemIdentity> for LiveRunStreamSourceIdentity {
    fn from(identity: LiveRunStreamItemIdentity) -> Self {
        Self::OutputItem(identity)
    }
}

impl From<LiveRunObservationIdentity> for LiveRunStreamSourceIdentity {
    fn from(identity: LiveRunObservationIdentity) -> Self {
        Self::RunObservation(identity)
    }
}

impl fmt::Debug for LiveRunStreamSourceIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputItem(identity) => {
                formatter.debug_tuple("OutputItem").field(identity).finish()
            }
            Self::RunObservation(identity) => formatter
                .debug_tuple("RunObservation")
                .field(identity)
                .finish(),
        }
    }
}

/// Safe, already-authorized public projection before connection sequencing.
/// This type has a body-free custom `Debug` implementation and intentionally
/// has no `Serialize` implementation.
#[derive(Clone, PartialEq)]
pub enum LiveRunStreamPayload {
    OutputItemAdded {
        item: RunOutputItem,
    },
    ContentPartAdded {
        content_index: u32,
        part: RunOutputContentPart,
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
        part: RunOutputContentPart,
    },
    FunctionCallArgumentsDelta {
        delta: String,
    },
    FunctionCallArgumentsDone {
        name: String,
        arguments: String,
    },
    OutputItemDone {
        item: RunOutputItem,
    },
    FileSearchCallInProgress,
    FileSearchCallSearching,
    FileSearchCallCompleted,
    ToolStarted {
        call_id: String,
        tool_name: String,
        arguments: Option<Value>,
    },
    ToolProgress {
        call_id: String,
        tool_name: String,
        content: Vec<RunToolProgressContent>,
    },
    ToolCompleted {
        call_id: String,
        tool_name: String,
        duration_ms: u64,
        content: Vec<RunToolContent>,
    },
    ToolFailed {
        call_id: String,
        tool_name: String,
        duration_ms: u64,
        error: RunPublicError,
    },
    RetrievalCompleted {
        retrieval_id: String,
        query: Option<String>,
        results: Vec<RunRetrievalResult>,
    },
}

impl LiveRunStreamPayload {
    pub const fn event_type(&self) -> RunStreamEventType {
        match self {
            Self::OutputItemAdded { .. } => RunStreamEventType::RunOutputItemAdded,
            Self::ContentPartAdded { .. } => RunStreamEventType::RunOutputContentPartAdded,
            Self::OutputTextDelta { .. } => RunStreamEventType::RunOutputTextDelta,
            Self::OutputTextDone { .. } => RunStreamEventType::RunOutputTextDone,
            Self::ContentPartDone { .. } => RunStreamEventType::RunOutputContentPartDone,
            Self::FunctionCallArgumentsDelta { .. } => {
                RunStreamEventType::RunOutputFunctionCallArgumentsDelta
            }
            Self::FunctionCallArgumentsDone { .. } => {
                RunStreamEventType::RunOutputFunctionCallArgumentsDone
            }
            Self::OutputItemDone { .. } => RunStreamEventType::RunOutputItemDone,
            Self::FileSearchCallInProgress => RunStreamEventType::RunOutputFileSearchCallInProgress,
            Self::FileSearchCallSearching => RunStreamEventType::RunOutputFileSearchCallSearching,
            Self::FileSearchCallCompleted => RunStreamEventType::RunOutputFileSearchCallCompleted,
            Self::ToolStarted { .. } => RunStreamEventType::RunToolStarted,
            Self::ToolProgress { .. } => RunStreamEventType::RunToolProgress,
            Self::ToolCompleted { .. } => RunStreamEventType::RunToolCompleted,
            Self::ToolFailed { .. } => RunStreamEventType::RunToolFailed,
            Self::RetrievalCompleted { .. } => RunStreamEventType::RunRetrievalCompleted,
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

    const fn requires_run_observation_source(&self) -> bool {
        matches!(
            self,
            Self::ToolStarted { .. }
                | Self::ToolProgress { .. }
                | Self::ToolCompleted { .. }
                | Self::ToolFailed { .. }
                | Self::RetrievalCompleted { .. }
        )
    }
}

impl fmt::Debug for LiveRunStreamPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveRunStreamPayload")
            .field("event_type", &self.event_type())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq)]
pub struct LiveRunStreamPublication {
    source: LiveRunStreamSourceIdentity,
    local_sequence: u64,
    payload: LiveRunStreamPayload,
}

impl LiveRunStreamPublication {
    /// Constructs one durable Response output-item publication.
    ///
    /// Workflow tool and retrieval payloads are rejected; use
    /// [`Self::new_run_observation`] for those observations.
    pub fn new(
        identity: LiveRunStreamItemIdentity,
        local_sequence: u64,
        payload: LiveRunStreamPayload,
    ) -> Result<Self, LiveRunStreamBrokerError> {
        Self::from_source(identity.into(), local_sequence, payload)
    }

    /// Constructs one best-effort run tool or retrieval observation.
    /// Response output-item payloads are rejected.
    pub fn new_run_observation(
        identity: LiveRunObservationIdentity,
        local_sequence: u64,
        payload: LiveRunStreamPayload,
    ) -> Result<Self, LiveRunStreamBrokerError> {
        Self::from_source(identity.into(), local_sequence, payload)
    }

    pub(crate) fn from_source(
        source: LiveRunStreamSourceIdentity,
        local_sequence: u64,
        payload: LiveRunStreamPayload,
    ) -> Result<Self, LiveRunStreamBrokerError> {
        let source_matches = match &source {
            LiveRunStreamSourceIdentity::OutputItem(identity) => {
                payload.requires_output_item_source()
                    && match &payload {
                        LiveRunStreamPayload::OutputItemAdded { item }
                        | LiveRunStreamPayload::OutputItemDone { item } => {
                            item.id() == identity.item_id()
                        }
                        _ => true,
                    }
            }
            LiveRunStreamSourceIdentity::RunObservation(_) => {
                payload.requires_run_observation_source()
            }
        };
        if !source_matches {
            return Err(LiveRunStreamBrokerError::new(
                LIVE_RUN_STREAM_IDENTITY_INVALID,
                "live Run stream payload does not match its source identity",
            ));
        }
        Ok(Self {
            source,
            local_sequence,
            payload,
        })
    }

    pub fn source(&self) -> &LiveRunStreamSourceIdentity {
        &self.source
    }

    pub fn output_item_identity(&self) -> Option<&LiveRunStreamItemIdentity> {
        self.source.output_item()
    }

    pub fn run_observation_identity(&self) -> Option<&LiveRunObservationIdentity> {
        self.source.run_observation()
    }

    pub fn run_id(&self) -> &RunId {
        self.source.run_id()
    }

    pub fn local_sequence(&self) -> u64 {
        self.local_sequence
    }

    pub fn payload_type(&self) -> RunStreamEventType {
        self.payload.event_type()
    }

    pub(crate) fn payload(&self) -> &LiveRunStreamPayload {
        &self.payload
    }

    fn public_wire_bytes(&self) -> usize {
        serde_json::to_vec(&self.clone().into_public_event(u64::MAX))
            .map(|encoded| encoded.len())
            .unwrap_or(usize::MAX)
    }

    /// Applies the connection-local sequence and drops all internal ordering
    /// and execution identity fields from the public wire value.
    pub fn into_public_event(self, sequence_number: u64) -> RunStreamEvent {
        let LiveRunStreamPublication {
            source, payload, ..
        } = self;
        match (source, payload) {
            (
                LiveRunStreamSourceIdentity::OutputItem(identity),
                LiveRunStreamPayload::OutputItemAdded { item },
            ) => RunStreamEvent::RunOutputItemAdded {
                sequence_number,
                output_index: identity.output_index,
                item,
            },
            (
                LiveRunStreamSourceIdentity::OutputItem(identity),
                LiveRunStreamPayload::ContentPartAdded {
                    content_index,
                    part,
                },
            ) => RunStreamEvent::RunOutputContentPartAdded {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
                content_index,
                part,
            },
            (
                LiveRunStreamSourceIdentity::OutputItem(identity),
                LiveRunStreamPayload::OutputTextDelta {
                    content_index,
                    delta,
                },
            ) => RunStreamEvent::RunOutputTextDelta {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
                content_index,
                delta,
            },
            (
                LiveRunStreamSourceIdentity::OutputItem(identity),
                LiveRunStreamPayload::OutputTextDone {
                    content_index,
                    text,
                },
            ) => RunStreamEvent::RunOutputTextDone {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
                content_index,
                text,
            },
            (
                LiveRunStreamSourceIdentity::OutputItem(identity),
                LiveRunStreamPayload::ContentPartDone {
                    content_index,
                    part,
                },
            ) => RunStreamEvent::RunOutputContentPartDone {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
                content_index,
                part,
            },
            (
                LiveRunStreamSourceIdentity::OutputItem(identity),
                LiveRunStreamPayload::FunctionCallArgumentsDelta { delta },
            ) => RunStreamEvent::RunOutputFunctionCallArgumentsDelta {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
                delta,
            },
            (
                LiveRunStreamSourceIdentity::OutputItem(identity),
                LiveRunStreamPayload::FunctionCallArgumentsDone { name, arguments },
            ) => RunStreamEvent::RunOutputFunctionCallArgumentsDone {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
                name,
                arguments,
            },
            (
                LiveRunStreamSourceIdentity::OutputItem(identity),
                LiveRunStreamPayload::OutputItemDone { item },
            ) => RunStreamEvent::RunOutputItemDone {
                sequence_number,
                output_index: identity.output_index,
                item,
            },
            (
                LiveRunStreamSourceIdentity::OutputItem(identity),
                LiveRunStreamPayload::FileSearchCallInProgress,
            ) => RunStreamEvent::RunOutputFileSearchCallInProgress {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
            },
            (
                LiveRunStreamSourceIdentity::OutputItem(identity),
                LiveRunStreamPayload::FileSearchCallSearching,
            ) => RunStreamEvent::RunOutputFileSearchCallSearching {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
            },
            (
                LiveRunStreamSourceIdentity::OutputItem(identity),
                LiveRunStreamPayload::FileSearchCallCompleted,
            ) => RunStreamEvent::RunOutputFileSearchCallCompleted {
                sequence_number,
                item_id: identity.item_id,
                output_index: identity.output_index,
            },
            (
                LiveRunStreamSourceIdentity::RunObservation(_),
                LiveRunStreamPayload::ToolStarted {
                    call_id,
                    tool_name,
                    arguments,
                },
            ) => RunStreamEvent::RunToolStarted {
                sequence_number,
                call_id,
                tool_name,
                arguments,
            },
            (
                LiveRunStreamSourceIdentity::RunObservation(_),
                LiveRunStreamPayload::ToolProgress {
                    call_id,
                    tool_name,
                    content,
                },
            ) => RunStreamEvent::RunToolProgress {
                sequence_number,
                call_id,
                tool_name,
                content,
            },
            (
                LiveRunStreamSourceIdentity::RunObservation(_),
                LiveRunStreamPayload::ToolCompleted {
                    call_id,
                    tool_name,
                    duration_ms,
                    content,
                },
            ) => RunStreamEvent::RunToolCompleted {
                sequence_number,
                call_id,
                tool_name,
                duration_ms,
                content,
            },
            (
                LiveRunStreamSourceIdentity::RunObservation(_),
                LiveRunStreamPayload::ToolFailed {
                    call_id,
                    tool_name,
                    duration_ms,
                    error,
                },
            ) => RunStreamEvent::RunToolFailed {
                sequence_number,
                call_id,
                tool_name,
                duration_ms,
                error,
            },
            (
                LiveRunStreamSourceIdentity::RunObservation(_),
                LiveRunStreamPayload::RetrievalCompleted {
                    retrieval_id,
                    query,
                    results,
                },
            ) => RunStreamEvent::RunRetrievalCompleted {
                sequence_number,
                retrieval_id,
                query,
                results,
            },
            _ => unreachable!("LiveRunStreamPublication validates source and payload pairing"),
        }
    }
}

impl fmt::Debug for LiveRunStreamPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveRunStreamPublication")
            .field("source", &self.source)
            .field("local_sequence", &self.local_sequence)
            .field("event_type", &self.payload.event_type())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveRunStreamSealStatus {
    Completed,
    Incomplete,
}

#[derive(Clone, PartialEq, Eq)]
pub struct LiveRunStreamSeal {
    identity: LiveRunStreamItemIdentity,
    last_local_sequence: Option<u64>,
    status: LiveRunStreamSealStatus,
}

impl LiveRunStreamSeal {
    pub fn new(
        identity: LiveRunStreamItemIdentity,
        last_local_sequence: Option<u64>,
        status: LiveRunStreamSealStatus,
    ) -> Self {
        Self {
            identity,
            last_local_sequence,
            status,
        }
    }

    pub fn identity(&self) -> &LiveRunStreamItemIdentity {
        &self.identity
    }

    pub fn last_local_sequence(&self) -> Option<u64> {
        self.last_local_sequence
    }

    pub fn status(&self) -> LiveRunStreamSealStatus {
        self.status
    }
}

impl fmt::Debug for LiveRunStreamSeal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveRunStreamSeal")
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
    publications: [LiveRunStreamPublication; 4],
    seal: LiveRunStreamSeal,
}

impl CompletedFunctionCallPublication {
    pub const LAST_LOCAL_SEQUENCE: u64 = 3;

    pub fn build(
        identity: LiveRunStreamItemIdentity,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments_jcs: impl Into<String>,
    ) -> Result<Self, LiveRunStreamBrokerError> {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        let arguments_jcs = arguments_jcs.into();
        if !valid_public_label(&call_id) || !valid_public_label(&tool_name) {
            return Err(invalid_completed_function_call());
        }
        validate_function_call_arguments(&arguments_jcs)?;

        let item_id = identity.item_id().to_owned();
        let added_item = RunOutputItem::FunctionCall {
            id: item_id.clone(),
            status: RunOutputItemStatus::InProgress,
            call_id: call_id.clone(),
            name: tool_name.clone(),
            arguments: String::new(),
        };
        let completed_item = RunOutputItem::FunctionCall {
            id: item_id,
            status: RunOutputItemStatus::Completed,
            call_id,
            name: tool_name.clone(),
            arguments: arguments_jcs.clone(),
        };
        let publications = [
            LiveRunStreamPublication::new(
                identity.clone(),
                0,
                LiveRunStreamPayload::OutputItemAdded { item: added_item },
            )?,
            LiveRunStreamPublication::new(
                identity.clone(),
                1,
                LiveRunStreamPayload::FunctionCallArgumentsDelta {
                    delta: arguments_jcs.clone(),
                },
            )?,
            LiveRunStreamPublication::new(
                identity.clone(),
                2,
                LiveRunStreamPayload::FunctionCallArgumentsDone {
                    name: tool_name,
                    arguments: arguments_jcs,
                },
            )?,
            LiveRunStreamPublication::new(
                identity.clone(),
                Self::LAST_LOCAL_SEQUENCE,
                LiveRunStreamPayload::OutputItemDone {
                    item: completed_item,
                },
            )?,
        ];
        let seal = LiveRunStreamSeal::new(
            identity,
            Some(Self::LAST_LOCAL_SEQUENCE),
            LiveRunStreamSealStatus::Completed,
        );
        Ok(Self { publications, seal })
    }

    pub fn publications(&self) -> &[LiveRunStreamPublication; 4] {
        &self.publications
    }

    pub fn seal(&self) -> &LiveRunStreamSeal {
        &self.seal
    }

    pub fn into_parts(self) -> ([LiveRunStreamPublication; 4], LiveRunStreamSeal) {
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
    publications: [LiveRunStreamPublication; 2],
    seal: LiveRunStreamSeal,
}

impl CompletedFunctionCallTailPublication {
    pub fn build(
        identity: LiveRunStreamItemIdentity,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments_jcs: impl Into<String>,
        seal_index: u64,
    ) -> Result<Self, LiveRunStreamBrokerError> {
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
        let completed_item = RunOutputItem::FunctionCall {
            id: identity.item_id().to_owned(),
            status: RunOutputItemStatus::Completed,
            call_id,
            name: tool_name.clone(),
            arguments: arguments_jcs.clone(),
        };
        let publications = [
            LiveRunStreamPublication::new(
                identity.clone(),
                done_sequence,
                LiveRunStreamPayload::FunctionCallArgumentsDone {
                    name: tool_name,
                    arguments: arguments_jcs,
                },
            )?,
            LiveRunStreamPublication::new(
                identity.clone(),
                seal_index,
                LiveRunStreamPayload::OutputItemDone {
                    item: completed_item,
                },
            )?,
        ];
        let seal = LiveRunStreamSeal::new(
            identity,
            Some(seal_index),
            LiveRunStreamSealStatus::Completed,
        );
        Ok(Self { publications, seal })
    }

    pub fn into_parts(self) -> ([LiveRunStreamPublication; 2], LiveRunStreamSeal) {
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
pub struct LiveRunStreamGap {
    identity: LiveRunStreamItemIdentity,
    missing_from: u64,
    missing_to: Option<u64>,
    unknown_tail: bool,
}

impl LiveRunStreamGap {
    pub fn known(
        identity: LiveRunStreamItemIdentity,
        missing_from: u64,
        missing_to: u64,
    ) -> Result<Self, LiveRunStreamBrokerError> {
        if missing_from > missing_to {
            return Err(LiveRunStreamBrokerError::new(
                LIVE_RUN_STREAM_IDENTITY_INVALID,
                "live Run stream gap range is invalid",
            ));
        }
        Ok(Self {
            identity,
            missing_from,
            missing_to: Some(missing_to),
            unknown_tail: false,
        })
    }

    pub fn unknown_tail(identity: LiveRunStreamItemIdentity, missing_from: u64) -> Self {
        Self {
            identity,
            missing_from,
            missing_to: None,
            unknown_tail: true,
        }
    }

    pub fn identity(&self) -> &LiveRunStreamItemIdentity {
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

    pub fn into_public_event(self, sequence_number: u64) -> RunStreamEvent {
        RunStreamEvent::RunStreamGap {
            sequence_number,
            item_id: self.identity.item_id,
            attempt_no: self.identity.attempt_no.get(),
            missing_from: self.missing_from,
            missing_to: self.missing_to,
            unknown_tail: self.unknown_tail,
            action: RunStreamGapAction::DiscardProvisionalItem,
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

impl fmt::Debug for LiveRunStreamGap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveRunStreamGap")
            .field("identity", &self.identity)
            .field("missing_from", &self.missing_from)
            .field("missing_to", &self.missing_to)
            .field("unknown_tail", &self.unknown_tail)
            .finish()
    }
}

pub enum LiveRunStreamDelivery {
    Publication(LiveRunStreamPublication),
    Gap(LiveRunStreamGap),
    Seal(LiveRunStreamSeal),
}

impl fmt::Debug for LiveRunStreamDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Publication(publication) => publication.fmt(formatter),
            Self::Gap(gap) => gap.fmt(formatter),
            Self::Seal(seal) => seal.fmt(formatter),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveRunStreamPublishOutcome {
    Enqueued,
    EnqueuedAfterGap,
    /// A run observation was retained after one or more producer-local
    /// indices were skipped. No output-item gap is synthesized.
    EnqueuedAfterBestEffortLoss,
    DroppedWithGap,
    DroppedOversizeWithGap,
    /// A run observation was dropped by a bounded live-only queue. It is
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
pub struct LiveRunStreamCloseOutcome {
    unknown_tail_gaps: usize,
    omitted_unknown_tail_gaps: usize,
}

impl LiveRunStreamCloseOutcome {
    pub fn unknown_tail_gaps(self) -> usize {
        self.unknown_tail_gaps
    }

    pub fn omitted_unknown_tail_gaps(self) -> usize {
        self.omitted_unknown_tail_gaps
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveRunStreamBrokerError {
    code: &'static str,
    message: &'static str,
}

impl LiveRunStreamBrokerError {
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

impl fmt::Display for LiveRunStreamBrokerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl Error for LiveRunStreamBrokerError {}

#[async_trait]
pub trait LiveRunStreamSubscriber: Send {
    fn run_id(&self) -> &RunId;

    async fn recv(&mut self) -> Result<LiveRunStreamDelivery, LiveRunStreamBrokerError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveRunStreamBrokerCapability {
    SingleProcess,
    Shared,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveRunStreamByteLimits {
    pub max_frame_bytes: usize,
    pub max_item_bytes: usize,
    pub max_run_bytes: usize,
}

impl LiveRunStreamByteLimits {
    pub fn new(
        max_frame_bytes: usize,
        max_item_bytes: usize,
        max_run_bytes: usize,
    ) -> Result<Self, LiveRunStreamBrokerError> {
        if max_frame_bytes == 0
            || max_item_bytes < max_frame_bytes
            || max_run_bytes < max_item_bytes
        {
            return Err(LiveRunStreamBrokerError::new(
                LIVE_RUN_STREAM_CONFIG_INVALID,
                "live Run stream byte limits are invalid",
            ));
        }
        Ok(Self {
            max_frame_bytes,
            max_item_bytes,
            max_run_bytes,
        })
    }
}

impl Default for LiveRunStreamByteLimits {
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
pub trait LiveRunStreamBroker: Send + Sync {
    fn deployment_capability(&self) -> LiveRunStreamBrokerCapability;

    async fn check_readiness(
        &self,
        _readiness_timeout: std::time::Duration,
    ) -> Result<(), LiveRunStreamBrokerError> {
        Ok(())
    }

    async fn shutdown(&self, _grace: std::time::Duration) -> Result<(), LiveRunStreamBrokerError> {
        Ok(())
    }

    async fn subscribe(
        &self,
        run_id: RunId,
    ) -> Result<Box<dyn LiveRunStreamSubscriber>, LiveRunStreamBrokerError>;

    fn publish(&self, publication: LiveRunStreamPublication) -> LiveRunStreamPublishOutcome;

    fn seal(&self, seal: LiveRunStreamSeal) -> LiveRunStreamPublishOutcome;

    fn close_run(&self, run_id: &RunId) -> LiveRunStreamCloseOutcome;
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
        source: LiveRunStreamSourceIdentity,
        local_sequence: u64,
        payload: LiveRunStreamPayload,
    ) -> Result<LiveRunStreamPublication, LiveRunStreamBrokerError> {
        LiveRunStreamPublication::from_source(source, local_sequence, payload)
    }

    /// Borrows the already-authorized payload for a concrete transport codec.
    pub fn publication_payload(publication: &LiveRunStreamPublication) -> &LiveRunStreamPayload {
        publication.payload()
    }

    /// Reconstructs and validates one durable terminal run snapshot.
    pub fn durable_run_stream_snapshot_new(
        run_id: String,
        terminal_kind: RunTerminalKind,
        run: Value,
        public_item_manifest: Value,
        snapshot_hash: crate::ContentHash,
    ) -> Result<DurableRunStreamSnapshot, crate::repository::RepositoryError> {
        DurableRunStreamSnapshot::new(
            run_id,
            terminal_kind,
            run,
            public_item_manifest,
            snapshot_hash,
        )
    }

    /// Parses one closed durable run terminal discriminator.
    pub fn run_terminal_kind_parse(
        value: &str,
    ) -> Result<RunTerminalKind, crate::repository::RepositoryError> {
        RunTerminalKind::parse(value)
    }

    /// Parses one closed durable run usage discriminator.
    pub fn run_usage_status_parse(
        value: &str,
    ) -> Result<RunUsageStatus, crate::repository::RepositoryError> {
        RunUsageStatus::parse(value)
    }

    pub struct RunQueue {
        run_id: RunId,
        body_capacity: usize,
        control_capacity: usize,
        byte_limits: LiveRunStreamByteLimits,
        state: Mutex<RunQueueState>,
        notify: Notify,
    }

    #[derive(Default)]
    struct RunQueueState {
        body: VecDeque<LiveRunStreamPublication>,
        controls: VecDeque<QueueControl>,
        item_cursors: BTreeMap<LiveRunStreamItemIdentity, ItemCursor>,
        observation_cursors: BTreeMap<LiveRunObservationIdentity, ObservationCursor>,
        observed_bytes: usize,
        size_exhausted: bool,
        closed: bool,
    }

    #[derive(Clone)]
    enum QueueControl {
        Gap(LiveRunStreamGap),
        Seal(LiveRunStreamSeal),
    }

    #[derive(Clone, Default)]
    struct ItemCursor {
        next_local_sequence: u64,
        sequence_exhausted: bool,
        observed_bytes: usize,
        size_exhausted: bool,
        seal: Option<LiveRunStreamSeal>,
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
            byte_limits: LiveRunStreamByteLimits,
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

        pub fn publish(
            &self,
            publication: LiveRunStreamPublication,
        ) -> LiveRunStreamPublishOutcome {
            if publication.run_id() != &self.run_id {
                return LiveRunStreamPublishOutcome::NoSubscriber;
            }
            let source = publication.source().clone();
            let local_sequence = publication.local_sequence();
            let mut state = lock(&self.state);
            if state.closed {
                return LiveRunStreamPublishOutcome::RunClosed;
            }
            match source {
                LiveRunStreamSourceIdentity::OutputItem(identity) => {
                    self.publish_output_item(&mut state, publication, identity, local_sequence)
                }
                LiveRunStreamSourceIdentity::RunObservation(identity) => {
                    self.publish_run_observation(&mut state, publication, identity, local_sequence)
                }
            }
        }

        fn publish_output_item(
            &self,
            state: &mut RunQueueState,
            publication: LiveRunStreamPublication,
            identity: LiveRunStreamItemIdentity,
            local_sequence: u64,
        ) -> LiveRunStreamPublishOutcome {
            let cursor = state
                .item_cursors
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            if cursor.seal.is_some() {
                return LiveRunStreamPublishOutcome::RejectedAfterSeal;
            }
            let Some(expected) = cursor.expected() else {
                return LiveRunStreamPublishOutcome::RejectedOutOfOrder;
            };
            if local_sequence < expected {
                return LiveRunStreamPublishOutcome::RejectedOutOfOrder;
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
                    LiveRunStreamGap::known(identity.clone(), expected, local_sequence)
                        .expect("the observed sequence is never below the expected sequence"),
                )
            } else if local_sequence > expected {
                Some(
                    LiveRunStreamGap::known(identity.clone(), expected, local_sequence - 1)
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
                return LiveRunStreamPublishOutcome::ControlQueueFull;
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
                LiveRunStreamPublishOutcome::DroppedWithGap
            } else {
                state.body.push_back(publication);
                if gap.is_some() {
                    LiveRunStreamPublishOutcome::EnqueuedAfterGap
                } else {
                    LiveRunStreamPublishOutcome::Enqueued
                }
            };
            self.notify.notify_one();
            outcome
        }

        fn publish_run_observation(
            &self,
            state: &mut RunQueueState,
            publication: LiveRunStreamPublication,
            identity: LiveRunObservationIdentity,
            local_sequence: u64,
        ) -> LiveRunStreamPublishOutcome {
            let cursor = state
                .observation_cursors
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            let Some(expected) = cursor.expected() else {
                return LiveRunStreamPublishOutcome::RejectedOutOfOrder;
            };
            if local_sequence < expected {
                return LiveRunStreamPublishOutcome::RejectedOutOfOrder;
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
                return LiveRunStreamPublishOutcome::DroppedBestEffort;
            }
            state.body.push_back(publication);
            self.notify.notify_one();
            if local_sequence > expected {
                LiveRunStreamPublishOutcome::EnqueuedAfterBestEffortLoss
            } else {
                LiveRunStreamPublishOutcome::Enqueued
            }
        }

        /// Records producer-side loss for an observation that cannot fit another
        /// transient transport envelope. No public output-item gap is created.
        pub fn discard_run_observation(
            &self,
            identity: LiveRunObservationIdentity,
            local_sequence: u64,
        ) -> LiveRunStreamPublishOutcome {
            if identity.run_id() != &self.run_id {
                return LiveRunStreamPublishOutcome::NoSubscriber;
            }
            let mut state = lock(&self.state);
            if state.closed {
                return LiveRunStreamPublishOutcome::RunClosed;
            }
            let cursor = state
                .observation_cursors
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            let Some(expected) = cursor.expected() else {
                return LiveRunStreamPublishOutcome::RejectedOutOfOrder;
            };
            if local_sequence < expected {
                return LiveRunStreamPublishOutcome::RejectedOutOfOrder;
            }
            let mut next_cursor = cursor;
            next_cursor.observe(local_sequence);
            state.observation_cursors.insert(identity, next_cursor);
            LiveRunStreamPublishOutcome::DroppedBestEffort
        }

        pub fn seal(&self, seal: LiveRunStreamSeal) -> LiveRunStreamPublishOutcome {
            if seal.identity().run_id() != &self.run_id {
                return LiveRunStreamPublishOutcome::NoSubscriber;
            }
            let identity = seal.identity().clone();
            let mut state = lock(&self.state);
            if state.closed {
                return LiveRunStreamPublishOutcome::RunClosed;
            }
            let cursor = state
                .item_cursors
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            if let Some(existing) = &cursor.seal {
                return if existing == &seal {
                    LiveRunStreamPublishOutcome::SealExactReplay
                } else {
                    LiveRunStreamPublishOutcome::SealConflict
                };
            }
            let observed_last = cursor.observed_last();
            if seal.last_local_sequence < observed_last {
                return LiveRunStreamPublishOutcome::SealConflict;
            }

            let mut controls = state.controls.clone();
            if let Some(last) = seal.last_local_sequence {
                let missing_from = observed_last.map_or(0, |observed| observed.saturating_add(1));
                if observed_last.is_none_or(|observed| last > observed) {
                    let gap = LiveRunStreamGap::known(identity.clone(), missing_from, last)
                        .expect("a seal beyond observed data always forms a valid gap");
                    if !enqueue_gap(&mut controls, self.control_capacity, gap) {
                        return LiveRunStreamPublishOutcome::ControlQueueFull;
                    }
                }
            }
            if controls.len() >= self.control_capacity {
                return LiveRunStreamPublishOutcome::ControlQueueFull;
            }
            controls.push_back(QueueControl::Seal(seal.clone()));
            let mut next_cursor = cursor;
            next_cursor.seal = Some(seal);
            state.controls = controls;
            state.item_cursors.insert(identity, next_cursor);
            drop(state);
            self.notify.notify_one();
            LiveRunStreamPublishOutcome::SealEnqueued
        }

        pub fn discard_with_gap(
            &self,
            identity: LiveRunStreamItemIdentity,
            local_sequence: u64,
        ) -> LiveRunStreamPublishOutcome {
            if identity.run_id() != &self.run_id {
                return LiveRunStreamPublishOutcome::NoSubscriber;
            }
            let mut state = lock(&self.state);
            if state.closed {
                return LiveRunStreamPublishOutcome::RunClosed;
            }
            let cursor = state
                .item_cursors
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            if cursor.seal.is_some() {
                return LiveRunStreamPublishOutcome::RejectedAfterSeal;
            }
            let Some(expected) = cursor.expected() else {
                return LiveRunStreamPublishOutcome::RejectedOutOfOrder;
            };
            if local_sequence < expected {
                return LiveRunStreamPublishOutcome::RejectedOutOfOrder;
            }
            let gap = LiveRunStreamGap::known(identity.clone(), expected, local_sequence)
                .expect("the discarded sequence is never below the expected sequence");
            let mut controls = state.controls.clone();
            if !enqueue_gap(&mut controls, self.control_capacity, gap) {
                return LiveRunStreamPublishOutcome::ControlQueueFull;
            }
            let mut next_cursor = cursor;
            next_cursor.observe(local_sequence);
            state.controls = controls;
            state.item_cursors.insert(identity, next_cursor);
            drop(state);
            self.notify.notify_one();
            LiveRunStreamPublishOutcome::DroppedWithGap
        }

        pub fn discard_seal_with_gap(
            &self,
            identity: LiveRunStreamItemIdentity,
        ) -> LiveRunStreamPublishOutcome {
            if identity.run_id() != &self.run_id {
                return LiveRunStreamPublishOutcome::NoSubscriber;
            }
            let mut state = lock(&self.state);
            if state.closed {
                return LiveRunStreamPublishOutcome::RunClosed;
            }
            let cursor = state
                .item_cursors
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            if cursor.seal.is_some() {
                return LiveRunStreamPublishOutcome::RejectedAfterSeal;
            }
            let missing_from = cursor.expected().unwrap_or(u64::MAX);
            let gap = LiveRunStreamGap::unknown_tail(identity, missing_from);
            let mut controls = state.controls.clone();
            if !enqueue_gap(&mut controls, self.control_capacity, gap) {
                return LiveRunStreamPublishOutcome::ControlQueueFull;
            }
            state.controls = controls;
            drop(state);
            self.notify.notify_one();
            LiveRunStreamPublishOutcome::DroppedWithGap
        }

        pub fn accept_gap(&self, gap: LiveRunStreamGap) -> LiveRunStreamPublishOutcome {
            if gap.identity().run_id() != &self.run_id {
                return LiveRunStreamPublishOutcome::NoSubscriber;
            }
            let identity = gap.identity().clone();
            let mut state = lock(&self.state);
            if state.closed {
                return LiveRunStreamPublishOutcome::RunClosed;
            }
            let cursor = state
                .item_cursors
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            if cursor.seal.is_some() {
                return LiveRunStreamPublishOutcome::RejectedAfterSeal;
            }
            let mut controls = state.controls.clone();
            if !enqueue_gap(&mut controls, self.control_capacity, gap.clone()) {
                return LiveRunStreamPublishOutcome::ControlQueueFull;
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
            LiveRunStreamPublishOutcome::EnqueuedAfterGap
        }

        pub async fn recv(&self) -> Result<LiveRunStreamDelivery, LiveRunStreamBrokerError> {
            loop {
                let notified = self.notify.notified();
                {
                    let mut state = lock(&self.state);
                    if let Some(delivery) = next_delivery(&mut state) {
                        return Ok(delivery);
                    }
                    if state.closed {
                        return Err(LiveRunStreamBrokerError::new(
                            LIVE_RUN_STREAM_STREAM_CLOSED,
                            "live Run stream is closed",
                        ));
                    }
                }
                notified.await;
            }
        }

        pub fn close(&self) -> LiveRunStreamCloseOutcome {
            let mut state = lock(&self.state);
            if state.closed {
                return LiveRunStreamCloseOutcome::default();
            }
            let mut controls = state.controls.clone();
            let mut outcome = LiveRunStreamCloseOutcome::default();
            for (identity, cursor) in &state.item_cursors {
                if cursor.seal.is_some() {
                    continue;
                }
                let missing_from = cursor.expected().unwrap_or(u64::MAX);
                let gap = LiveRunStreamGap::unknown_tail(identity.clone(), missing_from);
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
        gap: LiveRunStreamGap,
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

    fn next_delivery(state: &mut RunQueueState) -> Option<LiveRunStreamDelivery> {
        if let Some(position) = state
            .controls
            .iter()
            .position(|control| matches!(control, QueueControl::Gap(_)))
        {
            let QueueControl::Gap(gap) = state.controls.remove(position)? else {
                unreachable!("the selected control is a gap")
            };
            return Some(LiveRunStreamDelivery::Gap(gap));
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
            return Some(LiveRunStreamDelivery::Seal(seal));
        }

        state
            .body
            .pop_front()
            .map(LiveRunStreamDelivery::Publication)
    }
}

fn validate_function_call_arguments(arguments_jcs: &str) -> Result<(), LiveRunStreamBrokerError> {
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

fn invalid_completed_function_call() -> LiveRunStreamBrokerError {
    LiveRunStreamBrokerError::new(
        LIVE_RUN_STREAM_FUNCTION_CALL_INVALID,
        "completed function-call publication is invalid",
    )
}

fn validate_optional_public_string(
    value: Option<&str>,
    max_bytes: usize,
    message: &'static str,
) -> Result<(), RunPublicResultError> {
    match value {
        Some(value) => validate_bounded_public_string(value, max_bytes, message),
        None => Ok(()),
    }
}

fn validate_bounded_public_string(
    value: &str,
    max_bytes: usize,
    message: &'static str,
) -> Result<(), RunPublicResultError> {
    if value.is_empty() || value.len() > max_bytes {
        return Err(RunPublicResultError::new(message));
    }
    Ok(())
}

fn validate_bounded_public_json(
    value: &Value,
    max_bytes: usize,
) -> Result<(), RunPublicResultError> {
    let encoded = serde_jcs::to_vec(value)
        .map_err(|_| RunPublicResultError::new("workflow public JSON must be canonicalizable"))?;
    if encoded.len() > max_bytes {
        return Err(RunPublicResultError::new(
            "workflow public JSON exceeds the inline byte limit",
        ));
    }

    let mut stack = vec![(value, 0_usize)];
    let mut observed_values = 0_usize;
    while let Some((current, depth)) = stack.pop() {
        observed_values = observed_values.saturating_add(1);
        if observed_values > MAX_RUN_PUBLIC_JSON_VALUES || depth > MAX_RUN_PUBLIC_JSON_DEPTH {
            return Err(RunPublicResultError::new(
                "workflow public JSON exceeds the structural limit",
            ));
        }
        match current {
            Value::String(string) if string.len() > MAX_RUN_PUBLIC_JSON_STRING_BYTES => {
                return Err(RunPublicResultError::new(
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
                    return Err(RunPublicResultError::new(
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
    use std::collections::BTreeSet;

    use super::*;
    use crate::{ArtifactId, ContentHash};
    use serde_json::json;

    fn run(value: &str) -> RunId {
        RunId::new(value).unwrap()
    }

    fn identity(run_id: &str) -> LiveRunStreamItemIdentity {
        LiveRunStreamItemIdentity::new(
            run(run_id),
            ActivationId::new("activation_answer").unwrap(),
            AttemptNo::FIRST,
            1,
            "msg_answer",
            0,
        )
        .unwrap()
    }

    fn workflow_identity(run_id: &str, source_id: &str) -> LiveRunObservationIdentity {
        LiveRunObservationIdentity::new(
            run(run_id),
            ActivationId::new("activation_run_observation").unwrap(),
            AttemptNo::FIRST,
            source_id,
        )
        .unwrap()
    }

    fn delta(
        identity: LiveRunStreamItemIdentity,
        sequence: u64,
        text: &str,
    ) -> LiveRunStreamPublication {
        LiveRunStreamPublication::new(
            identity,
            sequence,
            LiveRunStreamPayload::OutputTextDelta {
                content_index: 0,
                delta: text.to_owned(),
            },
        )
        .unwrap()
    }

    fn tool_started(
        identity: LiveRunObservationIdentity,
        sequence: u64,
        call_id: &str,
    ) -> LiveRunStreamPublication {
        LiveRunStreamPublication::new_run_observation(
            identity,
            sequence,
            LiveRunStreamPayload::ToolStarted {
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

    fn sample_output() -> Vec<RunOutputItem> {
        vec![
            RunOutputItem::Message {
                id: "msg_schema".to_owned(),
                status: RunOutputItemStatus::Completed,
                role: RunOutputRole::Assistant,
                content: vec![RunOutputContentPart::OutputText {
                    text: "complete".to_owned(),
                    annotations: vec![json!({"kind": "citation"})],
                }],
            },
            RunOutputItem::FunctionCall {
                id: "fn_schema".to_owned(),
                status: RunOutputItemStatus::Completed,
                call_id: "call_schema".to_owned(),
                name: "lookup".to_owned(),
                arguments: r#"{"indicator":"WBC"}"#.to_owned(),
            },
            RunOutputItem::FileSearchCall {
                id: "search_schema".to_owned(),
                status: RunOutputItemStatus::Completed,
                queries: vec!["WBC".to_owned()],
                results: vec![json!({"document_id": "doc_schema"})],
            },
        ]
    }

    fn sample_usage() -> RunUsage {
        RunUsage {
            input_tokens: 11,
            input_tokens_details: RunUsageInputDetails { cached_tokens: 3 },
            output_tokens: 7,
            output_tokens_details: RunUsageOutputDetails {
                reasoning_tokens: 2,
            },
            total_tokens: 18,
        }
    }

    fn sample_completed_run() -> RunCompletedSnapshot {
        let artifact = artifact("artifact_schema", "image/png");
        let tool_result = RunToolResult::new(
            "call_schema",
            "lookup",
            vec![
                RunToolContent::output_text("tool text").unwrap(),
                RunToolContent::output_json(json!({"ok": true})).unwrap(),
                RunToolContent::output_image(artifact.clone()),
                RunToolContent::output_file(artifact.clone()),
                RunToolContent::output_audio(artifact.clone()),
            ],
        )
        .unwrap();
        let retrieval_result = RunRetrievalResult::new(
            "result_schema",
            Some("Lab handbook".to_owned()),
            Some("https://example.test/lab".to_owned()),
            Some(0.95),
            Some("Reference range".to_owned()),
            RunRetrievalMetadata::new(BTreeMap::from([("source".to_owned(), json!("handbook"))]))
                .unwrap(),
            Some(artifact),
        )
        .unwrap();
        let retrieval =
            RunRetrieval::new("ret_schema", Some("WBC".to_owned()), vec![retrieval_result])
                .unwrap();
        RunCompletedSnapshot {
            id: "run_schema".to_owned(),
            object: RunObjectKind::Run,
            status: RunStatus::Completed,
            output: sample_output(),
            result: json!({"answer": "complete"}),
            tool_results: vec![tool_result],
            retrievals: vec![retrieval],
            usage: Some(sample_usage()),
            usage_status: RunUsageStatus::Complete,
        }
    }

    fn run_stream_event_samples() -> Vec<RunStreamEvent> {
        let empty_part = RunOutputContentPart::OutputText {
            text: String::new(),
            annotations: Vec::new(),
        };
        vec![
            RunStreamEvent::RunLifecycleCreated {
                sequence_number: 0,
                run: RunInitialSnapshot::new("run_schema", RunStatus::Created).unwrap(),
            },
            RunStreamEvent::RunLifecycleRunning {
                sequence_number: 1,
                run: RunInitialSnapshot::new("run_schema", RunStatus::Running).unwrap(),
            },
            RunStreamEvent::RunOutputItemAdded {
                sequence_number: 2,
                output_index: 0,
                item: RunOutputItem::Message {
                    id: "msg_schema".to_owned(),
                    status: RunOutputItemStatus::InProgress,
                    role: RunOutputRole::Assistant,
                    content: Vec::new(),
                },
            },
            RunStreamEvent::RunOutputContentPartAdded {
                sequence_number: 3,
                item_id: "msg_schema".to_owned(),
                output_index: 0,
                content_index: 0,
                part: empty_part,
            },
            RunStreamEvent::RunOutputTextDelta {
                sequence_number: 4,
                item_id: "msg_schema".to_owned(),
                output_index: 0,
                content_index: 0,
                delta: "partial".to_owned(),
            },
            RunStreamEvent::RunOutputTextDone {
                sequence_number: 5,
                item_id: "msg_schema".to_owned(),
                output_index: 0,
                content_index: 0,
                text: "complete".to_owned(),
            },
            RunStreamEvent::RunOutputContentPartDone {
                sequence_number: 6,
                item_id: "msg_schema".to_owned(),
                output_index: 0,
                content_index: 0,
                part: RunOutputContentPart::OutputText {
                    text: "complete".to_owned(),
                    annotations: Vec::new(),
                },
            },
            RunStreamEvent::RunOutputFunctionCallArgumentsDelta {
                sequence_number: 7,
                item_id: "fn_schema".to_owned(),
                output_index: 1,
                delta: r#"{"indicator":"#.to_owned(),
            },
            RunStreamEvent::RunOutputFunctionCallArgumentsDone {
                sequence_number: 8,
                item_id: "fn_schema".to_owned(),
                output_index: 1,
                name: "lookup".to_owned(),
                arguments: r#"{"indicator":"WBC"}"#.to_owned(),
            },
            RunStreamEvent::RunOutputItemDone {
                sequence_number: 9,
                output_index: 1,
                item: RunOutputItem::FunctionCall {
                    id: "fn_schema".to_owned(),
                    status: RunOutputItemStatus::Completed,
                    call_id: "call_schema".to_owned(),
                    name: "lookup".to_owned(),
                    arguments: r#"{"indicator":"WBC"}"#.to_owned(),
                },
            },
            RunStreamEvent::RunOutputFileSearchCallInProgress {
                sequence_number: 10,
                item_id: "search_schema".to_owned(),
                output_index: 2,
            },
            RunStreamEvent::RunOutputFileSearchCallSearching {
                sequence_number: 11,
                item_id: "search_schema".to_owned(),
                output_index: 2,
            },
            RunStreamEvent::RunOutputFileSearchCallCompleted {
                sequence_number: 12,
                item_id: "search_schema".to_owned(),
                output_index: 2,
            },
            RunStreamEvent::RunLifecycleCompleted {
                sequence_number: 13,
                run: sample_completed_run(),
            },
            RunStreamEvent::RunLifecycleFailed {
                sequence_number: 14,
                run: RunFailedSnapshot {
                    id: "run_schema".to_owned(),
                    object: RunObjectKind::Run,
                    status: RunStatus::Failed,
                    output: Vec::new(),
                    error: RunPublicError {
                        code: "RUN_FAILED".to_owned(),
                        message: "run failed".to_owned(),
                    },
                    tool_results: Vec::new(),
                    retrievals: Vec::new(),
                    usage: None,
                    usage_status: RunUsageStatus::Partial,
                },
            },
            RunStreamEvent::RunStreamError {
                sequence_number: 15,
                code: "RUN_STREAM_ERROR".to_owned(),
                message: "stream failed".to_owned(),
            },
            RunStreamEvent::RunToolStarted {
                sequence_number: 16,
                call_id: "call_schema".to_owned(),
                tool_name: "lookup".to_owned(),
                arguments: Some(json!({"indicator": "WBC"})),
            },
            RunStreamEvent::RunToolProgress {
                sequence_number: 17,
                call_id: "call_schema".to_owned(),
                tool_name: "lookup".to_owned(),
                content: vec![RunToolProgressContent::output_json(json!({"completed": 1})).unwrap()],
            },
            RunStreamEvent::RunToolCompleted {
                sequence_number: 18,
                call_id: "call_schema".to_owned(),
                tool_name: "lookup".to_owned(),
                duration_ms: 12,
                content: vec![RunToolContent::output_text("complete").unwrap()],
            },
            RunStreamEvent::RunToolFailed {
                sequence_number: 19,
                call_id: "call_failed".to_owned(),
                tool_name: "lookup".to_owned(),
                duration_ms: 7,
                error: RunPublicError {
                    code: "TOOL_FAILED".to_owned(),
                    message: "tool failed".to_owned(),
                },
            },
            RunStreamEvent::RunRetrievalCompleted {
                sequence_number: 20,
                retrieval_id: "retrieval_schema".to_owned(),
                query: Some("WBC".to_owned()),
                results: Vec::new(),
            },
            RunStreamEvent::RunStreamGap {
                sequence_number: 21,
                item_id: "msg_schema".to_owned(),
                attempt_no: 1,
                missing_from: 3,
                missing_to: None,
                unknown_tail: true,
                action: RunStreamGapAction::DiscardProvisionalItem,
            },
            RunStreamEvent::RunLifecycleTimedOut {
                sequence_number: 22,
                run: RunFailedSnapshot {
                    id: "run_schema".to_owned(),
                    object: RunObjectKind::Run,
                    status: RunStatus::TimedOut,
                    output: Vec::new(),
                    error: RunPublicError {
                        code: "RUN_TIMEOUT".to_owned(),
                        message: "run timed out".to_owned(),
                    },
                    tool_results: Vec::new(),
                    retrievals: Vec::new(),
                    usage: None,
                    usage_status: RunUsageStatus::Partial,
                },
            },
            RunStreamEvent::RunLifecycleCancelled {
                sequence_number: 23,
                run: RunStoppedSnapshot {
                    id: "run_schema".to_owned(),
                    object: RunObjectKind::Run,
                    status: RunStatus::Cancelled,
                    output: Vec::new(),
                    tool_results: Vec::new(),
                    retrievals: Vec::new(),
                    usage: None,
                    usage_status: RunUsageStatus::Partial,
                },
            },
            RunStreamEvent::RunLifecycleInterrupted {
                sequence_number: 24,
                run: RunStoppedSnapshot {
                    id: "run_schema".to_owned(),
                    object: RunObjectKind::Run,
                    status: RunStatus::Interrupted,
                    output: Vec::new(),
                    tool_results: Vec::new(),
                    retrievals: Vec::new(),
                    usage: None,
                    usage_status: RunUsageStatus::Partial,
                },
            },
        ]
    }

    #[test]
    fn run_stream_schema_is_pinned_to_the_complete_v1_event_contract() {
        let schema: Value =
            serde_json::from_str(workspace_asset_str!("schemas/run-stream-v1.json")).unwrap();
        assert_eq!(schema["$id"], "urn:insight-agent-platform:run-stream:v1");
        let validator =
            crate::schema::compile_schema_2020(&schema).expect("run-stream/v1 schema must compile");
        let samples = run_stream_event_samples();
        assert_eq!(samples.len(), RunStreamEventType::ALL.len());
        for (sample, expected_type) in samples
            .iter()
            .zip(RunStreamEventType::ALL.map(RunStreamEventType::as_str))
        {
            assert_eq!(sample.event_type().as_str(), expected_type);
            let encoded = serde_json::to_value(sample).unwrap();
            assert!(
                validator.is_valid(&encoded),
                "real {expected_type} serialization must match run-stream/v1"
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
    }

    #[test]
    fn run_stream_v2_schema_samples_cover_every_v1_event_without_weakening_v1() {
        let v1_schema: Value =
            serde_json::from_str(workspace_asset_str!("schemas/run-stream-v1.json")).unwrap();
        let v2_schema: Value =
            serde_json::from_str(workspace_asset_str!("schemas/run-stream-v2.json")).unwrap();
        let v1 =
            crate::schema::compile_schema_2020(&v1_schema).expect("run-stream/v1 schema compiles");
        let v2 =
            crate::schema::compile_schema_2020(&v2_schema).expect("run-stream/v2 schema compiles");
        let samples = run_stream_event_samples();
        assert_eq!(samples.len(), 25);
        for sample in samples {
            let mut encoded = serde_json::to_value(&sample).unwrap();
            encoded
                .as_object_mut()
                .unwrap()
                .insert("protocol".to_owned(), json!("run-stream/v2"));
            if matches!(
                sample.event_type(),
                RunStreamEventType::RunLifecycleCompleted
                    | RunStreamEventType::RunLifecycleFailed
                    | RunStreamEventType::RunLifecycleTimedOut
                    | RunStreamEventType::RunLifecycleCancelled
                    | RunStreamEventType::RunLifecycleInterrupted
            ) {
                encoded["run"]
                    .as_object_mut()
                    .expect("terminal v1 sample has a Run snapshot")
                    .insert("interactions".to_owned(), json!([]));
            }
            assert!(
                v2.is_valid(&encoded),
                "v2 sample for {} must validate",
                sample.event_type().as_str()
            );
            assert!(
                !v1.is_valid(&encoded),
                "the v1 decoder must reject a v2 identity"
            );
        }
    }

    #[test]
    fn checked_in_run_stream_v2_samples_cover_closed_protocol_surface() {
        let schema: Value =
            serde_json::from_str(workspace_asset_str!("schemas/run-stream-v2.json")).unwrap();
        let validator =
            crate::schema::compile_schema_2020(&schema).expect("run-stream/v2 schema compiles");
        let samples: Vec<Value> =
            serde_json::from_str(workspace_asset_str!("schemas/run-stream-v2.samples.json"))
                .unwrap();
        assert_eq!(samples.len(), 27);
        let types = samples
            .iter()
            .map(|sample| {
                assert!(
                    validator.is_valid(sample),
                    "checked-in v2 sample must validate"
                );
                sample["type"].as_str().unwrap().to_owned()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(types.len(), 27);
        for event_type in RunStreamEventType::ALL {
            assert!(types.contains(event_type.as_str()));
        }
        assert!(types.contains("run.interaction.required"));
        assert!(types.contains("run.interaction.closed"));
        for interaction in samples.iter().filter(|sample| {
            sample["type"]
                .as_str()
                .is_some_and(|name| name.starts_with("run.interaction."))
        }) {
            let encoded = serde_json::to_string(interaction).unwrap();
            for forbidden in [
                "requestState",
                "inputResponses",
                "access_token",
                "refresh_token",
                "form_response",
            ] {
                assert!(!encoded.contains(forbidden));
            }
        }
    }

    #[test]
    fn event_type_set_is_exact_and_rejects_unknown_names() {
        let names = RunStreamEventType::ALL
            .into_iter()
            .map(RunStreamEventType::as_str)
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "run.lifecycle.created",
                "run.lifecycle.running",
                "run.output.item.added",
                "run.output.content_part.added",
                "run.output.text.delta",
                "run.output.text.done",
                "run.output.content_part.done",
                "run.output.function_call.arguments.delta",
                "run.output.function_call.arguments.done",
                "run.output.item.done",
                "run.output.file_search_call.in_progress",
                "run.output.file_search_call.searching",
                "run.output.file_search_call.completed",
                "run.lifecycle.completed",
                "run.lifecycle.failed",
                "run.stream.error",
                "run.tool.started",
                "run.tool.progress",
                "run.tool.completed",
                "run.tool.failed",
                "run.retrieval.completed",
                "run.stream.gap",
                "run.lifecycle.timed_out",
                "run.lifecycle.cancelled",
                "run.lifecycle.interrupted",
            ]
        );
        for event_type in RunStreamEventType::ALL {
            let encoded = serde_json::to_value(event_type).unwrap();
            assert_eq!(encoded, json!(event_type.as_str()));
            assert_eq!(
                serde_json::from_value::<RunStreamEventType>(encoded).unwrap(),
                event_type
            );
        }
        assert!(serde_json::from_value::<RunStreamEventType>(json!("run.completed")).is_err());
        assert!(
            serde_json::from_value::<RunStreamEventType>(json!("workflow.tool_result.done"))
                .is_err()
        );
        for legacy_type in [
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
            "workflow.tool.progress",
            "workflow.tool.completed",
            "workflow.tool.failed",
            "workflow.retrieval.completed",
            "workflow.stream.gap",
            "workflow.response.timed_out",
            "workflow.response.cancelled",
            "workflow.response.interrupted",
        ] {
            assert!(
                serde_json::from_value::<RunStreamEventType>(json!(legacy_type)).is_err(),
                "legacy event type {legacy_type} must be rejected"
            );
            assert!(
                serde_json::from_value::<RunStreamEvent>(json!({
                    "type": legacy_type,
                    "sequence_number": 0
                }))
                .is_err(),
                "legacy event {legacy_type} must be rejected"
            );
        }
        assert!(!RunStreamEventType::RunStreamError.is_run_terminal());
        assert!(RunStreamEventType::RunStreamError.ends_stream());
        assert!(RunStreamEventType::RunLifecycleCompleted.is_run_terminal());
    }

    #[test]
    fn lifecycle_envelopes_reject_run_status_mismatches() {
        for (event_type, wrong_status) in [
            ("run.lifecycle.created", "running"),
            ("run.lifecycle.running", "created"),
            ("run.lifecycle.completed", "failed"),
            ("run.lifecycle.failed", "timed_out"),
            ("run.lifecycle.timed_out", "failed"),
            ("run.lifecycle.cancelled", "interrupted"),
            ("run.lifecycle.interrupted", "cancelled"),
        ] {
            let mut encoded = run_stream_event_samples()
                .into_iter()
                .find_map(|event| {
                    (event.event_type().as_str() == event_type)
                        .then(|| serde_json::to_value(event).unwrap())
                })
                .unwrap();
            encoded["run"]["status"] = json!(wrong_status);
            assert!(
                serde_json::from_value::<RunStreamEvent>(encoded).is_err(),
                "{event_type} must reject run.status={wrong_status}"
            );
        }
    }

    #[test]
    fn stream_error_rejects_unscoped_codes_and_unsafe_messages() {
        for invalid in [
            json!({
                "type": "run.stream.error",
                "sequence_number": 1,
                "code": "BROKER_LOST",
                "message": "stream failed"
            }),
            json!({
                "type": "run.stream.error",
                "sequence_number": 1,
                "code": "RUN_STREAM_LOST",
                "message": "unsafe\nmessage"
            }),
        ] {
            assert!(serde_json::from_value::<RunStreamEvent>(invalid).is_err());
        }
        assert!(serde_json::to_value(RunStreamEvent::RunStreamError {
            sequence_number: 1,
            code: "BROKER_LOST".to_owned(),
            message: "stream failed".to_owned(),
        })
        .is_err());
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
                "type": "run.output.text.delta",
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
        assert!(serde_json::from_value::<RunStreamEvent>(unknown).is_err());
    }

    #[test]
    fn publication_source_union_rejects_mismatches_and_hides_workflow_identity() {
        let item = identity("run_source_contract");
        assert!(LiveRunStreamPublication::new(
            item,
            0,
            LiveRunStreamPayload::ToolStarted {
                call_id: "call_wrong_source".to_owned(),
                tool_name: "lookup".to_owned(),
                arguments: None,
            },
        )
        .is_err());

        let observation = workflow_identity("run_source_contract", "tool_call_source");
        assert!(LiveRunStreamPublication::new_run_observation(
            observation.clone(),
            0,
            LiveRunStreamPayload::OutputTextDelta {
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
                "type": "run.tool.started",
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
                .map(LiveRunStreamPublication::local_sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(plan.seal().last_local_sequence(), Some(3));
        assert_eq!(plan.seal().status(), LiveRunStreamSealStatus::Completed);
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
                    "type": "run.output.item.added",
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
                    "type": "run.output.function_call.arguments.delta",
                    "sequence_number": 11,
                    "item_id": "msg_answer",
                    "output_index": 0,
                    "delta": arguments
                }),
                json!({
                    "type": "run.output.function_call.arguments.done",
                    "sequence_number": 12,
                    "item_id": "msg_answer",
                    "output_index": 0,
                    "name": "weather",
                    "arguments": arguments
                }),
                json!({
                    "type": "run.output.item.done",
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
                .map(LiveRunStreamPublication::local_sequence)
                .collect::<Vec<_>>(),
            vec![4, 5],
        );
        assert_eq!(
            frames
                .iter()
                .map(LiveRunStreamPublication::payload_type)
                .collect::<Vec<_>>(),
            vec![
                RunStreamEventType::RunOutputFunctionCallArgumentsDone,
                RunStreamEventType::RunOutputItemDone,
            ],
        );
        assert_eq!(seal.last_local_sequence(), Some(5));
        assert_eq!(seal.status(), LiveRunStreamSealStatus::Completed);
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
            assert_eq!(error.code(), LIVE_RUN_STREAM_FUNCTION_CALL_INVALID);
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
        assert_eq!(error.code(), LIVE_RUN_STREAM_FUNCTION_CALL_INVALID);
    }

    #[test]
    fn terminal_tool_and_retrieval_results_use_closed_typed_envelopes() {
        let image = serde_json::to_value(artifact("art_image", "image/png")).unwrap();
        let file = serde_json::to_value(artifact("art_file", "application/pdf")).unwrap();
        let audio = serde_json::to_value(artifact("art_audio", "audio/mpeg")).unwrap();
        let run: RunCompletedSnapshot = serde_json::from_value(json!({
            "id": "run_typed_terminal",
            "object": "run",
            "status": "completed",
            "output": [],
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
            "usage": null,
            "usage_status": "unavailable"
        }))
        .unwrap();
        assert_eq!(run.tool_results[0].call_id(), "call_lookup");
        assert_eq!(
            run.tool_results[0].content()[1].json(),
            Some(&json!({"score": 0.9}))
        );
        assert_eq!(run.retrievals[0].retrieval_id(), "ret_lookup");
        assert_eq!(run.retrievals[0].results()[0].id(), "doc_1");
        assert_eq!(run.retrievals[0].results()[0].score(), Some(0.92));
        assert_eq!(run.tool_results[0].content().len(), 5);

        let mut encoded = serde_json::to_value(run).unwrap();
        encoded["tool_results"][0]["raw_provider_payload"] = json!("private");
        assert!(serde_json::from_value::<RunCompletedSnapshot>(encoded).is_err());
    }

    #[test]
    fn terminal_public_results_reject_unknown_and_wrong_variants() {
        let base = json!({
            "id": "run_invalid_terminal",
            "object": "run",
            "status": "completed",
            "output": [],
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
            "usage": null,
            "usage_status": "unavailable"
        });

        let mut unknown_variant = base.clone();
        unknown_variant["tool_results"][0]["content"][0]["type"] = json!("output_video");
        assert!(serde_json::from_value::<RunCompletedSnapshot>(unknown_variant).is_err());

        let mut inline_binary = base.clone();
        inline_binary["tool_results"][0]["content"][0] = json!({
            "type": "output_image",
            "base64": "aGVsbG8="
        });
        assert!(serde_json::from_value::<RunCompletedSnapshot>(inline_binary).is_err());

        let mut unknown_retrieval_field = base.clone();
        unknown_retrieval_field["retrievals"][0]["results"][0]["raw_document"] = json!("private");
        assert!(serde_json::from_value::<RunCompletedSnapshot>(unknown_retrieval_field).is_err());

        let mut wrong_retrieval_shape = base;
        wrong_retrieval_shape["retrievals"][0]["results"][0] = json!("doc_1");
        assert!(serde_json::from_value::<RunCompletedSnapshot>(wrong_retrieval_shape).is_err());
    }

    #[test]
    fn public_result_constructors_enforce_identity_score_and_inline_bounds() {
        assert!(RunToolResult::new("not stable", "lookup", Vec::new()).is_err());
        assert!(RunToolContent::output_text("x".repeat(MAX_RUN_PUBLIC_TEXT_BYTES + 1)).is_err());
        assert!(RunToolContent::output_json(json!({
            "payload": "x".repeat(MAX_RUN_PUBLIC_JSON_BYTES)
        }))
        .is_err());

        let metadata = RunRetrievalMetadata::default();
        assert!(
            RunRetrievalResult::new("doc_1", None, None, Some(f64::NAN), None, metadata, None,)
                .is_err()
        );
        assert!(RunRetrieval::new(
            "ret_1",
            Some("x".repeat(MAX_RUN_RETRIEVAL_QUERY_BYTES + 1)),
            Vec::new(),
        )
        .is_err());

        let oversized_metadata = BTreeMap::from([(
            "public".to_owned(),
            json!("x".repeat(MAX_RUN_RETRIEVAL_METADATA_BYTES)),
        )]);
        assert!(RunRetrievalMetadata::new(oversized_metadata).is_err());
    }

    #[test]
    fn closed_event_envelopes_reject_unknown_fields() {
        let event = RunStreamEvent::RunLifecycleCreated {
            sequence_number: 0,
            run: RunInitialSnapshot::new("run_1", RunStatus::Created).unwrap(),
        };
        let mut encoded = serde_json::to_value(event).unwrap();
        encoded["unexpected"] = json!(true);
        assert!(serde_json::from_value::<RunStreamEvent>(encoded).is_err());

        let tool = json!({
            "type": "run.tool.failed",
            "sequence_number": 3,
            "call_id": "call_1",
            "tool_name": "lookup",
            "duration_ms": 5,
            "error": {"code": "LOOKUP_FAILED", "message": "lookup failed", "raw": "secret"}
        });
        assert!(serde_json::from_value::<RunStreamEvent>(tool).is_err());

        for terminal_type in ["run.tool.completed", "run.tool.failed"] {
            let missing_duration = if terminal_type == "run.tool.completed" {
                json!({
                    "type": terminal_type,
                    "sequence_number": 3,
                    "call_id": "call_1",
                    "tool_name": "lookup",
                    "content": []
                })
            } else {
                json!({
                    "type": terminal_type,
                    "sequence_number": 3,
                    "call_id": "call_1",
                    "tool_name": "lookup",
                    "error": {"code": "LOOKUP_FAILED", "message": "lookup failed"}
                })
            };
            assert!(
                serde_json::from_value::<RunStreamEvent>(missing_duration).is_err(),
                "{terminal_type} must require duration_ms"
            );
        }
    }

    #[test]
    fn workflow_tool_progress_wire_is_closed_nonempty_and_excludes_artifacts() {
        let valid = json!({
            "type": "run.tool.progress",
            "sequence_number": 4,
            "call_id": "call_progress",
            "tool_name": "example.progress",
            "content": [
                {"type": "output_text", "text": "halfway"},
                {"type": "output_json", "json": {"completed": 1, "total": 2}}
            ]
        });
        let decoded = serde_json::from_value::<RunStreamEvent>(valid.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), valid);

        for invalid in [
            json!({
                "type": "run.tool.progress",
                "sequence_number": 4,
                "call_id": "call_progress",
                "tool_name": "example.progress",
                "content": []
            }),
            json!({
                "type": "run.tool.progress",
                "sequence_number": 4,
                "call_id": "call_progress",
                "tool_name": "example.progress",
                "content": [{"type": "output_file", "artifact": {
                    "artifact_id": "artifact_1",
                    "content_hash": concat!(
                        "sha256:",
                        "0000000000000000000000000000000000000000000000000000000000000000"
                    ),
                    "size_bytes": 1,
                    "media_type": "text/plain"
                }}]
            }),
            json!({
                "type": "run.tool.progress",
                "sequence_number": 4,
                "call_id": "call_progress",
                "tool_name": "example.progress",
                "content": [{"type": "output_text", "text": "safe", "raw": "secret"}]
            }),
        ] {
            assert!(serde_json::from_value::<RunStreamEvent>(invalid).is_err());
        }

        let too_many = RunStreamEvent::RunToolProgress {
            sequence_number: 4,
            call_id: "call_progress".to_owned(),
            tool_name: "example.progress".to_owned(),
            content: (0..=MAX_RUN_TOOL_CONTENT_PARTS)
                .map(|_| RunToolProgressContent::output_text("safe").unwrap())
                .collect(),
        };
        assert!(serde_json::to_value(too_many).is_err());
    }
}
