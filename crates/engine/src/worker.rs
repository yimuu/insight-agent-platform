//! Closed worker-adapter boundary for durable leaf tasks.
//!
//! Workers execute one already-leased leaf effect. They never choose control
//! edges, mint Activations, or commit Run state. The durable repository owns
//! fencing and result publication; this module only resolves an immutable
//! implementation/version tuple and validates its typed output.

use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use tokio_util::sync::CancellationToken;

pub use crate::WorkerFailureClass;

use super::{
    plan::{DataPortId, DescriptorValue, PlanType, PortName, SecretRef, VersionTag},
    retrieval::RetrievalCompletion,
    scheduler::{
        BoundTaskInput, RuntimeValue, SafeError, SchedulerAction, SchedulerIntent, SchedulerTaskId,
        SchedulerTaskKind, TaskAdmissionClass, TaskOutputContract,
    },
    ActivationId, AttemptNo, EffectEvidence, EffectId, LeaseEpoch, NodeId, RunId,
    WorkerEffectPolicy,
};

pub const WORKER_TASK_KIND_MISMATCH: &str = "ENGINE_WORKER_TASK_KIND_MISMATCH";
pub const WORKER_IMPLEMENTATION_NOT_FOUND: &str = "ENGINE_WORKER_IMPLEMENTATION_NOT_FOUND";
pub const WORKER_OUTPUT_INVALID: &str = "ENGINE_WORKER_OUTPUT_INVALID";
pub const WORKER_FAILURE_INVALID: &str = "ENGINE_WORKER_FAILURE_INVALID";
pub const WORKER_EXECUTION_CONTEXT_INVALID: &str = "ENGINE_WORKER_EXECUTION_CONTEXT_INVALID";
pub const WORKER_MODEL_TOOL_CLAIM_INVALID: &str = "ENGINE_WORKER_MODEL_TOOL_CLAIM_INVALID";

const MAX_FAILURE_CODE_BYTES: usize = 128;
const MAX_MODEL_CONTINUATION_TURNS: usize = 1_024;
const MAX_MODEL_CONTINUATION_CALLS: usize = 1_024;
const MAX_MODEL_CONTINUATION_BYTES: usize = 8 * 1024 * 1024;
const MAX_MODEL_TOOL_ARGUMENT_JSON_BYTES: usize = 256 * 1024;
const MAX_MODEL_TOOL_RESULT_JSON_BYTES: usize = 1024 * 1024;
const MAX_MODEL_ASSISTANT_CONTENT_BYTES: usize = 1024 * 1024;
const MAX_FAILED_MODEL_FUNCTION_SEAL_INDEX: u64 = 16_385;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseItemAuthority {
    item_id: String,
    output_index: u32,
}

impl ResponseItemAuthority {
    pub fn new(item_id: impl Into<String>, output_index: u32) -> Result<Self, &'static str> {
        let item_id = item_id.into();
        if item_id.is_empty()
            || item_id.len() > 256
            || item_id
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
        {
            return Err(WORKER_EXECUTION_CONTEXT_INVALID);
        }
        Ok(Self {
            item_id,
            output_index,
        })
    }

    pub fn item_id(&self) -> &str {
        &self.item_id
    }

    pub fn output_index(&self) -> u32 {
        self.output_index
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCallAuthority {
    response_id: String,
    model_call_no: u32,
    publish: bool,
    public_item: Option<ResponseItemAuthority>,
}

impl ModelCallAuthority {
    pub fn new(
        response_id: impl Into<String>,
        model_call_no: u32,
        public_item: Option<ResponseItemAuthority>,
    ) -> Result<Self, &'static str> {
        Self::new_with_publication(
            response_id,
            model_call_no,
            public_item.is_some(),
            public_item,
        )
    }

    pub fn new_with_publication(
        response_id: impl Into<String>,
        model_call_no: u32,
        publish: bool,
        public_item: Option<ResponseItemAuthority>,
    ) -> Result<Self, &'static str> {
        let response_id = response_id.into();
        if response_id.is_empty()
            || response_id.len() > 256
            || response_id
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
            || model_call_no == 0
            || (!publish && public_item.is_some())
        {
            return Err(WORKER_EXECUTION_CONTEXT_INVALID);
        }
        Ok(Self {
            response_id,
            model_call_no,
            publish,
            public_item,
        })
    }

    pub fn response_id(&self) -> &str {
        &self.response_id
    }

    pub fn model_call_no(&self) -> u32 {
        self.model_call_no
    }

    pub fn publication_enabled(&self) -> bool {
        self.publish
    }

    pub fn public_item(&self) -> Option<&ResponseItemAuthority> {
        self.public_item.as_ref()
    }
}

/// Workspace-internal bridge used by runtime adapters. This module is public
/// only because those adapters live in a different crate; it is not part of
/// the stable root facade.
#[doc(hidden)]
pub mod adapter {
    use tokio::sync::{mpsc, oneshot};
    use tokio_util::sync::CancellationToken;

    use super::{
        ResponseItemAuthority, SafeError, TaskExecutionRequest, TaskExecutionResult,
        WorkerExecutionContext, WorkerExecutorRegistry, WorkerFailure, WorkerRuntimeServices,
    };

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ModelCallPublicItemReservationError {
        OperationDeadlineElapsed,
        StaleLease,
        StateConflict,
        AuthorityUnavailable,
    }

    pub struct ModelCallPublicItemReservationRequest {
        kind: ModelCallPublicItemReservationKind,
        response:
            oneshot::Sender<Result<ResponseItemAuthority, ModelCallPublicItemReservationError>>,
    }

    #[derive(Clone, PartialEq, Eq)]
    pub enum ModelCallPublicItemReservationKind {
        Message,
        FunctionCall {
            call_index: u32,
            call_id: String,
            tool_name: String,
        },
    }

    /// One-shot bridge from an executing model adapter back to the scheduler
    /// loop that owns the current, renewable task claim. Public item allocation
    /// must never use the stale claim snapshot captured before provider I/O.
    #[derive(Clone)]
    pub struct ModelCallPublicItemAllocator {
        requests: mpsc::Sender<ModelCallPublicItemReservationRequest>,
    }

    impl ModelCallPublicItemAllocator {
        async fn reserve_kind(
            &self,
            kind: ModelCallPublicItemReservationKind,
        ) -> Result<ResponseItemAuthority, ModelCallPublicItemReservationError> {
            let (response, receiver) = oneshot::channel();
            self.requests
                .send(ModelCallPublicItemReservationRequest { kind, response })
                .await
                .map_err(|_| ModelCallPublicItemReservationError::AuthorityUnavailable)?;
            receiver
                .await
                .map_err(|_| ModelCallPublicItemReservationError::AuthorityUnavailable)?
        }
    }

    pub fn model_call_public_item_reservation_channel() -> (
        ModelCallPublicItemAllocator,
        mpsc::Receiver<ModelCallPublicItemReservationRequest>,
    ) {
        let (requests, receiver) = mpsc::channel(1);
        (ModelCallPublicItemAllocator { requests }, receiver)
    }

    pub fn reservation_kind(
        request: &ModelCallPublicItemReservationRequest,
    ) -> &ModelCallPublicItemReservationKind {
        &request.kind
    }

    pub fn respond_reservation(
        request: ModelCallPublicItemReservationRequest,
        outcome: Result<ResponseItemAuthority, ModelCallPublicItemReservationError>,
    ) {
        let _ = request.response.send(outcome);
    }

    pub async fn reserve_public_item(
        allocator: &ModelCallPublicItemAllocator,
    ) -> Result<ResponseItemAuthority, ModelCallPublicItemReservationError> {
        allocator
            .reserve_kind(ModelCallPublicItemReservationKind::Message)
            .await
    }

    pub async fn reserve_public_function_call(
        allocator: &ModelCallPublicItemAllocator,
        call_index: u32,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
    ) -> Result<ResponseItemAuthority, ModelCallPublicItemReservationError> {
        allocator
            .reserve_kind(ModelCallPublicItemReservationKind::FunctionCall {
                call_index,
                call_id: call_id.into(),
                tool_name: tool_name.into(),
            })
            .await
    }

    pub fn services_with_model_call_public_item_allocator(
        mut services: WorkerRuntimeServices,
        allocator: ModelCallPublicItemAllocator,
    ) -> WorkerRuntimeServices {
        services.model_call_public_item_allocator = Some(allocator);
        services
    }

    pub fn services_model_call_public_item_allocator(
        services: &WorkerRuntimeServices,
    ) -> Option<&ModelCallPublicItemAllocator> {
        services.model_call_public_item_allocator.as_ref()
    }

    pub fn worker_failure_typed_safe_error(failure: &WorkerFailure) -> Option<&SafeError> {
        failure.typed_safe_error()
    }

    pub fn is_model_tool_action_request(request: &TaskExecutionRequest) -> bool {
        request.is_model_tool_action_request()
    }

    pub async fn execute_with_runtime_services(
        registry: &WorkerExecutorRegistry,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        services: &WorkerRuntimeServices,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        registry
            .execute_with_runtime_services(context, request, services, cancellation)
            .await
    }
}

#[derive(Clone, Default)]
pub struct WorkerRuntimeServices {
    model_call_public_item_allocator: Option<adapter::ModelCallPublicItemAllocator>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelFinishReason {
    Stop,
    ToolCalls,
    Length,
    ContentFilter,
    Invalid,
}

impl ModelFinishReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::ToolCalls => "tool_calls",
            Self::Length => "length",
            Self::ContentFilter => "content_filter",
            Self::Invalid => "invalid",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTokenUsage {
    pub input_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
}

impl ModelTokenUsage {
    pub fn is_complete(&self) -> bool {
        self.input_tokens.is_some()
            && self.cached_tokens.is_some()
            && self.output_tokens.is_some()
            && self.reasoning_tokens.is_some()
            && self.total_tokens.is_some()
    }

    pub fn public_value(&self) -> Option<serde_json::Value> {
        self.is_complete().then(|| {
            serde_json::json!({
                "input_tokens": self.input_tokens,
                "input_tokens_details": {"cached_tokens": self.cached_tokens},
                "output_tokens": self.output_tokens,
                "output_tokens_details": {"reasoning_tokens": self.reasoning_tokens},
                "total_tokens": self.total_tokens,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelIncompleteFunctionCallPublication {
    call_index: u32,
    public_item: ResponseItemAuthority,
    call_id: String,
    tool_name: String,
    seal_index: u64,
}

impl ModelIncompleteFunctionCallPublication {
    pub fn new(
        call_index: u32,
        public_item: ResponseItemAuthority,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        seal_index: u64,
    ) -> Result<Self, &'static str> {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        if !valid_model_call_id(&call_id)
            || !valid_model_tool_name(&tool_name)
            || !(1..=MAX_FAILED_MODEL_FUNCTION_SEAL_INDEX).contains(&seal_index)
        {
            return Err(WORKER_OUTPUT_INVALID);
        }
        Ok(Self {
            call_index,
            public_item,
            call_id,
            tool_name,
            seal_index,
        })
    }

    pub fn call_index(&self) -> u32 {
        self.call_index
    }

    pub fn public_item(&self) -> &ResponseItemAuthority {
        &self.public_item
    }

    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }

    pub fn seal_index(&self) -> u64 {
        self.seal_index
    }

    fn is_valid(&self) -> bool {
        valid_model_call_id(&self.call_id)
            && valid_model_tool_name(&self.tool_name)
            && (1..=MAX_FAILED_MODEL_FUNCTION_SEAL_INDEX).contains(&self.seal_index)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCallCompletion {
    model_call_no: u32,
    finish_reason: ModelFinishReason,
    usage: Option<ModelTokenUsage>,
    public_item_seal_index: Option<u64>,
    safe_public_item: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    incomplete_function_calls: Vec<ModelIncompleteFunctionCallPublication>,
}

impl ModelCallCompletion {
    pub fn new(
        model_call_no: u32,
        finish_reason: ModelFinishReason,
        usage: Option<ModelTokenUsage>,
        public_item_seal_index: Option<u64>,
        safe_public_item: Option<serde_json::Value>,
    ) -> Result<Self, &'static str> {
        if model_call_no == 0
            || safe_public_item
                .as_ref()
                .is_some_and(|item| !item.is_object() || public_item_seal_index.is_none())
        {
            return Err(WORKER_OUTPUT_INVALID);
        }
        Ok(Self {
            model_call_no,
            finish_reason,
            usage,
            public_item_seal_index,
            safe_public_item,
            incomplete_function_calls: Vec::new(),
        })
    }

    pub fn with_incomplete_function_calls(
        mut self,
        calls: Vec<ModelIncompleteFunctionCallPublication>,
    ) -> Result<Self, &'static str> {
        if calls.is_empty() {
            return Ok(self);
        }
        let mut item_ids = BTreeSet::new();
        let mut output_indices = BTreeSet::new();
        if matches!(
            self.finish_reason,
            ModelFinishReason::Stop | ModelFinishReason::ToolCalls
        ) || calls.iter().enumerate().any(|(position, call)| {
            !call.is_valid()
                || (position > 0 && calls[position - 1].call_index >= call.call_index)
                || !item_ids.insert(call.public_item.item_id.as_str())
                || !output_indices.insert(call.public_item.output_index)
        }) {
            return Err(WORKER_OUTPUT_INVALID);
        }
        self.incomplete_function_calls = calls;
        Ok(self)
    }

    pub fn model_call_no(&self) -> u32 {
        self.model_call_no
    }

    pub fn finish_reason(&self) -> ModelFinishReason {
        self.finish_reason
    }

    pub fn usage(&self) -> Option<&ModelTokenUsage> {
        self.usage.as_ref()
    }

    pub fn public_item_seal_index(&self) -> Option<u64> {
        self.public_item_seal_index
    }

    pub fn safe_public_item(&self) -> Option<&serde_json::Value> {
        self.safe_public_item.as_ref()
    }

    pub fn incomplete_function_calls(&self) -> &[ModelIncompleteFunctionCallPublication] {
        &self.incomplete_function_calls
    }
}

/// One complete, schema-validated function call returned by a model. This is
/// a scheduler handoff payload, never an instruction for the worker to execute
/// the tool in memory.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolCall {
    index: u32,
    call_id: String,
    name: String,
    arguments: serde_json::Value,
}

impl fmt::Debug for ModelToolCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelToolCall")
            .field("index", &self.index)
            .field("name", &self.name)
            .field("arguments_present", &true)
            .finish()
    }
}

impl ModelToolCall {
    pub fn new(
        index: u32,
        call_id: impl Into<String>,
        name: impl Into<String>,
        arguments: serde_json::Value,
    ) -> Result<Self, &'static str> {
        let call_id = call_id.into();
        let name = name.into();
        if !valid_model_call_id(&call_id)
            || !valid_model_tool_name(&name)
            || !valid_bounded_model_json(&arguments, true, MAX_MODEL_TOOL_ARGUMENT_JSON_BYTES)
        {
            return Err(WORKER_OUTPUT_INVALID);
        }
        Ok(Self {
            index,
            call_id,
            name,
            arguments,
        })
    }

    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn arguments(&self) -> &serde_json::Value {
        &self.arguments
    }

    fn is_valid(&self) -> bool {
        valid_model_call_id(&self.call_id)
            && valid_model_tool_name(&self.name)
            && valid_bounded_model_json(&self.arguments, true, MAX_MODEL_TOOL_ARGUMENT_JSON_BYTES)
    }
}

fn valid_model_call_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
}

fn valid_model_tool_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn valid_bounded_model_json(
    value: &serde_json::Value,
    require_object: bool,
    max_bytes: usize,
) -> bool {
    (!require_object || value.is_object())
        && serde_jcs::to_vec(value).is_ok_and(|bytes| bytes.len() <= max_bytes)
}

/// Closed durable-continuation disposition for one model call. The assistant
/// content is retained because a later model continuation must reconstruct the
/// exact assistant tool-call message before appending tool results.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolCallBatch {
    model_call_no: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    assistant_content: Option<String>,
    calls: Vec<ModelToolCall>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    public_function_calls: Vec<ModelFunctionCallPublication>,
}

impl fmt::Debug for ModelToolCallBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelToolCallBatch")
            .field("model_call_no", &self.model_call_no)
            .field(
                "assistant_content_present",
                &self.assistant_content.is_some(),
            )
            .field("call_count", &self.calls.len())
            .field(
                "public_function_call_count",
                &self.public_function_calls.len(),
            )
            .finish()
    }
}

impl ModelToolCallBatch {
    pub fn new(
        model_call_no: u32,
        assistant_content: Option<String>,
        calls: Vec<ModelToolCall>,
    ) -> Result<Self, &'static str> {
        let mut call_ids = BTreeMap::new();
        let valid = model_call_no > 0
            && assistant_content
                .as_ref()
                .is_none_or(|content| !content.is_empty())
            && !calls.is_empty()
            && calls.iter().enumerate().all(|(index, call)| {
                call.index == u32::try_from(index).unwrap_or(u32::MAX)
                    && call_ids.insert(call.call_id.as_str(), ()).is_none()
            });
        if !valid {
            return Err(WORKER_OUTPUT_INVALID);
        }
        Ok(Self {
            model_call_no,
            assistant_content,
            calls,
            public_function_calls: Vec::new(),
        })
    }

    pub fn with_public_function_calls(
        mut self,
        public_function_calls: Vec<ModelFunctionCallPublication>,
    ) -> Result<Self, &'static str> {
        if public_function_calls
            .iter()
            .enumerate()
            .any(|(position, publication)| {
                publication.argument_delta_count == 0
                    || self
                        .calls
                        .get(usize::try_from(publication.call_index).unwrap_or(usize::MAX))
                        .is_none_or(|call| call.index != publication.call_index)
                    || public_function_calls[..position]
                        .iter()
                        .any(|prior| prior.call_index == publication.call_index)
            })
        {
            return Err(WORKER_OUTPUT_INVALID);
        }
        self.public_function_calls = public_function_calls;
        Ok(self)
    }

    pub fn model_call_no(&self) -> u32 {
        self.model_call_no
    }

    pub fn assistant_content(&self) -> Option<&str> {
        self.assistant_content.as_deref()
    }

    pub fn calls(&self) -> &[ModelToolCall] {
        &self.calls
    }

    pub fn public_function_calls(&self) -> &[ModelFunctionCallPublication] {
        &self.public_function_calls
    }
}

/// Repository-minted public identity and the exact count of Provider argument
/// fragments already emitted for one completed function call. Bodies remain
/// transient; only this bounded sequencing metadata crosses the checkpoint.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelFunctionCallPublication {
    call_index: u32,
    public_item: ResponseItemAuthority,
    argument_delta_count: u64,
}

impl fmt::Debug for ModelFunctionCallPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelFunctionCallPublication")
            .field("call_index", &self.call_index)
            .field("output_index", &self.public_item.output_index())
            .field("argument_delta_count", &self.argument_delta_count)
            .finish()
    }
}

impl ModelFunctionCallPublication {
    pub fn new(
        call_index: u32,
        public_item: ResponseItemAuthority,
        argument_delta_count: u64,
    ) -> Result<Self, &'static str> {
        if argument_delta_count == 0 || argument_delta_count > 16_384 {
            return Err(WORKER_OUTPUT_INVALID);
        }
        Ok(Self {
            call_index,
            public_item,
            argument_delta_count,
        })
    }

    pub fn call_index(&self) -> u32 {
        self.call_index
    }

    pub fn public_item(&self) -> &ResponseItemAuthority {
        &self.public_item
    }

    pub fn argument_delta_count(&self) -> u64 {
        self.argument_delta_count
    }

    pub fn completed_seal_index(&self) -> Option<u64> {
        self.argument_delta_count.checked_add(2)
    }
}

/// One successful, durable tool result used to continue a model conversation.
///
/// The wire retains structured JSON. Provider adapters must use
/// [`ModelToolResult::canonical_content`] when constructing a `role=tool`
/// message so object ordering can never change the resumed transcript.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolResult {
    call_id: String,
    content: serde_json::Value,
}

impl fmt::Debug for ModelToolResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelToolResult")
            .field("content_present", &true)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelToolResultWire {
    call_id: String,
    content: serde_json::Value,
}

impl ModelToolResult {
    pub fn new(
        call_id: impl Into<String>,
        content: serde_json::Value,
    ) -> Result<Self, &'static str> {
        let call_id = call_id.into();
        if !valid_model_call_id(&call_id)
            || !valid_bounded_model_json(&content, false, MAX_MODEL_TOOL_RESULT_JSON_BYTES)
        {
            return Err(WORKER_EXECUTION_CONTEXT_INVALID);
        }
        Ok(Self { call_id, content })
    }

    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    pub fn content(&self) -> &serde_json::Value {
        &self.content
    }

    pub fn canonical_content(&self) -> String {
        // Construction and deserialization both prove JCS serialization.
        // Keeping the canonical string transient avoids duplicating tool
        // payloads in the durable worker envelope.
        serde_jcs::to_string(&self.content)
            .expect("validated model tool result remains JCS serializable")
    }
}

impl<'de> Deserialize<'de> for ModelToolResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelToolResultWire::deserialize(deserializer)?;
        Self::new(wire.call_id, wire.content)
            .map_err(|_| D::Error::custom(WORKER_EXECUTION_CONTEXT_INVALID))
    }
}

/// One fully settled model/tool round. Calls and successful results are both
/// canonical arrays: result `n` must answer call `n`, including the same
/// `call_id`. A scheduler may therefore reconstruct the provider transcript
/// without map iteration or provider-specific state.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelContinuationTurn {
    model_call_no: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    assistant_content: Option<String>,
    calls: Vec<ModelToolCall>,
    tool_results: Vec<ModelToolResult>,
}

impl fmt::Debug for ModelContinuationTurn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelContinuationTurn")
            .field("model_call_no", &self.model_call_no)
            .field(
                "assistant_content_present",
                &self.assistant_content.is_some(),
            )
            .field("call_count", &self.calls.len())
            .field("tool_result_count", &self.tool_results.len())
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelContinuationTurnWire {
    model_call_no: u32,
    #[serde(default)]
    assistant_content: Option<String>,
    calls: Vec<ModelToolCall>,
    tool_results: Vec<ModelToolResult>,
}

impl ModelContinuationTurn {
    pub fn new(
        model_call_no: u32,
        assistant_content: Option<String>,
        calls: Vec<ModelToolCall>,
        tool_results: Vec<ModelToolResult>,
    ) -> Result<Self, &'static str> {
        let mut call_ids = BTreeMap::new();
        let valid = model_call_no > 0
            && assistant_content.as_ref().is_none_or(|content| {
                !content.trim().is_empty() && content.len() <= MAX_MODEL_ASSISTANT_CONTENT_BYTES
            })
            && !calls.is_empty()
            && calls.len() == tool_results.len()
            && calls.len() <= MAX_MODEL_CONTINUATION_CALLS
            && calls
                .iter()
                .zip(&tool_results)
                .enumerate()
                .all(|(index, (call, result))| {
                    call.index == u32::try_from(index).unwrap_or(u32::MAX)
                        && call.is_valid()
                        && call_ids.insert(call.call_id.as_str(), ()).is_none()
                        && result.call_id == call.call_id
                        && valid_bounded_model_json(
                            &result.content,
                            false,
                            MAX_MODEL_TOOL_RESULT_JSON_BYTES,
                        )
                });
        if !valid {
            return Err(WORKER_EXECUTION_CONTEXT_INVALID);
        }
        let turn = Self {
            model_call_no,
            assistant_content,
            calls,
            tool_results,
        };
        if !serde_jcs::to_vec(&turn).is_ok_and(|bytes| bytes.len() <= MAX_MODEL_CONTINUATION_BYTES)
        {
            return Err(WORKER_EXECUTION_CONTEXT_INVALID);
        }
        Ok(turn)
    }

    pub fn model_call_no(&self) -> u32 {
        self.model_call_no
    }

    pub fn assistant_content(&self) -> Option<&str> {
        self.assistant_content.as_deref()
    }

    pub fn calls(&self) -> &[ModelToolCall] {
        &self.calls
    }

    pub fn tool_results(&self) -> &[ModelToolResult] {
        &self.tool_results
    }
}

impl<'de> Deserialize<'de> for ModelContinuationTurn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelContinuationTurnWire::deserialize(deserializer)?;
        Self::new(
            wire.model_call_no,
            wire.assistant_content,
            wire.calls,
            wire.tool_results,
        )
        .map_err(|_| D::Error::custom(WORKER_EXECUTION_CONTEXT_INVALID))
    }
}

/// Repository-minted authority that must accompany the immutable request all
/// the way into the exact worker/provider implementation.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerExecutionContext {
    attempt_no: AttemptNo,
    lease_epoch: LeaseEpoch,
    fencing_token: String,
    deadline: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_call: Option<ModelCallAuthority>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    continuation_turns: Vec<ModelContinuationTurn>,
}

impl fmt::Debug for WorkerExecutionContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerExecutionContext")
            .field("attempt_no", &self.attempt_no)
            .field("lease_epoch", &self.lease_epoch)
            .field("deadline", &self.deadline)
            .field("model_call_present", &self.model_call.is_some())
            .field("continuation_turn_count", &self.continuation_turns.len())
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerExecutionContextWire {
    attempt_no: AttemptNo,
    lease_epoch: LeaseEpoch,
    fencing_token: String,
    deadline: DateTime<Utc>,
    #[serde(default)]
    model_call: Option<ModelCallAuthority>,
    #[serde(default)]
    continuation_turns: Vec<ModelContinuationTurn>,
}

impl WorkerExecutionContext {
    pub fn new(
        attempt_no: AttemptNo,
        lease_epoch: LeaseEpoch,
        fencing_token: impl Into<String>,
        deadline: DateTime<Utc>,
    ) -> Result<Self, &'static str> {
        let fencing_token = fencing_token.into();
        if lease_epoch.get() < u64::from(attempt_no.get())
            || fencing_token.is_empty()
            || fencing_token.len() > 256
            || fencing_token.chars().any(char::is_control)
        {
            return Err(WORKER_EXECUTION_CONTEXT_INVALID);
        }
        Ok(Self {
            attempt_no,
            lease_epoch,
            fencing_token,
            deadline,
            model_call: None,
            continuation_turns: Vec::new(),
        })
    }

    pub fn with_model_call(mut self, model_call: ModelCallAuthority) -> Self {
        self.model_call = Some(model_call);
        self
    }

    pub fn with_model_continuation(
        mut self,
        continuation_turns: Vec<ModelContinuationTurn>,
    ) -> Result<Self, &'static str> {
        validate_model_continuation(self.model_call.as_ref(), &continuation_turns)?;
        self.continuation_turns = continuation_turns;
        Ok(self)
    }

    pub fn attempt_no(&self) -> AttemptNo {
        self.attempt_no
    }

    pub fn lease_epoch(&self) -> LeaseEpoch {
        self.lease_epoch
    }

    pub fn fencing_token(&self) -> &str {
        &self.fencing_token
    }

    pub fn deadline(&self) -> DateTime<Utc> {
        self.deadline
    }

    pub fn model_call(&self) -> Option<&ModelCallAuthority> {
        self.model_call.as_ref()
    }

    pub fn continuation_turns(&self) -> &[ModelContinuationTurn] {
        &self.continuation_turns
    }

    pub fn validate_model_continuation(&self) -> Result<(), &'static str> {
        validate_model_continuation(self.model_call.as_ref(), &self.continuation_turns)
    }
}

impl<'de> Deserialize<'de> for WorkerExecutionContext {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = WorkerExecutionContextWire::deserialize(deserializer)?;
        let mut context = Self::new(
            wire.attempt_no,
            wire.lease_epoch,
            wire.fencing_token,
            wire.deadline,
        )
        .map_err(D::Error::custom)?;
        context.model_call = wire.model_call;
        context
            .with_model_continuation(wire.continuation_turns)
            .map_err(D::Error::custom)
    }
}

fn validate_model_continuation(
    model_call: Option<&ModelCallAuthority>,
    turns: &[ModelContinuationTurn],
) -> Result<(), &'static str> {
    if turns.is_empty() {
        return Ok(());
    }
    let call_count = turns
        .iter()
        .try_fold(0usize, |total, turn| total.checked_add(turn.calls.len()))
        .filter(|total| *total <= MAX_MODEL_CONTINUATION_CALLS)
        .ok_or(WORKER_EXECUTION_CONTEXT_INVALID)?;
    let mut call_ids = BTreeMap::new();
    if call_count == 0
        || turns.len() > MAX_MODEL_CONTINUATION_TURNS
        || turns
            .iter()
            .enumerate()
            .any(|(index, turn)| turn.model_call_no != u32::try_from(index + 1).unwrap_or(u32::MAX))
        || turns
            .iter()
            .flat_map(|turn| &turn.calls)
            .any(|call| call_ids.insert(call.call_id.as_str(), ()).is_some())
        || !serde_jcs::to_vec(turns).is_ok_and(|bytes| bytes.len() <= MAX_MODEL_CONTINUATION_BYTES)
    {
        return Err(WORKER_EXECUTION_CONTEXT_INVALID);
    }
    let next_model_call_no = u32::try_from(turns.len())
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or(WORKER_EXECUTION_CONTEXT_INVALID)?;
    if model_call.is_none_or(|authority| authority.model_call_no() != next_model_call_no) {
        return Err(WORKER_EXECUTION_CONTEXT_INVALID);
    }
    Ok(())
}

/// Body-free internal failure. Provider bodies, prompts, secrets and arbitrary
/// user values cannot be carried across this boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerFailure {
    class: WorkerFailureClass,
    code: String,
    retryable: bool,
    safe_error: Option<Box<SafeError>>,
    model_call: Option<Box<ModelCallCompletion>>,
}

impl WorkerFailure {
    pub fn new(
        class: WorkerFailureClass,
        code: impl Into<String>,
        retryable: bool,
    ) -> Result<Self, &'static str> {
        Self::build(class, code.into(), retryable, None)
    }

    pub fn safe_business(
        code: impl Into<String>,
        retryable: bool,
        safe_error: RuntimeValue,
    ) -> Result<Self, &'static str> {
        let code = code.into();
        let safe_error = SafeError::try_from(safe_error).map_err(|_| WORKER_FAILURE_INVALID)?;
        if code != safe_error.code() {
            return Err(WORKER_FAILURE_INVALID);
        }
        Self::build(
            WorkerFailureClass::SafeBusinessFailure,
            code,
            retryable,
            Some(Box::new(safe_error)),
        )
    }

    fn build(
        class: WorkerFailureClass,
        code: String,
        retryable: bool,
        safe_error: Option<Box<SafeError>>,
    ) -> Result<Self, &'static str> {
        if code.is_empty()
            || code.len() > MAX_FAILURE_CODE_BYTES
            || !code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            || code.as_bytes()[0].is_ascii_digit()
        {
            return Err(WORKER_FAILURE_INVALID);
        }
        if matches!(
            class,
            WorkerFailureClass::ControlTermination | WorkerFailureClass::InvariantCorruption
        ) && retryable
        {
            return Err(WORKER_FAILURE_INVALID);
        }
        if (class == WorkerFailureClass::SafeBusinessFailure) != safe_error.is_some() {
            return Err(WORKER_FAILURE_INVALID);
        }
        Ok(Self {
            class,
            code,
            retryable,
            safe_error,
            model_call: None,
        })
    }

    /// Attaches body-free, fence-bound telemetry from a provider call that
    /// finished with a non-success model reason. The scheduler checkpoints
    /// it before committing the failed Attempt; it is never used to decide
    /// the workflow outcome.
    pub fn with_model_call(mut self, model_call: ModelCallCompletion) -> Self {
        self.model_call = Some(Box::new(model_call));
        self
    }

    pub fn class(&self) -> WorkerFailureClass {
        self.class
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn retryable(&self) -> bool {
        self.retryable
    }

    pub fn safe_error(&self) -> Option<&RuntimeValue> {
        self.safe_error.as_deref().map(SafeError::runtime_value)
    }

    pub(crate) fn typed_safe_error(&self) -> Option<&SafeError> {
        self.safe_error.as_deref()
    }

    pub fn model_call(&self) -> Option<&ModelCallCompletion> {
        self.model_call.as_deref()
    }

    /// Removes transport telemetry after it has been checkpointed through its
    /// dedicated fenced repository authority. Scheduler success facts retain
    /// only business outputs and effect evidence.
    pub fn without_model_call(mut self) -> Self {
        self.model_call = None;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ExecutorKey {
    task_kind: SchedulerTaskKind,
    implementation: String,
    descriptor_version: VersionTag,
    worker_version: VersionTag,
}

/// Immutable worker request extracted from a scheduler-owned DispatchTask
/// intent. `effect_id` is the stable provider idempotency key.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskExecutionOrigin {
    Workflow,
    ModelTool {
        parent_activation_id: ActivationId,
        model_call_no: u32,
        call_index: u32,
        tool_task_id: SchedulerTaskId,
    },
}

/// Minimal engine-owned projection of frozen Action authority needed to build
/// a worker request. Durable lease and fencing fields deliberately have no
/// representation here.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelToolActionExecutionSpec {
    action_id: String,
    action_version: String,
    descriptor_hash: String,
    input_schema: serde_json::Value,
    effect_policy: WorkerEffectPolicy,
    deployment_binding: serde_json::Value,
}

impl ModelToolActionExecutionSpec {
    pub fn new(
        action_id: impl Into<String>,
        action_version: impl Into<String>,
        descriptor_hash: impl Into<String>,
        input_schema: serde_json::Value,
        effect_policy: WorkerEffectPolicy,
        deployment_binding: serde_json::Value,
    ) -> Self {
        Self {
            action_id: action_id.into(),
            action_version: action_version.into(),
            descriptor_hash: descriptor_hash.into(),
            input_schema,
            effect_policy,
            deployment_binding,
        }
    }

    pub fn action_id(&self) -> &str {
        &self.action_id
    }

    pub fn action_version(&self) -> &str {
        &self.action_version
    }

    pub fn descriptor_hash(&self) -> &str {
        &self.descriptor_hash
    }

    pub fn input_schema(&self) -> &serde_json::Value {
        &self.input_schema
    }

    pub fn effect_policy(&self) -> &WorkerEffectPolicy {
        &self.effect_policy
    }

    pub fn deployment_binding(&self) -> &serde_json::Value {
        &self.deployment_binding
    }
}

/// Engine-owned business projection of one durable model-tool claim.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelToolExecutionSpec {
    run_id: RunId,
    parent_activation_id: ActivationId,
    model_call_no: u32,
    tool_task_id: SchedulerTaskId,
    effect_id: EffectId,
    call_index: u32,
    action: ModelToolActionExecutionSpec,
    arguments: serde_json::Value,
}

impl ModelToolExecutionSpec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_id: RunId,
        parent_activation_id: ActivationId,
        model_call_no: u32,
        tool_task_id: SchedulerTaskId,
        effect_id: EffectId,
        call_index: u32,
        action: ModelToolActionExecutionSpec,
        arguments: serde_json::Value,
    ) -> Result<Self, &'static str> {
        if model_call_no == 0 {
            return Err(WORKER_MODEL_TOOL_CLAIM_INVALID);
        }
        Ok(Self {
            run_id,
            parent_activation_id,
            model_call_no,
            tool_task_id,
            effect_id,
            call_index,
            action,
            arguments,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn parent_activation_id(&self) -> &ActivationId {
        &self.parent_activation_id
    }

    pub fn model_call_no(&self) -> u32 {
        self.model_call_no
    }

    pub fn tool_task_id(&self) -> &SchedulerTaskId {
        &self.tool_task_id
    }

    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }

    pub fn call_index(&self) -> u32 {
        self.call_index
    }

    pub fn action(&self) -> &ModelToolActionExecutionSpec {
        &self.action
    }

    pub fn arguments(&self) -> &serde_json::Value {
        &self.arguments
    }
}

/// Compatibility projection implemented by durable claim owners. Implementors
/// return only immutable business authority, never lease or fencing state.
#[doc(hidden)]
pub trait ModelToolTaskClaimView {
    fn model_tool_execution_spec(&self) -> Result<ModelToolExecutionSpec, &'static str>;
}

impl ModelToolTaskClaimView for ModelToolExecutionSpec {
    fn model_tool_execution_spec(&self) -> Result<ModelToolExecutionSpec, &'static str> {
        Ok(self.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskExecutionRequest {
    task_id: SchedulerTaskId,
    run_id: RunId,
    activation_id: ActivationId,
    node_id: NodeId,
    effect_id: EffectId,
    origin: TaskExecutionOrigin,
    admission_class: TaskAdmissionClass,
    task_kind: SchedulerTaskKind,
    implementation: String,
    descriptor_version: VersionTag,
    worker_version: VersionTag,
    effect_policy: WorkerEffectPolicy,
    deployment_binding: serde_json::Value,
    public_configuration: BTreeMap<String, DescriptorValue>,
    secret_configuration: BTreeMap<String, SecretRef>,
    inputs: Vec<BoundTaskInput>,
    outputs: Vec<TaskOutputContract>,
}

impl TaskExecutionRequest {
    pub fn from_scheduler_intent(intent: &SchedulerIntent) -> Result<Self, &'static str> {
        let action = intent.action();
        let SchedulerAction::DispatchTask {
            task_id,
            effect_id,
            admission_class,
            activation_id,
            node_id,
            task_kind,
            implementation,
            descriptor_version,
            worker_version,
            effect_policy,
            deployment_binding,
            public_configuration,
            secret_configuration,
            inputs,
            outputs,
        } = action
        else {
            return Err(WORKER_TASK_KIND_MISMATCH);
        };
        Ok(Self {
            task_id: task_id.clone(),
            run_id: intent.run_id().clone(),
            activation_id: activation_id.clone(),
            node_id: node_id.clone(),
            effect_id: effect_id.clone(),
            origin: TaskExecutionOrigin::Workflow,
            admission_class: *admission_class,
            task_kind: *task_kind,
            implementation: implementation.clone(),
            descriptor_version: descriptor_version.clone(),
            worker_version: worker_version.clone(),
            effect_policy: effect_policy.clone(),
            deployment_binding: deployment_binding.clone(),
            public_configuration: public_configuration.clone(),
            secret_configuration: secret_configuration.clone(),
            inputs: inputs.clone(),
            outputs: outputs.clone(),
        })
    }

    /// Projects one independently leased durable model-tool claim into the
    /// existing Action worker boundary. Every identity and policy field is
    /// copied from frozen claim authority; this path never consults a mutable
    /// Action catalog or reconstructs model-side state.
    pub fn from_model_tool_claim<C: ModelToolTaskClaimView + ?Sized>(
        claim: &C,
    ) -> Result<Self, &'static str> {
        let spec = claim.model_tool_execution_spec()?;
        let action = spec.action();
        validate_model_tool_action_binding(action)?;
        if !spec.arguments().is_object()
            || serde_jcs::to_vec(spec.arguments()).is_err()
            || !crate::schema::compile_schema_2020(action.input_schema())
                .map_err(|_| WORKER_MODEL_TOOL_CLAIM_INVALID)?
                .is_valid(spec.arguments())
        {
            return Err(WORKER_MODEL_TOOL_CLAIM_INVALID);
        }
        // This enforces the canonical Plan JSON domain, including the
        // interoperable safe-integer range, before values enter descriptor
        // configuration.
        PlanType::literal(spec.arguments().clone()).map_err(|_| WORKER_MODEL_TOOL_CLAIM_INVALID)?;
        let arguments = descriptor_value_from_model_tool_json(spec.arguments())?;
        let node_id = model_tool_node_id(spec.tool_task_id())?;
        let descriptor_version =
            VersionTag::new("1").map_err(|_| WORKER_MODEL_TOOL_CLAIM_INVALID)?;
        let worker_version = VersionTag::new(action.action_version())
            .map_err(|_| WORKER_MODEL_TOOL_CLAIM_INVALID)?;
        Ok(Self {
            task_id: spec.tool_task_id().clone(),
            run_id: spec.run_id().clone(),
            activation_id: spec.parent_activation_id().clone(),
            node_id,
            effect_id: spec.effect_id().clone(),
            origin: TaskExecutionOrigin::ModelTool {
                parent_activation_id: spec.parent_activation_id().clone(),
                model_call_no: spec.model_call_no(),
                call_index: spec.call_index(),
                tool_task_id: spec.tool_task_id().clone(),
            },
            admission_class: TaskAdmissionClass::Normal,
            task_kind: SchedulerTaskKind::Action,
            implementation: action.action_id().to_owned(),
            descriptor_version,
            worker_version,
            effect_policy: action.effect_policy().clone(),
            deployment_binding: action.deployment_binding().clone(),
            public_configuration: BTreeMap::from([("inputs".to_owned(), arguments)]),
            secret_configuration: BTreeMap::new(),
            inputs: Vec::new(),
            outputs: vec![crate::internal::task_output_contract(
                DataPortId::new("model_tool_result")
                    .map_err(|_| WORKER_MODEL_TOOL_CLAIM_INVALID)?,
                PortName::new("result").map_err(|_| WORKER_MODEL_TOOL_CLAIM_INVALID)?,
                PlanType::Any,
                true,
            )],
        })
    }

    pub(crate) fn is_model_tool_action_request(&self) -> bool {
        matches!(
            &self.origin,
            TaskExecutionOrigin::ModelTool {
                parent_activation_id,
                model_call_no,
                call_index: _,
                tool_task_id,
            } if parent_activation_id == &self.activation_id
                && *model_call_no > 0
                && tool_task_id == &self.task_id
        ) && self.task_kind == SchedulerTaskKind::Action
            && model_tool_node_id(&self.task_id).as_ref() == Ok(&self.node_id)
            && self.admission_class == TaskAdmissionClass::Normal
            && self
                .public_configuration
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>()
                == ["inputs"]
            && self.secret_configuration.is_empty()
            && self.inputs.is_empty()
            && self.outputs.len() == 1
            && self.outputs[0].port_id().as_str() == "model_tool_result"
            && self.outputs[0].name().as_str() == "result"
            && self.outputs[0].value_type() == &PlanType::Any
            && self.outputs[0].required()
    }

    fn executor_key(&self) -> ExecutorKey {
        ExecutorKey {
            task_kind: self.task_kind,
            implementation: self.implementation.clone(),
            descriptor_version: self.descriptor_version.clone(),
            worker_version: self.worker_version.clone(),
        }
    }

    pub fn task_id(&self) -> &SchedulerTaskId {
        &self.task_id
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }
    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }
    pub fn origin(&self) -> &TaskExecutionOrigin {
        &self.origin
    }
    pub fn admission_class(&self) -> TaskAdmissionClass {
        self.admission_class
    }
    pub fn task_kind(&self) -> SchedulerTaskKind {
        self.task_kind
    }
    pub fn implementation(&self) -> &str {
        &self.implementation
    }
    pub fn descriptor_version(&self) -> &VersionTag {
        &self.descriptor_version
    }
    pub fn worker_version(&self) -> &VersionTag {
        &self.worker_version
    }
    pub fn effect_policy(&self) -> &WorkerEffectPolicy {
        &self.effect_policy
    }
    pub fn deployment_binding(&self) -> &serde_json::Value {
        &self.deployment_binding
    }
    pub fn public_configuration(&self) -> &BTreeMap<String, DescriptorValue> {
        &self.public_configuration
    }
    pub fn secret_configuration(&self) -> &BTreeMap<String, SecretRef> {
        &self.secret_configuration
    }
    pub fn inputs(&self) -> &[BoundTaskInput] {
        &self.inputs
    }
    pub fn outputs(&self) -> &[TaskOutputContract] {
        &self.outputs
    }
}

fn model_tool_node_id(task_id: &SchedulerTaskId) -> Result<NodeId, &'static str> {
    let hash = task_id
        .as_str()
        .strip_prefix("task_")
        .ok_or(WORKER_MODEL_TOOL_CLAIM_INVALID)?;
    NodeId::new(format!("model_tool_{hash}")).map_err(|_| WORKER_MODEL_TOOL_CLAIM_INVALID)
}

fn validate_model_tool_action_binding(
    action: &ModelToolActionExecutionSpec,
) -> Result<(), &'static str> {
    let binding = action
        .deployment_binding()
        .as_object()
        .ok_or(WORKER_MODEL_TOOL_CLAIM_INVALID)?;
    let expected_keys = BTreeSet::from([
        "action_id",
        "action_version",
        "adapter",
        "cancellation",
        "descriptor_hash",
        "effect",
        "idempotency",
        "public",
        "required_capabilities",
    ]);
    if binding.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected_keys
        || binding.get("adapter").and_then(serde_json::Value::as_str) != Some("native_action")
        || binding.get("action_id").and_then(serde_json::Value::as_str) != Some(action.action_id())
        || binding
            .get("action_version")
            .and_then(serde_json::Value::as_str)
            != Some(action.action_version())
        || binding
            .get("descriptor_hash")
            .and_then(serde_json::Value::as_str)
            != Some(action.descriptor_hash())
        || !binding
            .get("effect")
            .is_some_and(serde_json::Value::is_string)
        || !binding
            .get("idempotency")
            .is_some_and(serde_json::Value::is_string)
        || !binding
            .get("cancellation")
            .is_some_and(serde_json::Value::is_string)
        || !binding
            .get("required_capabilities")
            .is_some_and(serde_json::Value::is_array)
        || !binding
            .get("public")
            .is_some_and(serde_json::Value::is_object)
    {
        return Err(WORKER_MODEL_TOOL_CLAIM_INVALID);
    }
    Ok(())
}

fn descriptor_value_from_model_tool_json(
    value: &serde_json::Value,
) -> Result<DescriptorValue, &'static str> {
    Ok(match value {
        serde_json::Value::Null => DescriptorValue::Null,
        serde_json::Value::Bool(value) => DescriptorValue::Boolean(*value),
        serde_json::Value::Number(value) => match value.as_i64() {
            Some(value) => DescriptorValue::Integer(value),
            None => DescriptorValue::Number(value.clone()),
        },
        serde_json::Value::String(value) => DescriptorValue::String(value.clone()),
        serde_json::Value::Array(values) => DescriptorValue::Array(
            values
                .iter()
                .map(descriptor_value_from_model_tool_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        serde_json::Value::Object(values) => DescriptorValue::Object(
            values
                .iter()
                .map(|(name, value)| {
                    Ok((name.clone(), descriptor_value_from_model_tool_json(value)?))
                })
                .collect::<Result<BTreeMap<_, _>, &'static str>>()?,
        ),
    })
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskExecutionResult {
    outputs: BTreeMap<DataPortId, RuntimeValue>,
    effect_evidence: EffectEvidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_call: Option<ModelCallCompletion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_tool_call_batch: Option<ModelToolCallBatch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retrieval: Option<RetrievalCompletion>,
}

impl TaskExecutionResult {
    pub fn new(
        outputs: BTreeMap<DataPortId, RuntimeValue>,
        effect_evidence: EffectEvidence,
    ) -> Self {
        Self {
            outputs,
            effect_evidence,
            model_call: None,
            model_tool_call_batch: None,
            retrieval: None,
        }
    }

    pub fn with_model_call(mut self, model_call: ModelCallCompletion) -> Self {
        self.model_call = Some(model_call);
        self
    }

    pub fn with_model_tool_call_batch(mut self, batch: ModelToolCallBatch) -> Self {
        self.model_tool_call_batch = Some(batch);
        self
    }

    pub fn with_retrieval_completion(mut self, completion: RetrievalCompletion) -> Self {
        self.retrieval = Some(completion);
        self
    }

    pub fn outputs(&self) -> &BTreeMap<DataPortId, RuntimeValue> {
        &self.outputs
    }

    pub fn effect_evidence(&self) -> EffectEvidence {
        self.effect_evidence
    }

    pub fn model_call(&self) -> Option<&ModelCallCompletion> {
        self.model_call.as_ref()
    }

    pub fn model_tool_call_batch(&self) -> Option<&ModelToolCallBatch> {
        self.model_tool_call_batch.as_ref()
    }

    pub fn retrieval_completion(&self) -> Option<&RetrievalCompletion> {
        self.retrieval.as_ref()
    }

    /// Removes model-call telemetry after its dedicated fenced checkpoint;
    /// the durable scheduler success fact then contains only business output.
    pub fn without_model_call(mut self) -> Self {
        self.model_call = None;
        self
    }
}

#[async_trait]
pub trait LeafTaskExecutor: Send + Sync {
    fn live_response_capable(&self) -> bool {
        false
    }

    async fn execute(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure>;

    async fn execute_with_runtime_services(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        services: &WorkerRuntimeServices,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        let _ = services;
        self.execute(context, request, cancellation).await
    }
}

#[derive(Default)]
pub struct WorkerExecutorRegistry {
    executors: BTreeMap<ExecutorKey, Arc<dyn LeafTaskExecutor>>,
}

impl WorkerExecutorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn supports_public_llm_response(&self) -> bool {
        let mut found = false;
        for (key, executor) in &self.executors {
            if key.task_kind == SchedulerTaskKind::Llm && key.implementation == "core.llm" {
                found = true;
                if !executor.live_response_capable() {
                    return false;
                }
            }
        }
        found
    }

    pub fn register(
        &mut self,
        task_kind: SchedulerTaskKind,
        implementation: impl Into<String>,
        descriptor_version: VersionTag,
        worker_version: VersionTag,
        executor: Arc<dyn LeafTaskExecutor>,
    ) -> Result<(), &'static str> {
        let implementation = implementation.into();
        if implementation.is_empty()
            || implementation
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
        {
            return Err(WORKER_IMPLEMENTATION_NOT_FOUND);
        }
        let key = ExecutorKey {
            task_kind,
            implementation,
            descriptor_version,
            worker_version,
        };
        match self.executors.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(executor);
            }
            Entry::Occupied(_) => return Err(WORKER_IMPLEMENTATION_NOT_FOUND),
        }
        Ok(())
    }

    /// Read-only startup capability check for an exact frozen deployment
    /// tuple. Recovery must not discover this by executing user work.
    pub fn contains(
        &self,
        task_kind: SchedulerTaskKind,
        implementation: &str,
        descriptor_version: &VersionTag,
        worker_version: &VersionTag,
    ) -> bool {
        self.executors.contains_key(&ExecutorKey {
            task_kind,
            implementation: implementation.to_owned(),
            descriptor_version: descriptor_version.clone(),
            worker_version: worker_version.clone(),
        })
    }

    pub async fn execute(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        let executor = self.executors.get(&request.executor_key()).ok_or_else(|| {
            WorkerFailure::new(
                WorkerFailureClass::InvariantCorruption,
                "WORKER_IMPLEMENTATION_NOT_FOUND",
                false,
            )
            .expect("constant failure is valid")
        })?;
        let result = executor.execute(context, request, cancellation).await?;
        validate_outputs(request.task_kind(), request.outputs(), &result)?;
        Ok(result)
    }

    pub(crate) async fn execute_with_runtime_services(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        services: &WorkerRuntimeServices,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        let executor = self.executors.get(&request.executor_key()).ok_or_else(|| {
            WorkerFailure::new(
                WorkerFailureClass::InvariantCorruption,
                "WORKER_IMPLEMENTATION_NOT_FOUND",
                false,
            )
            .expect("constant failure is valid")
        })?;
        let result = executor
            .execute_with_runtime_services(context, request, services, cancellation)
            .await?;
        validate_outputs(request.task_kind(), request.outputs(), &result)?;
        Ok(result)
    }
}

fn validate_outputs(
    task_kind: SchedulerTaskKind,
    contracts: &[TaskOutputContract],
    result: &TaskExecutionResult,
) -> Result<(), WorkerFailure> {
    let valid_retrieval_sidecar = match task_kind {
        SchedulerTaskKind::Retrieval => {
            result.retrieval_completion().is_some()
                && result.model_call().is_none()
                && result.model_tool_call_batch().is_none()
        }
        SchedulerTaskKind::Llm
        | SchedulerTaskKind::Action
        | SchedulerTaskKind::Http
        | SchedulerTaskKind::Tool => result.retrieval_completion().is_none(),
    };
    if !valid_retrieval_sidecar {
        return Err(WorkerFailure::new(
            WorkerFailureClass::InvariantCorruption,
            WORKER_OUTPUT_INVALID,
            false,
        )
        .expect("constant failure is valid"));
    }
    if let Some(batch) = result.model_tool_call_batch() {
        let valid_tool_disposition = result.effect_evidence == EffectEvidence::Committed
            && result.outputs.is_empty()
            && result.model_call().is_some_and(|completion| {
                completion.finish_reason() == ModelFinishReason::ToolCalls
                    && completion.model_call_no() == batch.model_call_no()
            });
        if valid_tool_disposition {
            return Ok(());
        }
        return Err(WorkerFailure::new(
            WorkerFailureClass::InvariantCorruption,
            WORKER_OUTPUT_INVALID,
            false,
        )
        .expect("constant failure is valid"));
    }
    if result
        .model_call()
        .is_some_and(|completion| completion.finish_reason() == ModelFinishReason::ToolCalls)
    {
        return Err(WorkerFailure::new(
            WorkerFailureClass::InvariantCorruption,
            WORKER_OUTPUT_INVALID,
            false,
        )
        .expect("constant failure is valid"));
    }
    let contracts_by_id = contracts
        .iter()
        .map(|contract| (contract.port_id(), contract))
        .collect::<BTreeMap<_, _>>();
    let invalid = result.effect_evidence != EffectEvidence::Committed
        || result.outputs.iter().any(|(port_id, value)| {
            contracts_by_id
                .get(port_id)
                .is_none_or(|contract| !value.matches(contract.value_type()))
        })
        || contracts.iter().any(|contract| {
            contract.required() && !result.outputs.contains_key(contract.port_id())
        });
    if invalid {
        return Err(WorkerFailure::new(
            WorkerFailureClass::InvariantCorruption,
            "WORKER_OUTPUT_INVALID",
            false,
        )
        .expect("constant failure is valid"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        plan::{PlanType, PortName},
        scheduler::TaskOutputContract,
        EffectIdempotency, WorkerCancellation, WorkerEffectClass,
    };
    use serde_json::json;

    struct FixedExecutor {
        output: TaskExecutionResult,
    }

    #[async_trait]
    impl LeafTaskExecutor for FixedExecutor {
        async fn execute(
            &self,
            _context: &WorkerExecutionContext,
            _request: &TaskExecutionRequest,
            _cancellation: CancellationToken,
        ) -> Result<TaskExecutionResult, WorkerFailure> {
            Ok(self.output.clone())
        }
    }

    fn output_contract() -> TaskOutputContract {
        crate::internal::task_output_contract(
            DataPortId::new("answer_port").unwrap(),
            PortName::new("answer").unwrap(),
            PlanType::String,
            true,
        )
    }

    fn model_tool_spec(arguments: serde_json::Value) -> ModelToolExecutionSpec {
        let effect_policy = WorkerEffectPolicy::frozen(
            WorkerEffectClass::ReadOnly,
            EffectIdempotency::Idempotent,
            3,
            10,
            100,
            30_000,
            WorkerCancellation::Cooperative,
        )
        .unwrap();
        let descriptor_hash = "b".repeat(64);
        let deployment_binding = json!({
            "adapter": "native_action",
            "action_id": "lookup",
            "action_version": "1.2.3",
            "descriptor_hash": descriptor_hash,
            "effect": "read_only",
            "idempotency": "idempotent",
            "cancellation": "cooperative",
            "required_capabilities": [],
            "public": {"call": false, "arguments": "private", "result": null},
        });
        let action = ModelToolActionExecutionSpec::new(
            "lookup",
            "1.2.3",
            descriptor_hash,
            json!({"type": "object"}),
            effect_policy,
            deployment_binding,
        );
        ModelToolExecutionSpec::new(
            RunId::new("run_model_tool").unwrap(),
            ActivationId::new("activation_model_parent").unwrap(),
            1,
            SchedulerTaskId::parse(format!("task_{}", "a".repeat(64))).unwrap(),
            EffectId::new("effect_model_tool").unwrap(),
            0,
            action,
            arguments,
        )
        .unwrap()
    }

    #[test]
    fn model_tool_claim_projects_one_stable_closed_action_request_without_stringifying_json() {
        let spec = model_tool_spec(json!({
            "query": "$literal-not-a-binding",
            "options": {"limit": 3, "flags": [true, null]},
        }));
        let first = TaskExecutionRequest::from_model_tool_claim(&spec).unwrap();
        let replay = TaskExecutionRequest::from_model_tool_claim(&spec).unwrap();
        assert_eq!(first, replay);
        assert_eq!(first.task_id(), spec.tool_task_id());
        assert_eq!(first.effect_id(), spec.effect_id());
        assert_eq!(first.run_id(), spec.run_id());
        assert_eq!(first.activation_id(), spec.parent_activation_id());
        assert_eq!(first.task_kind(), SchedulerTaskKind::Action);
        assert_eq!(first.admission_class(), TaskAdmissionClass::Normal);
        assert_eq!(first.implementation(), "lookup");
        assert_eq!(first.descriptor_version().as_str(), "1");
        assert_eq!(first.worker_version().as_str(), "1.2.3");
        assert_eq!(first.effect_policy(), spec.action().effect_policy());
        assert_eq!(
            first.deployment_binding(),
            spec.action().deployment_binding()
        );
        assert!(first.node_id().as_str().starts_with("model_tool_"));
        assert_eq!(
            first.node_id().as_str().strip_prefix("model_tool_"),
            first.task_id().as_str().strip_prefix("task_")
        );
        assert!(first.secret_configuration().is_empty());
        assert!(first.inputs().is_empty());
        assert_eq!(first.public_configuration().len(), 1);
        let DescriptorValue::Object(arguments) = &first.public_configuration()["inputs"] else {
            panic!("model tool arguments must remain one structured object");
        };
        assert_eq!(
            arguments["query"],
            DescriptorValue::String("$literal-not-a-binding".to_owned())
        );
        assert!(matches!(&arguments["options"], DescriptorValue::Object(_)));
        assert_eq!(first.outputs().len(), 1);
        assert_eq!(first.outputs()[0].name().as_str(), "result");
        assert_eq!(first.outputs()[0].value_type(), &PlanType::Any);
        assert!(adapter::is_model_tool_action_request(&first));

        let mut with_extra_field = first.clone();
        with_extra_field
            .public_configuration
            .insert("model_call_no".to_owned(), DescriptorValue::Integer(1));
        assert!(!adapter::is_model_tool_action_request(&with_extra_field));
    }

    #[test]
    fn model_tool_claim_rejects_values_outside_canonical_plan_json() {
        let spec = model_tool_spec(json!({"unsafe_integer": (1_u64 << 53) + 1}));
        assert_eq!(
            TaskExecutionRequest::from_model_tool_claim(&spec),
            Err(WORKER_MODEL_TOOL_CLAIM_INVALID)
        );
    }

    fn continuation_call(index: u32, call_id: &str) -> ModelToolCall {
        ModelToolCall::new(
            index,
            call_id,
            "lookup",
            json!({"query": format!("query-{call_id}")}),
        )
        .unwrap()
    }

    fn continuation_result(call_id: &str) -> ModelToolResult {
        ModelToolResult::new(call_id, json!({"z": 1, "a": call_id})).unwrap()
    }

    fn continuation_turn(model_call_no: u32, call_ids: &[&str]) -> ModelContinuationTurn {
        ModelContinuationTurn::new(
            model_call_no,
            Some(format!("round {model_call_no}")),
            call_ids
                .iter()
                .enumerate()
                .map(|(index, call_id)| continuation_call(u32::try_from(index).unwrap(), call_id))
                .collect(),
            call_ids
                .iter()
                .map(|call_id| continuation_result(call_id))
                .collect(),
        )
        .unwrap()
    }

    fn continuation_context(current_model_call_no: u32) -> WorkerExecutionContext {
        WorkerExecutionContext::new(
            AttemptNo::FIRST,
            LeaseEpoch::FIRST,
            "continuation-fence",
            Utc::now() + chrono::Duration::minutes(1),
        )
        .unwrap()
        .with_model_call(
            ModelCallAuthority::new("response_continuation", current_model_call_no, None).unwrap(),
        )
    }

    #[test]
    fn model_continuation_round_trip_is_closed_ordered_and_jcs_bounded() {
        let context = continuation_context(3)
            .with_model_continuation(vec![
                continuation_turn(1, &["call_1"]),
                continuation_turn(2, &["call_2", "call_3"]),
            ])
            .unwrap();
        assert_eq!(context.continuation_turns().len(), 2);
        assert_eq!(
            context.continuation_turns()[1].tool_results()[0].canonical_content(),
            r#"{"a":"call_2","z":1}"#
        );

        let wire = serde_json::to_value(&context).unwrap();
        assert_eq!(
            serde_json::from_value::<WorkerExecutionContext>(wire.clone()).unwrap(),
            context
        );

        let mut unknown_context = wire.clone();
        unknown_context["provider_state"] = json!("forbidden");
        assert!(serde_json::from_value::<WorkerExecutionContext>(unknown_context).is_err());

        let mut unknown_turn = wire;
        unknown_turn["continuation_turns"][0]["status"] = json!("completed");
        assert!(serde_json::from_value::<WorkerExecutionContext>(unknown_turn).is_err());

        assert_eq!(
            ModelToolResult::new(
                "call_large",
                json!("x".repeat(MAX_MODEL_TOOL_RESULT_JSON_BYTES)),
            )
            .unwrap_err(),
            WORKER_EXECUTION_CONTEXT_INVALID
        );
    }

    #[test]
    fn model_continuation_debug_is_body_free() {
        let call = continuation_call(0, "call_secret");
        let result =
            ModelToolResult::new("call_secret", json!({"private_result": "result secret"}))
                .unwrap();
        let turn = ModelContinuationTurn::new(
            1,
            Some("assistant secret".to_owned()),
            vec![call.clone()],
            vec![result.clone()],
        )
        .unwrap();
        let batch =
            ModelToolCallBatch::new(1, Some("assistant secret".to_owned()), vec![call.clone()])
                .unwrap();
        let context = continuation_context(2)
            .with_model_continuation(vec![turn.clone()])
            .unwrap();
        let rendered = format!("{call:?} {result:?} {turn:?} {batch:?} {context:?}");
        for forbidden in [
            "call_secret",
            "query-call_secret",
            "result secret",
            "assistant secret",
            "continuation-fence",
            "response_continuation",
        ] {
            assert!(!rendered.contains(forbidden), "debug leaked {forbidden}");
        }
    }

    #[test]
    fn model_continuation_rejects_round_gaps_and_mismatched_call_id_sets() {
        let invalid_turns = [
            vec![continuation_turn(2, &["call_1"])],
            vec![
                continuation_turn(1, &["call_1"]),
                continuation_turn(3, &["call_2"]),
            ],
            vec![
                continuation_turn(1, &["call_1"]),
                continuation_turn(2, &["call_1"]),
            ],
        ];
        for turns in invalid_turns {
            assert_eq!(
                continuation_context(3)
                    .with_model_continuation(turns)
                    .unwrap_err(),
                WORKER_EXECUTION_CONTEXT_INVALID
            );
        }
        assert_eq!(
            continuation_context(2)
                .with_model_continuation(vec![continuation_turn(1, &["call_1"]),])
                .unwrap()
                .continuation_turns()
                .len(),
            1
        );
        assert_eq!(
            continuation_context(4)
                .with_model_continuation(vec![continuation_turn(1, &["call_1"]),])
                .unwrap_err(),
            WORKER_EXECUTION_CONTEXT_INVALID
        );

        let calls = vec![
            continuation_call(0, "call_1"),
            continuation_call(1, "call_2"),
        ];
        let cases = [
            vec![continuation_result("call_1")],
            vec![continuation_result("call_2"), continuation_result("call_1")],
            vec![
                continuation_result("call_1"),
                continuation_result("call_unknown"),
            ],
        ];
        for results in cases {
            assert_eq!(
                ModelContinuationTurn::new(1, None, calls.clone(), results).unwrap_err(),
                WORKER_EXECUTION_CONTEXT_INVALID
            );
        }

        let duplicate_calls = vec![
            continuation_call(0, "call_1"),
            continuation_call(1, "call_1"),
        ];
        let duplicate_results = vec![continuation_result("call_1"), continuation_result("call_1")];
        assert_eq!(
            ModelContinuationTurn::new(1, None, duplicate_calls, duplicate_results).unwrap_err(),
            WORKER_EXECUTION_CONTEXT_INVALID
        );
    }

    #[test]
    fn closed_failure_taxonomy_defers_unknown_retry_to_frozen_effect_policy() {
        for class in [
            WorkerFailureClass::ControlTermination,
            WorkerFailureClass::InvariantCorruption,
        ] {
            assert_eq!(
                WorkerFailure::new(class, "SAFE_CODE", true),
                Err(WORKER_FAILURE_INVALID)
            );
        }
        assert!(WorkerFailure::new(
            WorkerFailureClass::EffectOutcomeUnknown,
            "EFFECT_STATUS_UNKNOWN",
            true,
        )
        .is_ok());
        assert!(WorkerFailure::new(
            WorkerFailureClass::InfrastructureFailure,
            "PROVIDER_UNAVAILABLE",
            true,
        )
        .is_ok());
    }

    #[test]
    fn safe_business_failure_validates_payload_and_has_one_code_authority() {
        let safe_error = RuntimeValue::new(json!({
            "kind": "safe_error",
            "code": "RISK_REJECTED",
            "message": "risk policy rejected the request"
        }))
        .unwrap();
        assert!(WorkerFailure::safe_business("RISK_REJECTED", false, safe_error.clone(),).is_ok());
        assert_eq!(
            WorkerFailure::safe_business("DIFFERENT_CODE", false, safe_error),
            Err(WORKER_FAILURE_INVALID),
        );
        assert_eq!(
            WorkerFailure::safe_business(
                "RISK_REJECTED",
                false,
                RuntimeValue::new(json!({
                    "kind": "safe_error",
                    "code": "RISK_REJECTED",
                    "message": "rejected",
                    "provider_body": "must not be exposed"
                }))
                .unwrap(),
            ),
            Err(WORKER_FAILURE_INVALID),
        );
    }

    #[tokio::test]
    async fn executor_registry_is_version_exact_and_validates_required_typed_outputs() {
        let contract = output_contract();
        let request = TaskExecutionRequest {
            task_id: SchedulerTaskId::parse(format!("task_{}", "1".repeat(64))).unwrap(),
            run_id: RunId::new("run_worker_test").unwrap(),
            activation_id: ActivationId::new("activation_worker_test").unwrap(),
            node_id: NodeId::new("node_worker_test").unwrap(),
            effect_id: EffectId::new("effect_worker_test").unwrap(),
            origin: TaskExecutionOrigin::Workflow,
            admission_class: TaskAdmissionClass::Normal,
            task_kind: SchedulerTaskKind::Action,
            implementation: "test.action".to_owned(),
            descriptor_version: VersionTag::new("descriptor-1").unwrap(),
            worker_version: VersionTag::new("worker-1").unwrap(),
            effect_policy: WorkerEffectPolicy::new(
                EffectIdempotency::Idempotent,
                1,
                WorkerCancellation::Cooperative,
            )
            .unwrap(),
            deployment_binding: json!({}),
            public_configuration: BTreeMap::new(),
            secret_configuration: BTreeMap::new(),
            inputs: vec![],
            outputs: vec![contract.clone()],
        };
        let output_id = contract.port_id().clone();
        let context = WorkerExecutionContext::new(
            AttemptNo::FIRST,
            LeaseEpoch::FIRST,
            "worker-test-fence",
            Utc::now() + chrono::Duration::minutes(1),
        )
        .unwrap();
        let mut registry = WorkerExecutorRegistry::new();
        registry
            .register(
                SchedulerTaskKind::Action,
                "test.action",
                VersionTag::new("descriptor-1").unwrap(),
                VersionTag::new("worker-1").unwrap(),
                Arc::new(FixedExecutor {
                    output: TaskExecutionResult::new(
                        BTreeMap::from([(
                            output_id.clone(),
                            RuntimeValue::new(json!("ok")).unwrap(),
                        )]),
                        EffectEvidence::Committed,
                    ),
                }),
            )
            .unwrap();
        assert!(registry
            .execute(&context, &request, CancellationToken::new())
            .await
            .is_ok());

        let missing = WorkerExecutorRegistry::new()
            .execute(&context, &request, CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(missing.code(), "WORKER_IMPLEMENTATION_NOT_FOUND");

        let invalid = validate_outputs(
            SchedulerTaskKind::Action,
            &[contract],
            &TaskExecutionResult::new(
                BTreeMap::from([(output_id, RuntimeValue::new(json!(7)).unwrap())]),
                EffectEvidence::Committed,
            ),
        )
        .unwrap_err();
        assert_eq!(invalid.code(), "WORKER_OUTPUT_INVALID");
    }
}
