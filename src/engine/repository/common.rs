use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::engine::worker::{ModelCallCompletion, ModelFinishReason};
use crate::engine::{
    ActivationId, AdmissionState, AttemptNo, ContentHash, EventSeq, ExecutionEventEnvelope,
    ExecutionEventId, IntentHash, PendingExecutionEvent, PublicEventContext, PublicEventEnvelope,
    PublicEventId, PublicEventKind, PublicEventPayload, ResponseItemAuthority, RunId, RunLifecycle,
    TransitionKey, EXECUTION_EVENT_SCHEMA_VERSION, PUBLIC_EVENT_SCHEMA_VERSION,
};
use crate::runtime::{WorkflowToolPublicProjection, WorkflowToolResult};

use super::{RepositoryError, REPOSITORY_DATA_INVALID};
use super::{ResponseTerminalKind, ResponseUsageStatus};

pub(crate) struct StoredResponseItem {
    pub activation_id: String,
    pub attempt_no: u32,
    pub model_call_no: u32,
    pub item_id: String,
    pub output_index: u32,
    pub node_id: String,
    pub item_kind: String,
    pub item_status: String,
    pub seal_index: Option<u64>,
    pub safe_item: Option<Value>,
}

pub(crate) struct StoredModelCallUsage {
    pub usage: Option<Value>,
    pub usage_complete: bool,
}

/// One completed durable tool-call row at the terminal snapshot boundary.
///
/// This type intentionally has no `Debug` implementation: both JSON values
/// may contain executor-private material and must only cross the closed public
/// projection below. Repository callers must read rows in the stable identity
/// order documented by [`project_terminal_tool_results`].
pub(crate) struct StoredSucceededModelToolCall {
    pub activation_id: String,
    pub attempt_no: u32,
    pub model_call_no: u32,
    pub call_index: u32,
    pub call_id: String,
    pub tool_name: String,
    pub effective_public_policy: Value,
    pub completed_result: Value,
}

impl StoredSucceededModelToolCall {
    fn order_key(&self) -> (&str, u32, u32, u32) {
        (
            &self.activation_id,
            self.attempt_no,
            self.model_call_no,
            self.call_index,
        )
    }
}

/// Revalidates durable completed tool results against their exact frozen
/// publication policies and returns only the caller-safe typed projection.
///
/// The ordering check is part of the hash/replay authority: callers must use
/// `(activation_id, attempt_no, model_call_no, call_index)` ascending order.
/// Fully private results are omitted without inspecting their executor value;
/// an invalid public policy or public result fails closed.
pub(crate) fn project_terminal_tool_results(
    calls: Vec<StoredSucceededModelToolCall>,
) -> Result<Vec<WorkflowToolResult>, RepositoryError> {
    let mut previous: Option<(&str, u32, u32, u32)> = None;
    let mut results = Vec::with_capacity(calls.len());
    for call in &calls {
        let current = call.order_key();
        if previous.is_some_and(|previous| previous >= current) {
            return Err(RepositoryError::invalid_data());
        }
        previous = Some(current);

        let projection = WorkflowToolPublicProjection::from_frozen_effective_policy(
            &call.effective_public_policy,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        if let Some(result) = projection
            .project_validated_completed_result(
                call.call_id.clone(),
                call.tool_name.clone(),
                &call.completed_result,
            )
            .map_err(|_| RepositoryError::invalid_data())?
        {
            results.push(result);
        }
    }
    Ok(results)
}

pub(crate) struct PreparedModelCallCompletion {
    pub call_status: &'static str,
    pub finish_reason: &'static str,
    pub usage: Option<Value>,
    pub usage_complete: bool,
    pub seal_index: Option<u64>,
    pub safe_item: Option<Value>,
    pub function_items: Vec<PreparedResponseFunctionPublication>,
}

pub(crate) struct PreparedResponseFunctionPublication {
    pub call_index: u32,
    pub item: ResponseItemAuthority,
    pub seal_index: u64,
    pub reserved_safe_item: Value,
    pub terminal_item_status: &'static str,
    pub terminal_safe_item: Value,
}

pub(crate) fn response_item_id(
    run_id: &RunId,
    activation_id: &crate::engine::ActivationId,
    attempt_no: crate::engine::AttemptNo,
    model_call_no: u32,
) -> String {
    let identity = format!(
        "insight-agent/response-item/v1/{}/{}/{}/{}",
        run_id.as_str(),
        activation_id.as_str(),
        attempt_no.get(),
        model_call_no
    );
    let digest = ContentHash::from_bytes(identity.as_bytes());
    format!(
        "msg_{}",
        digest
            .as_str()
            .strip_prefix("sha256:")
            .unwrap_or(digest.as_str())
    )
}

pub(crate) fn function_call_response_item_id(
    run_id: &RunId,
    activation_id: &crate::engine::ActivationId,
    attempt_no: crate::engine::AttemptNo,
    model_call_no: u32,
    call_index: u32,
    call_id: &str,
    tool_name: &str,
) -> String {
    let identity = format!(
        "insight-agent/function-call-item/v1/{}/{}/{}/{}/{}/{}/{}",
        run_id.as_str(),
        activation_id.as_str(),
        attempt_no.get(),
        model_call_no,
        call_index,
        call_id,
        tool_name,
    );
    let digest = ContentHash::from_bytes(identity.as_bytes());
    format!(
        "fc_{}",
        digest
            .as_str()
            .strip_prefix("sha256:")
            .unwrap_or(digest.as_str())
    )
}

pub(crate) fn prepare_model_call_completion(
    completion: &ModelCallCompletion,
    expected_item_id: Option<&str>,
    run_id: &RunId,
    activation_id: &ActivationId,
    attempt_no: AttemptNo,
) -> Result<PreparedModelCallCompletion, RepositoryError> {
    let safe_item = completion.safe_public_item().cloned();
    if expected_item_id.is_none()
        && (completion.public_item_seal_index().is_some() || safe_item.is_some())
    {
        return Err(RepositoryError::invalid_data());
    }
    if let Some(value) = safe_item.as_ref() {
        if completion.finish_reason() != ModelFinishReason::Stop {
            return Err(RepositoryError::invalid_data());
        }
        validate_completed_message_item(
            value,
            expected_item_id.ok_or_else(RepositoryError::invalid_data)?,
        )?;
    }
    let usage = completion
        .usage()
        .map(|usage| {
            if usage.is_complete() {
                usage
                    .public_value()
                    .ok_or_else(RepositoryError::invalid_data)
            } else {
                serde_json::to_value(usage).map_err(|_| RepositoryError::canonicalization())
            }
        })
        .transpose()?;
    let mut previous_call_index = None;
    let mut item_ids = std::collections::BTreeSet::new();
    let mut output_indices = std::collections::BTreeSet::new();
    let function_items = completion
        .incomplete_function_calls()
        .iter()
        .map(|call| {
            if matches!(
                completion.finish_reason(),
                ModelFinishReason::Stop | ModelFinishReason::ToolCalls
            ) || previous_call_index.is_some_and(|previous| previous >= call.call_index())
                || call.call_index().checked_add(1).is_none()
                || !item_ids.insert(call.public_item().item_id())
                || !output_indices.insert(call.public_item().output_index())
            {
                return Err(RepositoryError::invalid_data());
            }
            previous_call_index = Some(call.call_index());
            let expected_function_item_id = function_call_response_item_id(
                run_id,
                activation_id,
                attempt_no,
                completion.model_call_no(),
                call.call_index(),
                call.call_id(),
                call.tool_name(),
            );
            if call.public_item().item_id() != expected_function_item_id {
                return Err(RepositoryError::invalid_data());
            }
            let safe_item = serde_json::json!({
                "id": expected_function_item_id,
                "type": "function_call",
                "status": "incomplete",
                "call_id": call.call_id(),
                "name": call.tool_name(),
                "arguments": "",
            });
            validate_incomplete_function_call_item(&safe_item, call.public_item().item_id())?;
            Ok(PreparedResponseFunctionPublication {
                call_index: call.call_index(),
                item: call.public_item().clone(),
                seal_index: call.seal_index(),
                reserved_safe_item: safe_item.clone(),
                terminal_item_status: "incomplete",
                terminal_safe_item: safe_item,
            })
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
    Ok(PreparedModelCallCompletion {
        call_status: match completion.finish_reason() {
            ModelFinishReason::Stop | ModelFinishReason::ToolCalls => "completed",
            ModelFinishReason::Length
            | ModelFinishReason::ContentFilter
            | ModelFinishReason::Invalid => "failed",
        },
        finish_reason: completion.finish_reason().as_str(),
        usage_complete: completion.usage().is_some_and(|usage| usage.is_complete()),
        usage,
        seal_index: completion.public_item_seal_index(),
        safe_item,
        function_items,
    })
}

fn validate_completed_message_item(
    value: &Value,
    expected_item_id: &str,
) -> Result<(), RepositoryError> {
    let object = value
        .as_object()
        .ok_or_else(RepositoryError::invalid_data)?;
    let fields_are_closed = object
        .keys()
        .all(|key| matches!(key.as_str(), "id" | "type" | "status" | "role" | "content"));
    if !fields_are_closed
        || object.get("id").and_then(Value::as_str) != Some(expected_item_id)
        || object.get("type").and_then(Value::as_str) != Some("message")
        || object.get("status").and_then(Value::as_str) != Some("completed")
        || object.get("role").and_then(Value::as_str) != Some("assistant")
    {
        return Err(RepositoryError::invalid_data());
    }
    let content = object
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(RepositoryError::invalid_data)?;
    if content.iter().any(|part| {
        let Some(part) = part.as_object() else {
            return true;
        };
        !part
            .keys()
            .all(|key| matches!(key.as_str(), "type" | "text" | "annotations"))
            || part.get("type").and_then(Value::as_str) != Some("output_text")
            || part.get("text").and_then(Value::as_str).is_none()
            || part
                .get("annotations")
                .is_some_and(|annotations| !annotations.is_array())
    }) {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

pub(crate) struct PendingResponseSnapshot {
    pub response_id: String,
    pub terminal_kind: ResponseTerminalKind,
    pub response_status: &'static str,
    pub response: Value,
    pub workflow: Value,
    pub manifest: Value,
    pub usage: Option<Value>,
    pub usage_status: ResponseUsageStatus,
    pub snapshot_hash: ContentHash,
}

pub(crate) struct TerminalResponseSnapshotInput<'a> {
    pub run_id: &'a RunId,
    pub lifecycle: RunLifecycle,
    pub output: Option<&'a Value>,
    pub error_code: Option<&'a str>,
    pub items: Vec<StoredResponseItem>,
    pub model_calls: Vec<StoredModelCallUsage>,
    pub tool_results: Vec<WorkflowToolResult>,
    pub retrievals: Vec<crate::runtime::response_stream::WorkflowRetrieval>,
}

pub(crate) fn build_terminal_response_snapshot(
    input: TerminalResponseSnapshotInput<'_>,
) -> Result<PendingResponseSnapshot, RepositoryError> {
    let TerminalResponseSnapshotInput {
        run_id,
        lifecycle,
        output,
        error_code,
        items,
        model_calls,
        tool_results,
        retrievals,
    } = input;
    let response_id = format!("resp_{}", run_id.as_str());
    let (terminal_kind, response_status) = match lifecycle {
        RunLifecycle::Succeeded => (ResponseTerminalKind::Completed, "completed"),
        RunLifecycle::Failed => (ResponseTerminalKind::Failed, "failed"),
        RunLifecycle::TimedOut => (ResponseTerminalKind::TimedOut, "failed"),
        RunLifecycle::Cancelled => (ResponseTerminalKind::Cancelled, "cancelled"),
        RunLifecycle::Interrupted => (ResponseTerminalKind::Interrupted, "incomplete"),
        RunLifecycle::Created
        | RunLifecycle::Active
        | RunLifecycle::Waiting
        | RunLifecycle::Completing
        | RunLifecycle::Terminating => return Err(RepositoryError::invalid_configuration()),
    };
    if (lifecycle == RunLifecycle::Succeeded) != output.is_some() {
        return Err(RepositoryError::invalid_configuration());
    }

    let mut public_output = Vec::with_capacity(items.len());
    let mut manifest = Vec::with_capacity(items.len());
    for item in items {
        let status = match item.item_status.as_str() {
            "completed" => "completed",
            "reserved" | "incomplete" | "incomplete_unsealed" => "incomplete",
            _ => return Err(RepositoryError::invalid_data()),
        };
        let safe_item = match item.safe_item {
            Some(value) => {
                validate_terminal_safe_item(
                    &value,
                    &item.item_id,
                    &item.item_kind,
                    &item.item_status,
                )?;
                value
            }
            None if item.item_kind == "message" => serde_json::json!({
                "id": item.item_id,
                "type": "message",
                "status": status,
                "role": "assistant",
                "content": [],
            }),
            None => return Err(RepositoryError::invalid_data()),
        };
        public_output.push(safe_item);
        manifest.push(serde_json::json!({
            "activation_id": item.activation_id,
            "attempt_no": item.attempt_no,
            "model_call_no": item.model_call_no,
            "item_id": item.item_id,
            "output_index": item.output_index,
            "node_id": item.node_id,
            "kind": item.item_kind,
            "status": if item.item_status == "reserved" { "incomplete_unsealed" } else { item.item_status.as_str() },
            "seal_index": item.seal_index,
        }));
    }

    let (usage, usage_status) = aggregate_model_usage(&model_calls)?;
    let public_error = match lifecycle {
        RunLifecycle::Failed => Some(serde_json::json!({
            "code": error_code.unwrap_or("RUN_FAILED"),
            "message": "run failed",
            "param": Value::Null,
        })),
        RunLifecycle::TimedOut => Some(serde_json::json!({
            "code": "RUN_TIMEOUT",
            "message": "run timed out",
            "param": Value::Null,
        })),
        _ => None,
    };
    let response = serde_json::json!({
        "id": response_id,
        "object": "response",
        "status": response_status,
        "output": public_output,
        "usage": usage,
        "error": public_error,
    });
    let shared = serde_json::json!({
        "run_id": run_id.as_str(),
        "tool_results": tool_results,
        "retrievals": retrievals,
        "usage_status": usage_status.as_str(),
    });
    let mut workflow = shared
        .as_object()
        .cloned()
        .ok_or_else(RepositoryError::canonicalization)?;
    match lifecycle {
        RunLifecycle::Succeeded => {
            workflow.insert(
                "result".to_owned(),
                output
                    .cloned()
                    .ok_or_else(RepositoryError::invalid_configuration)?,
            );
        }
        RunLifecycle::Failed => {
            workflow.insert(
                "error".to_owned(),
                serde_json::json!({
                    "code": error_code.unwrap_or("RUN_FAILED"),
                    "message": "run failed",
                }),
            );
        }
        RunLifecycle::TimedOut => {
            workflow.insert(
                "error".to_owned(),
                serde_json::json!({"code":"RUN_TIMEOUT", "message":"run timed out"}),
            );
        }
        RunLifecycle::Cancelled => {
            workflow.insert("reason".to_owned(), Value::String("cancelled".to_owned()));
        }
        RunLifecycle::Interrupted => {
            workflow.insert("reason".to_owned(), Value::String("interrupted".to_owned()));
        }
        _ => unreachable!("nonterminal lifecycles were rejected above"),
    }
    let workflow = Value::Object(workflow);
    let manifest = Value::Array(manifest);
    let hash_projection = serde_json::json!({
        "response_id": response_id,
        "terminal_kind": terminal_kind.as_str(),
        "response": response,
        "workflow": workflow,
        "public_item_manifest": manifest,
        "usage": usage,
        "usage_status": usage_status.as_str(),
    });
    let (_, snapshot_hash) = canonical_value(&hash_projection)?;
    Ok(PendingResponseSnapshot {
        response_id,
        terminal_kind,
        response_status,
        response,
        workflow,
        manifest,
        usage,
        usage_status,
        snapshot_hash,
    })
}

fn validate_terminal_safe_item(
    value: &Value,
    expected_item_id: &str,
    expected_kind: &str,
    stored_status: &str,
) -> Result<(), RepositoryError> {
    match expected_kind {
        "message" if stored_status == "completed" => {
            validate_completed_message_item(value, expected_item_id)
        }
        "function_call" if stored_status == "completed" => {
            validate_completed_function_call_item(value, expected_item_id)
        }
        "function_call" => validate_incomplete_function_call_item(value, expected_item_id),
        _ => Err(RepositoryError::invalid_data()),
    }
}

fn validate_completed_function_call_item(
    value: &Value,
    expected_item_id: &str,
) -> Result<(), RepositoryError> {
    validate_function_call_item(value, expected_item_id, true)
}

pub(crate) fn validate_incomplete_function_call_item(
    value: &Value,
    expected_item_id: &str,
) -> Result<(), RepositoryError> {
    validate_function_call_item(value, expected_item_id, false)
}

fn validate_function_call_item(
    value: &Value,
    expected_item_id: &str,
    completed: bool,
) -> Result<(), RepositoryError> {
    const MAX_FUNCTION_ARGUMENT_BYTES: usize = 262_144;

    let object = value
        .as_object()
        .ok_or_else(RepositoryError::invalid_data)?;
    if !object.keys().all(|key| {
        matches!(
            key.as_str(),
            "id" | "type" | "status" | "call_id" | "name" | "arguments"
        )
    }) || object.len() != 6
        || object.get("id").and_then(Value::as_str) != Some(expected_item_id)
        || object.get("type").and_then(Value::as_str) != Some("function_call")
        || object.get("status").and_then(Value::as_str)
            != Some(if completed { "completed" } else { "incomplete" })
    {
        return Err(RepositoryError::invalid_data());
    }
    let call_id = object
        .get("call_id")
        .and_then(Value::as_str)
        .ok_or_else(RepositoryError::invalid_data)?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(RepositoryError::invalid_data)?;
    let arguments = object
        .get("arguments")
        .and_then(Value::as_str)
        .ok_or_else(RepositoryError::invalid_data)?;
    if !valid_terminal_label(call_id, 256)
        || !valid_terminal_label(name, 128)
        || arguments.len() > MAX_FUNCTION_ARGUMENT_BYTES
    {
        return Err(RepositoryError::invalid_data());
    }
    if completed {
        let parsed: Value =
            serde_json::from_str(arguments).map_err(|_| RepositoryError::invalid_data())?;
        if !parsed.is_object()
            || serde_jcs::to_string(&parsed).map_err(|_| RepositoryError::canonicalization())?
                != arguments
        {
            return Err(RepositoryError::invalid_data());
        }
    } else if !arguments.is_empty() {
        // An incomplete durable item may retain stable call metadata, but
        // never provisional Provider argument bytes.
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

fn valid_terminal_label(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && !value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
}

fn aggregate_model_usage(
    calls: &[StoredModelCallUsage],
) -> Result<(Option<Value>, ResponseUsageStatus), RepositoryError> {
    if calls.is_empty() {
        return Ok((None, ResponseUsageStatus::Unavailable));
    }
    if calls
        .iter()
        .any(|call| !call.usage_complete || call.usage.is_none())
    {
        return Ok((None, ResponseUsageStatus::Partial));
    }
    let mut input_tokens = 0_u64;
    let mut cached_tokens = 0_u64;
    let mut output_tokens = 0_u64;
    let mut reasoning_tokens = 0_u64;
    let mut total_tokens = 0_u64;
    for call in calls {
        let usage = call
            .usage
            .as_ref()
            .and_then(Value::as_object)
            .ok_or_else(RepositoryError::invalid_data)?;
        input_tokens = checked_usage_sum(input_tokens, usage_token(usage, "input_tokens")?)?;
        output_tokens = checked_usage_sum(output_tokens, usage_token(usage, "output_tokens")?)?;
        total_tokens = checked_usage_sum(total_tokens, usage_token(usage, "total_tokens")?)?;
        cached_tokens = checked_usage_sum(
            cached_tokens,
            usage_detail_token(usage, "input_tokens_details", "cached_tokens")?,
        )?;
        reasoning_tokens = checked_usage_sum(
            reasoning_tokens,
            usage_detail_token(usage, "output_tokens_details", "reasoning_tokens")?,
        )?;
    }
    Ok((
        Some(serde_json::json!({
            "input_tokens": input_tokens,
            "input_tokens_details": {"cached_tokens": cached_tokens},
            "output_tokens": output_tokens,
            "output_tokens_details": {"reasoning_tokens": reasoning_tokens},
            "total_tokens": total_tokens,
        })),
        ResponseUsageStatus::Complete,
    ))
}

fn usage_token(usage: &serde_json::Map<String, Value>, name: &str) -> Result<u64, RepositoryError> {
    usage
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(RepositoryError::invalid_data)
}

fn usage_detail_token(
    usage: &serde_json::Map<String, Value>,
    details: &str,
    name: &str,
) -> Result<u64, RepositoryError> {
    usage
        .get(details)
        .and_then(Value::as_object)
        .and_then(|details| details.get(name))
        .and_then(Value::as_u64)
        .ok_or_else(RepositoryError::invalid_data)
}

fn checked_usage_sum(left: u64, right: u64) -> Result<u64, RepositoryError> {
    left.checked_add(right)
        .ok_or_else(RepositoryError::invalid_data)
}

pub(crate) fn canonical_intent_hash<T>(intent: &T) -> Result<IntentHash, RepositoryError>
where
    T: Serialize + ?Sized,
{
    IntentHash::from_serializable(intent).map_err(|_| RepositoryError::canonicalization())
}

pub(crate) fn canonical_json(value: &Value) -> Result<String, RepositoryError> {
    serde_jcs::to_string(value).map_err(|_| RepositoryError::canonicalization())
}

/// Decodes the schema discriminator on every persisted execution-event row
/// before any of that row's authority is used. Database constraints prevent
/// new unknown rows, while this decoder fails closed for legacy corruption,
/// disabled constraints, snapshots, and future writers.
pub(crate) fn decode_execution_event_schema_version(stored: i64) -> Result<(), RepositoryError> {
    let version = u32::try_from(stored).map_err(|_| RepositoryError::invalid_data())?;
    if version != EXECUTION_EVENT_SCHEMA_VERSION {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

/// Backend-neutral persisted row used by the one closed execution-event
/// decoder. Repositories must not independently infer payload/context
/// validity from a subset of columns.
pub(crate) struct StoredExecutionEventRow {
    pub schema_version: i64,
    pub event_id: String,
    pub run_id: String,
    pub transition_key: String,
    pub intent_hash: String,
    pub seq: i64,
    pub occurred_at: DateTime<Utc>,
    pub kind: String,
    pub node_id: Option<String>,
    pub scope_instance_id: Option<String>,
    pub activation_id: Option<String>,
    pub attempt_no: Option<i64>,
    pub causation_event_id: Option<String>,
    pub safe_payload: Value,
}

/// Reconstructs the canonical typed envelope so serde's closed payload wire,
/// event-kind equality, identifier parsing, context-level validation, and
/// causation checks all run at the repository boundary. This is deliberately
/// Rust-owned rather than duplicating every payload rule in SQL triggers.
pub(crate) fn decode_stored_execution_event(
    stored: StoredExecutionEventRow,
) -> Result<ExecutionEventEnvelope, RepositoryError> {
    decode_execution_event_schema_version(stored.schema_version)?;
    let schema_version =
        u32::try_from(stored.schema_version).map_err(|_| RepositoryError::invalid_data())?;
    serde_json::from_value(serde_json::json!({
        "schema_version": schema_version,
        "event_id": stored.event_id,
        "run_id": stored.run_id,
        "transition_key": stored.transition_key,
        "intent_hash": stored.intent_hash,
        "seq": stored.seq,
        "occurred_at": stored.occurred_at,
        "kind": stored.kind,
        "node_id": stored.node_id,
        "scope_instance_id": stored.scope_instance_id,
        "activation_id": stored.activation_id,
        "attempt_no": stored.attempt_no,
        "causation_event_id": stored.causation_event_id,
        "payload": stored.safe_payload,
    }))
    .map_err(|_| RepositoryError::invalid_data())
}

/// Permanent public-projection decision plus the optional receipt/outbox
/// witnesses observed at a repository boundary. All backend-specific SQL
/// decoders normalize into this one closed authority contract.
pub(crate) struct StoredPublicProjectionDecision {
    pub run_id: String,
    pub execution_event_id: String,
    pub execution_seq: i64,
    pub execution_occurred_at: DateTime<Utc>,
    pub execution_transition_key: String,
    pub decision: String,
    pub public_event_id: Option<String>,
    pub public_ordinal: Option<i64>,
    pub public_schema_version: Option<i64>,
    pub event_kind: Option<String>,
    pub is_terminal: Option<bool>,
    pub receipt_public_event_id: Option<String>,
    pub receipt_causation_event_id: Option<String>,
    pub receipt_public_ordinal: Option<i64>,
    pub receipt_public_schema_version: Option<i64>,
    pub receipt_event_kind: Option<String>,
    pub receipt_is_terminal: Option<bool>,
    pub outbox_public_event_id: Option<String>,
    pub outbox_causation_event_id: Option<String>,
    pub outbox_public_ordinal: Option<i64>,
    pub outbox_public_schema_version: Option<i64>,
    pub outbox_event_kind: Option<String>,
    pub outbox_is_terminal: Option<bool>,
}

pub(crate) fn decode_public_event_kind(value: &str) -> Result<PublicEventKind, RepositoryError> {
    match value {
        "run.created" => Ok(PublicEventKind::RunCreated),
        "run.started" => Ok(PublicEventKind::RunStarted),
        "operation.started" => Ok(PublicEventKind::OperationStarted),
        "operation.completed" => Ok(PublicEventKind::OperationCompleted),
        "operation.failed" => Ok(PublicEventKind::OperationFailed),
        "run.completed" => Ok(PublicEventKind::RunCompleted),
        "run.failed" => Ok(PublicEventKind::RunFailed),
        "run.cancelled" => Ok(PublicEventKind::RunCancelled),
        "run.interrupted" => Ok(PublicEventKind::RunInterrupted),
        _ => Err(RepositoryError::invalid_data()),
    }
}

pub(crate) const fn public_event_is_terminal(kind: PublicEventKind) -> bool {
    matches!(
        kind,
        PublicEventKind::RunCompleted
            | PublicEventKind::RunFailed
            | PublicEventKind::RunCancelled
            | PublicEventKind::RunInterrupted
    )
}

/// Decodes None versus Some without treating a missing prunable outbox body as
/// evidence. A public decision requires an exact permanent receipt; an outbox
/// witness is optional only because a published nonterminal body may expire.
pub(crate) fn decode_public_projection_decision(
    stored: StoredPublicProjectionDecision,
    execution: &ExecutionEventEnvelope,
) -> Result<Option<String>, RepositoryError> {
    if execution.run_id().as_str() != stored.run_id
        || execution.event_id().as_str() != stored.execution_event_id
        || i64_from_u64(execution.seq().get())? != stored.execution_seq
        || execution.occurred_at() != &stored.execution_occurred_at
        || execution.transition_key().as_str() != stored.execution_transition_key
    {
        return Err(RepositoryError::invalid_data());
    }

    let no_public_metadata = stored.public_event_id.is_none()
        && stored.public_ordinal.is_none()
        && stored.public_schema_version.is_none()
        && stored.event_kind.is_none()
        && stored.is_terminal.is_none();
    let no_receipt = stored.receipt_public_event_id.is_none()
        && stored.receipt_causation_event_id.is_none()
        && stored.receipt_public_ordinal.is_none()
        && stored.receipt_public_schema_version.is_none()
        && stored.receipt_event_kind.is_none()
        && stored.receipt_is_terminal.is_none();
    let no_outbox = stored.outbox_public_event_id.is_none()
        && stored.outbox_causation_event_id.is_none()
        && stored.outbox_public_ordinal.is_none()
        && stored.outbox_public_schema_version.is_none()
        && stored.outbox_event_kind.is_none()
        && stored.outbox_is_terminal.is_none();
    if stored.decision == "none" {
        return if no_public_metadata && no_receipt && no_outbox {
            Ok(None)
        } else {
            Err(RepositoryError::invalid_data())
        };
    }
    if stored.decision != "public" || no_public_metadata {
        return Err(RepositoryError::invalid_data());
    }

    let stored_public_event_id = stored
        .public_event_id
        .as_deref()
        .ok_or_else(RepositoryError::invalid_data)?;
    let event_kind = stored
        .event_kind
        .as_deref()
        .ok_or_else(RepositoryError::invalid_data)?;
    let kind = decode_public_event_kind(event_kind)?;
    let ordinal = stored
        .public_ordinal
        .ok_or_else(RepositoryError::invalid_data)?;
    let schema_version = stored
        .public_schema_version
        .ok_or_else(RepositoryError::invalid_data)?;
    let terminal = stored
        .is_terminal
        .ok_or_else(RepositoryError::invalid_data)?;
    let run_id = RunId::new(stored.run_id.clone()).map_err(|_| RepositoryError::invalid_data())?;
    let expected_id = public_event_id(&run_id, execution.transition_key(), kind);
    if schema_version != i64::from(PUBLIC_EVENT_SCHEMA_VERSION)
        || ordinal != i64::from(public_event_ordinal(kind))
        || terminal != public_event_is_terminal(kind)
        || stored_public_event_id != expected_id
        || stored.receipt_public_event_id.as_deref() != Some(stored_public_event_id)
        || stored.receipt_causation_event_id.as_deref() != Some(execution.event_id().as_str())
        || stored.receipt_public_ordinal != Some(ordinal)
        || stored.receipt_public_schema_version != Some(schema_version)
        || stored.receipt_event_kind.as_deref() != Some(event_kind)
        || stored.receipt_is_terminal != Some(terminal)
    {
        return Err(RepositoryError::invalid_data());
    }
    if !no_outbox
        && (stored.outbox_public_event_id.as_deref() != Some(stored_public_event_id)
            || stored.outbox_causation_event_id.as_deref() != Some(execution.event_id().as_str())
            || stored.outbox_public_ordinal != Some(ordinal)
            || stored.outbox_public_schema_version != Some(schema_version)
            || stored.outbox_event_kind.as_deref() != Some(event_kind)
            || stored.outbox_is_terminal != Some(terminal))
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(Some(stored_public_event_id.to_owned()))
}

/// Builds the one canonical public wire envelope from the committed execution
/// event authority. In particular, `occurred_at` must be the timestamp returned
/// by the execution-event INSERT in the same transaction.
pub(crate) fn durable_public_event_envelope(
    run_id: &RunId,
    public_event_id: &str,
    causation_event_id: &str,
    seq: u64,
    occurred_at: DateTime<Utc>,
    payload: PublicEventPayload,
) -> Result<PublicEventEnvelope, RepositoryError> {
    let public_event_id = PublicEventId::parse(public_event_id.to_owned())
        .map_err(|_| RepositoryError::invalid_data())?;
    let causation_event_id = ExecutionEventId::parse(causation_event_id.to_owned())
        .map_err(|_| RepositoryError::invalid_data())?;
    let seq = EventSeq::new(seq).map_err(|_| RepositoryError::invalid_data())?;
    Ok(PublicEventEnvelope::new(
        PublicEventContext::new(public_event_id, run_id.clone(), causation_event_id),
        seq,
        occurred_at,
        payload,
    ))
}

/// Stable idempotency identity shared by direct loser delivery and the
/// startup/runtime late-audit reconciler. Caller request keys cannot provide
/// this authority because a process may die after observing the durable
/// first-winner state but before preserving that request for replay.
pub(crate) fn wait_late_audit_identity(
    loser_kind: &'static str,
    run_id: &RunId,
    loser_id: &str,
) -> Result<(TransitionKey, IntentHash), RepositoryError> {
    if !matches!(loser_kind, "signal" | "timer") || loser_id.is_empty() {
        return Err(RepositoryError::invalid_data());
    }
    let transition_key = TransitionKey::derive(
        "repository.wait-late-audit.v1",
        &[loser_kind, run_id.as_str(), loser_id],
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    #[derive(Serialize)]
    struct WaitLateAuditIntent<'a> {
        contract: &'static str,
        loser_kind: &'static str,
        run_id: &'a str,
        loser_id: &'a str,
    }
    let intent_hash = canonical_intent_hash(&WaitLateAuditIntent {
        contract: "wait_late_audit.v1",
        loser_kind,
        run_id: run_id.as_str(),
        loser_id,
    })?;
    Ok((transition_key, intent_hash))
}

pub(crate) fn canonical_value(value: &Value) -> Result<(String, ContentHash), RepositoryError> {
    let encoded = canonical_json(value)?;
    let hash = ContentHash::from_bytes(encoded.as_bytes());
    Ok((encoded, hash))
}

/// A JSON payload reconstructed from the bytes/value stored in the durable
/// payload authority.  Callers must use this result rather than trusting the
/// denormalized `content_hash` or `canonical_bytes` columns independently.
pub(crate) struct ValidatedInlinePayload {
    value: Value,
    canonical: String,
    content_hash: ContentHash,
    canonical_bytes: u64,
}

impl ValidatedInlinePayload {
    pub(crate) fn value(&self) -> &Value {
        &self.value
    }

    pub(crate) fn canonical(&self) -> &str {
        &self.canonical
    }

    pub(crate) fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }

    pub(crate) const fn canonical_bytes(&self) -> u64 {
        self.canonical_bytes
    }
}

/// Validates the complete backend-neutral inline-payload authority.
///
/// `sqlite_source` is the exact TEXT stored by SQLite. PostgreSQL JSONB does
/// not preserve its source spelling, so its callers pass `None`; both backends
/// still recompute RFC 8785 JCS from the parsed JSON value. `binary_is_null`
/// is checked in Rust as well as by the schema so disabled constraints and
/// imported/corrupt snapshots fail closed.
pub(crate) fn validate_inline_payload(
    stored_payload_id: &str,
    stored_content_hash: &str,
    stored_canonical_bytes: i64,
    stored_encoding: &str,
    value: Value,
    sqlite_source: Option<&str>,
    binary_is_null: bool,
) -> Result<ValidatedInlinePayload, RepositoryError> {
    if stored_encoding != "json_jcs" || !binary_is_null {
        return Err(RepositoryError::invalid_data());
    }
    let (canonical, content_hash) = canonical_value(&value)?;
    let canonical_bytes =
        u64::try_from(canonical.len()).map_err(|_| RepositoryError::invalid_data())?;
    if stored_content_hash != content_hash.as_str()
        || u64_from_i64(stored_canonical_bytes)? != canonical_bytes
        || stored_payload_id != payload_id(&content_hash)
        || sqlite_source.is_some_and(|source| source != canonical)
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(ValidatedInlinePayload {
        value,
        canonical,
        content_hash,
        canonical_bytes,
    })
}

/// Canonical authority for a durable scheduler checkpoint. The hash binds the
/// complete immutable provenance of the committed boundary rather than only
/// its fact payload, so a checkpoint cannot be transplanted between Runs or
/// scheduler transitions.
#[allow(clippy::too_many_arguments)]
pub(crate) fn scheduler_checkpoint_content_hash(
    run_id: &str,
    checkpoint_id: &str,
    checkpoint_kind: &str,
    transition_key: &str,
    intent_hash: &str,
    event_id: &str,
    checkpoint_schema_version: u32,
    scheduler_projection_version: u64,
    fact_payload: &Value,
) -> Result<ContentHash, RepositoryError> {
    #[derive(Serialize)]
    struct Provenance<'a> {
        domain: &'static str,
        run_id: &'a str,
        checkpoint_id: &'a str,
        checkpoint_kind: &'a str,
        transition_key: &'a str,
        intent_hash: &'a str,
        event_id: &'a str,
        checkpoint_schema_version: u32,
        scheduler_projection_version: u64,
        fact_payload: &'a Value,
    }

    let provenance = Provenance {
        domain: "insight-agent/scheduler-checkpoint/v1",
        run_id,
        checkpoint_id,
        checkpoint_kind,
        transition_key,
        intent_hash,
        event_id,
        checkpoint_schema_version,
        scheduler_projection_version,
        fact_payload,
    };
    serde_jcs::to_vec(&provenance)
        .map(|encoded| ContentHash::from_bytes(&encoded))
        .map_err(|_| RepositoryError::canonicalization())
}

pub(crate) fn event_id(key: &TransitionKey) -> String {
    format!("event_{}", transition_id_token(key))
}

pub(crate) const fn public_event_ordinal(kind: crate::engine::PublicEventKind) -> u16 {
    match kind {
        crate::engine::PublicEventKind::RunCreated => 10,
        crate::engine::PublicEventKind::RunStarted => 20,
        crate::engine::PublicEventKind::OperationStarted => 30,
        crate::engine::PublicEventKind::OperationCompleted
        | crate::engine::PublicEventKind::OperationFailed => 40,
        crate::engine::PublicEventKind::RunCompleted
        | crate::engine::PublicEventKind::RunFailed
        | crate::engine::PublicEventKind::RunCancelled
        | crate::engine::PublicEventKind::RunInterrupted => 50,
    }
}

pub(crate) fn public_event_id(
    run_id: &RunId,
    key: &TransitionKey,
    kind: crate::engine::PublicEventKind,
) -> String {
    // Transition keys are intentionally scoped by Run and may be identical in
    // two Runs. Public notifications contain only this ID, so their identity
    // must bind both dimensions before the globally unique database lookup.
    // Both model IDs reject control characters, making NUL unambiguous framing.
    let derived = ContentHash::from_bytes(
        format!(
            "insight-agent/public-event/v4\0{}\0{}\0{}",
            run_id.as_str(),
            key.as_str(),
            kind.as_str(),
        )
        .as_bytes(),
    );
    format!(
        "public_event_{}",
        &derived.as_str()["sha256:".len()..][..32]
    )
}

pub(crate) fn task_id(key: &TransitionKey) -> String {
    format!("task_{}", transition_id_token(key))
}

pub(crate) fn timer_id(key: &TransitionKey, purpose: &str) -> String {
    let derived = ContentHash::from_bytes(
        format!("insight-agent/repository/{purpose}/{}", key.as_str()).as_bytes(),
    );
    format!("timer_{}", &derived.as_str()["sha256:".len()..][..32])
}

pub(crate) fn companion_transition_key(
    primary: &TransitionKey,
    role: &str,
) -> Result<TransitionKey, RepositoryError> {
    TransitionKey::derive("repository.companion", &[primary.as_str(), role])
        .map_err(|_| RepositoryError::invalid_data())
}

#[derive(Serialize)]
struct CompanionEventIntent<'a> {
    operation: &'static str,
    primary_transition_key: &'a TransitionKey,
    primary_event_id: &'a str,
    role: &'a str,
    event: &'a PendingExecutionEvent,
}

pub(crate) fn companion_event_intent_hash(
    primary_transition_key: &TransitionKey,
    primary_event_id: &str,
    role: &str,
    event: &PendingExecutionEvent,
) -> Result<IntentHash, RepositoryError> {
    canonical_intent_hash(&CompanionEventIntent {
        operation: "repository.companion_event",
        primary_transition_key,
        primary_event_id,
        role,
        event,
    })
}

pub(crate) fn fencing_token(key: &TransitionKey) -> String {
    format!("fence_{}", transition_id_token(key))
}

pub(crate) fn payload_id(hash: &ContentHash) -> String {
    format!(
        "payload_{}",
        hash.as_str()
            .strip_prefix("sha256:")
            .unwrap_or(hash.as_str())
    )
}

fn transition_digest(key: &TransitionKey) -> &str {
    key.as_str()
        .strip_prefix("transition_")
        .unwrap_or(key.as_str())
}

fn transition_id_token(key: &TransitionKey) -> &str {
    // Transition keys carry a SHA-256 digest, while the closed event identity
    // contract uses 128-bit (32 hex digit) tokens. Truncation is deterministic
    // and keeps repository-assigned IDs parseable by Execution/PublicEventId.
    &transition_digest(key)[..32]
}

pub(crate) fn i64_from_u64(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| {
        RepositoryError::new(
            REPOSITORY_DATA_INVALID,
            "durable repository counter is out of range",
        )
    })
}

pub(crate) fn u64_from_i64(value: i64) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| RepositoryError::invalid_data())
}

pub(crate) fn parse_run_lifecycle(value: &str) -> Result<RunLifecycle, RepositoryError> {
    match value {
        "created" => Ok(RunLifecycle::Created),
        "active" => Ok(RunLifecycle::Active),
        "waiting" => Ok(RunLifecycle::Waiting),
        "completing" => Ok(RunLifecycle::Completing),
        "terminating" => Ok(RunLifecycle::Terminating),
        "succeeded" => Ok(RunLifecycle::Succeeded),
        "failed" => Ok(RunLifecycle::Failed),
        "cancelled" => Ok(RunLifecycle::Cancelled),
        "interrupted" => Ok(RunLifecycle::Interrupted),
        "timed_out" => Ok(RunLifecycle::TimedOut),
        _ => Err(RepositoryError::invalid_data()),
    }
}

pub(crate) fn parse_admission_state(value: &str) -> Result<AdmissionState, RepositoryError> {
    match value {
        "open" => Ok(AdmissionState::Open),
        "paused" => Ok(AdmissionState::Paused),
        "draining" => Ok(AdmissionState::Draining),
        "closed" => Ok(AdmissionState::Closed),
        _ => Err(RepositoryError::invalid_data()),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use crate::{
        engine::{PublicEventId, PublicEventKind, RunId, RunLifecycle, TransitionKey},
        resources::actions::{ToolPublicArguments, ToolPublicPolicy},
    };

    use super::{
        build_terminal_response_snapshot, project_terminal_tool_results, public_event_id,
        public_event_ordinal, validate_completed_function_call_item, StoredModelCallUsage,
        StoredResponseItem, StoredSucceededModelToolCall, TerminalResponseSnapshotInput,
    };

    fn normalized_public_result_schema() -> Value {
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {},
            "type": "object",
            "properties": {"indicator": {"type": "string"}},
            "required": ["indicator"],
            "additionalProperties": false
        })
    }

    fn frozen_policy(call: bool, result: Option<Value>) -> Value {
        serde_json::to_value(ToolPublicPolicy {
            call,
            arguments: ToolPublicArguments::Private,
            result_schema: result,
        })
        .unwrap()
    }

    fn stored_tool_call(
        activation_id: &str,
        call_index: u32,
        policy: Value,
        result: Value,
    ) -> StoredSucceededModelToolCall {
        StoredSucceededModelToolCall {
            activation_id: activation_id.to_owned(),
            attempt_no: 1,
            model_call_no: 1,
            call_index,
            call_id: format!("call_{call_index}"),
            tool_name: "lookup".to_owned(),
            effective_public_policy: policy,
            completed_result: result,
        }
    }

    #[test]
    fn public_event_identity_binds_run_transition_key_and_kind() {
        let transition =
            TransitionKey::derive("repository.public-event.test", &["same-key"]).unwrap();
        let run_a = RunId::new("run_public_identity_a").unwrap();
        let run_b = RunId::new("run_public_identity_b").unwrap();

        let first = public_event_id(&run_a, &transition, PublicEventKind::RunCreated);
        assert_eq!(
            first,
            public_event_id(&run_a, &transition, PublicEventKind::RunCreated)
        );
        assert_ne!(
            first,
            public_event_id(&run_b, &transition, PublicEventKind::RunCreated)
        );
        assert_ne!(
            first,
            public_event_id(&run_a, &transition, PublicEventKind::RunStarted)
        );
        PublicEventId::parse(first).unwrap();
        PublicEventId::parse(public_event_id(
            &run_b,
            &transition,
            PublicEventKind::RunCreated,
        ))
        .unwrap();
    }

    #[test]
    fn public_event_ordinal_totally_orders_same_execution_event_projections() {
        assert!(
            public_event_ordinal(PublicEventKind::OperationFailed)
                < public_event_ordinal(PublicEventKind::RunFailed)
        );
        assert_eq!(
            public_event_ordinal(PublicEventKind::OperationCompleted),
            public_event_ordinal(PublicEventKind::OperationFailed)
        );
    }

    #[test]
    fn terminal_function_call_items_are_closed_and_keep_canonical_object_arguments() {
        let valid = json!({
            "id": "fc_public",
            "type": "function_call",
            "status": "completed",
            "call_id": "call_1",
            "name": "lookup",
            "arguments": "{\"indicator\":\"WBC\",\"value\":12}"
        });
        validate_completed_function_call_item(&valid, "fc_public").unwrap();

        let mut extra = valid.clone();
        extra["private"] = json!("must-not-leak");
        assert!(validate_completed_function_call_item(&extra, "fc_public").is_err());

        let mut noncanonical = valid.clone();
        noncanonical["arguments"] = json!("{\"value\":12,\"indicator\":\"WBC\"}");
        assert!(validate_completed_function_call_item(&noncanonical, "fc_public").is_err());

        let mut non_object = valid;
        non_object["arguments"] = json!("[1,2,3]");
        assert!(validate_completed_function_call_item(&non_object, "fc_public").is_err());
    }

    #[test]
    fn terminal_tool_results_omit_private_and_fail_closed_for_order_or_public_values() {
        let projected = project_terminal_tool_results(vec![
            stored_tool_call(
                "activation_a",
                0,
                frozen_policy(false, None),
                json!({"executor_secret": "never inspect or publish"}),
            ),
            stored_tool_call(
                "activation_b",
                0,
                frozen_policy(true, Some(normalized_public_result_schema())),
                json!({"indicator": "WBC"}),
            ),
        ])
        .unwrap();
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].call_id(), "call_0");
        assert_eq!(
            projected[0].content()[0].json(),
            Some(&json!({"indicator": "WBC"}))
        );

        assert!(project_terminal_tool_results(vec![
            stored_tool_call("activation_b", 0, frozen_policy(false, None), Value::Null,),
            stored_tool_call("activation_a", 0, frozen_policy(false, None), Value::Null,),
        ])
        .is_err());
        assert!(project_terminal_tool_results(vec![stored_tool_call(
            "activation_a",
            0,
            frozen_policy(true, Some(normalized_public_result_schema())),
            json!({"indicator": 7}),
        )])
        .is_err());
    }

    #[test]
    fn every_terminal_workflow_snapshot_carries_the_same_stable_tool_results() {
        let run_id = RunId::new("run_terminal_tool_results").unwrap();
        let terminal_cases = [
            RunLifecycle::Succeeded,
            RunLifecycle::Failed,
            RunLifecycle::TimedOut,
            RunLifecycle::Cancelled,
            RunLifecycle::Interrupted,
        ];
        let mut observed = Vec::new();
        for lifecycle in terminal_cases {
            let tool_results = project_terminal_tool_results(vec![stored_tool_call(
                "activation_a",
                0,
                frozen_policy(true, Some(normalized_public_result_schema())),
                json!({"indicator": "WBC"}),
            )])
            .unwrap();
            let snapshot = build_terminal_response_snapshot(TerminalResponseSnapshotInput {
                run_id: &run_id,
                lifecycle,
                output: (lifecycle == RunLifecycle::Succeeded)
                    .then_some(&json!({"answer": "safe"})),
                error_code: (lifecycle == RunLifecycle::Failed).then_some("RUN_FAILED"),
                items: Vec::new(),
                model_calls: Vec::new(),
                tool_results,
                retrievals: Vec::new(),
            })
            .unwrap();
            observed.push(snapshot.workflow["tool_results"].clone());
        }
        assert!(observed.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(observed[0][0]["content"][0]["type"], "output_json");
        assert_eq!(observed[0][0]["content"][0]["json"]["indicator"], "WBC");

        let build_replay = || {
            build_terminal_response_snapshot(TerminalResponseSnapshotInput {
                run_id: &run_id,
                lifecycle: RunLifecycle::Cancelled,
                output: None,
                error_code: Some("SCHEDULER_CANCELLED"),
                items: Vec::new(),
                model_calls: Vec::new(),
                tool_results: project_terminal_tool_results(vec![stored_tool_call(
                    "activation_a",
                    0,
                    frozen_policy(true, Some(normalized_public_result_schema())),
                    json!({"indicator": "WBC"}),
                )])
                .unwrap(),
                retrievals: Vec::new(),
            })
            .unwrap()
        };
        let first = build_replay();
        let replay = build_replay();
        assert_eq!(first.workflow, replay.workflow);
        assert_eq!(first.snapshot_hash, replay.snapshot_hash);
    }

    #[test]
    fn terminal_usage_retains_reported_calls_across_failed_and_cancelled_lifecycles() {
        let run_id = RunId::new("run_terminal_usage_failure_matrix").unwrap();
        let complete_calls = || {
            vec![
                StoredModelCallUsage {
                    usage: Some(json!({
                        "input_tokens": 10,
                        "input_tokens_details": {"cached_tokens": 1},
                        "output_tokens": 2,
                        "output_tokens_details": {"reasoning_tokens": 0},
                        "total_tokens": 12,
                    })),
                    usage_complete: true,
                },
                StoredModelCallUsage {
                    usage: Some(json!({
                        "input_tokens": 20,
                        "input_tokens_details": {"cached_tokens": 2},
                        "output_tokens": 4,
                        "output_tokens_details": {"reasoning_tokens": 1},
                        "total_tokens": 24,
                    })),
                    usage_complete: true,
                },
            ]
        };
        let expected = json!({
            "input_tokens": 30,
            "input_tokens_details": {"cached_tokens": 3},
            "output_tokens": 6,
            "output_tokens_details": {"reasoning_tokens": 1},
            "total_tokens": 36,
        });

        for lifecycle in [RunLifecycle::Failed, RunLifecycle::Cancelled] {
            let snapshot = build_terminal_response_snapshot(TerminalResponseSnapshotInput {
                run_id: &run_id,
                lifecycle,
                output: None,
                error_code: (lifecycle == RunLifecycle::Failed).then_some("PROVIDER_FAILED"),
                items: Vec::new(),
                model_calls: complete_calls(),
                tool_results: Vec::new(),
                retrievals: Vec::new(),
            })
            .unwrap();
            assert_eq!(snapshot.response["usage"], expected);
            assert_eq!(snapshot.workflow["usage_status"], "complete");
            assert_eq!(snapshot.usage.as_ref(), Some(&expected));
        }
    }

    #[test]
    fn terminal_usage_marks_provider_omission_without_fabricating_zeroes() {
        let run_id = RunId::new("run_terminal_usage_missing").unwrap();
        let unavailable = build_terminal_response_snapshot(TerminalResponseSnapshotInput {
            run_id: &run_id,
            lifecycle: RunLifecycle::Cancelled,
            output: None,
            error_code: None,
            items: Vec::new(),
            model_calls: Vec::new(),
            tool_results: Vec::new(),
            retrievals: Vec::new(),
        })
        .unwrap();
        assert_eq!(unavailable.response["usage"], Value::Null);
        assert_eq!(unavailable.workflow["usage_status"], "unavailable");
        assert_eq!(unavailable.usage, None);

        let partial = build_terminal_response_snapshot(TerminalResponseSnapshotInput {
            run_id: &run_id,
            lifecycle: RunLifecycle::Failed,
            output: None,
            error_code: Some("PROVIDER_FAILED"),
            items: Vec::new(),
            model_calls: vec![
                StoredModelCallUsage {
                    usage: Some(json!({
                        "input_tokens": 10,
                        "input_tokens_details": {"cached_tokens": 1},
                        "output_tokens": 2,
                        "output_tokens_details": {"reasoning_tokens": 0},
                        "total_tokens": 12,
                    })),
                    usage_complete: true,
                },
                StoredModelCallUsage {
                    usage: None,
                    usage_complete: false,
                },
            ],
            tool_results: Vec::new(),
            retrievals: Vec::new(),
        })
        .unwrap();
        assert_eq!(partial.response["usage"], Value::Null);
        assert_eq!(partial.workflow["usage_status"], "partial");
        assert_eq!(partial.usage, None);
    }

    #[test]
    fn terminal_function_call_reservation_is_incomplete_without_provisional_arguments() {
        let run_id = RunId::new("run_incomplete_function_call").unwrap();
        let item = |arguments: &str| StoredResponseItem {
            activation_id: "activation_a".to_owned(),
            attempt_no: 1,
            model_call_no: 1,
            item_id: "fc_safe".to_owned(),
            output_index: 0,
            node_id: "answer".to_owned(),
            item_kind: "function_call".to_owned(),
            item_status: "reserved".to_owned(),
            seal_index: None,
            safe_item: Some(json!({
                "id": "fc_safe",
                "type": "function_call",
                "status": "incomplete",
                "call_id": "call_safe",
                "name": "lookup",
                "arguments": arguments,
            })),
        };
        let snapshot = build_terminal_response_snapshot(TerminalResponseSnapshotInput {
            run_id: &run_id,
            lifecycle: RunLifecycle::Cancelled,
            output: None,
            error_code: Some("SCHEDULER_CANCELLED"),
            items: vec![item("")],
            model_calls: Vec::new(),
            tool_results: Vec::new(),
            retrievals: Vec::new(),
        })
        .unwrap();
        assert_eq!(snapshot.response["output"][0]["status"], "incomplete");
        assert_eq!(snapshot.response["output"][0]["arguments"], "");
        assert_eq!(snapshot.manifest[0]["status"], "incomplete_unsealed");

        assert!(
            build_terminal_response_snapshot(TerminalResponseSnapshotInput {
                run_id: &run_id,
                lifecycle: RunLifecycle::Cancelled,
                output: None,
                error_code: Some("SCHEDULER_CANCELLED"),
                items: vec![item(r#"{"secret":"#)],
                model_calls: Vec::new(),
                tool_results: Vec::new(),
                retrievals: Vec::new(),
            })
            .is_err()
        );
    }
}
