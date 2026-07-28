use std::fmt::Write as _;

use chrono::{DateTime, Utc};
use insight_agent_platform::{
    api::v1::{ConversationMessageCursor, RunPersistenceCapability},
    dsl::{compile_source, CompileOptions},
    engine::{
        ActivationId, AttemptNo, DefinitionRevisionId, ExecutionEventContext,
        ExecutionEventEnvelope, ExecutionEventKind, ExecutionEventPayload, NodeId,
        PendingExecutionEvent, PublicEventEnvelope, PublicEventKind, PublicEventPayload, RunId,
        ScopeInstanceId,
    },
    events::{RunEvent, RunEventScope, RunEventType},
    history::types::{
        RunAttachment, RunLifecycle as HistoryRunLifecycle, RunStatus, RunSummaryLifecycle,
        StopError,
    },
    outcome::{FailureKind, RunFailure, RunOutput, TerminalOutcome, WorkflowError},
    runtime::{ResponseStreamEvent, ResponseStreamEventType},
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").unwrap();
    }
    encoded
}

fn fixed_time() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-07-18T01:02:03Z")
        .unwrap()
        .with_timezone(&Utc)
}

fn deserialize_values<T: DeserializeOwned>(values: Vec<Value>) -> Vec<T> {
    values
        .into_iter()
        .map(|value| serde_json::from_value(value).unwrap())
        .collect()
}

fn execution_event_payloads() -> Vec<ExecutionEventPayload> {
    let hash = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    let output = || json!({ "content_hash": hash, "size_bytes": 17 });
    let failure = || json!({ "kind": "workflow", "code": "WORKFLOW_FAILED" });

    deserialize_values(vec![
        json!({
            "type": "run_created",
            "definition_revision_id": "definition_phase0",
            "deployment_revision_id": "deployment_phase0",
            "run_deadline_at": "2026-07-18T01:02:03Z"
        }),
        json!({ "type": "run_lifecycle_changed", "lifecycle": "active" }),
        json!({ "type": "run_admission_changed", "admission": "open" }),
        json!({ "type": "run_termination_claimed", "reason": "interrupted" }),
        json!({
            "type": "scope_created",
            "scope_kind": "root",
            "parent_scope_instance_id": null
        }),
        json!({
            "type": "scope_draining",
            "admitted_children": 2,
            "settled_children": 1,
            "live_attempts": 1
        }),
        json!({
            "type": "scope_settled",
            "admitted_children": 2,
            "settled_children": 2,
            "live_attempts": 0
        }),
        json!({ "type": "activation_created", "effect_id": "effect_phase0" }),
        json!({ "type": "activation_ready" }),
        json!({ "type": "activation_leased", "attempt_no": 1, "lease_epoch": 1 }),
        json!({ "type": "activation_running", "attempt_no": 1 }),
        json!({ "type": "activation_retry_wait", "attempt_no": 1 }),
        json!({ "type": "activation_waiting" }),
        json!({ "type": "activation_terminating", "reason": "cancelled" }),
        json!({
            "type": "activation_succeeded",
            "attempt_no": 1,
            "output": output()
        }),
        json!({
            "type": "activation_failed",
            "attempt_no": 1,
            "reason": "failure",
            "failure": failure()
        }),
        json!({ "type": "activation_cancelled" }),
        json!({ "type": "activation_timed_out" }),
        json!({ "type": "attempt_created", "lease_epoch": 1 }),
        json!({ "type": "attempt_leased", "lease_epoch": 1 }),
        json!({ "type": "attempt_running", "lease_epoch": 1 }),
        json!({ "type": "attempt_succeeded", "output": output() }),
        json!({ "type": "attempt_failed", "failure": failure() }),
        json!({ "type": "attempt_timed_out" }),
        json!({ "type": "attempt_abandoned", "effect_evidence": "unknown" }),
        json!({ "type": "attempt_cancelled" }),
        json!({ "type": "effect_evidence_recorded", "evidence": "started" }),
        json!({
            "type": "control_token_emitted",
            "token_id": "token_phase0",
            "source_port": "success",
            "token_scope_instance_id": "scope_phase0",
            "frames": [{
                "kind": "branch",
                "branch_activation_id": "activation_phase0",
                "selected_port": "success",
                "scope_instance_id": "scope_phase0"
            }]
        }),
        json!({ "type": "control_token_consumed", "token_id": "token_phase0" }),
        json!({ "type": "control_token_revoked", "token_id": "token_phase0" }),
        json!({
            "type": "fork_created",
            "fork_group_id": "fork_phase0",
            "legs": [{
                "leg_id": "left",
                "output_port": "success",
                "scope_instance_id": "scope_leg_phase0"
            }]
        }),
        json!({
            "type": "join_arrived",
            "fork_group_id": "fork_phase0",
            "leg_id": "left",
            "token_id": "token_phase0",
            "settlement": "succeeded",
            "value": output()
        }),
        json!({
            "type": "join_completed",
            "fork_group_id": "fork_phase0",
            "mode": "all_success",
            "settled_leg_count": 1
        }),
        json!({ "type": "signal_received", "signal_id": "signal_phase0", "value": output() }),
        json!({ "type": "signal_late", "signal_id": "signal_phase0", "value": null }),
        json!({
            "type": "timer_scheduled",
            "timer_id": "timer_phase0",
            "fire_at": "2026-07-18T01:02:03Z"
        }),
        json!({ "type": "timer_fired", "timer_id": "timer_phase0" }),
        json!({ "type": "timer_late", "timer_id": "timer_phase0" }),
        json!({ "type": "projection_mutated", "mutation": "signal_received" }),
    ])
}

fn context_for(kind: ExecutionEventKind) -> ExecutionEventContext {
    let run = || ExecutionEventContext::for_run(RunId::new("run_phase0_wire").unwrap());
    match kind {
        ExecutionEventKind::RunCreated
        | ExecutionEventKind::RunLifecycleChanged
        | ExecutionEventKind::RunAdmissionChanged
        | ExecutionEventKind::RunTerminationClaimed
        | ExecutionEventKind::ProjectionMutated => run(),
        ExecutionEventKind::ScopeCreated
        | ExecutionEventKind::ScopeDraining
        | ExecutionEventKind::ScopeSettled => run().in_scope(ScopeInstanceId::root()),
        ExecutionEventKind::AttemptCreated
        | ExecutionEventKind::AttemptLeased
        | ExecutionEventKind::AttemptRunning
        | ExecutionEventKind::AttemptSucceeded
        | ExecutionEventKind::AttemptFailed
        | ExecutionEventKind::AttemptTimedOut
        | ExecutionEventKind::AttemptAbandoned
        | ExecutionEventKind::AttemptCancelled
        | ExecutionEventKind::EffectEvidenceRecorded => run().for_attempt(
            ScopeInstanceId::root(),
            NodeId::new("phase0_node").unwrap(),
            ActivationId::new("activation_phase0").unwrap(),
            AttemptNo::FIRST,
        ),
        ExecutionEventKind::ActivationCreated
        | ExecutionEventKind::ActivationReady
        | ExecutionEventKind::ActivationLeased
        | ExecutionEventKind::ActivationRunning
        | ExecutionEventKind::ActivationRetryWait
        | ExecutionEventKind::ActivationWaiting
        | ExecutionEventKind::ActivationTerminating
        | ExecutionEventKind::ActivationSucceeded
        | ExecutionEventKind::ActivationFailed
        | ExecutionEventKind::ActivationCancelled
        | ExecutionEventKind::ActivationTimedOut
        | ExecutionEventKind::ControlTokenEmitted
        | ExecutionEventKind::ControlTokenConsumed
        | ExecutionEventKind::ControlTokenRevoked
        | ExecutionEventKind::ForkCreated
        | ExecutionEventKind::JoinArrived
        | ExecutionEventKind::JoinCompleted
        | ExecutionEventKind::SignalReceived
        | ExecutionEventKind::SignalLate
        | ExecutionEventKind::TimerScheduled
        | ExecutionEventKind::TimerFired
        | ExecutionEventKind::TimerLate => run().for_activation(
            ScopeInstanceId::root(),
            NodeId::new("phase0_node").unwrap(),
            ActivationId::new("activation_phase0").unwrap(),
        ),
    }
}

fn public_event_payloads() -> Vec<PublicEventPayload> {
    let failure = || json!({ "kind": "workflow", "code": "WORKFLOW_FAILED" });

    deserialize_values(vec![
        json!({ "type": "run_created" }),
        json!({ "type": "run_started" }),
        json!({
            "type": "operation_started",
            "node_id": "phase0_node",
            "activation_id": "activation_phase0",
            "attempt_no": 1
        }),
        json!({
            "type": "operation_completed",
            "node_id": "phase0_node",
            "activation_id": "activation_phase0",
            "attempt_no": 1,
            "elapsed_ms": 23,
            "output_bytes": 17
        }),
        json!({
            "type": "operation_failed",
            "node_id": "phase0_node",
            "activation_id": "activation_phase0",
            "attempt_no": 1,
            "elapsed_ms": 23,
            "failure": failure()
        }),
        json!({ "type": "run_completed" }),
        json!({ "type": "run_failed", "failure": failure() }),
        json!({
            "type": "run_cancelled",
            "failure": { "kind": "stop", "code": "RUN_CANCELLED" }
        }),
        json!({
            "type": "run_interrupted",
            "failure": { "kind": "stop", "code": "RUN_INTERRUPTED" }
        }),
    ])
}

fn assert_public_event_kind_is_known(kind: PublicEventKind) {
    match kind {
        PublicEventKind::RunCreated
        | PublicEventKind::RunStarted
        | PublicEventKind::OperationStarted
        | PublicEventKind::OperationCompleted
        | PublicEventKind::OperationFailed
        | PublicEventKind::RunCompleted
        | PublicEventKind::RunFailed
        | PublicEventKind::RunCancelled
        | PublicEventKind::RunInterrupted => {}
    }
}

fn public_response(status: &str) -> Value {
    json!({
        "id": "response_phase0",
        "object": "response",
        "status": status,
        "output": [],
        "usage": null,
        "error": if status == "failed" {
            json!({ "code": "MODEL_FAILED", "message": "model failed", "param": null })
        } else {
            Value::Null
        }
    })
}

fn workflow_failure() -> Value {
    json!({
        "run_id": "run_phase0_wire",
        "error": { "code": "WORKFLOW_FAILED", "message": "workflow failed" },
        "tool_results": [],
        "retrievals": [],
        "usage_status": "partial"
    })
}

fn response_stream_events() -> Vec<ResponseStreamEvent> {
    deserialize_values(vec![
        json!({
            "type": "response.created",
            "sequence_number": 0,
            "response": public_response("in_progress")
        }),
        json!({
            "type": "response.in_progress",
            "sequence_number": 1,
            "response": public_response("in_progress")
        }),
        json!({
            "type": "response.output_item.added",
            "sequence_number": 2,
            "output_index": 0,
            "item": {
                "type": "message",
                "id": "message_phase0",
                "status": "in_progress",
                "role": "assistant",
                "content": []
            }
        }),
        json!({
            "type": "response.content_part.added",
            "sequence_number": 3,
            "item_id": "message_phase0",
            "output_index": 0,
            "content_index": 0,
            "part": { "type": "output_text", "text": "", "annotations": [] }
        }),
        json!({
            "type": "response.output_text.delta",
            "sequence_number": 4,
            "item_id": "message_phase0",
            "output_index": 0,
            "content_index": 0,
            "delta": "frozen delta"
        }),
        json!({
            "type": "response.output_text.done",
            "sequence_number": 5,
            "item_id": "message_phase0",
            "output_index": 0,
            "content_index": 0,
            "text": "complete"
        }),
        json!({
            "type": "response.content_part.done",
            "sequence_number": 6,
            "item_id": "message_phase0",
            "output_index": 0,
            "content_index": 0,
            "part": {
                "type": "output_text",
                "text": "complete",
                "annotations": [{ "kind": "citation" }]
            }
        }),
        json!({
            "type": "response.function_call_arguments.delta",
            "sequence_number": 7,
            "item_id": "function_phase0",
            "output_index": 1,
            "delta": "{\"query\":"
        }),
        json!({
            "type": "response.function_call_arguments.done",
            "sequence_number": 8,
            "item_id": "function_phase0",
            "output_index": 1,
            "name": "lookup",
            "arguments": "{\"query\":\"phase0\"}"
        }),
        json!({
            "type": "response.output_item.done",
            "sequence_number": 9,
            "output_index": 1,
            "item": {
                "type": "function_call",
                "id": "function_phase0",
                "status": "completed",
                "call_id": "call_phase0",
                "name": "lookup",
                "arguments": "{\"query\":\"phase0\"}"
            }
        }),
        json!({
            "type": "response.file_search_call.in_progress",
            "sequence_number": 10,
            "item_id": "search_phase0",
            "output_index": 2
        }),
        json!({
            "type": "response.file_search_call.searching",
            "sequence_number": 11,
            "item_id": "search_phase0",
            "output_index": 2
        }),
        json!({
            "type": "response.file_search_call.completed",
            "sequence_number": 12,
            "item_id": "search_phase0",
            "output_index": 2
        }),
        json!({
            "type": "response.completed",
            "sequence_number": 13,
            "response": public_response("completed"),
            "workflow": {
                "run_id": "run_phase0_wire",
                "result": { "answer": "complete" },
                "tool_results": [],
                "retrievals": [],
                "usage_status": "complete"
            }
        }),
        json!({
            "type": "response.failed",
            "sequence_number": 14,
            "response": public_response("failed"),
            "workflow": workflow_failure()
        }),
        json!({
            "type": "error",
            "sequence_number": 15,
            "code": "STREAM_ERROR",
            "message": "stream failed",
            "param": "response"
        }),
        json!({
            "type": "workflow.tool.started",
            "sequence_number": 16,
            "call_id": "call_phase0",
            "tool_name": "lookup",
            "arguments": { "query": "phase0" }
        }),
        json!({
            "type": "workflow.tool.completed",
            "sequence_number": 17,
            "call_id": "call_phase0",
            "tool_name": "lookup",
            "content": [{ "type": "output_text", "text": "tool result" }]
        }),
        json!({
            "type": "workflow.tool.failed",
            "sequence_number": 18,
            "call_id": "call_phase0",
            "tool_name": "lookup",
            "error": { "code": "TOOL_FAILED", "message": "tool failed" }
        }),
        json!({
            "type": "workflow.retrieval.completed",
            "sequence_number": 19,
            "retrieval_id": "retrieval_phase0",
            "query": "phase0",
            "results": [{
                "id": "result_phase0",
                "title": "Phase 0",
                "uri": "https://example.test/phase0",
                "score": 0.75,
                "snippet": "compatibility baseline",
                "metadata": { "source": "fixture" }
            }]
        }),
        json!({
            "type": "workflow.stream.gap",
            "sequence_number": 20,
            "item_id": "message_phase0",
            "attempt_no": 2,
            "missing_from": 8,
            "missing_to": 10,
            "unknown_tail": false,
            "action": "discard_provisional_item"
        }),
        json!({
            "type": "workflow.response.timed_out",
            "sequence_number": 21,
            "response": public_response("failed"),
            "workflow": workflow_failure()
        }),
        json!({
            "type": "workflow.response.cancelled",
            "sequence_number": 22,
            "response": public_response("cancelled"),
            "workflow": {
                "run_id": "run_phase0_wire",
                "reason": "cancelled",
                "tool_results": [],
                "retrievals": [],
                "usage_status": "partial"
            }
        }),
        json!({
            "type": "workflow.response.interrupted",
            "sequence_number": 23,
            "response": public_response("incomplete"),
            "workflow": {
                "run_id": "run_phase0_wire",
                "reason": "interrupted",
                "tool_results": [],
                "retrievals": [],
                "usage_status": "unavailable"
            }
        }),
    ])
}

fn assert_response_stream_event_type_is_known(event_type: ResponseStreamEventType) {
    match event_type {
        ResponseStreamEventType::ResponseCreated
        | ResponseStreamEventType::ResponseInProgress
        | ResponseStreamEventType::ResponseOutputItemAdded
        | ResponseStreamEventType::ResponseContentPartAdded
        | ResponseStreamEventType::ResponseOutputTextDelta
        | ResponseStreamEventType::ResponseOutputTextDone
        | ResponseStreamEventType::ResponseContentPartDone
        | ResponseStreamEventType::ResponseFunctionCallArgumentsDelta
        | ResponseStreamEventType::ResponseFunctionCallArgumentsDone
        | ResponseStreamEventType::ResponseOutputItemDone
        | ResponseStreamEventType::ResponseFileSearchCallInProgress
        | ResponseStreamEventType::ResponseFileSearchCallSearching
        | ResponseStreamEventType::ResponseFileSearchCallCompleted
        | ResponseStreamEventType::ResponseCompleted
        | ResponseStreamEventType::ResponseFailed
        | ResponseStreamEventType::Error
        | ResponseStreamEventType::WorkflowToolStarted
        | ResponseStreamEventType::WorkflowToolCompleted
        | ResponseStreamEventType::WorkflowToolFailed
        | ResponseStreamEventType::WorkflowRetrievalCompleted
        | ResponseStreamEventType::WorkflowStreamGap
        | ResponseStreamEventType::WorkflowResponseTimedOut
        | ResponseStreamEventType::WorkflowResponseCancelled
        | ResponseStreamEventType::WorkflowResponseInterrupted => {}
    }
}

fn legacy_run_event_types() -> [RunEventType; 9] {
    [
        RunEventType::RunCreated,
        RunEventType::RunStarted,
        RunEventType::OperationStarted,
        RunEventType::OperationCompleted,
        RunEventType::OperationFailed,
        RunEventType::RunCompleted,
        RunEventType::RunFailed,
        RunEventType::RunCancelled,
        RunEventType::RunInterrupted,
    ]
}

fn assert_legacy_run_event_type_is_known(event_type: RunEventType) {
    match event_type {
        RunEventType::RunCreated
        | RunEventType::RunStarted
        | RunEventType::OperationStarted
        | RunEventType::OperationCompleted
        | RunEventType::OperationFailed
        | RunEventType::RunCompleted
        | RunEventType::RunFailed
        | RunEventType::RunCancelled
        | RunEventType::RunInterrupted => {}
    }
}

fn legacy_run_events() -> Vec<RunEvent> {
    let scope = RunEventScope::for_run(
        "request_phase0",
        "run_phase0_wire",
        "fixture-agent",
        "deployment_phase0",
    );
    legacy_run_event_types()
        .into_iter()
        .enumerate()
        .map(|(index, event_type)| {
            let data = json!({ "kind": event_type.as_str() });
            if matches!(
                event_type,
                RunEventType::OperationFailed
                    | RunEventType::RunFailed
                    | RunEventType::RunCancelled
                    | RunEventType::RunInterrupted
            ) {
                RunEvent::error_at(
                    event_type,
                    index as u64 + 1,
                    scope.clone(),
                    fixed_time(),
                    "PHASE0_FAILURE",
                    "phase 0 representative failure",
                    data,
                )
            } else {
                RunEvent::ok_at(
                    event_type,
                    index as u64 + 1,
                    scope.clone(),
                    fixed_time(),
                    data,
                )
            }
        })
        .collect()
}

fn history_baseline() -> Value {
    let output = || RunOutput {
        content: Some("phase 0 answer".to_owned()),
        format: Some("text".to_owned()),
        data: json!({ "answer": "phase 0" }),
    };
    let failure = || RunFailure {
        kind: FailureKind::Workflow,
        code: "WORKFLOW_FAILED".to_owned(),
        message: "workflow failed".to_owned(),
    };
    let cancelled = || StopError {
        code: "RUN_CANCELLED".to_owned(),
        message: "run cancelled".to_owned(),
    };
    let interrupted = || StopError {
        code: "RUN_INTERRUPTED".to_owned(),
        message: "run interrupted".to_owned(),
    };

    let statuses = [
        RunStatus::Created,
        RunStatus::Running,
        RunStatus::Completed,
        RunStatus::Failed,
        RunStatus::Cancelled,
        RunStatus::Interrupted,
    ];
    let attachments = [RunAttachment::Attached, RunAttachment::Detached];
    let lifecycles = [
        HistoryRunLifecycle::Created,
        HistoryRunLifecycle::Running,
        HistoryRunLifecycle::Completed { output: output() },
        HistoryRunLifecycle::Failed { error: failure() },
        HistoryRunLifecycle::Cancelled { error: cancelled() },
        HistoryRunLifecycle::Interrupted {
            error: interrupted(),
        },
    ];
    let summary_lifecycles = [
        RunSummaryLifecycle::Created,
        RunSummaryLifecycle::Running,
        RunSummaryLifecycle::Completed,
        RunSummaryLifecycle::Failed { error: failure() },
        RunSummaryLifecycle::Cancelled { error: cancelled() },
        RunSummaryLifecycle::Interrupted {
            error: interrupted(),
        },
    ];

    assert_eq!(statuses.len(), 6);
    assert_eq!(attachments.len(), 2);
    assert_eq!(lifecycles.len(), 6);
    assert_eq!(summary_lifecycles.len(), 6);
    for status in statuses {
        match status {
            RunStatus::Created
            | RunStatus::Running
            | RunStatus::Completed
            | RunStatus::Failed
            | RunStatus::Cancelled
            | RunStatus::Interrupted => {}
        }
    }
    for attachment in attachments {
        match attachment {
            RunAttachment::Attached | RunAttachment::Detached => {}
        }
    }
    for lifecycle in &lifecycles {
        match lifecycle {
            HistoryRunLifecycle::Created
            | HistoryRunLifecycle::Running
            | HistoryRunLifecycle::Completed { .. }
            | HistoryRunLifecycle::Failed { .. }
            | HistoryRunLifecycle::Cancelled { .. }
            | HistoryRunLifecycle::Interrupted { .. } => {}
        }
    }
    for lifecycle in &summary_lifecycles {
        match lifecycle {
            RunSummaryLifecycle::Created
            | RunSummaryLifecycle::Running
            | RunSummaryLifecycle::Completed
            | RunSummaryLifecycle::Failed { .. }
            | RunSummaryLifecycle::Cancelled { .. }
            | RunSummaryLifecycle::Interrupted { .. } => {}
        }
    }

    json!({
        "run_statuses": statuses,
        "run_attachments": attachments,
        "run_lifecycles": lifecycles,
        "run_summary_lifecycles": summary_lifecycles,
    })
}

fn outcome_baseline() -> Value {
    let outcomes = [
        TerminalOutcome::Success {
            output: RunOutput {
                content: Some("phase 0 answer".to_owned()),
                format: Some("text".to_owned()),
                data: json!({ "answer": "phase 0" }),
            },
        },
        TerminalOutcome::Failure {
            error: WorkflowError {
                code: "WORKFLOW_FAILED".to_owned(),
                message: "workflow failed".to_owned(),
            },
        },
    ];
    let failure_kinds = [
        FailureKind::Workflow,
        FailureKind::Operation,
        FailureKind::Timeout,
        FailureKind::Infrastructure,
    ];

    assert_eq!(outcomes.len(), 2);
    assert_eq!(failure_kinds.len(), 4);
    for outcome in &outcomes {
        match outcome {
            TerminalOutcome::Success { .. } | TerminalOutcome::Failure { .. } => {}
        }
    }
    for kind in failure_kinds {
        match kind {
            FailureKind::Workflow
            | FailureKind::Operation
            | FailureKind::Timeout
            | FailureKind::Infrastructure => {}
        }
    }

    json!({
        "terminal_outcomes": outcomes,
        "failure_kinds": failure_kinds,
    })
}

fn api_baseline() -> Value {
    let cursor = ConversationMessageCursor {
        message_order: 42,
        message_id: "message_phase0".to_owned(),
    };
    json!({
        "run_persistence_capabilities": [
            RunPersistenceCapability::FULL,
            RunPersistenceCapability::FULL_CONVERSATION,
            RunPersistenceCapability::TERMINAL_ONLY,
        ],
        "conversation_message_cursor": cursor.encode(),
    })
}

fn actual_baseline() -> Value {
    let source = include_str!("fixtures/dsl/linear.yaml");
    let plan = compile_source(
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("dsl_fixture_revision").unwrap(),
            "fixture.yaml",
            source,
        ),
    )
    .unwrap();
    let plan_json = serde_json::to_vec(&plan).unwrap();

    let execution_payloads = execution_event_payloads();
    assert_eq!(ExecutionEventKind::ALL.len(), 39);
    assert_eq!(execution_payloads.len(), 39);
    assert_eq!(
        execution_payloads
            .iter()
            .map(ExecutionEventPayload::kind)
            .collect::<Vec<_>>(),
        ExecutionEventKind::ALL.to_vec()
    );
    for payload in &execution_payloads {
        PendingExecutionEvent::new(context_for(payload.kind()), payload.clone()).unwrap();
    }

    let public_payloads = public_event_payloads();
    assert_eq!(PublicEventKind::ALL.len(), 9);
    assert_eq!(public_payloads.len(), 9);
    PublicEventKind::ALL
        .iter()
        .copied()
        .for_each(assert_public_event_kind_is_known);
    assert_eq!(
        public_payloads
            .iter()
            .map(PublicEventPayload::kind)
            .collect::<Vec<_>>(),
        PublicEventKind::ALL.to_vec()
    );

    let response_events = response_stream_events();
    assert_eq!(ResponseStreamEventType::ALL.len(), 24);
    assert_eq!(response_events.len(), 24);
    ResponseStreamEventType::ALL
        .into_iter()
        .for_each(assert_response_stream_event_type_is_known);
    assert_eq!(
        response_events
            .iter()
            .map(ResponseStreamEvent::event_type)
            .collect::<Vec<_>>(),
        ResponseStreamEventType::ALL.to_vec()
    );

    let legacy_events = legacy_run_events();
    assert_eq!(legacy_events.len(), 9);
    legacy_run_event_types()
        .into_iter()
        .for_each(assert_legacy_run_event_type_is_known);
    assert_eq!(
        legacy_events
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        legacy_run_event_types().to_vec()
    );

    let execution_event_seed = json!({
        "schema_version": 2,
        "event_id": "event_00000000000000000000000000000001",
        "run_id": "run_phase0_wire",
        "transition_key": concat!("transition_", "0000000000000000000000000000000000000000000000000000000000000000"),
        "intent_hash": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        "seq": 1,
        "occurred_at": "2026-07-18T01:02:03Z",
        "kind": "run.created",
        "node_id": null,
        "scope_instance_id": null,
        "activation_id": null,
        "attempt_no": null,
        "causation_event_id": null,
        "payload": execution_payloads[0]
    });
    let execution_event =
        serde_json::from_value::<ExecutionEventEnvelope>(execution_event_seed).unwrap();

    // Store commit metadata has no public constructor. A typed round trip of a
    // fixed, valid wire seed still freezes the complete public envelope bytes.
    let public_event_seed = json!({
        "schema_version": 1,
        "public_event_id": "public_event_00000000000000000000000000000001",
        "run_id": "run_phase0_wire",
        "causation_event_id": "event_00000000000000000000000000000001",
        "seq": 1,
        "occurred_at": "2026-07-18T01:02:03Z",
        "kind": "run.created",
        "payload": public_payloads[0]
    });
    let public_event = serde_json::from_value::<PublicEventEnvelope>(public_event_seed).unwrap();

    json!({
        "plan": {
            "fixture": "tests/fixtures/dsl/linear.yaml",
            "json_length": plan_json.len(),
            "json_sha256": sha256(&plan_json),
            "semantic_hash": plan.semantic_hash().as_str(),
        },
        "execution_event_kinds": ExecutionEventKind::ALL,
        "execution_event_payloads": execution_payloads,
        "execution_event": execution_event,
        "public_event_kinds": PublicEventKind::ALL,
        "public_event_payloads": public_payloads,
        "public_event": public_event,
        "legacy_run_event_types": legacy_run_event_types(),
        "legacy_run_events": legacy_events,
        "response_stream_event_types": ResponseStreamEventType::ALL,
        "response_stream_events": response_events,
        "api": api_baseline(),
        "history": history_baseline(),
        "outcome": outcome_baseline(),
    })
}

#[test]
fn phase0_plan_and_wire_json_match_checked_in_golden() {
    let actual = actual_baseline();
    if std::env::var_os("PRINT_PHASE0_WIRE_BASELINE").is_some() {
        println!("{}", serde_json::to_string_pretty(&actual).unwrap());
        return;
    }
    let expected: Value = serde_json::from_str(include_str!("baselines/phase0-wire.json")).unwrap();
    assert_eq!(actual, expected);
}
