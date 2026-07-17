use chrono::{TimeZone, Utc};
use insight_agent_platform::{
    events::protocol::{RunEvent, RunEventScope, RunEventType},
    history::types::{
        NewRun, RunAttachment, RunLifecycle, RunRecord, RunStatus, RunSummary, RunTerminal,
        StopError, TerminalUpdate,
    },
    outcome::{FailureKind, RunFailure, RunOutput},
};
use serde_json::json;

fn at(second: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 10, 0, 0, second).unwrap()
}

fn scope() -> RunEventScope {
    RunEventScope {
        request_id: "req_1".to_string(),
        run_id: "run_1".to_string(),
        agent_id: "researcher".to_string(),
        agent_version: "sha256:abc".to_string(),
    }
}

#[test]
fn operation_event_serializes_the_exact_formal_v2_envelope() {
    let event = RunEvent::ok_at(
        RunEventType::OperationCompleted,
        4,
        scope(),
        at(4),
        json!({
            "operation_id":"/workflow/plan#Authored",
            "operation_type":"ai.chat",
            "attempt":1,
            "elapsed_ms":12,
            "output_bytes":128
        }),
    );

    let value = serde_json::to_value(event).unwrap();

    assert_eq!(value["schema_version"], 2);
    assert_eq!(value["type"], "operation.completed");
    assert_eq!(value["seq"], 4);
    assert_eq!(value["request_id"], "req_1");
    assert_eq!(value["run_id"], "run_1");
    assert_eq!(value["agent_id"], "researcher");
    assert_eq!(value["agent_version"], "sha256:abc");
    assert_eq!(value["time"], "2026-07-10T00:00:04Z");
    assert_eq!(value["code"], "OK");
    assert_eq!(value["message"], "ok");
    assert_eq!(
        value["data"],
        json!({
            "operation_id":"/workflow/plan#Authored",
            "operation_type":"ai.chat",
            "attempt":1,
            "elapsed_ms":12,
            "output_bytes":128
        })
    );
    assert_eq!(value.as_object().unwrap().len(), 11);
}

#[test]
fn run_and_operation_error_events_keep_stable_string_codes() {
    let run_event = RunEvent::ok_at(RunEventType::RunStarted, 2, scope(), at(2), json!({}));
    let value = serde_json::to_value(run_event).unwrap();
    assert_eq!(value["type"], "run.started");
    assert!(value.get("node_id").is_none());

    let failed = RunEvent::error_at(
        RunEventType::OperationFailed,
        3,
        scope(),
        at(3),
        "UPSTREAM_FAILURE",
        "operation failed",
        json!({
            "operation_id":"/workflow/answer#Authored",
            "operation_type":"ai.chat",
            "attempt":1,
            "elapsed_ms":7,
            "error_kind":"operation"
        }),
    );
    let value = serde_json::to_value(failed).unwrap();
    assert_eq!(value["code"], "UPSTREAM_FAILURE");
    assert_eq!(value["message"], "operation failed");
}

#[test]
fn formal_event_type_set_is_exact_and_uses_dotted_names() {
    let types = [
        RunEventType::RunCreated,
        RunEventType::RunStarted,
        RunEventType::OperationStarted,
        RunEventType::OperationCompleted,
        RunEventType::OperationFailed,
        RunEventType::RunCompleted,
        RunEventType::RunFailed,
        RunEventType::RunCancelled,
        RunEventType::RunInterrupted,
    ];
    let serialized = types
        .iter()
        .copied()
        .map(|event_type| serde_json::to_value(event_type).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        serialized,
        vec![
            json!("run.created"),
            json!("run.started"),
            json!("operation.started"),
            json!("operation.completed"),
            json!("operation.failed"),
            json!("run.completed"),
            json!("run.failed"),
            json!("run.cancelled"),
            json!("run.interrupted"),
        ]
    );
    for (event_type, serialized) in types.into_iter().zip(serialized) {
        assert_eq!(serialized, json!(event_type.as_str()));
        assert_eq!(
            RunEventType::parse(serialized.as_str().unwrap()),
            Some(event_type)
        );
        assert_eq!(
            serde_json::from_value::<RunEventType>(serialized).unwrap(),
            event_type
        );
    }
    for legacy in [
        "node.started",
        "content.delta",
        "operation.content_delta",
        "node.completed",
        "node.failed",
        "branch.started",
        "branch.completed",
        "branch.failed",
    ] {
        assert_eq!(RunEventType::parse(legacy), None);
    }
}

#[test]
fn statuses_and_attachments_serialize_as_snake_case() {
    let statuses = [
        RunStatus::Created,
        RunStatus::Running,
        RunStatus::Completed,
        RunStatus::Failed,
        RunStatus::Cancelled,
        RunStatus::Interrupted,
    ];
    let values = statuses
        .into_iter()
        .map(|status| serde_json::to_value(status).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![
            json!("created"),
            json!("running"),
            json!("completed"),
            json!("failed"),
            json!("cancelled"),
            json!("interrupted"),
        ]
    );
    assert_eq!(
        serde_json::to_value(RunAttachment::Attached).unwrap(),
        json!("attached")
    );
    assert_eq!(
        serde_json::to_value(RunAttachment::Detached).unwrap(),
        json!("detached")
    );
}

#[test]
fn only_completed_failed_cancelled_and_interrupted_are_terminal() {
    assert!(!RunStatus::Created.is_terminal());
    assert!(!RunStatus::Running.is_terminal());
    assert!(RunStatus::Completed.is_terminal());
    assert!(RunStatus::Failed.is_terminal());
    assert!(RunStatus::Cancelled.is_terminal());
    assert!(RunStatus::Interrupted.is_terminal());
}

#[test]
fn run_lifecycle_serializes_mutually_exclusive_terminal_shapes() {
    let completed = RunLifecycle::Completed {
        output: RunOutput {
            content: Some("answer".into()),
            format: Some("text".into()),
            data: json!({"answer":"answer"}),
        },
    };
    assert_eq!(
        serde_json::to_value(completed).unwrap(),
        json!({
            "status":"completed",
            "output":{"content":"answer","format":"text","data":{"answer":"answer"}}
        })
    );

    let failed = RunLifecycle::Failed {
        error: RunFailure {
            kind: FailureKind::Workflow,
            code: "WORKFLOW_REJECTED".into(),
            message: "workflow rejected".into(),
        },
    };
    assert_eq!(
        serde_json::to_value(failed).unwrap(),
        json!({
            "status":"failed",
            "error":{"kind":"workflow","code":"WORKFLOW_REJECTED","message":"workflow rejected"}
        })
    );
}

#[test]
fn every_run_lifecycle_round_trips_through_flattened_record_and_summary_shapes() {
    let lifecycles = vec![
        RunLifecycle::Created,
        RunLifecycle::Running,
        RunLifecycle::Completed {
            output: RunOutput {
                content: Some("answer".into()),
                format: Some("text".into()),
                data: json!({"answer":"answer"}),
            },
        },
        RunLifecycle::Failed {
            error: RunFailure {
                kind: FailureKind::Workflow,
                code: "WORKFLOW_REJECTED".into(),
                message: "workflow rejected".into(),
            },
        },
        RunLifecycle::Cancelled {
            error: StopError {
                code: "RUN_CANCELLED".into(),
                message: "run cancelled".into(),
            },
        },
        RunLifecycle::Interrupted {
            error: StopError {
                code: "RUN_INTERRUPTED".into(),
                message: "run interrupted".into(),
            },
        },
    ];

    for lifecycle in lifecycles {
        let record = RunRecord {
            run_id: "run_round_trip".into(),
            request_id: "req_round_trip".into(),
            agent_id: "agent_round_trip".into(),
            agent_version: "sha256:round-trip".into(),
            attachment: RunAttachment::Detached,
            started_at: Some(at(1)),
            ended_at: lifecycle.status().is_terminal().then(|| at(2)),
            updated_at: at(2),
            input_summary: json!({"keys":[],"serialized_bytes":2}),
            lifecycle,
        };

        let lifecycle_value = serde_json::to_value(&record.lifecycle).unwrap();
        assert_eq!(
            serde_json::from_value::<RunLifecycle>(lifecycle_value).unwrap(),
            record.lifecycle
        );

        let record_value = serde_json::to_value(&record).unwrap();
        assert_eq!(
            serde_json::from_value::<RunRecord>(record_value).unwrap(),
            record
        );

        let summary = RunSummary::from(&record);
        let summary_value = serde_json::to_value(&summary).unwrap();
        assert_eq!(
            serde_json::from_value::<RunSummary>(summary_value).unwrap(),
            summary
        );
    }
}

#[test]
fn terminal_update_derives_status_from_the_terminal_variant() {
    let update = TerminalUpdate::new(
        "run_1",
        at(10),
        RunTerminal::Failed {
            error: RunFailure {
                kind: FailureKind::Operation,
                code: "OPERATION_FAILED".into(),
                message: "operation failed".into(),
            },
        },
    );
    assert_eq!(update.status(), RunStatus::Failed);
}

#[test]
fn formal_history_records_preserve_version_attachment_and_sanitized_terminal_data() {
    let new_run = NewRun {
        run_id: "run_1".to_string(),
        request_id: "req_1".to_string(),
        agent_id: "researcher".to_string(),
        agent_version: "sha256:abc".to_string(),
        attachment: RunAttachment::Detached,
        created_at: at(0),
        input_summary: json!({"keys":["question"], "serialized_bytes":24}),
    };
    let record = RunRecord {
        run_id: new_run.run_id.clone(),
        request_id: new_run.request_id.clone(),
        agent_id: new_run.agent_id.clone(),
        agent_version: new_run.agent_version.clone(),
        attachment: new_run.attachment,
        lifecycle: RunLifecycle::Failed {
            error: RunFailure {
                kind: FailureKind::Infrastructure,
                code: "UPSTREAM_FAILURE".to_string(),
                message: "model request failed".to_string(),
            },
        },
        started_at: Some(at(1)),
        ended_at: Some(at(2)),
        updated_at: at(2),
        input_summary: new_run.input_summary.clone(),
    };
    let summary = RunSummary::from(&record);
    let completed_summary = RunSummary::from(&RunRecord {
        lifecycle: RunLifecycle::Completed {
            output: RunOutput {
                content: Some("must be omitted from summary".to_string()),
                format: Some("text".to_string()),
                data: json!({"private":"terminal output"}),
            },
        },
        ..record.clone()
    });
    assert_eq!(summary.agent_version, "sha256:abc");
    assert_eq!(summary.attachment, RunAttachment::Detached);
    assert_eq!(summary.status(), RunStatus::Failed);
    assert_eq!(
        serde_json::to_value(&summary).unwrap()["error"]["code"],
        "UPSTREAM_FAILURE"
    );
    let completed_summary = serde_json::to_value(completed_summary).unwrap();
    assert_eq!(completed_summary["status"], "completed");
    assert!(completed_summary.get("output").is_none());
    assert!(completed_summary.get("error").is_none());
}
