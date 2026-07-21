# Terminal Boundary Review Corrections Implementation Plan

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

> **Historical / superseded:** this plan targeted the removed graph/node runtime. See [DSL vNext Region/SSA Design](../specs/2026-07-16-dsl-vnext-region-ssa-design.md) for the current runtime and terminal contracts.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the last three internal terminal-consistency gaps by deriving terminal events from typed updates, distinguishing durable recovery winners, and making infrastructure impossible in settled branch results.

**Architecture:** EventHub receives only `RunEventScope + TerminalUpdate`, projects the complete terminal event, and returns `TerminalResolution::{Requested, Authoritative}`. Branch results use a dedicated three-variant `BranchFailureKind`; Join counts that exhaustive type and fails closed if its count identity is ever violated.

**Tech Stack:** Rust 2024, Tokio, Serde/serde_json, tracing, SQLx repository abstractions, Cargo integration tests.

## Global Constraints

- No public DSL or HTTP/SSE JSON shape changes.
- No migration or persisted-row layout changes.
- No compatibility overload may retain independent terminal event type/code/message/data arguments.
- Event timestamp, type, code, message, and data are derived from the exact `TerminalUpdate` persisted with the event.
- `FailureKind` remains run-wide; `BranchFailureKind` contains exactly Workflow, Node, and Timeout.
- Stop and infrastructure remain outer scheduler failures and never enter Join results.
- Graph validation, `END_REQUIRED` precedence, Select semantics, Agents, README, and the `spin@0.9.8` policy exception are out of scope.
- Use strict RED/GREEN evidence and rerun the complete final gate after the last correction.

---

### Task 1: Make TerminalUpdate the only EventHub terminal truth

**Files:**
- Modify: `src/events/hub.rs`
- Modify: `src/runtime/coordinator.rs`
- Modify: `src/runtime/service.rs`
- Modify: `tests/event_hub.rs`
- Modify: `tests/run_coordinator.rs`
- Test: `tests/run_service.rs`

**Interfaces:**
- Consumes: `RunEventScope`, `TerminalUpdate { run_id, ended_at, terminal }`, `RunTerminal`, `RunEvent`.
- Produces: `TerminalResolution::{Requested(RunEvent), Authoritative(RunEvent)}`; `EventHub::publish_terminal(scope, update)`; `EventHub::recover_terminal(scope, update)`.

- [ ] **Step 1: Write EventHub projection and resolution tests before production edits**

In `tests/event_hub.rs`, import `StopError` and `TerminalResolution`. Replace the old mismatch test with the remaining identity test and add a projection table. Use this helper to unwrap the new result without hiding the variant:

```rust
fn requested_event(resolution: TerminalResolution) -> RunEvent {
    match resolution {
        TerminalResolution::Requested(event) => event,
        TerminalResolution::Authoritative(event) => {
            panic!("new terminal unexpectedly lost to {event:?}")
        }
    }
}
```

Add a table-driven test whose inputs are complete typed updates:

```rust
#[tokio::test]
async fn terminal_events_are_projected_only_from_typed_updates() {
    let cases = vec![
        (
            TerminalUpdate::new(
                "run_completed",
                at(10),
                RunTerminal::Completed {
                    output: RunOutput {
                        content: None,
                        format: None,
                        data: json!({"answer": 42}),
                    },
                },
            ),
            RunEventType::RunCompleted,
            "OK",
            "ok",
            json!({"data":{"answer":42}}),
        ),
        (
            TerminalUpdate::new(
                "run_workflow",
                at(11),
                RunTerminal::Failed {
                    error: RunFailure {
                        kind: FailureKind::Workflow,
                        code: "WORKFLOW_REJECTED".into(),
                        message: "workflow rejected".into(),
                    },
                },
            ),
            RunEventType::RunFailed,
            "WORKFLOW_REJECTED",
            "workflow rejected",
            json!({"kind":"workflow"}),
        ),
        (
            TerminalUpdate::new(
                "run_node",
                at(12),
                RunTerminal::Failed {
                    error: RunFailure {
                        kind: FailureKind::Node,
                        code: "NODE_FAILED".into(),
                        message: "node failed".into(),
                    },
                },
            ),
            RunEventType::RunFailed,
            "NODE_FAILED",
            "node failed",
            json!({"kind":"node"}),
        ),
        (
            TerminalUpdate::new(
                "run_timeout",
                at(13),
                RunTerminal::Failed {
                    error: RunFailure {
                        kind: FailureKind::Timeout,
                        code: "RUN_TIMEOUT".into(),
                        message: "run timed out".into(),
                    },
                },
            ),
            RunEventType::RunFailed,
            "RUN_TIMEOUT",
            "run timed out",
            json!({"kind":"timeout"}),
        ),
        (
            TerminalUpdate::new(
                "run_infrastructure",
                at(14),
                RunTerminal::Failed {
                    error: RunFailure {
                        kind: FailureKind::Infrastructure,
                        code: "INFRASTRUCTURE_FAILURE".into(),
                        message: "runtime infrastructure failed".into(),
                    },
                },
            ),
            RunEventType::RunFailed,
            "INFRASTRUCTURE_FAILURE",
            "runtime infrastructure failed",
            json!({"kind":"infrastructure"}),
        ),
        (
            TerminalUpdate::new(
                "run_cancelled",
                at(15),
                RunTerminal::Cancelled {
                    error: StopError {
                        code: "RUN_CANCELLED".into(),
                        message: "run cancelled".into(),
                    },
                },
            ),
            RunEventType::RunCancelled,
            "RUN_CANCELLED",
            "run cancelled",
            json!({}),
        ),
        (
            TerminalUpdate::new(
                "run_interrupted",
                at(16),
                RunTerminal::Interrupted {
                    error: StopError {
                        code: "RUN_INTERRUPTED".into(),
                        message: "run interrupted".into(),
                    },
                },
            ),
            RunEventType::RunInterrupted,
            "RUN_INTERRUPTED",
            "run interrupted",
            json!({}),
        ),
    ];

    for (update, event_type, code, message, data) in cases {
        let run_id = update.run_id.clone();
        let ended_at = update.ended_at;
        let repository = Arc::new(MemoryRepository::default());
        let hub = EventHub::new(repository, config(8));
        let event = requested_event(
            hub.publish_terminal(scope_for(&run_id, None), update)
                .await
                .unwrap(),
        );
        assert_eq!(event.event_type, event_type);
        assert_eq!(event.timestamp, ended_at);
        assert_eq!(event.code, code);
        assert_eq!(event.message, message);
        assert_eq!(event.data, data);
    }
}
```

Change `terminal_publish_rejects_mismatched_typed_request_before_storage` so it passes only `scope(None)` and `failed_update("different_run", 10)`, then retains the assertions that no terminal update, event, or live state was created. Delete the event-type mismatch case because the new API cannot express it.

- [ ] **Step 2: Add the same-event-type durable-winner logging regression**

In `tests/run_coordinator.rs`, correct the in-memory repository contract so `recover_run` returns the event actually stored by `finish_run`, not its attempted local copy:

```rust
self.finish_run(update, terminal).await?;
self.events
    .lock()
    .await
    .last()
    .cloned()
    .ok_or_else(|| HistoryError::new(
        "TEST_TERMINAL_EVENT_MISSING",
        "synthetic recovery did not store a terminal event",
    ))
```

Extend the single global-subscriber test `run_finished_log_uses_durable_failed_terminal_when_completion_loses_race` with a second repository/coordinator execution:

```rust
let repository = Arc::new(MemoryRepository::default());
repository.fail_next_append.store(true, Ordering::SeqCst);
*repository.terminal_race.lock().await = Some(TerminalUpdate::new(
    RUN_ID,
    Utc::now(),
    RunTerminal::Failed {
        error: RunFailure {
            kind: FailureKind::Workflow,
            code: "DURABLE_WORKFLOW_FAILURE".into(),
            message: "durable workflow failure won recovery".into(),
        },
    },
));
let coordinator = coordinator(
    agent(
        vec![node(
            "result",
            None,
            Duration::from_secs(1),
            Behavior::Complete(RunOutput {
                content: Some("uncommitted output".into()),
                format: Some("text".into()),
                data: json!({}),
            }),
        )],
        "result",
    ),
    Arc::clone(&repository),
    true,
);
let (_, stop) = stop_pair();
assert_eq!(
    coordinator.execute(new_run(), json!({}), stop).await.unwrap(),
    RunStatus::Failed
);
let finished = recorded_info_logs(&recorded, "run.finished")
    .into_iter()
    .find(|event| event.field("error_code") == Some("DURABLE_WORKFLOW_FAILURE"))
    .expect("durable workflow run.finished log");
assert_eq!(finished.field("failure_kind"), Some("workflow"));
```

- [ ] **Step 3: Run the focused tests RED**

Run:

```bash
cargo test --locked --test event_hub terminal_events_are_projected_only_from_typed_updates
cargo test --locked --test event_hub terminal_publish_rejects_mismatched_typed_request_before_storage
cargo test --locked --test run_coordinator run_finished_log_uses_durable_failed_terminal_when_completion_loses_race -- --nocapture --test-threads=1
```

Expected: EventHub tests fail to compile because `TerminalResolution` and the two-argument terminal API do not exist. After test-only signature adaptation, the coordinator regression must fail because same-`run.failed` recovery still selects the attempted infrastructure log summary.

- [ ] **Step 4: Define the typed projection and resolution**

In `src/events/hub.rs`, add:

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum TerminalResolution {
    Requested(RunEvent),
    Authoritative(RunEvent),
}

fn validate_terminal_scope(
    scope: &RunEventScope,
    update: &TerminalUpdate,
) -> Result<(), EventError> {
    if update.run_id != scope.run_id {
        return Err(EventError::History(HistoryError::new(
            "HISTORY_EVENT_INVALID",
            "terminal event scope does not match its typed update",
        )));
    }
    Ok(())
}

fn completed_data(output: &RunOutput) -> Value {
    let mut data = serde_json::Map::from([("data".to_string(), output.data.clone())]);
    if let Some(content) = &output.content {
        data.insert("content".to_string(), Value::String(content.clone()));
    }
    if let Some(format) = &output.format {
        data.insert("format".to_string(), Value::String(format.clone()));
    }
    Value::Object(data)
}

fn terminal_event(scope: RunEventScope, seq: u64, update: &TerminalUpdate) -> RunEvent {
    match &update.terminal {
        RunTerminal::Completed { output } => RunEvent::ok_at(
            RunEventType::RunCompleted,
            seq,
            scope,
            update.ended_at,
            completed_data(output),
        ),
        RunTerminal::Failed { error } => RunEvent::error_at(
            RunEventType::RunFailed,
            seq,
            scope,
            update.ended_at,
            error.code.clone(),
            error.message.clone(),
            json!({"kind": error.kind}),
        ),
        RunTerminal::Cancelled { error } => RunEvent::error_at(
            RunEventType::RunCancelled,
            seq,
            scope,
            update.ended_at,
            error.code.clone(),
            error.message.clone(),
            json!({}),
        ),
        RunTerminal::Interrupted { error } => RunEvent::error_at(
            RunEventType::RunInterrupted,
            seq,
            scope,
            update.ended_at,
            error.code.clone(),
            error.message.clone(),
            json!({}),
        ),
    }
}

fn terminal_resolution(requested: RunEvent, authoritative: RunEvent) -> TerminalResolution {
    if requested == authoritative {
        TerminalResolution::Requested(authoritative)
    } else {
        TerminalResolution::Authoritative(authoritative)
    }
}
```

Import `RunTerminal` and `RunOutput`. Remove `validate_terminal_request` and delete independent terminal fields from `RecoveryRequest`; its `terminal_event` delegates to the shared projection.

- [ ] **Step 5: Replace EventHub terminal APIs without a compatibility overload**

Change both signatures exactly to the interfaces above. In `publish_terminal`, build `requested` after scope validation and before journal persistence. Return:

```rust
if !self.inner.journal.finish(update, requested.clone()).await? {
    // retain the existing authoritative event lookup and validation
    return Ok(terminal_resolution(requested, existing));
}
commit_live_event(&mut state, requested.clone());
drop(state);
self.isolate_run_state(&run_id, &state_handle).await;
Ok(TerminalResolution::Requested(requested))
```

In `recover_terminal_direct`, build the requested event at `state.next_seq`, pass its clone to `recover_run`, reconcile the returned authoritative event, and return:

```rust
Ok(terminal_resolution(requested, authoritative))
```

Background recovery uses the same `RecoveryRequest::terminal_event(1)` projection. Delete all comparisons that use only `event_type`.

- [ ] **Step 6: Migrate coordinator and service callers**

In `src/runtime/coordinator.rs`, stop constructing event type/code/message/data. Derive only the attempted log summary from the typed terminal:

```rust
fn terminal_log_summary_from_terminal(terminal: &RunTerminal) -> TerminalLogSummary {
    match terminal {
        RunTerminal::Completed { output } => TerminalLogSummary {
            status: RunStatus::Completed,
            output_bytes: json_size_bytes(output),
            error_code: String::new(),
            failure_kind: None,
        },
        RunTerminal::Failed { error } => TerminalLogSummary {
            status: RunStatus::Failed,
            output_bytes: 0,
            error_code: error.code.clone(),
            failure_kind: Some(error.kind),
        },
        RunTerminal::Cancelled { error } => TerminalLogSummary {
            status: RunStatus::Cancelled,
            output_bytes: 0,
            error_code: error.code.clone(),
            failure_kind: None,
        },
        RunTerminal::Interrupted { error } => TerminalLogSummary {
            status: RunStatus::Interrupted,
            output_bytes: 0,
            error_code: error.code.clone(),
            failure_kind: None,
        },
    }
}
```

Call `publish_terminal(run_scope(run), update)` and `recover_terminal(run_scope(run), update)`. Change `commit_terminal_state` to:

```rust
let durable = match resolution {
    TerminalResolution::Requested(_) => attempted,
    TerminalResolution::Authoritative(_) => {
        let record = self.repository.get_run(&run.run_id).await
            .map_err(history_error)?
            .ok_or_else(|| RunError::new(
                "RUN_NOT_FOUND",
                "run not found after terminal race",
            ))?;
        terminal_log_summary_from_record(&record)
    }
};
```

In `src/runtime/service.rs`, make `terminal_spec` return only `RunTerminal`, remove event-field derivation, and call `publish_terminal(run_scope(&run), update)`.

Update every EventHub call in `tests/event_hub.rs` and every affected assertion in `tests/run_coordinator.rs` to the two-argument API and explicit `TerminalResolution` assertions. Do not add a deprecated adapter.

- [ ] **Step 7: Run focused and owning suites GREEN**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --test event_hub
cargo test --locked --test run_coordinator -- --nocapture --test-threads=1
cargo test --locked --test run_service
cargo test --locked --test api
rg -n -U "(publish_terminal|recover_terminal)\((?:[^\n]*\n){0,5}[^)]*RunEventType::" src tests
git diff --check
```

Expected: all tests exit 0; the search has no legacy multi-source terminal call; diff check is clean.

- [ ] **Step 8: Commit the typed terminal boundary**

```bash
git add src/events/hub.rs src/runtime/coordinator.rs src/runtime/service.rs tests/event_hub.rs tests/run_coordinator.rs tests/run_service.rs
git diff --cached --check
git commit -m "refactor: derive terminal events from typed updates"
```

---

### Task 2: Make branch failure taxonomy unrepresentable outside three origins

**Files:**
- Modify: `src/runtime/state.rs`
- Modify: `src/runtime/mod.rs`
- Modify: `src/runtime/scheduler.rs`
- Modify: `src/nodes/join.rs`
- Modify: `tests/fork_join_nodes.rs`
- Test: `tests/run_scheduler.rs`

**Interfaces:**
- Consumes: authored workflow End, `RunErrorKind::{Node, Timeout}`, outer Stop/Infrastructure propagation.
- Produces: `BranchFailureKind::{Workflow, Node, Timeout}` and `BranchError { kind: BranchFailureKind, code, message }`.

- [ ] **Step 1: Write the branch-kind contract tests before production edits**

In `tests/fork_join_nodes.rs`, import `BranchFailureKind` from `runtime`, replace all `BranchError.kind` fixtures with the new enum, and add:

```rust
#[test]
fn branch_failure_kind_serializes_only_settleable_origins() {
    assert_eq!(
        [
            BranchFailureKind::Workflow,
            BranchFailureKind::Node,
            BranchFailureKind::Timeout,
        ]
        .map(|kind| serde_json::to_value(kind).unwrap()),
        [json!("workflow"), json!("node"), json!("timeout")]
    );
}
```

Expand `join_serializes_the_stable_all_settled_envelope` or add a mixed-results test containing one failure of each kind and assert:

```rust
assert_eq!(outcome.output["summary"]["failed"], 3);
assert_eq!(
    outcome.output["summary"]["failures"],
    json!({"workflow":1,"node":1,"timeout":1})
);
```

- [ ] **Step 2: Run the branch/Join tests RED**

Run:

```bash
cargo test --locked --test fork_join_nodes
```

Expected: compilation fails because `runtime::BranchFailureKind` does not exist and `BranchError.kind` still requires `FailureKind`.

- [ ] **Step 3: Define the branch-only enum and migrate scheduler mappings**

In `src/runtime/state.rs`, add immediately before `BranchError`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchFailureKind {
    Workflow,
    Node,
    Timeout,
}
```

Change `BranchError.kind` to `BranchFailureKind`. Re-export it from `src/runtime/mod.rs`.

In `src/runtime/scheduler.rs`, import `BranchFailureKind` and map exactly:

```rust
let kind = match error.kind() {
    RunErrorKind::Node => BranchFailureKind::Node,
    RunErrorKind::Timeout => BranchFailureKind::Timeout,
    RunErrorKind::Stop | RunErrorKind::Infrastructure => {
        return Err(invariant(
            "contained node failure had a non-settleable runtime origin",
        ));
    }
};
```

Authored branch End failure constructs `BranchFailureKind::Workflow`. Do not create a conversion from run-wide `FailureKind::Infrastructure`.

- [ ] **Step 4: Count Join failures exhaustively and fail closed**

In `src/nodes/join.rs`, remove the `FailureKind` import. Replace the three independent filters with one exhaustive loop:

```rust
let mut workflow = 0;
let mut node = 0;
let mut timeout = 0;
for result in results.values() {
    if let BranchResult::Failed { error, .. } = result {
        match error.kind {
            BranchFailureKind::Workflow => workflow += 1,
            BranchFailureKind::Node => node += 1,
            BranchFailureKind::Timeout => timeout += 1,
        }
    }
}
if failed != workflow + node + timeout {
    return Err(RunError::infrastructure(
        "JOIN_RESULT_INVALID",
        "join branch failure taxonomy is inconsistent",
    ));
}
```

Import `BranchFailureKind`. Keep the serialized Join envelope unchanged.

- [ ] **Step 5: Run branch, scheduler, and containment suites GREEN**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --test fork_join_nodes
cargo test --locked --test run_scheduler
cargo test --locked --test run_coordinator
rg -n "kind: FailureKind::(Workflow|Node|Timeout)" src/runtime/scheduler.rs tests/fork_join_nodes.rs
git diff --check
```

Expected: all tests exit 0; the search has no broad `FailureKind` in `BranchError`; Join count identities and existing stop/infrastructure drain tests pass.

- [ ] **Step 6: Commit the closed branch taxonomy**

```bash
git add src/runtime/state.rs src/runtime/mod.rs src/runtime/scheduler.rs src/nodes/join.rs tests/fork_join_nodes.rs
git diff --cached --check
git commit -m "refactor: close branch failure taxonomy"
```

---

### Task 3: Re-run complete acceptance gates and whole-branch review

**Files:**
- Verify: entire repository
- Compare: `docs/superpowers/specs/2026-07-13-unified-core-end-terminal-model-design.md`
- Compare: `docs/superpowers/specs/2026-07-14-terminal-boundary-review-corrections-design.md`
- Record: `.superpowers/sdd/task-6-report.md`

**Interfaces:**
- Consumes: Task 1 typed terminal projection/resolution and Task 2 closed branch taxonomy.
- Produces: fresh evidence that every terminal/event/Join representation has one authoritative typed origin.

- [ ] **Step 1: Run final source and scope searches**

```bash
git status --short
rg -n "core\.output|OutputNode|NodeTransition::Complete|core\.fail" README.md agents src tests docs/formal-v1-breaking-changes.md
rg -n "terminal: bool|pub terminal: bool" src tests
rg -n -U "(publish_terminal|recover_terminal)\((?:[^\n]*\n){0,5}[^)]*RunEventType::" src tests
rg -n "kind: FailureKind::(Workflow|Node|Timeout)" src/runtime/scheduler.rs tests/fork_join_nodes.rs
git diff --check b7c6ba6..HEAD
```

Expected: clean status; all four searches exit 1 with no matches; diff check exits 0.

- [ ] **Step 2: Run the complete fresh verification gate**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
cargo audit
cargo deny check
```

Expected: every command exits 0. `cargo audit` may report the documented transitive yanked warning. `cargo deny check` may report existing duplicate/MPL warnings and the exact ignored yanked package, but its final line must be `advisories ok, bans ok, licenses ok, sources ok`.

- [ ] **Step 3: Audit the correction acceptance criteria**

Record exact test names in `.superpowers/sdd/task-6-report.md` for:

```text
typed terminal event projection       tests/event_hub.rs
scope mismatch before storage         tests/event_hub.rs
Requested/Authoritative resolution    tests/event_hub.rs
same-type durable-winner log summary  tests/run_coordinator.rs
three-value branch failure kind       tests/fork_join_nodes.rs
Join taxonomy count identity          tests/fork_join_nodes.rs
stop/infrastructure exclusion         tests/run_scheduler.rs
HTTP/SSE/history consistency          tests/api.rs, tests/event_hub.rs
```

Retain the explicit caveat that live PostgreSQL execution self-skips without `RUN_HISTORY_POSTGRES_URL`.

- [ ] **Step 4: Request a fresh whole-branch architecture review**

Review `b7c6ba6..HEAD` against both design specs and both implementation plans. The reviewer must explicitly verify:

```text
no independent terminal event payload inputs
exact update-to-event projection for every terminal variant
value-based Requested versus Authoritative resolution
durable-winner run.finished metadata
infrastructure unrepresentable in BranchError
Join count identity and unchanged JSON shape
all original unified End/Fork/Join/lifecycle acceptance criteria
```

Expected: no Critical, Important, or Minor correctness issue; `Ready to merge? Yes`.

- [ ] **Step 5: Apply review fixes test-first if required**

For each finding, add a focused failing regression before the smallest implementation correction. After the final correction, rerun every command from Steps 1 and 2. Do not create an empty commit if review is clean.

- [ ] **Step 6: Finish the branch**

Use `superpowers:verification-before-completion` and `superpowers:finishing-a-development-branch`. Detect the named worktree and present the four integration options only after all fresh gates and review pass.
