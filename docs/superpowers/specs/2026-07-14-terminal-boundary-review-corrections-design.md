# Terminal Boundary Review Corrections Design

Date: 2026-07-14

Status: approved approach, pending written-spec review

## 1. Context

The unified `core.end` branch review found three remaining internal consistency gaps:

1. EventHub accepts both a typed `TerminalUpdate` and independently authored terminal event fields, so durable history and the SSE event can disagree.
2. `BranchError` uses the run-wide `FailureKind`, which includes `Infrastructure`, even though Join results admit only workflow, node, and timeout failures.
3. Terminal recovery returns only `Option<RunEvent>`, so the coordinator cannot distinguish the requested terminal from a different durable winner when both use the same event type.

These are internal contract defects. The public Formal V1 DSL, HTTP schema, SSE schema, database schema, and checked-in Agents do not need another migration.

## 2. Decision

Adopt one typed source of truth at both boundaries:

- `TerminalUpdate` is the only input from which EventHub may build a terminal event.
- `BranchFailureKind` is the only failure-origin type accepted in `BranchResult`.
- EventHub returns an explicit terminal resolution that distinguishes a matching requested terminal from a different authoritative durable terminal.

Do not retain compatibility overloads that accept independent terminal event fields.

## 3. Typed Terminal Event Projection

Change the terminal APIs to:

```rust
pub async fn publish_terminal(
    &self,
    scope: RunEventScope,
    update: TerminalUpdate,
) -> Result<TerminalResolution, EventError>;

pub async fn recover_terminal(
    &self,
    scope: RunEventScope,
    update: TerminalUpdate,
) -> Result<TerminalResolution, EventError>;
```

EventHub validates only the remaining independent identity boundary: `scope.run_id == update.run_id`. It then projects the complete event from `update.terminal`:

| Terminal variant | Event type | Code/message | Data |
|---|---|---|---|
| `Completed { output }` | `run.completed` | `OK` / `ok` | exact serialized `RunOutput` |
| `Failed { error }` | `run.failed` | exact error code/message | `{ "kind": error.kind }` |
| `Cancelled { error }` | `run.cancelled` | exact stop code/message | `{}` |
| `Interrupted { error }` | `run.interrupted` | exact stop code/message | `{}` |

The event timestamp is `TerminalUpdate.ended_at`, not a second clock read. Event type, timestamp, code, message, and data therefore cannot drift from the durable terminal proposal.

`RecoveryRequest` retains only `scope` and `update`; background recovery uses the same projection function as ordinary terminal publication.

## 4. Explicit Durable Resolution

Replace `Option<RunEvent>` with:

```rust
pub enum TerminalResolution {
    Requested(RunEvent),
    Authoritative(RunEvent),
}
```

Semantics:

- `Requested`: the authoritative event exactly matches the event projected from this request. The coordinator may safely use its attempted typed log summary.
- `Authoritative`: a different durable terminal event won. The coordinator must load the authoritative `RunRecord` and derive status, output size, error code, and failure kind from that record.

The distinction is about value equality, not writer ownership. If another writer committed an exactly identical event, treating it as `Requested` is safe because both terminal representations and log summaries are identical.

Both the normal `finish` race and direct recovery compare the authoritative event with the projected request event. Event type equality alone is insufficient.

## 5. Branch-only Failure Origin

Introduce:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchFailureKind {
    Workflow,
    Node,
    Timeout,
}
```

`BranchError.kind` becomes `BranchFailureKind`. Scheduler mappings are exhaustive:

- authored branch End failure -> `Workflow`;
- contained executor failure -> `Node`;
- contained node deadline -> `Timeout`;
- stop or infrastructure -> outer scheduler termination, never a `BranchResult`.

The run-wide `FailureKind` remains unchanged because durable Run failure legitimately includes `Infrastructure`.

Join counts all failed branches in one exhaustive match over `BranchFailureKind`, then defensively verifies:

```text
failed == workflow + node + timeout
```

Any future model change that violates this invariant returns an infrastructure `JOIN_RESULT_INVALID` error rather than serializing a contradictory envelope.

## 6. Data Flow

```text
Scheduler/Coordinator
  -> TerminalUpdate
  -> EventHub typed projection
  -> repository atomically stores Run terminal + projected event
  -> TerminalResolution
       Requested     -> attempted typed log summary
       Authoritative -> authoritative RunRecord -> durable log summary
```

```text
Branch execution
  -> BranchFailureKind (workflow | node | timeout)
  -> immutable BranchResult map
  -> Join exhaustive taxonomy + invariant check
  -> Condition selects degraded success or failure End
```

## 7. Error Handling

- Scope/update run-ID mismatch remains `HISTORY_EVENT_INVALID` and occurs before EventHub allocates live state or writes storage.
- Typed event serialization failure is an infrastructure/history boundary error and cannot partially persist a terminal.
- A different durable winner is not an error; it is `TerminalResolution::Authoritative`.
- Missing or divergent durable terminal events retain the existing recovery errors.
- Invalid Join taxonomy fails closed as `JOIN_RESULT_INVALID`; it is never exposed as authored workflow data.

## 8. Tests

Use strict test-first changes.

EventHub tests must cover:

- completed projection, including optional output fields and exact `ended_at` timestamp;
- failed projection for workflow, node, timeout, and infrastructure kinds;
- cancelled and interrupted projection;
- run-ID mismatch before storage;
- normal and recovery paths using the same projection;
- a same-`run.failed` recovery race whose durable winner has a different kind/code.

Coordinator tests must prove that the same-event-type recovery race logs the durable winner's failure kind/code rather than the attempted infrastructure summary.

Branch/Join tests must cover:

- exact serialization of `workflow`, `node`, and `timeout` branch kinds;
- scheduler mappings for all three settleable origins;
- the Join count identity for mixed and all-failed results;
- stop and infrastructure still drain/cancel siblings and never enter Join results.

After focused RED/GREEN evidence, rerun formatting, strict Clippy, locked all-targets, audit, deny, legacy searches, diff checks, and whole-branch review.

## 9. Compatibility and Scope

- No public DSL or HTTP/SSE JSON shape changes.
- No migration or persisted-row layout changes.
- Internal Rust EventHub callers must migrate to the new typed API in one commit range; no deprecated overload remains.
- Existing external node extensions keep using the scheduler boundary; they cannot author infrastructure branch results.
- Graph validation, `END_REQUIRED` precedence, Select semantics, Agent YAML, README examples, and the exact `spin@0.9.8` policy exception are out of scope.

## 10. Acceptance Criteria

1. There is no EventHub terminal API that accepts independent event type/code/message/data alongside `TerminalUpdate`.
2. Every terminal event field is derived from the exact typed update persisted with it.
3. Same-type terminal recovery logs the durable winner when terminal values differ.
4. Infrastructure cannot be represented as a settled `BranchError`.
5. Every serialized Join envelope satisfies the taxonomy count identity.
6. Existing external Formal V1 contracts and all prior tests remain valid.
