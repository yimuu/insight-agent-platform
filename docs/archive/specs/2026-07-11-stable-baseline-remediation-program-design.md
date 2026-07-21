# Stable Baseline Remediation Program Design

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

**Status:** Approved in conversation on 2026-07-11; written-spec review pending.

## Context

The stable-baseline review at `docs/reviews/2026-07-11-stable-baseline-review.md` identifies fourteen confirmed architecture and correctness findings: ten P1, two P2, and two P3. They span compiler semantics, node extensions, runtime ownership, cancellation, durable recovery, provider memory, transport security, observability, and documentation. Treating them as one implementation change would couple unrelated failure domains and make review, rollback, and regression attribution unreliable.

This design creates a remediation program with nine independently deliverable milestones. It fully specifies the first milestone, A0 Sensitive Error Containment, and leaves A1–A8 behind their own future design and plan gates.

## Goals

- Turn the review roadmap into an ordered remediation program with explicit prerequisites.
- Close `BASE-P1-010` first by preventing Action JSON Schema validation from exposing raw input or output instances.
- Preserve existing Action error codes and public HTTP, SSE, event, Run, and repository shapes.
- Make the A0 validation boundary reusable by later compile-time Action validation in A5.
- Verify containment through direct registry, node, runtime, transport, persistence, and logging paths.
- Reset disposable historical Run data explicitly during deployment instead of attempting unreliable content-level cleansing.

## Non-goals

- Implementing A1–A8 as part of A0.
- Adding compile-time Action input validation from `BASE-P1-001`; that belongs to A5.
- Changing `RunError`, the Formal V1 event envelope, Run records, migrations, or repository traits.
- Preserving, scanning, redacting, or migrating existing Run history.
- Adding a history-reset API, CLI command, or automatic startup deletion.
- Upgrading CEL, JSON Schema, SQLx, or any other dependency.
- Adding a general metrics system; A7 owns observability beyond A0's secret-absence assertions.

## Program structure

The program retains the review's nine independently reviewable milestones:

| Order | Milestone | Confirmed findings | Prerequisites |
|---|---|---|---|
| A0 | Sensitive error containment | `BASE-P1-010` | None |
| A1 | Provider memory bounds | `BASE-P1-011` | None |
| A2 | Preparing/active lifecycle ownership | `BASE-P1-006` | None |
| A3 | Authoritative Stop semantics | `BASE-P1-007` | Extension contract agreement |
| A4 | Durable recovery and live-state finalization | `BASE-P1-008`, `BASE-P1-009` | A2; one shared cleanup primitive |
| A5 | Semantic compile-time validation | `BASE-P1-001`, `BASE-P1-002`, `BASE-P1-003` | A0; explicit approval for breaking items; CEL coordination |
| A6 | Extension integration contract | `BASE-P2-005` | Coordinate with A3 fixtures |
| A7 | Body-free INFO observability | `BASE-P2-013` | A1 byte-accounting contract |
| A8 | Contract and transport decisions | `BASE-P1-012`, `BASE-P3-004`, `BASE-P3-014` | Separate approval for breaking items; loopback/DNS decision |

Every milestone receives its own design, implementation plan, branch or worktree, test cycle, code review, and integration decision. Dependency work remains in the separate dependency-governance roadmap. A milestone must re-establish a releasable baseline before the next dependent milestone begins.

## A0 design decision

### Alternatives considered

1. **Sanitize at the Action Schema boundary — selected.** Convert failed Action input/output validation directly into fixed safe `RunError` values. Every downstream consumer then receives safe data without additional filtering.
2. **Sanitize every `RunError`.** This is too broad and cannot reliably distinguish sensitive values from legitimate diagnostic text. It would also alter unrelated node, runtime, and infrastructure errors.
3. **Sanitize at persistence and transport outputs.** This is too late and duplicates policy across branch events, terminal events, GET Run, SSE, and logs. One missed sink would preserve the disclosure.

The selected design fixes the source of the unsafe message and keeps downstream code unchanged.

## A0 component boundary

The only production logic change is in the Action validation boundary in `src/resources/actions.rs`.

Current flow:

```text
JSONSchema::validate(value)
  -> ValidationError::to_string()
  -> RunError.message
  -> node/branch/run events
  -> Run history and GET Run
  -> Attached SSE
```

Replacement flow:

```text
JSONSchema::is_valid(value)
  -> true: continue
  -> false:
       ACTION_INPUT_INVALID / "action input validation failed"
       ACTION_OUTPUT_INVALID / "action output validation failed"
```

The validator is used only as a boolean decision. A0 must not format or retain `ValidationError.instance`, `ValidationError.kind`, `instance_path`, `schema_path`, enum/constant options, property names, or the original Action value in a public/default diagnostic.

`RegisteredAction::validate_input(&Value) -> Result<(), RunError>` remains the reusable input-validation interface. `RegisteredAction::call` retains mandatory runtime input validation before invoking the Action and mandatory output validation after the Action returns. A5 may later call `validate_input` for statically decidable literal input without inventing a second formatter.

## Stable error contract

The following error codes and messages are exact:

| Boundary | Code | Message |
|---|---|---|
| Action input | `ACTION_INPUT_INVALID` | `action input validation failed` |
| Action output | `ACTION_OUTPUT_INVALID` | `action output validation failed` |

A0 does not add public metadata, schema keywords, instance paths, or schema paths. It does not change HTTP status mapping, event types, event data shape, Run status, terminal persistence, or branch all-settled shape. Linear and parallel propagation continue to copy the `RunError` code/message, but those values are safe at their source.

JSON Schema resolution or validation-engine errors that currently surface through Action validation remain classified under the same Action invalid code. External-reference reachability and resolver policy remain in the dependency-governance JSON Schema milestone.

## Data-flow consequences

- Invalid rendered input is rejected before `Action::call`; the Action must not observe or execute on it.
- Invalid Action output is discarded and replaced by the fixed output-validation error.
- Sequential failure produces safe node and Run failure messages.
- Parallel branch failure and the all-settled error envelope contain only the fixed safe code/message.
- Terminal Run records persist only the fixed safe message.
- `run_events.message` and error objects in event data contain only the fixed safe message.
- GET Run and Attached SSE inherit the same safe message without sink-specific redaction.
- Default tracing and debug output must not receive the original instance or the formatter output that previously embedded it.

## Historical data policy

Existing history is disposable. A0 does not attempt to detect which prior messages contain sensitive values because validator text is unstructured and reliable classification is impossible.

Deployment is explicitly destructive:

1. Stop every runtime process using the target history store.
2. For SQLite, delete the configured database file and any same-name WAL/SHM sidecar files.
3. For PostgreSQL, with exclusive ownership of the Formal V1 store, run:

   ```sql
   BEGIN;
   TRUNCATE TABLE node_outputs, run_events, runs;
   COMMIT;
   ```

4. Keep SQLx migration metadata intact.
5. Deploy and start the A0 build.
6. Execute one safe failing-Action smoke Run and verify the fixed code/message through the selected transport.

The application must not delete history automatically. No management endpoint or CLI is added. Rollback restores the old binary only; deleted history is not recoverable. The operation and its reason must be documented in `docs/formal-v1-breaking-changes.md`, with README linking to that single source of truth.

## Test design

### Direct registry contract

Extend `tests/resource_registries.rs` to cover input and output violations for representative type, length, pattern, enum, and composite-schema failures. Each fixture includes a unique secret marker. Assertions cover exact code/message, absence of the secret from `Debug` and message output, and proof that invalid input never invokes the Action.

### Node execution contract

Extend `tests/core_chat_action.rs` to exercise rendered invalid input and invalid Action output through the real `ActionNode`. Assert the exact fixed messages, unchanged stable codes, no Action call for invalid input, and unchanged successful Action behavior.

### End-to-end containment contract

Create `tests/action_error_containment.rs`. It uses production Action registration, node compilation/execution, `RunService`, `EventHub`, an in-memory SQLite repository, and the Formal HTTP router. It covers:

- linear Attached input failure through node/run events and terminal EOF;
- Detached failure through GET Run and raw Run/event history;
- parallel branch output failure through `branch.failed` and all-settled data;
- secret absence from SSE bytes, serialized API responses, `RunRecord.error_message`, persisted event message/data, and captured tracing output;
- exact input/output error codes and messages;
- subscriber/permit cleanup and a successful subsequent Run.

The broad integration test uses one unique secret per path so an assertion cannot pass because the wrong fixture was inspected.

### Documentation and release verification

Update `docs/formal-v1-breaking-changes.md` with the destructive history-reset procedure and rationale. README links to that section and does not duplicate the SQL. No migration-layout assertion changes because migrations remain untouched.

The A0 release gate is:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --test resource_registries --test core_chat_action \
  --test action_error_containment -- --nocapture
cargo test --all-targets
cargo audit
cargo deny check
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' \
  cargo test --test history_postgres -- --nocapture
```

The real PostgreSQL gate verifies that the destructive-reset documentation and A0 changes did not accidentally alter repository compatibility; A0 does not add PostgreSQL-specific product logic.

## Rollout and rollback

A0 is source-compatible at the Rust/HTTP/event/repository shape level but operationally incompatible with retained Run history because deployment deliberately resets it. The fixed human-readable error messages are a security correction; consumers must rely on stable error codes rather than previous dynamic validator text.

Rollout order is stop, reset, deploy, migrate-check/start, smoke test, then reopen traffic. If the smoke test fails, stop the new service and roll back the binary. Do not restore pre-A0 history into either binary because it may contain disclosed Action values.

## Acceptance criteria

1. No Action validation path calls `ValidationError::to_string()` or otherwise formats an instance-bearing validator error.
2. Input and output validation return the exact fixed code/message pairs.
3. Invalid input does not invoke the Action.
4. Linear, parallel, Attached, Detached, SSE, GET Run, raw history, and default tracing contain no fixture secret.
5. Existing successful Action behavior and error codes remain unchanged.
6. `RunError`, HTTP, SSE, event, RunRecord, repository, and migration shapes are unchanged.
7. Deployment documentation requires explicit history reset and the application performs no automatic deletion.
8. Focused tests, the full Rust gate, dependency policy gates, and real PostgreSQL contract test pass.
9. A1–A8 remain unimplemented and require their own approved designs and plans.

