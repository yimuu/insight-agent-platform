# A4 Durable Recovery and Live-State Finalization Design

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

**Status:** Approved for written spec in conversation on 2026-07-11; written-spec review pending.

## Context

The stable-baseline review identifies two related P1 findings:

- `BASE-P1-008`: `EventHub::recover_terminal` closes the failed journal and then awaits `repository.recover_run(...)` directly. SQLite and PostgreSQL recovery intentionally perform locked reads and atomic terminal writes, but the direct call is not bounded by `EventHubConfig::operation_timeout`.
- `BASE-P1-009`: after `recover_run` returns an authoritative terminal event, `reconcile_durable_through` may fail or time out before `remove_run_state` runs. At that point the durable Run is terminal, but live subscribers can remain open and one `EventRunState` can remain retained.

A2 already made preparing/active Run ownership explicit. A4 builds on that: foreground Run ownership must not be held indefinitely by uncertain durable recovery, and EventHub live state must be isolated deterministically once journal-backed live delivery can no longer be trusted.

## Goals

- Bound foreground direct recovery work by `EventHubConfig::operation_timeout`.
- Never treat a timeout or dropped recovery future as rollback proof.
- Transfer uncertain recovery to one deduplicated process-lifetime recovery owner per Run.
- Remove EventHub live state unconditionally after an authoritative durable terminal is known, even when reconciliation fails.
- Close subscribers instead of broadcasting unvalidated history.
- Make `RunService::shutdown` wait for EventHub recovery owners with the caller's shutdown deadline.
- Preserve existing HTTP, SSE envelope, event envelope, repository trait, DSL, migration, and dependency shapes.

## Non-goals

- No cross-process recovery queue, distributed lock, or external worker.
- No repository trait redesign.
- No new public SSE replay or reconnect semantics.
- No attempt to guarantee that a database operation cancelled by client timeout did not commit.
- No metrics backend or structured observability expansion; A7 owns broader observability.
- No PostgreSQL or SQLite schema change.

## Current Failure Model

The current flow is:

1. `RunCoordinator` sees an infrastructure failure and calls `EventHub::recover_terminal`.
2. `recover_terminal` first tries `publish_terminal`.
3. If the journal path fails, it closes the journal worker and calls `repository.recover_run(update, event)` directly.
4. If `recover_run` blocks on a pool, lock, query, or commit, the active Run and capacity permit remain held.
5. If `recover_run` succeeds but `reconcile_durable_through` fails, durable terminal state exists but EventHub state removal does not run.

The safe recovery contract is therefore not "retry until the foreground caller succeeds". The safe contract is "make foreground ownership bounded, then move any uncertain durable convergence to an explicit recovery owner".

## Considered Approaches

### Approach 1: Add `timeout(...)` around `repository.recover_run`

This bounds foreground latency but is not sufficient. A timeout does not prove the database did not commit, and dropping the future cannot be treated as rollback. It also does not solve retained EventHub state after post-commit reconciliation failures.

### Approach 2: Timeout and close live subscribers only

This releases live memory and avoids stranded SSE subscriptions, but it can leave the current process with no owner continuing durable convergence. The service would be unhealthy and foreground ownership would be gone, but the Run could remain non-terminal until restart reconciliation.

### Approach 3: Bounded foreground plus deduplicated recovery owner

This is the selected approach. Foreground recovery is bounded. On uncertainty, EventHub isolates the live state and starts or reuses one per-Run recovery owner. That owner begins with the repository's authoritative locked recovery path and continues without holding active Run capacity. Shutdown waits for those owners within the shutdown deadline.

This keeps the system honest about commit ambiguity and gives each layer one clear responsibility:

- Repository owns authoritative durable state.
- EventHub owns live-state isolation and recovery-owner deduplication.
- RunService owns foreground admission, active capacity, and shutdown drain.

## Architecture

### EventHub recovery request

`EventHub::recover_terminal` snapshots the terminal proposal into an internal `RecoveryRequest`:

- `scope: RunEventScope`
- `event_type: RunEventType`
- `update: TerminalUpdate`
- `code: String`
- `message: String`
- `data: serde_json::Value`

The request is cloneable and contains only data needed to reconstruct the terminal event proposal. The repository remains authoritative for final sequence assignment.

### Foreground recovery

After the journal path fails and `journal.close_and_wait()` returns, `recover_terminal` performs a bounded direct recovery:

1. Acquire or create the current run state.
2. Build a proposed terminal event using the current `next_seq`.
3. Await `repository.recover_run(update, event)` under `EventHubConfig::operation_timeout`.
4. If it succeeds, reconcile the validated durable suffix before broadcasting anything.
5. If it times out or returns a history error, isolate live state, start or reuse the background recovery owner, and return the foreground error.

Foreground timeout returns `EventError::JournalOperationTimeout`. Foreground history errors return `EventError::History(...)`. The existing `SequenceExhausted` behavior remains terminal and does not start a background owner.

### Background recovery owner

EventHub adds an internal recovery-owner registry keyed by `run_id`.

Only one owner may be active for a Run. If another caller encounters the same recovery uncertainty while the owner is active, it reuses the existing owner and returns its own foreground error without spawning duplicate recovery work.

The owner:

1. Starts from the same `RecoveryRequest`.
2. Rebuilds a proposed terminal event. The sequence in the proposal is not trusted as authoritative; `recover_run` derives or returns the durable terminal sequence under lock.
3. Calls `repository.recover_run(...)` through the existing repository contract.
4. Does not broadcast events if live state was already isolated.
5. Removes itself from the recovery-owner registry and notifies waiters when finished.

If the repository reports an existing competing terminal, `recover_run` returns that terminal. The owner treats that as convergence. It does not fabricate a replacement terminal.

If the owner fails with a permanent repository error, it records the error through tracing and completes. The process is already unhealthy because the journal was closed; restart reconciliation remains the external recovery path for unrecoverable storage failures.

### Live-state isolation

EventHub adds one cleanup primitive:

```rust
async fn isolate_run_state(
    &self,
    run_id: &str,
    expected: &Arc<Mutex<EventRunState>>,
)
```

It removes exactly the expected state handle from `states`. Dropping the last `broadcast::Sender` closes subscribers. The function is idempotent and pointer-checked so a stale cleanup cannot remove a newer state.

Isolation is required in three cases:

- Foreground recovery times out or returns a history error before an authoritative terminal is known.
- Foreground recovery returns an authoritative terminal but reconciliation fails.
- Foreground recovery succeeds and all validated durable suffix events have been broadcast.

The second case is the `BASE-P1-009` fix: durable terminal convergence must not depend on live reconciliation success.

### Reconciliation rules

`reconcile_durable_through` remains the only path that can broadcast durable events recovered from history.

It must validate the entire suffix before sending any event:

- terminal sequence must not precede the live `next_seq`;
- missing durable suffix length must remain within the current small recovery window;
- every recovered event sequence must be contiguous;
- the final event must exactly equal the authoritative terminal event.

If validation fails, no event is broadcast. EventHub isolates the state and returns the typed error.

### RunService shutdown drain

RunService currently waits until preparing and active maps are empty. A4 extends shutdown to wait for EventHub recovery owners after lifecycle ownership is empty.

The shutdown deadline is still supplied by the caller. RunService uses the remaining time after active/preparing drain to wait for EventHub recovery owners:

```rust
events.wait_for_recoveries(remaining_deadline).await
```

If recovery owners are still active when the deadline expires, shutdown returns `SHUTDOWN_TIMEOUT`. This is an internal interface addition on `EventHub`; it is needed because active Run capacity is intentionally released before background durable convergence completes.

## Interface Changes and Rationale

### `EventHub::wait_for_recoveries(deadline: Duration)`

This method is added for `RunService::shutdown`. Without it, A4 would release active Run ownership while leaving no shutdown-visible work item for durable convergence.

The method does not alter HTTP, SSE, event, repository, or DSL contracts. It exposes only internal process drain state.

### Internal recovery registry

EventHub gains internal state to track per-Run recovery owners. This is not a public protocol change. It exists to prevent duplicate recovery attempts from several failing foreground callers.

### Test-only diagnostics

Existing tests already use `EventHub::retained_run_count`. A4 may add or reuse test-visible diagnostics only for EventHub internals if they are needed to assert no retained owner or state. These diagnostics must not appear in platform configuration or HTTP APIs.

## Error Semantics

- `JOURNAL_OPERATION_TIMEOUT` continues to mean a journal or recovery operation exceeded the configured operation timeout.
- Repository errors keep their existing `HistoryError::code()`.
- Subscriber closure after recovery isolation surfaces as `SUBSCRIPTION_CLOSED`.
- No synthetic terminal event is sent when reconciliation cannot validate durable history.
- Background owner failure does not fabricate an event and does not mark a Run terminal in memory. Durable state remains the source of truth.

## Data Flow

### Successful direct recovery with validated suffix

1. Journal path fails.
2. EventHub closes the journal.
3. Foreground bounded `recover_run` returns an authoritative terminal.
4. EventHub reads and validates the missing durable suffix.
5. EventHub broadcasts only the fully validated suffix.
6. EventHub isolates the run state.
7. RunCoordinator commits the terminal status into `RunState`.
8. RunService removes the active Run and releases capacity.

### Foreground recovery timeout

1. Journal path fails.
2. EventHub closes the journal.
3. Foreground bounded `recover_run` times out.
4. EventHub isolates the run state and closes subscribers.
5. EventHub starts or reuses the per-Run background recovery owner.
6. `recover_terminal` returns `JOURNAL_OPERATION_TIMEOUT`.
7. RunCoordinator returns infrastructure failure to RunService.
8. RunService marks the service unhealthy, removes active ownership, and releases capacity.
9. Background owner continues durable convergence.
10. Shutdown waits for the recovery owner within the remaining shutdown deadline.

### Authoritative terminal returned, reconciliation fails

1. Journal path fails.
2. Foreground bounded `recover_run` returns an authoritative terminal.
3. EventHub attempts bounded durable suffix reconciliation.
4. Reconciliation fails due to timeout, error, gap, overflow, excessive missing events, or terminal mismatch.
5. EventHub isolates the run state and closes subscribers.
6. `recover_terminal` returns the reconciliation error.
7. Durable terminal state remains authoritative; no unvalidated event is broadcast.

## Testing Strategy

### EventHub unit/integration tests

Add tests in `tests/event_hub.rs` using the existing memory repository fixture extended with recovery and list-event controls.

Required cases:

- Foreground `recover_run` timeout returns `JOURNAL_OPERATION_TIMEOUT`, closes subscribers, removes run state, and starts exactly one background owner.
- Duplicate `recover_terminal` calls while an owner is active do not spawn duplicate owners.
- Background owner converges a Run after the foreground timed out and the repository is later released.
- Foreground `recover_run` succeeds, but `list_events_after` times out; EventHub removes state and subscribers close.
- Foreground `recover_run` succeeds, but `list_events_after` returns empty, short, gapped, overflowing, or final-mismatched history; EventHub removes state and broadcasts nothing.
- Existing successful uncertain-append recovery still broadcasts a fully validated suffix and removes state.
- Competing durable terminal returned by `recover_run` is treated as convergence and does not create a duplicate terminal event.

### RunService tests

Add tests in `tests/run_service.rs`:

- A blocked background recovery owner keeps `shutdown(deadline)` from completing until the deadline.
- Releasing the repository allows the background owner to finish, after which shutdown completes.
- Active Run capacity is released after foreground recovery hands off to the recovery owner.
- New admission remains rejected while EventHub is unhealthy.

### Repository-backed tests

Where available, extend SQLite and PostgreSQL history tests to cover:

- recovery under a held lock eventually derives the terminal sequence once released;
- an existing competing terminal is returned without duplicate terminal event insertion;
- recovery event sequence remains contiguous.

These tests validate repository contracts. EventHub A4 behavior must not depend on SQLite/PostgreSQL-specific details beyond the existing `RunRepository::recover_run` contract.

## Acceptance Criteria

- Foreground direct recovery never waits longer than `operation_timeout` plus scheduling tolerance.
- No active Run or capacity permit is held solely because recovery is waiting on durable storage.
- Every recovery uncertainty has at most one active background owner per Run.
- EventHub retained run state reaches zero after terminal recovery success, foreground recovery handoff, or post-commit reconciliation failure.
- Subscribers receive validated terminal/suffix events only on successful reconciliation; otherwise they receive channel closure.
- Shutdown waits for background recovery owners and returns `SHUTDOWN_TIMEOUT` if they exceed the shutdown deadline.
- No changes to HTTP routes, SSE envelope, event envelope, repository trait, DSL, migrations, or dependencies.

## Rollout and Rollback

A4 is a runtime behavior change only. It requires no history reset and no data migration.

Rolling forward makes journal-failure recovery bounded in the foreground and closes stranded subscribers promptly. Rolling back restores the previous risk: foreground direct recovery can retain active ownership indefinitely, and post-commit reconciliation failure can retain EventHub state.

## Open Decisions

None. The selected approach is bounded foreground recovery plus a per-Run EventHub recovery owner and RunService shutdown drain.
