# Repository Terminal Proposal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` and strict RED/GREEN execution.

**Goal:** Remove the final repository-level terminal duplicate truth source by replacing
independent typed-update/event inputs with one opaque canonical proposal.

**Design:**
`docs/superpowers/specs/2026-07-14-repository-terminal-proposal-design.md`

## Global Constraints

- No public DSL, HTTP/SSE JSON, event JSON, Run JSON, migration, or row-layout change.
- No compatibility overload for `finish_run` or `recover_run`.
- `TerminalProposal::event_at` is the only terminal event projection function.
- `TerminalSequence` controls only sequence selection and cannot carry event payload.
- Existing value-based `TerminalResolution` and durable-winner logging remain intact.
- Keep the live PostgreSQL caveat when `RUN_HISTORY_POSTGRES_URL` is unset.

## Task 1: Close the Repository terminal mutation contract

**Files:**

- Modify `src/history/repository.rs`
- Modify `src/history/sqlite.rs`
- Modify `src/history/postgres.rs`
- Modify `src/events/journal.rs`
- Modify `src/events/hub.rs`
- Modify repository fakes in integration tests
- Modify `tests/history_sqlite_v1.rs`
- Modify `tests/history_postgres.rs`
- Modify `tests/event_hub.rs` as required by the new fake contract

### Step 1: Write RED contract tests

Adapt SQLite and PostgreSQL repository tests to construct a `TerminalProposal` and call
the not-yet-existing `commit_terminal` API. Remove independently authored terminal
events from those calls.

For SQLite, assert the exact returned and replayed completed event:

```text
type      run.completed
time      update.ended_at
code      OK
message   ok
data      content + format + data from RunOutput
```

Assert `Expected(seq)` commits only at the durable next sequence and that a terminal
loser returns the existing authoritative event. Retain recovery coverage with
`NextDurable`, including next-sequence derivation and a different durable winner.

Run `cargo test --locked --test history_sqlite_v1`. Expected RED: unresolved
`TerminalProposal`, `TerminalSequence`, and `commit_terminal` contract.

### Step 2: Add the opaque proposal and canonical projection

In `src/history/repository.rs`:

- define private-field `TerminalProposal { scope, update }`;
- add a validated constructor returning `HISTORY_EVENT_INVALID` before storage;
- add `scope()`, `update()`, `run_id()`, `into_parts()`, and `event_at(seq)` accessors;
- move completed/failed/cancelled/interrupted projection into `event_at`;
- define `TerminalSequence::{Expected(u64), NextDurable}`;
- replace `finish_run` and `recover_run` with `commit_terminal` returning `RunEvent`;
- delete `validate_recovery_event` and all APIs that accept an independent terminal
  event.

### Step 3: Implement built-in atomic commits

For both SQLite and PostgreSQL:

- serialize/lock the Run row;
- return the stored terminal event when the Run is already terminal;
- compute the durable next sequence;
- reject a mismatched `Expected` sequence before mutation;
- project the terminal event only with `proposal.event_at(committed_seq)`;
- atomically update the Run and insert that event;
- verify contiguous event history;
- retry once only for transient `NextDurable` failures.

Keep stable existing HistoryError codes where semantics already exist. Use
`HISTORY_EVENT_INVALID` for an expected-sequence mismatch before mutation.

### Step 4: Migrate EventJournal and EventHub

The journal Finish command carries `TerminalProposal` plus an expected sequence and
returns the repository's authoritative `RunEvent`.

EventHub:

- constructs the proposal before live-state allocation;
- computes `requested = proposal.event_at(state.next_seq)`;
- calls the journal with `Expected(state.next_seq)`;
- commits/broadcasts the returned authoritative event;
- resolves Requested/Authoritative by full value equality;
- uses the same proposal with `NextDurable` for direct/background recovery;
- deletes the private duplicate projection and old false-plus-history-lookup path.

### Step 5: Migrate repository test doubles

Update every `RunRepository` implementation in tests to the single method. Test doubles
may inspect the proposal through read-only accessors and must produce events only via
`event_at(seq)`. Preserve existing injected failures, gates, races, call counters, and
authoritative-winner behavior.

### Step 6: GREEN verification and commit

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --test history_sqlite_v1
cargo test --locked --test history_postgres
cargo test --locked --test event_hub
cargo test --locked --test run_coordinator -- --test-threads=1
cargo test --locked --test run_service
cargo test --locked --test api
rg -n "finish_run|recover_run|validate_recovery_event" src tests
rg -n -U "commit_terminal\((?:[^\n]*\n){0,5}[^)]*RunEvent" src tests
git diff --check
```

Expected: tests pass; both legacy searches have no matches; PostgreSQL compiles and
self-skips its live body when the environment variable is unset.

Commit the implementation as:

```text
refactor: make repository terminal proposals canonical
```

## Task 2: Fresh complete gates and final review

From `b7c6ba6..HEAD`, rerun the correction-plan source searches, formatting, strict
Clippy, locked all-targets, audit, deny, and diff check. Update
`.superpowers/sdd/task-6-report.md` with the repository proposal coverage and unchanged
PostgreSQL caveat.

Generate a fresh whole-branch review package and require zero Critical, Important, and
Minor correctness findings plus `Ready to merge: Yes`. Apply any finding test-first and
repeat the entire gate.
