# Repository Terminal Proposal Design

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

## Problem

The typed EventHub boundary is not sufficient while `RunRepository` still accepts a
`TerminalUpdate` and an independently authored `RunEvent`. Built-in repositories
currently validate only run identity, terminal event type, and run scope. A caller can
therefore atomically persist a typed Run row and a terminal history event whose
timestamp, code, message, or data disagrees with that row.

The repository boundary must make that contradictory pair unrepresentable rather than
depending on callers to project matching values.

## Decision

Introduce an opaque typed proposal:

```rust
pub struct TerminalProposal {
    scope: RunEventScope,
    update: TerminalUpdate,
}
```

Its validated constructor rejects a scope/update run-ID mismatch. Its fields remain
private. Read-only accessors support repository implementations, and `event_at(seq)` is
the only terminal-event projection function.

Replace both repository terminal methods with one operation:

```rust
pub enum TerminalSequence {
    Expected(u64),
    NextDurable,
}

async fn commit_terminal(
    &self,
    proposal: TerminalProposal,
    sequence: TerminalSequence,
) -> Result<RunEvent, HistoryError>;
```

`Expected(seq)` is used by the ordered journal path. `NextDurable` is used after an
uncertain journal failure and derives the next sequence while holding the durable run
lock. Sequence selection is independent control metadata; it cannot change terminal
type, timestamp, code, message, or data.

The repository always returns the authoritative durable terminal event. If this call
commits the proposal, the returned event is `proposal.event_at(committed_seq)`. If the
run is already terminal, the returned event is the stored terminal event.

## Canonical Projection

`TerminalProposal::event_at` projects every terminal field from the exact typed update:

- completed -> `run.completed`, `OK`, `ok`, and output content/format/data;
- failed -> `run.failed`, typed failure code/message/kind;
- cancelled -> `run.cancelled`, typed stop code/message, empty data;
- interrupted -> `run.interrupted`, typed stop code/message, empty data;
- timestamp -> `TerminalUpdate.ended_at` for every variant.

The scope supplies only event identity fields. The constructor requires
`scope.run_id == update.run_id`. No repository API accepts an independent terminal
event or terminal payload field.

## Commit Semantics

Both SQLite and PostgreSQL commit under one transaction:

1. lock or otherwise serialize the target Run row;
2. reject a missing Run;
3. if already terminal, validate and return its stored terminal event;
4. require the Run to be created or running;
5. read the durable maximum event sequence;
6. for `Expected(seq)`, require `seq == max + 1`; for `NextDurable`, use `max + 1`;
7. construct the event with `proposal.event_at(seq)`;
8. update the typed Run lifecycle and insert the projected event atomically;
9. verify contiguous history and commit;
10. return the projected event.

Only `NextDurable` retains the existing one-time retry for transient read/write
failures. An uncertain `Expected` write continues through EventHub recovery so a retry
cannot append a second terminal event.

## EventHub and Journal

EventHub constructs one `TerminalProposal` before allocating live state. It uses
`event_at(next_seq)` to form the requested value, then asks the journal/repository to
commit it. The returned authoritative event is compared with the requested event by
full `RunEvent` value equality and broadcast only after durable commit.

The journal `Finish` command carries `TerminalProposal + Expected(seq)`, not a
`TerminalUpdate + RunEvent` pair. Direct and background recovery carry the same
proposal with `NextDurable`.

This preserves the existing `TerminalResolution::{Requested, Authoritative}` contract
while removing its last lower-level duplicate truth source.

## Compatibility and Scope

- No DSL, HTTP, SSE, event JSON, Run JSON, migration, or persisted-row layout changes.
- No change to End/Fork/Join, graph validation, Select, Agents, README, or dependency
  policy.
- The public Rust repository trait intentionally changes because the project is in its
  initial, unused stage and correctness is preferred over a compatibility adapter.
- No deprecated `finish_run` or `recover_run` overload remains.

## Acceptance Criteria

1. `RunRepository` has exactly one terminal mutation entry point and accepts no
   independent `RunEvent`.
2. Every newly persisted terminal event is produced by
   `TerminalProposal::event_at` from the exact update.
3. SQLite and PostgreSQL use identical proposal and sequence-mode semantics.
4. Normal journal commits and uncertain recovery both return the authoritative durable
   event and preserve value-based terminal resolution.
5. Existing external JSON contracts and all prior End/Fork/Join/lifecycle behavior are
   unchanged.
6. Focused repository tests, complete locked tests, strict Clippy, audit, deny, legacy
   searches, and final whole-branch review all pass.
