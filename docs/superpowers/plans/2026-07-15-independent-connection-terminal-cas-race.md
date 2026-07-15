# Independent-Connection Terminal CAS Race Implementation Plan

**Goal:** Directly prove that concurrent terminal proposals from independent SQLite
and PostgreSQL connections resolve to one durable authoritative Run terminal.

**Design:**
`docs/superpowers/specs/2026-07-15-independent-connection-terminal-cas-race-design.md`

**Status:** Implemented and verified

## Constraints

- Test the production `RunRepository::commit_terminal` method.
- Race different typed proposals at the same expected sequence.
- Require both calls to return the exact durable winner, not a generic conflict.
- Prove exactly one proposal won and only one terminal event exists.
- Keep PostgreSQL racers inside one owned repository generation.
- Prove transaction and row locks are released after both calls.
- Do not add test-only production hooks, new ownership bypasses, schemas, migrations,
  public APIs, or distributed-worker claims.

## Task 1: Add the SQLite independent-pool race

1. Create one temporary SQLite database and two separately connected repositories.
2. Persist a running Run and its contiguous nonterminal prefix.
3. Hold a third-connection writer lock and release two gated terminal calls.
4. Release the lock, collect both results, and identify exactly one requested winner.
5. Assert the authoritative Run record and single terminal event match that winner.
6. Submit a third proposal and prove it returns the same event.
7. Use a fresh pool to acquire and commit a new write transaction without waiting.

## Task 2: Add the PostgreSQL independent-backend race

1. Create an isolated test schema and acquire one exclusive runtime owner.
2. Persist a running Run and contiguous nonterminal prefix.
3. Lock the Run row from a separate inspector transaction.
4. Start two cloned-repository terminal calls behind a common gate.
5. Poll `pg_stat_activity` until two distinct repository backend PIDs are waiting on
   the row lock, then release the inspector transaction.
6. Assert one requested winner, one authoritative loser, one Run terminal, and one
   terminal event.
7. Prove a third proposal is idempotent.
8. Acquire Run and ownership rows with `FOR UPDATE NOWAIT`, assert no repository
   backend is idle in transaction, then release the owner and clean the schema.

## Task 3: Synchronize status

1. Mark remediation item 9 `Addressed` only after both direct backend boundaries
   pass under their existing local/CI policies.
2. Add the design and test names to the post-baseline terminal evidence.
3. Keep PostgreSQL exclusive-store ownership and the lack of distributed execution
   explicit.
4. Mark this design and plan implemented only after complete gates pass.

## Task 4: Complete gates and independent review

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --test history_sqlite_v1 -- --nocapture --test-threads=1
cargo test --locked --test history_postgres -- --nocapture --test-threads=1
cargo test --locked --all-targets --all-features -- --nocapture --test-threads=1
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo audit
cargo deny check
git diff --check
```

Then independently review physical-connection evidence, winner/loser identification,
Run/event parity, ownership fencing, and post-race lock cleanup.
