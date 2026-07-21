# Independent-Connection Terminal CAS Race Design

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

**Date:** 2026-07-15

**Status:** Implemented and verified

**Scope:** Direct SQLite and PostgreSQL repository verification for concurrent,
different terminal proposals issued through independent physical database connections

## 1. Context

The repository terminal contract already serializes a Run row, atomically updates the
Run and inserts its terminal event, and returns an existing durable terminal when a
later proposal loses. Existing tests exercise winner-then-loser behavior
sequentially. They do not establish what happens when two database connections read
and attempt to terminalize the same nonterminal Run concurrently.

This is a persistence CAS verification boundary. It does not add multi-worker Run
execution, distributed scheduling, or a second PostgreSQL runtime owner.

## 2. Authoritative Race Contract

Given one durable `running` Run whose next event sequence is `N`, two distinct
terminal proposals race with `TerminalSequence::Expected(N)`.

The required result is:

1. one transaction obtains the Run-row write lock first and atomically commits its
   typed terminal record plus event;
2. the other transaction observes the committed terminal after acquiring the lock
   and returns that exact existing event;
3. both `commit_terminal` calls return `Ok` with byte-for-byte equivalent
   `RunEvent` values;
4. exactly one returned event equals the event projected from its own proposal at
   `N`; that caller is the winner and the other is the authoritative loser;
5. the loser does not return a generic conflict, retry with a new terminal, append an
   event, or modify the Run row.

The durable store must contain one terminal Run record and exactly one terminal event
at `N`, with no duplicate or missing sequence. A later third proposal returns the
same winner.

## 3. SQLite Connection Boundary

The SQLite test uses a temporary file and creates two separate
`SqliteRunRepository` instances from the same URL. Each instance owns a distinct
SQLx pool, so the two calls cannot be serialized by one borrowed connection.

A third connection acquires the SQLite writer lock before the racers are released
from a start gate. Both repository tasks then enter `commit_terminal` while the lock
is held. Releasing the third transaction lets the two independent pools contend for
the repository's no-op Run-row write lock. SQLite's configured busy handling waits
for the current writer rather than turning the intended CAS loser into a transient
`database is locked` result.

After both calls finish, a fresh third pool begins a write transaction and performs a
no-op update on the Run row. Successful commit proves neither race branch left a
transaction or writer lock behind.

## 4. PostgreSQL Connection and Ownership Boundary

PostgreSQL exclusive-store ownership permits one runtime writer, not one database
connection. The owned `PostgresRunRepository` intentionally uses a pool with multiple
connections. Two clones of that repository share the same owner token and ownership
loss state while their concurrent writes can use two physical PostgreSQL backends.

The test must not call `connect_owned` twice for the same schema and must not add a
raw write path that bypasses generation fencing.

A separately scoped inspector transaction locks the target Run row. After both
repository calls start, `pg_stat_activity` is polled until two distinct backend PIDs
for the repository's unique `application_name` are waiting on the Run-row lock. This
is the deterministic proof that the production method reached the database through
two independent connections. Releasing the inspector lock lets PostgreSQL serialize
the two `FOR UPDATE` acquisitions.

Every repository write transaction still performs the existing owner-token check
under `runtime_ownership ... FOR SHARE` before touching the Run row. The race stays
inside one authoritative runtime generation.

After resolution, an inspector transaction obtains both the Run row and ownership
row with `FOR UPDATE NOWAIT`. The owner monitor's expected advisory lock is not a
residual transaction lock and remains held until normal owner release. No repository
backend may remain `idle in transaction`.

## 5. Verification Matrix

Both backend tests use different typed proposals, for example `completed` and
`failed`, so equality cannot pass accidentally. They assert:

- both calls succeed and return the same exact terminal event;
- exactly one result matches that call's proposed event at the expected sequence;
- the final `RunRecord` status, timestamp, output/error fields, and lifecycle match
  the winner;
- persisted sequences are exactly the nonterminal prefix followed by one terminal;
- only the last event is terminal and it exactly equals the returned winner;
- a third different proposal returns the same authoritative event;
- a fresh writer can immediately lock and no-op update the Run afterward.

The PostgreSQL test remains optional locally when `RUN_HISTORY_POSTGRES_URL` is not
set and required in CI under the existing test policy.

## 6. Production Change Policy

This milestone starts as verification-only. The current implementations already use
SQLite writer serialization and PostgreSQL row-level `FOR UPDATE` inside the same
transaction that updates the Run and inserts the event. Production code changes are
made only if the direct race tests expose a concrete failure.

No repository trait, SQL schema, migration, public API, EventHub, runtime ownership,
or deployment topology changes are in scope.

## 7. Acceptance Criteria

1. SQLite races through two separately connected repository pools.
2. PostgreSQL observes two distinct production repository backends waiting on the
   same Run-row lock under one valid ownership token.
3. Both callers receive one exact authoritative terminal and exactly one is the
   requested winner.
4. Run and event storage contains one complete, contiguous terminal transaction.
5. A third proposal is idempotent and returns the same event.
6. Fresh independent write locks succeed after the race and PostgreSQL has no idle
   transaction residue.
7. Focused and complete repository gates pass before remediation item 9 is marked
   `Addressed`.
