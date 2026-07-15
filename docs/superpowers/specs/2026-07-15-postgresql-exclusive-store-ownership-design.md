# PostgreSQL Exclusive Store Ownership Design

**Date:** 2026-07-15

**Status:** Approved

**Scope:** One active runtime per PostgreSQL Formal V1 history store, database-enforced
fencing, ownership-loss fail-stop behavior, startup reconciliation ownership, and
real-process verification

## 1. Context

The Formal V1 runtime treats startup reconciliation as authoritative: before binding
HTTP, a new process marks every persisted `created` or `running` Run as
`interrupted`. That is correct only when one runtime process owns a PostgreSQL history
store. Today, two processes can open the same store, and the second can reconcile Runs
that the first is still executing.

PostgreSQL row locks and terminal compare-and-set behavior protect individual
transactions, but they do not assign whole-store execution and recovery ownership.
An advisory lock alone is also insufficient because the repository writes through a
pool of independent connections. If a dedicated advisory-lock connection is lost,
another process can acquire the lock while surviving pool connections in the old
process can still write.

The platform therefore needs both:

1. session ownership, so a second process fails before reconciliation or HTTP bind;
2. a persistent generation fence, so a stale process cannot write after takeover.

## 2. Decision

Every PostgreSQL Formal V1 history store has exactly one active runtime owner. The
owner acquires the store during startup, retains it through normal runtime and clean
drain, and releases it only after runtime and HTTP shutdown finish.

The implementation combines:

- one session-level PostgreSQL advisory lock held on a dedicated connection;
- one singleton `runtime_ownership` row containing an opaque process UUID and a
  monotonically increasing generation;
- a fencing check at the start of every repository write transaction;
- a bounded monitor for the dedicated connection;
- process-level fail-stop behavior when ownership is lost.

The binary does not wait as an internal standby. A contender that cannot acquire the
store exits nonzero. Restart and replacement policy belongs to the deployment system.

SQLite behavior is unchanged.

## 3. Ownership Data Model

The PostgreSQL Formal V1 baseline migration adds:

```sql
CREATE TABLE runtime_ownership (
    singleton SMALLINT PRIMARY KEY CHECK (singleton = 1),
    generation BIGINT NOT NULL CHECK (generation >= 0),
    owner_id TEXT,
    claimed_at TIMESTAMPTZ,
    CHECK (
        (generation = 0 AND owner_id IS NULL AND claimed_at IS NULL)
        OR
        (generation > 0 AND owner_id IS NOT NULL AND claimed_at IS NOT NULL)
    )
);

INSERT INTO runtime_ownership (singleton, generation, owner_id, claimed_at)
VALUES (1, 0, NULL, NULL);
```

The project remains in the disposable-development-data phase. This table is folded
into `migrations/formal_v1/postgres/202607100001_formal_v1.sql`; no compatibility
migration is added. Existing development PostgreSQL schemas must be recreated.

SQLite does not receive an equivalent table. The existing migration-layout test keeps
the three Run data tables equivalent across backends and separately asserts that
`runtime_ownership` is a PostgreSQL-only deployment-safety table.

## 4. Store Identity and Advisory Lock

Before migrations run, the dedicated ownership connection resolves the OID of its
current PostgreSQL schema. PostgreSQL advisory locks are local to one database, so a
64-bit key formed from a fixed Insight Agent Platform namespace in the high bits and
the schema OID in the low bits identifies one Formal V1 store without relying on a
table that may not exist yet.

This definition gives the required store identity:

- two runtimes whose connections resolve the same current schema compete for the same
  lock;
- isolated schemas in one PostgreSQL database have different schema OIDs and can run
  independently;
- recreating a disposable schema creates a new store and therefore a new lock key.

Acquiring ownership before migrations prevents a contender or rolling replacement
from applying schema changes while the active runtime still owns the store.

The ownership connection invokes `pg_try_advisory_lock` exactly once. Failure returns
`HISTORY_STORE_ALREADY_OWNED`; the process does not update the ownership row, run
startup reconciliation, or bind HTTP.

The dedicated connection is never returned to the query pool. With the current pool
limit, PostgreSQL operation uses at most the configured pool connections plus one
ownership connection.

## 5. Claim Protocol

After acquiring the advisory lock, the dedicated connection runs the Formal V1
migrations while the store is exclusively owned. Running migrations on that same
connection guarantees that the lock identity and migration target use the same
database and schema. The connection then claims the persistent fence:

1. begin a transaction;
2. select the singleton ownership row `FOR UPDATE`;
3. increment `generation` exactly once;
4. store a freshly generated UUID string in `owner_id` and database time in
   `claimed_at`;
5. commit and retain the resulting `(owner_id, generation)` as an immutable token.

The claim is bounded by the existing `runtime.journal_operation_timeout`. If an old
write transaction still holds a shared ownership-row lock, the claim waits for that
transaction up to this budget. Timeout or any invalid/missing ownership row fails
startup and drops the dedicated connection.

A contender that fails the advisory-lock attempt never advances the generation. A
normal replacement advances it once.

## 6. Write Fencing

`PostgresRunRepository` stores the immutable ownership token and a shared sticky
ownership state. Every mutating repository method uses one internal transaction entry
point:

1. begin a PostgreSQL transaction;
2. select the singleton ownership row `FOR SHARE`;
3. compare both `owner_id` and `generation` with the repository token;
4. on an exact match, perform the existing repository mutation and commit;
5. on mismatch or a missing row, roll back, mark ownership permanently lost, and
   return `HISTORY_OWNERSHIP_LOST`.

This applies to:

- `create_run`;
- `mark_running`;
- `append_events`;
- `put_node_output`;
- `commit_terminal`;
- `mark_incomplete_interrupted`.

The claim transaction's `FOR UPDATE` conflicts with old writers' `FOR SHARE` locks.
It therefore forms a cutover barrier: writes that started before takeover may finish;
the new generation is published only after they finish; and all later stale writes
fail the token check.

`get_run` and `list_events_after` remain readable during fail-stop drain and are not
fenced. `check_health` checks both the sticky ownership state and the currently stored
token. A token mismatch discovered by `check_health` or a write triggers the same
process-level ownership-loss signal as the dedicated-connection monitor.

Existing terminal retry behavior must never retry `HISTORY_OWNERSHIP_LOST` as an
ordinary transient read or write error.

## 7. Component Boundaries

Ownership-specific code lives in `src/history/postgres/ownership.rs`. It owns:

- advisory-lock acquisition;
- generation claim;
- the immutable token;
- the dedicated connection monitor;
- sticky loss notification;
- explicit clean release.

`src/history/postgres.rs` retains the repository queries and adds the token plus the
shared `begin_owned_write` transaction entry point. The generic `RunRepository` trait
does not gain PostgreSQL-specific methods.

Repository initialization returns an internal package containing:

- `Arc<dyn RunRepository>` for the existing runtime layers;
- an optional PostgreSQL store-owner guard and loss receiver.

The binary retains the owner guard for the entire process lifecycle. SQLite
initialization returns no guard. EventHub, RunService, nodes, resources, and the public
API remain backend-agnostic.

The binary target may require public-but-hidden library types to carry this package;
they are infrastructure APIs, not supported extension or client contracts.

## 8. Ownership Monitoring

After a successful claim, the dedicated connection is probed once per second. Each
probe is bounded by the existing `runtime.readiness_probe_timeout`.

Any probe error or timeout permanently loses ownership. The monitor:

1. marks the shared ownership state lost exactly once;
2. notifies the process lifecycle;
3. drops the dedicated connection;
4. never reconnects or reacquires from the same process.

Failing closed on an ambiguous timeout may reduce availability, but it prevents an old
process from silently returning to service after another process has advanced the
generation.

Loss notification is idempotent and sticky. A repository token mismatch, monitor
failure, or unexpected monitor termination produces the same state transition.

## 9. Startup, Runtime Loss, and Shutdown

PostgreSQL startup order is:

1. load configuration and compile enabled Agents;
2. open the dedicated ownership connection;
3. resolve the current schema and acquire its namespaced advisory lock;
4. run the baseline migration on that connection under exclusive ownership;
5. claim a new generation;
6. connect the query pool and construct the fenced repository;
7. construct EventHub and RunService;
8. perform startup reconciliation under the fence;
9. bind HTTP and report readiness.

The ownership monitor remains active during startup. Loss before HTTP bind fails
startup.

At runtime, ownership loss performs a fail-stop sequence:

1. close Run admission immediately;
2. make readiness fail through the existing `503/RUNTIME_UNHEALTHY` response;
3. ask preparing and active Runs to stop;
4. attempt runtime and HTTP drain within the existing shutdown hard deadline;
5. exit nonzero regardless of whether drain otherwise finishes cleanly.

The old process does not attempt to persist after fencing rejects it. A replacement
owner runs normal startup reconciliation and converts any durable `created` or
`running` Run to the existing `interrupted/RUN_INTERRUPTED` terminal exactly once.

Normal signal shutdown keeps the ownership connection and token until runtime drain
and HTTP graceful shutdown both finish. It then explicitly unlocks and drops the
dedicated connection. Unlock failure makes shutdown nonzero; connection drop still
allows PostgreSQL to release the session lock.

Ownership loss is sticky across races. If a normal signal starts drain and ownership
is lost before explicit release, final process status is nonzero. A hard-deadline exit
drops the ownership connection as the runtime terminates.

## 10. Error and Public Contract

Two internal stable history codes define the ownership boundary:

- `HISTORY_STORE_ALREADY_OWNED`: another process owns the target store during
  startup;
- `HISTORY_OWNERSHIP_LOST`: the current repository token is no longer authoritative.

Messages and logs must not include the database URL, credentials, owner UUID, advisory
key, or another process's identity. Structured logs may record the stable code and the
fact that acquisition or ownership was lost.

No public API shape changes. Health continues to use the existing sanitized response,
Run creation during drain continues to return `503/RUN_SERVICE_UNAVAILABLE`, and Run,
event, terminal, and Agent contracts are unchanged.

## 11. Verification

### 11.1 Migration and repository tests

PostgreSQL tests must prove:

1. the baseline creates one constrained ownership row;
2. two isolated schemas can be owned concurrently;
3. a second repository for the same store returns
   `HISTORY_STORE_ALREADY_OWNED` without advancing generation;
4. clean release permits a new owner and advances generation once;
5. terminating only the old advisory-lock backend permits a replacement to claim;
6. every old mutating repository method then returns
   `HISTORY_OWNERSHIP_LOST`, while reads remain available;
7. the replacement repository can write normally;
8. a held shared ownership-row lock delays generation advancement until the old
   transaction completes;
9. `check_health` fails after fencing-token loss;
10. claim and monitor failures are bounded and sanitized.

The PostgreSQL gate keeps the repository's current environment policy:
`RUN_HISTORY_POSTGRES_URL` is optional locally and required in CI.

### 11.2 Real-process tests

A real-binary PostgreSQL test uses a fresh schema, different loopback ports, and a
blocking OpenAI-compatible fixture.

It proves:

1. the first process reaches ready and holds a real `running` Run;
2. a second process targeting the same store exits nonzero before listener bind;
3. the contender does not reconcile or otherwise change the first process's Run;
4. contender output contains no database URL, credential sentinel, owner UUID, or
   advisory key;
5. terminating the first process's ownership backend changes readiness to 503 and
   closes admission;
6. a replacement process claims the store and reconciles the incomplete Run to one
   `interrupted/RUN_INTERRUPTED` terminal;
7. the fenced old process cannot overwrite the replacement's terminal and exits
   nonzero;
8. a cleanly drained owner releases only after drain, after which a new process can
   reach ready.

### 11.3 Regression gates

The completion gate includes:

- `cargo fmt --check`;
- complete `cargo test --locked` coverage;
- the real PostgreSQL gate with `RUN_HISTORY_POSTGRES_URL`;
- `cargo clippy --locked --all-targets --all-features -- -D warnings`;
- `cargo deny check`.

SQLite quickstart, binary lifecycle, public API, terminal convergence, and recovery
tests must remain unchanged in behavior.

## 12. Documentation

The README PostgreSQL section must state:

- one active runtime is allowed per Formal V1 PostgreSQL store;
- contenders fail startup rather than waiting as standby;
- ownership loss makes readiness fail and the process exit nonzero;
- a deployment supervisor is responsible for restart or replacement.

The current remediation-status document marks PostgreSQL exclusive-store topology as
implemented only after the real PostgreSQL and real-process gates pass.

## 13. Out of Scope

- Internal standby waiting, retry loops, or automatic reacquisition.
- TTL or wall-clock leases.
- An external leader-election service.
- Multiple active workers, per-Run ownership, sharding, or distributed scheduling.
- SQLite multi-process ownership.
- Read-replica routing.
- Ownership management APIs or public ownership metadata.
- Kubernetes, systemd, or other orchestrator manifests.
- Changes to public Run, Agent, event, terminal, SSE, or authentication contracts.

## 14. Acceptance Criteria

1. One PostgreSQL history store has at most one authoritative runtime writer.
2. A contender fails before migrations, reconciliation, and HTTP bind.
3. Store identity distinguishes isolated PostgreSQL schemas.
4. Takeover waits for begun old writes and fences all later stale writes.
5. Advisory-connection loss closes admission, fails readiness, and produces a
   bounded nonzero process exit without automatic reacquisition.
6. A replacement reconciles incomplete Runs exactly once and the old process cannot
   overwrite the result.
7. Clean shutdown retains ownership through runtime and HTTP drain.
8. SQLite and all public contracts remain unchanged.
9. Focused, real-PostgreSQL, real-binary, and complete repository gates pass.
