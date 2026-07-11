# Live-Only SSE Design

**Date:** 2026-07-11
**Status:** Pending written-spec review
**Scope:** Remove public SSE replay and make Attached streaming live-only

## 1. Context

Formal V1 currently exposes two SSE paths:

- `POST /v1/agents/{agent_id}/runs/stream` creates an Attached Run and streams it from the beginning.
- `GET /v1/runs/{run_id}/events?after_seq=N` subscribes to an existing Run, reads durable history after `N`, merges it with an in-memory replay ring, and then switches to live delivery.

The replay path makes event recovery durable across disconnects, process restarts, and instances, but every reconnect performs a database history query. A large reconnect wave can consume the history connection pool, deserialize many event payloads, and compete with the journal writes that determine Run correctness. The runtime does not yet have replay-specific load shedding, singleflight, or client backoff enforcement.

Replay is not required for the current stable development baseline. The simpler contract is one live stream attached atomically to one newly created Run. Detached execution remains available through polling.

## 2. Goals

- Keep real-time SSE for a newly created Attached Run.
- Remove public event replay and all `after_seq` behavior.
- Ensure an Attached stream subscribes before its Run begins, so its live stream has no creation-time gap.
- Close SSE immediately after a durable Run terminal event.
- Cancel a nonterminal Attached Run when its stream disconnects, without a reconnect grace period.
- Detect silent half-open connections with a configurable five-second keepalive target.
- Keep event persistence, event sequence numbers, audit history, repository recovery, and terminal-state guarantees unchanged.
- Keep Detached Runs independent of SSE and queryable through the Run resource.
- Delete replay-only runtime state and configuration rather than retaining a disabled compatibility branch.

## 3. Non-goals

- SSE reconnect, resume, cursor pagination, or missed-event recovery.
- `Last-Event-ID` support.
- Ring-only best-effort replay.
- A feature flag that can re-enable replay.
- Redis, a read replica, an event cache, or replay load shedding.
- A public event-history or audit API.
- Changing Agent execution, fork/join scheduling, event schemas, Run statuses, or database tables.
- Guaranteeing a hard five-second half-open detection deadline across every operating system, proxy, and network.

## 4. Chosen Approach

Delete the existing public recovery contract and keep only atomic Attached streaming.

Two alternatives were rejected:

1. **Keep replay behind a default-off flag.** This retains two subscription contracts, replay state, database queries, and a large test surface for a capability with no current consumer.
2. **Serve replay only from the process-local ring.** This behaves differently after process restart, ring eviction, load-balancer redistribution, and horizontal scaling. A formal endpoint should not sometimes recover and sometimes silently omit events.

Removing the endpoint gives one reliable rule: Attached streams receive events from Run creation until terminal state while the connection remains alive. Detached clients poll the Run resource.

## 5. HTTP Contract

Remove:

```text
GET /v1/runs/{run_id}/events
```

The route is absent and returns the router's normal `404` response. The runtime accepts neither `after_seq` nor `Last-Event-ID` for recovery.

Keep:

```text
POST   /v1/agents/{agent_id}/runs/stream
POST   /v1/agents/{agent_id}/runs
GET    /v1/runs/{run_id}
DELETE /v1/runs/{run_id}
```

### 5.1 Attached Run

`POST /runs/stream` validates input, allocates the Run, opens its EventHub live state, subscribes to the broadcast channel, and only then launches execution. Its response continues to expose `x-run-id` and `x-request-id`.

The stream emits ordinary Formal V1 events with their global `seq` and SSE `id`. Keeping those fields supports ordering and audit correlation; they no longer imply resumability.

### 5.2 Detached Run

`POST /runs` starts a Run whose lifecycle does not depend on a subscriber. Clients use `GET /v1/runs/{run_id}` to poll status and retrieve the final output. `DELETE` remains the explicit cancellation mechanism.

No live subscription endpoint is provided for a Detached Run. Adding one after Run creation would create an unavoidable gap before subscription.

## 6. SSE Lifecycle

### 6.1 Normal completion

The coordinator persists the Run terminal update and terminal event through EventHub. SSE emits that terminal event and immediately ends the response stream. There is no post-terminal delay and no wait for the next keepalive tick.

Terminal event types remain:

```text
run.completed
run.failed
run.cancelled
run.interrupted
```

### 6.2 Client disconnect

Dropping the Attached response releases its subscription lease. If the Run is still active, the service immediately requests `StopReason::Cancelled`. There is no reconnect generation, subscriber grace timer, or delayed cancellation task.

A terminal event may race with response-body cleanup. Terminal persistence already occurs before terminal broadcast, and existing one-terminal-state logic prevents a late cancellation request from rewriting the durable terminal result.

### 6.3 Half-open connection

SSE writes a keepalive every configured interval. A failed write causes the body/lease to drop and triggers Attached cancellation. With the default interval of five seconds, the detection target is the next write, normally within five seconds. TCP stacks, proxies, scheduling, and buffering mean this is an operational target, not a strict deadline.

### 6.4 Stream transport failure

Encoding failure, broadcast lag, or subscription closure terminates the stream. A transport error may be emitted when the connection still permits a final event, but it must not instruct the client to reconnect or include an `after_seq` cursor. Lease release then cancels an active Attached Run.

## 7. Configuration

Remove strict runtime fields:

```yaml
attached_reconnect_grace: 10s
replay_ring_capacity: 512
```

Add:

```yaml
runtime:
  sse_keep_alive_interval: 5s
```

`sse_keep_alive_interval` must parse as a strictly positive duration. The default checked-in configuration is exactly `5s`.

Although the field is loaded with the runtime configuration, the HTTP/SSE layer consumes it. `RunService` and the scheduler do not own transport keepalive policy. `FormalApiState` or an equivalent focused SSE configuration boundary carries the resolved duration to `response_stream`.

The existing `run_timeout: 5m` remains the maximum Run execution duration. No separate SSE maximum duration is added.

## 8. Runtime and EventHub Simplification

Remove public replay behavior:

- `RunService::subscribe(run_id, after_seq)`.
- `EventHub::replay_after` and `EventHub::replay_page_after`.
- `EventHub::subscribe_existing`.
- `ReplayPage`.
- The per-Run replay ring and ring capacity.
- `RunSubscription` replay queue, truncation flag, live/replay switching, and replay deduplication.
- `ReplayTruncated` and reconnect cursor transport behavior.
- Attached subscriber counts, reconnect generations, grace deadlines, and delayed cancellation tasks.

Retain the minimum live path:

```text
create Attached Run
-> open EventHub Run state
-> subscribe live broadcast receiver
-> launch Run
-> emit ordered live events
-> terminal event or transport close
-> drop lease
```

The live subscription may continue tracking its most recently delivered sequence for diagnostics and lag reporting, but that value is not a public recovery cursor.

## 9. Persistence Boundary

Do not remove event persistence.

The following remain unchanged:

- Formal event `seq` allocation.
- Journal persistence before broadcast.
- SQLite and PostgreSQL `run_events` rows and `(run_id, seq)` uniqueness.
- Node outputs and Run terminal records.
- Repository `list_events_after` where it is required for journal reconciliation, uncertain-commit recovery, startup behavior, audit tests, or future internal administration.
- Branch lifecycle events and fork/join ordering.

The public SSE path must not call `list_events_after`. Database event history becomes an internal durability/audit mechanism, not a reconnect transport.

No migration is needed because the stored schema does not change.

## 10. Failure Semantics

- A terminal event closes the stream after it is emitted.
- A client disconnect or unrecoverable transport failure requests Attached cancellation immediately.
- An Attached Run cancellation still drains concurrent branches and commits one durable terminal state.
- A Detached Run continues without subscribers and is unaffected by streaming changes.
- Journal or repository failure remains infrastructure-fatal and preserves service-health degradation behavior.
- A lagged SSE subscriber cannot reconnect for missing events; the stream closes and its Attached Run is cancelled.
- Clients that know the `run_id` may query the Run resource after any stream failure, but no event gap is repaired.

## 11. Interface Changes and Reasons

This milestone deliberately changes HTTP and internal Rust interfaces without a compatibility adapter.

- Delete `GET /v1/runs/{run_id}/events` because a live-only late subscription cannot promise a complete stream.
- Delete `after_seq` because retaining a cursor implies replay support.
- Remove `attached_reconnect_grace` because reconnect is no longer supported; immediate cancellation is the only coherent Attached behavior.
- Remove `replay_ring_capacity` because replay state no longer exists.
- Add `sse_keep_alive_interval` because half-open detection is now the only time-based SSE transport policy.
- Simplify RunSubscription and EventHub replay types because dead code would preserve accidental complexity and invite an unsupported recovery path.

Event schema version stays `1`: event envelopes and meanings do not change. Agent DSL and Run resource HTTP contracts also remain unchanged.

## 12. Test Strategy

### 12.1 HTTP and SSE

- `POST /runs/stream` subscribes before execution and receives `run.created` as its first Run event.
- Terminal event is the last SSE item and is followed by EOF immediately.
- `GET /v1/runs/{run_id}/events` returns `404`.
- `after_seq` and `Last-Event-ID` do not activate any recovery path.
- Transport error payloads contain no reconnect instruction or cursor.
- SSE keepalive uses the configured five-second interval.

### 12.2 Disconnect and Run lifecycle

- Dropping an Attached subscription requests cancellation without advancing a grace timer.
- Parallel branch tasks observe cancellation and drain before terminal completion.
- Terminal-versus-disconnect races persist exactly one terminal result.
- Detached Runs complete with zero subscribers and remain queryable.
- Explicit `DELETE` cancellation remains idempotent.

### 12.3 No database replay

- A spy repository proves Attached SSE performs no history-list query.
- Event writes still occur before broadcast.
- SQLite and PostgreSQL still persist and read event history through repository-level contract tests.
- Journal uncertain-commit recovery continues to use repository history where required internally.

### 12.4 Configuration

- Strict config accepts `sse_keep_alive_interval: 5s`.
- Zero or invalid keepalive durations prevent startup.
- Removed `attached_reconnect_grace` and `replay_ring_capacity` fields are rejected as unknown.
- README and checked-in configuration describe live-only behavior and polling for Detached results.

## 13. Acceptance Criteria

The milestone is complete when:

1. The only public SSE operation is atomic Attached Run creation and streaming.
2. No public request can replay historical events or provide a recovery cursor.
3. The SSE request path executes no database history query.
4. A durable terminal event closes SSE immediately.
5. An active Attached Run is cancelled immediately when its subscription lease drops.
6. The configured keepalive interval is exactly five seconds and is transport-owned.
7. Detached Runs continue independently and expose final state through `GET /v1/runs/{run_id}`.
8. Event persistence, sequence ordering, parallel branch draining, journal recovery, and SQLite/PostgreSQL parity remain intact.
9. Format, strict Clippy, all targets, audit, deny, and real PostgreSQL gates pass.
