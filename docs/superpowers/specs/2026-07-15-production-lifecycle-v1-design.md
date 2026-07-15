# Production Lifecycle V1 Design

**Date:** 2026-07-15

**Status:** Implemented; residual verification tracked separately

**Scope:** Process startup, public liveness/readiness probes, bounded history
readiness checks, signal-driven draining, real-binary recovery coverage, and shutdown
deadlines

## 1. Context

The production binary already initializes configuration, compiles enabled Agents,
opens and migrates history, reconciles incomplete Runs, binds HTTP, and handles
SIGINT/SIGTERM. `RunService::shutdown` already closes admission and preserves the
attachment-specific terminal contract: Attached Runs become `cancelled`, while
Detached Runs become `interrupted`.

The remaining gap is at the process boundary:

- `/health` combines admission and journal state without distinguishing liveness from
  readiness;
- readiness does not actively establish that the history backend can currently serve
  a request;
- signal handling begins HTTP and runtime shutdown concurrently, so draining is not a
  deterministic externally observable phase;
- the runtime and hard shutdown deadlines are constants and therefore cannot be
  validated quickly through the real binary;
- real-binary coverage does not exercise in-flight signal handling or crash recovery.

## 2. Lifecycle Contract

ProductionLifecycleV1 has five externally meaningful states. The implementation may
derive them from existing process and service ownership rather than introducing a
second mutable state machine.

| State | HTTP | New Runs | Liveness | Readiness |
|---|---|---|---|---|
| Starting | not bound | closed | unreachable | unreachable |
| Ready | serving | open | 200 | 200 |
| Draining | serving until runtime drain finishes | closed | 200 | 503 |
| Degraded | serving | closed for irreversible journal failure; repository operations remain authoritative for transient history loss | 200 | 503 |
| Stopped | closed | closed | unreachable | unreachable |

Startup preserves the existing order: configuration, Agent compilation, repository
initialization/migration, and startup reconciliation must all succeed before the
listener is bound. There is no early listener that reports `starting`.

Readiness is fail closed but may recover after a transient history probe failure. A
failed probe does not claim ownership of process restart policy. An irreversible
journal failure continues to close Run admission through the existing RunService
health boundary.

## 3. Public Probe Contract

All probe routes are public and remain outside `/v1` bearer authentication.
All three responses include `Cache-Control: no-store` so intermediaries cannot reuse a
stale ready response during draining.

### `GET /health/live`

If the HTTP handler can run, liveness returns:

```json
{"code":"OK","message":"ok","data":{"status":"live"}}
```

with HTTP 200. It does not query history, inspect journal health, or reject a draining
process. Process supervisors use this endpoint only to determine whether the process
and HTTP runtime are responsive.

### `GET /health/ready`

Readiness requires all of the following:

1. Run admission is open;
2. the event journal is healthy;
3. a bounded repository `check_health` operation succeeds;
4. admission and journal health are still valid after the asynchronous repository
   check.

Success preserves the existing health envelope:

```json
{"code":"OK","message":"ok","data":{"status":"ok"}}
```

Failure returns HTTP 503 with the existing sanitized contract:

```json
{"code":"RUNTIME_UNHEALTHY","message":"runtime is unhealthy","data":{"status":"degraded"}}
```

No database URL, SQL, backend error, Run input, or credential may appear in a probe
response.

### `GET /health`

`/health` remains a direct compatibility alias for `/health/ready`. It must have the
same status code and JSON body in every state; it is not implemented as a redirect.

Unsupported methods retain Axum's 405 behavior and unknown paths remain 404.

## 4. Repository Readiness

`RunRepository` gains an explicit asynchronous health operation.

- SQLite and PostgreSQL execute a minimal `SELECT 1` through their existing pools.
- The call is bounded by `runtime.readiness_probe_timeout` at the RunService boundary.
- Concurrent calls share one in-flight operation. Both success and failure are cached
  for at most 250ms, limiting probe amplification against the shared repository pool.
- The configured timeout includes both waiting for the shared probe and executing the
  repository operation.
- Backend errors remain private and map only to the sanitized not-ready response.
- The readiness probe is advisory admission evidence, not a transaction spanning a
  later Run creation. Repository operations remain authoritative under races.
- Test repositories must implement the operation explicitly so future repository
  implementations cannot accidentally omit the readiness contract.

## 5. Signal and Drain Ordering

SIGINT and SIGTERM use one clean-drain sequence:

1. synchronously close Run admission;
2. keep HTTP serving so liveness, readiness, Run lookup, and requests already in flight
   have a stable draining window;
3. ask all preparing and active Runs to stop;
4. preserve Attached `cancelled` and Detached `interrupted` terminals;
5. wait for terminal persistence and background recovery ownership within
   `runtime.shutdown_grace_period`;
6. trigger Axum graceful shutdown only after runtime drain completes or fails;
7. bound the entire sequence by `runtime.shutdown_hard_deadline`.

Once admission is closed, a Run creation request that still reaches the application
returns HTTP 503 `RUN_SERVICE_UNAVAILABLE`. Capacity exhaustion and other true state
conflicts remain HTTP 409 `RUN_CONFLICT`.

A clean signal drain exits zero only if runtime and HTTP drain successfully. Runtime
drain failure, HTTP failure, or the hard deadline produces a non-zero process exit.
The hard deadline bounds asynchronous shutdown ownership; it does not attempt to make
arbitrary non-cooperative native blocking code preemptible.

## 6. Runtime Configuration

The following optional runtime fields are added with backward-compatible defaults:

```yaml
runtime:
  readiness_probe_timeout: 2s
  shutdown_grace_period: 30s
  shutdown_hard_deadline: 35s
```

All values must be positive. `shutdown_hard_deadline` must be strictly greater than
`shutdown_grace_period`, preserving time for HTTP drain after runtime drain. Existing
configs that omit the fields retain the current 30/35 second behavior.

## 7. Real-Binary Verification

The test boundary remains the real `CARGO_BIN_EXE_insight-agent-platform` executable,
temporary configuration, loopback networking, and SQLite history.

Coverage must prove:

1. the binary waits on `/health/ready`, while `/health` is an exact compatibility
   alias and `/health/live` is public;
2. a local blocking OpenAI-compatible fixture keeps real Attached and Detached Runs
   in flight during SIGTERM;
3. clean shutdown persists Attached `cancelled/RUN_CANCELLED` and Detached
   `interrupted/RUN_INTERRUPTED`, closes the model sockets, and exits zero;
4. those terminals survive restart unchanged;
5. a forcibly killed process leaves a real running row, and the next process
   reconciles it to `interrupted` before its first successful readiness response;
6. a second restart is idempotent and does not append another terminal event;
7. an intentionally incomplete HTTP request holds Axum drain open, and short configured
   deadlines force a bounded non-zero exit without creating a Run.

In-process tests additionally prove the stable draining probe window and the 503 Run
admission response without relying on signal/request scheduling races.

## 8. Out of Scope

- PostgreSQL exclusive-store ownership or distributed worker coordination;
- automatic journal worker reconstruction;
- orchestrator-specific deployment manifests;
- forced preemption of CPU-bound or foreign blocking executors;
- changes to Run terminal, persistence schema, SSE replay, Agent DSL, or public Agent
  metadata.

## 9. Acceptance Criteria

1. `/health` remains compatible and equals `/health/ready` exactly.
2. Liveness and readiness are distinct, public, sanitized contracts.
3. Readiness includes a bounded real-repository probe.
4. Signal drain closes admission before runtime drain and closes HTTP afterward.
5. Draining Run creation returns 503 and creates no durable Run.
6. Real Attached/Detached signal terminals and crash recovery are persisted correctly.
7. Configured hard deadline behavior is proven against the real executable.
8. Focused and complete repository gates pass.
9. Concurrent public readiness requests produce one repository probe per cache window,
   and probe responses cannot be reused by HTTP caches.
