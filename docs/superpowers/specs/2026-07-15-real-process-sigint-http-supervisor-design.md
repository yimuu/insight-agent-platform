# Real-Process SIGINT and HTTP Supervisor Design

**Date:** 2026-07-15

**Status:** Implemented and verified

**Scope:** Unix real-binary SIGINT coverage and deterministic verification of the
private production lifecycle supervisor's unexpected HTTP-server completion path

## 1. Context

Before this milestone, the real-process lifecycle suite already covered SIGTERM,
forced process death, startup reconciliation, and a bounded hard-deadline failure.
This milestone closes two review boundaries:

1. deliver SIGINT to the actual production binary while Attached and Detached Runs
   are in flight;
2. prove that an HTTP server future which finishes before the platform asks it to
   shut down causes a bounded fail-stop drain rather than a clean exit.

These boundaries cannot use the same external trigger with the current HTTP stack.
The repository locks Axum 0.8.9. Its `TcpListener` implementation retries accept
errors indefinitely, and `WithGracefulShutdown` returns `Ok(())` only after its
shutdown future completes. The production shutdown future is a private oneshot whose
sender remains held until another lifecycle trigger wins. Network traffic, malformed
requests, client disconnects, listener exhaustion, and ordinary socket errors
therefore cannot make the current production server future finish early.

This design keeps that fact explicit. SIGINT is verified against the real binary.
The structurally unreachable defensive HTTP branch is verified by extracting the
private production supervisor and injecting a finite future directly in unit tests.
No runtime fault switch is added.

## 2. Trigger and Outcome Contract

Lifecycle causes have this fail-stop precedence:

```text
PostgreSQL ownership loss
  > outer Run-task fatality
  > HTTP server completed before its graceful command
  > shutdown-signal handler failure
  > successful SIGINT or SIGTERM
```

The first matching sticky fatal state determines the final process result even when
another trigger was selected initially. Ownership loss never performs a normal owner
release. Every other path best-effort releases a healthy owner after runtime and HTTP
drain work completes.

Successful SIGINT/SIGTERM is a clean zero exit only when runtime drain and HTTP
graceful shutdown both succeed. Ownership loss, runtime fatality, unexpected HTTP
completion, signal-handler failure, runtime drain failure, HTTP shutdown failure, or
the hard deadline produces a nonzero exit.

## 3. HTTP Completion Linearization

`HttpStopped` means the supervised HTTP future completed, with either `Ok` or `Err`,
before the supervisor sent its private graceful-shutdown command.

The supervisor follows this order:

1. choose the initial trigger with biased precedence;
2. synchronously close Run admission;
3. start cooperative runtime drain;
4. while runtime drain is pending, continue polling the HTTP future;
5. if HTTP completes first, latch `HttpStopped` and continue runtime drain;
6. if runtime drain completes while HTTP is still pending, send the graceful command
   and then await the HTTP result;
7. combine sticky ownership/runtime fatal state, latched early HTTP completion,
   signal-handler status, runtime result, and HTTP result into the final outcome;
8. bound the complete sequence by the configured hard deadline.

The HTTP branch uses a fixed sanitized process error when the best-effort drain
succeeds. An underlying HTTP error may be logged, but is not required to become the
stable external diagnostic.

Before this milestone, the implementation did not poll the server while awaiting
runtime drain. Under a future finite server implementation, a Signal could therefore
win first, the HTTP future could finish during drain, and the later graceful command
could make the exit appear clean. This milestone closes that supervision gap.

## 4. Run Drain Contract

Both successful signal drain and unexpected HTTP completion use the same production
`RunService::shutdown` path:

- admission closes before any further Run can be accepted;
- in-flight Attached Runs become `cancelled` with `RUN_CANCELLED`;
- in-flight Detached Runs become `interrupted` with `RUN_INTERRUPTED`;
- each Run has exactly one durable matching terminal event;
- provider connections and Attached SSE bodies close;
- background terminal recovery owners converge before shutdown returns.

If the hard deadline prevents durable convergence, the process still exits nonzero.
The next process reconciles any remaining Created/Running Run once; it must not
pretend that an incomplete drain already produced attachment-specific terminals.

## 5. Real-Process SIGINT Matrix

The existing SIGTERM scenario becomes a shared Unix signal harness with separate
SIGTERM and SIGINT tests. For SIGINT, the test:

1. spawns `CARGO_BIN_EXE_insight-agent-platform` with temporary SQLite/config;
2. creates one Detached and one Attached Run;
3. waits for two model connections and both durable `running` states;
4. invokes `kill -INT <direct-child-pid>`;
5. requires both model sockets and the Attached SSE body to close;
6. requires process exit zero;
7. reads SQLite directly and requires Cancelled/Interrupted records and exactly one
   matching terminal event each;
8. restarts the real binary, checks the public API, shuts it down, and proves raw Run
   records and event lists did not change.

The suite is already Unix-only. This establishes Linux/macOS SIGINT behavior and
does not claim Windows console-control coverage.

## 6. Private Supervisor Verification

The lifecycle selection and drain logic is extracted into a private function used by
production `main`. Its dependencies are futures and existing runtime/ownership
objects, not environment variables or test configuration.

Unit tests may supply:

- an immediately completed HTTP future to enter `HttpStopped`;
- controlled HTTP and runtime futures to prove completion during runtime drain is
  latched before any graceful command;
- ready fatal futures to verify deterministic precedence;
- an erroring signal future to prove handler failure cannot masquerade as a clean
  signal.

At least one `HttpStopped` test uses a real in-memory SQLite repository, EventHub,
RunService, one active Attached Run, and one active Detached Run. It requires the
same exact durable terminal contract as signal drain and a final HTTP-stop error.

No environment hook, debug-only route, formal config field, Cargo feature, SIGUSR
command, or public test API is permitted. The test demonstrates supervisor behavior;
it does not claim to reproduce a natural Axum accept failure that Axum 0.8.9 cannot
produce.

## 7. Signal Error Hardening

The shutdown-signal waiter returns an explicit `io::Result<()>`.

- a delivered SIGINT or SIGTERM returns `Ok(())`;
- signal registration failure, Ctrl-C handler failure, or a closed Unix signal stream
  returns `Err`;
- the supervisor closes admission, attempts the normal bounded drain, and exits
  nonzero with a fixed signal-handler diagnostic.

An internal handler failure must never be logged and classified as a successful user
shutdown.

## 8. Production Change Policy

Allowed production changes are limited to:

- extracting the private lifecycle supervisor actually called by `main`;
- biased trigger selection and continuous HTTP supervision during runtime drain;
- explicit signal-handler errors;
- ensuring healthy PostgreSQL owner release is attempted after HTTP-result errors.

Public HTTP/API/config/schema/history/runtime contracts do not change. No new
dependency, migration, endpoint, signal command, or deployment topology is added.

## 9. Acceptance Criteria

1. Direct real-binary SIGINT passes the full Attached/Detached terminal and restart
   matrix; the existing SIGTERM coverage remains intact.
2. The private production supervisor treats finite HTTP `Ok` and `Err` completion
   before its graceful command as nonzero `HttpStopped`.
3. HTTP completion during runtime drain is latched deterministically.
4. Ownership loss and runtime fatality retain precedence over HTTP completion.
5. Signal-handler failure is nonzero and cannot be classified as clean shutdown.
6. HTTP-stop drain produces exact one-terminal Attached/Detached persistence through
   a real RunService/repository boundary.
7. No production fault-injection surface is introduced.
8. Focused real-process tests, complete gates, and independent review pass before
   remediation item 15 is marked `Addressed`.
