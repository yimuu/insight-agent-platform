# Real-Process SIGINT and HTTP Supervisor Implementation Plan

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

**Goal:** Close the remaining lifecycle verification boundary without adding a
production HTTP fault-injection backdoor.

**Design:**
`docs/superpowers/specs/2026-07-15-real-process-sigint-http-supervisor-design.md`

**Status:** Completed

## Constraints

- Deliver SIGINT to the actual production child process.
- Preserve the existing SIGTERM, crash reconciliation, and hard-deadline tests.
- Treat Axum 0.8.9's non-terminating accept behavior as an explicit architecture
  fact; do not fake a natural network failure.
- Test HTTP early completion only through the private production supervisor's future
  boundary.
- Keep exact terminal status/code/event and restart assertions.
- Add no environment hook, endpoint, config field, Cargo feature, signal command,
  migration, dependency, or public test API.

## Task 1: Generalize the Unix real-process signal harness

1. Introduce a typed internal `Signal` enum for `TERM` and `INT`.
2. Extract the current complete SIGTERM scenario into a shared helper.
3. Keep separate SIGTERM and SIGINT test functions for clear failure reporting.
4. Send the selected signal with the existing external `kill` command.
5. Reuse deterministic model-accepted and durable-Running barriers.
6. Preserve zero exit, SSE EOF, exact raw SQLite terminal/event, public restart, and
   post-restart equality assertions.

## Task 2: Extract and correct the private lifecycle supervisor

1. Move trigger selection and bounded drain from `main` into one private async
   supervisor called by production `main`.
2. Make selection biased by ownership, runtime fatal, HTTP completion, then signal.
3. Return explicit errors from the shutdown-signal future.
4. Poll runtime drain and the still-pending HTTP future together.
5. Latch HTTP completion that occurs before the graceful command.
6. Send the graceful command only after runtime drain wins while HTTP remains
   pending.
7. Preserve sticky fatal precedence and attempt healthy owner release after HTTP
   result errors.

## Task 3: Add deterministic supervisor tests

1. Build a private blocking test Agent over in-memory SQLite and production
   RunService/EventHub.
2. Start one Attached and one Detached Run and wait until both are Running.
3. Inject an immediately completed HTTP future into the production supervisor.
4. Require nonzero HTTP-stop result, exact Cancelled/Interrupted records, and one
   matching terminal event each.
5. Use controlled futures to prove an HTTP completion during runtime drain is
   latched and no graceful command is needed to classify it.
6. Extend precedence tests for simultaneous fatal/HTTP/signal readiness.
7. Inject signal-future failure and require the fixed nonzero classification.

## Task 4: Synchronize status

1. Mark remediation item 15 `Addressed` only after both verification forms pass.
2. Record exact real-process SIGINT and private supervisor test names.
3. State explicitly that Axum 0.8.9 cannot naturally terminate from accept errors
   and that no runtime backdoor was added.
4. Mark this design and plan implemented only after complete gates pass.

## Task 5: Complete gates and independent review

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --bin insight-agent-platform -- --nocapture
cargo test --locked --test binary_lifecycle -- --nocapture --test-threads=1
cargo test --locked --all-targets --all-features -- --nocapture --test-threads=1
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo audit
cargo deny check
git diff --check
```

Then independently review real-child signal delivery, supervisor linearization,
terminal persistence, restart idempotency, error precedence, owner release, child
cleanup, and the absence of a runtime injection surface.
