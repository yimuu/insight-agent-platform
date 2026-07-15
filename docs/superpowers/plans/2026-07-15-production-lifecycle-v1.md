# Production Lifecycle V1 Implementation Plan

**Goal:** Make the real platform process expose distinct liveness/readiness, drain in
a deterministic order, verify history readiness, and prove signal/restart/deadline
behavior through the production executable.

**Design:**
`docs/superpowers/specs/2026-07-15-production-lifecycle-v1-design.md`

**Status:** Implemented; residual verification tracked separately

## Constraints

- Preserve startup reconciliation before HTTP bind.
- Preserve `/health` status and JSON as the compatibility readiness route.
- Preserve Attached `cancelled` and Detached `interrupted` shutdown terminals.
- Keep probe errors sanitized and public; do not move them under `/v1` auth.
- Do not add PostgreSQL store leases, migrations, new Agent nodes, or test-only
  production actions.
- Use real binary, loopback model, and SQLite boundaries for process tests.

## Task 1: Freeze probe and configuration contracts

1. Add API RED tests for `/health/live`, `/health/ready`, `/health` equality, draining
   readiness, and 503 Run admission.
2. Add platform-config RED tests for lifecycle defaults and deadline validation.
3. Add RunService RED tests for bounded repository readiness success/failure/timeout.
4. Add RED coverage for single-flight probe amplification and `Cache-Control: no-store`.
5. Run the focused tests and retain the expected missing-route/config/method failures.

## Task 2: Implement readiness and deterministic drain

1. Add required `RunRepository::check_health` implementations for production and test
   repositories.
2. Add `RunService::check_readiness` and idempotent `begin_shutdown`.
3. Add public `/health/live` and `/health/ready`; keep `/health` on the same readiness
   handler.
4. Map `RUN_SERVICE_STOPPING` to 503 `RUN_SERVICE_UNAVAILABLE`.
5. Add optional lifecycle duration fields and validation to platform config.
6. Change main signal handling to runtime-first, HTTP-second drain under one hard
   deadline.
7. Coalesce concurrent repository probes behind a 250ms success/failure cache and mark
   all dynamic probe responses `no-store`.

## Task 3: Prove the real process boundary

1. Update the existing smoke startup wait to `/health/ready` and assert both companion
   probes.
2. Add a lifecycle binary suite with a temporary blocking Chat Agent and loopback
   OpenAI-compatible server.
3. Prove clean SIGTERM terminalizes in-flight Attached and Detached Runs and survives
   restart.
4. Prove forced process death is reconciled before the next readiness success and is
   idempotent across another restart.
5. Prove an incomplete HTTP request is cut off by the configured hard deadline and
   produces a non-zero exit without a Run row.

## Task 4: Synchronize documentation and status

1. Update README probe, shutdown, and runtime-config guidance.
2. Update Formal V1 breaking/additive contract notes.
3. Refresh remediation status for real-binary lifecycle and readiness, retaining any
   genuinely unverified database-loss or deployment remainder.

## Task 5: Complete gates and independent review

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --all-targets --all-features -- --nocapture --test-threads=1
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo audit
cargo deny check
git diff --check
```

Then perform independent architecture and test-boundary reviews of the complete diff.
