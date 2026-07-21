# PostgreSQL Exclusive Store Ownership Implementation Plan

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

**Goal:** Enforce one authoritative runtime per PostgreSQL Formal V1 history store,
fence stale writers after takeover, and fail-stop the process when ownership is lost.

**Design:**
`docs/superpowers/specs/2026-07-15-postgresql-exclusive-store-ownership-design.md`

## Constraints

- Acquire ownership before migrations, startup reconciliation, and HTTP bind.
- Identify a store by database plus current-schema OID; isolated schemas must not
  contend.
- Keep the advisory lock on one dedicated connection that is never pooled.
- Fence every PostgreSQL mutation with the persistent owner UUID and generation.
- Keep reads available for drain diagnostics, but make readiness ownership-aware.
- Never log or return the database URL, credentials, owner UUID, or advisory key.
- Do not change SQLite or any public HTTP, Run, Agent, event, or terminal shape.

## Task 1: Freeze migration and acquisition contracts

1. Add a RED migration-layout assertion for the PostgreSQL-only
   `runtime_ownership` table and singleton row.
2. Add real-PostgreSQL RED tests proving same-schema contention, isolated-schema
   independence, no generation advance on contention, and clean-release takeover.
3. Retain the existing local-skip/CI-required PostgreSQL environment policy.

## Task 2: Implement session ownership and generation claim

1. Add `src/history/postgres/ownership.rs` for schema identity, namespaced advisory
   lock acquisition, generation claim, sticky loss state, monitor, and clean release.
2. Run the baseline migration on the dedicated locked connection.
3. Create the normal query pool only after the generation claim succeeds.
4. Return stable sanitized `HISTORY_STORE_ALREADY_OWNED` and
   `HISTORY_OWNERSHIP_LOST` errors.

## Task 3: Fence every PostgreSQL mutation

1. Add one `begin_owned_write` entry point that locks and validates the singleton
   ownership row.
2. Route create, mark-running, event append, node output, terminal commit, and startup
   reconciliation through it.
3. Keep reads unfenced and make `check_health` validate the current token.
4. Prove stale writes fail after takeover while reads remain available and the new
   owner can write.
5. Prove generation takeover waits for an already-started fenced write transaction.

## Task 4: Integrate process fail-stop lifecycle

1. Return the generic repository plus optional PostgreSQL owner guard from startup.
2. Retain the guard through runtime and HTTP drain; explicitly release it afterward.
3. Select on sticky ownership loss alongside signals and unexpected HTTP termination.
4. On loss, close admission, drain within the existing hard deadline, and always exit
   nonzero.
5. Add real-process PostgreSQL coverage for contender-before-bind, connection loss,
   replacement reconciliation, stale-process fencing, and clean release.

## Task 5: Synchronize documentation and status

1. Document the one-runtime-per-store deployment contract and fail-stop behavior in
   README.
2. Document the PostgreSQL-only baseline reset and unchanged public Formal V1 API.
3. Mark remediation item 5 addressed only after the real PostgreSQL and real-process
   gates pass; preserve any genuinely unverified deployment remainder.

## Task 6: Complete gates and independent review

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --all-targets --all-features -- --nocapture --test-threads=1
RUN_HISTORY_POSTGRES_URL=... cargo test --locked --test history_postgres -- --nocapture --test-threads=1
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo audit
cargo deny check
git diff --check
```

Then perform independent reviews of fencing completeness, lock ordering, lifecycle
failure behavior, and sensitive-data containment.
