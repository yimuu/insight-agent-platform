# Raw-Row Input-Summary Privacy Implementation Plan

**Goal:** Prove that production `RunService` persists only the accepted input shape
metadata, never raw input values, in SQLite and PostgreSQL.

**Design:**
`docs/superpowers/specs/2026-07-15-raw-row-input-summary-privacy-design.md`

**Status:** Implemented and verified

## Constraints

- Send the raw input through `RunService`; do not pre-summarize it in the test.
- Use the production repository and EventHub paths.
- Require an exact two-field summary, not only substring absence.
- Inspect native raw SQLite and PostgreSQL storage through independent connections.
- Keep PostgreSQL inside one exclusive owner generation.
- Do not add migrations, test-only production hooks, repository bypasses, or a new
  public privacy contract.

## Task 1: Build one deterministic service fixture

1. Add a dedicated integration test module for the privacy boundary.
2. Construct a minimal compiled Agent with a strict public input schema.
3. Register a fixed-output executor that never copies its Run input.
4. Build a production `RunService` and `EventHub` over a supplied real repository.
5. Add bounded terminal waiting and clean shutdown helpers.

## Task 2: Verify the SQLite raw row

1. Create a temporary file-backed `SqliteRunRepository`.
2. Submit the secret-bearing input through `create_detached`.
3. Verify the returned and reconstructed summaries equal the independently computed
   sorted-key and serialized-byte object.
4. Select the native `runs.input_summary` text through a separate `SqlitePool`.
5. Parse it for exact equality and scan for raw and JSON-escaped secret sentinels.
6. Shut down the service and close all pools cleanly.

## Task 3: Verify the PostgreSQL raw row

1. Create an isolated schema and acquire one exclusive store owner.
2. Run the same service/input contract over the owned repository.
3. Select `input_summary` as native JSONB and as text through a separate read-only
   scoped pool.
4. Require exact decoded equality, repository parity, and complete secret absence.
5. Shut down, release the one owner, and clean the schema.
6. Preserve the existing local-skip and CI-required environment policy.

## Task 4: Synchronize status

1. Mark remediation item 8 `Addressed` only after both backend boundaries pass.
2. Add the design and both exact test names to post-baseline evidence.
3. Keep the accepted key-name and byte-count disclosure explicit.
4. Mark this design and plan implemented only after complete gates pass.

## Task 5: Complete gates and independent review

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --test input_summary_privacy -- --nocapture --test-threads=1
cargo test --locked --all-targets --all-features -- --nocapture --test-threads=1
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo audit
cargo deny check
git diff --check
```

Then independently review the actual `RunService` boundary, exact summary shape,
raw-row decoding, escaped-sentinel checks, PostgreSQL ownership, and cleanup.
