# OpenAI Stream Completion Evidence Implementation Plan

**Goal:** Prevent partial OpenAI-compatible streams from becoming successful Runs
while preserving canonical `[DONE]` completion.

**Design:**
`docs/superpowers/specs/2026-07-15-openai-stream-completion-evidence-design.md`

**Status:** Implemented and verified

## Constraints

- Treat `[DONE]` as immediate logical completion.
- Reject every clean EOF without `[DONE]`, including after `finish_reason` or usage.
- Continue reading after a finish reason for usage and later failure detection.
- Never treat content, usage-only, empty, or comment-only EOF as success.
- Preserve existing size, malformed-payload, UTF-8, and transport error contracts.
- Keep all new error text body-free and credential-free.
- Do not change public types, configuration, dependencies, persistence, or migrations.

## Task 1: Freeze RED protocol tests

1. Prove `[DONE]` completes under a timeout while the provider keeps its socket open.
2. Prove clean EOF after a terminal finish fails without `[DONE]`.
3. Prove a usage-only chunk after terminal finish is yielded before incomplete
   failure.
4. Prove clean EOF after content without finish evidence returns the exact incomplete
   error.
5. Cover empty and usage-only clean EOF as incomplete.
6. Prove final malformed JSON and truncated UTF-8 keep their existing invalid errors.
7. Prove a transport error after a terminal finish remains a transport error.
8. Prove a final `[DONE]` line without LF succeeds.

## Task 2: Implement completion state

1. Record `[DONE]` explicitly in `SseDecoder` and stop decoding after it.
2. Record clean EOF so the underlying body is never re-polled after `None`.
3. Drain queued chunks before deciding sentinel success or incomplete failure.
4. Drop the HTTP body as soon as `[DONE]` is parsed while retaining all preceding
   queued chunks for ordered delivery.
5. Add one fixed `UPSTREAM_STREAM_INCOMPLETE` constructor.

## Task 3: Synchronize documentation and status

1. Add the provider completion contract to README.
2. Record the stricter provider behavior and migration requirement in
   `docs/formal-v1-breaking-changes.md`.
3. Mark remediation item 14 `Addressed` with direct test evidence.
4. Leave Direct Chat cancellation as the remaining Chat boundary.
5. Mark this design and plan implemented only after all gates pass.

## Task 4: Complete gates and independent review

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --test formal_resources -- --nocapture --test-threads=1
cargo test --locked --all-targets --all-features -- --nocapture --test-threads=1
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo audit
cargo deny check
git diff --check
```

Then independently review decoder state transitions, error precedence, connection
drop behavior, partial-output handling, and sensitive-data containment.
