# Parallel Synthesis Data Transfer Implementation Plan

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

> **Historical / superseded authored syntax:** the successful-data/failure-metadata separation remains a requirement, but the `core.chat` and graph-era examples below are not accepted. Current `all_settled` synthesis and LLM messages/content contracts are defined by [DSL Authoring Surface Redesign](../specs/2026-07-17-dsl-authoring-surface-redesign.md).

**Status:** Implemented and verified on 2026-07-16

**Goal:** Deliver structured successful branch results and only safe failure
metadata to Chat synthesis without global template behavior changes, and make
zero/partial success policy explicit.

## Task 1: Establish the JSON content boundary

- Add failing `core.chat` tests for structured values, references, limits, missing
  sources and body-free errors.
- Add a typed static message part with canonical node-output path validation.
- Serialize through a bounded compact-JSON writer into an existing provider text
  part.
- Prove existing dominance and fork-region validators see the new reference.

## Task 2: Adopt the boundary in `parallel_researcher`

- Route zero successful branches to a static failure End.
- Pass only succeeded branch outputs through JSON parts. Partial paths include
  typed failure kind/code but exclude free-form failure messages.
- Converge the mutually exclusive synthesis nodes through `core.select`.
- Route the completed synthesis to literal full/degraded result Ends.
- Keep perspective prompt hookup, `prepare` cleanup and branch terminal rename out
  of this change.

## Task 3: Verify production behavior

- Compile the checked-in Agent through production registries.
- Run full, A-only, B-only and zero-success scenarios with a recording model and
  the real RunService, SQLite history, EventHub and Scheduler.
- Assert real branch sentinels and typed errors reach synthesis, `[object]` never
  appears, and zero success does not invoke synthesis.
- Update README, Formal V1 migration guidance and the current remediation status.
- Run formatting, Clippy, all tests/features, PostgreSQL integration, audit, deny
  and diff checks before commit.
