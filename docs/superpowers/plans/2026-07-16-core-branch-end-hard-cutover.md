# Core Branch End Hard-Cutover Implementation Plan

> **Historical / superseded:** `core.branch_end` and explicit Fork/Join authoring were subsequently removed. Current lexical branch results and `parallel` ownership are defined by [DSL Authoring Surface Redesign](../specs/2026-07-17-dsl-authoring-surface-redesign.md).

**Status:** Implemented and verified on 2026-07-16

**Goal:** Make branch settlement explicit in authored DSL without changing Join,
BranchResult, Run terminal, persistence or event payload semantics.

## Task 1: Add the explicit terminal kind

- Refactor the existing End compilation/execution implementation so `EndNode` and
  `BranchEndNode` share configuration, rendering, validation and terminal output.
- Emit distinct `NodeControl::End` and `NodeControl::BranchEnd` discriminators.
- Register `core.branch_end` in both default registries.
- Prove config, next/emit and executor behavior parity.

## Task 2: Enforce scope at both boundaries

- Treat both controls as structural graph terminals.
- Accept only Branch End as a branch terminal; temporarily bound a wrong End for
  topology-error precedence, then reject it in the scope pass.
- Reject any Branch End left outside an assigned branch region with
  `TERMINAL_SCOPE_INVALID`.
- Make the Scheduler verify scope, control and transition agreement before acting.
- Preserve existing topology error precedence and all Join activation behavior.

## Task 3: Hard-cutover authored Agents and fixtures

- Migrate only real branch terminals in the checked-in Agent and executable test
  fixtures.
- Keep all main and post-Join workflow terminals as `core.end`.
- Add explicit negative matrices for no-Fork, main, post-Join and mixed branch
  terminal misuse.
- Keep the structured synthesis provider and terminal assertions unchanged.

## Task 4: Document and verify

- Update README, Formal V1 migration guidance and current remediation status.
- Document atomic binary/YAML deployment and the absence of database changes.
- Run focused DSL/node/Scheduler/observability/Agent tests, formatting, Clippy,
  all-target/all-feature tests, real PostgreSQL integration, audit, deny and diff
  checks.
- Commit this hard cutover independently from structured synthesis transfer.
