# Explicit Branch Terminal Design

**Status:** Implemented and verified on 2026-07-16

## Problem

Formal V1 currently uses `core.end` for both workflow termination and fork-branch
settlement. The Scheduler interprets the same terminal transition differently from
its hidden execution scope: in main scope it ends the Run, while in branch scope it
settles only that branch. Runtime behavior is correct, but the authored DSL does not
state which meaning is intended. A branch-local `core.end` therefore looks like a
global workflow exit and makes correctness depend on implicit scope inference.

## Decision

Add a distinct built-in terminal node:

```yaml
branch_result:
  type: core.branch_end
  config:
    outcome: success
    data: {value: "{{ nodes.work.output }}"}
```

`core.branch_end` is legal only inside one declared `core.fork` branch region.
`core.end` is legal only in the main flow, including after a `core.join`. Neither
node accepts `next` or `emit: content`.

Both nodes deliberately share the existing terminal payload contract:

- success requires `content` or `data`, with the existing format rules;
- failure requires the existing static `WORKFLOW_*` code and safe message;
- the node output and `TerminalOutcome` representation are unchanged;
- a successful Branch End becomes `BranchResult::Succeeded`;
- a failure Branch End becomes a workflow-kind `BranchResult::Failed`;
- only a main End produces the Run terminal outcome.

The Join contract remains implicit: branch authors do not point at the Join. When
every branch has settled, the Scheduler activates the Fork's declared Join exactly
once.

## Compile-time scope contract

The execution-plan compiler owns terminal-scope validation because it is the layer
that has computed branch regions.

- Only `NodeControl::BranchEnd` is accepted as a branch terminal. Discovery may
  temporarily bound a path at `NodeControl::End` so higher-precedence topology
  diagnostics finish first; the later scope pass always rejects that End with
  `TERMINAL_SCOPE_INVALID`.
- After all branch regions are assigned, any `NodeControl::BranchEnd` without a
  branch owner also fails with `TERMINAL_SCOPE_INVALID`. This covers a no-Fork
  Agent, the main path before a Fork, and the continuation after a Join.
- Mixed conditional exits are rejected if any path uses the wrong terminal kind.
- Existing graph errors keep precedence where the graph is invalid for another
  reason, including missing edges, cycles, direct Join edges, nested Forks and
  cross-region edges.

`core.branch_end` has its own `NodeControl::BranchEnd { outcome }` discriminator.
It is a first-class built-in, not an alias for `core.end`, so the Agent version hash
naturally changes when YAML is migrated.

## Runtime defense in depth

The Scheduler validates the compiled control discriminator against both the work
scope and the executor transition before applying it:

- main scope accepts an End transition only from `NodeControl::End`;
- branch scope accepts an End transition only from `NodeControl::BranchEnd`;
- the control's declared success/failure kind must match the concrete
  `TerminalOutcome` variant;
- a terminal control returning a non-terminal transition, or a non-terminal
  control returning an End transition, trips an internal
  `SCHEDULER_INVARIANT_VIOLATION`. The Scheduler's public boundary keeps its
  existing sanitized `INFRASTRUCTURE_FAILURE` result.

Static scope/control agreement is checked before `node.started` and before the
executor is invoked. Control/transition/outcome agreement is checked immediately
after the executor returns but before node output or `node.completed` is persisted.
Thus a wrong scope cannot execute, and a lying executor cannot leave a durable
successful-node record before the Run fails closed.

This check is not a substitute for compilation. It protects custom registries and
manually constructed `CompiledAgent` values from accidentally reintroducing the
implicit dual meaning.

## Migration

This is a hard cutover for authored DSL:

- migrate every real Fork branch terminal to `core.branch_end`;
- keep every main-flow terminal as `core.end`;
- in `parallel_researcher`, migrate only `end_a` and `end_b`; keep
  `result_full`, `result_degraded` and `fail_all` as `core.end`.

There is no compatibility alias, database migration or history reset. Binary and
Agent YAML must be deployed atomically because old binaries do not know the new
kind and new binaries reject branch-local `core.end`.

Historical design documents remain historical. README, Formal V1 migration
guidance and the current remediation status describe the new contract.

## Acceptance

- Both terminal kinds have identical success/failure payload and rendering tests.
- The default type and executor registries contain exactly nine built-ins.
- Main End, branch Branch End and post-Join main End compile.
- Branch End outside a branch and End inside a branch fail with the stable scope
  error; conditional mixed exits fail as well.
- Existing Fork/Join topology and reference diagnostics do not regress.
- Branch success/failure settles locally, the Join runs once, and the main End
  remains the only Run terminal.
- Control/scope/transition mismatches fail closed in Scheduler tests.
- Checked-in Agents compile through production registries and the
  `parallel_researcher` full/partial/zero-success runtime matrix remains unchanged.
- Node and branch events/logs use `core.branch_end` for branch terminals without
  exposing terminal bodies.
