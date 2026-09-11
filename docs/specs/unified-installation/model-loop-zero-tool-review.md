# Text-only ModelLoop tool budgets

Status: jointly accepted, implemented and independently reviewed. The
[O delivery](deployment-review.md#o-current-delivery-evidence) passed its actual Native ModelLoop,
typed result, complete public events and business restart. Prior failures retain their own history.
Native E is retained unchanged. This document
does not authorize changing its Plan, restarting its Run or reporting a provider invocation.

## Actual failure

The single E Run passed Start and reached the ModelLoop node, whose orchestration Job had two
attempts before timing out. No Invocation/ModelTurn row exists for that exact tenant/Run. The
Orchestration process recorded two `plan commit failed: InvariantViolation` errors; the Run's
time window contains no PostgreSQL ERROR/FATAL. The public durable page contains Run queued and
node-started events, with no model-start/completion evidence.

The exact compiled Plan has no Skill/Capability slots but freezes tool budgets `1 / 1`.
[Basic Model declaration](../../../crates/definitions/platform-registry/src/model_configuration.rs)
honestly declares no tools, zero tool calls and zero parallel tool calls.
[Controller admission](../../../crates/adapters/platform-postgres/src/controller_admission.rs)
rejected Plan budgets above those profile limits before creating ModelTurn. Its initial guard
also rejected zero budgets. Before this repair, changing only a client field could not fix the
Plan, compiler, public profile and runtime rejection.

## Accepted single meaning

Keep rounds and token budgets strictly positive. Tool-call and parallel-call budgets form one
pair: either both zero, or both positive with parallel not exceeding total. Only a ModelLoop with
both Skill and Capability slot sets empty may use the zero pair; any selected slot requires the
positive pair. This does not invent tool support for a text-only ModelProfile.

Put the reusable pair/slot validation in the
[Plan owner](../../../crates/definitions/platform-plan/src/lib.rs). The public profile, compiler
and PostgreSQL controller already depend on it. A profile has no selected slots, so it validates
the pair alone; the lowered exact Plan and runtime request validate the pair against the actual
selected slot sets. Runtime also checks that request limits and slot identities equal the Plan.
No consumer substitutes positive limits, skips current authorization or silently rewrites a
frozen request.

The installation's authoring profile explicitly supplies zero/zero for its text-only Model chat
template. The shared compiler freezes those values into the output Plan and source evidence.
Explicit positive profiles remain bounded, but they cannot be used to demand tools from a
profile that declares zero capability. The existing strict profile comparison remains in place;
zero now compares correctly with zero. Tool-bearing Plan fixtures retain their positive limits.

## Affected contracts and consumers

- Plan ModelLoop validation uses the shared pair/slot rule instead of rejecting every zero;
  duplicate-slot, output-port and other structural checks remain.
- Public `AgentAuthoringProfileV1::build` creates `0 / 0` for this template. Its validator and
  `AgentCompilerProfile` validator use the pair rule. Shared native
  and WASM compiler output must agree byte for byte for the same new exact input.
- Generated OpenAPI and authoring-profile JSON Schema in `machine.rs`, and the ModelLoop profile
  definition in `evaluation_schema.rs`, must admit the zero pair with a conditional rejecting
  mixed zero/positive values. Preserve maxima and the independent ChildBudget definition.
- Controller admission uses the owning Plan rule after parsing the exact ModelLoop. It retains
  profile limit checks, current model/provider/policy/Secret checks and request validation.
- `StoredModelTurnWaitPayload` already stores these budgets as unsigned integers. No SQL column
  or table change is needed. Its original Plan equality, call-count accounting and continuation
  guards remain. A positive tool intent exceeds a zero budget and must never create a Capability
  Invocation. The canonical no-tools request keeps `allow_tool_intents=false`; strict response
  validation also rejects any unexpected tool intent.
- CLI and Console profile adapters copy numeric fields into the shared compiler. Verify the real
  generated profile through those adapters; do not create a frontend-only zero-budget exception.

The similarly named positive `ChildBudget.maximum_capability_calls` is a different owner and is
not changed. Model tools/usage/cost declarations, token estimation, quotas, lease fencing,
request deadlines, generation defaults and provider wire behavior are unchanged.

## Exact semantic identities

This is an accepted-input change: the old interpreter rejects a zero-tool Plan. Keep IR wire
shape version 6, but revise the explicit program semantic descriptor in
[Plan execution identity](../../../crates/definitions/platform-plan/src/execution.rs), so an old
worker cannot claim a new zero-tool Plan under an unchanged compatibility identity. Bump the
compiler semantic version in
[compiler boundary](../../../crates/authoring/platform-agent-compiler/src/boundary.rs) because its
accepted profile/defaulting behavior changes. Producer/worker manifests and generated corpus use
their owning helpers; no manual digest substitution or compatibility fallback is added.

New builds, Console WASM and exact compilation evidence must be sealed together. E's old Plan,
compiler, Agent deployment, Run and failure record remain immutable.

## Required proof

1. Preserve the real compiler-produced E empty-slot Plan and basic-profile failure. Separately
   prove the old controller's zero-budget rejection with the owning Plan in a real PostgreSQL
   fixture before any ModelTurn row is created.
2. Pair/slot matrix: empty slots with zero/zero succeeds; mixed pairs fail; either nonempty slot
   set with zero fails; positive valid pairs succeed; parallel above total fails; zero rounds or
   token budget still fails. Cover Plan, public profile, compiler boundary and generated schemas.
3. Actual PostgreSQL controller with zero-tool profile: exact admission creates one ModelTurn and
   dispatch Job, preserving frozen request/Plan identity and quota reservations. Reject a forged
   selected slot, profile-budget mismatch and nonzero tool intent; prove no Capability side effect.
4. Existing tool-capable ModelLoop, repeated rounds, usage settlement, cancellation/lease fencing
   and restart/continuation tests stay valid. No database/admin fixture substitutes for the
   installed role where that path is already qualified.
5. Rebuild shared native/WASM compiler corpus and executable semantic capabilities; prove old
   semantic identity does not satisfy the new requirement. Run affected strict Clippy and
   contract generation/conformance checks.
6. Only a fresh authorized installation/intent with sealed new artifacts can provide the final
   public ModelTurn/output proof. Unit or PostgreSQL success is not a real model qualification.

The accepted ADR and current documentation explain the observable zero-tool behavior without
copying the field registry or corpus.

## Implementation evidence

The Plan and controller use the same pair/slot validator. This repair introduced program
`ir-v6/semantics-v2` and compiler semantic version 6 with an explicit zero-tool-budget descriptor.
The later [node-response-schema repair](model-node-response-schema-review.md) advances the current
identities to semantics v3/compiler 7 and retains this zero-tool rule. The independent compiler
corpus was regenerated by its owning tool with an explicit zero
pair and was reproduced by the rebuilt actual WASM adapter. No schema/table or provider metadata
was changed for this repair. Review of all budget consumers found no zero-path division, chunk
size or semaphore construction; tool dispatch and continuation retain their original count guards.

Actual isolated PostgreSQL evidence: the old controller rejected the valid no-slot zero Plan at
`assemble` with `InvariantViolation` in 0.45 seconds (`insight-zero-tools-pg-red3-test.log`).
The repaired controller passed the same fixture in a fresh database in 0.58 seconds. It creates
one exact ModelTurn and Model Job atomically, preserves the zero continuation budget, rejects
nonempty slots and profile mismatch before mutation, rejects unexpected tool output, and replays
the original request without a second Invocation. The valid text-only response is independently
validated before the unexpected-tool negative. This helper is also called by the normal complete
Model test, which passed in a separate fresh database in 6.27 seconds. The targeted ignored entry
exists only to isolate this fixture for diagnosis; normal CI coverage remains in the main test.

Local owner tests passed: Plan 9, compiler 24, API 69; generated budget schema test 1 and actual
WASM/corpus tests 5. These are implementation/PG/portable-compiler proofs, not a vendor call,
public ModelLoop result or production qualification. E's original timeout remains unchanged.

An earlier complete test invocation failed before reaching this helper at the existing concurrent
credential revoke test with SQLSTATE 40P01. That separate failure (`insight-zero-tools-pg-red2-test.log`)
is retained and is not overwritten or declared fixed by the later complete-test pass.

Final joint review accepted this slice. After the separately reviewed test-only 40P01 handling
was added, the complete PostgreSQL Model target passed in another fresh database in 6.41 seconds.
Only the precise, explicitly rolled-back deadlock can resume the same revoke command; unrelated
errors still fail. This does not claim that production deadlocks have been eliminated. The safe
local evidence index is `insight-zero-tools-pg-fixture/evidence-index.json`; it contains no
connection URL, password or provider content. Final strict Clippy and boundary checks also passed.

The supplemental zero-tool fixture now derives its replacement Model slot digest with the
existing `FrozenSlotBinding::expected_binding_digest` owner before freezing the new Run. The
complete Model target passed again in a new database in 18.51 seconds after this test-only
precision change; final PostgreSQL target Clippy passed. The exact fixture container was stopped
with exit 0 and retained with its failed databases and private PostgreSQL log.
