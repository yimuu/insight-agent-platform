# Stable Baseline Review

> Historical snapshot. DSL and scheduler references describe the retired graph runtime; the current authored contract is [DSL Authoring Surface Redesign](../superpowers/specs/2026-07-17-dsl-authoring-surface-redesign.md), lowered to the retained Region/SSA runtime described by the README and vNext design.
>
> Current remediation status: see `docs/reviews/2026-07-12-remediation-status.md`.
> This document remains a dated audit snapshot and should not be read as the current open-work list.

## Audited baseline

- Product baseline: `af414de7f43bc4c5ce580ed77db17053faab7a9f` (`main@af414de`).
- Review checkout before synthesis: `ac37b8f53fbc689205dc0f497f02aee713d2adfd` on `review/stable-baseline`.
- Review date and zone: 2026-07-11, Asia/Shanghai.
- Change control: before these reports, `git diff --name-only af414de..HEAD` contained only the approved review design and plan under `docs/superpowers/`. The audited production source, tests, Agents, configuration, migrations, manifests, lockfile, dependency policy, and toolchain are identical to `af414de`.
- Evidence basis: the contract map and six domain/verification records under `.superpowers/review/`, plus their Task 1–7 execution reports. Those ignored artifacts are evidence, not tracked deliverables.

## Executive assessment

The baseline is a credible foundation, but it is not yet a fully stable production baseline. Its strongest properties are strict startup compilation, immutable graph/parallel metadata, typed single-winner Run state and cancellation, persistence-before-broadcast, transactional terminal recovery, a deliberately live-only HTTP/SSE contract, strict configuration, and broad focused tests. The full local gate passed formatting, Clippy with warnings denied, 177 Rust tests, dependency policy checks, and a separately forced real-PostgreSQL contract test.

Fourteen confirmed architecture/correctness findings remain: **P0: 0, P1: 10, P2: 2, P3: 2**. The most consequential groups are fail-before-serving gaps in Action/CEL validation, incomplete preparing/active lifecycle ownership, cancellation-reason authority, recovery timeout and EventHub teardown, unsafe Action validation messages, unbounded model-response memory, and plaintext model transport. Twenty narrower questions remain under Needs verification and are not included in the confirmed count.

Four recommendations are intentionally breaking: `BASE-P1-001`, `BASE-P1-003`, `BASE-P3-004`, and `BASE-P1-012`. This review recommends decisions; it does **not** approve or implement them. User approval count is **0**, and each breaking item requires a later design and implementation plan before work begins.

Dependency findings and upgrades are intentionally excluded from the count and roadmap here; they are independently actionable in `docs/reviews/2026-07-11-dependency-governance-review.md`.

## Evidence-backed strengths

- The public router matches the current live-only contract: `/health` is public; every `/v1` route shares bearer protection; Attached creation atomically returns the only SSE stream; Detached creation, lookup, and idempotent cancellation are direct; replay routes and recovery headers are absent and tested.
- YAML envelopes are strict and versioned. Agent compilation freezes templates, CEL programs, resources, graph edges, dominators, fork/join regions, timeouts, node controls, and typed bodies before serving.
- Graph validation directly covers missing edges, cycles, unreachable nodes, invalid terminals, future/self/missing references, fork/join pairing, branch overlap and cross-region edges, nested forks, join bypass, and post-join branch access.
- Runtime state, stop reasons, branch settlement, activation, and durable terminals each have explicit single-owner or first-winner boundaries. Parallel branches receive isolated context snapshots and join through a deterministic all-settled envelope.
- Run, per-Run node, and global node capacity are independent; execution permits remain held through output and completion-event persistence. Async cancellation/timeouts stop admission and drain active siblings.
- EventHub holds a per-Run sequence lock through durable acknowledgement. Normal and terminal events broadcast only after commit; uncertain commits recover from durable state under backend-appropriate locks.
- SQLite and PostgreSQL migrations and repository algorithms are structurally parallel for fields, constraints, transactions, terminal compare-and-set, recovery, startup reconciliation, cascade deletion, and internal event pagination.
- Platform/model configuration is strict, secret values come from environment variables, `Debug` implementations are redacted, prompt paths are canonically contained, network redirects are disabled, and restricted HTTP Actions impose HTTPS/host/timeout/body controls.
- The dynamic gate was clean: format and all-target/all-feature Clippy passed; `cargo test --all-targets` reported 177 passed, 0 failed, 0 ignored; the explicit PostgreSQL gate reported 1 passed, 0 failed, 0 ignored against PostgreSQL 16.14.

## Confirmed findings

### P0 — Immediate

None.

### P1 — Near-term

#### BASE-P1-001 — Statically invalid literal Action input survives Agent compilation

- **Severity:** P1 — Near-term.
- **Evidence:** `ActionNode::compile` resolves the Action and compiles templates but does not call the registered input validator (`src/nodes/action.rs:45-72`). Validation first occurs in `RegisteredAction::call` (`src/resources/actions.rs:60-79`). `tests/core_chat_action.rs:353-388` compiles a fully literal schema-invalid input and observes `ACTION_INPUT_INVALID` only at runtime, contrary to the approved fail-before-serving contract.
- **Affected contract/subsystem:** Formal V1 Agent compilation, `core.action`, and the Action schema boundary.
- **Trigger:** An enabled Agent contains template-free or otherwise statically decidable Action input that violates the registered input schema.
- **Impact:** Startup accepts a deterministically invalid workflow; every Run reaching that node fails under user traffic.
- **Why current safeguards are insufficient:** Runtime validation protects the Action but cannot satisfy the promised deployment-time validity for already knowable values.
- **Focused recommendation:** Validate fully static inputs, and statically decidable portions where the schema permits, during Agent compilation; retain mandatory runtime validation after rendering.
- **Required verification:** Literal valid/invalid, partially templated, custom Action, stable-code, and checked-in-Agent compilation tests; reuse the non-instance error formatter required by `BASE-P1-010`.
- **Dependencies:** Coordinate error sanitization with `BASE-P1-010`; otherwise independent.
- **breaking:** yes — currently accepted, statically invalid Agents would fail startup.

#### BASE-P1-002 — Raw dependency scanning is parser-oblivious for Handlebars and CEL

- **Severity:** P1 — Near-term.
- **Evidence:** `node_references` applies one regex to raw source (`src/dsl/compiler.rs:157-164`). Handlebars compilation scans original template text (`src/dsl/compiler.rs:105-130`), and `ConditionNode` scans original CEL after compiling a program (`src/nodes/condition.rs:81-109`). Literal prose, comments, raw blocks, or CEL strings containing `nodes.<id>.output` can therefore become false graph dependencies.
- **Affected contract/subsystem:** Reference discovery for template-bearing nodes and `core.condition`, plus dominator/parallel-region validation.
- **Trigger:** Inert Handlebars or CEL syntax contains the lexical node-output pattern.
- **Impact:** Valid Agents can fail startup with misleading predecessor or branch errors; dependency metadata does not represent the parsed programs.
- **Why current safeguards are insufficient:** The language parsers validate syntax, but graph validation receives only raw regex captures.
- **Focused recommendation:** Traverse parser-specific Handlebars and CEL structures, ignoring inert syntax and feeding semantic references into the existing graph validator.
- **Required verification:** Literal/comment/raw-block/string-literal matrices, real evaluated selections, recursive fields, and the complete sequential/parallel reference suite.
- **Dependencies:** Implement with the canonical CEL access policy in `BASE-P1-003`; coordinate with dependency roadmap CEL replacement.
- **breaking:** no — removing false positives admits source already valid under the documented grammars.

#### BASE-P1-003 — Indexed CEL access bypasses compile-time predecessor and branch validation

- **Severity:** P1 — Near-term.
- **Evidence:** The pinned CEL interpreter accepts indexed map access, but `ConditionNode` discovers only dotted text through the shared regex (`src/nodes/condition.rs:81-109`, `src/dsl/compiler.rs:157-164`, `Cargo.lock:307-329`). Expressions such as `nodes["future"].output` leave `CompiledNode.references` empty and bypass `validate_references`.
- **Affected contract/subsystem:** CEL compilation, predecessor/dominator checks, branch isolation validation, and fail-before-serving behavior.
- **Trigger:** A condition accesses the `nodes` map through indexed/computed rather than documented dotted syntax.
- **Impact:** Future, missing, sibling, or post-join references compile and fail only when a Run evaluates the expression.
- **Why current safeguards are insufficient:** Runtime context isolation prevents sibling disclosure but converts a compile-time contract error into a request-time failure.
- **Focused recommendation:** Traverse the CEL AST, extract canonical dotted references, and reject every other access rooted at `nodes` with a stable compilation error.
- **Required verification:** Indexed future/self/missing/same-branch/sibling/post-join matrices, canonical dotted positives, stable errors, and checked-in Agents.
- **Dependencies:** Shares parser work with `BASE-P1-002`; coordinate with the CEL dependency replacement milestone.
- **breaking:** yes — currently accepted indexed/computed node access would be rejected.

#### BASE-P1-006 — Shutdown can miss an admitted Run before it becomes active

- **Severity:** P1 — Near-term.
- **Evidence:** `prepare_run` checks admission and then awaits repository/EventHub work (`src/runtime/service.rs:330-369`); insertion into `active` occurs only later in `launch` (`src/runtime/service.rs:382-450`). `shutdown` closes admission but snapshots and waits only on `active` (`src/runtime/service.rs:279-313`). Existing tests start shutdown only after Runs are Running (`tests/run_service.rs:729-747`).
- **Affected contract/subsystem:** Attached/Detached creation, Run capacity, graceful shutdown, and durable terminal ownership.
- **Trigger:** Shutdown begins after a request passed admission but while it is suspended before active-map insertion.
- **Impact:** Runtime drain can report completion before the handler launches work; process deadline/teardown can leave Created/Running state for next-start reconciliation and lose attachment-specific shutdown semantics.
- **Why current safeguards are insufficient:** HTTP drain, bounded capacity, the 35-second hard deadline, and startup reconciliation reduce exposure but do not make preparing ownership visible.
- **Focused recommendation:** Register an admission/preparing guard before the first durable await; shutdown must close admission and drain both preparing and active ownership, terminalizing post-insert Runs rather than launching across the shutdown epoch.
- **Required verification:** Deterministic barriers at `create_run`, `open_run`, Attached subscribe, and Detached `get_run`; assert attachment-specific terminal, no post-drain launch, and complete permit/EventHub/active cleanup; add a real HTTP/process case.
- **Dependencies:** Establish this ownership primitive before the recovery-supervisor work in `BASE-P1-008`; coordinate with `BASE-P1-009` cleanup.
- **breaking:** no — internal lifecycle ordering changes without public shape changes.

#### BASE-P1-007 — Unbacked or mismatched typed Stop can stall siblings or override the winner

- **Severity:** P1 — Near-term.
- **Evidence:** Public `NodeExecutor` code can return `RunError::stopped`; execution classifies it as Stop without comparing `StopSignal` (`src/runtime/execution.rs:159-176,230-241`). Scheduler drains without setting the shared signal (`src/runtime/scheduler.rs:89-99,340-357`). `StopController::request` already defines an atomic first winner (`src/runtime/control.rs:55-67`).
- **Affected contract/subsystem:** Extension error contract, global cancellation, sibling drain, terminal reason, and capacity release.
- **Trigger:** A custom executor returns a stopped error without a shared reason, or with a reason different from the shared winner.
- **Impact:** Siblings can run until timeout while holding permits, or durable terminal reason can disagree with the reason siblings observed.
- **Why current safeguards are insufficient:** Typed classification prevents code-string impersonation but does not prove signal origin or reason equality.
- **Focused recommendation:** Treat an existing shared reason as authoritative and normalize the return; classify a stopped return without a signal as infrastructure, cancel wrappers, and recover one generic Failed terminal.
- **Required verification:** All returned/shared-reason combinations with a blocking sibling; assert prompt drain, shared-winner status/code, no join, one terminal, and permit release.
- **Dependencies:** Align with the public extension contract and the integration test in `BASE-P2-005`.
- **breaking:** no — the public signature and controller-driven contract remain.

#### BASE-P1-008 — Direct terminal recovery bypasses the configured operation timeout

- **Severity:** P1 — Near-term.
- **Evidence:** EventJournal bounds repository work and worker close, but `EventHub::recover_terminal` then awaits `repository.recover_run` directly (`src/events/hub.rs:241-250`). SQLite and PostgreSQL recovery may wait for pool/locks/query/commit and retry once (`src/history/sqlite.rs:201-215,325-393`; `src/history/postgres.rs:181-196,302-360`).
- **Affected contract/subsystem:** Recovery latency, durable terminal convergence, health, ActiveRun/permit ownership, and shutdown.
- **Trigger:** Journal failure enters direct recovery and the database operation remains pending beyond the configured operation timeout.
- **Impact:** An unhealthy process can retain Run ownership indefinitely while durable state is unknown; timeout/cancellation cannot be treated as rollback.
- **Why current safeguards are insufficient:** The bounded call is the precursor to the unbounded one; health stops new admission but does not release existing ownership, and retries require a returned error.
- **Focused recommendation:** Bound foreground waiting without claiming rollback, isolate live state, release active capacity, and transfer the Run to one deduplicated process-lifetime recovery owner that always begins with an authoritative locked read, or explicitly hand off to controlled restart/reconciliation.
- **Required verification:** Never-ready, pre-commit failure, commit-then-block, and competing-terminal matrices on held SQLite/PostgreSQL locks; assert foreground budget, one terminal, no duplicate, cleanup, observability, and shutdown handoff.
- **Dependencies:** Requires the lifecycle ownership/drain contract from `BASE-P1-006` and shared state-isolation behavior with `BASE-P1-009`.
- **breaking:** no — reuse the existing timeout or only add a defaulted optional budget.

#### BASE-P1-009 — Post-commit reconciliation failure retains EventHub state

- **Severity:** P1 — Near-term.
- **Evidence:** After `recover_run` returns an authoritative terminal, `reconcile_durable_through` may fail on history timeout/error/gap/mismatch; state removal occurs only after it succeeds (`src/events/hub.rs:241-253,296-353`). Service still removes ActiveRun and releases its permit (`src/runtime/service.rs:423-438`).
- **Affected contract/subsystem:** Durable-to-live reconciliation, Attached stream closure, EventHub state lifetime, and recovery health.
- **Trigger:** Any history read or validation failure after durable terminal convergence.
- **Impact:** The durable Run is terminal and runtime ownership is gone, but subscribers receive neither terminal nor channel closure and one sender/state remains retained per incident.
- **Why current safeguards are insufficient:** Journal health and admission fail closed for new work, but neither owns EventHub teardown; the validation await lacks an error cleanup guard.
- **Focused recommendation:** Once authoritative recovery returns, make exact-state isolation/removal unconditional. Broadcast only a fully validated suffix; otherwise close subscribers, preserve the durable terminal, record the typed error, and never fabricate events.
- **Required verification:** Timeout/error/empty/short/gap/intermediate-terminal/final-mismatch/overflow matrices; assert one durable terminal, no unvalidated broadcast, retained count zero, subscriber closure, service cleanup, and preserved successful reconciliation.
- **Dependencies:** Share cleanup and background-ownership primitives with `BASE-P1-008`; coordinate shutdown drain with `BASE-P1-006`.
- **breaking:** no — an already stranded stream closes promptly without changing its public envelope.

#### BASE-P1-010 — Action schema errors disclose raw instances in durable/public messages

- **Severity:** P1 — Near-term.
- **Evidence:** Action input/output validation concatenates `jsonschema` error display (`src/resources/actions.rs:60-95`); the locked formatter includes invalid values. The unchanged message flows through Action execution, branch events, coordinator terminal records/events, and GET Run (`src/nodes/action.rs:76-108`, `src/runtime/scheduler.rs:181-209`, `src/runtime/coordinator.rs:209-255`, `src/api/formal/routes.rs:184-194`).
- **Affected contract/subsystem:** Action validation, error sanitization, SSE/events, Run history, and secret/input omission.
- **Trigger:** Secret-bearing Action input or output violates a schema keyword whose display contains the instance.
- **Impact:** Raw data can be persisted in `runs.error_message` and `run_events`, returned by GET, and streamed to clients.
- **Why current safeguards are insufficient:** Initial input summary, redacted config/network errors, and fixed API errors do not sanitize successful Run-resource/event fields.
- **Focused recommendation:** Map validation failures to stable safe messages and structural metadata such as keyword and redacted path; never format instances in default errors/logs.
- **Required verification:** Secret-bearing invalid input/output across linear/parallel Attached/Detached Runs; inspect SSE, GET, raw Run/event rows, representative schema keywords, and absence of the fixture secret.
- **Dependencies:** The same formatter must be used by compile-time Action validation in `BASE-P1-001`; assess already stored messages operationally.
- **breaking:** no — codes and public shapes remain; unsafe dynamic detail is removed.

#### BASE-P1-011 — OpenAI streams and accumulated chat output have no byte bound

- **Severity:** P1 — Near-term.
- **Evidence:** Arbitrary response frames append to the SSE decoder until LF/EOF (`src/resources/openai_chat.rs:156-181,204-237`), and `ChatNode` appends every decoded text chunk to one `String` (`src/nodes/chat.rs:269-302`). No content-length, line, event, total-body, or accumulated-text limit exists.
- **Affected contract/subsystem:** Provider response parsing, chat output accumulation, memory/capacity isolation, and denial-of-service boundaries.
- **Trigger:** A provider/proxy emits no-LF frames, an oversized SSE line/event, large usage JSON, or many valid chunks before timeouts expire.
- **Impact:** One Run can exhaust process memory and affect unrelated Runs despite concurrency limits.
- **Why current safeguards are insufficient:** Timeouts bound elapsed time, queue capacities bound event count, and optional provider `max_tokens` neither bounds protocol bytes nor guarantees enforcement.
- **Focused recommendation:** Add defaulted per-model limits for total upstream bytes, buffered line/event bytes, decoded chunk/usage size, and accumulated text; enforce before append/allocation and fail with one sanitized stable error.
- **Required verification:** Exact-boundary and over-limit no-LF/one-line/many-event/usage matrices; assert bounded termination, socket close, one failed terminal, no body echo, permit release, and subsequent service health.
- **Dependencies:** Define byte semantics jointly with observability `BASE-P2-013`.
- **breaking:** no — optional/defaulted limits preserve public shapes and fail excessive responses safely.

#### BASE-P1-012 — OpenAI configuration permits remote plaintext HTTP

- **Severity:** P1 — Near-term.
- **Evidence:** `OpenAiChatModel::new` accepts `http` and `https` for any host (`src/resources/openai_chat.rs:25-47`); request creation sends full messages and optional bearer credentials regardless of scheme (`src/resources/openai_chat.rs:107-147`). Tests intentionally exercise HTTP loopback with a key (`tests/formal_resources.rs:46-109`).
- **Affected contract/subsystem:** Model configuration, prompt/result/API-key confidentiality and integrity, and TLS policy.
- **Trigger:** An operator configures a remote `http://` model endpoint.
- **Impact:** Network observers or an intermediary can read/alter prompts, outputs, and bearer credentials. The checked-in default is HTTPS, but strict configuration does not fail the unsafe deployment.
- **Why current safeguards are insufficient:** Rustls applies only after HTTPS is selected; redaction protects formatting, and redirect blocking does not secure the initial hop.
- **Focused recommendation:** Require HTTPS by default. Permit HTTP only through a new explicit development-only option restricted to a precisely defined loopback endpoint; never allow that option to authorize remote HTTP.
- **Required verification:** Scheme/host/opt-in/key matrix, remote rejection even with opt-in, rustls certificate/hostname failures, redirect behavior, redacted diagnostics, and capture proving authorization is never sent over unapproved HTTP.
- **Dependencies:** Coordinate loopback/DNS policy with restricted-HTTP Needs verification; dependency TLS/provider changes remain in the dependency report.
- **breaking:** yes — all current HTTP model configurations would fail by default unless an eligible loopback development configuration explicitly opts in.

### P2 — Planned

#### BASE-P2-005 — Synthetic extension test bypasses AgentCompiler and runtime integration

- **Severity:** P2 — Planned.
- **Evidence:** `tests/node_extensions.rs:92-135` calls `NodeType::compile` and `NodeExecutor::execute` directly, manually constructing `CompiledNode`; it does not traverse `AgentCompiler`, plan/reference validation, scheduler/coordinator, persistence, or terminal publication required by the approved extension contract.
- **Affected contract/subsystem:** Test architecture for the statically linked custom node boundary.
- **Trigger:** Generic integration regresses while isolated trait calls still pass.
- **Impact:** Extension authors can discover hidden compiler/runtime coupling only after embedding or deployment.
- **Why current safeguards are insufficient:** Built-in end-to-end tests cover only core registrations; the synthetic test proves trait mechanics, not a non-core production path.
- **Focused recommendation:** Compile YAML containing a synthetic node through `AgentCompiler`, register type/executor without core edits, and run through the production scheduler/coordinator/EventHub/repository harness.
- **Required verification:** Envelope, graph/reference, dispatch/downcast, lifecycle/content, durable output/event, terminal output, and mismatch/parity cases.
- **Dependencies:** Can host the Stop regression matrix from `BASE-P1-007`; registry parity remains Needs verification.
- **breaking:** no — test coverage only.

#### BASE-P2-013 — Approved INFO observability lacks timings and output sizes

- **Severity:** P2 — Planned.
- **Evidence:** The approved design requires body-free identifiers, timings, counts, and output sizes, but the thirteen production `tracing::` sites provide selected identifiers/counts and no systematic Run/node elapsed time or output-size record (`src/main.rs`, `src/runtime/coordinator.rs`, `src/runtime/service.rs`, `src/api/formal/sse.rs`, `src/resources/openai_chat.rs`).
- **Affected contract/subsystem:** Structured INFO execution observability.
- **Trigger:** Operators diagnose Run/node latency or output growth from the promised INFO surface.
- **Impact:** Routine performance and growth questions cannot be answered consistently without deeper reproduction.
- **Why current safeguards are insufficient:** Health, error codes, startup counts, and selected transport logs do not provide the missing fields or consistent lifecycle records.
- **Focused recommendation:** Add defined body-free Run/node lifecycle records with applicable identifiers, elapsed timing, safe counts, and serialized/output byte sizes; exclude bodies, credentials, and full URLs.
- **Required verification:** Recording-subscriber tests across linear/parallel/model/Action success/failure; assert consistent values, once-only emission, and fixture body/secret absence.
- **Dependencies:** Use the byte-accounting contract from `BASE-P1-011`; broader metrics require a separate design.
- **breaking:** no — additive structured fields.

### P3 — Opportunistic

#### BASE-P3-004 — DurationSpec accepts a broader grammar than Formal V1

- **Severity:** P3 — Opportunistic.
- **Evidence:** The approved DSL permits one positive integer plus `ms`, `s`, or `m`, but `DurationSpec` delegates to `humantime::parse_duration` and checks only nonzero (`src/dsl/raw.rs:60-70`, `Cargo.lock:1008-1012`). `tests/dsl_raw.rs:61-79` lacks fractional, alias, compound, and larger-unit boundaries.
- **Affected contract/subsystem:** Public YAML node timeout grammar and Agent portability.
- **Trigger:** An Agent uses a fraction, compound value, alias, or unit outside `ms|s|m` that humantime accepts.
- **Impact:** Accepted DSL exceeds the stable contract and becomes implementation-dependent.
- **Why current safeguards are insufficient:** The nonzero check constrains neither numeric form nor unit set.
- **Focused recommendation:** Parse the documented grammar directly and keep `DSL_YAML_INVALID` as the stable outer error.
- **Required verification:** Accepted positive integer units; rejected zero/fraction/alias/compound/hour/day cases; serialization and checked-in-Agent gates.
- **Dependencies:** None.
- **breaking:** yes — accepted out-of-contract spellings would be rejected.

#### BASE-P3-014 — README opening still advertises reconnectable SSE

- **Severity:** P3 — Opportunistic.
- **Evidence:** `README.md:3` promises reconnectable SSE, while `README.md:186-219`, `docs/formal-v1-breaking-changes.md:70-90`, router code, and `tests/api.rs:405-441` define live-only streaming with no replay endpoint or recovery cursor.
- **Affected contract/subsystem:** Top-level product documentation and client recovery expectations.
- **Trigger:** A reader relies on the opening description before the detailed contract.
- **Impact:** Clients can design unsupported reconnect/replay behavior; later documentation and 404 behavior limit impact.
- **Why current safeguards are insufficient:** Correct later sections do not remove the document's internal contradiction.
- **Focused recommendation:** Describe live-only Attached SSE in the opening and direct durable recovery users to Detached polling.
- **Required verification:** Search all reconnect/replay/cursor language and verify every remaining occurrence is historical, explicitly unsupported, or internal.
- **Dependencies:** None.
- **breaking:** no — documentation alignment only.

## Needs verification

The following **20** items remain outside the confirmed count. They preserve the domain evidence's uncertainty and state the missing check.

1. **Type/executor registry parity.** Compile an enabled Agent with a registered node type but an absent or differently keyed executor; decide whether construction must fail before health/traffic or runtime infrastructure failure is the intended contract.
2. **Synchronous execution preemption.** Stress large Handlebars/CEL work and a synthetic CPU-bound executor under millisecond timeout/cancellation; measure Tokio worker responsiveness and decide whether documented complexity limits suffice or work needs a bounded/blocking boundary.
3. **Direct Chat cancellation.** Use a blocking fake model during stream acquisition and between chunks; assert the shared stop code, no later content/output, and one node failure event.
4. **Dropped create-future ownership.** Abort Attached and Detached creation after durable insertion at every pre-launch await; determine whether a permanent Created orphan remains and cannot be cancelled, then design Drop/supervisor behavior if confirmed.
5. **PostgreSQL exclusive-store topology.** Decide whether Formal V1 deployment must document or enforce one runtime process per PostgreSQL history store; if enforcement is chosen, prove a second process fails closed rather than interrupting the first.
6. **Outer Run-task panic cleanup.** Inject a panic after active insertion but outside the scheduler JoinSet; observe active-map removal, permit release, waiters, health, and shutdown before deciding on an unwind guard/supervisor.
7. **Repository parity and full-record fidelity.** Mirror SQLite partial-batch and invalid-terminal tests in PostgreSQL; compare every RunRecord field on both backends and query raw node-output JSON/timestamps.
8. **End-to-end input-summary privacy.** Send secret-bearing raw input through RunService into each real backend and inspect the raw row; assert only sorted keys/byte count and no values survive.
9. **Defensive concurrent terminal CAS.** Race independent connections on both backends; require one winner, one loser, one terminal event/status, and no lock residue without implying distributed-worker support.
10. **EventHub terminal boundary.** Enforce/test ordinary nonterminal publishing, authoritative terminal-conflict fallback, and an intermediate terminal in a reconciliation suffix; require fail-closed cleanup.
11. **Internal pagination boundaries.** Run zero/one/small limit, cursor-at-last, cursor-past-last, and oversized-cursor matrices on both backends and compare complete envelopes/pages.
12. **Stored event identity policy.** Decide whether mismatched request/agent/version fields supplied to repository append are rejected or normalized from the Run row, then test both backends.
13. **Restricted HTTP DNS/private-address policy.** Define policy for private, loopback, link-local, multicast, mixed DNS, literals, IDNA/trailing-dot, proxy, retry, and rebinding; test every connected peer or explicitly trust deployment DNS.
14. **OpenAI clean-EOF semantics.** Decide whether `[DONE]` and/or a terminal finish reason is required; test truncated JSON/UTF-8, EOF before/after finish, and transport error after content so partial success is never ambiguous.
15. **Real-binary lifecycle.** Spawn the binary with temporary SQLite/config; verify reconcile-before-bind, server failure, in-flight Attached/Detached POSTs, SIGINT/SIGTERM, admission close, attachment-specific terminals, runtime/HTTP drain, and hard deadline.
16. **Restricted HTTP positive boundary matrix.** Use local TLS to verify allowed success, redirect refusal, exact/over-size bodies, chunking, timeout, cancellation/socket close, non-2xx handling, redaction, and permit release.
17. **API/auth/error/SSE boundary matrix.** Cover every method/route/auth form, all ServiceError mappings, body/content-type/header/request-ID limits, SSE lag closure, and absence of secret/raw input in every error.
18. **Readiness and deployment paths.** Decide liveness versus readiness and trusted path policy; test idle/active database loss and absolute/parent/symlink platform/Agent path forms before classifying defects.
19. **Documentation precedence.** Mark older approved replay/reconnect-grace designs as superseded by the live-only design, or explicitly document the precedence rule; audit all current contract references.
20. **Checked-in example execution.** Execute deterministic `code_node_demo` from its checked-in YAML end to end; decide whether model-backed examples belong in CI or an environment-backed smoke gate.

## Contract and test coverage map

| Design domain | Contract/implementation coverage | Direct evidence and tests | Remaining boundary |
|---|---|---|---|
| DSL/compiler | Strict RawAgent/version parsing; compile-time schemas/templates/CEL/resources; graph/dominator/fork/join plan freezing | `src/dsl/*`; `tests/dsl_raw.rs`, `dsl_compiler.rs`, `dsl_parallel.rs`, `repository_agents_v1.rs` | Action static validation, semantic reference extraction, timeout grammar; NV 1–2, 19–20 |
| Nodes/resources/extensions | Seven built-ins, typed node bodies/controls, registries, model/Action schemas, prompt containment | `src/nodes/*`, `src/resources/*`; core node, registry, and extension tests | Findings 001–003, 005, 007, 010–012; NV 1–3, 13–14, 16 |
| Runtime/scheduling | Attached/Detached ownership, sequential/parallel activation, capacity, timeouts, cancellation, panic recovery, health/shutdown | `src/runtime/*`; `run_scheduler`, `run_coordinator`, `run_service` (42 focused passes) | Findings 006–009; NV 2, 4–6, 15 |
| Events/persistence | Sequence lock, persistence-before-broadcast, bounded journal, terminal CAS/recovery, both migrations/backends | `src/events/*`, `src/history/*`, migrations; 30 common/SQLite plus 1 real PostgreSQL pass | Findings 008–010; NV 7–12 |
| HTTP/config/process | Authenticated `/v1`, public health, live-only SSE, Detached polling, strict config, startup/reconcile/bind, drain/deadline | `src/api/formal/*`, `src/config.rs`, `src/main.rs`; API/config/resource suites (36 focused passes) | Findings 006, 009–012, 014; NV 13–18 |
| Security/operations/testing | Secret redaction, fixed API errors, network controls, health, focused/full gates, example compilation | Task 5 evidence; 177 all-target tests; format/Clippy clean | Findings 005, 010–013; NV 3, 8, 13–18, 20 |
| Dependencies | Locked graph, RustSec/deny policy, licenses/sources, upgrades and duplicate roots | Separate dependency governance report; `cargo audit` and `cargo deny check` passed with recorded warnings | Three dependency findings, seven dependency Needs items, and eight dependency roadmap groups are intentionally separate |

## Breaking change decision records

No record below is approved. Approval count is **0**; adoption requires a later user decision and dedicated design/plan.

### BASE-P1-001 — Compile-time Action input validation

1. **Current interface and problem:** `core.action.config.input` accepts arbitrary literal/template JSON at compilation; even fully literal schema-invalid data is deferred to runtime.
2. **Why compatible/additive alternatives are inadequate:** Warnings, opt-in strictness, or runtime-only validation retain traffic-time failure and split startup validity.
3. **Replacement contract:** Every statically decidable Action input must satisfy the registered schema during Agent compilation; dynamic values remain runtime-validated.
4. **Long-term benefit:** Resource schemas become deterministic deployment contracts and compiled Agent validity becomes meaningful.
5. **Migration impact:** Invalid Agent YAML must be corrected before startup; HTTP clients, Run/history data, model config, and database schemas are unchanged.
6. **Rejected alternatives:** Runtime-only validation, warnings, or built-in-only validation.
7. **Required tests/docs:** Literal/templated/custom Action matrices, stable sanitized errors, checked-in-Agent gate, and DSL/Action documentation.

### BASE-P1-003 — Canonical CEL node-reference syntax

1. **Current interface and problem:** CEL accepts indexed/computed access rooted at `nodes`, while dependency discovery recognizes only dotted text and omits the actual graph dependency.
2. **Why compatible/additive alternatives are inadequate:** Expanding a regex misses computed/nested forms; supporting both broadens the stable DSL and still requires semantic extraction.
3. **Replacement contract:** Cross-node CEL access uses exactly `nodes.<node_id>.output`; other access rooted at `nodes` fails compilation with a stable error.
4. **Long-term benefit:** One auditable reference grammar, complete predecessor/branch validation, and deterministic startup failure.
5. **Migration impact:** Agents using indexed/computed forms must convert to canonical dotted paths; no HTTP, persisted-data, or database migration is required.
6. **Rejected alternatives:** Larger regexes, indefinite constant-index support, or runtime missing-map failures.
7. **Required tests/docs:** Indexed/computed negative matrix, dotted positive/negative sequential/parallel/post-join coverage, error documentation, and a migration note.

### BASE-P3-004 — Narrow Formal V1 duration grammar

1. **Current interface and problem:** Public YAML timeout parsing inherits humantime's broad grammar despite a documented positive-integer `ms|s|m` contract.
2. **Why compatible/additive alternatives are inadequate:** Blessing both forms exposes dependency-specific aliases/fractions permanently; warnings do not define the final grammar.
3. **Replacement contract:** One positive base-10 integer immediately followed by exactly `ms`, `s`, or `m`.
4. **Long-term benefit:** A small portable grammar and precise compatibility tests.
5. **Migration impact:** Fractional, compound, alias, or larger-unit values must be converted; no HTTP/data/database migration is required.
6. **Rejected alternatives:** Bless all humantime forms, accept both indefinitely, or special-case selected extra units.
7. **Required tests/docs:** Accepted/rejected grammar table, serializer behavior, migration conversions, and checked-in-Agent compilation.

### BASE-P1-012 — HTTPS-only model transport by default

1. **Current interface and problem:** `open_ai_chat.base_url` accepts remote HTTP and sends full prompts plus optional bearer credentials over it.
2. **Why compatible/additive alternatives are inadequate:** Warnings/documentation still transmit data; TLS cannot be transparently added to an HTTP-only remote server.
3. **Replacement contract:** HTTPS with normal certificate/hostname validation is default; a new explicit development-only option may permit only precisely defined loopback HTTP.
4. **Long-term benefit:** Transport confidentiality and integrity become startup invariants aligned with strict secret handling.
5. **Migration impact:** Remote HTTP endpoints must move to TLS; eligible local development must explicitly opt in or use injected/TLS test clients. Agents, HTTP clients, Run data, and history schemas are unchanged.
6. **Rejected alternatives:** Warning-only, no-key exception, trusted-network assumption, or disabled certificate validation.
7. **Required tests/docs:** Scheme/host/opt-in matrix, certificate/hostname failures, authorization capture, redirect behavior, migration note, and production hardening guidance.

## Architecture and correctness remediation roadmap

These are independently reviewable proposals, not approvals. Every milestone requires its own design/plan; all user-approval cells are **No**.

| Order / milestone | Scope and findings | Prerequisites and cross-milestone dependencies | Required acceptance tests | User approved? |
|---|---|---|---|---|
| A0 — Sensitive error containment | `BASE-P1-010`; define one instance-free schema-error formatter and assess existing stored messages | None; land before A5 so compile-time validation reuses it | Linear/parallel Attached/Detached Action input/output secrets; raw DB, SSE, GET, logs; schema-keyword matrix | No |
| A1 — Provider memory bounds | `BASE-P1-011`; incremental body/line/event/text limits and safe failure | None; define byte semantics consumed by A7 | No-LF, one-line, aggregate text, usage, exact boundary, socket close, permits, subsequent Run | No |
| A2 — Preparing/active lifecycle ownership | `BASE-P1-006`; one admission epoch and drain contract | None; prerequisite for A4 recovery handoff | Barriers across every create await, API/process drain, attachment terminal, permit/EventHub cleanup | No |
| A3 — Authoritative stop semantics | `BASE-P1-007`; normalize shared winner and treat unbacked Stop as infrastructure | Extension contract agreement; use A6 custom-node harness when available or add focused harness here | Returned/shared-reason matrix with blocking siblings, terminal/code/event/permit assertions | No |
| A4 — Durable recovery and live-state finalization | `BASE-P1-008` and `BASE-P1-009`; bounded foreground, authoritative background owner, unconditional EventHub isolation | A2 lifecycle owner; one shared cleanup primitive for pre/post-commit uncertainty | SQLite/PostgreSQL lock matrices, commit ambiguity, reconciliation failure matrix, subscriber closure, shutdown handoff | No |
| A5 — Semantic compile-time validation | `BASE-P1-001`, `BASE-P1-002`, `BASE-P1-003`; Action static validation and parser-aware Handlebars/CEL references | A0 safe errors; explicit user approval for 001/003; coordinate CEL package milestone without bundling it | Literal/templated Action; Handlebars/CEL AST reference corpus; sequential/parallel/post-join; checked-in Agents | No |
| A6 — Extension integration contract | `BASE-P2-005`; synthetic YAML through compiler, scheduler, events, repository, terminal | Can follow A3 or supply its custom executor fixture | Generic custom-node success, mismatch, events/output/terminal, no core source edits | No |
| A7 — Body-free INFO observability | `BASE-P2-013`; Run/node identifiers, timing, counts, sizes | A1 byte accounting; no metrics backend implied | Recording subscriber across representative paths; consistency, once-only records, secret/body absence | No |
| A8 — Contract and transport decisions | `BASE-P1-012`, `BASE-P3-004`, `BASE-P3-014`; HTTPS default, timeout grammar, documentation cleanup | Explicit approval for 012/004; resolve loopback/DNS policy; docs-only 014 may land independently | TLS policy matrix; duration grammar matrix; full documentation route/replay search | No |

## Dynamic verification results

| Command | Result | Qualification |
|---|---|---|
| `cargo fmt --all -- --check` | PASS, exit 0 | No formatting output or warnings |
| `cargo clippy --all-targets --all-features -- -D warnings` | PASS, exit 0 | Zero Clippy warnings |
| `cargo test --all-targets` | PASS, exit 0; 177 passed, 0 failed, 0 ignored | Darwin arm64, Rust 1.94.1, debug/test profile. Its PostgreSQL harness took a source-defined early return because URL and CI were unset |
| `cargo audit` | PASS, exit 0 | 333 packages; zero vulnerability failures; one allowed unmaintained `paste` warning retained in the dependency report |
| `cargo deny check` | PASS, exit 0 | advisories/bans/licenses/sources all ok; 14 configured duplicate warnings and one unmatched MPL-2.0 allowance warning |
| Explicit `RUN_HISTORY_POSTGRES_URL=... cargo test --test history_postgres -- --nocapture` | PASS, exit 0; 1 passed, 0 failed, 0 ignored | Real PostgreSQL 16.14 on loopback; unique schema created, migrated, exercised, and dropped |

Passing gates do not invalidate the source/call-chain findings above. Three passing panic-containment tests deliberately panic spawned tasks; their captured panic behavior is expected test machinery, not an escaped failure.

## Review limitations

- This is a read-only assessment of `af414de`; no finding was reproduced by changing production/test code, and no remediation was implemented.
- Dynamic results are qualified to one Darwin arm64 host, Rust/Cargo 1.94.1, debug/test profiles, and PostgreSQL 16.14. Linux/CI, release profile, other toolchains/databases, TLS, and multi-process behavior were not dynamically established.
- Source-confirmed findings can still require the listed regression tests before a fix is accepted. Needs-verification items must not be represented as defects until their exact checks produce evidence.
- The real PostgreSQL gate is one broad repository contract test; it does not establish every parity/fidelity boundary listed under Needs verification.
- Recommendations, severities, breaking records, accepted-risk decisions, and roadmap order have received **no user approval**. This report is decision input only.
