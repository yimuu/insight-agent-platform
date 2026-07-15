# Remediation Status

Current through: 2026-07-15

This document is the current operational status entrypoint for the stable-baseline and dependency-governance remediation work. It supersedes the open-work status in the 2026-07-11 reviews, but it does not replace their historical evidence.

Historical snapshots:

- `docs/reviews/2026-07-11-stable-baseline-review.md`
- `docs/reviews/2026-07-11-dependency-governance-review.md`

## Repository state

- Primary integration branch: `main`.
- Historical rollback/reference tag: `stable-baseline-2026-07-12`.
- This status includes the post-tag quickstart, Chat, Select, unified End, typed terminal,
  canonical repository terminal proposal, public Agent discovery, production lifecycle
  hardening, PostgreSQL exclusive-store ownership, and raw-row input-summary privacy
  through 2026-07-15.
- Remote synchronization is an operational checkout fact and is intentionally not
  encoded as a durable source contract in this document.

## Executive status

- Stable-baseline remediation milestones A0-A8 are implemented.
- Dependency-governance groups R0-R6 are implemented.
- R7 remains a future SQLx upgrade gate and is not required for the current baseline.
- Residual duplicate dependencies remain where upstream crates still require separate major lines. They are tracked, not suppressed.
- The original Needs verification list now records `Addressed`, `Partial`, or `Open`
  dispositions so completed post-baseline work is not presented as untouched.

## Post-baseline contract work

| Area | Current status | Evidence | Remaining note |
|---|---|---|---|
| Real binary quickstart | Implemented | `tests/binary_smoke.rs`; `0af0548`, `5bb1d94`, `78475f6` | No quickstart-specific verification boundary remains; lifecycle verification is recorded under Production lifecycle V1. |
| Chat optional image, dynamic message sources, and stream completion | Implemented | `3ce4352`, `0b29aa7`, `ef64358`; `docs/superpowers/specs/2026-07-15-openai-stream-completion-evidence-design.md`; `tests/formal_resources.rs` | Direct Chat cancellation remains open. |
| Condition result convergence | Implemented | `3b995e3`, `912d3cb`, `926d87d` | No additional Select expansion is currently planned. |
| Unified End and typed terminal model | Implemented | `559bfc8` through `7ef2cee`; terminal suites under `tests/event_hub.rs` and `tests/core_end.rs`; `docs/superpowers/specs/2026-07-15-independent-connection-terminal-cas-race-design.md`; `independent_sqlite_connections_choose_one_authoritative_terminal_without_lock_residue`; `postgres_independent_connections_resolve_one_authoritative_terminal` | Independent-connection CAS is verified within SQLite and one PostgreSQL owner generation; distributed execution remains out of scope. |
| Canonical repository terminal proposal | Implemented | `7a6865a`; `tests/history_sqlite_v1.rs`, `tests/history_postgres.rs`, `tests/event_hub.rs` | PostgreSQL exclusive-store topology is handled by the ownership milestone below. |
| Public Agent input contract | Implemented in the 2026-07-15 change | `docs/superpowers/specs/2026-07-15-public-agent-contract-design.md`; schema, API, compiler, and binary smoke tests | Output schemas and Agent CRUD are intentionally out of scope. |
| Production lifecycle V1 | Implemented and verified | `docs/superpowers/specs/2026-07-15-production-lifecycle-v1-design.md`; `docs/superpowers/specs/2026-07-15-real-process-sigint-http-supervisor-design.md`; `sigterm_terminalizes_attached_and_detached_runs_and_restart_preserves_them`; `sigint_terminalizes_attached_and_detached_runs_and_restart_preserves_them`; `finite_http_error_drains_real_runs_to_exact_attachment_terminals`; `http_completion_during_runtime_drain_is_latched_before_graceful_command`; `supervisor_preserves_fatal_precedence_and_rejects_signal_failure`; `owner_release_ownership_loss_promotes_every_lower_shutdown_outcome` | Unix SIGTERM and SIGINT are verified against the production binary, and real PostgreSQL ownership-loss fail-stop is covered by the exclusive-store milestone. Axum 0.8.9 retries `TcpListener` accept errors indefinitely and `WithGracefulShutdown` returns only after its shutdown future completes, so unexpected early HTTP completion is verified at the private production-supervisor future boundary without a runtime fault-injection surface. Windows console-control behavior and deployment trusted-path policy remain outside this verification. |
| Outer Run-task panic fail-stop | Implemented in the 2026-07-15 change | `docs/superpowers/specs/2026-07-15-outer-run-task-panic-fail-stop-design.md`; direct scheduler/finalizer panic, recovery-failure, start-gate, waiter, sticky-fatal, and shutdown-decision tests in `src/runtime/service.rs` and `src/main.rs` | Production uses a payload-independent global panic hook; deterministic task injection remains private to unit tests rather than becoming a runtime backdoor. |
| PostgreSQL exclusive-store ownership | Implemented in the 2026-07-15 change | `docs/superpowers/specs/2026-07-15-postgresql-exclusive-store-ownership-design.md`; `tests/history_postgres.rs`; `tests/binary_postgres_ownership.rs` | Real PostgreSQL repository and process gates cover contention, clean release, ownership loss, fencing, and replacement reconciliation. Deployment requires a direct or session-affine PostgreSQL connection; old runtimes cannot participate in a rolling upgrade. |
| Raw-row input-summary privacy | Implemented in the 2026-07-15 change | `docs/superpowers/specs/2026-07-15-raw-row-input-summary-privacy-design.md`; `sqlite_run_service_persists_only_shape_metadata_in_raw_row`; `postgres_run_service_persists_only_shape_metadata_in_raw_jsonb_row` | The formal `RunService` path persists sorted top-level keys and compact serialized byte count only. Those metadata remain intentionally visible; lower-level repository callers and Agent-authored outputs are outside this guarantee. |

## Stable baseline remediation status

| Milestone | Original finding scope | Current status | Evidence | Remaining note |
|---|---|---|---|---|
| A0 — Sensitive error containment | `BASE-P1-010` | Implemented | `7740cfd`, `5d37fde`, `62bb41f`, `f39ac47`; `docs/superpowers/plans/2026-07-11-action-error-containment.md` | Historical stored messages were reset/covered by the history reset note. |
| A1 — Provider memory bounds | `BASE-P1-011` | Implemented | `8593326`, `dcd7d47`, `fa9f878`, `8293d52`, `d7990da`, `e1851a9`; `docs/superpowers/specs/2026-07-11-provider-memory-bounds-design.md` | Broader provider-specific limits remain future design work only if new providers are added. |
| A2 — Preparing/active lifecycle ownership | `BASE-P1-006` | Implemented | `aba2c18`, `969ff2a`, `19a87d7`, `fca31f2`; `docs/superpowers/specs/2026-07-11-preparing-active-lifecycle-ownership-design.md` | Real-binary signal and recovery behavior is covered by Production Lifecycle V1. |
| A3 — Authoritative stop semantics | `BASE-P1-007` | Implemented | `0cb6bcb`, `7cc18fa`, `a50e7e6`; `docs/superpowers/specs/2026-07-11-authoritative-stop-semantics-design.md` | Extension runtime coverage is implemented separately by A6. |
| A4 — Durable recovery and live-state finalization | `BASE-P1-008`, `BASE-P1-009` | Implemented | `fcf245a`, `087102b`, `616d0f4`, `258dc81`; `docs/superpowers/specs/2026-07-11-durable-recovery-finalization-design.md` | PostgreSQL whole-store ownership and replacement reconciliation are implemented and verified separately. |
| A5 — Semantic compile-time validation | `BASE-P1-001`, `BASE-P1-002`, `BASE-P1-003` | Implemented | `c62f9b1`, `e38604d`, `934f806`, `da78868`, `675ed90`, `dff6b00`, `659f47d`; `docs/superpowers/specs/2026-07-11-semantic-compile-time-validation-design.md` | Additional CEL language compatibility is covered by dependency work and future verification. |
| A6 — Extension integration contract | `BASE-P2-005` | Implemented | `3a75440`, `a147f5f`, `3fb7854`, `4e02c9e`; `docs/superpowers/specs/2026-07-12-extension-integration-contract-design.md` | Third-party extension packaging is outside current scope. |
| A7 — Body-free INFO observability | `BASE-P2-013` | Implemented | `4129a53`, `7feb556`, `7a11634`, `eb86bba`, `865628e`; `docs/superpowers/specs/2026-07-12-body-free-info-observability-design.md` | Metrics backend/export remains outside current scope. |
| A8 — Contract and transport decisions | `BASE-P1-012`, `BASE-P3-004`, `BASE-P3-014` | Implemented | `595f91d`, `25bad11`, `2537dbd`, `980e66f`, `7f3b890`, `363aacf`, `962e256`; `docs/superpowers/specs/2026-07-12-contract-transport-decisions-design.md` | DNS/rebinding policy remains Needs verification for restricted HTTP. |

## Dependency governance remediation status

| Group | Original scope | Current status | Evidence | Remaining note |
|---|---|---|---|---|
| R0 — PostgreSQL TLS contract | Close `DEP-P1-001` without SQLx version upgrade | Implemented | `836ac1c`, `3f20031`, `72731e7`; `Cargo.toml` uses `sqlx` feature `tls-rustls-ring-webpki`; config tests require `sslmode=verify-full` for remote PostgreSQL | Real deployment certificate/private-CA matrix remains a deployment verification topic. |
| R1 — CEL semantic replacement | Close `DEP-P2-002` and AR-DEP-001 | Implemented | `c911b55`, `60a2384`, `bedb548`; current graph has `cel@0.14.0`; `paste` and `cel-interpreter` are absent | Further CEL semantic probes remain useful for future expression-language changes. |
| R2 — JSON Schema contract and upgrade | Upgrade `jsonschema` behind project adapter | Implemented | `3c87b95`, `9175da3`, `11c26b4`, `8e5635b`, `0eeccd3`; current graph has `jsonschema@0.47.0` | Adapter policy is now the project boundary for future upstream changes. |
| R3 — YAML parser replacement | Close `DEP-P2-003` and AR-DEP-002 | Implemented | `06d715e`; current graph has `yaml_serde@0.10.4`; `serde_yaml` is absent | YAML compatibility/resource corpus can still be expanded, but archived parser use is closed. |
| R4 — Compatible lock refresh | Safe direct updates for bytes and regex | Implemented | `55bc8cb` | No current follow-up unless a new compatible refresh is requested. |
| R5 — HTTP stack majors | Axum 0.8 and Reqwest 0.13 | Implemented | `8b966fa`, `63bee41`, `d4fa74d`, `62006fa`, `c81f495`; current graph has `axum@0.8.9` and `reqwest@0.13.4` | Public Formal V1 paths and live-only SSE contract remain unchanged. |
| R6 — Direct macro/crypto alignment | SHA-2 0.11 and thiserror direct dependency cleanup | Implemented and pushed | `0357aba`, `1fe9ca1`, `d211a49`, `ebb3a00`, `62695a9`, `4a35941`, `7db444f`; root crate directly uses `sha2@0.11.0`; direct `thiserror` was removed | Residual `sha2 0.10` and `thiserror 1` are upstream-owned transitive paths. |
| R7 — Future SQLx upgrade gate | Governance gate for any future SQLx upgrade | Future gate / not executed | Current direct SQLx remains `0.9.0`, which was current in the 2026-07-11 review | Execute only when a future SQLx version is selected; requires its own MSRV, feature, DB, migration, TLS, and recovery matrix. |

## Residual duplicate dependency status

Residual duplicate dependencies are tracked as dependency-graph facts, not treated as automatic defects and not hidden by warning suppression.

- `sha2 0.10/0.11`: direct root ownership moved to `sha2 0.11`; `sha2 0.10` remains through SQLx core/sqlite/macro paths and the Handlebars/Pest build path.
- `thiserror 1/2`: direct root `thiserror` was removed; `thiserror 1` remains through CEL and `thiserror 2` remains through Handlebars/SQLx.
- Other duplicate groups such as `getrandom`, `hashbrown`, `windows-sys`, and Rustls-related splits remain ecosystem-coupled. They need separate value/risk analysis before any future convergence work.

## Needs verification disposition

These were originally uncertainty-preserving review items, not confirmed defects.
`Addressed` means the requested decision and direct regression boundary now exist;
`Partial` retains the unverified remainder.

| # | Original item | Disposition | Current evidence or remaining boundary |
|---|---|---|---|
| 1 | Type/executor registry parity | Addressed | Formal V1 intentionally terminalizes a missing executor as infrastructure failure; `custom_node_missing_executor_terminalizes_as_infrastructure_failure`. |
| 2 | Synchronous execution preemption | Open | Large Handlebars/CEL and CPU-bound extension responsiveness has not been dynamically established. |
| 3 | Direct Chat cancellation | Open | Stream acquisition and between-chunk stop behavior still need a dedicated fake-provider matrix. |
| 4 | Dropped create-future ownership | Addressed | Preparing ownership and capacity release are covered by `attached_subscription_drop_before_launch_finalizes_cancelled` and `dropped_detached_create_future_releases_capacity_after_durable_create`. |
| 5 | PostgreSQL exclusive-store topology | Addressed | A dedicated-session advisory lock plus persistent generation fence now enforces fail-before-bind contention and fail-stop ownership loss; real PostgreSQL repository and process gates cover replacement reconciliation and stale-writer rejection. |
| 6 | Outer Run-task panic cleanup | Addressed | `scheduler_outer_panic_trips_fatal_recovers_terminal_and_releases_ownership`, `prelaunch_finalizer_outer_panic_uses_same_fatal_cleanup_and_recovery`, `panic_recovery_failure_still_releases_local_ownership_and_shutdown_waiters`, and `active_task_start_gate_blocks_work_until_registration` directly cover fatality, terminal recovery, active/permit cleanup, waiter convergence, and shutdown. |
| 7 | Repository parity and full-record fidelity | Partial | Both real backends cover lifecycle and canonical terminal proposals; the complete mirrored raw-record matrix remains open. |
| 8 | End-to-end input-summary privacy | Addressed | `sqlite_run_service_persists_only_shape_metadata_in_raw_row` and `postgres_run_service_persists_only_shape_metadata_in_raw_jsonb_row` send secret-bearing input through the formal `RunService`, then prove native raw rows contain exactly sorted top-level keys and compact serialized byte count with no raw or JSON-escaped values. PostgreSQL inspection is forced read-only and stays inside one owner generation. |
| 9 | Defensive concurrent terminal CAS | Addressed | `independent_sqlite_connections_choose_one_authoritative_terminal_without_lock_residue` and `postgres_independent_connections_resolve_one_authoritative_terminal` directly prove one requested winner, one exact authoritative loser, one durable terminal, contiguous events, third-proposal idempotency, independent physical connections, and no lock residue. PostgreSQL stays inside one exclusive owner generation. |
| 10 | EventHub terminal boundary | Addressed | Typed-only projection, generic terminal rejection, authoritative same-type conflicts, recovery, and cleanup are covered in `tests/event_hub.rs`. |
| 11 | Internal pagination boundaries | Open | Complete SQLite/PostgreSQL cursor and limit parity remains unverified. |
| 12 | Stored event identity policy | Open | Nonterminal append identity normalization/rejection still needs an explicit decision. |
| 13 | Restricted HTTP DNS/private-address/rebinding policy | Open | The action currently validates HTTPS and host allowlisting, not connected peer addresses or rebinding. |
| 14 | OpenAI clean-EOF semantics | Addressed | `[DONE]` is the only successful application-stream completion evidence; `openai_done_marker_completes_and_closes_an_open_transport`, the clean-EOF matrix, final JSON/UTF-8 truncation cases, and the post-finish transport-error case directly cover completion and failure precedence. |
| 15 | Real-binary lifecycle | Addressed | Real executable coverage includes public probes, in-flight Attached/Detached SIGTERM and SIGINT terminalization, restart persistence, crash reconciliation/idempotency, and bounded hard-deadline failure through `sigterm_terminalizes_attached_and_detached_runs_and_restart_preserves_them` and `sigint_terminalizes_attached_and_detached_runs_and_restart_preserves_them`. Private production-supervisor and shutdown-decision tests `finite_http_error_drains_real_runs_to_exact_attachment_terminals`, `http_completion_during_runtime_drain_is_latched_before_graceful_command`, `supervisor_preserves_fatal_precedence_and_rejects_signal_failure`, and `owner_release_ownership_loss_promotes_every_lower_shutdown_outcome` verify early HTTP `Err`/`Ok`, completion during runtime drain, fatal-cause precedence, signal-handler failure, release-time ownership-loss promotion, and exact durable Attached/Detached terminals. Axum 0.8.9 retries `TcpListener` accept errors indefinitely, so this is supervisor-boundary verification rather than a claim of a naturally triggerable real-process accept failure; no runtime backdoor was added. The Unix suite does not establish Windows console-control behavior. |
| 16 | Restricted HTTP positive boundary matrix | Open | Allowed TLS, redirects, size, timeout, cancellation, redaction, and socket-close cases need a local test service. |
| 17 | API/auth/error/SSE boundary matrix | Partial | Core routes, list/detail auth, validation, live-only SSE, cancellation, and typed failures are covered; an exhaustive route/header/lag/error matrix remains open. |
| 18 | Readiness and deployment paths | Partial | Public probes, bounded repository checks, startup-before-bind, two-phase drain, and real PostgreSQL ownership-loss fail-stop behavior are verified; trusted path policy remains open. |
| 19 | Documentation precedence | Open | Historical replay/reconnect designs still need explicit supersession markers. |
| 20 | Checked-in example execution | Addressed | The real binary smoke compiles and executes checked-in `code_node_demo` and constructs its input from the discovered schema. |

### Dependency Needs verification status

| Item | Current disposition |
|---|---|
| dotenvy maintenance intent | Still open; no remediation was attempted. |
| YAML successor selection | Superseded for immediate remediation by the approved `yaml_serde` replacement; deeper parser bakeoff can be reopened only if needed. |
| CEL 0.14 compatibility details | Partially addressed by R1 and A5; broader language corpus remains optional future verification. |
| JSON Schema external-reference reachability | Addressed by R2 adapter policy for production use; additional upstream behavior probes are optional future verification. |
| SQLx TLS deployment shape | Partially addressed by R0 policy and tests; private CA / real deployment matrix remains future verification. |
| SQLx SQLite umbrella cost | Still open; no feature reduction was attempted. |
| Axum/Reqwest major behavior | Addressed by R5 for current public API and transport contract. |

## Recommended next actions

1. Treat this document as the current open-work entrypoint and keep the 2026-07-11 reviews as historical evidence.
2. Prioritize direct Chat cancellation with a dedicated fake-provider matrix covering cancellation during stream acquisition and between chunks, then complete SQLite/PostgreSQL repository parity starting with pagination and full-record fidelity.
3. Keep restricted HTTP DNS/private-address/rebinding work deferred until before `http_get` is enabled for a production Agent.
4. Do not repeat A0-A8, R0-R6, or completed terminal-model work unless a regression is found.
5. Execute R7 only when a concrete future SQLx version is selected; it is not the default next task.
