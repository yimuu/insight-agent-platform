# Remediation Status

Date: 2026-07-12

This document is the current operational status entrypoint for the stable-baseline and dependency-governance remediation work. It supersedes the open-work status in the 2026-07-11 reviews, but it does not replace their historical evidence.

Historical snapshots:

- `docs/reviews/2026-07-11-stable-baseline-review.md`
- `docs/reviews/2026-07-11-dependency-governance-review.md`

## Repository state

- Branch: `main`.
- Remote state: `main` is synchronized with `origin/main`.
- Stable-baseline tag candidate: latest `main` after this status refresh is pushed.
- Scope of this document: status synchronization only. It does not change runtime code, dependencies, migrations, API behavior, DSL behavior, SSE behavior, or persistence behavior.

## Executive status

- Stable-baseline remediation milestones A0-A8 are implemented.
- Dependency-governance groups R0-R6 are implemented.
- R7 remains a future SQLx upgrade gate and is not required for the current baseline.
- Residual duplicate dependencies remain where upstream crates still require separate major lines. They are tracked, not suppressed.
- Needs verification items remain open unless an implemented milestone directly covers them.

## Stable baseline remediation status

| Milestone | Original finding scope | Current status | Evidence | Remaining note |
|---|---|---|---|---|
| A0 — Sensitive error containment | `BASE-P1-010` | Implemented | `7740cfd`, `5d37fde`, `62bb41f`, `f39ac47`; `docs/superpowers/plans/2026-07-11-action-error-containment.md` | Historical stored messages were reset/covered by the history reset note. |
| A1 — Provider memory bounds | `BASE-P1-011` | Implemented | `8593326`, `dcd7d47`, `fa9f878`, `8293d52`, `d7990da`, `e1851a9`; `docs/superpowers/specs/2026-07-11-provider-memory-bounds-design.md` | Broader provider-specific limits remain future design work only if new providers are added. |
| A2 — Preparing/active lifecycle ownership | `BASE-P1-006` | Implemented | `aba2c18`, `969ff2a`, `19a87d7`, `fca31f2`; `docs/superpowers/specs/2026-07-11-preparing-active-lifecycle-ownership-design.md` | Real-binary lifecycle remains under Needs verification. |
| A3 — Authoritative stop semantics | `BASE-P1-007` | Implemented | `0cb6bcb`, `7cc18fa`, `a50e7e6`; `docs/superpowers/specs/2026-07-11-authoritative-stop-semantics-design.md` | Extension runtime coverage is implemented separately by A6. |
| A4 — Durable recovery and live-state finalization | `BASE-P1-008`, `BASE-P1-009` | Implemented | `fcf245a`, `087102b`, `616d0f4`, `258dc81`; `docs/superpowers/specs/2026-07-11-durable-recovery-finalization-design.md` | Multi-process/PostgreSQL parity remains Needs verification. |
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

## Open Needs verification

The items below remain verification work, not confirmed defects. They should be selected as future engineering/review tasks only when their exact check is valuable.

### Stable-baseline Needs verification still open

1. Type/executor registry parity.
2. Synchronous execution preemption for large template/CEL work and CPU-bound custom executors.
3. Direct Chat cancellation during stream acquisition and chunk boundaries.
4. Dropped create-future ownership at pre-launch awaits.
5. PostgreSQL exclusive-store topology.
6. Outer Run-task panic cleanup.
7. Repository parity and full-record fidelity across SQLite/PostgreSQL.
8. End-to-end input-summary privacy in raw backend rows.
9. Defensive concurrent terminal CAS.
10. EventHub terminal boundary.
11. Internal pagination boundaries.
12. Stored event identity policy.
13. Restricted HTTP DNS/private-address/rebinding policy.
14. OpenAI clean-EOF semantics.
15. Real-binary lifecycle.
16. Restricted HTTP positive boundary matrix.
17. API/auth/error/SSE boundary matrix.
18. Readiness and deployment paths.
19. Documentation precedence across older replay/reconnect-grace designs.
20. Checked-in example execution policy.

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

1. Tag the current synchronized `main` as the stable baseline if the team wants an explicit rollback/reference point.
2. Treat `docs/reviews/2026-07-12-remediation-status.md` as the current open-work entrypoint.
3. Do not repeat A0-A8 or R0-R6 unless a regression is found.
4. Choose future work from either R7 or one Needs verification item at a time.
