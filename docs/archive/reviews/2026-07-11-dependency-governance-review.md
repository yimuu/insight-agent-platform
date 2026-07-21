# Dependency Governance Review

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

> Historical snapshot. DSL and scheduler references describe the retired graph runtime; the then-current authored contract was [DSL Authoring Surface Redesign](../specs/2026-07-17-dsl-authoring-surface-redesign.md), lowered to the Region/SSA runtime described by the historical README and vNext design.
>
> Current remediation status: see `docs/reviews/2026-07-12-remediation-status.md`.
> This document remains a dated audit snapshot and should not be read as the current open-work list.

## Audited baseline and date

- Product baseline: `af414de7f43bc4c5ce580ed77db17053faab7a9f` (`main@af414de`).
- Dependency inputs: `Cargo.toml`, version-4 `Cargo.lock`, `deny.toml`, and `rust-toolchain.toml`, unchanged from the product baseline.
- Review date: **2026-07-11, Asia/Shanghai**. Every current-version, release-date, MSRV, and maintenance claim is an as-of-2026-07-11 statement backed by the linked crates.io page and/or official upstream source.
- Toolchain: Rust/Cargo 1.94.1. Direct inventory: 25 production plus 2 development dependencies. Locked graph: 333 packages scanned by `cargo audit`.

## Executive assessment

The locked graph is reproducible, its license/source policy is sound, and the required dependency gates pass. Twenty of 27 direct/dev-direct crates are at their newest stable release; `bytes` and `regex` have compatible same-major updates; five direct lines require separately governed breaking-range migrations. Version lag and duplicate versions were not treated as defects by themselves.

Three confirmed dependency findings remain: **P0: 0, P1: 1, P2: 2, P3: 0**. PostgreSQL support has no SQLx TLS backend and therefore accepts remote plaintext under default `prefer`; the CEL line retains unmaintained `paste` after upstream moved to `cel`; and all public/deployment YAML parsing directly uses archived `serde_yaml`. All three recommendations are breaking replacement/policy projects and none is approved.

Governance classifications are intentionally distinct:

| Classification | Meaning in this report | Current items |
|---|---|---|
| Warning suppression | Changes policy output only; never closes technical risk | No ignore implemented or approved; a possible `paste` advisory ignore would require AR-DEP-001 approval |
| Accepted risk | Time-bounded accountable deferral with owner, controls, expiry, and exit | Two proposals, approvals **0** |
| Safe direct update | Same compatible range, independently reviewable, normal full gate | `bytes 1.12.0 -> 1.12.1`, `regex 1.12.4 -> 1.13.0` |
| Coupled upgrade | Behavior or graph changes span a semantic/stack boundary and need a dedicated matrix | JSON Schema, Axum, Reqwest, SHA-2/thiserror alignment, any future SQLx upgrade |
| Replacement project | An unmaintained/superseded component or unsafe contract needs a new supported boundary | PostgreSQL TLS policy, `cel-interpreter -> cel`, `serde_yaml` replacement |

Confirmed findings: **3**. Needs verification: **7**. Accepted-risk proposals: **2**; approvals: **0**. Roadmap groups: **8**.

## Inventory and policy baseline

The following is the direct/dev-direct inventory as of 2026-07-11. “Current” means newest stable, not a guarantee of future support. A quiet release line is not called unmaintained without an upstream declaration or advisory.

| Dependency | Locked | Newest stable and release date as of 2026-07-11 | Published MSRV | Maintenance / disposition | License |
|---|---:|---|---:|---|---|
| `async-trait` | 0.1.89 | [0.1.89](https://crates.io/crates/async-trait/0.1.89), 2025-08-14 | 1.56 | Current; [official repo](https://github.com/dtolnay/async-trait) | MIT OR Apache-2.0 |
| `axum` | 0.7.9 | [0.8.9](https://crates.io/crates/axum/0.8.9), 2026-04-14 | 1.80 | Active; coupled 0.8 HTTP/API migration via [official changelog](https://github.com/tokio-rs/axum/blob/main/axum/CHANGELOG.md) | MIT |
| `bytes` | 1.12.0 | [1.12.1](https://crates.io/crates/bytes/1.12.1), 2026-07-08 | 1.57 | Active patch; safe direct update candidate | MIT |
| `cel-interpreter` | 0.10.0 | [0.10.0](https://crates.io/crates/cel-interpreter/0.10.0), 2025-07-23 | Not published | Package line superseded by [`cel` 0.14.0](https://crates.io/crates/cel/0.14.0) in the [same upstream](https://github.com/cel-rust/cel-rust) | MIT |
| `chrono` | 0.4.45 | [0.4.45](https://crates.io/crates/chrono/0.4.45), 2026-06-04 | 1.62 | Current; [official repo](https://github.com/chronotope/chrono) | MIT OR Apache-2.0 |
| `chrono-tz` | 0.10.4 | [0.10.4](https://crates.io/crates/chrono-tz/0.10.4), 2025-07-11 | 1.65 | Current; [official repo](https://github.com/chronotope/chrono-tz) | MIT OR Apache-2.0 |
| `dotenvy` | 0.15.7 | [0.15.7](https://crates.io/crates/dotenvy/0.15.7), 2023-03-22 | 1.56.1 | Quiet; [upstream](https://github.com/allan2/dotenvy) does not declare retirement; Needs verification | MIT |
| `futures` | 0.3.32 | [0.3.32](https://crates.io/crates/futures/0.3.32), 2026-02-15 | 1.71 | Current; [official repo](https://github.com/rust-lang/futures-rs) | MIT OR Apache-2.0 |
| `handlebars` | 6.4.2 | [6.4.2](https://crates.io/crates/handlebars/6.4.2), 2026-06-24 | 1.85 | Current; [official repo](https://github.com/sunng87/handlebars-rust) | MIT |
| `humantime` | 2.4.0 | [2.4.0](https://crates.io/crates/humantime/2.4.0), 2026-07-02 | 1.60 | Current; [official repo](https://github.com/chronotope/humantime) | MIT OR Apache-2.0 |
| `jsonschema` | 0.18.3 | [0.47.0](https://crates.io/crates/jsonschema/0.47.0), 2026-07-07 UTC | 1.85 | Active; 29 pre-1.0 minor lines and semantic/API migration via [official guide](https://github.com/Stranger6667/jsonschema/blob/master/MIGRATION.md) | MIT |
| `regex` | 1.12.4 | [1.13.0](https://crates.io/crates/regex/1.13.0), 2026-07-09 | 1.65 | Active same-major; safe direct update candidate | MIT OR Apache-2.0 |
| `reqwest` | 0.12.28 | [0.13.4](https://crates.io/crates/reqwest/0.13.4), 2026-05-25 | 1.85 | Active; coupled TLS/provider/root migration via [official changelog](https://github.com/seanmonstar/reqwest/blob/master/CHANGELOG.md) | MIT OR Apache-2.0 |
| `serde` | 1.0.228 | [1.0.228](https://crates.io/crates/serde/1.0.228), 2025-09-27 | 1.56 | Current; [official repo](https://github.com/serde-rs/serde) | MIT OR Apache-2.0 |
| `serde_json` | 1.0.150 | [1.0.150](https://crates.io/crates/serde_json/1.0.150), 2026-05-21 | 1.71 | Current; [official repo](https://github.com/serde-rs/json) | MIT OR Apache-2.0 |
| `serde_yaml` | 0.9.34+deprecated | [0.9.34+deprecated](https://crates.io/crates/serde_yaml/0.9.34%2Bdeprecated), 2024-03-25 | 1.64 | Explicitly unmaintained; [official repo archived](https://github.com/dtolnay/serde-yaml) | MIT OR Apache-2.0 |
| `sha2` | 0.10.9 | [0.11.0](https://crates.io/crates/sha2/0.11.0), 2026-03-25 | 1.85 | Active; independent API/hash-golden migration, not duplicate convergence | MIT OR Apache-2.0 |
| `sqlx` | 0.9.0 | [0.9.0](https://crates.io/crates/sqlx/0.9.0), 2026-05-21 | 1.94.0 | Current; [official 0.9 changelog](https://github.com/transact-rs/sqlx/blob/main/CHANGELOG.md#090---2026-05-06) | MIT OR Apache-2.0 |
| `thiserror` | 1.0.69 | [2.0.18](https://crates.io/crates/thiserror/2.0.18), 2026-01-18 | 1.68 | Active; direct derive migration does not remove CEL's v1 line | MIT OR Apache-2.0 |
| `tokio` | 1.52.3 | [1.52.3](https://crates.io/crates/tokio/1.52.3), 2026-05-08 | 1.71 | Current; [official repo](https://github.com/tokio-rs/tokio) | MIT |
| `tokio-stream` | 0.1.18 | [0.1.18](https://crates.io/crates/tokio-stream/0.1.18), 2026-01-04 | 1.71 | Current; [official repo](https://github.com/tokio-rs/tokio) | MIT |
| `tokio-util` | 0.7.18 | [0.7.18](https://crates.io/crates/tokio-util/0.7.18), 2026-01-04 | 1.71 | Current; [official repo](https://github.com/tokio-rs/tokio) | MIT |
| `tracing` | 0.1.44 | [0.1.44](https://crates.io/crates/tracing/0.1.44), 2025-12-18 | 1.65 | Current; [official repo](https://github.com/tokio-rs/tracing) | MIT |
| `tracing-subscriber` | 0.3.23 | [0.3.23](https://crates.io/crates/tracing-subscriber/0.3.23), 2026-03-13 | 1.65 | Current; [official repo](https://github.com/tokio-rs/tracing) | MIT |
| `uuid` | 1.23.4 | [1.23.4](https://crates.io/crates/uuid/1.23.4), 2026-06-24 | 1.85 | Current; [official repo](https://github.com/uuid-rs/uuid) | Apache-2.0 OR MIT |
| `tower` (dev) | 0.5.3 | [0.5.3](https://crates.io/crates/tower/0.5.3), 2026-01-12 | 1.64 | Current; [official repo](https://github.com/tower-rs/tower) | MIT |
| `tempfile` (dev) | 3.27.0 | [3.27.0](https://crates.io/crates/tempfile/3.27.0), 2026-03-11 | 1.63 | Current; [official repo](https://github.com/Stebalien/tempfile) | MIT OR Apache-2.0 |

Policy results on this baseline:

- `cargo audit`: exit 0; 333 packages; zero vulnerability failures; one allowed INFO/unmaintained warning for `paste 1.0.15`, [RUSTSEC-2024-0436](https://rustsec.org/advisories/RUSTSEC-2024-0436.html), with no patched version.
- `cargo deny check`: exit 0; `advisories ok, bans ok, licenses ok, sources ok`; 14 configured multiple-version warnings and one `license-not-encountered` warning for the unused MPL-2.0 allowance.
- All resolved sources are crates.io registry sources; unknown registries and Git sources are denied. No yanked package or incompatible license was reported.

## Security findings

### DEP-P1-001 — PostgreSQL is compiled without TLS and uses plaintext under default prefer

- **Severity:** P1 — Near-term.
- **Evidence:** SQLx enables `runtime-tokio`, SQLite, PostgreSQL, chrono, json, migrate, and macros with defaults off but no TLS feature (`Cargo.toml:25`). SQLx 0.9 defaults to `PgSslMode::Prefer`, and its [tagged TLS connection code](https://github.com/transact-rs/sqlx/blob/v0.9.0/sqlx-postgres/src/connection/tls.rs#L23-L45) returns the raw socket when TLS is unavailable. Config accepts arbitrary PostgreSQL URLs (`src/config.rs:334-355`) and connects before migrations (`src/history/postgres.rs:25-36`).
- **Affected contract/subsystem:** PostgreSQL credentials, Run/event/output transport confidentiality/integrity, startup migration, config validation, and deployment guidance.
- **Trigger:** Any TCP PostgreSQL deployment under this build, including remote URLs, using default/prefer. Require/verify modes cannot supply TLS and fail instead.
- **Impact:** Authentication and all durable runtime data can traverse the network without encryption or server identity verification.
- **Why current safeguards are insufficient:** Secret redaction protects formatting, not the wire; URL scheme checks do not enforce TLS; merely enabling TLS while retaining `prefer` still allows downgrade.
- **Focused recommendation:** Enable one explicit SQLx rustls provider/root feature, parse `PgConnectOptions`, require `verify-full` for non-loopback TCP, and permit plaintext only through an explicit narrow loopback/Unix development exception.
- **Required verification:** Real TLS PostgreSQL matrix for valid CA/hostname, invalid CA, mismatch, no-TLS server, disable/prefer downgrade, local exception, redacted errors, migration, CRUD/CAS/recovery, reconnect, and shutdown.
- **Dependencies:** Decide provider/root/private-CA delivery; align with, but do not bundle into, Reqwest TLS work. No SQLx version update is required.
- **breaking:** yes — remote/non-explicit plaintext configurations that currently start would fail closed.

**Breaking change decision record**

1. **Current interface and problem:** Any secret PostgreSQL URL is accepted; omitted SSL mode effectively becomes plaintext because the build has no TLS implementation.
2. **Why compatible/additive alternatives are inadequate:** Redaction, VPN assumptions, server settings, a proxy recommendation, or TLS compiled under `prefer` do not enforce authenticated encryption or prevent downgrade.
3. **Replacement contract:** `verify-full` for non-loopback TCP with explicit trust-root handling; only an explicit narrow loopback/Unix development exception.
4. **Long-term benefit:** One auditable transport contract protects every history record and fails before migration/traffic.
5. **Migration impact:** Operators must provision certificates/trust roots and update connection policy; local plaintext setups must explicitly opt in or use Unix sockets.
6. **Rejected alternatives:** Keep prefer; `require` without identity verification; document trusted networks; suppress risk; proxy-only enforcement.
7. **Required tests/docs:** The TLS matrix, certificate/root/hostname guidance, local-development examples, migration/rollback instructions, and secret-free diagnostics.

No other known vulnerability was reported by RustSec as of 2026-07-11. The `paste` warning is a maintenance finding below; passing policy does not remediate it.

## Maintenance findings

### DEP-P2-002 — CEL retains unmaintained paste after upstream moved package lines

- **Severity:** P2 — Planned.
- **Evidence:** `cargo audit` reports `paste 1.0.15` under [RUSTSEC-2024-0436](https://rustsec.org/advisories/RUSTSEC-2024-0436.html), with no patched version. The only path is `paste -> cel-interpreter 0.10.0 -> insight-agent-platform`. The same official project now publishes [`cel` 0.14.0](https://crates.io/crates/cel/0.14.0), whose manifest uses `pastey`; its [0.14 release notes](https://github.com/cel-rust/cel-rust/releases/tag/v0.14.0) record breaking language/type/overload changes.
- **Affected contract/subsystem:** `core.condition` compilation/evaluation, JSON conversion, stable condition error codes, reference discovery, Agent DSL, and maintenance policy.
- **Trigger:** Continuing the locked line or needing a compiler/platform/security fix unavailable from archived `paste`.
- **Impact:** The graph has no patched upstream path for the transitive macro crate while CEL changes accumulate on a semantically evolved package.
- **Why current safeguards are insufficient:** Locking and warnings provide reproducibility/visibility, not fixes; an ignore or direct transitive fork does not move to maintained CEL.
- **Focused recommendation:** Replace `cel-interpreter` with current `cel` in an isolated semantic milestone after freezing expression/value/error/reference behavior; prove `paste` absent afterward.
- **Required verification:** CEL corpus for null/numbers/collections/field/index/functions/comprehensions/regex/time, errors/non-bool results, stable codes, semantic reference extraction, sequential/parallel graphs, and checked-in Agents.
- **Dependencies:** Resolve stable report `BASE-P1-002/003` consistently; close AR-DEP-001 only after lock-graph proof.
- **breaking:** yes — package/API and 0.x semantic changes can alter which expressions compile, evaluate, or route.

**Breaking change decision record**

1. **Current interface and problem:** `core.condition.when` inherits `cel-interpreter 0.10` syntax/value/error behavior and its unmaintained `paste` path.
2. **Why compatible/additive alternatives are inadequate:** Advisory suppression and a transitive fork do not restore upstream maintenance or verify CEL semantics.
3. **Replacement contract:** Current upstream `cel` behind project-owned conversion/error/reference normalization.
4. **Long-term benefit:** Supported releases, removal of `paste`, and a controlled future upgrade seam.
5. **Migration impact:** Some Agent expressions may require edits; stable platform error codes/output shape remain project-owned.
6. **Rejected alternatives:** Permanent ignore, `[patch]` fork, blind rename, or simultaneous language replacement.
7. **Required tests/docs:** Frozen corpus, checked-in Agents, compatibility/migration examples, and a time-bounded rollback that restores AR-DEP-001.

### DEP-P2-003 — Public configuration parsing directly depends on archived serde_yaml

- **Severity:** P2 — Planned.
- **Evidence:** The locked/latest [`serde_yaml 0.9.34+deprecated`](https://crates.io/crates/serde_yaml/0.9.34%2Bdeprecated) is explicitly unmaintained and its [official repository](https://github.com/dtolnay/serde-yaml) was archived on 2024-03-25. Agent, platform, and model YAML all call `serde_yaml::from_str` (`src/dsl/raw.rs:89-96`, `src/config.rs:137-143`, `src/resources/config.rs:25-35`).
- **Affected contract/subsystem:** All Agent/platform/model YAML syntax, strict fields, aliases/tags/scalars, diagnostics, resource bounds, and startup availability.
- **Trigger:** A parser/security/compatibility fix is needed, or a maintained parser differs in accepted behavior.
- **Impact:** Three fail-before-serving trust boundaries have no supported upstream fix path; emergency response requires a fork or hurried migration.
- **Why current safeguards are insufficient:** Strict deserialization constrains typed output after parsing but cannot repair tokenizer/parser defects or guarantee parser resource bounds.
- **Focused recommendation:** Introduce one project-owned YAML adapter, select a maintained parser through a compatibility/security/ownership bakeoff, and migrate all three surfaces together with explicit syntax/resource policy.
- **Required verification:** Checked-in files and adversarial corpus for unknown/duplicate fields, tags, anchors/merges, scalars, Unicode, depth/alias expansion, multi-doc/trailing input, paths/secrets, and stable outer error codes.
- **Dependencies:** Successor choice and resource policy remain Needs verification; coordinate any intentional syntax delta with config version/docs.
- **breaking:** yes — a maintained parser or safer subset can reject/reinterpret documents accepted today.

**Breaking change decision record**

1. **Current interface and problem:** Three public/deployment YAML surfaces inherit unmaintained `serde_yaml 0.9` syntax and diagnostics.
2. **Why compatible/additive alternatives are inadequate:** Permanent retention, warnings, or indefinite vendoring preserve maintenance failure; a blind crate swap can silently change meaning.
3. **Replacement contract:** One maintained parser behind a project adapter with documented YAML subset and resource policy.
4. **Long-term benefit:** Supported fixes, stable project-owned error mapping, and a small future replacement surface.
5. **Migration impact:** Some YAML constructs may need edits; outer codes stay stable, and a config-version bump is needed only for material documented syntax changes.
6. **Rejected alternatives:** Permanent retention, blind fork adoption, internal parser fork, or different parsers per file type.
7. **Required tests/docs:** Selection record, compatibility/resource corpus, accepted-syntax documentation, delta migration examples, and whole-parser rollback.

Confirmed count across Security and Maintenance: **3 total — P0: 0, P1: 1, P2: 2, P3: 0**. Breaking recommendations: **3**. User approvals: **0**.

## Compatibility and MSRV findings

- The pinned compiler is Rust 1.94.1. Current SQLx 0.9.0 publishes MSRV 1.94.0, leaving only a one-patch margin; every future SQLx selection must recheck MSRV before lockfile change.
- Current newer direct lines have different minimums: Axum 0.8 requires Rust 1.80; Reqwest 0.13, JSON Schema 0.47, SHA-2 0.11, Handlebars 6.4, and UUID 1.23 publish Rust 1.85; CEL 0.14 publishes Rust 1.86. All fit the pinned compiler as of 2026-07-11, but MSRV compatibility alone does not establish semantic compatibility.
- Axum 0.8 changes path capture syntax and extractor/serve behavior; Reqwest 0.13 changes TLS feature names, crypto provider, and platform roots. Each needs its own API/transport matrix.
- JSON Schema 0.47 changes construction, validation error behavior, drafts, retrieval, and registry APIs. CEL 0.14 contains explicit language/type/overload changes. Neither is a safe version bump.
- SHA-2 0.11 must preserve Agent hash golden values. thiserror 2 is largely derive-facing here, but direct alignment does not eliminate CEL's v1 transitive line.

No additional compatibility finding is confirmed: the current locked graph builds, lints, and tests under the pinned compiler.

## License and source findings

- `cargo deny check` reported `licenses ok` and `sources ok`. Every resolved package comes from crates.io; unknown registries and unknown Git sources are denied.
- Direct licenses are MIT, Apache-2.0, or the dual expression. No dependency requires a license outside policy.
- The only license warning is `license-not-encountered` for the configured MPL-2.0 allowance. It is stale policy surface, not an incompatible package or a reason for warning suppression.
- No license/source finding is confirmed.

## Direct dependency hygiene

- **Safe direct updates:** review `bytes 1.12.1` and `regex 1.13.0` independently under the normal full gate. Same-major compatibility does not waive verification.
- **Breaking/coupled upgrades:** Axum 0.8, JSON Schema 0.47, Reqwest 0.13, SHA-2 0.11, and thiserror 2 are separate changes. Do not combine them into one generic update branch.
- Direct SHA-2 0.11 cannot converge the graph because SQLx core and Handlebars' Pest build path retain SHA-2 0.10 while SQLx PostgreSQL already uses 0.11.
- Direct thiserror 2 cannot converge the graph while current CEL requires v1. Align only for project value, not duplicate-count reduction.
- Axum 0.7 already shares Tower 0.5, and Tokio/Tower are current; the Axum migration does not require their major upgrades.
- Reqwest deliberately disables defaults and selects rustls/JSON/streaming. Reqwest 0.13 still requires an explicit crypto-provider and trust-root choice rather than accepting new defaults implicitly.

## Duplicate-version root causes

`cargo tree --locked --duplicates` and cargo-deny report **14** true multiple-version warning groups. Warnings are evidence, not defects; no runtime type crossing or material standalone cost was demonstrated.

| Groups | Locked split | Root cause | Disposition |
|---|---|---|---|
| `bit-set`, `bit-vec` | 0.5/0.8; 0.6/0.8 | JSON Schema fancy-regex vs CEL ANTLR parser | Coupled CEL/JSON Schema work only |
| `block-buffer`, `crypto-common`, `digest`, `cpufeatures`, `sha2` | paired RustCrypto 0.10/0.11 chains | App/SQLx core/Handlebars build vs SQLx PostgreSQL | Direct SHA-2 cannot converge; revisit after ecosystem moves |
| `getrandom` | 0.2/0.3/0.4 | rustls ring, JSON Schema/ahash, UUID/CEL/SQLx/tempfile | Ecosystem-coupled; JSON Schema 0.47 still does not fully converge |
| `hashbrown` | 0.16/0.17 | SQLx hashlink vs indexmap consumers | No demonstrated cost beyond compiled graph |
| `nom` | 7/8 | CEL vs JSON Schema ISO-8601 parsing | Ecosystem-coupled |
| `r-efi` | 5/6 | getrandom target-support split | Follows randomness convergence |
| `thiserror`, `thiserror-impl` | 1/2 | App/CEL v1 vs Handlebars/SQLx v2 | Revisit with CEL; direct alignment alone is incomplete |
| `windows-sys` | 0.52/0.61 | rustls ring vs current Tokio/CLI/tempfile | Target-specific; no demonstrated defect |

Same-version nodes printed through multiple feature paths are not duplicate-version findings.

## CEL upgrade assessment

- Blast radius: `CelProgram::compile`, JSON-to-context conversion, `Program::execute`, bool/type results, ordered routing, stable condition error codes, graph dependency discovery, and all checked-in expressions (`src/nodes/condition.rs`, `src/dsl/compiler.rs`).
- Migration is a replacement project, not a package rename. First freeze null/numeric/map/list/field/index, functions/comprehensions/regex/time, errors, and reference extraction.
- Coordinate stable findings `BASE-P1-002/003` so AST access and the canonical node-reference grammar are solved once. Do not bundle JSON Schema, SQLx, or HTTP stack upgrades.
- Exit gate: all CEL/graph/Agent tests pass and `cargo tree -i paste --locked` proves the path absent.

## JSON Schema upgrade assessment

- Locked `jsonschema 0.18.3` default features activate unused CLI plus HTTP/file resolution and blocking Reqwest. Production uses schemas for Agent input, Action input/output, and OpenAI parameters; it never uses the CLI.
- Locked-source tracing indicates compilation records an external `$ref`, while the first `is_valid`/`validate` traversal that reaches it performs synchronous retrieval. No repository test dynamically proves this phase boundary, timeout/cache behavior, or Tokio blocking cost, so it remains Needs verification rather than a fourth finding.
- Before upgrading, explicitly decide drafts, no-`$schema` fallback, formats, allowed local/remote/file references, resolver/registry policy, and safe error cardinality/order/path/message. Coordinate instance-free errors with `BASE-P1-010`.
- Then migrate 0.18 to 0.47 using the [official migration guide](https://github.com/Stranger6667/jsonschema/blob/master/MIGRATION.md). Do not justify it as duplicate convergence: current 0.47 still does not eliminate the randomness split.

## SQLx upgrade assessment

- SQLx 0.9.0 is current as of 2026-07-11. The immediate TLS finding is a feature/configuration-policy change, **not** a version-upgrade recommendation.
- Every enabled non-TLS feature has a concrete use: Tokio runtime, SQLite/PostgreSQL, chrono/json adapters, embedded migrations, macros/FromRow, pools, transactions, dynamic queries, terminal CAS/recovery, pagination, and reconciliation.
- The `sqlite` umbrella also enables load-extension/deserialization/unlock-notify; a narrower supported feature set is a verification candidate, not an assumed cleanup.
- Any future SQLx patch/next-minor gate must recheck MSRV, exact features, bundled SQLite, both migration trees, JSON/timestamps, transaction/CAS/recovery, pagination/reconciliation, and real TLS-enabled PostgreSQL.

## Explicitly accepted dependencies

No dependency risk is accepted or approved by this review. **Accepted-risk proposals: 2; approvals: 0.** No Cargo audit ignore, cargo-deny skip/allow, manifest patch, or dependency change was implemented.

1. **AR-DEP-001 — temporary paste advisory acceptance proposal.** Scope: only `RUSTSEC-2024-0436` / `paste 1.0.15` through `cel-interpreter 0.10.0`. Owner: CEL replacement milestone. Rationale: INFO/unmaintained, no known vulnerability or patched release, replacement is semantically coupled. Controls: locked source, no direct use, audit on every change. Expiry: 2026-10-31 or first CEL migration release, whichever is earlier. Exit: maintained `cel` graph with `paste` absent. Any warning suppression requires explicit approval of this exact record and does not count as remediation.
2. **AR-DEP-002 — temporary serde_yaml retention proposal.** Scope: startup parsing of checked-in platform/model/Agent YAML. Owner: configuration compatibility milestone. Rationale: archived/deprecated with no reported audit vulnerability. Controls: strict Serde envelopes, trusted deployment files, pinned lock, existing parser tests. Expiry: 2026-12-31. Exit: approved maintained parser plus compatibility/resource corpus. This is risk acceptance, not advisory suppression, because cargo-audit emitted no serde_yaml advisory.

No accepted-risk proposal exists for remote plaintext PostgreSQL. A general ignore, `sslmode=prefer`, or “trusted network” statement is not an adequate replacement for authenticated TLS.

## Needs verification

Exactly **7** items remain outside the confirmed count:

1. **dotenvy maintenance intent:** inspect upstream ownership, issues/releases, and current CI before classifying its quiet line; release age alone is insufficient.
2. **YAML successor selection:** compare ownership, security policy, cadence, YAML coverage, duplicate/alias/depth controls, licenses, MSRV, and the full compatibility corpus; no successor is selected here.
3. **CEL 0.14 compatibility details:** build a read-only probe for JSON numbers, maps/indexing, overloads, regex/time, comprehensions, errors, and AST access to enumerate exact Agent deltas.
4. **JSON Schema external-reference reachability:** controlled HTTP/file probes must prove compilation makes zero requests, first `is_valid`/`validate` retrieval, timeout/redirect/error/cache/repeat behavior, and Tokio-worker impact before severity is assigned.
5. **SQLx TLS deployment shape:** choose WebPKI/native roots, private-CA delivery/rotation, Unix sockets, container DNS, and exact loopback-development semantics; these decisions do not weaken the confirmed no-TLS finding.
6. **SQLx SQLite umbrella cost:** prove whether `sqlite-bundled` plus narrower features preserves macros, migrations, concurrency, and all SQLite/event tests before feature reduction is proposed.
7. **Axum/Reqwest major behavior:** compile probes identify edits, but full route/extractor/SSE and TLS/provider/root/stream/cancellation matrices are required before scheduling either migration.

## Dependency remediation roadmap

The roadmap contains exactly **8** independently reviewable groups. None is approved; user approvals are **0**. Warning suppression is never an exit criterion.

| Order / group | Change type and scope | Prerequisites / coupling | Required tests and exit evidence | Approved? |
|---|---|---|---|---|
| R0 — PostgreSQL TLS contract | Replacement/security policy; close `DEP-P1-001` without changing SQLx version | Decide provider/roots/private CA/local exception; coordinate config docs | Full real TLS matrix, migrations, CRUD/CAS/recovery, redaction; remote plaintext never connects | No |
| R1 — CEL semantic replacement | Replacement; close `DEP-P2-002` and AR-DEP-001 | Freeze corpus; coordinate `BASE-P1-002/003` canonical references | CEL value/error/reference corpus, graph/Agents, stable codes; `paste` absent | No |
| R2 — JSON Schema contract and upgrade | Coupled semantic upgrade 0.18 -> 0.47 | First resolve draft/reference/retrieval policy and NV-4; coordinate `BASE-P1-010` | Agent/Action/model schema matrix, external-reference behavior, safe errors, async responsiveness | No |
| R3 — YAML parser replacement | Replacement; close `DEP-P2-003` and AR-DEP-002 | Select maintained parser and explicit YAML/resource policy | Checked-in/adversarial corpus, stable outer codes, documented deltas/rollback | No |
| R4 — Compatible lock refresh | Safe direct updates for bytes and regex only | Independent of other groups | Full format/Clippy/tests/audit/deny; lock diff contains only intended compatible paths | No |
| R5 — HTTP stack majors | Coupled but separate Axum 0.8 and Reqwest 0.13 changes | Complete NV-7; explicit Reqwest crypto/root choice; do not combine the two commits | Route/auth/extractor/SSE matrix; provider/restricted-HTTP TLS/redirect/timeout/stream/cancel/redaction | No |
| R6 — Direct macro/crypto alignment | Coupled direct migrations for SHA-2 0.11 and thiserror 2, independently reviewed | Agent hash golden contract; error-derive behavior; no duplicate-convergence promise | Golden hashes, all error derives/codes, full gates, documented residual duplicate paths | No |
| R7 — Future SQLx upgrade gate | Governance gate, not current upgrade recommendation | A future version must remain MSRV-compatible; R0 TLS contract already established | Exact feature audit, both DB/migrations, transactions/CAS/recovery/pagination/reconciliation, TLS Postgres | No |

### R6 execution note — direct macro/crypto alignment

R6 was implemented as direct dependency ownership cleanup rather than a mechanical `thiserror = "2"` declaration.

- The root crate now directly depends on `sha2 0.11` for Agent version hashing.
- The existing stable compiler fixture preserves the exact Agent hash `sha256:ddb7849ef262359b787d928f2bca65c90cfe5e670fad04a4a15614af0cf6f30c`.
- The root crate no longer directly depends on `thiserror` because project error types are handwritten and no project-owned `thiserror::Error` derives are used.
- Residual `sha2 0.10` paths remain expected through SQLx core/sqlite/macro paths and the Handlebars/Pest build path.
- Residual `thiserror 1` remains expected through CEL, while `thiserror 2` remains expected through Handlebars and SQLx.
- This phase does not change public API routes, Formal V1 schemas, DSL syntax, Run lifecycle, SSE behavior, persistence, or user-facing error codes.

## Primary sources

Current-version links for every direct dependency appear in the inventory. The material decision sources are:

- [RustSec RUSTSEC-2024-0436 for paste](https://rustsec.org/advisories/RUSTSEC-2024-0436.html).
- [CEL 0.14.0 crates.io release](https://crates.io/crates/cel/0.14.0), [official repository](https://github.com/cel-rust/cel-rust), and [0.14 release notes](https://github.com/cel-rust/cel-rust/releases/tag/v0.14.0).
- [serde_yaml 0.9.34+deprecated](https://crates.io/crates/serde_yaml/0.9.34%2Bdeprecated) and the [archived official repository](https://github.com/dtolnay/serde-yaml).
- [JSON Schema 0.47.0](https://crates.io/crates/jsonschema/0.47.0), [migration guide](https://github.com/Stranger6667/jsonschema/blob/master/MIGRATION.md), and [changelog](https://github.com/Stranger6667/jsonschema/blob/master/CHANGELOG.md).
- [SQLx 0.9.0](https://crates.io/crates/sqlx/0.9.0), [official 0.9 changelog](https://github.com/transact-rs/sqlx/blob/main/CHANGELOG.md#090---2026-05-06), and [tagged TLS connection source](https://github.com/transact-rs/sqlx/blob/v0.9.0/sqlx-postgres/src/connection/tls.rs#L23-L45).
- [Axum official changelog](https://github.com/tokio-rs/axum/blob/main/axum/CHANGELOG.md) and [Reqwest official changelog](https://github.com/seanmonstar/reqwest/blob/master/CHANGELOG.md).
- crates.io version pages and official repositories linked row-by-row in the inventory for release, license, MSRV, and maintenance evidence as of 2026-07-11.

## Review limitations

- This is a read-only review of the locked `af414de` graph. No dependency, feature, policy, toolchain, manifest, lockfile, source, test, or configuration change was made.
- Primary-source status is dated 2026-07-11; later releases/advisories can change the inventory and must be rechecked when a roadmap group begins.
- `cargo audit` and cargo-deny identify known/policy-visible conditions, not the absence of undisclosed vulnerabilities. An allowed warning is not remediation.
- Duplicate analysis establishes roots, not measured build-time/binary-size cost. No convergence is promised without a feasible path and demonstrated value.
- CEL, YAML, JSON Schema, HTTP-stack, and SQLx decisions require the listed semantic/environment matrices; compile-only success is insufficient.
- The two accepted-risk records are proposals only, approvals are **0**, and no recommendation or breaking change has user approval.
