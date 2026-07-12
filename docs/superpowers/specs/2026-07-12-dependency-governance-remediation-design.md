# Dependency Governance Remediation Design

Date: 2026-07-12

## Status

Design approved in conversation on 2026-07-12 for one continuous remediation pass over the dependency-governance review, scoped to `R0 + R1 + R3 + R4`.

## Context

`docs/reviews/2026-07-11-dependency-governance-review.md` identified three confirmed dependency findings and a dependency roadmap. The stable runtime baseline A0-A8 has already landed, so dependency governance can now proceed without mixing runtime hardening work into dependency remediation.

This design deliberately does not interpret "一次性完成" as "one giant undifferentiated dependency bump". The remediation is one branch and one delivery wave, but it keeps independently reviewable commits and verification gates because the review itself says the roadmap groups are independently reviewable and that warning suppression is never remediation.

## Goals

- Close `DEP-P1-001` by adding an explicit PostgreSQL TLS transport contract without upgrading SQLx.
- Close `DEP-P2-002` by replacing `cel-interpreter` with the maintained `cel` package and proving the `paste` path is gone.
- Close `DEP-P2-003` by replacing direct `serde_yaml` usage with a project-owned YAML adapter backed by a maintained parser.
- Complete `R4` by refreshing only compatible `bytes` and `regex` lockfile versions.
- Preserve project-owned public error codes, DSL semantics, API/event/repository shapes, and secret-redaction behavior unless explicitly listed below.

## Non-goals

- Do not upgrade JSON Schema to 0.47 in this branch.
- Do not upgrade Axum to 0.8 in this branch.
- Do not upgrade Reqwest to 0.13 in this branch.
- Do not align direct SHA-2 or thiserror versions in this branch.
- Do not upgrade SQLx itself in this branch.
- Do not add cargo-audit ignores or cargo-deny warning suppressions as a substitute for remediation.
- Do not change database schema, migrations, HTTP response shapes, SSE envelope shapes, event repository shapes, or Run status semantics.

## R0 PostgreSQL TLS contract

### Decision

Keep SQLx at `0.9.0` and enable the SQLx Rustls WebPKI TLS backend through `tls-rustls-ring-webpki`. This matches the existing Reqwest/OpenAI transport posture: WebPKI roots, Rustls, and explicit exceptions rather than implicit plaintext trust.

PostgreSQL URL validation moves from string-prefix checks to `PgConnectOptions` parsing plus a small project policy layer. Remote TCP PostgreSQL must use `sslmode=verify-full`. Plaintext PostgreSQL is allowed only for exact local-development targets:

- host `localhost`;
- host `127.0.0.1`;
- host `[::1]` or `::1` as represented by SQLx parsing;
- Unix socket paths, if SQLx exposes the connection as a socket path rather than a TCP host.

Every other TCP PostgreSQL URL fails config loading before migrations or repository startup.

### Interface impact

This is intentionally breaking for remote PostgreSQL deployments that currently use implicit plaintext or `sslmode=prefer`. The replacement contract is:

```text
Remote PostgreSQL: sslmode=verify-full
Local development PostgreSQL: exact loopback or Unix socket only
```

The public config field remains `history.database_url_env`. No new top-level config key is introduced in this branch. This keeps the surface narrow and makes the PostgreSQL URL itself the single source of transport policy.

### Error handling

Errors use the existing platform config error boundary. Diagnostics must not include passwords, complete URLs with credentials, or environment variable values. Messages may include policy names such as `sslmode=verify-full` and generic host classes such as `loopback`.

### Verification

Unit/config tests must cover:

- remote PostgreSQL without `sslmode` fails;
- remote PostgreSQL with `sslmode=prefer`, `allow`, `disable`, `require`, or `verify-ca` fails;
- remote PostgreSQL with `sslmode=verify-full` passes config validation;
- loopback development PostgreSQL passes without TLS;
- non-exact loopback aliases do not bypass the policy where raw host text is available;
- URL secrets are redacted from errors.

Real PostgreSQL integration remains the existing opt-in `RUN_HISTORY_POSTGRES_URL` test. The test documentation should use a local loopback URL for local development and a `verify-full` example for remote deployments.

## R1 CEL replacement

### Decision

Replace `cel-interpreter = "0.10.0"` with `cel = "0.14.0"` from the same upstream project. Keep `cel-parser = "0.10.1"` for compile-time reference extraction unless the implementation proves it is unnecessary and can be removed without reducing validation coverage.

The runtime condition node gets a project-owned wrapper around `cel::Program`, `cel::Context`, and `cel::Value`. The wrapper owns:

- compilation error mapping to existing `DSL_COMPILE_INVALID`;
- execution error mapping to existing condition/runtime error codes;
- JSON input conversion into CEL values;
- boolean-result enforcement.

This prevents the rest of the runtime from depending directly on upstream CEL error shapes.

### Compatibility contract

The branch must freeze the current condition behavior with tests before replacement. Required corpus:

- `nodes.<id>.output` object access;
- booleans, strings, integers, floats, null;
- list and map access used by checked-in agents/tests;
- missing fields and non-bool results;
- branch selection order for first true condition;
- compile-time invalid expressions.

If `cel` 0.14 differs on expressions not used or not documented by Formal V1, prefer the maintained package and document the delta in `docs/formal-v1-breaking-changes.md` only when the delta affects accepted public Agent YAML.

### Exit evidence

`cargo tree -i paste --locked` must fail with "package ID specification `paste` did not match any packages" or equivalent absence evidence.

## R3 YAML parser replacement

### Decision

Replace direct `serde_yaml` calls with a project-owned adapter module:

```rust
pub(crate) fn from_str<T>(source: &str, surface: YamlSurface) -> Result<T, String>
where
    T: serde::de::DeserializeOwned;

pub(crate) fn to_value<T>(value: T) -> Result<yaml_serde::Value, String>
where
    T: serde::Serialize;
```

The adapter is backed by `yaml_serde = "0.10.4"`. The selection rationale:

- `yaml_serde` is the actively maintained fork of `serde_yaml` published by the YAML Organization;
- it preserves the current `serde_yaml`-style API and minimizes public YAML semantic drift;
- it closes the archived `serde_yaml` dependency finding without turning this remediation into a broader YAML policy redesign;
- its current crate metadata reports Rust 1.82, MIT/Apache licensing, and repository ownership under `github.com/yaml/yaml-serde`.

`noyalib` remains a viable future alternative for a stricter YAML policy project because it is pure Rust, offers `ParserConfig::strict()`, can reject duplicate keys, and was faster in a local micro-benchmark against the current repository fixtures. It is not selected for this branch because it would introduce broader YAML behavior changes and has a `0.0.x` API maturity profile. `serde_yml` is explicitly excluded because its current release is deprecated and forwards to another library.

### Adapter policy

All public/deployment YAML entry points must call the adapter:

- `src/dsl/raw.rs`;
- `src/config.rs`;
- `src/resources/config.rs`;
- tests that need YAML serialization helpers.

The adapter maps parser-specific errors into strings at the boundary. Existing outer codes stay stable:

- Agent YAML parse failures remain `DSL_YAML_INVALID`;
- platform config YAML parse failures remain `PLATFORM_CONFIG_INVALID`;
- model resources YAML parse failures remain `MODEL_CONFIG_INVALID`.

The adapter must reject multi-document YAML consistently with current `serde_yaml` behavior. It should not add new duplicate-key rejection or resource-budget policy in this branch, because those are semantic changes that belong in a dedicated YAML policy project with its own compatibility corpus.

### Verification

Add adapter-level and existing-surface tests for:

- valid checked-in Agent/platform/model YAML still loads;
- unknown fields still fail through existing outer codes;
- multi-document input fails;
- parser errors do not include secret environment variable values.

## R4 compatible lock refresh

### Decision

Refresh only:

- `bytes 1.12.0 -> 1.12.1`;
- `regex 1.12.4 -> 1.13.0`.

No Axum, Reqwest, JSON Schema, SQLx, SHA-2, thiserror, CEL, or YAML parser upgrade may be hidden in the R4 commit. R4 may be committed separately before or after the replacement commits, but its lockfile diff must be reviewed independently.

## Source evidence

The implementation plan must cite the local command evidence collected before this design:

- `cargo info sqlx@0.9.0 --verbose` shows `tls-rustls-ring-webpki`.
- `cargo info cel@0.14.0` shows MSRV 1.86 and the maintained `cel` package line.
- `cargo info yaml_serde` shows `yaml_serde 0.10.4`, Rust 1.82, YAML Organization repository ownership, and MIT/Apache licensing.
- `cargo info noyalib` plus a local repository-fixture micro-benchmark show that `noyalib` is a plausible future stricter-parser option, but it is not selected for this compatibility-focused remediation.
- `cargo info jsonschema@0.47.0` confirms JSON Schema upgrade remains a separate R2 project.

## Rollout and rollback

This branch is intended to merge only after all selected remediation groups pass full verification. Rollback is normal git rollback: because no schema/API/repository migration is introduced, reverting the dependency-governance commits restores the previous dependency graph and config behavior.

Operational rollout notes:

- Remote PostgreSQL deployments must update URLs to `sslmode=verify-full` and provide certificates/hostnames compatible with WebPKI verification.
- Local PostgreSQL development may keep loopback plaintext URLs.
- Agent YAML using CEL expressions outside the frozen corpus may need edits if `cel` 0.14 differs from `cel-interpreter` 0.10.
- YAML parser migration is expected to preserve current `serde_yaml`-style syntax for repository Agent/platform/model YAML; any newly discovered parser delta must be documented before merge.

## Acceptance criteria

1. `Cargo.toml` no longer depends on `cel-interpreter`.
2. `cargo tree -i paste --locked` proves `paste` is absent.
3. `Cargo.toml` no longer depends on `serde_yaml`.
4. All YAML entry points use the project adapter.
5. PostgreSQL support compiles with a SQLx Rustls TLS feature.
6. Remote PostgreSQL config without `sslmode=verify-full` fails before repository startup.
7. Exact loopback/local PostgreSQL development URLs still work.
8. `bytes` and `regex` are refreshed without unrelated lockfile churn.
9. Existing public error codes remain stable.
10. No API/event/repository schema/migration changes are introduced.
11. `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --all-targets --quiet` pass.
12. `cargo audit` and `cargo deny check` pass with no new warnings beyond documented residual duplicate/license warnings; the `paste` advisory is gone rather than suppressed.
