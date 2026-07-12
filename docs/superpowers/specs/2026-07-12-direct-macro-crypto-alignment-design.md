# Direct Macro/Crypto Alignment Design

Date: 2026-07-12

Status: Design approved in conversation; awaiting written spec review.

Source review: `docs/reviews/2026-07-11-dependency-governance-review.md` R6 — Direct macro/crypto alignment.

## Purpose

R6 aligns the repository's direct dependency ownership for hash and error-helper crates without pretending to solve upstream duplicate dependency lines. The project directly uses SHA-256 to build the formal Agent version hash, so the direct `sha2` dependency should move to the active 0.11 line with a golden contract test. The project does not directly use `thiserror` derives today, so the stale direct `thiserror = "1"` dependency should be removed instead of upgraded mechanically.

This keeps the dependency baseline stable and intentional: direct dependencies must correspond to project-owned behavior; transitive duplicates are documented as upstream constraints.

## Current state

- `Cargo.toml` directly declares `sha2 = "0.10"`.
- `src/dsl/compiler.rs` imports `sha2::{Digest, Sha256}` and computes `CompiledAgent.version_hash` as `sha256:<hex>`.
- `Cargo.lock` already contains both `sha2@0.10.9` and `sha2@0.11.0`.
  - `sha2@0.10.9` is used by the app, SQLx core/sqlite/macro paths, and the Handlebars/Pest build path.
  - `sha2@0.11.0` is used by SQLx PostgreSQL.
- `Cargo.toml` directly declares `thiserror = "1"`.
- Project source has no direct `thiserror::Error` derive usage. Existing project error types use handwritten `Display` and `std::error::Error` implementations.
- `Cargo.lock` already contains both `thiserror@1.0.69` and `thiserror@2.0.18`.
  - `thiserror@1.0.69` is retained by CEL.
  - `thiserror@2.0.18` is retained by Handlebars and SQLx.
- `rust-toolchain.toml` pins Rust `1.94.1`; `sha2 0.11`'s Rust 1.85 floor fits the repository baseline.

## Goals

1. Move the project-owned SHA-256 implementation dependency from direct `sha2 0.10` to direct `sha2 0.11`.
2. Preserve the Agent version hash contract with an exact golden hash test generated from the current pre-upgrade behavior.
3. Remove the unused direct `thiserror` dependency instead of upgrading it to a still-unused direct `thiserror 2` dependency.
4. Document residual transitive duplicate paths for `sha2` and `thiserror`.
5. Keep all public runtime, API, DSL, SSE, Run, persistence, and error response behavior unchanged.

## Non-goals

- Do not attempt to converge all `sha2` transitive lines in this phase.
- Do not attempt to converge all `thiserror` transitive lines in this phase.
- Do not upgrade SQLx, CEL, Handlebars, or Pest as part of R6.
- Do not rewrite project error types to `thiserror` derives.
- Do not change the Agent hash algorithm, hash input ordering, hash prefix, or persisted version-hash semantics.
- Do not introduce public interface changes.

## Chosen approach

Adopt the dependency-ownership cleanup approach:

1. Add a golden hash assertion for an existing stable DSL compiler fixture while the repository still uses direct `sha2 0.10`.
2. Upgrade the direct `sha2` manifest entry to `sha2 = "0.11"`.
3. Verify the exact golden hash still matches after the upgrade.
4. Remove the direct `thiserror` manifest entry because the project does not directly use it.
5. Update dependency-governance documentation to state that residual duplicate `sha2` and `thiserror` paths are expected and upstream-owned.

This intentionally differs from a literal "`thiserror = 2` direct alignment" because retaining an unused direct dependency is worse governance than removing it. If a future error type benefits from derive-based implementation, that feature can add `thiserror = "2"` in the same change that introduces the direct usage.

## Architecture and behavior

### Agent hash path

The Agent hash remains owned by `src/dsl/compiler.rs`:

1. Load and normalize the raw Agent configuration with `serde_json::to_vec`.
2. Feed the normalized raw configuration bytes into `Sha256`.
3. Feed prompt names and prompt bodies into `Sha256` with zero-byte separators.
4. Return `sha256:<lowercase-hex-digest>`.

The implementation may need minor API import adjustments if `sha2 0.11` changes trait paths, but it must preserve the data flow and output format exactly. The golden test is the contract guard.

### Error handling path

Project error behavior remains handwritten and unchanged. Removing direct `thiserror` must not alter:

- Error codes.
- `Display` strings.
- API error response bodies.
- `std::error::Error` implementation availability.

The implementation should not add derive macros or alter error type definitions for this phase.

### Dependency graph expectation

After R6:

- `cargo tree -i sha2@0.10.9` should no longer show the root crate as a direct user, but may still show SQLx core/sqlite/macro paths and the Handlebars/Pest build path.
- `cargo tree -i sha2@0.11.0` should show the root crate as a direct user and may still show SQLx PostgreSQL.
- `cargo tree -i thiserror@1.0.69` should no longer show the root crate as a direct user, but may still show CEL.
- `cargo tree -i thiserror@2.0.18` should remain present through Handlebars and SQLx.

These residual duplicates are accepted. The acceptance criterion is direct dependency correctness, not full duplicate convergence.

## Interface impact

No public interface changes are intended or allowed in R6.

- No HTTP route changes.
- No Formal V1 request/response schema changes.
- No DSL syntax changes.
- No Run lifecycle changes.
- No SSE behavior changes.
- No migration or history table changes.
- No user-facing error-code changes.

The only externally observable contract under test is that existing Agent hash values for the golden fixture remain identical after the `sha2` upgrade.

## Testing and verification design

The implementation must use a hash-golden-first workflow:

1. Capture the exact `CompiledAgent.version_hash` for the existing stable compiler fixture before changing `sha2`.
2. Add an exact golden assertion to the DSL compiler tests.
3. Run the targeted test and verify it passes on the old dependency line.
4. Upgrade `sha2` and remove direct `thiserror`.
5. Re-run the targeted test and verify the exact hash is unchanged.

Required verification gates:

- `cargo test --test dsl_compiler compiles_valid_graph_and_hashes_prompt_contents -- --exact --nocapture`
- `cargo test --test formal_agent_compile -- --nocapture`
- `cargo fmt --check`
- `cargo test --all-targets --all-features -- --nocapture --test-threads=1`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo audit`
- `cargo deny check`
- `cargo tree -i sha2@0.10.9`
- `cargo tree -i sha2@0.11.0`
- `cargo tree -i thiserror@1.0.69`
- `cargo tree -i thiserror@2.0.18`

## Documentation updates

R6 should update the dependency-governance record or a focused follow-up note with:

- Direct `sha2` moved to 0.11.
- Direct `thiserror` removed as unused.
- Agent hash golden contract preserved.
- Residual duplicate `sha2` and `thiserror` paths are upstream-owned and expected.
- No public API or DSL changes were introduced.

## Rollback strategy

If `sha2 0.11` changes the hash output, trait behavior, or digest formatting in a way that cannot be reconciled while preserving the golden contract, rollback is simple:

1. Revert the `sha2` manifest change.
2. Keep or remove the golden test based on whether it still represents the current intended contract.
3. Keep the direct `thiserror` removal only if the rest of the branch remains valid and all verification gates pass.

If removing direct `thiserror` unexpectedly breaks compilation, the failure indicates hidden direct usage not found by source search. In that case, either restore the direct dependency with an explicit direct usage rationale or migrate that usage to `thiserror 2` in a separate reviewed change.

## Acceptance criteria

R6 is complete when:

1. The direct manifest dependency is `sha2 = "0.11"`.
2. `Cargo.toml` no longer directly declares `thiserror`.
3. The Agent hash golden test passes before and after the `sha2` upgrade.
4. Full formatting, test, clippy, audit, and deny gates pass.
5. Dependency tree evidence shows the root crate no longer directly owns `sha2@0.10.9` or `thiserror@1.0.69`.
6. Documentation explains why residual transitive duplicate paths remain.
7. No public API, DSL, SSE, Run, persistence, or error-response behavior changes are introduced.
