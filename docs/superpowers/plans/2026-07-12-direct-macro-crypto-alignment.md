# Direct Macro/Crypto Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move direct SHA-256 ownership to `sha2 0.11`, remove unused direct `thiserror`, and prove the Agent hash contract remains stable.

**Architecture:** Treat R6 as direct dependency ownership cleanup, not duplicate convergence. First freeze the existing Agent hash as a golden test under the current `sha2 0.10` behavior, then move the root crate to `sha2 0.11`. Remove direct `thiserror` because project code does not use derives or other direct APIs; document remaining transitive duplicates as upstream-owned.

**Tech Stack:** Rust 1.94.1, Cargo, `sha2 0.11`, transitive `thiserror 1/2`, SQLx 0.9.0, CEL 0.14.0, Handlebars 6.4.2, existing DSL compiler tests, cargo-audit, cargo-deny.

## Global Constraints

- Scope is exactly dependency-governance R6: direct macro/crypto alignment.
- Move the project-owned SHA-256 implementation dependency from direct `sha2 0.10` to direct `sha2 0.11`.
- Preserve the Agent version hash contract with an exact golden hash test generated from the current pre-upgrade behavior.
- Remove the unused direct `thiserror` dependency instead of upgrading it to a still-unused direct `thiserror 2` dependency.
- Do not attempt to converge all `sha2` transitive lines in this phase.
- Do not attempt to converge all `thiserror` transitive lines in this phase.
- Do not upgrade SQLx, CEL, Handlebars, or Pest as part of R6.
- Do not rewrite project error types to `thiserror` derives.
- Do not change the Agent hash algorithm, hash input ordering, hash prefix, or persisted version-hash semantics.
- Do not introduce public interface changes.
- No HTTP route changes.
- No Formal V1 request/response schema changes.
- No DSL syntax changes.
- No Run lifecycle changes.
- No SSE behavior changes.
- No migration or history table changes.
- No user-facing error-code changes.

---

## File Structure

- `tests/dsl_compiler.rs`: add the exact Agent hash golden contract for the existing stable compiler fixture.
- `Cargo.toml`: change direct `sha2` from `0.10` to `0.11`; remove direct `thiserror`.
- `Cargo.lock`: reflect the root package dependency change while preserving expected transitive duplicate paths.
- `docs/reviews/2026-07-11-dependency-governance-review.md`: add an R6 execution note explaining the direct-dependency decision and residual duplicate paths.

## Task 1: Freeze the Agent Hash Golden Contract

**Files:**
- Modify: `tests/dsl_compiler.rs`

**Interfaces:**
- Consumes: `compiler().compile_dir(&root).unwrap() -> CompiledAgent`.
- Consumes: `CompiledAgent.version_hash: String`.
- Produces: `const VALID_AGENT_VERSION_HASH: &str` in `tests/dsl_compiler.rs`.
- Produces: an exact contract assertion for `compiles_valid_graph_and_hashes_prompt_contents`.

- [ ] **Step 1: Add the golden hash constant**

In `tests/dsl_compiler.rs`, insert this constant immediately after `fn valid_yaml() -> &'static str`:

```rust
const VALID_AGENT_VERSION_HASH: &str =
    "sha256:45c05b5e6d369203beb19b3665b6218ea9005111b05a25d369dd";
```

- [ ] **Step 2: Replace the prefix-only hash assertions**

In `tests/dsl_compiler.rs`, inside `compiles_valid_graph_and_hashes_prompt_contents`, replace:

```rust
    assert_eq!(first.version_hash, second.version_hash);
    assert!(first.version_hash.starts_with("sha256:"));
```

with:

```rust
    assert_eq!(
        first.version_hash, VALID_AGENT_VERSION_HASH,
        "Agent hash changed before the sha2 migration"
    );
    assert_eq!(
        second.version_hash, VALID_AGENT_VERSION_HASH,
        "Agent hash is not stable across repeated compiles"
    );
```

Do not remove the later `assert_ne!(first.version_hash, changed.version_hash);` check; it still proves prompt contents participate in the hash.

- [ ] **Step 3: Verify the characterization test passes before dependency changes**

Run:

```bash
cargo test --test dsl_compiler compiles_valid_graph_and_hashes_prompt_contents -- --exact --nocapture
```

Expected: PASS while `Cargo.toml` still contains `sha2 = "0.10"`. If this fails, do not update the golden blindly; inspect `serde_json::to_vec(raw)` ordering and prompt hashing input first.

- [ ] **Step 4: Commit Task 1**

Run:

```bash
git diff -- tests/dsl_compiler.rs
git add tests/dsl_compiler.rs
git commit -m "test: freeze agent hash golden contract"
```

Expected: one commit containing only `tests/dsl_compiler.rs`.

## Task 2: Upgrade Direct SHA-2 to 0.11

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

**Interfaces:**
- Consumes: `use sha2::{Digest, Sha256};` in `src/dsl/compiler.rs`.
- Consumes: `fn agent_hash(raw: &super::RawAgent, prompts: &BTreeMap<String, String>) -> Result<String, CompileError>`.
- Produces: the same `sha256:<lowercase-hex-digest>` Agent hash output through direct `sha2 0.11`.

- [ ] **Step 1: Change the direct dependency declaration**

In `Cargo.toml`, change:

```toml
sha2 = "0.10"
```

to:

```toml
sha2 = "0.11"
```

Do not change SQLx, CEL, Handlebars, Pest, or other dependency lines in this task.

- [ ] **Step 2: Run the golden hash test to refresh lock state and verify behavior**

Run:

```bash
cargo test --test dsl_compiler compiles_valid_graph_and_hashes_prompt_contents -- --exact --nocapture
```

Expected:

- Cargo resolves the root crate's direct `sha2` dependency to `0.11.0`.
- The test passes with `sha256:45c05b5e6d369203beb19b3665b6218ea9005111b05a25d369dd`.
- No source change is required in `src/dsl/compiler.rs` unless `sha2 0.11` reports a compile error for the existing `Digest`/`Sha256` API.

- [ ] **Step 3: Inspect the SHA-2 dependency paths**

Run:

```bash
cargo tree -i sha2@0.10.9
cargo tree -i sha2@0.11.0
```

Expected:

- `cargo tree -i sha2@0.10.9` still shows SQLx core/sqlite/macro paths and the Handlebars/Pest build path.
- `cargo tree -i sha2@0.10.9` no longer has an immediate child line for `insight-agent-platform v0.1.0`.
- `cargo tree -i sha2@0.11.0` shows an immediate child line for `insight-agent-platform v0.1.0`.
- `cargo tree -i sha2@0.11.0` may also show SQLx PostgreSQL paths.

- [ ] **Step 4: Commit Task 2**

Run:

```bash
git diff -- Cargo.toml Cargo.lock src/dsl/compiler.rs tests/dsl_compiler.rs
git add Cargo.toml Cargo.lock src/dsl/compiler.rs
git commit -m "chore: upgrade direct sha2 to 0.11"
```

Expected:

- Commit includes `Cargo.toml` and `Cargo.lock`.
- Commit includes `src/dsl/compiler.rs` only if an actual `sha2 0.11` API adjustment was required.
- Commit does not include `tests/dsl_compiler.rs`; that file was already committed in Task 1.

## Task 3: Remove Unused Direct thiserror

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

**Interfaces:**
- Consumes: existing handwritten project error implementations.
- Produces: no direct `thiserror` dependency in the root package.
- Produces: unchanged project error codes, `Display` messages, API error envelopes, and `std::error::Error` implementations.

- [ ] **Step 1: Confirm there is no direct thiserror usage**

Run:

```bash
rg -n "thiserror|derive\\([^)]*Error|#\\[error\\(" src tests
```

Expected:

- No `thiserror::Error` import in `src` or `tests`.
- No project-owned `#[derive(Error)]` usage.
- No project-owned `#[error(...)]` attributes.

- [ ] **Step 2: Remove the direct dependency declaration**

In `Cargo.toml`, delete this line:

```toml
thiserror = "1"
```

Do not add `thiserror = "2"` in this phase.

- [ ] **Step 3: Compile all targets to catch hidden direct usage**

Run:

```bash
cargo check --all-targets --all-features
```

Expected: PASS. A compile failure mentioning `thiserror` means there is hidden direct usage; in that case, stop and inspect the failing path instead of re-adding the dependency blindly.

- [ ] **Step 4: Inspect thiserror dependency paths**

Run:

```bash
cargo tree -i thiserror@1.0.69
cargo tree -i thiserror@2.0.18
```

Expected:

- `cargo tree -i thiserror@1.0.69` shows CEL as the remaining owner.
- `cargo tree -i thiserror@1.0.69` no longer has an immediate child line for `insight-agent-platform v0.1.0`.
- `cargo tree -i thiserror@2.0.18` remains present through Handlebars and SQLx.
- The root crate may still appear as the parent of CEL/Handlebars/SQLx paths; that is transitive ownership, not a direct dependency.

- [ ] **Step 5: Commit Task 3**

Run:

```bash
git diff -- Cargo.toml Cargo.lock
git add Cargo.toml Cargo.lock
git commit -m "chore: remove unused direct thiserror"
```

Expected: one commit containing only `Cargo.toml` and `Cargo.lock`.

## Task 4: Document R6 Evidence and Run Full Gates

**Files:**
- Modify: `docs/reviews/2026-07-11-dependency-governance-review.md`

**Interfaces:**
- Consumes: Task 1 golden hash result.
- Consumes: Task 2 SHA-2 dependency tree output.
- Consumes: Task 3 thiserror dependency tree output.
- Produces: a written governance note that direct dependency ownership changed while residual duplicates remain expected.

- [ ] **Step 1: Add the R6 execution note**

In `docs/reviews/2026-07-11-dependency-governance-review.md`, insert this section immediately after the remediation roadmap table and before `## Primary sources`:

```markdown

### R6 execution note — direct macro/crypto alignment

R6 was implemented as direct dependency ownership cleanup rather than a mechanical `thiserror = "2"` declaration.

- The root crate now directly depends on `sha2 0.11` for Agent version hashing.
- The existing stable compiler fixture preserves the exact Agent hash `sha256:45c05b5e6d369203beb19b3665b6218ea9005111b05a25d369dd`.
- The root crate no longer directly depends on `thiserror` because project error types are handwritten and no project-owned `thiserror::Error` derives are used.
- Residual `sha2 0.10` paths remain expected through SQLx core/sqlite/macro paths and the Handlebars/Pest build path.
- Residual `thiserror 1` remains expected through CEL, while `thiserror 2` remains expected through Handlebars and SQLx.
- This phase does not change public API routes, Formal V1 schemas, DSL syntax, Run lifecycle, SSE behavior, persistence, or user-facing error codes.
```

- [ ] **Step 2: Run targeted regression tests**

Run:

```bash
cargo test --test dsl_compiler compiles_valid_graph_and_hashes_prompt_contents -- --exact --nocapture
cargo test --test formal_agent_compile -- --nocapture
```

Expected:

- `compiles_valid_graph_and_hashes_prompt_contents` passes with the exact golden hash.
- `formal_agent_compile` passes, proving formal sample compilation and prompt hashing behavior still work.

- [ ] **Step 3: Run full repository gates**

Run:

```bash
cargo fmt --check
cargo test --all-targets --all-features -- --nocapture --test-threads=1
cargo clippy --all-targets --all-features -- -D warnings
cargo audit
cargo deny check
```

Expected: all commands pass with no new suppressions.

- [ ] **Step 4: Capture final dependency evidence**

Run:

```bash
cargo tree -i sha2@0.10.9
cargo tree -i sha2@0.11.0
cargo tree -i thiserror@1.0.69
cargo tree -i thiserror@2.0.18
```

Expected:

- `sha2@0.10.9`: no immediate root-crate child; residual SQLx core/sqlite/macro and Handlebars/Pest build paths are acceptable.
- `sha2@0.11.0`: immediate root-crate child exists; SQLx PostgreSQL may also appear.
- `thiserror@1.0.69`: no immediate root-crate child; CEL remains acceptable.
- `thiserror@2.0.18`: Handlebars and SQLx remain acceptable.

- [ ] **Step 5: Commit Task 4**

Run:

```bash
git diff -- docs/reviews/2026-07-11-dependency-governance-review.md
git add docs/reviews/2026-07-11-dependency-governance-review.md
git commit -m "docs: record direct macro crypto alignment"
```

Expected: one commit containing only the R6 execution note.

- [ ] **Step 6: Confirm final worktree state**

Run:

```bash
git status --short --branch
git log --oneline -5
```

Expected:

- `git status --short --branch` shows `main` ahead of `origin/main` with no uncommitted files.
- Recent commits include:
  - `docs: record direct macro crypto alignment`
  - `chore: remove unused direct thiserror`
  - `chore: upgrade direct sha2 to 0.11`
  - `test: freeze agent hash golden contract`
