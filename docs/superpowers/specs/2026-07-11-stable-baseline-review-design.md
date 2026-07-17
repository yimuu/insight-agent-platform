# Stable Baseline Review and Dependency Governance Design

> **Historical / superseded baseline:** findings and remediation history remain useful, but its graph/node DSL snapshot is not current. See [DSL Authoring Surface Redesign](./2026-07-17-dsl-authoring-surface-redesign.md) for the current authored contract.

**Date:** 2026-07-11
**Status:** Approved
**Code baseline:** `main@af414de`
**Scope:** Read-only whole-repository review followed by two independent remediation roadmaps

## 1. Context

The repository now has a generic Formal V1 Agent runtime, compiled DSL, extensible node and action registries, parallel fork/join scheduling, durable SQLite/PostgreSQL history, and live-only Attached SSE. Recent task and branch reviews verified the changed feature ranges, but they did not re-audit every unchanged subsystem as one stable product baseline.

The dependency gates also expose known maintenance signals. `cargo audit` reports the unmaintained transitive `paste` crate through `cel-interpreter`, while `cargo deny` reports license-policy noise and multiple transitive versions across CEL, JSON Schema, SQLx, crypto, randomness, and platform support chains. These signals deserve explicit governance, but mixing broad dependency upgrades into an architecture review would make causes and regressions difficult to isolate.

The chosen approach is review-first and read-only. It freezes one code baseline, produces evidence-backed findings, and then separates architecture/correctness remediation from dependency governance.

## 2. Goals

- Evaluate whether the current repository is a stable, extensible development baseline.
- Review correctness, concurrency, durability, API/DSL contracts, security, operations, and test coverage across the whole repository.
- Identify cross-module ownership or abstraction problems that task-scoped reviews could miss.
- Permit recommendations for HTTP, DSL, event, configuration, or Rust interface changes when they have clear long-term value.
- Explain every recommended breaking change with evidence, alternatives, benefits, and migration impact.
- Produce a separate dependency governance assessment covering security, maintenance, licensing, MSRV, upgrades, and duplicate versions.
- Turn findings into two ordered remediation roadmaps without changing production code or dependencies during the review.

## 3. Non-goals

- Modifying Rust source, tests, Agent YAML, runtime configuration, migrations, dependencies, or lockfiles.
- Fixing findings while the baseline is still being reviewed.
- Treating style preferences as architecture defects.
- Requiring all transitive dependency versions to converge.
- Suppressing dependency warnings merely to make command output quiet.
- Combining every recommendation into one implementation branch.
- Promising backward compatibility for interfaces that the review proves should change.

## 4. Baseline and Change Control

The audited code baseline is exactly `main@af414de`. Review specifications, plans, and reports may be committed after that SHA, but no production, test, configuration, migration, example, manifest, or lockfile change is permitted during the review.

If the code baseline changes before the reports are complete, the review stops and records the divergence. The reviewer must either restart from the new SHA or explicitly limit the report to `af414de`; findings from two code baselines must not be merged silently.

The current V1 contracts are not assumed immutable. A breaking recommendation is valid only when the report demonstrates that a compatible internal correction cannot adequately solve a correctness, security, scalability, or long-term architecture problem.

## 5. Review Domains

### 5.1 DSL and compiler

- Raw schema strictness and versioning.
- Graph construction, reachability, cycle and predecessor validation.
- Fork/join region compilation and reference boundaries.
- Template and CEL compilation timing and error taxonomy.
- Extension contracts for new node types.

### 5.2 Nodes and resources

- Separation between node compilation and execution.
- Registry ownership, duplicate handling, and type safety.
- Chat, action, condition, template, fork, join, and output semantics.
- Model/action capability validation, cancellation, secret handling, and network restrictions.

### 5.3 Runtime and scheduling

- Run state transitions and one-terminal guarantees.
- Sequential and parallel activation correctness.
- Global/per-Run capacity, timeout, cancellation, and branch draining.
- Task panic, infrastructure failure, and shutdown behavior.
- Attached versus Detached ownership and lifecycle.

### 5.4 Events and persistence

- Sequence allocation and persistence-before-broadcast.
- Journal capacity, batching, timeout, closure, and uncertain-commit recovery.
- EventHub live subscriber behavior and internal history reads.
- SQLite/PostgreSQL transactional parity, migrations, constraints, and startup reconciliation.
- Audit-data completeness and sanitization.

### 5.5 HTTP, configuration, and process lifecycle

- Route/auth boundaries, error mapping, headers, SSE lifecycle, and resource contracts.
- Strict YAML parsing, path resolution, secrets, positive durations, and capacity invariants.
- Startup construction order, health semantics, graceful shutdown, and failure visibility.
- Whether transport, runtime, persistence, and workflow policies live in the correct layer.

### 5.6 Security, operations, and testing

- Secret exposure, SSRF/network policy, unsafe input expansion, and denial-of-service boundaries.
- Logging, health, diagnosability, operational limits, and missing metrics.
- Unit/integration/contract test balance and meaningful failure-path coverage.
- SQLite/PostgreSQL and sequential/parallel parity.
- Documentation and examples matching actual contracts.

### 5.7 Dependencies

- Direct dependency necessity and feature selection.
- Security advisories, yanked versions, and maintenance status.
- License policy and source provenance.
- Rust toolchain/MSRV compatibility.
- Direct-upgrade and replacement paths.
- Transitive duplicate-version origins and their real cost.

## 6. Evidence Standard

Every confirmed finding must contain:

- A stable identifier and severity.
- File and line evidence, a concrete call chain, a failing/absent test, or command output.
- The affected contract or subsystem.
- Trigger conditions and user/operational impact.
- The reason current safeguards are insufficient.
- A focused recommendation and required verification.
- Dependencies on other findings.
- A `breaking: yes/no` classification.

Unverified concerns are labeled `Needs verification` and kept outside the confirmed issue count. They must state what evidence is missing and the exact check required. Reviewers must not convert uncertainty into a defect.

Strengths are also evidence-backed. The report should preserve sound boundaries and invariants, not only list problems.

## 7. Severity Model

- **P0 — Immediate:** exploitable security issue, data loss/corruption, deadlock, durable terminal divergence, or another release-blocking failure.
- **P1 — Near-term:** demonstrated correctness, stability, capacity, isolation, or extensibility risk that should enter the next remediation cycle.
- **P2 — Planned:** maintainability, ownership, observability, test-architecture, or dependency-aging problem with material medium-term cost.
- **P3 — Opportunistic:** documentation, naming, localized duplication, or low-risk optimization that should not displace higher-value work.

Severity is based on impact and likelihood, not implementation effort. A large refactor is not automatically P1, and a one-line defect is not automatically minor.

## 8. Breaking Change Decision Record

Every proposed breaking interface change must document:

1. The current interface and observed problem.
2. Why an internal or additive change is inadequate.
3. The proposed replacement contract.
4. Long-term architecture or correctness benefit.
5. Client, Agent, data, configuration, and operational migration impact.
6. Alternatives considered and rejected.
7. Tests and documentation required before adoption.

The review may recommend a breaking change, but it does not approve or implement it. Each such recommendation requires a later user decision and its own design/plan cycle.

## 9. Dependency Governance

Dependency findings are grouped into:

- **Security:** known advisories and exploitability in this repository.
- **Maintenance:** unmaintained, archived, or yanked crates.
- **Compatibility:** MSRV, edition, platform, and public API changes.
- **License/source:** policy alignment and provenance.
- **Direct hygiene:** unused dependencies, excessive features, and stale direct versions.
- **Transitive duplication:** duplicate versions with attributable compile-time, binary-size, security, or maintenance cost.

Duplicate versions are not defects by default. A convergence recommendation needs evidence of cost and a feasible upstream path. Adding `skip`, `allow`, or advisory ignores is classified as a policy decision, not as dependency cleanup.

CEL, JSON Schema, and SQLx are reviewed as separate upgrade domains because each affects a different semantic boundary:

- CEL affects `core.condition` compilation and evaluation.
- JSON Schema affects Agent input and resource validation.
- SQLx affects both database implementations, migrations, and runtime compatibility.

Current version and maintenance claims must be verified against primary sources such as crates.io, upstream repositories/releases, RustSec, and official project documentation, with an `as of 2026-07-11` date.

## 10. Review Method

### Phase 1: Contract map

Map README and approved design claims to source modules, public interfaces, migrations, and tests. Record contracts that lack a direct verification path.

### Phase 2: Domain review

Review each domain in Section 5 against the fixed baseline. Trace cross-layer ownership for Run creation, execution, event publication, persistence, cancellation, and shutdown.

### Phase 3: Dynamic verification

Run and capture:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo audit
cargo deny check
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' cargo test --test history_postgres -- --nocapture
```

A failed command becomes review evidence. It is not fixed during this milestone. Environment failures are distinguished from product failures and include the reproduction context.

### Phase 4: Dependency inventory

Capture direct and duplicate dependency trees, reverse paths for every advisory/maintenance warning, feature usage, and available upgrade/replacement paths. Separate changes that can be made independently from coupled ecosystem upgrades.

### Phase 5: Synthesis

Deduplicate findings, resolve severity conflicts, identify shared root causes, and order remediation by safety and dependency. No roadmap item may depend on an unstated prerequisite.

## 11. Deliverables

### 11.1 Stable baseline review

Create:

```text
docs/reviews/2026-07-11-stable-baseline-review.md
```

It contains:

- Executive assessment.
- Evidence-backed strengths.
- P0-P3 confirmed findings.
- Needs-verification items.
- Contract/test coverage map.
- Breaking-change decision records.
- Ordered architecture/correctness remediation roadmap.

### 11.2 Dependency governance review

Create:

```text
docs/reviews/2026-07-11-dependency-governance-review.md
```

It contains:

- Dependency inventory and policy baseline.
- Advisory/maintenance/license/MSRV findings.
- Duplicate-version root-cause groups.
- CEL, JSON Schema, and SQLx upgrade assessments.
- Explicitly accepted dependencies and rationale.
- Ordered dependency remediation roadmap.

The reports may link to each other but remain independently actionable. Neither report contains implementation commits or claims that a recommendation has already been approved.

## 12. Acceptance Criteria

The review milestone is complete when:

1. The audited source SHA is recorded and unchanged throughout review.
2. Every domain in Section 5 has explicit coverage in the stable baseline report.
3. Every confirmed issue follows the evidence schema and P0-P3 severity model.
4. Speculative concerns are separated as Needs verification.
5. Every breaking recommendation includes the full decision record.
6. Dynamic verification results and environmental qualifications are recorded accurately.
7. Dependency claims are current as of 2026-07-11 and cite primary sources.
8. Duplicate dependencies are judged by demonstrated cost rather than a zero-duplicate target.
9. Architecture/correctness and dependency remediation are two separate ordered roadmaps.
10. No production, test, configuration, migration, example, manifest, or lockfile change is included.
11. Both reports pass incomplete-section, contradiction, scope, and evidence self-review.
12. The user can select roadmap items for later design and implementation without redoing the review.
