# Stable Baseline Review Implementation Plan

> **Historical / superseded for DSL references:** this plan audits the removed graph/node baseline. Current authored syntax is defined by [DSL Authoring Surface Redesign](../specs/2026-07-17-dsl-authoring-surface-redesign.md).

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce an evidence-backed whole-repository stability review and an independent dependency-governance review for the fixed `main@af414de` code baseline without changing product code or dependencies.

**Architecture:** Domain review tasks inspect one frozen source baseline and write normalized evidence into ignored `.superpowers/review/` files. A verification task records dynamic gates without fixing failures. A synthesis task creates exactly two tracked reports under `docs/reviews/`, followed by independent evidence and quality review.

**Tech Stack:** Rust 1.94.1, Cargo, Tokio/Axum, SQLx SQLite/PostgreSQL, RustSec `cargo audit`, Embark `cargo deny`, Git, Markdown

## Global Constraints

- Audit code baseline exactly `main@af414de`; later review documents may be committed, but product/test/configuration/migration/example/manifest/lockfile content must remain identical to that baseline.
- The review is read-only with respect to Rust source, tests, Agent YAML, runtime/model configuration, migrations, `Cargo.toml`, `Cargo.lock`, `deny.toml`, and `rust-toolchain.toml`.
- Cover every domain from the approved design: DSL/compiler, nodes/resources, runtime/scheduling, events/persistence, HTTP/config/process lifecycle, security/operations/testing, and dependencies.
- Confirmed findings require file/line, call-chain, test, or command evidence; uncertainty is recorded only under `Needs verification`.
- Use severity P0 Immediate, P1 Near-term, P2 Planned, and P3 Opportunistic based on impact and likelihood.
- Every confirmed finding states affected contract, trigger, impact, insufficient safeguards, focused recommendation, verification, dependencies, and `breaking: yes/no`.
- Every breaking recommendation includes current interface, problem, inadequacy of compatible alternatives, replacement, long-term benefit, migration impact, rejected alternatives, and required tests/docs.
- Duplicate dependency versions are not defects by default; recommend convergence only with demonstrated cost and a feasible path.
- Treat policy suppression (`skip`, `allow`, advisory ignore) as a policy decision, not dependency cleanup.
- Verify current dependency claims against primary sources and date them `as of 2026-07-11`.
- Produce exactly two tracked deliverables: `docs/reviews/2026-07-11-stable-baseline-review.md` and `docs/reviews/2026-07-11-dependency-governance-review.md`.
- Architecture/correctness and dependency remediation remain separate ordered roadmaps; neither report approves or implements a recommendation.
- Intermediate evidence lives only under ignored `.superpowers/review/` and is not committed.

## Evidence Artifact Map

- `.superpowers/review/01-baseline-contract-map.md`: baseline identity, change-control check, public/internal contract-to-code/test map.
- `.superpowers/review/02-dsl-nodes.md`: DSL, compiler, graph, node, and extension findings.
- `.superpowers/review/03-runtime.md`: Run lifecycle, scheduler, concurrency, cancellation, timeout, and shutdown findings.
- `.superpowers/review/04-events-history.md`: protocol, journal, EventHub, repository, migration, and backend-parity findings.
- `.superpowers/review/05-api-security-ops.md`: HTTP/SSE, auth, configuration, resources, secrets, network policy, observability, docs, and test-system findings.
- `.superpowers/review/06-dependencies.md`: dependency inventory, primary-source research, warnings, domain upgrade assessments, and dependency roadmap candidates.
- `.superpowers/review/07-verification.md`: exact dynamic command outcomes and environment qualifications.
- `docs/reviews/2026-07-11-stable-baseline-review.md`: synthesized stable-baseline assessment and architecture/correctness roadmap.
- `docs/reviews/2026-07-11-dependency-governance-review.md`: synthesized dependency assessment and dependency roadmap.

## Evidence File Contract

Every intermediate Markdown evidence file must contain these exact headings:

```markdown
# Domain Evidence

## Baseline
## Coverage
## Evidence-backed strengths
## Confirmed findings
## Needs verification
## Commands and checks
## Domain conclusion
```

If a category has no entries, write `None.`; do not omit the heading. Each confirmed issue uses an ID shaped as `BASE-P1-001` or `DEP-P2-001` and includes all fields required by Global Constraints. Evidence validation replaces TDD because this milestone deliberately changes no executable behavior: the artifact-heading check is RED before file creation and GREEN after evidence is written.

---

### Task 1: Freeze the baseline and build the contract map

**Files:**
- Read: `README.md`
- Read: `docs/formal-v1-breaking-changes.md`
- Read: `docs/superpowers/specs/2026-07-10-general-agent-runtime-v1-rewrite-design.md`
- Read: `docs/superpowers/specs/2026-07-10-parallel-fork-join-scheduler-design.md`
- Read: `docs/superpowers/specs/2026-07-11-live-only-sse-design.md`
- Read: `src/lib.rs`
- Read: `src/main.rs`
- Read: `src/api/formal/routes.rs`
- Read: `src/dsl/raw.rs`
- Read: `src/events/protocol.rs`
- Read: `src/config.rs`
- Read: `src/history/repository.rs`
- Read: `migrations/formal_v1/sqlite/202607100001_formal_v1.sql`
- Read: `migrations/formal_v1/postgres/202607100001_formal_v1.sql`
- Read: `tests/api.rs`
- Read: `tests/dsl_raw.rs`
- Read: `tests/formal_protocol.rs`
- Read: `tests/platform_config_v1.rs`
- Read: `tests/history_sqlite_v1.rs`
- Read: `tests/history_postgres.rs`
- Create ignored: `.superpowers/review/01-baseline-contract-map.md`

**Interfaces:**
- Consumes: approved design baseline `af414de` and repository contracts
- Produces: one normalized baseline/contract evidence file consumed by Tasks 2-8

- [ ] **Step 1: Prove the evidence artifact is absent or incomplete**

Run:

```bash
test -f .superpowers/review/01-baseline-contract-map.md \
  && rg -q '^## Domain conclusion$' .superpowers/review/01-baseline-contract-map.md
```

Expected: FAIL because the evidence file does not yet exist.

- [ ] **Step 2: Record and verify the immutable code baseline**

Run:

```bash
git rev-parse af414de^{commit}
git branch --show-current
git status --short
if git diff --name-only af414de..HEAD \
  | rg -qv '^docs/(superpowers|reviews)/'; then
  echo 'non-review content diverged from af414de' >&2
  exit 1
fi
```

Expected: the SHA resolves; branch is named; status contains no product changes; the divergence guard exits 0 because changes since `af414de` are review specifications/plans only.

- [ ] **Step 3: Map documented contracts to implementation and tests**

Inspect every listed file and use `apply_patch` to create `.superpowers/review/01-baseline-contract-map.md` with the Evidence File Contract. Under `Coverage`, include a table with these exact rows:

```text
HTTP routes and auth
Attached/Detached lifecycle
SSE event and terminal contract
Run resource and cancellation
DSL raw schema and version
Compiled graph and fork/join contract
Node/action/model extension contract
Runtime state and scheduling
Event sequencing and journal
SQLite/PostgreSQL repository
Platform/model configuration
Startup, health, and shutdown
Checked-in Agent examples
```

Every row records documentation source, production symbol/path, direct test path, and any uncovered claim. Record mismatches as finding candidates without assigning final severity.

- [ ] **Step 4: Validate baseline evidence structure and coverage**

Run:

```bash
for heading in Baseline Coverage 'Evidence-backed strengths' \
  'Confirmed findings' 'Needs verification' 'Commands and checks' \
  'Domain conclusion'; do
  rg -q "^## ${heading}$" .superpowers/review/01-baseline-contract-map.md
done
for row in 'HTTP routes and auth' 'Attached/Detached lifecycle' \
  'DSL raw schema and version' 'Runtime state and scheduling' \
  'Event sequencing and journal' 'SQLite/PostgreSQL repository' \
  'Startup, health, and shutdown' 'Checked-in Agent examples'; do
  rg -q "$row" .superpowers/review/01-baseline-contract-map.md
done
```

Expected: PASS; every required heading and contract group exists.

- [ ] **Step 5: Confirm this task changed no tracked content**

Run:

```bash
git status --short
```

Expected: no tracked or untracked output because `.superpowers/review/` is ignored.

---

### Task 2: Review DSL, graph compilation, nodes, and extension boundaries

**Files:**
- Read: `src/dsl/raw.rs`
- Read: `src/dsl/compiled.rs`
- Read: `src/dsl/graph.rs`
- Read: `src/dsl/plan.rs`
- Read: `src/dsl/compiler.rs`
- Read: `src/catalog.rs`
- Read: `src/nodes/registry.rs`
- Read: `src/nodes/action.rs`
- Read: `src/nodes/chat.rs`
- Read: `src/nodes/condition.rs`
- Read: `src/nodes/template.rs`
- Read: `src/nodes/fork.rs`
- Read: `src/nodes/join.rs`
- Read: `src/nodes/output.rs`
- Read: `tests/dsl_raw.rs`
- Read: `tests/dsl_compiler.rs`
- Read: `tests/dsl_parallel.rs`
- Read: `tests/fork_join_nodes.rs`
- Read: `tests/core_chat_action.rs`
- Read: `tests/core_template_condition.rs`
- Read: `tests/core_output.rs`
- Read: `tests/node_extensions.rs`
- Read: `tests/formal_agent_compile.rs`
- Read: `tests/repository_agents_v1.rs`
- Create ignored: `.superpowers/review/02-dsl-nodes.md`

**Interfaces:**
- Consumes: Task 1 contract map and exact compiler/node implementation
- Produces: normalized DSL/node findings and breaking-change candidates for the stable report

- [ ] **Step 1: Prove the domain evidence artifact is absent**

Run:

```bash
test -f .superpowers/review/02-dsl-nodes.md \
  && rg -q '^## Domain conclusion$' .superpowers/review/02-dsl-nodes.md
```

Expected: FAIL before domain evidence is created.

- [ ] **Step 2: Trace compile-time ownership and graph invariants**

Read the listed DSL/compiler files in data-flow order:

```text
RawAgent
-> AgentCompiler / CompileContext
-> graph validation
-> ExecutionPlan / ParallelRegion
-> CompiledAgent / CompiledNode
-> NodeExecutorRegistry at runtime
```

For each transition, record accepted inputs, frozen outputs, error codes, trust boundary, and direct tests. Explicitly inspect cycles, unreachable nodes, predecessor references, fork ownership, join barriers, branch isolation, nested-fork rejection, prompt path containment, template compilation, and CEL compilation timing.

- [ ] **Step 3: Trace each built-in node and extension interface**

For `core.action`, `core.chat`, `core.condition`, `core.template`, `core.fork`, `core.join`, and `core.output`, record compile contract, runtime input/output, cancellation behavior, event emission, and invalid states. Check custom `NodeType`, `NodeExecutor`, Action, and model registration for duplicate handling, downcast/type mismatch behavior, and core-code modification requirements.

- [ ] **Step 4: Run focused DSL/node verification**

Run:

```bash
cargo test --test dsl_raw --test dsl_compiler --test dsl_parallel -- --nocapture
cargo test --test fork_join_nodes --test core_chat_action \
  --test core_template_condition --test core_output -- --nocapture
cargo test --test node_extensions --test formal_agent_compile \
  --test repository_agents_v1 -- --nocapture
```

Expected: record exact pass/fail counts and output. A failure becomes evidence; do not edit code or tests.

- [ ] **Step 5: Write and validate DSL/node evidence**

Create `.superpowers/review/02-dsl-nodes.md` with the Evidence File Contract. Confirmed issues use `BASE-P0/P1/P2/P3` IDs and all required fields. Breaking candidates add the full decision record from the design. If no issue exists for a reviewed area, record an evidence-backed strength or `None.` rather than inventing a finding.

Run:

```bash
for heading in Baseline Coverage 'Evidence-backed strengths' \
  'Confirmed findings' 'Needs verification' 'Commands and checks' \
  'Domain conclusion'; do
  rg -q "^## ${heading}$" .superpowers/review/02-dsl-nodes.md
done
rg -q 'RawAgent' .superpowers/review/02-dsl-nodes.md
rg -q 'core.condition' .superpowers/review/02-dsl-nodes.md
rg -q 'core.join' .superpowers/review/02-dsl-nodes.md
rg -q 'NodeExecutorRegistry' .superpowers/review/02-dsl-nodes.md
```

Expected: PASS; the full DSL/node surface and extension boundary are represented.

---

### Task 3: Review Run lifecycle, scheduler, concurrency, cancellation, and shutdown

**Files:**
- Read: `src/runtime/state.rs`
- Read: `src/runtime/control.rs`
- Read: `src/runtime/context.rs`
- Read: `src/runtime/attachment.rs`
- Read: `src/runtime/execution.rs`
- Read: `src/runtime/scheduler.rs`
- Read: `src/runtime/coordinator.rs`
- Read: `src/runtime/service.rs`
- Read: `src/main.rs`
- Read: `tests/run_scheduler.rs`
- Read: `tests/run_coordinator.rs`
- Read: `tests/run_service.rs`
- Create ignored: `.superpowers/review/03-runtime.md`

**Interfaces:**
- Consumes: compiled execution plan and EventHub/repository interfaces
- Produces: normalized runtime/concurrency findings, ordering evidence, and remediation dependencies

- [ ] **Step 1: Prove the runtime evidence artifact is absent**

Run:

```bash
test -f .superpowers/review/03-runtime.md \
  && rg -q '^## Domain conclusion$' .superpowers/review/03-runtime.md
```

Expected: FAIL before evidence creation.

- [ ] **Step 2: Trace the complete Run lifecycle and ownership graph**

Trace these exact flows and record lock/task/permit ownership at every transition:

```text
create Attached -> prepare -> subscribe -> launch -> execute -> terminal -> lease drop
create Detached -> prepare -> launch -> poll/cancel -> terminal
shutdown -> stop active Attached/Detached with distinct reasons -> drain -> process exit
```

Inspect state-transition single-winner behavior, active-map mutation, semaphore permit lifetime, node/global/per-Run limits, stop precedence, terminal/drop races, and service health degradation.

- [ ] **Step 3: Review sequential and parallel scheduler invariants**

Inspect activation queues, duplicate/missing activation handling, branch task creation, per-Run/global permit acquisition order, all-settled result construction, cancellation and infrastructure propagation, panic draining, join activation, and post-join context. Search targeted risk sites:

```bash
rg -n 'tokio::spawn|JoinSet|select!|Semaphore|OwnedSemaphorePermit|Mutex|RwLock|\.lock\(\)\.await|timeout\(|sleep\(' src/runtime
rg -n 'unwrap\(|expect\(|panic!|unreachable!' src/runtime
```

Expected: record each match's safety rationale or finding candidate; search matches are not defects by themselves.

- [ ] **Step 4: Run focused runtime verification**

Run:

```bash
cargo test --test run_scheduler --test run_coordinator --test run_service -- --nocapture
```

Expected: record exact results, including intentional panic-test output separately from failures.

- [ ] **Step 5: Write and validate runtime evidence**

Create `.superpowers/review/03-runtime.md` with the Evidence File Contract and explicit coverage of Run state, Attached/Detached, sequential scheduling, parallel scheduling, capacity, timeout, cancellation, panic/infrastructure failure, health, startup reconciliation, and shutdown.

Run:

```bash
for term in 'Attached' 'Detached' 'sequential' 'parallel' 'capacity' \
  'timeout' 'cancellation' 'panic' 'health' 'shutdown'; do
  rg -qi "$term" .superpowers/review/03-runtime.md
done
rg -q '^## Domain conclusion$' .superpowers/review/03-runtime.md
```

Expected: PASS with all runtime concerns represented.

---

### Task 4: Review event sequencing, journal durability, repositories, and migrations

**Files:**
- Read: `src/events/protocol.rs`
- Read: `src/events/journal.rs`
- Read: `src/events/hub.rs`
- Read: `src/history/types.rs`
- Read: `src/history/repository.rs`
- Read: `src/history/sqlite.rs`
- Read: `src/history/postgres.rs`
- Read: `migrations/formal_v1/sqlite/202607100001_formal_v1.sql`
- Read: `migrations/formal_v1/postgres/202607100001_formal_v1.sql`
- Read: `tests/formal_protocol.rs`
- Read: `tests/event_hub.rs`
- Read: `tests/history_sqlite_v1.rs`
- Read: `tests/history_postgres.rs`
- Read: `tests/migration_layout.rs`
- Create ignored: `.superpowers/review/04-events-history.md`

**Interfaces:**
- Consumes: coordinator event calls and repository contracts
- Produces: normalized durability/parity findings and database remediation prerequisites

- [ ] **Step 1: Prove the events/history evidence artifact is absent**

Run:

```bash
test -f .superpowers/review/04-events-history.md \
  && rg -q '^## Domain conclusion$' .superpowers/review/04-events-history.md
```

Expected: FAIL before evidence creation.

- [ ] **Step 2: Trace persistence-before-broadcast and recovery paths**

Trace nonterminal publish, terminal publish, terminal conflict, journal failure, timeout, close-and-recover, and `reconcile_durable_through`. Record sequence ownership, lock scope, queue backpressure, operation timeout, repository call, transaction boundary, broadcast point, state removal, and health impact.

- [ ] **Step 3: Compare repository and migration contracts**

Build a field/constraint/transaction table covering `runs`, `run_events`, and `node_outputs` for SQLite and PostgreSQL. Verify uniqueness, foreign keys/cascades, timestamps, JSON serialization, attachment/status validation, terminal compare-and-set, startup interruption, uncertain commit recovery, and pagination/internal history queries.

Run static comparisons:

```bash
rg -n 'CREATE TABLE|UNIQUE|FOREIGN KEY|CHECK|ON DELETE|INDEX' migrations/formal_v1
rg -n 'BEGIN|transaction|commit|rollback|finish_run|recover_run|list_events_after' \
  src/history src/events
```

Expected: every match is mapped to a contract or finding candidate.

- [ ] **Step 4: Run focused persistence verification**

Run:

```bash
cargo test --test formal_protocol --test event_hub \
  --test history_sqlite_v1 --test migration_layout -- --nocapture
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' \
  cargo test --test history_postgres -- --nocapture
```

Expected: record exact results. If PostgreSQL is unavailable, record the environment error and exact setup required; do not silently count a skipped test as parity evidence.

- [ ] **Step 5: Write and validate event/history evidence**

Create `.superpowers/review/04-events-history.md` with the Evidence File Contract and explicit SQLite/PostgreSQL parity assessment.

Run:

```bash
for term in 'sequence' 'persistence-before-broadcast' 'journal' \
  'terminal conflict' 'uncertain commit' 'SQLite' 'PostgreSQL' \
  'migration' 'startup reconciliation'; do
  rg -qi "$term" .superpowers/review/04-events-history.md
done
rg -q '^## Domain conclusion$' .superpowers/review/04-events-history.md
```

Expected: PASS with durability and backend parity fully represented.

---

### Task 5: Review HTTP, configuration, resources, security, operations, tests, and documentation

**Files:**
- Read: `src/api/formal/auth.rs`
- Read: `src/api/formal/response.rs`
- Read: `src/api/formal/routes.rs`
- Read: `src/api/formal/sse.rs`
- Read: `src/config.rs`
- Read: `src/resources/actions.rs`
- Read: `src/resources/builtin_actions.rs`
- Read: `src/resources/config.rs`
- Read: `src/resources/models.rs`
- Read: `src/resources/openai_chat.rs`
- Read: `src/catalog.rs`
- Read: `src/main.rs`
- Read: `config/platform.yaml`
- Read: `config/models.yaml`
- Read: `agents/code_node_demo/agent.yaml`
- Read: `agents/medical_report_interpreter/agent.yaml`
- Read: `agents/researcher/agent.yaml`
- Read: `agents/parallel_researcher/agent.yaml`
- Read: `README.md`
- Read: `docs/formal-v1-breaking-changes.md`
- Read: `tests/api.rs`
- Read: `tests/platform_config_v1.rs`
- Read: `tests/formal_resources.rs`
- Read: `tests/model_resources_v1.rs`
- Read: `tests/resource_registries.rs`
- Create ignored: `.superpowers/review/05-api-security-ops.md`

**Interfaces:**
- Consumes: runtime/service, repository, registry, and model/action interfaces
- Produces: normalized transport/config/security/operations/test findings and documentation mismatches

- [ ] **Step 1: Prove the API/security/operations evidence artifact is absent**

Run:

```bash
test -f .superpowers/review/05-api-security-ops.md \
  && rg -q '^## Domain conclusion$' .superpowers/review/05-api-security-ops.md
```

Expected: FAIL before evidence creation.

- [ ] **Step 2: Trace route, auth, error, SSE, and Run-resource contracts**

Inspect router layering and every endpoint for auth coverage, request validation, response/status/error mapping, request/run IDs, secret/input omission, Attached subscribe-before-launch, terminal EOF, keepalive ownership, live subscriber lag, Detached polling, and idempotent cancellation. Cross-check each behavior against README, breaking-change docs, and `tests/api.rs`.

- [ ] **Step 3: Review strict configuration and process lifecycle**

Trace platform/model YAML deserialization through resolution and main construction. Record unknown-field policy, secret sources/redaction, path containment, duration/capacity checks, resource enablement, history backend selection, health degradation, startup reconciliation, signal handling, shutdown deadlines, and server/runtime drain order.

- [ ] **Step 4: Review resource and security boundaries**

Inspect model/action registration, JSON Schema validation, OpenAI request/SSE decoding, response-body limits, TLS configuration, HTTP allowlist/redirect/DNS behavior, secret formatting/logging, cancellation, and error sanitization. Run targeted searches and judge each match in context:

```bash
rg -n 'api_key|token|authorization|database_url|SecretString|expose\(' src config tests
rg -n 'http://|https://|redirect|allowlist|resolve|dns|localhost|127\.0\.0\.1|max_bytes' \
  src/resources tests/formal_resources.rs config
rg -n 'tracing::|health|shutdown|signal|timeout\(' src/main.rs src/api src/resources
rg -n 'unwrap\(|expect\(|panic!|unreachable!' src/api src/config.rs src/resources src/main.rs
```

Expected: record a safety rationale, finding candidate, or Needs-verification item for every concrete risk; keyword matches alone are not findings.

- [ ] **Step 5: Assess test architecture, examples, and documentation parity**

Map production modules to direct unit/integration tests. Identify untested failure paths, backend/parallel parity gaps, assertions that only mirror mocks, and oversized test files that obscure contracts. Compile checked-in Agents through existing repository tests and verify README examples match enabled resources and current routes.

Run:

```bash
cargo test --test api --test platform_config_v1 -- --nocapture
cargo test --test formal_resources --test model_resources_v1 \
  --test resource_registries -- --nocapture
cargo test --test repository_agents_v1 --test formal_agent_compile -- --nocapture
```

Expected: record exact outcomes without editing code, examples, or tests.

- [ ] **Step 6: Write and validate API/security/operations evidence**

Create `.superpowers/review/05-api-security-ops.md` with the Evidence File Contract and explicit coverage of HTTP, SSE, auth, errors, platform config, model config, resources, secrets, network policy, health, startup, shutdown, observability, test architecture, examples, and docs.

Run:

```bash
for term in 'HTTP' 'SSE' 'auth' 'error' 'platform config' 'model config' \
  'secret' 'network' 'health' 'startup' 'shutdown' 'observability' \
  'test architecture' 'documentation'; do
  rg -qi "$term" .superpowers/review/05-api-security-ops.md
done
rg -q '^## Domain conclusion$' .superpowers/review/05-api-security-ops.md
```

Expected: PASS with all boundary and operational topics represented.

---

### Task 6: Build the dependency governance evidence and upgrade-domain assessments

**Files:**
- Read: `Cargo.toml`
- Read: `Cargo.lock`
- Read: `deny.toml`
- Read: `rust-toolchain.toml`
- Read: `src/nodes/condition.rs`
- Read: `src/config.rs`
- Read: `src/history/sqlite.rs`
- Read: `src/history/postgres.rs`
- Read: `src/resources/openai_chat.rs`
- Read: `tests/core_template_condition.rs`
- Read: `tests/dsl_compiler.rs`
- Read: `tests/platform_config_v1.rs`
- Read: `tests/history_sqlite_v1.rs`
- Read: `tests/history_postgres.rs`
- Create ignored: `.superpowers/review/06-dependencies.md`

**Interfaces:**
- Consumes: direct dependency declarations, locked graph, policy, and semantic usage sites
- Produces: dated dependency facts, finding candidates, accepted-risk candidates, and independent roadmap groups

- [ ] **Step 1: Prove the dependency evidence artifact is absent**

Run:

```bash
test -f .superpowers/review/06-dependencies.md \
  && rg -q '^## Domain conclusion$' .superpowers/review/06-dependencies.md
```

Expected: FAIL before evidence creation.

- [ ] **Step 2: Inventory direct, reverse, duplicate, and feature dependency graphs**

Run:

```bash
cargo tree --locked --depth 1
cargo tree --locked --duplicates
cargo tree --locked -i paste
cargo tree --locked -i cel-interpreter
cargo tree --locked -i jsonschema
cargo tree --locked -i sqlx
cargo tree --locked -e features -i reqwest
cargo tree --locked -e features -i sqlx
```

Expected: capture exact direct versions/features, duplicate groups, and reverse paths. Do not assign a duplicate finding until cost and convergence feasibility are established.

- [ ] **Step 3: Run security, maintenance, license, source, and policy checks**

Run:

```bash
cargo audit
cargo deny check
```

Expected: record exit status and every advisory/warning separately. Distinguish vulnerability, unmaintained, yanked, license, source, and configured duplicate-version policy. Existing allowed warnings remain review evidence; do not edit manifests or policies.

- [ ] **Step 4: Verify current releases and maintenance using primary sources**

For every direct dependency, record locked/resolved version, newest stable release as of 2026-07-11, release date, upstream maintenance signal, MSRV when published, and likely breaking range. Use crates.io and official upstream repositories/releases/documentation. Use RustSec for advisories and link the exact advisory.

Run registry metadata checks for the complete direct set:

```bash
for crate in async-trait axum bytes cel-interpreter chrono chrono-tz \
  dotenvy futures handlebars humantime jsonschema regex reqwest serde \
  serde_json serde_yaml sha2 sqlx thiserror tokio tokio-stream tokio-util \
  tracing tracing-subscriber uuid tower tempfile; do
  cargo info "$crate"
done
```

Expected: each crate resolves through crates.io and reports its current metadata/repository link. Follow each repository link to the official release/changelog/MSRV source before recording maintenance or compatibility claims.

At minimum, perform detailed assessments for:

```text
cel-interpreter and paste
jsonschema and its regex/randomness chain
sqlx with sqlite/postgres/macros/migrate features
axum/tokio/tower
reqwest/rustls
serde_yaml deprecation status
sha2 and thiserror direct-version alignment
```

Do not rely on search-result snippets or third-party version aggregators. Every current-version claim includes a direct primary-source URL and `as of 2026-07-11`.

- [ ] **Step 5: Assess semantic upgrade blast radius**

For CEL, JSON Schema, and SQLx, map each possible upgrade/replacement to the exact production APIs and test suites it can change:

```text
CEL -> condition compile/evaluate, value conversion, error codes, DSL tests
JSON Schema -> Agent input/resource validation, draft/default behavior, validation errors
SQLx -> runtime features, macros, migrations, transaction semantics, both repositories, MSRV
```

Separate independently safe direct updates from coupled ecosystem upgrades. Treat a policy ignore as an accepted-risk proposal, not a technical remediation.

- [ ] **Step 6: Write and validate dependency evidence**

Create `.superpowers/review/06-dependencies.md` with the Evidence File Contract plus subsections for inventory, security, maintenance, compatibility/MSRV, license/source, direct hygiene, duplicate groups, CEL, JSON Schema, SQLx, accepted-risk candidates, and roadmap candidates.

Run:

```bash
for term in 'as of 2026-07-11' 'RustSec' 'MSRV' 'license' \
  'duplicate' 'CEL' 'JSON Schema' 'SQLx' 'accepted risk' \
  'roadmap'; do
  rg -qi "$term" .superpowers/review/06-dependencies.md
done
rg -q 'https://' .superpowers/review/06-dependencies.md
rg -q '^## Domain conclusion$' .superpowers/review/06-dependencies.md
```

Expected: PASS with dated, primary-source-backed dependency evidence.

---

### Task 7: Run complete dynamic verification and record environment-qualified evidence

**Files:**
- Read: all existing `tests/*.rs`
- Read: `docker-compose.postgres.yml`
- Create ignored: `.superpowers/review/07-verification.md`

**Interfaces:**
- Consumes: frozen baseline plus domain finding candidates
- Produces: authoritative dynamic gate evidence consumed by both final reports

- [ ] **Step 1: Prove the verification evidence artifact is absent**

Run:

```bash
test -f .superpowers/review/07-verification.md \
  && rg -q '^## Domain conclusion$' .superpowers/review/07-verification.md
```

Expected: FAIL before evidence creation.

- [ ] **Step 2: Re-run the baseline divergence guard**

Run:

```bash
if git diff --name-only af414de..HEAD \
  | rg -qv '^docs/(superpowers|reviews)/'; then
  echo 'non-review content diverged from af414de' >&2
  exit 1
fi
git status --short
```

Expected: no code/config/test/dependency divergence. Report any unexpected status before running gates.

- [ ] **Step 3: Run formatting, lint, and full test gates**

Run each command separately and capture exit code, totals, warnings, and intentional panic-test noise:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

Expected: do not assume success. Record actual output; any failure becomes a confirmed or Needs-verification candidate after root-cause inspection, not an automatic code change.

- [ ] **Step 4: Run dependency policy gates**

Run:

```bash
cargo audit
cargo deny check
```

Expected: record successful/failing exit status and all warnings. Link each warning to Task 6 evidence.

- [ ] **Step 5: Run a real PostgreSQL contract gate**

Run:

```bash
docker compose -f docker-compose.postgres.yml up -d
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' \
  cargo test --test history_postgres -- --nocapture
```

Expected: one real PostgreSQL contract test executes and passes or produces a concrete product/environment failure. If port 5433 is already owned, identify the container and compare image, health, port, credentials, and Compose config before deciding whether it is an equivalent test service; do not stop an unrelated container.

- [ ] **Step 6: Write and validate verification evidence**

Create `.superpowers/review/07-verification.md` with the Evidence File Contract. Under `Commands and checks`, give each command its own subsection containing command, timestamp, exit code, pass/fail count, warnings, environment, and interpretation.

Run:

```bash
for term in 'cargo fmt' 'cargo clippy' 'cargo test --all-targets' \
  'cargo audit' 'cargo deny check' 'history_postgres' 'exit code' \
  'environment'; do
  rg -qi "$term" .superpowers/review/07-verification.md
done
rg -q '^## Domain conclusion$' .superpowers/review/07-verification.md
```

Expected: PASS with every dynamic gate represented honestly.

---

### Task 8: Synthesize and commit the two final review reports

**Files:**
- Read ignored: `.superpowers/review/01-baseline-contract-map.md`
- Read ignored: `.superpowers/review/02-dsl-nodes.md`
- Read ignored: `.superpowers/review/03-runtime.md`
- Read ignored: `.superpowers/review/04-events-history.md`
- Read ignored: `.superpowers/review/05-api-security-ops.md`
- Read ignored: `.superpowers/review/06-dependencies.md`
- Read ignored: `.superpowers/review/07-verification.md`
- Create: `docs/reviews/2026-07-11-stable-baseline-review.md`
- Create: `docs/reviews/2026-07-11-dependency-governance-review.md`

**Interfaces:**
- Consumes: all normalized domain and verification evidence
- Produces: the only two tracked review deliverables, with distinct ordered roadmaps

- [ ] **Step 1: Prove both final reports are absent or structurally incomplete**

Run:

```bash
test -f docs/reviews/2026-07-11-stable-baseline-review.md \
  && rg -q '^## Architecture and correctness remediation roadmap$' \
    docs/reviews/2026-07-11-stable-baseline-review.md
test -f docs/reviews/2026-07-11-dependency-governance-review.md \
  && rg -q '^## Dependency remediation roadmap$' \
    docs/reviews/2026-07-11-dependency-governance-review.md
```

Expected: one or both commands FAIL because final reports do not yet exist.

- [ ] **Step 2: Reconcile domain findings before writing reports**

Read every evidence file. Merge duplicate findings only when they share one root cause; preserve multiple impacts under the merged item. Resolve severity using impact/likelihood. Do not promote Needs-verification items. Assign stable IDs in deterministic severity/domain order and list dependencies explicitly.

Cross-check every confirmed finding against the frozen source and at least one evidence file. If the evidence no longer supports it, remove it or move it to Needs verification.

- [ ] **Step 3: Create the stable baseline review**

Use `apply_patch` to create `docs/reviews/2026-07-11-stable-baseline-review.md` with these exact headings:

```markdown
# Stable Baseline Review

## Audited baseline
## Executive assessment
## Evidence-backed strengths
## Confirmed findings
### P0 — Immediate
### P1 — Near-term
### P2 — Planned
### P3 — Opportunistic
## Needs verification
## Contract and test coverage map
## Breaking change decision records
## Architecture and correctness remediation roadmap
## Dynamic verification results
## Review limitations
```

The coverage map explicitly covers every design domain. If a severity has no finding, write `None.`. The roadmap groups fixes into independently reviewable milestones, orders prerequisites, lists required tests, and does not claim user approval.

- [ ] **Step 4: Create the dependency governance review**

Use `apply_patch` to create `docs/reviews/2026-07-11-dependency-governance-review.md` with these exact headings:

```markdown
# Dependency Governance Review

## Audited baseline and date
## Executive assessment
## Inventory and policy baseline
## Security findings
## Maintenance findings
## Compatibility and MSRV findings
## License and source findings
## Direct dependency hygiene
## Duplicate-version root causes
## CEL upgrade assessment
## JSON Schema upgrade assessment
## SQLx upgrade assessment
## Explicitly accepted dependencies
## Needs verification
## Dependency remediation roadmap
## Primary sources
## Review limitations
```

Each current-version/maintenance claim is dated and cited. The report distinguishes warning suppression, accepted risk, safe direct updates, coupled upgrades, and replacement projects.

- [ ] **Step 5: Validate report completeness and finding schema**

Run:

```bash
for heading in 'Audited baseline' 'Executive assessment' \
  'Evidence-backed strengths' 'Confirmed findings' 'Needs verification' \
  'Contract and test coverage map' 'Breaking change decision records' \
  'Architecture and correctness remediation roadmap' \
  'Dynamic verification results' 'Review limitations'; do
  rg -q "^## ${heading}$" docs/reviews/2026-07-11-stable-baseline-review.md
done
for heading in 'Audited baseline and date' 'Inventory and policy baseline' \
  'Security findings' 'Maintenance findings' \
  'Compatibility and MSRV findings' 'License and source findings' \
  'Duplicate-version root causes' 'CEL upgrade assessment' \
  'JSON Schema upgrade assessment' 'SQLx upgrade assessment' \
  'Explicitly accepted dependencies' 'Dependency remediation roadmap' \
  'Primary sources' 'Review limitations'; do
  rg -q "^## ${heading}$" docs/reviews/2026-07-11-dependency-governance-review.md
done
rg -n 'T[B]D|T[O]DO|F[I]XME|place[h]older|implement la[t]er|fill [i]n' \
  docs/reviews && exit 1 || true
git diff --check
```

Expected: every required heading exists, no incomplete marker exists, and Markdown has no whitespace errors.

- [ ] **Step 6: Re-run change-control and evidence-reference checks**

Run:

```bash
if git diff --name-only af414de..HEAD \
  | rg -qv '^docs/(superpowers|reviews)/'; then
  exit 1
fi
rg -n 'src/|tests/|Cargo\.toml|Cargo\.lock|deny\.toml|rust-toolchain\.toml' \
  docs/reviews/2026-07-11-*.md
rg -n 'https://' docs/reviews/2026-07-11-dependency-governance-review.md
```

Expected: only review documentation changed; reports contain concrete repository references; dependency report contains primary-source links.

- [ ] **Step 7: Commit both review deliverables**

```bash
git add docs/reviews/2026-07-11-stable-baseline-review.md \
  docs/reviews/2026-07-11-dependency-governance-review.md
git commit -m "docs: review stable runtime baseline"
```

---

### Task 9: Independently review evidence quality, scope, and roadmap actionability

**Files:**
- Read: `docs/superpowers/specs/2026-07-11-stable-baseline-review-design.md`
- Read: `docs/superpowers/plans/2026-07-11-stable-baseline-review.md`
- Read: `docs/reviews/2026-07-11-stable-baseline-review.md`
- Read: `docs/reviews/2026-07-11-dependency-governance-review.md`
- Read ignored: `.superpowers/review/*.md`
- Modify only if review finds defects: the two `docs/reviews/` reports

**Interfaces:**
- Consumes: completed reports and all underlying evidence
- Produces: independently reviewed, internally consistent reports ready for user roadmap selection

- [ ] **Step 1: Generate a review package for the tracked report commit**

Record the report commit base and head, then generate a package with the subagent-driven development review-package helper. The package must include only review-spec/plan/report documentation changes; no product files.

Run:

```bash
git log --oneline --decorate -5
git diff --stat af414de..HEAD
git diff --name-only af414de..HEAD
/Users/cc/.codex/plugins/cache/superpowers-dev/superpowers/6.1.1/skills/subagent-driven-development/scripts/review-package \
  af414de HEAD
```

Expected: the changed-file list is limited to `docs/superpowers/` and `docs/reviews/`.

- [ ] **Step 2: Run independent stable-baseline evidence review**

The reviewer checks every confirmed stable-baseline finding against its cited source/test/command evidence, verifies severity and `breaking` classification, rejects speculative claims, confirms every Section 5 domain is covered, and checks that roadmap ordering honors prerequisites.

Required verdict:

```text
Evidence compliance: Approved | Needs fixes
Coverage: Complete | Incomplete
Roadmap: Actionable | Needs fixes
Critical/Important/Minor report defects with file:line evidence
```

- [ ] **Step 3: Run independent dependency evidence review**

The reviewer verifies every version/date/maintenance/license/MSRV/advisory claim against cited primary sources, checks reverse dependency paths, distinguishes suppression from remediation, validates duplicate-cost reasoning, and confirms CEL/JSON Schema/SQLx are separate upgrade domains.

Required verdict:

```text
Source compliance: Approved | Needs fixes
Risk classification: Approved | Needs fixes
Roadmap: Actionable | Needs fixes
Critical/Important/Minor report defects with file:line evidence
```

- [ ] **Step 4: Fix all Critical and Important report defects and re-review**

Use `apply_patch` only on the two final reports. For each fix, append exact evidence to the relevant ignored evidence file, rerun the heading/incomplete-marker checks from Task 8, and request re-review. Do not change severity or remove a finding merely to satisfy the reviewer; resolve the evidence or move an unsupported item to Needs verification.

- [ ] **Step 5: Record Minor observations and run final scope checks**

Run:

```bash
git diff --check
git status --short
if git diff --name-only af414de..HEAD \
  | rg -qv '^docs/(superpowers|reviews)/'; then
  exit 1
fi
rg -n 'T[B]D|T[O]DO|F[I]XME|place[h]older|implement la[t]er|fill [i]n' \
  docs/reviews && exit 1 || true
```

Expected: no whitespace/incomplete markers, only review documentation changes, and Minor observations are explicitly preserved for user triage.

- [ ] **Step 6: Commit review corrections when necessary**

If Task 9 changed either final report, commit:

```bash
git add docs/reviews/2026-07-11-stable-baseline-review.md \
  docs/reviews/2026-07-11-dependency-governance-review.md
git commit -m "docs: correct baseline review findings"
```

If no report changed, do not create an empty commit.

- [ ] **Step 7: Hand the two roadmaps to the user without starting fixes**

Report the audited SHA, report commit range, P0-P3 counts, Needs-verification count, dependency risk summary, dynamic gate outcomes, accepted risks, and independent-review verdicts. Link both reports and ask the user which architecture/correctness or dependency milestone should enter its own design/implementation cycle.
