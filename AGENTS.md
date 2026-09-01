# Insight Agent Platform Engineering Rules

These rules apply to the entire repository. They constrain implementation choices; the Platform
specifications define product behaviour. When they disagree, stop implementation, repair the
specification and cross-review first, then continue from the reviewed contract.

## Product and cutover

- The target public protocol and routes remain `insight.platform/v1` and `/v1`.
- The replacement is clean-cut. Do not add a `/v2` API, schema compatibility layer, dual writes,
  legacy fallbacks, or migration code for an unshipped candidate schema.
- Draft or architecture-revision documents are targets, not current behaviour.

## Architecture

- Specifications state observable behaviour and invariants. They must not require a particular
  table name, trigger, migration number, checksum, or proof-table layout.
- Keep the control plane, durable orchestration plane, and untrusted execution plane separate.
  Gateway/API processes validate and coordinate; they do not execute user code or become the
  durable authority for background work.
- One business fact has one current-state authority. Do not mirror the same state across aggregate,
  transition, outcome, receipt, and evidence tables.
- Prefer shared `Resource`, `ResourceVersion`, `Deployment`, `Run`, `Invocation`, `Job`, `Task`,
  `Event`, `Receipt`, and `Outbox` concepts over per-domain copies.
- Create a domain-specific table only when the object has an independent lifecycle, independent
  concurrency, or a demonstrated core query that the shared aggregate cannot serve.
- Do not create a table per transition, evidence kind, rejection reason, operation result, release,
  or cross-review finding.
- Strong consistency does not mean implementing all business semantics twice in Rust and database
  triggers. Choose one semantic authority and retain only structural database guards.

## Capability, Skill, MCP, and agents

- Capability is the only generic callable contract. Native, HTTP/gRPC, MCP Tool, and Sandbox code
  are typed implementation backends, not separate invocation models.
- Skill is an immutable, versioned method package containing instructions, references, assets, and
  declared requirements. A Skill is not an Agent, process, tool, or script runner, and it does not
  own execution state.
- Scripts are optional Capability implementations. Publish executable code as an immutable Sandbox
  package and bind it through an exact Capability Deployment; never execute a file merely because
  it appears in a Skill package.
- MCP is an independent protocol host, not an alias for Action or Capability. Preserve Tool,
  Resource, Prompt, Task, transport, authorization, and subscription semantics when projecting MCP
  objects into platform contracts. The first release supports remote Streamable HTTP only; managed
  stdio servers and persistent sandbox sessions are deferred.
- A Subagent is a durable child Run with exact bindings, its own state and quota, and a typed parent
  link. Do not implement agent-to-agent work as an in-memory function call, unbounded recursion, or
  an opaque tool JSON exchange.
- Cross-agent and cross-process progress travels through committed Run, Invocation, Job, Task,
  Event, Receipt, and Outbox contracts. Direct RPC may transport a command but never replaces the
  durable state transition.
- Dynamic management uses the shared Resource -> immutable ResourceVersion -> Deployment -> Binding
  lifecycle. A Run freezes exact Agent, Skill, Capability, Model, Context, Policy, and Sandbox
  revisions; active-head changes never mutate an existing Run.

## Code execution

- Python, Node.js, WASM, and trusted Shell run only in the independent Sandbox Execution Plane.
  API, Scheduler, Model Worker, Capability Worker, and MCP Host processes must not spawn them. If
  managed MCP stdio is introduced after the first release, it must obey the same boundary.
- Sandbox submission is an authenticated service operation backed by a durable shared Job. The
  Sandbox Dispatcher may report a fenced physical outcome; OpenSandbox cannot directly mutate Job,
  Run, NodeExecution, or Invocation authority.
- Isolate Sandbox queue, permits, connection pools, pods, and where required node pools from the
  control plane. Sandbox exhaustion or failure must not consume API, Scheduler, Model, native
  Capability, or MCP admission capacity.
- The first release has exactly one physical code provider: an internal OpenSandbox Server using
  explicit per-attempt Docker/runc containers. There is no WASI, gVisor, host, microVM, Firecracker,
  KVM, managed-stdio, hardware-virtualized, or silent fallback backend in the first-release contract.
- Sandbox provisioning and shared Job terminal commit are idempotent and fenced. Once a workload
  may have started, recovery must not automatically submit it again. Network/database/message/API
  side effects created inside the workload, including their idempotency, are owned by the Sandbox
  Package and target service rather than the platform.
- A published Sandbox Profile may enable direct outbound network access. This is not a Platform
  Egress Broker or exactly-once boundary, and it must not grant host networking, runtime sockets,
  Platform credentials, public OpenSandbox ingress, or direct business-state mutation.
- Runtime dependencies are resolved, scanned, and frozen during publication. Do not run package
  managers, mutable image tags, string-built shell commands, or arbitrary installers at execution
  time.
- The Sandbox is for bounded platform code execution, not heavy compute. Route long-running or
  resource-intensive workloads to an independently deployed, quota-controlled remote Capability.
- Model responses are Inline-only in the first release. Files and large generated outputs are
  produced through the shared Artifact data service by Capability or Sandbox work, not by a
  dedicated Model Artifact Producer.

## Persistence

- The clean baseline target is 18–24 core tables. This is a design budget rather than a table-count
  contest. Any table beyond the budget needs a written consolidation analysis in the relevant ADR.
- Keep identity, tenant, state, optimistic version, scheduling time, ownership, and hot predicates in
  typed relational columns.
- Put immutable configuration closures, frozen admission data, low-frequency evidence, result
  details, and diagnostics in bounded typed JSONB snapshots.
- Every JSONB contract has a Rust nominal type, `schema_version`, closed validation, a size limit,
  canonical serialization, and a digest. Published snapshots are immutable.
- Aggregates own current state. A shared append-only event stream owns history and audit; events do
  not become a second current-state projection.
- PostgreSQL owns primary/foreign keys, tenant scoping, uniqueness, optimistic CAS, lease fencing,
  transaction atomicity, and outbox durability.
- Rust application services own state-machine semantics, policy decisions, schema validation,
  retry/cancel/reconciliation decisions, and construction of frozen snapshots.
- Message systems carry wake hints or committed outbox messages; they are not execution-state
  authorities.

## Shared models

- Agent, Skill, Capability, Context, MCP, Model, Policy, and Sandbox definitions reuse one resource
  lifecycle. Domain meaning remains typed even when persistence is shared.
- Attempt, remote work, polling, recovery, and background operations reuse `Job`.
- A public asynchronous Operation is a safe projection of its shared `Job`; it is not a separate
  aggregate, state machine, or table.
- Approval, interaction, and human work reuse `Task`.
- Command and callback idempotency reuse `Receipt`.
- Transitions, outcomes, rejection evidence, and audit records reuse `Event`.
- Artifact metadata and blobs may stay domain-specific because they have independent storage and
  security lifecycles; their references, grants, and holds should share one typed link model.
- Artifact deployment has three first-release roles: Gateway, Data Worker, and Maintenance. Logical
  client methods and capacity lanes may be distinct, but do not introduce ordinary or model-specific
  producer infrastructures that duplicate staging, verification, quota, or cleanup state machines.

## Release and schema authority

- Application promotion and rollback are owned by Kubernetes/GitOps. Candidate and qualification
  reports are content-addressed CI/CD artifacts, not database aggregates or public management API
  resources.
- A Run freezes exact tenant-scoped ResourceVersion and Deployment bindings at admission. There is
  no `InstallationReleaseState`, compatibility generation, release singleton, or installation
  promotion/rollback transaction in the business database.
- Public HTTP contracts use OpenAPI/JSON Schema, internal RPC contracts use protobuf, and persisted
  JSONB uses Rust nominal types. Do not hand-maintain all three representations for an object that
  does not cross those boundaries; generate registries and schemas from the owning type where
  possible.

## Migrations

- Replace the unshipped migration 1–35 candidate set with one reviewed minimal baseline migration.
- A migration expresses a real physical schema change, not a cross-review item or proof step.
- After a baseline is shipped, migrations are immutable and forward-only.
- Business specifications must not use migration checksums, schema digests, object counts, or
  trigger counts as feature-completion evidence.
- Never delete a shipped schema or production data under this clean-cut rule; confirm deployment
  state before destructive migration work.

## Specification workflow

- Read `docs/specs/platform-v2/00-overview.md` before changing Platform contracts.
- Treat this file as a compact engineering policy index, not a second source of product truth. Put
  field schemas, state transitions, error details, limits, and acceptance evidence in their owning
  specification and link them through the cross-review.
- Architecture changes move every affected specification back to Draft / Architecture Revision.
- Update upstream contracts before downstream contracts, then run a full 00–18 cross-review.
- Cross-review must cover state ownership, IDs, JSON schemas, errors, transactions, events,
  permissions, capacity, failure recovery, and test fixtures without prescribing redundant storage.
- Only Reviewed/Accepted contracts may generate an implementation plan or baseline schema.
- Qualification is layered: contract/unit, PostgreSQL transaction/concurrency, RPC identity and
  backpressure, critical end-to-end, then release-only security/chaos/capacity/soak/restore. Do not
  repeat every field permutation at every layer or persist qualification gates as runtime state.

## Implementation quality

- Prefer the smallest model that preserves the complete required semantics, not the smallest patch
  that keeps old code compiling.
- Before adding an abstraction, table, service, queue, or projection, identify its independent owner
  and explain why an existing shared model cannot represent it.
- Preserve unrelated user changes in a dirty worktree. Do not stage or commit unless requested.
- The repository owner has requested continuous commit hygiene: after a coherent implementation
  batch passes its proportionate checks, commit it before starting the next batch. Do not leave
  completed work from multiple phases accumulated in the working tree.
- Keep each commit reviewable and single-purpose. Include the affected tests, contracts, deployment
  manifests, and documentation in the same commit as the behavior they prove; do not commit known
  failing or half-wired integration states.
- Tests prove behaviour, concurrency, crash recovery, and security boundaries. Object counts,
  trigger counts, and broad snapshots alone are not completion evidence.
- Do not claim an API, topology, capacity, migration, or runtime behaviour is current until its
  implementation and qualification gates have passed.
