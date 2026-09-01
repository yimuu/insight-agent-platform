# Insight Agent Platform Engineering Rules

These rules apply to the entire repository. They are durable engineering guardrails, not a second
source of product truth. The reviewed Platform specifications and ADRs own product behaviour. If
code, documentation, and the reviewed contract disagree, stop and repair the contract and
cross-review before continuing.

## Core architecture

- Make replacements clean-cut. Do not add compatibility layers, dual writes, or legacy fallbacks
  unless an explicitly reviewed contract requires them.
- Keep control, durable orchestration, untrusted execution, and data boundaries separate. A
  transport or physical provider must not become a second business-state authority.
- One business fact has one current-state authority. Reuse an existing shared model before adding
  a new aggregate, table, service, queue, or projection; add one only for an independent lifecycle,
  concurrency boundary, or demonstrated core query.
- PostgreSQL owns durable state, transaction atomicity, concurrency guards, lease fencing, and
  outbox durability. Application code owns business semantics. Messages carry wake hints or
  committed projections, never current execution state.
- Specifications describe observable behaviour and invariants, not required table names,
  migration numbers, trigger layouts, checksums, or proof artifacts.
- Application promotion and rollback belong to the deployment/GitOps system, not the business
  database or public management API.

## Contract workflow

- Before changing a Platform contract or implementation, read
  `docs/specs/platform-v2/00-overview.md`, the owning reviewed specification, and linked ADRs.
- Draft and architecture-revision documents describe targets, not current behaviour.
- Update affected upstream contracts before downstream contracts. Architecture changes require a
  cross-review covering ownership, identities, schemas, errors, transactions, events, security,
  capacity, recovery, and test evidence.
- Keep fields, state machines, limits, topology, and acceptance evidence in their owning
  specification. Keep this file small and principle-oriented.
- Only Reviewed or Accepted contracts may authorize implementation or schema work.

## Data and migration safety

- Contracts crossing persistence or process boundaries must be typed, bounded, versioned, and
  validated. Generate boundary representations from the owning type where practical.
- Database constraints enforce structure and concurrency; do not duplicate the full business state
  machine in both application code and database triggers.
- Migrations represent real physical schema changes. Once shipped, they are immutable and
  forward-only. Never rewrite or delete shipped schema or production data without an explicit,
  reviewed, and deployment-aware plan.

## Delivery discipline

- Prefer the smallest model that preserves the complete reviewed semantics.
- Preserve unrelated user changes. Keep each commit coherent and single-purpose, include its tests
  and contracts, and do not commit a known-broken or half-wired state.
- Verify changes in proportion to risk. Tests should prove behaviour, concurrency, recovery, and
  security boundaries rather than object counts or broad snapshots alone.
- Do not claim behaviour, topology, capacity, migration, or qualification as current until the
  corresponding implementation and evidence have passed.
