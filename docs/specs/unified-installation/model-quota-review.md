# Model quota allocation review

Status: jointly accepted; owning/backend implementation and fresh PostgreSQL regressions passed.
The installed public-client/external-provider journey remains separate evidence.

The shared ModelQuotaLimitsV1 validator accepts only nonnegative safe JSON integers. Public DTOs use
ModelQuotaLimitsV1 and ModelQuotaViewV1; allocation contains limits, reserved and used triples.
The installation seed freezes the tenant Model quota account ID with an initial finite limit of
eight concurrent operations. Its later valid limit and counters are not seed-owned immutable facts.
No schema change or parallel allocation authority is required. The existing quota scope uniqueness,
version guards, command Receipts and metadata events remain authoritative.

The installed ModelWorker requires an existing tenant Model concurrency account and three exact
ModelDeployment consumable accounts. Registration does not create these accounts; the earlier
qualification populated them directly through test SQL. A successful configuration or connection
probe therefore does not establish that a real ModelLoop can be claimed.

Keep the existing PostgreSQL quota accounts and ledger as the only current authority. Installation
creates a finite tenant Model concurrency account with a frozen identity, alongside the existing
tenant quota roots. Its initial limit is a reviewed local deployment default, not a provider rate
guarantee. Verification preserves usage/reservations and never recreates a missing account.

Add GET and PUT `/v1/model-quotas/{model_deployment_id}`. GET requires current ModelRead; PUT requires
current TenantManage, independently of ModelDeploy. Both bind the authenticated active tenant and
an existing exact ModelDeployment. Old frozen deployments remain legitimate budget targets; an
active-head change must not reroute a budget command. PUT has closed input
`{schema_version: 1, model_deployment: ExactDeploymentRef, limits: {requests, tokens, cost_microunits}}`.
Each finite nonnegative limit is bounded by its shared owning type. Cost accounting units are not
provider prices or an invoice estimate. Zero is an explicit cap, not unlimited.

The response is a closed, versioned tenant/exact-deployment projection. It contains a nullable
allocation with the three limits and their reserved/used counters, the read-only tenant concurrency
limit/reserved/used counters, and a strong ETag. Do not expose internal quota account identifiers or
add an independent allocation version. The ETag is derived from the existing exact tenant/target,
three account identities and versions (or complete absence). It deliberately excludes the unrelated
tenant concurrency account, since this PUT does not modify it. Partially present or inconsistent
accounts fail closed rather than appearing unconfigured.

PUT requires the original GET ETag and Receipt. Canonical command identity includes tenant, actor,
exact target, complete desired limits and original ETag. Current authorization precedes replay.
An absence token only creates an entirely absent three-account set. Existing accounts are locked in
the same deterministic order used by the scheduler and compared as a complete CAS snapshot; the
tenant lock serializes absent-set creation. New limits must cover current used plus reserved values.
The transaction never resets those counters or the ledger, grants permission, changes model
selection, rotates credentials, or writes physical provider state. Record metadata events against
the existing quota account authorities with their actual versions; do not invent a new aggregate.
Commit all three account changes, audit events and Receipt atomically. Unknown outcomes retry only
the original request/ETag/Receipt and verify current limits afterwards.

CLI model configuration supplies explicit per-model limits and completes allocation/readback before
default selection or reporting an executable model. Console exposes the same limits and a resumable
allocation action; publication and a successful connection probe remain distinguishable from
available execution budget. Merely editing or restarting an installation must not replenish spent
quota. No hidden SQL setup, infinite quota, or implicit budget grant through ModelDeploy is allowed.

Evidence must include fresh installation account creation, zero/partial/stale/cross-tenant inputs,
permission withdrawal before replay, concurrent allocation, limits below reservations, response
loss recovery, unchanged counters/ledger, and a real ModelLoop claim/reservation/settlement using
only the public allocation operation. These tests supplement the existing model execution tests.

The PostgreSQL ModelTurn fixture now creates its three deployment accounts through the same typed
management command used by Gateway. It then passes its original claim, reservation and settlement
assertions. Added cases pass for complete absence, partial-set rejection, two concurrent identical
Receipts, competing CAS intents, revoked management permission before replay, zero limits, actual
reservations and settled usage. Limit changes retain the ledger and counters. The fresh installation
fixture verifies the frozen concurrency account, preservation of later valid limits/reservations,
and rejection of missing or mismatched accounts without recreation. HTTP tests cover exact target,
closed numeric input, required strong CAS and the private JSON/ETag response contract. These are
database and HTTP boundary regressions, not a claim that an external model or installed client has
completed the full journey.
