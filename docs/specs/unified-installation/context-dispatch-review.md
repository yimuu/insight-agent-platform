# Remote Context current dispatch authorization

Status: jointly accepted; owning contracts and runtime implementation complete with local TLS,
mTLS and PostgreSQL regression evidence. The full public document-review Run and human response
remain separate, unqualified work until an actual endpoint and user response are available.

## Problem and ownership

Before this change the Egress catalog embedded server-assigned Context Deployment and Implementation identities.
A fresh installation therefore cannot select a remote search address before ordinary Registry
publication creates those identities. Replacing this catalog with a physical destination grant is
only valid if current execution authorization moves to the existing PostgreSQL owners rather than
disappearing. The companion installation review owns address selection and rendering.

ContextQuery, its frozen admission, shared Job/lease/quota, Run/Node and Registry remain the only
business authorities. The metadata-only `ContextDispatchAuthority` uses the existing Security process.
Its PostgreSQL adapter uses one read-only repeatable-read transaction, database time and the already
reviewed Security SELECT grants. It creates no table, state transition, lock privilege, Receipt,
event, credential preparation, quota reservation or new execution permission.

The foundation authorization request binds tenant, query, Job, worker generation, physical attempt,
lease generation and token digest, admission digest, canonical transport-metadata digest, actual Inline input-content
digest and deadline. A bounded permit returns the exact authorization-request digest and an expiry
capped by current lease, query, Run and request deadlines. It is a short-lived read authorization,
not an exactly-once dispatch token; it cannot guarantee revocation of an already-open request.

The existing domain Remote request remains the transport owner. Its internal version advances to 2,
adding worker generation, lease token digest, admission digest and maximum request bytes. A pure domain builder derives
this request from the frozen admission and actual claimed identities, shared by Worker and the
Security adapter. The metadata digest covers the complete typed request except `query_input`.
Egress computes the actual Inline content digest separately; Security compares that digest with
`admission.request.input.content_digest`. `normalized_query_digest` is a separate semantic fact and
must not be assumed equal to the input digest. No input, filter, result body or provider value is
sent to Security, logged or written into error text. No RunValue body SELECT grant is needed.

## Current authorization

The adapter must validate the owning Query and Job payload with their current types, then require:

- The current InFlight query owns the Running Context Job; its attempt, worker generation and lease
  generation and token digest match, the lease is unexpired and the existing quota reservation is present.
  Job version is not frozen across concurrent heartbeats: this read authorization checks the current
  stable lease identity, while outcome writes keep their existing exact CAS fence.
- The request admission digest and the complete expected metadata digest match the frozen admission;
  input content matches its exact reference. This binds deployment, revision, protocol/mapping,
  endpoint, region, all secrets and transport policies, projection, classification, query/filter
  identities, pagination, byte caps and deadline without a second independently maintained schema.
- Tenant, current principal and Context query permission remain active. The original Run/Node remain
  active, in the expected relationship and kind, before deadline, without pause/cancel/timeout;
  Run bindings still match admission. The current principal must still satisfy the frozen grant's
  entitlement projection: require its canonical snapshot digest to equal the admitted principal.
  The grant principal digest remains that admitted snapshot; its policy generation is compared with
  its actual owner, the frozen Run principal binding generation, not the current membership epoch.
  A membership change after Query admission denies the old Query grant. A change between Run and
  Query admission remains governed by the existing fresh admission lifecycle and is not newly denied.
- Context resource/deployment and exact interface/implementation and every frozen policy remain
  enabled and exact. Each secret binding is currently active and permits its frozen resolution
  policy/generation. This uses existing Registry and Secret read owners.

Egress first validates shape, capacity and physical destination. It performs current authorization
before credential resolution or DNS, builds the bounded body with both physical and frozen request
caps, then rechecks the same permit immediately before opening HTTP. No application queue, retries,
redirects, proxy inheritance, private-address exceptions, TLS bypass or new trust installation is
introduced. Physical grants cannot supply a principal, tenant or Context identity.

## Versions, capacity and failure

The external Remote Search HTTP request/response protocol and result mapping stay at version 1.
The worker's remote adapter capability additionally binds internal execution schema version 2 and
current dispatch authorization semantics. Native Context and other protocols keep their identities.
This is a clean cut; no old reader, catalog fallback or editing of an existing immutable installation.

Existing semaphore, RPC metadata, HTTP request/response and time limits remain hard ceilings. Metadata
and inline data are bounded before hashing. All authorization transport failures are safe fixed
classifications before dispatch; no upstream error text is returned. After dispatch, existing
Context failure evidence and fenced settlement/recovery remain the only outcome path. Read
unavailability cannot become permission or a reason to bypass authorization.

## Evidence and remaining qualification

Use the real claimed Remote Context PostgreSQL fixture and the dedicated Security role: positive
permit, wrong tenant/query/job/worker/attempt/lease/admission/body/metadata, expired lease, changed
Run control/bindings, revoked current permission, disabled exact resources/policies and revoked
secret must fail. Verify no business writes or RunValue-body privileges. Test the real Egress
connector with recording authority/secret/DNS ports: rejected/unavailable/expired permit prevents
HTTP and hidden retry; forged actual input or metadata fails; exact allowed request reaches the
production encoder and result mapper. Test closed RPC shape and authenticated peer role, old
execution version rejection and exact capability changes. Actual HTTPS sample execution remains
separate and still requires a real deployment and a user-owned HumanTask response.

The current implementation has passed the claimed PostgreSQL authorization matrix, real Security
mTLS role/shape checks, real Egress TLS/authorization tests and the current Worker regressions.
The final Egress tests also reject empty/malformed roots during catalog construction and verify
that a later valid root in a bundle participates in real TLS trust; `Certificate::from_pem` alone
was not accepted as certificate validation. Input/metadata/fence/current-principal tests retain
the separate body and normalized-query identities. No new business storage or Security body-read
privilege was added. These tests establish the dispatch boundary, not live corpus conformance.
