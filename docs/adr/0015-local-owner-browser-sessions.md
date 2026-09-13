# ADR-0015: Local owner login and browser sessions

Status: Accepted design, implementation pending verification.

Local installations provide a first-owner setup and password login. The existing bootstrapped
administrator principal remains the only business identity; login does not create a second tenant,
membership or permissions projection. A separate local identity service owns password verification
and browser session issuance. Only that role receives the local issuer key and narrowly scoped
PostgreSQL access to its credential and session tables. Console transport receives neither.

The browser holds a random opaque HttpOnly, SameSite session cookie. The local identity service
stores only its digest and an expiry, and exchanges a valid session for the existing short-lived
RS256 assertion on an internal endpoint. Console forwards that assertion to the same Gateway.
Gateway still checks issuer, audience, expiry and current principal/membership permissions. Page
reloads reuse the cookie; logout deletes the durable session. No access token is stored in browser
storage, returned by the public session endpoint or placed in a URL.

First setup is available only for a fresh local installation exposed on loopback (or through an
operator's local port forward). A PostgreSQL singleton constraint arbitrates concurrent setup.
This is not open registration or a qualified internet-facing setup procedure. Serving never runs
DDL. Password hashes use a bounded memory-hard KDF; attempts, sessions, request bodies and hashing
concurrency are bounded. Credential errors do not reveal whether an email exists. Mutating browser
requests require the configured same origin; cookies never flow to model or object-storage hosts.

## Contract and architecture review

The implementing Codex reviewed the strict existing OIDC owning claims, principal authentication
lookup, current schema and grants, installation identity and role publication, Console transport
and current installation documentation together before changing these boundaries. This is an
implementation cross-review, not independent human approval. The review requires two independent
credential/session lifecycles in PostgreSQL, no business event or orchestration changes, and
transactional first setup, failed-attempt throttling, session limits and logout. The service has
no business DML, schema ownership, provider root credential or workload-management authority.

Recovery preserves the installed issuer and owner record. Missing credentials or schema cause
startup failure, never owner replacement or anonymous access. Restart must retain sessions;
expired/revoked sessions must fail, concurrent setup must select one owner, invalid passwords and
cross-origin requests must fail, and role isolation and Gateway permission revocation need tests.
No browser or deployment qualification is claimed until those paths run against the real services.

This supersedes ADR-0010/0012's local token-file-only browser login and prohibition on a dedicated
serving issuer. Explicit CLI assertions retain their existing interface and authorization checks.
It does not change external OIDC deployments or authorize public exposure of the setup endpoint.
