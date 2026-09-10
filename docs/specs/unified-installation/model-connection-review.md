# Model connection diagnostic and credential revocation

Status: jointly accepted boundary; implementation and bounded HTTP/TLS/PostgreSQL regressions passed. External provider acceptance remains separate.

## Connection diagnostic

The public management entry is `POST /v1/model-configuration:probe`. Its bounded versioned input
contains only the installation digest and an exact ModelDeployment reference. Tenant and Principal
come from authentication. The current installed catalog digest must match. The model must already
be published/deployed: a source alone cannot identify the model to test. Callers cannot supply a
URL, credential, prompt, headers, generation parameters, arbitrary model name or execution identity.
The configured Model/Provider facts supply those physical selectors. The public request and response
are no-store and contain no provider response text.

Foundation owns a bounded request, exact target projection, permit and closed observed result.
Security owns a new read-only `ModelConnectionProbeAuthority` port; it is separate from durable
Model dispatch. Egress calls it over the existing authenticated Security transport before DNS and
again after Secret resolution and immediately before the HTTP send. The exact target is frozen after the first authorization;
a changed target on the second read is rejected. PostgreSQL uses one current read-only repeatable
snapshot per authorization: active Tenant and current membership with ModelRead plus SecretBind,
exact current Model deployment, its bound Provider deployment and published revisions, current
Resource gates, owning Policy kinds/digests/gates, and exact current active model_api_key Binding.
The Provider deployment remains the exact one frozen in the Model; a later Provider head does not
silently change the tested account. Both deployment environments must match the installation.
No ModelTurn, Job, attempt, Run, quota reservation, Receipt or business event is manufactured.
The permit binds the canonical authenticated request and target projection, and expires within the
original authentication/operation deadline. No authorization is cached across diagnostics.

Gateway calls a new Gateway-role-only Egress diagnostic RPC. Egress matches the authorized target
against its existing installed destination grant across protocol, endpoint digest, region,
credential purpose and exact network/TLS/trust/data policy references. The diagnostic reuses the
existing pinned HTTPS transport, DNS safety predicate, late Secret resolver, redirect denial and
trust roots. It does not construct a fake ModelProviderWireRequest to invoke the business method.
Shared physical helpers can be extracted within the same Egress owner, preserving ordinary dispatch
behavior. The existing shared capacity permit is held through response consumption, with at most
four diagnostic requests additionally admitted; overflow fails without provider IO.

The operation has one absolute 30-second budget, bounded further by authentication and the actual
Provider limits. Connect time is at most five seconds. It makes exactly one attempt with a fixed
public request such as “Reply with OK.”, no tools, no structured-output claim, at most 32 output
tokens (also bounded by the declared Model limit), at most 4096 request bytes and 65536 response
bytes. JSON and response headers remain bounded; timeout/cancellation releases permits and closes
the response. No automatic retry follows an uncertain response. The two supported protocol
encoders and a small diagnostic response validator belong to the existing Model adapter owner;
they use the shared strict JSON decoder without durable execution frames or invented usage facts.
A successful observation means the provider returned a valid bounded response for that protocol.
It does not assert the model followed the prompt, supports tools/structured output, satisfies a
latency/retention/tokenizer claim, or completed a durable Agent turn. Diagnostic text and provider
errors are discarded; only a timestamp, tested exact identities/protocol and closed safe outcome
are returned. An unavailable or rejected observation remains visible, never silently retried.

The diagnostic response validator also recognizes bounded Responses reasoning items. This
refinement was jointly reviewed on 2026-09-10 against the owning response observation, the existing
full adapter and the [provider protocol documentation](https://www.alibabacloud.com/help/en/model-studio/qwen-api-via-openai-responses).
Completed responses still require a valid text message. An incomplete response may consist only
of valid reasoning items when its short output budget is exhausted; the existing ResponseReceived
outcome asserts protocol contact, not a completed answer. Every item and its reasoning parts must
validate; empty output, unknown tools/items and malformed optional fields fail closed. Reasoning
content is discarded. Request parameters, bounds, attempt count and capability declarations do not
change. This does not qualify a real provider or a ModelTurn.

A separately authorized streaming diagnostic against the configured Qwen endpoint returned HTTP
200 and an incomplete, reasoning-only response under the fixed short output budget. Its terminal
object also contained `completed_at`, `frequency_penalty` and `presence_penalty`. The diagnostic
retained only safe response shape information, not their values or response content. This observation
is not evidence that the platform's non-streaming probe or durable ModelLoop succeeded, and the
linked provider documentation does not currently specify these three response fields.

The two response decoders jointly accept exactly those optional metadata keys through one pure
validator: absence/null are allowed; the completion timestamp is a nonnegative JSON integer and
each penalty is a finite number within the reviewed numeric bounds. The owning helper and its
independent decoder tests define those bounds. Validation precedes terminal success, malformed
metadata produces a permanent safe failure, and an unrecognized fourth field is still rejected.
These fields never alter canonical requests, normalized text/usage, capabilities or retry policy.
The full response digest naturally covers provider metadata. The shared change passed all 34
adapter tests and strict Clippy after its new regression first failed against the previous decoder.

Tests must cover actor/tenant/source/model/Policy/Secret swaps, revoked permission or Binding before
either authorization, target drift, permit expiry during DNS/Secret IO, foreign workload mTLS,
capacity exhaustion, no IO before authority, exact endpoint pinning, redirect/encoding/JSON/body
bounds, both protocol encoders, malformed/failed responses, timeout and permit cleanup. Real TLS
fixtures prove transport behavior. Actual external model credentials remain a separate user-owned
acceptance step; fixture responses do not establish provider conformance.

## Credential revocation

The existing owning command is `RevokeSecretBinding`. Its current PostgreSQL implementation requires
SecretRevoke, not SecretBind, before Receipt replay; it compares both generation and version, sets
Revoked and increments both under the same transaction, then writes the existing event/Receipt.
The public adapter must preserve that permission rather than grant revocation through import rights.
No physical Secret Manager deletion, new state owner or new Security command is needed.

Expose a safe `GET /v1/model-credentials/{secret_binding_id}` for callers with current SecretBind or
SecretRevoke, and `POST /v1/model-credentials/{secret_binding_id}:revoke` for current SecretRevoke.
Both require the authenticated active Tenant and the model_api_key purpose; no cross-tenant or
non-model secret is projected. The GET returns only the authenticated Tenant identity and typed
Binding ID/provider/purpose/state/generation,
version and the existing resource_etag(ID, version) shape. It returns no ciphertext, key identifier,
opaque physical locator or secret body. The revoke input is closed/versioned with the expected
generation; If-Match carries the exact observed version and the existing Receipt header freezes
request identity. Canonical command identity includes tenant, principal, Binding, generation and
version. Retry uses the original CAS and Receipt; it never reads a newer version to replay the same
intent. The Gateway can reuse its existing PgSecurityTransaction adapter, so this metadata-only
operation needs no physical broker call or new transport credential. Successful responses use the
existing private, no-store cache contract and carry the exact body ETag in the response header.
Revocation leaves Model deployments and the Tenant default pointer intact; their normal current
Secret checks will reject further use. Tests cover permission distinctions, stale CAS, wrong
purpose/tenant, concurrent/repeated revoke, lost-response Receipt replay and denial after current
permission withdrawal, with exact event/generation/version assertions.


## Implementation evidence

The shared default-model closure validator now checks all Model and Provider Policy kinds as well
as the existing exact version/digest/gate closure. Default selection, authoring and diagnostics use
that same validation. The diagnostic has no separate policy interpretation.

The fresh PostgreSQL `phase3_model_turn` fixture passed with the default-model tests, current probe
authorization, wrong Policy kind rejection and concurrent logical credential revocation. The
negative cases cover current permissions, tenant/environment/identity changes, disabled Resource
gates, missing current Model head, credential purpose, stale generation and withdrawal of revoke
permission before Receipt replay. Concurrent identical revocations produce one event and one
version/generation change; replay retains the original identity. The original ModelTurn execution
and quota assertions also pass. No provider network call is made by this database fixture.

The HTTP tests assert closed bodies, the exact successful private cache header, Tenant binding,
ETag/CAS and the distinct read/revoke permissions. Egress tests cover both protocol request bodies,
fixed prompt/output bounds, first and second current authorization, target drift, body bounds and
timeout, four-request capacity held through body consumption, and a real local TLS server with
pinned address, CA and SAN rejection. The authenticated RPC tests deny non-Gateway callers. The
response tests deliberately accept valid text different from the requested word, preserving the
limited protocol-observation claim. A final affected-crate strict Clippy pass completed as well.

These are regression and local transport proofs. They do not establish external provider
conformance, an actual external model response, a user-approved Agent result or deployment
qualification; those remain the separately authorized installation/acceptance journey.

The installed Security Authority's declaration-Artifact read was independently reproduced with
its actual owning grants on a private NOLOGIN PostgreSQL role: the original probe failed with
Artifact permission denied. Joint review accepts only the exact Artifact/Blob readiness metadata
column reads; object locators, Artifact metadata, RunValue bodies, writes and row locks remain
denied. No schema object or business authorization rule changes. The full ModelTurn fixture then
passed with all probe and dispatch authorization calls running through that restricted role, not
an optional administrator connection. Current Artifact/Blob state and digest changes reject the
probe; the existing dispatch identity, lease, permission, gate and credential revocation checks
remain enforced. Temporary test roles were removed after both failure and success. This does not
modify or qualify the already running installation.

A later concurrent identical-revocation run encountered PostgreSQL `40P01` while both transactions
used the same candidate Receipt identity. That original failure remains retained. The public
owner already maps a database failure to Unavailable; it does not promise that PostgreSQL never
aborts a transaction. Joint review therefore corrected the fixture, without changing production
locking or adding automatic command retry: retain both initial results, require exactly one
Applied result, explicitly roll back a deadlock victim, and permit only that exact SQLSTATE to
make one same-command recovery in a new transaction. Every other error and a failed recovery
still fail the test. Current permission checks, one successful Receipt/event and a single
generation/version increment remain required. The complete real PostgreSQL target passed with
this correction; that result is not a claim that database deadlocks were eliminated.
