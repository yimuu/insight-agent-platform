# Model credential import boundary review

Status: jointly accepted for implementation after independent boundary review. This extends the existing SecretBinding
and prepared external version owners; it does not add a credentials table or another secret store.

## Public and internal request

`POST /v1/model-credentials` accepts a caller-chosen UUID operation identity, an installed
SecretProvider identity, and one bounded API key. Tenant and original Principal identity come from
the authenticated Gateway context. The credential purpose is the existing `model_api_key` purpose.
The key is a bounded redacted/zeroing transient value, carried in the private RPC payload, never in
canonical metadata, Resource JSON, Receipt/Event payloads, public hashes or logs. The response is
only an exact SecretBinding reference. HTTP and browser responses use no-store; the Console clears
the input after completion and never persists it. CLI environment mapping is resolved once locally.

Gateway calls one new Egress RPC authorized only for the Gateway workload URI. Being authorized for
this RPC does not permit Gateway to dispatch model/capability requests or install destinations.
Egress requires an installed SecretProvider and asks Security for current authorization before
Secret Manager or KMS access. Security derives current tenant membership from the original
Principal identity and requires SecretBind. The metadata-only permit binds tenant, Principal,
operation UUID, provider, purpose and a short deadline. The permit lasts at most 30 seconds; Egress checks its exact request digest and expiry immediately before each Secret Manager or KMS operation.

## External preparation and final registration

A domain-separated preparation digest is derived from tenant, Principal identity, provider, purpose
and operation UUID, excluding the raw key. The existing provider namespace and deterministic version
identity choose one exact external object/version. An encrypted provider-side envelope holds the
metadata plus credential. `prepare-or-load` compares the complete metadata and credential with the
existing winner; different input for the same operation fails. AWS CreateSecret uncertainty permits
only bounded exact readback of the same name and version; absence/unavailability remains unknown.
A retry uses the same identities and input. It cannot create another name or silently rotate an
existing Binding. The operation identity is persisted by the importing client before sending; a
client losing it must explicitly start a new import. No raw key is stored in a client recovery file.

The existing prepared registration command gains an optional delegated import identity, included in
its semantic digest. Service-only MCP registrations keep the same owning command path with no
delegated identity. An import registration must match the preparation metadata and purpose. In one
PostgreSQL transaction Security checks both the trusted service's SecretBind and the original
Principal's current SecretBind before Receipt replay or insert. Revocation between preparation and
commit therefore prevents registration. Existing exact Binding facts cover replay after a Receipt
has expired: a matching active Binding is returned, while a revoked/different Binding is rejected.
The first committed encrypted opaque reference remains current. Import events identify the original
Principal and preparation, but include neither plaintext nor a hash of the API key.

Prepared material whose registration fails remains an exact external orphan, identifiable by its
installation namespace and operation identity. The import response must distinguish rejected,
temporarily unavailable and outcome unknown; it must not imply atomic external+PostgreSQL commit or
silently delete a prepared winner that another retry may be registering. Operator cleanup is exact
and deployment-owned. There is no background secret deletion inferred merely from elapsed time.

## Evidence required

Tests cover unauthorized/revoked callers before external writes and before final commit; private RPC
role separation; provider/tenant/operation/input swaps; concurrent same-operation winner; exact
readback after timeout and stale readback; retry after lost response and expired Receipt; revoked
Binding replay; real KMS sealing and Secret Manager readback; no raw credential in Debug, errors,
events, receipt bodies or recovery files. These tests qualify import only, not model capabilities.
