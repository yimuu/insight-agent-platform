# Independent boundary review

This is a temporary implementation review, not a qualification report. Current contracts and the
accepted ADR remain authoritative. Remove this file with the proposal after implementation and
evidence agree.

## Existing authorities

The ModelProvider resource can represent an individual source/account; ModelProfile and
ModelDeployment already represent multiple models bound to its exact Provider revision/deployment.
A vendor name must not select credentials or imply a supported protocol. Selection Policy owns the
candidate algorithm, not a tenant-wide default pointer. A default in TenantConfig and an immutable
alias on the existing Resource are acceptable additions with current authorization, ETag checks,
same-tenant exact references and database uniqueness. They must not change frozen Run selections.
Physical changes use the current schema and fresh provisioning, as required by the current AGENTS.

Installed model endpoints currently repeat exact Provider identities. Replacing this duplication
with a closed deployment-owned destination grant is acceptable only after the authorization chain
is explicit. Current Secret resolution checks the exact Binding but does not independently validate
ModelTurn/Job/lease/admission identities. It must not be described as an existing per-dispatch Model
authorization check. Removing the catalog keys alone is insufficient evidence of equivalent
cross-account isolation. The grant must preserve all endpoint/protocol/region/purpose and exact
policy constraints; public clients cannot install it or select arbitrary network destinations.

Prepared Secret registration is currently a service-only internal operation, and external writes
are MCP-specific. A public credential import needs a reviewed actor/service authorization boundary
and a bounded generic prepare/load contract. Receipt retention is not permanent initialization
state. Stable preparation identities, exact external readback and existing Binding facts must cover
unknown outcomes; if no external write happened and raw input was never persisted, recovery needs
the same explicit input again. Raw credentials do not enter resource JSON, command events, public
digests, browser persistence or diagnostic output.

## Model dispatch authorization and destination grants

Decision: the refined upstream boundary is jointly accepted for implementation. The reviewed
ModelDispatchAuthorizationV1 includes exact execution/Provider/Secret identities and physical
request byte/time limits. Its caller identity is never a submitted user field: current ModelInvoke
authorization derives from the frozen admission principal and current tenant membership. The
closed wire protocol determines the adapter name, which must match the frozen installed adapter.

Security delegates to the existing Model/PostgreSQL owner using one consistent read snapshot.
It checks current Run/Node/Turn/Job control, worker/attempt/lease ownership and expiry, active quota,
the frozen admission and request digests, complete Provider closure, exact Secret membership and
physical limits. Missing, stale, cross-account or unavailable authority fails before DNS or Secret
access, including the anonymous development branch. The response contains only a request-bound
permit whose lifetime is capped by the database lease and original request deadline. Egress checks
that binding and expiry both before DNS and immediately before HTTP; it does not cache permission.

The permit is an authorization snapshot, not a unique dispatch claim or a promise of atomic
cancellation across an external network. PostgreSQL transactions do not span provider IO. A trusted
ModelWorker remains the canonical-to-wire encoder; metadata authorization is not proof that PG
re-encoded the HTTP body. Existing per-request identity, cancellation, response limits and Secret
current-state checks remain independent requirements. Catalog exact keys stay until the new
authority consumer and denial tests are connected. No permissive legacy fallback is introduced.

Required tests cover two accounts using one grant, credential/Provider/request/lease/limit swaps,
revoked users and Bindings, cancelled/expired jobs, anonymous-path authorization, permit identity
and expiry during DNS/Secret work, authority unavailability, no credentials in errors, and zero
physical calls on denial. Real database tests must exercise the owning read and its narrow role
grants; fixture tests alone do not close this implementation slice.

## Resource aliases and the tenant default model

Decision: jointly accepted for implementation in the existing Resource/Tenant/authoring owners.
ResourceDraftPayload holds one optional immutable ResourceAlias, independent of display_name. The
current schema constrains its JSON shape and enforces uniqueness within tenant and resource kind;
it does not add another alias table or make display names unique. Draft replacement cannot add,
remove or rename an alias after creation. Normal validation/publication preserves it. Concurrent
alias collision rejects the whole ordinary command transaction without a successful Receipt/event.

TenantConfig holds an optional exact ModelDeployment default. GET/PUT `/v1/model-default` use current
ModelRead/ModelWrite authorization, the owning tenant ETag, and existing Receipt semantics for
mutation. Setting validates the exact same-tenant active/enabled Model deployment and its closure;
clearing is explicit. Bootstrap replay preserves this later mutable setting rather than comparing
it to or overwriting it with the original seed. Schema verification remains independent of settings.

Authoring selectors gain Alias with environment, and DefaultModel with environment only for Model
slots. Resolution reads the selected current authority in the existing consistent transaction and
returns exact bindings. A missing default is explicitly unconfigured; a disabled, mismatched or
wrong-environment default never falls back. The request still supplies a real exact Selection Policy
binding. This slice does not invent AgentAuthoringProfile model entries from a pointer that lacks
such a binding; the higher-level compiler must receive the actual initialization/registration policy.

Tests must cover two tenants and kinds reusing an alias, same-scope conflict and rollback, immutable
alias under display-name edits, default ETag races and revoked-user replay, wrong kind/tenant/digest/
environment/control state, clear/unconfigured, unchanged frozen Agent/Run bindings, and bootstrap
replay preserving the current setting. Public DTO, current schema inventory and generated contracts
must be updated from their owning definitions before this implementation is considered complete.

## Textual JSON output completion

Decision: accept completing the existing `StructuredOutputContract.textual_json_fallback`
capability for the two installed Responses and Anthropic Messages adapters, subject to the bounds
below. The parent review selected this scope. It does not introduce another provider protocol,
change ModelLoop output semantics, or certify arbitrary provider capabilities.

The owning contract already permits this output mode and the Model response validator already
requires the exact frozen output schema. ModelLoop currently always requests a structured result;
both adapters currently reject that request when native structured output is absent. A profile flag
alone therefore cannot make a text-only model usable.

Mode selection is deterministic before dispatch. When native support is declared, preserve the
existing native request. Otherwise require explicit textual JSON support and reject a request that
mixes tools or tool-intent output with this mode. A provider error never triggers a mode switch,
another request, a different credential or a different model. Existing protocol support and usage
restrictions continue to apply.

Do not inject instructions in the adapter. Text mode preserves canonical message text exactly and
omits native structured-output parameters. Instructions asking the model for JSON belong in the
normal canonical prompt assembly before estimation and quota admission. Adapter-side insertion
would bypass that accounting, even if the wire body remained within its byte limit. Preserve the
existing canonical request identity, source map, deadlines and lease/usage authority. The encoded
provider body, schema and output remain within the owning byte bounds. This review does not treat
the current UTF-8-byte estimate as a provider tokenizer or a proven token upper bound.

Normalize the provider text with the existing bounded strict JSON parser, then validate the exact
schema and construct the ordinary structured result. Reject duplicate keys, malformed JSON,
non-finite values, trailing text, Markdown fences, schema mismatch and oversized output. Do not
extract an apparent JSON substring, repair output or downgrade validation. Stream errors and
unsupported tool output remain ordinary safe failures. No schema version or new persistence field
is required for this existing output capability; actual adapter binaries and their installed
worker evidence must nevertheless be rebuilt.

Required regression evidence covers both protocols: explicit text mode success; native mode
unchanged; native-plus-fallback still choosing native; absent support and mixed tools rejected before
HTTP; real provider frame decoding; Unicode/escaped schema values; malformed, duplicate, fenced,
trailing and schema-invalid output; input/schema/output bounds; interrupted streams; and failure
without retry or secret/body disclosure. These tests prove the adapter boundary, not real-provider
qualification. A later real model call and the requested human approval remain separate evidence.
