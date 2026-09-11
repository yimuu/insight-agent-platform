# Model configuration compiler review

Status: jointly accepted upstream boundary after independent review of metadata, ownership and recovery.

## Truthful minimal model

A basic registration is an operator declaration of a text model and local request budgets. It is
not a provider qualification report. The ModelCatalogEvidence owner gains a closed evidence basis
(`operator_declaration`, `protocol_observation`, `qualification`). Its Artifact must contain the
actual corresponding declaration/report and its digest. Provider deployment replaces the ambiguous
mandatory conformance Artifact field with a typed admission evidence value, distinguishing an
operator declaration from conformance evidence. Registry still requires the exact ready Artifact.

Context bounds remain enforced operator admission limits. A tokenizer digest becomes optional;
absence means no tokenizer was installed or qualified. The estimator digest identifies the actual
implemented UTF-8-byte/4 rounded-up estimator. It is explicitly an estimate, never an upper bound
or a provider tokenizer claim. Provider retention and subprocessor digest become optional and
training gains `unspecified`. The existing maximum classification is an operator-authorized outbound
ceiling, not a provider guarantee. The basic compiler explicitly declares Internal, which is needed
for the existing canonical platform/Agent/node instruction blocks. It never defaults to Confidential
or Restricted. Unknown provider facts do not erase current principal, ModelInvoke or data-policy
checks. Public-only profiles cannot execute a prompt that contains Internal platform material; the
compiler does not relabel that material or silently relax the selected ceiling.
Zero retention is a valid explicit value distinct from unknown. Existing explicit documented claims
remain available in the advanced owning profile. The basic compiler emits unknown values rather
than claiming no retention, no training or an empty subprocessor set.

The basic model uses externally-mutable provider identity, text input/output, tools disabled and
native structured output disabled. Explicit bounded textual JSON fallback uses the existing exact
schema validator. The compiler puts its JSON-only instruction in the ordinary platform instruction
source before canonical assembly so byte/token budgets and source maps include it. The adapter
does not add hidden prompt content. Streaming transport support and successful JSON output are
separate observations; configuration alone does not mark either as tested.

## Installation catalog and ordinary Resource lifecycle

One typed deployment-owned model installation catalog supplies installed adapter identities,
destination grants and the exact Policy bindings to Gateway, Egress and authoring defaults. It is a
configuration projection of existing deployment facts, not a business aggregate. Gateway exposes
only safe destination choices and adapter capabilities. Public clients cannot install a grant or
relax one. Foundation owns the protocol selector used by this catalog and the wire request; Egress
retains DNS, HTTPS/trust-root and private-address enforcement.

The one-shot owner provisions the actual default protocol/network/TLS/trust/data/safety/budget/
projection/OnlyCandidate Policies using the existing bootstrap Resource lifecycle and actual
declaration Artifact. Their generated identities and digests enter the same installation catalog.
They are verified on recovery. This is explicit installation provisioning, never lazy DDL or an
unreviewed policy discovered by scanning for the first available candidate.

A pure server-side configuration compiler accepts an alias/display name, a selected installed
destination, an exact imported credential and (for a model) an existing exact source plus provider
model identity and bounded request limits. It returns the ordinary typed Resource authoring document
and deployment bindings. Client entry points perform the existing Artifact upload, draft create,
validation-operation poll, publish and deployment APIs, with stable per-step idempotency identities.
Only the authority allocates business IDs; returned published identities fill exact self references.
The compiler owns the generated semantics and is shared by Console and CLI through one API. Clients
own progress/retry UX, not another registry or workflow state machine. Recovery reads existing
Resource/Operation/Deployment authority and compares the requested semantic digest; it never trusts
only a local success flag. Credential import completes separately with its reviewed recovery rules.

Updates use the existing Resource ETag and immutable alias rules, publish a new exact revision and
deployment, and explicitly select defaults if requested. Credential rotation uses a new exact
Binding; current frozen Runs are unchanged. Revocation uses the existing current Secret owner.

## Connection test

A connection test is a bounded Egress operation authorized for the current authenticated caller's
ModelRead plus SecretBind (because it exercises an account credential) and the same active source,
exact Secret and destination policy closure. It does not manufacture a ModelTurn/Job lease to reuse
the invocation RPC. Its result is a safe observed outcome with timestamp and tested protocol/model;
no prompt/provider error/key body is retained. Read-only authorization is repeated before physical
IO and never bypasses destination/DNS/TLS limits. The test uses a fixed public diagnostic prompt,
small output budget and one attempt, and does not create a conformance claim. Real Agent execution
continues to use ModelInvoke, quota, durable ModelTurn/Job fencing and the normal adapters.

## Evidence required

### Usage declaration and observed accounting — jointly accepted

`provider_reports_usage=false` in the operator declaration means that reporting is not asserted as a
provider guarantee. It does not prohibit an installed adapter from consuming actual protocol usage.
Responses and Anthropic adapters must not reject this declaration before dispatch solely because of
that flag. Their successful terminal decoders still require actual input/output token counts; missing
or malformed usage is a dispatched failure and retains the existing conservative accounting path.
No estimate or zero count may be invented to accept a successful response.

The domain requires both counts whenever `accounting_quality=provider_reported`, independently of
the declaration flag. Complete observed usage remains subject to overflow checks, the existing
cache/reasoning/cost contract and exact admission ceilings. Estimated/Reconciled, quota settlement,
lease/CAS/Receipt and immutable request/profile identity semantics do not change. The declaration
stays false; no evidence or provider qualification is fabricated, and no wire/schema version changes.

Required regression: both protocols with a real false declaration and valid usage succeed while
preserving exact observed amounts; missing/negative/non-integer/overflow usage fails after dispatch;
both flag values require complete ProviderReported counts at the domain boundary; a false profile
settles actual usage once and still rejects excess admission usage. Tests must not replace actual
provider invocation or installation qualification with fixture success.

Evidence: the domain regression first reproduced false + ProviderReported + missing counts being
accepted as zero tokens, then passed with the independent presence guard. All 15 Model domain tests
pass, including exact observed settlement and an unchanged admission-ceiling rejection. The adapter
owner reports all 35 tests passing after both real protocol fixtures exercised false/true declarations
with observed 50/10 token counts and rejected missing or malformed counts. These are contract and
protocol-fixture results; the installed application's real ModelLoop qualification remains separate.

Tests must verify unknown metadata is represented honestly and the explicit outbound ceiling is enforced;
explicit zero retention; stale/incorrect evidence Artifact; actual estimator digest; instruction
source accounting; compiler output accepted by normal Registry validation; destination/source/
tenant/policy/credential swaps; no installation mutation via API; lost-response lifecycle replay;
source and model updates; model defaults; both CLI and Console calling the same compiler; and real
diagnostic/Agent calls through the installed Egress and Secret path. No claim of complete deployment
or model qualification precedes that evidence.
