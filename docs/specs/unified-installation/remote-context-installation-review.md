# Remote Context installation and document-review sample

Status: jointly accepted; typed grant/input, shared renderer, all three consumers and the sample
source/constructors are implemented with targeted test evidence. New-image installation and the
full public document-review business journey retain separate qualification records.
This supplements the unified-installation target; the J image evidence remains bound to its
already frozen source and does not include this proposal.

## Confirmed gap and existing owners

Before this change the shared installation's base roles omitted ContextRemote and its Egress producer
always emitted an empty remote Context catalog. Adding a Context deployment through the ordinary Registry API could not
enable that physical route. An empty catalog rejects before DNS or HTTP. The local native Context
catalog contains a fixed development item and is not the document-review corpus.

The existing compiler and public authoring resolver already support FullPlan Context and Model
slots, ContextQuery and HumanTask. Context Interface/Implementation resources, Policy revisions,
Context Deployment and Agent publication remain ordinary Registry lifecycles. Their exact IDs must
come from that lifecycle, not from an installation input or a sample placeholder.

The former installed Egress entry repeated exact Context deployment, implementation and policy
identities. Requiring these in a fresh installation input creates an ordering cycle with later
server-assigned Registry identities. Removing those checks alone is unsafe: the current Remote
Context RPC checked the ContextWorker identity and request shape, but did not independently reread
current authorization before dispatch.

Relevant owners:

- [Installation input](../../../crates/deployment/platform-deployment-contracts/src/installation.rs)
  and [shared producer](../../../tools/rust/platform-deployment-tooling/src/full_profile.rs).
- [Installed remote transport](../../../crates/adapters/platform-egress/src/remote_context.rs),
  [execution request](../../../crates/domains/platform-context/src/remote.rs), and
  [worker](../../../apps/services/platform-context-worker/src/lib.rs).
- [Context PostgreSQL owner](../../../crates/adapters/platform-postgres/src/context_query_repository.rs)
  and [public slot resolution](../../../crates/definitions/platform-registry/src/authoring.rs).

## Accepted installation contract

Add a required, bounded `remote_context_destinations` collection to the current installation input,
empty in the default generated declaration. Each optional destination contains a canonical public
HTTPS DNS endpoint, explicit region, public TLS roots, and positive request/response byte ceilings.
The unified installation accepts DNS names, matching its Model installation policy; the Egress
owner retains its own public-address validation at dispatch rather than inheriting a DNS bypass.
It also contains the existing closed HTTP credential-injection mapping by Secret purpose. The
shared pure injection type moves to Foundation with the new Context grant; it contains no raw key
or SecretBinding ID. Purpose and destination-header mappings must be unambiguous. At dispatch, the
currently authorized exact SecretBindings must match the installed purposes; a missing, extra or
revoked binding rejects before resolution. The public-document sample uses the empty mapping, but
the physical contract retains existing authenticated-provider support.

The local limits are at most 16 destinations, at most 16 KiB of public PEM roots per entry,
64 KiB request bytes and 1 MiB response bytes. The existing total input bound still applies. The
owning type must raise its string bound only as needed for those public roots and validate every
other field at its own narrower limit. Protocol and result-mapping digests come from the existing
closed Remote Context owners, not user-supplied names or arbitrary digests. Endpoint identity comes
from its canonical endpoint. Duplicate identities or conflicting entries are rejected.

The shared renderer emits the same pure destination-grant value to Egress and enables the existing
ContextRemote worker when entries are present. It emits the actual worker manifest and the owning
protocol/mapping identities. Compose, Helm and Native consume the resulting process closure and
credentials; none patches JSON or creates another worker configuration generator. The initializer
does not contact the document provider, create Context resources, or fabricate conformance evidence.
Destination input is frozen before installation and ordinary management requests cannot modify it.

Use a Context-specific Foundation physical grant, not a Model grant or Model adapter type. The
transport's pure configuration type separates deployment-controlled physical grants from
PostgreSQL's exact business closure. Root owns that upstream type and runtime cutover.
Installation tooling consumes the accepted type; it must not depend on an HTTP adapter merely to
construct configuration. Current tags/fields cut over together without a legacy exact-entry fallback.

## Dispatch authorization required with that cutover

Root's accepted metadata-only Context dispatch authority rereads the existing Context Query,
current Job/worker lease fence, Run, principal/tenant gates and exact deployed closure. It checks
current permission and frozen policy identities before Egress resolves DNS or materializes a
credential. No new business table, dispatch journal, reservation or write transaction is introduced.
The existing Security read role is used only for these current metadata reads.

The internal request needs sufficient current worker/fence and admission identity to bind that
decision; Root owns the exact fields and execution-version change. The external provider HTTP wire
remains version 1. Egress computes the canonical digest of the actual Inline query body and sends
that digest to Security for comparison with the frozen input content digest. The normalized query
and filter digests are distinct facts and are checked separately. Security receives no query body
and reads no RunValue or Artifact bytes. Authorization is then intersected with the installed
physical endpoint, roots, region, protocol and capacity grant. Changed lease, principal, closure,
body digest or missing destination rejects before any provider effect. Unavailability remains a
safe dependency failure; it is not interpreted as authorization or retried as a new request.

Do not reuse Model bootstrap Trust policy as a Context policy: its declaration names the model API
key purpose. Context parser, chunker, ranking, data and transport policies continue to be real,
separate ordinary Policy revisions referenced by the Context closure.

## Deliverable sample using ordinary publication

The sample should include the complete FullPlan source, input/output schemas and instructions for
Start → ContextQuery → ModelLoop → HumanTask → Return. A small source assembler may consume the
actual public authoring profile and slot-resolution inputs; it may not query private installation
files, choose business IDs or synthesize required-feature evidence.

The documented sequence is:

1. Deploy the existing document-review HTTPS server at the declared public destination with a real
   certificate and the checked-in corpus. Install that destination before the platform's first up.
2. Obtain the administrator session and public CA through the installation owners, then establish
   the ordinary public CLI connection. Configure the model, its finite quota and default normally.
3. Upload actual interface/implementation declarations and policy source bytes as Artifacts. Use
   ordinary create, validation, publish and deployment operations, retaining every returned exact
   reference, Receipt and ETag. Do not write PostgreSQL or process configuration from this sample.
4. Run a bounded live protocol qualification against that exact endpoint, checking real TLS,
   production request encoding/result normalization, corpus provenance, empty matches and rejected
   request shapes. Upload its actual result bytes as an Artifact and reference that available
   Artifact in the Context deployment's existing conformance field. A local fixture report or
   declaration digest is not evidence that this deployed provider passed. Existing owned protocol
   tests can inform this harness but cannot substitute for the live result.
5. Resolve the real Context deployment, ranking/authorization policies and model through the public
   authoring resolver. Compile and publish the complete source through the shared compiler and
   Registry; start a Run through the public API.
6. Read the resulting human work item and present the actual answer and citations. Wait for the
   user's explicit response using the existing task API. No harness supplies an approval on their
   behalf. Observe the original Run and result; retain failed or unknown attempts without replacing
   their request identities.

The complete source, ordinary publication constructors/instructions and explicit live protocol
qualifier are now present in the document-review example. The qualifier has no business IDs and
reserves/fsyncs its attempt and progress records before dispatch; failure preserves possible dispatch
and completed-case evidence. It only publishes conformance after all actual cases pass. The public
HTTPS endpoint and an actual human response remain external inputs; model-only, local-protocol or
transport tests do not complete that business journey.

## Review and evidence before implementation closes

Review the pure grant and installation input with the dispatch authority first. Then test default
deny-all, exact rendering across three consumers, missing worker closure, invalid roots/address,
duplicate/bounded inputs, current authorization and stale-fence/body/closure rejection before DNS.
Run the complete sample against real public Context and model deployments with a user response.
Retain separate installation, Context protocol, browser and full business evidence. This adds no
production HA/PITR/soak claim and no alternate business-state authority.

Accepted file split: Root owns dispatch authority and transport grant; Operations/Noether owns
installation input, shared renderer and its consumer tests after joint review; sample source and
public lifecycle documentation use those frozen contracts. There is no permission to modify an
already prepared installation or rebuild the J evidence in place.

Targeted evidence includes all three topology renderings and input consumers, strict root/selector/
credential mappings, shared compiler source and typed resource constructors, actual local corpus
TLS tests and Egress wire/authorization tests. The optional factory argument
`--remote-context-destinations FILE` consumes the same Foundation array; default remains empty.
The L image qualification enabled ContextRemote and passed first startup/readiness, read-only
verification and public CA delivery without sending a provider request. Same-volume container
reconstruction then failed; read-only diagnostics observed object GET failures before S3 volume
registration. The original failure, stopped containers and volumes remain preserved. The accepted
bounded read-only observation fix is included in M-final's separately passed Compose startup and
same-volume reconstruction, recorded in the [deployment evidence](deployment-review.md#m-final-compose-evidence).
Only L's exact empty network was removed with authorization to release an address pool; L was not
repaired or rerun. M enabled ContextRemote but made no provider dispatch. These records are independent
of J and do not qualify the live document-review journey.

The sample also exposed the intermediate ModelLoop response-schema defect covered by the
[separate joint review](model-node-response-schema-review.md). Source compilation and the local
corpus capacity fixture do not qualify that controller fix or a live document-review Run.
