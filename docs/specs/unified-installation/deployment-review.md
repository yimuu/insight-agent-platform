# Deployment and initialization joint review

Deployment lifecycle update: [ADR-0012](../../adr/0012-declarative-container-installation.md)
supersedes the host Compose/Helm controller and two-mode OpenBao lifecycle described in this
historical review. Current commands are in [installation](../../current/installation.md); earlier
wrapper test results do not qualify the declarative replacement. Other acceptance work below
remains separately tracked.

Status: accepted and implemented deployment sub-boundary after root cross-review. This temporary
record retains the initial findings, decisions and separately bounded implementation evidence.
Scope: shared installation inputs, role configuration generation,
one-shot setup, Compose/Helm composition and Console transport. Model management and Secret import
application workflows belong to their separate reviews. The architecture decision is
[ADR-0010](../../adr/0010-unified-installation-and-model-configuration.md).

## Initial findings before implementation

At the start of this work the existing pieces were reusable, but packaging them unchanged would not meet the requested entry
point:

- `deploy/dev/compose.yaml` starts only PostgreSQL, NATS and LocalStack. CLI `lib.rs` owns native
  child processes, fixed dependency addresses, identity generation and bootstrap ordering.
- `full_profile.rs`, `outbox_profile.rs`, `history_profile.rs` and `worker_profile.rs` already
  generate the real role configuration and executable-bound manifests. Their physical addresses,
  filesystem paths, errors and CLI process inputs are mixed together. JSON replacement after
  generation would invalidate the intended endpoint, certificate and digest relationships.
- `platform-dev-bootstrap` uses the real PostgreSQL development bootstrap transaction, including
  actual Scheduling Policy binding. `platform-bootstrap-operator` is a distinct installation-only
  command; calling it before the development transaction would create a partially different root.
  The new local composition must invoke the atomic development owner once with its reviewed inputs.
- The current developer token has `agent_author` kind and a 900-second lifetime. Its real binding
  explicitly excludes TenantManage, SecretInspect, SecretRotate and SecretRevoke. The new setup
  needs an explicit tenant administrator binding; it must not silently increase the permissions of
  existing native developer identities.
- Current native and Kind profiles still give some roles a test owner credential. The existing
  Artifact, Security Authority, Outbox and History grants are already separate. New serving
  composition must use non-owner credentials for the remaining roles before claiming no DDL access.
- The Console image is currently a `scratch` bundle artifact. The usable same-origin server is a
  loopback-only qualification helper in `apps/console/tests/gateway-server.ts`, not a deployable
  service. The role Helm charts already consume externally generated config digests and selected
  Secrets, and directly execute role binaries; they should keep that ownership.

## Module split

1. `crates/deployment/platform-deployment-contracts/src/installation.rs`: pure current-version
   bounded input/output types, closed role selection, path/reference validation and canonical
   identities. No filesystem, process, random generation, database or network operations.
2. `tools/rust/platform-deployment-tooling`: a reusable Rust library; the direct one-shot
   `platform-installation` binary lives in `tools/rust/platform-installation-tooling` so CLI rendering
   does not acquire its AWS SDK or PostgreSQL initialization dependencies. Move the existing profile generators and local identity/TLS
   material preparation here, separating deterministic rendering from I/O. The library depends on
   the existing domain capability catalog helpers and consumes owning role contracts; applications
   do not depend on tooling. CLI consumes this library for native rendering, keeping its native
   supervisor. Compose and Helm consume this same generator with their own typed topology inputs.
3. `platform-storage-tooling`: keep schema, role provisioning and PostgreSQL bootstrap here.
   Expose reviewed reusable command functions to the installation tool or invoke the existing
   direct binaries with explicit bounded inputs. Do not copy SQL or bootstrap business decisions.
4. `apps/console/server`: promote transparent transport behaviour into an explicitly owned service,
   with tests importing that service rather than a second implementation. Build a runnable image
   with the immutable Console bundle and this transport. It receives only its two Gateway targets
   and transport limits; it has no PostgreSQL, signing key or service credentials.
5. `deploy/compose` and the existing role charts: process/network/volume composition only. A Helm
   installation entry composes the same generated per-role artifacts and explicit initialization
   Job. Helm lifecycle hooks must not imply database rollback or automatic state replacement.

The first code slice is the owning installation types and shared renderer used by both CLI and the
installation tool. Private CLI helper facades are replaced directly, without compatibility reexports.

## Contract draft to freeze upstream

The durable-provider replacement is reviewed separately in
[the physical-provider review](durable-physical-provider-review.md). Its accepted initialization
boundary begins the platform's exact-resume guarantee only after an authenticated `ProviderReady`
observation. An initial OpenBao failure preserves the same files and volume with a safe incomplete
or unknown-outcome error. It does not authorize another self-init, a new cluster or a replacement
directory. This qualification is distinct from the earlier composition and role-restart tests.

The following concrete installation changes are submitted for final owner review before Rust edits:

- Make physical backend selection required and tagged in the current installation input. Retain the
  canonical Artifact object origin in network topology. The explicit AWS branch owns its KMS and
  Secrets origins; the local S3/OpenBao branch owns the OpenBao HTTPS origin. Remove the old
  unconditional KMS/Secrets topology fields instead of interpreting them differently per backend.
  Missing tags and old shapes fail closed. The shared renderer consumes the same pure OpenBao
  types as the physical adapter and does not depend on its HTTP transport.
- A bounded private provider-initialization journal binds input identity, installation identity and
  the fixed provider configuration. It distinguishes preparation, a durably requested first start,
  and an observed ProviderReady result. A first-start command persists the request before granting
  the invoking wrapper permission to start initialization. An interrupted or repeated request does
  not authorize another start: it can only inspect an already running matching instance or return
  incomplete/unknown. Successful authenticated inspection freezes actual cluster and mount
  accessors, exact Transit versions and the exact non-secret canary version.
- Preparation freezes two server configurations: first initialization and ordinary serving without
  a self-init stanza. All restarts after ProviderReady use the latter. This prevents a missing Raft
  volume from silently becoming a new cluster before the verifier discovers the mismatch. Compose
  and Helm select only those fixed modes using the original private journal; a values boolean or
  stale Job result cannot substitute for it. No candidate-directory publication protocol is added.
- Initial self-init creates the closed mounts, two separate reference-encryption keys, cert roles,
  exact policies and a non-secret canary. Every cert role disables the default token policy. The
  initializer certificate has only the read permissions needed to verify this setup; serving gets
  its own certificate and exact role policy, with no seal, root token or issuer key. ProviderReady
  alone does not publish role configuration or mark the whole installation Ready.
- After ProviderReady, the existing dependency/schema/role/bootstrap progression continues and
  preserves legitimate business changes. Verify may obtain a short-lived authentication token and
  permit the provider's own authentication lease/audit effects, but performs no provider
  configuration, business Secret or object write. Local Compose and local Helm share the same
  private-volume static-seal trust boundary; production consumes explicit deployment trust inputs.

Names below are proposed Rust owner names, not a new public management API. Every persisted envelope
is current-only, has `schema_version: 1`, rejects unknown fields and passes strict JSON limits before
typed decoding. Input and non-secret manifest limits are 256 KiB; individual role files retain their
existing owning limits. Role, endpoint and credential collections are bounded by the installed
closed role/use catalogs, reject duplicates, and cannot introduce arbitrary environment entries.

| Type | Meaning and constraints |
| --- | --- |
| `InstallationInputV1` | Explicit `environment_class=development`, topology, selected existing `ComponentRole`s, installation name, immutable package identity, auth mode and physical dependency inputs. The name is a bounded deployment label, not a tenant/resource identifier. No raw secret values or user business resources. |
| `NetworkTopologyV1` | A closed topology kind (`Native`, `Compose`, `KubernetesLocal`), per-role listen sockets, typed exact service origins/TLS server names, PostgreSQL and NATS endpoints, browser Console origin and browser object origin. Internal endpoint and browser endpoint are distinct fields, not interchangeable strings. No userinfo/query/fragment or arbitrary proxy path. TLS peer URI identities retain their current owners. |
| `RolePathsV1` | Per-role absolute paths for config, its credential mount and writable temporary/state directories. Reject relative traversal, path overlap across secret scopes and a serving mount containing the installation root. Container defaults are fixed; native supplies its private project paths. |
| `CredentialReferencesV1` | Closed credential use and consuming role, plus a private file reference. Public CA/JWKS may be shared; private keys, database passwords and provider credentials are granted only to listed physical roles. Never embed values in generated Compose, ConfigMaps, metadata manifests or stdout. Deployment-only issuer/CA/admin credentials cannot be referenced by a serving role. |
| `InstallationIdentityV1` | Persisted installation nonce, actual tenant/principal/request/policy IDs and public identity digests established before an external write. New installations explicitly contain a real `tenant_admin` principal/binding separate from installation operator and existing native `agent_author`. Reuse nominal ID types; do not derive business IDs from display names. |
| `ExecutableCatalogV1` | Exact selected role, executable byte SHA-256, immutable package/image identity and owning supported capability catalog. Hash actual installed executable files from the selected image; retain the existing startup hash/catalog verification. Never use version text, image tag or adapter semantic digest as executable evidence. |
| `RenderedInstallationV1` | Non-secret manifest of each role's config digest, referenced credential uses, public TLS fingerprints, executable identities and dependency edges. Render only from complete resolved inputs. This manifest is deployment evidence, not a second record of business resource current state. |
| `InstallationProgressV1` | A private local setup journal binding the immutable input digest and installation identity to completed setup effects and exact external references. It contains no resource current projections or user keys. State survives failures and is fsynced before the corresponding effect. |
| `InstallationError` | Closed safe codes: invalid input, unsupported topology/role, identity/config drift, foreign state, credential invalid, prerequisite unavailable, current schema mismatch, external outcome unknown, conflict and incomplete. Diagnostic context is a safe field/role/stage; no raw external error body, token, DSN password or private path contents. |

The renderer takes typed resolved inputs and returns bytes/digests without changing them. Entropy,
file reading, external observation and executable hashing are separate preparation adapters. It
must not accept a generic arbitrary JSON patch or a caller-supplied capability claim as supported
catalog. Internal addresses need not be globally unique: separate containers can use the same port;
native loopback listeners retain the existing uniqueness requirement.

### One-shot lifecycle and recovery

`platform-installation prepare` reads declared inputs, acquires the installation filesystem lock,
loads or creates its private identity once, and writes dependency files needed before services can
start (for example NATS TLS). Secret files are regular, bounded and private; reject symlinks and
hard links, persist with atomic replace and fsync, and never mount this whole root in a serving role.

`platform-installation provision` holds the same ownership guard, validates prerequisites and
invokes the existing owners for current schema, limited database roles, physical development
S3/KMS/Secret prerequisites, bootstrap and the exact JetStream stream. It persists actual external
references, completes role rendering and publishes the ready manifest only when the entire selected
closure validates. Setup success does not claim every role is live or ready.

PostgreSQL provisioning is fresh-only. Resume accepts an exact current schema only for the same
owned installation, invokes the owning idempotent bootstrap and preserves current tenant bindings,
credits and business data. Arbitrary schema verification failure does not trigger repair/reset.
Concurrent invocations must not race separate identities into a shared database: private installation
serialization and the existing PostgreSQL bootstrap transaction both apply. Unknown or foreign
nonempty state fails closed.

For S3/KMS/Secret setup, persist the fixed operation identity before a write and verify exact
provider references after a known or uncertain response. A provider operation without an idempotent
request token cannot be blindly retried after a lost response. If exact bounded readback cannot
establish the owned result, report `external_outcome_unknown` and preserve the journal; do not create
another key, silently choose one of several matches or mark the step complete. No new business table
or fallback queue is needed.

`platform-installation verify` is read-only: verify saved input/config/identity, current schema and
already installed physical prerequisites. A stopped or running restart does not rewrite credentials,
role permissions, CORS, policy bindings or files. Compose starts fresh initialization explicitly and
starts serving only after successful verification/provision. Recovery of an incomplete initialization
is explicit and uses the same journal; starting an already configured installation is not permission
to reapply grants.

`platform-installation session` explicitly signs a new 900-second token for the saved initial
tenant-admin subject using the existing RS256/JWKS verifier model. Deliver via a private 0600 file;
ordinary output reports the delivery path and expiry, never the token. The signer is a one-shot tool
with exclusive access to issuer material. Console stores the pasted token in memory. Bootstrap
authority is used only to establish the binding; issuing an ordinary session cannot create a new
principal or rewrite/re-enable a revoked one. Native developer session semantics stay unchanged.

### Database and role privileges

Use the current Artifact four-role, Security Authority, Outbox and History grants without broadening
their rights. Add one reviewed local non-owner runtime DML role for the remaining development
processes: no schema/table ownership, CREATE/CREATEDB/CREATEROLE, superuser, bypass-RLS, inherited owner
membership or unrestricted SECURITY DEFINER execution. Grants must enumerate the current required
tables/columns/functions in the PostgreSQL owner, with current schema verification tested under that
role. This development role is not a claim of production per-domain least privilege.

Extend the provisioning tool with an explicit typed container/local-Kubernetes target policy; its
existing fixed native/Kind loopback checks must not become an unrestricted URL environment variable.
Admin credentials are mounted only into the one-shot provisioning process. The serving image may
contain unrelated binaries, but serving process credentials and mounts must make initialization
operations unauthorized. Restart must detect unexpected role membership/ownership instead of
silently correcting it.

### Physical topology, object transport and Console

The minimum real model closure includes Security Authority and Egress Broker as well as both
Gateways, orchestration, Registry validation, Artifact Gateway/data/maintenance, native capability,
Model worker, Outbox and History. Retrieval additionally requires its actual Context roles. MCP and
Sandbox remain explicit selections; the Compose topology rejects Sandbox rather than substituting
host execution. Each role directly execs its binary with bounded pools and its own startup/readiness
checks. NATS starts after prepare, stream provisioning after NATS readiness, Outbox after that exact
stream exists. No runtime process gets stream-creation credentials.

TLS server SANs use the actual service DNS names. A shared CA does not mean shared leaf private keys.
The public Gateway and Management may retain their already reviewed shared Artifact client identity;
Registry retains its separate validation-only client. Dependencies are internal except Console and
the explicitly selected browser object endpoint; there is no need to publish PostgreSQL or NATS.

S3 signatures bind the host/port. A URL for `localstack:4566` is not a browser URL, and rewriting an
already signed URL is invalid. Prefer one canonical development object HTTPS origin reachable both
from the browser and through the container network alias to the same physical service, with a valid
certificate; any distinct signing endpoint design requires the Artifact provider's owning review.
KMS/Secret endpoints can remain internal. CORS belongs to the exact configured Console origin and
the existing PUT/content-type contract; first setup installs it, later verification rejects drift.
Do not weaken TLS, add broad origins or route object bodies through the Console server to avoid it.

The Console transport takes a bounded current-only config with its listen address, exact two
Gateway origins and body/time/connection limits. It preserves Authorization, Receipt, ETag,
Last-Event-ID and SSE cancellation/backpressure, and never retries a mutation itself. Validate
fixed configured origins; do not accept a request-selected upstream or forward arbitrary absolute
URLs. Backend TLS is verified when selected; development internal HTTP is an explicit topology
choice. The process has only bundle read access and no installation directory. Helm routes the
same browser origin through this transport and consumes the same generated role files/digests.

## Evidence required before calling this current

- Deterministic renderer tests with native, Compose and local Kubernetes inputs; actual owning
  decoder validation, exact role closure and real binary/catalog identity; missing/extra role,
  unsupported destination, duplicated credential use, wrong SAN and path escape rejection.
- Real fresh PostgreSQL setup, replay, crash between phases, simultaneous initializers, foreign
  state/config rejection and preserved current Scheduling Policy credit. Under serving credentials,
  DDL, owner membership, unauthorized SECURITY DEFINER calls and access to other restricted roles
  fail; allowed commands and schema verification succeed.
- Private-file permission, symlink/hardlink and interrupted persistence tests; signing keys never
  appear in serving mounts. Tenant-admin token maps to its real binding; expiry/renewal work without
  granting installation authority or changing the native agent-author contract.
- Real Compose startup from built images without host Cargo/Node, no socket mounts or supervisor,
  complete readiness and same-origin browser Artifact upload/SSE. Restart retains IDs, object/key
  references, credentials, current resources and default selection. Drift fails without repair.
- HTTP transport tests for Receipt/ETag passthrough, upstream failure, bounded payloads, mutation
  response loss without replay and cancellation releasing an SSE upstream. Image contains a real
  entrypoint as well as the verified static bundle.
- Helm render/deployment checks validate the same generated configuration and minimum mounts;
  actual local cluster and Sandbox evidence remain separate from Compose and production qualification.

No code, schema or live service mutation was made during this review. The root accepted the new local tenant-admin bootstrap input and the
non-owner development runtime DML grants before implementation. All other work replaces physical composition or extracts
the existing owner implementations.

## Accepted file-level implementation contract

The deployment contract uses `InstallationProcess` for physical instances. Existing `ComponentRole`
is a release family (for example, several Context executables share one family); it cannot identify
a per-process private mount. The closed installation process maps to the current binary and existing
worker executable catalog without changing any business role, manifest wire or permission meaning.

`installation.rs` owns bounded topology, process paths, credential references, setup phases and safe
errors. `platform-deployment-tooling` owns deterministic renderer and shared local identity material
preparation, with no dependency on the CLI. Its one-shot command invokes storage-tool binaries; the
CLI can link the renderer without a transitive PostgreSQL dependency. The accepted native consumer refinement moves ordinary native supervision to the deployment
launcher consuming the same frozen plan; the CLI keeps public management and explicit AWS fixture commands. Storage-tooling owns all admin SQL/role changes.

### Accepted initialization recovery refinements

The physical initializer is a separate `platform-installation-tooling` executable; the CLI links
only the shared renderer and identity library. The installer freezes its SecretProvider identity
before external effects. The accepted durable-provider refinement selects persistent S3 plus OpenBao, freezes the
nonce-scoped bucket and exact cluster/mount/Transit/KV identities, and separates ProviderReady from
platform authority bootstrap. The durable physical provider review records the initialization
Unknown boundary; no LocalStack fallback or repeated self-initialization is permitted.

An interrupted first schema request may recover by read-only current-schema verification only when
the prepared input, random private administrator credential and installation-owned PostgreSQL
volume/service identify the same isolated database. The Compose entry must establish that ownership;
local Kubernetes requires an equivalent exclusive PVC binding. A reachable shared or unknown volume
is insufficient. A known failed provisioning result remains failed and cannot become adoption on
retry. Recovery also verifies the same installation's role markers and exact effective grants.
Completed initialization uses read-only bootstrap verification; it cannot re-enable revoked users,
reset Scheduling credit or restore old defaults.

Model seed declarations use real versioned S3 bytes, verified with the Artifact provider's exact
HEAD/read and digest checks before the atomic seed transaction. They do not add a built-in plaintext
Artifact backend or pretend that a trusted local seed is scanner conformance evidence. Unknown
physical outcomes retain their frozen intent and exact-object recovery boundary.

The shared Model adapter declaration is owned by `ModelProviderWireProtocol`: its semantic digest
covers the closed wire protocol/name, canonical input/output and normalized stream ABI, endpoint
path and protocol version. Executable identity remains in the actual WorkerManifest. Installation
inputs require explicit HTTPS model destinations; the shared producer and physical adapter consume
the same declaration. No default external URL or provider qualification is invented.

JetStream initialization uses the existing separate provisioning identity. The closed `create`
command verifies the installed stream afterward; `verify` uses the worker's exact stream contract
reader and sends no create/update operation. A recorded interrupted create is resolved by readback,
while configuration drift remains an error. The current command reports no typed rejection/timeout
distinction, so a nonzero create keeps the Requested intent; retry is readback only.

### Session delivery and model endpoint prefix closure

Root cross-review accepted the current `InstallationSessionDeliveryV1` metadata envelope on
2026-09-10: its input and prepared identity digests bind the existing tenant, Console origin,
expiry and private token-file handoff to the completed installation. Token bytes never enter
this envelope. Kubernetes delivery uses an explicitly owned nonce Pod without API credentials;
only the host operator retrieves the two fixed files over exec, persists the token privately and
removes that exact Pod using a UID precondition. Unknown delivery resumes its original Pod;
renewal after expiry is an explicit session operation. Ordinary verify never issues a session.

`CanonicalHttpEndpoint.base_path` is an installation-selected prefix. It is not the protocol's
fixed request path. Installation validates the owning endpoint and HTTPS/public-host limits;
the physical adapter appends its own closed `/v1/responses` or `/v1/messages` path. The reviewed
DashScope installation therefore freezes `/compatible-mode` and region `cn-beijing`, without
making claims about provider training, retention or subprocessors.

### Accepted local image retention and installation evidence

Root accepted a per-installation cache tag for each already-verified immutable image. The tag only
keeps local Docker content reachable after another build retargets an unrelated build tag; it is
never an execution or package identity. An existing installation tag with a different image is
rejected. Runtime and Console references in the frozen declaration remain immutable.

The independently selected installation CI lane builds actual images, then uses a new isolated
Compose project to test initialization, all role and Console readiness, read-only verification,
configuration drift rejection without repair, and stopped-role restart with unchanged installation
identity and configuration. The fixture alone restores the exact bytes it changed for the negative
case. The harness removes only its label-verified task resources and retains a safe report; a passed
result is emitted after cleanup succeeds. This is local installation evidence, not production
capacity, cloud-provider or Kubernetes qualification.

### Accepted issuer and leaf certificate distinction

Root cross-review accepted distinct CA and workload subject names after the actual Linux
OpenSSL verifier rejected the same-name chain even though its signature, authority key identifier
and DNS SAN were correct. The shared generator retains the exact SAN, EKU, key usages, CA and
SPIFFE checks; common names do not grant authority. Existing private certificates are not repaired.
The certificate fixture consumes this shared producer and exercises the actual AWS SDK with the
correct root, wrong root and wrong hostname. Kind additionally checks its actual service-DNS leaf
with Linux OpenSSL 3 and the one-shot SDK operations.

### Accepted provider consumer closure and qualification cleanup

The root final review accepted the host Helm sequence from a durable provider-start Job intent
through one bare initializer Pod, actual ProviderReady, UID-precondition deletion, clean exit, and
atomic removal of the exact evidence finalizer before serving. Missing identities, extra processes,
foreign finalizers, restart counters and capacity drift fail closed. Read-only observation Jobs may
be repeated only after the exact original Job/Pod reports a closed temporary observation error;
they never issue a new initialization permission. Pending platform provision/verify Job identities
survive intervening dependency phases. Provider callbacks are clipped to their remaining absolute
wait budget.

Full installation qualifiers preserve the original resources and private recovery material on
failure. Only a passed qualification or an explicitly authorized exact cleanup deletes them.
The Compose fixture installs one explicit qualification-only destination to exercise the internal
model Policy Artifact stage/readback path; it imports no model key and sends no provider request.
Its controlled recreation retains all dependency volumes and checks the frozen declaration and
material again. This does not expand claims to power-loss, production or full browser qualification.

### Accepted dependency shutdown protocol

Root jointly accepted the shared SeaweedFS shutdown grace and the fixed NATS image's normal SIGINT
protocol for Compose and Native. The S3 duration is owned once by the shared producer and is a
required field of its Helm plan; neither consumer substitutes a missing default. Kubernetes does
not gain an unsupported stop-signal feature. The previous NATS SIGTERM exit code was the pinned
server's deliberate protocol behavior, not evidence of damaged state.

The Compose qualifier stops serving processes first, then NATS, S3, OpenBao and PostgreSQL. It checks
each original container ID and an actual zero exit status before removing any container. A failed
stop or changed identity preserves every original container and volume and fails qualification;
it is not retried as a successful recovery. The new pair must pass controlled reconstruction before
the earlier failed installation can be superseded as evidence.

### Accepted normal-provider readiness observation

The actual Helm run reached ProviderReady through its original initializer, stopped that exact
Pod successfully and started the ordinary OpenBao Deployment. Its subsequent platform provision
returned PrerequisiteUnavailable before the S3 journal existed, immediately after TCP readiness.
Root jointly accepted a separate current-only `serve_observe_intent` in the private Helm state.
It uses the existing provider-observe Job and does not change the initializer's readiness proof.
Any entry-existing intent only resolves its old result; a current fresh observation must follow.
Only closed temporary read errors permit another observation, within one total deadline. The
ordinary Pod/container identity and its fixed owner/input/image/serve command are checked before
and after the current observation. Pending platform provision identity remains unchanged.

The strict state field is added before its consumer; old missing fields are rejected and failed
fixtures are not patched. Tests cover old completed/pending intent, unknown outcomes, replaced
provider identity, failed error classes and timeout. The final I pair passed a fresh Kind run through
the new gate, base readiness, exact-DNS TLS positive/negative checks, read-only verification, one
Gateway Pod replacement and private session delivery. The test cluster was cleaned. This does not
claim whole-dependency Kubernetes recreation or model Policy bootstrap with the empty model catalog.
The previous failed attempt remains separate.

The same pair's Compose first attempt failed before retained service or volume creation; its original
cause was not captured and remains unknown. An explicitly authorized resume reused that exact input,
image pair and private directory. It passed initial provisioning, internal model Policy Artifact
stage/readback, drift rejection without repair and read-only verification. All original serving and
dependency containers exited successfully before removal; new containers on the original volumes
became ready and preserved the frozen installation JSON digest. The separate successful resume
report does not replace the first failure report. The qualifier now records only a fixed Docker
address-pool-exhausted code when that exact diagnostic appears, without retaining raw process output.

### Accepted exact public trust delivery

Root and Operations jointly accepted a separate read-only `public-trust` command after the final
user-path review found that container users had no verified way to obtain the installation CA.
The owning `InstallationPublicTrustV1` envelope contains only its schema version, frozen input and
identity digests, one bounded public certificate PEM and the digest of those exact PEM bytes.
The owner requires Ready and reuses current public-identity verification. It acquires the existing
installation lock using a read-only descriptor, without creating or fsyncing files; all directory
write methods reject this mode. It neither renews a session nor accesses a provider.

Each host consumer verifies the same envelope against its frozen installation before atomically
publishing `public-ca.pem`; a pre-existing different or foreign file is never replaced. Compose and
Helm mount installation state read-only for this command. The path and PEM file digest are safe output,
but export alone is not proof of TLS, endpoint routing or SAN verification. Browser trust remains an
explicit operator choice. Tests must cover pre-Ready state, identity and certificate drift, bounded
and closed decoding, private-key fields, foreign output files and an actual read-only mount.

The host binding comes from the existing successful provision/verify Ready envelope, retained as
immutable `ready-owner-proof.json`, or Helm's existing Ready result. Export cannot establish its
own identity. Existing installations without that host evidence require explicit verification;
it may retain the binding locally but does not renew a session or mutate provider/business state.
File publication uses an atomic no-replace link and rejects foreign bytes, including a concurrent
destination creation. A crash with multiple links fails closed.

The J pair passed actual fresh Kind installation and a separate read-only export Pod. The installed
CA file kept its inode, modification time and bytes; the session digest and workload/PVC/role-file
snapshot were unchanged, and the exact export Pod was removed. Native/Compose/Helm consumer and
owning Rust tests passed. J Compose qualification retains its initial unclassified failure and the
first explicit resume's confirmed Docker address-pool exhaustion. After separately authorized
removal of exact empty networks from stopped task-owned installations, the same J input and images
passed a second explicit resume. Its actual read-only mount rejected pre-Ready export with no JSON,
then exported the Ready CA without altering any private file hash or the delivered CA. Full serving
and dependency containers stopped with zero exits, were recreated on their original volumes and
preserved frozen state; exact fixture cleanup passed. Neither successful report replaces earlier
failures. The reported SHA-256 identifies
the original PEM file bytes, not the DER certificate fingerprint shown by browsers.

### Accepted observation of an already verified model object

Root and Operations jointly accepted a bounded read-only observation after the L fixture's
controlled container recreation exposed a gap between S3 listening and its volume registration.
The retained attempt failed while reading the original exact model Policy Artifact generation;
its completed installation and object journals were unchanged. This is evidence of read
unavailability, not proof of permanent data loss or permission to recreate the object.

Only `Mode::Verify` with an already `Verified` object journal may repeat the existing complete
`verify_staged_bytes` operation when it returns `StorageUnavailable`. One 30-second deadline
bounds all calls and their one-second intervals. Input, identity, credential, seed, locator,
generation, content and readback evidence checks remain unchanged. Other errors retain their
original classification and return immediately; expiry remains `PrerequisiteUnavailable`.
Requested or Staged objects and readback following a new write use the original single call.
No write is repeated, no journal is repaired and no old proof substitutes for a current successful
read. Tests must exercise this exact gate, the single deadline, immediate errors and unchanged
first-write behavior. L's failed report and resources remain intact pending explicit action;
this acceptance does not qualify new image bytes or a successful recovery.

### M-final Compose evidence

The final M image pair and its actual runtime-source/executable and Console byte inventories are
frozen in `/private/tmp/insight-unified-m-final-image-pair.json`. The separate result is
`/private/tmp/insight-unified-m-final-compose-explicit-resume-report.json`; its image references
match that immutable build manifest. The build manifest is not rewritten to record a later result.

The first M attempt failed with the closed Docker classification `address_pool_exhausted`, before
provider initialization or installation identity allocation. Its original failed report remains
unchanged and is digest-bound by the later report. Root authorized removal of only L's exact empty
network after checking its identity, empty endpoints and prior clean stop. The independent release
intent/result agree; L's stopped containers, volumes, private material and failed report remain.
There was no global prune or replacement installation. One explicit resume reused M's original
input bytes, project and image pair.

That resume passed all selected roles, ContextRemote and Console readiness, pre-Ready public-CA
rejection, actual read-only CA export with immutable delivery, read-only verification, and rejection
of configuration drift without product repair. The fixture restored only the bytes it changed for
the negative case. It cleanly stopped and recreated serving/dependency containers on the original
volumes, reached Ready again, and preserved identity, CA, configuration and completed journals.
The actual internal model Policy Artifact was staged and read back at its exact generation. Exact
cleanup of M's own successful fixture completed; both first failure and explicit-resume success
reports are retained.

This is Compose startup and controlled dependency-reconstruction evidence, not a first-try pass,
power-loss/Kubernetes qualification or recovery of user model business state. The fixture sent no
external Model or Context request and performed no browser signed-S3 upload. Native M's separately
failed real Run is not promoted by this report. Native N's separate evidence follows; real retrieval
and a human response remain absent.

### Native N model and business restart evidence

Native N completed one actual Qwen request: the Run and its unique ModelTurn succeeded, the typed
result passed ordinary public readback, and accounting recorded 1,159 ProviderReported tokens with
no remaining Model reservation. Provider cost is unknown; the recorded zero cost is not a claim
of free service. The original harness report remains `failed / no_completed_model_turn` because
the expected public Model events were absent. Supplemental read-only evidence does not turn that
original report green or repeat the provider call. The accepted
[event projection/watch repair](model-public-event-review.md#recorded-local-evidence) passed local
PostgreSQL/HTTP regressions; the later O build, actual Run and business restart have their own
evidence below and do not change N's original report.

The evidence directory is
`/private/tmp/insight-native-cli-model-review-20260910/workspace-n/evidence`.
`supplemental-success-summary.json` binds the retained original report and public result readback
by byte digest; those references were independently checked. It is a pre-restart snapshot, so its
`restart_after_verified` remains false. The later, separate `restart-comparison.json` records all
13 business/identity comparisons equal, the same before/after state digest and no new execution
request. The checks cover original Run/result, resources, credentials metadata, default and quotas.
No provider text or credential is copied into this document.

Root stopped the four exact Native dependency containers normally and verified Exit0, retained
all containers and data, then resumed the same installation input and frozen package to Ready.
`/private/tmp/insight-unified-native-20260910-n/business-restart-stop-result.json` records that stop;
the sibling `business-restart-frozen-before.json` and `business-restart-frozen-after.json` agree
on all 38 frozen files, including identity/configuration and public CA. This is controlled Native
stop/start and business-state preservation, not container replacement, crash-durability or HA.

The immutable N image pair is separately bound in `/private/tmp/insight-unified-n-image-pair.json`.
`/private/tmp/insight-unified-n-consumer-smoke-report.json` passed the current input factories,
Compose/Helm plan consumers and negative cases with network disabled. Its manifest byte digest
matches; it made no provider call and did not run a Native plan or repeat physical reconstruction.
The build manifest's original pending qualification marker is unchanged. M's Compose physical
evidence remains bound to M; neither offline N checks nor Native business recovery qualify N
Compose/Kind, real retrieval/human approval or browser signed-object transport.

### O current delivery evidence

The final Linux arm64 image build and offline typed consumers passed. The immutable evidence index
is `/private/tmp/insight-unified-o-evidence-index.json`, with the image pair, source inventory and
consumer report beside it. Its nine artifact references and two sealed upstream evidence references
were independently verified by byte digest. The 1,007 source inputs were unchanged across the build;
30 actual runtime binaries and nine Console image files were checked. These are image and offline
consumer results, not an O Compose/Kind installation or a Linux model transaction qualification.

The separately frozen Native O package completed a real public CLI journey. Its report is
`/private/tmp/insight-native-cli-model-review-20260910/workspace-o/evidence/report.json`.
It passed in 6.578 seconds: the Run and unique ModelTurn succeeded, the typed result matched, and
the sole attempt recorded one sent request with 1,089 ProviderReported tokens. All Model quota
reservations returned to zero. Provider cost remains unknown; recorded zero cost does not mean
the service was free, and this one success is not a provider capability conformance claim.

The sibling `events.json` contains nine consecutive public sequences, including Model start and
completion. The CLI also drained the Node completion after Run completion, directly exercising
the repaired terminal-page behavior. `readonly-model-state.json`, `pre-execution-default.json`
and `execution-intent.json` retain the exact original attempt and readback context. No second
provider request was needed to verify the result. The initial O startup address-pool failure and
the earlier M/N failures remain separate retained evidence; none is rewritten by this success.

O's same-input/package business restart passed. The final safe index is
`/private/tmp/insight-unified-o-native-evidence-index.json`; all 34 artifact byte digests were
independently checked. Both before/after snapshots made 13 public GETs and no execution request.
All 13 comparison domains and their state digest match; all 38 frozen private/configuration files
are unchanged. The four exact dependency containers stopped with Exit0, retained their data, and
the original input/package returned to Ready. The actual CLI read the same nine public events after
restart, including the tail after Run completion. This is controlled Native business recovery,
not a new model invocation, container replacement, power-loss test or current-image Compose/Kind
qualification. Earlier M/N records remain evidence only for their own packages and installations.

The accepted product implementation is present. Remaining full-scenario evidence is physical
Compose/Kind on the current image pair, the actual public retrieval endpoint and
conformance/exact resource chain with a user-supplied human response, and browser signed-S3 upload
under real certificate trust. The unaccepted abandoned-upload proposal is a separate known gap,
not a prerequisite added to the accepted mainline; no expiry cleanup is claimed as implemented.
