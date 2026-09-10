# Abandoned public upload: review proposal

Status: diagnosis and proposed contract only. The CA propagation repair is independently tested;
the timeout/cleanup changes below are not implemented or jointly accepted. Native D remains a
retained failed attempt. A new installation is independent evidence, not recovery of D.

## Observed failure and current owners

The D CLI prepared an Artifact, then failed before recording a successful HTTPS object PUT.
After the original grant expired, authenticated public reads still returned Artifact `staging`
and Operation `waiting`, both version 1. No Agent or Run was created. The local journal is not
proof that the provider received no object. It retains the original capability and is not a
public diagnostic artifact.

The relevant current owners are:

- [Artifact commands](../../../crates/domains/platform-artifacts/src/lib.rs), including prepare,
  complete, retention admission and staging quota inputs.
- [Artifact work](../../../crates/domains/platform-artifacts/src/work.rs) and
  [PostgreSQL implementation](../../../crates/adapters/platform-postgres/src/artifact_repository.rs).
- [S3 upload and exact-version storage](../../../crates/adapters/platform-artifact-broker/src/aws.rs)
  and [Broker authorization](../../../crates/adapters/platform-artifact-broker/src/lib.rs).
- [Artifact Gateway](../../../apps/services/platform-artifact-service/src/bin/gateway.rs),
  [public Operation reader](../../../crates/protocols/platform-api/src/operation.rs), and
  [existing architecture](../../adr/0010-unified-installation-and-model-configuration.md).

Prepare inserts the public `artifact_scan` Operation as `waiting`. Its deadline and the grant's
expiry derive from the same original deadline. Complete rejects an expired grant. Generic claim
only admits Ready/RetryScheduled Jobs before their deadline. Artifact lease recovery selects only
Leased/Running/Cancelling Jobs with expired leases; public finalization selects Verified objects
with a still-live deadline. Neither handles an uncompleted upload. The Operation router exposes
only GET; a permission registry entry named `operation.cancel` is not an implemented cancel API.
Retention deletion admits only Ready/Rejected/Quarantined/Corrupt Artifacts whose Blob is
Verified/Corrupt. It cannot collect this Staging pair. The reservation is settled during
finalization, so this path also retains the original staging quota.

This is an implementation gap, not normal completed recovery. Waiting longer or renewing a
session does not extend the original upload grant.

## Physical facts that constrain a repair

The public presigner uses the exact tenant/Artifact/Blob-derived object key, fixed Content-Length
and optional Content-Type. It does **not** set `If-None-Match`, and its input does not include a
body checksum. Expected content digest remains a PostgreSQL/verification fact; it is not a
presigned payload commitment. The internal `stage_bytes` producer does use `If-None-Match: *`;
that property must not be transferred to public uploads by inference.

The provider returns only the URL, not an explicit required-header map. A future signed-header
change must update the owning upload response and both actual clients, or establish through the
SDK and real provider that their existing headers satisfy the signature. Do not silently add a
required header which the Console or CLI does not send.

Public presigned URLs can be used repeatedly during their validity; with versioning this can
create multiple versions of one owned key. AWS documents repeated use and request-start expiry
checks. Its example of a transfer continuing after expiry is a download; a PUT drain bound must
be proved for each installed provider, not inferred from that example.
[AWS presigned URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html).

The sealed locator authenticates the backend, exact object key and storage binding under the
existing tenant/Blob/encryption-domain AAD. It does not contain a generation at prepare time.
Current complete can HEAD the exact owned key and freeze the returned nonempty generation, then
scan that version. This is an observation of one current version, not evidence that no earlier
version or admitted in-flight write exists. Current `complete_current_upload` also collapses a
missing object into StorageUnavailable; it is not an authoritative absence API.

Consequently, expiry plus one HEAD 404 cannot justify Deleted or quota release. A latest-object
DELETE, prefix deletion, invented generation, or a wait chosen without a provider contract is
unacceptable. A successful exact-version DELETE alone does not prove all versions are gone.

## Smallest complete business closure

Add a bounded **Artifact-owned public-upload deadline transition**, driven through the existing
Artifact Gateway periodic owner and PostgreSQL transaction boundary. It is distinct from worker
lease recovery and never adopts internal AwaitingStage producers or scan/finalize Jobs.

The command re-locks the original Artifact, Blob, grant, Operation and quota account in the
existing upload lock order, then validates all original identities, payload digests, versions,
public-upload origin and the database clock. Only an unchanged Staging/Waiting/unconsumed closure
whose original Operation deadline has passed is eligible. A committed complete wins through the
same locks and causes the timeout command to reject or return its recorded outcome. There is no
caller assertion that a PUT was absent and no new public receipt that replaces the prepare.

The proposed atomic result is: original Operation TimedOut, Artifact Rejected, grant Expired,
with original Blob still Staging until its physical disposition is known. The original Job's
trace/owner/request remain intact. Record an idempotent owner Receipt, Event/Outbox and one
durable cleanup/reconciliation Job in this transaction. Existing Job/Artifact/Link states can
express these facts; do not invent Blob Quarantine, mark unread bytes Corrupt, or mark them
Verified. A timed-out upload is never successfully published merely because bytes later arrive.

Keep the original staging reservation while physical occupancy is unresolved. Its amount comes
from the existing immutable reservation ledger, not a client estimate or the currently observed
one version. The cleanup correlation must settle/release that original reservation exactly once
under the quota account lock and unique ledger guard. Never reset used/reserved counters.

The bounded scanner uses the existing safety-scan cursor/shard/slot conventions and finite page
limits. Both scanning and mutation are restartable from PostgreSQL. Unknown commit outcomes use
the recorded command identity; they do not generate a second timeout or cleanup intent. No
PostgreSQL transaction spans S3 or key-unsealing requests.

## Physical cleanup and uncertainty

Reuse the existing Artifact maintenance executor, Job lease fencing, Broker and exact-version
delete evidence. The existing `ArtifactBlobCleanupSnapshot` specifically means verified
deduplication and requires a real replacement Blob; it cannot be filled with a fabricated
replacement for abandonment. Its owning payload needs a reviewed tagged cleanup purpose, with
the current deduplication meaning retained as an explicit branch and an abandonment branch bound
to the original Artifact/Blob/Operation/grant/reservation. This is one Job-owned progress record,
not a new table or independent business authority.

The abandonment branch may advance only from typed provider evidence:

1. Unseal through the existing Broker with the exact stored context. Resolve only the installed
   storage binding and this exact owned key. Reject locator, backend, tenant or key drift.
2. Obtain a bounded inventory of **all versions and delete markers for that exact key**, and
   persist a bounded page/cursor and exact work identities before deleting any version. If the
   provider API uses a prefix, require every returned key to equal the one authorized key and
   reject other keys. No bucket-wide enumeration or payload/body read is necessary. This is a
   new narrow physical capability and requires explicit installed IAM/readiness evidence; the
   current HEAD method does not provide it.
3. Delete only frozen nonempty exact generations using the existing non-marker delete checks.
   A lost DELETE response is resolved by exact-generation readback. Persist progress with the
   current maintenance Job lease/version; a stale worker cannot settle quota or declare cleanup.
4. Claim final absence only when provider evidence also establishes that no authorized write can
   still commit, and a complete bounded inventory is empty. Retention, holds and any applicable
   approval constraints must be rechecked by the Artifact owner; abandonment is not an implicit
   authorization to bypass them. On confirmed cleanup, transition the original Blob to Deleted
   and release its original reservation atomically with the cleanup outcome.

There is currently no reviewed all-writes-drained proof for the unrestricted public URL. Until
that capability is established, the automatic result must be explicit ReconciliationRequired
for the cleanup Job, with Blob/reservation retained and safe reason classification. This is a
complete representation of uncertainty, **not completed physical cleanup**. It permits the
original upload Operation to reach an honest terminal result without losing its cleanup debt.
The UI/CLI may report that terminal failure and its cleanup status; it must not describe the
installation or storage reclamation as fully recovered.

A prospective single-write upload contract is the smaller way to bound future versions:
signed `If-None-Match: *`, required-header delivery, and actual concurrent/repeated PUT tests.
AWS conditional writes compare the current version and permit creation if the current version
is a delete marker; therefore deleting the only version while an admitted write remains is not
a write fence. [AWS conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).
Single-write support, a terminal write fence/drain proof, and old unrestricted capabilities must
be reviewed together. Neither a single-write flag nor two successive empty HEADs solves the
existing D capability by itself. D must remain retained or undergo explicitly reviewed physical
reconciliation; this proposal does not retrofit or mutate it.

## Acceptance evidence before implementation is called complete

- Owner tests: just-before/at/after deadline, exact public versus internal origin, wrong tenant or
  digest, duplicate deadline command, quota amount/version, and stale lease rejection.
- Real PostgreSQL races: complete wins first; deadline transition wins first; two sweepers;
  lost response and restart; exactly one terminal outcome, Receipt/event and quota settlement.
- Actual narrow roles: Gateway deadline transaction and Maintenance observation/delete/commit;
  no new Registry/Tenant writes, no locator in public diagnostics, no unchecked blanket grant.
- Actual provider: no PUT, completed PUT without complete, repeated versions, markers, slow PUT
  crossing expiry, bad signed headers/length, lost write/delete responses and inventory pagination.
  A synthetic provider proves consumer control flow only. AWS and local S3 capabilities are
  qualified separately; an unsupported proof remains ReconciliationRequired.
- Restart during each persisted cleanup phase; no lost version, duplicate ledger release,
  deletion of another key, prefix/latest deletion, or terminal success from incomplete evidence.
- Existing successful prepare/PUT/complete/scan/finalize/replay remains unchanged. CLI and Console
  retain failed upload journals, show original terminal failure, and require an explicit new
  intent; an expired grant is never renewed by replay.

Joint review must choose and qualify the physical write-fence/inventory capability before
automatic reclamation is implemented. The business deadline closure can be specified and tested
independently, provided its retained reconciliation debt is explicit and never claimed as clean.
