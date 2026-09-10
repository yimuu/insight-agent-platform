# S3 startup observation

Status: jointly accepted by the root and installation owners before implementation. This is a
temporary boundary review; the original Native E recovery failure remains retained.

## Evidence and ownership

Native E stopped and reaped its serving processes, then stopped all four original dependencies
with exit code zero and removed only those containers. The same frozen installation and data
created replacement containers. Its OpenBao serving observation returned ProviderReady, but S3
HeadBucket reported a transport dispatch failure before the S3 server began listening. No serving
process started. This is evidence of missing S3 readiness observation, not lost persistent data.
It does not establish the cause of the separately retained first Compose H recovery failure.

The existing S3 installation owner already has the exact input, identity, credentials, endpoint,
CA and bucket journal. Keep that authority instead of adding another deployment state machine or
an unauthenticated TCP readiness assertion to each consumer.

## Accepted change

The first signed, read-only bucket observation in `ensure_with_api` has one thirty-second deadline.
Only the existing `PrerequisiteUnavailable` classification permits another observation; each
attempt is clipped to the remaining deadline, with at most one second between observations.
The SDK retains explicit credentials, TLS validation, closed endpoint and one SDK attempt.

A returned missing or existing bucket immediately enters the original journal rules. In particular,
Configured and Verify still reject a missing bucket, and a fresh Planned intent still rejects an
existing foreign bucket. Observation never creates a new installation identity, changes the
journal phase, renews a write intent or repairs a mismatch. The initial Planned journal may already
exist before observation, as in the existing owner; it is not an external effect.

Create, tagging, versioning, CORS and their subsequent exact readbacks are outside this retry loop.
Response loss after a write retains its original Requested state and existing uncertainty rules.
The helper returns no new public or persistent type; there is no database or process-wire change.
It runs under the existing exclusive installation lock and bounded provisioning operation.

## Required evidence

Exercise delayed first readiness, a hanging read and exhausted deadline, fatal errors without
another read, Configured/Verify missing or changed state, and interrupted Requested phases without
another write. Verify byte-for-byte preservation of completed journals and no external effects on
readiness failure. Post-write readback failure must still return immediately without entering this
loop. A new frozen runtime must separately pass actual container reconstruction; a later explicit
recovery of the old E package cannot prove the new behavior.

The thirteen S3 owner tests, including the four new startup/recovery cases, passed. The original
E package subsequently completed one explicit same-identity recovery with all private records and
public business views unchanged, followed by a real Qwen credential probe. That result confirms
retained data in this controlled reconstruction; the first startup failure is not erased and the
new readiness helper has not yet received its separate physical qualification.
