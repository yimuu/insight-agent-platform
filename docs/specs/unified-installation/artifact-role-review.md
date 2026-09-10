# Artifact role integration review

The policy-lock and staging-ledger changes below were jointly accepted for implementation on
2026-09-10. This is a temporary review record, not evidence that a complete installation or model
journey passed. The owning schema, repository, role grants and ADR-0010 remain authoritative.

## Findings

A real public source-registration attempt completed credential import, Artifact preparation and
object upload. Completing the upload failed because the Artifact Gateway's restricted database role
could not acquire the policy row locks used while scheduling the initial scan. The enclosing
transaction rolled back, including upload completion and its Receipts. A later public read observed
the original Staging Artifact and waiting Operation; neither observation proves a terminal failure
or permits extending the expired upload grant.

An independent examination of the remaining upload path also found a redundant row lock on the
append-only quota ledger during finalization. Testing preparation alone did not exercise either
failure. The regression must execute the complete nonempty upload using its actual process roles.

## Policy locking

Keep the current transaction's shared locks on the exact Policy Revision and its owning Resource.
Provide a restricted PostgreSQL physical locking function rather than giving the Artifact Gateway
UPDATE access to Registry state. Its bounded nominal inputs identify a tenant and Policy Revision;
it locks only that revision and its same-tenant Policy Resource and returns whether they exist.

The function owns no business decision: it does not return payloads or interpret active gates,
semantic digests, policy kinds within the Policy document, or authorization. The Rust repository
performs its current reads and exact-policy checks after acquiring the locks, in the same transaction.
Existing Registry mutations continue to conflict with these ordinary row locks; no second advisory
lock convention or duplicated state machine is introduced.

Use a fixed `pg_catalog` search path, fully qualified tables and no dynamic SQL. Revoke execution
from PUBLIC and grant it to Artifact Gateway and Artifact Data Worker. Gateway uses it when
scheduling scans; Data Worker also uses the shared policy check for internal workload Artifact
stage authorization and staging. The read-only materialization and maintenance roles must remain
unable to execute the function. Existing pool and transaction bounds
continue to limit concurrent locking; the function does not retain a lock beyond its transaction.

The physical schema and its generated inventory must change together. Serving verifies the exact
new contract. Existing test installations are not silently patched or granted broader permissions;
qualification of the new revision uses a fresh installation.

## Staging quota finalization

Remove the shared lock from the exact reservation-ledger read. The repository already locks the
owning QuotaAccount for update, checks its expected version and reservation, and settles under that
same lock and the existing unique ledger constraints. Reservation entries are immutable facts;
reserve and settle append records. The Gateway retains SELECT/INSERT on the ledger and receives no
UPDATE or DELETE privilege. Used amounts, account CAS, ledger uniqueness and atomic finalization
remain unchanged.

## Required evidence

- A nonempty upload under the real Gateway role reaches scan scheduling, uses the real Data Worker
  role for scan work and commit, and finalizes under Gateway with quota settlement. Repeating the
  original commands reads back the same accepted result and does not append another reservation.
- Registry gate changes wait for the policy lock; if a gate mutation commits first, the subsequent
  Artifact check observes and rejects the disabled policy. Wrong tenant or revision cannot lock an
  unrelated matching resource through a broadened lookup.
- The internal workload-stage path also executes under its actual Data Worker role. PUBLIC,
  the read-only materialization role and maintenance cannot execute the function. Registry rows and quota
  ledger entries remain protected from Gateway UPDATE/DELETE; no trigger or generic write function
  supplies an alternative path.
- Physical inventory verification and fresh provisioning accept the new schema and reject drift.
  A subsequent full installation and real public model-registration journey provide separate
  integration evidence; prior readiness checks do not transfer that qualification.

## Deletion approval

The deletion branch was also jointly accepted after examining the Task resolution owner and its
current writers. Artifact Gateway already owns creating a pending deletion-approval Task inside
`mark_deletion`; grant the INSERT needed for that existing operation, retaining the prohibition on
Task UPDATE/DELETE.

Remove the shared lock when reading an already-approved deletion Task. Task resolution is a
Pending-only first-winner operation with generation/version guards; approved resolution facts do
not change. Current history retirement deletes external-authorization cleanup chains, not approval
Tasks. The Artifact/Blob locks and Rust checks of the exact owner version, request digest, policy
and resolution remain mandatory. This does not authorize Gateway to approve or alter a Task.

The regression must create the pending Task under Gateway, resolve it through the existing Task
owner, and prove the approved branch and original-command replay under Gateway. It must reject
another resolution of the terminal Task, an inexact approval and Gateway Task UPDATE/DELETE.
Fixture approval proves the code path only; it is not the user's required human confirmation.

## Internal workload staging

The Data Worker also implements the existing Context/MCP workload-stage port. Its actual stage
command creates Artifact and Blob records; grant INSERT on those two owning tables. The producer's
exact Job fence, preallocated identities and policy checks remain unchanged. Context producer
authorization does not require a new Tenant lock or Tenant mutation privilege.

For MCP discovery, the existing owner loads and locks its Invocation before staging. After scan
verification, the existing atomic wake command also advances that Invocation's version and update
timestamp under its original CAS, kind/state, Job and evidence checks. Grant Data Worker SELECT on
Invocations and UPDATE only on those two columns. This is permission for an existing required write,
not a newly invented write to obtain a row lock. State, payload, owner, INSERT and DELETE remain
inaccessible to this role; the same Job wake and event transaction remains authoritative.

Real Context and MCP fixtures must use the restricted work pool for authorization and staging;
the MCP fixture must also execute the scan wake and verify its original fences and replay behavior.
The public upload's absent producer cannot prove these internal paths. Role tests must distinguish
the required Invocation column updates from denied business-column and row creation/deletion access.

## Scheduler DataReader authority

The F installation's first chat Run failed before ModelLoop creation. Three PostgreSQL permission
errors on `runs` matched the exact Scheduler TypedPlan object-authorization SQL; no ModelTurn or
Model Job existed. The Artifact Data process wires this reader to its restricted read pool, whose
old grants covered only the earlier Artifact/Sandbox path. This is not provider-call evidence.

Joint review accepts the exact required column reads on Runs, ResourceVersions, Deployments and
Resources for the existing TypedPlan, RunValue and Skill readers. Skill also evaluates its frozen
Selection Policy through the existing current-gate reader with row locking disabled. Run current
payload, unrelated columns, table-wide reads on these four tables, DML and row locks remain denied.
There is no new schema object, reader authorization algorithm, lease, retry, timeout or capacity
change. The existing Artifact physical read scope is unchanged; no running installation is patched.

The existing full Run kernel fixture must execute all three real request resolvers/object read
authorities with actual DataReader grants, first reproducing the old failure. It must retain tenant,
lease, digest, frozen-slot and current Selection Policy rejection cases, and directly reject the
ungranted fields, writes and shared locks. Fixture-only source metadata uses the current owning
Artifact format. This database proof does not substitute for the next complete installed Run.

Evidence: the actual DataReader role with the old grants failed at the first Scheduler read in a
fresh database. After the column-only grant update, the complete Run kernel target passed, including
all three readers, original recovery/control assertions, current Selection Policy withdrawal,
tenant/lease/digest rejection and 24 direct ungranted-field/read/write/lock checks. The first new
gate assertion was corrected to expect the owner's existing `NotFound` result; production error
mapping was unchanged. All temporary roles were removed after failure and success. PostgreSQL
all-target strict Clippy and the Artifact/Security deployment checks passed. No F data or grants
were modified, and no new provider call was made by these tests.
