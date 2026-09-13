# ADR-0012: Declarative container installation

Status: Accepted and implemented. Local deployment evidence is recorded separately in
[qualification reports](../qualifications/README.md); no production or release qualification is implied.

## Decision

Compose and Helm own container lifecycle. A configuration preparation command may render a complete
Compose document or Helm values, but no host Python controller advances installation phases. The
existing Rust installer remains a finite, deployment-only process; it never receives a Docker socket,
Kubernetes API token, or permission to create, stop or inspect workloads.

Compose declares prepare, dependency bootstrap and platform provision completion dependencies.
Helm submits its dependencies, one installation Job and serving Deployments together. Dependencies
wait for completed configuration publication; serving init containers wait for the installer's
bounded, input-bound completion envelope. These envelopes are deployment wake gates, not business
authority: ordinary processes still validate their actual configuration, schema and provider identity.
Only the installation task writes the dedicated gate volume; workloads mount it read-only. This
retains the existing single-node development PVC topology and does not claim multi-node storage.

OpenBao runs one ordinary persistent server without a self-initialize stanza. An installation-only
bootstrap command contacts its management API under the existing private CA and client certificate.
It records the existing one-time request permission before initialization. Its temporary root token
and recovery material are confined to the private installation volume; configuration writes are
performed once, then the root token is revoked before ProviderReady. Retrying an uncertain attempt
may observe the original provider and finish root-token revocation, but never repeat initialization
or configuration writes. A response lost before private recovery material is persisted remains
ExternalOutcomeUnknown. Missing data never authorizes a replacement provider identity.

The serving OpenBao process receives only its server/seal configuration. Its clients keep their
existing certificate policies. Database schema, grants, bootstrap, S3 and JetStream reuse their
existing owners, transaction boundaries and recovery journals. Serving processes gain no DDL or
provider administration permission. No business schema, message contract or public HTTP API changes.

Session and public-CA delivery are explicit one-shot operations and are independent of service
startup. No token is put into logs or Helm notes, and no serving process receives the issuer key.
Native execution consumes the same provider bootstrap while keeping its existing host supervision.

## Contract and architecture cross-review

The pre-implementation cross-review compared the owning installation/provider types, this decision,
the current PostgreSQL schema, installer workflow and generated process contracts. This was a
cross-boundary review by the implementing Codex, not an independent human approval. It established
the following implementation obligations:

- Ownership and identities: keep existing installation/provider identities and one-time write guards;
  gate files cannot authorize business effects or synthesize provider observation.
- Schemas and errors: bounded versioned gates; no database changes; retain typed drift, conflict,
  incomplete and unknown-outcome failures, including when a task is restarted.
- Transactions and events: PostgreSQL retains business atomicity; no new queue or business-state
  projection; deployment locks only serialize installation material.
- Security: private bootstrap credentials, root revocation, read-only role/gate mounts, no workload
  management credentials, and unchanged TLS/certificate authentication.
- Capacity: finite waits and Job deadlines; no per-replica privileged provisioning. Shared local
  PVCs remain node-bound and are not presented as production high availability.
- Recovery: no write retries after uncertain provider initialization; preserve failed resources and
  journals; container/Pod recreation verifies original state.
- Evidence: contract tests, real OpenBao initialization/revocation/restart, direct Compose fresh and
  repeated startup, Helm rendering and real disposable-cluster startup/reconstruction. No earlier
  wrapper qualification is inherited by the new path.

Review findings resolved before implementation: an init API response must be durably retained
before subsequent writes; its root token must be revoked even when configuration fails; loss of
the init response cannot be treated as successful recovery. Helm startup gates must be separate
from role-private state and must not wait for serving readiness in the installation Job. File
gates only signal publication and never replace the existing database/provider validation.

This supersedes ADR-0010's host-controlled Compose/Helm phases and two-process OpenBao switch.
The remaining business, credential and persistence boundaries in ADR-0010 continue to apply.

The implementation review found two physical startup ordering issues: the init API can finish
before OpenBao's ordinary Raft leader accepts writes, and ConfigMap directory projection produces
symlinks rejected by the strict input reader. Active-server read waits and explicit file mounts
resolved them. Fresh Compose/Kind qualification, repeated startup, reconstruction, bounded fault
tests and the original schema/contract checks passed for the recorded images. The replacement
removes the host controllers and keeps serving DDL and workload-management permissions unchanged.
