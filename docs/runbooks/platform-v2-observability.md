# Platform v2 observability alerts

These alerts use only bounded component-role and operation labels. Never paste tenant IDs,
resource IDs, request bodies, prompts, tool arguments, URLs with query strings, object keys,
credentials, or tokens into dashboards, tickets, or chat.

## InsightPlatformTelemetryMissing

Confirm Prometheus and ServiceMonitor discovery first, then compare the expected 19 workload pools
with the live target inventory. Check namespace and Pod selectors, NetworkPolicy denial counters,
and rollout readiness. Treat a total telemetry loss as an observability failure; do not restart
business dependencies until target discovery and network reachability are distinguished.

## InsightPlatformWorkloadNotReady

Identify the bounded `component_role`, inspect rollout status and the process startup error class,
then follow the dependency recovery runbook for the failing PostgreSQL, NATS, S3/KMS, Secret,
Egress, Artifact, or Sandbox authority. Do not page on one tenant/provider failure unless the
component-level readiness contract itself is unavailable.

## InsightPlatformOperationalCapacityExhausted

Inspect `component_role` and `resource`, then correlate the exhausted authority with request latency,
rejection outcomes and dependency health. For `sandbox-controller/artifact_response`, verify that
Artifact response streams are completing and that the Artifact Data Worker is responsive. Preserve
critical-control capacity and reduce or shed business admission before changing a qualified value.
Escalate persistent exhaustion for a new CapacityProfile qualification; do not hot-edit limits.

## InsightPlatformHttpFailureRatioHigh

Confirm meaningful request volume and the affected bounded role/operation. Compare failure ratio
with readiness, dependency latency and recent rollout events. Roll back only through GitOps when
the failure begins with a candidate rollout; otherwise apply the dependency recovery runbook.

## InsightPlatformHttpLatencyHigh

Confirm request volume, saturation and downstream dependency latency before changing replicas.
Use the qualified CapacityProfile limits for scaling and permit changes. Do not bypass queue,
admission, hard-limit, or isolation controls to reduce latency.

## InsightPlatformCriticalControlPermitsExhausted

Confirm the `scheduler-recovery` Pod is Ready and compare business versus critical-control permit
availability. Inspect recovery scan duration and PostgreSQL critical-control pool health. Do not
lend business permits to recovery or increase concurrency outside an approved CapacityProfile;
remove the blocking dependency or use the qualified GitOps scaling path.

## InsightPlatformRecoveryFailureRatioHigh

Compare attempted and failed recovery scans with PostgreSQL availability, lease expiry volume and
recent rollout events. Determine whether every recovery family is failing or one bounded scan is
affected before changing replicas. Preserve fencing and owner transactions; never repair recovery
state with direct row edits.

## InsightPlatformDurableJobLagHigh

Compare the `due` backlog and oldest lag with business permits, claim outcomes, PostgreSQL latency,
and recent rollout events. A growing count indicates admission exceeds qualified capacity; a flat
small count with growing lag indicates stalled claims. Do not delete or reprioritize Job rows. Use
the qualified GitOps scaling path or repair the blocked dependency while preserving fairness.

## InsightPlatformExpiredLeaseRecoveryLagHigh

Compare expired-lease count and lag with critical-control permits and recovery scan outcomes. Check
Worker loss, database time, and fencing errors before changing recovery cadence. Never clear lease
columns directly or lend business capacity to recovery; owner recovery transactions must settle the
expired lease.

## InsightPlatformModelDurableJobLagHigh

Compare Model `due` count and oldest lag with Model business permits, PostgreSQL health, provider
dependency outcomes, NATS health and rollout events. A growing queue may indicate admission above
qualified capacity; a small queue with growing age may indicate stalled claims. Do not edit Job
priority or state directly. Repair the blocked dependency or use the qualified GitOps scaling path.

## InsightPlatformModelExpiredLeaseRecoveryLagHigh

Compare Model expired-lease count and lag with Model critical-control permits, Worker restarts,
PostgreSQL time and provider cancellation/recovery outcomes. Preserve the frozen ModelTurn, quota
and Job fence. Never clear lease fields or force terminal state through direct SQL; allow the Model
owner recovery transaction to settle the lost attempt.

## InsightPlatformCapabilityDurableJobLagHigh

Use the fixed `component_role` to distinguish Native from Remote Capability work, then compare due
count and age with that role's business permits and PostgreSQL health. For Remote, also correlate
Egress and MCP Host dependency outcomes without adding endpoint or codec labels. Do not move work
between WorkClasses or edit Job priority/state; repair the dependency or use qualified GitOps scale.

## InsightPlatformCapabilityExpiredLeaseRecoveryLagHigh

Identify the fixed Native or Remote role, then check Worker restarts, critical-control permits,
PostgreSQL time, fence failures and the applicable external dependency. Preserve Invocation, quota
and Job fencing. Never clear leases or replay non-idempotent Remote effects by hand; let the owning
Capability recovery transaction choose the safe terminal or retry path.

## InsightPlatformSandboxDurableJobLagHigh

Compare the Sandbox execution due count and age with Controller readiness, executor admission,
Artifact response capacity and PostgreSQL health. This series intentionally excludes managed MCP
session work by selecting the exact Sandbox capability-execution JobKind. Do not bypass admission,
move work between owners or execute code in the Controller; restore the blocked executor/Artifact
path or use the qualified GitOps scaling path.

## InsightPlatformSandboxExpiredLeaseRecoveryLagHigh

Check executor loss, process-generation attestation, database time, Controller fencing and
critical-control recovery. Preserve the shared Job and physical execution fence; never clear lease
or attestation fields directly and never fall back to host execution. Recovery must prove the old
process generation absent before the Controller permits a new physical attempt.

## InsightPlatformArtifactDurableJobLagHigh

Use `component_role` to distinguish Data Worker scan/rescan from Maintenance delete/blob-cleanup
work. Correlate due count and age with PostgreSQL, S3/KMS dependency outcomes and the role's local
capacity. Do not move Jobs between roles or edit their kind, priority or state; restore the blocked
provider path or use the qualified GitOps scaling path.

## InsightPlatformArtifactExpiredLeaseRecoveryLagHigh

Identify the fixed Artifact role, then inspect Worker restarts, PostgreSQL time, object-store/KMS
outcomes and Artifact owner fencing. Preserve the Artifact/Blob generation and Job lease. Never
clear a lease or force Ready/Deleted through direct SQL; allow the owning recovery transaction to
revalidate storage evidence and settle the attempt.

## InsightPlatformContextDurableJobLagHigh

Use `component_role` to distinguish Native query, Remote query and subscription-refresh work, then
correlate due count and age with PostgreSQL, local permits and the exact adapter dependency. Do not
move work between roles or change JobKind; repair the qualified adapter/Host path or use the
qualified GitOps scaling path.

## InsightPlatformContextExpiredLeaseRecoveryLagHigh

Inspect the affected Context role's process restarts, PostgreSQL time, quota and Job fence. Remote
and subscription work must preserve their frozen request/execution identity; never replay external
I/O or clear leases manually. Let the Context owner transaction choose safe retry, recovery or
terminal settlement.

## InsightPlatformMcpDiscoveryDurableJobLagHigh

Compare the exact discovery due count and age with the dedicated worker's discovery permit,
PostgreSQL health, Egress outcomes and Artifact Data Worker outcomes. Do not let an ordinary MCP
Host claim this lane or bypass the durable Artifact verification wake; restore the qualified
worker path or use qualified GitOps scaling.

## InsightPlatformMcpDiscoveryExpiredLeaseRecoveryLagHigh

Check discovery-worker restarts, database time, heartbeat/fence failures and the staged Artifact
verification state. Preserve the exact MCP operation, Job lease, Artifact/Blob generation and
transport evidence. Never clear the lease, restage the object or publish a Snapshot manually; the
owner recovery transaction must revalidate the frozen closure and resume from durable evidence.

## InsightPlatformMcpSubscriptionDurableJobLagHigh

Compare the exact logical-subscription due count and age with the worker's subscription permit,
PostgreSQL health and Egress dependency outcomes. Check whether recovery or periodic reconcile is
repeatedly making the Job ready without a successful claim. Do not let the ordinary MCP Host claim
this lane or edit JobKind/state; restore the dedicated worker path or use qualified GitOps scaling.

## InsightPlatformMcpSubscriptionExpiredLeaseRecoveryLagHigh

Check subscription-worker restarts, database time, heartbeat/fence failures and Egress stream
termination. Preserve the durable session generation, notification event generation and exact Job
lease. Never clear the lease or activate an uncommitted stream manually; the bounded recovery scan
must rebuild the session and require a full Context reconcile through owner transactions.

## InsightPlatformDurableObservationFailureRatioHigh

Confirm the affected fixed `component_role`, then correlate PostgreSQL transport health and pool
capacity. Durable gauge values retain their last successful snapshot, so do not treat a flat gauge
as proof that the backlog is current while this alert is firing. Restore the bounded read-only
sampler path; never replace it with payload scans, direct state mutation or high-cardinality labels.

## InsightPlatformDependencyFailureRatioHigh

Use only the fixed `component_role` and `dependency` labels to identify the affected boundary, then
correlate the failure ratio with readiness, capacity, latency, rollout events and the owning
dependency's provider telemetry. For PostgreSQL, preserve business and critical-control pool
separation and compare durable queue observations. For NATS, S3, KMS, Secret or Egress, distinguish
a component-wide transport failure from a single rejected business request. Follow the dependency
recovery runbook; do not paste endpoints, database names, subjects, object keys, provider identities,
error text or tenant data into the incident record.

## InsightPlatformDueOutboxLagHigh

Compare the due count and oldest lag with publisher readiness, NATS reachability, PostgreSQL
latency, and recent rollout events. Do not delete, publish, or advance Outbox rows manually. Repair
the publisher or transport dependency and let the fenced Outbox owner preserve ordering and replay.

## InsightPlatformExpiredOutboxClaimLagHigh

Check publisher process loss, database time, claim fencing, and critical-control availability.
Never clear `claim_owner`, increment epochs, or move `next_publish_at` by direct SQL; recovery must
reclaim through the Outbox owner so a late publisher cannot become a second winner.

## InsightPlatformOutboxDeadEventsPresent

Identify the fixed dead queue and correlate its first appearance with safe failure-code aggregates
and dependency incidents without exporting Event payloads or identities. Follow the owning domain's
reconciliation procedure, retain the dead record as audit evidence, and escalate any unsupported
event kind rather than replaying it outside the owner contract.
