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

## InsightPlatformPostgresObservationFailing

Confirm the scheduler critical-control PostgreSQL pool can acquire a connection and execute bounded
read-only queries. Correlate failures with readiness and recovery scan failures. The exporter keeps
the last valid gauge snapshot, so do not interpret an unchanged backlog as healthy while this alert
is active. Follow the dependency recovery runbook and retain the reserved connection boundary.

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
