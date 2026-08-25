# Platform v2 observability alerts

These alerts use only bounded component-role and operation labels. Never paste tenant IDs,
resource IDs, request bodies, prompts, tool arguments, URLs with query strings, object keys,
credentials, or tokens into dashboards, tickets, or chat.

## InsightPlatformTelemetryMissing

Confirm Prometheus and ServiceMonitor discovery first, then compare the expected 17 workload pools
with the live target inventory. Check namespace and Pod selectors, NetworkPolicy denial counters,
and rollout readiness. Treat a total telemetry loss as an observability failure; do not restart
business dependencies until target discovery and network reachability are distinguished.

## InsightPlatformWorkloadNotReady

Identify the bounded `component_role`, inspect rollout status and the process startup error class,
then follow the dependency recovery runbook for the failing PostgreSQL, NATS, S3/KMS, Secret,
Egress, Artifact, or Sandbox authority. Do not page on one tenant/provider failure unless the
component-level readiness contract itself is unavailable.

## InsightPlatformHttpFailureRatioHigh

Confirm meaningful request volume and the affected bounded role/operation. Compare failure ratio
with readiness, dependency latency and recent rollout events. Roll back only through GitOps when
the failure begins with a candidate rollout; otherwise apply the dependency recovery runbook.

## InsightPlatformHttpLatencyHigh

Confirm request volume, saturation and downstream dependency latency before changing replicas.
Use the qualified CapacityProfile limits for scaling and permit changes. Do not bypass queue,
admission, hard-limit, or isolation controls to reduce latency.
