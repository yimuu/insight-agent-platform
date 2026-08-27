#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import pathlib
import subprocess

root = pathlib.Path.cwd()
rpc = (root / "crates/platform-mcp-rpc/src/lib.rs").read_text(encoding="utf-8")
source = (root / "crates/platform-mcp-service/src/main.rs").read_text(encoding="utf-8")
resource_source = (root / "crates/platform-mcp-service/src/resource_main.rs").read_text(encoding="utf-8")
discovery_source = (root / "crates/platform-mcp-service/src/discovery_main.rs").read_text(encoding="utf-8")
subscription_source = (root / "crates/platform-mcp-service/src/subscription_main.rs").read_text(encoding="utf-8")
dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
chart = root / "deploy/helm/insight-platform-mcp-host"
failures = []

for required in (
    "McpHostGrpcService",
    "McpHostGrpcClient",
    "CapabilityWorkerWorkloadIdentity",
    "CAPABILITY_WORKER_WORKLOAD_IDENTITY",
    "parse_strict_json",
    "metadata_digest",
    "McpRequestCapacity",
):
    if required not in rpc:
        failures.append(f"MCP Host RPC boundary is missing {required}")
for required in (
    "McpHostService",
    "StreamableHttpMcpTransport",
    "EgressBrokerGrpcClient",
    "McpHostExecutionServiceServer",
    "ServerTlsConfig",
    "client_ca_root",
    "CapabilityWorkerWorkloadIdentity",
    "serve_with_shutdown",
    "drain_grace_milliseconds",
    "process_observability_router",
    "ProcessHttpMetrics::install_with_capacities",
    "request_capacity_metric",
    "maximum_in_flight_requests",
):
    if required not in source:
        failures.append(f"MCP Host production composition is missing {required}")
for forbidden in (
    "PgRepository",
    "sqlx",
    "reqwest::",
    "std::process::Command",
    "ManagedStdioMcpTransport",
):
    if forbidden in source:
        failures.append(f"MCP Host crosses a forbidden boundary through {forbidden}")
for required in (
    "PgRepository",
    "McpResourceRefreshHost",
    "McpResourceRefreshGrpcService",
    "ContextWorkerWorkloadIdentity",
    "StreamableHttpMcpResourceRefreshProtocol",
    "ProcessHttpMetrics::install_with_capacities",
    "request_capacity_metric",
    "maximum_in_flight_requests",
):
    if required not in resource_source:
        failures.append(f"MCP Resource Host production composition is missing {required}")
for required in (
    "McpDiscoveryDriver",
    "McpDiscoveryWorker",
    "StreamableHttpMcpDiscoveryTransport",
    "ArtifactDataWorkerGrpcClient",
    "EgressBrokerGrpcClient",
    "run_discovery_queue_sampler",
    "discovery_capacity_metric",
    "ProcessHttpMetrics::install_with_capacities",
):
    if required not in discovery_source:
        failures.append(f"MCP Discovery Worker production composition is missing {required}")
for forbidden in (
    "TcpIncoming",
    "Server::builder",
    "reqwest::",
    "std::process::Command",
    "ManagedStdioMcpTransport",
):
    if forbidden in discovery_source:
        failures.append(f"MCP Discovery Worker crosses a forbidden boundary through {forbidden}")
for required in (
    "McpSubscriptionDriver",
    "McpSubscriptionWorker",
    "StreamableHttpMcpSubscriptionTransport",
    "McpStreamableHttpSubscriptionIngress",
    "ContextSubscriptionInvalidationTarget",
    "EgressBrokerGrpcClient",
    "run_subscription_queue_sampler",
    "subscription_capacity_metric",
    "ProcessHttpMetrics::install_with_capacities",
):
    if required not in subscription_source:
        failures.append(f"MCP Subscription Worker production composition is missing {required}")
for forbidden in (
    "TcpIncoming",
    "Server::builder",
    "reqwest::",
    "std::process::Command",
    "ManagedStdioMcpTransport",
):
    if forbidden in subscription_source:
        failures.append(f"MCP Subscription Worker crosses a forbidden boundary through {forbidden}")
if "/usr/local/bin/platform-mcp-host" not in dockerfile:
    failures.append("runtime image is missing platform-mcp-host")
if "/usr/local/bin/platform-mcp-resource-host" not in dockerfile:
    failures.append("runtime image is missing platform-mcp-resource-host")
if "/usr/local/bin/platform-mcp-discovery-worker" not in dockerfile:
    failures.append("runtime image is missing platform-mcp-discovery-worker")
if "/usr/local/bin/platform-mcp-subscription-worker" not in dockerfile:
    failures.append("runtime image is missing platform-mcp-subscription-worker")

try:
    subprocess.run(["helm", "lint", str(chart)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    rendered = subprocess.run(["helm", "template", "platform", str(chart)], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True).stdout
except (FileNotFoundError, subprocess.CalledProcessError) as error:
    failures.append(f"MCP Host Helm contract did not render: {error}")
    rendered = ""

for needle in (
    "kind: Service",
    "kind: Deployment",
    "kind: PodDisruptionBudget",
    "kind: HorizontalPodAutoscaler",
    "kind: ServiceMonitor",
    "name: observability",
    "path: /readyz",
    "path: /metrics",
    'command: ["/usr/local/bin/platform-mcp-host"]',
    'command: ["/usr/local/bin/platform-mcp-resource-host"]',
    'command: ["/usr/local/bin/platform-mcp-discovery-worker"]',
    'command: ["/usr/local/bin/platform-mcp-subscription-worker"]',
    "insight.platform/workload-role: mcp-host",
    "insight.platform/workload-namespace: mcp-host",
    "automountServiceAccountToken: false",
    "readOnlyRootFilesystem: true",
    "allowPrivilegeEscalation: false",
    'capabilities: {drop: ["ALL"]}',
    "PLATFORM_MCP_HOST_SERVER_CLIENT_CA_PATH",
    "PLATFORM_MCP_HOST_EGRESS_CA_PATH",
    "PLATFORM_MCP_RESOURCE_HOST_DATABASE_URL",
    "PLATFORM_MCP_DISCOVERY_WORKER_DATABASE_URL",
    "PLATFORM_MCP_DISCOVERY_WORKER_EGRESS_CA_PATH",
    "PLATFORM_MCP_DISCOVERY_WORKER_ARTIFACT_CA_PATH",
    "PLATFORM_MCP_SUBSCRIPTION_WORKER_DATABASE_URL",
    "PLATFORM_MCP_SUBSCRIPTION_WORKER_EGRESS_CA_PATH",
    "insight.platform/workload-role: mcp-discovery-worker",
    "insight.platform/workload-role: mcp-subscription-worker",
    "app.kubernetes.io/component: artifact-data-worker",
    "app.kubernetes.io/component: context-subscription-worker",
    "app.kubernetes.io/component: capability-remote-worker",
    "app.kubernetes.io/component: egress-broker",
    "port: 9443",
    "port: 8443",
):
    if needle not in rendered:
        failures.append(f"rendered MCP Host contract is missing {needle}")
for forbidden in (
    "kind: Ingress",
    "hostNetwork: true",
    "hostPID: true",
    "privileged: true",
    "automountServiceAccountToken: true",
    "port: 4222",
):
    if forbidden in rendered:
        failures.append(f"rendered MCP Host has forbidden capability {forbidden}")
if rendered.count("\nkind: Deployment\n") != 4 or rendered.count("\nkind: NetworkPolicy\n") != 5:
    failures.append("MCP Host must render four isolated workload pools and five NetworkPolicies")
if rendered.count("port: 5432") != 3:
    failures.append("only the MCP Resource Host, Discovery Worker and Subscription Worker may reach PostgreSQL")

negative_values = (
    ("--set", "replicas=1", "at least two replicas"),
    ("--set", "image.digest=latest", "exact sha256"),
    ("--set", "config.digest=latest", "exact sha256"),
    ("--set", "serverTls.existingSecret=", "both TLS identities"),
    ("--set", "networkPolicy.callerNamespace=", "exact caller"),
    ("--set", "autoscaling.minReplicas=1", "at least two replicas"),
    ("--set", "observability.port=9443", "observability port must be distinct"),
    ("--set-json", "networkPolicy.monitoringPodSelector=null", "monitoring requires exact"),
    ("--set", "discoveryPool.replicas=1", "Discovery Worker requires at least two replicas"),
    ("--set", "discoveryPool.config.digest=latest", "discoveryPool config.digest must be exact sha256"),
    ("--set", "discoveryPool.clientTls.existingSecret=", "exact upstream TLS identities"),
    ("--set", "discoveryPool.database.existingSecret=insight-platform-mcp-resource-host-database", "independently provisioned database credential"),
    ("--set-json", "discoveryPool.artifactPodSelector=null", "exact PostgreSQL and Artifact destinations"),
    ("--set", "discoveryPool.autoscaling.minReplicas=1", "Discovery Worker HPA requires at least two replicas"),
    ("--set", "subscriptionPool.replicas=1", "Subscription Worker requires at least two replicas"),
    ("--set", "subscriptionPool.config.digest=latest", "subscriptionPool config.digest must be exact sha256"),
    ("--set", "subscriptionPool.clientTls.existingSecret=", "exact Egress TLS identity"),
    ("--set", "subscriptionPool.database.existingSecret=insight-platform-mcp-discovery-worker-database", "independently provisioned database credential"),
    ("--set-json", "subscriptionPool.postgresCidrs=[]", "exact PostgreSQL CIDRs"),
    ("--set", "subscriptionPool.autoscaling.minReplicas=1", "Subscription Worker HPA requires at least two replicas"),
)
for flag, assignment, expected in negative_values:
    result = subprocess.run(["helm", "template", "platform", str(chart), flag, assignment], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode == 0 or expected not in result.stderr:
        failures.append(f"MCP Host chart accepted invalid override {assignment}")

if failures:
    raise SystemExit("\n".join(f"MCP Host deployment: {failure}" for failure in failures))
print("MCP Host static deployment boundary passed.")
PY
