#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import pathlib
import subprocess

root = pathlib.Path.cwd()
rpc = (root / "crates/platform-mcp-rpc/src/lib.rs").read_text(encoding="utf-8")
source = (root / "crates/platform-mcp-service/src/main.rs").read_text(encoding="utf-8")
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
if "/usr/local/bin/platform-mcp-host" not in dockerfile:
    failures.append("runtime image is missing platform-mcp-host")

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
    "insight.platform/workload-role: mcp-host",
    "insight.platform/workload-namespace: mcp-host",
    "automountServiceAccountToken: false",
    "readOnlyRootFilesystem: true",
    "allowPrivilegeEscalation: false",
    'capabilities: {drop: ["ALL"]}',
    "PLATFORM_MCP_HOST_SERVER_CLIENT_CA_PATH",
    "PLATFORM_MCP_HOST_EGRESS_CA_PATH",
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
    "port: 5432",
    "port: 4222",
):
    if forbidden in rendered:
        failures.append(f"rendered MCP Host has forbidden capability {forbidden}")
if rendered.count("\nkind: Deployment\n") != 1 or rendered.count("\nkind: NetworkPolicy\n") != 2:
    failures.append("MCP Host must render one workload and two NetworkPolicies")

negative_values = (
    ("--set", "replicas=1", "at least two replicas"),
    ("--set", "image.digest=latest", "exact sha256"),
    ("--set", "config.digest=latest", "exact sha256"),
    ("--set", "serverTls.existingSecret=", "both TLS identities"),
    ("--set", "networkPolicy.callerNamespace=", "exact caller"),
    ("--set", "autoscaling.minReplicas=1", "at least two replicas"),
    ("--set", "observability.port=9443", "observability port must be distinct"),
    ("--set-json", "networkPolicy.monitoringPodSelector=null", "monitoring requires exact"),
)
for flag, assignment, expected in negative_values:
    result = subprocess.run(["helm", "template", "platform", str(chart), flag, assignment], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode == 0 or expected not in result.stderr:
        failures.append(f"MCP Host chart accepted invalid override {assignment}")

if failures:
    raise SystemExit("\n".join(f"MCP Host deployment: {failure}" for failure in failures))
print("MCP Host static deployment boundary passed.")
PY
