#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import pathlib
import subprocess

root = pathlib.Path.cwd()
manifest = (root / "crates/platform-capability-worker/Cargo.toml").read_text(encoding="utf-8")
source = (root / "crates/platform-capability-worker/src/remote_main.rs").read_text(encoding="utf-8")
library = (root / "crates/platform-capability-worker/src/lib.rs").read_text(encoding="utf-8")
dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
chart = root / "deploy/helm/insight-platform-capability-remote-worker"
failures = []

for dependency in (
    "insight-platform-capability-adapters.workspace = true",
    "insight-platform-egress-rpc.workspace = true",
    "insight-platform-mcp-host.workspace = true",
    "insight-platform-mcp-rpc.workspace = true",
    "insight-platform-postgres.workspace = true",
    "insight-platform-worker.workspace = true",
    "insight-platform-observability.workspace = true",
):
    if dependency not in manifest:
        failures.append(f"Remote Capability Worker process is missing {dependency}")
for forbidden in (
    "insight-platform-secret-broker.workspace = true",
    "insight-platform-sandbox.workspace = true",
    "reqwest.workspace = true",
):
    if forbidden in manifest:
        failures.append(f"Remote Capability Worker bypasses a plane boundary through {forbidden}")
if "/usr/local/bin/platform-capability-remote-worker" not in dockerfile:
    failures.append("runtime image is missing platform-capability-remote-worker")
for required in (
    "parse_strict_json",
    "canonical_digest",
    "install_builtin_http_json_codecs",
    "install_builtin_grpc_json_codecs",
    "EgressBrokerGrpcClient",
    "ClientTlsConfig",
    "HttpCapabilityAdapter",
    "GrpcCapabilityAdapter",
    "McpCapabilityAdapter",
    "McpHostGrpcClient",
    "connect_lazy",
    "business_max_connections",
    "critical_control_max_connections",
    "verify_schema",
    "CancellationToken",
    "driver.run",
    "process_observability_router",
):
    if required not in source:
        failures.append(f"Remote Capability Worker production composition is missing {required}")
for forbidden in (
    "McpHostService",
    "StreamableHttpMcpTransport",
    "reqwest::",
    "SecretManager",
    "KmsClient",
    "std::process::Command",
):
    if forbidden in source:
        failures.append(f"Remote Capability Worker owns a forbidden external boundary: {forbidden}")
for required in (
    "parse_bounded_remote_json",
    "RetryableAfterDispatch",
    "TimedOutUncertain",
    "recover_expired_capability_jobs",
):
    if required not in library:
        failures.append(f"Remote Capability Worker runtime is missing {required}")

try:
    subprocess.run(
        ["helm", "lint", str(chart)], check=True,
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    rendered = subprocess.run(
        ["helm", "template", "platform", str(chart)], check=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ).stdout
except (FileNotFoundError, subprocess.CalledProcessError) as error:
    failures.append(f"Remote Capability Worker Helm contract did not render: {error}")
    rendered = ""

for needle in (
    "kind: Deployment",
    "kind: HorizontalPodAutoscaler",
    "kind: PodDisruptionBudget",
    "kind: ServiceMonitor",
    "name: observability",
    "path: /readyz",
    "path: /metrics",
    'command: ["/usr/local/bin/platform-capability-remote-worker"]',
    "insight.platform/workload-role: capability-remote-worker",
    "insight.platform/workload-namespace: capability-remote-worker",
    "automountServiceAccountToken: false",
    "readOnlyRootFilesystem: true",
    "allowPrivilegeEscalation: false",
    'capabilities: {drop: ["ALL"]}',
    "PLATFORM_CAPABILITY_REMOTE_WORKER_CONFIG_DIGEST",
    "PLATFORM_CAPABILITY_REMOTE_WORKER_DATABASE_URL",
    "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_CA_PATH",
    "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_CERT_PATH",
    "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_KEY_PATH",
    "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_CA_PATH",
    "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_CERT_PATH",
    "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_KEY_PATH",
    "app.kubernetes.io/component: egress-broker",
    "app.kubernetes.io/component: mcp-host",
):
    if needle not in rendered:
        failures.append(f"rendered Remote Capability Worker contract is missing {needle}")
for forbidden in (
    "kind: Ingress",
    "hostNetwork: true",
    "hostPID: true",
    "privileged: true",
    "automountServiceAccountToken: true",
    "PLATFORM_CAPABILITY_REMOTE_WORKER_SANDBOX_",
    "PLATFORM_CAPABILITY_REMOTE_WORKER_ARTIFACT_",
    "port: 4222",
):
    if forbidden in rendered:
        failures.append(f"rendered Remote Capability Worker has forbidden capability {forbidden}")
if rendered.count("\nkind: Deployment\n") != 1 or rendered.count("\nkind: NetworkPolicy\n") != 2:
    failures.append("Remote Capability Worker must render one workload and two NetworkPolicies")
for port in ("port: 53", "port: 5432", "port: 8443", "port: 9443"):
    if port not in rendered:
        failures.append(f"Remote Capability Worker egress is missing {port}")

negative_values = (
    ("--set", "replicas=1", "at least two replicas"),
    ("--set", "image.digest=latest", "exact sha256"),
    ("--set", "config.digest=latest", "exact sha256"),
    ("--set-json", "networkPolicy.postgresCidrs=[]", "PostgreSQL CIDRs"),
    ("--set", "networkPolicy.egressNamespace=", "exact Egress Broker"),
    ("--set", "networkPolicy.mcpHostNamespace=", "MCP Host selectors"),
    ("--set", "autoscaling.minReplicas=1", "at least two replicas"),
    ("--set", "autoscaling.maxReplicas=1", "maximum must be at least"),
    ("--set", "observability.port=0", "observability port"),
    ("--set-json", "networkPolicy.monitoringPodSelector=null", "monitoring requires exact"),
)
for flag, assignment, expected in negative_values:
    result = subprocess.run(
        ["helm", "template", "platform", str(chart), flag, assignment],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    if result.returncode == 0 or expected not in result.stderr:
        failures.append(f"Remote Capability Worker chart accepted invalid override {assignment}")

if failures:
    raise SystemExit("\n".join(f"Remote Capability Worker deployment: {failure}" for failure in failures))
print("Remote Capability Worker static deployment boundary passed.")
PY
