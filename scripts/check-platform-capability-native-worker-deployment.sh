#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import pathlib
import subprocess

root = pathlib.Path.cwd()
manifest = (root / "crates/platform-capability-worker/Cargo.toml").read_text(encoding="utf-8")
source = (root / "crates/platform-capability-worker/src/main.rs").read_text(encoding="utf-8")
library = (root / "crates/platform-capability-worker/src/lib.rs").read_text(encoding="utf-8")
dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
chart = root / "deploy/helm/insight-platform-capability-native-worker"
failures = []

for dependency in (
    "insight-platform-capability-adapters.workspace = true",
    "insight-platform-postgres.workspace = true",
    "insight-platform-worker.workspace = true",
):
    if dependency not in manifest:
        failures.append(f"Native Capability Worker process is missing {dependency}")
for forbidden in (
    "insight-platform-egress.workspace = true",
    "insight-platform-secret-broker.workspace = true",
    "insight-platform-sandbox.workspace = true",
    "reqwest.workspace = true",
):
    if forbidden in manifest:
        failures.append(f"Native Capability Worker bypasses a plane boundary through {forbidden}")
if "/usr/local/bin/platform-capability-native-worker" not in dockerfile:
    failures.append("runtime image is missing platform-capability-native-worker")
for required in (
    "parse_strict_json",
    "canonical_digest",
    "BuiltinEchoCapabilityAdapter::installed_descriptor",
    "install_builtin_native_adapters",
    "business_max_connections",
    "critical_control_max_connections",
    "verify_schema",
    "CancellationToken",
    "driver.run",
):
    if required not in source:
        failures.append(f"Native Capability Worker production composition is missing {required}")
for forbidden in ("McpHost", "EgressBrokerGrpcClient", "reqwest", "async_nats", "aws_sdk", "SecretManager", "KmsClient"):
    if forbidden in source:
        failures.append(f"Native Capability Worker owns a forbidden external client: {forbidden}")
for required in (
    "recover_expired_capability_jobs",
    "DriveExpiredCapabilityJobs",
    "initial_scan_delay",
    "report.recovered",
):
    if required not in library:
        failures.append(f"Native Capability Worker recovery loop is missing {required}")

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
    failures.append(f"Native Capability Worker Helm contract did not render: {error}")
    rendered = ""

for needle in (
    "kind: Deployment",
    "kind: HorizontalPodAutoscaler",
    "kind: PodDisruptionBudget",
    'command: ["/usr/local/bin/platform-capability-native-worker"]',
    "insight.platform/workload-role: capability-native-worker",
    "insight.platform/workload-namespace: capability-native-worker",
    "automountServiceAccountToken: false",
    "readOnlyRootFilesystem: true",
    "allowPrivilegeEscalation: false",
    "capabilities: {drop: [\"ALL\"]}",
    "PLATFORM_CAPABILITY_NATIVE_WORKER_CONFIG_DIGEST",
    "PLATFORM_CAPABILITY_NATIVE_WORKER_DATABASE_URL",
):
    if needle not in rendered:
        failures.append(f"rendered Native Capability Worker contract is missing {needle}")
for forbidden in (
    "kind: Ingress",
    "kind: Service\n",
    "hostNetwork: true",
    "hostPID: true",
    "privileged: true",
    "automountServiceAccountToken: true",
    "PLATFORM_CAPABILITY_NATIVE_WORKER_EGRESS_",
    "PLATFORM_CAPABILITY_NATIVE_WORKER_MCP_",
    "PLATFORM_CAPABILITY_NATIVE_WORKER_SANDBOX_",
    "PLATFORM_CAPABILITY_NATIVE_WORKER_ARTIFACT_",
):
    if forbidden in rendered:
        failures.append(f"rendered Native Capability Worker has forbidden capability {forbidden}")
if rendered.count("\nkind: Deployment\n") != 1 or rendered.count("\nkind: NetworkPolicy\n") != 2:
    failures.append("Native Capability Worker must render one workload and two NetworkPolicies")
if "port: 5432" not in rendered or "port: 53" not in rendered:
    failures.append("Native Capability Worker egress must contain only DNS and PostgreSQL destinations")

negative_values = (
    ("--set", "replicas=1", "at least two replicas"),
    ("--set", "image.digest=latest", "exact sha256"),
    ("--set", "config.digest=latest", "exact sha256"),
    ("--set-json", "networkPolicy.postgresCidrs=[]", "PostgreSQL CIDRs"),
    ("--set", "autoscaling.minReplicas=1", "at least two replicas"),
    ("--set", "autoscaling.maxReplicas=1", "maximum must be at least"),
)
for flag, assignment, expected in negative_values:
    result = subprocess.run(
        ["helm", "template", "platform", str(chart), flag, assignment],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    if result.returncode == 0 or expected not in result.stderr:
        failures.append(f"Native Capability Worker chart accepted invalid override {assignment}")

if failures:
    raise SystemExit("\n".join(f"Native Capability Worker deployment: {failure}" for failure in failures))
print("Native Capability Worker static deployment boundary passed.")
PY
