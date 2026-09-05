#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import pathlib
import subprocess

root = pathlib.Path.cwd()
manifest = (root / "crates/platform-orchestration-worker/Cargo.toml").read_text(encoding="utf-8")
source = (root / "crates/platform-orchestration-worker/src/main.rs").read_text(encoding="utf-8")
dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
chart = root / "deploy/helm/insight-platform-orchestration-worker"
failures = []

for dependency in (
    "insight-platform-artifact-rpc.workspace = true",
    "insight-platform-postgres.workspace = true",
    "insight-platform-runtime.workspace = true",
    "insight-platform-worker.workspace = true",
):
    if dependency not in manifest:
        failures.append(f"Orchestration Worker process is missing {dependency}")
for forbidden in (
    "insight-platform-egress.workspace = true",
    "insight-platform-secret-broker.workspace = true",
    "insight-platform-sandbox.workspace = true",
    "reqwest.workspace = true",
):
    if forbidden in manifest:
        failures.append(f"Orchestration Worker bypasses a plane boundary through {forbidden}")
if "/usr/local/bin/platform-orchestration-worker" not in dockerfile:
    failures.append("runtime image is missing platform-orchestration-worker")
for required in (
    "PostgresConnectionBulkheads",
    "ArtifactSchedulerGrpcClient",
    "start_production_orchestration",
    "verify_schema",
    "runtime.is_finished()",
    "runtime.shutdown()",
    "ProcessHttpMetrics",
    "process_observability_router",
):
    if required not in source:
        failures.append(f"Orchestration Worker production composition is missing {required}")
for forbidden in ("async_nats", "reqwest", "aws_sdk", "SecretManager", "KmsClient"):
    if forbidden in source:
        failures.append(f"Orchestration Worker owns a forbidden external client: {forbidden}")

try:
    subprocess.run(["helm", "lint", str(chart)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    rendered = subprocess.run(["helm", "template", "platform", str(chart)], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True).stdout
except (FileNotFoundError, subprocess.CalledProcessError) as error:
    failures.append(f"Orchestration Worker Helm contract did not render: {error}")
    rendered = ""

for needle in (
    'kind: Deployment',
    'kind: HorizontalPodAutoscaler',
    'kind: PodDisruptionBudget',
    'kind: ServiceMonitor',
    'name: observability',
    'path: /readyz',
    'path: /metrics',
    'command: ["/usr/local/bin/platform-orchestration-worker"]',
    'insight.platform/workload-role: orchestration-worker',
    'automountServiceAccountToken: false',
    'readOnlyRootFilesystem: true',
    'allowPrivilegeEscalation: false',
    'PLATFORM_ORCHESTRATION_WORKER_DATABASE_URL',
    'PLATFORM_ORCHESTRATION_WORKER_ARTIFACT_CERT_PATH',
    'app.kubernetes.io/component: artifact-data-worker',
):
    if needle not in rendered:
        failures.append(f"rendered Orchestration Worker contract is missing {needle}")
for forbidden in (
    'kind: Ingress', 'hostNetwork: true', 'hostPID: true',
    'privileged: true', 'automountServiceAccountToken: true', 'NATS',
    'PLATFORM_ORCHESTRATION_WORKER_EGRESS_', 'PLATFORM_ORCHESTRATION_WORKER_SANDBOX_',
):
    if forbidden in rendered:
        failures.append(f"rendered Orchestration Worker has forbidden capability {forbidden}")
if rendered.count('\nkind: Deployment\n') != 1 or rendered.count('\nkind: NetworkPolicy\n') != 2:
    failures.append("Orchestration Worker must render one workload and two NetworkPolicies")

negative_values = (
    ("--set", "replicas=1", "at least two replicas"),
    ("--set", "image.digest=latest", "exact sha256"),
    ("--set-json", "networkPolicy.postgresCidrs=[]", "PostgreSQL CIDRs"),
    ("--set", "networkPolicy.artifactPort=0", "Artifact port"),
    ("--set", "artifactTls.keys.privateKey=", "Artifact mTLS projected keys"),
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
        failures.append(f"Orchestration Worker chart accepted invalid override {assignment}")

if failures:
    raise SystemExit("\n".join(f"Orchestration Worker deployment: {failure}" for failure in failures))
print("Orchestration Worker static deployment boundary passed.")
PY
