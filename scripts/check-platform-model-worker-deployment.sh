#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import pathlib
import subprocess

root = pathlib.Path.cwd()
manifest = (root / "crates/platform-model-worker/Cargo.toml").read_text(encoding="utf-8")
source = (root / "crates/platform-model-worker/src/main.rs").read_text(encoding="utf-8")
dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
egress_values = (root / "deploy/helm/insight-platform-security-egress/values.yaml").read_text(encoding="utf-8")
chart = root / "deploy/helm/insight-platform-model-worker"
failures = []

for dependency in (
    "async-nats.workspace = true",
    "insight-platform-egress-rpc.workspace = true",
    "insight-platform-model-adapters.workspace = true",
    "insight-platform-postgres.workspace = true",
    "insight-platform-worker.workspace = true",
):
    if dependency not in manifest:
        failures.append(f"Model Worker process is missing {dependency}")
for forbidden in (
    "insight-platform-egress.workspace = true",
    "insight-platform-secret-broker.workspace = true",
    "insight-platform-artifact-broker.workspace = true",
    "insight-platform-artifact-rpc.workspace = true",
):
    if forbidden in manifest:
        failures.append(f"Model Worker bypasses a broker boundary through {forbidden}")
if "/usr/local/bin/platform-model-worker" not in dockerfile:
    failures.append("runtime image is missing platform-model-worker")
if "insight.platform/workload-namespace: model-worker" not in egress_values:
    failures.append("Egress ingress policy does not admit the Model Worker namespace")
for forbidden in ("reqwest", "aws_sdk", "SecretManager", "KmsClient"):
    if forbidden in source:
        failures.append(f"Model Worker owns a forbidden Provider/Secret client: {forbidden}")
for required in (
    "BufferedNatsModelLiveDeltaSink",
    "InlineModelRequestMaterializer",
    "InlineModelOutputMaterializer",
    "verify_schema",
    "EgressBrokerGrpcClient",
):
    if required not in source:
        failures.append(f"Model Worker production composition is missing {required}")

try:
    subprocess.run(
        ["helm", "lint", str(chart)],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    rendered = subprocess.run(
        ["helm", "template", "platform", str(chart)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout
except (FileNotFoundError, subprocess.CalledProcessError) as error:
    failures.append(f"Model Worker Helm contract did not render: {error}")
    rendered = ""

required_rendered = (
    'kind: Deployment',
    'kind: HorizontalPodAutoscaler',
    'kind: PodDisruptionBudget',
    'command: ["/usr/local/bin/platform-model-worker"]',
    'insight.platform/workload-role: model-worker',
    'automountServiceAccountToken: false',
    'readOnlyRootFilesystem: true',
    'allowPrivilegeEscalation: false',
    'PLATFORM_MODEL_WORKER_DATABASE_URL',
    'PLATFORM_MODEL_WORKER_EGRESS_CERT_PATH',
    'PLATFORM_MODEL_WORKER_NATS_CERT_PATH',
    'name: nats-tls',
)
for needle in required_rendered:
    if needle not in rendered:
        failures.append(f"rendered Model Worker contract is missing {needle}")
for forbidden in (
    'kind: Ingress',
    'kind: Service\n',
    'hostNetwork: true',
    'hostPID: true',
    'privileged: true',
    'AWS_ACCESS_KEY',
    'AWS_SECRET',
    'SECRET_MANAGER',
    'KMS_ENDPOINT',
    'artifact-broker-model',
    'PLATFORM_MODEL_WORKER_ARTIFACT_',
):
    if forbidden in rendered:
        failures.append(f"rendered Model Worker has forbidden capability {forbidden}")
if rendered.count('\nkind: Deployment\n') != 1 or rendered.count('\nkind: NetworkPolicy\n') != 2:
    failures.append("Model Worker must render one workload and two NetworkPolicies")

negative_values = (
    ("--set", "replicas=1", "at least two replicas"),
    ("--set", "image.digest=latest", "exact sha256"),
    ("--set-json", "networkPolicy.postgresCidrs=[]", "PostgreSQL/NATS CIDRs"),
    ("--set-json", "networkPolicy.natsCidrs=[]", "PostgreSQL/NATS CIDRs"),
    ("--set", "networkPolicy.natsPort=0", "NATS port"),
    ("--set", "natsTls.keys.privateKey=", "NATS mTLS projected keys"),
    ("--set", "autoscaling.minReplicas=1", "at least two replicas"),
    ("--set", "autoscaling.maxReplicas=1", "maximum must be at least"),
)
for flag, assignment, expected in negative_values:
    result = subprocess.run(
        ["helm", "template", "platform", str(chart), flag, assignment],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode == 0 or expected not in result.stderr:
        failures.append(f"Model Worker chart accepted invalid override {assignment}")

if failures:
    raise SystemExit("\n".join(f"Model Worker deployment: {failure}" for failure in failures))
print("Model Worker static deployment boundary passed.")
PY
