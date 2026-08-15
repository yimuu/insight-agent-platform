#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import pathlib
import re
import subprocess

root = pathlib.Path.cwd()
rpc = (root / "crates/platform-artifact-rpc/Cargo.toml").read_text(encoding="utf-8")
service = (root / "crates/platform-artifact-service/Cargo.toml").read_text(encoding="utf-8")
broker = (root / "crates/platform-artifact-broker/Cargo.toml").read_text(encoding="utf-8")
proto = (root / "proto/insight/platform/v1/artifact_internal.proto").read_text(encoding="utf-8")
grants = (root / "crates/platform-postgres/artifact-broker-grants.sql").read_text(encoding="utf-8")
dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
chart = root / "deploy/helm/insight-platform-artifact-broker"
failures = []

for dependency in (
    "insight-platform-artifact-broker.workspace = true",
    "insight-platform-artifact-rpc.workspace = true",
    "insight-platform-postgres.workspace = true",
):
    if dependency not in service:
        failures.append(f"Artifact service is missing {dependency}")
if "sqlx.workspace = true" not in service:
    failures.append("Artifact service must own the restricted PostgreSQL adapter")
if re.search(r"^sqlx(?:\.|\s*=)", rpc, re.MULTILINE):
    failures.append("Artifact RPC must not own SQL")
for sdk in ("aws-config", "aws-sdk-kms", "aws-sdk-s3"):
    if sdk in rpc:
        failures.append(f"Artifact RPC must not own provider SDK {sdk}")
    if sdk not in broker:
        failures.append(f"Artifact Broker core is missing provider SDK {sdk}")

expected_method = "rpc ReadModelRequest(ClosedArtifactReadRequest) returns (stream ArtifactReadChunk);"
if expected_method not in proto or proto.count("  rpc ") != 1:
    failures.append("Artifact internal RPC must expose exactly the reviewed Model read method")
if "/usr/local/bin/platform-artifact-broker" not in dockerfile:
    failures.append("runtime image is missing platform-artifact-broker")

required_select = {
    "schema_migrations", "invocations", "jobs", "run_values", "artifact_links",
    "artifacts", "artifact_blobs",
}
for table in required_select:
    if table not in grants:
        failures.append(f"Artifact restricted role is missing SELECT authority for {table}")
if re.search(r"GRANT\s+(INSERT|UPDATE|DELETE|TRUNCATE)", grants, re.IGNORECASE):
    failures.append("Artifact restricted role must remain read-only")

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
    failures.append(f"Artifact Broker Helm contract did not render: {error}")
    rendered = ""

required_rendered = (
    "kind: Namespace",
    "kind: Service",
    "kind: Deployment",
    "kind: PodDisruptionBudget",
    "kind: HorizontalPodAutoscaler",
    'command: ["/usr/local/bin/platform-artifact-broker"]',
    "insight.platform/workload-role: artifact-broker",
    "PLATFORM_ARTIFACT_BROKER_DATABASE_URL",
    "PLATFORM_ARTIFACT_BROKER_CLIENT_CA_PATH",
    "automountServiceAccountToken: false",
    "readOnlyRootFilesystem: true",
    "allowPrivilegeEscalation: false",
)
for needle in required_rendered:
    if needle not in rendered:
        failures.append(f"rendered Artifact Broker contract is missing {needle}")
for forbidden in (
    "kind: Ingress",
    "hostNetwork: true",
    "hostPID: true",
    "privileged: true",
    "AWS_ACCESS_KEY",
    "AWS_SECRET_ACCESS_KEY",
    "SECRET_MANAGER",
):
    if forbidden in rendered:
        failures.append(f"rendered Artifact Broker has forbidden capability {forbidden}")
if rendered.count("\nkind: Deployment\n") != 1 or rendered.count("\nkind: NetworkPolicy\n") != 2:
    failures.append("Artifact Broker must render one workload and two NetworkPolicies")

negative_values = (
    ("--set", "replicas=1", "at least two replicas"),
    ("--set", "image.digest=latest", "exact lowercase sha256"),
    ("--set-json", "networkPolicy.postgresCidrs=[]", "CIDRs are required"),
    ("--set-json", "networkPolicy.storageProviderCidrs=[]", "CIDRs are required"),
    ("--set", "networkPolicy.modelWorkerPodSelector=", "caller selectors"),
    ("--set", "serviceAccount.annotations=", "workload-identity annotation"),
    ("--set", "autoscaling.minReplicas=1", "at least two replicas"),
)
for flag, assignment, expected in negative_values:
    result = subprocess.run(
        ["helm", "template", "platform", str(chart), flag, assignment],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode == 0 or expected not in result.stderr:
        failures.append(f"Artifact Broker chart accepted invalid override {assignment}")

if failures:
    raise SystemExit("\n".join(f"Artifact Broker deployment: {failure}" for failure in failures))
print("Artifact Broker static deployment boundary passed.")
PY
