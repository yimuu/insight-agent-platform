#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import pathlib
import subprocess

root = pathlib.Path.cwd()
manifest = (root / "crates/platform-gateway/Cargo.toml").read_text(encoding="utf-8")
source = (root / "crates/platform-gateway/src/main.rs").read_text(encoding="utf-8")
dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
chart = root / "deploy/helm/insight-platform-gateway"
failures = []

for dependency in (
    "insight-platform-api.workspace = true",
    "insight-platform-postgres.workspace = true",
    "insight-platform-observability.workspace = true",
):
    if dependency not in manifest:
        failures.append(f"Gateway process is missing {dependency}")
for forbidden in (
    "insight-platform-artifact-broker.workspace = true",
    "insight-platform-egress.workspace = true",
    "insight-platform-secret-broker.workspace = true",
):
    if forbidden in manifest:
        failures.append(f"Gateway must not own privileged backend dependency {forbidden}")
if "/usr/local/bin/platform-gateway" not in dockerfile:
    failures.append("runtime image is missing platform-gateway")
if "authenticate_public_request" not in source or "read_public_operation" not in source:
    failures.append("Gateway does not compose authentication and Operation authority")
for role_contract in ("ProcessRole::ManagementApi", "ProcessRole::RuntimeApi"):
    if role_contract not in source:
        failures.append(f"Gateway does not close API process role {role_contract}")
for metric_contract in (
    'route("/metrics", get(prometheus_metrics))',
    "ProcessHttpMetrics::install",
    "gateway_operation",
):
    if metric_contract not in source:
        failures.append(f"Gateway observability contract is missing {metric_contract}")

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
    failures.append(f"Gateway Helm contract did not render: {error}")
    rendered = ""

for override, expected in (
    ("roles.management-api.componentRole=runtime_api", "management-api componentRole is invalid"),
    ("roles.runtime-api.replicas=1", "runtime-api replicas must be at least 2"),
):
    rejected = subprocess.run(
        ["helm", "template", "platform", str(chart), "--set-string", override],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    if rejected.returncode == 0 or expected not in rejected.stderr:
        failures.append(f"Gateway Helm contract accepted invalid override {override}")

for required in (
    "insight.platform/workload-role: management-api",
    "insight.platform/workload-role: runtime-api",
    "insight.platform/component-role: management_api",
    "insight.platform/component-role: runtime_api",
    "insight.platform/public-gateway-namespace: \"true\"",
    "PLATFORM_GATEWAY_CONFIG_DIGEST",
    "PLATFORM_GATEWAY_DATABASE_URL",
    "PLATFORM_GATEWAY_RUN_EVENT_CURSOR_KEY_PATH",
    "PLATFORM_GATEWAY_RUN_EVENT_CURSOR_KEY_DIGEST",
    "PLATFORM_GATEWAY_ARTIFACT_ENDPOINT",
    "PLATFORM_GATEWAY_ARTIFACT_CA_PATH",
    "PLATFORM_GATEWAY_ARTIFACT_CERT_PATH",
    "PLATFORM_GATEWAY_ARTIFACT_KEY_PATH",
    "secretName: insight-platform-runtime-api-artifact-client-tls",
    "app.kubernetes.io/component: artifact-gateway",
    "secretName: insight-platform-runtime-api-run-event-cursor",
    "path: /v1/agents",
    "path: /v1/runs",
    "path: /v1/artifacts",
    "pathType: Prefix",
    "kind: HorizontalPodAutoscaler",
    "kind: PodDisruptionBudget",
    "kind: ServiceMonitor",
    "path: /metrics",
    "insight.platform/monitoring-namespace: \"true\"",
    "app.kubernetes.io/name: prometheus",
):
    if required not in rendered:
        failures.append(f"Gateway render is missing {required}")
if rendered.count("name: default-deny") != 1:
    failures.append("Gateway namespace requires exactly one default-deny NetworkPolicy")
if rendered.count("\nkind: Deployment\n") != 2:
    failures.append("Management and Runtime API require exactly two Deployments")
if rendered.count("\nkind: HorizontalPodAutoscaler\n") != 2:
    failures.append("Management and Runtime API require independent HPAs")
if rendered.count("\nkind: PodDisruptionBudget\n") != 2:
    failures.append("Management and Runtime API require independent PDBs")

deployments = [
    document for document in rendered.split("\n---\n") if "kind: Deployment\n" in document
]
management = next((item for item in deployments if "app.kubernetes.io/component: management-api" in item), "")
runtime = next((item for item in deployments if "app.kubernetes.io/component: runtime-api" in item), "")
if not management or not runtime:
    failures.append("Management and Runtime API Deployment identities are incomplete")
for runtime_only in (
    "PLATFORM_GATEWAY_RUN_EVENT_CURSOR_KEY_PATH",
    "PLATFORM_GATEWAY_ARTIFACT_ENDPOINT",
    "artifact-tls",
):
    if runtime_only in management:
        failures.append(f"Management API received Runtime-only authority {runtime_only}")
    if runtime_only not in runtime:
        failures.append(f"Runtime API is missing required authority {runtime_only}")
for forbidden in (
    "AWS_ACCESS_KEY", "AWS_SECRET", "SECRET_MANAGER", "KMS_ENDPOINT",
    "ARTIFACT_STORAGE", "SANDBOX_RUNTIME",
):
    if forbidden in rendered:
        failures.append(f"Gateway Deployment contains forbidden privileged setting: {forbidden}")

if failures:
    raise SystemExit("\n".join(f"Gateway deployment: {failure}" for failure in failures))
print("Public Gateway static deployment boundary passed.")
PY
