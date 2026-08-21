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

for required in (
    "insight.platform/workload-role: public-gateway",
    "insight.platform/public-gateway-namespace: \"true\"",
    "PLATFORM_GATEWAY_CONFIG_DIGEST",
    "PLATFORM_GATEWAY_DATABASE_URL",
    "PLATFORM_GATEWAY_RUN_EVENT_CURSOR_KEY_PATH",
    "PLATFORM_GATEWAY_RUN_EVENT_CURSOR_KEY_DIGEST",
    "PLATFORM_GATEWAY_ARTIFACT_ENDPOINT",
    "PLATFORM_GATEWAY_ARTIFACT_CA_PATH",
    "PLATFORM_GATEWAY_ARTIFACT_CERT_PATH",
    "PLATFORM_GATEWAY_ARTIFACT_KEY_PATH",
    "secretName: insight-platform-gateway-artifact-client-tls",
    "app.kubernetes.io/component: artifact-gateway",
    "secretName: insight-platform-gateway-run-event-cursor",
    "path: /v1",
    "pathType: Prefix",
    "kind: HorizontalPodAutoscaler",
    "kind: PodDisruptionBudget",
):
    if required not in rendered:
        failures.append(f"Gateway render is missing {required}")
if rendered.count("name: default-deny") != 1:
    failures.append("Gateway namespace requires exactly one default-deny NetworkPolicy")
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
