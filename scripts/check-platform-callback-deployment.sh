#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import pathlib
import re
import subprocess

root = pathlib.Path.cwd()
manifest = (root / "crates/platform-callback-api/Cargo.toml").read_text(encoding="utf-8")
source = (root / "crates/platform-callback-api/src/main.rs").read_text(encoding="utf-8")
dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
egress_values = (root / "deploy/helm/insight-platform-security-egress/values.yaml").read_text(encoding="utf-8")
chart = root / "deploy/helm/insight-platform-callback-api"
failures = []

for dependency in (
    "insight-platform-api.workspace = true",
    "insight-platform-egress-rpc.workspace = true",
    "insight-platform-postgres.workspace = true",
    "insight-platform-observability.workspace = true",
):
    if dependency not in manifest:
        failures.append(f"callback process is missing {dependency}")
if "insight-platform-egress.workspace = true" in manifest or "insight-platform-secret-broker.workspace = true" in manifest:
    failures.append("callback process must not own outbound HTTP or Secret Provider clients")
if "/usr/local/bin/platform-callback-api" not in dockerfile:
    failures.append("runtime image is missing platform-callback-api")
if "insight.platform/workload-namespace: callback-api" not in egress_values:
    failures.append("Egress ingress policy does not admit the callback workload namespace")
if "MCP_OAUTH_CALLBACK_PATH" not in (root / "crates/platform-api/src/lib.rs").read_text(encoding="utf-8"):
    failures.append("callback HTTP adapter is not installed")
if re.search(r"reqwest|aws_sdk|SecretManager|KmsClient", source):
    failures.append("callback composition bypasses the Egress RPC boundary")
for metric_contract in (
    'route("/metrics", get(callback_metrics))',
    "ProcessHttpMetrics::install",
    "callback_operation",
):
    if metric_contract not in source:
        failures.append(f"callback observability contract is missing {metric_contract}")

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
    failures.append(f"callback Helm contract did not render: {error}")
    rendered = ""

if rendered.count("kind: Deployment") != 1:
    failures.append("callback chart must render exactly one Deployment")
if rendered.count("kind: Ingress") != 1 or "path: /v1/mcp/oauth/callback" not in rendered or "pathType: Exact" not in rendered:
    failures.append("callback chart must publish only the exact /v1/mcp/oauth/callback ingress path")
if rendered.count("name: default-deny") != 1:
    failures.append("callback namespace requires default-deny NetworkPolicy")
if "/etc/insight/oauth-state-keys" not in rendered:
    failures.append("callback Deployment must mount the OAuth state-key Secret")
if "PLATFORM_CALLBACK_API_DATABASE_URL" not in rendered:
    failures.append("callback Deployment is missing its database authority credential")
for required in (
    "kind: ServiceMonitor",
    "path: /metrics",
    "path: /livez",
    "path: /readyz",
    'insight.platform/monitoring-namespace: "true"',
    "app.kubernetes.io/name: prometheus",
):
    if required not in rendered:
        failures.append(f"callback render is missing {required}")
for forbidden in ("AWS_ACCESS_KEY", "AWS_SECRET", "SECRET_MANAGER", "KMS_ENDPOINT"):
    if forbidden in rendered:
        failures.append(f"callback Deployment contains forbidden external-provider credential: {forbidden}")

if failures:
    raise SystemExit("\n".join(f"callback deployment: {failure}" for failure in failures))
print("Callback API static deployment boundary passed.")
PY
