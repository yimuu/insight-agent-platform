#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import pathlib
import subprocess

root = pathlib.Path.cwd()
manifest = (root / "crates/platform-mcp-cleanup-worker/Cargo.toml").read_text(encoding="utf-8")
source = (root / "crates/platform-mcp-cleanup-worker/src/main.rs").read_text(encoding="utf-8")
dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
egress_values = (root / "deploy/helm/insight-platform-security-egress/values.yaml").read_text(encoding="utf-8")
chart = root / "deploy/helm/insight-platform-mcp-cleanup-worker"
failures = []

for dependency in ("insight-platform-egress-rpc.workspace = true", "insight-platform-mcp-host.workspace = true", "insight-platform-observability.workspace = true", "insight-platform-postgres.workspace = true"):
    if dependency not in manifest:
        failures.append(f"cleanup process is missing {dependency}")
for forbidden in ("insight-platform-egress.workspace = true", "insight-platform-secret-broker.workspace = true"):
    if forbidden in manifest:
        failures.append(f"cleanup process owns forbidden dependency {forbidden}")
if "/usr/local/bin/platform-mcp-cleanup-worker" not in dockerfile:
    failures.append("runtime image is missing platform-mcp-cleanup-worker")
if "insight.platform/workload-namespace: mcp-cleanup-worker" not in egress_values:
    failures.append("Egress policy does not admit the cleanup worker namespace")
if "McpOAuthPkceCleanupWorker::new" not in source or "McpOAuthPkceCleanupConsumer::new" not in source:
    failures.append("cleanup process does not compose the durable cleanup owner")

try:
    subprocess.run(["helm", "lint", str(chart)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    rendered = subprocess.run(["helm", "template", "platform", str(chart)], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True).stdout
except (FileNotFoundError, subprocess.CalledProcessError) as error:
    failures.append(f"cleanup Helm contract did not render: {error}")
    rendered = ""

for required in ("kind: Deployment", "kind: PodDisruptionBudget", "kind: ServiceMonitor", "name: default-deny", "insight.platform/workload-role: mcp-cleanup-worker", "PLATFORM_MCP_CLEANUP_CONFIG_DIGEST", "PLATFORM_MCP_CLEANUP_DATABASE_URL", "PLATFORM_MCP_CLEANUP_EGRESS_CA_PATH", "app.kubernetes.io/component: egress-broker", "path: /readyz", "path: /metrics", "name: observability"):
    if required not in rendered:
        failures.append(f"cleanup render is missing {required}")
for forbidden in ("AWS_ACCESS_KEY", "AWS_SECRET", "SECRET_MANAGER", "KMS_ENDPOINT"):
    if forbidden in rendered:
        failures.append(f"cleanup Deployment contains forbidden provider credential: {forbidden}")

if failures:
    raise SystemExit("\n".join(f"MCP cleanup deployment: {failure}" for failure in failures))
print("MCP cleanup worker static deployment boundary passed.")
PY
