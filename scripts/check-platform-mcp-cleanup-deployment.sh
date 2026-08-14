#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
chart="$repo_root/deploy/helm/insight-platform-mcp-cleanup-worker"
rendered="$(mktemp "${TMPDIR:-/tmp}/insight-platform-mcp-cleanup.XXXXXX")"
trap 'rm -f "$rendered"' EXIT

helm lint "$chart"
helm template candidate "$chart" >"$rendered"

python3 - "$rendered" <<'PY'
import pathlib
import sys

text = pathlib.Path(sys.argv[1]).read_text()
required = [
    'kind: Deployment',
    'kind: NetworkPolicy',
    'kind: PodDisruptionBudget',
    'command: ["/usr/local/bin/platform-mcp-cleanup-worker"]',
    'insight.platform/workload-role: mcp-host',
    'automountServiceAccountToken: false',
    'readOnlyRootFilesystem: true',
    'allowPrivilegeEscalation: false',
    'PLATFORM_MCP_CLEANUP_DATABASE_URL',
    'PLATFORM_MCP_CLEANUP_EGRESS_CERT_PATH',
]
for needle in required:
    if needle not in text:
        raise SystemExit(f"missing MCP cleanup deployment contract: {needle}")
for forbidden in ['kind: Ingress', 'kind: Service\n', 'hostNetwork: true', 'hostPID: true', 'privileged: true']:
    if forbidden in text:
        raise SystemExit(f"forbidden MCP cleanup deployment capability: {forbidden}")
if text.count('kind: Deployment') != 1 or text.count('kind: NetworkPolicy') != 2:
    raise SystemExit('MCP cleanup deployment must render one workload and two NetworkPolicies')
print('MCP OAuth cleanup deployment boundary passed.')
PY
