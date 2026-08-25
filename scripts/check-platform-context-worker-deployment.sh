#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rendered="$(mktemp)"
trap 'rm -f "$rendered"' EXIT

helm template context-worker "$repo_root/deploy/helm/insight-platform-context-worker" >"$rendered"
python3 - "$repo_root" "$rendered" <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1])
rendered = Path(sys.argv[2]).read_text(encoding="utf-8")
dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
source = (root / "crates/platform-context-worker/src/main.rs").read_text(encoding="utf-8")
failures = []

required = [
    "/usr/local/bin/platform-context-worker",
    "insight.platform/workload-namespace: context-worker",
    "insight.platform/workload-role: context-worker",
    "automountServiceAccountToken: false",
    "readOnlyRootFilesystem: true",
    "allowPrivilegeEscalation: false",
    "PLATFORM_CONTEXT_WORKER_CONFIG_DIGEST",
    "PLATFORM_CONTEXT_WORKER_DATABASE_URL",
    "kind: NetworkPolicy",
    "port: 5432",
]
for token in required:
    if token not in rendered and token not in dockerfile:
        failures.append(f"missing deployment invariant: {token}")
for forbidden in ["EGRESS_", "SECRET_", "NATS_", "SANDBOX_", "platform-egress", "port: 4222"]:
    if forbidden in rendered or forbidden in source:
        failures.append(f"NativeCatalog Context Worker gained forbidden dependency: {forbidden}")
if "platform-context-worker" not in dockerfile:
    failures.append("runtime image is missing platform-context-worker")
if failures:
    raise SystemExit("\n".join(failures))
PY
