#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rendered="$(mktemp)"
trap 'rm -f "$rendered"' EXIT
helm template remote-context-worker "$repo_root/deploy/helm/insight-platform-remote-context-worker" >"$rendered"
python3 - "$repo_root" "$rendered" <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1])
rendered = Path(sys.argv[2]).read_text(encoding="utf-8")
dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
source = (root / "crates/platform-context-worker/src/remote_main.rs").read_text(encoding="utf-8")
required = [
    "/usr/local/bin/platform-remote-context-worker",
    "insight.platform/workload-role: context-worker",
    "automountServiceAccountToken: false",
    "readOnlyRootFilesystem: true",
    "allowPrivilegeEscalation: false",
    "PLATFORM_REMOTE_CONTEXT_WORKER_CONFIG_DIGEST",
    "PLATFORM_REMOTE_CONTEXT_WORKER_DATABASE_URL",
    "PLATFORM_REMOTE_CONTEXT_WORKER_EGRESS_CA_PATH",
    "PLATFORM_REMOTE_CONTEXT_WORKER_EGRESS_CERT_PATH",
    "PLATFORM_REMOTE_CONTEXT_WORKER_EGRESS_KEY_PATH",
    "kind: NetworkPolicy",
    "port: 5432",
    "port: 8443",
]
failures = [f"missing Remote Context deployment invariant: {token}" for token in required if token not in rendered and token not in dockerfile and token not in source]
for forbidden in ["SECRET_BROKER", "NATS_", "SANDBOX_", "port: 4222"]:
    if forbidden in rendered or forbidden in source:
        failures.append(f"Remote Context Worker gained forbidden dependency: {forbidden}")
if failures:
    raise SystemExit("\n".join(failures))
PY
