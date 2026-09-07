#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
import pathlib
import re
import subprocess

root = pathlib.Path.cwd()
chart = root / "deploy/helm/insight-platform-history-maintenance"
subprocess.run(["helm", "lint", str(chart)], check=True)
rendered = subprocess.run(["helm", "template", "history", str(chart)], check=True, capture_output=True, text=True).stdout
for kind in ("Deployment", "HorizontalPodAutoscaler", "PodDisruptionBudget", "ServiceAccount", "ServiceMonitor"):
    assert len(re.findall(rf"^kind: {kind}$", rendered, flags=re.MULTILINE)) == 1, kind
for marker in (
    "insight.platform/component-role: history_maintenance", "name: default-deny",
    'command: ["/usr/local/bin/platform-history-maintenance"]',
    "PLATFORM_HISTORY_MAINTENANCE_CONFIG_DIGEST", "PLATFORM_HISTORY_MAINTENANCE_DATABASE_URL",
    "automountServiceAccountToken: false", "readOnlyRootFilesystem: true",
    "allowPrivilegeEscalation: false", "path: /readyz", "path: /metrics", "port: 5432",
):
    assert marker in rendered, marker
for forbidden in ("AWS_", "ARTIFACT_", "NATS_", "KMS_ENDPOINT", "PROVISION_", "hostNetwork: true", "port: 4222"):
    assert forbidden not in rendered, forbidden
source = (root / "apps/services/platform-maintenance-worker/src/main.rs").read_text()
for marker in ("verify_schema", "executable_digest", "canonical_digest", "scan_history_retention_runs", "purge_public_run_event_prefix", "statement_timeout", "MissedTickBehavior::Skip"):
    assert marker in source, marker
assert "GRANT " not in source and "DELETE FROM" not in source
assert "/usr/local/bin/platform-history-maintenance" in (root / "deploy/images/platform.Dockerfile").read_text()
grants = (root / "crates/adapters/platform-postgres/history-role-grants.sql").read_text()
assert "GRANT EXECUTE ON FUNCTION" in grants
for forbidden in ("GRANT SELECT", "GRANT UPDATE", "GRANT DELETE", "GRANT INSERT", "GRANT ALL"):
    assert forbidden not in grants, forbidden
for primitive in ("history_scan_runs", "history_lock_run", "history_lock_event_prefix", "history_delete_prefix"):
    assert primitive in grants, primitive
print("History maintenance isolated process, executable/config evidence and least-privilege Helm boundary passed.")
PY
