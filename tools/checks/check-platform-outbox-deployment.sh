#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import json
import pathlib
import re
import subprocess

root = pathlib.Path.cwd()
chart = root / "deploy/helm/insight-platform-outbox-worker"
subprocess.run(["helm", "lint", str(chart)], check=True)
rendered = subprocess.run(["helm", "template", "outbox", str(chart)], check=True, capture_output=True, text=True).stdout
for kind in ("Deployment", "HorizontalPodAutoscaler", "PodDisruptionBudget", "ServiceAccount", "ServiceMonitor"):
    assert len(re.findall(rf"^kind: {kind}$", rendered, flags=re.MULTILINE)) == 1, kind
for marker in (
    "insight.platform/component-role: outbox_worker", "name: default-deny",
    'command: ["/usr/local/bin/platform-outbox-worker"]',
    "PLATFORM_OUTBOX_CONFIG_DIGEST", "PLATFORM_OUTBOX_DATABASE_URL",
    "PLATFORM_OUTBOX_NATS_CA_PATH", "PLATFORM_OUTBOX_NATS_CERT_PATH", "PLATFORM_OUTBOX_NATS_KEY_PATH",
    "automountServiceAccountToken: false", "readOnlyRootFilesystem: true",
    "allowPrivilegeEscalation: false", "path: /readyz", "path: /metrics",
    "port: 4222", "port: 5432", "ephemeral-storage:",
):
    assert marker in rendered, marker
for forbidden in ("AWS_ACCESS_KEY", "AWS_SECRET", "KMS_ENDPOINT", "PROVISION_", "hostNetwork: true"):
    assert forbidden not in rendered, forbidden
stream = json.loads((root / "deploy/jetstream/committed-events-v1.json").read_bytes())
assert stream["schema_version"] == 1 and stream["maximum_age_seconds"] == 0
assert stream["maximum_messages"] > 0 and stream["maximum_bytes"] > 0
permissions = json.loads((root / "deploy/jetstream/outbox-publisher-permissions.json").read_bytes())
assert permissions["publish"]["allow"] == ["insight.platform.v1.committed", "$JS.API.STREAM.INFO.INSIGHT_COMMITTED_V1"]
assert permissions["subscribe"]["allow"] == ["_INBOX.insight.outbox.>"]
dockerfile = (root / "deploy/images/platform.Dockerfile").read_text()
for binary in ("platform-outbox-worker", "platform-jetstream-provision", "platform-database-role"):
    assert f"/usr/local/bin/{binary}" in dockerfile
source = (root / "apps/services/platform-outbox-worker/src/main.rs").read_text()
assert "require_tls(true)" in source and "verify_schema" in source
assert "create_stream" not in source and "update_stream" not in source
grants = (root / "crates/adapters/platform-postgres/outbox-role-grants.sql").read_text()
assert "GRANT SELECT (tenant_id, event_id, aggregate_id" in grants
assert "GRANT SELECT ON insight_platform.events" not in grants
assert "GRANT DELETE" not in grants and "GRANT INSERT" not in grants
print("Outbox process, provisioning, least-privilege assets and rendered Helm boundary passed.")
PY
