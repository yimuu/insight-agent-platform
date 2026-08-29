#!/usr/bin/env python3
"""Fail closed when Productization CI loses its path-aware lane boundaries."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
candidate = (ROOT / ".github/workflows/platform-production-candidate.yml").read_text(
    encoding="utf-8"
)
dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
failures: list[str] = []

for marker in (
    "Classify changed paths",
    "Quick contracts",
    "Affected CLI",
    "Affected Console",
    "Workspace lint and checks",
    "Workspace full tests",
    "Required CI summary",
    "scripts/classify-ci-paths.py",
    "needs.changes.outputs.runtime == 'true'",
    "needs.changes.outputs.cli == 'true'",
    "needs.changes.outputs.console == 'true'",
    "needs.changes.outputs.mcp_interop == 'true'",
    "needs.changes.outputs.policy == 'true'",
    "corepack pnpm --dir web/console install --frozen-lockfile",
    "corepack pnpm --dir web/console browser:fixture:qualify",
    "corepack install --global pnpm@11.19.0",
    "corepack install --global pnpm@11.9.0",
    "runs-on: ubuntu-24.04",
    "node-version: \"24\"",
    "workflow_dispatch:",
    "schedule:",
):
    if marker not in ci:
        failures.append(f"path-aware CI misses {marker!r}")

for forbidden in (
    "docker/build-push-action@",
    "docker push",
    "cosign sign",
    "actions/attest@",
):
    if forbidden in ci:
        failures.append(f"ordinary CI contains candidate-only operation {forbidden!r}")

trigger_block = candidate.split("permissions:", 1)[0]
if "workflow_dispatch:" not in trigger_block:
    failures.append("candidate workflow is not explicitly dispatched")
for forbidden_trigger in ("pull_request:", "schedule:", "tags:"):
    if forbidden_trigger in trigger_block:
        failures.append(f"candidate workflow contains automatic trigger {forbidden_trigger!r}")
if "timeout-minutes: 10" not in candidate or candidate.count("timeout-minutes: 10") < 2:
    failures.append("candidate sign and verify steps must retain bounded ten-minute timeouts")
if dockerfile.count("cargo build --locked --release") != 1:
    failures.append("candidate Dockerfile must compile the production closure once")
if "cargo build --locked --release --workspace" not in dockerfile:
    failures.append("candidate Dockerfile must use one workspace binary build graph")

if failures:
    raise SystemExit("\n".join(failures))
print("Productization path-aware CI contract passed.")
