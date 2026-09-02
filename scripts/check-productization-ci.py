#!/usr/bin/env python3
"""Fail closed when Productization CI loses its path-aware lane boundaries."""

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
candidate = (ROOT / ".github/workflows/platform-production-candidate.yml").read_text(
    encoding="utf-8"
)
starter_journey = (
    ROOT / ".github/workflows/productization-journey.yml"
).read_text(encoding="utf-8")
journey_runner = (ROOT / "scripts/run-productization-journey.sh").read_text(
    encoding="utf-8"
)
dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
development_profile = json.loads(
    (ROOT / "release/development-profile-v1.json").read_text(encoding="utf-8")
)
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
    "scripts/check-product-release.py",
    "scripts/check-required-ci-results.py",
    "scripts/tests/test_product_release.py",
    "scripts/tests/test_platform_observability_redaction.py",
    "scripts/tests/test_required_ci_results.py",
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
    "image: postgres:",
    "image: nats:",
):
    if forbidden in ci:
        failures.append(f"ordinary CI contains candidate-only operation {forbidden!r}")

profile_images = {
    dependency["name"]: dependency["image"]
    for dependency in development_profile["dependencies"]
}
for profile_name, workflow_name in (("postgresql", "postgres"), ("nats", "nats")):
    image = profile_images.get(profile_name)
    if not isinstance(image, str) or not re.fullmatch(
        rf"{workflow_name}@sha256:[0-9a-f]{{64}}", image
    ):
        failures.append(
            f"development profile {profile_name} image is not digest-pinned: {image!r}"
        )
    elif f"image: {image}" not in ci:
        failures.append(
            f"ordinary CI {workflow_name} service does not reuse development profile image {image!r}"
        )

for action in re.findall(r"^\s*-?\s*uses:\s*([^\s#]+)", ci, flags=re.MULTILINE):
    revision = action.rsplit("@", 1)[-1]
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        failures.append(f"ordinary CI action is not pinned to a commit: {action}")

for marker in (
    "RUNTIME_SELECTED: ${{ needs.changes.outputs.runtime }}",
    "CLI_SELECTED: ${{ needs.changes.outputs.cli }}",
    "CONSOLE_SELECTED: ${{ needs.changes.outputs.console }}",
    "MCP_SELECTED: ${{ needs.changes.outputs.mcp_interop }}",
    "POLICY_SELECTED: ${{ needs.changes.outputs.policy }}",
    "steps:\n      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4\n      - name: Reject any selected lane failure",
    "python3 scripts/check-required-ci-results.py",
    '--runtime-selected "$RUNTIME_SELECTED"',
    '--cli-selected "$CLI_SELECTED"',
    '--console-selected "$CONSOLE_SELECTED"',
    '--mcp-interop-selected "$MCP_SELECTED"',
    '--policy-selected "$POLICY_SELECTED"',
    '--changes-result "$CHANGES_RESULT"',
    '--quick-result "$QUICK_RESULT"',
    '--lint-result "$LINT_RESULT"',
    '--test-result "$TEST_RESULT"',
    '--cli-result "$CLI_RESULT"',
    '--console-result "$CONSOLE_RESULT"',
    '--mcp-interop-result "$MCP_RESULT"',
    '--policy-result "$POLICY_RESULT"',
):
    if marker not in ci:
        failures.append(f"required CI summary misses fail-closed marker {marker!r}")

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

journey_trigger = starter_journey.split("permissions:", 1)[0]
if "workflow_dispatch:" not in journey_trigger:
    failures.append("starter journey qualification is not explicitly dispatched")
for forbidden_trigger in ("push:", "pull_request:", "schedule:"):
    if forbidden_trigger in journey_trigger:
        failures.append(
            f"starter journey qualification contains automatic trigger {forbidden_trigger!r}"
        )
for marker in (
    "runs-on: ubuntu-24.04",
    "scripts/run-productization-journey.sh",
    "--report-directory",
    "--console-browser",
    "Record fresh-checkout journey start",
    "--north-star-report",
    "--journey-started-epoch",
    "--fresh-checkout",
    "productization-starter-north-star-${{ github.sha }}",
    'node-version: "24"',
    "pnpm@11.19.0",
    "timeout-minutes: 60",
    "type: choice",
    "--features \"${{ inputs.features }}\"",
):
    if marker not in starter_journey:
        failures.append(f"starter journey qualification misses {marker!r}")
if "scripts/qualify-productization-first-run.py" not in journey_runner:
    failures.append("starter journey runner misses the lightweight first-Run qualifier")
for forbidden in ("cosign sign", "docker/build-push-action@", "docker push"):
    if forbidden in starter_journey:
        failures.append(
            f"starter journey qualification contains candidate-only operation {forbidden!r}"
        )

if failures:
    raise SystemExit("\n".join(failures))
print("Productization path-aware CI contract passed.")
