#!/usr/bin/env bash
set -euo pipefail

workspace="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
project=""
report_directory=""
features=""
profile_label="starter"
insight_bin="${PLATFORM_INSIGHT_BIN:-$workspace/target/release/insight}"
keep_dependencies=false
console_browser=false
node_bin="${PLATFORM_PRODUCTIZATION_NODE_BIN:-}"
browser_bin="${INSIGHT_CONSOLE_BROWSER_BIN:-}"
corepack_bin=""
north_star_report=""
journey_started_epoch=""
fresh_checkout=false
sandbox_evidence=""
aggregate_report=""
release_candidate=""
console_bundle=""
sandbox_environment=""
insight_bin_explicit=false

usage() {
  cat <<'EOF'
Usage: scripts/run-productization-journey.sh [options]

Runs a fresh selected-profile productization journey against real local Platform roles.

Options:
  --project <new-path>          Use this not-yet-existing project path instead of mktemp.
  --report-directory <path>    Write and validate exact-revision scenario evidence.
  --features <list|all>        Add a canonical feature closure to starter (default: none).
  --insight-bin <path>         Use an existing insight binary (default: target/release/insight).
  --keep-dependencies          Leave the exact Docker Compose dependencies running.
  --console-browser           Run the static Console against the fresh real Gateway in headless Chromium.
  --node-bin <path>           Node.js executable for the remote-framework fixture and --console-browser (default: current or login-shell PATH).
  --browser-bin <path>        Chromium/Chrome executable for --console-browser.
  --north-star-report <path>  Write a closed checkout-to-first-Run qualification report.
  --journey-started-epoch <s> Start clock captured before the fresh checkout.
  --fresh-checkout            Assert the caller started the clock before a fresh checkout.
  --sandbox-evidence <path>   Consume same-revision real OpenSandbox L3 evidence (required by sandbox/all).
  --aggregate-report <path>   Write strict exact-revision 10/10 evidence (required by all).
  --release-candidate <dir>   Consume a locally verified signed release candidate without source fallback.
  --console-bundle <dir>      Serve the extracted candidate Console bundle (candidate mode only).
  --sandbox-environment <p>   Bind reports to the fresh Kind bootstrap environment (sandbox/all).
  -h, --help                   Show this help.
EOF
}

while (($# > 0)); do
  case "$1" in
    --project)
      (($# >= 2)) || { echo "--project requires a value" >&2; exit 2; }
      project=$2
      shift 2
      ;;
    --report-directory)
      (($# >= 2)) || { echo "--report-directory requires a value" >&2; exit 2; }
      report_directory=$2
      shift 2
      ;;
    --features)
      (($# >= 2)) || { echo "--features requires a value" >&2; exit 2; }
      features=$2
      shift 2
      ;;
    --insight-bin)
      (($# >= 2)) || { echo "--insight-bin requires a value" >&2; exit 2; }
      insight_bin=$2
      insight_bin_explicit=true
      shift 2
      ;;
    --keep-dependencies)
      keep_dependencies=true
      shift
      ;;
    --console-browser)
      console_browser=true
      shift
      ;;
    --node-bin)
      (($# >= 2)) || { echo "--node-bin requires a value" >&2; exit 2; }
      node_bin=$2
      shift 2
      ;;
    --browser-bin)
      (($# >= 2)) || { echo "--browser-bin requires a value" >&2; exit 2; }
      browser_bin=$2
      shift 2
      ;;
    --north-star-report)
      (($# >= 2)) || { echo "--north-star-report requires a value" >&2; exit 2; }
      north_star_report=$2
      shift 2
      ;;
    --journey-started-epoch)
      (($# >= 2)) || { echo "--journey-started-epoch requires a value" >&2; exit 2; }
      journey_started_epoch=$2
      shift 2
      ;;
    --fresh-checkout)
      fresh_checkout=true
      shift
      ;;
    --sandbox-evidence)
      (($# >= 2)) || { echo "--sandbox-evidence requires a value" >&2; exit 2; }
      sandbox_evidence=$2
      shift 2
      ;;
    --aggregate-report)
      (($# >= 2)) || { echo "--aggregate-report requires a value" >&2; exit 2; }
      aggregate_report=$2
      shift 2
      ;;
    --release-candidate)
      (($# >= 2)) || { echo "--release-candidate requires a value" >&2; exit 2; }
      release_candidate=$2
      shift 2
      ;;
    --console-bundle)
      (($# >= 2)) || { echo "--console-bundle requires a value" >&2; exit 2; }
      console_bundle=$2
      shift 2
      ;;
    --sandbox-environment)
      (($# >= 2)) || { echo "--sandbox-environment requires a value" >&2; exit 2; }
      sandbox_environment=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unsupported option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! features="$(python3 - "$features" <<'PY'
import sys

raw = sys.argv[1]
allowed = {"context", "mcp", "model", "remote-capability", "sandbox"}
if not raw:
    print("")
elif raw == "all":
    print("all")
else:
    values = raw.split(",")
    if any(not value or value.strip() != value or value not in allowed for value in values):
        raise SystemExit("invalid feature closure")
    if len(values) != len(set(values)):
        raise SystemExit("duplicate feature")
    selected = set(values)
    if "sandbox" in selected:
        selected.add("remote-capability")
    print(",".join(sorted(selected)))
PY
)"; then
  echo "--features must be all or a unique comma-separated set of context,mcp,model,remote-capability,sandbox" >&2
  exit 2
fi
if [[ "$features" == "all" ]]; then
  profile_label="all"
elif [[ -n "$features" ]]; then
  profile_label="starter+$features"
fi
if [[ -n "$north_star_report" ]]; then
  if [[ -n "$features" ]]; then
    echo "--north-star-report only qualifies the starter profile" >&2
    exit 2
  fi
  if [[ "$fresh_checkout" != true || ! "$journey_started_epoch" =~ ^[0-9]{10}$ ]]; then
    echo "--north-star-report requires --fresh-checkout and a ten-digit --journey-started-epoch captured before checkout" >&2
    exit 2
  fi
fi
if [[ "$features" == "all" ]]; then
  if [[ -z "$report_directory" || -z "$aggregate_report" || "$console_browser" != true ]]; then
    echo "--features all requires --report-directory, --aggregate-report, and --console-browser" >&2
    exit 2
  fi
fi
if [[ "$features" == "all" || ",$features," == *",sandbox,"* ]]; then
  if [[ -z "$sandbox_evidence" || -z "$sandbox_environment" ]]; then
    echo "sandbox qualification requires --sandbox-evidence and --sandbox-environment" >&2
    exit 2
  fi
  for sandbox_input in "$sandbox_evidence" "$sandbox_environment"; do
    if [[ ! -f "$sandbox_input" || -L "$sandbox_input" ]]; then
      echo "sandbox qualification inputs must be regular, non-symlink files" >&2
      exit 2
    fi
  done
elif [[ -n "$sandbox_evidence" || -n "$sandbox_environment" ]]; then
  echo "--sandbox-evidence and --sandbox-environment are only valid for a sandbox feature closure" >&2
  exit 2
fi
if [[ -n "$aggregate_report" && "$features" != "all" ]]; then
  echo "--aggregate-report is only valid with --features all" >&2
  exit 2
fi
if [[ -n "$project" && -e "$project" ]]; then
  echo "--project must name a path that does not already exist: $project" >&2
  exit 2
fi
if [[ -n "$report_directory" && -e "$report_directory" ]]; then
  echo "--report-directory must name a path that does not already exist" >&2
  exit 2
fi
if [[ -n "$report_directory" && -n "$(git -C "$workspace" status --porcelain)" ]]; then
  echo "exact-revision reports require a clean Git working tree" >&2
  exit 2
fi
if [[ -n "$release_candidate" ]]; then
  if [[ "$(uname -s)" != "Linux" || "$insight_bin_explicit" != true ]]; then
    echo "--release-candidate requires Linux and an explicit --insight-bin" >&2
    exit 2
  fi
  if [[ ! -d "$release_candidate" || -L "$release_candidate" ]]; then
    echo "--release-candidate must be a real directory" >&2
    exit 2
  fi
  for name in release-bundle.json release-bundle.signature.json; do
    if [[ ! -f "$release_candidate/$name" || -L "$release_candidate/$name" ]]; then
      echo "signed candidate is missing a regular $name" >&2
      exit 2
    fi
  done
  if [[ "$console_browser" != true || -z "$console_bundle" ]]; then
    echo "--release-candidate requires --console-browser and --console-bundle" >&2
    exit 2
  fi
  if [[ ! -d "$console_bundle" || -L "$console_bundle" || ! -f "$console_bundle/index.html" || -L "$console_bundle/index.html" ]]; then
    echo "--console-bundle must be an extracted real directory with a regular index.html" >&2
    exit 2
  fi
elif [[ -n "$console_bundle" ]]; then
  echo "--console-bundle is only valid with --release-candidate" >&2
  exit 2
fi
if ! command -v pgrep >/dev/null 2>&1; then
  echo "pgrep is required to prove that no repository-local Platform process is already running" >&2
  exit 2
fi
requires_node=false
if [[ "$features" == "all" || (",$features," == *",remote-capability,"* && ",$features," == *",sandbox,"*) || "$console_browser" == true ]]; then
  requires_node=true
fi
if [[ "$requires_node" == true ]]; then
  if [[ -z "$node_bin" ]]; then
    node_bin="$(command -v node || true)"
  fi
  if [[ -z "$node_bin" ]]; then
    login_shell="${SHELL:-/bin/zsh}"
    if [[ -x "$login_shell" ]]; then
      # NVM's lazy-loading shell plugin exposes `node` as a function until first use, so ask the
      # runtime for its physical executable instead of trusting `command -v` to return a path.
      node_bin="$("$login_shell" -lic 'node -p process.execPath' 2>/dev/null | tail -n 1)"
    fi
  fi
  if [[ -z "$node_bin" || ! -x "$node_bin" ]]; then
    echo "the selected remote-framework fixture or --console-browser requires an executable Node.js; pass --node-bin if it is not exposed by the current or login-shell PATH" >&2
    exit 2
  fi
fi
if [[ "$requires_node" == true ]]; then
  corepack_bin="$(dirname "$node_bin")/corepack"
  if [[ ! -x "$corepack_bin" ]]; then
    echo "the selected remote-framework fixture or --console-browser requires corepack next to the selected Node.js executable" >&2
    exit 2
  fi
fi
if [[ "$console_browser" == true ]]; then
  if [[ -z "$browser_bin" && "$(uname -s)" == "Darwin" ]]; then
    browser_bin="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
  fi
  if [[ -z "$browser_bin" || ! -x "$browser_bin" ]]; then
    echo "--console-browser requires an executable Chromium/Chrome; pass --browser-bin" >&2
    exit 2
  fi
fi
orphaned_processes="$(pgrep -f "$workspace/target/release/platform-" || true)"
if [[ -n "$orphaned_processes" ]]; then
  echo "repository-local Platform processes are already running (PIDs: ${orphaned_processes//$'\n'/, })" >&2
  echo "stop their owning profile with 'insight stop --path <project>' before starting a fresh journey" >&2
  exit 2
fi

if [[ -z "$project" ]]; then
  # Keep the retained journey path short and easy to inspect on macOS and Linux.
  project="$(mktemp -d "/tmp/insight-productization.XXXXXX")"
fi
project="$(cd "$(dirname "$project")" && pwd)/$(basename "$project")"
if [[ -n "$report_directory" ]]; then
  mkdir -p "$(dirname "$report_directory")"
  mkdir "$report_directory"
  report_directory="$(cd "$report_directory" && pwd)"
fi
if [[ -n "$sandbox_evidence" ]]; then
  sandbox_evidence="$(cd "$(dirname "$sandbox_evidence")" && pwd)/$(basename "$sandbox_evidence")"
  sandbox_environment="$(cd "$(dirname "$sandbox_environment")" && pwd)/$(basename "$sandbox_environment")"
fi
if [[ -n "$aggregate_report" ]]; then
  mkdir -p "$(dirname "$aggregate_report")"
  aggregate_report="$(cd "$(dirname "$aggregate_report")" && pwd)/$(basename "$aggregate_report")"
fi
first_run_marker=""
if [[ -n "$north_star_report" ]]; then
  mkdir -p "$(dirname "$north_star_report")"
  north_star_report="$(cd "$(dirname "$north_star_report")" && pwd)/$(basename "$north_star_report")"
  first_run_marker="${north_star_report}.first-run-marker"
  rm -f "$first_run_marker"
fi

compose_project=""
cleanup() {
  set +e
  if [[ -x "$insight_bin" && -d "$project/.insight" ]]; then
    "$insight_bin" stop --path "$project"
  fi
  if [[ "$keep_dependencies" == false && -f "$project/.insight/project.json" ]]; then
    if compose_project="$(python3 - \
      "$project/.insight/project.json" \
      "$project/.insight/runtime/processes.json" <<'PY'
import json
import pathlib
import re
import sys

identity = json.loads(pathlib.Path(sys.argv[1]).read_text())
processes = pathlib.Path(sys.argv[2])
if processes.is_file():
    project = json.loads(processes.read_text()).get("compose_project", "")
else:
    tenant_id = identity.get("identity", {}).get("tenant_id", "")
    match = re.fullmatch(
        r"ten_([0-9a-f]{8})-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        tenant_id,
    )
    project = f"insight-{match.group(1)}" if match else ""
if not re.fullmatch(r"insight-[0-9a-f]{8}", project):
    raise SystemExit("refusing to clean an unexpected Compose project")
print(project)
PY
)"; then
      INSIGHT_DEV_NATS_CA_PATH="$project/.insight/runtime/tls/ca.pem" \
      INSIGHT_DEV_NATS_SERVER_CERT_PATH="$project/.insight/runtime/tls/nats-server.pem" \
      INSIGHT_DEV_NATS_SERVER_KEY_PATH="$project/.insight/runtime/tls/nats-server-key.pem" \
        docker compose \
        --project-name "$compose_project" \
        --file "$workspace/deploy/dev/compose.yaml" \
        down
    fi
  fi
  echo "fresh project retained at $project"
}
trap cleanup EXIT

cd "$workspace"
if [[ -z "$release_candidate" ]]; then
  cargo build --locked --release -p insight-cli --bin insight
else
  release_candidate="$(cd "$release_candidate" && pwd)"
fi
if [[ "$features" == "all" || (",$features," == *",remote-capability,"* && ",$features," == *",sandbox,"*) ]]; then
  PATH="$(dirname "$node_bin"):$PATH" "$corepack_bin" pnpm \
    --dir "$workspace/examples/productization/langgraph-reference" install --frozen-lockfile
  PATH="$(dirname "$node_bin"):$PATH" "$corepack_bin" pnpm \
    --dir "$workspace/examples/productization/langgraph-reference" run check
  PATH="$(dirname "$node_bin"):$PATH" "$corepack_bin" pnpm \
    --dir "$workspace/examples/productization/langgraph-reference" test
fi
if [[ "$console_browser" == true ]]; then
  if [[ -z "$release_candidate" ]]; then
    PATH="$(dirname "$node_bin"):$PATH" "$corepack_bin" pnpm --dir "$workspace/web/console" install --frozen-lockfile
    PATH="$(dirname "$node_bin"):$PATH" "$corepack_bin" pnpm --dir "$workspace/web/console" run build
  fi
fi
if [[ ! -x "$insight_bin" ]]; then
  echo "insight binary is not executable: $insight_bin" >&2
  exit 2
fi

"$insight_bin" doctor --json
"$insight_bin" init --path "$project" --name "productization-${profile_label%%+*}"
dev_arguments=(--path "$project")
if [[ -n "$release_candidate" ]]; then
  candidate_identity="$(python3 - \
    "$release_candidate/release-bundle.json" "$insight_bin" <<'PY'
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

bundle_path, insight = sys.argv[1:]
bundle = json.loads(Path(bundle_path).read_bytes())
version = bundle.get("version")
revision = bundle.get("git_commit")
if not isinstance(version, str) or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
    raise SystemExit("candidate ReleaseBundle version is invalid")
if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
    raise SystemExit("candidate ReleaseBundle revision is invalid")
reported = json.loads(subprocess.check_output([insight, "version", "--json"]))
expected_target = "x86_64-unknown-linux-gnu"
if reported.get("version") != version or reported.get("git_commit") != revision or reported.get("target") != expected_target:
    raise SystemExit("candidate CLI identity differs from its signed ReleaseBundle")
print(version, revision)
PY
  )"
  read -r candidate_version candidate_revision <<< "$candidate_identity"
  source_revision="$(git rev-parse HEAD)"
  if [[ "$candidate_revision" != "$source_revision" ]]; then
    echo "candidate ReleaseBundle revision differs from the checked-out qualification source" >&2
    exit 2
  fi
  release_cache="$project/.insight/cache/releases/$candidate_version"
  mkdir -p "$release_cache"
  cp "$release_candidate/release-bundle.json" "$release_cache/release-bundle.json"
  cp "$release_candidate/release-bundle.signature.json" "$release_cache/release-bundle.signature.json"
  dev_arguments+=(--offline)
else
  dev_arguments+=(--from-source)
fi
if [[ -n "$features" ]]; then
  dev_arguments+=(--features "$features")
fi
"$insight_bin" dev "${dev_arguments[@]}"
"$insight_bin" status --path "$project"
# A feature-rich profile can spend longer than the deliberately short local token TTL
# compiling and starting every role. Rotate only after the runtime is ready so
# the public journey receives a fresh credential. Never print the bearer token
# into CI logs; the CLI persists it with the existing private-file permissions.
"$insight_bin" token --path "$project" >/dev/null

source_revision="$(git rev-parse HEAD)"
runtime_identity="$(python3 - \
  "$project/.insight/project.json" "$project/.insight/runtime/profile.json" \
  "$source_revision" "$profile_label" "$sandbox_evidence" <<'PY'
import hashlib
import json
from pathlib import Path
import re
import sys

project_path, profile_path, revision, expected_label, sandbox_evidence_path = sys.argv[1:]
project = json.loads(Path(project_path).read_bytes())
profile = json.loads(Path(profile_path).read_bytes())
identity = project.get("identity")
profile_digest = profile.get("profile_digest")
features = profile.get("features")
if not isinstance(identity, dict) or not isinstance(profile_digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", profile_digest):
    raise SystemExit("fresh project identity or runtime profile digest is invalid")
if not isinstance(features, list) or any(not isinstance(value, str) for value in features):
    raise SystemExit("runtime profile feature closure is invalid")
actual_label = "starter" if not features else ("all" if set(features) == {"context", "mcp", "model", "remote-capability", "sandbox"} else "starter+" + ",".join(features))
if actual_label != expected_label:
    raise SystemExit("runtime profile differs from the requested closed profile")
closure = {
    "schema_version": 1,
    "project_identity": identity,
    "source_revision": revision,
    "runtime_profile_digest": profile_digest,
}
encoded = json.dumps(closure, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
qualification_run_id = "sha256:" + hashlib.sha256(encoded).hexdigest()
if sandbox_evidence_path:
    sandbox_evidence = json.loads(Path(sandbox_evidence_path).read_bytes())
    qualification_run_id = sandbox_evidence.get("qualification_run_id")
    if (
        sandbox_evidence.get("schema_version") != 1
        or sandbox_evidence.get("report_kind")
        != "insight.productization.opensandbox-qualification/v1"
        or sandbox_evidence.get("source_revision") != revision
        or sandbox_evidence.get("status") != "passed"
        or not isinstance(qualification_run_id, str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", qualification_run_id)
    ):
        raise SystemExit("raw OpenSandbox evidence does not identify a passed same-revision qualification run")
print(profile_digest, qualification_run_id)
PY
)"
read -r runtime_profile_digest qualification_run_id <<< "$runtime_identity"

test_environment=(
  "PLATFORM_INSIGHT_BIN=$insight_bin"
  "PLATFORM_PRODUCTIZATION_PROJECT=$project"
  "PLATFORM_PRODUCTIZATION_FEATURES=$features"
  "PLATFORM_PRODUCTIZATION_ACTUAL_PROFILE=$profile_label"
  "PLATFORM_PRODUCTIZATION_PROFILE_DIGEST=$runtime_profile_digest"
  "PLATFORM_PRODUCTIZATION_QUALIFICATION_RUN_ID=$qualification_run_id"
)
if [[ -n "$node_bin" ]]; then
  test_environment+=("PLATFORM_PRODUCTIZATION_NODE_BIN=$node_bin")
fi
if [[ "$console_browser" == true ]]; then
  test_environment+=(
    "PLATFORM_PRODUCTIZATION_CONSOLE_BROWSER=true"
    "INSIGHT_CONSOLE_BROWSER_BIN=$browser_bin"
  )
  if [[ -n "$console_bundle" ]]; then
    test_environment+=("INSIGHT_CONSOLE_BUNDLE_ROOT=$console_bundle")
  fi
fi
if [[ -n "$report_directory" ]]; then
  test_environment+=(
    "PLATFORM_PRODUCTIZATION_REPORT_DIRECTORY=$report_directory"
    "PLATFORM_PRODUCTIZATION_FRESH_PROFILE=true"
  )
fi
if [[ -n "$sandbox_evidence" ]]; then
  test_environment+=(
    "PLATFORM_PRODUCTIZATION_SANDBOX_EVIDENCE=$sandbox_evidence"
    "PLATFORM_PRODUCTIZATION_SANDBOX_ENVIRONMENT=$sandbox_environment"
  )
fi
if [[ -n "$first_run_marker" ]]; then
  python3 scripts/qualify-productization-first-run.py \
    --insight-bin "$insight_bin" \
    --project "$project" \
    --marker "$first_run_marker"

  source_revision="$(git rev-parse HEAD)"
  python3 scripts/write-productization-north-star-report.py \
    --marker "$first_run_marker" \
    --output "$north_star_report" \
    --source-revision "$source_revision" \
    --started-epoch "$journey_started_epoch" \
    --fresh-checkout
  python3 scripts/check-productization-north-star-report.py \
    "$north_star_report" \
    --source-revision "$source_revision"
  rm -f "$first_run_marker"
fi

env "${test_environment[@]}" \
  cargo test --locked -p insight-platform-qualification-tests --test productization public_cli_deterministic_first_run -- --nocapture

if [[ -n "$report_directory" ]]; then
  if [[ "$features" == "all" ]]; then
    source_revision="$(git rev-parse HEAD)"
    python3 scripts/check-productization-scenario-reports.py \
      "$report_directory" \
      --source-revision "$source_revision" \
      --sandbox-evidence "$sandbox_evidence" \
      --sandbox-environment "$sandbox_environment" \
      --aggregate-output "$aggregate_report"
  else
    python3 scripts/check-productization-scenario-reports.py \
      "$report_directory" \
      --allow-incomplete
  fi
fi
