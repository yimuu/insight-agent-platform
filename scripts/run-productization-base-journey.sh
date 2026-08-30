#!/usr/bin/env bash
set -euo pipefail

workspace="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
project=""
report_directory=""
profile="base"
insight_bin="${PLATFORM_INSIGHT_BIN:-$workspace/target/release/insight}"
keep_dependencies=false
console_browser=false
node_bin="${PLATFORM_PRODUCTIZATION_NODE_BIN:-}"
browser_bin="${INSIGHT_CONSOLE_BROWSER_BIN:-}"
corepack_bin=""

usage() {
  cat <<'EOF'
Usage: scripts/run-productization-base-journey.sh [options]

Runs a fresh selected-profile productization journey against real local Platform roles.

Options:
  --project <new-path>          Use this not-yet-existing project path instead of mktemp.
  --report-directory <path>    Write and validate exact-revision scenario evidence.
  --profile <base|full>        Start the selected closed local profile (default: base).
  --insight-bin <path>         Use an existing insight binary (default: target/release/insight).
  --keep-dependencies          Leave the exact Docker Compose dependencies running.
  --console-browser           Run the static Console against the fresh real Gateway in headless Chromium.
  --node-bin <path>           Node.js executable for full remote fixtures and --console-browser (default: current or login-shell PATH).
  --browser-bin <path>        Chromium/Chrome executable for --console-browser.
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
    --profile)
      (($# >= 2)) || { echo "--profile requires a value" >&2; exit 2; }
      profile=$2
      shift 2
      ;;
    --insight-bin)
      (($# >= 2)) || { echo "--insight-bin requires a value" >&2; exit 2; }
      insight_bin=$2
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

if [[ "$profile" != "base" && "$profile" != "full" ]]; then
  echo "--profile must be base or full" >&2
  exit 2
fi
if [[ -n "$project" && -e "$project" ]]; then
  echo "--project must name a path that does not already exist: $project" >&2
  exit 2
fi
if [[ -n "$report_directory" && -n "$(git -C "$workspace" status --porcelain)" ]]; then
  echo "exact-revision reports require a clean Git working tree" >&2
  exit 2
fi
if ! command -v pgrep >/dev/null 2>&1; then
  echo "pgrep is required to prove that no repository-local Platform process is already running" >&2
  exit 2
fi
if [[ "$profile" == "full" || "$console_browser" == true ]]; then
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
    echo "the full profile and --console-browser require an executable Node.js; pass --node-bin if it is not exposed by the current or login-shell PATH" >&2
    exit 2
  fi
fi
if [[ "$profile" == "full" || "$console_browser" == true ]]; then
  corepack_bin="$(dirname "$node_bin")/corepack"
  if [[ ! -x "$corepack_bin" ]]; then
    echo "the full profile and --console-browser require corepack next to the selected Node.js executable" >&2
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
  # Keep the default project path short enough for the Sandbox attestor's Unix
  # registration socket. macOS TMPDIR paths commonly exceed the 100-byte
  # closed socket-path limit once `.insight/runtime/registration.sock` is added.
  project="$(mktemp -d "/tmp/insight-productization.XXXXXX")"
fi
project="$(cd "$(dirname "$project")" && pwd)/$(basename "$project")"
if [[ -n "$report_directory" ]]; then
  mkdir -p "$report_directory"
  report_directory="$(cd "$report_directory" && pwd)"
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
cargo build --locked --release -p insight-cli --bin insight
if [[ "$profile" == "full" ]]; then
  PATH="$(dirname "$node_bin"):$PATH" "$corepack_bin" pnpm \
    --dir "$workspace/examples/productization/langgraph-reference" install --frozen-lockfile
  PATH="$(dirname "$node_bin"):$PATH" "$corepack_bin" pnpm \
    --dir "$workspace/examples/productization/langgraph-reference" run check
  PATH="$(dirname "$node_bin"):$PATH" "$corepack_bin" pnpm \
    --dir "$workspace/examples/productization/langgraph-reference" test
fi
if [[ "$console_browser" == true ]]; then
  PATH="$(dirname "$node_bin"):$PATH" "$corepack_bin" pnpm --dir "$workspace/web/console" install --frozen-lockfile
  PATH="$(dirname "$node_bin"):$PATH" "$corepack_bin" pnpm --dir "$workspace/web/console" run build
fi
if [[ ! -x "$insight_bin" ]]; then
  echo "insight binary is not executable: $insight_bin" >&2
  exit 2
fi

"$insight_bin" doctor --json
"$insight_bin" init --path "$project" --name "productization-$profile"
"$insight_bin" dev --path "$project" --profile "$profile"
"$insight_bin" status --path "$project"
# The full profile can spend longer than the deliberately short local token TTL
# compiling and starting every role. Rotate only after the runtime is ready so
# the public journey receives a fresh credential. Never print the bearer token
# into CI logs; the CLI persists it with the existing private-file permissions.
"$insight_bin" token --path "$project" >/dev/null

test_environment=(
  "PLATFORM_INSIGHT_BIN=$insight_bin"
  "PLATFORM_PRODUCTIZATION_PROJECT=$project"
  "PLATFORM_PRODUCTIZATION_PROFILE=$profile"
)
if [[ -n "$node_bin" ]]; then
  test_environment+=("PLATFORM_PRODUCTIZATION_NODE_BIN=$node_bin")
fi
if [[ "$console_browser" == true ]]; then
  test_environment+=(
    "PLATFORM_PRODUCTIZATION_CONSOLE_BROWSER=true"
    "INSIGHT_CONSOLE_BROWSER_BIN=$browser_bin"
  )
fi
if [[ -n "$report_directory" ]]; then
  test_environment+=(
    "PLATFORM_PRODUCTIZATION_REPORT_DIRECTORY=$report_directory"
    "PLATFORM_PRODUCTIZATION_FRESH_PROFILE=true"
  )
fi

env "${test_environment[@]}" \
  cargo test --locked --test productization public_cli_deterministic_first_run -- --nocapture

if [[ -n "$report_directory" ]]; then
  python3 scripts/check-productization-scenario-reports.py \
    "$report_directory" \
    --allow-incomplete
fi
