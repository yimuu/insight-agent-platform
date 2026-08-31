#!/usr/bin/env bash
set -euo pipefail

workspace="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
insight_bin=""
output=""
release_root=""
network_mbps="100"
stabilization_seconds="300"

usage() {
  cat <<'EOF'
Usage: scripts/qualify-development-profile.sh --insight-bin <official-binary> --release-root <https-url> --output <report.json>

Runs the prebuilt starter closure on an otherwise disposable Linux Docker runner. The runner must
provide at least 4 CPUs, 8 GiB memory and 100 Mbps equivalent network capacity. This script removes
only the exact signed release/dependency image cache and project-scoped volumes that it creates.
EOF
}

while (($# > 0)); do
  case "$1" in
    --insight-bin) insight_bin=${2:-}; shift 2 ;;
    --release-root) release_root=${2:-}; shift 2 ;;
    --output) output=${2:-}; shift 2 ;;
    --network-mbps) network_mbps=${2:-}; shift 2 ;;
    --stabilization-seconds) stabilization_seconds=${2:-}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unsupported option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ "$(uname -s)" != "Linux" || -z "$insight_bin" || ! -x "$insight_bin" || -z "$output" ]]; then
  echo "qualification requires Linux, an executable official CLI, and --output" >&2
  exit 2
fi
if [[ ! "$release_root" =~ ^https://[^[:space:]]+$ ]]; then
  echo "--release-root must be an absolute HTTPS release root" >&2
  exit 2
fi
if [[ ! "$network_mbps" =~ ^[0-9]+$ || ! "$stabilization_seconds" =~ ^[0-9]+$ ]]; then
  echo "network and stabilization values must be positive integers" >&2
  exit 2
fi

project="$(mktemp -d /tmp/insight-dev-qualification.XXXXXX)"
project_name="performance"
cleanup() {
  set +e
  if [[ -d "$project/.insight" ]]; then
    "$insight_bin" stop --path "$project" >/dev/null 2>&1
    "$insight_bin" reset --path "$project" --confirm "$project_name" >/dev/null 2>&1
  fi
  rmdir "$project" >/dev/null 2>&1
}
trap cleanup EXIT

version_json="$($insight_bin version --json)"
read -r version revision target < <(python3 - "$version_json" <<'PY'
import json, sys
value = json.loads(sys.argv[1])
if value.get("target") != "x86_64-unknown-linux-gnu":
    raise SystemExit("qualification runner requires the official Linux x86_64 CLI")
if value.get("git_commit") == "development":
    raise SystemExit("qualification refuses an unversioned development CLI")
print(value["version"], value["git_commit"], value["target"])
PY
)

bundle_url="${release_root%/}/download/v${version}/release-bundle.json"
bundle="$(mktemp /tmp/insight-release-bundle.XXXXXX)"
trap 'rm -f "$bundle"; cleanup' EXIT
curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 "$bundle_url" --output "$bundle"
mapfile -t images < <(python3 - "$bundle" "$workspace/release/development-profile-v1.json" <<'PY'
import json, pathlib, sys
bundle = json.loads(pathlib.Path(sys.argv[1]).read_bytes())
registry = json.loads(pathlib.Path(sys.argv[2]).read_bytes())
runtime = next(image for image in bundle["images"] if image["name"] == "runtime")
print(f'{runtime["subject"]}@{runtime["index_digest"]}')
for dependency in registry["dependencies"]:
    print(dependency["image"])
PY
)
if [[ "${#images[@]}" -ne 4 ]]; then
  echo "release/profile image closure is not exact" >&2
  exit 1
fi

for image in "${images[@]}"; do
  docker image rm "$image" >/dev/null 2>&1 || true
done
"$insight_bin" init --path "$project" --name "$project_name"
cold_started="$(date +%s%N)"
download_started="$cold_started"
for image in "${images[@]}"; do
  docker pull "$image"
done
download_finished="$(date +%s%N)"
INSIGHT_UPDATE_BASE_URL="${release_root%/}" "$insight_bin" dev --path "$project"
cold_finished="$(date +%s%N)"
"$insight_bin" status --path "$project"

profile_json="$project/.insight/runtime/profile.json"
profile_digest="$(python3 - "$profile_json" <<'PY'
import json, pathlib, sys
value = json.loads(pathlib.Path(sys.argv[1]).read_bytes())
if value.get("features") != [] or not str(value.get("release_identity", "")).startswith("release:"):
    raise SystemExit("qualification did not start the prebuilt starter closure")
print(value["profile_digest"])
PY
)"
source_compilations=0
if [[ -e "$project/.insight/runtime/build.json" ]]; then
  source_compilations=1
fi

"$insight_bin" stop --path "$project"
warm_started="$(date +%s%N)"
INSIGHT_UPDATE_BASE_URL="${release_root%/}" "$insight_bin" start --path "$project"
warm_finished="$(date +%s%N)"
sleep "$stabilization_seconds"

read -r idle_rss_bytes idle_cpu_percent < <(python3 - "$project/.insight/runtime/processes.json" <<'PY'
import json, pathlib, re, subprocess, sys

state = json.loads(pathlib.Path(sys.argv[1]).read_bytes())
pids = [str(process["pid"]) for process in state["processes"].values()]
compose_project = state["compose_project"]
rss_kib = 0
cpu = 0.0
if pids:
    output = subprocess.check_output(["ps", "-o", "rss=", "-o", "%cpu=", "-p", ",".join(pids)], text=True)
    for line in output.splitlines():
        rss, percent = line.split()
        rss_kib += int(rss)
        cpu += float(percent)

units = {"B": 1, "kB": 1000, "KiB": 1024, "MB": 1000**2, "MiB": 1024**2, "GB": 1000**3, "GiB": 1024**3}
container_ids = subprocess.check_output(
    ["docker", "ps", "--quiet", "--filter", f"label=com.docker.compose.project={compose_project}"],
    text=True,
).split()
if container_ids:
    containers = subprocess.check_output(
        ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}|{{.CPUPerc}}", *container_ids],
        text=True,
    )
    for line in containers.splitlines():
        memory, percent = line.split("|", 1)
        current = memory.split("/", 1)[0].strip()
        match = re.fullmatch(r"([0-9.]+)([A-Za-z]+)", current)
        if not match or match.group(2) not in units:
            raise SystemExit(f"cannot parse Docker memory measurement {current!r}")
        rss_kib += int(float(match.group(1)) * units[match.group(2)] / 1024)
        cpu += float(percent.rstrip("%"))
print(rss_kib * 1024, cpu)
PY
)

project_disk_kib="$(du -sk "$project/.insight" | awk '{print $1}')"
compose_project="$(python3 - "$project/.insight/runtime/processes.json" <<'PY'
import json, pathlib, sys
print(json.loads(pathlib.Path(sys.argv[1]).read_bytes())["compose_project"])
PY
)"
while IFS= read -r volume; do
  [[ -n "$volume" ]] || continue
  mountpoint="$(docker volume inspect --format '{{.Mountpoint}}' "$volume")"
  volume_kib="$(sudo du -sk "$mountpoint" | awk '{print $1}')"
  project_disk_kib=$((project_disk_kib + volume_kib))
done < <(docker volume ls --quiet --filter "label=com.docker.compose.project=$compose_project")

download_bytes=0
for image in "${images[@]}"; do
  bytes="$(docker image inspect --format '{{.Size}}' "$image")"
  download_bytes=$((download_bytes + bytes))
done
cpu_count="$(nproc)"
memory_bytes="$(awk '/MemTotal:/ {print $2 * 1024}' /proc/meminfo | cut -d. -f1)"
seconds_between() { python3 - "$1" "$2" <<'PY'
import sys
print((int(sys.argv[2]) - int(sys.argv[1])) / 1_000_000_000)
PY
}

python3 "$workspace/scripts/build-development-profile-performance.py" \
  --version "$version" --git-commit "$revision" --profile-digest "$profile_digest" \
  --cpu-count "$cpu_count" --memory-bytes "$memory_bytes" --network-mbps "$network_mbps" \
  --cold-ready-seconds "$(seconds_between "$cold_started" "$cold_finished")" \
  --warm-ready-seconds "$(seconds_between "$warm_started" "$warm_finished")" \
  --download-seconds "$(seconds_between "$download_started" "$download_finished")" \
  --download-bytes "$download_bytes" --idle-rss-bytes "$idle_rss_bytes" \
  --idle-cpu-percent "$idle_cpu_percent" --idle-stabilization-seconds "$stabilization_seconds" \
  --project-disk-bytes "$((project_disk_kib * 1024))" --source-compilations "$source_compilations" \
  --output "$output"
