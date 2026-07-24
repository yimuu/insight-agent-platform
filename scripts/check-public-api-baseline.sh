#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
baseline="$repo_root/tests/baselines/root-public-api.txt"
target_dir="${CARGO_TARGET_DIR:-$repo_root/target/public-api-baseline}"
if [[ "$target_dir" != /* ]]; then
  target_dir="$repo_root/$target_dir"
fi
rustdoc_dir="$target_dir/doc"
root_rustdoc_json="$rustdoc_dir/insight_agent_platform.json"
actual="$(mktemp "${TMPDIR:-/tmp}/insight-public-api.XXXXXX")"
trap 'rm -f "$actual"' EXIT

expected_rustc="rustc 1.94.1 (e408947bf 2026-03-25)"
actual_rustc="$(rustc --version)"
if [[ "$actual_rustc" != "$expected_rustc" ]]; then
  echo "public API baseline requires $expected_rustc; found $actual_rustc" >&2
  exit 1
fi

(
  cd "$repo_root"
  # `cargo rustdoc` has no `--workspace` flag. Resolve the explicit workspace
  # lib packages from metadata, then document each package with identical
  # flags. Keep the root first so a failure cannot leave a fresh member JSON
  # beside a stale facade document.
  while IFS= read -r package_name; do
    CARGO_TARGET_DIR="$target_dir" RUSTC_BOOTSTRAP=1 cargo rustdoc \
      --locked --all-features --package "$package_name" --lib -- \
      -Z unstable-options --output-format json --document-hidden-items
  done < <(
    cargo metadata --locked --format-version 1 --no-deps |
      python3 -c '
import json
import sys

metadata = json.load(sys.stdin)
workspace = set(metadata["workspace_members"])
packages = []
for package in metadata["packages"]:
    if package["id"] not in workspace:
        continue
    if any("lib" in target["kind"] for target in package["targets"]):
        packages.append(package["name"])
packages.sort(key=lambda name: (name != "insight-agent-platform", name))
for name in packages:
    print(name)
'
  )
)

# Keep the facade first.  The remaining names are the fixed crate-boundary
# design workspace; supplying their documents lets the normalizer expand an
# external module/glob reexport back into root-facade paths.  During Phase 0
# only the root document exists.
rustdoc_jsons=("$root_rustdoc_json")
workspace_crates=(
  insight_engine
  insight_dsl
  insight_durable
  insight_resources
  insight_storage
  insight_runtime
  insight_api
)
for crate_name in "${workspace_crates[@]}"; do
  member_json="$rustdoc_dir/$crate_name.json"
  if [[ -f "$member_json" ]]; then
    rustdoc_jsons+=("$member_json")
  fi
done

if [[ ! -f "$root_rustdoc_json" ]]; then
  echo "missing root rustdoc JSON: $root_rustdoc_json" >&2
  exit 1
fi

python3 "$repo_root/scripts/normalize-public-api-rustdoc.py" \
  "${rustdoc_jsons[@]}" \
  --rustc-version "$actual_rustc" \
  --workspace-root "$repo_root" >"$actual"

if [[ "${UPDATE_PUBLIC_API_BASELINE:-0}" == "1" ]]; then
  python3 "$repo_root/scripts/check-public-api-compat-bridges.py" \
    --freeze-bridges "$actual" "$baseline"
  echo "updated $baseline"
  exit 0
fi

if [[ ! -f "$baseline" ]]; then
  echo "missing public API baseline: $baseline" >&2
  echo "run UPDATE_PUBLIC_API_BASELINE=1 bash scripts/check-public-api-baseline.sh" >&2
  exit 1
fi

python3 "$repo_root/scripts/check-public-api-compat-bridges.py" \
  "$baseline" "$actual"
echo "root public API baseline matches with audited source-compatible bridges"
