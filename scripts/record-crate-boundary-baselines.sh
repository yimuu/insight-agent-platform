#!/usr/bin/env bash
set -euo pipefail

workspace_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
baseline_dir="$workspace_root/scripts/baselines"
temporary_dir=$(mktemp -d "${TMPDIR:-/tmp}/insight-crate-boundary-baselines.XXXXXX")
trap 'rm -rf "$temporary_dir"' EXIT

cd "$workspace_root"
mkdir -p "$baseline_dir"

cargo metadata --locked --all-features --format-version 1 >"$temporary_dir/metadata.json"
python3 scripts/check-crate-boundaries.py snapshot "$temporary_dir/metadata.json" \
  >"$temporary_dir/crate-boundary-third-party-features.tsv"

{
  printf '# cargo-tree-workspace-all-features-v1\n'
  printf '# command: cargo tree --locked --workspace --all-features -e features\n'
  printf '# toolchain: %s; %s\n' "$(cargo --version)" "$(rustc --version)"
  cargo tree --locked --workspace --all-features -e features \
    | python3 -c 'import sys; root = sys.argv[1]; sys.stdout.write(sys.stdin.read().replace(root, "<workspace>"))' "$workspace_root"
} >"$temporary_dir/cargo-tree-workspace-all-features.txt"

mv "$temporary_dir/crate-boundary-third-party-features.tsv" \
  "$baseline_dir/crate-boundary-third-party-features.tsv"
mv "$temporary_dir/cargo-tree-workspace-all-features.txt" \
  "$baseline_dir/cargo-tree-workspace-all-features.txt"

printf 'Recorded crate boundary dependency baselines in %s.\n' "$baseline_dir"
