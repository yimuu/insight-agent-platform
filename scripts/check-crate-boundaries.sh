#!/usr/bin/env bash
set -euo pipefail

workspace_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
metadata_file=$(mktemp "${TMPDIR:-/tmp}/insight-crate-boundaries.XXXXXX")
trap 'rm -f "$metadata_file"' EXIT

cd "$workspace_root"

# --all-features and the complete resolve graph are intentional.  Manifest
# text alone cannot detect renamed dependencies, dev/build edges, transitive
# reachability, or the final feature unification chosen by Cargo.
cargo metadata --locked --all-features --format-version 1 >"$metadata_file"

python3 scripts/check-crate-boundaries.py \
  check \
  "$metadata_file" \
  scripts/baselines/crate-boundary-third-party-features.tsv \
  "$workspace_root"
