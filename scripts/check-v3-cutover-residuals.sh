#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

failed=0

fail() {
  printf 'v3 cutover residual: %s\n' "$1" >&2
  failed=1
}

report_matches() {
  local label=$1
  shift
  local matches
  matches=$(grep "$@" 2>/dev/null || true)
  if [[ -n "$matches" ]]; then
    fail "$label"
    printf '%s\n' "$matches" >&2
  fi
}

# Deleted implementation boundaries must not be recreated under another CI
# path. Historical specifications and explicitly negative fixtures are not in
# this list: they are allowed to describe inputs that v3 rejects.
for path in \
  src/catalog.rs \
  src/dsl/vnext \
  src/runtime/coordinator.rs \
  src/runtime/run_state.rs \
  src/runtime/scope_scheduler.rs \
  src/runtime/service.rs \
  src/engine/repository/legacy_test \
  migrations/formal_v2
do
  if [[ -e "$path" ]]; then
    fail "deleted implementation path exists: $path"
  fi
done

# Production code may contain a small parser guard that rejects the literal
# old control keyword. It must not contain an executable old node, scheduler,
# local value store, compatibility flag, or old internal control instruction.
report_matches \
  "production source contains a deleted execution symbol" \
  -RInE --include='*.rs' \
  '(scope_scheduler|mark_incomplete_interrupted|legacy_scheduler|LegacyScheduler|OldScheduler|runtime_local_(only_)?value_store|RegionYield|BranchPhi|ExecutableRegion|NodeKind::Switch|RawSwitch|SwitchDescriptor|SchedulerAction::Switch|core\.branch_end|USE_(OLD|V3)_SCHEDULER|ENABLE_(OLD|V3)_SCHEDULER)' \
  src

report_matches \
  "Cargo feature reintroduces an old/new scheduler split" \
  -nE '(old|legacy|v3)[_-]?scheduler|scheduler[_-]?(old|legacy|v3)' \
  Cargo.toml

# The durable-v3 binary must not accept no-op safety settings retained from
# the deleted in-process runtime. Negative parser tests may name these fields,
# so this gate is intentionally limited to production and active surfaces.
report_matches \
  "active configuration contains a deleted runtime setting" \
  -nEH \
  '(operation_cancel_grace_period|max_template_output_bytes|journal_capacity|journal_batch_size|journal_operation_timeout)' \
  src config agents README.md docs/superpowers/README.md

# Author-controlled positive surfaces must be v3-only. Negative fixtures are
# selected by name and intentionally excluded from this scan.
positive_files=()
while IFS= read -r -d '' file; do
  positive_files+=("$file")
done < <(find agents -type f \( -name '*.yaml' -o -name '*.yml' \) -print0)
while IFS= read -r -d '' file; do
  positive_files+=("$file")
done < <(find tests/fixtures/v3 -type f \( -name '*.yaml' -o -name '*.yml' \) ! -name 'negative-*' -print0)

if ((${#positive_files[@]} > 0)); then
  report_matches \
    "checked-in Agent or positive fixture contains deleted author syntax" \
    -nEH \
    '(api_version:[[:space:]]*insight\.agent/v2|type:[[:space:]]*switch([[:space:]]|$)|type:[[:space:]]*core\.|core\.branch_end|scope_scheduler|legacy_scheduler)' \
    "${positive_files[@]}"
fi

# These are the two active entry documents. Dated specs/plans remain historical
# records and may name removed concepts when explaining why they were removed.
report_matches \
  "active documentation describes a deleted production contract" \
  -nEH \
  '(Region/SSA|RegionYield|Branch/Phi|runtime-local-only|scope_scheduler|mark_incomplete_interrupted|api_version:[[:space:]]*insight\.agent/v2|type:[[:space:]]*switch([[:space:]]|$)|core\.branch_end|formal_v2|legacy scheduler)' \
  README.md docs/superpowers/README.md

if ((failed != 0)); then
  exit 1
fi

printf 'DSL v3 cutover residual scan passed.\n'
