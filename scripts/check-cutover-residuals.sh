#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

failed=0

# Keep scans aligned with both the Phase 0 single-package baseline and the
# target workspace layout.  Do not derive these from the current workspace
# members alone: a newly added crates/* source tree or manifest must be
# scanned even before it is wired into Cargo's member list.
production_source_roots=(src)
member_manifests=(Cargo.toml)
if [[ -d crates ]]; then
  while IFS= read -r -d '' source_root; do
    production_source_roots+=("$source_root")
  done < <(find crates -type d -name src -print0)
  while IFS= read -r -d '' manifest; do
    member_manifests+=("$manifest")
  done < <(find crates -type f -name Cargo.toml -print0)
fi

fail() {
  printf 'cutover residual: %s\n' "$1" >&2
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
# path. Match owner-independently: listing only today's expected owner would
# let the same implementation return under (for example) crates/api/src.
# Historical specifications and explicitly negative fixtures are outside the
# production source roots and remain free to describe rejected v2 inputs.
for source_root in "${production_source_roots[@]}"; do
  while IFS= read -r -d '' path; do
    fail "deleted implementation path exists: $path"
  done < <(
    find "$source_root" \
      \( -type f \( \
        -name coordinator.rs -o \
        -name run_state.rs -o \
        -name scope_scheduler.rs -o \
        -name service.rs \
      \) -o -type d \( -name vnext -o -name legacy_test \) \) \
      -print0
  )
done

migration_roots=()
[[ -d migrations ]] && migration_roots+=(migrations)
[[ -d crates ]] && migration_roots+=(crates)
if ((${#migration_roots[@]} > 0)); then
  while IFS= read -r -d '' path; do
    fail "deleted implementation path exists: $path"
  done < <(
    find "${migration_roots[@]}" -type d -path '*/migrations/formal_v2' -print0
  )
fi

if [[ -d migrations/durable ]]; then
  fail "pre-1.0 durable migration history still exists: migrations/durable"
fi

report_matches \
  "production source contains deleted runtime Schema migration authority" \
  -RInE --include='*.rs' \
  '(DURABLE_MIGRATIONS|DurableMigration|SqliteMigrationGuard|apply_migrations|migrate_schema|initialize_schema|migration_manifest)' \
  "${production_source_roots[@]}"

report_matches \
  "production source embeds the provisioning-only durable Schema" \
  -RInE --include='*.rs' \
  '(include_(str|bytes)!\([^)]*database/durable|database/durable/(postgres|sqlite)/schema\.sql)' \
  "${production_source_roots[@]}"

# Production code may contain a small parser guard that rejects the literal
# old control keyword. It must not contain an executable old node, scheduler,
# local value store, compatibility flag, or old internal control instruction.
report_matches \
  "production source contains a deleted execution symbol" \
  -RInE --include='*.rs' \
  '(scope_scheduler|mark_incomplete_interrupted|legacy_scheduler|LegacyScheduler|OldScheduler|runtime_local_(only_)?value_store|RegionYield|BranchPhi|ExecutableRegion|NodeKind::Switch|RawSwitch|SwitchDescriptor|SchedulerAction::Switch|core\.branch_end|USE_(OLD|V[0-9]+)_SCHEDULER|ENABLE_(OLD|V[0-9]+)_SCHEDULER)' \
  "${production_source_roots[@]}"

report_matches \
  "Cargo feature reintroduces an old/new scheduler split" \
  -nE '(old|legacy|v[0-9]+)[_-]?scheduler|scheduler[_-]?(old|legacy|v[0-9]+)' \
  "${member_manifests[@]}"

# The durable binary must not accept no-op safety settings retained from
# the deleted in-process runtime. Negative parser tests may name these fields,
# so this gate is intentionally limited to production and active surfaces.
report_matches \
  "active configuration contains a deleted runtime setting" \
  -nEH \
  '(operation_cancel_grace_period|max_template_output_bytes|journal_capacity|journal_batch_size|journal_operation_timeout)' \
  "${production_source_roots[@]}" config agents README.md docs/README.md docs/current/*.md \
  docs/qualifications/*.md

# Author-controlled positive surfaces must use the current DSL. Negative fixtures are
# selected by name and intentionally excluded from this scan.
positive_files=()
while IFS= read -r -d '' file; do
  positive_files+=("$file")
done < <(find agents -type f \( -name '*.yaml' -o -name '*.yml' \) -print0)
while IFS= read -r -d '' file; do
  positive_files+=("$file")
done < <(find tests/fixtures/dsl -type f \( -name '*.yaml' -o -name '*.yml' \) ! -name 'negative-*' -print0)

if ((${#positive_files[@]} > 0)); then
  report_matches \
    "checked-in Agent or positive fixture contains deleted author syntax" \
    -nEH \
    '(api_version:[[:space:]]*insight\.agent/v2|type:[[:space:]]*switch([[:space:]]|$)|type:[[:space:]]*core\.|core\.branch_end|scope_scheduler|legacy_scheduler)' \
    "${positive_files[@]}"
fi

# These are the active user-facing documents. Current normative specifications
# and archived records may name removed concepts when explaining cutover history.
report_matches \
  "active documentation describes a deleted production contract" \
  -nEH \
  '(Region/SSA|RegionYield|Branch/Phi|runtime-local-only|scope_scheduler|mark_incomplete_interrupted|api_version:[[:space:]]*insight\.agent/v2|type:[[:space:]]*switch([[:space:]]|$)|core\.branch_end|formal_v2|legacy scheduler|migrations/durable|schema_migrations|migration manifest|自动(执行|运行).*(migration|迁移))' \
  README.md docs/README.md docs/current/*.md docs/qualifications/*.md

# M5 product-surface closure. Historical source and deployment material may remain only under
# explicit archive paths; the default Cargo build, candidate image and current contract must be
# the clean-cut Platform `/v1` product.
if ! grep -Fq 'default-members = ["crates/insight-cli"]' Cargo.toml; then
  fail "Cargo default-members does not select the insight CLI"
fi
if grep -Fq -- '--bin insight-agent-platform' Dockerfile || \
   grep -Fq '/usr/local/bin/insight-agent-platform' Dockerfile; then
  fail "candidate image still builds or launches the archived single-process runtime"
fi
if ! grep -Fq 'ENTRYPOINT ["/usr/local/bin/platform-gateway"]' Dockerfile; then
  fail "candidate runtime image does not default to the Platform Gateway"
fi
if [[ -d deploy/helm/insight-agent-platform ]]; then
  fail "archived single-process Helm chart remains in active deploy/helm"
fi
if ! grep -Fq 'x-insight-contract-status: current' contracts/platform-v1/openapi.yaml; then
  fail "Platform OpenAPI is not marked current after repository clean cut"
fi
for current_doc in README.md architecture.md cli.md api.md http-authoring.md console.md mcp.md operations.md; do
  if [[ ! -f "docs/current/$current_doc" ]]; then
    fail "current product documentation is missing docs/current/$current_doc"
  fi
done
report_matches \
  "current product documentation contains a positive old-runtime instruction" \
  -nEH \
  '(PLATFORM_CONFIG=config/platform\.quickstart\.yaml|api_version:[[:space:]]*insight\.agent/v1|cargo run[[:space:]]*$|terminal_only persistence|DSL v1 指南)' \
  docs/current/*.md

if ((failed != 0)); then
  exit 1
fi

printf 'Cutover residual scan passed.\n'
