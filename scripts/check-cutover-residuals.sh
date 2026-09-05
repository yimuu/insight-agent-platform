#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

failed=0

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

# The active tree is a virtual Platform workspace. These paths belonged to the removed
# insight.agent/v1 facade, its single-process runtime, or its executable qualification harnesses.
# Historical evidence remains available from Git rather than as a second active product surface.
removed_active_paths=(
  src
  crates/engine
  crates/dsl
  crates/durable
  crates/resources
  crates/mcp
  crates/storage
  crates/runtime
  crates/api
  agents
  catalog
  config
  database
  schemas
  deploy/archive
  docker-compose.postgres.yml
  examples/run_stream_attached_qualification.rs
  tests/baselines
  tests/interop
  tests/support
  tests/fixtures/dsl
  bench/k8s
  bench/phase0-full
  bench/reports
  bench/run-stream-nats-core
  bench/terminal-only
  scripts/check-public-api-baseline.sh
  scripts/check-public-api-compat-bridges.py
  scripts/normalize-public-api-rustdoc.py
  scripts/provision-postgres-schema.sh
  scripts/provision-sqlite-schema.sh
  scripts/qualify-mcp-external-sdk.sh
)
for path in "${removed_active_paths[@]}"; do
  while IFS= read -r tracked_path; do
    if [[ -e "$tracked_path" || -L "$tracked_path" ]]; then
      fail "removed tracked active surface exists: $tracked_path"
    fi
  done < <(git ls-files -- "$path")
done

# A virtual workspace cannot silently acquire another root facade and root integration suite.
while IFS= read -r path; do
  if [[ "$path" =~ ^tests/[^/]+\.rs$ && -e "$path" ]]; then
    fail "root integration test bypasses the qualification package: $path"
  fi
done < <(git ls-files -- tests)

production_source_roots=()
member_manifests=(Cargo.toml)
while IFS= read -r -d '' source_root; do
  production_source_roots+=("$source_root")
done < <(find crates -type d -name src -print0)
while IFS= read -r -d '' manifest; do
  member_manifests+=("$manifest")
done < <(find crates -type f -name Cargo.toml -print0)

# Deleted implementation boundaries must not be recreated under another crate path.
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

if [[ -d migrations/durable ]]; then
  fail "pre-1.0 durable migration history still exists: migrations/durable"
fi
while IFS= read -r -d '' path; do
  fail "deleted implementation path exists: $path"
done < <(find crates -type d -path '*/migrations/formal_v2' -print0)

report_matches \
  "production source contains deleted runtime Schema migration authority" \
  -RInE --include='*.rs' \
  '(DURABLE_MIGRATIONS|DurableMigration|SqliteMigrationGuard|apply_migrations|migrate_schema|initialize_schema|migration_manifest)' \
  "${production_source_roots[@]}"

report_matches \
  "production source embeds the removed durable Schema" \
  -RInE --include='*.rs' \
  '(include_(str|bytes)!\([^)]*database/durable|database/durable/(postgres|sqlite)/schema\.sql)' \
  "${production_source_roots[@]}"

report_matches \
  "production source contains a deleted execution symbol" \
  -RInE --include='*.rs' \
  '(scope_scheduler|mark_incomplete_interrupted|legacy_scheduler|LegacyScheduler|OldScheduler|runtime_local_(only_)?value_store|RegionYield|BranchPhi|ExecutableRegion|NodeKind::Switch|RawSwitch|SwitchDescriptor|SchedulerAction::Switch|core\.branch_end|USE_(OLD|V[0-9]+)_SCHEDULER|ENABLE_(OLD|V[0-9]+)_SCHEDULER)' \
  "${production_source_roots[@]}"

report_matches \
  "CLI runtime profile contains a legacy port compatibility fallback" \
  -nE '(legacy_defaults|persisted_pre_[[:alnum:]_]*ports.*defaults|serde\(default[[:space:]]*=[^)]*port)' \
  crates/insight-cli/src/lib.rs crates/insight-cli/src/full_profile.rs

report_matches \
  "local development state retains a pre-egress compatibility branch" \
  -nF 'matches!(identity.schema_version, 2 | 3)' \
  crates/insight-cli/src/lib.rs
report_matches \
  "development bootstrap retains a pre-egress compatibility branch" \
  -nE 'matches!\(self\.schema_version,[[:space:]]*1[[:space:]]*\|[[:space:]]*2\)|egress_broker:[[:space:]]*Option<' \
  crates/platform-postgres/src/bin/platform_dev_bootstrap.rs
report_matches \
  "durable PostgreSQL payload retains an implicit required-field compatibility default" \
  -nE '#\[serde\(default[[:space:]]*\)\]' \
  crates/platform-postgres/src/repository.rs
if ! grep -Fq 'if identity.schema_version != 3' crates/insight-cli/src/lib.rs; then
  fail "local identity does not require the current schema_version 3"
fi
if ! grep -Fq 'if self.schema_version != 2' \
  crates/platform-postgres/src/bin/platform_dev_bootstrap.rs; then
  fail "development bootstrap does not require the current schema_version 2"
fi

for removed_runner_contract in \
  deploy/helm/insight-platform-sandbox/vendor/runner-protocol-v1.schema.json \
  deploy/helm/insight-platform-sandbox/vendor/containerd-runc-runtime-v1.json; do
  if [[ -e "$removed_runner_contract" || -L "$removed_runner_contract" ]]; then
    fail "removed runner contract exists: $removed_runner_contract"
  fi
done
report_matches \
  "active Sandbox boundary retains the removed runner template or route identity" \
  -RInE '(armed-runner-v1|runner protocol v1|/v1/(state|activate|result))' \
  crates/platform-opensandbox-client/src \
  deploy/helm/insight-platform-sandbox/files \
  deploy/helm/insight-platform-sandbox/templates \
  deploy/kind/probes
runner_production_matches=$(
  sed '/^#\[cfg(test)\]/,$d' crates/platform-sandbox-runner/src/lib.rs |
    grep -nE '(armed-runner-v1|runner protocol v1|/v1/(state|activate|result))' || true
)
if [[ -n "$runner_production_matches" ]]; then
  fail "production runner source retains the removed runner route identity"
  printf '%s\n' "$runner_production_matches" >&2
fi
if grep -Fq \
  'COPY --from=builder /workspace/target/release/platform-sandbox-runner /usr/local/bin/platform-sandbox-runner' \
  Dockerfile; then
  fail "generic runtime image retains the removed non-launcher Sandbox runner path"
fi

report_matches \
  "Cargo feature reintroduces an old/new scheduler split" \
  -nE '(old|legacy|v[0-9]+)[_-]?scheduler|scheduler[_-]?(old|legacy|v[0-9]+)' \
  "${member_manifests[@]}"

report_matches \
  "active product surface contains the removed facade, DSL, persistence mode, or management routes" \
  -RInE --exclude-dir=node_modules --exclude='*.lock' \
  '(insight_agent_platform|api_version:[[:space:]]*insight\.agent/v1|terminal_only|/v1/admin/agents|/v1/graph-agents)' \
  crates contracts/platform-v1 release examples/productization tests/productization web/console

# The active Cargo and image closure must not retain a second runtime or persistence authority.
if grep -Eq '^\[package\]' Cargo.toml; then
  fail "workspace root is a package instead of a virtual manifest"
fi
if ! grep -Fq 'default-members = ["crates/insight-cli"]' Cargo.toml; then
  fail "Cargo default-members does not select the insight CLI"
fi
report_matches \
  "Cargo manifest references a removed legacy crate" \
  -nE 'crates/(engine|dsl|durable|resources|mcp|storage|runtime|api)(/|")|^[[:space:]]*insight-(engine|dsl|durable|resources|mcp|storage|runtime|api)[[:space:]]*=' \
  Cargo.toml
report_matches \
  "Cargo SQLx closure enables the removed SQLite business-state backend" \
  -nE '^sqlx[[:space:]]*=.*sqlite' \
  Cargo.toml
report_matches \
  "candidate image copies a removed root source or data authority" \
  -nE 'COPY[[:space:]]+(src|catalog|config|database|schemas)([[:space:]]|/)|/app/database|--bin[[:space:]]+(insight-agent-platform|agentctl|providerctl|management-migrate)' \
  Dockerfile
if ! grep -Fq 'ENTRYPOINT ["/usr/local/bin/platform-gateway"]' Dockerfile; then
  fail "candidate runtime image does not default to the Platform Gateway"
fi
if [[ -d deploy/helm/insight-agent-platform ]]; then
  fail "single-process Helm chart remains in active deploy/helm"
fi
report_matches \
  "ordinary CI references a removed root package or compatibility harness" \
  -nE '(-p|--package)[[:space:]]+insight-agent-platform|root public API baseline|mcp-interop|qualify-mcp-external-sdk' \
  .github/workflows/*.yml

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
