#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench/terminal-only/lib.sh
source "$script_dir/lib.sh"

require_command jq
profile=${1:-qualification}
case "$profile" in
  qualification)
    message_count=1000000
    sample_count=1000
    ;;
  smoke)
    message_count=${AGED_MESSAGE_COUNT:-10000}
    sample_count=${AGED_QUERY_SAMPLES:-100}
    ;;
  *)
    printf 'usage: run-aged-query.sh [qualification|smoke] [result-directory]\n' >&2
    exit 2
    ;;
esac

batch_id=${AGED_BATCH_ID:-"$(date -u +%Y%m%dT%H%M%S)-${RANDOM}"}
conversation_id="aged-conversation-$batch_id"
tenant_id="aged-tenant-$batch_id"
gate_d_tenant_id=${AGED_ADMISSION_TENANT_ID:-"gate-d-$batch_id"}
deployment_revision_id="aged-fixture-revision-$batch_id"
result_dir=${2:-"$terminal_bench_root/bench/results/terminal-only-aged-$profile-$batch_id"}
mkdir -p "$result_dir"
assert_postgres_durability

postgres_file "$script_dir/sql/seed-aged-conversation.sql" \
  -v "conversation_id=$conversation_id" \
  -v "tenant_id=$tenant_id" \
  -v 'user_id=aged-query-user' \
  -v 'agent_id=action_demo' \
  -v "deployment_revision_id=$deployment_revision_id" \
  -v "message_count=$message_count" \
  >"$result_dir/seed.log"
actual_message_count=$(postgres_command -qAt -v ON_ERROR_STOP=1 -c "
  SELECT count(*)
  FROM conversation_messages
  WHERE conversation_id='$conversation_id';
")
printf '%s\n' "$actual_message_count" >"$result_dir/actual-message-count.txt"
[[ "$actual_message_count" =~ ^[0-9]+$ ]] &&
  (( actual_message_count == message_count )) || {
  printf 'aged fixture contains %s messages, expected exactly %s\n' \
    "$actual_message_count" "$message_count" >&2
  exit 1
}
postgres_file "$script_dir/sql/explain-aged-query.sql" \
  -v "conversation_id=$conversation_id" \
  >"$result_dir/explain.txt"
postgres_file "$script_dir/sql/measure-aged-query.sql" \
  -qAt \
  -v "conversation_id=$conversation_id" \
  -v "sample_count=$sample_count" \
  >"$result_dir/query-latency.json"

hot_query_fixture_source=gate_d_batch
postgres_file "$script_dir/sql/select-aged-hot-query-fixture.sql" \
  -qAt \
  -v "tenant_id=$gate_d_tenant_id" \
  >"$result_dir/hot-query-fixture.json"
if ! jq -e 'type == "object" and .run_id != null' \
  "$result_dir/hot-query-fixture.json" >/dev/null 2>&1; then
  if [[ "$profile" == "qualification" ]]; then
    printf 'no completed Gate D admission found for tenant %s\n' \
      "$gate_d_tenant_id" >&2
    exit 1
  fi
  hot_query_fixture_source=recent_existing_smoke
  postgres_file "$script_dir/sql/select-aged-hot-query-fixture.sql" \
    -qAt \
    -v 'tenant_id=' \
    >"$result_dir/hot-query-fixture.json"
fi
jq -e '
  type == "object" and
  (.run_id | type == "string" and length > 0) and
  (.tenant_id | type == "string" and length > 0) and
  (.request_id | type == "string" and length > 0) and
  .result_present == true and
  (.owner_registry_rows | type == "number") and
  .owner_registry_rows <= 1
' "$result_dir/hot-query-fixture.json" >/dev/null
hot_run_id=$(jq -er '.run_id' "$result_dir/hot-query-fixture.json")
hot_tenant_id=$(jq -er '.tenant_id' "$result_dir/hot-query-fixture.json")
hot_request_id=$(jq -er '.request_id' "$result_dir/hot-query-fixture.json")
owner_registry_rows=$(jq -er '.owner_registry_rows' \
  "$result_dir/hot-query-fixture.json")

postgres_file "$script_dir/sql/explain-aged-admission-lookup.sql" \
  -qAt \
  -v "tenant_id=$hot_tenant_id" \
  -v "request_id=$hot_request_id" \
  >"$result_dir/admission-lookup-plan.json"
postgres_file "$script_dir/sql/explain-aged-derived-run-lookup.sql" \
  -qAt \
  -v "run_id=$hot_run_id" \
  -v "tenant_id=$hot_tenant_id" \
  >"$result_dir/derived-run-lookup-plan.json"

plan_latency_ceiling_ms=${AGED_HOT_QUERY_PLAN_MAX_MS:-20}
[[ "$plan_latency_ceiling_ms" =~ ^[0-9]+([.][0-9]+)?$ ]] || {
  printf 'AGED_HOT_QUERY_PLAN_MAX_MS must be a non-negative number\n' >&2
  exit 2
}

# Admissions/results are unbounded ledgers and may never use a sequential scan
# in these point lookups. The owner registry has at most one row by the
# single-runtime invariant, so PostgreSQL may correctly prefer a bounded scan
# over its primary key; that exception is accepted only when both the captured
# registry cardinality and the plan's examined-row count remain <= 1.
jq -e \
  --argjson latency_ceiling_ms "$plan_latency_ceiling_ms" '
    def nodes: [.. | objects | select(has("Node Type"))];
    .[0] as $explain
    | (nodes) as $nodes
    | ($nodes | map(select(
        ."Relation Name" == "terminal_run_admissions" and
        (."Node Type" | test("Index")) and
        ."Index Name" == "terminal_run_admissions_tenant_request_key"
      )) | length) >= 1
      and ($nodes | map(select(."Node Type" == "Seq Scan")) | length) == 0
      and $explain.Plan."Actual Rows" == 1
      and $explain."Execution Time" <= $latency_ceiling_ms
  ' "$result_dir/admission-lookup-plan.json" >/dev/null

jq -e \
  --argjson latency_ceiling_ms "$plan_latency_ceiling_ms" \
  --argjson owner_registry_rows "$owner_registry_rows" '
    def nodes: [.. | objects | select(has("Node Type"))];
    .[0] as $explain
    | (nodes) as $nodes
    | ($nodes | map(select(
        ."Relation Name" == "terminal_run_admissions" and
        (."Node Type" | test("Index")) and
        ."Index Name" == "terminal_run_admissions_pkey"
      )) | length) >= 1
      and ($nodes | map(select(
        ."Relation Name" == "terminal_run_results" and
        (."Node Type" | test("Index")) and
        ."Index Name" == "terminal_run_results_pkey"
      )) | length) >= 1
      and ($nodes | map(select(
        ."Node Type" == "Seq Scan" and
        ."Relation Name" != "terminal_runtime_instances"
      )) | length) == 0
      and (
        $nodes
        | map(select(."Relation Name" == "terminal_runtime_instances"))
        | length
      ) >= 1
      and (
        $nodes
        | map(select(
            ."Node Type" == "Seq Scan" and
            ."Relation Name" == "terminal_runtime_instances"
          ))
        | length
      ) <= 1
      and (
        $nodes
        | map(select(
            ."Node Type" == "Seq Scan" and
            ."Relation Name" == "terminal_runtime_instances" and
            (
              (."Actual Rows" // 0) > 1 or
              (."Actual Loops" // 0) > 1 or
              (."Rows Removed by Filter" // 0) > 1
            )
          ))
        | length
      ) == 0
      and $owner_registry_rows <= 1
      and $explain.Plan."Actual Rows" == 1
      and $explain."Execution Time" <= $latency_ceiling_ms
  ' "$result_dir/derived-run-lookup-plan.json" >/dev/null

admission_plan_summary=$(jq -c '
  def nodes: [.. | objects | select(has("Node Type"))];
  .[0] as $explain
  | (nodes) as $nodes
  | {
      planning_time_ms: $explain."Planning Time",
      execution_time_ms: $explain."Execution Time",
      root_actual_rows: $explain.Plan."Actual Rows",
      node_types: ($nodes | map(."Node Type") | unique),
      indexes: (
        $nodes
        | map(select(has("Index Name")) | ."Index Name")
        | unique
      ),
      seq_scan_relations: (
        $nodes
        | map(select(."Node Type" == "Seq Scan") | ."Relation Name")
        | unique
      )
    }
' "$result_dir/admission-lookup-plan.json")
derived_plan_summary=$(jq -c '
  def nodes: [.. | objects | select(has("Node Type"))];
  .[0] as $explain
  | (nodes) as $nodes
  | {
      planning_time_ms: $explain."Planning Time",
      execution_time_ms: $explain."Execution Time",
      root_actual_rows: $explain.Plan."Actual Rows",
      node_types: ($nodes | map(."Node Type") | unique),
      indexes: (
        $nodes
        | map(select(has("Index Name")) | ."Index Name")
        | unique
      ),
      seq_scan_relations: (
        $nodes
        | map(select(."Node Type" == "Seq Scan") | ."Relation Name")
        | unique
      ),
      growing_relation_seq_scans: (
        $nodes
        | map(select(
            ."Node Type" == "Seq Scan" and
            ."Relation Name" != "terminal_runtime_instances"
          ) | ."Relation Name")
        | unique
      ),
      bounded_owner_scan_rows: (
        $nodes
        | map(select(
            ."Node Type" == "Seq Scan" and
            ."Relation Name" == "terminal_runtime_instances"
          ) | {
            actual_rows: (."Actual Rows" // 0),
            actual_loops: (."Actual Loops" // 0),
            rows_removed_by_filter: (."Rows Removed by Filter" // 0)
          })
      )
    }
' "$result_dir/derived-run-lookup-plan.json")

jq -e \
  --argjson samples "$sample_count" '
    .samples == $samples and .p95_ms <= 20
  ' "$result_dir/query-latency.json" >/dev/null
if ! grep -Eq 'Index Scan|Index Only Scan|Bitmap Index Scan' \
  "$result_dir/explain.txt"; then
  printf 'aged recent-message query did not use an index\n' >&2
  exit 1
fi
assert_postgres_durability
jq -n \
  --slurpfile latency "$result_dir/query-latency.json" \
  --arg profile "$profile" \
  --argjson messages "$actual_message_count" \
  --argjson configured_messages "$message_count" \
  --arg conversation_id "$conversation_id" \
  --arg persistence_mode terminal_only \
  --arg deployment_revision_id "$deployment_revision_id" \
  --arg hot_query_fixture_source "$hot_query_fixture_source" \
  --slurpfile hot_query_fixture "$result_dir/hot-query-fixture.json" \
  --argjson plan_latency_ceiling_ms "$plan_latency_ceiling_ms" \
  --argjson admission_plan "$admission_plan_summary" \
  --argjson derived_plan "$derived_plan_summary" \
  '{
    passed: true,
    profile: $profile,
    messages: $messages,
    configured_messages: $configured_messages,
    seeded_message_count_verified: ($messages == $configured_messages),
    conversation_id: $conversation_id,
    persistence_mode: $persistence_mode,
    deployment_revision_id: $deployment_revision_id,
    query_latency: {
      samples: $latency[0].samples,
      p50_ms: $latency[0].p50_ms,
      p95_ms: $latency[0].p95_ms,
      p99_ms: $latency[0].p99_ms,
      max_ms: $latency[0].max_ms
    },
    hot_query_plans: {
      fixture_source: $hot_query_fixture_source,
      run_id: $hot_query_fixture[0].run_id,
      tenant_id: $hot_query_fixture[0].tenant_id,
      result_present: $hot_query_fixture[0].result_present,
      owner_present: $hot_query_fixture[0].owner_present,
      owner_registry_rows: $hot_query_fixture[0].owner_registry_rows,
      latency_ceiling_ms: $plan_latency_ceiling_ms,
      admission_lookup: ($admission_plan + {
        expected_index:
          "terminal_run_admissions_tenant_request_key",
        no_seq_scan: true,
        passed: true
      }),
      derived_run_lookup: ($derived_plan + {
        expected_growing_relation_indexes: [
          "terminal_run_admissions_pkey",
          "terminal_run_results_pkey"
        ],
        no_growing_relation_seq_scan: true,
        bounded_owner_registry_policy_passed: true,
        passed: true
      })
    }
  }' >"$result_dir/aged-query-report.json"
printf 'Aged query evidence: %s\n' "$result_dir"
