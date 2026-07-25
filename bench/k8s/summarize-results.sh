#!/usr/bin/env bash
set -euo pipefail

root=${1:-bench/results}

command -v jq >/dev/null 2>&1 || {
  printf 'jq is required to summarize k6 JSON results\n' >&2
  exit 1
}

printf '| profile | completed | failed/rejected | success | completed/s | create p95 | lifecycle p50 | lifecycle p95 | lifecycle p99 | runtime peak | PostgreSQL peak | max lock waiters |\n'
printf '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n'

find "$root" -mindepth 1 -maxdepth 1 -type d -print | sort |
  while IFS= read -r directory; do
    summary="$directory/summary.json"
    [[ -f "$summary" ]] || continue
    profile=$(basename "$directory")
    read -r elapsed_ms completed failed success create_p95 lifecycle_p50 lifecycle_p95 lifecycle_p99 < <(
      jq -r '[
        .state.testRunDurationMs,
        (.metrics.run_completed.values.count // 0),
        (.metrics.run_failed.values.count // 0),
        ((.metrics.run_terminal_success.values.rate // 0) * 100),
        (.metrics.run_create_duration.values["p(95)"] // 0),
        (.metrics.run_lifecycle_duration.values.med // 0),
        (.metrics.run_lifecycle_duration.values["p(95)"] // 0),
        (.metrics.run_lifecycle_duration.values["p(99)"] // 0)
      ] | @tsv' "$summary"
    )

    read -r runtime_cpu runtime_memory postgresql_cpu postgresql_memory < <(
      awk -F, '
        NR > 1 {
          cpu = $4
          memory = $5
          if (cpu ~ /m$/) sub(/m$/, "", cpu); else cpu *= 1000
          if (memory ~ /Ki$/) {sub(/Ki$/, "", memory); memory /= 1024}
          else if (memory ~ /Mi$/) sub(/Mi$/, "", memory)
          else if (memory ~ /Gi$/) {sub(/Gi$/, "", memory); memory *= 1024}
          cpu += 0
          memory += 0
          if (cpu > peak_cpu[$3]) peak_cpu[$3] = cpu
          if (memory > peak_memory[$3]) peak_memory[$3] = memory
        }
        END {
          printf "%.0f %.0f %.0f %.0f\n",
            peak_cpu["runtime"], peak_memory["runtime"],
            peak_cpu["postgresql"], peak_memory["postgresql"]
        }
      ' "$directory/resources.csv"
    )

    lock_waiters=$(awk -F, '
      NR > 1 && $3 > maximum {maximum = $3}
      END {print maximum + 0}
    ' "$directory/database-activity.csv")

    awk \
      -v profile="$profile" \
      -v elapsed_ms="$elapsed_ms" \
      -v completed="$completed" \
      -v failed="$failed" \
      -v success="$success" \
      -v create_p95="$create_p95" \
      -v lifecycle_p50="$lifecycle_p50" \
      -v lifecycle_p95="$lifecycle_p95" \
      -v lifecycle_p99="$lifecycle_p99" \
      -v runtime_cpu="$runtime_cpu" \
      -v runtime_memory="$runtime_memory" \
      -v postgresql_cpu="$postgresql_cpu" \
      -v postgresql_memory="$postgresql_memory" \
      -v lock_waiters="$lock_waiters" '
      BEGIN {
        measured_seconds = (elapsed_ms - 8000) / 1000
        if (measured_seconds <= 0) measured_seconds = 0.001
        printf "| %s | %.0f | %.0f | %.2f%% | %.2f | %.0fms | %.0fms | %.0fms | %.0fms | %.0fm/%.0fMi | %.0fm/%.0fMi | %.0f |\n",
          profile, completed, failed, success, completed / measured_seconds,
          create_p95, lifecycle_p50, lifecycle_p95, lifecycle_p99,
          runtime_cpu, runtime_memory, postgresql_cpu, postgresql_memory,
          lock_waiters
      }
    '
  done
