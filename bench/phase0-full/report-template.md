# Phase 0 full-runtime 10 rps × 10 minute WAL baseline

Status: `Not run | Blocked | Passed | Failed`

## Reproduction identity

| Field | Value / evidence |
|---|---|
| Git commit | |
| runtime image digest | |
| PostgreSQL image/version | |
| Kubernetes context / namespace / release | |
| fresh preflight | `infrastructure-freshness.json` |
| database statistics epoch | `statistics-reset-before-warmup.json` |
| effective Helm profile | `full`, one `action_demo` |
| PostgreSQL / Artifact PVC | `24Gi` / `2Gi` |
| `max_wal_size` / `wal_keep_size` | `4GB` / `8GB` |

## Fixed workload

| Field | Required | Observed |
|---|---:|---:|
| excluded warm-up | 60s | |
| warm-up arrivals / terminal success | 600 / 600 | |
| measured arrivals | 10/s × 600s = 6,000 | |
| raw iterations / exact scheduled arrivals | 6,000 / 6,000 | |
| dropped / late arrivals | 0 / 0 | |
| arrival lateness p95 / p99 / max | ordered and `<100ms` | |
| Agent / persistence | `action_demo` / `full` | |
| fixed body | `{"text":"phase0 full WAL baseline fixture"}` | |
| accepted closure | 100% | |
| scheduled success | ≥99.9% | |
| completed throughput | ≥9/s | |
| runtime/PostgreSQL restart | 0 / 0 | |

## WAL attribution

| Metric | Observed | Integrity rule |
|---|---:|---:|
| `pg_stat_wal` delta | | >0 |
| WAL / accepted Run | | report only; no terminal ceiling |
| top-level pgss top-30 WAL | | embedded snapshot = derived CSV |
| all top-level pgss WAL | | top-30 ≤ all |
| nested pgss WAL | | diagnostic; never added to top-level |
| exact-LSN `pg_walinspect` record bytes | | |
| physical / `pg_stat_wal` | | 95%–105% |
| physical / LSN byte span | | 95%–105% |
| payload-relation WAL | | |
| Artifact-metadata WAL | | |
| structural WAL | | |
| mixed + unmapped WAL | | |
| payload/object vs structural classification | | ≥95% |

Link `postgres-before.json`, `postgres-after.json`,
`postgres-top-wal-statements.csv`, `postgres-physical-wal.json`, and
`postgres-physical-wal.csv`.

## Relation, index, and object growth

| Category | Table/auxiliary bytes | Index bytes | Total |
|---|---:|---:|---:|
| payload relations | | | |
| Artifact metadata relations | | | |
| external Artifact objects | n/a | n/a | |
| structural relations | | | |
| static catalog relations | | | |

Attach the ranked table and individual-index deltas from
`phase0-full-report.json`. Report exact row growth for `workflow_runs`,
`payloads`, `execution_events`, `projection_checkpoints`,
`scheduler_checkpoints`, and `public_event_outbox`; these rows are expected in
full mode and have no terminal-only zero-delta threshold.

## Historical comparison

The retained full-runtime two-hour reference is `71,033,480,938` WAL bytes for
`71,801` accepted Runs. Record the new/old WAL-per-accepted ratio, while
explicitly stating that the old top-statements capture explained only part of
that interval and is not the new ≥95% physical attribution evidence.

## Decision

- Phase 0 evidence integrity:
- failures / blockers:
- interpretation:
- raw evidence directory:
