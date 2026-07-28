# Terminal-only qualification report

- Date (UTC):
- Commit SHA:
- Dirty worktree patch:
- Runtime image digest:
- PostgreSQL version/image:
- Kubernetes/node:
- Effective Helm values/manifest:
- Status: `Passed | Failed | Blocked | Not run`

## Fixed workload

- Agent/Deployment Revision:
- Frozen persistence mode:
- Runtime replicas / max active:
- Arrival rate / measured duration / warm-up:
- Input bytes / output bytes / Artifact bytes:
- Fault fixture/provider:

## PostgreSQL durability

| setting | observed | required |
|---|---:|---:|
| fsync |  | on |
| full_page_writes |  | on |
| synchronous_commit |  | on or remote_apply |
| pg_stat_statements.track |  | all |
| terminal/Conversation/deletion/staging relations |  | LOGGED |
| runtime owner registry |  | UNLOGGED |

Attach `postgres-before.json`, `postgres-after.json`, top-30 WAL SQL, complete
public durable-table row snapshot, table/index sizes, checkpoint/fsync latency,
database logs, and PostgreSQL cgroup evidence. Confirm WAL/bgwriter/database/I/O
`stats_reset` continuity and non-negative counter deltas.

## Gate A

| case | admissions Δ | results Δ | messages Δ | existing ledger Δ | result |
|---|---:|---:|---:|---:|---|
| standalone action |  |  | 0 | 0 |  |
| Conversation turn |  |  | 2 | 0 |  |
| Attached/SSE scaling |  |  | 2/turn | 0 |  |

## Gate B

| metric | observed | threshold | result |
|---|---:|---:|---|
| accepted / terminal observed |  | closure 100% |  |
| configured / actual duration |  | 7200s / 7200–7320s |  |
| raw iterations / exact scheduled arrivals |  | 72000 / 72000 |  |
| dropped / late arrivals |  | 0 / 0 |  |
| arrival lateness p95 / p99 / max |  | ordered and `<100ms` |  |
| admission / result row delta |  | accepted / terminal observed |  |
| scheduled success |  | ≥99.9% |  |
| completed throughput |  | ≥9/s |  |
| lifecycle p95 / p99 |  | ≤1s / ≤3s |  |
| structural WAL |  | report |  |
| payload/object WAL |  | report |  |
| total WAL |  | ≤2.2GiB |  |
| WAL / accepted |  | ≤32KiB |  |
| structural relation growth / accepted |  | ≤16KiB |  |
| requested checkpoint |  | 0 |  |
| deadlock / temp spill / OOM |  | 0 |  |
| forbidden durable-table row delta |  | 0 |  |
| raw top-30 / total WAL ratio |  | report |  |
| raw top-level-all / total WAL ratio |  | ≤105% |  |
| top-30 / top-level-all SQL coverage |  | ≥95% |  |
| nested SQL WAL/calls |  | diagnostic only |  |
| physical record WAL coverage |  | 95%–105% |  |
| wal_keep_size before / after |  | ≥3GiB / unchanged |  |
| active sample count/span/max gap |  | ≥95% / ≥95% / ≤5s |  |
| desired/ready/selected runtime Pods |  | 1 / 1 / 1 |  |

Record process RSS/PSS, active Run, terminal commit retries, and transaction
counts in addition to the table. Record the unchanged Pod UID, raw top-30/total
and top-level-all/total SQL ratios, top-30 coverage of top-level SQL, nested
diagnostics, and unchanged statement deallocation counter. Attach canonical,
strictly increasing LSNs and ordered boundary timestamps. Attach authoritative
`pg_walinspect` JSON plus its mechanically derived CSV, including extension
version, resource-manager/record-type groups, group/totals equality and
95%–105% physical record coverage. Per-table autovacuum/autoanalyze counts and
timestamps are correlation-only diagnostics and must never claim residual
bytes. Attach fresh namespace/release/PVC/database preflight JSON.

## Gate C

| fault point | admissions | committed terminal | interrupted | effect calls | duplicate result | orphan assistant | result |
|---|---:|---:|---:|---:|---:|---:|---|
| before execution |  |  |  |  | 0 | 0 |  |
| action/LLM executing |  |  |  |  | 0 | 0 |  |
| after external effect |  |  |  |  | 0 | 0 |  |
| after terminal commit, before SSE |  |  |  |  | 0 | 0 |  |
| PostgreSQL restart |  |  |  |  | 0 | 0 |  |
| summary worker crash |  |  |  | n/a | 0 | 0 |  |
| graceful shutdown |  |  |  |  | 0 | 0 |  |

- Same request ID replay evidence:
- New request ID explicit retry evidence:
- No recovery observation window:
- Provider idempotency/effect ledger:
- Active Runs immediately before kill (must be 50):
- Killed / replacement Pod names and UIDs:
- PostgreSQL postmaster start before / after:

## Gate D

| metric | observed | required | result |
|---|---:|---:|---|
| Conversations × turns |  | 100 × 100 |  |
| user / assistant messages |  | 10000 / 10000 |  |
| idempotent replay duplicates |  | 0 |  |
| cursor omissions / duplicates |  | 0 / 0 |  |
| turn/order/atomicity violations |  | 0 |  |
| context selection |  | latest summary + bounded recent |  |
| 1× / 10× chunk message rows |  | identical (2/turn) |  |
| SSE delta / terminal / Run GET / assistant content |  | identical |  |
| aged message population |  | 1,000,000 |  |
| recent-50 samples / p95 |  | 1000 / ≤20ms |  |
| summary failure blocked turns |  | 0 |  |
| privacy content readable |  | 0 |  |
| object deletion receipt/retries |  | complete |  |
| stored Artifact magic / active key version |  | IAPTEA01 / configured |  |
| tenant ID / marker plaintext in ciphertext |  | absent / absent |  |
| control tenant API / objects after target DELETE |  | readable / present |  |

Attach cursor workload summary, DB atomicity JSON, context metrics/tests,
summary-fault log, aged-query latency and EXPLAIN, privacy HTTP/DB evidence,
tenant-encryption report (never the key), and object-store receipt.

## Decision

- Gate A:
- Gate B:
- Gate C:
- Gate D:
- Default remains `full`:
- Independent rollout decision reference:
- Deviations/blockers:
