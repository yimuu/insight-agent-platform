# Phase 0 full-runtime WAL baseline

Formal status: **Passed** on 2026-07-28. The authoritative evidence is
`bench/results/2026-07-28-terminal-only-qualified/phase0-full-10rps-10m-final2/phase0-full-report.json`:
600/600 warm-up and 6,000/6,000 measured Runs succeeded, with no dropped,
rejected, failed, interrupted, early, or late arrival.

This historical harness closed the terminal-only Phase 0 evidence prerequisite; the retired
specification remains available in Git history.
It measures the existing **full** durable engine on a fresh PostgreSQL 16
deployment; it is not a shortened Terminal-only Gate B and it
does not apply terminal-only forbidden-ledger or WAL ceilings.

## Formal profile

- one freshly provisioned runtime and PostgreSQL deployment;
- only `action_demo`, frozen to `persistence_mode: full`;
- byte-for-byte fixed request body
  `{"text":"phase0 full WAL baseline fixture"}`;
- 1 minute warm-up excluded from the boundary only after all 600 scheduled
  Runs are admitted, observed terminal, and successful;
- 10 arrivals/s for exactly 10 minutes (6,000 scheduled arrivals);
- maximum 50 VUs and the C1 runtime/PostgreSQL resource profile;
- `fsync=on`, `full_page_writes=on`, `synchronous_commit=on` or stronger;
- PostgreSQL PVC `24Gi`, Artifact PVC `2Gi`, `max_wal_size=4GB`,
  `wal_keep_size=8GB`.

The storage headroom is intentional. The historical full-runtime result
generated 71,033,480,938 bytes in two hours, approximately 5.5GiB per ten
minutes at the same arrival rate. The 8GB retention floor must preserve the
whole exact LSN interval for `pg_walinspect`; the 24Gi PVC also leaves room for
the full ledgers and checkpoints created during the baseline.

## Fresh deployment and execution

First prove the namespace, Helm release, and PVC names do not exist. The shared
preflight creates and annotates the fresh namespace:

```bash
BENCH_NAMESPACE=insight-phase0-full \
BENCH_RELEASE=phase0-full \
  bash bench/terminal-only/preflight-fresh-qualification.sh \
  bench/results/phase0-full-preflight.json
```

Build/push the intended benchmark image, then deploy the exact full profile:

```bash
helm upgrade --install phase0-full \
  deploy/helm/insight-agent-platform \
  -n insight-phase0-full \
  -f deploy/helm/insight-agent-platform/values-benchmark.yaml \
  -f deploy/helm/insight-agent-platform/values-benchmark-c1.yaml \
  -f deploy/helm/insight-agent-platform/values-phase0-full-baseline.yaml
```

After making the runtime API reachable through `BASE_URL`, run:

```bash
BASE_URL=http://127.0.0.1:3000 \
BENCH_NAMESPACE=insight-phase0-full \
BENCH_RELEASE=phase0-full \
PHASE0_FULL_PREFLIGHT_EVIDENCE=bench/results/phase0-full-preflight.json \
  bash bench/phase0-full/run-phase0-full.sh qualification \
  bench/results/phase0-full-10rps-10m
```

`smoke` accepts duration/rate overrides and checks wiring only. It is never
formal Phase 0 evidence.

## Fail-closed evidence

The evaluator rejects missing or inconsistent evidence, including:

- a non-fresh namespace/PVC/release, a non-`full` `action_demo`, a shortened
  formal duration, fixture drift, a missing explicit `pg_stat_reset()` epoch,
  any dropped/unclosed/failed warm-up or measured arrival, runtime restart,
  PostgreSQL restart, weakened durability, or an undersized `wal_keep_size`;
- `pg_stat_statements` reset/deallocation drift, nested/top-level
  double-counting, or a derived top-30 CSV that differs from the top-level
  rows embedded in the after boundary;
- `pg_walinspect` start/end LSNs that differ from the snapshots, resource
  manager/record groups or classification groups that do not sum exactly, or
  physical-record coverage outside 95%–105% of either `pg_stat_wal` or the LSN
  byte span;
- payload/object versus structural physical classification below 95%.

Every WAL record is counted once. Heap, index, and TOAST block references map
back to a root relation. `payloads` is the payload category; the closed
Artifact relation set is object metadata; block-free records and all other
mapped relations are structural. Mixed/unmapped records remain explicit and
cannot be silently reassigned. Artifact object bytes are independently
measured from the shared volume.

The report also saves exact table row deltas and table/index byte deltas for
every public relation. Full durable ledgers are expected to grow and are
reported without applying terminal-only zero-write or 16KiB/Run limits.

The historical two-hour value remains a comparison:

- total WAL: `71,033,480,938` bytes;
- accepted Runs: `71,801`;
- report:
  [2026-07-26 durable-runtime capacity qualification](../reports/2026-07-26-durable-runtime-50-active-runs-optimized.md).

It is not relabeled as 95%-attributed evidence; the new exact-LSN run supplies
that independent attribution.
