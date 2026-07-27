# Limited-resource Kubernetes benchmark

The Helm chart deploys one runtime Pod and one PostgreSQL 16 Pod. Benchmark
profiles are overlays on `values-benchmark.yaml`:

- `values-benchmark-limited.yaml`: Gate A, `500m / 256Mi` per service;
- `values-benchmark-c1.yaml`: Gate B/C/D C1 capacity;
- `values-benchmark-c2.yaml`: Gate B/C/D qualification ceiling.

The artifact volume uses a PVC because its durable authority must survive
runtime Pod replacement. C1/C2 also enable PostgreSQL persistence so aged and
soak evidence is not silently discarded by a Pod replacement.

The load generator is measurement infrastructure rather than part of the
runtime/PostgreSQL qualification budget. The benchmark base gives k6 `2 CPU /
2Gi` so a two-hour lifecycle soak is not distorted by k6 heap reclaim or CPU
throttling. A result is invalid when the load generator reaches its cgroup
limit, even if the service Pods remain below theirs.

Build and install:

```bash
docker build --platform linux/arm64 -t insight-agent-platform:benchmark .
helm upgrade --install bench deploy/helm/insight-agent-platform \
  --namespace insight-bench \
  --create-namespace \
  --values deploy/helm/insight-agent-platform/values-benchmark.yaml \
  --values deploy/helm/insight-agent-platform/values-benchmark-limited.yaml \
  --wait --timeout 5m
```

Run a lifecycle profile with Grafana k6:

```bash
BENCH_IMAGE_TAG=qualification \
  bash bench/k8s/run-profile.sh limited-smoke 1 15s
bash bench/k8s/run-profile.sh limited-baseline 4 30s
bash bench/k8s/run-profile.sh c1-saturation 8 45s
bash bench/k8s/run-profile.sh c2-overload 16 45s
bash bench/k8s/run-profile.sh c1-soak 1 2m
```

The profile prefix automatically selects the matching overlay; set
`BENCH_PROFILE_VALUES=/absolute/path/to/values.yaml` to override it. Set
`BENCH_IMAGE_REPOSITORY` and `BENCH_IMAGE_TAG` to pin an
already-built qualification image without editing the profile overlays.
Long-duration tests can raise only the load-generator memory budget with
`BENCH_LOADTEST_MEMORY_REQUEST` and `BENCH_LOADTEST_MEMORY_LIMIT`; these
overrides do not change the runtime or PostgreSQL budget and are captured in
the effective Helm values. Each virtual user repeatedly creates an `action_demo` Run and polls its durable projection
until terminal. k6 reports accepted, capacity-rejected, conflict, 5xx, timeout
and other rejection counts separately. `run_create_duration` contains accepted
requests only; `run_create_request_duration` contains every HTTP attempt.
Set `BENCH_SCENARIO=wait|burst|sustained|lifecycle` for qualification runs so
scenario selection is explicit; profile-name inference is only a convenience
for ad-hoc smoke tests.

Results also include Kubernetes resource samples, cgroup throttling/memory
counters, PostgreSQL activity and lock samples, checkpoint/WAL counters, top
SQL, Pod state, events, and the runtime `/metrics` surface. Each profile also saves the effective Helm
values/manifest, image ID, schema contract, commit SHA, dirty-worktree patch,
Kubernetes/node identity, Run/table counts, and signal-authority latency.

Summarize a result directory:

```bash
bash bench/k8s/summarize-results.sh \
  bench/results/2026-07-25-k8s-limited
```

After an aged profile has drained to zero active Runs, capture five minutes of
idle discovery evidence. This resets statistics in the dedicated benchmark
PostgreSQL instance before the observation window:

```bash
BENCH_NAMESPACE=insight-bench-c1 BENCH_RELEASE=c1 \
  bash bench/k8s/capture-idle-discovery.sh \
  bench/results/2026-07-26-optimized/c1-aged-idle
```

Gate D fault injection helpers terminate only the durable work LISTEN backend,
or delete the exact runtime Pod after observing a claimed scheduler task:

```bash
BENCH_NAMESPACE=insight-bench-c1 BENCH_RELEASE=c1 \
  bash bench/k8s/inject-listener-fault.sh \
  bench/results/2026-07-26-optimized/c1-soak-listener-fault

BENCH_NAMESPACE=insight-bench-c1 BENCH_RELEASE=c1 \
  bash bench/k8s/inject-runtime-restart-during-claim.sh \
  bench/results/2026-07-26-optimized/c1-soak-runtime-restart
```

For a repeatable 2-hour or 24-hour Gate D, use the wrapper below. It keeps the
load at 10 arrivals/s and injects the listener fault at 20 minutes and the
claimed-task runtime restart at 45 minutes. The offsets can be changed with
`BENCH_LISTENER_FAULT_DELAY_SECONDS` and
`BENCH_RUNTIME_RESTART_DELAY_SECONDS` for a smoke test.

```bash
BENCH_NAMESPACE=insight-bench-c1 BENCH_RELEASE=c1 \
  BENCH_IMAGE_TAG=qualification-v3 \
  bash bench/k8s/run-gate-d.sh c1-rc-soak-24h 24h \
  bench/results/2026-07-26-qualification-v3/gate-d-rc-soak-10rps-24h
```

On a macOS/OrbStack benchmark host, keep the lid open and AC power connected
for the complete qualification window. `caffeinate` prevents idle sleep, but
does not override macOS `Clamshell Sleep` when the lid is closed. A host sleep
can leave both the Docker and Kubernetes control planes unavailable even
though the workload Pods had been healthy, which invalidates the fault
schedule and evidence collection. Prefer an always-on Kubernetes runner for
release-candidate evidence:

```bash
BENCH_NAMESPACE=insight-bench-c1 BENCH_RELEASE=c1 \
  BENCH_IMAGE_TAG=qualification-v3 \
  caffeinate -dimsu -- \
  bash bench/k8s/run-gate-d.sh c1-rc-soak-24h 24h \
  bench/results/2026-07-26-qualification-v3/gate-d-rc-soak-10rps-24h
```
