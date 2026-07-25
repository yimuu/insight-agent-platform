# Limited-resource Kubernetes benchmark

The Helm chart deploys one runtime Pod and one PostgreSQL 16 Pod. The benchmark
values cap each service at `500m` CPU and `256Mi` memory. PostgreSQL data is
ephemeral for repeatable local benchmarking; the artifact volume uses a PVC
because its durable authority must survive runtime Pod replacement.

Build and install:

```bash
docker build --platform linux/arm64 -t insight-agent-platform:benchmark .
helm upgrade --install bench deploy/helm/insight-agent-platform \
  --namespace insight-bench \
  --create-namespace \
  --values deploy/helm/insight-agent-platform/values-benchmark.yaml \
  --wait --timeout 5m
```

Run a lifecycle profile with Grafana k6:

```bash
bash bench/k8s/run-profile.sh smoke 1 15s
bash bench/k8s/run-profile.sh baseline 4 30s
bash bench/k8s/run-profile.sh saturation 8 45s
bash bench/k8s/run-profile.sh overload 16 45s
bash bench/k8s/run-profile.sh soak 1 2m
```

Each virtual user repeatedly creates an `action_demo` Run and polls its durable
projection until terminal. Results include k6 latency/error metrics, Kubernetes
resource samples, cgroup throttling/memory counters, PostgreSQL activity and lock
samples, top SQL, Pod state, and events.

Summarize a result directory:

```bash
bash bench/k8s/summarize-results.sh \
  bench/results/2026-07-25-k8s-limited
```
