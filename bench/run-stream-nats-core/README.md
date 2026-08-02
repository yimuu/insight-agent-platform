# Core NATS Run Stream qualification

This directory holds the reproducible release gates for the transient
`nats_core` Run Stream backend. It never treats NATS as durable authority and
does not enable JetStream.

Run the static Helm gates:

```bash
bash bench/run-stream-nats-core/helm-static.sh
```

Run a short real-server smoke profile:

```bash
bash bench/run-stream-nats-core/run-real-nats.sh
```

Run the release profile (20 x 50 bursts, a 30-minute mixed workload, and a
2-hour soak):

```bash
QUALIFICATION_PROFILE=full \
  bash bench/run-stream-nats-core/run-real-nats.sh \
  bench/results/run-stream-nats-core-release
```

The harness pins `nats:2.12.4-alpine` and `natsio/nats-box:0.18.0`, creates
ephemeral credentials and CAs under a private temporary directory, injects a
NATS server restart in every workload, and removes credentials and containers
on exit. Evidence includes only public server metrics, toolchain/image
versions, closed platform metrics, and test output; credential bodies are not
captured.

The PostgreSQL regression runs 50 real Attached SSE connections with a
10-connection runtime pool and verifies terminal/EOF/GET/canonical snapshot
hash equality. Build the qualification image once, then execute both backends:

```bash
docker build --platform linux/arm64 \
  -t insight-agent-platform:run-stream-nats-core-qualification .
bash bench/run-stream-nats-core/run-k8s-database-regression.sh in_memory
bash bench/run-stream-nats-core/run-k8s-database-regression.sh nats_core
```

Each run uses a unique ephemeral Kubernetes namespace. The NATS profile
creates a TLS/credentials/subject-ACL Core NATS server, checks that the server
sees one Runtime data connection at the 50-stream peak, and removes all
qualification Secrets and resources on exit.

The probe issues 50 concurrent `GET Run` requests while all SSE subscriptions
are active. Every request traverses the bounded Runtime PostgreSQL pool, so its
end-to-end p95 is a conservative upper bound for pool-acquire p95. The statement
report can still contain the durable scheduler's body-free `pg_notify` wakeup;
the Run Stream gate is zero Run Stream-specific listener connections, source
matches, and per-frame notification SQL.
