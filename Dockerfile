FROM rust:1.94-bullseye@sha256:f4f82b80e5f2945fed4ba17af177c6d6be85d98cde38ff318fc7666ce4505617 AS builder
WORKDIR /workspace

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY src ./src
COPY catalog ./catalog
COPY contracts ./contracts
COPY proto ./proto
COPY database ./database

RUN cargo build --locked --release --bin insight-agent-platform \
    && cargo build --locked --release -p insight-platform-callback-api --bin platform-callback-api \
    && cargo build --locked --release -p insight-platform-gateway --bin platform-gateway \
    && cargo build --locked --release -p insight-platform-model-worker --bin platform-model-worker \
    && cargo build --locked --release -p insight-platform-context-worker --bin platform-context-worker \
    && cargo build --locked --release -p insight-platform-context-worker --bin platform-remote-context-worker \
    && cargo build --locked --release -p insight-platform-orchestration-worker --bin platform-orchestration-worker \
    && cargo build --locked --release -p insight-platform-capability-worker --bin platform-capability-native-worker \
    && cargo build --locked --release -p insight-platform-capability-worker --bin platform-capability-remote-worker \
    && cargo build --locked --release -p insight-platform-mcp-cleanup-worker --bin platform-mcp-cleanup-worker \
    && cargo build --locked --release -p insight-platform-mcp-service --bin platform-mcp-host \
    && cargo build --locked --release -p insight-platform-artifact-service --bin platform-artifact-data-worker \
    && cargo build --locked --release -p insight-platform-artifact-service --bin platform-artifact-gateway \
    && cargo build --locked --release -p insight-platform-artifact-service --bin platform-artifact-maintenance \
    && cargo build --locked --release -p insight-platform-egress-broker --bin platform-egress-broker \
    && cargo build --locked --release -p insight-platform-security-authority --bin platform-security-authority \
    && cargo build --locked --release -p insight-platform-sandbox-controller --bin platform-sandbox-controller \
    && cargo build --locked --release -p insight-platform-sandbox-attestor --bin platform-sandbox-attestor \
    && cargo build --locked --release -p insight-platform-sandbox-executor --bin platform-sandbox-executor \
    && cargo build --locked --release -p insight-platform-sandbox-guest --bin platform-sandbox-guest

FROM debian:bullseye-slim@sha256:f313b4bd62667092a59b3a664d7d3ab8b5e65f41675f48e81455a15dc5abe792 AS runtime-base

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 insight \
    && useradd --uid 10001 --gid insight --create-home --home-dir /app insight

WORKDIR /app

FROM runtime-base AS runtime

COPY --from=builder /workspace/target/release/insight-agent-platform /usr/local/bin/insight-agent-platform
COPY --from=builder /workspace/target/release/platform-callback-api /usr/local/bin/platform-callback-api
COPY --from=builder /workspace/target/release/platform-gateway /usr/local/bin/platform-gateway
COPY --from=builder /workspace/target/release/platform-model-worker /usr/local/bin/platform-model-worker
COPY --from=builder /workspace/target/release/platform-context-worker /usr/local/bin/platform-context-worker
COPY --from=builder /workspace/target/release/platform-remote-context-worker /usr/local/bin/platform-remote-context-worker
COPY --from=builder /workspace/target/release/platform-orchestration-worker /usr/local/bin/platform-orchestration-worker
COPY --from=builder /workspace/target/release/platform-capability-native-worker /usr/local/bin/platform-capability-native-worker
COPY --from=builder /workspace/target/release/platform-capability-remote-worker /usr/local/bin/platform-capability-remote-worker
COPY --from=builder /workspace/target/release/platform-mcp-cleanup-worker /usr/local/bin/platform-mcp-cleanup-worker
COPY --from=builder /workspace/target/release/platform-mcp-host /usr/local/bin/platform-mcp-host
COPY --from=builder /workspace/target/release/platform-artifact-data-worker /usr/local/bin/platform-artifact-data-worker
COPY --from=builder /workspace/target/release/platform-artifact-gateway /usr/local/bin/platform-artifact-gateway
COPY --from=builder /workspace/target/release/platform-artifact-maintenance /usr/local/bin/platform-artifact-maintenance
COPY --from=builder /workspace/target/release/platform-egress-broker /usr/local/bin/platform-egress-broker
COPY --from=builder /workspace/target/release/platform-security-authority /usr/local/bin/platform-security-authority
COPY --from=builder /workspace/target/release/platform-sandbox-controller /usr/local/bin/platform-sandbox-controller
COPY --from=builder /workspace/target/release/platform-sandbox-attestor /usr/local/bin/platform-sandbox-attestor
COPY --from=builder /workspace/target/release/platform-sandbox-executor /usr/local/bin/platform-sandbox-executor
COPY agents /app/agents
COPY config /app/config
COPY database /app/database

RUN mkdir -p /data/artifacts \
    && chown -R insight:insight /app /data

USER 10001:10001

ENV PLATFORM_CONFIG=/app/config/platform.yaml
EXPOSE 3000

ENTRYPOINT ["/usr/local/bin/insight-agent-platform"]

# The gVisor RuntimeClass isolates this single-Job image. Runtime dependencies are resolved only
# while publishing this immutable image; the guest never invokes a package manager.
FROM runtime-base AS sandbox-guest

USER root
RUN apt-get update \
    && apt-get install --yes --no-install-recommends python3=3.9.2-3 nodejs=12.22.12~dfsg-1~deb11u4 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /workspace/target/release/platform-sandbox-guest /usr/local/bin/platform-sandbox-guest
RUN mkdir -p /scratch \
    && chown 65532:65532 /scratch \
    && chmod 0700 /scratch

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/platform-sandbox-guest"]
