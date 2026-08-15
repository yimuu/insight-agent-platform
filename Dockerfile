FROM --platform=$BUILDPLATFORM rust:1.94-bullseye AS builder

ARG TARGETPLATFORM
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
    && cargo build --locked --release -p insight-platform-mcp-cleanup-worker --bin platform-mcp-cleanup-worker \
    && cargo build --locked --release -p insight-platform-model-worker --bin platform-model-worker \
    && cargo build --locked --release -p insight-platform-egress-broker --bin platform-egress-broker \
    && cargo build --locked --release -p insight-platform-security-authority --bin platform-security-authority \
    && cargo build --locked --release -p insight-platform-sandbox-controller --bin platform-sandbox-controller \
    && cargo build --locked --release -p insight-platform-sandbox-attestor --bin platform-sandbox-attestor \
    && cargo build --locked --release -p insight-platform-sandbox-executor --bin platform-sandbox-executor

FROM debian:bullseye-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 insight \
    && useradd --uid 10001 --gid insight --create-home --home-dir /app insight

WORKDIR /app

COPY --from=builder /workspace/target/release/insight-agent-platform /usr/local/bin/insight-agent-platform
COPY --from=builder /workspace/target/release/platform-callback-api /usr/local/bin/platform-callback-api
COPY --from=builder /workspace/target/release/platform-mcp-cleanup-worker /usr/local/bin/platform-mcp-cleanup-worker
COPY --from=builder /workspace/target/release/platform-model-worker /usr/local/bin/platform-model-worker
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
