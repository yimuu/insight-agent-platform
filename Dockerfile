FROM --platform=$BUILDPLATFORM rust:1.94-bullseye AS builder

ARG TARGETPLATFORM
WORKDIR /workspace

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY src ./src
COPY catalog ./catalog

RUN cargo build --locked --release --bin insight-agent-platform

FROM debian:bullseye-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 insight \
    && useradd --uid 10001 --gid insight --create-home --home-dir /app insight

WORKDIR /app

COPY --from=builder /workspace/target/release/insight-agent-platform /usr/local/bin/insight-agent-platform
COPY agents /app/agents
COPY config /app/config
COPY database /app/database

RUN mkdir -p /data/artifacts \
    && chown -R insight:insight /app /data

USER 10001:10001

ENV PLATFORM_CONFIG=/app/config/platform.yaml
EXPOSE 3000

ENTRYPOINT ["/usr/local/bin/insight-agent-platform"]
