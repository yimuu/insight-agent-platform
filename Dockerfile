FROM rust:1.94-bullseye@sha256:f4f82b80e5f2945fed4ba17af177c6d6be85d98cde38ff318fc7666ce4505617 AS chef
RUN cargo install --locked --version 0.1.78 cargo-chef
WORKDIR /workspace

FROM chef AS planner
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY contracts ./contracts
COPY deploy ./deploy
COPY release ./release
COPY proto ./proto
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /workspace/recipe.json recipe.json
# RPC build scripts consume checked-in protobuf sources while Cargo Chef compiles the stable
# dependency skeleton. This layer remains reusable when ordinary Rust source files change.
COPY proto ./proto
RUN cargo chef cook --locked --release --workspace --recipe-path recipe.json \
    --bin insight \
    --bin platform-schema \
    --bin platform-dev-bootstrap \
    --bin platform-callback-api \
    --bin platform-gateway \
    --bin platform-model-worker \
    --bin platform-context-worker \
    --bin platform-context-dataset-worker \
    --bin platform-remote-context-worker \
    --bin platform-subscription-context-worker \
    --bin platform-orchestration-worker \
    --bin platform-registry-validation-worker \
    --bin platform-capability-native-worker \
    --bin platform-capability-remote-worker \
    --bin platform-mcp-cleanup-worker \
    --bin platform-mcp-host \
    --bin platform-mcp-resource-host \
    --bin platform-mcp-discovery-worker \
    --bin platform-mcp-subscription-worker \
    --bin platform-artifact-data-worker \
    --bin platform-artifact-gateway \
    --bin platform-artifact-maintenance \
    --bin platform-egress-broker \
    --bin platform-security-authority \
    --bin platform-sandbox-dispatcher

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY contracts ./contracts
COPY deploy ./deploy
COPY release ./release

RUN cargo build --locked --release --workspace \
    --bin insight \
    --bin platform-schema \
    --bin platform-dev-bootstrap \
    --bin platform-callback-api \
    --bin platform-gateway \
    --bin platform-model-worker \
    --bin platform-context-worker \
    --bin platform-context-dataset-worker \
    --bin platform-remote-context-worker \
    --bin platform-subscription-context-worker \
    --bin platform-orchestration-worker \
    --bin platform-registry-validation-worker \
    --bin platform-capability-native-worker \
    --bin platform-capability-remote-worker \
    --bin platform-mcp-cleanup-worker \
    --bin platform-mcp-host \
    --bin platform-mcp-resource-host \
    --bin platform-mcp-discovery-worker \
    --bin platform-mcp-subscription-worker \
    --bin platform-artifact-data-worker \
    --bin platform-artifact-gateway \
    --bin platform-artifact-maintenance \
    --bin platform-egress-broker \
    --bin platform-security-authority \
    --bin platform-sandbox-dispatcher

# The capability handoff enters the Rust runner before any runtime thread exists. Both the
# credential-scrubbing launcher and the runner core are static so package layers cannot interpose a
# loader or constructor while either process holds the runner capability set.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends musl-tools binutils \
    && rm -rf /var/lib/apt/lists/*
RUN runner_target="$(rustc -vV | sed -n 's/^host: //p' | sed 's/-gnu$/-musl/')" \
    && rustup target add "$runner_target" \
    && CC=musl-gcc CXX=musl-gcc AR=ar \
       RUSTFLAGS='-C target-feature=+crt-static' \
       cargo build --locked --release --target "$runner_target" \
         -p insight-platform-sandbox-runner --bin platform-sandbox-runner \
    && cp "target/$runner_target/release/platform-sandbox-runner" \
         /workspace/platform-sandbox-runner-core
RUN musl-gcc -std=c11 -O2 -Wall -Wextra -Werror -fPIE -static -pie \
      -Wl,-z,relro,-z,now,-z,noexecstack \
      -o /workspace/platform-sandbox-launcher \
      crates/platform-sandbox-runner/native/launcher.c
RUN ! readelf -l /workspace/platform-sandbox-runner-core | grep -q INTERP
RUN ! readelf -d /workspace/platform-sandbox-runner-core | grep -q NEEDED
RUN ! readelf -l /workspace/platform-sandbox-launcher | grep -q INTERP
RUN ! readelf -d /workspace/platform-sandbox-launcher | grep -q NEEDED

FROM debian:bullseye-slim@sha256:f313b4bd62667092a59b3a664d7d3ab8b5e65f41675f48e81455a15dc5abe792 AS runtime-base

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 insight \
    && useradd --uid 10001 --gid insight --create-home --home-dir /app insight

WORKDIR /app

FROM runtime-base AS runtime

COPY --from=builder /workspace/target/release/insight /usr/local/bin/insight
COPY --from=builder /workspace/target/release/platform-schema /usr/local/bin/platform-schema
COPY --from=builder /workspace/target/release/platform-dev-bootstrap /usr/local/bin/platform-dev-bootstrap
COPY --from=builder /workspace/target/release/platform-callback-api /usr/local/bin/platform-callback-api
COPY --from=builder /workspace/target/release/platform-gateway /usr/local/bin/platform-gateway
COPY --from=builder /workspace/target/release/platform-model-worker /usr/local/bin/platform-model-worker
COPY --from=builder /workspace/target/release/platform-context-worker /usr/local/bin/platform-context-worker
COPY --from=builder /workspace/target/release/platform-context-dataset-worker /usr/local/bin/platform-context-dataset-worker
COPY --from=builder /workspace/target/release/platform-remote-context-worker /usr/local/bin/platform-remote-context-worker
COPY --from=builder /workspace/target/release/platform-subscription-context-worker /usr/local/bin/platform-subscription-context-worker
COPY --from=builder /workspace/target/release/platform-orchestration-worker /usr/local/bin/platform-orchestration-worker
COPY --from=builder /workspace/target/release/platform-registry-validation-worker /usr/local/bin/platform-registry-validation-worker
COPY --from=builder /workspace/target/release/platform-capability-native-worker /usr/local/bin/platform-capability-native-worker
COPY --from=builder /workspace/target/release/platform-capability-remote-worker /usr/local/bin/platform-capability-remote-worker
COPY --from=builder /workspace/target/release/platform-mcp-cleanup-worker /usr/local/bin/platform-mcp-cleanup-worker
COPY --from=builder /workspace/target/release/platform-mcp-host /usr/local/bin/platform-mcp-host
COPY --from=builder /workspace/target/release/platform-mcp-resource-host /usr/local/bin/platform-mcp-resource-host
COPY --from=builder /workspace/target/release/platform-mcp-discovery-worker /usr/local/bin/platform-mcp-discovery-worker
COPY --from=builder /workspace/target/release/platform-mcp-subscription-worker /usr/local/bin/platform-mcp-subscription-worker
COPY --from=builder /workspace/target/release/platform-artifact-data-worker /usr/local/bin/platform-artifact-data-worker
COPY --from=builder /workspace/target/release/platform-artifact-gateway /usr/local/bin/platform-artifact-gateway
COPY --from=builder /workspace/target/release/platform-artifact-maintenance /usr/local/bin/platform-artifact-maintenance
COPY --from=builder /workspace/target/release/platform-egress-broker /usr/local/bin/platform-egress-broker
COPY --from=builder /workspace/target/release/platform-security-authority /usr/local/bin/platform-security-authority
COPY --from=builder /workspace/target/release/platform-sandbox-dispatcher /usr/local/bin/platform-sandbox-dispatcher

USER 10001:10001

EXPOSE 3000

ENTRYPOINT ["/usr/local/bin/platform-gateway"]

# Published Sandbox Package images derive from this target, add only their immutable package
# payload, and retain this exact entrypoint. OpenSandbox execd is PID 1 and supervises the static
# launcher/core pair; the core remains inert until the Dispatcher sends its one-shot activation.
FROM runtime-base AS sandbox-runner

USER root
RUN apt-get update \
    && apt-get install --yes --no-install-recommends libcap2-bin \
    && rm -rf /var/lib/apt/lists/* \
    && find / -xdev -perm /6000 -exec chmod a-s '{}' +
COPY --from=builder --chown=0:0 /workspace/platform-sandbox-launcher /usr/local/bin/platform-sandbox-runner
COPY --from=builder --chown=0:0 /workspace/platform-sandbox-runner-core /usr/local/libexec/platform-sandbox-runner-core
RUN chmod 0555 /usr/local/bin/platform-sandbox-runner \
    /usr/local/libexec/platform-sandbox-runner-core \
    && install -d -o 0 -g 0 -m 0755 /opt/insight \
    && setcap cap_kill,cap_setgid,cap_setuid=ep /usr/local/bin/platform-sandbox-runner \
    && test "$(getcap /usr/local/bin/platform-sandbox-runner)" = \
       '/usr/local/bin/platform-sandbox-runner cap_kill,cap_setgid,cap_setuid=ep' \
    && test -z "$(find / -xdev -perm /6000 -print -quit)"
USER 65532:65532
EXPOSE 18080
ENTRYPOINT ["/usr/local/bin/platform-sandbox-runner"]

# Source-mode qualification derives its Package image from the exact runner stage in this build
# graph. The released Package flow uses the separately verified, digest-pinned runner image.
FROM builder AS sandbox-l3-package-builder
COPY tests/fixtures/platform-sandbox-l3-package.rs /workspace/platform-sandbox-l3-package.rs
RUN rustc --edition=2021 -C opt-level=2 -C strip=symbols \
      -o /workspace/platform-sandbox-l3-package \
      /workspace/platform-sandbox-l3-package.rs

FROM sandbox-runner AS sandbox-l3-package
COPY --from=sandbox-l3-package-builder --chown=65533:65532 \
  /workspace/platform-sandbox-l3-package /opt/insight/package
