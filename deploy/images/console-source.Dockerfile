FROM rust:1.94-bookworm@sha256:6ae102bdbf528294bc79ad6e1fae682f6f7c2a6e6621506ba959f9685b308a55 AS compiler
WORKDIR /workspace
RUN rustup target add wasm32-unknown-unknown && cargo install --locked wasm-bindgen-cli --version 0.2.126
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY apps ./apps
COPY tools/rust ./tools/rust
COPY tests/qualification ./tests/qualification
COPY contracts ./contracts
COPY deploy ./deploy
RUN rustup target add wasm32-unknown-unknown \
    && cargo build --locked --release -p insight-platform-agent-compiler-wasm --target wasm32-unknown-unknown --no-default-features \
    && wasm-bindgen target/wasm32-unknown-unknown/release/insight_platform_agent_compiler_wasm.wasm --target web --out-dir /compiler

FROM node:24.11.1-bookworm-slim@sha256:48abc13a19400ca3985071e287bd405a1d99306770eb81d61202fb6b65cf0b57 AS frontend
RUN npm install --global pnpm@11.19.0
WORKDIR /workspace/apps/console
COPY apps/console/package.json apps/console/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY apps/console ./
COPY contracts /workspace/contracts
COPY crates/adapters/platform-postgres/schema-inventory.json /workspace/crates/adapters/platform-postgres/schema-inventory.json
COPY --from=compiler /compiler ./src/shared/compiler/generated
RUN pnpm typecheck && pnpm server:build && pnpm exec vite build
RUN pnpm prune --prod

FROM node:24.11.1-bookworm-slim@sha256:48abc13a19400ca3985071e287bd405a1d99306770eb81d61202fb6b65cf0b57
WORKDIR /console
COPY --from=frontend /workspace/apps/console/dist ./dist
COPY --from=frontend /workspace/apps/console/server-dist ./server-dist
COPY --from=frontend /workspace/apps/console/node_modules ./node_modules
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/node", "/console/server-dist/main.js"]
CMD ["--config", "/run/insight/console/config.json"]
