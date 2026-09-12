FROM --platform=$BUILDPLATFORM node:24.11.1-bookworm-slim@sha256:48abc13a19400ca3985071e287bd405a1d99306770eb81d61202fb6b65cf0b57 AS server-build
WORKDIR /build
COPY apps/console/package.json apps/console/pnpm-lock.yaml ./
RUN npm install --global pnpm@11.19.0 && pnpm install --frozen-lockfile --ignore-scripts
COPY apps/console/tsconfig.server.json ./
COPY apps/console/server/ ./server/
RUN pnpm exec tsc -p tsconfig.server.json && pnpm prune --prod --ignore-scripts

FROM node:24.11.1-bookworm-slim@sha256:48abc13a19400ca3985071e287bd405a1d99306770eb81d61202fb6b65cf0b57
COPY apps/console/dist/ /console/dist/
COPY --from=server-build /build/server-dist/config.js /build/server-dist/gateway-server.js /build/server-dist/main.js /build/server-dist/process.js /build/server-dist/identity-config.js /build/server-dist/identity-main.js /build/server-dist/identity-schema.js /build/server-dist/identity-server.js /console/server-dist/
COPY crates/adapters/platform-postgres/schema-inventory.json /console/server-dist/
COPY --from=server-build /build/node_modules/ /console/node_modules/
USER 1000:1000
WORKDIR /console
ENTRYPOINT ["/usr/local/bin/node", "/console/server-dist/main.js"]
CMD ["--config", "/config/console.json"]
LABEL org.opencontainers.image.title="Insight Agent Platform Console"
LABEL org.opencontainers.image.description="Immutable Console bundle with bounded same-origin Gateway transport and local identity service"
