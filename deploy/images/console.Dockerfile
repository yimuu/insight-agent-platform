FROM node:24.11.1-bookworm-slim@sha256:48abc13a19400ca3985071e287bd405a1d99306770eb81d61202fb6b65cf0b57
COPY dist/ /console/dist/
COPY server-dist/ /console/server-dist/
COPY package.json pnpm-lock.yaml /console/
WORKDIR /console
RUN npm install --global pnpm@11.19.0 && pnpm install --prod --frozen-lockfile
USER 1000:1000
WORKDIR /console
ENTRYPOINT ["/usr/local/bin/node", "/console/server-dist/main.js"]
CMD ["--config", "/config/console.json"]
LABEL org.opencontainers.image.title="Insight Agent Platform Console"
LABEL org.opencontainers.image.description="Immutable Console bundle with bounded same-origin Gateway transport; no business state or credentials"
