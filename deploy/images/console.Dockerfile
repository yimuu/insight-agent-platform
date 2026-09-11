FROM node:24.11.1-bookworm-slim@sha256:48abc13a19400ca3985071e287bd405a1d99306770eb81d61202fb6b65cf0b57
COPY dist/ /console/dist/
COPY server/config.mjs server/gateway-server.mjs server/main.mjs server/process.mjs /console/server/
USER 1000:1000
WORKDIR /console
ENTRYPOINT ["/usr/local/bin/node", "/console/server/main.mjs"]
CMD ["--config", "/config/console.json"]
LABEL org.opencontainers.image.title="Insight Agent Platform Console"
LABEL org.opencontainers.image.description="Immutable Console bundle with bounded same-origin Gateway transport; no business state or credentials"
