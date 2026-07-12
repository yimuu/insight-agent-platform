# HTTP Stack Majors Design

Date: 2026-07-12

## Status

Design approved in conversation on 2026-07-12 for Phase 3 / dependency-governance `R5`: complete the Axum 0.8 and Reqwest 0.13 migrations in one delivery wave while keeping the server and client changes separately reviewable.

## Context

`docs/reviews/2026-07-11-dependency-governance-review.md` identified Axum 0.8 and Reqwest 0.13 as coupled HTTP-stack major upgrades that need their own route, extractor, SSE, TLS, redirect, timeout, streaming, cancellation, and redaction matrices.

Earlier dependency-governance remediation deliberately did not upgrade Axum or Reqwest. The runtime baseline now has stable Formal V1 routes, live-only SSE, explicit Attached disconnect cancellation, model transport policy, restricted HTTP action limits, and project-owned YAML/JSON Schema boundaries. Phase 3 should preserve those contracts rather than re-open architecture decisions.

Current direct versions:

- `axum = "0.7"` resolves to `0.7.9`.
- `reqwest = { version = "0.12", features = ["json", "stream", "rustls-tls"], default-features = false }` resolves to `0.12.28`.

Target direct versions:

- `axum = "0.8"` with target resolution `0.8.9`.
- `reqwest = { version = "0.13", features = ["json", "stream", "rustls"], default-features = false }` with target resolution `0.13.4`.

Local `cargo info` reports Axum 0.8.9 MSRV 1.80 and Reqwest 0.13.4 MSRV 1.85. The project toolchain baseline remains Rust 1.94.1, so these version floors are acceptable.

## Goals

- Upgrade Axum to 0.8 without changing the Formal V1 HTTP contract.
- Upgrade Reqwest to 0.13 without changing the platform's explicit transport security posture.
- Keep `default-features = false` for Reqwest and prevent implicit `default-tls`, `native-tls`, `system-proxy`, HTTP/2, compression, cookies, SOCKS, HTTP/3, or DNS feature expansion.
- Keep live-only SSE behavior: no replay route, no recovery cursor, no event-history lookup on public stream reconnects, and Attached Run cancellation when the stream disconnects.
- Keep model plaintext HTTP policy exactly as currently implemented: default HTTPS-only, explicit `loopback`, and explicit `trusted_private`.
- Keep restricted `http_get` action behavior: HTTPS only, host allowlist, no redirects, timeout/cancel aware streaming, response byte limit, and sanitized errors.
- Document every intentional interface/dependency-contract change and why it is required.

## Non-goals

- Do not reintroduce SSE recovery or a public event replay route.
- Do not change API response envelopes, event payloads, Run status semantics, repository schema, migrations, or history persistence.
- Do not change DSL syntax, node execution semantics, scheduler behavior, or resource configuration version.
- Do not add custom CA bundle configuration in this phase.
- Do not switch model HTTP policy to automatic private-network detection.
- Do not upgrade Tokio, Tower, SQLx, JSON Schema, CEL, YAML parser, SHA-2, thiserror, Handlebars, UUID, or other direct lines as part of this phase.
- Do not add cargo-audit or cargo-deny suppressions as a substitute for dependency remediation.

## Approved approach

Use one Phase 3 branch/plan and two implementation commits:

1. Axum 0.8 server migration.
2. Reqwest 0.13 client/TLS migration.

This preserves "一次性完成" at the delivery level while keeping the two risk boundaries independently reviewable. A single mixed commit would obscure whether a regression came from Axum routing/SSE behavior or Reqwest TLS/client behavior. Separate branches would be slower without materially reducing risk because the current code has narrow, well-isolated Axum and Reqwest usage.

## Axum 0.8 server contract

### Existing server usage

Axum usage is concentrated in:

- `src/api/formal/routes.rs`: router construction, state extraction, bearer middleware, JSON extraction/rejection mapping, headers, and route handlers.
- `src/api/formal/sse.rs`: `Sse`, `Event`, `KeepAlive`, event encoding, terminal stream closure, and transport-error event fallback.
- `src/api/formal/response.rs`: `IntoResponse` mapping for `ApiError`.
- `src/main.rs`: `axum::serve(listener, app).with_graceful_shutdown(...).into_future()`.
- `tests/api.rs` and `tests/action_error_containment.rs`: route, auth, SSE, cancellation, history, and response-shape integration coverage through `tower::ServiceExt::oneshot`.

### Interface impact

Public HTTP paths remain unchanged:

- `GET /health`
- `GET /v1/agents`
- `GET /v1/agents/{agent_id}`
- `POST /v1/agents/{agent_id}/runs`
- `POST /v1/agents/{agent_id}/runs/stream`
- `GET /v1/runs/{run_id}`
- `DELETE /v1/runs/{run_id}`

Internal route definitions must change from Axum 0.7-style path captures to Axum 0.8-style captures:

```rust
"/v1/agents/{agent_id}"
"/v1/agents/{agent_id}/runs/stream"
"/v1/agents/{agent_id}/runs"
"/v1/runs/{run_id}"
```

Reason: Axum 0.8 uses `{name}` path capture syntax. Keeping `/:name` would rely on 0.7 compatibility behavior and makes the upgraded route table harder to audit.

No public response shape changes are allowed. `ApiResponse<T>` keeps `code`, `message`, and `data`. Error codes and status mappings remain stable:

- malformed/invalid JSON or input schema: `400 INPUT_INVALID`
- missing/invalid bearer token: `401 UNAUTHORIZED`
- unknown agent: `404 AGENT_NOT_FOUND`
- unknown run: `404 RUN_NOT_FOUND`
- capacity/conflict/stopping: `409 RUN_CONFLICT`
- runtime unavailable: `503 RUN_SERVICE_UNAVAILABLE`
- upstream failures: `502 UPSTREAM_FAILURE`
- unclassified internal failures: `500 INTERNAL`

### SSE contract

SSE remains live-only:

- Attached stream validates request input before returning `text/event-stream`.
- Response includes `x-run-id` and `x-request-id` headers when a Run is created.
- SSE event `id` is the event sequence number.
- SSE event name is `RunEventType::as_str()`.
- SSE `data` is the serialized `RunEvent`.
- Keepalive text remains `keep-alive` and uses `runtime.sse_keep_alive_interval`.
- Terminal Run events close the stream.
- Subscriber lag or subscription closure may emit one `transport.error` event if the connection is still writable.
- `transport.error` must not include `after_seq`, reconnect instructions, or recovery metadata.
- Dropping an Attached SSE body cancels the Attached Run.
- `/v1/runs/{run_id}/events`, `after_seq`, and `Last-Event-ID` remain unsupported.

### Axum verification matrix

Focused verification must cover:

- route matching for every public Formal V1 route with the new `{param}` syntax;
- public `/health` and protected `/v1/*` auth behavior;
- JSON extraction rejection still maps to `INPUT_INVALID` and not SSE;
- response headers `x-run-id` and `x-request-id`;
- SSE content type, ordered `seq`, terminal close, and keepalive;
- Attached disconnect cancellation;
- no replay route and no recovery header behavior;
- service startup/graceful shutdown compilation with Axum 0.8.

## Reqwest 0.13 client/TLS contract

### Existing client usage

Reqwest usage is concentrated in:

- `src/resources/openai_chat.rs`: OpenAI-compatible chat client, `Client::builder`, URL parsing, redirect blocking, connect/request timeouts, optional bearer auth, streaming SSE byte decoding, upstream byte limits, sanitized transport/status/stream errors, and model transport policy validation.
- `src/resources/builtin_actions.rs`: restricted `http_get` action client, HTTPS URL validation, allowlist enforcement, redirect blocking, timeout/cancel aware response streaming, response byte limit, and sanitized HTTP errors.
- `src/resources/config.rs`: model resource YAML parsing and mapping `transport.plaintext_http` into `OpenAiTransportPolicy`.
- `tests/formal_resources.rs`, `tests/model_resources_v1.rs`, `tests/core_chat_action.rs`, and `tests/observability.rs`: transport policy, loopback HTTP tests, redirects, streaming, body limits, logs, and redaction.

### Dependency feature impact

Reqwest must be upgraded with this direct dependency shape:

```toml
reqwest = { version = "0.13", features = ["json", "stream", "rustls"], default-features = false }
```

Reason: Reqwest 0.13 changed TLS features. The old `rustls-tls` feature is not the correct 0.13 contract. The 0.13 default feature set includes `default-tls`, `http2`, and `system-proxy`, which would broaden behavior beyond the current platform contract. Therefore this phase keeps default features disabled and opts in only to JSON, streaming, and rustls.

The implementation should call `tls_backend_rustls()` on every project-built Reqwest client.

Reason: Reqwest documents that `default-tls` currently uses rustls but is intentionally backend-abstract. The platform should not rely on an implicit backend alias for security-sensitive model and action clients.

Do not enable:

- `default-tls`
- `native-tls`
- `native-tls-*`
- `system-proxy`
- `http2`
- `http3`
- `gzip`, `brotli`, `deflate`, or `zstd`
- `cookies`
- `socks`
- `hickory-dns`
- `multipart`
- `blocking`

### TLS trust-root decision

Use Reqwest 0.13 `rustls` with its platform verifier path for HTTPS verification. Do not add custom CA configuration in this phase.

Reason:

- Reqwest 0.13 no longer presents the old `rustls-tls-webpki-roots` direct feature as the project contract.
- The platform already allows `transport.plaintext_http: trusted_private` for model services deployed inside a trusted private network. For HTTPS private model services, platform trust stores are the pragmatic baseline because enterprise/private CA roots can be installed at the host/container layer.
- Adding project-level CA bundle config is a separate interface decision and should not be hidden inside the major upgrade.

Operational impact:

- Public HTTPS providers must chain to a root trusted by the runtime environment.
- Private HTTPS providers may work if their CA is installed in the runtime environment's trust store.
- Images/hosts without a usable CA store may fail HTTPS client construction or requests earlier than before; that is acceptable and should be documented if observed by tests.
- If future deployments need per-model CA pinning or isolated root bundles, add an explicit model TLS config in a later phase.

### Model plaintext HTTP policy

Do not change the current model policy:

- HTTPS is accepted by default.
- HTTP is rejected by default.
- `transport.plaintext_http: loopback` accepts only exact raw hosts `localhost`, `127.0.0.1`, and `[::1]`.
- `transport.plaintext_http: trusted_private` accepts HTTP because the deployment owner has explicitly accepted a trusted private-network plaintext hop.
- URL username/password remains rejected for every policy.

This is intentionally not automatic private-IP detection. DNS and network topology are deployment-specific, and false positives would be worse than explicit operator intent.

### Reqwest behavior contract

Both OpenAI-compatible model clients and restricted HTTP action clients must preserve:

- `redirect(Policy::none())`;
- configured connect/request timeout behavior;
- streaming through `bytes_stream()`;
- response byte limits and line/payload/chunk/usage limits for chat streaming;
- cancel/stop aware streaming for actions;
- no upstream body/header/token/message leakage in errors or debug output;
- existing error-code boundaries:
  - model transport: `UPSTREAM_TRANSPORT`
  - model non-success status: `UPSTREAM_STATUS`
  - model stream failure: `UPSTREAM_STREAM`
  - model invalid stream payload: `UPSTREAM_STREAM_INVALID`
  - model/action oversized response: existing too-large codes
  - action blocked URL/redirect: `ACTION_HTTP_BLOCKED`
  - action transport failure: `ACTION_HTTP_FAILED`
  - action oversized response: `ACTION_HTTP_TOO_LARGE`

### Reqwest verification matrix

Focused verification must cover:

- model config rejects plaintext HTTP by default and preserves secret redaction;
- model config accepts explicit `loopback` and `trusted_private` HTTP;
- non-exact loopback aliases are rejected under `loopback`;
- URL userinfo is rejected;
- OpenAI-compatible streaming still parses data lines and `[DONE]`;
- upstream non-success status maps to sanitized `UPSTREAM_STATUS`;
- malformed upstream stream maps to sanitized `UPSTREAM_STREAM_INVALID`;
- oversized upstream response maps to the existing too-large error;
- upstream redirects are not followed;
- restricted `http_get` remains HTTPS-only and allowlist-bound;
- restricted `http_get` blocks redirects and enforces byte limits;
- restricted `http_get` remains cancellation/timeout aware while streaming;
- dependency tree does not include `native-tls`, `hyper-tls`, `tokio-native-tls`, `system-proxy`, or Reqwest default TLS features through the direct Reqwest line.

## Documentation impact

`docs/formal-v1-breaking-changes.md` should receive a small dependency-contract note, not a new product-level API breaking change.

Required content:

- Axum route definitions now use `{param}` syntax internally because Axum 0.8 treats it as the canonical path-capture syntax.
- Reqwest 0.13 replaces the old direct `rustls-tls` feature selection with explicit `rustls` and `tls_backend_rustls()`.
- Reqwest default features remain disabled; operators should not expect automatic system proxy behavior.
- HTTPS verification uses the runtime environment's trusted root store through Reqwest's rustls/platform verifier path.
- Existing `transport.plaintext_http` semantics remain unchanged.

README changes are optional unless tests or dependency behavior expose a concrete operator-facing note that is not already documented.

## Rollback

Rollback is straightforward because no repository schema or public API shape changes are allowed:

1. Revert the Reqwest commit to restore `reqwest = "0.12"` with `["json", "stream", "rustls-tls"]`.
2. Revert the Axum commit to restore `axum = "0.7"` and `/:param` route definitions.
3. Re-run the API and resource/client verification commands.

Do not partially rollback by keeping Reqwest 0.13 while removing explicit rustls backend selection, and do not keep Axum 0.8 with legacy route syntax.

## External references

- Axum repository and changelog: <https://github.com/tokio-rs/axum>
- Axum Router documentation: <https://docs.rs/axum/latest/axum/struct.Router.html>
- Reqwest repository and changelog: <https://github.com/seanmonstar/reqwest/blob/master/CHANGELOG.md>
- Reqwest 0.13 release notes: <https://github.com/seanmonstar/reqwest/releases>
- Reqwest feature list: <https://docs.rs/crate/reqwest/latest/features>
- Reqwest TLS documentation: <https://docs.rs/reqwest/latest/reqwest/tls/index.html>
