# A8 — Contract and transport decisions design

Date: 2026-07-12

## Status

Design approved in conversation on 2026-07-12; revised after written-spec feedback to support explicitly trusted private HTTP model deployments.

## Context

The stable-baseline review groups three remaining contract issues into A8:

1. `BASE-P1-012`: `open_ai_chat.base_url` accepts remote plaintext HTTP and can send prompts, model outputs, and bearer credentials over an unprotected hop.
2. `BASE-P3-004`: `DurationSpec` delegates to `humantime`, so public Agent YAML accepts fractions, aliases, compound durations, and larger units that are outside Formal V1.
3. `BASE-P3-014`: top-level documentation must not imply reconnectable public SSE now that Formal V1 exposes live-only Attached SSE and Detached polling.

This milestone intentionally makes two breaking contract decisions. Existing implicit HTTP model configurations must either move to HTTPS or explicitly declare the accepted plaintext HTTP trust boundary. Existing out-of-contract duration spellings must be rewritten to the documented grammar. The repository does not need backward compatibility for those invalid or unsafe forms, but it must explain the interface changes and keep the stable outer error boundaries.

## Goals

- Make HTTPS the default protected transport for OpenAI-compatible model resources.
- Allow plaintext HTTP only through an explicit per-model transport policy.
- Support two explicit plaintext HTTP scopes: loopback development endpoints and trusted private-network model services.
- Prevent API keys, prompts, and model bodies from being sent over unapproved HTTP.
- Replace `humantime` duration parsing at the DSL boundary with the Formal V1 grammar: one positive base-10 integer followed immediately by `ms`, `s`, or `m`.
- Keep public error codes stable: model transport policy failures use `MODEL_CONFIG_INVALID`; duration parse failures remain surfaced as `DSL_YAML_INVALID`.
- Align README and migration notes so public SSE is described as live-only, with Detached polling as the durable client recovery path.

## Non-goals

- Changing provider response memory bounds; A1 already owns those limits.
- Adding custom CA roots, certificate pinning, disabled certificate validation, or a TLS configuration surface.
- Adding DNS resolution or network probing to decide whether a hostname is private, internal, or eventually resolves to loopback.
- Proving that `trusted_private` endpoints are actually private at runtime. That is a deployment/network trust assertion made by the operator.
- Changing Restricted HTTP Action policy.
- Changing event persistence, internal recovery, SSE envelope fields, or adding public replay endpoints.
- Removing `humantime` from the dependency graph if other code still uses it. A8 only stops using it for public Agent timeout parsing.

## OpenAI-compatible model transport contract

### Default policy

`OpenAiChatModel` accepts only `https://` endpoints by default. It continues to require a valid URL with a host and continues to strip query and fragment components before constructing the `/chat/completions` endpoint. Model base URLs must not contain username or password userinfo; credentials belong in environment-backed `api_key_env`, not in URLs. Redirects remain disabled.

If a model resource uses `http://` without explicit plaintext HTTP opt-in, model loading fails with:

- code: `MODEL_CONFIG_INVALID`
- message: `OpenAI base URL must use HTTPS unless plaintext HTTP is explicitly allowed`

The diagnostic must not include the full URL, query string, API key, headers, or prompt/body values.

### Plaintext HTTP policy

Model YAML may opt into plaintext HTTP per `open_ai_chat` resource:

```yaml
models:
  internal_model:
    type: open_ai_chat
    base_url: http://model-service.internal:8080/v1
    model: example-chat
    transport:
      plaintext_http: trusted_private
```

`transport.plaintext_http` defaults to `disabled`. Supported values are:

| Value | HTTP behavior | Intended use |
|---|---|---|
| `disabled` | rejects all `http://` URLs | default and internet-facing deployments |
| `loopback` | allows only exact loopback hosts | local tests and local development |
| `trusted_private` | allows non-loopback HTTP hosts | explicitly accepted private-network model service links |

`loopback` permits HTTP only for exact loopback hosts:

- `127.0.0.1`
- `[::1]`
- `localhost`

The exception allows any explicit or implicit port on those exact hosts. It does not allow other `127.0.0.0/8` addresses, wildcard addresses, private network addresses, DNS names that resolve to loopback, URL username/password userinfo, remote hosts, or non-HTTP schemes. DNS is deliberately not consulted; configuration safety must be decidable from the URL string at startup.

`trusted_private` allows HTTP for any syntactically valid URL with a host and without URL username/password userinfo. It is not a cryptographic security mechanism and does not prove the endpoint is private. It is an explicit operator assertion that the model service is reachable only across a trusted private boundary such as a VPC, Kubernetes cluster network, service mesh, private datacenter network, or dedicated private link. Because private topology is deployment-specific, A8 does not try to classify RFC1918 ranges, `.internal` names, Kubernetes DNS names, or DNS results.

This design fixes the unsafe default without blocking legitimate internal model deployments. The security property is: plaintext HTTP cannot happen accidentally; it must be declared in configuration and reviewed as part of deployment.

### Rust construction interface

Existing constructors keep their names but become HTTPS-only:

- `OpenAiChatModel::new(...)`
- `OpenAiChatModel::new_with_limits(...)`

Tests and configuration code that intentionally use loopback HTTP must call a new explicit constructor that includes the transport policy:

```rust
OpenAiChatModel::new_with_limits_and_transport_policy(
    api_key,
    base_url,
    model,
    capabilities,
    connect_timeout,
    request_timeout,
    limits,
    OpenAiTransportPolicy::AllowLoopbackHttp,
)
```

Trusted private HTTP uses the same explicit constructor with `OpenAiTransportPolicy::AllowTrustedPrivateHttp`.

The interface change is intentional: tests, local-development loading, and trusted-private loading must spell out the unsafe transport exception at the call site, while the shorter constructors remain safe defaults. This prevents accidental HTTP from entering through helper functions that were written before the transport decision.

## Duration grammar contract

Formal V1 Agent YAML timeout values use this exact grammar:

```text
duration := positive_integer unit
positive_integer := [1-9][0-9]*
unit := "ms" | "s" | "m"
```

There is no whitespace, sign, fraction, compound duration, alias, uppercase unit, or larger unit. Examples:

| Value | Result |
|---|---|
| `1ms` | accepted |
| `250ms` | accepted |
| `5s` | accepted |
| `2m` | accepted |
| `0s` | rejected |
| `01s` | rejected |
| `1.5s` | rejected |
| `1 sec` | rejected |
| `1s 500ms` | rejected |
| `1h` | rejected |
| `+5s` | rejected |
| `5S` | rejected |

Parsing multiplies into milliseconds with checked arithmetic. Overflow fails parsing and surfaces through `parse_raw_agent` as `DSL_YAML_INVALID`.

`DurationSpec` serialization emits the same narrow grammar. Because the type owns a `Duration`, not the original token, serialization is canonical:

1. minutes when the duration is exactly divisible by 60 seconds;
2. seconds when exactly divisible by 1 second;
3. milliseconds otherwise.

`DurationSpec` values are only created by the parser, so serialized values are always positive and millisecond-precision.

## Documentation contract

README opening and API sections must describe:

- Attached SSE is live-only.
- Attached disconnect cancels the active Attached Run.
- Terminal events close SSE immediately after delivery.
- Detached Run creation is independent of SSE; clients use `GET /v1/runs/{run_id}` polling for durable recovery.
- `GET /v1/runs/{run_id}/events`, `after_seq`, and `Last-Event-ID` are not supported public recovery mechanisms.

The docs may still mention event sequence numbers for ordering, internal recovery, or audit correlation, but not as a reconnect or replay cursor.

`docs/formal-v1-breaking-changes.md` must document both A8 breaking changes:

1. implicit HTTP model endpoints must move to HTTPS or declare `transport.plaintext_http` as `loopback` or `trusted_private`;
2. timeout spellings must use the positive-integer `ms|s|m` grammar.

No database migration or history reset is required.

## Testing strategy

### Transport policy

Add tests around `OpenAiChatModel` construction and model-resource loading:

- HTTPS endpoint succeeds by default.
- Remote HTTP fails by default.
- Remote HTTP succeeds only with `transport.plaintext_http: trusted_private`.
- `http://127.0.0.1`, `http://localhost`, and `http://[::1]` succeed with `transport.plaintext_http: loopback`.
- Loopback HTTP also succeeds with `trusted_private`, but tests should prefer `loopback` when the fixture is local because it documents the narrower intent.
- Loopback HTTP fails without opt-in.
- Non-loopback HTTP fails with `loopback`.
- Unsupported schemes and missing hosts still fail.
- URL username/password userinfo fails for HTTPS, loopback HTTP, and trusted-private HTTP.
- Query strings and API keys are absent from errors and `Debug`.
- Existing loopback streaming tests opt into loopback HTTP explicitly and continue proving no Authorization header is sent over unapproved HTTP because unapproved HTTP cannot construct a model.

### Duration grammar

Extend DSL parser tests:

- accept representative `ms`, `s`, and `m` values;
- reject zero, leading-zero, signed, fractional, whitespace, alias, compound, hour/day, uppercase, and overflow values;
- assert failures keep outer code `DSL_YAML_INVALID`;
- assert serialization emits the canonical Formal V1 grammar.

### Documentation search

Search for reconnect/replay/cursor/after_seq/Last-Event-ID language in README and formal migration docs. Every remaining occurrence must be one of:

- an explicitly unsupported public API statement;
- internal repository/event-history behavior;
- historical migration rationale.

## Rollout and rollback

A8 is source-compatible for public HTTP and repository shapes but intentionally breaks unsafe or out-of-contract configuration:

- Model `http://` endpoints fail at model loading unless they declare `transport.plaintext_http`.
- Internet-facing or otherwise untrusted model endpoints must use HTTPS.
- Local plaintext development endpoints should use `transport.plaintext_http: loopback` and `127.0.0.1`, `[::1]`, or `localhost`.
- Private-network plaintext model endpoints may use `transport.plaintext_http: trusted_private` when the deployment owner accepts that trust boundary.
- Agent YAML timeout values accepted only by `humantime` must be rewritten to Formal V1 values, for example `1 sec` to `1s`, `90 seconds` to `90s`, and `1h` to `60m`.

Rollback restores the previous broad parser and remote HTTP acceptance. No data migration rollback is needed.

## Acceptance criteria

1. `OpenAiChatModel::new(...)` and `new_with_limits(...)` reject every HTTP URL by default.
2. A new explicit transport-policy constructor is the only way for tests/config loading to enable plaintext HTTP.
3. Config loading supports `transport.plaintext_http` values `disabled`, `loopback`, and `trusted_private`; the default is `disabled`.
4. `loopback` HTTP is restricted to exact `127.0.0.1`, `[::1]`, and `localhost` hosts.
5. `trusted_private` HTTP allows non-loopback HTTP only as explicit operator risk acceptance and does not attempt DNS/IP private-network classification.
6. Transport errors and `Debug` never expose API keys, bearer tokens, full URLs with query, headers, prompts, or model bodies.
7. URL username/password userinfo fails for HTTPS, loopback HTTP, and trusted-private HTTP.
8. Existing OpenAI loopback tests use explicit loopback opt-in and still pass.
9. Agent timeout parsing accepts only the Formal V1 positive-integer `ms|s|m` grammar.
10. Duration parse failures keep the public outer code `DSL_YAML_INVALID`.
11. Duration serialization emits only the Formal V1 grammar.
12. README and breaking-change docs describe live-only SSE and the two A8 migration requirements.
13. No API response shape, event envelope, repository schema, or migration file changes are introduced.
