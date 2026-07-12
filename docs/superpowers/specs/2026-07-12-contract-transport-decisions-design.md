# A8 — Contract and transport decisions design

Date: 2026-07-12

## Status

Design approved in conversation on 2026-07-12; written-spec review pending.

## Context

The stable-baseline review groups three remaining contract issues into A8:

1. `BASE-P1-012`: `open_ai_chat.base_url` accepts remote plaintext HTTP and can send prompts, model outputs, and bearer credentials over an unprotected hop.
2. `BASE-P3-004`: `DurationSpec` delegates to `humantime`, so public Agent YAML accepts fractions, aliases, compound durations, and larger units that are outside Formal V1.
3. `BASE-P3-014`: top-level documentation must not imply reconnectable public SSE now that Formal V1 exposes live-only Attached SSE and Detached polling.

This milestone intentionally makes two breaking contract decisions. Existing remote HTTP model configurations must move to HTTPS. Existing out-of-contract duration spellings must be rewritten to the documented grammar. The repository does not need backward compatibility for those invalid or unsafe forms, but it must explain the interface changes and keep the stable outer error boundaries.

## Goals

- Make HTTPS the default and only production transport for OpenAI-compatible model resources.
- Allow local plaintext HTTP only through an explicit development option, restricted to exact loopback hosts.
- Prevent API keys, prompts, and model bodies from being sent over unapproved HTTP.
- Replace `humantime` duration parsing at the DSL boundary with the Formal V1 grammar: one positive base-10 integer followed immediately by `ms`, `s`, or `m`.
- Keep public error codes stable: model transport policy failures use `MODEL_CONFIG_INVALID`; duration parse failures remain surfaced as `DSL_YAML_INVALID`.
- Align README and migration notes so public SSE is described as live-only, with Detached polling as the durable client recovery path.

## Non-goals

- Changing provider response memory bounds; A1 already owns those limits.
- Adding custom CA roots, certificate pinning, disabled certificate validation, or a TLS configuration surface.
- Adding DNS resolution to decide whether a hostname eventually resolves to loopback.
- Changing Restricted HTTP Action policy.
- Changing event persistence, internal recovery, SSE envelope fields, or adding public replay endpoints.
- Removing `humantime` from the dependency graph if other code still uses it. A8 only stops using it for public Agent timeout parsing.

## OpenAI-compatible model transport contract

### Default policy

`OpenAiChatModel` accepts only `https://` endpoints by default. It continues to require a valid URL with a host and continues to strip query and fragment components before constructing the `/chat/completions` endpoint. Model base URLs must not contain username or password userinfo; credentials belong in environment-backed `api_key_env`, not in URLs. Redirects remain disabled.

If a model resource uses `http://` without explicit development opt-in, model loading fails with:

- code: `MODEL_CONFIG_INVALID`
- message: `OpenAI base URL must use HTTPS unless loopback HTTP is explicitly enabled for development`

The diagnostic must not include the full URL, query string, API key, headers, or prompt/body values.

### Development loopback exception

Model YAML may opt into local development HTTP per `open_ai_chat` resource:

```yaml
models:
  local_dev:
    type: open_ai_chat
    base_url: http://127.0.0.1:8080/v1
    model: local-model
    allow_loopback_http: true
```

`allow_loopback_http` defaults to `false`. When it is `true`, HTTP is permitted only for exact loopback hosts:

- `127.0.0.1`
- `[::1]`
- `localhost`

The exception allows any explicit or implicit port on those exact hosts. It does not allow other `127.0.0.0/8` addresses, wildcard addresses, private network addresses, DNS names that resolve to loopback, URL username/password userinfo, remote hosts, or non-HTTP schemes. DNS is deliberately not consulted; configuration safety must be decidable from the URL string at startup.

Remote HTTP remains rejected even when `allow_loopback_http: true`.

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

The interface change is intentional: tests and local-development loading must spell out the unsafe transport exception at the call site, while the shorter constructors remain safe defaults. This prevents accidental remote HTTP from entering through helper functions that were written before the transport decision.

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

1. remote HTTP model endpoints must move to HTTPS or use exact loopback development opt-in;
2. timeout spellings must use the positive-integer `ms|s|m` grammar.

No database migration or history reset is required.

## Testing strategy

### Transport policy

Add tests around `OpenAiChatModel` construction and model-resource loading:

- HTTPS endpoint succeeds by default.
- Remote HTTP fails by default.
- Remote HTTP fails even with `allow_loopback_http`.
- `http://127.0.0.1`, `http://localhost`, and `http://[::1]` succeed only with `allow_loopback_http`.
- Loopback HTTP fails without opt-in.
- Unsupported schemes and missing hosts still fail.
- URL username/password userinfo fails for both HTTPS and loopback HTTP.
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

- Remote `http://` model endpoints fail at model loading. Operators must use HTTPS.
- Local plaintext development endpoints must add `allow_loopback_http: true` and use `127.0.0.1`, `[::1]`, or `localhost`.
- Agent YAML timeout values accepted only by `humantime` must be rewritten to Formal V1 values, for example `1 sec` to `1s`, `90 seconds` to `90s`, and `1h` to `60m`.

Rollback restores the previous broad parser and remote HTTP acceptance. No data migration rollback is needed.

## Acceptance criteria

1. `OpenAiChatModel::new(...)` and `new_with_limits(...)` reject every HTTP URL by default.
2. A new explicit transport-policy constructor is the only way for tests/config loading to enable loopback HTTP.
3. Loopback HTTP is restricted to exact `127.0.0.1`, `[::1]`, and `localhost` hosts.
4. Remote HTTP fails even with development opt-in.
5. Transport errors and `Debug` never expose API keys, bearer tokens, full URLs with query, headers, prompts, or model bodies.
6. Existing OpenAI loopback tests use explicit loopback opt-in and still pass.
7. Agent timeout parsing accepts only the Formal V1 positive-integer `ms|s|m` grammar.
8. Duration parse failures keep the public outer code `DSL_YAML_INVALID`.
9. Duration serialization emits only the Formal V1 grammar.
10. README and breaking-change docs describe live-only SSE and the two A8 migration requirements.
11. No API response shape, event envelope, repository schema, or migration file changes are introduced.
