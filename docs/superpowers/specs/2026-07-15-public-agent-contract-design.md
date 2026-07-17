# Public Agent Contract Design

**Date:** 2026-07-15

**Status:** Implemented

**Scope:** Compiled Agent schema ownership, `GET /v1/agents`,
`GET /v1/agents/{agent_id}`, binary smoke coverage, and public documentation

## 1. Context

The platform owns and compiles every enabled Agent. External clients discover those
Agents and submit inputs, but the current discovery responses expose only identity and
version metadata. The runtime validates every Run input against the Agent's JSON
Schema, while clients cannot discover that exact contract from the platform.

The public discovery contract must expose the same validated schema used at Run
creation without exposing prompts, compiled workflow IR, model configuration, Action
configuration, or other runtime internals.

## 2. Decision

Both discovery endpoints return one public representation:

```json
{
  "id": "action_demo",
  "name": "Action Demo",
  "description": "Demonstrates a typed native Rust action.",
  "version": "sha256:...",
  "input_schema": {
    "type": "object",
    "required": ["text"],
    "additionalProperties": false,
    "properties": {
      "text": {"type": "string"}
    }
  }
}
```

- `GET /v1/agents` returns this representation for every enabled Agent.
- `GET /v1/agents/{agent_id}` returns the same representation for one enabled Agent.
- `input_schema` is the exact canonical validator document used at Run creation:
  the authored `input.schema` plus the Agent's top-level `$defs` when present. It uses
  the required Draft 2020-12 policy. Exactness means structured JSON `Value` equality,
  not preservation of YAML bytes, comments, scalar spelling, or key order.
- It validates the entire JSON request body sent to either Run creation endpoint; the
  body is not wrapped in an additional `input` property.
- Apart from injecting the already-authored top-level `$defs`, the API does not insert,
  remove, normalize, or upgrade schema keywords.
- The Agent's required top-level `schema_dialect` is exactly
  `https://json-schema.org/draft/2020-12/schema`; a future dialect change must use a
  new authored contract and may not silently reinterpret existing documents.
- The complete schema document is public metadata. Agent authors must not put secrets
  in schema annotations such as descriptions, examples, or defaults.

## 3. Single Source of Truth

The project-owned schema adapter retains both:

1. the immutable schema document;
2. the compiled validator built from that document.

`CompiledWorkflow` owns the adapter. Run input validation reads the compiled
validator, while discovery serializes a clone of the retained document. No API layer
re-reads Agent YAML and no second schema representation is authored by hand.

This narrowly extends the existing project-owned schema adapter with a read-only
document accessor. The upstream validator and its error representation remain private;
the adapter still exposes only document access and boolean validation to callers.

The Agent version hash already includes the raw Agent configuration, including the
input schema. A schema-only change therefore changes `version`, and clients may cache
the public contract by `(id, version)` without a second schema hash. Retaining and
exposing the document does not otherwise change version semantics.

## 4. Security and Compatibility

- Prompts, templates, authored workflow bodies, Region/SSA IR, model aliases, provider
  configuration, Action configuration, and credentials remain private.
- `id`, `name`, `description`, `version`, and the complete `input_schema` are explicitly
  public and are not heuristically redacted. Private-field sentinel tests enforce the
  boundary instead of scanning legitimate public text for suspicious words.
- Only explicitly enabled Agents are discoverable.
- Existing bearer authentication continues to protect both `/v1/agents` endpoints.
- This historical addition is retained by the canonical v2 implementation; it does not
  expose new runtime or authored-DSL internals.
- No Agent CRUD API, output schema, dynamic plugin contract, or schema negotiation is
  added.

## 5. Verification

Tests must prove:

1. the schema adapter returns the exact canonical document used to build its validator;
2. list and detail responses return that exact Agent input validator document;
3. the matching list item and detail response are equal as complete metadata objects;
4. both endpoints retain authentication and enabled-only behavior;
5. serialized metadata contains no prompts, templates, workflow/IR, model configuration, or
   credentials;
6. the real binary exposes the checked-in `action_demo` schema and accepts an input
   constructed from that discovered contract;
7. all existing input validation, Agent hashing, API, and binary behavior remains
   unchanged.

## 6. Acceptance Criteria

1. Every public Agent metadata response contains `input_schema`.
2. The returned value equals the exact canonical schema document in the compiled Agent.
3. Runtime validation and discovery cannot drift because both use one schema adapter.
4. No private Agent or resource implementation detail becomes serializable through the
   discovery response.
5. Focused tests and the complete repository gates pass.
