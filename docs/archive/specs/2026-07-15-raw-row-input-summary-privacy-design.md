# Raw-Row Input-Summary Privacy Design

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

**Date:** 2026-07-15

**Status:** Implemented and verified

**Scope:** End-to-end `RunService` input summarization and direct raw-row verification
for SQLite and PostgreSQL

## 1. Context

Formal V1 intentionally persists an input summary rather than a Run's raw input.
The summary implementation and an in-memory unit assertion already show the intended
shape, but they do not prove the real service-to-database path. A repository-only
test that constructs `NewRun { input_summary: summarize_input(...) }` would also miss
the critical boundary: it would assume that `RunService` supplied a safe value.

This milestone therefore sends secret-bearing input through the production
`RunService`, waits for its normal durable lifecycle, and inspects the native raw
SQLite and PostgreSQL rows.

## 2. Authoritative Summary Contract

For a JSON input value, the only allowed persisted `input_summary` object is:

```json
{
  "keys": ["sorted", "top_level", "property_names"],
  "serialized_bytes": 123
}
```

- `keys` is the lexicographically sorted list of top-level object property names;
  non-object inputs have an empty list.
- `serialized_bytes` is the byte length of the compact `serde_json` encoding of the
  already-parsed input value, not the original HTTP request body's byte length.
- The summary has exactly those two fields. It contains no raw value, nested key,
  type sample, prefix, suffix, hash, or encoded copy of a value.

Top-level property names and total serialized size are intentionally observable
metadata in Formal V1. They are not promised to be confidential. The regression
corpus therefore places unique secret sentinels in values, not in property names,
and proves that the accepted metadata is exact. These fields can still reveal or
help infer input shape and size; this is a bounded metadata contract, not a
zero-information-leak claim.

## 3. Ownership Boundary

`RunService::prepare_run` owns raw-input reduction and constructs the `NewRun` with
`summarize_input(&input)`. The repository owns faithful storage and reconstruction of
that already-reduced JSON value. Re-summarizing inside a repository would corrupt
the contract and would not protect alternate callers that deliberately supply an
arbitrary `NewRun` summary.

Accordingly, this milestone proves the platform's formal `RunService` path. It does
not claim that lower-level callers of `RunRepository::create_run` are prevented by a
database constraint from supplying a value-bearing `input_summary`, nor that an
Agent executor cannot deliberately copy input into outputs or events.

The end-to-end regression must therefore:

1. build a minimal deterministic compiled Agent and production `RunService`;
2. call `create_detached` with the raw secret-bearing input;
3. let the fixed-output Agent reach its durable terminal;
4. query `runs.input_summary` through an independent backend connection;
5. compare the service result, repository reconstruction, and native raw value with
   the independently computed expected summary.

The test Agent never echoes input into output or events, so the test isolates the
input-summary persistence boundary.

## 4. Secret and Encoding Matrix

The input includes multiple public schema fields whose values exercise distinct
storage representations:

- a unique plain ASCII secret;
- a nested object containing another unique secret;
- a string containing a quote, backslash, and newline so a copied value would be
  JSON-escaped in raw storage;
- non-string values whose presence affects only the accepted byte count.

Exact JSON equality with the two-field expected summary is the primary proof that no
additional value-bearing field survived. Raw text is also checked for the plain
sentinels and for both the original and JSON-escaped forms of the escaped sentinel.

## 5. SQLite Evidence

The SQLite case uses a temporary file-backed repository, because an independent raw
connection must observe the same durable database. It selects `input_summary` as the
native `TEXT` value, parses it as JSON, and requires exact equality with the expected
summary. It also verifies the returned and reconstructed `RunRecord` values before
closing the service and pools.

No assertion depends on JSON whitespace or object-key serialization order.

## 6. PostgreSQL Evidence and Ownership

The PostgreSQL case uses a fresh isolated schema and exactly one
`PostgresRunRepository::connect_owned` call. The service and its repository clones
share that one owner generation. A separately scoped read-only pool selects both the
native `JSONB` value and `input_summary::text`.

JSONB may normalize whitespace and key order, so its text representation is parsed
before exact comparison. The text is used only for leak-sentinel checks. The test
then shuts down the service, releases the same owner normally, and drops the schema.

The real PostgreSQL test remains optional locally when
`RUN_HISTORY_POSTGRES_URL` is absent and required in CI under the existing policy.

## 7. Production Change Policy

This milestone begins as verification-only. The existing service already calls
`summarize_input`, and both repositories serialize only the supplied summary.
Production behavior changes are made only if the direct raw-row tests reveal a
concrete leak or reconstruction mismatch.

No migration, public API, summary schema, repository trait, runtime ownership, Agent
execution, or logging behavior change is in scope.

## 8. Acceptance Criteria

1. Secret-bearing input enters each backend only through `RunService`.
2. The created record and repository reconstruction contain exactly sorted top-level
   keys and the compact serialized byte count.
3. Native SQLite `TEXT` and PostgreSQL `JSONB` rows decode to that same exact object.
4. Plain, nested, raw escaped, and JSON-escaped secret values are absent from raw
   summary storage.
5. PostgreSQL uses one valid exclusive-store owner generation and a separate
   read-only inspector connection.
6. Focused backend tests, complete gates, and independent review pass before
   remediation item 8 is marked `Addressed`.
