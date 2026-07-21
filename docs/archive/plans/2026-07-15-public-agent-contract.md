# Public Agent Contract Implementation Plan

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

**Goal:** Complete the platform's discover-and-run boundary by exposing each enabled
Agent's exact validated input schema through the existing discovery endpoints.

**Design:**
`docs/superpowers/specs/2026-07-15-public-agent-contract-design.md`

## Constraints

- Use strict RED/GREEN execution for contract behavior.
- Keep list and detail metadata shapes identical.
- Do not add output schemas, Agent CRUD, new DSL nodes, migrations, or compatibility
  aliases.
- Do not expose prompts, nodes, templates, model/Action configuration, or secrets.
- Preserve current Agent version hashes and Run input-validation behavior.

## Task 1: Lock the schema ownership contract

1. Add a schema-adapter test proving the compiled validator retains the exact input
   document while continuing to validate values.
2. Add API RED assertions for exact `input_schema` values in list and detail responses.
3. Strengthen negative assertions for runtime internals and secret markers.
4. Run the focused tests and record the expected missing-field/method failures.

## Task 2: Implement the public projection

1. Retain an immutable clone of the validated document in `JsonSchemaValidator`.
2. Add a read-only document accessor.
3. Add `input_schema` to `AgentMetadata` and project it from the compiled adapter.
4. Run schema, API, compiler, and service tests.

## Task 3: Prove the real boundary and document it

1. Extend `tests/binary_smoke.rs` to assert the checked-in `action_demo` schema.
2. Keep the existing Run creation based on the discovered required `text` field.
3. Update README discovery documentation with the response shape, Draft 7 rule, and
   public-schema warning.
4. Update the current remediation status so completed recent terminal work is no longer
   presented as untouched open work and this discovery contract is recorded.

## Task 4: Complete gates and review

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --all-targets --all-features -- --nocapture --test-threads=1
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo audit
cargo deny check
git diff --check
```

Then review the complete diff for public-contract drift, schema duplication, accidental
private-field serialization, stale documentation, and unrelated scope expansion.
