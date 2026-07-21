# A6 — Extension integration contract design

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

> **Historical / superseded:** the node registry and public generic-operation extension contract below were removed. The current authored surface has no generic extension escape hatch; use typed Actions defined by [DSL Authoring Surface Redesign](./2026-07-17-dsl-authoring-surface-redesign.md).

Date: 2026-07-12

## Status

Design approved for A6 implementation planning.

## Context

`BASE-P2-005` identifies a coverage gap in the current extension boundary. `tests/node_extensions.rs` proves that a custom node can implement `NodeType` and `NodeExecutor`, but it calls those traits directly and manually constructs a `CompiledNode`. That bypasses the production path extension authors actually depend on:

1. YAML parsing and `AgentCompiler`.
2. graph, envelope, reference, and execution-plan validation.
3. scheduler/coordinator dispatch through `NodeExecutorRegistry`.
4. durable node output, event journal, and terminal Run publication.

The platform goal is a stable, general architecture baseline. Extension support should therefore prove the statically linked custom-node contract end to end without introducing a broader plugin system.

## Decision

A6 will add an end-to-end extension contract test harness and documentation around the existing registry injection points.

The implementation should use the current public seams:

- `NodeTypeRegistry` for compile-time type registration.
- `NodeExecutorRegistry` for runtime executor registration.
- `AgentCompiler::new` for compiling synthetic YAML with custom node types.
- `RunCoordinator` or `RunService` with `EventHub` and a repository-backed history path for runtime execution.

No production interface change is planned. If implementation discovers that one existing seam cannot express this contract safely, the change must be minimal and documented with its reason. Acceptable examples are helper constructors or narrowly scoped test utilities. Dynamic plugin loading, remote extension execution, tenant policy, and binary ABI stability are outside A6.

## Extension contract to verify

An extension node is considered integrated only when all of the following are true:

1. The node type is registered outside core node modules.
2. Agent YAML references the custom `type` value and compiles through `AgentCompiler`.
3. The compiler applies the same envelope, `next`, graph, and reference rules as core nodes.
4. Runtime dispatch resolves the executor by the compiled node kind.
5. The executor downcasts the compiled body it produced and returns a `NodeOutcome`.
6. Node output is persisted through the normal `EventHub::put_node_output` path.
7. `node.started`, optional `content.delta`, `node.completed` or `node.failed`, and terminal Run events are sequenced and stored through the normal event journal.
8. Final Run state and terminal output are visible through the repository-backed Run record.

This contract intentionally treats extensions as statically linked Rust code. The platform owner registers extension types and executors during process setup. Extension authors do not edit `src/nodes/mod.rs` or any core node implementation to prove their node works.

## Test architecture

The primary implementation target is `tests/node_extensions.rs`.

The test module should define synthetic node fixtures locally:

- a successful custom node, for example `test.constant`, with strict config and deterministic output;
- a mismatch fixture that compiles with one body type but executes with an incompatible executor or missing executor registration.

The tests should create a temporary agent directory with `agent.yaml`, compile it with a registry that includes the synthetic type, and execute it through production runtime components. The preferred runtime path is `RunService` because it proves the public service-level integration and repository terminal record. `RunCoordinator` is acceptable for focused mismatch cases if it keeps the test simpler while still using scheduler, events, and repository.

The repository should be an existing in-memory or lightweight test repository pattern from current runtime tests. If duplicating a repository fixture would make `tests/node_extensions.rs` noisy, extract a small test helper only if it is test-scoped and does not become part of the production API.

## Required acceptance coverage

The A6 implementation is complete when these cases pass:

1. Custom-node success:
   - YAML custom node compiles through `AgentCompiler`.
   - No core source file needs to register the synthetic type.
   - Runtime completes successfully.
   - Node output is persisted.
   - Terminal Run output includes the expected custom-node result, either directly or through a following `core.output` node.
   - Stored event sequence includes Run lifecycle, custom node lifecycle, and terminal completion.

2. Registry mismatch:
   - compiler-only registration with missing runtime executor fails through the production runtime path as an infrastructure failure and commits a failed terminal Run with the existing stable infrastructure terminal code;
   - executor/body mismatch produces `NODE_BODY_TYPE_MISMATCH` and a failed terminal Run when surfaced through the runtime path.

3. Envelope and graph parity:
   - custom node `NextPolicy::Required` still requires `next`;
   - custom node references are validated against the same graph rules as core node references;
   - custom node content emit is rejected unless `allows_content_emit` is true.

4. Durable event/output path:
   - node output reaches repository storage;
   - event rows are sequenced;
   - terminal state is committed exactly once.

## Error handling

Compile-time errors remain `CompileError` values from the extension node compiler or shared graph validation. Runtime extension failures remain `RunError` values classified by the existing runtime:

- missing executor is infrastructure failure;
- body downcast mismatch is a node failure because `CompiledNode::body` returns `RunError::new`;
- executor-returned node errors publish `node.failed` and a failed terminal Run;
- event or repository errors remain infrastructure failures and use existing terminal recovery behavior.

A6 should not invent new error schemas unless an existing error code is ambiguous. If a new code is needed, it must identify the violated contract without leaking extension body data.

## Documentation impact

Update the extension-facing documentation in the smallest useful location. The docs should state:

- extension nodes are statically linked and registered at process setup;
- registration has two halves: compile-time type and runtime executor;
- custom nodes use the same DSL envelope, graph, reference, event, output, and terminal semantics as core nodes;
- A6 does not support dynamic loading or remote plugins.

`docs/reviews/2026-07-11-stable-baseline-review.md` is a historical review artifact and should not be rewritten as if the original finding disappeared. If status tracking is needed, add a short implementation note elsewhere rather than mutating review evidence.

## Rejected alternatives

### Add a new extension builder API now

This would make registration ergonomics clearer, but current constructors already accept `NodeTypeRegistry` and `NodeExecutorRegistry`. Adding API before an end-to-end test proves the gap risks designing the wrong abstraction.

### Build dynamic plugin loading

Dynamic plugin loading would require versioning, trust boundaries, isolation, and deployment policy. Those concerns are real but not part of a stable development baseline for the current Rust crate.

### Keep only direct trait tests

Direct trait tests are still useful for small mechanics, but they cannot catch compiler/runtime coupling. They are insufficient as the primary extension contract.

## Success criteria

A6 succeeds when a reviewer can inspect the tests and see a synthetic extension node travel through the same production path as a core node: YAML compile, runtime dispatch, event persistence, node output storage, and terminal Run state. The implementation should not require core node source edits and should not broaden the platform into a plugin system.
