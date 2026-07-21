# A5 Semantic Compile-Time Validation Design

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

> **Historical / superseded:** this document targets the removed graph/reference scanner. Current typed authored validation is defined by [DSL Authoring Surface Redesign](./2026-07-17-dsl-authoring-surface-redesign.md) and executable compiler tests; retained Region/SSA verification remains in [DSL vNext Region/SSA Design](./2026-07-16-dsl-vnext-region-ssa-design.md).

**Status:** Superseded by DSL vNext

## Context

The stable-baseline review identifies three related P1 findings:

- `BASE-P1-001`: `core.action` accepts fully static schema-invalid input at Agent compilation time and fails only when a Run reaches the node.
- `BASE-P1-002`: Handlebars and CEL node references are discovered by scanning raw source text with a shared regex, so inert syntax such as prose, comments, raw blocks, or string literals can create false graph dependencies.
- `BASE-P1-003`: CEL indexed access such as `nodes["future"].output` is accepted by the interpreter but bypasses compile-time predecessor and branch validation because the raw regex recognizes only dotted text.

A0 already made Action JSON Schema validation errors instance-free. A5 builds on that formatter and moves deterministic DSL errors to compilation without changing the runtime requirement that every Action input and output is still validated.

## Goals

- Replace raw regex reference scanning with parser-aware reference extraction for Handlebars templates and CEL conditions.
- Define one canonical node-output reference shape across template languages: `nodes.<node_id>.output`.
- Reject non-canonical access rooted at `nodes`, including indexed or computed CEL access before the canonical `output` segment.
- Validate fully static `core.action.input` values during Agent compilation by reusing `RegisteredAction::validate_input`.
- Preserve runtime Action input/output validation after template rendering and Action execution.
- Keep existing graph, dominator, fork, branch, and post-join validation as the authority for whether a discovered node reference is legal.
- Keep this milestone focused; do not replace the CEL execution engine.

## Non-goals

- No CEL dependency replacement or expression-language redesign.
- No runtime evaluation of CEL, Handlebars, or Actions during Agent compilation.
- No partial JSON Schema theorem prover for mixed static/dynamic Action input.
- No public HTTP, SSE, Run, repository, history, or migration change.
- No change to Action output validation timing; outputs remain runtime-only.
- No compatibility shim for previously accepted non-canonical references.

## Selected Approach

A5 uses a canonical-reference-only design.

All DSL references to prior node output must use a dotted prefix:

```text
nodes.<node_id>.output
```

The compiler extracts only semantic references produced by the relevant parser:

- Handlebars references are extracted from the compiled Handlebars AST.
- CEL references are extracted from the CEL parser AST.

The existing graph validator then receives the same `BTreeSet<String>` of referenced node IDs it receives today. This deliberately avoids rewriting graph correctness logic. A5 changes how references are discovered and which reference syntax is allowed.

This approach is intentionally breaking. It is the right baseline because compile-time graph validation cannot be sound if users can reach the `nodes` map through arbitrary computed access.

## DSL Reference Contract

### Node and branch identifiers

Node IDs and fork branch IDs must be valid DSL identifiers:

```text
[A-Za-z_][A-Za-z0-9_]*
```

Hyphens are no longer allowed in node IDs or branch IDs.

Reason: `nodes.some-node.output` is not a stable dotted reference in CEL; it can be parsed as subtraction. A stable baseline needs node and branch identifiers that mean the same thing in Handlebars, CEL, join output envelopes, and future node types.

### Canonical node-output prefix

The only valid way to reference node output is:

```text
nodes.<node_id>.output
```

Examples:

```text
nodes.prepare.output.question
nodes.collect.output.branches.source_a.output.text
```

The compiler records only `<node_id>` as the graph dependency. Field access after `.output` is runtime data traversal and does not affect graph topology.

### Allowed traversal after `output`

After the canonical `nodes.<node_id>.output` prefix, language-native data traversal may continue. For example, CEL may inspect fields or array elements under the output value, and Handlebars may render nested fields under the output value.

The restriction applies to reaching a node output, not to reading inside the already-selected output value.

### Rejected access rooted at `nodes`

The compiler rejects any access rooted at `nodes` that does not start with the canonical dotted prefix:

```text
nodes["prepare"].output
nodes[id].output
nodes.prepare["output"]
nodes.prepare
nodes
```

Reason: these forms either bypass reference extraction, expose implementation-shaped runtime context, or make graph validation depend on runtime values.

## Architecture

### New reference extraction boundary

Add a focused DSL module, conceptually:

```text
src/dsl/references.rs
```

It owns:

- DSL identifier validation;
- canonical node-output path parsing;
- Handlebars AST reference extraction;
- CEL AST reference extraction;
- stable reference-policy errors.

Node compilers should no longer call a shared raw-text regex. They should call the relevant parser-aware extraction function through `CompileContext` or the new reference module.

The graph validator remains unchanged except for receiving more accurate reference sets.

### Identifier validation

The Agent compiler validates node IDs before node-specific compilation. `core.fork` validates branch IDs with the same identifier policy.

Invalid node IDs fail with `NODE_ID_INVALID`. Invalid fork branch IDs fail with the existing fork-specific boundary `FORK_BRANCH_ID_INVALID`, with the tightened identifier rule.

This validation happens before reference extraction so error behavior does not depend on which node happens to reference an invalid ID.

### Handlebars extraction

`CompileContext::compile_inline_template` continues to register templates with strict Handlebars mode and `no_escape`.

After successful template compilation, the compiler extracts references from the compiled Handlebars `Template` structure rather than from the original source string.

Extraction walks expression-bearing AST nodes:

- normal expressions;
- HTML expressions;
- helper blocks;
- helper parameters;
- hash parameters;
- subexpressions;
- nested block templates and inverse templates.

It ignores inert syntax:

- raw strings;
- comments;
- escaped template text;
- raw-block body text represented as raw content.

For each Handlebars path expression, the extractor applies canonical node-output path parsing. Literal string parameters containing text such as `nodes.prepare.output` are ignored because they are not references.

If an expression touches `nodes` without a valid canonical prefix, compilation fails with `TEMPLATE_REFERENCE_INVALID`.

### CEL extraction

`core.condition` continues to compile each `when` expression into a `cel_interpreter::Program` for runtime execution.

A5 also makes `cel-parser` an explicit direct dependency for compile-time AST inspection. This is not a CEL engine replacement; it exposes the same parser family already used by the interpreter so the compiler can inspect expression shape.

For each condition case, the compiler:

1. Parses the CEL expression into a parser AST.
2. Traverses the AST to collect canonical `nodes.<node_id>.output` selections.
3. Rejects non-canonical access rooted at `nodes`, including indexed/computed access before `.output`.
4. Compiles the expression into a `cel_interpreter::Program` for runtime execution.

CEL string literals containing `nodes.prepare.output` are ignored. CEL expressions such as `size(nodes.prepare.output.items) > 0` are accepted and record `prepare` as a reference.

Non-canonical condition references fail with `CONDITION_REFERENCE_INVALID`. CEL syntax or interpreter compilation failures continue to fail with `CONDITION_EXPRESSION_INVALID`.

### Static Action input validation

`CompiledTemplateValue` gains a way to report whether its rendered value is fully known at compile time.

An Action input is fully static when the entire JSON value tree contains no dynamic Handlebars expression. A plain string with no Handlebars expression is static even though the template system can render it. An object or array containing any dynamic template expression is dynamic as a whole.

`ActionNode::compile` flow becomes:

1. Resolve the Action.
2. Compile the input as a `CompiledTemplateValue`.
3. If the compiled input has a fully static value, call `RegisteredAction::validate_input(&value)`.
4. Convert validation failure to a compile error with the same safe code/message as runtime validation.
5. Store the compiled input for runtime rendering.

Partially templated inputs are not rejected just because a static subset might be schema-invalid. They are still reference-extracted at compile time and fully validated at runtime after rendering. This avoids false compile-time failures caused by templates that produce values unknown until Run input is available.

## Interface Changes and Rationale

### Static invalid Action input fails at compilation

Previously accepted Agents with fully static invalid Action input will fail startup.

Reason: the platform contract is fail-before-serving for deterministically invalid workflows. Runtime validation remains mandatory, but it is too late for values already known during Agent compilation.

### Indexed and computed `nodes` access is rejected

Previously accepted CEL such as `nodes["future"].output` will fail compilation.

Reason: computed access cannot be validated against predecessor, sibling-branch, and post-join rules at compile time. Rejecting it is simpler and safer than attempting partial support that still leaves dynamic holes.

### Node and branch IDs no longer allow hyphens

Previously accepted IDs such as `some-node` will fail compilation.

Reason: the canonical dotted reference form must be valid and unambiguous in all supported expression/template contexts. Restricting IDs to identifier syntax gives future node types a stable reference grammar.

### Direct `nodes` map access is rejected

Previously accepted expressions that render or inspect the `nodes` object directly will fail compilation.

Reason: the public DSL contract is node output references, not the internal shape of the runtime context map.

## Error Semantics

- Graph legality errors continue to use existing graph validator codes such as `INVALID_NODE_REFERENCE`, `CROSS_BRANCH_REFERENCE`, and `POST_JOIN_BRANCH_REFERENCE`.
- Handlebars policy violations use `TEMPLATE_REFERENCE_INVALID`.
- CEL policy violations use `CONDITION_REFERENCE_INVALID`.
- CEL parse/interpreter compilation failures continue to use `CONDITION_EXPRESSION_INVALID`.
- Static Action input validation uses `ACTION_INPUT_INVALID` and the safe message `action input validation failed`.
- No compile-time error may include raw Action input, rendered template data, Run input, or secret-bearing JSON instances.

## Data Flow

### Template-bearing nodes

1. Node config is strictly deserialized.
2. Each template source is registered with Handlebars strict mode.
3. The compiled Handlebars template AST is walked for semantic references.
4. The node compiler stores the template program and the extracted reference set.
5. The Agent compiler runs the existing graph and execution-plan validators.

### Condition nodes

1. Condition config is strictly deserialized.
2. Each CEL expression is parsed for AST inspection.
3. Canonical references are collected; non-canonical `nodes` access is rejected.
4. The CEL program is compiled for runtime execution.
5. The node compiler emits condition edges and extracted references.
6. The Agent compiler runs the existing graph and execution-plan validators.

### Action nodes

1. Action config is strictly deserialized.
2. The Action is resolved from the registry.
3. The input JSON tree is compiled as a template value.
4. References are extracted from dynamic template expressions.
5. If the full input value is static, it is validated against the registered input schema.
6. Runtime execution still renders input and calls `RegisteredAction::call`, which validates input and output.

## Testing Strategy

### Reference parser tests

Add focused tests for the new reference module:

- valid and invalid DSL identifiers;
- canonical node-output path parsing;
- rejection of direct `nodes` map access;
- rejection of indexed/computed access before `.output`;
- allowance for traversal after `.output`.

### Handlebars tests

Cover both extraction and false-positive removal:

- raw text containing `nodes.prepare.output` is ignored;
- comments containing `nodes.prepare.output` are ignored;
- raw block body containing `nodes.prepare.output` is ignored;
- escaped template syntax is ignored;
- normal expressions extract references;
- helper parameters extract references;
- hash parameters extract references;
- subexpressions extract references;
- nested block and inverse templates extract references;
- non-canonical access rooted at `nodes` fails with `TEMPLATE_REFERENCE_INVALID`.

### CEL tests

Cover:

- canonical dotted references are extracted;
- string literals containing `nodes.prepare.output` are ignored;
- function calls such as `size(nodes.prepare.output.items)` extract references;
- `nodes["x"].output`, `nodes[id].output`, `nodes.x["output"]`, `nodes.x`, and `nodes` fail with `CONDITION_REFERENCE_INVALID`;
- indexed future, self, missing, same-branch, sibling, and post-join cases fail stably because indexed node access is not part of the DSL.

### Graph integration tests

Reuse and extend the existing sequential and parallel suites:

- future references fail with `INVALID_NODE_REFERENCE`;
- self references fail with `INVALID_NODE_REFERENCE`;
- missing references fail with `INVALID_NODE_REFERENCE`;
- cross-branch references fail with `CROSS_BRANCH_REFERENCE`;
- post-join direct branch references fail with `POST_JOIN_BRANCH_REFERENCE`;
- joined aggregate references remain valid.

These tests should use canonical dotted references so they verify the existing graph rules after the new extraction layer.

### Action compile-time validation tests

Cover:

- literal valid Action input compiles;
- literal invalid Action input fails at Agent or node compilation with `ACTION_INPUT_INVALID`;
- plain static strings are treated as static values;
- partially templated Action input compiles and remains runtime-validated;
- custom registered Action schemas participate in compile-time validation;
- errors do not contain raw invalid instances or fixture secrets.

### Checked-in Agent tests

Existing checked-in Agents under `agents/` must compile under the tightened identifier and canonical reference rules.

## Dependency Impact

A5 adds `cel-parser` as a direct dependency so the compiler can inspect CEL ASTs. The runtime interpreter remains `cel-interpreter`.

This is a narrow dependency declaration, not a language-engine migration. The broader CEL package replacement remains outside A5.

## Operational Impact

No database migration, HTTP API change, SSE protocol change, or runtime state migration is required.

Operators should expect some previously accepted Agent YAML files to fail startup if they contain:

- fully static invalid Action input;
- hyphenated node or branch IDs;
- indexed/computed `nodes` access;
- direct access to the internal `nodes` map.

This is intentional and must be called out in release notes or operator-facing upgrade notes for the stable baseline.

## Acceptance Criteria

A5 is complete when:

1. Raw regex node-reference scanning is removed from compiler and node compilers.
2. Handlebars and CEL references are extracted from parser structures.
3. Non-canonical `nodes` access is rejected before graph validation.
4. Fully static invalid Action input fails compilation with safe `ACTION_INPUT_INVALID`.
5. Partially templated Action input remains runtime-validated.
6. Existing graph validation behavior is preserved for canonical references.
7. Checked-in Agents compile under the tightened DSL.
8. The full Rust gate passes: formatting, Clippy with warnings denied, and all tests.
