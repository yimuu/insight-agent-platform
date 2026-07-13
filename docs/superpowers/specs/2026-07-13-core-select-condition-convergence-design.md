# Core Select Condition Convergence Design

**Date:** 2026-07-13

**Status:** Approved for implementation

**Scope:** Explicit one-of-N result convergence for mutually exclusive control-flow paths

## 1. Context

Formal V1 conditions select exactly one outgoing path, but a later common node cannot safely reference a path-local output. A path-local node does not dominate the convergence point, so the existing reference validator correctly rejects a template or CEL expression that names it. Relaxing that rule globally would turn absent outputs into runtime surprises and weaken the platform's compile-time guarantees.

Parallel convergence already has an explicit `core.fork` and `core.join` contract. That contract does not fit condition paths: every fork branch runs and settles, while only one condition path runs. A condition convergence primitive therefore needs one-of-N semantics rather than `all_settled` semantics.

The chosen design adds `core.select`. A Select receives a declared set of mutually exclusive direct predecessors, identifies the one whose output is visible in the current `RunContext`, and publishes that value under one stable output contract. Downstream nodes reference the Select output instead of referencing path-local nodes.

## 2. Goals

- Add an explicit `core.select` node for mutually exclusive path convergence.
- Preserve the current strict dominance rule for every ordinary node reference.
- Require enough graph structure to prove that Select candidates are local, direct, and mutually exclusive.
- Treat an executed node whose JSON output is `null` as present, not missing.
- Produce one stable output containing the selected node ID and its exact JSON value.
- Allow Select inside a linear graph or inside one fixed fork branch without crossing branch boundaries.
- Reuse the existing scheduler, node lifecycle, timeout, cancellation, event, and persistence machinery.
- Keep all existing Agent YAML behaviorally unchanged.

## 3. Non-goals

- Global nullable outputs for unexecuted nodes.
- A `previous` or implicit-predecessor template context.
- Parallel aggregation; `core.join` remains the only fixed parallel convergence primitive.
- Value merging, array concatenation, object overlay, reduction, or priority-based fallback.
- Selecting the first candidate by configuration order.
- Named condition case IDs.
- Output-schema unification or static type checking across candidates.
- New join policies such as `fail_fast`, `any`, or `quorum`.
- Nested forks, dynamic `foreach`, wait/resume, Human Input, or Dify DSL import.
- A visual workflow editor.

## 4. Chosen Approach

Add a dedicated `core.select` node with an explicit `sources` list:

```yaml
selected_answer:
  type: core.select
  next: render
  config:
    sources:
      - normalize_medical
      - normalize_general
```

Each declared source must be a direct graph predecessor of the Select, and every direct predecessor must be declared. Sources must share the Select's execution region and must be pairwise unreachable from each other. In the current single-successor control-flow model, those rules prove that the candidates are alternatives rather than sequential steps. Fork region validation separately prevents candidates from representing concurrently active sibling branches.

At runtime, Select examines only the declared sources in the current context:

- exactly one visible source: succeed;
- no visible source: fail with `SELECT_SOURCE_MISSING`;
- more than one visible source: fail with `SELECT_SOURCE_AMBIGUOUS`.

The two error cases remain runtime checks even though valid compiled graphs should make them unreachable. They defend the node executor contract against constructed compiled nodes, extension mistakes, and future scheduler changes.

### 4.1 Rejected alternatives

#### Treat every unexecuted node output as `null`

This would make the YAML shorter, but it would conflate an unexecuted node with a node that successfully returned JSON `null`. It would also require relaxing dominance validation throughout templates, CEL expressions, Chat dynamic sources, Actions, and future node types. Missing-data handling would become an implicit responsibility of every consumer.

#### Add a `previous` context value

This is compact but makes data dependencies depend on immediate topology. Inserting a normalization node would silently change what a downstream node reads. The formal DSL currently uses named outputs specifically so dependencies remain visible and compiler-checkable.

#### Infer candidates from incoming edges with no DSL declaration

The graph compiler could derive candidates, but an explicit list documents that the node intentionally performs one-of-N value selection. It also allows configuration errors and undeclared incoming paths to fail with Select-specific diagnostics.

## 5. DSL Contract

The complete shape is:

```yaml
<node_id>:
  type: core.select
  next: <successor_node_id>
  config:
    sources:
      - <source_node_id_1>
      - <source_node_id_2>
```

Rules:

- `config` denies unknown fields.
- `sources` is required.
- `sources` contains at least two node IDs.
- Every source ID uses the canonical DSL identifier grammar.
- Source IDs are unique.
- Source order has no priority or fallback meaning.
- `next` is required.
- `emit: content` is forbidden.
- Select is non-terminal.

The source list is serialized as authored and remains part of the normalized Agent hash, but runtime behavior and output do not depend on list order.

## 6. Example

```yaml
version: 1
id: select_demo
name: Select Demo
input:
  schema:
    type: object
    additionalProperties: false
    required: [kind]
    properties:
      kind:
        type: string

entry: route
nodes:
  route:
    type: core.condition
    config:
      cases:
        - when: "input.kind == 'medical'"
          next: medical
      default: general

  medical:
    type: core.template
    next: selected_answer
    config:
      value:
        kind: medical
        text: "medical answer"

  general:
    type: core.template
    next: selected_answer
    config:
      value:
        kind: general
        text: "general answer"

  selected_answer:
    type: core.select
    next: result
    config:
      sources: [medical, general]

  result:
    type: core.output
    config:
      data:
        source: "{{ nodes.selected_answer.output.source_node_id }}"
        answer: "{{ nodes.selected_answer.output.value.text }}"
```

For `input.kind == 'medical'`, `selected_answer` produces:

```json
{
  "source_node_id": "medical",
  "value": {
    "kind": "medical",
    "text": "medical answer"
  }
}
```

The `value` field is the source output exactly. It is not wrapped, coerced, merged, or normalized by Select.

## 7. Compiled Contract

`NodeControl` gains a typed Select variant:

```rust
pub enum NodeControl {
    Ordinary,
    Fork {
        branches: BTreeMap<String, String>,
        join: String,
    },
    Join {
        policy: JoinPolicy,
    },
    Select {
        sources: BTreeSet<String>,
    },
}
```

`SelectNode::compile` parses and validates the local config, creates the typed control, and uses the ordinary node envelope:

```text
next: required
emit: content forbidden
terminal: false
outgoing edges: envelope next only
ordinary required references: none
```

Select sources are not inserted into `CompiledNode.references`. They are a distinct one-of-N dependency class and would fail the ordinary dominance rule by design. Their validity is established through the typed control and Select topology validation instead.

No new `NodeTransition` variant is required. A successful Select returns `NodeTransition::Next`.

No new execution-plan collection is required. The source set is immutable in `NodeControl` and in the compiled node body. The existing `NodeRegion` mapping supplies the region information needed during validation.

## 8. Graph Validation

Select validation runs after ordinary graph structure and fork-region compilation, and before ordinary reference validation completes. For each Select node, the compiler must validate the following.

### 8.1 Source existence and identity

- Every declared source exists.
- The Select does not name itself.
- Source IDs are unique.
- There are at least two sources.

Local config validation should reject malformed identifiers, duplicates, and a source count below two. Graph validation should still defensively enforce the compiled invariant.

### 8.2 Exact predecessor match

Let `P` be the set of direct graph predecessors of the Select and `S` its declared source set. Compilation requires:

```text
P == S
```

This rejects both:

- a declared source that does not flow directly into the Select;
- an incoming path that can activate the Select but is absent from its source contract.

The relationship is derived from compiled graph edges, so it works for ordinary `next` edges and typed conditional edges.

### 8.3 Region locality

Every source must have the same `NodeRegion` as the Select:

- linear sources may feed a linear Select;
- sources inside one fork branch may feed a Select inside that same branch;
- sources from sibling fork branches may not feed one Select;
- a branch source may not feed a post-join Select directly;
- a Join region node may not be mixed with ordinary alternatives.

Existing fork-region validation remains authoritative. Select does not provide an escape hatch around `CROSS_BRANCH_REFERENCE`, `POST_JOIN_BRANCH_REFERENCE`, or branch boundary rules.

If fork-region compilation rejects a topology before Select validation runs, its existing `BRANCH_*` or `JOIN_*` diagnostic remains authoritative. `SELECT_REGION_INVALID` is the defensive Select-specific diagnostic for a source/Select region mismatch once an execution plan exists.

### 8.4 Mutual exclusivity

For every pair of sources `(a, b)`:

- `b` must not be reachable from `a`;
- `a` must not be reachable from `b`.

This prevents a sequential path from completing one candidate and later completing another before activating Select. Because ordinary and conditional execution activate one successor per completed node, and because actual parallelism is confined to compiler-validated fork regions, pairwise non-reachability plus region locality is the formal V1 exclusivity proof.

Select does not require an explicit owning Condition ID. This keeps the DSL small and permits convergence after nested conditional routing within one non-parallel region, provided the exact-predecessor and exclusivity rules hold.

### 8.5 Downstream references

The Select output follows ordinary dominance rules. Once all successful paths pass through the Select, downstream nodes may reference:

```text
nodes.<select_id>.output.source_node_id
nodes.<select_id>.output.value
```

Downstream nodes still may not directly reference one of the candidate sources unless that source independently satisfies the ordinary dominance rule.

### 8.6 Stable compile diagnostics

Select-specific validation failures use stable codes:

| Code | Meaning |
| --- | --- |
| `NODE_CONFIG_INVALID` | `config` cannot be decoded or contains unknown fields. |
| `SELECT_SOURCE_COUNT_INVALID` | Fewer than two sources are declared. |
| `SELECT_SOURCE_ID_INVALID` | A source does not use the canonical node-ID grammar or names the Select itself. |
| `SELECT_SOURCE_DUPLICATE` | A source is declared more than once. |
| `SELECT_SOURCE_NOT_FOUND` | A declared source node does not exist. |
| `SELECT_PREDECESSOR_MISMATCH` | The declared source set is not exactly the direct predecessor set. |
| `SELECT_REGION_INVALID` | A source and the Select do not occupy the same compiler region. |
| `SELECT_SOURCES_NOT_EXCLUSIVE` | One declared source is reachable from another. |

Diagnostics may include node IDs and graph relationships, but must not include source output bodies. Tests assert the codes rather than full prose so explanatory messages can improve without changing the contract.

## 9. Runtime Semantics

`SelectNode::execute` performs these steps:

1. Observe the shared stop signal and return the authoritative stop error if set.
2. Read the immutable compiled source set.
3. Query `RunContext::node_output` for every declared source.
4. Count sources for which the lookup returns `Some`, including `Some(Value::Null)`.
5. Return the stable output when exactly one source is present.
6. Return `SELECT_SOURCE_MISSING` when none is present.
7. Return `SELECT_SOURCE_AMBIGUOUS` when more than one is present.

Stable success output:

```json
{
  "source_node_id": "normalize_medical",
  "value": null
}
```

The example above is a successful selection when `normalize_medical` executed and returned JSON `null`.

Stable node errors:

```text
SELECT_SOURCE_MISSING
select node '<select_id>' has no completed source

SELECT_SOURCE_AMBIGUOUS
select node '<select_id>' has multiple completed sources
```

Error messages do not include source output bodies. The ambiguous error may include the sorted source node IDs because node IDs are non-sensitive structural metadata.

Outside a fork, a Select node error follows the existing node-failure path and fails the Run. Inside a fork branch, it becomes that branch's ordinary failure under the existing `all_settled` contract. Stop and infrastructure failures retain their current classification and never become Select data.

## 10. Scheduler and Context Impact

The scheduler requires no new activation or synchronization directive:

- a selected path completes its terminal source node;
- that source's output is added to the path context;
- the source activates Select through its ordinary successor edge;
- Select reads the visible candidate outputs and returns `Next`.

The existing node state machine ensures Select executes at most once. A valid condition graph activates only one source. Duplicate activation remains a scheduler invariant failure and is not reinterpreted as Select ambiguity.

`RunContext` requires no structural changes. It already distinguishes absence (`None`) from an executed JSON-null output (`Some(Value::Null)`). No placeholder node outputs are inserted.

## 11. Node Registration and Extension Contract

The built-in implementation adds:

```text
src/nodes/select.rs
```

`SelectNode` is registered in both default registries. Registry parity remains testable through the existing extension contract. Custom node APIs do not change: Select is implemented through the existing `NodeType` and `NodeExecutor` traits.

## 12. Events, Persistence, and Observability

Select uses the ordinary node lifecycle:

```text
node.started
node.completed
```

or, on failure:

```text
node.started
node.failed
```

No Select-specific event type is added. The selected source node ID and selected value are persisted through the ordinary node-output repository record before `node.completed` is published; `node.completed.data.output` carries the same ordinary event payload as every other successful node. Existing body-free INFO logging policy continues to record only safe identifiers, timings, counts, and sizes, not the selected value body.

SQLite and PostgreSQL repository contracts require no schema or migration change.

## 13. Compatibility

The change is additive to Agent DSL version 1:

- existing YAML requires no edits;
- existing compiled graph behavior is unchanged;
- no API, SSE, history, migration, model, Action, or provider contract changes;
- the Agent version hash naturally changes only for Agents that add Select nodes.

The new node kind must be listed in README documentation and in the built-in registry contract tests.

## 14. Test Strategy

### 14.1 Node contract

- Compile a valid source list into `NodeControl::Select`.
- Reject missing, empty, one-item, duplicate, malformed, and unknown config fields.
- Require `next` and reject `emit: content`.
- Select each source independently.
- Treat an executed JSON-null output as present.
- Reject zero visible sources with `SELECT_SOURCE_MISSING`.
- Reject multiple visible sources with `SELECT_SOURCE_AMBIGUOUS`.
- Confirm errors never include source output bodies.

### 14.2 Graph and reference validation

- Compile a valid two-way condition convergence.
- Compile a valid multi-way condition convergence.
- Reject a missing source node.
- Reject a source that is not a direct predecessor.
- Reject an undeclared direct predecessor.
- Reject source pairs connected by a path in either direction.
- Reject candidates from sibling fork branches.
- Reject candidates that cross linear, branch, or Join regions.
- Allow Select inside one fork branch after a branch-local Condition.
- Allow downstream references to the Select output.
- Continue rejecting downstream direct references to non-dominating candidates.
- Preserve every existing fork/join and ordinary reference test.

### 14.3 Scheduler integration

- Prove each condition route executes only its selected source and then Select.
- Prove the unselected source never executes.
- Prove Select output reaches template, Chat, Action, and Output consumers through existing context propagation.
- Prove a Select failure outside a fork fails the Run.
- Prove a Select failure inside a fork settles only that branch.
- Preserve cancellation, node timeout, and infrastructure-failure classification.

### 14.4 Events and repositories

- Persist and replay Select's ordinary node events and output.
- Verify selected JSON bodies remain absent from INFO logs.
- Require no repository migration.

### 14.5 Repository example and documentation

- Add a deterministic compiler/scheduler fixture demonstrating Condition -> alternatives -> Select -> Output; do not add a new enabled production Agent for this milestone.
- Add the complete deterministic DSL example to the README node documentation.
- Document that Select is one-of-N convergence and Join is all-branches convergence.
- Document candidate normalization as the recommended pattern when source output shapes differ.

## 15. Verification Gates

The implementation is complete only after fresh success from:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
cargo audit
cargo deny check
```

PostgreSQL integration is required only if implementation changes repository behavior. This design requires no repository change, so existing repository parity tests and the full locked test suite are sufficient for the milestone.

## 16. Acceptance Criteria

The milestone is complete when:

1. Formal V1 accepts `core.select` with at least two explicit sources and one ordinary successor.
2. The compiler proves exact predecessor matching, region locality, and pairwise source exclusivity.
3. Exactly one completed source produces the exact stable `{source_node_id, value}` output.
4. JSON `null` remains distinguishable from an unexecuted source.
5. Zero and multiple visible sources fail with stable, body-free errors.
6. Downstream nodes consume only the dominating Select output; ordinary reference safety is not weakened.
7. Select works inside one fork branch without changing fork/join semantics.
8. Existing Agents, APIs, events, persistence, and scheduler behavior remain compatible.
9. README documentation and a deterministic compiler/scheduler fixture demonstrate the contract.
10. All verification gates pass.

## 17. Follow-on Work

The next independent design after Select should cover explicit workflow failure and `all_settled` consumption:

1. add a built-in `core.fail` node;
2. route `core.join` through `core.condition` on `summary.succeeded`;
3. demonstrate deliberate failure or degraded output when no parallel branch succeeds.

Named Condition case IDs and production concurrency verification remain separate later milestones. They are not prerequisites for Select.
