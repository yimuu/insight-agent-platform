# DSL vNext Region-SSA Design

**Status:** Implemented

## Objective

Replace the public flat control-flow graph with a structured, lexically scoped
authoring language. The compiler lowers that language into a typed Region/SSA
plan; the runtime executes the plan as a hierarchy of scopes and tasks.

Compatibility with Formal V1 is not a design constraint. vNext optimizes for:

- unambiguous workflow and child-scope completion;
- composable nested concurrency and selection;
- type-preserving data transfer;
- compile-time scope, dominance, path, and schema validation;
- deterministic durable identities;
- hierarchical cancellation and terminal persistence;
- safe public events and model-input boundaries.

## Architectural decision

The canonical authored document is a structured AST. It is not the runtime
graph and it does not expose runtime control instructions as registered nodes.

```text
authored YAML
  sequence + operation + parallel + switch + result/return/raise
                         |
                         v
typed Region/SSA IR
  Call + Parallel + Branch/Phi + RegionYield + WorkflowReturn + Raise
                         |
                         v
runtime scope tree
  admission + child tasks + barriers + cancellation/drain + terminal commit
```

The public DSL removes, without aliases:

- `entry`;
- the flat `nodes` map;
- `next` and authored control edges;
- `core.fork`;
- `core.join`;
- `core.branch_end`;
- `core.condition`;
- `core.select`;
- `core.end`.

Leaf capabilities such as Chat and Action remain extensible operations. An
extension may compute a typed value or perform a declared effect, but it may not
create control edges or workflow terminal transitions.

## Canonical document

```yaml
api_version: insight.agent/v2
kind: agent

metadata:
  id: example
  name: Example
  description: Example workflow.

schema_dialect: https://json-schema.org/draft/2020-12/schema

$defs: {}
prompts: {}
errors: {}

input:
  schema: {type: object}

output:
  # This validates the platform-owned RunOutput.data field.
  data_schema: {type: object}

workflow:
  steps: []
  result:
    return:
      data: {literal: {}}
```

Unknown fields are rejected at every authored boundary. `api_version` and
`kind` are exact scalar discriminators. `schema_dialect` is required and must
equal canonical `https://json-schema.org/draft/2020-12/schema`. Identifiers match
`[A-Za-z_][A-Za-z0-9_]*`.

Layout and editor state are not part of this document or its semantic hash.
Top-level `$defs` are injected into every authored contract schema before
resolution, so `#/$defs/Name` has one unambiguous meaning throughout the
document.

### Typed JSON Schema profile

Runtime validation uses Draft 2020-12, while static compilation accepts only a
profile that can be represented conservatively by `ValueType`:

- boolean schemas and `type`, including type arrays;
- scalar `const` and `enum`;
- homogeneous arrays with explicit `items` and optional `minItems`;
- objects with `properties`, `required`, and `additionalProperties`;
- `oneOf`/`anyOf` without shape-changing sibling keywords;
- exact local `#/$defs/<Identifier>` references, expanded before type
  compilation.

Value constraints that do not establish reachable fields, such as
`minLength`, `maximum`, `format`, and `maxItems`, remain in the runtime
validator but grant no static path or narrowing capability. Shape applicators
that are not represented by `ValueType` fail compilation explicitly. This
includes `allOf`, `not`, `if`/`then`/`else`, `dependentSchemas`,
`dependentRequired`, `patternProperties`, `propertyNames`,
`unevaluatedProperties`, `minProperties`/`maxProperties`, `prefixItems`,
`unevaluatedItems`, and dynamic references. A union combinator
also cannot have `properties`, `required`, `additionalProperties`, `items`, or
`minItems` siblings. The compiler never silently derives a weaker field model
from one of these authored contracts.

## Value expressions

Every runtime-derived value uses exactly one recursive `ValueExpr` form:

```yaml
{literal: 10}
{from: input.question}
{from: steps.search.output.items}
{object: {question: {from: input.question}, limit: {literal: 10}}}
{array: [{literal: technical}, {literal: risk}]}
{prompt: synthesis_system}
template:
  text: "Found {{ count }} results"
  bindings:
    count: {from: steps.search.output.count}
```

Contracts:

- `literal` preserves the complete JSON type;
- `from` preserves its source JSON type;
- `object` and `array` recursively evaluate typed expressions;
- `template` receives only its explicit bindings and returns a string;
- `prompt` resolves one authored prompt and returns a string;
- plain strings are not templates or references;
- a ValueExpr object contains exactly one expression key;
- computed step IDs and dynamic property expressions are forbidden;
- DSL identifiers constrain declarations and bindings, not JSON property names;
- a missing path is an error unless a future expression explicitly declares a
  different missing policy.

`from` is parsed to a canonical path AST. The initial roots are:

- `input`;
- `run` for the closed safe metadata view containing only string fields `id`,
  `request_id`, `agent_id`, `agent_version`, and `started_at`;
- `scope` for values explicitly captured by a structured block;
- `steps.<id>.output` for an earlier step in the current body.

Identifier-shaped object keys may use dot shorthand. Arbitrary JSON object keys
and fixed array indices use a static JSON Pointer suffix:

```yaml
{from: input#/items/0/display-name}
{from: steps.search.output#/data/a~1b}
```

JSON Pointer escaping follows RFC 6901 (`~0` for `~`, `~1` for `/`). The
compiler retains the full path, rejects computed access and non-canonical array
indices, and validates every segment against source schemas.
Handlebars never receives a global `nodes` or `steps` map.

## Workflow body and steps

`workflow.steps` executes in authored order. Concurrency exists only inside a
structured concurrency operation.

### Leaf operation

```yaml
- kind: operation
  id: analyze
  uses: ai.chat
  with:
    question: {from: input.question}
  config:
    model: general_chat
```

`config` is deployment-time static configuration. `with` contains runtime
ValueExpr inputs. A versioned operation descriptor defines config schema, input
schema, resolved output schema, effects, idempotency, streaming support,
provenance, and required runtime capabilities.

A successful operation binds `steps.<id>.output` in its body. A failure
propagates through the nearest structured scope according to that scope's
declared policy.

### Parallel

```yaml
- kind: parallel
  id: analyses
  with:
    question: {from: input.question}
  settle: all_settled
  max_concurrency: 2
  branches:
    technical:
      output_schema: {$ref: "#/$defs/Perspective"}
      steps:
        - kind: operation
          id: analyze
          uses: ai.chat
          with:
            question: {from: scope.question}
          config: {model: general_chat}
      result:
        return: {from: steps.analyze.output.data}

    risk:
      output_schema: {$ref: "#/$defs/Perspective"}
      steps: []
      result:
        raise: risk_unavailable
```

The Parallel owns both spawn and barrier semantics. A branch never defines a
successor. After every required child has settled and cancellation cleanup has
drained, the parent sequence continues with the next step.

Initial settle strategies are intentionally discrete:

- `all`: every branch must return successfully; otherwise the Parallel fails;
- `all_settled`: every settleable branch failure becomes typed result data and
  the Parallel returns after all branches settle.

Race and quorum have different cancellation and result types; they are future
separate constructs, not boolean switches on Parallel.

For `all`, the result is a record keyed by branch ID. For `all_settled`, each
branch is a discriminated union:

```text
{status: "ok", value: T}
|
{status: "error", error: SafeBranchError}
```

`SafeBranchError` contains stable category/code/retryable/origin fields. It does
not contain arbitrary diagnostic text. Private Operation messages are retained
only inside the in-flight error object and are discarded at public persistence
and default-log boundaries.

Business acceptance such as "at least one perspective succeeded" belongs in a
following Switch or assertion. It is not conflated with settlement.

### Switch

```yaml
- kind: switch
  id: synthesis_input
  with:
    results: {from: steps.analyses.output}
  output_schema: {$ref: "#/$defs/SynthesisInput"}
  cases:
    - id: complete
      when:
        cel: >-
          scope.results.technical.status == 'ok' &&
          scope.results.risk.status == 'ok'
      steps: []
      result:
        return: {object: {mode: {literal: full}}}
  default:
    id: partial
    steps: []
    result:
      return: {object: {mode: {literal: partial}}}
```

Cases are ordered and first-match. A default is mandatory unless future type
analysis can prove an exhaustive match. Exactly one arm executes.

Each `when.cel` program belongs to a typed predicate profile: its only root is
`scope`, every selected field must be statically readable, supported logical
and comparison operators must type-check, and the final result must be
boolean. Conjunctive scalar equality checks such as
`scope.result.status == 'ok'` narrow a discriminated union inside that case, so
the case may read `scope.result.value`. Narrowing is local to the selected case;
the compiler does not infer a type for `default` from the negation of preceding
cases. A default that needs a variant-only field must instead be expressed as
an explicit predicate-bearing case, with a final fallback.

Every normally completing arm returns a value compatible with the Switch output
schema; an arm may instead raise. The Switch itself binds one direct output.
There is no public Select node and no skipped/null output for unselected arms.

## Result, return, and raise

A child block has one declarative `result`:

```yaml
result:
  return: {from: steps.analyze.output}
```

or:

```yaml
result:
  raise: declared_error_id
```

The compiler lowers a child return to `RegionYield`; it is not a public node and
does not create `RunOutput`.

The Workflow root also has one `result`. A root return owns the public envelope:

```yaml
result:
  return:
    content: {from: steps.synthesize.output.data.answer}
    format: markdown
    data: {from: steps.synthesize.output.data}
```

Only this form can complete a Run successfully. `data` must satisfy
`output.data_schema`. `content`, when present, must evaluate to a string and
requires `format`.

Authored errors are declared once:

```yaml
errors:
  all_failed:
    category: workflow
    code: WORKFLOW_ALL_BRANCHES_FAILED
    public_message: No analysis branch produced usable evidence.
```

`raise: all_failed` cannot spoof operation, timeout, stop, ownership, or
infrastructure failure categories.

## Lexical scope and visibility

The compiler enforces:

1. `input` and safe `run` metadata are visible at the root.
2. A structured step evaluates its `with` bindings in the parent, then exposes
   only those immutable values as `scope` to child regions.
3. `steps` contains only earlier successful steps in the current body.
4. Branch and arm locals never escape their region.
5. Sibling branches and sibling arms cannot reference one another.
6. The only outward value of a child region is its declared result.
7. Every use is dominated by its definition on every reachable path.
8. Local step IDs are unique in one body; disjoint regions may reuse local IDs.
9. Every operation, block, and result has a resolved schema.
10. A discriminated result must be narrowed before variant-only fields are read.

Qualified authored paths, not list indexes, provide durable identity, for
example:

```text
/workflow/analyses/branches/technical/analyze
/workflow/synthesis_input/cases/complete
```

## Error, cancellation, and completion

Settleable errors are authored workflow errors, leaf-operation failures,
operation timeouts, and output-contract failures. Stop, interruption,
ownership loss, persistence loss, task panic, and other infrastructure failures
are never collected as business data.

RunService computes the Run timeout once as an absolute execution deadline and
passes it down unchanged. Each leaf attempt uses the earlier of that deadline
and its own operation deadline, including through `ExecutionControl.remaining`.
When either boundary fires, the attempt receives typed stop and may drain within
the independent operation-cancel grace period. Cleanup and authoritative
terminal persistence may therefore finish after the execution deadline; the
deadline is not a total request-wall-clock bound. External stop and internal
sibling cancellation use the same bounded drain contract. Template syntax and
typed CEL predicates are compiled during lowering and verified again on IR
ingestion. Template output is bounded while rendering.

When a scope must stop children it:

1. closes child admission;
2. requests cooperative cancellation;
3. drains every admitted task;
4. publishes the scope outcome or propagates failure.

No detached workflow task may outlive its parent scope.

Workflow success is not "a node reached End". Success becomes eligible only
after the root return exists, every descendant has completed or been cancelled
and drained, output contracts and limits pass, and the authoritative terminal
transaction commits. The durable terminal commit is the completion
linearization point.

## Typed Region/SSA IR

The minimum IR contains:

- `Region` with kind, parameters, result schema, operations, and terminator;
- `Call` for one registered leaf operation;
- `Parallel` owning child branch regions;
- `Branch` owning ordered Switch arm regions;
- `Phi` or block argument for mutually exclusive arm results;
- `RegionYield` for child success;
- `WorkflowReturn` for root success;
- `Raise` for typed failure.

The verifier rejects:

- duplicate stable region/operation/value identities;
- missing or wrong-scope terminators;
- unknown or use-before-definition values;
- cross-region value escape;
- child captures not declared as operation inputs;
- parameter/result schema mismatch;
- malformed Parallel, Branch, or Phi structures.

Parallel lowers to spawnable child regions and a typed barrier. Switch lowers to
one ordered decision, arm regions, and one result merge. These instructions are
runtime concepts, not authorable node types.

## Runtime scope tree

The Scheduler executes the verified IR as a hierarchy:

```text
RunScope
  Sequence
    OperationScope
    ParallelScope
      BranchScope
      BranchScope
    SwitchScope
      SelectedArmScope
```

Each scope has a stable qualified ID, parent, immutable captures, local values,
deadline, cancellation token, admitted children, and pending result/error.
Nested Parallel and Switch blocks are ordinary recursive scopes; there is no
singleton `active_fork` state.

## Leaf operations and public boundaries

Only leaf operations are registered extensions. Chat and Action migrate to the
new operation descriptor/executor contract.

Action uses one explicit typed input:

```yaml
- kind: operation
  id: now
  uses: action.call
  with:
    input:
      object:
        timezone: {literal: Asia/Shanghai}
  config:
    action: current_time
```

The action descriptor supplies input/output schemas, effect, idempotency, and
streaming metadata. Each call receives a qualified operation ID, attempt ID,
and an idempotency key stable across attempts.

Chat separates authored instructions from runtime data in its config:

```yaml
- kind: operation
  id: analyze
  uses: ai.chat
  with:
    question: {from: scope.question}
  config:
    model: general_chat
    messages:
      - role: system
        parts:
          - {kind: prompt, prompt: technical_system}
      - role: user
        parts:
          - {kind: text, text: "Analyze the following question."}
          - {kind: data, input: question}
    parameters: {temperature: 0.2}
    response:
      format: json
      schema: {$ref: "#/$defs/Perspective"}
```

Chat distinguishes authored instruction parts from runtime data parts. System
messages accept only authored instruction sources. Input, model output, and
external Action output are untrusted data by default. Structured response mode
declares a JSON Schema and validates the completed model result before binding
the operation output. Runtime data parts are permitted only in user messages,
are labelled and JSON-encoded, and must name a `with` binding. Chat returns a
stable `{data, finish_reason, usage}` object; `data` is a string in text mode or
the validated JSON value in structured mode.

Action receives a fully evaluated typed input, validates it, executes with a
stable operation/attempt identity, and validates output. Automatic retry is
legal only for descriptors declaring idempotency and only with a stable
idempotency key.

Leaf Operations have no content-emission contract. Provider model streams are
aggregated and validated inside `ai.chat`; Operation outputs are not public
events. Public `operation.completed` events contain status, type, timing,
attempt, and output-size metadata. Intermediate values remain runtime-local,
private diagnostics enter neither public persistence nor default logs, and
model data parts require explicit ValueExpr projection. Only the workflow root
result enters durable Run history.

## Repository cutover

The completed cutover has one canonical vNext parser/compiler/runtime path.
Checked-in Agents use structured syntax. Public registrations and executors for
the removed control nodes are deleted. Layout stays separate. The Formal v2
history migration is a clean replacement: V1 stores must be discarded or
exported externally, and no V1 document or database is silently interpreted as
vNext.

## Acceptance for implementation stages 1-7

### 1. Language semantics

- This specification and the implementation agree on every field and variant.
- Strict parser tests cover the canonical document and all negative grammar
  boundaries.
- Removed graph/control fields and kinds are rejected.

### 2. Value, type, error, and scope contracts

- All JSON kinds pass through `literal/from/object/array` without coercion.
- Templates see only explicit bindings.
- Full paths, dominance, scope boundaries, discriminated unions, declared
  errors, input schema, block schemas, and public output schema are validated.

### 3. Region/SSA IR

- Valid nested regions pass the verifier.
- Missing/wrong terminators, duplicate identities, undefined/forward values,
  cross-region escape, invalid captures, type mismatch, malformed Parallel,
  Branch, and Phi fail before runtime.

### 4. Lowering

- Golden tests prove deterministic qualified IDs and stable normalized plans.
- Operation, Parallel, Switch, child result, root return, and raise lower to the
  corresponding typed IR without authored control nodes.

### 5. Scope runtime

- Nested all/all-settled Parallel and ordered Switch execute correctly.
- Partial/all failure, cancellation, timeout, stop, and infrastructure paths
  obey collection and drain contracts.
- Root completion has one durable terminal outcome and no surviving children.

### 6. Chat, Action, and event boundaries

- Chat instruction/data provenance, aggregate request limits, structured
  response validation, and streaming completion are tested.
- Action input/output schemas, idempotency metadata, and cancellation are
  tested.
- Public events do not expose operation values or internal diagnostics by
  default.

### 7. Agent and repository cutover

- `parallel_researcher` uses distinct technical/risk prompts and vNext
  Parallel/Switch/result syntax.
- Full, partial, zero-success, cancellation, and output-schema matrices execute
  through the production compiler and runtime.
- Every checked-in Agent, README example, and active design document uses the
  canonical syntax.
- Formatting, Clippy with warnings denied, all targets/tests, binary smoke,
  SQLite/PostgreSQL integration, dependency audit, and deny checks pass from the
  final worktree.
