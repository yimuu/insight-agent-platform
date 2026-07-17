# Semantic Compile-Time Validation Implementation Plan

> **Historical / superseded:** this plan targets the removed graph/reference scanner and its YAML fixtures. Current typed authored validation is defined by [DSL Authoring Surface Redesign](../specs/2026-07-17-dsl-authoring-surface-redesign.md) and executable compiler tests.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close `BASE-P1-001`, `BASE-P1-002`, and `BASE-P1-003` by making Action static input validation and DSL node-reference discovery fail before serving traffic.

**Architecture:** Add one focused DSL reference-policy module and route Handlebars/CEL reference extraction through it. Keep `CompiledNode.references` and the existing graph validator as the legality boundary, while tightening the reference grammar and static Action validation before graph validation.

**Tech Stack:** Rust 2021, Handlebars 6 AST, `cel-interpreter` 0.10 runtime execution, direct `cel-parser` 0.10 AST inspection, `jsonschema` 0.18, existing Rust integration tests.

## Global Constraints

- Canonical node-output references use exactly `nodes.<node_id>.output`.
- Node IDs and fork branch IDs use `[A-Za-z_][A-Za-z0-9_]*`.
- Non-canonical access rooted at `nodes` fails compilation before graph validation.
- Runtime Action input/output validation remains mandatory.
- Fully static `core.action.input` values fail Agent compilation with `ACTION_INPUT_INVALID` and message `action input validation failed` when invalid.
- Partially templated Action inputs continue to compile and remain runtime-validated.
- Public HTTP, SSE, Run, repository, history, migration, and event-envelope shapes do not change.
- CEL execution engine stays `cel-interpreter`; A5 only adds direct `cel-parser` AST inspection.
- Each task ends with a commit.
- Execute implementation from an isolated worktree created with `superpowers:using-git-worktrees`.

---

## File Structure

- Create `src/dsl/references.rs`
  - Owns DSL identifier validation.
  - Owns canonical node-output path classification.
  - Owns Handlebars AST reference extraction.
  - Owns CEL AST reference extraction.
- Modify `src/dsl/mod.rs`
  - Exposes `references` as `pub(crate)` only.
- Modify `src/dsl/compiler.rs`
  - Removes raw regex scanning.
  - Compiles Handlebars templates through `Template::compile`.
  - Validates node IDs before node-specific compilation.
  - Stores static template metadata in `TemplateProgram`.
- Modify `src/nodes/template.rs`
  - Adds `CompiledTemplateValue::static_value`.
  - Preserves recursive rendering and reference aggregation.
- Modify `src/nodes/action.rs`
  - Validates fully static Action input during node compilation.
- Modify `src/nodes/condition.rs`
  - Uses parser-aware CEL reference extraction.
- Modify `src/nodes/fork.rs`
  - Uses shared DSL identifier validation for branch IDs.
- Modify `Cargo.toml` and `Cargo.lock`
  - Adds direct `cel-parser`.
  - Removes direct `regex` when raw scanning is gone.
- Modify tests:
  - `tests/core_template_condition.rs`
  - `tests/core_chat_action.rs`
  - `tests/dsl_compiler.rs`
  - `tests/dsl_parallel.rs`
  - `tests/repository_agents_v1.rs`
- Modify docs:
  - `README.md`
  - `docs/formal-v1-breaking-changes.md`

---

### Task 1: Add the DSL reference-policy foundation

**Files:**
- Create: `src/dsl/references.rs`
- Modify: `src/dsl/mod.rs`

**Interfaces:**
- Produces: `pub(crate) fn is_dsl_identifier(value: &str) -> bool`
- Produces: `pub(crate) fn validate_node_id(node_id: &str) -> Result<(), CompileError>`
- Produces: `pub(crate) enum NodeReferencePath { None, Reference(String), Invalid }`
- Produces: `pub(crate) fn classify_node_reference_path(path: &str) -> NodeReferencePath`

- [ ] **Step 1: Write failing unit tests for identifier and canonical path classification**

Create `src/dsl/references.rs` with these tests and minimal imports only:

```rust
use crate::dsl::CompileError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NodeReferencePath {
    None,
    Reference(String),
    Invalid,
}

#[cfg(test)]
mod tests {
    use super::{classify_node_reference_path, is_dsl_identifier, NodeReferencePath};

    #[test]
    fn validates_dsl_identifiers() {
        for value in ["node", "node_1", "_start", "A0"] {
            assert!(is_dsl_identifier(value), "{value} should be valid");
        }
        for value in ["", "1node", "node-name", "node.name", "node name", "节点"] {
            assert!(!is_dsl_identifier(value), "{value} should be invalid");
        }
    }

    #[test]
    fn classifies_canonical_node_output_paths() {
        assert_eq!(
            classify_node_reference_path("nodes.prepare.output"),
            NodeReferencePath::Reference("prepare".to_string())
        );
        assert_eq!(
            classify_node_reference_path("nodes.prepare.output.text"),
            NodeReferencePath::Reference("prepare".to_string())
        );
        assert_eq!(
            classify_node_reference_path("input.question"),
            NodeReferencePath::None
        );
    }

    #[test]
    fn rejects_non_canonical_nodes_paths() {
        for value in [
            "nodes",
            "nodes.prepare",
            "nodes.prepare.value",
            "nodes.prepare[\"output\"]",
            "nodes[\"prepare\"].output",
            "nodes.prepare-name.output",
        ] {
            assert_eq!(
                classify_node_reference_path(value),
                NodeReferencePath::Invalid,
                "{value} should be invalid"
            );
        }
    }
}
```

Add the module declaration in `src/dsl/mod.rs`:

```rust
pub(crate) mod references;
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test references --lib --quiet
```

Expected: FAIL because `is_dsl_identifier`, `classify_node_reference_path`, and `validate_node_id` are not implemented.

- [ ] **Step 3: Implement the reference-policy foundation**

Replace the top of `src/dsl/references.rs` with this implementation, keeping the tests from Step 1 below it:

```rust
use crate::dsl::CompileError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NodeReferencePath {
    None,
    Reference(String),
    Invalid,
}

pub(crate) fn is_dsl_identifier(value: &str) -> bool {
    let mut characters = value.chars();
    let Some(first) = characters.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

pub(crate) fn validate_node_id(node_id: &str) -> Result<(), CompileError> {
    if is_dsl_identifier(node_id) {
        return Ok(());
    }
    Err(CompileError::new(
        "NODE_ID_INVALID",
        format!("node id '{node_id}' must match [A-Za-z_][A-Za-z0-9_]*"),
    ))
}

pub(crate) fn classify_node_reference_path(path: &str) -> NodeReferencePath {
    if path == "nodes" || path.starts_with("nodes[") {
        return NodeReferencePath::Invalid;
    }
    let Some(rest) = path.strip_prefix("nodes.") else {
        return NodeReferencePath::None;
    };
    let mut parts = rest.split('.');
    let Some(node_id) = parts.next() else {
        return NodeReferencePath::Invalid;
    };
    let Some(output) = parts.next() else {
        return NodeReferencePath::Invalid;
    };
    if !is_dsl_identifier(node_id) || output != "output" {
        return NodeReferencePath::Invalid;
    }
    NodeReferencePath::Reference(node_id.to_string())
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test references --lib --quiet
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dsl/mod.rs src/dsl/references.rs
git commit -m "feat: add DSL reference policy"
```

---

### Task 2: Replace Handlebars raw scanning with AST extraction

**Files:**
- Modify: `src/dsl/references.rs`
- Modify: `src/dsl/compiler.rs`
- Modify: `tests/core_template_condition.rs`

**Interfaces:**
- Consumes: `classify_node_reference_path(path: &str) -> NodeReferencePath`
- Produces: `pub(crate) fn extract_handlebars_references(template: &handlebars::Template, owner: &str, field: &str) -> Result<BTreeSet<String>, CompileError>`
- Produces: `pub(crate) fn handlebars_static_text(template: &handlebars::Template) -> Option<String>`
- Modifies: `TemplateProgram { name: String, references: BTreeSet<String>, static_value: Option<String> }`

- [ ] **Step 1: Write failing Handlebars extraction tests**

Append these tests to `tests/core_template_condition.rs`:

```rust
#[test]
fn template_reference_extraction_ignores_inert_handlebars_syntax() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);

    let compilation = TemplateNode
        .compile(
            "render",
            json!({
                "value": {
                    "plain": "literal nodes.future.output.text",
                    "comment": "{{!-- nodes.future.output.text --}}visible",
                    "escaped": "\\{{ nodes.future.output.text }}",
                    "real": "{{ nodes.prepare.output.text }}",
                    "helper": "{{#if nodes.ready.output.flag}}{{ nodes.prepare.output.text }}{{/if}}"
                }
            }),
            &mut compile_context,
        )
        .unwrap();

    assert_eq!(
        compilation.references,
        BTreeSet::from(["prepare".to_string(), "ready".to_string()])
    );
}

#[test]
fn template_reference_extraction_rejects_non_canonical_nodes_paths() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);

    let error = TemplateNode
        .compile(
            "render",
            json!({"value": "{{ nodes.prepare }}"}),
            &mut compile_context,
        )
        .unwrap_err();

    assert_eq!(error.code(), "TEMPLATE_REFERENCE_INVALID");
}
```

- [ ] **Step 2: Run tests to verify they fail under raw scanning**

Run:

```bash
cargo test --test core_template_condition template_reference_extraction --quiet
```

Expected: FAIL because raw scanning incorrectly records `future` from inert template text and does not reject `{{ nodes.prepare }}`.

- [ ] **Step 3: Implement Handlebars AST extraction**

In `src/dsl/references.rs`, add these imports and functions:

```rust
use std::collections::BTreeSet;

use handlebars::{
    template::{HelperTemplate, Parameter, TemplateElement},
    Template,
};

pub(crate) fn extract_handlebars_references(
    template: &Template,
    owner: &str,
    field: &str,
) -> Result<BTreeSet<String>, CompileError> {
    let mut references = BTreeSet::new();
    for element in &template.elements {
        collect_handlebars_element(element, &mut references, owner, field)?;
    }
    Ok(references)
}

pub(crate) fn handlebars_static_text(template: &Template) -> Option<String> {
    let mut output = String::new();
    for element in &template.elements {
        match element {
            TemplateElement::RawString(value) => output.push_str(value),
            TemplateElement::Comment(_) => {}
            _ => return None,
        }
    }
    Some(output)
}

fn collect_handlebars_element(
    element: &TemplateElement,
    references: &mut BTreeSet<String>,
    owner: &str,
    field: &str,
) -> Result<(), CompileError> {
    match element {
        TemplateElement::RawString(_) | TemplateElement::Comment(_) => Ok(()),
        TemplateElement::Expression(template)
        | TemplateElement::HtmlExpression(template)
        | TemplateElement::HelperBlock(template) => {
            collect_handlebars_helper(template, references, owner, field)
        }
        TemplateElement::DecoratorExpression(template)
        | TemplateElement::DecoratorBlock(template)
        | TemplateElement::PartialExpression(template)
        | TemplateElement::PartialBlock(template) => {
            collect_handlebars_parameter(&template.name, references, owner, field)?;
            for parameter in &template.params {
                collect_handlebars_parameter(parameter, references, owner, field)?;
            }
            for parameter in template.hash.values() {
                collect_handlebars_parameter(parameter, references, owner, field)?;
            }
            if let Some(template) = &template.template {
                for element in &template.elements {
                    collect_handlebars_element(element, references, owner, field)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn collect_handlebars_helper(
    template: &HelperTemplate,
    references: &mut BTreeSet<String>,
    owner: &str,
    field: &str,
) -> Result<(), CompileError> {
    collect_handlebars_parameter(&template.name, references, owner, field)?;
    for parameter in &template.params {
        collect_handlebars_parameter(parameter, references, owner, field)?;
    }
    for parameter in template.hash.values() {
        collect_handlebars_parameter(parameter, references, owner, field)?;
    }
    if let Some(template) = &template.template {
        for element in &template.elements {
            collect_handlebars_element(element, references, owner, field)?;
        }
    }
    if let Some(template) = &template.inverse {
        for element in &template.elements {
            collect_handlebars_element(element, references, owner, field)?;
        }
    }
    Ok(())
}

fn collect_handlebars_parameter(
    parameter: &Parameter,
    references: &mut BTreeSet<String>,
    owner: &str,
    field: &str,
) -> Result<(), CompileError> {
    match parameter {
        Parameter::Name(name) => collect_handlebars_path(name, references, owner, field),
        Parameter::Path(path) => collect_handlebars_path(path.raw(), references, owner, field),
        Parameter::Literal(_) => Ok(()),
        Parameter::Subexpression(expression) => {
            collect_handlebars_element(&expression.element, references, owner, field)
        }
        _ => Ok(()),
    }
}

fn collect_handlebars_path(
    path: &str,
    references: &mut BTreeSet<String>,
    owner: &str,
    field: &str,
) -> Result<(), CompileError> {
    match classify_node_reference_path(path) {
        NodeReferencePath::None => Ok(()),
        NodeReferencePath::Reference(node_id) => {
            references.insert(node_id);
            Ok(())
        }
        NodeReferencePath::Invalid => Err(CompileError::new(
            "TEMPLATE_REFERENCE_INVALID",
            format!("template '{owner}.{field}' must use nodes.<node_id>.output references"),
        )),
    }
}
```

In `src/dsl/compiler.rs`:

- Remove `OnceLock`, `regex::Regex`, and `node_references`.
- Import `handlebars::Template`.
- Import `extract_handlebars_references` and `handlebars_static_text`.
- Change `TemplateProgram` to:

```rust
pub struct TemplateProgram {
    pub name: String,
    pub references: BTreeSet<String>,
    pub static_value: Option<String>,
}
```

- Change `compile_inline_template` to compile once, extract references, and register the compiled template:

```rust
let template = Template::compile(source).map_err(|error| {
    CompileError::new(
        "TEMPLATE_INVALID",
        format!("invalid template '{owner}.{field}': {error}"),
    )
})?;
let references = extract_handlebars_references(&template, owner, field)?;
let static_value = handlebars_static_text(&template);
self.templates.register_template(&name, template);
Ok(TemplateProgram {
    name,
    references,
    static_value,
})
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
cargo test --test core_template_condition template_reference_extraction --quiet
```

Expected: PASS.

- [ ] **Step 5: Run existing template/compiler reference tests**

Run:

```bash
cargo test --test core_template_condition --test dsl_compiler --test dsl_parallel --quiet
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/dsl/references.rs src/dsl/compiler.rs tests/core_template_condition.rs
git commit -m "fix: extract Handlebars references from AST"
```

---

### Task 3: Replace CEL raw scanning with AST extraction and reject indexed node access

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/dsl/references.rs`
- Modify: `src/nodes/condition.rs`
- Modify: `tests/core_template_condition.rs`
- Modify: `tests/dsl_parallel.rs`

**Interfaces:**
- Consumes: `classify_node_reference_path(path: &str) -> NodeReferencePath`
- Produces: `pub(crate) fn extract_cel_references(expression: &cel_parser::Expression, node_id: &str, case_index: usize) -> Result<BTreeSet<String>, CompileError>`

- [ ] **Step 1: Add direct parser dependency**

In `Cargo.toml`, add:

```toml
cel-parser = "0.10.1"
```

Keep `cel-interpreter = "0.10.0"` unchanged.

- [ ] **Step 2: Write failing CEL extraction and rejection tests**

Append these tests to `tests/core_template_condition.rs`:

```rust
#[test]
fn condition_reference_extraction_ignores_cel_string_literals() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut context = CompileContext::new(&models, &actions);

    let compilation = ConditionNode
        .compile(
            "route",
            json!({
                "cases": [{
                    "when": "nodes.prepare.output.kind == 'ready' && 'nodes.future.output' == 'nodes.future.output' && size(nodes.prepare.output.items) >= 0",
                    "next": "done"
                }],
                "default": "done"
            }),
            &mut context,
        )
        .unwrap();

    assert_eq!(
        compilation.references,
        BTreeSet::from(["prepare".to_string()])
    );
}

#[test]
fn condition_rejects_non_canonical_nodes_access() {
    let cases = [
        "nodes[\"prepare\"].output == true",
        "nodes[id].output == true",
        "nodes.prepare[\"output\"] == true",
        "nodes.prepare == true",
        "nodes == {}",
    ];

    for expression in cases {
        let models = ModelRegistry::default();
        let actions = ActionRegistry::default();
        let mut context = CompileContext::new(&models, &actions);
        let error = ConditionNode
            .compile(
                "route",
                json!({
                    "cases": [{"when": expression, "next": "done"}],
                    "default": "done"
                }),
                &mut context,
            )
            .unwrap_err();

        assert_eq!(
            error.code(),
            "CONDITION_REFERENCE_INVALID",
            "{expression} should be rejected"
        );
    }
}
```

Append this test to `tests/dsl_parallel.rs`:

```rust
#[test]
fn condition_indexed_node_access_fails_before_branch_validation() {
    let yaml = valid_parallel_yaml_with(
        "summarize_a",
        "{{ nodes.search_a.output.text }}"
    )
    .replace(
        "  search_b:\n    type: core.condition\n    config:\n      cases:\n        - when: \"true\"\n          next: summarize_b\n      default: collect",
        "  search_b:\n    type: core.condition\n    config:\n      cases:\n        - when: 'nodes[\"search_a\"].output.text == \"x\"'\n          next: summarize_b\n      default: collect",
    );
    assert_compile_error(&yaml, "CONDITION_REFERENCE_INVALID");
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
cargo test --test core_template_condition condition_reference_extraction_ignores_cel_string_literals --quiet
cargo test --test core_template_condition condition_rejects_non_canonical_nodes_access --quiet
cargo test --test dsl_parallel condition_indexed_node_access_fails_before_branch_validation --quiet
```

Expected: FAIL because `ConditionNode` still calls the removed/raw reference scanner and does not use CEL AST policy.

- [ ] **Step 4: Implement CEL AST extraction**

In `src/dsl/references.rs`, add:

```rust
use cel_parser::{
    ast::{operators, Expr, IdedExpr},
    Expression,
};

pub(crate) fn extract_cel_references(
    expression: &Expression,
    node_id: &str,
    case_index: usize,
) -> Result<BTreeSet<String>, CompileError> {
    let mut references = BTreeSet::new();
    collect_cel_expression(expression, &mut references, node_id, case_index)?;
    Ok(references)
}

fn collect_cel_expression(
    expression: &IdedExpr,
    references: &mut BTreeSet<String>,
    node_id: &str,
    case_index: usize,
) -> Result<(), CompileError> {
    if let Some(path) = cel_select_path(expression) {
        return collect_cel_path(&path, references, node_id, case_index);
    }

    match &expression.expr {
        Expr::Unspecified | Expr::Literal(_) => Ok(()),
        Expr::Ident(name) if name == "nodes" => invalid_cel_reference(node_id, case_index),
        Expr::Ident(_) => Ok(()),
        Expr::Select(select) => {
            collect_cel_expression(&select.operand, references, node_id, case_index)
        }
        Expr::Call(call) => {
            if matches!(call.func_name.as_str(), operators::INDEX | operators::OPT_INDEX) {
                if let Some(target) = &call.target {
                    if let Some(path) = cel_select_path(target) {
                        collect_cel_path(&path, references, node_id, case_index)?;
                    } else if cel_expression_has_nodes_root(target) {
                        return invalid_cel_reference(node_id, case_index);
                    } else {
                        collect_cel_expression(target, references, node_id, case_index)?;
                    }
                }
            } else if let Some(target) = &call.target {
                collect_cel_expression(target, references, node_id, case_index)?;
            }
            for argument in &call.args {
                collect_cel_expression(argument, references, node_id, case_index)?;
            }
            Ok(())
        }
        Expr::List(list) => {
            for element in &list.elements {
                collect_cel_expression(element, references, node_id, case_index)?;
            }
            Ok(())
        }
        Expr::Map(map) => {
            for entry in &map.entries {
                match &entry.expr {
                    cel_parser::ast::EntryExpr::StructField(field) => {
                        collect_cel_expression(&field.value, references, node_id, case_index)?;
                    }
                    cel_parser::ast::EntryExpr::MapEntry(entry) => {
                        collect_cel_expression(&entry.key, references, node_id, case_index)?;
                        collect_cel_expression(&entry.value, references, node_id, case_index)?;
                    }
                }
            }
            Ok(())
        }
        Expr::Struct(structure) => {
            for entry in &structure.entries {
                match &entry.expr {
                    cel_parser::ast::EntryExpr::StructField(field) => {
                        collect_cel_expression(&field.value, references, node_id, case_index)?;
                    }
                    cel_parser::ast::EntryExpr::MapEntry(entry) => {
                        collect_cel_expression(&entry.key, references, node_id, case_index)?;
                        collect_cel_expression(&entry.value, references, node_id, case_index)?;
                    }
                }
            }
            Ok(())
        }
        Expr::Comprehension(comprehension) => {
            collect_cel_expression(&comprehension.iter_range, references, node_id, case_index)?;
            collect_cel_expression(&comprehension.accu_init, references, node_id, case_index)?;
            collect_cel_expression(&comprehension.loop_cond, references, node_id, case_index)?;
            collect_cel_expression(&comprehension.loop_step, references, node_id, case_index)?;
            collect_cel_expression(&comprehension.result, references, node_id, case_index)
        }
    }
}

fn cel_select_path(expression: &IdedExpr) -> Option<Vec<String>> {
    match &expression.expr {
        Expr::Ident(name) => Some(vec![name.clone()]),
        Expr::Select(select) => {
            let mut path = cel_select_path(&select.operand)?;
            path.push(select.field.clone());
            Some(path)
        }
        _ => None,
    }
}

fn collect_cel_path(
    path: &[String],
    references: &mut BTreeSet<String>,
    node_id: &str,
    case_index: usize,
) -> Result<(), CompileError> {
    if path.first().map(String::as_str) != Some("nodes") {
        return Ok(());
    }
    if path.len() >= 3 && is_dsl_identifier(&path[1]) && path[2] == "output" {
        references.insert(path[1].clone());
        return Ok(());
    }
    invalid_cel_reference(node_id, case_index)
}

fn cel_expression_has_nodes_root(expression: &IdedExpr) -> bool {
    cel_select_path(expression)
        .as_ref()
        .and_then(|path| path.first())
        .map(String::as_str)
        == Some("nodes")
}

fn invalid_cel_reference<T>(node_id: &str, case_index: usize) -> Result<T, CompileError> {
    Err(CompileError::new(
        "CONDITION_REFERENCE_INVALID",
        format!(
            "condition node '{node_id}' case {case_index} must use nodes.<node_id>.output references"
        ),
    ))
}
```

In `src/nodes/condition.rs`:

- Remove `node_references`.
- Import `cel_parser::Parser`.
- Import `extract_cel_references`.
- For each case, parse and extract before compiling the runtime program:

```rust
let parser = Parser::default();
let parsed = parser.parse(&expression).map_err(|error| {
    CompileError::new(
        "CONDITION_EXPRESSION_INVALID",
        format!("condition node '{node_id}' case {index} has invalid CEL expression: {error}"),
    )
})?;
references.extend(extract_cel_references(&parsed, node_id, index)?);
let program = CelProgram::compile(&expression).map_err(|error| {
    CompileError::new(
        "CONDITION_EXPRESSION_INVALID",
        format!("condition node '{node_id}' case {index} has invalid CEL expression: {error}"),
    )
})?;
```

- [ ] **Step 5: Run focused CEL tests**

Run:

```bash
cargo test --test core_template_condition condition_reference_extraction_ignores_cel_string_literals --quiet
cargo test --test core_template_condition condition_rejects_non_canonical_nodes_access --quiet
cargo test --test dsl_parallel condition_indexed_node_access_fails_before_branch_validation --quiet
```

Expected: PASS.

- [ ] **Step 6: Run full condition and parallel suites**

Run:

```bash
cargo test --test core_template_condition --test dsl_parallel --quiet
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock src/dsl/references.rs src/nodes/condition.rs tests/core_template_condition.rs tests/dsl_parallel.rs
git commit -m "fix: validate CEL node references semantically"
```

---

### Task 4: Enforce DSL identifiers for node IDs and fork branch IDs

**Files:**
- Modify: `src/dsl/compiler.rs`
- Modify: `src/nodes/fork.rs`
- Modify: `tests/dsl_compiler.rs`
- Modify: `tests/dsl_parallel.rs`

**Interfaces:**
- Consumes: `validate_node_id(node_id: &str) -> Result<(), CompileError>`
- Consumes: `is_dsl_identifier(value: &str) -> bool`

- [ ] **Step 1: Write failing identifier validation tests**

Append this test to `tests/dsl_compiler.rs`:

```rust
#[test]
fn rejects_node_ids_outside_canonical_identifier_grammar() {
    let invalid = valid_yaml().replace("  first:", "  first-node:");
    assert_compile_error(&invalid, "NODE_ID_INVALID");
}
```

Append this test to `tests/dsl_parallel.rs`:

```rust
#[test]
fn rejects_fork_branch_ids_outside_canonical_identifier_grammar() {
    let invalid = parallel_yaml().replace("source_a: search_a", "source-a: search_a");
    assert_compile_error(&invalid, "FORK_BRANCH_ID_INVALID");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --test dsl_compiler rejects_node_ids_outside_canonical_identifier_grammar --quiet
cargo test --test dsl_parallel rejects_fork_branch_ids_outside_canonical_identifier_grammar --quiet
```

Expected: FAIL because node IDs and fork branch IDs still allow hyphenated identifiers.

- [ ] **Step 3: Validate node IDs before node-specific compilation**

In `src/dsl/compiler.rs`, import:

```rust
use super::references::validate_node_id;
```

After `let raw = parse_raw_agent(&yaml)?;`, add:

```rust
for node_id in raw.nodes.keys() {
    validate_node_id(node_id)?;
}
```

- [ ] **Step 4: Use the shared identifier policy in `core.fork`**

In `src/nodes/fork.rs`, import:

```rust
use crate::dsl::references::is_dsl_identifier;
```

Replace:

```rust
if !valid_identifier(branch_id) {
```

with:

```rust
if !is_dsl_identifier(branch_id) {
```

Delete the local `valid_identifier` function at the bottom of `src/nodes/fork.rs`.

- [ ] **Step 5: Run focused tests**

Run:

```bash
cargo test --test dsl_compiler rejects_node_ids_outside_canonical_identifier_grammar --quiet
cargo test --test dsl_parallel rejects_fork_branch_ids_outside_canonical_identifier_grammar --quiet
```

Expected: PASS.

- [ ] **Step 6: Run graph and parallel suites**

Run:

```bash
cargo test --test dsl_compiler --test dsl_parallel --quiet
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/dsl/compiler.rs src/nodes/fork.rs tests/dsl_compiler.rs tests/dsl_parallel.rs
git commit -m "fix: enforce canonical DSL identifiers"
```

---

### Task 5: Validate fully static Action input at compile time

**Files:**
- Modify: `src/nodes/template.rs`
- Modify: `src/nodes/action.rs`
- Modify: `tests/core_chat_action.rs`

**Interfaces:**
- Consumes: `TemplateProgram::static_value: Option<String>`
- Produces: `impl CompiledTemplateValue { pub(crate) fn static_value(&self) -> Option<Value> }`

- [ ] **Step 1: Write failing Action static validation tests**

Append these tests to `tests/core_chat_action.rs` after the existing `EchoAction` definition:

```rust
#[test]
fn action_compile_validates_literal_input_against_registered_schema() {
    const STATIC_SECRET: &str = "static-action-input-never-expose";

    let mut actions = ActionRegistry::default();
    actions
        .register(EchoAction {
            calls: Arc::new(Mutex::new(Vec::new())),
            invalid_output: false,
        })
        .unwrap();
    let models = ModelRegistry::default();
    let mut context = CompileContext::new(&models, &actions);

    let error = ActionNode
        .compile(
            "echo",
            json!({
                "action": "echo",
                "input": {"payload": STATIC_SECRET}
            }),
            &mut context,
        )
        .unwrap_err();

    assert_eq!(error.code(), "ACTION_INPUT_INVALID");
    assert_eq!(error.to_string(), "action input validation failed");
    assert!(!format!("{error:?} {error}").contains(STATIC_SECRET));
}

#[test]
fn action_compile_allows_valid_literal_and_dynamic_inputs() {
    let mut actions = ActionRegistry::default();
    actions
        .register(EchoAction {
            calls: Arc::new(Mutex::new(Vec::new())),
            invalid_output: false,
        })
        .unwrap();
    let models = ModelRegistry::default();

    let mut literal_context = CompileContext::new(&models, &actions);
    ActionNode
        .compile(
            "literal",
            json!({
                "action": "echo",
                "input": {"payload": {"text": "static"}}
            }),
            &mut literal_context,
        )
        .unwrap();

    let mut dynamic_context = CompileContext::new(&models, &actions);
    ActionNode
        .compile(
            "dynamic",
            json!({
                "action": "echo",
                "input": {"payload": "{{ input.payload }}"}
            }),
            &mut dynamic_context,
        )
        .unwrap();
}
```

- [ ] **Step 2: Run tests to verify the invalid literal still compiles**

Run:

```bash
cargo test --test core_chat_action action_compile_validates_literal_input_against_registered_schema --quiet
cargo test --test core_chat_action action_compile_allows_valid_literal_and_dynamic_inputs --quiet
```

Expected: FAIL because the invalid literal Action input currently compiles.

- [ ] **Step 3: Add static value propagation to template values**

In `src/nodes/template.rs`, add this method to `impl CompiledTemplateValue`:

```rust
pub(crate) fn static_value(&self) -> Option<Value> {
    match self {
        Self::String(program) => program.static_value.clone().map(Value::String),
        Self::Array(values) => values
            .iter()
            .map(Self::static_value)
            .collect::<Option<Vec<_>>>()
            .map(Value::Array),
        Self::Object(values) => values
            .iter()
            .map(|(key, value)| Some((key.clone(), value.static_value()?)))
            .collect::<Option<serde_json::Map<_, _>>>()
            .map(Value::Object),
        Self::Literal(value) => Some(value.clone()),
    }
}
```

- [ ] **Step 4: Validate static Action input during compilation**

In `src/nodes/action.rs`, after:

```rust
let input = CompiledTemplateValue::compile(config.input, node_id, "input", context)?;
```

add:

```rust
if let Some(static_input) = input.static_value() {
    action.validate_input(&static_input).map_err(|error| {
        CompileError::new(error.code(), error.message().to_string())
    })?;
}
```

Keep runtime execution unchanged so `RegisteredAction::call` still validates after rendering.

- [ ] **Step 5: Run focused Action compile tests**

Run:

```bash
cargo test --test core_chat_action action_compile_validates_literal_input_against_registered_schema --quiet
cargo test --test core_chat_action action_compile_allows_valid_literal_and_dynamic_inputs --quiet
```

Expected: PASS.

- [ ] **Step 6: Run full Action and resource validation suites**

Run:

```bash
cargo test --test core_chat_action --test resource_registries --test action_error_containment --quiet
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/nodes/template.rs src/nodes/action.rs tests/core_chat_action.rs
git commit -m "fix: validate static action input at compile time"
```

---

### Task 6: Update checked-in Agent and breaking-change documentation coverage

**Files:**
- Modify: `README.md`
- Modify: `docs/formal-v1-breaking-changes.md`
- Modify: `tests/repository_agents_v1.rs`

**Interfaces:**
- Consumes: tightened DSL rules from Tasks 1-5.

- [ ] **Step 1: Strengthen checked-in Agent assertions**

Modify `tests/repository_agents_v1.rs` to assert that checked-in Agents expose canonical references after compilation. Add these assertions near the end of `enabled_repository_agents_compile_through_production_registries`:

```rust
let code_demo = agents.get("code_node_demo").unwrap();
assert!(code_demo.nodes["render"].references.contains("analyze_text"));

let parallel = agents.get("parallel_researcher").unwrap();
assert!(parallel.nodes["synthesize"].references.contains("collect"));
assert!(!parallel.nodes["synthesize"].references.contains("analyze_a"));
assert!(!parallel.nodes["synthesize"].references.contains("normalize_a"));
```

- [ ] **Step 2: Run checked-in Agent test**

Run:

```bash
cargo test --test repository_agents_v1 --quiet
```

Expected: PASS. If this fails because a checked-in Agent violates the new grammar, update that Agent YAML or prompt to canonical `nodes.<node_id>.output` references and rerun this command.

- [ ] **Step 3: Document the tightened DSL reference contract in README**

In `README.md`, replace:

```markdown
`emit: none` 保持节点增量私有，`emit: content` 才发布 `content.delta`。模板上下文只暴露 `input`、`run` 和已完成的 `nodes.<id>.output`。
```

with:

```markdown
`emit: none` 保持节点增量私有，`emit: content` 才发布 `content.delta`。模板上下文只暴露 `input`、`run` 和已完成的 `nodes.<node_id>.output`。节点 ID 和 fork branch ID 必须匹配 `[A-Za-z_][A-Za-z0-9_]*`；跨节点引用只能使用 `nodes.<node_id>.output`，不能使用 `nodes["id"]`、computed access 或直接访问 `nodes` map。
```

In `README.md`, replace:

```markdown
A0 Action 校验错误安全修复不兼容既有 Run 历史；部署前按[正式 V1 破坏性变更中的 A0 重置流程](docs/formal-v1-breaking-changes.md#a0-action-validation-error-containment)停止服务并显式清空历史。应用不会自动删除数据。
```

with:

```markdown
A0 Action 校验错误安全修复不兼容既有 Run 历史；部署前按[正式 V1 破坏性变更中的 A0 重置流程](docs/formal-v1-breaking-changes.md#a0-action-validation-error-containment)停止服务并显式清空历史。A5 会让静态非法 Action input、hyphenated node/branch ID、indexed/computed `nodes` access 在启动编译期失败；迁移理由见[正式 V1 破坏性变更中的 A5 语义编译期校验](docs/formal-v1-breaking-changes.md#a5-semantic-compile-time-validation)。应用不会自动删除数据。
```

- [ ] **Step 4: Document A5 in formal breaking changes**

Append this section after the A0 section in `docs/formal-v1-breaking-changes.md`:

```markdown
### A5 Semantic compile-time validation

A5 收紧 Agent DSL 的编译期语义校验：

- fully static `core.action.config.input` 会在 Agent 编译期按注册 Action input schema 校验，失败码为 `ACTION_INPUT_INVALID`，消息固定为 `action input validation failed`；
- 节点 ID 和 fork branch ID 必须匹配 `[A-Za-z_][A-Za-z0-9_]*`；
- 跨节点引用只能使用 `nodes.<node_id>.output`；
- CEL 中的 `nodes["id"].output`、`nodes[id].output`、`nodes.<id>["output"]`、直接访问 `nodes` map 等形式会失败；
- Handlebars/CEL 字符串、注释、raw 文本中的 `nodes.<id>.output` 不再被误识别为图依赖。

这样做的原因是编译期图校验必须知道每个跨节点依赖的确定 node ID。computed/indexed access 依赖运行时值，无法可靠参与 predecessor、parallel branch 和 post-join 校验。静态非法 Action input 已经在部署前可知，延迟到用户请求时失败不符合 fail-before-serving 合同。

迁移方式：

```yaml
# 不再支持
route:
  type: core.condition
  config:
    cases:
      - when: 'nodes["classify"].output.kind == "medical"'
        next: medical
    default: general

# 使用 canonical dotted reference
route:
  type: core.condition
  config:
    cases:
      - when: 'nodes.classify.output.kind == "medical"'
        next: medical
    default: general
```

如果旧 Agent 使用 `some-node` 这类 ID，需要改为 `some_node` 并同步更新所有 `next`、fork branch、join output 和模板/CEL 引用。A5 不需要数据库 migration，也不需要重置 Run 历史；它只影响 Agent 启动编译。
```

- [ ] **Step 5: Run docs-adjacent tests**

Run:

```bash
cargo test --test repository_agents_v1 --test dsl_compiler --test dsl_parallel --quiet
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add README.md docs/formal-v1-breaking-changes.md tests/repository_agents_v1.rs
git commit -m "docs: document semantic compile-time validation"
```

---

### Task 7: Remove obsolete raw-scanning dependency and run final gates

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Verify: `src/dsl/compiler.rs`
- Verify: `src/dsl/references.rs`
- Verify: all tests

**Interfaces:**
- Consumes: parser-aware reference extraction from Tasks 2-3.

- [ ] **Step 1: Confirm raw regex scanner is gone**

Run:

```bash
rg -n 'node_references|Regex|regex::' src tests
```

Expected: no matches for `node_references`, `Regex`, or `regex::`. Matches for literal DSL examples such as `nodes.prepare.output` are acceptable.

- [ ] **Step 2: Remove direct `regex` dependency when unused**

If Step 1 shows no direct Rust use of `regex`, remove this line from `Cargo.toml`:

```toml
regex = "1"
```

Then run:

```bash
cargo test --test core_template_condition --quiet
```

Expected: PASS and `Cargo.lock` updated as needed. Transitive `regex` entries may remain because other crates use them.

- [ ] **Step 3: Run formatting**

Run:

```bash
cargo fmt --all -- --check
```

Expected: PASS. If it fails, run `cargo fmt --all`, inspect the diff, and rerun the check.

- [ ] **Step 4: Run Clippy**

Run:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: PASS.

- [ ] **Step 5: Run all Rust tests**

Run:

```bash
cargo test --all-targets --quiet
```

Expected: PASS.

- [ ] **Step 6: Run final semantic searches**

Run:

```bash
rg -n 'nodes\[|nodes\.\[|node_references|Regex|regex::' src tests agents README.md docs/formal-v1-breaking-changes.md
```

Expected:

- no production source use of raw scanning;
- remaining `nodes[` matches only in negative tests or migration documentation;
- checked-in Agents use canonical dotted references.

- [ ] **Step 7: Commit final cleanup**

```bash
git add Cargo.toml Cargo.lock src tests README.md docs/formal-v1-breaking-changes.md
git commit -m "chore: finalize semantic validation cleanup"
```

If there are no changes after Steps 1-6, skip the commit and record the verification output in the implementation handoff.

---

## Final Verification

Run after Task 7:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --quiet
git status --short --branch
```

Expected:

- formatting check passes;
- Clippy passes with warnings denied;
- all tests pass;
- working tree is clean except for intentional uncommitted review notes.

## Self-Review Checklist

- `BASE-P1-001`: covered by Task 5 and `core_chat_action` tests.
- `BASE-P1-002`: covered by Task 2, Task 3, and parser-aware false-positive tests.
- `BASE-P1-003`: covered by Task 3 indexed/computed CEL rejection tests.
- Canonical identifier grammar: covered by Task 1 and Task 4.
- Existing graph legality: covered by Task 2, Task 3, Task 4, and existing `dsl_parallel` suites.
- Checked-in Agents: covered by Task 6.
- Breaking-change reasons: covered by Task 6 docs.
- Dependency scope: `cel-parser` direct dependency only; CEL engine replacement remains outside A5.
