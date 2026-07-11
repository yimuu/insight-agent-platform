# Task 1 Implementation Report: Action Error Containment

## Outcome

Action input and output validation now returns only the existing fixed `RunError`
code/message pairs. Runtime JSON Schema validation no longer creates or formats
instance-bearing `ValidationError` values.

Commit subject: `fix: contain action validation errors`

## TDD Evidence

### RED: required focused command

Command:

```text
cargo test --test resource_registries --test core_chat_action -- --nocapture
```

Result: exit 101. Cargo reached the `core_chat_action` binary first and stopped
after the intended ActionNode regression failure:

```text
thread 'action_validation_errors_are_fixed_and_instance_free' panicked
assertion `left == right` failed
  left: "action input validation failed: \"rendered-input-never-expose\" is not of type \"object\""
 right: "action input validation failed"
test result: FAILED. 6 passed; 1 failed
```

This proved the current validator formatter leaked the rendered Action input
instance through the unchanged downstream `RunError`.

Because Cargo stopped after the first failing integration-test binary, the
direct registry binary was also run before the production change:

```text
cargo test --test resource_registries -- --nocapture
```

Result: exit 101. Both new direct tests failed for the intended reason:

```text
left: "action input validation failed: {\"secret\":\"never-expose-input\"} is not of type \"string\""
right: "action input validation failed"

left: "action input validation failed: \"keyword-matrix-never-expose\" is not of type \"integer\""
right: "action input validation failed"

test result: FAILED. 3 passed; 2 failed
```

### GREEN: required focused command

Command:

```text
cargo test --test resource_registries --test core_chat_action -- --nocapture
```

Result: exit 0. `core_chat_action` passed 7/7 and
`resource_registries` passed 5/5 (12/12 total). This includes the direct
input/output checks, five-keyword matrix, ActionNode boundary checks,
successful Action execution, cancellation, and streaming behavior.

### Source guard

Command:

```text
rg -n 'validator\.validate|ValidationError|error\.to_string\(\)|details\.join' \
  src/resources/actions.rs
```

Result: exit 1 with empty output, which is the expected no-match result. No
runtime instance formatter remains in the Action validation path.

### Full suite

Commands and results:

```text
cargo test
```

Exit 0: 178 passed, 0 failed.

```text
cargo test --all-targets --quiet
```

Exit 0: 178 passed, 0 failed.

Additional pre-commit checks:

```text
cargo fmt --check
git diff --check
```

Both exited 0 with no output.

## Files Changed

- `src/resources/actions.rs`: replaced runtime `ValidationError` iteration and
  formatting with `JSONSchema::is_valid` and the existing fixed `RunError`.
- `tests/resource_registries.rs`: added exact input/output code/message and
  Display/Debug secret-containment assertions plus the representative
  type/maxLength/pattern/enum/oneOf matrix.
- `tests/core_chat_action.rs`: added rendered-input and returned-output secret
  containment checks at the real ActionNode boundary and retained the
  not-called-on-invalid-input assertion.
- `.superpowers/sdd/task-1-report.md`: this implementation and verification
  report.

## Self-Review and Constraint Audit

- Exact public codes remain `ACTION_INPUT_INVALID` and
  `ACTION_OUTPUT_INVALID`.
- Exact public messages remain `action input validation failed` and
  `action output validation failed`.
- Runtime validation uses only a boolean decision; no instance-bearing
  validator error is formatted, logged, or retained.
- Schema-compilation error handling is unchanged.
- `RegisteredAction` and `ActionNode` interfaces and public error shapes are
  unchanged.
- No migrations, dependencies, configuration, Agent files, or A1-A8 work were
  changed.
- The production diff is limited to the planned `validate_json` body; there is
  no unrelated refactor.
- The implementation from the brief compiled without correction or deviation.
- Final diff review found no whitespace errors, formatting changes, secret
  values outside test fixtures, or files outside Task 1 scope.

Concerns: none.
