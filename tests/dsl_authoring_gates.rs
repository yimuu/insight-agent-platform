use std::{
    fs,
    path::{Path, PathBuf},
};

use insight_agent_platform::dsl::vnext::{validate_author_source, AUTHOR_PARSE_ERROR_CODE};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExampleKind {
    Canonical,
    Negative,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Entrypoint {
    Agent,
    Step,
    Messages,
    Content,
    Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExampleMarker {
    kind: ExampleKind,
    entrypoint: Entrypoint,
    expected_code: Option<String>,
}

#[test]
fn production_authored_surface_has_no_removed_source_symbols() {
    let root = repository_root().join("src");
    let mut rust_sources = Vec::new();
    collect_files_with_extension(&root, "rs", &mut rust_sources);
    rust_sources.sort();

    let forbidden_symbols = [
        "OperationStep",
        "Step::Operation",
        "RawStep::Operation",
        "ValueExpr::Prompt",
    ];
    let forbidden_public_constructors = [
        "pub fn with_extensions(",
        "pub async fn with_extensions(",
        "pub const fn with_extensions(",
        "pub unsafe fn with_extensions(",
    ];

    let mut violations = Vec::new();
    for path in rust_sources {
        let source = fs::read_to_string(&path).expect("production Rust source must be UTF-8");
        let whitespace_normalized = source.split_whitespace().collect::<Vec<_>>().join(" ");
        for symbol in forbidden_symbols {
            if source.contains(symbol) {
                violations.push(format!("{} contains {symbol}", display_path(&path)));
            }
        }
        for constructor in forbidden_public_constructors {
            let signature = constructor.trim_end_matches('(');
            if whitespace_normalized.contains(&format!("{signature}("))
                || whitespace_normalized.contains(&format!("{signature} ("))
            {
                violations.push(format!(
                    "{} exposes {}",
                    display_path(&path),
                    constructor.trim_end_matches('(')
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "removed authored-surface symbols re-entered production source:\n{}",
        violations.join("\n")
    );
}

#[test]
fn checked_in_agents_contain_no_removed_author_syntax() {
    for (path, document) in checked_in_agent_documents() {
        let root = document
            .as_object()
            .unwrap_or_else(|| panic!("{} must contain an object", display_path(&path)));
        for removed in ["schema_dialect", "$schema", "$defs", "input"] {
            assert!(
                !root.contains_key(removed),
                "{} retains removed top-level key {removed}",
                display_path(&path)
            );
        }
        assert!(
            root.get("output").is_some_and(Value::is_string),
            "{} must declare output as a type expression",
            display_path(&path)
        );

        let mut violations = Vec::new();
        collect_removed_author_syntax(&document, "$", &mut violations);
        assert!(
            violations.is_empty(),
            "{} retains removed author syntax:\n{}",
            display_path(&path),
            violations.join("\n")
        );

        let steps = document
            .pointer("/workflow/steps")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        assert_simplified_steps(&path, steps, "workflow.steps");
    }
}

#[test]
fn checked_in_agent_content_parts_are_closed_single_key_variants() {
    let mut content_part_count = 0usize;
    for (path, document) in checked_in_agent_documents() {
        let steps = document
            .pointer("/workflow/steps")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        inspect_message_parts(&path, steps, "workflow.steps", &mut content_part_count);
    }
    assert!(
        content_part_count > 0,
        "no authored content parts were gated"
    );
}

#[test]
fn checked_in_agents_use_one_platform_message_array_contract() {
    let mut dynamic_message_source_count = 0usize;
    let mut platform_message_input_count = 0usize;
    for (path, document) in checked_in_agent_documents() {
        let root = document.as_object().unwrap();
        if let Some(types) = root.get("types").and_then(Value::as_object) {
            for name in types.keys() {
                assert!(
                    name != "Message",
                    "{} redeclares platform message type {name}",
                    display_path(&path)
                );
            }
        }

        let inputs = root
            .get("inputs")
            .and_then(Value::as_object)
            .unwrap_or_else(|| panic!("{} must declare inputs", display_path(&path)));
        for (name, declaration) in inputs {
            if declared_type(declaration) == Some("Message[]") {
                platform_message_input_count += 1;
                assert_eq!(
                    declaration.get("default"),
                    Some(&json!([])),
                    "{} inputs.{name} must default to []",
                    display_path(&path)
                );
            }
        }

        let steps = document
            .pointer("/workflow/steps")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mut dynamic_sources = Vec::new();
        collect_dynamic_message_sources(steps, &mut dynamic_sources);
        if !dynamic_sources.is_empty() {
            assert!(
                inputs
                    .values()
                    .any(|declaration| declared_type(declaration) == Some("Message[]")),
                "{} expands a dynamic message source without a platform Message[] input",
                display_path(&path)
            );
        }
        dynamic_message_source_count += dynamic_sources.len();
    }
    assert!(
        dynamic_message_source_count > 0,
        "no dynamic Message[] expansion was gated"
    );
    assert!(
        platform_message_input_count > 0,
        "no platform Message[] input was gated"
    );
}

#[test]
fn removed_syntax_gate_allows_business_keys_and_legal_runtime_paths() {
    let allowed = json!({
        "bindings": {"label": "business data"},
        "payload": {"output": "business field"},
        "reference": "$result.output"
    });
    let mut violations = Vec::new();
    collect_removed_author_syntax(&allowed, "$", &mut violations);
    assert!(violations.is_empty(), "{violations:?}");

    let steps = json!([{
        "type": "action",
        "id": "business_keys",
        "call": "example.business_keys",
        "inputs": {
            "uses": "business data",
            "with": {"label": "business data"},
            "config": {"enabled": true}
        }
    }]);
    assert_simplified_steps(
        Path::new("synthetic-agent.yaml"),
        steps.as_array().unwrap(),
        "workflow.steps",
    );
}

#[test]
fn marked_active_markdown_examples_obey_their_entrypoint_contracts() {
    let root = repository_root();
    let mut markdown = vec![root.join("README.md")];
    collect_files_with_extension(&root.join("docs"), "md", &mut markdown);
    markdown.sort();

    let mut canonical_count = 0usize;
    let mut negative_count = 0usize;
    for path in markdown {
        let source = fs::read_to_string(&path).expect("Markdown source must be UTF-8");
        let historical = is_historical_or_superseded(&source);
        for example in marked_examples(&path, &source) {
            if historical && example.marker.kind == ExampleKind::Canonical {
                continue;
            }

            let result = parse_entrypoint(example.marker.entrypoint, &example.body);
            match example.marker.kind {
                ExampleKind::Canonical => {
                    canonical_count += 1;
                    assert!(
                        result.is_ok(),
                        "canonical DSL example failed its {:?} parser at {}:{}",
                        example.marker.entrypoint,
                        display_path(&path),
                        example.marker_line
                    );
                }
                ExampleKind::Negative => {
                    negative_count += 1;
                    let expected = example
                        .marker
                        .expected_code
                        .as_deref()
                        .expect("negative markers require a stable code");
                    assert_eq!(
                        result,
                        Err(expected),
                        "negative DSL example did not fail with its declared code at {}:{}",
                        display_path(&path),
                        example.marker_line
                    );
                }
            }
        }
    }

    assert!(
        canonical_count > 0,
        "no active canonical DSL examples were gated"
    );
    assert!(negative_count > 0, "no negative DSL examples were gated");
}

#[test]
fn historical_detection_does_not_confuse_the_active_legacy_filename_notice() {
    let root = repository_root();
    let active = fs::read_to_string(root.join("docs/formal-v1-breaking-changes.md"))
        .expect("active migration guide must be readable");
    let historical = fs::read_to_string(
        root.join("docs/superpowers/specs/2026-07-16-dsl-vnext-region-ssa-design.md"),
    )
    .expect("superseded authored-surface design must be readable");

    assert!(!is_historical_or_superseded(&active));
    assert!(is_historical_or_superseded(&historical));
}

#[test]
fn markdown_scanner_ignores_marker_like_text_inside_unmarked_fences() {
    let source = r#"
````md
<!-- dsl-example: canonical; entry: step -->
```yaml
- kind: operation
```
````
"#;

    assert!(marked_examples(Path::new("synthetic.md"), source).is_empty());
}

#[test]
fn negative_dsl_vnext_fixtures_execute_with_the_declared_parse_code() {
    let directory = repository_root().join("tests/fixtures/negative/dsl-vnext");
    let mut fixtures = Vec::new();
    collect_files_with_extension(&directory, "yaml", &mut fixtures);
    collect_files_with_extension(&directory, "json", &mut fixtures);
    fixtures.sort();

    assert!(
        fixtures.len() >= 4,
        "the negative DSL fixture category must retain a minimal migration matrix"
    );

    for path in fixtures {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("negative fixture name must be UTF-8");
        let (expected_code, _) = file_name
            .split_once("--")
            .expect("negative fixture names must be CODE--case.yaml");
        let source = fs::read_to_string(&path).expect("negative fixture must be UTF-8");
        let error =
            validate_author_source(&source).expect_err("negative fixture unexpectedly parsed");
        assert_eq!(
            error.code(),
            expected_code,
            "negative fixture returned the wrong stable code: {}",
            display_path(&path)
        );
    }
}

#[test]
fn every_markdown_entrypoint_accepts_json_as_well_as_yaml() {
    let cases = [
        (
            Entrypoint::Agent,
            r#"{"api_version":"insight.agent/v2","kind":"agent","metadata":{"id":"json_agent","name":"JSON Agent","description":""},"inputs":{},"output":"string","workflow":{"steps":[],"result":{"return":"ok"}}}"#,
        ),
        (
            Entrypoint::Step,
            r#"[{"type":"action","id":"now","call":"current_time","inputs":{"timezone":"UTC"}}]"#,
        ),
        (
            Entrypoint::Messages,
            r#"[{"role":"user","content":[{"text":"Hello"}]}]"#,
        ),
        (Entrypoint::Content, r#"[{"text":"Hello"}]"#),
        (Entrypoint::Value, r#"{"ok":true}"#),
    ];

    for (entrypoint, source) in cases {
        assert_eq!(
            parse_entrypoint(entrypoint, source),
            Ok(()),
            "JSON failed the {entrypoint:?} parser"
        );
    }
}

#[derive(Debug)]
struct MarkedExample {
    marker: ExampleMarker,
    marker_line: usize,
    body: String,
}

fn marked_examples(path: &Path, source: &str) -> Vec<MarkedExample> {
    let lines = source.lines().collect::<Vec<_>>();
    let mut examples = Vec::new();
    let mut index = 0usize;

    while index < lines.len() {
        let trimmed = lines[index].trim();
        if let Some((delimiter, width)) = markdown_fence(trimmed) {
            index += 1;
            while index < lines.len()
                && !is_markdown_fence_close(lines[index].trim(), delimiter, width)
            {
                index += 1;
            }
            index = (index + 1).min(lines.len());
            continue;
        }
        if !trimmed.starts_with("<!--") || !trimmed.contains("dsl-example:") {
            index += 1;
            continue;
        }

        let marker = parse_marker(trimmed).unwrap_or_else(|reason| {
            panic!(
                "invalid DSL marker at {}:{}: {reason}",
                display_path(path),
                index + 1
            )
        });
        let fence = lines.get(index + 1).map(|line| line.trim());
        assert!(
            matches!(fence, Some("```yaml" | "```json")),
            "DSL marker must be immediately followed by a yaml/json fence at {}:{}",
            display_path(path),
            index + 1
        );

        let body_start = index + 2;
        let body_end = (body_start..lines.len())
            .find(|line| lines[*line].trim() == "```")
            .unwrap_or_else(|| {
                panic!(
                    "DSL fence is not closed at {}:{}",
                    display_path(path),
                    index + 1
                )
            });
        examples.push(MarkedExample {
            marker,
            marker_line: index + 1,
            body: lines[body_start..body_end].join("\n"),
        });
        index = body_end + 1;
    }

    examples
}

fn markdown_fence(line: &str) -> Option<(char, usize)> {
    let delimiter = line.chars().next()?;
    if !matches!(delimiter, '`' | '~') {
        return None;
    }
    let width = line
        .chars()
        .take_while(|character| *character == delimiter)
        .count();
    (width >= 3).then_some((delimiter, width))
}

fn is_markdown_fence_close(line: &str, delimiter: char, minimum_width: usize) -> bool {
    let width = line
        .chars()
        .take_while(|character| *character == delimiter)
        .count();
    width >= minimum_width && line[width..].trim().is_empty()
}

fn parse_marker(line: &str) -> Result<ExampleMarker, String> {
    let body = line
        .strip_prefix("<!-- dsl-example: ")
        .and_then(|value| value.strip_suffix(" -->"))
        .ok_or_else(|| "marker must use the frozen HTML-comment format".to_string())?;
    let fields = body.split("; ").collect::<Vec<_>>();
    let kind = match fields.first().copied() {
        Some("canonical") => ExampleKind::Canonical,
        Some("negative") => ExampleKind::Negative,
        _ => return Err("marker kind must be canonical or negative".to_string()),
    };
    let entrypoint = fields
        .get(1)
        .and_then(|field| field.strip_prefix("entry: "))
        .ok_or_else(|| "marker must declare its entrypoint second".to_string())?;
    let entrypoint = match entrypoint {
        "agent" => Entrypoint::Agent,
        "step" => Entrypoint::Step,
        "messages" => Entrypoint::Messages,
        "content" => Entrypoint::Content,
        "value" => Entrypoint::Value,
        _ => return Err("unknown DSL example entrypoint".to_string()),
    };

    let expected_code = match kind {
        ExampleKind::Canonical if fields.len() == 2 => None,
        ExampleKind::Negative if fields.len() == 3 => {
            let code = fields[2]
                .strip_prefix("code: ")
                .ok_or_else(|| "negative marker must declare code third".to_string())?;
            let mut characters = code.chars();
            if !characters
                .next()
                .is_some_and(|first| first.is_ascii_uppercase())
                || !characters.all(|character| {
                    character == '_' || character.is_ascii_uppercase() || character.is_ascii_digit()
                })
            {
                return Err(
                    "negative marker code must be a stable uppercase identifier".to_string()
                );
            }
            Some(code.to_string())
        }
        ExampleKind::Canonical => return Err("canonical marker has unexpected fields".to_string()),
        ExampleKind::Negative => {
            return Err("negative marker must declare a stable code".to_string())
        }
    };

    Ok(ExampleMarker {
        kind,
        entrypoint,
        expected_code,
    })
}

fn parse_entrypoint(entrypoint: Entrypoint, source: &str) -> Result<(), &'static str> {
    if entrypoint == Entrypoint::Agent {
        return validate_author_source(source).map_err(|error| error.code());
    }

    let fragment = yaml_serde::from_str::<Value>(source).map_err(|_| AUTHOR_PARSE_ERROR_CODE)?;
    let mut inputs = json!({
        "question": "string",
        "messages": {"type": "Message[]", "default": []},
        "image_url": {"type": "string", "optional": true},
        "report_text": {"type": "string", "optional": true},
        "analyses": "string",
        "technical": "string",
        "risk": "string",
        "perspectives": "string[]",
        "failed_branches": "string[]",
        "abnormal_indicators": {"type": "string", "optional": true},
        "comprehensive_interpretation": {"type": "string", "optional": true}
    })
    .as_object()
    .unwrap()
    .clone();

    let steps = match entrypoint {
        Entrypoint::Step => match fragment {
            Value::Array(steps) if !steps.is_empty() => steps,
            Value::Object(_) => vec![fragment],
            _ => return Err(AUTHOR_PARSE_ERROR_CODE),
        },
        Entrypoint::Messages => vec![json!({
            "type": "llm",
            "id": "gate_llm",
            "model": "general_chat",
            "messages": fragment,
            "response": "string"
        })],
        Entrypoint::Content => {
            let content = match fragment {
                Value::Array(parts) => Value::Array(parts),
                Value::Object(_) => Value::Array(vec![fragment]),
                _ => return Err(AUTHOR_PARSE_ERROR_CODE),
            };
            vec![json!({
                "type": "llm",
                "id": "gate_llm",
                "model": "general_chat",
                "messages": [{"role": "user", "content": content}],
                "response": "string"
            })]
        }
        Entrypoint::Value => vec![json!({
            "type": "action",
            "id": "gate_action",
            "call": "gate.action",
            "inputs": {"payload": fragment}
        })],
        Entrypoint::Agent => unreachable!(),
    };

    for step in &steps {
        if let Some(id) = step.get("id").and_then(Value::as_str) {
            inputs.remove(id);
        }
    }

    let document = json!({
        "api_version": "insight.agent/v2",
        "kind": "agent",
        "metadata": {
            "id": "entrypoint_gate",
            "name": "Entrypoint Gate",
            "description": "Synthetic authored-surface parser fixture."
        },
        "types": {
            "Perspective": {"type": "string", "min_length": 1},
            "SynthesisInput": {"fields": {"perspectives": "string[]"}},
            "SynthesisResult": {"fields": {"content": "string", "degraded": "boolean"}},
            "HealthResult": {"fields": {"answer": "string"}}
        },
        "inputs": inputs,
        "output": "string",
        "workflow": {
            "steps": steps,
            "result": {"return": "ok"}
        }
    });
    validate_author_source(&document.to_string()).map_err(|error| error.code())
}

fn checked_in_agent_documents() -> Vec<(PathBuf, Value)> {
    let mut files = Vec::new();
    collect_files_with_extension(&repository_root().join("agents"), "yaml", &mut files);
    files.retain(|path| path.file_name().and_then(|name| name.to_str()) == Some("agent.yaml"));
    files.sort();
    assert!(
        !files.is_empty(),
        "no checked-in agent.yaml files were found"
    );
    files
        .into_iter()
        .map(|path| {
            let source = fs::read_to_string(&path).expect("agent source must be UTF-8");
            let document = yaml_serde::from_str::<Value>(&source)
                .unwrap_or_else(|error| panic!("{} is not YAML: {error}", display_path(&path)));
            (path, document)
        })
        .collect()
}

fn collect_removed_author_syntax(value: &Value, path: &str, violations: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            for removed in ["$schema", "$defs", "$ref"] {
                if object.contains_key(removed) {
                    violations.push(format!("{path} contains removed key {removed}"));
                }
            }
            if object.len() == 1 {
                let key = object.keys().next().unwrap();
                if matches!(
                    key.as_str(),
                    "from" | "literal" | "object" | "array" | "template"
                ) {
                    violations.push(format!("{path} uses removed {{{key}: ...}} wrapper"));
                }
            }
            for (key, child) in object {
                collect_removed_author_syntax(child, &format!("{path}.{key}"), violations);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                collect_removed_author_syntax(child, &format!("{path}[{index}]"), violations);
            }
        }
        _ => {}
    }
}

fn assert_simplified_steps(path: &Path, steps: &[Value], location: &str) {
    for (index, step) in steps.iter().enumerate() {
        let step_location = format!("{location}[{index}]");
        let object = step
            .as_object()
            .unwrap_or_else(|| panic!("{} {step_location} must be an object", display_path(path)));
        assert!(
            !object.contains_key("kind"),
            "{} {step_location} must use type, not kind",
            display_path(path)
        );
        assert!(
            !object.contains_key("output_schema"),
            "{} {step_location} retains output_schema",
            display_path(path)
        );
        for removed in ["uses", "with", "config"] {
            assert!(
                !object.contains_key(removed),
                "{} {step_location} retains removed key {removed}",
                display_path(path)
            );
        }
        let step_type = object
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_else(|| {
                panic!(
                    "{} {step_location} must declare a scalar type",
                    display_path(path)
                )
            });
        match step_type {
            "llm" => {
                assert!(
                    !object.contains_key("inputs"),
                    "{} {step_location} must not author LLM inputs",
                    display_path(path)
                );
                assert!(
                    object.get("response").is_some_and(Value::is_string),
                    "{} {step_location}.response must be a type expression",
                    display_path(path)
                );
            }
            "action" => {}
            "parallel" => {
                let branches = object
                    .get("branches")
                    .and_then(Value::as_object)
                    .unwrap_or_else(|| {
                        panic!(
                            "{} {step_location}.branches must be an object",
                            display_path(path)
                        )
                    });
                for (name, branch) in branches {
                    let branch = branch.as_object().unwrap();
                    assert!(
                        !branch.contains_key("output_schema"),
                        "{} {step_location}.branches.{name} retains output_schema",
                        display_path(path)
                    );
                    assert!(
                        branch.get("output").is_some_and(Value::is_string),
                        "{} {step_location}.branches.{name}.output must be a type expression",
                        display_path(path)
                    );
                    let child_steps = branch
                        .get("steps")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or_default();
                    assert_simplified_steps(
                        path,
                        child_steps,
                        &format!("{step_location}.branches.{name}.steps"),
                    );
                }
            }
            "switch" => {
                assert!(
                    object.get("output").is_some_and(Value::is_string),
                    "{} {step_location}.output must be a type expression",
                    display_path(path)
                );
                for (arm_location, arm) in switch_arms(object, &step_location) {
                    let child_steps = arm
                        .get("steps")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or_default();
                    assert_simplified_steps(path, child_steps, &format!("{arm_location}.steps"));
                }
            }
            other => panic!(
                "{} {step_location} has unknown authored step type {other}",
                display_path(path)
            ),
        }
    }
}

fn inspect_message_parts(
    path: &Path,
    steps: &[Value],
    location: &str,
    content_part_count: &mut usize,
) {
    for (index, step) in steps.iter().enumerate() {
        let step_location = format!("{location}[{index}]");
        let object = step.as_object().unwrap();
        match object.get("type").and_then(Value::as_str) {
            Some("llm") => {
                let messages = object
                    .get("messages")
                    .and_then(Value::as_array)
                    .unwrap_or_else(|| {
                        panic!(
                            "{} {step_location}.messages must be a list",
                            display_path(path)
                        )
                    });
                for (message_index, message) in messages.iter().enumerate() {
                    let message_location = format!("{step_location}.messages[{message_index}]");
                    if let Some(source) = message.as_str() {
                        assert!(
                            source.starts_with('$'),
                            "{} {message_location} dynamic source must use $",
                            display_path(path)
                        );
                        continue;
                    }
                    let message = message.as_object().unwrap_or_else(|| {
                        panic!(
                            "{} {message_location} must be a message object or $messages",
                            display_path(path)
                        )
                    });
                    assert_eq!(
                        message.len(),
                        2,
                        "{} {message_location} must contain only role and content",
                        display_path(path)
                    );
                    assert!(message.contains_key("role") && message.contains_key("content"));
                    let content = message["content"].as_array().unwrap_or_else(|| {
                        panic!(
                            "{} {message_location}.content must be a list",
                            display_path(path)
                        )
                    });
                    for (part_index, part) in content.iter().enumerate() {
                        *content_part_count += 1;
                        let part_location = format!("{message_location}.content[{part_index}]");
                        let part = part.as_object().unwrap_or_else(|| {
                            panic!(
                                "{} {part_location} must be a single-key object",
                                display_path(path)
                            )
                        });
                        assert_eq!(
                            part.len(),
                            1,
                            "{} {part_location} must contain exactly one key",
                            display_path(path)
                        );
                        let (key, value) = part.iter().next().unwrap();
                        assert!(
                            matches!(key.as_str(), "text" | "image_url"),
                            "{} {part_location} uses unsupported key {key}",
                            display_path(path)
                        );
                        assert!(
                            value.is_string(),
                            "{} {part_location}.{key} must be a string scalar",
                            display_path(path)
                        );
                    }
                }
            }
            Some("parallel") => {
                for (name, branch) in object["branches"].as_object().unwrap() {
                    let child_steps = branch
                        .get("steps")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or_default();
                    inspect_message_parts(
                        path,
                        child_steps,
                        &format!("{step_location}.branches.{name}.steps"),
                        content_part_count,
                    );
                }
            }
            Some("switch") => {
                for (arm_location, arm) in switch_arms(object, &step_location) {
                    let child_steps = arm
                        .get("steps")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or_default();
                    inspect_message_parts(
                        path,
                        child_steps,
                        &format!("{arm_location}.steps"),
                        content_part_count,
                    );
                }
            }
            _ => {}
        }
    }
}

fn collect_dynamic_message_sources<'a>(steps: &'a [Value], sources: &mut Vec<&'a str>) {
    for step in steps {
        let object = step.as_object().unwrap();
        match object.get("type").and_then(Value::as_str) {
            Some("llm") => {
                for message in object["messages"].as_array().unwrap() {
                    if let Some(source) = message.as_str() {
                        sources.push(source);
                    }
                }
            }
            Some("parallel") => {
                for branch in object["branches"].as_object().unwrap().values() {
                    let child_steps = branch
                        .get("steps")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or_default();
                    collect_dynamic_message_sources(child_steps, sources);
                }
            }
            Some("switch") => {
                for (_, arm) in switch_arms(object, "switch") {
                    let child_steps = arm
                        .get("steps")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or_default();
                    collect_dynamic_message_sources(child_steps, sources);
                }
            }
            _ => {}
        }
    }
}

fn switch_arms<'a>(
    step: &'a serde_json::Map<String, Value>,
    location: &str,
) -> Vec<(String, &'a serde_json::Map<String, Value>)> {
    let mut arms = Vec::new();
    if let Some(cases) = step.get("cases").and_then(Value::as_array) {
        for (index, case) in cases.iter().enumerate() {
            arms.push((
                format!("{location}.cases[{index}]"),
                case.as_object().unwrap(),
            ));
        }
    }
    if let Some(default) = step.get("default").and_then(Value::as_object) {
        arms.push((format!("{location}.default"), default));
    }
    arms
}

fn declared_type(declaration: &Value) -> Option<&str> {
    declaration
        .as_str()
        .or_else(|| declaration.get("type").and_then(Value::as_str))
}

fn is_historical_or_superseded(source: &str) -> bool {
    source.lines().take(12).any(|line| {
        let lower = line.trim().to_ascii_lowercase();
        lower.starts_with("> historical")
            || lower.starts_with("> **historical")
            || lower.starts_with("status: superseded")
            || lower.starts_with("**status:** superseded")
    })
}

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn collect_files_with_extension(directory: &Path, extension: &str, files: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", display_path(directory)));
    for entry in entries {
        let path = entry.expect("directory entry must be readable").path();
        if path.is_dir() {
            collect_files_with_extension(&path, extension, files);
        } else if path.extension().and_then(|value| value.to_str()) == Some(extension) {
            files.push(path);
        }
    }
}

fn display_path(path: &Path) -> String {
    path.strip_prefix(repository_root())
        .unwrap_or(path)
        .display()
        .to_string()
}
