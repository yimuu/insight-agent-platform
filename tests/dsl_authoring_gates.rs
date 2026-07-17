use std::{
    fs,
    path::{Path, PathBuf},
};

use insight_agent_platform::dsl::vnext::{
    message::{AuthoredContentExpr, MessageListExpr},
    parse_workflow,
    raw::{Step, PARSE_ERROR_CODE},
    ValueExpr,
};

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
        let error = parse_workflow(&source).expect_err("negative fixture unexpectedly parsed");
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
            r#"{"api_version":"insight.agent/v2","kind":"agent","metadata":{"id":"json_agent","name":"JSON Agent","description":""},"schema_dialect":"https://json-schema.org/draft/2020-12/schema","input":{"schema":{"type":"object"}},"output":{"data_schema":{"type":"null"}},"workflow":{"steps":[],"result":{"return":{"data":{"literal":null}}}}}"#,
        ),
        (
            Entrypoint::Step,
            r#"[{"kind":"action","id":"now","call":"current_time","inputs":{"timezone":{"literal":"UTC"}}}]"#,
        ),
        (
            Entrypoint::Messages,
            r#"[{"role":"user","content":{"text":"Hello"}}]"#,
        ),
        (Entrypoint::Content, r#"[{"text":"Hello"}]"#),
        (Entrypoint::Value, r#"{"literal":{"ok":true}}"#),
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
    match entrypoint {
        Entrypoint::Agent => parse_workflow(source)
            .map(|_| ())
            .map_err(|error| error.code()),
        Entrypoint::Step => parse_single_or_list::<Step>(source),
        Entrypoint::Messages => yaml_serde::from_str::<MessageListExpr>(source)
            .map(|_| ())
            .map_err(|_| PARSE_ERROR_CODE),
        Entrypoint::Content => yaml_serde::from_str::<AuthoredContentExpr>(source)
            .map(|_| ())
            .map_err(|_| PARSE_ERROR_CODE),
        Entrypoint::Value => yaml_serde::from_str::<ValueExpr>(source)
            .map(|_| ())
            .map_err(|_| PARSE_ERROR_CODE),
    }
}

fn parse_single_or_list<T>(source: &str) -> Result<(), &'static str>
where
    T: serde::de::DeserializeOwned,
{
    if yaml_serde::from_str::<Vec<T>>(source).is_ok_and(|values| !values.is_empty())
        || yaml_serde::from_str::<T>(source).is_ok()
    {
        Ok(())
    } else {
        Err(PARSE_ERROR_CODE)
    }
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
