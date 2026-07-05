use std::{fs, path::Path};

use insight_agent_platform::agent::loader::load_agents;
use jsonschema::JSONSchema;
use serde_json::json;

fn write_file(path: &Path, body: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, body).unwrap();
}

#[test]
fn loads_medical_report_interpreter_agent() {
    let agents = load_agents(Path::new("agents")).unwrap();
    let agent = agents
        .iter()
        .find(|agent| agent.config.id == "medical_report_interpreter")
        .unwrap();

    assert_eq!(agent.config.steps.len(), 3);
    assert_eq!(agent.config.steps[0].id, "abnormal_indicators");
    assert_eq!(
        agent.config.steps[0].image_input.as_deref(),
        Some("input.images")
    );
    assert!(agent.prompts.contains_key("health_advice"));
    let abnormal_prompt = agent.prompts.get("abnormal_indicators").unwrap();
    assert!(abnormal_prompt.contains("### 异常指标解读"));
    assert!(abnormal_prompt.contains("1. 指标名称"));
    assert!(abnormal_prompt.contains("必须使用有序列表输出所有异常指标"));
    assert!(abnormal_prompt.contains("列表结束后不要输出总结"));

    let comprehensive_prompt = agent.prompts.get("comprehensive_interpretation").unwrap();
    assert!(comprehensive_prompt.contains("### 综合解读"));
    assert!(comprehensive_prompt.contains("只输出一段话"));

    let health_advice_prompt = agent.prompts.get("health_advice").unwrap();
    assert!(health_advice_prompt.contains("### 健康建议"));
    assert!(health_advice_prompt.contains("不要输出“关键异常”"));
    assert!(health_advice_prompt.contains("额外标题"));
}

#[test]
fn medical_report_interpreter_allows_http_image_urls_for_local_testing() {
    let agents = load_agents(Path::new("agents")).unwrap();
    let agent = agents
        .iter()
        .find(|agent| agent.config.id == "medical_report_interpreter")
        .unwrap();
    let schema = JSONSchema::compile(&agent.config.input.schema).unwrap();

    let input = json!({
        "report_text": "",
        "images": ["http://127.0.0.1:8080/report.png"],
        "messages": [],
        "question": "请解读这张报告"
    });

    assert!(schema.validate(&input).is_ok());
}

#[test]
fn loads_agent_with_multiple_prompt_files() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("researcher");
    write_file(&agent.join("prompts/system.md"), "You are helpful.");
    write_file(
        &agent.join("prompts/final.md"),
        "Answer {{ input.question }}",
    );
    write_file(
        &agent.join("agent.yaml"),
        r#"
id: researcher
name: Researcher
description: Test agent
model:
  provider: openai_compatible
prompts:
  system: prompts/system.md
  final: prompts/final.md
input:
  schema:
    type: object
steps:
  - id: answer
    type: llm
    system_prompt_ref: system
    prompt_ref: final
"#,
    );

    let agents = load_agents(dir.path()).unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].config.id, "researcher");
    assert_eq!(agents[0].prompts.get("system").unwrap(), "You are helpful.");
}

#[test]
fn loads_text_step_with_template_source() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("text_agent");
    write_file(
        &agent.join("agent.yaml"),
        r#"
id: text_agent
name: Text Agent
model:
  provider: openai_compatible
input:
  schema:
    type: object
steps:
  - id: intro
    type: text
    prompt: "你好 {{ input.name }}"
"#,
    );

    let agents = load_agents(dir.path()).unwrap();
    assert_eq!(agents[0].config.steps[0].id, "intro");
}

#[test]
fn rejects_duplicate_step_ids() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("bad");
    write_file(
        &agent.join("agent.yaml"),
        r#"
id: bad
name: Bad
model:
  provider: openai_compatible
input:
  schema:
    type: object
steps:
  - id: same
    type: prompt
    prompt: one
  - id: same
    type: prompt
    prompt: two
"#,
    );

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("duplicate step id"));
}

#[test]
fn rejects_text_step_without_template_source() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("bad");
    write_file(
        &agent.join("agent.yaml"),
        r#"
id: bad
name: Bad
model:
  provider: openai_compatible
input:
  schema:
    type: object
steps:
  - id: intro
    type: text
"#,
    );

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("type 'text' requires prompt or prompt_ref"));
}

#[test]
fn rejects_prompt_ref_and_inline_prompt_together() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("bad");
    write_file(&agent.join("prompts/a.md"), "hello");
    write_file(
        &agent.join("agent.yaml"),
        r#"
id: bad
name: Bad
model:
  provider: openai_compatible
prompts:
  a: prompts/a.md
input:
  schema:
    type: object
steps:
  - id: answer
    type: prompt
    prompt_ref: a
    prompt: inline
"#,
    );

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("prompt_ref and prompt are mutually exclusive"));
}

#[test]
fn rejects_missing_prompt_ref() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("bad");
    write_file(
        &agent.join("agent.yaml"),
        r#"
id: bad
name: Bad
model:
  provider: openai_compatible
input:
  schema:
    type: object
steps:
  - id: answer
    type: prompt
    prompt_ref: missing
"#,
    );

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("unknown prompt_ref"));
}

#[test]
fn rejects_prompt_step_without_prompt_source() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("bad");
    write_file(
        &agent.join("agent.yaml"),
        r#"
id: bad
name: Bad
model:
  provider: openai_compatible
input:
  schema:
    type: object
steps:
  - id: answer
    type: prompt
"#,
    );

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("type 'prompt' requires prompt or prompt_ref"));
}

#[test]
fn rejects_llm_step_without_prompt_source() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("bad");
    write_file(
        &agent.join("agent.yaml"),
        r#"
id: bad
name: Bad
model:
  provider: openai_compatible
input:
  schema:
    type: object
steps:
  - id: answer
    type: llm
"#,
    );

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("type 'llm' requires prompt or prompt_ref"));
}

#[test]
fn rejects_tool_step_without_tool_name() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("bad");
    write_file(
        &agent.join("agent.yaml"),
        r#"
id: bad
name: Bad
model:
  provider: openai_compatible
input:
  schema:
    type: object
steps:
  - id: answer
    type: tool
"#,
    );

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("type 'tool' requires tool"));
}

#[test]
fn rejects_unsupported_image_input_path() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("bad");
    write_file(
        &agent.join("agent.yaml"),
        r#"
id: bad
name: Bad
model:
  provider: openai_compatible
input:
  schema:
    type: object
steps:
  - id: answer
    type: llm
    prompt: hello
    image_input: input.report_images
"#,
    );

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("unsupported image_input"));
}

#[test]
fn rejects_image_input_on_non_llm_step() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("bad");
    write_file(
        &agent.join("agent.yaml"),
        r#"
id: bad
name: Bad
model:
  provider: openai_compatible
input:
  schema:
    type: object
steps:
  - id: render
    type: prompt
    prompt: hello
    image_input: input.images
"#,
    );

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("image_input is only supported on llm steps"));
}

#[test]
fn rejects_prompt_path_outside_agent_directory() {
    let dir = tempfile::tempdir().unwrap();
    write_file(&dir.path().join("secret.md"), "secret");
    let agent = dir.path().join("bad");
    write_file(
        &agent.join("agent.yaml"),
        r#"
id: bad
name: Bad
model:
  provider: openai_compatible
prompts:
  secret: ../secret.md
input:
  schema:
    type: object
steps:
  - id: answer
    type: prompt
    prompt_ref: secret
"#,
    );

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("must stay inside agent directory"));
}

#[test]
fn rejects_invalid_input_schema_during_load() {
    let dir = tempfile::tempdir().unwrap();
    let agent = dir.path().join("bad");
    write_file(
        &agent.join("agent.yaml"),
        r#"
id: bad
name: Bad
model:
  provider: openai_compatible
input:
  schema:
    type: 42
steps:
  - id: answer
    type: prompt
    prompt: hello
"#,
    );

    let err = load_agents(dir.path()).unwrap_err().to_string();
    assert!(err.contains("invalid input schema"));
}
