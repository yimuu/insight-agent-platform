use std::{fs, path::Path};

use insight_agent_platform::agent::loader::load_agents;

fn write_file(path: &Path, body: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, body).unwrap();
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
