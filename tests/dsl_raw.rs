use insight_agent_platform::dsl::{parse_raw_agent, EmitPolicy};

const FORMAL_V1: &str = r#"
version: 1
id: test_agent
name: Test Agent
description: Strict parser fixture.

input:
  schema:
    type: object
    required: [text]

prompts:
  system: prompts/system.md

entry: answer

nodes:
  answer:
    type: core.template
    next: result
    emit: content
    timeout: 5s
    config:
      value: "{{ input.text }}"

  result:
    type: core.output
    config:
      content:
        template: "{{ nodes.answer.output }}"
      format: text
"#;

#[test]
fn parses_formal_v1_node_envelope() {
    let agent = parse_raw_agent(FORMAL_V1).unwrap();

    assert_eq!(agent.version, 1);
    assert_eq!(agent.entry, "answer");
    assert_eq!(agent.nodes["answer"].kind, "core.template");
    assert_eq!(agent.nodes["answer"].emit, EmitPolicy::Content);
    assert_eq!(agent.nodes["answer"].timeout.unwrap().get().as_secs(), 5);
    assert_eq!(agent.prompts["system"], "prompts/system.md");
}

#[test]
fn rejects_unknown_agent_and_node_fields() {
    let top_level = FORMAL_V1.replace("version: 1", "version: 1\nunknown: true");
    let error = parse_raw_agent(&top_level).unwrap_err();
    assert_eq!(error.code(), "DSL_YAML_INVALID");
    assert!(error.to_string().contains("unknown field"));

    let node = FORMAL_V1.replace("    emit: content", "    emit: content\n    tyop: broken");
    let error = parse_raw_agent(&node).unwrap_err();
    assert_eq!(error.code(), "DSL_YAML_INVALID");
    assert!(error.to_string().contains("unknown field"));
}

#[test]
fn rejects_unsupported_version_and_invalid_duration() {
    let unsupported = FORMAL_V1.replace("version: 1", "version: 2");
    assert_eq!(
        parse_raw_agent(&unsupported).unwrap_err().code(),
        "DSL_VERSION_UNSUPPORTED"
    );

    let zero = FORMAL_V1.replace("timeout: 5s", "timeout: 0s");
    assert_eq!(
        parse_raw_agent(&zero).unwrap_err().code(),
        "DSL_YAML_INVALID"
    );

    let malformed = FORMAL_V1.replace("timeout: 5s", "timeout: soon");
    assert_eq!(
        parse_raw_agent(&malformed).unwrap_err().code(),
        "DSL_YAML_INVALID"
    );
}

#[test]
fn defaults_optional_envelope_fields() {
    let yaml = FORMAL_V1
        .replace("    emit: content\n", "")
        .replace("    timeout: 5s\n", "");
    let agent = parse_raw_agent(&yaml).unwrap();
    let node = &agent.nodes["answer"];

    assert_eq!(node.emit, EmitPolicy::None);
    assert!(node.timeout.is_none());
}
