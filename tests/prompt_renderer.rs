use serde_json::json;

use insight_agent_platform::prompt::renderer::PromptRenderer;

#[test]
fn renders_input_and_step_values() {
    let renderer = PromptRenderer::new();
    let out = renderer
        .render(
            "Question: {{ input.question }} Plan: {{ steps.plan.output }}",
            &json!({
                "input": { "question": "What is Rust?" },
                "steps": { "plan": { "output": "Explain ownership." } }
            }),
        )
        .unwrap();

    assert_eq!(out, "Question: What is Rust? Plan: Explain ownership.");
}

#[test]
fn fails_for_missing_variable() {
    let renderer = PromptRenderer::new();
    let err = renderer
        .render("{{ input.missing }}", &json!({ "input": {} }))
        .unwrap_err();
    assert!(err.to_string().contains("prompt render error"));
}
