use std::path::Path;

use insight_agent_platform::{
    catalog::compile_enabled_agents,
    config::PlatformConfig,
    dsl::compiled::JoinPolicy,
    dsl::compiler::CompileLimits,
    nodes::default_node_registries,
    resources::{
        builtin_actions::{builtin_action_registry, RestrictedHttpGetAction},
        config::load_model_registry_with_env,
    },
};

#[test]
fn enabled_repository_agents_compile_through_production_registries() {
    let config = PlatformConfig::load(Path::new("config/platform.yaml")).unwrap();
    let models = load_model_registry_with_env(&config.models.config, |name| {
        (name == "OPENAI_API_KEY").then(|| "repository-test-secret".to_string())
    })
    .unwrap();
    let http_get = config
        .actions
        .http_get
        .as_ref()
        .map(|http| {
            RestrictedHttpGetAction::new(
                http.timeout,
                http.max_bytes,
                http.allowlist.iter().cloned().collect(),
            )
        })
        .transpose()
        .unwrap();
    let actions = builtin_action_registry(
        &config.actions.enabled.iter().cloned().collect::<Vec<_>>(),
        http_get,
    )
    .unwrap();
    let (node_types, _executors) = default_node_registries().unwrap();
    let agents = compile_enabled_agents(
        &config.agents.directory,
        &config.agents.enabled,
        node_types,
        models,
        actions,
        config.runtime.default_node_timeout,
        CompileLimits {
            max_fork_branches: 32,
        },
    )
    .unwrap();

    assert_eq!(agents.list().count(), 4);

    let parallel = agents.get("parallel_researcher").unwrap();
    let fork = &parallel.execution_plan.forks["fanout"];
    assert_eq!(fork.branches.len(), 2);
    assert_eq!(fork.join_id, "collect");
    assert!(fork.branches.values().all(|branch| branch.nodes.len() >= 2));
    assert_eq!(fork.policy, JoinPolicy::AllSettled);

    let researcher = agents.get("researcher").unwrap();
    assert_eq!(researcher.nodes["plan"].kind, "core.chat");
    assert_eq!(
        researcher.nodes["plan"].emit,
        insight_agent_platform::dsl::EmitPolicy::None
    );
    assert_eq!(
        researcher.nodes["answer"].emit,
        insight_agent_platform::dsl::EmitPolicy::Content
    );
    assert_eq!(researcher.nodes["result"].kind, "core.end");

    let code_demo = agents.get("code_node_demo").unwrap();
    assert_eq!(code_demo.nodes["analyze_text"].kind, "core.action");
    assert_eq!(code_demo.nodes["result"].kind, "core.end");
    assert!(code_demo.nodes["render"]
        .references
        .contains("analyze_text"));

    assert!(parallel.nodes["synthesize"].references.contains("collect"));
    assert!(!parallel.nodes["synthesize"]
        .references
        .contains("analyze_a"));
    assert!(!parallel.nodes["synthesize"]
        .references
        .contains("normalize_a"));

    let medical = agents.get("medical_report_interpreter").unwrap();
    for node_id in [
        "abnormal_indicators",
        "comprehensive_interpretation",
        "health_advice",
    ] {
        assert_eq!(medical.nodes[node_id].kind, "core.chat");
    }
    assert_eq!(medical.nodes["result"].kind, "core.end");
    assert!(medical.nodes["abnormal_indicators"].references.is_empty());
    assert!(medical.nodes["comprehensive_interpretation"]
        .references
        .contains("abnormal_indicators"));
}
