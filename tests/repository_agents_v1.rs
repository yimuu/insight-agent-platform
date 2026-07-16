use std::{collections::BTreeSet, path::Path};

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
fn checked_in_repository_agents_compile_through_production_registries() {
    let config = PlatformConfig::load(Path::new("config/platform.yaml")).unwrap();
    let production_agents = BTreeSet::from([
        "code_node_demo".to_string(),
        "medical_report_interpreter".to_string(),
        "parallel_researcher".to_string(),
        "researcher".to_string(),
    ]);
    assert_eq!(config.agents.enabled, production_agents);
    assert!(!config.agents.enabled.contains("workflow_failure_demo"));

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
    let repository_agents = BTreeSet::from([
        "code_node_demo".to_string(),
        "medical_report_interpreter".to_string(),
        "parallel_researcher".to_string(),
        "researcher".to_string(),
        "workflow_failure_demo".to_string(),
    ]);
    let agents = compile_enabled_agents(
        &config.agents.directory,
        &repository_agents,
        node_types,
        models,
        actions,
        config.runtime.default_node_timeout,
        CompileLimits {
            max_fork_branches: 32,
        },
    )
    .unwrap();

    assert_eq!(agents.list().count(), 5);

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

    assert_eq!(parallel.nodes["decide"].kind, "core.condition");
    for node_id in ["synthesize_full", "synthesize_a_only", "synthesize_b_only"] {
        assert_eq!(parallel.nodes[node_id].kind, "core.chat");
        assert_eq!(
            parallel.nodes[node_id].references,
            BTreeSet::from(["collect".to_string()])
        );
        assert!(!parallel.nodes[node_id].references.contains("analyze_a"));
        assert!(!parallel.nodes[node_id].references.contains("normalize_a"));
    }
    assert_eq!(parallel.nodes["selected_synthesis"].kind, "core.select");
    assert_eq!(parallel.nodes["result_policy"].kind, "core.condition");
    assert_eq!(parallel.nodes["result_full"].kind, "core.end");
    assert_eq!(parallel.nodes["result_degraded"].kind, "core.end");
    assert_eq!(parallel.nodes["fail_all"].kind, "core.end");
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

    let workflow_failure = agents.get("workflow_failure_demo").unwrap();
    assert_eq!(workflow_failure.nodes.len(), 1);
    assert_eq!(workflow_failure.nodes["reject"].kind, "core.end");
}
