use std::{collections::BTreeSet, sync::Arc, time::Duration};

use insight_dsl::{CompileOptions, GraphAuthorDocument, GraphDocumentId};
use insight_engine::{
    author::CompileError,
    plan::{
        DescriptorValue, LeafTaskDescriptor, LeafTaskKind, PlanIndex, SubflowContractRegistry,
        ValueSource, VersionTag,
    },
    ContentHash, DefinitionRevisionId, EffectIdempotency, NodeId, PersistenceMode,
    WorkerCancellation, WorkerEffectClass, WorkerEffectPolicy,
};
use insight_runtime::catalog::{
    compile_agent_dir, compile_enabled_agents, deploy_agents, deploy_agents_with_persistence,
    AgentStreamingContract, DeployedAgent, DeploymentPersistencePolicy, DeploymentRiskCode,
    DeploymentRiskDiagnostic, DeploymentRiskSeverity, LeafDeploymentResolver, PublishedAgent,
    ResolvedLeafDeployment, TerminalOnlyDeploymentConfig,
};
use serde_json::json;

#[macro_use]
#[path = "../../../tests/support/workspace_assets.rs"]
mod workspace_assets;

struct FixtureResolver;

impl LeafDeploymentResolver for FixtureResolver {
    fn resolve_leaf(
        &self,
        kind: LeafTaskKind,
        descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        let configuration = serde_jcs::to_vec(&descriptor.public_configuration).unwrap();
        ResolvedLeafDeployment::new(
            VersionTag::new("fixture-worker-1").unwrap(),
            json!({
                "task_kind": kind.name(),
                "implementation": descriptor.implementation,
                "configuration_hash": ContentHash::from_bytes(&configuration),
            }),
        )
    }
}

struct SafeFixtureResolver;

impl LeafDeploymentResolver for SafeFixtureResolver {
    fn resolve_leaf(
        &self,
        kind: LeafTaskKind,
        descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        let resolved = FixtureResolver.resolve_leaf(kind, descriptor)?;
        let effect_policy = WorkerEffectPolicy::frozen(
            WorkerEffectClass::ReadOnly,
            EffectIdempotency::Idempotent,
            1,
            0,
            0,
            20,
            WorkerCancellation::Cooperative,
        )
        .unwrap();
        Ok(resolved.with_effect_policy(effect_policy))
    }
}

struct RetryingFixtureResolver;

impl LeafDeploymentResolver for RetryingFixtureResolver {
    fn resolve_leaf(
        &self,
        kind: LeafTaskKind,
        descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        let resolved = FixtureResolver.resolve_leaf(kind, descriptor)?;
        Ok(resolved.with_effect_policy(
            WorkerEffectPolicy::frozen(
                WorkerEffectClass::ReadOnly,
                EffectIdempotency::Idempotent,
                2,
                1,
                1,
                10,
                WorkerCancellation::Cooperative,
            )
            .unwrap(),
        ))
    }
}

fn published_fixture(id: &str, source: &str) -> Arc<PublishedAgent> {
    let graph = GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new(format!("{id}_graph")).unwrap(),
        source,
        CompileOptions::new(
            DefinitionRevisionId::new(format!("{id}_definition_revision")).unwrap(),
            format!("{id}/agent.yaml"),
            source,
        ),
    )
    .unwrap();
    Arc::new(PublishedAgent::from_verified_graph(id, id, "persistence fixture", graph).unwrap())
}

#[test]
fn all_checked_in_agents_compile_into_verified_immutable_revisions() {
    let root = workspace_assets::workspace_path("agents");
    let enabled = BTreeSet::from([
        "action_demo".to_owned(),
        "benchmark_wait".to_owned(),
        "conversation_context_probe".to_owned(),
        "conversation_demo".to_owned(),
        "medical_report_interpreter".to_owned(),
        "parallel_researcher".to_owned(),
        "researcher".to_owned(),
        "terminal_commit_fixture".to_owned(),
        "terminal_failure_fixture".to_owned(),
        "terminal_llm_failure_fixture".to_owned(),
        "terminal_stream_fixture".to_owned(),
        "tool_assistant".to_owned(),
        "workflow_failure_demo".to_owned(),
    ]);
    let catalog = compile_enabled_agents(&root, &enabled).unwrap();
    assert_eq!(
        catalog.ids().collect::<Vec<_>>(),
        enabled.iter().map(String::as_str).collect::<Vec<_>>()
    );
    for agent in catalog.list() {
        agent.plan().verify().unwrap();
        assert_eq!(
            agent.plan().metadata().definition_revision_id(),
            agent.definition_revision_id()
        );
    }
}

fn qualification_overlay_enabled_agents() -> BTreeSet<String> {
    let values = workspace_asset_str!(
        "deploy/helm/insight-agent-platform/values-terminal-only-qualification.yaml"
    );
    let enabled_marker = "  enabled:\n";
    let enabled_start = values
        .find(enabled_marker)
        .expect("qualification values must contain agents.enabled")
        + enabled_marker.len();
    values[enabled_start..]
        .lines()
        .take_while(|line| line.starts_with("    - "))
        .map(|line| line.trim_start_matches("    - ").to_owned())
        .collect()
}

#[test]
fn qualification_helm_catalog_resolves_without_a_durable_coordinator() {
    let values = workspace_asset_str!(
        "deploy/helm/insight-agent-platform/values-terminal-only-qualification.yaml"
    );
    assert!(
        values.contains("defaultPersistenceMode: terminal_only"),
        "qualification Helm values must retain the terminal-only process default"
    );

    let enabled = qualification_overlay_enabled_agents();
    assert!(!enabled.is_empty());
    assert!(!enabled.contains("benchmark_wait"));
    let catalog =
        compile_enabled_agents(&workspace_assets::workspace_path("agents"), &enabled).unwrap();
    let deployment_config =
        TerminalOnlyDeploymentConfig::new(true, true, Duration::from_secs(30)).unwrap();
    let graph_policy = deployment_config
        .resolve(PersistenceMode::TerminalOnly)
        .unwrap();
    let resolved = catalog
        .list()
        .map(|agent| {
            deployment_config
                .resolve(
                    agent
                        .declared_persistence_mode()
                        .unwrap_or(PersistenceMode::TerminalOnly),
                )
                .unwrap()
        })
        .collect::<Vec<_>>();

    assert!(resolved
        .iter()
        .all(|policy| policy.persistence_mode() == PersistenceMode::TerminalOnly));
    let durable_coordinator_enabled = graph_policy.persistence_mode() == PersistenceMode::Full
        || resolved
            .iter()
            .any(|policy| policy.persistence_mode() == PersistenceMode::Full);
    assert!(
        !durable_coordinator_enabled,
        "qualification catalog must not start durable background pumps"
    );
}

#[test]
fn revision_pins_main_source_and_every_referenced_prompt() {
    let root = workspace_assets::workspace_path("agents/researcher");
    let first = compile_agent_dir(&root).unwrap();
    let second = compile_agent_dir(&root).unwrap();
    assert_eq!(
        first.definition_revision_id(),
        second.definition_revision_id()
    );
    assert!(!first.prompt_files().is_empty());
}

#[test]
fn public_output_and_streaming_discovery_are_canonical_closed_and_sorted() {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: z_buffered
      type: llm
      model: general_chat
      messages: [{role: user, content: [{text: buffered}]}]
      stream: false
      publish: true
      response: string
    - id: private
      type: llm
      model: general_chat
      messages: [{role: user, content: [{text: private}]}]
      stream: true
      publish: false
      response: string
    - id: m_retrieval
      type: retrieval
      retrieval: fixture.search
      inputs: {query: lookup}
      publish: true
      response: string
    - id: private_retrieval
      type: retrieval
      retrieval: fixture.private_search
      inputs: {query: private}
      publish: false
      response: string
    - id: a_streaming
      type: llm
      model: general_chat
      messages: [{role: user, content: [{text: streaming}]}]
      stream: true
      publish: true
      response: string
    - return: $a_streaming
"#;
    let graph = GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("public_streaming_contract_graph").unwrap(),
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("public_streaming_contract_revision").unwrap(),
            "public-streaming/agent.yaml",
            source,
        ),
    )
    .unwrap();
    let native_plan = graph.plan().clone();
    let agent = PublishedAgent::from_verified_graph(
        "public_streaming",
        "Public streaming",
        "Public contract fixture",
        graph,
    )
    .unwrap();

    assert_eq!(
        agent.public_output_schema(),
        agent
            .plan()
            .metadata()
            .output_type()
            .json_schema_document()
            .unwrap()
    );
    assert_eq!(
        agent.public_output_schema(),
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "string"
        })
    );

    let wire = serde_json::to_value(agent.public_streaming_contract()).unwrap();
    assert_eq!(
        wire,
        json!({
            "protocol": "run-stream/v1",
            "transport": "sse",
            "live_only": true,
            "sources": [
                {"id": "a_streaming", "kind": "llm", "mode": "streaming", "format": "text"},
                {"id": "m_retrieval", "kind": "retrieval", "mode": "buffered", "format": "structured/retrieval"},
                {"id": "z_buffered", "kind": "llm", "mode": "buffered", "format": "text"}
            ]
        })
    );
    serde_json::from_value::<AgentStreamingContract>(wire.clone()).unwrap();

    let mut unknown_contract_field = wire.clone();
    unknown_contract_field
        .as_object_mut()
        .unwrap()
        .insert("replay".to_owned(), json!(true));
    assert!(
        serde_json::from_value::<AgentStreamingContract>(unknown_contract_field).is_err(),
        "streaming discovery must reject fields outside its closed protocol"
    );

    let mut unknown_source_field = wire.clone();
    unknown_source_field["sources"][0]
        .as_object_mut()
        .unwrap()
        .insert("model".to_owned(), json!("private-alias"));
    assert!(
        serde_json::from_value::<AgentStreamingContract>(unknown_source_field).is_err(),
        "streaming sources must not grow private fields implicitly"
    );

    for (field, value) in [
        ("kind", json!("future_source")),
        ("mode", json!("eventual")),
        ("format", json!("structured/unknown")),
    ] {
        let mut unknown_vocabulary = wire.clone();
        unknown_vocabulary["sources"][1][field] = value;
        assert!(
            serde_json::from_value::<AgentStreamingContract>(unknown_vocabulary).is_err(),
            "streaming discovery must reject unknown {field} vocabulary"
        );
    }

    let native = GraphAuthorDocument::from_verified_plan(
        GraphDocumentId::new("invalid_public_streaming_native_graph").unwrap(),
        native_plan,
    )
    .unwrap();
    let mut native_wire: serde_json::Value =
        serde_json::from_slice(&native.encode_json().unwrap()).unwrap();
    let llm = native_wire["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["kind"]["kind"] == "llm_task")
        .unwrap();
    llm["kind"]["descriptor"]["descriptor_version"] = json!("1");
    let native = GraphAuthorDocument::decode_json(&serde_json::to_vec(&native_wire).unwrap())
        .expect("generic Plan verification deliberately permits versioned leaf descriptors");
    let error = PublishedAgent::from_verified_graph(
        "invalid_public_streaming",
        "Invalid public streaming",
        "Strict publication fixture",
        native,
    )
    .unwrap_err();
    assert_eq!(error.code(), "LLM_DESCRIPTOR_INVALID");
}

#[test]
fn markdown_prompt_template_slots_are_compiled_into_typed_runtime_bindings() {
    let root = workspace_assets::workspace_path("agents/researcher");
    let agent = compile_agent_dir(&root).unwrap();
    let index = PlanIndex::new(agent.plan()).unwrap();

    let plan_node = NodeId::new("plan").unwrap();
    let descriptor = index.leaf_descriptor(&plan_node).unwrap().descriptor();
    let DescriptorValue::Object(bindings) = descriptor
        .public_configuration
        .get("runtime_bindings")
        .unwrap()
    else {
        panic!("LLM runtime bindings must be a closed object")
    };
    let DescriptorValue::String(question_port) = bindings.get("question").unwrap() else {
        panic!("planner.md question slot must compile into one data port")
    };
    let question_port = insight_engine::DataPortId::new(question_port).unwrap();
    assert!(matches!(
        index.source_for_input(&question_port),
        Some(ValueSource::RunInput { path }) if path == &["question".to_owned()]
    ));

    let answer_node = NodeId::new("answer").unwrap();
    let descriptor = index.leaf_descriptor(&answer_node).unwrap().descriptor();
    let DescriptorValue::Object(bindings) = descriptor
        .public_configuration
        .get("runtime_bindings")
        .unwrap()
    else {
        panic!("final prompt bindings must be a closed object")
    };
    assert_eq!(
        bindings.keys().map(String::as_str).collect::<Vec<_>>(),
        ["current_time", "plan", "question"]
    );
}

#[test]
fn public_input_is_normalized_before_scheduler_admission() {
    let root = workspace_assets::workspace_path("agents/medical_report_interpreter");
    let agent = compile_agent_dir(&root).unwrap();
    let value = agent
        .normalize_input(json!({
            "report_text": "report",
            "question": "what does this mean?"
        }))
        .unwrap();
    let object = value.value().as_object().unwrap();
    assert!(!object.contains_key("image_url"));
    assert_eq!(object.get("messages"), Some(&json!([])));
    assert!(value.matches(&agent.plan().metadata().input_contract().run_type().unwrap()));

    let schema = agent.public_input_schema();
    assert_eq!(schema.get("$schema"), None);
    assert_eq!(schema["$defs"], json!({}));
    assert_eq!(schema["required"], json!(["question", "report_text"]));
    assert_eq!(schema["properties"]["messages"]["default"], json!([]));
    assert!(schema["properties"]["image_url"].get("default").is_none());

    let error = agent
        .normalize_input(json!({
            "report_text": "report",
            "image_url": null,
            "question": "question"
        }))
        .unwrap_err();
    assert_eq!(error.code(), "AGENT_INPUT_CONTRACT_INVALID");

    let error = agent
        .normalize_input(json!({
            "report_text": "report",
            "messages": [],
            "question": "question",
            "unexpected": true
        }))
        .unwrap_err();
    assert_eq!(error.code(), "AGENT_INPUT_UNKNOWN_FIELD");
}

#[test]
fn graph_publication_uses_the_same_frozen_input_normalization_contract() {
    let root = workspace_assets::workspace_path("agents/medical_report_interpreter");
    let structured = compile_agent_dir(&root).unwrap();
    let graph = GraphAuthorDocument::from_verified_plan(
        GraphDocumentId::new("medical_input_contract_graph").unwrap(),
        structured.plan().clone(),
    )
    .unwrap();
    let graph = PublishedAgent::from_verified_graph(
        "medical_graph",
        "Medical Graph",
        "Graph normalization fixture.",
        graph,
    )
    .unwrap();

    let value = graph
        .normalize_input(json!({
            "report_text": "report",
            "question": "what does this mean?"
        }))
        .unwrap();
    let object = value.value().as_object().unwrap();
    assert!(!object.contains_key("image_url"));
    assert_eq!(object.get("messages"), Some(&json!([])));

    let error = graph
        .normalize_input(json!({
            "report_text": "report",
            "image_url": null,
            "question": "question"
        }))
        .unwrap_err();
    assert_eq!(error.code(), "AGENT_INPUT_CONTRACT_INVALID");
}

#[test]
fn deployment_publication_contextually_links_every_leaf_and_freezes_binding_identity() {
    let root = workspace_assets::workspace_path("agents/parallel_researcher");
    let published = std::sync::Arc::new(compile_agent_dir(&root).unwrap());
    let deployment =
        DeployedAgent::publish(published, &FixtureResolver, SubflowContractRegistry::new())
            .unwrap();
    deployment.linked_plan().unwrap();
    let diagnostic = deployment
        .risk_diagnostics()
        .first()
        .expect("fixture resolver deliberately publishes a non-idempotent effect warning");
    assert_eq!(diagnostic.code(), DeploymentRiskCode::NonIdempotentEffect);
    assert_eq!(diagnostic.code().as_str(), "NON_IDEMPOTENT_EFFECT");
    assert_eq!(diagnostic.severity(), DeploymentRiskSeverity::Warning);
    assert_eq!(diagnostic.severity().as_str(), "warning");
    assert!(!diagnostic.node().as_str().is_empty());
    assert_eq!(diagnostic.implementation(), "core.llm");
    let wire = serde_json::to_value(diagnostic).unwrap();
    assert_eq!(
        wire.as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "code".to_owned(),
            "implementation".to_owned(),
            "node".to_owned(),
            "severity".to_owned(),
        ])
    );
    let mut unknown_field = wire.clone();
    unknown_field
        .as_object_mut()
        .unwrap()
        .insert("message".to_owned(), json!("unbounded prose is forbidden"));
    assert!(serde_json::from_value::<DeploymentRiskDiagnostic>(unknown_field).is_err());
    assert!(deployment
        .deployment_revision_id()
        .as_str()
        .starts_with("deployrev_"));
    assert_eq!(
        deployment.versioned_plan().definition_revision_id(),
        deployment.published().definition_revision_id()
    );
    let stored = serde_json::to_value(deployment.versioned_plan()).unwrap();
    assert_eq!(
        stored["author_document"]["authoring_mode"],
        json!("structured")
    );
    assert!(stored["author_document"]
        .as_object()
        .is_some_and(|document| !document.contains_key("input_normalization")));
}

#[test]
fn persistence_policy_is_immutable_and_part_of_deployment_identity() {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - return: done
"#;
    let published = published_fixture("persistence_identity", source);
    let full = DeployedAgent::publish(
        Arc::clone(&published),
        &FixtureResolver,
        SubflowContractRegistry::new(),
    )
    .unwrap();
    let terminal = DeployedAgent::publish_with_persistence_policy(
        published,
        &FixtureResolver,
        SubflowContractRegistry::new(),
        DeploymentPersistencePolicy::terminal_only(false, Duration::from_secs(30)).unwrap(),
    )
    .unwrap();

    assert_eq!(full.persistence_mode(), PersistenceMode::Full);
    assert_eq!(terminal.persistence_mode(), PersistenceMode::TerminalOnly);
    assert_ne!(
        full.versioned_plan().binding_hash(),
        terminal.versioned_plan().binding_hash()
    );
    assert_ne!(
        full.deployment_revision_id(),
        terminal.deployment_revision_id()
    );
    let wire = serde_json::to_value(terminal.versioned_plan()).unwrap();
    assert!(wire["resolved_bindings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|document| {
            document["deployment_policy"]["persistence_mode"] == json!("terminal_only")
                && document["deployment_policy"]["allow_volatile_waits"] == json!(false)
                && document["deployment_policy"]["execution_budget_ms"] == json!(30_000)
        }));
}

#[test]
fn omitted_deployment_policy_defaults_to_full_and_explicit_terminal_only_obeys_feature_gate() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("terminal_agent");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join("agent.yaml"),
        r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: terminal_agent
  name: Terminal Agent
execution:
  persistence_mode: terminal_only
inputs: {}
output: string
workflow:
  steps:
    - return: done
"#,
    )
    .unwrap();
    let enabled = BTreeSet::from(["terminal_agent".to_owned()]);
    let catalog = compile_enabled_agents(directory.path(), &enabled).unwrap();
    assert_eq!(
        catalog
            .get("terminal_agent")
            .unwrap()
            .declared_persistence_mode(),
        Some(PersistenceMode::TerminalOnly)
    );

    let disabled = deploy_agents(&catalog, &FixtureResolver).unwrap_err();
    assert_eq!(disabled.code(), "TERMINAL_ONLY_DISABLED");

    let terminal = deploy_agents_with_persistence(
        &catalog,
        &FixtureResolver,
        PersistenceMode::Full,
        TerminalOnlyDeploymentConfig::new(true, false, Duration::from_secs(30)).unwrap(),
    )
    .unwrap();
    assert_eq!(
        terminal[0].persistence_mode(),
        PersistenceMode::TerminalOnly
    );

    let implicit = published_fixture(
        "implicit_full",
        "api_version: insight.agent/v1\nkind: agent\ninputs: {}\noutput: string\nworkflow:\n  steps:\n    - return: done\n",
    );
    let implicit =
        DeployedAgent::publish(implicit, &FixtureResolver, SubflowContractRegistry::new()).unwrap();
    assert_eq!(implicit.persistence_mode(), PersistenceMode::Full);
}

#[test]
fn terminal_only_validator_rejects_durable_waits_long_timers_and_effect_fences() {
    let terminal_policy =
        DeploymentPersistencePolicy::terminal_only(false, Duration::from_millis(20)).unwrap();
    let volatile_policy =
        DeploymentPersistencePolicy::terminal_only(true, Duration::from_millis(20)).unwrap();

    let signal = published_fixture(
        "terminal_signal",
        "api_version: insight.agent/v1\nkind: agent\ninputs: {}\noutput: string\nworkflow:\n  steps:\n    - id: gate\n      wait: {signal: continue, response: string}\n    - return: $gate\n",
    );
    let error = DeployedAgent::publish_with_persistence_policy(
        signal,
        &FixtureResolver,
        SubflowContractRegistry::new(),
        volatile_policy,
    )
    .unwrap_err();
    assert_eq!(error.code(), "TERMINAL_ONLY_WORKFLOW_INCOMPATIBLE");

    let short_timer = published_fixture(
        "terminal_short_timer",
        "api_version: insight.agent/v1\nkind: agent\ninputs: {}\noutput: string\nworkflow:\n  steps:\n    - id: delay\n      wait: {duration_ms: 20}\n    - return: done\n",
    );
    DeployedAgent::publish_with_persistence_policy(
        short_timer,
        &FixtureResolver,
        SubflowContractRegistry::new(),
        volatile_policy,
    )
    .unwrap();

    let long_timer = published_fixture(
        "terminal_long_timer",
        "api_version: insight.agent/v1\nkind: agent\ninputs: {}\noutput: string\nworkflow:\n  steps:\n    - id: delay\n      wait: {duration_ms: 21}\n    - return: done\n",
    );
    let error = DeployedAgent::publish_with_persistence_policy(
        long_timer,
        &FixtureResolver,
        SubflowContractRegistry::new(),
        volatile_policy,
    )
    .unwrap_err();
    assert_eq!(error.code(), "TERMINAL_ONLY_WORKFLOW_INCOMPATIBLE");

    let loop_without_deadline = published_fixture(
        "terminal_unbounded_loop",
        "api_version: insight.agent/v1\nkind: agent\ninputs: {seed: string}\noutput: string\nworkflow:\n  steps:\n    - id: repeat\n      loop:\n        initial: $seed\n        until: true\n        max_iterations: 2\n        steps:\n          - id: next_state\n            type: retrieval\n            retrieval: fixture.search\n            inputs: {query: $state}\n            response: string\n          - yield: $next_state\n    - return: $repeat\n",
    );
    let error = DeployedAgent::publish_with_persistence_policy(
        loop_without_deadline,
        &SafeFixtureResolver,
        SubflowContractRegistry::new(),
        volatile_policy,
    )
    .unwrap_err();
    assert_eq!(error.code(), "TERMINAL_ONLY_WORKFLOW_INCOMPATIBLE");

    let bounded_loop = published_fixture(
        "terminal_bounded_loop",
        "api_version: insight.agent/v1\nkind: agent\ninputs: {seed: string}\noutput: string\nworkflow:\n  steps:\n    - id: repeat\n      loop:\n        initial: $seed\n        until: true\n        max_iterations: 2\n        deadline_ms: 20\n        steps:\n          - id: next_state\n            type: retrieval\n            retrieval: fixture.search\n            inputs: {query: $state}\n            response: string\n          - yield: $next_state\n    - return: $repeat\n",
    );
    DeployedAgent::publish_with_persistence_policy(
        bounded_loop,
        &SafeFixtureResolver,
        SubflowContractRegistry::new(),
        DeploymentPersistencePolicy::terminal_only(true, Duration::from_millis(40)).unwrap(),
    )
    .unwrap();

    let human = published_fixture(
        "terminal_human",
        "api_version: insight.agent/v1\nkind: agent\ninputs: {}\noutput: string\nworkflow:\n  steps:\n    - id: review\n      human_task: {signal: review, request: Review, response: string}\n    - return: $review\n",
    );
    let error = DeployedAgent::publish_with_persistence_policy(
        human,
        &FixtureResolver,
        SubflowContractRegistry::new(),
        volatile_policy,
    )
    .unwrap_err();
    assert_eq!(error.code(), "TERMINAL_ONLY_WORKFLOW_INCOMPATIBLE");

    let fenced_action = published_fixture(
        "terminal_fenced_action",
        "api_version: insight.agent/v1\nkind: agent\ninputs: {}\noutput: string\nworkflow:\n  steps:\n    - type: action\n      id: mutate\n      call: fixture.mutate\n      inputs: {}\n      response: string\n    - return: $mutate\n",
    );
    let error = DeployedAgent::publish_with_persistence_policy(
        fenced_action,
        &FixtureResolver,
        SubflowContractRegistry::new(),
        terminal_policy,
    )
    .unwrap_err();
    assert_eq!(error.code(), "TERMINAL_ONLY_WORKFLOW_INCOMPATIBLE");
}

#[test]
fn terminal_only_validator_rejects_worker_retry_and_aggregate_workflow_over_budget() {
    let policy =
        DeploymentPersistencePolicy::terminal_only(true, Duration::from_millis(20)).unwrap();
    let worker = published_fixture(
        "terminal_retry_budget",
        "api_version: insight.agent/v1\nkind: agent\ninputs: {query: string}\noutput: string\nworkflow:\n  steps:\n    - id: search\n      type: retrieval\n      retrieval: fixture.search\n      inputs: {query: $query}\n      response: string\n    - return: $search\n",
    );
    let error = DeployedAgent::publish_with_persistence_policy(
        worker,
        &RetryingFixtureResolver,
        SubflowContractRegistry::new(),
        policy,
    )
    .unwrap_err();
    assert_eq!(error.code(), "TERMINAL_ONLY_WORKFLOW_INCOMPATIBLE");
    assert!(error.message().contains("retry, backoff, and timeout"));

    let aggregate = published_fixture(
        "terminal_aggregate_budget",
        "api_version: insight.agent/v1\nkind: agent\ninputs: {query: string}\noutput: string\nworkflow:\n  steps:\n    - id: delay\n      wait: {duration_ms: 10}\n    - id: search\n      type: retrieval\n      retrieval: fixture.search\n      inputs: {query: $query}\n      response: string\n    - return: $search\n",
    );
    let error = DeployedAgent::publish_with_persistence_policy(
        aggregate,
        &SafeFixtureResolver,
        SubflowContractRegistry::new(),
        policy,
    )
    .unwrap_err();
    assert_eq!(error.code(), "TERMINAL_ONLY_WORKFLOW_INCOMPATIBLE");
    assert!(error.message().contains("conservative workflow"));

    let loop_then_timer = published_fixture(
        "terminal_loop_then_timer_budget",
        "api_version: insight.agent/v1\nkind: agent\ninputs: {seed: string}\noutput: string\nworkflow:\n  steps:\n    - id: repeat\n      loop:\n        initial: $seed\n        until: true\n        max_iterations: 2\n        deadline_ms: 20\n        steps:\n          - id: next_state\n            type: retrieval\n            retrieval: fixture.search\n            inputs: {query: $state}\n            response: string\n          - yield: $next_state\n    - id: delay\n      wait: {duration_ms: 1}\n    - return: $repeat\n",
    );
    let error = DeployedAgent::publish_with_persistence_policy(
        loop_then_timer,
        &SafeFixtureResolver,
        SubflowContractRegistry::new(),
        DeploymentPersistencePolicy::terminal_only(true, Duration::from_millis(40)).unwrap(),
    )
    .unwrap_err();
    assert_eq!(error.code(), "TERMINAL_ONLY_WORKFLOW_INCOMPATIBLE");
    assert!(error.message().contains("conservative workflow"));
}

#[test]
fn deployment_identity_includes_plan_and_definition_even_when_bindings_are_empty() {
    let directory = tempfile::tempdir().unwrap();
    let write = |id: &str, output: &str| {
        let root = directory.path().join(id);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("agent.yaml"),
            format!(
                r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: {id}
  name: {id}
  description: Empty-binding deployment identity fixture.
inputs: {{}}
output: string
workflow:
  steps:
    - return: {output}
"#,
            ),
        )
        .unwrap();
    };
    write("first", "first");
    write("second", "second");
    let enabled = BTreeSet::from(["first".to_owned(), "second".to_owned()]);
    let published = compile_enabled_agents(directory.path(), &enabled).unwrap();
    let deployed = deploy_agents(&published, &FixtureResolver).unwrap();
    assert_eq!(
        deployed[0].versioned_plan().binding_hash(),
        deployed[1].versioned_plan().binding_hash(),
        "both deployments deliberately have the same empty binding projection"
    );
    assert_ne!(
        deployed[0].deployment_revision_id(),
        deployed[1].deployment_revision_id(),
        "definition/Plan identity must distinguish executable deployments"
    );
}

#[test]
fn catalog_deployment_topologically_pins_real_subflow_revision_and_interface() {
    let directory = tempfile::tempdir().unwrap();
    let child_dir = directory.path().join("child");
    std::fs::create_dir_all(&child_dir).unwrap();
    std::fs::write(
        child_dir.join("agent.yaml"),
        r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: child
  name: Child
  description: Exact child revision used by the parent.
inputs: {question: string}
output: string
workflow:
  steps:
    - return: $question
"#,
    )
    .unwrap();
    let child = compile_agent_dir(&child_dir).unwrap();

    let parent_dir = directory.path().join("parent");
    std::fs::create_dir_all(&parent_dir).unwrap();
    std::fs::write(
        parent_dir.join("agent.yaml"),
        format!(
            r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: parent
  name: Parent
  description: Calls one immutable child revision.
inputs: {{question: string}}
output: string
workflow:
  steps:
    - id: child_answer
      type: call
      definition_revision: {}
      interface_version: child-v1
      input: {{question: $question}}
      response: string
    - return: $child_answer
"#,
            child.definition_revision_id()
        ),
    )
    .unwrap();

    let enabled = BTreeSet::from(["child".to_owned(), "parent".to_owned()]);
    let published = compile_enabled_agents(directory.path(), &enabled).unwrap();
    let deployed = deploy_agents(&published, &FixtureResolver).unwrap();
    let child = deployed
        .iter()
        .find(|agent| agent.published().metadata().id == "child")
        .unwrap();
    let parent = deployed
        .iter()
        .find(|agent| agent.published().metadata().id == "parent")
        .unwrap();
    let linked = parent.linked_plan().unwrap();
    let call = parent
        .published()
        .plan()
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), insight_engine::plan::NodeKind::SubflowCall(_)))
        .unwrap();
    let contract = linked.subflow(call.id()).unwrap();
    assert_eq!(
        contract.execution_revision().deployment_revision_id(),
        child.deployment_revision_id()
    );
    assert_eq!(
        contract.execution_revision().plan_hash(),
        child.versioned_plan().plan_hash()
    );
    assert_eq!(
        contract.execution_revision().binding_hash(),
        child.versioned_plan().binding_hash()
    );
    assert_eq!(contract.interface_version().as_str(), "child-v1");
}
