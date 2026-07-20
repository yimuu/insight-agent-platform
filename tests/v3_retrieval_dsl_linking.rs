use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use insight_agent_platform::{
    catalog_v3::{DeployedV3Agent, ProductionLeafDeploymentResolver, PublishedV3Agent},
    dsl::v3::{compile_source, CompileOptions, GraphAuthorDocument, GraphDocumentId},
    engine::{
        plan::{DescriptorValue, LeafTaskKind, NodeKind, PlanIndex, SubflowContractRegistry},
        DefinitionRevisionId, RunId, RuntimeValue, SchedulerAction, SchedulerDecision,
        SchedulerFacts, SchedulerPlanner, SchedulerTaskKind,
    },
    resources::{
        actions::{ActionRegistry, CancellationClass, EffectClass, IdempotencyClass},
        models::ModelRegistry,
        retrievals::{
            Retrieval, RetrievalCapability, RetrievalContext, RetrievalDescriptor,
            RetrievalExecutionResult, RetrievalPublicPolicy, RetrievalRegistry,
        },
    },
    runtime::RunError,
};
use serde_json::{json, Value};

const RETRIEVAL_SOURCE: &str = r#"api_version: insight.agent/v3
kind: agent
types:
  SearchOutput:
    fields:
      answer: string
inputs:
  question: string
output: SearchOutput
workflow:
  steps:
    - type: retrieval
      id: search
      retrieval: medical.search
      inputs:
        query: $question
      response: SearchOutput
    - return: $search
"#;

fn compile_options(source: &str) -> CompileOptions {
    CompileOptions::new(
        DefinitionRevisionId::new("retrieval_dsl_fixture_revision").unwrap(),
        "retrieval/agent.yaml",
        source,
    )
}

fn published(source: &str, id: &str) -> Arc<PublishedV3Agent> {
    let graph = GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new(format!("{id}_graph")).unwrap(),
        source,
        compile_options(source),
    )
    .unwrap();
    Arc::new(PublishedV3Agent::from_verified_graph(id, id, "retrieval fixture", graph).unwrap())
}

#[derive(Clone)]
struct SearchRetrieval;

#[async_trait]
impl Retrieval for SearchRetrieval {
    fn descriptor(&self) -> RetrievalDescriptor {
        RetrievalDescriptor {
            id: "medical.search",
            version: "2.3.4",
            input_schema: json!({
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
                "additionalProperties": false
            }),
            output_schema: json!({
                "type": "object",
                "properties": {"answer": {"type": "string"}},
                "required": ["answer"],
                "additionalProperties": false
            }),
            query_field: "query",
            effect: EffectClass::ReadOnly,
            idempotency: IdempotencyClass::Idempotent,
            cancellation: CancellationClass::Cooperative,
            required_capabilities: BTreeSet::from([RetrievalCapability::new(
                "medical.search.read",
            )]),
        }
    }

    fn public_policy(&self) -> RetrievalPublicPolicy {
        RetrievalPublicPolicy {
            query: true,
            result_schema: Some(json!({
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "title": {"type": "string"},
                    "metadata": {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": false
                    }
                },
                "required": ["id", "title"],
                "additionalProperties": false
            })),
        }
    }

    async fn retrieve(
        &self,
        _input: Value,
        _context: RetrievalContext,
    ) -> Result<RetrievalExecutionResult, RunError> {
        Ok(RetrievalExecutionResult::new(
            json!({"answer": "fixture"}),
            Some(json!([{"id": "doc_fixture", "title": "fixture", "metadata": {}}])),
        ))
    }
}

fn retrieval_registry() -> RetrievalRegistry {
    let mut registry = RetrievalRegistry::default();
    registry.register(SearchRetrieval).unwrap();
    registry
}

#[test]
fn retrieval_is_a_first_class_leaf_and_publish_defaults_private_across_roundtrip() {
    let plan = compile_source(RETRIEVAL_SOURCE, compile_options(RETRIEVAL_SOURCE)).unwrap();
    let retrieval = plan
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), NodeKind::RetrievalTask(_)))
        .expect("retrieval leaf is represented by its own Plan node kind");
    let NodeKind::RetrievalTask(descriptor) = retrieval.kind() else {
        unreachable!();
    };
    assert_eq!(descriptor.implementation, "medical.search");
    assert_eq!(descriptor.descriptor_version.as_str(), "1");
    assert_eq!(
        descriptor.public_configuration.get("publish"),
        Some(&DescriptorValue::Boolean(false))
    );
    assert_eq!(
        PlanIndex::new(&plan)
            .unwrap()
            .leaf_descriptor(retrieval.id())
            .unwrap()
            .kind(),
        LeafTaskKind::Retrieval
    );

    let native = GraphAuthorDocument::from_verified_plan(
        GraphDocumentId::new("native_retrieval_roundtrip").unwrap(),
        plan,
    )
    .unwrap();
    let reduced = native.to_structured().unwrap();
    let recompiled = compile_source(reduced.source(), compile_options(reduced.source())).unwrap();
    assert_eq!(native.semantic_hash(), recompiled.semantic_hash());
    assert!(reduced.source().contains("\"type\": \"retrieval\""));
    assert!(reduced.source().contains("\"publish\": false"));
}

#[test]
fn retrieval_author_surface_is_closed_and_requires_object_inputs() {
    let missing_inputs = RETRIEVAL_SOURCE.replace("      inputs:\n        query: $question\n", "");
    assert!(compile_source(&missing_inputs, compile_options(&missing_inputs)).is_err());

    let invalid_publish = RETRIEVAL_SOURCE.replace(
        "      response: SearchOutput",
        "      publish: yes\n      response: SearchOutput",
    );
    assert!(compile_source(&invalid_publish, compile_options(&invalid_publish)).is_err());

    let unknown = RETRIEVAL_SOURCE.replace(
        "      response: SearchOutput",
        "      arbitrary: true\n      response: SearchOutput",
    );
    assert!(compile_source(&unknown, compile_options(&unknown)).is_err());
}

#[test]
fn production_link_freezes_exact_retrieval_identity_and_effective_public_policy() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let retrievals = retrieval_registry();

    let private_agent = published(RETRIEVAL_SOURCE, "private_retrieval");
    let missing_registry = DeployedV3Agent::publish(
        Arc::clone(&private_agent),
        &ProductionLeafDeploymentResolver::new(&models, &actions),
        SubflowContractRegistry::new(),
    )
    .unwrap_err();
    assert_eq!(missing_registry.code(), "RETRIEVAL_REGISTRY_UNAVAILABLE");

    let deployment = DeployedV3Agent::publish(
        private_agent,
        &ProductionLeafDeploymentResolver::new(&models, &actions).with_retrievals(&retrievals),
        SubflowContractRegistry::new(),
    )
    .unwrap();
    let linked = deployment.linked_plan().unwrap();
    let node = deployment
        .published()
        .plan()
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), NodeKind::RetrievalTask(_)))
        .unwrap();
    let contract = linked.descriptor(node.id()).unwrap();
    let binding = contract.deployment_binding();
    let keys = binding
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        keys,
        BTreeSet::from([
            "adapter",
            "cancellation",
            "descriptor_hash",
            "effect",
            "effective_public_policy",
            "idempotency",
            "input_schema",
            "output_schema",
            "public",
            "publish",
            "query_field",
            "required_capabilities",
            "retrieval_id",
            "retrieval_version",
        ])
    );
    assert_eq!(binding["adapter"], json!("native_retrieval"));
    assert_eq!(binding["retrieval_id"], json!("medical.search"));
    assert_eq!(binding["retrieval_version"], json!("2.3.4"));
    assert_eq!(binding["query_field"], json!("query"));
    assert_eq!(binding["publish"], json!(false));
    assert_eq!(
        binding["effective_public_policy"],
        json!({"query": false, "result": null})
    );
    assert_eq!(binding["public"]["query"], json!(true));
    assert!(binding["public"]["result"].is_object());
    assert_eq!(contract.worker().task_kind(), LeafTaskKind::Retrieval);
    assert_eq!(contract.worker().worker_version().as_str(), "2.3.4");

    let public_source = RETRIEVAL_SOURCE.replace(
        "      response: SearchOutput",
        "      publish: true\n      response: SearchOutput",
    );
    let public_deployment = DeployedV3Agent::publish(
        published(&public_source, "public_retrieval"),
        &ProductionLeafDeploymentResolver::new(&models, &actions).with_retrievals(&retrievals),
        SubflowContractRegistry::new(),
    )
    .unwrap();
    let public_linked = public_deployment.linked_plan().unwrap();
    let public_node = public_deployment
        .published()
        .plan()
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), NodeKind::RetrievalTask(_)))
        .unwrap();
    let public_binding = public_linked
        .descriptor(public_node.id())
        .unwrap()
        .deployment_binding();
    assert_eq!(public_binding["publish"], json!(true));
    assert_eq!(
        public_binding["effective_public_policy"],
        public_binding["public"]
    );
}

#[test]
fn scheduler_dispatches_the_first_class_retrieval_task_kind() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let retrievals = retrieval_registry();
    let deployment = DeployedV3Agent::publish(
        published(RETRIEVAL_SOURCE, "scheduled_retrieval"),
        &ProductionLeafDeploymentResolver::new(&models, &actions).with_retrievals(&retrievals),
        SubflowContractRegistry::new(),
    )
    .unwrap();
    let linked = deployment.linked_plan().unwrap();
    let planner = SchedulerPlanner::new(&linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_retrieval_scheduler_fixture").unwrap(),
        0,
        RuntimeValue::new(json!({"question": "what changed?"})).unwrap(),
    );

    for _ in 0..4 {
        let SchedulerDecision::Action(planned) = planner.plan(&facts).unwrap() else {
            panic!("retrieval scheduler quiesced before dispatch");
        };
        match planned.intent().action() {
            SchedulerAction::AdmitActivation { activation_id, .. } => {
                facts.record_activation(activation_id.clone());
            }
            SchedulerAction::DispatchTask { task_kind, .. } => {
                assert_eq!(*task_kind, SchedulerTaskKind::Retrieval);
                return;
            }
            action => panic!("unexpected action before retrieval dispatch: {action:?}"),
        }
        facts.commit_checkpoint(planned.intent().checkpoint_id().clone());
        facts.set_projection_version(facts.projection_version() + 1);
    }
    panic!("scheduler did not dispatch retrieval");
}
