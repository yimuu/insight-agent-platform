use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use insight_dsl::{CompileOptions, GraphAuthorDocument, GraphDocumentId};
use insight_engine::{
    execution::RunError,
    plan::{LeafTaskKind, NodeKind, SubflowContractRegistry},
    DefinitionRevisionId, RunId, RuntimeValue, SchedulerAction, SchedulerDecision, SchedulerFacts,
    SchedulerPlanner, SchedulerTaskKind,
};
use insight_resources::{
    actions::{ActionRegistry, CancellationClass, EffectClass, IdempotencyClass},
    models::ModelRegistry,
    retrievals::{
        Retrieval, RetrievalCapability, RetrievalContext, RetrievalDescriptor,
        RetrievalExecutionResult, RetrievalPublicPolicy, RetrievalRegistry,
    },
};
use insight_runtime::catalog::{DeployedAgent, ProductionLeafDeploymentResolver, PublishedAgent};
use serde_json::{json, Value};

const RETRIEVAL_SOURCE: &str = r#"api_version: insight.agent/v1
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

fn published(source: &str, id: &str) -> Arc<PublishedAgent> {
    let graph = GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new(format!("{id}_graph")).unwrap(),
        source,
        compile_options(source),
    )
    .unwrap();
    Arc::new(PublishedAgent::from_verified_graph(id, id, "retrieval fixture", graph).unwrap())
}

#[derive(Clone)]
struct SearchRetrieval;

#[async_trait]
impl Retrieval for SearchRetrieval {
    fn descriptor(&self) -> RetrievalDescriptor {
        RetrievalDescriptor {
            id: "medical.search".to_owned(),
            version: "2.3.4".to_owned(),
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
            query_field: "query".to_owned(),
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
fn production_link_freezes_exact_retrieval_identity_and_effective_public_policy() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let retrievals = retrieval_registry();

    let private_agent = published(RETRIEVAL_SOURCE, "private_retrieval");
    let missing_registry = DeployedAgent::publish(
        Arc::clone(&private_agent),
        &ProductionLeafDeploymentResolver::new(&models, &actions),
        SubflowContractRegistry::new(),
    )
    .unwrap_err();
    assert_eq!(missing_registry.code(), "RETRIEVAL_REGISTRY_UNAVAILABLE");

    let deployment = DeployedAgent::publish(
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
    let public_deployment = DeployedAgent::publish(
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
    let deployment = DeployedAgent::publish(
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
