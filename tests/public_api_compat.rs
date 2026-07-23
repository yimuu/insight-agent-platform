//! Downstream compile fixture for the root public facade.
//!
//! `scripts/check-public-api-baseline.sh` freezes the complete normalized API
//! inventory.  This independently compiled downstream fixture also proves
//! source-level path resolution, type identity, and high-risk relationships.

use std::{future::Future, sync::Arc};

use axum::{http::HeaderMap, Router};
use insight_agent_platform::{
    api::v1::{build_router, ApiAuth, ApiState, HumanPrincipalResolver, ResolvedHumanPrincipal},
    config::AuthConfig,
    dsl::{
        v3::{compile_source, CompileOptions, GraphAuthorDocument, GraphSurfaceRepository},
        CompileError,
    },
    engine::{
        plan::Plan as NestedPlan,
        repository::{
            ClaimSchedulerRunCommand, FencedSchedulerRunCommand, HumanTaskDurableRepository,
            ModelToolTaskClaim, PostgresDurableRepository, PublicEventOutboxRepository,
            RecoveryDurableRepository, RepositoryError, RuntimeIngressDurableRepository,
            SchedulerDurableRepository, SqliteDurableRepository, VersionedPlan,
        },
        DeploymentRevisionId, FrozenRetrievalTarget, Plan, RunId, TaskExecutionRequest,
        TransitionKey,
    },
    resources::retrievals::RegisteredRetrieval,
    runtime::{
        v3_service::PendingMigrationWait, ProductionRunRepository, RunRepositoryCapability,
        RunService,
    },
};

fn expect_future<T>(_: impl Future<Output = T>) {}

fn assert_production_supertraits<R: ProductionRunRepository>(repository: &R) {
    fn scheduler<T: SchedulerDurableRepository + ?Sized>(_: &T) {}
    fn public_events<T: PublicEventOutboxRepository + ?Sized>(_: &T) {}
    fn ingress<T: RuntimeIngressDurableRepository + ?Sized>(_: &T) {}
    fn recovery<T: RecoveryDurableRepository + ?Sized>(_: &T) {}
    fn human_tasks<T: HumanTaskDurableRepository + ?Sized>(_: &T) {}
    fn graph<T: GraphSurfaceRepository + ?Sized>(_: &T) {}

    scheduler(repository);
    public_events(repository);
    ingress(repository);
    recovery(repository);
    human_tasks(repository);
    graph(repository);
}

#[allow(dead_code)]
fn assert_production_associated_items<R: ProductionRunRepository>(
    repository: &R,
    run_id: &RunId,
    transition_key: TransitionKey,
    claim: ClaimSchedulerRunCommand,
    fence: FencedSchedulerRunCommand,
) {
    let _: RunRepositoryCapability = repository.run_repository_capability();
    expect_future::<Result<(), RepositoryError>>(repository.check_production_health());
    expect_future::<Result<Option<serde_json::Value>, RepositoryError>>(
        repository.load_production_run_input(run_id),
    );
    expect_future::<Result<Option<FencedSchedulerRunCommand>, RepositoryError>>(
        repository.claim_production_scheduler_run(transition_key.clone(), claim),
    );
    expect_future::<Result<(), RepositoryError>>(
        repository.release_production_scheduler_run(transition_key, fence),
    );
    expect_future::<Result<Vec<PendingMigrationWait>, RepositoryError>>(
        repository.load_pending_migration_waits(run_id),
    );
}

fn assert_production_impl<T: ProductionRunRepository>() {}

#[allow(dead_code)]
fn assert_verified_graph_constructor_call(
    graph: &GraphAuthorDocument,
    deployment_revision_id: DeploymentRevisionId,
) {
    let _: Result<VersionedPlan, RepositoryError> = VersionedPlan::from_verified_graph(
        "phase0-definition",
        "phase0-agent",
        "Phase 0 agent",
        deployment_revision_id,
        "phase0-expression-engine",
        graph,
        serde_json::Value::Null,
        serde_json::Value::Null,
        serde_json::Value::Null,
    );
}

// A representative downstream-owned implementation proves that the facade
// still exposes implementable traits, not merely nameable trait objects.
struct DownstreamPrincipalResolver;

impl HumanPrincipalResolver for DownstreamPrincipalResolver {
    fn resolve(&self, _headers: &HeaderMap) -> Option<ResolvedHumanPrincipal> {
        ResolvedHumanPrincipal::new("phase0-user", vec!["operators".to_owned()])
    }
}

#[test]
fn root_facade_keeps_key_paths_signatures_and_type_identity() {
    let _: fn(&str, CompileOptions) -> Result<Plan, CompileError> = compile_source;
    let _: fn(ApiState) -> Router = build_router;

    fn root_plan_is_nested_plan(value: Plan) -> NestedPlan {
        value
    }
    let _ = root_plan_is_nested_plan as fn(Plan) -> NestedPlan;

    fn accept_run_service(_: RunService) {}
    let _ = accept_run_service as fn(RunService);

    // These inherent methods currently cross the intended workspace crate
    // boundary.  Their exact downstream call signatures must remain usable
    // while ownership is split across engine/durable/resources crates.
    let _: fn(&ModelToolTaskClaim) -> Result<TaskExecutionRequest, &'static str> =
        TaskExecutionRequest::from_model_tool_claim;
    let _: fn(&FrozenRetrievalTarget, &RegisteredRetrieval) -> Result<(), &'static str> =
        FrozenRetrievalTarget::validate_registered;

    // This conversion is easy to drop when API/config ownership moves even
    // though both names remain publicly reachable.
    fn assert_auth_conversion()
    where
        for<'a> ApiAuth: From<&'a AuthConfig>,
    {
    }
    assert_auth_conversion();
}

#[test]
fn production_repository_contract_and_public_impls_remain_available() {
    assert_production_impl::<SqliteDurableRepository>();
    assert_production_impl::<PostgresDurableRepository>();

    fn accept_object(_: Arc<dyn ProductionRunRepository>) {}
    let _ = accept_object as fn(Arc<dyn ProductionRunRepository>);

    fn exercise_supertraits<R: ProductionRunRepository>(repository: &R) {
        assert_production_supertraits(repository);
    }
    let _ = exercise_supertraits::<SqliteDurableRepository>;
    let _ = exercise_supertraits::<PostgresDurableRepository>;
}

#[test]
fn downstream_trait_impl_fixture_remains_usable() {
    let resolver: Arc<dyn HumanPrincipalResolver> = Arc::new(DownstreamPrincipalResolver);
    let auth = insight_agent_platform::api::v1::ApiAuth::disabled()
        .with_human_principal_resolver(resolver);
    drop(auth);
}
