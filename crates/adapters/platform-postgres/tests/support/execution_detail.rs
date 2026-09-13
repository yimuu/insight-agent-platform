//! Disposable-fixture qualification of current execution reads and exact value associations.
use super::*;
use insight_platform_contracts::PublicRunEventSourceKind as K;
pub(super) async fn verify() {
    let url = std::env::var("PLATFORM_LIVE_TEST_DATABASE_URL").expect("explicit fixture");
    assert!(url
        .rsplit('/')
        .next()
        .unwrap()
        .starts_with("insight_live_fixture_"));
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    insight_platform_postgres::provision_schema(&pool)
        .await
        .unwrap();
    let repo = PgRepository::new(pool.clone());
    let f = seed_fixture(&pool, &repo).await;
    let command = command_for_node(&f, &f.primary_node_id, 0x4100);
    execute_create(&repo, command.clone()).await.unwrap();
    let read = |kind, source: ResourceId| {
        let repo = repo.clone();
        let f = &f;
        async move {
            repo.read_run_execution_for_principal(
                &f.tenant_id,
                &f.principal_id,
                PrincipalKind::AgentRunner,
                &f.run_id,
                kind,
                &source,
            )
            .await
        }
    };
    let model = read(K::ModelTurn, command.model_turn_id.clone())
        .await
        .unwrap();
    assert_eq!(model.source_id, command.model_turn_id);
    assert_eq!(model.node_execution_id, f.primary_node_id);
    assert_eq!(model.values.len(), 1);
    assert_eq!(
        model.input_value_id.as_ref(),
        Some(&model.values[0].value_id)
    );
    assert_eq!(model.output_value_id, None);
    assert!(read(K::NodeExecution, command.model_turn_id.clone())
        .await
        .is_err());
    assert!(repo
        .read_run_execution_for_principal(
            &f.tenant_id,
            &f.principal_id,
            PrincipalKind::AgentRunner,
            &id(ResourceKind::Run, 0x4999),
            K::ModelTurn,
            &command.model_turn_id
        )
        .await
        .is_err());
    assert!(repo
        .read_run_execution_for_principal(
            &id(ResourceKind::Tenant, 0x4998),
            &f.principal_id,
            PrincipalKind::AgentRunner,
            &f.run_id,
            K::ModelTurn,
            &command.model_turn_id
        )
        .await
        .is_err());
    // A legacy controller may have no terminal envelope: current state still controls the detail.
    sqlx::query("UPDATE insight_platform.run_nodes SET state='succeeded',terminal_at=clock_timestamp() WHERE tenant_id=$1 AND node_id=$2").bind(f.tenant_id.to_string()).bind(f.primary_node_id.to_string()).execute(&pool).await.unwrap();
    let node = read(K::NodeExecution, f.primary_node_id.clone())
        .await
        .unwrap();
    assert_eq!(node.state.as_str(), "succeeded");
    assert!(node.terminal_at.is_some());
    assert!(node.input_value_id.is_none() && node.output_value_id.is_none());
    // More than the closed metadata page is reported explicitly, not silently dropped.
    for n in 0..65 {
        sqlx::query("INSERT INTO insight_platform.run_values(tenant_id,value_id,run_id,node_id,value_kind,classification,schema_digest,content_digest,inline_value,artifact_id) SELECT tenant_id,$2,run_id,node_id,value_kind,classification,schema_digest,content_digest,inline_value,artifact_id FROM insight_platform.run_values WHERE tenant_id=$1 AND value_id=$3")
 .bind(f.tenant_id.to_string()).bind(id(ResourceKind::RunValue,0x4200+n).to_string()).bind(model.input_value_id.as_ref().unwrap().to_string()).execute(&pool).await.unwrap();
    }
    let node = read(K::NodeExecution, f.primary_node_id.clone())
        .await
        .unwrap();
    assert_eq!(node.values.len(), 64);
    assert!(node.values_truncated);
    let model_again = read(K::ModelTurn, command.model_turn_id.clone())
        .await
        .unwrap();
    assert_eq!(model_again.values.len(), 1);
    assert!(!model_again.values_truncated);
    // A forged parent association cannot turn an invocation into a detail for another Run.
    sqlx::query("UPDATE insight_platform.run_nodes SET record_kind='scope_instance',plan_node_key=NULL,activation_ordinal=NULL WHERE tenant_id=$1 AND node_id=$2").bind(f.tenant_id.to_string()).bind(f.primary_node_id.to_string()).execute(&pool).await.unwrap();
    assert!(read(K::ModelTurn, command.model_turn_id.clone())
        .await
        .is_err());
    sqlx::query("UPDATE insight_platform.tenant_principals SET state='revoked' WHERE tenant_id=$1 AND principal_id=$2").bind(f.tenant_id.to_string()).bind(f.principal_id.to_string()).execute(&pool).await.unwrap();
    assert!(read(K::NodeExecution, f.primary_node_id.clone())
        .await
        .is_err());
    pool.close().await;
}
