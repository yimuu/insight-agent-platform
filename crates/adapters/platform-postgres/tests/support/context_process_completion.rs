//! Drive a recovered Context result through its original Node and the real controller worker.
use super::*;
use insight_platform_orchestrator::ExternalLeafCompletionOwner;

#[allow(clippy::too_many_arguments)]
pub(super) async fn complete_with_orchestration(
    pool: &PgPool,
    fixture: &Fixture,
    database_url: &str,
    orchestration_binary: &str,
    prefix: &Path,
    tls: &ContextProcessMtlsFixture,
    query_id: &ResourceId,
    context_job_id: &ResourceId,
) {
    let node_state: String = sqlx::query_scalar(
        "SELECT state FROM insight_platform.run_nodes WHERE tenant_id=$1 AND run_id=$2 AND node_id=$3 AND record_kind='node_execution' AND node_kind='context_query'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(fixture.node_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(node_state, "ready");
    let mut continuations: Vec<(String, i32, serde_json::Value, String)> = sqlx::query_as(
        "SELECT job_id,payload_schema_version,payload,payload_digest FROM insight_platform.jobs WHERE tenant_id=$1 AND run_id=$2 AND node_id=$3 AND owner_id=$3 AND job_kind='orchestration_node' AND work_class='orchestration' AND state='ready'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(fixture.node_id.to_string())
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(
        continuations.len(),
        1,
        "one continuation for the original Node"
    );
    let (marker_job_id, schema_version, value, digest) = continuations.pop().unwrap();
    let marker = OrchestrationJobPayload::from_payload(&TypedPayload {
        schema_version,
        value,
        digest,
    })
    .unwrap();
    assert_eq!(marker.node_execution_id, fixture.node_id);
    let completion = marker.external_leaf_completion.unwrap();
    assert_eq!(
        completion.owner,
        ExternalLeafCompletionOwner::Context {
            context_query_id: query_id.clone(),
            context_job_id: context_job_id.clone(),
        }
    );
    assert_ne!(
        completion.source_orchestration_job_id.to_string(),
        marker_job_id
    );
    let output: (String, String, String) = sqlx::query_as(
        "SELECT value.value_id,value.schema_digest,value.content_digest FROM insight_platform.invocations invocation JOIN insight_platform.run_values value ON value.tenant_id=invocation.tenant_id AND value.run_id=invocation.run_id AND value.node_id=invocation.node_id AND value.value_id=invocation.output_value_id WHERE invocation.tenant_id=$1 AND invocation.run_id=$2 AND invocation.node_id=$3 AND invocation.invocation_id=$4 AND invocation.invocation_kind='context' AND invocation.state='succeeded' AND invocation.terminal_at IS NOT NULL AND value.value_kind='context_observation'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(fixture.node_id.to_string())
    .bind(query_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        output,
        (
            completion.output.value_id.to_string(),
            completion.output.schema_digest.to_string(),
            completion.output.content_digest.to_string(),
        )
    );
    let context_jobs: Vec<(String, String, i32)> = sqlx::query_as(
        "SELECT job_id,state,attempt_no FROM insight_platform.jobs WHERE tenant_id=$1 AND run_id=$2 AND work_class='context' ORDER BY job_id",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(
        context_jobs,
        vec![(context_job_id.to_string(), "succeeded".to_owned(), 2)]
    );

    // The shared seed includes an unrelated text2sql placeholder without a Job.
    sqlx::query(
        "UPDATE insight_platform.run_nodes SET state='cancelled',version=version+1,terminal_at=statement_timestamp(),updated_at=statement_timestamp() WHERE tenant_id=$1 AND run_id=$2 AND node_id=$3 AND state='running'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(fixture.text2sql_node_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let plan_broker = Arc::new(TypedPlanArtifactBroker {
        bytes: canonical_json(&serde_json::to_value(&fixture.runtime_plan).unwrap()).unwrap(),
        reads: AtomicUsize::new(0),
    });
    let artifact_incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let artifact_address = artifact_incoming.local_addr().unwrap();
    let artifact_service = ArtifactSchedulerServiceServer::new(ArtifactSchedulerGrpcService::new(
        plan_broker.clone(),
        ArtifactInternalRpcLimits::new(262_144, 262_144).unwrap(),
    ));
    let artifact_service = tonic::service::interceptor::InterceptedService::new(
        artifact_service,
        SchedulerWorkloadIdentity,
    );
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
    let artifact_server = tokio::spawn(
        Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(
                        &tls.artifact_server_certificate_pem,
                        &tls.artifact_server_key_pem,
                    ))
                    .client_ca_root(Certificate::from_pem(tls.ca_pem.clone())),
            )
            .unwrap()
            .add_service(artifact_service)
            .serve_with_incoming_shutdown(artifact_incoming, async move {
                let _ = shutdown_receiver.await;
            }),
    );
    let completion_prefix = PathBuf::from(format!("{}-completion", prefix.display()));
    let ca_path = PathBuf::from(format!("{}-ca.pem", completion_prefix.display()));
    let cert_path = PathBuf::from(format!("{}-scheduler.pem", completion_prefix.display()));
    let key_path = PathBuf::from(format!("{}-scheduler-key.pem", completion_prefix.display()));
    std::fs::write(&ca_path, &tls.ca_pem).unwrap();
    std::fs::write(&cert_path, &tls.scheduler_certificate_pem).unwrap();
    std::fs::write(&key_path, &tls.scheduler_key_pem).unwrap();
    let config = orchestration_process_config(format!("https://{artifact_address}/"));
    let (config_path, config_digest) =
        write_context_process_config(&completion_prefix, "orchestration", &config);
    let mut worker = spawn_orchestration_worker(
        orchestration_binary,
        &config_path,
        &config_digest,
        database_url,
        &ca_path,
        &cert_path,
        &key_path,
    );
    let worker_log = observe_context_worker_start(&mut worker);
    wait_run_state(pool, &fixture.run_id, "succeeded").await;
    worker.kill().unwrap();
    worker.wait().unwrap();
    worker_log.join().unwrap();
    assert!(plan_broker.reads.load(Ordering::SeqCst) >= 1);
    let _ = shutdown_sender.send(());
    tokio::time::timeout(StdDuration::from_secs(5), artifact_server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for path in [ca_path, cert_path, key_path, config_path] {
        std::fs::remove_file(path).unwrap();
    }

    // This frozen Plan returns the Run input; the Context result is the independently
    // validated structural completion above and is not implicitly the Run output.
    let RuntimeNode::Return {
        value: ExactDataPortRef::RunInput { schema_digest },
    } = fixture
        .runtime_plan
        .node(&PlanNodeKey::new("finish".to_owned()).unwrap())
        .unwrap()
    else {
        panic!("fixture Return must identify its frozen RunInput");
    };
    let expected_output: String = sqlx::query_scalar(
        "SELECT value.value_id FROM insight_platform.runs run JOIN insight_platform.run_values value ON value.tenant_id=run.tenant_id AND value.run_id=run.run_id AND value.value_id=run.input_value_id WHERE run.tenant_id=$1 AND run.run_id=$2 AND value.schema_digest=$3",
    ).bind(fixture.tenant_id.to_string()).bind(fixture.run_id.to_string()).bind(schema_digest.to_string()).fetch_one(pool).await.unwrap();
    let terminal: (String, String, i32, String, String, i64, i64) = sqlx::query_as(
        r#"SELECT run.state,run.output_value_id,run.active_work_count,
            (SELECT state FROM insight_platform.run_nodes WHERE tenant_id=$1 AND run_id=$2 AND node_id=$3),
            (SELECT state FROM insight_platform.jobs WHERE tenant_id=$1 AND run_id=$2 AND job_id=$4),
            (SELECT count(*) FROM insight_platform.run_nodes WHERE tenant_id=$1 AND run_id=$2 AND node_kind='return' AND plan_node_key='finish'),
            (SELECT count(*) FROM insight_platform.run_nodes node JOIN insight_platform.jobs job ON job.tenant_id=node.tenant_id AND job.run_id=node.run_id AND job.node_id=node.node_id WHERE node.tenant_id=$1 AND node.run_id=$2 AND node.node_kind='return' AND node.plan_node_key='finish' AND node.state='succeeded' AND job.state='succeeded')
        FROM insight_platform.runs run WHERE run.tenant_id=$1 AND run.run_id=$2"#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(fixture.node_id.to_string())
    .bind(&marker_job_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        terminal,
        (
            "succeeded".to_owned(),
            expected_output,
            0,
            "succeeded".to_owned(),
            "succeeded".to_owned(),
            1,
            1,
        )
    );
    let after_context_jobs: Vec<(String, String, i32)> = sqlx::query_as(
        "SELECT job_id,state,attempt_no FROM insight_platform.jobs WHERE tenant_id=$1 AND run_id=$2 AND work_class='context' ORDER BY job_id",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(
        after_context_jobs, context_jobs,
        "completion cannot dispatch Context again"
    );
}
