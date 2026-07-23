use std::path::PathBuf;

use chrono::{Duration, Utc};
use insight_durable::{
    AcknowledgeArtifactDeletionCommand, ArtifactDurableRepository, ArtifactState,
    BindArtifactStoreAuthorityCommand, CreateRunCommand, DurableRepository, OrphanSweepCommand,
    PlanInstallOutcome, PutInlinePayloadCommand, StageArtifactCommand, VerifyArtifactCommand,
    VersionedPlan,
};
use insight_engine::{
    plan::{
        AuthorFormat, DataBinding, DataBindingId, DataPort, DataPortId, Node, NodeKind,
        PlanBuilder, PlanInputContract, PlanMetadata, PlanType, PortDirection, PortName,
        ReturnDescriptor, ScopeId, ScopeMetadata, ValueSource, VersionTag,
    },
    repository::{StorageLocator, REPOSITORY_ARTIFACT_STORE_CONFLICT, REPOSITORY_INTENT_CONFLICT},
    ArtifactId, ArtifactRef, ContentHash, DefinitionRevisionId, DeploymentRevisionId, NodeId,
    RunId, TransitionKey, TransitionOutcome,
};
use insight_storage::{
    artifact_store::{LocalContentAddressedArtifactStore, WorkerArtifactStore},
    PostgresDurableRepository, SqliteDurableRepository,
};
use serde_json::{json, Value};
use sqlx::{
    postgres::PgPoolOptions,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    AssertSqlSafe,
};

fn key(label: &str) -> TransitionKey {
    TransitionKey::derive("artifact.repository.test", &[label]).unwrap()
}

fn verified_plan(label: &str) -> insight_engine::plan::Plan {
    let return_id = NodeId::new("return_node").unwrap();
    let root_id = ScopeId::new("root_scope").unwrap();
    let value_input = DataPortId::new("return_value").unwrap();
    let safe_error = PlanType::safe_error().unwrap();
    let mut builder = PlanBuilder::new(PlanMetadata::new(
        DefinitionRevisionId::new(format!("definition_revision_{label}")).unwrap(),
        VersionTag::new("compiler-3.0.0").unwrap(),
        AuthorFormat::Programmatic,
        return_id.clone(),
        PlanInputContract::new(PlanType::Any),
        PlanType::Any,
        safe_error,
    ));
    builder
        .add_scope(ScopeMetadata::root(root_id.clone()))
        .add_node(Node::new(
            return_id.clone(),
            root_id,
            NodeKind::Return(ReturnDescriptor {
                value_input: value_input.clone(),
            }),
        ))
        .add_data_port(DataPort::new(
            value_input.clone(),
            return_id,
            PortName::new("value").unwrap(),
            PortDirection::Input,
            PlanType::Any,
            true,
        ))
        .add_data_binding(DataBinding::new(
            DataBindingId::new("bind_return").unwrap(),
            ValueSource::RunInput { path: vec![] },
            value_input,
        ));
    builder.build().unwrap()
}

fn plan(label: &str) -> VersionedPlan {
    VersionedPlan::from_verified_plan(
        format!("definition_artifact_{label}"),
        format!("agent_artifact_{label}"),
        "Artifact repository contract",
        DeploymentRevisionId::new(format!("deployment_revision_{label}")).unwrap(),
        "expression-3.0.0",
        json!({"author": "programmatic", "version": 3}),
        &verified_plan(label),
        json!({"return": "descriptor-v1"}),
        json!({"model": "fixed"}),
        json!({}),
    )
    .unwrap()
}

async fn create_run<R: DurableRepository>(repository: &R, label: &str, input: Value) -> RunId {
    let plan = plan(label);
    assert_eq!(
        repository.install_versioned_plan(&plan).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let run_id = RunId::new(format!("run_artifact_{label}")).unwrap();
    assert!(matches!(
        repository
            .create_run(
                key(&format!("{label}.create")),
                CreateRunCommand::new(run_id.clone(), &plan, input).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    run_id
}

fn artifact(label: &str, bytes: &[u8]) -> ArtifactRef {
    ArtifactRef::new(
        ArtifactId::new(format!("artifact_{label}")).unwrap(),
        ContentHash::from_bytes(bytes),
        u64::try_from(bytes.len()).unwrap(),
        Some("application/octet-stream".to_owned()),
    )
    .unwrap()
}

struct SharedObjectFixture {
    run_b: RunId,
    artifact: ArtifactRef,
    locator: StorageLocator,
    object_path: PathBuf,
    bytes: Vec<u8>,
}

async fn prepare_shared_object<R: ArtifactDurableRepository>(
    repository: &R,
    store: &LocalContentAddressedArtifactStore,
    object_root: &std::path::Path,
    label: &str,
) -> SharedObjectFixture {
    let run_a = create_run(
        repository,
        &format!("{label}_shared_a"),
        json!({"input": "a"}),
    )
    .await;
    let run_b = create_run(
        repository,
        &format!("{label}_shared_b"),
        json!({"input": "b"}),
    )
    .await;
    let bytes = format!("shared-object-{label}").into_bytes();
    let artifact = store
        .artifact_for_bytes(&bytes, Some("application/octet-stream".to_owned()))
        .unwrap();
    let locator = store.storage_locator(&artifact).unwrap();
    let hash = artifact
        .content_hash()
        .as_str()
        .strip_prefix("sha256:")
        .unwrap();
    let object_path = object_root.join(&hash[..2]).join(hash);
    assert!(matches!(
        repository
            .stage_artifact(StageArtifactCommand::new(
                run_a.clone(),
                artifact.clone(),
                locator.clone(),
                None,
            ))
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let size_conflict_run = create_run(
        repository,
        &format!("{label}_shared_size_conflict"),
        json!({"input": "size-conflict"}),
    )
    .await;
    let wrong_size = ArtifactRef::new(
        ArtifactId::new(format!("artifact_{label}_wrong_size")).unwrap(),
        artifact.content_hash().clone(),
        artifact.size_bytes() + 1,
        artifact.media_type().map(str::to_owned),
    )
    .unwrap();
    assert_eq!(
        repository
            .stage_artifact(StageArtifactCommand::new(
                size_conflict_run,
                wrong_size,
                locator.clone(),
                None,
            ))
            .await
            .unwrap(),
        TransitionOutcome::StateConflict,
        "one physical object cannot have conflicting byte-size metadata"
    );
    // Media type remains Run-scoped interpretation metadata; it does not alter
    // the byte identity or create a second physical object.
    let run_b_artifact = ArtifactRef::new(
        artifact.artifact_id().clone(),
        artifact.content_hash().clone(),
        artifact.size_bytes(),
        Some("application/x-shared-object".to_owned()),
    )
    .unwrap();
    assert!(matches!(
        repository
            .stage_artifact(StageArtifactCommand::new(
                run_b.clone(),
                run_b_artifact,
                locator.clone(),
                Some(Utc::now() + Duration::minutes(5)),
            ))
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let (actual_hash, actual_size) = store.put_and_verify(&artifact, &bytes).await.unwrap();
    for run_id in [&run_a, &run_b] {
        assert!(matches!(
            repository
                .verify_artifact(VerifyArtifactCommand::new(
                    run_id.clone(),
                    artifact.artifact_id().clone(),
                    actual_hash.clone(),
                    actual_size,
                ))
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
    }
    SharedObjectFixture {
        run_b,
        artifact,
        locator,
        object_path,
        bytes,
    }
}

async fn assert_repository_contract<R>(repository: &R, label: &str)
where
    R: ArtifactDurableRepository,
{
    let run_a = create_run(repository, &format!("{label}_a"), json!({"input": "a"})).await;
    let run_b = create_run(repository, &format!("{label}_b"), json!({"input": "b"})).await;

    let payload = PutInlinePayloadCommand::new(run_a.clone(), json!({"b": 2, "a": 1})).unwrap();
    let first = repository.put_inline_payload(payload).await.unwrap();
    let receipt = match first {
        TransitionOutcome::Committed { result } => result,
        other => panic!("unexpected payload outcome: {other:?}"),
    };
    let replay = repository
        .put_inline_payload(
            PutInlinePayloadCommand::new(run_a.clone(), json!({"a": 1, "b": 2})).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        replay,
        TransitionOutcome::ExactReplay {
            authoritative: receipt.clone()
        }
    );
    let loaded = repository
        .get_inline_payload(&run_a, receipt.payload_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.value(), &json!({"a": 1, "b": 2}));
    assert!(
        repository
            .get_inline_payload(&run_b, receipt.payload_id())
            .await
            .unwrap()
            .is_none(),
        "a payload ID from one Run must not confer authority in another Run"
    );

    let verified = artifact(&format!("{label}_verified"), b"verified-object");
    let locator = StorageLocator::new(format!("s3://private/{label}/verified")).unwrap();
    assert_eq!(format!("{locator:?}"), "StorageLocator(<redacted>)");
    let stage = StageArtifactCommand::new(run_a.clone(), verified.clone(), locator, None);
    assert!(matches!(
        repository.stage_artifact(stage.clone()).await.unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository.stage_artifact(stage).await.unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
    assert!(repository
        .get_retained_artifact(&run_a, verified.artifact_id())
        .await
        .unwrap()
        .is_none());
    let extended_retain_until = Utc::now() + Duration::seconds(1);
    assert!(matches!(
        repository
            .stage_artifact(StageArtifactCommand::new(
                run_a.clone(),
                verified.clone(),
                StorageLocator::new(format!("s3://private/{label}/verified")).unwrap(),
                Some(extended_retain_until),
            ))
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .stage_artifact(StageArtifactCommand::new(
                run_a.clone(),
                verified.clone(),
                StorageLocator::new(format!("s3://private/{label}/verified")).unwrap(),
                None,
            ))
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
    assert!(repository
        .get_retained_artifact(&run_a, verified.artifact_id())
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .get_retained_artifact(&run_b, verified.artifact_id())
        .await
        .unwrap()
        .is_none());
    let conflicting_locator = StageArtifactCommand::new(
        run_a.clone(),
        verified.clone(),
        StorageLocator::new("s3://private/changed").unwrap(),
        None,
    );
    assert_eq!(
        repository
            .stage_artifact(conflicting_locator)
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    );
    let same_hash_different_id = ArtifactRef::new(
        ArtifactId::new(format!("artifact_{label}_collision")).unwrap(),
        verified.content_hash().clone(),
        verified.size_bytes(),
        verified.media_type().map(str::to_owned),
    )
    .unwrap();
    assert_eq!(
        repository
            .stage_artifact(StageArtifactCommand::new(
                run_a.clone(),
                same_hash_different_id,
                StorageLocator::new("s3://private/collision").unwrap(),
                None,
            ))
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    );
    assert_eq!(
        repository
            .verify_artifact(VerifyArtifactCommand::new(
                run_a.clone(),
                verified.artifact_id().clone(),
                verified.content_hash().clone(),
                verified.size_bytes() + 1,
            ))
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    );
    let verified_receipt = repository
        .verify_artifact(VerifyArtifactCommand::new(
            run_a.clone(),
            verified.artifact_id().clone(),
            verified.content_hash().clone(),
            verified.size_bytes(),
        ))
        .await
        .unwrap();
    assert_eq!(
        verified_receipt.committed_result().unwrap().state(),
        ArtifactState::Verified
    );
    assert!(matches!(
        repository
            .verify_artifact(VerifyArtifactCommand::new(
                run_a.clone(),
                verified.artifact_id().clone(),
                verified.content_hash().clone(),
                verified.size_bytes(),
            ))
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
    assert_eq!(
        repository
            .verify_artifact(VerifyArtifactCommand::new(
                run_b.clone(),
                verified.artifact_id().clone(),
                verified.content_hash().clone(),
                verified.size_bytes(),
            ))
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    );
    assert!(repository
        .get_retained_artifact(&run_a, verified.artifact_id())
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .get_retained_artifact(&run_b, verified.artifact_id())
        .await
        .unwrap()
        .is_none());

    let staged = artifact(&format!("{label}_staged"), b"staged-object");
    repository
        .stage_artifact(StageArtifactCommand::new(
            run_a.clone(),
            staged,
            StorageLocator::new(format!("s3://private/{label}/staged")).unwrap(),
            None,
        ))
        .await
        .unwrap();
    // Eligibility is derived from the database clock. A caller cannot move
    // the cutoff into the future to collect a fresh upload.
    let fresh = repository
        .sweep_orphan_artifacts(
            key(&format!("{label}.sweep.fresh")),
            OrphanSweepCommand::new(1, format!("fresh_sweeper_{label}"), 1, 100).unwrap(),
        )
        .await
        .unwrap();
    assert!(fresh.committed_result().unwrap().claims().is_empty());
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let sweep_command = OrphanSweepCommand::new(1, format!("sweeper_{label}"), 1, 100).unwrap();
    let sweep_key = key(&format!("{label}.sweep.first"));
    let sweep = match repository
        .sweep_orphan_artifacts(sweep_key.clone(), sweep_command.clone())
        .await
        .unwrap()
    {
        TransitionOutcome::Committed { result } => result,
        other => panic!("unexpected sweep result: {other:?}"),
    };
    assert_eq!(sweep.claims().len(), 2);
    let mut locators = sweep
        .claims()
        .iter()
        .map(|claim| {
            assert_eq!(claim.deletion_fence().as_str().len(), 71);
            claim
                .storage_locator()
                .expose_to_storage_adapter()
                .to_owned()
        })
        .collect::<Vec<_>>();
    locators.sort();
    assert_eq!(
        locators,
        vec![
            format!("s3://private/{label}/staged"),
            format!("s3://private/{label}/verified"),
        ]
    );
    assert_eq!(
        repository
            .sweep_orphan_artifacts(sweep_key.clone(), sweep_command.clone())
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay {
            authoritative: sweep.clone()
        }
    );
    assert_eq!(
        repository
            .sweep_orphan_artifacts(
                sweep_key,
                OrphanSweepCommand::new(
                    sweep_command.orphan_retention_seconds(),
                    format!("different_{label}"),
                    1,
                    100,
                )
                .unwrap(),
            )
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_INTENT_CONFLICT
    );

    let before_expiry = repository
        .sweep_orphan_artifacts(
            key(&format!("{label}.sweep.before_expiry")),
            OrphanSweepCommand::new(1, format!("sweeper_before_{label}"), 1, 100).unwrap(),
        )
        .await
        .unwrap();
    assert!(before_expiry
        .committed_result()
        .unwrap()
        .claims()
        .is_empty());

    let verified_claim = sweep
        .claims()
        .iter()
        .find(|claim| {
            claim
                .storage_locator()
                .expose_to_storage_adapter()
                .ends_with("/verified")
        })
        .unwrap();
    let staged_claim = sweep
        .claims()
        .iter()
        .find(|claim| {
            claim
                .storage_locator()
                .expose_to_storage_adapter()
                .ends_with("/staged")
        })
        .unwrap();
    let verified_ack = AcknowledgeArtifactDeletionCommand::from_claim(verified_claim);
    assert!(matches!(
        repository
            .acknowledge_artifact_deleted(verified_ack.clone())
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .acknowledge_artifact_deleted(verified_ack)
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));

    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let reclaimed = repository
        .sweep_orphan_artifacts(
            key(&format!("{label}.sweep.reclaim")),
            OrphanSweepCommand::new(1, format!("sweeper_reclaim_{label}"), 30, 100).unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .unwrap();
    assert_eq!(reclaimed.claims().len(), 1);
    let reclaimed_claim = &reclaimed.claims()[0];
    assert_eq!(
        reclaimed_claim.deletion_fence(),
        staged_claim.deletion_fence()
    );
    assert_ne!(reclaimed_claim.claim_token(), staged_claim.claim_token());
    assert_eq!(
        repository
            .acknowledge_artifact_deleted(AcknowledgeArtifactDeletionCommand::from_claim(
                staged_claim,
            ))
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    );
    let reclaimed_ack = AcknowledgeArtifactDeletionCommand::from_claim(reclaimed_claim);
    assert!(matches!(
        repository
            .acknowledge_artifact_deleted(reclaimed_ack.clone())
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .acknowledge_artifact_deleted(reclaimed_ack)
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
}

#[tokio::test]
async fn sqlite_payload_and_artifact_repository_is_run_scoped_and_fail_closed() {
    let repository = SqliteDurableRepository::in_memory().await.unwrap();
    assert_repository_contract(&repository, "sqlite").await;
}

#[tokio::test]
async fn sqlite_retained_artifact_read_metadata_is_run_scoped_and_expires() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("read-authority.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    let control = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    let run = create_run(&repository, "retained_read", json!({"input": "a"})).await;
    let other_run = create_run(&repository, "retained_read_other", json!({"input": "b"})).await;
    let bytes = b"retained artifact";
    let artifact = artifact("retained_read", bytes);
    let locator = StorageLocator::new("content-addressed:v1/sha256/private").unwrap();
    repository
        .stage_artifact(StageArtifactCommand::new(
            run.clone(),
            artifact.clone(),
            locator.clone(),
            None,
        ))
        .await
        .unwrap();
    repository
        .verify_artifact(VerifyArtifactCommand::new(
            run.clone(),
            artifact.artifact_id().clone(),
            artifact.content_hash().clone(),
            artifact.size_bytes(),
        ))
        .await
        .unwrap();
    assert!(repository
        .get_retained_artifact(&run, artifact.artifact_id())
        .await
        .unwrap()
        .is_none());

    sqlx::query(
        "UPDATE artifacts SET artifact_state='referenced',referenced_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND artifact_id=? AND artifact_state='verified'",
    )
    .bind(run.as_str())
    .bind(artifact.artifact_id().as_str())
    .execute(&control)
    .await
    .unwrap();
    let retained = repository
        .get_retained_artifact(&run, artifact.artifact_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.run_id(), &run);
    assert_eq!(retained.artifact(), &artifact);
    assert_eq!(retained.storage_locator(), &locator);
    assert!(!format!("{retained:?}").contains(locator.expose_to_storage_adapter()));
    assert!(repository
        .get_retained_artifact(&other_run, artifact.artifact_id())
        .await
        .unwrap()
        .is_none());

    let (event_id, event_seq): (String, i64) = sqlx::query_as(
        "SELECT event_id,seq FROM execution_events WHERE run_id=? ORDER BY seq LIMIT 1",
    )
    .bind(run.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO artifact_retention_releases
         (run_id,transition_key,intent_hash,event_id,event_seq,retain_until,artifact_count,created_at)
         VALUES (?,?,?,?,?,STRFTIME('%Y-%m-%dT%H:%M:%fZ','now','+1 hour'),1,CURRENT_TIMESTAMP)",
    )
    .bind(run.as_str())
    .bind(key("retained.read.release").as_str())
    .bind(ContentHash::from_bytes(b"retained.read.release").as_str())
    .bind(event_id)
    .bind(event_seq)
    .execute(&control)
    .await
    .unwrap();
    assert!(repository
        .get_retained_artifact(&run, artifact.artifact_id())
        .await
        .unwrap()
        .is_some());
    sqlx::query(
        "UPDATE artifact_retention_releases
         SET retain_until=STRFTIME('%Y-%m-%dT%H:%M:%fZ','now','-1 second') WHERE run_id=?",
    )
    .bind(run.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert!(repository
        .get_retained_artifact(&run, artifact.artifact_id())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn sqlite_artifact_store_authority_is_atomic_immutable_and_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("authority.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    let command = BindArtifactStoreAuthorityCommand::shared_filesystem(
        "production",
        "artifact_store_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .unwrap();
    let mut handles = Vec::new();
    for _ in 0..16 {
        let repository = repository.clone();
        let command = command.clone();
        handles.push(tokio::spawn(async move {
            repository
                .bind_artifact_store_authority(command)
                .await
                .unwrap()
        }));
    }
    let mut committed = 0;
    for handle in handles {
        match handle.await.unwrap() {
            TransitionOutcome::Committed { result } => {
                committed += 1;
                assert_eq!(result.namespace(), "production");
            }
            TransitionOutcome::ExactReplay { authoritative } => {
                assert_eq!(authoritative.namespace(), "production");
            }
            outcome => panic!("unexpected authority outcome: {outcome:?}"),
        }
    }
    assert_eq!(committed, 1);

    assert_eq!(
        repository
            .bind_artifact_store_authority(
                BindArtifactStoreAuthorityCommand::shared_filesystem(
                    "production",
                    "artifact_store_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                )
                .unwrap(),
            )
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_ARTIFACT_STORE_CONFLICT
    );

    let control = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    assert!(
        sqlx::query("UPDATE artifact_store_authority SET namespace='forged'")
            .execute(&control)
            .await
            .is_err()
    );
    assert!(sqlx::query("DELETE FROM artifact_store_authority")
        .execute(&control)
        .await
        .is_err());
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT store_id FROM artifact_store_authority WHERE singleton=1",
        )
        .fetch_one(&control)
        .await
        .unwrap(),
        command.store_id()
    );
}

#[tokio::test]
async fn sqlite_shared_content_hash_is_deleted_only_after_every_run_releases_it() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("repository.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    let control = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    let object_root = directory.path().join("objects");
    let store = LocalContentAddressedArtifactStore::open(object_root.clone(), 1)
        .await
        .unwrap();
    let fixture =
        prepare_shared_object(&repository, &store, &object_root, "sqlite_global_hash").await;
    sqlx::query(
        "UPDATE artifacts SET created_at=STRFTIME('%Y-%m-%dT%H:%M:%fZ','now','-10 seconds')
         WHERE content_hash=? AND storage_uri=?",
    )
    .bind(fixture.artifact.content_hash().as_str())
    .bind(fixture.locator.expose_to_storage_adapter())
    .execute(&control)
    .await
    .unwrap();

    let protected = repository
        .sweep_orphan_artifacts(
            key("sqlite.global_hash.protected"),
            OrphanSweepCommand::new(1, "sqlite_protected_sweeper", 30, 100).unwrap(),
        )
        .await
        .unwrap();
    assert!(protected.committed_result().unwrap().claims().is_empty());
    assert_eq!(
        tokio::fs::read(&fixture.object_path).await.unwrap(),
        fixture.bytes
    );
    assert!(matches!(
        repository
            .verify_artifact(VerifyArtifactCommand::new(
                fixture.run_b.clone(),
                fixture.artifact.artifact_id().clone(),
                fixture.artifact.content_hash().clone(),
                fixture.artifact.size_bytes(),
            ))
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));

    sqlx::query(
        "UPDATE artifacts SET retain_until=STRFTIME('%Y-%m-%dT%H:%M:%fZ','now','-1 second')
         WHERE run_id=? AND artifact_id=?",
    )
    .bind(fixture.run_b.as_str())
    .bind(fixture.artifact.artifact_id().as_str())
    .execute(&control)
    .await
    .unwrap();
    let left = repository.clone();
    let right = repository.clone();
    let (left, right) = tokio::join!(
        left.sweep_orphan_artifacts(
            key("sqlite.global_hash.concurrent.left"),
            OrphanSweepCommand::new(1, "sqlite_left_sweeper", 30, 100).unwrap(),
        ),
        right.sweep_orphan_artifacts(
            key("sqlite.global_hash.concurrent.right"),
            OrphanSweepCommand::new(1, "sqlite_right_sweeper", 30, 100).unwrap(),
        )
    );
    let left = left.unwrap();
    let right = right.unwrap();
    let claims = [left, right]
        .iter()
        .flat_map(|outcome| outcome.committed_result().unwrap().claims().iter().cloned())
        .collect::<Vec<_>>();
    assert_eq!(
        claims.len(),
        1,
        "one object must produce one durable delete claim"
    );
    store
        .delete(claims[0].artifact(), claims[0].storage_locator())
        .await
        .unwrap();
    assert!(matches!(
        repository
            .acknowledge_artifact_deleted(AcknowledgeArtifactDeletionCommand::from_claim(
                &claims[0],
            ))
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let states = sqlx::query_scalar::<_, String>(
        "SELECT artifact_state FROM artifacts
         WHERE content_hash=? AND storage_uri=? ORDER BY run_id",
    )
    .bind(fixture.artifact.content_hash().as_str())
    .bind(fixture.locator.expose_to_storage_adapter())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(states, vec!["deleted", "deleted"]);
    let private_locator = fixture.locator.expose_to_storage_adapter();
    let execution_payloads = sqlx::query_scalar::<_, String>(
        "SELECT safe_payload FROM execution_events WHERE run_id IN (
             SELECT run_id FROM artifacts WHERE content_hash=? AND storage_uri=?
         )",
    )
    .bind(fixture.artifact.content_hash().as_str())
    .bind(private_locator)
    .fetch_all(&control)
    .await
    .unwrap();
    assert!(
        !execution_payloads.is_empty(),
        "the Artifact runs must retain execution-event authority rows"
    );
    for payload in execution_payloads {
        assert!(
            !payload.contains(private_locator),
            "execution event payload exposed an Artifact storage locator"
        );
    }
    let public_envelopes = sqlx::query_scalar::<_, String>(
        "SELECT safe_envelope FROM public_event_outbox WHERE run_id IN (
             SELECT run_id FROM artifacts WHERE content_hash=? AND storage_uri=?
         )",
    )
    .bind(fixture.artifact.content_hash().as_str())
    .bind(private_locator)
    .fetch_all(&control)
    .await
    .unwrap();
    assert!(
        !public_envelopes.is_empty(),
        "the Artifact runs must retain public-event authority rows"
    );
    for envelope in public_envelopes {
        assert!(
            !envelope.contains(private_locator),
            "public event envelope exposed an Artifact storage locator"
        );
    }
    assert!(tokio::fs::metadata(&fixture.object_path).await.is_err());
    let after_ack = repository
        .sweep_orphan_artifacts(
            key("sqlite.global_hash.after_ack"),
            OrphanSweepCommand::new(1, "sqlite_after_ack_sweeper", 30, 100).unwrap(),
        )
        .await
        .unwrap();
    assert!(after_ack.committed_result().unwrap().claims().is_empty());
    control.close().await;
}

/// Uses a dedicated disposable database when supplied. External object I/O is
/// intentionally absent: the contract ends at the returned storage locator.
#[tokio::test]
async fn postgres_payload_and_artifact_repository_contract_when_available() {
    let Ok(database_url) = std::env::var("V3_ARTIFACT_TEST_POSTGRES_URL") else {
        return;
    };
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let schema = format!("artifact_repo_{}", &suffix[..16]);
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();
    assert_repository_contract(&repository, "postgres").await;
    drop(repository);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn postgres_artifact_store_authority_is_atomic_immutable_and_fail_closed_when_available() {
    let Ok(database_url) = std::env::var("V3_ARTIFACT_TEST_POSTGRES_URL") else {
        eprintln!("skipping real PostgreSQL Artifact authority test: env is not configured");
        return;
    };
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let schema = format!("artifact_authority_{}", &suffix[..16]);
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();
    let command = BindArtifactStoreAuthorityCommand::shared_filesystem(
        "production",
        "artifact_store_cccccccccccccccccccccccccccccccc",
    )
    .unwrap();
    let mut handles = Vec::new();
    for _ in 0..16 {
        let repository = repository.clone();
        let command = command.clone();
        handles.push(tokio::spawn(async move {
            repository
                .bind_artifact_store_authority(command)
                .await
                .unwrap()
        }));
    }
    let mut committed = 0;
    for handle in handles {
        match handle.await.unwrap() {
            TransitionOutcome::Committed { result } => {
                committed += 1;
                assert_eq!(result.namespace(), "production");
            }
            TransitionOutcome::ExactReplay { authoritative } => {
                assert_eq!(authoritative.namespace(), "production");
            }
            outcome => panic!("unexpected authority outcome: {outcome:?}"),
        }
    }
    assert_eq!(committed, 1);
    assert_eq!(
        repository
            .bind_artifact_store_authority(
                BindArtifactStoreAuthorityCommand::shared_filesystem(
                    "production",
                    "artifact_store_dddddddddddddddddddddddddddddddd",
                )
                .unwrap(),
            )
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_ARTIFACT_STORE_CONFLICT
    );

    let control = PgPoolOptions::new()
        .max_connections(2)
        .connect(&scoped_url)
        .await
        .unwrap();
    assert!(
        sqlx::query("UPDATE artifact_store_authority SET namespace='forged'")
            .execute(&control)
            .await
            .is_err()
    );
    assert!(sqlx::query("DELETE FROM artifact_store_authority")
        .execute(&control)
        .await
        .is_err());
    control.close().await;
    drop(repository);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn postgres_shared_content_hash_is_deleted_once_after_global_release_when_available() {
    let Ok(database_url) = std::env::var("V3_ARTIFACT_TEST_POSTGRES_URL") else {
        return;
    };
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let schema = format!("artifact_global_hash_{}", &suffix[..16]);
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let object_root = directory.path().join("objects");
    let store = LocalContentAddressedArtifactStore::open(object_root.clone(), 1)
        .await
        .unwrap();
    let fixture =
        prepare_shared_object(&repository, &store, &object_root, "postgres_global_hash").await;
    sqlx::query(
        "UPDATE artifacts SET created_at=CURRENT_TIMESTAMP-INTERVAL '10 seconds'
         WHERE content_hash=$1 AND storage_uri=$2",
    )
    .bind(fixture.artifact.content_hash().as_str())
    .bind(fixture.locator.expose_to_storage_adapter())
    .execute(&control)
    .await
    .unwrap();

    let protected = repository
        .sweep_orphan_artifacts(
            key("postgres.global_hash.protected"),
            OrphanSweepCommand::new(1, "postgres_protected_sweeper", 30, 100).unwrap(),
        )
        .await
        .unwrap();
    assert!(protected.committed_result().unwrap().claims().is_empty());
    assert_eq!(
        tokio::fs::read(&fixture.object_path).await.unwrap(),
        fixture.bytes
    );
    assert!(matches!(
        repository
            .verify_artifact(VerifyArtifactCommand::new(
                fixture.run_b.clone(),
                fixture.artifact.artifact_id().clone(),
                fixture.artifact.content_hash().clone(),
                fixture.artifact.size_bytes(),
            ))
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));

    sqlx::query(
        "UPDATE artifacts SET retain_until=CURRENT_TIMESTAMP-INTERVAL '1 second'
         WHERE run_id=$1 AND artifact_id=$2",
    )
    .bind(fixture.run_b.as_str())
    .bind(fixture.artifact.artifact_id().as_str())
    .execute(&control)
    .await
    .unwrap();
    let left_repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    let right_repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    let (left, right) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(
            left_repository.sweep_orphan_artifacts(
                key("postgres.global_hash.concurrent.left"),
                OrphanSweepCommand::new(1, "postgres_left_sweeper", 30, 100).unwrap(),
            ),
            right_repository.sweep_orphan_artifacts(
                key("postgres.global_hash.concurrent.right"),
                OrphanSweepCommand::new(1, "postgres_right_sweeper", 30, 100).unwrap(),
            )
        )
    })
    .await
    .expect("concurrent object sweepers must not deadlock");
    let left = left.unwrap();
    let right = right.unwrap();
    let claims = [left, right]
        .iter()
        .flat_map(|outcome| outcome.committed_result().unwrap().claims().iter().cloned())
        .collect::<Vec<_>>();
    assert_eq!(
        claims.len(),
        1,
        "one object must produce one durable delete claim"
    );
    store
        .delete(claims[0].artifact(), claims[0].storage_locator())
        .await
        .unwrap();
    assert!(matches!(
        repository
            .acknowledge_artifact_deleted(AcknowledgeArtifactDeletionCommand::from_claim(
                &claims[0],
            ))
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let states = sqlx::query_scalar::<_, String>(
        "SELECT artifact_state FROM artifacts
         WHERE content_hash=$1 AND storage_uri=$2 ORDER BY run_id",
    )
    .bind(fixture.artifact.content_hash().as_str())
    .bind(fixture.locator.expose_to_storage_adapter())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(states, vec!["deleted", "deleted"]);
    let private_locator = fixture.locator.expose_to_storage_adapter();
    let execution_payloads = sqlx::query_scalar::<_, Value>(
        "SELECT safe_payload FROM execution_events WHERE run_id IN (
             SELECT run_id FROM artifacts WHERE content_hash=$1 AND storage_uri=$2
         )",
    )
    .bind(fixture.artifact.content_hash().as_str())
    .bind(private_locator)
    .fetch_all(&control)
    .await
    .unwrap();
    assert!(
        !execution_payloads.is_empty(),
        "the Artifact runs must retain execution-event authority rows"
    );
    for payload in execution_payloads {
        assert!(
            !payload.to_string().contains(private_locator),
            "execution event payload exposed an Artifact storage locator"
        );
    }
    let public_envelopes = sqlx::query_scalar::<_, Value>(
        "SELECT safe_envelope FROM public_event_outbox WHERE run_id IN (
             SELECT run_id FROM artifacts WHERE content_hash=$1 AND storage_uri=$2
         )",
    )
    .bind(fixture.artifact.content_hash().as_str())
    .bind(private_locator)
    .fetch_all(&control)
    .await
    .unwrap();
    assert!(
        !public_envelopes.is_empty(),
        "the Artifact runs must retain public-event authority rows"
    );
    for envelope in public_envelopes {
        assert!(
            !envelope.to_string().contains(private_locator),
            "public event envelope exposed an Artifact storage locator"
        );
    }
    assert!(tokio::fs::metadata(&fixture.object_path).await.is_err());
    let after_ack = repository
        .sweep_orphan_artifacts(
            key("postgres.global_hash.after_ack"),
            OrphanSweepCommand::new(1, "postgres_after_ack_sweeper", 30, 100).unwrap(),
        )
        .await
        .unwrap();
    assert!(after_ack.committed_result().unwrap().claims().is_empty());

    control.close().await;
    drop(left_repository);
    drop(right_repository);
    drop(repository);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
