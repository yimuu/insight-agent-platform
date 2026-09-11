use super::*;
use insight_platform_agent_compiler::{
    AgentCompilationV1, AgentCompileResponseV1, AgentSourceBundleV1, ArtifactAuthority,
};
use insight_platform_postgres::repository::{PublishedResource, ResourceRecord};

fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}

fn publication_audit(label: &str) -> CommandAudit {
    let receipt_id = fresh(ResourceKind::Receipt);
    CommandAudit {
        trace: insight_platform_contracts::TraceIdentityV1::generate(),
        tenant_id: id(TENANT_ID),
        principal_id: id(PRINCIPAL_ID),
        principal_kind: PrincipalKind::TenantAdmin,
        event_id: fresh(ResourceKind::Event),
        outbox_id: fresh(ResourceKind::OutboxEvent),
        idempotency_key_digest: insight_platform_contracts::canonical_digest(&json!({
            "publication_fixture": label, "receipt_id": receipt_id,
        }))
        .unwrap()
        .parse()
        .unwrap(),
        request_digest: insight_platform_contracts::canonical_digest(&json!({
            "publication_fixture_request": label, "receipt_id": receipt_id,
        }))
        .unwrap()
        .parse()
        .unwrap(),
        receipt_id,
        receipt_expires_at: Utc::now() + Duration::hours(1),
    }
}

async fn facts(pool: &PgPool) -> serde_json::Value {
    sqlx::query_scalar(
        r#"
        SELECT jsonb_build_object(
            'resources', (SELECT jsonb_agg(to_jsonb(r) ORDER BY resource_id)
                FROM insight_platform.resources r WHERE tenant_id=$1),
            'versions', (SELECT jsonb_agg(to_jsonb(r) ORDER BY resource_version_id)
                FROM insight_platform.resource_versions r WHERE tenant_id=$1),
            'jobs', (SELECT jsonb_agg(to_jsonb(r) ORDER BY job_id)
                FROM insight_platform.jobs r WHERE tenant_id=$1),
            'receipts', (SELECT jsonb_agg(to_jsonb(r) ORDER BY receipt_id)
                FROM insight_platform.receipts r WHERE tenant_id=$1),
            'events', (SELECT jsonb_agg(to_jsonb(r) ORDER BY event_id)
                FROM insight_platform.events r WHERE tenant_id=$1),
            'outbox', (SELECT jsonb_agg(to_jsonb(r) ORDER BY outbox_id)
                FROM insight_platform.outbox_events r WHERE tenant_id=$1),
            'artifacts', (SELECT jsonb_agg(to_jsonb(r) ORDER BY artifact_id)
                FROM insight_platform.artifacts r WHERE tenant_id=$1),
            'blobs', (SELECT jsonb_agg(to_jsonb(r) ORDER BY blob_id)
                FROM insight_platform.artifact_blobs r WHERE tenant_id=$1),
            'quota_accounts', (SELECT jsonb_agg(to_jsonb(r) ORDER BY quota_account_id)
                FROM insight_platform.quota_accounts r WHERE tenant_id=$1),
            'quota_ledger', (SELECT jsonb_agg(to_jsonb(r) ORDER BY quota_entry_id)
                FROM insight_platform.quota_ledger r WHERE tenant_id=$1)
        )
        "#,
    )
    .bind(TENANT_ID)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn version_facts(pool: &PgPool, published: &PublishedResource) -> serde_json::Value {
    let ids = published
        .versions
        .iter()
        .map(|version| version.resource_version_id.clone())
        .collect::<Vec<_>>();
    sqlx::query_scalar(
        "SELECT jsonb_agg(to_jsonb(v) ORDER BY resource_version_id) FROM insight_platform.resource_versions v WHERE tenant_id=$1 AND resource_version_id=ANY($2)",
    )
    .bind(TENANT_ID)
    .bind(&ids)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn seed_compiled_source(
    pool: &PgPool,
    compilation: &AgentCompilationV1,
) -> ArtifactAuthority {
    let intent = &compilation.compiled.resource_intent.authoring_artifact;
    let artifact_id = fresh(ResourceKind::Artifact);
    let blob_id = fresh(ResourceKind::InternalBlob);
    let stamp = Utc::now();
    let inserted = sqlx::query(
            r#"
            INSERT INTO insight_platform.artifact_blobs (
                tenant_id,blob_id,backend,storage_binding_digest,security_domain_digest,
                object_reference_ciphertext,object_generation,key_id,encryption_domain_id,
                content_digest,size_bytes,state,verified_at,created_at,updated_at
            ) SELECT tenant_id,$3,backend,storage_binding_digest,security_domain_digest,
                object_reference_ciphertext,'compiled-publication-source',key_id,encryption_domain_id,
                $4,$5,'verified',$6,$6,$6
            FROM insight_platform.artifact_blobs WHERE tenant_id=$1 AND blob_id=$2
            "#,
        )
        .bind(TENANT_ID)
        .bind(ARTIFACT_BLOB_ID)
        .bind(blob_id.to_string())
        .bind(intent.content_digest.to_string())
        .bind(i64::try_from(intent.byte_length).unwrap())
        .bind(stamp)
        .execute(pool)
        .await
        .unwrap();
    assert_eq!(inserted.rows_affected(), 1);
    // As in the parent compiler fixture, only Ready metadata is seeded; validation
    // below recompiles the actual source and Plan bytes through the owning compiler.
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id,artifact_id,blob_id,purpose,classification,expected_size_bytes,expected_digest,
            declared_media_type,verified_media_type,state,metadata_schema_version,metadata,
            metadata_digest,retention_policy_revision_id,retain_until,created_by
        ) SELECT tenant_id,$3,$4,'authoring_document',classification,$5,$6,$7,$7,'ready',
            metadata_schema_version,metadata,metadata_digest,retention_policy_revision_id,
            retain_until,created_by
        FROM insight_platform.artifacts WHERE tenant_id=$1 AND artifact_id=$2
        "#,
    )
    .bind(TENANT_ID)
    .bind(ARTIFACT_ID)
    .bind(artifact_id.to_string())
    .bind(blob_id.to_string())
    .bind(i64::try_from(intent.byte_length).unwrap())
    .bind(intent.content_digest.to_string())
    .bind(&intent.media_type)
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(inserted.rows_affected(), 1);
    ArtifactAuthority {
        purpose: intent.purpose,
        state: insight_platform_contracts::ArtifactState::Ready,
        artifact: ArtifactRef::new(
            artifact_id,
            intent.content_digest.clone(),
            intent.byte_length,
            intent.media_type.clone(),
            intent.classification,
            intent.display_name.clone(),
        )
        .unwrap(),
    }
}

async fn validate_compilation(
    repository: &PgRepository,
    current: &ResourceRecord,
    compilation: &AgentCompilationV1,
    source: &ArtifactAuthority,
    plan: &ArtifactAuthority,
    document: &ResourceDocument,
) -> ResourceRecord {
    let evidence = insight_platform_agent_compiler::validate_frozen_agent_artifacts(
        document,
        source,
        &compilation.source_bundle_bytes,
        plan,
        &compilation.compiled.typed_plan_bytes,
    )
    .unwrap();
    let validation_job = applied(
        registry_command!(
            repository,
            request_resource_validation,
            RequestResourceValidation {
                audit: publication_audit("request-validation"),
                resource_id: id(&current.resource_id),
                expected_resource_version: current.version,
                job_id: fresh(ResourceKind::Job),
                validator_digest: digest('5'),
                validation_profile_digest: digest('f'),
                compilation_input: Some(evidence.input.clone()),
                attempt_limit: 3,
                scheduled_at: Utc::now() - Duration::seconds(1),
                deadline: Utc::now() + Duration::minutes(5),
            }
        )
        .unwrap(),
    );
    let mut manifest = support::registry();
    manifest.execution_capabilities.capabilities.push(
        insight_platform_contracts::WorkerExecutionCapability::AgentCompilation {
            compiler_semantic_identity: compilation.compiler_semantic_identity.clone(),
        },
    );
    manifest.validate().unwrap();
    let worker_id = fresh(ResourceKind::WorkerProcessGeneration);
    let claimed = claim_registry_fixture(
        repository,
        ClaimJobs {
            worker_manifest: manifest,
            work_class: "registry_validation".to_owned(),
            worker_id: worker_id.clone(),
            limit: 1,
            lease_milliseconds: 30_000,
            lease_token_digests: vec![digest('0')],
        },
        &id(VALIDATOR_PRINCIPAL_ID),
    )
    .await
    .unwrap()
    .into_iter()
    .find(|job| job.job_id == validation_job.job_id)
    .expect("actual compiler validation Job must be claimed");
    let fence = JobFence {
        tenant_id: claimed.tenant_id,
        job_id: claimed.job_id,
        worker_id,
        lease_epoch: claimed.lease_epoch,
        expected_job_version: claimed.version,
        lease_token_digest: claimed.lease_token_digest.unwrap().parse().unwrap(),
    };
    let running = repository.start_job(fence.clone()).await.unwrap();
    let completion = publication_audit("commit-validation");
    let RegistryValidationCommitOutcome::Committed { resource, job } = repository
        .commit_registry_validation(CommitRegistryValidation {
            fence: JobFence {
                expected_job_version: running.version,
                ..fence
            },
            validator_principal_id: id(VALIDATOR_PRINCIPAL_ID),
            validator_digest: digest('5'),
            validation_profile_digest: digest('f'),
            compilation: Some(evidence.clone()),
            receipt_id: completion.receipt_id,
            resource_event_id: completion.event_id,
            resource_outbox_id: completion.outbox_id,
            job_event_id: fresh(ResourceKind::Event),
            job_outbox_id: fresh(ResourceKind::OutboxEvent),
            idempotency_key_digest: completion.idempotency_key_digest,
            request_digest: completion.request_digest,
            receipt_expires_at: completion.receipt_expires_at,
        })
        .await
        .unwrap()
    else {
        panic!("actual compilation must commit its frozen validation");
    };
    assert_eq!(job.state, "succeeded");
    let validation: ValidationSummary =
        serde_json::from_value(resource.payload.value["validation"].clone()).unwrap();
    assert_eq!(
        validation.validated_draft_digest,
        ResourceDraftPayload {
            alias: None,
            display_name: "Repeated publication".to_owned(),
            document: document.clone(),
            validation: None,
        }
        .document_digest()
        .unwrap()
    );
    assert_eq!(
        validation.program_requirement,
        Some(evidence.program_requirement)
    );
    *resource
}

pub(super) async fn verify(
    pool: &PgPool,
    repository: &PgRepository,
    baseline: &AgentCompilationV1,
) {
    let original_bundle: AgentSourceBundleV1 =
        serde_json::from_slice(&baseline.source_bundle_bytes).unwrap();
    let plan_intent = &baseline.compiled.resource_intent.typed_plan_artifact;
    let plan = ArtifactAuthority {
        purpose: plan_intent.purpose,
        state: insight_platform_contracts::ArtifactState::Ready,
        artifact: ArtifactRef::new(
            id(TYPED_PLAN_ARTIFACT_ID),
            plan_intent.content_digest.clone(),
            plan_intent.byte_length,
            plan_intent.media_type.clone(),
            plan_intent.classification,
            plan_intent.display_name.clone(),
        )
        .unwrap(),
    };
    let agent_id = fresh(ResourceKind::Agent);
    let mut current: Option<ResourceRecord> = None;
    let mut history: Vec<(
        PublishResourceVersions,
        PublishedResource,
        serde_json::Value,
    )> = vec![];
    let mut sources: std::collections::BTreeMap<char, ArtifactAuthority> =
        std::collections::BTreeMap::new();
    let mut source_bytes = std::collections::BTreeMap::new();
    let mut source_ids = std::collections::BTreeSet::new();
    let mut validation_digests = std::collections::BTreeSet::new();
    for (index, label) in ['A', 'B', 'C', 'B'].into_iter().enumerate() {
        let mut bundle = original_bundle.clone();
        bundle
            .sources
            .files
            .get_mut(&bundle.sources.manifest_path)
            .unwrap()
            .push_str(&format!("# publication source {label}\n"));
        let AgentCompileResponseV1::Compiled { compilation } =
            insight_platform_agent_compiler::compile_source_bundle(bundle)
        else {
            panic!("current compiler rejected publication source {label}");
        };
        assert_eq!(
            compilation.compiled.typed_plan_bytes,
            baseline.compiled.typed_plan_bytes
        );
        assert_eq!(
            compilation.compiled.typed_plan_digest,
            baseline.compiled.typed_plan_digest
        );
        assert_eq!(
            compilation.compiled.resource_intent.contract_digest,
            baseline.compiled.resource_intent.contract_digest
        );
        if let Some(previous) = source_bytes.get(&label) {
            assert_eq!(&compilation.source_bundle_bytes, previous);
        } else {
            assert!(source_bytes
                .values()
                .all(|bytes| bytes != &compilation.source_bundle_bytes));
            source_bytes.insert(label, compilation.source_bundle_bytes.clone());
        }
        let source = if let Some(previous) = sources.get(&label) {
            previous.clone()
        } else {
            let source = seed_compiled_source(pool, &compilation).await;
            assert!(source_ids.insert(source.artifact.artifact_id().clone()));
            sources.insert(label, source.clone());
            source
        };
        let document = compilation.materialize(&source, &plan).unwrap();
        if index == 3 {
            assert_eq!(document, history[1].0.versions[0].payload.document);
            assert_eq!(source, sources[&'B']);
        }
        let draft = ResourceDraftPayload {
            alias: None,
            display_name: "Repeated publication".to_owned(),
            document: document.clone(),
            validation: None,
        };
        let drafted = if let Some(previous) = current.take() {
            applied(
                registry_command!(
                    repository,
                    update_resource_draft,
                    insight_platform_registry::UpdateResourceDraft {
                        audit: publication_audit("update-draft"),
                        resource_id: agent_id.clone(),
                        expected_resource_version: previous.version,
                        draft: draft.clone(),
                    }
                )
                .unwrap(),
            )
        } else {
            applied(
                registry_command!(
                    repository,
                    create_resource_draft,
                    CreateResourceDraft {
                        audit: publication_audit("create-draft"),
                        resource_id: agent_id.clone(),
                        draft: draft.clone(),
                    }
                )
                .unwrap(),
            )
        };
        let validated = validate_compilation(
            repository,
            &drafted,
            &compilation,
            &source,
            &plan,
            &document,
        )
        .await;
        let validation: ValidationSummary =
            serde_json::from_value(validated.payload.value["validation"].clone()).unwrap();
        if index == 3 {
            assert_eq!(
                validation.validated_draft_digest,
                history[1].0.expected_draft_digest
            );
        }
        validation_digests.insert(validation.validated_draft_digest.clone());
        let ordinal = i64::try_from(index + 1).unwrap();
        let payload = PublishedVersionPayload {
            document,
            validation,
        };
        let command = PublishResourceVersions {
            audit: publication_audit("publish"),
            resource_id: agent_id.clone(),
            expected_resource_version: validated.version,
            expected_draft_digest: draft.document_digest().unwrap(),
            versions: vec![
                NewPublishedVersion {
                    resource_version_id: fresh(ResourceKind::AgentInterfaceRevision),
                    revision_no: ordinal,
                    content_digest: compilation.compiled.resource_intent.contract_digest.clone(),
                    artifact_id: None,
                    payload: payload.clone(),
                },
                NewPublishedVersion {
                    resource_version_id: fresh(ResourceKind::AgentPlanRevision),
                    revision_no: ordinal,
                    content_digest: compilation.compiled.typed_plan_digest.clone(),
                    artifact_id: Some(plan.artifact.artifact_id().clone()),
                    payload,
                },
            ],
        };
        if index == 3 {
            let before = facts(pool).await;
            let mut duplicate_ordinal = command.clone();
            duplicate_ordinal.audit = publication_audit("duplicate-ordinal");
            for version in &mut duplicate_ordinal.versions {
                version.revision_no = 1;
            }
            assert!(matches!(
                registry_command!(repository, publish_resource_versions, duplicate_ordinal),
                Err(RepositoryError::Conflict(
                    "Resource version identity or revision"
                ))
            ));
            assert_eq!(facts(pool).await, before);

            let mut duplicate_second_id = command.clone();
            duplicate_second_id.versions[1].resource_version_id =
                history[0].0.versions[1].resource_version_id.clone();
            assert!(matches!(
                registry_command!(repository, publish_resource_versions, duplicate_second_id),
                Err(RepositoryError::Conflict(
                    "Resource version identity or revision"
                ))
            ));
            assert_eq!(facts(pool).await, before);
            // The next actual submission preserves this request's Receipt and first AIR ID.
            // Its success proves the second INSERT conflict rolled both of them back.
        }
        let published = applied(
            registry_command!(repository, publish_resource_versions, command.clone()).unwrap(),
        );
        assert_eq!(published.versions.len(), 2);
        assert!(published
            .versions
            .iter()
            .all(|version| version.revision_no == ordinal));
        for (stored, submitted) in published.versions.iter().zip(&command.versions) {
            assert_eq!(
                stored.resource_version_id,
                submitted.resource_version_id.to_string()
            );
            assert_eq!(stored.content_digest, submitted.content_digest.to_string());
            assert_eq!(
                stored.payload,
                TypedPayload::new(1, &submitted.payload).unwrap()
            );
            assert!(history.iter().all(|(_, previous, _)| {
                previous
                    .versions
                    .iter()
                    .all(|old| old.resource_version_id != stored.resource_version_id)
            }));
        }
        let stored = version_facts(pool, &published).await;
        current = Some(published.resource.clone());
        history.push((command, published, stored));
        for (original, published, stored) in &history {
            assert_eq!(version_facts(pool, published).await, *stored);
            for expected in &published.versions {
                let read = repository
                    .read_resource_version_for_principal(
                        &id(TENANT_ID),
                        &id(PRINCIPAL_ID),
                        PrincipalKind::TenantAdmin,
                        RegistryResourceKind::Agent,
                        &agent_id,
                        &id(&expected.resource_version_id),
                    )
                    .await
                    .unwrap();
                assert_eq!(&read, expected);
                assert_eq!(
                    read.payload.value["document"],
                    serde_json::to_value(&original.versions[0].payload.document).unwrap()
                );
                assert_eq!(
                    read.payload.value["validation"],
                    serde_json::to_value(&original.versions[0].payload.validation).unwrap()
                );
            }
            let before_replay = facts(pool).await;
            let CommandOutcome::Replayed(replayed) =
                registry_command!(repository, publish_resource_versions, original.clone()).unwrap()
            else {
                panic!("the same publish Receipt must return its original immutable batch");
            };
            assert_eq!(replayed.resource, published.resource);
            assert_eq!(replayed.versions, published.versions);
            assert_eq!(facts(pool).await, before_replay);
        }
    }
    let before_stale = facts(pool).await;
    let mut stale = history[0].0.clone();
    stale.audit = publication_audit("stale-cas-new-receipt");
    stale.versions[0].resource_version_id = fresh(ResourceKind::AgentInterfaceRevision);
    stale.versions[1].resource_version_id = fresh(ResourceKind::AgentPlanRevision);
    assert!(matches!(
        registry_command!(repository, publish_resource_versions, stale),
        Err(RepositoryError::Conflict("resource"))
    ));
    assert_eq!(facts(pool).await, before_stale);
    assert_eq!(source_ids.len(), 3);
    assert_eq!(source_bytes.len(), 3);
    assert_eq!(validation_digests.len(), 3);
}
