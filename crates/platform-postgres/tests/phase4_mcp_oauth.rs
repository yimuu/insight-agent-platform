use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    AllowedMcpServerCapabilities, ArtifactRef, AuthoringPackage, CapabilityEndpointScheme,
    CommandAudit, DataClassification, DeploymentClosure, ExactDeploymentRef, ExactSecretBindingRef,
    ExactVersionRef, McpAuthPolicyDocument, McpClientCapabilities, McpMetadataPolicy,
    McpMethodLimits, McpOAuthClientAuthenticationKind, McpOAuthEndpoint, McpProtocolPolicyDocument,
    McpServerLimits, McpServerResourceSpec, McpTransportBinding, McpTransportFeatures, Permission,
    PermissionSet, PolicyKind, PolicyResourceSpec, PrincipalBindingsPayload, PrincipalKind,
    PublishedMcpMethod, PublishedVersionPayload, RegistryResourceKind, ResourceDocument,
    ResourceId, ResourceKind, SecretBindingPayload, SecretPurpose, SecretResolutionPolicy,
    Sha256Digest, TenantConfig, TenantPrincipalPayload, ValidationSummary, MCP_PROTOCOL_BASELINE,
};
use insight_platform_mcp_host::{
    ClaimDueMcpOAuthPkceCleanups, McpOAuthAuthorizationPreparationBroker,
    McpOAuthAuthorizationPreparationError, McpOAuthAuthorizationPreparationRequest,
    McpOAuthAuthorizationStartCommitDisposition, McpOAuthAuthorizationStartConfig,
    McpOAuthAuthorizationStartIntent, McpOAuthAuthorizationStartService, McpOAuthPkceCleanupCause,
    McpOAuthPkceCleanupHint, McpOAuthPkceCleanupOutbox, McpOAuthPkceCleanupSettlement,
    PreparedMcpOAuthAuthorization, SensitiveMcpOAuthNonce, SensitiveOAuthValue,
    MCP_OAUTH_PKCE_SECRET_PURPOSE,
};
use insight_platform_postgres::{
    repository::{
        NewPrincipal, NewSecretBinding, NewTenant, NewTenantPrincipal, PgRepository, TypedPayload,
    },
    verify_schema,
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{collections::BTreeMap, sync::Arc};

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1c8-32e4-75e1-a9e8-d95ca0f6{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

fn sha(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn exact(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
    ExactVersionRef::new(id(kind, suffix), sha(character)).unwrap()
}

fn artifact(suffix: u16, character: char) -> ArtifactRef {
    ArtifactRef::new(
        id(ResourceKind::Artifact, suffix),
        sha(character),
        64,
        "application/json",
        DataClassification::Internal,
        Some(format!("mcp-oauth-{suffix}.json")),
    )
    .unwrap()
}

fn authoring(suffix: u16, character: char) -> AuthoringPackage {
    AuthoringPackage {
        artifact: artifact(suffix, character),
        manifest_digest: sha(character),
    }
}

fn validation() -> ValidationSummary {
    ValidationSummary {
        validator_digest: sha('1'),
        validated_draft_digest: sha('2'),
        dependency_closure_digest: sha('3'),
        security_evidence_digest: sha('4'),
        warnings: vec![],
    }
}

fn oauth_endpoint(host: &str, path: &str) -> McpOAuthEndpoint {
    let endpoint = insight_platform_contracts::CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: host.to_owned(),
        port: 443,
        base_path: path.to_owned(),
    };
    McpOAuthEndpoint {
        endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
        endpoint,
    }
}

fn method_limits() -> McpMethodLimits {
    McpMethodLimits {
        maximum_request_bytes: 4_096,
        maximum_response_bytes: 4_096,
        maximum_metadata_entries: 16,
        maximum_progress_events: 16,
        maximum_pages: 8,
        minimum_poll_milliseconds: 10,
        maximum_poll_milliseconds: 1_000,
    }
}

fn server_limits() -> McpServerLimits {
    McpServerLimits {
        maximum_message_bytes: 8_192,
        maximum_response_bytes: 8_192,
        maximum_headers: 32,
        maximum_sse_event_bytes: 4_096,
        maximum_in_flight: 8,
        maximum_connections: 4,
        maximum_sessions: 4,
        maximum_session_milliseconds: 3_600_000,
        idle_timeout_milliseconds: 1_000,
        initialize_timeout_milliseconds: 2_000,
        request_timeout_milliseconds: 10_000,
        total_timeout_milliseconds: 30_000,
    }
}

fn protocol_document() -> McpProtocolPolicyDocument {
    McpProtocolPolicyDocument {
        schema_version: 1,
        offered_versions: vec![MCP_PROTOCOL_BASELINE.to_owned()],
        transport_features: McpTransportFeatures {
            streamable_http_get: true,
            streamable_http_sse: true,
            resumable_stream: true,
            session_affinity: true,
        },
        client_capabilities: McpClientCapabilities {
            elicitation_form: true,
            elicitation_url: false,
            tasks_elicitation_create: false,
            sampling: false,
            roots: false,
        },
        allowed_server_capabilities: AllowedMcpServerCapabilities {
            tools: true,
            resources: false,
            prompts: false,
            logging: false,
            tasks: false,
            subscriptions: false,
        },
        experimental_features: vec![],
        method_limits: BTreeMap::from([(PublishedMcpMethod::ToolsCall, method_limits())]),
        metadata_policy: McpMetadataPolicy {
            maximum_server_name_bytes: 128,
            maximum_server_version_bytes: 64,
            maximum_instruction_bytes: 4_096,
            maximum_object_name_bytes: 128,
            maximum_description_bytes: 8_192,
            maximum_icon_bytes: 1_048_576,
        },
    }
}

fn policy_document(
    suffix: u16,
    kind: PolicyKind,
    rules_digest: Sha256Digest,
    protocol: Option<McpProtocolPolicyDocument>,
    auth: Option<McpAuthPolicyDocument>,
) -> ResourceDocument {
    ResourceDocument::Policy(PolicyResourceSpec {
        authoring_package: authoring(suffix, '5'),
        contract_digest: sha('6'),
        dependency_versions: vec![],
        policy_versions: vec![],
        policy_kind: kind,
        rules_digest,
        selection: None,
        scheduling: None,
        retention: None,
        mcp_protocol: protocol,
        mcp_auth: auth.map(Box::new),
        sandbox_isolation: None,
        sandbox_resource: None,
        sandbox_network: None,
        sandbox_artifact_io: None,
        sandbox_secret_resolution: None,
    })
}

struct Fixture {
    tenant_id: ResourceId,
    principal_id: ResourceId,
    policy_resource_id: ResourceId,
    server_resource_id: ResourceId,
    policy_versions: Vec<(ExactVersionRef, ResourceDocument)>,
    server_revision: ExactVersionRef,
    server_document: ResourceDocument,
    deployment: ExactDeploymentRef,
    closure_payload: TypedPayload,
    conformance: ArtifactRef,
    retention_policy: ExactVersionRef,
    pkce_binding: ExactSecretBindingRef,
    auth_profile: McpAuthPolicyDocument,
    intent: McpOAuthAuthorizationStartIntent,
}

fn fixture(now: DateTime<Utc>) -> Fixture {
    let tenant_id = id(ResourceKind::Tenant, 1);
    let principal_id = id(ResourceKind::Principal, 2);
    let policy_resource_id = id(ResourceKind::Policy, 3);
    let server_resource_id = id(ResourceKind::McpServer, 4);
    let protocol = protocol_document();
    let protocol_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 5),
        protocol.canonical_digest().unwrap(),
    )
    .unwrap();
    let network_policy = exact(ResourceKind::PolicyRevision, 6, '7');
    let tls_policy = exact(ResourceKind::PolicyRevision, 7, '8');
    let trust_policy = exact(ResourceKind::PolicyRevision, 8, '9');
    let auth_profile = McpAuthPolicyDocument {
        schema_version: 1,
        issuer: oauth_endpoint("auth.example.test", "/"),
        authorization_endpoint: oauth_endpoint("auth.example.test", "/oauth/authorize"),
        token_endpoint: oauth_endpoint("auth.example.test", "/oauth/token"),
        client_id: "insight-platform".to_owned(),
        client_authentication: McpOAuthClientAuthenticationKind::None,
        client_credential_purpose: None,
        pkce_secret_provider_id: id(ResourceKind::SecretProvider, 13),
        token_secret_provider_id: id(ResourceKind::SecretProvider, 118),
        redirect_uri: oauth_endpoint("platform.example.test", "/v1/mcp/oauth/callback"),
        resource_indicator: oauth_endpoint("mcp.example.test", "/mcp"),
        allowed_scopes: vec!["tools.call".to_owned(), "tools.read".to_owned()],
        maximum_token_response_bytes: 65_536,
        connect_timeout_milliseconds: 5_000,
        total_timeout_milliseconds: 30_000,
        maximum_clock_skew_seconds: 60,
    };
    let auth_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 9),
        auth_profile.canonical_digest().unwrap(),
    )
    .unwrap();
    let policy_versions = vec![
        (
            protocol_policy.clone(),
            policy_document(
                0x80,
                PolicyKind::Protocol,
                protocol_policy.semantic_digest.clone(),
                Some(protocol),
                None,
            ),
        ),
        (
            network_policy.clone(),
            policy_document(0x81, PolicyKind::Network, sha('7'), None, None),
        ),
        (
            tls_policy.clone(),
            policy_document(0x82, PolicyKind::Tls, sha('8'), None, None),
        ),
        (
            trust_policy.clone(),
            policy_document(0x83, PolicyKind::Trust, sha('9'), None, None),
        ),
        (
            auth_policy.clone(),
            policy_document(
                0x84,
                PolicyKind::McpAuth,
                auth_policy.semantic_digest.clone(),
                None,
                Some(auth_profile.clone()),
            ),
        ),
    ];
    let server_revision = exact(ResourceKind::McpServerRevision, 10, 'a');
    let token_purpose = "mcp.oauth.token".parse::<SecretPurpose>().unwrap();
    let server_document = ResourceDocument::McpServer(McpServerResourceSpec {
        authoring_package: authoring(0x85, 'b'),
        contract_digest: sha('c'),
        dependency_versions: vec![],
        policy_versions: vec![],
        transport: insight_platform_contracts::McpTransportKind::StreamableHttp,
        protocol_policy: protocol_policy.clone(),
        deployment_credential_requirements: vec![],
        authorization_credential_purpose: Some(token_purpose),
        limits: server_limits(),
    });
    let endpoint = auth_profile.resource_indicator.endpoint.clone();
    let transport = McpTransportBinding::StreamableHttp {
        endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
        endpoint,
        network_policy,
        tls_policy,
    };
    let conformance = artifact(0x86, 'd');
    let closure = insight_platform_contracts::McpDeploymentClosure {
        server_revision: server_revision.clone(),
        server_identity_digest: auth_profile
            .resource_indicator
            .endpoint_identity_digest
            .clone(),
        transport,
        protocol_policy,
        trust_policy: trust_policy.clone(),
        auth_policy: Some(auth_policy),
        secret_bindings: vec![],
        conformance_evidence: conformance.clone(),
    };
    let closure_payload = TypedPayload::new(1, &DeploymentClosure::McpServer(closure)).unwrap();
    let deployment = ExactDeploymentRef::new(
        id(ResourceKind::McpDeployment, 11),
        closure_payload.digest.parse().unwrap(),
    )
    .unwrap();
    let pkce_binding = ExactSecretBindingRef::build(
        id(ResourceKind::SecretBinding, 12),
        1,
        id(ResourceKind::SecretProvider, 13),
        MCP_OAUTH_PKCE_SECRET_PURPOSE.parse().unwrap(),
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: sha('e'),
        },
    )
    .unwrap();
    let deadline = now + Duration::minutes(10);
    let intent = McpOAuthAuthorizationStartIntent {
        audit: CommandAudit {
            tenant_id: tenant_id.clone(),
            principal_id: principal_id.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            receipt_id: id(ResourceKind::Receipt, 14),
            event_id: id(ResourceKind::Event, 15),
            outbox_id: id(ResourceKind::OutboxEvent, 16),
            idempotency_key_digest: sha('f'),
            request_digest: sha('0'),
            receipt_expires_at: now + Duration::minutes(30),
        },
        task_id: id(ResourceKind::Interaction, 17),
        authorization_binding_id: id(ResourceKind::McpAuthorizationBinding, 18),
        mcp_deployment: deployment.clone(),
        expected_principal_binding_generation: 1,
        requested_scopes: vec!["tools.call".to_owned(), "tools.read".to_owned()],
        reauthorization: None,
        safe_prompt_key: "mcp_oauth_authorize".to_owned(),
        deadline,
    };
    Fixture {
        tenant_id,
        principal_id,
        policy_resource_id,
        server_resource_id,
        policy_versions,
        server_revision,
        server_document,
        deployment,
        closure_payload,
        conformance,
        retention_policy: trust_policy,
        pkce_binding,
        auth_profile,
        intent,
    }
}

async fn insert_resource(
    pool: &PgPool,
    tenant_id: &ResourceId,
    resource_id: &ResourceId,
    kind: RegistryResourceKind,
    _principal_id: &ResourceId,
) {
    let payload = TypedPayload::new(1, &serde_json::json!({"fixture": true})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, $3, 'active', 'enabled', $4, $5, $6)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(resource_id.to_string())
    .bind(kind.as_str())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_version(
    pool: &PgPool,
    fixture: &Fixture,
    resource_id: &ResourceId,
    exact: &ExactVersionRef,
    revision_no: i64,
    document: ResourceDocument,
) {
    document.validate().unwrap();
    let payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document,
            validation: validation(),
        },
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resource_versions (
            tenant_id, resource_version_id, resource_id, resource_version_kind,
            revision_no, content_digest, payload_schema_version, payload,
            payload_digest, created_by
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(exact.revision_id.to_string())
    .bind(resource_id.to_string())
    .bind(exact.resource_kind.descriptor().name)
    .bind(revision_no)
    .bind(exact.semantic_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(fixture.principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resources SET active_version_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(resource_id.to_string())
    .bind(exact.revision_id.to_string())
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_ready_artifact(pool: &PgPool, fixture: &Fixture) {
    let blob_id = id(ResourceKind::InternalBlob, 0x90);
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest, security_domain_digest,
            object_reference_ciphertext, object_generation, key_id, encryption_domain_id,
            content_digest, size_bytes, state, version, verified_at, created_at, updated_at
        ) VALUES ($1, $2, 'fixture', $3, $4, $5, 'generation-1', 'fixture-key', $6,
                  $7, $8, 'verified', 1, $9, $9, $9)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(blob_id.to_string())
    .bind(sha('1').to_string())
    .bind(sha('2').to_string())
    .bind(vec![1_u8, 2, 3])
    .bind(id(ResourceKind::Policy, 0x91).to_string())
    .bind(fixture.conformance.content_digest().to_string())
    .bind(i64::try_from(fixture.conformance.byte_length()).unwrap())
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification, expected_size_bytes,
            expected_digest, declared_media_type, verified_media_type, state, version,
            metadata_schema_version, metadata, metadata_digest, retention_policy_revision_id,
            retain_until, created_by, created_at, updated_at
        ) VALUES ($1, $2, $3, 'conformance', $4, $5, $6, $7, $7, 'ready', 1,
                  1, '{}'::jsonb, $8, $9, $10, $11, $12, $12)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.conformance.artifact_id().to_string())
    .bind(blob_id.to_string())
    .bind(fixture.conformance.classification().as_str())
    .bind(i64::try_from(fixture.conformance.byte_length()).unwrap())
    .bind(fixture.conformance.content_digest().to_string())
    .bind(fixture.conformance.media_type())
    .bind(sha('3').to_string())
    .bind(fixture.retention_policy.revision_id.to_string())
    .bind(now + Duration::days(1))
    .bind(fixture.principal_id.to_string())
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed(pool: &PgPool, repository: &PgRepository, fixture: &Fixture) {
    repository
        .create_tenant(NewTenant {
            tenant_id: fixture.tenant_id.to_string(),
            state: "active".to_owned(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    repository
        .create_principal(NewPrincipal {
            principal_id: fixture.principal_id.clone(),
            authentication_authority_digest: sha('4'),
            subject_digest: sha('5'),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: fixture.tenant_id.clone(),
            principal_id: fixture.principal_id.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![Permission::McpWrite]).unwrap(),
            },
        })
        .await
        .unwrap();
    insert_resource(
        pool,
        &fixture.tenant_id,
        &fixture.policy_resource_id,
        RegistryResourceKind::Policy,
        &fixture.principal_id,
    )
    .await;
    insert_resource(
        pool,
        &fixture.tenant_id,
        &fixture.server_resource_id,
        RegistryResourceKind::McpServer,
        &fixture.principal_id,
    )
    .await;
    for (index, (exact, document)) in fixture.policy_versions.iter().enumerate() {
        insert_version(
            pool,
            fixture,
            &fixture.policy_resource_id,
            exact,
            i64::try_from(index + 1).unwrap(),
            document.clone(),
        )
        .await;
    }
    insert_version(
        pool,
        fixture,
        &fixture.server_resource_id,
        &fixture.server_revision,
        1,
        fixture.server_document.clone(),
    )
    .await;
    insert_ready_artifact(pool, fixture).await;
    repository
        .create_secret_binding(NewSecretBinding {
            tenant_id: fixture.tenant_id.clone(),
            secret_binding_id: fixture.pkce_binding.secret_binding_id.clone(),
            purpose: fixture.pkce_binding.purpose.clone(),
            provider_id: fixture.pkce_binding.provider_id.clone(),
            opaque_reference_ciphertext: vec![9, 8, 7],
            key_id: "fixture-key".to_owned(),
            reference_digest: sha('6'),
            payload: SecretBindingPayload {
                provider_id: fixture.pkce_binding.provider_id.clone(),
                resolution_policy: fixture.pkce_binding.resolution_policy.clone(),
            },
        })
        .await
        .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id,
            environment, bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.deployment.deployment_id.to_string())
    .bind(fixture.server_resource_id.to_string())
    .bind(fixture.server_revision.revision_id.to_string())
    .bind(&fixture.closure_payload.digest)
    .bind(fixture.closure_payload.schema_version)
    .bind(&fixture.closure_payload.value)
    .bind(fixture.principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
}

struct FixedPreparation {
    pkce_binding: ExactSecretBindingRef,
}

#[async_trait]
impl McpOAuthAuthorizationPreparationBroker for FixedPreparation {
    async fn prepare_or_load(
        &self,
        request: &McpOAuthAuthorizationPreparationRequest,
        _now: DateTime<Utc>,
    ) -> Result<PreparedMcpOAuthAuthorization, McpOAuthAuthorizationPreparationError> {
        Ok(PreparedMcpOAuthAuthorization {
            preparation_digest: request.preparation_digest.clone(),
            state: SensitiveOAuthValue::from_decoded(
                b"oauth-state-canary".to_vec(),
                insight_platform_mcp_host::MAX_MCP_OAUTH_STATE_BYTES,
            )
            .unwrap(),
            nonce: SensitiveMcpOAuthNonce::new(b"n".repeat(43)).unwrap(),
            pkce_challenge: "a".repeat(43),
            pkce_secret_binding: self.pkce_binding.clone(),
            storage_evidence_digest: sha('7'),
        })
    }
}

fn competing_intent(
    mut intent: McpOAuthAuthorizationStartIntent,
) -> McpOAuthAuthorizationStartIntent {
    intent.audit.receipt_id = id(ResourceKind::Receipt, 20);
    intent.audit.event_id = id(ResourceKind::Event, 21);
    intent.audit.outbox_id = id(ResourceKind::OutboxEvent, 22);
    intent.audit.idempotency_key_digest = sha('8');
    intent.audit.request_digest = sha('9');
    intent.task_id = id(ResourceKind::Interaction, 23);
    intent.authorization_binding_id = id(ResourceKind::McpAuthorizationBinding, 24);
    intent
}

#[tokio::test]
async fn phase4_mcp_oauth_start_is_idempotent_secret_free_and_first_winner() {
    let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
        eprintln!("PLATFORM_TEST_DATABASE_URL is unset; real PostgreSQL fixture skipped");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let now = Utc::now();
    let fixture = fixture(now);
    seed(&pool, &repository, &fixture).await;
    let callback_binding_digest = fixture
        .auth_profile
        .redirect_uri
        .endpoint_identity_digest
        .clone();
    let service = Arc::new(McpOAuthAuthorizationStartService::new(
        McpOAuthAuthorizationStartConfig {
            callback_binding_digest,
        },
        Arc::new(repository.clone()),
        Arc::new(FixedPreparation {
            pkce_binding: fixture.pkce_binding.clone(),
        }),
    ));
    let first_intent = fixture.intent.clone();
    let second_intent = competing_intent(fixture.intent.clone());
    let (first, second) = tokio::join!(
        service.start(first_intent.clone(), now),
        service.start(second_intent.clone(), now)
    );
    let (winner, winner_intent) = match (first, second) {
        (Ok(outcome), Err(_)) => (outcome, first_intent),
        (Err(_), Ok(outcome)) => (outcome, second_intent),
        outcomes => panic!("expected one OAuth start winner, got {outcomes:?}"),
    };
    assert_eq!(
        winner.disposition,
        McpOAuthAuthorizationStartCommitDisposition::Applied
    );
    let winner_url = winner.authorization_url.as_str().to_owned();
    let replay = service.start(winner_intent, now).await.unwrap();
    assert_eq!(
        replay.disposition,
        McpOAuthAuthorizationStartCommitDisposition::Replayed
    );
    assert_eq!(replay.authorization_url.as_str(), winner_url);

    let task_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.tasks WHERE tenant_id = $1 AND task_kind = 'external_authorization'",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND operation = 'mcp.oauth.begin'",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    let event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_type = 'mcp.oauth_authorization_started'",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    let outbox_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id = $1",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        (task_count, receipt_count, event_count, outbox_count),
        (1, 1, 1, 1)
    );
    let persisted: String = sqlx::query_scalar(
        r#"
        SELECT concat_ws('|', task.payload::text, receipt.payload::text, event.payload::text)
        FROM insight_platform.tasks AS task
        JOIN insight_platform.receipts AS receipt
          ON receipt.tenant_id = task.tenant_id AND receipt.scope_id = task.task_id
        JOIN insight_platform.events AS event
          ON event.tenant_id = task.tenant_id AND event.aggregate_id = task.task_id
        WHERE task.tenant_id = $1 AND task.task_kind = 'external_authorization'
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!persisted.contains("oauth-state-canary"));
    assert!(!persisted.contains(&"n".repeat(43)));
}

#[tokio::test]
async fn phase4_mcp_oauth_cleanup_outbox_claim_is_reclaimable_and_exactly_fenced() {
    let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
        eprintln!("PLATFORM_TEST_DATABASE_URL is unset; real PostgreSQL fixture skipped");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let tenant_id = id(ResourceKind::Tenant, 0xb0);
    repository
        .create_tenant(NewTenant {
            tenant_id: tenant_id.to_string(),
            state: "active".to_owned(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    let authorization_binding_id = id(ResourceKind::McpAuthorizationBinding, 0xb1);
    let task_id = id(ResourceKind::Interaction, 0xb2);
    let pkce_secret_binding_id = id(ResourceKind::SecretBinding, 0xb3);
    let event_id = id(ResourceKind::Event, 0xa0);
    let outbox_id = id(ResourceKind::OutboxEvent, 0xa1);
    let callback_generation = id(ResourceKind::WorkerProcessGeneration, 0xa2);
    let payload = TypedPayload::new(
        1,
        &serde_json::json!({
            "authorization_binding_id": authorization_binding_id,
            "callback_ingress_generation_id": callback_generation,
            "pkce_cleanup": McpOAuthPkceCleanupHint {
                schema_version: 1,
                secret_binding_id: pkce_secret_binding_id,
                binding_generation: 1,
            },
            "state": "responded",
            "task_id": task_id,
        }),
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.events (
            tenant_id, event_id, aggregate_kind, aggregate_id, aggregate_version,
            event_type, visibility, payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'mcp_authorization', $3, 1,
                  'mcp.oauth_authorization_completed', 'internal', $4, $5, $6)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(event_id.to_string())
    .bind(authorization_binding_id.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO insight_platform.outbox_events (tenant_id, outbox_id, event_id) VALUES ($1, $2, $3)",
    )
    .bind(tenant_id.to_string())
    .bind(outbox_id.to_string())
    .bind(event_id.to_string())
    .execute(&pool)
    .await
    .unwrap();

    let worker_a = id(ResourceKind::WorkerProcessGeneration, 0xa3);
    let first = repository
        .claim_due_mcp_oauth_pkce_cleanups(ClaimDueMcpOAuthPkceCleanups {
            claim_owner: worker_a,
            maximum_claims: 1,
            lease_milliseconds: 30_000,
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(first.request.cause, McpOAuthPkceCleanupCause::Authorized);
    assert_eq!(first.request.task_id, task_id);
    sqlx::query(
        "UPDATE insight_platform.outbox_events SET claim_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND outbox_id = $2",
    )
    .bind(tenant_id.to_string())
    .bind(outbox_id.to_string())
    .execute(&pool)
    .await
    .unwrap();

    let worker_b = id(ResourceKind::WorkerProcessGeneration, 0xa4);
    let second = repository
        .claim_due_mcp_oauth_pkce_cleanups(ClaimDueMcpOAuthPkceCleanups {
            claim_owner: worker_b,
            maximum_claims: 1,
            lease_milliseconds: 30_000,
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(!repository
        .settle_mcp_oauth_pkce_cleanup(&first, McpOAuthPkceCleanupSettlement::Completed,)
        .await
        .unwrap());
    assert!(repository
        .settle_mcp_oauth_pkce_cleanup(&second, McpOAuthPkceCleanupSettlement::Completed,)
        .await
        .unwrap());
    let row = sqlx::query(
        "SELECT state, published_at, claim_owner, claim_expires_at FROM insight_platform.outbox_events WHERE tenant_id = $1 AND outbox_id = $2",
    )
        .bind(tenant_id.to_string())
        .bind(outbox_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("state"), "cleanup_completed");
    assert!(row
        .get::<Option<DateTime<Utc>>, _>("published_at")
        .is_none());
    assert!(row.get::<Option<String>, _>("claim_owner").is_none());
    assert!(row
        .get::<Option<DateTime<Utc>>, _>("claim_expires_at")
        .is_none());
}
