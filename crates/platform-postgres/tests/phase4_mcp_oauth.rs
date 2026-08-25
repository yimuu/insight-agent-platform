use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use insight_platform_capability_adapters::{
    CapabilityAdapterFailure, CapabilityTransportCancelOutcome, CapabilityTransportCancelRequest,
    GrpcNetworkTransport, GrpcTransportRequest, GrpcTransportResponse, HttpNetworkTransport,
    HttpTransportRequest, HttpTransportResponse,
};
use insight_platform_contracts::{
    canonical_digest, AllowedMcpServerCapabilities, ArtifactRef, AuthoringPackage,
    CapabilityEndpointScheme, CommandAudit, DataClassification, DeploymentClosure,
    ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef, McpAuthPolicyDocument,
    McpClientCapabilities, McpMetadataPolicy, McpMethodLimits, McpOAuthClientAuthenticationKind,
    McpOAuthEndpoint, McpProtocolPolicyDocument, McpServerLimits, McpServerResourceSpec,
    McpTransportBinding, McpTransportFeatures, Permission, PermissionSet, PolicyKind,
    PolicyResourceSpec, PrincipalBindingsPayload, PrincipalKind, PublishedMcpMethod,
    PublishedVersionPayload, RegistryResourceKind, ResourceDocument, ResourceId, ResourceKind,
    SecretBindingPayload, SecretPurpose, SecretResolutionPolicy, Sha256Digest, TenantConfig,
    TenantPrincipalPayload, ValidationSummary, MCP_PROTOCOL_BASELINE,
};
use insight_platform_egress_rpc::{
    proto::egress_broker_service_server::EgressBrokerServiceServer, EgressBrokerGrpcService,
    EgressCallerWorkloadIdentity, EgressInternalRpcLimits,
};
use insight_platform_mcp_host::{
    AuthorizedMcpOAuthPkceCleanup, ClaimDueMcpOAuthPkceCleanups,
    McpOAuthAuthorizationPreparationBroker, McpOAuthAuthorizationPreparationError,
    McpOAuthAuthorizationPreparationRequest, McpOAuthAuthorizationStartCommitDisposition,
    McpOAuthAuthorizationStartConfig, McpOAuthAuthorizationStartIntent,
    McpOAuthAuthorizationStartService, McpOAuthAuthorizedGrant, McpOAuthCredentialBroker,
    McpOAuthCredentialBrokerError, McpOAuthExchangeContract, McpOAuthPkceCleanupCause,
    McpOAuthPkceCleanupHint, McpOAuthPkceCleanupOutbox, McpOAuthPkceCleanupSettlement,
    McpOAuthPkceSecretCleaner, McpOAuthPkceSecretCleanupDisposition,
    McpOAuthPkceSecretCleanupError, PreparedMcpOAuthAuthorization, SensitiveMcpOAuthNonce,
    SensitiveOAuthValue, MCP_OAUTH_PKCE_SECRET_PURPOSE,
};
use insight_platform_model_adapters::{
    ModelAdapterCancelOutcome, ModelAdapterCancelRequest, ModelAdapterFailure,
    ModelProviderWireConnector, ModelProviderWireProtocol, ModelProviderWireRequest,
    ModelProviderWireStream,
};
use insight_platform_postgres::{
    repository::{
        NewPrincipal, NewSecretBinding, NewTenant, NewTenantPrincipal, PgRepository, TypedPayload,
    },
    verify_schema,
};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::{Child, Stdio},
    sync::Arc,
    time::{Duration as StdDuration, Instant},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

const OAUTH_CLEANUP_EGRESS_CONFIG_ENV: &str = "PLATFORM_OAUTH_CLEANUP_EGRESS_FIXTURE_CONFIG";

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
    ResourceDocument::Policy(Box::new(PolicyResourceSpec {
        authoring_package: authoring(suffix, '5'),
        contract_digest: sha('6'),
        dependency_versions: vec![],
        policy_versions: vec![],
        policy_kind: kind,
        rules_digest,
        selection: None,
        scheduling: None,
        retention: None,
        model_safety: None,
        model_budget: None,
        model_public_projection: None,
        mcp_protocol: protocol,
        mcp_auth: auth.map(Box::new),
        sandbox_isolation: None,
        sandbox_resource: None,
        sandbox_network: None,
        sandbox_artifact_io: None,
        sandbox_secret_resolution: None,
    }))
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

struct EmptyModel;

#[async_trait]
impl ModelProviderWireConnector for EmptyModel {
    async fn open(
        &self,
        _request: ModelProviderWireRequest,
    ) -> Result<ModelProviderWireStream, ModelAdapterFailure> {
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn cancel(
        &self,
        _protocol: ModelProviderWireProtocol,
        _request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        Ok(ModelAdapterCancelOutcome::Unsupported)
    }
}

struct EmptyHttp;

#[async_trait]
impl HttpNetworkTransport for EmptyHttp {
    async fn round_trip(
        &self,
        _request: HttpTransportRequest,
    ) -> Result<HttpTransportResponse, CapabilityAdapterFailure> {
        unreachable!("OAuth cleanup fixture does not dispatch HTTP Capability work")
    }

    async fn cancel(
        &self,
        _request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        Ok(CapabilityTransportCancelOutcome::Unsupported)
    }
}

struct EmptyGrpc;

#[async_trait]
impl GrpcNetworkTransport for EmptyGrpc {
    async fn unary(
        &self,
        _request: GrpcTransportRequest,
    ) -> Result<GrpcTransportResponse, CapabilityAdapterFailure> {
        unreachable!("OAuth cleanup fixture does not dispatch gRPC Capability work")
    }

    async fn cancel(
        &self,
        _request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        Ok(CapabilityTransportCancelOutcome::Unsupported)
    }
}

struct RejectedOAuth;

#[async_trait]
impl McpOAuthCredentialBroker for RejectedOAuth {
    async fn exchange_authorization_code(
        &self,
        _contract: &McpOAuthExchangeContract,
        _authorization_code: SensitiveOAuthValue,
        _now: DateTime<Utc>,
    ) -> Result<McpOAuthAuthorizedGrant, McpOAuthCredentialBrokerError> {
        Err(McpOAuthCredentialBrokerError::Rejected)
    }
}

struct ProcessCleanupSecretCleaner {
    call_path: PathBuf,
    stall: bool,
}

#[async_trait]
impl McpOAuthPkceSecretCleaner for ProcessCleanupSecretCleaner {
    async fn delete_exact(
        &self,
        authorization: &AuthorizedMcpOAuthPkceCleanup,
    ) -> Result<McpOAuthPkceSecretCleanupDisposition, McpOAuthPkceSecretCleanupError> {
        authorization
            .validate_for(&insight_platform_mcp_host::McpOAuthPkceCleanupRequest {
                tenant_id: authorization.tenant_id.clone(),
                task_id: authorization.task_id.clone(),
                cause: McpOAuthPkceCleanupCause::Authorized,
                hint: McpOAuthPkceCleanupHint {
                    schema_version: 1,
                    secret_binding_id: authorization.secret_binding.secret_binding_id.clone(),
                    binding_generation: authorization.secret_binding.binding_generation,
                },
            })
            .map_err(|_| McpOAuthPkceSecretCleanupError::Rejected)?;
        std::fs::write(&self.call_path, b"called")
            .map_err(|_| McpOAuthPkceSecretCleanupError::TemporarilyUnavailable)?;
        if self.stall {
            std::future::pending::<()>().await;
        }
        Ok(McpOAuthPkceSecretCleanupDisposition::Deleted)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct OAuthCleanupEgressProcessConfig {
    listen_address: std::net::SocketAddr,
    ca_pem: String,
    server_certificate_pem: String,
    server_key_pem: String,
    call_path: PathBuf,
    ready_path: PathBuf,
    stall: bool,
}

async fn run_oauth_cleanup_egress_fixture(config: OAuthCleanupEgressProcessConfig) {
    let limits = EgressInternalRpcLimits::new(65_536, 1_048_576).unwrap();
    let service = EgressBrokerServiceServer::new(
        EgressBrokerGrpcService::new(
            Arc::new(EmptyModel),
            Arc::new(EmptyHttp),
            Arc::new(EmptyGrpc),
            limits,
        )
        .with_mcp_oauth(
            Arc::new(RejectedOAuth),
            Arc::new(ProcessCleanupSecretCleaner {
                call_path: config.call_path,
                stall: config.stall,
            }),
        ),
    );
    let service =
        tonic::service::interceptor::InterceptedService::new(service, EgressCallerWorkloadIdentity);
    let listener = tokio::net::TcpListener::bind(config.listen_address)
        .await
        .unwrap();
    std::fs::write(config.ready_path, b"ready").unwrap();
    Server::builder()
        .tls_config(
            ServerTlsConfig::new()
                .identity(Identity::from_pem(
                    config.server_certificate_pem,
                    config.server_key_pem,
                ))
                .client_ca_root(Certificate::from_pem(config.ca_pem)),
        )
        .unwrap()
        .add_service(service)
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .unwrap();
}

#[test]
fn oauth_cleanup_egress_fixture_process() {
    let Ok(path) = std::env::var(OAUTH_CLEANUP_EGRESS_CONFIG_ENV) else {
        return;
    };
    let config = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_oauth_cleanup_egress_fixture(config));
}

struct OAuthCleanupTlsFixture {
    ca: String,
    server_cert: String,
    server_key: String,
    client_cert: String,
    client_key: String,
}

fn oauth_cleanup_tls_fixture() -> OAuthCleanupTlsFixture {
    let mut ca_parameters = CertificateParams::default();
    ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_parameters.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca = CertifiedIssuer::self_signed(ca_parameters, KeyPair::generate().unwrap()).unwrap();
    let issue = |sans, usage| {
        let mut parameters = CertificateParams::default();
        parameters.subject_alt_names = sans;
        parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        parameters.extended_key_usages = vec![usage];
        let key = KeyPair::generate().unwrap();
        let certificate = parameters.signed_by(&key, &ca).unwrap();
        (certificate.pem(), key.serialize_pem())
    };
    let (server_cert, server_key) = issue(
        vec![SanType::DnsName("egress.test".try_into().unwrap())],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (client_cert, client_key) = issue(
        vec![SanType::URI(
            insight_platform_egress_rpc::MCP_HOST_WORKLOAD_IDENTITY
                .try_into()
                .unwrap(),
        )],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    OAuthCleanupTlsFixture {
        ca: ca.pem(),
        server_cert,
        server_key,
        client_cert,
        client_key,
    }
}

fn available_address() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn wait_for_file(path: &Path, timeout: StdDuration) {
    let started = Instant::now();
    while !path.exists() {
        assert!(
            started.elapsed() < timeout,
            "file was not created: {}",
            path.display()
        );
        std::thread::sleep(StdDuration::from_millis(10));
    }
}

fn spawn_oauth_cleanup_egress(config_path: &Path) -> Child {
    std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("oauth_cleanup_egress_fixture_process")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(OAUTH_CLEANUP_EGRESS_CONFIG_ENV, config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn write_json(path: &Path, value: &serde_json::Value) -> Sha256Digest {
    std::fs::write(path, serde_jcs::to_vec(value).unwrap()).unwrap();
    canonical_digest(value).unwrap().parse().unwrap()
}

struct CleanupWorkerSpawn<'a> {
    binary: &'a str,
    config_path: &'a Path,
    config_digest: &'a Sha256Digest,
    database_url: &'a str,
    ca_path: &'a Path,
    client_cert_path: &'a Path,
    client_key_path: &'a Path,
}

fn spawn_cleanup_worker(input: CleanupWorkerSpawn<'_>) -> Child {
    std::process::Command::new(input.binary)
        .env("PLATFORM_MCP_CLEANUP_CONFIG", input.config_path)
        .env(
            "PLATFORM_MCP_CLEANUP_CONFIG_DIGEST",
            input.config_digest.as_str(),
        )
        .env("PLATFORM_MCP_CLEANUP_DATABASE_URL", input.database_url)
        .env("PLATFORM_MCP_CLEANUP_EGRESS_CA_PATH", input.ca_path)
        .env(
            "PLATFORM_MCP_CLEANUP_EGRESS_CERT_PATH",
            input.client_cert_path,
        )
        .env(
            "PLATFORM_MCP_CLEANUP_EGRESS_KEY_PATH",
            input.client_key_path,
        )
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn kill(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
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

#[tokio::test]
async fn phase4_mcp_oauth_cleanup_process_recovers_egress_and_worker_kill() {
    let (Ok(database_url), Ok(cleanup_binary)) = (
        std::env::var("PLATFORM_TEST_DATABASE_URL"),
        std::env::var("PLATFORM_MCP_CLEANUP_WORKER_BIN"),
    ) else {
        eprintln!(
            "PLATFORM_TEST_DATABASE_URL or PLATFORM_MCP_CLEANUP_WORKER_BIN is unset; process L3 skipped"
        );
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let fixture = fixture(now);
    seed(&pool, &repository, &fixture).await;
    let start = McpOAuthAuthorizationStartService::new(
        McpOAuthAuthorizationStartConfig {
            callback_binding_digest: fixture
                .auth_profile
                .redirect_uri
                .endpoint_identity_digest
                .clone(),
        },
        Arc::new(repository.clone()),
        Arc::new(FixedPreparation {
            pkce_binding: fixture.pkce_binding.clone(),
        }),
    );
    let started = start.start(fixture.intent.clone(), now).await.unwrap();
    assert_eq!(
        started.disposition,
        McpOAuthAuthorizationStartCommitDisposition::Applied
    );
    let mut task_payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM insight_platform.tasks WHERE tenant_id = $1 AND task_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.intent.task_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    let principal = task_payload.get("created_by").cloned().unwrap();
    task_payload
        .as_object_mut()
        .unwrap()
        .remove("schema_version");
    task_payload.as_object_mut().unwrap().insert(
        "resolution".to_owned(),
        serde_json::json!({
            "state": "responded",
            "principal": principal,
            "response_value_id": null,
            "response_schema_digest": null,
        }),
    );
    let terminal_task_payload = TypedPayload::new(1, &task_payload).unwrap();
    sqlx::query(
        r#"
        UPDATE insight_platform.tasks
        SET state = 'responded', version = version + 1,
            payload_schema_version = $3, payload = $4, payload_digest = $5,
            responded_at = clock_timestamp(), updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND task_id = $2 AND state = 'pending'
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.intent.task_id.to_string())
    .bind(terminal_task_payload.schema_version)
    .bind(&terminal_task_payload.value)
    .bind(&terminal_task_payload.digest)
    .execute(&pool)
    .await
    .unwrap();

    let event_id = id(ResourceKind::Event, 0xc0);
    let outbox_id = id(ResourceKind::OutboxEvent, 0xc1);
    let completion = TypedPayload::new(
        1,
        &serde_json::json!({
            "authorization_binding_id": fixture.intent.authorization_binding_id,
            "callback_ingress_generation_id": id(ResourceKind::WorkerProcessGeneration, 0xc2),
            "pkce_cleanup": McpOAuthPkceCleanupHint {
                schema_version: 1,
                secret_binding_id: fixture.pkce_binding.secret_binding_id.clone(),
                binding_generation: fixture.pkce_binding.binding_generation,
            },
            "state": "responded",
            "task_id": fixture.intent.task_id,
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
    .bind(fixture.tenant_id.to_string())
    .bind(event_id.to_string())
    .bind(fixture.intent.authorization_binding_id.to_string())
    .bind(completion.schema_version)
    .bind(&completion.value)
    .bind(&completion.digest)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO insight_platform.outbox_events (tenant_id, outbox_id, event_id) VALUES ($1, $2, $3)",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(outbox_id.to_string())
    .bind(event_id.to_string())
    .execute(&pool)
    .await
    .unwrap();

    let temporary = std::env::temp_dir().join(format!(
        "platform-oauth-cleanup-l3-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir(&temporary).unwrap();
    let tls = oauth_cleanup_tls_fixture();
    let ca_path = temporary.join("ca.pem");
    let client_cert_path = temporary.join("client.pem");
    let client_key_path = temporary.join("client-key.pem");
    std::fs::write(&ca_path, &tls.ca).unwrap();
    std::fs::write(&client_cert_path, &tls.client_cert).unwrap();
    std::fs::write(&client_key_path, &tls.client_key).unwrap();
    let egress_address = available_address();
    let cleanup_config_path = temporary.join("cleanup.json");
    let cleanup_config = serde_json::json!({
        "schema_version": 1,
        "observability_listen_address": available_address().to_string(),
        "database_max_connections": 4,
        "database_acquire_timeout_milliseconds": 2_000,
        "egress_endpoint": format!("https://{egress_address}/"),
        "egress_tls_server_name": "egress.test",
        "egress_connect_timeout_milliseconds": 2_000,
        "egress_request_timeout_milliseconds": 30_000,
        "maximum_rpc_metadata_bytes": 65_536,
        "maximum_rpc_payload_bytes": 1_048_576,
        "poll_interval_milliseconds": 20,
        "maximum_batch": 64,
        "maximum_lease_milliseconds": 60_000,
        "claim_batch": 1,
        "lease_milliseconds": 300,
        "retry_base_milliseconds": 20,
        "retry_maximum_milliseconds": 1_000,
    });
    let cleanup_config_digest = write_json(&cleanup_config_path, &cleanup_config);
    let first_call = temporary.join("first-call");
    let first_ready = temporary.join("first-ready");
    let first_egress_config_path = temporary.join("egress-first.json");
    let first_egress_config = serde_json::to_value(OAuthCleanupEgressProcessConfig {
        listen_address: egress_address,
        ca_pem: tls.ca.clone(),
        server_certificate_pem: tls.server_cert.clone(),
        server_key_pem: tls.server_key.clone(),
        call_path: first_call.clone(),
        ready_path: first_ready.clone(),
        stall: true,
    })
    .unwrap();
    write_json(&first_egress_config_path, &first_egress_config);
    let mut first_egress = spawn_oauth_cleanup_egress(&first_egress_config_path);
    wait_for_file(&first_ready, StdDuration::from_secs(5));
    let spawn_worker = || {
        spawn_cleanup_worker(CleanupWorkerSpawn {
            binary: &cleanup_binary,
            config_path: &cleanup_config_path,
            config_digest: &cleanup_config_digest,
            database_url: &database_url,
            ca_path: &ca_path,
            client_cert_path: &client_cert_path,
            client_key_path: &client_key_path,
        })
    };
    let mut first_worker = spawn_worker();
    wait_for_file(&first_call, StdDuration::from_secs(10));
    let first_claim_epoch: i64 = sqlx::query_scalar(
        "SELECT claim_epoch FROM insight_platform.outbox_events WHERE tenant_id = $1 AND outbox_id = $2 AND state = 'cleanup_claimed'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(outbox_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(first_claim_epoch, 1);
    kill(&mut first_egress);
    kill(&mut first_worker);
    tokio::time::sleep(StdDuration::from_millis(450)).await;

    let second_call = temporary.join("second-call");
    let second_ready = temporary.join("second-ready");
    let second_egress_config_path = temporary.join("egress-second.json");
    let second_egress_config = serde_json::to_value(OAuthCleanupEgressProcessConfig {
        listen_address: egress_address,
        ca_pem: tls.ca,
        server_certificate_pem: tls.server_cert,
        server_key_pem: tls.server_key,
        call_path: second_call.clone(),
        ready_path: second_ready.clone(),
        stall: false,
    })
    .unwrap();
    write_json(&second_egress_config_path, &second_egress_config);
    let mut second_egress = spawn_oauth_cleanup_egress(&second_egress_config_path);
    wait_for_file(&second_ready, StdDuration::from_secs(5));
    let mut second_worker = spawn_worker();
    wait_for_file(&second_call, StdDuration::from_secs(10));
    let started_wait = Instant::now();
    let terminal = loop {
        let row = sqlx::query(
            "SELECT state, claim_epoch, claim_owner, claim_expires_at FROM insight_platform.outbox_events WHERE tenant_id = $1 AND outbox_id = $2",
        )
        .bind(fixture.tenant_id.to_string())
        .bind(outbox_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        if row.get::<String, _>("state") == "cleanup_completed" {
            break row;
        }
        assert!(started_wait.elapsed() < StdDuration::from_secs(10));
        tokio::time::sleep(StdDuration::from_millis(20)).await;
    };
    kill(&mut second_worker);
    kill(&mut second_egress);
    assert_eq!(terminal.get::<i64, _>("claim_epoch"), 2);
    assert!(terminal.get::<Option<String>, _>("claim_owner").is_none());
    assert!(terminal
        .get::<Option<DateTime<Utc>>, _>("claim_expires_at")
        .is_none());
    std::fs::remove_dir_all(temporary).unwrap();
}
