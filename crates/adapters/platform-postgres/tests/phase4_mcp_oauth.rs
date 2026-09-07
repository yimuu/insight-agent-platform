mod support;
use async_trait::async_trait;
use base64::Engine as _;
use chrono::{DateTime, Duration, Utc};
use insight_platform_capability_adapters::{
    CapabilityAdapterFailure, CapabilityTransportCancelOutcome, CapabilityTransportCancelRequest,
    GrpcNetworkTransport, GrpcTransportRequest, GrpcTransportResponse, HttpNetworkTransport,
    HttpTransportRequest, HttpTransportResponse,
};
use insight_platform_contracts::TypedPayload;
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
use insight_platform_egress::{
    DnsResolutionError, EgressDnsResolver, InstalledMcpOAuthJwtAlgorithm,
    InstalledMcpOAuthVerificationBinding, InstalledMcpOAuthVerificationCatalog,
    McpOAuthEgressLimits, McpOAuthTokenPreparation, McpOAuthTokenSet, McpOAuthTokenStore,
    McpOAuthTokenStoreError, McpOAuthTokenVerificationError, McpOAuthTokenVerifier,
    ReqwestMcpOAuthCredentialBroker, ResolvedSecretMaterial, SecretMaterialResolutionError,
    SecretMaterialResolver, StoredMcpOAuthTokenSecret, VerifiedMcpOAuthToken,
};
use insight_platform_egress_rpc::{
    proto::egress_broker_service_server::EgressBrokerServiceServer, EgressBrokerGrpcClient,
    EgressBrokerGrpcService, EgressCallerWorkloadIdentity, EgressInternalRpcLimits,
};
use insight_platform_mcp_host::{
    AuthenticatedMcpOAuthState, AuthorizedMcpOAuthPkceCleanup, ClaimDueMcpOAuthPkceCleanups,
    CompleteMcpOAuthCallback, McpOAuthAuthorizationPreparationBroker,
    McpOAuthAuthorizationPreparationError, McpOAuthAuthorizationPreparationRequest,
    McpOAuthAuthorizationStartCommitDisposition, McpOAuthAuthorizationStartConfig,
    McpOAuthAuthorizationStartIntent, McpOAuthAuthorizedGrant, McpOAuthCallbackAuthority,
    McpOAuthCallbackAuthorityError, McpOAuthCallbackCommitOutcome, McpOAuthCallbackIngressConfig,
    McpOAuthCredentialBroker, McpOAuthCredentialBrokerError, McpOAuthExchangeContract,
    McpOAuthPkceCleanupAuthority, McpOAuthPkceCleanupCause, McpOAuthPkceCleanupJobs,
    McpOAuthPkceCleanupSettlement, McpOAuthPkceSecretCleaner, McpOAuthPkceSecretCleanupDisposition,
    McpOAuthPkceSecretCleanupError, McpOAuthStateIssuer, PreparedMcpOAuthAuthorization,
    SensitiveMcpOAuthNonce, SensitiveOAuthValue, MCP_OAUTH_PKCE_SECRET_PURPOSE,
};
use insight_platform_mcp_runtime::{
    McpOAuthAuthorizationStartService, McpOAuthCallbackIngress, UuidMcpOAuthCallbackIdentityFactory,
};
use insight_platform_mcp_transport::{
    AeadMcpOAuthStateCodec, McpOAuthStateCodecConfig, McpOAuthStateKey, SensitiveMcpOAuthStateKey,
};
use insight_platform_model_adapters::{
    ModelAdapterCancelOutcome, ModelAdapterCancelRequest, ModelAdapterFailure,
    ModelProviderWireConnector, ModelProviderWireProtocol, ModelProviderWireRequest,
    ModelProviderWireStream,
};
use insight_platform_postgres::{
    repository::{
        NewPrincipal, NewSecretBinding, NewTenant, NewTenantPrincipal, PgRepository,
        RepositoryError,
    },
    verify_schema,
};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair as RingKeyPair},
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{
    collections::BTreeMap,
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    process::{Child, Stdio},
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::{Duration as StdDuration, Instant},
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig};

const OAUTH_CLEANUP_EGRESS_CONFIG_ENV: &str = "PLATFORM_OAUTH_CLEANUP_EGRESS_FIXTURE_CONFIG";
const OAUTH_EXCHANGE_EGRESS_CONFIG_ENV: &str = "PLATFORM_OAUTH_EXCHANGE_EGRESS_FIXTURE_CONFIG";
const OAUTH_EXCHANGE_CALLBACK_CONFIG_ENV: &str = "PLATFORM_OAUTH_EXCHANGE_CALLBACK_FIXTURE_CONFIG";
const OAUTH_TOKEN_ENDPOINT_CONFIG_ENV: &str = "PLATFORM_OAUTH_TOKEN_ENDPOINT_FIXTURE_CONFIG";

static OAUTH_FIXTURE_RUN: std::sync::LazyLock<u32> =
    std::sync::LazyLock::new(|| uuid::Uuid::now_v7().as_u128() as u32);

static OAUTH_FIXTURE_NAMESPACE: AtomicU16 = AtomicU16::new(0xb100);
static OAUTH_FIXTURE_NAMESPACE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn select_fixture_namespace(namespace: u16) -> tokio::sync::MutexGuard<'static, ()> {
    let guard = OAUTH_FIXTURE_NAMESPACE_LOCK.lock().await;
    OAUTH_FIXTURE_NAMESPACE.store(namespace, Ordering::SeqCst);
    guard
}

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    let namespace = OAUTH_FIXTURE_NAMESPACE.load(Ordering::SeqCst);
    format!(
        "{}_{:08x}-32e4-75e1-a9e8-d95c{namespace:04x}{suffix:04x}",
        kind.descriptor().prefix,
        *OAUTH_FIXTURE_RUN
    )
    .parse()
    .unwrap()
}

fn sha(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn namespaced_digest(label: &str) -> Sha256Digest {
    let namespace = OAUTH_FIXTURE_NAMESPACE.load(Ordering::SeqCst);
    canonical_digest(&serde_json::json!({
        "phase4_mcp_oauth": label,
        "fixture_namespace": namespace,
        "fixture_run": *OAUTH_FIXTURE_RUN,
    }))
    .unwrap()
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
        program_requirement: None,
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
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
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

fn fixture_with_token_endpoint_port(now: DateTime<Utc>, port: u16) -> Fixture {
    let mut fixture = fixture(now);
    fixture.auth_profile.allowed_scopes = vec![
        "openid".to_owned(),
        "tools.call".to_owned(),
        "tools.read".to_owned(),
    ];
    fixture.intent.requested_scopes = fixture.auth_profile.allowed_scopes.clone();
    fixture.auth_profile.token_endpoint.endpoint.port = port;
    fixture.auth_profile.token_endpoint.endpoint_identity_digest = fixture
        .auth_profile
        .token_endpoint
        .endpoint
        .canonical_digest()
        .unwrap();
    let auth_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 9),
        fixture.auth_profile.canonical_digest().unwrap(),
    )
    .unwrap();
    let installed_policy = fixture
        .policy_versions
        .iter_mut()
        .find(|(revision, _)| revision.revision_id == auth_policy.revision_id)
        .unwrap();
    *installed_policy = (
        auth_policy.clone(),
        policy_document(
            0x84,
            PolicyKind::McpAuth,
            auth_policy.semantic_digest.clone(),
            None,
            Some(fixture.auth_profile.clone()),
        ),
    );
    let mut closure_value = fixture.closure_payload.value.clone();
    closure_value
        .as_object_mut()
        .unwrap()
        .remove("schema_version");
    let DeploymentClosure::McpServer(mut closure) =
        serde_json::from_value::<DeploymentClosure>(closure_value).unwrap()
    else {
        panic!("fixture closure must be MCP")
    };
    closure.auth_policy = Some(auth_policy);
    fixture.closure_payload = TypedPayload::new(1, &DeploymentClosure::McpServer(closure)).unwrap();
    fixture.deployment = ExactDeploymentRef::new(
        fixture.deployment.deployment_id.clone(),
        fixture.closure_payload.digest.parse().unwrap(),
    )
    .unwrap();
    fixture.intent.mcp_deployment = fixture.deployment.clone();
    fixture
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
            authentication_authority_digest: namespaced_digest("authentication_authority"),
            subject_digest: namespaced_digest("subject"),
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

struct FixedProcessPreparation {
    pkce_binding: ExactSecretBindingRef,
    state: Vec<u8>,
}

#[async_trait]
impl McpOAuthAuthorizationPreparationBroker for FixedProcessPreparation {
    async fn prepare_or_load(
        &self,
        request: &McpOAuthAuthorizationPreparationRequest,
        _now: DateTime<Utc>,
    ) -> Result<PreparedMcpOAuthAuthorization, McpOAuthAuthorizationPreparationError> {
        Ok(PreparedMcpOAuthAuthorization {
            preparation_digest: request.preparation_digest.clone(),
            state: SensitiveOAuthValue::from_decoded(
                self.state.clone(),
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

struct NoCleanupInExchangeFixture;
#[async_trait]
impl McpOAuthPkceSecretCleaner for NoCleanupInExchangeFixture {
    async fn delete_exact(
        &self,
        _: &AuthorizedMcpOAuthPkceCleanup,
    ) -> Result<McpOAuthPkceSecretCleanupDisposition, McpOAuthPkceSecretCleanupError> {
        Err(McpOAuthPkceSecretCleanupError::Rejected)
    }
}

struct ProcessCleanupSecretCleaner {
    call_path: PathBuf,
    exact_secret_path: PathBuf,
    expected_binding: ExactSecretBindingRef,
    stall: bool,
}

#[async_trait]
impl McpOAuthPkceSecretCleaner for ProcessCleanupSecretCleaner {
    async fn delete_exact(
        &self,
        authorization: &AuthorizedMcpOAuthPkceCleanup,
    ) -> Result<McpOAuthPkceSecretCleanupDisposition, McpOAuthPkceSecretCleanupError> {
        authorization
            .secret_binding
            .validate()
            .map_err(|_| McpOAuthPkceSecretCleanupError::Rejected)?;
        if authorization.secret_binding != self.expected_binding {
            return Err(McpOAuthPkceSecretCleanupError::Rejected);
        }
        std::fs::write(&self.call_path, b"called")
            .map_err(|_| McpOAuthPkceSecretCleanupError::TemporarilyUnavailable)?;
        if self.stall {
            std::future::pending::<()>().await;
        }
        match std::fs::remove_file(&self.exact_secret_path) {
            Ok(()) => Ok(McpOAuthPkceSecretCleanupDisposition::Deleted),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(McpOAuthPkceSecretCleanupDisposition::AlreadyAbsent)
            }
            Err(_) => Err(McpOAuthPkceSecretCleanupError::TemporarilyUnavailable),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct OAuthCleanupEgressProcessConfig {
    exact_secret_path: PathBuf,
    expected_binding: ExactSecretBindingRef,
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
                exact_secret_path: config.exact_secret_path,
                expected_binding: config.expected_binding,
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

#[derive(serde::Serialize, serde::Deserialize)]
struct OAuthTokenEndpointProcessConfig {
    listen_address: std::net::SocketAddr,
    server_certificate_der: Vec<u8>,
    server_key_der: Vec<u8>,
    call_path: PathBuf,
    ready_path: PathBuf,
}

async fn run_oauth_token_endpoint_fixture(config: OAuthTokenEndpointProcessConfig) {
    let tls = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![rustls::pki_types::CertificateDer::from(
                    config.server_certificate_der,
                )],
                rustls::pki_types::PrivatePkcs8KeyDer::from(config.server_key_der).into(),
            )
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind(config.listen_address)
        .await
        .unwrap();
    std::fs::write(config.ready_path, b"ready").unwrap();
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let Ok(mut stream) = TlsAcceptor::from(Arc::clone(&tls)).accept(stream).await else {
            continue;
        };
        let mut request = Vec::new();
        let header_end = loop {
            let mut chunk = [0_u8; 1_024];
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&chunk[..read]);
            if let Some(position) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = String::from_utf8(request[..header_end].to_vec()).unwrap();
        assert!(headers.starts_with("POST /oauth/token HTTP/1.1\r\n"));
        let content_length = headers
            .to_ascii_lowercase()
            .lines()
            .find_map(|line| line.strip_prefix("content-length: "))
            .unwrap()
            .parse::<usize>()
            .unwrap();
        while request.len() - header_end < content_length {
            let mut chunk = [0_u8; 1_024];
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&chunk[..read]);
        }
        let body = std::str::from_utf8(&request[header_end..header_end + content_length]).unwrap();
        assert!(body.contains("code=one-time-code"));
        assert!(body.contains("code_verifier=pkce-verifier"));
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.call_path)
            .unwrap()
            .write_all(b"1")
            .unwrap();
        let body = br#"{"access_token":"access-secret","refresh_token":"refresh-secret","token_type":"Bearer","expires_in":600,"scope":"openid tools.call tools.read"}"#;
        let headers = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(headers.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        stream.shutdown().await.unwrap();
    }
}

#[test]
fn oauth_token_endpoint_fixture_process() {
    let Ok(path) = std::env::var(OAUTH_TOKEN_ENDPOINT_CONFIG_ENV) else {
        return;
    };
    let config = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_oauth_token_endpoint_fixture(config));
}

struct ProcessOAuthDns {
    address: std::net::SocketAddr,
}

#[async_trait]
impl EgressDnsResolver for ProcessOAuthDns {
    async fn resolve(
        &self,
        _host: &str,
        port: u16,
    ) -> Result<Vec<std::net::SocketAddr>, DnsResolutionError> {
        Ok(vec![std::net::SocketAddr::new(self.address.ip(), port)])
    }
}

struct ProcessOAuthSecrets;

#[async_trait]
impl SecretMaterialResolver for ProcessOAuthSecrets {
    async fn resolve(
        &self,
        _tenant_id: &ResourceId,
        binding: &ExactSecretBindingRef,
    ) -> Result<ResolvedSecretMaterial, SecretMaterialResolutionError> {
        ResolvedSecretMaterial::new(
            binding.secret_binding_id.clone(),
            binding.provider_id.clone(),
            binding.purpose.clone(),
            binding.binding_generation,
            match &binding.resolution_policy {
                SecretResolutionPolicy::Pinned {
                    opaque_version_identity_digest,
                } => opaque_version_identity_digest.clone(),
                SecretResolutionPolicy::FollowProviderRotation { .. } => sha('f'),
            },
            b"pkce-verifier".to_vec(),
        )
        .map_err(|_| SecretMaterialResolutionError::InvalidEvidence)
    }
}

struct ProcessOAuthVerifier;

#[async_trait]
impl McpOAuthTokenVerifier for ProcessOAuthVerifier {
    async fn verify(
        &self,
        contract: &McpOAuthExchangeContract,
        tokens: &McpOAuthTokenSet,
        now: DateTime<Utc>,
    ) -> Result<VerifiedMcpOAuthToken, McpOAuthTokenVerificationError> {
        if tokens.access_token.expose() != b"access-secret" {
            return Err(McpOAuthTokenVerificationError::Rejected);
        }
        Ok(VerifiedMcpOAuthToken {
            granted_scopes: tokens.granted_scopes.clone(),
            audience_identity_digest: contract.binding.audience_identity_digest.clone(),
            issuer_identity_digest: contract
                .auth_profile
                .issuer
                .endpoint_identity_digest
                .clone(),
            subject_identity_digest: sha('a'),
            verification_evidence_digest: sha('b'),
            expires_at: now + Duration::minutes(5),
            nonce_verified: true,
        })
    }
}

struct ProcessOAuthTokenStore {
    marker_path: PathBuf,
    token_secret_binding: ExactSecretBindingRef,
}

impl ProcessOAuthTokenStore {
    fn stored(
        &self,
        preparation: &McpOAuthTokenPreparation,
        now: DateTime<Utc>,
    ) -> StoredMcpOAuthTokenSecret {
        StoredMcpOAuthTokenSecret {
            schema_version: 1,
            preparation_digest: preparation.preparation_digest.clone(),
            token_secret_binding: self.token_secret_binding.clone(),
            granted_scopes: preparation.requested_scopes.clone(),
            audience_identity_digest: preparation.audience_identity_digest.clone(),
            issuer_identity_digest: preparation.issuer_identity_digest.clone(),
            subject_identity_digest: sha('a'),
            verification_evidence_digest: sha('b'),
            expires_at: now + Duration::minutes(5),
            storage_evidence_digest: sha('d'),
        }
    }
}

#[async_trait]
impl McpOAuthTokenStore for ProcessOAuthTokenStore {
    async fn load_prepared(
        &self,
        preparation: &McpOAuthTokenPreparation,
        now: DateTime<Utc>,
    ) -> Result<Option<StoredMcpOAuthTokenSecret>, McpOAuthTokenStoreError> {
        Ok(self
            .marker_path
            .exists()
            .then(|| self.stored(preparation, now)))
    }

    async fn store_prepared(
        &self,
        preparation: &McpOAuthTokenPreparation,
        _tokens: &McpOAuthTokenSet,
        _verified: &VerifiedMcpOAuthToken,
        now: DateTime<Utc>,
    ) -> Result<StoredMcpOAuthTokenSecret, McpOAuthTokenStoreError> {
        std::fs::write(&self.marker_path, preparation.preparation_digest.as_str())
            .map_err(|_| McpOAuthTokenStoreError::WriteUncertain)?;
        Ok(self.stored(preparation, now))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct OAuthExchangeEgressProcessConfig {
    listen_address: std::net::SocketAddr,
    token_endpoint_address: std::net::SocketAddr,
    ca_pem: String,
    server_certificate_pem: String,
    server_key_pem: String,
    verification_binding: InstalledMcpOAuthVerificationBinding,
    token_secret_binding: ExactSecretBindingRef,
    token_store_marker_path: PathBuf,
    ready_path: PathBuf,
}

async fn run_oauth_exchange_egress_fixture(config: OAuthExchangeEgressProcessConfig) {
    let limits = EgressInternalRpcLimits::new(65_536, 1_048_576).unwrap();
    let catalog =
        InstalledMcpOAuthVerificationCatalog::new(vec![config.verification_binding]).unwrap();
    let broker = ReqwestMcpOAuthCredentialBroker::new(
        Arc::new(ProcessOAuthSecrets),
        Arc::new(ProcessOAuthDns {
            address: config.token_endpoint_address,
        }),
        catalog,
        Arc::new(ProcessOAuthVerifier),
        Arc::new(ProcessOAuthTokenStore {
            marker_path: config.token_store_marker_path,
            token_secret_binding: config.token_secret_binding,
        }),
        McpOAuthEgressLimits::default(),
    )
    .unwrap()
    .allow_loopback_for_protocol_fixture();
    let service = EgressBrokerServiceServer::new(
        EgressBrokerGrpcService::new(
            Arc::new(EmptyModel),
            Arc::new(EmptyHttp),
            Arc::new(EmptyGrpc),
            limits,
        )
        .with_mcp_oauth(Arc::new(broker), Arc::new(NoCleanupInExchangeFixture)),
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
fn oauth_exchange_egress_fixture_process() {
    let Ok(path) = std::env::var(OAUTH_EXCHANGE_EGRESS_CONFIG_ENV) else {
        return;
    };
    let config = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_oauth_exchange_egress_fixture(config));
}

struct ProcessCallbackAuthority {
    repository: PgRepository,
    before_commit_path: PathBuf,
    stall_before_commit: bool,
}

#[async_trait]
impl McpOAuthCallbackAuthority for ProcessCallbackAuthority {
    async fn resolve_exchange_contract(
        &self,
        identity: &AuthenticatedMcpOAuthState,
    ) -> Result<McpOAuthExchangeContract, McpOAuthCallbackAuthorityError> {
        self.repository.resolve_exchange_contract(identity).await
    }

    async fn commit_callback(
        &self,
        command: CompleteMcpOAuthCallback,
    ) -> Result<McpOAuthCallbackCommitOutcome, McpOAuthCallbackAuthorityError> {
        std::fs::write(&self.before_commit_path, b"before-commit")
            .map_err(|_| McpOAuthCallbackAuthorityError::Unavailable)?;
        if self.stall_before_commit {
            std::future::pending::<()>().await;
        }
        self.repository.commit_callback(command).await
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct OAuthExchangeCallbackProcessConfig {
    database_url: String,
    egress_endpoint: String,
    egress_tls_server_name: String,
    egress_ca_pem: String,
    egress_client_certificate_pem: String,
    egress_client_key_pem: String,
    callback_binding_digest: Sha256Digest,
    state_key: Vec<u8>,
    raw_query: String,
    before_commit_path: PathBuf,
    outcome_path: PathBuf,
    ready_path: PathBuf,
    stall_before_commit: bool,
}

async fn run_oauth_exchange_callback_fixture(config: OAuthExchangeCallbackProcessConfig) {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&config.database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let authority = Arc::new(ProcessCallbackAuthority {
        repository: PgRepository::new(pool),
        before_commit_path: config.before_commit_path,
        stall_before_commit: config.stall_before_commit,
    });
    let states = Arc::new(
        AeadMcpOAuthStateCodec::new(
            McpOAuthStateCodecConfig {
                active_key_id: "callback-key-1".to_owned(),
                callback_binding_digest: config.callback_binding_digest.clone(),
                maximum_lifetime_seconds: 600,
                clock_skew_seconds: 30,
            },
            vec![McpOAuthStateKey {
                key_id: "callback-key-1".to_owned(),
                key_material: SensitiveMcpOAuthStateKey::new(config.state_key).unwrap(),
            }],
        )
        .unwrap(),
    );
    let channel = Endpoint::from_shared(config.egress_endpoint)
        .unwrap()
        .connect_timeout(StdDuration::from_secs(2))
        .timeout(StdDuration::from_secs(30))
        .tls_config(
            ClientTlsConfig::new()
                .domain_name(config.egress_tls_server_name)
                .ca_certificate(Certificate::from_pem(config.egress_ca_pem))
                .identity(Identity::from_pem(
                    config.egress_client_certificate_pem,
                    config.egress_client_key_pem,
                )),
        )
        .unwrap()
        .connect()
        .await
        .unwrap();
    let broker = Arc::new(EgressBrokerGrpcClient::new(
        channel,
        EgressInternalRpcLimits::new(65_536, 1_048_576).unwrap(),
    ));
    let ingress = McpOAuthCallbackIngress::new(
        McpOAuthCallbackIngressConfig {
            callback_ingress_generation_id: ResourceId::from_uuid_v7(
                ResourceKind::WorkerProcessGeneration,
                uuid::Uuid::now_v7(),
            )
            .unwrap(),
            callback_binding_digest: config.callback_binding_digest,
            receipt_ttl_seconds: 3_600,
        },
        states,
        Arc::new(UuidMcpOAuthCallbackIdentityFactory),
        authority,
        broker,
    )
    .unwrap();
    std::fs::write(config.ready_path, b"ready").unwrap();
    let outcome = ingress
        .handle_query(config.raw_query.as_bytes(), Utc::now())
        .await;
    let body = match outcome {
        Ok(value) => format!("ok:{:?}", value.disposition),
        Err(error) => format!("error:{}", error.safe_code()),
    };
    std::fs::write(config.outcome_path, body).unwrap();
}

#[test]
fn oauth_exchange_callback_fixture_process() {
    let Ok(path) = std::env::var(OAUTH_EXCHANGE_CALLBACK_CONFIG_ENV) else {
        return;
    };
    let config = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_oauth_exchange_callback_fixture(config));
}

struct OAuthRpcTlsFixture {
    ca: String,
    server_cert: String,
    server_key: String,
    client_cert: String,
    client_key: String,
}

fn oauth_rpc_tls_fixture(client_workload_identity: &str) -> OAuthRpcTlsFixture {
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
        vec![SanType::URI(client_workload_identity.try_into().unwrap())],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    OAuthRpcTlsFixture {
        ca: ca.pem(),
        server_cert,
        server_key,
        client_cert,
        client_key,
    }
}

struct OAuthTokenTlsFixture {
    ca_pem: String,
    server_certificate_der: Vec<u8>,
    server_key_der: Vec<u8>,
}

fn oauth_token_tls_fixture() -> OAuthTokenTlsFixture {
    let mut ca_parameters = CertificateParams::default();
    ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca_parameters, KeyPair::generate().unwrap()).unwrap();
    let mut server_parameters = CertificateParams::default();
    server_parameters.subject_alt_names =
        vec![SanType::DnsName("auth.example.test".try_into().unwrap())];
    server_parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_key = KeyPair::generate().unwrap();
    let server_certificate = server_parameters.signed_by(&server_key, &ca).unwrap();
    OAuthTokenTlsFixture {
        ca_pem: ca.pem(),
        server_certificate_der: server_certificate.der().to_vec(),
        server_key_der: server_key.serialize_der(),
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

fn wait_for_process_file(path: &Path, child: &mut Child, timeout: StdDuration) {
    let started = Instant::now();
    while !path.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            let mut stderr = String::new();
            if let Some(mut stream) = child.stderr.take() {
                let _ = stream.read_to_string(&mut stderr);
            }
            panic!(
                "fixture process exited with {status} before creating {}: {stderr}",
                path.display()
            );
        }
        assert!(
            started.elapsed() < timeout,
            "fixture process did not create {} within {timeout:?}",
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

fn spawn_fixture_process(test_name: &str, env_name: &str, config_path: &Path) -> Child {
    std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(env_name, config_path)
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

struct FixtureChild(Child);
impl std::ops::Deref for FixtureChild {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}
impl std::ops::DerefMut for FixtureChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}
impl Drop for FixtureChild {
    fn drop(&mut self) {
        kill(&mut self.0);
    }
}

fn kill(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test]
async fn phase4_mcp_oauth_start_is_idempotent_secret_free_and_first_winner() {
    let _fixture_namespace = select_fixture_namespace(0xb100).await;
    let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
        panic!(
            "PLATFORM_TEST_DATABASE_URL is unset; real PostgreSQL fixture requires its declared fixture environment"
        );
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
    let replay = service.start(winner_intent.clone(), now).await.unwrap();
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
    let direct_command = insight_platform_mcp_host::BeginMcpOAuthAuthorization {
        audit: winner_intent.audit,
        task_id: winner_intent.task_id,
        authorization_binding_id: winner_intent.authorization_binding_id,
        mcp_deployment: winner_intent.mcp_deployment,
        expected_principal_binding_generation: winner_intent.expected_principal_binding_generation,
        requested_scopes: winner_intent.requested_scopes,
        state_digest: insight_platform_mcp_host::mcp_oauth_state_digest(
            &SensitiveOAuthValue::from_decoded(
                b"oauth-state-canary".to_vec(),
                insight_platform_mcp_host::MAX_MCP_OAUTH_STATE_BYTES,
            )
            .unwrap(),
        )
        .unwrap(),
        nonce_digest: insight_platform_mcp_host::mcp_oauth_nonce_digest(
            &SensitiveMcpOAuthNonce::new(b"n".repeat(43)).unwrap(),
        )
        .unwrap(),
        callback_binding_digest: fixture
            .auth_profile
            .redirect_uri
            .endpoint_identity_digest
            .clone(),
        pkce_secret_binding: fixture.pkce_binding.clone(),
        reauthorization: winner_intent.reauthorization,
        safe_prompt_key: winner_intent.safe_prompt_key,
        deadline: winner_intent.deadline,
    };
    let mut replay_transaction = repository.begin_registry_transaction().await.unwrap();
    assert!(matches!(
        replay_transaction
            .begin_mcp_oauth_authorization(direct_command.clone())
            .await
            .unwrap(),
        insight_platform_contracts::CommandOutcome::Replayed(_)
    ));
    replay_transaction.commit().await.unwrap();
    support::revoke_fixture_principal(
        &pool,
        &repository,
        &fixture.tenant_id,
        &fixture.principal_id,
        PrincipalKind::AgentRunner,
    )
    .await;
    let before = support::fixture_durable_counts(&pool, &fixture.tenant_id).await;
    let mut denied_transaction = repository.begin_registry_transaction().await.unwrap();
    assert!(matches!(
        denied_transaction
            .begin_mcp_oauth_authorization(direct_command)
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    denied_transaction.rollback().await.unwrap();
    assert_eq!(
        before,
        support::fixture_durable_counts(&pool, &fixture.tenant_id).await
    );
}

fn cleanup_manifest(build: &[u8]) -> insight_platform_contracts::WorkerManifest {
    use sha2::Digest;
    insight_platform_contracts::WorkerManifest {
        manifest_version: insight_platform_contracts::WORKER_MANIFEST_VERSION,
        worker_role: "mcp-cleanup-worker".into(),
        work_class: insight_platform_contracts::WorkClass::Recovery,
        adapter_runtime_digest: namespaced_digest("cleanup-adapter"),
        worker_build_digest: format!(
            "sha256:{}",
            sha2::Sha256::digest(build)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        )
        .parse()
        .unwrap(),
        execution_capabilities: insight_platform_contracts::WorkerExecutionCapabilities {
            schema_version: 1,
            capabilities: vec![insight_platform_mcp_host::mcp_oauth_cleanup_execution_capability()],
        },
        protocol_version: insight_platform_contracts::WORKER_PROTOCOL_VERSION,
        max_concurrency: 16,
        critical_control_reserved_slots: 1,
    }
}
async fn seed_pending_cleanup(pool: &PgPool, repository: &PgRepository, fixture: &Fixture) {
    seed(pool, repository, fixture).await;
    let service = McpOAuthAuthorizationStartService::new(
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
    service
        .start(fixture.intent.clone(), Utc::now())
        .await
        .unwrap();
}

async fn seed_terminal_cleanup(pool: &PgPool, repository: &PgRepository, fixture: &Fixture) {
    seed_pending_cleanup(pool, repository, fixture).await;
    complete_cleanup_task(
        repository,
        fixture,
        insight_platform_mcp_host::McpOAuthCallbackResolution::Declined {
            safe_reason_code: "user_declined".into(),
            evidence_digest: namespaced_digest("decline-evidence"),
        },
    )
    .await;
}

async fn complete_cleanup_task(
    repository: &PgRepository,
    fixture: &Fixture,
    resolution: insight_platform_mcp_host::McpOAuthCallbackResolution,
) {
    let exchange = repository
        .resolve_exchange_contract(&AuthenticatedMcpOAuthState {
            tenant_id: fixture.tenant_id.clone(),
            task_id: fixture.intent.task_id.clone(),
        })
        .await
        .unwrap();
    let mut callback = repository.begin_registry_transaction().await.unwrap();
    callback
        .complete_mcp_oauth_callback(CompleteMcpOAuthCallback {
            audit: insight_platform_mcp_host::McpOAuthCallbackAudit {
                trace: fixture.intent.audit.trace,
                tenant_id: fixture.tenant_id.clone(),
                callback_ingress_generation_id: id(ResourceKind::WorkerProcessGeneration, 0xa2),
                receipt_id: id(ResourceKind::Receipt, 0xa3),
                event_id: id(ResourceKind::Event, 0xa4),
                outbox_id: id(ResourceKind::OutboxEvent, 0xa5),
                idempotency_key_digest: namespaced_digest("cleanup-callback-key"),
                request_digest: namespaced_digest("cleanup-callback-request"),
                callback_binding_digest: exchange.binding.callback_binding_digest.clone(),
                receipt_expires_at: Utc::now() + Duration::minutes(30),
            },
            task_id: fixture.intent.task_id.clone(),
            authorization_binding_id: fixture.intent.authorization_binding_id.clone(),
            expected_task_generation: exchange.task_generation,
            expected_task_version: exchange.task_version,
            state_digest: exchange.binding.state_digest.clone(),
            resolution,
        })
        .await
        .unwrap();
    callback.commit().await.unwrap();
}
async fn claim_cleanup_for(
    repository: &PgRepository,
    task: &ResourceId,
    worker: ResourceId,
    build: &[u8],
) -> insight_platform_mcp_host::ClaimedMcpOAuthPkceCleanup {
    for _ in 0..(usize::from(insight_platform_contracts::SCHEDULER_PARTITION_COUNT) * 8) {
        let claims = repository
            .claim_due_mcp_oauth_pkce_cleanups(ClaimDueMcpOAuthPkceCleanups {
                worker_manifest: cleanup_manifest(build),
                claim_owner: worker.clone(),
                lease_token_digests: vec![namespaced_digest(&uuid::Uuid::new_v4().to_string())],
                maximum_claims: 1,
                lease_milliseconds: 30_000,
            })
            .await
            .unwrap();
        if let Some(claim) = claims
            .into_iter()
            .find(|claim| &claim.request.task_id == task)
        {
            return claim;
        }
        tokio::task::yield_now().await;
    }
    panic!("finite partition rounds did not reach cleanup Job")
}
#[tokio::test]
async fn phase4_mcp_oauth_cleanup_job_is_reclaimable_and_exactly_fenced() {
    let _fixture_namespace = select_fixture_namespace(0xb101).await;
    let url = std::env::var("PLATFORM_TEST_DATABASE_URL")
        .expect("PLATFORM_TEST_DATABASE_URL is required");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let fixture = fixture(Utc::now());
    seed_terminal_cleanup(&pool, &repository, &fixture).await;
    let task = &fixture.intent.task_id;
    let first = claim_cleanup_for(
        &repository,
        task,
        id(ResourceKind::WorkerProcessGeneration, 0xb1),
        b"cleanup-fixture-binary-a",
    )
    .await;
    assert_eq!(first.request.cause, McpOAuthPkceCleanupCause::Declined);
    assert!(!repository
        .settle_mcp_oauth_pkce_cleanup(&first, McpOAuthPkceCleanupSettlement::Stale)
        .await
        .unwrap());
    let proof:Option<String>=sqlx::query_scalar("SELECT payload->>'deletion_proof' FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2").bind(fixture.tenant_id.to_string()).bind(first.request.cleanup_job_id.to_string()).fetch_one(&pool).await.unwrap();
    assert!(proof.is_none());
    sqlx::query("UPDATE insight_platform.jobs SET lease_expires_at=clock_timestamp()-interval '1 millisecond',heartbeat_at=clock_timestamp()-interval '2 milliseconds' WHERE tenant_id=$1 AND job_id=$2").bind(fixture.tenant_id.to_string()).bind(first.request.cleanup_job_id.to_string()).execute(&pool).await.unwrap();
    let second = claim_cleanup_for(
        &repository,
        task,
        id(ResourceKind::WorkerProcessGeneration, 0xb2),
        b"cleanup-fixture-binary-b",
    )
    .await;
    assert!(second.request.fence.lease_generation > first.request.fence.lease_generation);
    assert_eq!(
        second.request.deletion_effect_identity,
        first.request.deletion_effect_identity
    );
    assert!(!repository
        .settle_mcp_oauth_pkce_cleanup(
            &first,
            McpOAuthPkceCleanupSettlement::Completed {
                proof: McpOAuthPkceSecretCleanupDisposition::Deleted
            }
        )
        .await
        .unwrap());
    assert!(repository
        .settle_mcp_oauth_pkce_cleanup(
            &second,
            McpOAuthPkceCleanupSettlement::Completed {
                proof: McpOAuthPkceSecretCleanupDisposition::AlreadyAbsent
            }
        )
        .await
        .unwrap());
    let row=sqlx::query("SELECT job.state,job.payload->>'deletion_proof' AS proof,job.attempt_build_digest,task.state AS task_state,task.current_cleanup_job_id FROM insight_platform.jobs job JOIN insight_platform.tasks task ON task.tenant_id=job.tenant_id AND task.task_id=job.owner_id WHERE job.tenant_id=$1 AND job.job_id=$2").bind(fixture.tenant_id.to_string()).bind(second.request.cleanup_job_id.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(row.get::<String, _>("state"), "succeeded");
    assert_eq!(row.get::<String, _>("proof"), "already_absent");
    assert_eq!(row.get::<String, _>("task_state"), "declined");
    assert_eq!(
        row.get::<String, _>("attempt_build_digest"),
        cleanup_manifest(b"cleanup-fixture-binary-b")
            .worker_build_digest
            .to_string()
    );
}

#[tokio::test]
#[ignore = "requires the built cleanup worker and real local PostgreSQL/mTLS process fixture"]
async fn phase4_mcp_oauth_cleanup_process_recovers_egress_and_worker_kill() {
    let _fixture_namespace = select_fixture_namespace(0xb102).await;
    let (Ok(database_url), Ok(cleanup_binary)) = (
        std::env::var("PLATFORM_TEST_DATABASE_URL"),
        std::env::var("PLATFORM_MCP_CLEANUP_WORKER_BIN"),
    ) else {
        panic!(
            "PLATFORM_TEST_DATABASE_URL or PLATFORM_MCP_CLEANUP_WORKER_BIN is unset; process L3 requires its declared fixture environment"
        );
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
    let mut fixture = fixture(now);
    fixture.intent.deadline = now + Duration::seconds(2);
    seed_pending_cleanup(&pool, &repository, &fixture).await;
    let pending:(String,Option<String>)=sqlx::query_as("SELECT state,current_cleanup_job_id FROM insight_platform.tasks WHERE tenant_id=$1 AND task_id=$2").bind(fixture.tenant_id.to_string()).bind(fixture.intent.task_id.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(pending, ("pending".into(), None));

    let temporary = std::env::temp_dir().join(format!(
        "platform-oauth-cleanup-l3-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir(&temporary).unwrap();
    // The exact-generation provider fixture is a durable external file shared by both
    // egress processes. A stalled call must not remove it; the replacement must delete it.
    let exact_secret_path = temporary.join("exact-pkce-generation");
    std::fs::write(&exact_secret_path, b"test-only-secret-generation").unwrap();
    let tls =
        oauth_rpc_tls_fixture(insight_platform_egress_rpc::MCP_CLEANUP_WORKER_WORKLOAD_IDENTITY);
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
        "worker_manifest": cleanup_manifest(&std::fs::read(&cleanup_binary).unwrap()),
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
        "lease_milliseconds": 2000,
        "retry_base_milliseconds": 20,
        "retry_maximum_milliseconds": 1_000,
    });
    let cleanup_config_digest = write_json(&cleanup_config_path, &cleanup_config);
    let first_call = temporary.join("first-call");
    let first_ready = temporary.join("first-ready");
    let first_egress_config_path = temporary.join("egress-first.json");
    let first_egress_config = serde_json::to_value(OAuthCleanupEgressProcessConfig {
        exact_secret_path: exact_secret_path.clone(),
        expected_binding: fixture.pkce_binding.clone(),
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
    let mut first_egress = FixtureChild(spawn_oauth_cleanup_egress(&first_egress_config_path));
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
    let mut first_worker = FixtureChild(spawn_worker());
    wait_for_file(&first_call, StdDuration::from_secs(10));
    let (state,cleanup_job_id):(String,String)=sqlx::query_as("SELECT state,current_cleanup_job_id FROM insight_platform.tasks WHERE tenant_id=$1 AND task_id=$2").bind(fixture.tenant_id.to_string()).bind(fixture.intent.task_id.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(state, "expired");
    assert!(exact_secret_path.exists());
    let callback_receipts:i64=sqlx::query_scalar("SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1 AND operation LIKE 'mcp.oauth.callback%'").bind(fixture.tenant_id.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(
        callback_receipts, 0,
        "system expiry requires no callback or manual expiry command"
    );
    let first_claim_epoch: i64 = sqlx::query_scalar(
        "SELECT lease_epoch FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(&cleanup_job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(first_claim_epoch, 1);
    kill(&mut first_egress);
    kill(&mut first_worker);
    tokio::time::sleep(StdDuration::from_millis(2200)).await;

    let second_call = temporary.join("second-call");
    let second_ready = temporary.join("second-ready");
    let second_egress_config_path = temporary.join("egress-second.json");
    let second_egress_config = serde_json::to_value(OAuthCleanupEgressProcessConfig {
        exact_secret_path: exact_secret_path.clone(),
        expected_binding: fixture.pkce_binding.clone(),
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
    let mut second_egress = FixtureChild(spawn_oauth_cleanup_egress(&second_egress_config_path));
    wait_for_file(&second_ready, StdDuration::from_secs(5));
    let mut second_worker = FixtureChild(spawn_worker());
    wait_for_file(&second_call, StdDuration::from_secs(10));
    let started_wait = Instant::now();
    let terminal = loop {
        let row = sqlx::query(
            "SELECT state, lease_epoch AS claim_epoch, worker_id AS claim_owner, lease_expires_at AS claim_expires_at FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(fixture.tenant_id.to_string())
        .bind(&cleanup_job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        if row.get::<String, _>("state") == "succeeded" {
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
    assert!(
        !exact_secret_path.exists(),
        "successful exact-generation cleanup must delete the external fixture secret"
    );
    let proof:serde_json::Value=sqlx::query_scalar("SELECT payload->'deletion_proof' FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2").bind(fixture.tenant_id.to_string()).bind(&cleanup_job_id).fetch_one(&pool).await.unwrap();
    assert_eq!(
        serde_json::from_value::<McpOAuthPkceSecretCleanupDisposition>(proof).unwrap(),
        McpOAuthPkceSecretCleanupDisposition::Deleted,
    );
    std::fs::remove_dir_all(temporary).unwrap();
}

fn oauth_verification_binding(
    fixture: &Fixture,
    token_endpoint_trust_roots_pem: String,
) -> InstalledMcpOAuthVerificationBinding {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
    let jwks = serde_json::json!({
        "keys": [{
            "alg": "EdDSA",
            "crv": "Ed25519",
            "kid": "key-1",
            "kty": "OKP",
            "use": "sig",
            "x": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
                key_pair.public_key().as_ref()
            ),
        }]
    });
    let auth_policy = fixture
        .policy_versions
        .iter()
        .find(|(_, document)| {
            matches!(
                document,
                ResourceDocument::Policy(policy) if policy.policy_kind == PolicyKind::McpAuth
            )
        })
        .map(|(revision, _)| revision.clone())
        .unwrap();
    InstalledMcpOAuthVerificationBinding {
        schema_version: 1,
        auth_policy,
        trust_policy: fixture.retention_policy.clone(),
        auth_profile: fixture.auth_profile.clone(),
        token_endpoint_trust_roots_pem,
        algorithms: vec![InstalledMcpOAuthJwtAlgorithm::EdDsa],
        jwks_digest: canonical_digest(&jwks).unwrap().parse().unwrap(),
        jwks,
    }
}

#[tokio::test]
#[ignore = "requires the PostgreSQL and mTLS callback/egress process fixture"]
async fn phase4_mcp_oauth_callback_and_egress_recover_after_token_store_before_commit() {
    let _fixture_namespace = select_fixture_namespace(0xb103).await;
    let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
        panic!(
            "PLATFORM_TEST_DATABASE_URL is unset; OAuth exchange process L3 requires its declared fixture environment"
        );
    };
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let temporary = std::env::temp_dir().join(format!(
        "platform-oauth-exchange-l3-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir(&temporary).unwrap();
    let token_address = available_address();
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let fixture = fixture_with_token_endpoint_port(now, token_address.port());
    seed(&pool, &repository, &fixture).await;
    let token_binding = ExactSecretBindingRef::build(
        id(ResourceKind::SecretBinding, 0xd0),
        1,
        fixture.auth_profile.token_secret_provider_id.clone(),
        "mcp.oauth.token".parse().unwrap(),
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: sha('c'),
        },
    )
    .unwrap();
    repository
        .create_secret_binding(NewSecretBinding {
            tenant_id: fixture.tenant_id.clone(),
            secret_binding_id: token_binding.secret_binding_id.clone(),
            purpose: token_binding.purpose.clone(),
            provider_id: token_binding.provider_id.clone(),
            opaque_reference_ciphertext: vec![1, 2, 3],
            key_id: "fixture-key".to_owned(),
            reference_digest: sha('d'),
            payload: SecretBindingPayload {
                provider_id: token_binding.provider_id.clone(),
                resolution_policy: token_binding.resolution_policy.clone(),
            },
        })
        .await
        .unwrap();

    let state_key = vec![0x51; 32];
    let callback_binding_digest = fixture
        .auth_profile
        .redirect_uri
        .endpoint_identity_digest
        .clone();
    let state_codec = AeadMcpOAuthStateCodec::new(
        McpOAuthStateCodecConfig {
            active_key_id: "callback-key-1".to_owned(),
            callback_binding_digest: callback_binding_digest.clone(),
            maximum_lifetime_seconds: 600,
            clock_skew_seconds: 30,
        },
        vec![McpOAuthStateKey {
            key_id: "callback-key-1".to_owned(),
            key_material: SensitiveMcpOAuthStateKey::new(state_key.clone()).unwrap(),
        }],
    )
    .unwrap();
    let state = state_codec
        .issue_state(
            &AuthenticatedMcpOAuthState {
                tenant_id: fixture.tenant_id.clone(),
                task_id: fixture.intent.task_id.clone(),
            },
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
    let state_bytes = state.as_bytes().to_vec();
    let start = McpOAuthAuthorizationStartService::new(
        McpOAuthAuthorizationStartConfig {
            callback_binding_digest: callback_binding_digest.clone(),
        },
        Arc::new(repository.clone()),
        Arc::new(FixedProcessPreparation {
            pkce_binding: fixture.pkce_binding.clone(),
            state: state_bytes.clone(),
        }),
    );
    assert_eq!(
        start
            .start(fixture.intent.clone(), now)
            .await
            .unwrap()
            .disposition,
        McpOAuthAuthorizationStartCommitDisposition::Applied
    );

    let token_tls = oauth_token_tls_fixture();
    let token_calls = temporary.join("token-calls");
    let token_ready = temporary.join("token-ready");
    let token_config_path = temporary.join("token.json");
    write_json(
        &token_config_path,
        &serde_json::to_value(OAuthTokenEndpointProcessConfig {
            listen_address: token_address,
            server_certificate_der: token_tls.server_certificate_der,
            server_key_der: token_tls.server_key_der,
            call_path: token_calls.clone(),
            ready_path: token_ready.clone(),
        })
        .unwrap(),
    );
    let mut token_process = spawn_fixture_process(
        "oauth_token_endpoint_fixture_process",
        OAUTH_TOKEN_ENDPOINT_CONFIG_ENV,
        &token_config_path,
    );
    wait_for_process_file(&token_ready, &mut token_process, StdDuration::from_secs(5));

    let rpc_tls =
        oauth_rpc_tls_fixture(insight_platform_egress_rpc::MCP_CALLBACK_WORKLOAD_IDENTITY);
    let egress_address = available_address();
    let token_store_marker = temporary.join("token-stored");
    let verification_binding = oauth_verification_binding(&fixture, token_tls.ca_pem.clone());
    let write_egress_config = |path: &Path, ready_path: &Path| {
        write_json(
            path,
            &serde_json::to_value(OAuthExchangeEgressProcessConfig {
                listen_address: egress_address,
                token_endpoint_address: token_address,
                ca_pem: rpc_tls.ca.clone(),
                server_certificate_pem: rpc_tls.server_cert.clone(),
                server_key_pem: rpc_tls.server_key.clone(),
                verification_binding: verification_binding.clone(),
                token_secret_binding: token_binding.clone(),
                token_store_marker_path: token_store_marker.clone(),
                ready_path: ready_path.to_path_buf(),
            })
            .unwrap(),
        );
    };
    let raw_query = format!(
        "state={}&code=one-time-code",
        String::from_utf8(state_bytes).unwrap()
    );
    let write_callback_config =
        |path: &Path, ready_path: &Path, before_commit: &Path, outcome: &Path, stall| {
            write_json(
                path,
                &serde_json::to_value(OAuthExchangeCallbackProcessConfig {
                    database_url: database_url.clone(),
                    egress_endpoint: format!("https://{egress_address}/"),
                    egress_tls_server_name: "egress.test".to_owned(),
                    egress_ca_pem: rpc_tls.ca.clone(),
                    egress_client_certificate_pem: rpc_tls.client_cert.clone(),
                    egress_client_key_pem: rpc_tls.client_key.clone(),
                    callback_binding_digest: callback_binding_digest.clone(),
                    state_key: state_key.clone(),
                    raw_query: raw_query.clone(),
                    before_commit_path: before_commit.to_path_buf(),
                    outcome_path: outcome.to_path_buf(),
                    ready_path: ready_path.to_path_buf(),
                    stall_before_commit: stall,
                })
                .unwrap(),
            );
        };

    let first_egress_ready = temporary.join("egress-first-ready");
    let first_egress_config = temporary.join("egress-first.json");
    write_egress_config(&first_egress_config, &first_egress_ready);
    let mut first_egress = spawn_fixture_process(
        "oauth_exchange_egress_fixture_process",
        OAUTH_EXCHANGE_EGRESS_CONFIG_ENV,
        &first_egress_config,
    );
    wait_for_process_file(
        &first_egress_ready,
        &mut first_egress,
        StdDuration::from_secs(5),
    );
    let first_callback_ready = temporary.join("callback-first-ready");
    let first_before_commit = temporary.join("callback-first-before-commit");
    let first_outcome = temporary.join("callback-first-outcome");
    let first_callback_config = temporary.join("callback-first.json");
    write_callback_config(
        &first_callback_config,
        &first_callback_ready,
        &first_before_commit,
        &first_outcome,
        true,
    );
    let mut first_callback = spawn_fixture_process(
        "oauth_exchange_callback_fixture_process",
        OAUTH_EXCHANGE_CALLBACK_CONFIG_ENV,
        &first_callback_config,
    );
    wait_for_process_file(
        &first_callback_ready,
        &mut first_callback,
        StdDuration::from_secs(5),
    );
    // The callback-to-egress RPC contract permits a 30 second request. Process supervision needs
    // a separate bounded scheduler margin beyond that protocol deadline; otherwise a loaded
    // runner can race the RPC timeout at exactly 30 seconds. An early child exit is surfaced with
    // its stderr instead of being misreported as an absent marker.
    wait_for_process_file(
        &token_store_marker,
        &mut first_callback,
        StdDuration::from_secs(45),
    );
    wait_for_process_file(
        &first_before_commit,
        &mut first_callback,
        StdDuration::from_secs(45),
    );
    kill(&mut first_callback);
    kill(&mut first_egress);

    let pending: String = sqlx::query_scalar(
        "SELECT state FROM insight_platform.tasks WHERE tenant_id = $1 AND task_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.intent.task_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pending, "pending");
    assert_eq!(std::fs::read(&token_calls).unwrap(), b"1");

    let second_egress_ready = temporary.join("egress-second-ready");
    let second_egress_config = temporary.join("egress-second.json");
    write_egress_config(&second_egress_config, &second_egress_ready);
    let mut second_egress = spawn_fixture_process(
        "oauth_exchange_egress_fixture_process",
        OAUTH_EXCHANGE_EGRESS_CONFIG_ENV,
        &second_egress_config,
    );
    wait_for_process_file(
        &second_egress_ready,
        &mut second_egress,
        StdDuration::from_secs(5),
    );
    let second_callback_ready = temporary.join("callback-second-ready");
    let second_before_commit = temporary.join("callback-second-before-commit");
    let second_outcome = temporary.join("callback-second-outcome");
    let second_callback_config = temporary.join("callback-second.json");
    write_callback_config(
        &second_callback_config,
        &second_callback_ready,
        &second_before_commit,
        &second_outcome,
        false,
    );
    let mut second_callback = spawn_fixture_process(
        "oauth_exchange_callback_fixture_process",
        OAUTH_EXCHANGE_CALLBACK_CONFIG_ENV,
        &second_callback_config,
    );
    wait_for_process_file(
        &second_outcome,
        &mut second_callback,
        StdDuration::from_secs(45),
    );
    assert!(std::fs::read_to_string(&second_outcome)
        .unwrap()
        .starts_with("ok:Authorized"));
    let _ = second_callback.wait();
    kill(&mut second_egress);
    kill(&mut token_process);

    let task_state: String = sqlx::query_scalar(
        "SELECT state FROM insight_platform.tasks WHERE tenant_id = $1 AND task_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.intent.task_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND operation = 'mcp.oauth.callback'",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    let completion_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_type = 'mcp.oauth_authorization_completed'",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(task_state, "responded");
    assert_eq!((receipt_count, completion_count), (1, 1));
    assert_eq!(std::fs::read(&token_calls).unwrap(), b"1");
    std::fs::remove_dir_all(temporary).unwrap();
}

#[tokio::test]
async fn phase4_mcp_oauth_cleanup_recovery_is_explicit_and_replays_original_job() {
    use insight_platform_mcp_host::RecoverMcpOAuthPkceCleanup;
    let _namespace = select_fixture_namespace(0xb104).await;
    let url = std::env::var("PLATFORM_TEST_DATABASE_URL")
        .expect("PLATFORM_TEST_DATABASE_URL is required");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let fixture = fixture(Utc::now());
    seed_terminal_cleanup(&pool, &repository, &fixture).await;
    let first = claim_cleanup_for(
        &repository,
        &fixture.intent.task_id,
        id(ResourceKind::WorkerProcessGeneration, 0xe0),
        b"recovery-fixture-worker",
    )
    .await;
    assert!(repository
        .settle_mcp_oauth_pkce_cleanup(
            &first,
            McpOAuthPkceCleanupSettlement::DeadLetter {
                failure_code: "mcp_oauth_pkce_cleanup_outcome_uncertain"
            }
        )
        .await
        .unwrap());
    let task_version: i64 = sqlx::query_scalar(
        "SELECT version FROM insight_platform.tasks WHERE tenant_id=$1 AND task_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.intent.task_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut command = RecoverMcpOAuthPkceCleanup {
        audit: fixture.intent.audit.clone(),
        task_id: fixture.intent.task_id.clone(),
        expected_task_generation: first.request.task_generation,
        expected_task_version: task_version as u64,
        previous_job_id: first.request.cleanup_job_id.clone(),
        new_job_id: id(ResourceKind::Job, 0xe1),
        attempt_limit: 2,
        recovery_evidence_digest: namespaced_digest("reviewed-narrow-recovery"),
    };
    command.audit.receipt_id = id(ResourceKind::Receipt, 0xe2);
    command.audit.event_id = id(ResourceKind::Event, 0xe3);
    command.audit.outbox_id = id(ResourceKind::OutboxEvent, 0xe4);
    command.audit.idempotency_key_digest = namespaced_digest("recovery-key");
    command.audit.request_digest = command.request_digest().unwrap();
    assert!(matches!(
        repository
            .recover_mcp_oauth_pkce_cleanup(command.clone())
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    let recovery_principal = id(ResourceKind::Principal, 0xe5);
    repository
        .create_principal(NewPrincipal {
            principal_id: recovery_principal.clone(),
            authentication_authority_digest: namespaced_digest("recovery-authority"),
            subject_digest: namespaced_digest("recovery-operator"),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: fixture.tenant_id.clone(),
            principal_id: recovery_principal.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![Permission::McpCleanupRecover]).unwrap(),
            },
        })
        .await
        .unwrap();
    command.audit.principal_id = recovery_principal;
    let recovered = match repository
        .recover_mcp_oauth_pkce_cleanup(command.clone())
        .await
        .unwrap()
    {
        insight_platform_contracts::CommandOutcome::Applied(job) => job,
        _ => panic!("first recovery must apply"),
    };
    assert_eq!(recovered.attempt_limit, 2);
    assert_eq!(recovered.attempt_no, 0);
    let mut replay = command.clone();
    replay.new_job_id = id(ResourceKind::Job, 0xe6);
    replay.audit.receipt_id = id(ResourceKind::Receipt, 0xe7);
    replay.audit.event_id = id(ResourceKind::Event, 0xe8);
    replay.audit.outbox_id = id(ResourceKind::OutboxEvent, 0xe9);
    assert_eq!(
        replay.request_digest().unwrap(),
        command.audit.request_digest
    );
    let replayed = match repository
        .recover_mcp_oauth_pkce_cleanup(replay)
        .await
        .unwrap()
    {
        insight_platform_contracts::CommandOutcome::Replayed(job) => job,
        _ => panic!("retry must reuse original recovery Job"),
    };
    assert_eq!(replayed.job_id, recovered.job_id);
    assert!(!repository
        .settle_mcp_oauth_pkce_cleanup(
            &first,
            McpOAuthPkceCleanupSettlement::Completed {
                proof: McpOAuthPkceSecretCleanupDisposition::Deleted
            }
        )
        .await
        .unwrap());
    // Revoking the original initiator cannot prevent restricted exact cleanup.
    sqlx::query("UPDATE insight_platform.tenant_principals SET state='revoked',generation=generation+1,version=version+1 WHERE tenant_id=$1 AND principal_id=$2").bind(fixture.tenant_id.to_string()).bind(fixture.principal_id.to_string()).execute(&pool).await.unwrap();
    let replacement = claim_cleanup_for(
        &repository,
        &fixture.intent.task_id,
        id(ResourceKind::WorkerProcessGeneration, 0xea),
        b"new-recovery-build",
    )
    .await;
    assert_eq!(
        replacement.request.deletion_effect_identity,
        first.request.deletion_effect_identity
    );
    assert_ne!(
        replacement.request.cleanup_job_id,
        first.request.cleanup_job_id
    );
    repository
        .authorize_cleanup(&replacement.request)
        .await
        .unwrap();
    assert!(repository
        .settle_mcp_oauth_pkce_cleanup(
            &replacement,
            McpOAuthPkceCleanupSettlement::Completed {
                proof: McpOAuthPkceSecretCleanupDisposition::AlreadyAbsent
            }
        )
        .await
        .unwrap());
    let state:(String,i64,String)=sqlx::query_as("SELECT state,generation,current_cleanup_job_id FROM insight_platform.tasks WHERE tenant_id=$1 AND task_id=$2").bind(fixture.tenant_id.to_string()).bind(fixture.intent.task_id.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(
        state,
        (
            "declined".into(),
            first.request.task_generation as i64,
            recovered.job_id
        )
    );
    let old:(String,Option<String>)=sqlx::query_as("SELECT state,payload->>'deletion_proof' FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2").bind(fixture.tenant_id.to_string()).bind(first.request.cleanup_job_id.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(old, ("failed".into(), None));
    assert_completed_cleanup_chain_retirement(&pool, &fixture).await;
}

#[tokio::test]
async fn phase4_mcp_authorization_receipts_require_current_permissions() {
    use insight_platform_contracts::{
        CommandOutcome, McpAuthorizationPrincipalKind, McpAuthorizationState,
    };
    use insight_platform_mcp_host::{
        CreateMcpAuthorizationBinding, NewMcpAuthorizationBinding,
        TransitionMcpAuthorizationBinding,
    };
    let _namespace = select_fixture_namespace(0xb106).await;
    let database_url = std::env::var("PLATFORM_TEST_DATABASE_URL")
        .expect("real PostgreSQL fixture requires PLATFORM_TEST_DATABASE_URL");
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
    let token = ExactSecretBindingRef::build(
        id(ResourceKind::SecretBinding, 0xc1),
        1,
        fixture.auth_profile.token_secret_provider_id.clone(),
        "mcp.oauth.token".parse().unwrap(),
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: namespaced_digest("replay-token-version"),
        },
    )
    .unwrap();
    repository
        .create_secret_binding(NewSecretBinding {
            tenant_id: fixture.tenant_id.clone(),
            secret_binding_id: token.secret_binding_id.clone(),
            purpose: token.purpose.clone(),
            provider_id: token.provider_id.clone(),
            opaque_reference_ciphertext: vec![1, 2, 3],
            key_id: "fixture-key".into(),
            reference_digest: namespaced_digest("replay-token-reference"),
            payload: SecretBindingPayload {
                provider_id: token.provider_id.clone(),
                resolution_policy: token.resolution_policy.clone(),
            },
        })
        .await
        .unwrap();
    let mut create_audit = fixture.intent.audit.clone();
    create_audit.receipt_id = id(ResourceKind::Receipt, 0xc2);
    create_audit.event_id = id(ResourceKind::Event, 0xc2);
    create_audit.outbox_id = id(ResourceKind::OutboxEvent, 0xc2);
    let create = CreateMcpAuthorizationBinding {
        audit: create_audit,
        input: NewMcpAuthorizationBinding {
            tenant_id: fixture.tenant_id.clone(),
            authorization_binding_id: id(ResourceKind::McpAuthorizationBinding, 0xc3),
            mcp_deployment: fixture.deployment.clone(),
            principal_kind: McpAuthorizationPrincipalKind::PerUser,
            principal_id: fixture.principal_id.clone(),
            principal_identity_kind: PrincipalKind::AgentRunner,
            principal_binding_generation: 1,
            audience_identity_digest: fixture
                .auth_profile
                .resource_indicator
                .endpoint_identity_digest
                .clone(),
            granted_scopes: fixture.intent.requested_scopes.clone(),
            token_secret_binding: token,
            expires_at: now + Duration::hours(1),
        },
    };
    let mut tx = repository.begin_registry_transaction().await.unwrap();
    let created = tx
        .create_mcp_authorization_binding(create.clone())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(matches!(created, CommandOutcome::Applied(_)));
    let mut transition_audit = fixture.intent.audit.clone();
    transition_audit.receipt_id = id(ResourceKind::Receipt, 0xc4);
    transition_audit.event_id = id(ResourceKind::Event, 0xc4);
    transition_audit.outbox_id = id(ResourceKind::OutboxEvent, 0xc4);
    let transition = TransitionMcpAuthorizationBinding {
        audit: transition_audit,
        authorization_binding_id: create.input.authorization_binding_id.clone(),
        expected_version: 1,
        target: McpAuthorizationState::ReauthRequired,
    };
    let mut tx = repository.begin_registry_transaction().await.unwrap();
    assert!(matches!(
        tx.transition_mcp_authorization_binding(transition.clone())
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    tx.commit().await.unwrap();
    let mut tx = repository.begin_registry_transaction().await.unwrap();
    assert!(matches!(
        tx.create_mcp_authorization_binding(create.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    assert!(matches!(
        tx.transition_mcp_authorization_binding(transition.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    tx.commit().await.unwrap();
    support::revoke_fixture_principal(
        &pool,
        &repository,
        &fixture.tenant_id,
        &fixture.principal_id,
        PrincipalKind::AgentRunner,
    )
    .await;
    let before = support::fixture_durable_counts(&pool, &fixture.tenant_id).await;
    let mut tx = repository.begin_registry_transaction().await.unwrap();
    assert!(matches!(
        tx.create_mcp_authorization_binding(create).await,
        Err(RepositoryError::PermissionDenied)
    ));
    tx.rollback().await.unwrap();
    let mut tx = repository.begin_registry_transaction().await.unwrap();
    assert!(matches!(
        tx.transition_mcp_authorization_binding(transition).await,
        Err(RepositoryError::PermissionDenied)
    ));
    tx.rollback().await.unwrap();
    assert_eq!(
        before,
        support::fixture_durable_counts(&pool, &fixture.tenant_id).await
    );
}

#[tokio::test]
async fn phase4_mcp_oauth_cleanup_recovery_budget_is_chain_owned_and_concurrent() {
    use insight_platform_contracts::CommandOutcome;
    use insight_platform_mcp_host::{RecoverMcpOAuthPkceCleanup, MCP_OAUTH_CLEANUP_RECOVERY_LIMIT};
    let _namespace = select_fixture_namespace(0xb109).await;
    let url = std::env::var("PLATFORM_TEST_DATABASE_URL")
        .expect("PLATFORM_TEST_DATABASE_URL is required");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let fixture = fixture(Utc::now());
    seed_terminal_cleanup(&pool, &repository, &fixture).await;
    let operator = id(ResourceKind::Principal, 0xf0);
    repository
        .create_principal(NewPrincipal {
            principal_id: operator.clone(),
            authentication_authority_digest: namespaced_digest("bounded-recovery-authority"),
            subject_digest: namespaced_digest("bounded-recovery-operator"),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: fixture.tenant_id.clone(),
            principal_id: operator.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![Permission::McpCleanupRecover]).unwrap(),
            },
        })
        .await
        .unwrap();
    let mut last_command = None;
    for recovery in 1..=MCP_OAUTH_CLEANUP_RECOVERY_LIMIT + 1 {
        let claim = claim_cleanup_for(
            &repository,
            &fixture.intent.task_id,
            id(
                ResourceKind::WorkerProcessGeneration,
                0xf100 + recovery as u16,
            ),
            b"bounded-recovery-fixture",
        )
        .await;
        assert!(repository
            .settle_mcp_oauth_pkce_cleanup(
                &claim,
                McpOAuthPkceCleanupSettlement::DeadLetter {
                    failure_code: "mcp_oauth_pkce_cleanup_outcome_uncertain",
                }
            )
            .await
            .unwrap());
        let task_version: i64 = sqlx::query_scalar(
            "SELECT version FROM insight_platform.tasks WHERE tenant_id=$1 AND task_id=$2",
        )
        .bind(fixture.tenant_id.to_string())
        .bind(fixture.intent.task_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        let mut command = RecoverMcpOAuthPkceCleanup {
            audit: fixture.intent.audit.clone(),
            task_id: fixture.intent.task_id.clone(),
            expected_task_generation: claim.request.task_generation,
            expected_task_version: task_version as u64,
            previous_job_id: claim.request.cleanup_job_id.clone(),
            new_job_id: id(ResourceKind::Job, 0xf200 + recovery as u16),
            attempt_limit: 1,
            recovery_evidence_digest: namespaced_digest(&format!("recovery-evidence-{recovery}")),
        };
        command.audit.principal_id = operator.clone();
        command.audit.receipt_id = id(ResourceKind::Receipt, 0xf300 + recovery as u16);
        command.audit.event_id = id(ResourceKind::Event, 0xf400 + recovery as u16);
        command.audit.outbox_id = id(ResourceKind::OutboxEvent, 0xf500 + recovery as u16);
        command.audit.idempotency_key_digest =
            namespaced_digest(&format!("recovery-key-{recovery}"));
        command.audit.request_digest = command.request_digest().unwrap();
        if recovery <= MCP_OAUTH_CLEANUP_RECOVERY_LIMIT {
            let result = if recovery == MCP_OAUTH_CLEANUP_RECOVERY_LIMIT {
                let mut contender = command.clone();
                contender.new_job_id = id(ResourceKind::Job, 0xf600);
                contender.audit.receipt_id = id(ResourceKind::Receipt, 0xf600);
                contender.audit.event_id = id(ResourceKind::Event, 0xf600);
                contender.audit.outbox_id = id(ResourceKind::OutboxEvent, 0xf600);
                contender.audit.idempotency_key_digest = namespaced_digest("boundary-contender");
                let (first, second) = tokio::join!(
                    repository.recover_mcp_oauth_pkce_cleanup(command.clone()),
                    repository.recover_mcp_oauth_pkce_cleanup(contender.clone())
                );
                match (first, second) {
                    (Ok(applied), Err(RepositoryError::Conflict(_))) => applied,
                    (Err(RepositoryError::Conflict(_)), Ok(applied)) => {
                        command = contender;
                        applied
                    }
                    results => panic!("exactly one boundary recovery must win: {results:?}"),
                }
            } else {
                repository
                    .recover_mcp_oauth_pkce_cleanup(command.clone())
                    .await
                    .unwrap()
            };
            assert!(matches!(result, CommandOutcome::Applied(_)));
            let mut replay = command.clone();
            replay.new_job_id = id(ResourceKind::Job, 0xf700 + recovery as u16);
            replay.audit.receipt_id = id(ResourceKind::Receipt, 0xf700 + recovery as u16);
            replay.audit.event_id = id(ResourceKind::Event, 0xf700 + recovery as u16);
            replay.audit.outbox_id = id(ResourceKind::OutboxEvent, 0xf700 + recovery as u16);
            match repository
                .recover_mcp_oauth_pkce_cleanup(replay)
                .await
                .unwrap()
            {
                CommandOutcome::Replayed(job) => {
                    assert_eq!(job.job_id, command.new_job_id.to_string())
                }
                _ => panic!("replay must return the original successor without spending budget"),
            }
            last_command = Some(command);
        } else {
            assert!(matches!(
                repository.recover_mcp_oauth_pkce_cleanup(command).await,
                Err(RepositoryError::Conflict(
                    "cleanup recovery budget exhausted"
                ))
            ));
            assert!(matches!(
                repository
                    .recover_mcp_oauth_pkce_cleanup(last_command.clone().unwrap())
                    .await
                    .unwrap(),
                CommandOutcome::Replayed(_)
            ));
        }
        let counts: (i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM insight_platform.jobs WHERE tenant_id=$1 AND owner_id=$2),
                    (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1 AND operation='mcp.pkce.cleanup.recover'),
                    (SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1 AND event_type='mcp.pkce.cleanup_recovered')")
            .bind(fixture.tenant_id.to_string()).bind(fixture.intent.task_id.to_string())
            .fetch_one(&pool).await.unwrap();
        let allowed = recovery.min(MCP_OAUTH_CLEANUP_RECOVERY_LIMIT) as i64;
        assert_eq!(counts, (allowed + 1, allowed, allowed));
    }
    // These changes model damaged durable evidence. Recompute the normal payload
    // envelope so rejection proves chain validation rather than a bad checksum.
    let initial: (String, serde_json::Value) = sqlx::query_as(
        "SELECT job_id,payload FROM insight_platform.jobs WHERE tenant_id=$1 AND owner_id=$2 AND payload->>'predecessor_job_id' IS NULL")
        .bind(fixture.tenant_id.to_string()).bind(fixture.intent.task_id.to_string())
        .fetch_one(&pool).await.unwrap();
    let last = last_command.unwrap();
    let mut retry = last.clone();
    retry.expected_task_version += 1;
    retry.previous_job_id = last.new_job_id.clone();
    retry.new_job_id = id(ResourceKind::Job, 0xf800);
    retry.audit.receipt_id = id(ResourceKind::Receipt, 0xf800);
    retry.audit.event_id = id(ResourceKind::Event, 0xf800);
    retry.audit.outbox_id = id(ResourceKind::OutboxEvent, 0xf800);
    retry.audit.idempotency_key_digest = namespaced_digest("corrupt-chain-recovery");
    retry.audit.request_digest = retry.request_digest().unwrap();
    for predecessor in [last.new_job_id, id(ResourceKind::Job, 0xf900)] {
        let mut damaged = initial.1.clone();
        damaged["predecessor_job_id"] = serde_json::to_value(predecessor).unwrap();
        damaged["recovery_evidence_digest"] =
            serde_json::to_value(namespaced_digest("damaged-chain")).unwrap();
        let stored = TypedPayload::from_versioned(1, &damaged, 65_536).unwrap();
        sqlx::query("UPDATE insight_platform.jobs SET payload=$3,payload_digest=$4 WHERE tenant_id=$1 AND job_id=$2")
            .bind(fixture.tenant_id.to_string()).bind(&initial.0).bind(stored.value).bind(stored.digest)
            .execute(&pool).await.unwrap();
        assert!(matches!(
            repository
                .recover_mcp_oauth_pkce_cleanup(retry.clone())
                .await,
            Err(RepositoryError::CorruptRow(_))
        ));
    }
    let original = TypedPayload::from_versioned(1, &initial.1, 65_536).unwrap();
    sqlx::query("UPDATE insight_platform.jobs SET payload=$3,payload_digest=$4 WHERE tenant_id=$1 AND job_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(initial.0).bind(original.value).bind(original.digest)
        .execute(&pool).await.unwrap();
    let rejected_receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1 AND receipt_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(retry.audit.receipt_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rejected_receipts, 0);
}

async fn assert_completed_cleanup_chain_retirement(admin: &sqlx::PgPool, fixture: &Fixture) {
    use insight_platform_orchestrator::history::{retirement::*, HistoryRetentionPolicy};
    let url = std::env::var("PLATFORM_TEST_DATABASE_URL").unwrap();
    sqlx::raw_sql("DO $r$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='insight_history_oauth_test') THEN CREATE ROLE insight_history_oauth_test NOLOGIN; END IF; END $r$;").execute(admin).await.unwrap();
    let grants = insight_platform_postgres::history_repository::history_role_grants_sql()
        .lines()
        .filter(|line| !line.starts_with('\\'))
        .collect::<Vec<_>>()
        .join("\n")
        .replace(
            ":'history_maintenance_role'",
            "'insight_history_oauth_test'",
        );
    sqlx::raw_sql(sqlx::AssertSqlSafe(grants))
        .execute(admin)
        .await
        .unwrap();
    let limited = PgPoolOptions::new()
        .max_connections(3)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET ROLE insight_history_oauth_test")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .unwrap();
    let denied = sqlx::query("SELECT payload FROM insight_platform.tasks LIMIT 1")
        .execute(&limited)
        .await
        .unwrap_err();
    assert_eq!(
        denied
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("42501")
    );
    let repository = PgRepository::new(limited.clone());
    let policy = HistoryRetentionPolicy {
        schema_version: 2,
        public_event_minimum_seconds: 1,
        audit_event_minimum_seconds: 1,
        receipt_minimum_seconds: 1,
        published_outbox_minimum_seconds: 1,
        cleanup_minimum_seconds: 1,
    };
    let target = HistoryRecordKey {
        tenant_id: fixture.tenant_id.clone(),
        record_id: fixture.intent.task_id.clone(),
    };
    let tenant = fixture.tenant_id.to_string();
    let chain_count: i64 = sqlx::query_scalar("SELECT count(*) FROM insight_platform.jobs WHERE tenant_id=$1 AND job_kind='mcp_oauth_pkce_cleanup'")
        .bind(&tenant).fetch_one(admin).await.unwrap();
    assert!((1..=9).contains(&chain_count));
    let authorization_before: Option<(String,String)> = sqlx::query_as("SELECT lifecycle_state,payload_digest FROM insight_platform.resources WHERE tenant_id=$1 AND resource_id=$2")
        .bind(&tenant).bind(fixture.intent.authorization_binding_id.to_string()).fetch_optional(admin).await.unwrap();
    let secret_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.secret_bindings WHERE tenant_id=$1",
    )
    .bind(&tenant)
    .fetch_one(admin)
    .await
    .unwrap();
    let old = Utc::now() - Duration::days(70);
    // Advance only fixture clock columns after real fenced completion; proof,
    // predecessor, exact binding and canonical payloads remain untouched.
    sqlx::query("UPDATE insight_platform.tasks SET created_at=$2,updated_at=$2+interval '1 day',responded_at=$2+interval '1 day',deadline=$2+interval '1 day' WHERE tenant_id=$1").bind(&tenant).bind(old).execute(admin).await.unwrap();
    sqlx::query("UPDATE insight_platform.jobs SET created_at=$2,updated_at=$2+interval '1 day',terminal_at=$2+interval '1 day',deadline=$2+interval '30 days' WHERE tenant_id=$1 AND job_kind='mcp_oauth_pkce_cleanup'").bind(&tenant).bind(old).execute(admin).await.unwrap();
    sqlx::query("UPDATE insight_platform.receipts SET created_at=$2,completed_at=$2+interval '1 day',expires_at=$2+interval '2 days' WHERE tenant_id=$1").bind(&tenant).bind(old).execute(admin).await.unwrap();
    sqlx::query(
        "UPDATE insight_platform.events SET occurred_at=$2+interval '1 day' WHERE tenant_id=$1",
    )
    .bind(&tenant)
    .bind(old)
    .execute(admin)
    .await
    .unwrap();
    sqlx::query("UPDATE insight_platform.outbox_events SET state='published',created_at=$2,updated_at=$2+interval '1 day',published_at=$2+interval '1 day',claim_owner=NULL,claim_expires_at=NULL WHERE tenant_id=$1").bind(&tenant).bind(old).execute(admin).await.unwrap();
    let source:String=sqlx::query_scalar("SELECT payload->>'source_event_id' FROM insight_platform.jobs WHERE tenant_id=$1 AND job_kind='mcp_oauth_pkce_cleanup' ORDER BY job_id LIMIT 1").bind(&tenant).fetch_one(admin).await.unwrap();
    assert!(matches!(
        repository
            .retire_history_record(
                HistoryRetirementLane::EventDelivery,
                HistoryRecordKey {
                    tenant_id: fixture.tenant_id.clone(),
                    record_id: source.parse().unwrap()
                },
                &policy
            )
            .await
            .unwrap(),
        HistoryRetirementOutcome::Retired {
            events: 0,
            outbox: 1,
            ..
        }
    ));
    assert_eq!(
        repository
            .retire_history_record(HistoryRetirementLane::OAuthTask, target.clone(), &policy)
            .await
            .unwrap(),
        HistoryRetirementOutcome::Retained(HistoryRetainedReason::Reference)
    );
    let receipts: Vec<String> = sqlx::query_scalar(
        "SELECT receipt_id FROM insight_platform.receipts WHERE tenant_id=$1 ORDER BY receipt_id",
    )
    .bind(&tenant)
    .fetch_all(admin)
    .await
    .unwrap();
    for receipt in receipts {
        let outcome = repository
            .retire_history_record(
                HistoryRetirementLane::Receipt,
                HistoryRecordKey {
                    tenant_id: fixture.tenant_id.clone(),
                    record_id: receipt.parse().unwrap(),
                },
                &policy,
            )
            .await
            .unwrap();
        assert!(
            matches!(
                outcome,
                HistoryRetirementOutcome::Retired { receipts: 1, .. }
            ),
            "{outcome:?}"
        );
    }
    let (current,version):(String,i64)=sqlx::query_as("SELECT current_cleanup_job_id,version FROM insight_platform.tasks WHERE tenant_id=$1 AND task_id=$2").bind(&tenant).bind(fixture.intent.task_id.to_string()).fetch_one(admin).await.unwrap();
    let original: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(&tenant)
    .bind(&current)
    .fetch_one(admin)
    .await
    .unwrap();
    let mut missing = original.clone();
    missing["deletion_proof"] = serde_json::Value::Null;
    let missing = TypedPayload::from_versioned(1, &missing, 65_536).unwrap();
    sqlx::query("UPDATE insight_platform.jobs SET payload=$3,payload_digest=$4 WHERE tenant_id=$1 AND job_id=$2").bind(&tenant).bind(&current).bind(missing.value).bind(missing.digest).execute(admin).await.unwrap();
    assert_eq!(
        repository
            .retire_history_record(HistoryRetirementLane::OAuthTask, target.clone(), &policy)
            .await
            .unwrap(),
        HistoryRetirementOutcome::Retained(HistoryRetainedReason::UnknownEffect)
    );
    assert!(sqlx::query(
        "SELECT insight_platform.history_retire_oauth_chain($1,$2,$3,$4,$5,$6,$7)"
    )
    .bind(&tenant)
    .bind(fixture.intent.task_id.to_string())
    .bind(version)
    .bind(&current)
    .bind(id(ResourceKind::Event, 0xfc01).to_string())
    .bind(id(ResourceKind::OutboxEvent, 0xfc01).to_string())
    .bind(namespaced_digest("missing-proof").to_string())
    .execute(&limited)
    .await
    .is_err());
    let original = TypedPayload::from_versioned(1, &original, 65_536).unwrap();
    sqlx::query("UPDATE insight_platform.jobs SET payload=$3,payload_digest=$4 WHERE tenant_id=$1 AND job_id=$2").bind(&tenant).bind(&current).bind(original.value).bind(original.digest).execute(admin).await.unwrap();
    assert_eq!(
        repository
            .retire_history_record(HistoryRetirementLane::OAuthTask, target.clone(), &policy)
            .await
            .unwrap(),
        HistoryRetirementOutcome::Retired {
            receipts: 0,
            events: 0,
            outbox: 0,
            tasks: 1,
            jobs: chain_count as u16
        }
    );
    assert_eq!(
        repository
            .retire_history_record(HistoryRetirementLane::OAuthTask, target, &policy)
            .await
            .unwrap(),
        HistoryRetirementOutcome::AlreadyAbsent
    );
    let archived:(serde_json::Value,String)=sqlx::query_as("SELECT payload,payload_digest FROM insight_platform.events WHERE tenant_id=$1 AND event_type='mcp.pkce.cleanup_retired'").bind(&tenant).fetch_one(admin).await.unwrap();
    assert_eq!(
        insight_platform_contracts::canonical_digest(&archived.0).unwrap(),
        archived.1
    );
    assert_eq!(
        archived.0["chain"].as_array().unwrap().len(),
        chain_count as usize
    );
    let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM insight_platform.tasks WHERE tenant_id=$1),(SELECT count(*) FROM insight_platform.jobs WHERE tenant_id=$1 AND job_kind='mcp_oauth_pkce_cleanup'),(SELECT count(*) FROM insight_platform.secret_bindings WHERE tenant_id=$1)").bind(&tenant).fetch_one(admin).await.unwrap();
    assert_eq!(counts, (0, 0, secret_count));
    let authorization_after: Option<(String,String)> = sqlx::query_as("SELECT lifecycle_state,payload_digest FROM insight_platform.resources WHERE tenant_id=$1 AND resource_id=$2")
        .bind(&tenant).bind(fixture.intent.authorization_binding_id.to_string()).fetch_optional(admin).await.unwrap();
    assert_eq!(authorization_after, authorization_before);

    // The one archive Event expires normally, without recursively producing a
    // second archive or retaining the Task/Job current-pointer lifecycle.
    sqlx::query(
        "UPDATE insight_platform.events SET occurred_at=$2+interval '1 day' WHERE tenant_id=$1",
    )
    .bind(&tenant)
    .bind(old)
    .execute(admin)
    .await
    .unwrap();
    sqlx::query("UPDATE insight_platform.outbox_events SET state='published',created_at=$2,updated_at=$2+interval '1 day',published_at=$2+interval '1 day' WHERE tenant_id=$1").bind(&tenant).bind(old).execute(admin).await.unwrap();
    let events: Vec<String> =
        sqlx::query_scalar("SELECT event_id FROM insight_platform.events WHERE tenant_id=$1")
            .bind(&tenant)
            .fetch_all(admin)
            .await
            .unwrap();
    for event in events {
        let result = repository
            .retire_history_record(
                HistoryRetirementLane::EventDelivery,
                HistoryRecordKey {
                    tenant_id: fixture.tenant_id.clone(),
                    record_id: event.parse().unwrap(),
                },
                &policy,
            )
            .await
            .unwrap();
        assert!(
            matches!(result, HistoryRetirementOutcome::Retired { events: 1, .. }),
            "{result:?}"
        );
    }
    let left: i64 =
        sqlx::query_scalar("SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1")
            .bind(&tenant)
            .fetch_one(admin)
            .await
            .unwrap();
    assert_eq!(left, 0);
}

#[tokio::test]
async fn phase4_mcp_oauth_retirement_expires_tasks_and_preserves_authorized_credentials() {
    use insight_platform_mcp_host::{
        DriveExpiredMcpOAuthTasks, McpOAuthCallbackResolution, McpOAuthExpirySlot,
    };
    let url = std::env::var("PLATFORM_TEST_DATABASE_URL")
        .expect("PLATFORM_TEST_DATABASE_URL is required");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    for authorized in [false, true] {
        let _namespace = select_fixture_namespace(if authorized { 0xb10b } else { 0xb10a }).await;
        let mut fixture = fixture(Utc::now());
        if !authorized {
            fixture.intent.deadline = Utc::now() + Duration::seconds(2);
        }
        seed_pending_cleanup(&pool, &repository, &fixture).await;
        let token = if authorized {
            let token = ExactSecretBindingRef::build(
                id(ResourceKind::SecretBinding, 0xfd01),
                1,
                fixture.auth_profile.token_secret_provider_id.clone(),
                "mcp.oauth.token".parse().unwrap(),
                SecretResolutionPolicy::Pinned {
                    opaque_version_identity_digest: namespaced_digest("retained-token-version"),
                },
            )
            .unwrap();
            repository
                .create_secret_binding(NewSecretBinding {
                    tenant_id: fixture.tenant_id.clone(),
                    secret_binding_id: token.secret_binding_id.clone(),
                    purpose: token.purpose.clone(),
                    provider_id: token.provider_id.clone(),
                    opaque_reference_ciphertext: vec![1, 2, 3],
                    key_id: "fixture-key".into(),
                    reference_digest: namespaced_digest("retained-token-reference"),
                    payload: SecretBindingPayload {
                        provider_id: token.provider_id.clone(),
                        resolution_policy: token.resolution_policy.clone(),
                    },
                })
                .await
                .unwrap();
            let exchange = repository
                .resolve_exchange_contract(&AuthenticatedMcpOAuthState {
                    tenant_id: fixture.tenant_id.clone(),
                    task_id: fixture.intent.task_id.clone(),
                })
                .await
                .unwrap();
            let grant = McpOAuthAuthorizedGrant::build(
                Utc::now(),
                fixture.intent.requested_scopes.clone(),
                token.clone(),
                exchange.binding.audience_identity_digest.clone(),
                fixture.auth_profile.issuer.endpoint_identity_digest.clone(),
                namespaced_digest("retained-token-subject"),
                namespaced_digest("retained-token-exchange"),
                Utc::now() + Duration::hours(1),
            )
            .unwrap();
            complete_cleanup_task(
                &repository,
                &fixture,
                McpOAuthCallbackResolution::Authorized(Box::new(grant)),
            )
            .await;
            let count:i64=sqlx::query_scalar("SELECT count(*) FROM insight_platform.resources WHERE tenant_id=$1 AND resource_id=$2")
                .bind(fixture.tenant_id.to_string()).bind(fixture.intent.authorization_binding_id.to_string()).fetch_one(&pool).await.unwrap();
            assert_eq!(
                count, 1,
                "callback must create the actual authorization root"
            );
            Some(token)
        } else {
            let wait = (fixture.intent.deadline - Utc::now())
                .to_std()
                .unwrap_or_default()
                + StdDuration::from_millis(20);
            tokio::time::sleep(wait).await;
            let mut after = None;
            let mut target_expired = false;
            for _ in 0..256 {
                let page = repository
                    .drive_expired_mcp_oauth_tasks(DriveExpiredMcpOAuthTasks {
                        after,
                        scheduler_generation_id: id(ResourceKind::WorkerProcessGeneration, 0xfd02),
                        limit: 1,
                        slots: vec![McpOAuthExpirySlot {
                            event_id: ResourceId::from_uuid_v7(
                                ResourceKind::Event,
                                uuid::Uuid::now_v7(),
                            )
                            .unwrap(),
                            outbox_id: ResourceId::from_uuid_v7(
                                ResourceKind::OutboxEvent,
                                uuid::Uuid::now_v7(),
                            )
                            .unwrap(),
                        }],
                    })
                    .await
                    .unwrap();
                target_expired = page.records.iter().any(|task| {
                    task.task_id == fixture.intent.task_id.to_string()
                        && task.state == insight_platform_tasks::TaskState::Expired
                });
                if target_expired || page.exhausted {
                    break;
                }
                after = page.next_cursor;
            }
            assert!(
                target_expired,
                "bounded global expiry must reach the target after retained invalid Tasks"
            );
            let count:i64=sqlx::query_scalar("SELECT count(*) FROM insight_platform.resources WHERE tenant_id=$1 AND resource_id=$2")
                .bind(fixture.tenant_id.to_string()).bind(fixture.intent.authorization_binding_id.to_string()).fetch_one(&pool).await.unwrap();
            assert_eq!(
                count, 0,
                "expired Task owns only a prospective authorization identity"
            );
            None
        };
        let claim = claim_cleanup_for(
            &repository,
            &fixture.intent.task_id,
            id(ResourceKind::WorkerProcessGeneration, 0xfd05),
            b"retirement-cleanup-worker",
        )
        .await;
        assert!(repository
            .settle_mcp_oauth_pkce_cleanup(
                &claim,
                McpOAuthPkceCleanupSettlement::Completed {
                    proof: McpOAuthPkceSecretCleanupDisposition::Deleted
                }
            )
            .await
            .unwrap());
        assert_completed_cleanup_chain_retirement(&pool, &fixture).await;
        if let Some(token) = token {
            let stored:(i64,String,Vec<u8>)=sqlx::query_as("SELECT generation,purpose,opaque_reference_ciphertext FROM insight_platform.secret_bindings WHERE tenant_id=$1 AND secret_binding_id=$2")
                .bind(fixture.tenant_id.to_string()).bind(token.secret_binding_id.to_string()).fetch_one(&pool).await.unwrap();
            assert_eq!(stored, (1, "mcp.oauth.token".into(), vec![1, 2, 3]));
        } else {
            // After retirement, the old key is a fresh command, not an old
            // response tombstone. The current authorizer and owner checks run.
            let service = McpOAuthAuthorizationStartService::new(
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
            let mut fresh = fixture.intent.clone();
            fresh.task_id = id(ResourceKind::Interaction, 0xfd06);
            fresh.authorization_binding_id = id(ResourceKind::McpAuthorizationBinding, 0xfd07);
            fresh.audit.receipt_id = id(ResourceKind::Receipt, 0xfd08);
            fresh.audit.event_id = id(ResourceKind::Event, 0xfd09);
            fresh.audit.outbox_id = id(ResourceKind::OutboxEvent, 0xfd0a);
            fresh.deadline = Utc::now() + Duration::minutes(5);
            fresh.audit.receipt_expires_at = Utc::now() + Duration::minutes(10);
            assert_eq!(
                fresh.audit.idempotency_key_digest,
                fixture.intent.audit.idempotency_key_digest
            );
            sqlx::query("UPDATE insight_platform.tenant_principals SET state='revoked',version=version+1 WHERE tenant_id=$1 AND principal_id=$2")
                .bind(fixture.tenant_id.to_string()).bind(fixture.principal_id.to_string()).execute(&pool).await.unwrap();
            assert!(service.start(fresh.clone(), Utc::now()).await.is_err());
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1",
            )
            .bind(fixture.tenant_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                count, 0,
                "new command must not create Receipt before current authorization"
            );
            sqlx::query("UPDATE insight_platform.tenant_principals SET state='active',version=version+1 WHERE tenant_id=$1 AND principal_id=$2")
                .bind(fixture.tenant_id.to_string()).bind(fixture.principal_id.to_string()).execute(&pool).await.unwrap();
            let applied = service.start(fresh, Utc::now()).await.unwrap();
            assert_eq!(
                applied.disposition,
                McpOAuthAuthorizationStartCommitDisposition::Applied
            );
        }
    }
}

#[tokio::test]
async fn phase4_mcp_oauth_expiry_isolates_bad_task_and_advances_global_pages() {
    let _namespace = select_fixture_namespace(0xb110).await;
    let url = std::env::var("PLATFORM_TEST_DATABASE_URL")
        .expect("PLATFORM_TEST_DATABASE_URL is required for OAuth expiry isolation");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let base: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let mut fixtures = Vec::new();
    for ordinal in 0..3 {
        OAUTH_FIXTURE_NAMESPACE.store(0xb110 + ordinal, Ordering::SeqCst);
        let mut item = fixture(base);
        item.intent.deadline =
            base + Duration::seconds(2) + Duration::milliseconds(i64::from(ordinal));
        seed_pending_cleanup(&pool, &repository, &item).await;
        fixtures.push(item);
    }
    sqlx::query(
        "UPDATE insight_platform.tasks SET payload_digest=$3 WHERE tenant_id=$1 AND task_id=$2",
    )
    .bind(fixtures[0].tenant_id.to_string())
    .bind(fixtures[0].intent.task_id.to_string())
    .bind(sha('0').to_string())
    .execute(&pool)
    .await
    .unwrap();
    let bad_before: serde_json::Value = sqlx::query_scalar(
        "SELECT to_jsonb(t) FROM insight_platform.tasks t WHERE tenant_id=$1 AND task_id=$2",
    )
    .bind(fixtures[0].tenant_id.to_string())
    .bind(fixtures[0].intent.task_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    while Utc::now() <= fixtures[2].intent.deadline {
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    let mut cursor = None;
    let mut expired = Vec::new();
    let mut invalid = Vec::new();
    for _ in 0..4 {
        let page = repository
            .drive_expired_mcp_oauth_tasks(insight_platform_mcp_host::DriveExpiredMcpOAuthTasks {
                after: cursor.clone(),
                scheduler_generation_id: ResourceId::from_uuid_v7(
                    ResourceKind::WorkerProcessGeneration,
                    uuid::Uuid::now_v7(),
                )
                .unwrap(),
                limit: 1,
                slots: vec![insight_platform_mcp_host::McpOAuthExpirySlot {
                    event_id: ResourceId::from_uuid_v7(ResourceKind::Event, uuid::Uuid::now_v7())
                        .unwrap(),
                    outbox_id: ResourceId::from_uuid_v7(
                        ResourceKind::OutboxEvent,
                        uuid::Uuid::now_v7(),
                    )
                    .unwrap(),
                }],
            })
            .await
            .unwrap();
        expired.extend(page.records);
        invalid.extend(page.diagnostics);
        cursor = page.next_cursor;
        if page.exhausted {
            break;
        }
    }
    assert_eq!(invalid.len(), 1);
    assert_eq!(invalid[0].item_id, fixtures[0].intent.task_id);
    assert_eq!(
        expired
            .iter()
            .map(|task| task.task_id.clone())
            .collect::<std::collections::BTreeSet<_>>(),
        fixtures[1..]
            .iter()
            .map(|item| item.intent.task_id.to_string())
            .collect()
    );
    let bad_after: serde_json::Value = sqlx::query_scalar(
        "SELECT to_jsonb(t) FROM insight_platform.tasks t WHERE tenant_id=$1 AND task_id=$2",
    )
    .bind(fixtures[0].tenant_id.to_string())
    .bind(fixtures[0].intent.task_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(bad_before, bad_after);
    for (index, item) in fixtures.iter().enumerate() {
        let cleanup_count:i64=sqlx::query_scalar("SELECT count(*) FROM insight_platform.jobs WHERE tenant_id=$1 AND owner_id=$2 AND job_kind='mcp_oauth_pkce_cleanup'").bind(item.tenant_id.to_string()).bind(item.intent.task_id.to_string()).fetch_one(&pool).await.unwrap();
        assert_eq!(cleanup_count, if index == 0 { 0 } else { 1 });
        let expiry_events:i64=sqlx::query_scalar("SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1 AND aggregate_id=$2 AND event_type='mcp.oauth_authorization_expired'").bind(item.tenant_id.to_string()).bind(item.intent.task_id.to_string()).fetch_one(&pool).await.unwrap();
        assert_eq!(expiry_events, if index == 0 { 0 } else { 1 });
    }
}
