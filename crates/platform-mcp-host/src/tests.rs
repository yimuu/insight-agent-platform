use super::*;
use chrono::Duration as ChronoDuration;
use insight_platform_context::{
    AcceptedContextSubscriptionRefresh, AdmitContextSubscriptionRefresh,
    ContextSubscriptionAdmissionAuthority, ContextSubscriptionAdmissionError,
    ContextSubscriptionRefreshCause, CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
};
use insight_platform_contracts::{
    AllowedMcpServerCapabilities, ArtifactRef, CanonicalHttpEndpoint, CapabilityEndpointScheme,
    CommandAudit, CommandOutcome, DataClassification, ExactSecretBindingRef, ExactVersionRef,
    JobState, McpAuthPolicyDocument, McpAuthorizationState, McpClientCapabilities,
    McpMetadataPolicy, McpMethodLimits, McpNegotiatedCapabilities,
    McpOAuthClientAuthenticationKind, McpOAuthEndpoint, McpOAuthTaskBinding, McpServerLimits,
    McpSessionState, McpTransportBinding, McpTransportFeatures, Permission, PermissionSet,
    PrincipalSnapshot, SecretPurpose, SecretResolutionPolicy, MCP_PROTOCOL_BASELINE,
};
use insight_platform_jobs::JobFence;
use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f5{suffix:04x}",
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

fn discovery_preallocation(suffix: u16) -> McpDiscoveryArtifactPreallocation {
    discovery_preallocation_for(
        id(ResourceKind::Artifact, suffix),
        id(ResourceKind::ArtifactLink, suffix + 3),
        id(ResourceKind::McpDiscoverySnapshot, suffix + 4),
        suffix,
    )
}

fn discovery_preallocation_for(
    artifact_id: ResourceId,
    artifact_link_id: ResourceId,
    snapshot_id: ResourceId,
    suffix: u16,
) -> McpDiscoveryArtifactPreallocation {
    McpDiscoveryArtifactPreallocation::build(
        artifact_id,
        id(ResourceKind::InternalBlob, suffix + 1),
        id(ResourceKind::Job, suffix + 2),
        artifact_link_id,
        snapshot_id,
        id(ResourceKind::QuotaLedgerEntry, suffix + 5),
    )
    .unwrap()
}

fn exact(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
    ExactVersionRef::new(id(kind, suffix), sha(character)).unwrap()
}

fn principal(tenant_id: ResourceId, principal_id: ResourceId) -> PrincipalSnapshot {
    PrincipalSnapshot::build(
        tenant_id,
        principal_id,
        PrincipalKind::AgentRunner,
        PermissionSet::new(vec![Permission::McpWrite]).unwrap(),
        1,
        1,
        1,
    )
    .unwrap()
}

fn rotated_token_binding(
    current: &ExactSecretBindingRef,
    generation: u64,
    character: char,
) -> ExactSecretBindingRef {
    ExactSecretBindingRef::build(
        current.secret_binding_id.clone(),
        generation,
        current.provider_id.clone(),
        current.purpose.clone(),
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: sha(character),
        },
    )
    .unwrap()
}

fn method_limits() -> McpMethodLimits {
    McpMethodLimits {
        maximum_request_bytes: 4_096,
        maximum_response_bytes: 4_096,
        maximum_metadata_entries: 16,
        maximum_progress_events: 32,
        maximum_pages: 16,
        minimum_poll_milliseconds: 10,
        maximum_poll_milliseconds: 1_000,
    }
}

fn server_limits(total_timeout_milliseconds: u64) -> McpServerLimits {
    McpServerLimits {
        maximum_message_bytes: 8_192,
        maximum_response_bytes: 8_192,
        maximum_headers: 32,
        maximum_sse_event_bytes: 4_096,
        maximum_in_flight: 8,
        maximum_connections: 4,
        maximum_sessions: 4,
        maximum_session_milliseconds: 3_600_000,
        idle_timeout_milliseconds: 5,
        initialize_timeout_milliseconds: 5,
        request_timeout_milliseconds: total_timeout_milliseconds,
        total_timeout_milliseconds,
    }
}

fn protocol(tasks: bool) -> McpProtocolPolicyDocument {
    let mut limits = BTreeMap::from([(PublishedMcpMethod::ToolsCall, method_limits())]);
    if tasks {
        for method in [
            PublishedMcpMethod::TasksGet,
            PublishedMcpMethod::TasksResult,
            PublishedMcpMethod::TasksCancel,
        ] {
            limits.insert(method, method_limits());
        }
    }
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
            tasks,
            subscriptions: false,
        },
        experimental_features: if tasks {
            vec![McpExperimentalFeature::Tasks]
        } else {
            Vec::new()
        },
        method_limits: limits,
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

struct Fixture {
    contract: McpHostExecutionContract,
    request: McpOperationRequest,
}

fn fixture(tasks: bool, effect: Effect, timeout_milliseconds: u64) -> Fixture {
    let now = Utc::now();
    let tenant_id = id(ResourceKind::Tenant, 1);
    let deployment = ExactDeploymentRef::new(id(ResourceKind::McpDeployment, 2), sha('2')).unwrap();
    let server_revision = exact(ResourceKind::McpServerRevision, 3, '3');
    let protocol_document = protocol(tasks);
    let protocol_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 4),
        protocol_document.canonical_digest().unwrap(),
    )
    .unwrap();
    let network_policy = exact(ResourceKind::PolicyRevision, 5, '5');
    let tls_policy = exact(ResourceKind::PolicyRevision, 6, '6');
    let trust_policy = exact(ResourceKind::PolicyRevision, 7, '7');
    let auth_policy = exact(ResourceKind::PolicyRevision, 8, '8');
    let endpoint = CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: "mcp.example.test".to_owned(),
        port: 443,
        base_path: "/v1/mcp".to_owned(),
    };
    let server_identity_digest = endpoint.canonical_digest().unwrap();
    let transport = McpTransportBinding::StreamableHttp {
        endpoint_identity_digest: server_identity_digest.clone(),
        endpoint,
        network_policy,
        tls_policy,
    };
    let secret_binding_id = id(ResourceKind::SecretBinding, 9);
    let secret_purpose = SecretPurpose::from_str("mcp.oauth").unwrap();
    let secret_binding = ExactSecretBindingRef::build(
        secret_binding_id.clone(),
        1,
        id(ResourceKind::SecretProvider, 9),
        secret_purpose.clone(),
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: sha('9'),
        },
    )
    .unwrap();
    let conformance_evidence = ArtifactRef::new(
        id(ResourceKind::Artifact, 10),
        sha('a'),
        128,
        "application/json",
        DataClassification::Internal,
        Some("mcp-conformance.json".to_owned()),
    )
    .unwrap();
    let deployment_closure = McpDeploymentClosure {
        server_revision: server_revision.clone(),
        server_identity_digest: server_identity_digest.clone(),
        transport,
        protocol_policy: protocol_policy.clone(),
        trust_policy,
        auth_policy: Some(auth_policy),
        secret_bindings: vec![],
        conformance_evidence,
    };
    let server = McpServerExecutionContract::build(
        server_revision.clone(),
        McpTransportKind::StreamableHttp,
        protocol_policy.clone(),
        vec![],
        Some(secret_purpose),
        server_limits(timeout_milliseconds),
    )
    .unwrap();
    let authorization = McpAuthorizationContext::build(NewMcpAuthorizationContext {
        tenant_id: tenant_id.clone(),
        authorization_binding_id: id(ResourceKind::McpAuthorizationBinding, 11),
        mcp_deployment: deployment.clone(),
        principal_kind: McpAuthorizationPrincipalKind::PerUser,
        principal_id: id(ResourceKind::Principal, 12),
        principal_identity_kind: PrincipalKind::AgentRunner,
        principal_binding_generation: 1,
        audience_identity_digest: server_identity_digest,
        granted_scopes: vec!["tools.read".to_owned(), "tools.call".to_owned()],
        token_secret_binding: secret_binding,
        generation: 1,
        expires_at: now + ChronoDuration::hours(1),
    })
    .unwrap();
    let objects_artifact = ArtifactRef::new(
        id(ResourceKind::Artifact, 13),
        sha('d'),
        256,
        "application/json",
        DataClassification::Internal,
        Some("mcp-discovery.json".to_owned()),
    )
    .unwrap();
    let discovery = McpDiscoverySnapshot::build(
        id(ResourceKind::McpDiscoverySnapshot, 14),
        deployment.clone(),
        server_revision,
        protocol_policy,
        authorization.canonical_digest.clone(),
        MCP_PROTOCOL_BASELINE.to_owned(),
        McpNegotiatedCapabilities {
            tools: true,
            resources: false,
            prompts: false,
            logging: false,
            tasks,
            tasks_list: tasks,
            tasks_cancel: tasks,
            tasks_tools_call: tasks,
            elicitation: true,
            sampling: false,
            roots: false,
            subscriptions: false,
        },
        objects_artifact,
        now - ChronoDuration::seconds(1),
        now + ChronoDuration::minutes(30),
    )
    .unwrap();
    let contract = McpHostExecutionContract::build(NewMcpHostExecutionContract {
        deployment,
        deployment_closure,
        server,
        protocol_profile: protocol_document,
        authorization,
        discovery,
    })
    .unwrap();
    let request = McpOperationRequest {
        schema_version: 1,
        mcp_operation_id: id(ResourceKind::McpOperation, 15),
        tenant_id,
        invocation_id: id(ResourceKind::CapabilityInvocation, 16),
        job_id: id(ResourceKind::Job, 17),
        worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 18),
        lease_generation: 1,
        physical_attempt: 1,
        snapshot_id: contract.discovery.snapshot_id.clone(),
        authorization_binding_id: contract.authorization.authorization_binding_id.clone(),
        method: PublishedMcpMethod::ToolsCall,
        params: ClosedJsonValue::build(sha('e'), serde_json::json!({"query": "hello"})).unwrap(),
        task_requested: false,
        continuation: None,
        idempotency_key_digest: sha('f'),
        idempotency: CapabilityIdempotencyKind::None,
        effect,
        deadline: now + ChronoDuration::minutes(5),
    };
    Fixture { contract, request }
}

fn authorization_record(fixture: &Fixture, now: DateTime<Utc>) -> McpAuthorizationBindingRecord {
    let authorization = &fixture.contract.authorization;
    McpAuthorizationBindingRecord::create(
        NewMcpAuthorizationBinding {
            tenant_id: authorization.tenant_id.clone(),
            authorization_binding_id: authorization.authorization_binding_id.clone(),
            mcp_deployment: authorization.mcp_deployment.clone(),
            principal_kind: authorization.principal_kind,
            principal_id: authorization.principal_id.clone(),
            principal_identity_kind: authorization.principal_identity_kind,
            principal_binding_generation: authorization.principal_binding_generation,
            audience_identity_digest: authorization.audience_identity_digest.clone(),
            granted_scopes: authorization.granted_scopes.clone(),
            token_secret_binding: authorization.token_secret_binding.clone(),
            expires_at: now + ChronoDuration::hours(1),
        },
        now,
    )
    .unwrap()
}

#[derive(Clone)]
enum StaticBehavior {
    Outcome(Box<McpOperationOutcome>),
    Failure(McpTransportFailure),
    Delay(Duration),
    Panic,
}

struct StaticTransport {
    kind: McpTransportKind,
    behavior: StaticBehavior,
}

#[async_trait]
impl McpHostTransport for StaticTransport {
    fn kind(&self) -> McpTransportKind {
        self.kind
    }

    async fn execute(
        &self,
        _contract: &McpHostExecutionContract,
        _request: &McpOperationRequest,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        match &self.behavior {
            StaticBehavior::Outcome(outcome) => Ok(outcome.as_ref().clone()),
            StaticBehavior::Failure(failure) => Err(failure.clone()),
            StaticBehavior::Delay(duration) => {
                tokio::time::sleep(*duration).await;
                Ok(completed())
            }
            StaticBehavior::Panic => panic!("contained transport panic"),
        }
    }

    async fn cancel_remote_task(
        &self,
        _contract: &McpHostExecutionContract,
        _request: &McpOperationRequest,
        _deadline: DateTime<Utc>,
    ) -> Result<McpRemoteTaskCancelOutcome, McpTransportFailure> {
        Ok(McpRemoteTaskCancelOutcome::Accepted)
    }
}

fn completed() -> McpOperationOutcome {
    McpOperationOutcome::Completed {
        result: ClosedJsonValue::build(sha('1'), serde_json::json!({"ok": true})).unwrap(),
        evidence_digest: sha('2'),
    }
}

fn safe_failure(code: &str) -> SafeMcpFailure {
    SafeMcpFailure {
        safe_code: code.to_owned(),
        safe_message: "safe transport failure".to_owned(),
        evidence_digest: sha('3'),
    }
}

#[test]
fn execution_contract_binds_tenant_audience_scope_and_discovery() {
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    assert!(fixture
        .contract
        .deployment_closure
        .secret_bindings
        .is_empty());
    assert_eq!(
        fixture
            .contract
            .server
            .authorization_credential_purpose
            .as_ref(),
        Some(&fixture.contract.authorization.token_secret_binding.purpose)
    );
    fixture.contract.validate_canonical_at(Utc::now()).unwrap();
    fixture
        .request
        .validate_for(&fixture.contract, Utc::now())
        .unwrap();

    let mut forged = fixture.contract.clone();
    forged.discovery.authorization_context_digest = sha('0');
    assert_eq!(
        forged.validate_at(Utc::now()),
        Err(McpHostError::InvalidExecutionContract)
    );

    let mut cross_tenant = fixture.request;
    cross_tenant.tenant_id = id(ResourceKind::Tenant, 99);
    assert_eq!(
        cross_tenant.validate_for(&fixture.contract, Utc::now()),
        Err(McpHostError::InvalidOperation)
    );
}

#[test]
fn authorization_principal_class_and_scope_are_fail_closed() {
    let test_fixture = fixture(false, Effect::ReadOnly, 1_000);
    let mut wrong_class = test_fixture.contract.authorization.clone();
    wrong_class.principal_kind = McpAuthorizationPrincipalKind::ServiceIdentity;
    assert_eq!(
        wrong_class.validate_at(Utc::now()),
        Err(McpHostError::InvalidAuthorization)
    );

    let mut duplicate_scope = test_fixture.contract.authorization.clone();
    duplicate_scope.granted_scopes = vec!["tools.call".to_owned(), "tools.call".to_owned()];
    assert_eq!(
        duplicate_scope.validate_at(Utc::now()),
        Err(McpHostError::InvalidAuthorization)
    );

    let mut follows_rotation = test_fixture.contract.authorization;
    follows_rotation.token_secret_binding.resolution_policy =
        SecretResolutionPolicy::FollowProviderRotation {
            rotation_policy_revision_id: id(ResourceKind::PolicyRevision, 90),
        };
    assert_eq!(
        follows_rotation.validate_at(Utc::now()),
        Err(McpHostError::InvalidAuthorization)
    );
}

#[test]
fn oauth_callback_is_state_bound_scope_reducing_and_secret_free() {
    let now = Utc::now();
    let tenant_id = id(ResourceKind::Tenant, 91);
    let principal_id = id(ResourceKind::Principal, 92);
    let authorization_binding_id = id(ResourceKind::McpAuthorizationBinding, 93);
    let task_id = id(ResourceKind::Interaction, 94);
    let pkce = ExactSecretBindingRef::build(
        id(ResourceKind::SecretBinding, 95),
        1,
        id(ResourceKind::SecretProvider, 95),
        MCP_OAUTH_PKCE_SECRET_PURPOSE.parse().unwrap(),
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: sha('1'),
        },
    )
    .unwrap();
    let begin = BeginMcpOAuthAuthorization {
        audit: CommandAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: tenant_id.clone(),
            principal_id: principal_id.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            receipt_id: id(ResourceKind::Receipt, 96),
            event_id: id(ResourceKind::Event, 97),
            outbox_id: id(ResourceKind::OutboxEvent, 98),
            idempotency_key_digest: sha('2'),
            request_digest: sha('3'),
            receipt_expires_at: now + ChronoDuration::minutes(15),
        },
        task_id: task_id.clone(),
        authorization_binding_id: authorization_binding_id.clone(),
        mcp_deployment: ExactDeploymentRef::new(id(ResourceKind::McpDeployment, 99), sha('4'))
            .unwrap(),
        expected_principal_binding_generation: 1,
        requested_scopes: vec!["tools.call".to_owned(), "tools.read".to_owned()],
        state_digest: sha('5'),
        nonce_digest: sha('6'),
        callback_binding_digest: sha('7'),
        pkce_secret_binding: pkce,
        reauthorization: None,
        safe_prompt_key: "authorize_mcp_server".to_owned(),
        deadline: now + ChronoDuration::minutes(10),
    };
    let binding = begin
        .task_binding(
            &principal(tenant_id.clone(), principal_id),
            sha('8'),
            "mcp.oauth".parse().unwrap(),
            exact(ResourceKind::PolicyRevision, 109, 'f'),
        )
        .unwrap();
    let grant = McpOAuthAuthorizedGrant::build(
        vec!["tools.call".to_owned()],
        ExactSecretBindingRef::build(
            id(ResourceKind::SecretBinding, 100),
            1,
            id(ResourceKind::SecretProvider, 100),
            "mcp.oauth".parse().unwrap(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: sha('9'),
            },
        )
        .unwrap(),
        sha('8'),
        sha('a'),
        sha('b'),
        sha('c'),
        now + ChronoDuration::hours(1),
    )
    .unwrap();
    let mut callback = CompleteMcpOAuthCallback {
        audit: McpOAuthCallbackAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: tenant_id.clone(),
            callback_ingress_generation_id: id(ResourceKind::WorkerProcessGeneration, 101),
            receipt_id: id(ResourceKind::Receipt, 102),
            event_id: id(ResourceKind::Event, 103),
            outbox_id: id(ResourceKind::OutboxEvent, 104),
            idempotency_key_digest: sha('d'),
            request_digest: sha('e'),
            callback_binding_digest: sha('7'),
            receipt_expires_at: now + ChronoDuration::minutes(15),
        },
        task_id: task_id.clone(),
        authorization_binding_id,
        expected_task_generation: 1,
        expected_task_version: 1,
        state_digest: sha('5'),
        resolution: McpOAuthCallbackResolution::Authorized(Box::new(grant)),
    };
    callback
        .validate_for_binding(&tenant_id, &task_id, &binding, now)
        .unwrap();

    callback.state_digest = sha('0');
    assert_eq!(
        callback.validate_for_binding(&tenant_id, &task_id, &binding, now),
        Err(McpHostError::InvalidAuthorization)
    );
}

#[test]
fn oauth_expiry_scan_is_bounded_and_has_unique_event_slots() {
    let valid = DriveExpiredMcpOAuthTasks {
        tenant_id: id(ResourceKind::Tenant, 105),
        scheduler_generation_id: id(ResourceKind::WorkerProcessGeneration, 106),
        limit: 1,
        slots: vec![McpOAuthExpirySlot {
            event_id: id(ResourceKind::Event, 107),
            outbox_id: id(ResourceKind::OutboxEvent, 108),
        }],
    };
    valid.validate().unwrap();

    let mut unbounded = valid.clone();
    unbounded.limit = MAX_MCP_OAUTH_EXPIRY_BATCH + 1;
    assert_eq!(
        unbounded.validate(),
        Err(McpHostError::InvalidAuthorization)
    );
}

fn oauth_endpoint(host: &str, path: &str) -> McpOAuthEndpoint {
    let endpoint = CanonicalHttpEndpoint {
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

fn oauth_exchange_contract(now: DateTime<Utc>) -> (McpOAuthExchangeContract, Vec<u8>) {
    let raw_query =
        b"state=opaque-state&code=one-time-code&iss=https%3A%2F%2Fauth.example.test".to_vec();
    let parsed = parse_mcp_oauth_callback_query(&raw_query).unwrap();
    let state_digest = mcp_oauth_state_digest(&parsed.state).unwrap();
    let issuer = oauth_endpoint("auth.example.test", "/");
    assert_eq!(
        mcp_oauth_issuer_identity_digest(parsed.issuer.as_ref().unwrap()).unwrap(),
        issuer.endpoint_identity_digest
    );
    let redirect_uri = oauth_endpoint("platform.example.test", "/v1/mcp/oauth/callback");
    let resource_indicator = oauth_endpoint("mcp.example.test", "/mcp");
    let callback_binding_digest = redirect_uri.endpoint_identity_digest.clone();
    let audience_identity_digest = resource_indicator.endpoint_identity_digest.clone();
    let auth_profile = McpAuthPolicyDocument {
        schema_version: 1,
        issuer,
        authorization_endpoint: oauth_endpoint("auth.example.test", "/oauth/authorize"),
        token_endpoint: oauth_endpoint("auth.example.test", "/oauth/token"),
        client_id: "insight-platform".to_owned(),
        client_authentication: McpOAuthClientAuthenticationKind::None,
        client_credential_purpose: None,
        pkce_secret_provider_id: id(ResourceKind::SecretProvider, 128),
        token_secret_provider_id: id(ResourceKind::SecretProvider, 137),
        redirect_uri,
        resource_indicator,
        allowed_scopes: vec!["tools.call".to_owned(), "tools.read".to_owned()],
        maximum_token_response_bytes: 65_536,
        connect_timeout_milliseconds: 5_000,
        total_timeout_milliseconds: 30_000,
        maximum_clock_skew_seconds: 60,
    };
    let auth_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 120),
        auth_profile.canonical_digest().unwrap(),
    )
    .unwrap();
    let server_revision = exact(ResourceKind::McpServerRevision, 121, '1');
    let protocol_policy = exact(ResourceKind::PolicyRevision, 122, '2');
    let network_policy = exact(ResourceKind::PolicyRevision, 123, '3');
    let tls_policy = exact(ResourceKind::PolicyRevision, 124, '4');
    let endpoint = CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: "mcp.example.test".to_owned(),
        port: 443,
        base_path: "/mcp".to_owned(),
    };
    let transport = McpTransportBinding::StreamableHttp {
        endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
        endpoint,
        network_policy,
        tls_policy,
    };
    let token_purpose: SecretPurpose = "mcp.oauth".parse().unwrap();
    let binding = McpOAuthTaskBinding {
        authorization_binding_id: id(ResourceKind::McpAuthorizationBinding, 125),
        mcp_deployment: ExactDeploymentRef::new(id(ResourceKind::McpDeployment, 126), sha('5'))
            .unwrap(),
        auth_policy: auth_policy.clone(),
        principal_binding_generation: 1,
        audience_identity_digest: audience_identity_digest.clone(),
        requested_scopes: vec!["tools.call".to_owned(), "tools.read".to_owned()],
        token_credential_purpose: token_purpose.clone(),
        state_digest,
        nonce_digest: sha('6'),
        callback_binding_digest,
        pkce_secret_binding: Box::new(
            ExactSecretBindingRef::build(
                id(ResourceKind::SecretBinding, 127),
                1,
                id(ResourceKind::SecretProvider, 128),
                MCP_OAUTH_PKCE_SECRET_PURPOSE.parse().unwrap(),
                SecretResolutionPolicy::Pinned {
                    opaque_version_identity_digest: sha('9'),
                },
            )
            .unwrap(),
        ),
        expected_authorization_generation: None,
        expected_authorization_version: None,
    };
    let server = McpServerExecutionContract::build(
        server_revision.clone(),
        McpTransportKind::StreamableHttp,
        protocol_policy.clone(),
        vec![],
        Some(token_purpose),
        server_limits(30_000),
    )
    .unwrap();
    let contract = McpOAuthExchangeContract {
        tenant_id: id(ResourceKind::Tenant, 129),
        task_id: id(ResourceKind::Interaction, 130),
        task_generation: 1,
        task_version: 1,
        task_deadline: now + ChronoDuration::minutes(5),
        binding,
        deployment_closure: McpDeploymentClosure {
            server_revision,
            server_identity_digest: audience_identity_digest,
            transport,
            protocol_policy,
            trust_policy: exact(ResourceKind::PolicyRevision, 131, 'a'),
            auth_policy: Some(auth_policy),
            secret_bindings: vec![],
            conformance_evidence: ArtifactRef::new(
                id(ResourceKind::Artifact, 132),
                sha('b'),
                128,
                "application/json",
                DataClassification::Internal,
                Some("mcp-oauth-conformance.json".to_owned()),
            )
            .unwrap(),
        },
        server,
        auth_profile,
    };
    contract.validate_at(now).unwrap();
    (contract, raw_query)
}

struct FixtureStateAuthenticator {
    identity: AuthenticatedMcpOAuthState,
}

#[async_trait]
impl McpOAuthStateAuthenticator for FixtureStateAuthenticator {
    async fn authenticate(
        &self,
        state: &SensitiveOAuthValue,
        _now: DateTime<Utc>,
    ) -> Result<AuthenticatedMcpOAuthState, McpOAuthCallbackAuthorityError> {
        if state.as_bytes() != b"opaque-state" {
            return Err(McpOAuthCallbackAuthorityError::NotFoundOrChanged);
        }
        Ok(self.identity.clone())
    }
}

struct FixtureCallbackIds;

impl McpOAuthCallbackIdentityFactory for FixtureCallbackIds {
    fn next_ids(
        &self,
        _tenant_id: &ResourceId,
        _task_id: &ResourceId,
        _request_digest: &Sha256Digest,
    ) -> Result<McpOAuthCallbackCommitIds, McpOAuthCallbackAuthorityError> {
        Ok(McpOAuthCallbackCommitIds {
            receipt_id: id(ResourceKind::Receipt, 133),
            event_id: id(ResourceKind::Event, 134),
            outbox_id: id(ResourceKind::OutboxEvent, 135),
        })
    }
}

struct FixtureCallbackAuthority {
    contract: McpOAuthExchangeContract,
    commits: Arc<Mutex<Vec<CompleteMcpOAuthCallback>>>,
}

#[async_trait]
impl McpOAuthCallbackAuthority for FixtureCallbackAuthority {
    async fn resolve_exchange_contract(
        &self,
        identity: &AuthenticatedMcpOAuthState,
    ) -> Result<McpOAuthExchangeContract, McpOAuthCallbackAuthorityError> {
        if identity.tenant_id != self.contract.tenant_id
            || identity.task_id != self.contract.task_id
        {
            return Err(McpOAuthCallbackAuthorityError::NotFoundOrChanged);
        }
        Ok(self.contract.clone())
    }

    async fn commit_callback(
        &self,
        command: CompleteMcpOAuthCallback,
    ) -> Result<McpOAuthCallbackCommitOutcome, McpOAuthCallbackAuthorityError> {
        self.commits.lock().unwrap().push(command);
        Ok(McpOAuthCallbackCommitOutcome {
            disposition: McpOAuthCallbackCommitDisposition::Authorized,
        })
    }
}

struct FixtureCredentialBroker {
    grant: McpOAuthAuthorizedGrant,
    exchanges: Arc<Mutex<u32>>,
}

#[async_trait]
impl McpOAuthCredentialBroker for FixtureCredentialBroker {
    async fn exchange_authorization_code(
        &self,
        _contract: &McpOAuthExchangeContract,
        authorization_code: SensitiveOAuthValue,
        _now: DateTime<Utc>,
    ) -> Result<McpOAuthAuthorizedGrant, McpOAuthCredentialBrokerError> {
        if authorization_code.as_bytes() != b"one-time-code" {
            return Err(McpOAuthCredentialBrokerError::Rejected);
        }
        *self.exchanges.lock().unwrap() += 1;
        Ok(self.grant.clone())
    }
}

#[tokio::test]
async fn oauth_callback_ingress_authenticates_state_before_broker_and_never_commits_raw_values() {
    let now = Utc::now();
    let (contract, raw_query) = oauth_exchange_contract(now);
    let commits = Arc::new(Mutex::new(Vec::new()));
    let exchanges = Arc::new(Mutex::new(0));
    let grant = McpOAuthAuthorizedGrant::build(
        vec!["tools.call".to_owned()],
        ExactSecretBindingRef::build(
            id(ResourceKind::SecretBinding, 136),
            1,
            id(ResourceKind::SecretProvider, 137),
            contract.binding.token_credential_purpose.clone(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: sha('c'),
            },
        )
        .unwrap(),
        contract.binding.audience_identity_digest.clone(),
        contract
            .auth_profile
            .issuer
            .endpoint_identity_digest
            .clone(),
        sha('d'),
        sha('e'),
        now + ChronoDuration::hours(1),
    )
    .unwrap();
    let ingress = McpOAuthCallbackIngress::new(
        McpOAuthCallbackIngressConfig {
            callback_ingress_generation_id: id(ResourceKind::WorkerProcessGeneration, 138),
            callback_binding_digest: contract.binding.callback_binding_digest.clone(),
            receipt_ttl_seconds: 3_600,
        },
        Arc::new(FixtureStateAuthenticator {
            identity: AuthenticatedMcpOAuthState {
                tenant_id: contract.tenant_id.clone(),
                task_id: contract.task_id.clone(),
            },
        }),
        Arc::new(FixtureCallbackIds),
        Arc::new(FixtureCallbackAuthority {
            contract,
            commits: commits.clone(),
        }),
        Arc::new(FixtureCredentialBroker {
            grant,
            exchanges: exchanges.clone(),
        }),
    )
    .unwrap();

    let outcome = ingress.handle_query(&raw_query, now).await.unwrap();
    assert_eq!(
        outcome.disposition,
        McpOAuthCallbackCommitDisposition::Authorized
    );
    assert!(outcome.no_store);
    assert_eq!(*exchanges.lock().unwrap(), 1);
    let rendered = format!("{:?}", commits.lock().unwrap());
    assert!(!rendered.contains("one-time-code"));
    assert!(!rendered.contains("opaque-state"));
}

#[tokio::test]
async fn oauth_callback_ingress_accepts_only_aead_state_for_its_fixed_callback() {
    let now = Utc::now();
    let (mut contract, _) = oauth_exchange_contract(now);
    let identity = AuthenticatedMcpOAuthState {
        tenant_id: contract.tenant_id.clone(),
        task_id: contract.task_id.clone(),
    };
    let state_codec = Arc::new(
        AeadMcpOAuthStateCodec::new(
            McpOAuthStateCodecConfig {
                active_key_id: "current".to_owned(),
                callback_binding_digest: contract.binding.callback_binding_digest.clone(),
                maximum_lifetime_seconds: 600,
                clock_skew_seconds: 30,
            },
            vec![McpOAuthStateKey {
                key_id: "current".to_owned(),
                key_material: SensitiveMcpOAuthStateKey::new(vec![7; MCP_OAUTH_STATE_KEY_BYTES])
                    .unwrap(),
            }],
        )
        .unwrap(),
    );
    let state = state_codec
        .issue_state(&identity, now, now + ChronoDuration::minutes(5))
        .unwrap();
    contract.binding.state_digest = mcp_oauth_state_digest(&state).unwrap();
    let raw_query = format!(
        "state={}&code=one-time-code&iss=https%3A%2F%2Fauth.example.test",
        std::str::from_utf8(state.as_bytes()).unwrap()
    );
    let grant = McpOAuthAuthorizedGrant::build(
        vec!["tools.call".to_owned()],
        ExactSecretBindingRef::build(
            id(ResourceKind::SecretBinding, 142),
            1,
            id(ResourceKind::SecretProvider, 143),
            contract.binding.token_credential_purpose.clone(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: sha('3'),
            },
        )
        .unwrap(),
        contract.binding.audience_identity_digest.clone(),
        contract
            .auth_profile
            .issuer
            .endpoint_identity_digest
            .clone(),
        sha('4'),
        sha('5'),
        now + ChronoDuration::hours(1),
    )
    .unwrap();
    let commits = Arc::new(Mutex::new(Vec::new()));
    let ingress = McpOAuthCallbackIngress::new(
        McpOAuthCallbackIngressConfig {
            callback_ingress_generation_id: id(ResourceKind::WorkerProcessGeneration, 144),
            callback_binding_digest: contract.binding.callback_binding_digest.clone(),
            receipt_ttl_seconds: 3_600,
        },
        state_codec,
        Arc::new(FixtureCallbackIds),
        Arc::new(FixtureCallbackAuthority {
            contract,
            commits: commits.clone(),
        }),
        Arc::new(FixtureCredentialBroker {
            grant,
            exchanges: Arc::new(Mutex::new(0)),
        }),
    )
    .unwrap();

    assert_eq!(
        ingress
            .handle_query(raw_query.as_bytes(), now)
            .await
            .unwrap()
            .disposition,
        McpOAuthCallbackCommitDisposition::Authorized
    );
    assert_eq!(commits.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn oauth_callback_parser_rejects_duplicates_tokens_and_wrong_state_before_exchange() {
    assert!(parse_mcp_oauth_callback_query(b"state=a&state=b&code=c").is_err());
    assert!(parse_mcp_oauth_callback_query(b"state=a&access_token=b").is_err());
    assert!(parse_mcp_oauth_callback_query(b"state=%GG&code=c").is_err());

    let now = Utc::now();
    let (contract, _) = oauth_exchange_contract(now);
    let exchanges = Arc::new(Mutex::new(0));
    let grant = McpOAuthAuthorizedGrant::build(
        vec!["tools.call".to_owned()],
        ExactSecretBindingRef::build(
            id(ResourceKind::SecretBinding, 139),
            1,
            id(ResourceKind::SecretProvider, 140),
            contract.binding.token_credential_purpose.clone(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: sha('f'),
            },
        )
        .unwrap(),
        contract.binding.audience_identity_digest.clone(),
        contract
            .auth_profile
            .issuer
            .endpoint_identity_digest
            .clone(),
        sha('1'),
        sha('2'),
        now + ChronoDuration::hours(1),
    )
    .unwrap();
    let ingress = McpOAuthCallbackIngress::new(
        McpOAuthCallbackIngressConfig {
            callback_ingress_generation_id: id(ResourceKind::WorkerProcessGeneration, 141),
            callback_binding_digest: contract.binding.callback_binding_digest.clone(),
            receipt_ttl_seconds: 3_600,
        },
        Arc::new(FixtureStateAuthenticator {
            identity: AuthenticatedMcpOAuthState {
                tenant_id: contract.tenant_id.clone(),
                task_id: contract.task_id.clone(),
            },
        }),
        Arc::new(FixtureCallbackIds),
        Arc::new(FixtureCallbackAuthority {
            contract,
            commits: Arc::new(Mutex::new(Vec::new())),
        }),
        Arc::new(FixtureCredentialBroker {
            grant,
            exchanges: exchanges.clone(),
        }),
    )
    .unwrap();
    let error = ingress
        .handle_query(b"state=wrong-state&code=one-time-code", now)
        .await
        .unwrap_err();
    assert_eq!(error.safe_code(), "mcp_oauth_callback_not_found");
    assert_eq!(*exchanges.lock().unwrap(), 0);
}

#[test]
fn authorization_reauth_uses_version_and_generation_fences() {
    let now = Utc::now();
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    let active = authorization_record(&fixture, now);
    let reauth = active
        .transition(1, McpAuthorizationState::ReauthRequired, now)
        .unwrap();
    assert_eq!(reauth.generation, 1);
    assert_eq!(reauth.principal_binding_generation, 1);
    assert_eq!(
        reauth.execution_context(now),
        Err(McpHostError::InvalidAuthorization)
    );

    let restored = reauth
        .reactivate(
            2,
            McpAuthorizationReplacement {
                principal_binding_generation: 2,
                granted_scopes: vec!["tools.call".to_owned()],
                token_secret_binding: rotated_token_binding(&reauth.token_secret_binding, 2, '0'),
                expires_at: now + ChronoDuration::hours(2),
            },
            now,
        )
        .unwrap();
    assert_eq!(restored.generation, 2);
    assert_eq!(restored.version, 3);
    assert_eq!(restored.execution_context(now).unwrap().generation, 2);
    assert_eq!(
        reauth.reactivate(
            1,
            McpAuthorizationReplacement {
                principal_binding_generation: 2,
                granted_scopes: vec!["tools.call".to_owned()],
                token_secret_binding: rotated_token_binding(&reauth.token_secret_binding, 2, '0',),
                expires_at: now + ChronoDuration::hours(2),
            },
            now,
        ),
        Err(McpHostError::InvalidAuthorization)
    );
}

#[test]
fn due_authorization_can_expire_without_refreshing_credentials() {
    let now = Utc::now();
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    let active = authorization_record(&fixture, now);
    let expiry = active.expires_at;
    let expired = active
        .transition(1, McpAuthorizationState::Expired, expiry)
        .unwrap();
    assert_eq!(expired.generation, active.generation);
    assert_eq!(expired.token_secret_binding, active.token_secret_binding);
    assert_eq!(
        expired.execution_context(expiry),
        Err(McpHostError::InvalidAuthorization)
    );
}

#[test]
fn session_key_prevents_cross_principal_and_generation_reuse() {
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    let key = McpSessionBindingKey::build(&fixture.contract).unwrap();
    key.validate_canonical_for(&fixture.contract).unwrap();

    let mut changed = fixture.contract.clone();
    changed.authorization.generation += 1;
    assert_eq!(
        key.validate_for(&changed),
        Err(McpHostError::InvalidSession)
    );
}

#[test]
fn session_record_rejects_a_tampered_nested_binding_key() {
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    let key = McpSessionBindingKey::build(&fixture.contract).unwrap();
    let mut session = McpSessionRecord::disconnected(key).unwrap();
    session.binding_key.principal_binding_generation += 1;
    session.canonical_digest = digest_without_field(&session, "canonical_digest").unwrap();

    assert_eq!(
        session.validate_canonical_at(Utc::now()),
        Err(McpHostError::InvalidSession)
    );
}

#[test]
fn session_state_is_closed_and_opaque_state_is_encrypted() {
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    let key = McpSessionBindingKey::build(&fixture.contract).unwrap();
    let disconnected = McpSessionRecord::disconnected(key).unwrap();
    let connecting = disconnected
        .transition(
            1,
            insight_platform_contracts::McpSessionState::Connecting,
            None,
            None,
            Utc::now(),
        )
        .unwrap();
    assert_eq!(connecting.generation, 1);
    assert_eq!(
        connecting.transition(
            2,
            insight_platform_contracts::McpSessionState::Ready,
            None,
            None,
            Utc::now(),
        ),
        Err(McpHostError::InvalidSession)
    );
}

#[test]
fn tasks_require_profile_negotiation_and_method_gate() {
    let disabled = fixture(false, Effect::ReadOnly, 1_000);
    let mut request = disabled.request;
    request.method = PublishedMcpMethod::TasksGet;
    assert_eq!(
        request.validate_for(&disabled.contract, Utc::now()),
        Err(McpHostError::InvalidOperation)
    );

    let mut enabled = fixture(true, Effect::ReadOnly, 1_000);
    let outcome = McpOperationOutcome::RemoteTask {
        encrypted_state: EncryptedMcpState {
            scheme: "aes256_gcm_v1".to_owned(),
            ciphertext: vec![1, 2, 3],
            key_id: "mcp-state-key".to_owned(),
            key_reference_digest: sha('1'),
            plaintext_digest: sha('4'),
        },
        external_identity_digest: sha('5'),
        next_poll_at: Utc::now() + ChronoDuration::milliseconds(100),
    };
    assert_eq!(
        outcome.validate_for(&enabled.request, &enabled.contract, Utc::now()),
        Err(McpHostError::InvalidOutcome)
    );
    enabled.request.task_requested = true;
    outcome
        .validate_for(&enabled.request, &enabled.contract, Utc::now())
        .unwrap();
}

#[cfg(any())]
#[tokio::test]
async fn wrong_transport_fails_before_dispatch() {
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    let service = McpHostService::new(Arc::new(StaticTransport {
        kind: McpTransportKind::ManagedStdio,
        behavior: StaticBehavior::Outcome(Box::new(completed())),
    }));
    assert_eq!(
        service.execute(&fixture.contract, &fixture.request).await,
        Err(McpHostError::WrongTransport)
    );
}

#[tokio::test]
async fn timeout_and_panic_are_retryable_only_when_replay_is_safe() {
    for behavior in [
        StaticBehavior::Delay(Duration::from_millis(50)),
        StaticBehavior::Panic,
    ] {
        let unsafe_fixture = fixture(false, Effect::NonIdempotentWrite, 10);
        let service = McpHostService::new(Arc::new(StaticTransport {
            kind: McpTransportKind::StreamableHttp,
            behavior: behavior.clone(),
        }));
        assert!(matches!(
            service
                .execute(&unsafe_fixture.contract, &unsafe_fixture.request)
                .await
                .unwrap(),
            McpOperationOutcome::Uncertain { .. }
        ));

        let safe_fixture = fixture(false, Effect::ReadOnly, 10);
        let service = McpHostService::new(Arc::new(StaticTransport {
            kind: McpTransportKind::StreamableHttp,
            behavior,
        }));
        assert!(matches!(
            service
                .execute(&safe_fixture.contract, &safe_fixture.request)
                .await
                .unwrap(),
            McpOperationOutcome::RetryableFailure(_)
        ));
    }
}

#[tokio::test]
async fn transport_failure_phase_controls_retry_vs_uncertain() {
    let fixture = fixture(false, Effect::NonIdempotentWrite, 1_000);
    let before = McpHostService::new(Arc::new(StaticTransport {
        kind: McpTransportKind::StreamableHttp,
        behavior: StaticBehavior::Failure(McpTransportFailure::RetryableBeforeDispatch(
            safe_failure("connect_unavailable"),
        )),
    }));
    assert!(matches!(
        before
            .execute(&fixture.contract, &fixture.request)
            .await
            .unwrap(),
        McpOperationOutcome::RetryableFailure(_)
    ));

    let after = McpHostService::new(Arc::new(StaticTransport {
        kind: McpTransportKind::StreamableHttp,
        behavior: StaticBehavior::Failure(McpTransportFailure::PostDispatchUncertain {
            failure: safe_failure("connection_lost"),
            external_identity_digest: sha('6'),
        }),
    }));
    assert!(matches!(
        after
            .execute(&fixture.contract, &fixture.request)
            .await
            .unwrap(),
        McpOperationOutcome::Uncertain { .. }
    ));
}

#[tokio::test]
async fn transient_poll_failure_preserves_the_exact_remote_task_continuation() {
    let mut fixture = fixture(true, Effect::NonIdempotentWrite, 1_000);
    let encrypted_state = EncryptedMcpState {
        scheme: "aes256_gcm_v1".to_owned(),
        ciphertext: vec![1, 2, 3],
        key_id: "mcp-state-key".to_owned(),
        key_reference_digest: sha('1'),
        plaintext_digest: sha('2'),
    };
    fixture.request.continuation = Some(McpOperationContinuation {
        encrypted_state: encrypted_state.clone(),
        external_identity_digest: sha('3'),
        poll_count: 1,
        elicitation_response: None,
    });
    let service = McpHostService::new(Arc::new(StaticTransport {
        kind: McpTransportKind::StreamableHttp,
        behavior: StaticBehavior::Failure(McpTransportFailure::PostDispatchUncertain {
            failure: safe_failure("task_poll_connection_lost"),
            external_identity_digest: sha('4'),
        }),
    }));
    let McpOperationOutcome::RemoteTask {
        encrypted_state: observed_state,
        external_identity_digest,
        ..
    } = service
        .execute(&fixture.contract, &fixture.request)
        .await
        .unwrap()
    else {
        panic!("a transient poll failure must re-defer the existing Task")
    };
    assert_eq!(observed_state, encrypted_state);
    assert_eq!(external_identity_digest, sha('3'));
}

#[tokio::test]
async fn remote_task_cancel_uses_negotiated_capability_and_cleanup_deadline() {
    let mut fixture = fixture(true, Effect::NonIdempotentWrite, 1_000);
    fixture.request.deadline = Utc::now() - ChronoDuration::milliseconds(1);
    fixture.request.continuation = Some(McpOperationContinuation {
        encrypted_state: EncryptedMcpState {
            scheme: "aes256_gcm_v1".to_owned(),
            ciphertext: vec![1, 2, 3],
            key_id: "mcp-state-key".to_owned(),
            key_reference_digest: sha('1'),
            plaintext_digest: sha('2'),
        },
        external_identity_digest: sha('3'),
        poll_count: 1,
        elicitation_response: None,
    });
    let service = McpHostService::new(Arc::new(StaticTransport {
        kind: McpTransportKind::StreamableHttp,
        behavior: StaticBehavior::Outcome(Box::new(completed())),
    }));
    assert_eq!(
        service
            .cancel_remote_task(
                &fixture.contract,
                &fixture.request,
                Utc::now() + ChronoDuration::seconds(1),
            )
            .await,
        Ok(McpRemoteTaskCancelOutcome::Accepted)
    );

    fixture
        .contract
        .discovery
        .negotiated_capabilities
        .tasks_cancel = false;
    assert_eq!(
        service
            .cancel_remote_task(
                &fixture.contract,
                &fixture.request,
                Utc::now() + ChronoDuration::seconds(1),
            )
            .await,
        Err(McpHostError::InvalidExecutionContract)
    );
}

#[tokio::test]
async fn oversized_result_is_rejected_at_host_boundary() {
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    let result = ClosedJsonValue::build(sha('7'), serde_json::json!("x".repeat(5_000))).unwrap();
    let service = McpHostService::new(Arc::new(StaticTransport {
        kind: McpTransportKind::StreamableHttp,
        behavior: StaticBehavior::Outcome(Box::new(McpOperationOutcome::Completed {
            result,
            evidence_digest: sha('8'),
        })),
    }));
    assert_eq!(
        service.execute(&fixture.contract, &fixture.request).await,
        Err(McpHostError::InvalidOutcome)
    );
}

#[derive(Default)]
struct RecordingHttpConnector {
    request: Mutex<Option<McpStreamableHttpRequest>>,
}

#[async_trait]
impl McpStreamableHttpConnector for RecordingHttpConnector {
    async fn execute(
        &self,
        request: McpStreamableHttpRequest,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        *self.request.lock().unwrap() = Some(request);
        Ok(completed())
    }
}

#[tokio::test]
async fn streamable_http_connector_receives_only_exact_opaque_authority() {
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    let connector = Arc::new(RecordingHttpConnector::default());
    let service = McpHostService::new(Arc::new(StreamableHttpMcpTransport::new(connector.clone())));
    service
        .execute(&fixture.contract, &fixture.request)
        .await
        .unwrap();
    let captured = connector.request.lock().unwrap().clone().unwrap();
    assert_eq!(captured.deployment, fixture.contract.deployment);
    assert_eq!(
        captured.authorization_generation,
        fixture.contract.authorization.generation
    );
    assert_eq!(
        captured.token_secret_binding.secret_binding_id,
        fixture
            .contract
            .authorization
            .token_secret_binding
            .secret_binding_id
    );
    assert_eq!(captured.endpoint.host, "mcp.example.test");
}

#[cfg(any())]
#[derive(Default)]
struct RecordingRunnerBroker {
    request: Mutex<Option<McpManagedRunnerRequest>>,
}

#[cfg(any())]
#[async_trait]
impl ManagedMcpRunnerBroker for RecordingRunnerBroker {
    async fn execute(
        &self,
        request: McpManagedRunnerRequest,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        *self.request.lock().unwrap() = Some(request);
        Ok(completed())
    }
}

#[cfg(any())]
fn managed_stdio_fixture() -> Fixture {
    let mut fixture = fixture(false, Effect::ReadOnly, 1_000);
    let protocol_policy = fixture.contract.server.protocol_policy.clone();
    let package = exact(ResourceKind::SandboxPackageRevision, 30, 'a');
    let runtime = exact(ResourceKind::SandboxRuntimeRevision, 31, 'b');
    let profile = exact(ResourceKind::SandboxProfileRevision, 32, 'c');
    let isolation_policy = exact(ResourceKind::PolicyRevision, 33, 'd');
    let resource_policy = exact(ResourceKind::PolicyRevision, 34, 'e');
    let artifact_io_policy = exact(ResourceKind::PolicyRevision, 35, 'f');
    fixture.contract.deployment_closure.transport = McpTransportBinding::ManagedStdio {
        package,
        runtime,
        profile,
        isolation: SandboxIsolationClass::MicroVm,
        isolation_policy,
        resource_policy,
        artifact_io_policy,
    };
    fixture.contract.server = McpServerExecutionContract::build(
        fixture.contract.server.revision.clone(),
        McpTransportKind::ManagedStdio,
        protocol_policy,
        fixture
            .contract
            .server
            .deployment_credential_requirements
            .clone(),
        fixture
            .contract
            .server
            .authorization_credential_purpose
            .clone(),
        fixture.contract.server.limits,
    )
    .unwrap();
    fixture.contract.protocol_profile.transport_features = McpTransportFeatures {
        streamable_http_get: false,
        streamable_http_sse: false,
        resumable_stream: false,
        session_affinity: true,
        managed_stdio: true,
    };
    let digest = fixture
        .contract
        .protocol_profile
        .canonical_digest()
        .unwrap();
    fixture.contract.server.protocol_policy.semantic_digest = digest.clone();
    fixture
        .contract
        .deployment_closure
        .protocol_policy
        .semantic_digest = digest.clone();
    fixture.contract.discovery.protocol_profile.semantic_digest = digest;
    fixture.contract.server = McpServerExecutionContract::build(
        fixture.contract.server.revision.clone(),
        McpTransportKind::ManagedStdio,
        fixture.contract.deployment_closure.protocol_policy.clone(),
        fixture
            .contract
            .server
            .deployment_credential_requirements
            .clone(),
        fixture
            .contract
            .server
            .authorization_credential_purpose
            .clone(),
        fixture.contract.server.limits,
    )
    .unwrap();
    fixture.contract.discovery = McpDiscoverySnapshot::build(
        fixture.contract.discovery.snapshot_id.clone(),
        fixture.contract.deployment.clone(),
        fixture.contract.server.revision.clone(),
        fixture.contract.server.protocol_policy.clone(),
        fixture.contract.authorization.canonical_digest.clone(),
        MCP_PROTOCOL_BASELINE.to_owned(),
        fixture.contract.discovery.negotiated_capabilities.clone(),
        fixture.contract.discovery.objects_artifact.clone(),
        Utc::now() - ChronoDuration::seconds(1),
        Utc::now() + ChronoDuration::minutes(30),
    )
    .unwrap();
    fixture.contract = McpHostExecutionContract::build(NewMcpHostExecutionContract {
        deployment: fixture.contract.deployment,
        deployment_closure: fixture.contract.deployment_closure,
        server: fixture.contract.server,
        protocol_profile: fixture.contract.protocol_profile,
        authorization: fixture.contract.authorization,
        discovery: fixture.contract.discovery,
    })
    .unwrap();
    fixture.request.snapshot_id = fixture.contract.discovery.snapshot_id.clone();
    fixture
}

#[cfg(any())]
#[tokio::test]
async fn managed_stdio_is_brokered_with_exact_microvm_contract() {
    let fixture = managed_stdio_fixture();
    let broker = Arc::new(RecordingRunnerBroker::default());
    let service = McpHostService::new(Arc::new(ManagedStdioMcpTransport::new(broker.clone())));
    service
        .execute(&fixture.contract, &fixture.request)
        .await
        .unwrap();
    let captured = broker.request.lock().unwrap().clone().unwrap();
    assert_eq!(captured.isolation, SandboxIsolationClass::MicroVm);
    assert_eq!(
        captured.worker_process_generation_id,
        fixture.request.worker_process_generation_id
    );
    assert_eq!(captured.lease_generation, fixture.request.lease_generation);
    assert_eq!(
        captured.authorization_binding_id,
        fixture.contract.authorization.authorization_binding_id
    );
}

#[cfg(any())]
#[test]
fn managed_stdio_logical_operation_binds_physical_fence_only_after_claim() {
    let fixture = managed_stdio_fixture();
    let logical = fixture.request.logical();
    logical.validate_for(&fixture.contract, Utc::now()).unwrap();
    let replacement_generation = id(ResourceKind::WorkerProcessGeneration, 39);
    let bound = logical.bind_physical(replacement_generation.clone(), 7);
    bound.validate_for(&fixture.contract, Utc::now()).unwrap();
    assert_eq!(bound.worker_process_generation_id, replacement_generation);
    assert_eq!(bound.lease_generation, 7);
    assert_eq!(bound.logical(), logical);
}

#[test]
fn discovery_authority_binds_operation_artifact_and_exact_admission() {
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    let now = Utc::now();
    let operation_id = fixture.request.mcp_operation_id.clone();
    let artifact_link_id = id(ResourceKind::ArtifactLink, 41);
    let admission = McpDiscoveryAdmission::build(NewMcpDiscoveryAdmission {
        operation_id: operation_id.clone(),
        job_id: fixture.request.job_id.clone(),
        tenant_id: fixture.contract.authorization.tenant_id.clone(),
        mcp_deployment: fixture.contract.deployment.clone(),
        server_revision: fixture.contract.server.revision.clone(),
        protocol_profile: fixture.contract.server.protocol_policy.clone(),
        authorization_binding_id: fixture
            .contract
            .authorization
            .authorization_binding_id
            .clone(),
        authorization_generation: fixture.contract.authorization.generation,
        authorization_context_digest: fixture.contract.authorization.canonical_digest.clone(),
        principal_id: fixture.contract.authorization.principal_id.clone(),
        artifact_preallocation: discovery_preallocation_for(
            fixture
                .contract
                .discovery
                .objects_artifact
                .artifact_id()
                .clone(),
            artifact_link_id.clone(),
            fixture.contract.discovery.snapshot_id.clone(),
            50,
        ),
        requested_at: now - ChronoDuration::seconds(2),
        deadline: now + ChronoDuration::minutes(10),
    })
    .unwrap();
    let pending = McpDiscoveryOperationPayload::pending(admission).unwrap();
    let completed = pending
        .complete(McpDiscoveryResultBinding {
            snapshot_id: fixture.contract.discovery.snapshot_id.clone(),
            snapshot_digest: fixture.contract.discovery.canonical_digest.clone(),
            objects_artifact: fixture.contract.discovery.objects_artifact.clone(),
            artifact_link_id: artifact_link_id.clone(),
        })
        .unwrap();
    assert!(completed.validate().is_ok());

    let record = McpDiscoverySnapshotRecord::build(NewMcpDiscoverySnapshotRecord {
        tenant_id: fixture.contract.authorization.tenant_id.clone(),
        source_operation_id: operation_id,
        artifact_link_id,
        snapshot: fixture.contract.discovery.clone(),
        completed_at: now,
    })
    .unwrap();
    assert!(record.validate().is_ok());

    let mut tampered = record;
    tampered.source_operation_id = id(ResourceKind::McpOperation, 42);
    assert_eq!(tampered.validate(), Err(McpHostError::InvalidDiscovery));
}

#[test]
fn discovery_preallocation_is_closed_and_digest_bound() {
    let preallocation = discovery_preallocation(80);
    assert!(preallocation.validate().is_ok());

    let mut wrong_kind = preallocation.clone();
    wrong_kind.verification_job_id = id(ResourceKind::Artifact, 82);
    assert_eq!(wrong_kind.validate(), Err(McpHostError::InvalidDiscovery));

    let mut tampered = preallocation;
    tampered.snapshot_id = id(ResourceKind::McpDiscoverySnapshot, 90);
    assert_eq!(tampered.validate(), Err(McpHostError::InvalidDiscovery));
}

#[test]
fn execution_contract_query_is_closed_and_generation_fenced() {
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    let mut query = McpExecutionContractQuery {
        schema_version: 1,
        tenant_id: fixture.contract.authorization.tenant_id.clone(),
        mcp_deployment: fixture.contract.deployment.clone(),
        discovery_snapshot_id: fixture.contract.discovery.snapshot_id.clone(),
        discovery_snapshot_digest: fixture.contract.discovery.canonical_digest.clone(),
        authorization_binding_id: fixture
            .contract
            .authorization
            .authorization_binding_id
            .clone(),
        authorization_generation: fixture.contract.authorization.generation,
        authorization_context_digest: fixture.contract.authorization.canonical_digest.clone(),
        principal_id: fixture.contract.authorization.principal_id.clone(),
    };
    assert!(query.validate().is_ok());
    query.authorization_generation = 0;
    assert_eq!(
        query.validate(),
        Err(McpExecutionContractResolutionError::InvalidQuery)
    );
}

struct StaticDiscoveryTransport {
    candidate: McpDiscoveryCandidate,
}

struct FailingDiscoveryTransport {
    failure: McpTransportFailure,
}

struct StaticDiscoveryResolver {
    resolved: ResolvedMcpDiscoveryExecution,
}

#[async_trait]
impl McpDiscoveryExecutionContractResolver for StaticDiscoveryResolver {
    async fn resolve_mcp_discovery_execution(
        &self,
        _query: &McpDiscoveryContractQuery,
    ) -> Result<ResolvedMcpDiscoveryExecution, McpExecutionContractResolutionError> {
        Ok(self.resolved.clone())
    }
}

enum CapturedDiscoveryMutation {
    Commit(Box<CommitMcpDiscovery>),
    Resolve(Box<ResolveMcpDiscoveryAttempt>),
}

#[derive(Default)]
struct CapturingDiscoveryStore {
    mutation: Mutex<Option<CapturedDiscoveryMutation>>,
}

#[async_trait]
impl McpDiscoveryResultStore for CapturingDiscoveryStore {
    async fn commit_mcp_discovery_result(
        &self,
        command: CommitMcpDiscovery,
    ) -> Result<CommandOutcome<McpDiscoveryOperationRecord>, McpDiscoveryPersistenceError> {
        *self.mutation.lock().unwrap() = Some(CapturedDiscoveryMutation::Commit(Box::new(command)));
        Err(McpDiscoveryPersistenceError::AuthorityUnavailable)
    }

    async fn resolve_mcp_discovery_attempt_result(
        &self,
        command: ResolveMcpDiscoveryAttempt,
    ) -> Result<CommandOutcome<McpDiscoveryOperationRecord>, McpDiscoveryPersistenceError> {
        *self.mutation.lock().unwrap() =
            Some(CapturedDiscoveryMutation::Resolve(Box::new(command)));
        Err(McpDiscoveryPersistenceError::AuthorityUnavailable)
    }
}

#[async_trait]
impl McpDiscoveryTransport for FailingDiscoveryTransport {
    fn kind(&self) -> McpTransportKind {
        McpTransportKind::StreamableHttp
    }

    async fn discover(
        &self,
        _contract: &McpDiscoveryExecutionContract,
        _request: &McpDiscoveryRequest,
    ) -> Result<McpDiscoveryCandidate, McpTransportFailure> {
        Err(self.failure.clone())
    }
}

#[async_trait]
impl McpDiscoveryTransport for StaticDiscoveryTransport {
    fn kind(&self) -> McpTransportKind {
        McpTransportKind::StreamableHttp
    }

    async fn discover(
        &self,
        _contract: &McpDiscoveryExecutionContract,
        _request: &McpDiscoveryRequest,
    ) -> Result<McpDiscoveryCandidate, McpTransportFailure> {
        Ok(self.candidate.clone())
    }
}

#[tokio::test]
async fn discovery_executes_without_a_preexisting_snapshot_and_freezes_candidate() {
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    let contract = McpDiscoveryExecutionContract::build(NewMcpDiscoveryExecutionContract {
        deployment: fixture.contract.deployment.clone(),
        deployment_closure: fixture.contract.deployment_closure.clone(),
        server: fixture.contract.server.clone(),
        protocol_profile: fixture.contract.protocol_profile.clone(),
        authorization: fixture.contract.authorization.clone(),
    })
    .unwrap();
    let now = Utc::now();
    let candidate = McpDiscoveryCandidate::build(
        MCP_PROTOCOL_BASELINE.to_owned(),
        fixture.contract.discovery.negotiated_capabilities.clone(),
        fixture.contract.discovery.objects_artifact.clone(),
        now,
        now + ChronoDuration::minutes(10),
        &contract,
    )
    .unwrap();
    let service = McpDiscoveryService::new(Arc::new(StaticDiscoveryTransport {
        candidate: candidate.clone(),
    }));
    let request = McpDiscoveryRequest {
        schema_version: 1,
        operation_id: fixture.request.mcp_operation_id.clone(),
        tenant_id: fixture.request.tenant_id.clone(),
        job_id: fixture.request.job_id.clone(),
        worker_process_generation_id: fixture.request.worker_process_generation_id.clone(),
        lease_generation: fixture.request.lease_generation,
        physical_attempt: fixture.request.physical_attempt,
        authorization_binding_id: fixture.request.authorization_binding_id.clone(),
        deadline: now + ChronoDuration::minutes(5),
    };
    let McpDiscoveryOutcome::Candidate(observed) =
        service.discover(&contract, &request).await.unwrap()
    else {
        panic!("fixture discovery must return a candidate");
    };
    let snapshot = observed
        .into_snapshot(id(ResourceKind::McpDiscoverySnapshot, 43), &contract)
        .unwrap();
    assert_eq!(snapshot.mcp_deployment, contract.deployment);
    assert_eq!(
        snapshot.authorization_context_digest,
        contract.authorization.canonical_digest
    );

    let mut forbidden = candidate;
    forbidden.negotiated_capabilities.roots = true;
    assert_eq!(
        forbidden.validate_for(&contract),
        Err(McpHostError::InvalidDiscovery)
    );
}

#[tokio::test]
async fn discovery_transport_failure_retains_retry_and_reauth_semantics() {
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    let contract = McpDiscoveryExecutionContract::build(NewMcpDiscoveryExecutionContract {
        deployment: fixture.contract.deployment.clone(),
        deployment_closure: fixture.contract.deployment_closure.clone(),
        server: fixture.contract.server.clone(),
        protocol_profile: fixture.contract.protocol_profile.clone(),
        authorization: fixture.contract.authorization.clone(),
    })
    .unwrap();
    let request = McpDiscoveryRequest {
        schema_version: 1,
        operation_id: fixture.request.mcp_operation_id.clone(),
        tenant_id: fixture.request.tenant_id.clone(),
        job_id: fixture.request.job_id.clone(),
        worker_process_generation_id: fixture.request.worker_process_generation_id.clone(),
        lease_generation: fixture.request.lease_generation,
        physical_attempt: fixture.request.physical_attempt,
        authorization_binding_id: fixture.request.authorization_binding_id.clone(),
        deadline: Utc::now() + ChronoDuration::minutes(5),
    };
    let failure = SafeMcpFailure {
        safe_code: "discovery_unavailable".to_owned(),
        safe_message: "MCP discovery is temporarily unavailable".to_owned(),
        evidence_digest: sha('7'),
    };
    let retrying = McpDiscoveryService::new(Arc::new(FailingDiscoveryTransport {
        failure: McpTransportFailure::PostDispatchUncertain {
            failure: failure.clone(),
            external_identity_digest: sha('6'),
        },
    }));
    assert_eq!(
        retrying.discover(&contract, &request).await.unwrap(),
        McpDiscoveryOutcome::RetryableFailure(failure)
    );

    let challenge_digest = sha('5');
    let reauth = McpDiscoveryService::new(Arc::new(FailingDiscoveryTransport {
        failure: McpTransportFailure::ReauthorizationRequired {
            challenge_digest: challenge_digest.clone(),
        },
    }));
    assert_eq!(
        reauth.discover(&contract, &request).await.unwrap(),
        McpDiscoveryOutcome::ReauthorizationRequired { challenge_digest }
    );
}

#[test]
fn discovery_resolution_is_bound_to_the_exact_running_job_fence() {
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    let now = Utc::now();
    let contract = McpDiscoveryExecutionContract::build(NewMcpDiscoveryExecutionContract {
        deployment: fixture.contract.deployment.clone(),
        deployment_closure: fixture.contract.deployment_closure.clone(),
        server: fixture.contract.server.clone(),
        protocol_profile: fixture.contract.protocol_profile.clone(),
        authorization: fixture.contract.authorization.clone(),
    })
    .unwrap();
    let query = McpDiscoveryContractQuery {
        schema_version: 1,
        tenant_id: fixture.request.tenant_id.clone(),
        operation_id: fixture.request.mcp_operation_id.clone(),
        job_id: fixture.request.job_id.clone(),
        fence: JobFence {
            expected_version: 3,
            worker_process_generation_id: fixture.request.worker_process_generation_id.clone(),
            lease_generation: fixture.request.lease_generation,
            token_digest: sha('9'),
        },
    };
    let resolved = ResolvedMcpDiscoveryExecution {
        operation_version: 1,
        admission_digest: sha('8'),
        artifact_preallocation: discovery_preallocation(60),
        attempt_limit: 3,
        request: McpDiscoveryRequest {
            schema_version: 1,
            operation_id: query.operation_id.clone(),
            tenant_id: query.tenant_id.clone(),
            job_id: query.job_id.clone(),
            worker_process_generation_id: query.fence.worker_process_generation_id.clone(),
            lease_generation: query.fence.lease_generation,
            physical_attempt: 1,
            authorization_binding_id: contract.authorization.authorization_binding_id.clone(),
            deadline: now + ChronoDuration::minutes(5),
        },
        contract,
    };
    assert!(resolved.validate_for(&query, now).is_ok());

    let mut stale = query;
    stale.fence.lease_generation += 1;
    assert_eq!(
        resolved.validate_for(&stale, now),
        Err(McpExecutionContractResolutionError::NotFoundOrChanged)
    );
}

#[tokio::test]
async fn discovery_worker_commits_only_a_fenced_validated_candidate() {
    let fixture = fixture(false, Effect::ReadOnly, 1_000);
    let now = Utc::now();
    let contract = McpDiscoveryExecutionContract::build(NewMcpDiscoveryExecutionContract {
        deployment: fixture.contract.deployment.clone(),
        deployment_closure: fixture.contract.deployment_closure.clone(),
        server: fixture.contract.server.clone(),
        protocol_profile: fixture.contract.protocol_profile.clone(),
        authorization: fixture.contract.authorization.clone(),
    })
    .unwrap();
    let candidate = McpDiscoveryCandidate::build(
        MCP_PROTOCOL_BASELINE.to_owned(),
        fixture.contract.discovery.negotiated_capabilities.clone(),
        fixture.contract.discovery.objects_artifact.clone(),
        now,
        now + ChronoDuration::minutes(10),
        &contract,
    )
    .unwrap();
    let query = McpDiscoveryContractQuery {
        schema_version: 1,
        tenant_id: fixture.request.tenant_id.clone(),
        operation_id: fixture.request.mcp_operation_id.clone(),
        job_id: fixture.request.job_id.clone(),
        fence: JobFence {
            expected_version: 3,
            worker_process_generation_id: fixture.request.worker_process_generation_id.clone(),
            lease_generation: fixture.request.lease_generation,
            token_digest: sha('9'),
        },
    };
    let artifact_preallocation = discovery_preallocation_for(
        fixture
            .contract
            .discovery
            .objects_artifact
            .artifact_id()
            .clone(),
        id(ResourceKind::ArtifactLink, 73),
        id(ResourceKind::McpDiscoverySnapshot, 74),
        70,
    );
    let resolved = ResolvedMcpDiscoveryExecution {
        operation_version: 1,
        admission_digest: sha('8'),
        artifact_preallocation: artifact_preallocation.clone(),
        attempt_limit: 3,
        request: McpDiscoveryRequest {
            schema_version: 1,
            operation_id: query.operation_id.clone(),
            tenant_id: query.tenant_id.clone(),
            job_id: query.job_id.clone(),
            worker_process_generation_id: query.fence.worker_process_generation_id.clone(),
            lease_generation: query.fence.lease_generation,
            physical_attempt: 1,
            authorization_binding_id: contract.authorization.authorization_binding_id.clone(),
            deadline: now + ChronoDuration::minutes(5),
        },
        contract,
    };
    let store = Arc::new(CapturingDiscoveryStore::default());
    let worker = McpDiscoveryWorker::new(
        Arc::new(StaticDiscoveryResolver { resolved }),
        Arc::new(McpDiscoveryService::new(Arc::new(
            StaticDiscoveryTransport { candidate },
        ))),
        store.clone(),
    );
    let mut prepared = worker
        .prepare(ExecuteMcpDiscoveryJob {
            query: query.clone(),
            audit: McpWorkerAudit {
                tenant_id: query.tenant_id.clone(),
                worker_process_generation_id: query.fence.worker_process_generation_id.clone(),
                receipt_id: id(ResourceKind::Receipt, 44),
                event_id: id(ResourceKind::Event, 45),
                outbox_id: id(ResourceKind::OutboxEvent, 46),
                idempotency_key_digest: sha('4'),
                request_digest: sha('3'),
                receipt_expires_at: now + ChronoDuration::hours(1),
            },
            retry_at: now + ChronoDuration::seconds(30),
        })
        .await
        .unwrap();
    let mut invalid_fence = query.fence.clone();
    invalid_fence.expected_version += 1;
    invalid_fence.token_digest = sha('0');
    assert_eq!(
        prepared.refresh_fence(invalid_fence),
        Err(McpDiscoveryWorkerError::InvalidCommand)
    );
    let mut heartbeat_fence = query.fence.clone();
    heartbeat_fence.expected_version += 1;
    prepared.refresh_fence(heartbeat_fence.clone()).unwrap();
    let failure = worker.commit(prepared).await.unwrap_err();
    assert_eq!(
        failure,
        McpDiscoveryWorkerError::Persistence(McpDiscoveryPersistenceError::AuthorityUnavailable)
    );
    let commit = match store.mutation.lock().unwrap().take().unwrap() {
        CapturedDiscoveryMutation::Commit(commit) => *commit,
        CapturedDiscoveryMutation::Resolve(resolve) => panic!(
            "candidate unexpectedly resolved operation {}",
            resolve.operation_id
        ),
    };
    assert_eq!(commit.operation_id, query.operation_id);
    assert_eq!(commit.fence, heartbeat_fence);
    assert_eq!(
        commit.snapshot.snapshot_id,
        artifact_preallocation.snapshot_id
    );
    assert_eq!(
        commit.artifact_link_id,
        artifact_preallocation.artifact_link_id
    );
}

fn subscription_contract(now: DateTime<Utc>) -> McpHostExecutionContract {
    let base = fixture(false, Effect::ReadOnly, 5_000).contract;
    subscription_contract_from(base, now)
}

fn subscription_contract_from(
    base: McpHostExecutionContract,
    now: DateTime<Utc>,
) -> McpHostExecutionContract {
    let mut protocol = base.protocol_profile.clone();
    protocol.allowed_server_capabilities.resources = true;
    protocol.allowed_server_capabilities.subscriptions = true;
    protocol
        .method_limits
        .insert(PublishedMcpMethod::ResourcesRead, method_limits());
    let protocol_ref = ExactVersionRef::new(
        base.server.protocol_policy.revision_id.clone(),
        protocol.canonical_digest().unwrap(),
    )
    .unwrap();
    let mut closure = base.deployment_closure.clone();
    closure.protocol_policy = protocol_ref.clone();
    let server = McpServerExecutionContract::build(
        base.server.revision.clone(),
        base.server.transport,
        protocol_ref.clone(),
        base.server.deployment_credential_requirements.clone(),
        base.server.authorization_credential_purpose.clone(),
        base.server.limits,
    )
    .unwrap();
    let mut negotiated = base.discovery.negotiated_capabilities.clone();
    negotiated.resources = true;
    negotiated.subscriptions = true;
    let discovery = McpDiscoverySnapshot::build(
        base.discovery.snapshot_id.clone(),
        base.deployment.clone(),
        server.revision.clone(),
        protocol_ref,
        base.authorization.canonical_digest.clone(),
        base.discovery.negotiated_version.clone(),
        negotiated,
        base.discovery.objects_artifact.clone(),
        now - ChronoDuration::seconds(1),
        now + ChronoDuration::minutes(30),
    )
    .unwrap();
    McpHostExecutionContract::build(NewMcpHostExecutionContract {
        deployment: base.deployment,
        deployment_closure: closure,
        server,
        protocol_profile: protocol,
        authorization: base.authorization,
        discovery,
    })
    .unwrap()
}

fn subscription_record(
    contract: &McpHostExecutionContract,
    now: DateTime<Utc>,
) -> McpSubscriptionRecord {
    let subscription_id = id(ResourceKind::McpOperation, 0x501);
    let job_id = id(ResourceKind::Job, 0x502);
    let binding = McpResourceSubscriptionBinding::build(
        NewMcpResourceSubscriptionBinding {
            subscription_id: subscription_id.clone(),
            job_id: job_id.clone(),
            context_deployment: ExactDeploymentRef::new(
                id(ResourceKind::ContextDeployment, 0x503),
                sha('5'),
            )
            .unwrap(),
            resource_uri: "mcp://catalog.example/items/42".to_owned(),
        },
        contract,
        now,
    )
    .unwrap();
    let session =
        McpSessionRecord::disconnected(McpSessionBindingKey::build(contract).unwrap()).unwrap();
    McpSubscriptionRecord {
        tenant_id: binding.tenant_id.clone(),
        subscription_id,
        job_id,
        logical_key: "resource-items-42".to_owned(),
        state: McpSubscriptionState::Pending,
        version: 1,
        payload: McpSubscriptionPayload::pending(binding, session).unwrap(),
        deadline: now + ChronoDuration::minutes(20),
        created_at: now,
        updated_at: now,
        terminal_at: None,
    }
}

fn subscription_fence(_record: &McpSubscriptionRecord) -> JobFence {
    JobFence {
        expected_version: 7,
        worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 0x504),
        lease_generation: 3,
        token_digest: sha('6'),
    }
}

fn subscription_audit(
    tenant_id: &ResourceId,
    worker_id: &ResourceId,
    base: u16,
    now: DateTime<Utc>,
) -> McpSubscriptionWorkerAudit {
    McpSubscriptionWorkerAudit {
        trace: insight_platform_contracts::TraceIdentityV1::generate(),
        tenant_id: tenant_id.clone(),
        worker_process_generation_id: worker_id.clone(),
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: sha('7'),
        request_digest: sha('8'),
        receipt_expires_at: now + ChronoDuration::hours(1),
    }
}

fn subscription_audits(
    tenant_id: &ResourceId,
    worker_id: &ResourceId,
    now: DateTime<Utc>,
) -> McpSubscriptionWorkerAudits {
    McpSubscriptionWorkerAudits {
        connecting: subscription_audit(tenant_id, worker_id, 0x510, now),
        initializing: subscription_audit(tenant_id, worker_id, 0x520, now),
        ready: subscription_audit(tenant_id, worker_id, 0x530, now),
        terminal: subscription_audit(tenant_id, worker_id, 0x540, now),
        refresh: subscription_audit(tenant_id, worker_id, 0x550, now),
    }
}

struct StaticSubscriptionResolver {
    resolved: ResolvedMcpSubscriptionExecution,
}

#[async_trait]
impl McpSubscriptionExecutionResolver for StaticSubscriptionResolver {
    async fn resolve_mcp_subscription_execution(
        &self,
        _query: &McpSubscriptionContractQuery,
    ) -> Result<ResolvedMcpSubscriptionExecution, McpExecutionContractResolutionError> {
        Ok(self.resolved.clone())
    }
}

struct StaticSubscriptionTransport {
    kind: McpTransportKind,
    outcome: Result<EstablishedMcpSubscription, McpTransportFailure>,
    calls: Mutex<u32>,
    session_generations: Mutex<Vec<u64>>,
    activation_targets: Option<Arc<Mutex<Vec<McpSessionState>>>>,
}

struct RecordingSubscriptionActivation {
    targets: Option<Arc<Mutex<Vec<McpSessionState>>>>,
}

#[async_trait]
impl McpSubscriptionActivation for RecordingSubscriptionActivation {
    async fn activate(self: Box<Self>) {
        if let Some(targets) = &self.targets {
            assert_eq!(
                targets.lock().unwrap().last(),
                Some(&McpSessionState::Ready)
            );
        }
    }
}

#[async_trait]
impl McpSubscriptionTransport for StaticSubscriptionTransport {
    fn kind(&self) -> McpTransportKind {
        self.kind
    }

    async fn establish(
        &self,
        _contract: &McpHostExecutionContract,
        _binding: &McpResourceSubscriptionBinding,
        session_generation: u64,
        _worker_process_generation_id: &ResourceId,
        _deadline: DateTime<Utc>,
    ) -> Result<PreparedMcpSubscription, McpTransportFailure> {
        *self.calls.lock().unwrap() += 1;
        self.session_generations
            .lock()
            .unwrap()
            .push(session_generation);
        self.outcome.clone().map(|established| {
            PreparedMcpSubscription::new(
                established,
                Box::new(RecordingSubscriptionActivation {
                    targets: self.activation_targets.clone(),
                }),
            )
        })
    }
}

#[derive(Default)]
struct CapturingSubscriptionTarget {
    request: Mutex<Option<McpSubscriptionInvalidationRequest>>,
    reconcile: Mutex<Option<McpSubscriptionReconcileRequest>>,
}

#[derive(Default)]
struct CapturingContextAdmissionAuthority {
    commands: Mutex<Vec<AdmitContextSubscriptionRefresh>>,
}

#[async_trait]
impl ContextSubscriptionAdmissionAuthority for CapturingContextAdmissionAuthority {
    async fn admit_context_subscription_refresh(
        &self,
        command: AdmitContextSubscriptionRefresh,
    ) -> Result<AcceptedContextSubscriptionRefresh, ContextSubscriptionAdmissionError> {
        command.validate_at(Utc::now())?;
        let accepted = AcceptedContextSubscriptionRefresh {
            schema_version: CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
            request_digest: command.request.request_digest.clone(),
            durable_work_digest: sha('9'),
            job_id: id(ResourceKind::Job, 0x5f0),
            accepted_at: Utc::now(),
        };
        self.commands.lock().unwrap().push(command);
        Ok(accepted)
    }
}

#[async_trait]
impl McpSubscriptionInvalidationTarget for CapturingSubscriptionTarget {
    async fn accept_invalidation(
        &self,
        request: McpSubscriptionInvalidationRequest,
    ) -> Result<AcceptedMcpSubscriptionInvalidation, McpSubscriptionInvalidationError> {
        *self.request.lock().unwrap() = Some(request.clone());
        Ok(AcceptedMcpSubscriptionInvalidation {
            request_digest: request.request_digest,
            durable_work_digest: sha('9'),
            accepted_at: Utc::now(),
        })
    }

    async fn accept_reconcile(
        &self,
        request: McpSubscriptionReconcileRequest,
    ) -> Result<AcceptedMcpSubscriptionInvalidation, McpSubscriptionInvalidationError> {
        *self.reconcile.lock().unwrap() = Some(request.clone());
        Ok(AcceptedMcpSubscriptionInvalidation {
            request_digest: request.request_digest,
            durable_work_digest: sha('9'),
            accepted_at: Utc::now(),
        })
    }
}

struct InMemorySubscriptionAuthority {
    record: Mutex<McpSubscriptionRecord>,
    targets: Arc<Mutex<Vec<McpSessionState>>>,
    phase_evidence: Mutex<Vec<Option<Sha256Digest>>>,
    refresh_evidence: Mutex<Option<Sha256Digest>>,
}

impl InMemorySubscriptionAuthority {
    fn new(record: McpSubscriptionRecord) -> Self {
        Self {
            record: Mutex::new(record),
            targets: Arc::new(Mutex::new(Vec::new())),
            phase_evidence: Mutex::new(Vec::new()),
            refresh_evidence: Mutex::new(None),
        }
    }
}

#[async_trait]
impl McpSubscriptionAuthority for InMemorySubscriptionAuthority {
    async fn save_subscription_session(
        &self,
        command: SaveMcpSubscriptionSession,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError> {
        let now = Utc::now();
        command
            .validate_at(now)
            .map_err(|_| McpSubscriptionPersistenceError::InvalidCommand)?;
        let mut current = self.record.lock().unwrap();
        if current.subscription_id != command.subscription_id
            || current.job_id != command.job_id
            || current.version != command.expected_subscription_version
        {
            return Err(McpSubscriptionPersistenceError::Conflict);
        }
        let phase_evidence_digest = command.phase_evidence_digest.clone();
        let (payload, state) = current
            .payload
            .transition_session(
                command.expected_session_version,
                command.target,
                command.encrypted_opaque_session,
                command.expires_at,
                3_600_000,
                now,
            )
            .map_err(|_| McpSubscriptionPersistenceError::InvalidCommand)?;
        current.version += 1;
        current.payload = payload;
        current.state = state;
        current.updated_at = now;
        current.terminal_at = state.is_terminal().then_some(now);
        self.targets.lock().unwrap().push(command.target);
        self.phase_evidence
            .lock()
            .unwrap()
            .push(phase_evidence_digest);
        Ok(CommandOutcome::Applied(current.clone()))
    }

    async fn complete_subscription_refresh(
        &self,
        command: CompleteMcpSubscriptionRefresh,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError> {
        let now = Utc::now();
        command
            .validate_at(now)
            .map_err(|_| McpSubscriptionPersistenceError::InvalidCommand)?;
        let mut current = self.record.lock().unwrap();
        if current.version != command.expected_subscription_version
            || current.subscription_id != command.subscription_id
            || current.job_id != command.job_id
        {
            return Err(McpSubscriptionPersistenceError::Conflict);
        }
        current.payload = current
            .payload
            .acknowledge_invalidation(
                command.expected_session_generation,
                command.expected_event_generation,
                now,
            )
            .map_err(|_| McpSubscriptionPersistenceError::InvalidCommand)?;
        current.version += 1;
        current.updated_at = now;
        *self.refresh_evidence.lock().unwrap() = Some(command.refresh_evidence_digest);
        Ok(CommandOutcome::Applied(current.clone()))
    }

    async fn complete_subscription_reconcile(
        &self,
        command: CompleteMcpSubscriptionReconcile,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError> {
        let now = Utc::now();
        command
            .validate_at(now)
            .map_err(|_| McpSubscriptionPersistenceError::InvalidCommand)?;
        let mut current = self.record.lock().unwrap();
        if current.version != command.expected_subscription_version
            || current.subscription_id != command.subscription_id
            || current.job_id != command.job_id
            || current.payload.session.generation != command.expected_session_generation
            || current.payload.pending_invalidation.is_some()
        {
            return Err(McpSubscriptionPersistenceError::Conflict);
        }
        current.payload = current
            .payload
            .acknowledge_full_reconcile(command.expected_session_generation, now)
            .map_err(|_| McpSubscriptionPersistenceError::InvalidCommand)?;
        current.version += 1;
        current.updated_at = now;
        *self.refresh_evidence.lock().unwrap() = Some(command.reconcile_evidence_digest);
        Ok(CommandOutcome::Applied(current.clone()))
    }
}

fn execute_subscription_command(
    record: &McpSubscriptionRecord,
    fence: JobFence,
    now: DateTime<Utc>,
) -> ExecuteMcpSubscriptionJob {
    ExecuteMcpSubscriptionJob {
        query: McpSubscriptionContractQuery {
            schema_version: 1,
            tenant_id: record.tenant_id.clone(),
            subscription_id: record.subscription_id.clone(),
            job_id: record.job_id.clone(),
            fence: fence.clone(),
        },
        audits: subscription_audits(&record.tenant_id, &fence.worker_process_generation_id, now),
    }
}

#[cfg(any())]
#[tokio::test]
async fn subscription_worker_rejects_managed_stdio_before_transport_dispatch() {
    let now = Utc::now();
    let contract = subscription_contract_from(managed_stdio_fixture().contract, now);
    let record = subscription_record(&contract, now);
    let fence = subscription_fence(&record);
    let authority = Arc::new(InMemorySubscriptionAuthority::new(record.clone()));
    let transport = Arc::new(StaticSubscriptionTransport {
        kind: McpTransportKind::ManagedStdio,
        outcome: Err(McpTransportFailure::RetryableBeforeDispatch(
            SafeMcpFailure {
                safe_code: "managed_subscription_transport_must_not_run".to_owned(),
                safe_message: "managed subscriptions require Sandbox admission".to_owned(),
                evidence_digest: sha('9'),
            },
        )),
        calls: Mutex::new(0),
        session_generations: Mutex::new(Vec::new()),
        activation_targets: None,
    });
    let worker = McpSubscriptionWorker::new(
        Arc::new(StaticSubscriptionResolver {
            resolved: ResolvedMcpSubscriptionExecution {
                record: record.clone(),
                contract,
            },
        }),
        transport.clone(),
        Arc::new(CapturingSubscriptionTarget::default()),
        authority.clone(),
    );

    assert_eq!(
        worker
            .execute(execute_subscription_command(&record, fence, now))
            .await,
        Err(McpSubscriptionWorkerError::InvalidCommand)
    );
    assert_eq!(*transport.calls.lock().unwrap(), 0);
    assert!(authority.targets.lock().unwrap().is_empty());
}

#[tokio::test]
async fn subscription_worker_persists_session_phases_before_ready() {
    let now = Utc::now();
    let contract = subscription_contract(now);
    let record = subscription_record(&contract, now);
    let fence = subscription_fence(&record);
    let authority = Arc::new(InMemorySubscriptionAuthority::new(record.clone()));
    let transport = Arc::new(StaticSubscriptionTransport {
        kind: McpTransportKind::StreamableHttp,
        outcome: Ok(EstablishedMcpSubscription {
            transport_kind: McpTransportKind::StreamableHttp,
            binding_digest: record.payload.binding.canonical_digest.clone(),
            encrypted_opaque_session: EncryptedMcpState {
                scheme: "aes256_gcm_v1".to_owned(),
                ciphertext: b"encrypted-session-canary".to_vec(),
                key_id: "session-key".to_owned(),
                key_reference_digest: sha('1'),
                plaintext_digest: sha('a'),
            },
            established_at: now,
            expires_at: now + ChronoDuration::minutes(5),
            evidence_digest: sha('b'),
        }),
        calls: Mutex::new(0),
        session_generations: Mutex::new(Vec::new()),
        activation_targets: Some(Arc::clone(&authority.targets)),
    });
    let target = Arc::new(CapturingSubscriptionTarget::default());
    let worker = McpSubscriptionWorker::new(
        Arc::new(StaticSubscriptionResolver {
            resolved: ResolvedMcpSubscriptionExecution {
                record: record.clone(),
                contract,
            },
        }),
        transport.clone(),
        target.clone(),
        authority.clone(),
    );
    let result = worker
        .execute(execute_subscription_command(&record, fence, now))
        .await
        .unwrap();
    let McpSubscriptionWorkerResult::Established(outcome) = result else {
        panic!("subscription was not established");
    };
    let ready = match outcome {
        CommandOutcome::Applied(record) | CommandOutcome::Replayed(record) => record,
    };
    assert_eq!(
        authority.targets.lock().unwrap().as_slice(),
        &[
            McpSessionState::Connecting,
            McpSessionState::Initializing,
            McpSessionState::Ready,
        ]
    );
    assert_eq!(*transport.calls.lock().unwrap(), 1);
    assert_eq!(
        transport.session_generations.lock().unwrap().as_slice(),
        &[1]
    );
    assert_eq!(
        authority.phase_evidence.lock().unwrap().as_slice(),
        &[None, None, Some(sha('b'))]
    );
    assert!(target.request.lock().unwrap().is_none());
    assert_eq!(ready.state, McpSubscriptionState::Active);
    assert_eq!(ready.payload.session.state, McpSessionState::Ready);
}

#[tokio::test]
async fn subscription_worker_terminalizes_permanent_establish_failure() {
    let now = Utc::now();
    let contract = subscription_contract(now);
    let record = subscription_record(&contract, now);
    let fence = subscription_fence(&record);
    let authority = Arc::new(InMemorySubscriptionAuthority::new(record.clone()));
    let worker = McpSubscriptionWorker::new(
        Arc::new(StaticSubscriptionResolver {
            resolved: ResolvedMcpSubscriptionExecution {
                record: record.clone(),
                contract,
            },
        }),
        Arc::new(StaticSubscriptionTransport {
            kind: McpTransportKind::StreamableHttp,
            outcome: Err(McpTransportFailure::Permanent(SafeMcpFailure {
                safe_code: "mcp_subscription_rejected".to_owned(),
                safe_message: "MCP subscription was rejected".to_owned(),
                evidence_digest: sha('c'),
            })),
            calls: Mutex::new(0),
            session_generations: Mutex::new(Vec::new()),
            activation_targets: None,
        }),
        Arc::new(CapturingSubscriptionTarget::default()),
        authority.clone(),
    );
    let result = worker
        .execute(execute_subscription_command(&record, fence, now))
        .await
        .unwrap();
    assert!(matches!(
        result,
        McpSubscriptionWorkerResult::Terminalized(_)
    ));
    assert_eq!(
        authority.targets.lock().unwrap().as_slice(),
        &[
            McpSessionState::Connecting,
            McpSessionState::Initializing,
            McpSessionState::Failed,
        ]
    );
    assert_eq!(
        authority.phase_evidence.lock().unwrap().as_slice(),
        &[None, None, Some(sha('c'))]
    );
}

#[tokio::test]
async fn subscription_worker_acknowledges_only_after_durable_refresh_acceptance() {
    let now = Utc::now();
    let contract = subscription_contract(now);
    let mut record = subscription_record(&contract, now);
    for target in [
        McpSessionState::Connecting,
        McpSessionState::Initializing,
        McpSessionState::Ready,
    ] {
        let ready = (target == McpSessionState::Ready).then(|| {
            (
                EncryptedMcpState {
                    scheme: "aes256_gcm_v1".to_owned(),
                    ciphertext: vec![1, 2, 3],
                    key_id: "session-key".to_owned(),
                    key_reference_digest: sha('1'),
                    plaintext_digest: sha('d'),
                },
                now + ChronoDuration::minutes(5),
            )
        });
        let (opaque, expiry) = ready.unzip();
        let (payload, state) = record
            .payload
            .transition_session(
                record.payload.session.version,
                target,
                opaque,
                expiry,
                3_600_000,
                now,
            )
            .unwrap();
        record.payload = payload;
        record.state = state;
        record.version += 1;
    }
    let mut notification = McpNotificationCommit {
        audit: McpNotificationAudit {
            tenant_id: record.tenant_id.clone(),
            receipt_id: id(ResourceKind::Receipt, 0x560),
            event_id: id(ResourceKind::Event, 0x561),
            outbox_id: id(ResourceKind::OutboxEvent, 0x562),
            receipt_expires_at: now + ChronoDuration::hours(1),
        },
        tenant_id: record.tenant_id.clone(),
        subscription_id: record.subscription_id.clone(),
        authorization_generation: record.payload.binding.authorization_generation,
        session_generation: record.payload.session.generation,
        event_key_digest: sha('e'),
        event_generation: 1,
        class: McpNotificationClass::ResourceUpdated,
        resource_uri_digest: Some(record.payload.binding.resource_uri_digest.clone()),
        body_digest: sha('f'),
        wire_bytes: 128,
        received_at: now,
    };
    notification.audit.receipt_expires_at = now + ChronoDuration::hours(1);
    let (payload, disposition) = record
        .payload
        .apply_notification(&notification, now)
        .unwrap();
    assert_eq!(disposition, McpNotificationApplyDisposition::Wake);
    record.payload = payload;
    record.version += 1;
    let fence = subscription_fence(&record);
    let authority = Arc::new(InMemorySubscriptionAuthority::new(record.clone()));
    let target = Arc::new(CapturingSubscriptionTarget::default());
    let worker = McpSubscriptionWorker::new(
        Arc::new(StaticSubscriptionResolver {
            resolved: ResolvedMcpSubscriptionExecution {
                record: record.clone(),
                contract,
            },
        }),
        Arc::new(StaticSubscriptionTransport {
            kind: McpTransportKind::StreamableHttp,
            outcome: Err(McpTransportFailure::RetryableBeforeDispatch(
                SafeMcpFailure {
                    safe_code: "unused_transport".to_owned(),
                    safe_message: "transport must not be used for refresh".to_owned(),
                    evidence_digest: sha('1'),
                },
            )),
            calls: Mutex::new(0),
            session_generations: Mutex::new(Vec::new()),
            activation_targets: None,
        }),
        target.clone(),
        authority.clone(),
    );
    let result = worker
        .execute(execute_subscription_command(&record, fence, now))
        .await
        .unwrap();
    assert!(matches!(
        result,
        McpSubscriptionWorkerResult::RefreshAccepted(_)
    ));
    let request = target.request.lock().unwrap().clone().unwrap();
    assert!(matches!(
        request.reason,
        McpSubscriptionRefreshReason::ResourceUpdated { .. }
    ));
    assert_eq!(
        authority.refresh_evidence.lock().unwrap().as_ref(),
        Some(&sha('9'))
    );
    assert!(authority
        .record
        .lock()
        .unwrap()
        .payload
        .pending_invalidation
        .is_none());
}

#[tokio::test]
async fn context_invalidation_adapter_maps_notification_and_reconcile_without_job_input() {
    let now = Utc::now();
    let contract = subscription_contract(now);
    let mut invalidated = ready_subscription_record(&contract, now);
    let notification = McpNotificationCommit {
        audit: McpNotificationAudit {
            tenant_id: invalidated.tenant_id.clone(),
            receipt_id: id(ResourceKind::Receipt, 0x5e0),
            event_id: id(ResourceKind::Event, 0x5e1),
            outbox_id: id(ResourceKind::OutboxEvent, 0x5e2),
            receipt_expires_at: now + ChronoDuration::hours(1),
        },
        tenant_id: invalidated.tenant_id.clone(),
        subscription_id: invalidated.subscription_id.clone(),
        authorization_generation: invalidated.payload.binding.authorization_generation,
        session_generation: invalidated.payload.session.generation,
        event_key_digest: sha('a'),
        event_generation: 7,
        class: McpNotificationClass::ResourceUpdated,
        resource_uri_digest: Some(invalidated.payload.binding.resource_uri_digest.clone()),
        body_digest: sha('b'),
        wire_bytes: 128,
        received_at: now,
    };
    let (payload, disposition) = invalidated
        .payload
        .apply_notification(&notification, now)
        .unwrap();
    assert_eq!(disposition, McpNotificationApplyDisposition::Wake);
    invalidated.payload = payload;
    invalidated.version += 1;

    let authority = Arc::new(CapturingContextAdmissionAuthority::default());
    let target = ContextSubscriptionInvalidationTarget::new(authority.clone());
    let invalidation = McpSubscriptionInvalidationRequest::build(&invalidated).unwrap();
    let accepted = target
        .accept_invalidation(invalidation.clone())
        .await
        .unwrap();
    assert_eq!(accepted.request_digest, invalidation.request_digest);
    assert_eq!(accepted.durable_work_digest, sha('9'));

    let reconciled = ready_subscription_record(&contract, now);
    let reconcile = McpSubscriptionReconcileRequest::build(&reconciled).unwrap();
    let reconcile_accepted = target.accept_reconcile(reconcile.clone()).await.unwrap();
    assert_eq!(reconcile_accepted.request_digest, reconcile.request_digest);
    assert_eq!(reconcile_accepted.durable_work_digest, sha('9'));

    let commands = authority.commands.lock().unwrap();
    assert_eq!(commands.len(), 2);
    assert_eq!(commands[0].request.resource_uri, invalidation.resource_uri);
    assert_eq!(
        commands[0].request.resource_uri_digest,
        invalidation.resource_uri_digest
    );
    assert!(matches!(
        commands[0].request.cause,
        ContextSubscriptionRefreshCause::ResourceUpdated
    ));
    assert!(matches!(
        commands[1].request.cause,
        ContextSubscriptionRefreshCause::FullReconcile {
            observed_subscription_version
        } if observed_subscription_version == reconciled.version
    ));
    assert_eq!(commands[1].request.event_generation, reconciled.version);
}

fn ready_subscription_record(
    contract: &McpHostExecutionContract,
    now: DateTime<Utc>,
) -> McpSubscriptionRecord {
    let mut record = subscription_record(contract, now);
    for target in [
        McpSessionState::Connecting,
        McpSessionState::Initializing,
        McpSessionState::Ready,
    ] {
        let ready = (target == McpSessionState::Ready).then(|| {
            (
                EncryptedMcpState {
                    scheme: "aes256_gcm_v1".to_owned(),
                    ciphertext: vec![4, 5, 6],
                    key_id: "periodic-session-key".to_owned(),
                    key_reference_digest: sha('1'),
                    plaintext_digest: sha('2'),
                },
                now + ChronoDuration::minutes(5),
            )
        });
        let (opaque, expiry) = ready.unzip();
        let (payload, state) = record
            .payload
            .transition_session(
                record.payload.session.version,
                target,
                opaque,
                expiry,
                3_600_000,
                now,
            )
            .unwrap();
        record.payload = payload;
        record.state = state;
        record.version += 1;
    }
    record
}

#[tokio::test]
async fn subscription_worker_periodic_reconcile_uses_the_durable_target() {
    let now = Utc::now();
    let contract = subscription_contract(now);
    let record = ready_subscription_record(&contract, now);
    let fence = subscription_fence(&record);
    let authority = Arc::new(InMemorySubscriptionAuthority::new(record.clone()));
    let target = Arc::new(CapturingSubscriptionTarget::default());
    let transport = Arc::new(StaticSubscriptionTransport {
        kind: McpTransportKind::StreamableHttp,
        outcome: Err(McpTransportFailure::RetryableBeforeDispatch(
            SafeMcpFailure {
                safe_code: "unused_transport".to_owned(),
                safe_message: "transport must not be used for reconcile".to_owned(),
                evidence_digest: sha('3'),
            },
        )),
        calls: Mutex::new(0),
        session_generations: Mutex::new(Vec::new()),
        activation_targets: None,
    });
    let worker = McpSubscriptionWorker::new(
        Arc::new(StaticSubscriptionResolver {
            resolved: ResolvedMcpSubscriptionExecution {
                record: record.clone(),
                contract,
            },
        }),
        transport.clone(),
        target.clone(),
        authority.clone(),
    );
    let result = worker
        .execute(execute_subscription_command(&record, fence, now))
        .await
        .unwrap();
    assert!(matches!(result, McpSubscriptionWorkerResult::Reconciled(_)));
    let reconcile = target.reconcile.lock().unwrap().clone().unwrap();
    assert_eq!(reconcile.resource_uri, record.payload.binding.resource_uri);
    assert_eq!(reconcile.observed_subscription_version, record.version);
    assert_eq!(*transport.calls.lock().unwrap(), 0);
    assert_eq!(
        authority.refresh_evidence.lock().unwrap().as_ref(),
        Some(&sha('9'))
    );
}

#[tokio::test]
async fn subscription_worker_rebuilds_lost_session_before_full_reconcile() {
    let now = Utc::now();
    let contract = subscription_contract(now);
    let mut record = ready_subscription_record(&contract, now);
    let (payload, state) = record
        .payload
        .rebuild_after_session_loss(record.payload.session.version, now)
        .unwrap();
    record.payload = payload;
    record.state = state;
    record.version += 1;
    let fence = subscription_fence(&record);
    let authority = Arc::new(InMemorySubscriptionAuthority::new(record.clone()));
    let target = Arc::new(CapturingSubscriptionTarget::default());
    let transport = Arc::new(StaticSubscriptionTransport {
        kind: McpTransportKind::StreamableHttp,
        outcome: Ok(EstablishedMcpSubscription {
            transport_kind: McpTransportKind::StreamableHttp,
            binding_digest: record.payload.binding.canonical_digest.clone(),
            encrypted_opaque_session: EncryptedMcpState {
                scheme: "aes256_gcm_v1".to_owned(),
                ciphertext: b"rebuilt-encrypted-session".to_vec(),
                key_id: "rebuilt-session-key".to_owned(),
                key_reference_digest: sha('1'),
                plaintext_digest: sha('4'),
            },
            established_at: now,
            expires_at: now + ChronoDuration::minutes(5),
            evidence_digest: sha('5'),
        }),
        calls: Mutex::new(0),
        session_generations: Mutex::new(Vec::new()),
        activation_targets: None,
    });
    let worker = McpSubscriptionWorker::new(
        Arc::new(StaticSubscriptionResolver {
            resolved: ResolvedMcpSubscriptionExecution {
                record: record.clone(),
                contract,
            },
        }),
        transport.clone(),
        target.clone(),
        authority.clone(),
    );
    let result = worker
        .execute(execute_subscription_command(&record, fence, now))
        .await
        .unwrap();
    assert!(matches!(result, McpSubscriptionWorkerResult::Reconciled(_)));
    assert_eq!(*transport.calls.lock().unwrap(), 1);
    assert!(target.reconcile.lock().unwrap().is_some());
    assert!(
        !authority
            .record
            .lock()
            .unwrap()
            .payload
            .full_reconcile_required
    );
}

struct StaticReconcileAuthority {
    candidates: Vec<DueMcpSubscriptionReconcile>,
    record: McpSubscriptionRecord,
    wakes: Mutex<Vec<WakeMcpSubscriptionReconcile>>,
}

#[async_trait]
impl McpSubscriptionReconcileAuthority for StaticReconcileAuthority {
    async fn list_due_reconciliations(
        &self,
        _scan: McpSubscriptionReconcileScan,
    ) -> Result<Vec<DueMcpSubscriptionReconcile>, McpSubscriptionPersistenceError> {
        Ok(self.candidates.clone())
    }

    async fn wake_reconciliation(
        &self,
        command: WakeMcpSubscriptionReconcile,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError> {
        let is_stale = command.candidate.job_version == 8;
        self.wakes.lock().unwrap().push(command);
        if is_stale {
            Err(McpSubscriptionPersistenceError::Conflict)
        } else {
            Ok(CommandOutcome::Applied(self.record.clone()))
        }
    }
}

#[tokio::test]
async fn subscription_reconcile_driver_is_bounded_and_treats_races_as_stale() {
    let now = Utc::now();
    let contract = subscription_contract(now);
    let record = ready_subscription_record(&contract, now);
    let candidates = [7_u64, 8]
        .into_iter()
        .enumerate()
        .map(|(index, job_version)| DueMcpSubscriptionReconcile {
            tenant_id: record.tenant_id.clone(),
            subscription_id: if index == 0 {
                record.subscription_id.clone()
            } else {
                id(ResourceKind::McpOperation, 0x5a0)
            },
            job_id: if index == 0 {
                record.job_id.clone()
            } else {
                id(ResourceKind::Job, 0x5a1)
            },
            subscription_version: record.version,
            job_version,
            session_generation: record.payload.session.generation,
            not_updated_after: now - ChronoDuration::minutes(1),
            observed_at: now,
        })
        .collect::<Vec<_>>();
    let authority = Arc::new(StaticReconcileAuthority {
        candidates,
        record: record.clone(),
        wakes: Mutex::new(Vec::new()),
    });
    let driver = McpSubscriptionReconcileDriver::new(authority.clone(), 1).unwrap();
    let worker_id = id(ResourceKind::WorkerProcessGeneration, 0x570);
    let outcome = driver
        .drive(DriveMcpSubscriptionReconciliations {
            scan: McpSubscriptionReconcileScan {
                tenant_id: record.tenant_id.clone(),
                limit: 2,
                minimum_idle_milliseconds: 60_000,
            },
            audits: vec![
                subscription_audit(&record.tenant_id, &worker_id, 0x580, now),
                subscription_audit(&record.tenant_id, &worker_id, 0x590, now),
            ],
        })
        .await
        .unwrap();
    assert_eq!(outcome.observed, 2);
    assert_eq!(outcome.scheduled, 1);
    assert_eq!(outcome.stale, 1);
    assert_eq!(authority.wakes.lock().unwrap().len(), 2);
}

struct StaticRecoveryAuthority {
    candidates: Vec<DueMcpSubscriptionRecovery>,
    record: McpSubscriptionRecord,
    recoveries: Mutex<Vec<RecoverDueMcpSubscription>>,
}

#[async_trait]
impl McpSubscriptionRecoveryAuthority for StaticRecoveryAuthority {
    async fn list_due_recoveries(
        &self,
        _scan: McpSubscriptionRecoveryScan,
    ) -> Result<Vec<DueMcpSubscriptionRecovery>, McpSubscriptionPersistenceError> {
        Ok(self.candidates.clone())
    }

    async fn recover_due_subscription(
        &self,
        command: RecoverDueMcpSubscription,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError> {
        let is_stale = command.candidate.job_version == 8;
        self.recoveries.lock().unwrap().push(command);
        if is_stale {
            Err(McpSubscriptionPersistenceError::Conflict)
        } else {
            Ok(CommandOutcome::Applied(self.record.clone()))
        }
    }

    async fn report_session_loss(
        &self,
        _command: ReportMcpSubscriptionSessionLoss,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError> {
        Err(McpSubscriptionPersistenceError::InvalidCommand)
    }
}

#[tokio::test]
async fn subscription_recovery_driver_is_bounded_and_treats_races_as_stale() {
    let now = Utc::now();
    let contract = subscription_contract(now);
    let record = ready_subscription_record(&contract, now);
    let candidates = vec![
        DueMcpSubscriptionRecovery {
            tenant_id: record.tenant_id.clone(),
            subscription_id: record.subscription_id.clone(),
            job_id: record.job_id.clone(),
            subscription_version: record.version,
            session_version: record.payload.session.version,
            session_generation: record.payload.session.generation,
            job_version: 7,
            observed_job_state: JobState::Waiting,
            observed_lease_generation: None,
            observed_lease_expires_at: None,
            observed_session_expires_at: Some(now - ChronoDuration::seconds(1)),
            cause: McpSubscriptionRecoveryCause::ExpiredSession,
            observed_at: now,
        },
        DueMcpSubscriptionRecovery {
            tenant_id: record.tenant_id.clone(),
            subscription_id: id(ResourceKind::McpOperation, 0x5b0),
            job_id: id(ResourceKind::Job, 0x5b1),
            subscription_version: record.version,
            session_version: record.payload.session.version,
            session_generation: record.payload.session.generation,
            job_version: 8,
            observed_job_state: JobState::Running,
            observed_lease_generation: Some(4),
            observed_lease_expires_at: Some(now - ChronoDuration::seconds(1)),
            observed_session_expires_at: record.payload.session.expires_at,
            cause: McpSubscriptionRecoveryCause::ExpiredLease,
            observed_at: now,
        },
    ];
    let authority = Arc::new(StaticRecoveryAuthority {
        candidates,
        record: record.clone(),
        recoveries: Mutex::new(Vec::new()),
    });
    let driver = McpSubscriptionRecoveryDriver::new(authority.clone(), 1).unwrap();
    let worker_id = id(ResourceKind::WorkerProcessGeneration, 0x5b2);
    let outcome = driver
        .drive(DriveMcpSubscriptionRecoveries {
            scan: McpSubscriptionRecoveryScan {
                tenant_id: record.tenant_id.clone(),
                limit: 2,
            },
            audits: vec![
                subscription_audit(&record.tenant_id, &worker_id, 0x5c0, now),
                subscription_audit(&record.tenant_id, &worker_id, 0x5d0, now),
            ],
        })
        .await
        .unwrap();
    assert_eq!(outcome.observed, 2);
    assert_eq!(outcome.recovered, 1);
    assert_eq!(outcome.stale, 1);
    assert_eq!(authority.recoveries.lock().unwrap().len(), 2);
}
