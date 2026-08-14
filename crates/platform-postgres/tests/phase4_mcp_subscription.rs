use chrono::{DateTime, Duration, Utc};
use insight_platform_artifacts::ArtifactReferenceSnapshot;
use insight_platform_contracts::{
    canonical_digest, AllowedMcpServerCapabilities, ArtifactPurpose, ArtifactRef,
    ArtifactReferenceKind, AuthoringPackage, CapabilityEndpointScheme, CodeTrustClass,
    CommandAudit, CommandOutcome, ContextBackendBinding, ContextBackendContract,
    ContextBackendKind, ContextBackendLimits, ContextCitationContract, ContextCitationStrength,
    ContextConsistencyMode, ContextDataPolicyContract, ContextDeploymentClosure,
    ContextImplementationContract, ContextImplementationResourceSpec, ContextInterfaceLimits,
    ContextInterfaceResourceSpec, ContextLocatorKind, ContextPaginationContract,
    ContextRankingContract, DataClassification, DataRegion, DeploymentClosure, ExactDeploymentRef,
    ExactSecretBindingRef, ExactVersionRef, McpAuthPolicyDocument, McpAuthorizationPrincipalKind,
    McpClientCapabilities, McpDiscoverySnapshot, McpMetadataPolicy, McpMethodLimits,
    McpNegotiatedCapabilities, McpOAuthClientAuthenticationKind, McpOAuthEndpoint,
    McpProtocolPolicyDocument, McpServerLimits, McpServerResourceSpec, McpSessionState,
    McpTransportBinding, McpTransportFeatures, Permission, PermissionSet, PolicyKind,
    PolicyResourceSpec, PrincipalBindingsPayload, PrincipalKind, PublishedMcpMethod,
    PublishedVersionPayload, RegistryResourceKind, ResourceDocument, ResourceId, ResourceKind,
    SandboxAbiVersion, SandboxArtifactIoPolicyDocument, SandboxCleanupPolicy,
    SandboxEntrypointKind, SandboxIsolationClass, SandboxIsolationPolicyDocument,
    SandboxNetworkPolicyDocument, SandboxPackageResourceSpec, SandboxProfileResourceSpec,
    SandboxResourcePolicyDocument, SandboxRuntimeFamily, SandboxRuntimeResourceSpec,
    SandboxSecretDeliveryMode, SandboxSecretResolutionPolicyDocument, SecretBindingPayload,
    SecretPurpose, SecretResolutionPolicy, Sha256Digest, TenantConfig, TenantPrincipalPayload,
    ValidationSummary, MCP_PROTOCOL_BASELINE,
};
use insight_platform_jobs::JobFence as DomainJobFence;
use insight_platform_mcp_host::{
    CompleteMcpSubscriptionReconcile, CompleteMcpSubscriptionRefresh,
    CreateMcpResourceSubscription, EncryptedMcpState, ManagedMcpSandboxSessionIdentity,
    McpAuthorizationBindingRecord, McpDiscoveryAdmission, McpDiscoveryOperationPayload,
    McpDiscoveryResultBinding, McpDiscoverySnapshotRecord, McpExecutionContractQuery,
    McpNotificationApplyDisposition, McpNotificationAudit, McpNotificationClass,
    McpNotificationCommit, McpSubscriptionContractQuery, McpSubscriptionExecutionResolver,
    McpSubscriptionReconcileScan, McpSubscriptionRecord, McpSubscriptionRecoveryCause,
    McpSubscriptionRecoveryScan, McpSubscriptionWorkerAudit, NewMcpAuthorizationBinding,
    NewMcpDiscoveryAdmission, NewMcpDiscoverySnapshotRecord, RecoverDueMcpSubscription,
    ReportMcpSubscriptionTransportTermination, SaveMcpSubscriptionSession,
    WakeMcpSubscriptionReconcile,
};
use insight_platform_postgres::{
    repository::{
        ClaimJobs, JobFence as RepositoryJobFence, NewPrincipal, NewSecretBinding, NewTenant,
        NewTenantPrincipal, PgRepository, RepositoryError, TypedPayload,
    },
    verify_schema,
};
use insight_platform_sandbox::{
    AcceptManagedMcpSandboxSession, ManagedMcpSandboxSessionCallback,
    ManagedMcpSandboxSessionRequest, SandboxExecutionPolicyClosure, SandboxNetworkMode,
    SandboxResourceEnvelope, ScopedArtifactGrant, ScopedSecretGrant, SANDBOX_PROTOCOL_VERSION,
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::collections::BTreeMap;

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1cf-32e4-75e1-a9e8-d95ca0f6{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

fn named_digest(name: &str) -> Sha256Digest {
    canonical_digest(&serde_json::json!({"fixture": name}))
        .unwrap()
        .parse()
        .unwrap()
}

fn exact(kind: ResourceKind, suffix: u16, name: &str) -> ExactVersionRef {
    ExactVersionRef::new(id(kind, suffix), named_digest(name)).unwrap()
}

fn artifact(suffix: u16, name: &str) -> ArtifactRef {
    ArtifactRef::new(
        id(ResourceKind::Artifact, suffix),
        named_digest(name),
        128,
        "application/json",
        DataClassification::Internal,
        Some(format!("{name}.json")),
    )
    .unwrap()
}

fn authoring(suffix: u16, name: &str) -> AuthoringPackage {
    AuthoringPackage {
        artifact: artifact(suffix, name),
        manifest_digest: named_digest(&format!("{name}-manifest")),
    }
}

fn validation(name: &str) -> ValidationSummary {
    ValidationSummary {
        validator_digest: named_digest(&format!("{name}-validator")),
        validated_draft_digest: named_digest(&format!("{name}-draft")),
        dependency_closure_digest: named_digest(&format!("{name}-dependencies")),
        security_evidence_digest: named_digest(&format!("{name}-security")),
        warnings: vec![],
    }
}

fn endpoint(host: &str, path: &str) -> insight_platform_contracts::CanonicalHttpEndpoint {
    insight_platform_contracts::CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: host.to_owned(),
        port: 443,
        base_path: path.to_owned(),
    }
}

fn oauth_endpoint(host: &str, path: &str) -> McpOAuthEndpoint {
    let endpoint = endpoint(host, path);
    McpOAuthEndpoint {
        endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
        endpoint,
    }
}

fn method_limits() -> McpMethodLimits {
    McpMethodLimits {
        maximum_request_bytes: 16_384,
        maximum_response_bytes: 65_536,
        maximum_metadata_entries: 32,
        maximum_progress_events: 32,
        maximum_pages: 16,
        minimum_poll_milliseconds: 10,
        maximum_poll_milliseconds: 5_000,
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
            managed_stdio: false,
        },
        client_capabilities: McpClientCapabilities {
            elicitation_form: false,
            elicitation_url: false,
            tasks_elicitation_create: false,
            sampling: false,
            roots: false,
        },
        allowed_server_capabilities: AllowedMcpServerCapabilities {
            tools: false,
            resources: true,
            prompts: false,
            logging: false,
            tasks: false,
            subscriptions: true,
        },
        experimental_features: vec![],
        method_limits: BTreeMap::from([(PublishedMcpMethod::ResourcesRead, method_limits())]),
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

fn managed_protocol_document() -> McpProtocolPolicyDocument {
    let mut protocol = protocol_document();
    protocol.transport_features = McpTransportFeatures {
        streamable_http_get: false,
        streamable_http_sse: false,
        resumable_stream: false,
        session_affinity: true,
        managed_stdio: true,
    };
    protocol
}

fn managed_sandbox_policy_closure(token_purpose: SecretPurpose) -> SandboxExecutionPolicyClosure {
    SandboxExecutionPolicyClosure {
        isolation: SandboxIsolationPolicyDocument {
            schema_version: 1,
            minimum_isolation: SandboxIsolationClass::MicroVm,
            allowed_runtime_families: vec![SandboxRuntimeFamily::ManagedMcpServer],
            allowed_trust_classes: vec![CodeTrustClass::BuiltIn],
            require_hardware_virtualization_for_microvm: true,
            fresh_jail_per_job: true,
            fresh_guest_kernel_per_job: true,
            single_tenant_guest: true,
            single_use_guest: true,
            deny_host_devices: true,
        },
        resource: SandboxResourcePolicyDocument {
            schema_version: 1,
            maximum_cpu_millicores: 1_000,
            maximum_memory_mebibytes: 1_024,
            maximum_pids: 64,
            maximum_files: 256,
            maximum_io_bytes: 16_777_216,
            maximum_stdout_bytes: 1_048_576,
            maximum_stderr_bytes: 1_048_576,
            maximum_result_bytes: 1_048_576,
            maximum_artifact_output_bytes: 0,
            maximum_network_connections: 0,
            maximum_network_request_bytes: 0,
            maximum_network_response_bytes: 0,
            maximum_startup_milliseconds: 30_000,
            maximum_idle_milliseconds: 60_000,
            maximum_wall_milliseconds: 300_000,
            maximum_cleanup_milliseconds: 10_000,
            maximum_wasm_fuel: None,
            maximum_wasm_memory_pages: None,
            swap_disabled: true,
        },
        network: SandboxNetworkPolicyDocument {
            schema_version: 1,
            destinations: vec![],
            maximum_redirects: 0,
            maximum_dns_answers: 8,
            maximum_response_bytes: 1_048_576,
            require_https: true,
            require_tls12_or_newer: true,
            deny_private_addresses: true,
            deny_link_local_addresses: true,
            deny_metadata_addresses: true,
            deny_proxy_environment: true,
            deny_connect_tunnel: true,
            deny_listen: true,
            deny_udp: true,
        },
        artifact_io: SandboxArtifactIoPolicyDocument {
            schema_version: 1,
            allowed_input_media_types: vec!["application/octet-stream".to_owned()],
            allowed_output_media_types: vec![],
            maximum_input_artifacts: 8,
            maximum_output_artifacts: 0,
            deny_symlink: true,
            deny_hardlink: true,
            deny_device: true,
            deny_fifo: true,
            deny_socket: true,
            deny_sparse_file: true,
            archive_expansion_disabled: true,
        },
        secret_resolution: Some(SandboxSecretResolutionPolicyDocument {
            schema_version: 1,
            allowed_purposes: vec![token_purpose],
            maximum_reads_per_binding: 1,
            delivery_mode: SandboxSecretDeliveryMode::BrokeredMemory,
            deny_environment: true,
            deny_argv: true,
            deny_snapshot: true,
            scrub_after_read: true,
        }),
    }
}

fn managed_sandbox_policy_document(
    suffix: u16,
    kind: PolicyKind,
    policies: &SandboxExecutionPolicyClosure,
) -> ResourceDocument {
    let mut document = PolicyResourceSpec {
        authoring_package: authoring(suffix, &format!("managed-policy-{suffix}")),
        contract_digest: named_digest(&format!("managed-policy-contract-{suffix}")),
        dependency_versions: vec![],
        policy_versions: vec![],
        policy_kind: kind,
        rules_digest: named_digest("placeholder"),
        scheduling: None,
        retention: None,
        mcp_protocol: None,
        mcp_auth: None,
        sandbox_isolation: None,
        sandbox_resource: None,
        sandbox_network: None,
        sandbox_artifact_io: None,
        sandbox_secret_resolution: None,
    };
    match kind {
        PolicyKind::Isolation => {
            document.rules_digest = policies.isolation.canonical_digest().unwrap();
            document.sandbox_isolation = Some(policies.isolation.clone());
        }
        PolicyKind::Resource => {
            document.rules_digest = policies.resource.canonical_digest().unwrap();
            document.sandbox_resource = Some(policies.resource.clone());
        }
        PolicyKind::Network => {
            document.rules_digest = policies.network.canonical_digest().unwrap();
            document.sandbox_network = Some(policies.network.clone());
        }
        PolicyKind::ArtifactIo => {
            document.rules_digest = policies.artifact_io.canonical_digest().unwrap();
            document.sandbox_artifact_io = Some(policies.artifact_io.clone());
        }
        PolicyKind::SecretResolution => {
            let secret = policies.secret_resolution.clone().unwrap();
            document.rules_digest = secret.canonical_digest().unwrap();
            document.sandbox_secret_resolution = Some(secret);
        }
        _ => unreachable!("managed Sandbox fixture uses only Sandbox policy kinds"),
    }
    ResourceDocument::Policy(document)
}

fn server_limits() -> McpServerLimits {
    McpServerLimits {
        maximum_message_bytes: 65_536,
        maximum_response_bytes: 1_048_576,
        maximum_headers: 32,
        maximum_sse_event_bytes: 8_192,
        maximum_in_flight: 8,
        maximum_connections: 4,
        maximum_sessions: 4,
        maximum_session_milliseconds: 3_600_000,
        idle_timeout_milliseconds: 30_000,
        initialize_timeout_milliseconds: 30_000,
        request_timeout_milliseconds: 60_000,
        total_timeout_milliseconds: 120_000,
    }
}

fn policy_document(
    suffix: u16,
    kind: PolicyKind,
    rules_digest: Sha256Digest,
    protocol: Option<McpProtocolPolicyDocument>,
    auth: Option<McpAuthPolicyDocument>,
) -> ResourceDocument {
    let mut document = PolicyResourceSpec {
        authoring_package: authoring(suffix, &format!("policy-{suffix}")),
        contract_digest: named_digest(&format!("policy-contract-{suffix}")),
        dependency_versions: vec![],
        policy_versions: vec![],
        policy_kind: kind,
        rules_digest,
        scheduling: None,
        retention: None,
        mcp_protocol: protocol,
        mcp_auth: auth.map(Box::new),
        sandbox_isolation: None,
        sandbox_resource: None,
        sandbox_network: None,
        sandbox_artifact_io: None,
        sandbox_secret_resolution: None,
    };
    let fixture_sandbox =
        managed_sandbox_policy_closure("fixture.policy.secret".parse::<SecretPurpose>().unwrap());
    match kind {
        PolicyKind::Network if document.mcp_protocol.is_none() => {
            document.rules_digest = fixture_sandbox.network.canonical_digest().unwrap();
            document.sandbox_network = Some(fixture_sandbox.network);
        }
        PolicyKind::Resource => {
            document.rules_digest = fixture_sandbox.resource.canonical_digest().unwrap();
            document.sandbox_resource = Some(fixture_sandbox.resource);
        }
        _ => {}
    }
    ResourceDocument::Policy(document)
}

async fn insert_resource(
    pool: &PgPool,
    tenant_id: &ResourceId,
    resource_id: &ResourceId,
    kind: &str,
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
    .bind(kind)
    .bind(payload.schema_version)
    .bind(payload.value)
    .bind(payload.digest)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_version(
    pool: &PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    resource_id: &ResourceId,
    exact: &ExactVersionRef,
    revision_no: i64,
    document: ResourceDocument,
) {
    document.validate().unwrap_or_else(|failure| {
        panic!(
            "invalid fixture ResourceVersion {} revision {}: {failure}",
            exact.revision_id, revision_no
        )
    });
    let payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document,
            validation: validation(&format!("version-{revision_no}")),
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
    .bind(tenant_id.to_string())
    .bind(exact.revision_id.to_string())
    .bind(resource_id.to_string())
    .bind(exact.resource_kind.descriptor().name)
    .bind(revision_no)
    .bind(exact.semantic_digest.to_string())
    .bind(payload.schema_version)
    .bind(payload.value)
    .bind(payload.digest)
    .bind(principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_deployment(
    pool: &PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    deployment_id: &ResourceId,
    resource_id: &ResourceId,
    resource_version_id: &ResourceId,
    closure: &DeploymentClosure,
) -> ExactDeploymentRef {
    let payload = TypedPayload::new(1, closure).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id,
            environment, bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'fixture', $5, $6, $7, $8)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(deployment_id.to_string())
    .bind(resource_id.to_string())
    .bind(resource_version_id.to_string())
    .bind(&payload.digest)
    .bind(payload.schema_version)
    .bind(payload.value)
    .bind(principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    ExactDeploymentRef::new(deployment_id.clone(), payload.digest.parse().unwrap()).unwrap()
}

async fn insert_ready_artifact(
    pool: &PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    reference: &ArtifactRef,
    retention_revision: &ExactVersionRef,
) {
    let blob_id =
        ResourceId::from_uuid_v7(ResourceKind::InternalBlob, reference.artifact_id().uuid())
            .unwrap();
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
    .bind(tenant_id.to_string())
    .bind(blob_id.to_string())
    .bind(named_digest("storage").to_string())
    .bind(named_digest("security-domain").to_string())
    .bind(vec![1_u8, 2, 3])
    .bind(id(ResourceKind::Policy, 0x171).to_string())
    .bind(reference.content_digest().to_string())
    .bind(i64::try_from(reference.byte_length()).unwrap())
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
        ) VALUES ($1, $2, $3, 'mcp_resource', $4, $5, $6, $7, $7, 'ready', 1,
                  1, '{}'::jsonb, $8, $9, $10, $11, $12, $12)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(reference.artifact_id().to_string())
    .bind(blob_id.to_string())
    .bind(reference.classification().as_str())
    .bind(i64::try_from(reference.byte_length()).unwrap())
    .bind(reference.content_digest().to_string())
    .bind(reference.media_type())
    .bind(named_digest("artifact-metadata").to_string())
    .bind(retention_revision.revision_id.to_string())
    .bind(now + Duration::days(1))
    .bind(principal_id.to_string())
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
}

struct Fixture {
    tenant_id: ResourceId,
    other_tenant_id: ResourceId,
    principal_id: ResourceId,
    mcp_deployment: ExactDeploymentRef,
    context_deployment: ExactDeploymentRef,
    auth_policy: ExactVersionRef,
    authorization: McpAuthorizationBindingRecord,
    snapshot: McpDiscoverySnapshot,
    subscription_id: ResourceId,
    job_id: ResourceId,
    deadline: DateTime<Utc>,
}

async fn seed(pool: &PgPool, repository: &PgRepository, now: DateTime<Utc>) -> Fixture {
    let tenant_id = id(ResourceKind::Tenant, 1);
    let other_tenant_id = id(ResourceKind::Tenant, 2);
    let principal_id = id(ResourceKind::Principal, 3);
    for tenant in [&tenant_id, &other_tenant_id] {
        repository
            .create_tenant(NewTenant {
                tenant_id: tenant.to_string(),
                state: "active".to_owned(),
                config: TenantConfig {
                    scheduling_policy: None,
                },
            })
            .await
            .unwrap();
    }
    repository
        .create_principal(NewPrincipal {
            principal_id: principal_id.clone(),
            authentication_authority_digest: named_digest("auth-authority"),
            subject_digest: named_digest("subject"),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    for tenant in [&tenant_id, &other_tenant_id] {
        repository
            .bind_tenant_principal(NewTenantPrincipal {
                tenant_id: tenant.clone(),
                principal_id: principal_id.clone(),
                principal_kind: PrincipalKind::AgentRunner,
                payload: TenantPrincipalPayload {
                    permissions: PermissionSet::new(vec![
                        Permission::ContextRead,
                        Permission::McpRead,
                    ])
                    .unwrap(),
                },
            })
            .await
            .unwrap();
    }

    let policy_resource = id(ResourceKind::Policy, 0x10);
    let server_resource = id(ResourceKind::McpServer, 0x11);
    let context_interface_resource = id(ResourceKind::ContextSourceInterface, 0x12);
    let context_implementation_resource = id(ResourceKind::ContextSourceImplementation, 0x13);
    for (resource, kind) in [
        (&policy_resource, RegistryResourceKind::Policy),
        (&server_resource, RegistryResourceKind::McpServer),
        (
            &context_interface_resource,
            RegistryResourceKind::ContextSourceInterface,
        ),
        (
            &context_implementation_resource,
            RegistryResourceKind::ContextSourceImplementation,
        ),
    ] {
        insert_resource(pool, &tenant_id, resource, kind.as_str()).await;
    }

    let protocol = protocol_document();
    let protocol_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x20),
        protocol.canonical_digest().unwrap(),
    )
    .unwrap();
    let auth_profile = McpAuthPolicyDocument {
        schema_version: 1,
        issuer: oauth_endpoint("auth.example.test", "/"),
        authorization_endpoint: oauth_endpoint("auth.example.test", "/oauth/authorize"),
        token_endpoint: oauth_endpoint("auth.example.test", "/oauth/token"),
        client_id: "insight-platform".to_owned(),
        client_authentication: McpOAuthClientAuthenticationKind::None,
        client_credential_purpose: None,
        pkce_secret_provider_id: id(ResourceKind::SecretProvider, 0x2d),
        token_secret_provider_id: id(ResourceKind::SecretProvider, 0x2e),
        redirect_uri: oauth_endpoint("platform.example.test", "/v1/mcp/oauth/callback"),
        resource_indicator: oauth_endpoint("mcp.example.test", "/mcp"),
        allowed_scopes: vec![
            "resources.read".to_owned(),
            "resources.subscribe".to_owned(),
        ],
        maximum_token_response_bytes: 65_536,
        connect_timeout_milliseconds: 5_000,
        total_timeout_milliseconds: 30_000,
        maximum_clock_skew_seconds: 60,
    };
    let auth_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x21),
        auth_profile.canonical_digest().unwrap(),
    )
    .unwrap();
    let network_policy = exact(ResourceKind::PolicyRevision, 0x22, "network");
    let tls_policy = exact(ResourceKind::PolicyRevision, 0x23, "tls");
    let trust_policy = exact(ResourceKind::PolicyRevision, 0x24, "trust");
    let uri_policy = exact(ResourceKind::PolicyRevision, 0x25, "uri");
    let parser_policy = exact(ResourceKind::PolicyRevision, 0x26, "parser");
    let ranking_policy = exact(ResourceKind::PolicyRevision, 0x27, "ranking");
    let data_policy = exact(ResourceKind::PolicyRevision, 0x28, "data");
    let cache_policy = exact(ResourceKind::PolicyRevision, 0x29, "cache");
    let policy_documents = vec![
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
            auth_policy.clone(),
            policy_document(
                0x81,
                PolicyKind::McpAuth,
                auth_policy.semantic_digest.clone(),
                None,
                Some(auth_profile.clone()),
            ),
        ),
        (
            network_policy.clone(),
            policy_document(
                0x82,
                PolicyKind::Network,
                named_digest("network"),
                None,
                None,
            ),
        ),
        (
            tls_policy.clone(),
            policy_document(0x83, PolicyKind::Tls, named_digest("tls"), None, None),
        ),
        (
            trust_policy.clone(),
            policy_document(0x84, PolicyKind::Trust, named_digest("trust"), None, None),
        ),
        (
            uri_policy.clone(),
            policy_document(0x85, PolicyKind::Resource, named_digest("uri"), None, None),
        ),
        (
            parser_policy.clone(),
            policy_document(0x86, PolicyKind::Parser, named_digest("parser"), None, None),
        ),
        (
            ranking_policy.clone(),
            policy_document(
                0x87,
                PolicyKind::Ranking,
                named_digest("ranking"),
                None,
                None,
            ),
        ),
        (
            data_policy.clone(),
            policy_document(0x88, PolicyKind::DataFlow, named_digest("data"), None, None),
        ),
        (
            cache_policy.clone(),
            policy_document(
                0x89,
                PolicyKind::DataFlow,
                named_digest("cache"),
                None,
                None,
            ),
        ),
    ];
    for (index, (reference, document)) in policy_documents.into_iter().enumerate() {
        insert_version(
            pool,
            &tenant_id,
            &principal_id,
            &policy_resource,
            &reference,
            i64::try_from(index + 1).unwrap(),
            document,
        )
        .await;
    }

    let server_revision = exact(ResourceKind::McpServerRevision, 0x30, "server");
    let token_purpose = "mcp.oauth.token".parse::<SecretPurpose>().unwrap();
    insert_version(
        pool,
        &tenant_id,
        &principal_id,
        &server_resource,
        &server_revision,
        1,
        ResourceDocument::McpServer(McpServerResourceSpec {
            authoring_package: authoring(0x90, "server"),
            contract_digest: named_digest("server-contract"),
            dependency_versions: vec![],
            policy_versions: vec![],
            transport: insight_platform_contracts::McpTransportKind::StreamableHttp,
            protocol_policy: protocol_policy.clone(),
            deployment_credential_requirements: vec![],
            authorization_credential_purpose: Some(token_purpose.clone()),
            limits: server_limits(),
        }),
    )
    .await;

    let interface_revision = exact(
        ResourceKind::ContextSourceInterfaceRevision,
        0x31,
        "context-interface",
    );
    insert_version(
        pool,
        &tenant_id,
        &principal_id,
        &context_interface_resource,
        &interface_revision,
        1,
        ResourceDocument::ContextSourceInterface(ContextInterfaceResourceSpec {
            authoring_package: authoring(0x91, "context-interface"),
            contract_digest: named_digest("context-interface-contract"),
            dependency_versions: vec![],
            policy_versions: vec![],
            query_schema_digest: named_digest("query-schema"),
            filter_schema_digest: named_digest("filter-schema"),
            item_schema_digest: named_digest("item-schema"),
            observation_schema_digest: named_digest("observation-schema"),
            allowed_consistency: vec![ContextConsistencyMode::ExternalObservation],
            citation: ContextCitationContract {
                allowed_strengths: vec![ContextCitationStrength::ObservationOnly],
                locator_kinds: vec![ContextLocatorKind::RemoteOpaque],
                require_content_digest: true,
                maximum_display_label_bytes: 256,
            },
            pagination: ContextPaginationContract {
                maximum_page_size: 100,
                maximum_cursor_bytes: 1_024,
                cursor_ttl_milliseconds: 60_000,
            },
            ranking: ContextRankingContract {
                score_domain_digest: named_digest("score-domain"),
                reranker_contract_digest: None,
                maximum_candidates: 1_000,
            },
            data_policy: ContextDataPolicyContract {
                maximum_classification: DataClassification::Confidential,
                allowed_regions: vec!["cn-east-1".parse::<DataRegion>().unwrap()],
                entitlement_policy: data_policy.clone(),
                cache_policy,
                maximum_retention_milliseconds: 86_400_000,
            },
            limits: ContextInterfaceLimits {
                maximum_query_bytes: 4_096,
                maximum_filter_bytes: 4_096,
                maximum_item_bytes: 65_536,
                maximum_total_bytes: 1_048_576,
                maximum_items: 100,
                maximum_fan_out: 4,
            },
        }),
    )
    .await;
    let implementation_revision = exact(
        ResourceKind::ContextSourceImplementationRevision,
        0x32,
        "context-implementation",
    );
    insert_version(
        pool,
        &tenant_id,
        &principal_id,
        &context_implementation_resource,
        &implementation_revision,
        1,
        ResourceDocument::ContextSourceImplementation(ContextImplementationResourceSpec {
            authoring_package: authoring(0x92, "context-implementation"),
            contract_digest: named_digest("context-implementation-contract"),
            dependency_versions: vec![interface_revision.clone()],
            policy_versions: vec![],
            interface_revision: interface_revision.clone(),
            backend_kind: ContextBackendKind::McpResources,
            contract: ContextImplementationContract {
                backend: ContextBackendContract::McpResources {
                    resource_contract_digest: named_digest("mcp-resource-contract"),
                    uri_policy,
                },
                credential_requirements: vec![],
                limits: ContextBackendLimits {
                    maximum_request_bytes: 65_536,
                    maximum_response_bytes: 1_048_576,
                    maximum_candidates: 1_000,
                    maximum_remote_state_bytes: 65_536,
                    maximum_poll_count: 16,
                    total_timeout_milliseconds: 120_000,
                },
            },
        }),
    )
    .await;

    let objects = artifact(0x100, "mcp-discovery-objects");
    insert_ready_artifact(pool, &tenant_id, &principal_id, &objects, &trust_policy).await;
    let mcp_endpoint = endpoint("mcp.example.test", "/mcp");
    let mcp_closure = insight_platform_contracts::McpDeploymentClosure {
        server_revision: server_revision.clone(),
        server_identity_digest: mcp_endpoint.canonical_digest().unwrap(),
        transport: McpTransportBinding::StreamableHttp {
            endpoint_identity_digest: mcp_endpoint.canonical_digest().unwrap(),
            endpoint: mcp_endpoint,
            network_policy,
            tls_policy,
        },
        protocol_policy: protocol_policy.clone(),
        trust_policy: trust_policy.clone(),
        auth_policy: Some(auth_policy.clone()),
        secret_bindings: vec![],
        conformance_evidence: objects.clone(),
    };
    let mcp_deployment = insert_deployment(
        pool,
        &tenant_id,
        &principal_id,
        &id(ResourceKind::McpDeployment, 0x40),
        &server_resource,
        &server_revision.revision_id,
        &DeploymentClosure::McpServer(mcp_closure.clone()),
    )
    .await;

    let token_binding = ExactSecretBindingRef::build(
        id(ResourceKind::SecretBinding, 0x41),
        1,
        id(ResourceKind::SecretProvider, 0x2e),
        token_purpose.clone(),
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: named_digest("token-version"),
        },
    )
    .unwrap();
    repository
        .create_secret_binding(NewSecretBinding {
            tenant_id: tenant_id.clone(),
            secret_binding_id: token_binding.secret_binding_id.clone(),
            purpose: token_purpose,
            provider_id: token_binding.provider_id.clone(),
            opaque_reference_ciphertext: vec![9, 8, 7],
            key_id: "fixture-key".to_owned(),
            reference_digest: named_digest("token-reference"),
            payload: SecretBindingPayload {
                provider_id: token_binding.provider_id.clone(),
                resolution_policy: token_binding.resolution_policy.clone(),
            },
        })
        .await
        .unwrap();
    let authorization = McpAuthorizationBindingRecord::create(
        NewMcpAuthorizationBinding {
            tenant_id: tenant_id.clone(),
            authorization_binding_id: id(ResourceKind::McpAuthorizationBinding, 0x43),
            mcp_deployment: mcp_deployment.clone(),
            principal_kind: McpAuthorizationPrincipalKind::PerUser,
            principal_id: principal_id.clone(),
            principal_identity_kind: PrincipalKind::AgentRunner,
            principal_binding_generation: 1,
            audience_identity_digest: mcp_closure.server_identity_digest.clone(),
            granted_scopes: vec![
                "resources.read".to_owned(),
                "resources.subscribe".to_owned(),
            ],
            token_secret_binding: token_binding,
            expires_at: now + Duration::hours(4),
        },
        now,
    )
    .unwrap();
    let authorization_payload = TypedPayload::from_versioned(1, &authorization, 262_144).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            version, payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'mcp_authorization_binding', 'active', 'enabled',
                  1, $3, $4, $5)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(authorization.authorization_binding_id.to_string())
    .bind(authorization_payload.schema_version)
    .bind(authorization_payload.value)
    .bind(authorization_payload.digest)
    .execute(pool)
    .await
    .unwrap();

    let authorization_context = authorization.execution_context(now).unwrap();
    let source_operation_id = id(ResourceKind::McpOperation, 0x44);
    let source_job_id = id(ResourceKind::Job, 0x45);
    let snapshot_id = id(ResourceKind::McpDiscoverySnapshot, 0x46);
    let artifact_link_id = id(ResourceKind::ArtifactLink, 0x47);
    let snapshot = McpDiscoverySnapshot::build(
        snapshot_id.clone(),
        mcp_deployment.clone(),
        server_revision.clone(),
        protocol_policy.clone(),
        authorization_context.canonical_digest.clone(),
        MCP_PROTOCOL_BASELINE.to_owned(),
        McpNegotiatedCapabilities {
            tools: false,
            resources: true,
            prompts: false,
            logging: false,
            tasks: false,
            tasks_list: false,
            tasks_cancel: false,
            tasks_tools_call: false,
            elicitation: false,
            sampling: false,
            roots: false,
            subscriptions: true,
        },
        objects.clone(),
        now - Duration::seconds(2),
        now + Duration::hours(2),
    )
    .unwrap();
    let admission = McpDiscoveryAdmission::build(NewMcpDiscoveryAdmission {
        operation_id: source_operation_id.clone(),
        job_id: source_job_id,
        tenant_id: tenant_id.clone(),
        mcp_deployment: mcp_deployment.clone(),
        server_revision: server_revision.clone(),
        protocol_profile: protocol_policy,
        authorization_binding_id: authorization.authorization_binding_id.clone(),
        authorization_generation: authorization.generation,
        authorization_context_digest: authorization_context.canonical_digest.clone(),
        principal_id: principal_id.clone(),
        requested_at: now - Duration::seconds(3),
        deadline: now + Duration::hours(2),
    })
    .unwrap();
    let operation_payload = McpDiscoveryOperationPayload::pending(admission)
        .unwrap()
        .complete(McpDiscoveryResultBinding {
            snapshot_id: snapshot_id.clone(),
            snapshot_digest: snapshot.canonical_digest.clone(),
            objects_artifact: objects.clone(),
            artifact_link_id: artifact_link_id.clone(),
        })
        .unwrap();
    let operation_payload = TypedPayload::from_versioned(1, &operation_payload, 1_048_576).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.invocations (
            tenant_id, invocation_id, invocation_kind, owner_kind, owner_id,
            logical_key, deployment_id, state, payload_schema_version, payload,
            payload_digest, deadline, terminal_at, created_at, updated_at
        ) VALUES ($1, $2, 'mcp_discovery', 'mcp_operation', $2,
                  'fixture-discovery', $3, 'succeeded', $4, $5, $6, $7, $8, $8, $8)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(source_operation_id.to_string())
    .bind(mcp_deployment.deployment_id.to_string())
    .bind(operation_payload.schema_version)
    .bind(operation_payload.value)
    .bind(operation_payload.digest)
    .bind(now + Duration::hours(2))
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    let reference = ArtifactReferenceSnapshot {
        schema_version: 1,
        artifact_id: objects.artifact_id().clone(),
        owner_id: source_operation_id.clone(),
        reference_kind: ArtifactReferenceKind::Evidence,
        purpose: ArtifactPurpose::McpResource,
        created_by: principal_id.clone(),
    };
    let link_payload = TypedPayload::from_versioned(1, &reference, 262_144).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_links (
            tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
            target_artifact_id, link_key_digest, state, payload_schema_version,
            payload, payload_digest
        ) VALUES ($1, $2, 'reference', 'mcp_operation', $3,
                  $4, $5, 'active', $6, $7, $8)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_link_id.to_string())
    .bind(source_operation_id.to_string())
    .bind(objects.artifact_id().to_string())
    .bind(reference.link_key_digest().unwrap().to_string())
    .bind(link_payload.schema_version)
    .bind(link_payload.value)
    .bind(link_payload.digest)
    .execute(pool)
    .await
    .unwrap();
    let snapshot_record = McpDiscoverySnapshotRecord::build(NewMcpDiscoverySnapshotRecord {
        tenant_id: tenant_id.clone(),
        source_operation_id,
        artifact_link_id,
        snapshot: snapshot.clone(),
        completed_at: now,
    })
    .unwrap();
    let snapshot_payload = TypedPayload::from_versioned(1, &snapshot_record, 1_048_576).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            version, payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'mcp_discovery_snapshot', 'active', 'enabled',
                  1, $3, $4, $5)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(snapshot_id.to_string())
    .bind(snapshot_payload.schema_version)
    .bind(snapshot_payload.value)
    .bind(snapshot_payload.digest)
    .execute(pool)
    .await
    .unwrap();

    let context_closure = ContextDeploymentClosure {
        implementation: implementation_revision,
        interface: interface_revision.clone(),
        backend: ContextBackendBinding::McpResources {
            mcp_deployment: mcp_deployment.clone(),
            discovery_snapshot_id: snapshot.snapshot_id.clone(),
            discovery_snapshot_digest: snapshot.canonical_digest.clone(),
        },
        secret_bindings: vec![],
        network_policy: None,
        parser_policy,
        ranking_policy,
        data_policy,
        conformance_evidence: objects,
    };
    let context_deployment = insert_deployment(
        pool,
        &tenant_id,
        &principal_id,
        &id(ResourceKind::ContextDeployment, 0x48),
        &context_interface_resource,
        &interface_revision.revision_id,
        &DeploymentClosure::ContextSourceInterface(context_closure),
    )
    .await;

    Fixture {
        tenant_id,
        other_tenant_id,
        principal_id,
        mcp_deployment,
        context_deployment,
        auth_policy,
        authorization,
        snapshot,
        subscription_id: id(ResourceKind::McpOperation, 0x49),
        job_id: id(ResourceKind::Job, 0x4a),
        deadline: now + Duration::hours(1),
    }
}

struct ManagedSandboxFixture {
    subscription: Fixture,
    runtime_revision: ExactVersionRef,
    runtime: SandboxRuntimeResourceSpec,
    package_revision: ExactVersionRef,
    package: SandboxPackageResourceSpec,
    profile_revision: ExactVersionRef,
    profile: SandboxProfileResourceSpec,
    policies: SandboxExecutionPolicyClosure,
}

async fn seed_managed_sandbox_variant(
    pool: &PgPool,
    base: &Fixture,
    now: DateTime<Utc>,
) -> ManagedSandboxFixture {
    let policy_resource = id(ResourceKind::Policy, 0x10);
    let managed_protocol = managed_protocol_document();
    let protocol_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x500),
        managed_protocol.canonical_digest().unwrap(),
    )
    .unwrap();
    insert_version(
        pool,
        &base.tenant_id,
        &base.principal_id,
        &policy_resource,
        &protocol_policy,
        11,
        policy_document(
            0x501,
            PolicyKind::Protocol,
            protocol_policy.semantic_digest.clone(),
            Some(managed_protocol),
            None,
        ),
    )
    .await;

    let token_purpose = base.authorization.token_secret_binding.purpose.clone();
    let policies = managed_sandbox_policy_closure(token_purpose.clone());
    let isolation_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x502),
        policies.isolation.canonical_digest().unwrap(),
    )
    .unwrap();
    let resource_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x503),
        policies.resource.canonical_digest().unwrap(),
    )
    .unwrap();
    let network_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x504),
        policies.network.canonical_digest().unwrap(),
    )
    .unwrap();
    let artifact_io_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x505),
        policies.artifact_io.canonical_digest().unwrap(),
    )
    .unwrap();
    let secret_policy_document = policies.secret_resolution.as_ref().unwrap();
    let secret_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x506),
        secret_policy_document.canonical_digest().unwrap(),
    )
    .unwrap();
    for (index, (reference, kind)) in [
        (isolation_policy.clone(), PolicyKind::Isolation),
        (resource_policy.clone(), PolicyKind::Resource),
        (network_policy.clone(), PolicyKind::Network),
        (artifact_io_policy.clone(), PolicyKind::ArtifactIo),
        (secret_policy.clone(), PolicyKind::SecretResolution),
    ]
    .into_iter()
    .enumerate()
    {
        insert_version(
            pool,
            &base.tenant_id,
            &base.principal_id,
            &policy_resource,
            &reference,
            i64::try_from(index + 12).unwrap(),
            managed_sandbox_policy_document(u16::try_from(0x510 + index).unwrap(), kind, &policies),
        )
        .await;
    }

    let runtime_resource = id(ResourceKind::SandboxRuntime, 0x520);
    let package_resource = id(ResourceKind::SandboxPackage, 0x521);
    let profile_resource = id(ResourceKind::SandboxProfile, 0x522);
    for (resource_id, kind) in [
        (&runtime_resource, RegistryResourceKind::SandboxRuntime),
        (&package_resource, RegistryResourceKind::SandboxPackage),
        (&profile_resource, RegistryResourceKind::SandboxProfile),
    ] {
        insert_resource(pool, &base.tenant_id, resource_id, kind.as_str()).await;
    }
    let runtime_revision = exact(
        ResourceKind::SandboxRuntimeRevision,
        0x523,
        "managed-runtime",
    );
    let package_revision = exact(
        ResourceKind::SandboxPackageRevision,
        0x524,
        "managed-package",
    );
    let profile_revision = exact(
        ResourceKind::SandboxProfileRevision,
        0x525,
        "managed-profile",
    );
    let runtime = SandboxRuntimeResourceSpec {
        authoring_package: authoring(0x526, "managed-runtime"),
        contract_digest: named_digest("managed-runtime-contract"),
        dependency_versions: vec![],
        policy_versions: vec![],
        runtime_family: SandboxRuntimeFamily::ManagedMcpServer,
        runtime_version: "managed-mcp-runner-1.0.0".to_owned(),
        image_or_module_digest: named_digest("managed-runtime-image"),
        guest_kernel_digest: Some(named_digest("managed-guest-kernel")),
        guest_agent_digest: named_digest("managed-guest-agent"),
        supported_isolation: vec![SandboxIsolationClass::MicroVm],
        abi: SandboxAbiVersion::V1,
        builtin_modules_manifest_digest: named_digest("managed-builtins"),
        sbom_artifact: artifact(0x527, "managed-runtime-sbom"),
        provenance_evidence: artifact(0x528, "managed-runtime-provenance"),
        semantic_digest: named_digest("managed-runtime-semantic"),
    };
    let runtime_bundle = ArtifactRef::new(
        id(ResourceKind::Artifact, 0x529),
        named_digest("managed-runtime-bundle"),
        128,
        "application/octet-stream",
        DataClassification::Internal,
        Some("managed-mcp-server.bin".to_owned()),
    )
    .unwrap();
    let package = SandboxPackageResourceSpec {
        authoring_package: authoring(0x52a, "managed-package"),
        contract_digest: named_digest("managed-package-contract"),
        dependency_versions: vec![runtime_revision.clone()],
        policy_versions: vec![],
        source_artifact: artifact(0x52b, "managed-package-source"),
        source_digest: named_digest("managed-package-source-digest"),
        runtime_revision: runtime_revision.clone(),
        entrypoint_kind: SandboxEntrypointKind::ManagedMcpServer,
        entrypoint: "bin/mcp-server".to_owned(),
        dependency_lock_digest: named_digest("managed-package-lock"),
        runtime_bundle_artifact: runtime_bundle.clone(),
        build_evidence: artifact(0x52c, "managed-package-build"),
        trust_class: CodeTrustClass::BuiltIn,
        package_digest: named_digest("managed-package-digest"),
    };
    let profile = SandboxProfileResourceSpec {
        authoring_package: authoring(0x52d, "managed-profile"),
        contract_digest: named_digest("managed-profile-contract"),
        dependency_versions: vec![],
        policy_versions: vec![
            isolation_policy.clone(),
            resource_policy.clone(),
            network_policy.clone(),
            artifact_io_policy.clone(),
            secret_policy.clone(),
        ],
        allowed_trust_classes: vec![CodeTrustClass::BuiltIn],
        allowed_runtime_families: vec![SandboxRuntimeFamily::ManagedMcpServer],
        minimum_isolation: SandboxIsolationClass::MicroVm,
        isolation_policy: isolation_policy.clone(),
        resource_policy: resource_policy.clone(),
        network_policy,
        artifact_io_policy: artifact_io_policy.clone(),
        secret_policy: Some(secret_policy),
        cleanup: SandboxCleanupPolicy::SingleUseDestroy,
        max_job_duration_milliseconds: 300_000,
        semantic_digest: named_digest("managed-profile-semantic"),
    };
    for (resource_id, revision, revision_no, document) in [
        (
            &runtime_resource,
            &runtime_revision,
            1,
            ResourceDocument::SandboxRuntime(runtime.clone()),
        ),
        (
            &package_resource,
            &package_revision,
            1,
            ResourceDocument::SandboxPackage(package.clone()),
        ),
        (
            &profile_resource,
            &profile_revision,
            1,
            ResourceDocument::SandboxProfile(profile.clone()),
        ),
    ] {
        insert_version(
            pool,
            &base.tenant_id,
            &base.principal_id,
            resource_id,
            revision,
            revision_no,
            document,
        )
        .await;
    }
    let trust_policy = exact(ResourceKind::PolicyRevision, 0x24, "trust");
    insert_ready_artifact(
        pool,
        &base.tenant_id,
        &base.principal_id,
        &runtime_bundle,
        &trust_policy,
    )
    .await;

    let server_resource = id(ResourceKind::McpServer, 0x530);
    insert_resource(
        pool,
        &base.tenant_id,
        &server_resource,
        RegistryResourceKind::McpServer.as_str(),
    )
    .await;
    let server_revision = exact(ResourceKind::McpServerRevision, 0x531, "managed-server");
    insert_version(
        pool,
        &base.tenant_id,
        &base.principal_id,
        &server_resource,
        &server_revision,
        1,
        ResourceDocument::McpServer(McpServerResourceSpec {
            authoring_package: authoring(0x532, "managed-server"),
            contract_digest: named_digest("managed-server-contract"),
            dependency_versions: vec![],
            policy_versions: vec![],
            transport: insight_platform_contracts::McpTransportKind::ManagedStdio,
            protocol_policy: protocol_policy.clone(),
            deployment_credential_requirements: vec![],
            authorization_credential_purpose: Some(token_purpose),
            limits: server_limits(),
        }),
    )
    .await;
    let server_identity_digest = endpoint("mcp.example.test", "/mcp")
        .canonical_digest()
        .unwrap();
    let objects = base.snapshot.objects_artifact.clone();
    let mcp_closure = insight_platform_contracts::McpDeploymentClosure {
        server_revision: server_revision.clone(),
        server_identity_digest: server_identity_digest.clone(),
        transport: McpTransportBinding::ManagedStdio {
            package: package_revision.clone(),
            runtime: runtime_revision.clone(),
            profile: profile_revision.clone(),
            isolation: SandboxIsolationClass::MicroVm,
            isolation_policy,
            resource_policy,
            artifact_io_policy,
        },
        protocol_policy: protocol_policy.clone(),
        trust_policy,
        auth_policy: Some(base.auth_policy.clone()),
        secret_bindings: vec![],
        conformance_evidence: objects.clone(),
    };
    let mcp_deployment = insert_deployment(
        pool,
        &base.tenant_id,
        &base.principal_id,
        &id(ResourceKind::McpDeployment, 0x533),
        &server_resource,
        &server_revision.revision_id,
        &DeploymentClosure::McpServer(mcp_closure.clone()),
    )
    .await;

    let authorization = McpAuthorizationBindingRecord::create(
        NewMcpAuthorizationBinding {
            tenant_id: base.tenant_id.clone(),
            authorization_binding_id: id(ResourceKind::McpAuthorizationBinding, 0x534),
            mcp_deployment: mcp_deployment.clone(),
            principal_kind: McpAuthorizationPrincipalKind::PerUser,
            principal_id: base.principal_id.clone(),
            principal_identity_kind: PrincipalKind::AgentRunner,
            principal_binding_generation: 1,
            audience_identity_digest: server_identity_digest,
            granted_scopes: vec![
                "resources.read".to_owned(),
                "resources.subscribe".to_owned(),
            ],
            token_secret_binding: base.authorization.token_secret_binding.clone(),
            expires_at: now + Duration::hours(4),
        },
        now,
    )
    .unwrap();
    let authorization_payload = TypedPayload::from_versioned(1, &authorization, 262_144).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            version, payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'mcp_authorization_binding', 'active', 'enabled',
                  1, $3, $4, $5)
        "#,
    )
    .bind(base.tenant_id.to_string())
    .bind(authorization.authorization_binding_id.to_string())
    .bind(authorization_payload.schema_version)
    .bind(authorization_payload.value)
    .bind(authorization_payload.digest)
    .execute(pool)
    .await
    .unwrap();

    let authorization_context = authorization.execution_context(now).unwrap();
    let source_operation_id = id(ResourceKind::McpOperation, 0x535);
    let snapshot_id = id(ResourceKind::McpDiscoverySnapshot, 0x536);
    let artifact_link_id = id(ResourceKind::ArtifactLink, 0x537);
    let snapshot = McpDiscoverySnapshot::build(
        snapshot_id.clone(),
        mcp_deployment.clone(),
        server_revision.clone(),
        protocol_policy.clone(),
        authorization_context.canonical_digest.clone(),
        MCP_PROTOCOL_BASELINE.to_owned(),
        McpNegotiatedCapabilities {
            tools: false,
            resources: true,
            prompts: false,
            logging: false,
            tasks: false,
            tasks_list: false,
            tasks_cancel: false,
            tasks_tools_call: false,
            elicitation: false,
            sampling: false,
            roots: false,
            subscriptions: true,
        },
        objects.clone(),
        now - Duration::seconds(2),
        now + Duration::hours(2),
    )
    .unwrap();
    let admission = McpDiscoveryAdmission::build(NewMcpDiscoveryAdmission {
        operation_id: source_operation_id.clone(),
        job_id: id(ResourceKind::Job, 0x538),
        tenant_id: base.tenant_id.clone(),
        mcp_deployment: mcp_deployment.clone(),
        server_revision: server_revision.clone(),
        protocol_profile: protocol_policy,
        authorization_binding_id: authorization.authorization_binding_id.clone(),
        authorization_generation: authorization.generation,
        authorization_context_digest: authorization_context.canonical_digest.clone(),
        principal_id: base.principal_id.clone(),
        requested_at: now - Duration::seconds(3),
        deadline: now + Duration::hours(2),
    })
    .unwrap();
    let operation_payload = McpDiscoveryOperationPayload::pending(admission)
        .unwrap()
        .complete(McpDiscoveryResultBinding {
            snapshot_id: snapshot_id.clone(),
            snapshot_digest: snapshot.canonical_digest.clone(),
            objects_artifact: objects.clone(),
            artifact_link_id: artifact_link_id.clone(),
        })
        .unwrap();
    let operation_payload = TypedPayload::from_versioned(1, &operation_payload, 1_048_576).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.invocations (
            tenant_id, invocation_id, invocation_kind, owner_kind, owner_id,
            logical_key, deployment_id, state, payload_schema_version, payload,
            payload_digest, deadline, terminal_at, created_at, updated_at
        ) VALUES ($1, $2, 'mcp_discovery', 'mcp_operation', $2,
                  'managed-fixture-discovery', $3, 'succeeded', $4, $5, $6, $7, $8, $8, $8)
        "#,
    )
    .bind(base.tenant_id.to_string())
    .bind(source_operation_id.to_string())
    .bind(mcp_deployment.deployment_id.to_string())
    .bind(operation_payload.schema_version)
    .bind(operation_payload.value)
    .bind(operation_payload.digest)
    .bind(now + Duration::hours(2))
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    let reference = ArtifactReferenceSnapshot {
        schema_version: 1,
        artifact_id: objects.artifact_id().clone(),
        owner_id: source_operation_id.clone(),
        reference_kind: ArtifactReferenceKind::Evidence,
        purpose: ArtifactPurpose::McpResource,
        created_by: base.principal_id.clone(),
    };
    let link_payload = TypedPayload::from_versioned(1, &reference, 262_144).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_links (
            tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
            target_artifact_id, link_key_digest, state, payload_schema_version,
            payload, payload_digest
        ) VALUES ($1, $2, 'reference', 'mcp_operation', $3,
                  $4, $5, 'active', $6, $7, $8)
        "#,
    )
    .bind(base.tenant_id.to_string())
    .bind(artifact_link_id.to_string())
    .bind(source_operation_id.to_string())
    .bind(objects.artifact_id().to_string())
    .bind(reference.link_key_digest().unwrap().to_string())
    .bind(link_payload.schema_version)
    .bind(link_payload.value)
    .bind(link_payload.digest)
    .execute(pool)
    .await
    .unwrap();
    let snapshot_record = McpDiscoverySnapshotRecord::build(NewMcpDiscoverySnapshotRecord {
        tenant_id: base.tenant_id.clone(),
        source_operation_id,
        artifact_link_id,
        snapshot: snapshot.clone(),
        completed_at: now,
    })
    .unwrap();
    let snapshot_payload = TypedPayload::from_versioned(1, &snapshot_record, 1_048_576).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            version, payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'mcp_discovery_snapshot', 'active', 'enabled',
                  1, $3, $4, $5)
        "#,
    )
    .bind(base.tenant_id.to_string())
    .bind(snapshot_id.to_string())
    .bind(snapshot_payload.schema_version)
    .bind(snapshot_payload.value)
    .bind(snapshot_payload.digest)
    .execute(pool)
    .await
    .unwrap();

    let interface_revision = exact(
        ResourceKind::ContextSourceInterfaceRevision,
        0x31,
        "context-interface",
    );
    let context_closure = ContextDeploymentClosure {
        implementation: exact(
            ResourceKind::ContextSourceImplementationRevision,
            0x32,
            "context-implementation",
        ),
        interface: interface_revision.clone(),
        backend: ContextBackendBinding::McpResources {
            mcp_deployment: mcp_deployment.clone(),
            discovery_snapshot_id: snapshot.snapshot_id.clone(),
            discovery_snapshot_digest: snapshot.canonical_digest.clone(),
        },
        secret_bindings: vec![],
        network_policy: None,
        parser_policy: exact(ResourceKind::PolicyRevision, 0x26, "parser"),
        ranking_policy: exact(ResourceKind::PolicyRevision, 0x27, "ranking"),
        data_policy: exact(ResourceKind::PolicyRevision, 0x28, "data"),
        conformance_evidence: objects,
    };
    let context_deployment = insert_deployment(
        pool,
        &base.tenant_id,
        &base.principal_id,
        &id(ResourceKind::ContextDeployment, 0x539),
        &id(ResourceKind::ContextSourceInterface, 0x12),
        &interface_revision.revision_id,
        &DeploymentClosure::ContextSourceInterface(context_closure),
    )
    .await;

    for (ordinal, metric) in [
        "durable_quota.sandbox_concurrent_executions",
        "durable_quota.sandbox_cpu_seconds",
        "durable_quota.sandbox_memory_mebibytes",
        "durable_quota.sandbox_output_bytes",
    ]
    .into_iter()
    .enumerate()
    {
        let payload = TypedPayload::new(1, &serde_json::json!({"fixture": true})).unwrap();
        sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_accounts (
                tenant_id, quota_account_id, scope_kind, scope_id, work_class,
                metric, limit_value, payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, 'tenant', $1, 'sandbox', $3, 100000000, $4, $5, $6)
            "#,
        )
        .bind(base.tenant_id.to_string())
        .bind(id(ResourceKind::QuotaAccount, 0x540 + ordinal as u16).to_string())
        .bind(metric)
        .bind(payload.schema_version)
        .bind(payload.value)
        .bind(payload.digest)
        .execute(pool)
        .await
        .unwrap();
    }

    ManagedSandboxFixture {
        subscription: Fixture {
            tenant_id: base.tenant_id.clone(),
            other_tenant_id: base.other_tenant_id.clone(),
            principal_id: base.principal_id.clone(),
            mcp_deployment,
            context_deployment,
            auth_policy: base.auth_policy.clone(),
            authorization,
            snapshot,
            subscription_id: id(ResourceKind::McpOperation, 0x54a),
            job_id: id(ResourceKind::Job, 0x54b),
            deadline: now + Duration::minutes(5),
        },
        runtime_revision,
        runtime,
        package_revision,
        package,
        profile_revision,
        profile,
        policies,
    }
}

fn command_audit(
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    base: u16,
    now: DateTime<Utc>,
) -> CommandAudit {
    CommandAudit {
        tenant_id: tenant_id.clone(),
        principal_id: principal_id.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: named_digest(&format!("command-key-{base}")),
        request_digest: named_digest("placeholder"),
        receipt_expires_at: now + Duration::hours(2),
    }
}

fn create_command(fixture: &Fixture, now: DateTime<Utc>) -> CreateMcpResourceSubscription {
    let context = fixture.authorization.execution_context(now).unwrap();
    let mut command = CreateMcpResourceSubscription {
        audit: command_audit(&fixture.tenant_id, &fixture.principal_id, 0x200, now),
        subscription_id: fixture.subscription_id.clone(),
        job_id: fixture.job_id.clone(),
        logical_key: "fixture-resource-subscription".to_owned(),
        execution: McpExecutionContractQuery {
            schema_version: 1,
            tenant_id: fixture.tenant_id.clone(),
            mcp_deployment: fixture.mcp_deployment.clone(),
            discovery_snapshot_id: fixture.snapshot.snapshot_id.clone(),
            discovery_snapshot_digest: fixture.snapshot.canonical_digest.clone(),
            authorization_binding_id: fixture.authorization.authorization_binding_id.clone(),
            authorization_generation: fixture.authorization.generation,
            authorization_context_digest: context.canonical_digest,
            principal_id: fixture.principal_id.clone(),
        },
        context_deployment: fixture.context_deployment.clone(),
        resource_uri: "mcp://catalog.example.test/items/42".to_owned(),
        attempt_limit: 4,
        deadline: fixture.deadline,
    };
    command.audit.request_digest = command.request_digest().unwrap();
    command
}

async fn create_subscription(
    repository: &PgRepository,
    command: CreateMcpResourceSubscription,
) -> Result<CommandOutcome<McpSubscriptionRecord>, RepositoryError> {
    let mut transaction = repository.begin_registry_transaction().await.unwrap();
    match transaction.create_mcp_resource_subscription(command).await {
        Ok(outcome) => {
            transaction.commit().await.unwrap();
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await.unwrap();
            Err(failure)
        }
    }
}

async fn claim_and_start(
    repository: &PgRepository,
    fixture: &Fixture,
    worker_id: &ResourceId,
    token_domain: &str,
) -> DomainJobFence {
    let tokens = (0..16)
        .map(|index| named_digest(&format!("{token_domain}-{index}")))
        .collect::<Vec<_>>();
    let claimed = repository
        .claim_jobs(ClaimJobs {
            work_class: "mcp".to_owned(),
            worker_id: worker_id.clone(),
            limit: u16::try_from(tokens.len()).unwrap(),
            lease_milliseconds: 60_000,
            lease_token_digests: tokens,
        })
        .await
        .unwrap()
        .into_iter()
        .find(|job| {
            job.tenant_id == fixture.tenant_id.to_string()
                && job.job_id == fixture.job_id.to_string()
        })
        .expect("subscription Job must be claimed");
    let token = claimed
        .lease_token_digest
        .as_ref()
        .unwrap()
        .parse::<Sha256Digest>()
        .unwrap();
    let started = repository
        .start_job(RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: fixture.job_id.to_string(),
            worker_id: worker_id.clone(),
            lease_epoch: claimed.lease_epoch,
            expected_job_version: claimed.version,
            lease_token_digest: token.clone(),
        })
        .await
        .unwrap();
    DomainJobFence {
        expected_version: u64::try_from(started.version).unwrap(),
        worker_process_generation_id: worker_id.clone(),
        lease_generation: u64::try_from(started.lease_epoch).unwrap(),
        token_digest: token,
    }
}

fn worker_audit(
    tenant_id: &ResourceId,
    worker_id: &ResourceId,
    base: u16,
    now: DateTime<Utc>,
) -> McpSubscriptionWorkerAudit {
    McpSubscriptionWorkerAudit {
        tenant_id: tenant_id.clone(),
        worker_process_generation_id: worker_id.clone(),
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: named_digest(&format!("worker-key-{base}")),
        request_digest: named_digest("placeholder"),
        receipt_expires_at: now + Duration::hours(2),
    }
}

#[allow(clippy::too_many_arguments)]
fn managed_session_command(
    fixture: &ManagedSandboxFixture,
    record: &McpSubscriptionRecord,
    contract: insight_platform_mcp_host::McpHostExecutionContract,
    worker_id: &ResourceId,
    fence: DomainJobFence,
    base: u16,
    now: DateTime<Utc>,
) -> AcceptManagedMcpSandboxSession {
    let physical_job_id = id(ResourceKind::Job, base);
    let sandbox_job_id =
        ResourceId::from_uuid_v7(ResourceKind::SandboxJob, physical_job_id.uuid()).unwrap();
    let identity = ManagedMcpSandboxSessionIdentity::build(
        &record.payload.binding,
        record.version,
        fence.expected_version,
        record.payload.session.generation + 1,
        sandbox_job_id.clone(),
        physical_job_id,
    )
    .unwrap();
    let runtime_bundle = fixture.package.runtime_bundle_artifact.clone();
    let artifact_grant = ScopedArtifactGrant {
        schema_version: 1,
        grant_id: id(ResourceKind::ArtifactGrant, base + 1),
        tenant_id: identity.tenant_id.clone(),
        sandbox_job_id: sandbox_job_id.clone(),
        operation: insight_platform_contracts::ArtifactGrantOperation::ReadWhole,
        port: "runtime_bundle".to_owned(),
        artifact: Some(runtime_bundle.clone()),
        staging_artifact_id: None,
        byte_range: None,
        maximum_bytes: runtime_bundle.byte_length(),
        generation: 1,
        expires_at: fixture.subscription.deadline,
        grant_digest: named_digest("placeholder"),
    }
    .seal()
    .unwrap();
    let secret_grant = ScopedSecretGrant {
        schema_version: 1,
        tenant_id: identity.tenant_id.clone(),
        sandbox_job_id: sandbox_job_id.clone(),
        secret_binding: fixture
            .subscription
            .authorization
            .token_secret_binding
            .clone(),
        resolved_binding_generation: 1,
        runtime_digest: fixture.runtime.image_or_module_digest.clone(),
        maximum_reads: 1,
        expires_at: fixture.subscription.deadline,
        grant_digest: named_digest("placeholder"),
    }
    .seal()
    .unwrap();
    let callback = ManagedMcpSandboxSessionCallback {
        schema_version: 1,
        session_identity_digest: identity.canonical_digest.clone(),
        audience_identity_digest: named_digest("managed-session-callback-audience"),
        expires_at: fixture.subscription.deadline,
        binding_digest: named_digest("placeholder"),
    }
    .seal()
    .unwrap();
    let request = ManagedMcpSandboxSessionRequest {
        schema_version: 1,
        protocol_version: SANDBOX_PROTOCOL_VERSION,
        identity,
        subscription_binding: record.payload.binding.clone(),
        mcp_contract: Box::new(contract),
        runtime_revision: fixture.runtime_revision.clone(),
        runtime: fixture.runtime.clone(),
        package_revision: fixture.package_revision.clone(),
        package: fixture.package.clone(),
        profile_revision: fixture.profile_revision.clone(),
        profile: fixture.profile.clone(),
        policies: Box::new(fixture.policies.clone()),
        isolation_class: SandboxIsolationClass::MicroVm,
        executor_worker_manifest_digest: named_digest("managed-executor-manifest"),
        isolation_backend_contract_digest: named_digest("managed-microvm-backend"),
        classification: DataClassification::Internal,
        artifact_grants: vec![artifact_grant],
        secret_grants: vec![secret_grant],
        network_mode: SandboxNetworkMode::None,
        resources: SandboxResourceEnvelope {
            cpu_millicores: 100,
            memory_mebibytes: 64,
            pids: 4,
            files: 16,
            io_bytes: 1_048_576,
            stdout_bytes: 4_096,
            stderr_bytes: 4_096,
            result_bytes: 4_096,
            artifact_output_bytes: 0,
            network_connections: 0,
            network_request_bytes: 0,
            network_response_bytes: 0,
            startup_milliseconds: 10_000,
            idle_milliseconds: 60_000,
            wall_milliseconds: 300_000,
            cleanup_milliseconds: 10_000,
            wasm_fuel: None,
            wasm_memory_pages: None,
        },
        deadline: fixture.subscription.deadline,
        callback,
        request_digest: named_digest("placeholder"),
    }
    .seal()
    .unwrap();
    AcceptManagedMcpSandboxSession {
        audit: McpSubscriptionWorkerAudit {
            tenant_id: request.identity.tenant_id.clone(),
            worker_process_generation_id: worker_id.clone(),
            receipt_id: id(ResourceKind::Receipt, base + 2),
            event_id: id(ResourceKind::Event, base + 3),
            outbox_id: id(ResourceKind::OutboxEvent, base + 4),
            idempotency_key_digest: named_digest(&format!("managed-session-key-{base}")),
            request_digest: request.request_digest.clone(),
            receipt_expires_at: now + Duration::hours(2),
        },
        logical_fence: fence,
        request,
        usage_reservation_id: id(ResourceKind::UsageReservation, base + 5),
        quota_entry_ids: (base + 6..base + 10)
            .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
            .collect(),
    }
}

struct SessionCommandInput {
    target: McpSessionState,
    opaque: Option<EncryptedMcpState>,
    expires_at: Option<DateTime<Utc>>,
    base: u16,
    now: DateTime<Utc>,
}

fn session_command(
    fixture: &Fixture,
    worker_id: &ResourceId,
    fence: DomainJobFence,
    record: &McpSubscriptionRecord,
    input: SessionCommandInput,
) -> SaveMcpSubscriptionSession {
    let mut command = SaveMcpSubscriptionSession {
        audit: worker_audit(&fixture.tenant_id, worker_id, input.base, input.now),
        subscription_id: fixture.subscription_id.clone(),
        job_id: fixture.job_id.clone(),
        fence,
        expected_subscription_version: record.version,
        expected_session_version: record.payload.session.version,
        target: input.target,
        encrypted_opaque_session: input.opaque,
        expires_at: input.expires_at,
        phase_evidence_digest: matches!(
            input.target,
            McpSessionState::Ready
                | McpSessionState::Degraded
                | McpSessionState::ReauthRequired
                | McpSessionState::Failed
        )
        .then(|| named_digest("session-phase-evidence")),
    };
    command.audit.request_digest = command.request_digest().unwrap();
    command
}

fn notification(
    fixture: &Fixture,
    session_generation: u64,
    event_generation: u64,
    base: u16,
    now: DateTime<Utc>,
) -> McpNotificationCommit {
    McpNotificationCommit {
        audit: McpNotificationAudit {
            tenant_id: fixture.tenant_id.clone(),
            receipt_id: id(ResourceKind::Receipt, base),
            event_id: id(ResourceKind::Event, base + 1),
            outbox_id: id(ResourceKind::OutboxEvent, base + 2),
            receipt_expires_at: now + Duration::hours(2),
        },
        tenant_id: fixture.tenant_id.clone(),
        subscription_id: fixture.subscription_id.clone(),
        authorization_generation: fixture.authorization.generation,
        session_generation,
        event_key_digest: named_digest(&format!("notification-key-{event_generation}")),
        event_generation,
        class: McpNotificationClass::ResourceUpdated,
        resource_uri_digest: Some(named_digest("resource-uri-placeholder")),
        body_digest: named_digest(&format!("notification-body-{event_generation}")),
        wire_bytes: 512,
        received_at: now,
    }
}

#[test]
fn mcp_subscription_is_durable_fenced_coalesced_and_tenant_scoped() {
    std::thread::Builder::new()
        .name("phase4-mcp-subscription-fixture".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(mcp_subscription_fixture());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn mcp_subscription_fixture() {
    let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
        eprintln!("PLATFORM_TEST_DATABASE_URL is unset; real PostgreSQL fixture skipped");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let now = Utc::now();
    let fixture = seed(&pool, &repository, now).await;
    let command = create_command(&fixture, now);

    let applied = create_subscription(&repository, command.clone())
        .await
        .unwrap();
    let mut record = match applied {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first create must apply"),
    };
    assert_eq!(record.state.to_string(), "pending");
    assert!(matches!(
        create_subscription(&repository, command.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    let mut conflicting = command.clone();
    conflicting.resource_uri = "mcp://catalog.example.test/items/43".to_owned();
    assert!(create_subscription(&repository, conflicting).await.is_err());

    let mut cross_tenant = command.clone();
    cross_tenant.audit = command_audit(&fixture.other_tenant_id, &fixture.principal_id, 0x210, now);
    cross_tenant.subscription_id = id(ResourceKind::McpOperation, 0x213);
    cross_tenant.job_id = id(ResourceKind::Job, 0x214);
    cross_tenant.execution.tenant_id = fixture.other_tenant_id.clone();
    cross_tenant.audit.request_digest = cross_tenant.request_digest().unwrap();
    assert!(create_subscription(&repository, cross_tenant)
        .await
        .is_err());

    let worker_id = id(ResourceKind::WorkerProcessGeneration, 0x220);
    let mut fence = claim_and_start(&repository, &fixture, &worker_id, "session-lease").await;
    let resolved = repository
        .resolve_mcp_subscription_execution(&McpSubscriptionContractQuery {
            schema_version: 1,
            tenant_id: fixture.tenant_id.clone(),
            subscription_id: fixture.subscription_id.clone(),
            job_id: fixture.job_id.clone(),
            fence: fence.clone(),
        })
        .await
        .unwrap();
    assert_eq!(resolved.record.version, record.version);
    assert!(resolved
        .record
        .payload
        .session
        .binding_key
        .validate_canonical_for(&resolved.contract)
        .is_ok());
    let connecting = session_command(
        &fixture,
        &worker_id,
        fence.clone(),
        &record,
        SessionCommandInput {
            target: McpSessionState::Connecting,
            opaque: None,
            expires_at: None,
            base: 0x230,
            now,
        },
    );
    record = match repository
        .save_mcp_subscription_session(connecting.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("connecting must apply"),
    };
    assert!(matches!(
        repository
            .save_mcp_subscription_session(connecting)
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    fence.expected_version += 1;
    let initializing = session_command(
        &fixture,
        &worker_id,
        fence.clone(),
        &record,
        SessionCommandInput {
            target: McpSessionState::Initializing,
            opaque: None,
            expires_at: None,
            base: 0x240,
            now,
        },
    );
    record = match repository
        .save_mcp_subscription_session(initializing)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("initializing must apply"),
    };
    fence.expected_version += 1;
    let ready = session_command(
        &fixture,
        &worker_id,
        fence,
        &record,
        SessionCommandInput {
            target: McpSessionState::Ready,
            opaque: Some(EncryptedMcpState {
                scheme: "aes256_gcm_v1".to_owned(),
                ciphertext: b"opaque-session-ciphertext-canary".to_vec(),
                key_id: "opaque-session-key-canary".to_owned(),
                key_reference_digest: named_digest("opaque-session-key-reference"),
                plaintext_digest: named_digest("opaque-session"),
            }),
            expires_at: Some(now + Duration::minutes(30)),
            base: 0x250,
            now,
        },
    );
    record = match repository
        .save_mcp_subscription_session(ready)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("ready must apply"),
    };
    assert_eq!(record.state.to_string(), "active");
    assert_eq!(record.payload.session.generation, 1);

    let resource_uri_digest = record.payload.binding.resource_uri_digest.clone();
    let mut first = notification(&fixture, 1, 1, 0x260, now);
    first.resource_uri_digest = Some(named_digest("resource-subresource-uri"));
    let mut second = notification(&fixture, 1, 2, 0x270, now + Duration::milliseconds(1));
    second.resource_uri_digest = Some(resource_uri_digest);
    let (first_outcome, second_outcome) = tokio::join!(
        repository.commit_mcp_notification(first.clone()),
        repository.commit_mcp_notification(second.clone())
    );
    let outcomes = [first_outcome.unwrap(), second_outcome.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.disposition == McpNotificationApplyDisposition::Wake)
            .count(),
        1
    );
    assert!(outcomes.iter().all(|outcome| !outcome.replayed));
    let mut redelivery = second.clone();
    redelivery.audit.receipt_id = id(ResourceKind::Receipt, 0x275);
    redelivery.audit.event_id = id(ResourceKind::Event, 0x276);
    redelivery.audit.outbox_id = id(ResourceKind::OutboxEvent, 0x277);
    redelivery.received_at += Duration::seconds(1);
    let replay = repository
        .commit_mcp_notification(redelivery)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(
        replay
            .record
            .payload
            .pending_invalidation
            .as_ref()
            .unwrap()
            .event_generation,
        2
    );

    let refresh_worker = id(ResourceKind::WorkerProcessGeneration, 0x280);
    let refresh_fence =
        claim_and_start(&repository, &fixture, &refresh_worker, "refresh-lease").await;
    let mut refresh = CompleteMcpSubscriptionRefresh {
        audit: worker_audit(&fixture.tenant_id, &refresh_worker, 0x290, now),
        subscription_id: fixture.subscription_id.clone(),
        job_id: fixture.job_id.clone(),
        fence: refresh_fence,
        expected_subscription_version: replay.record.version,
        expected_session_generation: 1,
        expected_event_generation: 2,
        refresh_evidence_digest: named_digest("refresh-evidence"),
    };
    refresh.audit.request_digest = refresh.request_digest().unwrap();
    let refreshed = match repository
        .complete_mcp_subscription_refresh(refresh.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("refresh must apply"),
    };
    assert!(refreshed.payload.pending_invalidation.is_none());
    assert!(matches!(
        repository
            .complete_mcp_subscription_refresh(refresh)
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));

    let mut stale = notification(&fixture, 1, 1, 0x2a0, now + Duration::seconds(1));
    stale.event_key_digest = named_digest("stale-distinct-key");
    stale.resource_uri_digest = Some(refreshed.payload.binding.resource_uri_digest.clone());
    let stale = repository.commit_mcp_notification(stale).await.unwrap();
    assert_eq!(stale.disposition, McpNotificationApplyDisposition::Stale);
    assert!(!stale.replayed);

    let job = sqlx::query(
        "SELECT state, wake_kind, wake_state, worker_id FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(job.try_get::<String, _>("state").unwrap(), "waiting");
    assert_eq!(
        job.try_get::<Option<String>, _>("wake_kind")
            .unwrap()
            .as_deref(),
        Some("remote_invocation")
    );
    assert_eq!(
        job.try_get::<Option<String>, _>("wake_state")
            .unwrap()
            .as_deref(),
        Some("pending")
    );
    assert!(job
        .try_get::<Option<String>, _>("worker_id")
        .unwrap()
        .is_none());

    sqlx::query(
        r#"
        UPDATE insight_platform.invocations
        SET created_at = created_at - interval '5 minutes',
            updated_at = updated_at - interval '5 minutes'
        WHERE tenant_id = $1 AND invocation_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.subscription_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let due = repository
        .list_due_mcp_subscription_reconciliations(McpSubscriptionReconcileScan {
            tenant_id: fixture.tenant_id.clone(),
            limit: 8,
            minimum_idle_milliseconds: 60_000,
        })
        .await
        .unwrap();
    assert_eq!(due.len(), 1);
    let scan_worker = id(ResourceKind::WorkerProcessGeneration, 0x2b0);
    let mut wake = WakeMcpSubscriptionReconcile {
        audit: worker_audit(&fixture.tenant_id, &scan_worker, 0x2c0, now),
        candidate: due[0].clone(),
    };
    wake.audit.request_digest = wake.request_digest().unwrap();
    assert!(matches!(
        repository
            .wake_mcp_subscription_reconcile(wake.clone())
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    assert!(matches!(
        repository
            .wake_mcp_subscription_reconcile(wake)
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    let reconcile_worker = id(ResourceKind::WorkerProcessGeneration, 0x2d0);
    let reconcile_fence =
        claim_and_start(&repository, &fixture, &reconcile_worker, "reconcile-lease").await;
    let mut reconcile = CompleteMcpSubscriptionReconcile {
        audit: worker_audit(&fixture.tenant_id, &reconcile_worker, 0x2e0, now),
        subscription_id: fixture.subscription_id.clone(),
        job_id: fixture.job_id.clone(),
        fence: reconcile_fence,
        expected_subscription_version: refreshed.version,
        expected_session_generation: refreshed.payload.session.generation,
        reconcile_evidence_digest: named_digest("periodic-reconcile-evidence"),
    };
    reconcile.audit.request_digest = reconcile.request_digest().unwrap();
    let reconciled = repository
        .complete_mcp_subscription_reconcile(reconcile)
        .await
        .unwrap();
    let reconciled = match reconciled {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("periodic reconcile must apply"),
    };
    let reconcile_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_type IN ('mcp.subscription_reconcile_due', 'mcp.subscription_reconciled')",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(reconcile_events, 2);

    let loss_worker = id(ResourceKind::WorkerProcessGeneration, 0x2f0);
    let mut stale_termination = ReportMcpSubscriptionTransportTermination {
        audit: worker_audit(&fixture.tenant_id, &loss_worker, 0x2f4, now),
        subscription_id: fixture.subscription_id.clone(),
        expected_authorization_generation: fixture.authorization.generation,
        expected_session_generation: reconciled.payload.session.generation + 1,
        reported_at: now,
        session_loss_evidence_digest: named_digest("stale-transport-loss-evidence"),
    };
    stale_termination.audit.request_digest = stale_termination.request_digest().unwrap();
    assert!(matches!(
        repository
            .report_mcp_subscription_transport_termination(stale_termination)
            .await,
        Err(RepositoryError::StaleFence)
    ));

    let mut session_loss = ReportMcpSubscriptionTransportTermination {
        audit: worker_audit(&fixture.tenant_id, &loss_worker, 0x300, now),
        subscription_id: fixture.subscription_id.clone(),
        expected_authorization_generation: fixture.authorization.generation,
        expected_session_generation: reconciled.payload.session.generation,
        reported_at: now,
        session_loss_evidence_digest: named_digest("transport-loss-evidence"),
    };
    session_loss.audit.request_digest = session_loss.request_digest().unwrap();
    let lost = match repository
        .report_mcp_subscription_transport_termination(session_loss.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("session loss must apply"),
    };
    assert!(matches!(
        repository
            .report_mcp_subscription_transport_termination(session_loss)
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    assert_eq!(lost.state.to_string(), "pending");
    assert_eq!(lost.payload.session.state, McpSessionState::Disconnected);
    assert!(lost.payload.session.encrypted_opaque_session.is_none());
    assert!(lost.payload.full_reconcile_required);
    let requeued_job = sqlx::query(
        "SELECT state, wake_kind, wake_state, worker_id FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(requeued_job.try_get::<String, _>("state").unwrap(), "ready");
    assert!(requeued_job
        .try_get::<Option<String>, _>("wake_kind")
        .unwrap()
        .is_none());
    assert!(requeued_job
        .try_get::<Option<String>, _>("wake_state")
        .unwrap()
        .is_none());
    assert!(requeued_job
        .try_get::<Option<String>, _>("worker_id")
        .unwrap()
        .is_none());

    let rebuild_worker = id(ResourceKind::WorkerProcessGeneration, 0x310);
    let rebuild_fence =
        claim_and_start(&repository, &fixture, &rebuild_worker, "rebuild-lease").await;
    let connecting = session_command(
        &fixture,
        &rebuild_worker,
        rebuild_fence,
        &lost,
        SessionCommandInput {
            target: McpSessionState::Connecting,
            opaque: None,
            expires_at: None,
            base: 0x320,
            now,
        },
    );
    let connecting = match repository
        .save_mcp_subscription_session(connecting)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("rebuild connecting must apply"),
    };
    sqlx::query(
        "UPDATE insight_platform.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.job_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let recoveries = repository
        .list_due_mcp_subscription_recoveries(McpSubscriptionRecoveryScan {
            tenant_id: fixture.tenant_id.clone(),
            limit: 8,
        })
        .await
        .unwrap();
    assert_eq!(recoveries.len(), 1);
    assert_eq!(
        recoveries[0].cause,
        McpSubscriptionRecoveryCause::ExpiredLease
    );
    assert_eq!(recoveries[0].subscription_version, connecting.version);
    let recovery_worker = id(ResourceKind::WorkerProcessGeneration, 0x330);
    let mut recovery = RecoverDueMcpSubscription {
        audit: worker_audit(&fixture.tenant_id, &recovery_worker, 0x340, now),
        candidate: recoveries[0].clone(),
    };
    recovery.audit.request_digest = recovery.request_digest().unwrap();
    let recovered = match repository
        .recover_due_mcp_subscription(recovery.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("expired lease recovery must apply"),
    };
    assert!(matches!(
        repository
            .recover_due_mcp_subscription(recovery)
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    assert_eq!(
        recovered.payload.session.state,
        McpSessionState::Disconnected
    );
    assert!(recovered.payload.full_reconcile_required);

    let invalidation_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_type = 'mcp.subscription_invalidated'",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(invalidation_events, 1);
    let notification_receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND operation = 'mcp.notification'",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(notification_receipts, 3);
    managed_mcp_sandbox_session_admission_fixture(&pool, &repository, &fixture, now).await;
    let durable_projection: String = sqlx::query_scalar(
        r#"
        SELECT concat_ws('|',
            COALESCE((SELECT string_agg(to_jsonb(receipt)::text, '|') FROM insight_platform.receipts AS receipt WHERE tenant_id = $1), ''),
            COALESCE((SELECT string_agg(to_jsonb(event)::text, '|') FROM insight_platform.events AS event WHERE tenant_id = $1), ''),
            COALESCE((SELECT string_agg(to_jsonb(outbox)::text, '|') FROM insight_platform.outbox_events AS outbox WHERE tenant_id = $1), '')
        )
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!durable_projection.contains("opaque-session-key-canary"));
    assert!(!durable_projection.contains("opaque-session-ciphertext-canary"));
    assert!(!durable_projection.contains("mcp://catalog.example.test/items/42"));
}

async fn managed_mcp_sandbox_session_admission_fixture(
    pool: &PgPool,
    repository: &PgRepository,
    base: &Fixture,
    now: DateTime<Utc>,
) {
    let fixture = seed_managed_sandbox_variant(pool, base, now).await;
    let mut create = create_command(&fixture.subscription, now);
    create.audit = command_audit(
        &fixture.subscription.tenant_id,
        &fixture.subscription.principal_id,
        0x580,
        now,
    );
    create.logical_key = "fixture-managed-resource-subscription".to_owned();
    create.audit.request_digest = create.request_digest().unwrap();
    let record = match create_subscription(repository, create).await.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("managed subscription create must apply"),
    };
    sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET scheduled_at = clock_timestamp() + interval '10 minutes'
        WHERE tenant_id = $1 AND work_class = 'mcp' AND state = 'ready' AND job_id <> $2
        "#,
    )
    .bind(fixture.subscription.tenant_id.to_string())
    .bind(fixture.subscription.job_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let worker_id = id(ResourceKind::WorkerProcessGeneration, 0x590);
    let fence = claim_and_start(
        repository,
        &fixture.subscription,
        &worker_id,
        "managed-session-lease",
    )
    .await;
    let resolved = repository
        .resolve_mcp_subscription_execution(&McpSubscriptionContractQuery {
            schema_version: 1,
            tenant_id: fixture.subscription.tenant_id.clone(),
            subscription_id: fixture.subscription.subscription_id.clone(),
            job_id: fixture.subscription.job_id.clone(),
            fence: fence.clone(),
        })
        .await
        .unwrap();
    assert!(matches!(
        resolved.contract.deployment_closure.transport,
        McpTransportBinding::ManagedStdio { .. }
    ));
    let command_a = managed_session_command(
        &fixture,
        &record,
        resolved.contract.clone(),
        &worker_id,
        fence.clone(),
        0x600,
        now,
    );
    let command_b = managed_session_command(
        &fixture,
        &record,
        resolved.contract,
        &worker_id,
        fence,
        0x620,
        now,
    );
    let (result_a, result_b) = tokio::join!(
        repository.accept_managed_mcp_sandbox_session(command_a.clone()),
        repository.accept_managed_mcp_sandbox_session(command_b.clone()),
    );
    let a_applied = matches!(result_a, Ok(CommandOutcome::Applied(_)));
    let b_applied = matches!(result_b, Ok(CommandOutcome::Applied(_)));
    assert_ne!(
        a_applied, b_applied,
        "exactly one generation admission wins: a={result_a:?}, b={result_b:?}"
    );
    let (winner, loser, applied) = if a_applied {
        assert!(matches!(result_b, Err(RepositoryError::StaleFence)));
        (command_a, command_b, result_a.unwrap())
    } else {
        assert!(matches!(result_a, Err(RepositoryError::StaleFence)));
        (command_b, command_a, result_b.unwrap())
    };
    let accepted = match applied {
        CommandOutcome::Applied(accepted) => accepted,
        CommandOutcome::Replayed(_) => unreachable!(),
    };
    assert_eq!(accepted.logical_state.to_string(), "pending");
    assert_eq!(
        accepted.logical_payload.session.state,
        McpSessionState::Connecting
    );
    assert_eq!(
        accepted.physical_job.job_id,
        winner.request.identity.physical_job_id
    );
    assert!(matches!(
        repository
            .accept_managed_mcp_sandbox_session(winner.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    let mut conflicting = winner.clone();
    conflicting.usage_reservation_id = id(ResourceKind::UsageReservation, 0x650);
    assert!(matches!(
        repository
            .accept_managed_mcp_sandbox_session(conflicting)
            .await,
        Err(RepositoryError::IdempotencyConflict)
    ));

    let logical = sqlx::query(
        r#"
        SELECT state, version, worker_id, lease_token_digest, lease_expires_at,
               wake_kind, wake_state, wake_generation
        FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2
        "#,
    )
    .bind(fixture.subscription.tenant_id.to_string())
    .bind(fixture.subscription.job_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(logical.try_get::<String, _>("state").unwrap(), "waiting");
    assert_eq!(
        logical.try_get::<i64, _>("version").unwrap(),
        i64::try_from(winner.logical_fence.expected_version + 1).unwrap()
    );
    assert!(logical
        .try_get::<Option<String>, _>("worker_id")
        .unwrap()
        .is_none());
    assert!(logical
        .try_get::<Option<String>, _>("lease_token_digest")
        .unwrap()
        .is_none());
    assert!(logical
        .try_get::<Option<DateTime<Utc>>, _>("lease_expires_at")
        .unwrap()
        .is_none());
    assert_eq!(
        logical.try_get::<String, _>("wake_kind").unwrap(),
        "remote_invocation"
    );
    assert_eq!(
        logical.try_get::<String, _>("wake_state").unwrap(),
        "pending"
    );
    assert_eq!(logical.try_get::<i64, _>("wake_generation").unwrap(), 1);

    let physical = sqlx::query(
        r#"
        SELECT work_class, owner_kind, owner_id, invocation_id, state,
               request_digest, quota_reservation_id, payload ->> 'workload_kind' AS workload_kind
        FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2
        "#,
    )
    .bind(fixture.subscription.tenant_id.to_string())
    .bind(winner.request.identity.physical_job_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        physical.try_get::<String, _>("work_class").unwrap(),
        "sandbox"
    );
    assert_eq!(
        physical.try_get::<String, _>("owner_kind").unwrap(),
        "sandbox_job"
    );
    assert_eq!(
        physical.try_get::<String, _>("owner_id").unwrap(),
        winner.request.identity.sandbox_job_id.to_string()
    );
    assert_eq!(
        physical.try_get::<String, _>("invocation_id").unwrap(),
        fixture.subscription.subscription_id.to_string()
    );
    assert_eq!(physical.try_get::<String, _>("state").unwrap(), "ready");
    assert_eq!(
        physical.try_get::<String, _>("request_digest").unwrap(),
        winner.request.request_digest.to_string()
    );
    assert_eq!(
        physical
            .try_get::<String, _>("quota_reservation_id")
            .unwrap(),
        winner.usage_reservation_id.to_string()
    );
    assert_eq!(
        physical.try_get::<String, _>("workload_kind").unwrap(),
        "managed_mcp_subscription_session"
    );
    let loser_physical_jobs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.subscription.tenant_id.to_string())
    .bind(loser.request.identity.physical_job_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(loser_physical_jobs, 0);
    let grant_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.artifact_links WHERE tenant_id = $1 AND owner_kind = 'sandbox_job' AND owner_id = $2 AND link_kind = 'grant' AND state = 'active'",
    )
    .bind(fixture.subscription.tenant_id.to_string())
    .bind(winner.request.identity.sandbox_job_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(grant_count, 1);
    let loser_grants: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.artifact_links WHERE tenant_id = $1 AND owner_kind = 'sandbox_job' AND owner_id = $2",
    )
    .bind(fixture.subscription.tenant_id.to_string())
    .bind(loser.request.identity.sandbox_job_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(loser_grants, 0);
    let quota_entries: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'reserve'",
    )
    .bind(fixture.subscription.tenant_id.to_string())
    .bind(winner.usage_reservation_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(quota_entries, 4);
    let loser_quota_entries: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id = $1 AND correlation_id = $2",
    )
    .bind(fixture.subscription.tenant_id.to_string())
    .bind(loser.usage_reservation_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(loser_quota_entries, 0);
    let durable_rows: (i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
            (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2 AND state = 'succeeded'),
            (SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_id = $3 AND event_type = 'mcp.managed_sandbox_session_scheduled'),
            (SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id = $1 AND outbox_id = $4)
        "#,
    )
    .bind(fixture.subscription.tenant_id.to_string())
    .bind(winner.audit.receipt_id.to_string())
    .bind(winner.audit.event_id.to_string())
    .bind(winner.audit.outbox_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(durable_rows, (1, 1, 1));
}
