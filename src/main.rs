use std::{error::Error, ffi::OsStr, future::IntoFuture, io, sync::Arc};

use insight_agent_platform::{
    catalog::{OwnedProductionLeafDeploymentResolver, TerminalOnlyDeploymentConfig},
    config::{
        ArtifactStoreProvider, DebugExecutionProfileMode, DeploymentMode, HistoryConfig,
        LiveRunStreamBrokerProvider, PlatformConfig, RunStreamConfig,
    },
    engine::{
        production_worker_registry_with_live_run_stream_and_retrievals,
        repository::{PostgresDurableRepository, SqliteDurableRepository},
        LocalContentAddressedArtifactStore, TenantArtifactEncryptionKeyring, WorkerArtifactStore,
    },
    resources::{
        builtin_actions::{builtin_action_registry, RestrictedHttpGetAction},
        management_observability::ManagementObservabilityRuntime,
        mcp::{DurableMcpInteractionHandler, McpInteractionHandler},
        mcp_management::{
            mcp_management_policy_material, DurableMcpRunAdmissionAuthority,
            ManagedMcpRevisionReadiness, McpDiscoveryRuntime, McpManagementStoreReadiness,
            McpRevisionRuntime,
        },
        mcp_server::PlatformMcpServerBackend,
        models::ModelRegistry,
        provider_management::{
            DurableProviderRunAdmissionAuthority, ManagedProviderRevisionReadiness,
            ProviderManagementRuntime, ProviderManagementRuntimeConfig,
        },
        retrievals::RetrievalRegistry,
    },
    runtime::{
        DeployedAgentCatalog, InMemoryLiveRunStreamBroker, LiveRunStreamBroker,
        LiveRunStreamByteLimits, PostgresLiveRunStreamBroker, PostgresLiveRunStreamBrokerOptions,
        ProductionRunRepository, RunService, RunServiceConfig, TerminalOnlyRunConfig,
        TerminalOnlyStore, WorkCoordinatorConfig,
    },
};
use insight_api::mcp::{
    build_mcp_protected_resource_metadata_router, build_mcp_server_router,
    oauth_protected_resource_metadata, ApiAuthMcpHttpAuthorizer, DisabledMcpHttpAuthorizer,
    McpHttpAuthorizer, McpHttpService, OAuthResourceServerAuthorizer,
};
use insight_api::v1::{
    build_agent_management_router, build_mcp_catalog_router, build_mcp_management_router,
    build_provider_management_router, build_router, AgentDebugProfileMode, AgentDebugProfilePolicy,
    AgentManagementApiState, AgentManagementPolicy, ApiAuth, ApiState,
    BearerHumanPrincipalResolver, McpCatalogApiState, McpCatalogRegistry, McpManagementApiState,
    McpManagementPolicy, McpManifestSignerPolicy, McpOAuthRegistry, McpProfileReport, OperatorAuth,
    ProviderManagementApiState, ProviderManagementPolicy, ProviderTemplate,
};
use insight_api::v1::{build_router_with_mcp, McpOAuthApiState};
use insight_durable::{
    AgentManagementDurableRepository, McpInteractionDurableRepository,
    McpManagementDurableRepository, McpOAuthDurableRepository, McpRemoteTaskDurableRepository,
    McpSecretProtector, McpServerTaskDurableRepository, ProviderManagementDurableRepository,
};
use insight_mcp::McpServerDispatcher;
use insight_storage::mcp_secret::McpSecretEncryptionKeyring;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const PROCESS_PANICKED_CODE: &str = "PROCESS_PANICKED";
const PROCESS_PANICKED_MESSAGE: &str = "process panic captured";
const RUNTIME_POSTGRES_APPLICATION_NAME: &str = "insight-agent-platform-runtime";
const QUALIFICATION_ENABLED_ENV: &str = "INSIGHT_QUALIFICATION_ENABLED";
const QUALIFICATION_SELF_ABORT_HANDOFF_DELAY: std::time::Duration =
    std::time::Duration::from_millis(250);

#[tokio::main]
async fn main() -> MainResult<()> {
    let _ = dotenvy::dotenv();
    init_tracing();
    install_sanitized_panic_hook();

    let qualification_enabled = qualification_enabled_from_environment()?;
    let mut qualification_self_abort =
        QualificationSelfAbortControl::prepare(qualification_enabled)?;
    let config = PlatformConfig::from_env()?;
    let (
        repository,
        terminal_store,
        live_run_stream_broker,
        mcp_interactions,
        mcp_oauth,
        mcp_remote_tasks,
        mcp_server_tasks,
        mcp_management,
        provider_management,
        agent_management,
    ) = initialize_repository_and_live_run_stream(&config.history, config.runtime.run_stream)
        .await?;
    let mcp_policy_material = mcp_management_policy_material(
        &config.mcp.client,
        &config.mcp.protocol.preferred,
        &config.mcp.protocol.legacy_fallback,
    )?;
    let mcp_secret_protector = config
        .mcp
        .client
        .secret_encryption
        .as_ref()
        .map(|encryption| {
            McpSecretEncryptionKeyring::from_secret_json(
                encryption.active_key_version.clone(),
                encryption.keyring.expose(),
            )
            .map(|protector| Arc::new(protector) as Arc<dyn McpSecretProtector>)
        })
        .transpose()?;
    let mut mcp_discovery_runtime = McpDiscoveryRuntime::start(
        Arc::clone(&mcp_management),
        config.mcp.client.clone(),
        mcp_policy_material.policy_fingerprint.clone(),
    );
    let mcp_interaction_handler = mcp_secret_protector.as_ref().map(|protector| {
        Arc::new(DurableMcpInteractionHandler::new(
            Arc::clone(&mcp_interactions),
            Arc::clone(protector),
        )) as Arc<dyn McpInteractionHandler>
    });
    // Durable managed Provider revisions are the only live model routes. The
    // built-in catalog and platform extensions are import/template inputs,
    // never implicit executable registrations.
    let models = ModelRegistry::default();
    let provider_runtime_config = ProviderManagementRuntimeConfig {
        enabled: config.management.enabled,
        workers: config.mcp.client.management_api.discovery_workers.max(1) as usize,
        poll_interval: std::time::Duration::from_millis(250),
        lease_duration: config
            .runtime
            .operation_timeout
            .checked_add(std::time::Duration::from_secs(30))
            .unwrap_or(std::time::Duration::from_secs(630)),
        retention: chrono::Duration::days(i64::from(
            config.management.limits.operation_retention_days,
        )),
        max_response_bytes: config.mcp.client.default_limits.limits.max_response_bytes,
        allow_loopback_development: config.mcp.client.network_policy.allow_loopback_development,
    };
    let mut provider_management_runtime = ProviderManagementRuntime::start(
        Arc::clone(&provider_management),
        models.clone(),
        provider_runtime_config.clone(),
    )
    .await
    .map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message))?;
    let http_get = config
        .actions
        .http_get
        .as_ref()
        .map(|http| {
            RestrictedHttpGetAction::new(
                http.timeout,
                http.max_bytes,
                http.allowlist.iter().cloned().collect(),
            )
        })
        .transpose()?;
    let actions = builtin_action_registry(
        &config.actions.enabled.iter().cloned().collect::<Vec<_>>(),
        http_get,
    )?;
    let retrievals = RetrievalRegistry::default();
    let mcp_oauth_runtime = mcp_secret_protector.as_ref().map(|protector| {
        insight_agent_platform::resources::mcp::McpOAuthRuntimeAuthority::new(
            Arc::clone(&mcp_interactions),
            Arc::clone(&mcp_oauth),
            Arc::clone(protector),
        )
    });
    let mcp_remote_task_runtime = mcp_secret_protector.as_ref().map(|protector| {
        insight_agent_platform::resources::mcp::McpRemoteTaskRuntimeAuthority::new(
            Arc::clone(&mcp_interactions),
            Arc::clone(&mcp_remote_tasks),
            Arc::clone(protector),
        )
    });
    let mcp_catalog_registry = McpCatalogRegistry::default();
    let mcp_oauth_registry = McpOAuthRegistry::default();
    let mut mcp_revision_runtime = McpRevisionRuntime::start(
        Arc::clone(&mcp_management),
        config.mcp.client.clone(),
        mcp_policy_material.clone(),
        actions.clone(),
        retrievals.clone(),
        mcp_catalog_registry.clone(),
        mcp_oauth_registry.clone(),
        mcp_interaction_handler.as_ref().map(Arc::clone),
        mcp_oauth_runtime.clone(),
        mcp_remote_task_runtime.clone(),
    )
    .await
    .map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message))?;

    let graph_publication_resolver = Arc::new(
        OwnedProductionLeafDeploymentResolver::new(&models, &actions)
            .with_retrievals(&retrievals)
            .with_llm_tool_continuation_capability()
            .with_operation_timeout(config.runtime.operation_timeout)?
            .with_llm_tool_limits(
                config.runtime.max_llm_tool_rounds,
                config.runtime.max_llm_tool_calls,
            )?,
    );
    let terminal_execution_budget = config
        .runtime
        .terminal_only
        .owner_lease
        .min(config.runtime.shutdown_grace_period);
    let terminal_only_deployment = TerminalOnlyDeploymentConfig::new(
        config.runtime.terminal_only.enabled,
        config.runtime.terminal_only.allow_volatile_waits,
        terminal_execution_budget,
    )?;
    let graph_persistence_policy =
        terminal_only_deployment.resolve(config.runtime.default_persistence_mode)?;
    // RunService restores exact durable Deployment archives and current heads
    // during startup. File Agents no longer create process-local public routes.
    let agents = DeployedAgentCatalog::new(Vec::new())?;
    let workers = production_worker_registry_with_live_run_stream_and_retrievals(
        &models,
        &actions,
        &retrievals,
        Arc::clone(&live_run_stream_broker),
    )?;
    let tenant_encryption = config
        .artifacts
        .tenant_encryption
        .as_ref()
        .map(|encryption| {
            TenantArtifactEncryptionKeyring::from_secret_json(
                encryption.active_key_version.clone(),
                encryption.keyring.expose(),
            )
        })
        .transpose()?;
    let artifact_store: Arc<dyn WorkerArtifactStore> =
        Arc::new(match (config.artifacts.provider, tenant_encryption) {
            (ArtifactStoreProvider::LocalFilesystem, None) => {
                LocalContentAddressedArtifactStore::open(
                    config.artifacts.directory.clone(),
                    config.artifacts.inline_threshold_bytes,
                )
                .await?
            }
            (ArtifactStoreProvider::LocalFilesystem, Some(encryption)) => {
                LocalContentAddressedArtifactStore::open_with_tenant_encryption(
                    config.artifacts.directory.clone(),
                    config.artifacts.inline_threshold_bytes,
                    encryption,
                )
                .await?
            }
            (ArtifactStoreProvider::SharedFilesystem, encryption) => {
                let namespace = config.artifacts.namespace.clone().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "shared Artifact namespace is missing",
                    )
                })?;
                match encryption {
                    Some(encryption) => {
                        LocalContentAddressedArtifactStore::open_shared_with_tenant_encryption(
                            config.artifacts.directory.clone(),
                            config.artifacts.inline_threshold_bytes,
                            namespace,
                            encryption,
                        )
                        .await?
                    }
                    None => {
                        LocalContentAddressedArtifactStore::open_shared(
                            config.artifacts.directory.clone(),
                            config.artifacts.inline_threshold_bytes,
                            namespace,
                        )
                        .await?
                    }
                }
            }
        });
    let service_config = match config.deployment_mode {
        DeploymentMode::Production => RunServiceConfig::production(
            config.runtime.max_concurrent_runs,
            config.runtime.max_concurrent_operations,
            config.runtime.max_concurrent_operations_per_run,
            config.runtime.subscriber_capacity,
        ),
        DeploymentMode::SingleProcessDevelopment => RunServiceConfig::single_process_development(
            config.runtime.max_concurrent_runs,
            config.runtime.max_concurrent_operations,
            config.runtime.max_concurrent_operations_per_run,
            config.runtime.subscriber_capacity,
        ),
    }
    .with_work_coordinator(WorkCoordinatorConfig {
        active_poll_interval: config.runtime.scheduler.active_poll_interval,
        idle_poll_min_interval: config.runtime.scheduler.idle_poll_min_interval,
        idle_poll_max_interval: config.runtime.scheduler.idle_poll_max_interval,
        safety_poll_interval: config.runtime.scheduler.safety_poll_interval,
        claim_batch_size: config.runtime.scheduler.claim_batch_size,
        notification_reconnect_interval: config.runtime.scheduler.notification_reconnect_interval,
    })
    .with_run_timeout(config.runtime.run_timeout)
    .with_public_event_retention(
        config.runtime.public_event_retention,
        config.runtime.public_event_prune_interval,
    )
    .with_artifact_gc(
        config.artifacts.orphan_retention,
        config.artifacts.reference_retention,
        config.artifacts.gc_interval,
        config.artifacts.deletion_claim_seconds,
    )
    .with_artifact_read_limit(config.artifacts.max_read_bytes)
    .with_live_run_stream_limits(
        config.runtime.run_stream.body_queue_capacity,
        config.runtime.run_stream.control_queue_capacity,
        config.runtime.run_stream.terminal_barrier_timeout,
        config.runtime.run_stream.outbound_write_timeout,
    )
    .with_live_run_stream_byte_limits(
        config.runtime.run_stream.max_frame_bytes,
        config.runtime.run_stream.max_item_bytes,
        config.runtime.run_stream.max_run_bytes,
    )
    .with_graph_persistence_policy(graph_persistence_policy);
    let service = RunService::start_with_artifact_store_graph_publication_and_live_run_stream(
        agents,
        repository,
        workers,
        live_run_stream_broker,
        Arc::clone(&artifact_store),
        graph_publication_resolver,
        service_config,
    )
    .await?;
    service.set_mcp_run_admission_authority(Arc::new(DurableMcpRunAdmissionAuthority::new(
        Arc::clone(&mcp_management),
        mcp_policy_material.clone(),
    )))?;
    service.set_provider_run_admission_authority(Arc::new(
        DurableProviderRunAdmissionAuthority::new(Arc::clone(&provider_management)),
    ))?;
    if config.mcp.client.management_api.enabled {
        service.add_readiness_probe(Arc::new(McpManagementStoreReadiness::new(Arc::clone(
            &mcp_management,
        ))))?;
    }
    let terminal_runtime_config = TerminalOnlyRunConfig {
        owner_lease: config.runtime.terminal_only.owner_lease,
        owner_heartbeat: config.runtime.terminal_only.owner_heartbeat,
        terminal_commit_retry: config.runtime.terminal_only.terminal_commit_retry,
        run_timeout: config.runtime.run_timeout.min(terminal_execution_budget),
        max_concurrent_runs: config.runtime.terminal_only.max_concurrent_runs,
        conversations_enabled: config.conversations.enabled,
        inline_content_max_bytes: config.conversations.inline_content_max_bytes,
        message_page_size_default: config.conversations.message_page_size_default as u32,
        message_page_size_max: config.conversations.message_page_size_max as u32,
        recent_context_messages: config.conversations.recent_context_messages as u32,
        summary_trigger_messages: config.conversations.summary_trigger_messages as u32,
        summary_trigger_tokens: config.conversations.summary_trigger_tokens,
        run_retention: std::time::Duration::from_secs(
            u64::from(config.runtime.terminal_only.run_retention_days) * 24 * 60 * 60,
        ),
        conversation_retention: std::time::Duration::from_secs(
            u64::from(config.conversations.retention_days) * 24 * 60 * 60,
        ),
        terminal_barrier_timeout: config.runtime.run_stream.terminal_barrier_timeout,
        outbound_write_timeout: config.runtime.run_stream.outbound_write_timeout,
    };
    if config.runtime.terminal_only.enabled {
        service
            .enable_terminal_only(
                terminal_store,
                format!("http://{}", config.bind_addr),
                terminal_runtime_config,
            )
            .await?;
    } else if config.conversations.enabled {
        service.enable_conversations(terminal_store, terminal_runtime_config)?;
    }
    let mut mcp_server_task_maintenance = McpServerTaskMaintenanceRuntime::start(
        service.clone(),
        Arc::clone(&mcp_server_tasks),
        config.mcp.server.enabled,
    );
    let mut agent_debug_maintenance = AgentDebugMaintenanceRuntime::start(
        Arc::clone(&agent_management),
        config.management.enabled,
    );
    let mut management_observability = ManagementObservabilityRuntime::start(
        Arc::clone(&agent_management),
        Arc::clone(&provider_management),
        provider_management_runtime.projection_health(),
        config.management.enabled,
    )
    .await?;
    if config.management.enabled {
        let probe = management_observability.probe();
        service.add_readiness_probe(probe.clone())?;
        service.add_metrics_source(probe)?;
    }

    let api_auth = build_api_auth(&config)?;
    let api_state = ApiState {
        service: service.clone(),
        auth: api_auth.clone(),
        sse_keep_alive_interval: config.runtime.sse_keep_alive_interval,
        readiness_probe_timeout: config.runtime.readiness_probe_timeout,
    };
    // MCP v2 OAuth registrations are a live projection of active durable
    // revisions. The API can start with an empty projection and is populated
    // by the reconciler without changing its router contract.
    let oauth_state = mcp_secret_protector
        .as_ref()
        .map(|protector| {
            McpOAuthApiState::from_registry(
                api_auth.clone(),
                Arc::clone(&mcp_oauth),
                Arc::clone(protector),
                mcp_oauth_registry.clone(),
                std::time::Duration::from_secs(30),
            )
            .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))
        })
        .transpose()?;
    let mut mcp_oauth_maintenance = McpOAuthMaintenanceRuntime::start(Arc::clone(&mcp_oauth));
    let mut app = match mcp_secret_protector.as_ref() {
        Some(protector) => build_router_with_mcp(
            api_state,
            Arc::clone(&mcp_interactions),
            Arc::clone(protector),
            oauth_state,
        ),
        None => build_router(api_state),
    };
    if config.management.enabled {
        let operator_auth = OperatorAuth::new(config.management.operator_credentials.iter().map(
            |credential| {
                (
                    credential.token().expose().to_owned(),
                    credential.identity().to_owned(),
                    credential.capabilities().clone(),
                )
            },
        ))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "shared management Operator authentication is invalid",
            )
        })?;
        let mut cursor_hasher = Sha256::new();
        cursor_hasher.update(b"insight-management-cursor-v1\0");
        for credential in &config.management.operator_credentials {
            cursor_hasher.update(credential.identity().as_bytes());
            cursor_hasher.update([0]);
            cursor_hasher.update(credential.token().expose().as_bytes());
            cursor_hasher.update([0]);
        }
        let cursor_signing_key: [u8; 32] = cursor_hasher.finalize().into();
        if config.mcp.client.management_api.enabled {
            let policy = McpManagementPolicy {
                preferred_protocol: config.mcp.protocol.preferred.clone(),
                legacy_protocols: config
                    .mcp
                    .protocol
                    .legacy_fallback
                    .iter()
                    .cloned()
                    .collect(),
                allowed_secret_refs: config.mcp.client.secret_resolver.allowed_names.clone(),
                resolvable_secret_refs: config
                    .mcp
                    .client
                    .secret_resolver
                    .allowed_names
                    .iter()
                    .filter(|name| std::env::var_os(name).is_some())
                    .cloned()
                    .collect(),
                stdio_profile_fingerprints: mcp_policy_material.stdio_profile_fingerprints.clone(),
                stdio_profile_allowed_parameters: mcp_policy_material
                    .stdio_profile_allowed_parameters
                    .clone(),
                allow_loopback_development: config
                    .mcp
                    .client
                    .network_policy
                    .allow_loopback_development,
                trusted_manifest_signers: config
                    .mcp
                    .client
                    .signed_manifest_trust
                    .trusted_signers
                    .iter()
                    .map(|(key_id, signer)| {
                        (
                            key_id.clone(),
                            McpManifestSignerPolicy {
                                public_key: signer.public_key.clone(),
                            },
                        )
                    })
                    .collect(),
                manifest_max_validity: config.mcp.client.signed_manifest_trust.max_validity,
                max_pending_discoveries: config.mcp.client.management_api.max_pending_discoveries,
                max_request_bytes: config.mcp.client.default_limits.limits.max_request_bytes,
                max_response_bytes: config.mcp.client.default_limits.limits.max_response_bytes,
                max_sse_line_bytes: config.mcp.client.default_limits.limits.max_sse_line_bytes,
                max_sse_event_bytes: config.mcp.client.default_limits.limits.max_sse_event_bytes,
                max_content_items: config.mcp.client.default_limits.limits.max_content_items,
                max_catalog_items: config.mcp.client.default_limits.limits.max_catalog_items,
                policy_fingerprint: mcp_policy_material.policy_fingerprint.clone(),
                cursor_signing_key,
            };
            let managed_mcp_revision = ManagedMcpRevisionReadiness::new(config.mcp.client.clone());
            app = app.merge(build_mcp_management_router(McpManagementApiState {
                auth: operator_auth.clone(),
                repository: Arc::clone(&mcp_management),
                policy: Arc::new(policy),
                authoring: Arc::new(managed_mcp_revision.clone()),
                readiness: Arc::new(managed_mcp_revision),
            }));
        }

        let provider_secret_refs = config
            .management
            .provider_secret_resolver_allowed_names
            .iter()
            .map(|name| format!("secret://environment/{name}"))
            .collect::<std::collections::BTreeSet<_>>();
        let provider_resolvable_secret_refs = config
            .management
            .provider_secret_resolver_allowed_names
            .iter()
            .filter(|name| std::env::var_os(name).is_some())
            .map(|name| format!("secret://environment/{name}"))
            .collect::<std::collections::BTreeSet<_>>();
        let provider_policy_document = serde_json::json!({
            "adapter":"open_ai_compatible",
            "adapter_version":"2.1.0",
            "allowed_secret_refs":provider_secret_refs,
            "max_models":config.management.limits.max_provider_models,
            "transport":{"tls":"required","redirects":"deny"}
        });
        let provider_policy_bytes = serde_jcs::to_vec(&provider_policy_document).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Provider policy is not canonicalizable",
            )
        })?;
        let mut provider_policy_hash = String::from("sha256:");
        for byte in Sha256::digest(&provider_policy_bytes) {
            use std::fmt::Write as _;
            write!(&mut provider_policy_hash, "{byte:02x}")?;
        }
        let mut provider_cursor_hasher = Sha256::new();
        provider_cursor_hasher.update(b"insight-provider-management-cursor-v1\0");
        provider_cursor_hasher.update(cursor_signing_key);
        let provider_cursor_signing_key: [u8; 32] = provider_cursor_hasher.finalize().into();
        let provider_template_document = serde_json::json!({
            "adapter":{"type":"open_ai_compatible","version":"2.1.0"},
            "transport":{"tls":"required","redirects":"deny"},
            "credential_slots":["bearer","api_key"],
            "model_discovery":"models_endpoint"
        });
        let provider_template_digest = {
            let bytes = serde_jcs::to_vec(&provider_template_document).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Provider template is not canonicalizable",
                )
            })?;
            let mut value = String::from("sha256:");
            for byte in Sha256::digest(bytes) {
                use std::fmt::Write as _;
                write!(&mut value, "{byte:02x}")?;
            }
            value
        };
        app = app.merge(build_provider_management_router(
            ProviderManagementApiState {
                auth: operator_auth.clone(),
                repository: Arc::clone(&provider_management),
                policy: Arc::new(ProviderManagementPolicy {
                    installed_adapters: std::collections::BTreeMap::from([(
                        "open_ai_compatible".to_owned(),
                        "2.1.0".to_owned(),
                    )]),
                    allowed_secret_refs: provider_secret_refs,
                    resolvable_secret_refs: provider_resolvable_secret_refs,
                    templates: vec![ProviderTemplate {
                        template_id: "open-ai-compatible".to_owned(),
                        adapter_type: "open_ai_compatible".to_owned(),
                        adapter_version: "2.1.0".to_owned(),
                        manifest_digest: provider_template_digest,
                        document: provider_template_document,
                    }],
                    allow_loopback_development: config
                        .mcp
                        .client
                        .network_policy
                        .allow_loopback_development,
                    max_models: config.management.limits.max_provider_models,
                    max_connect_timeout_ms: 30_000,
                    max_request_timeout_ms: 600_000,
                    max_pending_operations: config.management.limits.max_pending_operations,
                    require_connection_test_for_publish: false,
                    policy_fingerprint: provider_policy_hash,
                    cursor_signing_key: provider_cursor_signing_key,
                }),
                readiness: Arc::new(ManagedProviderRevisionReadiness::new(
                    provider_runtime_config.clone(),
                )),
            },
        ));

        let debug_profiles = config
            .management
            .debug_execution_profiles
            .iter()
            .map(|(profile_id, profile)| -> MainResult<_> {
                Ok((
                    profile_id.clone(),
                    AgentDebugProfilePolicy {
                        mode: match profile.mode {
                            DebugExecutionProfileMode::Sandbox => AgentDebugProfileMode::Sandbox,
                            DebugExecutionProfileMode::Live => AgentDebugProfileMode::Live,
                        },
                        max_concurrent_sessions: profile.max_concurrent_sessions,
                        session_timeout: chrono::Duration::from_std(profile.session_timeout)
                            .map_err(|_| {
                                io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "management Debug Session timeout is unsupported",
                                )
                            })?,
                        content_retention: chrono::Duration::from_std(profile.retention).map_err(
                            |_| {
                                io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "management Debug content retention is unsupported",
                                )
                            },
                        )?,
                        allow_external_actions: profile.allow_external_actions,
                        allow_live_provider_credentials: profile.allow_live_provider_credentials,
                    },
                ))
            })
            .collect::<MainResult<std::collections::BTreeMap<_, _>>>()?;
        let validation_policy_document = serde_json::json!({
            "schema_version":1,
            "max_agent_draft_bytes":config.management.limits.max_agent_draft_bytes,
            "max_agent_prompt_files":config.management.limits.max_agent_prompt_files,
            "deployment_mode":match config.deployment_mode {
                DeploymentMode::Production => "production",
                DeploymentMode::SingleProcessDevelopment => "single_process_development",
            }
        });
        let validation_policy_digest = {
            let bytes = serde_jcs::to_vec(&validation_policy_document).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Agent validation policy is invalid",
                )
            })?;
            let mut value = String::from("sha256:");
            for byte in Sha256::digest(bytes) {
                use std::fmt::Write as _;
                write!(&mut value, "{byte:02x}")?;
            }
            value
        };
        let mut agent_cursor_hasher = Sha256::new();
        agent_cursor_hasher.update(b"insight-agent-management-cursor-v1\0");
        agent_cursor_hasher.update(cursor_signing_key);
        app = app.merge(build_agent_management_router(AgentManagementApiState {
            auth: operator_auth,
            repository: Arc::clone(&agent_management),
            run_service: service.clone(),
            debug_stream_keep_alive_interval: config.runtime.sse_keep_alive_interval,
            policy: Arc::new(AgentManagementPolicy {
                validation_policy_digest,
                cursor_signing_key: agent_cursor_hasher.finalize().into(),
                max_prompt_files: config.management.limits.max_agent_prompt_files,
                max_prompt_file_bytes: config.management.limits.max_agent_draft_bytes,
                max_package_bytes: config.management.limits.max_agent_draft_bytes,
                resolution_ttl: chrono::Duration::minutes(10),
                debug_profiles,
            }),
        }));
    }
    let mut mcp_profiles = McpProfileReport::default();
    mcp_profiles.modern_client.enabled = config.mcp.client.enabled;
    mcp_profiles.modern_server.enabled = config.mcp.server.enabled;
    mcp_profiles.tasks.enabled = config.mcp.server.exports.agents.iter().any(|agent| {
        agent.execution != insight_agent_platform::config::McpExportExecutionConfig::Synchronous
    });
    mcp_profiles.legacy_client.enabled = config
        .mcp
        .protocol
        .legacy_fallback
        .iter()
        .any(|version| version == insight_agent_platform::config::MCP_LEGACY_PROTOCOL_VERSION);
    app = app.merge(build_mcp_catalog_router(
        McpCatalogApiState::from_registry(
            api_auth.clone(),
            Arc::clone(&mcp_oauth),
            mcp_catalog_registry,
        )
        .with_run_service(service.clone())
        .with_profile_report(mcp_profiles),
    ));
    if config.mcp.server.enabled {
        let backend = PlatformMcpServerBackend::new(
            &config.mcp.server,
            &actions,
            service.clone(),
            mcp_server_tasks,
            Arc::clone(&mcp_interactions),
            mcp_secret_protector.clone(),
            config.runtime.operation_timeout,
        )?;
        let mcp_service = Arc::new(McpServerDispatcher::new(backend)) as Arc<dyn McpHttpService>;
        let mut protected_resource_metadata = None;
        let mcp_authorizer: Arc<dyn McpHttpAuthorizer> = match &config.mcp.server.authorization {
            insight_agent_platform::config::McpServerAuthorizationConfig::Disabled => {
                Arc::new(DisabledMcpHttpAuthorizer)
            }
            insight_agent_platform::config::McpServerAuthorizationConfig::BearerCompatible => {
                Arc::new(ApiAuthMcpHttpAuthorizer::new(api_auth))
            }
            insight_agent_platform::config::McpServerAuthorizationConfig::OauthResourceServer {
                resource,
                authorization_servers,
                required_scopes,
            } => {
                protected_resource_metadata = Some(
                    oauth_protected_resource_metadata(
                        resource.clone(),
                        authorization_servers.clone(),
                        required_scopes.clone(),
                    )
                    .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?,
                );
                Arc::new(
                    OAuthResourceServerAuthorizer::discover(
                        resource.clone(),
                        authorization_servers.clone(),
                        required_scopes.clone(),
                        config
                            .runtime
                            .operation_timeout
                            .min(std::time::Duration::from_secs(30)),
                    )
                    .await
                    .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?,
                )
            }
        };
        app = app.merge(
            build_mcp_server_router(
                &config.mcp.server.endpoint,
                mcp_service,
                mcp_authorizer,
                None,
            )
            .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?,
        );
        if let Some(metadata) = protected_resource_metadata {
            app = app.merge(
                build_mcp_protected_resource_metadata_router(metadata)
                    .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?,
            );
        }
    }
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tracing::info!(
        bind_addr = %config.bind_addr,
        agents = service.agents().list().count(),
        "durable runtime listening"
    );

    let (http_shutdown, wait_http_shutdown) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = wait_http_shutdown.await;
        })
        .into_future();
    tokio::pin!(server);

    tokio::select! {
        result = &mut server => {
            service.begin_shutdown();
            mcp_discovery_runtime.shutdown().await;
            mcp_revision_runtime.shutdown().await;
            provider_management_runtime.shutdown().await;
            agent_debug_maintenance.shutdown().await;
            management_observability.shutdown().await;
            mcp_oauth_maintenance.shutdown().await;
            mcp_server_task_maintenance.shutdown().await;
            service.shutdown(config.runtime.shutdown_grace_period).await?;
            result?;
            Err(io::Error::other("HTTP server stopped before a shutdown signal").into())
        }
        signal = wait_for_shutdown_signal() => {
            signal?;
            tracing::info!("shutdown signal received");
            service.begin_shutdown();
            let _ = http_shutdown.send(());
            mcp_discovery_runtime.shutdown().await;
            mcp_revision_runtime.shutdown().await;
            provider_management_runtime.shutdown().await;
            agent_debug_maintenance.shutdown().await;
            management_observability.shutdown().await;
            mcp_oauth_maintenance.shutdown().await;
            mcp_server_task_maintenance.shutdown().await;
            tokio::time::timeout(config.runtime.shutdown_hard_deadline, async {
                service.shutdown(config.runtime.shutdown_grace_period).await?;
                (&mut server).await?;
                Ok::<(), Box<dyn Error + Send + Sync>>(())
            })
            .await
            .map_err(|_| io::Error::new(
                io::ErrorKind::TimedOut,
                "server shutdown exceeded its hard deadline",
            ))??;
            Ok(())
        }
        self_abort = qualification_self_abort.wait() => {
            mcp_discovery_runtime.shutdown().await;
            mcp_revision_runtime.shutdown().await;
            provider_management_runtime.shutdown().await;
            agent_debug_maintenance.shutdown().await;
            management_observability.shutdown().await;
            self_abort?;
            tracing::error!(
                code = "QUALIFICATION_SELF_ABORT",
                "qualification-only process self-abort triggered"
            );
            // Let the short-lived `kubectl exec` signal sender observe a
            // successful `kill(2)` before PID 1 terminates. The harness still
            // proves the subsequent abort independently from Pod status and
            // the previous-container log marker.
            tokio::time::sleep(QUALIFICATION_SELF_ABORT_HANDOFF_DELAY).await;
            std::process::abort();
        }
    }
}

struct McpServerTaskMaintenanceRuntime {
    cancellation: CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl McpServerTaskMaintenanceRuntime {
    fn start(
        service: RunService,
        repository: Arc<dyn McpServerTaskDurableRepository>,
        enabled: bool,
    ) -> Self {
        const BATCH_SIZE: u32 = 128;
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let handle = enabled.then(|| {
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        _ = task_cancellation.cancelled() => return,
                        _ = interval.tick() => {}
                    }
                    let now = chrono::Utc::now();
                    let expired = match repository
                        .list_expired_mcp_server_tasks(now, BATCH_SIZE)
                        .await
                    {
                        Ok(expired) => expired,
                        Err(_) => {
                            tracing::warn!(
                                code = "MCP_SERVER_TASK_REAPER_UNAVAILABLE",
                                "MCP Server task expiry scan failed"
                            );
                            continue;
                        }
                    };
                    for task in expired {
                        if task_cancellation.is_cancelled() {
                            return;
                        }
                        let cancelled = service
                            .cancel_for_principal(
                                task.principal().tenant_id(),
                                Some(task.principal().user_id()),
                                task.run_id(),
                            )
                            .await;
                        if cancelled.is_err() {
                            tracing::warn!(
                                code = "MCP_SERVER_TASK_EXPIRED_RUN_CANCEL_FAILED",
                                "MCP Server task expiry could not cancel its Run"
                            );
                            continue;
                        }
                        match repository
                            .delete_expired_mcp_server_task(
                                task.task_id(),
                                task.expires_at(),
                                chrono::Utc::now(),
                            )
                            .await
                        {
                            Ok(true) => tracing::info!(
                                "expired MCP Server task authority after Run cancellation"
                            ),
                            Ok(false) => {}
                            Err(_) => tracing::warn!(
                                code = "MCP_SERVER_TASK_EXPIRED_DELETE_FAILED",
                                "MCP Server task expiry cleanup failed"
                            ),
                        }
                    }
                }
            })
        });
        Self {
            cancellation,
            handle,
        }
    }

    async fn shutdown(&mut self) {
        self.cancellation.cancel();
        if let Some(handle) = self.handle.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }
    }
}

impl Drop for McpServerTaskMaintenanceRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

struct AgentDebugMaintenanceRuntime {
    cancellation: CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl AgentDebugMaintenanceRuntime {
    fn start(repository: Arc<dyn AgentManagementDurableRepository>, enabled: bool) -> Self {
        const BATCH_SIZE: u32 = 256;
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let handle = enabled.then(|| {
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = task_cancellation.cancelled() => return,
                        _ = interval.tick() => {}
                    }
                    match repository
                        .cleanup_expired_agent_debug_sessions(chrono::Utc::now(), BATCH_SIZE)
                        .await
                    {
                        Ok(expired) if expired >= u64::from(BATCH_SIZE) => {
                            tokio::task::yield_now().await;
                        }
                        Ok(_) => {}
                        Err(_) => tracing::warn!(
                            code = "AGENT_DEBUG_RETENTION_UNAVAILABLE",
                            "Agent Debug expiry and content-retention cleanup failed"
                        ),
                    }
                }
            })
        });
        Self {
            cancellation,
            handle,
        }
    }

    async fn shutdown(&mut self) {
        self.cancellation.cancel();
        if let Some(handle) = self.handle.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }
    }
}

impl Drop for AgentDebugMaintenanceRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

struct McpOAuthMaintenanceRuntime {
    cancellation: CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl McpOAuthMaintenanceRuntime {
    fn start(repository: Arc<dyn McpOAuthDurableRepository>) -> Self {
        const BATCH_SIZE: u32 = 256;
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = task_cancellation.cancelled() => return,
                    _ = interval.tick() => {}
                }
                loop {
                    match repository
                        .expire_mcp_oauth_transactions(chrono::Utc::now(), BATCH_SIZE)
                        .await
                    {
                        Ok(0) => break,
                        Ok(expired) => {
                            tracing::info!(expired, "expired MCP OAuth authorization transactions");
                            if expired < u64::from(BATCH_SIZE) {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                        Err(_) => {
                            tracing::warn!(
                                code = "MCP_OAUTH_TRANSACTION_REAPER_UNAVAILABLE",
                                "MCP OAuth transaction expiry pass failed"
                            );
                            break;
                        }
                    }
                    if task_cancellation.is_cancelled() {
                        return;
                    }
                }
            }
        });
        Self {
            cancellation,
            handle: Some(handle),
        }
    }

    async fn shutdown(&mut self) {
        self.cancellation.cancel();
        if let Some(handle) = self.handle.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }
    }
}

impl Drop for McpOAuthMaintenanceRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

struct QualificationSelfAbortControl {
    enabled: bool,
    #[cfg(unix)]
    signal: Option<tokio::signal::unix::Signal>,
}

impl QualificationSelfAbortControl {
    fn prepare(enabled: bool) -> io::Result<Self> {
        #[cfg(unix)]
        {
            let signal = if enabled {
                Some(tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::user_defined2(),
                )?)
            } else {
                None
            };
            Ok(Self { enabled, signal })
        }
        #[cfg(not(unix))]
        {
            if enabled {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "qualification self-abort requires SIGUSR2 support",
                ));
            }
            Ok(Self { enabled })
        }
    }

    async fn wait(&mut self) -> io::Result<()> {
        if !self.enabled {
            return std::future::pending().await;
        }
        #[cfg(unix)]
        {
            self.signal
                .as_mut()
                .expect("enabled qualification control owns a SIGUSR2 stream")
                .recv()
                .await
                .ok_or_else(|| io::Error::other("qualification SIGUSR2 stream closed"))
        }
        #[cfg(not(unix))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "qualification self-abort requires SIGUSR2 support",
            ))
        }
    }
}

fn qualification_enabled_from_environment() -> io::Result<bool> {
    qualification_enabled_value(std::env::var_os(QUALIFICATION_ENABLED_ENV).as_deref())
}

fn qualification_enabled_value(value: Option<&OsStr>) -> io::Result<bool> {
    match value.and_then(OsStr::to_str) {
        None if value.is_none() => Ok(false),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "INSIGHT_QUALIFICATION_ENABLED must be true or false",
        )),
    }
}

fn build_api_auth(config: &PlatformConfig) -> MainResult<ApiAuth> {
    let base = ApiAuth::from(&config.auth);
    if config.human_task_credentials.is_empty() {
        return Ok(base);
    }

    let resolver =
        BearerHumanPrincipalResolver::new(config.human_task_credentials.iter().map(|credential| {
            (
                credential.token().expose().to_owned(),
                credential.identity().to_owned(),
                credential.groups().iter().cloned().collect(),
            )
        }))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "HumanTask credential configuration is invalid",
            )
        })?;
    Ok(base.with_human_principal_resolver(Arc::new(resolver)))
}

async fn initialize_repository_and_live_run_stream(
    config: &HistoryConfig,
    run_stream: RunStreamConfig,
) -> MainResult<(
    Arc<dyn ProductionRunRepository>,
    Arc<dyn TerminalOnlyStore>,
    Arc<dyn LiveRunStreamBroker>,
    Arc<dyn McpInteractionDurableRepository>,
    Arc<dyn McpOAuthDurableRepository>,
    Arc<dyn McpRemoteTaskDurableRepository>,
    Arc<dyn McpServerTaskDurableRepository>,
    Arc<dyn McpManagementDurableRepository>,
    Arc<dyn ProviderManagementDurableRepository>,
    Arc<dyn AgentManagementDurableRepository>,
)> {
    match config {
        HistoryConfig::Sqlite { path } => {
            if run_stream.broker != LiveRunStreamBrokerProvider::InProcess {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "SQLite history supports only the in-process live Run stream broker",
                )
                .into());
            }
            let concrete = Arc::new(SqliteDurableRepository::connect_path(path).await?);
            let repository: Arc<dyn ProductionRunRepository> = concrete.clone();
            let terminal_store: Arc<dyn TerminalOnlyStore> = concrete.clone();
            let mcp_interactions: Arc<dyn McpInteractionDurableRepository> = concrete.clone();
            let mcp_oauth: Arc<dyn McpOAuthDurableRepository> = concrete.clone();
            let mcp_remote_tasks: Arc<dyn McpRemoteTaskDurableRepository> = concrete.clone();
            let mcp_server_tasks: Arc<dyn McpServerTaskDurableRepository> = concrete.clone();
            let mcp_management: Arc<dyn McpManagementDurableRepository> = concrete.clone();
            let provider_management: Arc<dyn ProviderManagementDurableRepository> =
                concrete.clone();
            let agent_management: Arc<dyn AgentManagementDurableRepository> = concrete;
            let broker = Arc::new(InMemoryLiveRunStreamBroker::new_with_limits(
                run_stream.body_queue_capacity,
                run_stream.control_queue_capacity,
                LiveRunStreamByteLimits::new(
                    run_stream.max_frame_bytes,
                    run_stream.max_item_bytes,
                    run_stream.max_run_bytes,
                )?,
            )?) as Arc<dyn LiveRunStreamBroker>;
            Ok((
                repository,
                terminal_store,
                broker,
                mcp_interactions,
                mcp_oauth,
                mcp_remote_tasks,
                mcp_server_tasks,
                mcp_management,
                provider_management,
                agent_management,
            ))
        }
        HistoryConfig::Postgres {
            database_url,
            max_connections,
        } => {
            let database_url = runtime_postgres_url(database_url.expose());
            let repository = PostgresDurableRepository::connect_with_max_connections(
                &database_url,
                *max_connections,
            )
            .await?;
            let broker: Arc<dyn LiveRunStreamBroker> = match run_stream.broker {
                LiveRunStreamBrokerProvider::InProcess => {
                    Arc::new(InMemoryLiveRunStreamBroker::new_with_limits(
                        run_stream.body_queue_capacity,
                        run_stream.control_queue_capacity,
                        LiveRunStreamByteLimits::new(
                            run_stream.max_frame_bytes,
                            run_stream.max_item_bytes,
                            run_stream.max_run_bytes,
                        )?,
                    )?)
                }
                LiveRunStreamBrokerProvider::PostgresNotify => Arc::new(
                    PostgresLiveRunStreamBroker::start(
                        repository.connection_pool(),
                        PostgresLiveRunStreamBrokerOptions::new(
                            run_stream.body_queue_capacity,
                            run_stream.control_queue_capacity,
                            run_stream.max_frame_bytes,
                        )?
                        .with_publication_limits(
                            run_stream.max_item_bytes,
                            run_stream.max_run_bytes,
                        )?,
                    )
                    .await?,
                ),
            };
            let concrete = Arc::new(repository);
            let production: Arc<dyn ProductionRunRepository> = concrete.clone();
            let terminal_store: Arc<dyn TerminalOnlyStore> = concrete.clone();
            let mcp_interactions: Arc<dyn McpInteractionDurableRepository> = concrete.clone();
            let mcp_oauth: Arc<dyn McpOAuthDurableRepository> = concrete.clone();
            let mcp_remote_tasks: Arc<dyn McpRemoteTaskDurableRepository> = concrete.clone();
            let mcp_server_tasks: Arc<dyn McpServerTaskDurableRepository> = concrete.clone();
            let mcp_management: Arc<dyn McpManagementDurableRepository> = concrete.clone();
            let provider_management: Arc<dyn ProviderManagementDurableRepository> =
                concrete.clone();
            let agent_management: Arc<dyn AgentManagementDurableRepository> = concrete;
            Ok((
                production,
                terminal_store,
                broker,
                mcp_interactions,
                mcp_oauth,
                mcp_remote_tasks,
                mcp_server_tasks,
                mcp_management,
                provider_management,
                agent_management,
            ))
        }
    }
}

fn runtime_postgres_url(database_url: &str) -> String {
    let separator = if database_url.contains('?') { '&' } else { '?' };
    format!("{database_url}{separator}application_name={RUNTIME_POSTGRES_APPLICATION_NAME}")
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

fn install_sanitized_panic_hook() {
    std::panic::set_hook(Box::new(|_| {
        tracing::error!(code = PROCESS_PANICKED_CODE, "{PROCESS_PANICKED_MESSAGE}");
    }));
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        received = terminate.recv() => received
            .map(|_| ())
            .ok_or_else(|| io::Error::other("SIGTERM signal stream closed")),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualification_self_abort_enablement_is_closed() {
        assert!(!qualification_enabled_value(None).unwrap());
        assert!(qualification_enabled_value(Some(OsStr::new("true"))).unwrap());
        assert!(!qualification_enabled_value(Some(OsStr::new("false"))).unwrap());
        assert!(qualification_enabled_value(Some(OsStr::new("1"))).is_err());
        assert!(qualification_enabled_value(Some(OsStr::new("TRUE"))).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn qualification_self_abort_receives_handled_sigusr2() {
        let mut control = QualificationSelfAbortControl::prepare(true).unwrap();
        let process_id = std::process::id().to_string();
        let sender = std::process::Command::new("kill")
            .args(["-USR2", process_id.as_str()])
            .status()
            .unwrap();
        assert!(sender.success());
        tokio::time::timeout(std::time::Duration::from_secs(1), control.wait())
            .await
            .unwrap()
            .unwrap();
    }
}
