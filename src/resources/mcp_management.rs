//! Background MCP discovery workers for durable management operations.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::Utc;
use insight_api::v1::{
    McpCatalogRegistry, McpDraftAuthorization, McpDraftDiscovery, McpDraftDocument,
    McpDraftTransport, McpOAuthRegistry, McpRevisionAuthoring, McpRevisionReadiness,
};
use insight_durable::{
    ClaimMcpDiscoveriesCommand, CompleteMcpDiscoveryCommand, CompleteMcpDiscoveryResult,
    MarkMcpDiscoveryStaleCommand, McpDiscoveryClaim, McpDiscoveryFailure, McpManagedServerState,
    McpManagementDurableRepository,
};
use insight_mcp::{
    ClientCapabilities, ClientError, ClientInfo, HttpCredential, LegacyCompatibilityTransport,
    McpClient, McpClientLimits, McpGetPromptOutcome, McpTransport, NoopNotificationObserver,
    OAuthClient, OAuthClientRegistration, Prompt, ProtocolLimits, StdioTransport,
    StdioTransportPolicy, StreamableHttpPolicy, StreamableHttpTransport, TransportError,
    MCP_LEGACY_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION,
};
use insight_runtime::{McpRunAdmissionAuthority, RuntimeReadinessProbe, ServiceError};
use ring::signature;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::{task::JoinSet, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::config::McpClientConfig;
use crate::resources::{
    actions::ActionRegistry,
    mcp::{
        publish_managed_mcp_revision_actions, McpClientSubscription, McpExecutionFence,
        McpInteractionHandler, McpOAuthRuntimeAuthority, McpRemoteTaskRuntimeAuthority,
    },
    retrievals::RetrievalRegistry,
};

#[derive(Debug, Clone)]
pub struct McpManagementPolicyMaterial {
    pub policy_fingerprint: String,
    pub stdio_profile_fingerprints: BTreeMap<String, String>,
    pub stdio_profile_allowed_parameters: BTreeMap<String, BTreeSet<String>>,
}

pub fn mcp_management_policy_material(
    config: &McpClientConfig,
    preferred_protocol: &str,
    legacy_protocols: &[String],
) -> Result<McpManagementPolicyMaterial, serde_json::Error> {
    let stdio_profile_fingerprints = config
        .stdio_launch_profiles
        .iter()
        .map(|(profile_id, profile)| {
            let document = json!({
                "executable":profile.executable,
                "fixed_args":profile.fixed_args,
                "working_directory":profile.working_directory,
                "allowed_parameters":profile.allowed_parameters,
                "secret_environment":profile.secret_environment,
                "isolation_profile":profile.isolation_profile,
            });
            serde_jcs::to_vec(&document).map(|bytes| (profile_id.clone(), prefixed_sha256(&bytes)))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let stdio_profile_allowed_parameters = config
        .stdio_launch_profiles
        .iter()
        .map(|(profile_id, profile)| (profile_id.clone(), profile.allowed_parameters.clone()))
        .collect::<BTreeMap<_, _>>();
    let manifest_signer_fingerprints = config
        .signed_manifest_trust
        .trusted_signers
        .iter()
        .map(|(key_id, signer)| (key_id.clone(), prefixed_sha256(&signer.public_key)))
        .collect::<BTreeMap<_, _>>();
    let policy_document = json!({
        "protocol":{"preferred":preferred_protocol,"legacy":legacy_protocols},
        "network":{
            "allow_loopback_development":config.network_policy.allow_loopback_development,
            "allow_private_networks":config.network_policy.allow_private_networks,
            "allow_redirects":config.network_policy.allow_redirects,
        },
        "timeouts":{
            "connect_ms":config.default_limits.connect_timeout.as_millis(),
            "request_ms":config.default_limits.request_timeout.as_millis(),
        },
        "limits":{
            "max_request_bytes":config.default_limits.limits.max_request_bytes,
            "max_response_bytes":config.default_limits.limits.max_response_bytes,
            "max_sse_line_bytes":config.default_limits.limits.max_sse_line_bytes,
            "max_sse_event_bytes":config.default_limits.limits.max_sse_event_bytes,
            "max_content_items":config.default_limits.limits.max_content_items,
            "max_catalog_items":config.default_limits.limits.max_catalog_items,
        },
        "secret_resolver_allowed_names":config.secret_resolver.allowed_names,
        "stdio_profile_fingerprints":stdio_profile_fingerprints,
        "signed_manifest_trust":{
            "max_validity_ms":config.signed_manifest_trust.max_validity.as_millis(),
            "signer_fingerprints":manifest_signer_fingerprints,
        },
    });
    Ok(McpManagementPolicyMaterial {
        policy_fingerprint: prefixed_sha256(&serde_jcs::to_vec(&policy_document)?),
        stdio_profile_fingerprints,
        stdio_profile_allowed_parameters,
    })
}

#[derive(Clone)]
pub struct ManagedMcpRevisionReadiness {
    config: McpClientConfig,
}

impl ManagedMcpRevisionReadiness {
    pub fn new(config: McpClientConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl McpRevisionAuthoring for ManagedMcpRevisionReadiness {
    async fn snapshot_definition_prompts(
        &self,
        server_id: &str,
        draft: &McpDraftDocument,
        snapshot: &insight_durable::McpDiscoverySnapshot,
    ) -> Result<BTreeMap<String, Value>, ()> {
        let requests = draft
            .imports
            .prompts
            .iter()
            .filter_map(|prompt| {
                prompt
                    .definition_arguments
                    .clone()
                    .map(|arguments| (prompt.remote.clone(), arguments))
            })
            .collect::<BTreeMap<_, _>>();
        if requests.is_empty() {
            return Ok(BTreeMap::new());
        }
        if matches!(
            draft.authorization,
            Some(McpDraftAuthorization::OauthUser { .. })
        ) {
            return Err(());
        }
        let prompts = snapshot
            .document
            .get("prompts")
            .and_then(Value::as_array)
            .ok_or(())?
            .iter()
            .cloned()
            .map(serde_json::from_value::<Prompt>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ())?
            .into_iter()
            .map(|prompt| (prompt.name.clone(), prompt))
            .collect::<BTreeMap<_, _>>();
        if requests.keys().any(|name| !prompts.contains_key(name)) {
            return Err(());
        }

        let selected_protocol = snapshot
            .document
            .get("selected_protocol")
            .and_then(Value::as_str)
            .unwrap_or(&draft.protocol.preferred);
        let transport = build_transport(&self.config, server_id, draft)?;
        let limits = resolved_limits(&self.config, draft);
        let client_limits = McpClientLimits {
            max_tools: limits.max_catalog_items,
            max_resources: limits.max_catalog_items,
            max_prompts: limits.max_catalog_items,
            max_content_items: limits.max_content_items,
            max_content_bytes: limits.max_response_bytes,
            ..McpClientLimits::default()
        };
        let client = build_discovery_client(
            Arc::clone(&transport),
            ClientInfo {
                name: "insight-agent-platform".to_owned(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                title: Some("Insight Agent Platform".to_owned()),
                description: None,
                website_url: None,
                icons: Vec::new(),
            },
            client_limits,
            limits.max_request_bytes.max(limits.max_response_bytes),
            selected_protocol,
        )
        .map_err(|_| ())?;
        let cancellation = CancellationToken::new();
        let authored = async {
            client
                .discover(&cancellation, &NoopNotificationObserver)
                .await
                .map_err(|_| ())?;
            let mut results = BTreeMap::new();
            for (name, arguments) in requests {
                let prompt = prompts.get(&name).ok_or(())?;
                let result = client
                    .get_prompt(
                        prompt,
                        arguments,
                        None,
                        None,
                        &cancellation,
                        &NoopNotificationObserver,
                    )
                    .await
                    .map_err(|_| ())?;
                let McpGetPromptOutcome::Complete(result) = result else {
                    return Err(());
                };
                results.insert(name, serde_json::to_value(result).map_err(|_| ())?);
            }
            Ok(results)
        }
        .await;
        transport.shutdown().await.map_err(|_| ())?;
        authored
    }
}

#[async_trait::async_trait]
impl McpRevisionReadiness for ManagedMcpRevisionReadiness {
    async fn probe(&self, revision: &insight_durable::McpServerRevision) -> Result<String, ()> {
        let draft: McpDraftDocument =
            serde_json::from_value(revision.document.get("draft").cloned().ok_or(())?)
                .map_err(|_| ())?;
        let selected_protocol = revision
            .document
            .pointer("/discovery/selected_protocol")
            .and_then(Value::as_str)
            .unwrap_or(&draft.protocol.preferred);
        let timeout = self
            .config
            .default_limits
            .request_timeout
            .checked_mul(2)
            .unwrap_or(Duration::from_secs(300));
        let observed: String = if let Some(McpDraftAuthorization::OauthUser {
            scopes,
            client_id,
            redirect_uri,
        }) = &draft.authorization
        {
            let endpoint = match draft.transport.as_ref().ok_or(())? {
                McpDraftTransport::StreamableHttp { endpoint } => endpoint,
                McpDraftTransport::Stdio { .. } => return Err(()),
            };
            tokio::time::timeout(timeout, async {
                let client =
                    OAuthClient::new(self.config.default_limits.request_timeout).map_err(|_| ())?;
                let protected = client
                    .discover_protected_resource(endpoint)
                    .await
                    .map_err(|_| ())?;
                let registration = OAuthClientRegistration::new(
                    client_id.clone(),
                    redirect_uri.clone(),
                    scopes.clone(),
                )
                .map_err(|_| ())?;
                let mut compatible = false;
                for issuer in &protected.authorization_servers {
                    let authorization_server = client
                        .discover_authorization_server(issuer)
                        .await
                        .map_err(|_| ())?;
                    if client
                        .begin_authorization(&protected, &authorization_server, &registration)
                        .is_ok()
                    {
                        compatible = true;
                        break;
                    }
                }
                compatible.then_some("oauth_metadata".to_owned()).ok_or(())
            })
            .await
            .map_err(|_| ())??
        } else {
            let transport = build_transport(&self.config, &revision.server_id, &draft)?;
            let limits = resolved_limits(&self.config, &draft);
            let client_limits = McpClientLimits {
                max_tools: limits.max_catalog_items,
                max_resources: limits.max_catalog_items,
                max_prompts: limits.max_catalog_items,
                max_content_items: limits.max_content_items,
                max_content_bytes: limits.max_response_bytes,
                ..McpClientLimits::default()
            };
            let client = build_discovery_client(
                Arc::clone(&transport),
                ClientInfo {
                    name: "insight-agent-platform".to_owned(),
                    version: env!("CARGO_PKG_VERSION").to_owned(),
                    title: Some("Insight Agent Platform".to_owned()),
                    description: None,
                    website_url: None,
                    icons: Vec::new(),
                },
                client_limits,
                limits.max_request_bytes.max(limits.max_response_bytes),
                selected_protocol,
            )
            .map_err(|_| ())?;
            let cancellation = CancellationToken::new();
            let protocol = tokio::time::timeout(
                timeout,
                client.discover(&cancellation, &NoopNotificationObserver),
            )
            .await
            .map_err(|_| ())?
            .map(|_| client.protocol_version().to_owned())
            .map_err(|_| ())?;
            transport.shutdown().await.map_err(|_| ())?;
            protocol
        };
        canonical_hash(&json!({
            "revision_id":revision.revision_id,
            "revision_hash":revision.revision_hash,
            "selected_protocol":selected_protocol,
            "observed":observed,
        }))
        .map_err(|_| ())
    }
}

#[derive(Debug)]
pub struct McpDiscoveryRuntime {
    cancellation: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl McpDiscoveryRuntime {
    pub fn start(
        repository: Arc<dyn McpManagementDurableRepository>,
        config: McpClientConfig,
        policy_fingerprint: String,
    ) -> Self {
        let cancellation = CancellationToken::new();
        if !config.enabled || !config.management_api.enabled {
            return Self {
                cancellation,
                task: None,
            };
        }
        let worker_count = config.management_api.discovery_workers.max(1);
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut retention_interval = tokio::time::interval(Duration::from_secs(60));
            retention_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut metrics_interval = tokio::time::interval(Duration::from_secs(1));
            metrics_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut running = JoinSet::new();
            loop {
                tokio::select! {
                    _ = task_cancellation.cancelled() => break,
                    _ = metrics_interval.tick() => {
                        if let Ok(stats) = repository.load_mcp_management_runtime_stats().await {
                            let oldest_age = stats.oldest_open_discovery_at
                                .map(|created_at| Utc::now().signed_duration_since(created_at).num_seconds().max(0) as u64)
                                .unwrap_or(0);
                            insight_mcp::set_management_gauge(insight_mcp::McpManagementGauge::DiscoveryPending, stats.pending_discoveries);
                            insight_mcp::set_management_gauge(insight_mcp::McpManagementGauge::DiscoveryRunning, stats.running_discoveries);
                            insight_mcp::set_management_gauge(insight_mcp::McpManagementGauge::DiscoveryOldestAgeSeconds, oldest_age);
                            insight_mcp::set_management_gauge(insight_mcp::McpManagementGauge::ActiveServers, stats.active_servers);
                            insight_mcp::set_management_gauge(insight_mcp::McpManagementGauge::DisabledServers, stats.disabled_servers);
                            insight_mcp::set_management_gauge(insight_mcp::McpManagementGauge::StaleServers, stats.stale_servers);
                        }
                    }
                    _ = retention_interval.tick() => {
                        let now = Utc::now();
                        let finished_before = now - chrono::Duration::days(30);
                        if repository
                            .cleanup_terminal_mcp_discoveries(finished_before, now, 1_000)
                            .await
                            .is_err()
                        {
                            tracing::warn!(
                                code = "MCP_DISCOVERY_RETENTION_FAILED",
                                "MCP terminal discovery retention batch failed"
                            );
                        }
                    }
                    _ = interval.tick(), if running.len() < worker_count as usize => {
                        let available = worker_count as usize - running.len();
                        let now = Utc::now();
                        let operation_timeout = config.default_limits.request_timeout
                            .checked_mul(4)
                            .unwrap_or(Duration::from_secs(600));
                        let lease_duration = operation_timeout
                            .checked_add(Duration::from_secs(30))
                            .unwrap_or(Duration::from_secs(630));
                        let lease_duration = chrono::Duration::from_std(lease_duration)
                            .unwrap_or_else(|_| chrono::Duration::seconds(630));
                        let claims = repository.claim_mcp_discoveries(ClaimMcpDiscoveriesCommand {
                            worker_id: format!("mcp-discovery-{}", std::process::id()),
                            now,
                            lease_expires_at: now + lease_duration,
                            limit: u32::try_from(available).unwrap_or(worker_count),
                        }).await;
                        match claims {
                            Ok(claims) => for claim in claims {
                                let repository = Arc::clone(&repository);
                                let config = config.clone();
                                let policy_fingerprint = policy_fingerprint.clone();
                                let cancellation = task_cancellation.child_token();
                                running.spawn(async move {
                                    process_claim(repository, config, policy_fingerprint, claim, cancellation).await;
                                });
                            },
                            Err(_) => tracing::warn!(code="MCP_DISCOVERY_CLAIM_FAILED", "MCP discovery claim failed"),
                        }
                    }
                    Some(_) = running.join_next(), if !running.is_empty() => {}
                }
            }
            task_cancellation.cancel();
            while running.join_next().await.is_some() {}
        });
        Self {
            cancellation,
            task: Some(task),
        }
    }

    pub async fn shutdown(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for McpDiscoveryRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[derive(Clone)]
struct DurableExecutionFence {
    repository: Arc<dyn McpManagementDurableRepository>,
}

fn revision_policy_compatible(
    revision: &insight_durable::McpServerRevision,
    policy: &McpManagementPolicyMaterial,
) -> bool {
    if revision
        .document
        .get("policy_fingerprint")
        .and_then(Value::as_str)
        != Some(policy.policy_fingerprint.as_str())
    {
        return false;
    }
    let Some(transport_type) = revision
        .document
        .pointer("/draft/transport/type")
        .and_then(Value::as_str)
    else {
        return false;
    };
    match transport_type {
        "streamable_http" => true,
        "stdio" => {
            let Some(profile_id) = revision
                .document
                .pointer("/draft/transport/launch_profile_id")
                .and_then(Value::as_str)
            else {
                return false;
            };
            revision
                .document
                .get("stdio_profile_fingerprint")
                .and_then(Value::as_str)
                .zip(
                    policy
                        .stdio_profile_fingerprints
                        .get(profile_id)
                        .map(String::as_str),
                )
                .is_some_and(|(stored, current)| stored == current)
        }
        _ => false,
    }
}

#[derive(Clone)]
pub struct DurableMcpRunAdmissionAuthority {
    repository: Arc<dyn McpManagementDurableRepository>,
    policy: McpManagementPolicyMaterial,
}

impl DurableMcpRunAdmissionAuthority {
    pub fn new(
        repository: Arc<dyn McpManagementDurableRepository>,
        policy: McpManagementPolicyMaterial,
    ) -> Self {
        Self { repository, policy }
    }
}

#[async_trait::async_trait]
impl McpRunAdmissionAuthority for DurableMcpRunAdmissionAuthority {
    async fn active_fences(
        &self,
        server_ids: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, u64>, ServiceError> {
        let mut fences = BTreeMap::new();
        for server_id in server_ids {
            let fence = self
                .repository
                .load_mcp_server_fence(server_id)
                .await
                .map_err(|_| {
                    ServiceError::new(
                        "RUN_SERVICE_UNAVAILABLE",
                        "MCP Run admission authority is unavailable",
                    )
                })?
                .filter(|fence| fence.state == McpManagedServerState::Active)
                .ok_or_else(|| {
                    ServiceError::new(
                        "MCP_SERVER_DISABLED",
                        "MCP Server is not active for Run admission",
                    )
                })?;
            let revision_id = fence.active_revision_id.as_deref().ok_or_else(|| {
                ServiceError::new(
                    "MCP_SERVER_DISABLED",
                    "MCP Server is not active for Run admission",
                )
            })?;
            let revision = self
                .repository
                .get_mcp_revision(server_id, revision_id)
                .await
                .map_err(|_| {
                    ServiceError::new(
                        "RUN_SERVICE_UNAVAILABLE",
                        "MCP Run admission authority is unavailable",
                    )
                })?
                .filter(|revision| revision_policy_compatible(revision, &self.policy))
                .ok_or_else(|| {
                    ServiceError::new(
                        "MCP_SERVER_DISABLED",
                        "MCP Server policy is incompatible with Run admission",
                    )
                })?;
            if revision.revision_id != revision_id {
                return Err(ServiceError::new(
                    "MCP_SERVER_DISABLED",
                    "MCP Server is not active for Run admission",
                ));
            }
            fences.insert(server_id.clone(), fence.disable_fence);
        }
        Ok(fences)
    }
}

#[derive(Clone)]
pub struct McpManagementStoreReadiness {
    repository: Arc<dyn McpManagementDurableRepository>,
}

impl McpManagementStoreReadiness {
    pub fn new(repository: Arc<dyn McpManagementDurableRepository>) -> Self {
        Self { repository }
    }
}

#[async_trait::async_trait]
impl RuntimeReadinessProbe for McpManagementStoreReadiness {
    async fn check_readiness(&self, timeout: Duration) -> Result<(), ServiceError> {
        tokio::time::timeout(timeout, self.repository.list_mcp_servers(None, None, 1))
            .await
            .map_err(|_| {
                ServiceError::new(
                    "RUN_SERVICE_UNAVAILABLE",
                    "MCP management store readiness timed out",
                )
            })?
            .map(|_| ())
            .map_err(|_| {
                ServiceError::new(
                    "RUN_SERVICE_UNAVAILABLE",
                    "MCP management store is unavailable",
                )
            })
    }
}

#[async_trait::async_trait]
impl McpExecutionFence for DurableExecutionFence {
    async fn permits(&self, server_id: &str, _revision_id: &str, disable_fence: u64) -> bool {
        self.repository
            .load_mcp_server_fence(server_id)
            .await
            .is_ok_and(|fence| {
                fence.is_some_and(|fence| {
                    fence.state == McpManagedServerState::Active
                        && fence.disable_fence == disable_fence
                })
            })
    }
}

#[derive(Debug)]
pub struct McpRevisionRuntime {
    cancellation: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

struct InstalledSubscription {
    server_id: String,
    cancellation: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl InstalledSubscription {
    fn stop(self) {
        insight_mcp::set_operational_gauge(
            &self.server_id,
            insight_mcp::McpOperationalGauge::ActiveSubscriptions,
            0,
        );
        insight_mcp::set_operational_gauge(
            &self.server_id,
            insight_mcp::McpOperationalGauge::StalePublicationCandidates,
            0,
        );
        self.cancellation.cancel();
        self.task.abort();
    }
}

impl Drop for InstalledSubscription {
    fn drop(&mut self) {
        insight_mcp::set_operational_gauge(
            &self.server_id,
            insight_mcp::McpOperationalGauge::ActiveSubscriptions,
            0,
        );
        insight_mcp::set_operational_gauge(
            &self.server_id,
            insight_mcp::McpOperationalGauge::StalePublicationCandidates,
            0,
        );
        self.cancellation.cancel();
        self.task.abort();
    }
}

struct InstalledRevision {
    revision_id: String,
    actions: Vec<(String, String)>,
    retrievals: Vec<(String, String)>,
    subscription: Option<InstalledSubscription>,
}

impl McpRevisionRuntime {
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        repository: Arc<dyn McpManagementDurableRepository>,
        config: McpClientConfig,
        policy: McpManagementPolicyMaterial,
        actions: ActionRegistry,
        retrievals: RetrievalRegistry,
        catalogs: McpCatalogRegistry,
        oauth_servers: McpOAuthRegistry,
        interaction_handler: Option<Arc<dyn McpInteractionHandler>>,
        oauth_runtime: Option<McpOAuthRuntimeAuthority>,
        remote_task_runtime: Option<McpRemoteTaskRuntimeAuthority>,
    ) -> Result<Self, &'static str> {
        let cancellation = CancellationToken::new();
        if !config.enabled {
            return Ok(Self {
                cancellation,
                task: None,
            });
        }
        let mut installed = BTreeMap::new();
        reconcile_revisions(
            RevisionReconcileContext {
                repository: &repository,
                config: &config,
                policy: &policy,
                actions: &actions,
                retrievals: &retrievals,
                catalogs: &catalogs,
                oauth_servers: &oauth_servers,
                interaction_handler: interaction_handler.as_ref(),
                oauth_runtime: oauth_runtime.as_ref(),
                remote_task_runtime: remote_task_runtime.as_ref(),
            },
            &mut installed,
        )
        .await?;
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = task_cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        if reconcile_revisions(
                            RevisionReconcileContext {
                                repository: &repository,
                                config: &config,
                                policy: &policy,
                                actions: &actions,
                                retrievals: &retrievals,
                                catalogs: &catalogs,
                                oauth_servers: &oauth_servers,
                                interaction_handler: interaction_handler.as_ref(),
                                oauth_runtime: oauth_runtime.as_ref(),
                                remote_task_runtime: remote_task_runtime.as_ref(),
                            },
                            &mut installed,
                        ).await.is_err() {
                            tracing::warn!(code="MCP_REVISION_RECONCILE_FAILED", "MCP active Revision reconciliation failed");
                        }
                    }
                }
            }
        });
        Ok(Self {
            cancellation,
            task: Some(task),
        })
    }

    pub async fn shutdown(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for McpRevisionRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct RevisionReconcileContext<'a> {
    repository: &'a Arc<dyn McpManagementDurableRepository>,
    config: &'a McpClientConfig,
    policy: &'a McpManagementPolicyMaterial,
    actions: &'a ActionRegistry,
    retrievals: &'a RetrievalRegistry,
    catalogs: &'a McpCatalogRegistry,
    oauth_servers: &'a McpOAuthRegistry,
    interaction_handler: Option<&'a Arc<dyn McpInteractionHandler>>,
    oauth_runtime: Option<&'a McpOAuthRuntimeAuthority>,
    remote_task_runtime: Option<&'a McpRemoteTaskRuntimeAuthority>,
}

async fn reconcile_revisions(
    context: RevisionReconcileContext<'_>,
    installed: &mut BTreeMap<String, InstalledRevision>,
) -> Result<(), &'static str> {
    let RevisionReconcileContext {
        repository,
        config,
        policy,
        actions,
        retrievals,
        catalogs,
        oauth_servers,
        interaction_handler,
        oauth_runtime,
        remote_task_runtime,
    } = context;
    let active = repository
        .load_active_mcp_revisions()
        .await
        .map_err(|_| "repository")?
        .into_iter()
        .filter(|(_, revision)| revision_policy_compatible(revision, policy))
        .collect::<Vec<_>>();
    let active_ids = active
        .iter()
        .map(|(server, _)| server.server_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    for server_id in installed
        .keys()
        .filter(|id| !active_ids.contains(*id))
        .cloned()
        .collect::<Vec<_>>()
    {
        if let Some(installed) = installed.remove(&server_id) {
            if let Some(subscription) = installed.subscription {
                subscription.stop();
            }
            for (action_id, version) in installed.actions {
                let _ = actions.withdraw_current(&action_id, &version);
            }
            for (retrieval_id, version) in installed.retrievals {
                let _ = retrievals.withdraw_current(&retrieval_id, &version);
            }
        }
        catalogs.remove(&server_id)?;
        oauth_servers.remove(&server_id)?;
    }
    for (server, revision) in active {
        if installed
            .get(&server.server_id)
            .is_some_and(|installed| installed.revision_id == revision.revision_id)
        {
            continue;
        }
        let fence = Arc::new(DurableExecutionFence {
            repository: Arc::clone(repository),
        }) as Arc<dyn McpExecutionFence>;
        let projection = publish_managed_mcp_revision_actions(
            config,
            &server,
            &revision,
            actions,
            retrievals,
            interaction_handler.cloned(),
            oauth_runtime.cloned(),
            remote_task_runtime.cloned(),
            fence,
        )
        .await
        .map_err(|_| "projection")?;
        catalogs.upsert(projection.catalog_server)?;
        if let Some(oauth_server) = projection.oauth_server {
            oauth_servers.upsert(oauth_server)?;
        } else {
            oauth_servers.remove(&server.server_id)?;
        }
        let subscription = projection
            .subscription
            .map(|subscription| start_managed_subscription(Arc::clone(repository), subscription));
        if let Some(previous) = installed.insert(
            server.server_id.clone(),
            InstalledRevision {
                revision_id: revision.revision_id,
                actions: projection.action_identities,
                retrievals: projection.retrieval_identities,
                subscription,
            },
        ) {
            if let Some(subscription) = previous.subscription {
                subscription.stop();
            }
            for (action_id, version) in previous.actions {
                let _ = actions.withdraw_current(&action_id, &version);
            }
            for (retrieval_id, version) in previous.retrievals {
                let _ = retrievals.withdraw_current(&retrieval_id, &version);
            }
        }
    }
    Ok(())
}

fn start_managed_subscription(
    repository: Arc<dyn McpManagementDurableRepository>,
    subscription: McpClientSubscription,
) -> InstalledSubscription {
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let server_id = subscription.server_id.clone();
    let task = tokio::spawn(async move {
        insight_mcp::set_operational_gauge(
            &subscription.server_id,
            insight_mcp::McpOperationalGauge::ActiveSubscriptions,
            1,
        );
        let mut backoff = Duration::from_millis(250);
        let mut durable_stale_marked = false;
        loop {
            let attempt_cancellation = task_cancellation.child_token();
            let listener = subscription.context.listen_with_filter(
                subscription.filter.clone(),
                subscription.invalidation.as_ref(),
                &attempt_cancellation,
            );
            tokio::pin!(listener);
            let mut inspection = tokio::time::interval(Duration::from_millis(250));
            inspection.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let disconnected = loop {
                tokio::select! {
                    _ = task_cancellation.cancelled() => {
                        attempt_cancellation.cancel();
                        insight_mcp::set_operational_gauge(
                            &subscription.server_id,
                            insight_mcp::McpOperationalGauge::ActiveSubscriptions,
                            0,
                        );
                        return;
                    }
                    result = &mut listener => break result,
                    _ = inspection.tick() => {
                        let state = subscription.invalidation.snapshot();
                        let stale = state.tools_stale || state.resources_stale || state.prompts_stale;
                        insight_mcp::set_operational_gauge(
                            &subscription.server_id,
                            insight_mcp::McpOperationalGauge::StalePublicationCandidates,
                            u64::from(stale),
                        );
                        if stale && !durable_stale_marked
                            && repository.mark_mcp_discovery_stale(MarkMcpDiscoveryStaleCommand {
                                server_id: subscription.server_id.clone(),
                                reason_code: "mcp_list_changed".to_owned(),
                                now: Utc::now(),
                            }).await.is_ok() {
                            durable_stale_marked = true;
                            insight_mcp::record_operational_event(
                                &subscription.server_id,
                                insight_mcp::McpOperationalEvent::CacheInvalidated,
                            );
                        }
                    }
                }
            };
            attempt_cancellation.cancel();
            insight_mcp::set_operational_gauge(
                &subscription.server_id,
                insight_mcp::McpOperationalGauge::ActiveSubscriptions,
                0,
            );
            tracing::warn!(
                server_id = %subscription.server_id,
                connected = disconnected.is_ok(),
                "MCP active Revision subscription disconnected; reconnecting"
            );
            tokio::select! {
                _ = task_cancellation.cancelled() => return,
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = backoff
                .checked_mul(2)
                .unwrap_or(Duration::from_secs(30))
                .min(Duration::from_secs(30));
            insight_mcp::set_operational_gauge(
                &subscription.server_id,
                insight_mcp::McpOperationalGauge::ActiveSubscriptions,
                1,
            );
        }
    });
    InstalledSubscription {
        server_id,
        cancellation,
        task,
    }
}

async fn process_claim(
    repository: Arc<dyn McpManagementDurableRepository>,
    config: McpClientConfig,
    policy_fingerprint: String,
    claim: McpDiscoveryClaim,
    cancellation: CancellationToken,
) {
    let started = std::time::Instant::now();
    if claim.operation.attempts > 1 {
        insight_mcp::record_management_event(
            insight_mcp::McpManagementEvent::DiscoveryRetried,
            Duration::ZERO,
        );
    }
    let discovery_id = claim.operation.discovery_id.clone();
    let claim_token = claim.claim_token.clone();
    let operation_cancellation = cancellation.child_token();
    let cancellation_repository = Arc::clone(&repository);
    let cancellation_discovery_id = discovery_id.clone();
    let cancellation_server_id = claim.operation.server_id.clone();
    let cancellation_signal = operation_cancellation.clone();
    let cancellation_watcher = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancellation_signal.cancelled() => return,
                _ = interval.tick() => {
                    match cancellation_repository
                        .get_mcp_discovery(&cancellation_server_id, &cancellation_discovery_id)
                        .await
                    {
                        Ok(Some(operation)) if operation.cancel_requested => {
                            cancellation_signal.cancel();
                            return;
                        }
                        Ok(Some(_)) => {}
                        Ok(None) | Err(_) => return,
                    }
                }
            }
        }
    });
    let timeout = config
        .default_limits
        .request_timeout
        .checked_mul(4)
        .unwrap_or(Duration::from_secs(600));
    let result = tokio::time::timeout(
        timeout,
        discover(
            &repository,
            &config,
            &policy_fingerprint,
            &claim,
            &operation_cancellation,
        ),
    )
    .await;
    operation_cancellation.cancel();
    cancellation_watcher.abort();
    let result = match result {
        Ok(Ok((catalog_fingerprint, snapshot_document))) => CompleteMcpDiscoveryResult::Succeeded {
            catalog_fingerprint,
            snapshot_document,
        },
        Ok(Err(failure)) => CompleteMcpDiscoveryResult::Failed(failure),
        Err(_) => CompleteMcpDiscoveryResult::Failed(McpDiscoveryFailure {
            code: "MCP_DISCOVERY_TIMEOUT".to_owned(),
            stage: "overall".to_owned(),
            retryable: true,
            correlation_id: None,
        }),
    };
    let completion = CompleteMcpDiscoveryCommand {
        discovery_id,
        claim_token,
        now: Utc::now(),
        result,
    };
    if repository.complete_mcp_discovery(completion).await.is_err() {
        tracing::warn!(
            code = "MCP_DISCOVERY_COMMIT_FAILED",
            "MCP discovery completion was not committed"
        );
        return;
    }
    let Ok(Some(operation)) = repository
        .get_mcp_discovery(&claim.operation.server_id, &claim.operation.discovery_id)
        .await
    else {
        return;
    };
    let event = match operation.status {
        insight_durable::McpDiscoveryStatus::Succeeded => {
            insight_mcp::McpManagementEvent::DiscoverySucceeded
        }
        insight_durable::McpDiscoveryStatus::Cancelled => {
            insight_mcp::McpManagementEvent::DiscoveryCancelled
        }
        insight_durable::McpDiscoveryStatus::Failed => {
            insight_mcp::McpManagementEvent::DiscoveryFailed
        }
        insight_durable::McpDiscoveryStatus::Pending
        | insight_durable::McpDiscoveryStatus::Running => return,
    };
    insight_mcp::record_management_event(event, started.elapsed());
    if operation.status == insight_durable::McpDiscoveryStatus::Succeeded {
        if let Ok(Some(snapshot)) = repository
            .get_mcp_discovery_snapshot(&claim.operation.server_id, &claim.operation.discovery_id)
            .await
        {
            record_discovery_catalog_metrics(&snapshot.document);
        }
    }
}

fn record_discovery_catalog_metrics(document: &Value) {
    let array_len = |pointer: &str| {
        document
            .pointer(pointer)
            .and_then(Value::as_array)
            .map_or(0, Vec::len)
    };
    let resources = array_len("/resources").saturating_add(array_len("/resource_templates"));
    let rejected_resources = array_len("/rejections/resources")
        .saturating_add(array_len("/rejections/resource_templates"));
    for (kind, discovered, rejected) in [
        ("tool", array_len("/tools"), array_len("/rejections/tools")),
        ("resource", resources, rejected_resources),
        (
            "prompt",
            array_len("/prompts"),
            array_len("/rejections/prompts"),
        ),
    ] {
        insight_mcp::record_management_catalog_count(
            kind,
            "discovered",
            u64::try_from(discovered).unwrap_or(u64::MAX),
        );
        insight_mcp::record_management_catalog_count(
            kind,
            "rejected",
            u64::try_from(rejected).unwrap_or(u64::MAX),
        );
    }
}

fn failure(stage: &str, retryable: bool) -> McpDiscoveryFailure {
    McpDiscoveryFailure {
        code: "MCP_DISCOVERY_REMOTE_ERROR".to_owned(),
        stage: stage.to_owned(),
        retryable,
        correlation_id: None,
    }
}

async fn discover(
    repository: &Arc<dyn McpManagementDurableRepository>,
    config: &McpClientConfig,
    policy_fingerprint: &str,
    claim: &McpDiscoveryClaim,
    cancellation: &CancellationToken,
) -> Result<(String, Value), McpDiscoveryFailure> {
    let draft: McpDraftDocument = serde_json::from_value(claim.draft_document.clone())
        .map_err(|_| failure("draft", false))?;
    let current_discovery_input = canonical_hash(&json!({
        "transport":claim.draft_document.get("transport"),
        "discovery":claim.draft_document.get("discovery"),
        "authorization":claim.draft_document.get("authorization"),
        "protocol":claim.draft_document.get("protocol"),
        "limits":claim.draft_document.get("limits"),
        "policy_fingerprint":policy_fingerprint,
    }))
    .map_err(|_| failure("policy", false))?;
    if current_discovery_input != claim.operation.discovery_input_hash {
        return Err(failure("policy_stale", false));
    }
    if let Some(McpDraftDiscovery::SignedManifest {
        manifest_id,
        content_hash,
    }) = &draft.discovery
    {
        let manifest = repository
            .get_mcp_manifest(&claim.operation.server_id, manifest_id)
            .await
            .map_err(|_| failure("manifest", true))?
            .ok_or_else(|| failure("manifest", false))?;
        if &manifest.content_hash != content_hash || manifest.expires_at <= Utc::now() {
            return Err(failure("manifest", false));
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(&manifest.payload)
            .map_err(|_| failure("manifest", false))?;
        if serde_jcs::to_vec(
            &serde_json::from_slice::<Value>(&bytes).map_err(|_| failure("manifest", false))?,
        )
        .map_err(|_| failure("manifest", false))?
            != bytes
        {
            return Err(failure("manifest", false));
        }
        let signer = config
            .signed_manifest_trust
            .trusted_signers
            .get(&manifest.key_id)
            .ok_or_else(|| failure("manifest_trust", false))?;
        let signature_bytes = URL_SAFE_NO_PAD
            .decode(&manifest.signature)
            .map_err(|_| failure("manifest_trust", false))?;
        signature::UnparsedPublicKey::new(&signature::ED25519, &signer.public_key)
            .verify(&bytes, &signature_bytes)
            .map_err(|_| failure("manifest_trust", false))?;
        let payload: Value =
            serde_json::from_slice(&bytes).map_err(|_| failure("manifest", false))?;
        let mut hash_payload = payload.clone();
        hash_payload
            .as_object_mut()
            .ok_or_else(|| failure("manifest", false))?
            .remove("content_hash");
        if canonical_hash(&hash_payload).map_err(|_| failure("manifest", false))?
            != manifest.content_hash
        {
            return Err(failure("manifest", false));
        }
        let selected_protocol = payload
            .pointer("/protocol/preferred")
            .and_then(Value::as_str)
            .unwrap_or(&draft.protocol.preferred);
        if selected_protocol != draft.protocol.preferred
            && !draft
                .protocol
                .legacy_fallback
                .iter()
                .any(|version| version == selected_protocol)
        {
            return Err(failure("manifest_protocol", false));
        }
        let max_catalog_items = resolved_limits(config, &draft).max_catalog_items;
        let tools = normalize_manifest_tools(payload.get("tools"), max_catalog_items)
            .map_err(|_| failure("manifest_catalog", false))?;
        let resources =
            normalize_manifest_items(payload.get("resources"), &["uri"], max_catalog_items)
                .map_err(|_| failure("manifest_catalog", false))?;
        let resource_templates = normalize_manifest_items(
            payload.get("resource_templates"),
            &["uriTemplate", "uri_template"],
            max_catalog_items,
        )
        .map_err(|_| failure("manifest_catalog", false))?;
        let prompts =
            normalize_manifest_items(payload.get("prompts"), &["name"], max_catalog_items)
                .map_err(|_| failure("manifest_catalog", false))?;
        let capabilities = payload
            .get("discover")
            .and_then(|value| value.get("capabilities"))
            .filter(|value| value.is_object())
            .cloned()
            .ok_or_else(|| failure("manifest_catalog", false))?;
        let server_info = payload
            .get("discover")
            .and_then(|value| value.get("serverInfo"))
            .filter(|value| value.is_object())
            .cloned()
            .ok_or_else(|| failure("manifest_catalog", false))?;
        let document = json!({
            "capabilities":capabilities,
            "server_info":server_info,
            "tools":tools,
            "resources":resources,
            "resource_templates":resource_templates,
            "prompts":prompts,
            "rejections":[],"source":"signed_manifest","manifest_id":manifest_id,
            "selected_protocol":selected_protocol,
        });
        return Ok((
            canonical_hash(&document).map_err(|_| failure("canonicalization", false))?,
            document,
        ));
    }

    if !matches!(draft.discovery, Some(McpDraftDiscovery::LiveServiceAccount)) {
        return Err(failure("draft", false));
    }
    if matches!(
        draft.authorization,
        Some(McpDraftAuthorization::OauthUser { .. })
    ) {
        return Err(failure("authorization", false));
    }
    let transport = build_transport(config, &claim.operation.server_id, &draft)
        .map_err(|_| failure("transport", false))?;
    let limits = resolved_limits(config, &draft);
    let client_limits = McpClientLimits {
        max_tools: limits.max_catalog_items,
        max_resources: limits.max_catalog_items,
        max_prompts: limits.max_catalog_items,
        max_content_items: limits.max_content_items,
        max_content_bytes: limits.max_response_bytes,
        ..McpClientLimits::default()
    };
    let client_info = ClientInfo {
        name: "insight-agent-platform".to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        title: Some("Insight Agent Platform".to_owned()),
        description: None,
        website_url: None,
        icons: Vec::new(),
    };
    let modern_client = build_discovery_client(
        Arc::clone(&transport),
        client_info.clone(),
        client_limits,
        limits.max_request_bytes.max(limits.max_response_bytes),
        MCP_PROTOCOL_VERSION,
    )?;
    let modern_discovery = modern_client
        .discover(cancellation, &NoopNotificationObserver)
        .await;
    let (client, discovered, active_transport) = match modern_discovery {
        Ok(discovered) => (modern_client, discovered, Arc::clone(&transport)),
        Err(error)
            if draft
                .protocol
                .legacy_fallback
                .iter()
                .any(|version| version == MCP_LEGACY_PROTOCOL_VERSION)
                && legacy_fallback_allowed(&error) =>
        {
            transport
                .shutdown()
                .await
                .map_err(|_| failure("legacy_shutdown", true))?;
            let legacy_transport = build_transport(config, &claim.operation.server_id, &draft)
                .map_err(|_| failure("legacy_transport", false))?;
            let legacy_client = build_discovery_client(
                Arc::clone(&legacy_transport),
                client_info,
                client_limits,
                limits.max_request_bytes.max(limits.max_response_bytes),
                MCP_LEGACY_PROTOCOL_VERSION,
            )?;
            let discovered = legacy_client
                .discover(cancellation, &NoopNotificationObserver)
                .await
                .map_err(|_| failure("legacy_discover", true))?;
            (legacy_client, discovered, legacy_transport)
        }
        Err(_) => {
            let _ = transport.shutdown().await;
            return Err(failure("discover", true));
        }
    };
    let outcome = async {
        let tools = if discovered.capabilities.tools.is_some() {
            client
                .list_tools(cancellation, &NoopNotificationObserver)
                .await
                .map_err(|_| failure("tools_list", true))?
        } else {
            insight_mcp::ToolCatalog {
                tools: Vec::new(),
                rejected: Vec::new(),
                ttl_ms: 0,
                cache_scope: insight_mcp::CacheScope::Private,
                descriptor_hash: canonical_hash(&json!([]))
                    .map_err(|_| failure("tools_list", false))?,
            }
        };
        let resources = if discovered.capabilities.resources.is_some() {
            client
                .list_resources(cancellation, &NoopNotificationObserver)
                .await
                .map_err(|_| failure("resources_list", true))?
        } else {
            insight_mcp::ResourceCatalog::empty("resources")
                .map_err(|_| failure("resources_list", false))?
        };
        let resource_templates = if discovered.capabilities.resources.is_some() {
            client
                .list_resource_templates(cancellation, &NoopNotificationObserver)
                .await
                .map_err(|_| failure("resource_templates_list", true))?
        } else {
            insight_mcp::ResourceTemplateCatalog::empty("resource_templates")
                .map_err(|_| failure("resource_templates_list", false))?
        };
        let prompts = if discovered.capabilities.prompts.is_some() {
            client
                .list_prompts(cancellation, &NoopNotificationObserver)
                .await
                .map_err(|_| failure("prompts_list", true))?
        } else {
            insight_mcp::PromptCatalog::empty("prompts")
                .map_err(|_| failure("prompts_list", false))?
        };
        let tool_items = tools
            .tools
            .iter()
            .map(tool_projection)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| failure("canonicalization", false))?;
        let document = json!({
            "capabilities":discovered.capabilities,"server_info":discovered.server_info,
            "tools":tool_items,"resources":resources.items,"prompts":prompts.items,
            "resource_templates":resource_templates.items,
            "selected_protocol":client.protocol_version(),
            "rejections":{
                "tools":tools.rejected.iter().map(|item|json!({"identity":item.tool_name,"code":item.code})).collect::<Vec<_>>(),
                "resources":resources.rejected.iter().map(|item|json!({"identity":item.identity,"code":item.code})).collect::<Vec<_>>(),
                "resource_templates":resource_templates.rejected.iter().map(|item|json!({"identity":item.identity,"code":item.code})).collect::<Vec<_>>(),
                "prompts":prompts.rejected.iter().map(|item|json!({"identity":item.identity,"code":item.code})).collect::<Vec<_>>(),
            },
            "source":"live_service_account",
        });
        Ok((
            canonical_hash(&document).map_err(|_| failure("canonicalization", false))?,
            document,
        ))
    }
    .await;
    let shutdown = active_transport.shutdown().await;
    match outcome {
        Ok(result) if shutdown.is_ok() => Ok(result),
        Ok(_) => Err(failure("shutdown", true)),
        Err(error) => Err(error),
    }
}

fn build_discovery_client(
    transport: Arc<dyn McpTransport>,
    client_info: ClientInfo,
    limits: McpClientLimits,
    max_message_bytes: usize,
    protocol_version: &str,
) -> Result<Arc<McpClient>, McpDiscoveryFailure> {
    let capabilities = ClientCapabilities::default();
    let client = if protocol_version == MCP_LEGACY_PROTOCOL_VERSION {
        let adapter =
            LegacyCompatibilityTransport::new(transport, client_info.clone(), capabilities.clone())
                .map_err(|_| failure("legacy_client", false))?;
        McpClient::new_legacy(Arc::new(adapter), client_info, capabilities)
    } else {
        McpClient::new(transport, client_info, capabilities)
    }
    .and_then(|client| client.with_limits(limits))
    .and_then(|client| {
        client.with_protocol_limits(ProtocolLimits {
            max_message_bytes,
            ..ProtocolLimits::default()
        })
    })
    .map_err(|_| failure("client", false))?;
    Ok(Arc::new(client))
}

fn legacy_fallback_allowed(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Protocol(-32601)
            | ClientError::Transport(TransportError::Remote { code: -32601, .. })
            | ClientError::Transport(TransportError::Timeout)
    )
}

#[derive(Clone, Copy)]
struct EffectiveLimits {
    max_request_bytes: usize,
    max_response_bytes: usize,
    max_content_items: usize,
    max_catalog_items: usize,
}

fn resolved_limits(config: &McpClientConfig, draft: &McpDraftDocument) -> EffectiveLimits {
    EffectiveLimits {
        max_request_bytes: draft
            .limits
            .max_request_bytes
            .unwrap_or(config.default_limits.limits.max_request_bytes),
        max_response_bytes: draft
            .limits
            .max_response_bytes
            .unwrap_or(config.default_limits.limits.max_response_bytes),
        max_content_items: draft
            .limits
            .max_content_items
            .unwrap_or(config.default_limits.limits.max_content_items),
        max_catalog_items: draft
            .limits
            .max_catalog_items
            .unwrap_or(config.default_limits.limits.max_catalog_items),
    }
}

fn build_transport(
    config: &McpClientConfig,
    server_id: &str,
    draft: &McpDraftDocument,
) -> Result<Arc<dyn McpTransport>, ()> {
    let limits = resolved_limits(config, draft);
    match draft.transport.as_ref().ok_or(())? {
        McpDraftTransport::StreamableHttp { endpoint } => {
            let policy = if config.network_policy.allow_loopback_development
                && endpoint.starts_with("http://")
            {
                StreamableHttpPolicy::for_loopback_test(endpoint)
            } else {
                StreamableHttpPolicy::new(endpoint)
            }
            .map_err(|_| ())?
            .with_timeouts(
                config.default_limits.connect_timeout,
                config.default_limits.request_timeout,
            )
            .map_err(|_| ())?
            .with_server_id(server_id)
            .map_err(|_| ())?
            .with_request_limit(limits.max_request_bytes)
            .map_err(|_| ())?
            .with_limits(
                limits.max_response_bytes,
                draft
                    .limits
                    .max_sse_line_bytes
                    .unwrap_or(config.default_limits.limits.max_sse_line_bytes),
                draft
                    .limits
                    .max_sse_event_bytes
                    .unwrap_or(config.default_limits.limits.max_sse_event_bytes),
            )
            .map_err(|_| ())?;
            let credential = match draft.authorization.as_ref().ok_or(())? {
                McpDraftAuthorization::None => None,
                McpDraftAuthorization::BearerSecretRef { secret_ref } => {
                    let token = std::env::var(secret_ref).map_err(|_| ())?;
                    Some(HttpCredential::bearer(&token).map_err(|_| ())?)
                }
                McpDraftAuthorization::OauthUser { .. } => return Err(()),
            };
            Ok(Arc::new(
                StreamableHttpTransport::new(policy, credential).map_err(|_| ())?,
            ))
        }
        McpDraftTransport::Stdio {
            launch_profile_id,
            parameters,
        } => {
            let profile = config
                .stdio_launch_profiles
                .get(launch_profile_id)
                .ok_or(())?;
            if parameters
                .keys()
                .any(|name| !profile.allowed_parameters.contains(name))
            {
                return Err(());
            }
            let mut args = profile.fixed_args.clone();
            for (name, value) in parameters {
                args.push(format!("--{name}"));
                args.push(value.clone());
            }
            let environment = profile
                .secret_environment
                .iter()
                .map(|(name, secret_ref)| {
                    std::env::var(secret_ref)
                        .map(|value| (name.clone(), value))
                        .map_err(|_| ())
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let policy = StdioTransportPolicy::new(
                profile.executable.clone(),
                args,
                profile.working_directory.clone(),
                environment,
            )
            .map_err(|_| ())?
            .with_timeouts(
                Duration::from_secs(10),
                config.default_limits.request_timeout,
                Duration::from_secs(10),
            )
            .map_err(|_| ())?
            .with_limits(
                limits.max_request_bytes,
                limits.max_response_bytes,
                64 * 1024,
            )
            .map_err(|_| ())?
            .with_server_id(server_id)
            .map_err(|_| ())?;
            Ok(Arc::new(StdioTransport::new(policy).map_err(|_| ())?))
        }
    }
}

fn tool_projection(tool: &insight_mcp::Tool) -> Result<Value, serde_json::Error> {
    let schema = json!({"input":tool.input_schema,"output":tool.output_schema});
    let schema_bytes = serde_jcs::to_vec(&schema)?;
    Ok(json!({
        "remote":tool.name,"title":tool.title,"description":tool.description,"annotations":tool.annotations,
        "input_schema":tool.input_schema,"output_schema":tool.output_schema,
        "schema_hash":prefixed_sha256(&schema_bytes),"byte_count":schema_bytes.len(),"importable":true,
    }))
}

fn normalize_manifest_tools(value: Option<&Value>, max_items: usize) -> Result<Vec<Value>, ()> {
    let tools = value.and_then(Value::as_array).ok_or(())?;
    if tools.len() > max_items {
        return Err(());
    }
    let mut names = BTreeSet::new();
    tools
        .iter()
        .map(|tool| {
            let remote = tool
                .get("remote")
                .or_else(|| tool.get("name"))
                .and_then(Value::as_str)
                .filter(|name| valid_manifest_identity(name))
                .ok_or(())?;
            if !names.insert(remote.to_owned()) {
                return Err(());
            }
            let input = tool
                .get("input_schema")
                .or_else(|| tool.get("inputSchema"))
                .filter(|value| value.is_object())
                .ok_or(())?;
            let output = tool
                .get("output_schema")
                .or_else(|| tool.get("outputSchema"));
            if output.is_some_and(|value| !value.is_null() && !value.is_object()) {
                return Err(());
            }
            let schema = json!({"input":input,"output":output});
            let bytes = serde_jcs::to_vec(&schema).map_err(|_| ())?;
            Ok(json!({"remote":remote,"title":tool.get("title"),"description":tool.get("description"),"annotations":tool.get("annotations"),"input_schema":input,"output_schema":output,"schema_hash":prefixed_sha256(&bytes),"byte_count":bytes.len(),"importable":true}))
        })
        .collect()
}

fn normalize_manifest_items(
    value: Option<&Value>,
    identity_fields: &[&str],
    max_items: usize,
) -> Result<Vec<Value>, ()> {
    let items = value.and_then(Value::as_array).ok_or(())?;
    if items.len() > max_items {
        return Err(());
    }
    let mut identities = BTreeSet::new();
    for item in items {
        let identity = identity_fields
            .iter()
            .find_map(|field| item.get(*field).and_then(Value::as_str))
            .filter(|identity| {
                !identity.is_empty()
                    && identity.len() <= 8 * 1024
                    && !identity.chars().any(char::is_control)
            })
            .ok_or(())?;
        if !identities.insert(identity.to_owned()) {
            return Err(());
        }
    }
    Ok(items.clone())
}

fn valid_manifest_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn canonical_hash(value: &Value) -> Result<String, serde_json::Error> {
    serde_jcs::to_vec(value).map(|bytes| prefixed_sha256(&bytes))
}
fn prefixed_sha256(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::from("sha256:");
    for byte in Sha256::digest(bytes) {
        write!(&mut output, "{byte:02x}").expect("String write");
    }
    output
}
