use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use insight_mcp::{
    CompleteResult, CompletionArgument, CompletionReference, InputRequiredResult, InputResponse,
    McpCatalog, McpClient, McpGetPromptOutcome, McpNotificationObserver, McpPromptBinding,
    McpReadResourceOutcome, McpResourceBinding, McpResourceBindingKind, McpServerBindingIdentity,
    Prompt, PromptCatalog, ReadResourceResult, Resource, ResourceCatalog, ResourceContents,
    ResourceTemplate, ResourceTemplateCatalog, SubscriptionFilter, TransportError,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct McpResourceImportPolicy {
    pub uri_pattern: String,
    pub mime_allowlist: Vec<String>,
    pub max_content_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct McpPromptImportPolicy {
    pub remote_name: String,
    pub allow_user_invocation: bool,
    pub allow_definition_snapshot: bool,
    pub definition_arguments: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum McpImportedResourceDescriptor {
    Resource(Resource),
    Template(ResourceTemplate),
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpImportedResource {
    pub binding: McpResourceBinding,
    pub descriptor: McpImportedResourceDescriptor,
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpImportedPrompt {
    pub binding: McpPromptBinding,
    pub descriptor: Prompt,
    pub policy: McpPromptImportPolicy,
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpResourceSnapshot {
    pub binding: McpResourceBinding,
    pub canonical_uri: String,
    pub result: ReadResourceResult,
    pub content_hash: String,
    pub observed_at_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpPromptSnapshot {
    pub binding: McpPromptBinding,
    pub result: insight_mcp::GetPromptResult,
    pub content_hash: String,
    pub observed_at_unix_ms: u64,
    pub untrusted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct McpResourceCacheKey {
    principal_scope: String,
    uri: String,
}

#[derive(Debug, Clone)]
struct McpResourceCacheEntry {
    snapshot: McpResourceSnapshot,
    expires_at_unix_ms: u64,
}

#[derive(Default)]
struct McpResourceContinuation {
    input_responses: Option<BTreeMap<String, InputResponse>>,
    request_state: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum McpContextOutcome<T> {
    Complete(T),
    InputRequired(InputRequiredResult),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpPrincipalClientResolverError {
    AuthorizationRequired,
    InsufficientScope,
    Unavailable,
}

#[async_trait::async_trait]
pub trait McpPrincipalClientResolver: Send + Sync {
    async fn client_for_principal(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Arc<McpClient>, McpPrincipalClientResolverError>;
}

#[derive(Clone)]
pub struct McpContextProvider {
    client: Arc<McpClient>,
    principal_client_resolver: Option<Arc<dyn McpPrincipalClientResolver>>,
    resources: Vec<McpImportedResource>,
    prompts: BTreeMap<String, McpImportedPrompt>,
    definition_prompt_snapshots: BTreeMap<String, McpPromptSnapshot>,
    resource_cache: Arc<Mutex<BTreeMap<McpResourceCacheKey, McpResourceCacheEntry>>>,
    catalog_invalidation: Arc<McpCatalogInvalidation>,
}

impl McpContextProvider {
    pub fn freeze(
        server: McpServerBindingIdentity,
        client: Arc<McpClient>,
        resources: ResourceCatalog,
        templates: ResourceTemplateCatalog,
        prompts: PromptCatalog,
        resource_policies: Vec<McpResourceImportPolicy>,
        prompt_policies: Vec<McpPromptImportPolicy>,
    ) -> Result<Self, McpContextError> {
        server.validate().map_err(|_| McpContextError::Server)?;
        validate_unique_resource_policies(&resource_policies)?;
        validate_unique_prompt_policies(&prompt_policies)?;
        reject_explicit_catalog_rejections(&resources, &resource_policies)?;
        reject_explicit_catalog_rejections(&templates, &resource_policies)?;

        let mut imported_resources = Vec::new();
        let mut seen_resource_identities = BTreeSet::new();
        for descriptor in resources.items {
            if let Some(policy) = resource_policies
                .iter()
                .find(|policy| wildcard_matches(&policy.uri_pattern, &descriptor.uri))
            {
                if !seen_resource_identities.insert(descriptor.uri.clone()) {
                    return Err(McpContextError::Duplicate);
                }
                imported_resources.push(import_resource(
                    &server,
                    McpImportedResourceDescriptor::Resource(descriptor),
                    policy,
                    &resources.descriptor_hash,
                )?);
            }
        }
        for descriptor in templates.items {
            if let Some(policy) = resource_policies
                .iter()
                .find(|policy| wildcard_matches(&policy.uri_pattern, &descriptor.uri_template))
            {
                if !seen_resource_identities.insert(descriptor.uri_template.clone()) {
                    return Err(McpContextError::Duplicate);
                }
                imported_resources.push(import_resource(
                    &server,
                    McpImportedResourceDescriptor::Template(descriptor),
                    policy,
                    &templates.descriptor_hash,
                )?);
            }
        }
        if resource_policies.iter().any(|policy| {
            !imported_resources
                .iter()
                .any(|resource| wildcard_matches(&policy.uri_pattern, &resource.binding.remote_uri))
        }) {
            return Err(McpContextError::Missing);
        }
        imported_resources.sort_by(|left, right| {
            left.binding
                .remote_uri
                .as_bytes()
                .cmp(right.binding.remote_uri.as_bytes())
        });

        let prompt_descriptors = prompts
            .items
            .into_iter()
            .map(|prompt| (prompt.name.clone(), prompt))
            .collect::<BTreeMap<_, _>>();
        let mut imported_prompts = BTreeMap::new();
        for policy in prompt_policies {
            let descriptor = prompt_descriptors
                .get(&policy.remote_name)
                .cloned()
                .ok_or(McpContextError::Missing)?;
            if prompts
                .rejected
                .iter()
                .any(|rejection| rejection.identity == policy.remote_name)
            {
                return Err(McpContextError::Rejected);
            }
            let binding = McpPromptBinding::seal(
                server.clone(),
                descriptor.name.clone(),
                descriptor.title.clone(),
                descriptor.description.clone(),
                descriptor.arguments.clone(),
                prompts.descriptor_hash.clone(),
                canonical_sha256(&policy)?,
            )
            .map_err(|_| McpContextError::Binding)?;
            let name = descriptor.name.clone();
            if imported_prompts
                .insert(
                    name,
                    McpImportedPrompt {
                        binding,
                        descriptor,
                        policy,
                    },
                )
                .is_some()
            {
                return Err(McpContextError::Duplicate);
            }
        }
        Ok(Self {
            client,
            principal_client_resolver: None,
            resources: imported_resources,
            prompts: imported_prompts,
            definition_prompt_snapshots: BTreeMap::new(),
            resource_cache: Arc::new(Mutex::new(BTreeMap::new())),
            catalog_invalidation: Arc::new(McpCatalogInvalidation::default()),
        })
    }

    pub fn with_principal_client_resolver(
        mut self,
        resolver: Arc<dyn McpPrincipalClientResolver>,
    ) -> Self {
        self.principal_client_resolver = Some(resolver);
        self
    }

    pub fn with_catalog_invalidation(mut self, invalidation: Arc<McpCatalogInvalidation>) -> Self {
        self.catalog_invalidation = invalidation;
        self
    }

    pub fn resources(&self) -> &[McpImportedResource] {
        &self.resources
    }

    pub fn prompts(&self) -> impl Iterator<Item = &McpImportedPrompt> {
        self.prompts.values()
    }

    pub fn definition_prompt_snapshot(&self, name: &str) -> Option<&McpPromptSnapshot> {
        self.definition_prompt_snapshots.get(name)
    }

    pub async fn freeze_definition_prompts(
        mut self,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<Self, McpContextError> {
        let requests = self
            .prompts
            .values()
            .filter_map(|prompt| {
                prompt
                    .policy
                    .definition_arguments
                    .clone()
                    .map(|arguments| (prompt.descriptor.name.clone(), arguments))
            })
            .collect::<Vec<_>>();
        for (name, arguments) in requests {
            let snapshot = match self
                .get_prompt_with_client(
                    Arc::clone(&self.client),
                    &name,
                    arguments,
                    true,
                    cancellation,
                    observer,
                )
                .await?
            {
                McpContextOutcome::Complete(snapshot) => snapshot,
                McpContextOutcome::InputRequired(_) => return Err(McpContextError::Interaction),
            };
            self.definition_prompt_snapshots.insert(name, snapshot);
        }
        Ok(self)
    }

    pub fn with_definition_prompt_results(
        mut self,
        results: BTreeMap<String, insight_mcp::GetPromptResult>,
    ) -> Result<Self, McpContextError> {
        let required = self
            .prompts
            .values()
            .filter(|prompt| prompt.policy.definition_arguments.is_some())
            .map(|prompt| prompt.descriptor.name.clone())
            .collect::<BTreeSet<_>>();
        if results.keys().cloned().collect::<BTreeSet<_>>() != required {
            return Err(McpContextError::Policy);
        }
        for (name, result) in results {
            let imported = self.prompts.get(&name).ok_or(McpContextError::Policy)?;
            self.definition_prompt_snapshots.insert(
                name,
                McpPromptSnapshot {
                    binding: imported.binding.clone(),
                    content_hash: canonical_sha256(&result)?,
                    result,
                    observed_at_unix_ms: 0,
                    untrusted: true,
                },
            );
        }
        Ok(self)
    }

    pub async fn read_resource(
        &self,
        uri: &str,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<McpContextOutcome<McpResourceSnapshot>, McpContextError> {
        self.read_resource_with_client(
            Arc::clone(&self.client),
            uri,
            None,
            McpResourceContinuation::default(),
            cancellation,
            observer,
        )
        .await
    }

    pub async fn read_resource_for_principal(
        &self,
        tenant_id: &str,
        user_id: &str,
        uri: &str,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<McpContextOutcome<McpResourceSnapshot>, McpContextError> {
        let client = self.principal_client(tenant_id, user_id).await?;
        self.read_resource_with_client(
            client,
            uri,
            Some((tenant_id, user_id)),
            McpResourceContinuation::default(),
            cancellation,
            observer,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn read_resource_with_inputs(
        &self,
        uri: &str,
        input_responses: BTreeMap<String, InputResponse>,
        request_state: Option<String>,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<McpContextOutcome<McpResourceSnapshot>, McpContextError> {
        self.read_resource_with_client(
            Arc::clone(&self.client),
            uri,
            None,
            McpResourceContinuation {
                input_responses: Some(input_responses),
                request_state,
            },
            cancellation,
            observer,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn read_resource_for_principal_with_inputs(
        &self,
        tenant_id: &str,
        user_id: &str,
        uri: &str,
        input_responses: BTreeMap<String, InputResponse>,
        request_state: Option<String>,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<McpContextOutcome<McpResourceSnapshot>, McpContextError> {
        let client = self.principal_client(tenant_id, user_id).await?;
        self.read_resource_with_client(
            client,
            uri,
            Some((tenant_id, user_id)),
            McpResourceContinuation {
                input_responses: Some(input_responses),
                request_state,
            },
            cancellation,
            observer,
        )
        .await
    }

    async fn read_resource_with_client(
        &self,
        client: Arc<McpClient>,
        uri: &str,
        principal: Option<(&str, &str)>,
        continuation: McpResourceContinuation,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<McpContextOutcome<McpResourceSnapshot>, McpContextError> {
        let imported = self
            .resources
            .iter()
            .find(|resource| resource_matches(resource, uri))
            .ok_or(McpContextError::Unauthorized)?;
        let cache_key = McpResourceCacheKey {
            principal_scope: principal.map_or_else(
                || "service".to_owned(),
                |(tenant_id, user_id)| format!("tenant:{tenant_id}\0user:{user_id}"),
            ),
            uri: uri.to_owned(),
        };
        let is_continuation =
            continuation.input_responses.is_some() || continuation.request_state.is_some();
        if !is_continuation && !self.catalog_invalidation.resource_is_stale(uri)? {
            if let Some(snapshot) = self.cached_resource(&cache_key)? {
                insight_mcp::record_operational_event(
                    &imported.binding.server.server_id,
                    insight_mcp::McpOperationalEvent::CacheHit,
                );
                return Ok(McpContextOutcome::Complete(snapshot));
            }
        }
        if !is_continuation {
            insight_mcp::record_operational_event(
                &imported.binding.server.server_id,
                insight_mcp::McpOperationalEvent::CacheMiss,
            );
        }
        match client
            .read_resource(
                uri,
                continuation.input_responses,
                continuation.request_state,
                cancellation,
                observer,
            )
            .await
            .map_err(|_| McpContextError::Remote)?
        {
            McpReadResourceOutcome::InputRequired(input) => {
                Ok(McpContextOutcome::InputRequired(input))
            }
            McpReadResourceOutcome::Complete(result) => {
                validate_resource_result(&imported.binding, uri, &result)?;
                let ttl_ms = result.ttl_ms.min(24 * 60 * 60 * 1_000);
                let snapshot = McpResourceSnapshot {
                    binding: imported.binding.clone(),
                    canonical_uri: uri.to_owned(),
                    content_hash: canonical_sha256(&result)?,
                    result,
                    observed_at_unix_ms: now_unix_ms()?,
                };
                if !is_continuation && ttl_ms > 0 {
                    self.cache_resource(cache_key, snapshot.clone(), ttl_ms)?;
                    self.catalog_invalidation.acknowledge_resource(uri)?;
                }
                Ok(McpContextOutcome::Complete(snapshot))
            }
        }
    }

    fn cached_resource(
        &self,
        key: &McpResourceCacheKey,
    ) -> Result<Option<McpResourceSnapshot>, McpContextError> {
        let now = now_unix_ms()?;
        let mut cache = self
            .resource_cache
            .lock()
            .map_err(|_| McpContextError::Remote)?;
        match cache.get(key) {
            Some(entry) if entry.expires_at_unix_ms > now => Ok(Some(entry.snapshot.clone())),
            Some(_) => {
                cache.remove(key);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    fn cache_resource(
        &self,
        key: McpResourceCacheKey,
        snapshot: McpResourceSnapshot,
        ttl_ms: u64,
    ) -> Result<(), McpContextError> {
        const MAX_CACHE_ENTRIES: usize = 1_024;
        let expires_at_unix_ms = now_unix_ms()?
            .checked_add(ttl_ms)
            .ok_or(McpContextError::Clock)?;
        let mut cache = self
            .resource_cache
            .lock()
            .map_err(|_| McpContextError::Remote)?;
        if cache.len() >= MAX_CACHE_ENTRIES && !cache.contains_key(&key) {
            if let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, entry)| entry.snapshot.observed_at_unix_ms)
                .map(|(key, _)| key.clone())
            {
                cache.remove(&oldest);
            }
        }
        cache.insert(
            key,
            McpResourceCacheEntry {
                snapshot,
                expires_at_unix_ms,
            },
        );
        Ok(())
    }

    pub async fn get_prompt(
        &self,
        name: &str,
        arguments: BTreeMap<String, String>,
        definition_snapshot: bool,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<McpContextOutcome<McpPromptSnapshot>, McpContextError> {
        self.get_prompt_with_client(
            Arc::clone(&self.client),
            name,
            arguments,
            definition_snapshot,
            cancellation,
            observer,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_prompt_for_principal(
        &self,
        tenant_id: &str,
        user_id: &str,
        name: &str,
        arguments: BTreeMap<String, String>,
        definition_snapshot: bool,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<McpContextOutcome<McpPromptSnapshot>, McpContextError> {
        let client = self.principal_client(tenant_id, user_id).await?;
        self.get_prompt_with_client(
            client,
            name,
            arguments,
            definition_snapshot,
            cancellation,
            observer,
        )
        .await
    }

    async fn get_prompt_with_client(
        &self,
        client: Arc<McpClient>,
        name: &str,
        arguments: BTreeMap<String, String>,
        definition_snapshot: bool,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<McpContextOutcome<McpPromptSnapshot>, McpContextError> {
        let imported = self
            .prompts
            .get(name)
            .ok_or(McpContextError::Unauthorized)?;
        if (definition_snapshot && !imported.policy.allow_definition_snapshot)
            || (!definition_snapshot && !imported.policy.allow_user_invocation)
        {
            return Err(McpContextError::Unauthorized);
        }
        match client
            .get_prompt(
                &imported.descriptor,
                arguments,
                None,
                None,
                cancellation,
                observer,
            )
            .await
            .map_err(|_| McpContextError::Remote)?
        {
            McpGetPromptOutcome::InputRequired(input) => {
                Ok(McpContextOutcome::InputRequired(input))
            }
            McpGetPromptOutcome::Complete(result) => {
                Ok(McpContextOutcome::Complete(McpPromptSnapshot {
                    binding: imported.binding.clone(),
                    content_hash: canonical_sha256(&result)?,
                    result,
                    observed_at_unix_ms: now_unix_ms()?,
                    untrusted: true,
                }))
            }
        }
    }

    pub async fn complete(
        &self,
        reference: CompletionReference,
        argument: CompletionArgument,
        context: BTreeMap<String, String>,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<CompleteResult, McpContextError> {
        self.complete_with_client(
            Arc::clone(&self.client),
            reference,
            argument,
            context,
            cancellation,
            observer,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn complete_for_principal(
        &self,
        tenant_id: &str,
        user_id: &str,
        reference: CompletionReference,
        argument: CompletionArgument,
        context: BTreeMap<String, String>,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<CompleteResult, McpContextError> {
        let client = self.principal_client(tenant_id, user_id).await?;
        self.complete_with_client(client, reference, argument, context, cancellation, observer)
            .await
    }

    async fn complete_with_client(
        &self,
        client: Arc<McpClient>,
        reference: CompletionReference,
        argument: CompletionArgument,
        context: BTreeMap<String, String>,
        cancellation: &CancellationToken,
        observer: &dyn McpNotificationObserver,
    ) -> Result<CompleteResult, McpContextError> {
        match &reference {
            CompletionReference::Prompt { name, .. } if self.prompts.contains_key(name) => {}
            CompletionReference::Resource { uri }
                if self
                    .resources
                    .iter()
                    .any(|resource| resource.binding.remote_uri == *uri) => {}
            _ => return Err(McpContextError::Unauthorized),
        }
        client
            .complete(reference, argument, context, cancellation, observer)
            .await
            .map_err(|_| McpContextError::Remote)
    }

    async fn principal_client(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Arc<McpClient>, McpContextError> {
        match &self.principal_client_resolver {
            Some(resolver) => resolver
                .client_for_principal(tenant_id, user_id)
                .await
                .map_err(|error| match error {
                    McpPrincipalClientResolverError::AuthorizationRequired
                    | McpPrincipalClientResolverError::InsufficientScope => {
                        McpContextError::Unauthorized
                    }
                    McpPrincipalClientResolverError::Unavailable => McpContextError::Remote,
                }),
            None => Ok(Arc::clone(&self.client)),
        }
    }

    pub async fn listen(
        &self,
        invalidation: &McpCatalogInvalidation,
        cancellation: &CancellationToken,
    ) -> Result<(), McpContextError> {
        let filter = SubscriptionFilter {
            tools_list_changed: Some(true),
            resources_list_changed: Some(true),
            prompts_list_changed: Some(true),
            resource_subscriptions: self
                .resources
                .iter()
                .filter(|resource| resource.binding.kind == McpResourceBindingKind::Resource)
                .map(|resource| resource.binding.remote_uri.clone())
                .collect(),
            task_ids: Vec::new(),
        };
        self.listen_with_filter(filter, invalidation, cancellation)
            .await
    }

    pub async fn listen_with_filter(
        &self,
        filter: SubscriptionFilter,
        invalidation: &McpCatalogInvalidation,
        cancellation: &CancellationToken,
    ) -> Result<(), McpContextError> {
        self.client
            .listen_subscriptions(filter, cancellation, invalidation)
            .await
            .map_err(|_| McpContextError::Remote)
    }
}

fn import_resource(
    server: &McpServerBindingIdentity,
    descriptor: McpImportedResourceDescriptor,
    policy: &McpResourceImportPolicy,
    catalog_fingerprint: &str,
) -> Result<McpImportedResource, McpContextError> {
    let (remote_uri, kind) = match &descriptor {
        McpImportedResourceDescriptor::Resource(resource) => {
            (resource.uri.clone(), McpResourceBindingKind::Resource)
        }
        McpImportedResourceDescriptor::Template(template) => (
            template.uri_template.clone(),
            McpResourceBindingKind::Template,
        ),
    };
    validate_resource_identity(server, &remote_uri, kind)?;
    let binding = McpResourceBinding::seal(
        server.clone(),
        remote_uri,
        kind,
        policy.mime_allowlist.clone(),
        policy.max_content_bytes,
        catalog_fingerprint.to_owned(),
        canonical_sha256(policy)?,
    )
    .map_err(|_| McpContextError::Binding)?;
    Ok(McpImportedResource {
        binding,
        descriptor,
    })
}

fn validate_resource_identity(
    _server: &McpServerBindingIdentity,
    remote_uri: &str,
    kind: McpResourceBindingKind,
) -> Result<(), McpContextError> {
    let parsed = match kind {
        McpResourceBindingKind::Resource => reqwest::Url::parse(remote_uri),
        McpResourceBindingKind::Template => {
            let mut rendered = String::with_capacity(remote_uri.len());
            let mut remainder = remote_uri;
            while let Some(open) = remainder.find('{') {
                rendered.push_str(&remainder[..open]);
                let after_open = &remainder[open + 1..];
                let close = after_open.find('}').ok_or(McpContextError::Policy)?;
                let variable = &after_open[..close];
                if variable.is_empty()
                    || !variable.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
                    })
                {
                    return Err(McpContextError::Policy);
                }
                rendered.push('x');
                remainder = &after_open[close + 1..];
            }
            if remainder.contains('}') {
                return Err(McpContextError::Policy);
            }
            rendered.push_str(remainder);
            reqwest::Url::parse(&rendered)
        }
    }
    .map_err(|_| McpContextError::Policy)?;
    if remote_uri.len() > 8 * 1024
        || remote_uri.chars().any(char::is_control)
        || parsed.fragment().is_some()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || matches!(parsed.scheme(), "data" | "javascript" | "file")
    {
        return Err(McpContextError::Policy);
    }
    Ok(())
}

fn validate_unique_resource_policies(
    policies: &[McpResourceImportPolicy],
) -> Result<(), McpContextError> {
    let mut patterns = BTreeSet::new();
    if policies.iter().any(|policy| {
        policy.uri_pattern.is_empty()
            || policy.uri_pattern.len() > 8 * 1024
            || !patterns.insert(policy.uri_pattern.clone())
            || policy.max_content_bytes == 0
            || policy.max_content_bytes > 256 * 1024 * 1024
            || policy.mime_allowlist.len() > 128
    }) {
        return Err(McpContextError::Policy);
    }
    Ok(())
}

fn validate_unique_prompt_policies(
    policies: &[McpPromptImportPolicy],
) -> Result<(), McpContextError> {
    let mut names = BTreeSet::new();
    if policies.iter().any(|policy| {
        policy.remote_name.is_empty()
            || !names.insert(policy.remote_name.clone())
            || policy.allow_definition_snapshot != policy.definition_arguments.is_some()
            || (!policy.allow_user_invocation && !policy.allow_definition_snapshot)
            || policy
                .definition_arguments
                .as_ref()
                .is_some_and(|arguments| {
                    arguments.len() > 128
                        || arguments.iter().any(|(name, value)| {
                            name.is_empty()
                                || name.len() > 128
                                || value.len() > 8 * 1024
                                || name.chars().any(char::is_control)
                                || value.chars().any(char::is_control)
                        })
                })
    }) {
        return Err(McpContextError::Policy);
    }
    Ok(())
}

fn reject_explicit_catalog_rejections<T>(
    catalog: &McpCatalog<T>,
    policies: &[McpResourceImportPolicy],
) -> Result<(), McpContextError> {
    if catalog.rejected.iter().any(|rejection| {
        policies
            .iter()
            .any(|policy| wildcard_matches(&policy.uri_pattern, &rejection.identity))
    }) {
        Err(McpContextError::Rejected)
    } else {
        Ok(())
    }
}

fn resource_matches(resource: &McpImportedResource, uri: &str) -> bool {
    match resource.binding.kind {
        McpResourceBindingKind::Resource => resource.binding.remote_uri == uri,
        McpResourceBindingKind::Template => uri_template_matches(&resource.binding.remote_uri, uri),
    }
}

fn uri_template_matches(template: &str, uri: &str) -> bool {
    let mut remainder = uri;
    let mut template_remainder = template;
    loop {
        let Some(open) = template_remainder.find('{') else {
            return remainder == template_remainder;
        };
        let literal = &template_remainder[..open];
        let Some(after_literal) = remainder.strip_prefix(literal) else {
            return false;
        };
        let Some(close) = template_remainder[open + 1..].find('}') else {
            return false;
        };
        let after_close = open + close + 2;
        let tail = &template_remainder[after_close..];
        let next_literal_end = tail.find('{').unwrap_or(tail.len());
        let next_literal = &tail[..next_literal_end];
        let consumed = if next_literal.is_empty() {
            after_literal.len()
        } else {
            let Some(position) = after_literal.find(next_literal) else {
                return false;
            };
            position
        };
        let variable = &after_literal[..consumed];
        if variable.is_empty()
            || variable
                .chars()
                .any(|character| matches!(character, '/' | '?' | '#') || character.is_control())
        {
            return false;
        }
        remainder = &after_literal[consumed..];
        template_remainder = tail;
    }
}

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == value,
        Some((prefix, suffix)) if !suffix.contains('*') => {
            value.starts_with(prefix)
                && value.ends_with(suffix)
                && value.len() >= prefix.len() + suffix.len()
        }
        Some(_) => false,
    }
}

fn validate_resource_result(
    binding: &McpResourceBinding,
    requested_uri: &str,
    result: &ReadResourceResult,
) -> Result<(), McpContextError> {
    let mut total_content_bytes = 0usize;
    for content in &result.contents {
        let (content_uri, mime_type, content_bytes) = match content {
            ResourceContents::Text {
                uri,
                text,
                mime_type,
                ..
            } => (uri, mime_type.as_deref(), text.len()),
            ResourceContents::Blob {
                uri,
                blob,
                mime_type,
                ..
            } => {
                let maximum_encoded = binding
                    .max_content_bytes
                    .saturating_add(2)
                    .saturating_div(3)
                    .saturating_mul(4)
                    .saturating_add(4);
                if blob.len() > maximum_encoded {
                    return Err(McpContextError::Content);
                }
                let decoded = BASE64_STANDARD
                    .decode(blob)
                    .map_err(|_| McpContextError::Content)?;
                (uri, mime_type.as_deref(), decoded.len())
            }
        };
        if content_uri != requested_uri {
            return Err(McpContextError::Content);
        }
        if mime_type.is_some_and(|value| !valid_mime_type(value)) {
            return Err(McpContextError::Mime);
        }
        if !binding.mime_allowlist.is_empty()
            && !mime_type.is_some_and(|value| {
                binding
                    .mime_allowlist
                    .iter()
                    .any(|allowed| wildcard_matches(allowed, value))
            })
        {
            return Err(McpContextError::Mime);
        }
        total_content_bytes = total_content_bytes
            .checked_add(content_bytes)
            .ok_or(McpContextError::Content)?;
        if total_content_bytes > binding.max_content_bytes {
            return Err(McpContextError::Content);
        }
    }
    Ok(())
}

fn valid_mime_type(value: &str) -> bool {
    let essence = value.split(';').next().unwrap_or_default().trim();
    let Some((type_, subtype)) = essence.split_once('/') else {
        return false;
    };
    !type_.is_empty()
        && !subtype.is_empty()
        && value.len() <= 255
        && value.is_ascii()
        && !value.bytes().any(|byte| byte.is_ascii_control())
        && [type_, subtype].into_iter().all(|component| {
            component.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(
                        byte,
                        b'!' | b'#' | b'$' | b'&' | b'-' | b'^' | b'_' | b'.' | b'+'
                    )
            })
        })
}

fn canonical_sha256(value: &impl Serialize) -> Result<String, McpContextError> {
    let canonical = serde_jcs::to_vec(value).map_err(|_| McpContextError::Evidence)?;
    let digest = Sha256::digest(canonical);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(encoded)
}

fn now_unix_ms() -> Result<u64, McpContextError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| McpContextError::Clock)?;
    u64::try_from(duration.as_millis()).map_err(|_| McpContextError::Clock)
}

#[derive(Debug, Default)]
pub struct McpCatalogInvalidation {
    state: Mutex<McpCatalogInvalidationState>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct McpCatalogInvalidationState {
    pub tools_stale: bool,
    pub resources_stale: bool,
    pub prompts_stale: bool,
    pub updated_resources: BTreeSet<String>,
}

impl McpCatalogInvalidation {
    pub fn snapshot(&self) -> McpCatalogInvalidationState {
        self.state
            .lock()
            .expect("invalidation mutex poisoned")
            .clone()
    }

    pub fn resource_is_stale(&self, uri: &str) -> Result<bool, McpContextError> {
        let state = self.state.lock().map_err(|_| McpContextError::Remote)?;
        Ok(state.resources_stale || state.updated_resources.contains(uri))
    }

    pub fn acknowledge_resource(&self, uri: &str) -> Result<(), McpContextError> {
        self.state
            .lock()
            .map_err(|_| McpContextError::Remote)?
            .updated_resources
            .remove(uri);
        Ok(())
    }
}

impl McpNotificationObserver for McpCatalogInvalidation {
    fn on_notification(
        &self,
        notification: &insight_mcp::JsonRpcNotification<serde_json::Value>,
    ) -> Result<(), TransportError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TransportError::Notification)?;
        match notification.method.as_str() {
            "notifications/subscriptions/acknowledged" => {}
            "notifications/tools/list_changed" => state.tools_stale = true,
            "notifications/resources/list_changed" => state.resources_stale = true,
            "notifications/prompts/list_changed" => state.prompts_stale = true,
            "notifications/resources/updated" => {
                let uri = notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("uri"))
                    .and_then(serde_json::Value::as_str)
                    .ok_or(TransportError::Notification)?;
                if uri.len() > 8 * 1024 || uri.chars().any(char::is_control) {
                    return Err(TransportError::Notification);
                }
                state.updated_resources.insert(uri.to_owned());
            }
            _ => return Err(TransportError::Notification),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpContextError {
    Server,
    Policy,
    Missing,
    Duplicate,
    Rejected,
    Binding,
    Unauthorized,
    Remote,
    Content,
    Mime,
    Evidence,
    Clock,
    Interaction,
}

impl std::fmt::Display for McpContextError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MCP context provider failed")
    }
}

impl std::error::Error for McpContextError {}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use insight_mcp::{
        ClientCapabilities, ClientInfo, McpNotificationObserver, McpTransport,
        NoopNotificationObserver, TransportKind,
    };
    use serde_json::{json, Value};

    use super::*;

    #[derive(Default)]
    struct FixtureTransport {
        requests: Mutex<Vec<Value>>,
        responses: Mutex<Vec<Value>>,
    }

    #[async_trait::async_trait]
    impl McpTransport for FixtureTransport {
        fn kind(&self) -> TransportKind {
            TransportKind::StreamableHttp
        }

        async fn exchange(
            &self,
            request: &Value,
            _parameter_headers: &BTreeMap<String, String>,
            _cancellation: &CancellationToken,
            _observer: &dyn McpNotificationObserver,
        ) -> Result<Value, TransportError> {
            self.requests.lock().unwrap().push(request.clone());
            let mut response = self.responses.lock().unwrap().remove(0);
            response["id"] = request["id"].clone();
            Ok(response)
        }
    }

    fn server(transport: insight_mcp::McpTransportKind) -> McpServerBindingIdentity {
        McpServerBindingIdentity {
            connection_id: "mcp.fixture".to_owned(),
            server_id: "fixture".to_owned(),
            protocol_version: insight_mcp::MCP_PROTOCOL_VERSION.to_owned(),
            transport,
            principal_scope: insight_mcp::PrincipalScope::Service,
            discovery_fingerprint: "a".repeat(64),
        }
    }

    fn fixture_client(transport: Arc<FixtureTransport>) -> Arc<McpClient> {
        Arc::new(
            McpClient::new(
                transport,
                ClientInfo {
                    name: "fixture".to_owned(),
                    version: "1.0.0".to_owned(),
                    title: None,
                    description: None,
                    website_url: None,
                    icons: Vec::new(),
                },
                ClientCapabilities::default(),
            )
            .unwrap(),
        )
    }

    fn resource_response(text: &str, ttl_ms: u64) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": 0,
            "result": {
                "resultType": "complete",
                "contents": [{
                    "uri": "repo://project/readme",
                    "text": text,
                    "mimeType": "text/plain"
                }],
                "ttlMs": ttl_ms,
                "cacheScope": "private"
            }
        })
    }

    fn input_required_response() -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": 0,
            "result": {
                "resultType": "input_required",
                "requestState": "continue-1"
            }
        })
    }

    fn fixture_context(transport: Arc<FixtureTransport>) -> McpContextProvider {
        McpContextProvider::freeze(
            server(insight_mcp::McpTransportKind::StreamableHttp),
            fixture_client(transport),
            ResourceCatalog {
                items: vec![Resource {
                    uri: "repo://project/readme".to_owned(),
                    name: "readme".to_owned(),
                    title: None,
                    description: None,
                    mime_type: Some("text/plain".to_owned()),
                    size: None,
                    icons: Vec::new(),
                    annotations: None,
                    metadata: None,
                }],
                rejected: Vec::new(),
                ttl_ms: 1_000,
                cache_scope: insight_mcp::CacheScope::Private,
                descriptor_hash: "b".repeat(64),
            },
            ResourceTemplateCatalog::empty("resource_templates").unwrap(),
            PromptCatalog::empty("prompts").unwrap(),
            vec![McpResourceImportPolicy {
                uri_pattern: "repo://project/readme".to_owned(),
                mime_allowlist: vec!["text/plain".to_owned()],
                max_content_bytes: 1_024,
            }],
            Vec::new(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn resource_cache_is_principal_scoped_and_exact_updates_invalidate_it() {
        let transport = Arc::new(FixtureTransport {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(vec![
                resource_response("service", 60_000),
                resource_response("user-1", 60_000),
                resource_response("user-2", 60_000),
                resource_response("user-1-updated", 60_000),
            ]),
        });
        let invalidation = Arc::new(McpCatalogInvalidation::default());
        let context = fixture_context(Arc::clone(&transport))
            .with_catalog_invalidation(Arc::clone(&invalidation));
        let cancellation = CancellationToken::new();

        context
            .read_resource(
                "repo://project/readme",
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        context
            .read_resource(
                "repo://project/readme",
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        context
            .read_resource_with_client(
                fixture_client(Arc::clone(&transport)),
                "repo://project/readme",
                Some(("tenant-a", "user-1")),
                McpResourceContinuation::default(),
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        context
            .read_resource_with_client(
                fixture_client(Arc::clone(&transport)),
                "repo://project/readme",
                Some(("tenant-a", "user-1")),
                McpResourceContinuation::default(),
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        context
            .read_resource_with_client(
                fixture_client(Arc::clone(&transport)),
                "repo://project/readme",
                Some(("tenant-a", "user-2")),
                McpResourceContinuation::default(),
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        assert_eq!(transport.requests.lock().unwrap().len(), 3);

        invalidation
            .on_notification(&insight_mcp::JsonRpcNotification {
                jsonrpc: "2.0".to_owned(),
                method: "notifications/resources/updated".to_owned(),
                params: Some(json!({"uri": "repo://project/readme"})),
            })
            .unwrap();
        context
            .read_resource_with_client(
                fixture_client(Arc::clone(&transport)),
                "repo://project/readme",
                Some(("tenant-a", "user-1")),
                McpResourceContinuation::default(),
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        assert_eq!(transport.requests.lock().unwrap().len(), 4);
        assert!(!invalidation
            .snapshot()
            .updated_resources
            .contains("repo://project/readme"));
    }

    #[tokio::test]
    async fn zero_ttl_and_interactive_continuations_are_never_cached() {
        let transport = Arc::new(FixtureTransport {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(vec![
                resource_response("zero-1", 0),
                resource_response("zero-2", 0),
                input_required_response(),
                resource_response("interactive", 60_000),
                resource_response("fresh", 60_000),
            ]),
        });
        let context = fixture_context(Arc::clone(&transport));
        let cancellation = CancellationToken::new();
        for _ in 0..2 {
            context
                .read_resource(
                    "repo://project/readme",
                    &cancellation,
                    &NoopNotificationObserver,
                )
                .await
                .unwrap();
        }
        assert!(matches!(
            context
                .read_resource(
                    "repo://project/readme",
                    &cancellation,
                    &NoopNotificationObserver,
                )
                .await
                .unwrap(),
            McpContextOutcome::InputRequired(_)
        ));
        context
            .read_resource_with_inputs(
                "repo://project/readme",
                BTreeMap::from([(
                    "confirm".to_owned(),
                    InputResponse::Elicitation(insight_mcp::ElicitResult {
                        action: insight_mcp::ElicitAction::Accept,
                        content: None,
                    }),
                )]),
                Some("continue-1".to_owned()),
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        context
            .read_resource(
                "repo://project/readme",
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        context
            .read_resource(
                "repo://project/readme",
                &cancellation,
                &NoopNotificationObserver,
            )
            .await
            .unwrap();
        assert_eq!(transport.requests.lock().unwrap().len(), 5);
    }

    #[test]
    fn uri_templates_and_wildcards_do_not_cross_path_boundaries() {
        assert!(uri_template_matches(
            "repo://project/{path}",
            "repo://project/readme"
        ));
        assert!(!uri_template_matches(
            "repo://project/{path}",
            "repo://project/a/b"
        ));
        assert!(wildcard_matches("repo://project/*", "repo://project/a/b"));
        assert!(!wildcard_matches("repo://*/x/*", "repo://project/x/a"));
    }

    #[test]
    fn subscription_notifications_only_mark_non_authoritative_staleness() {
        let invalidation = McpCatalogInvalidation::default();
        invalidation
            .on_notification(&insight_mcp::JsonRpcNotification {
                jsonrpc: "2.0".to_owned(),
                method: "notifications/resources/updated".to_owned(),
                params: Some(serde_json::json!({"uri":"repo://project/readme"})),
            })
            .unwrap();
        let state = invalidation.snapshot();
        assert_eq!(
            state.updated_resources,
            BTreeSet::from(["repo://project/readme".to_owned()])
        );
        assert!(!state.resources_stale);
    }

    #[test]
    fn remote_resource_identity_rejects_local_file_and_unsafe_templates() {
        assert_eq!(
            validate_resource_identity(
                &server(insight_mcp::McpTransportKind::StreamableHttp),
                "file:///etc/passwd",
                McpResourceBindingKind::Resource,
            ),
            Err(McpContextError::Policy)
        );
        assert_eq!(
            validate_resource_identity(
                &server(insight_mcp::McpTransportKind::Stdio),
                "file:///workspace/readme",
                McpResourceBindingKind::Resource,
            ),
            Err(McpContextError::Policy)
        );
        assert_eq!(
            validate_resource_identity(
                &server(insight_mcp::McpTransportKind::StreamableHttp),
                "repo://project/{?path}",
                McpResourceBindingKind::Template,
            ),
            Err(McpContextError::Policy)
        );
    }

    #[test]
    fn resource_read_cannot_return_content_for_another_uri() {
        let binding = McpResourceBinding::seal(
            server(insight_mcp::McpTransportKind::StreamableHttp),
            "repo://project/readme".to_owned(),
            McpResourceBindingKind::Resource,
            vec!["text/plain".to_owned()],
            1024,
            "b".repeat(64),
            "c".repeat(64),
        )
        .unwrap();
        let result = ReadResourceResult {
            result_type: "complete".to_owned(),
            contents: vec![ResourceContents::Text {
                uri: "repo://project/secret".to_owned(),
                text: "secret".to_owned(),
                mime_type: Some("text/plain".to_owned()),
                metadata: None,
            }],
            ttl_ms: 1000,
            cache_scope: insight_mcp::CacheScope::Private,
            metadata: None,
        };
        assert_eq!(
            validate_resource_result(&binding, "repo://project/readme", &result),
            Err(McpContextError::Content)
        );
    }

    #[test]
    fn resource_blob_must_be_valid_base64_and_obey_decoded_size_bound() {
        let binding = McpResourceBinding::seal(
            server(insight_mcp::McpTransportKind::StreamableHttp),
            "repo://project/image".to_owned(),
            McpResourceBindingKind::Resource,
            vec!["image/png".to_owned()],
            3,
            "b".repeat(64),
            "c".repeat(64),
        )
        .unwrap();
        let result = |blob: &str| ReadResourceResult {
            result_type: "complete".to_owned(),
            contents: vec![ResourceContents::Blob {
                uri: "repo://project/image".to_owned(),
                blob: blob.to_owned(),
                mime_type: Some("image/png".to_owned()),
                metadata: None,
            }],
            ttl_ms: 1000,
            cache_scope: insight_mcp::CacheScope::Private,
            metadata: None,
        };
        assert!(
            validate_resource_result(&binding, "repo://project/image", &result("AQID")).is_ok()
        );
        assert_eq!(
            validate_resource_result(&binding, "repo://project/image", &result("not-base64")),
            Err(McpContextError::Content)
        );
        assert_eq!(
            validate_resource_result(&binding, "repo://project/image", &result("AQIDBA==")),
            Err(McpContextError::Content)
        );
    }
}
