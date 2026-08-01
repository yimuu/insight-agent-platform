//! Principal-scoped, allowlist-only MCP catalog and preview APIs.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use insight_durable::{McpInteractionPrincipal, McpOAuthDurableRepository};
use insight_mcp::{
    CompletionArgument, CompletionReference, McpServerBindingIdentity, McpToolBinding,
    NoopNotificationObserver, PrincipalScope, ServerCapabilities, ServerInfo,
    MCP_TASKS_EXTENSION_ID,
};
use insight_resources::mcp_context::{
    McpCatalogInvalidation, McpContextOutcome, McpContextProvider, McpImportedResourceDescriptor,
};
use insight_runtime::{RequestMetadata, RunService};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use super::{
    auth::ApiAuth,
    response::{ApiError, ApiResponse},
};

const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PAGE_SIZE: usize = 100;

#[derive(Clone)]
pub struct McpCatalogServer {
    pub server_id: String,
    pub binding: McpServerBindingIdentity,
    pub capabilities: ServerCapabilities,
    pub server_info: ServerInfo,
    pub required_scopes: BTreeSet<String>,
    pub tools: Vec<McpToolBinding>,
    pub context: Option<Arc<McpContextProvider>>,
    pub catalog_invalidation: Option<Arc<McpCatalogInvalidation>>,
}

impl McpCatalogServer {
    pub fn new(
        server_id: impl Into<String>,
        binding: McpServerBindingIdentity,
        capabilities: ServerCapabilities,
        server_info: ServerInfo,
        required_scopes: BTreeSet<String>,
        mut tools: Vec<McpToolBinding>,
    ) -> Result<Self, &'static str> {
        let server_id = server_id.into();
        tools.sort_by(|left, right| left.remote_name.cmp(&right.remote_name));
        if server_id.is_empty()
            || server_id != binding.server_id
            || tools.iter().any(|tool| tool.server.server_id != server_id)
        {
            return Err("invalid MCP catalog server");
        }
        Ok(Self {
            server_id,
            binding,
            capabilities,
            server_info,
            required_scopes,
            tools,
            context: None,
            catalog_invalidation: None,
        })
    }

    pub fn with_context(
        mut self,
        context: Option<Arc<McpContextProvider>>,
        catalog_invalidation: Option<Arc<McpCatalogInvalidation>>,
    ) -> Self {
        self.context = context;
        self.catalog_invalidation = catalog_invalidation;
        self
    }
}

#[derive(Clone)]
pub struct McpCatalogApiState {
    auth: ApiAuth,
    oauth_repository: Arc<dyn McpOAuthDurableRepository>,
    servers: McpCatalogRegistry,
    profiles: McpProfileReport,
    run_service: Option<RunService>,
}

/// Process-local projection of active immutable MCP Revisions. The durable
/// active pointer remains authoritative; this registry is replaced by the
/// revision reconciler and is never a configuration source.
#[derive(Clone, Default)]
pub struct McpCatalogRegistry {
    servers: Arc<RwLock<BTreeMap<String, McpCatalogServer>>>,
}

impl McpCatalogRegistry {
    pub fn replace(&self, servers: Vec<McpCatalogServer>) -> Result<(), &'static str> {
        let mut indexed = BTreeMap::new();
        for server in servers {
            if indexed.insert(server.server_id.clone(), server).is_some() {
                return Err("duplicate MCP catalog server");
            }
        }
        *self
            .servers
            .write()
            .map_err(|_| "MCP catalog registry unavailable")? = indexed;
        Ok(())
    }

    pub fn upsert(&self, server: McpCatalogServer) -> Result<(), &'static str> {
        self.servers
            .write()
            .map_err(|_| "MCP catalog registry unavailable")?
            .insert(server.server_id.clone(), server);
        Ok(())
    }

    pub fn remove(&self, server_id: &str) -> Result<(), &'static str> {
        self.servers
            .write()
            .map_err(|_| "MCP catalog registry unavailable")?
            .remove(server_id);
        Ok(())
    }

    fn get(&self, server_id: &str) -> Result<Option<McpCatalogServer>, ApiError> {
        Ok(self
            .servers
            .read()
            .map_err(|_| ApiError::internal())?
            .get(server_id)
            .cloned())
    }

    fn snapshot(&self) -> Result<Vec<McpCatalogServer>, ApiError> {
        Ok(self
            .servers
            .read()
            .map_err(|_| ApiError::internal())?
            .values()
            .cloned()
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct McpProfileState {
    pub implemented: bool,
    pub enabled: bool,
    pub capabilities: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct McpProfileReport {
    pub modern_client: McpProfileState,
    pub modern_server: McpProfileState,
    pub tasks: McpProfileState,
    pub legacy_client: McpProfileState,
}

impl Default for McpProfileReport {
    fn default() -> Self {
        Self {
            modern_client: McpProfileState {
                implemented: true,
                enabled: false,
                capabilities: BTreeSet::from([
                    "authorization".to_owned(),
                    "completion".to_owned(),
                    "elicitation".to_owned(),
                    "prompts".to_owned(),
                    "resources".to_owned(),
                    "stdio".to_owned(),
                    "streamable_http".to_owned(),
                    "subscriptions".to_owned(),
                    "tools".to_owned(),
                ]),
            },
            modern_server: McpProfileState {
                implemented: true,
                enabled: false,
                capabilities: BTreeSet::from([
                    "authorization".to_owned(),
                    "completion".to_owned(),
                    "elicitation".to_owned(),
                    "prompts".to_owned(),
                    "resources".to_owned(),
                    "streamable_http".to_owned(),
                    "subscriptions".to_owned(),
                    "tools".to_owned(),
                ]),
            },
            tasks: McpProfileState {
                implemented: true,
                enabled: false,
                capabilities: BTreeSet::from([
                    "notifications/tasks/status".to_owned(),
                    "tasks/cancel".to_owned(),
                    "tasks/get".to_owned(),
                    "tasks/update".to_owned(),
                ]),
            },
            legacy_client: McpProfileState {
                implemented: true,
                enabled: false,
                capabilities: BTreeSet::from([
                    "completion".to_owned(),
                    "initialize_session".to_owned(),
                    "prompts".to_owned(),
                    "resources".to_owned(),
                    "stdio".to_owned(),
                    "streamable_http".to_owned(),
                    "subscriptions".to_owned(),
                    "tools".to_owned(),
                ]),
            },
        }
    }
}

impl McpCatalogApiState {
    pub fn new(
        auth: ApiAuth,
        oauth_repository: Arc<dyn McpOAuthDurableRepository>,
        servers: Vec<McpCatalogServer>,
    ) -> Result<Self, &'static str> {
        let registry = McpCatalogRegistry::default();
        registry.replace(servers)?;
        Ok(Self {
            auth,
            oauth_repository,
            servers: registry,
            profiles: McpProfileReport::default(),
            run_service: None,
        })
    }

    pub fn from_registry(
        auth: ApiAuth,
        oauth_repository: Arc<dyn McpOAuthDurableRepository>,
        registry: McpCatalogRegistry,
    ) -> Self {
        Self {
            auth,
            oauth_repository,
            servers: registry,
            profiles: McpProfileReport::default(),
            run_service: None,
        }
    }

    pub fn with_run_service(mut self, run_service: RunService) -> Self {
        self.run_service = Some(run_service);
        self
    }

    pub fn with_profile_report(mut self, profiles: McpProfileReport) -> Self {
        self.profiles = profiles;
        self
    }
}

pub fn build_mcp_catalog_router(state: McpCatalogApiState) -> Router {
    let state = Arc::new(state);
    let auth = state.auth.clone();
    Router::new()
        .route("/v1/mcp/profiles", get(get_profiles))
        .route("/v1/mcp/servers", get(list_servers))
        .route("/v1/mcp/servers/{server_id}/tools", get(list_tools))
        .route("/v1/mcp/servers/{server_id}/resources", get(list_resources))
        .route(
            "/v1/mcp/servers/{server_id}/resources/read",
            post(read_resource),
        )
        .route("/v1/mcp/servers/{server_id}/prompts", get(list_prompts))
        .route(
            "/v1/mcp/servers/{server_id}/prompts/{prompt_name}/preview",
            post(preview_prompt),
        )
        .route("/v1/mcp/servers/{server_id}/completion", post(complete))
        .route(
            "/v1/mcp/servers/{server_id}/agents/{agent_id}/runs",
            post(create_run_with_context),
        )
        .route_layer(middleware::from_fn(
            move |headers: HeaderMap, request: Request<Body>, next: Next| {
                let auth = auth.clone();
                async move {
                    if !auth.accepts(&headers) {
                        return ApiError::unauthorized().into_response();
                    }
                    next.run(request).await
                }
            },
        ))
        .with_state(state)
}

async fn get_profiles(State(state): State<Arc<McpCatalogApiState>>) -> Result<Response, ApiError> {
    ok(&state.profiles)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageQuery {
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

impl PageQuery {
    fn validate(self) -> Result<(Option<String>, usize), ApiError> {
        if self.cursor.as_ref().is_some_and(|cursor| {
            cursor.is_empty() || cursor.len() > 8 * 1024 || cursor.chars().any(char::is_control)
        }) {
            return Err(ApiError::input_invalid());
        }
        let limit = self.limit.unwrap_or(DEFAULT_PAGE_SIZE);
        if limit == 0 || limit > MAX_PAGE_SIZE {
            return Err(ApiError::input_invalid());
        }
        Ok((self.cursor, limit))
    }
}

#[derive(Debug, Serialize)]
struct CatalogPage {
    items: Vec<Value>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct ServerDto {
    server_id: String,
    name: String,
    version: String,
    protocol_version: String,
    transport: insight_mcp::McpTransportKind,
    authorization: &'static str,
    connection_status: &'static str,
    capabilities: ServerCapabilities,
    tasks: bool,
    required_scopes: BTreeSet<String>,
    catalog_stale: bool,
    updated_resources: usize,
}

async fn list_servers(
    State(state): State<Arc<McpCatalogApiState>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let principal = principal(&headers)?;
    let projected = state.servers.snapshot()?;
    let mut servers = Vec::with_capacity(projected.len());
    for server in &projected {
        let (authorization, connection_status) = match &server.binding.principal_scope {
            PrincipalScope::Service => ("service", "authorized"),
            PrincipalScope::Tenant(_) => ("tenant", "authorized"),
            PrincipalScope::User(_) => {
                let credential = state
                    .oauth_repository
                    .load_mcp_oauth_credential(&principal, &server.server_id)
                    .await
                    .map_err(|_| unavailable())?;
                let connection_status = match credential {
                    Some(credential)
                        if credential.revoked_at().is_none()
                            && server.required_scopes.iter().all(|required| {
                                credential
                                    .scopes()
                                    .iter()
                                    .any(|granted| granted == required)
                            }) =>
                    {
                        "authorized"
                    }
                    Some(credential) if credential.revoked_at().is_none() => "insufficient_scope",
                    Some(_) | None => "authorization_required",
                };
                ("oauth_user", connection_status)
            }
        };
        servers.push(ServerDto {
            server_id: server.server_id.clone(),
            name: server.server_info.name.clone(),
            version: server.server_info.version.clone(),
            protocol_version: server.binding.protocol_version.clone(),
            transport: server.binding.transport,
            authorization,
            connection_status,
            capabilities: server.capabilities.clone(),
            tasks: server
                .capabilities
                .extensions
                .as_ref()
                .and_then(|extensions| extensions.get(MCP_TASKS_EXTENSION_ID))
                .is_some(),
            required_scopes: server.required_scopes.clone(),
            catalog_stale: server
                .catalog_invalidation
                .as_ref()
                .is_some_and(|invalidation| {
                    let state = invalidation.snapshot();
                    state.tools_stale || state.resources_stale || state.prompts_stale
                }),
            updated_resources: server
                .catalog_invalidation
                .as_ref()
                .map(|invalidation| invalidation.snapshot().updated_resources.len())
                .unwrap_or(0),
        });
    }
    ok(servers)
}

async fn list_tools(
    State(state): State<Arc<McpCatalogApiState>>,
    Path(server_id): Path<String>,
    Query(query): Query<PageQuery>,
) -> Result<Response, ApiError> {
    let server = state.servers.get(&server_id)?.ok_or_else(not_found)?;
    let (cursor, limit) = query.validate()?;
    let items = server
        .tools
        .iter()
        .map(|binding| {
            json!({
                "name": binding.remote_name,
                "title": binding.title,
                "description": binding.description,
                "input_schema": binding.input_schema,
                "output_schema": binding.output_schema,
                "binding_hash": binding.descriptor_hash,
                "action_id": binding.action_id,
                "model_tool_name": binding.model_tool_name,
            })
        })
        .collect::<Vec<_>>();
    ok(page(items, cursor.as_deref(), limit, |item| {
        item["name"].as_str().unwrap_or_default()
    }))
}

async fn list_resources(
    State(state): State<Arc<McpCatalogApiState>>,
    Path(server_id): Path<String>,
    Query(query): Query<PageQuery>,
) -> Result<Response, ApiError> {
    let server = state.servers.get(&server_id)?.ok_or_else(not_found)?;
    let context = server.context.as_ref().ok_or_else(not_found)?;
    let (cursor, limit) = query.validate()?;
    let mut items = context
        .resources()
        .iter()
        .map(resource_dto)
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        left["uri"]
            .as_str()
            .unwrap_or_default()
            .cmp(right["uri"].as_str().unwrap_or_default())
    });
    ok(page(items, cursor.as_deref(), limit, |item| {
        item["uri"].as_str().unwrap_or_default()
    }))
}

async fn list_prompts(
    State(state): State<Arc<McpCatalogApiState>>,
    Path(server_id): Path<String>,
    Query(query): Query<PageQuery>,
) -> Result<Response, ApiError> {
    let server = state.servers.get(&server_id)?.ok_or_else(not_found)?;
    let context = server.context.as_ref().ok_or_else(not_found)?;
    let (cursor, limit) = query.validate()?;
    let items = context
        .prompts()
        .map(|prompt| {
            json!({
                "name": prompt.descriptor.name,
                "title": prompt.descriptor.title,
                "description": prompt.descriptor.description,
                "arguments": prompt.descriptor.arguments,
                "binding_hash": prompt.binding.descriptor_hash,
                "allow_user_invocation": prompt.policy.allow_user_invocation,
                "allow_definition_snapshot": prompt.policy.allow_definition_snapshot,
                "definition_snapshot_hash": context
                    .definition_prompt_snapshot(&prompt.descriptor.name)
                    .map(|snapshot| snapshot.content_hash.as_str()),
            })
        })
        .collect::<Vec<_>>();
    ok(page(items, cursor.as_deref(), limit, |item| {
        item["name"].as_str().unwrap_or_default()
    }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadResourceRequest {
    uri: String,
}

async fn read_resource(
    State(state): State<Arc<McpCatalogApiState>>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ReadResourceRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ApiError> {
    let principal = principal(&headers)?;
    let server = state.servers.get(&server_id)?.ok_or_else(not_found)?;
    require_runtime_authorization(&state, &server, &principal).await?;
    let context = server.context.as_ref().ok_or_else(not_found)?;
    let Json(input) = input.map_err(ApiError::from)?;
    match context
        .read_resource_for_principal(
            principal.tenant_id(),
            principal.user_id(),
            &input.uri,
            &CancellationToken::new(),
            &NoopNotificationObserver,
        )
        .await
        .map_err(|_| remote_failed())?
    {
        McpContextOutcome::Complete(snapshot) => ok(json!({
            "binding_hash": snapshot.binding.descriptor_hash,
            "content_hash": snapshot.content_hash,
            "canonical_uri": snapshot.canonical_uri,
            "result": snapshot.result,
        })),
        McpContextOutcome::InputRequired(_) => Err(interaction_required()),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PromptPreviewRequest {
    #[serde(default)]
    arguments: BTreeMap<String, String>,
}

async fn preview_prompt(
    State(state): State<Arc<McpCatalogApiState>>,
    Path((server_id, prompt_name)): Path<(String, String)>,
    headers: HeaderMap,
    input: Result<Json<PromptPreviewRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ApiError> {
    let principal = principal(&headers)?;
    let server = state.servers.get(&server_id)?.ok_or_else(not_found)?;
    require_runtime_authorization(&state, &server, &principal).await?;
    let context = server.context.as_ref().ok_or_else(not_found)?;
    let Json(input) = input.map_err(ApiError::from)?;
    match context
        .get_prompt_for_principal(
            principal.tenant_id(),
            principal.user_id(),
            &prompt_name,
            input.arguments,
            false,
            &CancellationToken::new(),
            &NoopNotificationObserver,
        )
        .await
        .map_err(|_| remote_failed())?
    {
        McpContextOutcome::Complete(snapshot) => ok(json!({
            "binding_hash": snapshot.binding.descriptor_hash,
            "content_hash": snapshot.content_hash,
            "untrusted": true,
            "result": snapshot.result,
        })),
        McpContextOutcome::InputRequired(_) => Err(interaction_required()),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletionRequest {
    #[serde(rename = "ref")]
    reference: CompletionReference,
    argument: CompletionArgument,
    #[serde(default)]
    context: BTreeMap<String, String>,
}

async fn complete(
    State(state): State<Arc<McpCatalogApiState>>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<CompletionRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ApiError> {
    let principal = principal(&headers)?;
    let server = state.servers.get(&server_id)?.ok_or_else(not_found)?;
    require_runtime_authorization(&state, &server, &principal).await?;
    let context = server.context.as_ref().ok_or_else(not_found)?;
    let Json(input) = input.map_err(ApiError::from)?;
    let result = context
        .complete_for_principal(
            principal.tenant_id(),
            principal.user_id(),
            input.reference,
            input.argument,
            input.context,
            &CancellationToken::new(),
            &NoopNotificationObserver,
        )
        .await
        .map_err(|_| remote_failed())?;
    ok(result)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpContextRunRequest {
    input: Value,
    #[serde(default)]
    context: Vec<McpAdmissionContext>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum McpAdmissionContext {
    Resource {
        uri: String,
        input_field: String,
    },
    Prompt {
        name: String,
        #[serde(default)]
        arguments: BTreeMap<String, String>,
        input_field: String,
    },
}

async fn create_run_with_context(
    State(state): State<Arc<McpCatalogApiState>>,
    Path((server_id, agent_id)): Path<(String, String)>,
    headers: HeaderMap,
    input: Result<Json<McpContextRunRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ApiError> {
    let principal = principal(&headers)?;
    let server = state.servers.get(&server_id)?.ok_or_else(not_found)?;
    require_runtime_authorization(&state, &server, &principal).await?;
    let provider = server.context.as_ref().ok_or_else(not_found)?;
    let run_service = state.run_service.as_ref().ok_or_else(unavailable)?;
    let Json(mut request) = input.map_err(ApiError::from)?;
    if request.context.len() > 16 {
        return Err(ApiError::input_invalid());
    }
    let object = request
        .input
        .as_object_mut()
        .ok_or_else(ApiError::input_invalid)?;
    let mut input_fields = BTreeSet::new();
    for selection in request.context {
        let (input_field, snapshot) = match selection {
            McpAdmissionContext::Resource { uri, input_field } => {
                let snapshot = provider
                    .read_resource_for_principal(
                        principal.tenant_id(),
                        principal.user_id(),
                        &uri,
                        &CancellationToken::new(),
                        &NoopNotificationObserver,
                    )
                    .await
                    .map_err(|_| remote_failed())?;
                let McpContextOutcome::Complete(snapshot) = snapshot else {
                    return Err(interaction_required());
                };
                (
                    input_field,
                    json!({
                        "type": "mcp_resource_snapshot",
                        "server_id": server_id,
                        "binding_hash": snapshot.binding.descriptor_hash,
                        "canonical_uri": snapshot.canonical_uri,
                        "content_hash": snapshot.content_hash,
                        "result": snapshot.result,
                    }),
                )
            }
            McpAdmissionContext::Prompt {
                name,
                arguments,
                input_field,
            } => {
                let snapshot = provider
                    .get_prompt_for_principal(
                        principal.tenant_id(),
                        principal.user_id(),
                        &name,
                        arguments,
                        false,
                        &CancellationToken::new(),
                        &NoopNotificationObserver,
                    )
                    .await
                    .map_err(|_| remote_failed())?;
                let McpContextOutcome::Complete(snapshot) = snapshot else {
                    return Err(interaction_required());
                };
                (
                    input_field,
                    json!({
                        "type": "mcp_prompt_snapshot",
                        "server_id": server_id,
                        "binding_hash": snapshot.binding.descriptor_hash,
                        "content_hash": snapshot.content_hash,
                        "untrusted": true,
                        "result": snapshot.result,
                    }),
                )
            }
        };
        if !valid_input_field(&input_field)
            || !input_fields.insert(input_field.clone())
            || object.contains_key(&input_field)
        {
            return Err(ApiError::input_invalid());
        }
        object.insert(input_field, snapshot);
    }
    let request_id = bounded_header(&headers, "x-request-id")?.map(str::to_owned);
    let record = run_service
        .create_detached_for_tenant(
            principal.tenant_id(),
            &agent_id,
            request.input,
            RequestMetadata { request_id },
        )
        .await
        .map_err(ApiError::from)?;
    let mut response = (StatusCode::ACCEPTED, Json(ApiResponse::ok(record))).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "private, no-store".parse().unwrap());
    Ok(response)
}

fn valid_input_field(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

async fn require_runtime_authorization(
    state: &McpCatalogApiState,
    server: &McpCatalogServer,
    principal: &McpInteractionPrincipal,
) -> Result<(), ApiError> {
    if matches!(server.binding.principal_scope, PrincipalScope::User(_)) {
        let credential = state
            .oauth_repository
            .load_mcp_oauth_credential(principal, &server.server_id)
            .await
            .map_err(|_| unavailable())?
            .filter(|credential| credential.revoked_at().is_none())
            .ok_or_else(|| {
                ApiError::new(
                    StatusCode::CONFLICT,
                    "MCP_AUTHORIZATION_REQUIRED",
                    "MCP authorization is required",
                )
            })?;
        if !server.required_scopes.iter().all(|required| {
            credential
                .scopes()
                .iter()
                .any(|granted| granted == required)
        }) {
            return Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "MCP_AUTHORIZATION_INSUFFICIENT_SCOPE",
                "MCP authorization has insufficient scope",
            ));
        }
    }
    Ok(())
}

fn resource_dto(resource: &insight_resources::mcp_context::McpImportedResource) -> Value {
    let (descriptor, uri) = match &resource.descriptor {
        McpImportedResourceDescriptor::Resource(descriptor) => (
            serde_json::to_value(descriptor).unwrap_or(Value::Null),
            descriptor.uri.as_str(),
        ),
        McpImportedResourceDescriptor::Template(descriptor) => (
            serde_json::to_value(descriptor).unwrap_or(Value::Null),
            descriptor.uri_template.as_str(),
        ),
    };
    json!({
        "uri": uri,
        "kind": resource.binding.kind,
        "descriptor": descriptor,
        "binding_hash": resource.binding.descriptor_hash,
        "max_content_bytes": resource.binding.max_content_bytes,
    })
}

fn page(
    items: Vec<Value>,
    cursor: Option<&str>,
    limit: usize,
    identity: impl for<'a> Fn(&'a Value) -> &'a str,
) -> CatalogPage {
    let start = cursor
        .map(|cursor| items.partition_point(|item| identity(item) <= cursor))
        .unwrap_or(0);
    let end = start.saturating_add(limit).min(items.len());
    let next_cursor = (end < items.len())
        .then(|| items.get(end - 1))
        .flatten()
        .map(|item| identity(item).to_owned());
    CatalogPage {
        items: items[start..end].to_vec(),
        next_cursor,
    }
}

fn principal(headers: &HeaderMap) -> Result<McpInteractionPrincipal, ApiError> {
    let tenant = bounded_header(headers, "x-tenant-id")?.unwrap_or("default");
    let user = bounded_header(headers, "x-user-id")?.unwrap_or("service");
    McpInteractionPrincipal::new(tenant, user).map_err(|_| ApiError::input_invalid())
}

fn bounded_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, ApiError> {
    let mut values = headers.get_all(name).iter();
    let value = values.next();
    if values.next().is_some() {
        return Err(ApiError::input_invalid());
    }
    value
        .map(|value| {
            value
                .to_str()
                .ok()
                .filter(|value| {
                    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
                })
                .ok_or_else(ApiError::input_invalid)
        })
        .transpose()
}

fn ok<T: Serialize>(data: T) -> Result<Response, ApiError> {
    let mut response = Json(ApiResponse::ok(data)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "private, no-store".parse().unwrap());
    Ok(response)
}

fn not_found() -> ApiError {
    ApiError::new(
        StatusCode::NOT_FOUND,
        "MCP_SERVER_NOT_FOUND",
        "MCP server or catalog entry was not found",
    )
}

fn unavailable() -> ApiError {
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "MCP_CATALOG_UNAVAILABLE",
        "MCP catalog is unavailable",
    )
}

fn remote_failed() -> ApiError {
    ApiError::new(
        StatusCode::BAD_GATEWAY,
        "MCP_REMOTE_FAILED",
        "MCP remote operation failed",
    )
}

fn interaction_required() -> ApiError {
    ApiError::new(
        StatusCode::CONFLICT,
        "MCP_INTERACTION_REQUIRED",
        "MCP interaction must be completed through a durable Run",
    )
}
