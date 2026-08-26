//! Deployable public Gateway for the clean-cut Platform `/v1` contract.

mod dependency_observer;

use dependency_observer::install_postgres_dependency_metrics;

use async_trait::async_trait;
use axum::{
    extract::{Extension, Request, State},
    http::{header::CACHE_CONTROL, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use insight_platform_api::{
    artifact::{
        build_artifact_router, ArtifactApplication, ArtifactApplicationError,
        ArtifactContentStreamV1, ArtifactHttpState, ArtifactMutationAcceptedV1, ArtifactViewV1,
        CompleteArtifactUploadIntent, DeleteArtifactIntent, PrepareArtifactUploadIntent,
        PrepareArtifactUploadResponseV1, ReadArtifactIntent, SystemArtifactClock,
    },
    authentication::{
        authenticate_public_request, AuthenticationError, ExternalPrincipalBindingAuthority,
        PublicAuthenticationState, SystemAuthenticationClock,
    },
    oidc::InstalledOidcVerifierConfig,
    operation::{
        build_operation_router, OperationApplication, OperationApplicationError,
        OperationHttpState, SystemOperationClock,
    },
    resource::{
        build_resource_router, deployment_etag, resource_etag, resource_version_etag,
        BuildContextDatasetIntent, ControlDeploymentIntent, CreateDeploymentIntent,
        CreateResourceIntent, DeploymentViewV1, DiscoverMcpDeploymentIntent,
        PublishResourceDraftIntent, PublishResourceDraftRequestV1, PublishResourceDraftResponseV1,
        PublishedResourceVersionSummaryV1, ReadDeploymentIntent, ReadResourceIntent,
        ReadResourceVersionIntent, ResourceApplication, ResourceApplicationError,
        ResourceHttpState, ResourceVersionViewV1, ResourceViewV1, SystemResourceClock,
        UpdateResourceDraftIntent, ValidateResourceDraftIntent,
    },
    run::{
        build_run_router, run_etag, ControlRunIntent, CreateRunIntent, HmacRunEventCursorCodec,
        ReadRunEventsIntent, ReadRunIntent, RunApplication, RunApplicationError,
        RunEventCursorCodec, RunEventProjectionV1, RunHttpState, RunResultViewV1, RunViewV1,
        SignalRunIntent, SystemRunClock,
    },
    task::{
        build_task_router, task_etag, ReadTaskIntent, ResolveTaskIntent, SystemTaskClock,
        TaskActionV1, TaskApplication, TaskApplicationError, TaskHttpState, TaskOwnerLinkV1,
        TaskViewV1,
    },
    trace::establish_public_trace,
};
use insight_platform_context::RequestContextDatasetBuild;
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, ActiveTarget, AdministrativeGate, CommandAudit,
    DeploymentClosure, EntityLifecycle, ExactDeploymentRef, ExternalLeafFailureMutationIds,
    JsonLimits, OperationViewV1, PrincipalKind, PrincipalSnapshot, ReadOperation, ResourceId,
    ResourceKind, RunBindingsSnapshot, Sha256Digest, ValueRef, MAX_ARTIFACT_BYTES,
};
use insight_platform_invocations::{
    CapabilityApprovalDecision, CapabilityApprovalDispatchMutationIds, InvocationTransaction,
    ResolveCapabilityApproval,
};
use insight_platform_jobs::WakeSource;
use insight_platform_mcp_host::CreateMcpDiscoveryOperation;
use insight_platform_observability::{
    DependencyObservationMetrics, OperationalCapacityMetric, OperationalCapacitySnapshot,
    OperationalCapacitySource, ProcessHttpMetrics,
};
use insight_platform_orchestrator::{
    AdmitRun, PlanNodeKey, RequestRunCancel, RunInputValue, SetRunPause,
};
use insight_platform_postgres::{
    artifact_repository::{ArtifactDeletionApprovalDecision, ResolveArtifactDeletionApproval},
    dependency_health::run_postgres_health_sampler,
    operation_repository::{
        project_context_dataset_build_operation, project_registry_validation_operation,
        OperationReadError,
    },
    repository::{
        OrchestrationSignalAuthority, OrchestrationWakeMutationIds, PgRepository, RepositoryError,
        ResolveOrchestrationSignalTarget, ResolveOrchestrationTask,
        ResolveOrchestrationTaskMutationIds, WakeOrchestrationJob,
    },
    verify_schema,
};
use insight_platform_registry::{
    ActivateResource, CreateDeployment, CreateResourceDraft, NewPublishedVersion,
    PublishResourceVersions, RequestResourceValidation, SuspendResourceDeployment,
    UpdateResourceDraft,
};
use insight_platform_tasks::{TaskDefinition, TaskPayload, TaskState};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use sqlx::postgres::PgPoolOptions;
use std::{
    error::Error,
    fmt,
    fs::File,
    io::Read as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

const CONFIG_PATH_ENV: &str = "PLATFORM_GATEWAY_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_GATEWAY_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_GATEWAY_DATABASE_URL";
const RUN_EVENT_CURSOR_KEY_PATH_ENV: &str = "PLATFORM_GATEWAY_RUN_EVENT_CURSOR_KEY_PATH";
const RUN_EVENT_CURSOR_KEY_DIGEST_ENV: &str = "PLATFORM_GATEWAY_RUN_EVENT_CURSOR_KEY_DIGEST";
const ARTIFACT_GATEWAY_ENDPOINT_ENV: &str = "PLATFORM_GATEWAY_ARTIFACT_ENDPOINT";
const ARTIFACT_GATEWAY_CA_PATH_ENV: &str = "PLATFORM_GATEWAY_ARTIFACT_CA_PATH";
const ARTIFACT_GATEWAY_CERT_PATH_ENV: &str = "PLATFORM_GATEWAY_ARTIFACT_CERT_PATH";
const ARTIFACT_GATEWAY_KEY_PATH_ENV: &str = "PLATFORM_GATEWAY_ARTIFACT_KEY_PATH";
const ARTIFACT_GATEWAY_TLS_AUDIENCE: &str =
    "insight-platform-artifact-gateway.platform-artifacts.svc";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_RUN_EVENT_CURSOR_KEY_BYTES: usize = 64;
const MAX_TLS_FILE_BYTES: usize = 1_048_576;
const MAX_ARTIFACT_RESPONSE_BYTES: usize = 262_144;
const GATEWAY_HTTP_OPERATIONS: &[&str] = &[
    "live",
    "ready",
    "metrics",
    "resources",
    "runs",
    "tasks",
    "artifacts",
    "operations",
    "mcp",
    "other",
];

fn gateway_operation(path: &str) -> &'static str {
    match path {
        "/livez" => "live",
        "/readyz" => "ready",
        "/metrics" => "metrics",
        _ => match path
            .strip_prefix("/v1/")
            .and_then(|suffix| suffix.split('/').next())
        {
            Some("resources") => "resources",
            Some("runs") => "runs",
            Some("tasks") => "tasks",
            Some("artifacts") => "artifacts",
            Some("operations") => "operations",
            Some("mcp-bindings" | "mcp") => "mcp",
            _ => "other",
        },
    }
}

#[cfg(test)]
fn install_gateway_metrics(role: ProcessRole) -> Arc<ProcessHttpMetrics> {
    Arc::new(
        ProcessHttpMetrics::install(role.component_role(), GATEWAY_HTTP_OPERATIONS)
            .expect("static Gateway metric labels are valid"),
    )
}

struct PostgresPoolCapacity {
    pool: sqlx::PgPool,
    maximum_connections: u32,
}

impl OperationalCapacitySource for PostgresPoolCapacity {
    fn snapshot(&self) -> OperationalCapacitySnapshot {
        let capacity = u64::from(self.maximum_connections);
        let established = u64::from(self.pool.size());
        let idle = u64::try_from(self.pool.num_idle()).unwrap_or(u64::MAX);
        let used = established.saturating_sub(idle).min(capacity);
        OperationalCapacitySnapshot::new(capacity, capacity.saturating_sub(used))
            .expect("Gateway PostgreSQL pool preserves its configured maximum")
    }
}

#[cfg(test)]
fn install_gateway_metrics_with_postgres(
    role: ProcessRole,
    pool: sqlx::PgPool,
    maximum_connections: u32,
) -> Arc<ProcessHttpMetrics> {
    install_gateway_metrics_with_postgres_and_dependencies(role, pool, maximum_connections, None)
}

fn install_gateway_metrics_with_postgres_and_dependencies(
    role: ProcessRole,
    pool: sqlx::PgPool,
    maximum_connections: u32,
    dependencies: Option<Arc<DependencyObservationMetrics>>,
) -> Arc<ProcessHttpMetrics> {
    let source: Arc<dyn OperationalCapacitySource> = Arc::new(PostgresPoolCapacity {
        pool,
        maximum_connections,
    });
    let metrics = ProcessHttpMetrics::install_with_capacities(
        role.component_role(),
        GATEWAY_HTTP_OPERATIONS,
        vec![OperationalCapacityMetric::new(
            "postgresql_connections",
            source,
        )],
    )
    .expect("static Gateway capacity metric labels are valid");
    Arc::new(match dependencies {
        Some(dependencies) => metrics.with_dependency_observations(dependencies),
        None => metrics,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProcessRole {
    ManagementApi,
    RuntimeApi,
}

impl ProcessRole {
    const fn component_role(self) -> &'static str {
        match self {
            Self::ManagementApi => "management-api",
            Self::RuntimeApi => "runtime-api",
        }
    }

    fn permits_path(self, path: &str) -> bool {
        if matches!(path, "/livez" | "/readyz" | "/metrics") {
            return true;
        }
        let Some(noun) = path
            .strip_prefix("/v1/")
            .and_then(|suffix| suffix.split('/').next())
        else {
            return false;
        };
        noun == "operations"
            || match self {
                Self::ManagementApi => matches!(
                    noun,
                    "agents"
                        | "skills"
                        | "capabilities"
                        | "contexts"
                        | "context-datasets"
                        | "models"
                        | "mcp-servers"
                        | "policies"
                        | "sandboxes"
                ),
                Self::RuntimeApi => matches!(noun, "runs" | "tasks" | "artifacts"),
            }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    role: ProcessRole,
    listen_address: String,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    shutdown_grace_milliseconds: u64,
    registry_validator_digest: Sha256Digest,
    registry_validation_profile_digest: Sha256Digest,
    oidc: InstalledOidcVerifierConfig,
}

impl ProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded_file(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 16,
                max_items_per_array: 16,
                max_properties_per_object: 32,
                max_string_bytes: 524_288,
            },
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        let expected: Sha256Digest = required(CONFIG_DIGEST_ENV)?
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let actual: Sha256Digest = canonical_digest(&value)
            .map_err(|_| ProcessError::InvalidConfiguration)?
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if actual != expected {
            return Err(ProcessError::InvalidConfiguration);
        }
        let config: Self =
            serde_json::from_value(value).map_err(|_| ProcessError::InvalidConfiguration)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ProcessError> {
        let listen: SocketAddr = self
            .listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || listen.port() == 0
            || !(2..=64).contains(&self.database_max_connections)
            || self.database_acquire_timeout_milliseconds == 0
            || self.database_acquire_timeout_milliseconds > 30_000
            || self.shutdown_grace_milliseconds == 0
            || self.shutdown_grace_milliseconds > 60_000
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct PgPrincipalBindings(Arc<PgRepository>);

#[async_trait]
impl ExternalPrincipalBindingAuthority for PgPrincipalBindings {
    async fn resolve_external_principal(
        &self,
        tenant_id: ResourceId,
        authentication_authority_digest: Sha256Digest,
        subject_digest: Sha256Digest,
        asserted_principal_kind: PrincipalKind,
    ) -> Result<PrincipalSnapshot, AuthenticationError> {
        self.0
            .resolve_external_principal(
                tenant_id,
                authentication_authority_digest,
                subject_digest,
                asserted_principal_kind,
            )
            .await
            .map_err(|error| match error {
                RepositoryError::InvalidInput(_)
                | RepositoryError::NotFound(_)
                | RepositoryError::PermissionDenied => AuthenticationError::Unauthenticated,
                _ => AuthenticationError::Unavailable,
            })
    }
}

#[derive(Clone)]
struct PgOperations(Arc<PgRepository>);

#[async_trait]
impl OperationApplication for PgOperations {
    async fn read_operation(
        &self,
        request: ReadOperation,
    ) -> Result<OperationViewV1, OperationApplicationError> {
        self.0
            .read_public_operation(&request)
            .await
            .map_err(|error| match error {
                OperationReadError::InvalidRequest => OperationApplicationError::Invalid,
                OperationReadError::Denied => OperationApplicationError::Denied,
                OperationReadError::NotFound => OperationApplicationError::NotFound,
                OperationReadError::NotPublic => OperationApplicationError::NotPublic,
                OperationReadError::AuthorityUnavailable => OperationApplicationError::Unavailable,
                OperationReadError::CorruptAuthority => OperationApplicationError::Internal,
            })
    }
}

#[derive(Clone)]
struct PgRuns(Arc<PgRepository>);

#[derive(Clone)]
struct PgArtifacts {
    mutation_forwarder: Arc<dyn ArtifactMutationForwarder>,
}

#[async_trait]
trait ArtifactMutationForwarder: Send + Sync {
    async fn prepare(
        &self,
        intent: PrepareArtifactUploadIntent,
    ) -> Result<PrepareArtifactUploadResponseV1, ArtifactApplicationError>;

    async fn complete(
        &self,
        intent: CompleteArtifactUploadIntent,
    ) -> Result<ArtifactMutationAcceptedV1, ArtifactApplicationError>;

    async fn delete(
        &self,
        intent: DeleteArtifactIntent,
    ) -> Result<ArtifactMutationAcceptedV1, ArtifactApplicationError>;

    async fn read(
        &self,
        intent: ReadArtifactIntent,
    ) -> Result<ArtifactViewV1, ArtifactApplicationError>;

    async fn read_content(
        &self,
        intent: ReadArtifactIntent,
    ) -> Result<ArtifactContentStreamV1, ArtifactApplicationError>;
}

struct ExactLengthResponseStream {
    inner: Pin<Box<dyn futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>,
    remaining: u64,
    finished: bool,
}

impl futures::Stream for ExactLengthResponseStream {
    type Item = Result<bytes::Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_next(context) {
            Poll::Ready(Some(Ok(bytes))) => {
                let Ok(length) = u64::try_from(bytes.len()) else {
                    self.finished = true;
                    return Poll::Ready(Some(Err(std::io::Error::other(
                        "Artifact content length overflow",
                    ))));
                };
                if length > self.remaining {
                    self.finished = true;
                    return Poll::Ready(Some(Err(std::io::Error::other(
                        "Artifact content exceeded its verified length",
                    ))));
                }
                self.remaining -= length;
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(_))) => {
                self.finished = true;
                Poll::Ready(Some(Err(std::io::Error::other(
                    "Artifact content stream failed",
                ))))
            }
            Poll::Ready(None) if self.remaining != 0 => {
                self.finished = true;
                Poll::Ready(Some(Err(std::io::Error::other(
                    "Artifact content ended before its verified length",
                ))))
            }
            Poll::Ready(None) => {
                self.finished = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Clone)]
struct MtlsArtifactMutationForwarder {
    client: reqwest::Client,
    endpoint: String,
}

impl MtlsArtifactMutationForwarder {
    fn install() -> Result<Self, ProcessError> {
        let endpoint = required(ARTIFACT_GATEWAY_ENDPOINT_ENV)?;
        let parsed =
            reqwest::Url::parse(&endpoint).map_err(|_| ProcessError::InvalidConfiguration)?;
        if parsed.scheme() != "https"
            || parsed.host_str() != Some(ARTIFACT_GATEWAY_TLS_AUDIENCE)
            || parsed.port() != Some(8080)
            || parsed.username() != ""
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.path() != "/"
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        let ca = read_bounded_file(
            &required_absolute_path(ARTIFACT_GATEWAY_CA_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?;
        let certificate = read_bounded_file(
            &required_absolute_path(ARTIFACT_GATEWAY_CERT_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?;
        let key = read_bounded_file(
            &required_absolute_path(ARTIFACT_GATEWAY_KEY_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?;
        let mut identity = certificate;
        identity.extend_from_slice(&key);
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .add_root_certificate(
                reqwest::Certificate::from_pem(&ca)
                    .map_err(|_| ProcessError::InvalidConfiguration)?,
            )
            .identity(
                reqwest::Identity::from_pem(&identity)
                    .map_err(|_| ProcessError::InvalidConfiguration)?,
            )
            .build()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        Ok(Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_owned(),
        })
    }

    fn request(
        &self,
        path: &str,
        principal: &insight_platform_api::authentication::AuthenticatedPrincipal,
        idempotency_key_digest: &Sha256Digest,
    ) -> reqwest::RequestBuilder {
        self.client
            .post(format!("{}{path}", self.endpoint))
            .header(
                "x-insight-verified-tenant-id",
                principal.tenant_id.to_string(),
            )
            .header(
                "x-insight-verified-principal-id",
                principal.principal_id.to_string(),
            )
            .header(
                "x-insight-verified-principal-kind",
                principal.principal_kind.as_str(),
            )
            .header(
                "x-insight-idempotency-key-digest",
                idempotency_key_digest.as_str(),
            )
    }

    fn read_request(
        &self,
        path: &str,
        principal: &insight_platform_api::authentication::AuthenticatedPrincipal,
    ) -> reqwest::RequestBuilder {
        self.client
            .get(format!("{}{path}", self.endpoint))
            .header(
                "x-insight-verified-tenant-id",
                principal.tenant_id.to_string(),
            )
            .header(
                "x-insight-verified-principal-id",
                principal.principal_id.to_string(),
            )
            .header(
                "x-insight-verified-principal-kind",
                principal.principal_kind.as_str(),
            )
    }
}

#[async_trait]
impl ArtifactMutationForwarder for MtlsArtifactMutationForwarder {
    async fn prepare(
        &self,
        intent: PrepareArtifactUploadIntent,
    ) -> Result<PrepareArtifactUploadResponseV1, ArtifactApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(ArtifactApplicationError::Unavailable);
        }
        let response = self
            .request(
                "/v1/artifacts:prepare-upload",
                &intent.principal,
                &intent.idempotency_key_digest,
            )
            .json(&intent.request)
            .send()
            .await
            .map_err(|_| ArtifactApplicationError::Unavailable)?;
        decode_artifact_response(response).await
    }

    async fn complete(
        &self,
        intent: CompleteArtifactUploadIntent,
    ) -> Result<ArtifactMutationAcceptedV1, ArtifactApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(ArtifactApplicationError::Unavailable);
        }
        let response = self
            .request(
                &format!("/v1/artifacts/{}:complete-upload", intent.artifact_id),
                &intent.principal,
                &intent.idempotency_key_digest,
            )
            .header(
                "x-insight-artifact-expected-version",
                intent.expected_artifact_version,
            )
            .json(&intent.request)
            .send()
            .await
            .map_err(|_| ArtifactApplicationError::Unavailable)?;
        decode_artifact_response(response).await
    }

    async fn delete(
        &self,
        intent: DeleteArtifactIntent,
    ) -> Result<ArtifactMutationAcceptedV1, ArtifactApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(ArtifactApplicationError::Unavailable);
        }
        let response = self
            .request(
                &format!("/v1/artifacts/{}:delete", intent.artifact_id),
                &intent.principal,
                &intent.idempotency_key_digest,
            )
            .header(
                "x-insight-artifact-expected-version",
                intent.expected_artifact_version,
            )
            .body(Vec::new())
            .send()
            .await
            .map_err(|_| ArtifactApplicationError::Unavailable)?;
        decode_artifact_response(response).await
    }

    async fn read(
        &self,
        intent: ReadArtifactIntent,
    ) -> Result<ArtifactViewV1, ArtifactApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(ArtifactApplicationError::Unavailable);
        }
        let response = self
            .read_request(
                &format!("/v1/artifacts/{}", intent.artifact_id),
                &intent.principal,
            )
            .send()
            .await
            .map_err(|_| ArtifactApplicationError::Unavailable)?;
        decode_artifact_response(response).await
    }

    async fn read_content(
        &self,
        intent: ReadArtifactIntent,
    ) -> Result<ArtifactContentStreamV1, ArtifactApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(ArtifactApplicationError::Unavailable);
        }
        let response = self
            .read_request(
                &format!("/v1/artifacts/{}/content", intent.artifact_id),
                &intent.principal,
            )
            .send()
            .await
            .map_err(|_| ArtifactApplicationError::Unavailable)?;
        if !response.status().is_success() {
            return Err(map_artifact_status(response.status()));
        }
        let content_length = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| (1..=MAX_ARTIFACT_BYTES).contains(value))
            .ok_or(ArtifactApplicationError::Internal)?;
        let media_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty() && value.len() <= 255)
            .ok_or(ArtifactApplicationError::Internal)?
            .to_owned();
        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty() && value.len() <= 128)
            .ok_or(ArtifactApplicationError::Internal)?
            .to_owned();
        if response
            .headers()
            .get(reqwest::header::CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok())
            != Some("attachment")
        {
            return Err(ArtifactApplicationError::Internal);
        }
        Ok(ArtifactContentStreamV1 {
            stream: Box::pin(ExactLengthResponseStream {
                inner: Box::pin(response.bytes_stream()),
                remaining: content_length,
                finished: false,
            }),
            content_length,
            media_type,
            etag,
        })
    }
}

fn map_artifact_status(status: reqwest::StatusCode) -> ArtifactApplicationError {
    match status {
        reqwest::StatusCode::BAD_REQUEST => ArtifactApplicationError::Invalid,
        reqwest::StatusCode::UNAUTHORIZED => ArtifactApplicationError::Unauthenticated,
        reqwest::StatusCode::FORBIDDEN => ArtifactApplicationError::Denied,
        reqwest::StatusCode::NOT_FOUND => ArtifactApplicationError::NotFound,
        reqwest::StatusCode::CONFLICT => ArtifactApplicationError::IdempotencyConflict,
        reqwest::StatusCode::PRECONDITION_FAILED => ArtifactApplicationError::Conflict,
        reqwest::StatusCode::PAYLOAD_TOO_LARGE => ArtifactApplicationError::TooLarge,
        _ if status.is_server_error() => ArtifactApplicationError::Unavailable,
        _ => ArtifactApplicationError::Internal,
    }
}

async fn decode_artifact_response<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, ArtifactApplicationError> {
    let status = response.status();
    if !status.is_success() {
        return Err(map_artifact_status(status));
    }
    let length = response.content_length().unwrap_or(0);
    if length > MAX_ARTIFACT_RESPONSE_BYTES as u64 {
        return Err(ArtifactApplicationError::Internal);
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|_| ArtifactApplicationError::Unavailable)?;
    if bytes.len() > MAX_ARTIFACT_RESPONSE_BYTES {
        return Err(ArtifactApplicationError::Internal);
    }
    serde_json::from_slice(&bytes).map_err(|_| ArtifactApplicationError::Internal)
}

#[async_trait]
impl ArtifactApplication for PgArtifacts {
    async fn prepare_artifact_upload(
        &self,
        intent: PrepareArtifactUploadIntent,
    ) -> Result<PrepareArtifactUploadResponseV1, ArtifactApplicationError> {
        self.mutation_forwarder.prepare(intent).await
    }

    async fn complete_artifact_upload(
        &self,
        intent: CompleteArtifactUploadIntent,
    ) -> Result<ArtifactMutationAcceptedV1, ArtifactApplicationError> {
        self.mutation_forwarder.complete(intent).await
    }

    async fn delete_artifact(
        &self,
        intent: DeleteArtifactIntent,
    ) -> Result<ArtifactMutationAcceptedV1, ArtifactApplicationError> {
        self.mutation_forwarder.delete(intent).await
    }

    async fn read_artifact(
        &self,
        intent: ReadArtifactIntent,
    ) -> Result<ArtifactViewV1, ArtifactApplicationError> {
        self.mutation_forwarder.read(intent).await
    }

    async fn read_artifact_content(
        &self,
        intent: ReadArtifactIntent,
    ) -> Result<ArtifactContentStreamV1, ArtifactApplicationError> {
        self.mutation_forwarder.read_content(intent).await
    }
}

#[derive(Clone)]
struct PgTasks(Arc<PgRepository>);

#[async_trait]
impl TaskApplication for PgTasks {
    async fn read_task(&self, intent: ReadTaskIntent) -> Result<TaskViewV1, TaskApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(TaskApplicationError::Unavailable);
        }
        let record = self
            .0
            .read_task_for_principal(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                &intent.task_id,
            )
            .await
            .map_err(map_task_repository_error)?;
        task_view_from_record(record)
    }

    async fn resolve_task(
        &self,
        intent: ResolveTaskIntent,
    ) -> Result<TaskViewV1, TaskApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(TaskApplicationError::Unavailable);
        }
        let current = self
            .0
            .read_task_for_principal(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                &intent.task_id,
            )
            .await
            .map_err(map_task_repository_error)?;
        if intent.task_id.kind() == ResourceKind::ApprovalTask {
            if current.owner_kind == "artifact" {
                let artifact_id: ResourceId = current
                    .owner_id
                    .parse()
                    .map_err(|_| TaskApplicationError::Internal)?;
                let payload = decode_task_payload(&current)?;
                let owner_version = match payload.definition {
                    TaskDefinition::Approval { owner_version, .. } => owner_version,
                    _ => return Err(TaskApplicationError::Internal),
                };
                let operation_id = self
                    .0
                    .find_artifact_deletion_operation(
                        &intent.principal.tenant_id,
                        &artifact_id,
                        &intent.task_id,
                    )
                    .await
                    .map_err(map_task_repository_error)?;
                let decision = match intent.action {
                    TaskActionV1::Approve => ArtifactDeletionApprovalDecision::Approve,
                    TaskActionV1::Reject => ArtifactDeletionApprovalDecision::Reject,
                    TaskActionV1::Cancel => ArtifactDeletionApprovalDecision::Cancel,
                    TaskActionV1::SubmitInput => return Err(TaskApplicationError::Invalid),
                };
                let principal = intent.principal.clone();
                let task_id = intent.task_id.clone();
                self.0
                    .resolve_artifact_deletion_approval(ResolveArtifactDeletionApproval {
                        audit: task_command_audit(&intent)?,
                        artifact_id,
                        operation_id,
                        approval_task_id: intent.task_id,
                        expected_artifact_version: owner_version,
                        expected_task_generation: u64::try_from(current.generation)
                            .map_err(|_| TaskApplicationError::Internal)?,
                        expected_task_version: intent.expected_task_version,
                        decision,
                    })
                    .await
                    .map_err(map_task_repository_error)?;
                return self
                    .0
                    .read_task_for_principal(
                        &principal.tenant_id,
                        &principal.principal_id,
                        principal.principal_kind,
                        &task_id,
                    )
                    .await
                    .map_err(map_task_repository_error)
                    .and_then(task_view_from_record);
            }
            let decision = match intent.action {
                TaskActionV1::Approve => CapabilityApprovalDecision::Approve,
                TaskActionV1::Reject => CapabilityApprovalDecision::Reject,
                TaskActionV1::Cancel => CapabilityApprovalDecision::Cancel,
                TaskActionV1::SubmitInput => return Err(TaskApplicationError::Invalid),
            };
            let payload = decode_task_payload(&current)?;
            let (expected_invocation_version, eligible_principal_rule_digest) =
                match payload.definition {
                    TaskDefinition::Approval {
                        owner_version,
                        approver_rule_digest,
                        ..
                    } => (owner_version, approver_rule_digest),
                    _ => return Err(TaskApplicationError::Internal),
                };
            let invocation_id: ResourceId = current
                .invocation_id
                .as_deref()
                .ok_or(TaskApplicationError::Internal)?
                .parse()
                .map_err(|_| TaskApplicationError::Internal)?;
            let principal = intent.principal.clone();
            let task_id = intent.task_id.clone();
            let command = ResolveCapabilityApproval {
                audit: task_command_audit(&intent)?,
                invocation_id,
                approval_task_id: intent.task_id,
                expected_invocation_version,
                expected_task_generation: u64::try_from(current.generation)
                    .map_err(|_| TaskApplicationError::Internal)?,
                expected_task_version: intent.expected_task_version,
                eligible_principal_rule_digest,
                decision,
                dispatch_mutations: (decision == CapabilityApprovalDecision::Approve)
                    .then(|| {
                        Ok(CapabilityApprovalDispatchMutationIds {
                            receipt_id: new_task_id(ResourceKind::Receipt)?,
                            event_id: new_task_id(ResourceKind::Event)?,
                            outbox_id: new_task_id(ResourceKind::OutboxEvent)?,
                        })
                    })
                    .transpose()?,
                failure_mutations: (decision != CapabilityApprovalDecision::Approve)
                    .then(|| {
                        Ok(ExternalLeafFailureMutationIds {
                            convergence_job_id: new_task_id(ResourceKind::Job)?,
                            run_event_id: new_task_id(ResourceKind::Event)?,
                            run_outbox_id: new_task_id(ResourceKind::OutboxEvent)?,
                            leaf_node_event_id: new_task_id(ResourceKind::Event)?,
                            leaf_node_outbox_id: new_task_id(ResourceKind::OutboxEvent)?,
                            convergence_job_event_id: new_task_id(ResourceKind::Event)?,
                            convergence_job_outbox_id: new_task_id(ResourceKind::OutboxEvent)?,
                        })
                    })
                    .transpose()?,
            };
            let mut transaction = self
                .0
                .begin_invocation_transaction()
                .await
                .map_err(map_task_repository_error)?;
            transaction
                .resolve_capability_approval(command)
                .await
                .map_err(map_task_repository_error)?;
            transaction
                .commit()
                .await
                .map_err(map_task_repository_error)?;
            let record = self
                .0
                .read_task_for_principal(
                    &principal.tenant_id,
                    &principal.principal_id,
                    principal.principal_kind,
                    &task_id,
                )
                .await
                .map_err(map_task_repository_error)?;
            return task_view_from_record(record);
        }
        if intent.task_id.kind() != ResourceKind::Interaction
            || matches!(intent.action, TaskActionV1::Approve)
        {
            return Err(TaskApplicationError::Invalid);
        }
        let target = match intent.action {
            TaskActionV1::SubmitInput => TaskState::Responded,
            TaskActionV1::Reject => TaskState::Declined,
            TaskActionV1::Cancel => TaskState::Cancelled,
            TaskActionV1::Approve => return Err(TaskApplicationError::Invalid),
        };
        let audit = task_command_audit(&intent)?;
        let response = intent
            .input
            .map(|input| -> Result<RunInputValue, TaskApplicationError> {
                let content_digest = match &input.value {
                    ValueRef::Inline { value } => canonical_digest(value)
                        .map_err(|_| TaskApplicationError::Invalid)?
                        .parse()
                        .map_err(|_| TaskApplicationError::Internal)?,
                    ValueRef::Artifact { artifact }
                        if artifact.classification() == input.classification =>
                    {
                        artifact.content_digest().clone()
                    }
                    ValueRef::Artifact { .. } => return Err(TaskApplicationError::Invalid),
                };
                Ok(RunInputValue {
                    value_id: new_task_id(ResourceKind::RunValue)?,
                    classification: input.classification,
                    schema_digest: input.schema_digest,
                    content_digest,
                    value: input.value,
                })
            })
            .transpose()?;
        let command = ResolveOrchestrationTask {
            audit,
            task_id: intent.task_id,
            expected_generation: u64::try_from(current.generation)
                .map_err(|_| TaskApplicationError::Internal)?,
            expected_task_version: intent.expected_task_version,
            target,
            response,
            resume_job_id: new_task_id(ResourceKind::Job)?,
            resume_request_digest: intent.request_digest,
            mutations: ResolveOrchestrationTaskMutationIds {
                run_event_id: new_task_id(ResourceKind::Event)?,
                run_outbox_id: new_task_id(ResourceKind::OutboxEvent)?,
                node_event_id: new_task_id(ResourceKind::Event)?,
                node_outbox_id: new_task_id(ResourceKind::OutboxEvent)?,
                job_event_id: new_task_id(ResourceKind::Event)?,
                job_outbox_id: new_task_id(ResourceKind::OutboxEvent)?,
            },
        };
        let mut transaction = self
            .0
            .begin_scheduler_transaction()
            .await
            .map_err(map_task_repository_error)?;
        let outcome = transaction
            .resolve_orchestration_task(command)
            .await
            .map_err(map_task_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_task_repository_error)?;
        task_view_from_record(match outcome {
            insight_platform_contracts::CommandOutcome::Applied(resolved)
            | insight_platform_contracts::CommandOutcome::Replayed(resolved) => resolved.task,
        })
    }
}

fn new_task_id(kind: ResourceKind) -> Result<ResourceId, TaskApplicationError> {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).map_err(|_| TaskApplicationError::Internal)
}

fn task_command_audit(intent: &ResolveTaskIntent) -> Result<CommandAudit, TaskApplicationError> {
    Ok(CommandAudit {
        trace: intent.principal.trace,
        tenant_id: intent.principal.tenant_id.clone(),
        principal_id: intent.principal.principal_id.clone(),
        principal_kind: intent.principal.principal_kind,
        receipt_id: new_task_id(ResourceKind::Receipt)?,
        event_id: new_task_id(ResourceKind::Event)?,
        outbox_id: new_task_id(ResourceKind::OutboxEvent)?,
        idempotency_key_digest: intent.idempotency_key_digest.clone(),
        request_digest: intent.request_digest.clone(),
        receipt_expires_at: chrono::Utc::now() + chrono::Duration::hours(24),
    })
}

fn task_view_from_record(
    record: insight_platform_postgres::repository::TaskRecord,
) -> Result<TaskViewV1, TaskApplicationError> {
    let mut payload = record.payload.value;
    payload
        .as_object_mut()
        .ok_or(TaskApplicationError::Internal)?
        .remove("schema_version");
    let payload: TaskPayload =
        serde_json::from_value(payload).map_err(|_| TaskApplicationError::Internal)?;
    let safe_prompt_key = match payload.definition {
        TaskDefinition::Approval {
            safe_prompt_key, ..
        }
        | TaskDefinition::Interaction {
            safe_prompt_key, ..
        }
        | TaskDefinition::CapabilityInput {
            safe_prompt_key, ..
        }
        | TaskDefinition::McpOAuthAuthorization {
            safe_prompt_key, ..
        }
        | TaskDefinition::HumanWork {
            safe_prompt_key, ..
        } => safe_prompt_key,
    };
    let task_id: ResourceId = record
        .task_id
        .parse()
        .map_err(|_| TaskApplicationError::Internal)?;
    let owner = if let Some(invocation_id) = record.invocation_id {
        TaskOwnerLinkV1::Invocation {
            invocation_id: invocation_id
                .parse()
                .map_err(|_| TaskApplicationError::Internal)?,
        }
    } else if let Some(run_id) = record.run_id {
        TaskOwnerLinkV1::Run {
            run_id: run_id.parse().map_err(|_| TaskApplicationError::Internal)?,
        }
    } else if record.owner_kind == "artifact" {
        TaskOwnerLinkV1::Artifact {
            artifact_id: record
                .owner_id
                .parse()
                .map_err(|_| TaskApplicationError::Internal)?,
        }
    } else {
        return Err(TaskApplicationError::Internal);
    };
    let version = u64::try_from(record.version).map_err(|_| TaskApplicationError::Internal)?;
    Ok(TaskViewV1 {
        schema_version: 1,
        task_id: task_id.clone(),
        task_kind: record.task_kind,
        state: record.state,
        generation: u64::try_from(record.generation).map_err(|_| TaskApplicationError::Internal)?,
        version,
        safe_prompt_key,
        response_schema_digest: record
            .response_schema_digest
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| TaskApplicationError::Internal)?,
        owner,
        deadline: insight_platform_contracts::UtcTimestamp::from_datetime(record.deadline),
        responded_at: record
            .responded_at
            .map(insight_platform_contracts::UtcTimestamp::from_datetime),
        created_at: insight_platform_contracts::UtcTimestamp::from_datetime(record.created_at),
        updated_at: insight_platform_contracts::UtcTimestamp::from_datetime(record.updated_at),
        etag: task_etag(&task_id, version),
    })
}

fn decode_task_payload(
    record: &insight_platform_postgres::repository::TaskRecord,
) -> Result<TaskPayload, TaskApplicationError> {
    let mut payload = record.payload.value.clone();
    payload
        .as_object_mut()
        .ok_or(TaskApplicationError::Internal)?
        .remove("schema_version");
    serde_json::from_value(payload).map_err(|_| TaskApplicationError::Internal)
}

#[async_trait]
impl RunApplication for PgRuns {
    async fn create_run(&self, intent: CreateRunIntent) -> Result<RunViewV1, RunApplicationError> {
        let now = chrono::Utc::now();
        if intent.deadline <= now {
            return Err(RunApplicationError::Unavailable);
        }
        if let Some(record) = self
            .0
            .read_root_run_admission_replay(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                &intent.request.agent_id,
                &intent.idempotency_key_digest,
                &intent.request_digest,
            )
            .await
            .map_err(map_run_repository_error)?
        {
            return run_view_from_record(record);
        }
        let target = self
            .0
            .resolve_root_run_target(&intent.principal.tenant_id, &intent.request.agent_id)
            .await
            .map_err(map_run_repository_error)?;
        let principal = PrincipalSnapshot::build(
            intent.principal.tenant_id.clone(),
            intent.principal.principal_id.clone(),
            intent.principal.principal_kind,
            intent.principal.permissions.clone(),
            intent.principal.principal_version,
            intent.principal.binding_generation,
            intent.principal.binding_version,
        )
        .map_err(|_| RunApplicationError::Internal)?;
        let bindings = RunBindingsSnapshot::build_with_context_dataset_views(
            target.agent.clone(),
            principal,
            &target.closure,
            target.context_dataset_views,
        )
        .map_err(|_| RunApplicationError::Internal)?;
        let requested_deadline =
            chrono::DateTime::parse_from_rfc3339(intent.request.deadline.as_str())
                .map_err(|_| RunApplicationError::Invalid)?
                .with_timezone(&chrono::Utc);
        let content_digest = match &intent.request.input.value {
            ValueRef::Inline { value } => canonical_digest(value)
                .map_err(|_| RunApplicationError::Invalid)?
                .parse()
                .map_err(|_| RunApplicationError::Internal)?,
            ValueRef::Artifact { artifact }
                if artifact.classification() == intent.request.input.classification =>
            {
                artifact.content_digest().clone()
            }
            ValueRef::Artifact { .. } => return Err(RunApplicationError::Invalid),
        };
        let make_id = |kind| {
            ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7())
                .map_err(|_| RunApplicationError::Internal)
        };
        let audit = CommandAudit {
            trace: intent.principal.trace,
            tenant_id: intent.principal.tenant_id,
            principal_id: intent.principal.principal_id,
            principal_kind: intent.principal.principal_kind,
            receipt_id: make_id(ResourceKind::Receipt)?,
            event_id: make_id(ResourceKind::Event)?,
            outbox_id: make_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: intent.idempotency_key_digest,
            request_digest: intent.request_digest,
            receipt_expires_at: now + chrono::Duration::hours(24),
        };
        let run_id = make_id(ResourceKind::Run)?;
        let command = AdmitRun {
            audit,
            admission_scope_id: intent.request.agent_id,
            run_id,
            agent_deployment_id: target.agent.deployment_id,
            root_scope_id: make_id(ResourceKind::ScopeInstance)?,
            entry_node_execution_id: make_id(ResourceKind::NodeExecution)?,
            orchestration_job_id: make_id(ResourceKind::Job)?,
            entry_plan_node_key: PlanNodeKey::new(target.closure.entry_node_id)
                .map_err(|_| RunApplicationError::Internal)?,
            entry_node_kind: target.closure.entry_node_kind,
            bindings,
            input: RunInputValue {
                value_id: make_id(ResourceKind::RunValue)?,
                classification: intent.request.input.classification,
                schema_digest: intent.request.input.schema_digest,
                content_digest,
                value: intent.request.input.value,
            },
            deadline: requested_deadline,
            inline_limits: JsonLimits::CONTRACT_FIXTURE,
            attempt_limit: 3,
            retry_backoff_milliseconds: 100,
        };
        let mut transaction = self
            .0
            .begin_run_transaction()
            .await
            .map_err(map_run_repository_error)?;
        let outcome = transaction
            .admit_run(command)
            .await
            .map_err(map_run_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_run_repository_error)?;
        let record = match outcome {
            insight_platform_contracts::CommandOutcome::Applied(record)
            | insight_platform_contracts::CommandOutcome::Replayed(record) => record,
        };
        run_view_from_record(record)
    }

    async fn pause_run(&self, intent: ControlRunIntent) -> Result<RunViewV1, RunApplicationError> {
        self.set_pause(intent, true).await
    }

    async fn resume_run(&self, intent: ControlRunIntent) -> Result<RunViewV1, RunApplicationError> {
        self.set_pause(intent, false).await
    }

    async fn cancel_run(&self, intent: ControlRunIntent) -> Result<RunViewV1, RunApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(RunApplicationError::Unavailable);
        }
        let current = self.read_control_target(&intent).await?;
        let command = RequestRunCancel {
            audit: run_control_audit(&intent)?,
            run_id: intent.run_id,
            expected_run_version: i64::try_from(intent.expected_run_version)
                .map_err(|_| RunApplicationError::Invalid)?,
            expected_cancel_generation: u64::try_from(current.cancel_generation)
                .map_err(|_| RunApplicationError::Internal)?,
            reason_code: "operator_request".to_owned(),
        };
        let mut transaction = self
            .0
            .begin_run_transaction()
            .await
            .map_err(map_run_repository_error)?;
        let outcome = transaction
            .request_run_cancel(command)
            .await
            .map_err(map_run_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_run_repository_error)?;
        run_view_from_record(match outcome {
            insight_platform_contracts::CommandOutcome::Applied(record)
            | insight_platform_contracts::CommandOutcome::Replayed(record) => record,
        })
    }

    async fn signal_run(&self, intent: SignalRunIntent) -> Result<(), RunApplicationError> {
        let now = chrono::Utc::now();
        if intent.deadline <= now {
            return Err(RunApplicationError::Unavailable);
        }
        let target = self
            .0
            .resolve_signal_wake_target_for_principal(&ResolveOrchestrationSignalTarget {
                tenant_id: intent.principal.tenant_id.clone(),
                principal_id: intent.principal.principal_id.clone(),
                principal_kind: intent.principal.principal_kind,
                run_id: intent.run_id.clone(),
                signal_key: intent.signal_key.clone(),
                idempotency_key_digest: intent.idempotency_key_digest.clone(),
                request_digest: intent.request_digest.clone(),
            })
            .await
            .map_err(map_run_repository_error)?;
        let make_id = |kind| {
            ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7())
                .map_err(|_| RunApplicationError::Internal)
        };
        let signal_payload = intent
            .request
            .payload
            .map(|payload| {
                let content_digest = match &payload.value {
                    ValueRef::Inline { value } => canonical_digest(value)
                        .map_err(|_| RunApplicationError::Invalid)?
                        .parse()
                        .map_err(|_| RunApplicationError::Internal)?,
                    ValueRef::Artifact { artifact }
                        if artifact.classification() == payload.classification =>
                    {
                        artifact.content_digest().clone()
                    }
                    ValueRef::Artifact { .. } => return Err(RunApplicationError::Invalid),
                };
                Ok(RunInputValue {
                    value_id: make_id(ResourceKind::RunValue)?,
                    classification: payload.classification,
                    schema_digest: payload.schema_digest,
                    content_digest,
                    value: payload.value,
                })
            })
            .transpose()?;
        let signal_authority = OrchestrationSignalAuthority {
            tenant_id: intent.principal.tenant_id.clone(),
            principal_id: intent.principal.principal_id,
            principal_kind: intent.principal.principal_kind,
            run_id: intent.run_id,
            idempotency_key_digest: intent.idempotency_key_digest.clone(),
            request_digest: intent.request_digest.clone(),
        };
        let mut transaction = self
            .0
            .begin_scheduler_transaction()
            .await
            .map_err(map_run_repository_error)?;
        transaction
            .wake_orchestration_job(WakeOrchestrationJob {
                tenant_id: intent.principal.tenant_id,
                job_id: target.job_id,
                expected_job_version: target.job_version,
                expected_wake_generation: target.wake_generation,
                source: WakeSource::Signal,
                signal_key: Some(intent.signal_key),
                signal_payload,
                idempotency_key_digest: intent.idempotency_key_digest,
                request_digest: intent.request_digest,
                receipt_expires_at: now + chrono::Duration::hours(24),
                signal_authority: Some(signal_authority),
                mutations: OrchestrationWakeMutationIds {
                    receipt_id: make_id(ResourceKind::Receipt)?,
                    node_event_id: make_id(ResourceKind::Event)?,
                    node_outbox_id: make_id(ResourceKind::OutboxEvent)?,
                    job_event_id: make_id(ResourceKind::Event)?,
                    job_outbox_id: make_id(ResourceKind::OutboxEvent)?,
                },
            })
            .await
            .map_err(map_run_repository_error)?;
        transaction.commit().await.map_err(map_run_repository_error)
    }

    async fn read_run(&self, intent: ReadRunIntent) -> Result<RunViewV1, RunApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(RunApplicationError::Unavailable);
        }
        let record = self
            .0
            .read_run_for_principal(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                &intent.run_id,
            )
            .await
            .map_err(map_run_repository_error)?;
        run_view_from_record(record)
    }

    async fn read_run_result(
        &self,
        intent: ReadRunIntent,
    ) -> Result<RunResultViewV1, RunApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(RunApplicationError::Unavailable);
        }
        let result = self
            .0
            .read_run_result_for_principal(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                &intent.run_id,
            )
            .await
            .map_err(|error| match error {
                RepositoryError::Conflict("run result is not terminal") => {
                    RunApplicationError::NotTerminal
                }
                other => map_run_repository_error(other),
            })?;
        Ok(RunResultViewV1 {
            schema_version: 1,
            run_id: result.run_id,
            value_id: result.value_id,
            classification: result.classification,
            schema_digest: result.schema_digest,
            content_digest: result.content_digest,
            value: result.value,
        })
    }

    async fn read_run_events(
        &self,
        intent: ReadRunEventsIntent,
    ) -> Result<Vec<RunEventProjectionV1>, RunApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(RunApplicationError::Unavailable);
        }
        self.0
            .read_public_run_events_for_principal(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                &intent.run_id,
                intent.after_sequence,
                intent.limit,
            )
            .await
            .map_err(map_run_repository_error)?
            .into_iter()
            .map(|event| {
                let source_kind = event
                    .event_type
                    .durable_source_kind()
                    .ok_or(RunApplicationError::Internal)?;
                Ok(RunEventProjectionV1 {
                    event_id: event.event_id,
                    trace_id: event.trace_id,
                    sequence: event.sequence,
                    event_type: event.event_type,
                    source_kind,
                    source_id: event.source_id,
                    source_projection_version: event.source_projection_version,
                    occurred_at: event.occurred_at,
                })
            })
            .collect()
    }
}

impl PgRuns {
    async fn read_control_target(
        &self,
        intent: &ControlRunIntent,
    ) -> Result<insight_platform_postgres::repository::RunRecord, RunApplicationError> {
        self.0
            .read_run_for_principal(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                &intent.run_id,
            )
            .await
            .map_err(map_run_repository_error)
    }

    async fn set_pause(
        &self,
        intent: ControlRunIntent,
        requested: bool,
    ) -> Result<RunViewV1, RunApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(RunApplicationError::Unavailable);
        }
        let current = self.read_control_target(&intent).await?;
        let command = SetRunPause {
            audit: run_control_audit(&intent)?,
            run_id: intent.run_id,
            expected_run_version: i64::try_from(intent.expected_run_version)
                .map_err(|_| RunApplicationError::Invalid)?,
            expected_pause_generation: u64::try_from(current.pause_generation)
                .map_err(|_| RunApplicationError::Internal)?,
            requested,
        };
        let mut transaction = self
            .0
            .begin_run_transaction()
            .await
            .map_err(map_run_repository_error)?;
        let outcome = transaction
            .set_run_pause(command)
            .await
            .map_err(map_run_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_run_repository_error)?;
        run_view_from_record(match outcome {
            insight_platform_contracts::CommandOutcome::Applied(record)
            | insight_platform_contracts::CommandOutcome::Replayed(record) => record,
        })
    }
}

fn run_control_audit(intent: &ControlRunIntent) -> Result<CommandAudit, RunApplicationError> {
    let make_id = |kind| {
        ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7())
            .map_err(|_| RunApplicationError::Internal)
    };
    Ok(CommandAudit {
        trace: intent.principal.trace,
        tenant_id: intent.principal.tenant_id.clone(),
        principal_id: intent.principal.principal_id.clone(),
        principal_kind: intent.principal.principal_kind,
        receipt_id: make_id(ResourceKind::Receipt)?,
        event_id: make_id(ResourceKind::Event)?,
        outbox_id: make_id(ResourceKind::OutboxEvent)?,
        idempotency_key_digest: intent.idempotency_key_digest.clone(),
        request_digest: intent.request_digest.clone(),
        receipt_expires_at: chrono::Utc::now() + chrono::Duration::hours(24),
    })
}

fn run_view_from_record(
    record: insight_platform_postgres::repository::RunRecord,
) -> Result<RunViewV1, RunApplicationError> {
    let run_id: ResourceId = record
        .run_id
        .parse()
        .map_err(|_| RunApplicationError::Internal)?;
    let agent_deployment_id = record
        .agent_deployment_id
        .parse()
        .map_err(|_| RunApplicationError::Internal)?;
    let input_value_id = record
        .input_value_id
        .ok_or(RunApplicationError::Internal)?
        .parse()
        .map_err(|_| RunApplicationError::Internal)?;
    let output_value_id = record
        .output_value_id
        .map(|id| id.parse())
        .transpose()
        .map_err(|_| RunApplicationError::Internal)?;
    let state = record
        .state
        .parse()
        .map_err(|_| RunApplicationError::Internal)?;
    let version = u64::try_from(record.version).map_err(|_| RunApplicationError::Internal)?;
    Ok(RunViewV1 {
        schema_version: 1,
        run_id: run_id.clone(),
        agent_deployment_id,
        state,
        version,
        input_value_id,
        output_value_id,
        pause_generation: u64::try_from(record.pause_generation)
            .map_err(|_| RunApplicationError::Internal)?,
        cancel_generation: u64::try_from(record.cancel_generation)
            .map_err(|_| RunApplicationError::Internal)?,
        deadline: insight_platform_contracts::UtcTimestamp::from_datetime(record.deadline),
        started_at: record
            .started_at
            .map(insight_platform_contracts::UtcTimestamp::from_datetime),
        terminal_at: record
            .terminal_at
            .map(insight_platform_contracts::UtcTimestamp::from_datetime),
        created_at: insight_platform_contracts::UtcTimestamp::from_datetime(record.created_at),
        updated_at: insight_platform_contracts::UtcTimestamp::from_datetime(record.updated_at),
        etag: run_etag(&run_id, version),
    })
}

#[derive(Clone)]
struct PgResources {
    repository: Arc<PgRepository>,
    validator_digest: Sha256Digest,
    validation_profile_digest: Sha256Digest,
}

#[async_trait]
impl ResourceApplication for PgResources {
    async fn create_resource(
        &self,
        intent: CreateResourceIntent,
    ) -> Result<ResourceViewV1, ResourceApplicationError> {
        let now = chrono::Utc::now();
        if intent.deadline <= now {
            return Err(ResourceApplicationError::Unavailable);
        }
        let candidate_resource_id = new_id(intent.resource_kind.id_kind())?;
        let audit = CommandAudit {
            trace: intent.principal.trace,
            tenant_id: intent.principal.tenant_id,
            principal_id: intent.principal.principal_id,
            principal_kind: intent.principal.principal_kind,
            receipt_id: new_id(ResourceKind::Receipt)?,
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: intent.idempotency_key_digest,
            request_digest: intent.request_digest,
            receipt_expires_at: now + chrono::Duration::hours(24),
        };
        let draft = intent.draft;
        let mut transaction = self
            .repository
            .begin_registry_transaction()
            .await
            .map_err(map_resource_repository_error)?;
        let outcome = transaction
            .create_resource_draft(CreateResourceDraft {
                audit,
                resource_id: candidate_resource_id,
                draft: draft.clone(),
            })
            .await
            .map_err(map_resource_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_resource_repository_error)?;
        let record = match outcome {
            insight_platform_contracts::CommandOutcome::Applied(record)
            | insight_platform_contracts::CommandOutcome::Replayed(record) => record,
        };
        let resource_id: ResourceId = record
            .resource_id
            .parse()
            .map_err(|_| ResourceApplicationError::Internal)?;
        if resource_id.kind() != intent.resource_kind.id_kind() {
            return Err(ResourceApplicationError::Internal);
        }
        Ok(ResourceViewV1 {
            schema_version: 1,
            resource_id: resource_id.clone(),
            resource_kind: intent.resource_kind,
            lifecycle_state: EntityLifecycle::Active,
            gate_state: AdministrativeGate::Enabled,
            draft_generation: 1,
            version: 1,
            draft,
            etag: resource_etag(&resource_id, 1),
        })
    }

    async fn discover_mcp_deployment(
        &self,
        intent: DiscoverMcpDeploymentIntent,
    ) -> Result<OperationViewV1, ResourceApplicationError> {
        let now = chrono::Utc::now();
        if intent.deadline <= now {
            return Err(ResourceApplicationError::Invalid);
        }
        let deployment = self
            .repository
            .read_mcp_deployment_for_discovery(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                &intent.resource_id,
                &intent.deployment_id,
            )
            .await
            .map_err(map_resource_repository_error)?;
        let exact_deployment = ExactDeploymentRef::new(
            intent.deployment_id,
            deployment
                .bindings
                .digest
                .parse()
                .map_err(|_| ResourceApplicationError::Internal)?,
        )
        .map_err(|_| ResourceApplicationError::Internal)?;
        let operation_id = new_id(ResourceKind::McpOperation)?;
        let job_id = new_id(ResourceKind::Job)?;
        let artifact_preallocation =
            insight_platform_mcp_host::McpDiscoveryArtifactPreallocation::build(
                new_id(ResourceKind::Artifact)?,
                new_id(ResourceKind::InternalBlob)?,
                new_id(ResourceKind::Job)?,
                new_id(ResourceKind::ArtifactLink)?,
                new_id(ResourceKind::McpDiscoverySnapshot)?,
                new_id(ResourceKind::QuotaLedgerEntry)?,
            )
            .map_err(|_| ResourceApplicationError::Internal)?;
        let audit = CommandAudit {
            trace: intent.principal.trace,
            tenant_id: intent.principal.tenant_id.clone(),
            principal_id: intent.principal.principal_id.clone(),
            principal_kind: intent.principal.principal_kind,
            receipt_id: new_id(ResourceKind::Receipt)?,
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: intent.idempotency_key_digest,
            request_digest: intent.request_digest.clone(),
            receipt_expires_at: now + chrono::Duration::hours(24),
        };
        self.repository
            .create_mcp_discovery_operation(CreateMcpDiscoveryOperation {
                audit,
                operation_id,
                job_id: job_id.clone(),
                logical_key: intent.request_digest.to_string(),
                mcp_deployment: exact_deployment,
                authorization_binding_id: intent.authorization_binding_id,
                artifact_preallocation,
                attempt_limit: 3,
                deadline: intent.deadline,
            })
            .await
            .map_err(map_resource_repository_error)?;
        self.repository
            .read_public_operation(&ReadOperation {
                tenant_id: intent.principal.tenant_id,
                principal_id: intent.principal.principal_id,
                principal_kind: intent.principal.principal_kind,
                operation_id: job_id,
                request_digest: intent.request_digest,
                deadline: now + chrono::Duration::seconds(5),
            })
            .await
            .map_err(map_operation_read_for_resource)
    }

    async fn build_context_dataset(
        &self,
        intent: BuildContextDatasetIntent,
    ) -> Result<OperationViewV1, ResourceApplicationError> {
        let now = chrono::Utc::now();
        if intent.deadline <= now {
            return Err(ResourceApplicationError::Invalid);
        }
        let deployment = self
            .repository
            .read_context_deployment_for_build(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                &intent.resource_id,
                &intent.deployment_id,
            )
            .await
            .map_err(map_resource_repository_error)?;
        let exact_deployment = ExactDeploymentRef::new(
            intent.deployment_id,
            deployment
                .bindings
                .digest
                .parse()
                .map_err(|_| ResourceApplicationError::Internal)?,
        )
        .map_err(|_| ResourceApplicationError::Internal)?;
        let dataset_id = match intent.dataset_id {
            Some(dataset_id) => dataset_id,
            None => new_id(ResourceKind::ContextDataset)?,
        };
        let audit = CommandAudit {
            trace: intent.principal.trace,
            tenant_id: intent.principal.tenant_id,
            principal_id: intent.principal.principal_id,
            principal_kind: intent.principal.principal_kind,
            receipt_id: new_id(ResourceKind::Receipt)?,
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: intent.idempotency_key_digest,
            request_digest: intent.request_digest,
            receipt_expires_at: now + chrono::Duration::hours(24),
        };
        let outcome = self
            .repository
            .request_context_dataset_build(RequestContextDatasetBuild {
                audit,
                context_resource_id: intent.resource_id,
                context_deployment: exact_deployment,
                dataset_id,
                job_id: new_id(ResourceKind::Job)?,
                attempt_limit: 3,
                deadline: intent.deadline,
            })
            .await
            .map_err(map_resource_repository_error)?;
        let job = match outcome {
            insight_platform_contracts::CommandOutcome::Applied(job)
            | insight_platform_contracts::CommandOutcome::Replayed(job) => job,
        };
        project_context_dataset_build_operation(job).map_err(map_operation_read_for_resource)
    }

    async fn read_resource(
        &self,
        intent: ReadResourceIntent,
    ) -> Result<ResourceViewV1, ResourceApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(ResourceApplicationError::Unavailable);
        }
        let record = self
            .repository
            .read_resource_for_principal(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                intent.resource_kind,
                &intent.resource_id,
            )
            .await
            .map_err(map_resource_repository_error)?;
        resource_view_from_record(record, intent.resource_kind, &intent.resource_id)
    }

    async fn update_resource_draft(
        &self,
        intent: UpdateResourceDraftIntent,
    ) -> Result<ResourceViewV1, ResourceApplicationError> {
        let now = chrono::Utc::now();
        if intent.deadline <= now {
            return Err(ResourceApplicationError::Unavailable);
        }
        let expected_resource_version = i64::try_from(intent.expected_resource_version)
            .map_err(|_| ResourceApplicationError::Invalid)?;
        let audit = CommandAudit {
            trace: intent.principal.trace,
            tenant_id: intent.principal.tenant_id,
            principal_id: intent.principal.principal_id,
            principal_kind: intent.principal.principal_kind,
            receipt_id: new_id(ResourceKind::Receipt)?,
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: intent.idempotency_key_digest,
            request_digest: intent.request_digest,
            receipt_expires_at: now + chrono::Duration::hours(24),
        };
        let mut transaction = self
            .repository
            .begin_registry_transaction()
            .await
            .map_err(map_resource_repository_error)?;
        let outcome = transaction
            .update_resource_draft(UpdateResourceDraft {
                audit,
                resource_id: intent.resource_id.clone(),
                expected_resource_version,
                draft: intent.draft,
            })
            .await
            .map_err(map_resource_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_resource_repository_error)?;
        let record = match outcome {
            insight_platform_contracts::CommandOutcome::Applied(record)
            | insight_platform_contracts::CommandOutcome::Replayed(record) => record,
        };
        resource_view_from_record(record, intent.resource_kind, &intent.resource_id)
    }

    async fn validate_resource_draft(
        &self,
        intent: ValidateResourceDraftIntent,
    ) -> Result<OperationViewV1, ResourceApplicationError> {
        let now = chrono::Utc::now();
        if intent.deadline <= now {
            return Err(ResourceApplicationError::Unavailable);
        }
        let expected_resource_version = i64::try_from(intent.expected_resource_version)
            .map_err(|_| ResourceApplicationError::Invalid)?;
        let audit = CommandAudit {
            trace: intent.principal.trace,
            tenant_id: intent.principal.tenant_id,
            principal_id: intent.principal.principal_id,
            principal_kind: intent.principal.principal_kind,
            receipt_id: new_id(ResourceKind::Receipt)?,
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: intent.idempotency_key_digest,
            request_digest: intent.request_digest,
            receipt_expires_at: now + chrono::Duration::hours(24),
        };
        let mut transaction = self
            .repository
            .begin_registry_transaction()
            .await
            .map_err(map_resource_repository_error)?;
        let outcome = transaction
            .request_resource_validation(RequestResourceValidation {
                audit,
                resource_id: intent.resource_id,
                expected_resource_version,
                job_id: new_id(ResourceKind::Job)?,
                validator_digest: self.validator_digest.clone(),
                validation_profile_digest: self.validation_profile_digest.clone(),
                attempt_limit: 3,
                scheduled_at: now,
                deadline: now + chrono::Duration::minutes(5),
            })
            .await
            .map_err(map_resource_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_resource_repository_error)?;
        let accepted = match outcome {
            insight_platform_contracts::CommandOutcome::Applied(accepted)
            | insight_platform_contracts::CommandOutcome::Replayed(accepted) => accepted,
        };
        project_registry_validation_operation(accepted.job).map_err(|error| match error {
            OperationReadError::InvalidRequest => ResourceApplicationError::Invalid,
            OperationReadError::Denied => ResourceApplicationError::Denied,
            OperationReadError::NotFound | OperationReadError::NotPublic => {
                ResourceApplicationError::NotFound
            }
            OperationReadError::AuthorityUnavailable => ResourceApplicationError::Unavailable,
            OperationReadError::CorruptAuthority => ResourceApplicationError::Internal,
        })
    }

    async fn read_resource_version(
        &self,
        intent: ReadResourceVersionIntent,
    ) -> Result<ResourceVersionViewV1, ResourceApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(ResourceApplicationError::Unavailable);
        }
        let record = self
            .repository
            .read_resource_version_for_principal(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                intent.resource_kind,
                &intent.resource_id,
                &intent.resource_version_id,
            )
            .await
            .map_err(map_resource_repository_error)?;
        if record.payload.schema_version != 1
            || record.resource_id != intent.resource_id.to_string()
            || record.resource_version_id != intent.resource_version_id.to_string()
        {
            return Err(ResourceApplicationError::Internal);
        }
        let mut value = record.payload.value;
        value
            .as_object_mut()
            .ok_or(ResourceApplicationError::Internal)?
            .remove("schema_version");
        let payload =
            serde_json::from_value(value).map_err(|_| ResourceApplicationError::Internal)?;
        let revision_no =
            u64::try_from(record.revision_no).map_err(|_| ResourceApplicationError::Internal)?;
        let content_digest: Sha256Digest = record
            .content_digest
            .parse()
            .map_err(|_| ResourceApplicationError::Internal)?;
        let artifact_id = record
            .artifact_id
            .map(|artifact_id| artifact_id.parse())
            .transpose()
            .map_err(|_| ResourceApplicationError::Internal)?;
        Ok(ResourceVersionViewV1 {
            schema_version: 1,
            resource_id: intent.resource_id,
            resource_kind: intent.resource_kind,
            resource_version_id: intent.resource_version_id.clone(),
            revision_no,
            content_digest: content_digest.clone(),
            artifact_id,
            payload,
            created_at: insight_platform_contracts::UtcTimestamp::from_datetime(record.created_at),
            etag: resource_version_etag(&intent.resource_version_id, &content_digest),
        })
    }

    async fn read_deployment(
        &self,
        intent: ReadDeploymentIntent,
    ) -> Result<DeploymentViewV1, ResourceApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(ResourceApplicationError::Unavailable);
        }
        let record = self
            .repository
            .read_deployment_for_principal(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                intent.resource_kind,
                &intent.resource_id,
                &intent.deployment_id,
            )
            .await
            .map_err(map_resource_repository_error)?;
        deployment_view_from_record(record, intent.resource_kind, &intent.resource_id)
    }

    async fn create_deployment(
        &self,
        intent: CreateDeploymentIntent,
    ) -> Result<DeploymentViewV1, ResourceApplicationError> {
        let now = chrono::Utc::now();
        if intent.deadline <= now {
            return Err(ResourceApplicationError::Unavailable);
        }
        let expected_resource_version = i64::try_from(intent.expected_resource_version)
            .map_err(|_| ResourceApplicationError::Invalid)?;
        let audit = CommandAudit {
            trace: intent.principal.trace,
            tenant_id: intent.principal.tenant_id,
            principal_id: intent.principal.principal_id,
            principal_kind: intent.principal.principal_kind,
            receipt_id: new_id(ResourceKind::Receipt)?,
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: intent.idempotency_key_digest,
            request_digest: intent.request_digest,
            receipt_expires_at: now + chrono::Duration::hours(24),
        };
        let mut transaction = self
            .repository
            .begin_registry_transaction()
            .await
            .map_err(map_resource_repository_error)?;
        let outcome = transaction
            .create_deployment(CreateDeployment {
                audit,
                deployment_id: new_id(
                    intent
                        .resource_kind
                        .deployment_kind()
                        .ok_or(ResourceApplicationError::Invalid)?,
                )?,
                resource_id: intent.resource_id.clone(),
                resource_version_id: intent.request.resource_version_id,
                environment: intent.request.environment,
                closure: intent.request.closure,
                expected_resource_version,
            })
            .await
            .map_err(map_resource_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_resource_repository_error)?;
        let record = match outcome {
            insight_platform_contracts::CommandOutcome::Applied(record)
            | insight_platform_contracts::CommandOutcome::Replayed(record) => record,
        };
        deployment_view_from_record(record, intent.resource_kind, &intent.resource_id)
    }

    async fn activate_deployment(
        &self,
        intent: ControlDeploymentIntent,
    ) -> Result<ResourceViewV1, ResourceApplicationError> {
        let now = chrono::Utc::now();
        if intent.deadline <= now {
            return Err(ResourceApplicationError::Unavailable);
        }
        let deployment = self
            .repository
            .read_deployment_for_activator(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                intent.resource_kind,
                &intent.resource_id,
                &intent.deployment_id,
            )
            .await
            .map_err(map_resource_repository_error)?;
        let deployment_digest: Sha256Digest = deployment
            .bindings
            .digest
            .parse()
            .map_err(|_| ResourceApplicationError::Internal)?;
        let expected_resource_version = i64::try_from(intent.expected_resource_version)
            .map_err(|_| ResourceApplicationError::Invalid)?;
        let audit = resource_command_audit(&intent, now)?;
        let mut transaction = self
            .repository
            .begin_registry_transaction()
            .await
            .map_err(map_resource_repository_error)?;
        let outcome = transaction
            .activate_resource(ActivateResource {
                audit,
                resource_id: intent.resource_id.clone(),
                expected_resource_version,
                target: ActiveTarget::Deployment {
                    deployment: ExactDeploymentRef::new(intent.deployment_id, deployment_digest)
                        .map_err(|_| ResourceApplicationError::Invalid)?,
                },
            })
            .await
            .map_err(map_resource_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_resource_repository_error)?;
        let record = match outcome {
            insight_platform_contracts::CommandOutcome::Applied(record)
            | insight_platform_contracts::CommandOutcome::Replayed(record) => record,
        };
        resource_view_from_record(record, intent.resource_kind, &intent.resource_id)
    }

    async fn suspend_deployment(
        &self,
        intent: ControlDeploymentIntent,
    ) -> Result<ResourceViewV1, ResourceApplicationError> {
        let now = chrono::Utc::now();
        if intent.deadline <= now {
            return Err(ResourceApplicationError::Unavailable);
        }
        let expected_resource_version = i64::try_from(intent.expected_resource_version)
            .map_err(|_| ResourceApplicationError::Invalid)?;
        let audit = resource_command_audit(&intent, now)?;
        let mut transaction = self
            .repository
            .begin_registry_transaction()
            .await
            .map_err(map_resource_repository_error)?;
        let outcome = transaction
            .suspend_resource_deployment(SuspendResourceDeployment {
                audit,
                resource_id: intent.resource_id.clone(),
                deployment_id: intent.deployment_id,
                expected_resource_version,
            })
            .await
            .map_err(map_resource_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_resource_repository_error)?;
        let record = match outcome {
            insight_platform_contracts::CommandOutcome::Applied(record)
            | insight_platform_contracts::CommandOutcome::Replayed(record) => record,
        };
        resource_view_from_record(record, intent.resource_kind, &intent.resource_id)
    }

    async fn publish_resource_draft(
        &self,
        intent: PublishResourceDraftIntent,
    ) -> Result<PublishResourceDraftResponseV1, ResourceApplicationError> {
        let now = chrono::Utc::now();
        if intent.deadline <= now {
            return Err(ResourceApplicationError::Unavailable);
        }
        let expected_resource_version = i64::try_from(intent.expected_resource_version)
            .map_err(|_| ResourceApplicationError::Invalid)?;
        let audit = CommandAudit {
            trace: intent.principal.trace,
            tenant_id: intent.principal.tenant_id,
            principal_id: intent.principal.principal_id,
            principal_kind: intent.principal.principal_kind,
            receipt_id: new_id(ResourceKind::Receipt)?,
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: intent.idempotency_key_digest,
            request_digest: intent.request_digest,
            receipt_expires_at: now + chrono::Duration::hours(24),
        };
        let preparation = self
            .repository
            .prepare_resource_publish(&audit, intent.resource_kind, &intent.resource_id)
            .await
            .map_err(map_resource_repository_error)?;
        let current = match preparation {
            insight_platform_postgres::repository::ResourcePublishPreparation::Current(current) => {
                current
            }
            insight_platform_postgres::repository::ResourcePublishPreparation::Replayed(
                published,
            ) => return published_resource_response(published, intent.resource_kind),
        };
        if current.payload.schema_version != 1 {
            return Err(ResourceApplicationError::Internal);
        }
        let mut value = current.payload.value;
        value
            .as_object_mut()
            .ok_or(ResourceApplicationError::Internal)?
            .remove("schema_version");
        let draft: insight_platform_contracts::ResourceDraftPayload =
            serde_json::from_value(value).map_err(|_| ResourceApplicationError::Internal)?;
        let validation = draft
            .validation
            .clone()
            .ok_or(ResourceApplicationError::Conflict)?;
        let expected_draft_digest = draft
            .document_digest()
            .map_err(|_| ResourceApplicationError::Internal)?;
        let payload = insight_platform_contracts::PublishedVersionPayload {
            document: draft.document,
            validation,
        };
        let (revision_no, artifact_id, materials) = match intent.request {
            PublishResourceDraftRequestV1::Single {
                revision_no,
                content_digest,
                artifact_id,
            } => (
                revision_no,
                artifact_id,
                vec![(
                    public_single_version_kind(intent.resource_kind)?,
                    content_digest,
                )],
            ),
            PublishResourceDraftRequestV1::Agent {
                revision_no,
                interface_content_digest,
                plan_content_digest,
                artifact_id,
            } => (
                revision_no,
                artifact_id,
                vec![
                    (
                        ResourceKind::AgentInterfaceRevision,
                        interface_content_digest,
                    ),
                    (ResourceKind::AgentPlanRevision, plan_content_digest),
                ],
            ),
        };
        let revision_no =
            i64::try_from(revision_no).map_err(|_| ResourceApplicationError::Invalid)?;
        let versions = materials
            .into_iter()
            .map(|(kind, content_digest)| {
                Ok(NewPublishedVersion {
                    resource_version_id: new_id(kind)?,
                    revision_no,
                    content_digest,
                    artifact_id: artifact_id.clone(),
                    payload: payload.clone(),
                })
            })
            .collect::<Result<Vec<_>, ResourceApplicationError>>()?;
        let mut transaction = self
            .repository
            .begin_registry_transaction()
            .await
            .map_err(map_resource_repository_error)?;
        let outcome = transaction
            .publish_resource_versions(PublishResourceVersions {
                audit,
                resource_id: intent.resource_id,
                expected_resource_version,
                expected_draft_digest,
                versions,
            })
            .await
            .map_err(map_resource_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_resource_repository_error)?;
        let published = match outcome {
            insight_platform_contracts::CommandOutcome::Applied(published)
            | insight_platform_contracts::CommandOutcome::Replayed(published) => published,
        };
        published_resource_response(published, intent.resource_kind)
    }
}

fn public_single_version_kind(
    kind: insight_platform_contracts::RegistryResourceKind,
) -> Result<ResourceKind, ResourceApplicationError> {
    use insight_platform_contracts::RegistryResourceKind as Kind;
    match kind {
        Kind::Skill => Ok(ResourceKind::SkillRevision),
        Kind::CapabilityInterface => Ok(ResourceKind::CapabilityInterfaceRevision),
        Kind::ContextSourceInterface => Ok(ResourceKind::ContextSourceInterfaceRevision),
        Kind::McpServer => Ok(ResourceKind::McpServerRevision),
        Kind::ModelProfile => Ok(ResourceKind::ModelProfileRevision),
        Kind::Policy => Ok(ResourceKind::PolicyRevision),
        Kind::SandboxProfile => Ok(ResourceKind::SandboxProfileRevision),
        Kind::Agent
        | Kind::CapabilityImplementation
        | Kind::ContextSourceImplementation
        | Kind::ContextDataset
        | Kind::ModelProvider
        | Kind::SandboxRuntime
        | Kind::SandboxPackage => Err(ResourceApplicationError::Invalid),
    }
}

fn resource_command_audit(
    intent: &ControlDeploymentIntent,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<CommandAudit, ResourceApplicationError> {
    Ok(CommandAudit {
        trace: intent.principal.trace,
        tenant_id: intent.principal.tenant_id.clone(),
        principal_id: intent.principal.principal_id.clone(),
        principal_kind: intent.principal.principal_kind,
        receipt_id: new_id(ResourceKind::Receipt)?,
        event_id: new_id(ResourceKind::Event)?,
        outbox_id: new_id(ResourceKind::OutboxEvent)?,
        idempotency_key_digest: intent.idempotency_key_digest.clone(),
        request_digest: intent.request_digest.clone(),
        receipt_expires_at: now + chrono::Duration::hours(24),
    })
}

fn published_resource_response(
    published: insight_platform_postgres::repository::PublishedResource,
    resource_kind: insight_platform_contracts::RegistryResourceKind,
) -> Result<PublishResourceDraftResponseV1, ResourceApplicationError> {
    let resource_id: ResourceId = published
        .resource
        .resource_id
        .parse()
        .map_err(|_| ResourceApplicationError::Internal)?;
    if resource_id.kind() != resource_kind.id_kind()
        || published.resource.resource_kind != resource_kind.as_str()
    {
        return Err(ResourceApplicationError::Internal);
    }
    let version = u64::try_from(published.resource.version)
        .map_err(|_| ResourceApplicationError::Internal)?;
    let draft_generation = u64::try_from(published.resource.draft_generation)
        .map_err(|_| ResourceApplicationError::Internal)?;
    let published_versions = published
        .versions
        .into_iter()
        .map(|record| {
            let resource_version_id: ResourceId = record
                .resource_version_id
                .parse()
                .map_err(|_| ResourceApplicationError::Internal)?;
            let revision_no = u64::try_from(record.revision_no)
                .map_err(|_| ResourceApplicationError::Internal)?;
            let content_digest: Sha256Digest = record
                .content_digest
                .parse()
                .map_err(|_| ResourceApplicationError::Internal)?;
            let artifact_id = record
                .artifact_id
                .map(|artifact_id| artifact_id.parse())
                .transpose()
                .map_err(|_| ResourceApplicationError::Internal)?;
            Ok(PublishedResourceVersionSummaryV1 {
                resource_version_id: resource_version_id.clone(),
                revision_no,
                content_digest: content_digest.clone(),
                artifact_id,
                etag: resource_version_etag(&resource_version_id, &content_digest),
            })
        })
        .collect::<Result<Vec<_>, ResourceApplicationError>>()?;
    Ok(PublishResourceDraftResponseV1 {
        schema_version: 1,
        resource_id: resource_id.clone(),
        resource_kind,
        draft_generation,
        version,
        published_versions,
        etag: resource_etag(&resource_id, version),
    })
}

fn deployment_view_from_record(
    record: insight_platform_postgres::repository::DeploymentRecord,
    resource_kind: insight_platform_contracts::RegistryResourceKind,
    resource_id: &ResourceId,
) -> Result<DeploymentViewV1, ResourceApplicationError> {
    if record.bindings.schema_version != 1 || record.resource_id != resource_id.to_string() {
        return Err(ResourceApplicationError::Internal);
    }
    let deployment_id: ResourceId = record
        .deployment_id
        .parse()
        .map_err(|_| ResourceApplicationError::Internal)?;
    if resource_kind.deployment_kind() != Some(deployment_id.kind()) {
        return Err(ResourceApplicationError::Internal);
    }
    let closure_digest: Sha256Digest = record
        .bindings
        .digest
        .parse()
        .map_err(|_| ResourceApplicationError::Internal)?;
    let mut value = record.bindings.value;
    value
        .as_object_mut()
        .ok_or(ResourceApplicationError::Internal)?
        .remove("schema_version");
    let closure: DeploymentClosure =
        serde_json::from_value(value).map_err(|_| ResourceApplicationError::Internal)?;
    let resource_version_id: ResourceId = record
        .resource_version_id
        .parse()
        .map_err(|_| ResourceApplicationError::Internal)?;
    Ok(DeploymentViewV1 {
        schema_version: 1,
        deployment_id: deployment_id.clone(),
        resource_id: resource_id.clone(),
        resource_kind,
        resource_version_id,
        environment: record.environment,
        closure_digest: closure_digest.clone(),
        closure,
        created_at: insight_platform_contracts::UtcTimestamp::from_datetime(record.created_at),
        etag: deployment_etag(&deployment_id, &closure_digest),
    })
}

fn resource_view_from_record(
    record: insight_platform_postgres::repository::ResourceRecord,
    resource_kind: insight_platform_contracts::RegistryResourceKind,
    resource_id: &ResourceId,
) -> Result<ResourceViewV1, ResourceApplicationError> {
    if record.payload.schema_version != 1
        || record.resource_id != resource_id.to_string()
        || record.resource_kind != resource_kind.as_str()
    {
        return Err(ResourceApplicationError::Internal);
    }
    let mut value = record.payload.value;
    let object = value
        .as_object_mut()
        .ok_or(ResourceApplicationError::Internal)?;
    object.remove("schema_version");
    let draft = serde_json::from_value(value).map_err(|_| ResourceApplicationError::Internal)?;
    let version = u64::try_from(record.version).map_err(|_| ResourceApplicationError::Internal)?;
    let draft_generation =
        u64::try_from(record.draft_generation).map_err(|_| ResourceApplicationError::Internal)?;
    Ok(ResourceViewV1 {
        schema_version: 1,
        resource_id: resource_id.clone(),
        resource_kind,
        lifecycle_state: record
            .lifecycle_state
            .parse()
            .map_err(|_| ResourceApplicationError::Internal)?,
        gate_state: record
            .gate_state
            .parse()
            .map_err(|_| ResourceApplicationError::Internal)?,
        draft_generation,
        version,
        draft,
        etag: resource_etag(resource_id, version),
    })
}

fn new_id(kind: ResourceKind) -> Result<ResourceId, ResourceApplicationError> {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7())
        .map_err(|_| ResourceApplicationError::Internal)
}

fn map_resource_repository_error(error: RepositoryError) -> ResourceApplicationError {
    match error {
        RepositoryError::InvalidInput(_) => ResourceApplicationError::Invalid,
        RepositoryError::NotFound(_) => ResourceApplicationError::NotFound,
        RepositoryError::Conflict(_) | RepositoryError::StaleFence => {
            ResourceApplicationError::Conflict
        }
        RepositoryError::PermissionDenied => ResourceApplicationError::Denied,
        RepositoryError::IdempotencyConflict => ResourceApplicationError::IdempotencyConflict,
        RepositoryError::Database(_) | RepositoryError::LeaseExpired => {
            ResourceApplicationError::Unavailable
        }
        RepositoryError::QuotaExceeded | RepositoryError::CorruptRow(_) => {
            ResourceApplicationError::Internal
        }
    }
}

fn map_operation_read_for_resource(error: OperationReadError) -> ResourceApplicationError {
    match error {
        OperationReadError::InvalidRequest => ResourceApplicationError::Invalid,
        OperationReadError::Denied => ResourceApplicationError::Denied,
        OperationReadError::NotFound | OperationReadError::NotPublic => {
            ResourceApplicationError::NotFound
        }
        OperationReadError::AuthorityUnavailable => ResourceApplicationError::Unavailable,
        OperationReadError::CorruptAuthority => ResourceApplicationError::Internal,
    }
}

fn map_run_repository_error(error: RepositoryError) -> RunApplicationError {
    match error {
        RepositoryError::PermissionDenied => RunApplicationError::Denied,
        RepositoryError::NotFound(_) => RunApplicationError::NotFound,
        RepositoryError::Database(_) | RepositoryError::LeaseExpired => {
            RunApplicationError::Unavailable
        }
        RepositoryError::InvalidInput(_) => RunApplicationError::Invalid,
        RepositoryError::Conflict(_) | RepositoryError::StaleFence => RunApplicationError::Conflict,
        RepositoryError::IdempotencyConflict => RunApplicationError::IdempotencyConflict,
        RepositoryError::QuotaExceeded | RepositoryError::CorruptRow(_) => {
            RunApplicationError::Internal
        }
    }
}

fn map_task_repository_error(error: RepositoryError) -> TaskApplicationError {
    match error {
        RepositoryError::InvalidInput(_) => TaskApplicationError::Invalid,
        RepositoryError::PermissionDenied => TaskApplicationError::Denied,
        RepositoryError::NotFound(_) => TaskApplicationError::NotFound,
        RepositoryError::Conflict(_) | RepositoryError::StaleFence => {
            TaskApplicationError::Conflict
        }
        RepositoryError::IdempotencyConflict => TaskApplicationError::IdempotencyConflict,
        RepositoryError::Database(_) | RepositoryError::LeaseExpired => {
            TaskApplicationError::Unavailable
        }
        RepositoryError::QuotaExceeded | RepositoryError::CorruptRow(_) => {
            TaskApplicationError::Internal
        }
    }
}

struct RouterDependencies {
    repository: Arc<PgRepository>,
    artifact_mutation_forwarder: Option<Arc<dyn ArtifactMutationForwarder>>,
    verifier: insight_platform_api::oidc::InstalledOidcVerifier,
    validator_digest: Sha256Digest,
    validation_profile_digest: Sha256Digest,
    run_event_cursor_codec: Option<Arc<dyn RunEventCursorCodec>>,
    metrics: Arc<ProcessHttpMetrics>,
}

fn build_router(
    role: ProcessRole,
    dependencies: RouterDependencies,
) -> Result<Router, ProcessError> {
    let RouterDependencies {
        repository,
        artifact_mutation_forwarder,
        verifier,
        validator_digest,
        validation_profile_digest,
        run_event_cursor_codec,
        metrics,
    } = dependencies;
    let authentication = PublicAuthenticationState::new(
        Arc::new(verifier),
        Arc::new(PgPrincipalBindings(repository.clone())),
        Arc::new(SystemAuthenticationClock),
    );
    let operation = build_operation_router(OperationHttpState::new(
        Arc::new(PgOperations(repository.clone())),
        Arc::new(SystemOperationClock),
    ));
    let protected = match role {
        ProcessRole::ManagementApi => {
            operation.merge(build_resource_router(ResourceHttpState::new(
                Arc::new(PgResources {
                    repository,
                    validator_digest,
                    validation_profile_digest,
                }),
                Arc::new(SystemResourceClock),
            )))
        }
        ProcessRole::RuntimeApi => {
            let run_event_cursor_codec =
                run_event_cursor_codec.ok_or(ProcessError::InvalidConfiguration)?;
            let artifact_mutation_forwarder =
                artifact_mutation_forwarder.ok_or(ProcessError::InvalidConfiguration)?;
            operation
                .merge(build_run_router(
                    RunHttpState::new(
                        Arc::new(PgRuns(repository.clone())),
                        Arc::new(SystemRunClock),
                    )
                    .with_event_cursor_codec(run_event_cursor_codec),
                ))
                .merge(build_task_router(TaskHttpState::new(
                    Arc::new(PgTasks(repository)),
                    Arc::new(SystemTaskClock),
                )))
                .merge(build_artifact_router(ArtifactHttpState::new(
                    Arc::new(PgArtifacts {
                        mutation_forwarder: artifact_mutation_forwarder,
                    }),
                    Arc::new(SystemArtifactClock),
                )))
        }
    }
    .route_layer(middleware::from_fn_with_state(
        authentication,
        authenticate_public_request,
    ));
    Ok(Router::new()
        .route("/livez", get(live))
        .route("/readyz", get(ready))
        .route("/metrics", get(prometheus_metrics))
        .merge(protected)
        .layer(middleware::from_fn_with_state(role, enforce_process_role))
        .layer(middleware::from_fn(observe_gateway_request))
        .layer(Extension(metrics))
        .layer(middleware::from_fn(establish_public_trace)))
}

async fn enforce_process_role(
    State(role): State<ProcessRole>,
    request: Request,
    next: Next,
) -> Response {
    if role.permits_path(request.uri().path()) {
        next.run(request).await
    } else {
        let mut response = StatusCode::NOT_FOUND.into_response();
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
    }
}

async fn live() -> Response {
    health("live")
}

async fn ready(Extension(metrics): Extension<Arc<ProcessHttpMetrics>>) -> Response {
    if metrics.is_ready() {
        health("ready")
    } else {
        let mut response = (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response();
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
    }
}

async fn prometheus_metrics(Extension(metrics): Extension<Arc<ProcessHttpMetrics>>) -> Response {
    let mut response = metrics.render_prometheus().into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn observe_gateway_request(request: Request, next: Next) -> Response {
    let metrics = request
        .extensions()
        .get::<Arc<ProcessHttpMetrics>>()
        .cloned()
        .expect("Gateway metrics Extension is installed by build_router");
    let operation = gateway_operation(request.uri().path());
    let started = Instant::now();
    let response = next.run(request).await;
    metrics.observe(operation, response.status().as_u16(), started.elapsed());
    response
}

fn health(state: &'static str) -> Response {
    let mut response = (StatusCode::OK, state).into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[derive(Debug)]
enum ProcessError {
    InvalidConfiguration,
    Io(std::io::Error),
    Database(sqlx::Error),
    Schema(insight_platform_postgres::AuthoritySchemaError),
    ServerUnavailable,
    DependencyObserverUnavailable,
    ShutdownDeadlineExceeded,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => formatter.write_str("configuration is invalid"),
            Self::Io(error) => write!(formatter, "I/O failed: {error}"),
            Self::Database(error) => write!(formatter, "database failed: {error}"),
            Self::Schema(error) => write!(formatter, "schema verification failed: {error}"),
            Self::ServerUnavailable => formatter.write_str("Gateway HTTP server unavailable"),
            Self::DependencyObserverUnavailable => {
                formatter.write_str("Gateway dependency observer unavailable")
            }
            Self::ShutdownDeadlineExceeded => {
                formatter.write_str("Gateway shutdown deadline exceeded")
            }
        }
    }
}

impl Error for ProcessError {}

impl From<std::io::Error> for ProcessError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<sqlx::Error> for ProcessError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

fn required(name: &str) -> Result<String, ProcessError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty() && value.len() <= 16_384)
        .ok_or(ProcessError::InvalidConfiguration)
}

fn required_absolute_path(name: &str) -> Result<PathBuf, ProcessError> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(path)
}

fn read_bounded_file(path: &Path, maximum_bytes: usize) -> Result<Vec<u8>, ProcessError> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(u64::try_from(maximum_bytes).unwrap_or(u64::MAX) + 1)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() > maximum_bytes {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}

fn load_run_event_cursor_codec() -> Result<Arc<dyn RunEventCursorCodec>, ProcessError> {
    let path = required_absolute_path(RUN_EVENT_CURSOR_KEY_PATH_ENV)?;
    let key = read_bounded_file(&path, MAX_RUN_EVENT_CURSOR_KEY_BYTES)?;
    let expected: Sha256Digest = required(RUN_EVENT_CURSOR_KEY_DIGEST_ENV)?
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    install_run_event_cursor_codec(&key, &expected)
}

fn install_run_event_cursor_codec(
    key: &[u8],
    expected: &Sha256Digest,
) -> Result<Arc<dyn RunEventCursorCodec>, ProcessError> {
    let actual = Sha256::digest(key);
    let hexadecimal = actual
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let actual: Sha256Digest = format!("sha256:{hexadecimal}")
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    if &actual != expected {
        return Err(ProcessError::InvalidConfiguration);
    }
    HmacRunEventCursorCodec::install(key)
        .map(|codec| Arc::new(codec) as Arc<dyn RunEventCursorCodec>)
        .map_err(|_| ProcessError::InvalidConfiguration)
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let config = ProcessConfig::load()?;
    let verifier = config
        .oidc
        .clone()
        .install()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let (run_event_cursor_codec, artifact_mutation_forwarder) = match config.role {
        ProcessRole::ManagementApi => (None, None),
        ProcessRole::RuntimeApi => (
            Some(load_run_event_cursor_codec()?),
            Some(Arc::new(MtlsArtifactMutationForwarder::install()?)
                as Arc<dyn ArtifactMutationForwarder>),
        ),
    };
    let database_url = required(DATABASE_URL_ENV)?;
    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(Duration::from_millis(
            config.database_acquire_timeout_milliseconds,
        ))
        .connect(&database_url)
        .await?;
    verify_schema(&pool).await.map_err(ProcessError::Schema)?;
    let database_health_pool = pool.clone();
    let repository = Arc::new(PgRepository::new(pool.clone()));
    let listener = tokio::net::TcpListener::bind(&config.listen_address).await?;
    let (dependency_metrics, postgres_observer) =
        install_postgres_dependency_metrics().map_err(|_| ProcessError::InvalidConfiguration)?;
    let metrics = install_gateway_metrics_with_postgres_and_dependencies(
        config.role,
        pool,
        config.database_max_connections,
        Some(dependency_metrics),
    );
    metrics.mark_ready();
    tracing::info!(listen_address = %config.listen_address, "public gateway ready");
    let cancellation = CancellationToken::new();
    let server = axum::serve(
        listener,
        build_router(
            config.role,
            RouterDependencies {
                repository,
                artifact_mutation_forwarder,
                verifier,
                validator_digest: config.registry_validator_digest,
                validation_profile_digest: config.registry_validation_profile_digest,
                run_event_cursor_codec,
                metrics,
            },
        )?,
    )
    .with_graceful_shutdown(cancellation.child_token().cancelled_owned());
    let mut server = tokio::spawn(async move { server.await });
    let mut postgres_health = tokio::spawn(run_postgres_health_sampler(
        database_health_pool,
        postgres_observer,
        cancellation.child_token(),
    ));
    let shutdown_grace = Duration::from_millis(config.shutdown_grace_milliseconds);
    let result = tokio::select! {
        _ = shutdown_signal() => {
            cancellation.cancel();
            let drained = tokio::time::timeout(shutdown_grace, async {
                let (server_result, postgres_result) = tokio::join!(
                    &mut server,
                    &mut postgres_health,
                );
                server_result.map_err(|_| ProcessError::ServerUnavailable)?
                    .map_err(ProcessError::Io)?;
                postgres_result.map_err(|_| ProcessError::DependencyObserverUnavailable)
            }).await;
            match drained {
                Ok(result) => result,
                Err(_) => {
                    server.abort();
                    postgres_health.abort();
                    let _ = tokio::join!(server, postgres_health);
                    Err(ProcessError::ShutdownDeadlineExceeded)
                }
            }
        }
        result = &mut server => {
            cancellation.cancel();
            let server_result = result.map_err(|_| ProcessError::ServerUnavailable)
                .and_then(|result| result.map_err(ProcessError::Io));
            let postgres_result = postgres_health.await
                .map_err(|_| ProcessError::DependencyObserverUnavailable);
            server_result?;
            postgres_result?;
            Err(ProcessError::ServerUnavailable)
        }
        result = &mut postgres_health => {
            cancellation.cancel();
            let postgres_result = result
                .map_err(|_| ProcessError::DependencyObserverUnavailable);
            let server_result = server.await.map_err(|_| ProcessError::ServerUnavailable)
                .and_then(|result| result.map_err(ProcessError::Io));
            postgres_result?;
            server_result?;
            Err(ProcessError::DependencyObserverUnavailable)
        }
    };
    result.map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    #[derive(Debug)]
    struct UnavailableArtifactForwarder;

    #[async_trait]
    impl ArtifactMutationForwarder for UnavailableArtifactForwarder {
        async fn prepare(
            &self,
            _intent: PrepareArtifactUploadIntent,
        ) -> Result<PrepareArtifactUploadResponseV1, ArtifactApplicationError> {
            Err(ArtifactApplicationError::Unavailable)
        }

        async fn complete(
            &self,
            _intent: CompleteArtifactUploadIntent,
        ) -> Result<ArtifactMutationAcceptedV1, ArtifactApplicationError> {
            Err(ArtifactApplicationError::Unavailable)
        }

        async fn delete(
            &self,
            _intent: DeleteArtifactIntent,
        ) -> Result<ArtifactMutationAcceptedV1, ArtifactApplicationError> {
            Err(ArtifactApplicationError::Unavailable)
        }

        async fn read(
            &self,
            _intent: ReadArtifactIntent,
        ) -> Result<ArtifactViewV1, ArtifactApplicationError> {
            Err(ArtifactApplicationError::Unavailable)
        }

        async fn read_content(
            &self,
            _intent: ReadArtifactIntent,
        ) -> Result<ArtifactContentStreamV1, ArtifactApplicationError> {
            Err(ArtifactApplicationError::Unavailable)
        }
    }

    fn unavailable_artifact_forwarder() -> Arc<dyn ArtifactMutationForwarder> {
        Arc::new(UnavailableArtifactForwarder)
    }

    fn fixed_digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn oidc_config() -> InstalledOidcVerifierConfig {
        let keys = serde_json::json!({"keys": [{
            "kty": "RSA", "kid": "key-1", "use": "sig", "alg": "RS256",
            "n": "4KoeIFhx35ADyXYT0MpVCFDcWPKi1KUDxNTnPu1uubb9hqbnpgq68U8YQAGT1Dh1B4lyZmqUvYbGLNBj7CEcuJdms6JkohM50AdwBv6-TCy_uLpZzcUs8AGh8zFyVeyceX2CkZptlaP-362KPVB0tnvmjRVO2tJLiiqFBGqe9OKKGL-WevKFrUlSoaWTova7baKBBIMUx8GckC9NHvSj9oMbaaOTziTSOhonVnzHr1diFh5CbluUn3ef6KFcO8mssT-prqfqHYnNCEeLRsEUZT79oCVXb2H9RasBv7mU-FNPNwj8dcWcfUIV6ePEDjAGH-KU1eStSTYxeJEfbgW9zw",
            "e": "AQAB"
        }]});
        InstalledOidcVerifierConfig {
            issuer: "https://issuer.example".to_owned(),
            audience: "insight-platform-public".to_owned(),
            jwks_digest: canonical_digest(&keys).unwrap().parse().unwrap(),
            jwks: keys,
        }
    }

    fn test_run_event_cursor_codec() -> Arc<dyn RunEventCursorCodec> {
        Arc::new(HmacRunEventCursorCodec::install(&[7_u8; 32]).unwrap())
    }

    #[test]
    fn process_config_rejects_unbounded_or_ambiguous_values() {
        let mut config = ProcessConfig {
            schema_version: 1,
            role: ProcessRole::RuntimeApi,
            listen_address: "0.0.0.0:8080".to_owned(),
            database_max_connections: 8,
            database_acquire_timeout_milliseconds: 1_000,
            shutdown_grace_milliseconds: 5_000,
            registry_validator_digest: fixed_digest('1'),
            registry_validation_profile_digest: fixed_digest('2'),
            oidc: oidc_config(),
        };
        assert!(config.validate().is_ok());
        config.database_max_connections = 1_000;
        assert!(matches!(
            config.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));
    }

    #[test]
    fn run_event_cursor_key_requires_exact_digest_and_bounded_entropy() {
        let key = [7_u8; 32];
        let hexadecimal = Sha256::digest(key)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let expected: Sha256Digest = format!("sha256:{hexadecimal}").parse().unwrap();
        assert!(install_run_event_cursor_codec(&key, &expected).is_ok());
        assert!(matches!(
            install_run_event_cursor_codec(&key, &fixed_digest('0')),
            Err(ProcessError::InvalidConfiguration)
        ));
        let short_key = [7_u8; 31];
        let hexadecimal = Sha256::digest(short_key)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let short_digest: Sha256Digest = format!("sha256:{hexadecimal}").parse().unwrap();
        assert!(matches!(
            install_run_event_cursor_codec(&short_key, &short_digest),
            Err(ProcessError::InvalidConfiguration)
        ));
    }

    #[tokio::test]
    async fn health_is_public_but_operation_routes_require_verified_authentication() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let metrics = install_gateway_metrics(ProcessRole::RuntimeApi);
        metrics.mark_ready();
        let router = build_router(
            ProcessRole::RuntimeApi,
            RouterDependencies {
                repository: Arc::new(PgRepository::new(pool)),
                artifact_mutation_forwarder: Some(unavailable_artifact_forwarder()),
                verifier: oidc_config().install().unwrap(),
                validator_digest: fixed_digest('1'),
                validation_profile_digest: fixed_digest('2'),
                run_event_cursor_codec: Some(test_run_event_cursor_codec()),
                metrics: Arc::clone(&metrics),
            },
        )
        .unwrap();
        let live = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/livez")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(live.status(), StatusCode::OK);
        assert_eq!(live.headers()[CACHE_CONTROL], "no-store");

        let operation = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/operations/job_0198f1cc-32e4-75e1-a9e8-d95ca0f80001")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(operation.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(operation.headers()[CACHE_CONTROL], "no-store");

        let metrics_response = router
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metrics_response.status(), StatusCode::OK);
        assert_eq!(metrics_response.headers()[CACHE_CONTROL], "no-store");
        let body = axum::body::to_bytes(metrics_response.into_body(), 65_536)
            .await
            .unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("insight_platform_process_ready{component_role=\"runtime-api\"} 1"));
        assert!(body.contains(
            "insight_platform_http_requests_total{component_role=\"runtime-api\",operation=\"operations\",outcome=\"rejected\"} 1"
        ));
        assert!(!body.contains("job_0198f1cc"));
    }

    #[tokio::test]
    async fn management_and_runtime_roles_expose_disjoint_business_routes() {
        let management_pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let management_metrics = install_gateway_metrics(ProcessRole::ManagementApi);
        let management = build_router(
            ProcessRole::ManagementApi,
            RouterDependencies {
                repository: Arc::new(PgRepository::new(management_pool)),
                artifact_mutation_forwarder: None,
                verifier: oidc_config().install().unwrap(),
                validator_digest: fixed_digest('1'),
                validation_profile_digest: fixed_digest('2'),
                run_event_cursor_codec: None,
                metrics: management_metrics,
            },
        )
        .unwrap();
        let runtime_pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let runtime_metrics = install_gateway_metrics(ProcessRole::RuntimeApi);
        let runtime = build_router(
            ProcessRole::RuntimeApi,
            RouterDependencies {
                repository: Arc::new(PgRepository::new(runtime_pool)),
                artifact_mutation_forwarder: Some(unavailable_artifact_forwarder()),
                verifier: oidc_config().install().unwrap(),
                validator_digest: fixed_digest('1'),
                validation_profile_digest: fixed_digest('2'),
                run_event_cursor_codec: Some(test_run_event_cursor_codec()),
                metrics: runtime_metrics,
            },
        )
        .unwrap();

        for (uri, management_status, runtime_status) in [
            (
                "/v1/agents/res_0198f1cc-32e4-75e1-a9e8-d95ca0f80001",
                401,
                404,
            ),
            (
                "/v1/runs/run_0198f1cc-32e4-75e1-a9e8-d95ca0f80001",
                404,
                401,
            ),
            (
                "/v1/tasks/task_0198f1cc-32e4-75e1-a9e8-d95ca0f80001",
                404,
                401,
            ),
            (
                "/v1/artifacts/art_0198f1cc-32e4-75e1-a9e8-d95ca0f80001",
                404,
                401,
            ),
            (
                "/v1/operations/job_0198f1cc-32e4-75e1-a9e8-d95ca0f80001",
                401,
                401,
            ),
        ] {
            let management_response = management
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                management_response.status().as_u16(),
                management_status,
                "{uri}"
            );
            let runtime_response = runtime
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(runtime_response.status().as_u16(), runtime_status, "{uri}");
        }
    }

    #[test]
    fn gateway_metrics_use_only_closed_operation_and_outcome_labels() {
        let metrics = install_gateway_metrics(ProcessRole::RuntimeApi);
        metrics.observe(
            gateway_operation("/v1/runs/run_sensitive"),
            200,
            Duration::from_millis(7),
        );
        metrics.observe(
            gateway_operation("/v1/unregistered/tenant_sensitive"),
            503,
            Duration::from_secs(8),
        );
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("operation=\"runs\",outcome=\"success\",le=\"0.01\"} 1"));
        assert!(rendered.contains("operation=\"other\",outcome=\"failure\",le=\"+Inf\"} 1"));
        assert!(!rendered.contains("run_sensitive"));
        assert!(!rendered.contains("tenant_sensitive"));
    }

    #[tokio::test]
    async fn gateway_postgresql_capacity_includes_unopened_pool_slots() {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let metrics = install_gateway_metrics_with_postgres(ProcessRole::RuntimeApi, pool, 4);
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("resource=\"postgresql_connections\",state=\"available\"} 4"));
        assert!(rendered.contains("resource=\"postgresql_connections\",state=\"used\"} 0"));
    }

    #[tokio::test]
    async fn gateway_postgresql_capacity_tracks_a_real_checked_out_connection() {
        let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
            return;
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .unwrap();
        let metrics =
            install_gateway_metrics_with_postgres(ProcessRole::ManagementApi, pool.clone(), 2);
        let connection = pool.acquire().await.unwrap();
        assert!(metrics
            .render_prometheus()
            .contains("resource=\"postgresql_connections\",state=\"used\"} 1"));
        drop(connection);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if metrics
                    .render_prometheus()
                    .contains("resource=\"postgresql_connections\",state=\"used\"} 0")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("released SQLx connection must return to the idle pool");
    }
}
