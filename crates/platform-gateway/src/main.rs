//! Deployable public Gateway for the clean-cut Platform `/v1` contract.

use async_trait::async_trait;
use axum::{
    http::{header::CACHE_CONTROL, HeaderValue, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use insight_platform_api::{
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
        ControlDeploymentIntent, CreateDeploymentIntent, CreateResourceIntent, DeploymentViewV1,
        PublishResourceDraftIntent, PublishResourceDraftRequestV1, PublishResourceDraftResponseV1,
        PublishedResourceVersionSummaryV1, ReadDeploymentIntent, ReadResourceIntent,
        ReadResourceVersionIntent, ResourceApplication, ResourceApplicationError,
        ResourceHttpState, ResourceVersionViewV1, ResourceViewV1, SystemResourceClock,
        UpdateResourceDraftIntent, ValidateResourceDraftIntent,
    },
    run::{
        build_run_router, run_etag, ControlRunIntent, CreateRunIntent, ReadRunIntent,
        RunApplication, RunApplicationError, RunHttpState, RunViewV1, SystemRunClock,
    },
};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, ActiveTarget, AdministrativeGate, CommandAudit,
    DeploymentClosure, EntityLifecycle, ExactDeploymentRef, JsonLimits, OperationViewV1,
    PrincipalKind, PrincipalSnapshot, ReadOperation, ResourceId, ResourceKind, RunBindingsSnapshot,
    Sha256Digest, ValueRef,
};
use insight_platform_orchestrator::{
    AdmitRun, PlanNodeKey, RequestRunCancel, RunInputValue, SetRunPause,
};
use insight_platform_postgres::{
    operation_repository::{project_registry_validation_operation, OperationReadError},
    repository::{PgRepository, RepositoryError},
    verify_schema,
};
use insight_platform_registry::{
    ActivateResource, CreateDeployment, CreateResourceDraft, NewPublishedVersion,
    PublishResourceVersions, RequestResourceValidation, SuspendResourceDeployment,
    UpdateResourceDraft,
};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{
    error::Error,
    fmt,
    fs::File,
    io::Read as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

const CONFIG_PATH_ENV: &str = "PLATFORM_GATEWAY_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_GATEWAY_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_GATEWAY_DATABASE_URL";
const MAX_CONFIG_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
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

fn build_router(
    repository: Arc<PgRepository>,
    verifier: insight_platform_api::oidc::InstalledOidcVerifier,
    validator_digest: Sha256Digest,
    validation_profile_digest: Sha256Digest,
) -> Router {
    let authentication = PublicAuthenticationState::new(
        Arc::new(verifier),
        Arc::new(PgPrincipalBindings(repository.clone())),
        Arc::new(SystemAuthenticationClock),
    );
    let operation = build_operation_router(OperationHttpState::new(
        Arc::new(PgOperations(repository.clone())),
        Arc::new(SystemOperationClock),
    ));
    let resource = build_resource_router(ResourceHttpState::new(
        Arc::new(PgResources {
            repository: repository.clone(),
            validator_digest,
            validation_profile_digest,
        }),
        Arc::new(SystemResourceClock),
    ));
    let run = build_run_router(RunHttpState::new(
        Arc::new(PgRuns(repository.clone())),
        Arc::new(SystemRunClock),
    ));
    let protected =
        operation
            .merge(resource)
            .merge(run)
            .route_layer(middleware::from_fn_with_state(
                authentication,
                authenticate_public_request,
            ));
    Router::new()
        .route("/livez", get(live))
        .route("/readyz", get(ready))
        .merge(protected)
}

async fn live() -> Response {
    health("live")
}

async fn ready() -> Response {
    health("ready")
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
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => formatter.write_str("configuration is invalid"),
            Self::Io(error) => write!(formatter, "I/O failed: {error}"),
            Self::Database(error) => write!(formatter, "database failed: {error}"),
            Self::Schema(error) => write!(formatter, "schema verification failed: {error}"),
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

async fn shutdown_signal(grace: Duration) {
    let _ = tokio::signal::ctrl_c().await;
    tokio::time::sleep(grace.min(Duration::from_secs(1))).await;
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
    let database_url = required(DATABASE_URL_ENV)?;
    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(Duration::from_millis(
            config.database_acquire_timeout_milliseconds,
        ))
        .connect(&database_url)
        .await?;
    verify_schema(&pool).await.map_err(ProcessError::Schema)?;
    let repository = Arc::new(PgRepository::new(pool));
    let listener = tokio::net::TcpListener::bind(&config.listen_address).await?;
    tracing::info!(listen_address = %config.listen_address, "public gateway ready");
    axum::serve(
        listener,
        build_router(
            repository,
            verifier,
            config.registry_validator_digest,
            config.registry_validation_profile_digest,
        ),
    )
    .with_graceful_shutdown(shutdown_signal(Duration::from_millis(
        config.shutdown_grace_milliseconds,
    )))
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

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

    #[test]
    fn process_config_rejects_unbounded_or_ambiguous_values() {
        let mut config = ProcessConfig {
            schema_version: 1,
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

    #[tokio::test]
    async fn health_is_public_but_operation_routes_require_verified_authentication() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let router = build_router(
            Arc::new(PgRepository::new(pool)),
            oidc_config().install().unwrap(),
            fixed_digest('1'),
            fixed_digest('2'),
        );
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
    }
}
