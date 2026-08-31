//! Pure management-domain commands and transaction ports for the shared Resource registry.
//!
//! This crate deliberately has no SQLx, HTTP, queue, or provider dependency. Storage adapters
//! implement [`RegistryStore`] and [`RegistryTransaction`]; application services own commit.

#![allow(async_fn_in_trait)]

use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, ActiveTarget, AdministrativeGate, DeploymentClosure, EntityLifecycle,
    PublishedVersionPayload, RegistryResourceKind, ResourceDraftPayload, ResourceId, ResourceKind,
    Sha256Digest, ValidationSummary,
};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

pub use insight_platform_contracts::{CommandAudit, CommandOutcome};

#[derive(Debug, Clone)]
pub struct CreateResourceDraft {
    pub audit: CommandAudit,
    pub resource_id: ResourceId,
    pub draft: ResourceDraftPayload,
}

impl CreateResourceDraft {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), RegistryCommandError> {
        validate_audit(&self.audit, now)?;
        self.draft
            .validate()
            .map_err(|failure| RegistryCommandError::Contract(failure.to_string()))?;
        if self.resource_id.kind() != self.draft.document.kind().id_kind()
            || self.draft.validation.is_some()
        {
            return Err(RegistryCommandError::InvalidResourceDraft);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct UpdateResourceDraft {
    pub audit: CommandAudit,
    pub resource_id: ResourceId,
    pub expected_resource_version: i64,
    pub draft: ResourceDraftPayload,
}

impl UpdateResourceDraft {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), RegistryCommandError> {
        validate_audit(&self.audit, now)?;
        self.draft
            .validate()
            .map_err(|failure| RegistryCommandError::Contract(failure.to_string()))?;
        if self.expected_resource_version <= 0
            || self.resource_id.kind() != self.draft.document.kind().id_kind()
            || self.draft.validation.is_some()
        {
            return Err(RegistryCommandError::InvalidResourceDraft);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RequestResourceValidation {
    pub audit: CommandAudit,
    pub resource_id: ResourceId,
    pub expected_resource_version: i64,
    pub job_id: ResourceId,
    pub validator_digest: Sha256Digest,
    pub validation_profile_digest: Sha256Digest,
    pub attempt_limit: i32,
    pub scheduled_at: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
}

impl RequestResourceValidation {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), RegistryCommandError> {
        validate_audit(&self.audit, now)?;
        if self.expected_resource_version <= 0
            || self.job_id.kind() != ResourceKind::Job
            || self.attempt_limit <= 0
            || self.attempt_limit > 32
            || self.deadline <= self.scheduled_at
        {
            return Err(RegistryCommandError::InvalidValidationJob);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryValidationJobPayload {
    pub schema_version: u32,
    pub job_id: ResourceId,
    pub resource_id: ResourceId,
    pub resource_kind: RegistryResourceKind,
    pub expected_resource_version: u64,
    pub draft_digest: Sha256Digest,
    pub validator_digest: Sha256Digest,
    pub validation_profile_digest: Sha256Digest,
}

impl RegistryValidationJobPayload {
    pub fn from_request(
        command: &RequestResourceValidation,
        resource_kind: RegistryResourceKind,
        draft_digest: Sha256Digest,
    ) -> Result<Self, RegistryCommandError> {
        let expected_resource_version = u64::try_from(command.expected_resource_version)
            .map_err(|_| RegistryCommandError::InvalidValidationJob)?;
        let payload = Self {
            schema_version: 1,
            job_id: command.job_id.clone(),
            resource_id: command.resource_id.clone(),
            resource_kind,
            expected_resource_version,
            draft_digest,
            validator_digest: command.validator_digest.clone(),
            validation_profile_digest: command.validation_profile_digest.clone(),
        };
        payload.validate_for_owner(&command.job_id)?;
        Ok(payload)
    }

    pub fn validate_for_owner(&self, owner_id: &ResourceId) -> Result<(), RegistryCommandError> {
        if self.schema_version != 1
            || self.job_id.kind() != ResourceKind::Job
            || &self.job_id != owner_id
            || self.resource_id.kind() != self.resource_kind.id_kind()
            || self.expected_resource_version == 0
        {
            return Err(RegistryCommandError::InvalidValidationJob);
        }
        Ok(())
    }
}

/// Builds the only success summary accepted for a RegistryValidation Job.
///
/// The worker supplies the installed validator/profile identities, while the owner transaction
/// supplies the current Draft.  No public caller, queue message, or process-local result can
/// inject any part of the resulting validation evidence.
pub fn build_registry_validation_summary(
    payload: &RegistryValidationJobPayload,
    draft: &ResourceDraftPayload,
    installed_validator_digest: &Sha256Digest,
    installed_validation_profile_digest: &Sha256Digest,
) -> Result<ValidationSummary, RegistryCommandError> {
    payload.validate_for_owner(&payload.job_id)?;
    draft
        .validate()
        .map_err(|failure| RegistryCommandError::Contract(failure.to_string()))?;
    if payload.validator_digest != *installed_validator_digest
        || payload.validation_profile_digest != *installed_validation_profile_digest
        || draft.document.kind() != payload.resource_kind
        || draft
            .document_digest()
            .map_err(|failure| RegistryCommandError::Contract(failure.to_string()))?
            != payload.draft_digest
    {
        return Err(RegistryCommandError::InvalidValidationResult);
    }
    let dependency_closure_digest = canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "resource_kind": payload.resource_kind,
        "exact_version_refs": draft.document.exact_version_refs(),
    }))
    .map_err(|_| RegistryCommandError::InvalidValidationResult)?
    .parse()
    .map_err(|_| RegistryCommandError::InvalidValidationResult)?;
    let security_evidence_digest = canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "resource_kind": payload.resource_kind,
        "validated_draft_digest": payload.draft_digest,
        "validation_profile_digest": installed_validation_profile_digest,
    }))
    .map_err(|_| RegistryCommandError::InvalidValidationResult)?
    .parse()
    .map_err(|_| RegistryCommandError::InvalidValidationResult)?;
    let summary = ValidationSummary {
        validator_digest: installed_validator_digest.clone(),
        validated_draft_digest: payload.draft_digest.clone(),
        dependency_closure_digest,
        security_evidence_digest,
        warnings: Vec::new(),
    };
    summary
        .validate()
        .map_err(|failure| RegistryCommandError::Contract(failure.to_string()))?;
    Ok(summary)
}

#[derive(Debug, Clone)]
pub struct RecordResourceValidation {
    pub audit: CommandAudit,
    pub resource_id: ResourceId,
    pub expected_resource_version: i64,
    pub expected_draft_digest: Sha256Digest,
    pub validation: ValidationSummary,
}

impl RecordResourceValidation {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), RegistryCommandError> {
        validate_audit(&self.audit, now)?;
        self.validation
            .validate()
            .map_err(|failure| RegistryCommandError::Contract(failure.to_string()))?;
        if self.expected_resource_version <= 0
            || self.validation.validated_draft_digest != self.expected_draft_digest
        {
            return Err(RegistryCommandError::InvalidValidationResult);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct NewPublishedVersion {
    pub resource_version_id: ResourceId,
    pub revision_no: i64,
    pub content_digest: Sha256Digest,
    pub artifact_id: Option<ResourceId>,
    pub payload: PublishedVersionPayload,
}

#[derive(Debug, Clone)]
pub struct PublishResourceVersions {
    pub audit: CommandAudit,
    pub resource_id: ResourceId,
    pub expected_resource_version: i64,
    pub expected_draft_digest: Sha256Digest,
    pub versions: Vec<NewPublishedVersion>,
}

impl PublishResourceVersions {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), RegistryCommandError> {
        validate_audit(&self.audit, now)?;
        if self.expected_resource_version <= 0
            || self.versions.is_empty()
            || self.versions.len() > 2
            || self.versions.iter().any(|version| {
                version.revision_no <= 0
                    || version
                        .artifact_id
                        .as_ref()
                        .is_some_and(|artifact| artifact.kind() != ResourceKind::Artifact)
            })
        {
            return Err(RegistryCommandError::InvalidPublishBatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CreateDeployment {
    pub audit: CommandAudit,
    pub deployment_id: ResourceId,
    pub resource_id: ResourceId,
    pub resource_version_id: ResourceId,
    pub environment: String,
    pub closure: DeploymentClosure,
    pub expected_resource_version: i64,
}

impl CreateDeployment {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), RegistryCommandError> {
        validate_audit(&self.audit, now)?;
        self.closure
            .validate()
            .map_err(|failure| RegistryCommandError::Contract(failure.to_string()))?;
        if self.expected_resource_version <= 0
            || !self.deployment_id.kind().is_deployment()
            || !self.resource_version_id.kind().is_revision()
            || !is_code(&self.environment)
        {
            return Err(RegistryCommandError::InvalidDeployment);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ActivateResource {
    pub audit: CommandAudit,
    pub resource_id: ResourceId,
    pub expected_resource_version: i64,
    pub target: ActiveTarget,
}

#[derive(Debug, Clone)]
pub struct SuspendResourceDeployment {
    pub audit: CommandAudit,
    pub resource_id: ResourceId,
    pub deployment_id: ResourceId,
    pub expected_resource_version: i64,
}

impl SuspendResourceDeployment {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), RegistryCommandError> {
        validate_audit(&self.audit, now)?;
        require_positive_version(self.expected_resource_version)?;
        if !self.deployment_id.kind().is_deployment() {
            return Err(RegistryCommandError::InvalidDeployment);
        }
        Ok(())
    }
}

impl ActivateResource {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), RegistryCommandError> {
        validate_audit(&self.audit, now)?;
        require_positive_version(self.expected_resource_version)
    }
}

#[derive(Debug, Clone)]
pub struct TransitionResourceLifecycle {
    pub audit: CommandAudit,
    pub resource_id: ResourceId,
    pub expected_resource_version: i64,
    pub target: EntityLifecycle,
}

impl TransitionResourceLifecycle {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), RegistryCommandError> {
        validate_audit(&self.audit, now)?;
        require_positive_version(self.expected_resource_version)
    }
}

#[derive(Debug, Clone)]
pub struct SetResourceGate {
    pub audit: CommandAudit,
    pub resource_id: ResourceId,
    pub expected_resource_version: i64,
    pub target: AdministrativeGate,
}

impl SetResourceGate {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), RegistryCommandError> {
        validate_audit(&self.audit, now)?;
        require_positive_version(self.expected_resource_version)
    }
}

/// One caller-owned registry transaction. Implementations must not commit from mutation methods.
pub trait RegistryTransaction {
    type Error;
    type ResourceRecord;
    type PublishedResource;
    type DeploymentRecord;
    type ValidationJob;

    async fn create_resource_draft(
        &mut self,
        command: CreateResourceDraft,
    ) -> Result<CommandOutcome<Self::ResourceRecord>, Self::Error>;
    async fn update_resource_draft(
        &mut self,
        command: UpdateResourceDraft,
    ) -> Result<CommandOutcome<Self::ResourceRecord>, Self::Error>;
    async fn request_resource_validation(
        &mut self,
        command: RequestResourceValidation,
    ) -> Result<CommandOutcome<Self::ValidationJob>, Self::Error>;
    async fn record_resource_validation(
        &mut self,
        command: RecordResourceValidation,
    ) -> Result<CommandOutcome<Self::ResourceRecord>, Self::Error>;
    async fn publish_resource_versions(
        &mut self,
        command: PublishResourceVersions,
    ) -> Result<CommandOutcome<Self::PublishedResource>, Self::Error>;
    async fn create_deployment(
        &mut self,
        command: CreateDeployment,
    ) -> Result<CommandOutcome<Self::DeploymentRecord>, Self::Error>;
    async fn activate_resource(
        &mut self,
        command: ActivateResource,
    ) -> Result<CommandOutcome<Self::ResourceRecord>, Self::Error>;

    async fn suspend_resource_deployment(
        &mut self,
        command: SuspendResourceDeployment,
    ) -> Result<CommandOutcome<Self::ResourceRecord>, Self::Error>;
    async fn transition_resource_lifecycle(
        &mut self,
        command: TransitionResourceLifecycle,
    ) -> Result<CommandOutcome<Self::ResourceRecord>, Self::Error>;
    async fn set_resource_gate(
        &mut self,
        command: SetResourceGate,
    ) -> Result<CommandOutcome<Self::ResourceRecord>, Self::Error>;
    async fn commit(self) -> Result<(), Self::Error>;
    async fn rollback(self) -> Result<(), Self::Error>;
}

pub trait RegistryStore {
    type Error;
    type Transaction<'a>: RegistryTransaction<Error = Self::Error>
    where
        Self: 'a;

    async fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryCommandError {
    InvalidAudit,
    InvalidResourceDraft,
    InvalidValidationJob,
    InvalidValidationResult,
    InvalidPublishBatch,
    InvalidDeployment,
    InvalidVersion,
    Contract(String),
}

impl fmt::Display for RegistryCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAudit => {
                formatter.write_str("command audit identity or expiry is invalid")
            }
            Self::InvalidResourceDraft => {
                formatter.write_str("resource draft kind or CAS is invalid")
            }
            Self::InvalidValidationJob => {
                formatter.write_str("validation job identity, attempts, or deadline is invalid")
            }
            Self::InvalidValidationResult => {
                formatter.write_str("validation result does not bind the expected draft")
            }
            Self::InvalidPublishBatch => formatter.write_str("publish batch is invalid"),
            Self::InvalidDeployment => {
                formatter.write_str("deployment identity, environment, closure, or CAS is invalid")
            }
            Self::InvalidVersion => {
                formatter.write_str("expected resource version must be positive")
            }
            Self::Contract(message) => write!(formatter, "resource contract failed: {message}"),
        }
    }
}

impl Error for RegistryCommandError {}

fn validate_audit(audit: &CommandAudit, now: DateTime<Utc>) -> Result<(), RegistryCommandError> {
    audit
        .validate_at(now)
        .map_err(|_| RegistryCommandError::InvalidAudit)
}

fn require_positive_version(version: i64) -> Result<(), RegistryCommandError> {
    if version <= 0 {
        return Err(RegistryCommandError::InvalidVersion);
    }
    Ok(())
}

fn is_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> ResourceId {
        value.parse().unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    #[test]
    fn command_audit_rejects_interchangeable_ids() {
        let audit = CommandAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: id("ten_0198f1c3-8f49-7c3e-b1f3-773c28367b90"),
            principal_id: id("prn_0198f1c3-8f49-7c3e-b1f3-773c28367b91"),
            principal_kind: insight_platform_contracts::PrincipalKind::AgentRunner,
            receipt_id: id("evt_0198f1c3-8f49-7c3e-b1f3-773c28367b92"),
            event_id: id("evt_0198f1c3-8f49-7c3e-b1f3-773c28367b93"),
            outbox_id: id("obx_0198f1c3-8f49-7c3e-b1f3-773c28367b94"),
            idempotency_key_digest: digest('a'),
            request_digest: digest('b'),
            receipt_expires_at: Utc::now() + chrono::Duration::minutes(1),
        };
        assert_eq!(
            validate_audit(&audit, Utc::now()),
            Err(RegistryCommandError::InvalidAudit)
        );
    }

    #[test]
    fn registry_validation_summary_is_bound_to_the_exact_job_and_installed_closure() {
        let payload = RegistryValidationJobPayload {
            schema_version: 1,
            job_id: id("job_0198f1c3-8f49-7c3e-b1f3-773c28367b90"),
            resource_id: id("agt_0198f1c3-8f49-7c3e-b1f3-773c28367b91"),
            resource_kind: RegistryResourceKind::Agent,
            expected_resource_version: 1,
            draft_digest: digest('a'),
            validator_digest: digest('b'),
            validation_profile_digest: digest('c'),
        };
        let schema = insight_platform_contracts::ClosedJsonSchema::build(serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": false
        }))
        .unwrap();
        let draft = ResourceDraftPayload {
            display_name: "example".to_owned(),
            document: insight_platform_contracts::ResourceDocument::Agent(
                insight_platform_contracts::AgentResourceSpec {
                    authoring_package: insight_platform_contracts::AuthoringPackage {
                        artifact: insight_platform_contracts::ArtifactRef::new(
                            id("art_0198f1c3-8f49-7c3e-b1f3-773c28367b92"),
                            digest('d'),
                            1,
                            "application/json",
                            insight_platform_contracts::DataClassification::Internal,
                            None,
                        )
                        .unwrap(),
                        manifest_digest: digest('e'),
                    },
                    contract_digest: digest('f'),
                    dependency_versions: Vec::new(),
                    policy_versions: Vec::new(),
                    author_instructions: None,
                    input_schema: schema.clone(),
                    output_schema: schema.clone(),
                    error_schema: schema,
                    typed_plan_artifact_id: id("art_0198f1c3-8f49-7c3e-b1f3-773c28367b93"),
                    typed_plan_digest: digest('1'),
                },
            ),
            validation: None,
        };
        let mut payload = payload;
        payload.draft_digest = draft.document_digest().unwrap();
        let summary =
            build_registry_validation_summary(&payload, &draft, &digest('b'), &digest('c'))
                .unwrap();
        assert_eq!(summary.validated_draft_digest, payload.draft_digest);
        assert!(
            build_registry_validation_summary(&payload, &draft, &digest('0'), &digest('c'))
                .is_err()
        );
    }
}
