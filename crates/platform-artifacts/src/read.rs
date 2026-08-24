use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    ArtifactRef, DataClassification, PrincipalKind, ResourceId, ResourceKind, Sha256Digest,
};
use std::fmt;

pub const MAX_TYPED_PLAN_ARTIFACT_BYTES: usize = 16 * 1_024 * 1_024;
pub const MAX_SCHEDULER_RUN_VALUE_BYTES: usize = 1_048_576;

pub const MAX_ARTIFACT_OBJECT_REFERENCE_BYTES: usize = 16_384;
pub const MAX_ARTIFACT_OBJECT_GENERATION_BYTES: usize = 255;
pub const MAX_ARTIFACT_STORAGE_BACKEND_BYTES: usize = 64;
pub const MAX_ARTIFACT_KMS_KEY_ID_BYTES: usize = 255;

/// Envelope-encrypted physical object locator exposed only to the trusted Artifact Broker.
///
/// It is intentionally non-clone, redacted in diagnostics and zeroed on drop. Public Artifact
/// projections never contain this value.
pub struct EncryptedArtifactObjectReference(Vec<u8>);

impl EncryptedArtifactObjectReference {
    pub fn new(mut ciphertext: Vec<u8>) -> Result<Self, ArtifactObjectReadAuthorityError> {
        if ciphertext.is_empty() || ciphertext.len() > MAX_ARTIFACT_OBJECT_REFERENCE_BYTES {
            ciphertext.fill(0);
            return Err(ArtifactObjectReadAuthorityError::InvalidEvidence);
        }
        Ok(Self(ciphertext))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for EncryptedArtifactObjectReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncryptedArtifactObjectReference")
            .field("byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Drop for EncryptedArtifactObjectReference {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// One non-persisted authorization winner for reading an exact physical Artifact generation.
///
/// The authority digest binds caller-specific authorization evidence. The object locator remains
/// encrypted; only the Broker's KMS adapter may expose it.
pub struct AuthorizedArtifactObjectRead {
    pub tenant_id: ResourceId,
    pub blob_id: ResourceId,
    pub artifact: ArtifactRef,
    pub backend: String,
    pub storage_binding_digest: Sha256Digest,
    pub encryption_domain_id: ResourceId,
    pub key_id: String,
    pub object_reference_ciphertext: EncryptedArtifactObjectReference,
    pub object_generation: String,
    pub authorization_digest: Sha256Digest,
}

impl AuthorizedArtifactObjectRead {
    pub fn validate(&self) -> Result<(), ArtifactObjectReadAuthorityError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.encryption_domain_id.kind() != ResourceKind::EncryptionDomain
            || self.artifact.validate().is_err()
            || !stable_code(&self.backend, MAX_ARTIFACT_STORAGE_BACKEND_BYTES)
            || self.key_id.is_empty()
            || self.key_id.len() > MAX_ARTIFACT_KMS_KEY_ID_BYTES
            || self.key_id.chars().any(char::is_control)
            || self.object_generation.is_empty()
            || self.object_generation.len() > MAX_ARTIFACT_OBJECT_GENERATION_BYTES
            || self.object_generation.chars().any(char::is_control)
        {
            return Err(ArtifactObjectReadAuthorityError::InvalidEvidence);
        }
        Ok(())
    }
}

impl fmt::Debug for AuthorizedArtifactObjectRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizedArtifactObjectRead")
            .field("tenant_id", &self.tenant_id)
            .field("blob_id", &self.blob_id)
            .field("artifact", &self.artifact)
            .field("backend", &self.backend)
            .field("storage_binding_digest", &self.storage_binding_digest)
            .field("encryption_domain_id", &self.encryption_domain_id)
            .field("key_id", &"[redacted]")
            .field(
                "object_reference_ciphertext",
                &self.object_reference_ciphertext,
            )
            .field("object_generation", &"[redacted]")
            .field("authorization_digest", &self.authorization_digest)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactObjectReadAuthorityError {
    Unavailable,
    Denied,
    NotFound,
    InvalidEvidence,
}

/// Read-only database authority. `R` is the consumer-specific closed authorization request.
#[async_trait]
pub trait ArtifactObjectReadAuthority<R>: Send + Sync {
    async fn authorize_object_read(
        &self,
        request: &R,
    ) -> Result<AuthorizedArtifactObjectRead, ArtifactObjectReadAuthorityError>;
}

/// Scheduler-owned lookup key for resolving the exact Typed Plan descriptor frozen by a Run.
/// It carries no caller-selected Plan or Artifact identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerTypedPlanLease {
    pub tenant_id: ResourceId,
    pub run_id: ResourceId,
    pub orchestration_job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub lease_token_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub maximum_bytes: usize,
    pub deadline: DateTime<Utc>,
}

impl SchedulerTypedPlanLease {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactObjectReadAuthorityError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.run_id.kind() != ResourceKind::Run
            || self.orchestration_job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation == 0
            || self.maximum_bytes == 0
            || self.maximum_bytes > MAX_TYPED_PLAN_ARTIFACT_BYTES
            || self.deadline <= now
        {
            return Err(ArtifactObjectReadAuthorityError::Denied);
        }
        Ok(())
    }
}

#[async_trait]
pub trait SchedulerTypedPlanRequestResolver: Send + Sync {
    async fn resolve_typed_plan_read(
        &self,
        lease: SchedulerTypedPlanLease,
    ) -> Result<SchedulerTypedPlanReadRequest, ArtifactObjectReadAuthorityError>;
}

/// Closed Scheduler request for the immutable Typed Plan bound to one leased orchestration Job.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerTypedPlanReadRequest {
    pub tenant_id: ResourceId,
    pub run_id: ResourceId,
    pub orchestration_job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub lease_token_digest: Sha256Digest,
    pub plan_revision_id: ResourceId,
    pub artifact: ArtifactRef,
    pub request_digest: Sha256Digest,
    pub maximum_bytes: usize,
    pub deadline: DateTime<Utc>,
}

impl SchedulerTypedPlanReadRequest {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactObjectReadAuthorityError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.run_id.kind() != ResourceKind::Run
            || self.orchestration_job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.plan_revision_id.kind() != ResourceKind::AgentPlanRevision
            || self.lease_generation == 0
            || self.artifact.validate().is_err()
            || self.artifact.media_type() != "application/json"
            || self.maximum_bytes == 0
            || self.maximum_bytes > MAX_TYPED_PLAN_ARTIFACT_BYTES
            || u64::try_from(self.maximum_bytes)
                .map_or(true, |maximum| maximum < self.artifact.byte_length())
            || self.deadline <= now
        {
            return Err(ArtifactObjectReadAuthorityError::Denied);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerTypedPlanReadError {
    Unavailable,
    Denied,
    NotFound,
    TooLarge,
    Integrity,
}

#[async_trait]
pub trait SchedulerTypedPlanReader: Send + Sync {
    async fn read_exact(
        &self,
        request: SchedulerTypedPlanReadRequest,
    ) -> Result<Vec<u8>, SchedulerTypedPlanReadError>;
}

/// Scheduler-owned lookup key for one immutable Artifact-backed RunValue already resolved by the
/// durable controller authority. The resolver rechecks the value under the current Job fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerRunValueLease {
    pub tenant_id: ResourceId,
    pub run_id: ResourceId,
    pub orchestration_job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub lease_token_digest: Sha256Digest,
    pub run_value_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub maximum_bytes: usize,
    pub deadline: DateTime<Utc>,
}

impl SchedulerRunValueLease {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactObjectReadAuthorityError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.run_id.kind() != ResourceKind::Run
            || self.orchestration_job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.run_value_id.kind() != ResourceKind::RunValue
            || self.lease_generation == 0
            || self.maximum_bytes == 0
            || self.maximum_bytes > MAX_SCHEDULER_RUN_VALUE_BYTES
            || self.deadline <= now
        {
            return Err(ArtifactObjectReadAuthorityError::Denied);
        }
        Ok(())
    }
}

#[async_trait]
pub trait SchedulerRunValueRequestResolver: Send + Sync {
    async fn resolve_run_value_read(
        &self,
        lease: SchedulerRunValueLease,
    ) -> Result<SchedulerRunValueReadRequest, ArtifactObjectReadAuthorityError>;
}

/// Closed Scheduler request for the exact Artifact generation referenced by one immutable
/// RunValue. It never contains a physical object locator or storage credential.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerRunValueReadRequest {
    pub tenant_id: ResourceId,
    pub run_id: ResourceId,
    pub orchestration_job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub lease_token_digest: Sha256Digest,
    pub run_value_id: ResourceId,
    pub schema_digest: Sha256Digest,
    pub classification: DataClassification,
    pub artifact: ArtifactRef,
    pub request_digest: Sha256Digest,
    pub maximum_bytes: usize,
    pub deadline: DateTime<Utc>,
}

impl SchedulerRunValueReadRequest {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactObjectReadAuthorityError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.run_id.kind() != ResourceKind::Run
            || self.orchestration_job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.run_value_id.kind() != ResourceKind::RunValue
            || self.lease_generation == 0
            || self.artifact.validate().is_err()
            || (self.artifact.media_type() != "application/json"
                && !self.artifact.media_type().ends_with("+json"))
            || self.artifact.classification() != self.classification
            || self.maximum_bytes == 0
            || self.maximum_bytes > MAX_SCHEDULER_RUN_VALUE_BYTES
            || u64::try_from(self.maximum_bytes)
                .map_or(true, |maximum| maximum < self.artifact.byte_length())
            || self.deadline <= now
        {
            return Err(ArtifactObjectReadAuthorityError::Denied);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerRunValueReadError {
    Unavailable,
    Denied,
    NotFound,
    TooLarge,
    Integrity,
}

#[async_trait]
pub trait SchedulerRunValueReader: Send + Sync {
    async fn read_exact(
        &self,
        request: SchedulerRunValueReadRequest,
    ) -> Result<Vec<u8>, SchedulerRunValueReadError>;
}

/// Closed public-Gateway request for one exact, bounded Artifact generation.
///
/// Authentication is terminated before the Gateway constructs this value. The database still
/// revalidates the current tenant binding, `artifact.read` permission, Ready state and an active
/// durable reference before every physical read, including the post-I/O authorization pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayArtifactReadRequest {
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub artifact: ArtifactRef,
    pub request_digest: Sha256Digest,
    pub maximum_bytes: usize,
    pub deadline: DateTime<Utc>,
}

impl GatewayArtifactReadRequest {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ArtifactObjectReadAuthorityError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.principal_id.kind() != ResourceKind::Principal
            || self.artifact.validate().is_err()
            || self.maximum_bytes == 0
            || u64::try_from(self.maximum_bytes)
                .map_or(true, |maximum| maximum < self.artifact.byte_length())
            || self.deadline <= now
        {
            return Err(ArtifactObjectReadAuthorityError::Denied);
        }
        Ok(())
    }
}

/// Exact pre-verification object authority used only by the Artifact Data Worker scanner.
/// Unlike a normal read, content digest and media type may not yet be authoritative.
pub struct AuthorizedArtifactScanObjectRead {
    pub tenant_id: ResourceId,
    pub artifact_id: ResourceId,
    pub blob_id: ResourceId,
    pub backend: String,
    pub storage_binding_digest: Sha256Digest,
    pub encryption_domain_id: ResourceId,
    pub key_id: String,
    pub object_reference_ciphertext: EncryptedArtifactObjectReference,
    pub object_generation: String,
    pub maximum_bytes: u64,
    pub expected_digest: Option<Sha256Digest>,
    pub declared_media_type: Option<String>,
    pub authorization_digest: Sha256Digest,
}

impl AuthorizedArtifactScanObjectRead {
    pub fn validate(&self) -> Result<(), ArtifactObjectReadAuthorityError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.encryption_domain_id.kind() != ResourceKind::EncryptionDomain
            || !stable_code(&self.backend, MAX_ARTIFACT_STORAGE_BACKEND_BYTES)
            || self.key_id.is_empty()
            || self.key_id.len() > MAX_ARTIFACT_KMS_KEY_ID_BYTES
            || self.key_id.chars().any(char::is_control)
            || self.object_generation.is_empty()
            || self.object_generation.len() > MAX_ARTIFACT_OBJECT_GENERATION_BYTES
            || self.object_generation.chars().any(char::is_control)
            || self.maximum_bytes == 0
            || self.declared_media_type.as_deref().is_some_and(|value| {
                value.is_empty()
                    || value.len() > 255
                    || !value.is_ascii()
                    || value.chars().any(char::is_control)
            })
        {
            return Err(ArtifactObjectReadAuthorityError::InvalidEvidence);
        }
        Ok(())
    }
}

impl fmt::Debug for AuthorizedArtifactScanObjectRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizedArtifactScanObjectRead")
            .field("tenant_id", &self.tenant_id)
            .field("artifact_id", &self.artifact_id)
            .field("blob_id", &self.blob_id)
            .field("backend", &self.backend)
            .field("storage_binding_digest", &self.storage_binding_digest)
            .field("key_id", &"[redacted]")
            .field(
                "object_reference_ciphertext",
                &self.object_reference_ciphertext,
            )
            .field("object_generation", &"[redacted]")
            .field("maximum_bytes", &self.maximum_bytes)
            .field("expected_digest", &self.expected_digest)
            .field("declared_media_type", &self.declared_media_type)
            .field("authorization_digest", &self.authorization_digest)
            .finish()
    }
}

#[async_trait]
pub trait ArtifactScanObjectReadAuthority<R>: Send + Sync {
    async fn authorize_scan_object_read(
        &self,
        request: &R,
    ) -> Result<AuthorizedArtifactScanObjectRead, ArtifactObjectReadAuthorityError>;
}

pub struct AuthorizedArtifactDeleteObject {
    pub tenant_id: ResourceId,
    pub blob_id: ResourceId,
    pub backend: String,
    pub storage_binding_digest: Sha256Digest,
    pub encryption_domain_id: ResourceId,
    pub key_id: String,
    pub object_reference_ciphertext: EncryptedArtifactObjectReference,
    pub object_generation: String,
    pub authorization_digest: Sha256Digest,
}

impl AuthorizedArtifactDeleteObject {
    pub fn validate(&self) -> Result<(), ArtifactObjectReadAuthorityError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.encryption_domain_id.kind() != ResourceKind::EncryptionDomain
            || !stable_code(&self.backend, MAX_ARTIFACT_STORAGE_BACKEND_BYTES)
            || self.key_id.is_empty()
            || self.key_id.len() > MAX_ARTIFACT_KMS_KEY_ID_BYTES
            || self.key_id.chars().any(char::is_control)
            || self.object_generation.is_empty()
            || self.object_generation.len() > MAX_ARTIFACT_OBJECT_GENERATION_BYTES
            || self.object_generation.chars().any(char::is_control)
        {
            return Err(ArtifactObjectReadAuthorityError::InvalidEvidence);
        }
        Ok(())
    }
}

impl fmt::Debug for AuthorizedArtifactDeleteObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizedArtifactDeleteObject")
            .field("tenant_id", &self.tenant_id)
            .field("blob_id", &self.blob_id)
            .field("backend", &self.backend)
            .field("storage_binding_digest", &self.storage_binding_digest)
            .field("key_id", &"[redacted]")
            .field(
                "object_reference_ciphertext",
                &self.object_reference_ciphertext,
            )
            .field("object_generation", &"[redacted]")
            .field("authorization_digest", &self.authorization_digest)
            .finish()
    }
}

#[async_trait]
pub trait ArtifactDeleteObjectAuthority<R>: Send + Sync {
    async fn authorize_delete_object(
        &self,
        request: &R,
    ) -> Result<AuthorizedArtifactDeleteObject, ArtifactObjectReadAuthorityError>;
}

fn stable_code(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.bytes().enumerate().all(|(index, byte)| match byte {
            b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => {
                index != 0 || byte.is_ascii_lowercase()
            }
            _ => false,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{DataClassification, ResourceKind};
    use uuid::Uuid;

    fn id(kind: ResourceKind, _suffix: u128) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    #[test]
    fn trusted_projection_is_valid_and_diagnostics_are_redacted() {
        let artifact = ArtifactRef::new(
            id(ResourceKind::Artifact, 3),
            digest('a'),
            7,
            "application/json".to_owned(),
            DataClassification::Internal,
            None,
        )
        .unwrap();
        let projection = AuthorizedArtifactObjectRead {
            tenant_id: id(ResourceKind::Tenant, 1),
            blob_id: id(ResourceKind::InternalBlob, 2),
            artifact,
            backend: "s3".to_owned(),
            storage_binding_digest: digest('b'),
            encryption_domain_id: id(ResourceKind::EncryptionDomain, 4),
            key_id: "kms-key-canary".to_owned(),
            object_reference_ciphertext: EncryptedArtifactObjectReference::new(
                b"ciphertext-canary".to_vec(),
            )
            .unwrap(),
            object_generation: "version-canary".to_owned(),
            authorization_digest: digest('c'),
        };
        projection.validate().unwrap();
        let diagnostic = format!("{projection:?}");
        assert!(!diagnostic.contains("kms-key-canary"));
        assert!(!diagnostic.contains("ciphertext-canary"));
        assert!(!diagnostic.contains("version-canary"));
    }

    #[test]
    fn gateway_read_request_is_exact_bounded_and_time_limited() {
        let now = Utc::now();
        let artifact = ArtifactRef::new(
            id(ResourceKind::Artifact, 3),
            digest('a'),
            7,
            "application/json".to_owned(),
            DataClassification::Internal,
            None,
        )
        .unwrap();
        let request = GatewayArtifactReadRequest {
            tenant_id: id(ResourceKind::Tenant, 1),
            principal_id: id(ResourceKind::Principal, 2),
            principal_kind: PrincipalKind::TenantAdmin,
            artifact,
            request_digest: digest('b'),
            maximum_bytes: 7,
            deadline: now + chrono::Duration::seconds(1),
        };
        request.validate_at(now).unwrap();

        let mut too_small = request.clone();
        too_small.maximum_bytes = 6;
        assert_eq!(
            too_small.validate_at(now),
            Err(ArtifactObjectReadAuthorityError::Denied)
        );
        let mut expired = request;
        expired.deadline = now;
        assert_eq!(
            expired.validate_at(now),
            Err(ArtifactObjectReadAuthorityError::Denied)
        );
    }

    #[test]
    fn scheduler_typed_plan_read_is_exact_json_bounded_and_fenced() {
        let now = Utc::now();
        let artifact = ArtifactRef::new(
            id(ResourceKind::Artifact, 7),
            digest('a'),
            7,
            "application/json".to_owned(),
            DataClassification::Internal,
            Some("typed-plan.json".to_owned()),
        )
        .unwrap();
        let request = SchedulerTypedPlanReadRequest {
            tenant_id: id(ResourceKind::Tenant, 1),
            run_id: id(ResourceKind::Run, 2),
            orchestration_job_id: id(ResourceKind::Job, 3),
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 4),
            lease_generation: 2,
            lease_token_digest: digest('b'),
            plan_revision_id: id(ResourceKind::AgentPlanRevision, 5),
            artifact,
            request_digest: digest('c'),
            maximum_bytes: 7,
            deadline: now + chrono::Duration::seconds(1),
        };
        request.validate_at(now).unwrap();

        let mut unfenced = request.clone();
        unfenced.lease_generation = 0;
        assert_eq!(
            unfenced.validate_at(now),
            Err(ArtifactObjectReadAuthorityError::Denied)
        );
        let mut too_small = request.clone();
        too_small.maximum_bytes = 6;
        assert_eq!(
            too_small.validate_at(now),
            Err(ArtifactObjectReadAuthorityError::Denied)
        );
        let mut wrong_media = request.clone();
        wrong_media.artifact = ArtifactRef::new(
            id(ResourceKind::Artifact, 8),
            digest('d'),
            7,
            "application/octet-stream".to_owned(),
            DataClassification::Internal,
            None,
        )
        .unwrap();
        assert_eq!(
            wrong_media.validate_at(now),
            Err(ArtifactObjectReadAuthorityError::Denied)
        );
        let mut oversized = request.clone();
        oversized.maximum_bytes = MAX_TYPED_PLAN_ARTIFACT_BYTES + 1;
        assert_eq!(
            oversized.validate_at(now),
            Err(ArtifactObjectReadAuthorityError::Denied)
        );

        let lease = SchedulerTypedPlanLease {
            tenant_id: request.tenant_id,
            run_id: request.run_id,
            orchestration_job_id: request.orchestration_job_id,
            worker_process_generation_id: request.worker_process_generation_id,
            lease_generation: 2,
            lease_token_digest: digest('e'),
            request_digest: digest('f'),
            maximum_bytes: MAX_TYPED_PLAN_ARTIFACT_BYTES,
            deadline: now + chrono::Duration::seconds(1),
        };
        lease.validate_at(now).unwrap();
        let mut expired_lease = lease;
        expired_lease.deadline = now;
        assert_eq!(
            expired_lease.validate_at(now),
            Err(ArtifactObjectReadAuthorityError::Denied)
        );
    }

    #[test]
    fn scheduler_run_value_read_binds_value_classification_and_json_limit() {
        let now = Utc::now();
        let artifact = ArtifactRef::new(
            id(ResourceKind::Artifact, 9),
            digest('a'),
            7,
            "application/problem+json".to_owned(),
            DataClassification::Confidential,
            Some("terminal.json".to_owned()),
        )
        .unwrap();
        let request = SchedulerRunValueReadRequest {
            tenant_id: id(ResourceKind::Tenant, 1),
            run_id: id(ResourceKind::Run, 2),
            orchestration_job_id: id(ResourceKind::Job, 3),
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 4),
            lease_generation: 2,
            lease_token_digest: digest('b'),
            run_value_id: id(ResourceKind::RunValue, 5),
            schema_digest: digest('c'),
            classification: DataClassification::Confidential,
            artifact,
            request_digest: digest('d'),
            maximum_bytes: 7,
            deadline: now + chrono::Duration::seconds(1),
        };
        request.validate_at(now).unwrap();

        let mut mismatched = request.clone();
        mismatched.classification = DataClassification::Internal;
        assert_eq!(
            mismatched.validate_at(now),
            Err(ArtifactObjectReadAuthorityError::Denied)
        );
        let mut oversized = request.clone();
        oversized.maximum_bytes = MAX_SCHEDULER_RUN_VALUE_BYTES + 1;
        assert_eq!(
            oversized.validate_at(now),
            Err(ArtifactObjectReadAuthorityError::Denied)
        );

        let lease = SchedulerRunValueLease {
            tenant_id: request.tenant_id,
            run_id: request.run_id,
            orchestration_job_id: request.orchestration_job_id,
            worker_process_generation_id: request.worker_process_generation_id,
            lease_generation: request.lease_generation,
            lease_token_digest: request.lease_token_digest,
            run_value_id: request.run_value_id,
            request_digest: request.request_digest,
            maximum_bytes: MAX_SCHEDULER_RUN_VALUE_BYTES,
            deadline: request.deadline,
        };
        lease.validate_at(now).unwrap();
        let mut wrong_kind = lease;
        wrong_kind.run_value_id = id(ResourceKind::Artifact, 6);
        assert_eq!(
            wrong_kind.validate_at(now),
            Err(ArtifactObjectReadAuthorityError::Denied)
        );
    }
}
